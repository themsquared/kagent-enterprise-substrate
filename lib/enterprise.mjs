// kagent-enterprise adapter: everything Scope needs from the controller's
// gRPC-Web API (kagent-enterprise 1.0.0-alpha3, Agent Substrate 0.2.0-beta5).
//
// Model, as observed on a live alpha3 cluster:
//   AgentTemplate + Harness  -> ActorTemplate "<agent>-<harness>-<rev12>" with a
//                               golden snapshot
//   AgentInstance            -> one actor "ai-<instance id>", created SUSPENDED
//                               straight from the golden snapshot
//   A2A turn to an instance  -> RESUMING@<worker> -> RUNNING -> SUSPENDING
//
// Two alpha3 behaviors shape this file:
//   1. An oversubscribed send does not fast-reject. It times out (~5s,
//      INTERNAL "actor ... request timed out") and leaves its task SUBMITTED;
//      the actor then pins a worker until the task is cancelled. So Scope
//      admits only as many turns as there are free workers (the rest wait in
//      Scope's queue lane), and a watchdog cancels orphaned tasks.
//   2. An instance runs one task at a time ("already has an active task").
//      Each agent gets a small pool of Scope-owned instances.
import { client, GrpcError } from './grpcweb.mjs';

const SYS = 'kagent.api.v1alpha1.SystemService/';
const INST = 'kagent.api.v1alpha1.AgentInstanceService/';
const A2A = 'lf.a2a.v1.A2AService/';
const HDR_INSTANCE = 'x-kagent-agent-instance-id';
const OWN = 'scope-';              // name prefix of Scope-owned instances
const TURNS_PER_INSTANCE = 12;     // rotate so a conversation's context stays small
// Orphan = SUBMITTED while its actor already sits RUNNING (the wedge: the
// actor holds a worker and never picks the task up), or SUBMITTED past the
// hard limit. SUBMITTED + RESUMING is a normal restore, never cancelled.
const ORPHAN_MS = 25_000;
const ORPHAN_HARD_MS = 120_000;

const STATE = {                    // ateapi.ActorState -> the board's vocabulary
  ACTOR_STATE_RESUMING: 'Resuming', ACTOR_STATE_RUNNING: 'Running',
  ACTOR_STATE_SUSPENDING: 'Suspending', ACTOR_STATE_SUSPENDED: 'Suspended',
  ACTOR_STATE_PAUSING: 'Pausing', ACTOR_STATE_PAUSED: 'Paused',
  ACTOR_STATE_CRASHED: 'Crashed', ACTOR_STATE_DELETING: 'Deleting',
  ACTOR_STATE_REVERTING: 'Reverting',
};
const ON_WORKER = new Set(['Resuming', 'Running', 'Suspending']);
const sleep = ms => new Promise(r => setTimeout(r, ms));
// A2A parts are streamed chunks (often mid-word): concatenate, never space-join.
const textOf = parts => (parts ?? []).map(p => p.text ?? '').join('');

// ── auth: bearer token for the controller ────────────────────────────────────
// KAGENT_TOKEN wins (a real OIDC deployment). Otherwise mint one from the
// bundled autoauth IdP through the UI, exactly as the kagent UI does.
// The bundled IdP runs inside the controller pod, so every controller restart
// (a helm upgrade, a crash) mints a new signing key and invalidates cached
// tokens. invalidate() forces a fresh token on the next call.
export function tokenSource({ uiBase, token }) {
  let cached = token || '', exp = token ? Infinity : 0;
  const get = async () => {
    if (cached && Date.now() < exp - 60_000) return { Authorization: `Bearer ${cached}` };
    const r = await fetch(`${uiBase}/autoauth/token`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ user_type: 'admin' }), signal: AbortSignal.timeout(5000) });
    if (!r.ok) throw new Error(`autoauth token: HTTP ${r.status} (set KAGENT_TOKEN for an OIDC cluster)`);
    cached = (await r.json()).access_token;
    try { exp = JSON.parse(Buffer.from(cached.split('.')[1], 'base64url')).exp * 1000; }
    catch { exp = Date.now() + 10 * 60_000; }
    return { Authorization: `Bearer ${cached}` };
  };
  get.invalidate = () => { if (!token) { cached = ''; exp = 0; } };
  return get;
}

// Re-mint the token and retry once when the controller rejects it.
const AUTH_REJECT = e => e instanceof GrpcError && (e.code === 16 || (e.code === 7 && /authori[sz]ed|credential/i.test(e.message)));
function withAuthRetry(c, tokens) {
  return {
    unary: async (m, req, opts) => {
      try { return await c.unary(m, req, opts); }
      catch (e) { if (!AUTH_REJECT(e)) throw e; tokens.invalidate(); return c.unary(m, req, opts); }
    },
    async *stream(m, req, opts) {
      let started = false;
      try { for await (const ev of c.stream(m, req, opts)) { started = true; yield ev; } }
      catch (e) {
        if (started || !AUTH_REJECT(e)) throw e;
        tokens.invalidate();
        yield* c.stream(m, req, opts);
      }
    },
  };
}

export function enterprise({ apiBase, uiBase, token, atespace = 'kagent', log = () => {} }) {
  const tokens = tokenSource({ uiBase, token });
  const api = withAuthRetry(client(apiBase, tokens), tokens);
  const onInstance = id => ({ headers: { [HDR_INSTANCE]: id } });
  // Page through a List* RPC (the API caps page.limit at 100).
  // Board reads are small and frequent: fail fast (5s) so a dead
  // port-forward is noticed in seconds, not after the 20s default.
  async function listAll(method, req, key, maxPages = 20) {
    const out = []; let token = '', resp = {};
    for (let i = 0; i < maxPages; i++) {
      resp = await api.unary(method, { ...req, page: { limit: 100, page_token: token } }, { timeout: 5000 });
      out.push(...(resp[key] ?? []));
      token = resp.page?.next_page_token;
      if (!token) break;
    }
    return { ...resp, [key]: out };
  }

  // ActorTemplate name -> agent name. Generated names are
  // "<agent>-<harness>-<12 hex>"; harness names come from the live list.
  let harnessNames = ['claude', 'codex', 'kagent'];
  const agentOf = tpl => {
    if (!tpl) return tpl;
    const base = tpl.replace(/-[0-9a-f]{12}$/, '');
    const h = harnessNames.find(h => base.endsWith(`-${h}`));
    return h ? base.slice(0, -(h.length + 1)) : base;
  };

  // ── agents: AgentTemplates that some Harness has made Ready ────────────────
  // Each agent runs on its Harness's WorkerPool (Harness.spec.substrate.
  // workerPoolRef), so the pool is a property of the agent: admission and
  // autoscaling are per pool.
  let agentCache = { at: 0, list: [] };
  async function agents() {
    if (Date.now() - agentCache.at < 10_000) return agentCache.list;
    const [r, hr] = await Promise.all([
      api.unary('kagent.api.v1alpha1.AgentTemplateService/ListAgentTemplates', {}),
      api.unary('kagent.api.v1alpha1.HarnessService/ListHarnesses', { namespace: atespace }),
    ]);
    const poolOf = new Map((hr.harnesses ?? []).map(h =>
      [h.ref?.name, h.resource?.value?.spec?.substrate?.workerPoolRef?.name]));
    // the label selector is WHY an agent lands on a Harness (and so a pool)
    const selectorOf = new Map((hr.harnesses ?? []).map(h =>
      [h.ref?.name, Object.entries(h.resource?.value?.spec?.allowedAgentTemplates?.selector?.matchLabels ?? {})
                          .map(([k, v]) => `${k}=${v}`).join(',')]));
    const list = [];
    const hs = new Set();
    for (const t of r.agent_templates ?? []) {
      const v = t.resource?.value ?? {};
      for (const h of v.status?.harnesses ?? []) {
        hs.add(h.harness);
        const ready = (h.conditions ?? []).some(c => c.type === 'Ready' && c.status === 'True');
        if (ready) list.push({ ns: t.ref.namespace, name: t.ref.name, harness: h.harness,
                               pool: poolOf.get(h.harness) ?? null, selector: selectorOf.get(h.harness) ?? '',
                               description: v.spec?.description ?? '' });
      }
    }
    // longest first, so "claude-oncall" is stripped before "claude" could be
    if (hs.size) harnessNames = [...hs].sort((a, b) => b.length - a.length);
    agentCache = { at: Date.now(), list };
    return list;
  }

  // ── snapshot in the board's shape (the OSS /api/substrate/status shape) ────
  let summary = { at: 0, v: null };
  let actorState = new Map();      // actor name -> board status, from the last snapshot
  async function snapshot() {
    const fleet = await agents().catch(() => agentCache.list);   // harness names before agentOf()
    const [w, a] = await Promise.all([
      listAll(SYS + 'ListSubstrateWorkers', {}, 'workers'),
      listAll(SYS + 'ListSubstrateActors', { atespace }, 'actors'),
    ]);
    if (Date.now() - summary.at > 5000 || !summary.v)
      summary = { at: Date.now(), v: await api.unary(SYS + 'GetSubstrateSummary', { namespace: atespace }, { timeout: 5000 }) };
    const s = summary.v;
    const actors = (a.actors ?? []).map(x => ({
      actorId: x.metadata?.name,
      actorTemplateName: agentOf(x.actor_template?.name),
      actorTemplateNamespace: x.actor_template?.atespace || atespace,
      status: STATE[x.status?.state] ?? 'Unknown',
      workerPod: x.status?.worker_assignment?.worker_pod ?? null,
    }));
    actorState = new Map(actors.map(x => [x.actorId, x.status]));
    const byPod = new Map(actors.filter(x => x.workerPod && ON_WORKER.has(x.status))
                                .map(x => [x.workerPod, x]));
    const workers = (w.workers ?? []).map(x => {
      const hot = byPod.get(x.worker_pod);
      return { workerNamespace: x.worker_namespace, workerPool: x.worker_pool, workerPod: x.worker_pod,
               actorId: hot?.actorId, actorTemplate: hot?.actorTemplateName };
    });
    // One entry per agent: moving an agent to another Harness leaves its old
    // ActorTemplate behind, and it must not count as a second agent.
    const byAgent = new Map();
    for (const t of s.actor_templates ?? []) {
      const name = agentOf(t.metadata?.name);
      if (byAgent.has(name)) continue;
      byAgent.set(name, {
        namespace: t.metadata?.atespace || atespace, name,
        phase: t.status?.golden_snapshot_status?.golden_tag?.name ? 'Ready' : 'Pending',
        limits: Object.fromEntries((t.resources?.limits ?? []).map(l => [l.name, l.quantity])),
      });
    }
    const actorTemplates = [...byAgent.values()];
    // A template with no instance yet still deserves a chip: its golden actor.
    const seen = new Set(actors.map(x => x.actorTemplateName));
    for (const t of actorTemplates)
      if (!seen.has(t.name)) actors.push({ actorId: `golden-${t.name}`, actorTemplateName: t.name,
                                           actorTemplateNamespace: t.namespace, status: 'Suspended' });
    const workerPools = (s.worker_pools ?? []).map(p => ({
      namespace: p.ref?.namespace, name: p.ref?.name,
      replicas: p.resource?.value?.spec?.replicas ?? workers.length }));
    const agentPools = Object.fromEntries(fleet.map(a => [a.name, a.pool]));
    const agentRoutes = Object.fromEntries(fleet.map(a =>
      [a.name, { pool: a.pool, harness: a.harness, selector: a.selector }]));
    return { enabled: true, atespace, workerPools, actorTemplates, actors, workers, agentPools, agentRoutes,
             ateApiError: s.ate_api_error || a.ate_api_error || w.ate_api_error || undefined };
  }

  // ── instance pool: Scope-owned sessions per agent ───────────────────────────
  const pool = new Map();          // "ns/name" -> [{id, context_id, turns, busy}]
  // Fresh instances every run: adopting leftovers risks inheriting one wedged
  // by an earlier overload (alpha3 can leave a lifecycle op pending). Scope
  // deletes its own on exit and clears leftovers at start, so the kagent UI's
  // "Cost per Agent Instance" table stays readable.
  async function cleanup({ leftovers = false } = {}) {
    const mine = new Set([...pool.values()].flat().filter(i => !i.busy).map(i => i.id));
    let ids = [...mine];
    if (leftovers) {
      const r = await listAll(INST + 'ListAgentInstances', {}, 'agent_instances');
      ids = (r.agent_instances ?? []).filter(i => i.name?.startsWith(OWN)).map(i => i.id);
    }
    const results = await Promise.all(ids.map(id =>
      api.unary(INST + 'DeleteAgentInstance', { agent_instance_id: id }).then(() => 1, () => 0)));
    for (const list of pool.values()) for (let i = list.length - 1; i >= 0; i--)
      if (ids.includes(list[i].id)) list.splice(i, 1);
    return { deleted: results.reduce((a, b) => a + b, 0), stuck: results.length - results.reduce((a, b) => a + b, 0) };
  }

  async function acquire(ns, name) {
    const k = `${ns}/${name}`;
    const list = pool.get(k) ?? [];
    pool.set(k, list);
    // a CRASHED actor can never be assigned again: retire its instance
    for (const i of list.filter(i => !i.busy && actorState.get(`ai-${i.id}`) === 'Crashed')) {
      list.splice(list.indexOf(i), 1);
      api.unary(INST + 'DeleteAgentInstance', { agent_instance_id: i.id }).catch(() => {});
    }
    let inst = list.find(i => !i.busy && i.turns < TURNS_PER_INSTANCE);
    if (!inst) {
      const agent = (await agents()).find(a => a.ns === ns && a.name === name);
      if (!agent) throw new Error(`no Ready AgentTemplate ${k}`);
      const r = await api.unary(INST + 'CreateAgentInstance', {
        harness: { namespace: ns, name: agent.harness }, agent_template: { namespace: ns, name },
        request_id: `${OWN}${name}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`,
        name: `${OWN}${name}` });
      inst = { id: r.agent_instance.id, context_id: r.agent_instance.context_id, turns: 0, busy: false };
      list.push(inst);
    }
    inst.busy = true;
    return inst;
  }
  function release(ns, name, inst) {
    inst.busy = false; inst.turns++;
    if (inst.turns < TURNS_PER_INSTANCE) return;
    const list = pool.get(`${ns}/${name}`);
    list.splice(list.indexOf(inst), 1);
    api.unary(INST + 'DeleteAgentInstance', { agent_instance_id: inst.id }).catch(() => {});
  }

  // Cancel a task that never reached a worker (alpha3 wedge, see header).
  const cancelled = new Set();      // task ids already cancelled
  // Orphans are SUBMITTED (never picked up); RESET also takes WORKING.
  async function cancelStale(inst, olderThanMs = 0, states = /SUBMITTED/) {
    const r = await api.unary(A2A + 'ListTasks', { context_id: inst.context_id, page_size: 20 },
                              onInstance(inst.id)).catch(() => ({}));
    let n = 0;
    for (const t of r.tasks ?? []) {
      if (!states.test(t.status?.state ?? '')) continue;
      const age = Date.now() - new Date(t.status?.timestamp ?? t.metadata?.['kagent.dev/task-created-at'] ?? 0);
      if (age < olderThanMs || cancelled.has(t.id)) continue;
      cancelled.add(t.id);         // one attempt per task; a cancel that doesn't stick won't loop
      if (cancelled.size > 5000) cancelled.delete(cancelled.values().next().value);
      await api.unary(A2A + 'CancelTask', { id: t.id }, onInstance(inst.id)).catch(() => {});
      n++;
    }
    return n;
  }

  // One real turn: restore -> LLM turn -> checkpoint. Resolves to
  // { ok, text, ms, retry } — retry=true means it never got a worker and was
  // cleaned up; the caller should queue it again.
  async function converse(ns, name, prompt) {
    const inst = await acquire(ns, name);
    const t0 = Date.now();
    let text = '', state = '', fail = '';
    try {
      for await (const ev of api.stream(A2A + 'SendStreamingMessage', {
          message: { message_id: `${OWN}${Date.now()}-${Math.floor(Math.random() * 1e6)}`,
                     context_id: inst.context_id, role: 'ROLE_USER', parts: [{ text: prompt }] } },
          { ...onInstance(inst.id), timeout: 300_000 })) {
        if (ev.artifact_update) text += textOf(ev.artifact_update.artifact?.parts);
        const st = ev.status_update?.status ?? ev.task?.status;
        if (st?.state) state = st.state;
        if (st?.state === 'TASK_STATE_FAILED') fail = textOf(st.message?.parts);
        if (ev.task?.artifacts) text = ev.task.artifacts.map(a => textOf(a.parts)).join(' ');
      }
    } catch (e) {
      const timedOut = e instanceof GrpcError && /timed out/i.test(e.message);
      const busy = e instanceof GrpcError && /active task/i.test(e.message);
      // NOT_FOUND / PERMISSION_DENIED: the instance was deleted under us (kagent
      // answers "not authorized" for an instance id that no longer exists,
      // e.g. another Scope run cleaned it up). Drop it; the retry makes a new one.
      if (e instanceof GrpcError && (e.code === 5 || e.code === 7)) {
        const list = pool.get(`${ns}/${name}`) ?? [];
        if (list.includes(inst)) list.splice(list.indexOf(inst), 1);
        return { ok: false, retry: true, ms: Date.now() - t0, text: e.message };
      }
      if (timedOut || busy) {
        await cancelStale(inst, busy ? ORPHAN_MS : 0);
        release(ns, name, inst);
        return { ok: false, retry: true, ms: Date.now() - t0, text: e.message };
      }
      release(ns, name, inst);
      return { ok: false, ms: Date.now() - t0, text: e.message };
    }
    release(ns, name, inst);
    const ms = Date.now() - t0;
    if (state === 'TASK_STATE_COMPLETED') return { ok: true, ms, text: text || '(no text)' };
    return { ok: false, ms, text: [text, fail].filter(Boolean).join(' · ') || state || 'no result' };
  }

  // ── watchdog + reset: free workers pinned by orphaned tasks ────────────────
  // Watchdog: sweep() cancels Scope-owned tasks stuck past ORPHAN_MS.
  // RESET POOL: sweep(0, true) cancels every open task on every instance.
  // The watchdog only tends instances this process created: a leftover from
  // an earlier run can be wedged beyond CancelTask (alpha3: CRASHED actor with
  // a pending lifecycle op), and re-cancelling it forever helps no one.
  async function sweep(olderThanMs = ORPHAN_MS, all = false) {
    const r = await listAll(INST + 'ListAgentInstances', { all_creators: all }, 'agent_instances');
    const mine = new Set([...pool.values()].flat().map(i => i.id));
    let n = 0;
    for (const i of r.agent_instances ?? []) {
      if (!all && !mine.has(i.id)) continue;
      if (all) { n += await cancelStale(i, 0, /SUBMITTED|WORKING/); continue; }
      const st = actorState.get(`ai-${i.id}`);
      n += await cancelStale(i, st === 'Running' ? olderThanMs : ORPHAN_HARD_MS);
    }
    return n;
  }

  // ── chats from the kagent UI (and anyone else) ─────────────────────────────
  // Poll recent non-Scope instances' tasks; report each task's prompt once and
  // its terminal reply once.
  const seen = new Map();          // task id -> last state
  const lastUpdate = new Map();    // instance id -> updated_at last read
  let firstPass = true;            // history from before Scope started is not news
  async function foreignActivity() {
    const out = [];
    const r = await listAll(INST + 'ListAgentInstances', { all_creators: true }, 'agent_instances', 3);
    const recent = (r.agent_instances ?? [])
      .filter(i => !i.name?.startsWith(OWN) && i.state === 'AGENT_INSTANCE_STATE_READY')
      .sort((x, y) => String(y.updated_at).localeCompare(String(x.updated_at))).slice(0, 10);
    for (const i of recent) {
      // re-read an instance while it changes, and for 30s after (a reply can
      // land without bumping updated_at); skip the quiet ones
      const quiet = lastUpdate.get(i.id)?.u === i.updated_at && Date.now() - lastUpdate.get(i.id).t > 30_000;
      if (quiet && !firstPass) continue;
      if (lastUpdate.get(i.id)?.u !== i.updated_at) lastUpdate.set(i.id, { u: i.updated_at, t: Date.now() });
      const t = await api.unary(A2A + 'ListTasks', { context_id: i.context_id, page_size: 10,
        include_artifacts: true, history_length: 4 }, onInstance(i.id)).catch(() => ({}));
      for (const task of t.tasks ?? []) {
        const st = task.status?.state, prev = seen.get(task.id);
        if (prev === st) continue;
        seen.set(task.id, st);
        if (seen.size > 2000) seen.delete(seen.keys().next().value);
        if (firstPass) continue;
        const agent = i.agent_template?.name;
        if (prev === undefined) {
          const um = (task.history ?? []).find(m => m.role === 'ROLE_USER');
          // obo: who this instance acts for — the creator's identity claim
          if (um) out.push({ agent, kind: 'prompt', text: textOf(um.parts).slice(0, 400),
                             via: 'kagent-ui', obo: i.creator });
        }
        if (/COMPLETED|FAILED/.test(st ?? '')) {
          const reply = (task.artifacts ?? []).map(a => textOf(a.parts)).join(' ')
                     || textOf(task.status?.message?.parts);
          out.push({ agent, kind: st.endsWith('FAILED') ? 'error' : 'reply',
                     text: (reply || '(no text)').slice(0, 400), via: 'kagent-ui' });
        }
      }
    }
    firstPass = false;
    return out;
  }

  return { agents, snapshot, converse, sweep, foreignActivity, cleanup };
}
