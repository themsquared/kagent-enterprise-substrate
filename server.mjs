#!/usr/bin/env node
// Substrate Scope — Enterprise edition. A live visualizer for Agent Substrate
// under kagent-enterprise (1.0.0-alpha3 + Substrate 0.2.0-beta5).
// Zero dependencies; needs node >= 18 and kubectl on the PATH.
//
//   node server.mjs                        # simulated feed (built into the page)
//   node server.mjs --live                 # watch KUBE_CONTEXT (or the current context)
//   node server.mjs --live --source crd    # force the kubectl-only adapter
//
// Live-mode source adapters (auto-detected by default):
//   enterprise  Full fidelity. Talks gRPC-Web to the kagent-enterprise
//               controller (lib/enterprise.mjs): Substrate workers and actors
//               straight from ate-api, AgentInstances as the chat sessions,
//               A2A 1.0 turns, and chat ingestion from the kagent UI.
//   crd         Works on ANY substrate cluster: WorkerPools, worker pods, and
//               ActorTemplates via kubectl. No per-actor runtime state.
import { createServer } from 'node:http';
import { readFile, writeFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { execFile, spawn } from 'node:child_process';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { enterprise } from './lib/enterprise.mjs';
import { GrpcError } from './lib/grpcweb.mjs';
import { TOOL_PROMPTS } from './lib/prompts.mjs';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), 'public');
const PKG_VERSION = JSON.parse(readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'package.json'), 'utf8')).version;
const LIVE = process.argv.includes('--live');
const PORT = Number(process.env.PORT || 8123);
const srcIdx = process.argv.indexOf('--source');
const SOURCE = srcIdx > -1 ? process.argv[srcIdx + 1] : 'auto';   // enterprise | crd | auto
let source = SOURCE === 'auto' ? null : SOURCE;
// The controller's gRPC-Web API and the UI (for the bundled autoauth IdP).
// Scope port-forwards both itself unless KAGENT_API is set.
const KAGENT_API = process.env.KAGENT_API || 'http://127.0.0.1:8083';
const UI_PORT = process.env.KAGENT_UI_PORT || 8001;
const KAGENT_UI = process.env.KAGENT_UI || `http://127.0.0.1:${UI_PORT}`;
const ATESPACE = process.env.ATESPACE || 'kagent';
// Pin every kubectl call (and port-forward) to one context so Scope can
// never watch or scale a cluster you didn't point it at. Defaults to the
// current context.
const KUBE_CONTEXT = process.env.KUBE_CONTEXT || '';
const KCTX = KUBE_CONTEXT ? ['--context', KUBE_CONTEXT] : [];

const ent = enterprise({ apiBase: KAGENT_API, uiBase: KAGENT_UI, token: process.env.KAGENT_TOKEN,
                         atespace: ATESPACE });

const clients = new Set();
const send = ev => {
  const line = `data: ${JSON.stringify(ev)}\n\n`;
  for (const res of clients) res.write(line);
};
const readBody = req => new Promise(resolve => {
  let body = '';
  req.on('data', c => body += c);
  req.on('end', () => { try { resolve(JSON.parse(body || '{}')); } catch { resolve(null); } });
});
const json = (res, obj) => { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(obj)); };

const server = createServer(async (req, res) => {
  if (req.url === '/scale' && req.method === 'POST') {
    const b = await readBody(req);
    if (!LIVE) return json(res, { ok: false, error: 'not in --live mode' });
    const pool = b?.pool ? state.pools[b.pool] : Object.values(state.pools)[0];
    if (!pool) return json(res, { ok: false, error: b?.pool ? `no workerpool ${b.pool}` : 'no workerpool seen yet' });
    const replicas = Math.max(1, Math.min(AS.MAX, Number(b?.replicas)));
    if (!replicas) return json(res, { ok: false, error: 'bad request' });
    // the documented ephemeral scaling path: kubectl scale workerpool
    execFile('kubectl', [...KCTX, 'scale', 'workerpools.ate.dev', pool.name,
                         '-n', pool.ns, `--replicas=${replicas}`], { timeout: 10_000 },
      err => json(res, err ? { ok: false, error: String(err.message).slice(0, 200) } : { ok: true, pool: pool.name, replicas }));
    return;
  }
  if (req.url === '/reset' && req.method === 'POST') {
    // frees workers pinned by orphaned tasks (the alpha3 wedge: a send that
    // timed out waiting for a worker leaves its task SUBMITTED and its actor
    // holding a worker). Cancelling the open tasks suspends those actors;
    // snapshots survive.
    // Cancelling is not enough for the other alpha3 wedge: a resume interrupted
    // by a controller restart holds its worker in Resuming forever, and only
    // replacing the worker pod releases it. So reset also restarts each pool's
    // worker Deployment (named after the pool). Snapshots survive both.
    if (!LIVE || source !== 'enterprise') return json(res, { ok: false, error: 'reset needs --live with the enterprise source' });
    admitQ.length = 0; broadcastQueue();
    const cancelled = await ent.sweep(0, true).catch(() => 0);
    const bounced = await Promise.all(Object.values(state.pools).map(p => new Promise(done =>
      execFile('kubectl', [...KCTX, 'rollout', 'restart', `deploy/${p.name}`, '-n', p.ns], { timeout: 15_000 },
        err => done(err ? `${p.name}: ${String(err.message).slice(0, 120)}` : p.name)))));
    return json(res, { ok: true, cancelled, restarted: bounced });
  }
  if (req.url === '/queue' && req.method === 'POST') {
    // external load generators may still report their own client-side waits
    const b = await readBody(req);
    if (!b) { res.writeHead(400); return res.end('{"ok":false}'); }
    extWaiting = (b.waiting ?? []).map(w => w.name ?? w);
    broadcastQueue();
    return json(res, { ok: true });
  }
  if (req.url === '/demo') {
    // master kill switch: stimulate.mjs polls this and stops dispatching
    // real (billable) chats when run=false; surge and the queue respect it too
    if (req.method === 'POST') {
      const b = await readBody(req);
      demoRun = b && 'run' in b ? !!b.run : !demoRun;
      if (!demoRun) { admitQ.splice(0).forEach(j => j.resolve({ ok: false, stopped: true, text: 'demo stopped' })); broadcastQueue(); }
      send({ type: 'demo_state', run: demoRun });
      return json(res, { ok: true, run: demoRun });
    }
    return json(res, { run: demoRun });
  }
  if (req.url === '/activity' && req.method === 'POST') {
    const b = await readBody(req);
    if (!b) { res.writeHead(400); return res.end('{"ok":false}'); }
    recordActivity(b);
    return json(res, { ok: true });
  }
  if (req.url === '/agents') {
    // what stimulate.mjs drives: Ready AgentTemplates, and the live pool size
    if (!LIVE || source !== 'enterprise') return json(res, { agents: [], workers: 0 });
    const list = await ent.agents().catch(() => []);
    return json(res, { agents: list.map(a => `${a.ns}/${a.name}`), workers: (state.lastSnap?.workers ?? []).length,
                       tools: Object.fromEntries(list.map(a => [a.name, a.tools ?? []])) });
  }
  if (req.url === '/chat' && req.method === 'POST') {
    // the drawer's "talk to the agent": one real chat, which restores the
    // actor — activation you can watch happen on the board. Fire-and-forget.
    const b = await readBody(req);
    if (!LIVE || source !== 'enterprise') return json(res, { ok: false, error: 'chat needs --live with the enterprise source' });
    if (!demoRun) return json(res, { ok: false, error: 'demo is stopped (STOP DEMO)' });
    if (!b?.agent || !b?.text) { res.writeHead(400); return res.end('{"ok":false}'); }
    chatWithAgent(b.ns || ATESPACE, b.agent, String(b.text).slice(0, 1000), 'you');
    return json(res, { ok: true });
  }
  if (req.url === '/converse' && req.method === 'POST') {
    // one real chat, answered when the turn finishes (stimulate.mjs uses this)
    const b = await readBody(req);
    if (!LIVE || source !== 'enterprise') return json(res, { ok: false, text: 'needs --live with the enterprise source' });
    if (!demoRun) return json(res, { ok: false, stopped: true, text: 'demo is stopped (STOP DEMO)' });
    if (!b?.agent || !b?.text) { res.writeHead(400); return res.end('{"ok":false}'); }
    return json(res, await chatWithAgent(b.ns || ATESPACE, b.agent, String(b.text).slice(0, 4000), b.via || 'stimulator'));
  }
  if (req.url === '/surge' && req.method === 'POST') {
    if (!LIVE || source !== 'enterprise') return json(res, { ok: false, error: 'surge requires the enterprise source' });
    return json(res, { ok: true, fired: await surge() });
  }
  if (req.url === '/autoscale' && req.method === 'POST') {
    const b = await readBody(req);
    autoscale = b && 'on' in b ? !!b.on : !autoscale;
    for (const st of Object.values(asState)) { st.upStreak = 0; st.downStreak = 0; }
    send({ type: 'autoscale_state', on: autoscale });
    return json(res, { ok: true, on: autoscale });
  }
  if (req.url === '/events') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream',
                         'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    clients.add(res);
    res.write(`data: ${JSON.stringify({ type: 'mode', live: LIVE, source })}\n\n`);
    if (LIVE) replayState(res);
    req.on('close', () => clients.delete(res));
    return;
  }
  const bare = req.url.split('?')[0];          // so /?group=session still serves the board
  const path = bare === '/' ? '/index.html' : bare;
  try {
    const body = await readFile(join(ROOT, path));
    res.writeHead(200, { 'Content-Type':
      path.endsWith('.html') ? 'text/html; charset=utf-8' : 'text/plain; charset=utf-8' });
    res.end(body);
  } catch { res.writeHead(404); res.end('not found'); }
});

// ── live mode: polling ───────────────────────────────────────────────────────
const state = { pools: {}, actors: {} };   // last-seen, for diffing + replay

// ── metrics: kubelet stats (no metrics-server needed) ────────────────────────
// Two things make naive sampling wrong here, both measured on alpha3:
//   * The kubelet refreshes pod stats only every ~12-18s, while a Claude turn
//     is 3-10s. An instantaneous CPU rate misses most turns, so CPU comes from
//     the cumulative usageCoreNanoSeconds counter: delta CPU-seconds over delta
//     stat time counts every CPU-second a turn burned, even between refreshes.
//   * Nothing declares resources (worker pods and ActorTemplates carry no
//     requests), so there is no "unit" to read. The unit is MEASURED: the p95
//     footprint of a worker that ran an agent during a stats window. That is
//     what an always-on agent pod would have to reserve. Until enough samples
//     exist the 50m/128Mi default is used and the point says so.
// The gVisor sandbox is inside the worker pod's cgroup (its memory file shows
// as shmem), so a worker's working set does include the agent.
const metrics = [];              // ring buffer of sample points
const METRICS_MAX = 1200;        // ~1h at 3s
let queuedNow = 0;               // demand waiting for a worker
let nodeNames = null;

const DEFAULT_UNIT = { cpu: 50, mem: 128 };
const unit = { cpu: Number(process.env.UNIT_CPU_M) || DEFAULT_UNIT.cpu,
               mem: Number(process.env.UNIT_MEM_MI) || DEFAULT_UNIT.mem,
               source: process.env.UNIT_CPU_M ? 'env' : 'default' };
const parseCpu = s => !s ? 0 : s.endsWith('m') ? parseInt(s) : parseFloat(s) * 1000;
const parseMem = s => !s ? 0 : s.endsWith('Gi') ? parseFloat(s) * 1024
                            : s.endsWith('Ki') ? parseInt(s) / 1024 : parseInt(s);
function fetchUnit(snap) {          // a declared ActorTemplate limit beats measuring
  if (unit.source === 'env') return;
  const l = (snap.actorTemplates ?? []).find(t => t.limits?.cpu || t.limits?.memory)?.limits;
  if (!l) return;
  if (l.cpu) unit.cpu = parseCpu(l.cpu) || unit.cpu;
  if (l.memory) unit.mem = Math.round(parseMem(l.memory)) || unit.mem;
  unit.source = 'declared';
}
// The measured unit survives restarts (per context), so a demo never opens on
// the placeholder: the chart is honest from its first point.
const UNIT_FILE = join(tmpdir(), `substrate-scope-unit-${(KUBE_CONTEXT || 'current').replace(/[^\w.-]/g, '_')}.json`);
if (unit.source === 'default') {
  try {
    const u = JSON.parse(readFileSync(UNIT_FILE, 'utf8'));
    if (u.cpu > 0 && u.mem > 0) Object.assign(unit, { cpu: u.cpu, mem: u.mem, source: 'measured' });
  } catch {}
}
const podStat = new Map();       // pod -> { t, cpuNs, rate, mem }   (last kubelet stat)
const lastBusyAt = new Map();    // pod -> ms it last held an actor (from 1s snapshots)
const busySamples = { cpu: [], mem: [] };
const p95 = xs => { const v = [...xs].sort((a, b) => a - b); return v[Math.min(v.length - 1, Math.floor(v.length * 0.95))]; };
function noteBusy(snap) {
  const now = Date.now();
  for (const w of snap.workers ?? []) if (w.actorId) lastBusyAt.set(w.workerPod, now);
}

async function sampleMetrics() {
  if (!LIVE) return;
  if (!nodeNames) {
    const n = await kubectl(['get', 'nodes', '-o', 'json']);
    nodeNames = (n?.items ?? []).map(i => i.metadata.name);
    if (!nodeNames.length) { nodeNames = null; return; }
  }
  const snap = state.lastSnap;
  if (!snap) return;
  const workerPods = new Set((snap.workers ?? []).map(w => w.workerPod));
  let wCpu = 0, wMem = 0;
  for (const node of nodeNames) {
    const s = await kubectl(['get', '--raw', `/api/v1/nodes/${node}/proxy/stats/summary`]);
    for (const p of s?.pods ?? []) {
      const pod = p.podRef.name;
      if (!workerPods.has(pod)) continue;
      const t = Date.parse(p.cpu?.time ?? '') || 0;
      const cpuNs = p.cpu?.usageCoreNanoSeconds ?? 0;
      const mem = (p.memory?.workingSetBytes ?? 0) / 1048576;           // MiB
      const prev = podStat.get(pod);
      let rate = prev?.rate ?? ((p.cpu?.usageNanoCores ?? 0) / 1e6);     // mCPU
      if (prev && t > prev.t && cpuNs >= prev.cpuNs) {
        rate = (cpuNs - prev.cpuNs) / ((t - prev.t) * 1e3);             // ns / µs = mCPU
        // a fresh stats window: did this worker run an agent inside it?
        if ((lastBusyAt.get(pod) ?? 0) >= prev.t - 1000) {
          busySamples.cpu.push(rate); busySamples.mem.push(mem);
          for (const k of ['cpu', 'mem']) if (busySamples[k].length > 300) busySamples[k].shift();
        }
      }
      if (!prev || t !== prev.t) podStat.set(pod, { t, cpuNs, rate, mem });
      wCpu += rate; wMem += mem;
    }
  }
  if (unit.source !== 'env' && unit.source !== 'declared' && busySamples.mem.length >= 3) {
    const cpu = Math.max(1, Math.round(p95(busySamples.cpu))), mem = Math.round(p95(busySamples.mem));
    if (cpu !== unit.cpu || mem !== unit.mem)
      writeFile(UNIT_FILE, JSON.stringify({ cpu, mem, samples: busySamples.mem.length, at: new Date().toISOString() })).catch(() => {});
    Object.assign(unit, { cpu, mem, source: 'measured' });
  }
  const agents = (snap.actorTemplates ?? []).length;
  const slots = (snap.workers ?? []).length;
  const active = (snap.workers ?? []).filter(w => w.actorId).length;
  const pools = {};
  for (const name of Object.keys(state.pools)) {
    const ws = (snap.workers ?? []).filter(w => w.workerPool === name);
    pools[name] = { slots: ws.length, active: ws.filter(w => w.actorId).length,
                    queued: admitQ.filter(j => poolOf(j.name) === name).length };
  }
  const point = {
    t: Date.now(), wCpu: Math.round(wCpu), wMem: Math.round(wMem),
    benchCpu: 0, benchMem: 0,       // no always-on agent pods exist on enterprise
    unitCpu: unit.cpu, unitMem: unit.mem, unitSource: unit.source, unitSamples: busySamples.mem.length,
    active, slots, queued: queuedNow, agents, pools,
  };
  metrics.push(point);
  if (metrics.length > METRICS_MAX) metrics.shift();
  send({ type: 'metrics', p: point });
  autoscaleTick(point);
}

// ── autoscaler: demand-driven (queue depth up, idle capacity down) ───────────
// CPU is the wrong signal here: workers are slot-bound, and LLM turns are
// mostly I/O wait. Demand (queued + busy) vs slots is what actually matters.
let autoscale = LIVE && !process.argv.includes('--no-autoscale');
const asState = {};     // pool -> { lastScaleAt, upStreak, downStreak, demandWin }
const asFor = name => (asState[name] ??= { lastScaleAt: 0, upStreak: 0, downStreak: 0, demandWin: [] });
// SCOPE_MAX_WORKERS caps scale-up: each gVisor worker restoring Claude Code
// costs real CPU, and a laptop Docker VM shared with other clusters starves
// (probes time out, ate-api-server restarts) well before 8.
const AS = { MIN: 2, MAX: Number(process.env.SCOPE_MAX_WORKERS) || 8, COOL_UP: 8_000, COOL_DOWN: 20_000,
             UP_TICKS: 2, DOWN_TICKS: 6, WINDOW: 10 };

function scaleTo(pool, n, why) {
  asFor(pool.name).lastScaleAt = Date.now();
  execFile('kubectl', [...KCTX, 'scale', 'workerpools.ate.dev', pool.name, '-n', pool.ns,
                       `--replicas=${n}`], { timeout: 10_000 },
    err => send({ type: 'autoscale', pool: pool.name, replicas: n, why: `${pool.name}: ${why}`, ok: !err }));
}

// Target-based, both directions: scale straight to what demand needs, not ±1.
// Up = current demand (fast, queued work is user-visible latency). Down = the
// PEAK demand over the rolling window (one decisive jump, but a brief dip
// can't slash the pool).
// Each pool scales on its own demand: that is the point of separate pools.
// Never more workers than the pool has agents: an agent runs on one worker at
// a time, and queued TURNS (several can queue per agent) are not agents. A pool
// with more workers than agents would make substrate reserve more than the
// always-on fleet it replaces.
function autoscaleTick(point) {
  if (!autoscale) return;
  for (const pool of Object.values(state.pools)) {
    const p = point.pools?.[pool.name];
    if (!p) continue;
    const members = Object.values(state.lastSnap?.agentPools ?? {}).filter(x => x === pool.name).length;
    const hi = Math.min(AS.MAX, members || AS.MAX), lo = Math.min(AS.MIN, hi);
    const clamp = n => Math.min(hi, Math.max(lo, n));
    const st = asFor(pool.name);
    const demand = p.active + p.queued;
    st.demandWin.push(demand);
    if (st.demandWin.length > AS.WINDOW) st.demandWin.shift();
    const upTarget = clamp(demand);
    const downTarget = clamp(Math.max(...st.demandWin));
    if (p.queued > 0) { st.upStreak++; st.downStreak = 0; }
    else if (downTarget < p.slots) { st.downStreak++; st.upStreak = 0; }
    else { st.upStreak = 0; st.downStreak = 0; }
    const now = Date.now();
    if (st.upStreak >= AS.UP_TICKS && upTarget > p.slots && now - st.lastScaleAt > AS.COOL_UP) {
      st.upStreak = 0;
      scaleTo(pool, upTarget, `demand ${demand} (${p.queued} queued)`);
    } else if (st.downStreak >= AS.DOWN_TICKS && downTarget < p.slots
               && now - st.lastScaleAt > AS.COOL_DOWN) {
      st.downStreak = 0;
      scaleTo(pool, downTarget, `peak demand ${Math.max(...st.demandWin)} over 30s`);
    }
  }
}

// ── per-agent activity: prompts, replies, latency — the "click into an agent"
//    stream. Chat I/O IS the agent's observable output (in-sandbox stdout is
//    not reachable: substrate exposes network ingress only).
const activity = [];
const ACT_MAX = 400;
function recordActivity(ev) {
  const e = { t: Date.now(), ...ev };
  activity.push(e);
  if (activity.length > ACT_MAX) activity.shift();
  send({ type: 'activity', e });
}

// ── ingest kagent-UI chats (anything not sent by Scope) ──────────────────────
async function pollForeign() {
  try { for (const e of await ent.foreignActivity()) recordActivity(e); } catch {}
}

// ── stuck-resume watchdog ────────────────────────────────────────────────────
// A restore normally takes about a second. An actor still Resuming on the same
// worker after RESUME_STUCK_MS is the alpha3 wedge (its resume was cut off,
// e.g. by a controller restart) and it holds that worker until the pod goes.
// Replace just that pod; the WorkerPool brings a fresh one, the actor is
// released, and Scope retires its session as Crashed.
const RESUME_STUCK_MS = Number(process.env.RESUME_STUCK_MS) || 90_000;
const resumingSince = new Map();   // "actor@pod" -> first seen Resuming
const bouncedAt = new Map();       // pod -> ms we last replaced it
function watchResumes(snap) {
  const now = Date.now(), seen = new Set();
  for (const a of snap.actors ?? []) {
    if (a.status !== 'Resuming' || !a.workerPod) continue;
    const k = `${a.actorId}@${a.workerPod}`;
    seen.add(k);
    if (!resumingSince.has(k)) resumingSince.set(k, now);
    const stuck = now - resumingSince.get(k);
    if (stuck < RESUME_STUCK_MS || now - (bouncedAt.get(a.workerPod) ?? 0) < 120_000) continue;
    bouncedAt.set(a.workerPod, now);
    const ns = (snap.workers ?? []).find(w => w.workerPod === a.workerPod)?.workerNamespace ?? ATESPACE;
    console.log(`watchdog: ${a.actorTemplateName} stuck resuming on ${a.workerPod} for ${Math.round(stuck / 1000)}s; replacing that worker pod`);
    execFile('kubectl', [...KCTX, 'delete', 'pod', a.workerPod, '-n', ns, '--wait=false'], { timeout: 15_000 }, () => {});
    recordActivity({ agent: a.actorTemplateName, kind: 'error', via: 'watchdog',
                     text: `stuck resuming ${Math.round(stuck / 1000)}s on ${a.workerPod}: worker replaced` });
  }
  for (const k of resumingSince.keys()) if (!seen.has(k)) resumingSince.delete(k);
}

// ── admission queue ──────────────────────────────────────────────────────────
// alpha3 does not fast-reject when the pool is full: an oversubscribed send
// times out and orphans its task (see lib/enterprise.mjs). So Scope admits a
// turn only when a worker is free, and everything else waits here, visibly,
// in the queue lane. This queue depth is also the autoscaler's demand signal.
const admitQ = [];          // { ns, name, prompt, via, t0, resolve }
const inFlight = {};        // pool -> Scope turns dispatched and not yet finished
let extWaiting = [];        // names reported by external load generators
function broadcastQueue() {
  const names = [...new Set([...admitQ.map(j => j.name), ...extWaiting])];
  queuedNow = admitQ.length + extWaiting.length;
  send({ type: 'queue', waiting: names.map(name => ({ name })) });
}
// an agent's pool, from the Harness it runs on (first pool if unknown)
const poolOf = name => state.lastSnap?.agentPools?.[name] ?? Object.keys(state.pools)[0];
function freeWorkers(pool) {
  const ws = (state.lastSnap?.workers ?? []).filter(w => w.workerPool === pool);
  const busy = ws.filter(w => w.actorId).length;
  return ws.length - Math.max(busy, inFlight[pool] ?? 0);
}
// FIFO within a pool, independent across pools: a backlog on one pool never
// blocks a turn whose own pool has a free worker.
function pump() {
  let changed = false;
  for (let i = 0; demoRun && i < admitQ.length;) {
    const pool = poolOf(admitQ[i].name);
    if (freeWorkers(pool) <= 0) { i++; continue; }
    const [job] = admitQ.splice(i, 1); changed = true;
    inFlight[pool] = (inFlight[pool] ?? 0) + 1;
    ent.converse(job.ns, job.name, job.prompt).then(r => {
      inFlight[pool]--;
      // never reached a worker (or its session vanished): back to the front,
      // a bounded number of times so a real permission error still surfaces
      if (r.retry && demoRun && Date.now() - job.t0 < 300_000 && (job.tries = (job.tries ?? 0) + 1) <= 3) {
        admitQ.unshift(job);
        return;
      }
      const ms = Date.now() - job.t0;
      recordActivity({ agent: job.name, kind: r.ok ? 'reply' : 'error',
                       text: String(r.text).slice(0, 400), ms, via: job.via });
      job.resolve({ ...r, ms });
    }).catch(e => {
      inFlight[pool]--;
      recordActivity({ agent: job.name, kind: 'error', text: String(e.message).slice(0, 200), via: job.via });
      job.resolve({ ok: false, text: e.message });
    }).finally(() => { broadcastQueue(); pump(); });
  }
  if (changed) broadcastQueue();
}

let demoRun = true;   // master switch for anything that costs LLM tokens

// One real chat to one agent: restore → LLM turn → checkpoint. Used by surge,
// the drawer's "talk to the agent" box, and stimulate.mjs via /converse.
function chatWithAgent(ns, name, prompt, via) {
  recordActivity({ agent: name, kind: 'prompt', text: prompt.slice(0, 400), via });
  return new Promise(resolve => {
    admitQ.push({ ns, name, prompt, via, t0: Date.now(), resolve });
    broadcastQueue();
    pump();
  });
}

// ── surge: a burst of real chats across every Ready agent ────────────────────
async function surge() {
  if (!demoRun) return 0;
  const list = await ent.agents().catch(() => []);
  for (const a of list) {
    const srv = a.tools?.[Math.floor(Math.random() * a.tools.length)];
    chatWithAgent(a.ns, a.name, srv && TOOL_PROMPTS[srv]
      ? `${TOOL_PROMPTS[srv][0]} Then, in about 80 words, what would you do first in a production incident?`
      : 'Explain in about 120 words what you would do first in a production incident.', 'surge');
  }
  return list.length;
}

function kubectl(args) {
  return new Promise(resolve => {
    execFile('kubectl', [...KCTX, ...args], { timeout: 10_000, maxBuffer: 32 << 20 }, (err, stdout) => {
      if (err) return resolve(null);
      try { resolve(JSON.parse(stdout)); } catch { resolve(null); }
    });
  });
}

// ── versions: what this board is actually running against ────────────────────
// The controller's own GetVersion says "dev" in alpha3, so read what the
// cluster declares: the chart version label, the ateapi image tag, the
// Harness image (by the scope.demo/image-tag annotation; it is pinned by
// digest), and the apiserver.
let versions = null;
async function fetchVersions() {
  const [ctl, api, hs, k8s] = await Promise.all([
    kubectl(['get', 'deploy', 'kagent-controller', '-n', 'kagent', '-o', 'json']),
    kubectl(['get', 'deploy', 'ate-api-server', '-n', 'ate-system', '-o', 'json']),
    kubectl(['get', 'harnesses.kagent.dev', '-n', ATESPACE, '-o', 'json']),
    kubectl(['get', '--raw', '/version']),
  ]);
  const tagOf = img => (img ?? '').match(/:([\w.-]+)$/)?.[1];
  const harness = [...new Set((hs?.items ?? []).map(h =>
    h.metadata?.annotations?.['scope.demo/image-tag']
    ?? (h.spec?.workload?.image ?? '').replace(/^.*\//, '').replace(/@sha256:(\w{7}).*/, '@$1')))].filter(Boolean);
  const v = {
    kagent: ctl?.metadata?.labels?.['app.kubernetes.io/version'] ?? null,
    substrate: tagOf(api?.spec?.template?.spec?.containers?.[0]?.image)?.replace(/^v/, '') ?? null,
    harness, kubernetes: k8s?.gitVersion ?? null, scope: PKG_VERSION,
  };
  if (JSON.stringify(v) !== JSON.stringify(versions)) { versions = v; send({ type: 'versions', ...v }); }
}

// ── MCP tool servers: what each agent can call, for chips and the drawer ─────
let toolServers = null;
async function fetchToolServers() {
  const r = await kubectl(['get', 'remotemcpservers.kagent.dev', '-n', ATESPACE, '-o', 'json']);
  if (!r) return;
  const v = Object.fromEntries((r.items ?? []).map(x => {
    const d = x.spec?.description ?? '';
    const icon = [...d][0] && /\p{Extended_Pictographic}/u.test([...d][0]) ? [...d][0] : '🔧';
    return [x.metadata.name, { icon, description: d.replace(/^\S+\s+/, ''),
                               tools: (x.status?.discoveredTools ?? []).map(t => t.name) }];
  }));
  if (JSON.stringify(v) !== JSON.stringify(toolServers)) { toolServers = v; send({ type: 'tool_servers', servers: v }); }
}

function replayState(res) {
  if (toolServers) res.write(`data: ${JSON.stringify({ type: 'tool_servers', servers: toolServers })}\n\n`);
  if (versions) res.write(`data: ${JSON.stringify({ type: 'versions', ...versions })}\n\n`);
  res.write(`data: ${JSON.stringify({ type: 'demo_state', run: demoRun })}\n\n`);
  if (activity.length)
    res.write(`data: ${JSON.stringify({ type: 'activity_history', items: activity.slice(-200) })}\n\n`);
  if (metrics.length)
    res.write(`data: ${JSON.stringify({ type: 'metrics_history', points: metrics })}\n\n`);
  res.write(`data: ${JSON.stringify({ type: 'autoscale_state', on: autoscale })}\n\n`);
  if (state.lastSnap) { res.write(`data: ${JSON.stringify(state.lastSnap)}\n\n`); return; }
  for (const p of Object.values(state.pools))
    res.write(`data: ${JSON.stringify(p)}\n\n`);
}

// crd adapter: synthesize the same snapshot shape from what any substrate
// cluster exposes to kubectl. Worker occupancy and per-session actor state
// are not knowable here (they live in ate-api); templates render as agents
// with their golden-snapshot phase.
async function crdStatus() {
  const pools = await kubectl(['get', 'workerpools.ate.dev', '-A', '-o', 'json']);
  if (!pools) return null;
  const workerPools = [], workers = [];
  for (const item of pools.items ?? []) {
    const ns = item.metadata.namespace, name = item.metadata.name;
    workerPools.push({ namespace: ns, name, replicas: item.spec?.replicas ?? 0 });
    const sel = item.status?.selector || `ate.dev/worker-pool=${name}`;
    const pods = await kubectl(['get', 'pods', '-n', ns, '-l', sel, '-o', 'json']);
    for (const p of pods?.items ?? [])
      if (p.status?.phase === 'Running')
        workers.push({ workerNamespace: ns, workerPool: name, workerPod: p.metadata.name });
  }
  const actorTemplates = [], actors = [];
  const tpls = await kubectl(['get', 'actortemplates.ate.dev', '-A', '-o', 'json']);
  for (const t of tpls?.items ?? []) {
    const ns = t.metadata.namespace, name = t.metadata.name;
    const phase = t.status?.phase ?? 'Unknown';
    actorTemplates.push({ namespace: ns, name, phase });
    actors.push({ actorId: `tpl-${ns}-${name}`, status: phase === 'Ready' ? 'Suspended' : phase,
                  actorTemplateNamespace: ns, actorTemplateName: name });
  }
  return { enabled: true, workerPools, actorTemplates, actors, workers };
}

let pollTick = 0, lastErr = '', pollFails = 0;
const forwards = new Set();      // our kubectl port-forward children
let shuttingDown = false;
process.on('exit', () => { shuttingDown = true; for (const c of forwards) c.kill(); });
function restartForwards(why) {
  if (!forwards.size) return;
  console.log(`controller unreachable (${why}); restarting port-forwards`);
  for (const c of forwards) c.kill('SIGKILL');   // a hung kubectl may ignore SIGTERM; each respawns on exit
}
async function poll() {
  pollTick++;
  let snap = null;
  if (source === 'enterprise') {
    try { snap = await ent.snapshot(); lastErr = ''; pollFails = 0; }
    catch (e) {
      const m = String(e.message).slice(0, 160);
      if (m !== lastErr) console.log(`enterprise poll: ${m}`);
      lastErr = m;
      // transport failure, not a gRPC status: the forward is likely dead.
      // (A fetch timeout is a DOMException, which has a numeric .code too.)
      if (!(e instanceof GrpcError) && ++pollFails === 5) restartForwards(m);
      if (pollFails > 30) pollFails = 0;   // try again every ~30s while down
    }
  }
  if (!snap && source === 'crd' && pollTick % 3 === 1) snap = await crdStatus();
  if (!snap) return;
  for (const p of snap.workerPools ?? [])
    state.pools[p.name] = { type: 'pool', name: p.name, ns: p.namespace, replicas: p.replicas };
  if (pollTick % 20 === 1) fetchUnit(snap);
  state.lastSnap = { type: 'snapshot', ...snap };
  noteBusy(snap);
  watchResumes(snap);
  send(state.lastSnap);
  pump();                           // a worker may have just freed up
}

async function detectSource() {
  if (source) return;
  const svc = await kubectl(['get', 'svc', 'kagent-controller', '-n', 'kagent', '-o', 'json']);
  const enterpriseCtl = (svc?.spec?.ports ?? []).some(p => p.name === 'enterprise');
  source = enterpriseCtl ? 'enterprise' : 'crd';
}

server.listen(PORT, async () => {
  console.log(`Substrate Scope (enterprise) → http://localhost:${PORT}  (${LIVE ? `LIVE, context ${KUBE_CONTEXT || '(current)'}` : 'simulated'})`);
  if (!LIVE) return;
  await detectSource();
  console.log(`source: ${source}${source === 'crd'
    ? ' (kubectl-only: no kagent-enterprise controller found; per-actor state needs the enterprise source)' : ''}`);
  if (source === 'enterprise' && !process.env.KAGENT_API) {
    // A port-forward pins one pod: when that pod's container restarts, kubectl
    // keeps the port open and forwards to nothing. Scope owns its forwards —
    // restarts them when the controller stops answering (see poll), kills
    // them on exit (orphans used to hold the ports and break the next run),
    // and says so plainly when someone else holds the port.
    const forward = (svc, ports) => {
      if (shuttingDown) return;
      const c = spawn('kubectl', [...KCTX, 'port-forward', '-n', 'kagent', svc, ports],
                      { stdio: ['ignore', 'ignore', 'pipe'] });
      forwards.add(c);
      c.stderr.on('data', d => {
        if (/address already in use/i.test(String(d))) {
          const port = ports.split(':')[0];
          console.log(`port ${port} is held by another process (a stale port-forward?): lsof -ti :${port} | xargs kill`);
        }
      });
      c.on('exit', () => { forwards.delete(c); setTimeout(() => forward(svc, ports), 2000); }); // survive pod restarts
    };
    forward('svc/kagent-controller', '8083:8083');
    forward('svc/kagent-ui', `${UI_PORT}:8080`);
    console.log(`kagent UI → http://localhost:${UI_PORT}  (chat with agents to light up the board)`);
  }
  setTimeout(() => { poll(); setInterval(poll, 1000); }, 2500);  // fast enough for short turns
  fetchVersions(); setInterval(fetchVersions, 60_000);
  fetchToolServers(); setInterval(fetchToolServers, 30_000);
  setTimeout(() => { sampleMetrics(); setInterval(sampleMetrics, 3000); }, 5000);
  if (source === 'enterprise') {
    // clear sessions a previous Scope run left behind (a wedged one can refuse)
    setTimeout(() => ent.cleanup({ leftovers: true }).then(r => (r.deleted || r.stuck) && console.log(
      `cleanup: deleted ${r.deleted} leftover Scope session${r.deleted === 1 ? '' : 's'}`
      + (r.stuck ? `; ${r.stuck} wedged (alpha3 pending lifecycle op) left in place` : ''))).catch(() => {}), 3000);
    let closing = false;
    const bye = () => {
      if (closing) process.exit(0);          // second Ctrl-C: leave now
      closing = true;
      console.log('\ndeleting this run\'s agent sessions… (Ctrl-C again to skip)');
      ent.cleanup().catch(() => {}).finally(() => process.exit(0));
      setTimeout(() => process.exit(0), 8000).unref();
    };
    process.on('SIGINT', bye); process.on('SIGTERM', bye);
    setTimeout(() => { pollForeign(); setInterval(pollForeign, 2500); }, 4000);
    // watchdog: cancel Scope tasks orphaned past the threshold (alpha3 wedge)
    setInterval(() => ent.sweep().then(n => n && console.log(
      `watchdog: cancelled ${n} orphaned task${n > 1 ? 's' : ''} (pinned worker freed)`)).catch(() => {}), 15_000);
  }
});
