#!/usr/bin/env node
// The doorbell: the one always-on piece of the Telegram demo, and deliberately
// dumb. Telegram POSTs each message sent to the bot here (through the
// Cloudflare tunnel); the doorbell hands it to the agent over kagent's A2A API
// and is done. Substrate restores the agent, the agent answers on Telegram
// with its own bot (telegram/mcp.mjs), and the agent suspends again.
//
// It holds no bot token and no model key, and it never replies to Telegram
// itself. What it does hold:
//   - TELEGRAM_WEBHOOK_SECRET: Telegram echoes it on every delivery, so a
//     request without it (anyone else who finds the public URL) is refused.
//   - TELEGRAM_ALLOWED_CHATS: only these chats reach the agent. A message
//     from any other chat is logged with its chat id, for the allowlist.
//
// One kagent AgentInstance (a Substrate actor) per Telegram chat, named
// telegram-<agent>-<chat>: the conversation, and the agent's memory of it,
// survive between messages as a snapshot. "/new" starts a fresh one.
//
// :8080 is the webhook (the only port the tunnel reaches). :8081 is admin,
// cluster-internal: POST /register asks the agent to register its webhook.
import { createServer } from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import { client, GrpcError } from './lib/grpcweb.mjs';
import { tokenSource } from './lib/enterprise.mjs';

const env = process.env;
const NS = env.ATESPACE || 'kagent';
const AGENT = env.TELEGRAM_AGENT || 'sre-oncall';
const PUBLIC_URL = (env.PUBLIC_URL || '').replace(/\/$/, '');
const SECRET = env.TELEGRAM_WEBHOOK_SECRET || '';
const ALLOWED = new Set((env.TELEGRAM_ALLOWED_CHATS || '').split(',').map(s => s.trim()).filter(Boolean));
const tokens = tokenSource({ uiBase: env.KAGENT_UI || 'http://kagent-ui.kagent.svc:8080', token: env.KAGENT_TOKEN });
const c = client(env.KAGENT_API || 'http://kagent-controller.kagent.svc:8083', tokens);
const INST = 'kagent.api.v1alpha1.AgentInstanceService/';
const A2A = 'lf.a2a.v1.A2AService/';
const HDR = id => ({ headers: { 'x-kagent-agent-instance-id': id } });
const sleep = ms => new Promise(r => setTimeout(r, ms));
const rid = p => `${p}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
const log = o => console.log(JSON.stringify({ t: new Date().toISOString(), ...o }));
if (!SECRET) { console.error('TELEGRAM_WEBHOOK_SECRET is required'); process.exit(1); }
// PID 1 in the container: without this, a rollout waits out the 30s grace period
// while the old pod keeps serving with the old config.
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => process.exit(0));

// Re-mint the controller token once when it is rejected (a controller restart
// rotates the bundled IdP's key; see lib/enterprise.mjs).
const REJECT = e => e instanceof GrpcError && (e.code === 16 || (e.code === 7 && /authori[sz]ed|credential/i.test(e.message)));
async function unary(m, req, opts) {
  try { return await c.unary(m, req, opts); }
  catch (e) { if (!REJECT(e)) throw e; tokens.invalidate(); return c.unary(m, req, opts); }
}

async function harnessOf(agent) {
  const r = await unary('kagent.api.v1alpha1.AgentTemplateService/ListAgentTemplates', {});
  const t = (r.agent_templates ?? []).find(t => t.ref?.name === agent && (t.ref?.namespace ?? NS) === NS);
  const h = t?.resource?.value?.status?.harnesses?.[0]?.harness;
  if (!h) throw new Error(`agent ${NS}/${agent} has no Ready harness`);
  return h;
}

// chat -> its session. Looked up by name, so a doorbell restart keeps chats.
const sessions = new Map();
async function sessionFor(chat, { fresh = false } = {}) {
  const name = `telegram-${AGENT}-${chat}`;
  if (!fresh) {
    if (sessions.has(chat)) return sessions.get(chat);
    const r = await unary(INST + 'ListAgentInstances', { all_creators: true, page: { limit: 100 } });
    const mine = (r.agent_instances ?? []).filter(i => i.name === name && i.state === 'AGENT_INSTANCE_STATE_READY')
      .sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)))[0];
    if (mine) { sessions.set(chat, mine); return mine; }
  }
  const r = await unary(INST + 'CreateAgentInstance', { harness: { namespace: NS, name: await harnessOf(AGENT) },
    agent_template: { namespace: NS, name: AGENT }, request_id: rid('telegram'), name });
  sessions.set(chat, r.agent_instance);
  log({ event: 'session', chat, instance: r.agent_instance.id, fresh });
  return r.agent_instance;
}

// One turn. alpha3/alpha4 time out instead of queueing when the pool is full,
// leaving the task SUBMITTED: cancel it and try again (as seed-demo.mjs does).
async function turn(inst, text) {
  const t0 = Date.now();
  for (let tries = 0; tries < 12; tries++) {
    let state = '', reply = '';
    try {
      for await (const ev of c.stream(A2A + 'SendStreamingMessage', {
          message: { message_id: rid('telegram'), context_id: inst.context_id, role: 'ROLE_USER', parts: [{ text }] } },
          { ...HDR(inst.id), timeout: 240_000 })) {
        if (ev.artifact_update) reply += (ev.artifact_update.artifact?.parts ?? []).map(p => p.text ?? '').join('');
        state = ev.status_update?.status?.state ?? ev.task?.status?.state ?? state;
        if (state === 'TASK_STATE_FAILED') reply ||= (ev.status_update?.status?.message?.parts ?? []).map(p => p.text ?? '').join('');
      }
      return { state, reply, ms: Date.now() - t0 };
    } catch (e) {
      if (REJECT(e)) { tokens.invalidate(); continue; }
      if (!(e instanceof GrpcError && /timed out|active task/i.test(e.message))) throw e;
      const t = await unary(A2A + 'ListTasks', { context_id: inst.context_id, page_size: 10 }, HDR(inst.id)).catch(() => ({}));
      for (const x of t.tasks ?? []) if (/SUBMITTED/.test(x.status?.state ?? ''))
        await unary(A2A + 'CancelTask', { id: x.id }, HDR(inst.id)).catch(() => {});
      await sleep(4000);
    }
  }
  throw new Error('no free worker after 12 tries');
}

// Messages from one chat run one at a time (a session takes one task at once).
const queues = new Map();
function enqueue(chat, job) {
  const next = (queues.get(chat) ?? Promise.resolve()).then(job).catch(e => log({ chat, error: e.message }));
  queues.set(chat, next);
}

const seenUpdates = new Set();
function onUpdate(u) {
  if (seenUpdates.has(u.update_id)) return;               // Telegram retries deliveries
  seenUpdates.add(u.update_id);
  if (seenUpdates.size > 1000) seenUpdates.delete(seenUpdates.values().next().value);
  const m = u.message;
  if (!m?.chat) return;
  const chat = String(m.chat.id);
  const who = [m.from?.first_name, m.from?.username && `@${m.from.username}`].filter(Boolean).join(' ') || 'unknown';
  if (!ALLOWED.has(chat)) {
    log({ event: 'ignored', chat, from: who, hint: 'add this chat id to TELEGRAM_ALLOWED_CHATS to let it reach the agent' });
    return;
  }
  const body = m.text ?? m.caption ?? `(sent a ${Object.keys(m).find(k => ['photo', 'sticker', 'voice', 'video', 'document'].includes(k)) ?? 'message'} with no text; you can read text only)`;
  const fresh = /^\/new\b/.test(body);
  const text = fresh ? `[telegram chat_id=${chat} from=${who}] (They started a new conversation. Greet them in one line.)`
                     : `[telegram chat_id=${chat} from=${who}] ${body}`;
  log({ event: 'ring', chat, from: who, chars: body.length });
  enqueue(chat, async () => {
    const inst = await sessionFor(chat, { fresh });
    const r = await turn(inst, text);
    log({ event: 'done', chat, instance: inst.id, state: r.state, ms: r.ms, reply: r.reply.slice(0, 160) });
  });
}

const secretOk = h => {
  const a = Buffer.from(String(h ?? '')), b = Buffer.from(SECRET);
  return a.length === b.length && timingSafeEqual(a, b);
};
const readBody = req => new Promise((resolve, reject) => {
  let b = '';
  req.on('data', c => { b += c; if (b.length > 1e6) req.destroy(); });
  req.on('end', () => resolve(b)); req.on('error', reject);
});

// :8080, public through the tunnel: the webhook and nothing else
createServer(async (req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (req.method !== 'POST' || req.url !== '/telegram') { res.writeHead(404); return res.end(); }
  if (!secretOk(req.headers['x-telegram-bot-api-secret-token'])) {
    log({ event: 'refused', why: 'bad or missing webhook secret' });
    res.writeHead(401); return res.end();
  }
  let u;
  try { u = JSON.parse(await readBody(req)); } catch { res.writeHead(400); return res.end(); }
  res.writeHead(200); res.end();                         // ack first: the agent's turn can take seconds
  onUpdate(u);
}).listen(8080, () => log({ event: 'listening', webhook: `${PUBLIC_URL || '(PUBLIC_URL unset)'}/telegram`, agent: `${NS}/${AGENT}`,
                           allowed: [...ALLOWED] }));

// :8081, cluster-internal (the tunnel does not route here)
createServer(async (req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (req.method === 'POST' && req.url === '/register') {
    if (!PUBLIC_URL) { res.writeHead(400); return res.end('PUBLIC_URL is not set\n'); }
    try {
      // a throwaway session: registration is not part of any chat
      const r = await unary(INST + 'CreateAgentInstance', { harness: { namespace: NS, name: await harnessOf(AGENT) },
        agent_template: { namespace: NS, name: AGENT }, request_id: rid('telegram-setup'), name: `telegram-setup-${AGENT}` });
      const t = await turn(r.agent_instance, `[setup] Register your Telegram webhook: call the telegram set_webhook tool with url=${PUBLIC_URL}/telegram, then call get_webhook_info and report both results in one line.`);
      unary(INST + 'DeleteAgentInstance', { agent_instance_id: r.agent_instance.id }).catch(() => {});
      log({ event: 'register', state: t.state, reply: t.reply.slice(0, 200) });
      res.writeHead(t.state === 'TASK_STATE_COMPLETED' ? 200 : 502); return res.end(`${t.state}: ${t.reply}\n`);
    } catch (e) { res.writeHead(500); return res.end(`${e.message}\n`); }
  }
  res.writeHead(404); res.end();
}).listen(8081);
