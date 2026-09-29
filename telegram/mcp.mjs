#!/usr/bin/env node
// The Telegram tools the agent uses to talk on its own bot: send_message,
// send_typing, set_webhook, get_webhook_info.
//
// This server holds NO bot token. The agent's RemoteMCPServer binding
// (headersFrom -> Secret telegram-bot) makes Substrate's egress gateway add the
// token as the x-telegram-bot-token header on each call the agent makes, so the
// token never enters the agent's sandbox and never rests here. A caller without
// that binding arrives with no header (or kagent's inert placeholder) and gets
// an error: only an agent bound to the bot can use it.
//
// Same transport as mcp/server.mjs: streamable HTTP, JSON responses, zero deps.
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';

const PORT = Number(process.env.PORT || 8080);
const API = (process.env.TELEGRAM_API || 'https://api.telegram.org').replace(/\/$/, '');
const HEADER = 'x-telegram-bot-token';
const PLACEHOLDER = 'kagent-credential-injected';   // kagent's stand-in before egress injects
// Defense in depth: even a bound agent can only message these chats (none, if unset).
const ALLOWED = new Set((process.env.TELEGRAM_ALLOWED_CHATS || '').split(',').map(s => s.trim()).filter(Boolean));
const WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
const text = t => ({ content: [{ type: 'text', text: t }] });
// PID 1 in the container: without this, a rollout waits out the 30s grace period
// while the old pod keeps serving with the old config.
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => process.exit(0));
const fail = t => ({ ...text(t), isError: true });

// One Bot API call. The token is only in the URL path, which is never logged
// or returned: errors carry Telegram's description, not the request.
async function bot(token, method, body) {
  let r;
  try {
    r = await fetch(`${API}/bot${token}/${method}`, { method: 'POST', headers: { 'content-type': 'application/json' },
                    body: JSON.stringify(body), signal: AbortSignal.timeout(10_000) });
  } catch (e) { throw new Error(`Telegram unreachable (${e.name})`); }
  const j = await r.json().catch(() => ({}));
  if (!j.ok) throw new Error(`Telegram ${method}: ${j.description || `HTTP ${r.status}`}`);
  return j.result;
}
const chatAllowed = id => ALLOWED.has(String(id));      // empty allowlist: nobody

const TOOLS = {
  send_message: {
    description: 'Send a text message to a Telegram chat from your bot. This is the only way the person on Telegram sees your reply.',
    schema: { type: 'object', properties: {
      chat_id: { type: ['integer', 'string'], description: 'The chat_id from the [telegram chat_id=...] tag' },
      text: { type: 'string', description: 'Plain text, at most 4096 characters' } }, required: ['chat_id', 'text'] },
    run: async (token, { chat_id, text: t }) => {
      if (!chatAllowed(chat_id)) return fail(`chat ${chat_id} is not on this bot's allowlist`);
      const m = await bot(token, 'sendMessage', { chat_id, text: String(t).slice(0, 4096) });
      return text(`sent (message_id ${m.message_id})`);
    },
  },
  send_typing: {
    description: 'Show "typing…" in a Telegram chat while you work on the answer (lasts about 5 seconds).',
    schema: { type: 'object', properties: { chat_id: { type: ['integer', 'string'] } }, required: ['chat_id'] },
    run: async (token, { chat_id }) => {
      if (!chatAllowed(chat_id)) return fail(`chat ${chat_id} is not on this bot's allowlist`);
      await bot(token, 'sendChatAction', { chat_id, action: 'typing' });
      return text('typing shown');
    },
  },
  set_webhook: {
    description: 'Register the HTTPS URL where Telegram delivers messages sent to your bot.',
    schema: { type: 'object', properties: { url: { type: 'string', description: 'https://.../telegram' } }, required: ['url'] },
    run: async (token, { url }) => {
      if (!/^https:\/\/[^/\s]+\/\S*$/.test(url)) return fail('url must be an absolute https:// URL');
      // the shared secret Telegram echoes back, so the doorbell can tell real
      // deliveries from anyone else who finds the public URL
      await bot(token, 'setWebhook', { url, allowed_updates: ['message'], ...(WEBHOOK_SECRET && { secret_token: WEBHOOK_SECRET }) });
      return text(`webhook registered: ${new URL(url).host}`);
    },
  },
  get_webhook_info: {
    description: "Show where Telegram delivers this bot's messages, how many are pending, and the last delivery error.",
    schema: { type: 'object', properties: {} },
    run: async token => {
      const w = await bot(token, 'getWebhookInfo', {});
      return text(`url: ${w.url ? new URL(w.url).host : '(none)'} · pending: ${w.pending_update_count ?? 0}` +
                  (w.last_error_message ? ` · last error: ${w.last_error_message}` : ''));
    },
  },
};

async function handle(msg, token) {
  const { id, method, params } = msg;
  const ok = result => ({ jsonrpc: '2.0', id, result });
  const err = (code, message) => ({ jsonrpc: '2.0', id, error: { code, message } });
  switch (method) {
    case 'initialize':
      return ok({ protocolVersion: params?.protocolVersion || '2025-06-18', capabilities: { tools: { listChanged: false } },
                  serverInfo: { name: 'telegram', title: 'Telegram bot', version: '1.0.0' } });
    case 'ping': return ok({});
    case 'tools/list':      // discovery needs no credential
      return ok({ tools: Object.entries(TOOLS).map(([name, t]) => ({ name, description: t.description, inputSchema: t.schema })) });
    case 'tools/call': {
      const t = TOOLS[params?.name];
      if (!t) return err(-32602, `unknown tool ${params?.name}`);
      const args = params.arguments ?? {};
      if (!token) {
        console.log(JSON.stringify({ t: new Date().toISOString(), tool: params.name, denied: 'no bot credential' }));
        return ok(fail('no bot credential on this call: this agent is not bound to the Telegram bot'));
      }
      try {
        const out = await t.run(token, args);
        console.log(JSON.stringify({ t: new Date().toISOString(), tool: params.name, chat_id: args.chat_id, ok: !out.isError }));
        return ok(out);
      } catch (e) {
        console.log(JSON.stringify({ t: new Date().toISOString(), tool: params.name, chat_id: args.chat_id, error: e.message }));
        return ok(fail(e.message));
      }
    }
    default:
      return id === undefined ? null : err(-32601, `method not found: ${method}`);
  }
}

createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (req.method === 'DELETE') { res.writeHead(200); return res.end(); }
  if (req.method !== 'POST') { res.writeHead(405, { Allow: 'POST' }); return res.end(); }
  const raw = String(req.headers[HEADER] ?? '').trim();
  const token = raw && raw !== PLACEHOLDER ? raw : '';
  let body = '';
  req.on('data', c => { body += c; if (body.length > 1e6) req.destroy(); });
  req.on('end', async () => {
    let msg;
    try { msg = JSON.parse(body); } catch { res.writeHead(400); return res.end(); }
    const out = Array.isArray(msg) ? (await Promise.all(msg.map(m => handle(m, token)))).filter(Boolean) : await handle(msg, token);
    const headers = { 'Content-Type': 'application/json' };
    if (!Array.isArray(msg) && msg.method === 'initialize') headers['Mcp-Session-Id'] = randomUUID();
    if (out === null || (Array.isArray(out) && !out.length)) { res.writeHead(202, headers); return res.end(); }
    res.writeHead(200, headers);
    res.end(JSON.stringify(out));
  });
}).listen(PORT, () => console.log(`telegram MCP on :${PORT}/mcp (allowlist: ${ALLOWED.size ? [...ALLOWED].join(',') : 'none set'})`));
