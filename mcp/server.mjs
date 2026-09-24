#!/usr/bin/env node
// Three deliberately useless MCP servers in one file, so agents have tools to
// call and traces have tool spans to show. MCP_PERSONA picks the tool set:
//   oracle   ask_the_oracle, roll_dice          (a Magic 8-Ball for SREs)
//   coffee   brew, bean_level                   (the break-room machine)
//   excuses  generate_excuse, blame_dns         (incident excuse generator)
//
// Streamable HTTP transport, JSON responses only (no SSE), zero dependencies:
// POST /mcp with JSON-RPC 2.0. Runs on stock node:22-alpine from a ConfigMap.
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';

const PERSONA = process.env.MCP_PERSONA || 'oracle';
const PORT = Number(process.env.PORT || 8080);
const pick = a => a[Math.floor(Math.random() * a.length)];
const text = t => ({ content: [{ type: 'text', text: t }] });

const PERSONAS = {
  oracle: {
    title: 'The SRE Oracle',
    tools: {
      ask_the_oracle: {
        description: 'Ask the SRE Oracle a yes/no question about operations. It answers like a Magic 8-Ball, with the same accuracy.',
        schema: { type: 'object', properties: { question: { type: 'string', description: 'The question to ask' } }, required: ['question'] },
        run: ({ question }) => text(`🔮 "${question}"\n→ ${pick([
          'It is certain. Ship it.', 'Outlook good, if you add a feature flag.', 'Reply hazy: check the dashboards again.',
          'Ask again after the change freeze.', 'Very doubtful. It is Friday.', 'My sources say the pager disagrees.',
          'Signs point to yes, but roll back first.', 'Do not deploy. Mercury is in retrograde and so is your canary.'])}`),
      },
      roll_dice: {
        description: 'Roll dice to pick which service gets the next chaos experiment. Returns each roll and the total.',
        schema: { type: 'object', properties: { sides: { type: 'integer', minimum: 2, maximum: 100, default: 20 },
                                               count: { type: 'integer', minimum: 1, maximum: 10, default: 1 } } },
        run: ({ sides = 20, count = 1 }) => {
          const rolls = Array.from({ length: Math.min(10, count) }, () => 1 + Math.floor(Math.random() * Math.min(100, sides)));
          const total = rolls.reduce((a, b) => a + b, 0);
          return text(`🎲 ${count}d${sides}: [${rolls.join(', ')}] = ${total}${rolls.includes(sides) ? ' (critical: chaos wins)' : ''}`);
        },
      },
    },
  },
  coffee: {
    title: 'Break-room Coffee Machine',
    tools: {
      brew: {
        description: 'Brew a coffee drink for the on-call engineer. Returns the brew status and an ETA.',
        schema: { type: 'object', properties: { drink: { type: 'string', description: 'e.g. espresso, flat white, cold brew' },
                                               size: { type: 'string', enum: ['small', 'large', 'incident'], default: 'large' } },
                  required: ['drink'] },
        run: ({ drink, size = 'large' }) => text(`☕ Brewing a ${size} ${drink}. ETA ${size === 'incident' ? '0s (pre-brewed for emergencies)' : `${20 + Math.floor(Math.random() * 70)}s`}. ${pick([
          'Grinder sounds healthy.', 'Milk frother is degraded but within SLO.', 'Water tank at 12%: someone file a ticket.',
          'Cup dispenser is flaky; retrying with exponential backoff.'])}`),
      },
      bean_level: {
        description: 'Check the coffee bean level and the derived on-call morale index.',
        schema: { type: 'object', properties: {} },
        run: () => {
          const beans = Math.floor(Math.random() * 101);
          return text(`🫘 Beans at ${beans}%. On-call morale index: ${beans > 60 ? 'nominal' : beans > 25 ? 'degraded' : 'SEV1, restock immediately'}.`);
        },
      },
    },
  },
  excuses: {
    title: 'Incident Excuse Generator',
    tools: {
      generate_excuse: {
        description: 'Generate a plausible-sounding (and entirely unfalsifiable) excuse for an incident in a given system.',
        schema: { type: 'object', properties: { system: { type: 'string', description: 'The system that broke' } }, required: ['system'] },
        run: ({ system }) => text(`🙃 ${system}: ${pick([
          `a cosmic ray flipped a single bit in ${system}'s config`, `${system} was fine; the monitoring was down`,
          `an upstream dependency of ${system} changed behavior without a changelog`, `${system} hit a leap-second edge case, again`,
          `the intern's test traffic found a real bug in ${system}, which is a good thing`,
          `${system} worked on my machine`])}. Confidence: ${40 + Math.floor(Math.random() * 60)}%.`),
      },
      blame_dns: {
        description: 'Determine whether DNS is to blame. It is always DNS.',
        schema: { type: 'object', properties: { symptom: { type: 'string' } } },
        run: ({ symptom }) => text(`🌐 ${symptom ? `Symptom "${symptom}": ` : ''}It was DNS. (Confidence: 100%. It is always DNS.)`),
      },
    },
  },
};
const P = PERSONAS[PERSONA];
if (!P) { console.error(`unknown MCP_PERSONA ${PERSONA}`); process.exit(1); }

function handle(msg) {
  const { id, method, params } = msg;
  const ok = result => ({ jsonrpc: '2.0', id, result });
  const err = (code, message) => ({ jsonrpc: '2.0', id, error: { code, message } });
  switch (method) {
    case 'initialize':
      return ok({ protocolVersion: params?.protocolVersion || '2025-06-18', capabilities: { tools: { listChanged: false } },
                  serverInfo: { name: `scope-${PERSONA}`, title: P.title, version: '1.0.0' } });
    case 'ping': return ok({});
    case 'tools/list':
      return ok({ tools: Object.entries(P.tools).map(([name, t]) =>
        ({ name, description: t.description, inputSchema: t.schema })) });
    case 'tools/call': {
      const t = P.tools[params?.name];
      if (!t) return err(-32602, `unknown tool ${params?.name}`);
      console.log(JSON.stringify({ t: new Date().toISOString(), tool: params.name, args: params.arguments ?? {} }));
      try { return ok(t.run(params.arguments ?? {})); }
      catch (e) { return ok({ ...text(`tool error: ${e.message}`), isError: true }); }
    }
    default:
      return id === undefined ? null : err(-32601, `method not found: ${method}`);   // notifications get no reply
  }
}

createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (req.method === 'DELETE') { res.writeHead(200); return res.end(); }        // session teardown
  if (req.method !== 'POST') { res.writeHead(405, { Allow: 'POST' }); return res.end(); }
  let body = '';
  req.on('data', c => { body += c; if (body.length > 1e6) req.destroy(); });
  req.on('end', () => {
    let msg;
    try { msg = JSON.parse(body); } catch { res.writeHead(400); return res.end(); }
    const out = Array.isArray(msg) ? msg.map(handle).filter(Boolean) : handle(msg);
    const headers = { 'Content-Type': 'application/json' };
    if (!Array.isArray(msg) && msg.method === 'initialize') headers['Mcp-Session-Id'] = randomUUID();
    if (out === null || (Array.isArray(out) && !out.length)) { res.writeHead(202, headers); return res.end(); }
    res.writeHead(200, headers);
    res.end(JSON.stringify(out));
  });
}).listen(PORT, () => console.log(`scope-${PERSONA} MCP (${P.title}) on :${PORT}/mcp`));
