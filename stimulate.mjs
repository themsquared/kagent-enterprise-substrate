#!/usr/bin/env node
// Auto-stimulator: sends REAL chats to random agents so the live board keeps
// moving during a POC. Nothing is faked — every prompt resumes a real actor
// from its snapshot, runs a real Claude Code turn, and checkpoints back.
//
//   node stimulate.mjs                      # auto concurrency: workers + oversub
//   node stimulate.mjs --concurrency 3 --interval 4 --budget 400
//
// Everything goes through the Scope server (server.mjs --live): it owns the
// enterprise API, the admission queue (alpha3 times out oversubscribed sends,
// so turns wait for a free worker in Scope's queue lane), and STOP DEMO.
const arg = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > -1 ? Number(process.argv[i + 1]) : dflt;
};
const VIZ = process.env.VIZ || 'http://127.0.0.1:8123';
const INTERVAL = arg('interval', 2);   // mean seconds between dispatch checks
const LOAD = arg('load', 0.75);        // fraction of long-form prompts
// --concurrency N to pin; otherwise auto-sized to (live worker count +
// oversub) so every bay stays lit AND a visible retry-queue forms.
const CONC_ARG = arg('concurrency', 0);
const OVERSUB = arg('oversub', 4);     // extra in-flight beyond the pool size —
                                       // deep enough that queued agents visibly wait their turn
const BUDGET = arg('budget', 0);       // stop after N chats total (0 = unlimited)
                                       // — overnight-safe: no runaway API spend

const QUICK_PROMPTS = [
  'In one short sentence, what is your job?',
  'Give me one tip from your specialty. One sentence.',
  'What would you check first during an incident? One sentence.',
  'Reply with a haiku about Kubernetes.',
  'One sentence: why do snapshots beat idle pods?',
  // tool turns: the MCP servers (mcp/server.mjs) give traces execute_tool spans
  'Use your tools: is this outage DNS? Answer in one sentence.',
  'Use your tools: ask the oracle whether we should deploy on Friday.',
  'Use your tools: brew an incident-size coffee and check the bean level.',
  'Use your tools: roll 2d20 to pick the next chaos target, then generate an excuse for it.',
];
// Long generations hold an actor on its worker for 10–20s — that's what makes
// several bays glow at once instead of a single 2s flash.
const LONG_PROMPTS = [
  'Write a detailed 15-step runbook for a failed rollout, two sentences per step.',
  'Draft a ~450-word incident postmortem for a fictional cache outage, with timeline, root cause, and action items.',
  'Explain in ~400 words how you would triage rising p99 latency, covering dashboards, traces, and rollback criteria.',
  'List 20 things to check after a Kubernetes upgrade, with a sentence of rationale for each.',
  'Write a ~400-word briefing on why idle agents waste cluster capacity and how snapshot-based multiplexing fixes it.',
  'Compose a ~450-word status update to leadership about a resolved sev-2, including impact, response, and prevention.',
];
const PROMPTS = null; // superseded by QUICK_PROMPTS / LONG_PROMPTS

const sleep = ms => new Promise(r => setTimeout(r, ms));
const rand = a => a[Math.floor(Math.random() * a.length)];
const jitter = s => (s * (0.5 + Math.random())) * 1000;

async function fleet() {
  const r = await fetch(`${VIZ}/agents`, { signal: AbortSignal.timeout(8000) });
  return r.json();                     // { agents: ["ns/name"], workers: n }
}

let inFlight = 0, sent = 0, ok = 0, failed = 0;

async function workerCount() {
  try { return (await fleet()).workers || 2; } catch { return 2; }
}

async function chat(agentRef) {
  const [ns, name] = agentRef.split('/');
  const prompt = Math.random() < LOAD ? rand(LONG_PROMPTS) : rand(QUICK_PROMPTS);
  inFlight++; sent++;
  try {
    // the server records prompt + reply in the activity feed and queues the
    // turn until a worker is free; this resolves when the turn is done
    const r = await fetch(`${VIZ}/converse`, { method: 'POST',
      body: JSON.stringify({ ns, agent: name, text: prompt, via: 'stimulator' }),
      signal: AbortSignal.timeout(600_000) });
    const j = await r.json();
    const secs = ((j.ms ?? 0) / 1000).toFixed(1);
    if (j.ok) { ok++; console.log(`✓ ${name} (${secs}s): ${String(j.text).slice(0, 90)}`); }
    else if (j.stopped) sent--;
    else { failed++; console.log(`✗ ${name} (${secs}s): ${String(j.text).slice(0, 90)}`); }
  } catch (e) {
    failed++; console.log(`✗ ${name}: ${String(e.message).slice(0, 90)}`);
  } finally { inFlight--; }
}

const { agents } = await fleet().catch(() => ({ agents: [] }));
if (!agents.length) { console.error(`no Ready agents at ${VIZ}/agents — is server.mjs --live running?`); process.exit(1); }
let concurrency = CONC_ARG || (await workerCount()) + OVERSUB;
console.log(`stimulating ${agents.length} agents via ${VIZ} — ≤${concurrency} in flight`
  + `${CONC_ARG ? '' : ` (auto: workers + ${OVERSUB} oversub; re-checks as you scale)`}, ${Math.round(LOAD*100)}% long-form`
  + (BUDGET ? `, budget ${BUDGET} chats` : ', no budget (Ctrl-C or STOP DEMO to halt)'));
console.log(agents.map(a => '  ' + a).join('\n'));

let stop = false;
process.on('SIGINT', () => { stop = true;
  console.log(`\nstopping… sent=${sent} ok=${ok} failed=${failed}`); });

// the viz server's STOP DEMO button is the master switch for billable chats
let demoRun = true, lastDemoCheck = 0;
async function checkDemo(){
  if (Date.now() - lastDemoCheck < 2000) return;
  lastDemoCheck = Date.now();
  try {
    const j = await (await fetch(`${VIZ}/demo`, { signal: AbortSignal.timeout(2000) })).json();
    if (demoRun !== j.run) console.log(j.run ? '▶ demo resumed' : '■ demo stopped — no new chats');
    demoRun = j.run;
  } catch {}   // viz server absent → keep running standalone
}

let lastSize = Date.now();
while (!stop) {
  if (BUDGET && sent >= BUDGET){
    console.log(`■ budget reached: ${sent}/${BUDGET} chats dispatched — stopping`);
    // flip the board's master switch so STOP DEMO shows why traffic ended
    fetch(`${VIZ}/demo`, { method: 'POST', body: JSON.stringify({ run: false }),
      signal: AbortSignal.timeout(2000) }).catch(() => {});
    break;
  }
  await checkDemo();
  if (!CONC_ARG && Date.now() - lastSize > 10_000){   // follow live pool scaling
    lastSize = Date.now();
    workerCount().then(n => { const c = n + OVERSUB; if (c !== concurrency){
      console.log(`pool is now ${n} workers — concurrency → ${c}`); concurrency = c; } });
  }
  while (demoRun && inFlight < concurrency && !stop
         && !(BUDGET && sent >= BUDGET)) { chat(rand(agents)); await sleep(400); }
  await sleep(jitter(INTERVAL));
}
while (inFlight > 0) await sleep(500);
process.exit(0);
