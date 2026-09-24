#!/usr/bin/env node
// Seed the kagent UI with demo content: named multi-turn chats, checkpoints
// at meaningful turns (the Snapshots page), and a fork from one of them.
//
//   node deploy/seed-demo.mjs          # needs server.mjs --live running (its port-forwards)
//   node deploy/seed-demo.mjs --fresh  # delete the seeded chats (forks, checkpoints) first
//
// Idempotent: a scenario whose chat already exists is skipped. The chats are
// not named scope-*, so Scope's session cleanup leaves them alone. Each turn is
// a real Claude call (about 20 short turns in total).
import { client, GrpcError } from '../lib/grpcweb.mjs';
import { tokenSource } from '../lib/enterprise.mjs';

const API = process.env.KAGENT_API || 'http://127.0.0.1:8083';
const UI = process.env.KAGENT_UI || 'http://127.0.0.1:8001';
const NS = process.env.ATESPACE || 'kagent';
const c = client(API, tokenSource({ uiBase: UI, token: process.env.KAGENT_TOKEN }));
const INST = 'kagent.api.v1alpha1.AgentInstanceService/';
const CKPT = 'kagent.api.v1alpha1.CheckpointService/';
const sleep = ms => new Promise(r => setTimeout(r, ms));
const rid = p => `${p}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

// harness per agent (claude or claude-oncall), from the templates' status
const tpls = (await c.unary('kagent.api.v1alpha1.AgentTemplateService/ListAgentTemplates', {})).agent_templates ?? [];
const harnessOf = Object.fromEntries(tpls.map(t => [t.ref.name, t.resource?.value?.status?.harnesses?.[0]?.harness]));
const existing = new Set(((await c.unary(INST + 'ListAgentInstances', { all_creators: true, page: { limit: 100 } }))
  .agent_instances ?? []).map(i => i.name).filter(Boolean));

async function turn(inst, text) {
  for (let tries = 0; tries < 20; tries++) {
    let reply = '', state = '';
    try {
      for await (const ev of c.stream('lf.a2a.v1.A2AService/SendStreamingMessage', {
          message: { message_id: rid('seed'), context_id: inst.context_id, role: 'ROLE_USER', parts: [{ text }] } },
          { headers: { 'x-kagent-agent-instance-id': inst.id }, timeout: 180_000 })) {
        if (ev.artifact_update) reply += (ev.artifact_update.artifact?.parts ?? []).map(p => p.text ?? '').join('');
        state = ev.status_update?.status?.state ?? ev.task?.status?.state ?? state;
      }
      return { state, reply };
    } catch (e) {
      // pool full (alpha3 times out instead of queueing): cancel and wait
      if (e instanceof GrpcError && /timed out|active task/i.test(e.message)) {
        const t = await c.unary('lf.a2a.v1.A2AService/ListTasks', { context_id: inst.context_id, page_size: 10 },
                                { headers: { 'x-kagent-agent-instance-id': inst.id } }).catch(() => ({}));
        for (const x of t.tasks ?? []) if (/SUBMITTED/.test(x.status?.state ?? ''))
          await c.unary('lf.a2a.v1.A2AService/CancelTask', { id: x.id }, { headers: { 'x-kagent-agent-instance-id': inst.id } }).catch(() => {});
        await sleep(4000); continue;
      }
      throw e;
    }
  }
  throw new Error('no free worker after 20 tries');
}

async function checkpoint(inst, name) {
  let ck = (await c.unary(CKPT + 'CreateCheckpoint', { agent_instance_id: inst.id, request_id: rid('ckpt') })).checkpoint;
  for (let i = 0; i < 60 && ck.state !== 'CHECKPOINT_STATE_READY'; i++) {
    if (ck.state === 'CHECKPOINT_STATE_FAILED') throw new Error(`checkpoint failed: ${ck.failure?.message}`);
    await sleep(2000);
    ck = (await c.unary(CKPT + 'GetCheckpoint', { checkpoint_id: ck.id })).checkpoint;
  }
  await c.unary(CKPT + 'UpdateCheckpointName', { checkpoint_id: ck.id, name });
  console.log(`   📸 ${name}`);
  return ck;
}

async function chat(agent, name) {
  const r = await c.unary(INST + 'CreateAgentInstance', {
    harness: { namespace: NS, name: harnessOf[agent] }, agent_template: { namespace: NS, name: agent },
    request_id: rid('seed'), name });
  return r.agent_instance;
}

async function say(inst, text) {
  const { state, reply } = await turn(inst, text);
  console.log(`   ${state.endsWith('COMPLETED') ? '✓' : '✗'} ${text.slice(0, 60)}  →  ${reply.replace(/\s+/g, ' ').slice(0, 70)}`);
}

const SCENARIOS = [
  { agent: 'sre-oncall', name: 'Incident · EU checkout 5xx', steps: [
      ['say', 'Alert: checkout returns 5xx for about 20% of EU customers since 14:09 UTC. Severity?'],
      ['say', 'Deploy 4.12 went out at 14:02, seven minutes before the alert. What do we do?'],
      ['ckpt', 'before mitigation'],
      ['say', 'We rolled back 4.12 and errors dropped to 0.1%. What next?'],
      ['ckpt', 'after rollback'],
      ['fork', 'before mitigation', 'What-if · rollback blocked', [
        'Rollback is blocked: 4.12 ran a schema migration. What now?']],
    ] },
  { agent: 'incident-scribe', name: 'Postmortem · EU checkout 5xx', steps: [
      ['say', 'SEV2, customer-facing. Events: 14:02 deploy 4.12, 14:09 alert fired, 14:15 rollback started, 14:21 errors normal. Reply with the timeline.'],
      ['ckpt', 'timeline v1'],
      ['say', 'Root cause: the new retry logic in 4.12 exhausted the database connection pool. Reply with the timeline plus a root-cause line.'],
      ['ckpt', 'draft with root cause'],
    ] },
  { agent: 'cost-warden', name: 'Capacity review · agent fleet', steps: [
      ['say', 'We run 40 agents as always-on pods that each reserve 300Mi and 350m. What does the fleet reserve?'],
      ['say', 'On Agent Substrate, 4 workers serve those 40 agents, and each worker reserves the same 300Mi and 350m. What do the 4 workers reserve, and what is the saving against the 40 always-on pods?'],
      ['ckpt', 'savings estimate'],
    ] },
  { agent: 'release-notary', name: 'Release notes · 4.13', steps: [
      ['say', 'Changes in 4.13: fix the checkout retry storm, add an EU region failover flag, upgrade to Go 1.27. Reply with the release notes (do not write a file).'],
      ['ckpt', 'draft notes'],
    ] },
];

if (process.argv.includes('--fresh')) {
  // forks first (a checkpoint cannot be deleted while a fork of it exists),
  // then each chat's checkpoints, then the chats
  const all = (await c.unary(INST + 'ListAgentInstances', { all_creators: true, page: { limit: 100 } })).agent_instances ?? [];
  const seeded = new Set(SCENARIOS.flatMap(s => [s.name, ...s.steps.filter(x => x[0] === 'fork').map(x => x[2])]));
  const forks = new Set(SCENARIOS.flatMap(s => s.steps.filter(x => x[0] === 'fork').map(x => x[2])));
  const mine = all.filter(i => seeded.has(i.name)).sort((a, b) => forks.has(b.name) - forks.has(a.name));
  for (const i of mine) {
    const cks = (await c.unary(CKPT + 'ListCheckpoints', { agent_instance_id: i.id, page: { limit: 100 } })).checkpoints ?? [];
    for (const k of cks) await c.unary(CKPT + 'DeleteCheckpoint', { checkpoint_id: k.id }).catch(e => console.log(`   ! checkpoint ${k.name}: ${e.message}`));
    await c.unary(INST + 'DeleteAgentInstance', { agent_instance_id: i.id }).catch(e => console.log(`   ! ${i.name}: ${e.message}`));
    existing.delete(i.name);
    console.log(`✗ removed ${i.name} (${cks.length} checkpoint${cks.length === 1 ? '' : 's'})`);
  }
}

for (const s of SCENARIOS) {
  if (existing.has(s.name)) { console.log(`• ${s.name}: exists, skipped`); continue; }
  if (!harnessOf[s.agent]) { console.log(`• ${s.name}: agent ${s.agent} not found, skipped`); continue; }
  console.log(`• ${s.name} (${s.agent})`);
  const inst = await chat(s.agent, s.name);
  const ckpts = {};
  for (const [op, a, b, more] of s.steps) {
    if (op === 'say') await say(inst, a);
    else if (op === 'ckpt') ckpts[a] = await checkpoint(inst, a);
    else if (op === 'fork') {
      const f = (await c.unary(CKPT + 'ForkAgentInstance', { checkpoint_id: ckpts[a].id, request_id: rid('fork') })).agent_instance;
      await c.unary(INST + 'UpdateAgentInstanceName', { agent_instance_id: f.id, name: b });
      console.log(`   ⑂ fork of "${a}" → ${b}`);
      for (const t of more) await say(f, t);
    }
  }
}
console.log('done: see the kagent UI Snapshots page, and the chats in each agent');
