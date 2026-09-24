#!/usr/bin/env bash
# Pre-demo check for the enterprise rig. Run it with Scope already running:
#
#   ./deploy/preflight.sh            # checks only
#   ./deploy/preflight.sh --fix      # also resets the pool if workers are stuck
#
# Every check here is a failure we hit live on alpha3. Exit 0 = safe to demo.
set -uo pipefail
C=${KUBE_CONTEXT:-kind-kagent-ent}
SCOPE=${SCOPE:-http://127.0.0.1:8123}
FIX=${1:-}
K() { kubectl --context "$C" "$@"; }
fail=0; ok() { echo "  ok    $*"; }; bad() { echo "  FAIL  $*"; fail=1; }; warn() { echo "  warn  $*"; }

echo "Docker VM (other clusters starve gVisor restores and cause ate-api restart cascades)"
busy=$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' | awk -v me="${C#kind-}-control-plane" \
  '$1!=me {gsub("%","",$2); s+=$2} END {printf "%d", s}')
[ "$busy" -lt 150 ] && ok "other containers use ${busy}% CPU" \
  || warn "other containers use ${busy}% CPU: stop clusters you don't need (docker stop ...)"

echo "Controller (a restart mid-demo re-bakes snapshots, drops tokens, and can wedge resumes)"
start=$(K get pods -n kagent -l app.kubernetes.io/component=controller \
  -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="controller")].state.running.startedAt}')
age=$(( $(date -u +%s) - $(date -ju -f '%Y-%m-%dT%H:%M:%SZ' "$start" +%s 2>/dev/null || date -u -d "$start" +%s) ))
[ "$age" -gt 600 ] && ok "controller up $((age / 60))m" \
  || warn "controller restarted $((age / 60))m ago: wait until templates settle, then re-run"

echo "Agents (every template Ready on its current revision)"
not_ready=$(K get agenttemplate -n kagent -o json | jq -r '.items[] | select(any(.status.harnesses[]?;
  .desiredRevision != .latestSuccessfulRevision or ([.conditions[]? | select(.type=="Ready")][0].status != "True")))
  | .metadata.name' | tr '\n' ' ')
[ -z "$not_ready" ] && ok "all AgentTemplates Ready" || bad "not ready: $not_ready(golden snapshots still baking?)"

echo "Scope board"
snap=$(curl -s -N -m 4 "$SCOPE/events" | sed -n 's/^data: //p' | jq -c 'select(.type=="snapshot")' | tail -1)
if [ -z "$snap" ]; then bad "no snapshot from $SCOPE (is server.mjs --live running?)"; else
  stuck=$(jq -r '[.actors[] | select(.status=="Resuming")] | length' <<<"$snap")
  held=$(jq -r '[.workers[] | select(.actorId)] | length' <<<"$snap")
  total=$(jq -r '.workers | length' <<<"$snap")
  [ "$stuck" = 0 ] && ok "no actors stuck resuming" || bad "$stuck actor(s) Resuming at rest"
  [ "$held" = 0 ] && ok "$total workers, all free" || warn "$held/$total workers busy at rest (traffic running?)"
  if [ "$fail" = 1 ] && [ "$FIX" = --fix ]; then
    echo "  fix   RESET POOL (cancel open tasks, replace worker pods; snapshots survive)"
    curl -s -m 60 -X POST "$SCOPE/reset" | jq -c .
  fi
fi

echo "Port-forwards (only Scope's own; a stale one serves a dead pod)"
for port in 8083 8001; do
  pid=$(lsof -t -nP -iTCP:$port -sTCP:LISTEN 2>/dev/null | head -1)
  parent=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')
  if [ -z "$pid" ]; then bad ":$port not listening";
  elif ps -o command= -p "$parent" 2>/dev/null | grep -q 'server.mjs'; then ok ":$port forwarded by Scope"
  else bad ":$port held by pid $pid, not Scope (stale forward?): kill $pid"; fi
done

echo "One real turn (key valid, restore -> Claude -> checkpoint)"
r=$(curl -s -m 120 -X POST "$SCOPE/converse" -d '{"agent":"hello-substrate","text":"Reply with the single word ready.","via":"preflight"}')
[ "$(jq -r .ok <<<"$r")" = true ] && ok "hello-substrate answered in $(jq -r .ms <<<"$r")ms" \
  || bad "turn failed: $(jq -r .text <<<"$r" | cut -c1-120)"

echo "Tracing (the Tracing page needs Claude's call_llm spans, which have broken silently before)"
sleep 12   # the collector batches
CH=$(K get pods -n kagent -o name | grep clickhouse | head -1)
llm=$(K exec -n kagent "$CH" -- clickhouse-client -q "SELECT count() FROM kagent.otel_traces_json WHERE SpanName='call_llm'
  AND TraceId = (SELECT TraceId FROM kagent.otel_traces_json WHERE SpanName LIKE 'invoke_agent hello-substrate%' ORDER BY Timestamp DESC LIMIT 1)" 2>/dev/null)
[ "${llm:-0}" -gt 0 ] && ok "last turn has $llm call_llm span(s): Model and Tokens will show" \
  || bad "last turn has no call_llm spans: the Tracing page will show '—' for Model and Tokens"

echo; [ "$fail" = 0 ] && echo "PREFLIGHT OK" || echo "PREFLIGHT FAILED (re-run with --fix, or rebuild with ./deploy/install.sh)"
exit $fail
