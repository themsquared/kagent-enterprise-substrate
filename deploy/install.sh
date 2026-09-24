#!/usr/bin/env bash
# One-shot lab: kind (k8s 1.37) + Agent Substrate 0.2.0-beta5 +
# kagent-enterprise 1.0.0-alpha3 + the 9-agent Claude Code fleet.
# Follows solo-io/enterprise-kagent discussion #191, sections 4-10.
#
#   ANTHROPIC_KEY_FILE=~/.anthropic_key SOLO_LICENSE_KEY=... ./deploy/install.sh
#
# SOLO_LICENSE_KEY is optional: without it the controller logs
# "[SOLO LICENSE] ... missing or invalid" and still runs. Re-run with a
# kagent-enterprise license to license it (an agentgateway key is rejected).
# Idempotent enough to re-run after a failure; skips steps already done.
set -euo pipefail
cd "$(dirname "$0")"
CLUSTER=${CLUSTER:-kagent-ent}
C=kind-$CLUSTER
K() { kubectl --context "$C" "$@"; }
KEYFILE=${ANTHROPIC_KEY_FILE:-$HOME/.anthropic_key}
[ -s "$KEYFILE" ] || { echo "Anthropic key file $KEYFILE missing (set ANTHROPIC_KEY_FILE)"; exit 1; }
kind version | grep -qE 'v0\.(3[2-9]|[4-9][0-9])' || { echo "kind >= v0.32 required (brew upgrade kind)"; exit 1; }

# §4 cluster: 1.37 with certificates.k8s.io/v1beta1 on (Substrate needs it)
kind get clusters | grep -qx "$CLUSTER" || kind create cluster --config kind-kagent-ent.yaml --name "$CLUSTER"
K get --raw /apis/certificates.k8s.io/v1beta1 | jq -e '.resources[] | select(.name=="clustertrustbundles")' >/dev/null

# §5 CRDs (substrate.enabled=true installs workerpools.ate.dev)
helm status --kube-context "$C" kagent-crds -n kagent >/dev/null 2>&1 || \
helm install --kube-context "$C" kagent-crds \
  oci://us-docker.pkg.dev/solo-public/kagent-enterprise-helm/charts/kagent-enterprise-crds \
  --version 1.0.0-alpha3 --namespace kagent --create-namespace --set substrate.enabled=true

# §6.1 Substrate as its own release: name "substrate", namespace ate-system.
# upgrade --install so a re-run applies value changes (otel.endpoint, which
# feeds the kagent dashboard, is not in the #191 procedure).
helm upgrade --install --kube-context "$C" substrate oci://ghcr.io/kagent-dev/substrate/helm/substrate \
  --version 0.2.0-beta5 --namespace ate-system --create-namespace --wait=false -f substrate-values.yaml

# §6.2 pools (once only: make-* fails if the Secret exists)
ATE=./kubectl-ate
if [ ! -x "$ATE" ]; then
  OS=$(uname -s | tr '[:upper:]' '[:lower:]'); ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
  curl -fsSL -o "$ATE" "https://github.com/kagent-dev/substrate/releases/download/v0.2.0-beta5/kubectl-ate-${OS}-${ARCH}"
  chmod +x "$ATE"
fi
pool() { local kind=$1 name=$2 ns=$3
  K get secret "$name" -n "$ns" >/dev/null 2>&1 && return
  if [ "$kind" = jwt ]; then "$ATE" --context "$C" admin make-jwt-pool --key-id=1 --name="$name" --secret-namespace="$ns"
  else "$ATE" --context "$C" admin make-ca-pool --ca-id=1 --name="$name" --secret-namespace="$ns"; fi; }
pool ca  service-dns-ca-pool  podcertificate-controller-system
pool ca  pod-identity-ca-pool podcertificate-controller-system
pool jwt actor-id-jwt-pool    ate-system
pool ca  actor-id-ca-pool     ate-system
pool ca  egress-mitm-ca-pool  ate-system

# §6.3 actor-id root as a PEM Secret
if ! K get secret actor-id-ca-certs -n ate-system >/dev/null 2>&1; then
  K get secret actor-id-ca-pool -n ate-system -o jsonpath='{.data.pool}' | base64 --decode \
    | jq -r '.CAs[0].RootCertificateDER' | base64 --decode \
    | openssl x509 -inform der -outform pem > /tmp/actor-id-ca.$$.crt
  K create secret generic actor-id-ca-certs -n ate-system --from-file=ca.crt=/tmp/actor-id-ca.$$.crt
  rm -f /tmp/actor-id-ca.$$.crt
fi

# §6.5 ate-api audience
K get configmap ate-api-authentication -n ate-system >/dev/null 2>&1 || \
K create configmap ate-api-authentication -n ate-system --from-literal=authentication.yaml='actorIdentityJWTProvider: kubernetes
jwtProviders:
- name: kubernetes
  issuer: https://kubernetes.default.svc
  audiences: [api.ate-system.svc]
  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
'

# §6.6 wait for Substrate (the controller dials ate-api at start)
K rollout status deploy/podcertificate-controller -n podcertificate-controller-system --timeout=300s
for d in ate-api-server ate-controller atenet-router atenet-egress k8s-credential-provider; do
  K rollout status deploy/$d -n ate-system --timeout=300s
done
K rollout status ds/atelet -n ate-system --timeout=300s

# §8 (before §7 so default-model-config resolves): the Anthropic key Secret
K get ns kagent >/dev/null 2>&1 || K create ns kagent
K create secret generic kagent-anthropic -n kagent \
  --from-literal=ANTHROPIC_API_KEY="$(tr -d '[:space:]' < "$KEYFILE")" \
  --dry-run=client -o yaml | K apply -f - >/dev/null
# the egress gateway caches credentials: pick up a rotated key
K rollout restart deploy/k8s-credential-provider deploy/atenet-egress -n ate-system >/dev/null
K rollout status deploy/atenet-egress -n ate-system --timeout=120s

# §7 kagent-enterprise
LIC=(--set global.licensing.createSecret=false)
[ -n "${SOLO_LICENSE_KEY:-}" ] && LIC=(--set global.licensing.createSecret=true
                                       --set-string global.licensing.licenseKey="$SOLO_LICENSE_KEY")
helm upgrade --install --kube-context "$C" kagent \
  oci://us-docker.pkg.dev/solo-public/kagent-enterprise-helm/charts/kagent-enterprise \
  --version 1.0.0-alpha3 --namespace kagent -f values.yaml "${LIC[@]}" --wait --timeout 15m

# The useless MCP servers the agents bind to (mcp/server.mjs on node:22-alpine)
K create configmap scope-mcp-src -n kagent --from-file=../mcp/server.mjs --dry-run=client -o yaml | K apply -f - >/dev/null
K apply -f mcp.yaml >/dev/null
for s in oracle coffee excuses; do K rollout status deploy/scope-mcp-$s -n kagent --timeout=180s >/dev/null; done
for _ in $(seq 1 30); do   # kagent discovers each server's tools (first try can race the pod)
  n=$(K get remotemcpservers.kagent.dev -n kagent -l demo=substrate-scope -o json \
    | jq '[.items[] | select(any(.status.conditions[]?; .type=="Accepted" and .status=="True"))] | length')
  [ "$n" = 3 ] && break; sleep 5
done

# §10 two WorkerPools, two Harnesses, the fleet; then wait for 9 golden snapshots
K apply -f prompts.yaml -f fleet.yaml >/dev/null   # prompt libraries and tools before the agents that use them
echo "waiting for golden snapshots..."
for _ in $(seq 1 60); do
  ready=$(K get agenttemplate -n kagent -l demo=substrate-scope -o json \
    | jq '[.items[] | select(any(.status.harnesses[]?.conditions[]?; .type=="Ready" and .status=="True"))] | length')
  echo "  $ready/9 Ready"; [ "$ready" = 9 ] && break; sleep 10
done
echo
echo "Done. Start the board:"
echo "  KUBE_CONTEXT=$C node server.mjs --live     # http://localhost:8123 (kagent UI on :8001)"
echo "Then, once, fill the kagent UI's chats and Snapshots page:"
echo "  node deploy/seed-demo.mjs"
