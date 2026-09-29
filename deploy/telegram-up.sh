#!/usr/bin/env bash
# sre-oncall on Telegram (see deploy/telegram.yaml for the design).
#
#   TELEGRAM_PUBLIC_URL=https://oncall-bot.example.com ./deploy/telegram-up.sh
#   TELEGRAM_ALLOWED_CHATS=123456789 TELEGRAM_PUBLIC_URL=... ./deploy/telegram-up.sh
#   ./deploy/telegram-up.sh --register      # ask the agent to (re)register its webhook
#
# Reads (never prints) these files, when present:
#   ~/.telegram_bot_token          from @BotFather        -> Secret telegram-bot
#   ~/.cloudflared_tunnel_token    Zero Trust tunnel token -> Secret cloudflared-tunnel
# Without the bot token, the Secret holds a placeholder so sre-oncall stays
# Ready (its template binds the telegram server); the tools just fail.
# Re-run it any time: it keeps the webhook secret and an allowlist it already has.
set -euo pipefail
cd "$(dirname "$0")"
C=${KUBE_CONTEXT:-kind-kagent-ent}
K() { kubectl --context "$C" "$@"; }
BOT_FILE=${TELEGRAM_BOT_TOKEN_FILE:-$HOME/.telegram_bot_token}
TUNNEL_FILE=${CLOUDFLARED_TOKEN_FILE:-$HOME/.cloudflared_tunnel_token}

if [ "${1:-}" = --register ]; then
  echo "asking sre-oncall to register its webhook (one real turn)..."
  K exec -n kagent deploy/telegram-doorbell -- wget -qO- --post-data= http://127.0.0.1:8081/register
  exit
fi

# Secrets. The bot token and tunnel token come from files, on stdin, so they
# never appear in a process list or in this script's output.
secret_from_file() { local name=$1 file=$2
  K create secret generic "$name" -n kagent --from-file=token=/dev/stdin --dry-run=client -o yaml \
    < <(tr -d '[:space:]' < "$file") | K apply -f - >/dev/null; }
if [ -s "$BOT_FILE" ]; then secret_from_file telegram-bot "$BOT_FILE"; echo "bot token: loaded from $BOT_FILE"
elif ! K get secret telegram-bot -n kagent >/dev/null 2>&1; then
  K create secret generic telegram-bot -n kagent --from-literal=token=unset >/dev/null
  echo "bot token: none yet (placeholder; save it to $BOT_FILE and re-run)"
fi
K get secret telegram-webhook -n kagent >/dev/null 2>&1 || \
  K create secret generic telegram-webhook -n kagent --from-literal=secret="$(openssl rand -hex 24)" >/dev/null
tunnel=0
if [ -s "$TUNNEL_FILE" ]; then secret_from_file cloudflared-tunnel "$TUNNEL_FILE"; tunnel=1; echo "tunnel token: loaded from $TUNNEL_FILE"
else echo "tunnel token: none ($TUNNEL_FILE); no tunnel"; fi

# Config: keep what is already set unless overridden.
cur() { K get configmap telegram-config -n kagent -o jsonpath="{.data.$1}" 2>/dev/null || true; }
URL=${TELEGRAM_PUBLIC_URL:-$(cur PUBLIC_URL)}
CHATS=${TELEGRAM_ALLOWED_CHATS:-$(cur TELEGRAM_ALLOWED_CHATS)}
[ -n "$URL" ] && [[ "$URL" != https://* ]] && { echo "TELEGRAM_PUBLIC_URL must start with https://"; exit 1; }
K create configmap telegram-config -n kagent --from-literal=PUBLIC_URL="${URL%/}" \
  --from-literal=TELEGRAM_ALLOWED_CHATS="$CHATS" --dry-run=client -o yaml | K apply -f - >/dev/null

# Code, then the objects. Roll the pods so they pick up new code or config.
K create configmap telegram-src -n kagent --from-file=../telegram/doorbell.mjs --from-file=../telegram/mcp.mjs \
  --from-file=../lib/grpcweb.mjs --from-file=../lib/enterprise.mjs --from-file=../lib/schema.json \
  --dry-run=client -o yaml | K apply -f - >/dev/null
K apply -f telegram.yaml >/dev/null
K rollout restart deploy/telegram-mcp deploy/telegram-doorbell -n kagent >/dev/null
[ "$tunnel" = 1 ] && K apply -f telegram-tunnel.yaml >/dev/null
K rollout status deploy/telegram-mcp -n kagent --timeout=120s >/dev/null
K rollout status deploy/telegram-doorbell -n kagent --timeout=120s >/dev/null
[ "$tunnel" = 1 ] && K rollout status deploy/telegram-tunnel -n kagent --timeout=120s >/dev/null
# rollout status returns while an old pod may still be terminating (and serving)
for _ in $(seq 1 30); do
  [ "$(K get pods -n kagent -l 'app in (telegram-mcp,telegram-doorbell)' --no-headers | wc -l | tr -d ' ')" = 2 ] && break; sleep 2
done

echo
echo "public URL : ${URL:-(unset: set TELEGRAM_PUBLIC_URL)}"
echo "allowlist  : ${CHATS:-(empty: message the bot, then find your chat id with the command below)}"
echo "  kubectl --context $C logs -n kagent deploy/telegram-doorbell | grep ignored"
echo "webhook    : once the URL, the tunnel and a real bot token are in place, register it (one turn):"
echo "  ./deploy/telegram-up.sh --register"
