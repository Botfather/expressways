#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TRANSPORT="${TRANSPORT:-tcp}"
ADDRESS="${ADDRESS:-127.0.0.1:7766}"
TOKEN_FILE="${TOKEN_FILE:-./var/auth/developer.token}"
STATE_DIR="${STATE_DIR:-./var/agent/nanobot-runtime-smoke}"
INBOUND_TOPIC="${INBOUND_TOPIC:-nanobot.inbound}"
RUNTIME_EVENTS_TOPIC="${RUNTIME_EVENTS_TOPIC:-nanobot.runtime.events}"
PRIMARY_PROVIDER="${PRIMARY_PROVIDER:-openai}"
PRIMARY_BAD_BASE_URL="${PRIMARY_BAD_BASE_URL:-http://127.0.0.1:1}"
SESSION_ID="${SESSION_ID:-smoke-failover-$(date +%s)}"

if [[ ! -f "$TOKEN_FILE" ]]; then
  echo "Missing token file: $TOKEN_FILE"
  exit 1
fi

FALLBACK_PROVIDER=""
FALLBACK_MODEL=""
FALLBACK_KEY=""
if [[ "$PRIMARY_PROVIDER" == "openai" ]]; then
  FALLBACK_PROVIDER="anthropic"
  FALLBACK_MODEL="${FALLBACK_MODEL:-claude-3-5-sonnet-latest}"
  FALLBACK_KEY="${ANTHROPIC_API_KEY:-}"
  if [[ -z "$FALLBACK_KEY" ]]; then
    echo "Set ANTHROPIC_API_KEY to run the smoke test with OpenAI primary failover."
    exit 1
  fi
elif [[ "$PRIMARY_PROVIDER" == "anthropic" ]]; then
  FALLBACK_PROVIDER="openai"
  FALLBACK_MODEL="${FALLBACK_MODEL:-gpt-4o-mini}"
  FALLBACK_KEY="${OPENAI_API_KEY:-}"
  if [[ -z "$FALLBACK_KEY" ]]; then
    echo "Set OPENAI_API_KEY to run the smoke test with Anthropic primary failover."
    exit 1
  fi
else
  echo "Unsupported PRIMARY_PROVIDER: $PRIMARY_PROVIDER (use openai or anthropic)"
  exit 1
fi

echo "Publishing smoke message for session: $SESSION_ID"
cargo run -p expressways-nanobot-system -- \
  --transport "$TRANSPORT" \
  --address "$ADDRESS" \
  ingest \
  --token-file "$TOKEN_FILE" \
  --session-id "$SESSION_ID" \
  --channel smoke \
  --account-id smoke \
  --sender-id smoke-user \
  --text "smoke failover test"

echo "Running runtime once with intentionally broken primary provider base URL..."
cargo run -p expressways-nanobot-system -- \
  --transport "$TRANSPORT" \
  --address "$ADDRESS" \
  run-runtime \
  --token-file "$TOKEN_FILE" \
  --state-dir "$STATE_DIR" \
  --inbound-topic "$INBOUND_TOPIC" \
  --runtime-events-topic "$RUNTIME_EVENTS_TOPIC" \
  --provider "$PRIMARY_PROVIDER" \
  --provider-base-url "$PRIMARY_BAD_BASE_URL" \
  --provider-model "smoke-primary-model" \
  --provider-api-key "smoke-primary-key" \
  --provider-max-attempts 1 \
  --provider-circuit-failure-threshold 1 \
  --provider-circuit-cooldown-seconds 5 \
  --provider-failover \
  --fallback-provider-model "$FALLBACK_MODEL" \
  --fallback-provider-api-key "$FALLBACK_KEY" \
  --once true

echo "Summarizing provider runtime events for session: $SESSION_ID"
SUMMARY="$(
  cargo run -p expressways-nanobot-system -- \
    --transport "$TRANSPORT" \
    --address "$ADDRESS" \
    summarize-provider-events \
    --token-file "$TOKEN_FILE" \
    --runtime-events-topic "$RUNTIME_EVENTS_TOPIC" \
    --session-id "$SESSION_ID" \
    --offset 0 \
    --limit 500
)"
echo "$SUMMARY"

if ! grep -q '"provider_failover_attempt"' <<<"$SUMMARY"; then
  echo "Smoke test failed: missing provider_failover_attempt"
  exit 1
fi
if ! grep -q '"provider_failover_succeeded"' <<<"$SUMMARY"; then
  echo "Smoke test failed: missing provider_failover_succeeded"
  exit 1
fi
if ! grep -q '"provider_error"' <<<"$SUMMARY"; then
  echo "Smoke test failed: missing provider_error"
  exit 1
fi

echo "Smoke test passed: failover attempt and success were observed."
