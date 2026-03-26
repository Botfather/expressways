#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TRANSPORT="${TRANSPORT:-tcp}"
ADDRESS="${ADDRESS:-127.0.0.1:7766}"
TOKEN_FILE="${TOKEN_FILE:-./var/auth/developer.token}"
STATE_DIR="${STATE_DIR:-./var/agent/nanobot-runtime-streaming-smoke}"
INBOUND_TOPIC="${INBOUND_TOPIC:-nanobot.inbound}"
OUTBOUND_TOPIC="${OUTBOUND_TOPIC:-nanobot.outbound}"
OUTBOUND_STREAM_TOPIC="${OUTBOUND_STREAM_TOPIC:-nanobot.outbound.stream}"
SESSION_ID="${SESSION_ID:-smoke-stream-$(date +%s)}"
USER_TEXT="${USER_TEXT:-streaming smoke test}"
LIMIT="${LIMIT:-500}"
PROVIDER="${PROVIDER:-openai}"

if [[ ! -f "$TOKEN_FILE" ]]; then
  echo "Missing token file: $TOKEN_FILE"
  exit 1
fi

MODEL=""
API_KEY=""
if [[ "$PROVIDER" == "openai" ]]; then
  MODEL="${MODEL:-gpt-4o-mini}"
  API_KEY="${OPENAI_API_KEY:-}"
  if [[ -z "$API_KEY" ]]; then
    echo "Set OPENAI_API_KEY to run OpenAI streaming smoke."
    exit 1
  fi
elif [[ "$PROVIDER" == "anthropic" ]]; then
  MODEL="${MODEL:-claude-3-5-sonnet-latest}"
  API_KEY="${ANTHROPIC_API_KEY:-}"
  if [[ -z "$API_KEY" ]]; then
    echo "Set ANTHROPIC_API_KEY to run Anthropic streaming smoke."
    exit 1
  fi
else
  echo "Unsupported PROVIDER: $PROVIDER (use openai or anthropic)"
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
  --text "$USER_TEXT"

echo "Running runtime once with provider streaming enabled..."
cargo run -p expressways-nanobot-system -- \
  --transport "$TRANSPORT" \
  --address "$ADDRESS" \
  run-runtime \
  --token-file "$TOKEN_FILE" \
  --state-dir "$STATE_DIR" \
  --inbound-topic "$INBOUND_TOPIC" \
  --outbound-topic "$OUTBOUND_TOPIC" \
  --outbound-stream-topic "$OUTBOUND_STREAM_TOPIC" \
  --provider "$PROVIDER" \
  --provider-model "$MODEL" \
  --provider-api-key "$API_KEY" \
  --provider-streaming true \
  --provider-max-attempts 1 \
  --once true

echo "Consuming stream topic: $OUTBOUND_STREAM_TOPIC"
STREAM_OUTPUT="$(
  cargo run -p expressways-client --bin expresswaysctl -- \
    --transport "$TRANSPORT" \
    --address "$ADDRESS" \
    consume \
    --token-file "$TOKEN_FILE" \
    --topic "$OUTBOUND_STREAM_TOPIC" \
    --offset 0 \
    --limit "$LIMIT"
)"
echo "$STREAM_OUTPUT"

echo "Reading outbound response payloads from topic: $OUTBOUND_TOPIC"
OUTBOUND_OUTPUT="$(
  cargo run -p expressways-nanobot-system -- \
    --transport "$TRANSPORT" \
    --address "$ADDRESS" \
    tail-outbound \
    --token-file "$TOKEN_FILE" \
    --outbound-topic "$OUTBOUND_TOPIC" \
    --offset 0 \
    --limit "$LIMIT" \
    --raw true
)"
echo "$OUTBOUND_OUTPUT"

if ! grep -q "$SESSION_ID" <<<"$STREAM_OUTPUT"; then
  echo "Smoke test failed: stream topic output does not include session id $SESSION_ID"
  exit 1
fi

if ! grep -Eq '\\"done\\":\\s*true|"done"\\s*:\\s*true' <<<"$STREAM_OUTPUT"; then
  echo "Smoke test failed: stream output missing done=true marker"
  exit 1
fi

if ! grep -Eq '\\"text_delta\\":\\s*\\"[^\\"]+|"text_delta"\\s*:\\s*"[^"]+' <<<"$STREAM_OUTPUT"; then
  echo "Smoke test failed: stream output missing non-empty text_delta chunk"
  exit 1
fi

if ! grep -q "$SESSION_ID" <<<"$OUTBOUND_OUTPUT"; then
  echo "Smoke test failed: outbound response missing session id $SESSION_ID"
  exit 1
fi

if ! grep -Eq '\\"role\\":\\"assistant\\"|"role"\\s*:\\s*"assistant"' <<<"$OUTBOUND_OUTPUT"; then
  echo "Smoke test failed: outbound response missing assistant role"
  exit 1
fi

echo "Smoke test passed: streaming chunks and final assistant response were observed."
