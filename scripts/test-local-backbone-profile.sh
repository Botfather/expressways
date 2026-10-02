#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BROKER_PORT="${EXPRESSWAYS_BACKBONE_BROKER_PORT:-37766}"
GATEWAY_PORT="${EXPRESSWAYS_BACKBONE_GATEWAY_PORT:-38790}"
BRIDGE_PORT="${EXPRESSWAYS_BACKBONE_BRIDGE_PORT:-38891}"
EGRESS_PORT="${EXPRESSWAYS_BACKBONE_EGRESS_PORT:-39990}"
PROVIDER_PORT="${EXPRESSWAYS_BACKBONE_PROVIDER_PORT:-39991}"
BROKER_ADDRESS="127.0.0.1:${BROKER_PORT}"
GATEWAY_URL="http://127.0.0.1:${GATEWAY_PORT}"
BRIDGE_URL="http://127.0.0.1:${BRIDGE_PORT}"
EGRESS_URL="http://127.0.0.1:${EGRESS_PORT}"
PROVIDER_URL="http://127.0.0.1:${PROVIDER_PORT}"
PROFILE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/expressways-backbone-profile.XXXXXX")"
CONFIG_PATH="$PROFILE_ROOT/expressways.toml"
PRIVATE_KEY="$PROFILE_ROOT/issuer.private"
PUBLIC_KEY="$PROFILE_ROOT/issuer.public"
TOKEN_FILE="$PROFILE_ROOT/developer.token"
WORKSPACE_ROOT="$PROFILE_ROOT/workspace"
TOOL_FILE="$WORKSPACE_ROOT/tool-proof.txt"
EGRESS_RECORDS="$PROFILE_ROOT/egress.jsonl"
PROVIDER_RECORDS="$PROFILE_ROOT/provider.jsonl"
BRIDGE_STATE="$PROFILE_ROOT/bridge-state.json"
INGRESS_BEARER="backbone-ingress"
EGRESS_BEARER="backbone-egress"
SERVER_PID=""
GATEWAY_PID=""
BRIDGE_PID=""
ORCHESTRATOR_PID=""
WORKER_PID=""
EGRESS_PID=""
PROVIDER_PID=""

stop_pid() {
  local pid="${1:-}"
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

cleanup() {
  stop_pid "$WORKER_PID"
  stop_pid "$ORCHESTRATOR_PID"
  stop_pid "$BRIDGE_PID"
  stop_pid "$GATEWAY_PID"
  stop_pid "$SERVER_PID"
  stop_pid "$EGRESS_PID"
  stop_pid "$PROVIDER_PID"
  rm -rf "$PROFILE_ROOT"
}
trap cleanup EXIT INT TERM

fail_with_logs() {
  printf 'Local backbone profile failed: %s\n' "$1" >&2
  local label path
  for label in broker gateway bridge orchestrator worker egress provider; do
    path="$PROFILE_ROOT/$label.log"
    if [[ -f "$path" ]]; then
      printf '%s\n' "--- $label log ---" >&2
      tail -n 80 "$path" >&2 || true
    fi
  done
  for path in "$EGRESS_RECORDS" "$PROVIDER_RECORDS"; do
    if [[ -f "$path" ]]; then
      printf '%s\n' "--- $(basename "$path") ---" >&2
      tail -n 20 "$path" >&2 || true
    fi
  done
  exit 1
}
trap 'fail_with_logs "unexpected command failure at line $LINENO"' ERR

for command in cargo curl jq python3 sed seq; do
  command -v "$command" >/dev/null 2>&1 || fail_with_logs "required command not found: $command"
done

mkdir -p \
  "$PROFILE_ROOT/data" "$PROFILE_ROOT/audit" "$PROFILE_ROOT/registry" \
  "$PROFILE_ROOT/auth" "$PROFILE_ROOT/tmp" "$PROFILE_ROOT/orchestrator" \
  "$PROFILE_ROOT/worker" "$WORKSPACE_ROOT"
printf 'BACKBONE_TOOL_EXECUTED\n' > "$TOOL_FILE"

cargo build -q \
  -p expressways-server \
  -p expressways-client \
  -p expressways-http-gateway \
  -p expressways-orchestrator \
  -p expressways-interop-bridge \
  -p expressways-nanobot-system --bins

target/debug/expresswaysctl generate-keypair \
  --key-id dev --private-key "$PRIVATE_KEY" --public-key "$PUBLIC_KEY" >/dev/null
sed \
  -e "s|listen_addr = \"127.0.0.1:7766\"|listen_addr = \"${BROKER_ADDRESS}\"|" \
  -e "s|socket_path = \"./tmp/expressways.sock\"|socket_path = \"${PROFILE_ROOT}/tmp/expressways.sock\"|" \
  -e "s|data_dir = \"./var/data\"|data_dir = \"${PROFILE_ROOT}/data\"|" \
  -e "s|path = \"./var/audit/audit.jsonl\"|path = \"${PROFILE_ROOT}/audit/audit.jsonl\"|" \
  -e "s|path = \"./var/registry/agents.json\"|path = \"${PROFILE_ROOT}/registry/agents.json\"|" \
  -e "s|revocation_path = \"./var/auth/revocations.json\"|revocation_path = \"${PROFILE_ROOT}/auth/revocations.json\"|" \
  -e "s|public_key_path = \"./var/auth/issuer.public\"|public_key_path = \"${PUBLIC_KEY}\"|" \
  -e 's|publish_requests_per_window = 20|publish_requests_per_window = 500|' \
  -e 's|consume_requests_per_window = 20|consume_requests_per_window = 500|' \
  configs/expressways.example.toml > "$CONFIG_PATH"
target/debug/expresswaysctl issue-token \
  --key-id dev --private-key "$PRIVATE_KEY" --principal local:developer \
  --audience expressways --expires-in-seconds 3600 \
  --scope system:broker:health --scope 'topic:*:admin,publish,consume' \
  --scope 'artifact:*:publish,consume,admin' --scope 'registry:agents*:admin' \
  --output "$TOKEN_FILE" >/dev/null
CAPABILITY_TOKEN="$(< "$TOKEN_FILE")"

ctl() {
  target/debug/expresswaysctl --transport tcp --address "$BROKER_ADDRESS" "$@"
}

python3 scripts/fixtures/adapter-egress-server.py \
  --port "$EGRESS_PORT" --bearer "$EGRESS_BEARER" --log "$EGRESS_RECORDS" \
  >>"$PROFILE_ROOT/egress.log" 2>&1 &
EGRESS_PID=$!
python3 scripts/fixtures/openai-tool-provider.py \
  --port "$PROVIDER_PORT" --read-path "$TOOL_FILE" --log "$PROVIDER_RECORDS" \
  >>"$PROFILE_ROOT/provider.log" 2>&1 &
PROVIDER_PID=$!

for url in "$EGRESS_URL/health" "$PROVIDER_URL/health"; do
  for _ in $(seq 1 60); do
    curl -fsS "$url" >/dev/null 2>&1 && break
    sleep 0.1
  done
  curl -fsS "$url" >/dev/null || fail_with_logs "fixture did not become ready: $url"
done

target/debug/expressways-server --config "$CONFIG_PATH" >>"$PROFILE_ROOT/broker.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 80); do
  ctl health --token-file "$TOKEN_FILE" >/dev/null 2>&1 && break
  kill -0 "$SERVER_PID" 2>/dev/null || fail_with_logs "broker exited during startup"
  sleep 0.1
done
ctl health --token-file "$TOKEN_FILE" >/dev/null || fail_with_logs "broker did not become ready"

target/debug/expressways-http-gateway \
  --listen "127.0.0.1:${GATEWAY_PORT}" --broker-address "$BROKER_ADDRESS" \
  >>"$PROFILE_ROOT/gateway.log" 2>&1 &
GATEWAY_PID=$!
for _ in $(seq 1 60); do
  curl -fsS -H "Authorization: Bearer ${CAPABILITY_TOKEN}" "$GATEWAY_URL/v1/health" \
    | jq -e '.type == "health"' >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS -H "Authorization: Bearer ${CAPABILITY_TOKEN}" "$GATEWAY_URL/v1/health" \
  | jq -e '.type == "health"' >/dev/null || fail_with_logs "HTTP API did not become ready"

start_bridge() {
  target/debug/expressways-interop-bridge \
    --transport tcp --address "$BROKER_ADDRESS" --listen "127.0.0.1:${BRIDGE_PORT}" \
    --token-file "$TOKEN_FILE" --ingress-bearer "$INGRESS_BEARER" \
    --egress-url "$EGRESS_URL/v1/replies" --egress-bearer "$EGRESS_BEARER" \
    --state-path "$BRIDGE_STATE" --egress-poll-interval-ms 50 \
    >>"$PROFILE_ROOT/bridge.log" 2>&1 &
  BRIDGE_PID=$!
  for _ in $(seq 1 60); do
    local status
    status="$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
      -H 'Content-Type: application/json' --data '{}' \
      "$BRIDGE_URL/v1/webhook/handoff" 2>/dev/null || true)"
    [[ "$status" == "401" ]] && return
    sleep 0.1
  done
  fail_with_logs "bridge did not become ready"
}

start_worker() {
  target/debug/expressways-nanobot-system \
    --transport tcp --address "$BROKER_ADDRESS" run-runtime \
    --token-file "$TOKEN_FILE" --agent-id nanobot-interop \
    --state-dir "$PROFILE_ROOT/worker" --interop-worker \
    --provider openai --provider-base-url "$PROVIDER_URL/v1" \
    --provider-model backbone-fixture --provider-api-key fixture-key \
    --provider-max-attempts 1 --poll-interval-ms 50 \
    --workspace-root "$WORKSPACE_ROOT" \
    >>"$PROFILE_ROOT/worker.log" 2>&1 &
  WORKER_PID=$!
}

start_bridge
target/debug/expressways-orchestrator \
  --transport tcp --address "$BROKER_ADDRESS" supervise --token-file "$TOKEN_FILE" \
  --state-path "$PROFILE_ROOT/orchestrator/state.json" \
  --tasks-topic interop.chat.requests --task-events-topic interop.chat.results \
  --poll-interval-ms 50 >>"$PROFILE_ROOT/orchestrator.log" 2>&1 &
ORCHESTRATOR_PID=$!
start_worker

post_message() {
  local id="$1"
  jq -cn --arg id "$id" '{
    schema_version:"interop.chat.handoff.v1",
    idempotency_key:$id,
    source_runtime:"pigeon",
    session:{session_id:"backbone-chat",channel:"whatsapp",account_id:"local",sender_id:"user",message_id:$id},
    message:{text:"Read the proof file and report the result.",attachments:[]},
    skill:"interop-chat"
  }' | curl -fsS -X POST \
    -H "Authorization: Bearer ${INGRESS_BEARER}" -H 'Content-Type: application/json' \
    --data-binary @- "$BRIDGE_URL/v1/webhook/handoff"
}

wait_for_deliveries() {
  local expected="$1"
  for _ in $(seq 1 160); do
    local count=0
    [[ -f "$EGRESS_RECORDS" ]] && count="$(wc -l < "$EGRESS_RECORDS" | tr -d ' ')"
    [[ "$count" -ge "$expected" ]] && return
    kill -0 "$WORKER_PID" 2>/dev/null || fail_with_logs "Nanobot interop worker exited"
    sleep 0.1
  done
  fail_with_logs "timed out waiting for $expected delivered replies"
}

FIRST_RESPONSE="$(post_message backbone-message-1)"
FIRST_TASK="$(jq -r '.task_id' <<<"$FIRST_RESPONSE")"
wait_for_deliveries 1
jq -s -e --arg task "$FIRST_TASK" '
  length == 1
  and .[0].envelope.in_reply_to_task_id == $task
  and .[0].envelope.delivery_id == ("interop-reply:" + $task)
  and (. [0].envelope.message.text | contains("BACKBONE_TOOL_EXECUTED"))
' "$EGRESS_RECORDS" >/dev/null || fail_with_logs "first correlated tool-backed reply was invalid"
jq -s -e 'length == 2 and .[0].saw_tool_result == false and .[1].saw_tool_result == true' \
  "$PROVIDER_RECORDS" >/dev/null || fail_with_logs "provider/tool loop was not exercised"

# Durable assignment recovery: accept work while the agent is down, then
# restart the same worker state and require exactly one correlated delivery.
stop_pid "$WORKER_PID"
WORKER_PID=""
SECOND_RESPONSE="$(post_message backbone-message-2)"
SECOND_TASK="$(jq -r '.task_id' <<<"$SECOND_RESPONSE")"
sleep 0.3
[[ "$(wc -l < "$EGRESS_RECORDS" | tr -d ' ')" == "1" ]] \
  || fail_with_logs "work was delivered while the only agent was stopped"
start_worker
wait_for_deliveries 2
jq -s -e --arg task "$SECOND_TASK" '
  length == 2
  and .[1].envelope.in_reply_to_task_id == $task
  and .[1].envelope.delivery_id == ("interop-reply:" + $task)
' "$EGRESS_RECORDS" >/dev/null || fail_with_logs "queued work was not recovered exactly once"

# Durable egress cursor recovery: restarting the bridge after both destination
# acknowledgements must not replay either reply.
stop_pid "$BRIDGE_PID"
BRIDGE_PID=""
start_bridge
sleep 0.5
[[ "$(wc -l < "$EGRESS_RECORDS" | tr -d ' ')" == "2" ]] \
  || fail_with_logs "bridge restart duplicated an acknowledged reply"

printf 'Local backbone profile passed: HTTP API, durable chat ingress, orchestrated LLM tool execution, correlated reply delivery, worker recovery, and bridge recovery.\n'
