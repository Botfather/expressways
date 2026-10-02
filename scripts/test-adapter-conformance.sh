#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BROKER_PORT="${EXPRESSWAYS_ADAPTER_BROKER_PORT:-27766}"
BRIDGE_PORT="${EXPRESSWAYS_ADAPTER_BRIDGE_PORT:-28891}"
EGRESS_PORT="${EXPRESSWAYS_ADAPTER_EGRESS_PORT:-29990}"
BROKER_ADDRESS="127.0.0.1:${BROKER_PORT}"
BRIDGE_URL="http://127.0.0.1:${BRIDGE_PORT}"
EGRESS_URL="http://127.0.0.1:${EGRESS_PORT}"
CONFORMANCE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/expressways-adapter-conformance.XXXXXX")"
CONFIG_PATH="$CONFORMANCE_ROOT/expressways.toml"
PRIVATE_KEY="$CONFORMANCE_ROOT/issuer.private"
PUBLIC_KEY="$CONFORMANCE_ROOT/issuer.public"
TOKEN_FILE="$CONFORMANCE_ROOT/developer.token"
BRIDGE_STATE="$CONFORMANCE_ROOT/bridge-state.json"
MEDIA_FILE="$CONFORMANCE_ROOT/media.bin"
EGRESS_RECORDS="$CONFORMANCE_ROOT/egress.jsonl"
SERVER_LOG="$CONFORMANCE_ROOT/server.log"
BRIDGE_LOG="$CONFORMANCE_ROOT/bridge.log"
ORCHESTRATOR_LOG="$CONFORMANCE_ROOT/orchestrator.log"
EGRESS_LOG="$CONFORMANCE_ROOT/egress.log"
INGRESS_BEARER="adapter-ingress-conformance"
EGRESS_BEARER="adapter-egress-conformance"
SERVER_PID=""
BRIDGE_PID=""
ORCHESTRATOR_PID=""
EGRESS_PID=""

stop_pid() {
  local pid="${1:-}"
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

cleanup() {
  stop_pid "$ORCHESTRATOR_PID"
  stop_pid "$BRIDGE_PID"
  stop_pid "$SERVER_PID"
  stop_pid "$EGRESS_PID"
  rm -rf "$CONFORMANCE_ROOT"
}
trap cleanup EXIT INT TERM

fail_with_logs() {
  printf 'Adapter conformance failed: %s\n' "$1" >&2
  for entry in \
    "broker:$SERVER_LOG" \
    "bridge:$BRIDGE_LOG" \
    "orchestrator:$ORCHESTRATOR_LOG" \
    "egress:$EGRESS_LOG"; do
    local label="${entry%%:*}"
    local path="${entry#*:}"
    if [[ -f "$path" ]]; then
      printf '%s\n' "--- $label log ---" >&2
      tail -n 80 "$path" >&2 || true
    fi
  done
  exit 1
}

trap 'fail_with_logs "unexpected command failure at line $LINENO"' ERR

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail_with_logs "required command not found: $1"
}

for command in cargo curl jq python3 sed seq shasum; do
  require_command "$command"
done

mkdir -p \
  "$CONFORMANCE_ROOT/data" \
  "$CONFORMANCE_ROOT/audit" \
  "$CONFORMANCE_ROOT/registry" \
  "$CONFORMANCE_ROOT/auth" \
  "$CONFORMANCE_ROOT/tmp" \
  "$CONFORMANCE_ROOT/orchestrator"

cargo build -q \
  -p expressways-server \
  -p expressways-client \
  -p expressways-orchestrator \
  -p expressways-interop-bridge \
  --bins

target/debug/expresswaysctl generate-keypair \
  --key-id dev \
  --private-key "$PRIVATE_KEY" \
  --public-key "$PUBLIC_KEY" >/dev/null

sed \
  -e "s|listen_addr = \"127.0.0.1:7766\"|listen_addr = \"${BROKER_ADDRESS}\"|" \
  -e "s|socket_path = \"./tmp/expressways.sock\"|socket_path = \"${CONFORMANCE_ROOT}/tmp/expressways.sock\"|" \
  -e "s|data_dir = \"./var/data\"|data_dir = \"${CONFORMANCE_ROOT}/data\"|" \
  -e "s|path = \"./var/audit/audit.jsonl\"|path = \"${CONFORMANCE_ROOT}/audit/audit.jsonl\"|" \
  -e "s|path = \"./var/registry/agents.json\"|path = \"${CONFORMANCE_ROOT}/registry/agents.json\"|" \
  -e "s|revocation_path = \"./var/auth/revocations.json\"|revocation_path = \"${CONFORMANCE_ROOT}/auth/revocations.json\"|" \
  -e "s|public_key_path = \"./var/auth/issuer.public\"|public_key_path = \"${PUBLIC_KEY}\"|" \
  configs/expressways.example.toml > "$CONFIG_PATH"

target/debug/expresswaysctl issue-token \
  --key-id dev \
  --private-key "$PRIVATE_KEY" \
  --principal local:developer \
  --audience expressways \
  --expires-in-seconds 3600 \
  --scope system:broker:health \
  --scope 'topic:*:admin,publish,consume' \
  --scope 'artifact:*:publish,consume,admin' \
  --scope 'registry:agents*:admin' \
  --output "$TOKEN_FILE" >/dev/null

ctl() {
  target/debug/expresswaysctl --transport tcp --address "$BROKER_ADDRESS" "$@"
}

python3 scripts/fixtures/adapter-egress-server.py \
  --port "$EGRESS_PORT" \
  --bearer "$EGRESS_BEARER" \
  --log "$EGRESS_RECORDS" \
  --fail-first 1 >>"$EGRESS_LOG" 2>&1 &
EGRESS_PID=$!

for _ in $(seq 1 60); do
  if curl -fsS "$EGRESS_URL/health" >/dev/null 2>&1; then break; fi
  kill -0 "$EGRESS_PID" 2>/dev/null || fail_with_logs "egress fixture exited during startup"
  sleep 0.1
done
curl -fsS "$EGRESS_URL/health" >/dev/null || fail_with_logs "egress fixture did not become ready"

target/debug/expressways-server --config "$CONFIG_PATH" >>"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 80); do
  if ctl health --token-file "$TOKEN_FILE" >/dev/null 2>&1; then break; fi
  kill -0 "$SERVER_PID" 2>/dev/null || fail_with_logs "broker exited during startup"
  sleep 0.1
done
ctl health --token-file "$TOKEN_FILE" >/dev/null 2>&1 || fail_with_logs "broker did not become ready"

start_bridge() {
  target/debug/expressways-interop-bridge \
    --transport tcp \
    --address "$BROKER_ADDRESS" \
    --listen "127.0.0.1:${BRIDGE_PORT}" \
    --token-file "$TOKEN_FILE" \
    --ingress-bearer "$INGRESS_BEARER" \
    --egress-url "$EGRESS_URL/v1/replies" \
    --egress-bearer "$EGRESS_BEARER" \
    --state-path "$BRIDGE_STATE" \
    --egress-poll-interval-ms 100 >>"$BRIDGE_LOG" 2>&1 &
  BRIDGE_PID=$!
  for _ in $(seq 1 60); do
    local status
    status="$(curl -sS -o /dev/null -w '%{http_code}' \
      -X POST -H 'Content-Type: application/json' \
      --data '{}' "$BRIDGE_URL/v1/webhook/handoff" 2>/dev/null || true)"
    if [[ "$status" == "401" ]]; then return; fi
    kill -0 "$BRIDGE_PID" 2>/dev/null || fail_with_logs "bridge exited during startup"
    sleep 0.1
  done
  fail_with_logs "bridge did not become ready"
}

stop_bridge() {
  stop_pid "$BRIDGE_PID"
  BRIDGE_PID=""
}

start_bridge

unauthorized_status="$(curl -sS -o /dev/null -w '%{http_code}' \
  -X POST -H 'Content-Type: application/json' --data '{}' \
  "$BRIDGE_URL/v1/webhook/handoff")"
[[ "$unauthorized_status" == "401" ]] \
  || fail_with_logs "unauthenticated ingress returned HTTP $unauthorized_status"

dd if=/dev/zero of="$MEDIA_FILE" bs=1048576 count=2 2>/dev/null
MEDIA_SHA="$(shasum -a 256 "$MEDIA_FILE" | awk '{print $1}')"
artifact_response="$(curl -fsS -X POST \
  -H "Authorization: Bearer ${INGRESS_BEARER}" \
  -H 'Content-Type: application/octet-stream' \
  -H 'X-Artifact-Id: adapter-conformance-media' \
  -H "X-Content-Sha256: ${MEDIA_SHA}" \
  --data-binary "@$MEDIA_FILE" \
  "$BRIDGE_URL/v1/artifacts")"
jq -e --arg sha "$MEDIA_SHA" \
  '.artifact_id == "adapter-conformance-media" and .byte_length == 2097152 and .sha256 == $sha' \
  <<<"$artifact_response" >/dev/null \
  || fail_with_logs "raw artifact upload response was invalid"

artifact_retry_response="$(curl -fsS -X POST \
  -H "Authorization: Bearer ${INGRESS_BEARER}" \
  -H 'Content-Type: application/octet-stream' \
  -H 'X-Artifact-Id: adapter-conformance-media' \
  -H "X-Content-Sha256: ${MEDIA_SHA}" \
  --data-binary "@$MEDIA_FILE" \
  "$BRIDGE_URL/v1/artifacts")"
jq -e --arg sha "$MEDIA_SHA" \
  '.artifact_id == "adapter-conformance-media" and .byte_length == 2097152 and .sha256 == $sha' \
  <<<"$artifact_retry_response" >/dev/null \
  || fail_with_logs "identical named artifact retry was not idempotent"

unauthorized_download_status="$(curl -sS -o /dev/null -w '%{http_code}' \
  "$BRIDGE_URL/v1/artifacts/adapter-conformance-media")"
[[ "$unauthorized_download_status" == "401" ]] \
  || fail_with_logs "unauthenticated bridge artifact download returned HTTP $unauthorized_download_status"
curl -fsS \
  -H "Authorization: Bearer ${INGRESS_BEARER}" \
  "$BRIDGE_URL/v1/artifacts/adapter-conformance-media" \
  -o "$CONFORMANCE_ROOT/downloaded-media.bin"
[[ "$(shasum -a 256 "$CONFORMANCE_ROOT/downloaded-media.bin" | awk '{print $1}')" == "$MEDIA_SHA" ]] \
  || fail_with_logs "bridge artifact download did not preserve media bytes"

webhook_payload() {
  local key="$1"
  local message_id="$2"
  local text="$3"
  jq -cn \
    --arg key "$key" \
    --arg message_id "$message_id" \
    --arg text "$text" \
    --arg sha "$MEDIA_SHA" \
    '{
      schema_version:"interop.chat.handoff.v1",
      idempotency_key:$key,
      source_runtime:"pigeon",
      session:{
        session_id:"conversation-1",
        channel:"whatsapp",
        account_id:"account-1",
        sender_id:"sender-1",
        message_id:$message_id
      },
      message:{
        text:$text,
        attachments:[{
          name:"media.bin",
          content_type:"application/octet-stream",
          artifact_id:"adapter-conformance-media",
          byte_length:2097152,
          sha256:$sha
        }]
      },
      skill:"chat.reply"
    }'
}

post_webhook() {
  curl -fsS -X POST \
    -H "Authorization: Bearer ${INGRESS_BEARER}" \
    -H 'Content-Type: application/json' \
    --data "$1" \
    "$BRIDGE_URL/v1/webhook/handoff"
}

first_payload="$(webhook_payload adapter-message-1 message-1 first)"
first_response="$(post_webhook "$first_payload")"
replay_response="$(post_webhook "$first_payload")"
second_response="$(post_webhook "$(webhook_payload adapter-message-2 message-2 second)")"

FIRST_TASK_ID="$(jq -r '.task_id' <<<"$first_response")"
[[ "$FIRST_TASK_ID" == "$(jq -r '.task_id' <<<"$replay_response")" ]] \
  || fail_with_logs "replayed ingress produced a different task id"
SECOND_TASK_ID="$(jq -r '.task_id' <<<"$second_response")"
[[ "$FIRST_TASK_ID" != "$SECOND_TASK_ID" ]] \
  || fail_with_logs "distinct ingress identities produced the same task id"

requests="$(ctl consume --token-file "$TOKEN_FILE" --topic interop.chat.requests --offset 0 --limit 10)"
jq -e --arg first "$FIRST_TASK_ID" --arg second "$SECOND_TASK_ID" --arg sha "$MEDIA_SHA" '
  (.messages | length) == 3
  and ([.messages[].payload | fromjson | .task_id] == [$first, $first, $second])
  and ([.messages[].payload | fromjson | .requirements.affinity_key] | unique | length) == 1
  and all(.messages[].payload | fromjson;
    .payload.message.attachments[0].artifact_id == "adapter-conformance-media"
    and .payload.message.attachments[0].sha256 == $sha)
' <<<"$requests" >/dev/null || fail_with_logs "ingress replay, affinity, or artifact references were invalid"

ctl register-agent \
  --token-file "$TOKEN_FILE" \
  --agent-id adapter-conformance-agent \
  --display-name 'Adapter Conformance Agent' \
  --version 1.0.0 \
  --summary 'live adapter ordering probe' \
  --skill chat.reply \
  --subscribe topic:interop.chat.requests \
  --publish-topic topic:interop.chat.replies \
  --endpoint-address 127.0.0.1:39001 \
  --ttl-seconds 120 >/dev/null

target/debug/expressways-orchestrator \
  --transport tcp \
  --address "$BROKER_ADDRESS" \
  supervise \
  --token-file "$TOKEN_FILE" \
  --state-path "$CONFORMANCE_ROOT/orchestrator/state.json" \
  --tasks-topic interop.chat.requests \
  --task-events-topic interop.chat.results \
  --poll-interval-ms 50 >>"$ORCHESTRATOR_LOG" 2>&1 &
ORCHESTRATOR_PID=$!

events='{"messages":[]}'
for _ in $(seq 1 100); do
  events="$(ctl consume --token-file "$TOKEN_FILE" --topic interop.chat.results --offset 0 --limit 100 2>/dev/null || printf '{"messages":[]}')"
  if jq -e --arg task "$FIRST_TASK_ID" \
    'any((.messages // [])[].payload | fromjson; .task_id == $task and .status == "assigned")' \
    <<<"$events" >/dev/null; then break; fi
  kill -0 "$ORCHESTRATOR_PID" 2>/dev/null || fail_with_logs "orchestrator exited before first assignment"
  sleep 0.1
done

assigned_before_completion="$(jq '[.messages[].payload | fromjson | select(.status == "assigned")] | length' <<<"$events")"
[[ "$assigned_before_completion" == "1" ]] \
  || fail_with_logs "affinity ordering allowed more than one assignment before completion"
ASSIGNMENT_ID="$(jq -r --arg task "$FIRST_TASK_ID" \
  '.messages[].payload | fromjson | select(.task_id == $task and .status == "assigned") | .assignment_id' \
  <<<"$events" | head -n 1)"
[[ -n "$ASSIGNMENT_ID" && "$ASSIGNMENT_ID" != "null" ]] \
  || fail_with_logs "first assignment id was missing"

ctl report-task \
  --token-file "$TOKEN_FILE" \
  --topic interop.chat.results \
  --task-id "$FIRST_TASK_ID" \
  --assignment-id "$ASSIGNMENT_ID" \
  --agent-id adapter-conformance-agent \
  --status completed \
  --attempt 1 >/dev/null

for _ in $(seq 1 100); do
  events="$(ctl consume --token-file "$TOKEN_FILE" --topic interop.chat.results --offset 0 --limit 100)"
  if jq -e --arg task "$SECOND_TASK_ID" \
    'any((.messages // [])[].payload | fromjson; .task_id == $task and .status == "assigned")' \
    <<<"$events" >/dev/null; then break; fi
  sleep 0.1
done
jq -e --arg task "$SECOND_TASK_ID" \
  'any((.messages // [])[].payload | fromjson; .task_id == $task and .status == "assigned")' \
  <<<"$events" >/dev/null || fail_with_logs "second affinity task was not released after completion"
stop_pid "$ORCHESTRATOR_PID"
ORCHESTRATOR_PID=""

reply_payload() {
  local delivery_id="$1"
  local correlation_id="$2"
  local task_id="$3"
  jq -cn \
    --arg delivery_id "$delivery_id" \
    --arg correlation_id "$correlation_id" \
    --arg task_id "$task_id" \
    --arg sha "$MEDIA_SHA" \
    '{
      schema_version:"interop.chat.reply.v1",
      delivery_id:$delivery_id,
      correlation_id:$correlation_id,
      source_runtime:"adapter-conformance-agent",
      target_runtime:"pigeon",
      session:{
        session_id:"conversation-1",
        channel:"whatsapp",
        account_id:"account-1",
        sender_id:"sender-1",
        reply_to_message_id:"message-1"
      },
      in_reply_to_task_id:$task_id,
      message:{
        text:"conformance reply",
        attachments:[{
          name:"media.bin",
          content_type:"application/octet-stream",
          artifact_id:"adapter-conformance-media",
          byte_length:2097152,
          sha256:$sha
        }]
      },
      metadata:{conformance:true},
      created_at:"2026-01-01T00:00:00Z"
    }'
}

ctl publish \
  --token-file "$TOKEN_FILE" \
  --topic interop.chat.replies \
  --payload "$(reply_payload delivery-1 correlation-1 "$FIRST_TASK_ID")" >/dev/null

for _ in $(seq 1 100); do
  attempts="$(curl -fsS "$EGRESS_URL/health" | jq -r '.attempts')"
  [[ "$attempts" -ge 2 ]] && break
  sleep 0.1
done
[[ "$(curl -fsS "$EGRESS_URL/health" | jq -r '.attempts')" == "2" ]] \
  || fail_with_logs "failed destination delivery was not retried exactly once"
jq -s -e '
  length == 2
  and .[0].status == 503
  and .[1].status == 204
  and (.[0].idempotency_key == "delivery-1" and .[1].idempotency_key == "delivery-1")
  and all(.[]; .authorization == "Bearer adapter-egress-conformance")
' "$EGRESS_RECORDS" >/dev/null || fail_with_logs "egress retry or authentication contract was invalid"
jq -e '.cursors["interop.chat.replies"] == 1' "$BRIDGE_STATE" >/dev/null \
  || fail_with_logs "durable reply cursor did not advance after acknowledgement"

stop_bridge
attempts_before_restart="$(curl -fsS "$EGRESS_URL/health" | jq -r '.attempts')"
start_bridge
sleep 0.5
attempts_after_restart="$(curl -fsS "$EGRESS_URL/health" | jq -r '.attempts')"
[[ "$attempts_after_restart" == "$attempts_before_restart" ]] \
  || fail_with_logs "acknowledged reply was duplicated after bridge restart"

stop_bridge
ctl publish \
  --token-file "$TOKEN_FILE" \
  --topic interop.chat.replies \
  --payload "$(reply_payload delivery-2 correlation-2 "$SECOND_TASK_ID")" >/dev/null
start_bridge
for _ in $(seq 1 100); do
  attempts="$(curl -fsS "$EGRESS_URL/health" | jq -r '.attempts')"
  [[ "$attempts" -ge 3 ]] && break
  sleep 0.1
done
[[ "$(curl -fsS "$EGRESS_URL/health" | jq -r '.attempts')" == "3" ]] \
  || fail_with_logs "reply queued during downtime was not delivered once after restart"
jq -e '.cursors["interop.chat.replies"] == 2' "$BRIDGE_STATE" >/dev/null \
  || fail_with_logs "restart recovery did not persist the second reply cursor"

structured_payload="$(jq -cn '{
  schema_version:"interop.chat.handoff.v1",
  idempotency_key:"adapter-location-1",
  source_runtime:"pigeon",
  session:{
    session_id:"conversation-structured",
    channel:"whatsapp",
    account_id:"account-1",
    sender_id:"sender-1",
    message_id:"location-1"
  },
  message:{
    text:"   ",
    content:[{
      type:"location",
      data:{latitude:28.6139,longitude:77.2090}
    }]
  },
  skill:"chat.reply"
}')"
post_webhook "$structured_payload" >/dev/null
requests="$(ctl consume --token-file "$TOKEN_FILE" --topic interop.chat.requests --offset 0 --limit 20)"
jq -e 'any(.messages[].payload | fromjson; .payload.message.content[0].type == "location")' \
  <<<"$requests" >/dev/null \
  || fail_with_logs "structured content-only handoff was not preserved"

pressure_pids=()
for index in $(seq 1 40); do
  (
    payload="$(webhook_payload "pressure-$index" "pressure-$index" pressure)"
    curl -sS -o /dev/null -w '%{http_code}\n' \
      -X POST \
      -H "Authorization: Bearer ${INGRESS_BEARER}" \
      -H 'Content-Type: application/json' \
      --data "$payload" \
      "$BRIDGE_URL/v1/webhook/handoff" > "$CONFORMANCE_ROOT/backpressure-$index.status" \
      || printf '000\n' > "$CONFORMANCE_ROOT/backpressure-$index.status"
  ) &
  pressure_pids+=("$!")
done
for pressure_pid in "${pressure_pids[@]}"; do
  wait "$pressure_pid"
done

backpressure_rejections=0
for status_file in "$CONFORMANCE_ROOT"/backpressure-*.status; do
  if grep -qx '502' "$status_file"; then
    backpressure_rejections=$((backpressure_rejections + 1))
  fi
done
[[ "$backpressure_rejections" -gt 0 ]] \
  || fail_with_logs "parallel adapter load did not surface broker backpressure"

printf 'Adapter conformance passed: auth, replay, ordering, idempotent 2 MiB media upload/download, backpressure, retry, and restart recovery.\n'
