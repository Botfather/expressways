#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BROKER_PORT="${EXPRESSWAYS_CONFORMANCE_BROKER_PORT:-17766}"
GATEWAY_PORT="${EXPRESSWAYS_CONFORMANCE_GATEWAY_PORT:-18790}"
BROKER_ADDRESS="127.0.0.1:${BROKER_PORT}"
GATEWAY_URL="http://127.0.0.1:${GATEWAY_PORT}"
CONFORMANCE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/expressways-http-conformance.XXXXXX")"
CONFIG_PATH="$CONFORMANCE_ROOT/expressways.toml"
PRIVATE_KEY="$CONFORMANCE_ROOT/issuer.private"
PUBLIC_KEY="$CONFORMANCE_ROOT/issuer.public"
TOKEN_FILE="$CONFORMANCE_ROOT/developer.token"
UPLOAD_FILE="$CONFORMANCE_ROOT/upload.bin"
DOWNLOAD_FILE="$CONFORMANCE_ROOT/download.bin"
SSE_FILE="$CONFORMANCE_ROOT/registry.sse"
SERVER_LOG="$CONFORMANCE_ROOT/server.log"
GATEWAY_LOG="$CONFORMANCE_ROOT/gateway.log"
SERVER_PID=""
GATEWAY_PID=""

stop_stack() {
  if [[ -n "$GATEWAY_PID" ]]; then
    kill "$GATEWAY_PID" 2>/dev/null || true
    wait "$GATEWAY_PID" 2>/dev/null || true
    GATEWAY_PID=""
  fi
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
}

cleanup() {
  stop_stack
  rm -rf "$CONFORMANCE_ROOT"
}
trap cleanup EXIT INT TERM

fail_with_logs() {
  printf 'HTTP gateway conformance failed: %s\n' "$1" >&2
  if [[ -f "$SERVER_LOG" ]]; then
    printf '%s\n' '--- broker log ---' >&2
    tail -n 80 "$SERVER_LOG" >&2 || true
  fi
  if [[ -f "$GATEWAY_LOG" ]]; then
    printf '%s\n' '--- gateway log ---' >&2
    tail -n 80 "$GATEWAY_LOG" >&2 || true
  fi
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail_with_logs "required command not found: $1"
}

require_command curl
require_command jq
require_command sed

mkdir -p \
  "$CONFORMANCE_ROOT/data" \
  "$CONFORMANCE_ROOT/audit" \
  "$CONFORMANCE_ROOT/registry" \
  "$CONFORMANCE_ROOT/auth" \
  "$CONFORMANCE_ROOT/tmp"

cargo build -q \
  -p expressways-server \
  -p expressways-http-gateway \
  -p expressways-client --bin expresswaysctl

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

CAPABILITY_TOKEN="$(< "$TOKEN_FILE")"

start_stack() {
  target/debug/expressways-server --config "$CONFIG_PATH" >>"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!

  local attempt
  for attempt in $(seq 1 60); do
    if target/debug/expresswaysctl \
      --transport tcp \
      --address "$BROKER_ADDRESS" \
      health \
      --token-file "$TOKEN_FILE" >/dev/null 2>&1; then
      break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      fail_with_logs "broker exited during startup"
    fi
    sleep 0.1
  done
  if ! target/debug/expresswaysctl \
    --transport tcp \
    --address "$BROKER_ADDRESS" \
    health \
    --token-file "$TOKEN_FILE" >/dev/null 2>&1; then
    fail_with_logs "broker did not become healthy"
  fi

  target/debug/expressways-http-gateway \
    --listen "127.0.0.1:${GATEWAY_PORT}" \
    --broker-address "$BROKER_ADDRESS" >>"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!

  for attempt in $(seq 1 60); do
    if curl -fsS \
      -H "Authorization: Bearer ${CAPABILITY_TOKEN}" \
      "$GATEWAY_URL/v1/health" 2>/dev/null | jq -e '.type == "health"' >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "$GATEWAY_PID" 2>/dev/null; then
      fail_with_logs "HTTP gateway exited during startup"
    fi
    sleep 0.1
  done
  fail_with_logs "HTTP gateway did not become healthy"
}

auth_header=(-H "Authorization: Bearer ${CAPABILITY_TOKEN}")

start_stack

unauthorized_status="$(curl -sS -o /dev/null -w '%{http_code}' "$GATEWAY_URL/v1/health")"
[[ "$unauthorized_status" == "401" ]] || fail_with_logs "health without a bearer returned HTTP $unauthorized_status"

for topic in conformance.events tasks; do
  target/debug/expresswaysctl \
    --transport tcp \
    --address "$BROKER_ADDRESS" \
    create-topic \
    --token-file "$TOKEN_FILE" \
    --topic "$topic" >/dev/null
done

publish_response="$(curl -fsS -X POST \
  "${auth_header[@]}" \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/topics/conformance.events/messages" \
  --data '{"classification":"internal","payload":{"kind":"conformance.event","sequence":1}}')"
jq -e '.type == "publish_accepted" and .offset == 0' <<<"$publish_response" >/dev/null \
  || fail_with_logs "topic publish response was invalid"

consume_response="$(curl -fsS \
  "${auth_header[@]}" \
  "$GATEWAY_URL/v1/topics/conformance.events/messages?offset=0&limit=10")"
jq -e '.type == "messages" and (.messages | length) == 1 and .next_offset == 1' \
  <<<"$consume_response" >/dev/null || fail_with_logs "topic consume response was invalid"

task_response="$(curl -fsS -X POST \
  "${auth_header[@]}" \
  -H 'Content-Type: application/json' \
  -H 'X-Classification: internal' \
  "$GATEWAY_URL/v1/tasks" \
  --data '{"task_id":"http-conformance-task","task_type":"conformance","priority":0,"requirements":{"skill":"conformance","topic":null,"principal":null,"preferred_agents":[],"avoid_agents":[],"required_agent":null,"affinity_key":"conformance"},"payload":{"ok":true},"retry_policy":{"max_attempts":1,"timeout_seconds":30,"retry_delay_seconds":1},"submitted_at":"2026-01-01T00:00:00Z"}')"
jq -e '.type == "publish_accepted"' <<<"$task_response" >/dev/null \
  || fail_with_logs "task submission response was invalid"

printf 'Expressways artifact conformance\n' > "$UPLOAD_FILE"
artifact_response="$(curl -fsS -X POST \
  "${auth_header[@]}" \
  -H 'Content-Type: application/octet-stream' \
  -H 'X-Artifact-Id: http-conformance-artifact' \
  --data-binary "@$UPLOAD_FILE" \
  "$GATEWAY_URL/v1/artifacts")"
jq -e '.type == "artifact_stored" and .artifact.artifact_id == "http-conformance-artifact"' \
  <<<"$artifact_response" >/dev/null || fail_with_logs "artifact upload response was invalid"
curl -fsS "${auth_header[@]}" \
  "$GATEWAY_URL/v1/artifacts/http-conformance-artifact" > "$DOWNLOAD_FILE"
cmp "$UPLOAD_FILE" "$DOWNLOAD_FILE" || fail_with_logs "downloaded artifact did not match upload"

registry_cursor="$(curl -fsS "${auth_header[@]}" "$GATEWAY_URL/v1/agents" | jq -r '.cursor')"
registration_response="$(curl -fsS -X POST \
  "${auth_header[@]}" \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/agents" \
  --data '{"agent_id":"http-conformance-agent","display_name":"HTTP Conformance Agent","version":"1.0.0","summary":"live gateway conformance","skills":["conformance"],"subscriptions":["topic:tasks"],"publications":["topic:task_events"],"schemas":[],"endpoint":{"transport":"http","address":"http://127.0.0.1:19000"},"classification":"internal","retention_class":"operational","ttl_seconds":60}')"
jq -e '.type == "agent_registered" and .card.principal == "local:developer"' \
  <<<"$registration_response" >/dev/null || fail_with_logs "agent registration response was invalid"

sse_status=0
curl -sS -N --max-time 2 \
  "${auth_header[@]}" \
  "$GATEWAY_URL/v1/agents/events?cursor=${registry_cursor}&wait_timeout_ms=1000" \
  > "$SSE_FILE" 2>/dev/null || sse_status=$?
[[ "$sse_status" == "0" || "$sse_status" == "28" ]] \
  || fail_with_logs "registry SSE request failed with curl status $sse_status"
grep -q '^event: registry_event$' "$SSE_FILE" \
  || fail_with_logs "registry SSE did not emit a registry_event"
grep -q '"agent_id":"http-conformance-agent"' "$SSE_FILE" \
  || fail_with_logs "registry SSE did not contain the registered agent"

heartbeat_response="$(curl -fsS -X POST \
  "${auth_header[@]}" \
  "$GATEWAY_URL/v1/agents/http-conformance-agent/heartbeat")"
jq -e '.type == "agent_heartbeat"' <<<"$heartbeat_response" >/dev/null \
  || fail_with_logs "agent heartbeat response was invalid"

remove_response="$(curl -fsS -X DELETE \
  "${auth_header[@]}" \
  "$GATEWAY_URL/v1/agents/http-conformance-agent")"
jq -e '.type == "agent_removed"' <<<"$remove_response" >/dev/null \
  || fail_with_logs "agent removal response was invalid"

stop_stack
start_stack

post_restart_messages="$(curl -fsS \
  "${auth_header[@]}" \
  "$GATEWAY_URL/v1/topics/conformance.events/messages?offset=0&limit=10")"
jq -e '.type == "messages" and (.messages | length) == 1 and .next_offset == 1' \
  <<<"$post_restart_messages" >/dev/null \
  || fail_with_logs "topic message was lost or duplicated after restart"

curl -fsS "${auth_header[@]}" \
  "$GATEWAY_URL/v1/artifacts/http-conformance-artifact" > "$DOWNLOAD_FILE"
cmp "$UPLOAD_FILE" "$DOWNLOAD_FILE" \
  || fail_with_logs "artifact was lost or corrupted after restart"

printf '%s\n' 'HTTP gateway conformance passed.'
