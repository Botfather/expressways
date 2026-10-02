#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EVIDENCE_DIR="${EVIDENCE_DIR:-./var/agent/pilot-runs}"
LOG_DIR="${LOG_DIR:-$EVIDENCE_DIR/logs}"
WORK_ROOT="${WORK_ROOT:-$EVIDENCE_DIR/key-rotation-workdir}"
BROKER_PORT="${BROKER_PORT:-7792}"
OLD_KEY_ID="${OLD_KEY_ID:-dev}"
NEW_KEY_ID="${NEW_KEY_ID:-dev-2026q2}"
mkdir -p "$EVIDENCE_DIR" "$LOG_DIR" "$WORK_ROOT"

TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
REPORT_PATH="${REPORT_PATH:-$EVIDENCE_DIR/key-rotation-rehearsal-${TIMESTAMP}.md}"
JSON_PATH="${JSON_PATH:-$EVIDENCE_DIR/key-rotation-rehearsal-${TIMESTAMP}.json}"

BROKER_ADDRESS="127.0.0.1:${BROKER_PORT}"
CONFIG_PATH="$WORK_ROOT/configs/expressways.rotation.toml"
OLD_PRIVATE_KEY_PATH="$WORK_ROOT/var/auth/${OLD_KEY_ID}.private"
OLD_PUBLIC_KEY_PATH="$WORK_ROOT/var/auth/${OLD_KEY_ID}.public"
NEW_PRIVATE_KEY_PATH="$WORK_ROOT/var/auth/${NEW_KEY_ID}.private"
NEW_PUBLIC_KEY_PATH="$WORK_ROOT/var/auth/${NEW_KEY_ID}.public"
OLD_TOKEN_PATH="$WORK_ROOT/var/auth/${OLD_KEY_ID}.token"
OLD_POST_CUTOVER_TOKEN_PATH="$WORK_ROOT/var/auth/${OLD_KEY_ID}.post-cutover.token"
NEW_TOKEN_PATH="$WORK_ROOT/var/auth/${NEW_KEY_ID}.token"
AUTH_STATE_PATH="$WORK_ROOT/var/agent/auth-state.json"
BROKER_LOG="$LOG_DIR/${TIMESTAMP}-key-rotation-broker.log"

STEP_LABELS=()
STEP_COMMANDS=()
STEP_EXPECTED=()
STEP_SECONDS=()
STEP_STATUS=()
STEP_LOGS=()
OVERALL_STATUS="PASS"
LAST_FAILURE=""
BROKER_PID=""

slugify() {
  local value="$1"
  value="$(echo "$value" | tr '[:upper:]' '[:lower:]')"
  value="${value//[^a-z0-9]/-}"
  value="$(echo "$value" | tr -s '-')"
  value="${value#-}"
  value="${value%-}"
  if [[ -z "$value" ]]; then
    value="step"
  fi
  echo "$value"
}

run_step_expect() {
  local label="$1"
  local expected="$2"
  shift 2
  local command="$*"
  local started_at ended_at elapsed status_code=0
  local log_path="$LOG_DIR/${TIMESTAMP}-$(slugify "$label").log"

  echo "==> $label"
  echo "    expected: $expected"
  echo "    command:  $command"
  started_at="$(date +%s)"
  if "$@" >"$log_path" 2>&1; then
    :
  else
    status_code="$?"
  fi
  ended_at="$(date +%s)"
  elapsed="$((ended_at - started_at))"

  STEP_LABELS+=("$label")
  STEP_COMMANDS+=("$command")
  STEP_EXPECTED+=("$expected")
  STEP_SECONDS+=("$elapsed")
  STEP_LOGS+=("$log_path")

  local step_passed="false"
  if [[ "$expected" == "pass" && "$status_code" -eq 0 ]]; then
    step_passed="true"
  fi
  if [[ "$expected" == "fail" && "$status_code" -ne 0 ]]; then
    step_passed="true"
  fi

  if [[ "$step_passed" == "true" ]]; then
    STEP_STATUS+=("PASS")
    echo "    PASS (${elapsed}s)"
    return 0
  fi

  STEP_STATUS+=("FAIL")
  OVERALL_STATUS="FAIL"
  LAST_FAILURE="$label (exit $status_code, expected $expected)"
  echo "    FAIL (${elapsed}s). Log: $log_path"
  tail -n 60 "$log_path" || true
  return 1
}

stop_broker() {
  if [[ -z "$BROKER_PID" ]]; then
    return 0
  fi
  if kill -0 "$BROKER_PID" >/dev/null 2>&1; then
    kill "$BROKER_PID" >/dev/null 2>&1 || true
    for _ in {1..40}; do
      if ! kill -0 "$BROKER_PID" >/dev/null 2>&1; then
        break
      fi
      sleep 0.25
    done
    if kill -0 "$BROKER_PID" >/dev/null 2>&1; then
      kill -9 "$BROKER_PID" >/dev/null 2>&1 || true
    fi
  fi
  BROKER_PID=""
  return 0
}

cleanup() {
  stop_broker || true
}
trap cleanup EXIT

write_config_baseline() {
  cat >"$CONFIG_PATH" <<CFG
[schema]
version = 1

[server]
node_name = "rotation-node"
transport = "tcp"
listen_addr = "${BROKER_ADDRESS}"
data_dir = "${WORK_ROOT}/var/data"
log_level = "info"

[storage]
segment_max_bytes = 1048576
retention_class = "operational"
default_classification = "internal"
ephemeral_retention_bytes = 4194304
operational_retention_bytes = 16777216
regulated_retention_bytes = 67108864
max_total_bytes = 134217728
reclaim_target_bytes = 117440512

[audit]
path = "${WORK_ROOT}/var/audit/audit.jsonl"

[registry]
backend = "file"
path = "${WORK_ROOT}/var/registry/agents.json"
default_ttl_seconds = 300
event_history_limit = 1024
stream_send_timeout_ms = 1000
stream_idle_keepalive_limit = 12

[auth]
audience = "expressways"
revocation_path = "${WORK_ROOT}/var/auth/revocations.json"

[[auth.issuers]]
key_id = "${OLD_KEY_ID}"
public_key_path = "${OLD_PUBLIC_KEY_PATH}"
status = "active"

[[auth.principals]]
id = "local:developer"
kind = "developer"
display_name = "Rotation Operator"
status = "active"
allowed_key_ids = ["${OLD_KEY_ID}"]
quota_profile = "operator"

[quotas]

[[quotas.profiles]]
name = "operator"
publish_payload_max_bytes = 16384
publish_requests_per_window = 20
publish_window_seconds = 1
consume_max_limit = 100
consume_requests_per_window = 20
consume_window_seconds = 1
backpressure_mode = "reject"
backpressure_delay_ms = 0

[policy]
default_decision = "deny"

[[policy.rules]]
principal = "local:developer"
resource = "system:broker"
actions = ["health", "admin"]

[[policy.rules]]
principal = "local:developer"
resource = "topic:*"
actions = ["publish", "consume", "admin"]

[[policy.rules]]
principal = "local:developer"
resource = "registry:agents*"
actions = ["admin"]
CFG
}

write_config_overlap() {
  cat >"$CONFIG_PATH" <<CFG
[schema]
version = 1

[server]
node_name = "rotation-node"
transport = "tcp"
listen_addr = "${BROKER_ADDRESS}"
data_dir = "${WORK_ROOT}/var/data"
log_level = "info"

[storage]
segment_max_bytes = 1048576
retention_class = "operational"
default_classification = "internal"
ephemeral_retention_bytes = 4194304
operational_retention_bytes = 16777216
regulated_retention_bytes = 67108864
max_total_bytes = 134217728
reclaim_target_bytes = 117440512

[audit]
path = "${WORK_ROOT}/var/audit/audit.jsonl"

[registry]
backend = "file"
path = "${WORK_ROOT}/var/registry/agents.json"
default_ttl_seconds = 300
event_history_limit = 1024
stream_send_timeout_ms = 1000
stream_idle_keepalive_limit = 12

[auth]
audience = "expressways"
revocation_path = "${WORK_ROOT}/var/auth/revocations.json"

[[auth.issuers]]
key_id = "${OLD_KEY_ID}"
public_key_path = "${OLD_PUBLIC_KEY_PATH}"
status = "active"

[[auth.issuers]]
key_id = "${NEW_KEY_ID}"
public_key_path = "${NEW_PUBLIC_KEY_PATH}"
status = "rotating"

[[auth.principals]]
id = "local:developer"
kind = "developer"
display_name = "Rotation Operator"
status = "active"
allowed_key_ids = ["${OLD_KEY_ID}", "${NEW_KEY_ID}"]
quota_profile = "operator"

[quotas]

[[quotas.profiles]]
name = "operator"
publish_payload_max_bytes = 16384
publish_requests_per_window = 20
publish_window_seconds = 1
consume_max_limit = 100
consume_requests_per_window = 20
consume_window_seconds = 1
backpressure_mode = "reject"
backpressure_delay_ms = 0

[policy]
default_decision = "deny"

[[policy.rules]]
principal = "local:developer"
resource = "system:broker"
actions = ["health", "admin"]

[[policy.rules]]
principal = "local:developer"
resource = "topic:*"
actions = ["publish", "consume", "admin"]

[[policy.rules]]
principal = "local:developer"
resource = "registry:agents*"
actions = ["admin"]
CFG
}

write_config_cutover() {
  cat >"$CONFIG_PATH" <<CFG
[schema]
version = 1

[server]
node_name = "rotation-node"
transport = "tcp"
listen_addr = "${BROKER_ADDRESS}"
data_dir = "${WORK_ROOT}/var/data"
log_level = "info"

[storage]
segment_max_bytes = 1048576
retention_class = "operational"
default_classification = "internal"
ephemeral_retention_bytes = 4194304
operational_retention_bytes = 16777216
regulated_retention_bytes = 67108864
max_total_bytes = 134217728
reclaim_target_bytes = 117440512

[audit]
path = "${WORK_ROOT}/var/audit/audit.jsonl"

[registry]
backend = "file"
path = "${WORK_ROOT}/var/registry/agents.json"
default_ttl_seconds = 300
event_history_limit = 1024
stream_send_timeout_ms = 1000
stream_idle_keepalive_limit = 12

[auth]
audience = "expressways"
revocation_path = "${WORK_ROOT}/var/auth/revocations.json"

[[auth.issuers]]
key_id = "${OLD_KEY_ID}"
public_key_path = "${OLD_PUBLIC_KEY_PATH}"
status = "disabled"

[[auth.issuers]]
key_id = "${NEW_KEY_ID}"
public_key_path = "${NEW_PUBLIC_KEY_PATH}"
status = "active"

[[auth.principals]]
id = "local:developer"
kind = "developer"
display_name = "Rotation Operator"
status = "active"
allowed_key_ids = ["${NEW_KEY_ID}"]
quota_profile = "operator"

[quotas]

[[quotas.profiles]]
name = "operator"
publish_payload_max_bytes = 16384
publish_requests_per_window = 20
publish_window_seconds = 1
consume_max_limit = 100
consume_requests_per_window = 20
consume_window_seconds = 1
backpressure_mode = "reject"
backpressure_delay_ms = 0

[policy]
default_decision = "deny"

[[policy.rules]]
principal = "local:developer"
resource = "system:broker"
actions = ["health", "admin"]

[[policy.rules]]
principal = "local:developer"
resource = "topic:*"
actions = ["publish", "consume", "admin"]

[[policy.rules]]
principal = "local:developer"
resource = "registry:agents*"
actions = ["admin"]
CFG
}

prepare_workspace() {
  rm -rf "$WORK_ROOT"
  mkdir -p "$WORK_ROOT/configs" "$WORK_ROOT/var/auth" "$WORK_ROOT/var/audit" "$WORK_ROOT/var/data" "$WORK_ROOT/var/registry" "$WORK_ROOT/var/agent"
  printf '{"schema_version":1,"agents":[]}\n' >"$WORK_ROOT/var/registry/agents.json"
  printf '{"schema_version":1,"revoked_tokens":[],"revoked_principals":[],"revoked_key_ids":[]}\n' >"$WORK_ROOT/var/auth/revocations.json"
  : >"$WORK_ROOT/var/audit/audit.jsonl"
  write_config_baseline
}

start_broker() {
  stop_broker || true
  cargo run -p expressways-server -- --config "$CONFIG_PATH" >>"$BROKER_LOG" 2>&1 &
  BROKER_PID=$!
  sleep 1
  if ! kill -0 "$BROKER_PID" >/dev/null 2>&1; then
    tail -n 80 "$BROKER_LOG" || true
    return 1
  fi
  return 0
}

assert_health_success_with_token() {
  local token_file="$1"
  local output_file="$WORK_ROOT/var/agent/health-response-$(basename "$token_file").json"
  cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address "$BROKER_ADDRESS" health --token-file "$token_file" >"$output_file"
  if ! rg -q '"type"\s*:\s*"health"' "$output_file"; then
    echo "Expected health response, got:"
    cat "$output_file"
    return 1
  fi
}

assert_health_denied_with_token() {
  local token_file="$1"
  local output_file="$WORK_ROOT/var/agent/health-response-$(basename "$token_file").json"
  cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address "$BROKER_ADDRESS" health --token-file "$token_file" >"$output_file"
  if ! rg -q '"type"\s*:\s*"error"' "$output_file"; then
    echo "Expected error response for denied token, got:"
    cat "$output_file"
    return 1
  fi
  if ! rg -q '"code"\s*:\s*"(access_denied|invalid_capability)"' "$output_file"; then
    echo "Expected access_denied or invalid_capability code for denied token, got:"
    cat "$output_file"
    return 1
  fi
}

issue_admin_token() {
  local key_id="$1"
  local private_key="$2"
  local output_path="$3"
  cargo run -p expressways-client --bin expresswaysctl -- issue-token --key-id "$key_id" --private-key "$private_key" --principal local:developer --audience expressways --scope system:broker:health --scope system:broker:admin --scope 'topic:*:admin,publish,consume' --scope 'registry:agents*:admin' --output "$output_path"
}

capture_auth_state() {
  cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address "$BROKER_ADDRESS" auth-state --token-file "$NEW_TOKEN_PATH" >"$AUTH_STATE_PATH"
}

assert_old_key_revoked() {
  if ! awk '/"revoked_key_ids"/,/\]/' "$AUTH_STATE_PATH" | rg -q "\"${OLD_KEY_ID}\""; then
    echo "Old key id ${OLD_KEY_ID} not found under revoked_key_ids in ${AUTH_STATE_PATH}."
    return 1
  fi
}

emit_report() {
  local total_seconds="$1"
  local generated_at
  generated_at="$(date -u +"%Y-%m-%d %H:%M:%SZ")"

  {
    echo "# Key Rotation Rehearsal Report"
    echo
    echo "Date (UTC): $generated_at"
    echo "Status: $OVERALL_STATUS"
    echo "Total Duration (seconds): $total_seconds"
    echo "Total Duration (minutes): $(awk "BEGIN { printf \"%.2f\", $total_seconds / 60 }")"
    echo
    echo "Broker Address: $BROKER_ADDRESS"
    echo "Config: $CONFIG_PATH"
    echo "Old Key Id: $OLD_KEY_ID"
    echo "New Key Id: $NEW_KEY_ID"
    if [[ "$OVERALL_STATUS" == "FAIL" ]]; then
      echo "Failure: $LAST_FAILURE"
    fi
    echo
    echo "| Step | Expected | Command | Duration (s) | Status | Log |"
    echo "| --- | --- | --- | ---: | --- | --- |"
    local index
    for index in "${!STEP_LABELS[@]}"; do
      echo "| ${STEP_LABELS[$index]} | ${STEP_EXPECTED[$index]} | \`${STEP_COMMANDS[$index]}\` | ${STEP_SECONDS[$index]} | ${STEP_STATUS[$index]} | ${STEP_LOGS[$index]} |"
    done
  } >"$REPORT_PATH"

  {
    echo "{"
    echo "  \"timestamp_utc\": \"$generated_at\","
    echo "  \"status\": \"$OVERALL_STATUS\","
    echo "  \"total_duration_seconds\": $total_seconds,"
    echo "  \"broker_address\": \"$BROKER_ADDRESS\","
    echo "  \"config\": \"$CONFIG_PATH\","
    echo "  \"old_key_id\": \"$OLD_KEY_ID\","
    echo "  \"new_key_id\": \"$NEW_KEY_ID\","
    echo "  \"steps\": ["
    local index
    for index in "${!STEP_LABELS[@]}"; do
      local comma=","
      if [[ "$index" -eq "$((${#STEP_LABELS[@]} - 1))" ]]; then
        comma=""
      fi
      echo "    {\"label\": \"${STEP_LABELS[$index]}\", \"expected\": \"${STEP_EXPECTED[$index]}\", \"command\": \"${STEP_COMMANDS[$index]}\", \"duration_seconds\": ${STEP_SECONDS[$index]}, \"status\": \"${STEP_STATUS[$index]}\", \"log\": \"${STEP_LOGS[$index]}\"}$comma"
    done
    echo "  ]"
    echo "}"
  } >"$JSON_PATH"
}

started_total="$(date +%s)"

if run_step_expect "Prepare isolated workspace" pass prepare_workspace \
  && run_step_expect "Generate baseline issuer keypair" pass cargo run -p expressways-client --bin expresswaysctl -- generate-keypair --key-id "$OLD_KEY_ID" --private-key "$OLD_PRIVATE_KEY_PATH" --public-key "$OLD_PUBLIC_KEY_PATH" \
  && run_step_expect "Issue baseline token" pass issue_admin_token "$OLD_KEY_ID" "$OLD_PRIVATE_KEY_PATH" "$OLD_TOKEN_PATH" \
  && run_step_expect "Start broker baseline" pass start_broker \
  && run_step_expect "Verify baseline token accepted" pass assert_health_success_with_token "$OLD_TOKEN_PATH" \
  && run_step_expect "Generate rotating issuer keypair" pass cargo run -p expressways-client --bin expresswaysctl -- generate-keypair --key-id "$NEW_KEY_ID" --private-key "$NEW_PRIVATE_KEY_PATH" --public-key "$NEW_PUBLIC_KEY_PATH" \
  && run_step_expect "Write overlap rotation config" pass write_config_overlap \
  && run_step_expect "Restart broker for overlap phase" pass start_broker \
  && run_step_expect "Issue rotating key token" pass issue_admin_token "$NEW_KEY_ID" "$NEW_PRIVATE_KEY_PATH" "$NEW_TOKEN_PATH" \
  && run_step_expect "Verify baseline token accepted during overlap" pass assert_health_success_with_token "$OLD_TOKEN_PATH" \
  && run_step_expect "Verify rotating token accepted during overlap" pass assert_health_success_with_token "$NEW_TOKEN_PATH" \
  && run_step_expect "Write cutover config" pass write_config_cutover \
  && run_step_expect "Restart broker for cutover phase" pass start_broker \
  && run_step_expect "Verify rotating token accepted after cutover" pass assert_health_success_with_token "$NEW_TOKEN_PATH" \
  && run_step_expect "Issue old-key token after cutover" pass issue_admin_token "$OLD_KEY_ID" "$OLD_PRIVATE_KEY_PATH" "$OLD_POST_CUTOVER_TOKEN_PATH" \
  && run_step_expect "Verify old-key token denied after cutover" pass assert_health_denied_with_token "$OLD_POST_CUTOVER_TOKEN_PATH" \
  && run_step_expect "Revoke old issuer key" pass cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address "$BROKER_ADDRESS" revoke-key --token-file "$NEW_TOKEN_PATH" --key-id "$OLD_KEY_ID" \
  && run_step_expect "Capture auth-state after revocation" pass capture_auth_state \
  && run_step_expect "Verify auth-state includes revoked old key" pass assert_old_key_revoked \
  && run_step_expect "Verify rotating token accepted post-revocation" pass assert_health_success_with_token "$NEW_TOKEN_PATH" \
  && run_step_expect "Stop broker" pass stop_broker; then
  :
else
  OVERALL_STATUS="FAIL"
fi

ended_total="$(date +%s)"
total_elapsed="$((ended_total - started_total))"
emit_report "$total_elapsed"

echo
echo "Key rotation rehearsal report: $REPORT_PATH"
echo "Key rotation rehearsal json:   $JSON_PATH"

if [[ "$OVERALL_STATUS" != "PASS" ]]; then
  exit 1
fi
