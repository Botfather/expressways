#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EVIDENCE_DIR="${EVIDENCE_DIR:-./var/agent/pilot-runs}"
LOG_DIR="${LOG_DIR:-$EVIDENCE_DIR/logs}"
WORK_ROOT="${WORK_ROOT:-$EVIDENCE_DIR/dr-restore-workdir}"
BROKER_PORT="${BROKER_PORT:-7788}"
mkdir -p "$EVIDENCE_DIR" "$LOG_DIR" "$WORK_ROOT"

TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
REPORT_PATH="${REPORT_PATH:-$EVIDENCE_DIR/dr-restore-clean-env-${TIMESTAMP}.md}"
JSON_PATH="${JSON_PATH:-$EVIDENCE_DIR/dr-restore-clean-env-${TIMESTAMP}.json}"

CONFIG_PATH="$WORK_ROOT/configs/expressways.dr.toml"
PRIVATE_KEY_PATH="$WORK_ROOT/var/auth/issuer.private"
PUBLIC_KEY_PATH="$WORK_ROOT/var/auth/issuer.public"
TOKEN_FILE="$WORK_ROOT/var/auth/developer.token"
BACKUP_OUTPUT_DIR="$WORK_ROOT/var/agent/backups"
BACKUP_ID="dr-restore-${TIMESTAMP}"
BACKUP_DIR="$BACKUP_OUTPUT_DIR/$BACKUP_ID"
CONFIG_AUDIT_LOG="$WORK_ROOT/var/agent/config-audit/entries.jsonl"
ORCH_STATE_PATH="$WORK_ROOT/var/orchestrator/state.json"
BROKER_LOG="$LOG_DIR/${TIMESTAMP}-dr-restore-broker.log"
BROKER_ADDRESS="127.0.0.1:${BROKER_PORT}"

STEP_LABELS=()
STEP_COMMANDS=()
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

run_step() {
  local label="$1"
  shift
  local command="$*"
  local started_at ended_at elapsed status_code=0
  local log_path="$LOG_DIR/${TIMESTAMP}-$(slugify "$label").log"

  echo "==> $label"
  echo "    $command"
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
  STEP_SECONDS+=("$elapsed")
  STEP_LOGS+=("$log_path")

  if [[ "$status_code" -eq 0 ]]; then
    STEP_STATUS+=("PASS")
    echo "    PASS (${elapsed}s)"
    return 0
  fi

  STEP_STATUS+=("FAIL")
  OVERALL_STATUS="FAIL"
  LAST_FAILURE="$label (exit $status_code)"
  echo "    FAIL (${elapsed}s). Log: $log_path"
  tail -n 60 "$log_path" || true
  return "$status_code"
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

prepare_workspace() {
  rm -rf "$WORK_ROOT"
  mkdir -p "$WORK_ROOT/configs" "$WORK_ROOT/var/auth" "$WORK_ROOT/var/audit" "$WORK_ROOT/var/data" "$WORK_ROOT/var/registry" "$WORK_ROOT/var/agent/config-audit" "$WORK_ROOT/var/orchestrator"

  cat >"$CONFIG_PATH" <<CFG
[schema]
version = 1

[server]
node_name = "dr-node"
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
key_id = "dev"
public_key_path = "${PUBLIC_KEY_PATH}"
status = "active"

[[auth.principals]]
id = "local:developer"
kind = "developer"
display_name = "DR Developer"
status = "active"
allowed_key_ids = ["dev"]
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

  printf '{"entry":"dr-rehearsal"}\n' >"$CONFIG_AUDIT_LOG"
  printf '{"tasks":[]}\n' >"$ORCH_STATE_PATH"
  printf '{"schema_version":1,"agents":[]}\n' >"$WORK_ROOT/var/registry/agents.json"
  printf '{"revoked_tokens":[],"revoked_principals":[],"revoked_key_ids":[]}\n' >"$WORK_ROOT/var/auth/revocations.json"
  : >"$WORK_ROOT/var/audit/audit.jsonl"
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

wait_for_health() {
  for _ in {1..30}; do
    if cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address "$BROKER_ADDRESS" health --token-file "$TOKEN_FILE" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address "$BROKER_ADDRESS" health --token-file "$TOKEN_FILE"
  return 1
}

corrupt_runtime_state() {
  rm -rf "$WORK_ROOT/var/data"
  rm -f "$WORK_ROOT/var/audit/audit.jsonl"
  rm -f "$WORK_ROOT/var/auth/revocations.json"
  rm -f "$WORK_ROOT/var/registry/agents.json"
  rm -f "$CONFIG_AUDIT_LOG"
  rm -f "$ORCH_STATE_PATH"
}

emit_report() {
  local total_seconds="$1"
  local generated_at
  generated_at="$(date -u +"%Y-%m-%d %H:%M:%SZ")"

  {
    echo "# DR Restore Clean-Environment Rehearsal"
    echo
    echo "Date (UTC): $generated_at"
    echo "Status: $OVERALL_STATUS"
    echo "Total Duration (seconds): $total_seconds"
    echo "Total Duration (minutes): $(awk "BEGIN { printf \"%.2f\", $total_seconds / 60 }")"
    if [[ "$OVERALL_STATUS" == "FAIL" ]]; then
      echo "Failure: $LAST_FAILURE"
    fi
    echo
    echo "Config: $CONFIG_PATH"
    echo "Backup Dir: $BACKUP_DIR"
    echo "Broker Address: $BROKER_ADDRESS"
    echo
    echo "| Step | Command | Duration (s) | Status | Log |"
    echo "| --- | --- | ---: | --- | --- |"
    local index
    for index in "${!STEP_LABELS[@]}"; do
      echo "| ${STEP_LABELS[$index]} | \`${STEP_COMMANDS[$index]}\` | ${STEP_SECONDS[$index]} | ${STEP_STATUS[$index]} | ${STEP_LOGS[$index]} |"
    done
  } >"$REPORT_PATH"

  {
    echo "{"
    echo "  \"timestamp_utc\": \"$generated_at\"," 
    echo "  \"status\": \"$OVERALL_STATUS\"," 
    echo "  \"total_duration_seconds\": $total_seconds,"
    echo "  \"config\": \"$CONFIG_PATH\"," 
    echo "  \"backup_dir\": \"$BACKUP_DIR\"," 
    echo "  \"broker_address\": \"$BROKER_ADDRESS\"," 
    echo "  \"steps\": ["
    local index
    for index in "${!STEP_LABELS[@]}"; do
      local comma="," 
      if [[ "$index" -eq "$((${#STEP_LABELS[@]} - 1))" ]]; then
        comma=""
      fi
      echo "    {\"label\": \"${STEP_LABELS[$index]}\", \"command\": \"${STEP_COMMANDS[$index]}\", \"duration_seconds\": ${STEP_SECONDS[$index]}, \"status\": \"${STEP_STATUS[$index]}\", \"log\": \"${STEP_LOGS[$index]}\"}$comma"
    done
    echo "  ]"
    echo "}"
  } >"$JSON_PATH"
}

started_total="$(date +%s)"

if run_step "Prepare isolated workspace" prepare_workspace \
  && run_step "Generate issuer keypair" cargo run -p expressways-client --bin expresswaysctl -- generate-keypair --key-id dev --private-key "$PRIVATE_KEY_PATH" --public-key "$PUBLIC_KEY_PATH" \
  && run_step "Issue developer token" cargo run -p expressways-client --bin expresswaysctl -- issue-token --key-id dev --private-key "$PRIVATE_KEY_PATH" --principal local:developer --audience expressways --scope system:broker:health --scope system:broker:admin --scope 'topic:*:admin,publish,consume' --scope 'registry:agents*:admin' --output "$TOKEN_FILE" \
  && run_step "Start broker baseline" start_broker \
  && run_step "Verify baseline health" wait_for_health \
  && run_step "Stop broker baseline" stop_broker \
  && run_step "Create runtime backup" cargo run -p expressways-client --bin expresswaysctl -- backup-runtime --config "$CONFIG_PATH" --output-dir "$BACKUP_OUTPUT_DIR" --backup-id "$BACKUP_ID" --config-audit-log "$CONFIG_AUDIT_LOG" --orchestrator-state "$ORCH_STATE_PATH" --signing-private-key "$PRIVATE_KEY_PATH" --signing-key-id dev --overwrite \
  && run_step "Corrupt runtime state" corrupt_runtime_state \
  && run_step "Restore runtime backup" cargo run -p expressways-client --bin expresswaysctl -- restore-runtime --backup-dir "$BACKUP_DIR" --config "$CONFIG_PATH" --config-audit-log "$CONFIG_AUDIT_LOG" --orchestrator-state "$ORCH_STATE_PATH" --verification-public-key "$PUBLIC_KEY_PATH" --overwrite \
  && run_step "Start broker after restore" start_broker \
  && run_step "Verify health after restore" wait_for_health \
  && run_step "Verify metrics after restore" cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address "$BROKER_ADDRESS" metrics --token-file "$TOKEN_FILE" \
  && run_step "Stop broker after restore" stop_broker; then
  :
else
  OVERALL_STATUS="FAIL"
fi

ended_total="$(date +%s)"
total_elapsed="$((ended_total - started_total))"
emit_report "$total_elapsed"

echo
echo "DR restore rehearsal report: $REPORT_PATH"
echo "DR restore rehearsal json:   $JSON_PATH"

if [[ "$OVERALL_STATUS" != "PASS" ]]; then
  exit 1
fi
