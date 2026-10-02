#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EVIDENCE_DIR="${EVIDENCE_DIR:-./var/agent/pilot-runs}"
LOG_DIR="${LOG_DIR:-$EVIDENCE_DIR/logs}"
CONFIG_PATH="${CONFIG_PATH:-configs/expressways.example.toml}"
SUPPORT_BUNDLE_PATH="${SUPPORT_BUNDLE_PATH:-./var/agent/support-bundle.json}"
mkdir -p "$EVIDENCE_DIR" "$LOG_DIR" ./var/agent ./tmp

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Missing config file: $CONFIG_PATH"
  exit 1
fi

TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
REPORT_PATH="${REPORT_PATH:-$EVIDENCE_DIR/rollback-rehearsal-${TIMESTAMP}.md}"
ORIGINAL_CONFIG="$(mktemp "${TMPDIR:-/tmp}/expressways-rollback-config.XXXXXX")"
cp "$CONFIG_PATH" "$ORIGINAL_CONFIG"

STEP_LABELS=()
STEP_COMMANDS=()
STEP_EXPECTED=()
STEP_SECONDS=()
STEP_STATUS=()
STEP_LOGS=()
OVERALL_STATUS="PASS"
LAST_FAILURE=""

cleanup() {
  cp "$ORIGINAL_CONFIG" "$CONFIG_PATH" >/dev/null 2>&1 || true
  rm -f "$ORIGINAL_CONFIG"
}
trap cleanup EXIT

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
  tail -n 40 "$log_path" || true
  return 1
}

emit_report() {
  local total_seconds="$1"
  local generated_at
  generated_at="$(date -u +"%Y-%m-%d %H:%M:%SZ")"

  {
    echo "# Rollback Rehearsal Report"
    echo
    echo "Date (UTC): $generated_at"
    echo "Status: $OVERALL_STATUS"
    echo "Total Duration (seconds): $total_seconds"
    echo "Total Duration (minutes): $(awk "BEGIN { printf \"%.2f\", $total_seconds / 60 }")"
    echo
    echo "Config File: $CONFIG_PATH"
    echo "Support Bundle Path: $SUPPORT_BUNDLE_PATH"
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
}

started_total="$(date +%s)"

if run_step_expect "Bootstrap local keys and token" pass make bootstrap-local \
  && run_step_expect "Start broker service" pass bash scripts/expressways-service.sh restart expressways-server \
  && run_step_expect "Verify baseline first-run path" pass make verify-first-run \
  && run_step_expect "Inject invalid config for failed-upgrade simulation" pass bash -lc "printf '[server\n' > '$CONFIG_PATH'" \
  && run_step_expect "Restart broker with invalid config (expected failure)" fail bash scripts/expressways-service.sh restart expressways-server \
  && run_step_expect "Restore known-good config" pass cp "$ORIGINAL_CONFIG" "$CONFIG_PATH" \
  && run_step_expect "Restart broker after rollback" pass bash scripts/expressways-service.sh restart expressways-server \
  && run_step_expect "Verify first-run path after rollback" pass make verify-first-run \
  && run_step_expect "Seed config-audit evidence" pass bash -lc "mkdir -p ./var/agent/config-audit && printf '{\"actor\":\"rehearsal\",\"component\":\"rollback\",\"action\":\"rollback_verify\",\"timestamp\":\"%s\"}\n' \"\$(date -u +%Y-%m-%dT%H:%M:%SZ)\" >> ./var/agent/config-audit/entries.jsonl" \
  && run_step_expect "Export support bundle" pass make export-support-bundle; then
  :
else
  OVERALL_STATUS="FAIL"
fi

ended_total="$(date +%s)"
total_elapsed="$((ended_total - started_total))"
emit_report "$total_elapsed"

echo
echo "Rollback rehearsal report: $REPORT_PATH"
if [[ "$OVERALL_STATUS" != "PASS" ]]; then
  exit 1
fi
