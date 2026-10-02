#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EVIDENCE_DIR="${EVIDENCE_DIR:-./var/agent/pilot-runs}"
LOG_DIR="${LOG_DIR:-$EVIDENCE_DIR/logs}"
mkdir -p "$EVIDENCE_DIR" "$LOG_DIR"

TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
REPORT_PATH="${REPORT_PATH:-$EVIDENCE_DIR/m3-live-rehearsal-suite-${TIMESTAMP}.md}"
JSON_PATH="${JSON_PATH:-$EVIDENCE_DIR/m3-live-rehearsal-suite-${TIMESTAMP}.json}"
COVERAGE_OUTPUT="${COVERAGE_OUTPUT:-$EVIDENCE_DIR/support-bundle-coverage-${TIMESTAMP}.json}"

STEP_LABELS=()
STEP_COMMANDS=()
STEP_SECONDS=()
STEP_STATUS=()
STEP_LOGS=()
OVERALL_STATUS="PASS"
LAST_FAILURE=""

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

emit_report() {
  local total_seconds="$1"
  local generated_at
  generated_at="$(date -u +"%Y-%m-%d %H:%M:%SZ")"

  {
    echo "# M3 Live Rehearsal Suite"
    echo
    echo "Date (UTC): $generated_at"
    echo "Status: $OVERALL_STATUS"
    echo "Total Duration (seconds): $total_seconds"
    echo "Total Duration (minutes): $(awk "BEGIN { printf \"%.2f\", $total_seconds / 60 }")"
    if [[ "$OVERALL_STATUS" == "FAIL" ]]; then
      echo "Failure: $LAST_FAILURE"
    fi
    echo
    echo "Support Bundle Coverage Output: $COVERAGE_OUTPUT"
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
    echo "  \"support_bundle_coverage_output\": \"$COVERAGE_OUTPUT\"," 
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

if run_step "Rehearsal: clean machine" bash scripts/rehearsal-clean-machine.sh \
  && run_step "Rehearsal: rollback" bash scripts/rehearsal-rollback.sh \
  && run_step "Rehearsal: reliability denials" bash scripts/rehearsal-reliability-denials.sh \
  && run_step "Validate support bundle top-10 incident coverage" cargo run -p expressways-client --bin expresswaysctl -- validate-support-bundle --bundle ./var/agent/support-bundle.json --output "$COVERAGE_OUTPUT"; then
  :
else
  OVERALL_STATUS="FAIL"
fi

ended_total="$(date +%s)"
total_elapsed="$((ended_total - started_total))"
emit_report "$total_elapsed"

echo
echo "M3 live rehearsal suite report: $REPORT_PATH"
echo "M3 live rehearsal suite json:   $JSON_PATH"

if [[ "$OVERALL_STATUS" != "PASS" ]]; then
  exit 1
fi
