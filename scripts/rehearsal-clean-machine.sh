#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EVIDENCE_DIR="${EVIDENCE_DIR:-./var/agent/pilot-runs}"
LOG_DIR="${LOG_DIR:-$EVIDENCE_DIR/logs}"
mkdir -p "$EVIDENCE_DIR" "$LOG_DIR" ./var/agent

TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
REPORT_PATH="${REPORT_PATH:-$EVIDENCE_DIR/clean-machine-${TIMESTAMP}.md}"
SUPPORT_BUNDLE_PATH="${SUPPORT_BUNDLE_PATH:-./var/agent/support-bundle.json}"

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
  tail -n 40 "$log_path" || true
  return "$status_code"
}

emit_report() {
  local total_seconds="$1"
  local generated_at
  generated_at="$(date -u +"%Y-%m-%d %H:%M:%SZ")"

  {
    echo "# Clean-Machine Rehearsal Report"
    echo
    echo "Date (UTC): $generated_at"
    echo "Status: $OVERALL_STATUS"
    echo "Total Duration (seconds): $total_seconds"
    echo "Total Duration (minutes): $(awk "BEGIN { printf \"%.2f\", $total_seconds / 60 }")"
    echo
    echo "Support Bundle Path: $SUPPORT_BUNDLE_PATH"
    if [[ "$OVERALL_STATUS" == "FAIL" ]]; then
      echo "Failure: $LAST_FAILURE"
    fi
    echo
    echo "| Step | Command | Duration (s) | Status | Log |"
    echo "| --- | --- | ---: | --- | --- |"
    local index
    for index in "${!STEP_LABELS[@]}"; do
      echo "| ${STEP_LABELS[$index]} | \`${STEP_COMMANDS[$index]}\` | ${STEP_SECONDS[$index]} | ${STEP_STATUS[$index]} | ${STEP_LOGS[$index]} |"
    done
  } >"$REPORT_PATH"
}

started_total="$(date +%s)"

if run_step "Bootstrap local keys and token" make bootstrap-local \
  && run_step "Start broker service" bash scripts/expressways-service.sh restart expressways-server \
  && run_step "Verify first-run path" make verify-first-run \
  && run_step "Seed config-audit evidence" bash -lc "mkdir -p ./var/agent/config-audit && printf '{\"actor\":\"rehearsal\",\"component\":\"clean-machine\",\"action\":\"verify_first_run\",\"timestamp\":\"%s\"}\n' \"\$(date -u +%Y-%m-%dT%H:%M:%SZ)\" >> ./var/agent/config-audit/entries.jsonl" \
  && run_step "Export support bundle" make export-support-bundle; then
  :
else
  OVERALL_STATUS="FAIL"
fi

ended_total="$(date +%s)"
total_elapsed="$((ended_total - started_total))"
emit_report "$total_elapsed"

echo
echo "Clean-machine rehearsal report: $REPORT_PATH"
if [[ "$OVERALL_STATUS" != "PASS" ]]; then
  exit 1
fi
