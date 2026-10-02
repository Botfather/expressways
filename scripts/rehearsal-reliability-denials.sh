#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EVIDENCE_DIR="${EVIDENCE_DIR:-./var/agent/pilot-runs}"
LOG_DIR="${LOG_DIR:-$EVIDENCE_DIR/logs}"
mkdir -p "$EVIDENCE_DIR" "$LOG_DIR"

TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
REPORT_PATH="${REPORT_PATH:-$EVIDENCE_DIR/reliability-denials-rehearsal-${TIMESTAMP}.md}"
JSON_PATH="${JSON_PATH:-$EVIDENCE_DIR/reliability-denials-rehearsal-${TIMESTAMP}.json}"
RESULTS_PATH="$(mktemp "$EVIDENCE_DIR/reliability-denials-results-${TIMESTAMP}-XXXX.tsv")"

SCENARIOS=(
  "degraded_mode_audit::cargo test -p expressways-server startup_with_unavailable_audit_stays_servable_in_degraded_mode -- --nocapture"
  "degraded_mode_storage::cargo test -p expressways-server startup_with_unavailable_storage_returns_degraded_service_errors -- --nocapture"
  "storage_pressure::cargo test -p expressways-storage disk_pressure_rejects_when_no_more_segments_can_be_reclaimed -- --nocapture"
  "auth_revocation_denial::cargo test -p expressways-server revoked_token_requests_are_denied_and_audited -- --nocapture"
  "policy_denial::cargo test -p expressways-server policy_denials_are_rejected_and_audited -- --nocapture"
  "quota_denial::cargo test -p expressways-server quota_denials_are_rejected_and_audited -- --nocapture"
)

OVERALL_STATUS="PASS"
PASSED=0
FAILED=0
STARTED_AT="$(date +%s)"

run_scenario() {
  local name="$1"
  local command="$2"
  local started ended duration status log_path

  log_path="$LOG_DIR/${TIMESTAMP}-${name}.log"
  started="$(date +%s)"

  echo "==> Running $name"
  echo "    $command"
  if bash -lc "$command" >"$log_path" 2>&1; then
    status="PASS"
    PASSED=$((PASSED + 1))
  else
    status="FAIL"
    FAILED=$((FAILED + 1))
    OVERALL_STATUS="FAIL"
  fi

  ended="$(date +%s)"
  duration="$((ended - started))"
  printf "%s\t%s\t%s\t%s\n" "$name" "$status" "$duration" "$log_path" >>"$RESULTS_PATH"
}

for scenario in "${SCENARIOS[@]}"; do
  name="${scenario%%::*}"
  command="${scenario#*::}"
  run_scenario "$name" "$command"
done

ENDED_AT="$(date +%s)"
TOTAL_SECONDS="$((ENDED_AT - STARTED_AT))"
GENERATED_AT="$(date -u +"%Y-%m-%d %H:%M:%SZ")"

{
  echo "# Reliability and Denial Rehearsal"
  echo
  echo "Date (UTC): $GENERATED_AT"
  echo "Status: $OVERALL_STATUS"
  echo "Total Duration (seconds): $TOTAL_SECONDS"
  echo "Passed Scenarios: $PASSED"
  echo "Failed Scenarios: $FAILED"
  echo
  echo "| Scenario | Status | Duration (s) | Log |"
  echo "|---|---|---:|---|"
  while IFS=$'\t' read -r name status duration log_path; do
    echo "| $name | $status | $duration | $log_path |"
  done <"$RESULTS_PATH"
} >"$REPORT_PATH"

scenario_count="$(wc -l < "$RESULTS_PATH" | tr -d ' ')"
{
  echo "{"
  echo "  \"timestamp_utc\": \"$GENERATED_AT\"," 
  echo "  \"status\": \"$OVERALL_STATUS\"," 
  echo "  \"total_duration_seconds\": $TOTAL_SECONDS,"
  echo "  \"passed_scenarios\": $PASSED,"
  echo "  \"failed_scenarios\": $FAILED,"
  echo "  \"scenarios\": ["

  index=0
  while IFS=$'\t' read -r name status duration log_path; do
    index=$((index + 1))
    comma=","
    if [[ "$index" -eq "$scenario_count" ]]; then
      comma=""
    fi
    echo "    {\"name\": \"$name\", \"status\": \"$status\", \"duration_seconds\": $duration, \"log\": \"$log_path\"}$comma"
  done <"$RESULTS_PATH"

  echo "  ]"
  echo "}"
} >"$JSON_PATH"

echo
echo "Reliability+denial rehearsal report: $REPORT_PATH"
echo "Reliability+denial rehearsal json:   $JSON_PATH"

if [[ "$OVERALL_STATUS" != "PASS" ]]; then
  echo
  echo "One or more rehearsal scenarios failed. Tail of failing logs:"
  while IFS=$'\t' read -r name status _ log_path; do
    if [[ "$status" == "FAIL" ]]; then
      echo
      echo "--- $name ($log_path) ---"
      tail -n 60 "$log_path" || true
    fi
  done <"$RESULTS_PATH"
  rm -f "$RESULTS_PATH"
  exit 1
fi

rm -f "$RESULTS_PATH"
