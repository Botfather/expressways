#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EVIDENCE_DIR="${EVIDENCE_DIR:-./var/agent/pilot-runs}"
LOG_DIR="${LOG_DIR:-$EVIDENCE_DIR/logs}"
TEST_FILTER="${TEST_FILTER:-rollback_reliability_meets_m2_target}"
mkdir -p "$EVIDENCE_DIR" "$LOG_DIR"

TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
REPORT_PATH="${REPORT_PATH:-$EVIDENCE_DIR/config-rollback-reliability-${TIMESTAMP}.md}"
JSON_PATH="${JSON_PATH:-$EVIDENCE_DIR/config-rollback-reliability-${TIMESTAMP}.json}"
LOG_PATH="${LOG_PATH:-$LOG_DIR/${TIMESTAMP}-config-rollback-reliability.log}"

COMMAND=(cargo test -p expressways-console "$TEST_FILTER" -- --nocapture)
STARTED_AT="$(date +%s)"
OVERALL_STATUS="PASS"

echo "==> Running config rollback reliability rehearsal"
echo "    ${COMMAND[*]}"
if "${COMMAND[@]}" >"$LOG_PATH" 2>&1; then
  :
else
  OVERALL_STATUS="FAIL"
fi
ENDED_AT="$(date +%s)"
TOTAL_SECONDS="$((ENDED_AT - STARTED_AT))"

METRIC_LINE="$(rg 'ROLLBACK_RELIABILITY ' "$LOG_PATH" | tail -n 1 || true)"
SUCCESS_RATE="n/a"
ATTEMPTS="n/a"
SUCCESSES="n/a"
if [[ -n "$METRIC_LINE" ]]; then
  SUCCESS_RATE="$(echo "$METRIC_LINE" | sed -E 's/.*success_rate=([0-9]+\.[0-9]+).*/\1/')"
  ATTEMPTS="$(echo "$METRIC_LINE" | sed -E 's/.*attempts=([0-9]+).*/\1/')"
  SUCCESSES="$(echo "$METRIC_LINE" | sed -E 's/.*successes=([0-9]+).*/\1/')"
fi

GENERATED_AT="$(date -u +"%Y-%m-%d %H:%M:%SZ")"
{
  echo "# Config Rollback Reliability Rehearsal"
  echo
  echo "Date (UTC): $GENERATED_AT"
  echo "Status: $OVERALL_STATUS"
  echo "Total Duration (seconds): $TOTAL_SECONDS"
  echo "Total Duration (minutes): $(awk "BEGIN { printf \"%.2f\", $TOTAL_SECONDS / 60 }")"
  echo "Success Rate (%): $SUCCESS_RATE"
  echo "Attempts: $ATTEMPTS"
  echo "Successes: $SUCCESSES"
  echo "Pass Criterion: >= 99%"
  echo
  echo "Command: \`${COMMAND[*]}\`"
  echo "Log: $LOG_PATH"
} >"$REPORT_PATH"

{
  echo "{"
  echo "  \"timestamp_utc\": \"$GENERATED_AT\","
  echo "  \"status\": \"$OVERALL_STATUS\","
  echo "  \"total_duration_seconds\": $TOTAL_SECONDS,"
  if [[ "$SUCCESS_RATE" == "n/a" ]]; then
    echo "  \"success_rate_percent\": null,"
  else
    echo "  \"success_rate_percent\": $SUCCESS_RATE,"
  fi
  if [[ "$ATTEMPTS" == "n/a" ]]; then
    echo "  \"attempts\": null,"
  else
    echo "  \"attempts\": $ATTEMPTS,"
  fi
  if [[ "$SUCCESSES" == "n/a" ]]; then
    echo "  \"successes\": null,"
  else
    echo "  \"successes\": $SUCCESSES,"
  fi
  echo "  \"log\": \"$LOG_PATH\""
  echo "}"
} >"$JSON_PATH"

echo
echo "Config rollback reliability report: $REPORT_PATH"
echo "Config rollback reliability json:   $JSON_PATH"
if [[ "$OVERALL_STATUS" != "PASS" ]]; then
  tail -n 60 "$LOG_PATH" || true
  exit 1
fi
