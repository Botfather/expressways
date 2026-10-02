#!/usr/bin/env bash
set -euo pipefail

REPORT_DIR="${REPORT_DIR:-./var/agent/pilot-runs}"
OUTPUT_MD="${OUTPUT_MD:-$REPORT_DIR/config-rollback-reliability-trend-latest.md}"
OUTPUT_JSON="${OUTPUT_JSON:-$REPORT_DIR/config-rollback-reliability-trend-latest.json}"
WINDOW_SIZE="${WINDOW_SIZE:-30}"
ARTIFACT_RETENTION_DAYS="${ARTIFACT_RETENTION_DAYS:-30}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to generate rollback reliability trend summaries."
  exit 1
fi

if [[ ! "$WINDOW_SIZE" =~ ^[0-9]+$ ]] || [[ "$WINDOW_SIZE" -eq 0 ]]; then
  echo "WINDOW_SIZE must be a positive integer (received: $WINDOW_SIZE)"
  exit 1
fi

if [[ ! "$ARTIFACT_RETENTION_DAYS" =~ ^[0-9]+$ ]] || [[ "$ARTIFACT_RETENTION_DAYS" -eq 0 ]]; then
  echo "ARTIFACT_RETENTION_DAYS must be a positive integer (received: $ARTIFACT_RETENTION_DAYS)"
  exit 1
fi

mkdir -p "$REPORT_DIR"
mkdir -p "$(dirname "$OUTPUT_MD")" "$(dirname "$OUTPUT_JSON")"

reports=()
while IFS= read -r report; do
  reports+=("$report")
done < <(find "$REPORT_DIR" -maxdepth 1 -type f -name 'config-rollback-reliability-*.json' ! -name '*trend*' | sort)

if [[ "${#reports[@]}" -eq 0 ]]; then
  echo "No rollback reliability reports found in $REPORT_DIR"
  exit 1
fi

window_reports=()
if [[ "${#reports[@]}" -le "$WINDOW_SIZE" ]]; then
  window_reports=("${reports[@]}")
else
  start_index=$(( ${#reports[@]} - WINDOW_SIZE ))
  for ((index=start_index; index<${#reports[@]}; index++)); do
    window_reports+=("${reports[$index]}")
  done
fi

pass_count=0
fail_count=0
unknown_count=0
attempts_total=0
successes_total=0
rates=()
durations=()
table_rows=()
summary_entries='[]'

for report in "${window_reports[@]}"; do
  report_name="$(basename "$report")"
  timestamp="$(jq -r '.timestamp_utc // "unknown"' "$report")"
  status_raw="$(jq -r '.status // "unknown"' "$report")"
  status="$(printf '%s' "$status_raw" | tr '[:lower:]' '[:upper:]')"
  success_rate="$(jq -r 'if .success_rate_percent == null then "n/a" else (.success_rate_percent | tostring) end' "$report")"
  attempts="$(jq -r 'if .attempts == null then "n/a" else (.attempts | tostring) end' "$report")"
  successes="$(jq -r 'if .successes == null then "n/a" else (.successes | tostring) end' "$report")"
  duration_seconds="$(jq -r 'if .total_duration_seconds == null then "n/a" else (.total_duration_seconds | tostring) end' "$report")"

  if [[ "$status" == "PASS" ]]; then
    pass_count=$((pass_count + 1))
  elif [[ "$status" == "FAIL" ]]; then
    fail_count=$((fail_count + 1))
  else
    unknown_count=$((unknown_count + 1))
  fi

  if [[ "$success_rate" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    rates+=("$success_rate")
  fi

  if [[ "$duration_seconds" =~ ^[0-9]+$ ]]; then
    durations+=("$duration_seconds")
  fi

  if [[ "$attempts" =~ ^[0-9]+$ ]]; then
    attempts_total=$((attempts_total + attempts))
  fi

  if [[ "$successes" =~ ^[0-9]+$ ]]; then
    successes_total=$((successes_total + successes))
  fi

  table_rows+=("| $report_name | $timestamp | ${status:-unknown} | $success_rate | $attempts | $successes | $duration_seconds |")

  entry="$(jq -c --arg report_file "$report_name" '. + {report_file: $report_file}' "$report")"
  summary_entries="$(jq -c --argjson entry "$entry" '. + [$entry]' <<<"$summary_entries")"
done

window_count="${#window_reports[@]}"
report_count="${#reports[@]}"

window_pass_rate="n/a"
if [[ "$window_count" -gt 0 ]]; then
  window_pass_rate="$(awk "BEGIN { printf \"%.2f\", ($pass_count * 100) / $window_count }")"
fi

success_rate_count="${#rates[@]}"
success_rate_min="n/a"
success_rate_max="n/a"
success_rate_avg="n/a"
success_rate_p90="n/a"
if [[ "$success_rate_count" -gt 0 ]]; then
  sorted_rates=()
  while IFS= read -r value; do
    sorted_rates+=("$value")
  done < <(printf '%s\n' "${rates[@]}" | sort -n)

  success_rate_min="${sorted_rates[0]}"
  success_rate_max="${sorted_rates[$((success_rate_count - 1))]}"
  success_rate_p90_index=$(( (9 * success_rate_count + 9) / 10 - 1 ))
  success_rate_p90="${sorted_rates[$success_rate_p90_index]}"
  success_rate_avg="$(printf '%s\n' "${sorted_rates[@]}" | awk '{sum+=$1} END { if (NR>0) printf "%.4f", sum/NR; else print "n/a" }')"
fi

duration_count="${#durations[@]}"
duration_min="n/a"
duration_max="n/a"
duration_avg="n/a"
duration_p90="n/a"
if [[ "$duration_count" -gt 0 ]]; then
  sorted_durations=()
  while IFS= read -r value; do
    sorted_durations+=("$value")
  done < <(printf '%s\n' "${durations[@]}" | sort -n)

  duration_min="${sorted_durations[0]}"
  duration_max="${sorted_durations[$((duration_count - 1))]}"
  duration_p90_index=$(( (9 * duration_count + 9) / 10 - 1 ))
  duration_p90="${sorted_durations[$duration_p90_index]}"
  duration_avg="$(printf '%s\n' "${sorted_durations[@]}" | awk '{sum+=$1} END { if (NR>0) printf "%.2f", sum/NR; else print "n/a" }')"
fi

latest_report_path="${window_reports[$((window_count - 1))]}"
latest_report_name="$(basename "$latest_report_path")"
latest_timestamp="$(jq -r '.timestamp_utc // "unknown"' "$latest_report_path")"
latest_status="$(jq -r '.status // "unknown"' "$latest_report_path" | tr '[:lower:]' '[:upper:]')"
latest_success_rate="$(jq -r 'if .success_rate_percent == null then "n/a" else (.success_rate_percent | tostring) end' "$latest_report_path")"
latest_duration="$(jq -r 'if .total_duration_seconds == null then "n/a" else (.total_duration_seconds | tostring) end' "$latest_report_path")"

attempt_success_rate="n/a"
if [[ "$attempts_total" -gt 0 ]]; then
  attempt_success_rate="$(awk "BEGIN { printf \"%.4f\", ($successes_total * 100) / $attempts_total }")"
fi

generated_at="$(date -u +"%Y-%m-%d %H:%M:%SZ")"

{
  echo "# Config Rollback Reliability Trend Summary"
  echo
  echo "Date (UTC): $generated_at"
  echo "Artifact Retention Policy (days): $ARTIFACT_RETENTION_DAYS"
  echo "Trend Window Size (reports): $WINDOW_SIZE"
  echo "Pass Criterion (%): >= 99"
  echo
  echo "Reports Available: $report_count"
  echo "Reports Analyzed: $window_count"
  echo "PASS reports: $pass_count"
  echo "FAIL reports: $fail_count"
  echo "Unknown reports: $unknown_count"
  echo "Window PASS rate (%): $window_pass_rate"
  echo
  echo "Latest report: $latest_report_name"
  echo "Latest timestamp (UTC): $latest_timestamp"
  echo "Latest status: $latest_status"
  echo "Latest success rate (%): $latest_success_rate"
  echo "Latest duration (s): $latest_duration"
  echo
  echo "Success-rate samples: $success_rate_count"
  echo "Success-rate min (%): $success_rate_min"
  echo "Success-rate avg (%): $success_rate_avg"
  echo "Success-rate p90 (%): $success_rate_p90"
  echo "Success-rate max (%): $success_rate_max"
  echo
  echo "Duration samples: $duration_count"
  echo "Duration min (s): $duration_min"
  echo "Duration avg (s): $duration_avg"
  echo "Duration p90 (s): $duration_p90"
  echo "Duration max (s): $duration_max"
  echo
  echo "Attempt totals (sampled): $attempts_total"
  echo "Success totals (sampled): $successes_total"
  echo "Aggregate success rate from attempts (%): $attempt_success_rate"
  echo
  echo "| Report | Timestamp UTC | Status | Success Rate (%) | Attempts | Successes | Duration (s) |"
  echo "| --- | --- | --- | ---: | ---: | ---: | ---: |"
  for row in "${table_rows[@]}"; do
    echo "$row"
  done
} >"$OUTPUT_MD"

json_number_or_null() {
  local value="$1"
  if [[ "$value" == "n/a" ]]; then
    echo "null"
  else
    echo "$value"
  fi
}

jq -n \
  --arg generated_at_utc "$generated_at" \
  --argjson artifact_retention_days "$ARTIFACT_RETENTION_DAYS" \
  --argjson trend_window_reports "$WINDOW_SIZE" \
  --argjson pass_criterion_percent 99 \
  --argjson reports_available "$report_count" \
  --argjson reports_analyzed "$window_count" \
  --argjson pass_reports "$pass_count" \
  --argjson fail_reports "$fail_count" \
  --argjson unknown_reports "$unknown_count" \
  --argjson window_pass_rate_percent "$(json_number_or_null "$window_pass_rate")" \
  --arg latest_report "$latest_report_name" \
  --arg latest_timestamp_utc "$latest_timestamp" \
  --arg latest_status "$latest_status" \
  --argjson latest_success_rate_percent "$(json_number_or_null "$latest_success_rate")" \
  --argjson latest_duration_seconds "$(json_number_or_null "$latest_duration")" \
  --argjson success_rate_samples "$success_rate_count" \
  --argjson success_rate_min_percent "$(json_number_or_null "$success_rate_min")" \
  --argjson success_rate_avg_percent "$(json_number_or_null "$success_rate_avg")" \
  --argjson success_rate_p90_percent "$(json_number_or_null "$success_rate_p90")" \
  --argjson success_rate_max_percent "$(json_number_or_null "$success_rate_max")" \
  --argjson duration_samples "$duration_count" \
  --argjson duration_min_seconds "$(json_number_or_null "$duration_min")" \
  --argjson duration_avg_seconds "$(json_number_or_null "$duration_avg")" \
  --argjson duration_p90_seconds "$(json_number_or_null "$duration_p90")" \
  --argjson duration_max_seconds "$(json_number_or_null "$duration_max")" \
  --argjson attempts_total "$attempts_total" \
  --argjson successes_total "$successes_total" \
  --argjson attempt_success_rate_percent "$(json_number_or_null "$attempt_success_rate")" \
  --argjson reports "$summary_entries" \
  '{
    generated_at_utc: $generated_at_utc,
    policy: {
      artifact_retention_days: $artifact_retention_days,
      trend_window_reports: $trend_window_reports,
      pass_criterion_percent: $pass_criterion_percent
    },
    totals: {
      reports_available: $reports_available,
      reports_analyzed: $reports_analyzed,
      pass_reports: $pass_reports,
      fail_reports: $fail_reports,
      unknown_reports: $unknown_reports,
      window_pass_rate_percent: $window_pass_rate_percent
    },
    latest: {
      report: $latest_report,
      timestamp_utc: $latest_timestamp_utc,
      status: $latest_status,
      success_rate_percent: $latest_success_rate_percent,
      duration_seconds: $latest_duration_seconds
    },
    success_rate_stats_percent: {
      samples: $success_rate_samples,
      min: $success_rate_min_percent,
      avg: $success_rate_avg_percent,
      p90: $success_rate_p90_percent,
      max: $success_rate_max_percent
    },
    duration_stats_seconds: {
      samples: $duration_samples,
      min: $duration_min_seconds,
      avg: $duration_avg_seconds,
      p90: $duration_p90_seconds,
      max: $duration_max_seconds
    },
    attempt_totals: {
      attempts: $attempts_total,
      successes: $successes_total,
      aggregate_success_rate_percent: $attempt_success_rate_percent
    },
    reports: $reports
  }' >"$OUTPUT_JSON"

echo "Config rollback reliability trend summary: $OUTPUT_MD"
echo "Config rollback reliability trend json:    $OUTPUT_JSON"
