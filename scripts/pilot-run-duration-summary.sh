#!/usr/bin/env bash
set -euo pipefail

REPORT_DIR="${REPORT_DIR:-./var/agent/pilot-runs}"
OUTPUT_PATH="${OUTPUT_PATH:-$REPORT_DIR/summary-latest.md}"

if [[ ! -d "$REPORT_DIR" ]]; then
  echo "Pilot report directory not found: $REPORT_DIR"
  exit 1
fi

reports=()
while IFS= read -r report; do
  reports+=("$report")
done < <(find "$REPORT_DIR" -maxdepth 1 -type f -name '*.md' ! -name 'summary-*.md' | sort)
if [[ "${#reports[@]}" -eq 0 ]]; then
  echo "No pilot run markdown reports found in $REPORT_DIR"
  exit 1
fi

durations=()
pass_durations=()
pass_count=0
fail_count=0

table_rows=()
for report in "${reports[@]}"; do
  status="$(awk -F': ' '/^Status:/ {print $2; exit}' "$report" | tr -d '\r')"
  duration="$(awk -F': ' '/^Total Duration \(seconds\):/ {print $2; exit}' "$report" | tr -d '\r')"
  if [[ "$duration" =~ ^[0-9]+$ ]]; then
    durations+=("$duration")
  else
    duration="n/a"
  fi

  if [[ "$status" == "PASS" ]]; then
    pass_count=$((pass_count + 1))
    if [[ "$duration" =~ ^[0-9]+$ ]]; then
      pass_durations+=("$duration")
    fi
  elif [[ "$status" == "FAIL" ]]; then
    fail_count=$((fail_count + 1))
  fi

  table_rows+=("| $(basename "$report") | ${status:-unknown} | $duration |")
done

count="${#durations[@]}"
if [[ "$count" -eq 0 ]]; then
  echo "No numeric duration values found in reports under $REPORT_DIR"
  exit 1
fi

sorted_durations=()
while IFS= read -r value; do
  sorted_durations+=("$value")
done < <(printf '%s\n' "${durations[@]}" | sort -n)
sum=0
for value in "${sorted_durations[@]}"; do
  sum=$((sum + value))
done

min="${sorted_durations[0]}"
max="${sorted_durations[$((count - 1))]}"
p90_index=$(( (9 * count + 9) / 10 - 1 ))
p90="${sorted_durations[$p90_index]}"
average="$(awk "BEGIN { printf \"%.2f\", $sum / $count }")"
generated_at="$(date -u +"%Y-%m-%d %H:%M:%SZ")"

pass_duration_count="${#pass_durations[@]}"
pass_p90="n/a"
if [[ "$pass_duration_count" -gt 0 ]]; then
  sorted_pass_durations=()
  while IFS= read -r value; do
    sorted_pass_durations+=("$value")
  done < <(printf '%s\n' "${pass_durations[@]}" | sort -n)
  pass_p90_index=$(( (9 * pass_duration_count + 9) / 10 - 1 ))
  pass_p90="${sorted_pass_durations[$pass_p90_index]}"
fi

{
  echo "# Pilot Run Duration Summary"
  echo
  echo "Date (UTC): $generated_at"
  echo "Reports analyzed: ${#reports[@]}"
  echo "PASS reports: $pass_count"
  echo "FAIL reports: $fail_count"
  echo "Duration count: $count"
  echo "Duration min (s): $min"
  echo "Duration max (s): $max"
  echo "Duration avg (s): $average"
  echo "Duration p90 (s): $p90"
  echo "PASS duration count: $pass_duration_count"
  echo "PASS duration p90 (s): $pass_p90"
  echo
  echo "| Report | Status | Total Duration (s) |"
  echo "| --- | --- | ---: |"
  for row in "${table_rows[@]}"; do
    echo "$row"
  done
} >"$OUTPUT_PATH"

echo "Pilot run summary written to $OUTPUT_PATH"
