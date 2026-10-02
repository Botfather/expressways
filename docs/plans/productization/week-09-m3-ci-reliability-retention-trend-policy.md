# Week 9 M3 CI Reliability Evidence Retention and Trend Policy

Date: March 26, 2026  
Status: Delivered (Item 3)

## Objective

Close Week 9 backlog item 3 by defining explicit retention policy for rollback-reliability CI artifacts and automating trend-summary generation for operator visibility.

## Delivered Changes

1. Added a dedicated rollback-reliability trend summarizer:
   - `scripts/rollback-reliability-trend-summary.sh`
   - consumes `config-rollback-reliability-*.json` evidence files
   - emits markdown + JSON trend summaries with:
     - report-window totals and pass/fail mix
     - latest sample health (`status`, `success_rate_percent`, duration)
     - rolling success-rate and duration statistics (min/avg/p90/max)
     - explicit policy metadata (`artifact_retention_days`, `trend_window_reports`, pass criterion)
2. Added a local operator target for trend generation:
   - `make summarize-rollback-reliability-trend`
3. Hardened CI rollback-reliability evidence handling in `.github/workflows/ci.yml`:
   - defines policy variables:
     - `ROLLBACK_RELIABILITY_ARTIFACT_RETENTION_DAYS=30`
     - `ROLLBACK_RELIABILITY_TREND_WINDOW_REPORTS=30`
   - generates per-run trend summary artifacts after rehearsal execution
   - publishes trend markdown into GitHub job summary for fast inspection
   - uploads raw evidence and trend artifacts with explicit `retention-days` policy
4. Updated operator docs and milestone references:
   - `README.md` rehearsal command set includes trend summary target
   - Week 9 execution tracker and follow-up docs marked complete for retention/trend policy delivery

## Validation

1. `bash -n scripts/rollback-reliability-trend-summary.sh`
2. `bash scripts/rehearsal-config-rollback-reliability.sh`
3. `bash scripts/rollback-reliability-trend-summary.sh`

## Follow-Up (Week 10+)

1. Add cross-run CI trend rollup from persisted artifacts or metrics backend to provide long-horizon charts beyond per-run summaries.
