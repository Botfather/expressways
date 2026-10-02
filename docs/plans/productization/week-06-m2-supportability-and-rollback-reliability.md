# Week 6 M2 Supportability and Rollback Reliability

Date: March 26, 2026  
Status: Delivered

## Objective

Advance M2 safe-operations criteria by automating rollback reliability verification and improving support-bundle diagnostics with config-audit context.

## Delivered Changes

1. Added rollback reliability automation test for config-console apply/rollback paths:
   - new test `rollback_reliability_meets_m2_target` in `apps/expressways-console/src-tauri/src/lib.rs`
   - executes 100 apply/rollback cycles and enforces success rate `>= 99%`
2. Extended support bundle export to include config-audit head/tail slices:
   - CLI command adds `--config-audit-log` (default `./var/agent/config-audit/entries.jsonl`)
   - support bundle JSON now includes `config_audit` summary (line count + head/tail windows)
   - files: `crates/expressways-client/src/bin/expresswaysctl.rs`, `Makefile`
3. Added support-bundle coverage tests for labeled audit summaries:
   - verifies head/tail extraction and missing-file warning behavior for config-audit logs
4. Updated support-bundle schema and usage docs:
   - `docs/plans/productization/week-02-support-bundle-schema.md`
   - `README.md` support-bundle example command now includes `--config-audit-log`

## Week 6 Validation

1. `cargo test -p expressways-client -p expressways-console`
2. `cargo fmt`

## Follow-Up (Week 7+)

1. Add config-console integration/e2e runs in CI for rollback reliability trend tracking over time.
2. Extend schema-driven editors to nested table/list-heavy sections (`auth.*`, `policy.rules`, `quotas.profiles`).
3. Add support-bundle redaction controls for sensitive fields before broader pilot distribution.
