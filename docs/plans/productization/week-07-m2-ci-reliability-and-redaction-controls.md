# Week 7 M2 CI Reliability and Redaction Controls

Date: March 26, 2026  
Status: Delivered

## Objective

Close remaining M2 follow-up risk by adding continuous rollback-reliability evidence capture in CI and hardening support-bundle export with sensitive-line redaction controls.

## Delivered Changes

1. Added config rollback reliability rehearsal automation script:
   - `scripts/rehearsal-config-rollback-reliability.sh`
   - runs `cargo test -p expressways-console rollback_reliability_meets_m2_target -- --nocapture`
   - emits markdown + JSON evidence reports under `var/agent/pilot-runs/`
2. Added CI job for rollback reliability trend evidence:
   - `.github/workflows/ci.yml` now runs the Week 7 rehearsal script
   - uploads generated reliability reports/logs as build artifacts
3. Added support-bundle redaction controls:
   - new `expresswaysctl export-support-bundle` flags:
     - `--redact-sensitive` (default `true`)
     - `--redact-placeholder` (default `[REDACTED]`)
   - applies redaction on audit/config-audit/log line windows before bundle serialization
   - bundle now includes `redaction` metadata (`enabled`, `placeholder`, `redacted_lines`)
4. Added/updated tests:
   - redaction behavior for support-bundle line windows (`read_head_and_tail_lines_redacts_sensitive_entries`)
   - existing rollback reliability test now emits parseable reliability metric output for rehearsal logs

## Week 7 Validation

1. `cargo test -p expressways-client -p expressways-console`
2. `bash scripts/rehearsal-config-rollback-reliability.sh`
3. `cargo fmt`

## Follow-Up (Week 8+)

1. Expand schema-driven editors to nested list/table-heavy sections (`auth.*`, `policy.rules`, `quotas.profiles`).
2. [x] Add policy-driven redaction profiles (strict/standard) for broader pilot/support workflows. (Delivered in Week 9 item 2)
3. [x] Add release-side retention policy for CI reliability evidence artifacts. (Delivered in Week 9 item 3)
