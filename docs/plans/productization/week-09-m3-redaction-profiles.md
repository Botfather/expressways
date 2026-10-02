# Week 9 M3 Support-Bundle Redaction Profiles

Date: March 26, 2026  
Status: Delivered (Item 2)

## Objective

Close Week 9 backlog item 2 by adding policy-driven support-bundle redaction profiles (`standard` and `strict`) with explicit operator-facing documentation.

## Delivered Changes

1. Extended `expresswaysctl export-support-bundle` CLI redaction controls:
   - `--redaction-profile standard|strict` (default `standard`)
   - retained `--redact-sensitive` as an explicit enable/disable gate
   - retained `--redact-placeholder` replacement text override
2. Added profile-aware redaction policy behavior in `crates/expressways-client/src/bin/expresswaysctl.rs`:
   - `standard` preserves existing targeted marker/token matching behavior
   - `strict` applies broader marker, assignment, and opaque-token heuristics intended for higher-sensitivity incident-sharing workflows
3. Extended support-bundle redaction metadata payload:
   - redaction summary now includes `profile` and policy id (`policy`) alongside existing `enabled`, `placeholder`, and `redacted_lines`
4. Added regression coverage:
   - strict profile redacts lines that standard profile intentionally leaves visible
   - policy id mapping for profile names is covered by tests
5. Updated operator docs and schema notes:
   - `README.md` support-bundle example and metadata notes
   - `docs/plans/productization/week-02-support-bundle-schema.md`
   - follow-up trackers in Week 7/8 docs

## Validation

1. `cargo test -p expressways-client`
2. `cargo fmt`

## Follow-Up (Week 9+)

1. [x] Add retention/trend policy automation for rollback-reliability CI artifacts (Delivered in Week 9 item 3: `week-09-m3-ci-reliability-retention-trend-policy.md`).
