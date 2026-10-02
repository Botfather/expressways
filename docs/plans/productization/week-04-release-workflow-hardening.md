# Week 4 Release Workflow Hardening (M1 Closure)

Date: March 26, 2026  
Status: Delivered

## Objective

Close remaining M1 release-path gaps by hardening the release skeleton with compatibility checks and manifest schema validation.

## Delivered Changes

1. Hardened release workflow skeleton:
   - file: `.github/workflows/release-skeleton.yml`
   - staged bundle-internal `release-notes.md` and `checksums.txt`
   - added rollback-compatibility job for artifact layout checks
   - added release-manifest job with schema-constraint validation
   - added optional unsigned Tauri desktop bundle job (workflow-dispatch controlled)
2. Added rollback artifact compatibility checker:
   - `scripts/check-rollback-artifact-compatibility.sh`
3. Added release manifest schema constraints:
   - schema file: `.github/release-manifest.schema.json`
   - validator script: `scripts/validate-release-manifest.sh`
4. Added pilot run duration summary utility for install-time rollups:
   - `scripts/pilot-run-duration-summary.sh`
   - `make summarize-pilot-runs`
5. Hardened local service/rehearsal reliability:
   - `scripts/expressways-service.sh` now reconciles stale PID files and stops orphan service processes
   - `make verify-first-run` now includes bounded health retries before metrics/publish/consume checks

## Week 4 Validation

1. `bash -n scripts/validate-release-manifest.sh`
2. `bash -n scripts/check-rollback-artifact-compatibility.sh`
3. `bash scripts/check-rollback-artifact-compatibility.sh <fixture-artifact>`
4. `bash scripts/validate-release-manifest.sh <fixture-manifest>`
5. `make summarize-pilot-runs`

## Follow-Up (M2+ / M4)

1. Add full rollback rehearsal CI job with runtime boot + verify checks and artifact retention.
2. Add signing/notarization + SBOM/provenance for non-skeleton release channels.
