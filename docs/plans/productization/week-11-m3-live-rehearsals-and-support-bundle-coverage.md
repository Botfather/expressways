# Week 11 (M3): Live Rehearsal Closure and Support-Bundle Incident Coverage

Date: March 26, 2026  
Owner lanes: E, B, C

## Objective

Close remaining M3 acceptance gaps by:

1. validating support-bundle diagnostic sufficiency for the top 10 expected incident classes,
2. running three full live rehearsals with documented outcomes,
3. publishing repeatable local + CI evidence outputs.

## Delivered Changes

1. Added support-bundle coverage validation command in `expresswaysctl`:
   - `validate-support-bundle`
   - input: `--bundle <path>`
   - output: JSON report with per-incident required/missing evidence and pass/fail summary
   - optional persisted output via `--output <path>`

2. Added top-10 incident evidence matrix checks:
   - degraded audit path
   - degraded storage path
   - storage pressure/retention behavior
   - auth revocation denial
   - policy denial
   - quota denial
   - issuer/principal mismatch
   - registry discovery staleness
   - adopter probe health
   - config-change regression

3. Added Week 11 live-suite automation:
   - `scripts/rehearsal-m3-live-suite.sh`
   - runs:
     - `scripts/rehearsal-clean-machine.sh`
     - `scripts/rehearsal-rollback.sh`
     - `scripts/rehearsal-reliability-denials.sh`
     - support-bundle coverage validation
   - emits consolidated markdown/json reports in `./var/agent/pilot-runs/`

4. Added config-audit evidence seeding in live rehearsals before support-bundle export:
   - `scripts/rehearsal-clean-machine.sh`
   - `scripts/rehearsal-rollback.sh`

5. Added operator make targets:
   - `make validate-support-bundle`
   - `make rehearse-m3-live-suite`

6. Added CI execution and artifact publishing:
   - `.github/workflows/ci.yml`
   - new job: `m3-live-rehearsal-suite`
   - uploads suite report JSON/MD, support-bundle coverage JSON, and rehearsal logs

## Validation Evidence

Automated tests:

```bash
cargo test -p expressways-client
```

Live rehearsal suite:

```bash
bash scripts/rehearsal-m3-live-suite.sh
```

Latest successful suite outputs:

- `var/agent/pilot-runs/m3-live-rehearsal-suite-20260326T142346Z.md`
- `var/agent/pilot-runs/m3-live-rehearsal-suite-20260326T142346Z.json`
- `var/agent/pilot-runs/support-bundle-coverage-20260326T142346Z.json`

## M3 Acceptance Progress

This week closes two M3 acceptance items:

- three full live rehearsals pass with documented outcomes,
- support bundle is sufficient to diagnose top 10 expected incidents (validated by coverage report).
