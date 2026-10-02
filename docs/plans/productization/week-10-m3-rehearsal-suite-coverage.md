# Week 10 (M3): Rehearsal Suite Coverage for Degraded/Denial Paths

Date: March 26, 2026  
Owner lanes: E, B, C

## Objective

Add explicit rehearsal-suite automation for degraded mode, storage pressure, and auth/policy denial paths with operator-visible evidence outputs.

## Delivered Changes

1. Added rehearsal automation script:
   - `scripts/rehearsal-reliability-denials.sh`
   - Emits machine-readable evidence files (`.md` + `.json`) under `./var/agent/pilot-runs/`.
   - Captures per-scenario logs under `./var/agent/pilot-runs/logs/`.

2. Added coverage scenarios:
   - degraded startup with unavailable audit,
   - degraded startup with unavailable storage,
   - storage pressure denial,
   - auth revocation denial,
   - policy denial,
   - quota denial.

3. Added CI rehearsal job:
   - Workflow: `.github/workflows/ci.yml`
   - New job: `reliability-denials-rehearsal`
   - Publishes markdown summary to `GITHUB_STEP_SUMMARY`.
   - Uploads rehearsal markdown/json/log artifacts.

4. Added local operator make target:
   - `make rehearse-reliability-denials`

5. Added policy-denial regression test:
   - `policy_denials_are_rejected_and_audited` in `expressways-server` tests.

## Validation Evidence

Executed locally:

```bash
cargo test -p expressways-server policy_denials_are_rejected_and_audited -- --nocapture
bash scripts/rehearsal-reliability-denials.sh
```

Expected outputs:

- `var/agent/pilot-runs/reliability-denials-rehearsal-<timestamp>.md`
- `var/agent/pilot-runs/reliability-denials-rehearsal-<timestamp>.json`
- `var/agent/pilot-runs/logs/<timestamp>-<scenario>.log`
