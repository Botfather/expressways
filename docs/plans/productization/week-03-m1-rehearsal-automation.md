# Week 3 M1 Rehearsal Automation

Date: March 26, 2026  
Status: Delivered

## Objective

Automate M1 rehearsal evidence capture for clean-machine timing and rollback verification so operators can produce consistent pass/fail artifacts.

## Delivered Changes

1. Added clean-machine rehearsal script:
   - `scripts/rehearsal-clean-machine.sh`
   - runs bootstrap, broker start, first-run verification, and support bundle export
   - writes a timed markdown report to `./var/agent/pilot-runs/clean-machine-<timestamp>.md`
2. Added rollback rehearsal script:
   - `scripts/rehearsal-rollback.sh`
   - simulates failed upgrade via invalid config restart, restores known-good config, verifies recovery, and exports support bundle
   - writes a pass/fail markdown report to `./var/agent/pilot-runs/rollback-rehearsal-<timestamp>.md`
3. Added Make targets:
   - `make rehearse-clean-machine`
   - `make rehearse-rollback`
4. Added per-step log capture under:
   - `./var/agent/pilot-runs/logs/`

## Rehearsal Evidence

Validated in local run:

1. `make rehearse-clean-machine` produced a PASS report.
2. `make rehearse-rollback` produced a PASS report including expected-failure restart step.

## Follow-Up (Week 4+)

1. [ ] Add CI job that runs full rollback rehearsal in a controlled environment with artifact retention (beyond bundle-layout compatibility checks).
2. [x] Added p90 duration rollup script across `var/agent/pilot-runs/*.md`:
   - `make summarize-pilot-runs`
   - script: `scripts/pilot-run-duration-summary.sh`
