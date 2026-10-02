# Week 12 (M3): DR Restore to Healthy State on Clean Environment

Date: March 26, 2026  
Owner lanes: E, B, C

## Objective

Close the remaining M3 acceptance criterion by proving backup restore returns a clean environment to healthy broker operation.

## Delivered Changes

1. Added isolated DR restore rehearsal automation:
   - `scripts/rehearsal-dr-restore-clean-env.sh`
   - creates an isolated workspace config/state under `var/agent/pilot-runs/dr-restore-workdir`
   - generates issuer keys + developer token
   - validates baseline broker health
   - performs runtime backup
   - corrupts/deletes runtime state
   - restores from backup
   - verifies post-restore health + metrics
   - emits markdown/json evidence reports

2. Added operator make target:
   - `make rehearse-dr-restore-clean-env`

3. Added CI rehearsal job and artifact publishing:
   - `.github/workflows/ci.yml`
   - new job: `dr-restore-clean-env`
   - uploads report JSON/MD and DR restore logs

## Validation Evidence

Executed locally:

```bash
bash scripts/rehearsal-dr-restore-clean-env.sh
```

Successful outputs:

- `var/agent/pilot-runs/dr-restore-clean-env-20260326T142851Z.md`
- `var/agent/pilot-runs/dr-restore-clean-env-20260326T142851Z.json`

## M3 Acceptance Closure

This week closes the final M3 acceptance item:

- restore from backup to healthy state verified on a clean environment.
