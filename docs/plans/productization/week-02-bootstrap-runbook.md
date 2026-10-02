# Week 2 Bootstrap and First-Run Verification Runbook

Date: March 26, 2026  
Status: Active runbook with automated rehearsal scripts

## Goal

Reach a healthy local broker with validated first-run operations using guarded token generation and scripted verification.

## Steps

1. Generate local keys and guarded admin token:

```bash
make bootstrap-local
```

2. Start broker (separate terminal):

```bash
make run-expressways
```

3. Run first-run verification:

```bash
make verify-first-run
```

4. Export a support bundle for rehearsal evidence:

```bash
make export-support-bundle
```

`verify-first-run` checks:

1. health endpoint,
2. metrics endpoint,
3. baseline topic create/publish/consume path.
4. includes bounded startup health retries before running full checks.

## Guardrails

`make generate-admin-token` (used by `bootstrap-local`) validates before issuing:

1. principal is registered in `auth.principals`,
2. principal status is active (unless explicitly overridden in command usage),
3. principal has at least one policy rule,
4. principal allows the configured issuer key id.

## Rollback Outline (Failed Upgrade)

1. Stop broker service.
2. Restore previous broker and CLI binaries from prior release bundle.
3. Restart broker with previous known-good config.
4. Re-run `make verify-first-run` before reopening operator traffic.

## Follow-Up

Week 3 completion status:

1. [x] Added clean-machine timing capture with evidence report output:
   - `make rehearse-clean-machine`
   - report path: `./var/agent/pilot-runs/clean-machine-<timestamp>.md`
2. [x] Converted rollback outline into automated rehearsal script with pass/fail output:
   - `make rehearse-rollback`
   - report path: `./var/agent/pilot-runs/rollback-rehearsal-<timestamp>.md`
3. [x] Added rehearsal duration rollup summary:
   - `make summarize-pilot-runs`
   - summary path: `./var/agent/pilot-runs/summary-latest.md`
