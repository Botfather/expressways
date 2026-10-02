# Week 1 Release Channel Model

Date: March 26, 2026  
Status: Draft channel policy

## Channels

| Channel | Purpose | Stability Expectation | Upgrade Cadence |
| --- | --- | --- | --- |
| Alpha | Fast validation of in-progress milestone work | May include sharp edges; limited backward guarantees | Frequent (as needed) |
| Beta | Candidate path for pilot operators | Feature-complete for milestone scope with rehearsed rollback | Weekly/biweekly |
| Stable | Default operator recommendation | Highest confidence for documented scope | Scheduled release train |

## Entry Gates

### Alpha Entry

1. Core tests for changed surfaces pass.
2. Auth/policy/audit invariants preserved.
3. Operator-visible docs updated for behavior changes.

### Beta Entry

1. Alpha burn-in completed with no open P0/P1 defects.
2. Installer/bootstrap runbook validated on clean environments.
3. Rollback checklist rehearsed for the candidate build.

### Stable Entry

1. Milestone acceptance criteria complete for included scope.
2. Security review has no unresolved high-severity findings.
3. Release artifacts are signed with published checksums and SBOM.

## Exit / Rollback Rules

1. Any Sev 1 regression in default workflows triggers channel freeze.
2. Failed upgrade or startup regression requires rollback-ready artifact availability.
3. Security invariant regressions trigger immediate de-promotion until fixed.

## Artifact and Communication Requirements

1. Every channel release includes release notes, known issues, and upgrade/rollback instructions.
2. Channel status is visible in release metadata and docs.
3. Pilot operators are instructed to remain on Beta unless Stable is explicitly required.

