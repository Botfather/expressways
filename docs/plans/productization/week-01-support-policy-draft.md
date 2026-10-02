# Week 1 Support Policy Draft

Date: March 26, 2026  
Status: Draft for pilot use

## Purpose

Define what support covers for Expressways Desktop pilot operators and what remains out of scope until GA candidate.

## Covered in Pilot

1. Installer and bootstrap path for supported platforms in current milestone scope.
2. Broker startup, health, metrics, adopters status, and first-run verification workflow.
3. Auth/policy failure triage for documented capability and policy paths.
4. Config console workflows for supported sections (form + raw mode where implemented).
5. Recovery guidance for degraded mode, storage pressure, and denied request diagnostics.

## Not Covered in Pilot

1. Multi-node deployment or cluster operations.
2. Dynamic plugin loading or arbitrary runtime extensions.
3. Managed cloud control plane, remote tenancy, or billing flows.
4. Custom enterprise integration commitments not represented in published interfaces.
5. Recovery guarantees beyond documented backup/restore and rollback capabilities.

## Support Severity Targets (Pilot)

| Severity | Example | Initial Response Target | Owner Lane |
| --- | --- | --- | --- |
| Sev 1 | Broker unavailable or data-path blocked for pilot workflow | 4 business hours | B/E |
| Sev 2 | Significant feature impairment with workaround | 1 business day | A/B |
| Sev 3 | Minor defect or docs mismatch with workaround | 3 business days | A/C |

## Operator Responsibilities

1. Keep runtime artifacts in supported local paths (`./var`, `./tmp`) and avoid checked-in secrets.
2. Provide support bundle artifacts when filing incidents.
3. Use supported release channels and upgrade process for pilot environments.

## Escalation and Exit Criteria

- Repeated Sev 1 regressions in the same area trigger milestone gate review.
- Any auth/policy/audit invariant violation escalates directly to Lane D sign-off.
- Support scope updates require Product Lead and Tech Lead approval.

