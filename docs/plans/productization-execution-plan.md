# Expressways Productization Execution Plan

Date: March 28, 2026
Status: In execution (M3 complete; Week 14 complete; M4 in progress)
Planning horizon: 16 weeks to GA candidate

## Objective

Package Expressways as a product that can be installed, operated, upgraded, and supported without deep internal repository context.

This plan targets a **local-first single-node product** first, then expands to a hardened self-hosted team deployment path.

## Product Definition (Scope Lock)

Primary SKU in this plan:

- **Expressways Desktop**: local broker, local console, guided bootstrap, service lifecycle management, and optional Nanobot runtime integration.

Deferred from this plan:

- multi-node clustering and distributed coordination,
- dynamic runtime plugin loading,
- advanced hosted control plane,
- managed-cloud billing/control plane.

## Team and Ownership Lanes

Execution assumptions:

- 1 Product Lead (scope, UX priorities, acceptance sign-off),
- 1 Tech Lead (architecture and sequencing),
- 2 Core Rust Engineers (broker/runtime/auth/policy),
- 1 App Engineer (console UX/Tauri packaging),
- 1 Platform Engineer (release, signing, CI, installers),
- shared Security/Compliance reviewer.

Ownership lanes:

- **Lane A: Product and UX** (Product Lead + App Engineer)
- **Lane B: Core Runtime and APIs** (Tech Lead + Core Engineers)
- **Lane C: Packaging and Release** (Platform Engineer)
- **Lane D: Security and Compliance** (Tech Lead + Security reviewer)
- **Lane E: Ops, Reliability, Supportability** (Core + Platform)

## Milestones

## Execution Tracker

- Week 1 (M0): complete on March 26, 2026
- Week 2 (M1 tranche): complete on March 26, 2026
- Week 3 (M1 tranche): complete on March 26, 2026
- Week 4 (M1 tranche): complete on March 26, 2026
- Week 5 (M2 tranche): complete on March 26, 2026
- Week 6 (M2 tranche): complete on March 26, 2026
- Week 7 (M2 tranche): complete on March 26, 2026
- Week 8 (M2 tranche): complete on March 26, 2026
- Week 9 (M3 tranche): complete on March 26, 2026
- Week 10 (M3 tranche): complete on March 26, 2026
- Week 11 (M3 tranche): complete on March 26, 2026
- Week 12 (M3 tranche): complete on March 26, 2026
- Week 13 (M4 tranche): complete on March 27, 2026
- Week 14 (M4 tranche): complete on March 28, 2026
- Weeks 15-16 (M4): pending

## M0 - Product Contract and Pilot Criteria (Week 1)

Owner lanes: A, B, D

Deliverables:

- product contract doc (target user, top 5 workflows, non-goals),
- pilot acceptance checklist,
- support policy draft (what is covered vs not covered),
- release channel model (alpha/beta/stable).

Week 1 closure artifacts:

- Product contract: [docs/plans/productization/week-01-m0-product-contract.md](./productization/week-01-m0-product-contract.md)
- Pilot acceptance checklist: [docs/plans/productization/week-01-pilot-acceptance-checklist.md](./productization/week-01-pilot-acceptance-checklist.md)
- Support policy draft: [docs/plans/productization/week-01-support-policy-draft.md](./productization/week-01-support-policy-draft.md)
- Release channel model: [docs/plans/productization/week-01-release-channel-model.md](./productization/week-01-release-channel-model.md)

Acceptance criteria:

- one agreed SKU statement and non-goal list,
- measurable pilot success metrics defined,
- launch blocking risks documented with owners.

## M1 - Install and Bootstrap in < 15 Minutes (Weeks 2-4)

Owner lanes: B, C

Deliverables:

- signed installer outputs for macOS and Linux package path (Windows optional in this phase),
- one-command bootstrap (`make` + console flow) that creates keys, token, and local data dirs,
- service lifecycle scripts integrated and surfaced in console,
- first-run verification flow (health + metrics + basic topic publish/consume).

M1 artifacts (through Week 4):

- Installer artifact matrix: [docs/plans/productization/week-02-installer-artifact-matrix.md](./productization/week-02-installer-artifact-matrix.md)
- CI release workflow skeleton: [.github/workflows/release-skeleton.yml](../../.github/workflows/release-skeleton.yml)
- Bootstrap and first-run runbook: [docs/plans/productization/week-02-bootstrap-runbook.md](./productization/week-02-bootstrap-runbook.md)
- Config console mixed form + raw mode: [docs/plans/productization/week-02-config-console-mixed-mode.md](./productization/week-02-config-console-mixed-mode.md)
- Console service + operator controls: [docs/plans/productization/week-03-service-operator-console-controls.md](./productization/week-03-service-operator-console-controls.md)
- M1 rehearsal automation: [docs/plans/productization/week-03-m1-rehearsal-automation.md](./productization/week-03-m1-rehearsal-automation.md)
- Release workflow hardening + manifest/compatibility gates: [docs/plans/productization/week-04-release-workflow-hardening.md](./productization/week-04-release-workflow-hardening.md)

Acceptance criteria:

- clean-machine install runbook succeeds in under 15 minutes,
- no manual token-principal mismatch in default path,
- rollback path for failed upgrade documented and tested.

## M2 - Operator Experience and Safe Configuration (Weeks 5-8)

Owner lanes: A, B

Deliverables:

- schema-driven config editor for core broker sections,
- guarded advanced control workflow for non-form commands,
- config change audit trail with actor, timestamp, component, and diff metadata,
- backup/rollback/restart flow fully documented and rehearsed.

M2 artifacts (starting Week 5):

- M2 hardening kickoff (schema validation, guard workflow, config audit trail): [docs/plans/productization/week-05-m2-hardening-kickoff.md](./productization/week-05-m2-hardening-kickoff.md)
- M2 supportability and rollback reliability tranche: [docs/plans/productization/week-06-m2-supportability-and-rollback-reliability.md](./productization/week-06-m2-supportability-and-rollback-reliability.md)
- M2 CI reliability + redaction controls tranche: [docs/plans/productization/week-07-m2-ci-reliability-and-redaction-controls.md](./productization/week-07-m2-ci-reliability-and-redaction-controls.md)
- M2 nested config table-array editors tranche: [docs/plans/productization/week-08-m2-nested-config-table-arrays.md](./productization/week-08-m2-nested-config-table-arrays.md)

Acceptance criteria:

- 90% of day-1/day-2 operations possible without direct CLI editing,
- invalid config cannot be applied silently,
- rollback success rate >= 99% in automated config-console tests.

## M3 - Reliability and Supportability (Weeks 9-12)

Owner lanes: E, B, C

Deliverables:

- backup/restore utility and disaster-recovery runbook,
- migration/versioning framework for config and persisted state,
- support bundle export (logs, metrics snapshot, config metadata, audit head/tail),
- rehearsal suite for degraded mode, storage pressure, and auth/policy denial paths.

Early artifact draft:

- Support bundle schema + initial export command: [docs/plans/productization/week-02-support-bundle-schema.md](./productization/week-02-support-bundle-schema.md)
- M3 UI integration coverage kickoff (nested config table-array + raw/form transitions): [docs/plans/productization/week-09-m3-ui-config-integration-coverage.md](./productization/week-09-m3-ui-config-integration-coverage.md)
- M3 redaction profile hardening (`standard`/`strict`): [docs/plans/productization/week-09-m3-redaction-profiles.md](./productization/week-09-m3-redaction-profiles.md)
- M3 CI reliability evidence retention/trend policy: [docs/plans/productization/week-09-m3-ci-reliability-retention-trend-policy.md](./productization/week-09-m3-ci-reliability-retention-trend-policy.md)
- M3 backup/restore utility + DR runbook: [docs/plans/productization/week-10-m3-backup-restore-dr-runbook.md](./productization/week-10-m3-backup-restore-dr-runbook.md)
- M3 migration/versioning framework for config + persisted state: [docs/plans/productization/week-10-m3-migration-versioning-framework.md](./productization/week-10-m3-migration-versioning-framework.md)
- M3 rehearsal suite coverage for degraded/storage-pressure/auth-policy denial paths: [docs/plans/productization/week-10-m3-rehearsal-suite-coverage.md](./productization/week-10-m3-rehearsal-suite-coverage.md)
- M3 live rehearsal closure + support-bundle top-10 incident coverage: [docs/plans/productization/week-11-m3-live-rehearsals-and-support-bundle-coverage.md](./productization/week-11-m3-live-rehearsals-and-support-bundle-coverage.md)
- M3 DR restore to healthy-state clean environment validation: [docs/plans/productization/week-12-m3-dr-restore-clean-environment.md](./productization/week-12-m3-dr-restore-clean-environment.md)

Acceptance criteria:

- three full live rehearsals pass with documented outcomes,
- restore from backup to healthy state verified on a clean environment,
- support bundle sufficient to diagnose top 10 expected incidents.

## M4 - Security, Compliance, and GA Candidate (Weeks 13-16)

Owner lanes: D, C, A

Deliverables:

- key rotation workflow and operator guide,
- release signing, checksum publication, and SBOM generation,
- dependency and license audit gates in CI,
- final onboarding docs, troubleshooting matrix, and SLA/SLO draft.

M4 artifacts (starting Week 13):

- M4 key rotation workflow + operator guide: [docs/plans/productization/week-13-m4-key-rotation-workflow-and-operator-guide.md](./productization/week-13-m4-key-rotation-workflow-and-operator-guide.md)
- M4 release signing + checksum publication + SBOM generation: [docs/plans/productization/week-14-m4-release-signing-checksum-sbom.md](./productization/week-14-m4-release-signing-checksum-sbom.md)

Acceptance criteria:

- release artifacts are signed and reproducible from CI,
- no high-severity unresolved findings in security review,
- pilot operators can complete onboarding without engineering assistance.

## Cross-Milestone Workstreams

These run through all milestones:

- documentation freshness checks tied to behavior changes,
- automated test coverage expansion for auth/policy/storage/audit invariants,
- UX instrumentation for setup friction and operational failure points,
- weekly risk review with explicit de-scope decisions when needed.

## Pilot and GA Gates

Pilot entry gate:

- M0 + M1 acceptance criteria complete,
- installer and bootstrap flow validated by at least 2 internal operators,
- critical-path runbook reviewed and approved.

GA candidate gate:

- M2 + M3 + M4 acceptance criteria complete,
- no open P0/P1 defects,
- release checklist and rollback checklist validated in rehearsal.

## Risk Register (Initial)

Risk 1: Scope creep from enterprise asks before desktop path stabilizes  
Mitigation: strict non-goal governance and milestone gates.

Risk 2: Security posture gaps caused by fast UX iteration  
Mitigation: D-lane sign-off required for auth/token/secret handling changes.

Risk 3: Installer/platform matrix complexity delays timeline  
Mitigation: prioritize macOS + Linux first; stage Windows as controlled follow-on.

Risk 4: Operator confusion between capability scopes and policy rules  
Mitigation: guided token generation + principal registry validation in bootstrap flow.

Risk 5: Support burden from low-observability incidents  
Mitigation: support bundle and rehearsal requirements before GA gate.

## Immediate Backlog (Next 10 Working Days)

1. [x] Add bootstrap guardrails so generated tokens map to registered principals by default.
2. [x] Add token-principal-policy diagnostics panel in console.
3. [x] Define installer artifact matrix and CI release workflow skeleton.
4. [x] Draft support bundle schema and initial export command.
5. [x] Convert config-console core sections from raw TOML-only to mixed form + raw mode.
6. [x] Create pilot acceptance checklist file and assign initial owners.

## Week 3 Backlog (Completed)

1. [x] Surface service lifecycle controls (`start|stop|restart|status`) in the console for supported local services.
2. [x] Add guided operator actions in console (`bootstrap_local`, `generate_admin_token`, `verify_first_run`, `export_support_bundle`).
3. [x] Automate clean-machine timing rehearsal with evidence output for install-duration tracking.
4. [x] Automate rollback rehearsal with expected-failure simulation and pass/fail report output.

## Week 4 Backlog (Completed)

1. [x] Harden release skeleton with bundle-internal release notes/checksums and optional console desktop bundle job.
2. [x] Add rollback artifact compatibility check gate in release workflow.
3. [x] Add release manifest schema-constraint validation (`channel`, `version`, `checksums`, `notes`).
4. [x] Add pilot rehearsal duration rollup utility (`make summarize-pilot-runs`).
5. [x] Harden service/rehearsal reliability (stale PID reconciliation and startup retry gate in `verify-first-run`).

## Week 5 Backlog (Completed)

1. [x] Add schema-driven validation hints for form-editable core broker config fields and enforce them server-side on apply.
2. [x] Add guarded advanced-control workflow for mutating commands with acknowledgment + reason requirements.
3. [x] Add append-only config audit trail capture (actor/timestamp/context/diff) for config/service/operator/advanced actions.
4. [x] Add config audit trail panel in the console with reload and recent-entry inspection.
5. [x] Add targeted backend tests for guard classification, validation constraints, storage consistency checks, and audit append/list behavior.

## Week 6 Backlog (Completed)

1. [x] Add automated rollback reliability test for config-console apply/rollback flows and enforce `>= 99%` pass criterion.
2. [x] Extend support bundle export with config-audit head/tail slices (`--config-audit-log` and `config_audit` summary payload).
3. [x] Add support-bundle audit summary tests for head/tail extraction and missing-file warning behavior.
4. [x] Update support-bundle schema/README/Makefile surfaces to document config-audit evidence capture.

## Week 7 Backlog (Completed)

1. [x] Add config rollback reliability rehearsal script with machine-readable evidence output (`.md` + `.json`) for pilot tracking.
2. [x] Add CI job to run rollback reliability rehearsal and publish evidence artifacts for trend visibility.
3. [x] Add support-bundle redaction controls (`--redact-sensitive`, `--redact-placeholder`) and bundle-level redaction metadata.
4. [x] Add redaction behavior coverage tests and emit parseable rollback reliability metrics from the automated rollback test.

## Week 8 Backlog (Completed)

1. [x] Extend schema-driven config editor data model and section snapshots to include nested table-array fields for `auth.issuers`, `auth.principals`, `policy.rules`, and `quotas.profiles`.
2. [x] Extend section apply parsing and server-side validation so table-array edits enforce required fields, bounds, and allowlists before write.
3. [x] Add form-mode table-array editing UI (add/remove/update entries) with inline validation hints and per-entry error surfacing.
4. [x] Add targeted backend regression tests for table-array extraction, successful apply, and invalid nested allowlist rejection.

## Week 9 Backlog (Completed)

1. [x] Add UI-level integration coverage for nested table-array edits across form/raw mode transitions.
2. [x] Add policy-driven support-bundle redaction profiles (`standard`/`strict`) and operator docs.
3. [x] Add retention/trend policy for rollback-reliability CI artifacts.

## Week 10 Backlog (Completed)

1. [x] Add backup/restore utility and disaster-recovery runbook with operator validation path.
2. [x] Add migration/versioning framework for config and persisted state.
3. [x] Add rehearsal suite coverage for degraded mode, storage pressure, and auth/policy denial paths.

## Week 11 Backlog (Completed)

1. [x] Add support-bundle validation command for top-10 expected incident diagnostic coverage.
2. [x] Add live-suite rehearsal automation to run clean-machine, rollback, and denial rehearsals with consolidated evidence output.
3. [x] Add CI job to execute live-suite rehearsal and publish support-bundle coverage artifacts.

## Week 12 Backlog (Completed)

1. [x] Add clean-environment DR restore rehearsal that validates post-restore broker health and metrics.
2. [x] Add CI job to execute DR restore rehearsal and publish evidence artifacts.
3. [x] Capture successful DR restore rehearsal evidence and link closure artifact.

## Week 13 Backlog (Completed)

1. [x] Add isolated key-rotation rehearsal automation that validates overlap, cutover, old-key denial, and revocation-state evidence.
2. [x] Add operator workflow surface for key rotation (`make rehearse-key-rotation`) and document practical rotation steps.
3. [x] Add CI key-rotation rehearsal job with summary publication and evidence artifact upload.

## Week 14 Backlog (Completed)

1. [x] Add release-workflow support for detached artifact signing (secret-backed), aggregate checksum publication, and SBOM generation.
2. [x] Extend release-manifest schema and validator to include optional `sbom` and `signatures` metadata with checksum consistency checks.
3. [x] Update release operator docs and capture Week 14 closure artifact.
