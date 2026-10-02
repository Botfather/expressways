# Week 9 M3 UI Config Integration Coverage

Date: March 26, 2026  
Status: Delivered (Item 1)

## Objective

Start M3 reliability work by adding UI-level integration coverage for config-console nested table-array editing and raw/form mode transition safety behavior.

## Delivered Changes

1. Added frontend test harness for the Tauri console web UI:
   - `apps/expressways-console/package.json`
     - new scripts: `test`, `test:watch`
   - `apps/expressways-console/vite.config.ts`
     - vitest configuration (`jsdom`, setup file, mock reset behavior)
   - `apps/expressways-console/src/test/setup.ts`
     - jest-dom matcher setup
2. Added integration tests that exercise config-console UI flows through `App`:
   - `apps/expressways-console/src/__tests__/App.config.integration.test.tsx`
   - coverage includes:
     - nested `policy.rules` table-array form editing (`add/remove/update` path)
     - normalization/assertion of string-array actions on save payload
     - raw/form mode transition guard that blocks form apply when raw TOML is dirty
3. Preserved existing backend validation authority and regression coverage:
   - no behavior relaxations in server-side validation path
   - frontend tests validate UI wiring to backend section update payload contracts

## Validation

1. `pnpm --dir apps/expressways-console test`
2. `pnpm --dir apps/expressways-console lint`
3. `pnpm --dir apps/expressways-console build`
4. `cargo test -p expressways-console`

## Follow-Up (Week 9+)

1. [x] Add support-bundle redaction profiles (`standard` and `strict`) with explicit policy docs. (Delivered in Week 9 item 2: `week-09-m3-redaction-profiles.md`)
2. [x] Add retention policy and trend-summary automation for rollback-reliability CI evidence artifacts. (Delivered in Week 9 item 3: `week-09-m3-ci-reliability-retention-trend-policy.md`)
