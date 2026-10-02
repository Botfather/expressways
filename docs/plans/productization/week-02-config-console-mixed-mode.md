# Week 2 Config Console Mixed Mode (Form + Raw)

Date: March 26, 2026  
Status: Delivered

## Objective

Convert core broker configuration editing from raw TOML-only into mixed mode:

1. form mode for core sections,
2. raw TOML mode for full-file/advanced edits.

## Delivered Changes

1. Added section-level form fields for core broker sections:
   - `server`
   - `storage`
   - `audit`
   - `resilience`
   - `registry`
   - `auth`
   - `adopters`
   - `policy`
2. Added editor mode toggle per component (`Form` / `Raw TOML`).
3. Added backend section update command:
   - `config_console_update_section`
4. Preserved existing raw file apply flow with diff, backup, rollback, and restart orchestration.

## Operator Behavior

1. Form mode supports scalar and string-array fields from core sections.
2. Unsupported/nested table edits remain available in raw mode.
3. Form applies are blocked when raw draft has unsaved changes to avoid accidental overwrite confusion.

## Follow-Up Status

1. [x] Improve typed editors for nested auth/policy/quota table arrays. (Delivered in Week 8: `week-08-m2-nested-config-table-arrays.md`)
2. [x] Add inline field validation hints before submit. (Delivered in Week 5: `week-05-m2-hardening-kickoff.md`)
3. [ ] Add targeted UI tests for form mode apply and raw/form mode transitions. (Deferred to Week 9+ reliability tranche)
