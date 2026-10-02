# Week 1 Pilot Acceptance Checklist

Date: March 26, 2026  
Status: Active checklist with assigned owners

## Checklist

| Status | Item | Owner | Evidence |
| --- | --- | --- | --- |
| [x] | Product contract approved (target user, top workflows, non-goals). | Product Lead (Lane A) | `docs/plans/productization/week-01-m0-product-contract.md` |
| [x] | Pilot metrics defined and ownership assigned. | Product Lead + Tech Lead (Lanes A/B) | `docs/plans/productization/week-01-m0-product-contract.md` |
| [x] | Launch-blocking risk register with owners documented. | Tech Lead + Security Reviewer (Lane D) | `docs/plans/productization/week-01-m0-product-contract.md` |
| [x] | Support policy draft published (covered vs out-of-scope). | Product Lead (Lane A) | `docs/plans/productization/week-01-support-policy-draft.md` |
| [x] | Release channel model (alpha/beta/stable) defined. | Platform Engineer (Lane C) | `docs/plans/productization/week-01-release-channel-model.md` |
| [ ] | Two internal operators complete install + bootstrap rehearsal under 15 minutes. | Platform Engineer + Core Engineer (Lanes C/B) | `var/agent/pilot-runs/*.md` |
| [x] | Default bootstrap path demonstrates no token-principal mismatch. | Tech Lead (Lane B) | `make generate-admin-token`, diagnostics panel, and `docs/plans/productization/week-02-bootstrap-runbook.md` |
| [x] | Rollback workflow rehearsal passes on failed upgrade simulation. | Platform Engineer (Lane C) | `make rehearse-rollback`, `var/agent/pilot-runs/rollback-rehearsal-*.md` |
| [ ] | Pilot gate sign-off recorded. | Product Lead + Tech Lead (Lanes A/B) | Signed milestone note |

## Review Cadence

- Weekly gate review: Thursday sprint close.
- Blocking item policy: any unchecked blocking item prevents pilot gate promotion.
