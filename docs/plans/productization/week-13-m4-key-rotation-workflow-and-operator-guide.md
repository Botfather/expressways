# Week 13 (M4): Key Rotation Workflow and Operator Guide

Date: March 27, 2026  
Owner lanes: D, C, A

## Objective

Deliver a repeatable issuer key-rotation workflow that operators can rehearse and run with explicit overlap/cutover/revocation checkpoints.

## Delivered Changes

1. Added isolated key-rotation rehearsal automation:
   - `scripts/rehearsal-key-rotation.sh`
   - builds a clean workspace under `var/agent/pilot-runs/key-rotation-workdir`
   - validates baseline auth with existing issuer key
   - executes overlap phase (`old: active`, `new: rotating`, principals allow both keys)
   - executes cutover phase (`old: disabled`, `new: active`, principals allow new key only)
   - verifies old-key denial after cutover and captures key revocation evidence
   - emits markdown/json rehearsal reports for auditability

2. Added operator entrypoint:
   - `make rehearse-key-rotation`

3. Added CI rehearsal job and artifact publication:
   - `.github/workflows/ci.yml`
   - new job: `key-rotation-rehearsal`
   - publishes job summary and uploads rehearsal report/log artifacts

4. Added README operator guide section:
   - practical sequence for generating new keys, running overlap, cutting over, and revoking the retired key.

## Operator Workflow (Guide)

1. Generate a new issuer keypair and publish its public key path.
2. Update broker config to include both issuers:
   - current key `status = "active"`
   - new key `status = "rotating"`
   - principal `allowed_key_ids` includes both keys.
3. Restart broker and issue new tokens from the rotating key.
4. Confirm old and new tokens both succeed during overlap.
5. Cut over:
   - set old key `status = "disabled"`
   - set new key `status = "active"`
   - remove old key from principal `allowed_key_ids`.
6. Restart broker, confirm new tokens succeed, and confirm old-key tokens fail.
7. Revoke the retired key with `revoke-key`, then confirm auth-state records it under `revoked_key_ids`.

## Validation Evidence

Executed locally:

```bash
bash scripts/rehearsal-key-rotation.sh
```

Successful outputs:

- `var/agent/pilot-runs/key-rotation-rehearsal-20260327T072324Z.md`
- `var/agent/pilot-runs/key-rotation-rehearsal-20260327T072324Z.json`

## M4 Progress

This week closes the first M4 deliverable:

- key rotation workflow and operator guide.
