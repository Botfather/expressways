# Operator Runbook

Status: alpha, single-node operation

## Routine Checks

Use a capability token authorized for the requested administrative operation:

```bash
cargo run -p expressways-client --bin expresswaysctl -- \
  --transport tcp --address 127.0.0.1:7766 \
  health --token-file ./var/auth/admin.token

cargo run -p expressways-client --bin expresswaysctl -- \
  --transport tcp --address 127.0.0.1:7766 \
  metrics --token-file ./var/auth/admin.token

cargo run -p expressways-client --bin expresswaysctl -- \
  --transport tcp --address 127.0.0.1:7766 \
  adopters --token-file ./var/auth/admin.token
```

Treat degraded health as actionable. It means the process is serving only the operations it can perform safely; it does not mean all data paths are healthy.

## Logs and Audit

Broker logs are structured JSON. The audit log is hash-chained and should be exported and verified before incident sharing or archival. Do not publish raw logs or support bundles without reviewing them for tokens, provider keys, personal data, payload content, and absolute host paths.

```bash
make export-support-bundle
make validate-support-bundle
```

## Backup and Restore

Create a signed runtime backup:

```bash
make backup-runtime
```

Restore only during a controlled maintenance window, after preserving the current state and stopping writers:

```bash
make restore-runtime RESTORE_BACKUP_DIR=./var/agent/backups/<backup-directory>
```

The restore command verifies the backup signature and requires explicit overwrite behavior. After restoration, restart the broker and run `make verify-first-run`. Periodically exercise the full isolated recovery path with `make rehearse-dr-restore-clean-env`.

## Upgrade and Rollback

1. Read the changelog and release notes.
2. Verify checksums, signatures, and the SBOM for downloaded artifacts.
3. Back up configuration and state.
4. Stop the broker and dependent writers.
5. Install the new binaries without deleting the previous bundle.
6. Start the broker and verify health, metrics, audit, publish, and consume paths.
7. If validation fails, stop the new binary, restore the compatible prior bundle and backup, then verify again.

Persisted schema versions fail closed when newer than the running binary. Do not manually edit schema-version fields to bypass compatibility checks.

## Key Rotation

Use an overlap period: add the new issuer public key, issue and validate replacement tokens, move principals to the new key allowlist, then revoke the old key and remove old private material from active hosts. Run `make rehearse-key-rotation` before a production-like rotation. Never commit issuer private keys or issued tokens.

## Common Failures

| Symptom | Checks | Safe response |
| --- | --- | --- |
| `authentication_failed` | Token expiry, audience, issuer status, principal status, key allowlist, revocations | Issue a correctly scoped replacement token; do not weaken validation |
| `policy_denied` | Principal, resource, action, default-deny rules | Add the narrowest reviewed allow rule if access is intended |
| `quota_exceeded` | Principal quota profile and request size/rate | Reduce load or deliberately adjust a bounded quota |
| `service_degraded` | Health, metrics, audit/storage paths, adopter status | Repair the named subsystem; do not assume writes succeeded |
| `watch_cursor_expired` | Registry event-history limit and stored cursor | Refresh the full registry snapshot and resume from its cursor |
| Storage pressure | Retention limits, disk free space, reclaim metrics | Preserve regulated data, free capacity, or adjust reviewed bounds |
| Audit verification failure | File identity, permissions, truncation, hash chain | Stop sensitive mutations, preserve evidence, investigate before repair |

## Incident Handling

Preserve logs, audit data, configuration, relevant state files, binary versions, and timestamps. Rotate credentials when exposure is plausible. Use private vulnerability reporting for product security defects. The project has no remote telemetry or automatic upload mechanism.
