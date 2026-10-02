# Week 10 M3 Backup/Restore Utility and Disaster Recovery Runbook

Date: March 26, 2026  
Status: Delivered (Item 1)

## Objective

Close Week 10 backlog item 1 by adding a first-class runtime backup/restore utility and documenting a repeatable disaster-recovery procedure operators can run end-to-end.

## Delivered Changes

1. Added new `expresswaysctl` runtime backup command:
   - `backup-runtime`
   - captures a manifest-driven backup bundle under `./var/agent/backups/`
   - includes config and config-derived runtime paths (server data dir, auth revocations, issuer public keys, audit log, registry state when present) plus config-audit and orchestrator state paths
2. Added new `expresswaysctl` runtime restore command:
   - `restore-runtime`
   - restores from a backup bundle manifest to original destination paths
   - supports overwrite control and dry-run mode
   - fails fast when required targets were missing at backup time
3. Added Makefile entry points:
   - `make backup-runtime`
   - `make restore-runtime RESTORE_BACKUP_DIR=...`
4. Added client regression coverage for backup/restore behavior:
   - round-trip restore of runtime files and directories
   - guard rails for incomplete required backups
   - backup id validation constraints

## Disaster Recovery Runbook

1. Stop local broker/service processes before taking or restoring snapshots.
2. Capture backup bundle:
   - `cargo run -p expressways-client --bin expresswaysctl -- backup-runtime --config configs/expressways.example.toml --output-dir ./var/agent/backups --signing-private-key ./var/auth/issuer.private --signing-key-id dev`
3. Record the emitted `backup_dir` and `manifest` paths in incident notes.
4. If recovery is needed, restore from the chosen backup bundle:
   - `cargo run -p expressways-client --bin expresswaysctl -- restore-runtime --backup-dir <backup_dir> --verification-public-key ./var/auth/issuer.public --overwrite`
5. Start broker and run first-run verification:
   - `make run-expressways`
   - `make verify-first-run`
6. Export a support bundle after recovery for auditability:
   - `make export-support-bundle`

## Validation

1. `cargo test -p expressways-client`
2. `cargo fmt`
3. `make backup-runtime`
4. `make restore-runtime RESTORE_BACKUP_DIR=<backup_dir>`

## Follow-Up (Week 10+)

1. Add migration/version compatibility metadata for persisted state schemas so restore can block incompatible versions with explicit remediation guidance.
