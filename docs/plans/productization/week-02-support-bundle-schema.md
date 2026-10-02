# Week 2 Support Bundle Schema and Initial Export Command

Date: March 26, 2026  
Status: Delivered (updated with Week 9 policy-driven redaction profiles)

## Command

`expresswaysctl export-support-bundle`

Example:

```bash
cargo run -p expressways-client --bin expresswaysctl -- \
  --transport tcp \
  --address 127.0.0.1:7766 \
  export-support-bundle \
  --token-file ./var/auth/admin.token \
  --config configs/expressways.example.toml \
  --audit-log ./var/audit/audit.jsonl \
  --config-audit-log ./var/agent/config-audit/entries.jsonl \
  --redact-sensitive true \
  --redaction-profile standard \
  --redact-placeholder "[REDACTED]" \
  --logs-dir ./var/agent/service-control/logs \
  --output ./var/agent/support-bundle.json
```

## Schema (v1)

Top-level fields:

1. `schema_version`
2. `generated_at`
3. `broker_transport`
4. `broker_address`
5. `config`
6. `audit`
7. `config_audit`
8. `logs`
9. `broker_snapshot` (optional when token or broker connectivity is unavailable)
10. `redaction`
11. `warnings`

### `config`

1. `path`
2. `exists`
3. `size_bytes`
4. `modified_at`
5. `section_keys`
6. `parse_error`

### `audit`

1. `path`
2. `exists`
3. `size_bytes`
4. `modified_at`
5. `line_count`
6. `head` (first N lines)
7. `tail` (last N lines)

### `config_audit`

1. `path`
2. `exists`
3. `size_bytes`
4. `modified_at`
5. `line_count`
6. `head` (first N lines)
7. `tail` (last N lines)

### `logs[]`

1. `path`
2. `size_bytes`
3. `modified_at`
4. `tail` (last N lines per file)

### `redaction`

1. `enabled`
2. `profile` (`standard` or `strict`)
3. `policy` (active redaction policy id)
4. `placeholder`
5. `redacted_lines`

### `broker_snapshot`

1. `health`
2. `metrics`
3. `auth`

## Notes

1. The command degrades with warnings when broker snapshot commands fail or token is omitted.
2. Output is JSON and intended as the initial support artifact for pilot incident triage.
3. The command redacts sensitive lines by default (`--redact-sensitive true`) with `--redaction-profile standard`.
4. Operators can switch to broader policy matching with `--redaction-profile strict` and tune replacement text with `--redact-placeholder`.
5. Future milestones will add signed bundle manifests.
