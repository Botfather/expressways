## Summary

Describe the problem and the resulting behavior.

## Security and compatibility

- [ ] Authentication, capability, policy, quota, audit, and degraded-mode effects were considered.
- [ ] Wire or persistence compatibility is unchanged, or the migration is documented.
- [ ] No secrets, tokens, private keys, local paths, or generated state are included.

## Validation

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-features`
- [ ] Documentation and examples were updated where behavior changed.

List any additional checks and relevant results.
