# Security, Compliance, and Audit Baseline

## Principle

Security, compliance, and auditability are part of the runtime contract. They are not optional features and they are not a future hardening pass.

## Mandatory Controls

### Identity

- Every request carries a signed capability token.
- Anonymous write access is forbidden.
- Service principals and local developer principals are distinct.
- The runtime principal is derived from verified token claims, not caller-supplied strings.
- Principals must be registered locally before their tokens are accepted.
- Trusted issuers are explicitly configured and can be marked active, rotating, or disabled.
- Revocation state for tokens, principals, and issuer keys is part of the runtime decision path.
- Discovery-registry ownership is derived from the verified principal, never from caller-supplied owner fields.
- Discovery-registry freshness is explicit: cards must expire or be heartbeated, not linger indefinitely.

### Authorization

- Capability signature, expiry, and scope are verified before policy checks.
- Capability audience must match the broker audience.
- Every request is evaluated against policy.
- Default policy is deny.
- Policies apply to publish, consume, topic administration, discovery-registry administration, and audit access.

### Quotas and Backpressure

- Publish and consume operations must evaluate a quota profile.
- Payload-size limits and consume batch limits must be enforced before storage work begins.
- Rate-sensitive paths must choose an explicit overload behavior: reject or delay.
- Quota denials are audited the same way policy denials are audited.

### Audit

- Every allow and deny decision emits an audit event.
- Audit events are append-only.
- Audit events are hash-chained for tamper evidence.
- Audit events include principal, action, resource, decision, and outcome.
- Audit detail should include the capability token id when available.
- Audit trails must be locally verifiable and exportable without requiring the broker to be online.
- Discovery-card registration, lookup, and removal are audited the same way as other admin actions.
- Discovery-card heartbeat and stale cleanup are also audited admin operations.
- Discovery-registry watch requests are audited admin operations even when they time out without matching changes.
- Discovery-registry stream-open requests are audited admin operations before a multi-frame stream is established.

### Operational Logging

- Logs are structured JSON.
- Logs never replace audit events.
- Sensitive payloads are not logged directly.

### Operations and Recovery

- The broker must expose operator-usable metrics for request outcomes, storage state, and audit volume.
- Storage must enforce a global disk-pressure ceiling before local workstation disks are exhausted.
- Storage should recover from truncated trailing frames or stale indexes when safe to do so.
- Storage retention policy must be explicit for each retention class.
- Discovery-registry persistence must fail closed if the backing store cannot be read or written.
- Discovery-registry operators must be able to distinguish active cards from stale cards and explicitly sweep expired entries.
- Discovery watch history must be bounded so subscriptions cannot grow server memory without limit.
- Discovery watch streams must use bounded batching and keepalive frames rather than unbounded server-side buffering.

### Compliance Metadata

- Topics define a retention class.
- Messages carry or inherit a classification.
- Discovery cards carry both a retention class and a classification.
- The system records who set or changed compliance metadata.

## Retention Classes

- `ephemeral`: short-lived coordination messages.
- `operational`: standard broker activity needed for debugging.
- `regulated`: records requiring longer retention and tighter access.

## Classification Labels

- `public`
- `internal`
- `confidential`
- `restricted`

## Release Gate

No new endpoint, message path, or admin operation ships unless:

1. the action is covered by policy,
2. the action emits audit events,
3. the logs are structured,
4. the data model carries compliance metadata,
5. tests cover both allow and deny paths,
6. any publish or consume path has explicit quota behavior,
7. operator-facing metrics or verification tooling exist for the new path.
