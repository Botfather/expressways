# Control Protocol Reference

Status: alpha; the Rust types in `crates/expressways-protocol` are authoritative

Expressways uses length-delimited JSON control frames over TCP or Unix sockets. The configured `max_frame_bytes` applies before request deserialization. Every request contains a capability token and one tagged command:

```json
{
  "capability_token": "<redacted>",
  "command": {
    "type": "health"
  }
}
```

Command and response tags use `snake_case`. Unknown or malformed commands fail closed. Attachments use the wire envelope defined by the protocol crate and are independently bounded by the broker.

## Commands

| Command | Purpose | Typical action/resource |
| --- | --- | --- |
| `health` | Broker health | `health` on `system:broker` |
| `get_auth_state`, `get_metrics`, `get_adopters` | Operator visibility | `admin` on `system:broker` |
| `create_topic` | Create a topic contract | `admin` on `topic:<name>` |
| `publish`, `consume` | Append or read topic messages | `publish`/`consume` on `topic:<name>` |
| `register_agent`, `heartbeat_agent`, `list_agents` | Discovery registry lifecycle | `admin` on `registry:agents*` |
| `watch_agents`, `open_agent_watch_stream` | Paginated or streaming registry changes | `admin` on `registry:agents*` |
| `cleanup_stale_agents`, `remove_agent` | Registry administration | `admin` on `registry:agents*` |
| `revoke_token`, `revoke_principal`, `revoke_key` | Authentication revocation | `admin` on `system:broker` |
| `put_artifact`, `stat_artifact`, `get_artifact` | Broker-managed artifact lifecycle | scoped action on `artifact:<id>` |

Authorization is the conjunction of successful token verification, an active known principal, capability scope, server-side policy, and applicable quota checks. Possessing a valid token does not bypass policy.

## Responses and Errors

Successful responses are tagged variants such as `health`, `metrics`, `topic_created`, `publish_accepted`, `messages`, `agents`, or `artifact`. Failures use:

```json
{
  "type": "error",
  "code": "policy_denied",
  "message": "<human-readable context>"
}
```

Clients must branch on `code`, not parse `message`. Error strings may gain diagnostic detail without a compatibility guarantee. Response frames remain bounded; a response that cannot fit is replaced with an explicit bounded error.

## Cursors

- A consume `next_offset` is immediately after the final returned message, or the requested offset for an empty result. It is not a topic high-water mark.
- A registry cursor advances through the last event examined and never skips matching events omitted by pagination.
- `watch_cursor_expired` requires a fresh registry snapshot before streaming resumes.

## Compatibility

Version `0.x` is alpha. Additive fields may appear with defaults; command or persistence changes that cannot be made compatible require documentation, tests, and an explicit migration. Persisted documents carry schema versions and reject unknown newer versions rather than guessing.

Task requirements may include `required_agent` for a hard scheduling constraint and `affinity_key` for serialized, sticky routing. Interoperability handoffs and replies use the version constants and envelope types exported by `expressways-protocol`; their JSON schemas are documented in [Supported Chat Interoperability](design/openclaw-zeroclaw-interop.md).
