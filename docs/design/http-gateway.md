# Authenticated Local HTTP Gateway

## Purpose

`expressways-http-gateway` gives local harnesses and applications a supported HTTP/JSON interface without moving authentication, authorization, quota, or audit decisions out of the broker. Every request must carry an Expressways capability as an HTTP bearer token. The gateway forwards that same capability with the broker operation.

The gateway binds to `127.0.0.1:8790` by default and refuses non-loopback listeners. It does not implement TLS or a separate user/session system. For remote access, place an explicitly reviewed authenticated TLS proxy in front rather than exposing the process directly.

## Run

Start the broker, then run:

```bash
make run-http-api
```

Equivalent command:

```bash
cargo run -p expressways-http-gateway -- \
  --listen 127.0.0.1:8790 \
  --broker-address 127.0.0.1:7766
```

The default JSON body limit is 1 MiB. Raw artifact uploads have a separate 64 MiB limit. Both can be reduced with command-line options and cannot be raised above 64 MiB. Broker operations time out after 30 seconds by default (five-minute ceiling), and at most 128 are allowed concurrently (4,096 ceiling), preventing stalled local clients or broker connections from creating unbounded work.

## Authentication

All routes, including health, require:

```text
Authorization: Bearer <expressways-capability-token>
```

The capability must grant the action and resource used by the route, and the broker's registered principal, issuer state, revocations, server-side policy, and quotas still apply. The gateway does not accept a shared administrator token at startup and does not substitute its own identity for the caller.

## Routes

| HTTP route | Broker command | Required scope |
| --- | --- | --- |
| `GET /v1/health` | `health` | `health` on `system:broker` |
| `POST /v1/topics/{topic}/messages` | `publish` | `publish` on `topic:{topic}` |
| `GET /v1/topics/{topic}/messages?offset=0&limit=100` | `consume` | `consume` on `topic:{topic}` |
| `GET /v1/topics/{topic}/events?offset=0` | resumable SSE consume loop | `consume` on `topic:{topic}` |
| `POST /v1/tasks` | `publish` to `tasks` | `publish` on `topic:tasks` |
| `GET /v1/agents` | `list_agents` | `admin` on `registry:agents` |
| `POST /v1/artifacts` | `put_artifact` | artifact publish scope |
| `GET /v1/artifacts/{artifact_id}` | `get_artifact` | consume on that artifact |

Agent query parameters are `skill`, `topic`, `principal`, and `include_stale`. Consume limits must be between 1 and 10,000; the broker may enforce a smaller principal-specific quota.

## Stream topic events

`GET /v1/topics/{topic}/events` returns `text/event-stream`. Each stored message is emitted as a `message` event whose JSON data is the complete `StoredMessage`; the SSE event ID is its topic offset. Clients can reconnect with `Last-Event-ID`, which resumes at the following offset, or pass an explicit `offset` query parameter. Explicit offsets take precedence.

Optional parameters are `limit` (default `100`) and `wait_timeout_ms` (default `25000`, bounded from `1000` through `25000`). The gateway authenticates and authorizes an initial broker consume before returning HTTP 200. It then uses the broker's bounded `watch_topic` operation, sends SSE keepalives every 15 seconds, caps concurrent streams at 64 by default, and emits one terminal `error` event if broker delivery fails. Broker policy and consume quotas apply to every bounded wait.

The broker performs storage probes inside one authenticated, quota-checked, audited long-poll operation. Empty probes do not create additional audit records or network requests.

## Publish

```http
POST /v1/topics/events/messages
Authorization: Bearer <token>
Content-Type: application/json

{
  "classification": "internal",
  "payload": {
    "kind": "device.event",
    "value": 42
  }
}
```

String payload values are stored directly. Other JSON values are serialized as compact JSON before publication. The response is the broker's tagged `publish_accepted` response.

## Submit a task

`POST /v1/tasks` accepts the `TaskWorkItem` JSON contract from `expressways-protocol`. An optional `X-Classification` header sets the message classification. Payloads that reference large data should use `artifact_ref`, not inline base64.

## Upload and download artifacts

Artifact uploads use a raw request body so binary objects do not incur base64 expansion:

```http
POST /v1/artifacts
Authorization: Bearer <token>
Content-Type: image/png
X-Artifact-Id: optional-stable-id
X-Content-Sha256: optional-lowercase-sha256
X-Classification: confidential
X-Retention-Class: operational

<raw bytes>
```

The gateway computes SHA-256 for every upload and rejects a mismatching `X-Content-Sha256`. Downloads return the raw bytes plus `X-Artifact-Id`, `X-Content-Sha256`, `X-Classification`, and `X-Retention-Class` response headers. The broker remains responsible for durable storage, authorization, integrity verification, and audit.

## Error behavior

Gateway errors use:

```json
{
  "error": {
    "code": "policy_denied",
    "message": "..."
  }
}
```

Authentication failures map to `401`, policy/capability denials to `403`, missing resources to `404`, quota denials to `429`, degraded service to `503`, and broker connectivity/protocol failures to `502`. Responses include `Cache-Control: no-store` and defensive content headers. CORS is intentionally not enabled.
