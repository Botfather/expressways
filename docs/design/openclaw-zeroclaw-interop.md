# Supported Chat Interoperability

## Status

The `expressways-interop-bridge` binary is the supported, versioned integration path for channel owners such as Pigeon, OpenClaw, and ZeroClaw. It provides authenticated ingress, broker-managed media upload, ordered task handoff, and durable at-least-once reply delivery.

## Topics and Versions

- `interop.chat.requests`: `interop.chat.handoff.v1` task payloads.
- `interop.chat.results`: intermediate worker output.
- `interop.chat.replies`: `interop.chat.reply.v1` final replies.

The protocol crate exports the topic, task-type, and schema-version constants. V1 readers ignore unknown JSON fields. Breaking changes require a new schema version and a parallel migration period.

Schemas:

- [ingress webhook v1](schemas/interop-chat-ingress-v1.schema.json)
- [handoff v1](schemas/interop-chat-handoff-v1.schema.json)
- [reply v1](schemas/interop-chat-reply-v1.schema.json)

## Durable Deployment

Durable two-way mode is the normal operating mode:

```bash
cargo run -p expressways-interop-bridge -- \
  --token-file ./var/auth/developer.token \
  --ingress-bearer-file ./var/auth/interop-ingress.token \
  --egress-url https://pigeon.example/v1/expressways/replies \
  --egress-bearer-file ./var/auth/pigeon-egress.token \
  --state-path ./var/agent/interop-bridge-state.json
```

The bridge consumes replies from its persisted offset. It advances that offset only after the channel endpoint returns a 2xx response and the new cursor is atomically synced to disk. Failures are retried without advancing the cursor, so delivery is at least once. The receiving channel must deduplicate the `Idempotency-Key` header or the reply envelope's `delivery_id`.

HTTP egress must use HTTPS except for loopback development endpoints. Non-loopback ingress requires bearer authentication.

## Ingress

POST `interop.chat.handoff.v1` JSON to `/v1/webhook/handoff`. `schema_version` is required. Callers should supply:

- `idempotency_key`: stable for the source message and reused across retries;
- `correlation_id`: stable across the request and its eventual reply;
- `session.message_id`: the source channel's message identifier.

At least one stable source identity is required: `task_id`, `idempotency_key`, `correlation_id`, or `session.message_id`. The bridge deterministically derives omitted correlation and task identifiers from that identity. Repeated submissions therefore produce the same task ID, which the orchestrator deduplicates.

Unknown fields are accepted within v1 for forward-compatible additive evolution. Bounds, required fields, identifiers, hashes, and mutually exclusive attachment forms remain validated.

Messages may contain `text`, binary `attachments`, and/or structured `content`.
Structured entries use `{ "type", "data" }` and support `location`, `contact`,
`poll`, `edit`, `delete`, and `protocol`, so channel-native events do not need
to be flattened into fake text or discarded. Whitespace-only `text` is
normalized away and does not invalidate an otherwise populated structured
message.

## Media

Small payloads may use `inline_base64`, but production channels should upload media separately:

```bash
curl --fail-with-body \
  --request POST \
  --header "Authorization: Bearer $INGRESS_BEARER" \
  --header "Content-Type: application/pdf" \
  --header "X-Artifact-Id: pigeon-message-123-document" \
  --header "X-Content-Sha256: $SHA256" \
  --data-binary @document.pdf \
  http://127.0.0.1:8891/v1/artifacts
```

The artifact endpoint accepts raw binary bodies up to 64 MiB by default and sends them to the broker through its binary protocol. The handoff then references the returned `artifact_id`; media is not base64-expanded inside the task message.

Named artifact uploads are idempotent when the existing bytes have the same
length and SHA-256. The broker returns the original immutable metadata for an
identical retry and rejects an ID collision containing different bytes.

The bridge also serves authenticated artifact reads, so a channel adapter does
not need to deploy the separate HTTP gateway merely to resolve reply media:

```bash
curl --fail-with-body \
  --header "Authorization: Bearer $INGRESS_BEARER" \
  --output reply-media.bin \
  http://127.0.0.1:8891/v1/artifacts/ARTIFACT_ID
```

The response preserves the broker content type and includes `X-Artifact-Id`
and `X-Content-Sha256`. The bridge verifies the length and digest before
returning bytes.

## Ordering and Routing

The bridge derives an affinity key from `channel:account_id:session_id` unless `routing.affinity_key` is supplied. The orchestrator:

1. permits only one active task per affinity key,
2. keeps later messages blocked across retries and releases them only after the earlier task completes, is exhausted, or is canceled,
3. prefers the most recently used eligible agent for that affinity key.

Set `routing.agent_id` for a hard pin. A hard pin is enforced by the orchestrator; it is not merely a scheduling preference. `preferred_agents` remains a soft preference.

## Reply Contract

Workers publish `interop.chat.reply.v1` JSON to `interop.chat.replies`. Required identity fields are `delivery_id`, `correlation_id`, `source_runtime`, `target_runtime`, `session`, and `in_reply_to_task_id`. The message contains text, structured content, and/or broker artifact references.

The bridge POSTs the unchanged envelope to the configured egress URL with:

- `Idempotency-Key: <delivery_id>`
- `X-Expressways-Correlation-Id: <correlation_id>`
- optional bearer authorization

A 2xx response is the delivery acknowledgement. Any other response or transport failure retains the cursor for retry.

## Operational Guarantees

- Broker capability policy, quotas, classification, retention, and audit remain authoritative.
- Ingress retry is idempotent when the source reuses its idempotency key.
- Per-conversation ordering is enforced through affinity serialization.
- Egress is durable and at least once, not exactly once.
- A corrupt or unsupported reply blocks cursor advancement and is visible in structured logs; operators must correct or explicitly supersede it.

Run `make test-adapter-conformance` to verify these guarantees against an isolated live broker, bridge, orchestrator, and intentionally failing destination. The test covers authentication, replay identity, affinity ordering, idempotent raw 2 MiB media upload and bridge download, explicit backpressure, at-least-once retry, and restart recovery.
