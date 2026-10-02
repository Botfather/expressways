# OpenClaw and ZeroClaw Interop Contract

## Purpose

This document defines a concrete, operator-friendly contract for running OpenClaw and ZeroClaw on the same host while coordinating work through Expressways.

The intent is to keep channel/session ownership in those runtimes, while using Expressways as the governed execution and audit path.

## Topic Convention

Use these topic names for runtime interop:

- `interop.chat.requests`
  Ingress handoff requests from OpenClaw or ZeroClaw bridge services to Expressways workers.
- `interop.chat.results`
  Worker-produced intermediate outcomes, tool outputs, and normalization results.
- `interop.chat.replies`
  Final outbound payloads that bridge egress services deliver back to OpenClaw or ZeroClaw.

The protocol crate exposes constants for these names:

- `INTEROP_CHAT_REQUESTS_TOPIC`
- `INTEROP_CHAT_RESULTS_TOPIC`
- `INTEROP_CHAT_REPLIES_TOPIC`
- `INTEROP_CHAT_HANDOFF_TASK_TYPE`

Default compliance posture for these topics:

- retention class: `operational`
- classification: `internal`

## Canonical Task Type

Use task type `interop.chat.handoff` for inbound chat handoff work items.

Each submitted `TaskWorkItem` should carry a JSON payload with `schema_version` equal to `interop.chat.handoff.v1`.

## Canonical Payload Shape

Payload shape for `interop.chat.handoff.v1`:

- `schema_version` (string, required)
- `source_runtime` (string, required)
- `target_runtime` (string, optional)
- `session` (object, required)
- `message` (object, required)
- `routing` (object, optional)
- `metadata` (object, optional)
- `received_at` (RFC3339 timestamp string, required)

Session fields:

- `session_id` (string, required)
- `channel` (string, required)
- `account_id` (string, required)
- `sender_id` (string, required)
- `sender_display_name` (string, optional)
- `message_id` (string, optional)
- `reply_to_message_id` (string, optional)

Message fields:

- `text` (string, optional)
- `attachments` (array, optional)

Attachment fields:

- `name` (string, optional)
- `content_type` (string, optional)
- `artifact_id` (string, required)
- `sha256` (string, optional)
- `byte_length` (integer, optional)

Routing fields:

- `agent_id` (string, optional)
- `workspace` (string, optional)
- `skill_hint` (string, optional)
- `labels` (array of strings, optional)

## Attachment Rule

Attachments should be uploaded as broker-managed artifacts, then represented in payload as `artifact_id` references. This keeps large blobs out of message payload strings and preserves audit and quota behavior.

## Bridge Behavior

Bridge ingress services should:

1. authenticate the incoming webhook call,
2. optionally upload inline attachment bytes with `put_artifact`,
3. normalize into `interop.chat.handoff.v1`,
4. submit to `interop.chat.requests` as a `TaskWorkItem`,
5. rely on broker policy, quotas, audit, and orchestration for execution flow.

Bridge egress services should consume `interop.chat.replies` and deliver final responses back to their owning runtime (OpenClaw or ZeroClaw).

## JSON Schema

See:

- [docs/design/schemas/interop-chat-handoff-v1.schema.json](schemas/interop-chat-handoff-v1.schema.json)
