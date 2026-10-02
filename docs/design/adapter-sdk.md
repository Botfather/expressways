# Adapter SDK

`expressways-adapter-sdk` contains the reliability primitives shared by supported chat, harness, and protocol adapters. It complements `expressways-client`: the client speaks to the broker, while the adapter SDK handles delivery state, replay identity, and binary integrity at an edge.

## Supported primitives

- `CursorStore` keeps independent next-offset cursors in a bounded, versioned, private JSON file. Checkpoints are monotonic, atomically replaced, and should be advanced only after an external destination acknowledges delivery.
- `stable_id` derives deterministic identifiers from an upstream idempotency key without retaining the original key.
- `verify_artifact_claims` checks the 64 MiB size ceiling, declared length, and SHA-256.
- `put_artifact` sends raw binary bytes to the broker without base64 expansion and verifies the returned metadata.
- `get_artifact` downloads broker-managed bytes and verifies their declared length and SHA-256 before returning them.

The supported chat bridge uses `CursorStore` for reply delivery and `stable_id` for replay-safe task identity. Existing bridge state using `replies_offset` is migrated automatically.

## Delivery pattern

An adapter delivering topic messages to an external channel should:

1. load the next offset from `CursorStore`;
2. consume from that offset;
3. validate the versioned message contract;
4. send the message with its stable delivery identifier as the destination idempotency key;
5. wait for a successful destination acknowledgement;
6. checkpoint `stored.offset + 1`;
7. leave the cursor unchanged on any failure.

This is at-least-once delivery. The receiving channel must deduplicate the delivery identifier. Cursor state prevents acknowledged messages from being replayed during normal restart recovery, but it cannot make an arbitrary external endpoint transactional with the broker.

## Artifact pattern

Topics should carry metadata and artifact references, not large base64 payloads. Use `put_artifact` for inbound binary data and put the returned artifact identifier, byte length, content type, and SHA-256 into the coordination message. Use `get_artifact` on egress; it fails closed if the broker response is missing, truncated, or corrupt.

The current artifact ceiling is 64 MiB. Larger media requires a future resumable/chunked artifact protocol or a separately governed external object store.

## Support boundary

The SDK provides reusable correctness primitives and focused contract tests. A new adapter is not supported merely because it imports the crate. It must still pass live authentication, replay/idempotency, conversation ordering, raw artifact, backpressure, destination failure, and component-restart conformance tests.
