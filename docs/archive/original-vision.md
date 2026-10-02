# Original Expressways Vision (Archived)

Status: historical context only; not an implementation or roadmap

The original concept explored a broad distributed agent-bus architecture involving multi-node consensus, shared-memory IPC, `io_uring`, priority scheduling, cooperative consumer groups, schema-aware discovery, and in-broker transforms.

That proposal was intentionally narrowed after review. Those capabilities are not present unless current source code and current documentation explicitly say otherwise. In particular, Expressways Phase 1 is a single-node broker and does not implement Raft, clustering, shared-memory transport, `io_uring`, FlatBuffers, consumer groups, DIDs, mTLS, tiered storage, or WASM transforms.

The authoritative documents are:

- [Phase 1 system design](../design/phase-1-system-design.md)
- [Security, compliance, and audit baseline](../design/security-compliance-baseline.md)
- [Phase 1 scope ADR](../adr/0001-phase-1-scope.md)
- [Critical review](../reviews/expressways-critical-review.md)

Future work requires measured evidence, an explicit threat model, a compatibility plan, and a separately accepted ADR. This archive records direction considered—not work promised.
