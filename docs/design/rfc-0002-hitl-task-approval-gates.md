# RFC 0002: Human-in-the-Loop Task Approval Gates

## Status

Deferred

This RFC records a possible future protocol. It is not implemented behavior or
a Phase 1 requirement.

## Motivation

Stateful agent frameworks can pause before a consequential operation, such as
executing a shell command, changing credentials, transferring funds, deleting
data, deploying software, or sending an external message. Expressways can
currently carry durable approval request and response messages, but it does not
provide a broker-enforced, interoperable approval primitive.

Framework-owned approval flows are sufficient while integrations can retain
their own checkpoints and trust models. A native protocol becomes valuable
when multiple independent runtimes need the same authorization, expiry,
replay-protection, and audit guarantees.

## Decision Boundary

Do not add a native approval subsystem merely to reproduce a framework's pause
feature. Revisit this RFC when at least two independent runtimes require
interoperable approval for high-impact operations.

Until then:

- dangerous tools remain default-deny;
- frameworks retain and resume their own checkpoints;
- ordinary durable topics may carry integration-specific approval messages;
- those messages must not be described as cryptographic authorization proof.

## Proposed Ownership

The broker must own challenge issuance, authorization, expiry, single-use
consumption, atomic state transitions, and final audit recording. These are
security-boundary operations and must not be delegated to the orchestrator.

The orchestrator may expose the resulting lifecycle state, route notifications,
and resume eligible work. It must not mint or validate approval authority.
LangGraph and similar frameworks remain responsible for storing graph state;
Expressways stores only an opaque checkpoint reference and integrity digest.

## Proposed Lifecycle

```text
assigned
  -> pending_approval
  -> assigned             approved; resume the same assignment
  -> approval_rejected    terminal
  -> approval_timed_out   terminal
  -> canceled             explicit operator cancellation
```

The assignment lease is paused while approval is pending. Approval must not
increment the task attempt or silently create a different assignment.

## Proposed Authorization Contract

Approval authorization should be structured instead of encoded as an
ambiguous string:

```json
{
  "required_authorization": {
    "resource": "approval:task-8831",
    "action": "approve"
  }
}
```

`approve` would become a first-class protocol action. The broker must require
both a valid single-use challenge and a signed capability authorizing that
action on the specified resource. A reply received through WhatsApp, email, or
another egress channel is untrusted context, not authorization proof.

## Proposed Challenge Contract

A challenge must be bound to all security-relevant values:

- protocol version;
- approval request ID;
- task and assignment IDs;
- canonical action digest;
- payload or artifact digest;
- expiration timestamp;
- cryptographically random nonce.

Human-readable summaries are informational and must not be integrity inputs.
Prefer an opaque random 256-bit challenge and persist only a keyed hash. A
self-contained signed token is acceptable only after specifying signing-key
storage, rotation, revocation, and algorithm agility. Complete tokens must
never appear in logs, metrics, or audit records.

## Atomicity and Replay Protection

The response operation is an atomic compare-and-swap:

```text
pending + matching unused challenge + unexpired capability
  -> approved or rejected + challenge consumed
```

Only one response can win. An identical replay may return the already-recorded
result, while conflicting, consumed, mismatched, or late responses fail. Every
read and response path must enforce expiration; a background sweeper may make
timeouts timely but cannot be the sole expiry control.

## Audit Guarantees

The authenticated capability establishes the approver principal. The
Expressways audit hash chain makes the record tamper-evident; it is not itself
a digital signature by the approver.

Record bounded, non-secret evidence including:

- approver principal and capability token ID;
- decision and bounded reason;
- request and challenge identifiers;
- task, assignment, action, and payload digests;
- request, response, and expiry timestamps;
- accepted, rejected, replayed, expired, or mismatched outcome;
- egress channel metadata explicitly labeled as untrusted context.

## Delivery Model

The broker emits a versioned approval-request event for egress adapters.
Adapters may render the request in chat or email, but responses must invoke the
authenticated broker command rather than publish an arbitrary approval event.
No adapter receives authority merely because it delivered the challenge.

## Required Verification Before Acceptance

An implementation must include tests for:

- allow and deny capability paths;
- simultaneous conflicting responses with exactly one winner;
- duplicate identical responses;
- expired and already-consumed challenges;
- task, assignment, action, and payload substitution;
- orchestrator and broker restart recovery;
- lease pause and resume behavior;
- token and reason size bounds;
- absence of challenge secrets from structured logs and audit output;
- egress delivery failure without loss of the pending approval;
- audit-chain verification for every terminal outcome.

