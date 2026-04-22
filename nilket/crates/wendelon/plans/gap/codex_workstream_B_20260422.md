# Workstream B: Storage And Stellarator Correctness

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS1

## Mandate

Make the write path crash-correct before optimizing it. The current system is close enough to be dangerous: it has real machinery, but one of the critical partial-failure branches still degrades to best effort.

## Architectural Position

The write path must be recoverable or it must fail. There is no acceptable middle ground where quorum-durable data exists without deterministic recovery semantics.

My position is:

- pending-intent persistence is mandatory for the recoverable path
- local and remote durability legs must execute concurrently
- write outcomes must be modeled explicitly, not inferred from logs
- a leader that cannot trust its local recovery substrate may need to fence itself

## Scope

- pending-finalization correctness
- concurrent local and remote durability legs
- explicit write-outcome state machine
- restart replay and idempotency hardening
- failure injection around partial-finalization windows

## Primary Code Areas

- `src/network/data_server.rs`
- `src/network/pending_finalizations.rs`
- `src/storage/manager.rs`
- relevant integration tests under `tests/`

## Non-Goals

- `io_uring` topology split
- QUIC overload protection
- general metrics surface expansion outside the write path

Those belong elsewhere.

## Required Design Decisions

1. What exact client-visible status is returned when pending-intent persistence fails?
2. Under what conditions does the leader degrade or fence itself?
3. Is the leader's local durable leg always mandatory for quorum success?
4. Which terminal states are retry-safe, and which require operator involvement?

## Opinionated State Model

The code should expose an explicit state model:

- `DataBranchPending`
- `DurableQuorumReached`
- `FinalizationCommitted`
- `FinalizationRecoverablePending`
- `FinalizationTerminalFailure`

If a path cannot be expressed cleanly in this state model, the implementation is still too implicit.

## Work Packages

### B1. State Machine First

Land the write-outcome enum, transition table, and transition invariants before the large refactor.

This is deliberate. The team should refactor against named states, not comments and intuition.

### B2. Mandatory Pending-Intent Persistence

Change the branch that currently warns and continues.

Required behavior:

- if pending-intent persistence fails, do not continue as if restart recovery exists
- either:
  - fail the write as retriable
  - or fence/degrade the node if local persistence cannot be trusted

### B3. True Concurrent Durability

Refactor the leader path so that:

- local durable append starts immediately
- remote durable fan-out starts immediately
- quorum aggregation waits across both sets of futures

The implementation must match the architecture comments. Today it does not.

### B4. Idempotency And Replay Hardening

Make in-process retry and post-restart replay converge on the same outcome model.

Required properties:

- duplicate client retries do not create double finalization
- restart replay deterministically completes or cleanly reports a terminal failure
- cached write outcomes cannot misrepresent a quorum-durable-but-unfinalized write

### B5. Failure Harness

Add targeted failure scenarios:

- crash after durable quorum, before finalization
- local pending-intent store failure
- remote replica stall/timeouts
- duplicate client retry during pending state
- restart replay with partially persisted local state

## Deliverables

- explicit write-outcome enum and transition table
- refactored concurrent write path
- deterministic replay path
- client error/status mapping document
- targeted crash/failure integration suite

## Exit Criteria

- no best-effort pending-intent path remains
- local and remote durability legs are concurrent in code and test evidence
- `1,000` injected failure iterations complete with zero stranded quorum-durable writes
- every terminal outcome has explicit client semantics and a test

## Recommended Sequencing

1. state model and invariants
2. mandatory intent persistence
3. concurrency refactor
4. idempotency/replay alignment
5. failure harness and soak

## Risks

- refactor accidentally changes client-visible write semantics
- fencing logic causes unnecessary leader churn
- replay logic diverges from live-path behavior

## Opinionated Constraints

- Do not optimize latency first and try to bolt correctness on later.
- Do not hide ambiguous outcomes behind generic `INTERNAL_ERROR`.
- Do not leave replay behavior split across ad hoc caches and local stores without one authoritative state model.

## Open Questions

- Is local durable append a hard requirement before any quorum success can be reported, or can the leader permit pure-remote quorum in narrowly defined cases?
- What exact node-fencing trigger is acceptable when the pending-intent store fails?
- Should `FinalizationRecoverablePending` be exposed to clients directly, or normalized into a retryable error code?
- Do we require a separate background finalization worker model, or is the existing wakeup/replay path sufficient once hardened?
