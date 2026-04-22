# Workstream B: Storage And Stellarator Correctness

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS1`
Derived Backlog: `plans/impl/workstream_B_backlog.md`

## Objective

Make the bifurcated write path crash-correct, replay-correct, and explicit enough that the rest of the system can safely optimize around it.

## Why Now

This is the load-bearing workstream.

If Wendelon cannot explain exactly what happens when:

- local durable append succeeds
- some remote replicas succeed
- finalization has not yet committed
- the leader crashes

then nothing above it is trustworthy.

## ADR Anchors

- ADR `003`
- ADR `015`
- ADR `016`
- ADR `021`

## Scope

- explicit write-outcome state model
- mandatory pending-finalization persistence for recoverable branches
- concurrent leader-local and remote durability legs
- durable-quorum aggregation semantics
- restart replay and idempotency behavior
- leader degrade/fence behavior on local persistence distrust

Primary code areas:

- `src/network/data_server.rs`
- `src/network/pending_finalizations.rs`
- `src/storage/manager.rs`
- write-path integration tests under `tests/`

## Out Of Scope

- WAL/slab executor split
- QUIC admission control
- broad observability rollout outside write-path metrics
- operator automation

## Dependencies

- `A` for stable ADR semantics and milestone gating
- `D` for metrics, traces, and fault-injection evidence
- coordination with `C` where runtime refactors touch the same files

## Entry Criteria

- ADR `003` remains the authoritative semantics source for outcome states and failure behavior
- pending-finalization storage boundary is identified in code
- current best-effort branches are enumerated
- a fault-injection harness exists or is being landed in parallel by `D`

## Implementation Slices

### B1. Outcome-State Model

Land the explicit write-outcome enum and transition table first:

- `DataBranchPending`
- `DurableQuorumReached`
- `FinalizationCommitted`
- `FinalizationRecoverablePending`
- `FinalizationTerminalFailure`

The implementation should stop inferring state from log lines or loosely coupled flags.

### B2. Pending-Intent Durability Boundary

Make the recoverable branch impossible without durable pending intent.

Required behavior:

- if pending-intent persistence fails, do not claim recoverability
- return a structured retriable failure
- record enough context for operator diagnosis

### B3. Concurrent Durability Legs

Refactor the write path so:

- leader-local durable append
- remote replica fan-out

start concurrently when the leader is in the replica set.

No serial leader-first path should remain on the steady-state hot path.

### B4. Quorum Aggregation And Local-Leg Requirement

Make quorum computation explicit and enforce:

- local durable leg counts when it succeeds
- pure-remote quorum is not accepted when the leader is itself a replica-set member

This must be code-visible, not a comment convention.

### B5. Replay And Idempotency

Unify in-process retry, duplicate retry, and restart replay against the same state model.

The same write should not move through different terminal meanings depending on whether the process restarted.

### B6. Leader Degrade/Fence Policy

Implement the local-persistence distrust path:

- fail the active write
- surface a degraded reason
- optionally step down or fence leadership if the local recovery substrate cannot be trusted

## Tests And Benchmarks

- unit tests for transition-table legality
- crash-after-durable-quorum tests
- crash-before-finalization-commit tests
- pending-intent persistence failure tests
- duplicate client retry tests during recoverable-pending state
- restart replay tests for queued finalizations
- mixed local-success/remote-timeout tests
- benchmark for write-path latency before/after concurrent durability legs

## Exit Criteria

- no recoverable branch exists without durable pending intent
- local and remote durability legs are concurrent in code and proven by tests
- the outcome-state model is the single source of truth for write status
- restart replay closes recoverable-pending work without manual repair in the covered matrix
- injected failure runs show no stranded quorum-durable writes

## Artifacts

- write-outcome transition table
- fault-injection report
- replay/idempotency design note
- benchmark delta for write-path latency
- degraded/fenced-leader operator notes

## Deferred Follow-Ons

- richer client-facing retry guidance once external API shapes stabilize
- advanced partial-availability behavior under cluster-wide distress
- deeper integration with operator remediation flows in `H`
