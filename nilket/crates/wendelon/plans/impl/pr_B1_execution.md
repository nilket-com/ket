# PR B1 Execution Pack

Date: 2026-04-22
Author Role: Lead Architect
Parent Backlog: `plans/impl/workstream_B_backlog.md`

## PR Intent

Land the explicit write-outcome state model from ADR `003` without changing the external write wire contract unless absolutely necessary.

This PR should create the semantic vocabulary for the bifurcated write path. It should **not** try to fix every behavior problem in the write path yet.

## Current Code Reality

The current write path in `src/network/data_server.rs` already contains the bones of a state machine, but it is implicit:

- the idempotency cache stores `CachedWriteOutcome`
- pending finalizations are tracked through `PendingFinalizationRecord`
- successful finalize paths are handled by `complete_pending_finalization(...)`
- recoverable failure is represented indirectly by:
  - `pending_error_response(...)`
  - `pending_finalization_id: Some(...)`
  - wakeup of the pending-finalization reconciler

The result is that the code behaves like a state machine without naming one.

## Tactical Goal

Make the internal write path speak in terms of an explicit outcome enum, while preserving current external `WriteResponseMeta` behavior as much as possible.

## Primary Files

- `src/network/data_server.rs`

Secondary only if needed:

- `src/storage/manager.rs`

Preferred outcome: keep the first landing entirely inside `data_server.rs` plus local tests.

## Target Types

### New Internal Enum

Add a new internal enum near `CachedWriteOutcome`:

- `DataBranchPending`
- `DurableQuorumReached`
- `FinalizationCommitted`
- `FinalizationRecoverablePending`
- `FinalizationTerminalFailure`

Recommended shape:

```rust
enum WriteOutcome {
    DataBranchPending { commit_hlc: HybridTimestamp },
    DurableQuorumReached { commit_hlc: HybridTimestamp },
    FinalizationCommitted {
        response: WriteResponseMeta,
        trailing: Option<FinalizationMarker>,
    },
    FinalizationRecoverablePending {
        response: WriteResponseMeta,
        pending_finalization_id: PendingFinalizationId,
    },
    FinalizationTerminalFailure {
        response: WriteResponseMeta,
    },
}
```

The exact payloads may vary, but the names should match ADR `003`.

### Cache Shape

Refactor `CachedWriteOutcome` so it wraps the explicit outcome rather than encoding state indirectly via:

- `response`
- `finalization`
- `pending_finalization_id`

Recommended direction:

```rust
struct CachedWriteOutcome {
    outcome: WriteOutcome,
}
```

The cache only needs to persist the outcome variants that matter to duplicate/replay handling. The full enum can still be used as transient local state in the write pipeline.

## Target Functions And Edit Plan

### 1. `WendelonDataServer::write(...)`

Current role:

- performs epoch gate
- checks idempotency
- bifurcates payload
- runs durable quorum logic
- persists pending finalization
- attempts finalization
- inserts idempotency records

Required change:

- express local transitions through `WriteOutcome`
- make the current “pending error + pending_finalization_id” path become a named `FinalizationRecoverablePending`
- make the successful path become a named `FinalizationCommitted`

Important:

- do **not** change concurrent execution semantics yet
- do **not** change quorum counting semantics yet

### 2. `complete_pending_finalization(...)`

Current role:

- removes the pending row
- builds the committed response
- inserts a cached committed outcome

Required change:

- make this return or construct `WriteOutcome::FinalizationCommitted`
- keep `WriteResponseMeta` generation centralized here or in one small helper

### 3. Idempotency Fast Path Inside `write(...)`

Current role:

- replays committed result as duplicate
- if cached pending id exists, attempts to reconcile pending finalization first

Required change:

- branch on `WriteOutcome`, not on `pending_finalization_id: Option<_>`
- make duplicate replay logic explicitly understand:
  - `FinalizationCommitted`
  - `FinalizationRecoverablePending`

### 4. Response Helpers

Current helpers:

- `pending_error_response(...)`
- `committed_response_for(...)`

Required change:

- either keep these and wrap them into `WriteOutcome` constructors
- or replace them with `WriteOutcome` constructor helpers

Preferred direction:

- one helper to build a committed outcome
- one helper to build a recoverable-pending outcome
- one helper to derive the duplicate response tuple from an outcome

## Explicit Non-Goals For This PR

- no concurrent local/remote durability-leg rewrite
- no quorum-rule rewrite
- no leader fencing
- no transport or `io_uring` changes
- no client wire-shape changes unless the current internal response shape makes the refactor impossible

## Suggested Commit Slices

### Commit 1: Introduce `WriteOutcome`

- add enum
- add local helper constructors
- add unit tests for legal/illegal state interpretation

### Commit 2: Refactor Cache And Duplicate Replay

- move `CachedWriteOutcome` to explicit outcome storage
- update duplicate-path logic to branch on outcome

### Commit 3: Refactor Write Finalization Paths

- thread `WriteOutcome` through successful finalize and pending-recoverable branches
- keep external `WriteResponseMeta` unchanged

### Commit 4: Test Cleanup And Transition Assertions

- expand targeted tests
- remove any now-dead implicit-path comments

## Required Tests

Keep these existing tests green and strengthen them where useful:

- `duplicate_write_replays_finalization_marker_and_emits_one_audit_record`
- `pending_finalization_replays_after_restart_when_control_branch_failed`
- `write_counts_only_durable_quorum_acks_across_replica_crash_boundary`

Add new focused tests for:

- committed outcome maps to duplicate replay correctly
- recoverable-pending outcome maps to duplicate replay correctly
- terminal failure does not masquerade as recoverable pending

## Review Checklist

- does the PR create one explicit internal write-outcome vocabulary?
- does duplicate replay depend on that vocabulary rather than ad hoc option fields?
- is external write behavior unchanged except for clearer internal representation?
- did the PR avoid sneaking in B2/B3/B4 behavior changes?

## Abort Conditions

Stop and split the work if:

- you need to change external `WriteStatus` semantics
- you find that concurrent durability-leg changes are required just to land the enum
- `src/storage/manager.rs` starts growing into the main site of change

If that happens, the PR is too big or incorrectly scoped.

## Recommended First Code Move

Start by replacing `CachedWriteOutcome` with an outcome-backed form and update only the duplicate-replay branch in `write(...)`.

That is the smallest real cut that proves the model can work without rewriting the whole function at once.
