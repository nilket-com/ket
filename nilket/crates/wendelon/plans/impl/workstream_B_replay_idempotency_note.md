# Workstream B Replay And Idempotency Note

Date: 2026-04-22
Author Role: Lead Architect
Primary Code: `src/network/data_server.rs` `src/network/pending_finalizations.rs` `src/client/core.rs`

## Purpose

Describe the replay contract that now unifies:

- in-process duplicate retry
- restart replay
- pending-finalization reconciliation
- leader-turnover recovery

## Source Of Truth Order

For a write request with `write_id = W`, the server now resolves replay in this order:

1. **Idempotency cache hit**
   `src/network/data_server.rs:3938`

   If there is a cached terminal or retryable outcome, replay it immediately. This is authoritative for the local process and now intentionally bypasses the fresh-leader orphan-witness barrier.

2. **Fresh-leader orphan-witness recovery**
   `src/network/data_server.rs:3944`
   `src/network/data_server.rs:1810`

   This runs only on cache-miss paths. It imports unresolved witness rows from peers before local durable `write_id` lookup, so a newly elected leader does not re-append a write that already has a peer witness.

3. **Durable pending lookup by `write_id`**
   `src/network/data_server.rs:3953`
   `src/network/pending_finalizations.rs:180`

   If a durable pending row exists, the server reconstructs `FinalizationRecoverablePending`, rehydrates the cache, and replays through the same reconciliation path used by live duplicates.

4. **Fresh write admission**

   Only after the cache and durable pending store both miss does the request cross the bifurcation boundary again.

## Duplicate Behavior By Outcome

| Outcome | Duplicate Response | Notes |
| --- | --- | --- |
| `FinalizationCommitted` | `DUPLICATE` plus original trailing marker when present | `duplicate_response_for_outcome()` rewrites the status only. |
| `FinalizationRecoverablePending` | `FINALIZATION_PENDING`, or `NOT_LEADER` if leadership moved | Duplicate replay may reconcile inline and upgrade to committed. |
| `FinalizationTerminalFailure` | `DUPLICATE` with no trailing marker | Present in the replay contract, though not the preferred live-path outcome. |

## Restart Semantics

- Restart begins with an empty idempotency cache.
- Durable pending rows survive restart in `pending_finalizations.redb`.
- `find_by_write_id()` is now indexed, not an O(n) scan.
- If multiple pending rows share a `write_id`, `remove()` repoints the index to a surviving row rather than dropping lookup coverage.
- Raft storage now persists committed progress explicitly, and restart recovery will not replay an uncommitted surviving WAL tail into the state machine when no durable checkpoint exists.

This means restart replay and same-process duplicate replay now funnel through the same pending-record semantics instead of diverging.

Operator-facing note:

- the degraded/fenced-leader note in `plans/impl/workstream_B_operator_notes.md` now calls out the tighter restart boundary so a pre-checkpoint restart does not look like silent data loss when it is actually truncating uncommitted tail residue.

## Leader-Turnover Semantics

- Duplicate replay of `FinalizationRecoverablePending` checks the current Raft leader and redirects with `NOT_LEADER` if the local node is no longer leader.
- New leaders perform orphan-witness pull before cache-miss admission.
- Cached duplicate replay is allowed even if that sweep cannot currently reach every peer.

This closes the old ex-leader loop and the old fresh-leader duplicate re-append window.

## Client Retry Contract

The smart client currently treats these write statuses as retryable:

- `NOT_LEADER`
- `EPOCH_STALE`
- `LEASE_EXPIRED`
- `INVALID_LEASE_SIGNATURE`
- `FINALIZATION_PENDING`

Code:

- `src/client/core.rs:969`
- `src/schema/core.rs:585`

Important nuance:

- `FINALIZATION_PENDING` is the only status that means "data branch is already durable; retry the same `write_id`."
- Lease and topology statuses may refresh topology and reroute, but they do not imply durable data already exists.

## Current Boundaries

- Replay is unified for committed, pending, restart, and leader-turnover cases.
- The remaining gap is not replay semantics; it is observability and benchmark evidence around those semantics.
- A clean leadership-abdication flow is still separate from this note. Current distrust behavior is shutdown-and-fence, not a polished transfer-leadership sequence.
