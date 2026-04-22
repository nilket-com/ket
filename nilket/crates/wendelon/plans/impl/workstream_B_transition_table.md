# Workstream B Write-Outcome Transition Table

Date: 2026-04-22
Author Role: Lead Architect
Primary Code: `src/network/data_server.rs`
Supporting Code: `src/network/pending_finalizations.rs` `src/client/core.rs`

## Purpose

Pin the write-outcome state machine that now governs bifurcated-write replay, duplicate handling, and pending-finalization recovery.

This document reflects the code as landed, not the earlier planning sketch.

## State Table

| State | Meaning | Cached | Durable Pending Row | Duplicate / Retry Behavior | Main Code |
| --- | --- | --- | --- | --- | --- |
| `DataBranchPending` | Internal transient after the write crosses the bifurcation boundary and before durable quorum is established. | No | No | Must never be replayed. If observed in duplicate handling, that is a bug. | `src/network/data_server.rs:117` `src/network/data_server.rs:1378` |
| `DurableQuorumReached` | Internal transient after durable quorum exists and before finalization outcome is known. | No | Not yet guaranteed | Must never be replayed. This state exists to make the post-quorum branch explicit. | `src/network/data_server.rs:119` `src/network/data_server.rs:1378` |
| `FinalizationCommitted` | Data branch is quorum-durable and finalization is committed or observed applied. | Yes | Removed after commit | Same `write_id` replays as `DUPLICATE` and includes the original trailing marker when present. | `src/network/data_server.rs:120` `src/network/data_server.rs:1316` `src/network/data_server.rs:1361` |
| `FinalizationRecoverablePending` | Data branch is quorum-durable, pending-finalization intent is durable, finalization is not committed yet. | Yes | Yes | Same `write_id` replays as `FINALIZATION_PENDING`, may reconcile to committed inline, or redirects to current leader on turnover. | `src/network/data_server.rs:124` `src/network/data_server.rs:1347` `src/network/data_server.rs:1429` |
| `FinalizationTerminalFailure` | Terminal duplicate-replay shape with no trailing marker. Present in the state vocabulary and tests, but not the preferred steady-state live-path outcome. | Yes | No | Same `write_id` replays as `DUPLICATE` with no trailing marker. | `src/network/data_server.rs:128` `src/network/data_server.rs:1361` |

## Legal Transitions

| From | To | Trigger | Notes |
| --- | --- | --- | --- |
| `DataBranchPending` | `DurableQuorumReached` | `DurableQuorumTracker::is_satisfied()` becomes true. | Leader-local durable leg is mandatory when the leader is itself a replica. |
| `DataBranchPending` | no terminal outcome | Local durable append fails. | Node trips `LocalPersistenceDistrust`, fences writes, and returns the fenced response instead of caching a write outcome. |
| `DataBranchPending` | no terminal outcome | Quorum race exhausts without durable quorum. | Returns `INTERNAL_ERROR`; no replayable outcome is recorded. |
| `DurableQuorumReached` | `FinalizationCommitted` | Pending intent is durable and finalization resolves immediately. | Normal happy path. |
| `DurableQuorumReached` | `FinalizationCommitted` | Pending-intent persist fails, but immediate finalization succeeds. | Node still trips `LocalPersistenceDistrust` after committing because the local persistence substrate is suspect. |
| `DurableQuorumReached` | `FinalizationRecoverablePending` | Pending intent is durable, but finalization does not commit yet. | Durable pending row plus idempotency cache become the replay source of truth. |
| `DurableQuorumReached` | no terminal outcome | Pending-intent persist fails and immediate finalization also fails. | Node fences immediately and returns the fenced response. |
| `FinalizationRecoverablePending` | `FinalizationCommitted` | Duplicate replay, background reconciler, restart replay, or orphan-witness import resolves the pending record. | `complete_pending_finalization()` removes the pending row and replaces the cache entry with committed. |
| `FinalizationRecoverablePending` | `FinalizationRecoverablePending` | Reconcile attempt returns `WouldBlock` or cannot yet finish. | Retry remains safe. |
| `FinalizationRecoverablePending` | redirected pending response | Current Raft leader is not the local node. | Duplicate callers are told to refresh topology instead of looping on an ex-leader. |

## Invariants

- `DataBranchPending` and `DurableQuorumReached` are internal-only. They must not be inserted into the idempotency cache.
- `FinalizationRecoverablePending` is only legal if the pending-finalization record is durable.
- Duplicate replay is authoritative from the idempotency cache. Fresh-leader orphan-witness recovery runs before durable `write_id` lookup, but not before cache hits.
- `FinalizationCommitted` is the only steady-state terminal outcome produced by successful reconciliation.
- `FinalizationTerminalFailure` remains part of the vocabulary and duplicate contract, but the live write path now prefers either commit or distrust fencing.

## Tests Anchoring The Table

- `duplicate_write_replays_finalization_marker_and_emits_one_audit_record`
- `duplicate_write_stays_retryable_while_finalization_is_pending`
- `duplicate_write_replays_terminal_failure_without_recovery`
- `duplicate_write_replays_persisted_pending_intent_after_cache_loss`
- `pending_finalization_replays_after_restart_when_control_branch_failed`
- `leader_recovers_orphan_witnesses_from_peer_before_reconciliation`
- `fresh_leader_recovers_peer_witness_before_duplicate_write_replays`
- `cached_duplicate_replays_without_orphan_witness_sweep`

