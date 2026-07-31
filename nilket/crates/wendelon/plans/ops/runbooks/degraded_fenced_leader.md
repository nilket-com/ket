# Degraded And Fenced Leader

Date: 2026-04-22
Author Role: Lead Architect
Primary Code: `src/network/data_server.rs` `src/node.rs`

## Meaning Of `LocalPersistenceDistrust`

`LocalPersistenceDistrust` means the node has observed an integrity-threatening failure in the local write persistence substrate and is no longer trusted to admit new data writes.

Current runtime behavior:

- data writes are fenced
- Raft runtime heartbeats/election/ticks are disabled
- `raft.shutdown()` is attempted
- readiness reports the node as degraded
- a durable distrust marker is written to disk and survives restart

Code:

- fence activation: `src/network/data_server.rs:695`
- distrust trip: `src/network/data_server.rs:848`
- readiness surface: `src/node.rs:1540`

## Current Trip Conditions

The current implementation trips distrust on:

- leader-local durable append failure or task failure
- pending-finalization intent persistence failure after durable quorum
- replica-side pending witness persistence failure while handling replicated writes

Representative code:

- `src/network/data_server.rs:4121`
- `src/network/data_server.rs:4166`
- `src/network/data_server.rs:3603`

## Operator-Visible Symptoms

- write requests stop succeeding on the affected node
- readiness `degraded_reason` becomes `LocalPersistenceDistrust`
- the file `local_persistence_distrust.flag` exists under the slab data directory
- restarting the node does not clear the condition

Anchoring tests:

- `readiness_reporter_surfaces_local_persistence_distrust`
- `bootstrap_restores_local_persistence_distrust_fence`
- `local_persistence_failure_fences_future_writes`

## Immediate Operator Action

1. Treat the node as storage-suspect, not as a transient application blip.
2. Stop routing client traffic that depends on this node.
3. Capture logs around the distrust trip detail string before rebuilding or replacing the node.
4. Inspect the local disk, filesystem, kernel, and host-level I/O error history.
5. Prefer replace-or-rebuild over in-place trust restoration unless the root cause is clearly external and fixed.

## Current Recovery Model

The code intentionally persists the distrust marker and does not provide an automatic clear path.

That means recovery is currently an operator decision, not an automatic retry loop. Clearing the distrust condition should be treated as a maintenance action after root-cause resolution, not as part of normal request handling.

## Known Limitation

This is a safe fence, not a polished leadership handoff.

What exists now:

- shutdown-and-fence
- degraded readiness
- persistent distrust across restart

What does not exist yet:

- a clean transfer-leadership / abdication protocol with in-flight drain semantics
- a first-party operator workflow for clearing the distrust marker
- D-workstream metrics/traces proving distrust behavior under broader chaos coverage

## Restart Recovery Boundary

Recent recovery work tightened the restart contract for pre-checkpoint nodes:

- committed progress is now persisted explicitly through the Raft storage `save_committed` / `read_committed` path
- a torn WAL tail at EOF no longer implies `last_applied = last surviving log entry`
- when no durable checkpoint exists, restart recovery now treats an uncommitted surviving WAL tail as `last_applied = None`

Operational meaning:

- a restarted node may come back with a smaller recovered state-machine boundary than older builds implied
- that is intentional and safer; uncommitted WAL residue must not be replayed into the state machine
- operators investigating restart behavior should expect the recovered state machine to follow the committed boundary, not the raw last surviving WAL page

Cross-reference:

- ADR `003`
- `tests/stream_finalization.rs`
