# Workstream B — Raft Durability Lifecycle Completion

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_execution_20260417.md` (Phase 2)
**Status:** First pass
**Subsystems:** `src/consensus/raft_storage.rs`
**Depends on:** Workstream A (integration gate)

---

## Problem Statement

The Raft durability lifecycle is incomplete in three critical areas:

1. **Snapshots are memory-only.** `install_snapshot()` (line 1008) and
   `build_snapshot()` (line 1309) store bytes in `guard.snapshot` but
   never write `snapshot.wnd` to disk. Recovery *can* load `snapshot.wnd`
   (line 1405), but no live path creates it. After a crash, there is no
   snapshot to recover from.

2. **Checkpoint emission does not exist.** `REC_CHECKPOINT` is defined
   (line 90) and parsed during recovery (line 1589), but no production
   code writes checkpoint records. Without them, restart-time clamping of
   commit / `E_safe` depends on test-only paths.

3. **WAL truncation and purging are in-memory only.** `truncate()`
   (line 677) and `purge()` (line 697) mutate only the in-memory cache.
   WAL growth is unbounded on disk. A long-running node will accumulate
   WAL entries indefinitely.

Additionally, the snapshot install path uses `Cursor<Vec<u8>>` (line 1002),
meaning a large state-machine snapshot buffers entirely in RAM. At
enterprise scale this is untenable.

---

## Code Ownership / Subsystem Boundaries

| Area | Primary Files | Notes |
|------|--------------|-------|
| WAL + snapshot storage | `src/consensus/raft_storage.rs` | All changes are in this file and its test module |
| State machine | `src/consensus/raft_storage.rs` (SM impl) | `install_snapshot` and `build_snapshot` |
| Node bootstrap | `src/node.rs` | May need minor changes to handle new snapshot-on-disk lifecycle |
| Recovery tests | `src/consensus/raft_storage.rs` (test module) + `tests/` | New recovery scenarios |

**Boundary note:** This workstream does NOT touch slab storage, L2
tiering, or the network layer. It is purely the Raft control-plane
durability lifecycle.

---

## Ordered Implementation Steps

### B.1 — Snapshot Persistence

| # | Step | Target | Details |
|---|------|--------|---------|
| B.1.1 | Add `persist_snapshot` helper | `raft_storage.rs` | Write snapshot bytes to `snapshot.wnd.tmp`, fsync, rename to `snapshot.wnd`. Atomic via tmp+rename. |
| B.1.2 | Call `persist_snapshot` from `install_snapshot` | `raft_storage.rs:1008` | After updating in-memory state, persist to disk. Failure to persist should be a hard error (node cannot guarantee recovery). |
| B.1.3 | Call `persist_snapshot` from `build_snapshot` | `raft_storage.rs:1309` | After building the snapshot bytes, persist before returning. |
| B.1.4 | Recovery test: snapshot + WAL tail | `tests/` | Write entries, trigger snapshot, write more entries, crash, restart. Verify state = snapshot + tail entries. |

### B.2 — Checkpoint Emission

| # | Step | Target | Details |
|---|------|--------|---------|
| B.2.1 | Add `emit_checkpoint` method | `raft_storage.rs` | Write a `REC_CHECKPOINT` record to the WAL with the current `last_applied_log_id` and `E_safe`. |
| B.2.2 | Wire checkpoint emission to snapshot lifecycle | `raft_storage.rs` | After successful `persist_snapshot`, emit a checkpoint. This marks the WAL position where the snapshot is valid. |
| B.2.3 | Recovery test: checkpoint + truncation | `tests/` | Write entries, snapshot, checkpoint, write more, crash. Verify recovery uses checkpoint to clamp replay start. |

### B.3 — Durable WAL Compaction

| # | Step | Target | Details |
|---|------|--------|---------|
| B.3.1 | Design compaction strategy | — | Options: (a) rewrite WAL without purged prefix, (b) hole-punch via `fallocate(FALLOC_FL_PUNCH_HOLE)`, (c) rotate to a new WAL file and delete the old one after checkpoint. Option (c) is simplest and aligns with the checkpoint mechanism. |
| B.3.2 | Implement durable purge | `raft_storage.rs:697` | After `purge()` updates in-memory state, perform the chosen compaction strategy on disk. |
| B.3.3 | Make `truncate()` durable | `raft_storage.rs:677` | Write a truncation marker or physically truncate the WAL file. |
| B.3.4 | Long-running WAL-size test | `tests/` | Run 10,000 Raft entries with periodic snapshots + purges. Assert WAL file size stays bounded (not monotonically growing). |

### B.4 — Snapshot Streaming (large snapshots)

| # | Step | Target | Details |
|---|------|--------|---------|
| B.4.1 | Replace `Cursor<Vec<u8>>` with chunked reader/writer | `raft_storage.rs:1002` | Define a streaming snapshot interface that writes chunks to disk incrementally. |
| B.4.2 | Chunked snapshot install | `raft_storage.rs:1008` | `begin_receiving_snapshot` returns a file-backed writer. `install_snapshot` finalizes by fsync + rename. |
| B.4.3 | Snapshot compression | `raft_storage.rs` | Apply zstd or lz4 compression to snapshot bytes before persist. Decompress on load. |
| B.4.4 | Test: large snapshot round-trip | `tests/` | Build a snapshot > 100 MB, install via chunked path, crash, recover. Verify state integrity and that peak memory stayed bounded. |

---

## Test Plan

| Test | Type | What it proves |
|------|------|----------------|
| Snapshot persists to disk | Unit | `snapshot.wnd` exists after `build_snapshot` |
| Snapshot recovery after crash | Integration | Node recovers from `snapshot.wnd` + WAL tail |
| Checkpoint clamps replay | Unit | Recovery with checkpoint skips entries before checkpoint |
| WAL size bounded | Long-running | Purge + compaction keep WAL size from growing unboundedly |
| Truncation durability | Unit | After `truncate()`, restarted node does not see truncated entries |
| Large snapshot streaming | Integration | 100+ MB snapshot doesn't OOM; recovery works |

---

## Rollout and Migration Concerns

- **Backward compatibility with existing WAL files.** Nodes upgrading
  from current code have WALs that have never been compacted and no
  `snapshot.wnd` on disk. The recovery path must handle the "no snapshot
  file" case gracefully (it already does, but verify after changes).
- **WAL format versioning.** If the compaction strategy introduces new
  record types or changes the WAL file layout, add a version byte or
  magic header to distinguish old from new WAL formats.
- **Snapshot format versioning.** If compression is added, the snapshot
  file needs a header indicating whether it's compressed and with which
  codec. Otherwise an old node cannot read a new snapshot (or vice versa
  during rolling upgrade).

---

## Failure Modes and Rollback

- **Crash during snapshot persist.** The tmp+rename pattern ensures
  atomicity — a crash before rename leaves the old snapshot (or no
  snapshot) intact. Verify this explicitly in tests.
- **Crash during WAL compaction.** If using WAL file rotation, a crash
  between "new WAL created" and "old WAL deleted" must be handled on
  recovery (scan for both files, use the newer one, delete the old).
- **Disk full during snapshot.** `persist_snapshot` must handle ENOSPC
  gracefully — log the error, do not corrupt existing files, and surface
  the condition to monitoring. The node can continue operating without a
  new snapshot; it just can't compact the WAL.

---

## Exit Criteria

- `snapshot.wnd` exists on disk after every snapshot build/install.
- WAL file size is bounded in a long-running test with periodic snapshots.
- Recovery from snapshot + WAL tail is tested and passes.
- Checkpoint records are emitted and consumed during recovery.

---

## Open Questions

1. **WAL compaction strategy:** File rotation (simplest, aligns with
   checkpoints) vs. hole-punch (preserves single file, less disk churn)
   vs. rewrite (most flexible, most complex). Need to evaluate against
   io_uring + O_DIRECT constraints — does `fallocate` interact correctly
   with O_DIRECT?
2. **Snapshot trigger policy:** Who decides when to take a snapshot?
   openraft has its own snapshot policy, but is it configured? Currently
   openraft config is almost entirely defaulted (`node.rs:477–490`). The
   snapshot interval / log-count threshold should be explicit.
3. **Snapshot compression codec:** zstd (better ratio) vs. lz4 (faster)?
   For a control-plane snapshot the size is likely small (membership +
   epoch state), so compression may not matter much. But if the state
   machine grows to include slab metadata indexes, it could be large.
4. **Interaction with openraft's built-in snapshot/compaction.** openraft
   has its own `purge_log` and snapshot machinery. Are we fighting the
   framework by implementing durable purge ourselves, or is this the
   intended integration point? Review openraft docs for the expected
   `RaftLogStorage::purge` contract.
