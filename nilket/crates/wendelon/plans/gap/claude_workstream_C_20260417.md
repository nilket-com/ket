# Workstream C — Tiered Storage Closure

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_execution_20260417.md` (Phase 3)
**Status:** First pass
**Subsystems:** `src/storage/l2.rs`, `src/storage/sweeper.rs`, `src/storage/manager.rs`, `src/node.rs`
**Depends on:** Workstream A (integration gate, TierObserver wiring)

---

## Problem Statement

L2 object storage tiering has solid building blocks but is not
production-integrated. The critical issues are:

1. **Not wired into the daemon.** `SlabSweeper` is only started in tests.
   `config.rs` hardcodes `l2: None`. The stock daemon cannot tier data.
2. **Follower disk leak.** `TierObserver` is not attached during bootstrap,
   so followers never unlink L1 slabs after tiering.
3. **Crash-safety gaps.** Upload → Raft commit → local unlink are not
   transactionally journaled. A crash between upload and commit orphans
   S3 objects. A crash during local compaction can resurface stale data.
4. **No orphan GC.** No mechanism to reconcile S3 bucket contents against
   Raft-committed tiering state.
5. **No L2 object deletion lifecycle.** Objects are never deleted after
   retention expiry or migration cleanup.
6. **Whole-object reads.** L2 reads pull the entire slab (64 MiB+) for a
   single record lookup. The `ObjectStore` trait has no range-GET.
7. **Follower-side reconciliation on restart is incomplete.** After
   restart, a follower may have locally-present slabs that Raft says are
   tiered, but no cleanup runs.

---

## Code Ownership / Subsystem Boundaries

| Area | Primary Files | Notes |
|------|--------------|-------|
| ObjectStore trait + S3 impl | `src/storage/l2.rs` | Trait shape change for range-GET |
| Sweeper | `src/storage/sweeper.rs` | Production wiring, pending-upload journal |
| Slab manager | `src/storage/manager.rs` | Compaction crash-safety, reconciliation, rehydration |
| Node bootstrap | `src/node.rs` | Sweeper startup, TierObserver (Workstream A) |
| Config | `src/config.rs` | L2 parsing (Workstream A) |
| Tests | `tests/l2_s3.rs`, `tests/l2_rehydration.rs`, new integration tests | |

**Boundary note:** L2 config parsing and TierObserver wiring are in
Workstream A. This workstream assumes those are done.

---

## Ordered Implementation Steps

### C.1 — Production Wiring

| # | Step | Target | Details |
|---|------|--------|---------|
| C.1.1 | Start `SlabSweeper` in `bootstrap_with_provider` | `src/node.rs` | Currently only migration sweeper is spawned (line ~638). Add sweeper spawn alongside it when L2 config is present. |
| C.1.2 | Follower-side reconciliation on startup | `src/storage/manager.rs` | On `SlabManager::open`, scan `state.tiered` entries and unlink any locally-present L1 files that Raft says are tiered. |
| C.1.3 | Integration test: daemon with L2 config tiers a slab | `tests/` | Boot daemon with MinIO L2 config, write enough data to seal a slab, wait for sweeper to tier it, verify S3 object exists and L1 file is unlinked. |

### C.2 — Crash-Safety and Orphan Management

| # | Step | Target | Details |
|---|------|--------|---------|
| C.2.1 | Pending-upload journal | `src/storage/sweeper.rs` | Before uploading, write a durable journal entry (slab ID, upload intent timestamp). On recovery, journal entries without corresponding Raft commits are candidates for cleanup. |
| C.2.2 | Orphan GC reconciliation loop | `src/storage/sweeper.rs` or `src/storage/manager.rs` | Periodic task: list S3 bucket objects, compare against `tiered_slabs` in Raft state, delete objects not in state. Rate-limit to avoid API cost spikes. |
| C.2.3 | Crash-safe local compaction | `src/storage/manager.rs` | Journal the old → new slab swap before starting. On recovery, if journal entry exists but swap is incomplete, roll back (delete new slab) or roll forward (complete swap). |
| C.2.4 | Leader-only upload enforcement | `src/storage/sweeper.rs` | Check leadership before uploading. If not leader, skip the slab (the actual leader will handle it). Prevents follower-side wasted uploads and object leaks. |
| C.2.5 | Integration test: crash mid-tier, restart, verify no orphans | `tests/` | Upload succeeds, crash before Raft commit, restart. Orphan GC should clean up the dangling S3 object. |

### C.3 — L2 Object Lifecycle

| # | Step | Target | Details |
|---|------|--------|---------|
| C.3.1 | L2 object deletion on migration cleanup | `src/storage/manager.rs` | When `reclaim_tombstoned_ranges` removes a tiered slab from state, delete the corresponding S3 object. |
| C.3.2 | Retention-based L2 deletion | `src/storage/sweeper.rs` | Configurable retention policy: delete L2 objects older than N days after the last read. |
| C.3.3 | Tombstone compaction for tiered slabs | `src/storage/manager.rs` | Extend `compact_tombstoned_sealed_slabs` to also handle `state.tiered` entries (currently only iterates `state.sealed`). |

### C.4 — ObjectStore Trait Redesign

| # | Step | Target | Details |
|---|------|--------|---------|
| C.4.1 | Extend `ObjectStore` trait with `get_range(key, offset, length)` | `src/storage/l2.rs` | Breaking change. Also add `list(prefix)` for orphan GC. Consider adding `put_multipart` at the same time to avoid a second break. |
| C.4.2 | Implement range-GET for `RemoteObjectStore` | `src/storage/l2.rs` | Map to opendal `read_with().range()` or equivalent. |
| C.4.3 | Implement range-GET for `LocalFsObjectStore` | `src/storage/l2.rs` | Seek + read for the local test implementation. |
| C.4.4 | Update L2 read path to use range-GET | `src/storage/manager.rs` | `read_from_l2` should read only the slab record's byte range, not the entire slab. Requires a slab-level record offset index (either stored in S3 object metadata or in a separate manifest). |
| C.4.5 | Multipart upload for large slabs | `src/storage/l2.rs`, `src/storage/sweeper.rs` | Use opendal multipart for slabs above a size threshold. |

---

## Test Plan

| Test | Type | What it proves |
|------|------|----------------|
| Daemon tiers a slab via config | Integration | End-to-end L2 wiring through stock daemon path |
| Follower L1 cleanup after tiering | Integration | TierObserver + reconciliation on restart |
| Crash mid-upload, orphan cleaned | Integration | Pending-upload journal + orphan GC work |
| Crash mid-compaction, data intact | Unit | Journal-based compaction recovery |
| Range-GET reads partial slab | Unit | Only requested bytes transferred |
| L2 object deleted on migration | Unit | Object lifecycle not leaking |
| Long-running orphan GC | Integration | S3 bucket converges to only valid objects |

---

## Rollout and Migration Concerns

- **ObjectStore trait break.** Any external consumers of the trait (if
  any) will need to implement `get_range` and `list`. The `LocalFsObjectStore`
  used in tests must be updated first.
- **Slab record offset index.** Range-GET requires knowing where in the
  slab blob each record lives. This may require storing a manifest
  alongside the slab in S3 (e.g., as a separate small object or as S3
  object metadata). Design decision needed.
- **Orphan GC rate limiting.** S3 LIST + DELETE API calls cost money.
  The reconciliation loop needs configurable interval and batch size.
- **Existing clusters upgrading.** Clusters that already have tiered
  slabs but no pending-upload journal will not have journal entries for
  existing objects. The orphan GC must handle the "no journal" case
  gracefully (treat all objects in bucket as potentially valid until
  verified against Raft state).

---

## Failure Modes and Rollback

- **Orphan GC deletes a valid object.** If Raft state is stale on the
  GC runner (e.g., it's a lagging follower), it might delete an object
  the leader knows about. GC must run on the leader or verify against
  a quorum read.
- **Range-GET offset wrong.** If the record offset index is stale or
  corrupt, range-GET returns garbage. Always validate the blake3 checksum
  on the returned bytes — the existing per-record checksum path handles
  this.
- **Pending-upload journal grows unbounded.** If the node never becomes
  leader (so uploads never complete), journal entries accumulate. Add a
  TTL sweep for journal entries that are older than a threshold.

---

## Exit Criteria

- L2 tiering works end-to-end through the stock daemon with TOML config.
- Follower disk usage stabilizes (no leak) after tiering.
- Crash mid-tier does not produce permanent orphans (cleaned within one
  GC cycle).
- Point reads of tiered data do not download entire slabs.

---

## Open Questions

1. **Slab record offset index for range-GET.** Stored as S3 object
   metadata, as a sidecar manifest object, or embedded as a header in
   the slab blob itself? Each has different cost/complexity tradeoffs.
2. **Orphan GC leader-only or any-node?** Running on leader is safest
   (freshest Raft state) but concentrates work. Running on any node
   requires a quorum read to verify state freshness.
3. **Multipart upload threshold.** At what slab size should multipart
   kick in? opendal may have its own default. Need to align with the
   configurable `slab_max_bytes`.
4. **Pending-upload journal format.** Separate file, or additional record
   type in the existing WAL? Using the WAL is elegant but couples the
   tiering lifecycle to Raft log compaction. A separate journal file is
   simpler but another file to manage.
5. **ObjectStore trait break scope.** Should we batch range-GET, streaming
   put, multipart, and list into one breaking change to minimize churn?
   All three reviewers flagged this — better to do it once.
