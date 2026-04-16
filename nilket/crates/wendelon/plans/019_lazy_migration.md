# Plan: Implement Lazy Migration (Anti-Bootstrap Storm)

## Summary
This plan defines a correctness-first lazy migration protocol for epoch transitions. Logical ownership flips immediately at `ring(E+1)`, but physical bytes may remain on prior replicas until the new owner pulls them on demand or via a throttled background sweeper. To make that safe, the design adds five missing pieces:

1. Front-door fallback on point reads and range scans.
2. A dedicated peer-only migration RPC.
3. A Raft-persisted Migration Ledger that survives restarts and chained rebalances.
4. Idempotent migration ingest at the storage boundary.
5. Logical tombstones that defer physical reclaim to compaction instead of raw unlink.

The result preserves ADR 013 MVCC history, avoids bootstrap storms, and keeps the current append-only slab model intact.

## Motivation
Traditional rebalancing saturates network and disk by eagerly pushing terabytes of data when topology changes. Wendelon's promise is the opposite: the ring updates immediately, but bytes move only when reads demand them or when spare I/O exists.

That promise is only useful if migration is also correct. The implementation must:

- Keep point reads and scans complete immediately after an epoch change.
- Preserve every MVCC version still covered by the retention window.
- Survive chained migrations such as `A -> B -> C`, including node restarts mid-transition.
- Avoid duplicate migration appends when hot-read backfill and the sweeper race.
- Reclaim old-owner space in a way that matches append-only slabs rather than assuming in-place deletion.

## Goals
- **Front-Door Completeness:** `WendelonDataServer::read` and the query path must fall back to migration when the local node now logically owns a range but still lacks some or all physical records.
- **Dedicated Migration RPC:** Add a peer-to-peer administrative RPC for pulling retained versions without going through lease-based client routing.
- **Raft-Persisted Migration Ledger:** Persist migration lineage and completion state in the `WendelonStateMachine` so source selection does not depend on volatile process memory.
- **Pinned MVCC Cutoff:** Mint a per-migration `migration_hlc_floor` once and carry it through the entire migration lifecycle so source and destination agree exactly on which versions belong to the migration set.
- **Idempotent Migration Ingest:** Make migration writes no-op when an identical version is already present locally, even if multiple pulls race.
- **Tombstone-Compatible Cleanup:** Mark migrated ranges as logically moved and defer physical reclaim to slab rewrite / compaction instead of unlinking mixed slabs directly.

## Non-Goals
- **Eager Push Rebalancing:** The old owner does not proactively stream data to the new owner.
- **Immediate In-Place Range Deletion:** This target does not add hole punching or arbitrary record removal from existing slab files.
- **Client-Visible Migration API:** Clients continue to issue normal reads and queries; migration remains an internal server-side mechanism.
- **General-Purpose Compaction Redesign:** This target adds the minimum tombstone-aware reclaim path needed for migrated ranges, not a full storage-engine compaction overhaul.

## Implementation
1. **Seed a Raft Migration Ledger During Epoch Transitions**
   When the Control Ring advances from `ring(E)` to `ring(E+1)`, the leader treats the transition as a compound operation:
   - commit the epoch/config change
   - diff `ring(E)` against `ring(E+1)`
   - persist one deterministic `MigrationLedgerEntry` per reassigned contiguous hash range
   - wait for that seed batch to commit before making `E+1` externally visible

   The seed batch must be deterministic and idempotent so a retry or post-failover reconciliation produces the same logical migration set rather than duplicating it.

   Each entry should include at least:
   Each entry should include at least:
   - `migration_id`
   - `range_start`, `range_end`
   - `from_epoch`, `to_epoch`
   - `logical_owner` in `E+1`
   - ordered `source_replicas` from `E`
   - pinned `migration_hlc_floor`
   - `state` (`Pending`, `BackfillInProgress`, `Completed`)
   - `completed_by` and `completed_at_hlc` once finished

   `migration_hlc_floor` is minted exactly once when the ledger entry is created, typically as `epoch_commit_hlc - RETENTION_WINDOW` clamped at zero. It never slides afterward.

   Exact proposed state-machine shape:

   ```rust
   enum MigrationState {
       Pending,
       BackfillInProgress,
       Completed,
   }

   struct MigrationLedgerEntry {
       // Deterministic identifier derived from
       // blake3(to_epoch || range_start || range_end), truncated to u64.
       migration_id: u64,
       range_start: u64,
       range_end: u64,
       from_epoch: u64,
       to_epoch: u64,
       // Current logical owner for this epoch.
       logical_owner: NodeId,
       // If this entry was created while a previous migration for the same
       // range was still pending, point at that predecessor so apply-time
       // reconciliation can advance descendants when the predecessor completes.
       upstream_migration_id: Option<u64>,
       // Ordered pull candidates. This list is mutable inside the ledger:
       // redirects and predecessor completion may change the preferred source.
       source_replicas: Vec<NodeId>,
       // Pinned once at ledger creation; never recomputed.
       migration_hlc_floor: HybridTimestamp,
       state: MigrationState,
       completed_by: Option<NodeId>,
       completed_at_hlc: Option<HybridTimestamp>,
   }
   ```

   Deliberately excluded from the Raft ledger entry:
   - per-sweeper pagination cursor
   - transient retry counters
   - per-node in-flight pull registry state

   Those stay local so Raft only stores correctness-critical lineage and completion state, not high-churn worker progress.

2. **Add a Dedicated, Paged Migration RPC**
   Add `MIGRATE_REQ (0x0D)` and `MIGRATE_RESP (0x0E)` to the Data Ring.

   `MigrateRequestMeta` should carry:
   - `migration_id`
   - `expected_to_epoch`
   - `requester`
   - request kind (`PointLookup` or `RangePage`)
   - `range_start`, `range_end`
   - pinned `migration_hlc_floor`
   - pagination cursor
   - `max_records` / `max_bytes`

   `MigrateResponseMeta` should carry:
   - status (`OK`, `KEY_NOT_FOUND`, `REDIRECT`, `STALE_LEDGER`, `NOT_AUTHORIZED`, `INTERNAL_ERROR`)
   - `next_cursor` when more data remains
   - optional `source_hint`
   - number of returned versions / bytes

   The RPC is peer-only and bypasses lease checks, but it must still validate that:
   - the request names a valid `migration_id`
   - the requester is the ledger's current logical owner for that range
   - the requested floor matches the pinned floor in the ledger

   The response body should contain raw retained record versions, not client-shaped query results. This keeps the migration surface reusable for both point-read fallback and the background sweeper.

   Exact proposed wire metadata in `src/schema/core.rs`:

   ```rust
   #[repr(C)]
   struct MigrateRequestMeta {
       migration_id: u64,
       // The destination's belief about the current ownership epoch.
       expected_to_epoch: u64,
       requester: NodeId,
       range_start: u64,
       range_end: u64,
       migration_hlc_floor: HybridTimestamp,
       // Opaque cursor returned by the prior page. Zero means "start".
       cursor: u64,
       max_records: u32,
       max_bytes: u32,
       kind: MigrateRequestKind,
       _pad: [u8; 7],
   }

   #[repr(C)]
   struct MigrateResponseMeta {
       status: MigrateStatus,
       // 1 when another page exists; 0 when this response exhausted the source.
       has_more: u8,
       _pad0: [u8; 6],
       migration_id: u64,
       // Used on REDIRECT; zero on non-redirect success.
       source_hint: NodeId,
       next_cursor: u64,
       versions_returned: u64,
       bytes_returned: u32,
       _pad1: [u8; 4],
   }

   #[repr(C)]
   struct MigrateRequestKind(u8);
   // 0 = PointLookup, 1 = RangePage

   #[repr(C)]
   struct MigrateStatus(u8);
   // 0 = OK
   // 1 = KEY_NOT_FOUND
   // 2 = REDIRECT
   // 3 = STALE_LEDGER
   // 4 = NOT_AUTHORIZED
   // 5 = INTERNAL_ERROR
   ```

   Exact proposed response body shape:

   ```text
   Arrow IPC stream over migration_record_schema:
     key_hash: UInt64
     hlc_physical_ms: UInt64
     hlc_logical: UInt32
     hlc_node_id: UInt32
     payload_hash: FixedSizeBinary(32)
     frame_bytes: LargeBinary
   ```

   That body gives `append_migration` everything it needs for idempotency and local re-append without inventing a second decode path.

3. **Wire Migration Into the Point Read Path**
   Update `WendelonDataServer::read` so a local miss is not automatically `KEY_NOT_FOUND`.

   The new flow is:
   - run the normal epoch / lease / `E_safe` gates first
   - hash the key and attempt local `lookup_as_of`
   - if local hit: serve normally
   - if local miss: consult the Migration Ledger for the key's current range
   - if no pending ledger entry exists for a range owned by this node: return `KEY_NOT_FOUND`
   - if a pending ledger entry exists: issue a point-scoped `MIGRATE_REQ` to the current source candidate(s)
   - on success: serve the newest returned version satisfying the read's `as_of_hlc`, then asynchronously persist the returned versions via `append_migration`

   Point reads prefer a local hit, but once the range is ledger-marked `Pending`, a miss means "try migration" before surfacing not found.

4. **Wire Migration Into the Query Path Before WPBC Execution**
   The query path must not fall back only when the local scan is empty. That would miss partially backfilled ranges.

   Before `handle_query` / `execute_query_plan` executes WPBC:
   - consult the Migration Ledger for every overlapping hash subrange in the request window
   - split the query range into:
     - fully local subranges
     - pending-migration subranges
   - scan local records for the fully local parts
   - issue paged `MIGRATE_REQ` calls for pending parts
   - union the raw returned records with local records
   - de-duplicate by `(key_hash, hlc, payload_hash)`
   - only then apply `as_of_hlc`, WPBC filtering, and projection

   While a ledger entry is `Pending`, the pending range is treated as remotely backed for scan completeness even if some hot keys have already been cached locally by prior reads.

   This path must execute in bounded pages, not as "fetch the entire remote range into RAM first." `MIGRATE_REQ` pagination should be large enough to amortize RPC overhead, but the query engine should consume remote pages incrementally and run WPBC over bounded batches so a large pending range does not force whole-range buffering.

5. **Use the Ledger, Not Process Memory, for Chained Migrations**
   Chained migrations must not depend on retaining "the last N epochs" in memory.

   The authoritative rule is:
   - the Migration Ledger names the current logical owner and the ordered source candidates
   - redirect hints are an optimization, not the source of truth
   - after restart, every node rebuilds migration state from the state-machine snapshot

   For `A -> B -> C`:
   - when `E+1` assigns the range to `B`, the ledger points to previous-epoch source replicas on `A`
   - if `E+2` assigns the same range to `C` before `B` completes, the ledger still points `C` at `A` until `B` commits completion
   - once `B` finishes, it commits `MigrationComplete`, and the ledger's physical-holder view advances so future pulls go to `B`

   A source node may still answer `REDIRECT(next_source)` if it has more current local knowledge, but the requester must be able to recover by consulting the ledger alone.

   Applying `MigrationComplete` must therefore also reconcile any later pending ledger entries that overlap the completed range. If a descendant migration had been sourcing from an older physical holder only because the intermediate owner was incomplete, the state machine should advance that descendant entry's `source_replicas` view to prefer the newly completed holder.

6. **Add Version-Aware Storage Surfaces**
   Extend `SlabManager` with migration-specific scan helpers rather than reusing the current latest-only scan:
   - `scan_all_versions(range_start, range_end, migration_hlc_floor)`
   - helper(s) to materialize the raw frame bytes for each returned `RecordLocation`

   These helpers must:
   - return every retained version whose `hlc >= migration_hlc_floor`
   - preserve per-key HLC ordering
   - avoid collapsing historical versions, because ADR 013 time-travel reads depend on them

7. **Add an Idempotent Migration Ingest Path**
   Add `SlabManager::append_migration(...)` as a separate API from the ordinary write-path `append(...)`.

   Its inputs should include:
   - `migration_id`
   - `key_hash`
   - `hlc`
   - `payload_hash`
   - frame bytes

   Its semantics should be:
   - use the existing per-key version chain to fast-path collisions on `(key_hash, hlc)` rather than requiring `payload_hash` to live in every `RecordLocation`
   - on `(key_hash, hlc)` collision, compare the incoming payload hash against the already-stored frame on a slow path
   - if the payload is identical, treat the ingest as a no-op
   - if the same `(key_hash, hlc)` exists with a different payload hash, raise a fatal invariant violation instead of silently picking one
   - otherwise append the frame and index it as a normal retained version

   Keep an in-memory `MigrationRegistry` in `WendelonDataServer` to collapse concurrent pulls before disk I/O, but correctness must not depend on that registry. The durable idempotency boundary is `append_migration`.

8. **Run a Shared Dedupe Path for Hot-Read Backfill and the Sweeper**
   Create a `MigrationSweeper` that watches the ledger for entries whose `logical_owner` is the local node and whose state is not `Completed`.

   The sweeper should:
   - walk pending ranges with paged `MIGRATE_REQ`
   - use the pinned `migration_hlc_floor`
   - rate limit network and disk usage independently from the hot write path
   - ingest every returned record through `append_migration`
   - record progress by cursor / range in memory so retries resume cheaply

   Hot-read backfill and the sweeper must converge through the same ingest function so they can race without duplicating local versions.

9. **Commit Completion Through Raft, Then Install Logical Tombstones**
   Once the local node has drained the full ledger range at the pinned floor and reached end-of-source pagination for the selected source candidates, it commits:

   `WendelonRequest::MigrationComplete { migration_id, range_start, range_end, to_epoch, completed_by, completed_at_hlc, migration_hlc_floor }`

   Exact proposed Raft payload in `src/consensus/core.rs`:

   ```rust
   struct MigrationComplete {
       migration_id: u64,
       range_start: u64,
       range_end: u64,
       to_epoch: u64,
       completed_by: NodeId,
       completed_at_hlc: HybridTimestamp,
       migration_hlc_floor: HybridTimestamp,
   }
   ```

   The redundant range / epoch / floor fields are intentional. They let the
   state machine validate that the completion request still matches the live
   ledger entry rather than blindly trusting a stale `migration_id`.

   Applying that entry should:
   - transition the ledger entry to `Completed`
   - record the new physical holder for future chained migrations
   - notify prior source replicas that the range is now logically migrated

   Prior source replicas must not immediately unlink slab files. Instead they install a `MigratedRangeTombstone` into local migration / storage metadata. The tombstone:
   - fences the range out of future migration-source scans
   - marks those records as non-live for future local reclaim
   - leaves the bytes physically in place until a later slab rewrite or compaction pass

10. **Reclaim Space Through Tombstone-Aware Slab Rewrite**
    Because current slabs are append-only and may contain mixed live + migrated ranges, physical reclaim must happen later.

    The reclaim path should:
    - run during slab rotation, compaction, or tiering rewrite
    - skip records covered by a completed `MigratedRangeTombstone`
    - preserve any records still live for the source node
    - retire an old slab only when every remaining record is either migrated, expired by retention policy, or otherwise obsolete

    This keeps the storage model coherent: `MigrationComplete` changes logical liveness, and compaction converts that into eventual physical space recovery.

## Validation
The implementation is only complete once the following behaviors are covered by tests:

- point read on a newly migrated key falls back to `MIGRATE_REQ`, serves the value, and the next read hits local storage
- query over a partially backfilled range merges remote-backed and local records without dropping rows or duplicating versions
- time-travel reads after migration still resolve the correct historical version inside the retention window
- hot-read backfill racing the sweeper produces one indexed version, not duplicates
- restart during `A -> B -> C` still resolves the correct physical source from the Raft snapshot
- once `B` commits `MigrationComplete` in `A -> B -> C`, any descendant pending entry for `C` advances its source view to prefer `B`
- `MigrationComplete` installs a tombstone but does not raw-unlink mixed slabs
- tombstone-aware compaction eventually reclaims space without removing still-live records
- a mismatched `migration_hlc_floor` or stale `migration_id` is rejected by the migration RPC

## Risks and Mitigations
- **Risk:** Query latency spikes while a large pending range is still physically remote.
  - **Mitigation:** Keep `MIGRATE_REQ` paged, rate limit the sweeper, and treat redirect hints as a fast path so the owner can find the best source quickly.
- **Risk:** Disk usage remains elevated after migration completion because reclaim is deferred.
  - **Mitigation:** Accept the lag explicitly, expose metrics for tombstoned-but-not-yet-reclaimed bytes, and let compaction / slab rewrite burn the debt down asynchronously.
- **Risk:** Raft state grows with many per-range migration entries.
  - **Mitigation:** Track migration by vnode / reassigned range, not per key, and garbage-collect completed ledger entries once all source replicas have advanced past the corresponding tombstone / reclaim boundary.

## Timeline
This target should land after Per-Range Leases, because the read path needs a clear distinction between logical ownership and physical migration source. It also depends on the existing tiering sweeper patterns for rate limiting and two-phase metadata transitions.

## References
- `plans/adr/006_tiered_storage_lazy_migration.md`
- `plans/adr/013_time_travel_queries.md`
- `plans/consensus/epoch_transitions.md`
- `plans/network/read_flow.md`
- `src/network/data_server.rs`
- `src/network/ring.rs`
- `src/consensus/raft_storage.rs`
- `src/storage/manager.rs`
- `src/storage/sweeper.rs`
