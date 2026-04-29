# Implementation Plan: ADR 047 — Row-Level Tombstones on Append-Only Tables

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/047_row_tombstones_append_only.md`
Inherits substrate: `plans/adr/046_state_digest_substrate.md`
Bench reference: `plans/impl/046_state_digest_bench_results.md`

## Goal

Ship row-level tombstones on append-only tables under the
algebraic composition rules ratified in ADR 047. Validate
ADR 046's `subtract_element` against a real deletion-shaped
consumer. Land the lifecycle algebra that the retention ADR
(roadmap #3) and the secondary-index ADR (roadmap #8) compose
against.

Deliverables:

- One new synthetic column on append-only tables
  (`__row_tombstone__`).
- One reserved bit in the ADR 022 footer
  (`has_row_tombstone_records`) allocated as
  `HAS_ROW_TOMBSTONE_RECORDS_BIT = 1 << 0` in the HMAC-covered
  footer flag byte.
- One new MsgType pair in the typed-table write family
  (`TABLE_DELETE_REQ` / `TABLE_DELETE_RESP`).
- Tombstone-only multi-row record shape (no user columns).
- `is_tombstone` bit on local and persistent index entries.
- Latest-as-of-HLC visibility filtering across scan, PBC, AGG,
  and ADR 044 column-fetch.
- Compaction-window pair GC and retention-floor pair reclaim.
- Recovery: rebuild version chains with tombstone bit; fail-
  closed on duplicate tombstones.
- ADR 040 `tombstone_metadata` extension.
- Visible-domain `StateDigest` add / subtract at seal-time
  (in-memory; no durable footer field).
- Golden vectors and algebraic invariants exercising
  subtraction.
- Cold-path bench extension covering visible-subtract cost,
  no-delete fast path, and tombstone-density overhead.

## Non-goals

- No envelope `format_version` bump.
- No descriptor format-version bump.
- No new HRFT frame family. (`TABLE_DELETE_REQ` is a new
  MsgType inside the existing typed-table write family.)
- No new node capability bit.
- No `ObjectStore` trait change.
- No sidecar block. No new on-disk surface beyond the one
  footer bit.
- No durable `StateDigest` field anywhere (slab footer,
  pointer, sidecar, catalog, wire).
- No physical / tombstone / event-history digest domains.
  Only `digest.table.row.visible.v1` (already in ADR 046).
- No PK-keyed delete. No content-hash identity. No new
  per-table retention policy / syntax (the existing MVCC
  retention floor is consulted in Phase 7 for pair reclaim
  but no new retention surface is introduced — per-table
  retention is roadmap #3). No CDC cursor. No secondary-
  index propagation. No anti-entropy.
- No tolerant decoder for pre-tombstone slab bodies. The
  persistent local-index decoder's V1/V2 length tolerance
  pre-existed ADR 047 as ADR 042 index-rebuild compatibility;
  ADR 047 only adds the V3 `is_tombstone` bit.
- No `__row_tombstone_target__` column. The target identity
  is the row's shared `__row_key_id__`.

## Phase 0 — Contract freeze, fixtures, architectural guards

Lock the on-disk and wire shape before touching admission or
scan code.

Files:

- `src/storage/slab_footer.rs`
- `src/schema/synthetic_columns.rs`
- `src/network/frames.rs`
- `tests/fixtures/row_tombstones/` (new)
- `tests/architecture_test.rs`

Work:

1. **Audit ADR 022 / ADR 039 footer layout for a clean HMAC-
   covered flag slot** for `has_row_tombstone_records`. The
   audit found the existing HMAC-covered footer flag byte and
   allocated bit 0. The bit must remain:
   - inside the existing HMAC scope,
   - pinned via a named const
     (`HAS_ROW_TOMBSTONE_RECORDS_BIT`),
   - separable from any visibility-authority field.

2. Add the synthetic-column constant:
   ```rust
   pub const STORAGE_ROW_TOMBSTONE_COLUMN: &str = "__row_tombstone__";
   ```
   Storage type: Arrow `Boolean`. Append-only-only.

3. Reject user-declared columns named `__row_tombstone__` in
   descriptor validation.

4. Freeze the request / response shapes:
   ```rust
   pub struct DeleteRowsRequest {
       pub tenant_id: TenantId,
       pub table_object_id: TableObjectId,
       pub schema_generation: u64,
       pub parent_write_id: ClientWriteId,
       pub row_key_ids: Vec<Identity>,
   }

   pub struct DeleteRowsResponse {
       pub deleted_count: u32,
       pub already_deleted_count: u32,
       pub not_found_count: u32,
       pub write: WriteResponseMeta,
   }
   ```

5. Freeze the wire encoding. Fixed metadata struct + Arrow
   `RecordBatch` payload of one `FixedSizeBinary(32)` column
   named `__row_key_id__`:
   ```rust
   #[repr(C)]
   pub struct TableDeleteRowsRequestMeta {
       pub parent_write_id: ClientWriteId,
       pub client_hlc: HybridTimestamp,
       pub expected_epoch: u64,
       pub tenant_id: TenantId,
       pub table_object_id: TableObjectId,
       pub schema_generation: u64,
       pub row_count: u32,
       pub durability: DurabilityLevel,
       pub await_finalization: u8,
       pub _pad: [u8; 10],
   }

   #[repr(C)]
   pub struct TableDeleteRowsResponseMeta {
       pub write: WriteResponseMeta,
       pub deleted_count: u32,
       pub already_deleted_count: u32,
       pub not_found_count: u32,
       pub reject_code: u16,
       pub _pad: [u8; 10],
   }
   ```

   V1 delete requests are leader-direct. The frozen request meta
   intentionally omits `proxy_hop` / `proxy_source`; followers
   return `NOT_LEADER` before target resolution.

6. Allocate `MsgType::TABLE_DELETE_REQ` and
   `MsgType::TABLE_DELETE_RESP` from the typed-table write
   family allocation table. `0x34` / `0x35` are allocated;
   `0x2E` / `0x2F` were already occupied by schema-update
   frames.

7. Lock idempotency semantics:
   - duplicate row IDs deduped within a request;
   - one leader-merged `delete_hlc` per request;
   - latest-as-of-HLC tombstone → no second tombstone;
   - `parent_write_id` retry hits the existing idempotency
     cache when still resident;
   - after restart, durable logical idempotency can report
     already-deleted counts instead of replaying the original
     deleted counts.

8. Lock `StateDigest` semantics:
   - data row → `add_element(visible_row_element(t, T, k))`;
   - tombstone row →
     `subtract_element(visible_row_element(t, T, k))`;
   - no new domain helpers introduced;
   - no durable digest field anywhere.

9. Architectural guards (TODO entries; populated by later
   phases):
   - no PK-keyed delete path;
   - no `NodeCapability::*Tombstone*` / `*DeleteRows*`;
   - no new HRFT family;
   - no slab-footer `StateDigest` field;
   - no `format_version` bump;
   - no project-name prefix on new symbols;
   - no V1 / V2 suffix on new Rust types;
   - no `__row_tombstone_target__` column;
   - no `digest.table.row.physical.v1`,
     `digest.table.row.tombstone.v1`, or
     `digest.retention.gc.v1` domain helpers;
   - no `has_tombstones` short-name leftover (final symbol is
     `has_row_tombstone_records`; ADR 045 sketch shorthand
     does not survive the migration to source).

   ADR 047's new delete parent domain uses `table.delete-...`
   without a project-name prefix. The older ADR 045
   `table_write_parent_identity` tag still carries the legacy
   `wendelon.` prefix; changing it would alter parent identities
   and is tracked as ADR 045 protocol cleanup, not part of this
   row-tombstone landing.

Fixtures under `tests/fixtures/row_tombstones/`:

- One append-only table with three rows; delete one row;
  as-of before delete sees all three, as-of at / after delete
  sees two.
- Delete request with duplicate row IDs → one tombstone
  emitted.
- Retry after idempotency-cache loss → no second tombstone
  (target resolution sees latest = tombstone).
- Delete of missing row → `not_found_count = 1`, no
  tombstone.
- Delete of already-tombstoned row →
  `already_deleted_count = 1`, no second tombstone.
- Tombstone-only multi-row record fixture (synthetic columns
  only, no user columns).
- Slab with no tombstone rows reports
  `has_row_tombstone_records = false`; slab with tombstone
  rows reports `true`.
- Digest cancellation: `add_element(K) +
  subtract_element(K) == ZERO` over the same `K`.
- Compaction pair GC: visible digest before GC == visible
  digest after GC (over surviving rows).

Exit:

- Contract text and fixtures exist; tests fail with
  `unimplemented!()` until later phases.
- Architecture guards in place.

## Phase 1 — Schema constants, decoders, synthetic-column handling

Files:

- `src/schema/table.rs`
- `src/storage/multirow.rs`
- `src/storage/core.rs`
- `src/storage/columnar.rs`

Work:

1. Add `STORAGE_ROW_TOMBSTONE_COLUMN`. Wire into the
   existing append-only synthetic-column projection that
   ADR 045 established.

2. Extend multi-row synthetic detection to recognize two
   record shapes:
   - **Mixed / data records:** user columns + four ADR 045
     synthetic columns + `__row_tombstone__`.
   - **Tombstone-only records:** four ADR 045 synthetic
     columns + `__row_tombstone__` only. No user columns.

3. Extend `StorageRowMetadata` and `MultiRowSourceRow` with
   `is_tombstone: bool`.

4. Update `decode_multirow_synthetic_only` and
   `storage_rows_from_batch`:
   - tombstone rows are decoded as storage metadata;
   - they are never returned as user rows;
   - tombstone-only records produce an empty user batch (or
     a "no user rows" caller-visible result).

5. Update `strip_storage_synthetic_columns` to handle the new
   column. User-facing decode never surfaces
   `__row_tombstone__`.

6. Update ADR 044 columnar metadata probing so
   `__row_tombstone__` is recognized as a storage column,
   not an unknown user field.

7. Reject user-declared `__row_tombstone__` in descriptor
   validation.

8. Add footer / summary plumbing for
   `has_row_tombstone_records` using the Phase 0 allocated
   HMAC-covered flag bit. Readers use it only to skip
   tombstone-marker decoding; visibility authority stays with
   `tombstone_metadata` and indexed latest-as-of chains.

Tests:

- Append-only descriptor rejects user column named
  `__row_tombstone__`.
- Mixed records decode with `is_tombstone = false` per row.
- Tombstone-only records decode with `is_tombstone = true`
  and zero user rows.
- User-facing decode never returns the synthetic column.
- ADR 044 metadata probe recognizes the column.
- Footer summary round-trips
  `has_row_tombstone_records`.
- Architecture guard: no public API exposes the storage-only
  column as user-visible.

Exit:

- Storage decoders represent tombstone rows. Query layer
  doesn't use them yet.

## Phase 2 — `is_tombstone` on index entries

Files:

- `src/storage/index.rs`
- `src/storage/recovery.rs` (or wherever `RecoveredSlabEntry`
  lives)

Work:

1. Extend `MultiRowIndexEntry`, `RecoveredSlabEntry`, and
   `PersistentRecordRow` with `is_tombstone: bool`. Pack it
   alongside existing single-bit fields where layout allows.

2. Update the persistent local-index encoding to carry the
   bit. ADR 047 adds the V3 `is_tombstone` bit to the
   persistent-index row value. The tolerant V1/V2 record-row
   decoder is pre-existing ADR 042 index-rebuild compatibility;
   this ADR does not add a new tombstone-specific compat shim.

3. Index lookup helpers gain a
   `latest_at_or_before(hlc) -> (RecordLocation, bool)`
   accessor that returns `(record_location, is_tombstone)`.

Tests:

- Persistent index round-trips the bit.
- `latest_at_or_before(hlc)` returns the correct tombstone
  classification across mixed insert / delete sequences.

Exit:

- Index machinery can classify visibility without re-reading
  record bodies.

## Phase 3 — `DeleteRows` request admission

Files:

- `src/network/frames.rs`
- `src/network/data_server.rs`
- `src/network/traits.rs`
- `src/schema/table.rs`
- `src/schema/core.rs`

Work:

1. Allocate the MsgType pair and add request / response
   metadata structs (frozen in Phase 0).

2. Add `DataRing::table_delete` trait method.

3. Implement `WendelonDataServer::handle_table_delete`:
   - reject non-leaders with `NOT_LEADER` before target
     resolution (V1 has no follower proxy fields);
   - validate `tenant_id`, `table_object_id`,
     `schema_generation`;
   - reject if descriptor is not `AppendOnly`;
   - validate payload column shape and row count;
   - dedupe `row_key_ids` within the request;
   - compute `delete_hlc = hlc_driver.merge(client_hlc)`;
   - consult the in-process table-delete count replay cache;
     durable duplicate prevention still comes from latest-state
     classification.

Tests:

- Valid `DeleteRows` decodes and validates.
- Empty / malformed payload rejects.
- PK-keyed descriptor rejects.
- Schema-generation mismatch rejects.
- Duplicate keys deduped before target resolution.
- Idempotency-cache hit returns the cached response.

Exit:

- Server validates delete requests and computes
  `delete_hlc`. No tombstone rows emitted yet.

## Phase 4 — Target resolution and tombstone packing

Files:

- `src/storage/manager.rs`
- `src/storage/multirow.rs`
- `src/network/data_server.rs`

Work:

1. Add resolver:
   ```rust
   pub fn resolve_append_only_delete_targets(
       &self,
       tenant_id: TenantId,
       table_object_id: TableObjectId,
       row_key_ids: &[Identity],
       delete_hlc: HybridTimestamp,
   ) -> DeleteTargetResolution;
   ```
   For each row key, classify by `latest_at_or_before(hlc)`:
   - no location → `not_found`;
   - latest is data → `target_for_deletion`;
   - latest is tombstone → `already_deleted`.

   The resolver must use the target row's stored routing locality
   for data targets. If that locality is unavailable, fail closed
   with `PBC_REJECT_LOCALITY_UNSUPPORTED`; append-only table-
   spread rows must not fall back to identity-derived locality.

2. Construct tombstone source rows:
   ```rust
   MultiRowSourceRow {
       key_identity: target.row_key_id,         // shared with data
       routing_locality: target.routing_locality, // inherited
       row_hlc: delete_hlc,
       table_object_id: Some(table_object_id),
       is_tombstone: true,
       user_columns: None,                       // tombstone-only
   }
   ```
   Do not add `__row_tombstone_target__`; the target
   identity is the row's shared `__row_key_id__`.

3. Tombstone source rows flow through the existing append-
   only writer machinery. Group by inherited locality;
   replicate via Stellarator / Raft per ADR 045 §4a; durable
   quorum as for table writes.

4. Pack tombstone-only multi-row records
   (`encode_tombstone_multirow_records` or extension of
   `pack_admitted_record_batch_by_bucket`). Sort key
   matches existing append-only sort:
   `(__row_routing_locality__, __row_key_id__, __row_hlc__,
   original_ordinal)`.

5. Sealing a slab containing any tombstone row sets the
   `has_row_tombstone_records` footer bit.

6. If target resolution yields no targets, return success
   with counts and no data-branch append.

Tests:

- Delete of a visible row emits one tombstone with shared
  `__row_key_id__` and inherited locality.
- Tombstone `__row_hlc__ == delete_hlc`.
- Tombstone-only record sort is deterministic.
- Multi-locality delete produces multiple replica-group
  branches.
- Missing / already-deleted rows emit no tombstone.
- Sealing a slab with tombstones sets
  `has_row_tombstone_records`.
- Sealing a slab without tombstones leaves the bit clear.
- Locality byte-for-byte client preview vs server
  admission per ADR 045.

Exit:

- Tombstones reach durable storage. Reads still see all
  rows.

## Phase 5 — Visibility filter (scan, PBC, AGG, ADR 044 fetch)

Files:

- `src/storage/manager.rs`
- `src/storage/columnar.rs`
- `src/storage/aggregate.rs`
- `src/schema/pbc.rs`
- `src/network/data_server.rs`

Work:

1. Update row iteration to consult
   `latest_at_or_before(as_of_hlc)`:
   - latest is data → row visible;
   - latest is tombstone → row hidden;
   - no location ≤ as-of → not visible (yet).
   Tombstone rows themselves are never returned as user
   output.

2. Update ADR 044 column-projected fetch. The fast path
   skips `__row_tombstone__` only when **both** signals
   agree (ADR 047 §8 fast-path rule):
   ```text
   has_row_tombstone_records == false
   AND tombstone_metadata has no tombstone <= as_of_hlc
   ```
   Either signal alone is insufficient — a no-marker slab
   can still have its data rows tombstoned by a later slab;
   a marker-bearing slab may have all its targets in other
   slabs.

   When either signal demands it, include the visibility-
   relevant synthetic columns in the fetch plan:
   ```text
   __row_key_id__
   __row_hlc__
   __row_table_object_id__
   __row_tombstone__
   ```
   (`__row_routing_locality__` only if the existing path
   needs it.)

3. PBC filter / project: receives only visible user rows.

4. AGG: folds only visible user rows. Tombstone-only
   records contribute zero. CSB short-circuits drop slabs
   whose `tombstone_metadata` indicates a tombstone at or
   before `as_of_hlc` (Phase 6 wires `tombstone_metadata`).

5. Fail-closed path when a record is missing the synthetic
   columns required for visibility classification.

Tests:

- As-of before delete returns the row.
- As-of at / after delete hides the row.
- Whole-record scan and projected scan return identical
  visible row sets.
- PBC ignores tombstones.
- AGG `count_star`, `min`, `max` ignore tombstoned rows.
- Cross-slab: data row in slab1@T1, tombstone in slab2@T2.
  Scan AS OF T1 sees row. Scan AS OF T2 hides row.
- Steady-state slab (`has_row_tombstone_records = false`,
  no relevant `tombstone_metadata`): tombstone columns not
  fetched (assert column-fetch call shape).

Exit:

- Deletes are visible through all row-returning paths.

## Phase 6 — CSB metadata and aggregate direct-answer safety

Files:

- `src/storage/manager.rs`
- `src/storage/aggregate.rs`
- `src/storage/index.rs`
- `src/network/data_server.rs`

Work:

1. Extend ADR 040's `tombstone_metadata` documentation:
   "earliest visibility-deleting HLC affecting the slab —
   migrated range tombstones or row tombstones, same proof
   shape."

2. On row-tombstone commit, update every target data
   slab's `tombstone_metadata`:
   ```rust
   record_tombstone_hlc(data_slab_id, delete_hlc)
   ```
   - L1: local manager state.
   - L2: CAS-update existing
     `TieredSlabPointer.tombstone_metadata`. **No new
     pointer field.**

3. Tombstone-only records do not produce user-column CSBs.

4. `can_answer_from_csb` predicate stays the same shape
   (ADR 047 §7). A row-tombstone at or before `as_of_hlc`
   falls back to row-level filtering exactly as a range
   tombstone does.

Tests:

- CSB direct answer works before delete HLC.
- CSB falls back at and after delete HLC.
- L2 pointer `tombstone_metadata` updates on row delete.
- Tombstone-only records produce no CSB.
- Existing migrated-range tombstone tests still pass.

Exit:

- Aggregate direct answers remain sound under row
  tombstones.

## Phase 7 — Compaction GC, retention-floor reclaim, recovery

Files:

- `src/storage/manager.rs`
- `src/storage/sweeper.rs`
- `src/storage/core.rs`
- `src/storage/manager.rs` unit tests for L1/L2 retention pair
  reclaim and tiered tombstone metadata
- `src/storage/core.rs` unit tests for recovery duplicate
  fail-closed behavior

Work:

1. **Compaction window pair GC:**
   - Scan input for tombstone rows; build pair list
     `(target_row_key_id, tombstone_record_location)`.
   - For each pair, look up the data row by shared
     `__row_key_id__`.
   - In-window pair → eligible for joint drop **only when**
     `retention_floor > delete_hlc` (or an equivalent
     history-window proof exists). Otherwise the data row
     must be retained (regrouped into the output) so that
     `AS OF T < delete_hlc` reads still see it; the
     tombstone is also retained alongside it. Joint drop
     is the retention-floor case (step 2 below) — pair-GC
     in compaction without the floor check is unsafe even
     though the digest algebra is invariant.
   - Out-of-window tombstone (data row in older slab) →
     retain. Drop only when retention or a partition-scope
     scan proves the data row reclaimed.
   - Out-of-window data row (tombstone in newer slab) →
     retain. Rebuilt index plus external
     `tombstone_metadata` continues to hide the row from
     visibility-filtering reads; physical removal would
     break time-travel for `as_of_hlc < delete_hlc`.
   - Recompute output `has_row_tombstone_records` bit and
     `tombstone_metadata`.

2. **Retention-floor pair reclaim** (existing MVCC floor
   only — per-table retention is roadmap #3):
   - `delete_hlc < retention_floor` and target data row in
     window: drop both.
   - Data HLC < floor but `delete_hlc >= floor`: keep both
     (window between floor and tombstone HLC must still see
     pre-delete row).
   - Orphaned tombstones (data already retention-evicted)
     may be dropped at compaction discretion. Retention ADR
     (#3) tightens the rule.

3. **Recovery:**
   - Decode tombstone rows from multi-row records.
   - Rebuild `(tenant_id, key_identity) ->
     Vec<RecordLocation>` chains with the `is_tombstone`
     bit.
   - Recompute per-slab `tombstone_metadata` from
     survivors.
   - Multiple tombstones for one row: fail closed. Optional
     "keep earliest, emit corruption metric" repair belongs to a
     future explicit repair mode.

4. Compaction does **not** consult or update digest values.
   The visible-digest invariant under pair GC is algebraic:
   the pair's `+visible_row_element` and
   `-visible_row_element` already cancelled.

5. Recompute CSB stats from survivors (existing compaction
   path).

Tests:

Implemented in `src/storage/manager.rs` and `src/storage/core.rs`
rather than a new integration test file, because the relevant APIs
are internal compaction / recovery helpers.

- In-window pair, retention_floor <= delete_hlc:
  compaction retains both data and tombstone in the output
  (regrouped if needed). `has_row_tombstone_records = true`.
  Time-travel `AS OF T < delete_hlc` still sees the data row.
- In-window pair, retention_floor > delete_hlc: compaction
  drops both. `has_row_tombstone_records = false` if no
  other tombstones survive.
- Cross-window: tombstone retained.
  `has_row_tombstone_records = true`.
- Mixed: in-window pairs handled by retention-floor rule;
  orphans survive per their out-of-window rules.
- Retention floor crosses delete HLC: pair dropped
  together.
- Data below floor, tombstone above floor: both retained.
- Visible digest invariant: digest before == digest after
  pair GC (over surviving rows).
- Recovery preserves pre-delete and post-delete
  visibility.
- Recovery fail-closed on duplicate tombstones.
- Recovery rebuilds `tombstone_metadata` correctly.
- L2 retention rewrite handles partial multi-row records
  containing tombstone rows.

Exit:

- Tombstones survive recovery and compaction with correct
  time-travel; old pairs reclaim correctly.

## Phase 8 — `StateDigest` integration: visible-domain only

Files:

- `src/hash/state_digest.rs` (additive only — no new domain
  helpers)
- `src/storage/multirow.rs` (seal-time digest accumulation)
- `tests/state_digest_invariants.rs` (extend)

Work:

1. **No new domain constants.** Use the existing
   `DOMAIN_TABLE_ROW_VISIBLE` from ADR 046.

2. **No new element-projection helpers.** Use the existing
   `visible_row_element(t, T, k)` from ADR 046.

3. (Optional ergonomic helper, only if call sites get
   noisy.) Internal seal-time wrapper:
   ```rust
   pub(crate) fn apply_row_to_visible_digest(
       acc: &mut StateDigestAccumulator,
       tenant_id: TenantId,
       table_object_id: TableObjectId,
       row_key_id: Identity,
       is_tombstone: bool,
   ) {
       let element = visible_row_element(tenant_id, table_object_id, row_key_id);
       if is_tombstone {
           acc.subtract_element(element);
       } else {
           acc.add_element(element);
       }
   }
   ```
   Internal-only; call sites can also write the explicit
   if / else.

4. Seal-time visible-digest computation: at slab seal, walk
   the multi-row source rows and apply add / subtract per
   `is_tombstone`. The resulting `StateDigest` is **not**
   persisted.

5. **In-memory cache rule.** The seal-time digest **may** be
   cached in memory (per-slab, per-partition, or per-
   replica) as a recomputation-avoidance optimization. The
   cache **must not** be persisted to: the slab footer,
   `TieredSlabPointer`, the catalog, the local index, the
   durability log, or any wire frame. If a future workload
   pulls on durable cross-replica comparison, that pull
   belongs to the anti-entropy ADR. Architecture test
   enforces (Phase 10).

6. **Architectural test (substrate isolation, inherited
   from ADR 046):** curve math (`use curve25519_dalek`)
   does not appear in `src/network` or `src/storage`. The
   seal-time digest call goes through `src/hash/...`.

Tests (in `tests/state_digest_invariants.rs`):

- Add then subtract the same `visible_row_element` →
  `StateDigest::ZERO`.
- Three rows + one tombstone → digest equals two-row
  digest.
- Order independence: tombstones interleaved with appends
  in any order produce the same final visible digest.
- Compaction pair GC equivalence: pack a `(data, tombstone)`
  pair, take visible digest. Drop the pair (simulate GC).
  Take digest of the empty survivor set (= `ZERO`). Assert
  equal.
- Multi-slab equivalence:
  digest(slab1 ∪ slab2) ==
  merge_digest(slab1.digest, slab2.digest).
- Real-multirow smoke: pack rows + tombstones via the
  actual writer machinery, decode storage metadata,
  compute digest from `(row_key_id, is_tombstone)` only,
  match expected.
- Re-tombstone over-subtract guard: assert that the
  admission path's "latest is tombstone → no-op" rule is
  enforced; bypassing it would over-subtract (regression
  guard, mocked admission).

Exit:

- ADR 047 validates ADR 046 `subtract_element` semantics
  end-to-end. No durable digest placement introduced.

## Phase 9 — Bench: no-delete fast path, tombstone density, digest cost

Files:

- `benches/row_tombstones.rs` (new)
- `Cargo.toml` (`[[bench]]` entry)
- `plans/impl/047_row_tombstone_bench_results.md` (new,
  populated when the bench runs)

Current code-review coverage is a quick smoke bench. The matrix
below remains the promotion-to-shipped gate; the quick run alone
does not satisfy the quantitative thresholds.

Work:

1. **No-delete fast path.** Append-only table, zero
   tombstones, full scan. Measure scan latency with
   `has_row_tombstone_records = false` and
   `tombstone_metadata` clean. Verify the column-fetch path
   skips `__row_tombstone__` entirely. Compare to the
   ADR 045 baseline append-only scan envelope; the cost
   delta should be at most one footer-bit read +
   `tombstone_metadata` predicate per slab.

2. **Tombstone density: 1% and 10% over 100k rows.**
   - Single-slab: build the slab with mixed data and
     tombstone rows. Measure scan latency, projected-fetch
     latency, AGG `count_star` latency, PBC filter
     throughput.
   - Cross-slab: data rows in older slabs, tombstones in
     newer slabs. Measure `tombstone_metadata` fallback
     overhead and row-index latest-as-of classification
     cost.

3. **Visible-domain digest cost.** Extend the ADR 046 bench
   shape with 1% and 10% tombstone density at 100k and 1M
   rows. Confirm:
   - Total cold-path digest cost ≤ ADR 046 visible-only
     baseline + N_tombstone × per-element subtract cost.
   - The seal-time digest accumulation does not exceed the
     ADR 046 single-domain envelope.

4. **`DeleteRows` admission throughput.** Sequential and
   batched `DeleteRows` calls with target-resolution +
   tombstone packing. Measure latency from request entry to
   durable seal. Compare to `Append` baseline at the same
   row count.

5. **Compaction pair-GC throughput.** Single-coherent
   compaction over a window with mixed data + tombstones.
   Measure pair-detection cost, output-slab production
   throughput, and `tombstone_metadata` recomputation.

Acceptance criteria before promotion:

- No-delete fast path within 5% of the ADR 045 append-only
  scan baseline.
- 1% tombstone density: scan latency within 10% of no-
  delete baseline.
- 10% tombstone density: scan latency within 50% of no-
  delete baseline (degradation expected; quantify it).
- Per-tombstone digest subtract cost within 2× per-row add
  cost (substrate-symmetric).
- Compaction pair-GC: no more than 1.5× the cost of a no-
  tombstone compaction window of the same byte size.

Bench results documented in
`plans/impl/047_row_tombstone_bench_results.md`. Quick smoke
results may be recorded for code review, but they are explicitly
not evidence for the promotion thresholds above. If full-matrix
acceptance criteria fail, council pauses promotion to "shipped"
and revisits scan-path or admission shape.

Exit:

- Quick smoke bench is runnable in CI / local review.
- Full matrix bench results are documented before ADR 047 is
  declared shipped.
- ADR 047 promotion-to-shipped gate satisfied or council
  notified.

## Phase 10 — Metrics, audit, runbook, architectural guards

Files:

- `src/metrics/registry.rs`
- `src/audit.rs`
- `tests/metrics_smoke.rs`
- `tests/architecture_test.rs`
- `plans/ops/runbooks/row_tombstones.md` (new)
- `plans/ops/observability.md`

Metrics:

```text
storage_row_tombstone_delete_requests_total{tenant}
storage_row_tombstone_rows_deleted_total{tenant}
storage_row_tombstone_rows_already_deleted_total{tenant}
storage_row_tombstone_rows_not_found_total{tenant}
storage_row_tombstone_records_written_total{tenant}
storage_row_tombstone_compaction_pairs_dropped_total{tenant}
storage_row_tombstone_retention_pairs_dropped_total{tenant}
agg_csb_short_circuit_skipped_row_tombstone_total
storage_row_tombstone_presence_bit_slabs_total{tenant}
```

Audit:

- One audit event per `DeleteRows` request: tenant, table,
  request row count, deleted / already-deleted / not-found
  counts, `delete_hlc`.
- Row-key lists not logged at default level (volume).

Runbook:

- What `DeleteRows` does and does not do.
- Time-travel behavior around `delete_hlc`.
- Why deleted-row payload bytes remain until retention.
- CSB direct-answer fallback after row deletes.
- The difference between `has_row_tombstone_records`
  (presence flag for marker-row decoding) and
  `tombstone_metadata` (visibility-authority earliest-HLC
  bound). Both are required for the no-delete fast path.
- Idempotency posture (re-deletes are no-ops).
- Recovery before retention floor (`AS OF`).
- Metrics interpretation.

Architectural guards (extend `tests/architecture_test.rs`):

- No row-delete path on `RowIdentityPolicy::PrimaryKey`.
- No `NodeCapability::*Tombstone*` / `*DeleteRows*`.
- No new HRFT family for row tombstones.
- No slab-footer `StateDigest` field, and no durable row-
  tombstone digest / state field beyond the approved
  `has_row_tombstone_records` presence bit (a single
  HMAC-covered flag bit, not a digest).
- No `format_version` bump on slab envelopes.
- No project-name prefix on new symbols.
- No V1 / V2 suffix on new Rust types.
- `__row_tombstone__` referenced through the named const
  only.
- `HAS_ROW_TOMBSTONE_RECORDS_BIT` referenced through the
  named const only — no magic-number bit positions or
  footer offsets.
- The presence bit is **never** treated as visibility
  authority — visibility decisions go through
  `tombstone_metadata` + index `is_tombstone` lookup.
- `__row_tombstone__` references confined to schema /
  storage / query files that own storage synthetic columns,
  plus tests and docs.
- User-facing decode paths never expose `__row_tombstone__`.
- Curve math (`use curve25519_dalek`) does not appear in
  `src/network` or `src/storage` (substrate isolation
  inherited from ADR 046).
- `StateDigest` symbol does not appear on storage / network
  / control-ring struct definitions (no durable
  placement). An in-memory cache that holds a `StateDigest`
  by value inside a non-persisted struct (e.g., a per-slab
  scratch summary) is allowed; the guard is on durable /
  serialized fields, not on transient memory.
- **Rejected-alternative surface guards:**
  - No `__row_tombstone_target__` column (rejected unique-
    identity model).
  - No `digest.table.row.physical.v1` byte tag in source
    (rejected monotone-domain model).
  - No `digest.table.row.tombstone.v1` byte tag in source
    (event-history domain reserved by ADR 046 §4 but not
    used by this ADR).
  - No `digest.retention.gc.v1` byte tag in source
    (retention ADR owns its own preimage).
  - No `physical_row_element` / `tombstone_event_element` /
    `derive_visible_digest` helper definitions (rejected
    iter-1 helper shape).
  - No `has_tombstones` short-name leftover (final symbol
    is `has_row_tombstone_records`).

Exit:

- Operators can distinguish successful deletes, idempotent
  no-ops, missing keys, and reclaim activity.

## Verification gate

Required before declaring ADR 047 shipped:

```sh
cargo test --lib state_digest
cargo test --test state_digest_invariants
cargo test --test architecture_test --quiet
cargo test row_tombstone --quiet
cargo test storage::manager --quiet
cargo test storage::core --quiet
cargo test --test metrics_smoke
cargo bench --bench row_tombstones --no-run
cargo bench --bench row_tombstones -- --quick
git diff --check
rustfmt --check --edition 2024
```

Grep guards:

- Domain-tag well-formedness (extends ADR 046):
  ```sh
  rg -n 'b"digest\.[a-z_.]+\.v[0-9]+"' src tests benches | sort -u
  ```
  Output equals the ADR 046 set; no new domains added.

- Substrate isolation (inherited from ADR 046):
  ```sh
  rg -n 'use\s+curve25519_dalek' src/network src/storage
  ```
  Empty.

- No durable digest placement:
  ```sh
  rg -n 'StateDigest\b' src/network src/control_ring
  ```
  Empty (network and control-ring carry no durable digest
  surface). A `src/storage` hit is allowed only when
  scoped to non-persisted seal-time computation or an in-
  memory cache (no footer / pointer / catalog / wire
  serialization).

- No new HRFT family for tombstones:
  ```sh
  rg -n 'NodeCapability::.*(Tombstone|DeleteRows)|TombstoneCapability|DeleteRowsCapability' src tests
  ```
  Empty.

- Synthetic-column name through the const:
  ```sh
  rg -n '"__row_tombstone__"' src
  ```
  Hits go through `STORAGE_ROW_TOMBSTONE_COLUMN`.

- Footer-bit through the named const:
  ```sh
  rg -n 'has_row_tombstone_records|HAS_ROW_TOMBSTONE_RECORDS_BIT' src
  ```
  Hits go through `HAS_ROW_TOMBSTONE_RECORDS_BIT` or named
  footer-summary accessors — no literal offsets or magic-
  number masks.

- No `has_tombstones` short-name leftover:
  ```sh
  rg -n '\bhas_tombstones\b|\bHAS_TOMBSTONES_BIT\b' src tests
  ```
  Empty. `has_tombstones` is roadmap shorthand only; the
  final symbol is `has_row_tombstone_records`.

- No `format_version` bump on the ADR 047 diff scope:
  ```sh
  rg -n 'format_version\s*[=:]\s*[3-9]' src
  ```
  Empty.

- No version suffixes on ADR 047 Rust types:
  ```sh
  rg -n 'TombstoneV[0-9]|DeleteRowsV[0-9]|TableDeleteRows.*V[0-9]' src tests
  ```
  Empty.

- No project-name prefix:
  ```sh
  rg -n 'wendelo[n]_.*tombstone|Wendelo[n].*Tombstone' src tests benches plans
  ```
  Empty.

- **Rejected-alternative surface guards:**
  ```sh
  rg -n '__row_tombstone_target__' src tests
  rg -n 'digest\.table\.row\.physical\.v1' src tests
  rg -n 'digest\.table\.row\.tombstone\.v1' src tests
  rg -n 'digest\.retention\.gc\.v1' src tests
  rg -n 'physical_row_element|tombstone_event_element|derive_visible_digest' src tests
  ```
  All five empty. These guards prevent silent re-
  introduction of the unique-identity / monotone-three-
  domain / retention-domain shapes the council rejected.

Convention guards:

- No `#[serde(default)]` shim.
- No reserved enum slots.
- No envelope `format_version` bump.
- No new HRFT frame family.
- No new node capability bit.
- No `ObjectStore` trait break.
- No on-disk shape change beyond the one footer bit.
- The synthetic column is append-only-only; PK-keyed
  descriptors do not gain it.

## Deferred to follow-up ADRs

Mirrors ADR 047 *Deferred* section: per-table retention,
anti-entropy gossip, CDC cursor, PK-keyed delete, content-
hash identity, secondary-index propagation, LSM tombstone
GC, durable `StateDigest` placement, event-history digest
domain, tombstone-density alarms.
