# Implementation Plan: ADR 055 — LSM-Style Leveled Compaction Substrate

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR:
`plans/adr/055_lsm_leveled_compaction_substrate.md`

Inherits write-path bifurcation:
`plans/adr/003_stellarator_bifurcated_writes.md` (the
substrate's release gate).
Inherits control-ring carrier:
`plans/adr/016_durability_model_fsync_group_commit.md`.
Inherits row identity surface:
`plans/adr/045_pk_less_append_only_tables.md`,
`plans/adr/049_content_hash_row_identity.md`,
`plans/adr/050_pk_delete_supersession.md`.
Inherits tombstone surface:
`plans/adr/047_row_tombstones_append_only.md`.
Inherits retention contract:
`plans/adr/048_per_table_retention_policy.md`.
Inherits digest substrate:
`plans/adr/046_state_digest_substrate.md`.
Inherits sidecar rebuild discipline:
`plans/adr/052_secondary_indexes.md` §7, §9, §10.
Inherits CDC reachability contract:
`plans/adr/054_cdc_subscription_substrate.md`.

## Goal

Land the LSM-style leveled compaction substrate.

The implementation lifts the existing single-slab
rewrite helpers into a per-locality leveled scheduler
that:

- one `CompactionLevelIndex` newtype-unit (`u8`, range `0..=3`);
- one `CompactionJobId`, one `ManifestEpoch` newtype-
  unit (`u64` each);
- one `CompactionMergeKey = (Hlc, RowKeyId,
  RecordRowIndex)`; storage-local; never imported by
  CDC schema;
- per-locality slab manifest extension carrying
  `(compaction_level, state, successors, hlc_range, byte_size,
  row_count, sidecar_coverage, manifest_epoch)`;
- `LsmCompactionPicker` (pure function) and
  `LsmCompactionWorker` per locality leader (one
  worker, concurrency = 1 V1);
- `SlabPinTable` with bulk-atomic acquire and RAII
  guards;
- streaming elision wrapper around the existing
  per-slab `tombstone_compaction` /
  `retention_compaction` /
  `secondary_index_compaction_context` primitives in
  `src/storage/manager.rs`;
- `LsmGcWorker` deletes retired-and-unpinned slabs;
- `MutationKind::CompactionCommit` extension on the
  locality control ring (ADR 016);
- bounded `lsm_*` metric family with pinned label
  enumerations;
- 3-code `LsmRejectCode` taxonomy
  (`ManifestRace`, `SidecarDigestMismatch`,
  `VisibleDigestInvariance`);
- audit lifecycle events
  (`LsmCompactionCommitted`,
  `LsmInvariantViolation`,
  `LsmCompactionPaused`);
- bench matrix that distinguishes write amplification
  from read amplification from space amplification
  from invariance audit cost from CDC catchup
  isolation.

## Deliverables

- `CompactionLevelIndex`, `CompactionJobId`, `ManifestEpoch`
  newtype-units in `src/schema/units.rs`.
- `CompactionMergeKey`, `SlabState`, `LsmRunSummary`,
  `CompactionPlan`, `CompactionCommit`,
  `CompactionDigestSummary`, `CompactionOutcome`,
  `LsmConfig`, `LsmRejectCode`, `DominantReason`,
  `DropReason` in `src/storage/lsm.rs` (new — Phase 0
  audits actual path).
- Per-locality slab manifest extension in
  `src/storage/manifest.rs` (or current owner): the
  fields above plus compaction-level-aware queries
  (`slabs_at_compaction_level`, `overlapping_slabs`,
  `oldest_l0_slab_age_millis`,
  `compaction_level_total_bytes`,
  `compaction_level_slab_count`, `manifest_snapshot`).
- `LsmCompactionPicker` (pure) in `src/storage/lsm.rs`.
- `LsmCompactionWorker`, `LsmGcWorker` in
  `src/storage/lsm_worker.rs` (new).
- `SlabPinTable` in `src/storage/slab_pins.rs` (new).
- Streaming elision wrapper in
  `src/storage/manager.rs` lifting per-slab elision
  primitives to N-input streams.
- `MutationKind::CompactionCommit` extension in
  `src/network/r0_control.rs` (or current owner) plus
  apply-path validation.
- `LsmCompactionWorker` integration with the existing
  tier sweeper in `src/storage/sweeper.rs`.
- `lsm_*` metric family in `src/metrics/registry.rs`
  with bounded label enumerations.
- Audit event kinds in `src/audit/events.rs`.
- Architectural guards in `tests/architecture_test.rs`
  (Phase 12).
- Golden vectors per phase below.
- Bench harness in `benches/lsm_compaction.rs`.
- Operator runbook
  `plans/ops/runbooks/lsm_compaction.md`.

## Non-goals

- No size-tiered compaction picker.
- No subcompaction.
- No tiered storage tier moves.
- No bloom filters per slab.
- No cross-replica compaction deduplication.
- No new LSM-introduced `relocated_reads` rows or
  LSM-specific relocation helper. (Existing non-LSM
  relocation machinery in `src/storage/manager.rs` for
  retention rewrites is untouched.)
- No CDC compaction pin / subscription-held compaction
  lease.
- No manual compaction trigger RPC.
- No compaction throttling / IO QoS.
- No new `MsgType` containing `LSM` or `COMPACTION`.
- No new `FRAME_FLAG_*`.
- No new `NodeCapability` bit.
- No new wire / storage compression codec.
- No `ObjectStore` trait change.
- No table descriptor field for compaction level /
  manifest.
- No persisted visible `StateDigest` field on slab
  footer or run manifest.
- No `DigestEvictionMarker` on the control ring.
- No `StorageTier` on the LSM substrate.
- No L4+ extension.
- No reserved enum slots, compatibility shims, or
  `V1` / `V2` Rust type suffixes.
- No project-name prefix on new symbols.
- No high-cardinality metric labels.
- No new Cargo feature or dependency.

## Phase 0 — Contract freeze and path audit

Lock the level enum, `CompactionMergeKey`, the slab-
manifest extension, the picker / worker / pin-table
shapes, the install-batch surface, the reject taxonomy,
the metric label enumerations, the architectural
guards, and the V1 defaults before invasive storage
edits begin.

Files:

- `src/schema/units.rs` (newtype-unit extension)
- `src/storage/lsm.rs` (new — POD types, planner shape)
- `src/storage/lsm_worker.rs` (new — worker / GC shape)
- `src/storage/slab_pins.rs` (new — pin table shape)
- `src/storage/manifest.rs` (extension; Phase 0 audits
  actual owner)
- `src/storage/manager.rs` (install signature;
  streaming wrapper signature)
- `src/network/r0_control.rs` (mutation shape
  extension; Phase 0 audits actual owner)
- `src/metrics/registry.rs` (new metric family)
- `tests/fixtures/lsm_compaction/` (new)
- `tests/architecture_test.rs`

Work:

1. Audit current owners for slab manifest metadata,
   control-ring mutation discriminants, metric
   registration, and existing compaction helper
   names. Update file paths in subsequent phases if
   actual modules differ.

2. Freeze newtype-units:

   ```rust
   #[repr(transparent)]
   pub struct CompactionLevelIndex(u8);

   impl CompactionLevelIndex {
       pub fn try_new(raw: u8) -> Option<Self> {
           if raw <= 3 { Some(Self(raw)) } else { None }
       }
       pub const L0: Self = Self(0);
       pub const L3: Self = Self(3);
       pub fn next(self) -> Option<Self> {
           Self::try_new(self.0 + 1)
       }
   }

   #[repr(transparent)]
   pub struct CompactionJobId(u64);

   #[repr(transparent)]
   pub struct ManifestEpoch(u64);
   ```

3. Freeze `SlabState` and `LsmRunSummary`:

   ```rust
   pub enum SlabState {
       Live,
       Retired { successors: Vec<SlabId> },
       DeletePending,
   }

   pub struct LsmRunSummary {
       pub slab_id: SlabId,
       pub compaction_level: CompactionLevelIndex,
       pub state: SlabState,
       pub min_row_hlc: Hlc,
       pub max_row_hlc: Hlc,
       pub size_bytes: ByteCount,
       pub row_count: RowCount,
   }
   ```

4. Freeze `CompactionMergeKey`:

   ```rust
   pub struct CompactionMergeKey {
       pub row_hlc: Hlc,
       pub row_key_id: RowKeyId,
       pub record_row_index: RecordRowIndex,
   }
   ```

   Storage-local. Architectural guard: not imported
   by any CDC module.

5. Freeze the **reject taxonomy**:

   ```rust
   #[repr(u32)]
   pub enum LsmRejectCode {
       ManifestRace            = 1,
       SidecarDigestMismatch   = 2,
       VisibleDigestInvariance = 3,
   }
   ```

6. Freeze `LsmConfig`:

   ```rust
   pub struct LsmConfig {
       pub max_l0_slab_count: u32,                       // 4
       pub lsm_l0_max_age_millis: u64,                   // 5_000
       pub lsm_level_count: u8,                          // 4
       pub lsm_size_ratio: u32,                          // 10
       pub lsm_l1_slab_size_target_bytes: u64,           // 64 << 20
       pub max_l1_slab_count: u32,                       // 4
       pub lsm_picker_tick_millis: u64,                  // 100
       pub lsm_gc_tick_millis: u64,                      // 1_000
       pub lsm_compaction_concurrency_per_locality: u32, // 1
       pub lsm_digest_verify_sample_rate: f32,           // 1.0 / 0.01
   }
   ```

7. Freeze the **planner shape** as a pure function:

   ```rust
   pub fn pick_compaction(
       manifest: &ManifestSnapshot,
       config: &LsmConfig,
       now: Hlc,
   ) -> Option<CompactionPlan>;

   pub struct CompactionPlan {
       pub job_id: CompactionJobId,
       pub source_level: CompactionLevelIndex,
       pub target_level: CompactionLevelIndex,
       pub manifest_epoch_pre: ManifestEpoch,
       pub input_slabs: Vec<SlabId>,
       pub overlapping_slabs: Vec<SlabId>,
       pub retention_floor_snapshot: Hlc,
       pub compaction_start_hlc: Hlc,
       pub dominant_reason: DominantReason,
   }

   // Audit / scoring vocabulary; not a picker rule.
   pub enum DominantReason {
       LevelSize,
       RetentionDebt,
       TombstoneDebt,
       IndexCoverage,
       LayoutConvergence,
       Operator,
   }
   ```

8. Freeze the **worker shape**:

   ```rust
   pub struct LsmCompactionWorker { ... }

   impl LsmCompactionWorker {
       pub async fn run_one(
           &self,
           plan: CompactionPlan,
       ) -> Result<CompactionOutcome, CompactionFailure>;
   }

   pub struct CompactionOutcome {
       pub job_id: CompactionJobId,
       pub manifest_epoch_pre: ManifestEpoch,
       pub manifest_epoch_post: ManifestEpoch,
       pub output_slabs: Vec<LsmRunSummary>,
       pub bytes_in: u64,
       pub bytes_out: u64,
       pub rows_in: u64,
       pub rows_out: u64,
       pub elided_versions: u64,
   }

   pub enum CompactionFailure {
       ManifestRace,
       SidecarDigestMismatch,
       VisibleDigestInvariance,
       BackfillRaceLost,
       IoError(std::io::Error),
   }
   ```

9. Freeze the **`CompactionCommit` mutation shape**:

   ```rust
   pub struct CompactionCommit {
       pub job_id: CompactionJobId,
       pub manifest_epoch_pre: ManifestEpoch,
       pub slabs_to_retire: Vec<SlabId>,
       pub slabs_to_add: Vec<LsmRunSummary>,
       pub slabs_to_mark_delete_pending: Vec<SlabId>,
       pub slabs_to_remove: Vec<SlabId>,
       pub sidecar_swaps: Vec<SidecarSwap>,
       pub digest_summary: CompactionDigestSummary,
   }

   pub struct CompactionDigestSummary {
       pub visible_pre: StateDigest,
       pub visible_post: StateDigest,
       pub version_entry_delta: StateDigest,
   }
   ```

   Regular compaction commits populate
   `slabs_to_retire` / `slabs_to_add`; GC lifecycle
   commits populate exactly one of
   `slabs_to_mark_delete_pending` or
   `slabs_to_remove`. Mixed shapes are rejected.

   `MutationKind::CompactionCommit(CompactionCommit)`
   added to the existing locality control-ring
   mutation carrier. Phase 0 audits the existing
   allocation table.

10. Freeze the **in-memory elision helper signature**:

    ```rust
    pub fn apply_elision<I>(
        records: I,
        retention_floor: Hlc,
        lifecycle: &LifecycleSnapshot,
        table_schema: &TableSchemaSnapshot,
    ) -> Vec<SurvivorOrDrop>
    where I: Iterator<Item = VersionEntry>;

    pub enum SurvivorOrDrop {
        Survivor(VersionEntry),
        Drop { record: VersionEntry, reason: DropReason },
    }

    pub enum DropReason {
        Retention,
        TombstonePair,
        PkPrefix,
        MigratedRange,
        EmptyRun,
    }
    ```

    Wraps existing per-slab `tombstone_compaction` /
    `retention_compaction` primitives.

11. Freeze the **pin-table shape**:

    ```rust
    pub struct SlabPinTable { ... }

    impl SlabPinTable {
        pub fn acquire(&self, slabs: &[SlabId])
            -> Result<SlabPinGuard, AcquireError>;
    }

    pub struct SlabPinGuard { ... }  // RAII

    pub enum AcquireError {
        SlabAlreadyDeletePending,
    }
    ```

    Bulk-atomic acquire. `DeletePending` slab fails
    acquire; caller retries against fresh manifest
    snapshot.

12. Freeze metric shapes per ADR §9:

    ```text
    lsm_compactions_total{level,result}
    lsm_compaction_input_slabs{level}
    lsm_compaction_output_bytes{level}
    lsm_compaction_duration_millis{level}
    lsm_level_slab_count{level}
    lsm_level_total_bytes{level}
    lsm_pending_delete_slabs{}
    lsm_retired_slab_pinned_bytes{}
    lsm_digest_invariance_violations_total{kind}
    ```

    Pinned label enumerations:

    ```text
    level  = 0 | 1 | 2 | 3 for level gauges
    level  = 0 | 1 | 2 for compaction source-level metrics
    result = ok | manifest_race | sidecar_digest_mismatch
           | visible_digest_invariance | aborted
    kind   = visible_domain | version_entry
    ```

13. Freeze fixtures: pre-built input slabs for each
    golden vector (overlapping-HLC L0 set,
    supersession chain, tombstone pair, retention
    boundary, uncovered slab).

14. Architectural test stubs (full guards in Phase 12):

    - exactly one `CompactionLevelIndex` definition;
    - exactly one `CompactionJobId`, `ManifestEpoch`
      definition;
    - `CompactionLevelIndex` accepts only `0..=3`;
    - exactly one `CompactionMergeKey` definition;
    - `CompactionMergeKey` not imported by any CDC
      module;
    - no `Lsm*V[0-9]` / `Compaction*V[0-9]` type
      names;
    - no `Wendelon`-prefixed LSM symbols;
    - no `MsgType` constants attributable to this
      ADR;
    - no new `FRAME_FLAG_*`;
    - no new `NodeCapability` bit;
    - sidecar compaction path writes new files (no
      in-place rewrite helper exists);
    - no compaction pin held by CDC subscription code
      path;
    - `LsmCompactionWorker` reads retention floor only
      from `TableLifecycleCatalog`;
    - `LsmConfig` defaults match Phase 0 §6 verbatim;
    - no LSM code path writes into `relocated_reads`
      and no LSM type embeds a relocation table —
      scoped to LSM only; existing non-LSM relocation
      machinery in `src/storage/manager.rs` remains
      untouched;
    - no `StorageTier` field on LSM run summaries;
    - no `DigestEvictionMarker` on the control ring.

15. **Cached-reference audit (load-bearing for the
    no-LSM-relocation stance).** Enumerate every site
    in the existing codebase that caches a
    `RecordLocation` and dereferences it outside an
    iterator's lifetime. The known starting set:

    - `src/storage/manager.rs` `relocated_reads`
      `DashMap<RecordLocation, RecordLocation>` and
      its dereference path;
    - secondary-index sidecar entries (ADR 052) —
      retire with their slabs at compaction;
    - in-flight RPC response streams holding
      `RecordLocation` chunks;
    - query plans / planner caches that materialize
      `RecordLocation` ahead of dereference;
    - any catalog or admission-side code holding a
      `RecordLocation` for downstream lookup.

    For each site, classify:
    - **(P) protected by manifest-snapshot pin**: the
      lookup happens within the iterator that
      acquired the pin → safe under ADR 055 §7;
    - **(R) covered by existing non-LSM relocation
      machinery**: the lookup goes through the
      existing `relocated_reads` redirect → safe and
      untouched by ADR 055;
    - **(S) covered by sidecar replace-by-new**:
      the cache is in a sidecar that retires with
      its source slab during compaction → safe under
      ADR 052 §9;
    - **(U) uncovered**: lookup happens outside any
      pin, outside existing relocation, and not
      tied to a sidecar retirement.

    Any **(U)** site found is a Phase 0 blocker —
    LSM either extends pin acquisition to cover it
    (Phase 3 work item) or the no-LSM-relocation
    stance does not hold and the ADR must be
    revisited. The audit lands as a written report
    in `plans/impl/055_cached_reference_audit.md`
    before Phase 1 begins.

Exit:

- Types and guard intent pinned; fixtures ready.
- Cached-reference audit report committed; zero
  unclassified or **(U)** sites.

## Phase 1 — Manifest level-awareness and snapshot handles

Files:

- `src/storage/manifest.rs` (or current owner)
- `src/storage/manager.rs`
- `tests/lsm_manifest.rs` (new)

Work:

1. Extend the per-slab manifest record with:
   - `compaction_level: CompactionLevelIndex`;
   - `state: SlabState`;
   - `successors: Vec<SlabId>`;
   - HLC interval (`min_row_hlc`, `max_row_hlc`);
   - byte count and row count;
   - sidecar coverage references.

2. Add or reuse manifest-level
   `manifest_epoch: ManifestEpoch`.

3. Add level-aware queries:
   - `slabs_at_compaction_level(compaction_level)`;
   - `overlapping_slabs(level, range)`;
   - `oldest_l0_slab_age_millis(now)`;
   - `compaction_level_total_bytes(compaction_level)`;
   - `compaction_level_slab_count(compaction_level)`;
   - `manifest_snapshot()` returning
     `Arc<ManifestSnapshot>`.

4. Admission integration: every newly finalized
   sealed slab is inserted as `(L0, Live)` via the
   existing admission path's manifest update; ADR 055
   adds no new admission-time hook.

5. Recovery integration: on startup, the local
   manifest mirror rebuilds from finalized control-
   ring mutations + durable slab bytes. Any durable
   sealed slab missing compaction-level metadata after upgrade
   is conservatively classified as `(L0, Live)`.

Tests (`tests/lsm_manifest.rs`):

- New admissions enter `(L0, Live)`.
- L1+ overlap queries are HLC-based.
- Manifest snapshot is immutable across later
  mutations.
- Corrupt or missing local mirror rebuilds from
  durable state.
- Active slab never appears as a compaction input
  candidate.

Exit:

- Every authoritative non-active slab has level /
  state metadata; snapshots provide MVCC for readers.

## Phase 2 — Pure compaction picker

Files:

- `src/storage/lsm.rs`
- `tests/lsm_picker.rs` (new)

Work:

1. Implement `pick_compaction` per Phase 0 §7.

2. `L0 → L1` rule:
   - require `compaction_level_slab_count(L0) >=
     max_l0_slab_count`;
   - require `oldest_l0_slab_age_millis(now) >=
     lsm_l0_max_age_millis`;
   - select the current L0 slab set;
   - add every L1 slab whose HLC interval intersects
     the L0 union range;
   - dominant reason = `LevelSize`.

3. `L_n → L_{n+1}` rule (n = 1, 2):
   - compute level target as
     `lsm_l1_slab_size_target_bytes *
     max_l1_slab_count * lsm_size_ratio.pow(n - 1)`;
   - require source-level total > target;
   - select the source-level slab with highest size-
     pressure score;
   - retention/tombstone/index-coverage/layout debts
     refine the score, not whether to pick;
   - record `dominant_reason` per actual debt
     contribution;
   - add every next-level overlapping slab by HLC
     interval.

4. Picker is **pure**: no IO, no clock except `now`,
   no lease acquisition.

5. Active, retired, and `DeletePending` slabs are
   never selected.

Tests (`tests/lsm_picker.rs`):

- Empty manifest returns `None`.
- 3 L0 runs returns `None` (below threshold).
- 4 L0 runs all > age returns `Some(L0→L1 plan)`.
- 4 L0 runs but oldest below age returns `None`.
- L1 byte pressure returns `L1→L2` plan.
- L2 byte pressure returns `L2→L3` plan.
- Active, retired, delete-pending slabs never
  selected.
- Plan source-level labels bounded to `0`, `1`, `2`.
- Each `DominantReason` exercised via fixture combos.

Exit:

- Scheduling decisions deterministic and unit-
  testable.

## Phase 3 — Manifest snapshots and slab pins

Files:

- `src/storage/slab_pins.rs` (new)
- `src/storage/manager.rs`
- `src/storage/cdc.rs`
- `src/network/data_server.rs`
- `tests/lsm_pins.rs` (new)

Work:

1. Implement `SlabPinTable` per Phase 0 §11.

2. Wire ordinary scan / iterator open paths to:
   - read a manifest snapshot;
   - resolve slabs from that snapshot;
   - acquire pins for resolved slabs;
   - hold guard for iterator lifetime.

3. Wire CDC catchup open (ADR 054 §6) to acquire
   pins for slabs covering
   `[start_cursor, snapshot_hlc]`.

4. Do **not** pin live-tail CDC reads. Live-tail
   uses ADR 054's in-memory ring.

5. Remote read RPC handlers hold pins for the
   response stream lifetime.

Tests (`tests/lsm_pins.rs`):

- Pin count increments / decrements with guard
  lifetime.
- `DeletePending` acquire fails; caller retries
  against fresh snapshot.
- Old snapshot can read old slabs after a
  compaction commit.
- New snapshot sees output slabs.
- CDC catchup pins slabs during catchup; releases
  at handoff.
- Remote RPC handler holds pins for response stream
  lifetime.
- LSM compaction code writes no entries into
  `relocated_reads` and depends on no LSM-driven
  relocation rows (architectural guard, scoped to LSM
  only — existing non-LSM relocation machinery in
  `src/storage/manager.rs` for retention rewrites
  remains untouched).
- Cached-reference audit (Phase 0 Work §15): every
  pre-existing `RecordLocation`-caching site
  enumerated in the audit either acquires a pin
  through the new manifest-snapshot path, or falls
  inside an existing non-LSM relocation domain that
  ADR 055 leaves untouched. No new unprotected
  cache site introduced by LSM code.

Exit:

- Readers safely span manifest epoch swaps without
  relocation shims.

## Phase 4 — Survivor stream

Files:

- `src/storage/lsm_worker.rs` (new)
- `src/storage/manager.rs`
- `src/storage/retention.rs`
- `tests/lsm_survivors.rs` (new)

Work:

1. Implement `LsmCompactionWorker::run_one(plan)`
   skeleton.

2. At job start:
   - acquire pins on `plan.input_slabs ∪
     plan.overlapping_slabs`;
   - acquire sidecar read/write leases per ADR 052;
   - read retention floor once from
     `TableLifecycleCatalog`;
   - record as `plan.retention_floor_snapshot`.

3. Open a min-heap merger over input slabs in
   `(HLC, RowKeyId, RecordRowIndex)` order.

4. Implement `apply_elision` per Phase 0 §10. This
   helper may materialize a per-plan working set; the
   streaming-bounded contract belongs to the cold
   rebuild over input/output slab views.

5. Apply lifecycle rules in this exact order:
   - retention floor first (ADR 048);
   - append-only / content-hash data+tombstone
     pair-GC below floor (ADR 047 / 049);
   - PK prefix pruning with **boundary-sentinel
     retention** (ADR 050 §7);
   - migrated-range tombstone reclaim;
   - fail-closed on ambiguous table ownership.

6. Preserve every version above floor.

Tests (`tests/lsm_survivors.rs`):

- Append-only row below table floor drops.
- Append-only data+tombstone pair drops only when
  delete HLC is below floor.
- Tombstone above floor preserves the row history.
- PK chain retains all versions above floor plus
  newest below-floor boundary.
- PK tombstone boundary retained.
- Older below-floor PK versions drop.
- Ambiguous table ownership row retained.
- Migrated-range tombstone reclaim drops.
- Survivor stream sorted by merge key.
- Content-hash duplicate not re-collapsed.

Exit:

- Worker produces lifecycle-correct survivor stream.

## Phase 5 — Output slabs and sidecars

Files:

- `src/storage/lsm_worker.rs`
- `src/storage/manager.rs`
- `src/storage/multirow.rs`
- `src/storage/secondary_index.rs`
- `src/storage/secondary_index_segment.rs`
- `tests/lsm_output.rs` (new)
- `tests/lsm_secondary_index.rs` (new)

Work:

1. Encode survivor rows into fresh slab files using
   existing slab writer + multi-row packing
   helpers.

2. Split outputs at `lsm_l1_slab_size_target_bytes`
   (or level-specific target). Never append after
   fsync.

3. Rebuild CSBs from output rows. Do not merge
   input CSBs.

4. Use current descriptor compression policy for
   unambiguous typed rows. Preserve existing
   encoded frames for ambiguous rows.

5. For every output slab, build secondary-index
   sidecars for `Building` and `Ready` descriptors.
   Skip `Dropping`.

6. Verify rebuilt sidecar by cold rebuild before
   commit. Mismatch returns
   `CompactionFailure::SidecarDigestMismatch`.

7. Mark output slabs covered when uncovered inputs
   compact into index-covered outputs.

8. Backfill loses to compaction with the existing
   `compaction_race` reject.

9. Compute output run summaries.

10. Temp output filenames use
    `compaction-temp-<job_id>-` prefix.

Tests (`tests/lsm_output.rs`,
`tests/lsm_secondary_index.rs`):

- Output slabs contain all and only survivors.
- Output split respects target size.
- Output metadata preserves
  `(key, hlc, row_idx, tombstone, locality)`.
- CSB descriptor identity matches output
  descriptor.
- Output sidecar contains only survivor data rows.
- `Building` and `Ready` indexes get sidecars.
- `Dropping` index gets no output sidecar.
- Uncovered input heals to covered output.
- Sidecar digest mismatch aborts before commit.
- Backfill read-lease race aborts with
  `compaction_race`.
- Temp output filenames use correct prefix.

Exit:

- Compaction job produces durable output slabs and
  sidecars ready for atomic install.

## Phase 6 — Digest verification and control-ring commit

Files:

- `src/storage/lsm_worker.rs`
- `src/network/r0_control.rs` (or current owner)
- `src/storage/manifest.rs`
- `tests/lsm_commit.rs` (new)
- `tests/lsm_digest.rs` (new)

Work:

1. Add
   `MutationKind::CompactionCommit(CompactionCommit)`
   to the locality control-ring mutation carrier.

2. Pre-commit worker verification:
   - cold-rebuild `digest.table.row.visible.v1`
     over pre-commit manifest view, **localized to
     touched slabs** (input slabs only — visible-
     state digest is monoidal so localized
     comparison is equivalent to whole-table
     comparison);
   - cold-rebuild over proposed post-commit view
     (output slabs only);
   - assert equality unconditionally;
   - sample per-version-entry digest verification
     for rebuilt sidecars at
     `lsm_digest_verify_sample_rate`.

3. Apply path validation:
   - reject `ManifestRace` if `manifest_epoch_pre`
     differs from current epoch;
   - validate every input slab exists, is `Live`,
     and matches the plan compaction level;
   - validate every output slab ID is fresh;
   - validate sidecar swap digest summaries;
   - validate visible-domain invariance summary
     (`digest_summary.visible_pre ==
     digest_summary.visible_post`).

4. Commit apply:
   - increment `manifest_epoch`;
   - insert outputs at `target_level` as `Live`;
   - transition inputs to
     `Retired { successors: output_slab_ids }`;
   - install sidecar swaps;
   - emit `LsmCompactionCommitted`.

5. Reject behavior:
   - `ManifestRace`: release pins; let picker re-
     plan;
   - `SidecarDigestMismatch`: release pins; delete
     temp outputs; emit invariant audit;
   - `VisibleDigestInvariance`: pause compaction
     for the affected `(locality, table)` scope;
     emit page-worthy audit.

6. Crash recovery:
   - scan for `compaction-temp-*` files on startup;
   - delete any not referenced by the finalized
     manifest;
   - replay manifest from finalized control-ring
     mutations.

Tests (`tests/lsm_commit.rs`,
`tests/lsm_digest.rs`):

- Commit swaps N inputs for M outputs atomically at
  an epoch boundary.
- Manifest race leaves inputs `Live` and outputs
  unreferenced.
- Visible digest mismatch rejects and pauses
  compaction.
- Sidecar digest mismatch rejects before inputs
  retire.
- Old snapshot sees old slabs; new snapshot sees
  output slabs.
- Crash-shaped restart after output fsync but
  before commit deletes orphan temp files.
- Crash-shaped restart after commit rebuilds
  exact manifest state.
- Visible-domain audit cost localizes to
  input/output bytes.

Exit:

- Multi-slab replacement is atomic, digest-checked,
  and restart-safe.

## Phase 7 — Ref-counted retirement and GC sweep

Files:

- `src/storage/lsm_worker.rs`
- `src/storage/slab_pins.rs`
- `src/storage/manifest.rs`
- `tests/lsm_gc.rs` (new)

Work:

1. Add `LsmGcWorker` on locality leader, tick
   `lsm_gc_tick_millis`.

2. On each sweep:
   - find `Retired` slabs with `pin_count == 0`;
   - propose a manifest mutation transitioning to
     `DeletePending`;
   - delete slab and sidecar bytes through existing
     storage delete paths;
   - after deletion succeeds, propose a manifest
     mutation removing them.

3. Failed delete leaves `DeletePending` for retry.

4. Expose `lsm_pending_delete_slabs{}` and
   `lsm_retired_slab_pinned_bytes{}` gauges.

Tests (`tests/lsm_gc.rs`):

- Retired slab with active pin is not deleted.
- Retired slab transitions to `DeletePending` after
  last pin drops.
- `DeletePending` slab cannot be newly pinned.
- Delete failure retries without exposing slab to
  readers.
- Sidecar bytes deleted only after no reader can
  see the retired slab.

Exit:

- Old physical bytes reclaimed once active readers
  release pins.

## Phase 8 — CDC and retention race tests

Files:

- `src/storage/cdc.rs`
- `src/network/data_server.rs`
- `tests/lsm_cdc.rs` (new)

Work:

1. Architectural assertion: no CDC cursor-held
   compaction lease or dormant subscription pin in
   the code path.

2. Retained-row rewrite tests:
   - subscription catches up while compaction
     rewrites retained rows;
   - catchup uses pinned manifest snapshot until
     live-tail handoff;
   - emitted CDC cursor sequence remains monotonic;
   - no duplicate events across catchup/live-tail
     boundary.

3. Retention-bound rejection tests:
   - cursor below floor receives
     `CDC_REJECT_BELOW_RETENTION_FLOOR`;
   - compaction may physically drop below-floor
     rows;
   - in-retention cursor not rejected due to
     compaction alone.

4. Ring-overflow continuity:
   - compaction does not mask
     `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP`.

5. Long-running iterator pin lifecycle:
   - subscription opens → pins acquired;
   - compaction commits → input slabs retired but
     pinned;
   - subscription completes catchup → pins drop;
   - GC sweep deletes the slab on next tick.

Tests (`tests/lsm_cdc.rs`):

- All five scenarios as deterministic golden
  vectors.

Exit:

- CDC behavior is explicitly retention-bound and
  compaction-safe.

## Phase 9 — Worker startup, scheduling, tier-sweeper coordination

Files:

- `src/storage/lsm_worker.rs`
- `src/node.rs`
- `src/storage/sweeper.rs`
- `tests/lsm_worker.rs` (new)

Work:

1. Spawn one `LsmCompactionWorker` per locality
   leader unconditionally.

2. Concurrency = 1 per locality V1.

3. Worker loop:
   - sleep `lsm_picker_tick_millis`;
   - take manifest snapshot;
   - call pure picker;
   - if a plan exists, run one job;
   - on `ManifestRace`, re-plan next tick.

4. **Pause/resume via node-local maintenance
   config**. Pause stops new jobs; active job
   finishes.

5. Tier sweeper coordination:
   - tier sweeper skips slabs claimed by compaction
     write-lease;
   - compaction outputs return as ordinary sealed
     slabs;
   - tier sweeper may move them later.

Tests (`tests/lsm_worker.rs`):

- Worker starts for locality leader; not for non-
  leader.
- Worker respects concurrency cap.
- Pause stops new jobs but lets active job finish.
- Tier sweeper skips compaction-claimed slab.
- Manifest race does not spin; retries on next
  tick.

Exit:

- Compaction runs as a bounded background service.

## Phase 10 — Metrics, audit, runbook

Files:

- `src/metrics/registry.rs`
- `src/audit/events.rs`
- `plans/ops/runbooks/lsm_compaction.md` (new)
- `plans/ops/runbooks/README.md`
- `plans/ops/observability.md`
- `tests/metrics_smoke.rs`

Work:

1. Register `lsm_*` metrics per Phase 0 §12 / ADR
   §9.

2. Add audit events:
   - `LsmCompactionCommitted { locality_id,
     manifest_epoch_pre, manifest_epoch_post,
     level_from, input_slab_count,
     output_slab_count, bytes_in, bytes_out,
     elided_versions, dominant_reason }`;
   - `LsmInvariantViolation { locality_id, kind,
     manifest_epoch }`;
   - `LsmCompactionPaused { locality_id, reason }`.

3. Operator runbook:
   - reading L0 debt and oldest L0 age;
   - interpreting manifest-race rate;
   - responding to pending-delete growth;
   - diagnosing retired pinned bytes and stuck
     readers;
   - handling sidecar / visible digest invariance
     alarms (page on-call; do not auto-resume);
   - expected CDC rejects when consumers fall
     below retention;
   - tuning `max_l0_slab_count`, output target
     size, size ratio, concurrency.

4. Index runbook in `plans/ops/runbooks/README.md`.

Tests:

- Metrics register with exact bounded labels.
- No tenant / table / subscription / slab_id /
  error-string labels on `lsm_*`.
- Audit event names stable.

Exit:

- Operators can see compaction debt, throughput,
  failure, lag, and reader pin pressure.

## Phase 11 — Bench

Files:

- `benches/lsm_compaction.rs` (new)
- `Cargo.toml`
- `plans/impl/055_lsm_leveled_compaction_substrate_bench_results.md`
  (after promotion)

Bench scenarios:

1. Write amplification.
2. Read amplification (PK lookup) before/after.
3. Read amplification (HLC scan) before/after.
4. Space amplification (≈ 1.11 expected).
5. Visible-domain audit cost as fraction of
   compaction CPU.
6. Admission isolation (p99 idle vs active).
7. CDC catchup isolation under compaction.
8. L0 burst absorption (1 minute saturated
   admission).
9. `ManifestRace` rate under concurrent admission +
   compaction.
10. Retired-pin lifetime with long-running scan.
11. Manifest commit p99 for default-sized job.
12. Sidecar rebuild cost for 0 / 1 / 5 indexes.
13. L0 → L1 throughput at 1 / 10 / 100 input slabs.

Starting acceptance targets:

- Admission p99 regression under default active
  compaction ≤ 5%.
- CDC catchup throughput regression ≤ 5%.
- Atomic commit p99 ≤ 50 ms for default-sized job.
- No visible-digest invariance failures across the
  bench matrix.
- Steady-state space amplification ≈ 1.11×.
- Visible-domain audit ≤ 5% of compaction CPU.
- Compaction output throughput ≥ 100 MiB/sec
  decoded.
- Sidecar rebuild + digest verification ≤ 20% CPU
  over output write with one index.

Bench results: small / medium / large matrix per
ADR 030. Smoke artifact lands first; full matrix
is the promote-to-shipped gate.

Exit:

- Bench harness captures cost and value of the new
  background service.

## Phase 12 — Architectural lockdown

Files:

- `tests/architecture_test.rs`

Guards (per ADR §13):

1. `CompactionLevelIndex` exactly-one definition; range
   `0..=3`.
2. `CompactionJobId`, `ManifestEpoch` exactly-one
   definitions.
3. `CompactionMergeKey` exactly-one definition; not
   imported by any CDC module.
4. `lsm_*` metrics carry exactly the bounded
   labels.
5. No new `MsgType` containing `LSM` or `COMPACTION`.
6. No new `FRAME_FLAG_*`.
7. No new `NodeCapability` variant.
8. No `ObjectStore` trait method addition.
9. No descriptor field for compaction level /
   manifest.
10. No persisted visible `StateDigest` field on
    slab footer or run manifest.
11. CDC cursor types do not import locality, level,
    or compaction-key fields.
12. No CDC cursor-held compaction pin or dormant
    subscription lease.
13. Secondary-index sidecar compaction path writes
    new files; no in-place rewrite helper exists.
14. `LsmRejectCode` taxonomy closed to three V1
    codes.
15. `LsmCompactionWorker` reads the elision floor
    only through `TableLifecycleCatalog`.
16. Visible-domain digest invariance check is
    unconditional in the install path.
17. `LsmConfig` defaults match Phase 0 §6 verbatim.
18. No `Lsm*V[0-9]` / `Compaction*V[0-9]` type
    suffixes.
19. No project-name-prefixed LSM identifiers.
20. No LSM code path writes into `relocated_reads`
    and no LSM type embeds a relocation table.
    Existing non-LSM relocation machinery in
    `src/storage/manager.rs` (retention rewrites,
    sweeper paths) remains in place and is not
    touched by ADR 055. The guard is scoped to LSM-
    introduced code only.
21. No `StorageTier` field on LSM run summaries.
22. No `DigestEvictionMarker` or compaction-driven
    digest mutation on the control ring (only
    `CompactionCommit` carries digest summary, not
    digest mutation).

Exit:

- Future compaction edits cannot drift the
  substrate silently.

## Verification gate

```sh
cargo test --lib storage::lsm
cargo test --test lsm_manifest --quiet
cargo test --test lsm_picker --quiet
cargo test --test lsm_pins --quiet
cargo test --test lsm_survivors --quiet
cargo test --test lsm_output --quiet
cargo test --test lsm_secondary_index --quiet
cargo test --test lsm_commit --quiet
cargo test --test lsm_digest --quiet
cargo test --test lsm_gc --quiet
cargo test --test lsm_cdc --quiet
cargo test --test lsm_worker --quiet
cargo test --test architecture_test --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench lsm_compaction --no-run
cargo bench --bench lsm_compaction -- --quick
cargo check --quiet
cargo check --benches --quiet
git diff --check
cargo fmt --check
```

Promotion-to-shipped gate (after Phase 1–12 land):

- All Phase 1–12 golden vectors pass.
- Architectural lockdown tests pass.
- Bench small / medium results pass starting
  acceptance targets.
- Visible-domain invariance audit observed
  unfailing.
- ADR 052 §9 sidecar rebuild scenarios pass end-to-
  end.
- ADR 048 retention floor reads through the new
  compaction path produce same elision decisions
  as per-slab `retention_compaction`.
- ADR 054 CDC catchup over compacting workloads
  delivers every event with bounded latency and no
  duplicates.
- Operator runbook
  `plans/ops/runbooks/lsm_compaction.md`
  finalized.

## File-by-file expected edits

(Phase 0 audits actual paths.)

- `src/schema/units.rs` — extend (~30 LOC)
- `src/storage/lsm.rs` — new (~400 LOC)
- `src/storage/lsm_worker.rs` — new (~700 LOC)
- `src/storage/slab_pins.rs` — new (~150 LOC)
- `src/storage/manifest.rs` — extend (~200 LOC)
- `src/storage/manager.rs` — extend (~250 LOC)
- `src/storage/secondary_index.rs` — extend
  (~150 LOC)
- `src/storage/multirow.rs` — touched (~20 LOC)
- `src/storage/retention.rs` — touched (~30 LOC)
- `src/storage/cdc.rs` — extend (~50 LOC)
- `src/storage/sweeper.rs` — extend (~80 LOC)
- `src/network/r0_control.rs` — extend (~120 LOC)
- `src/network/data_server.rs` — touched (~20 LOC)
- `src/node.rs` — touched (~30 LOC)
- `src/metrics/registry.rs` — extend (~100 LOC)
- `src/audit/events.rs` — extend (~40 LOC)
- `tests/architecture_test.rs` — extend (~150 LOC,
  22 guards)
- 9 new test files (~200-400 LOC each)
- `benches/lsm_compaction.rs` — new (~500 LOC)
- `plans/ops/runbooks/lsm_compaction.md` — new
  (~200 lines)

Total ≈ 6000 LOC. Most net-new; existing surfaces
touched lightly.

## V1 design summary

ADR 055 ships the leveled compaction substrate.
Per-locality LSM, four levels (`L0..L3`), HLC-
disjoint partitioning at L1+, slabs as the run
format. Storage tier (sweeper concern) is
orthogonal and not part of the LSM substrate. Per-
locality manifest mirror tracks
`(compaction_level, state, successors, hlc_range, sidecar
coverage, manifest_epoch)`; mutations ride the
existing locality control ring (ADR 016) for atomic
epoch boundaries replicated to followers. Compaction
is deterministic so each replica reproduces the same
logical output set from the same inputs; cross-
replica logical equality holds via determinism.
Compaction picker chooses jobs primarily by level
size; retention/tombstone/index-coverage debts score
within levels. Compaction worker reads input slabs,
runs the merged stream through existing
`tombstone_compaction` / `retention_compaction` /
`secondary_index_compaction_context` primitives
lifted to streaming form, writes output slabs at
the next level with fresh sidecars per ADR 052 §9,
and proposes `CompactionCommit` mutation. Manifest
commit is atomic via the existing finalization-
marker boundary. Reader pins (manifest snapshot +
ref-counted slab pins) provide MVCC; retired slabs
delete-on-last-pin-release. **No new LSM-introduced
`relocated_reads` rows** — the existing non-LSM
relocation machinery in `src/storage/manager.rs`
(retention rewrites) stays in place untouched, and
the Phase 0 cached-reference audit verifies no
unprotected cache site survives. The elision floor is identical to ADR 048's
retention floor. Visible-domain digest invariance
is audited unconditionally at every commit via cold
rebuild over input/output slabs only — bounded by
touched bytes, not table size. Per-version-entry
digest algebra is verified against cold rebuild on
a sampled cadence. CDC catchup readers acquire
manifest-snapshot pins at open and operate
independently of compaction; reachability remains
the retention contract. No new wire surface, no new
node capability, no on-disk format change beyond
the slab-manifest extension carried by
`CompactionCommit`.

## Deferred to follow-up ADRs

- Size-tiered compaction picker.
- Subcompaction.
- Tiered storage tier moves (orthogonal sweeper).
- Bloom filters per slab.
- Cross-replica compaction deduplication.
- Manual compaction trigger RPC.
- Compaction throttling / IO QoS.
- Hot-key compaction prioritization.
- L4+ extension.
- Per-row audit grain at compaction-time elision.
- Rolling sidecar rebuild without full compaction.
- Compaction-time schema-evolution transforms.
- Durable visible-`StateDigest` placement (anti-
  entropy).
- Direct compaction of already-tiered objects.
- Manifest history checkpoint compaction.
- Periodic background cold-rebuild visible-domain
  audit.
