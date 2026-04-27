# Implementation plan: ADR 042 — Analytical Multi-Row Slab Fragments

Companion to `plans/adr/042_analytical_multi_row_fragments.md`.

## Goal

Implement `RecordLayoutPolicy::MultiRow` as a per-table opt-in
storage layout that packs N-row Arrow batches into single slab
records, using storage-only synthetic columns for per-row
identity material and a uniform 4-byte `record_row_index`
extension to `RecordLocation`.

Includes the supersession-metadata fix that closes a CSB direct-
answer soundness gap (real today on single-row tables, more
visible under multi-row).

## Non-goals (V1)

- **No vector-form identity/locality PBC opcodes.** ADR 042
  *removes* ADR 035's scalar `LOAD_IDENTITY` /
  `ROUTE_BY_LOCALITY`. Vector-form replacements are a follow-up
  ADR pulled when Toron's planner needs them.
- **No auto-triggered single→multi migration.** Descriptor edits
  drive the transition explicitly in V1.
- **No row-level DELETE tombstones.** Defers to ADR 045 or
  sibling.
- **No PK-less append-only tables.** ADR 045 territory.
- **No per-fragment CSB catalog.** Stats stay per-slab.
- **No grouped AGG work.** ADR 040 follow-up territory.
- **No new public HRFT frame family.** Storage-layout only.
- **No project-name-prefixed protocol acronyms, reject codes,
  metric prefixes, module names, or helper types** (per
  `plans/conventions/no-project-name-in-protocols.md`).
- **No new `V1` / `V2` suffixes** on Rust types, descriptors,
  capabilities, or helper structs (per
  `plans/conventions/no-version-suffixes.md`).
- **No LSM-tree leveled compaction.**

## Phase 0 — Contract freeze and golden fixtures

Lock the descriptor surface, synthetic-column schema, and index
extension. Every later phase references these.

Outputs:

- `RecordLayoutPolicy { SingleRow, MultiRow(MultiRowLayoutDescriptor) }`
  in `src/schema/table.rs`.
- `MultiRowLayoutDescriptor { target_rows, max_rows,
  max_record_bytes }` with newtype-validated fields.
- Newtypes in `src/schema/units.rs`: `TargetRowCount`,
  `MaxRowCount`, `RecordRowIndex`.
- Synthetic-column names pinned: `__row_hlc__: FixedSizeBinary(16)`,
  `__row_key_id__: FixedSizeBinary(32)`,
  `__row_routing_locality__: UInt64`.
- `RecordLocation` extends with `record_row_index: RecordRowIndex`.
- `SupersessionMetadata { Untracked, None, Earliest(HybridTimestamp) }`
  in `src/schema/core.rs` (parallel to `TombstoneMetadata`).
- Hard caps pinned: `MaxRowCount` ≤ 65,536; default
  `target_rows = 4096`; default `max_record_bytes = 1 MiB`;
  hard ceiling 8 MiB (descriptor validator rejects above).
- Golden fixtures: a single-row record (single-row table layout
  unchanged) + a multi-row record with two rows + a multi-row
  record with three rows mixing HLCs (recompaction-shaped).

Exit:

- Contract surface frozen.
- Golden fixtures land for synthetic-column IPC bytes and the
  redb encoding of `record_row_index`.
- Descriptor extension compiles.

## Phase 1 — Descriptor + index extension

Likely files:

- `src/schema/table.rs`
- `src/schema/core.rs`
- `src/schema/units.rs`
- `src/storage/index.rs` (redb encoding)
- `src/storage/core.rs` (`RecordLocation` extension)
- All call sites that construct `RecordLocation` literals

Work:

1. Add `RecordLayoutPolicy` to `TableSchemaDescriptor`. Default
   to `SingleRow` for legacy KV and typed tables that don't
   opt in. (The current codebase identifier is
   `TableSchemaDescriptorV1`; the convention's rename pass per
   `plans/conventions/no-version-suffixes.md` is separate planned
   work and is not gated on 042.)
2. Validate `MultiRowLayoutDescriptor`: `target_rows ≤ max_rows`,
   `max_rows ≤ 65,536`, `max_record_bytes` non-zero and below
   the 8 MiB hard ceiling.
3. Include layout in descriptor fingerprint computation.
4. Extend `RecordLocation` with `record_row_index:
   RecordRowIndex`. Existing call sites thread
   `RecordRowIndex::ZERO` for single-row paths.
5. Update redb persistent index encoding to include
   `record_row_index` (4 bytes per row across the board). No
   deployed external state, so a clean schema migration is
   appropriate (per `no-premature-backward-compat.md`).
6. Architectural-test guard: any new `RecordLocation` construction
   site must thread `record_row_index` explicitly. Catches silent
   `ZERO` defaults at review time.

Tests:

- `RecordLocation` round-trips through redb encoding with
  non-zero `record_row_index`.
- Descriptor round-trips `MultiRow(..)`.
- Fingerprint changes when layout changes.
- Invalid multi-row caps reject (out-of-range `target_rows`,
  `max_rows`, `max_record_bytes`).
- All existing single-row tests pass with `RecordRowIndex::ZERO`
  threaded explicitly.

Risk:

The `RecordLocation` extension touches every consumer of
`Vec<RecordLocation>`. Expect heavy mechanical diff.

Exit:

- Index carries row-position field cluster-wide; single-row
  paths are functionally unchanged with a 4-byte index-row
  schema delta.

## Phase 2 — Multi-row packing module

Likely files:

- `src/storage/multirow.rs` (new)
- `src/storage/mod.rs`
- `src/schema/table.rs`

Work:

1. Implement:

   ```rust
   pub fn pack_record_batch_by_bucket(
       descriptor: &TableSchemaDescriptor,
       commit_hlc: HybridTimestamp,
       batch: &RecordBatch,
   ) -> Result<Vec<PackedMultiRowRecord>, TableWriteValidationError>;
   ```

2. A `PackedMultiRowRecord` carries:
   - bucket identity/range (single bucket per record);
   - the IPC payload with synthetic columns appended;
   - row identities, HLCs, and routing localities already
     extractable from the IPC.

3. Append synthetic columns at packing time:
   - `__row_hlc__` (commit HLC for write-time, original row HLC
     for compact-time);
   - `__row_key_id__` (BLAKE3 of canonical PK per ADR 037);
   - `__row_routing_locality__` (descriptor-derived locality
     per ADR 037 / 038).

4. Sort rows inside each record by
   `(__row_routing_locality__, __row_key_id__, __row_hlc__,
   original_ordinal)` — encoder-enforced, not advisory. Same
   sort at write-time and rewrite-time so byte-stable
   determinism holds across both paths (ADR 042 §13).

5. Centralize synthetic-column stripping in one helper:

   ```rust
   pub fn strip_storage_columns(batch: &RecordBatch)
       -> io::Result<RecordBatch>;
   ```

6. Enforce `target_rows`, `max_rows`, and `max_record_bytes`.
7. Reject any input batch where a single row exceeds
   `max_record_bytes` after schema overhead.

Tests:

- Bucket-pure write of N rows produces one packed record;
  synthetic columns appear with correct values; user schema
  unchanged.
- Cross-bucket write fans out into one record per present
  bucket.
- Synthetic columns are stripped before user-visible output
  (architectural test).
- Packing is deterministic for the same input row set; encoder
  enforces sort order.
- Oversized rows reject when a single row exceeds
  `max_record_bytes`.

Exit:

- Packing and stripping are independently testable without the
  data server.

## Phase 3 — Write and replication path

Likely files:

- `src/network/data_server.rs`
- `src/storage/manager.rs`
- table-write tests

Work:

1. Keep `bifurcate_table_write` unchanged for `SingleRow`.
2. For `MultiRow`, route through `pack_record_batch_by_bucket`
   after existing ADR 037 admission validation.
3. Append one physical slab record per `PackedMultiRowRecord`.
4. Insert one logical redb/key-index row per Arrow row, with
   the correct `record_row_index`.
5. Public write and replica frame families remain stable. If
   internal payload-replication needs an enum, keep it below
   the wire boundary.
6. Parent write finalization and audit remain one parent write
   with N logical rows.

Tests:

- Multi-row table write appends fewer physical slab records than
  rows.
- Duplicate PK in one batch still rejects (ADR 037 invariant).
- Duplicate parent write is idempotent.
- Remote replica receives packed bytes and rebuilds identical
  row-index entries.
- Legacy table writes still split into single-row records.

Exit:

- Multi-row tables ingest packed records behind descriptor
  opt-in.

## Phase 4 — Read path: PBC and AGG composition + scalar-opcode removal

Likely files:

- `src/storage/manager.rs`
- `src/network/data_server.rs`
- `src/storage/aggregate.rs`
- `src/schema/pbc.rs` (opcode removal)
- `tests/` for any PBC tests exercising the removed opcodes

Work:

1. Point read flow (ADR 042 §5):
   - resolve `(tenant_id, key_identity)` → version chain;
   - binary-search for `hlc <= as_of_hlc`;
   - read physical record;
   - if multi-row IPC schema (sniffed via synthetic-column
     presence): decode, `take(&[record_row_index])`, strip
     synthetic columns, return one-row batch.

2. Range / scan flow:
   - existing slab-admission path under ADRs 038/039 unchanged;
   - per-record decode dispatches on synthetic-column presence;
   - **group selected multi-row locations by enclosing physical
     record** before decode so each record is decoded once per
     query;
   - take selected row ordinals from the decoded batch;
   - strip synthetic columns;
   - flow into the existing PBC batch pipeline.

3. PBC composition: multi-row IPCs are already batches;
   rebatcher becomes a fast path. Single-row records continue
   to need rebatching.

4. **Remove scalar-context PBC opcodes.** Per ADR 042 §8:
   delete `LOAD_IDENTITY` and `ROUTE_BY_LOCALITY` executor
   implementations from `src/schema/pbc.rs`. Remove their
   entries from the opcode catalog, decoder, validator,
   executor, tests, and fuzz corpora. Per
   `no-premature-backward-compat.md` no compat shim — direct
   deletion.

5. AGG path is unchanged externally; internally it folds over
   the larger batches.

Tests:

- Point read at multiple `as_of_hlc` values across a multi-row
  record with mixed-HLC rows.
- Range scan with predicate selecting one row from a 4096-row
  record: decoded record count = 1, returned-row count = 1.
- AGG `count_star` / `min` / `max` over a multi-row table
  matches the row-scan oracle.
- PBC programs containing the removed identity/locality scalar
  opcodes no longer decode or validate.
- Architectural / fuzz tests no longer reference the removed
  opcodes.
- Query budget counts logical rows admitted, not physical
  records.

Exit:

- Read path delivers correct results for multi-row, single-row,
  and mixed scans.
- Scalar-context opcodes are gone from the executor.

## Phase 5 — Supersession metadata + CSB direct-answer extension

Likely files:

- `src/schema/core.rs` (`SupersessionMetadata` enum)
- `src/storage/manager.rs`
- `src/storage/aggregate.rs` (`slab_visibility_complete`)
- `src/storage/index.rs` (`TieredSlabPointer` encoding)
- `src/consensus/raft_storage.rs` (apply observer extension)
- `src/network/data_server.rs` (CSB direct answer)
- `src/metrics/registry.rs`

Work:

1. Add `SupersessionMetadata` enum (parallel to
   `TombstoneMetadata`) on `TieredSlab` and
   `TieredSlabPointer`. New flag bits in the redb encoder.
2. Wire the four maintenance hooks from ADR 042 §11:
   - **Apply path**: when a write commits a new row version
     whose previous version lives in a sealed or tiered slab,
     the apply observer fires `on_row_supersession(slab_id, hlc)`
     and the manager updates `supersession_metadata` for that
     slab.
   - **Active-slab seal**: seal-time computation derives the
     earliest internal supersession HLC from the just-sealed
     row version chains.
   - **Retention / tombstone-aware compaction**: rewrites
     preserve or recompute `supersession_metadata` on output
     pointers from the survivor row HLCs.
   - **Sweeper backlog convergence**: a backlog pass over
     committed writes ensures pre-042 sealed slabs that have
     been superseded transition `Untracked → tracked` once the
     sweeper reconciles.
3. Extend `slab_visibility_complete`:

   ```rust
   seal_hlc <= as_of_hlc
       && tombstone_metadata.is_tracked()
       && tombstone_metadata.earliest()
           .is_none_or(|t| t > as_of_hlc)
       && supersession_metadata.is_tracked()
       && supersession_metadata.earliest()
           .is_none_or(|t| t > as_of_hlc)
   ```

4. CSB direct-answer path consults the state machine's
   committed-version log when pointer metadata is `None`
   (defense in depth, parallel to ADR 040's tombstone
   consultation).
5. New metric:
   `agg_csb_short_circuit_skipped_supersession_metadata_untracked_total`.
6. Update `plans/impl/040_aggregate_pushdown_impl.md` to note
   the parallel discipline.

Tests:

- Round-trip pointer encoding for all three
  `SupersessionMetadata` states.
- Post-supersession write makes the source slab ineligible at
  `as_of_hlc >= update_hlc`.
- `Untracked` pointer fails closed and increments the metric.
- Defense-in-depth: pointer says `None`, state machine has a
  superseding write — CSB direct answer falls through.

Exit:

- ADR 040's CSB short-circuit is sound under MVCC writes (not
  just tombstones). Multi-row slabs and single-row slabs both
  benefit.

## Phase 6 — Stats, tombstones, retention

Likely files:

- `src/storage/stats.rs`
- `src/storage/manager.rs`
- `src/storage/sweeper.rs`

Work:

1. Update CSB stats writer to iterate multi-row IPCs (decode →
   strip synthetic columns → per-row fold). Per-row logic
   unchanged.
2. Tombstone metadata composes unchanged (ADR 042 §16) — no
   sweeper change needed beyond preserving `tombstone_metadata`
   through multi-row recompaction outputs.
3. Retention rewrites of multi-row records: decode survivors,
   drop pruned versions, write a new record with a fresh IPC
   carrying preserved synthetic columns. Recompute CSB and
   visibility metadata.

Tests:

- CSB stats over a multi-row slab equal stats over the same
  rows in single-row layout.
- Stats pruning can skip a multi-row slab.
- Retention pruning of a 4096-row record with 50% pruned
  produces a 2048-row record with correct synthetic-column
  values.
- Tombstone-aware reclaim composes with multi-row records
  without regression to ADR 040 invariants.

Exit:

- Stats and visibility paths produce identical answers across
  multi-row / single-row layouts of the same logical table.

## Phase 7 — Compaction modes (recompact + descriptor migration)

Likely files:

- `src/storage/sweeper.rs`
- `src/storage/manager.rs` (`build_tiered_row_refresh_plan_*`)
- `src/storage/multirow.rs`

Work:

1. Multi-row recompact: merge two records whose combined rows
   ≤ `max_rows` and bytes ≤ `max_record_bytes`; preserve per-row
   HLCs from synthetic columns; output sorted by
   `(__row_routing_locality__, __row_key_id__, __row_hlc__,
   original_ordinal)`.
2. Single → multi migration: descriptor edit flips
   `RecordLayoutPolicy` from `SingleRow` to `MultiRow`. A
   re-cluster sweep reads single-row slabs, packs by bucket,
   writes multi-row records, atomically swaps the redb index
   entries. Concurrent writes during migration accept temporary
   mixed-layout slabs; the existing `relocated_reads` machinery
   handles in-flight reads.
3. Multi → single migration: symmetric.
4. **Recompaction trigger discipline.** V1 recompaction is
   opportunistic only — fired during retention rewrites and
   tombstone-aware reclaim, not as a separate sweep pass.
   The coherent-rewrite primitive (below) already does the
   work; a dedicated sweep would double the I/O.

5. **Single coherent compaction.** Each rewrite the sweeper
   emits MUST simultaneously perform every applicable upgrade
   on the output pointer in one atomic pointer-swap:

   - layout transition (this phase);
   - tombstone-metadata upgrade `Untracked → tracked` (ADR 040);
   - supersession-metadata upgrade `Untracked → tracked`
     (Phase 5).

   No piecemeal sweeper passes. The existing sweeper primitive
   "read N inputs, write M outputs, atomically swap pointer"
   already supports this; we extend what gets recomputed inside
   the swap, not how many swaps occur. (When a future LSM ADR
   opens, level promotion joins the same pointer-swap.)

Tests:

- Recompact: rows in == rows out (logically); per-row HLC
  preserved; index relocations sound; output sort order matches
  spec.
- Single → multi migration on a fixture table: read results
  identical pre- and post-migration at multiple `as_of_hlc`
  values.
- Multi → single migration preserves query and point-read
  results.
- Concurrent reads during migration resolve to either old or
  new locations (existing `relocated_reads` machinery), never
  a torn state.
- **Coherent upgrade**: a slab with `Untracked` tombstone AND
  `Untracked` supersession metadata transitions both to tracked
  in a single recompact pass; one pointer-swap commits both.
- Migrated-range tombstone compaction removes only affected
  rows.
- `storage_multirow_uncompacted_row_records_total` reflects
  row-record data still waiting for analytical-table
  re-fragmentation.

Exit:

- Sweeper can recompact multi-row records and migrate tables
  between layouts.
- Compaction is a single coherent operation.

## Phase 8 — LSM forward-compat checks (no LSM adopted)

Likely files:

- `src/storage/multirow.rs`
- compaction tests

Work:

1. Treat multi-row records as future LSM run output units,
   without adopting levels or read-merge policy (per ADR 042
   forward-compat section).
2. Encoding determinism: rewriting the same survivor set twice
   produces byte-identical IPCs with identical synthetic-column
   values (modulo expected AEAD nonce variance where encryption
   applies).
3. Helper APIs that a later leveled compactor could reuse:

   ```rust
   encode_multirow_from_survivors(...);
   decode_multirow_synthetic_only(...);   // fast metadata-only
   merge_multirow_streams(left, right);   // 2-way merge
   multirow_sort_key(row) -> SortKey;     // canonical sort key
   ```

4. Add explicit TODO hooks in the relevant compaction code so
   a future LSM ADR has clear extension points (per ADR 042
   forward-compat concerns):
   - bucket-pure records may overlap by key range within one
     bucket (concern §1);
   - global redb indexing vs. future per-segment indexes are
     different read-path models (concern §2);
   - `target_rows` cap divergence between segment and slab
     (concern §3);
   - slab-level CSB footers may need filter-cache promotion
     (concern §5).
5. Keep LSM-specific policy out of this ADR: no memtable
   redesign, no level fanout, no run manifest, no overlapping-
   run read merge, no bloom/filter block placement.

Tests:

- Determinism test on encode → decode → encode round-trip.
- Mixed-HLC survivor rows preserve AS OF semantics after
  rewrite.
- 2-way merge proof-of-concept produces a valid output record
  from sorted inputs without using source slab offsets.
- Fragment caps are enforced on rewrite output, not just the
  write path.
- The implementation contains no hidden LSM policy but leaves
  clear hooks for future run construction and metadata.

Exit:

- Helpers exist for a future LSM compactor without committing
  any LSM policy.

## Phase 9 — Recovery and observability

Likely files:

- `src/storage/manager.rs` (recovery scan)
- `src/metrics/registry.rs`

Work:

1. Recovery scan: for each slab record, check IPC schema for
   synthetic columns. If present, decode IPC and rebuild
   `RecordLocation` entries with correct `record_row_index`
   per row, plus locality-index entries from
   `__row_routing_locality__`.
2. Metrics — functional segments only, no brand prefix on the
   protocol segment (per
   `plans/conventions/no-project-name-in-protocols.md`):
   - `storage_multirow_records_written_total`
   - `storage_multirow_rows_written_total`
   - `storage_multirow_rows_per_record` (histogram)
   - `storage_multirow_decode_micros` (histogram)
   - `storage_multirow_rows_materialized_total`
   - `storage_multirow_point_read_amplification_bytes`
     (histogram)
   - `storage_multirow_recompact_total`
   - `storage_multirow_uncompacted_row_records_total`
     — counts row-record data still waiting to be re-packed
     into multi-row records on a multi-row-table descriptor
     (operator signal that conservative-migration backlog
     hasn't drained).
   - `agg_csb_short_circuit_skipped_supersession_metadata_untracked_total`
3. Architectural test or focused unit test verifying that
   synthetic columns never appear in user-facing query / read
   output.
4. Architectural test enforcing
   `plans/conventions/no-project-name-in-protocols.md` for new
   surfaces this ADR introduces: metric segments
   (`storage_multirow_*`, `agg_csb_*`), reject codes, synthetic
   column names (`__row_*__`) — none may carry a brand prefix
   on the protocol segment itself (e.g. no
   `wendelon_storage_wmultirow_*`). The check scans the
   registered metric / reject catalogs at test time. (The
   convention permits an optional repo-level Prometheus
   namespace wrapping a functional segment, so
   `wendelon_storage_multirow_*` would still pass — the
   forbidden form is brand-prefix on the protocol segment
   itself.)

Tests:

- Crash recovery on a node with mixed multi-row + single-row
  slabs reproduces the in-memory index identically.
- Recovery from a multi-row record builds locality-index
  entries from `__row_routing_locality__` without descriptor
  context.
- All metrics render under the architectural-test golden
  output with functional segments.

Exit:

- Multi-row layout is fully observable.
- Recovery is verified end-to-end and descriptor-free.

## Phase 10 — Benchmarks

Likely files:

- `benches/` (existing harness)
- `src/storage/multirow.rs` (test helpers)

Benchmarks:

- 65,536-row table write: single-row layout vs. multi-row
  layout. Target: at least 10× reduction in physical slab
  record count.
- Full-scan PBC over the table: single-row rebatching vs.
  multi-row decode. Target: no slower than single-row baseline.
- AGG count/min/max over a multi-row table. Target: no slower
  than single-row baseline; visible per-record decode-cost
  reduction in micros.
- Point read from a multi-row table at several
  `max_record_bytes` settings (256 KiB, 1 MiB, 8 MiB). Target:
  amplification metric matches expected bound.
- Retention rewrite with mixed survivor ratios (10%, 50%,
  90%). Target: rewrite throughput within sweeper budget.
- Multi-row rewrite from pre-sorted survivor streams. Target:
  approximate the future leveled-compaction merge-run shape
  to validate Phase 8 helpers.

Exit criteria:

- Multi-row writes reduce physical slab record count by at
  least 10× on a max-size analytical write.
- Full-scan PBC and AGG paths are no slower than single-row
  baseline.
- Point-read penalty is visible in metrics and bounded by
  configured `max_record_bytes`.

## Phase 11 — Ratification cleanup

- ADR canonical at `plans/adr/042_analytical_multi_row_fragments.md`.
- Impl plan canonical at this path.
- `plans/wip/roadmap-post-040.md` retired in favor of
  `plans/wip/roadmap-post-042.md`.
- All `plans/wip/{claude,codex,gemini}_{adr,impl}_042.md` and
  `plans/wip/aggregation_plan_042.md` deleted in the
  ratification commit.
- Add a convention if any new pattern recurred: potentially
  `plans/conventions/synthetic-storage-columns.md` if §3's
  pattern is general enough to appear in future ADRs (likely
  given row-tombstone follow-up).

## Next-pass execution plan

The current implementation pass has landed the core ADR 042
surface: descriptor opt-in, `record_row_index`, redb row-index
encoding, multi-row write/read composition, scalar-opcode
removal, recovery, metrics, stats/retention/tombstone
composition, coherent partial multi-row retention rewrites, and
the benchmark harness.

The next pass closes the remaining items from this plan without
reopening completed work.

Execution update:

- Item 1 is implemented: production writes, retention rewrites,
  L1 compaction, and `benches/multi_row_fragments.rs` now share
  `src/storage/multirow.rs`.
- Item 2 is implemented: `SupersessionMetadata` is persisted on
  tiered pointers, redb round-trips all three states, AGG
  visibility requires tracked tombstone and supersession
  metadata, post-tier overwrites update the source pointer, and
  the sweeper backfills `Untracked` tiered pointers to tracked
  supersession metadata through a committed `TierSlab` pointer
  update.
- Item 3 is implemented for sealed L1 slabs: schema evolution now
  allows explicit `SingleRow <-> MultiRow` layout flips while
  preserving stable primary-key and locality contracts, and
  `SlabManager::recluster_sealed_table_layout` performs the
  explicit replacement-slab rewrite with `relocated_reads`.
- Items 4 and 5 are implemented for the current compaction
  surface: retention rewrites, tombstone-aware L1 compaction, and
  layout recluster use shared survivor/packing helpers;
  `storage_multirow_recompact_total` increments on completed
  multi-row rewrites; and direct tests exercise
  `merge_multirow_streams` as a two-way sorted merge.
- Item 6 is implemented: synthetic-column IPC bytes and redb
  `record_row_index` encoding have golden checks; multi-row
  query materialization asserts user-visible batches strip
  `__row_*__` columns; architecture tests enforce brand-clean
  ADR 042 protocol surfaces, unsuffixed metric/synthetic names,
  and deleted scalar-context opcode absence.
- Item 7 verification completed for this pass with
  `cargo test --lib --quiet`,
  `cargo test --test architecture_test --quiet`,
  `cargo bench --bench multi_row_fragments --no-run`,
  `cargo bench --bench multi_row_fragments -- --quick`,
  `git diff --check`, staged diff check, rustfmt over changed
  Rust files, and the scalar-opcode kill-list grep over
  `src tests benches`.

1. **Extract the reusable multi-row storage module.**

   Files:

   - `src/storage/multirow.rs` (new)
   - `src/storage/mod.rs`
   - `src/network/data_server.rs`
   - `src/storage/manager.rs`
   - `benches/multi_row_fragments.rs`

   Work:

   - Move inline packing, synthetic-column append/strip,
     row-take, IPC encode/decode, and compaction survivor
     rewrite helpers into `src/storage/multirow.rs`.
   - Introduce the shared types/functions the original Phase 2
     and Phase 8 called for:
     `PackedMultiRowRecord`,
     `pack_record_batch_by_bucket`,
     `encode_multirow_from_survivors`,
     `decode_multirow_synthetic_only`,
     `multirow_sort_key`, and
     `merge_multirow_streams`.
   - Make write-time packing and rewrite-time packing use the
     same deterministic sort key:
     `(__row_routing_locality__, __row_key_id__, __row_hlc__,
     original_ordinal)`.
   - Keep all helpers storage-internal; do not add a public HRFT
     frame family.

   Exit:

   - Data server writes, retention rewrites, L1 compaction, and
     the benchmark harness call the same helper module.
   - Golden tests cover single-row IPC unchanged, two-row
     multi-row IPC, three-row mixed-HLC IPC, and redb
     `record_row_index` encoding.
   - Deterministic encode -> decode -> encode tests pass for
     identical survivor sets.

2. **Land persisted supersession metadata.**

   Files:

   - `src/schema/core.rs`
   - `src/storage/index.rs`
   - `src/storage/manager.rs`
   - `src/storage/aggregate.rs`
   - `src/network/data_server.rs`
   - `src/metrics/registry.rs`
   - apply-observer code that sees committed row writes

   Work:

   - Add `SupersessionMetadata { Untracked, None,
     Earliest(HybridTimestamp) }` parallel to
     `TombstoneMetadata`.
   - Add it to tiered slab pointer state and redb pointer
     encoding with tri-state round trips.
   - Wire the maintenance hooks:
     write-apply supersession observation, active-slab seal
     derivation, retention/tombstone compaction preservation or
     recomputation, and backlog convergence for existing
     pointers.
   - Extend `slab_visibility_complete` to require both tracked
     tombstone metadata and tracked supersession metadata.
   - Keep the existing conservative direct-answer supersession
     fence as defense in depth until persisted metadata is the
     normal fast path.
   - Add
     `agg_csb_short_circuit_skipped_supersession_metadata_untracked_total`.

   Exit:

   - Pointer encoding round-trips all supersession states.
   - Post-supersession writes make the source slab ineligible
     for CSB direct answer at `as_of_hlc >= update_hlc`.
   - `Untracked` fails closed and increments the metric.
   - Pointer says `None` but committed state shows a
     superseding write -> direct answer falls through.

3. **Implement explicit layout migration sweeps.**

   Files:

   - `src/schema/table.rs`
   - `src/storage/sweeper.rs`
   - `src/storage/manager.rs`
   - `src/storage/multirow.rs`

   Work:

   - Replace the current blanket `record_layout_policy` alter
     rejection with an explicit recluster/migration path.
   - Implement `SingleRow -> MultiRow` sweep: read eligible
     row-record data, pack by bucket through the shared
     multi-row helper, write replacement records, atomically
     swap row-index entries, and populate `relocated_reads`.
   - Implement `MultiRow -> SingleRow` sweep symmetrically for
     rollback or workload-shape correction.
   - Preserve mixed-layout readability during the migration
     window.
   - Keep migration explicit; do not introduce auto-triggered
     single-to-multi migration.

   Exit:

   - Reads and queries match before/after migration at multiple
     `as_of_hlc` values.
   - In-flight reads resolve through `relocated_reads` and never
     observe a torn state.
   - `storage_multirow_uncompacted_row_records_total` reflects
     remaining migration backlog.

4. **Complete coherent compaction metadata upgrades.**

   Files:

   - `src/storage/manager.rs`
   - `src/storage/sweeper.rs`
   - `src/storage/multirow.rs`

   Work:

   - Ensure every retention rewrite, tombstone-aware reclaim,
     and layout migration recomputes layout, tombstone metadata,
     and supersession metadata inside one output construction.
   - Commit those upgrades through one pointer/index swap; do
     not add piecemeal sweeper passes.
   - Increment `storage_multirow_recompact_total` when a
     multi-row rewrite/recompact completes.

   Exit:

   - A slab with both tombstone and supersession metadata
     `Untracked` transitions both to tracked in one committed
     rewrite.
   - Migrated-range tombstone compaction removes only affected
     rows from multi-row records.
   - Recompact tests verify rows-in == rows-out, per-row HLC
     preservation, relocation soundness, and canonical output
     sort order.

5. **Add LSM-forward helper proof points without LSM policy.**

   Files:

   - `src/storage/multirow.rs`
   - compaction tests
   - `benches/multi_row_fragments.rs`

   Work:

   - Exercise `merge_multirow_streams` as a two-way sorted
     merge over storage rows without using source slab offsets.
   - Enforce fragment caps on rewrite output, not just write
     input.
   - Add explicit TODO hooks for future LSM concerns: bucket
     overlap, global redb index vs. per-segment indexes,
     segment/slab `target_rows` divergence, and CSB filter-cache
     promotion.
   - Keep out LSM policy: no levels, no run manifest, no
     memtable redesign, no overlapping-run read merge.

   Exit:

   - Future LSM compaction has reusable row-sorted encode/merge
     primitives.
   - Tests prove mixed-HLC survivor rows retain AS OF semantics
     after helper-driven rewrites.

6. **Close architectural and golden-test gaps.**

   Files:

   - tests colocated with schema/storage modules
   - metric registry tests
   - scalar-opcode guard tests

   Work:

   - Add golden fixture checks for synthetic-column IPC bytes and
     redb row-index encoding.
   - Add a focused architectural test that user-facing read/query
     outputs never expose `__row_*__` storage columns.
   - Add a registry/catalog scan that ADR 042 metric segments,
     reject codes, synthetic columns, helper names, and opcode
     catalogs stay free of project-name protocol prefixes and
     new `V1` / `V2` suffixes.
   - Preserve the scalar-opcode kill-list check for
     `LOAD_IDENTITY` and `ROUTE_BY_LOCALITY`.

   Exit:

   - The implementation plan's remaining literal tests are
     represented in code.
   - No hidden compatibility shim reintroduces scalar-context
     PBC.

7. **Verification gate for the next pass.**

   Required before closing the pass:

   - `cargo test --lib --quiet`
   - `cargo bench --bench multi_row_fragments --no-run`
   - `cargo bench --bench multi_row_fragments -- --quick`
   - `git diff --check`
   - `rustfmt --check --edition 2024` on touched Rust files
   - scalar-opcode kill-list grep over `src tests benches`

## Post-merge stabilization watch

Mirrors ADR 039 / 040 discipline. Once 042 ships:

- `storage_multirow_rows_per_record` distribution →
  default `target_rows = 4096` validation per workload. Long
  tail below 100 rows/record signals a tenant who opted into
  multi-row on a workload that doesn't pack well; long tail
  above 50K signals tables hitting `max_rows` ceiling.
- `storage_multirow_point_read_amplification_bytes`
  distribution → whether operators are setting multi-row
  layout on tables that take point reads. High amplification
  flags a misconfigured table.
- `storage_multirow_recompact_total` rate → sweeper
  load under multi-row layout.
- `storage_multirow_uncompacted_row_records_total`
  → conservative-migration backlog. Should decay as compaction
  re-packs row-record data into multi-row records on opted-in
  tables.
- `storage_multirow_decode_micros` distribution → per-record
  decode cost; informs whether operators are hitting decode
  pathology on wide schemas.
- `agg_csb_short_circuit_skipped_supersession_metadata_untracked_total`
  → pre-042 supersession-untracked pointer backlog; should
  decay toward zero as compaction or row-write activity
  promotes pointers to tracked.
- AGG metrics: `agg_rows_scanned_total / agg_request_total`
  improvement vs. single-row baseline for the same query
  class.
- Architectural-test enforcement: synthetic-column names never
  appear in user-visible outputs; metric / reject prefixes
  stay brand-clean on the protocol segment (ADR 041); removed
  scalar opcodes don't reappear in the executor or test
  corpora.
