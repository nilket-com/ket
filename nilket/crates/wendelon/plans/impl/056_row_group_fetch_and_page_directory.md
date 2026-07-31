# Implementation Plan: ADR 056 — Row-Group Fetch and Layered Page Directory

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR:
`plans/adr/056_row_group_fetch_and_page_directory.md`

Inherits write-path bifurcation:
`plans/adr/003_stellarator_bifurcated_writes.md` (the
substrate's release gate).
Inherits chunked AEAD authenticator:
`plans/adr/020_l2_envelope_encryption_and_dek_lifecycle.md`.
Inherits unencrypted footer / offset index:
`plans/adr/022_l2_slab_format_footer_indexed_range_reads.md`.
Inherits multi-row record layout:
`plans/adr/042_analytical_multi_row_fragments.md`.
Inherits per-buffer LZ4_FRAME compression:
`plans/adr/043_arrow_ipc_body_compression.md`.
Inherits column-projected fetch substrate:
`plans/adr/044_l2_columnar_fetch.md` —
`RecordBufferDirectory`, `TieredMultiRangeReadPlan`,
`L2ColumnFetchConfig`, `L2FetchStrategy`,
`ColumnarFallbackReason`.
Inherits canonical statistics rules:
`plans/adr/039_slab_footer_column_statistics_v1.md`.
Inherits secondary-index hit shape:
`plans/adr/052_secondary_indexes.md` §3.
Inherits LSM slab-writer composition:
`plans/adr/055_lsm_leveled_compaction_substrate.md` §5.

## Status

Draft — staged for council review. Layered hierarchy
framing, V1 substrate shape, directory shapes,
metadata key, strategy variant, and reject taxonomy
are council-converged across the three drafts in
`plans/wip/`.

This plan implements the outer layer of the substrate
hierarchy (per ADR §Status):

1. **ADR 056 V1 (this plan)** — RecordBatch row
   groups + per-row-group stats. Outer macro-pruning
   layer.
2. **ADR 057 encoding cascades (future ADR / plan)** — per-buffer pages
   + encoding cascades inside each row group
   RecordBatch's encoded buffers. Inner layer.
3. **COCD (future ADR / plan)** — compute over
   encoded representation (COCD).

V1 implementation commitments (pinned by ADR):

- multi-RecordBatch IPC emission in the slab writer
  is the only intra-record paging mechanism in
  ADR 056 V1; no custom partial-buffer materializer
  ships in this ADR;
- `RecordPageDirectory` carries one
  `RowGroupDirectoryEntry` per row group plus one
  `BufferPageDirectoryEntry` per `(row_group_index,
  ipc_column_ordinal, ipc_buffer_ordinal)` triple
  — V1's per-buffer-per-row-group addressing
  surface;
- row-group stats (min / max / null-count for ADR
  039-supported column types) are V1 capability;
  Phase 3 covers them;
- pre-056 single-RecordBatch records continue to
  read via ADR 044 column fetch with no migration
  step;
- writer config defaults off; Phase 10 accepted the
  empirical gate and promotion keeps rollout opt-in.

ADR 057 encoding cascades forward-compatibility (preserved by V1 design,
not implemented here):

- `BufferPageDirectoryEntry` is extensible — ADR 057 encoding cascades
  may add a sibling `buffer_simd_pages: Vec<...>`
  field for intra-buffer page entries inside each
  encoded buffer, or a sibling custom_metadata key;
- the lockdown guards forbidding
  `partial_buffer_materialize` symbols and
  `mini_page_*` fields are scoped to ADR 056's V1
  surface, not project-wide; ADR 057 encoding cascades will introduce
  these inside its own namespaced module without
  violating the guards.

## Goal

Land the `RecordPageDirectory` + row-group fetch
substrate. Row groups are Arrow IPC RecordBatches; the
directory is encrypted IPC schema custom metadata; the
fetch planner extends ADR 044 to three phases with
stats-driven pruning.

The implementation:

- one `RowGroupIndex`, one `RowGroupRowCount`, one
  `IpcColumnOrdinal` newtype-unit (each `u32`);
  reuses `RecordRowIndex`, `RecordBodyByteOffset`,
  `RecordBodyByteLen`, `IpcBufferIndex`,
  `EncryptedChunkIndex`, `EncryptedChunkCount`,
  `RowCount` from ADR 042 / 043 / 044;
- one `RecordPageDirectory`, one
  `RowGroupDirectoryEntry`, one
  `BufferPageDirectoryEntry`, one
  `RowGroupColumnStats`, one `RecordBodyByteRange`
  definition; storage-local; not imported by CDC,
  PBC, recovery, or catalog modules;
- IPC `Schema.custom_metadata` key
  `storage.row_group_directory` carrying serialized
  directory (flatbuffer or bincode, decided in Phase
  0);
- writer-side multi-RecordBatch IPC emission for
  `MultiRow` records above paging thresholds, gated by
  node-local `RowGroupWriterConfig`;
- writer-side per-row-group min / max / null-count
  computation for ADR 039-supported types;
- reader-side directory parse during ADR 044 Phase A;
- conservative PBC predicate classifier (equality,
  range, IS NULL, IS NOT NULL; conjunction skips when
  any branch proves disjoint, unsupported siblings
  do not block; disjunction skips only when every
  branch proves disjoint);
- three-stage row-group classification (Skip / Keep /
  NeedsPredicateFetch) with fail-closed discipline;
- `RowGroupFetchPlan` extending
  `TieredMultiRangeReadPlan` with per-row-group +
  per-buffer ranges, separating Phase B and Phase C
  plans;
- `L2FetchStrategy::RowGroup(RowGroupFetchPlan)`
  variant joining `Columnar`, `WholeRecord`,
  `Fallback`;
- ADR 052 secondary-index `fetch_indexed_rows`
  adapter that maps index hits to row-group point
  reads;
- bounded `storage_l2_row_group_fetch_*` metric
  family;
- 3-code `RowGroupRejectCode` taxonomy
  (`PageDirectoryParseError`,
  `RowGroupRangeOverflow`, `LayoutUnsupported`);
- audit lifecycle events
  (`RowGroupFetchSampled`,
  `RowGroupInvariantViolation`);
- bench matrix that distinguishes point-read latency,
  row-range fetch latency, predicate-fetched vs.
  stats-pruned saving, compression overhead, writer
  CPU cost, planner overhead, GET-rate cost, L1 vs L2
  cost.

## Deliverables

- `RowGroupIndex`, `RowGroupRowCount`,
  `IpcColumnOrdinal` newtype-units in
  `src/schema/units.rs`.
- `RecordPageDirectory`, `RowGroupDirectoryEntry`,
  `BufferPageDirectoryEntry`, `RowGroupColumnStats`,
  `RecordBodyByteRange`, `RowGroupFetchPlan`,
  `RowGroupRejectCode`, `RowGroupFallbackReason`,
  `RowGroupWriterConfig`, `L2RowGroupFetchConfig` in
  `src/storage/row_group.rs` (new — Phase 0 audits
  actual path; may share file with
  `src/storage/columnar.rs` from ADR 044).
- `RecordBufferDirectory` extension in
  `src/storage/columnar.rs` (one optional
  `page_directory: Option<RecordPageDirectory>`
  field).
- `L2FetchStrategy` extension in
  `src/storage/columnar.rs` (new variant + new
  `RowGroupFallbackReason` enum).
- Multi-RecordBatch slab writer extension in
  `src/storage/multirow.rs` (or current owner).
- Stats computation in `src/storage/row_group.rs`
  (consults ADR 039 canonical rules from
  `src/storage/csb.rs`).
- ADR 044 Phase A parser extension in
  `src/storage/columnar.rs`.
- PBC predicate classifier in `src/schema/pbc.rs`
  (or current owner).
- Row-group fetch planner in
  `src/storage/columnar.rs` — extends
  `pick_l2_fetch_strategy`.
- ADR 052 secondary-index adapter in
  `src/storage/secondary_index.rs` —
  `fetch_indexed_rows` helper.
- LSM compaction integration in
  `src/storage/lsm_worker.rs` (passes
  `RowGroupWriterConfig` through to slab writer;
  deterministic recomputation).
- `storage_l2_row_group_fetch_*` metric family in
  `src/metrics/registry.rs` with bounded label
  enumerations.
- Audit event kinds in `src/audit/events.rs`.
- Architectural guards in `tests/architecture_test.rs`
  (Phase 11).
- Golden vectors per phase below.
- Bench harness in `benches/row_group_fetch.rs`.
- Operator runbook
  `plans/ops/runbooks/row_group_fetch.md`.

## Non-goals

- No intra-buffer SIMD mini-pages or sub-row-group
  page directory (ADR 057 encoding cascades territory).
- No new compression codec or LZ4 frame flag change
  (row-group RecordBatch boundaries provide
  independence; ADR 043 codec catalog unchanged).
- No variable per-buffer mini-page row count.
- No byte-budgeted (vs. row-aligned) row groups.
- No new envelope `format_version`.
- No ADR 022 footer extension.
- No `SlabRecordHeader` change.
- No descriptor field for paging or stats policy.
- No synthetic Arrow column for the directory.
- No sparse Arrow array materializer for unfetched
  row groups.
- No per-table opt-in / opt-out.
- No `RecordPageDirectory` cache across queries.
- No bulk slab-level page-directory pre-fetch.
- No new `MsgType` containing `ROW_GROUP` or
  `PAGE_DIRECTORY`.
- No new `FRAME_FLAG_*`.
- No new `NodeCapability` bit.
- No `ObjectStore` trait change.
- No public Toron-visible PBC opcode addition.
- No reserved enum slots, compatibility shims, or
  `V1` / `V2` Rust type suffixes.
- No project-name prefix on new symbols.
- No high-cardinality metric labels.
- No new Cargo feature or dependency.
- No custom partial-buffer materializer (stock Arrow
  decode handles row-group RecordBatches).

## Phase 0 — Contract freeze and path audit

Lock the directory schema, IPC custom_metadata key,
writer/reader configs, planner extensions, fetch-
strategy variant, predicate classifier shape, metric
label enumerations, architectural guards, and V1
defaults before invasive storage edits begin.

**Substrate shape is immutable post-Phase-0.** The
Phase 0 freeze PR is the explicit decision point at
which the V1 substrate becomes locked — once merged,
Phases 1–11 may not reintroduce a partial-buffer
materializer or `mini_page_*` config knobs. Those
land in ADR 057 encoding cascades inside their own namespaced module.
The Phase 11 lockdown guards 25-26 enforce this
post-freeze.

Files:

- `src/schema/units.rs` (newtype-unit extension)
- `src/storage/row_group.rs` (new — POD types,
  serialization, stats computation)
- `src/storage/columnar.rs` (extension; ADR 044
  owner)
- `src/storage/multirow.rs` (writer extension;
  Phase 0 audits actual owner)
- `src/storage/csb.rs` (stats-supported types
  audit)
- `src/storage/secondary_index.rs` (adapter)
- `src/storage/lsm_worker.rs` (config plumbing only)
- `src/schema/pbc.rs` (predicate classifier; Phase 0
  audits actual owner)
- `src/config.rs` (node-local config; Phase 0
  audits)
- `src/metrics/registry.rs` (new metric family)
- `tests/fixtures/row_group_fetch/` (new)
- `tests/architecture_test.rs`

Work:

1. Audit current owners for slab-writer entry point,
   ADR 042 multi-row writer, ADR 043 LZ4 wrapper,
   ADR 044 column-fetch implementation, ADR 052
   secondary-index lookup path, ADR 039 CSB stats
   helpers, PBC filter dispatch, and metric
   registration. Update file paths in subsequent
   phases if actual modules differ.

2. Decide page-directory serialization: **flatbuffer**
   vs. **bincode** vs. **packed bytes**. Prefer
   flatbuffer if Arrow's flatbuffer builder is already
   a dependency; else bincode. Document choice in
   ADR §2 and update before promotion.

3. Audit stats-supported types against ADR 039
   canonical rules. Pin the V1 set in
   `RowGroupWriterConfig::stats_supported_types`.

4. Freeze newtype-units:

   ```rust
   #[repr(transparent)]
   pub struct RowGroupIndex(u32);
   #[repr(transparent)]
   pub struct RowGroupRowCount(u32);
   #[repr(transparent)]
   pub struct IpcColumnOrdinal(u32);
   ```

   Reuse `RecordRowIndex`, `RecordBodyByteOffset`,
   `RecordBodyByteLen`, `IpcBufferIndex`,
   `EncryptedChunkIndex`, `EncryptedChunkCount`,
   `RowCount`, `RecordBodyByteLen` from ADR 042 / 043
   / 044 — do not redefine.

5. Freeze directory shapes:

   ```rust
   pub struct RecordBodyByteRange {
       pub start: RecordBodyByteOffset,
       pub len: RecordBodyByteLen,
   }

   pub struct RecordPageDirectory {
       pub record_id: TieredRecordId,
       pub row_count: RowCount,
       pub row_groups: SmallVec<[RowGroupDirectoryEntry; 16]>,
       pub buffer_pages: Vec<BufferPageDirectoryEntry>,
       pub row_group_stats: Vec<RowGroupColumnStats>,
       pub logical_schema_fingerprint: LogicalSchemaFingerprint,
   }

   pub struct RowGroupDirectoryEntry {
       pub row_group_index: RowGroupIndex,
       pub first_record_row_index: RecordRowIndex,
       pub row_count: RowGroupRowCount,
       pub metadata_range: RecordBodyByteRange,
       pub body_range: RecordBodyByteRange,
   }

   pub struct BufferPageDirectoryEntry {
       pub row_group_index: RowGroupIndex,
       pub ipc_column_ordinal: IpcColumnOrdinal,
       pub ipc_buffer_ordinal: IpcBufferIndex,
       pub range: RecordBodyByteRange,
   }

   pub struct RowGroupColumnStats {
       pub row_group_index: RowGroupIndex,
       pub ipc_column_ordinal: IpcColumnOrdinal,
       pub null_count: RowCount,
       pub min: Option<CanonicalScalar>,
       pub max: Option<CanonicalScalar>,
   }
   ```

   Storage-local. Architectural guard: not imported
   by CDC, PBC, recovery, catalog modules.

6. Freeze the **reject taxonomy**:

   ```rust
   #[repr(u32)]
   pub enum RowGroupRejectCode {
       PageDirectoryParseError = 1,
       RowGroupRangeOverflow   = 2,
       LayoutUnsupported       = 3,
   }
   ```

7. Freeze writer + reader configs:

   ```rust
   pub struct RowGroupWriterConfig {
       pub enabled: bool,                                  // false
       pub target_row_group_rows: RowGroupRowCount,        // 4_096
       pub max_row_group_rows: RowGroupRowCount,           // 8_192
       pub min_paged_record_rows: RowCount,                // 8_192
       pub min_paged_record_bytes: RecordBodyByteLen,      // 512 KiB
       pub stats_supported_types: BTreeSet<DataType>,      // ADR 039 audited
   }

   pub struct L2RowGroupFetchConfig {
       pub enabled: bool,                                  // false
       pub max_selected_row_group_fraction_micros: u32,    // 250_000
       pub max_selected_byte_fraction_micros: u32,         // 500_000
       pub max_ranges_per_record: EncryptedChunkCount,     // 16
       pub max_parallel_range_gets: EncryptedChunkCount,   // 4
   }
   ```

   Two configs because writer-side and reader-side
   toggle independently during fleet rollout.
   `RowGroupWriterConfig` is **deployment-wide** in V1.

8. Freeze the **`L2FetchStrategy` extension**:

   ```rust
   pub enum L2FetchStrategy {
       RowGroup(RowGroupFetchPlan),         // ADR 056
       Columnar(ColumnarRecordFetchPlan),   // ADR 044
       WholeRecord,
       Fallback(RowGroupFallbackReason),
   }

   pub enum RowGroupFallbackReason {
       Disabled,
       SingleRow,
       FullProjection,
       SmallRecord,
       TooFewRows,
       TooManyRowGroups,
       TooManyBytes,
       TooManyRanges,
       MetadataProbeCap,
       UnsupportedArrowLayout,
       RehydrationAvailable,
       UnpagedRecord,
   }
   ```

   Architectural guard: every dispatch site matches
   all four `L2FetchStrategy` variants explicitly;
   no `_` catch-all. Authentication / decode errors
   are NOT variants. `NoSurvivors` is NOT a fallback
   — it's a successful empty-result row-group plan.

9. Freeze `RowGroupFetchPlan` and the Phase B → Phase
   C handoff contract:

   ```rust
   pub struct RowGroupFetchPlan {
       pub record_id: TieredRecordId,
       // directory.page_directory is Some when paged.
       pub directory: RecordBufferDirectory,
       pub surviving_groups: SmallVec<[RowGroupIndex; 16]>,
       // Subset of surviving_groups; non-empty iff
       // phase_b_fetch is Some.
       pub predicate_fetched_groups:
           SmallVec<[RowGroupIndex; 16]>,
       pub phase_b_fetch:    Option<TieredMultiRangeReadPlan>,
       pub phase_c_fetch:    TieredMultiRangeReadPlan,
       pub schema_fingerprint: LogicalSchemaFingerprint,
   }

   /// Runtime intermediate state produced by Phase B
   /// execution and consumed by Phase 5 materialization
   /// when `phase_b_fetch` was `Some`. Carries the
   /// already-decoded predicate batches and per-group
   /// survival masks so Phase C does not refetch
   /// predicate columns and materialization does not
   /// rerun PBC over them.
   pub struct PhaseBOutcome {
       pub decoded_predicate_batches: HashMap<RowGroupIndex, RecordBatch>,
       pub survival_masks: HashMap<RowGroupIndex, BooleanArray>,
   }
   ```

   Contract:

   - `phase_b_fetch` is `Some` when stats-driven prune
     left any group classified `NeedsPredicateFetch`;
     `None` when stats fully resolved every group.
   - `predicate_fetched_groups` is the post-Phase-B
     intersection of `surviving_groups` with the groups
     that needed predicate fetch — i.e., the groups
     whose `PhaseBOutcome.survival_masks` are
     load-bearing for filtering rows. Stats-only
     `Skip` groups are dropped from `surviving_groups`
     before Phase B; stats-only `Keep` groups survive
     into Phase C without a mask.
   - Phase B execution returns `PhaseBOutcome`. The
     executor passes that outcome alongside Phase C
     fetched bytes into Phase 5 materialization.
   - `phase_c_fetch` MUST exclude buffers already
     fetched in `phase_b_fetch` (deduplication is
     range-planner duty, asserted in Phase 4 tests);
     Phase 5 then zips Phase B columns + Phase C
     columns into the projected batch.
   - When `phase_b_fetch` is `None`, `PhaseBOutcome`
     is empty and Phase 5 only consumes Phase C
     output.

10. Freeze `RecordBufferDirectory` extension:

    ```rust
    pub struct RecordBufferDirectory {
        // ... existing ADR 044 fields ...
        pub page_directory: Option<RecordPageDirectory>,
    }
    ```

11. Freeze the IPC `Schema.custom_metadata` key:
    literal string `storage.row_group_directory`. No
    project prefix. Architectural guard checks the
    key.

12. Freeze metric shapes per ADR §16:

    ```text
    storage_l2_row_group_fetch_requests_total{strategy}
    storage_l2_row_group_fetch_fallback_total{reason}
    storage_l2_row_group_fetch_row_groups_total{decision}
    storage_l2_row_group_fetch_bytes_total{kind}
    storage_l2_row_group_fetch_plan_micros
    storage_l2_row_group_fetch_selected_ranges
    storage_l2_row_group_fetch_directory_bytes
    storage_l2_row_group_fetch_invariant_violations_total{violation_kind}
    ```

    Pinned label enumerations per ADR §16.

13. Freeze fixtures:
    - small `MultiRow` record below paging thresholds
      (single batch, no directory expected);
    - large multi-RecordBatch record with
      fixed-width-only columns;
    - large multi-RecordBatch record with mixed
      fixed-width + UTF-8 + validity buffers;
    - record with `target_row_group_rows = 4096`
      uniformly;
    - record with last row group partial
      (`row_count < target_row_group_rows`);
    - pre-056 record (single RecordBatch, no
      custom_metadata key);
    - record with corrupted directory (invalid range);
    - record with directory at `row_group_count = 16`
      (typical analytical);
    - record with predicate matching all groups
      (Phase B keep-all);
    - record with predicate matching no groups
      (Phase B all-skip → empty plan, not fallback).

14. Architectural test stubs (full guards in Phase 11):

    - exactly one `RecordPageDirectory`,
      `RowGroupDirectoryEntry`,
      `BufferPageDirectoryEntry`,
      `RowGroupColumnStats` definition;
    - `RowGroupIndex`, `RowGroupRowCount`,
      `IpcColumnOrdinal` newtype-unit definitions;
    - `RowGroupFetchPlan` exactly-one definition;
      not imported by CDC, PBC, recovery, catalog
      modules;
    - `L2FetchStrategy::RowGroup` present; dispatch
      sites have no `_` catch-all;
    - no `RowGroup*V[0-9]` /
      `PageDirectory*V[0-9]` Rust type names;
    - no `Wendelon`-prefixed row-group symbols;
    - no `MsgType` constants attributable to this
      ADR; no new `FRAME_FLAG_*`; no new
      `NodeCapability` bit;
    - `Schema.custom_metadata` key literal is
      `storage.row_group_directory`;
    - writer reads from `RowGroupWriterConfig`
      only, not from a descriptor field;
    - no envelope `format_version` bump; no ADR 022
      footer field addition;
    - no synthetic Arrow column or
      `SyntheticColumn::RowGroupDirectory` enum
      variant;
    - no sparse Arrow array materializer for
      unfetched row groups;
    - no LZ4 frame block-independence flag mutation
      (each row-group RecordBatch's buffers use ADR
      043's existing frame shape).

Exit:

- Types and guard intent pinned; fixtures ready;
  serialization-format choice committed.

## Phase 1 — Writer multi-RecordBatch emission

Files:

- `src/storage/multirow.rs` (or current owner)
- `src/storage/manager.rs`
- `src/storage/row_group.rs`
- `tests/row_group_writer.rs` (new)

Work:

1. Plumb `RowGroupWriterConfig` through the
   `MultiRow` slab writer entry point. Default
   off.

2. For each `MultiRow` record body, if writer config
   enabled AND
   `record.row_count >= min_paged_record_rows` AND
   `record.estimated_bytes >= min_paged_record_bytes`:

   - split rows into row groups of
     `target_row_group_rows` (last group may be
     partial); cap at `max_row_group_rows`;
   - emit one Arrow IPC stream containing:
     - one schema message (with
       `custom_metadata["storage.row_group_directory"]`
       added below);
     - one `RecordBatch` message per row group,
       compressed per-buffer LZ4 per ADR 043;
   - while writing each RecordBatch, capture:
     - metadata byte range (FlatBuffer header);
     - body byte range;
     - per-buffer byte ranges by
       `(ipc_column_ordinal, ipc_buffer_ordinal)`;
   - while writing each RecordBatch, compute
     per-row-group min / max / null-count for
     columns in `stats_supported_types`;
   - assemble `RecordPageDirectory`;
   - serialize and stash into the schema's
     `custom_metadata` key
     `storage.row_group_directory`;
   - re-emit the schema message at IPC stream head
     with the custom_metadata populated (writer must
     build the schema message with directory present
     from the start; in-place metadata mutation
     after IPC header serialization is not
     supported).

3. Records below paging thresholds use the existing
   single-batch path (ADR 042 / 043) — no directory
   emitted, no schema custom_metadata addition.

4. Storage-only synthetic columns appear in every
   row group (per ADR 042 §6). Writer does not add
   or remove storage columns at row-group boundaries.

5. ADR 043 LZ4 frame shape unchanged — each row-
   group's buffers use the existing frame default;
   no block-independence flag mutation.

6. Architectural test: writer never collapses
   row-group splits when descriptor's
   `record_layout_policy` is `MultiRow` and
   thresholds are met.

Tests (`tests/row_group_writer.rs`):

- Writer disabled emits no
  `storage.row_group_directory` key.
- Small `MultiRow` record below thresholds remains
  single-batch.
- Large `MultiRow` record above both thresholds
  emits multiple RecordBatch messages under one
  schema.
- Last row group is partial when total rows are not
  a multiple of `target_row_group_rows`.
- Storage synthetic columns present in every row
  group.
- Directory's row-group spans cover `0..row_count`
  exactly.
- Stats present for ADR 039-supported types; absent
  for unsupported types (not opaque entries).
- Whole-IPC-stream decode (no row-group awareness)
  yields the same logical row set as pre-row-group
  writer on identical input.
- Single-row records unchanged.
- LZ4 frame shape unchanged in row-group buffers
  (no block-independence flag).

Exit:

- Slab writer emits paged records under config;
  pre-config behavior unchanged; whole-stream
  decode byte-equivalent.

## Phase 2 — Directory parser

Files:

- `src/storage/columnar.rs`
- `src/storage/row_group.rs`
- `tests/row_group_directory.rs` (new)

Work:

1. Extend ADR 044 Phase A parsing:
   - parse the IPC schema metadata window;
   - look up
     `Schema.custom_metadata["storage.row_group_directory"]`;
   - if absent: `RecordBufferDirectory.page_directory =
     None` (no error);
   - if present: deserialize into
     `RecordPageDirectory`;
   - validate invariants per ADR §2;
   - on validation failure: return
     `RowGroupRejectCode::PageDirectoryParseError`
     or `RowGroupRangeOverflow` (corruption, not
     fallback);
   - attach to `RecordBufferDirectory`.

2. Validate invariants:
   - row-group spans cover `0..row_count` exactly,
     no gaps no overlap;
   - `metadata_range` and `body_range` inside
     record body;
   - buffer page ranges inside their row group's
     `body_range`;
   - buffer page entries sorted and non-overlapping
     for the same `(row_group_index, ipc_buffer_ordinal)`;
   - every `ipc_column_ordinal` and
     `ipc_buffer_ordinal` exists in IPC schema;
   - `logical_schema_fingerprint` matches
     descriptor's current logical fingerprint
     (ADR 051);
   - row-group stats present only for supported
     types.

3. Probe-cap handling:
   - if directory key is present but cannot be
     reached within ADR 044's
     `max_ipc_metadata_probe_bytes`, planner
     fallback is `MetadataProbeCap`
     (informational, not corruption);
   - if directory bytes are reachable, parsed, and
     invalid, return corruption.

4. Architectural test: reader treats
   `MetadataProbeCap` and `LayoutUnsupported` as
   planner fallback reasons (informational);
   `PageDirectoryParseError` and
   `RowGroupRangeOverflow` as corruption (storage-
   error path).

Tests (`tests/row_group_directory.rs`):

- Pre-056 record (no key) parses with
  `page_directory: None`, no error.
- Well-formed directory parses cleanly; all
  invariants pass.
- Corrupted directory length / overlap / out-of-
  record range / fingerprint mismatch fail closed
  with appropriate reject code.
- Probe cap fallback only fires before parse
  begins.
- Roundtrip: writer → reader → byte-identical
  `RecordPageDirectory` for every fixture.
- Directory parser does not read ADR 022 footer
  fields.

Exit:

- Reader recognizes paged vs. unpaged records and
  rejects malformed directories closed.

## Phase 3 — Row-group stats pruning

Files:

- `src/storage/row_group.rs`
- `src/storage/columnar.rs`
- `src/schema/pbc.rs` (predicate classifier)
- `tests/row_group_pruning.rs` (new)

Work:

1. Add a conservative PBC predicate classifier:

   ```rust
   pub enum RowGroupPredicateKind {
       Equality { col: IpcColumnOrdinal, value: CanonicalScalar },
       RangeLt   { col: IpcColumnOrdinal, value: CanonicalScalar },
       RangeLe   { col: IpcColumnOrdinal, value: CanonicalScalar },
       RangeGt   { col: IpcColumnOrdinal, value: CanonicalScalar },
       RangeGe   { col: IpcColumnOrdinal, value: CanonicalScalar },
       IsNull    { col: IpcColumnOrdinal },
       IsNotNull { col: IpcColumnOrdinal },
       And(Vec<RowGroupPredicateKind>),
       Or(Vec<RowGroupPredicateKind>),
       /// Bail-out — keep all groups, no skip possible.
       Unsupported,
   }

   pub fn classify_pbc_for_row_group_pruning(
       program: &PbcProgram,
   ) -> RowGroupPredicateKind;
   ```

   Anything outside the listed shapes returns
   `Unsupported` → keep all row groups.

2. Implement per-row-group classification:

   ```rust
   pub enum RowGroupClassification {
       Skip,                  // stats prove disjoint
       Keep,                  // stats may match, no early predicate fetch needed
       NeedsPredicateFetch,   // stats insufficient, predicate fetch is economical
   }

   pub fn classify_row_groups(
       directory: &RecordPageDirectory,
       predicate: &RowGroupPredicateKind,
       config: &L2RowGroupFetchConfig,
   ) -> Vec<(RowGroupIndex, RowGroupClassification)>;
   ```

   Discipline:
   - missing stats → `Keep` or
     `NeedsPredicateFetch` based on Phase B
     economics (fail-closed);
   - unsupported type → `Keep` or
     `NeedsPredicateFetch`;
   - truncated stats → `Keep` unless still
     provably disjoint;
   - `And` (conjunction): if **any** branch
     classifies Skip, the group skips — the AND
     cannot match when even one branch proves
     disjoint, regardless of other branches'
     support level. Otherwise propagate the
     strictest of Keep / `NeedsPredicateFetch`
     across remaining branches; unsupported
     branches collapse to Keep without blocking
     skips proven by sibling branches;
   - `Or` (disjunction): the group skips **only
     when every** branch classifies Skip. Any
     Keep, `NeedsPredicateFetch`, or unsupported
     branch keeps the group — the OR cannot be
     proven disjoint unless all branches are;
   - null predicates use `null_count`
     conservatively (`IS NULL` → Skip when
     `null_count == 0`; `IS NOT NULL` → Skip
     when `null_count == row_count`).

3. Exit short-circuits:
   - all-skip: empty-result row-group plan
     (`decision = no_survivors` metric);
   - all-keep with no `NeedsPredicateFetch`:
     skip Phase B, proceed to Phase C with all
     groups surviving;
   - any `NeedsPredicateFetch`: Phase B fetches
     predicate columns for those groups, runs
     PBC filter, derives surviving rows.

Tests (`tests/row_group_pruning.rs`):

- Range predicate `WHERE x > 100` skips groups
  whose `max(x) <= 100`.
- Equality predicate skips groups whose stats
  prove `value` outside `[min, max]`.
- Unsupported PBC op (e.g., `LIKE`) keeps all
  groups.
- `IS NULL` skips groups with `null_count == 0`.
- `IS NOT NULL` skips groups with
  `null_count == row_count`.
- Conjunction skips a group when any branch
  proves disjoint, even when other branches are
  unsupported (e.g., `WHERE x > 100 AND
  name LIKE '%foo%'` skips a group whose
  `max(x) <= 100`, regardless of the LIKE
  branch).
- Conjunction with all branches Keep keeps the
  group.
- Conjunction with all branches unsupported
  keeps the group.
- Disjunction skips only when every branch
  proves disjoint; any unsupported or Keep
  branch keeps the group.
- Truncated stats keep groups unless comparison
  still proves disjoint.
- Row-group pruning never drops a row that
  whole-record PBC would keep, across golden
  fixtures.
- Stats absent → `NeedsPredicateFetch` if
  predicate is supported on the column AND
  Phase B economics are favorable; else
  `Keep`.
- All-skip produces empty plan (not Fallback).

Exit:

- Planner has a correct surviving-row-group set
  before Phase C, with conservative fail-closed
  discipline.

## Phase 4 — Range planning

Files:

- `src/storage/columnar.rs`
- `src/storage/row_group.rs`
- `src/crypto.rs`
- `tests/row_group_fetch_plan.rs` (new)

Work:

1. Extend `pick_l2_fetch_strategy` with the
   four-leg dispatch per ADR §4 decision rule:
   - cheap bypasses (Disabled, SingleRow,
     FullProjection, SmallRecord, TooFewRows,
     RehydrationAvailable);
   - Phase A → directory parse;
   - if absent → ADR 044 column fetch
     (`Fallback(UnpagedRecord)`);
   - Phase B → `classify_row_groups` →
     `execute_phase_b_predicate_fetch` if needed;
   - Phase C → `build_row_group_fetch_plan`;
   - apply thresholds.

2. Build `RowGroupFetchPlan`:
   - for each surviving row group, enumerate
     buffer page ranges from
     `BufferPageDirectoryEntry` filtered by
     `ColumnFetchRequirement`;
   - convert ranges to record-relative
     plaintext ranges;
   - call existing `TieredMultiRangeReadPlan`
     constructor to coalesce into chunk runs (no
     parallel chunk arithmetic);
   - if Phase B fetched predicate columns, those
     ranges are in `phase_b_fetch`; Phase C
     ranges (`phase_c_fetch`) exclude
     already-fetched columns.

3. Compute planning estimates:
   - selected row-group count;
   - selected byte estimate;
   - chunk-run count;
   - avoided record-body bytes.

4. Apply config thresholds; on threshold-exceed,
   return `Fallback(reason)` with appropriate
   variant.

5. Validate
   `RowGroupFetchPlan.schema_fingerprint`
   against directory's
   `logical_schema_fingerprint`.

Tests (`tests/row_group_fetch_plan.rs`):

- Point-read row index maps to one row group;
  plan fetches one row group's projected
  columns.
- Sparse row-group selection (e.g., 2 of 16
  surviving) fetches only those groups'
  projected buffers.
- Dense row-group selection (e.g., 12 of 16)
  returns `Fallback(TooManyRowGroups)`.
- Predicate-fetched groups carry Phase B +
  Phase C plans; Phase C **excludes** any
  buffer already fetched in Phase B (assertion
  on the planner output) so Phase 5 can zip
  Phase B columns + Phase C columns without
  duplication. `predicate_fetched_groups` is
  populated and is a subset of
  `surviving_groups`.
- Range coalescing matches ADR 044 multi-range
  planning for equivalent byte ranges.
- Too many ranges / too many bytes → bounded
  fallback labels.
- All-skip surviving set → empty plan (not
  Fallback).
- Schema fingerprint mismatch → corruption
  error (not Fallback).

Exit:

- Fetch plan deterministically chooses
  row-group, column-buffer, or whole-record
  paths.

## Phase 5 — Dense row-group materialization

Files:

- `src/storage/row_group.rs`
- `src/storage/columnar.rs`
- `src/network/data_server.rs`
- `tests/row_group_materialization.rs` (new)

Work:

1. Decode fetched row-group RecordBatches using
   the **stock Arrow IPC reader**. Each batch is
   a complete dense batch; no partial-buffer
   reconstruction needed.

2. **Phase B / Phase C zip and mask.** When
   `PhaseBOutcome` is non-empty, materialization
   for each row group `g` proceeds:

   ```text
   if g in predicate_fetched_groups:
       // predicate columns + storage cols
       phase_b_batch = decoded_predicate_batches[g]
       // output / aggregate cols
       phase_c_batch = decode_phase_c_buffers(g)
       // zip by ipc_column_ordinal
       zipped = zip_columns(phase_b_batch, phase_c_batch)
       mask = survival_masks[g]
       // arrow filter kernel
       projected_g = filter(zipped, mask)
   else:
       // all required cols arrive in Phase C
       phase_c_batch = decode_phase_c_buffers(g)
       projected_g = phase_c_batch
   ```

   PBC predicate columns are **never re-evaluated**
   in Phase 5 — Phase B already produced the mask.
   The architectural test forbids any post-Phase-B
   PBC re-evaluation in the materializer code path.

3. Apply ADR 044 ordinal-mapping discipline to
   construct `ProjectedRecordBatch` per fetched
   row group with narrow physical schema (only
   projected columns). Strip storage-only columns
   that aren't part of the user-visible projection
   after the zip and mask.

4. Stream batches through PBC / AGG executor
   one at a time, OR concatenate dense batches
   when the caller wants a single batch (Arrow
   `concat` kernels).

5. Never synthesize null arrays for unfetched
   row groups, or for rows filtered out by the
   Phase B mask. Filtered batches are dense
   over surviving rows. Architectural test
   forbids any null-fill path in the
   materializer.

6. For point reads (single
   `record_row_index`):
   - point-read plans never trigger Phase B
     (the predicate is identity-on-row, not
     stats-prunable);
   - decode the one row group from Phase C;
   - apply `take(row_within_group)` on the
     dense batch;
   - strip storage-only columns;
   - return one-row `ProjectedRecordBatch`.

7. Storage-only column projection (visibility
   folds, etc.) consults the same ordinal
   mapping; storage columns appear when the
   requirement names them.

Tests (`tests/row_group_materialization.rs`):

- Dense selected row groups produce
  byte-identical logical output to whole-record
  decode + PBC filter, on every fixture.
- **Phase B mask correctness:** for fixtures
  that exercise `NeedsPredicateFetch`, the
  filtered output equals whole-record-decode +
  whole-record PBC, even though Phase 5 never
  reruns PBC.
- **Phase B / Phase C column zip:** projected
  output contains both predicate columns
  (zipped from `PhaseBOutcome`) and
  output / aggregate columns (decoded from
  Phase C) at the correct ordinals; no column
  is fetched twice (assertion against the
  Phase 4 dedup property).
- **Mask covers full row group:** mask length
  equals row-group `row_count`; filter produces
  a dense sub-batch whose row count equals the
  mask's set-bit count.
- **All-rows-mask short-circuit:** a Phase B
  mask that selects every row produces output
  byte-identical to a stats-only Keep path
  (no extra filter kernel applied; or applied
  as a no-op).
- **Zero-rows-mask short-circuit:** a Phase B
  mask that selects no rows drops the row
  group from output, never emits a zero-row
  batch.
- Point read fetches one row group via Phase C
  only (no Phase B) and returns one user row.
- Aggregate fold over selected row groups
  matches whole-record fold.
- Unfetched row groups absent from output, not
  represented as null rows.
- Storage-column predicates work when
  requirement includes storage columns
  (storage cols travel with Phase B when
  predicate-fetched).
- Concatenate-then-PBC and PBC-stream-per-batch
  produce byte-identical results.

Exit:

- Row-group fetch produces correct narrow
  batches for query and point paths.

## Phase 6 — Storage and query integration

Files:

- `src/storage/manager.rs`
- `src/network/data_server.rs`
- `src/storage/sweeper.rs`
- `tests/row_group_query.rs` (new)

Work:

1. Extend `SlabManager::read_projected_record`
   to try row-group fetch before ADR 044
   column-buffer fetch when config permits.

2. Strategy ladder (economic / unsupported-
   layout reasons only; corruption never falls
   through):

   ```text
   row-group → column-buffer → whole-record
   ```

3. Integrate dense row-group batches with PBC
   projection (ADR 035) and AGG folds (ADR
   040).

4. Preserve AGG short-circuit: ADR 040
   slab-level aggregate answers run before any
   record-body fetch.

5. Preserve CSB ordering: ADR 039 slab pruning
   runs before Phase A metadata probes.

6. Preserve rehydration semantics: ADR 006
   `RehydrationAvailable` bypasses row-group
   fetch.

Tests (`tests/row_group_query.rs`):

- Query path with 1-of-50 columns and
  selective predicate uses row-group strategy;
  metric shows `strategy=row_group`.
- Query path with dense projection uses ADR
  044 column-buffer strategy or whole-record;
  metric shows `strategy=columnar_buffer` or
  `strategy=whole_record`.
- AGG path folds row-group projected inputs
  byte-equivalent to whole-record fold.
- Rehydration available bypasses row-group
  fetch with fallback reason
  `rehydration_available`.
- CSB short-circuit bypasses row-group
  planning.
- Single-row records bypass via cheap-
  bypass.

Exit:

- Production read path exercises row-group
  fetch under config.

## Phase 7 — ADR 052 secondary-index integration

Files:

- `src/storage/secondary_index.rs`
- `src/storage/columnar.rs`
- `tests/row_group_secondary_index.rs` (new)

Work:

1. Add `fetch_indexed_rows` adapter:

   ```rust
   pub async fn fetch_indexed_rows(
       hits: &[SecondaryIndexHit],
       requirement: &ColumnFetchRequirement,
   ) -> Result<Vec<ProjectedRecordBatch>, FetchError>;
   ```

   Behavior:
   - group hits by `(SlabId, RecordLocation)`;
   - for each record, derive a `RowGroupSet`
     from hit `RecordRowIndex` values via the
     directory;
   - call `pick_l2_fetch_strategy` with that
     set + requirement;
   - dispatch to the four `L2FetchStrategy`
     legs;
   - for `RowGroup(plan)`, fetch row-group
     batches, apply per-row `take` for each
     named row, return dense
     `ProjectedRecordBatch`.

2. Hit coalescing within a record: one fetch
   per record; row-group set is the union of
   mapped `RowGroupIndex` values.

3. Hit parallelism across records: bounded by
   `max_parallel_range_gets`.

4. Pre-056 records (no directory): adapter
   falls through to ADR 044 column fetch
   transparently; identity-axis output
   byte-identical.

Tests (`tests/row_group_secondary_index.rs`):

- Single hit on paged record fetches one row
  group and returns one row.
- Multiple hits on same record covering one
  row group coalesce to one fetch.
- Multiple hits on same record covering two
  row groups fetch both.
- Hits across records issue parallel fetches.
- Hit on a pre-056 record falls back to ADR
  044 column fetch with same row output.
- Storage-only columns required for visibility
  included and stripped.
- Schema fingerprint mismatch fails as
  corruption.

Exit:

- Secondary-index point reads benefit from
  row-group fetch; pre-056 records served
  correctly via fallback.

## Phase 8 — LSM, CDC, recovery, rehydration

Files:

- `src/storage/lsm_worker.rs`
- `src/storage/manager.rs`
- `src/storage/lsm.rs`
- `src/storage/cdc.rs` (guard test only)
- `src/storage/recovery.rs` (guard test only)
- `src/storage/rehydration.rs` (guard test only)
- `tests/row_group_lsm.rs` (new)
- `tests/row_group_neutral.rs` (new)

Work:

1. Plumb `RowGroupWriterConfig` through the LSM
   compaction worker (ADR 055 §5) to the slab
   writer used for compaction outputs.

2. Architectural assertion: compaction writer
   always uses the **same** writer config as
   admission writes (no per-compaction override
   in V1).

3. Determinism: row-group boundaries are
   deterministic given identical input bytes +
   identical config — same row-group splits,
   same stats, same byte ranges.

4. Compaction recomputes directories; never
   copies input directory bytes (architectural
   test).

5. Pre-056 → post-056 first-pass compaction:
   compaction output includes a directory even
   if input did not. Graceful upgrade path.

6. Visible-state digest invariance (ADR 055 §6)
   and per-version-entry digest (ADR 052)
   preserved under row-group boundary
   differences.

7. Secondary-index `RecordLocation.record_row_index`
   references unchanged across compaction.

8. Confirm CDC catchup keeps reading full
   record bodies per ADR 054.

9. Confirm recovery decodes whole record
   bodies; ignores row-group planning.

10. Confirm rehydration installs whole slabs;
    subsequent L1 reads find the directory
    and use row-group fetch path.

Tests (`tests/row_group_lsm.rs`):

- Compaction rewrites unpaged input into paged
  output when thresholds apply.
- Compaction rewrites paged input into fresh
  paged output with a freshly built directory
  (input directory not copied).
- Determinism: two replicas running compaction
  with identical configs produce
  byte-identical outputs.
- Compaction with writer config disabled
  produces no directory; output reads via ADR
  044 fallback.
- Visible-state digest unchanged across
  compaction with row-group boundary changes.
- Secondary-index lookup against compacted
  output fetches via row-group point-read
  path.

Tests (`tests/row_group_neutral.rs`):

- CDC subscription on directory-bearing slab
  emits the same events as on non-directory
  slab.
- Recovery from directory-bearing slab rebuilds
  redb indexes byte-identically.
- Rehydrated L1 reads pick up the directory
  and use row-group fetch automatically.

Exit:

- ADR 056 composes with LSM and remains
  byte-neutral to CDC, recovery, rehydration.

## Phase 9 — Metrics, audit, runbook, config

Files:

- `src/metrics/registry.rs`
- `src/audit/events.rs`
- `src/config.rs`
- `examples/wendelon.toml.example`
- `plans/ops/runbooks/row_group_fetch.md` (new)
- `plans/ops/runbooks/README.md`
- `plans/ops/observability.md`
- `tests/metrics_smoke.rs`

Work:

1. Register `storage_l2_row_group_fetch_*`
   metrics per Phase 0 §12 / ADR §16.

2. Add audit events:
   - `RowGroupFetchSampled { tier,
     row_groups_fetched, row_groups_total,
     predicate_fetched_groups,
     dominant_strategy }` (sampled,
     debug-grade);
   - `RowGroupInvariantViolation { kind }`
     (page-grade).

3. Add node-local config with `enabled =
   false` defaults. Phase 10 gates pass under the
   empirical writer-cost target, but rollout remains
   operator opt-in.

4. Operator runbook covering:
   - reading row-group vs. column-buffer vs.
     whole-record strategy distribution;
   - rollout coverage via
     `storage_l2_row_group_fetch_directory_bytes`;
   - tuning selected row-group + selected byte
     fractions;
   - diagnosing too-many-range fallbacks;
   - recognizing directory corruption as
     fail-closed (not fallback) — page-grade
     alarm;
   - cross-replica byte-equality
     reconciliation (config drift vs.
     corruption);
   - turning the writer config off as
     cluster-wide row-group-fetch disable.

5. Index runbook in
   `plans/ops/runbooks/README.md`.

Tests:

- Metric labels exactly match the bounded
  sets.
- Config validation rejects zero row-group
  sizes, max below target, fractions ≥
  1_000_000, zero range caps.
- Audit event names stable.
- Runbook is linked from runbooks index.

Exit:

- Operators can see row-group fetch use,
  savings, fallback distribution, and
  corruption signals.

## Phase 10 — Bench

Files:

- `benches/row_group_fetch.rs` (new)
- `Cargo.toml`
- `plans/impl/056_row_group_fetch_bench_results.md`

Bench scenarios:

1. **Point-read latency**: secondary-index
   lookup returning 1 row from legal record
   bodies, with and without page directory;
   L1 + L2; varying projection on wide and
   narrow legal fixtures.
2. **Selective scan latency**: 1% / 5% / 25%
   / 50% / 90% selective predicates; varying
   projection.
3. **Stats-pruned vs. predicate-fetched**:
   queries where stats fully resolve vs.
   where Phase B predicate fetch is needed.
4. **Compression overhead**: row-group LZ4
   ratio vs. single-batch ratio on integer /
   float / UTF-8 / mixed columns.
5. **Writer CPU cost**: time-to-emit slab
   body with and without paging, on
   fixed-width / variable-width / mixed
   schemas.
6. **Reader CPU cost**: time-to-decode N
   row-group batches vs. one whole-record
   batch.
7. **Planner overhead**: time-to-pick fetch
   strategy for a 100-record slab, with and
   without directories, varying selectivity.
8. **L2 GET-rate cost**: GET count for 1000
   secondary-index hits scattered across 100
   records, with and without coalescing.
9. **Pre-056 / post-056 mixed-slab read**:
   throughput on 50/50 mixed slab corpora.
10. **First-pass compaction byte cost**:
    pre-056 → post-056 compaction output size
    delta.
11. **Cross-replica determinism**:
    byte-identical compaction outputs on two
    replicas with matching configs.
12. **Directory size as fraction of record
    body**: legal wide and narrow fixtures.
13. **Phase A metadata window growth**:
    directory bytes vs. ADR 044's 64 KiB
    initial probe.
14. **CSB short-circuit interaction**:
    confirms row-group planner is bypassed
    when CSB skips the slab.
15. **`target_row_group_rows` tuning**:
    1024 / 4096 / 8192 default comparison.

Empirical acceptance targets:

- Point / sparse projected reads on legal
  fixtures fetch ≤ 5% of whole-record bytes
  for one selected group at 1-column
  projection.
- 5-of-50 projection at 5% row-group
  selectivity fetches ≤ 10% of whole-record
  bytes.
- Dense 90% selectivity falls back before
  Phase C.
- Directory size ≤ 2% of record body on
  legal wide and narrow fixtures.
- Compression ratio loss from row-group
  boundaries: ≤ 4% on integer / float; ≤ 2%
  on UTF-8.
- Writer CPU cost increase with paging
  enabled: ≤ 25% on the largest legal
  representative fixture
  (65,536 rows x 4 int64 columns).
  Threshold-margin wide fixtures are measured
  and documented as operator-facing cost
  warnings, not promotion blockers.
- Planner overhead for 100-record slab
  dispatch: ≤ 1 ms total on a single core.
- Cross-replica determinism: byte-identical
  compaction outputs under matching configs.
- PBC / AGG result equality against
  whole-record path across all fixtures.
- No regression on full-scan throughput when
  directories are present.
- No regression when `enabled = false`.

Bench results: Phase 10 accepted
`plans/impl/056_row_group_fetch_bench_results.md`
on 2026-05-06 with the empirical writer-cost
gate above. The original ≤ 15% writer-cost
target was provisional and revised after the
legal-fixture matrix established the cost floor.

Exit:

- Bench harness captures point-read,
  selective scan, stats-pruned vs.
  predicate-fetched savings, compression
  overhead, writer cost, planner overhead,
  GET-rate cost, determinism.

## Phase 11 — Architectural lockdown

Files:

- `tests/architecture_test.rs`

Guards (per ADR §17):

1. `RecordPageDirectory`,
   `RowGroupDirectoryEntry`,
   `BufferPageDirectoryEntry`,
   `RowGroupColumnStats` exactly-one
   definitions.
2. `RowGroupIndex`, `RowGroupRowCount`,
   `IpcColumnOrdinal` are newtype-units.
3. `RowGroupFetchPlan` exactly-one
   definition; not imported by CDC, PBC,
   recovery, catalog modules.
4. `L2FetchStrategy::RowGroup` present;
   dispatch sites match all four variants
   explicitly (no `_` catch-all).
5. `RowGroupRejectCode` taxonomy closed to
   three V1 codes.
6. `RowGroupFallbackReason` contains only
   economic / unsupported-layout reasons; no
   authentication / decode variants.
7. `storage_l2_row_group_fetch_*` metrics
   carry exactly the bounded labels.
8. No new `MsgType` containing `ROW_GROUP`
   or `PAGE_DIRECTORY`.
9. No new `FRAME_FLAG_*`.
10. No new `NodeCapability` variant.
11. No `ObjectStore` trait method addition.
12. No descriptor field for paging or stats
    policy.
13. CDC, recovery, rehydration code paths do
    not import `RecordPageDirectory` or
    `RowGroupFetchPlan`.
14. `Schema.custom_metadata` key literal is
    `storage.row_group_directory`.
15. Writer reads from `RowGroupWriterConfig`
    only, not from a descriptor field.
16. No envelope `format_version` bump.
17. No ADR 022 footer field addition.
18. No synthetic Arrow column for the
    directory.
19. No sparse Arrow array materializer for
    unfetched row groups.
20. `PageDirectoryParseError` and
    `RowGroupRangeOverflow` reach the
    storage-error path (corruption), never
    the `Fallback(...)` branch.
21. No `RowGroup*V[0-9]` /
    `PageDirectory*V[0-9]` Rust type
    suffixes.
22. No `Wendelon`-prefixed row-group
    identifiers.
23. Compaction writer recomputes
    directories; never copies input
    directory bytes.
24. No LZ4 frame block-independence flag
    mutation; row-group RecordBatch buffers
    use ADR 043's existing frame shape.
25. No custom partial-buffer materializer
    module / function in ADR 056's surface
    — row-group decode goes through stock
    Arrow IPC reader. Lockdown searches
    `src/` for `partial_buffer_materialize`,
    `slice_compressed_buffer`, or
    `intra_buffer_page_decode` symbols in
    `src/storage/row_group.rs` or
    `src/storage/columnar.rs`; if present,
    the substrate has drifted and the test
    fails closed. ADR 057 encoding cascades introduces
    intra-buffer page decode inside its
    own namespaced module
    (e.g. `src/storage/encoding/`) — this guard
    is scoped to keep that capability out
    of ADR 056's surface, not project-wide.
26. No `mini_page_*` field on
    `RowGroupWriterConfig`,
    `L2RowGroupFetchConfig`, or any
    `RecordPageDirectory` /
    `BufferPageDirectoryEntry` field in
    ADR 056's directory shape. ADR 057 encoding cascades will
    add a sibling
    `buffer_simd_pages: Vec<...>` field on
    `BufferPageDirectoryEntry` (or a
    sibling custom_metadata key like
    `encoding.simd_page_directory.v1`) per ADR
    056 §"Forward-compatibility analysis";
    that extension lands in a future ADR.
27. The Phase 5 materializer MUST NOT
    re-evaluate PBC predicates on Phase B
    columns. Phase B's per-group survival
    masks are authoritative for predicate-
    fetched groups; the materializer applies
    `arrow::filter` against the mask but
    must not call into the PBC executor for
    rows it has already classified.
    Lockdown searches the materializer code
    path for any post-Phase-B PBC executor
    call site and fails closed if found.

Exit:

- Future row-group edits cannot drift the
  substrate silently.

## Verification gate

```sh
cargo test --lib storage::row_group
cargo test --lib storage::columnar
cargo test --test row_group_writer --quiet
cargo test --test row_group_directory --quiet
cargo test --test row_group_pruning --quiet
cargo test --test row_group_fetch_plan --quiet
cargo test --test row_group_materialization --quiet
cargo test --test row_group_query --quiet
cargo test --test row_group_neutral --quiet
cargo test --test row_group_secondary_index --quiet
cargo test --test row_group_lsm --quiet
cargo test --test architecture_test --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench row_group_fetch --no-run
cargo bench --bench row_group_fetch -- --quick
cargo check --quiet
cargo check --benches --quiet
git diff --check
cargo fmt --check
```

Promotion-to-shipped gate (after Phase 1–11
land):

- All Phase 1–11 golden vectors pass.
- Architectural lockdown tests pass.
- Bench small / medium results pass empirical
  Phase 10 acceptance targets.
- ADR 044 column-fetch scenarios pass
  end-to-end with mixed-shape slab corpora
  (some directory-bearing, some not).
- ADR 052 secondary-index point-reads served
  via row-group fetch with byte-identical
  results to whole-record fetch.
- ADR 055 LSM compaction outputs deterministic
  across replicas under matching writer
  configs; visible-state and per-version-
  entry digests invariant.
- CDC, recovery, rehydration tests confirm
  byte-neutrality.
- Operator runbook
  `plans/ops/runbooks/row_group_fetch.md`
  finalized.

## File-by-file expected edits

(Phase 0 audits actual paths.)

- `src/schema/units.rs` — extend (~20 LOC for
  3 new newtype-units)
- `src/storage/row_group.rs` — new (~600 LOC:
  types, serialization, stats computation,
  classifier helpers)
- `src/storage/columnar.rs` — extend (~300
  LOC for `RecordBufferDirectory.page_directory`,
  `RowGroupFetchPlan`, planner four-leg
  dispatch, `L2FetchStrategy` extension)
- `src/storage/multirow.rs` — extend (~250
  LOC for multi-RecordBatch IPC emission)
- `src/storage/secondary_index.rs` — extend
  (~120 LOC for `fetch_indexed_rows`
  adapter)
- `src/storage/lsm_worker.rs` — touched
  (~30 LOC for config plumbing)
- `src/storage/manager.rs` — touched (~80
  LOC for fetch dispatch wiring)
- `src/storage/cdc.rs` — guard test only
- `src/storage/recovery.rs` — guard test only
- `src/storage/rehydration.rs` — guard test
  only
- `src/storage/csb.rs` — touched (~30 LOC;
  expose stats-supported-types audit)
- `src/schema/pbc.rs` — extend (~120 LOC for
  predicate classifier)
- `src/config.rs` — extend (~60 LOC for both
  configs)
- `src/metrics/registry.rs` — extend (~80
  LOC)
- `src/audit/events.rs` — extend (~30 LOC)
- `tests/architecture_test.rs` — extend
  (~200 LOC, 27 guards)
- 9 new test files (~250-450 LOC each)
- `benches/row_group_fetch.rs` — new (~500
  LOC)
- `plans/ops/runbooks/row_group_fetch.md` —
  new (~180 lines)

Total ≈ 5500 LOC. Mostly net-new in
`row_group.rs` + `multirow.rs` writer
extension; existing surfaces touched
lightly.

## V1 design summary

ADR 056 ships the row-group fetch substrate:

- **Row groups are Arrow IPC RecordBatches**
  — one slab record body becomes one IPC
  stream containing N row-group RecordBatch
  messages under one schema message.
- **`RecordPageDirectory` in IPC schema
  custom metadata** under key
  `storage.row_group_directory` — encrypted
  with the record body, authenticated by ADR
  020 chunk AEAD; carries row-group entries,
  per-buffer page entries (the "per-buffer"
  axis from the roadmap wording), and
  per-row-group min / max / null-count stats
  for ADR 039-supported types.
- **Three-phase fetch**: A (metadata +
  directory) → B (stats prune + predicate
  fetch when needed) → C (projected fetch
  over surviving groups).
- **`L2FetchStrategy` four-leg dispatch**:
  RowGroup / Columnar / WholeRecord /
  Fallback; fallbacks carry only economic /
  unsupported-layout reasons (no
  authentication / decode variants).
- **Conservative PBC predicate classifier**:
  equality, range, IS NULL, IS NOT NULL.
  Conjunction (AND) skips when **any** branch
  proves disjoint — unsupported sibling
  branches do not block. Disjunction (OR)
  skips only when **every** branch proves
  disjoint; any unsupported branch keeps the
  group. Top-level unsupported predicates
  (LIKE, IN, BETWEEN-with-cast on their own)
  keep all groups.
- **Three-stage row-group classification**:
  Skip / Keep / NeedsPredicateFetch.
- **Dense `ProjectedRecordBatch` outputs
  only** — never sparse arrays with nulls
  for unfetched row groups.
- **No new compression codec or LZ4 frame
  flag change** — row-group RecordBatch
  boundaries provide per-batch independence
  within ADR 043's existing per-buffer
  LZ4_FRAME.
- **Stock Arrow IPC reader handles
  decode** — no custom partial-buffer
  materializer.
- **Writer-side gated** by node-local
  `RowGroupWriterConfig` (default off in
  initial rollout, deployment-wide for
  cross-replica byte-equality);
  **reader-side automatic on directory
  presence**.
- **ADR 044 substrate reused**:
  `TieredMultiRangeReadPlan`,
  `RecordBufferDirectory` (extended with
  optional `page_directory`),
  `L2ColumnFetchConfig` (sister
  `L2RowGroupFetchConfig` for row-group-
  specific knobs).
- **ADR 052 secondary-index integration**
  via `fetch_indexed_rows` adapter.
- **ADR 055 LSM compaction integration**:
  compaction outputs include directories
  under matching config; deterministic
  across replicas; visible-state digest
  invariant.
- **CDC, recovery, rehydration
  byte-neutral** (read full IPC stream).
- **Authentication / parse failures route
  to storage-error path**, never silent
  fallback.
- **Architectural lockdown enforces the
  substrate invariants** (27 guards in
  `architecture_test.rs`, including two
  guards keeping partial-buffer
  materializer modules and `mini_page_*`
  config fields out of ADR 056's surface
  — those land in ADR 057 encoding cascades's namespaced
  module).
- **Intra-buffer SIMD pages deferred to
  ADR 057 encoding cascades** — brainstorm 011 §3 Lance idea
  ships inside the encoding-cascade payload.

## Deferred to follow-up ADRs

- Intra-buffer SIMD mini-pages (sub-row-
  group) (ADR 057 encoding cascades; brainstorm 011 §3 Lance
  idea).
- Variable per-buffer mini-page row count
  (ADR 057 encoding cascades).
- `RecordPageDirectory` cache across
  queries.
- Bulk slab-level page-directory pre-fetch.
- Compute-over-compressed-data (COCD) at
  row-group grain (COCD).
- Per-row-group DEK rotation.
- Wire-tier row-group fetch (ADR 053
  extension).
- Public Toron `PROJECT_ROWS` opcode (or
  equivalent).
- Variable row-group byte budget (hybrid
  byte-and-row).
- Cross-record / cross-slab row-group
  abstractions (LSM-level sort orders).
- Per-row-group encryption-domain
  isolation.
- Row-group stats sketches (HLL, t-digest,
  etc.).
- Adaptive row-group sizing per record
  based on schema width / cardinality.
