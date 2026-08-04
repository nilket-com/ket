# Implementation Plan: ADR 060 — Sparse Projection Fetch Narrowing

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR:
`plans/adr/0060_sparse_projection_fetch_narrowing.md`

Inherits substrate:
- `plans/adr/0056_row_group_fetch_and_page_directory.md` —
  three-stage fetch planner (directory probe →
  predicate evaluation → projection fetch);
  `RowGroupFetchPlan` shape; `max_ranges_per_record`
  budget; range coalescing;
  `selected_row_group_record_body_ranges(...)`
  projection-fetch range computation; tiered multi-
  range read planner.
- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md` —
  `EncodingDirectory`,
  `BufferEncoding.page_end_offsets`,
  `BufferEncoding.page_value_count`,
  `EncodingPageDescriptor`, `decode_page`,
  `expand_page_descriptors`, custom partial-buffer
  materializer in `src/storage/encoding/`.
- `plans/adr/052` — secondary-index encoded-page
  fast path in `src/storage/manager.rs`
  (`build_encoding_indexed_page_plan(...)`,
  `materialize_encoding_indexed_pages(...)`,
  `pick_point_row_group_fetch_strategy(...)`); the
  shared helper extraction target.
- `plans/adr/0058_compute_over_encoded_data.md` —
  `EncodedRowMask` enum; reject + fallback
  taxonomies; `PhaseBOutcome.survival_masks` shape
  (current name; sweep target →
  `PredicateOutcome.survival_masks`).
- `plans/adr/0059_native_cocd_kernels.md` — native
  COCD kernels (encoded predicate-evaluation mask
  producer for the sparse-engaged path).
- `plans/conventions/no-planning-phase-names.md` —
  new identifiers must use semantic names.
- `plans/conventions/` (full set) — newtype-unit
  rule, no version suffix, no premature backward
  compat, capability vs feature naming.

## Status

Council-converged, implemented, and promoted to
shipped. Full Phase 7 bench evidence at
`plans/impl/0060_sparse_projection_fetch_narrowing_bench_results.md`
clears the shipped-status gate for the council-
chosen default scope: `BitPacked` and `Dictionary`
sparse projection fetch are default-engaged when
the plan-time economics choose them; RLE remains
default-off after Phase 7 evidence and is recorded
as follow-up ADR territory.

Implementation contract — council-converged:

- per-row-group decision grain with cross-column
  geometry agreement;
- V1 default sparse projection engagement set:
  `BitPacked`, `Dictionary { inner: Plain | BitPacked }`;
  RLE is correctness-eligible per codec
  verification but remains default-off by Phase 7
  disposition, non-blocking for `BitPacked` /
  `Dictionary` promotion;
- V1 engages only for exact-COCD masks
  (`decoded_predicate_batches` empty);
- no new config fields, no new metric labels;
- existing economic caps + internal per-buffer
  fragmentation budget
  (`MAX_SPARSE_RANGES_PER_BUFFER` helper-local
  `const`);
- strict sparse-bytes < dense-bytes plan-time
  gate;
- byte-identical projected-batch invariant pinned
  by property suite;
- two-step dispatch discipline (eligibility →
  fail-closed reject post-fetch);
- two-module split: `page_materialize.rs` (decode
  + assembly helpers, shared with ADR 052) +
  `sparse_projection_fetch.rs` (planner +
  decision + economic logic);
- helper extraction from `manager.rs` first;
- internal `SparseProjectionFetchDecline` reasons
  never escape to public surfaces.

## Goal

Use survival masks from exact-COCD predicate
evaluation to narrow projection fetches from whole
row-group buffers to selected ADR 057 encoded
pages when (1) the selected rows are clustered,
(2) every required projected column is page-
materializable under the V1 eligibility set with
cross-column page-geometry agreement, and (3) the
estimated sparse encoded byte count is strictly
smaller than the full-row-group projection-fetch
byte count.

Today the manager can:

- fetch full row-group projection buffers via
  `selected_row_group_record_body_ranges(...)`;
- apply survival masks after decoding full row-
  group batches via
  `apply_phase_b_masks_to_projected_row_groups(...)`
  (current name; sweep target →
  `apply_survival_masks_to_projection_batches`);
- fetch individual encoded SIMD pages for
  secondary-index point rows via
  `build_encoding_indexed_page_plan(...)` /
  `materialize_encoding_indexed_pages(...)`.

ADR 060 adds the missing mask-driven bridge:

```text
exact-COCD survival mask
  → selected encoding pages per surviving row group
  → sparse projection-fetch multi-range plan
  → decode selected projection pages
  → apply page-local mask slices
  → dense ProjectedRecordBatch byte-identical to dense path
```

The full-row-group projection-fetch path remains
the production fallback and test oracle. The
shared page-materialize helper extracted in Phase
1 is reused by ADR 052 named-row materialization
and ADR 060 mask-driven materialization. Sparse
planner + decision live in their own module.

## Deliverables

- `src/storage/encoding/page_materialize.rs` (new)
  — shared encoded-page **materialization**
  helpers (extracted from `manager.rs`):
  - fixed-width byte-width detection;
  - encoded page body range calculation;
  - page decode through `decode_page`;
  - page-scoped `RecordBufferDirectory`
    construction;
  - projected page-batch assembly for fixed-width
    non-null columns.
- `src/storage/encoding/sparse_projection_fetch.rs`
  (new) — sparse projection-fetch **planner +
  decision + economic logic**:
  - `RowToPageMap` primitive (rows ↔ pages bridge
    derived from
    `BufferEncoding.page_value_count` /
    `page_end_offsets`);
  - `ActivePageSet` enum
    (`None` / `All` /
    `Sparse(Vec<EncodingPageIndex>)`);
  - `SparseProjectionFetchPlan` /
    `SparseProjectionFetchGroup` /
    `SparseProjectionPageFetch` planner types
    (`pub(crate)`);
  - `SparseProjectionFetchDecision` /
    `SparseProjectionFetchDecline` (internal-
    only; never surface as public fallback
    variants);
  - `decide_sparse_projection_fetch(...)` per-
    row-group decision entry point;
  - `MAX_SPARSE_RANGES_PER_BUFFER` helper-local
    `const` (per-buffer fragmentation budget;
    bench-tuned starting value).
- `src/storage/encoding/mod.rs` — re-export both
  modules.
- `src/storage/manager.rs` — wire sparse
  projection fetch before the existing full-row-
  group projection-fetch call; retain existing
  dense path as fallback; refactor ADR 052 named-
  row path to call shared helpers from
  `page_materialize.rs`.
- `src/storage/row_group.rs` — minimal additions
  for sparse plan integration into
  `RowGroupFetchPlan`; no new strategy variant,
  no shape change to `surviving_groups`.
  (Existing `phase_c_fetch` field name retained
  for ADR 060 scope; renamed in the broader
  convention sweep.)
- `tests/row_group_sparse_projection_fetch.rs` (new) —
  property suite + planner unit tests + dense-vs-
  sparse oracle tests.
- `tests/encoding_secondary_index.rs` /
  `tests/row_group_secondary_index.rs` — guard
  extracted helper reuse; existing tests pass
  byte-identical post-extraction.
- `tests/row_group_fetch_plan.rs` /
  `tests/row_group_materialization.rs` —
  extension cases for sparse decisions.
- `tests/architecture_test.rs` — incremental
  Phase 6 guards.
- `benches/row_group_fetch.rs` — extended with
  sparse vs dense vs point-read three-way
  comparison + bytes-fetched counter (clustered +
  random + all/none mask matrix; explicit
  predicate-mask source axis;
  `MAX_SPARSE_RANGES_PER_BUFFER` tuning row).
- `plans/impl/0060_sparse_projection_fetch_narrowing_bench_results.md`
  — checked-in evidence artifact (lands phase-
  by-phase as eligibility classes clear gates).
- `plans/adr/0056_row_group_fetch_and_page_directory.md` —
  §"Where we are" amendment (Phase 8 only).
- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md` —
  §"Where we are" amendment (Phase 8 only).
- `plans/wip/roadmap.md` — planning-cadence
  update at promotion (Phase 8 only).

## Non-goals

- No new public PBC opcodes;
  `src/schema/pbc.rs::Op` byte-identical post-
  implementation.
- No `RecordBodyCompressionPolicy::SparseFetch`
  descriptor variant.
- No new `L2FetchStrategy` variant.
- No new `EncodingComputeEvaluatorKind` /
  `EncodedAggregateKind` /
  `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` /
  `PredicateAtomOp` / `EncodedRowMask` /
  `EncodingRejectCode` / `EncodingFallbackReason`
  / `RowGroupFallbackReason` /
  `RowGroupRejectCode` / `EncodingCascadeKind`
  variants.
- No new fields in `L2EncodingFetchConfig`,
  `L2RowGroupFetchConfig`,
  `EncodingComputeConfig`, `EncodingWriterConfig`.
- No `sparse_projection_fetch_*` config field in
  `Config`. Per-buffer fragmentation budget is a
  `const` in `sparse_projection_fetch.rs`, not a
  `Config` field.
- No per-cascade `enabled_*` flag.
- No per-record activation knob.
- No Cargo feature gating sparse narrowing.
- No nightly rustc adoption.
- No `Fsst` / `Alp` / `Delta` / `Plain` /
  `Lz4Frame` sparse path.
- No nullable fixed-width sparse path (V1 routes
  to dense; Q2).
- No variable-width sparse path (V1 routes to
  dense).
- No ADR 057 directory shape extension.
- No new wire `MsgType` / `FRAME_FLAG_*` /
  `NodeCapability` bit / envelope
  `format_version` / `ObjectStore` trait change.
- No new metric label.
- No silent dense fallback after sparse fetch
  begins.
- No retirement of dense projection-fetch path
  from production code.
- No sparse predicate-column fetch (own ADR).
- No dense predicate-column zip with sparse
  projection in V1 (own ADR; Q3).
- No cross-row-group sparse coalescing beyond
  `max_ranges_per_record`.
- No sparse aggregate materialization.
- No mixed secondary-index named-row + sparse
  projection-fetch composition.
- No CDC-side filtering.
- No learned/adaptive sparse routing.
- No internal decline reasons leaking to public
  surface (metrics / fallback / reject).
- No new planning-phase identifiers on new
  symbols.

## Phase 0 — Contract freeze + current-seam snapshot

Lock the closed surface and snapshot pre-060
state before any sparse-narrowing code lands.
Read-only audit; no code changes.

Files:

- `plans/adr/0060_sparse_projection_fetch_narrowing.md`,
  `plans/impl/0060_sparse_projection_fetch_narrowing.md`
  (council-converged text; no code changes in
  Phase 0).
- `src/storage/row_group.rs`,
  `src/storage/manager.rs`,
  `src/storage/encoding/`,
  `tests/architecture_test.rs` (read-only
  audit).

Work:

1. Confirm ADR 060 surface is internal to the
   projection-fetch path — no public catalog
   mutation.
2. Snapshot current relevant seams (file-digest
   pin). Several are known planning-phase naming
   leaks slated for the broader convention sweep;
   ADR 060 must not extend them:
   - `RowGroupFetchPlan`,
     `RowGroupRangeFetchPlan`;
   - `PhaseBOutcome` (sweep target →
     `PredicateOutcome`; especially
     `survival_masks` and
     `decoded_predicate_batches`);
   - `selected_row_group_record_body_ranges(...)`
     /
     `selected_row_group_record_body_ranges_excluding_requirement(...)`;
   - `decode_projected_row_groups_from_plaintext_ranges(...)`;
   - `apply_phase_b_masks_to_projected_row_groups(...)`
     (sweep target →
     `apply_survival_masks_to_projection_batches`);
   - `zip_phase_b_and_phase_c_projected_row_group(...)`
     (sweep target →
     `zip_predicate_and_projection_columns`);
   - `build_encoding_indexed_page_plan(...)`,
     `materialize_encoding_indexed_pages(...)`,
     `pick_point_row_group_fetch_strategy(...)`
     (ADR 052 secondary-index path; helper-
     extraction source);
   - `crypto.plan_tiered_multi_range_read(...)`
     and
     `record_body_ranges_to_record_ranges(...)`;
   - `EncodingDirectory`, `BufferEncoding`
     (especially `page_end_offsets` +
     `page_value_count`),
     `EncodingPageDescriptor`
     (`first_value_index`, `value_count`),
     `decode_page` /
     `expand_page_descriptors`.
3. Confirm no public catalog extension is
   needed: `EncodingCascadeKind`,
   `EncodingComputeEvaluatorKind`,
   `EncodedRowMask`, `RowGroupFallbackReason`,
   `EncodingFallbackReason`.
4. **Verify codec page-row-count semantics.**
   `BufferEncoding.page_value_count` is
   declared at
   `src/storage/encoding/types.rs:185` as
   `SimdPageValueCount`. Set per-buffer at
   directory build
   (`src/storage/encoding/directory.rs:160`),
   read at materialize
   (`src/storage/encoding/page.rs:21`) for
   every page-emitting cascade including RLE.
   Row→page mapping is deterministic from
   directory metadata regardless of cascade
   shape.
5. Record V1 conservative eligibility:
   - fixed-width projection columns only;
   - non-null projected columns only;
   - V1 default sparse projection engagement set:
     `BitPacked`,
     `Dictionary { inner: Plain | BitPacked }`;
     RLE row→page mapping is correctness-eligible
     but default-off after Phase 7 evidence;
   - selected page geometry must match across
     every required value buffer in a row
     group;
   - exact-COCD predicate evaluation only;
   - dense predicate-column zip stays on
     existing path.
6. Confirm `target_simd_page_values = 1_024` is
   the ADR 057-shipped default; cross-replica
   `EncodingWriterConfig` byte-equality
   dependency noted.

**Exit gate:** Council agrees the draft does
not mutate ADR 056 / 057 / 058 / 059 public
surfaces. Pre-060 surface inventory recorded;
per-row-group + exact-COCD-only V1 scope
locked; RLE row→page mapping verified at code
level.

## Phase 1 — Extract encoded page materialization helpers

The ADR 052 secondary-index page path currently
owns useful logic in `src/storage/manager.rs`.
ADR 060 should not duplicate it. Extract the
generic pieces into a storage/encoding helper
module while preserving secondary-index behavior
byte-for-byte.

Files:

- `src/storage/encoding/page_materialize.rs`
  (new).
- `src/storage/encoding/sparse_projection_fetch.rs`
  (new — empty / planner-shell module until
  Phase 2; pinned by guard so the module
  boundary is settled early).
- `src/storage/encoding/mod.rs`.
- `src/storage/manager.rs`.
- `tests/encoding_secondary_index.rs`.
- `tests/row_group_secondary_index.rs`.
- `tests/architecture_test.rs`.

Work:

1. Move (or wrap as `pub(crate)` re-exports) the
   generic helpers from `manager.rs` into
   `page_materialize.rs`:
   - fixed-width byte-width detection per Arrow
     `DataType`;
   - encoded page body range calculation from
     `BufferEncoding.page_end_offsets`;
   - page decode entry that calls `decode_page`
     against an `EncodingPageDescriptor`;
   - page-scoped `RecordBufferDirectory`
     construction;
   - projected page-batch assembly for fixed-
     width non-null columns.
2. Keep secondary-index-specific row selection
   logic in `manager.rs` (or in a small named-
   row adapter inside `page_materialize.rs`):
   - index hit → page lookup;
   - row offset within page;
   - final named-row `take_rows`.
3. Preserve existing secondary-index tests
   byte-for-byte. The named-row encoded-page
   fast path must produce the same
   `ProjectedRecordBatch` post-extraction.
4. Add architecture guard that page-materialize
   helpers live under `src/storage/encoding/`,
   not as new public protocol types.
5. Keep planner decisions out of
   `page_materialize.rs`.
   `page_materialize.rs` owns page decode and
   batch assembly; `sparse_projection_fetch.rs`
   owns mask-to-page planning and economic
   decisions.

Tests:

```sh
cargo test --test encoding_secondary_index
cargo test --test row_group_secondary_index
cargo test --test architecture_test
```

**Exit gate:** No behavior change. Secondary-
index encoded-page fast path still passes byte-
for-byte. Plan shape unchanged. Module
boundaries pinned for Phase 2+ sparse work.

## Phase 2 — `RowToPageMap` + `ActivePageSet` primitives

Add the row→page bridge and the mask-walker
primitives in `sparse_projection_fetch.rs`. Pure
plan-time computation; no I/O.

Files:

- `src/storage/encoding/sparse_projection_fetch.rs`.
- `tests/row_group_sparse_projection_fetch.rs` (new).

Suggested types:

```rust
pub(crate) struct RowToPageMap {
    pub pages: Vec<EncodingPageDescriptor>,
    pub row_count: u32,
}

pub(crate) enum ActivePageSet {
    None,
    All,
    Sparse(SmallVec<[EncodingPageIndex; 16]>),
}

impl RowToPageMap {
    pub fn from_buffer_encoding(
        encoding: &BufferEncoding,
    ) -> Result<Self, EncodingRejectCode> {
        // Wraps existing expand_page_descriptors;
        // page_value_count drives row spans, last
        // page partial fallback handled by
        // existing helper.
        ...
    }

    pub fn page_for_row(&self, row: u32) -> Option<EncodingPageIndex> { ... }
    pub fn pages_for_row_range(&self, rows: Range<u32>) -> Range<EncodingPageIndex> { ... }
}
```

Work:

1. Build `RowToPageMap` from `BufferEncoding`
   metadata via `expand_page_descriptors` —
   uniform helper for every page-emitting
   cascade (BitPacked, Dictionary, Rle).
2. Mask walker —
   `mask_to_active_pages(mask: &EncodedRowMask,
   map: &RowToPageMap) -> ActivePageSet`:
   - `All { rows }` → `ActivePageSet::All`;
   - `None { rows }` → `ActivePageSet::None`;
   - `Bitmap { rows, bytes }` → iterate set
     bits; `page_for_row(bit)` per bit; ordered
     dedup into `SmallVec`;
   - `Runs { rows, runs }` → for each run
     `[start, end)`, take
     `pages_for_row_range(start..end)`; union
     into ordered set.
3. Property tests:
   - Walker output equals brute-force "decode
     mask, compute page indices, dedup" for
     every fixture.
   - `Runs` and `Bitmap` walkers produce
     identical `ActivePageSet` for the same
     logical row selection.
   - `RowToPageMap` correctness across partial
     last pages.

Tests:

- BitPacked at 1024 rows/page across 8K / 65K /
  1M.
- Dictionary inner index sub-buffer arithmetic
  correct.
- RLE byte-run buffer with `page_value_count`
  set per-buffer; row→page mapping
  deterministic.
- Bitmap density 1% / 5% / 25% — selected page
  count matches brute force.
- Runs with 1 / 4 / 16 — selected page set
  deduplicates correctly.
- Edge case: `Runs { runs: [] }` →
  `ActivePageSet::None`.

**Exit gate:** Primitives land; pure plan-time;
byte-equivalent to brute force on test
fixtures.

## Phase 3 — Sparse decision + planner

Combine `RowToPageMap` + `ActivePageSet` +
cross-column geometry validation + strict byte-
comparison gate + per-buffer fragmentation
budget into a per-row-group decision.

Files:

- `src/storage/encoding/sparse_projection_fetch.rs`.
- `src/storage/row_group.rs`.
- `tests/row_group_sparse_projection_fetch.rs`.

Suggested types:

```rust
pub(crate) struct SparseProjectionFetchPlan {
    pub selected_groups: Vec<RowGroupIndex>,
    pub group_pages: Vec<SparseProjectionFetchGroup>,
    pub fetches: Vec<SparseProjectionPageFetch>,
}

pub(crate) struct SparseProjectionFetchGroup {
    pub row_group_index: RowGroupIndex,
    pub selected_pages: Vec<EncodingPageIndex>,
}

pub(crate) struct SparseProjectionPageFetch {
    pub row_group_index: RowGroupIndex,
    pub page_index: EncodingPageIndex,
    pub ipc_column_ordinal: IpcColumnOrdinal,
    pub ipc_buffer_ordinal: IpcBufferIndex,
    pub body_range: RecordBodyByteRange,
    pub page: EncodingPageDescriptor,
    pub cascade: EncodingCascade,
    pub data_type: DataType,
    pub bytes_per_value: usize,
}

pub(crate) enum SparseProjectionFetchDecision {
    Sparse(SparseProjectionFetchPlan),
    Decline(SparseProjectionFetchDecline),
}

pub(crate) enum SparseProjectionFetchDecline {
    AllRowsMask,
    MissingExactCocdMask,
    DensePredicateBatchPresent,
    CascadeNotPageEmitting,
    CascadeNotProjectionEligible, // Fsst / Alp / etc., plus RLE in V1 default-off routing
    NullableProjectedColumn,
    VariableWidthProjectedColumn,
    PageGeometryMismatchAcrossColumns,
    ByteEstimateNotSparse, // sparse_bytes >= dense_bytes
    SimdPageCapExceeded,
    RangeCapExceeded,
    PerBufferFragmentationCapExceeded,
    SelectedByteFractionExceeded,
    Disabled,
}

const MAX_SPARSE_RANGES_PER_BUFFER: usize = 8;
// Phase 7 bench-selected starting value. Helper-
// local; not a Config field.
```

Decline reasons are test and routing aids only;
they must not become public fallback, reject, or
metric-label variants.

Work:

1. Decision flow:
   - both `l2_row_group_fetch.enabled` and
     `l2_encoding_fetch.enabled` must be true
     → else `Decline(Disabled)`;
   - `phase_b_outcome.decoded_predicate_batches`
     non-empty →
     `Decline(DensePredicateBatchPresent)`;
   - exact-COCD survival mask not present →
     `Decline(MissingExactCocdMask)`;
   - **inspect the survival mask first**: `None`
     → drop the row group from the projection-
     fetch set entirely (no fetches; this is a
     zero-byte short-circuit, not a decline that
     routes to dense — a no-survivor group must
     never trigger projection bytes regardless of
     its projection-column shape); `All` →
     `Decline(AllRowsMask)` (route to dense —
     full-row-group projection fetch handles
     `All` masks regardless of cascade);
   - any required projected column nullable →
     `Decline(NullableProjectedColumn)`;
   - any required projected column variable-
     width →
     `Decline(VariableWidthProjectedColumn)`;
   - any required projected column non-page-
     emitting (Plain / Lz4 / Delta / Fsst /
     Alp) → `Decline(CascadeNotPageEmitting)`;
   - any required projected column outside the
     V1 default projection set after Phase 7
     RLE routing decision (e.g., RLE if
     default-off) →
     `Decline(CascadeNotProjectionEligible)`;
   - cross-column page-geometry mismatch →
     `Decline(PageGeometryMismatchAcrossColumns)`;
   - mask → `ActivePageSet::Sparse(...)` (only
     `Bitmap` / `Runs` reach this step; `None` /
     `All` already short-circuited above);
   - selected pages >
     `max_simd_pages_per_record` →
     `Decline(SimdPageCapExceeded)`;
   - per-buffer coalesced range count >
     `MAX_SPARSE_RANGES_PER_BUFFER` →
     `Decline(PerBufferFragmentationCapExceeded)`;
   - record-level coalesced range count >
     `max_ranges_per_record` →
     `Decline(RangeCapExceeded)`;
   - selected byte fraction exceeded →
     `Decline(SelectedByteFractionExceeded)`;
   - sparse_bytes ≥ dense_bytes →
     `Decline(ByteEstimateNotSparse)`;
   - else: `Sparse(plan)`.

   **Mask-first ordering rationale:** `None`
   masks must short-circuit *before* projection-
   column eligibility checks. Otherwise a no-
   survivor group with a nullable / variable-
   width / non-page-emitting projected column
   would flow through `Decline(NullableProjectedColumn)`
   et al. and then route to dense projection
   fetch — wasting an entire row-group's worth of
   bytes only to produce a zero-row filtered
   batch. The decision flow inspects the mask
   first and drops `None` groups outright.
2. `decide_sparse_projection_fetch(
   predicate_outcome,
   projection_requirement,
   row_group_metadata,
   l2_row_group_fetch_config,
   l2_encoding_fetch_config,
   ) -> SparseProjectionFetchDecision`
3. Decline reason internal observability only;
   never escapes.
4. Pure plan-time — no I/O, no buffer body
   reads.

Tests:

- BitPacked + Bitmap @ 1% sel + 1024 rows/page
  → `Sparse` with ≤ 1 selected page per buffer.
- BitPacked + Bitmap @ 50% sel @ 65K rows →
  `Decline(ByteEstimateNotSparse)`.
- BitPacked + Runs spanning every other page →
  `Decline(PerBufferFragmentationCapExceeded)`
  if budget exceeded.
- RLE byte-run + clustered Runs with default V1
  routing →
  `Decline(CascadeNotProjectionEligible)`. RLE
  page mapping is correctness-eligible, but Phase
  7 settles the default route as off.
- Plain cascade →
  `Decline(CascadeNotPageEmitting)`.
- Mixed projection (BitPacked + Plain in same
  row group) →
  `Decline(CascadeNotPageEmitting)` for the
  group.
- Cross-column page-geometry mismatch →
  `Decline(PageGeometryMismatchAcrossColumns)`.
- Disabled config → `Decline(Disabled)` always.
- Dense predicate batch present →
  `Decline(DensePredicateBatchPresent)`.
- `None` mask + nullable projected column →
  group dropped (zero bytes fetched; no decline
  variant). The mask-first ordering means the
  no-survivor group must short-circuit before
  the projection-column eligibility check;
  otherwise the group would route to dense and
  fetch an entire row-group's worth of bytes
  only to filter to zero rows.
- `None` mask + non-page-emitting cascade
  (Plain / Lz4 / Delta) → group dropped (same
  short-circuit invariant).

**Exit gate:** Per-row-group decision lands;
returns either complete sparse plan or decline
reason **before any L2 fetch occurs**.

## Phase 4 — Sparse projection-fetch planning

Wire the sparse plan into the existing tiered
multi-range planner.

Files:

- `src/storage/manager.rs`.
- `src/storage/row_group.rs`.
- `src/storage/encoding/sparse_projection_fetch.rs`.
- `tests/row_group_sparse_projection_fetch.rs`.

Work:

1. After predicate evaluation has produced
   encoded masks and before current projection-
   fetch range selection, attempt sparse
   planning when:
   - `phase_b_outcome.survival_masks` is non-
     empty;
   - `phase_b_outcome.decoded_predicate_batches`
     is **empty** (exact-COCD predicate
     evaluation — V1 scope);
   - the projection requirement is non-empty;
   - both `l2_row_group_fetch.enabled` and
     `l2_encoding_fetch.enabled` are true.
2. Build sparse selected body ranges from per-
   `SparseProjectionPageFetch` entries.
3. Coalesce body ranges using existing range
   coalescing.
4. Convert body ranges to record ranges via
   `record_body_ranges_to_record_ranges(...)`.
5. Run
   `crypto.plan_tiered_multi_range_read(...)`.
6. Fill the projection-fetch plan field
   (currently
   `RowGroupFetchPlan.phase_c_fetch`;
   convention sweep target →
   `projection_fetch`) from the sparse tiered
   plan.
7. If sparse planning declines, continue with
   the existing
   `selected_row_group_record_body_ranges_excluding_requirement(...)`
   full-row-group path.

Important:
`RowGroupFetchPlan.surviving_groups` remains
row-group grain. The fact that projection
fetch is sparse is represented by
`phase_c_fetch.selected_plaintext_ranges` plus
the internal `SparseProjectionFetchPlan`; **no
new strategy variant**, **no new field on the
public plan shape**.

Tests:

- A clustered mask causes
  `phase_c_fetch.selected_plaintext_ranges`
  not to cover the whole projected row-group
  value buffer.
- A random mask touching every page uses the
  current full-row-group ranges (sparse
  declines on byte-comparison).
- A clustered mask that is byte-small but
  range-fragmented declines before tiered
  range planning.
- A predicate-only read with empty projection
  requirement still does no projection fetch.
- Existing row-group no-survivors path
  remains unchanged.
- `L2FetchStrategy` metrics still report
  `row_group`.
- `decoded_predicate_batches` non-empty (dense
  predicate evaluation) routes to existing
  dense path; sparse declines with
  `DensePredicateBatchPresent`.

**Exit gate:** Sparse fetch plans are
observable through smaller projection-fetch
ranges; fallback plans byte-identical to pre-
ADR-060 plans.

## Phase 5 — Sparse page materialization

Decode selected projection pages and apply
page-local survival-mask slices.

Files:

- `src/storage/encoding/sparse_projection_fetch.rs`.
- `src/storage/encoding/page_materialize.rs`.
- `src/storage/row_group.rs`.
- `src/storage/manager.rs`.
- `tests/row_group_sparse_projection_fetch.rs`.
- `tests/row_group_materialization.rs`.

Work:

1. For each `SparseProjectionPageFetch`,
   extract its plaintext bytes from the sparse
   projection-fetch plaintext extracts.
2. Decode each page with the existing ADR 057
   `decode_page` path (called via the
   `page_materialize.rs` helper).
3. Build a page-scoped
   `ProjectedRecordBatch` for the required
   projected/storage columns of that page
   (reusing the `page_materialize.rs` helper).
4. Derive the page-local mask slice from the
   group-level survival mask:
   - page first row =
     `EncodingPageDescriptor.first_value_index`;
   - page row count =
     `EncodingPageDescriptor.value_count`;
   - page mask length must equal page row
     count (assert; mismatch returns
     `EncodingRejectCode::DecodedLengthMismatch`).
5. Apply the page-local mask
   (`arrow::compute::filter`-equivalent on
   the page-scoped batch).
6. Concatenate filtered page batches in
   `(row_group_index, page_index)` order.
7. Concatenate groups in row-group order.
8. **Bypass the standard final survival-mask
   application** for sparse outputs — sparse
   materialization has already applied page-
   local masks. Double-application would
   corrupt row counts.

Correctness constraints:

- no PBC re-evaluation;
- no row reordering;
- no duplicate rows;
- storage column mapping preserved;
- empty output uses the same schema/ordinal
  mapping shape as current masked
  materialization.

Tests:

- Sparse output equals full-row-group
  projection fetch followed by
  `apply_phase_b_masks_to_projected_row_groups(...)`
  byte-for-byte for every BitPacked +
  Dictionary fixture; RLE fixtures verify the
  default-off eligibility route plus `None` mask
  short-circuit behavior.
- Sparse execution does not double-apply the
  survival mask.
- First / middle / last page selections
  preserve row order.
- Multi-group sparse output preserves group
  order.
- Empty selected page set returns an empty
  projected batch.
- Storage-only requirements materialize
  correctly.
- Corrupt page range after sparse fetch
  begins returns
  `EncodingRejectCode::PageRangeOverflow`
  (no silent dense fallback).
- Materialize-time invariant violation
  (`page_end_offsets` mismatch with fetched
  bytes) returns `DecodedLengthMismatch`.

**Exit gate:** Sparse materialization passes
byte-equivalence tests against the dense
oracle on the full V1 cascade matrix.

## Phase 6 — COCD integration + architecture guards + metrics discipline

Files:

- `src/storage/manager.rs`.
- `tests/encoding_compute_test.rs`.
- `tests/encoding_compute_kernel_invariants.rs`.
- `tests/row_group_sparse_projection_fetch.rs`.
- `tests/architecture_test.rs`.
- `src/metrics/registry.rs` (only if existing
  metrics need helper reuse).

COCD integration:

1. In the encoded predicate-evaluation branch,
   retain the survival masks as the sparse
   planner input.
2. Engage sparse projection fetch only after
   each encoded leaf passes existing
   selectivity checks and the combined mask
   passes mask-byte caps.
3. Require `decoded_predicate_batches` empty
   (exact-COCD path).
4. For point reads, preserve the existing
   early no-survivor short-circuit.
5. For selected indexed rows + predicate, do
   not mix the secondary-index named-row path
   with sparse projection fetch in this phase.

Architectural lockdown guards:

1. File-digest pin on
   `src/storage/encoding/page_materialize.rs`.
2. File-digest pin on
   `src/storage/encoding/sparse_projection_fetch.rs`.
3. No new `L2FetchStrategy` variant.
4. No new `RowGroupFallbackReason` variant.
5. No new `EncodingFallbackReason` variant.
6. No new `EncodingRejectCode` variant.
7. No new `RowGroupRejectCode` variant.
8. No new `EncodingCascadeKind` variant.
9. No new `EncodingComputeEvaluatorKind` /
   `EncodedAggregateKind` variant.
10. No new `EncodedRowMask` variant.
11. No `sparse_projection_fetch_*` config field
    in any config struct.
12. `MAX_SPARSE_RANGES_PER_BUFFER` exists as a
    `const` in `sparse_projection_fetch.rs`,
    not a `Config` field.
13. `SparseProjectionFetchDecline` exists only
    inside `sparse_projection_fetch.rs`; no
    escape to `EncodingFallbackReason` /
    `RowGroupFallbackReason` / metric labels.
14. No new IPC custom-metadata key.
15. No public PBC opcode addition.
16. No new `NodeCapability` bit.
17. No new wire `MsgType` / `FRAME_FLAG_*`.
18. No ADR 057 `EncodingDirectory` /
    `BufferEncoding` shape change.
19. Sparse materialization helpers live under
    `src/storage/encoding/`, not as new public
    protocol types.
20. `page_materialize.rs` owns decode +
    assembly helpers only — no planner
    decisions.
21. `sparse_projection_fetch.rs` owns planner
    + decision + economic logic only — no
    decode helpers duplicated.
22. Full-row-group projection-fetch helper
    remains present as fallback / oracle.
23. Existing ADR 052 secondary-index encoded-
    page path still calls the shared page-
    materialize helper (no behavior change).
24. New symbols (types, methods, tests,
    modules) contain no `phase_*` planning-
    cadence identifiers.
25. Project-name prefix rule
    (`Wendelon[A-Z]`) — new symbols clean.
26. Newtype-unit rule — any new newtypes
    route through `src/schema/units.rs` via
    `impl_unit!`.
27. No version suffix on new types.
28. Build remains on stable Rust.
29. No Cargo feature gates sparse narrowing.

Metrics:

- Continue recording row-group strategy as
  `row_group`.
- Record sparse projection-fetch bytes under
  existing
  `storage_l2_row_group_fetch_bytes_total{kind="phase_c"}`
  (current label; convention sweep target is
  `kind="projection"`).
- Record avoided bytes under existing
  `kind="avoided"`.
- Record selected page count under existing
  `storage_l2_encoding_simd_pages_fetched`.
- Record selected range count under existing
  `storage_l2_row_group_fetch_selected_ranges`.
- **Sparse declines are not fallback events.**
  ADR 060 sparse declines do not emit any
  `EncodingFallbackReason` /
  `RowGroupFallbackReason` metric event. The
  pre-existing fallback labels remain reserved
  for ADR 057 / 058 / 059 fallback events
  (unchanged). Sparse-vs-dense routing is
  inferred from the byte / page / range
  counters above; the internal
  `SparseProjectionFetchDecline` enum is
  observability inside the planner module only
  and never surfaces on a metric label.

**Exit gate:** ADR 059 default predicates can
feed ADR 060 sparse projection fetch without
COCD catalog changes. Architectural tests
prove ADR 060 did not leak into public
protocol / config / catalog surfaces or onto
metric labels.

## Phase 7 — Bench harness + evidence artifact

Files:

- `benches/row_group_fetch.rs`.
- `benches/encoding_compute.rs` (only for COCD
  end-to-end rows if needed).
- `plans/impl/0060_sparse_projection_fetch_narrowing_bench_results.md`
  (new, empirical, lands as eligibility
  classes clear gates).

Bench command:

```sh
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench row_group_fetch
```

Bench matrix:

- **Row counts:** 8K / 65K / 1M (1M
  mandatory).
- **Mask shapes:** clustered first-page,
  clustered middle 1%, clustered 5%,
  clustered 25%, random uniform 1%, `All`,
  `None`.
- **Predicate-mask sources** (drives the
  survival mask): BitPacked `UInt8`,
  Dictionary plain low-card, Dictionary
  bitpacked low-card, monotonic Delta
  `Int64`.
- **Projected-column cascades:** BitPacked
  `UInt8` / `UInt16` / `UInt32`, Dictionary
  plain low-card, Dictionary bitpacked low-
  card, **RLE byte-run (default-off after
  Phase 7 evidence)**, Plain (dense control),
  Delta (dense control), Lz4Frame (dense
  control), nullable BitPacked (dense control).
- **Projected-column count:** 1 / 4 / 16 /
  64.
- **Three-way per-cell measurement:** sparse
  projection fetch (ADR 060) vs full-row-
  group projection fetch (ADR 056 baseline)
  vs ADR 052 SIMD-page point-read (lower-
  bound comparator, single-row case).
- **Bytes-fetched counter:** instrumented
  via `MockObjectStore` GET counter.
- **Per-buffer fragmentation budget tuning
  row:** sweep candidate values (e.g., {2,
  4, 8, 16}) to find the bench-stable
  starting `const`.
- **Mixed-cascade row:** projected column
  set containing one sparse-eligible + one
  dense-only cascade — verifies per-row-
  group uniform decision routes the whole
  group to dense if any required column is
  ineligible.

Acceptance gates:

- Clustered 1% / 5% masks reduce projection-
  fetched **plaintext** bytes by **≥ 3×** vs
  full-row-group on representative
  `BitPacked` and `Dictionary` projected
  fixtures. This is the byte quantity the
  plan-time sparse_bytes < dense_bytes
  economic gate estimates.
- Clustered 1% / 5% masks improve end-to-end
  read CPU or wall time by **≥ 1.5×** after
  multi-range RTT overhead.
- Encrypted ciphertext-byte ratios are recorded
  but **not gated** in ADR 060 V1. ADR 044's
  chunk-aligned authenticated range-read
  substrate rounds selected plaintext ranges out
  to chunk boundaries, creating the observed
  1.0×–1.1× floor on representative 1M
  fixtures. Substantial ciphertext-byte
  narrowing requires a separate page-aligned
  authenticated-chunk layout ADR.
- Sparse plans stay within the chosen
  `MAX_SPARSE_RANGES_PER_BUFFER`.
- Random 1% masks decline sparse or stay
  within 5% of dense path.
- `All` masks decline with no positive
  overhead beyond planning noise.
- Output equality on every fixture
  (property suite).
- Cross-replica determinism byte-identical.
- ADR 052 / 056 / 057 / 058 / 059 / 040
  regression neutrality with sparse enabled
  and disabled.
- 1M tier mandatory; 8K and 65K rows
  informational.
- No fixture row slower than dense
  projection-fetch baseline on any cascade
  without documented council disposition.
- **RLE Q1 settles here**: RLE fixtures record
  default-off via eligibility-layer cascade
  exclusion. The decision is byte-correct and
  recorded in the V1 changeset; non-blocking for
  BitPacked / Dictionary promotion. RLE value-run
  format work remains a separate ADR.

Bench artifact follows ADR 056 / 057 / 058 /
059 shape: command, fixture matrix table,
predicate configuration, bytes-fetched
table, wall-clock table, dense-fallback
overhead, mixed-cascade row, operator-
disabled regression row, fragmentation-
budget tuning row, RLE routing decision,
council disposition.

Reports per row:

- full-row-group projection-fetch current
  path bytes and time;
- sparse projection-fetch selected page
  bytes and time;
- tiered ciphertext bytes after range
  coalescing (recorded, not promotion-gated);
- selected range count;
- max per-buffer sparse range count;
- per-buffer fragmentation budget value
  under test;
- selected SIMD page count;
- sparse planning time;
- materialization time;
- dense fallback overhead when sparse
  declines;
- output equality status.

**Exit gate:** Bench artifact records pass /
fail for every gate, the recorded-not-gated
ciphertext ratios, the RLE default-off Q1
disposition, and the finalized
`MAX_SPARSE_RANGES_PER_BUFFER` constant.
Empirical, not prescriptive.

## Phase 8 — Shipped-status promotion

The ADR document and this implementation plan
are already at their final paths
(`plans/adr/0060_sparse_projection_fetch_narrowing.md`,
`plans/impl/0060_sparse_projection_fetch_narrowing.md`)
from the council-convergence promotion
changeset. Phase 8 is now complete: Phase 7
bench evidence accepted the revised plaintext-
byte gate, recorded ciphertext bytes as an ADR
044 chunk-granularity floor, accepted RLE
default-off, and flipped ADR 060 to shipped.

Files:

- `plans/impl/0060_sparse_projection_fetch_narrowing_bench_results.md`
  (landed in the shipped-promotion changeset).
- `plans/adr/0060_sparse_projection_fetch_narrowing.md` —
  §"Status" amended to "Council-converged,
  implemented, and promoted to shipped" (matching
  the ADR 059 §"Status" pattern).
- `plans/adr/0056_row_group_fetch_and_page_directory.md` —
  §"Where we are" amendment pointing at the
  landed bench artifact.
- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md` —
  §"Where we are" amendment.
- `plans/wip/roadmap.md` — §"Where we are"
  amendment from "implementation phases
  pending" to "implemented and promoted to
  shipped"; §"Forward sequence" #16 status
  bumped accordingly.
- `tests/architecture_test.rs` — confirm Phase
  6 lockdown guards reference the final ADR
  path (already true post-convergence; this
  is a verification, not a rename).

Work completed:

1. Confirm Phase 7 bench artifact passes the
   revised promotion gates for the council-
   chosen default scope: plaintext-byte
   reduction ≥ 3×, wall-clock ≥ 1.5×, recorded-
   not-gated ciphertext ratios, RLE default-off,
   and `MAX_SPARSE_RANGES_PER_BUFFER = 8`.
2. Land the bench-results artifact at the
   path above.
3. Flip ADR 060 §"Status" to "Council-
   converged, implemented, and promoted to
   shipped."
4. Amend ADR 056 / 057 §"Where we are" to
   point at the landed bench artifact (closes
   the encoded-fetch sparse follow-up
   reference).
5. Roadmap §"Where we are" + §"Forward
   sequence" updates at the planning-cadence
   boundary (matches the ADR 057 / 058 / 059
   shipped-status pattern).

**Exit gate:** ADR 060 is marked shipped because
code, tests, and bench evidence landed together
with the recorded council disposition. Commit
granularity remains the repository's normal review
concern; the load-bearing Phase 8 requirement is
the artifact-backed status flip.

## Verification gate

Required before promotion:

```sh
cargo test --test row_group_sparse_projection_fetch
cargo test --test row_group_materialization
cargo test --test row_group_fetch_plan
cargo test --test row_group_secondary_index
cargo test --test encoding_secondary_index
cargo test --test encoding_compute_test
cargo test --test encoding_compute_kernel_invariants
cargo test --test architecture_test
cargo test --test metrics_smoke
cargo test --test l2_remote
cargo test --test l2_rehydration
cargo test --test lsm_manager_manifest
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench row_group_fetch
cargo bench --bench encoding_compute -- --quick
git diff --check
git diff --cached --check
```

Promotion requires:

- all tests above green;
- bench artifact meets revised Phase 7 gates for
  the council-chosen default scope, including RLE
  Q1 default-off resolution and recorded-not-gated
  ciphertext ratios;
- ADR 060 ships **without** any new config
  field;
- ADR 056 + ADR 057 §"Where we are" amended in
  the same changeset.

## File-by-file expected edits

- `src/storage/encoding/page_materialize.rs`
  (new) — shared encoded-page materialization
  helpers (decode + batch assembly), reused by
  ADR 052 named-row path and ADR 060 mask-
  driven path (Phase 1).
- `src/storage/encoding/sparse_projection_fetch.rs`
  (new) — sparse projection-fetch planner +
  decision + economic logic + decline reasons
  + `MAX_SPARSE_RANGES_PER_BUFFER` `const`
  (Phase 2–5).
- `src/storage/encoding/mod.rs` — re-export
  both modules.
- `src/storage/manager.rs` — integrate sparse
  projection fetch before current full-row-
  group projection fetch; reuse existing
  fallback path; refactor ADR 052 named-row
  path to call shared helpers (Phase 1, 4, 6).
- `src/storage/row_group.rs` — minimal
  additions for sparse plan integration into
  `RowGroupFetchPlan`; no new strategy
  variant (Phase 3, 4).
- `tests/row_group_sparse_projection_fetch.rs` —
  new property suite + planner unit tests +
  dense-vs-sparse oracle (Phase 2–6).
- `tests/encoding_secondary_index.rs` /
  `tests/row_group_secondary_index.rs` —
  guard extracted helper reuse; existing
  tests pass byte-identical post-extraction
  (Phase 1).
- `tests/row_group_materialization.rs` —
  dense-vs-sparse oracle tests (Phase 5).
- `tests/row_group_fetch_plan.rs` — existing
  row-group fetch planner regression target.
- `tests/architecture_test.rs` — ADR 060
  guards (Phase 6).
- `benches/row_group_fetch.rs` — sparse vs
  dense vs point-read three-way comparison +
  bytes-fetched counter + fragmentation-
  budget tuning row + RLE routing row
  (Phase 7).
- `plans/impl/0060_sparse_projection_fetch_narrowing_bench_results.md`
  — checked-in evidence artifact (Phase 7+).
- `plans/adr/0056_row_group_fetch_and_page_directory.md` —
  §"Where we are" amendment (Phase 8 only).
- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md` —
  §"Where we are" amendment (Phase 8 only).

## Risks and mitigations

**Sparse page selection does not help random
masks.** Mitigation: planner's strict byte-
comparison gate (sparse_bytes < dense_bytes)
declines when touched pages approach the full
group.

**Multi-range RTT overhead eats byte
savings.** Mitigation: existing
`max_ranges_per_record` cap plus selected
byte-fraction economics after coalescing, plus
`MAX_SPARSE_RANGES_PER_BUFFER` internal
budget. Bench reports post-coalesce
plaintext bytes, ciphertext bytes, and wall-clock.
Ciphertext-byte ratios are recorded as an ADR 044
chunk-granularity floor, not ADR 060's shipped
gate.

**Per-buffer fragmentation pathology.**
Mitigation: `MAX_SPARSE_RANGES_PER_BUFFER`
internal `const`. Distinct from
`max_ranges_per_record` because record-grain
cap doesn't constrain per-buffer
fragmentation.

**Cross-column page-geometry mismatch.**
Mitigation: explicit geometry check in the
planner; mismatch declines sparse for the
row group with internal
`PageGeometryMismatchAcrossColumns` reason.

**Dense predicate-column zip complicates
row alignment.** Mitigation: V1 restricts
sparse projection fetch to exact-COCD masks
where `decoded_predicate_batches` is empty.
Page-sliced zip remains deferred (Q3).

**Nullable projected columns break page-
scoped Arrow assembly.** Mitigation: V1
routes null-containing projected columns to
full-row-group projection fetch. Revisit
after fixed-width non-null path is proven
(Q2).

**RLE byte-run projection economics.**
Mitigation: codec verification confirms
row→page mapping is deterministic via
`page_value_count`; this is an economic
question, not correctness blocker. Phase 7
settles V1 as default-off via eligibility-layer
cascade exclusion (Q1); RLE value-run format work
remains a separate ADR.

**Manager grows harder to maintain.**
Mitigation: two-module split —
`page_materialize.rs` for passive decode +
assembly helpers,
`sparse_projection_fetch.rs` for active
planner + decision. ADR 052 secondary-index
path becomes a caller of the shared decode
helper, not a duplicate.

**Internal decline reasons leak to public
surface.** Mitigation: architectural guard
prohibits `SparseProjectionFetchDecline`
references outside
`sparse_projection_fetch.rs`; metrics use
existing labels only; no new fallback
variant.

**Cross-replica dispatch choices differ.**
Mitigation: sparse and dense paths required
to produce byte-identical projected
batches. Physical fetch plan differences
allowed; output byte-equality is the load-
bearing invariant.

**Plan-time CPU cost on dense cases.** If
sparse is rejected for most surviving row
groups, the per-group decision adds CPU to
projection-fetch planning. Mitigation:
dense-fallback overhead gate (≤ 5%); short-
circuit eligibility checks for common dense
cases.

**`MAX_SPARSE_RANGES_PER_BUFFER` starting
value wrong.** Mitigation: bench-tuning row
in Phase 7 sweeps candidate values; final
value comes from empirical evidence, not
Phase 0 guess.

## Deferred (matches ADR)

- Sparse projection fetch for dense
  predicate-column zip (Q3).
- Sparse projection fetch for nullable
  fixed-width projections (Q2).
- Sparse projection fetch for variable-width
  projections.
- Sparse projection fetch for `Plain` /
  `Lz4Frame` / `Delta` / `Fsst` / `Alp`
  projected buffers.
- RLE default-on promotion after V1; Phase 7
  settled current V1 routing as default-off (Q1).
- Operator-visible per-buffer fragmentation
  knob (V1 ships internal `const`).
- ADR 057 RLE value-run format follow-up.
- Page-aligned authenticated chunk layout for
  ciphertext-byte narrowing (own ADR).
- Page-level min/max stats (own ADR).
- Page-level null counts (own ADR).
- Delta prefix-sum side table (own ADR).
- Sparse predicate-column fetch (own ADR).
- Cross-row-group sparse coalescing beyond
  `max_ranges_per_record`.
- FSST evaluator + sparse path.
- ALP evaluator + sparse path.
- Variable-width Dictionary value-table
  sparse narrowing.
- Sparse aggregate materialization.
- Mixed secondary-index named-row + sparse
  projection-fetch composition.
- Provider-native multi-range API.

## Resolved implementation questions

Mirror ADR 060 resolved questions:

1. **RLE byte-run projection default routing** —
   default-off, non-blocking. Phase 7 evidence
   records RLE as default-off via eligibility-
   layer cascade exclusion. The decision is byte-
   correct and recorded in the V1 changeset.
   Non-blocking for `BitPacked` / `Dictionary`
   promotion; RLE value-run format work remains a
   separate ADR.
2. **Nullable fixed-width support** — deferred to
   follow-up ADR. V1 routes nullable projected
   columns to dense via the
   `NullableProjectedColumn` decline.
3. **Dense predicate-column zip with sparse
   projection** — deferred to follow-up ADR. V1
   engages only when
   `decoded_predicate_batches` is empty.
