# Implementation Plan: ADR 064 — Nullable Sparse Projection

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/064_nullable_sparse_projection.md`

Inherits substrate:

- `plans/adr/056_row_group_fetch_and_page_directory.md` —
  `RecordPageDirectory`, three-phase fetch planner,
  `BufferPageDirectoryEntry`.
- `plans/adr/057_per_buffer_pages_and_encoding_cascades.md` —
  `EncodingDirectory`, `BufferEncoding`, `page_end_offsets`
  prefix array, `page_value_count`.
- `plans/adr/058_compute_over_encoded_data.md` /
  `plans/adr/059_native_cocd_kernels.md` — exact-COCD
  survival masks, `EncodedRowMask::{All, None, Bitmap, Runs}`
  algebra.
- `plans/adr/060_sparse_projection_fetch_narrowing.md` —
  per-buffer selected-page set + multi-range fetch shape;
  internal-decline discipline; `SparseProjectionFetchPlan`
  / `SparseProjectionFetchDecline` machinery; planner module
  location; shared partial-buffer materializer at
  `src/storage/encoding/page_materialize.rs`.
- `plans/adr/061_page_aligned_authenticated_chunk_layout.md` —
  closed AEAD chunk-size set + macro-block coalescing.
- `plans/adr/062_row_group_sizing_profile.md` — closed
  `PointRead` / `Scan` catalog.
- `plans/adr/063_page_level_predicate_stats.md` — page-stats
  Phase B narrowing with mask-only output; nullable
  predicate classification.
- `plans/conventions/no-planning-phase-names.md`,
  `plans/conventions/newtype-units.md`,
  `plans/conventions/no-version-suffixes.md`,
  `plans/conventions/no-premature-backward-compat.md`,
  `plans/conventions/no-project-name-in-protocols.md`,
  `plans/conventions/capability-vs-feature-naming.md`.

## Status

Shipped 2026-05-09. Implementation, tests, architecture
guards, and bench evidence landed together. Evidence is
recorded at
`plans/impl/064_nullable_sparse_projection_bench_results.md`.
Phase 0 fork-resolution audit confirmed raw byte-
addressable validity is a non-no-op V1 engagement path
against current writer output. Headline bench result:
clustered 5% nullable sparse projection 5.36x-19.98x
plaintext-byte reduction (value + validity combined),
1.50x-5.45x wall-clock speedup across BitPacked and
Dictionary fixtures.

Implementation contract:

- `src/storage/encoding/sparse_projection_fetch.rs` continues
  to own the active sparse projection planner and private
  decline reasons; planner extends in place (no new module).
- `src/storage/encoding/page_materialize.rs` owns passive
  page decode, page-local validity slicing, and projected
  page-batch assembly.
- The planner includes validity byte ranges in selected
  sparse ranges and economics.
- `EncodedRowMask::None` masks short-circuit before nullable
  projection eligibility.
- `EncodedRowMask::All` masks still decline to dense.
- Dense predicate batches still decline to dense.
- Phase 0 pins the V1 value-column and value-cascade
  catalogs from ADR 060's current materializer; do not
  rederive from ADR 063's page-stat catalog.
- Raw byte-addressable validity is the V1 nullable path.
  Validity buffers with `EncodingDirectory` entries decline
  to dense with a private unsupported reason.
- Phase 0 must prove the raw-only cut is not a no-op against
  current writer output; if it is, revise ADR 064 pre-code.
- Encoded-validity detection is precise per
  `(row_group_index, null_buffer_ordinal, validity_body_range)`.
- Row groups with `column_null_counts[physical] == 0` for
  nullable Arrow fields stay on the existing all-valid
  sparse path with zero validity fetches.
- All-null selected pages do not use a special value-fetch
  skip path in V1 (V2 page-skip optimization guard-railed).
- No public surface changes.
- Bench artifact promotion gate: clustered 5% nullable
  sparse rows ≥ 1.5× plaintext-byte reduction (value +
  validity combined) on representative BitPacked and
  Dictionary fixtures, ≥ 1.2× wall-clock improvement on
  representative runtime fixtures (or council-revised
  gate); ≥ 3× recorded as headline diagnostic, not gating.

## Goal

Lift ADR 060's `SparseProjectionFetchDecline::NullableProjectedColumn`
decline for fixed-width projected columns by fetching, re-
basing, and materializing page-local Arrow validity bitmaps
alongside selected encoded value pages.

Today, one nullable projected column can disable the entire
ADR 060 sparse projection path for a row group. ADR 064
removes that cliff only for columns that otherwise satisfy
ADR 060's fixed-width sparse projection constraints.

The result preserves ADR 060's exact-mask-only sparse
projection contract:
`phase_b_outcome.decoded_predicate_batches.is_empty()`.

```text
write side: unchanged.

read side:
  Phase A: unchanged
  Phase B: unchanged (mask-only PhaseBOutcome)
  Phase C: sparse projection planner (NEW: nullable branch)
           - non-null projected column → existing path
           - nullable projected column → joint eligibility
             gate (raw byte-addressable validity required)
             - mask-first short-circuit (None / All / dense
               predicate batches all unchanged)
             - `column_null_counts == 0` → existing
               all-valid sparse path, no validity fetch
             - decline if validity has BufferEncoding entry
               for (row_group, null_buffer_ordinal,
               validity_body_range)
             - compute minimal validity byte span per
               selected page; bit_offset rebase
             - selected ranges (value + validity) flow
               through the same multi-range fetch +
               macro-block coalescer
             - economic gate: combined sparse_bytes <
               combined dense_bytes
             - decline routes to existing dense path
  Phase D: encoding materialization (extended:
           rebase_validity_bitmap helper + page-scoped
           RecordBufferDirectory with page-local null
           counts)
```

## Deliverables

- `src/storage/encoding/page_materialize.rs`:
  - rename `fixed_width_non_null_value_column` to
    `fixed_width_sparse_projection_column`; allow nullable;
  - add `validity_bitmap_body_range_for_page` helper;
  - add `rebase_validity_bitmap` helper;
  - add `PageValidityBitmap` private type;
  - add `PageMaterializeValidityFetch` private fetch shape;
  - extend `PageMaterializeColumn` with `nullable: bool`;
  - extend `projected_page_batch_from_decoded_buffers` to
    accept decoded page-local validity buffers and produce
    page-scoped `RecordBufferDirectory` with page-local
    null counts.
- `src/storage/encoding/sparse_projection_fetch.rs`:
  - replace `NullableProjectedColumn` decline with
    eligibility gate that accepts nullable fixed-width
    columns;
  - add private decline reasons
    (`EncodedValidityBufferUnsupported`,
    `ValidityBufferRangeMissing`,
    `NullableTypeNotInCatalog`);
  - extend `SparseProjectionFetchPlan` to carry validity
    fetches per nullable column;
  - extend economic gate to include validity-buffer
    plaintext bytes;
  - preserve mask-first ordering.
- `src/storage/manager.rs`:
  - thread validity-fetch execution through the existing
    sparse projection read path;
  - extract validity bytes from fetched ranges using the
    helper-computed body ranges; rebase to page-local;
    pass to materializer.
- `tests/encoding_materialize.rs`:
  - `rebase_validity_bitmap` correctness across byte-
    aligned, non-byte-aligned, tail-page, all-valid,
    all-null, mixed-validity, and 1,025-rows-per-page
    inputs.
- `tests/row_group_sparse_projection_fetch.rs`:
  - remove `nullable_active_projection_column_declines_sparse_projection`
    (or rewrite to assert engagement);
  - add nullable engagement / decline tests;
  - add ADR 063 composability tests
    (`IS NULL` / `IS NOT NULL` / nullable value-comparison
    exact masks).
- `tests/encoding_secondary_index.rs`:
  - if ADR 052 path needs nullable page-local assembly,
    confirm it imports the shared materializer helpers
    (single-source guard).
- `tests/encoding_chunk_layout.rs`:
  - nullable sparse projection composes with every closed
    ADR 061 chunk size.
- `tests/architecture_test.rs`:
  - ADR 064 lockdown guards.
- `benches/row_group_fetch.rs`:
  - extend with nullable-selectivity sweep + page-geometry
    sweep + sub-byte page-boundary fixture + all-null /
    all-non-null boundary fixtures + ADR 063 composability
    fixture (ExactMatch-driven vs Maybe-driven projection
    pages as separate rows) + encoded-validity decline
    control.
- `plans/impl/064_nullable_sparse_projection_bench_results.md`:
  - Phase 7 evidence artifact (at promotion only).
- `plans/adr/060_sparse_projection_fetch_narrowing.md`:
  - §"Where we are" amendment after promotion.
- `plans/adr/063_page_level_predicate_stats.md`:
  - §"Where we are" amendment after promotion.
- `plans/wip/roadmap.md`:
  - planning-cadence update at promotion.

## Non-goals

- No dense predicate-zip with sparse projection.
- No sparse predicate-column fetch for non-COCD predicates.
- No variable-width sparse projection.
- No nullable variable-width sparse projection.
- No nested nullable projection.
- No encoded-validity page decoding.
- No validity-buffer cascade engagement.
- No all-null selected-page value-fetch skipping.
- No page-stat-driven validity-fetch skipping.
- No mixed sparse / dense projected columns within one
  row-group materialization pass.
- No new `L2FetchStrategy`, fallback / reject / catalog
  variant, config field, metric label, IPC metadata key,
  descriptor field, wire type, capability bit, digest
  domain, or `ObjectStore` method.
- No Cargo feature, nightly rustc dependency, public PBC
  opcode, or operator-visible nullable engagement knob.
- No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.
- No tolerant or legacy encoding-directory decoder.
- No envelope `format_version` bump.
- No retirement of ADR 060 non-null path.
- No migration sweeper.
- No Toron-visible contract change.
- No internal decline reasons leaking to public surface.
- No third stats class.
- No FSST / ALP / RLE-value-run evaluator work.

## Phase 0 — Contract audit

Read-only audit before code changes. **This is a hard
fork-resolution gate.**

Files:

- `plans/adr/060_sparse_projection_fetch_narrowing.md`
- `plans/impl/060_sparse_projection_fetch_narrowing.md`
- `plans/adr/063_page_level_predicate_stats.md`
- `plans/adr/064_nullable_sparse_projection.md`
- `src/storage/encoding/sparse_projection_fetch.rs`
- `src/storage/encoding/page_materialize.rs`
- `src/storage/encoding/page.rs`
- `src/storage/encoding/directory.rs`
- `src/storage/columnar.rs`
- `src/storage/manager.rs`
- `src/storage/multirow.rs`
- `tests/architecture_test.rs`
- `tests/encoding_secondary_index.rs`
- `tests/encoding_chunk_layout.rs`
- `benches/row_group_fetch.rs`

Work:

1. Confirm the current decline site:
   `SparseProjectionFetchDecline::NullableProjectedColumn`.
2. Confirm `EncodedRowMask::None` masks are inspected before
   projection-column eligibility; preserve this ordering.
3. Confirm `EncodedRowMask::All` masks route to dense;
   preserve this ordering.
4. Confirm `decoded_predicate_batches.is_empty()` still
   gates sparse projection on (mask-only contract).
5. Inventory the helpers named around non-null sparse
   projection: `fixed_width_non_null_value_column`,
   `projected_page_batch_from_decoded_buffers`, planner
   test fixtures.
6. **Pin the exact fixed-width value type catalog accepted
   by the current ADR 060 materializer.** Nullable support
   must not add types ADR 060 cannot already materialize.
   Record the catalog in this implementation plan; do not
   rederive from ADR 063's page-stat type catalog.
7. **Pin the value-buffer cascade catalog accepted by
   ADR 060 sparse projection.**
8. Confirm where `RecordPageDirectory` records row-group
   validity buffer ranges for nullable fixed-width columns
   (`BufferPageDirectoryEntry`).
9. **Audit whether `EncodingDirectory` can contain entries
   for validity buffers** in fixtures and real writer
   output:
   - if validity buffers never have entries, the encoded-
     validity decline is a future-proof guard;
   - if validity buffers sometimes have entries, V1 engages
     on raw and declines on encoded;
   - if validity buffers always have entries, the raw-only
     V1 cut is a no-op and **the ADR must be revised
     before code changes.**
10. Pin raw byte-addressable Arrow validity bitmap slices
    as the only V1 validity-source catalog. Do not add
    encoded validity to the catalog during implementation;
    if encoded validity is the production common case,
    revise ADR 064 before code changes.
11. Confirm encoded-validity detection uses the selected
    row group's null buffer identity, not just a buffer
    ordinal: `(row_group_index, null_buffer_ordinal,
    validity_body_range)`. One row group's encoded buffer
    must not poison another row group's raw validity path.
12. Confirm row groups with `column_null_counts[physical] == 0`
    keep the existing all-valid sparse path and fetch no
    validity bytes merely because the Arrow field is
    nullable.
13. Confirm sub-byte page-boundary behavior for non-
    multiple-of-8 page starts, including page sizes like
    1,025 rows.
14. Confirm selected sparse byte accounting currently uses
    `selected_body_ranges`; ADR 064 validity fetches enter
    the same vector before coalescing.
15. Confirm dense projection baseline ranges include
    validity buffers for nullable projected columns (so
    the economic gate compares apples-to-apples).
16. Confirm current page materialization constructs a
    scoped uncompressed `RecordBufferDirectory` and rewrites
    `column_null_counts` to zero for non-null pages.
17. Confirm whether the ADR 052 secondary-index
    materialization path can reuse `page_materialize.rs`
    helpers for nullable page-local assembly instead of
    adding a separate bitmap slicer.
18. Confirm ADR 063 nullable predicate masks compose with
    nullable sparse projection: `IS NULL`, `IS NOT NULL`,
    nullable value comparisons that produce exact masks.
    Confirm ADR 063 `ExactMatch` pages and `Maybe` pages
    are represented distinctly in the bench matrix —
    ExactMatch can erase predicate fetch bytes while
    ADR 064 still fetches selected projection value and
    validity bytes for the surviving row span.
19. Confirm cross-replica determinism: page-local nullable
    assembly is deterministic over (selected page set,
    validity byte slice, bit-offset rebase, row span
    slicing, ADR 057 directory).
20. Confirm the existing row-group and encoding fetch
    toggles remain the only activation controls; ADR 064
    adds no new config or Cargo feature.

**Exit gate:** Council agrees ADR 064 is nullable fixed-
width sparse projection only, with raw byte-addressable
validity, encoded-validity decline, joint value+validity
eligibility, V1 type and cascade catalogs mirrored from
ADR 060's current materializer, and no dense predicate zip.
Phase 0 confirms raw-only V1 is not a no-op against current
writer output; if it is, ADR is revised pre-code.

## Phase 1 — Page-local validity helpers

Add passive validity helpers to `page_materialize.rs` in
isolation, before any planner change.

Files:

- `src/storage/encoding/page_materialize.rs`
- focused unit tests in the same module or
  `tests/encoding_materialize.rs`.

Work:

1. Add `PageValidityBitmap` private type:

   ```rust
   pub(crate) struct PageValidityBitmap {
       pub bytes: Vec<u8>,
       pub null_count: usize,
   }
   ```

2. Add `validity_bitmap_body_range_for_page`:

   ```rust
   pub(crate) fn validity_bitmap_body_range_for_page(
       validity_buffer_range: RecordBodyByteRange,
       page: EncodingPageDescriptor,
   ) -> Result<(RecordBodyByteRange, u8), ArrowError>;
   ```

   Computes:

   ```text
   bit_start  = page.first_value_index
   bit_len    = page.value_count
   byte_start = floor(bit_start / 8)
   bit_offset = bit_start % 8
   byte_len   = ceil((bit_offset + bit_len) / 8)
   ```

3. Add `rebase_validity_bitmap`:

   ```rust
   pub(crate) fn rebase_validity_bitmap(
       fetched: &[u8],
       bit_offset: u8,
       rows: usize,
   ) -> Result<PageValidityBitmap, ArrowError>;
   ```

4. Use Arrow's little-endian bitmap convention.
5. Count page-local nulls while writing re-based output
   bytes (single pass).
6. Validate bounds:
   - `bit_offset < 8`;
   - fetched bytes cover every requested bit;
   - output byte length is `ceil(rows / 8)`;
   - `null_count <= rows`.

Tests:

- byte-aligned page start, mixed validity;
- non-byte-aligned page start, mixed validity;
- 1,025-rows-per-page (page starts move across byte
  boundaries);
- tail page with non-multiple-of-8 row count;
- all-valid page returns `null_count == 0`;
- all-null page returns `null_count == rows`;
- alternating nulls;
- clustered nulls;
- empty rows handled or rejected consistently with page
  descriptors;
- fetched slice too short returns an error.

**Exit gate:** Helpers prove page-local bit 0 maps to page
row 0 for both byte-aligned and non-byte-aligned inputs.
`cargo test --test encoding_materialize` passes.

## Phase 2 — Page materialization shape

Teach page materialization to accept optional page-local
validity bytes.

Files:

- `src/storage/encoding/page_materialize.rs`
- `src/storage/encoding/sparse_projection_fetch.rs` test
  fixtures.

Work:

1. Extend `PageMaterializeColumn` with `nullable: bool`.
2. Rename `fixed_width_non_null_value_column` to
   `fixed_width_sparse_projection_column`. Allow nullable
   fixed-width fields; keep value-type and buffer-count
   restrictions; allowed types match the Phase-0-pinned V1
   catalog.
3. Add private fetch type:

   ```rust
   pub(crate) struct PageMaterializeValidityFetch {
       pub key: PageMaterializeBufferKey,
       pub body_range: RecordBodyByteRange,
       pub page: EncodingPageDescriptor,
       pub bit_offset: u8,
       pub page_row_count: RowGroupRowCount,
   }
   ```

4. Extend `projected_page_batch_from_decoded_buffers` to
   accept decoded value pages, decoded page-local validity
   buffers, and page-local null counts.
5. Build scoped `RecordBufferDirectory` per page:
   - non-null column: null buffer length 0, null count 0;
   - nullable with no nulls: null buffer length 0, null
     count 0;
   - nullable with nulls: null buffer length
     `ceil(page_rows / 8)`, null count from
     `PageValidityBitmap`.
6. Keep scoped `record_body_compression = Uncompressed`,
   no page directory, no encoding directory.
7. Do not change `decode_projected_record_batch_from_buffers`
   public behavior.
8. Keep the bitmap rebase helper reusable by any ADR 052
   secondary-index materialization path that needs page-
   local nullable fixed-width arrays (single-source guard).

Tests:

- nullable Int64 page with mixed nulls materializes the
  expected Arrow array;
- nullable UInt32 page with all-valid selected page → no
  null buffer, null count 0;
- nullable page with all nulls → all-null values;
- multi-column page with one nullable + one non-null
  preserves column ordinals and storage-column mapping;
- non-null sparse projection tests still pass byte-
  identically.

**Exit gate:** Page-local nullable projected batches can be
constructed without using dense row-group buffers.

## Phase 3 — Planner validity fetches

Lift the nullable decline in the sparse projection planner.

Files:

- `src/storage/encoding/sparse_projection_fetch.rs`
- `src/storage/encoding/page_materialize.rs`

Work:

1. Replace the `NullableProjectedColumn` decline with
   eligibility that accepts nullable fixed-width columns
   in the Phase-0-pinned V1 catalog.
2. Preserve existing declines for:
   - variable-width projected columns;
   - non-page-emitting value cascades;
   - cascades outside ADR 060's sparse projection
     engagement set;
   - cross-column value-page geometry mismatch;
   - dense predicate batches present;
   - missing exact masks;
   - range / byte / page cap misses.
3. Add private validity-fetch planning for nullable
   columns:
   - find the validity buffer ordinal from
     `column.null_buffer_ordinal`;
   - find its row-group `BufferPageDirectoryEntry`;
   - if row-group null count is zero, mark the selected
     page all-valid and fetch no validity bytes (existing
     all-valid sparse path);
   - if row-group null count is nonzero, decline when the
     validity row-group range is absent;
   - decline when the validity buffer has a
     `BufferEncoding` entry in `EncodingDirectory` for the
     same `(row_group, null_buffer_ordinal,
     validity_body_range)`;
   - compute selected validity byte span and bit_offset
     per selected value page;
   - add planned validity ranges to the same selected
     range set as value pages.
4. Coalesce value and validity ranges together before
   applying record range caps.
5. Include validity bytes in:
   - sparse plaintext bytes;
   - selected byte fraction;
   - per-buffer fragmentation cap;
   - record range cap.
6. Keep `SparseProjectionFetchDecline` private. New private
   reasons (`EncodedValidityBufferUnsupported`,
   `ValidityBufferRangeMissing`,
   `NullableTypeNotInCatalog`) do not surface in metrics
   or public fallbacks.
7. Preserve mask-first behavior:
   - `None` mask returns an empty sparse plan before
     nullable checks;
   - `All` mask declines dense before value / validity
     fetch planning;
   - non-empty `decoded_predicate_batches` declines before
     nullable checks.

Tests:

- `nullable_planner_engages_clustered_5pct`.
- `nullable_planner_declines_random_uniform_above_dense_baseline`.
- `nullable_planner_declines_encoded_validity_buffer`
  (private reason).
- `nullable_planner_declines_validity_range_missing`
  (private reason).
- `nullable_planner_declines_variable_width`.
- `nullable_planner_declines_unsupported_cascade`.
- `nullable_planner_declines_excess_fragmentation`.
- `nullable_none_mask_zero_projection_fetch`.
- `nullable_all_mask_routes_dense`.
- `nullable_dense_predicate_batches_decline_sparse`.
- `selected_validity_bytes_present_in_selected_body_ranges`.
- `validity_ranges_coalesce_with_adjacent_value_ranges_when_contiguous`.
- `validity_bytes_can_force_byte_estimate_not_sparse_on_weak_masks`.
- `non_null_path_byte_identical_pre_post` (regression).
- `nullable_zero_row_group_null_count_uses_all_valid_path_no_validity_fetch`.
- `encoded_validity_detection_per_row_group_does_not_poison_other_groups`.

**Exit gate:** Planner produces complete sparse plans
containing all value and validity ranges before any L2
fetch occurs. `cargo test --test
row_group_sparse_projection_fetch` passes.

## Phase 4 — Manager integration

Thread validity fetch execution into the existing sparse
projection read path.

Files:

- `src/storage/manager.rs`
- `src/storage/encoding/page_materialize.rs`

Work:

1. Ensure `SparseProjectionFetchPlan.selected_body_ranges`
   includes validity ranges so the existing tiered multi-
   range plan fetches them.
2. When processing sparse projection fetches:
   - extract selected encoded value page bytes as today;
   - extract selected raw validity bytes using the
     validity fetch body range;
   - re-base validity bytes to page-local bitmaps;
   - pass decoded value pages and page-local validity
     pages into page materialization.
3. Preserve current sparse projection materialization
   order: `(row_group_index, page_index)` order, then
   concatenate groups.
4. Preserve final survival-mask behavior: sparse outputs
   are already page-filtered and must not receive the
   standard full-row-group mask application a second time.
5. Post-fetch invariant failures fail closed through
   existing storage / Arrow errors and existing encoding
   invariant metrics. Do not silently retry dense after
   partial sparse fetch begins.

Tests:

- runtime encrypted L2 sparse read with nullable projected
  Int64 column returns dense-oracle-equivalent output;
- clustered mask selects one page and fetches value plus
  validity bytes, not the whole value buffer;
- random weak mask declines to dense and output equality
  holds;
- `None` mask with nullable projected column performs zero
  projection range GETs;
- `All` mask with nullable projected column routes dense;
- point-read path unchanged.

**Exit gate:** Production read path materializes nullable
sparse projection only from fetched selected ranges and
remains byte-identical to dense projection.

## Phase 5 — Architecture guards

Land ADR 064 guards.

Files:

- `tests/architecture_test.rs`.

Guards:

- `SparseProjectionFetchDecline::NullableProjectedColumn`
  removed or no longer used as an active nullable fixed-
  width decline.
- `decoded_predicate_batches` still gates sparse projection
  off.
- Sparse projection planner module location unchanged.
- Per-page validity helpers live in
  `src/storage/encoding/page_materialize.rs`; ADR 052
  secondary-index path imports them (single-source guard).
- `SparseProjectionFetchDecline` remains private to the
  planner module.
- All-null selected pages do not use a special value-fetch
  skip path in ADR 064.
- No new `L2FetchStrategy` variant.
- No new `EncodedRowMask` variant.
- No new public fallback / reject / catalog variant.
- No new config field on row-group, encoding, compute, or
  chunk-layout configs.
- No new metric label.
- No new IPC `custom_metadata` key.
- No `EncodingDirectory` shape extension for validity
  pages.
- No new wire `MsgType` / `FRAME_FLAG_*` /
  `NodeCapability` bit / digest domain / `ObjectStore`
  trait method.
- No new descriptor field / fingerprint surface change.
- No new public PBC opcode.
- No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.
- No Cargo feature gating nullable engagement.
- No nightly rustc dependency.
- New helper names avoid planning-phase identifiers.
- No new version-suffix types.
- No project-name-prefixed protocol identifiers.
- ADR 020 §2 AAD layout, ADR 022 §3 / §5 byte layouts,
  ADR 056 `RecordPageDirectory` shape, ADR 057
  `EncodingDirectory` shape, ADR 061 closed chunk-size
  set, ADR 062 `RowGroupSizingProfile` catalog, ADR 063
  `BufferEncoding.page_stats` shape: all unchanged.
- Cross-replica byte-equal output across two writers with
  the same config + nullable input fixture.
- Dense predicate-zip remains absent from ADR 064 code
  paths.

**Exit gate:** `cargo test --test architecture_test`
passes.

## Phase 6 — Focused tests

Focused test commands:

```sh
cargo test sparse_projection_fetch --lib
cargo test page_materialize --lib
cargo test --test row_group_sparse_projection_fetch
cargo test --test encoding_materialize
cargo test --test row_group_materialization
cargo test --test architecture_test
cargo test --test encoding_secondary_index
cargo test --test encoding_chunk_layout
```

Required coverage:

- planner accepts nullable fixed-width projected column;
- planner keeps variable-width nullable columns dense;
- planner keeps unsupported cascades dense;
- planner declines encoded validity buffers with a private
  unsupported reason;
- nullable Arrow field with row-group null count 0 stays
  on the all-valid sparse path and fetches zero validity
  bytes;
- planner preserves `None` and `All` mask behavior;
- bitmap rebase handles non-byte-aligned page starts;
- bitmap rebase handles a page size such as 1,025 rows
  where page starts move across byte boundaries;
- bitmap rebase handles tail pages;
- sparse nullable output equals dense output for:
  - all-valid selected pages;
  - all-null selected pages;
  - alternating nulls;
  - clustered nulls;
  - multiple nullable projected columns;
  - nullable user-visible plus nullable storage-column
    projection;
- ADR 063 nullable exact masks compose with nullable
  sparse projection:
  - `IS NULL` exact mask projects a second nullable column
    sparsely;
  - `IS NOT NULL` exact mask projects a nullable column
    sparsely;
  - nullable value comparison exact mask projects nullable
    columns sparsely;
- ADR 063 ExactMatch-driven and Maybe-driven projection
  pages are both covered as separate rows so the bench
  artifact records the predicate-zero/projection-nonzero
  byte-economy asymmetry;
- ADR 061 chunk sizes compose with nullable validity
  ranges;
- cross-replica nullable sparse projection output is byte-
  identical on the same config and input;
- ADR 052 secondary-index materialization, if touched,
  uses the shared validity helper and preserves dense-
  oracle equality.

**Exit gate:** All focused tests pass and existing ADR 060
tests remain green.

## Phase 7 — Bench matrix

Extend `benches/row_group_fetch.rs` with nullable sparse
projection rows. Land evidence at promotion time in:

```text
plans/impl/064_nullable_sparse_projection_bench_results.md
```

Bench dimensions:

- row-group profile: `PointRead`, `Scan`.
- row counts: 8K, 65K, 1M (multi-record tier if runtime
  bench cost permits).
- page geometry:
  - default page size;
  - non-byte-aligned page boundaries (e.g., 1,025
    rows/page).
- projected column shape:
  - one nullable fixed-width column;
  - four nullable fixed-width columns;
  - mixed nullable + non-null fixed-width columns;
  - nullable plus variable-width control.
- null distribution:
  - all-valid selected pages;
  - no-null row group control;
  - alternating nulls;
  - clustered nulls;
  - all-null selected page;
  - random 5% nulls.
- mask shape:
  - clustered 1% / 5% / 25% / 50%;
  - random 1%;
  - `None`;
  - `All`.
- predicate source:
  - exact `IS NOT NULL` mask with clustered non-null
    pages at 5% / 25% / 50%;
  - exact `IS NULL` mask with clustered-null pages at
    5% / 25%;
  - nullable value comparison exact mask, including
    ADR 063 `Maybe` page controls that route dense until
    they become exact;
  - ExactMatch-driven and Maybe-driven projection pages
    recorded as separate rows.
- cascade:
  - BitPacked;
  - DictionaryPlain;
  - DictionaryBitPacked;
  - RLE / Plain / Lz4Frame dense controls.
- encoded-validity decline control: a fixture whose
  validity buffer has a `BufferEncoding` entry, if
  constructible, routes to dense with the private
  unsupported reason.

Rows record:

- route (`sparse`, dense-decline reason class,
  `none_zero_fetch`);
- dense projection plaintext bytes;
- sparse selected value plaintext bytes;
- sparse selected validity plaintext bytes;
- sparse total selected plaintext bytes;
- dense / sparse ciphertext bytes;
- selected ranges;
- selected value pages;
- selected validity ranges;
- output row count;
- output equality digest;
- null-count equality;
- dense wall-clock;
- sparse wall-clock;
- range GET calls / bytes;
- `get_bytes` calls.

Acceptance gates (matching ADR 060 / 063 cadence):

- Output equality with dense projection on every
  correctness row.
- Null counts match dense projection on every nullable
  row.
- `None` mask fetches zero projection value AND validity
  bytes.
- `All` mask routes dense.
- **Promotion gate**: clustered 5% nullable sparse rows
  keep at least 1.5× selected plaintext-byte reduction
  (value + validity combined) on representative BitPacked
  and Dictionary fixtures, AND at least 1.2× wall-clock
  improvement on representative runtime fixtures (or
  council records a revised gate).
- **Headline diagnostic** (not gating): the artifact also
  reports whether ≥ 3× plaintext-byte reduction holds on
  best-case clustered fixtures.
- Dense-decline overhead ≤ 5% on random / weak rows.
- Non-null sparse projection rows remain within ADR 060's
  existing envelope (byte-identical regression).
- ADR 052 nullable point read byte-identical to dense via
  shared helper (if path touched).
- ADR 063 composability rows: combined byte savings
  strictly larger than either ADR alone on shared
  nullable-predicate fixtures. ExactMatch-driven and
  Maybe-driven projection pages recorded as separate
  rows.
- ADR 061 / 062 composability rows stay inside their
  existing envelopes.
- Cross-replica determinism on representative nullable
  fixtures.
- Encoded-validity decline control row records the private
  decline reason and routes to dense.
- Sub-byte page-boundary correctness on row counts not
  divisible by 8.
- `PointRead` rows record outcome; promotion does not
  require `PointRead` default engagement if validity
  bytes erase the win, but behavior must be byte-
  identical and deterministic.

**Exit gate:** Bench artifact provides enough evidence for
council promotion or explicit revision.

## Phase 8 — Promotion

Promotion happens only after implementation, tests,
architecture guards, and bench evidence land together.

Files:

- `plans/adr/064_nullable_sparse_projection.md` — Status
  amendment to "shipped" with bench evidence reference.
- `plans/impl/064_nullable_sparse_projection.md` — Status
  amendment.
- `plans/impl/064_nullable_sparse_projection_bench_results.md`
  — Phase 7 evidence artifact.
- `plans/adr/060_sparse_projection_fetch_narrowing.md` —
  §"Where we are" amendment.
- `plans/adr/063_page_level_predicate_stats.md` —
  §"Where we are" amendment.
- `plans/wip/roadmap.md` — planning-cadence update.

Work:

1. Pin the bench-validated parameters in the bench
   results artifact.
2. Amend ADR 060 §"Where we are":
   - nullable fixed-width sparse projection is shipped;
   - encoded-validity, variable-width, page-skip,
     dense-predicate-zip remain V2.
3. Amend ADR 063 §"Where we are":
   - composition pathway with sparse nullable projection
     on nullable predicate columns recorded;
   - combined byte-savings evidence linked from the bench
     artifact.
4. Mark ADR 064 shipped only after code, tests, and bench
   evidence land together.
5. Update the roadmap at the planning-cadence boundary.

**Exit gate:** ADR 064 promoted as a narrow ADR 060
amendment with no public protocol surface change.

## Verification gate

Before promotion:

```sh
cargo test sparse_projection_fetch --lib
cargo test page_materialize --lib
cargo test --test architecture_test
cargo test --test row_group_sparse_projection_fetch
cargo test --test encoding_materialize
cargo test --test row_group_materialization
cargo test --test row_group_fetch_plan
cargo test --test encoding_compute_test
cargo test --test encoding_compute_kernel_invariants
cargo test --test encoding_directory
cargo test --test encoding_secondary_index
cargo test --test encoding_chunk_layout
cargo test --test crypto_macro_block_coalescing
cargo test --test l2_remote
cargo test --test l2_rehydration
cargo test --test multirow
cargo test --test config_loader
cargo check
cargo check --benches
cargo bench --bench row_group_fetch -- --quick
git diff --check
git diff --cached --check
```

Full bench:

```sh
cargo bench --bench row_group_fetch
```

## Risks and mitigations

**Bitmap rebase off-by-one corrupts nulls.** Mitigation:
helper-level tests for byte-aligned, non-byte-aligned,
1,025-rows-per-page, tail-page, all-valid, all-null,
alternating, and clustered-null patterns; dense output
oracle in runtime tests; cross-replica byte-equality
bench gate.

**Validity bytes erase sparse economics.** Mitigation:
validity bytes are included in the existing strict sparse-
vs-dense byte comparison. Weak rows decline to dense.
Bench evidence on representative fixtures confirms the
≥ 1.5× plaintext-byte gate at clustered 5%.

**`None` masks regress by checking nullable columns too
early.** Mitigation: preserve mask-first order; add tests
where `None` mask with nullable / unsupported projected
columns returns zero fetches.

**Dense predicate zip sneaks in.** Mitigation: keep
`decoded_predicate_batches` decline; architecture guard;
runtime tests pin exact-mask-only paths.

**Validity-buffer representation differs from the raw-
slice assumption.** Mitigation: Phase 0 pins the V1
validity-source catalog. Raw byte-addressable validity is
supported directly. Encoded validity declines to dense in
V1. If Phase 0 finds encoded validity is the production
common case, ADR 064 is revised before code changes
instead of landing a nullable path that always declines.

**Encoded-validity detection is over-broad.** Mitigation:
detect encoded validity using the selected row group, null
buffer ordinal, and validity body range together
(`(row_group_index, null_buffer_ordinal,
validity_body_range)`), so one row group's encoded buffer
cannot poison another row group's raw validity path.

**All-null pages tempt a second optimization.**
Mitigation: ADR 064 does not skip selected value-page
fetches for all-null pages; explicit architectural guard.
Page-skip is a focused V2 follow-up.

**Scoped page directory reports wrong null counts.**
Mitigation: page-local null counts are computed from the
re-based bitmap and tested against dense Arrow arrays.

**Range fragmentation increases on wide nullable
projections.** Mitigation: validity ranges flow through
existing coalescing and existing range / fragmentation
caps. Bench records p50 / p95 GET payload sizes.

**ADR 060 non-null path regresses.** Mitigation: Phase 3
non-null regression test; Phase 7 bench includes ADR 060's
existing fixtures byte-identical pre / post.

**Public surface creep.** Mitigation: architecture guards
cover strategy, fallback / reject / catalog, config,
metric, descriptor, wire, capability, digest, ObjectStore,
and directory-shape surfaces.

**ADR 052 nullable point-read path diverges from sparse
projection materializer.** Mitigation: single-source guard
in architectural lockdown; ADR 052 path imports the same
helpers if it needs nullable page-local assembly.

**Cross-replica byte-equality breaks on materializer non-
determinism.** Mitigation: per-page validity assembly is
deterministic over (selected page set, validity-byte
slice, bit-offset rebase, row span slicing, ADR 057
directory); no runtime branches; Phase 6 / Phase 7 bench
rows gate empirically.

**Variable-width nullable scope creep.** Mitigation: V1
type catalog is fixed-width only; planner declines on
variable-width nullable; architectural guard pins it.

**ADR 063 composition pathway misuses ExactMatch slices.**
Mitigation: Phase 7 composition bench verifies ADR 063's
`IS NULL` / `IS NOT NULL` / value-comparison
classifications drive ADR 064 sparse projection
correctly; combined byte savings strictly larger than
either ADR alone on shared fixtures.
