# Implementation Plan: ADR 065 — Dense Predicate-Zip with Sparse Projection

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR:
`plans/adr/065_dense_predicate_zip_with_sparse_projection.md`

Inherits substrate:

- `plans/adr/056_row_group_fetch_and_page_directory.md` —
  `RecordPageDirectory`, three-stage row-group fetch
  planner, `BufferPageDirectoryEntry`,
  `RowGroupDirectoryEntry`.
- `plans/adr/057_per_buffer_pages_and_encoding_cascades.md` —
  `EncodingDirectory`, `BufferEncoding`, `page_end_offsets`
  prefix array, `page_value_count`,
  `EncodingPageDescriptor.first_value_index` /
  `value_count`.
- `plans/adr/058_compute_over_encoded_data.md` /
  `plans/adr/059_native_cocd_kernels.md` — exact-COCD
  survival masks, `EncodedRowMask::{All, None, Bitmap,
  Runs}` algebra, closed evaluator catalog, dense-fallback
  decode shape.
- `plans/adr/060_sparse_projection_fetch_narrowing.md` —
  per-buffer selected-page set + multi-range fetch shape;
  internal-decline discipline; `SparseProjectionFetchPlan`
  / `SparseProjectionFetchDecline` machinery; planner
  module location; shared partial-buffer materializer at
  `src/storage/encoding/page_materialize.rs`;
  `dense_predicate_batches_present` decline gate (this ADR
  lifts).
- `plans/adr/061_page_aligned_authenticated_chunk_layout.md`
  — closed AEAD chunk-size set + macro-block coalescing.
- `plans/adr/062_row_group_sizing_profile.md` — closed
  `PointRead` / `Scan` catalog.
- `plans/adr/063_page_level_predicate_stats.md` — page-stats
  predicate-fetch narrowing with mask-only output;
  nullable predicate classification; `Maybe`-page dense-
  decode pathway.
- `plans/adr/064_nullable_sparse_projection.md` — nullable
  validity rebase helpers in `page_materialize.rs`; shared
  with ADR 052 secondary-index path (single-source guard).
- `plans/conventions/no-planning-phase-names.md`,
  `plans/conventions/newtype-units.md`,
  `plans/conventions/no-version-suffixes.md`,
  `plans/conventions/no-premature-backward-compat.md`,
  `plans/conventions/no-project-name-in-protocols.md`,
  `plans/conventions/capability-vs-feature-naming.md`.

## Status

Council-converged 2026-05-09. Phase 0 read-only audit
completed. Implementation pending. Phase 7 bench evidence
will land at
`plans/impl/065_dense_predicate_zip_with_sparse_projection_bench_results.md`
at promotion.

Implementation contract:

- `src/storage/encoding/sparse_projection_fetch.rs`
  continues to own the active sparse projection planner
  and private decline reasons; planner extends in place
  (no new module).
- `src/storage/encoding/page_materialize.rs` continues to
  own passive page decode and ADR 064 validity rebase;
  gains `filtered_predicate_page_batch` for page-grain
  dense-predicate slicing + filtering.
- `src/storage/row_group.rs` gains
  `zip_predicate_and_projection_columns` adjacent to the
  existing dense-zip helper. The helper may delegate to
  `zip_phase_b_and_phase_c_projected_row_group` during ADR
  065's lifetime; the broader convention sweep retiring
  the phase-named identifier is out of scope.
- `materialize_sparse_projection_pages` is split into a
  lower per-page helper returning page-tagged
  `SparseProjectionPageBatch` values (already filtered by
  page-local mask) plus the existing concat wrapper used
  by Case A and Case C paths.
- The planner removes
  `SparseProjectionFetchInput.dense_predicate_batches_present`
  and retires active use of
  `SparseProjectionFetchDecline::DensePredicateBatchPresent`.
- The planner consumes only survival masks and projection
  metadata; decoded Arrow batches stay in the manager /
  row-group layer.
- Predicate-side fetch and predicate evaluation are
  unchanged.
- ADR 065 zipped output (Case B) and sparse-only output
  (Case C) bypass
  `apply_phase_b_masks_to_projected_row_groups` (already
  page-filtered).
- `EncodedRowMask::None` masks short-circuit before zip
  eligibility; output is a full-schema empty
  `ProjectedRecordBatch` via
  `empty_projected_batch_for_requirement`.
- `EncodedRowMask::All` masks still route to dense.
- ADR 063 `ExactMatch`-routed row groups bypass dense
  decode entirely; ADR 065 not engaged for those row
  groups.
- Maybe-mask survivors that did not reach an exact dense
  decode decline with a private `MaybeMaskSurvivor`
  reason.
- Page-grain row-span slicing only; row-group-grain
  assembly rejected; filler-based full-row-group
  reconstruction rejected.
- Predicate bytes are sunk; the projection economic gate
  unchanged from ADR 060 / 064.
- The dense predicate side may carry variable-width or
  nullable Arrow (already decoded); the sparse projection
  side stays ADR 064 fixed-width nullable.
- No public surface changes.
- Bench artifact promotion gate (three axes): clustered
  5% dense-zip rows ≥ 1.5× remaining-projection
  plaintext-byte reduction AND ≥ 1.25× total query
  plaintext-byte reduction (predicate decode + projection)
  AND ≥ 1.2× wall-clock improvement; ≥ 3× recorded as
  headline diagnostic, not gating.

## Goal

Lift ADR 060 / 063 / 064's mask-only Phase B contract for
sparse projection by accepting populated
`predicate_projection_by_group` when the survival mask is
exact, and assembling the output batch by zipping page-
local sparse-projected arrays with row-span-sliced dense
predicate columns at survivors.

```text
write side: unchanged.

read side:
  Stage A (directory probe): unchanged
  Stage B (predicate evaluation): unchanged (the scalar
           dense-decode branch at manager.rs:9931 populates
           predicate_projection_by_group; encoding-compute
           paths — including ADR 058/059 native and ADR 063
           Maybe-page dense COCD — emit mask-only
           PhaseBOutcome and do not populate this map in V1)
  Stage C (projection planning): sparse projection planner
           - mask exact + predicate_projection_by_group
             empty + remaining projection eligible →
             existing ADR 060 / 064 path (Case A)
           - mask exact + predicate_projection_by_group
             populated + remaining projection eligible +
             economic gate clears → engage sparse
             projection (Case B or Case C decided
             manager-side)
           - mask not exact (Maybe survivors remain) →
             decline (private MaybeMaskSurvivor)
           - row-group-split predicate batch → decline
             (private PredicateBatchRowGroupSplit)
           - row-span slicing infeasible → decline
             (private PredicateBatchRowSpanMismatch)
           - mask-first short-circuit (None / All)
             unchanged
  Stage D (encoding materialization): three-case manager
           dispatch
           - Case A: existing
             materialize_sparse_projection_pages concat
             wrapper
           - Case B (overlap zip): per-page helper +
             filtered_predicate_page_batch +
             zip_predicate_and_projection_columns
           - Case C (no-overlap fallback): existing concat
             wrapper, no zip
           - Case B / C output bypasses
             apply_phase_b_masks_to_projected_row_groups
```

## Deliverables

- `src/storage/encoding/page_materialize.rs`:
  - add `filtered_predicate_page_batch` helper for page-
    grain predicate slicing + page-local mask filtering;
  - re-use ADR 064 `rebase_validity_bitmap` for nullable
    sparse-projected columns;
  - keep page-local materializer surface single-source for
    ADR 052 secondary-index reuse.
- `src/storage/encoding/sparse_projection_fetch.rs`:
  - lift `dense_predicate_batches_present` decline gate
    at lines 120-122;
  - remove `dense_predicate_batches_present` field from
    `SparseProjectionFetchInput` (line 91);
  - retire active use of
    `SparseProjectionFetchDecline::DensePredicateBatchPresent`
    (line 65);
  - add private decline reasons (`MaybeMaskSurvivor`,
    `PredicateBatchRowSpanMismatch`,
    `PredicateBatchRowGroupSplit`);
  - planner inspects mask exactness + remaining
    projection requirement only; consumes survival masks
    and projection metadata, no decoded Arrow batches;
  - economic gate unchanged (predicate bytes sunk;
    remaining-projection bytes only).
- `src/storage/manager.rs`:
  - thread the three-case manager dispatch through the
    existing sparse projection read path;
  - allow sparse projection planning when
    `predicate_projection_by_group` is non-empty and the
    query is not point-read;
  - split `materialize_sparse_projection_pages`
    (line 12249) into a lower helper returning per-page
    tagged `SparseProjectionPageBatch` values + the
    existing concat wrapper;
  - dispatch Case A (mask-only concat), Case B (overlap
    zip), Case C (no-overlap concat) on
    `predicate_projection_by_group` populated-ness AND the
    derived `predicate_output_overlap` set
    (`predicate_fetched_requirement.predicate_columns ∩
    requirement.output_columns`); Case A applies when
    `predicate_projection_by_group` is empty, Case B when
    populated AND `predicate_output_overlap` non-empty,
    Case C when populated AND `predicate_output_overlap`
    empty;
  - bypass `apply_phase_b_masks_to_projected_row_groups`
    for Case B and Case C output;
  - return zero-row full-schema batches via
    `empty_projected_batch_for_requirement` when masks
    prove no survivors.
- `src/storage/row_group.rs`:
  - add semantic wrapper
    `zip_predicate_and_projection_columns` adjacent to the
    existing
    `zip_phase_b_and_phase_c_projected_row_group`. The
    wrapper may delegate to the existing helper; the
    broader convention sweep retiring the phase-named
    identifier is out of scope for ADR 065.
- `tests/encoding_materialize.rs`:
  - `filtered_predicate_page_batch` correctness across
    byte-aligned, non-byte-aligned, tail-page, all-
    survivor, no-survivor, and mixed-survivor inputs;
  - variable-width predicate column slicing tests.
- `tests/row_group_sparse_projection_fetch.rs`:
  - rewrite or remove
    `dense_predicate_batches_decline_sparse_projection`
    test;
  - add dense-predicate-zip engagement / decline tests;
  - add Maybe-survivor decline test;
  - add ADR 063 control tests confirming `Maybe`-driven
    dense COCD continues to engage Case A mask-only
    sparse projection (encoding-compute branch emits
    mask-only `PhaseBOutcome`; ADR 065 zip not engaged
    in V1);
  - add ADR 064 composability tests (nullable predicate
    column + nullable projected column).
- `tests/row_group_materialization.rs`:
  - page-local zip correctness tests;
  - storage-column projection order tests;
  - Case C no-overlap dense-fallback tests;
  - bypass-mask-application architectural test.
- `tests/encoding_secondary_index.rs`:
  - confirm ADR 052 path does not engage Phase B dense
    decode and is unaffected by ADR 065.
- `tests/encoding_chunk_layout.rs`:
  - dense-predicate-zip composes with every closed ADR
    061 chunk size.
- `tests/architecture_test.rs`:
  - ADR 065 lockdown guards.
- `benches/row_group_fetch.rs`:
  - dense-fallback selectivity sweep + dense-fallback
    evaluator-catalog fixtures + synthetic always-dense-
    fallback fixture (regression-stable across native-
    evaluator widening) + ADR 063 `Maybe`-driven dense
    fixture (separate row from `ExactMatch`-driven) +
    ADR 064 nullable-zip fixture + Maybe-survivor decline
    control + page-boundary fixture (1,025 rows/page) +
    variable-width predicate + sparse projection fixture
    + 16-column wide-table diagnostic + clustered-50%
    diagnostic mask shape.
- `plans/impl/065_dense_predicate_zip_with_sparse_projection_bench_results.md`:
  - Phase 7 evidence artifact (at promotion only).
- `plans/adr/060_sparse_projection_fetch_narrowing.md`:
  - §"Where we are" amendment after promotion.
- `plans/adr/063_page_level_predicate_stats.md`:
  - §"Where we are" amendment after promotion.
- `plans/adr/064_nullable_sparse_projection.md`:
  - §"Where we are" amendment after promotion.
- `plans/wip/roadmap.md`:
  - planning-cadence update at promotion.

## Non-goals

- No predicate evaluator changes (no FSST / ALP / RLE-
  value-run / Float native kernel work).
- No predicate-side fetch narrowing (sparse predicate-
  column fetch is a separate ADR).
- No page-sliced PBC evaluation.
- No PBC re-evaluation after Phase B.
- No variable-width sparse projection (variable-width on
  the dense predicate side is allowed because it's
  already decoded).
- No nullable variable-width projection.
- No nested nullable projection.
- No re-evaluation of predicates on Maybe survivors.
- No row-group-grain assembly path.
- No filler-based full-row-group sparse reconstruction.
- No mixed sparse / dense projected columns within one
  row-group materialization pass (ADR 065 is mixed
  *predicate-dense + projection-sparse*; the projection
  side itself is uniform sparse).
- No new `L2FetchStrategy`, fallback / reject / catalog
  variant, config field, metric label, IPC metadata key,
  descriptor field, wire type, capability bit, digest
  domain, or `ObjectStore` method.
- No Cargo feature, nightly rustc dependency, public PBC
  opcode, or operator-visible engagement knob.
- No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.
- No tolerant or legacy decoder.
- No envelope `format_version` bump.
- No retirement of ADR 060 / 064 mask-only path.
- No retirement of `zip_phase_b_and_phase_c_projected_row_group`,
  `apply_phase_b_masks_to_projected_row_groups`, or
  `PhaseBOutcome` phase-named identifiers (broader
  convention sweep, not bundled).
- No migration sweeper.
- No Toron-visible contract change.
- No internal decline reasons leaking to public surface.
- No third stats class.
- No fold of predicate bytes into the projection economic
  gate.

## Phase 0 — Contract audit

Phase 0 read-only audit completed 2026-05-09. Findings:

**Production symbols verified:**

- `SparseProjectionFetchDecline::DensePredicateBatchPresent`
  exists at
  `src/storage/encoding/sparse_projection_fetch.rs:65`.
- `SparseProjectionFetchInput.dense_predicate_batches_present`
  exists at
  `src/storage/encoding/sparse_projection_fetch.rs:91`.
- The decline gate fires at
  `src/storage/encoding/sparse_projection_fetch.rs:120-122`:

  ```rust
  if input.dense_predicate_batches_present {
      return Decline(SparseProjectionFetchDecline::DensePredicateBatchPresent);
  }
  ```

  ADR 065 lifts this gate.
- `SparseProjectionFetchPlan` exists at
  `src/storage/encoding/sparse_projection_fetch.rs:39`.
- `SparseProjectionFetchInput` exists at line 81 with
  fields `record_directory`, `page_directory`,
  `encoding_directory`, `requirement`,
  `predicate_outcome`, `selected_groups`,
  `dense_body_ranges`, `row_group_config`,
  `encoding_config`, plus the
  `dense_predicate_batches_present` field this ADR
  removes.

**Manager-side symbols verified:**

- Local variables use semantic names already
  (`src/storage/manager.rs:9720-9728`):
  - `predicate_fetched_groups: Vec<...>`
  - `predicate_fetch: Option<...>`
  - `predicate_fetched_requirement: ColumnFetchRequirement`
  - `predicate_projection_by_group: HashMap<...>`
  - `predicate_outcome: PhaseBOutcome`
- The `dense_predicate_batches_present` flag is computed
  at `src/storage/manager.rs:10043` from `predicate_outcome`
  shape and passed to the planner input at line 10057.
- The remaining projection requirement is computed at
  `src/storage/manager.rs:10017` via
  `requirement.excluding(&predicate_fetched_requirement)`.
- `materialize_sparse_projection_pages` exists at
  `src/storage/manager.rs:12249`. The function returns a
  single concatenated batch via per-page filtering
  inside; the empty-plan and zero-survivor paths use
  `empty_projected_batch_for_requirement` at lines
  12258 and 12347.
- `apply_phase_b_masks_to_projected_row_groups` exists
  at `src/storage/row_group.rs:1152`.
- `zip_phase_b_and_phase_c_projected_row_group` exists
  at `src/storage/row_group.rs:1236`.
- The manager applies the row-group survival mask via
  `apply_phase_b_masks_to_projected_row_groups` at
  `src/storage/manager.rs:10235` and 10308; ADR 065 Case
  B and Case C bypass these call sites.

**Page-grain row-span addressing verified:**

- `EncodingPageDescriptor.first_value_index` and
  `value_count` (`src/storage/encoding/page.rs:24-25`)
  use `BufferValueIndex` and `BufferValueCount` newtypes;
  page-grain row-span addressing is exact.
- ADR 064's `rebase_validity_bitmap` exists at
  `src/storage/encoding/page_materialize.rs:231` and is
  reusable for the sparse projection side of the zip.

**Dense-predicate-decode catalog (engagement surface) —
narrowed:**

The `dense_predicate_batches_present` flag is set whenever
`predicate_outcome.decoded_predicate_batches` is non-empty,
but the field that ADR 065 zip actually consumes —
`predicate_projection_by_group` — has exactly one
production insertion site: `manager.rs:9931`, inside the
**scalar dense-decode branch**. The encoding-compute
paths (`manager.rs:8998`, `9154`, `9219`, `9382`) build
`PhaseBOutcome::default()` and insert only
`survival_masks`; they return the outcome at
`manager.rs:9806` without populating
`predicate_projection_by_group`. The encoding-compute
paths cover ADR 058 / 059 native predicates *and* ADR 063
`Maybe`-page dense COCD — those paths emit mask-only
`PhaseBOutcome` and engage **Case A**, not Case B/C.

The V1 dense-predicate-decode catalog for ADR 065 Case B/C
engagement therefore narrows to:

1. **Scalar dense-decode branch** (`manager.rs:9931`).
   This branch fires when the predicate is over a type
   the encoding-compute path doesn't handle, or the
   encoding-compute fast path declines and the reader
   falls back to materializing the predicate column as a
   dense Arrow `ProjectedRecordBatch` for predicate
   evaluation.

The scalar dense-decode branch produces a survival mask at
row precision via decode-then-scan over the fully-decoded
predicate column; **exact-mask invariant holds for that
branch**.

The following paths produce mask-only `PhaseBOutcome` (no
`predicate_projection_by_group` entries) and engage Case A
in V1, **not Case B/C**:

- ADR 058 / 059 native predicate evaluators (in-kernel
  mask-only output);
- ADR 058 / 059 pre-kernel eligibility decline routed to
  the encoding-compute dense COCD fallback (mask-only
  output);
- ADR 063 `Maybe`-page dense COCD (encoding-compute
  branch; mask-only output);
- ADR 063 `Maybe`-page dense fallback if it lives in the
  encoding-compute branch (mask-only output).

Materializing predicate projections from the encoding-
compute branch so that ADR 065 Case B can engage on those
queries is **V2 work** — it would require the encoding-
compute branch to populate `predicate_projection_by_group`
alongside its survival mask. Out of V1 scope.

**Naming reconciliation (resolved):**

The Phase 0 audit confirmed that the production code has
already partially completed the convention sweep:
manager-side local variables use semantic names (e.g.,
`predicate_fetched_requirement`,
`predicate_projection_by_group`,
`predicate_fetch`, `predicate_outcome`). The residual
phase-named identifiers — `PhaseBOutcome` (struct),
`apply_phase_b_masks_to_projected_row_groups` (function),
`zip_phase_b_and_phase_c_projected_row_group` (function),
and `phase_c_requirement` (parameter in
`row_group_phase_record_body_ranges`) — are slated for
the broader convention sweep flagged by ADR 060. ADR 065
does not touch them.

**Helper module placement (committed):**

- `filtered_predicate_page_batch` lives in
  `src/storage/encoding/page_materialize.rs` (single-
  source guard with ADR 064 nullable rebase helpers).
- `zip_predicate_and_projection_columns` lives in
  `src/storage/row_group.rs` (adjacent to existing dense-
  zip helper).

**Total-query gate pre-modeling:**

On representative wide-table fixtures (one Float
predicate column projected + four Int64 sparse-projected
columns at clustered 5% selectivity, `Scan` profile,
65,536 rows), the predicate decode bytes scale with the
single predicate column (~500 KiB) and the dense-
projection baseline scales with 5 columns (~2.5 MiB).
Sparse projection cuts the projection-side bytes to ~5%
of dense (~125 KiB). Total-query plaintext bytes
therefore drop from ~3 MiB (full dense) to ~625 KiB (zip)
— a 4.8× total-query reduction at clustered 5%. The
1.25× promotion gate is achievable without being
trivially exceeded; council-revised gate is not needed.

**Engagement surface:** non-trivial. ADR 065 proceeds.

## Phase 1 — Predicate-batch row-span slicing

Add the passive predicate-batch slicing helper to
`page_materialize.rs` in isolation, before any planner
change.

Files:

- `src/storage/encoding/page_materialize.rs`
- focused unit tests in
  `tests/encoding_materialize.rs`.

Work:

1. Add `filtered_predicate_page_batch`:

   ```rust
   pub(crate) fn filtered_predicate_page_batch(
       predicate_batch: &ProjectedRecordBatch,
       page: EncodingPageDescriptor,
       page_local_survivors: &EncodedRowMask,
   ) -> Result<ProjectedRecordBatch, ArrowError>;
   ```

   - slices each predicate Arrow column to
     `[page.first_value_index, page.first_value_index +
     page.value_count)` via O(1) Arrow array slicing;
   - applies the page-local survivor mask using existing
     dense mask-filter helpers;
   - returns a page-local predicate-only
     `ProjectedRecordBatch` whose row count equals the
     survivor count for that page.
2. Validate bounds:
   - page row span fits within batch row count;
   - page-local survivor mask row count matches
     `page.value_count`;
   - returned record batch row count matches survivor
     count.
3. Use existing Arrow array-slice + filter helpers; no
   new bitmap algebra. Variable-width predicate columns
   work unchanged (Arrow array slicing is type-uniform).

Tests:

- byte-aligned page start, mixed survivors;
- non-byte-aligned page start, mixed survivors;
- 1,025-rows-per-page (page starts move across byte
  boundaries);
- tail page with non-multiple-of-8 row count;
- all-survivor page returns full sliced row count;
- no-survivor page returns zero-row batch (degenerate
  case);
- multi-column predicate batch retains column ordering;
- variable-width (UTF-8 / Binary) predicate column
  slices correctly;
- nullable predicate column preserves dense-decoded
  validity buffer;
- `EncodedRowMask::Bitmap` and `EncodedRowMask::Runs`
  both produce identical sliced output;
- predicate batch shorter than page span fails closed.

**Exit gate:** Helper proves page-local predicate columns
can be sliced from row-group-level batches at byte-
aligned and non-byte-aligned page boundaries. `cargo
test --test encoding_materialize` passes.

## Phase 2 — Sparse projection page-batch helper

Split `materialize_sparse_projection_pages` into a per-
page helper + concat wrapper.

Files:

- `src/storage/manager.rs`
- optionally `src/storage/encoding/page_materialize.rs`

Work:

1. Add the per-page tagged batch type:

   ```rust
   struct SparseProjectionPageBatch {
       row_group_index: RowGroupIndex,
       page: EncodingPageDescriptor,
       local_mask: BooleanArray,
       projected: ProjectedRecordBatch,
   }
   ```

   The `projected` field is already filtered by
   `local_mask`.
2. Split current `materialize_sparse_projection_pages`
   into:
   - a lower helper that returns
     `Vec<SparseProjectionPageBatch>` tagged by
     `(row_group_index, page_index)`;
   - the existing concat wrapper for Case A and Case C
     paths.
3. Preserve ADR 064 validity handling exactly:
   - decode selected value pages;
   - fetch and rebase selected raw validity pages;
   - use all-valid nullable page markers when row-group
     null count is zero.
4. Keep output order `(row_group_index, page_index)`.
5. Preserve empty-plan behavior via
   `empty_projected_batch_for_requirement`.
6. Keep current `materialize_sparse_projection_pages` as
   a wrapper that concatenates page batches for Case A
   and Case C (no behavior change for those cases).
7. Do not emit row-group-length projection arrays with
   null/default fillers for pages that were not fetched.

Tests:

- sparse page batch helper returns one page batch per
  selected page;
- `local_mask` row count equals page row count;
- filtered page batch row count equals `local_mask`
  true count;
- zero-true local page is handled gracefully (returned
  with zero rows or skipped);
- nullable page-local validity still matches dense
  oracle;
- ADR 060 / 064 mask-only path output byte-identical
  pre / post.

**Exit gate:** Sparse projection can be materialized as
page-tagged, already-filtered batches without changing
ADR 060 / 064 behavior.

## Phase 3 — Page-local zip assembly

Add the zip helper.

Files:

- `src/storage/row_group.rs`
- `src/storage/encoding/page_materialize.rs` (predicate-
  side helper from Phase 1)
- `tests/row_group_materialization.rs`

Work:

1. Add the semantic zip helper:

   ```rust
   pub(crate) fn zip_predicate_and_projection_columns(
       predicate: &ProjectedRecordBatch,
       projection: &ProjectedRecordBatch,
       requirement: &ColumnFetchRequirement,
   ) -> std::io::Result<ProjectedRecordBatch>;
   ```

   - builds a `ProjectedRecordBatch` whose columns
     follow `requirement` order;
   - draws each column from either `predicate` or
     `projection` based on which source contains it;
   - does not allocate new column data — reorders
     existing `ArrayRef`s into the requirement's column
     order;
   - validates row-count agreement
     (`predicate.num_rows() == projection.num_rows()`);
     mismatch is fail-closed.
2. Wire the helper to delegate to the existing
   `zip_phase_b_and_phase_c_projected_row_group` if the
   underlying logic is identical (broader convention
   sweep retires the phase-named identifier; ADR 065
   does not bundle that retirement).
3. The helper is reusable by ADR 052 secondary-index
   materialization if it ever needs page-local dense +
   sparse composition (single-source guard).

Tests:

- predicate column + projection column at side-by-side
  schema position;
- predicate column at index 0, projection column at
  index 1 (predicate-first schema);
- projection column at index 0, predicate column at
  index 1 (projection-first schema);
- interleaved schema: predicate, projection, predicate,
  projection;
- multi-column predicate + multi-column projection;
- nullable predicate column + non-null projection
  column;
- non-null predicate column + nullable projection
  column;
- nullable + nullable composition;
- variable-width predicate column + fixed-width sparse
  projection column;
- storage-column projection order preserved;
- row-count-mismatch fails closed;
- no-survivor page produces zero-row composed batch
  with correct full schema.

**Exit gate:** Page-local composed batches can be
constructed from any combination of sparse projection
columns and filtered dense predicate columns, in any
`ColumnFetchRequirement` order.

## Phase 4 — Planner gate lift

Lift the `dense_predicate_batches_present` decline in
the sparse projection planner.

Files:

- `src/storage/encoding/sparse_projection_fetch.rs`
- `src/storage/encoding/page_materialize.rs`

Work:

1. Replace the `dense_predicate_batches_present` decline
   gate (lines 120-122) with the new engagement gate:
   - mask exact + remaining projection requirement non-
     empty + remaining projection eligible → engage
     sparse projection (planner does not consult
     `predicate_projection_by_group`; the manager-side
     execution selects the case);
   - mask exact + remaining projection empty (predicate-
     only query) → no sparse projection planning needed;
   - mask not exact (any Maybe survivors remain) →
     decline with private `MaybeMaskSurvivor` reason;
   - decoded predicate batch covers multiple row groups
     → decline with private
     `PredicateBatchRowGroupSplit` reason (defensive
     guard);
   - decoded predicate batch row-span slicing infeasible
     → decline with private
     `PredicateBatchRowSpanMismatch` reason.
2. Preserve existing declines for:
   - variable-width sparse-projection columns;
   - non-page-emitting value cascades;
   - cascades outside ADR 060's sparse projection
     engagement set;
   - cross-column value-page geometry mismatch;
   - missing exact masks;
   - range / byte / page cap misses;
   - encoded validity buffers (ADR 064);
   - `column_null_counts == 0` nullable Arrow field
     stays on the existing all-valid sparse path
     (ADR 064).
3. **Remove** the `dense_predicate_batches_present`
   field from `SparseProjectionFetchInput` (line 91).
   The planner no longer needs to know whether dense
   predicate batches exist; case dispatch is manager-
   side.
4. **Retire** active use of
   `SparseProjectionFetchDecline::DensePredicateBatchPresent`
   (line 65). Keep the variant for transitional test-
   fixture compatibility if needed; remove if all
   fixtures migrate cleanly.
5. Keep `SparseProjectionFetchDecline` private. New
   private reasons (`MaybeMaskSurvivor`,
   `PredicateBatchRowSpanMismatch`,
   `PredicateBatchRowGroupSplit`) do not surface in
   metrics or public fallbacks.
6. Preserve mask-first behavior:
   - `None` mask returns an empty sparse plan before
     zip checks;
   - `All` mask declines dense before value / validity /
     zip planning;
   - ADR 063 `ExactMatch`-routed row groups never reach
     ADR 065's planner branch (they bypass dense decode
     upstream).
7. Economic gate unchanged:
   `sparse_projection_plaintext_bytes <
   dense_projection_plaintext_bytes`. Predicate bytes
   appear on both sides identically and cancel.

Tests:

- `dense_predicate_zip_engages_with_exact_mask`.
- `dense_predicate_zip_declines_maybe_survivor` (private
  reason).
- `dense_predicate_zip_declines_predicate_batch_row_span_mismatch`
  (private reason).
- `dense_predicate_zip_declines_predicate_batch_row_group_split`
  (private reason).
- `dense_predicate_zip_declines_when_remaining_projection_ineligible`.
- `dense_predicate_zip_declines_when_economic_gate_fails`.
- `dense_predicate_zip_none_mask_zero_projection_fetch`.
- `dense_predicate_zip_all_mask_routes_dense`.
- `dense_predicate_zip_economic_gate_unchanged_pre_post`.
- `mask_only_sparse_projection_byte_identical_pre_post`
  (regression).
- `dense_predicate_zip_engages_for_each_dense_decode_catalog_member`
  (parameterized over Phase 0 catalog).

**Exit gate:** Planner produces complete sparse plans
(consuming masks + projection metadata only) before any
L2 fetch or page materialization occurs. `cargo test
--test row_group_sparse_projection_fetch` passes.

## Phase 5 — Manager integration

Thread three-case manager dispatch into the existing
sparse projection read path.

Files:

- `src/storage/manager.rs`
- `src/storage/encoding/page_materialize.rs`

Work:

1. Allow sparse projection planning when:
   - not point-read;
   - remaining projection requirement non-empty;
   - exact survival masks present;
   - `predicate_projection_by_group` may or may not be
     populated.
2. Keep point-read unchanged.
3. After a sparse plan is selected, dispatch on the
   case triple:
   First compute the `predicate_output_overlap` derived
   set:

   ```rust
   let predicate_output_overlap: ColumnOrdinalSet =
       predicate_fetched_requirement
           .predicate_columns
           .intersection(&requirement.output_columns)
           .copied()
           .collect();
   ```

   This is **the** Case B/C discriminator. (Note:
   `predicate_fetched_requirement.is_empty()` would be the
   wrong check — it's built from
   `requirement.predicate_columns` and storage columns at
   `manager.rs:9853` and is non-empty whenever predicate
   fetch happened, regardless of output overlap.
   `predicate_fetched_requirement` retains its current
   role for excluding already-fetched columns from
   projection fetch via
   `requirement.excluding(&predicate_fetched_requirement)`
   at `manager.rs:10017`.) Then dispatch:

   - **Case A — mask-only**:
     `predicate_projection_by_group` is empty. Use
     existing ADR 060 / 064 mask-only sparse return
     path (concat wrapper). Behavior byte-identical
     pre / post ADR 065. This case also covers all
     encoding-compute paths (ADR 058 / 059 native, ADR
     063 Maybe-page dense COCD) because those paths emit
     mask-only `PhaseBOutcome` and never populate
     `predicate_projection_by_group` in V1.
   - **Case B — predicate/output-overlap zip**:
     `predicate_projection_by_group` is populated AND
     `predicate_output_overlap` is non-empty (some
     predicate columns appear in the output). Run the
     page-slice zip path:
     - extract per-page `SparseProjectionPageBatch`
       values (Phase 2 helper);
     - per page, compute the page-local mask slice from
       the row-group survival mask;
     - call `filtered_predicate_page_batch` (Phase 1)
       using the same `local_mask` to slice + filter
       the dense predicate batch (only columns named in
       `predicate_output_overlap`; predicate columns
       fetched but not in output are discarded at
       slice time);
     - call `zip_predicate_and_projection_columns`
       (Phase 3) to assemble the page-local
       `ProjectedRecordBatch` in
       `ColumnFetchRequirement` order;
     - concatenate per `(row_group_index, page_index)`.
   - **Case C — no-overlap dense-fallback**:
     `predicate_projection_by_group` is populated AND
     `predicate_output_overlap` is empty (predicate
     columns fetched dense for evaluation only; the
     fetched predicate columns do not appear in the
     user-visible output). Skip the zip step and
     concatenate per-page sparse-projected batches as
     in Case A. Dense predicate decode bytes remain
     sunk; projection-side bytes still narrow.
4. Ensure `RowGroupFetchPlan.phase_c_fetch` records only
   sparse projection selected ranges; `phase_b_fetch`
   continues to record the dense predicate fetch plan.
5. **Bypass `apply_phase_b_masks_to_projected_row_groups`
   for ADR 065 Case B and Case C output.** The page-
   local filter has already been applied; double-
   filtering would corrupt the result.
6. Preserve metrics by using existing row-group and
   encoding counters. Do not add sparse-zip-specific
   labels.
7. Ensure the strategy remains
   `L2FetchStrategy::RowGroup`.
8. Return zero-row full-schema batches via
   `empty_projected_batch_for_requirement` when masks
   prove no survivors (`None` mask or all-zero
   `Bitmap` / `Runs`).
9. Post-fetch invariant failures fail closed through
   existing storage / Arrow errors and existing
   encoding invariant metrics. Do not silently retry
   full-dense after partial zip fetch begins.

Tests:

- **Case A (regression)**: ADR 060 / 064 mask-only
  sparse path unchanged when
  `predicate_projection_by_group` is empty.
- **Case B (overlap zip)**: runtime encrypted L2
  sparse read with one Float predicate column (also
  projected) + Int64 projection column returns dense-
  oracle-equivalent output.
- **Case B**: one synthetic-dense predicate column
  (also projected) + fixed-width projection column.
- **Case A regression (not Case B)**: ADR 063 `Maybe`-
  page dense COCD continues to emit mask-only
  `PhaseBOutcome` via the encoding-compute branch and
  engages Case A mask-only sparse projection; ADR 065
  zip is not engaged on these fixtures in V1.
- **Case B**: ADR 064 nullable predicate column +
  nullable projected column zip produces dense-oracle-
  equivalent output.
- **Case B**: variable-width predicate column (also
  projected) + fixed-width sparse projection column.
- **Case C (no-overlap dense-fallback)**: synthetic-
  dense predicate column NOT in output + fixed-width
  projection column → sparse projection engages, no zip
  step, output equals Case A shape.
- clustered mask selects one page and fetches projection
  value plus validity bytes only;
- random weak mask declines to dense and output
  equality holds;
- `None` mask with `predicate_projection_by_group`
  populated performs zero projection range GETs and
  zero predicate-batch slicing; returns full-schema
  empty batch;
- `All` mask with `predicate_projection_by_group`
  populated routes dense;
- point-read path unchanged;
- multi-row-group fixture preserves
  `(row_group_index, page_index)` ordering;
- `apply_phase_b_masks_to_projected_row_groups` not
  invoked on ADR 065 Case B / C paths (assertion).

**Exit gate:** Production read path materializes dense-
predicate-zip output (Case B) and no-overlap fallback
(Case C) only from fetched selected ranges + existing
dense predicate batches and remains byte-identical to
full-dense projection.

## Phase 6 — Architecture guards

Land ADR 065 guards.

Files:

- `tests/architecture_test.rs`.

Guards:

- `dense_predicate_batches_present` /
  `DensePredicateBatchPresent` is no longer the gate for
  sparse projection engagement.
- ADR 060 / 064 mask-only sparse projection still
  engages when `predicate_projection_by_group` is empty
  and the mask is exact.
- Sparse projection planner module location unchanged.
- Sparse projection planner consumes only survival
  masks + projection metadata; no decoded Arrow batches
  reach the planner.
- `filtered_predicate_page_batch` lives in
  `src/storage/encoding/page_materialize.rs`; ADR 052
  secondary-index path imports it if needed (single-
  source guard).
- `zip_predicate_and_projection_columns` lives in
  `src/storage/row_group.rs`.
- `SparseProjectionFetchDecline` remains private to the
  planner module.
- Maybe-survivor decline reason exists, is private, and
  routes the row group to dense.
- Row-group-grain assembly path absent; page-grain
  only.
- Filler-based full-row-group sparse reconstruction
  absent.
- ADR 065 Case B zipped output and Case C sparse-only
  output never pass through
  `apply_phase_b_masks_to_projected_row_groups`
  (assertion on the manager-side code path).
- No new `L2FetchStrategy` variant.
- No new `EncodedRowMask` variant.
- No new public fallback / reject / catalog variant.
- No new config field on row-group, encoding, compute,
  or chunk-layout configs.
- No new metric label.
- No new IPC `custom_metadata` key.
- No `EncodingDirectory` shape extension.
- No new wire `MsgType` / `FRAME_FLAG_*` /
  `NodeCapability` bit / digest domain / `ObjectStore`
  trait method.
- No new descriptor field / fingerprint surface change.
- No new public PBC opcode.
- No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.
- No Cargo feature gating dense-predicate-zip
  engagement.
- No nightly rustc dependency.
- New helper names avoid planning-phase identifiers and
  version suffixes (`zip_predicate_and_projection_columns`,
  `filtered_predicate_page_batch`,
  `SparseProjectionPageBatch`, `MaybeMaskSurvivor`,
  `PredicateBatchRowSpanMismatch`,
  `PredicateBatchRowGroupSplit` are semantic).
- No project-name-prefixed protocol identifiers.
- ADR 020 §2 AAD layout, ADR 022 §3 / §5 byte layouts,
  ADR 056 / 057 / 063 directory shapes, ADR 058 / 059
  evaluator catalogs, ADR 060 / 064 sparse projection
  eligibility, ADR 061 closed chunk-size set, ADR 062
  `RowGroupSizingProfile` catalog: all unchanged.
- Cross-replica byte-equal output across two writers
  with the same config + dense-predicate-zip fixture.
- Predicate evaluation paths untouched (no PBC re-
  evaluation after Phase B).
- ADR 060 / 064 mask-only sparse projection byte-
  identical pre / post on shared fixtures.
- No retirement of phase-named helpers
  (`zip_phase_b_and_phase_c_projected_row_group`,
  `apply_phase_b_masks_to_projected_row_groups`,
  `PhaseBOutcome`, `phase_c_requirement` parameter);
  they are wrapped or delegated to, not renamed.

**Exit gate:** `cargo test --test architecture_test`
passes.

## Phase 7 — Focused tests

Focused test commands:

```sh
cargo test sparse_projection_fetch --lib
cargo test page_materialize --lib
cargo test --test row_group_sparse_projection_fetch
cargo test --test encoding_materialize
cargo test --test row_group_materialization
cargo test --test row_group_fetch_plan
cargo test --test architecture_test
cargo test --test encoding_secondary_index
cargo test --test encoding_chunk_layout
cargo test --test encoding_compute_test
```

Required coverage:

- planner accepts row groups with non-empty
  `predicate_projection_by_group` when mask is exact and
  remaining projection eligible;
- planner declines Maybe-survivor row groups with
  private reason;
- planner declines predicate-batch-row-group-split row
  groups with private reason;
- planner declines predicate-batch-row-span-mismatch row
  groups with private reason;
- planner preserves `None` and `All` mask behavior;
- predicate-batch row-span slicing handles non-byte-
  aligned page starts and tail pages;
- variable-width predicate column slices correctly;
- page-local zip assembly handles every output-schema
  column ordering (predicate-first, projection-first,
  interleaved);
- nullable predicate column composes;
- nullable projection column composes (ADR 064 helper
  reused);
- dense-zip output equals full-dense output for:
  - all-survivor pages;
  - no-survivor pages (degenerate);
  - mixed survivors;
  - alternating nulls (predicate or projection);
  - clustered nulls;
  - multiple predicate columns + multiple projection
    columns;
  - variable-width predicate column + fixed-width
    sparse projection;
  - storage-column projection order;
- ADR 063 control rows (regression, not zip
  composability):
  - ADR 063 `Maybe`-driven dense COCD produces mask-only
    `PhaseBOutcome` via the encoding-compute branch
    (`manager.rs:8998` / `9219`); does NOT populate
    `predicate_projection_by_group`; engages Case A
    mask-only sparse projection in V1, not Case B/C;
  - ADR 063 `ExactMatch`-routed row groups skip dense
    decode entirely; ADR 065 not engaged;
  - multiplicative ADR 063 + ADR 065 zip composition
    (Maybe → Case B) is V2 work, out of V1 scope.
- ADR 061 chunk sizes compose with dense-predicate-zip
  ranges;
- cross-replica dense-zip output is byte-identical on
  the same config and input;
- ADR 052 secondary-index materialization unaffected;
- `apply_phase_b_masks_to_projected_row_groups` not
  invoked on ADR 065 Case B / C path.

**Exit gate:** All focused tests pass and existing ADR
060 / 063 / 064 tests remain green.

## Phase 8 — Bench matrix

Extend `benches/row_group_fetch.rs` with dense-
predicate-zip rows. Land evidence at promotion time in:

```text
plans/impl/065_dense_predicate_zip_with_sparse_projection_bench_results.md
```

Bench dimensions:

- row-group profile: `PointRead`, `Scan`.
- row counts: 8K, 65K, 1M (multi-record tier if runtime
  bench cost permits).
- page geometry:
  - default page size;
  - non-byte-aligned page boundaries (1,025
    rows/page).
- predicate-evaluator path:
  - Float comparison (dense fallback);
  - FSST predicate (dense fallback; if constructible);
  - ALP predicate (dense fallback; if constructible);
  - RLE-value-run predicate (dense fallback);
  - mixed-cascade Dictionary predicate (dense
    fallback);
  - **synthetic always-dense predicate fixture**
    (regression-stable across native catalog
    widening).
- predicate column count: 1 / 4 / 8.
- projection column count: 1 / 4 / 16 (16 = wide-table
  diagnostic).
- predicate column shape (also projected, predicate/
  output overlap):
  - fixed-width predicate column projected;
  - nullable fixed-width predicate column projected;
  - variable-width predicate column projected (allowed
    on dense side);
  - storage-column projection control.
- remaining projection shape (excluding predicate
  columns):
  - one fixed-width non-null sparse column;
  - one nullable fixed-width sparse column;
  - four sparse projection columns;
  - mixed nullable + non-null;
  - variable-width remaining projection (declines).
- mask shape:
  - clustered 1% / 5% / 25% (correctness + economic
    gates);
  - clustered 50% diagnostic (where dense-zip win
    narrows or inverts);
  - random 1% / 5%;
  - `None`;
  - `All`.
- ADR 063 control rows (regression, not zip
  composability):
  - `ExactMatch`-driven (mask-only; ADR 060 / 064 Case A
    engages);
  - `Maybe`-driven dense COCD (encoding-compute branch
    emits mask-only `PhaseBOutcome`; engages Case A in
    V1, NOT Case B/C zip; multiplicative composition is
    V2 work).
- ADR 064 composability: nullable predicate column +
  nullable projection column zip.
- Maybe-survivor decline control: a fixture where
  Maybe-mask slices remain after upstream evaluation,
  declining with the private reason.
- Case C no-overlap dense-fallback control.

Rows record:

- route (`case_a_sparse_mask_only`, `case_b_sparse_zip`,
  `case_c_sparse_no_overlap`,
  `dense_decline_<reason>`,
  `none_zero_projection_fetch`,
  `dense_decline_all`);
- dense predicate plaintext bytes (sunk; reported for
  diagnostic);
- dense remaining-projection plaintext bytes (full-
  dense baseline);
- sparse selected value plaintext bytes;
- sparse selected validity plaintext bytes;
- sparse selected projection plaintext bytes (value +
  validity);
- total dense query plaintext bytes (predicate + dense
  remaining projection);
- total sparse query plaintext bytes (predicate +
  sparse remaining projection);
- ciphertext bytes (full-dense / dense-zip);
- selected ranges;
- selected value pages;
- selected validity ranges;
- output row count;
- output equality digest;
- null-count equality;
- cross-replica output digest when applicable;
- full-dense wall-clock;
- dense-zip wall-clock;
- range GET calls / bytes;
- `get_bytes` calls.

Acceptance gates (three axes):

- Output equality with full-dense projection on every
  correctness row.
- Null counts match full-dense projection on every
  nullable row.
- `None` mask fetches zero projection bytes AND skips
  predicate-batch slicing AND returns full-schema empty
  batch.
- `All` mask routes dense.
- Case C rows skip predicate slicing and preserve Case
  A output shape.
- **Promotion gate (axis 1)**: clustered 5% dense-zip
  rows keep at least 1.5× *remaining-projection*
  plaintext-byte reduction (value + validity combined)
  on representative wide-table fixtures (BitPacked +
  Dictionary cascades).
- **Promotion gate (axis 2)**: clustered 5% dense-zip
  rows keep at least 1.25× *total query* plaintext-byte
  reduction (dense predicate + projection) on the same
  fixtures.
- **Promotion gate (axis 3)**: clustered 5% dense-zip
  rows keep at least 1.2× wall-clock improvement on
  representative runtime fixtures (or council records a
  revised gate).
- **Headline diagnostic** (not gating): the artifact
  also reports whether ≥ 3× remaining-projection
  plaintext-byte reduction holds on best-case
  clustered wide-table fixtures.
- Dense-decline overhead ≤ 5% on random / weak rows
  (vs. full-dense projection).
- Mask-only sparse projection rows remain within ADR
  060 / 064's existing envelope (byte-identical
  regression).
- ADR 063 control rows: ExactMatch-driven and Maybe-
  driven dense COCD both engage Case A mask-only sparse
  projection (regression confirmation; combined
  multiplicative ADR 063 + ADR 065 byte savings is V2
  scope, not V1).
- ADR 064 composability rows: nullable + dense-zip
  composes byte-identically.
- ADR 061 / 062 composability rows stay inside their
  existing envelopes.
- Cross-replica determinism on representative dense-zip
  fixtures.
- Maybe-survivor decline control row records the
  private decline reason and routes to dense.
- Sub-byte page-boundary correctness on row counts not
  divisible by 8.
- `PointRead` rows record outcome; promotion does not
  require `PointRead` default engagement if predicate
  decode cost erases the win, but behavior must be
  byte-identical and deterministic.

**Exit gate:** Bench artifact provides enough evidence
for council promotion or explicit revision.

## Phase 9 — Promotion

Promotion happens only after implementation, focused
tests, architecture guards, and bench evidence land
together.

Files:

- `plans/adr/065_dense_predicate_zip_with_sparse_projection.md`
  — Status amendment to "shipped" with bench evidence
  reference.
- `plans/impl/065_dense_predicate_zip_with_sparse_projection.md`
  — Status amendment.
- `plans/impl/065_dense_predicate_zip_with_sparse_projection_bench_results.md`
  — Phase 8 evidence artifact.
- `plans/adr/060_sparse_projection_fetch_narrowing.md`
  — §"Where we are" amendment.
- `plans/adr/063_page_level_predicate_stats.md` —
  §"Where we are" amendment.
- `plans/adr/064_nullable_sparse_projection.md` —
  §"Where we are" amendment.
- `plans/wip/roadmap.md` — planning-cadence update.

Work:

1. Pin the bench-validated parameters in the bench
   results artifact.
2. Amend ADR 060 §"Where we are":
   - dense-predicate-zip with sparse projection is
     shipped;
   - mask-only Phase B contract is amended; predicate-
     decoded paths can engage sparse projection;
   - sparse predicate-column fetch, FSST / ALP / RLE-
     value-run native evaluators, variable-width sparse
     projection remain V2.
3. Amend ADR 063 §"Where we are":
   - composition pathway with `Maybe`-driven dense
     decode + ADR 065 zip recorded;
   - combined byte-savings evidence linked from the
     bench artifact.
4. Amend ADR 064 §"Where we are":
   - composition pathway with nullable predicate column
     + nullable projection column zip recorded.
5. Mark ADR 065 shipped only after code, tests, and
   bench evidence land together.
6. Update the roadmap at the planning-cadence boundary.

**Exit gate:** ADR 065 promoted as a narrow ADR 060 /
063 / 064 amendment with no public protocol surface
change.

## Verification gate

Before promotion:

```sh
cargo fmt --all
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

**Maybe-survivor decline misses target rows.**
Mitigation: Phase 0 audited the frequency of Maybe
survivors across the dense-predicate-decode catalog;
all catalog members produce exact masks via decode-
then-scan or decode-then-COCD. If post-implementation
production evidence reveals Maybe survivors that should
engage, council weighs bundling predicate-side
widening into a follow-up ADR.

**Page-grain row-span slicing fails on non-byte-aligned
page starts.** Mitigation: Phase 1 helper-level tests
for byte-aligned, non-byte-aligned, 1,025-rows-per-
page, tail-page, and clustered-survivor patterns; full-
dense oracle in runtime tests; cross-replica byte-
equality bench gate.

**Output schema column ordering mismatch.** Mitigation:
the zip helper respects `ColumnFetchRequirement` order
regardless of source ordering; Phase 3 tests cover
predicate-first, projection-first, and interleaved
orderings.

**Predicate decode cost erases the projection win.**
Mitigation: the V1 economic gate is projection-only
(predicate bytes sunk), so the gate captures pure
projection wins. Bench artifact records total query
bytes on a separate axis with its own ≥1.25× promotion
gate. Phase 0 pre-modeling confirmed 1.25× is
achievable (4.8× on representative wide-table fixtures
at clustered 5%).

**Mask-only sparse projection regresses.** Mitigation:
Phase 4 includes a regression test for empty
`predicate_projection_by_group` behavior; Phase 8 bench
includes ADR 060 / 064's existing fixtures byte-
identical pre / post.

**`None` masks regress by checking zip eligibility too
early.** Mitigation: preserve mask-first order; add
tests where `None` mask with
`predicate_projection_by_group` populated returns zero
fetches and zero predicate-batch slicing.

**ADR 063 ExactMatch-routed row groups accidentally
engage ADR 065.** Mitigation: ExactMatch routing skips
dense predicate decode entirely; the resulting
`predicate_projection_by_group` is empty; ADR 060 / 064
mask-only path engages, not ADR 065. Architectural
guard pins this.

**Predicate batch covers multiple row groups.**
Mitigation: defensive private decline reason
(`PredicateBatchRowGroupSplit`); fail closed if the
writer ever produces a batch spanning row groups
(should not occur but pinned).

**Double mask filtering.** Mitigation: ADR 065 Case B
zipped output and Case C sparse-only output bypass
`apply_phase_b_masks_to_projected_row_groups`;
architectural guard asserts the bypass; runtime test
asserts the call is not made on the ADR 065 path.

**Public surface creep.** Mitigation: architecture
guards cover strategy, fallback / reject / catalog,
config, metric, descriptor, wire, capability, digest,
ObjectStore, and directory-shape surfaces. Same
lockdown cadence as ADR 060 / 063 / 064.

**ADR 052 nullable point-read path diverges from
sparse projection materializer.** Mitigation: single-
source guard in architectural lockdown; ADR 052 path
imports the same helpers if it ever needs page-local
dense + sparse composition.

**Cross-replica byte-equality breaks on materializer
non-determinism.** Mitigation: page-local zip assembly
is deterministic over (selected page set, exact
survival mask, dense decoded predicate batches in
`predicate_projection_by_group`, ADR 057 directory,
ADR 064 nullable rebase, output schema ordering); no
runtime branches; Phase 7 / Phase 8 bench rows gate
empirically.

**Variable-width nullable scope creep.** Mitigation:
V1 sparse projection eligibility is fixed-width only
(inherited from ADR 060 / 064); planner declines on
variable-width sparse projection; architectural guard
pins it. (Variable-width on the dense predicate side
is allowed because it's already decoded.)

**ADR 058 / 059 native-evaluator widening invalidates
bench fixtures.** Mitigation: Phase 8 bench matrix
includes a synthetic always-dense-fallback fixture so
the regression baseline survives evaluator widening;
current-catalog fixtures are recorded as drift
markers, not regression gates.

**`materialize_sparse_projection_pages` split breaks
existing callers.** Mitigation: the lower per-page
helper is added alongside the existing concat wrapper;
ADR 060 / 064 callers continue to use the wrapper
unchanged. Phase 0 audited all callers; Phase 2
preserves byte-identical output on the wrapper path.

**Per-page Arrow slicing allocation cost.** Each
`filtered_predicate_page_batch` call produces fresh
`ArrayData` for each predicate column slice. Arrow
array slicing shares underlying buffer memory but
allocates new metadata per slice; filtering produces
new buffers. Across many selected pages on wide
predicate batches, this creates bounded but non-
trivial allocation churn. Mitigation: allocations are
O(selected_pages × predicate_columns) and bounded by
the sparse plan's selected-page count; the bench
artifact records dense-zip wall-clock against full-
dense to catch any regression where slicing churn
erases the projection-byte win. If churn becomes
load-bearing on operational workloads, a follow-up
ADR could pool allocations or batch slicing — out of
scope for V1.

**Manager-side case dispatch confuses Cases B and C.**
Mitigation: Phase 5 dispatches on the derived
`predicate_output_overlap` set
(`predicate_fetched_requirement.predicate_columns ∩
requirement.output_columns`), not on
`predicate_fetched_requirement.is_empty()`. The latter
would misclassify a query whose predicate column is
fetched dense but not in the SELECT list (i.e., real
Case C) as Case B, because
`predicate_fetched_requirement` is built at
`manager.rs:9853` from `requirement.predicate_columns`
and is non-empty whenever any predicate fetch happened
regardless of output overlap. Case A remains gated by
`predicate_projection_by_group.is_empty()`. Tests cover
all three cases independently with dense-oracle equality
checks.

**Phase-named helper retirement scope creep.**
Mitigation:
`zip_phase_b_and_phase_c_projected_row_group`,
`apply_phase_b_masks_to_projected_row_groups`,
`PhaseBOutcome`, and the `phase_c_requirement`
parameter in `row_group.rs` are wrapped / delegated
to, not renamed. Their retirement is the broader
convention sweep, explicitly not bundled with ADR
065.
