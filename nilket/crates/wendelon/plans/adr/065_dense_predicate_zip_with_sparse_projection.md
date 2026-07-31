# ADR 065: Dense Predicate-Zip with Sparse Projection

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/065_dense_predicate_zip_with_sparse_projection.md`

## Status

Shipped 2026-05-09 after council convergence,
implementation, focused verification, and Phase 8 promotion
evidence landed together. Bench evidence is recorded at
`plans/impl/065_dense_predicate_zip_with_sparse_projection_bench_results.md`.
The promotion gate passed on all three axes: clustered 5%
representative rows keep at least 1.5x remaining-projection
plaintext-byte reduction, at least 1.25x total-query
plaintext-byte reduction, and at least 1.2x wall-clock
improvement. V1 engagement is narrowed to the scalar
dense-decode branch that populates
`predicate_projection_by_group`; ADR 063 encoding-compute
paths remain Case A mask-only sparse projection because they
do not populate decoded predicate projection batches.

Council-converged 2026-05-09 after six iterations across
Claude / Codex / Gemini. Phase 0 read-only audit of
`src/storage/manager.rs`,
`src/storage/encoding/sparse_projection_fetch.rs`, and
`src/storage/row_group.rs` confirmed:

- `SparseProjectionFetchDecline::DensePredicateBatchPresent`
  (`sparse_projection_fetch.rs:65`) and the gate at
  `sparse_projection_fetch.rs:120-122` are the V1 cliff this
  ADR lifts;
- `SparseProjectionFetchInput.dense_predicate_batches_present`
  (`sparse_projection_fetch.rs:91`) is the planner-input
  field this ADR removes;
- `predicate_projection_by_group`,
  `predicate_fetched_requirement`, `predicate_fetch`, and
  `predicate_outcome` (`manager.rs:9720-9728`) are the
  manager-side semantic identifiers that already exist in
  production;
- `materialize_sparse_projection_pages`
  (`manager.rs:12249`) and
  `empty_projected_batch_for_requirement`
  (`manager.rs:12258, 12347`) exist as referenced;
- ADR 064's `rebase_validity_bitmap`
  (`page_materialize.rs:231`) is reusable for the sparse
  projection side of the zip;
- `EncodingPageDescriptor.first_value_index` /
  `value_count` (`page.rs:24-25`) provide the page-grain
  row-span addressing required for predicate-batch
  slicing.

`PhaseBOutcome` (struct), `apply_phase_b_masks_to_projected_row_groups`,
`zip_phase_b_and_phase_c_projected_row_group`, and the
`phase_c_requirement` parameter name in
`row_group.rs:1070` are residual phase-named identifiers
slated for the broader convention sweep flagged by ADR 060.
ADR 065 does not extend the leak: new identifiers it
introduces (`filtered_predicate_page_batch`,
`SparseProjectionPageBatch`) are semantic; it references
the existing phase-named symbols by current name only for
accuracy where they are unavoidable.

Implementation is complete. ADR 060 / 063 / 064 status text
and the forward roadmap have been amended at promotion.

## Context

ADR 060, ADR 063, and ADR 064 share a hard precondition for
sparse projection engagement: the predicate-evaluation path
must produce a mask-only outcome with no decoded predicate
batches. When predicate evaluation takes the **scalar dense-
decode path** — the predicate is over a type that the
encoding-compute path doesn't handle, or the encoding-
compute fast path declines and the reader falls back to
materializing the predicate column as a dense Arrow
`ProjectedRecordBatch` for predicate evaluation — the
row-group read path populates
`predicate_projection_by_group` (the single insertion site
at `manager.rs:9931`) and
`SparseProjectionFetchDecline::DensePredicateBatchPresent`
forces the entire row group to dense projection. That
declines sparse projection even when:

- the survival mask is exact (full predicate decode produced
  exact survivors with no probabilistic slack);
- the projected columns satisfy ADR 060 + 064 eligibility
  (fixed-width with optional nullable);
- selected projection bytes vastly outweigh full-dense
  projection bytes for the *remaining* (non-predicate)
  columns.

The motivating workload is the predicate/output-overlap case:

```sql
SELECT predicate_col, projected_col
FROM t
WHERE predicate_col < 10
```

If `predicate_col` falls into the scalar dense-decode
path — typically because its type is outside the encoding-
compute predicate evaluator catalog — the reader decodes it
densely as a `ProjectedRecordBatch` and stores it in
`predicate_projection_by_group` for output zip via the
existing dense path. The presence of that batch disables
sparse projection for `projected_col`, even when the
survival mask is clustered and the projected column is
ADR-064-eligible.

(Note: ADR 058 / 059 native predicates and ADR 063
`Maybe`-page dense COCD evaluation also fall back to dense
*decoding* internally when their kernels need scalar
evaluation, but they live inside the encoding-compute
branch and produce mask-only `PhaseBOutcome` — they never
populate `predicate_projection_by_group`. Those paths
already engage ADR 060 / 064 mask-only sparse projection
today and are unaffected by ADR 065. ADR 065's V1
engagement surface is the scalar dense-decode branch
specifically.)

Dense fallback is not an edge case. ADR 058 / 059
intentionally keep a closed native evaluator catalog;
unsupported cascades, unsupported predicate operations,
residual Prefilter evaluation, variable-width predicate
columns, float comparisons, and future FSST / ALP /
RLE-value-run gaps can all leave the reader with decoded
predicate batches plus an exact row-level survival mask. ADR
065 is the bridge that lets projection-side sparse fetch
keep paying off while predicate-side native-kernel work
proceeds independently.

The dense predicate bytes are already paid before ADR 065
can engage. The win is to avoid fetching dense bytes for the
remaining projection columns
(`requirement.excluding(predicate_fetched_requirement)`).

ADR 065 is intentionally projection-side materialization
composition with an upstream dense-predicate decode result.
It does not change predicate evaluation, mask algebra,
sparse projection eligibility, the COCD evaluator catalog,
or any public protocol surface.

### Three forces

**1. Predicate/output overlap is the OLAP common case.**
Analytical queries routinely project filter columns. Whenever
the predicate falls back to dense and the predicate column
is in the SELECT list, today's planner declines sparse
projection on every other projected column too — the larger
byte axis.

**2. The native COCD catalog is genuinely closed.** ADR
058 / 059 ship a finite predicate evaluator catalog; FSST,
ALP, RLE-value-run, and float comparisons are deferred to
follow-up ADRs. Each produces dense fallback today, and each
costs sparse projection. Closing each native-evaluator gap
is its own multi-iteration ADR; ADR 065 closes the
*projection-side* impact independently, so future native-
kernel work can land its byte-economy gate cleanly without
competing with the sparse-projection-blocked baseline.

**3. The mask is already exact when the predicate decoded.**
A dense predicate decode produces survivors at row precision,
not "Maybe" precision. The sparse projection planner already
consumes exact masks (`Bitmap` / `Runs`); ADR 065 just
removes the artificial gate that says "the predicate must
have *also* skipped decode."

### What this ADR does not propose

- **No new mask variant.** The `EncodedRowMask` algebra is
  unchanged.
- **No lift of sparse projection eligibility.** Projected
  columns still must satisfy ADR 060 + 064 (fixed-width;
  nullable allowed; cascade in the V1 sparse projection set;
  cross-column page-geometry agreement).
- **No native FSST / ALP / RLE-value-run / float
  evaluators.** Each remains a separate ADR; ADR 065 simply
  *survives* their dense fallback.
- **No sparse predicate-column fetch.** Predicate-side fetch
  narrowing is a separate ADR.
- **No page-sliced PBC evaluation.** Predicate evaluation
  continues to consume full row-group dense Arrow.
- **No re-evaluation of predicates on Maybe survivors.** ADR
  065 consumes existing dense decode output; if Maybe
  survivors did not route to dense exact decode, ADR 065
  declines.
- **No variable-width sparse projection.** Inherited
  deferral from ADR 060 / 064. Variable-width on the dense
  predicate side is allowed because it's already decoded;
  the sparse projection side stays fixed-width.
- **No filler-based full-row-group sparse arrays.** Page-
  local filtered zip is the V1 invariant.
- **No retirement of the residual phase-named identifiers
  in `row_group.rs` and `manager.rs` struct types.** That
  is the broader convention sweep flagged by ADR 060; ADR
  065 doesn't bundle it.
- **No new format version, directory shape, IPC
  custom_metadata key, wire MsgType, NodeCapability bit,
  ObjectStore trait method, descriptor field, public PBC
  opcode, or Cargo feature.**
- **No new fallback / reject / catalog variant on any
  public taxonomy.**
- **No new operator config field, metric label, or Cargo
  feature gating engagement.**

### Constraints carry forward unchanged

ADR 003, 016, 019, 020, 022, 029, 030, 035, 036, 037, 039,
040, 042, 043, 044, 052, 053, 054, 055, 056, 057, 058, 059,
060, 061, 062, 063, 064. ADR 057 directory shape unchanged.
ADR 060 / 064 sparse projection module location unchanged
(extended in place). ADR 063 page-stats shape unchanged. ADR
058 / 059 evaluator catalogs unchanged.

## Decision

ADR 065 ships page-aligned dense-predicate-zip with sparse
projection. When predicate evaluation populates
`predicate_projection_by_group` *and* produces an exact
survival mask per row group, the sparse projection planner
engages for the remaining projection requirement and
manager-side execution selects one of three cases.

1. **Mask exactness is the engagement gate.** ADR 065
   engages only when:
   - the survival mask is `EncodedRowMask::Bitmap` or
     `EncodedRowMask::Runs` and exact (no Maybe slices);
   - the remaining projection requirement
     (`requirement.excluding(predicate_fetched_requirement)`)
     satisfies ADR 060 + 064 eligibility;
   - the economic gate (§5) clears.

   `predicate_projection_by_group` may be empty or populated;
   both shapes pass the planner gate (case selection is
   manager-side, §3). `EncodedRowMask::None` short-circuits
   before zip eligibility (zero projection bytes; zero
   predicate-batch slicing; full-schema empty batch via
   `empty_projected_batch_for_requirement`).
   `EncodedRowMask::All` routes to existing dense projection
   (no sparse narrowing applies).

2. **The planner stays metadata-only.** The sparse projection
   planner consumes survival masks, projection metadata, and
   encoding-directory content; it does not consume Arrow
   `RecordBatch`es. Decoded predicate batches stay in the
   manager / row-group layer. ADR 065 removes
   `SparseProjectionFetchInput.dense_predicate_batches_present`
   (the field at `sparse_projection_fetch.rs:91`) and
   retires active use of
   `SparseProjectionFetchDecline::DensePredicateBatchPresent`
   (the variant at `sparse_projection_fetch.rs:65`).

3. **Manager-side execution selects one of three cases.**
   After a sparse plan is selected, the manager dispatches
   on two derived quantities:
   - `predicate_projection_by_group` — the per-row-group
     map of decoded predicate `ProjectedRecordBatch`es
     produced *only* by the scalar dense-decode branch (see
     §"Phase 0 audit findings"; the encoding-compute path
     produces mask-only `PhaseBOutcome` and does not
     populate this map).
   - `predicate_output_overlap` — derived at dispatch time
     as a bool: whether
     `predicate_fetched_requirement.predicate_columns ∩
     requirement.output_columns` is non-empty (some
     predicate columns also appear in the user's SELECT-
     list output).
     This is **not** the same as
     `predicate_fetched_requirement.is_empty()`:
     `predicate_fetched_requirement` is built from
     `requirement.predicate_columns` and storage columns
     (`manager.rs:9853`) and is non-empty whenever any
     predicate column was fetched, regardless of whether
     it appears in the output. ADR 065 introduces
     `predicate_output_overlap` as the Case B/C
     discriminator; `predicate_fetched_requirement` retains
     its current production role (excluding already-
     fetched columns from projection fetch).

   The three cases:

   - **Case A — mask-only sparse projection.**
     `predicate_projection_by_group` is empty. This is the
     existing ADR 060 / 064 path. The manager concatenates
     per-page sparse-projected batches via the existing
     `materialize_sparse_projection_pages` concat wrapper.
     Behavior byte-identical pre / post ADR 065. This case
     also covers all encoding-compute paths (ADR 058 / 059
     native predicates, ADR 063 Maybe-page dense COCD)
     because those paths emit mask-only `PhaseBOutcome`
     and never populate `predicate_projection_by_group` in
     V1.
   - **Case B — predicate/output-overlap zip.**
     `predicate_projection_by_group` is populated AND
     `predicate_output_overlap` is non-empty (some
     predicate columns appear in the output). For each
     selected `(row_group, page)` the manager:
     - obtains the per-page filtered sparse projection
       batch from a new lower helper that splits
       `materialize_sparse_projection_pages` into a per-
       page step plus the existing concat wrapper;
     - slices the dense predicate `ProjectedRecordBatch`
       to the page row span using
       `EncodingPageDescriptor.first_value_index +
       value_count`;
     - applies the *same* page-local survival-mask slice
       to both sides via
       `filtered_predicate_page_batch` (in
       `page_materialize.rs`);
     - assembles a single page-local `ProjectedRecordBatch`
       in `ColumnFetchRequirement` order via
       `zip_predicate_and_projection_columns` (in
       `row_group.rs`, adjacent to the existing
       phase-named zip helper);
     - concatenates page-local batches in
       `(row_group_index, page_index)` order.
   - **Case C — no-overlap dense fallback.**
     `predicate_projection_by_group` is populated AND
     `predicate_output_overlap` is empty (predicate fell
     back to dense for evaluation only; the predicate
     columns fetched by `predicate_fetched_requirement`
     are not in the user-visible output). The manager
     skips the zip step and concatenates per-page sparse-
     projected batches as in Case A. Dense predicate
     decode bytes remain sunk; projection-side bytes still
     narrow.

   The case dispatch is purely manager-side. The planner
   produces the same sparse plan for all three cases when
   the mask and remaining-projection metadata match.

4. **Row order and row count are load-bearing invariants.**
   The dense predicate batch must have the row count
   recorded by the row-group directory entry. Each selected
   page span must fit inside both the dense predicate batch
   and the survival mask. After applying the same local
   mask, the predicate page batch and sparse projection
   page batch must have identical row counts. Any mismatch
   is an invariant error, not a retry-to-dense after
   partial sparse fetch.

5. **Economic gate is unchanged from ADR 060 / 064.**
   Sparse projection bytes include selected value-page
   bytes plus selected validity-page bytes (per ADR 064).
   Dense projection baseline bytes are the existing dense
   projection plaintext baseline for the same *remaining*
   projected columns. **Predicate bytes are identical on
   both sides of the comparison and cancel.** The
   engagement gate is therefore:

   ```text
   sparse_projection_plaintext_bytes < dense_projection_plaintext_bytes
   ```

   The bench artifact additionally records *total query
   plaintext bytes* (predicate decode + projection) on both
   sides as a second axis (§"Bench fixture mandate"); the
   engagement gate stays marginal-on-projection.
   `MAX_SPARSE_RANGES_PER_BUFFER`, `max_simd_pages_per_record`,
   and `max_ranges_per_record` continue to apply
   unchanged.

6. **Mask-first ordering preserved.** `EncodedRowMask::None`
   short-circuits before zip eligibility — zero projection
   fetch, zero predicate-batch slicing, zero zip work. The
   output is a full-schema empty `ProjectedRecordBatch` via
   `empty_projected_batch_for_requirement`.
   `EncodedRowMask::All` routes to dense (no sparse
   narrowing applies). ADR 063 `ExactMatch`-routed row
   groups bypass dense decode entirely; ADR 065 not engaged
   for those row groups.

7. **Sparse zip output bypasses full-row-group mask
   application.** Case B zipped batches and Case C sparse-
   only batches are *already* row-filtered by the page-local
   survival-mask slice. They must NOT be passed through
   `apply_phase_b_masks_to_projected_row_groups` a second
   time — the mask length no longer matches the (filtered)
   batch length, and double-filtering would corrupt output.
   This bypass is an architectural invariant guarded in
   `tests/architecture_test.rs`.

8. **No public surface changes.** ADR 065 adds no new
   `L2FetchStrategy` / `EncodedRowMask` / `EncodingCascadeKind`
   / `RowGroupRejectCode` / `RowGroupFallbackReason` /
   `EncodingRejectCode` / `EncodingFallbackReason` /
   `EncodingComputeRejectCode` /
   `EncodingComputeFallbackReason` / `PredicateAtomOp` /
   `EncodingComputeEvaluatorKind` / `EncodedAggregateKind`
   variant; no new config field; no new metric label; no
   new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability`
   bit; no new descriptor field; no new digest domain; no
   new IPC `custom_metadata` key; no new `ObjectStore`
   trait method; no new public PBC opcode; no Cargo
   feature.

9. **No directory shape changes.** ADR 065 uses existing
   `RecordPageDirectory`, `EncodingDirectory`,
   `BufferEncoding.page_value_count` /
   `page_end_offsets`, ADR 063 `BufferEncoding.page_stats`,
   and ADR 064 raw validity rebasing. No new validity /
   predicate-batch / row-span / zip directories.

10. **No new activation surface.** Engagement remains
    controlled by the existing row-group / encoding fetch
    toggles plus private mask / projection-eligibility /
    economics gates. No new flag, Cargo feature, metric
    label, or operator knob.

11. **Cross-replica determinism.** Page-local zip is a pure
    function of (selected page set, exact survival mask,
    decoded predicate batches in
    `predicate_projection_by_group`, ADR 057 directory, ADR
    064 nullable rebase, `ColumnFetchRequirement`
    ordering). No runtime branches that should vary across
    replicas. Output byte-equality across replicas holds.

The slab-format byte layout, AAD shape, nonce derivation,
key hierarchy, footer HMAC scope, ADR 022 footer layout,
ADR 056 directory shape, ADR 057 encoding directory shape,
ADR 058 / 059 evaluator catalogs, ADR 060 sparse projection
planner module location, ADR 061 closed chunk-size set,
ADR 062 sizing profile catalog, ADR 063 page-stats shape,
and ADR 064 nullable validity helpers are all unchanged.

## Architectural inextricability

Five design choices are physically inseparable in V1.

**1. The mask must be exact before zip engages.** Maybe-
mask slices have unknown survivors and cannot be sliced
into selected projection pages without re-evaluating the
predicate. ADR 065 does not re-evaluate predicates; it
consumes existing dense decode output. If predicate
evaluation didn't produce an exact mask, ADR 065 declines.
ADR 063 `ExactMatch` slices and dense-decode survivors
both qualify as exact; Maybe slices that route through an
upstream Maybe-only path do not.

**2. Predicate and projection must be filtered with the
same page-local mask.** Filtering only the sparse
projection side and zipping with an unfiltered predicate
batch corrupts row alignment. Filtering both sides with
independently derived masks risks drift. ADR 065 derives
one local mask per selected page from the row-group
survival mask and applies it to both page batches.

**3. Dense predicate fetch is not sparse predicate fetch.**
The dense predicate batch exists because the existing
row-group predicate path needed dense Arrow arrays.
Reusing it for output zip is safe. Narrowing the predicate
fetch itself has a separate correctness and economics
story and remains outside this ADR.

**4. Sparse zip output must not receive a second full-row-
group mask.** ADR 064 sparse projection materialization
already filters selected pages by the survival mask. ADR
065's Case B zipped result and Case C sparse-only result
are likewise already filtered. Sending either through
`apply_phase_b_masks_to_projected_row_groups` would
double-filter against a mask whose length no longer
matches the filtered output.

**5. Page-local assembly is the invariant, not filler
reconstruction.** Building full-row-group sparse
projection arrays with null/default fillers for unselected
pages would create a second materialization style and make
placeholder semantics observable in failure modes. ADR 065
keeps selected pages as the unit of proof and filters both
sides at that grain.

## Identity-vs-Locality framing

ADR 065 is identity-neutral and locality-neutral. It
changes how a selected projected page is composed with
already-decoded predicate columns after row identity,
record location, and locality decisions are made.
Predicate decode and projection decode share
`BufferEncoding` shape under ADR 057 / 064; ADR 065 only
adds a row-aligned page-local assembly step. There is no
separate "zip plane" — Unified Ring rule applies.

Cross-replica determinism is preserved because page-local
zip assembly is a pure function of (selected page set,
exact survival mask, dense decoded predicate batches, ADR
057 directory, ADR 064 nullable rebase, output schema
ordering). Arrow-output byte equality across replicas
holds.

## V1 scope

### 0. Phase 0 audit findings

The Phase 0 audit confirms the engagement surface is non-
trivial against current writer output. Key findings:

- The decline at
  `src/storage/encoding/sparse_projection_fetch.rs:120-122`
  fires whenever `dense_predicate_batches_present` is true,
  which the manager sets at
  `src/storage/manager.rs:10043` from
  `predicate_outcome` shape. ADR 065 lifts this gate.
- **`predicate_projection_by_group` is populated by exactly
  one branch in production**: the scalar dense-decode path
  with its single insertion site at
  `src/storage/manager.rs:9931`. The encoding-compute paths
  (which handle ADR 058 / 059 native predicates *and* ADR
  063 Maybe-page dense COCD) build
  `PhaseBOutcome::default()` at `manager.rs:8998` and
  `manager.rs:9219`, insert only `survival_masks`, and
  return the outcome at `manager.rs:9806` without ever
  populating `predicate_projection_by_group`. Those paths
  therefore engage **Case A (mask-only sparse projection)**,
  not Case B/C — the manager's `predicate_projection_by_group`
  is empty for them. The V1 dense-predicate-decode catalog
  for Case B/C engagement is therefore narrower than earlier
  iterations claimed: only the scalar dense-decode branch
  (predicates that the encoding-compute path doesn't handle
  at all). Survival masks from the scalar branch are
  produced at row precision via decode-then-scan; exact-mask
  invariant holds for that branch.
- `predicate_fetched_requirement` is the
  `ColumnFetchRequirement` subset of predicate columns
  fetched dense, built at `manager.rs:9853` from
  `requirement.predicate_columns` and storage columns. It
  is non-empty whenever predicate fetch happened, regardless
  of whether the predicate column appears in the output —
  so it is **not** the Case B/C discriminator.
- `predicate_output_overlap` (introduced by ADR 065 as a
  derived dispatch quantity) is computed at dispatch time
  as a bool: whether
  `predicate_fetched_requirement.predicate_columns ∩
  requirement.output_columns` is non-empty. Case B engages
  when the bool is true (predicate columns are also in the
  user's SELECT-list output); Case C engages when it is
  false (predicate fetched dense for evaluation only;
  predicate columns not in output).
  `predicate_fetched_requirement` retains its current
  production role for the projection-fetch exclusion at
  `manager.rs:10017`
  (`requirement.excluding(&predicate_fetched_requirement)`)
  — a non-empty remaining projection requirement is the
  precondition for ADR 065 engagement.
- `materialize_sparse_projection_pages`
  (`manager.rs:12249`) currently produces a single
  concatenated batch via the per-page filtering inside.
  ADR 065 splits it into a lower per-page helper plus the
  existing concat wrapper for Case A / Case C reuse.
- `EncodingPageDescriptor.first_value_index` and
  `value_count` (`page.rs:24-25`) provide page-grain row-
  span addressing; Arrow array slicing at those positions
  is O(1).
- ADR 064's `rebase_validity_bitmap`
  (`page_materialize.rs:231`) is reusable for nullable
  sparse-projected columns in the zip; ADR 065 introduces
  no new validity helpers.

### 1. Predicate-batch row-span slicing

`page_materialize.rs` gains a page-grain helper:

```rust
pub(crate) fn filtered_predicate_page_batch(
    predicate_batch: &ProjectedRecordBatch,
    page: EncodingPageDescriptor,
    local_mask: &BooleanArray,
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

The helper handles variable-width predicate Arrow columns
unchanged (Arrow array slicing is type-uniform). The dense
predicate side is not constrained by ADR 064's fixed-width
catalog.

Helper placement in `page_materialize.rs` keeps it adjacent
to ADR 064's nullable rebase helpers (single-source guard
with ADR 052 secondary-index path).

### 2. Page-local zip assembly

`row_group.rs` gains a semantic zip helper adjacent to the
existing dense-zip helper:

```rust
pub(crate) fn zip_predicate_and_projection_columns(
    predicate: &ProjectedRecordBatch,
    projection: &ProjectedRecordBatch,
    requirement: &ColumnFetchRequirement,
) -> std::io::Result<ProjectedRecordBatch>;
```

Builds a `ProjectedRecordBatch` whose columns follow
`requirement` order, drawing each column from either
`predicate` or `projection` based on which source contains
it. Does not allocate new column data — reorders existing
`ArrayRef`s into the requirement's column order. Validates
row-count agreement (`predicate.num_rows() ==
projection.num_rows()`); mismatch is fail-closed.

The existing helper
`zip_phase_b_and_phase_c_projected_row_group`
(`row_group.rs:1236`) performs the dense-path zip today.
ADR 065 introduces `zip_predicate_and_projection_columns`
as the semantic replacement; during this ADR it may
delegate to the existing phase-named helper internally.
The broader convention sweep retiring the phase-named
identifier is out of scope for ADR 065.

### 3. Sparse projection page-batch helper

Split `materialize_sparse_projection_pages`
(`manager.rs:12249`) into:

- a lower helper returning per-page tagged filtered
  batches:

  ```rust
  struct SparseProjectionPageBatch {
      row_group_index: RowGroupIndex,
      page: EncodingPageDescriptor,
      local_mask: BooleanArray,
      projected: ProjectedRecordBatch,
  }
  ```

  The `projected` field is already filtered by `local_mask`;
- the existing concat wrapper used by Case A and Case C
  paths.

ADR 065 Case B zip path uses the lower helper directly,
slices the dense predicate batch per
`SparseProjectionPageBatch`, calls
`filtered_predicate_page_batch` with the same `local_mask`,
zips, and concatenates per `(row_group_index,
page_index)`. The concat wrapper preserves byte-identical
output for Case A regression; Case C reuses the same
wrapper.

### 4. Planner gate lift

`sparse_projection_fetch.rs` lifts the
`dense_predicate_batches_present` decline. The planner now
inspects only exact survival masks and projection metadata:

- mask exact + remaining projection requirement non-empty
  + remaining projection eligible → engage sparse
  projection (planner does not consult
  `predicate_projection_by_group`; the manager-side
  execution selects the case);
- mask exact + remaining projection empty (predicate-only
  query) → no sparse projection planning needed (existing
  dense path handles trivially);
- predicate-batch page-span and row-count invariant
  failures are manager-side fail-closed runtime errors
  after sparse fetch begins. V1 does not add planner-level
  private decline variants for these checks.

The `dense_predicate_batches_present` field is removed
from `SparseProjectionFetchInput` (decisive removal, not
rename). The `DensePredicateBatchPresent` variant of
`SparseProjectionFetchDecline` is removed rather than
replaced with unused private variants.

Decline reasons remain private; no public taxonomy
change.

### 5. Economic gate

Unchanged from ADR 060 / 064:
`sparse_projection_plaintext_bytes <
dense_projection_plaintext_bytes`. Predicate bytes appear
on both sides identically and cancel.

### 6. Mask-first ordering and bypass

Preserved unchanged:

- `EncodedRowMask::None` → zero projection fetch, zero
  predicate-batch slicing, full-schema empty
  `ProjectedRecordBatch` via
  `empty_projected_batch_for_requirement`.
- `EncodedRowMask::All` → dense projection (no sparse
  narrowing applies).
- ADR 063 `ExactMatch` → already produces a non-decoded
  path; ADR 065 not engaged for those row groups.
- `Bitmap` / `Runs` exact + decoded predicate batches
  present + remaining projection eligible → zip engages
  (Case B) or sparse-only concat (Case C).

ADR 065 zipped output and Case C sparse-only output
bypass `apply_phase_b_masks_to_projected_row_groups` (the
page-local filter has already been applied; double
filtering would corrupt the result).

## Composability

**ADR 058 / 059 dense fallback.** ADR 065 closes the
projection-side impact of every native-kernel-uncovered
predicate path. As ADR 058 / 059 widen the native catalog
(FSST, ALP, RLE-value-run, float comparisons), each row
group migrates from ADR 065 zip to ADR 060 / 064 mask-only
sparse projection without any planner reshape. ADR 065 is
the bridge layer that lets sparse projection ship without
waiting for native kernel coverage.

**ADR 060 sparse projection.** ADR 065 amends ADR 060's
`decoded_predicate_batches.is_empty()` precondition. All
other ADR 060 invariants hold (mask-to-page selection,
`None` / `All` routing, economics, internal-decline
discipline, no-fifth-strategy rule). Page-tagged sparse
projection batches are shared infrastructure — ADR 060 /
064 mask-only path uses the concat wrapper; ADR 065 zip
path uses the lower per-page helper.

**ADR 063 page-level predicate stats.** ADR 063's `Disjoint`
slices yield `EncodedRowMask::None`; ADR 063's `ExactMatch`
slices yield `EncodedRowMask::All`-style outcomes; ADR 063's
`Maybe`-page dense COCD lives inside the encoding-compute
path and emits a mask-only `PhaseBOutcome` (per Phase 0
audit: `manager.rs:8998` / `9219` insert only
`survival_masks`; `predicate_projection_by_group` stays
empty). All three ADR 063 routes therefore engage **Case A
mask-only sparse projection** in V1 — they do not drive
ADR 065 Case B/C zip. ADR 063 + ADR 065 do not compose
multiplicatively in V1; the byte savings stack only insofar
as ADR 063 narrows predicate-side bytes and Case A still
narrows projection-side bytes. Materializing predicate
projections from the encoding-compute Maybe path so that
ADR 065 Case B can engage on those queries is V2 work
(would require the encoding-compute branch to populate
`predicate_projection_by_group` alongside its survival
mask). ADR 065 does not consume page stats for zip
correctness.

**ADR 064 nullable sparse projection.** ADR 065 reuses ADR
064's `rebase_validity_bitmap` for nullable sparse-projected
columns. Nullable predicate columns use the dense decoded
validity buffer directly (no sparse rebase). The dense
predicate side may carry variable-width or nullable-
variable-width Arrow because it's already decoded; the
sparse projection side stays ADR 064 fixed-width nullable.

**ADR 052 secondary-index point reads.** Unchanged. Point
reads do not engage Phase B dense predicate decode; ADR
065 has no point-read surface. If ADR 052 ever needs page-
grain dense + sparse composition, it imports the same
helpers (single-source guard).

**ADR 061 chunk-aligned reads.** Predicate fetch ranges and
sparse projection ranges flow through the same range
planner. ADR 065 adds no new ranges; predicate ranges are
already in the dense plan, projection ranges are in the
sparse plan, and they coalesce per ADR 044 / 061.

**ADR 062 row-group sizing profile.** ADR 065 should show
its strongest absolute byte savings under `Scan` profile on
wide tables with mixed-evaluator predicates and
predicate/output overlap. `PointRead` is correctness-
covered; small-row-group economics may decline more often
than `Scan` if predicate decode dominates the projection
work.

## Architectural lockdown

Incremental guards in `tests/architecture_test.rs`:

- `dense_predicate_batches_present` /
  `DensePredicateBatchPresent` no longer the gate for
  sparse projection engagement.
- ADR 060 / 064 mask-only sparse projection still engages
  when `predicate_projection_by_group` is empty.
- Sparse projection planner module location unchanged
  (`src/storage/encoding/sparse_projection_fetch.rs`).
- `filtered_predicate_page_batch` lives in
  `src/storage/encoding/page_materialize.rs`; ADR 052
  secondary-index path imports it if needed (single-source
  guard).
- `zip_predicate_and_projection_columns` lives in
  `src/storage/row_group.rs`.
- `SparseProjectionFetchDecline` remains private; ADR 065
  adds no public fallback / reject taxonomy and no unused
  private decline variants.
- Sparse projection planner consumes only survival masks
  and projection metadata; no decoded Arrow batches reach
  the planner.
- Manager-side case dispatch is explicit on
  `predicate_projection_by_group` and
  `predicate_fetched_requirement` shape.
- Case A / B / C output bypasses
  `apply_phase_b_masks_to_projected_row_groups` for
  ADR 065 paths.
- Row-group-grain assembly absent; page-grain only.
- Filler-based full-row-group reconstruction absent.
- No new `L2FetchStrategy` / `EncodedRowMask` / catalog
  variant on any reject / fallback / cascade / evaluator /
  aggregate / atom kind.
- No new IPC `custom_metadata` key.
- No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.
- No new wire `MsgType` / `FRAME_FLAG_*` /
  `NodeCapability` bit / digest domain / `ObjectStore`
  trait method.
- No new descriptor field / fingerprint surface change.
- No new public PBC opcode.
- No new operator config field.
- No new metric label.
- No Cargo feature.
- No nightly rustc dependency.
- No version suffix or planning-phase identifier on new
  symbols. (`zip_predicate_and_projection_columns`,
  `filtered_predicate_page_batch`,
  `SparseProjectionPageBatch` are semantic.)
- No project-name prefix.
- ADR 020 §2 AAD layout, ADR 022 §3 / §5 byte layouts,
  ADR 056 / 057 / 063 directory shapes, ADR 058 / 059
  evaluator catalogs, ADR 060 / 064 sparse projection
  eligibility, ADR 061 closed chunk-size set, ADR 062
  `RowGroupSizingProfile` catalog: all unchanged.
- Cross-replica byte-equal output across two writers with
  the same config + dense-predicate-zip fixture.
- Predicate evaluation paths untouched (no PBC re-
  evaluation after Phase B).
- ADR 060 / 064 mask-only sparse projection byte-identical
  pre / post on shared fixtures.
- The residual phase-named identifiers
  (`apply_phase_b_masks_to_projected_row_groups`,
  `zip_phase_b_and_phase_c_projected_row_group`,
  `PhaseBOutcome`, `phase_c_requirement` parameter in
  `row_group.rs`) are not renamed by ADR 065 — that is
  the broader convention sweep flagged by ADR 060 §"no-
  planning-phase-names convention applied".

## Bench fixture mandate

Bench artifact records (matching ADR 060 / 063 / 064
cadence):

- predicate/output-overlap fixtures (predicate column
  projected) at `Scan` profile across selectivities
  (clustered 1% / 5% / 25% / 50%);
- ADR 058 / 059 dense-fallback trigger fixtures per
  catalog member: Float comparison, FSST predicate (if
  constructible), ALP predicate (if constructible), RLE-
  value-run predicate, mixed-cascade Dictionary
  predicate;
- **synthetic always-dense-fallback fixture** (regression-
  stable across native-evaluator widening) at clustered
  5%;
- ADR 063 `Maybe`-page dense fallback fixture (predicate
  evaluator non-native AND `Maybe` page);
- predicate column shape sweep:
  - fixed-width predicate column projected;
  - nullable fixed-width predicate column projected;
  - variable-width predicate column projected (allowed on
    dense side);
- remaining projection shape sweep:
  - one fixed-width non-null sparse column;
  - one nullable fixed-width sparse column;
  - four sparse projection columns;
  - sixteen sparse projection columns (wide-table
    diagnostic);
  - mixed nullable + non-null;
  - variable-width remaining projection (declines);
- predicate column count: 1 / 4 / 8;
- mask shape:
  - clustered 1% / 5% / 25% (correctness + economic
    gates);
  - clustered 50% diagnostic (where dense-zip win narrows
    or inverts);
  - random 1% / 5%;
  - `None`;
  - `All`;
- ADR 063 interaction (control rows, not zip rows):
  ExactMatch-driven and Maybe-driven dense COCD both
  produce mask-only `PhaseBOutcome` and engage Case A
  mask-only sparse projection in V1 (the encoding-compute
  branch never populates `predicate_projection_by_group`).
  Recorded as Case A regression rows confirming they do
  not engage Case B/C; multiplicative ADR 063 + ADR 065
  zip composition is V2 work (requires encoding-compute
  path to materialize predicate projections);
- ADR 064 composability: nullable predicate column +
  nullable projection column zip;
- ADR 060 / 064 mask-only path byte-identical pre / post
  (regression);
- predicate batch row-span slicing correctness on byte-
  aligned and non-byte-aligned page boundaries (1,025-
  rows-per-page fixture);
- Case C no-overlap dense fallback control;
- composability with ADR 061 (p50 / p95 GET payload
  unchanged on dense-zip fixtures);
- composability with ADR 062 (`Scan` profile produces
  strictly larger absolute byte savings than `PointRead`
  on matched-shape fixtures);
- cross-replica determinism on representative dense-zip
  fixtures.

**Promotion gate (three axes)**: clustered 5%
representative dense-zip rows keep:

1. at least **1.5×** *remaining-projection* plaintext-byte
   reduction (value + validity combined) on representative
   wide-table fixtures (BitPacked + Dictionary cascades);
2. at least **1.25×** *total query* plaintext-byte
   reduction (predicate decode bytes + projection bytes)
   on the same fixtures;
3. at least **1.2× wall-clock** improvement vs. full-
   dense projection on representative runtime fixtures
   (or council records a revised gate).

The bench artifact also reports whether **≥ 3×**
remaining-projection plaintext-byte reduction holds on
best-case clustered wide-table fixtures (headline
diagnostic, not gating). Random / weak rows ≤ 5% full-
dense overhead.

## V1 deferred (with explicit V2 promotion paths)

ADR 065 does not ship:

- **Sparse predicate-column fetch.** Predicate-side fetch
  narrowing for non-COCD predicates. **V2:** dedicated
  ADR.
- **Page-sliced PBC evaluation.** Predicate evaluation
  continues to consume full row-group dense Arrow.
- **Variable-width sparse projection.** Inherited ADR 060
  / 064 deferral. **V2:** dedicated ADR.
- **Native FSST / ALP / RLE-value-run / float
  evaluators.** Each is its own ADR; ADR 065 is the
  bridge that makes their absence non-blocking for
  projection-side byte economy.
- **Mixed sparse / dense projected columns within one
  materialization pass.** Inherited ADR 060 deferral.
- **Re-evaluating predicates after a Maybe slice
  survives.** Predicate re-evaluation is upstream. ADR
  065 V1 engages after exact masks are available and adds
  no Maybe-specific sparse-projection decline variant.
- **Row-group-grain assembly.** Explicit architectural
  rejection; page-grain only.
- **Filler-based full-row-group sparse reconstruction.**
  Explicit architectural rejection.
- **Folding predicate bytes into the projection economic
  gate.** Predicate bytes are sunk cost; folding them re-
  declines target rows.
- **Allocation pooling or batched slicing for the per-
  page Arrow slicing churn.** Bounded by
  `selected_pages × predicate_columns`; the wall-clock
  axis catches regressions. **V2:** follow-up
  optimization ADR if operational evidence warrants.
- **Cargo feature or operator-tunable engagement.**
  Implicit activation matches ADR 060 / 064.
- **Provider-native multi-range API.** Separate
  refinement.
- **Retiring the residual phase-named identifiers**
  (`apply_phase_b_masks_to_projected_row_groups`,
  `zip_phase_b_and_phase_c_projected_row_group`,
  `PhaseBOutcome`, `phase_c_requirement` parameter).
  Broader convention sweep, not bundled with ADR 065.

## Rejected alternatives

- **Keep dense predicate batches as a hard sparse
  decline.** Leaves a common predicate/output overlap
  query shape on the dense projection path even after ADR
  064 fixed nullable projection.
- **Wait for native FSST / ALP / RLE-value-run / float
  evaluators.** Each is its own multi-iteration ADR. The
  projection-side impact of dense fallback is independent
  of native-kernel coverage and can ship now.
- **Sparse predicate-column fetch.** That is a separate
  ADR with its own predicate-evaluation and PBC
  correctness proof. ADR 065 is the projection-side
  bridge only.
- **Carry decoded predicate `RecordBatch`es inside
  `SparseProjectionFetchPlan`.** Blurs the planner-vs-
  manager boundary by mixing active selection state with
  materialized Arrow state. ADR 065 holds the boundary:
  planner consumes survival masks + projection metadata
  only; manager-side execution consults
  `predicate_projection_by_group` after a sparse plan has
  been selected.
- **Build full-row-group sparse projection arrays with
  null/default fillers, then apply the existing full-row-
  group mask.** Keeps the old mask application shape but
  breaks the ADR 060 / 064 page-local materialization
  invariant and creates filler semantics for bytes that
  were intentionally not fetched.
- **Filter the full dense predicate batch once, then zip
  with a concatenated sparse projection batch.** Correct
  when carefully implemented, but page-local zipping
  gives a tighter row-alignment oracle and catches page-
  span mistakes earlier. Implementations may internally
  batch work; the proof obligation is page-local.
- **Row-group-grain assembly.** Forces full projection
  decode (defeating the purpose) or a dual-grain
  materialization style (breaks ADR 060 / 064 page-local
  invariant).
- **Re-evaluate predicate on Maybe survivors.** Couples
  ADR 065 to predicate evaluator implementation; double
  cost; out of scope for a projection-side optimization.
- **Re-run PBC after sparse projection.** Violates ADR
  056 / 060 discipline: predicate evaluation happens
  before final projection assembly; post-fetch zip must
  not re-evaluate PBC.
- **Fold predicate bytes into the projection economic
  gate.** Predicate bytes are identical on both sides of
  the comparison; folding them re-declines the rows ADR
  065 is meant to capture. The bench artifact reports
  total query bytes as a separate axis.
- **A new `L2FetchStrategy::DensePredicateSparseProjectionZip`
  variant.** ADR 060's no-fifth-strategy discipline.
- **A new `EncodedRowMask` variant for "exact-with-
  decoded-predicate".** Existing `Bitmap` / `Runs` exact
  masks suffice; the predicate-decoded fact lives in
  `predicate_projection_by_group`.
- **Add row-id side vectors to sparse materialization.**
  The page descriptor plus row-group survival mask
  already define row identity and order. A row-id side
  vector duplicates state and adds another alignment
  contract.
- **Allow sparse zip for `All` masks.** Fetching every
  projection page and zipping it sparsely buys no page-
  narrowing win. The existing `All` route stays dense.
- **Bundle sparse predicate-column fetch.** Two
  correctness stories per ADR violates closed-catalog
  discipline.
- **Operator-visible dense-zip engagement flag.** Implicit
  activation matches ADR 060 / 064.
- **Promote-as-public-fallback-variant.** Decline reasons
  stay private.
- **Forking the materializer.** Single-source discipline.
- **Bundle the broader convention sweep (retiring
  `PhaseBOutcome`, `apply_phase_b_masks_to_projected_row_groups`,
  `zip_phase_b_and_phase_c_projected_row_group`,
  `phase_c_requirement` parameter).** ADR 060 already
  flagged these for a separate sweep; bundling them with
  ADR 065 expands scope and risks correctness drift.

## What not to do

- **Don't lift sparse projection eligibility** to
  variable-width or out-of-catalog cascades.
- **Don't extend predicate evaluation.** ADR 065 only
  changes projection-side composition.
- **Don't add a fifth `L2FetchStrategy` variant** or any
  new `EncodedRowMask` / fallback / reject / catalog /
  config / metric / wire / capability / digest /
  `ObjectStore` surface.
- **Don't introduce planning-phase identifiers, version
  suffixes, or project-name prefixes** on new symbols.
- **Don't bump `ENCODING_DIRECTORY_FORMAT_VERSION`.**
- **Don't change ADR 057 cascade catalog or ADR 063 page-
  stats eligibility.**
- **Don't re-evaluate predicates on Maybe survivors.**
- **Don't re-run PBC after Phase B.**
- **Don't fold predicate bytes into the projection
  economic gate.**
- **Don't introduce row-group-grain assembly.**
- **Don't introduce filler-based full-row-group sparse
  reconstruction.**
- **Don't apply
  `apply_phase_b_masks_to_projected_row_groups` to ADR
  065 zipped output or Case C sparse-only output.**
- **Don't bundle ADR 065 with FSST / ALP / RLE-value-run
  evaluators / sparse predicate-column fetch / variable-
  width sparse projection / page-sliced PBC.**
- **Don't engage Phase B dense decode on ADR 052 point-
  read paths**; those paths do not produce dense
  predicate batches.
- **Don't leak planner decline reasons to public metrics,
  fallback enums, or reject codes.**
- **Don't broaden ADR 064's projection type or cascade
  catalog.**
- **Don't bundle the residual phase-named identifier
  retirement** with ADR 065.

## Convergence summary

```text
ADR 056 row-group fetch + per-row-group stats + page directory
  → ADR 057 per-buffer pages + cascades
  → ADR 058 substrate COCD
  → ADR 059 native COCD kernels
  → ADR 060 sparse projection fetch (mask-only Phase B
    contract, fixed-width + non-null only)
  → ADR 061 page-aligned AEAD chunks + macro-block
    coalescing
  → ADR 062 row-group sizing profile (PointRead / Scan)
  → ADR 063 page-level predicate stats + Phase B narrowing
  → ADR 064 nullable sparse projection
  → ADR 065 dense predicate-zip with sparse projection (this
    ADR; lifts ADR 060 / 063 / 064's mask-only Phase B
    contract; page-local zip of dense-decoded predicate
    columns in `predicate_projection_by_group` with
    sparse-projected remaining projection columns; three-
    case manager dispatch; predicate bytes sunk;
    projection economic gate unchanged; total-query gate
    recorded as second axis; page-grain row alignment
    only; no filler reconstruction; sparse zip output
    bypasses
    `apply_phase_b_masks_to_projected_row_groups`; ADR
    058 / 059 evaluator catalog unchanged; no public
    protocol surface; no directory shape change)
    ← here
  → next Toron-adjacent follow-ups (FSST evaluator, ALP
    evaluator, RLE value-run, sparse predicate-column
    fetch, variable-width sparse projection, encoded-
    validity page decoding, all-null page-skip
    optimization)
  → capacity-tier compression / serverless / EC bundle
```

## Promotion criteria

ADR 065 promotes from council-converged → shipped when:

1. Phase 0 audit pinned the dense-predicate-decode catalog
   and confirmed every member produces an exact mask.
2. Sparse projection planner accepts row groups with non-
   empty `predicate_projection_by_group` when the survival
   mask is exact and the remaining projection requirement
   is eligible.
3. `dense_predicate_batches_present` field removed from
   `SparseProjectionFetchInput`;
   `DensePredicateBatchPresent` no longer used as an
   active decline.
4. `None` masks short-circuit before zip-eligibility,
   fetch zero projection bytes, do zero predicate-batch
   slicing, and return a full-schema empty
   `ProjectedRecordBatch` via
   `empty_projected_batch_for_requirement`.
5. `All` masks route to dense.
6. ADR 065 adds no unused private sparse-projection
   decline variants; non-exact predicate masks remain an
   upstream encoding-compute concern in V1.
7. Page-grain row-span slicing is correct for byte-aligned
   and non-byte-aligned page boundaries (including 1,025-
   rows-per-page fixture).
8. Sparse zip output is byte-identical to full-dense
   projection across the dense-predicate-decode catalog ×
   representative selectivities, including:
   - fixed-width predicate column projected;
   - nullable fixed-width predicate column projected;
   - variable-width predicate column projected;
   - storage-column projection.
9. Case A (mask-only) preserves ADR 060 / 064 behavior
   byte-identically.
10. Case C (no-overlap dense-fallback;
    `predicate_projection_by_group` populated but
    `predicate_output_overlap` empty — i.e., no fetched
    predicate columns appear in `requirement.output_columns`)
    reaches the planner gate cleanly, manager-side
    execution skips the zip step, and sparse projection
    narrows projection-side bytes while dense predicate
    decode bytes remain sunk.
11. ADR 064 nullable sparse projection composes (nullable
    predicate column + nullable projected column); shared
    helper retained.
12. Predicate-batch row-span and row-count invariant
    fixtures fail closed through manager-side storage /
    Arrow errors when constructible.
13. ADR 063 ExactMatch and Maybe-driven dense COCD
    fixtures both produce mask-only `PhaseBOutcome` and
    engage Case A (regression confirmation: the encoding-
    compute path does not populate
    `predicate_projection_by_group` in V1; ADR 063 + ADR
    065 zip composition is V2 work).
14. ADR 065 zipped output and Case C sparse-only output
    never pass through
    `apply_phase_b_masks_to_projected_row_groups`
    (architectural guard).
15. Predicate-first, projection-first, and interleaved
    output schemas all follow `ColumnFetchRequirement`
    order. The zip helper reorders `ArrayRef`s by the
    requirement's column order regardless of which source
    (predicate or projection) provided each column.
16. ADR 061 chunk-size composition envelopes preserved
    (p50 / p95 GET payload unchanged on dense-zip
    fixtures); ADR 062 `Scan` profile produces strictly
    larger absolute byte savings than `PointRead` on
    matched-shape fixtures.
17. Architectural lockdown guards land per §"Architectural
    lockdown".
18. Bench artifact at
    `plans/impl/065_dense_predicate_zip_with_sparse_projection_bench_results.md`
    records: dense predicate plaintext bytes; dense
    remaining-projection plaintext bytes; sparse selected
    value plaintext bytes; sparse selected validity
    plaintext bytes; total dense query plaintext bytes;
    total sparse query plaintext bytes; ciphertext bytes
    (full-dense / dense-zip); selected ranges; selected
    pages; output row count; output equality digest; null-
    count equality; full-dense wall-clock; dense-zip
    wall-clock; range GET calls / bytes. Promotion gates:
    clustered 5% ≥ 1.5× remaining-projection plaintext-
    byte reduction; ≥ 1.25× total query plaintext-byte
    reduction; ≥ 1.2× wall-clock (or council-revised gate
    recorded); random / weak rows ≤ 5% full-dense
    overhead. Headline diagnostic: ≥ 3× remaining-
    projection plaintext-byte reduction on best-case
    clustered wide-table fixtures.
19. ADR 060 §"Where we are" amended: dense-predicate-zip
    is in V1 sparse-projection eligibility set; engagement
    catalog narrowed to scalar dense-decode branch only.
20. ADR 063 §"Where we are" amended: ADR 063 routes
    (Disjoint / ExactMatch / Maybe-page dense COCD) all
    engage Case A mask-only sparse projection in V1
    because the encoding-compute path does not populate
    `predicate_projection_by_group`; multiplicative ADR
    063 + ADR 065 zip composition is V2 work.
21. ADR 064 §"Where we are" amended: nullable + dense-
    predicate-zip composition recorded for Case B
    (scalar dense-decode path).
22. ADR 020 / 022 / 044 / 052 / 055 / 056 / 057 / 058 /
    059 / 060 / 061 / 062 / 063 / 064 focused tests stay
    green at both profiles.
