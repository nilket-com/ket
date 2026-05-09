# ADR 060: Sparse Projection Fetch Narrowing

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/060_sparse_projection_fetch_narrowing.md`

## Status

Council-converged, implemented, and promoted to
shipped. Full Phase 7 bench evidence at
`plans/impl/060_sparse_projection_fetch_narrowing_bench_results.md`
clears the shipped-status gate for the council-
chosen default scope: `BitPacked` and `Dictionary`
sparse projection fetch are default-engaged when
the plan-time economics choose them; RLE remains
default-off after Phase 7 evidence and is recorded
as follow-up ADR territory. The recorded-not-gated
ciphertext-ratio follow-up is closed by ADR 061's shipped
per-slab chunk-size selection and bench evidence at
`plans/impl/061_page_aligned_authenticated_chunk_layout_bench_results.md`.

## Where we are

ADR 063 page-level predicate stats is now implemented and
promoted as an upstream exact-mask source for ADR 060. Before
dense predicate fetch, the row-group path can classify
predicate-column pages as `Disjoint`, `ExactMatch`, or
`Maybe`; the engaged narrowed path returns a mask-only
`PhaseBOutcome` and leaves `decoded_predicate_batches` empty.
That exact mask feeds ADR 060's sparse projection planner
unchanged. ADR 063 still emits mask-only `PhaseBOutcome` in
V1; it narrows predicate bytes before mask construction and
does not populate decoded predicate projection batches.

ADR 064 (Nullable Sparse Projection) is now implemented and
promoted, lifting ADR 060's
`SparseProjectionFetchDecline::NullableProjectedColumn`
decline for fixed-width projection columns. The planner
fetches and re-bases raw Arrow validity bitmap slices
alongside selected value pages; the page materializer
assembles nullable Arrow arrays without using dense row-group
buffers. V1 is raw byte-addressable validity only — validity
buffers with `EncodingDirectory` entries decline to dense
behind a private planner reason. Row groups with
`column_null_counts == 0` for nullable Arrow fields stay on
the existing all-valid sparse path with zero validity-byte
fetches. Mask-first ordering preserved (`None` /
`All` ordering unchanged). The
bench artifact at
`plans/impl/064_nullable_sparse_projection_bench_results.md`
records 5.36x-19.98x plaintext-byte reduction and
1.50x-5.45x wall-clock speedup at clustered 5% across
BitPacked and Dictionary fixtures, with explicit
ExactMatch-driven and Maybe-driven projection page rows
documenting the multiplicative composition with ADR 063.

ADR 065 (Dense Predicate-Zip with Sparse Projection) is now
implemented and promoted. It lifts ADR 060's former dense-
predicate-batch cliff for the scalar dense-decode branch: when
`predicate_projection_by_group` is populated and the survival
mask is exact, the sparse projection planner still engages for
the remaining projection requirement. Case B zips page-local
dense predicate batches with sparse-projected pages when
predicate/output overlap exists; Case C skips zip and keeps
projection-side sparse fetch when the dense predicate columns
are not user-visible output. Predicate bytes are sunk; ADR 060
projection economics remain
`sparse_projection_plaintext_bytes <
dense_projection_plaintext_bytes`. Encoding-compute paths
from ADR 058 / 059 / 063 remain Case A mask-only sparse
projection in V1. Bench evidence is recorded at
`plans/impl/065_dense_predicate_zip_with_sparse_projection_bench_results.md`.

Encoded-validity page decoding, variable-width sparse
projection, all-null page-skip, sparse predicate-column fetch,
and native FSST / ALP / RLE-value-run / float evaluators
remain V2.

## Context

The Toron-pivot stretch shipped a four-layer storage
hierarchy:

- **ADR 056** — outer row-group fetch + page
  directory; three-stage planner: directory probe
  (metadata + directory read), predicate evaluation
  (stats prune / predicate-column fetch / per-group
  survival mask), projection fetch (projected fetch
  + zip + dense filter — PBC never re-evaluated
  post-predicate-evaluation).
- **ADR 057** — inner per-buffer pages with
  `page_end_offsets` + `page_value_count` per buffer
  + custom partial-buffer materializer; ADR 052
  secondary-index point reads accelerated via
  SIMD-page fast path.
- **ADR 058 / 059** — COCD substrate + native per-
  cascade kernels evaluating predicates against
  encoded bytes; default-enabled for the narrowed
  native set; produces an `EncodedRowMask` (`All` /
  `None` / `Bitmap` / `Runs`).

(ADR 056 prose used phase letters A/B/C; ADR 057
added a fourth stage D. The convention
`plans/conventions/no-planning-phase-names.md`
supersedes that vocabulary. Existing code identifiers
using `phase_*` are leaks slated for the broader
convention sweep — `PhaseBOutcome` →
`PredicateOutcome`,
`apply_phase_b_masks_to_projected_row_groups` →
`apply_survival_masks_to_projection_batches`,
`phase_c_fetch` → `projection_fetch`. ADR 060 must
not extend the leak; new identifiers use semantic
names from day one.)

The remaining inefficiency: **the projection-fetch
stage still fetches full buffers of every projected
column on every surviving row group.** A 1 M-row
record with one row group surviving and 100 selected
rows clustered in two pages still fetches and
decodes every page of every projected column buffer
in that row group. The mask is applied *after*
materialize, so bytes-fetched + decode-time scale
with row-group size, not selected-row count.

ADR 052's secondary-index point-read SIMD-page fast
path proved the substrate can fetch + decode
individual encoded pages. ADR 060 generalizes that
capability from named point rows to **mask survivors
under clustered selectivity**.

The target is **clustered sparse survival**, not
random uniform low selectivity. Random 1% masks
touch every SIMD page; page-grain narrowing wins
nothing because the page set equals the buffer page
set. The target is sorted/ranged predicates,
secondary-index assisted row sets, monotonic Delta /
sorted Dictionary outputs, and other masks where
selected rows concentrate in a small subset of ADR
057 pages.

ADR 060 is a **fetch-layer** optimization, not a
compute-layer one. COCD (058 / 059) reduces
predicate CPU; ADR 060 reduces projected-column
bytes-fetched + decode-time when the survival mask
is clustered.

Constraints carry forward unchanged: ADR 003, ADR
016, ADR 020, ADR 022, ADR 029, ADR 030, ADR 035,
ADR 036, ADR 037, ADR 039, ADR 040, ADR 042, ADR
043, ADR 044, ADR 052, ADR 053, ADR 054, ADR 055,
ADR 056, ADR 057, ADR 058, ADR 059.

## Decision

Add a sparse projection-fetch narrowing leg inside
the existing
`L2FetchStrategy::RowGroup(RowGroupFetchPlan)` path.
When predicate evaluation produces an authoritative
exact-COCD `EncodedRowMask` for a surviving row
group, the reader may convert that mask into a
selected encoding-page set per projected buffer and
fetch only the byte ranges spanning those pages.

This is **internal reader dispatch**:

- no new `L2FetchStrategy` variant;
- no new wire `MsgType` / `FRAME_FLAG_*`;
- no new `NodeCapability` bit;
- no descriptor field, no IPC `custom_metadata` key;
- no `EncodingCascade` / `EncodingComputeEvaluatorKind`
  / `EncodedAggregateKind` / `EncodingRejectCode` /
  `EncodingFallbackReason` / `RowGroupFallbackReason`
  / `EncodedRowMask` / atom variants;
- no public Toron-visible PBC opcode;
- no ADR 056 `RecordPageDirectory` shape change;
- no ADR 057 `EncodingDirectory` shape change (per-
  page row counts already exist in
  `BufferEncoding.page_value_count`);
- no new `L2EncodingFetchConfig` /
  `L2RowGroupFetchConfig` / `EncodingComputeConfig`
  field in V1.

Sparse projection fetch engages per row group when
*all* the following hold:

1. `l2_row_group_fetch.enabled = true` and
   `l2_encoding_fetch.enabled = true`.
2. Predicate evaluation produced an exact-COCD
   `EncodedRowMask` for at least one surviving row
   group; the predicate outcome carries no dense
   predicate batch that must be zipped into the
   final projection.
3. The projection requirement is non-empty after
   excluding any buffers already fetched for
   predicate evaluation.
4. Every projection-required value buffer for the
   row group is **page-materializable** (see §"Page-
   materializable columns").
5. Selected page boundaries match across every
   projection-required value buffer for that row
   group (cross-column geometry agreement; per-row-
   group uniform decision grain).
6. Selected pages pass existing economic caps
   (`max_simd_pages_per_record`,
   `max_ranges_per_record`, selected byte fraction,
   selected ciphertext byte fraction) **and** the
   internal per-buffer fragmentation budget.
7. The estimated sparse encoded byte count is
   **strictly less** than the full-row-group
   projection-fetch byte count for the same
   requirement.

If any check fails, the reader uses the existing
full-row-group projection-fetch path. This is a
**plan-time choice**, not fallback after a partial
materialize attempt.

## Architectural inextricability

Three design choices physically inseparable in V1:

**1. Mask-to-page selection + sparse materialization
+ byte-identical projected invariant ship together.**
Mask-to-page selection without sparse materialization
would only reduce HTTP byte ranges without reducing
decode work, but the materialize substrate (ADR 057
partial-buffer materializer) is the load-bearing
piece — without it, even narrow byte ranges still
require the cascade decoder to walk the whole buffer.
Sparse fetch + materialize without the byte-equality
invariant proof would silently break ADR 040 dense-
AGG / ADR 052 point-read / ADR 058-059 COCD
guarantees that downstream consumers depend on. The
three ship together. The dense projection-fetch path
stays as the reference oracle the property suite
calls and as the production fallback for all
sublanes the eligibility layer rejects.

**2. Sparse narrowing is internal fetch-planner
dispatch — Toron contract is byte-identical post-
implementation.** Closed dispatch, no plug-in trait,
no new public PBC opcodes, no descriptor field, no
NodeCapability bit. The decision lives entirely
inside the projection-fetch leg of
`L2FetchStrategy::RowGroup(plan)`; the predicate-
evaluation / projection-fetch interface (per-row-
group `EncodedRowMask` + projected column set + zip)
does not change. Toron does not know whether
Wendelon fetched a single page or a whole buffer for
a given column — the decoded batch is byte-identical.
`RowGroupFetchPlan.surviving_groups` remains row-
group grain; the fact that projection fetch is
sparse is represented by the existing
`phase_c_fetch.selected_plaintext_ranges` field
(convention sweep target → `projection_fetch`) and
an internal `SparseProjectionFetchPlan`, not by a
new strategy variant.

**3. V1 engages only for encoded predicate
evaluation (exact COCD masks); dense predicate-
column zip stays on existing path.** Dense
predicate-column zip would require page-sliced
zipping of dense predicate batches with sparse
projected batches — a load-bearing extra correctness
argument with its own row-alignment failure modes.
Decoupling V1 from dense predicate zip keeps the
byte-identity proof tight and matches the path ADR
059 made newly valuable: native COCD masks driving
narrower projection fetches. Dense predicate zip
earns its own follow-up.

## Identity-vs-Locality framing

Sparse projection fetch is identity-neutral and
locality-neutral. It runs after row-group admission
inside a single record body. It does not change
`Identity`, `Locality`, `RecordLocation`,
`RecordRowIndex`, or the visible row set. The same
logical row mask produces the same output rows
regardless of whether the reader chose full-row-
group projection fetch or sparse page projection
fetch.

Cross-replica determinism is preserved by **output
byte equality**, not by forcing every replica to
choose the same physical fetch plan. Like ADR 058 /
059 COCD dispatch, different internal reader paths
must converge to the same projected batch. Unified
Ring rule applies. There is no separate "narrowing
plane" — sparse projection fetch runs in the same
storage-read surface as ADR 057's encoding-
materialization stage, ADR 052's secondary-index
point-read fast path, and ADR 058 / 059's substrate
evaluators. Cross-locality compaction does not apply
(per-record substrate, downstream of admission,
upstream of ADR 055's per-locality compactor).

## V1 scope

ADR 060 ships **only** the sparse projection-fetch
narrowing path and the bench evidence that gates
default enablement. ADR 056 / 057 / 058 / 059
catalogs unchanged.

### 1. Mask-to-page selection

For each surviving row group with an exact-COCD
`EncodedRowMask`, **mask shape is inspected first**,
before projection-column eligibility checks. This
ordering is load-bearing: a `None` mask must
short-circuit a group out of the projection-fetch
set regardless of its projected columns' shapes.
If projection-eligibility ran first, a no-survivor
group with a nullable / variable-width / non-page-
emitting projected column would route to dense
projection fetch and pull an entire row group's
worth of bytes only to filter to zero rows.

- **`None { rows }`** — no page selected; remove
  the row group from the projection-fetch set
  entirely (zero bytes fetched; no decline
  variant; no dense-fallback). The
  short-circuit fires before projection
  eligibility.
- **`All { rows }`** — every page selected;
  sparse has no advantage; route to dense
  (full-row-group projection fetch handles `All`
  masks correctly regardless of projection
  cascade).
- **`Bitmap { rows, bytes }`** — for each set bit
  `r`, page index is determined by the buffer's
  `RowToPageMap` (built from
  `BufferEncoding.page_value_count` +
  `page_end_offsets` via the existing
  `expand_page_descriptors` helper). Compute the
  unique page-index set
  (`ActivePageSet::Sparse(...)`).
- **`Runs { rows, runs }`** — each run `[start,
  end)` projects to a contiguous page-index range
  via the `RowToPageMap`. Take the union.

Walker output is an `ActivePageSet` per (row group ×
required projected buffer):

```text
ActivePageSet::None              -- no pages selected; group dropped
ActivePageSet::All               -- every page selected; route to dense
ActivePageSet::Sparse(Vec<...>)  -- ordered, deduplicated page indices
```

```text
survival mask
  → RowToPageMap from existing page descriptors
  → ActivePageSet for one row group
  → SparseProjectionFetchPlan after projection / economic checks
```

The `RowToPageMap` reads existing
`BufferEncoding.page_value_count` (recorded per-
buffer at directory build) — no new directory
fields, no codec-shape assumptions about rows-per-
page being constant across cascades.

### 2. Page-materializable columns

A projection-required column is **page-materializable**
when:

- the column has Arrow fixed-width value bytes;
- the column is known non-null for the row group, or
  the null case is routed to the full-row-group path
  (V1: nullable projected columns route to dense;
  see Open questions Q2);
- its value buffer has a `BufferEncoding` entry with
  non-empty `page_end_offsets`;
- its cascade is in the V1 default engagement
  set: **`BitPacked`** and
  **`Dictionary { inner: Plain | BitPacked }`**.
  `Rle` is correctness-eligible because
  `BufferEncoding.page_value_count` records rows-
  per-page for byte-run RLE the same as for
  BitPacked / Dictionary; row→page mapping is
  deterministic from directory metadata regardless
  of byte-length variance per page. Phase 7
  evidence settles RLE default routing as **default-
  off** via eligibility-layer cascade exclusion
  (matching ADR 059's narrowed-default-set
  pattern);
- selected page boundaries match across every
  projection-required value buffer in the row group
  (cross-column geometry agreement — without this,
  per-page row alignment between projected columns
  cannot be proved).

This mirrors the safety profile of the existing ADR
052 secondary-index encoded-page fast path.
**Nullable projected columns**, **variable-width
columns**, **`Plain`**, **`Lz4Frame`**, **`Delta`**,
**`Fsst`**, and **`Alp`** projections fall back to
full-row-group projection fetch in V1. They earn
sparse handling in later ADRs.

The restriction is about **projection
materialization**, not predicate evaluation. A Delta
predicate can still produce the survival mask via
ADR 058 / 059; ADR 060 uses that mask to narrow
projection fetches when the projection columns are
page-materializable.

**Codec verification (load-bearing for RLE
inclusion).** `BufferEncoding.page_value_count` is
declared at `src/storage/encoding/types.rs:185` as
`SimdPageValueCount`. It is set per-buffer at
directory build time
(`src/storage/encoding/directory.rs:160`) and
consumed at materialize time
(`src/storage/encoding/page.rs:21`) where
`expand_page_descriptors` uses it as the per-page
row count with a last-page partial fallback. This
holds for every page-emitting cascade including RLE.
Row→page mapping is deterministic from directory
metadata; iter-2's "byte-run = variable rows per
page" framing was factually wrong.

### 3. Survival-mask source discipline

Sparse projection fetch consumes survival masks as
**authoritative**. It does not re-run PBC during
projection fetch. It does not reinterpret predicate
semantics.

V1 engages **only for exact-COCD predicate-
evaluation outcomes** where
`predicate_outcome.decoded_predicate_batches` (the
existing `PhaseBOutcome.decoded_predicate_batches`,
sweep-renamed) is **empty**. Dense predicate-fetch
groups (where ADR 058 falls through to dense
materialize-then-evaluate) continue through the
existing predicate / projection zip path until a
follow-up ADR adds page-sliced zipping for dense
predicate batches.

This narrows V1 to the path ADR 059 made newly
valuable: native COCD masks driving narrower
projection fetches.

### 4. Sparse materialization

For each selected (row group × selected page) pair:

1. Fetch each required projected encoded page range
   (coalesced into HTTP multi-range GETs, reusing
   the existing range coalescer and the
   `crypto.plan_tiered_multi_range_read(...)` path).
2. Decode the page with the existing ADR 057
   `decode_page` path (called via the shared helper
   in `page_materialize.rs`).
3. Build a page-scoped projected batch covering the
   selected page's rows.
4. Apply the page-local slice of the survival mask:
   - page first row =
     `EncodingPageDescriptor.first_value_index`;
   - page row count =
     `EncodingPageDescriptor.value_count`;
   - page mask length must equal page row count
     (assert; mismatch returns
     `EncodingRejectCode::DecodedLengthMismatch`).
5. Concatenate filtered page batches in
   `(row_group_index, page_index)` order.
6. Concatenate groups in row-group order.
7. **Bypass the standard final survival-mask
   application** for sparse outputs — sparse
   materialization has already applied page-local
   masks. Double-application would corrupt row
   counts.

The final `ProjectedRecordBatch` is byte-equivalent
to the existing full-row-group projection-fetch path
followed by `arrow::compute::filter` with the same
survival mask. Row order is stable. Storage columns
remain subject to the same requirement rules as ADR
056.

### 5. Economics

Sparse projection fetch is a physical optimization,
so it **must be cheaper before it is chosen**. Plan-
time comparison:

- **Sparse byte estimate** = sum of selected encoded
  page byte ranges per buffer, summed across
  buffers, after coalescing.
- **Dense byte estimate** = full-row-group
  projection-fetch selected plaintext bytes for the
  same requirement (existing computation in
  `selected_row_group_record_body_ranges_excluding_requirement(...)`).
- **Decline sparse if** sparse_bytes ≥ dense_bytes
  (no positive byte savings).
- **Cap by existing fragmentation budgets**:
  `max_simd_pages_per_record` (encoding fetch
  config), `max_ranges_per_record` (row-group fetch
  config), `max_selected_byte_fraction_micros`,
  selected ciphertext byte fraction.
- **Plus the internal per-buffer fragmentation
  budget** — bench-tuned `const` in
  `src/storage/encoding/sparse_projection_fetch.rs`.
  Distinct from `max_ranges_per_record` because
  that cap is record-grain and does not constrain
  per-buffer pathology (one buffer with many tiny
  ranges fits the record cap but burns RTT).

Existing row-group and encoding caps are sufficient
for V1 *combined with* the internal per-buffer
fragmentation budget. Do **not** add
`enable_sparse_projection_fetch`,
`min_sparse_byte_fraction`, or any operator-visible
sparse knob in V1.

### 6. Internal decline discipline

The sparse planner uses internal semantic decline
reasons (`AllRowsMask`, `MissingExactCocdMask`,
`DensePredicateBatchPresent`,
`PageGeometryMismatchAcrossColumns`,
`ByteEstimateNotSparse`,
`PerBufferFragmentationCapExceeded`,
`RangeCapExceeded`, `CascadeNotPageEmitting`,
`CascadeNotProjectionEligible`,
`NullableProjectedColumn`,
`VariableWidthProjectedColumn`,
`SimdPageCapExceeded`,
`SelectedByteFractionExceeded`, `Disabled`) to keep
tests precise. Those reasons:

- live entirely inside
  `src/storage/encoding/sparse_projection_fetch.rs`;
- never become new `EncodingFallbackReason` /
  `RowGroupFallbackReason` / reject-code variants;
- never become new metric labels;
- never escape via wire / descriptor / capability
  surface.

They are **plan-time routing facts inside the
reader**, used for unit-test assertions and
internal observability.

### 7. Metrics

Use existing bounded metric families. **No new
labels in V1.** Semantic label values (`projection`
instead of `phase_c`, `predicate` instead of
`phase_b`) are the convention-sweep target; the
current code's `kind="phase_c"` label is an
existing naming leak, not an ADR 060 precedent.

- `storage_l2_row_group_fetch_bytes_total{kind=
  "projection"}` (currently `kind="phase_c"`)
  reports sparse projection-fetch bytes as today;
- `storage_l2_row_group_fetch_bytes_total{kind=
  "avoided"}` reports the bytes-saved delta when
  sparse engages;
- `storage_l2_row_group_fetch_selected_ranges`
  reports the post-coalesce range count;
- `storage_l2_encoding_simd_pages_fetched` reports
  the selected page count.

Strategy metrics still report `row_group`. Sparse
decisions are observable from lower projection-
fetch bytes combined with elevated SIMD-page fetch
counts.

**Sparse declines are not fallback events.** The
internal `SparseProjectionFetchDecline` reasons
are plan-time routing facts — a choice between
two byte-equivalent paths (sparse vs full-row-
group projection fetch) within the existing
`L2FetchStrategy::RowGroup` leg. They do not
trigger any `EncodingFallbackReason` /
`RowGroupFallbackReason` metric event. The pre-
existing `EncodingFallbackReason` labels remain
reserved for ADR 057 / 058 / 059 fallback events
(cascade unsupported by the encoding-compute
substrate, dense materialize-then-evaluate
because COCD declined a leaf, etc.); ADR 060
does not add new fallback events, does not reuse
those labels for its own routing decisions, and
does not surface internal decline state on any
public metric. Sparse-vs-dense routing is
inferred from the byte / page / range counters
above, not from a labeled fallback event.

### 8. Closed reject taxonomy

ADR 057 `EncodingRejectCode`:

- `EncodingDirectoryParseError` — reused.
- `BufferEncodingMismatch` — reused.
- `PageRangeOverflow` — reused.
- `CascadeUnsupported` — reused.
- `DecodedLengthMismatch` — reused.

No new variants. No new `RowGroupRejectCode`
variants. No new `EncodingFallbackReason` /
`RowGroupFallbackReason` variants.

### 9. Default activation

V1 activation is **implicit on the existing pair of
toggles**: `l2_row_group_fetch.enabled = true` AND
`l2_encoding_fetch.enabled = true`. Both are
already default-enabled after ADR 056 / 057
promotion. ADR 060 adds **no new flag**.

### 10. Bench gate

Bench artifact at
`plans/impl/060_sparse_projection_fetch_narrowing_bench_results.md`.

Bench matrix:

- **Row counts**: 8K / 65K / 1M (1M mandatory).
- **Mask shapes**: clustered first-page, clustered
  middle 1%, clustered 5%, clustered 25%, random
  uniform 1%, `All`, `None`.
- **Predicate-mask sources** (drives the survival
  mask): BitPacked `UInt8`, Dictionary plain low-
  card, Dictionary bitpacked low-card, monotonic
  Delta `Int64`.
- **Projected-column cascades**: BitPacked `UInt8`
  / `UInt16` / `UInt32`, Dictionary plain low-card,
  Dictionary bitpacked low-card, **RLE byte-run
  (default-off after Phase 7 evidence)**, Plain
  (dense control), Delta (dense control), Lz4Frame
  (dense control), nullable BitPacked (dense
  control).
- **Projected-column count**: 1 / 4 / 16 / 64.
- **Three-way per-cell measurement**: sparse
  projection fetch (ADR 060) vs full-row-group
  projection fetch (ADR 056 baseline) vs ADR 052
  SIMD-page point-read (lower-bound comparator,
  single-row case).
- **Bytes-fetched counter**: instrumented via
  `MockObjectStore` GET counter.
- **Per-buffer fragmentation budget tuning row**:
  sweep candidate values (e.g., {2, 4, 8, 16}) to
  find the bench-stable starting `const`.
- **Mixed-cascade row**: projected column set
  containing one sparse-eligible + one dense-only
  cascade — verifies per-row-group uniform
  decision routes the whole group to dense if any
  required column is ineligible.

Promotion gates:

- clustered 1% / 5% masks reduce projection-
  fetched **plaintext** bytes by **≥ 3×** vs full-
  row-group on representative `BitPacked` and
  `Dictionary` projected fixtures. This is the
  byte quantity the plan-time sparse_bytes <
  dense_bytes economic gate estimates;
- clustered 1% / 5% masks improve end-to-end read
  CPU or wall time by **≥ 1.5×** after multi-
  range RTT overhead;
- encrypted ciphertext-byte ratios are recorded but
  **not gated** in ADR 060 V1. ADR 044's chunk-
  aligned authenticated range-read substrate rounds
  selected plaintext ranges out to chunk boundaries,
  which creates an architectural floor around the
  1.0×–1.1× ratios observed on representative 1M
  fixtures. Substantial ciphertext-byte narrowing
  requires a separate page-aligned authenticated-
  chunk layout ADR;
- sparse plans stay within the chosen per-buffer
  fragmentation budget;
- random 1% masks decline sparse or stay within
  5% of dense path;
- `All` masks decline with no positive overhead
  beyond planning noise;
- sparse output equals dense projection fetch +
  survival mask byte-for-byte;
- ADR 052 / 056 / 057 / 058 / 059 focused tests
  stay green with sparse enabled and disabled;
- cross-replica determinism byte-identical;
- 1M tier mandatory; 8K and 65K rows
  informational;
- no dense-fallback control row slower than the
  current dense baseline without checked-in
  council disposition;
- **RLE rows record default-off explicitly** via
  eligibility-layer cascade exclusion. RLE remains
  byte-correct by page metadata, but Phase 7 does
  not provide positive economic evidence for
  default engagement; the value-run format question
  remains a separate ADR. RLE outcome is non-
  blocking for BitPacked / Dictionary promotion.

The ciphertext-to-plaintext gate revision above is
the checked-in council disposition for Phase 7
evidence. It follows the ADR 056 / 057 / 058 / 059
provisional discipline: starting gates may be
revised only with bench evidence and recorded
rationale, not weakened silently.

### 11. Architectural lockdown

Incremental guards in `tests/architecture_test.rs`:

- file-digest pin on
  `src/storage/encoding/page_materialize.rs`
  (shared encoded-page materialization helper
  extracted from `manager.rs`'s ADR 052 secondary-
  index path);
- file-digest pin on
  `src/storage/encoding/sparse_projection_fetch.rs`
  (sparse planner + decision + economic logic);
- production projection-fetch path (currently
  `phase_c` shape in `manager.rs` /
  `row_group.rs`) routes through sparse
  eligibility layer; full-row-group projection
  fetch remains visible AND production-fallback-
  visible;
- no new `L2FetchStrategy` variant;
- no new variants in `EncodingRejectCode`,
  `EncodingFallbackReason`,
  `RowGroupFallbackReason`, `EncodedRowMask`,
  `EncodingCascadeKind`,
  `EncodingComputeEvaluatorKind`,
  `EncodedAggregateKind`;
- no new fields in `L2EncodingFetchConfig`,
  `L2RowGroupFetchConfig`,
  `EncodingComputeConfig`,
  `EncodingWriterConfig`;
- no `sparse_projection_fetch_*` config field;
- per-buffer fragmentation budget exists as a
  `const` in `sparse_projection_fetch.rs`, not a
  `Config` field;
- decline-reason enum exists only inside
  `sparse_projection_fetch.rs`; no escape to
  `EncodingFallbackReason` or metric labels;
- `page_materialize.rs` owns decode + assembly
  helpers only — no planner decisions;
- `sparse_projection_fetch.rs` owns planner +
  decision + economic logic only — no decode
  helpers duplicated;
- no Cargo feature gating sparse narrowing;
- no nightly rustc adoption (build passes on
  stable);
- no public PBC opcode addition;
- no new `NodeCapability` bit;
- no new wire `MsgType` / `FRAME_FLAG_*`;
- no IPC `custom_metadata` key change;
- no ADR 057 `EncodingDirectory` /
  `BufferEncoding` shape change;
- no `ObjectStore` trait change;
- bench artifact presence at the promotion path
  (Phase 8 guard);
- existing ADR 052 secondary-index encoded-page
  path still passes after the page-materialize
  helper extraction (no behavior change);
- existing metric labels retained; no new family;
- newtype-unit rule — any new newtypes route
  through `src/schema/units.rs` via `impl_unit!`;
- no version suffix on new types;
- no planning-phase identifier on new symbols
  (per
  `plans/conventions/no-planning-phase-names.md`):
  new types use `SparseProjectionFetch*` and the
  test file is
  `tests/row_group_sparse_projection_fetch.rs`,
  not `SparsePhaseC*` /
  `tests/sparse_phase_c_test.rs`.

## V1 deferred

ADR 060 deliberately does **not** ship:

- **dense predicate-column zip with sparse
  projection** — page-sliced zipping with dense
  predicate batches; follow-up ADR (Q3);
- **nullable fixed-width sparse projection** —
  V1 routes to dense; follow-up considers
  validity-page slicing (Q2);
- **variable-width sparse projection** (Utf8 /
  Binary projected columns) — follow-up ADR;
- **`Plain`**, **`Lz4Frame`**, **`Delta`**,
  **`Fsst`**, **`Alp`** sparse projections —
  full-buffer or non-page-emitting in V1;
- **sparse predicate-column fetch** (the
  predicate-evaluation stage's predicate-column
  fetch runs *before* a survival mask exists;
  that path needs a different selectivity
  signal; follow-up ADR);
- **page-sliced sparse aggregate
  materialization** — encoded aggregate folds
  (ADR 058 / 059) already consume masks;
  sparse-fetch-driven aggregates are redundant
  for full-row-group folds and follow-up for
  partial folds;
- **page-level min/max stats** — own ADR (would
  unlock predicate-driven page skipping in
  predicate evaluation, not projection fetch);
- **page-level null counts** — own ADR;
- **Delta prefix-sum side table** — own ADR;
- **cross-row-group sparse coalescing** beyond
  `max_ranges_per_record`;
- **provider-native multi-range API** in
  `ObjectStore` trait;
- **learned / adaptive sparse routing**;
- **secondary-index named-row + sparse
  projection-fetch composition** — follow-up;
- **per-cascade mixed sparse/dense within one
  projected batch** — V1 uses uniform per-row-
  group decision;
- **promoting the per-buffer fragmentation
  budget to operator config** — V1 ships as
  internal helper-local `const`;
- **RLE value-run format follow-up** — own
  ADR;
- **page-aligned authenticated chunk layout for
  ciphertext-byte narrowing** — own ADR; ADR 060
  controls selected plaintext pages and decode work,
  while ADR 044 chunk granularity bounds ciphertext
  range savings in V1;
- **runtime CPU-feature dispatch shim**, Cargo
  feature gating, nightly rustc adoption;
- **new wire `MsgType`** / `FRAME_FLAG_*` /
  `NodeCapability` bit / envelope
  `format_version` / footer extension /
  descriptor change / digest domain.

## Provisional discipline

Inherits ADR 056 / 057 / 058 / 059. The starting
gate used ciphertext-byte narrowing as a proxy for
fetch economics. Phase 7 evidence showed that ADR
044 chunk-aligned authenticated range reads bound
ciphertext-byte savings even when selected
projection plaintext bytes and wall-clock improve
substantially. Council revised the shipped gate to
**≥ 3× selected plaintext projection-byte
reduction** plus **≥ 1.5× wall-clock improvement**,
with ciphertext-byte ratios recorded as an ADR 044
substrate floor. This mirrors ADR 056's documented
gate revision pattern: a Phase 7 miss does not
silently weaken the engagement decision; revision
requires bench evidence and explicit council
disposition recorded in the bench artifact.

The internal per-buffer fragmentation budget is a
*provisional* `const`. Phase 7 bench results
choose its starting value; Phase 9 may revise it
on later operator evidence. Promoting it to a
`Config` field is a separate ADR decision.

## Rejected alternatives

- **Dense fetch, sparse decode.** Fetch the whole
  buffer; only decode active pages. Rejected —
  wastes selected projection bytes and leaves
  decode work tied to row-group size instead of
  selected-page count.
- **Row-level sparse fetch.** Fetch exact byte
  ranges for individual rows. Rejected — encoding
  cascades operate at the page level. Row-level
  addressing breaks codec boundaries.
- **Wait for page-level min/max/null stats.**
  Page-level stats apply to *input columns
  before* evaluation; sparse projection fetch
  applies to **output columns after the predicate
  is evaluated**. Orthogonal and composable, not
  substitutes. Page-level stats earn their own
  ADR.
- **Dense predicate-zip-included V1.** Page-
  sliced zipping of dense predicate batches with
  sparse projected batches. Rejected for V1 —
  adds a load-bearing extra correctness argument
  without a clean byte-equality oracle. Follow-
  up.
- **New L2 fetch strategy variant
  (`L2FetchStrategy::SparseRowGroup`).** Rejected
  — sparse projection fetch is internal dispatch
  inside the existing `RowGroup` strategy.
- **Sparse predicate-column fetch (in this
  ADR).** Rejected for V1 scope — predicate-
  column fetch runs *before* the survival mask
  exists. Follow-up ADR.
- **Per-cascade allow-list config field.**
  Rejected — cascade-grain misses enforced at
  the eligibility layer; config-grain allow-list
  pattern (ADR 058) does not apply because ADR
  060 has only one grain.
- **Strict-exclude RLE on variable-rows-per-page
  reasoning.** Rejected after codec verification:
  `BufferEncoding.page_value_count` records rows-
  per-page for every page-emitting cascade. RLE
  is correctness-eligible; default routing is
  bench-settled as off for V1 (Q1).
- **Operator-visible per-buffer fragmentation
  knob.** Rejected for V1 — bench-tuned starting
  `const` is the right shape; operators do not
  tune fragmentation budgets. Promotion requires
  evidence of operational pressure in a follow-
  up ADR.
- **Single module hosting helpers + planner.**
  Rejected — separation of concerns favors
  split: `page_materialize.rs` for passive
  decode + assembly helpers (shared with ADR
  052), `sparse_projection_fetch.rs` for active
  planner + decision + economic logic.
- **Mixed sparse / dense projected columns
  within one row-group materialization pass.**
  Rejected for V1 — uniform per-row-group
  decision keeps the byte-equality oracle
  tight; per-column mixing is a follow-up if
  benches show wins on the table.

## Resolved questions

The three forks the council iterated on have
resolved dispositions. None are correctness
questions; each is either bench-empirical or a
scope deferral. Promotion records the disposition
in this section; the bench artifact and follow-up
ADRs carry the empirical evidence and forward work.

**RLE byte-run projection default routing —
default-off, non-blocking.**

Codec verification confirms RLE row→page mapping
is deterministic (`page_value_count` recorded
per-buffer). RLE is correctness-eligible for
page mapping, but Phase 7 evidence records RLE as
default-off via eligibility-layer cascade
exclusion (matching ADR 059's narrowed-default-
set pattern). This decision is byte-correct,
recorded in the V1 changeset, and **non-blocking
for BitPacked / Dictionary promotion**. RLE value-
run format work remains a separate ADR.

**Nullable fixed-width support — closed by ADR 064.**

ADR 064 lifts the nullable projected-column decline for raw
byte-addressable validity buffers. Encoded-validity page
decoding remains a separate follow-up.

**Dense predicate-zip with sparse projection — closed by
ADR 065.**

ADR 065 lifts the dense-predicate-batch sparse-projection
cliff for the scalar dense-decode branch while keeping the
planner metadata-only and the projection economic gate
unchanged. Sparse predicate-column fetch and page-sliced PBC
evaluation remain separate follow-up ADRs.

## What not to do

- **Don't extend ADR 057's `EncodingDirectory`
  shape.** Per-page row counts already exist in
  `BufferEncoding.page_value_count`. ADR 060
  reads it; ADR 060 does not add fields.
- **Don't add a fifth `L2FetchStrategy`
  variant.** Sparse projection fetch is internal
  dispatch inside `RowGroup`.
- **Don't add a sparse config knob in V1.** Use
  existing row-group and encoding caps plus the
  internal per-buffer fragmentation budget.
  Operator-visible promotion requires a revised
  ADR decision with operational pressure
  evidence.
- **Don't re-run PBC during projection fetch.**
  Survival masks are authoritative; ADR 056's
  invariant holds.
- **Don't fetch sparse pages for columns that
  cannot be assembled byte-identically.** Fall
  back to full-row-group projection fetch. The
  byte-equality oracle is the load-bearing
  correctness argument.
- **Don't ship sparse narrowing without the
  byte-identical projected-batch property
  suite.** Without it, sparse narrowing could
  silently corrupt projection output and ADR
  040 / 052 / 036 invariants downstream.
- **Don't double-apply the survival mask.**
  Sparse materialization applies page-local
  mask slices during decode; the standard final
  survival-mask application is bypassed for
  sparse outputs.
- **Don't introduce a new `EncodedRowMask`
  variant.** The existing four-variant enum is
  sufficient.
- **Don't introduce planning-phase identifiers
  on new symbols.** Per
  `plans/conventions/no-planning-phase-names.md`,
  new types use `SparseProjectionFetchPlan`, not
  `SparsePhaseCPlan`. New tests live at
  `tests/row_group_sparse_projection_fetch.rs`,
  matching the existing `tests/row_group_*`
  convention. Existing leaks (`phase_c_fetch`,
  `apply_phase_b_masks_to_projected_row_groups`,
  `PhaseBOutcome`) are referenced by current
  name for accuracy and are slated for the
  broader convention sweep — ADR 060 must not
  extend them.
- **Don't exclude RLE for row-mapping reasons.**
  `BufferEncoding.page_value_count` makes RLE
  row→page mapping deterministic; RLE default
  routing is an economic bench call now settled
  as default-off (Q1).
- **Don't widen COCD catalogs.** ADR 060
  *consumes* masks; it does not add evaluator
  kinds, aggregate kinds, atoms, reject codes,
  or fallback reasons.
- **Don't add a new metric label.** Existing
  bounded label sets are sufficient; sparse
  decisions are observable from existing bytes /
  range-count / page-count counters.
- **Don't surface internal decline reasons via
  metric labels or fallback variants.** Decline
  reasons are plan-time routing facts internal
  to `sparse_projection_fetch.rs`; they never
  escape.
- **Don't change ADR 057's partial-buffer
  materializer interface gratuitously.**
  Extract shared helpers from `manager.rs`'s
  secondary-index path into
  `page_materialize.rs`; both secondary-index
  and sparse projection fetch share it. Keep
  planner decisions out of
  `page_materialize.rs` — those live in
  `sparse_projection_fetch.rs`.
- **Don't widen `max_ranges_per_record`** from
  ADR 056. Sparse narrowing participates in
  the existing budget plus the internal per-
  buffer fragmentation budget.
- **Don't make sparse projection fetch depend
  on RLE value-run work.** RLE value-run is a
  separate ADR.
- **Don't change secondary-index row
  semantics.** ADR 052's named-row path remains
  a named-row path. ADR 060 reuses page
  materialization helpers for mask-driven row
  groups, not the other way around. Mixed
  named-row + mask-driven projection fetch is
  follow-up.
- **Don't ship a per-cascade or per-record
  activation knob.** Cross-replica byte-
  equality holds because per-replica fetch-
  path decisions still produce byte-identical
  filtered batches.
- **Don't bench against synthetic best-case
  fixtures.** The bench matrix mirrors ADR 056
  / 057 Phase 10 fixture set + ADR 058 / 059
  Phase 8 / 10 cascade matrix.
- **Don't promote with random-mask wins as the
  primary evidence.** Random masks touch every
  page; any random-mask win is noise.
  Promotion evidence must be on **clustered**
  masks.

## Convergence summary

```text
ADR 058 substrate (decode-then-scan, COCD)
  → ADR 059 native per-cascade kernels (encoded compute fast)
  → ADR 060 sparse projection fetch narrowing (encoded fetch sparse)  ← here
  → next Toron-adjacent follow-up (page-level min/max stats,
    FSST evaluator, RLE value-run format, sparse predicate-
    column fetch, dense predicate-column zip with sparse
    projection, nullable sparse projection) — own ADRs
  → capacity-tier compression / serverless / EC  (rest of old #14 bundle)
```

## Promotion criteria

ADR 060 promoted from council-converged → shipped
when:

1. Shared encoded-page materialization helper
   lands at `src/storage/encoding/page_materialize.rs`
   (extracted from `manager.rs`); ADR 052
   secondary-index point-read path passes byte-
   for-byte after extraction.
2. Sparse-narrowing planner + decision land at
   `src/storage/encoding/sparse_projection_fetch.rs`.
3. Byte-identical projected-batch invariant
   property suite passes under `cargo test` on
   the full (cascade × mask shape × selectivity
   × tier) matrix.
4. Bench artifact at
   `plans/impl/060_sparse_projection_fetch_narrowing_bench_results.md`
   demonstrates the revised promotion gates for
   the council-chosen default scope, including RLE
   Q1 default-off resolution and recorded-not-gated
   ciphertext ratios; ADR 061 subsequently closes the
   ciphertext-ratio follow-up with shipped chunk-size evidence.
5. ADR 056 / 057 §"Where we are" amendments
   point at the landed bench artifact.
6. Architectural lockdown guards land per §11.
