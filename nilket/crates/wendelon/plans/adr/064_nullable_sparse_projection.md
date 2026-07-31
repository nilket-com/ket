# ADR 064: Nullable Sparse Projection

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/064_nullable_sparse_projection.md`

## Status

Shipped 2026-05-09 after implementation, full `cargo test`,
`cargo check --benches`, and bench evidence landed together.
Bench evidence is recorded at
`plans/impl/064_nullable_sparse_projection_bench_results.md`.
The Phase 0 fork-resolution audit confirmed raw byte-
addressable validity is a non-no-op V1 engagement path
against current writer output. Headline bench result:
clustered 5% nullable sparse projection passed across
BitPacked and Dictionary fixtures with 5.36x-19.98x
plaintext-byte reduction (value + validity combined) and
1.50x-5.45x wall-clock speedup — substantially exceeding
both the 1.5x plaintext-byte promotion gate and the 3x
headline diagnostic, plus the 1.2x wall-clock gate.

The shipped design is the council-converged result after
six iterations across Claude / Codex / Gemini. Post-review
corrections landed before promotion: per-row-group
`column_null_counts` consultation (the all-valid sparse
short-circuit applies per row group, not per record);
ADR 052 single-source guard and architectural guards for
`NullableProjectedColumn` removal + all-null page-skip
sneak-in; expanded rebase coverage for non-trivial bit
offsets, alternating nulls, and clustered nulls; ADR 063
exact-mask composition tests for `IS NULL`, `IS NOT NULL`,
and nullable value-comparison masks; legacy
`fixed_width_non_null_value_column` removed and replaced
with `fixed_width_encoding_indexed_value_column` for the
ADR 052 path.

ADR 064 is a narrow amendment to ADR 060. It lifts ADR 060's
`SparseProjectionFetchDecline::NullableProjectedColumn`
decline for fixed-width projection columns by fetching and
re-basing the corresponding Arrow validity bitmap slices
alongside selected value pages, and by extending the page
materializer to assemble nullable Arrow arrays without using
dense row-group buffers.

The V1 cut is raw byte-addressable validity only. Validity
buffers with `EncodingDirectory` entries decline to dense
behind a private planner reason. No implementation-time
scope expansion.

ADR 065 is now shipped and composes with ADR 064 for the
scalar dense-decode branch: nullable fixed-width projection
columns can remain on the sparse projection path while dense
predicate columns are page-sliced and zipped from
`predicate_projection_by_group`. ADR 065 does not broaden ADR
064's projection catalog; sparse projected columns remain
fixed-width with raw byte-addressable validity in V1, and ADR
063 encoding-compute rows remain Case A mask-only sparse
projection.

ADR 069 closed this ADR's encoded-validity page-decoding
follow-up for the page-aligned `Rle { inner: Plain }` validity
catalog member. This ADR still does not touch sparse predicate-
column fetch, RLE value-run, FSST / ALP evaluators, variable-
width sparse projection, all-null page-skip optimization, or any
public protocol surface.

### Where we are after ADR 069

ADR 064 shipped raw byte-addressable validity for nullable sparse
projection. ADR 069 adds the encoded-validity producer and reader
for page-local RLE validity bitmaps and narrows
`SparseProjectionFetchDecline::EncodedValidityBufferUnsupported`
to unsupported encoded-validity shapes. Remaining nullable sparse
projection deferrals are variable-width / nested nullable
projection, all-null page-skip, page-stat-assisted validity
skipping, and non-V1 validity cascades.

## Context

ADR 060 ships sparse projection fetch narrowing for exact-COCD
masks, but only for fixed-width *non-null* projection columns.
The current planner declines a row group when any active
projected column has nulls:

```text
SparseProjectionFetchDecline::NullableProjectedColumn
```

That creates a sharp practical cliff: adding one nullable
projected column to an otherwise sparse-friendly query
disables the whole ADR 060 byte-reduction path for that row
group. Most analytical schemas have nullable columns;
restricting sparse projection to non-null columns leaves the
optimization unreachable on realistic workloads.

ADR 060's V1 cut already proved mask → page selection
correctness, cross-column value-page geometry agreement,
selected-byte economics, page-local materialization, dense-
oracle byte equality, and the mask-first
`EncodedRowMask::None` short-circuit. Nullable projection
adds one more invariant: value bytes alone are insufficient
to construct an Arrow array. The page-local output must
carry a validity bitmap whose bit 0 corresponds to the *first
row of the selected page*, not to the row-group offset. For
pages whose row span does not start on a byte boundary, the
bitmap must be re-based, not just byte-sliced.

ADR 063 made null-position correctness explicit on the
predicate side: value comparisons cannot short-circuit
nullable pages without validity positions. ADR 064 applies
the same discipline on the projection side. Sparse projection
may fetch fewer value pages only when it also fetches enough
validity bytes to construct the exact page-local Arrow null
bitmap.

This ADR is intentionally about projection materialization,
not predicate-output shape. ADR 063 produces exact masks from
nullable predicates — including null-aware predicate pages —
and ADR 064 consumes those masks unchanged. The two ADRs
compose multiplicatively on byte economy: ADR 063 narrows
predicate fetch on the page axis; ADR 064 narrows projection
fetch on the column axis (now including nullable columns).

### Three forces

**1. Nullable columns are the operational common case.**
ADR 060's deferral was a closed-catalog convergence artifact,
not a worldview claim that nullable is rare.

**2. ADR 063 makes nullable predicates load-bearing.** Page
stats classify nullable predicates with `null_count`-based
correctness. A workload like
`SELECT id, value FROM t WHERE value IS NOT NULL` benefits
from ADR 063 page-grain prune *and* sparse projection of the
nullable `value` column. Today the projection axis falls back
to dense.

**3. ADR 060's machinery already handles multi-buffer
fetch.** Per-row-group sparse projection planning iterates
per projected column; nullable extension iterates per-buffer
*pair* (validity + value) within a column. No new top-level
shape; just one more byte range per nullable column.

### What this ADR does not propose

- **No lift of ADR 060 / ADR 063's mask-only Phase B
  contract.** Sparse projection still engages only when
  `phase_b_outcome.decoded_predicate_batches` is empty in
  ADR 064 itself. ADR 065 later lifts this for the scalar
  dense-decode branch without changing ADR 064's nullable
  projection catalog.
- **No variable-width nullable.** Separate ADR.
- **No new format version.** ADR 057's `EncodingDirectory`
  byte layout is unchanged.
- **No expansion of the V1 type or cascade catalog.** A
  nullable projected column is eligible only if the same
  column would already pass ADR 060's non-null eligibility.
- **No encoded-validity page decoding in ADR 064 itself.** V1
  declined if the selected nullable column's validity buffer had
  its own `BufferEncoding` entry. ADR 069 later closes the
  page-aligned RLE encoded-validity follow-up.
- **No all-null page-skip optimization in ADR 064 itself.**
  Selected nullable pages fetch the value page and the validity
  bits, then materialization marks null rows. ADR 070 later
  closes this deferral for metadata-all-null fixed-width pages
  by eliding both value and validity fetches and synthesizing
  canonical-null buffers locally.

### Constraints carry forward unchanged

ADR 003, 016, 019, 020, 022, 029, 030, 035, 036, 037, 039,
040, 042, 043, 044, 052, 053, 054, 055, 056, 057, 058, 059,
060, 061, 062, 063. ADR 057 directory shape unchanged.
ADR 060 module location unchanged (extended in place).
ADR 063 page-stats shape unchanged.

## Decision

ADR 064 ships nullable sparse projection for columns that
would already be ADR-060-eligible if they were non-null.

1. **No expansion of the value-column catalog.** A nullable
   projected column is eligible only when the same column
   would pass ADR 060's non-null eligibility:
   - fixed-width value buffer as recognized by the existing
     `page_materialize.rs` fixed-width helper;
   - value buffer has `BufferEncoding.page_end_offsets`;
   - cascade is in ADR 060's default sparse projection
     engagement set: `BitPacked` and
     `Dictionary { inner: Plain | BitPacked }`;
   - selected value-page geometry matches across all
     projected value buffers in the row group.

   Phase 0 pins the exact V1 type and cascade catalog from
   the *current* ADR 060 materializer and planner code, not
   from ADR 063's page-stat catalog. Types not already
   accepted by ADR 060 remain dense. This ADR does not add
   variable-width, RLE, Delta, FSST, ALP, Plain, Lz4Frame,
   or mixed sparse / dense projected-column support.

2. **Raw validity bitmap slices are fetched with selected
   value pages.** For each selected
   `(row group, projected nullable column, page)` whose
   row-group `column_null_counts[physical] > 0`:
   - locate the column's validity buffer via the existing
     `RecordBufferDirectory.column_to_buffers` mapping
     (`null_buffer_ordinal`);
   - locate the row-group validity-buffer byte range via
     the existing
     `RecordPageDirectory.BufferPageDirectoryEntry`;
   - compute the minimal byte span containing the selected
     page's validity bits:

     ```text
     bit_start  = page.first_value_index
     bit_len    = page.value_count
     byte_start = floor(bit_start / 8)
     bit_offset = bit_start % 8
     byte_len   = ceil((bit_offset + bit_len) / 8)
     ```

   - fetch that byte span as part of the same tiered
     multi-range plan as selected value pages;
   - re-base the bits into a page-local validity bitmap
     whose bit 0 is the selected page's first row.

3. **Validity support is raw byte-addressable in V1.** The
   nullable sparse path engages only when the validity
   bitmap can be sliced from the row-group validity buffer
   by byte range. Encoded-validity detection is precise per
   `(row_group_index, null_buffer_ordinal, validity_body_range)`
   — one row group's encoded validity buffer cannot poison
   another row group's raw validity path. If the selected
   nullable column's validity buffer for the selected row
   group has a `BufferEncoding` entry in
   `EncodingDirectory`, V1 declines to dense with a private
   unsupported reason. Encoded-validity page decoding is a
   follow-up ADR.

   **Phase 0 is a fork-resolution gate** for the writer-
   shape question:
   - if validity buffers never have `BufferEncoding`
     entries, the encoded-validity decline is a future-
     proof guard;
   - if validity buffers sometimes have `BufferEncoding`
     entries, V1 engages on raw and declines on encoded;
   - if validity buffers always have `BufferEncoding`
     entries, raw-only V1 is a no-op and **the ADR is
     revised pre-code** rather than landing a planner path
     that always declines.

   ADR 064 adds no new validity directory shape and does
   not bump `ENCODING_DIRECTORY_FORMAT_VERSION`.

4. **Eligibility is joint across value and validity
   buffers.** A nullable projected column engages sparse
   only when:
   - the value buffer passes ADR 060's fixed-width sparse
     projection eligibility;
   - the validity buffer is raw byte-addressable and has
     no `BufferEncoding` entry for the selected row group;
   - the selected value page's row span maps into the
     row-group validity bitmap;
   - all projected sparse columns share the same selected
     page geometry;
   - selected value bytes plus selected validity bytes
     still beat the dense projection byte baseline.

   If `column_null_counts[physical] == 0` for the row
   group, the column remains on the existing all-valid
   sparse projection path and fetches no validity bytes
   merely because the Arrow field is nullable. ADR 064
   changes the nonzero-null-count case that ADR 060
   currently routes dense.

5. **Page-local null counts are computed from the sliced
   validity bitmap.** The page materializer computes the
   page null count while re-basing bits. The scoped
   `RecordBufferDirectory` passed to
   `decode_projected_record_batch_from_buffers` uses that
   page-local null count:
   - `null_count == 0`: scoped null buffer is empty; Arrow
     treats the page as all-valid;
   - `null_count > 0`: scoped null buffer contains the
     re-based page-local validity bitmap.

   The output batch must be byte-identical to dense
   projection after the survival mask is applied.

6. **Mask-first ordering is unchanged.** `EncodedRowMask::None`
   still drops a row group before any projection-column
   eligibility check; nullable projected columns under a
   `None` mask fetch zero projection bytes and do not
   decline to dense. `EncodedRowMask::All` still routes to
   dense.

7. **Dense predicate batches remain excluded.** ADR 064
   preserves ADR 060 / ADR 063's current sparse projection
   precondition:
   `phase_b_outcome.decoded_predicate_batches.is_empty()`.
   Dense predicate-zip with sparse projection is a separate
   ADR.

8. **Existing byte and range economics absorb validity
   bytes.** The sparse projection planner includes selected
   validity byte ranges in selected plaintext byte count,
   selected byte fraction, selected range count,
   `max_simd_pages_per_record`, and
   `MAX_SPARSE_RANGES_PER_BUFFER`. The engagement rule
   remains strict:
   `sparse_projection_plaintext_bytes <
   dense_projection_plaintext_bytes`. If validity bytes
   erase the win, the planner declines.

9. **No public surface changes.** ADR 064 adds no new
   `L2FetchStrategy` / `EncodedRowMask` /
   `EncodingCascadeKind` / `RowGroupRejectCode` /
   `RowGroupFallbackReason` / `EncodingRejectCode` /
   `EncodingFallbackReason` / `EncodingComputeRejectCode` /
   `EncodingComputeFallbackReason` / `PredicateAtomOp` /
   `EncodingComputeEvaluatorKind` / `EncodedAggregateKind`
   variant; no new config field; no new metric label; no
   new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability`
   bit; no new descriptor field; no new digest domain; no
   new IPC `custom_metadata` key; no new `ObjectStore`
   trait method; no new public PBC opcode; no Cargo feature.

10. **No directory shape changes.** ADR 064 uses existing
    `RecordPageDirectory`, `EncodingDirectory`, and
    `BufferEncoding.page_value_count` /
    `page_end_offsets`. No validity-page descriptors,
    null-count directories, page-level projection stats,
    or second metadata key.

11. **No new activation surface.** Sparse projection
    engagement remains controlled by the existing row-group
    and encoding fetch toggles plus private type / cascade /
    economics gates. ADR 064 adds no nullable sparse config
    flag, Cargo feature, metric label, or operator-visible
    engagement knob.

The slab-format byte layout, AAD shape, nonce derivation,
key hierarchy, footer HMAC scope, ADR 022 footer layout,
ADR 056 directory shape, ADR 057 encoding directory shape,
ADR 060 sparse projection planner module location, ADR 061
closed chunk-size set, ADR 062 sizing profile catalog, and
ADR 063 page-stats shape are all unchanged.

## Architectural inextricability

Three design choices are physically inseparable in V1.

**1. Nullable sparse projection must fetch validity bits.**
Reusing value pages without validity bytes would construct
arrays that mark null rows as valid — result corruption,
not a performance regression. Decline-to-dense is
acceptable; output corruption is not.

**2. Validity bits must be page-local.** Arrow arrays
interpret validity bit 0 as row 0 of the array. A byte
slice from the original row group is not enough when the
selected page starts at a non-byte-aligned row; the bitmap
must be re-based.

**3. Dense predicate zip stays separate.** ADR 064 only
changes sparse projection materialization once an exact
survival mask already exists. It does not change predicate
evaluation output or allow sparse projection to consume
`decoded_predicate_batches`. That lift has its own row-
alignment correctness story and its own V2 promotion path.

## Identity-vs-Locality framing

ADR 064 is identity-neutral and locality-neutral. It changes
how a selected projected page is assembled from physical
buffers after row identity, record location, and locality
decisions are already made.

Cross-replica determinism is preserved because nullable page
assembly is a pure function of (selected page set, raw
validity byte slice, bit-offset rebase, row span, ADR 057
directory metadata). There are no runtime branches that
should vary across replicas. Arrow-output byte equality
across replicas holds.

There is no separate "nullable plane." Nullable sparse
projection lives on the same `BufferEncoding` shape ADR 057
placed in the storage-write surface and ADR 060 reads at
plan time. Unified Ring rule applies.

## V1 scope

### 0. Phase 0 catalog decisions

Before code changes, implementation must record the exact
nullable engagement catalog in the implementation plan:

- fixed-width value types accepted by the existing ADR 060
  materializer;
- value-buffer cascade forms accepted by the existing ADR 060
  sparse projection planner;
- whether current writer output stores nullable validity
  buffers as raw row-group body bytes or as
  `EncodingDirectory` entries;
- geometry checks required to prove the selected value
  page's row span maps into the raw validity bitmap.

The V1 validity source is the raw Arrow validity bitmap
already present in the row-group body. Encoded validity is
not eligible in V1. If Phase 0 finds encoded validity is the
production common case, the council revises ADR 064 before
implementation rather than landing a planner path that
always declines.

### 1. Eligible column shape

The sparse projection planner replaces the current
`fixed_width_non_null_value_column` gate with a sparse-
projection column helper that returns:

```rust
pub(crate) struct PageMaterializeColumn {
    pub physical_ordinal: usize,
    pub null_buffer_ordinal: IpcBufferIndex,
    pub value_buffer_ordinal: IpcBufferIndex,
    pub bytes_per_value: usize,
    pub data_type: DataType,
    pub nullable: bool,
}
```

The helper accepts a nullable field only if all other
ADR 060 eligibility checks pass. It still rejects variable-
width and unsupported layouts. The helper name is semantic
(`fixed_width_sparse_projection_column`), not a planning-
stage name.

### 2. Validity fetch shape

The planner extends the private sparse projection plan with
validity fetches:

```rust
pub(crate) struct PageMaterializeValidityFetch {
    pub key: PageMaterializeBufferKey,
    pub body_range: RecordBodyByteRange,
    pub page: EncodingPageDescriptor,
    pub bit_offset: u8,
    pub page_row_count: RowGroupRowCount,
}
```

The body range is the minimal row-group validity-buffer
byte span containing the selected page's bits. `bit_offset`
records the starting bit inside the first fetched byte.
Validity fetches are private planner / materializer state —
never a public fallback reason, metric label, or strategy
field.

### 3. Page-local validity rebase

`page_materialize.rs` owns:

```rust
pub(crate) fn rebase_validity_bitmap(
    fetched: &[u8],
    bit_offset: u8,
    rows: usize,
) -> Result<PageValidityBitmap, ArrowError>;
```

Returns page-local bytes and the page-local null count.
Arrow bitmap bit order is the existing Arrow little-endian
bit order. Helper handles byte-aligned page starts, non-
byte-aligned page starts, tail pages shorter than
`page_value_count`, all-valid / all-null / mixed-validity
selected pages.

### 4. Page-scoped record directory

`projected_page_batch_from_decoded_buffers` is extended to
receive decoded page-local validity buffers. Per projected
column:

- non-null: empty null buffer; `column_null_counts = 0`;
- nullable with page null count 0: empty null buffer;
  `column_null_counts = 0`;
- nullable with page null count > 0: page-local validity
  buffer; `column_null_counts = page_null_count`.

Scoped directory remains uncompressed; no page or encoding
directory attached.

### 5. Economic gate

Sparse bytes include both selected value-page bytes and
selected validity bytes. Dense bytes remain the existing
dense projection byte baseline for the same row groups and
projected columns. If validity bytes erase the win, the
planner declines. Nullable sparse projection is a read-side
optimization, not a correctness requirement.

## Composability

**ADR 060 sparse projection.** ADR 064 amends ADR 060's
projection-column eligibility and page materialization.
Mask-to-page selection, `None` / `All` routing, economics,
internal-decline discipline, and the no-fifth-strategy rule
remain unchanged.

**ADR 063 page-level predicate stats.** ADR 063 produces
exact masks upstream; ADR 064 consumes them unchanged. Page
stats are *not* used to skip validity fetches in V1 — that
would couple projection nullability to ADR 063's fixed-
width page-stat catalog and produce uneven behavior.
Promotion evidence must include shared nullable-predicate
fixtures where ADR 063 narrows predicate bytes and ADR 064
narrows projection bytes, so the combined byte savings are
*recorded instead of inferred*. The artifact must record
the asymmetry explicitly: an ADR 063 `ExactMatch` page can
reduce predicate bytes to zero, while ADR 064 still fetches
selected projection value bytes plus validity bits for the
surviving row span. ExactMatch-driven and Maybe-driven
projection pages are recorded as separate bench rows.

**ADR 061 chunk-aligned reads.** Validity ranges are
ordinary selected record-body byte ranges. ADR 044 / 061
coalescing handles them with the same range planner used
for selected value pages.

**ADR 062 row-group sizing profile.** Nullable sparse
projection should show its strongest absolute byte savings
under the `Scan` profile, where 65K-row groups have enough
pages for clustered selectivity to matter. `PointRead`
remains correctness-covered but does not need default
engagement if validity bytes erase the win.

**ADR 057 encoding pages.** Value-page geometry remains
defined by `BufferEncoding.page_value_count` and
`page_end_offsets`. Validity buffers do not add page
geometry to `EncodingDirectory` in V1.

**ADR 052 secondary-index point reads.** Point reads remain
unchanged. ADR 064 only changes the predicate-driven row-
group sparse projection path. If ADR 052 materialization
needs nullable page-local assembly, it must import the same
`page_materialize.rs` validity helper instead of adding a
second bitmap slicer.

## Architectural lockdown

Incremental guards in `tests/architecture_test.rs`:

- `SparseProjectionFetchDecline::NullableProjectedColumn`
  removed or no longer used as an active nullable fixed-
  width decline.
- Sparse projection planner module location unchanged
  (`src/storage/encoding/sparse_projection_fetch.rs`).
- Per-page validity helpers live in
  `src/storage/encoding/page_materialize.rs`; ADR 052
  secondary-index path imports them (single-source guard).
- `SparseProjectionFetchDecline` remains private; new
  private decline reasons (e.g.,
  `EncodedValidityBufferUnsupported`,
  `ValidityBufferRangeMissing`,
  `NullableTypeNotInCatalog`) do not surface in metrics or
  public fallbacks.
- All-null selected pages do not use a special value-fetch
  skip path (V2 page-skip optimization guard-railed out).
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
- No Cargo feature gating nullable engagement.
- No nightly rustc dependency.
- No version suffix on new types.
- No planning-phase identifier on new symbols.
- No project-name prefix.
- ADR 020 §2 AAD layout, ADR 022 §3 / §5 byte layouts,
  ADR 056 `RecordPageDirectory` shape, ADR 057
  `EncodingDirectory` shape, ADR 061 closed chunk-size
  set, ADR 062 `RowGroupSizingProfile` catalog, ADR 063
  `BufferEncoding.page_stats` shape: all unchanged.
- Cross-replica byte-equal output across two writers with
  the same config + nullable input fixture.
- Dense predicate-zip remains absent from ADR 064 code
  paths.

## Bench fixture mandate

Bench artifact records (matching ADR 060 / 063 cadence):

- predicate-driven nullable selectivity sweep at `Scan`
  profile (`IS NOT NULL` clustered-non-null at 5% / 25% /
  50%; `IS NULL` clustered-null at 5% / 25%; nullable
  value-comparison at 5% / 25%);
- random-uniform / weak masks: ≤ 5% dense-fallback
  overhead;
- mask-first: `None` mask fetches zero projection value
  AND validity bytes; `All` mask routes dense;
- sub-byte page-boundary correctness on row counts not
  divisible by 8 (e.g., 1,025 rows/page);
- all-null / all-non-null page boundaries;
- page-geometry sweep: default page size + non-byte-
  aligned page boundaries;
- ADR 063 composability (combined byte savings strictly
  larger than either ADR alone; ExactMatch-driven and
  Maybe-driven projection pages recorded as separate
  rows);
- ADR 060 non-null path byte-identical pre / post
  (regression);
- ADR 052 secondary-index nullable point read byte-
  identical to dense via shared helper (if path touched);
- cascade coverage (V1 value cascade set ×
  representative null distributions);
- type coverage (every fixed-width type in V1 catalog
  paired with ≥ 1 V1 cascade);
- encoded-validity decline control: a fixture whose
  validity buffer has a `BufferEncoding` entry, if
  constructible, declines with the private unsupported
  reason and routes to dense;
- composability with ADR 061 (p50 / p95 GET payload
  unchanged; ≥ 1 MiB p50 dense-path target preserved);
- composability with ADR 062 (`Scan` profile produces
  strictly larger absolute byte savings than `PointRead`
  on matched-shape fixtures).

**Promotion gate**: clustered 5% nullable sparse rows
keep at least **1.5×** plaintext-byte reduction (value +
validity combined) on representative BitPacked and
Dictionary fixtures, AND at least **1.2× wall-clock**
improvement on representative runtime fixtures (or council
records a revised gate). The bench artifact also reports
whether **≥ 3×** plaintext-byte reduction holds on best-
case clustered fixtures, but that is a *headline
diagnostic*, not the promotion gate.

## V1 deferred (with explicit V2 promotion paths)

ADR 064 does not ship:

- **Variable-width nullable sparse projection** (UTF-8 /
  LargeUtf8 / Binary / LargeBinary). **V2:** focused
  variable-width ADR.
- **Nested-type nullable sparse projection.** **V2:**
  dedicated nested-type ADR.
- **Encoded-validity page decoding.** Closed by ADR 069 for
  page-aligned `Rle { inner: Plain }` validity buffers. Non-V1
  encoded-validity cascades still decline and require their own
  follow-up ADR.
- **All-null page-skip optimization.** **V2:** focused
  page-skip ADR.
- **Page-stat-assisted validity-fetch skipping.** **V2:**
  follow-up ADR; closed-catalog discipline forbids
  coupling projection nullability to ADR 063 page-stat
  catalog in V1.
- **Mixed sparse / dense projected columns within one
  materialization pass.** Inherited deferral from ADR 060.
- **Dense predicate-zip with sparse projection.** Closed by
  ADR 065 for scalar dense-decode predicates. Encoding-
  compute materialization into predicate projection batches
  remains future work.
- **Sparse predicate-column fetch for non-COCD
  predicates.** Different correctness story; separate ADR.
- **RLE value-run / FSST / ALP / Delta sparse
  projection.** Each its own follow-up ADR.
- **Operator-tunable nullable engagement.** Activation is
  the existing pair of toggles plus type + cascade
  eligibility.
- **Validity-buffer Bloom filters / null-bitmap caches /
  null-count side indexes.** Per-class ADR.
- **Cargo feature or operator-tunable engagement.**
- **Provider-native multi-range API.** Separate
  refinement.

## Rejected alternatives

- **Leave nullable columns dense forever.** Validity
  slicing is bounded; nullable fixed-width columns are too
  common for "dense forever" to be a stable cut.
- **Wait for dense predicate zip before handling
  nullability.** Validity bitmap slicing is orthogonal to
  predicate-batch row alignment; can reduce bytes for
  exact-mask plans immediately.
- **Fetch the full validity buffer whenever any selected
  page is nullable.** Correct but undermines sparse
  economics on wide low-selectivity scans.
- **Skip value-page fetches when a selected page is all
  null.** V2 territory; couples projection planning to
  null summaries before the validity contract is settled.
- **Rely on ADR 063 page null counts to avoid validity
  fetches.** Couples projection nullability to a narrower
  predicate-stats catalog; sparse projection must not
  depend on stats being present to be correct.
- **Decode encoded validity buffers in V1.** The
  council's converged closed cut is raw byte-addressable
  validity. If Phase 0 proves encoded validity is the
  production common case, revise ADR 064 pre-code, not
  during implementation.
- **Let Phase 0 silently add encoded validity to V1.**
  Phase 0 is a fork-resolution gate, not an
  implementation-time scope expansion. Raw-only proceeds,
  mixed raw/encoded declines on encoded, encoded-only
  revises the ADR before code.
- **Add validity page descriptors to
  `EncodingDirectory`.** New directory shape for a read-
  side materialization problem.
- **Allow nullable sparse projection only for byte-
  aligned page starts.** Bakes a config accident into
  correctness.
- **Bundle dense predicate zip.** Different handoff
  contract; separate ADR.
- **A new `L2FetchStrategy::SparseNullableProjection`
  variant** or **a separate
  `nullable_sparse_projection_fetch.rs` module.** ADR
  060's no-fifth-strategy / no-fork-the-planner
  discipline.
- **A new `EncodedRowMask` variant for nullable masks.**
  Existing algebra suffices; nullability lives on the
  projected array, not the mask.
- **Operator-visible nullable sparse engagement flag.**
  Implicit activation matches ADR 060.
- **Decline-as-public-fallback-variant.** Decline reasons
  stay private.
- **Forking the materializer.** Single-source discipline.
- **Variable-width nullable bundled in.** Two correctness
  stories per ADR violates closed-catalog discipline.

## What not to do

- **Don't lift ADR 063's mask-only Phase B contract.**
  Dense predicate-zip is a separate ADR.
- **Don't extend to variable-width nullable.** Separate
  ADR.
- **Don't fork the partial-buffer materializer.** Extend
  `page_materialize.rs` in place.
- **Don't add a fifth `L2FetchStrategy` variant** or any
  new `EncodedRowMask` / fallback / reject / catalog /
  config / metric / wire / capability / digest /
  ObjectStore surface.
- **Don't introduce planning-phase identifiers, version
  suffixes, or project-name prefixes** on new symbols.
- **Don't bump `ENCODING_DIRECTORY_FORMAT_VERSION`.**
- **Don't change ADR 057 cascade catalog or ADR 063
  page-stats eligibility.**
- **Don't use page stats as a correctness dependency.**
- **Don't consume `decoded_predicate_batches`.**
- **Don't decode encoded validity buffers in V1.**
- **Don't add an all-null page-skip optimization** in
  this ADR; ADR 070 is the closure path for that deferral.
- **Don't bundle ADR 064 with FSST evaluator / RLE
  value-run / sparse predicate-column fetch / dense
  predicate-zip / variable-width / page-skip.**
- **Don't engage the row-group sparse projection planner
  on ADR 052 point-read paths**; those paths may reuse
  the validity helper only.
- **Don't leak planner decline reasons to public
  metrics, fallback enums, or reject codes.**

## Convergence summary

```text
ADR 056 row-group fetch + per-row-group stats + page directory
  → ADR 057 per-buffer pages + cascades
  → ADR 058 substrate COCD
  → ADR 059 native COCD kernels
  → ADR 060 sparse projection fetch (Phase C narrowing,
    fixed-width + non-null only)
  → ADR 061 page-aligned AEAD chunks + macro-block coalescing
  → ADR 062 row-group sizing profile (PointRead / Scan)
  → ADR 063 page-level predicate stats + Phase B narrowing
  → ADR 064 nullable sparse projection (this ADR;
    fixed-width nullable extension of ADR 060;
    raw byte-addressable validity only;
    encoded-validity deferred here and later closed by ADR 069;
    page-local bit-offset rebase; mask-first ordering
    preserved; mask-only Phase B contract preserved;
    ADR 063 composes multiplicatively but page null
    counts not used to skip validity fetches; V1 type
    catalog mirrors ADR 060's actual current catalog)
    ← here
  → dense predicate-zip with sparse projection (ADR 065)
  → next Toron-adjacent follow-ups (FSST evaluator,
    RLE value-run, sparse predicate-column fetch,
    variable-width nullable sparse projection,
    all-null page-skip optimization)
  → capacity-tier compression / serverless / EC bundle
```

## Promotion criteria

ADR 064 promotes from council-converged → shipped when:

1. Phase 0 has pinned the exact value-type and value-
   cascade catalogs and confirmed raw byte-addressable
   validity is a non-no-op V1 engagement path against
   current writer output.
2. The sparse projection planner accepts nullable fixed-
   width projected columns that otherwise pass ADR 060
   eligibility.
3. `None` masks short-circuit before nullable eligibility
   and fetch zero projection bytes.
4. `All` masks route to dense.
5. Selected validity byte ranges are included in sparse
   byte / range economics.
6. Page-local validity bitmaps re-base correctly for
   byte-aligned and non-byte-aligned page starts.
7. Sparse output is byte-identical to dense projection
   for nullable fixed-width fixtures across mixed null
   distributions.
8. Non-null sparse projection behavior and performance
   stay within ADR 060's existing envelope (byte-
   identical regression).
9. Dense predicate batches still decline sparse
   projection.
10. Variable-width and unsupported cascades still
    decline.
11. Encoded validity buffers decline with private
    unsupported reasons; detection is precise per
    `(row_group_index, null_buffer_ordinal,
    validity_body_range)`.
12. ADR 052 secondary-index materialization, if it needs
    nullable page-local assembly, imports the shared
    `page_materialize.rs` validity helpers (single-
    source guard).
13. ADR 063 nullable predicate composability fixtures
    (`IS NULL`, `IS NOT NULL`, nullable value comparison)
    drive sparse projection of nullable projected columns
    and record combined byte savings strictly larger than
    either ADR alone, with ExactMatch-driven and Maybe-
    driven projection pages recorded as separate rows.
14. Row groups with `column_null_counts == 0` for a
    nullable Arrow field continue to use the existing
    all-valid sparse path with zero validity-byte fetches.
15. Architectural lockdown guards land per §"Architectural
    lockdown".
16. Bench artifact at
    `plans/impl/064_nullable_sparse_projection_bench_results.md`
    records selected value bytes, selected validity
    bytes, combined selected plaintext bytes, ciphertext
    bytes, range counts, output equality, null-count
    equality, and wall-clock against the dense oracle.
    Promotion gates: clustered 5% ≥ 1.5× plaintext-byte
    reduction; ≥ 1.2× wall-clock at clustered 5% (or
    council-revised gate recorded); random / weak rows
    ≤ 5% dense-fallback overhead. The artifact also
    reports whether ≥ 3× plaintext-byte reduction holds
    on best-case clustered fixtures (headline diagnostic,
    not gating).
17. ADR 060 §"Where we are" amended: nullable fixed-
    width is in V1 sparse-projection eligibility set;
    encoded-validity, variable-width, page-skip, dense
    predicate-zip remain V2.
18. ADR 063 §"Where we are" amended: composition pathway
    with sparse nullable projection on nullable predicate
    columns recorded.
19. ADR 020 / 022 / 044 / 052 / 055 / 056 / 057 / 058 /
    059 / 060 / 061 / 062 / 063 focused tests stay green
    at both profiles.

## Council convergence

Iteration 6 across Claude / Codex / Gemini is the
convergence checkpoint. All open questions are resolved:

- **Validity-buffer representation (Codex / Gemini iter-1
  fork).** Resolved. All three drafts hold raw byte-
  addressable validity is the V1 cut, with encoded
  validity declining to dense behind a private planner
  reason. Phase 0 is a fork-resolution gate, not an
  implementation-time scope expansion.
- **V1 type and cascade catalog source-of-truth.**
  Resolved. Pinned to ADR 060's *current* materializer
  and planner code, not ADR 063's page-stat catalog.
- **Bench-gate calibration.** Resolved. Promotion gate is
  ≥ 1.5× plaintext-byte at clustered 5% + ≥ 1.2×
  wall-clock (council-revisable); ≥ 3× recorded as a
  headline diagnostic, not gating.
- **ADR 063 ExactMatch / Maybe asymmetry.** Resolved.
  Bench artifact records ExactMatch-driven and
  Maybe-driven projection pages as separate rows.
- **`column_null_counts == 0` for nullable Arrow field.**
  Resolved. Stays on existing all-valid sparse path with
  zero validity fetches.
- **Encoded-validity detection precision.** Resolved.
  Detection per `(row_group_index, null_buffer_ordinal,
  validity_body_range)`; one row group's encoded buffer
  cannot poison another's raw path.
- **All-null page-skip optimization.** Resolved.
  Deferred to V2 with explicit architectural guard
  against implementation-time sneak-in.

The single remaining implementation-time gate is Phase 0
audit confirming raw-only V1 is not a no-op against
current writer output. If it is, the ADR is revised
pre-code.
