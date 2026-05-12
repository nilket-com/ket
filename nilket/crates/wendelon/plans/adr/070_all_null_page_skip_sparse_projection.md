# ADR 070: All-Null Page-Skip for Nullable Sparse Projection

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/070_all_null_page_skip_sparse_projection.md`

## Status

Council-converged across Claude / Codex / Gemini after four
iterations under `plans/wip/`. Pre-implementation; Phase 0
calibration items pending (literal Rust predicates for the
architecture guard, soft-gate threshold values from
representative-fixture modeling, disposition storage
adjacency review, drift-sentinel pre-check against existing
ADR 063 fixtures).

This ADR is the tracked V2 follow-up to ADR 064's deferred
all-null page-skip and ADR 069's pinned "ADR 070 boundary"
guard. ADR 064 §"V2 deferrals" enumerated "all-null page
skip optimization deferred to V2 with explicit architectural
guard"; ADR 069 §"All-null value-page skip remains ADR 070's
scope" reinforced the boundary with a dedicated architecture
guard. ADR 070 owns the boundary now and the guard's
controlled removal.

Filename family follows ADR 064 / ADR 069 (sparse projection
follow-ups).

This ADR proposes no on-disk format change, no
`ENCODING_DIRECTORY_FORMAT_VERSION` bump, no operator-visible
configuration, and no public surface change. It introduces a
crate-private elision path that reads ADR 063 page stats at
plan time and elides both value and validity fetches for
selected pages metadata-proven all-null, plus a materializer
extension that synthesizes canonical-null Arrow buffers
locally.

## V1 contract summary

- **Metadata-only zero-RTT elision.** ADR 063 page stats are
  the sole eligibility signal. No validity-decode-driven
  classification, no two-stage fetch, no extra round trip.
  ADR 061 macro-block coalescing preserved.

- **Trigger condition.** For a selected value page k on a
  nullable fixed-width projection column whose value buffer's
  cascade × type is in the V1 catalog:

  ```text
  value_buf.page_stats[k].null_count == page_value_count[k]
  ```

  When this holds, the planner tags the page as
  `SelectedPageDisposition::AllNullElided` and emits no
  fetch — neither for the value page nor for the validity
  page.

- **Page stats are authoritative.** ADR 063 already trusts
  the same `null_count == page_value_count` for `IS NULL`
  predicate narrowing (`ExactMatch` → `EncodedRowMask::All`).
  ADR 070 inherits the trust contract; no new failure mode.
  No production verification fetch.

- **Both value and validity bytes elided.** An all-null
  page's validity bitmap is provably all zeros by the same
  page-stat proof. The materializer synthesizes both
  buffers from scratch.

- **Test-only drift sentinel.** Production never fetches
  validity to verify. Test fixtures independently decode
  raw or ADR 069 encoded validity bytes for pages where
  `null_count == page_value_count` and assert observably
  all-zero bitmaps over `page_value_count` bits. Zero
  production cost; guards the inherited ADR 063 trust
  contract.

- **Canonical-null logical oracle.** Dense fallback may
  carry arbitrary or input-derived value bytes under null
  slots that an elided page cannot reproduce. Promotion
  oracle for elided pages is canonical-null logical
  equality (zero-fill all null-slot value bytes on both
  sides, then compare byte-for-byte). Non-elided pages
  retain the byte-identical oracle.

- **Per-column independence.** Elision is keyed by
  `(row_group, column, value_buffer, page)`. One nullable
  column may elide while another fetches normally on the
  same page; non-nullable columns never elide.

- **Validity-source independence.** Elision works whether
  the column's validity is raw (ADR 064) or encoded (ADR
  069). The decision keys off the value buffer's page stat,
  not the validity-buffer encoding.

- **V1 type catalog = ADR 063 V1 ∩ ADR 060 / 064 sparse-
  projection value-column eligibility.** Fixed-width
  Int8/16/32/64, UInt8/16/32/64, Timestamp(ms/us/ns),
  Date32/64. Boolean is in ADR 063's stats catalog but not
  in ADR 060 / 064's sparse-projection value-column catalog
  (`fixed_width_value_bytes` excludes it); ADR 070 does not
  widen ADR 060 / 064's catalog. Boolean elision is V2
  alongside any future widening of sparse projection to
  bit-packed Boolean.

- **V1 cascade catalog = ADR 063's intersection.** Eligible
  value cascades are `BitPacked`, `DictionaryPlain`,
  `DictionaryBitPacked`.

- **ADR 063 page stats must be present.** If absent (writer's
  profile-emission gate declined, or ADR 063 `enabled = false`),
  no elision engages. Path stays byte-identical to ADR 064 /
  069.

- **Mask-first ordering preserved.** Survival mask decides
  selected pages; elision is a per-page post-selection
  metadata-driven optimization layered on top.

- **Per-page elision, not per-row-group.** Mixed nullity
  within a row group is the common warehouse case.

- **Closed surface.** No new public API. No new
  `EncodingCascade` / `EncodingComputeEvaluatorKind` /
  `L2FetchStrategy` / reject / fallback variant. No IPC
  `custom_metadata` key. No `ENCODING_DIRECTORY_FORMAT_VERSION`
  bump. No wire / descriptor / PBC / capability / digest /
  `ObjectStore` trait change. No Cargo feature, metric label,
  or operator-visible toggle.

- **Determinism.** Page stats are deterministic per ADR 063;
  elision is a pure function of stats + survival mask;
  `BTreeMap` planner storage provides deterministic
  iteration. Cross-replica byte-equality preserved.

## Context

ADR 064 (Nullable Sparse Projection) shipped raw byte-
addressable validity sparse projection for fixed-width
nullable columns. Its V1 deferred all-null page-skip with
an explicit architectural guard: the planner fetches every
selected value page even when that page is known to be
entirely null. ADR 069 (Encoded-Validity Sparse Projection)
shipped encoded-validity decoding and reinforced the guard:
decoded validity bits feed Arrow nullability assembly only
and cannot suppress already-planned value-page fetches.

The warehouse motivation is clustered nullity: optional
descriptor fields populated only during certain campaigns,
append-only update streams with tombstone runs, LSM-
compacted tables with nullity clustered by source segment.
ADR 069 reduced validity bytes for these pages; ADR 070
removes the much larger value bytes when a selected page is
proven all-null.

For a 65,536-row nullable `Int64` page, value bytes are
512 KiB. Validity bytes (ADR 064 raw) are 8 KiB or (ADR 069
encoded all-zeros) a few bytes. If page stats prove every
row is null, none of those bytes — value or validity — can
affect the projected logical result.

### The canonical-null oracle wrinkle

Dense fallback decodes value buffers and applies the
survival mask at materialization time. Under null slots, the
decoded value bytes are whatever the underlying cascade
produced — possibly input-derived, possibly zero. ADR 064 /
069 sparse output preserves those bytes byte-for-byte
because they fetch the value page.

An elided page cannot reproduce those bytes; the synthesized
zero-filled value buffer is canonical, not preserving. Under
Arrow null semantics the output is semantically identical
(validity=false masks value-byte content) but is not byte-
identical to dense fallback for null-slot positions.

ADR 070 formally adopts the canonical-null logical equality
oracle for elided pages. Non-elided pages retain the byte-
identical oracle as a hard regression gate.

### Why metadata-only beats validity-first probe

A two-stage path (fetch validity → decode → decide → fetch
value) adds a network round trip per row group with
candidate pages, fragments ADR 061's macro-block coalescing,
and re-introduces the latency penalty ADR 060 spent careful
effort to avoid. The marginal mixed-page-skip capability it
unlocks (mask selects only null rows in a partially-null
page) does not pay against the dominant clustered-nullity
workload, which is full-page all-null.

V1 is metadata-only. Mixed-page skip and any probe shape
are V2 deferred.

### Why elide validity too

The page stat asserts every row is null; the validity bitmap
is provably all zeros by the same stat. Fetching validity to
confirm a directory-provable property is asymmetric trust:

- ADR 063 already trusts `null_count` for `IS NULL`
  narrowing (predicate path).
- ADR 070 trusting `null_count` for value-page elision
  (projection path) is consistent.
- ADR 070 distrusting `null_count` enough to fetch validity
  for verification would create a stricter trust standard on
  the projection path than on the predicate path. Incoherent.

V1 elides both. Maximal byte and range savings, no extra
I/O, no asymmetric trust.

### Why a test-only drift sentinel

The trust contract is inherited from ADR 063, but the
specific invariant "`null_count == page_value_count` implies
all validity bits are zero" is not separately tested by
ADR 063's own oracle. ADR 063 verifies that `IS NULL`
returns the expected rows when `null_count` is full; it does
not explicitly assert the underlying validity bytes match
the metadata, because for `IS NULL` it emits
`EncodedRowMask::All` without fetching validity.

The drift sentinel is test-only: test fixtures independently
decode validity bytes and assert all-zero for elided pages.
Zero production cost; catches writer-validity drift that ADR
063's existing oracle does not explicitly assert.

## Decision

### 1. Page-stat helper

Add a crate-private helper in
`src/storage/encoding/sparse_projection_fetch.rs`:

```rust
pub(crate) fn value_page_is_metadata_all_null(
    value_buf: &BufferEncoding,
    page: EncodingPageDescriptor,
) -> bool;
```

Returns `true` iff `value_buf.page_stats[page.page_index]`
is present with `null_count == page.value_count`. Fail-
closed on every other shape: absent stats, out-of-bounds
page index, optional `null_count` missing, malformed
descriptor.

The helper does **not** check cascade × type catalog
membership: its signature does not carry `DataType` or a
resolved `EncodingCascade`. Catalog gating is the planner's
responsibility — the planner checks column nullability,
value-buffer cascade, and column `DataType` against the V1
catalog **before** invoking the helper. The
`AllNullElisionDeclineReason` diagnostic records catalog
declines at the planner site.

The helper name is intentionally explicit: it states both
the scope (value page) and the proof source (metadata).
The architecture guard text references the name; do not
shorten to a more generic helper name in implementation.

### 2. Selected-page disposition

```rust
pub(crate) enum SelectedPageDisposition {
    Materialize,
    AllNullElided {
        page_row_count: usize,
        bytes_per_value: usize,
    },
}
```

Per-page tag. `AllNullElided` carries synthesis metadata so
the materializer doesn't have to re-derive it from the page
descriptor.

Storage shape on `SparseProjectionFetchPlan`:

```rust
pub(crate) value_page_dispositions: BTreeMap<
    PageMaterializeBufferKey,
    SelectedPageDisposition,
>
```

Keyed by projected value-buffer page; deterministic
iteration order. Implementation may store `Materialize`
entries explicitly for assertion clarity or treat missing
keys as `Materialize` (elided-only map); both forms are
contract-equivalent. `data_type` is recovered from the
matching `PageMaterializeColumn` at materialization time.

For `AllNullElided`:

- No `PageMaterializeFetch` for the value page.
- No `PageMaterializeValidityFetch` for the validity page.
- No byte range in `selected_body_ranges` for either buffer.
- Materializer synthesizes both buffers locally.

### 3. Sparse economics

Elided pages contribute zero selected bytes and zero selected
ranges for both elided buffers. Non-elided pages keep ADR 064 /
069 accounting unchanged. The sparse-vs-dense byte gate
strictly improves on every fixture where elision engages.

### 4. Materializer: zero-byte synthesis

For an `AllNullElided` page, synthesize:

- `value_buffer = vec![0u8; page_row_count × bytes_per_value]`
  for fixed-width primitives (the only V1 catalog members;
  Boolean is V2).
- `validity_bitmap = vec![0u8; ceil(page_row_count / 8)]`
  with `null_count = page_row_count`.

```rust
pub(crate) fn synthesize_all_null_page_buffers(
    page_row_count: usize,
    bytes_per_value: usize,
) -> Result<(Vec<u8>, PageValidityBitmap), ArrowError>;
```

Uses checked multiplication; fails closed on overflow. No
`data_type` parameter is needed because all V1 catalog
members use the same fixed-width-primitive formula; a V2
extension to bit-packed Boolean would add the parameter
then. Both
buffers feed the existing
`projected_page_batch_from_decoded_buffers` callsite. Page-
local survival-mask filter unchanged. Surviving rows in
elided pages contribute nulls with validity=false and
canonical-zero value-byte slots.

```rust
pub(crate) enum PageMaterializeValueSource {
    Decoded,
    AllNullSynthesized,
}
```

Internal value-source dispatch enum, `pub(crate)`.

### 5. Architecture guard relocation

ADR 069's guard:

> "No code path inspects decoded validity to suppress an
> already-planned value-page fetch — ADR 070 boundary."

is replaced by ADR 070's five-part invariant set in
`tests/architecture_test.rs`:

1. **Metadata-only elision.** Both value-page and validity-
   page fetches may be elided only when the page's
   `SelectedPageDisposition` is `AllNullElided`, reachable
   only via `value_page_is_metadata_all_null` (the page-stat
   helper).
2. **No decoded-bit elision.** No code path inspects decoded
   validity bits, decoded value bytes, or any post-fetch
   quantity to decide page elision.
3. **Per-column scope.** Elision is keyed by `(row_group,
   column, value_buffer, page)`; one column's elision does
   not affect another's fetch.
4. **Non-nullable forbidden.** `AllNullElided` /
   `AllNullSynthesized` never fires for a non-nullable
   column.
5. **Synthesis shape.** Synthesized value buffer length is
   exactly `page_row_count × bytes_per_value`; synthesized
   validity bitmap length is `ceil(page_row_count / 8)`;
   both are zero bytes.

Literal Rust predicates and callsite assertions are
finalized in Phase 0.

### 6. Private diagnostic

```rust
pub(crate) enum AllNullElisionDeclineReason {
    PageStatsAbsent,
    NullCountBelowFull,
    TypeOutOfCatalog,
    CascadeOutOfCatalog,
    ColumnNotNullable,
}
```

Crate-private; diagnostic-only for bench artifact rows and
focused tests. Production behavior collapses every decline to
the existing normal sparse projection path (no public
surface, no fallback variant, no metric label).

### 7. Composability

- **ADR 060** — sparse projection still computes selected
  pages from the survival mask. Elision is a per-page
  bytes-saved optimization layered on top.
- **ADR 063** — ADR 070 is a consumer of ADR 063's existing
  `null_count` field. ADR 063 narrows predicate bytes; ADR
  070 narrows projection bytes (value + validity) for the
  full-page all-null case.
- **ADR 064** — ADR 070 lifts the all-null deferral guard.
  ADR 064's `column_null_counts == 0` row-group-grain short-
  circuit is preserved (no overlap with per-page all-null
  elision; ADR 064 elides validity-fetch entirely for all-
  valid row groups, ADR 070 elides value+validity per-page
  for proven all-null pages).
- **ADR 065** — manager-side three-case dispatch unchanged.
  Case B (predicate / output overlap) zips dense predicate
  batches with sparse projection batches; elided projection
  pages contribute canonical-null synthesized buffers.
- **ADR 069** — encoded-validity decode unchanged for non-
  elided pages. For elided pages, ADR 069's RLE validity
  range is omitted from the fetch plan.

### 8. ADR 063 enablement dependency

ADR 063 carries `enabled = false` writer default. ADR 070
V1 is operationally inert in current production until ADR
063 is enabled deployment-wide. The bench artifact records:

- ADR 063 disabled (control): byte-identical to ADR 064 /
  069.
- ADR 063 enabled (active): elision engages on eligible
  pages.

Capability lands; engagement follows ADR 063's separate
operational rollout. This decoupling is deliberate.

## Rationale

### Why no writer change

ADR 063 already records `null_count` per page on the value
buffer. Adding a new field would duplicate the source of
truth, force an `ENCODING_DIRECTORY_FORMAT_VERSION` bump,
and re-litigate ADR 063's profile-emission gate. Consume
the existing field.

### Why page stats over decoded bits

Three convergent reasons:

1. **Closed-system inheritance.** Trust contract already
   load-bearing in ADR 063 for `IS NULL` narrowing.
2. **Zero RTT.** Decoded-bit classification requires
   fetching validity first, blocking value-fetch coalescing;
   ADR 061 macro-block economics depend on wide multi-range
   GETs.
3. **V2 capability cap.** Mixed-page skip is the only
   capability decoded-bit classification unlocks beyond
   metadata. The dominant warehouse workload is full-page
   all-null.

### Why elide validity too

The page stat asserts every row is null; the validity bitmap
is provably all zeros by the same stat. ADR 063 already
trusts the field for `IS NULL`; fetching validity to verify
the same field for projection would create asymmetric trust
between predicate and projection paths.

### Why canonical-null logical oracle

Dense fallback's null-slot bytes are not semantically
observable but are byte-distinct from synthesized zero-
filled slots. Canonical-null oracle matches Arrow semantics
without over-claiming preserved-byte fidelity.

### Why per-page, not per-row-group

Mixed nullity within a row group is the common warehouse
case. Per-row-group elision is strictly weaker.

### Why the V1 catalog is ADR 063 ∩ ADR 060 / 064 sparse-projection eligibility

Elision needs two things to land: directory-time
`null_count` evidence (ADR 063 page stats) **and** a sparse-
projection materialization path for the column type
(`fixed_width_value_bytes` in ADR 060 / 064). The
intersection is `{BitPacked, DictionaryPlain,
DictionaryBitPacked}` × `{Int8-64, UInt8-64,
Timestamp(ms/us/ns), Date32/64}`. Boolean is in ADR 063's
stats catalog but excluded by `fixed_width_value_bytes`;
V1 inherits the exclusion. V2 widening is split: cascade /
type widening on the page-stats side is ADR 063's
territory, and Boolean / variable-width widening on the
sparse-projection side is ADR 060 / 064's territory.

## Consequences

- **One new helper** (`value_page_is_metadata_all_null`)
  reading existing ADR 063 field.
- **One new disposition enum**
  (`SelectedPageDisposition::AllNullElided`) at page grain.
- **One new value-source enum**
  (`PageMaterializeValueSource::AllNullSynthesized`) in
  materializer dispatch.
- **One new planner field**
  (`SparseProjectionFetchPlan.value_page_dispositions:
  BTreeMap<...>`).
- **Materializer extension**: synthesizes both zero value
  and zero validity buffers locally.
- **Architecture guard relocation**: ADR 069's boundary
  guard rewritten to ADR 070's five-part invariant set.
- **Canonical-null logical oracle**: promotion oracle for
  elided pages; byte-identical oracle for non-elided.
- **Test-only drift sentinel**: fixture-side validity-byte
  audit for elided pages.
- **One new private diagnostic**
  (`AllNullElisionDeclineReason`).
- **No format-version bump**, **no new on-disk shape**,
  **no public surface change**, **no writer change**.
- **Conditional on ADR 063 enablement**: V1 is
  operationally inert until ADR 063 is enabled deployment-
  wide.

## Phase 0 Pins

- **P-fork (oracle contract).** Council formally adopts
  canonical-null logical equality for elided pages; byte-
  identical retained for non-elided. P2 audit (below)
  builds a fixture demonstrating dense fallback carries
  non-zero bytes under null slots, closing the contingency.
- **P-fork (writer scope).** Confirm ADR 063 page stats
  emit per-page `null_count` on projection-only value
  buffers. Expected: yes (ADR 063 is per-buffer, not
  per-role).
- **P1: Eligibility catalog.** V1 cascade × type
  intersection: {`BitPacked`, `DictionaryPlain`,
  `DictionaryBitPacked`} × {Int8-64, UInt8-64,
  Timestamp(ms/us/ns), Date32/64}. Boolean is in ADR 063's
  stats catalog but excluded by ADR 060 / 064's
  `fixed_width_value_bytes` (sparse projection value-column
  eligibility); V1 inherits that exclusion. Boolean
  elision is V2 alongside any future widening of sparse
  projection to bit-packed Boolean.
- **P2: Dense null-slot audit.** Build a nullable fixed-
  width fixture whose null slots carry non-zero value
  bytes; prove dense fallback preserves them. Formally
  justifies canonical-null oracle adoption.
- **P3: Materializer zero-byte synthesis cost.**
  Microbenchmark allocation vs. small-page fetch.
  Expected: synthesis is negligible (microseconds); fetch
  dominates (milliseconds with RTT).
- **P4: ADR 060 multi-range coalescing.** Confirm elided
  pages register zero ranges for both buffers and the
  sparse-vs-dense byte gate counts elided pages as zero
  selected bytes. Record whether range / GET counts change
  after elision; P12 / soft gate #19 acknowledge that
  removing a middle range can split a coalesced run.
- **P5: Cross-replica determinism probe.** Two-process
  elision on identical clustered-all-null fixtures; byte-
  identical fetch plans and canonical-null-normalized
  Arrow output.
- **P6: Architecture-guard literal text.** Draft exact
  Rust predicates / callsite assertions for the five-part
  invariant set.
- **P7: Fixture identification.** Clustered all-null on
  `Int64`; alternating-pages-all-null; clustered-5%-null
  (re-use ADR 064 / 069); random-50% (no elision); two-
  nullable-column divergent.
- **P8: Bench home.** `benches/row_group_fetch.rs` as
  headline (read-scheduling axis is the primary outcome
  for ADR 070). Optional `encoding_compute.rs` spot-check
  rows for ADR 069 continuity.
- **P9: Disposition storage shape.** Confirm
  `BTreeMap<PageMaterializeBufferKey,
  SelectedPageDisposition>` fits the existing planner;
  elided-only map style is acceptable if guard text
  remains semantic.
- **P10: ADR 063 enablement.** Confirm bench harness
  enables / disables ADR 063 page stats; preserve disabled
  control rows.
- **P11: ADR 065 Case B handoff.** Confirm manager-side
  zip operates on synthesized canonical-null buffers
  without additional plumbing.
- **P12: Range / GET count behavior under elision.** ADR
  060's existing range coalescer
  (`sparse_projection_fetch.rs`) and ADR 061's chunk
  coalescer (`crypto.rs`) merge only overlapping or
  adjacent ranges. Removing a middle elided range can
  therefore split one coalesced run into two, producing a
  GET-count regression even when total bytes drop. V1
  records range / GET counts on every bench row and
  reports regressions explicitly; promotion does not gate
  on GET count never increasing. A future ADR may
  introduce bridge-overfetch logic to retain coalescing
  when GET count would regress; V1 keeps the planner
  simple.
- **P13: Per-column independence audit.** Two-nullable-
  column divergent fixture as audit.
- **P14: Test-only drift sentinel pre-check.** Pre-
  implementation, audit existing ADR 063 fixtures: for
  each page tagged `null_count == page_value_count`,
  independently decode the underlying raw or ADR 069
  encoded validity bytes (test-side, not production) and
  assert observably all-zero over `page_value_count`
  bits. If the writer invariant fails on any existing
  fixture, fix the writer before ADR 070 lands.
- **P15: Soft-gate threshold calibration.** Replace
  placeholder thresholds (2.0× / 1.5× / 5% / 55% / 1% /
  2%) with values derived from representative-fixture
  modeling.

## Promotion criteria

Bench artifact at
`plans/impl/070_all_null_page_skip_sparse_projection_bench_results.md`
records the following gates. Hard gates are correctness /
contract; soft gates are performance targets the artifact
can fail and still promote with explicit recorded
justification.

### Hard gates

1. **Canonical-null logical oracle equality.** Every
   fixture matches dense fallback after canonical-null
   normalization for elided pages.
2. **Byte-identical oracle for non-elided pages.** Pages
   not tagged `AllNullElided` produce byte-identical Arrow
   output and byte-identical fetch plans vs. ADR 064 / 069
   baseline.
3. **No value-page fetch for elided pages.** Bench artifact
   asserts; architecture guard enforces.
4. **No validity-page fetch for elided pages.** Holds for
   both ADR 064 raw and ADR 069 encoded validity sources.
5. **No decoded-bit elision.** Architecture guard forbids
   any code path from inspecting decoded validity bits or
   decoded value bytes to decide page elision.
6. **Per-column independence.** Two nullable columns on
   the same page may make different elision decisions;
   tested.
7. **Non-nullable forbidden.** Elision never fires for
   non-nullable columns; architecture guard enforces.
8. **ADR 063 disabled = no elision.** With page stats
   absent, plan and output byte-identical to ADR 064 / 069.
9. **Mixed row group.** A row group with both all-null and
   live pages elides only the all-null pages.
10. **Cross-replica determinism.** Two-process runs produce
    byte-identical fetch plans and canonical-null-
    normalized output.
11. **Drift sentinel.** Test fixtures with metadata-all-
    null tagged pages have observably all-zero underlying
    validity bytes (raw and encoded).
12. **Closed-catalog and closed-surface lockdown.** No new
    public surface; cascade × type catalog intersects ADR
    063 V1; compile-time exhaustion at the elision-
    eligibility site.

### Soft gates

Threshold values are Phase 0 P15 calibration targets;
current placeholders below. The artifact may fail soft
gates and still promote with recorded justification (per
ADR 067 precedent on writer-cost waiver).

13. **Elided byte fraction.** Clustered-all-null fixtures:
    selected value+validity bytes ≤ 5% of ADR 064 / 069
    baseline for the elided nullable column.
14. **Mixed-page byte reduction.** 50% all-null / 50% live
    pages: selected bytes ≤ 55% of baseline.
15. **Wall-clock speedup.** ≥ 2.0× on clustered-all-null
    sparse projection over ADR 064 / 069; ≥ 1.5× on 50/50
    mixed.
16. **Synthesis cost bounded.** Zero-byte synthesis wall-
    clock per elided page ≤ 1% of baseline fetch + decode
    wall-clock for the same page shape.
17. **Cross-validity-source parity.** Elision works
    logically-identically across ADR 064 raw and ADR 069
    encoded.
18. **No-elision controls.** Fixtures with no elision-
    eligible pages run within 2% wall-clock of ADR 064 /
    069 baseline.
19. **Range / GET count recorded.** Bench artifact
    records range count and macro-block GET count
    baseline vs. elision for every row. ADR 060 / 061
    coalescers merge only overlapping/adjacent ranges, so
    removing a middle range can split one coalesced run
    into two; promotion does not gate on GET count never
    increasing. Any regression is reported alongside the
    byte reduction that motivated it, and the artifact
    explicitly notes whether bridge-overfetch logic
    (V2 territory) would have preserved coalescing.

The Phase 6 promotion evidence will record hard-gate
pass/fail and soft-gate observations. If a hard gate
fails, the council decides among three explicit paths:
revise V1 (narrow eligibility, tighten oracle, fix
determinism source); scope down (ship helper + synthesis
+ architecture-guard relocation as plumbing only, defer
materializer integration to a separate ADR); shelve
writer side (keep ADR 064 / 069 unchanged, reopen when
ADR 063 ships broader page-stat coverage).

## Deferred (V2)

- **Mixed-page skip** via decoded validity + survival-mask
  intersection. Requires two-stage path or validity-decode-
  driven classification; ADR 061 fragmentation cost must
  be weighed against capability win.
- **Boolean elision.** Requires widening ADR 060 / 064's
  sparse-projection value-column catalog to bit-packed
  Boolean (currently excluded by
  `fixed_width_value_bytes`). Carries an Arrow
  `BooleanArray` value-buffer shape change in the
  synthesis helper.
- **Variable-width nullable sparse projection** (ADR 064
  V2 boundary).
- **Float / Time types** (ADR 063 V2 boundary).
- **Page stats on additional cascades** (`Rle`, `Delta`,
  `FsstRowSafe`, `Alp`). ADR 063 V2 territory.
- **Predicate-side null-only shortcuts** for aggregate
  folds. ADR 058 / 059 territory.
- **Operator-visible elision toggle**, public decline /
  fallback / metric labels.

## What Not To Do

- Do not add a new on-disk field for "is all-null page";
  ADR 063 already carries `null_count`.
- Do not promise byte-identical output to dense fallback
  for null-slot value bytes on elided pages; canonical-
  null oracle is the contract.
- Do not introduce a two-stage validity-first fetch path
  in V1; ADR 061 macro-block economics forbid.
- Do not classify mixed pages via decoded validity bits in
  V1; metadata-only.
- Do not fetch validity in production to verify the page
  stat; the drift sentinel is test-only.
- Do not elide pages with `0 < null_count <
  page_value_count`; V2 territory.
- Do not extend the V1 cascade / type catalog beyond
  ADR 063 V1 ∩ ADR 060 / 064 sparse-projection
  eligibility.
- Do not include Boolean in V1; ADR 060 / 064's
  `fixed_width_value_bytes` excludes it, and ADR 064 §"V1
  type and cascade catalog" forbids ADR 070 from widening
  the sparse-projection value-column catalog.
- Do not widen sparse projection to variable-width in
  ADR 070; ADR 064 V2 boundary.
- Do not gate promotion on GET count never increasing.
  ADR 060 / 061 coalescers merge only overlapping or
  adjacent ranges; eliding a middle range can split a
  coalesced run. Record GET count baseline vs. elision
  per bench row and justify any regression with the
  accompanying byte reduction.
- Do not add bridge-overfetch logic to retain coalescing
  in V1; that is V2 territory if evidence shows the GET-
  count regression material.
- Do not introduce an `L2FetchStrategy` variant or fork
  `sparse_projection_fetch.rs`.
- Do not expose `SelectedPageDisposition`,
  `PageMaterializeValueSource`,
  `value_page_is_metadata_all_null`,
  `value_page_dispositions`, or
  `AllNullElisionDeclineReason` through public API.
- Do not couple ADR 070 promotion to ADR 063 production
  enablement; capability lands, engagement follows.
- Do not shorten the helper name
  `value_page_is_metadata_all_null`; the explicit scope
  and proof source are load-bearing for the architecture
  guard.

## Council decisions

Compact archival summary. Iteration history lives in git
on `plans/wip/`.

1. **Metadata-only V1.** Zero-RTT, page-stat-keyed
   elision. No probe path. Settled iter-1 on the joint
   ADR 061 fragmentation + ADR 060 latency arguments.
2. **Canonical-null logical oracle.** Promotion oracle for
   elided pages; byte-identical retained for non-elided.
   Codex raised iter-0; settled iter-2 with the P2 audit
   commitment.
3. **Elide both value and validity.** Closed-system
   inheritance from ADR 063 `IS NULL` predicate trust.
   Settled iter-2 across all three drafts.
4. **`SelectedPageDisposition::AllNullElided` at page
   grain.** Per-page tag carrying `page_row_count` and
   `bytes_per_value` synthesis metadata. Settled iter-2.
5. **`BTreeMap<PageMaterializeBufferKey,
   SelectedPageDisposition>` planner storage.**
   Deterministic iteration order. Codex pinned iter-4.
6. **Page stats as correctness proof.** Symmetric trust
   with ADR 063 `IS NULL` narrowing. Settled iter-1;
   reinforced iter-2.
7. **Per-column independence.** `(row_group, column,
   value_buffer, page)` key discipline. Codex raised
   iter-0; settled iter-1.
8. **V1 catalog = ADR 063 V1 ∩ ADR 060 / 064 sparse-
   projection value-column eligibility.** {`BitPacked`,
   `DictionaryPlain`, `DictionaryBitPacked`} × {Int8-64,
   UInt8-64, Timestamp(ms/us/ns), Date32/64}. Boolean is
   in ADR 063's stats catalog but excluded by ADR 060 /
   064's `fixed_width_value_bytes`; V1 inherits the
   exclusion. Settled iter-1; Boolean exclusion clarified
   post-merge.
9. **ADR 063 enablement coupling.** Capability lands;
   engagement follows. Settled iter-1.
10. **Five-part architecture-guard relocation.** Replacing
    ADR 069's boundary guard with a narrower invariant
    set. Settled iter-2; literal Rust predicates in Phase
    0 P6.
11. **Test-only drift sentinel.** Hard gate; fixture-side
    validity-byte audit. Codex iter-2 P10 idea adopted
    iter-3 as the resolution to Claude iter-1's belt-and-
    suspenders concern.
12. **`row_group_fetch.rs` as headline bench home.**
    Read-scheduling axis is the primary outcome for ADR
    070. Codex iter-1 preference; settled iter-3.
13. **No writer change**, **closed surface**, **no
    format-version bump**, **deterministic across
    replicas**. Settled across all iterations.
