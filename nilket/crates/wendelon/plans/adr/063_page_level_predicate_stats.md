# ADR 063: Page-Level Predicate Stats + Predicate-Fetch Narrowing

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/063_page_level_predicate_stats.md`

## Status

Shipped 2026-05-09 after implementation, full `cargo test`,
`cargo check --benches`, and quick bench evidence landed
together. Bench evidence is recorded at
`plans/impl/063_page_level_predicate_stats_bench_results.md`.
The Phase 0 profile-emission gate kept both `PointRead` and
`Scan` in V1: quick clustered 5% predicate bytes drop
`9,153 -> 144` for `Scan` (63.56x) and `1,145 -> 144` for
`PointRead` (7.95x), while combined metadata stays inside
ADR 044's 64 KiB initial-probe envelope.

The shipped design is the council-converged result after five
iterations across Claude / Codex / Gemini, with post-merge
corrections from Codex's docs/design review (type-catalog
narrowing to ADR 056's actual fixed-width support; cascade
eligibility narrowed to the intersection of ADR 059's
allow-list AND `EncodingCascade::emits_pages_in_v1()`,
dropping `Delta` from V1 emission; profile-emission decision
moved entirely into Phase 0).

Amends ADR 057 (Per-Buffer Pages and Encoding Cascades) with
an additive `BufferEncoding.page_stats` field and an
`ENCODING_DIRECTORY_FORMAT_VERSION` bump (no compat decoder).
Adds an internal predicate-page fetch planner inside
`L2FetchStrategy::RowGroup`'s Phase B that narrows encoded
predicate fetch to surviving pages before survival-mask
construction. Composes with ADR 058 / 059 / 060 / 061 / 062
unchanged.

A standalone ADR rather than an inline ADR 057 amendment
because page-level stats introduce a new closed `EncodingPageStats`
shape, a three-state predicate-page classification with
distinct null-position correctness rules, a new internal
predicate-page fetch planner module, and its own bench-driven
promotion gates and architectural lockdown guards.

## Context

ADR 056 ships per-row-group min/max/null stats in
`RecordPageDirectory.RowGroupColumnStats`. Phase B of the
three-phase fetch planner uses them to prune row groups before
fetching predicate columns. That works at row-group grain.

ADR 062's `Scan` profile drives row groups to 65,536 rows.
A 65,536-row group with ADR 057's
`target_simd_page_values = 1_024` carries ~64 pages per buffer.
Row-group-grain stats prune the *whole* group or none of it;
they cannot prune individual pages within a surviving group.
Phase B therefore fetches whole row-group buffers for predicate
columns when row-group stats are insufficient — typically the
common case for clustered or selective predicates, where the
row group is "mostly out" but the row-group stats range still
intersects the predicate.

ADR 058 / 059 then evaluate the predicate against the full
buffer, producing a survival mask. ADR 060 uses the mask to
narrow *projection* fetch via sparse page selection. The
predicate axis has no equivalent narrowing — Phase B pays full
buffer fetch cost regardless of which pages would prove
disjoint or all-matching at finer grain.

### What's missing

A page-grain pruning surface so Phase B can fetch only the
predicate-column pages whose stats prove the page might
intersect the predicate, rather than the whole row-group
buffer. Three forces converge:

**1. ADR 062 `Scan` makes pages-per-buffer large enough to
matter.** `Scan` rows × pages-per-buffer ≈ 64; at 0.05–0.10
selectivity on a clustered key, ~6–12 of those 64 pages survive
stats prune. Page-grain narrowing is a 5–10× predicate-fetch
reduction on the geometry where it matters. `PointRead` has 4
pages per buffer; the win is smaller but still positive.

**2. ADR 060 already proved the page-level fetch shape.** The
sparse projection planner (`src/storage/encoding/sparse_projection_fetch.rs`)
converts a survival mask into a per-buffer selected-page set
and issues multi-range fetches. The same machinery applies one
phase upstream: convert page-stats survival into a per-buffer
selected-page set for predicate columns.

**3. ADR 044 / 061 already provide the chunk-grain transport.**
The `TieredMultiRangeReadPlan` coalesces contiguous chunk-
aligned ranges into multi-MiB GETs. Page-narrowed predicate
fetch produces multi-range plans that the existing coalescer
collapses into the same GET-economy shape ADR 061 ships.

### Why this is the first directory-shape extension

ADR 057 §"Don't extend ADR 057's directory for COCD" forbade
extension *for COCD*; ADR 058 read the existing
`BufferEncoding` shape. ADR 060 §"Don't extend ADR 057's
directory for ADR 060" similarly forbade extension *for
ADR 060*; per-page row counts already lived in
`page_value_count`. ADR 063 is the first consumer that
genuinely needs metadata not derivable from existing fields:
per-page min/max/null counts cannot be computed without
scanning the encoded bytes, which is exactly what predicate-
fetch narrowing exists to avoid. The exception is principled
and bench-defended; it does not open the directory to
arbitrary future extensions.

The codebase is pre-launch; ADR 057's `EncodingDirectory` byte
layout is *not* a deployed format contract. Per
`plans/conventions/no-premature-backward-compat.md`, the
extension bumps `ENCODING_DIRECTORY_FORMAT_VERSION` and the
decoder accepts only the new format. No compat struct, no
tolerant decoder, no `Option<>` hedge, no
`EncodingDirectoryV2` suffix type. Existing pre-063 slabs
recompact through the new shape via ADR 055 LSM compaction's
out-of-place sidecar rebuild on natural recompacts.

### Constraints carry forward unchanged

ADR 003, 016, 019, 020, 022, 029, 030, 035, 036, 037, 039, 040,
042, 043, 044, 052, 053, 054, 055, 056, 058, 059, 060, 061, 062.
ADR 057 directory shape is amended (additive `page_stats` field
on `BufferEncoding`, format-version bump, strict decode).

## Decision

ADR 063 ships:

1. **`EncodingPageStats` per-page struct, attached as
   `Vec<EncodingPageStats>` field on `BufferEncoding`.** Length
   is either zero (no page stats for this buffer) or exactly
   `page_end_offsets.len()`. One struct per page, parallel to
   the existing per-buffer page geometry.

2. **`ENCODING_DIRECTORY_FORMAT_VERSION` bump.** No compat
   decoder. No `Option<>` field hedge. No
   `EncodingDirectoryV2` type. Existing pre-063 slabs are
   handled by ADR 055 LSM compaction's natural recompacts.

3. **Three-state page classification: `Disjoint` /
   `ExactMatch` / `Maybe`.**
   - `Disjoint` (stats prove predicate false): emit
     `EncodedRowMask::None { rows }` slice; no fetch, no
     evaluation.
   - `ExactMatch` (stats prove predicate true for every row in
     the page): emit `EncodedRowMask::All { rows }` slice; no
     fetch, no evaluation.
   - `Maybe`: fetch the page's encoded byte range; COCD
     evaluates and produces a per-page kernel mask.

   **ExactMatch correctness is conservative.** For value
   comparisons (equality, range, inequality), `ExactMatch`
   requires both `null_count == 0` AND min/max prove every
   value satisfies the predicate. If `null_count > 0`, the
   page is `Maybe` — even if min/max prove every non-null
   value matches, we'd still need null-position bytes to
   build the correct mask, which defeats the optimization.
   For `IS NULL`, `ExactMatch` requires
   `null_count == page_value_count`. For `IS NOT NULL`,
   `ExactMatch` requires `null_count == 0`. The per-row-group
   `EncodedRowMask` is composed by stitching the per-page
   slices in row order; ADR 058's existing
   `EncodedRowMask::{All, None, Bitmap, Runs}` algebra
   absorbs the composition without a new variant.

4. **Type catalog narrows to the fixed-width subset of
   ADR 056's `default_stats_supported_types`.** ADR 056's
   row-group `stats_supported_types` covers (per
   `src/storage/row_group.rs:487`): Int8/16/32/64,
   UInt8/16/32/64, Timestamp(ms/us/ns) (timezone-naïve),
   Date32, Date64, Boolean, Utf8, LargeUtf8, Binary,
   LargeBinary. ADR 063's page-grain V1 takes the
   fixed-width subset only: **Int8/16/32/64,
   UInt8/16/32/64, Timestamp(ms/us/ns), Date32/64,
   Boolean**. Float types are not in ADR 056's row-group
   support and are out of scope here; expanding the
   stats-type catalog (floats, decimals, generic `Time`
   types) is a separate stats-type ADR. UTF-8 / Binary /
   LargeUtf8 / LargeBinary continue to receive row-group
   stats (ADR 056 unchanged) but do *not* receive page
   stats in V1; their `page_stats` is empty. Page-level
   variable-width predicate narrowing needs separate
   value-offset, dictionary, and FSST semantics that are
   V2 follow-up territory.

5. **Stats grain tied to value-buffer page boundaries.**
   Per-page row count is `page_value_count` (already in
   directory). Validity buffers carry empty `page_stats`;
   null counts attach to the corresponding value buffer's
   per-page entry.

6. **Cascade eligibility = ADR 059 default predicate
   allow-list ∩ ADR 057 page-emitting cascades.** Both
   conditions are required jointly; neither suffices alone.
   ADR 059's default predicate allow-list is `BitPacked`,
   `DictionaryPlain`, `DictionaryBitPacked`, `Delta`.
   ADR 057's `EncodingCascade::emits_pages_in_v1()` returns
   true for `BitPacked | Rle | Dictionary`. The intersection
   is **three cascades**: `BitPacked`, `DictionaryPlain`,
   `DictionaryBitPacked`. Page stats are emitted only for
   value buffers whose cascade is in this intersection AND
   whose buffer carries non-empty `page_end_offsets`.
   Cascades excluded by V1:
   - `Delta` is allow-listed but does not emit pages today
     (validation at `src/storage/encoding/directory.rs:869`
     enforces empty `page_end_offsets` for non-page-emitting
     cascades). When a future ADR amends `Delta` to emit
     pages, ADR 063's intersection automatically includes
     it; ADR 063 itself does not amend `Delta`'s page
     emission.
   - `Rle` (byte-run) is page-emitting but excluded from
     ADR 059's default allow-list (Phase 10 evidence missed
     the 2× gate). RLE byte-run carries empty `page_stats`.
     A future RLE value-run ADR may add an allow-listed
     evaluator and inherit page-stat emission.
   - `Plain`, `Lz4Frame`, `Fsst`, `Alp` are neither
     allow-listed nor page-emitting; they carry empty
     `page_stats`.

   **Writer eligibility ≠ reader narrowing.** The writer
   attaches stats to any buffer that meets the eligibility
   gate (intersection cascade + non-empty page geometry +
   fixed-width type + `stats_supported_types`). Reader-side
   narrowing applies additional plan-time requirements:
   COCD-compatible evaluator coverage for the specific
   predicate atom, compatible page geometry across predicate
   columns, and strict economic engagement
   (`selected_predicate_plaintext_bytes <
   dense_predicate_plaintext_bytes`). A buffer can carry
   page stats and still see its predicate planner decline
   into the existing dense path; that is not a writer
   misconfiguration.

7. **Page stats activate on existing toggles.** Engagement is
   determined by the existing pair (row-group writer enabled
   AND encoding writer enabled) plus stats-supported type plus
   eligible cascade. No new operator config field.

8. **Shared canonical-scalar encoding for row-group and page
   stats.** Row-group `CanonicalScalar` helpers move to
   `src/storage/stats.rs` and are reused by page stats. ADR
   063 must not fork a second stat-encoding path. Architectural
   test guards single-path use.

9. **Predicate-page planner is an internal Phase B sub-step.**
   No new `L2FetchStrategy` variant. The planner lives at
   `src/storage/encoding/predicate_page_fetch.rs` (sibling to
   ADR 060's `sparse_projection_fetch.rs`); production
   row-group read path consults it before issuing dense
   predicate fetch. Decline reasons live inside the module
   (matching ADR 060's internal-decline discipline).

10. **Engagement gate**: page-narrowed plan engages only when
    strictly cheaper than dense predicate-buffer fetch:
    `selected_predicate_plaintext_bytes < dense_predicate_plaintext_bytes`.
    Existing `L2RowGroupFetchConfig.max_ranges_per_record`,
    `L2RowGroupFetchConfig.max_selected_byte_fraction_micros`,
    and `L2EncodingFetchConfig.max_simd_pages_per_record` caps
    apply. ADR 044 + ADR 061 macro-block coalescing absorbs
    fragmentation downstream.

11. **Phase B output is mask-only on the narrowed path.**
    `PhaseBOutcome` carries the composed survival mask; no
    dense predicate batches. ADR 060 sparse projection fetch
    can engage downstream because the mask is exact.
    Dense-predicate-zip with sparse projection remains an
    explicit V1 deferral.

12. **Phase 0 metadata size-model gate is the sole V1
    discipline.** Pre-code modeling on representative
    fixtures must show combined
    `storage.row_group_directory + storage.encoding_directory`
    bytes fit within ADR 044's 64 KiB initial-probe
    envelope, with page-stats fraction ≤ 4% of record body.
    If representative legal fixtures miss the model, this
    ADR is revised before code lands (candidate revisions:
    narrower fixed-width type set, pre-code `Scan`-only
    profile pinning, further cascade-eligibility narrowing).
    The Phase 0 decision may pin a static profile-keyed
    eligibility predicate (e.g., `Scan`-only emission); that
    is a static eligibility gate built into the writer, not
    a runtime fallback. V1 does **not** add a runtime
    per-record budget fallback, runtime profile-keyed
    omission rule, partial per-column heuristic, operator
    knob, metric label, or public fallback variant beyond
    the Phase-0-pinned scope. Eligible writers emit page
    stats deterministically; promotion benches record actual
    directory sizes against the modeled envelope. Wendelon
    V1 house style is gate-driven, not runtime-fallback-
    driven (ADR 062 deterministic profile emission, ADR 060
    plan-time decline).

13. **No new `RowGroupRejectCode` / `RowGroupFallbackReason`
    / `EncodingRejectCode` / `EncodingFallbackReason` /
    `EncodingComputeRejectCode` /
    `EncodingComputeFallbackReason` / `L2FetchStrategy` /
    `EncodedRowMask` / `EncodingCascadeKind` /
    `EncodingComputeEvaluatorKind` / `EncodedAggregateKind` /
    `PredicateAtomOp` variant.** Page-stats parse failure
    routes to existing `EncodingDirectoryParseError`; range-
    overflow on page byte bounds routes to existing
    `PageRangeOverflow`. Closed catalogs unchanged.

14. **No new wire `MsgType` / `FRAME_FLAG_*` /
    `NodeCapability` bit / IPC `custom_metadata` key / digest
    domain / `ObjectStore` trait method / PBC opcode /
    descriptor field / metric label.** Existing
    `storage_l2_row_group_fetch_*` metric family absorbs
    narrowed-fetch decisions through existing `phase_b` byte
    and selected-ranges counters.

15. **Cross-replica byte-equality preserved.** Stats are
    deterministic over (input record, page boundaries,
    `stats_supported_types`, cascade eligibility, profile
    scope). Eligibility is purely a static function of the
    V1 chain (fixed-width type set ∩ non-empty page geometry
    ∩ (ADR 059 allow-list ∩ `emits_pages_in_v1()`) ∩
    Phase-0-pinned profile scope); there are no runtime
    branches that could vary across replicas. Two writers
    with the same config + same input produce byte-identical
    `EncodingPageStats`. ADR 055 LSM compaction recompute
    discipline carries forward.

16. **`enabled = false` writer default carried forward from
    ADR 056.** Page stats ship as part of the row-group writer
    surface; flipping the row-group writer default-on remains
    a separate operational ADR.

The slab-format byte layout, AAD shape, nonce derivation, key
hierarchy, footer HMAC scope, ADR 022 footer layout, ADR 056
directory shape, ADR 060 sparse projection planner, ADR 061
closed chunk-size set, and ADR 062 sizing profile catalog are
all unchanged.

## Architectural inextricability

Four design choices physically inseparable in V1.

**1. Page stats live on `BufferEncoding`, format-version
bumped.** Page stats are defined over
`BufferEncoding.page_end_offsets`; placing them in
`RecordPageDirectory` would force the outer row-group
directory to depend on inner encoded-page geometry and
duplicate page metadata across two directories. A second IPC
custom_metadata key would create a cross-directory consistency
surface. The format-version bump avoids the no-premature-
backward-compat anti-pattern.

**2. Page stats and predicate-fetch narrowing ship together.**
Page stats alone add metadata weight without a reader win.
Predicate-fetch narrowing alone has no selectivity signal
before the survival mask exists. Both ship in V1 because
together they prove one architectural claim: page-level
stats reduce predicate bytes before mask construction.

**3. V1 stays mask-only.** The narrowed predicate path returns
exact survival masks, not dense predicate batches. This
preserves ADR 060's sparse projection path. Dense predicate-
zip with sparse projection has a separate row-alignment
correctness story and is V1-deferred.

**4. ExactMatch short-circuit is conservative.** If stats
prove a page fully matches without requiring null-position
information, fetching predicate bytes would waste both I/O
and CPU. If null positions or unsupported predicate semantics
would be required to build the correct mask, the page is
`Maybe`, not `ExactMatch`. This keeps the mask oracle simple
and byte-identical and pins the correctness rule for nullable
columns explicitly.

## Identity-vs-Locality framing

ADR 063 is identity-neutral and locality-neutral. Page-level
stats are per-record physical metadata, downstream of admission
and orthogonal to row identity, locality projection, record
location, record row index, or visible row set.

Cross-replica determinism is preserved by deterministic stats
computation over (input batch, page boundaries,
`stats_supported_types`, cascade eligibility, profile scope).
Eligibility is purely a static function of the V1 chain
(fixed-width type set ∩ non-empty page geometry ∩ (ADR 059
allow-list ∩ `emits_pages_in_v1()`) ∩ Phase-0-pinned profile
scope); there are no runtime branches that could vary across
replicas. Slab-byte equality across replicas holds.

There is no separate "page-stats plane." Page stats live on
the `BufferEncoding` shape ADR 057 placed in the storage-write
surface. Unified Ring rule applies.

## V1 scope

### 1. `EncodingPageStats` shape

```rust
pub struct EncodingPageStats {
    pub null_count: PageNullCount,
    pub min: Option<CanonicalScalar>,
    pub max: Option<CanonicalScalar>,
}
```

`min` and `max` are `None` when the page has zero non-null
values; otherwise both are present and the `CanonicalScalar`
variant matches the column's Arrow type. `PageNullCount` is a
`#[repr(transparent)]` newtype-unit over `u32`.

### 2. `BufferEncoding` extension

```rust
pub struct BufferEncoding {
    // existing fields unchanged …
    pub page_value_count: SimdPageValueCount,
    pub page_end_offsets: Vec<EncodingPageByteOffset>,
    // NEW:
    pub page_stats: Vec<EncodingPageStats>,
}
```

Validation invariants:
- `page_stats.is_empty()` OR
  `page_stats.len() == page_end_offsets.len()`;
- per-stat `CanonicalScalar` variant matches the buffer's
  Arrow type;
- per-stat `null_count <= page_value_count` (with tail-row
  adjustment);
- if a page has zero non-null values, `min` and `max` are
  `None`;
- `page_stats` is non-empty only when the cascade is in
  the intersection (ADR 059 allow-list ∩
  `EncodingCascade::emits_pages_in_v1()`):
  `BitPacked` / `DictionaryPlain` / `DictionaryBitPacked`;
- `page_stats` is empty for `Delta` (allow-listed but not
  page-emitting in V1), `Rle*` (page-emitting but not
  allow-listed), `Plain`, `Lz4Frame`, `Fsst`, `Alp`;
- `page_stats` is empty on validity buffers; null counts
  attach to the corresponding value buffer's per-page entry;
- `page_stats` is empty for variable-width value buffers
  (UTF-8 / Binary / nested types).

### 3. `ENCODING_DIRECTORY_FORMAT_VERSION` bump

The encoding-directory value-shape constant under IPC
custom_metadata key `storage.encoding_directory` is bumped.
Decoder accepts only the new format. No compat struct, no
tolerant decoder, no legacy-decode test, no `V1` / `V2`
suffix types.

### 4. Shared canonical-scalar encoding

ADR 056 row-group stats currently host `CanonicalScalar`
helpers in `src/storage/row_group.rs`. ADR 063 moves them to
`src/storage/stats.rs` and uses them for both grains. Row-
group stat encoding bytes are unchanged (architectural test
pins single-source-of-truth). Page stats produce the same
canonical encoding for the same logical scalar.

### 5. Three-state predicate-page classification

The page classifier reuses `RowGroupPredicateKind` (ADR 056)
shape:

- equality (`col = K`): `Disjoint` if `K < min OR K > max`;
  `ExactMatch` only if `min == max == K AND null_count == 0`;
  else `Maybe`.
- range (`col BETWEEN A AND B`): `Disjoint` if
  `B < min OR A > max`; `ExactMatch` if
  `A <= min AND max <= B AND null_count == 0`; else `Maybe`.
- inequality (`< K` / `<= K` / `> K` / `>= K`): same shape
  with one-sided bound; `ExactMatch` requires the bound
  proven AND `null_count == 0`.
- `IS NULL`: `ExactMatch` if
  `null_count == page_value_count`; `Disjoint` if
  `null_count == 0`; else `Maybe`.
- `IS NOT NULL`: `ExactMatch` if `null_count == 0`;
  `Disjoint` if `null_count == page_value_count`; else
  `Maybe`.
- AND: `Disjoint` if any branch Disjoint; `ExactMatch` only
  if every branch ExactMatch; else `Maybe`.
- OR: `Disjoint` only if every branch Disjoint; `ExactMatch`
  if any branch ExactMatch; else `Maybe`.
- LIKE / IN / BETWEEN-with-cast / unsupported leaves →
  `Maybe`.
- missing / type-mismatched / absent stats → `Maybe`.
- truncated stats may only Disjoint (never ExactMatch); V1
  closed catalog is fixed-width-only, so truncation is not
  encountered.

### 6. Predicate-page fetch planner

Lives at `src/storage/encoding/predicate_page_fetch.rs`. The
production row-group read path
(`src/storage/manager.rs`) consults the planner before
issuing dense predicate fetch:

1. Walk surviving row groups (post-ADR-056 row-group prune).
2. For each predicate column, classify pages via stats.
3. Compose per-row-group `Disjoint` / `ExactMatch` / `Maybe`
   page sets.
4. Compute selected predicate plaintext bytes (Maybe pages
   only); compare to dense baseline.
5. If strictly cheaper AND within existing fragmentation /
   range / page caps → engage narrowed plan.
6. Fetch Maybe pages via ADR 044 `TieredMultiRangeReadPlan`
   (ADR 061 chunk-aligned coalescing absorbs fragmentation).
7. Run COCD kernels (ADR 058 / 059) on each Maybe page;
   stitch result with `EncodedRowMask::All` slices for
   ExactMatch pages and `EncodedRowMask::None` slices for
   Disjoint pages into the per-row-group survival mask.
8. Hand mask-only `PhaseBOutcome` downstream (no dense
   predicate batches).
9. If the planner declines pre-fetch → existing dense
   predicate path runs unchanged.
10. If a post-fetch invariant fails (parse error,
    decoded-length mismatch, etc.) → fail closed via existing
    storage-error / reject taxonomies; never silent retry.

### 7. Composability with ADR 060 / 061 / 062 / 058 / 059 / 052 / 055

**ADR 060 sparse projection fetch** unchanged. ADR 060
consumes the survival mask Phase B produces; that mask is
unaffected by *how* Phase B narrowed its predicate fetch.
The two narrowings compose multiplicatively on byte economy.
Page-stat-narrowed predicate fetch becomes an additional
upstream source of exact masks for ADR 060's planner.

**ADR 061 chunk-aligned reads** unchanged. Page-narrowed
predicate fetch produces byte ranges; ADR 044's coalescer
chunk-aligns and macro-block-merges. ADR 061's p50 ≥ 1 MiB
GET target survives because contiguous Maybe pages produce
contiguous chunk-aligned ranges.

**ADR 062 `Scan` profile** is the primary beneficiary.
65K-row groups × ~64 pages per buffer × stats-supported
columns yields the largest absolute predicate-fetch reduction.
`PointRead`'s smaller per-buffer page count produces smaller
relative wins; both are net positive.

**ADR 058 / 059 COCD** unchanged. Predicate evaluation runs
on the surviving-pages-only buffers Phase B fetched; encoded-
byte input shape per page is identical to whole-buffer input.
ExactMatch slices simply skip the kernel; Disjoint slices
skip both the fetch and the kernel.

**ADR 052 secondary-index point reads** unchanged. Point reads
target a single `RecordRowIndex`, bypassing the predicate-
driven Phase B path; ADR 063 is orthogonal.

**ADR 055 LSM compaction** recompacts pre-063 slabs through
the new directory shape on natural compaction; per-output-slab
directory recompute under matching writer config; cross-
replica byte-equality preserved.

### 8. Architectural lockdown

Incremental guards in `tests/architecture_test.rs`:

- `EncodingPageStats` is a closed-shape struct with the three
  fields above.
- `BufferEncoding.page_stats: Vec<EncodingPageStats>` is the
  only new public field.
- `ENCODING_DIRECTORY_FORMAT_VERSION` is bumped from the
  ADR 057 value.
- No `EncodingDirectoryV2` / `EncodingPageStatsV1` /
  `BufferEncodingV2` suffix types.
- No tolerant or legacy encoding-directory decoder; decoder
  accepts only the new current version.
- No second IPC `custom_metadata` key for page stats.
- `CanonicalScalar` lives in `src/storage/stats.rs`;
  row-group + page stats use the same encoding helpers
  (single import boundary).
- `PageNullCount` is a `#[repr(transparent)]` newtype-unit in
  `src/schema/units.rs`.
- `EncodingPageStats` lives in
  `src/storage/encoding/types.rs`.
- Predicate-page planner lives at
  `src/storage/encoding/predicate_page_fetch.rs`.
- `PredicatePageFetchDecline` / equivalent decline-reason
  type is private to the module.
- Row-group read path calls predicate-page planner before
  dense predicate fetch.
- **No runtime metadata-budget fallback in V1.** Phase 0
  modeling is the sole budget discipline. Architectural test
  rejects any writer-side per-record budget branch, profile-
  keyed omission rule, or partial per-column heuristic.
- Cascade-eligibility set for page-stat emission is the
  intersection of ADR 059's default predicate allow-list
  (`BitPacked`, `DictionaryPlain`, `DictionaryBitPacked`,
  `Delta`) AND `EncodingCascade::emits_pages_in_v1()`
  (`BitPacked`, `Rle`, `Dictionary`). The V1 intersection
  is `BitPacked`, `DictionaryPlain`, `DictionaryBitPacked`.
  Architectural test asserts the writer's emission set
  matches the intersection of
  `EncodingComputeConfig::default().enabled_predicate_evaluators`
  AND `emits_pages_in_v1()`. If a future ADR adds page
  emission for `Delta` or removes `Rle` from page emission,
  ADR 063's emission set follows automatically; ADR 063
  itself does not amend ADR 057 page emission.
- No new `L2FetchStrategy` / `EncodedRowMask` /
  `EncodingCascadeKind` / `EncodingFallbackReason` /
  `EncodingRejectCode` / `RowGroupFallbackReason` /
  `RowGroupRejectCode` / `EncodingComputeFallbackReason` /
  `EncodingComputeRejectCode` / `PredicateAtomOp` /
  `EncodingComputeEvaluatorKind` / `EncodedAggregateKind`
  variant.
- No new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability`
  bit / digest domain / `ObjectStore` trait method.
- No new descriptor field / fingerprint surface change.
- No new public PBC opcode.
- No new operator config field on `RowGroupWriterConfig`,
  `EncodingWriterConfig`, `EncodingComputeConfig`,
  `L2RowGroupFetchConfig`, `L2EncodingFetchConfig`,
  `L2ChunkLayoutConfig`.
- No new metric label.
- ADR 020 §2 AAD layout unchanged (48 B).
- ADR 022 §3 / §5 byte layouts unchanged.
- ADR 056 `RecordPageDirectory` shape unchanged.
- ADR 061 closed chunk-size set unchanged.
- ADR 062 `RowGroupSizingProfile` catalog unchanged.
- Cross-replica byte-equal output across two writers with the
  same config + input, including page stats.
- No Cargo feature gating page-stats activation.
- No nightly rustc.
- No version suffix on new types.
- No planning-phase identifier on new symbols.
- No project-name prefix.

### 9. Bench fixture mandate

Bench artifact records:

- **Pre-063 byte-identical recompact**: ADR 055 LSM
  compaction takes a pre-063 slab through the new shape;
  output stats match the deterministic expected stats over
  the same logical input.
- **Predicate-fetch byte reduction** at clustered selectivity
  on `Scan` profile:
  - clustered 5% ≥ 3× plaintext-byte reduction vs row-group-
    only baseline (headline);
  - clustered 25% ≥ 1.5×.
- **Random-uniform**: page-stats prune produces ≤ 10%
  additional reduction (intrinsically weak; recorded
  separately, not gated). Planner declines or stays within
  ≤ 5% dense-fallback overhead.
- **ExactMatch effectiveness**: sorted-broad predicate
  (e.g., `col > median`, no-nulls fixture) records the
  fraction of pages that classify ExactMatch and the
  predicate-bytes saved by skipping them. All-ExactMatch
  fixture records zero predicate fetch bytes.
- **ExactMatch correctness with nulls**: nullable column ×
  range predicate fixture verifies pages with
  `null_count > 0` AND min/max-pass classify `Maybe`, not
  `ExactMatch`; mask byte-identical to dense path.
- **Phase 0 model verification**: bench reproduces the
  Phase 0 size-model on the largest legal `Scan` and
  `PointRead` fixtures; actual directory bytes match
  modeled bytes within ≤ 5%. If model drift exceeds 5%,
  modeling methodology is revisited (not the runtime).
- **Combined directory metadata**: `storage.row_group_directory
  + storage.encoding_directory` (now including page stats)
  fits within ADR 044 64 KiB initial-probe envelope on every
  representative legal fixture.
- **Writer cost**: writer wall time stays within the existing
  ADR 057 / 062 accepted writer-cost envelope on the largest
  legal fixture, or the bench artifact records a council-
  approved revision.
- **Compression loss**: zero (page stats live in directory,
  not in encoded buffers).
- **Composability with ADR 060**: sparse projection output
  byte-identical after narrowed predicate fetch; projection
  byte count unchanged.
- **Composability with ADR 061**: p50 / p95 coalesced GET
  payload sizes unchanged on macro-block-coalesced predicate
  fetches; ADR 061's ≥ 1 MiB p50 dense-path target preserved.
- **Composability with ADR 062**: `Scan` profile predicate-
  fetch reduction strictly larger absolute byte savings than
  `PointRead` on matched-shape fixtures.
- **Composability with ADR 052**: point-read selected bytes
  unchanged; ADR 063 does not engage on point-read path.
- **Cross-replica determinism**: byte-equal output across two
  writers with same (config, input, profile).
- **Boundary fixtures**: `Scan` exact-target one-row-group
  record (one-entry directory) — page stats present and
  consumable; `PointRead` sub-paging-gate record — no page
  stats, dense path unchanged.
- **Type coverage**: every fixed-width type in ADR 063's V1
  catalog (Int8/16/32/64, UInt8/16/32/64, Timestamp(ms/us/ns),
  Date32/64, Boolean) × every cascade in the V1 intersection.
  UTF-8 / LargeUtf8 / Binary / LargeBinary buffers confirmed
  empty `page_stats` (row-group stats unchanged).
- **Cascade-eligibility coverage**: `BitPacked`,
  `DictionaryPlain`, `DictionaryBitPacked` carry page stats;
  `Delta` carries empty `page_stats` (allow-listed but not
  page-emitting in V1); `RlePlain`, `RleBitPacked` carry
  empty `page_stats` (page-emitting but not allow-listed);
  `Plain`, `Lz4Frame`, `Fsst`, `Alp` carry empty
  `page_stats`.
- **Survival-mask byte-equality**: narrowed-path mask
  byte-identical to dense-path mask for the same logical
  predicate.
- **All-pages-Disjoint and all-pages-ExactMatch** boundary
  rows record the correct mask shape (`None`-only and
  `All`-only respectively) and zero predicate-fetch bytes.
- **No-page-stats slab** (writer disabled) records dense-
  path-equivalent output as non-regression.
- **`PointRead` profile**: predicate-fetch reduction ≥ 1.5×
  at clustered 5% (hard gate). The Phase 0 size-model and
  win/cost analysis must establish whether `PointRead`
  emission is in scope *before* implementation begins; if
  Phase 0 evidence shows the win < 1.5×, ADR 063 is revised
  pre-code to `Scan`-only emission. There is no
  promotion-time scope change for the profile-emission
  decision.

## V1 deferred (with explicit V2 promotion paths)

ADR 063 deliberately does **not** ship:

- **Per-page stats for variable-width types (UTF-8 / Binary /
  Decimal).** UTF-8 / Binary continue to receive row-group
  stats per ADR 056; ADR 063 does *not* extend them to page
  grain. **V2 promotion path:** a string-stats ADR defines
  truncation rules and ordering invariants for UTF-8 min/max
  under a closed type catalog; ExactMatch behavior follows.
- **Per-page stats for nested types (List / Struct / Map).**
  **V2 promotion path:** if columnar nested-type stats prove
  load-bearing, a follow-up ADR defines per-leaf-type stats
  composition rules.
- **Per-page stats for RLE byte-run cascades.** ADR 059
  Phase 10 evidence excluded RLE from the default predicate
  allow-list; ADR 063 inherits that exclusion. **V2 promotion
  path:** if RLE value-run format ADR ships, page-stat
  emission for RLE follows automatically.
- **Per-page stats for FSST / ALP / Plain / Lz4Frame
  cascades.** Non-page-emitting or non-allow-listed cascades.
  **V2 promotion path:** dedicated cascade-evaluator ADRs
  (FSST evaluator, ALP evaluator) extend page-stat emission
  as their kernels land.
- **Dense predicate-zip with sparse projection.** ADR 060 V1
  deferred this; ADR 063 V1 narrowed-path is mask-only and
  intentionally leaves `decoded_predicate_batches` empty.
  **V2 promotion path:** a dedicated row-alignment ADR for
  page-sliced predicate-projection zip.
- **Sparse predicate fetch for non-COCD predicates.** V1
  narrows only exact-COCD-compatible predicate paths. Dense
  PBC page-sliced evaluation has a separate materialization
  story. **V2 promotion path:** if dense PBC over partial
  buffers proves correct under page-sliced reassembly,
  follow-up ADR.
- **Page-level stats other than min/max/null.** Bloom /
  histogram / KLL / HLL / dictionary-set intersections are
  separate stat classes. **V2 promotion path:** dedicated ADR
  per richer stat class; ADR 063 single-class V1 keeps the
  closed catalog tight.
- **Operator-tunable page-stats budget.** Activation is by
  existing toggles + type set + cascade. **V2 promotion path:**
  if operational pressure demands it.
- **Per-cascade page-stats opt-out.** All cascades in the
  V1 intersection (ADR 059 default predicate allow-list ∩
  `EncodingCascade::emits_pages_in_v1()` = `BitPacked` /
  `DictionaryPlain` / `DictionaryBitPacked`) emit stats
  unconditionally; cascades outside the intersection (Delta,
  RLE byte-run, Plain, Lz4Frame, Fsst, Alp) carry empty
  `page_stats`. **V2 promotion path:** bench evidence that
  a specific in-intersection cascade's page-stats cost
  exceeds its win.
- **Runtime metadata-budget fallback outside the
  Phase-0-pinned profile scope** (per-record branch, runtime
  profile-keyed omission, partial per-column heuristic).
  Distinct from the Phase 0 decision to pin `Scan`-only
  emission, which is a static eligibility predicate baked
  into the writer. **V2 promotion path:** if production
  evidence shows the Phase 0 modeling envelope is wrong on
  operational workloads, a follow-up ADR introduces the
  runtime budget shape with its own bench gate. Wendelon V1
  is gate-driven, not runtime-fallback-driven.
- **Stats-driven page reordering / sorting.** Page boundaries
  follow ADR 057's row-order emission. Stats-friendly
  clustering is a separate write-side ADR.
- **Page-level aggregate short-circuit.** ADR 058 aggregate
  folding remains unchanged; page stats may later inform
  partial folds, but ADR 063 uses them only for predicate
  classification.
- **Migration sweeper for existing slabs.** ADR 055 LSM
  compaction picks up page stats on natural recompacts.
- **Flipping row-group writer default-on.** Separate
  operational ADR (ADR 062 carried this forward; 063
  inherits).

## Council convergence path

Five iterations across Claude / Codex / Gemini converged on
the design above. Five questions drove the most council
oscillation:

**Where page stats live**: iter 1 surfaced three placements —
`BufferEncoding.page_stats` per-buffer field, sibling
`EncodingDirectory.page_stats: Vec<PageColumnStats>` per-
column collection, and `RecordPageDirectory` placement
alongside row-group stats. Iter 2 converged on per-buffer
placement: page geometry already lives on `BufferEncoding`
(`page_end_offsets`, `page_value_count`), so stats follow the
grain that defines them. Per-column placement would force a
cross-lookup; `RecordPageDirectory` placement would make the
outer row-group directory depend on inner encoded-page
geometry.

**Format compatibility**: iter 1 surfaced three shapes —
strict format-version bump (no compat decoder), tolerant
trailing-field decode, and `Option<Vec<EncodingPageStats>>`
field with `None` for legacy. Iter 2 converged on the strict
bump per `plans/conventions/no-premature-backward-compat.md`:
the codebase is pre-launch with no deployed format contract,
and ADR 055 LSM compaction recompacts pre-063 slabs through
the new shape on natural rebuilds.

**Page classification grain**: iter 1 / iter 2 oscillated
between two-state (`CannotMatch` / `MayMatch`) and three-state
(`Disjoint` / `ExactMatch` / `Maybe`). Iter 3 locked
three-state: `ExactMatch` maps cleanly to ADR 058's existing
`EncodedRowMask::All { rows }` algebra and unlocks zero-fetch
short-circuiting on sorted/clustered predicates. Iter 3 also
locked the null-position-conservative correctness rule:
`ExactMatch` requires `null_count == 0` for value comparisons
because building the correct mask on a nullable page would
otherwise require validity-buffer bytes — defeating the
optimization.

**Cascade eligibility**: iter 1 / iter 2 considered
"all page-emitting cascades" (broad) versus narrowing to
ADR 059's default predicate evaluator allow-list (tight). Iter
3 / iter 5 locked the tight scope and made the joint
requirement ("allow-list AND page geometry") explicit. The
post-merge code review further tightened this to the
**intersection** of two existing sets: ADR 059's default
predicate allow-list (`BitPacked`, `DictionaryPlain`,
`DictionaryBitPacked`, `Delta`) AND
`EncodingCascade::emits_pages_in_v1()` (`BitPacked`, `Rle`,
`Dictionary`). The V1 intersection is **three cascades**:
`BitPacked`, `DictionaryPlain`, `DictionaryBitPacked`.
`Delta` drops out because it is allow-listed but does not
emit pages today (ADR 063 does not amend ADR 057's page
emission). RLE byte-run drops out because Phase 10 evidence
excluded it from ADR 059's default allow-list. Future ADRs
can amend either set; ADR 063's intersection follows
automatically.

**Metadata budget gate**: iter 2 / iter 3 oscillated between
a Phase 0 pre-code size-model gate (sole discipline) and a
two-layer gate adding a writer-side runtime all-or-empty
fallback as defense-in-depth. Iter 4 locked Phase-0-only:
Wendelon V1 ships are gate-driven, not runtime-fallback-driven
(ADR 062 deterministic profile emission, ADR 060 plan-time
decline). If representative legal fixtures miss the model,
ADR 063 is revised before code lands; runtime budget fallback
is V2 territory if production evidence demands it.

Iter 5 also locked smaller refinements: `ExactMatch` naming
over `AllMatch` (more standard English vocabulary; tied 4-4
in earlier iterations); "value buffer" Arrow terminology; the
writer-eligibility-vs-reader-narrowing distinction explicit;
and the bench-evidence-driven promotion gates (Phase 0 model
≤ 5% drift, type/cascade coverage, `PointRead` profile
council-revisable gate).

## Rejected alternatives

- **Page stats as a sibling `storage.page_stats_directory`
  IPC custom_metadata key.** Doubles the parse path; creates
  a cross-directory consistency surface; page geometry
  already lives in `EncodingDirectory`.
- **Page stats in `RecordPageDirectory`.** ADR 056 is the
  outer row-group directory; page stats are inner ADR 057
  encoded-page metadata. Placement there would force the
  outer directory to depend on inner page geometry.
- **Per-column sibling `EncodingDirectory.page_stats:
  Vec<PageColumnStats>` collection.** Plausible, but per-
  buffer placement is simpler and aligns 1:1 with
  `page_end_offsets`. Per-column placement creates a cross-
  lookup and makes absent stats a column-level rather than
  buffer-local property.
- **Tolerant trailing-field decode / `Option<Vec<>>` field
  shape.** Both are hedges. Pre-launch codebase, no deployed
  format contract; strict format-version bump is convention-
  aligned.
- **`EncodingDirectoryV2` suffix type / compat decoder /
  legacy-decode test.** Per
  `plans/conventions/no-premature-backward-compat.md` and
  `plans/conventions/no-version-suffixes.md`.
- **Two-state classification.** Loses the ExactMatch
  optimization on sorted/clustered predicates and predicates
  with monotone columns. ExactMatch is safe in V1 because
  the closed type catalog is fixed-width and the conservative
  null-count rule pins the correctness story.
- **`ExactMatch` when null positions are unknown.** For
  value comparisons on nullable columns, even if min/max
  prove every non-null value matches, building the mask
  requires null-position bytes. Pages with `null_count > 0`
  are `Maybe` for value comparisons; null predicates use
  `null_count` directly.
- **`AllMatch` naming.** Considered for direct mapping to
  `EncodedRowMask::All`. `ExactMatch` is more standard
  English vocabulary (regex/equality) and avoids overload
  with ADR 058's "exact-COCD" planner-mode terminology.
- **Cascade eligibility broader than the V1 intersection.**
  Emitting for cascades V1 cannot consume (RLE byte-run,
  FSST, ALP, Plain, Lz4Frame, Delta) burns metadata budget
  without payoff. The V1 emission set must be the
  intersection of the allow-list and `emits_pages_in_v1()`,
  not either set alone.
- **Page stats without a reader consumer.** Metadata weight
  without immediate read-side win.
- **Predicate-fetch narrowing without page stats.** No
  selectivity signal before survival mask exists.
- **Dense PBC page-sliced predicate fetch in V1.** Different
  correctness story from exact-COCD page masks; defer to
  follow-up ADR.
- **A new top-level `L2FetchStrategy::PageNarrowedRowGroup`
  variant.** ADR 060's no-fifth-strategy framing applies:
  internal Phase B optimization, not a strategy shape.
- **A public PBC opcode for page stats.** Storage internals;
  Toron contract byte-identical.
- **Operator-visible page-stats config flag.** Engagement is
  determined by existing row-group / encoding toggles + type
  set + cascade eligibility.
- **Page-level Bloom filters or hyperloglog or dictionary-set
  intersections in V1.** Each is its own stat class; bundling
  forfeits closed-catalog discipline.
- **Stats catalog richer than min/max/null in V1.** Each
  richer class is a follow-up ADR.
- **Stats-driven row reordering for stats efficacy.**
  Separate write-side ADR.
- **A separate Phase B' between Phase A and Phase B.**
  Doubles round-trip count; directory is in hand at Phase A.
- **Independent page-stats encoding helper.** Forks
  `CanonicalScalar`; introduces row-group / page byte
  divergence risk.
- **Writer-side all-or-empty runtime budget fallback.** Phase
  0 modeling is the V1 budget discipline; if representative
  legal fixtures miss the probe envelope, ADR 063 is revised
  before code lands. Runtime budget fallback is V2 territory
  if production evidence demands it.
- **Any runtime metadata-budget fallback outside the
  Phase-0-pinned profile-emission scope** (per-record
  budget branch, runtime profile-keyed omission, partial
  per-column heuristic, operator knob, metric label, public
  fallback variant). The Phase 0 size-model + win/cost gate
  may pin the V1 scope to `Scan`-only emission as a static
  pre-code decision; that is *not* a runtime fallback —
  it's a fixed eligibility predicate built into the writer.
  Runtime budget fallback shapes that omit page stats based
  on per-record state at write time are V2 territory.

## What not to do

- **Don't extend `BufferEncoding` for anything other than
  page stats in this ADR.** No COCD-aware fields, no per-page
  Bloom filters, no per-page null bitmap caches.
- **Don't add `EncodingDirectoryV2` / `EncodingPageStatsV1`
  suffix types.**
- **Don't tolerant-decode the pre-063 encoding-directory
  format.** Strict version bump, single decoder.
- **Don't add a second IPC custom_metadata key for page
  stats.**
- **Don't copy page geometry into `RecordPageDirectory`.**
- **Don't classify a page `ExactMatch` if null-position bytes
  would be required to build the correct mask.** Pages with
  `null_count > 0` under value comparisons are `Maybe`.
- **Don't emit page stats for cascades outside the V1
  intersection** (ADR 059 default predicate allow-list ∩
  `EncodingCascade::emits_pages_in_v1()` = `BitPacked` /
  `DictionaryPlain` / `DictionaryBitPacked`). `Delta` is
  allow-listed but not page-emitting in V1; RLE byte-run
  is page-emitting but not allow-listed; both carry empty
  `page_stats`.
- **Don't add a runtime metadata-budget fallback in V1.**
  Phase 0 size-modeling gate is the sole discipline; if
  representative fixtures miss the model, revise the ADR
  before code lands. No writer-side per-record budget
  branch, no runtime profile-keyed omission rule, no
  partial per-column heuristic. (A Phase-0-pinned
  `Scan`-only profile predicate baked into the writer is
  a static eligibility gate, not a runtime fallback.)
- **Don't bundle 063 with RLE value-run / FSST evaluator /
  ALP evaluator / dense predicate-zip / nullable sparse
  projection / variable-width sparse projection / sparse
  predicate fetch for non-COCD predicates / page-level
  aggregate short-circuit / page-level Bloom filters.** Each
  is its own ADR.
- **Don't change ADR 057's RLE byte-run format inside
  ADR 063.**
- **Don't add a fifth `L2FetchStrategy` variant.**
- **Don't add public PBC opcodes or Toron-visible page-stat
  hints.**
- **Don't leak planner decline reasons into metric labels or
  fallback enums.**
- **Don't make page stats an operator-tuned surface in V1.**
- **Don't make ADR 060 sparse projection fetch depend on
  dense predicate batches from ADR 063.**
- **Don't change ADR 056 / 022 footer / ADR 020 AAD layout.**
- **Don't add a new `RowGroupRejectCode` /
  `RowGroupFallbackReason` / `EncodingRejectCode` /
  `EncodingFallbackReason` / `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` / `EncodedRowMask` /
  `EncodingCascadeKind` / `PredicateAtomOp` /
  `EncodingComputeEvaluatorKind` / `EncodedAggregateKind`
  variant.**
- **Don't add a new metric label.**
- **Don't introduce planning-phase identifiers
  (`PhaseB2PageStats`, `phase_b_page_*`, etc.).**
- **Don't introduce version suffixes
  (`PageColumnStatsV1`, `BufferEncodingV2`).**
- **Don't reuse ADR 040 / ADR 058 stats-related vocabulary
  for ADR 063 surface.** ADR 040 is dense aggregate
  validation; ADR 058 is COCD; ADR 063 is fetch-planner
  predicate stats. Different layers.
- **Don't fork `CanonicalScalar` between row-group and page
  stats.**
- **Don't engage page-stats prune on point-read paths**
  (ADR 052 secondary-index lookups bypass Phase B).

## Convergence summary

```text
ADR 056 row-group fetch + per-row-group stats + page directory
  → ADR 057 per-buffer pages + cascades
  → ADR 058 substrate COCD
  → ADR 059 native COCD kernels
  → ADR 060 sparse projection fetch (Phase C narrowing)
  → ADR 061 page-aligned AEAD chunks + macro-block coalescing
  → ADR 062 row-group sizing profile (PointRead/Scan)
  → ADR 063 page-level predicate stats + Phase B narrowing
    (this ADR; BufferEncoding.page_stats; ENCODING_DIRECTORY_
     FORMAT_VERSION bump; three-state Disjoint/ExactMatch/
     Maybe with null-position-conservative ExactMatch;
     cascade eligibility = ADR 059 allow-list ∩
     emits_pages_in_v1() = {BitPacked, DictionaryPlain,
     DictionaryBitPacked} in V1; type catalog = fixed-width
     subset of ADR 056 stats_supported_types;
     predicate_page_fetch planner; mask-only Phase B output
     preserves ADR 060; Phase 0 size-model + profile-emission
     gate is the sole budget discipline; no runtime fallback
     in V1)  ← here
  → next Toron-adjacent follow-ups (RLE value-run; FSST
    evaluator; ALP evaluator; nullable sparse projection;
    variable-width sparse projection; dense predicate-zip
    with sparse projection; ADR 061 V2)
  → capacity-tier compression / serverless / EC bundle
```

## Promotion criteria

ADR 063 promoted from council-converged → shipped when:

1. `EncodingPageStats` shape +
   `BufferEncoding.page_stats: Vec<EncodingPageStats>`
   field land in `src/storage/encoding/types.rs`.
2. `ENCODING_DIRECTORY_FORMAT_VERSION` is bumped; decoder
   accepts only the new format; no compat decoder lands.
3. `CanonicalScalar` (or equivalent shared scalar encoding)
   moves to `src/storage/stats.rs`; row-group stat byte
   layout unchanged; page stats use the shared helper.
4. `PageNullCount` newtype-unit lands in
   `src/schema/units.rs`.
5. Writer at `src/storage/multirow.rs` (or encoding-write
   subroutine) computes per-page stats for the V1 eligibility
   intersection — fixed-width types in ADR 063's V1 catalog
   AND cascades in (ADR 059 default predicate allow-list ∩
   `EncodingCascade::emits_pages_in_v1()`) — over value-buffer
   page boundaries that are non-empty, and emits them on
   `BufferEncoding`. If Phase 0 pinned `Scan`-only emission,
   the writer additionally gates on
   `RowGroupSizingProfile::Scan`. No runtime metadata-budget
   fallback; Phase 0 modeling is the V1 discipline and the
   profile-emission scope is fixed at Phase 0.
6. Predicate-page planner at
   `src/storage/encoding/predicate_page_fetch.rs` performs
   three-state classification with null-position-conservative
   ExactMatch, emits multi-range fetch plan, stitches per-row-
   group `EncodedRowMask` from `Disjoint` / `ExactMatch` /
   kernel-evaluated slices.
7. `src/storage/manager.rs` consults the planner before
   dense predicate fetch; declines fall through to existing
   dense path; engaged narrowed plan returns mask-only
   `PhaseBOutcome`.
8. Phase 0 size-modeling artifact records that combined
   directory bytes fit ADR 044 64 KiB initial-probe envelope
   on every representative legal fixture *before* code lands;
   if model misses, ADR is revised before code lands. V1 has
   no runtime budget fallback.
9. Bench artifact at
   `plans/impl/063_page_level_predicate_stats_bench_results.md`
   demonstrates:
   - clustered predicate-fetch ≥ 3× plaintext-byte reduction
     at 5% selectivity on `Scan` (headline);
   - clustered ≥ 1.5× at 25%;
   - random-uniform ≤ 5% dense-fallback overhead;
   - all-ExactMatch fixture: zero predicate fetch bytes,
     byte-identical mask;
   - all-Disjoint fixture: zero predicate fetch bytes,
     byte-identical mask;
   - writer wall time within ADR 057 / 062 envelope (or
     council-approved revision recorded);
   - cross-replica byte-equality;
   - survival-mask byte-equality vs dense path;
   - composability gates for ADR 060 / 061 / 062 / 052 / 058
     / 059 hold.
10. **Phase 0 model verification**: promotion bench
    reproduces the Phase 0 size-model on the largest legal
    `Scan` and `PointRead` fixtures; actual directory bytes
    match modeled bytes within ≤ 5%. If drift exceeds 5%,
    the modeling methodology is revised before promotion
    (not the runtime).
11. **ExactMatch correctness with nulls**: nullable-column ×
    range-predicate fixture confirms pages with
    `null_count > 0` classify `Maybe`, not `ExactMatch`,
    when null positions would be needed; final mask
    byte-identical to dense-path mask.
12. **Type and cascade coverage**: bench fixtures verify
    that every fixed-width type in ADR 063's V1 catalog
    (Int8/16/32/64, UInt8/16/32/64, Timestamp(ms/us/ns),
    Date32/64, Boolean) paired with every cascade in the
    V1 intersection (`BitPacked`, `DictionaryPlain`,
    `DictionaryBitPacked`) carries page stats, AND that
    variable-width buffers (UTF-8 / LargeUtf8 / Binary /
    LargeBinary), validity buffers, allow-listed-but-not-
    page-emitting cascades (`Delta` in V1), page-emitting-
    but-not-allow-listed cascades (`RlePlain`,
    `RleBitPacked`), and other non-allow-listed /
    non-page-emitting cascades (`Plain`, `Lz4Frame`,
    `Fsst`, `Alp`) carry empty `page_stats`.
13. **PointRead profile bench gate (hard)**: predicate-
    fetch reduction ≥ 1.5× at clustered 5% on `PointRead`
    profile fixtures. The profile-emission scope is fixed
    in Phase 0 (see Phase 0 size-model + win/cost gate);
    if Phase 0 evidence said `Scan`-only, this gate does
    not apply to `PointRead` because `PointRead` does not
    emit page stats. There is no promotion-time scope
    change.
14. Architectural lockdown guards land per §"Architectural
    lockdown".
15. ADR 057 §"Where we are" amended to reference the
    `BufferEncoding.page_stats` field and the format-version
    bump; ADR 056 §"Where we are" amended to reference Phase
    B page-grain pruning; ADR 060 §"Where we are" amended to
    record that exact mask production can now come from
    page-stat-narrowed predicate fetch (mask-only
    `PhaseBOutcome` upstream of ADR 060's sparse projection
    planner).
16. ADR 020 / 022 / 044 / 052 / 055 / 056 / 057 / 058 / 059 /
    060 / 061 / 062 focused tests stay green at both profiles.
