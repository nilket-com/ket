# Implementation Plan: ADR 063 — Page-Level Predicate Stats + Predicate-Fetch Narrowing

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/0063_page_level_predicate_stats.md`

Inherits substrate:

- `plans/adr/0056_row_group_fetch_and_page_directory.md` —
  `RecordPageDirectory`, `RowGroupColumnStats`, `CanonicalScalar`
  helpers, `L2FetchStrategy::RowGroup` four-leg dispatch,
  three-phase fetch planner, Phase B predicate-stats prune.
- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md` —
  `EncodingDirectory`, `BufferEncoding`, `page_end_offsets`
  prefix array, `page_value_count`, `EncodingCascade`,
  `ENCODING_DIRECTORY_FORMAT_VERSION`.
- `plans/adr/0058_compute_over_encoded_data.md` /
  `plans/adr/0059_native_cocd_kernels.md` — exact-COCD survival
  masks, `EncodedRowMask::{All, None, Bitmap, Runs}` algebra,
  ADR 059 default predicate allow-list (`BitPacked`,
  `DictionaryPlain`, `DictionaryBitPacked`, `Delta`).
- `plans/adr/0060_sparse_projection_fetch_narrowing.md` —
  per-buffer selected-page set + multi-range fetch shape;
  internal-decline discipline; `sparse_projection_fetch.rs`
  module location precedent.
- `plans/adr/0061_page_aligned_authenticated_chunk_layout.md` —
  closed AEAD chunk-size set + macro-block coalescing.
- `plans/adr/0062_row_group_sizing_profile.md` — closed
  `PointRead` / `Scan` catalog; deployment-wide profile;
  `Scan` 65K-row groups make page-grain prune load-bearing.
- `plans/conventions/no-planning-phase-names.md`,
  `plans/conventions/newtype-units.md`,
  `plans/conventions/no-version-suffixes.md`,
  `plans/conventions/no-premature-backward-compat.md`,
  `plans/conventions/no-project-name-in-protocols.md`,
  `plans/conventions/capability-vs-feature-naming.md`.

## Status

Shipped 2026-05-09. Implementation, tests, architecture
guards, and quick bench evidence landed together. Evidence is
recorded at
`plans/impl/0063_page_level_predicate_stats_bench_results.md`.
The Phase 0 profile-emission decision keeps both `PointRead`
and `Scan` in V1: quick clustered 5% predicate bytes drop
`9,153 -> 144` for `Scan` (63.56x) and `1,145 -> 144` for
`PointRead` (7.95x), with combined metadata inside ADR 044's
64 KiB initial-probe envelope.

Implementation contract:

- `BufferEncoding.page_stats: Vec<EncodingPageStats>`
  (per-buffer field; length `0` or `page_end_offsets.len()`);
- `EncodingPageStats { null_count, min, max }` per-page struct;
- `ENCODING_DIRECTORY_FORMAT_VERSION` bumped; no compat
  decoder; no `Option<>` field hedge; no suffix types;
- `CanonicalScalar` helpers shared via `src/storage/stats.rs`;
  row-group stat byte layout unchanged;
- writer computes per-page stats for the fixed-width
  page-stat type subset (Int8/16/32/64, UInt8/16/32/64,
  Timestamp(ms/us/ns), Date32/64, Boolean) AND cascades in
  the V1 intersection (`EncodingComputeConfig::default()
  .enabled_predicate_evaluators` ∩
  `EncodingCascade::emits_pages_in_v1()` = `BitPacked` +
  `DictionaryPlain` + `DictionaryBitPacked`); other
  cascades carry empty `page_stats`;
- `Delta` is allow-listed but does not emit pages in V1;
  it carries empty `page_stats` until a future ADR amends
  ADR 057 page emission;
- both intersection conditions required jointly; cascade
  in allow-list with empty page geometry leaves
  `page_stats` empty;
- no runtime metadata-budget fallback in V1: Phase 0 size-
  model gate confirms representative legal fixtures fit
  ADR 044's 64 KiB initial-probe envelope before code lands;
  if model misses, ADR is revised pre-code;
- predicate-page planner at
  `src/storage/encoding/predicate_page_fetch.rs` performs
  three-state `Disjoint` / `ExactMatch` / `Maybe` classification
  with null-position-conservative ExactMatch correctness;
- writer-side eligibility ≠ reader-side narrowing: a buffer
  may carry stats and still see its predicate-page planner
  decline at plan time (additional COCD/predicate/economic
  gates);
- production read path consults planner before dense predicate
  fetch; declines fall through to existing dense path;
- engaged path returns mask-only `PhaseBOutcome`; ADR 060
  sparse projection fetch composes downstream;
- multi-range fetch via ADR 044 `TieredMultiRangeReadPlan` +
  ADR 061 macro-block coalescer;
- no descriptor / wire / capability / PBC / digest-domain /
  `ObjectStore` / second-IPC-key / metric-label change;
- no new fallback / reject / catalog variant (existing
  `EncodingDirectoryParseError` / `PageRangeOverflow` cover);
- no ADR 022 / ADR 056 / ADR 060 / ADR 061 / ADR 062 surface
  change;
- bench artifact demonstrates clustered ≥ 3× predicate-fetch
  byte reduction at 5% selectivity, ≥ 1.5× at 25%; metadata
  fits ADR 044 64 KiB initial probe (Phase 0 model verified
  ≤ 5% on bench); ExactMatch correctness with nulls.

## Goal

Make Phase B of the three-phase fetch planner prune predicate
fetch at page grain, not just row-group grain. ADR 062's `Scan`
profile creates 65K-row groups with ~64 pages per stats-
supported buffer; row-group-only prune leaves significant
fetch fat for clustered predicates. Page-level stats let the
planner skip pages whose stats prove disjoint and short-circuit
pages whose stats prove all-matching, narrowing predicate fetch
by 3–10× on clustered low-selectivity workloads while
preserving every other ADR 056–062 contract.

The architecture realizes this by:

```text
write side:
  writer config (existing ADR 056 + ADR 062)
    → for stats-supported fixed-width columns AND cascades in
      ADR 059's default predicate allow-list, compute per-page
      (null_count, min, max) over the value-buffer page row
      spans
    → tie page boundaries to existing page_end_offsets
    → emit page_stats inline on BufferEncoding (Phase 0
      modeling has cleared metadata budget pre-code; no
      runtime fallback)
    → AEAD encryption + IPC custom_metadata authentication
      (unchanged byte layout aside from the version bump)

read side:
  Phase A: fetch metadata + EncodingDirectory (now includes page stats)
  Phase B: row-group stats prune (existing) → predicate-page
           classification (NEW) → narrowed multi-range fetch
           of Maybe pages → COCD eval on Maybe pages →
           stitched survival mask (Disjoint / ExactMatch /
           kernel-evaluated)
  Phase C: projected fetch (unchanged; ADR 060 sparse if mask exact)
  Phase D: encoding materialization (unchanged)
```

## Deliverables

- `src/storage/stats.rs` (new):
  - host shared `CanonicalScalar` helpers (moved from
    `src/storage/row_group.rs`);
  - expose page-slice min/max/null-count helper over an
    Arrow array slice;
  - keep stat byte encoding identical to current
    `RowGroupColumnStats` shape.
- `src/storage/row_group.rs`:
  - reuse shared helpers via `crate::storage::stats`;
  - row-group `RowGroupColumnStats` byte layout and pruning
    behavior unchanged.
- `src/schema/units.rs`:
  - add `PageNullCount(u32)` newtype-unit (or reuse existing
    null-count newtype if shape matches).
- `src/storage/encoding/types.rs`:
  - add `EncodingPageStats { null_count, min, max }` struct;
  - extend `BufferEncoding` with
    `pub page_stats: Vec<EncodingPageStats>` field.
- `src/storage/encoding/directory.rs`:
  - bump `ENCODING_DIRECTORY_FORMAT_VERSION`;
  - serialize / deserialize `page_stats`;
  - validate page-stat count, type tags, null-count
    invariants, page-emitting-cascade gate, allow-list gate;
  - build page stats inline during writer flush (no runtime
    metadata-budget branch).
- `src/storage/encoding/predicate_page_fetch.rs` (new):
  - three-state predicate-page classifier
    (`Disjoint` / `ExactMatch` / `Maybe`) with null-position-
    conservative ExactMatch correctness;
  - `PredicatePageFetchPlan` shape;
  - `PredicatePageFetchDecline` enum (private to module);
  - economic gate against dense-baseline plaintext bytes;
  - mask-stitching for per-row-group `EncodedRowMask`
    composition.
- `src/storage/manager.rs`:
  - call predicate-page planner before dense encoded
    predicate fetch;
  - execute narrowed fetches via existing
    `TieredMultiRangeReadPlan`;
  - reconstruct exact survival masks; preserve dense fallback
    when planner declines.
- `tests/encoding_directory.rs`:
  - page-stat serialization, parse, validation tests;
  - format-version bump enforcement (no legacy decode);
  - cascade-eligibility validation (allow-listed cascades
    with page geometry emit stats; non-allow-listed and
    non-page-emitting cascades carry empty `page_stats`).
- `tests/encoding_predicate_page_fetch.rs` (new):
  - classifier unit tests (three-state outcomes);
  - ExactMatch null-position-conservative correctness tests;
  - mask-stitching tests;
  - Maybe-only / ExactMatch-only / Disjoint-only boundary
    fixtures.
- `tests/row_group_sparse_projection_fetch.rs`:
  - extension showing sparse projection still engages after
    narrowed predicate fetch (mask-only Phase B output).
- `tests/encoding_chunk_layout.rs`:
  - page-narrowed predicate fetch composes with every closed
    ADR 061 chunk size.
- `tests/encoding_secondary_index.rs`:
  - ADR 052 point-read path unchanged (no Phase B engagement).
- `tests/multirow.rs`:
  - writer page-stats emission round-trip;
  - cross-replica byte-equality with same config + input;
  - eligibility-gate determinism (same input + config →
    byte-identical `page_stats` emission shape).
- `tests/architecture_test.rs`:
  - ADR 063 lockdown guards.
- `benches/row_group_fetch.rs`:
  - extend with page-stats axis + clustered selectivity sweep
    + ExactMatch fixture + nullable-column ExactMatch fixture
    + Phase 0 model verification fixture + boundary rows.
- `plans/impl/0063_page_level_predicate_stats_bench_results.md`:
  - Phase 8 evidence artifact (at promotion only).
- `plans/adr/0056_row_group_fetch_and_page_directory.md`:
  - §"Where we are" amendment after Phase 9 promotion
    referencing Phase B page-grain pruning.
- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md`:
  - §"Where we are" amendment referencing the
    `BufferEncoding.page_stats` field and format-version bump.
- `plans/adr/0060_sparse_projection_fetch_narrowing.md`:
  - §"Where we are" amendment recording that exact-mask
    production now has an additional upstream source via
    page-stat-narrowed predicate fetch (mask-only
    `PhaseBOutcome` upstream of ADR 060's sparse projection
    planner).
- `plans/wip/roadmap.md`:
  - planning-cadence update at promotion.

## Non-goals

- No new public PBC opcodes; ADR 035 / 036 byte-identical.
- No `RecordBodyCompressionPolicy` variant.
- No new `L2FetchStrategy` variant.
- No new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability`
  bit / IPC `custom_metadata` key / digest domain.
- No new `EncodingRejectCode` / `EncodingFallbackReason` /
  `RowGroupFallbackReason` / `RowGroupRejectCode` /
  `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` / `PredicateAtomOp` /
  `EncodedRowMask` / `EncodingCascadeKind` /
  `EncodingComputeEvaluatorKind` / `EncodedAggregateKind`
  variants.
- No new fields in `RowGroupWriterConfig`,
  `EncodingWriterConfig`, `L2EncodingFetchConfig`,
  `L2RowGroupFetchConfig`, `EncodingComputeConfig`,
  `L2ChunkLayoutConfig`.
- No operator-tunable page-stats budget knob.
- No per-cascade page-stats opt-out beyond the ADR 059
  default allow-list.
- No runtime metadata-budget fallback in V1 (Phase 0
  modeling is the sole discipline).
- No partial per-column metadata-budget heuristic.
- No per-record geometry-keyed page-stats activation.
- No Cargo feature gating page-stats activation.
- No nightly rustc.
- No descriptor surface change.
- No `ObjectStore` trait change.
- No envelope `format_version` bump (ADR 022 unchanged).
- No retirement of ADR 056 row-group stats.
- No migration sweeper for existing slabs (ADR 055 LSM
  compaction handles natural recompacts).
- No Toron-visible contract change.
- No new metric label.
- No internal decline reasons leaking to public surface.
- No third stats class (Bloom / histogram / KLL / HLL).
- No stats for variable-width or nested types.
- No stats for RLE byte-run cascades.
- No stats for FSST / ALP / Plain / Lz4Frame cascades.
- No stats-driven row reordering.
- No dense predicate-zip with sparse projection.
- No nullable sparse projection extension.
- No FSST / ALP / RLE-value-run evaluator work.
- No legacy / tolerant encoding-directory decoder.
- No page-level UTF-8 / Binary predicate stats; row-group
  stats for those types remain ADR 056-only.

## Phase 0 — Contract and size audit

Read-only audit before any code change. **This is a hard
gate**: if size modeling shows representative directory bytes
exceed ADR 044 probe economics on legal fixtures, the ADR is
revised pre-code (candidate revisions: narrower fixed-width
type set, profile-keyed gating, further cascade-eligibility
narrowing). V1 has no runtime backstop — Phase 0 modeling
is the sole budget discipline.

Files:

- `plans/adr/0056_row_group_fetch_and_page_directory.md`,
  `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md`,
  `plans/adr/0058_compute_over_encoded_data.md`,
  `plans/adr/0059_native_cocd_kernels.md`,
  `plans/adr/0060_sparse_projection_fetch_narrowing.md`,
  `plans/adr/0061_page_aligned_authenticated_chunk_layout.md`,
  `plans/adr/0062_row_group_sizing_profile.md`,
  `plans/adr/0063_page_level_predicate_stats.md`.
- `src/storage/row_group.rs`,
  `src/storage/encoding/types.rs`,
  `src/storage/encoding/directory.rs`,
  `src/storage/encoding/page.rs`,
  `src/storage/encoding/page_materialize.rs`,
  `src/storage/encoding/sparse_projection_fetch.rs`,
  `src/storage/encoding/compute/dispatch.rs`,
  `src/storage/multirow.rs`,
  `src/storage/manager.rs`,
  `src/schema/units.rs`.

Work:

1. Confirm `EncodingDirectory` IPC custom_metadata value-shape
   parser is identified (single decode site). Confirm a
   `format_version` constant exists and is the bump target.
2. Confirm the current encoding-directory parser is strict
   (no trailing-field tolerance) so the ADR 063 format-version
   bump does not need a compat decoder.
3. Confirm `CanonicalScalar` helpers (or equivalent) live in
   `src/storage/row_group.rs` and identify the move target
   list (encoding fns, comparison fns, type-tag fns).
4. Inventory `BufferEncoding` construction sites and tests.
5. Inventory exact COCD predicate path:
   `try_read_encoding_compute_phase_b_from_l2`,
   `build_encoding_compute_phase_b_plan` (or the equivalent
   current entry points). Identify the dense-baseline byte
   accounting site.
6. Inventory `RowGroupPredicateKind` (or current shape) for
   reuse at page grain.
7. Inventory ADR 059's default predicate allow-list:
   `EncodingComputeConfig::default().enabled_predicate_evaluators`
   should include `BitPacked`, `DictionaryPlain`,
   `DictionaryBitPacked`, `Delta`. Page-stat emission gate
   keys off this set.
8. **Size-model gate**: model `storage.encoding_directory`
   bytes pre-code on representative fixtures:
   - `PointRead` profile, 4,096-row groups, 4 / 50 / mixed
     columns, every closed ADR 061 chunk size;
   - `Scan` profile, 65,536-row groups, 4 / 50 / mixed
     columns, every closed ADR 061 chunk size;
   - clustered + random predicate distributions;
   - per-page `EncodingPageStats` size: ~16–24 B for fixed-
     width 64-bit (8 + 8 + 4 + framing);
   - cascade-eligibility filter applies (intersection of
     `EncodingComputeConfig::default().enabled_predicate_evaluators`
     and `EncodingCascade::emits_pages_in_v1()`: in V1,
     `BitPacked` + `DictionaryPlain` + `DictionaryBitPacked`);
   - 50-column int64 × `Scan` × ~64 pages × ~50 stats-
     supported × ~20 B = ~64 KiB raw. Apply IPC framing
     factor; confirm fits under ADR 044 64 KiB initial probe
     with `RowGroupColumnStats` already present.
9. **Profile-emission decision**: model the `PointRead`
   profile win/cost ratio on representative fixtures
   (clustered low-selectivity predicates × ~4 pages per
   buffer). Estimate predicate-fetch byte reduction at
   clustered 5%. If the modeled reduction is < 1.5×, narrow
   the V1 emission scope to `Scan`-only **before
   implementation begins** by amending this ADR's writer
   eligibility to include a profile gate. The profile-
   emission scope is fixed in Phase 0; later phases do not
   revisit it.
10. **Size-model gate decision**: if the size model says
    representative legal fixtures exceed the probe envelope,
    **stop and revise the ADR before code**. Candidate
    revisions: narrower fixed-width type set; profile-keyed
    emission (`Scan` only — see step 9); cascade-eligibility
    further narrowing.
11. Confirm composability with ADR 060: sparse projection
    reads `EncodingDirectory.buffer_encodings`; the new
    `page_stats` field on `BufferEncoding` does not break
    downstream consumers (verify decode site).
12. Confirm composability with ADR 062 `enabled = false`
    default: page stats only emitted when row-group writer is
    `enabled = true` AND encoding writer is engaged. Default-
    off path produces zero `EncodingPageStats`.
13. Confirm composability with ADR 052: point-read path
    bypasses Phase B (point reads target specific
    `RecordRowIndex`, not predicate-driven row-group survival).
14. Confirm composability with ADR 055: LSM compaction
    recompacts pre-063 slabs through new shape on natural
    compactions; per-output-slab directory recompute under
    matching writer config preserves cross-replica byte-
    equality.

**Exit gate:** Size model passes on representative fixtures;
profile-emission scope decided (`PointRead`+`Scan` or
`Scan`-only) and pinned in the ADR; surface inventory
recorded; no public negotiation surface needed; council
agrees the ADR scope is page stats + exact-COCD-compatible
predicate-page fetch narrowing + cascade-intersection
eligibility only. The profile-emission scope decided here is
fixed; Phase 8 verifies it empirically but does not revisit
it.

## Phase 1 — Shared canonical-scalar helpers

Move row-group stat-scalar machinery into a shared storage
stats helper without changing any encoding bytes.

Files:

- `src/storage/stats.rs` (new).
- `src/storage/row_group.rs`.

Work:

1. Move `CanonicalScalar` (or current type name) and its
   helper functions from `row_group.rs` to `stats.rs`.
   Keep the type name unchanged unless council requests a
   semantic rename. No `V1` / `V2` suffixes.
2. Move canonical min/max helpers so row-group stats and
   page stats call the same functions. The shared helper
   set covers the union of row-group stat types (per
   `default_stats_supported_types`):
   - signed integer (Int8 / Int16 / Int32 / Int64);
   - unsigned integer (UInt8 / UInt16 / UInt32 / UInt64);
   - Boolean;
   - Timestamp(ms / us / ns) (timezone-naïve);
   - Date32 / Date64;
   - Utf8 / LargeUtf8 (string min/max, row-group only);
   - Binary / LargeBinary (row-group only).
   ADR 063's V1 page-stat type catalog calls only the
   fixed-width subset (no UTF-8 / Binary at page grain).
   No float, no `Time` — those would need a separate
   stats-type ADR.
3. Add helper:

   ```rust
   pub fn canonical_stats_for_array_slice(
       data_type: &DataType,
       array: &dyn Array,
       offset: usize,
       len: usize,
   ) -> std::io::Result<Option<CanonicalStatsValues>>;
   ```

   Returns `None` for unsupported types. The set of
   page-stat-supported types is the fixed-width subset of
   `default_stats_supported_types`: Int8/16/32/64,
   UInt8/16/32/64, Timestamp(ms/us/ns), Date32/64, Boolean.
   UTF-8 / LargeUtf8 / Binary / LargeBinary continue to
   produce row-group stats via the same helper at full-batch
   slice grain but are excluded at page grain in V1.
   `CanonicalStatsValues` carries `(null_count, min, max)`.
4. Update `row_group_stats_for_batch` (or current row-group
   stats entry) to use the shared helper over the full batch
   slice.

Tests:

- Existing row-group directory serialization tests pass
  byte-identically.
- New unit tests verify shared helper returns same
  min/max/null-count as old row-group code for full-batch
  slices.
- Sliced helper tests cover first page, middle page, tail
  page, all-null page, no-null page, and mixed-null page.

**Exit gate:** No row-group stats byte change.
`cargo test --test row_group_directory` passes byte-identically.

## Phase 2 — `BufferEncoding.page_stats` shape

Add the page-stat field to the encoding directory shape and
bump the format version.

Files:

- `src/storage/encoding/types.rs`.
- `src/storage/encoding/directory.rs`.
- `src/schema/units.rs`.
- tests that construct `BufferEncoding` literals.

Work:

1. Add `PageNullCount(u32)` newtype-unit to
   `src/schema/units.rs` via `impl_unit!` macro.
2. Add to `src/storage/encoding/types.rs`:

   ```rust
   #[derive(Clone, Debug, PartialEq, Eq)]
   pub struct EncodingPageStats {
       pub null_count: PageNullCount,
       pub min: Option<CanonicalScalar>,
       pub max: Option<CanonicalScalar>,
   }
   ```

3. Add `pub page_stats: Vec<EncodingPageStats>` to
   `BufferEncoding`.
4. Bump `ENCODING_DIRECTORY_FORMAT_VERSION` in the directory
   serialization site.
5. Update serialization:
   - existing buffer fields written first (unchanged);
   - write `page_stats.len()`;
   - per stat, write `null_count`, `min`, `max` via shared
     `CanonicalScalar` codec.
6. Update deserialization to accept only the new format
   version. Old version → `EncodingDirectoryParseError`.
7. Update validation:
   - `page_stats.is_empty() || page_stats.len() == page_end_offsets.len()`;
   - per-stat scalar tag matches buffer's Arrow type;
   - `null_count <= page_value_count` (with tail-row
     adjustment);
   - if a page has zero non-null values, `min` and `max` are
     `None`;
   - `page_stats` is empty for non-page-emitting buffers
     (`page_end_offsets.is_empty()`);
   - `page_stats` is non-empty only when the cascade is in
     the V1 intersection (ADR 059 default predicate
     allow-list ∩ `EncodingCascade::emits_pages_in_v1()`):
     `BitPacked`, `DictionaryPlain`, `DictionaryBitPacked`.
     Validation reads cascade index → cascade kind →
     intersection membership;
   - `page_stats` is empty for `Delta` (allow-listed but
     not page-emitting in V1), `RlePlain` / `RleBitPacked`
     (page-emitting but not allow-listed), `Plain`,
     `Lz4Frame`, `Fsst`, `Alp`;
   - `page_stats` is empty for variable-width value buffers
     (UTF-8 / LargeUtf8 / Binary / LargeBinary);
   - `page_stats` is empty on validity buffers.
8. Update all test fixtures and builders that construct
   `BufferEncoding` literals.

Tests:

- `encoding_directory_serializes_page_stats` round-trip.
- `encoding_directory_rejects_page_stats_count_mismatch`.
- `encoding_directory_rejects_page_stat_type_mismatch`.
- `encoding_directory_rejects_page_null_count_overflow`.
- `encoding_directory_rejects_page_stats_on_non_page_emitting_buffer`.
- `encoding_directory_rejects_page_stats_on_non_allow_listed_cascade`
  (covers `RlePlain`, `RleBitPacked`, `Plain`, `Lz4Frame`,
  `Fsst`, `Alp`).
- `encoding_directory_rejects_page_stats_on_delta_cascade`
  (allow-listed but not page-emitting in V1; intersection
  excludes it).
- `encoding_directory_rejects_page_stats_on_variable_width_buffer`.
- `encoding_directory_rejects_legacy_format_version` (no
  tolerant decode).

**Exit gate:** Encoding-directory round-trip tests pass on
the new current format. No legacy decoder. No `Option<>`
hedge. No suffix types.

## Phase 3 — Writer page-stat generation

Generate stats for eligible encoded pages while building the
encoding directory. **No runtime metadata-budget fallback in
V1** — the Phase 0 size-model gate is the sole discipline.
Eligibility is the V1 intersection: fixed-width type set ∩
non-empty page geometry ∩ (ADR 059 allow-list ∩
`emits_pages_in_v1()`) ∩ Phase-0-pinned profile scope. All
gates are static / pre-code: there is no per-record budget
branch, no runtime profile-keyed omission, and no partial
per-column heuristic. (The Phase 0 decision to pin
`Scan`-only emission, if taken, is a static eligibility
predicate baked into the writer — not a runtime fallback.)

Files:

- `src/storage/multirow.rs`.
- `src/storage/encoding/directory.rs`.

Work:

1. Extend `build_encoding_directory_for_ipc_stream` (or
   equivalent entry) to receive the row-group writer's
   `stats_supported_types` AND the encoding writer's
   `enabled_predicate_evaluators` (ADR 059 allow-list).
2. For each encoded buffer:
   - **Cascade gate** (intersection): the buffer has
     non-empty `page_end_offsets` (ADR 057 page geometry)
     AND the cascade evaluator is in ADR 059's default
     predicate allow-list. The V1 intersection of the two
     conditions is exactly `BitPacked`, `DictionaryPlain`,
     `DictionaryBitPacked`. `Delta` (allow-listed but not
     page-emitting), RLE byte-run (page-emitting but not
     allow-listed), and other cascades leave `page_stats`
     empty.
   - locate the row group;
   - locate the Arrow column in the source `RecordBatch`
     slice for that row group;
   - confirm the column type is in ADR 063's fixed-width
     page-stat type set (Int8/16/32/64, UInt8/16/32/64,
     Timestamp(ms/us/ns), Date32/64, Boolean);
   - confirm the buffer is a value buffer (not a validity
     buffer, not a variable-width offsets buffer);
   - if the Phase 0 profile-emission decision was
     `Scan`-only, additionally gate on
     `RowGroupSizingProfile::Scan`;
   - expand page descriptors from `BufferEncoding`
     `page_end_offsets` + `page_value_count` (transient,
     planner-only — never serialized);
   - call `canonical_stats_for_array_slice` for each page
     row span;
   - if all pages produce stats AND all eligibility gates
     pass jointly, attach `page_stats`; otherwise leave
     empty.
3. Stats generation is deterministic and depends only on
   the source `RecordBatch` slice, page row spans, type set,
   and V1 cascade intersection (not on encoded byte layout).
4. **No operator config field is added.** Activation is the
   V1 intersection: existing row-group + encoding writer
   toggles, AND fixed-width type set, AND non-empty page
   geometry, AND (ADR 059 allow-list ∩ `emits_pages_in_v1()`),
   AND Phase-0-pinned profile scope.
5. **No runtime metadata-budget branch.** Phase 0 size
   modeling has already cleared representative fixtures pre-
   code, and the profile-emission scope is fixed pre-code;
   there is no writer-side per-record budget fallback, no
   runtime profile-keyed omission rule, no partial per-
   column heuristic, and no operator knob. (The
   Phase-0-pinned profile predicate, if any, is a static
   eligibility gate baked into the writer — not a runtime
   fallback.)

Tests:

- 3-page fixed-width int64 column → exact per-page
  `(min, max, null_count)`.
- Tail page stats use tail row count.
- All-null page records `null_count == page_value_count`
  and `min == max == None`.
- Unsupported type leaves `page_stats` empty (per buffer).
- Variable-width `Utf8` / `Binary` / nested types leave
  `page_stats` empty.
- Non-intersection cascade leaves `page_stats` empty:
  `Plain`, `Lz4Frame`, `Fsst`, `Alp` (neither),
  `RlePlain` / `RleBitPacked` (page-emitting but not
  allow-listed), `Delta` (allow-listed but not
  page-emitting in V1).
- Intersection cascade emits page stats: `BitPacked`,
  `DictionaryPlain`, `DictionaryBitPacked`.
- Cross-replica deterministic encoded output: two writers
  with same (config, input, profile) → byte-identical
  `EncodingDirectory` including `page_stats`.

**Exit gate:** Writer emits page stats deterministically
under the eligibility gates. Pre-063 non-regression: when
`stats_supported_types` is empty (no page-stats-emitting
columns), output byte-identical to pre-063 modulo the
format-version bump byte.

## Phase 4 — Three-state predicate-page classifier

Implement page-level conservative predicate classification
with three-state outcome and null-position-conservative
ExactMatch correctness.

Files:

- `src/storage/encoding/predicate_page_fetch.rs` (new).
- `src/storage/encoding/mod.rs`.

Work:

1. Add module-private types:

   ```rust
   pub(crate) enum PagePredicateClassification {
       Disjoint,
       ExactMatch,
       Maybe,
   }

   pub(crate) struct PredicatePageFetchInput<'a> { ... }

   pub(crate) struct PredicatePageFetchPlan {
       // per (row_group, column) selected page sets,
       // composed Disjoint / ExactMatch / Maybe slices
   }

   pub(crate) enum PredicatePageFetchDecision {
       Narrow(PredicatePageFetchPlan),
       Decline(PredicatePageFetchDecline),
   }

   pub(crate) enum PredicatePageFetchDecline {
       MissingPageStats,
       UnsupportedPredicate,
       UnsupportedCascade,
       PageGeometryMismatchAcrossPredicateColumns,
       NotCheaperThanDense,
       FragmentationCapExceeded,
       MaxRangesExceeded,
       MaxSimdPagesExceeded,
       // … (private; never escapes the module)
   }
   ```

2. Implement classifier for `RowGroupPredicateKind` at page
   grain using page stats. **ExactMatch correctness rules**:
   - equality (`col = K`):
     - `Disjoint` if `K < min OR K > max`;
     - `ExactMatch` only if `min == max == K AND null_count == 0`;
     - else `Maybe`.
   - range (`col BETWEEN A AND B`):
     - `Disjoint` if `B < min OR A > max`;
     - `ExactMatch` only if
       `A <= min AND max <= B AND null_count == 0`;
     - else `Maybe`.
   - inequality (`< K` / `<= K` / `> K` / `>= K`):
     - same shape with one-sided bound;
     - `ExactMatch` requires bound-proven AND `null_count == 0`.
   - `IS NULL`:
     - `ExactMatch` if `null_count == page_value_count`;
     - `Disjoint` if `null_count == 0`;
     - else `Maybe`.
   - `IS NOT NULL`:
     - `ExactMatch` if `null_count == 0`;
     - `Disjoint` if `null_count == page_value_count`;
     - else `Maybe`.
   - AND:
     - `Disjoint` if any branch Disjoint;
     - `ExactMatch` only if every branch ExactMatch;
     - else `Maybe`.
   - OR:
     - `Disjoint` only if every branch Disjoint;
     - `ExactMatch` if any branch ExactMatch;
     - else `Maybe`.
   - LIKE / IN / BETWEEN-with-cast / unsupported leaves →
     `Maybe`.
   - missing / type-mismatched / absent stats → `Maybe`.
   - V1 closed type catalog is fixed-width with exact min/max
     → no truncation rules needed.
3. **Why null-position-conservative ExactMatch is required**:
   for value comparisons on nullable columns, even if min/max
   prove every non-null value matches, building the correct
   mask requires marking null rows false (per Arrow null-
   semantics). Doing so requires the validity buffer, which
   defeats the optimization. So pages with `null_count > 0`
   are `Maybe` for value comparisons.
4. Reuse row-group stat comparison rules from
   `src/storage/stats.rs` shared helpers.
5. Decline at planner level if any required predicate column
   lacks page stats, page-emitting cascade, allow-listed
   cascade, compatible page geometry, or COCD-compatible
   evaluator coverage.

Tests:

- Equality `Disjoint` / `ExactMatch` / `Maybe` across all
  three outcomes; `null_count > 0 + min == max == K` →
  `Maybe`, not `ExactMatch`.
- Range `BETWEEN` with stats fully inside / fully outside /
  overlapping bounds; `null_count > 0 + bounds-pass` →
  `Maybe`.
- AND / OR composition rules.
- `IS NULL` boundaries: all-null → `ExactMatch`; no-null →
  `Disjoint`; mixed → `Maybe`.
- `IS NOT NULL` dual boundaries.
- Missing page stats → `Maybe`.
- Truncated stats (deferred type) — not encountered in V1
  catalog; test asserts reachability is empty.
- Property test: classifier never returns `ExactMatch` when
  null-position bytes would be needed to build the correct
  mask (oracle: dense-evaluation result XOR all-true mask
  is always zero for ExactMatch pages).

**Exit gate:** Classifier property tests show no false
negatives against dense PBC evaluation on generated fixed-
width fixtures across the type catalog. ExactMatch with-nulls
property test confirms `null_count > 0` value-comparison
pages classify `Maybe`.

## Phase 5 — Predicate-page fetch planner + mask stitching

Use the page classifier to fetch and evaluate only Maybe
encoded pages; stitch full row-group masks.

Files:

- `src/storage/encoding/predicate_page_fetch.rs`.
- `src/storage/manager.rs`.
- possibly `src/storage/encoding/compute/dispatch.rs`.

Work:

1. Build `PredicatePageFetchPlan` from selected row groups,
   predicate tree, page directory, encoding directory, and
   dense predicate-fetch baseline byte count.
2. Plan selected encoded-page body ranges only for `Maybe`
   pages via existing `encoded_page_body_range` /
   page-descriptor helpers (transient, computed from
   `page_end_offsets`).
3. Coalesce selected ranges via existing record-body range
   coalescer.
4. Apply economic gates:
   - `selected_predicate_plaintext_bytes < dense_predicate_plaintext_bytes`;
   - `selected_page_count <= L2EncodingFetchConfig.max_simd_pages_per_record`;
   - `selected_range_count <= L2RowGroupFetchConfig.max_ranges_per_record`;
   - `selected_byte_fraction` does not force known fallback.
5. Execute fetch via existing `TieredMultiRangeReadPlan`
   (ADR 044 + ADR 061 macro-block coalescing).
6. Evaluate COCD kernels (ADR 058 / 059) on each Maybe page.
7. Stitch full-row-group `EncodedRowMask`:
   - Disjoint pages → contribute `EncodedRowMask::None { rows }`;
   - ExactMatch pages → contribute `EncodedRowMask::All { rows }`;
   - Maybe pages → contribute kernel result, sliced to page
     row offsets;
   - groups with no candidate pages → `EncodedRowMask::None`
     for the whole group.
   Use existing `EncodedRowMask::{All, None, Bitmap, Runs}`
   algebra; no new variant.
8. Return mask-only `PhaseBOutcome`; no
   `decoded_predicate_batches`.
9. If planner declines pre-fetch → existing dense predicate
   path runs unchanged.
10. If post-fetch invariant fails (parse, decoded-length
    mismatch, etc.) → fail closed via existing storage-error
    / reject taxonomies. Never silent dense-retry.

Tests:

- Narrowed survival mask byte-identical to full-encoded-
  predicate mask for clustered equality.
- Narrowed survival mask byte-identical for clustered range.
- All-pages-Disjoint: empty fetch, all-`None` mask,
  zero predicate bytes recorded.
- All-pages-ExactMatch (no-nulls fixture): empty fetch,
  all-`All` mask, zero predicate bytes recorded.
- All-pages-Maybe: declines or stays within ≤ 5% dense-
  baseline overhead.
- Sparse projection fetch (ADR 060) still engages downstream
  because Phase B output is mask-only.
- ExactMatch correctness with nulls: nullable column × range
  predicate fixture verifies pages with `null_count > 0`
  stay `Maybe`; mask byte-identical to dense path.

**Exit gate:** `cargo test --test encoding_predicate_page_fetch`
passes; existing encoded-compute tests stay green;
`cargo test --test row_group_sparse_projection_fetch` passes
with new ADR 063 narrowed-predicate fixture.

## Phase 6 — Manager integration and fallback preservation

Thread the narrowed path into the production row-group reader.

Files:

- `src/storage/manager.rs`.
- `src/storage/columnar.rs` (if strategy observation needs
  existing-field adjustments — verify in Phase 0).

Work:

1. In the row-group predicate-evaluation path, compute the
   dense predicate-body-range baseline as today.
2. Before issuing the dense predicate fetch, ask the
   predicate-page planner for a narrowed plan.
3. If narrowed plan engages:
   - fetch narrowed predicate pages;
   - build mask-only `PhaseBOutcome`;
   - keep `phase_b_fetch` populated with actual narrowed
     selected ranges and ciphertext bytes.
4. If narrowed plan declines:
   - existing full predicate fetch runs unchanged.
5. `RowGroupFetchPlan` shape unchanged.
6. Existing metric labels unchanged. Existing `phase_b` byte
   and selected-ranges counters absorb narrowed fetches.

Tests:

- Existing row-group fetch tests pass unchanged when page
  stats are absent (e.g., disabled-writer path).
- Page stats present but unhelpful (Maybe-only) → existing
  dense path engages.
- Helpful page stats → fewer
  `phase_b_fetch.selected_plaintext_ranges` bytes than
  dense baseline.
- ExactMatch pages → no predicate-page bytes for those pages,
  while final mask is byte-identical.
- `L2FetchStrategy` remains `RowGroup`.

**Exit gate:** No new public strategy / fallback surface.
Pre-063 slabs do not reach this code path post-recompact;
test fixtures are produced by the new writer only (no compat
decoder).

## Phase 7 — Architectural lockdown

Land ADR 063 guards.

Files:

- `tests/architecture_test.rs`.

Guards:

- `EncodingPageStats` struct shape pinned (three fields).
- `BufferEncoding.page_stats: Vec<EncodingPageStats>` is the
  only new public field.
- `ENCODING_DIRECTORY_FORMAT_VERSION` is bumped from the
  ADR 057 value.
- No `EncodingDirectoryV2` / `EncodingPageStatsV1` /
  `BufferEncodingV2` / similar suffix types.
- No tolerant or legacy encoding-directory decoder; decoder
  accepts only the new current version.
- No second IPC `custom_metadata` key for page stats.
- `CanonicalScalar` (or equivalent) lives in
  `src/storage/stats.rs`; row-group + page stats import
  from the same module (single-source guard).
- `PageNullCount` is a `#[repr(transparent)]` newtype-unit
  in `src/schema/units.rs`.
- `EncodingPageStats` lives in
  `src/storage/encoding/types.rs`.
- Predicate-page planner lives at
  `src/storage/encoding/predicate_page_fetch.rs`.
- `PredicatePageFetchDecline` is private to the planner
  module.
- Production row-group read path calls predicate-page
  planner before dense predicate fetch.
- **No runtime metadata-budget fallback in V1.** Phase 0
  modeling is the sole budget discipline. The full V1
  static eligibility chain is: fixed-width type set ∩
  non-empty page geometry ∩ (ADR 059 default predicate
  allow-list ∩ `EncodingCascade::emits_pages_in_v1()`) ∩
  Phase-0-pinned profile scope. All four conjuncts are
  static / pre-code. Architectural test rejects any
  writer-side per-record budget branch, runtime
  profile-keyed omission rule, or partial per-column
  heuristic — i.e., any conditional page-stats emission
  *outside* the static V1 chain. A Phase-0-pinned
  `Scan`-only profile predicate baked into the writer is
  inside the chain and permitted; runtime fallbacks that
  vary per record are not.
- **Cascade-eligibility set for page-stat emission is the
  intersection of ADR 059's default predicate allow-list
  AND `EncodingCascade::emits_pages_in_v1()`**: in V1,
  `BitPacked`, `DictionaryPlain`, `DictionaryBitPacked`.
  Architectural test asserts the writer's emission set
  matches the runtime intersection of
  `EncodingComputeConfig::default().enabled_predicate_evaluators`
  AND `emits_pages_in_v1()`. `Delta` (allow-listed but
  not page-emitting), `RlePlain` / `RleBitPacked`
  (page-emitting but not allow-listed), and all other
  cascades carry empty `page_stats`.
- No new `L2FetchStrategy` / `EncodedRowMask` /
  `EncodingCascadeKind` / `EncodingFallbackReason` /
  `EncodingRejectCode` / `RowGroupFallbackReason` /
  `RowGroupRejectCode` / `EncodingComputeFallbackReason` /
  `EncodingComputeRejectCode` / `PredicateAtomOp` /
  `EncodingComputeEvaluatorKind` / `EncodedAggregateKind`
  variant.
- No new wire `MsgType` / `FRAME_FLAG_*` /
  `NodeCapability` bit / digest domain / `ObjectStore`
  trait method.
- No new descriptor field / fingerprint surface change.
- No new public PBC opcode.
- No new operator config field.
- No new metric label.
- ADR 020 §2 AAD layout unchanged.
- ADR 022 §3 / §5 byte layouts unchanged.
- ADR 056 `RecordPageDirectory` shape unchanged.
- ADR 061 closed chunk-size set unchanged.
- ADR 062 `RowGroupSizingProfile` catalog unchanged.
- Cross-replica byte-equal output across two writers with
  same (config, input, profile), including page stats.
- No Cargo feature gating page-stats activation.
- No nightly rustc.
- No version suffix on new types.
- No planning-phase identifier on new symbols.
- No project-name prefix.

**Exit gate:** `cargo test --test architecture_test` passes.

## Phase 8 — Bench matrix

Run the bench at both `PointRead` / `Scan` profiles across
the predicate-selectivity matrix. Land the evidence artifact
at `plans/impl/0063_page_level_predicate_stats_bench_results.md`.

Bench fixtures:

- **Recompact equivalence**: ADR 055 LSM compaction takes a
  pre-063 slab through new shape; output stats match
  deterministic expected stats over the same logical input.
- **Predicate selectivity sweep at `Scan` profile**:
  - clustered: 1%, 5%, 10%, 25%;
  - random-uniform: 1%, 5%, 10%, 25%;
  - sorted-broad (ExactMatch heavy): predicate aligned with
    row order, no-nulls fixture;
  - all-pages-Disjoint and all-pages-ExactMatch boundary
    rows.
- **Schema-shape sweep at `Scan` profile**:
  - `65,536 × 4 int64` (ADR 061 Phase 9 fixture);
  - `65,536 × 50 int64` (wide schema);
  - mixed analytical schema (int / timestamp / date /
    boolean);
  - schema with non-stats-supported columns mixed in (e.g.,
    Utf8 / Float64 / nested) — those buffers carry empty
    `page_stats`;
  - schema with non-allow-listed cascades mixed in (RLE /
    Plain / Lz4Frame).
- **`PointRead` profile sweep**: same selectivity matrix at
  4K-row groups; ~4 pages per buffer.
- **Chunk-size cross-product**: every closed ADR 061 chunk
  size × `Scan` profile.
- **ExactMatch correctness with nulls**: nullable column ×
  range predicate; pages with `null_count > 0` classify
  `Maybe`, mask byte-identical to dense path.
- **Phase 0 model verification**: bench reproduces the
  Phase 0 size-model on the largest legal `Scan` and
  `PointRead` fixtures; actual directory bytes match
  modeled bytes within ≤ 5%. If model drift exceeds 5%,
  revisit modeling methodology, not runtime.
- **Boundary fixtures**:
  - single-row-group `Scan` at exact target (one-entry
    directory); page stats present;
  - sub-target `Scan` (no paging); no page stats;
  - target-plus-one `Scan` (two row groups); page stats per
    group.
- **Type coverage**: every fixed-width type in ADR 063's
  V1 catalog (Int8/16/32/64, UInt8/16/32/64,
  Timestamp(ms/us/ns), Date32/64, Boolean). UTF-8 /
  LargeUtf8 / Binary / LargeBinary buffers confirmed empty
  `page_stats` (row-group stats unchanged).
- **Cascade-eligibility coverage**: `BitPacked`,
  `DictionaryPlain`, `DictionaryBitPacked` carry page
  stats; `Delta` carries empty `page_stats` (allow-listed
  but not page-emitting in V1); `RlePlain`, `RleBitPacked`
  carry empty `page_stats` (page-emitting but not
  allow-listed); `Plain`, `Lz4Frame`, `Fsst`, `Alp` carry
  empty `page_stats`.

Bench rows record:

- profile, predicate selectivity, distribution shape;
- `EncodingDirectory` bytes before / after page stats;
- combined row-group + encoding metadata bytes;
- metadata probe windows needed;
- dense predicate plaintext bytes;
- narrowed predicate plaintext bytes;
- dense predicate ciphertext bytes;
- narrowed predicate ciphertext bytes;
- selected page count, selected range count;
- Disjoint / ExactMatch / Maybe page counts;
- ExactMatch short-circuit row count and byte savings;
- survival-mask equality digest;
- projection output equality digest;
- writer wall time, materialization wall time;
- dense-fallback overhead;
- composability metrics: ADR 060 projection-fetch byte count
  unchanged; ADR 061 p50 / p95 GET payload unchanged;
  ADR 052 point-read selected bytes unchanged;
  ADR 058 / 059 COCD eval byte-identical mask + aggregate-
  partial.

Acceptance gates:

- **Predicate-fetch byte reduction**: clustered 5%
  selectivity ≥ 3× plaintext-byte reduction vs row-group-
  only baseline on `Scan` profile (headline);
  clustered 25% ≥ 1.5×.
- **Random-uniform**: ≤ 5% dense-fallback overhead;
  reduction ≤ 10% recorded as not-gated.
- **ExactMatch effectiveness**: sorted-broad fixture
  records ≥ 25% of pages classified ExactMatch and the
  predicate-fetch byte count drops accordingly. All-ExactMatch
  fixture records zero predicate fetch bytes.
- **ExactMatch correctness with nulls**: nullable-column
  fixture confirms `null_count > 0 + bounds-pass` pages
  classify `Maybe`; mask byte-identical to dense.
- **Combined metadata**: `storage.row_group_directory +
  storage.encoding_directory` ≤ 64 KiB initial-probe
  envelope on every representative legal fixture; combined
  page-stats fraction recorded; Phase 0 model verified
  ≤ 5% drift.
- **Writer cost**: writer wall time within ADR 057 / 062
  envelope on largest legal fixture (or council-approved
  revision recorded).
- **Compression loss**: zero (page stats live in directory,
  not encoded buffers).
- **Composability**: ADR 060 projection output byte-
  identical; ADR 061 GET payload unchanged; ADR 052 point-
  read selected bytes unchanged; ADR 058 / 059 COCD output
  byte-identical.
- **`PointRead` profile** (hard gate, conditioned on Phase
  0 decision): if Phase 0 decided `PointRead`+`Scan`
  emission, predicate-fetch reduction ≥ 1.5× at clustered
  5% on `PointRead` fixtures. If Phase 0 decided
  `Scan`-only emission, this gate is moot (PointRead does
  not emit page stats; PointRead-shape fixtures verify
  byte-identical pre-063 behavior). The profile-emission
  scope is fixed in Phase 0 and not revisited at promotion.
- **Cross-replica determinism**: byte-equal output across
  two writers with same (config, input, profile).
- **Boundary disposition**: one-entry-directory `Scan`
  fixture has page stats present and consumable.
- **Survival-mask byte-equality**: narrowed-path mask
  byte-identical to dense-path mask.
- **All-Disjoint / all-ExactMatch fixtures**: zero
  predicate fetch bytes; correct mask shape.
- **Type and cascade coverage** (explicit gate): every
  fixed-width type in ADR 063's V1 catalog paired with
  every cascade in the V1 intersection (`BitPacked`,
  `DictionaryPlain`, `DictionaryBitPacked`) carries page
  stats; variable-width buffers (UTF-8 / LargeUtf8 /
  Binary / LargeBinary), validity buffers, allow-listed-
  but-not-page-emitting cascades (`Delta` in V1),
  page-emitting-but-not-allow-listed cascades (`RlePlain`,
  `RleBitPacked`), and all other cascades (`Plain`,
  `Lz4Frame`, `Fsst`, `Alp`) carry empty `page_stats`.

**Exit gate:** Every promotion-criterion bench row passes
its gate. Bench artifact basis for council promotion
decision.

## Phase 9 — Promotion

Promote ADR 063 from council-converged → shipped after code,
tests, and bench evidence land together.

Files:

- `plans/impl/0063_page_level_predicate_stats_bench_results.md`
  — Phase 8 evidence artifact.
- `plans/adr/0063_page_level_predicate_stats.md` — Status
  amendment to "shipped" with bench evidence reference.
- `plans/adr/0056_row_group_fetch_and_page_directory.md` —
  §"Where we are" amendment referencing Phase B page-grain
  pruning.
- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md` —
  §"Where we are" amendment referencing
  `BufferEncoding.page_stats` field + format-version bump.
- `plans/adr/0060_sparse_projection_fetch_narrowing.md` —
  §"Where we are" amendment recording that exact-mask
  production now has an additional upstream source via
  page-stat-narrowed predicate fetch (mask-only
  `PhaseBOutcome` upstream of ADR 060's sparse projection
  planner).
- `plans/wip/roadmap.md` — planning-cadence amendment;
  ADR 063 marked shipped.

Work:

1. Pin the bench-validated parameters (Phase 0 model drift,
   PointRead-profile decision, V1 cascade intersection).
2. Amend ADR 056's where-we-are to distinguish row-group-
   grain and page-grain Phase B prune.
3. Amend ADR 057's where-we-are to reference the
   `BufferEncoding.page_stats` field and the format-version
   bump.
4. Amend ADR 060's where-we-are to record the additional
   upstream source for exact masks (page-stat-narrowed
   predicate fetch produces mask-only `PhaseBOutcome` that
   feeds ADR 060's sparse projection planner directly).
5. Mark ADR 063 shipped only after code, tests, and bench
   evidence land together.
6. Update the roadmap at the planning-cadence boundary.

**Exit gate:** ADR 063 promoted as ADR 056 + ADR 057
amendment with one additive directory field + one format-
version bump. All ADR 056 / 057 / 058 / 059 / 060 / 061 /
062 focused tests stay green at both profiles.

## Verification gate

Before promotion:

```sh
cargo test --test architecture_test
cargo test --test encoding_directory
cargo test --test encoding_predicate_page_fetch
cargo test --test encoding_materialize
cargo test --test encoding_compute_test
cargo test --test encoding_compute_kernel_invariants
cargo test --test row_group_directory
cargo test --test row_group_fetch_plan
cargo test --test row_group_sparse_projection_fetch
cargo test --test row_group_writer
cargo test --test encoding_chunk_layout
cargo test --test crypto_macro_block_coalescing
cargo test --test l2_remote
cargo test --test l2_rehydration
cargo test --test multirow
cargo test --test config_loader
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench row_group_fetch
git diff --check
git diff --cached --check
```

## Risks and mitigations

**Directory metadata exceeds ADR 044 probe envelope.**
Mitigation: Phase 0 size-model gate (pre-code). If
representative legal fixtures miss the model, ADR 063 is
revised before code lands (candidate revisions: narrower
type set, profile-keyed gating, further cascade narrowing).
V1 has no runtime budget fallback per council convergence
(Wendelon house style: gate-driven, not runtime-fallback-
driven). Edge-case pathological schemas discovered post-
promotion are revisit-ADR territory.

**Page-stats compute cost regresses writer wall time.**
Mitigation: Phase 8 writer-cost gate within ADR 057 / 062
envelope. Stats compute is single-pass over the array per
page boundary; amortized cost is small relative to encoding
work.

**Page-stats prune produces highly fragmented byte ranges
on random-uniform predicates.** Mitigation: ADR 044
coalescer absorbs fragmentation into chunk-aligned reads;
ADR 061 macro-block coalescing collapses contiguous runs.
Bench records fragmentation as not-gated for random-uniform.

**`CanonicalScalar` shape divergence between row-group and
page stats.** Mitigation: shared module + architectural-test
single-source guard; row-group bytes byte-identical pre /
post helper move.

**`EncodingDirectory` decode breaks on additive
`page_stats` field.** Mitigation: Phase 0 contract audit
verifies decoder shape; format-version bump enforces strict
new-format-only decode.

**Cross-replica byte-equality breaks on stats compute non-
determinism.** Mitigation: stats compute over (Arrow array
slice, page boundaries, type set, V1 cascade intersection,
Phase-0-pinned profile scope) is deterministic; no runtime
branches that could vary across replicas; Phase 3 + Phase 8
cross-replica bench rows gate empirically.

**Phase B fetch produces too many small ranges on highly
selective predicates.** Mitigation: ADR 044 coalescer +
ADR 061 macro-block coalescing collapse small ranges;
existing `max_ranges_per_record` cap enforces an absolute
fallback ceiling.

**False page skip corrupts results.** Mitigation:
conservative classifier; ExactMatch requires `null_count == 0`
for value comparisons (no null-position byte requirement);
property tests against dense PBC oracle on generated
fixtures; fail-closed post-fetch invariants.

**ExactMatch construction mishandles nulls.** Mitigation:
ExactMatch correctness rule explicit per predicate kind;
property test confirms classifier never returns ExactMatch
when null-position bytes would be needed; bench fixture
records nullable-column ExactMatch behavior.

**Cascade-eligibility set drifts from ADR 059 default
allow-list.** Mitigation: architectural test asserts the
eligibility set matches
`EncodingComputeConfig::default().enabled_predicate_evaluators`.
If ADR 059's allow-list changes, ADR 063's eligibility
follows automatically.

**Page-stats prune regresses ADR 052 secondary-index point
reads.** Mitigation: Phase 0 confirms point-read path
bypasses Phase B; Phase 8 bench gate verifies empirically.

**Dense predicate-zip with sparse projection sneaks in.**
Mitigation: narrowed path returns mask-only `PhaseBOutcome`
with empty `decoded_predicate_batches`. Architectural-test
guard pins shape.

**RLE / FSST / ALP / Delta scope creep.** Mitigation: V1
cascade-eligibility set is the **intersection** of ADR 059's
default predicate allow-list AND
`EncodingCascade::emits_pages_in_v1()` — in V1, this is
`BitPacked`, `DictionaryPlain`, `DictionaryBitPacked`. RLE
byte-run is page-emitting but not allow-listed (Phase 10
evidence missed the gate); FSST / ALP / Plain / Lz4Frame are
neither allow-listed nor page-emitting; `Delta` is allow-
listed but not page-emitting in V1 (ADR 063 does not amend
ADR 057's page emission). All four cases carry empty
`page_stats`. A future cascade enters the V1 intersection
only when both sets include it — e.g., a future ADR adding
page emission for `Delta` AND ADR 059's allow-list still
including `Delta`. ADR 063's emission set follows the
intersection automatically; it does not unilaterally add
cascades.

**Fallback taxonomy expansion.** Mitigation:
`PredicatePageFetchDecline` is private; architectural-test
pin; existing `phase_b` metrics absorb narrowed-fetch
decisions.

**Runtime metadata-budget fallback sneaks in.** Mitigation:
architectural-test guard rejects any conditional page-stats
emission outside the V1 static eligibility chain (fixed-width
type set ∩ non-empty page geometry ∩ (ADR 059 allow-list ∩
`emits_pages_in_v1()`) ∩ Phase-0-pinned profile scope). A
Phase-0-pinned `Scan`-only predicate is inside the chain;
runtime per-record fallbacks are outside it. Phase 0
size-modeling artifact is the V1 budget discipline; if it
fails on representative fixtures, the ADR is revised before
code lands.

**ADR 063 leaks into public protocol surfaces.** Mitigation:
Phase 7 architecture-test guards.
