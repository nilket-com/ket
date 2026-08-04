# Implementation Plan: ADR 070 — All-Null Page-Skip for Nullable Sparse Projection

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/0070_all_null_page_skip_sparse_projection.md`

Bench-results artifact (post-promotion):
`plans/impl/0070_all_null_page_skip_sparse_projection_bench_results.md`

Inherits substrate:

- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md` —
  `EncodingCascade`, `EncodingDirectory`, `BufferEncoding`,
  `page_end_offsets`. ADR 070 bumps no format version.
- `plans/adr/0060_sparse_projection_fetch_narrowing.md` —
  per-page byte-range fetch planner, multi-range
  coalescing, sparse-vs-dense byte gate, private planner-
  decline discipline.
- `plans/adr/0061_page_aligned_authenticated_chunk_layout.md` —
  AEAD chunk coalescing; ADR 070 preserves macro-block
  economics by avoiding any two-stage probe.
- `plans/adr/0063_page_level_predicate_stats.md` — additive
  `BufferEncoding.page_stats: Vec<EncodingPageStats>` field
  with per-page `null_count`. ADR 070 is a consumer; no
  shape change.
- `plans/adr/0064_nullable_sparse_projection.md` — raw
  validity fetch + rebase; nullable Arrow assembly path.
  ADR 070 lifts ADR 064's all-null deferral guard.
- `plans/adr/0065_dense_predicate_zip_with_sparse_projection.md`
  — manager-side three-case dispatch; page-local mask
  filtering.
- `plans/adr/0069_encoded_validity_sparse_projection.md` —
  encoded-validity decode; bitmap-specific page decode
  helper. ADR 070 lifts ADR 069's pinned "ADR 070
  boundary" guard.
- `plans/conventions/` — newtype-units, no-version-
  suffixes, no-planning-phase-names, no-premature-
  backward-compat.

## Status

Council-converged across Claude / Codex / Gemini after four
iterations under `plans/wip/`. Pre-implementation; Phase 0
calibration items pending.

## Implementation contract

- ADR 064 / 069 paths byte-identical for every page where
  elision declines.
- V1 elision trigger: `value_buf.page_stats[k].null_count ==
  page_value_count[k]`.
- Elided pages produce zero selected bytes and zero ranges
  for both the value buffer and the validity buffer.
- Synthesized value buffer is `page_row_count ×
  bytes_per_value` zero bytes (V1 fixed-width primitives
  only; Boolean is V2).
- Synthesized validity bitmap is `ceil(page_row_count / 8)`
  zero bytes with `null_count = page_row_count`.
- Planner stores per-page disposition in
  `BTreeMap<PageMaterializeBufferKey,
  SelectedPageDisposition>`.
- Architecture guard relocates from ADR 069's "no all-null
  elision" to ADR 070's five-part invariant set.
- Promotion oracle = canonical-null logical equality for
  elided pages + byte-identical for non-elided.
- Test-only drift sentinel: fixture-side validity-byte audit
  for elided pages (hard gate).
- Range count and macro-block GET count are recorded per
  bench row; regressions are justified by accompanying byte
  reduction. ADR 060 / 061 coalescers merge only
  overlapping/adjacent ranges, so eliding a middle range
  can split one coalesced run into two even when total
  bytes drop. Promotion does not gate on GET count never
  increasing.
- No new public API; no new `EncodingCascade` /
  `EncodingComputeEvaluatorKind` / `L2FetchStrategy`
  variant; no IPC `custom_metadata` key; no
  `ENCODING_DIRECTORY_FORMAT_VERSION` bump.
- Deterministic across replicas.
- No writer change (consumes ADR 063's existing field).

## Goal

Elide both value-page and validity-page fetches for selected
nullable fixed-width pages metadata-proven all-null by ADR
063. Lift ADR 064 / 069's all-null deferral guard under a
strict metadata-only eligibility key. Preserve mask-first
ordering, ADR 061 macro-block coalescing, and closed-system
writer correctness via the inherited ADR 063 trust contract
plus a test-only drift sentinel.

## Phase plan

### Phase 0 — Audit, oracle pin, and calibration

Pre-implementation pass. No code lands. Deliverable: a
"Phase 0 audit" section in the bench-results artifact
pinning each item.

- **P-fork (oracle contract).** Council has adopted
  canonical-null logical equality for elided pages and
  byte-identical for non-elided. Phase 0 records the
  formal pin alongside P2 fixture evidence.
- **P-fork (writer scope).** Audit whether ADR 063 page
  stats emit per-page `null_count` on projection-only value
  buffers (not only on predicate-evaluated columns).
  Expected: yes (ADR 063 is per-buffer, per-cascade, per-
  type — not per-role). If unexpectedly no, V1 narrows to
  predicate-evaluated projection columns only.
- **P1: Eligibility catalog.** Document V1 cascade × type
  intersection: {`BitPacked`, `DictionaryPlain`,
  `DictionaryBitPacked`} × {Int8-64, UInt8-64,
  Timestamp(ms/us/ns), Date32/64}. Confirm match with
  ADR 063 V1 ∩ ADR 060 / 064 sparse-projection
  eligibility (`fixed_width_value_bytes` in
  `src/storage/encoding/page_materialize.rs`). Boolean is
  in ADR 063's stats catalog but excluded by
  `fixed_width_value_bytes`; V1 inherits the exclusion.
  Boolean elision is V2.
- **P2: Dense null-slot audit.** Build a nullable fixed-
  width fixture whose null slots carry non-zero value
  bytes; prove dense fallback preserves those bytes. This
  formally justifies canonical-null oracle adoption. If
  dense surprisingly does canonicalize, the bench may
  report bonus byte-identical equality on elided pages;
  the contract remains canonical-null.
- **P3: Materializer zero-byte synthesis cost.**
  Microbenchmark `vec![0u8; n]` allocation + zero-fill vs.
  small-page object-store fetch. Expected: synthesis
  microseconds; fetch milliseconds with RTT.
- **P4: ADR 060 multi-range coalescing.** Confirm elided
  pages do not register byte ranges for either buffer and
  the sparse-vs-dense byte gate counts them as zero.
  Record whether range / GET counts change after elision;
  P12 acknowledges that removing a middle range can split
  a coalesced run.
- **P5: Cross-replica determinism probe.** Two-process
  elision on identical clustered-all-null fixtures; byte-
  identical fetch plans and canonical-null-normalized
  Arrow output.
- **P6: Architecture-guard literal text.** Draft exact
  Rust predicates / callsite assertions for the five-part
  invariant set:
  1. Elision reachable only via
     `SelectedPageDisposition::AllNullElided`.
  2. `AllNullElided` populated only via
     `value_page_is_metadata_all_null`.
  3. Per-column key
     `(row_group, column, value_buffer, page)`.
  4. Non-nullable forbidden.
  5. Synthesis shape (exact Arrow memory shape; zero
     bytes).
- **P7: Fixture identification.** Clustered all-null on
  `Int64`; alternating-pages-all-null; clustered-5%-null
  re-using ADR 064 / 069 fixtures; random-50% (no elision
  expected); two-nullable-column divergent.
- **P8: Bench home.** Pinned to
  `benches/row_group_fetch.rs` (read-scheduling axis is
  the headline for ADR 070). Optional
  `benches/encoding_compute.rs` spot-check rows for ADR
  069 continuity on selected-bytes axis.
- **P9: Disposition storage shape.** Pinned to
  `BTreeMap<PageMaterializeBufferKey,
  SelectedPageDisposition>` on
  `SparseProjectionFetchPlan`. Phase 1 picks between
  storing `Materialize` entries explicitly (assertion
  clarity) and treating missing keys as `Materialize`
  (elided-only map). Both forms are contract-equivalent.
- **P10: ADR 063 enablement.** Confirm bench harness can
  enable ADR 063 for elision rows and preserve a disabled
  control row.
- **P11: ADR 065 Case B handoff.** Confirm manager-side
  zip operates on synthesized canonical-null buffers
  without additional plumbing — the Arrow batch produced
  by `projected_page_batch_from_decoded_buffers` for an
  elided page has the same shape as for a non-elided page
  (just zero contents and zero validity bits).
- **P12: Range / GET count behavior under elision.**
  Audit ADR 060 / 061 coalescers
  (`sparse_projection_fetch.rs` range merger,
  `crypto.rs` chunk coalescer). Both merge only
  overlapping/adjacent ranges; eliding a middle range can
  split one coalesced run into two. V1 records range /
  GET counts on every bench row and reports regressions
  explicitly with the byte reduction that motivated them;
  promotion does not gate on GET count never increasing.
  Bridge-overfetch logic (retain coalescing when GET
  count would regress) is V2 territory.
- **P13: Per-column independence audit.** Two-nullable-
  column divergent fixture as audit (not just test): one
  column elides, another fetches, same page span.
- **P14: Test-only drift sentinel pre-check.** Pre-
  implementation, audit existing ADR 063 fixtures: for
  each page tagged `null_count == page_value_count`,
  independently decode the raw or ADR 069 encoded
  validity bytes (test-side) and assert observably all-
  zero over `page_value_count` bits. If the writer
  invariant holds across all existing fixtures, the
  trust contract is empirically validated. If a fixture
  surfaces a divergence, fix the writer before ADR 070
  lands. This Phase 0 pre-check is the strongest pre-code
  signal we have for the inherited trust contract's
  actual production state.
- **P15: Soft-gate threshold calibration.** Replace
  placeholder thresholds (2.0× / 1.5× / 5% / 55% / 1% /
  2%) with values derived from representative-fixture
  modeling.

### Phase 1 — Page-stat helper and synthesis helpers

Lands private helpers and focused tests before planner
wiring. Production behavior is byte-identical.

- Add helper in
  `src/storage/encoding/sparse_projection_fetch.rs`:

  ```rust
  pub(crate) fn value_page_is_metadata_all_null(
      value_buf: &BufferEncoding,
      page: EncodingPageDescriptor,
  ) -> bool;
  ```

  Rules:
  - Return `false` if `value_buf.page_stats` is empty.
  - Return `false` if `page.page_index >=
    page_stats.len()`.
  - Return `false` if the indexed stat lacks materialized
    `null_count` (ADR 063 may carry sparse / optional
    stats per cascade).
  - Return `false` on malformed descriptor.
  - Return `true` iff `null_count == page.value_count`.

  The helper does **not** validate cascade × type catalog
  membership; its signature does not carry `DataType` or
  a resolved `EncodingCascade`. The planner checks
  catalog membership before invoking the helper (see
  Phase 2) and records catalog declines via
  `AllNullElisionDeclineReason`.

- Add private diagnostic enum in the same module:

  ```rust
  pub(crate) enum AllNullElisionDeclineReason {
      NotNullable,
      ColumnHasNoNulls,
      DataTypeNotInCatalog,
      MissingPageStats,
      PageStatsNotAllNull,
      CorruptPageDescriptor,
  }
  ```

  Diagnostic-only; collapses to no public surface.
  Production behavior continues normal sparse projection
  when elision declines. Cascade out-of-catalog remains part
  of existing sparse-projection eligibility instead of this
  post-eligibility all-null diagnostic.

- Add disposition enum:

  ```rust
  pub(crate) enum SelectedPageDisposition {
      Materialize,
      AllNullElided {
          page_row_count: usize,
          bytes_per_value: usize,
      },
  }
  ```

- Add synthesis helper in
  `src/storage/encoding/page_materialize.rs`:

  ```rust
  pub(crate) fn synthesize_all_null_page_buffers(
      page_row_count: usize,
      bytes_per_value: usize,
  ) -> Result<(Vec<u8>, PageValidityBitmap), ArrowError>;
  ```

  Rules:
  - Value bytes = `page_row_count × bytes_per_value`,
    checked (V1 fixed-width primitives only; Boolean is
    V2 and would add a `data_type` parameter).
  - Validity bitmap: `ceil(page_row_count / 8)` zero
    bytes; `null_count = page_row_count`.
  - Checked multiplication overflow fails closed.

- Add value-source dispatch enum:

  ```rust
  pub(crate) enum PageMaterializeValueSource {
      Decoded,
      AllNullSynthesized,
  }
  ```

- Focused unit tests:
  - `value_page_is_metadata_all_null`: true / false /
    stats absent / out of bounds / partial-stats /
    malformed descriptor. (Catalog mismatch is tested at
    the planner level in Phase 2.)
  - Synthesis sizes for row counts 0, 1, 7, 8, 9, 256,
    1027, 65535, 65536, 65537.
  - Fixed-width primitive synthesis byte length
    invariants for 1 / 2 / 4 / 8-byte widths.
  - Checked multiplication overflow fails closed.
  - Validity bitmap `null_count` matches
    `page_row_count`.
  - Null-slot canonicalization helper (test support
    only) catches non-zero dense null-slot values.

### Phase 2 — Planner integration

Wires the helper and disposition into the sparse
projection planner. Production behavior changes when ADR
063 is enabled.

- Extend `SparseProjectionFetchPlan`:

  ```rust
  pub(crate) value_page_dispositions: BTreeMap<
      PageMaterializeBufferKey,
      SelectedPageDisposition,
  >,
  ```

  Default-empty for ADR 064 / 069 callers (no elision).

- In `sparse_projection_fetch.rs`, after selected pages are
  derived from the survival mask, assign per-page
  disposition:
  - For each nullable fixed-width projected `(row_group,
    column, page)` with cascade × type in V1 catalog:
    - If `value_page_is_metadata_all_null` returns true:
      - Insert `(key, AllNullElided { page_row_count,
        bytes_per_value })` into
        `value_page_dispositions`.
      - Do not push the value page into `fetches` or
        `selected_body_ranges`.
      - Do not push the validity page into
        `validity_fetches` or `selected_body_ranges`.
      - Do not increment per-buffer range caps for
        omitted ranges.
    - Else: schedule fetches as today (per-page
      disposition either stored as `Materialize` or left
      absent per implementation choice).
  - Non-nullable, non-eligible-catalog, no-page-stats:
    schedule fetches as today.
- Sparse-vs-dense byte gate counts elided pages as zero
  selected bytes (value + validity). Strict improvement.
- Elided pages contribute zero to
  `MAX_SPARSE_RANGES_PER_BUFFER`,
  `max_simd_pages_per_record`, `max_ranges_per_record`,
  selected-byte-fraction gate.
- Record `AllNullElisionDeclineReason` for bench/test
  diagnostics when an otherwise selected nullable page
  cannot elide.
- If the helper returns false, do not decline to dense.
  Continue normal sparse projection.
- Tests:
  - clustered-all-null row group: every selected page in
    `value_page_dispositions` as `AllNullElided`; no
    value ranges; no validity ranges in plan.
  - mixed-pages: some value pages elided, others
    fetched; per-page decision honored.
  - ADR 063 disabled (no page stats):
    `value_page_dispositions` empty (or all
    `Materialize`); plan byte-identical to ADR 064 / 069.
  - cascade out of catalog (e.g., `Rle` byte-run): no
    elision.
  - type out of catalog (Float64): no elision.
  - non-nullable column: no elision; architecture guard
    backs it.
  - two-nullable-column divergent: column A elides,
    column B fetches on same page; per-column
    independence.
  - cross-process determinism: identical
    `value_page_dispositions` map state and identical
    selected ranges.

### Phase 3 — Materializer integration

Updates the materializer to honor `AllNullElided`.

- In `materialize_sparse_projection_page_batches` (and
  ADR 065 / 069 sibling helpers), dispatch on per-page
  disposition lookup:
  - `Materialize` (or absent key): existing path (raw or
    encoded validity per ADR 064 / 069; decoded value
    bytes).
  - `AllNullElided { page_row_count, bytes_per_value }`:
    - Dispatch with
      `PageMaterializeValueSource::AllNullSynthesized`.
    - Call `synthesize_all_null_page_buffers`.
    - Feed both synthesized buffers (value + validity)
      into the existing
      `projected_page_batch_from_decoded_buffers`
      callsite.
- Materializer recovers `data_type` from the matching
  `PageMaterializeColumn` at materialization time.
- Page-local survival-mask filter runs unchanged.
- Materializer never inspects validity bits to decide page
  elision; the decision arrived pre-fetched in the
  disposition map.
- Tests:
  - elided `Int64` page: synthesized 8 × `page_row_count`
    zero value bytes; `ceil(page_row_count / 8)` zero
    validity bytes; Arrow output `Int64Array` with all
    nulls.
  - elided `Int32` and `Timestamp(ms)` pages: correct 4-
    and 8-byte synthesis sizes; correct Arrow output
    types.
  - elided page with non-byte-aligned row count (1027):
    correct synthesis sizes.
  - mixed row group: elided + non-elided pages combined
    into one Arrow batch; correct row order.
  - ADR 065 Case B zip with elided projection page:
    canonical-null Arrow output.
  - zero selected rows in an elided page: empty output,
    no synthesis cost accumulated.
  - non-nullable projected column on same page as
    nullable elided: fetches normally.
  - missing decoded value page without an elision
    disposition still errors.

### Phase 4 — Architecture guards

Extend `tests/architecture_test.rs`. Replace ADR 069's
guard with the five-part invariant set (Phase 0 P6
literal text):

1. **Metadata-only elision.** Value-page AND validity-
   page fetches may be elided only when the page's
   `SelectedPageDisposition` is `AllNullElided`,
   reachable only via `value_page_is_metadata_all_null`.
2. **No decoded-bit elision.** No code path inspects
   decoded validity bits, decoded value bytes, or any
   post-fetch quantity to decide page elision.
3. **Per-column scope.** Elision is keyed by `(row_group,
   column, value_buffer, page)`.
4. **Non-nullable forbidden.**
   `SelectedPageDisposition::AllNullElided` /
   `PageMaterializeValueSource::AllNullSynthesized` never
   fires for a non-nullable column.
5. **Synthesis shape.** Value buffer length is exactly
   `page_row_count × bytes_per_value`; validity bitmap
   length is `ceil(page_row_count / 8)`; both are zero
   bytes; both lengths verified.

Additional structural guards:

- `value_page_is_metadata_all_null`,
  `synthesize_all_null_page_buffers`,
  `SelectedPageDisposition`,
  `PageMaterializeValueSource`,
  `AllNullElisionDeclineReason`,
  `value_page_dispositions` are all `pub(crate)`.
- No new `L2FetchStrategy` variant, public metric label,
  `EncodingWriterConfig` field,
  `ENCODING_DIRECTORY_FORMAT_VERSION` bump,
  `EncodingCascade` or
  `EncodingComputeEvaluatorKind` variant, fallback /
  reject variant, wire / descriptor / capability / digest
  surface, or Cargo feature.
- Cascade × type catalog match exhausts ADR 063 V1
  intersection at the elision-eligibility site (compile-
  time exhaustion).
- ADR 069 `decode_rle_validity_page` remains the only
  caller for encoded-validity page decode; for elided
  pages, no caller invokes it.
- Helper name `value_page_is_metadata_all_null` is
  preserved (no shortening); architecture-guard text
  references it explicitly.

### Phase 5 — Focused tests

Cross-phase oracle tests after planner and materializer
wiring.

- **Canonical-null oracle.** Each fixture (clustered-all-
  null, alternating-pages-all-null, all-valid, random-
  50%, mixed-cascade, two-nullable-column divergent):
  plan sparse, elide eligible pages,
  materialize, assert canonical-null logical equality vs.
  dense fallback.
- **Byte-identical oracle for non-elided.** Pages not
  tagged `AllNullElided` produce byte-identical output
  and byte-identical fetch plans vs. ADR 064 / 069
  baseline.
- **No value-byte oracle.** For each elided page, assert
  the value-buffer byte range is absent from
  `selected_body_ranges` and the final multi-range read
  plan.
- **No validity-byte oracle.** Same for the validity-
  buffer byte range. Holds for both ADR 064 raw and ADR
  069 encoded validity sources.
- **ADR 063 disabled baseline.** With page stats off,
  sparse fetch byte plan and Arrow output byte-identical
  to ADR 064 / 069 path.
- **Mixed validity source per row group.** Row group with
  some raw, some encoded, some elided: canonical-null-
  identical output.
- **ADR 065 Case B zip with elided page.** Dense
  predicate batch + sparse projection batch with elided
  projection page: combined output canonical-null-
  identical to dense fallback.
- **Two-nullable-column divergent.** Column A elides
  (`null_count == page_value_count`), column B fetches
  (`null_count < page_value_count`). Per-column
  independence verified.
- **Boundary row counts.** 0, 1, 7, 8, 9, 256, 1027,
  65535, 65536, 65537 — correct synthesis sizes; correct
  Arrow output.
- **Cross-replica determinism.** Two-process elision;
  byte-equal disposition maps, fetch plans, projected
  output.
- **ADR 064 raw-validity regression-free.** Full ADR 064
  fixture matrix with ADR 070 in pipeline; raw-validity
  columns with non-eligible pages produce byte-identical
  output.
- **ADR 069 encoded-validity regression-free.** Same for
  ADR 069 fixtures.
- **Architecture-guard relocation test.** Assert ADR
  069's "no all-null elision" guard text is removed; the
  five-part ADR 070 guard text is present.
- **Non-nullable on elided page.** Non-nullable projected
  column on same page as nullable elided fetches
  normally; no `AllNullSynthesized` for non-nullable.
- **Drift sentinel property test.** For every fixture row
  in which `null_count == page_value_count`,
  independently decode the underlying raw or ADR 069
  encoded validity bytes (test-side, not production) and
  assert observably all-zero over `page_value_count`
  bits. Catches writer-validity drift for the inherited
  ADR 063 trust contract.

### Phase 6 — Bench matrix and promotion

Records all promotion gates from the ADR. Lives in
`plans/impl/0070_all_null_page_skip_sparse_projection_bench_results.md`.

- Bench home: `benches/row_group_fetch.rs` (Phase 0 P8
  pinned).
- Bench flag: `--all-null-elision-only` (mirrors ADR
  069's `--encoded-validity-only`).
- Axes:
  - ADR 063 enablement: disabled (control) / enabled
    (active).
  - Fixture: clustered-all-null, clustered-5%-null,
    all-valid, random-50%, alternating-pages-all-null,
    mixed-cascade, two-nullable-column divergent.
  - Row tiers: 8K / 65K / 1M.
  - Validity source: ADR 064 raw / ADR 069 encoded.
  - Profile: `PointRead` / `Scan`.
- Per-row metrics:
  - selected value bytes baseline vs. elision;
  - selected validity bytes baseline vs. elision;
  - total selected bytes;
  - elided page count;
  - synthesized byte count;
  - range count baseline vs. elision (regressions
    recorded with justification);
  - macro-block GET count (per ADR 061; regressions
    recorded with justification);
  - reader wall-clock baseline vs. elision;
  - projected Arrow output digest (canonical-null
    normalized);
  - cross-replica determinism digest across two runs.
- Suggested verification commands:

  ```text
  cargo fmt
  cargo check --benches
  cargo test -q --test architecture_test
  cargo test -q value_page_is_metadata_all_null
  cargo test -q synthesize_all_null_page_buffers
  cargo test -q all_null_elision
  cargo test -q sparse_projection
  cargo test -q --test encoding_materialize
  cargo bench --bench row_group_fetch --no-run
  cargo bench --bench row_group_fetch -- --quick --all-null-elision-only
  cargo bench --bench row_group_fetch -- --all-null-elision-only --rows=8192,65536,1048576
  git diff --check
  ```

- Promotion commit (single commit) lands helper,
  diagnostic, disposition, synthesis, planner integration,
  materializer dispatch, architecture guards, drift
  sentinel property test, and promotion artifact. No
  writer-routing toggle (no writer change).
- Amend
  `plans/adr/0064_nullable_sparse_projection.md`
  §"V2 deferrals" and §"Where we are" to record closure
  of the all-null page-skip follow-up.
- Amend
  `plans/adr/0069_encoded_validity_sparse_projection.md`
  to record removal of the ADR 070 boundary guard.
- Amend `plans/wip/roadmap.md` entry for ADR 070 to
  record promotion.

If hard gates pass, promote. If a hard gate fails, the
artifact records the gap. Council decides among three
explicit paths:

1. **Revise V1**: narrow eligibility (e.g.,
   `page_value_count > N` floor), tighten oracle, fix a
   determinism source.
2. **Scope down**: ship helper + synthesis + architecture-
   guard relocation as plumbing only; defer materializer
   integration to a separate ADR (low probability;
   materializer integration is small).
3. **Shelve**: keep ADR 064 / 069 paths unchanged,
   document the gap, reopen when ADR 063 ships with
   broader page-stat coverage.

## Files touched

Implementation:

- `src/storage/encoding/sparse_projection_fetch.rs` —
  `value_page_is_metadata_all_null` helper;
  `SelectedPageDisposition` enum;
  `AllNullElisionDeclineReason` private diagnostic;
  `SparseProjectionFetchPlan.value_page_dispositions`
  field; planner extension to populate per-page
  dispositions.
- `src/storage/encoding/page_materialize.rs` —
  `synthesize_all_null_page_buffers` helper;
  `PageMaterializeValueSource` enum; materializer
  dispatch on disposition lookup.
- `src/storage/manager.rs` — confirm ADR 065 Case B
  handoff handles canonical-null buffers (Phase 0 P11).
- `tests/architecture_test.rs` — five-part invariant
  guard set (Phase 0 P6 literal text) + structural
  guards.
- `benches/row_group_fetch.rs` — `--all-null-elision-only`
  bench matrix.

Planning / evidence:

- `plans/adr/0070_all_null_page_skip_sparse_projection.md`
- `plans/impl/0070_all_null_page_skip_sparse_projection.md`
- `plans/impl/0070_all_null_page_skip_sparse_projection_bench_results.md`

Out of scope:

- Mixed-page skip via decoded validity. V2.
- Page stats on additional cascades. ADR 063 V2.
- Variable-width nullable sparse projection. ADR 064 V2.
- Float / Time types. ADR 063 V2.
- Two-stage probe-route fetch. V2 if ever.
- Aggregate-fold short-circuit on elided pages. ADR 058 /
  059 territory.
- Operator-visible toggles, public decline / fallback /
  metric labels.

## Risks and mitigations

- **R1 — Trusting an incorrect page stat.** Worst
  correctness bug. *Mitigations:* ADR 063 already trusts
  the same field for `IS NULL` (closed-system
  inheritance); canonical-null oracle is a hard gate;
  Phase 0 P14 drift-sentinel pre-check empirically
  validates the writer invariant against existing
  fixtures before V1 ships; sentinel property test keeps
  the audit running.
- **R2 — Canonical-null oracle gap.** Oracle changes from
  byte-identical to canonical-null for elided pages.
  *Mitigations:* Phase 0 P-fork formal pin + P2 evidence;
  non-elided pages retain byte-identical oracle; fixture
  matrix covers elision-engaged and elision-declined
  cases separately.
- **R3 — Elided page byte plan registers ranges.**
  *Mitigations:* architecture guard asserts no value
  ranges AND no validity ranges for `AllNullElided`
  pages; focused test on `selected_body_ranges`.
- **R4 — Materializer reads from elided buffers OOB.**
  *Mitigations:* synthesized zero buffers are real
  allocations of correct length; reading yields zeros
  (no OOB). Architecture guard pins synthesis shape.
- **R5 — Architecture-guard relocation too permissive.**
  *Mitigations:* five-part invariant set; compile-time
  cascade × type exhaustion; fail-closed on disposition
  shapes outside catalog; Phase 0 P6 produces literal
  Rust predicates.
- **R6 — ADR 063 disabled deployment leaves V1 inert.**
  By design; capability lands behind stat-presence key;
  operational enablement separate. Bench artifact
  records both control and active rows.
- **R7 — Page stats sparse / optional within a buffer.**
  *Mitigations:* `value_page_is_metadata_all_null`
  fails-closed on absence; per-page decision; partial-
  stats fixture covered.
- **R8 — Boolean misclassified as V1.** ADR 063 includes
  Boolean in its stats catalog but ADR 060 / 064 exclude
  it via `fixed_width_value_bytes`; including Boolean in
  V1 would silently widen ADR 060 / 064's value-column
  catalog. *Mitigations:* P1 catalog audit explicitly
  intersects with `fixed_width_value_bytes`; helper
  cannot enforce catalog (no `DataType`), so the planner
  checks `DataType` and cascade explicitly before
  invoking; architecture guard exhausts the intersected
  catalog at the elision-eligibility site (compile-time).
- **R9 — ADR 065 Case B zip on synthesized buffers.**
  *Mitigations:* Phase 0 P11 audit; focused test in
  Phase 5.
- **R10 — Cross-replica nondeterminism in elision.**
  *Mitigations:* page stats are deterministic per ADR
  063; elision is a pure function; `BTreeMap`
  deterministic iteration; Phase 0 P5 probe; hard
  promotion gate.
- **R11 — Macro-block coalescing regression.** ADR 060's
  range coalescer
  (`src/storage/encoding/sparse_projection_fetch.rs`)
  and ADR 061's chunk coalescer (`src/crypto.rs`) both
  merge only overlapping or adjacent ranges. Eliding a
  middle range can split one coalesced run into two,
  producing a GET-count regression even when total bytes
  drop. *Mitigations:* Phase 0 P4 + P12 audit; bench
  artifact records range and macro-block GET count
  baseline vs. elision and reports any regression with
  the byte reduction that motivated it. Promotion does
  not gate on GET count never increasing. Bridge-
  overfetch logic (retain coalescing when GET count
  would regress) is V2 territory.
- **R12 — Per-column independence bug.** Elision for
  column A accidentally affects column B on the same
  page. *Mitigations:* `(row_group, column,
  value_buffer, page)` key discipline; two-nullable-
  column divergent fixture (Phase 0 P13 + Phase 5
  test).
- **R13 — Closed-system trust contract drift between
  ADRs.** If a future ADR amends ADR 063 to add an
  `unknown` / `partial` page-stat shape, ADR 070's
  trust contract must be re-examined. *Mitigations:*
  helper fails-closed on any non-`null_count` shape;
  documented in ADR 063 V2 territory.

## Definition of done

- All 15 Phase 0 pins recorded in the bench-results
  artifact; canonical-null oracle, writer-scope P-fork,
  and P14 drift-sentinel pre-check formally documented.
- `value_page_is_metadata_all_null`,
  `synthesize_all_null_page_buffers`,
  `SelectedPageDisposition`,
  `PageMaterializeValueSource`,
  `AllNullElisionDeclineReason`,
  `value_page_dispositions` all land as `pub(crate)`.
- Architecture-guard relocation lands: ADR 069's "no
  all-null elision" replaced by ADR 070's five-part
  invariant set + structural guards.
- Closed cascade × type catalog match at the elision-
  eligibility site (compile-time exhaustion over ADR 063
  V1 intersection).
- All 12 hard promotion gates pass; 7 soft gates met or
  explicitly justified (after Phase 0 P15 calibration).
- Drift sentinel property test runs across the full
  fixture matrix and asserts observably all-zero
  validity bytes for every metadata-all-null tagged
  page.
- ADR 063 disabled control row in bench artifact shows
  byte-identical output to ADR 064 / 069 path
  (regression-free).
- ADR 063 enabled fixtures show elided value+validity
  byte reduction, range count reduction, and wall-clock
  speedup hitting soft gates.
- Macro-block GET count and range count recorded per
  bench row vs. ADR 064 / 069 baseline; regressions
  reported alongside accompanying byte reduction.
- Promotion commit lands materializer dispatch on
  disposition lookup; no writer changes.
- ADR 064 §"V2 deferrals" and ADR 069's boundary guard
  amended to record closure.
- Roadmap entry for ADR 070 updated to record promotion.
