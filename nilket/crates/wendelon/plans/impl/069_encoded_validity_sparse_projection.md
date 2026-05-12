# Implementation Plan: ADR 069 — Encoded-Validity Page Decoding for Sparse Projection

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/069_encoded_validity_sparse_projection.md`

Bench-results artifact (created at promotion):
`plans/impl/069_encoded_validity_sparse_projection_bench_results.md`

Inherits substrate:

- `plans/adr/057_per_buffer_pages_and_encoding_cascades.md` —
  `EncodingCascade::Rle { inner }`, `EncodingDirectory`,
  `BufferEncoding`, `page_end_offsets`, page-emitting cascade
  discipline, decode/materialize helpers. ADR 069 bumps no
  format version.
- `plans/adr/060_sparse_projection_fetch_narrowing.md` — per-page
  byte-range fetch planner, multi-range coalescing, sparse-vs-
  dense byte gate, private planner-decline discipline.
- `plans/adr/063_page_level_predicate_stats.md` — ExactMatch /
  Maybe classifier composes on the upstream page-pick side
  (no surface change).
- `plans/adr/064_nullable_sparse_projection.md` — raw-validity
  fetch path, `rebase_validity_bitmap`,
  `column_null_counts == 0` short-circuit, fork-resolution Phase 0
  pattern. ADR 069 narrows ADR 064's
  `EncodedValidityBufferUnsupported` decline to admit one
  cascade.
- `plans/adr/065_dense_predicate_zip_with_sparse_projection.md`
  — page-local sparse projection materialization; ADR 069
  preserves the same page-local mask filtering shape.
- `plans/adr/067_row_canonical_fsst.md` — clean-break cross-
  version compatibility precedent.
- `plans/adr/068_fsst_row_safe_writer_estimator.md` — crate-
  private writer-routing pattern (`FsstRowSafeEstimatorRouting`
  → `WriterValidityCascadeRouting`).
- `plans/conventions/` — newtype-units, no-version-suffixes,
  no-planning-phase-names, no-premature-backward-compat,
  capability-vs-feature naming.

## Status

Council-converged across Claude / Codex / Gemini after four
iterations under `plans/wip/`. Pre-Phase 0; lands no code yet.

## Implementation contract

- ADR 064's raw-validity path remains byte-identical for every
  column where the writer economic gate does not select
  `Rle { inner: Plain }` for the validity buffer.
- The V1 encoded-validity catalog has exactly one member:
  `EncodingCascade::Rle { inner: EncodingLeafCascade::Plain }`.
- Validity pages align 1:1 with value-buffer pages by row span.
- Validity pages are emitted as page-local bitmaps starting at
  bit 0 of their own first byte. Decoded byte length of each
  page is `ceil(page_row_count / 8)`.
- The decline narrow at
  `src/storage/encoding/sparse_projection_fetch.rs:252-262`
  routes V1-eligible cascades through the encoded-validity
  decode path; everything else collapses to the existing
  `EncodedValidityBufferUnsupported`.
- Full-buffer decode callers
  (`materialize_encoding_buffer`, `decode_encoded_buffer`)
  route encoded-validity buffers through
  `stitch_validity_pages_into_row_group_bitmap`, not raw byte
  concatenation.
- No new public API. No new `EncodingCascade` /
  `EncodingComputeEvaluatorKind` / `L2FetchStrategy` variant.
  No new IPC `custom_metadata` key. No
  `ENCODING_DIRECTORY_FORMAT_VERSION` bump. No descriptor /
  PBC / capability / digest / `ObjectStore` trait change. No
  Cargo feature. No operator-visible toggle. No new metric
  label.
- Writer cascade selection for validity buffers is
  deterministic (integer-only economic gate, deterministic
  RLE encoder, no wall-clock / random / hash-map-iteration /
  CPU-capability input).

## Goal

Reduce selected validity bytes on clustered-nullity nullable
sparse projection without changing projected Arrow output
bytes. Narrow ADR 064's `EncodedValidityBufferUnsupported`
decline to admit exactly one validity cascade. Add the
minimum writer extension that creates eligible encoded-validity
production (conditional on Phase 0 P-fork outcome, essentially
confirmed). All-null value-page skip remains out of scope (ADR
070).

## Phase plan

### Phase 0 — Audit and fork resolution

Pre-implementation pass. No code lands. Deliverable: a "Phase 0
audit" section in the bench-results artifact pinning each item.

- **P-fork (writer scope).** Probe writer output for nullable
  fixed-width columns with clustered, random, all-valid, and
  all-null validity. Record whether any current code path emits
  a validity-buffer `BufferEncoding`. Expected: no
  (`is_fixed_width_value_buffer` at
  `src/storage/encoding/directory.rs:466-473` excludes validity
  ordinal 0). If confirmed, V1 ships writer + reader; if a
  producer is found, V1 narrows to reader-only. The writer route
  is a separate null-buffer-ordinal call site, not a relaxation
  of the value-buffer filter.
- **P1: `VALIDITY_RLE_BYTE_SAVING_FRACTION`.** Starts at 1/4.
  Sweep clustered, random, alternating, all-valid, all-null
  fixtures; record zero-false-encode rate.
- **P2: `MIN_VALIDITY_ENCODE_BYTES`.** Starts at 256 raw bytes.
- **P3: ADR 060 multi-range coalescing.** Confirm
  `TieredMultiRangeReadPlan` coalesces encoded-validity byte
  ranges with value-page byte ranges and counts them against
  existing sparse gates.
- **P4: Cross-replica determinism probe.** Two-process RLE
  encode over packed-bitmap input; require byte-identical
  output. Fix before Phase 1 if non-deterministic.
- **P5: Warehouse page-row-count modularity.** Confirm
  `page_value_count` is typically a multiple of 8; non-multiples
  remain correct with at most 7 padding bits per page.
- **P6: Module location.** `cascade/validity.rs` sibling vs.
  co-located in `select.rs`. Pick on file size and call-site
  clarity.
- **P7: Architecture-guard exhaustion.** Pin the exhaustive
  Rust `match` over all 8 catalog variants; future cascade
  variants must trip the compiler at this site.
- **P8: Validity-buffer encoder reuse.** Confirm ADR 057's RLE
  byte-run encoder accepts a packed-bitmap byte slice as opaque
  input and matches hand-traced RLE on a small fixture. Scope a
  small refactor if byte-semantic assumptions break on bitmap
  input.
- **P9: Full-buffer decode caller audit and role-dispatch
  shape.** `materialize_encoding_buffer` at
  `src/storage/encoding/materialize.rs:27` takes
  `(encoded_buffer_bytes, data_type, directory, encoding)` and
  carries no buffer-role context; the encoding's
  `ipc_buffer_ordinal` alone cannot distinguish validity from
  value without the column's `null_buffer_ordinal`. Enumerate
  every call site of `materialize_encoding_buffer` /
  `decode_encoded_buffer` that could receive an encoded-validity
  `BufferEncoding`. For each, confirm the call site has buffer-
  role context (knows the column's `null_buffer_ordinal`) and
  can route validity buffers through the new
  `materialize_encoding_validity_buffer` entry point (Phase 1).
  If a call site lacks role context, pin the smallest plumbing
  addition (caller-side null-ordinal lookup, role parameter, or
  split entry point). This audit is load-bearing: without it,
  full-buffer Boolean decode silently misaligns bits across page
  boundaries on non-byte-aligned page row counts. The
  architecture guard (Phase 5) enforces that
  `materialize_encoding_buffer` is never called on a
  `BufferEncoding` whose `ipc_buffer_ordinal` matches the
  column's validity ordinal.
- **P10: Clustered-nullity fixture identification.** Add or
  identify a clustered-nullity nullable fixed-width fixture
  (5%-null clustered runs on a nullable `Int64`). Existing ADR
  064 fixtures may not exercise the encoded-validity path
  strongly enough.
- **P11: Bench home.** Pick `benches/encoding_compute.rs`,
  `benches/row_group_fetch.rs`, or a focused sibling for the
  Phase 7 promotion matrix. Keep output as stdout diagnostic
  rows (no new metric labels per the closed-surface invariant).

### Phase 1 — Bitmap-specific decode and stitch helpers

Lands private reader helpers and focused tests before any
planner or writer changes. Production behavior is byte-
identical.

- Add `validity_page_decoded_byte_len(page_value_count: usize)
  -> usize` returning `(page_value_count + 7) / 8`. Used by all
  bitmap-aware paths.
- Add `decode_rle_validity_page`:

  ```rust
  pub(crate) fn decode_rle_validity_page(
      encoded_page: &[u8],
      page_value_count: usize,
  ) -> Result<PageValidityBitmap, ArrowError>;
  ```

  Decodes the RLE byte-run payload to a bitmap of
  `validity_page_decoded_byte_len(page_value_count)` bytes;
  validates decoded length matches `ceil(page_value_count / 8)`
  (fail-closed); **counts nulls only over the first
  `page_value_count` bits** — tail-padding bits at positions
  `page_value_count` through `8 * bytes.len() - 1` are ignored
  and do not contribute to `null_count`. (A 9-row all-valid
  page decodes to 2 bytes with bits 9–15 zero-padded by the
  writer; `null_count` must be 0, not 7.) Errors on corrupt RLE
  payloads.
- Add `stitch_validity_pages_into_row_group_bitmap`:

  ```rust
  pub(crate) fn stitch_validity_pages_into_row_group_bitmap(
      pages: &[(EncodingPageDescriptor, PageValidityBitmap)],
      row_group_row_count: usize,
  ) -> Result<Vec<u8>, ArrowError>;
  ```

  Allocates `ceil(row_group_row_count / 8)` bytes; bit-shifts
  each page-local bitmap into the row-group bitmap at
  `page.first_value_index`; errors on overlapping page row
  spans, gaps, page-count mismatch, or row spans outside the
  row-group row count.

- Add the role-aware full-buffer entry point
  `materialize_encoding_validity_buffer`:

  ```rust
  pub(crate) fn materialize_encoding_validity_buffer(
      encoded_buffer_bytes: &[u8],
      directory: &EncodingDirectory,
      encoding: &BufferEncoding,
      row_group_row_count: usize,
  ) -> Result<Vec<u8>, EncodingRejectCode>;
  ```

  This is the production entry point for full-buffer decode of
  an encoded-validity `BufferEncoding`. It expands page
  descriptors from `encoding.page_end_offsets`, decodes each
  page-local bitmap via `decode_rle_validity_page`, and stitches
  via `stitch_validity_pages_into_row_group_bitmap`. The
  original `materialize_encoding_buffer` at
  `src/storage/encoding/materialize.rs:27` is unchanged and
  remains the value-buffer entry point.
- Focused unit tests:
  - boundary row counts: 0, 1, 7, 8, 9, 255, 256, 257, 65535,
    65536, 65537;
  - bit patterns: all-valid, all-null, clustered, random,
    alternating;
  - **tail-padding `null_count` correctness**: a 9-row all-
    valid page (decoded byte length 2; bits 9–15 are
    writer-padded zeros) returns `null_count == 0`. Mirror with
    a 9-row all-null page (decoded byte length 2; bits 9–15 are
    zeros) returns `null_count == 9`. Similar checks at row
    counts 1, 7, 13, 1027 (non-byte-aligned), and exact
    boundaries 8 / 16 / 64 / 256;
  - corrupt RLE payload fails closed;
  - stitch with non-byte-aligned `first_value_index` (e.g.,
    page starting at row 13);
  - stitch with page-count mismatch fails closed;
  - stitch with overlapping page spans fails closed.

### Phase 2 — Writer page-local validity emission

Lands the writer integration behind crate-private routing.
Production stays disabled until Phase 7 promotion.

- Add `WriterValidityCascadeRouting { Disabled, Enabled }` as a
  crate-private enum, mirroring
  `FsstRowSafeEstimatorRouting`. Production initialization sets
  `Disabled`. Test/bench helpers can pass either.
- Add private types:

  ```rust
  pub(crate) struct ValidityRleEncodingPlan {
      pub(crate) encoded_bytes: Vec<u8>,
      pub(crate) page_end_offsets: Vec<EncodingPageByteOffset>,
      pub(crate) run_count: RunCount,
      pub(crate) raw_byte_len: usize,
  }

  pub(crate) struct ValuePageRowSpan {
      pub(crate) first_value_index: BufferValueIndex,
      pub(crate) value_count: BufferValueCount,
  }
  ```

  `run_count` is the sum of per-page run counts produced by the
  RLE encoder across all page-local bitmaps in the row group;
  it populates the cascade-level
  `EncodingCascade::Rle { run_count, .. }` field at
  `src/storage/encoding/types.rs:118-121`. Per-page run counts
  are recovered by the decoder from each page's encoded payload
  and are not separately stored.

- Add `select_validity_cascade` in `select.rs` (or
  `cascade/validity.rs` per Phase 0 P6):

  ```rust
  pub(crate) fn select_validity_cascade(
      raw_validity_bytes: &[u8],
      value_page_row_spans: &[ValuePageRowSpan],
      column_null_count: usize,
  ) -> Option<ValidityRleEncodingPlan>;
  ```

  Returns `None` when any of:
  - `column_null_count == 0` (ADR 064 short-circuit);
  - `value_page_row_spans.is_empty()` (value buffer not page-
    emitting);
  - `raw_validity_bytes.len() < MIN_VALIDITY_ENCODE_BYTES`;
  - accumulating `page_end_offsets` via `checked_add` overflows
    the typed newtype;
  - the strict saving threshold fails:
    `encoded_total + page_table_bytes >=
    raw_total * (1 - VALIDITY_RLE_BYTE_SAVING_FRACTION)`.

  Otherwise extracts page-local bitmaps (`ceil(page_rows / 8)`
  bytes; bit 0 = first row; tail bits zero-padded
  deterministically), runs the existing RLE byte-run encoder on
  each, accumulates per-page run counts into the plan's
  `run_count`, and returns the plan.
- Add `pub(crate) const VALIDITY_RLE_BYTE_SAVING_FRACTION` (1/4)
  and `pub(crate) const MIN_VALIDITY_ENCODE_BYTES` (256).
- Add `build_encoding_directory_for_ipc_stream_with_routing` in
  `src/storage/encoding/directory.rs`, mirroring ADR 068's
  `_with_fsst_routing` helper; carries both
  `FsstRowSafeEstimatorRouting` and
  `WriterValidityCascadeRouting`. Production entry points
  initialize validity routing `Disabled`.
- When routing is `Enabled` and a column is nullable with
  `column_null_count != 0` and a page-emitting value buffer,
  call `select_validity_cascade`. On `Some(plan)`, emit a
  `BufferEncoding` for the validity buffer with cascade
  `EncodingCascade::Rle { run_count: plan.run_count, inner:
  Box::new(EncodingLeafCascade::Plain) }`, `page_end_offsets`
  from the plan, `page_value_count` matching the value buffer's,
  `decoded_value_count = row_group.row_count`,
  `decoded_byte_len = ceil(row_group.row_count / 8)`. On `None`,
  emit no validity `BufferEncoding`.
- The existing `is_fixed_width_value_buffer` filter is
  unchanged.
- Tests:
  - routing `Disabled` preserves current directory output;
  - routing `Enabled` + clustered-nullity emits encoded entry;
  - routing `Enabled` + random-nullity declines (gate);
  - routing `Enabled` + all-valid declines (short-circuit);
  - routing `Enabled` + non-page-emitting value buffer declines;
  - boundary-threshold input declines (strict `<`);
  - typed page-offset overflow declines;
  - emitted validity cascade is exactly `Rle { inner: Plain }`;
  - validity page count and row spans match the value buffer;
  - cross-process determinism: two processes, byte-identical
    encoded bytes and `page_end_offsets`.

### Phase 3 — Sparse-projection planner extension

Narrows the existing encoded-validity decline. Production
routing is still `Disabled`, so this phase doesn't change
production behavior.

- Add `validity_v1_eligibility` helper in
  `src/storage/encoding/sparse_projection_fetch.rs`:

  ```rust
  fn validity_v1_eligibility(
      validity_buf: &BufferEncoding,
      selected_value_pages: &[EncodingPageDescriptor],
  ) -> ValidityV1Eligibility;

  pub(crate) enum ValidityV1Eligibility {
      EncodedSparse,
      Decline(EncodedValidityDeclineReason),
  }

  pub(crate) enum EncodedValidityDeclineReason {
      UnsupportedCascade,
      MissingPageMetadata,
      PageGeometryMismatch,
      DecodedByteLenMismatch,
      CorruptPageDescriptor,
  }
  ```

  The helper reads `validity_buf.cascade_index` internally; the
  raw-vs-encoded dispatch is done by the caller (a
  `BufferEncoding` would not be available for raw validity).

  - `EncodedSparse`: cascade is `Rle { run_count, inner:
    Box::new(Plain) }` + 1:1 alignment with selected value
    pages + `decoded_byte_len == ceil(decoded_value_count / 8)`
    + non-empty `page_end_offsets` → new encoded-validity path.
  - `Decline(reason)`: collapses to
    `SparseProjectionFetchDecline::EncodedValidityBufferUnsupported`
    at the planner boundary.

- Replace the unconditional decline at
  `sparse_projection_fetch.rs:252-262`:

  ```rust
  match find_buffer_encoding(input.encoding_directory, group_index,
                            column.null_buffer_ordinal,
                            validity_page.range) {
      None => { /* ADR 064 raw-validity path */ }
      Some(buf) => match validity_v1_eligibility(buf, selected_value_pages) {
          ValidityV1Eligibility::EncodedSparse => { /* new path */ }
          ValidityV1Eligibility::Decline(_) => {
              return Decline(EncodedValidityBufferUnsupported);
          }
      }
  }
  ```

- For `EncodedSparse`: resolve the selected value-page index k
  to validity-page index k (1:1 alignment), compute the
  validity page's encoded byte range as
  `[validity_buf.encoded_range.start + prev_end,
  validity_buf.encoded_range.start + page_end_offsets[k])`
  where `prev_end = if k == 0 { 0 } else { page_end_offsets[k-1] }`
  (ADR 057 prefix-end-array convention; the implicit zero is
  not stored). Create a `PageMaterializeValidityFetch` carrying
  source `PageMaterializeValiditySource::Encoded { cascade }`.
- Count encoded-validity ranges against
  `MAX_SPARSE_RANGES_PER_BUFFER`,
  `max_simd_pages_per_record`, `max_ranges_per_record`,
  selected-byte-fraction, and sparse-vs-dense byte gates.
- Preserve `all_valid_nullable_pages` behavior.
- Tests:
  - supported encoded validity engages sparse projection;
  - selected ranges include both value and validity ranges;
  - unsupported cascade declines with
    `EncodedValidityDeclineReason::UnsupportedCascade`;
  - missing page metadata declines with `MissingPageMetadata`;
  - geometry mismatch declines with `PageGeometryMismatch`;
  - decoded-byte-length mismatch declines with
    `DecodedByteLenMismatch`;
  - corrupt page descriptor declines with
    `CorruptPageDescriptor`;
  - empty masks short-circuit before encoded-validity checks;
  - encoded validity in one row group does not poison another
    row group's raw path.

### Phase 4 — Materialization integration

Updates `manager.rs` and `page_materialize.rs`.

- Change `PageMaterializeValidityFetch` to carry source shape:

  ```rust
  pub(crate) enum PageMaterializeValiditySource {
      Raw { bit_offset: u8 },
      Encoded { cascade: EncodingCascadeIndex },
  }

  pub(crate) struct PageMaterializeValidityFetch {
      pub key: ProjectedColumnPageKey,
      pub body_range: RecordBodyByteRange,
      pub page: EncodingPageDescriptor,
      pub page_row_count: usize,
      pub source: PageMaterializeValiditySource,
  }
  ```

- In `materialize_sparse_projection_page_batches`:
  - `Raw { bit_offset }` source: existing
    `rebase_validity_bitmap` path.
  - `Encoded { cascade }` source: dispatch on cascade. V1
    catalog accepts only `Rle { inner: Plain }`; call
    `decode_rle_validity_page(fetched, page_row_count)`.
    Result is a `PageValidityBitmap` with `bit_offset = 0`.
- Preserve `projected_page_batch_from_decoded_buffers` and
  `PageValidityBitmap` as the common handoff into Arrow batch
  assembly.
- Per Phase 0 P9 audit, update each full-buffer decode call
  site to dispatch on buffer role: callers materializing a
  validity buffer (i.e., the buffer whose `ipc_buffer_ordinal`
  matches the column's `null_buffer_ordinal`) route through
  `materialize_encoding_validity_buffer` (Phase 1) instead of
  `materialize_encoding_buffer`. The latter is unchanged and
  remains the value-buffer entry point. If a call site lacks
  role context, add the smallest plumbing addition Phase 0 P9
  pinned (caller-side null-ordinal lookup or role parameter).
  `decode_encoded_buffer` follows the same dispatch shape.
- Do *not* inspect decoded validity to skip value page fetches.
  Architecture guard enforces.
- Tests:
  - page batch with encoded validity matches dense projection
    byte-for-byte;
  - all-null selected page still fetches value page (ADR 070
    boundary);
  - local survival-mask filtering after page materialization is
    unchanged;
  - zero selected rows returns empty projection with correct
    schema;
  - full-buffer decode of writer-emitted encoded validity
    equals the original Arrow validity bitmap byte-for-byte.

### Phase 5 — Architecture guards

Extend `tests/architecture_test.rs`. Required guards:

- `WriterValidityCascadeRouting` is `pub(crate)` and not
  exposed through any public API.
- `select_validity_cascade`, `decode_rle_validity_page`,
  `stitch_validity_pages_into_row_group_bitmap`,
  `materialize_encoding_validity_buffer`,
  `validity_v1_eligibility`, `ValidityV1Eligibility`,
  `EncodedValidityDeclineReason`, and
  `PageMaterializeValiditySource` are all `pub(crate)`.
- `SparseProjectionFetchDecline::EncodedValidityBufferUnsupported`
  remains crate-private planner state.
- No new `L2FetchStrategy` variant, public metric label,
  `EncodingWriterConfig` field, `ENCODING_DIRECTORY_FORMAT_VERSION`
  bump, `EncodingCascade` or `EncodingComputeEvaluatorKind`
  variant, fallback/reject variant, wire/descriptor/capability/
  digest surface, or Cargo feature.
- The decline-narrow's cascade match exhausts all 8 catalog
  variants (compile-time exhaustion check).
- Validity-buffer cascade is `Rle { inner: Plain }` xor no
  `BufferEncoding` entry, on every emitted encoding directory.
- Validity `page_end_offsets.len()` equals value
  `page_end_offsets.len()` on every emitted encoding directory
  with encoded validity; row spans match 1:1.
- Validity-page decode goes through `decode_rle_validity_page`
  and never reuses the fixed-width value page decoder.
- The encoded-validity materialization site contains no code
  that inspects decoded validity bits to suppress an already-
  planned value-page fetch (ADR 070 boundary).
- Full-buffer decode callers route encoded-validity buffers
  through `materialize_encoding_validity_buffer` (which uses
  `stitch_validity_pages_into_row_group_bitmap` internally),
  not through `materialize_encoding_buffer`'s page-byte
  concatenation path. Specifically:
  `materialize_encoding_buffer` is never called on a
  `BufferEncoding` whose `ipc_buffer_ordinal` matches the
  column's validity ordinal.

### Phase 6 — Focused tests

Cross-phase oracle tests after planner and materializer wiring:

- **Encoded-validity round-trip oracle.** For each fixture in
  {clustered-5%, clustered-1%, all-valid, random-50%,
  alternating-bit, all-null}, encode with routing `Enabled`,
  fetch sparse, decode, materialize, assert byte-identical
  output vs. dense fallback.
- **Decline-narrow exhaustion.** Synthesize a `BufferEncoding`
  with each non-V1 cascade; assert
  `EncodedValidityBufferUnsupported` declines with the expected
  `EncodedValidityDeclineReason`.
- **Page-alignment invariant.** Mismatched page counts and
  mismatched row spans both trip the architecture guard.
- **Writer-economic-gate boundary.** Encoded byte length
  exactly at the threshold declines (strict `<`); just past it
  engages.
- **Page-offset overflow.** Synthetic input that overflows
  `EncodingPageByteOffset` accumulation; writer returns `None`
  and validity stays raw.
- **Cross-replica determinism.** Two-process encode + plan;
  byte-equal encoded bytes, `page_end_offsets`, projected
  output. ADR 068's two-process digest probe pattern.
- **ADR 064 raw-validity regression-free.** Full ADR 064
  fixture matrix with routing `Enabled`; raw-validity columns
  produce byte-identical output.
- **ADR 064 short-circuit preserved.** All-valid nullable
  column: no validity-buffer `BufferEncoding` even with
  routing `Enabled`.
- **All-null selected page does not skip value fetch.** ADR
  070 boundary test.
- **Non-byte-aligned page row count.** Page with row count
  1027 round-trips through encode → fetch → decode → stitch.
- **Full-buffer decode oracle.** Encode with routing
  `Enabled`, fetch entire validity buffer via
  `materialize_encoding_validity_buffer`, assert stitched
  output matches original Arrow validity bitmap byte-for-byte.
  Also encode with a non-byte-aligned page row count (e.g.,
  1027) and assert the stitched bytes correctly carry bits
  across page-boundary byte sharing.
- **Role-dispatch invariant.** A focused architecture test
  iterates the encoding directory for a fixture with encoded
  validity and asserts no call site invokes
  `materialize_encoding_buffer` on the validity-buffer
  `BufferEncoding`; the only legal entry point for that buffer
  is `materialize_encoding_validity_buffer`.
- **Multi-row-group fixture.** One row group encoded, one raw;
  same column; both produce byte-identical output.

### Phase 7 — Bench matrix and promotion

Records all promotion gates from the ADR. Lives in
`plans/impl/069_encoded_validity_sparse_projection_bench_results.md`.

- Extend the bench surface picked by Phase 0 P11
  (`encoding_compute.rs` or `row_group_fetch.rs`):
  - Routing axis: `WriterValidityCascadeRouting::Disabled` (ADR
    064 baseline) and `Enabled` (ADR 069).
  - Fixture axis: clustered-5%, clustered-1%, all-valid,
    all-null, random-50%, alternating-bit, unsupported-cascade
    decline, mismatched-page-geometry decline.
  - Row-count tiers: 8K / 65K / 1M per the ADR 062 / 063 /
    064 / 065 / 068 cadence.
  - Profiles: `PointRead` and `Scan`.
- Record per row:
  - selected validity bytes (encoded vs. raw);
  - selected total bytes (value + validity);
  - writer wall-clock (encoded vs. raw);
  - reader wall-clock (encoded vs. raw);
  - encoded-validity decline reason (`EncodedValidityDeclineReason`
    when applicable);
  - projected Arrow output digest (byte-equality oracle);
  - cache-miss / cache-hit / range count;
  - cross-replica determinism digest across two runs.
- Suggested verification commands:

  ```text
  cargo check -q
  cargo test -q --test architecture_test
  cargo test -q --test encoding_cascade
  cargo test -q --test encoding_compute_test
  cargo test -q sparse_projection
  cargo test -q encoded_validity
  cargo bench --bench row_group_fetch --no-run
  cargo bench --bench encoding_compute --no-run
  cargo bench --bench encoding_compute -- --quick --encoded-validity-only
  cargo bench --bench encoding_compute -- --encoded-validity-only --rows=8192,65536,1048576
  git diff --check
  ```

- Promotion commit (single commit) flips production
  initialization from `Disabled` to `Enabled` for the writer-
  routing toggle and lands the decline-narrow + reader decode
  helper as production. The routing enum stays in the codebase
  for bench/test comparison.
- Amend `plans/adr/064_nullable_sparse_projection.md`
  §"V2 deferrals" and §"Where we are" to record closure of the
  encoded-validity follow-up.
- Amend `plans/wip/roadmap.md` entry #25 to record promotion.

If hard gates pass, promote. If a hard gate fails, the
artifact records the gap; council decides among three explicit
paths:

1. **Revise V1**: adjust economic gate, narrow eligibility,
   fix a determinism source.
2. **Scope down**: ship reader-only decline narrow plus the
   bitmap decode helper, defer writer extension to a separate
   ADR.
3. **Shelve**: keep production routing `Disabled`, document
   the gap, reopen when stronger fixture evidence emerges.

## Files touched

Implementation:

- `src/storage/encoding/select.rs` —
  `select_validity_cascade`, `WriterValidityCascadeRouting`,
  `VALIDITY_RLE_BYTE_SAVING_FRACTION`,
  `MIN_VALIDITY_ENCODE_BYTES`, `ValidityRleEncodingPlan`,
  `ValuePageRowSpan`.
- `src/storage/encoding/cascade/validity.rs` (new, pending
  Phase 0 P6) — alternative location for the validity-cascade
  helper.
- `src/storage/encoding/directory.rs` —
  `build_encoding_directory_for_ipc_stream_with_routing`
  helper; validity-buffer `BufferEncoding` emission when
  routing is `Enabled`.
- `src/storage/encoding/page_materialize.rs` —
  `validity_page_decoded_byte_len`,
  `decode_rle_validity_page`,
  `stitch_validity_pages_into_row_group_bitmap`,
  `materialize_encoding_validity_buffer` (role-aware full-
  buffer entry point); `PageMaterializeValiditySource` enum;
  integration in `materialize_sparse_projection_page_batches`
  and `projected_page_batch_from_decoded_buffers`.
- `src/storage/encoding/sparse_projection_fetch.rs` —
  `validity_v1_eligibility` helper; narrowed
  `EncodedValidityBufferUnsupported` decline;
  `ValidityV1Eligibility` / `EncodedValidityDeclineReason`
  enums.
- `src/storage/encoding/materialize.rs` (and / or
  `src/storage/encoding/compute/dispatch.rs`) — full-buffer
  Boolean / validity page-stitch decode (Phase 0 P9 audit
  pins call sites).
- `src/storage/manager.rs` — materialization integration for
  raw vs. encoded validity sources.
- `tests/encoding_select.rs` — focused tests per Phase 6.
- `tests/architecture_test.rs` — architecture guards per
  Phase 5.
- `benches/row_group_fetch.rs` and/or
  `benches/encoding_compute.rs` — bench matrix per Phase 7.

Planning / evidence (created at promotion):

- `plans/adr/069_encoded_validity_sparse_projection.md`
- `plans/impl/069_encoded_validity_sparse_projection.md`
- `plans/impl/069_encoded_validity_sparse_projection_bench_results.md`

Out of scope:

- Bitmap-RLE (sub-byte runs) as a new cascade variant.
- `BitPacked` / `Dictionary` / `Delta` validity (codecs model
  fixed-width values).
- `Lz4Frame` validity (not page-emitting in ADR 057 V1).
- Encoded-validity for variable-width nullable sparse
  projection (ADR 064 V2 deferred variable-width entirely).
- All-null page-skip — ADR 070's scope.
- Page-stat-assisted validity-fetch skipping — ADR 064 V2
  deferred.
- Mismatched value/validity page-grid stitching — V1 declines.
- Operator-visible encoded-validity routing toggle.
- Public decline / fallback / metric labels for encoded-
  validity routing.

## Risks and mitigations

- **R1 — Treating validity as fixed-width values.** Single most
  likely correctness bug. *Mitigations:* bitmap-specific
  decode helper; architecture guard ensures no fixed-width
  decoder reuse; non-byte-aligned page-row-count test; fail-
  closed decoded-byte-length validation.
- **R2 — Full-buffer decode regression.** Byte concatenation
  would silently misalign bits across page boundaries on non-
  byte-aligned page row counts. *Mitigations:* shared
  `stitch_validity_pages_into_row_group_bitmap` helper; full-
  buffer decode oracle test; Phase 0 P9 audit; architecture
  guard.
- **R3 — Unsupported cascade silently materializes wrong
  validity.** *Mitigations:* closed V1 catalog (one member);
  exhaustive cascade match at the decline-narrow site;
  architecture guard.
- **R4 — Value/validity page-grid mismatch under future
  changes.** *Mitigations:* hard architecture guard;
  `PageGeometryMismatch` decline reason; future ADRs adjusting
  page geometry must explicitly amend or shelve the invariant.
- **R5 — Accidentally implementing ADR 070.** A code path
  inspecting decoded validity to suppress a planned value-
  page fetch would silently change semantics. *Mitigations:*
  architecture guard; all-null page test asserts the value
  page is still fetched.
- **R6 — Encoded-validity selected-byte saving doesn't pay.**
  If warehouse fixtures don't show clustered nullity at the
  expected scale, the writer economic gate declines everywhere
  and ADR 069 reduces to a no-op. *Mitigations:* Phase 0 P10
  fixture identification; Phase 7 three-way escape hatch.
- **R7 — Writer-economic-gate cost exceeds savings.**
  *Mitigations:* `MIN_VALIDITY_ENCODE_BYTES` floor avoids the
  encoder pass on small buffers; encoder runs in microseconds
  per page; writer-cost overhead measured in Phase 7.
- **R8 — Cross-replica nondeterminism.** *Mitigations:* Phase
  0 P4 determinism probe; hard promotion gate; encoder reuses
  ADR 057's existing deterministic implementation.
- **R9 — Pre-ADR-069 reader incompatibility.** *Mitigations:*
  deployment-cadence discipline per
  `plans/conventions/no-premature-backward-compat.md`; ADR
  067's `FsstRowSafe` rollout precedent; routing toggle stays
  internal; operator surface unchanged.
- **R10 — Sparse economics regress.** Adding encoded-validity
  bytes to the sparse-vs-dense comparison could push some
  workloads from sparse to dense. *Mitigations:* encoded-
  validity bytes participate in existing sparse byte and range
  gates; promotion artifact records total selected bytes.
- **R11 — Typed page-offset overflow.** *Mitigations:* writer
  helper uses `checked_add`; overflow returns `None` from the
  helper, keeping validity raw; focused test covers the case.

## Definition of done

- All 11 Phase 0 pins recorded in the bench-results artifact;
  P-fork outcome formally documented.
- `select_validity_cascade`,
  `decode_rle_validity_page`,
  `stitch_validity_pages_into_row_group_bitmap`,
  `materialize_encoding_validity_buffer`,
  `validity_v1_eligibility`,
  `EncodedValidityDeclineReason`,
  `PageMaterializeValiditySource`, and
  `WriterValidityCascadeRouting` all land as `pub(crate)`.
- Decline narrows
  `EncodedValidityBufferUnsupported` to non-V1-catalog
  cascades + unsupported shapes; covered by exhaustive cascade
  test.
- Architecture guards cover: routing-enum privacy, helper
  privacy, decline-narrow exhaustion, cascade-membership
  invariant, page-alignment invariant, bitmap-vs-fixed-width
  decode invariant, ADR 070 boundary, full-buffer stitch
  routing.
- All 9 hard promotion gates pass; 5 soft gates met or
  explicitly justified.
- Promotion commit lands writer-routing flip from
  `Disabled` to `Enabled` for production.
- ADR 064 §"V2 deferrals" and §"Where we are" amended to
  record closure of the encoded-validity follow-up.
- Roadmap entry #25 updated to record promotion.
