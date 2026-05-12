# ADR 069: Encoded-Validity Page Decoding for Sparse Projection

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/069_encoded_validity_sparse_projection.md`

## Status

Shipped after implementation and promotion evidence landed in
`plans/impl/069_encoded_validity_sparse_projection_bench_results.md`.
Council-converged across Claude / Codex / Gemini after four
iterations under `plans/wip/`.

This ADR is the tracked V2 follow-up to ADR 064's deferred
encoded-validity scope. ADR 064 §"V2 deferrals" explicitly
enumerated "encoded-validity page decoding ... if production
evidence shows it's load-bearing, follow-up ADR." ADR 069 settles
that follow-up.

Filename family follows ADR 064
(`064_nullable_sparse_projection.md`), of which ADR 069 is the
encoded-validity follow-up.

This ADR proposes no on-disk format change, no
`ENCODING_DIRECTORY_FORMAT_VERSION` bump, no operator-visible
configuration, and no public surface change. It introduces a
crate-private writer extension that emits page-aligned RLE-encoded
validity bitmaps in narrow eligibility, plus a sparse-projection
reader extension that narrows ADR 064's existing
`SparseProjectionFetchDecline::EncodedValidityBufferUnsupported`
decline to admit exactly one validity cascade.

## V1 contract summary

- **Integrated writer + reader.** Before ADR 069, production
  emitted no encoded validity buffers (the value-buffer writer
  path excluded validity ordinal 0 from cascade selection). ADR
  064's decline was forward-proof code with no producer. ADR 069
  creates the producer and sparse reader together, and promotion
  enabled the production writer route.

- **V1 encoded-validity catalog = exactly one member.** Validity-
  buffer encoding extends from `{Plain}` (today, unchanged — no
  `BufferEncoding` entry emitted) to `{Plain, Rle { inner:
  EncodingLeafCascade::Plain }}`. The RLE byte-run cascade reuses
  ADR 057's existing `EncodingCascade::Rle` variant; no new
  cascade variant, no format-version bump, no on-disk shape
  change. Other cascades remain
  `EncodedValidityBufferUnsupported` because their codecs model
  fixed-width values or whole-buffer payloads, not 1-bit-per-row
  bitmap lanes.

- **Validity is a bitmap, not a fixed-width value buffer.** The
  decoded byte length of a validity page is
  `ceil(page_row_count / 8)`, not
  `page_row_count × bytes_per_value`. ADR 069 lands a bitmap-
  specific page decode helper that returns a `PageValidityBitmap`
  with this exact length. The existing fixed-width value page
  decoder is never reused for validity; an architecture guard
  enforces. The decoder counts nulls only over the first
  `page_value_count` bits of the decoded bytes; tail-padding
  bits (positions `page_value_count` through
  `8 * ceil(page_value_count / 8) - 1`, zero-padded by the
  writer) do not contribute to `null_count`.

- **Writer emits page-local validity bitmaps.** When the writer
  chooses `Rle { inner: Plain }` for a validity buffer, it
  extracts each page's bits to a page-local bitmap (bit 0 = first
  row of the page), encodes that page-local bitmap as RLE byte-
  run, and records its byte length in `page_end_offsets`. The
  `BufferEncoding` carries
  `decoded_value_count = row_group.row_count` and
  `decoded_byte_len = ceil(row_group.row_count / 8)`. Each page's
  decoded form is self-contained.

- **Validity pages 1:1-aligned with value-buffer pages.** The
  writer emits one validity page per value page with the same
  `first_value_index` and `value_count`. Alignment is the load-
  bearing invariant: sparse projection uses the selected value-
  page index k directly as the selected validity-page index.
  Mismatched grids decline to dense.

- **Sparse-projection mask-first ordering preserved.** The exact
  survival mask decides which value and validity pages are
  fetched. Decoded validity bits are used only to build Arrow
  nullability for the selected projection page. Decoded validity
  does *not* suppress value page fetches, alter the survival
  mask, or decide page eligibility. All-null value-fetch elision
  is ADR 070's scope; an architecture guard pins the boundary.

- **Full-buffer Boolean decode requires page-stitching.**
  Validity emitted as page-local bitmaps cannot be reconstructed
  by byte-concatenating page payloads when `page_value_count` is
  not a multiple of 8. A new shared
  `stitch_validity_pages_into_row_group_bitmap` helper handles
  bit-level rebase from each page-local bitmap into the row-group
  bitmap at `page.first_value_index`. Used by
  `materialize_encoding_buffer` and `decode_encoded_buffer` (the
  existing full-buffer decode callers per Phase 0 P9 audit).

- **The decline narrows; it does not disappear.**
  `SparseProjectionFetchDecline::EncodedValidityBufferUnsupported`
  remains crate-private planner state and fires for any encoded
  validity buffer outside the V1 catalog: unsupported cascade,
  missing or empty `page_end_offsets`, value/validity page-
  geometry mismatch, `decoded_byte_len !=
  ceil(decoded_value_count / 8)`, or corrupt page descriptors.
  Internal `EncodedValidityDeclineReason` diagnostics distinguish
  these cases for tests and bench artifacts; they collapse to the
  public variant at the planner boundary.

- **Exact writer economics.** The writer emits RLE validity only
  when an exact byte-length check passes strictly:

  ```text
  rle_encoded_bytes + page_table_bytes
    < raw_validity_bytes * (1 - VALIDITY_RLE_BYTE_SAVING_FRACTION)
  ```

  `VALIDITY_RLE_BYTE_SAVING_FRACTION = 1/4` and
  `MIN_VALIDITY_ENCODE_BYTES = 256` start as `pub(crate) const`
  integer/rational constants; Phase 0 calibrates. Equality at the
  threshold declines to raw. Sample-based estimation (ADR 068's
  pattern) does not fit: validity buffers are 1 bit/row and small
  enough that exact RLE measurement is faster than estimation.

- **All-valid column short-circuit preserved.** ADR 064's
  `column_null_counts == 0` early-out continues to keep all-valid
  columns out of the validity-fetch path entirely; the writer
  emits no validity-buffer `BufferEncoding` entry for those
  columns.

- **Determinism is load-bearing.** Writer cascade selection for
  validity buffers must make byte-identical decisions across
  replicas: deterministic RLE encoder, integer-only economic
  gate, no wall-clock / random / hash-map-iteration / CPU-
  capability input.

- **Surfaces unchanged.** No new on-disk structure, no
  `EncodingCascade` / `EncodingComputeEvaluatorKind` /
  `L2FetchStrategy` variant, no `EncodingDirectory` shape change,
  no `ENCODING_DIRECTORY_FORMAT_VERSION` bump, no IPC
  `custom_metadata` key, no wire / descriptor / PBC / capability /
  digest / `ObjectStore` trait change, no Cargo feature, no
  operator-visible toggle, no metric label.

## Context

ADR 064 (Nullable Sparse Projection) shipped raw byte-addressable
validity for fixed-width nullable sparse projection. Its Phase 0
fork-resolution gate chose "raw-only proceeds; mixed declines on
encoded; encoded-only revises pre-code." V1 landed raw-only because
the writer at that time emitted no encoded validity buffers — the
decline at `sparse_projection_fetch.rs:252-262` was a forward-proof
guard against future writer state.

The current production raw path is:

1. Sparse projection derives selected value pages from an exact
   survival mask.
2. For nullable selected pages, raw validity uses
   `validity_bitmap_body_range_for_page` to fetch the minimal byte
   span covering the page's bits.
3. `rebase_validity_bitmap` shifts that raw span so bit 0 is the
   selected page's first row.
4. `projected_page_batch_from_decoded_buffers` assembles the
   nullable Arrow page batch and manager-side filtering applies
   the page-local survival mask.

This works well for raw validity. It falls back to dense when the
validity buffer is encoded, because sparse projection does not
currently know how to fetch and decode only the needed validity
pages.

The warehouse motivation is clustered-nullity columns where
validity bitmaps compress dramatically: append-only update streams
(tombstone runs), LSM-compacted tables (clustered nullity by
source-segment), optional descriptor fields populated only during
certain campaigns (wide all-zero or all-one stretches). For a 65K-
row page of all-valid rows, raw validity is 8 KiB; RLE byte-run
encoded is essentially one entry. The sparse projection narrowing
benefit compounds: fewer selected validity bytes per selected
value page.

### Bitmap-vs-fixed-width: the load-bearing correctness wrinkle

The most likely implementation bug is treating validity pages like
fixed-width value pages. A validity page with 1,024 rows decodes
to 128 bytes (`ceil(1024 / 8)`), not 1,024 bytes. The existing
fixed-width page-decode helpers assume
`page.value_count × bytes_per_value` and *will* silently produce
wrong-shaped output if reused for validity. ADR 069 lands a
dedicated bitmap-page decode helper to make this impossible by
construction.

### Why page-local emission matters

If the writer encodes a row-group validity bitmap as one stream
sliced by page-end byte offsets, then for any page whose
`page_value_count` is not a multiple of 8, the page payload's
first bit is at a non-zero bit offset within the first byte, and
the final bit may share its byte with the next page. Sparse
projection would have to fetch one byte beyond each selected page
boundary and bit-shift. Page-local emission (each page is a self-
contained bitmap starting at bit 0 of its own first byte)
eliminates this complexity at the cost of up to 7 wasted bits per
page tail.

## Decision

### 1. Writer extension

Add a private helper `select_validity_cascade` in
`src/storage/encoding/select.rs` (or `cascade/validity.rs` per
Phase 0 P6). Signature:

```rust
pub(crate) fn select_validity_cascade(
    raw_validity_bytes: &[u8],
    value_page_row_spans: &[ValuePageRowSpan],
    column_null_count: usize,
) -> Option<ValidityRleEncodingPlan>;
```

Returns `None` when any of:

- the column is all-valid (`column_null_count == 0`);
- the value buffer for the same column is not page-emitting
  (sparse projection has no selected pages to pair with);
- `raw_validity_bytes.len() < MIN_VALIDITY_ENCODE_BYTES`;
- accumulated `page_end_offsets` would overflow the typed
  `EncodingPageByteOffset` newtype (writer uses `checked_add`);
- the exact byte-saving threshold is not strictly exceeded.

For each value-page row span, the helper extracts the page-local
validity bitmap (`ceil(page_rows / 8)` bytes; bit 0 represents
`first_value_index`; tail bits zero-padded deterministically),
encodes it via ADR 057's RLE byte-run encoder, and accumulates
the per-page encoded byte length into `page_end_offsets`.

When `Some(plan)` is returned, the encoding-directory builder
emits a `BufferEncoding` entry for the validity buffer with
cascade `EncodingCascade::Rle { run_count: plan.run_count, inner:
Box::new(EncodingLeafCascade::Plain) }`, `page_end_offsets` from
the plan, `page_value_count` matching the value buffer's,
`decoded_value_count = row_group.row_count`, and
`decoded_byte_len = ceil(row_group.row_count / 8)`. The plan's
`run_count` is the sum of per-page run counts produced by the
RLE encoder across all page-local bitmaps in the row group (the
cascade-level count carries through ADR 057's existing
`EncodingCascade::Rle::run_count` field; per-page run counts are
recovered by the decoder from each page's encoded payload).

The existing `is_fixed_width_value_buffer` filter remains intact;
the validity-encoding route is a separate call site keyed on
`null_buffer_ordinal`.

### 2. Page-alignment invariant (hard)

For any row group's column with an encoded validity buffer:

```text
validity.page_end_offsets.len() == value.page_end_offsets.len()
forall k: validity_page[k].first_value_index == value_page[k].first_value_index
forall k: validity_page[k].value_count == value_page[k].value_count
```

Architecture-test guard enforces. Sparse projection consumes the
invariant by using the selected value-page index k directly as
the selected validity-page index. Mismatched grids decline via
`PageGeometryMismatch` (internal) →
`EncodedValidityBufferUnsupported` (public).

### 3. Reader: bitmap-specific page decode helper

```rust
pub(crate) fn decode_rle_validity_page(
    encoded_page: &[u8],
    page_value_count: usize,
) -> Result<PageValidityBitmap, ArrowError>;
```

Returns a `PageValidityBitmap { bytes, null_count }` where
`bytes.len() == ceil(page_value_count / 8)`. Internally decodes
the RLE byte-run payload, validates the decoded length against
the expected `ceil(...)` value, and counts nulls **only over the
first `page_value_count` bits** of the decoded bytes. Tail
padding bits (positions `page_value_count` through
`8 * bytes.len() - 1`) are ignored and do not contribute to
`null_count` — a 9-row all-valid page (decoded byte length 2,
trailing 7 padding bits zero-padded by the writer) must report
`null_count == 0`, not 7. Errors on decoded byte-length mismatch
(fail-closed) and on corrupt RLE payloads. Page-local encoded
validity has `bit_offset = 0` by writer construction; sparse
projection consumes the helper's output directly without a
rebase.

### 4. Full-buffer Boolean stitch helper + role-aware dispatch

```rust
pub(crate) fn stitch_validity_pages_into_row_group_bitmap(
    pages: &[(EncodingPageDescriptor, PageValidityBitmap)],
    row_group_row_count: usize,
) -> Result<Vec<u8>, ArrowError>;
```

Allocates `ceil(row_group_row_count / 8)` bytes and bit-shifts
each page-local bitmap into the row-group bitmap at
`page.first_value_index`. Errors on overlapping page row spans,
gaps, page-count mismatch, or row spans outside the row-group
row count. The inverse of `rebase_validity_bitmap`.

**Role-aware dispatch.** The existing
`materialize_encoding_buffer` /
`decode_encoded_buffer` signatures take `(encoded_buffer_bytes,
data_type, directory, encoding)` and do not carry buffer-role
context (validity vs. value); the encoding's
`ipc_buffer_ordinal` alone cannot distinguish without the
column's `null_buffer_ordinal`. ADR 069 introduces a separate
crate-private entry point for the validity case:

```rust
pub(crate) fn materialize_encoding_validity_buffer(
    encoded_buffer_bytes: &[u8],
    directory: &EncodingDirectory,
    encoding: &BufferEncoding,
    row_group_row_count: usize,
) -> Result<Vec<u8>, EncodingRejectCode>;
```

This is the production entry point for full-buffer decode of an
encoded-validity `BufferEncoding`. Internally it expands page
descriptors, decodes each page-local bitmap via
`decode_rle_validity_page`, and stitches via
`stitch_validity_pages_into_row_group_bitmap`. The original
`materialize_encoding_buffer` is unchanged and remains the
value-buffer entry point. Callers that materialize a column's
nullable buffer set dispatch on buffer role (the column's
`null_buffer_ordinal` is known at the call site). Phase 0 P9
audits every full-buffer decode call site and confirms each one
either already has buffer-role context or can acquire it via a
small plumbing addition. The architecture guard enforces that
`materialize_encoding_buffer` is never called on a
`BufferEncoding` whose `ipc_buffer_ordinal` matches the column's
validity ordinal.

### 5. Sparse-projection planner: decline narrowing

Replace the unconditional decline at
`sparse_projection_fetch.rs:252-262`. The raw-vs-encoded
dispatch lives at the call site (the planner already calls
`find_buffer_encoding(...)` to detect encoded validity); the
eligibility helper handles only the encoded case so its
signature requires the encoded buffer, its cascade, selected
value pages, and the row-group row count:

```rust
match find_buffer_encoding(input.encoding_directory, group_index,
                          column.null_buffer_ordinal,
                          validity_page.range) {
    None => { /* ADR 064 raw-validity path */ }
    Some(buf) => match encoded_validity_sparse_eligibility(
        buf, validity_cascade, selected_value_pages, group.row_count,
    ) {
        EncodedValiditySparseEligibility::Eligible => { /* new path */ }
        EncodedValiditySparseEligibility::Decline(_) => {
            return Decline(EncodedValidityBufferUnsupported);
        }
    }
}

fn encoded_validity_sparse_eligibility(
    validity_buf: &BufferEncoding,
    validity_cascade: &EncodingCascade,
    selected_value_pages: &[EncodingPageDescriptor],
    row_group_row_count: RowGroupRowCount,
) -> EncodedValiditySparseEligibility;

enum EncodedValiditySparseEligibility {
    Eligible,
    Decline(EncodedValidityDeclineReason),
}

enum EncodedValidityDeclineReason {
    UnsupportedCascade,
    MissingPageMetadata,
    PageGeometryMismatch,
    RowGroupRowCountMismatch,
    DecodedByteLenMismatch,
    CorruptPageDescriptor,
}
```

`EncodedValidityDeclineReason` is `pub(crate)` diagnostic detail
for focused tests and bench-results artifact rows; it collapses
to the existing public decline variant at the planner boundary so
the manager-visible decline taxonomy is unchanged. The helper
reads the cascade from `validity_buf.cascade_index`; no raw-path
variant in the return enum because the caller already routed raw
buffers (no `BufferEncoding`) to the ADR 064 path before
invoking the helper.

### 6. Encoded-validity fetch flow

For each selected value page of a nullable column with V1-eligible
encoded validity:

1. Resolve the selected value-page index k to validity-page index
   k by the 1:1 alignment invariant.
2. Compute the validity page's encoded byte range as
   `[validity_buf.encoded_range.start + prev_end,
   validity_buf.encoded_range.start + page_end_offsets[k])` where
   `prev_end = if k == 0 { 0 } else { page_end_offsets[k-1] }`.
   The `page_end_offsets` array is a prefix-end array per ADR
   057's convention; the implicit zero at the front is not
   stored.
3. Issue the fetch alongside selected value pages (ADR 060's
   existing multi-range plan; encoded-validity ranges coalesce
   transparently with value-page ranges per ADR 044's
   `TieredMultiRangeReadPlan`).
4. Decode via `decode_rle_validity_page`; the result is a page-
   local `PageValidityBitmap` with `bit_offset = 0`.
5. Feed the page-local bitmap to the existing nullable-array
   assembly path
   (`projected_page_batch_from_decoded_buffers`).

### 7. Materializer source-shape enum

```rust
pub(crate) enum PageMaterializeValiditySource {
    Raw { bit_offset: u8 },
    Encoded { cascade: EncodingCascadeIndex },
}
```

`PageMaterializeValidityFetch` carries this enum. The raw branch
runs ADR 064's existing path (`rebase_validity_bitmap` with the
recorded `bit_offset`); the encoded branch dispatches on cascade
(V1 catalog: exactly `Rle { inner: Plain }`) and runs the
bitmap-specific decode helper. Naming is semantic per
`plans/conventions/no-planning-phase-names.md`.

### 8. Writer routing toggle

Crate-private `WriterValidityCascadeRouting { Disabled, Enabled }`,
mirroring ADR 068's `FsstRowSafeEstimatorRouting` pattern. The
pre-promotion production route started `Disabled`; promotion
flipped the standard writer entry point to `Enabled`. Test/bench
helpers retain a disabled baseline for comparison. The directory-
builder helper carrying both ADR 068's FSST routing and ADR 069's
validity routing is `build_encoding_directory_for_ipc_stream_with_routing`.

### 9. Closed-catalog and decline-taxonomy discipline

The encoded-validity V1 catalog stays at exactly one member. The
architecture guard exhausts the closed cascade catalog: 7 of 8
catalog variants must trigger
`EncodedValidityBufferUnsupported`; exactly `Rle { inner: Plain }`
triggers the eligible path. Adding a cascade variant to ADR 057
in a future ADR forces the guard author to explicitly decide
validity-eligibility (the exhaustive `match` trips the compiler
at this site).

## Rationale

### Why writer + reader integrated

A reader-only ADR has no production test path: the decline guarded
by the reader fires zero times today and would continue firing
zero times after a reader-only V1. Promotion gates that require
byte savings against warehouse fixtures are unprovable without
the writer extension.

### Why narrow the decline instead of removing it

The agenda is "make supported encoded validity sparse," not
"promise every encoded validity shape is safe." Missing page
metadata, unsupported cascades, mismatched page geometry, decoded-
byte-length mismatches, and corrupt page descriptors are all real
reasons to stay dense. Keeping
`EncodedValidityBufferUnsupported` as fail-closed planner state
prevents unsupported encoded validity from becoming a partial-
materialization correctness hazard.

### Why exactly one cascade in V1

`Rle { inner: Plain }` is already in ADR 057's closed catalog and
is the natural fit for byte-run compression over a packed-bitmap
payload. Other cascades don't apply:

- `BitPacked`: codecs model fixed-width values, not 1-bit-per-row
  lanes. Validity is already 1 bit/row; bitpacking further is
  meaningless.
- `Dictionary`: 2 possible values (0/1) doesn't admit a useful
  dictionary at byte grain.
- `Delta`: validity bits aren't ordered numerically.
- `FsstWholeBuffer` / `FsstRowSafe`: string-token compression.
- `Alp`: float compression.
- `Lz4Frame`: not page-emitting in ADR 057 V1; would force a
  whole-buffer decode for sparse projection.

A future ADR could add a true bitmap-RLE cascade (sub-byte runs)
or bitmap-specific BitPacked / Dictionary variants if evidence
shows the lost compression matters. V1 stays scope-narrow.

### Why 1:1 page alignment

Sparse projection's whole benefit is "fetch only selected pages."
Independent value and validity page grids force split/stitch
planning; a single selected value page could require multiple
validity pages, and sparse economics become harder to reason
about. The writer can align validity pages to value pages
cheaply, so V1 takes that invariant and guards it.

### Why exact economic gate, not sample-based

Validity buffers are 1 bit/row → maximum ~131 KiB for a 1M-row
page. Running the exact RLE encoder over a few hundred KiB takes
microseconds; sample-based estimation has no meaningful CPU win
at this scale. ADR 068's pattern fits FSST (where per-buffer
training is expensive); it does not fit validity RLE.

### Why ADR 064 raw-validity stays the default

For columns where the writer economic gate declines (the common
case across warehouse and OLTP workloads after V1), the writer
continues to emit no validity-buffer `BufferEncoding` entry, and
ADR 064's existing path runs byte-identically. The new encoded-
validity path is gate-driven and engaged only when it pays.

## Consequences

- **One new bitmap-specific page-decode helper**
  (`decode_rle_validity_page`).
- **One new full-buffer stitch helper**
  (`stitch_validity_pages_into_row_group_bitmap`) for full-buffer
  decode callers; without it those callers would silently
  misalign bits across page boundaries when
  `page_value_count % 8 != 0`.
- **One new writer helper** (`select_validity_cascade`) that
  calls the existing RLE byte-run encoder once per page on the
  page-local bitmap.
- **Architecture guards.** Three new invariants: validity-buffer
  cascade is exactly `Rle { inner: Plain }`; validity pages
  align 1:1 with value pages; validity decode never reuses the
  fixed-width value page decoder. Plus the ADR 070 boundary
  guard (no all-null value-fetch elision) and the full-buffer
  stitch-routing guard.
- **All-valid column path unchanged.** ADR 064's
  `column_null_counts == 0` early-out continues to elide
  validity fetch entirely.
- **All-null page-skip remains ADR 070's scope.** Encoded-
  validity decode bits feed Arrow nullability assembly only;
  they cannot suppress already-planned value-page fetches.
- **No format-version bump.** Cross-version coordination is
  deployment-cadence per
  `plans/conventions/no-premature-backward-compat.md`; pre-
  ADR-069 readers that encounter writer-emitted RLE validity
  fail closed under the existing clean-break discipline. ADR
  067's `FsstRowSafe` rollout is the precedent.
- **Composability.** ADR 069 composes with ADR 063 page-stats
  (upstream page-pick), ADR 064 nullable sparse projection
  (raw-validity continues to work alongside encoded; both share
  the materializer), and ADR 065 dense predicate-zip (encoded-
  validity is on the projection side; predicate-zip already
  handles arbitrary projection geometry via page-local
  filtering). No surface changes on any composing ADR.

## Phase 0 Pins

Empirical calibration items the Phase 0 audit closes against
warehouse fixtures. None are council-blocking; P-fork is
essentially confirmed by iter-0 Explore evidence and formally
closes in Phase 0.

- **P-fork (writer scope).** Confirm production emits no
  validity-buffer `BufferEncoding` today. Expected: no. Closes
  integrated writer + reader scope. If unexpectedly yes, V1
  narrows to reader-only.
- **P1: `VALIDITY_RLE_BYTE_SAVING_FRACTION`.** Starts at 1/4.
  Sweep clustered, random, alternating, all-valid, and all-null
  fixtures; confirm zero-false-encode rate.
- **P2: `MIN_VALIDITY_ENCODE_BYTES`.** Starts at 256 raw bytes.
- **P3: ADR 060 multi-range coalescing.** Confirm
  `TieredMultiRangeReadPlan` coalesces encoded-validity byte
  ranges with value-page byte ranges.
- **P4: Cross-replica determinism probe.** Two-process RLE byte-
  run encode over packed-bitmap input; require byte-identical
  output.
- **P5: Warehouse page-row-count modularity.** Confirm
  `page_value_count` is typically a multiple of 8 in production
  writer config; non-multiples remain correct with at most 7
  padding bits per page.
- **P6: Estimator module location.** `cascade/validity.rs`
  sibling vs. co-located in `select.rs`; pick on file size and
  call-site clarity.
- **P7: Architecture-guard exhaustion.** Pin the exhaustive
  Rust `match` over all 8 catalog variants; future cascade
  variants trip the compiler at this site.
- **P8: Validity-buffer encoder reuse.** Confirm ADR 057's RLE
  byte-run encoder accepts a packed-bitmap byte slice as opaque
  input and matches hand-traced RLE on a small fixture.
- **P9: Full-buffer decode caller audit and role-dispatch
  shape.** Enumerate every call site of
  `materialize_encoding_buffer` / `decode_encoded_buffer` that
  could receive an encoded-validity `BufferEncoding`. For each,
  confirm the call site has buffer-role context (knows the
  column's `null_buffer_ordinal`) and can route validity buffers
  through `materialize_encoding_validity_buffer` rather than the
  generic page-byte-concatenation path. If a call site lacks
  role context, pin the smallest plumbing addition (caller-side
  null-ordinal lookup, role parameter, or split entry point).
  This audit is load-bearing: without it, full-buffer Boolean
  decode silently misaligns bits across page boundaries on
  non-byte-aligned page row counts. The architecture guard
  enforces that `materialize_encoding_buffer` is never called on
  a `BufferEncoding` whose `ipc_buffer_ordinal` matches the
  column's validity ordinal.
- **P10: Warehouse clustered-nullity fixture.** Add or identify
  a clustered-nullity nullable fixed-width fixture
  (5%-null clustered runs on a nullable `Int64`).
- **P11: Bench home.** Pick `benches/encoding_compute.rs`,
  `benches/row_group_fetch.rs`, or a focused sibling for the
  Phase 7 promotion matrix. Output as stdout diagnostic rows.

## Promotion criteria

Bench artifact at
`plans/impl/069_encoded_validity_sparse_projection_bench_results.md`
records the following gates. Hard gates are correctness /
contract; soft gates are performance targets the artifact can
fail and still promote with explicit recorded justification.

### Hard gates

1. **Dense oracle equality.** For every nullable sparse-
   projection fixture (raw, encoded-eligible, encoded-decline,
   all-valid, all-null page), the projected Arrow batches via
   the ADR 069 path are byte-identical to dense fallback.
2. **ADR 064 raw-validity regression-free.** Existing ADR 064
   fixtures continue to produce byte-identical output and non-
   regressed wall-clock; the new code path engages only when
   the writer economic gate passes.
3. **Unsupported encoded validity fails closed.** Non-V1
   cascades, missing `page_end_offsets`, geometry mismatch,
   decoded-byte-length mismatch, and corrupted page descriptors
   decline to dense via `EncodedValidityBufferUnsupported`.
4. **Bitmap-vs-fixed-width invariant.** Validity-page decode
   goes through `decode_rle_validity_page` and never reuses the
   fixed-width value page decoder.
5. **Page-alignment invariant.** For every encoded-validity
   `BufferEncoding`,
   `page_end_offsets.len() == value_page_end_offsets.len()` and
   row spans match 1:1.
6. **Full-buffer decode correctness.** Full-buffer decode
   callers of encoded-validity buffers produce row-group bitmaps
   byte-identical to the original Arrow validity input.
7. **All-null value-page-fetch invariant.** No code path
   inspects decoded validity to suppress a planned value-page
   fetch (ADR 070 boundary).
8. **Cross-replica determinism.** Encoded-validity bytes,
   validity-page byte ranges, and sparse projection plans are
   byte-equal across two writer / reader runs with the same
   input and config.
9. **Closed-catalog and closed-surface lockdown.** V1 accepts
   exactly `Rle { inner: Plain }` for encoded validity; the
   cascade match exhausts the closed catalog; no new public
   surface lands.

### Soft gates

10. **Encoded-validity byte reduction.** On clustered-nullity
    fixtures, selected validity bytes (encoded path) ≤ 25% of
    selected validity bytes (raw path).
11. **Uncompressible-validity decline.** On random or
    alternating-bit nullity fixtures, the writer economic gate
    leaves validity raw and read wall-clock is non-regressed
    vs. ADR 064.
12. **Total sparse bytes remain sparse.** Selected value +
    validity bytes (encoded path) remain below dense projection
    bytes on promotion fixtures.
13. **Wall-clock speedup.** ≥ 1.5× wall-clock speedup on
    clustered-5% sparse projection over the raw-validity ADR
    064 baseline, or the artifact explicitly justifies a
    smaller speedup (validity bytes are a fraction of total
    selected bytes; byte-reduction may not translate
    proportionally).
14. **Writer cost bounded.** Writer overhead ≤ 5% on non-
    eligible workloads (where the economic gate declines) and
    ≤ 10% on eligible workloads (where the gate engages), or
    the artifact explicitly justifies different thresholds.

The Phase 7 promotion evidence passed hard gates and explicitly
justified soft-gate deviations, so production initialization is
`Enabled` for the writer-routing toggle and the decline-narrow +
reader decode helper is production. The routing enum stays in the
codebase for bench/test comparison after promotion. ADR 064
§"Where we are" / §"V2 deferrals" records the closing of the
encoded-validity follow-up. Roadmap entry #25 is updated.

If a hard gate fails, the artifact records the gap. Council
decides among three explicit paths: revise V1 (adjust economic
gate, narrow eligibility, fix a determinism source); scope down
to reader-only decline-narrow with the bitmap decode helper,
deferring writer extension to a separate ADR; or shelve writer
extension until stronger fixture evidence emerges.

## Deferred (V2)

- All-null value-page skip (ADR 070).
- Mismatched value/validity page-grid stitching.
- Bitmap-specific `BitPacked` / `Dictionary` validity cascades.
- Sub-byte bitmap-RLE cascade variant.
- Variable-width nullable sparse projection.
- Page-stat-assisted validity-fetch skipping.
- Operator-visible sparse projection knobs.
- Public decline / fallback / metric labels for encoded-
  validity routing.

## What Not To Do

- Do not add all-null value-fetch elision to this ADR.
- Do not add a new cascade variant for bitmap RLE in V1.
- Do not accept encoded validity without page metadata.
- Do not accept unaligned value/validity page grids in V1.
- Do not treat validity as one byte per row.
- Do not reuse the fixed-width value page decoder for validity.
- Do not byte-concatenate page payloads for full-buffer Boolean
  decode.
- Do not add a new `L2FetchStrategy` variant or fork
  `sparse_projection_fetch.rs`.
- Do not add public metric labels or fallback reasons for
  private encoded-validity routing.
- Do not widen ADR 060's value-column sparse projection catalog.
- Do not change the `EncodingDirectory` byte layout or bump
  `ENCODING_DIRECTORY_FORMAT_VERSION`.
- Do not expose `WriterValidityCascadeRouting`,
  `EncodedValidityDeclineReason`, or other diagnostic types
  through any public API.
- Do not write encoded validity when the value buffer is not
  page-emitting (sparse projection has nothing to pair with).

## Council decisions

Compact archival summary. Iteration history lives in git on
`plans/wip/`.

1. **Integrated writer + reader, conditional on Phase 0
   P-fork.** Reader-only has no production consumer; Phase 0
   formally closes. Settled iter-1.
2. **V1 encoded-validity catalog = `{Rle { inner: Plain }}`.**
   Closed catalog with one member. Other cascades model fixed-
   width values, are not page-emitting in V1, or don't apply to
   1-bit payloads. Settled iter-1.
3. **1:1 value/validity page alignment as a hard invariant.**
   Enables single-lookup per-page validity fetch; mismatched
   grids decline. Settled iter-1.
4. **Page-local bitmap emission.** Each encoded validity page is
   self-contained (bit 0 = page's first row). Avoids non-byte-
   aligned page boundaries when `page_value_count % 8 != 0`.
   Codex raised iter-0; settled iter-2.
5. **Full-buffer Boolean stitch helper.** Page-local emission
   requires bit-level rebase for `materialize_encoding_buffer` /
   `decode_encoded_buffer`; byte-concatenation would silently
   misalign bits. Codex raised iter-0; settled iter-2.
6. **Narrow the decline, don't remove it.**
   `EncodedValidityBufferUnsupported` persists as fail-closed
   planner state for non-V1-catalog cascades, missing page
   metadata, geometry mismatch, decoded-byte-length mismatch,
   corrupt page descriptors, and page-offset overflow. Codex
   raised iter-0; settled iter-2.
7. **Internal `EncodedValidityDeclineReason` diagnostic enum.**
   Collapses to the existing public decline at the manager
   boundary. Used for focused tests and bench-results artifact
   rows. Claude raised iter-1; Codex adopted iter-2.
8. **Exact byte-length economic gate, not sample-based.**
   Validity buffers are too small for ADR 068's sample pattern
   to pay; RLE encoder runs in microseconds. Constants
   `MIN_VALIDITY_ENCODE_BYTES = 256`,
   `VALIDITY_RLE_BYTE_SAVING_FRACTION = 1/4`. Strict `<`
   boundary. Settled iter-1.
9. **Page-offset overflow returns `None`.** Writer helper uses
   `checked_add` on the prefix-end array; overflow leaves
   validity raw. Codex raised iter-1; settled iter-2.
10. **Materializer source enum = `{Raw, Encoded { cascade }}`.**
    Dispatcher-pattern shape; cascade carried as data. Claude
    raised iter-1; Codex briefly counter-proposed `Rle
    { cascade }` iter-1, walked back iter-2. Settled iter-3.
11. **Crate-private `WriterValidityCascadeRouting { Disabled,
    Enabled }`.** Mirrors ADR 068's
    `FsstRowSafeEstimatorRouting` pattern. Production started
    `Disabled` before promotion and now initializes `Enabled`;
    the disabled route remains for bench/test comparison.
    Settled iter-1.
12. **No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.** Clean-
    break cross-version compatibility per
    `plans/conventions/no-premature-backward-compat.md`; ADR
    067 `FsstRowSafe` rollout precedent. Settled iter-1.
13. **All-null value-page skip remains ADR 070's scope.**
    Architecture guard pins the boundary. Settled iter-1.
14. **No new public surface.** No new `L2FetchStrategy` /
    `EncodingCascade` / `EncodingComputeEvaluatorKind` / reject
    / fallback variant; no IPC `custom_metadata` key; no wire /
    descriptor / PBC / capability / digest / `ObjectStore` /
    Cargo feature / metric label / operator config. Settled
    iter-1.
