# Implementation plan: ADR 043 — Arrow IPC Body Compression (LZ4)

Companion to `plans/adr/043_arrow_ipc_body_compression.md`.

## Goal

Implement `RecordBodyCompressionPolicy::Lz4Frame` as a
per-table opt-in storage compression policy that wraps
Arrow IPC payloads inside slab records via Arrow's stock
`BodyCompression`. Default by layout: `MultiRow → Lz4Frame`,
`SingleRow → Uncompressed`.

Reuse Arrow's `ipc_compression` Cargo feature (already
enabled). No new envelope version, no new wire-protocol
frame, no PBC / AGG / stats protocol change. Two
storage-internal IPC encoders (`encode_arrow_schema_ipc`,
slab-footer CSB encoder) stay uncompressed by
architectural-test enforcement.

ADR 043 is the **substrate** for a future high-priority L2
columnar fetch ADR (per-buffer LZ4 means each column buffer
decompresses independently from its 64 KiB AEAD chunk),
not the implementation of column-level Range-GETs against
L2.

## Non-goals (V1)

- No homerolled encoding cascades (Brainstorm
  011 V2 / V3).
- No per-buffer page directory for O(1) point-read
  decompression.
- No L2 columnar Range-GET / page-fetch directory.
- No provider-native object-store filtering (S3 Select).
- No Parquet / SSE cold-tier fork.
- No encryption chunk-layout or AEAD range-authentication
  redesign.
- No zstd, snappy, or other codecs.
- No compression of slab-footer CSB, descriptor schema IPC,
  or `SlabRecordHeader`.
- No wire-frame compression.
- No envelope `format_version` bump.
- No new node capability bit.
- No `SingleRowLayoutDescriptor` refactor.
- No project-name-prefixed names.
- No new `V1` / `V2` suffixes.
- No dedicated compression-migration sweeper pass.
- No per-column compression policy.

## Phase 0 — Contract freeze and golden fixtures

Lock the descriptor surface, the writer-options helper
shape, and the L2 cold-tier scope.

Outputs:

- `RecordBodyCompressionPolicy { Uncompressed, Lz4Frame }`
  in `src/schema/table.rs`, parallel to
  `RecordLayoutPolicy`.
- `record_body_compression_policy: RecordBodyCompressionPolicy`
  field on `TableSchemaDescriptor`. Default function is
  layout-driven: `MultiRow → Lz4Frame`,
  `SingleRow → Uncompressed`. Persisted state is always
  explicit.
- Newtype `CompressionRatio(f32)` in `src/schema/units.rs`
  (encoded / plaintext, finite and >= 0; values > 1
  indicate compression overhead exceeded savings on
  high-entropy inputs — the newtype validator rejects NaN
  and infinity but does not cap at 1).
- Centralized helper `ipc_write_options_for(descriptor) ->
  IpcWriteOptions` exposed from a new `src/storage/ipc.rs`
  (or inside `src/storage/multirow.rs` if surface stays
  small). Two storage-internal exclusions are *not*
  reachable through this helper:
  - `encode_arrow_schema_ipc` in `src/schema/table.rs` —
    uses `IpcWriteOptions::default()` only.
  - slab-footer CSB encoder in `src/storage/stats.rs` /
    `src/storage/manager.rs` — uses
    `IpcWriteOptions::default()` only.
- Helper return type for record-body encode:

  ```rust
  struct EncodedRecordBatch {
      bytes: Vec<u8>,                          // bytes to write to slab
      plaintext_byte_len: u64,                  // uncompressed IPC length
      encoded_byte_len: u64,                    // == bytes.len()
      codec: RecordBodyCompressionPolicy,
  }
  ```

  The two byte counts are load-bearing for §3 of the ADR
  (uncompressed-axis enforcement vs. compressed-axis
  accounting). `EncodedRecordBatch` does **not** expose
  Arrow-buffer offsets for sub-record L2 Range-GETs; the
  follow-up L2 columnar fetch ADR adds that surface in its
  own helper.

- L2 scope (matches ADR 043 §12):
  - ADR 043 preserves existing slab-level CSB pruning and
    per-record Range-GET behavior over L2 objects.
  - ADR 043 shrinks admitted record bodies but still
    fetches whole admitted record bodies from L2.
  - Column-aware Range-GET pushdown is a follow-up ADR —
    not a Phase 9 addition to this plan and not a benchmark
    exit criterion below.

- Golden fixtures:
  - one uncompressed multi-row IPC byte-for-byte unchanged
    against the ADR 042 fixture set;
  - one Lz4Frame-compressed multi-row IPC over the same
    logical rows; fixture asserts *semantic round-trip*
    and *FlatBuffer-metadata presence* (`BodyCompression
    == LZ4_FRAME`), **not** raw payload bytes — LZ4 output
    may shift across `arrow` / `lz4_flex` upgrades without
    semantic change.
  - a mixed-shape slab (one record uncompressed, one
    record compressed) round-trips through the read path
    identically.

## Phase 1 — Descriptor + writer-options helper

Files:

- `src/schema/table.rs`
- `src/schema/units.rs`
- `src/schema/mod.rs`
- `src/storage/ipc.rs` (new) or `src/storage/multirow.rs`
- All `TableSchemaDescriptor` constructor literals
  (predominantly tests).
- `src/network/control_server.rs` (schema-update path)

Work:

1. Add `RecordBodyCompressionPolicy` enum.
2. Add `record_body_compression_policy` field on
   `TableSchemaDescriptor`. Layout-driven default via
   `new_with_layout(...)` constructor helper.
3. Include the field in descriptor fingerprint computation
   (`compute_schema_fingerprint`).
4. The field is required in serialized form; **no
   `#[serde(default)]` shim**. Per
   `plans/conventions/no-premature-backward-compat.md`,
   existing descriptor stores require a one-time pre-deploy
   re-encoding pass. The migration tool reads each
   existing descriptor (which lacks the new field),
   re-encodes it with `record_body_compression_policy` set
   per the layout-driven default, recomputes
   `schema_fingerprint` over the new shape, and atomically
   swaps the stored descriptor and its fingerprint
   together. The same discipline applies to other
   fingerprinted descriptor fields such as `stats_policy`
   and `record_layout_policy`: there is no tolerant serde
   decode shim for missing persisted fields. (A
   `#[serde(default)]` shim would default the field on
   decode but leave the stored fingerprint stale, and
   `validate()` recomputes the fingerprint on every load —
   `TableSchemaDescriptorV1::validate()` in
   `src/schema/table.rs` —
   so the next read after upgrade would fail with a
   descriptor-fingerprint-mismatch error.)
5. Permit explicit policy edits without forcing a layout
   recluster — compression is a write-time physical policy;
   existing records remain readable from Arrow IPC
   metadata.
6. No `Auto` / `Default` / reserved-codec variant in the
   enum.
7. Implement `ipc_write_options_for` returning default
   `IpcWriteOptions` for `Uncompressed` and
   `IpcWriteOptions::default().try_with_compression(Some(
   CompressionType::LZ4_FRAME))` for `Lz4Frame`.
8. Add `CompressionRatio(f32)` newtype.
9. Architectural-test guards (three layers):
   - **Call-site allowlist**: enumerate every storage-side
     `StreamWriter::try_new` site handling record bodies.
     New ones must route through `ipc_write_options_for`.
     Wire-side sites and the two record-body exclusions
     (`encode_arrow_schema_ipc`, slab-footer CSB encoder)
     stay on default options.
   - **Brand-prefix grep guard** (character-class
     self-defending so the regex does not match itself
     when scanned against `plans/`):

     ```sh
     rg -n 'wendelon_.*compressio[n]|Wendelo[n].*Compressio[n]|WCompressio[n]|WVorte[x]|WLC[E]' \
       src tests benches plans
     ```

     Must return empty.
   - **Encoder exclusion guard**: `encode_arrow_schema_ipc`
     and the slab-footer CSB encoder reject
     `IpcWriteOptions` with compression enabled.

Tests:

- `RecordBodyCompressionPolicy` round-trips through
  descriptor serialization (postcard).
- `new_with_layout` returns the right default per layout.
- Descriptor fingerprint changes when policy changes.
- Multi-row create defaults to `Lz4Frame`; single-row
  create defaults to `Uncompressed`.
- Explicit `SingleRow + Lz4Frame` round-trips
  (validate-and-allow per ADR §1).
- Compression policy alter accepted without requiring
  layout migration.
- `ipc_write_options_for(Uncompressed)` decodes
  identically to today.
- `ipc_write_options_for(Lz4Frame)` carries `LZ4_FRAME` in
  the FlatBuffer header.
- All existing tests pass with defaults threaded through.
- No new V1/V2 suffix on ADR 043 surfaces.

Exit:

- Descriptor carries compression policy.
- Writer-options helper centralizes codec selection.
- Three architectural guards in place.

## Phase 2 — Writer integration

Files:

- `src/storage/multirow.rs` (`encode_batch`,
  `pack_record_batch_by_bucket`,
  `encode_multirow_from_survivors`)
- `src/storage/manager.rs` (single-row encode path)
- `src/storage/core.rs` (snapshot/recovery encode helpers)

Work:

1. Replace ad hoc storage IPC encoding with one helper:

   ```rust
   pub fn encode_record_batch(
       batch: &RecordBatch,
       policy: RecordBodyCompressionPolicy,
   ) -> std::io::Result<EncodedRecordBatch>;
   ```

2. Thread policy through `pack_record_batch_by_bucket`,
   single-row write, and rewrite helpers from the
   descriptor.
3. For storage-internal helpers in `src/storage/core.rs`
   without a descriptor in scope, default to
   `RecordBodyCompressionPolicy::Uncompressed`.
4. Compute record checksum / payload hash over **encoded**
   bytes (the bytes that hit disk). Slab record header
   `length` field carries encoded byte length.
5. Charge stored bytes from **encoded** bytes plus padding
   and encryption overhead.
6. Map all Arrow encode errors to
   `std::io::ErrorKind::InvalidData` with codec context.

Tests:

- Multi-row write under `Lz4Frame` produces an IPC frame
  whose FlatBuffer header carries `LZ4_FRAME`.
- Multi-row write under `Uncompressed` produces a
  byte-for-byte identical frame to today (golden fixture).
- Single-row write under operator-opted-in `Lz4Frame`
  produces a compressed frame.
- Low-entropy multi-row fixture produces fewer encoded
  bytes under `Lz4Frame` than `Uncompressed`.
- Compressed and uncompressed records coexist in the same
  active slab.
- `EncodedRecordBatch` does not expose or promise
  Arrow-buffer offsets for sub-record L2 Range-GETs. The
  follow-up L2 columnar fetch ADR adds that surface in its
  own helper or directory; ADR 043's API stays at the
  record-body granularity.
- Record checksum / payload hash computed over encoded
  bytes.
- Stored-byte accounting reflects encoded bytes.

## Phase 3 — `max_record_bytes` enforcement axis

Files:

- `src/storage/multirow.rs`
- `src/storage/manager.rs`
- table-write tests

Work:

1. `max_record_bytes` enforced against the **uncompressed**
   IPC body (§3 of the ADR). The packer encodes once
   uncompressed first to measure, then encodes compressed
   for the actual write when the policy says `Lz4Frame`.
   Streaming-estimate path is a future refinement.
2. Reject any input batch where a single row exceeds
   `max_record_bytes` after schema overhead — the
   existing ADR 042 invariant — measured against
   uncompressed bytes.
3. `target_rows`, `max_rows`, and `max_record_bytes` cap
   the packing decision against the uncompressed axis.
   Compression does not raise these caps.
4. Quota accounting (Phase 2 step 5) uses encoded bytes;
   no interaction with the enforcement axis.

Tests:

- `max_record_bytes` rejects a batch that exceeds the cap
  on uncompressed bytes even when compressed bytes would
  fit.
- `max_record_bytes` admits a batch that fits the cap on
  uncompressed bytes regardless of compressibility.
- Oversized single-row reject path unchanged at the row
  granularity.
- Quota accounting reflects encoded bytes for compressed
  records and uncompressed bytes for uncompressed records.

## Phase 4 — Reader integration

Files:

- `src/storage/multirow.rs` (`decode_batch`)
- `src/storage/core.rs` (recovery decoder)
- `src/storage/manager.rs` (point-read and scan paths)

Work:

1. Verify `StreamReader::try_new(Cursor::new(bytes), None)`
   handles compressed buffers transparently with
   `ipc_compression` feature on. Arrow 58.x does this
   natively; verification is a unit test, not a code
   change.
2. No descriptor lookup at read time. The IPC FlatBuffer
   header carries the compression metadata; the reader
   dispatches internally.
3. Recovery decoder sees both shapes transparently.
   Verify by reconstructing index entries from a
   mixed-compression fixture slab.
4. Build-profile assertion: `ipc_compression` is in
   `default-features` for every production profile.
5. Keep L2 reads at current granularity. Cold scans may
   fetch fewer bytes per admitted record (because
   compressed bodies are smaller), but they still
   Range-GET whole admitted record bodies at the current
   encrypted-object chunk granularity. Do not add query
   column-set parameters, Arrow-buffer offset indexes, IPC
   file-footer emission, or AEAD range-layout changes in
   ADR 043. Those belong to the follow-up L2 columnar
   fetch ADR.

Tests:

- Round-trip an `Lz4Frame` batch through
  `encode_record_batch` / `decode_batch`; resulting
  `RecordBatch` equals input.
- Point read on a compressed multi-row record returns the
  same row as the equivalent uncompressed record.
- Range scan over a slab with mixed shapes returns the
  union of rows.
- Recovery scan rebuilds identical `RecordLocation`
  entries from a mixed-compression slab.
- Build-profile test: `ipc_compression` is in default
  features for all production profiles.
- Cold-read path over compressed records produces the same
  results without introducing column-buffer Range-GET
  behavior.

## Phase 5 — Stats writer composition (ADR 039)

Files:

- `src/storage/stats.rs`

Work:

1. Confirm the stats writer iterates rows via the same
   `decode_batch` helper Phase 4 verifies. If yes, no code
   change.
2. If the stats writer constructs its own decode path
   (audit to verify), route through the centralized
   decoder.
3. Verify the slab-footer CSB itself stays uncompressed.
   The footer encoder consumes `IpcWriteOptions::default()`
   only (architectural-test guard from Phase 1).

Tests:

- CSB stats over a compressed multi-row slab equal stats
  over the same rows in an uncompressed slab.
- Stats pruning admission decisions byte-identical pre/post
  compression flip.
- Slab-footer CSB bytes round-trip without compression
  (architectural test).
- Descriptor schema IPC bytes round-trip without
  compression (architectural test).

## Phase 6 — Read path: PBC + AGG composition

Files:

- `src/storage/aggregate.rs`
- `src/network/data_server.rs` (scan / aggregate handlers)

No code change expected. Verification pass:

1. Run existing PBC integration tests against tables
   configured with `Lz4Frame`. Outputs must match the
   uncompressed baseline.
2. Run existing AGG integration tests likewise.
3. Verify the CSB short-circuit (ADR 040 §5 + ADR 042 §11)
   continues to operate on uncompressed per-slab visibility
   metadata.

Tests:

- PBC programs over `Lz4Frame` tables produce identical
  results to uncompressed equivalents on the same logical
  rows.
- AGG `count_star` / `min` / `max` / `sum` matches across
  shapes.
- CSB short-circuit hit-rate metrics within noise of
  uncompressed baseline.
- Synthetic columns stripped from compressed records
  before user-visible output.

## Phase 7 — Compaction and migration

Files:

- `src/storage/sweeper.rs`
- `src/storage/manager.rs` (rewrite emitters)
- `src/storage/multirow.rs` (`encode_multirow_from_survivors`,
  `merge_multirow_streams`)

Work:

1. Every rewrite emitter calls
   `ipc_write_options_for(&descriptor)` to pick the codec.
   Compaction outputs follow the *current* descriptor.
2. Single-coherent-compaction primitive (ADR 042 §13)
   covers convergence: layout transition, tombstone
   metadata upgrade, supersession metadata upgrade, *and
   now* compression policy migration land in one
   pointer-swap.
3. No new sweeper pass.
4. Operator-driven recompact uses the existing
   `recluster_sealed_table_layout` path (ADR 042 Phase 7).

Tests:

- Recompact over a slab with `Untracked` tombstone +
  `Untracked` supersession + uncompressed IPC produces an
  output with tracked tombstone + tracked supersession +
  compressed IPC, in one pointer-swap.
- Tombstone-aware reclaim preserves the descriptor's
  compression policy on output.
- Layout migration applies the new layout's compression
  default.
- Descriptor edit `Lz4Frame → Uncompressed` results in
  uncompressed re-emit on next coherent compaction; the
  convergence gauge (Phase 8) reflects the backlog drain.
- No code path introduces a compression-only sweep.

## Phase 8 — Observability

Files:

- `src/metrics/registry.rs`
- metric smoke tests
- `tests/architecture_test.rs`

Metrics — `{codec}` label set; functional segment names;
brand prefix permitted only as the optional repo-level
Prometheus namespace wrapper. Tenant-axis labeling is left
to the metric registry's existing per-tenant conventions
and is not part of the ADR 043 contract.

```text
storage_compression_records_written_total{codec}     // counter
storage_compression_plaintext_bytes_total{codec}     // counter
storage_compression_encoded_bytes_total{codec}       // counter
storage_compression_encode_micros{codec}             // histogram
storage_compression_decode_micros{codec}             // histogram
storage_compression_decompressed_bytes_total{codec}  // counter
storage_compression_decode_error_total{codec}        // counter
storage_compression_l2_record_body_bytes_fetched_total{codec} // counter
storage_compression_ratio{codec}                     // histogram
storage_compression_uncompressed_resident_bytes      // gauge (unlabeled)
```

The unlabeled `uncompressed_resident_bytes` gauge measures
currently-resident uncompressed IPC bytes across sealed
and tiered slabs whose current descriptor selects
`Lz4Frame`. The codec dimension is omitted because the
metric's filter (descriptor-current = `Lz4Frame`) makes
"uncompressed codec" the only meaningful value.

The refresh that derives this gauge may inspect tiered
slab bodies. It is an explicit maintenance / compaction
diagnostic and must not run from a Prometheus scrape path.

`storage_compression_l2_record_body_bytes_fetched_total`
counts admitted whole-record L2 bytes after the record body
is fetched and its Arrow IPC codec can be read. It is a
Phase 11 stabilization signal, not a new columnar fetch
feature.

Architectural tests:

- Metric names match convention (no `wendelon_*` /
  brand-prefix on protocol segment).
- New metrics appear in metric-registry golden output.
- Grep guard from Phase 1 stays green.

Tests:

- Metrics render with both codec labels after a fixture
  write.
- Encoded/plaintext byte counters reflect LZ4 shrinkage on
  a low-entropy fixture.
- Decode error counter increments on malformed compressed
  IPC.
- `uncompressed_resident_bytes` gauge reflects mixed-shape
  state on `Lz4Frame`-configured tables; decays as
  coherent compaction re-emits.

## Phase 9 — Recovery and operator tooling

Files:

- `src/storage/manager.rs` (recovery scan)
- `tools/dump_slab_record` (small new helper if not
  already present)

Work:

1. Recovery scan iterates slab records and decodes IPC
   transparently. Add fixture-driven test for crash
   recovery on a node with a mixed-compression slab
   corpus.
2. Operator helper for raw-bytes inspection: decode IPC
   (compressed or not), strip synthetic columns,
   pretty-print the resulting batch.
3. Documentation note: recovery requires `ipc_compression`
   Cargo feature on (Phase 4 pins this in build profile).

Tests:

- Crash recovery on mixed compressed/uncompressed slabs
  reproduces the in-memory index identically.
- Recovery completes without descriptor context.

## Phase 10 — Benchmarks

Files:

- `benches/multi_row_fragments.rs` (extend existing
  harness)

Benchmarks:

- Multi-row write of 65,536 rows under `Uncompressed` vs.
  `Lz4Frame`. Targets: encode CPU within 25% degradation;
  on-disk bytes reduced by 50%+ on a typical analytical
  schema.
- Full-scan PBC over admitted compressed vs. uncompressed
  records. Target: scan throughput within 10% of baseline.
- AGG `count_star` / `min` / `max` over both shapes.
- Point read from compressed multi-row records at 256 KiB,
  1 MiB, and 8 MiB `max_record_bytes` settings. Target:
  amplification metric matches expected bound on the
  uncompressed axis.
- Cold-storage (L2) read latency under both shapes. Report
  bytes fetched as **whole admitted record bodies**;
  ADR 043 is not expected to reduce bytes for individual
  columns inside an admitted record.
- Optional diagnostic estimate: projected-column bytes
  versus fetched-record bytes after decode. This informs
  the follow-up L2 columnar fetch ADR but must not become
  an ADR 043 feature or exit criterion.
- Mixed-compression slab range scan. Target: no
  measurable per-record dispatch overhead.
- Low-entropy, medium-entropy, and high-entropy fixtures
  so the council sees when LZ4 is neutral or negative
  (informs future tightening on the SingleRow + Lz4Frame
  combination).

Exit criteria:

- Compression ratio histogram in 0.3–0.6 range on the
  default analytical schema (i.e. 40–70% bytes reduction).
- Full-scan throughput within 10% of uncompressed
  baseline.
- Point-read amplification metric matches `max_record_bytes`
  on the uncompressed axis.
- L2 cold-read bytes shrink only through smaller admitted
  record bodies; no benchmark may imply provider-side
  filtering or column-level Range-GET pushdown landed in
  ADR 043.
- No regression in any existing benchmark under
  `Uncompressed` default.

## Phase 11 — Stabilization watch (post-merge)

Mirrors ADR 039 / 040 / 042 discipline:

- `storage_compression_ratio{codec="lz4_frame"}`.
- `storage_compression_decode_micros{codec="lz4_frame"}`.
  High decode + low stats prune ratio = regression
  candidate.
- `storage_compression_encode_micros{codec="lz4_frame"}`.
- `storage_compression_uncompressed_resident_bytes` gauge
  — convergence signal; should decay monotonically.
- `storage_compression_decode_error_total` — corruption /
  version drift; expected zero.
- `storage_compression_l2_record_body_bytes_fetched_total`
  by codec — whole admitted record-body bytes fetched from
  L2. This should fall with LZ4 through smaller record
  bodies only; it is not evidence of provider-side
  filtering or column-level Range-GET pushdown.
- AGG / scan latency before-vs-after on representative
  tenants.
- `pbc_slab_stats_pruned_total /
  pbc_slab_stats_consulted_total` unchanged in
  expectation.
- `storage_multirow_point_read_amplification_bytes` —
  measures the uncompressed axis; compression must not
  mask decode amplification.
- Whole-record L2 fetch bytes for admitted records. This
  should fall with LZ4; projected-column overfetch
  remains a separate signal for the future L2 columnar
  fetch ADR.
- Stored ciphertext quota deltas; tenants see headroom at
  policy flip.
- Operator opt-outs from `Lz4Frame` back to
  `Uncompressed`; high rate → revisit default.
- Architectural-test enforcement: writer call sites,
  encoder exclusions, no envelope `format_version` bumps,
  no brand-prefix surfaces.
- Operator runbook:
  `plans/ops/runbooks/adr043_storage_body_stabilization.md`.

## Verification gate

Required before closing the implementation pass:

- `cargo test --lib --quiet`
- `cargo test --test architecture_test --quiet`
- `cargo bench --bench multi_row_fragments --no-run`
- `cargo bench --bench multi_row_fragments -- --quick`
- `git diff --check`
- `rustfmt --check --edition 2024` on touched Rust files
- Brand-prefix grep guard (character-class self-defending
  so the regex does not match itself when scanned against
  `plans/`):

  ```sh
  rg -n 'wendelon_.*compressio[n]|Wendelo[n].*Compressio[n]|WCompressio[n]|WVorte[x]|WLC[E]' \
    src tests benches plans
  ```

  Must be empty.

- Envelope-version grep guard:

  ```sh
  rg -n 'format_version\s*[=:]\s*[2-9]' src
  ```

  Must be empty on the ADR 043 diff scope.

- Grep for new V1/V2 suffixes on ADR 043 surfaces (must be
  empty).
