# Implementation Plan: ADR 057 — Per-Buffer Pages and Encoding Cascades

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR:
`plans/adr/057_per_buffer_pages_and_encoding_cascades.md`

Inherits write-path bifurcation:
`plans/adr/003_stellarator_bifurcated_writes.md`.
Inherits chunked AEAD authenticator:
`plans/adr/020_l2_envelope_encryption_and_dek_lifecycle.md`.
Inherits unencrypted footer / offset index:
`plans/adr/022_l2_slab_format_footer_indexed_range_reads.md`.
Inherits multi-row record layout:
`plans/adr/042_analytical_multi_row_fragments.md`.
Inherits per-buffer LZ4_FRAME compression:
`plans/adr/043_arrow_ipc_body_compression.md`.
Inherits column-projected fetch substrate:
`plans/adr/044_l2_columnar_fetch.md`.
Inherits canonical statistics rules:
`plans/adr/039_slab_footer_column_statistics_v1.md`.
Inherits secondary-index hit shape:
`plans/adr/052_secondary_indexes.md` §3.
Inherits LSM slab-writer composition:
`plans/adr/055_lsm_leveled_compaction_substrate.md` §5.
Inherits row-group + page-directory substrate:
`plans/adr/056_row_group_fetch_and_page_directory.md`.

## Status

Council-converged and promoted after implementation plus Phase
10 bench evidence. Inner layer of the
storage hierarchy. Opens on top of ADR 056's accepted Phase
10 bench artifact.

Implementation contract is locked at the council level: 8
cascades, compact directory with cascade dedup and
`page_end_offsets` prefix array, 5-code reject taxonomy,
node-local `EncodingWriterConfig` activation (no descriptor
policy), Phase D internal to `RowGroup` leg, ≤ 75% writer-
cost gate as starting target with mandatory compression +
read-side wins, decode-only V1, COCD deferred to ADR 058.

V1 implementation commitments (pinned by the companion ADR):

- decode-only Encoding substrate; compute-over-encoded-data is
  ADR 058 territory;
- closed cascade catalog: `Plain`, `Lz4Frame`, `BitPacked`,
  `Rle`, `Dictionary`, `Delta`, `Fsst`, `Alp`. Composition
  one level deep via `Box<EncodingLeafCascade>`;
- node-local `EncodingWriterConfig` is the activation surface;
  deployment-wide for cross-replica byte-equality;
- `EncodingDirectory` carried in IPC schema custom
  metadata under literal key
  `storage.encoding_directory`;
- writer cascade-selection heuristic is closed and rule-
  based, not ML;
- planner Phase D materialization layered on ADR 056's
  three-phase plan;
- pre-057 records (no `encoding_directory` key) read
  via ADR 056 / ADR 044 paths transparently;
- promotion gated by Phase 10 bench acceptance.

## Goal

Land the Encoding substrate as a self-contained module under
`src/storage/encoding/`. Cascades, page directory, materializer,
writer cascade-selection, and Phase D planner extension all
live inside the Encoding module; ADR 056's row-group module stays
unchanged.

The implementation:

- 16 newtype-units in `src/schema/units.rs`;
- `EncodingDirectory`, `BufferEncoding`,
  `EncodingPageDescriptor`, `EncodingCascade`, `EncodingLeafCascade`,
  `EncodingCascadeKind`, `EncodingWriterConfig`, `L2EncodingFetchConfig`,
  `EncodingRejectCode`, `EncodingFallbackReason` in
  `src/storage/encoding/mod.rs` and submodules;
- per-cascade encoder + decoder modules
  (`src/storage/encoding/cascade/{plain,lz4,bitpacked,rle,dict,
  delta,fsst,alp}.rs`);
- cascade-selection heuristic in
  `src/storage/encoding/select.rs`;
- directory serializer + parser in
  `src/storage/encoding/directory.rs`;
- materializer entry point in
  `src/storage/encoding/materialize.rs`;
- planner Phase D extension in `src/storage/columnar.rs`;
- ADR 052 secondary-index `fetch_indexed_rows` extension;
- bounded `storage_l2_encoding_*` metric family;
- 5-code `EncodingRejectCode` taxonomy;
- audit lifecycle events;
- bench matrix per ADR §"Open questions" Phase-10 list.

## Deliverables

- 16 newtype-units in `src/schema/units.rs`:
  `BitsPerValue`, `BaseValue`, `RunCount`,
  `DictValueCount`, `Exponent`, `Factor`,
  `ExceptionCount`, `EncodingPageIndex`,
  `EncodingPageByteOffset`, `EncodingCascadeIndex`,
  `BufferValueIndex`, `BufferValueCount`,
  `SimdPageValueCount`, `SampleRowCount`,
  `EncodingPageCount`, `TaskCount`.
- `src/storage/encoding/mod.rs` (new) — top-level types and
  module exports.
- `src/storage/encoding/cascade/{plain,lz4,bitpacked,rle,dict,
  delta,fsst,alp}.rs` (new) — eight per-cascade
  encoder + decoder modules.
- `src/storage/encoding/select.rs` (new) — cascade selection
  heuristic.
- `src/storage/encoding/directory.rs` (new) — directory
  serializer + parser + invariant validation.
- `src/storage/encoding/materialize.rs` (new) — Phase D
  materializer entry point.
- `src/storage/encoding/scratch.rs` (new) — scratch buffer pool.
- `src/storage/encoding/page.rs` (new) — SIMD page helpers.
- `src/storage/columnar.rs` — extend planner with Phase D
  step.
- `src/storage/multirow.rs` — writer side dispatches Encoding
  encoding when `EncodingWriterConfig.enabled`.
- `src/storage/secondary_index.rs` — page-aware
  `fetch_indexed_rows`.
- `src/storage/lsm_worker.rs` — `EncodingWriterConfig`
  plumbing.
- `src/config.rs` — `EncodingWriterConfig`, `L2EncodingFetchConfig`.
- `src/metrics/registry.rs` — metric family.
- `src/audit/events.rs` — audit events.
- `tests/architecture_test.rs` — Phase 11 guards.
- 9 new test files (`tests/encoding_*.rs`).
- `benches/encoding.rs` (new).
- `plans/ops/runbooks/encoding.md` (new).

## Non-goals

- No compute-over-encoded-data path (ADR 058 territory).
- No arbitrary-depth cascade nesting.
- No `RecordBodyCompressionPolicy::EncodingCascade` descriptor variant.
- No nested / list / struct Arrow type encoding in V1.
- No page-level min/max/null stats.
- No per-tenant cascade tuning surface.
- No ML-based cascade selection.
- No cross-row-group dictionary sharing.
- No half-encoded buffers.
- No per-page cascade variation within one buffer.
- No new envelope `format_version`.
- No ADR 022 footer extension.
- No `SlabRecordHeader` change.
- No new `MsgType` containing `Encoding` or `CASCADE`.
- No new `FRAME_FLAG_*`.
- No new `NodeCapability` bit.
- No `ObjectStore` trait change.
- No public Toron-visible PBC opcode addition.
- No reserved cascade slots, compatibility shims, or
  numeric-version Rust type suffixes.
- No project-name prefix on new symbols.
- No high-cardinality metric labels.
- No new Cargo feature or external codec dependency. The
  implementation may name the Arrow IPC stack's existing
  `flatbuffers` crate directly when rewriting RecordBatch metadata.

## Phase 0 — Contract freeze and module layout

Lock the cascade catalog, directory schema, custom_metadata
key, materializer module path, configs, planner extension
shape, metric label enumerations, architectural guards, and
V1 defaults before invasive storage edits begin.

**Substrate shape is immutable post-Phase-0.**

Files:

- `src/schema/units.rs`
- `src/storage/encoding/mod.rs` (new)
- `src/storage/encoding/cascade/mod.rs` (new — dispatch table)
- `src/storage/columnar.rs` (extension intent)
- `src/storage/multirow.rs` (writer extension intent)
- `src/storage/secondary_index.rs` (extension intent)
- `src/storage/lsm_worker.rs` (config plumbing)
- `src/config.rs`
- `src/metrics/registry.rs`
- `tests/fixtures/encoding/` (new)
- `tests/architecture_test.rs`

Work:

1. Audit `src/storage/row_group.rs` to identify the precise
   integration points (writer slab emission, parser Phase A,
   planner four-leg dispatch, secondary-index adapter,
   compaction worker config plumbing).

2. Decide Encoding directory serialization. Reuse the same
   serializer-flavor choice ADR 056 made (Phase 0 audits;
   document in ADR §3 and update before promotion).

3. Freeze newtype-units:

   ```rust
   #[repr(transparent)]
   pub struct BitsPerValue(u8);              // 1..=64
   #[repr(transparent)]
   pub struct BaseValue(i128);                // safer for
                                              //   timestamp deltas
   #[repr(transparent)]
   pub struct RunCount(u32);
   #[repr(transparent)]
   pub struct DictValueCount(u32);
   #[repr(transparent)]
   pub struct Exponent(i16);                  // wider ALP coverage
   #[repr(transparent)]
   pub struct Factor(i64);
   #[repr(transparent)]
   pub struct ExceptionCount(u32);
   #[repr(transparent)]
   pub struct EncodingPageIndex(u32);
   #[repr(transparent)]
   pub struct EncodingPageByteOffset(u32);
   #[repr(transparent)]
   pub struct EncodingCascadeIndex(u16);
   #[repr(transparent)]
   pub struct BufferValueIndex(u32);
   #[repr(transparent)]
   pub struct BufferValueCount(u32);
   #[repr(transparent)]
   pub struct SimdPageValueCount(u32);
   #[repr(transparent)]
   pub struct SampleRowCount(u32);
   #[repr(transparent)]
   pub struct EncodingPageCount(u32);
   #[repr(transparent)]
   pub struct TaskCount(u32);
   ```

   Reuse `RowGroupIndex`, `IpcColumnOrdinal`,
   `IpcBufferIndex`, `RecordRowIndex`,
   `RecordBodyByteOffset`, `RecordBodyByteLen`,
   `BufferByteLen`, `BufferByteRange`,
   `LogicalSchemaFingerprint`, `TieredRecordId` from ADR
   042 / 043 / 044 / 056.

4. Freeze cascade catalog:

   ```rust
   #[repr(u8)]
   pub enum EncodingCascadeKind {
       Plain      = 1,
       Lz4Frame   = 2,
       BitPacked  = 3,
       Rle        = 4,
       Dictionary = 5,
       Delta      = 6,
       Fsst       = 7,
       Alp        = 8,
   }

   pub enum EncodingLeafCascade {
       Plain,
       Lz4Frame,
       BitPacked { bits: BitsPerValue, base: BaseValue },
   }

   pub enum EncodingCascade {
       Plain,
       Lz4Frame,
       BitPacked  { bits: BitsPerValue, base: BaseValue },
       Rle        { run_count: RunCount,
                    inner: Box<EncodingLeafCascade> },
       Dictionary { dict_byte_len: BufferByteLen,
                    dict_value_count: DictValueCount,
                    inner: Box<EncodingLeafCascade> },
       Delta      { base: BaseValue,
                    inner: Box<EncodingLeafCascade> },
       Fsst       { symbol_table_byte_len: BufferByteLen },
       Alp        { exponent: Exponent, factor: Factor,
                    exception_count: ExceptionCount,
                    inner: Box<EncodingLeafCascade> },
   }
   ```

   Architectural guard: `EncodingCascadeKind` discriminants
   1..=8 stable; `EncodingLeafCascade` discriminants 1..=3
   stable; non-leaf variants carry `Box<EncodingLeafCascade>`,
   never `Box<EncodingCascade>`.

5. Freeze directory shapes (compact form — cascade dedup +
   per-buffer page prefix-end offsets):

   ```rust
   pub struct EncodingDirectory {
       pub record_id: TieredRecordId,
       pub row_count: RowCount,
       pub logical_schema_fingerprint: LogicalSchemaFingerprint,
       pub target_simd_page_values: SimdPageValueCount,
       pub cascades: Vec<EncodingCascade>,
       pub buffers: Vec<BufferEncoding>,
   }

   pub struct BufferEncoding {
       pub row_group_index: RowGroupIndex,
       pub ipc_column_ordinal: IpcColumnOrdinal,
       pub ipc_buffer_ordinal: IpcBufferIndex,
       pub cascade_index: EncodingCascadeIndex,
       pub decoded_byte_len: BufferByteLen,
       pub decoded_value_count: BufferValueCount,
       pub encoded_range: BufferByteRange,
       pub page_value_count: SimdPageValueCount,
       pub page_end_offsets: Vec<EncodingPageByteOffset>,
   }

   /// Transient planner-only structure; NOT serialized.
   /// Expanded from `BufferEncoding.page_end_offsets`
   /// during planning.
   pub struct EncodingPageDescriptor {
       pub page_index: EncodingPageIndex,
       pub first_value_index: BufferValueIndex,
       pub value_count: BufferValueCount,
       pub byte_range: BufferByteRange,
   }
   ```

   `encoded_range` and `page_end_offsets` are
   **buffer-relative**, not record-relative.

6. Freeze the **reject taxonomy** (5 codes; distinct
   invariants per code):

   ```rust
   #[repr(u32)]
   pub enum EncodingRejectCode {
       EncodingDirectoryParseError = 1,
       BufferEncodingMismatch      = 2,
       PageRangeOverflow           = 3,
       CascadeUnsupported          = 4,
       DecodedLengthMismatch       = 5,
   }

   pub enum EncodingFallbackReason {
       ReaderDisabled,
       DirectoryMissing,
       UnsupportedArrowLayout,
       TooManyPages,
       TooManyBytes,
       DenseSelection,
       PlainBuffer,
   }
   ```

7. Freeze writer + reader configs:

   ```rust
   pub struct EncodingWriterConfig {
       pub enabled: bool,                                  // false
       pub target_simd_page_values: SimdPageValueCount,    // 1_024
       pub cascade_sample_rows: SampleRowCount,            // 256
       pub min_buffer_bytes_for_encoding: BufferByteLen,        // 4_096
       pub enabled_cascades: BTreeSet<EncodingCascadeKind>,     // all by default
   }

   pub struct L2EncodingFetchConfig {
       pub enabled: bool,                                  // false
       pub max_simd_pages_per_record: EncodingPageCount,       // 64
       pub max_parallel_materialize_tasks: TaskCount,      // 4
   }
   ```

   `EncodingWriterConfig` is **deployment-wide** in V1 — applied
   uniformly across locality replicas to preserve
   cross-replica byte-equality (mirroring ADR 056 §13).

8. Freeze the IPC `Schema.custom_metadata` key: literal
   string `storage.encoding_directory`. Architectural
   guard checks the key.

9. Freeze metric shapes per ADR §13:

   ```text
   storage_l2_encoding_buffer_encodings_total{cascade}
   storage_l2_encoding_materialize_micros{cascade}
   storage_l2_encoding_simd_pages_fetched_total{cascade}
   storage_l2_encoding_compression_ratio{cascade}
   storage_l2_encoding_cascade_select_micros
   storage_l2_encoding_directory_bytes
   storage_l2_encoding_fallback_total{reason}
   storage_l2_encoding_bytes_total{kind}
   storage_l2_encoding_invariant_violations_total{violation_kind}
   ```

   Pinned label enumerations:

   ```text
   cascade        = plain | lz4_frame | bit_packed | rle
                  | dictionary | delta | fsst | alp
   reason         = reader_disabled | directory_missing
                  | unsupported_arrow_layout
                  | too_many_pages | too_many_bytes
                  | dense_selection | plain_buffer
   kind           = page | buffer | avoided
   violation_kind = encoding_directory_parse
                  | buffer_encoding_mismatch
                  | page_range_overflow
                  | cascade_unsupported
                  | decoded_length_mismatch
   ```

10. Freeze fixtures:
    - small `MultiRow` record below ADR 056 paging
      thresholds (no Encoding expected — falls through);
    - paged record with Encoding writer disabled (Encoding not
      engaged — ADR 056 path);
    - paged record with all int64 columns selecting
      `BitPacked`;
    - paged record with one-page low-cardinality strings selecting
      `Dictionary`, and multi-page variable-width strings avoiding
      `Dictionary` by default;
    - paged record with monotonic `__row_hlc__` selecting
      `Delta`;
    - paged record with constant
      `__row_routing_locality__` selecting
      `Rle{run_count:1,...}`;
    - paged record with bounded-range int columns
      selecting `BitPacked` with non-zero base;
    - paged record with high-run-length int columns
      selecting `Rle`;
    - paged record with high-cardinality UTF-8 selecting
      `Fsst`;
    - paged record with random UTF-8 selecting
      `Lz4Frame`;
    - paged record with restricted-exponent floats
      selecting `Alp`;
    - paged record with random floats selecting
      `Lz4Frame`;
    - paged record mixing all V1 cascade kinds across
      different columns;
    - paged record with corrupted Encoding directory (invalid
      cascade parameters, mismatched buffer ordinal,
      truncated bytes);
    - pre-057 record (no `encoding_directory` key);
    - record with cascade variant disabled in
      `enabled_cascades` (writer falls back to the next
      eligible cascade).

11. Architectural test stubs (full guards in Phase 11):
    - exactly one definition each of
      `EncodingDirectory`, `BufferEncoding`,
      `EncodingPageDescriptor`, `EncodingCascade`, `EncodingLeafCascade`,
      `EncodingCascadeKind`;
    - newtype-unit definitions for cascade parameters;
    - cascade composition is one level deep
      (`EncodingCascade::Rle.inner: EncodingLeafCascade`,
      never recursive);
    - exactly one materializer entry point in
      `src/storage/encoding/materialize.rs`;
    - CDC, recovery, rehydration code paths do not
      import Encoding-specific types directly;
    - no `Encoding*V[0-9]` / `Cascade*V[0-9]` Rust type
      names;
    - no `Wendelon`-prefixed Encoding symbols;
    - `Schema.custom_metadata` key literal is
      `storage.encoding_directory`;
    - writer reads from `EncodingWriterConfig` only, not from
      a descriptor field;
    - no `RecordBodyCompressionPolicy::EncodingCascade` variant;
    - no envelope `format_version` bump; no ADR 022
      footer field addition;
    - no synthetic Arrow column for Encoding metadata;
    - no PBC opcode addition or PBC executor call from
      inside `src/storage/encoding/`.

Exit:

- Types and guard intent pinned; fixtures ready;
  cascade catalog committed; module layout settled.

## Phase 1 — Per-cascade encoders and decoders

Files:

- `src/storage/encoding/cascade/mod.rs`
- `src/storage/encoding/cascade/plain.rs`
- `src/storage/encoding/cascade/lz4.rs`
- `src/storage/encoding/cascade/bitpacked.rs`
- `src/storage/encoding/cascade/rle.rs`
- `src/storage/encoding/cascade/dict.rs`
- `src/storage/encoding/cascade/delta.rs`
- `src/storage/encoding/cascade/fsst.rs`
- `src/storage/encoding/cascade/alp.rs`
- `tests/encoding_cascade.rs` (new)

Work:

1. Each cascade module exposes:

   ```rust
   pub fn encode(input: &[u8], dtype: DataType,
                 page_rows: SimdPageValueCount,
                 params: &CascadeParams)
                 -> std::io::Result<EncodedBuffer>;
   pub fn decode(encoded: &[u8], dtype: DataType,
                 params: &CascadeParams,
                 expected_byte_len: BufferByteLen,
                 expected_value_count: BufferValueCount)
                 -> std::io::Result<Vec<u8>>;
   pub fn decode_page(...)
                 -> std::io::Result<Vec<u8>>;
   ```

   `EncodedBuffer` carries encoded bytes + (optional)
   `page_end_offsets` prefix array for pageable cascades.
   `decode` reverses to dense Arrow-conformant bytes for
   the whole buffer. `decode_page` reverses one page only.

2. `Plain`: identity passthrough. No SIMD page emission.

3. `Lz4Frame`: delegate to ADR 043's existing storage LZ4
   frame codec. No new frame flag, no codec-catalog
   extension. No SIMD page emission.

4. `BitPacked`: scans int input, computes
   `bits = ceil_log2(max - min + 1)` and `base = min`.
   Packs N values into ⌈N×bits/8⌉ bytes. SIMD-friendly
   inner loop. SIMD page emission on every
   `target_simd_page_values` block.

5. `Rle`: walks values, emits `(run_value, run_length)`
   pairs; inner cascade encodes the `run_value` stream.
   Decode expands runs. SIMD page emission: pages map
   row-grain ranges, which map to run-grain ranges
   inside the run array.

6. `Dictionary`: hashes distinct values into a dict
   table; emits dict bytes followed by index array
   (encoded by inner cascade, typically BitPacked).
   Decode rebuilds full array via dict lookup.
   Deterministic dictionary order by first occurrence.
   SIMD page emission on the index array.

7. `Delta`: emits `base` + delta-from-prev stream
   (inner cascade encodes deltas). Decode reconstructs
   by prefix-sum. No SIMD page emission in V1 (Delta
   page-grain decode requires per-page base values; a
   future ADR may add this).

8. `Fsst`: builds a Fast Static Symbol Table (256
   symbols max) per buffer; encodes UTF-8 input as
   symbol index stream + literal escape sequences.
   Decode expands symbols. Home-rolled per brainstorm
   011 §"Why Home-Roll"; no external FSST library.
   No SIMD page emission in V1.

9. `Alp`: scans float input, finds dominant exponent +
   factor, casts non-exception values to int64 (encoded
   by inner cascade), emits exception positions + raw-
   float bytes. Decode reverses. No SIMD page emission
   in V1.

10. All encoders enforce parameter ranges:
    - `BitsPerValue` 1..=64;
    - `RunCount > 0`;
    - `DictValueCount > 0`;
    - `Fsst` symbol table ≤ `MAX_FSST_SYMBOL_TABLE_BYTES`
      (4 KiB).

11. All decoders reject malformed encoded bytes via
    `CascadeUnsupported` (parameter mismatch) or
    `DecodedLengthMismatch` (output exceeds
    `expected_byte_len`).

Tests (`tests/encoding_cascade.rs`):

- Round-trip per cascade per dtype: encode → decode →
  byte-identical input.
- Encoder is deterministic given identical input + params.
- Decoder rejects malformed encoded bytes (wrong byte
  length, parameter overflow, dictionary index out of
  range, RLE run overflow, FSST symbol overflow, ALP
  exponent / factor mismatch).
- BitPacked at every bit-width 1..=64.
- BitPacked with non-zero `base` (Frame-of-Reference
  case).
- Dictionary with 1, 16, 256, 65536 distinct values.
- Rle with all-equal input (one run; covers `Constant`
  case) and alternating input (many tiny runs).
- Fsst with ASCII / UTF-8 mixed / non-printable bytes.
- Alp with normal floats / exception-heavy floats /
  all-NaN.
- SIMD-page decode equals slicing the full-buffer decode
  for pageable cascades.

Exit:

- Eight cascade encoder/decoder pairs ready; round-trip
  tests pass; malformed input rejects closed.

## Phase 2 — Cascade selection heuristic

Files:

- `src/storage/encoding/select.rs` (new)
- `src/storage/encoding/sample.rs` (new)
- `tests/encoding_select.rs` (new)

Work:

1. Implement sample collection:

   ```rust
   pub struct ColumnSample {
       pub dtype: DataType,
       pub value_count: usize,
       pub null_count: usize,
       pub distinct_count_estimate: u32,    // HLL-bounded
       pub min: Option<CanonicalScalar>,
       pub max: Option<CanonicalScalar>,
       pub run_count_estimate: u32,
       pub avg_string_length: Option<f32>,
       pub is_monotonic: bool,
       pub fsst_compressible: bool,
       pub alp_candidate: bool,
       pub alp_exponent: Option<Exponent>,
       pub alp_factor: Option<Factor>,
   }

   pub fn sample_buffer(
       array: &dyn Array,
       sample_rows: SampleRowCount,
   ) -> ColumnSample;
   ```

   Sampling is deterministic — same input, same sample
   row indices.

2. Implement selection heuristic per ADR §6:

   ```rust
   pub fn select_cascade(
       sample: &ColumnSample,
       config: &EncodingWriterConfig,
       buffer_bytes: BufferByteLen,
   ) -> EncodingCascade;
   ```

   Each rule branch checks `config.enabled_cascades`;
   falls through to the next eligible cascade if
   disabled.

3. Sub-`min_buffer_bytes_for_encoding` columns return `Plain`
   immediately.

4. Bound heuristic CPU. Architectural test asserts via
   `storage_l2_encoding_cascade_select_micros`.

Tests (`tests/encoding_select.rs`):

- Constant column → `Rle { run_count: 1, ... }`.
- Low-cardinality int → `Dictionary(BitPacked)`.
- Monotonic int (e.g., `__row_hlc__`) → `Delta(BitPacked)`.
- Bounded-range int → `BitPacked` with non-zero base.
- High-run-length int → `Rle(BitPacked)`.
- Random int → `Plain` when `Plain` is enabled; `Lz4Frame`
  when `Plain` is removed from `enabled_cascades`.
- One-page low-cardinality short string →
  `Dictionary(BitPacked)`.
- Multi-page variable-width string → `Fsst` when the sample is
  compressible, otherwise the normal `Plain` / `Lz4Frame`
  fallback. This avoids the multi-page Dictionary writer-cost
  spike measured in Phase 10.
- High-cardinality UTF-8 → `Fsst` or `Lz4Frame` per
  sample.
- Random binary → `Plain` when `Plain` is enabled; `Lz4Frame`
  when `Plain` is removed from `enabled_cascades`.
- Restricted-exponent float → `Alp(BitPacked)`.
- Random float → `Plain` when `Plain` is enabled; `Lz4Frame`
  when `Plain` is removed from `enabled_cascades`.
- Sub-threshold buffer → `Plain`.
- Disabled cascade kind in `enabled_cascades` falls
  through to next eligible.
- Selection is deterministic (identical sample → identical
  cascade).
- Cross-replica determinism: same input bytes, same
  config → same cascade chosen.

Exit:

- Heuristic selects appropriate cascade per data shape;
  determinism verified; cost bounded.

## Phase 3 — Directory serializer and parser

Files:

- `src/storage/encoding/directory.rs` (new)
- `tests/encoding_directory.rs` (new)

Work:

1. Serializer writes magic + format_version + record_id +
   row_count + `logical_schema_fingerprint` +
   `target_simd_page_values` + cascade table +
   buffer-encoding entries (each carrying its own
   `page_end_offsets` prefix array). No serialized
   per-page rows; page descriptors are derived in memory
   at planning time. Each entry length-prefixed.

2. Cascade serialization: discriminant byte + parameter
   bytes. Non-leaf variants append the inner leaf
   cascade's discriminant + parameters.

3. Parser:

   ```rust
   pub fn parse_encoding_directory(
       custom_metadata_value: &[u8],
   ) -> Result<EncodingDirectory, EncodingRejectCode>;
   ```

   Validates magic, format_version, length-prefixed
   entries.

4. `validate_encoding_directory(directory, row_group_directory,
   schema, expected_fingerprint)` enforces:
   - `logical_schema_fingerprint` matches descriptor;
   - every `BufferEncoding` triple matches an entry
     in ADR 056's `BufferPageDirectoryEntry` set;
   - no duplicate `(row_group, column, buffer)` entries;
   - `page_end_offsets` strictly increasing, last entry
     equals `encoded_range.len`, page boundaries cover
     `0..decoded_value_count` for buffers with pages;
   - all per-buffer page boundaries fit inside ADR 056
     buffer range;
   - `Plain`, `Lz4Frame`, `Delta`, `Fsst`, `Alp`
     buffers carry empty `page_end_offsets`;
   - cascade parameter ranges per Phase 1;
   - composition is one level deep (caught by Rust
     deserializer type system).

5. ADR 056 Phase A parser extension:
   - look up
     `Schema.custom_metadata["storage.encoding_directory"]`;
   - if absent: ADR 057 reads via ADR 056 path
     transparently;
   - if present: parse and attach to
     `RecordBufferDirectory.encoding_directory: Option<EncodingDirectory>`.

6. On validation failure: `EncodingDirectoryParseError`,
   `BufferEncodingMismatch`, or `CascadeUnsupported` —
   never silent fallback.

Tests (`tests/encoding_directory.rs`):

- Pre-057 record (no key) parses with
  `encoding_directory: None`, no error.
- Well-formed directory parses cleanly.
- Roundtrip writer → reader → byte-identical
  `EncodingDirectory` for every fixture.
- Corrupted directory (bad magic, truncated entry,
  invalid cascade parameter, recursive cascade
  composition, out-of-range cascade discriminant) fails
  closed with appropriate reject code.
- Encoding entry referencing a buffer not in ADR 056
  directory → `BufferEncodingMismatch`.
- non-empty `page_end_offsets` on non-page cascade
  (`Plain`, `Lz4Frame`, `Delta`, `Fsst`, `Alp`) →
  `EncodingDirectoryParseError`.
- Schema fingerprint mismatch →
  `EncodingDirectoryParseError`.

Exit:

- Directory parser recognizes Encoding-paged vs. legacy
  records; rejects malformed closed.

## Phase 4 — Custom partial-buffer materializer

Files:

- `src/storage/encoding/materialize.rs` (new)
- `src/storage/encoding/scratch.rs` (new)
- `tests/encoding_materialize.rs` (new)

Work:

1. Materializer entry point:

   ```rust
   pub fn materialize_buffer(
       encoded_bytes: &[u8],
       encoding: &BufferEncoding,
       scratch: &mut ScratchBufferPool,
   ) -> Result<MaterializedBuffer, EncodingRejectCode>;
   ```

   Walks cascade outer-to-inner, returns dense Arrow-
   conformant buffer bytes.

2. Page-grain materializer (pageable cascades only):

   ```rust
   pub fn materialize_simd_page(
       page_bytes: &[u8],
       page_index: EncodingPageIndex,
       page_value_count: BufferValueCount,
       encoding: &BufferEncoding,
       scratch: &mut ScratchBufferPool,
   ) -> Result<MaterializedPage, EncodingRejectCode>;
   ```

3. Scratch buffer pool bounded by config; fail-closed on
   `DecodedLengthMismatch` if a buffer would exceed
   `decoded_byte_len`.

4. Per-row-group materialization helper:

   ```rust
   pub fn materialize_row_group(
       row_group_index: RowGroupIndex,
       fetched_buffers: &[(IpcColumnOrdinal, IpcBufferIndex, &[u8])],
       directory: &EncodingDirectory,
       scratch: &mut ScratchBufferPool,
   ) -> Result<MaterializedRowGroup, EncodingRejectCode>;
   ```

5. Hand materialized buffers to Arrow IPC reader. Reader
   sees Arrow-conformant bytes and constructs RecordBatch
   normally.

6. Architectural test: materializer code path does NOT
   call into PBC executor or AGG fold.

Tests (`tests/encoding_materialize.rs`):

- Round-trip per fixture: materialized buffer ==
  original input buffer (Arrow comparison kernels).
- Cascade materialization correctness for every cascade
  kind across every supported dtype.
- One-level cascade composition: `Rle(BitPacked)`,
  `Dictionary(BitPacked)`, `Delta(BitPacked)`,
  `Alp(BitPacked)` decode correctly.
- Page-grain materialization decodes byte-equivalent to
  full-buffer materialization sliced at page boundaries
  (pageable cascades).
- Scratch pool reuse: 1000 sequential materializations
  do not increase total allocated bytes beyond pool cap.
- Overflow protection: corrupted directory claiming
  smaller `decoded_byte_len` than actual decode
  output fails as `DecodedLengthMismatch`.
- No call back into PBC / AGG.
- `Plain` / `Lz4Frame` cascade buffers materialize
  without scratch allocation.

Exit:

- Materializer produces dense Arrow buffers; cascade
  composition decodes correctly; scratch pool bounded.

## Phase 5 — SIMD page directory and point-read fast path

Files:

- `src/storage/encoding/page.rs` (new)
- `src/storage/encoding/cascade/{bitpacked,rle,dict}.rs`
  (extend with page emission)
- `tests/encoding_simd_page.rs` (new)

Work:

1. Page emission during encoding for `BitPacked`, `Rle`,
   `Dictionary`. Each chunks input into
   `target_simd_page_values` blocks; emits per-page encoded
   bytes; appends prefix-end offsets to `page_end_offsets`.

2. Cascades not in the page set (`Plain`, `Lz4Frame`,
   `Delta`, `Fsst`, `Alp`) emit no page offsets — their
   `page_end_offsets` is empty. Architectural guard 28
   enforces.

3. Expand prefix offsets into transient
   `EncodingPageDescriptor` values during planning:

   ```rust
   pub fn expand_pages(
       encoding: &BufferEncoding,
   ) -> Vec<EncodingPageDescriptor>;
   ```

4. Point-read coordinator:

   ```rust
   pub fn map_row_to_page(
       row_within_group: RecordRowIndex,
       encoding: &BufferEncoding,
       pages: &[EncodingPageDescriptor],
   ) -> Option<(EncodingPageIndex, BufferValueIndex)>;
   ```

   Returns `None` when the buffer's cascade has no
   pages (empty `page_end_offsets`).

5. Point-read fetcher integration: when ADR 056 maps
   `record_row_index → (row_group_index, row_within_group)`,
   ADR 057 maps `row_within_group → (page_index,
   row_within_page)` for projected buffers, fetches only
   page bytes, materializes one page.

Tests (`tests/encoding_simd_page.rs`):

- Page boundaries are deterministic.
- Each page decodes independently.
- Last page may be partial.
- Buffers using non-page cascades return `None` from
  `map_row_to_simd_page`.
- Pages cover `0..decoded_value_count` exactly.
- Point read at row 405 in 1024-page-grain buffer fetches
  page 0 only.
- Cross-page point reads fetch both pages.

Exit:

- SIMD page directory enables sub-buffer point reads.

## Phase 6 — Planner Phase D extension

Files:

- `src/storage/columnar.rs`
- `src/storage/encoding/planner.rs` (new)
- `src/storage/manager.rs`
- `tests/encoding_planner.rs` (new)

Work:

1. Extend `RecordBufferDirectory` with optional
   `encoding_directory: Option<EncodingDirectory>`. ADR
   056 directory remains primary; the encoding directory is
   a sibling.

2. Extend `RowGroupFetchPlan` with optional
   `encoding_materialization: Option<EncodingMaterializationStep>`.

3. Extend the planner to add Phase D (per ADR §5).
   Phase D is internal to the `RowGroup(plan)` strategy
   leg — no fifth `L2FetchStrategy` variant.

4. Materialization step structure:

   ```rust
   pub struct EncodingMaterializationStep {
       pub buffer_decodes: Vec<EncodingBufferDecodePlan>,
       pub simd_page_decodes: Vec<EncodingPageDecodePlan>,
   }

   pub struct EncodingBufferDecodePlan {
       pub row_group_index: RowGroupIndex,
       pub ipc_column_ordinal: IpcColumnOrdinal,
       pub ipc_buffer_ordinal: IpcBufferIndex,
       pub byte_range: BufferByteRange,
   }

   pub struct EncodingPageDecodePlan {
       pub row_group_index: RowGroupIndex,
       pub ipc_column_ordinal: IpcColumnOrdinal,
       pub ipc_buffer_ordinal: IpcBufferIndex,
       pub page_index: EncodingPageIndex,
       pub byte_range: BufferByteRange,
   }
   ```

5. Executor integration: after Phase C bytes arrive,
   walk Phase D's `buffer_decodes` and
   `simd_page_decodes`, materialize each via the Encoding
   materializer, hand to Arrow IPC reader.

6. Strategy ladder unchanged from ADR 056:

   ```text
   row-group(+encoding-pages) → row-group(no-encoding, ADR 056) →
   column-buffer (ADR 044) → whole-record
   ```

   Apply config thresholds: page-fraction vs.
   row-group-fraction crossover, range caps,
   `max_simd_pages_per_record`,
   `max_parallel_materialize_tasks`.

Tests (`tests/encoding_planner.rs`):

- Query against Encoding-paged record uses RowGroup(plan)
  with Phase D step attached.
- Query against pre-057 record (ADR 056 only) has no
  Phase D step.
- Point-read with SIMD pages emits page-grain decode
  plan.
- Point-read on non-page cascade falls back to full-
  buffer decode.
- Mixed cascades across columns: Phase D step contains
  per-buffer decode plans matching cascade kinds.
- Dense projection prefers buffer-grain over
  page-grain.
- Too-many-pages fallback to row-group buffer fetch.

Exit:

- Production read path exercises Encoding materialization on
  directory presence.

## Phase 7 — ADR 052 secondary-index integration

Files:

- `src/storage/secondary_index.rs`
- `src/storage/encoding/page.rs`
- `tests/encoding_secondary_index.rs` (new)

Work:

1. Extend `fetch_indexed_rows`:

   - group hits by `(SlabId, RecordLocation)`;
   - for each record, if Encoding directory present AND SIMD
     pages cover all projected buffers AND hit count
     small (≤ `max_simd_pages_per_record`):
     emit page-grain fetch + materialize plan;
   - else: ADR 056 row-group fetch + materialize plan.

2. Pre-057 / non-Encoding records fall through to ADR 056
   row-group point-read path.

3. Hit coalescing: within a record, hits in the same
   page coalesce to one fetch.

Tests (`tests/encoding_secondary_index.rs`):

- Single hit on Encoding-paged record fetches one SIMD page,
  materializes one page, returns one row.
- Multiple hits in same page coalesce.
- Hits across pages within a row group fetch multiple
  pages.
- Hits across row groups fetch multiple row groups.
- Pre-057 record falls back to ADR 056 row-group path.
- Storage-only synthetic columns required for
  visibility materialize with the user projection.
- Hit-coalescing produces byte-identical row output.

Exit:

- Secondary-index point reads use SIMD-page fast path.

## Phase 8 — LSM, CDC, recovery, rehydration

Files:

- `src/storage/lsm_worker.rs`
- `src/storage/manager.rs`
- `src/storage/lsm.rs`
- `src/storage/cdc.rs` (no Encoding imports)
- `src/storage/recovery.rs`
- `src/storage/rehydration.rs`
- `tests/encoding_lsm.rs` (new)
- `tests/encoding_neutral.rs` (new)

Work:

1. Plumb `EncodingWriterConfig` through admission writes and
   LSM compaction writes (ADR 055 §5). Compaction has no
   separate override.

2. Determinism: identical input bytes + identical config
   produce byte-identical Encoding-encoded compaction outputs.

3. Compaction recomputes cascades; never copies input
   cascade choices (architectural guard 18).

4. Pre-056 → post-057 first-pass compaction produces
   fully-paged Encoding-encoded output even from unpaged
   uncompressed input.

5. Visible-state digest invariance (ADR 055 §6) preserved.

6. Per-version-entry digest (ADR 052) preserved.

7. Secondary-index `RecordLocation.record_row_index`
   references unchanged.

8. CDC catchup reads full record bodies, runs Phase D
   materialization through the storage manager, emits
   version entries.

9. Recovery decodes the IPC stream, parses both
   directories, materializes Encoding buffers via Phase D,
   rebuilds redb indexes.

10. Rehydration installs whole slab bytes; subsequent L1
    reads use Phase D automatically.

Tests (`tests/encoding_lsm.rs`):

- Encoding-disabled compaction produces no Encoding directory.
- Encoding-enabled compaction rewrites paged-non-Encoding input
  into paged-Encoding output.
- Compaction recomputes cascades.
- Determinism: two replicas with identical config
  produce byte-identical outputs.
- Visible-state digest unchanged.
- Secondary-index lookup against Encoding-compacted output
  fetches via SIMD-page fast path.

Tests (`tests/encoding_neutral.rs`):

- CDC subscription on Encoding-paged slab emits the same
  events as on non-Encoding slab.
- Recovery from Encoding-paged slab rebuilds redb indexes
  byte-identically.
- Rehydrated L1 reads use Phase D automatically.

Exit:

- ADR 057 composes with LSM; remains byte-neutral to
  CDC, recovery, rehydration.

## Phase 9 — Metrics, audit, runbook, config

Files:

- `src/metrics/registry.rs`
- `src/audit/events.rs`
- `src/config.rs`
- `examples/wendelon.toml.example`
- `plans/ops/runbooks/encoding.md` (new)
- `plans/ops/runbooks/README.md`
- `plans/ops/observability.md`
- `tests/metrics_smoke.rs`

Work:

1. Register `storage_l2_encoding_*` metrics per Phase 0 §9 /
   ADR §13.

2. Add audit events:
   - `EncodingMaterializeSampled { cascade,
     buffer_bytes_in, buffer_bytes_out }` (sampled,
     debug-grade);
   - `EncodingInvariantViolation { kind }` (page-grade).

3. Add `EncodingWriterConfig` and `L2EncodingFetchConfig` to
   `src/config.rs` with V1 defaults (both
   `enabled = false`).

4. Operator runbook covering:
   - reading Encoding cascade distribution per
     `storage_l2_encoding_buffer_encodings_total`;
   - per-cascade compression ratio;
   - SIMD-page fetch hit rate vs. row-group buffer
     fetch;
   - tuning `target_simd_page_values`,
     `min_buffer_bytes_for_encoding`,
     `cascade_sample_rows`;
   - disabling a cascade kind via `enabled_cascades`
     in response to a decode bug;
   - recognizing `EncodingInvariantViolation` as fail-
     closed corruption (not fallback);
   - cross-replica byte-equality reconciliation;
   - rolling out Encoding writer config across the cluster
     in waves;
   - turning the writer config off as a cluster-wide
     Encoding disable.

5. Index runbook in `plans/ops/runbooks/README.md`.

Tests:

- Metric labels exactly match the bounded sets.
- Config validation rejects zero `target_simd_page_values`,
  zero `cascade_sample_rows`, empty `enabled_cascades`
  set.
- Audit event names stable.
- Runbook is linked from runbooks index.

Exit:

- Operators can see Encoding engagement, per-cascade
  compression, fetch hit rate, and corruption signals.

## Phase 10 — Bench

Files:

- `benches/encoding.rs` (new)
- `Cargo.toml`
- `plans/wip/057_per_buffer_pages_bench_results.md`
  during iteration; final lands at
  `plans/impl/057_per_buffer_pages_bench_results.md`
  post-promotion

Bench scenarios:

1. **Per-cascade encode CPU** (8 cascades × 4 dtypes ×
   3 fixture shapes).
2. **Per-cascade full-buffer decode CPU**.
3. **SIMD-page decode CPU** (single 1024-value page,
   pageable cascades).
4. **Per-cascade compression ratio** vs. ADR 043 LZ4-
   only baseline. Targets:
   - int64: ≥ 2× LZ4
   - low-cardinality UTF-8 (Dictionary): ≥ 3× LZ4
   - timestamp (monotonic, Delta): ≥ 4× LZ4
   - high-cardinality UTF-8 (Fsst): ≥ 1.3× LZ4
   - float64 (Alp): ≥ 1.5× LZ4
5. **Cascade selection CPU** at 64 / 256 / 1024 sample
   rows.
6. **Writer total CPU**: ADR 056-only baseline vs. ADR
   057 enabled. Acceptance target **≤ 75%** increase on
   the largest legal representative fixture as starting
   target, **defended by mandatory compression + read-side
   wins** in the same bench artifact. ADR 056's ≤ 15%
   gate set without empirical data missed by 28× on
   threshold-margin fixtures; setting ≤ 25% for Encoding risks
   repeating the mistake. If Phase 10 evidence shows a
   tighter bar is achievable, the gate tightens
   (Provisional discipline).
7. **Reader total CPU**: Phase A+B+C (ADR 056) vs.
   Phase A+B+C+D (ADR 057). Materialization should be
   sub-millisecond per row group on all V1 cascades.
8. **Point-read latency**: secondary-index lookup
   returning 1 row from 65,536-row record. Compare ADR
   056 row-group decode vs. ADR 057 SIMD-page decode.
   Expected ≥ 4× reduction in decoded bytes; 2–4×
   reduction in latency.
9. **Selective scan latency**: 1% / 5% / 25% / 50%
   selectivity. Phase D adds < 10% to ADR 056 reader
   cost on dense projections.
10. **Directory size** as fraction of record body.
11. **64 KiB metadata probe pressure**. Verify combined
    row-group + Encoding directory fits inside initial
    probe.
12. **Cross-replica determinism**: byte-identical
    compaction outputs under matching configs.
13. **Pre-057 / post-057 mixed-slab read** throughput.
14. **First-pass compaction byte cost**: ADR 056-only
    → ADR 057-Encoding compaction output size delta.
    Expected 30–60% reduction on
    int / low-cardinality / timestamp columns.
15. **`enabled_cascades` ablation**: writer with each
    cascade disabled in turn, measure ratio + decode
    cost.
16. **`target_simd_page_values` tuning**:
    512 / 1024 / 2048.
17. **Cascade selection edge cases**: pathological
    inputs (all-NaN floats, near-monotonic ints,
    bimodal distributions, null-heavy strings).
18. **L2 GET-rate cost**: page ranges before and after
    coalescing, 1000 hits across 100 records.
19. **CSB short-circuit interaction**: confirm Encoding
    planner is bypassed when CSB skips the slab.

Starting acceptance targets:

- Compression ratio improvements per Phase 10 scenario
  4 above.
- High-entropy columns choose `Plain` / `Lz4Frame` and
  regress by ≤ 5% vs. ADR 043 LZ4-only baseline after
  directory metadata.
- Writer CPU regression ≤ 75% vs. ADR 056-only on
  representative legal fixture (starting target,
  defended by mandatory compression + read-side wins;
  revisable per Provisional discipline).
- Reader CPU regression ≤ 15% on full-row-group decode;
  ≥ 0% improvement on SIMD-page point reads.
- SIMD-page point reads reduce decoded value count ≥ 4×
  vs. ADR 056 row-group decode for page-capable
  cascades, and reduce selected L2 bytes ≥ 10× when the
  selected cascade materially compresses.
- Combined row-group + Encoding directory ≤ 2% of record
  body on the largest legal representative fixture.
- Combined metadata fits inside ADR 044's 64 KiB initial
  probe on representative legal fixtures and 256 KiB cap
  on all legal fixtures.
- Cross-replica determinism: byte-identical compaction
  outputs.
- PBC / AGG result equality against pre-Encoding
  materialized batches across all fixtures.
- No regression when `EncodingWriterConfig.enabled = false`.

Bench results: small / medium / large matrix per ADR
030. Smoke artifact lands first; full matrix is the
promote-to-shipped gate.

Exit:

- Bench harness captures per-cascade encode/decode
  cost, compression ratio, point-read latency, writer
  cost delta, materialization cost, determinism.

## Phase 11 — Architectural lockdown

Files:

- `tests/architecture_test.rs`

Guards (per ADR §15; ~30 guards):

1. Exactly one definition each of
   `EncodingDirectory`, `BufferEncoding`,
   `EncodingPageDescriptor`, `EncodingCascade`, `EncodingLeafCascade`,
   `EncodingCascadeKind`.
2. Newtype-units for cascade parameters, page indices,
   value counts, sample sizes, task caps (16 newtypes
   per Phase 0 §3).
3. `EncodingCascade` non-leaf variants (Rle, Dictionary,
   Delta, Alp) carry `inner: Box<EncodingLeafCascade>`,
   never `Box<EncodingCascade>`.
4. Exactly one materializer entry point in
   `src/storage/encoding/materialize.rs`.
5. CDC, recovery, rehydration code paths do not
   import Encoding-specific types directly.
6. `EncodingRejectCode` taxonomy closed to **five** V1 codes;
   discriminants 1..=5 stable.
7. Authentication / parse / decode failures cannot
   reach any silent fallback branch.
8. `storage_l2_encoding_*` metrics carry exactly the
   bounded labels.
9. No new `MsgType` containing `Encoding` or `CASCADE`.
10. No new `FRAME_FLAG_*`.
11. No new `NodeCapability` variant.
12. No `ObjectStore` trait method addition.
13. No table descriptor field for Encoding policy. No
    `RecordBodyCompressionPolicy::EncodingCascade` variant.
14. No synthetic Arrow column for Encoding metadata.
15. No envelope `format_version` bump; no ADR 022
    footer field addition.
16. `Schema.custom_metadata` key literal is
    `storage.encoding_directory`.
17. Writer reads from `EncodingWriterConfig` only.
18. Compaction writer recomputes cascades; never
    copies input cascade choices.
19. No `Wendelon`-prefixed Encoding identifiers.
20. No `Encoding*V[0-9]` / `Cascade*V[0-9]` Rust type
    suffixes.
21. CDC, recovery, rehydration code paths do not
    import Encoding-specific types directly.
22. No PBC opcode addition or PBC executor call from
    inside `src/storage/encoding/`.
23. `EncodingCascade` discriminants 1..=8 stable.
24. `EncodingLeafCascade` discriminants 1..=3 stable.
25. Cascade selection heuristic is not exposed as a
    plug-in / trait.
26. `BufferEncoding.encoded_range` and
    `page_end_offsets` are buffer-relative, never
    record-relative.
27. `page_end_offsets` (when non-empty) cover
    `0..decoded_value_count` exactly for buffers with
    pages; empty entirely for buffers without pages.
28. `Plain`, `Lz4Frame`, `Delta`, `Fsst`, `Alp`
    cascade buffers carry empty `page_end_offsets`.
29. No inline FSST symbol-table sharing across
    separate buffer entries.
30. Materializer code path does not call back into PBC
    executor or AGG fold.
31. Directory's `logical_schema_fingerprint` validates
    against the descriptor's current logical
    fingerprint (ADR 051) at parse time.
32. Serialized `EncodingDirectory` does not carry
    one row per page; `EncodingPageDescriptor` is a
    transient planner-only type derived from
    `page_end_offsets`.
33. Cascade dedup discipline:
    `EncodingDirectory.cascades` stored once;
    `BufferEncoding.cascade_index` references by
    index; cascade is never inlined per buffer entry.

Exit:

- Future Encoding edits cannot drift the substrate silently.

## Verification gate

```sh
cargo test --lib storage::encoding
cargo test --test encoding_cascade --quiet
cargo test --test encoding_select --quiet
cargo test --test encoding_directory --quiet
cargo test --test encoding_materialize --quiet
cargo test --test encoding_simd_page --quiet
cargo test --test encoding_planner --quiet
cargo test --test encoding_secondary_index --quiet
cargo test --test encoding_lsm --quiet
cargo test --test encoding_neutral --quiet
cargo test --test architecture_test --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench encoding --no-run
cargo bench --bench encoding -- --quick
cargo check --quiet
cargo check --benches --quiet
git diff --check
cargo fmt --check
```

Promotion-to-shipped gate (after Phase 1–11 land):

- All Phase 1–11 golden vectors pass.
- Architectural lockdown tests pass.
- Bench small / medium results pass starting acceptance
  targets (or council explicitly revises them from
  evidence per Provisional discipline).
- ADR 056 Phase 10 bench scenarios still pass with
  `EncodingWriterConfig.enabled = false` (regression check).
- ADR 044 column-fetch scenarios pass end-to-end with
  mixed-shape slab corpora.
- ADR 052 secondary-index point-reads served via
  SIMD-page fast path with byte-identical results.
- ADR 055 LSM compaction outputs deterministic across
  replicas under matching configs; visible-state and
  per-version-entry digests invariant.
- CDC, recovery, rehydration tests confirm
  byte-neutrality.
- Operator runbook `plans/ops/runbooks/encoding.md`
  finalized.

## File-by-file expected edits

(Phase 0 audits actual paths.)

- `src/schema/units.rs` — extend (~150 LOC for 16
  newtype-units).
- `src/storage/encoding/mod.rs` — new (~350 LOC).
- `src/storage/encoding/cascade/mod.rs` — new (~120 LOC:
  dispatch table).
- `src/storage/encoding/cascade/plain.rs` — new (~40 LOC).
- `src/storage/encoding/cascade/lz4.rs` — new (~100 LOC).
- `src/storage/encoding/cascade/bitpacked.rs` — new (~600
  LOC: encoder, decoder, page emission, FOR base
  parameter).
- `src/storage/encoding/cascade/rle.rs` — new (~400 LOC).
- `src/storage/encoding/cascade/dict.rs` — new (~500 LOC).
- `src/storage/encoding/cascade/delta.rs` — new (~250 LOC).
- `src/storage/encoding/cascade/fsst.rs` — new (~700 LOC:
  symbol table build, encoder, decoder).
- `src/storage/encoding/cascade/alp.rs` — new (~500 LOC:
  exponent search, exception handling).
- `src/storage/encoding/select.rs` — new (~300 LOC).
- `src/storage/encoding/sample.rs` — new (~200 LOC).
- `src/storage/encoding/directory.rs` — new (~400 LOC).
- `src/storage/encoding/materialize.rs` — new (~350 LOC).
- `src/storage/encoding/scratch.rs` — new (~150 LOC).
- `src/storage/encoding/page.rs` — new (~250 LOC).
- `src/storage/encoding/planner.rs` — new (~250 LOC).
- `src/storage/columnar.rs` — extend (~150 LOC for
  Phase D wiring).
- `src/storage/multirow.rs` — extend (~120 LOC for Encoding
  dispatch on writer config).
- `src/storage/secondary_index.rs` — extend (~100 LOC).
- `src/storage/lsm_worker.rs` — touched (~30 LOC for
  config plumbing).
- `src/storage/manager.rs` — touched (~80 LOC).
- `src/storage/cdc.rs` — guard test only.
- `src/storage/recovery.rs` — touched (~30 LOC).
- `src/storage/rehydration.rs` — guard test only.
- `src/config.rs` — extend (~60 LOC for both configs).
- `src/metrics/registry.rs` — extend (~120 LOC).
- `src/audit/events.rs` — extend (~30 LOC).
- `tests/architecture_test.rs` — extend (~280 LOC,
  ~30 guards).
- 9 new test files (~250–500 LOC each).
- `benches/encoding.rs` — new (~700 LOC).
- `plans/ops/runbooks/encoding.md` — new (~200 lines).

Total ≈ 7500 LOC. Mostly net-new in `src/storage/encoding/`;
existing surfaces touched lightly to wire Phase D, point-
read fast path, LSM config plumbing.

## V1 design summary

ADR 057 ships the Encoding substrate:

- **Node-local `EncodingWriterConfig`** as activation
  surface; deployment-wide for cross-replica byte-
  equality.
- **Closed cascade catalog**: `Plain`, `Lz4Frame`,
  `BitPacked`, `Rle`, `Dictionary`, `Delta`, `Fsst`,
  `Alp`. Composition one level deep via
  `EncodingLeafCascade`.
- **`EncodingDirectory` in IPC schema custom
  metadata** under key `storage.encoding_directory`
  — sibling to ADR 056's `storage.row_group_directory`.
- **Compact directory shape**: cascade dedup via
  `EncodingCascadeIndex`, per-buffer `page_end_offsets` prefix
  array. No per-page serialized rows.
- **`EncodingPageDescriptor`** transient planner-only type
  expanded from prefix offsets at planning time.
- **Per-row-group-buffer cascade choice** via closed
  rule-based heuristic over sampled column stats.
- **Page directory** for `BitPacked`, `Rle`, `Dictionary`
  cascades enables sub-buffer point reads.
- **Phase D materialization** layered on ADR 056's
  three-phase plan; produces dense Arrow buffers
  consumed by stock Arrow IPC reader.
- **Custom partial-buffer materializer** lives in
  `src/storage/encoding/materialize.rs`.
- **Decode-only V1**. PBC continues to evaluate against
  materialized dense batches. Compute-over-encoded-data
  is ADR 058 territory.
- **Activation via node-local config**, not descriptor
  policy. No `RecordBodyCompressionPolicy::EncodingCascade` variant.
- **ADR 056 substrate reused** unchanged.
- **ADR 052 secondary-index point reads accelerated**
  via SIMD-page fast path.
- **ADR 055 LSM compaction integration**: compaction
  outputs match deployment-wide `EncodingWriterConfig`;
  deterministic across replicas; visible-state digest
  invariant.
- **CDC, recovery, rehydration byte-neutral**.
- **Authentication / parse failures route to storage-
  error path**, never silent fallback.
- **Architectural lockdown** ~30 guards.
- **Compute-over-encoded-data deferred to ADR 058 /
  brainstorm 011.**

## Deferred to follow-up ADRs

- Compute-over-encoded-data (ADR 058).
- Nested / list / struct Arrow type encodings.
- Page-level min/max/null stats (ADR 039 follow-up).
- Cross-row-group dictionary sharing.
- Arbitrary-depth cascade composition.
- Per-row partial materialization within SIMD pages.
- `EncodingDirectory` cache across queries.
- Bulk slab-level Encoding-directory pre-fetch.
- Cascade-aware AGG short-circuit.
- Encoding on wire-tier transport (ADR 053 extension).
- Per-cascade DEK rotation.
- Public Toron-visible cascade-hint opcode.
- Adaptive cascade re-selection.
- SIMD-block-grain page directory (64-value pages).
- Page emission for Delta / Fsst / Alp.
- ML-based cascade selection.
