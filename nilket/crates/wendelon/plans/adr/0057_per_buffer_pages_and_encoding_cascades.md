# ADR 057: Per-Buffer Pages and Encoding Cascades

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/0057_per_buffer_pages_and_encoding_cascades.md`

## Status

Council-converged and promoted after implementation plus Phase
10 bench evidence. Inner layer of the
storage hierarchy initiated in ADR 056 (row groups). Opens
on top of ADR 056's accepted Phase 10 bench artifact, which
established the empirical writer-cost gate ADR 057 inherits
the discipline of (Provisional gate; revise on evidence).

The council process: five iterations between Claude, Codex,
and Gemini. The load-bearing fork was activation surface —
descriptor policy (`RecordBodyCompressionPolicy::EncodingCascade`) vs.
node-local writer config — which oscillated through iters
1–3 before all three drafts landed on node-local at iter 4.
Once that fork resolved, the rest fell into place: compact
directory shape with cascade dedup and `page_end_offsets`
prefix array, 8-cascade closed catalog with one-level
composition, 5-code reject taxonomy, ≤ 75% writer-cost gate
as starting target with mandatory compression + read-side
wins. Iter 5 polished the synthesis for promotion. The
arguments traded across iterations are preserved in council
git history; the convergent answer matches Parquet / Vortex
at the inner layer (encoded buffers + page directory +
custom materializer) just as ADR 056 matched it at the outer
layer (row-group RecordBatches + per-row-group stats).

## Where we are

ADR 060 sparse projection fetch narrowing is now
implemented and promoted to shipped. Its Phase 7
bench artifact at
`plans/impl/0060_sparse_projection_fetch_narrowing_bench_results.md`
confirms ADR 057's per-buffer page metadata is
sufficient for mask-driven projection fetch:
`BufferEncoding.page_value_count` provides
deterministic row-to-page mapping, selected
BitPacked / Dictionary pages materialize byte-
identically to the dense projection oracle, and no
ADR 057 directory shape change is required.

RLE remains correctness-eligible by page metadata,
but ADR 060 V1 routes RLE projection sparse fetch
default-off after Phase 7 evidence. RLE value-run
format work remains a separate ADR. ADR 060 also
records ciphertext-byte narrowing as bounded by ADR
044 chunk granularity; page-aligned authenticated
chunk layout is follow-up territory, not an ADR 057
directory extension.

ADR 063 page-level predicate stats is now implemented and
promoted as an additive ADR 057 directory amendment. The
encoding directory format version is bumped and
`BufferEncoding` gains `page_stats: Vec<EncodingPageStats>`,
with length `0` or `page_end_offsets.len()`. V1 emits page
stats only for fixed-width predicate value buffers whose
cascade is in the intersection of ADR 059's default predicate
allow-list and `EncodingCascade::emits_pages_in_v1()`:
`BitPacked`, `DictionaryPlain`, and `DictionaryBitPacked`.
`Delta` remains allow-listed but not page-emitting in V1, RLE
byte-run remains page-emitting but not allow-listed, and both
carry empty `page_stats`.

ADR 067 row-canonical FSST is now implemented and promoted.
The cascade catalog extends by one to nine variants: `Plain`,
`Lz4Frame`, `BitPacked`, `Rle`, `Dictionary`, `Delta`, `FsstWholeBuffer`,
`Alp`, `FsstRowSafe`. The `FsstRowSafe` cascade introduces the
`RSFS` on-disk frame (magic `b"RSFS"`, `block_offsets` pair table
keyed every 256 rows, per-row `u8` `encoded_lengths` with `255`
sentinel, `exception_lengths` table, encoded payload) and
emits buffer-grain (no page emission in V1). The `FsstWholeBuffer` cascade
discriminant is unchanged at 7, but the on-disk magic byte prefix was
renamed to `b"WBFS"` as part of the naming-prerequisite commit landed
alongside ADR 068. The pre-rename magic is not accepted by new readers
per `plans/conventions/no-premature-backward-compat.md` — Wendelon is
pre-launch, no deployed state, so the magic cut is a clean break, not
a back-compat seam. Whole-buffer FSST buffers written before the rename
must be re-encoded under the new magic. The encoding directory format
version bumped by one for the `FsstRowSafe` variant addition.
Default enablement is on (`EncodingCascadeKind::DEFAULT_ENABLED`
includes `FsstRowSafe`) per Phase 7 evidence in
`plans/impl/0067_row_canonical_fsst_bench_results.md`; council
waived the ≤ 75% writer-cost gate explicitly given the
transformative read-side wins (3.0–11.7× predicate, 1.8–5.4×
end-to-end) and the offline nature of writer cost on warehouse
workloads. Roll-out discipline is reader-before-writer.

ADR 068 sample-based `FsstRowSafe` writer estimator is now
implemented and promoted. It mitigates ADR 067's ~140%
writer-cost overhead by early-declining buffers unlikely to
pass the per-buffer exact size guard. A writer column-cache
amortizes symbol-training cost across row groups during an
encoding pass. The estimator achieves ≥ 1.5× writer speedup
on declined short-string buffers with ≤ 5% overhead on
plausible winners. It completely eliminates the worst-case
dual-encode cliffs without risking false declines. The remaining
partial writer-cost delta will be addressed in a V2 follow-up
(estimator-authorized emission without the exact guard).

V1 substrate commitments (council-converged):

- substrate-only scope: per-row-group-buffer encoding
  cascades + per-buffer page-end-offsets directory + custom
  partial-buffer materializer;
- **decode-only.** PBC continues to evaluate against dense
  Arrow batches; compute-over-encoded-data is ADR 058
  territory and explicitly out of scope here;
- **closed 8-variant cascade catalog**: `Plain`,
  `Lz4Frame`, `BitPacked`, `Rle`, `Dictionary`, `Delta`,
  `FsstWholeBuffer`, `Alp`. Cascade composition is one level deep via
  `Box<EncodingLeafCascade>` (`EncodingLeafCascade ∈ {Plain,
  Lz4Frame, BitPacked}`); arbitrary-depth nesting rejected
  in V1;
- **node-local `EncodingWriterConfig`** as activation surface;
  deployment-wide for cross-replica byte-equality; reader
  picks up directory presence automatically; **no
  `RecordBodyCompressionPolicy::EncodingCascade`** descriptor variant;
- **`EncodingDirectory`** carried in IPC schema custom
  metadata under literal key
  `storage.encoding_directory` — sibling to ADR 056's
  `storage.row_group_directory`, encrypted with the record
  body, authenticated by ADR 020 chunk AEAD;
- **compact directory shape**: cascades deduped once in
  `cascades: Vec<EncodingCascade>`; one `BufferEncoding` per
  `(row_group_index, ipc_column_ordinal,
  ipc_buffer_ordinal)` triple, referencing a cascade by
  `EncodingCascadeIndex`; page-grain bytes captured as
  `page_end_offsets: Vec<EncodingPageByteOffset>` prefix array
  per buffer — not as one serialized row per page;
- **`EncodingPageDescriptor` is transient planner-only** —
  in-memory expansion of `page_end_offsets`; never
  serialized;
- **page emission** required for `BitPacked`, `Rle`,
  `Dictionary` cascades; `Plain`, `Lz4Frame`, `Delta`,
  `FsstWholeBuffer`, `Alp` carry empty `page_end_offsets` and decode
  at full-buffer grain in V1;
- **buffer-relative byte offsets** in `encoded_range` and
  `page_end_offsets` — ADR 056 fetch planner locates the
  buffer's absolute position; Encoding materializer slices
  pages using relative offsets. Compaction or rewrites can
  move the buffer's absolute position without rewriting
  Encoding page metadata as long as the buffer's internal
  encoded layout is unchanged;
- **planner Phase D extension** to ADR 056's three-phase
  plan: Phase A (metadata) → Phase B (stats prune) →
  Phase C (projected fetch) → **Phase D (Encoding
  materialization)**. Internal to the
  `L2FetchStrategy::RowGroup(plan)` leg; no fifth
  strategy variant;
- **closed 5-code `EncodingRejectCode` taxonomy**:
  `EncodingDirectoryParseError`, `BufferEncodingMismatch`,
  `PageRangeOverflow`, `CascadeUnsupported`,
  `DecodedLengthMismatch`;
- **closed 7-variant `EncodingFallbackReason` enum**:
  `ReaderDisabled`, `DirectoryMissing`,
  `UnsupportedArrowLayout`, `TooManyPages`, `TooManyBytes`,
  `DenseSelection`, `PlainBuffer`;
- bounded `storage_l2_encoding_*` metric family with
  `{cascade, decision, reason, kind, violation_kind}`
  enumerations;
- **writer-cost gate ≤ 75%** as starting target on the
  largest legal representative fixture, defended by
  mandatory compression + read-side wins in the same
  bench artifact; may tighten on Phase 10 evidence per
  Provisional discipline (the same lesson ADR 056's
  ≤ 15% miss codified);
- pinned V1 defaults: `target_simd_page_values = 1_024`,
  `cascade_sample_rows = 256`,
  `min_buffer_bytes_for_encoding = 4_096`,
  `max_simd_pages_per_record = 64`,
  `max_parallel_materialize_tasks = 4`;
- no new wire `MsgType`, no new `FRAME_FLAG_*`, no new
  `NodeCapability` bit, no Cargo feature, no new digest
  domain, no `ObjectStore` trait change, no envelope
  `format_version` bump, no ADR 022 footer extension, no
  descriptor surface change, no public Toron-visible PBC
  opcode addition.

## Context

ADR 056 landed the outer layer of the storage hierarchy:
row-group RecordBatches + per-row-group min/max/null-count
stats + `RecordPageDirectory` for buffer-grain addressing
within a record body. The bench artifact shows the read-side
wins are real — sub-1% selected bytes for sparse projections,
≤ 2% compression loss from row-group boundaries, directory
size < 0.4% of record body. What the row-group layer does NOT
yet deliver:

1. **Density.** ADR 043 per-buffer LZ4_FRAME compresses ~2×
   on the int64 fixtures Codex's harness measured. Modern
   columnar encodings (FastLanes bit-packing, ALP, FSST,
   dictionary+bitpacked) routinely deliver 4–10× on the same
   data shapes — the bandwidth and resident-memory headroom
   brainstorm 011 §"Premise" called out is still on the
   table.

2. **Point-read decode cost.** Within a fetched row group, a
   point read still pays full LZ4 frame decompression on
   every projected buffer. The Lance "decompress only the
   page containing the target row" idea reduces this from
   `O(row_group_bytes)` to `O(simd_page_bytes)` — a
   1024-value page is typically <8 KiB on bit-packed
   integers, ~1000× smaller than a 4096-row LZ4-framed
   buffer.

3. **Cascade-aware predicate evaluation (forward
   dependency).** ADR 058 (compute-over-encoded-data)
   requires that encoded buffers carry self-describing
   cascade metadata so PBC opcodes can dispatch directly
   against `RLE(BitPacked(...))` bytes. ADR 057 ships the
   substrate; ADR 058 lifts compute to the encoded grain.

The Toron-pivot rationale (roadmap §"Toron-pivot stretch")
applies straightforwardly: density × point-read latency ×
predicate-on-encoded compose multiplicatively for analytical
scans. ADR 056 settled the row axis; ADR 057 settles the
intra-buffer axis; ADR 058 settles the compute axis.

Constraints carry forward unchanged: ADR 003 (bifurcated
writes), ADR 016 (control ring), ADR 020 (AEAD), ADR 022
(footer), ADR 029 (Identity vs Locality), ADR 030 (deployment
profiles), ADR 035 / 036 (PBC contract), ADR 037
(descriptors), ADR 039 (CSB), ADR 042 (multi-row), ADR 043
(per-buffer LZ4), ADR 044 (column fetch), ADR 052 (secondary
indexes), ADR 053 (wire compression), ADR 054 (CDC
substrate), ADR 055 (LSM substrate), ADR 056 (row groups +
page directory).

## Architectural inextricability

Three design choices are physically inseparable here:

**1. Encoding cascade + page directory + materializer = one
substrate.** A cascade catalog without a directory leaves the
reader unable to dispatch decoders. A directory without a
materializer leaves cascade metadata unused. A materializer
without cascade-rich entries collapses to ADR 043's LZ4-only
path. The three ship together or not at all.

**2. Encoding directory lives in IPC custom_metadata, sibling to
ADR 056's row-group directory.** Three alternatives are
physically rejected together:

- *Footer extension*: same workload-shape leakage rejection
  ADR 056 §"Architectural inextricability" cites; Encoding adds
  per-column cascade choice on top.
- *Synthetic Arrow column*: directory has one logical value
  per `(row_group, column, buffer)` triple, not per row.
- *Bake into ADR 056's `BufferPageDirectoryEntry`*: violates
  ADR 056's V1-scope guards 25–26. Sibling key keeps
  substrates cleanly separable.

**3. The materializer is custom partial-buffer code, not
stock Arrow IPC reader.** Brainstorm 011 §"How it Fits into
Wendelon" already named this: "The slab reader decompresses
the Encoding payload back into Arrow IPC just-in-time for the
PbcExecutor." Stock Arrow IPC reader cannot decode Encoding-
encoded buffer bytes; the encoded layout is not Arrow-
conformant. The materializer walks the cascade outermost-to-
innermost, produces dense Arrow buffers in scratch memory,
and hands them to the IPC reader as if they were stock
buffers. ADR 056 §"Why row groups as RecordBatches"
rejected this approach for V1 because the council had not
yet committed to the layered hierarchy; with the hierarchy
committed (per the substrate-sequencing decision recorded
2026-05-05), the materializer is the only correct shape for
the inner layer.

## Identity-vs-Locality framing

The encoding substrate is **identity-axis**:

- The decode unit is `(SlabId, RecordLocation, RowGroupIndex,
  IpcColumnOrdinal, IpcBufferIndex, EncodingPageIndex)` — all
  identity-bound coordinates.
- The directory addresses byte ranges within an encoded
  column buffer that is itself addressed by ADR 056's
  `BufferPageDirectoryEntry` (locality-axis: where in the
  record body) and ADR 022's plaintext byte offset
  (locality-axis: where in the slab).
- `BufferEncoding.encoded_range` and
  `page_end_offsets` are **buffer-relative**, not
  record-relative. The ADR 056
  fetch planner locates the buffer's absolute position; the
  Encoding materializer slices pages from the buffer using
  relative offsets. This decoupling means Encoding metadata is
  invariant under any compaction or rewrite that shifts the
  buffer's absolute position but preserves its internal
  encoded layout — Gemini's articulation captures this
  cleanly.
- Unified Ring rule applies. There is no separate "decode
  plane" — the materializer runs in the same storage-read
  surface as ADR 056's Phase C decoder.
- Readers (PBC executor, AGG, secondary-index lookup paths)
  consume dense `ProjectedRecordBatch` values regardless of
  whether the buffers behind them came from Encoding
  materialization or stock Arrow IPC decode. Identity-axis
  output is invariant; locality-axis fetch+decode shape is
  internal optimization.
- Cross-locality compaction does not apply — per-record
  substrate, downstream of admission, upstream of ADR 055's
  per-locality compactor (which inherits the deployment-wide
  `EncodingWriterConfig` and recomputes encoding cascades on
  output, never copying input cascade choices).

## V1 scope

ADR 057 ships **only** the Encoding substrate:

1. Closed cascade catalog: `Plain`, `Lz4Frame`, `BitPacked`,
   `Rle`, `Dictionary`, `Delta`, `FsstWholeBuffer`, `Alp`. Cascade
   composition one level deep.
2. `EncodingDirectory` carried in IPC
   `Schema.custom_metadata["storage.encoding_directory"]`
   with deduped cascade table + one `BufferEncoding`
   per `(row_group, column, buffer)` triple +
   per-buffer `page_end_offsets` prefix array (no
   serialized per-page rows).
3. Writer-side cascade-selection heuristic over sampled
   column stats (cardinality, value range, run lengths,
   monotonicity).
4. ADR 056 Phase A parser extension that reads the Encoding
   directory.
5. **Phase D (new): per-buffer cascade materialization.**
   Walks cascade outer-to-inner, produces dense Arrow
   buffers, hands to stock Arrow IPC reader.
6. SIMD page directory inside encoded buffers for
   `BitPacked`, `Rle`, `Dictionary` cascades (page-grain
   point-read fast path).
7. ADR 052 secondary-index point-read acceleration via
   SIMD-page fetch (`fetch_indexed_rows` extension).
8. Bounded `storage_l2_encoding_*` metric family.
9. Architectural lockdown.

It deliberately does **not** ship:

- compute-over-encoded-data PBC opcodes (ADR 058
  territory);
- arbitrary-depth cascade nesting (one level only);
- nested / list / struct Arrow type encodings (follow-up
  ADR);
- page-level min/max/null stats (ADR 039 follow-up
  territory);
- per-tenant cascade tuning surface;
- ML-based cascade selection;
- new public Toron-visible PBC opcodes;
- variable cascade choice across pages within one buffer
  (cascade is per row-group buffer; pages within one buffer
  all use the same cascade);
- a `EncodingDirectory` cache across queries;
- bulk slab-level pre-fetch of Encoding directories;
- partial-cascade fallback (a buffer is either fully
  cascade-encoded or marked `Plain` / `Lz4Frame`; no
  half-encoded buffers);
- cross-record / cross-row-group dictionary sharing (each
  Dictionary cascade carries its own dictionary inline);
- Encoding on the wire-tier transport (ADR 053 unchanged);
- per-row-group DEK rotation interaction with Encoding;
- compaction-time elision of Encoding cascade choices
  (compaction recomputes; never copies input cascades);
- SIMD-page emission for `Plain`, `Lz4Frame`, `Delta`,
  `FsstWholeBuffer`, `Alp` cascades (those decode at full-buffer grain
  in V1; future ADR may add page-grain support if bench
  data justifies).

## Non-goals (V1)

- **No descriptor field for Encoding policy.** No
  `RecordBodyCompressionPolicy::EncodingCascade`. Iter 2 attempted
  this; council convergence flipped it back to node-local
  config. The encoding substrate is a physical deployment choice, not table
  semantics.
- **No envelope `format_version` bump.** On-disk shape
  unchanged outside `Schema.custom_metadata`.
- **No ADR 022 footer extension.**
- **No `SlabRecordHeader` change.**
- **No new `NodeCapability` bit.**
- **No new public HRFT frame family.**
- **No new wire-protocol surface to Toron.**
- **No `ObjectStore` trait change.**
- **No partial-slab rehydration.**
- **No new compression codec on the wire side.** ADR 053
  unchanged.
- **No new cipher / nonce / AAD shape.** ADR 020
  byte-identical.
- **No new digest domain.** Visible-state digest (ADR 046)
  and per-version-entry digest (ADR 052) invariant under
  Encoding cascade choice.
- **No PBC opcode addition.**
- **No compute-over-encoded-data path.**
- **No synthetic Arrow column for Encoding metadata.**
- **No reserved cascade variant slots, compatibility
  shims, or numeric-version Rust type suffixes.**
- **No project-name prefix on new symbols.**
- **No new Cargo feature or external codec dependency** (no
  Vortex, no Lance, no Parquet pull-in). Implementation may name the
  Arrow IPC stack's existing `flatbuffers` crate directly when
  rewriting RecordBatch metadata.
- **No SQL / DDL surface.**
- **No bulk slab-level cascade-directory pre-fetch.**

## Decision

### 1. Node-local writer config

The encoding writer is selected by deployment-wide storage config, not by a
table descriptor field and not by a protocol-visible
capability:

```rust
pub struct EncodingWriterConfig {
    pub enabled: bool,
    pub target_simd_page_values: SimdPageValueCount,    // 1_024
    pub cascade_sample_rows: SampleRowCount,            // 256
    pub min_buffer_bytes_for_encoding: BufferByteLen,        // 4_096
    pub enabled_cascades: BTreeSet<EncodingCascadeKind>,     // all by V1 default
}

pub struct L2EncodingFetchConfig {
    pub enabled: bool,
    pub max_simd_pages_per_record: EncodingPageCount,        // 64
    pub max_parallel_materialize_tasks: TaskCount,      // 4
}
```

V1 deployment: both configs ship with `enabled = false`;
flipped on after Phase 10 bench acceptance. Admission writes
and LSM compaction writes use the same `EncodingWriterConfig`.
Operators must roll out uniformly across locality replicas
when byte-identical compaction outputs matter (the same
constraint ADR 056 §13 articulated for `RowGroupWriterConfig`).

`enabled_cascades` is the operator's per-cascade kill-switch:
if a cascade family is suspected of causing decode bugs,
operators flip its bit off; the writer falls back to other
cascades for buffers that would have selected it. Set is
closed to V1's eight cascade kinds.

The reader does not need a node config probe: directory
presence inside authenticated record bytes selects the Encoding
materializer. `L2EncodingFetchConfig.enabled` is the reader-side
opt-in for Phase D dispatch; default false during rollout,
flipped on after fleet has the reader code.

There is no `RecordBodyCompressionPolicy::EncodingCascade` in this ADR.
Logical schema fingerprints, table descriptors, Toron APIs,
wire frames, and node capabilities are unchanged.

### 2. Closed cascade catalog (eight variants)

```rust
#[repr(u8)]
pub enum EncodingCascadeKind {
    Plain      = 1,
    Lz4Frame   = 2,
    BitPacked  = 3,
    Rle        = 4,
    Dictionary = 5,
    Delta      = 6,
    FsstWholeBuffer = 7,
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
    FsstWholeBuffer { symbol_table_byte_len: BufferByteLen },
    Alp        { exponent: Exponent, factor: Factor,
                 exception_count: ExceptionCount,
                 inner: Box<EncodingLeafCascade> },
}
```

Composition rules pinned in V1:

- `Plain`, `Lz4Frame`, and `FsstWholeBuffer` are leaf-only (no inner
  parameter).
- `BitPacked` is a leaf with `{ bits, base }` parameters;
  the `base` field captures Frame-of-Reference (a constant
  offset subtracted before bit-packing). Can also serve as
  the inner of `Rle`, `Dictionary`, `Delta`, `Alp`.
- `Rle`, `Dictionary`, `Delta`, `Alp` always wrap an
  `EncodingLeafCascade` inner. Recursive nesting (`Rle(Rle(_))`,
  `Delta(Rle(_))`) is rejected via static type guard.
- `Lz4Frame` cascade variant delegates to ADR 043's
  existing storage LZ4 frame codec; no new frame flag, no
  codec-catalog extension.

This is **closed** — adding a new cascade variant requires
a follow-up ADR. Discriminants are stable and audited by
architectural test.

Cascade applicability by Arrow type (V1):

- **Boolean**: `Plain`, `BitPacked` (1-bit), `Rle`.
- **Signed/unsigned integers / Date / Timestamp**: `Plain`,
  `Lz4Frame`, `BitPacked`, `Rle`, `Dictionary`, `Delta`.
- **UTF-8 / Binary** (offsets + values buffers): `Plain`,
  `Lz4Frame`, `Dictionary`, `FsstWholeBuffer`. Dictionary indices use
  `BitPacked` inner.
- **Float32 / Float64**: `Plain`, `Lz4Frame`, `Alp`.
- **Storage-only synthetic columns** (`__row_hlc__`,
  `__row_key_id__`, `__row_routing_locality__`,
  `__row_tombstone__` when present): cascade selection
  often picks `Rle(BitPacked(_))` for routing locality
  (near-constant within a fragment) or
  `Delta(BitPacked(_))` for HLC (monotonic).

### 3. Encoding directory in IPC custom metadata

Encoding-enabled records carry the `EncodingDirectory` in the
IPC schema message's `custom_metadata` under literal key
`storage.encoding_directory`. Sibling to ADR 056's
`storage.row_group_directory`; both coexist in the same
schema's `custom_metadata` map.

**Compact serialized form** (cascade dedup +
per-buffer prefix-end offsets):

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
    pub encoded_range: BufferByteRange,        // buffer-relative
    pub page_value_count: SimdPageValueCount,  // may differ on
                                               //   partial last page
    pub page_end_offsets: Vec<EncodingPageByteOffset>,
}
```

`encoded_range` is buffer-relative — addresses bytes within
the ADR 056 `BufferPageDirectoryEntry` body range for the
same triple. `page_end_offsets` are buffer-relative
prefix-end offsets within `encoded_range`; page i starts at
`page_end_offsets[i-1]` (or 0 for page 0) and ends at
`page_end_offsets[i]`. Last entry equals `encoded_range.len`.

**Cascade dedup via `EncodingCascadeIndex`.** A row-group column's
physical buffers (e.g., offsets + values for UTF-8) often
share a logical cascade choice. Storing the cascade once in
`cascades: Vec<EncodingCascade>` and referencing by `cascade_index`
captures this relationship and reduces directory bytes on
wide schemas with many physical buffers per column.

**Transient planner expansion.** Planners expand prefix
offsets into per-page descriptors at planning time:

```rust
pub struct EncodingPageDescriptor {
    pub page_index: EncodingPageIndex,
    pub first_value_index: BufferValueIndex,
    pub value_count: BufferValueCount,
    pub byte_range: BufferByteRange,
}
```

`EncodingPageDescriptor` is **not serialized** — it is in-memory
only, derived from `page_end_offsets` during planning. The
serialized directory carries prefix offsets, which fit
materially better than per-page rows on wide schemas
(brainstorm 011 §"Lance idea" tolerates per-record metadata
growth, but ADR 056 bench data shows the metadata window is
already 22–26% of the 64 KiB initial probe; per-page entries
would push past the budget on wide records).

Layout invariants enforced at parse time:

- `logical_schema_fingerprint` matches descriptor's current
  logical fingerprint (ADR 051);
- `row_count` matches ADR 056 row-group directory's row
  count;
- every `BufferEncoding` triple matches an entry in ADR
  056's `BufferPageDirectoryEntry` set;
- no duplicate `(row_group, column, buffer)` encodings;
- `cascade_index < cascades.len()`;
- `encoded_range` fits inside referenced ADR 056 buffer
  range;
- `page_end_offsets` strictly increasing, last entry equals
  `encoded_range.len`;
- page offsets cover `0..decoded_value_count` exactly for
  page-capable buffers (`BitPacked`, `Rle`, `Dictionary`);
- `Plain`, `Lz4Frame`, `Delta`, `FsstWholeBuffer`, `Alp` buffers carry
  empty `page_end_offsets` (architectural guard);
- cascade composition is one level deep;
- cascade parameters validate (`BitsPerValue` 1..=64,
  `RunCount > 0`, `DictValueCount > 0`, etc.).

A directory failing any invariant fails as
`EncodingDirectoryParseError`, `BufferEncodingMismatch`,
`PageRangeOverflow`, or `CascadeUnsupported` — never silent
fallback (ADR 044 §7 / ADR 056 §6 discipline).

### 4. SIMD page directory default 1024 values

Default `target_simd_page_values = 1_024`. Justification:
FastLanes academic literature pins 1024 as the sweet spot
for AVX-512 / NEON SIMD register utilization; brainstorm
011 §3 ("Lance idea") frames page-grain point reads at this
grain. Smaller pages (256, 512) increase directory bytes;
larger pages (2048+) defeat the point-read win.

The encoder may use 64-value SIMD blocks internally for
BitPacked decode loops; the directory does not point at
every 64-value block. SIMD-block-grain page directory is
deferred to a follow-up ADR if bench data justifies the
metadata expansion.

### 5. Phase D materialization

ADR 056's three-phase plan extends to four phases when Encoding
is engaged:

```text
Phase A: metadata + directory probe
  - fetch IPC metadata window;
  - parse schema, RecordBatch headers, RowGroupDirectory,
    EncodingDirectory if present.

Phase B: row-group admission (ADR 056 §4 unchanged)
  - stats prune; predicate-fetch when stats insufficient.
    Predicate-fetch under Encoding may fetch only the required
    SIMD pages, not full buffers.

Phase C: projected page fetch
  - for surviving row groups, fetch buffer-grain ranges
    from ADR 056 directory AND SIMD-page-grain ranges from
    Encoding directory when Encoding engaged for projected columns
    with page-capable cascades;
  - coalesce adjacent page ranges within the same physical
    buffer.

Phase D: Encoding materialization (NEW)
  - for each fetched buffer / SIMD page set:
      - look up BufferEncoding in encoding_directory;
      - if cascade is Plain: hand bytes to Arrow IPC
        reader directly;
      - else: walk cascade outer-to-inner; produce dense
        Arrow buffer in scratch memory;
  - hand materialized buffers to Arrow IPC reader to
    construct RecordBatch;
  - emit dense ProjectedRecordBatch.
```

For point reads served via SIMD page fast path, Phase C
fetches only the page bytes containing the target row;
Phase D materializes that page only. The materializer runs
on the storage-read surface, not as a separate "decode
plane."

The strategy enum is unchanged from ADR 056 — the encoding substrate is internal
to the `RowGroup(RowGroupFetchPlan)` leg:

```rust
pub struct RowGroupFetchPlan {
    // ADR 056 fields...
    pub encoding_materialization: Option<EncodingMaterializationStep>,
}

pub struct EncodingMaterializationStep {
    pub buffer_decodes: Vec<EncodingBufferDecodePlan>,
    pub simd_page_decodes: Vec<EncodingPageDecodePlan>,
}
```

When `encoding_materialization.is_some()`, Phase D runs after
Phase C; when `None`, Phase D is skipped (ADR 056 path
unchanged).

### 6. Writer-side cascade selection

The writer samples each row-group buffer (default
`cascade_sample_rows = 256`) and chooses a cascade via a
closed rule cascade. Selection sketch:

```text
for each (row_group, column, buffer):
    sample = sample_buffer(buffer, sample_rows)
    if buffer_bytes < min_buffer_bytes_for_encoding:
        cascade = Plain
    elif column.dtype is integer-like:
        if sample.run_count is small:
            cascade = Rle(BitPacked { bits, base })
        elif sample.is_monotonic:
            cascade = Delta(BitPacked { bits: delta_bits, base: 0 })
        elif sample.range_fits:
            cascade = BitPacked { bits, base: sample.min }
        elif sample.distinct_count <= 256:
            cascade = Dictionary(BitPacked { bits, base: 0 })
        else:
            cascade = Lz4Frame
    elif column.dtype is UTF-8 / Binary:
        if sample.fsst_compressible:
            cascade = FsstWholeBuffer { symbol_table_byte_len }
        elif decoded_value_count <= target_simd_page_values
             and sample.distinct_count <= 1024
             and sample.avg_length < 64:
            cascade = Dictionary(BitPacked { ... })
        else:
            cascade = Lz4Frame
    elif column.dtype is float:
        if sample.alp_compressible:
            cascade = Alp(BitPacked { ... })
        else:
            cascade = Lz4Frame
    else:
        cascade = Lz4Frame
```

Rules check `EncodingWriterConfig.enabled_cascades` contains the
chosen cascade kind; fall through to the next eligible if
disabled. Phase 10 implementation resolves the `Plain` vs.
`Lz4Frame` fallback question conservatively: unmatched buffers
choose `Plain` when `Plain` is enabled, and choose `Lz4Frame`
only when `Plain` is removed from `enabled_cascades`. This keeps
the representative writer-cost gate under control while preserving
an explicit denser LZ4 fallback mode.

Phase 10 also resolves the low-cardinality UTF-8 default:
variable-width `Dictionary` is selected only when the buffer fits
within one `target_simd_page_values` page. Multi-page variable-width
buffers use `FsstWholeBuffer` when the sample is compressible and otherwise fall
through to the normal `Plain` / `Lz4Frame` fallback. This keeps
`Dictionary` available for small dense buffers while avoiding the
multi-page writer-cost spike the representative ablation exposed.

The heuristic is **closed** — no plug-in points. Adding new
rules or cascade families requires a follow-up ADR. Identical
input bytes plus identical config produce identical cascade
choices and encoded bytes (cross-replica determinism).

### 7. Composition with ADR 042 / 043 / 044 / 056

**ADR 042 (multi-row).** Row-group RecordBatches still carry
the storage-only synthetic columns. Encoding applies cascades to
their buffers — `__row_routing_locality__` typically picks
`Rle(BitPacked(_))`; `__row_hlc__` picks `Delta(BitPacked(_))`.
Synthetic columns stripped before user-facing output, per
ADR 042 §6 unchanged.

**ADR 043 (per-buffer LZ4).** When Encoding engages for any
buffer in a record, IPC `BodyCompression` is set to none for
that record. LZ4 remains available through the `Lz4Frame`
cascade variant, which delegates to ADR 043's storage LZ4
frame codec — no frame flag change, no codec-catalog
extension. Buffers the cascade-selection heuristic chose to
LZ4-only get the `Lz4Frame` cascade; the Encoding materializer
dispatches to the same LZ4 decoder ADR 043 already ships.

**ADR 044 (column fetch).** ADR 044 keeps the same
requirement contract. Encoding only refines byte ranges inside
the row-group leg. Pre-paged records served via ADR 044
column fetch as today.

**ADR 056 (row groups + page directory).** ADR 057 adds
sibling custom_metadata key, sibling directory shape, and
Phase D materialization step. Does not modify ADR 056's
directory shape, metadata key, fetch planner contracts, or
strategy enum. ADR 056 guards 25–26 (no
`partial_buffer_materialize` symbols, no `mini_page_*` fields
in ADR 056's surface) are honored — Encoding's materializer lives
in `src/storage/encoding/`, not in ADR 056's row_group module.

Pre-057 records (no `encoding_directory` key) read via
ADR 056 path transparently; identity-axis output is byte-
identical.

### 8. ADR 052 secondary-index integration

ADR 052 §3 hits map `(SlabId, RecordLocation,
RecordRowIndex)`. ADR 056 added `fetch_indexed_rows` mapping
hits to row-group fetches. ADR 057 extends:

- when an index hit's record has Encoding directory AND projected
  buffers have SIMD pages (cascade is `BitPacked`, `Rle`, or
  `Dictionary`) AND hit count is small (≤
  `max_simd_pages_per_record`):
  fetch only the page bytes containing target row;
- materialize just those pages;
- take the one row.

Adapter signature unchanged from ADR 056. Pre-057 / non-Encoding
records fall back to ADR 056's full-row-group decode path,
byte-identical output.

### 9. ADR 055 LSM compaction integration

LSM compaction outputs go through the same writer:

- compaction worker inherits `EncodingWriterConfig` from the node
  (deployment-wide; per-node override forbidden V1,
  mirroring ADR 056);
- compaction recomputes cascade choices from output values
  (architectural guard; never copies input cascade choices);
- compaction-time cascade choices are deterministic given
  identical input bytes + identical config + identical
  sample-row hash;
- visible-state digest invariance (ADR 055 §6) preserved —
  cascade choice does not change visible row set;
- per-version-entry digest (ADR 052) preserved;
- secondary-index `RecordLocation.record_row_index`
  references preserved.

Pre-056 → post-057 first-pass compaction produces fully-
paged Encoding-encoded output even from unpaged uncompressed
input; this is the graceful three-layer upgrade path.

### 10. CDC, recovery, rehydration are byte-neutral

- CDC catchup reads full record bodies, runs Phase D
  materialization through the storage manager (CDC module
  does not import Encoding-specific types directly, architectural
  guard), emits version entries in row-axis identity order.
- Recovery decodes the IPC stream, parses both directories,
  materializes Encoding buffers via Phase D, rebuilds redb
  indexes. Recovery cost rises by Phase D materialization
  time per record.
- Rehydration installs whole slab bytes; subsequent L1 reads
  find both directories and use the Encoding-aware path
  automatically.

### 11. Existing invariants stay fixed

- ADR 020 authentication unchanged.
- ADR 022 footer layout unchanged.
- ADR 039 CSB pruning runs before any record-body fetch.
- ADR 040 AGG short-circuit runs before any record-body
  fetch.
- ADR 042 synthetic columns stay storage-only.
- ADR 043 codec catalog unchanged; LZ4 carried via
  `Lz4Frame` cascade variant.
- ADR 044 `ColumnFetchRequirement` remains the source of
  required user / predicate / aggregate / storage columns.
- ADR 052 secondary-index sidecars retain identity-axis
  shape.
- ADR 055 compaction rebuilds outputs out-of-place;
  cascades recomputed.
- ADR 056 row-group directory shape unchanged.
- PBC opcode catalog (ADR 035) unchanged. Phase B predicate
  evaluation uses existing PBC opcodes against materialized
  dense batches. ADR 058 lifts this to encoded-grain
  evaluation.

### 12. Capability and protocol surface

No new negotiated `NodeCapability`. ADR 057 is an internal
storage-side substrate. Toron-visible PBC programs, ADR 036
pushdown contract, data-server / control-server wire frames
all unchanged.

A node either writes Encoding-encoded records (writer config
enabled) and reads them via Phase D (reader config enabled)
or does not. Mixed-fleet rollout is byte-identical on
logical results; Encoding metrics will show two populations
during rollout.

### 13. Observability

Metrics under `storage_l2_encoding_*`:

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
              | dictionary | delta | fsst_whole_buffer | alp
              | fsst_row_safe
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

No tenant / table / column / row index / slab id /
error-string / path labels.

Audit events:

- `EncodingMaterializeSampled { cascade, buffer_bytes_in,
  buffer_bytes_out }` (sampled, debug-grade);
- `EncodingInvariantViolation { kind }` (page-grade).

### 14. Reject taxonomy

Closed V1 taxonomy (variants unprefixed; the `EncodingRejectCode`
type already namespaces them, parallel to ADR 056's
`RowGroupRejectCode` family with unprefixed variants):

```rust
#[repr(u32)]
pub enum EncodingRejectCode {
    EncodingDirectoryParseError = 1,
    BufferEncodingMismatch      = 2,
    PageRangeOverflow           = 3,
    CascadeUnsupported          = 4,
    DecodedLengthMismatch       = 5,
}
```

Discriminants start at 1 (zero-init reject doesn't decode
as valid). New codes require a follow-up ADR.

Distinct invariants per code:

- `EncodingDirectoryParseError`: magic / format / length /
  recursive cascade composition / generic structural
  validation failure.
- `BufferEncodingMismatch`: cross-validation against ADR
  056 directory fails (Encoding entry references a triple that
  doesn't exist in `BufferPageDirectoryEntry`).
- `PageRangeOverflow`: page prefix-end offset exceeds
  `encoded_range.len` or `encoded_range` overflows the ADR
  056 buffer range.
- `CascadeUnsupported`: cascade variant or parameter outside
  the V1 catalog / ranges.
- `DecodedLengthMismatch`: actual decoded buffer length
  diverges from `decoded_byte_len`.

Authentication / parse / decode failures route to storage
errors. Planner decisions for valid bytes choose the ADR 056
path for bounded reasons via `EncodingFallbackReason`:

```rust
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

These are metric labels, not corruption rejects.

### 15. Architectural lockdown

`tests/architecture_test.rs` enforces (~30 guards; final
list pinned in Phase 11 of the impl plan):

1. Exactly one definition each of
   `EncodingDirectory`, `BufferEncoding`,
   `EncodingPageDescriptor`, `EncodingCascade`, `EncodingLeafCascade`,
   `EncodingCascadeKind`.
2. Newtype-units for cascade parameters, page indices,
   value counts, sample sizes, task caps.
3. `EncodingCascade` non-leaf variants (Rle, Dictionary, Delta,
   Alp) carry `inner: Box<EncodingLeafCascade>`, never
   `Box<EncodingCascade>`. Static type guard against recursive
   cascade composition.
4. Exactly one materializer entry point in
   `src/storage/encoding/materialize.rs`.
5. CDC, recovery, rehydration code paths do not import
   Encoding-specific types directly.
6. `EncodingRejectCode` taxonomy closed to **five** V1 codes;
   discriminants 1..=5 stable.
7. Authentication, parse, and decode failures cannot reach
   any silent fallback branch.
8. `storage_l2_encoding_*` metrics carry exactly the bounded
   labels.
9. No new `MsgType` containing `Encoding` or `CASCADE`.
10. No new `FRAME_FLAG_*`.
11. No new `NodeCapability` variant.
12. No `ObjectStore` trait method addition.
13. No table descriptor field for Encoding policy. No
    `RecordBodyCompressionPolicy::EncodingCascade`.
14. No synthetic Arrow column for Encoding metadata.
15. No envelope `format_version` bump; no ADR 022 footer
    field addition.
16. `Schema.custom_metadata` key for the Encoding directory is
    the literal string `storage.encoding_directory`.
17. Writer reads from `EncodingWriterConfig` only, not from a
    descriptor field.
18. Compaction writer recomputes cascades; never copies
    input cascade choices.
19. No `Wendelon`-prefixed Encoding identifiers.
20. No `Encoding*V[0-9]` / `Cascade*V[0-9]` Rust type
    suffixes.
21. CDC, recovery, rehydration code paths do not import
    Encoding-specific types directly (they call the
    materializer through the storage manager surface).
22. No PBC opcode addition or PBC executor call from
    inside `src/storage/encoding/`. (Compute-over-encoded-data
    is ADR 058 territory.)
23. `EncodingCascade` discriminants 1..=8 stable; new variants
    require a follow-up ADR.
24. `EncodingLeafCascade` discriminants 1..=3 stable.
25. Cascade selection heuristic is not exposed as a
    plug-in / trait.
26. `BufferEncoding.encoded_range` and
    `page_end_offsets` are buffer-relative, never
    record-relative.
27. `page_end_offsets` (when non-empty) cover
    `0..decoded_value_count` exactly for buffers with
    pages; empty entirely for buffers without pages — not
    partially populated.
28. `Plain`, `Lz4Frame`, `Delta`, `FsstWholeBuffer`, `Alp` cascade
    buffers carry empty `page_end_offsets` (V1 page-emission
    scope).
29. No inline FSST symbol-table sharing across separate
    buffer entries (each FsstWholeBuffer cascade carries its own
    symbol table inline).
30. Materializer code path does not call back into PBC
    executor or AGG fold.
31. Directory's `logical_schema_fingerprint` validates
    against the descriptor's current logical fingerprint
    (ADR 051) at parse time.
32. Serialized `EncodingDirectory` does not carry one
    row per page; page descriptors (`EncodingPageDescriptor`)
    are transient planner-only structures derived from
    `page_end_offsets` prefix arrays.
33. Cascade dedup discipline: `cascades: Vec<EncodingCascade>`
    is stored once per directory; `BufferEncoding`
    references via `cascade_index: EncodingCascadeIndex`,
    never inlines the cascade.

## Rationale

### Why node-local writer config, not descriptor policy

Encoding cascades do not change logical rows, table semantics, PBC
programs, or Toron contracts. A descriptor field would make a
physical deployment choice look like logical schema policy.
Cross-replica byte-equality already enforced via deployment-
wide `EncodingWriterConfig` (mirroring ADR 056 §13). Reader
auto-detects via directory presence — no descriptor probe
needed. Avoids descriptor churn for a physical encoding
choice and reduces operator surface area to one cluster-wide
config instead of per-table policy.

This was the strongest fork across iter 1 / iter 2 / iter 3
council synthesis. Iter 1: Codex descriptor, Claude+Gemini
node-local. Iter 2: Codex flipped to node-local; Claude
adopted Codex's iter 1 descriptor argument; Gemini stayed
node-local. Iter 3: convergence on node-local (council
2-of-3 in iter 2; Claude flips back in iter 3).

### Why a closed cascade catalog of eight

Brainstorm 011 §"Why Home-Roll" frames this: zero bloat,
FIPS / crypto control, MVCC synergy. A plug-in interface
would let third-party encodings land without ADR review —
explicitly rejected at codec-catalog level by ADR 043 / ADR
053. V1's eight cascades cover the integer / boolean /
string / float shapes brainstorm 011 §"Ideas to Steal"
identified.

`Constant` and `FrameOfReference` from iter 2 were folded:
- `Constant` is `Rle { run_count: 1, ... }` — same bytes,
  one variant fewer.
- `FrameOfReference` is `BitPacked { bits, base }` with
  non-zero base — the `base` field IS the FOR offset.

Council convergence on the 8-variant set is council
median of Codex's iter 1 / 2 catalogs and Gemini's.
FsstWholeBuffer and Alp are kept (council majority); their substantial
implementation surface (~700 / ~500 LOC) is the price of
covering high-cardinality strings and floats in V1.

### Why one-level cascade composition

Vortex supports arbitrary-depth nesting. V1 stops at depth
1 because: depth-vs-density curve flattens fast; decoder
dispatch grows quadratic with depth; cascade-validation
logic explodes; bench data from ADR 056 doesn't yet justify
depth-2.

### Why a custom partial-buffer materializer

Stock Arrow IPC reader expects buffer bytes to be Arrow-
conformant. Encoding-encoded bytes are not by construction. The
materializer walks the cascade outer-to-inner and produces
dense Arrow-conformant bytes in scratch memory, which the
IPC reader then consumes. This matches brainstorm 011's
"decompresses the Encoding payload back into Arrow IPC just-in-
time" framing and respects ADR 056's V1-scope guard 25.

### Why directory in IPC custom_metadata, sibling key

Three rejected alternatives:

- *Footer extension*: workload-shape leakage.
- *Bake into ADR 056 directory*: violates V1-scope guard.
- *Synthetic Arrow column*: directory has one logical value
  per `(row_group, column, buffer)` triple, not per row.

Sibling custom_metadata key keeps substrates separable,
encrypted with the record body, authenticated by ADR 020.

### Why buffer-relative page offsets

Gemini's articulation: if `page_end_offsets` used absolute
record byte offsets, every compaction or rewrite that
shifted the buffer would require deep re-computation of Encoding
metadata. Buffer-relative offsets mean Encoding metadata is
invariant as long as the buffer's internal encoded layout is
invariant. Compaction shifts buffer absolute positions
freely without touching `page_end_offsets`.

### Why decode-only V1 (compute-over-encoded deferred)

Compute-over-encoded-data is a substantial PBC-opcode
expansion. Bundling COCD with the substrate ADR would either
defer this inner-layer work behind COCD design churn or ship
a partial COCD design that would need to be redone. ADR 058's
design depends on ADR 057's cascade catalog being concrete;
same layering argument as ADR 056 → ADR 057.

### Why writer-side cascade selection is rule-based, not ML

Determinism (cross-replica byte-equality requires identical
cascade choices given identical input + config); cost (ML
inference per buffer would put writer CPU on the wrong side
of the bench gate); audit (closed rule cascade is
auditable line-by-line); V1 doesn't need it.

### Why `target_simd_page_values = 1_024`

FastLanes academic literature pins 1024 as the sweet spot
for AVX-512 / NEON SIMD register utilization. Smaller pages
(256, 512) increase directory size; larger pages (4096+)
defeat the point-read win.

### Why `enabled_cascades` operator switch

Each cascade family is a non-trivial decoder that may
surface a bug. Operators need a per-cascade kill-switch to
disable one family without disabling Encoding entirely. Set is
closed to V1's eight kinds.

### Why writer-cost gate is ≤ 75% (starting target, with mandatory wins)

Codex iter 3's framing carried in iter 4: ADR 056's ≤ 15%
gate was set without empirical data and missed by 28× on
threshold-margin fixtures and 4× on representative fixtures.
Setting ≤ 25% for Encoding risks repeating the same mistake. Encoding
is a fundamentally denser physical encoding — cascade
selection, multi-encoder dispatch, FSST symbol-table
construction, ALP exception handling all add real cost.
Encoding cost rises mechanically with density.

The starting gate is **≤ 75% writer-CPU increase vs. ADR
056 row groups on the largest legal representative
fixture**, deliberately looser than ADR 056's metadata
gate. The gate is **defended** by mandatory compression and
read-side wins in the same bench artifact:

- compression ratio improvements per Phase 10 acceptance
  targets;
- SIMD-page point-read decoded-bytes reduction ≥ 10× when
  the selected cascade materially compresses;
- full-row-group reader CPU regression ≤ 15% vs. ADR 056.

If Phase 10 evidence shows ≤ 25% is achievable, the gate
tightens. The Provisional discipline (set gate, run bench,
revise on evidence) is the substrate ADR 056 codified;
ADR 057 inherits it with a starting bar that reflects the
denser-physical-encoding reality.

Council resolution: iter 1 had Codex at descriptor / loose
gate, Claude+Gemini at node-local / tight gate. Iter 2 had
Codex flip to node-local while keeping tighter; Claude
adopted descriptor; Gemini stayed node-local / tight. Iter
3 had Gemini flip to descriptor; Claude flipped node-local /
tight. Iter 4 converges: 2-of-3 node-local; Codex's
loose-with-mandatory-wins framing on the gate carries
because it's empirically grounded.

## Consequences

- **Density.** Expected 4–10× compression on int /
  low-cardinality strings vs. ADR 043 LZ4-only baseline.
  Phase 10 bench measures.
- **Decode CPU drops** on point reads via SIMD-page fast
  path (decompress 1 page vs. 1 buffer = 4× / 16× depending
  on page-rows / row-group-rows ratio).
- **Decode CPU rises modestly** on full-buffer
  materialization vs. stock Arrow (cascade dispatch +
  scratch buffer allocation). Bench measures.
- **Writer CPU rises** substantially vs. ADR 056-only path.
  Phase 10 acceptance gate **≤ 75%** on representative
  legal fixture as starting target, defended by mandatory
  compression + read-side wins in the same bench artifact;
  may tighten on evidence per Provisional discipline.
- **Directory size grows** by ~10–20 KiB per record on
  16-row-group × 50-column fixtures. Stays within ADR 044
  64 KiB metadata probe.
- **L2 GET-rate cost falls on point reads** via SIMD page
  ranges.
- **Provider observes finer access pattern.** SIMD-page-
  grain access leaks cascade-selection correlation to the
  provider on top of ADR 044 / ADR 056 leakage. Tenants
  whose threat model treats sub-row-group access as
  confidential keep the Encoding writer config disabled.
- **Mixed legacy / new Encoding presence during rollout.**
  Pre-057 records lack Encoding directories; post-057 may or may
  not. Logical results byte-identical.
- **LSM compaction output bytes-per-row may differ from
  input.** Compaction recomputes cascades; first-pass
  pre-057 → post-057 compaction output is fully Encoding-
  encoded.
- **Recovery cost increases** by Phase D materialization
  time per record.
- **CDC paths unaffected** at contract level; catchup
  decode cost rises by Phase D per emitted version entry.
- **Variable-width string encodings** (Dictionary, FsstWholeBuffer)
  are a correctness hotspot — offsets, dict order, null
  semantics, FSST symbol tables must round-trip
  byte-identically at the Arrow level. Heavy test
  coverage required.
- **No win on `Plain` / `Lz4Frame` cascade buffers**
  (high-entropy data the heuristic can't compress).
- **No compute-over-encoded-data gain in V1.**
- **Implementation surface ~7000–8000 LOC**, mostly
  net-new in `src/storage/encoding/`.

## Rejected alternatives

- **`RecordBodyCompressionPolicy::EncodingCascade` descriptor policy.**
  Rejected: the encoding substrate is a physical deployment knob, not table
  semantics. Iter 2 attempted this; council convergence
  flipped it back.
- **Adopt Vortex / Lance / Apache Iceberg Encoding format
  directly.** Brainstorm 011 §"Why Home-Roll".
- **Open / extensible cascade plug-in interface.** Same
  closed-codec-catalog argument as ADR 043 / ADR 053.
- **Cascade composition arbitrary-depth.** V1 pins depth 1.
- **ML / learned cascade selection.** Determinism, cost,
  audit.
- **Per-page cascade variation within one buffer.**
  Cascade is per row-group buffer; pages within one buffer
  share a cascade.
- **Encoding directory as a synthetic Arrow column.** One
  logical value per record, not per row.
- **Encoding directory in ADR 022 footer.** Workload-shape
  leakage.
- **Bake Encoding cascade fields into ADR 056's
  `BufferPageDirectoryEntry`.** Violates V1-scope guards.
- **Stock Arrow IPC reader handles Encoding bytes.** Not
  Arrow-conformant by construction.
- **Compute-over-encoded-data in V1.** ADR 058 territory.
- **Cross-record / cross-row-group dictionary sharing.**
  Each Dictionary cascade carries its own dictionary
  inline.
- **Per-column-type custom cipher modes.**
- **Half-encoded buffers** (some pages encoded, some not).
- **Reserve cascade variant slots for future encodings.**
  Per `plans/conventions/no-premature-backward-compat.md`.
- **Include `Constant` cascade variant.** Iter 2 had this;
  council convergence drops it as redundant with
  `Rle { run_count: 1, ... }`.
- **Include `FrameOfReference` cascade variant.** Iter 2
  had this; council convergence drops it as subsumed by
  `BitPacked { bits, base }` with non-zero base.
- **SIMD page emission for `Delta`, `FsstWholeBuffer`, `Alp` in V1.**
  Deferred — those cascades are not naturally page-grain
  decodable without additional metadata; future ADR may
  add page-grain support if bench data justifies.
- **Per-page serialized directory rows.** Iter 3 ADR had
  flat `simd_pages: Vec<EncodingSimdPageEntry>` with one row
  per page. Iter 4 adopts Codex iter 3's compact form:
  one buffer entry with prefix-end offsets per buffer.
  Per-page serialization explodes directory metadata on
  wide schemas (50 columns × 16 row groups × 4 pages × 2
  buffers/column = 6,400 entries vs. 200 buffer entries
  with prefix arrays).
- **`storage.encoding_simd_page_directory.v1` versioned
  custom_metadata key** (Gemini's iter 1 / iter 3 shape).
  Per `no-version-suffixes.md`, custom_metadata keys do
  not pre-allocate version suffixes. Sibling key is
  `storage.encoding_directory` without versioning.
- **`MaterializationOverflow` reject code.** Renamed to
  `DecodedLengthMismatch` per Codex iter 3 — more accurate
  framing (the issue is declared length vs. actual decoded
  length, not generic overflow).
- **Tight ≤ 25% writer-cost gate as iter 3 starting
  target.** Council iter 4 loosens to ≤ 75% with
  mandatory compression + read-side wins. ADR 056's
  ≤ 15% gate set without empirical data missed by 28×;
  setting ≤ 25% for Encoding risks repeating the mistake.
  Provisional discipline applies — gate may tighten on
  Phase 10 evidence.

## Open questions

Bench-resolved (Phase 10):

1. **`target_simd_page_values`.** 512 / 1024 / 2048. V1
   default 1024 per FastLanes literature; bench tunes.
2. **`min_buffer_bytes_for_encoding` floor.** 1 KiB / 4 KiB /
   16 KiB. Bench measures cascade-decode dispatch
   overhead vs. encoding savings.
3. **`cascade_sample_rows` sweep.** 64 / 256 / 1024.
4. **Heuristic edge cases.** Run-count threshold for RLE
   engagement; distinct-count threshold for Dictionary;
   ALP exponent / factor coverage; FSST symbol-table
   compressibility threshold.
5. **Per-cascade decode latency curves.** Used to set the
   `max_simd_pages_per_record` reader cap.
6. **`Plain` vs. `Lz4Frame` fallback choice** for
   high-entropy buffers. Phase 10 chooses `Plain` by default
   and leaves `Lz4Frame` as the explicit fallback when `Plain`
   is removed from `enabled_cascades`.
7. **Whether `Delta`, `FsstWholeBuffer`, `Alp` need SIMD-page
   emission immediately** or can stay full-buffer in V1.
8. **Directory size pressure.** ADR 056 directories are
   already ~22–26% of the 64 KiB initial probe; Encoding
   directory adds ~10–20 KiB. Verify combined sum stays
   inside the probe-cap budget across legal fixtures.
9. **Writer-cost gate revision.** ≤ 75% starting target
   with mandatory compression + read-side wins; if Phase
   10 evidence shows ≤ 25% (or tighter) is achievable, the
   gate tightens. Same Provisional discipline as ADR 056.

Architectural (deferred to follow-up ADRs):

10. **Page-level min/max/null stats.** ADR 039 follow-up
    territory; might land before or after ADR 058.
11. **Cross-row-group dictionary sharing.** Density gain
    on highly-redundant string columns; complexity in
    compaction.
12. **Partial materialization within a SIMD page.**
    Decode N rows of a page without decoding the whole
    page. Likely future ADR.
13. **Encoding-on-wire-tier transport.** ADR 053 extension.
14. **Cascade-aware AGG short-circuit.** ADR 040
    pre-fetch may fold from cascade metadata (e.g.,
    `MIN(x) == FOR.base` for entire bit-packed column).
15. **SIMD-block-grain page directory** (64-value pages
    instead of 1024-value). Defer until bench data
    justifies the metadata expansion.
16. **Page emission for Delta / FsstWholeBuffer / Alp.**

## Forward-compatibility analysis

### Substrate sequencing

Three layers deep:

```text
record body
  └─ row-group RecordBatch                     (ADR 056)
       └─ encoded column buffer                 (ADR 057)
            └─ SIMD page (default 1024 values)  (ADR 057)
                 └─ encoded run / cascade leaf  (ADR 058 opens here)
```

ADR 058 (compute-over-encoded-data) lands the innermost
layer. PBC opcodes that evaluate against encoded bytes
consume the cascade metadata ADR 057 fixes —
`EVAL_DICT` walks Dictionary indices; `EVAL_RLE` walks
runs; `EVAL_BITPACKED_RANGE` walks bit-packed bytes
without unpacking. No cascade catalog changes; ADR 058
lifts compute, doesn't change shape.

### Future cascade additions

New cascade variants append to `EncodingCascadeKind`
(discriminants 9, 10, ...) and to the `EncodingCascade` enum.
Existing directory parsers fail closed on unknown
discriminants (`CascadeUnsupported`); pre-add records
remain readable because old discriminants are stable.

### COCD Mechanics

- **`EVAL_DICT` opcode.** Evaluates predicate against
  Dictionary value table once; runs integer-equality
  kernel against bit-packed indices.
- **`EVAL_RLE` opcode.** Per-run predicate evaluation.
- **`EVAL_BITPACKED_RANGE` opcode.** SIMD comparison
  without unpacking.

ADR 057's directory shape is forward-compatible:
cascade parameters expose enough metadata for these
opcodes to dispatch directly.

### Compaction-time refinements

Future LSM compaction follow-ups (size-tiered picker,
subcompaction) consume ADR 055's substrate and compose
with ADR 057's cascade choice transparently — outputs
are deterministic given input + config.

### Wire-tier extension

ADR 053 wire-side compression is currently codec-aligned
(LZ4). A future wire-tier ADR may extend the wire codec
set to include Encoding — substrate is in place. ADR 057
itself does not touch wire surface.

## Deferred to follow-up ADRs

1. **Compute-over-encoded-data.** ADR 058 / brainstorm
   011.
2. **Nested / list / struct Arrow types.**
3. **Page-level min/max/null stats.** ADR 039 follow-up.
4. **Cross-row-group dictionary sharing.**
5. **Arbitrary-depth cascade composition.**
6. **Per-row partial materialization within SIMD pages.**
7. **`EncodingDirectory` cache across queries.**
8. **Bulk slab-level Encoding-directory pre-fetch.**
9. **Cascade-aware AGG short-circuit.**
10. **Encoding on wire-tier transport.** ADR 053 extension.
11. **Per-cascade DEK rotation.**
12. **Public Toron-visible cascade-hint opcode.**
13. **Adaptive cascade re-selection.**
14. **SIMD-block-grain page directory** (64-value
    pages).
15. **Page emission for Delta / FsstWholeBuffer / Alp.**
16. **ML-based cascade selection.**

## References

- `plans/adr/0020_l2_envelope_encryption_and_dek_lifecycle.md`
- `plans/adr/0022_l2_slab_format_footer_indexed_range_reads.md`
- `plans/adr/0035_vectorized_pbc_executor.md`
- `plans/adr/0036_pbc_pushdown_contract.md`
- `plans/adr/0039_slab_footer_column_statistics_v1.md`
- `plans/adr/0040_aggregate_pushdown.md`
- `plans/adr/0041_protocol_naming_decoupling.md`
- `plans/adr/0042_analytical_multi_row_fragments.md`
- `plans/adr/0043_arrow_ipc_body_compression.md`
- `plans/adr/0044_l2_columnar_fetch.md`
- `plans/adr/0051_descriptor_fingerprint_split.md`
- `plans/adr/0052_secondary_indexes.md`
- `plans/adr/0053_wire_arrow_ipc_body_compression.md`
- `plans/adr/0054_cdc_subscription_substrate.md`
- `plans/adr/0055_lsm_leveled_compaction_substrate.md`
- `plans/adr/0056_row_group_fetch_and_page_directory.md`
- `plans/impl/0056_row_group_fetch_bench_results.md`
- `plans/brainstorms/011_encoding_cascades_vortex.md`
- `plans/conventions/newtype-units.md`
- `plans/conventions/no-version-suffixes.md`
- `plans/conventions/no-premature-backward-compat.md`
- `plans/conventions/no-project-name-in-protocols.md`
- `plans/philosophy.md` (Unified Ring; Identity vs
  Locality)
