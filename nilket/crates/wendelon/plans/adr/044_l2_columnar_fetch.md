# ADR 044: L2 Columnar Fetch (Sovereign Pushdown)

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan: `plans/impl/044_l2_columnar_fetch.md`

## Status

Accepted

## Context

ADR 043 shipped per-buffer LZ4_FRAME compression on the
storage-resident Arrow IPC body. The cold-tier read path on
`Lz4Frame` tables now moves ~50% fewer bytes per admitted
record from L2 to the storage node — but it still moves
*whole admitted record bodies*. For an analytical query
projecting 5 of 50 columns over a sealed multi-row table,
ADR 043 fetches all 50 columns' compressed buffers across
the S3-to-node link, decompresses everything, and PBC's
terminal `PROJECT` opcode discards 45 columns at the end.

That is the cold-tier bandwidth concern the council
deliberately deferred from ADR 043 and promoted to
high-priority pickup in `roadmap-post-043.md`. This ADR
closes it.

The infrastructure for chunk-aligned authenticated range
reads from L2 is already in place:

- ADR 020 §2 fixes the per-slab DEK + 64 KiB chunked AEAD
  discipline. Nonces are deterministic
  (`version(u32be) || chunk_index(u64be)`), AAD is the 48-byte
  fixed tuple `tenant_id || slab_id || chunk_index || slab_format_version || cipher_id || reserved`,
  and `crypto::plan_tiered_range_read(plaintext_range)`
  already computes a chunk-aligned ciphertext range from a
  plaintext byte range and decrypts it via
  `TieredRangeReadPlan::decrypt`.
- ADR 022's 256-byte unencrypted footer + variable
  offset-index addresses each admitted record by *plaintext*
  byte offset and length (`TieredOffsetIndexEntry`), so
  chunk-range arithmetic does not need the slab DEK.
- ADR 043 picked per-buffer LZ4 specifically so that each
  Arrow column buffer decompresses independently inside the
  IPC frame.

What's missing is the read-side machinery to use those
substrates for *sub-record* fetch:

1. A way to know, for a given admitted record, the byte
   range each Arrow column buffer occupies inside that
   record's IPC body — and to learn it without first
   downloading the whole record body.
2. A column-projection parameter pushed *upstream* of the
   IPC decode path, instead of post-decode terminal
   `PROJECT`.
3. A planner branch that picks column-projected fetch
   versus whole-record fetch per admitted record, based on
   the PBC program's projection set and the record's size.

ADR 044 fills exactly that gap.

## Non-goals (V1)

- **No homerolled lightweight columnar encodings.**
  FastLanes / ALP / FSST / RLE / encoding cascades —
  Brainstorm 011 V2/V3 territory. ADR 043's per-buffer LZ4
  remains the codec.
- **No compute-over-compressed-data.** PBC continues to see
  decompressed Arrow batches over the projected column set.
- **No mini-page or row-group granularity.** V1 fetch
  granularity stays at the Arrow buffer (≈ column) level.
  Selective predicates that hit a small fraction of rows
  inside a multi-row record still fetch the whole projected
  buffer set, then filter node-side.
- **No per-buffer page directory for O(1) point-read
  decompression.** Brainstorm 011 V2 sub-piece.
- **No provider-native object-store filtering** (S3 Select
  and equivalents). ADR 020 envelope encryption already
  precludes it; ADR 044 does not change the trust posture.
- **No Parquet / SSE cold-tier fork.** Storage architecture
  change with its own zero-trust trade-off; not a substitute
  for column-projected fetch.
- **No new AEAD nonce composition, AAD shape, chunk size,
  cipher catalogue, or key-wrapping change.** ADR 020 stays
  byte-identical. ADR 044 is a *use* of the existing chunk-
  aligned authenticated range-read primitive, not a redesign
  of it.
- **No envelope `format_version` bump.** Slab on-disk shape
  is unchanged; the new functionality is a read-side decode
  path that consumes the existing IPC FlatBuffer metadata.
- **No ADR 022 footer extension.** No per-record column-
  buffer offset index added to the unencrypted footer (see
  Rejected alternatives §). The Arrow IPC FlatBuffer is the
  single source of truth for buffer offsets.
- **No `SlabRecordHeader` change.** 72-byte fixed cleartext
  header per ADR 037 / 042 / 043 stays.
- **No new node capability bit.** Column-projected fetch is
  an internal storage-side optimization derived from the
  existing PBC `PROJECT` opcode; nothing new is negotiated
  on the wire.
- **No new public HRFT frame family** and no new reject-code
  family. Decode failures surface on the same paths that
  handle malformed Arrow IPC and AEAD verification failures
  today.
- **No new wire-protocol surface to Toron.** PBC programs
  carry projection in `PROJECT`; ADR 044 reads it and acts.
  Toron sees the same protocol it does today.
- **No `ObjectStore` trait break.** The existing
  `get_range` surface is sufficient. Provider-specific
  multi-range APIs (S3 `GetObject` with multiple ranges,
  R2 / GCS equivalents) are uneven and not worth widening
  the canonical storage trait before measurements.
- **No partial-slab rehydration.** ADR 006's hot-tier
  rehydration still installs whole slab bytes. Column-
  projected fetch counts toward existing rehydration heat
  but does not change what rehydration installs.
- **No project-name-prefixed names** (per
  `plans/conventions/no-project-name-in-protocols.md`).
- **No `V1` / `V2` suffixes** on new Rust types (per
  `plans/conventions/no-version-suffixes.md`).
- **No reserved enum slots, no `#[serde(default)]` shims, no
  "future ADR will pin this" hedging** in this ADR's spec
  surface (per `plans/conventions/no-premature-backward-compat.md`).
- **No descriptor surface change.** ADR 044 adds no
  field to `TableSchemaDescriptor`. Column fetch is a
  per-query, per-record read strategy decided at L2 fetch
  time, not a per-table policy. A table either has Arrow
  IPC records that admit partial materialization or it
  does not — orthogonal to a descriptor knob, and the right
  decision varies with each query's projection. No
  descriptor-fingerprint churn for a read-side
  optimization.
- **No compatibility shim for pre-044 readers.** No deployed
  external storage-format contract to preserve; the on-disk
  shape is unchanged.

## Decision

### 1. Two-phase chunk-aligned column fetch

A column-projected cold-tier fetch for one admitted record
runs in two phases over the same encrypted slab object:

```text
Phase A (metadata pull):
  - Read footer + offset index (existing path).
  - For each admitted record:
      ciphertext_range_for(record.plaintext_range[0 .. metadata_window])
      → AEAD-decrypt → parse Arrow IPC FlatBuffer message header
      → extract per-buffer (offset, length) vector.
  - Output: RecordBufferDirectory per record.

Phase B (column fetch):
  - From the PBC program's filter and project column sets,
    union the buffers each projected column requires
    (data + validity + offsets buffers per Arrow column).
  - Map buffer ranges → IPC body byte ranges
    → record-level plaintext byte ranges
    → chunk-aligned ciphertext ranges (existing
      plan_tiered_range_read primitive).
  - Coalesce contiguous chunk runs into the minimum number
    of L2 Range-GETs.
  - AEAD-decrypt fetched chunks, decompress LZ4 buffers
    when present, decode RecordBatch with column projection,
    hand to PBC.
```

`metadata_window` is the prefix size sufficient to carry
the IPC FlatBuffer schema + RecordBatch message metadata
(empirically a few KiB for typical analytical schemas).
The V1 default starts at one chunk = 64 KiB and grows by
one chunk at a time until either the metadata parses or
`max_ipc_metadata_probe_bytes` (default 256 KiB) is
reached — at which point the record falls back to whole-
record fetch (§7).

The "extra round trip" framing slightly oversells the
cost: if the projection touches *any* column whose buffers
live near the head of the IPC body (commonly a leading
fixed-width integer column, a leading validity bitmap, or
the dense buffer indexes column families produce), the
metadata-window chunk *is the chunk we'd have fetched
anyway*. Phase A becomes a free byproduct of Phase B in
that case, not a real round trip. Phase 7 of the impl plan
short-circuits the explicit Phase A whenever the planner
can prove this overlap.

### 2. Authentication invariants are preserved exactly

ADR 044 introduces no new authenticated surface. Every byte
returned to PBC has been verified by an existing
authenticator:

- Footer + offset-index → existing tenant-KEK HMAC over the
  ADR 022 footer scope.
- Each fetched chunk → existing per-slab-DEK AEAD with
  deterministic chunk-ordinal nonce and the unchanged 48-
  byte AAD tuple. Nothing in `chunk_index`, `slab_id`,
  `tenant_id`, `slab_format_version`, or `cipher_id` is
  weakened or rederived.

The two-phase fetch is therefore not a new protocol — it is
two consecutive uses of the existing chunk-aligned range-
read primitive. An adversarial L2 cannot splice chunks
across slabs (different `slab_id` in AAD), reorder chunks
within a slab (different `chunk_index` in nonce/AAD),
forge IPC metadata bytes (the chunks they live in are
AEAD-verified before parsing), or cause the reader to
fetch and trust unauthenticated bytes (every byte enters
the decoder only after AEAD verify).

A buffer-offset value parsed out of an AEAD-verified IPC
metadata chunk is itself trustworthy by transitivity: the
chunk it came from authenticated, the IPC FlatBuffer
parser is fed only authenticated bytes, and the resulting
`(offset, length)` pairs index into ranges that are *also*
chunk-AEAD-authenticated when fetched.

### 3. The Arrow IPC FlatBuffer is the single source of truth

ADR 044 deliberately does **not** add a per-record column-
buffer offset index to the ADR 022 unencrypted footer. The
IPC FlatBuffer's `RecordBatch` message already carries the
per-buffer `(offset, length)` vector required for column-
projected decode. Adding a parallel index in the footer
would create:

- A second source of truth (footer index vs. IPC metadata)
  that can drift, requiring an architectural test to
  enforce equality.
- A write-time format change with a footer migration
  story for pre-044 sealed slabs.
- ADR 022 footer-HMAC scope changes (the index would be
  inside the HMAC; pre-044 footers would not include it).
- A larger unencrypted footer surface (≈ C × R × 12 B for C
  columns × R records per slab — ~60 KiB for a 50-column,
  100-record slab).
- Operator confusion: which index governs?

Reading the IPC FlatBuffer keeps the existing format
authority discipline from ADR 043 §4: physical records are
self-describing.

The metadata window is robust by construction:

- The IPC FlatBuffer schema message and RecordBatch message
  headers live at the head of the IPC stream.
- Each FlatBuffer message is length-prefixed by a 4-byte
  `metadata_length` (Arrow IPC convention).
- The parser reads the prefix, reads exactly that many
  bytes, and stops. If `metadata_length` extends past the
  metadata window the reader holds, it issues a single
  follow-up fetch for the remainder. The body buffers
  themselves are not read in Phase A.

Empirically a 50-column RecordBatch IPC message header is
on the order of low single-digit KiB. The 64 KiB window
covers it with comfortable margin.

### 4. Partial materialization preserves PBC semantics

Today's read path is "decode whole RecordBatch → terminal
PROJECT." ADR 044 changes the *decode* call so that non-
projected columns never materialize as Arrow arrays in
node memory. The output of partial decode is:

```rust
pub struct ProjectedRecordBatch {
    batch: RecordBatch,                // physical, narrow schema
    ordinal_mapping: ColumnOrdinalMapping,  // descriptor ordinal → physical column index
}
```

The `ordinal_mapping` is load-bearing: PBC opcodes refer to
columns by *descriptor* ordinal (per ADR 035 / 036), but
the physical batch only carries projected columns. The
executor consumes `ProjectedRecordBatch` and resolves
descriptor ordinals through the mapping before indexing
into the physical batch. This avoids widening any public
PBC wire format and keeps ADR 035's `LOAD` / `PROJECT`
opcode semantics unchanged.

The projection set seen by decode is the *union* of:

- Filter columns referenced by PBC `LOAD` opcodes.
- Project columns named in the terminal `PROJECT` opcode.
- Aggregate input columns referenced by AGG accumulators
  (ADR 040), when the storage path is the AGG fold.
- Storage-only synthetic columns (ADR 042) needed for row
  visibility — `__row_hlc__`, `__row_key_id__`,
  `__row_routing_locality__`, future `__row_tombstone__`
  — but only when the scan path actually requires them
  (e.g. visibility-metadata folding). They never appear in
  user-facing PBC programs and are stripped before any
  response leaves the storage layer.

After PBC filtering, the post-filter `take` materializes
only the project columns through the ordinal mapping.

### 5. Range planning extends the existing ADR 020 primitive

`crypto::plan_tiered_range_read` already converts a single
plaintext byte range into a chunk-aligned ciphertext range,
with full footer-HMAC and per-chunk AEAD authority. ADR 044
introduces a thin multi-range generalization:

```rust
pub struct TieredPlaintextRange {
    start: RecordBodyByteOffset,
    len:   RecordBodyByteLen,
}

pub struct TieredMultiRangeReadPlan {
    chunk_runs: SmallVec<[ChunkRun; 8]>,    // contiguous chunk index ranges, sorted, disjoint
    fetched_bytes: u64,                     // ciphertext bytes (planning estimate)
    plaintext_extracts: SmallVec<[PlaintextByteRange; 16]>, // post-decrypt slices to surface
}
```

`TieredMultiRangeReadPlan` accepts a sorted list of
plaintext record-relative ranges plus the already-fetched
footer / offset-index metadata, converts ranges to ADR 020
chunk runs, coalesces adjacent or overlapping chunk runs,
and returns the encrypted byte ranges to fetch plus enough
metadata to decrypt each run and slice out the requested
plaintext windows. The existing single-range planner
becomes a wrapper around the multi-range implementation;
both share the same authentication path.

Three new types in `src/storage/columnar.rs` (new module)
support the column-projection-specific layer above the
multi-range planner:

```rust
pub struct RecordBufferDirectory {
    record_id: TieredRecordId,
    record_plaintext_range: PlaintextByteRange,
    /// Indexed by Arrow IPC buffer ordinal (not column ordinal).
    /// Multi-buffer columns (UTF-8, List, etc.) span N entries.
    buffers: SmallVec<[IpcBufferRange; 32]>,
    /// Maps IPC schema column ordinal → buffer ordinals consumed
    /// by that physical IPC column. Descriptor/user ordinals are
    /// resolved to IPC ordinals before this map is consulted;
    /// ADR 042 synthetic columns occupy their appended IPC slots.
    column_to_buffers: SmallVec<[ColumnBufferRange; 16]>,
    /// Descriptor fingerprint captured from the validated table
    /// descriptor. Used to reject cross-record/schema mixing when
    /// a slab-level or query-level plan spans multiple records.
    schema_fingerprint: SchemaFingerprint,
}

pub struct IpcBufferRange {
    offset_in_record: IpcBufferOffset,  // newtype over u64
    length: IpcBufferLength,             // newtype over u64
}

pub struct ColumnBufferRange {
    first_buffer_ordinal: u32,
    buffer_count: u32,
}
```

A `TieredMultiRangeReadPlan` for a column-projected record
fetch is built from a `RecordBufferDirectory` plus a
`ColumnProjection` plus the slab's footer metadata. The
plan-building step coalesces adjacent buffers' chunk runs
into a single Range-GET and reuses
`plan_tiered_range_read`'s authentication exactly — no
parallel chunk-math implementation, no new authentication
surface.

### 6. Selectivity-aware planner branch (node-local)

The storage-side admission path picks fetch shape per
admitted record using a node-local config — *not* a
descriptor field:

```rust
pub struct L2ColumnFetchConfig {
    enabled: bool,
    ipc_metadata_initial_window_bytes: RecordBodyByteLen, // probe starts here
    max_ipc_metadata_probe_bytes:      RecordBodyByteLen, // probe gives up here
    min_record_bytes:                  RecordBodyByteLen, // bypass below here
    max_selected_fraction_micros:      u32,               // fixed-point (670_000 = 0.67)
    max_ranges_per_record:             EncryptedChunkCount,
    max_parallel_range_gets:           EncryptedChunkCount,
}
```

V1 defaults (tunable at node startup, not per-query, not
per-table):

- `enabled` = true on production profiles once Phase 11
  bench targets are met.
- `ipc_metadata_initial_window_bytes` = 64 KiB (one ADR
  020 chunk).
- `max_ipc_metadata_probe_bytes` = 256 KiB (4 chunks).
  Probe starts at the initial window; on
  `MetadataTruncated { needed }`, grows once up to this
  cap; if the metadata still does not parse, the record
  falls back as `metadata_probe_cap`.
- `min_record_bytes` = 256 KiB. Records below this fit in
  ≤ 4 chunks; planning + Phase-A overhead dominate the
  byte savings. Records this small bypass Phase A
  entirely (see decision rule below).
- `max_selected_fraction_micros` = 670_000 (0.67).
- `max_ranges_per_record` = 8.
- `max_parallel_range_gets` = 4.

The fixed-point fraction (`u32` micros instead of `f32`)
avoids floating-point in config validation and metric
labels. Validation rejects an initial window larger than
the probe cap, zero probe cap, fraction at or above
1_000_000, zero range caps, and zero `min_record_bytes`.

The decision rule has *five cheap bypasses before Phase A*
(no metadata round trip when the answer is trivially
"whole record") and a richer taxonomy after Phase A
succeeds:

```text
For each admitted record:
  // Cheap bypasses — no Phase A round trip.
  if not config.enabled:
      use whole-record fetch (Disabled).
  if descriptor.record_layout_policy.is_single_row():
      use whole-record fetch (SingleRow).
  required = ColumnFetchRequirement (§4)
  if required.is_full(descriptor_user_schema):
      use whole-record fetch (FullProjection).
  if record.length < config.min_record_bytes:
      use whole-record fetch (SmallRecord).
  if rehydration_active(record):
      use whole-record fetch (RehydrationAvailable).

  // Phase A — fetch + decrypt the metadata window.
  directory = parse_record_buffer_directory(...)
  if directory == MetadataProbeCap:
      use whole-record fetch (MetadataProbeCap).
  if directory.layout_unsupported:
      use whole-record fetch (UnsupportedArrowLayout).

  // Phase A succeeded — build plan, check thresholds.
  plan = build_column_fetch_plan(directory, required)
  if plan.fetched_bytes + probe_bytes
       > record.length * config.max_selected_fraction_micros / 1_000_000:
      use whole-record fetch (TooManyBytes).
  if plan.chunk_runs.len() > config.max_ranges_per_record:
      use whole-record fetch (TooManyRanges).

  use the columnar plan.
```

The dispatcher carries a single value of:

```rust
pub enum L2FetchStrategy {
    WholeRecord,
    Columnar(ColumnarRecordFetchPlan),
    Fallback(ColumnarFallbackReason),
}

pub enum ColumnarFallbackReason {
    Disabled,
    SingleRow,
    FullProjection,
    SmallRecord,
    TooManyBytes,
    TooManyRanges,
    MetadataProbeCap,
    UnsupportedArrowLayout,
    RehydrationAvailable,
}
```

`pick_l2_fetch_strategy` itself returns either
`Columnar(plan)` or `Fallback(reason)`. `WholeRecord` is
reserved for dispatcher-level whole-record execution where
column planning is intentionally not invoked (for example,
an unencrypted L2 object without ADR 020 chunk/index
metadata). It is not a fallback reason and therefore does
not increment the fallback counter.

Authentication failures and invalid-data errors are *not*
variants of this enum — see §7. Each reason that *is* a
variant is a first-class metric label so operators can
tune the right knob. Fallback for any of these reasons
returns *byte-identical* logical results to the columnar
path — fallback is a planning-economics decision, not a
correctness escape.

Dispatch sites match each variant explicitly; an
architectural test forbids `_` catch-all arms so that
adding a future variant forces every consumer to update
deliberately.

### 7. Authentication failures never fall back

Fallback is for planning economics and unsupported Arrow
layouts only. If a chunk fails AEAD verification, a footer
fails its tenant-KEK HMAC, the IPC FlatBuffer fails to
parse *after* successful AEAD verification, or any
buffer-span sanity check fails (offset overflow, span
beyond `RecordLocation.length`, monotonicity violation),
the read fails as a corruption / invalid-data error on the
same paths that handle malformed Arrow IPC today.

Fetching more bytes must not turn a corruption signal into
a successful query. This rule is enforced by an
architectural test: the `Fallback(...)` branch in the
planner cannot receive an authentication or invalid-data
error — those paths short-circuit to the existing
storage-error reporting.

### 8. Rehydration stays whole-slab

ADR 006's hot-tier rehydration installs whole slab bytes
into L1. ADR 044 does not introduce partial-slab
rehydration: column-projected reads still increment the
existing rehydration heat counters, but once promotion
triggers, the manager fetches and installs the full slab
exactly as today. L1-resident slabs are read via the
existing local-file path; ADR 044 affects only the
tiered-L2 cold read path.

This is the same "graceful coexistence under existing
primitives" pattern ADR 042 / 043 used. Hot-table
workloads converge back to L1 rather than staying
permanently in columnar cold-read mode.

### 9. ADR 035 / 036 PBC composition

PBC's contract is unchanged. The vectorized executor keeps
consuming a decoded `RecordBatch` and returning a Boolean
mask + projected output. What changes is *which*
`RecordBatch` it sees: one materialized only over the
columns the program references, not the full schema.

The PBC opcode catalog is unchanged. No new opcode is
introduced for column projection — the *existing* terminal
`PROJECT` is the source of the projection set, and `LOAD`
opcodes are the source of filter-column requirements.

ADR 036's pushdown contract holds: Toron emits the same PBC
program; the storage node decides fetch shape internally.

### 10. ADR 040 AGG composition

AGG accumulators (`agg::*`) consume only their declared
input columns. ADR 044 lets the storage path fetch only
those columns from cold tier. The CSB short-circuit (ADR
040 §5 + ADR 042 §11) runs *before* any record-body fetch,
so it is unchanged. AGG metric latency improves on cold-
tier reads with narrow accumulator inputs.

### 11. ADR 042 multi-row composition

Multi-row records are the primary beneficiary: large IPC
bodies with many columns and many rows. `record_row_index`
projection (ADR 042 §10) composes naturally — column
projection narrows the buffer set; row-index projection
narrows the row set. Both happen post-decode against the
already-narrow batch.

`__row_hlc__`, `__row_key_id__`, and
`__row_routing_locality__` are storage-only synthetic
columns (ADR 042 §6). Their buffers are projected in only
when a PBC opcode or a visibility-metadata fold requires
them. Routine analytical scans never decompress them.

### 12. ADR 043 per-buffer LZ4 composition

Each fetched chunk run carries the chunk(s) that contain
the projected buffers. After AEAD decrypt, LZ4 decompresses
each buffer independently — the load-bearing property
ADR 043 selected per-buffer LZ4 for. Non-projected buffers
are never decompressed.

A buffer's LZ4-compressed bytes may straddle a 64 KiB chunk
boundary (Arrow's per-buffer compression frame is byte-
aligned, not chunk-aligned). The fetch planner over-fetches
chunk-aligned: if a buffer's compressed bytes occupy IPC
bytes [a, b], the plan fetches the chunk run covering
[chunk_for(a), chunk_for(b)]. Chunk-spanning buffers are
the common case and add no new code path.

### 13. Pre-044 sealed slabs are read transparently

A sealed pre-044 slab is *byte-identical* to a post-044
sealed slab — ADR 044 changes only the read path. The
two-phase fetch path consults the IPC FlatBuffer metadata,
which has been emitted by the writer since ADR 042. No
migration is required, no operator-triggered recompact is
required, no descriptor-fingerprint churn happens. Mixed-
shape slab corpora work indefinitely.

This is the same "read existing physical bytes via a new
decode path" pattern ADR 043 §4 used for the IPC-
FlatBuffer-driven compression dispatch.

### 14. Recovery is unchanged

Recovery from raw slab bytes (ADR 042 §3, ADR 043 §8)
decodes Arrow IPC and rebuilds the redb key/locality
indexes. Recovery does not run column-projected fetch — it
needs every record body. ADR 044 introduces no new recovery
code path and no new recovery dependency.

### 15. Capability and protocol surface

No new negotiated `NodeCapability` is introduced. ADR 044
is an internal storage-side read-path optimization. The
Toron-visible PBC program is unchanged; ADR 036's pushdown
contract is unchanged; the data-server / control-server
wire frames are unchanged.

A node either runs ADR 044's column-projected fetch path or
does not (build-time matter). When `L2ColumnFetchConfig::
enabled = false`, every read uses whole-record fetch with
no protocol error and no observable result-shape
difference. Mixed-fleet rollout is therefore safe.

## Rationale

**Why two-phase fetch over footer extension.** The IPC
FlatBuffer already carries authoritative per-buffer offsets
that survive any future Arrow upgrade. Putting a parallel
index in the ADR 022 footer means writers maintain two
sources of truth, the footer-HMAC scope grows, and
pre-044 sealed slabs need a migration step. Two-phase fetch
costs one extra small Range-GET per admitted record (or
zero, when the planner batches multiple records' metadata
chunks into a single GET, or when the head chunk is
already fetched as part of the projected buffer set). The
cost is bounded; the duplication isn't.

**Why no unencrypted IPC metadata in the cleartext
footer.** Even if data buffers stay encrypted, the Arrow
schema and the per-buffer offset shape themselves leak
workload information: column count, column types, buffer
ratios that signal high-cardinality vs. low-cardinality
columns, presence of dictionary encodings, presence of
nested types, presence of synthetic columns. ADR 020's
zero-trust posture treats schema and layout as
confidential alongside data. A cleartext-footer index
would weaken that line for no operational gain — the
authenticated chunked AEAD already lets the reader pull
encrypted metadata at chunk granularity, and the per-
record metadata is small enough that the round trip is
not the dominant cost (see "head-chunk usually fetched
anyway" framing in §1).

**Why no new authentication surface.** ADR 020's chunk-
aligned AEAD is the load-bearing primitive. Any redesign
risks the zero-trust posture. Two-phase fetch is two uses
of the *same* primitive; nothing changes about nonces,
AAD, key wrapping, or chunk boundaries.

**Why selectivity-aware fallback in V1.** A naive "always
column-fetch" rule would regress queries that project most
columns (extra round-trip + planning overhead, no bytes
saved). A naive "never column-fetch" rule wastes the
substrate ADR 043 set up. The planner branch with two
thresholds is the minimum viable shape; both thresholds
are tunable as fleet metrics accumulate.

**Why projection set comes from PBC, not a new wire field.**
Toron already sends `PROJECT` and `LOAD` opcodes that fully
describe the column dependency. Adding a parallel "fetch
projection" field to the wire would create another source
of truth and another integration burden on Toron. Storage-
side derivation from the existing program is free.

**Why upstream decode projection rather than post-decode
prune.** Even with column-projected fetch, a node that
decodes the full RecordBatch and projects post-hoc still
allocates Arrow arrays for non-projected columns. Arrow's
per-buffer skip on `StreamReader` avoids that cost. The
combined "fetch fewer bytes + decode fewer columns" is
where the latency win lands.

**Why no per-row mini-page granularity in V1.** That
requires either an Arrow IPC layout extension, a
homerolled encoding (Brainstorm 011 V2), or a page
directory in the footer. All three are multi-quarter
designs. Buffer-level fetch captures the dominant column-
projection win on the schemas analytical workloads actually
have. Row-level pruning lands when measurements show
buffer-level fetch leaves significant bandwidth on the
table.

**Why no provider-native filtering parity.** Same as
ADR 043: ADR 020 envelope encryption is incompatible with
S3 Select. Switching L2 to Parquet plus provider-side
encryption is a storage-architecture change, not a fetch
optimization.

**Why no descriptor field at all.** Column fetch is a
read strategy, not a table policy. The decision varies per
query (different projections at different selectivities)
and per record (different sizes, different chunk counts).
A descriptor knob would be too coarse — it cannot capture
"this table benefits from columnar fetch on the analytical
queries but not the lookup queries against it." A
descriptor knob would also churn the descriptor fingerprint
for a read-side optimization that does not change the
logical schema, the physical record layout, or the
on-disk bytes. Node-local config keeps the surface small,
keeps Toron unaware (no schema-update path change), and
lets the planner make the right per-query decision.

**Why fixed-point fraction in node config.** Floating-point
config values fail to round-trip cleanly through TOML / YAML
edits, complicate validation (NaN, signed zero, denormals),
and are awkward to surface in metric labels. A
`max_selected_fraction_micros: u32` (0–1_000_000) is
unambiguous, validates with a simple range check, and reads
cleanly in metric output.

**Why the `storage_l2_column_fetch_*` metric prefix.** The
existing storage-resident metric families established in
ADR 042 / 043 use `storage_multirow_*` and
`storage_compression_*`, including
`storage_compression_l2_record_body_bytes_fetched_total`
(ADR 043 §9). ADR 044 stays in that storage metric family.
The prefix is intentionally longer than a hypothetical
`l2_column_fetch_*` to express that this is *storage's
L2 column-fetch observability*, parallel to *storage's
compression observability* and *storage's multirow
observability* — not a sibling family living outside the
storage namespace. Operator dashboards that already
filter on `storage_*` to find storage-internal signals
pick up ADR 044 metrics without re-tuning their
selectors.

**Why no sparse dummy arrays for unfetched columns.**
A physical batch with null placeholders for columns that
were never fetched would create a *second* interpretation
of column presence. The first interpretation —
"`ordinal_mapping` says this descriptor ordinal has no
physical column" — is unambiguous: PBC and AGG paths
consult the mapping. The second interpretation —
"`batch.column(i)` returns a null array" — is plausible
to a downstream caller who forgot to consult the mapping,
and the resulting code would silently produce wrong
results (running a `MIN(col_x)` aggregator on an all-null
array yields `NULL`, not "this column was never
fetched"). The `ProjectedRecordBatch` shape keeps the
invariant crisp: present columns are real arrays; absent
descriptor ordinals are unavailable unless the requirement
analysis specifically requested them. Dummy nulls are
forbidden by an architectural unit test.

**Why no node capability negotiation.** Column-projected
fetch is internal storage behavior. A mixed-fleet rollout
sees some nodes do column fetch and others do whole-record
fetch — query results are byte-identical.

**Why no envelope `format_version` bump.** The on-disk shape
is unchanged. The IPC FlatBuffer is the format authority,
unchanged from ADR 043 §4.

## Consequences (the negatives)

- **Per-record metadata round trip.** Two-phase fetch costs
  one extra small Range-GET per admitted record on average.
  When admitted records are contiguous, the planner
  coalesces metadata fetches into one larger GET; when they
  are scattered, the round-trip cost is real. Quantified by
  a new metric
  (`storage_l2_column_fetch_phase_a_round_trips_total`).
- **Planner overhead.** Per-admitted-record selectivity
  decision + chunk-coalescing computation is non-trivial.
  CPU cost is bounded (linear in projected buffer count and
  chunk count), but it is non-zero. Visible in
  `storage_l2_column_fetch_plan_micros`.
- **Worse small-record latency.** For records that fall
  below `min_record_bytes`, the whole-record
  path runs as today, but the planner still touches the
  record to make the decision. Bypass is possible (e.g.
  short-circuit on slab-stat-derived average size) and is a
  Phase 8 refinement.
- **Chunk over-fetch.** Buffer-to-chunk alignment means a
  small projected buffer near a chunk boundary still costs
  a full chunk fetch. ADR 020's 64 KiB chunk size is the
  floor on per-buffer fetch; ADR 044 inherits it.
- **No mini-page row pruning.** Selective predicates that
  hit, say, 1% of rows in a 4096-row record still fetch the
  full projected buffers. The "fetch fewer rows from cold
  tier" axis stays open as a future ADR.
- **Schema drift between metadata and body chunks.** A
  pathological corruption that AEAD-verifies the metadata
  chunk but corrupts a body chunk surfaces as an Arrow
  decode error during column materialization, not as an
  AEAD-verify failure. The corrupted-body case is rare and
  surfaces on any read path; it is not new under ADR 044.
- **Mixed legacy / new fetch shape during rollout.** Until
  a build is fully ADR 044-aware, some nodes serve whole-
  record fetches and others column-projected fetches. Query
  *results* are byte-identical, but cold-tier byte-fetched
  metrics will show two populations.
- **Increased L2 GET count.** Even with chunk coalescing,
  column-projected fetch issues more (smaller) GETs than
  whole-record fetch. S3 / R2 / GCS bills GETs as well as
  bytes; on tenants whose workload is metadata-heavy with
  small projections, GET-rate cost may approach byte-rate
  savings. `L2ColumnFetchConfig::max_ranges_per_record` and
  `L2ColumnFetchConfig::enabled` exist exactly to let
  operators fall back cluster-wide if the bill flips the
  wrong way. The fallback-reason metric
  `storage_l2_column_fetch_fallback_total{reason="too_many_ranges"}`
  surfaces the tuning signal.
- **Provider observes column-correlated access pattern.**
  The L2 provider sees ciphertext-only chunk-range
  requests, but the *pattern* of which chunk runs are
  fetched correlates with which columns the query
  projected. ADR 022 already accepts chunk-granularity
  access-pattern leakage for whole-record range reads;
  ADR 044 makes the leakage more column-correlated. This
  is qualitatively the same threat (provider learns
  "this slab was scanned for predicate X") but more
  granular ("provider learns which columns were
  scanned"). Tenants whose threat model treats column
  projection as confidential should disable column fetch
  via the node config or run a private object-store
  backend.
- **Overfetch for variable-length columns.** UTF-8 / List
  columns produce 2–3 Arrow buffers each (data + offsets
  + nullability). Projecting one logical column fetches
  all its buffers — "column-level" fetch is in fact
  "buffer-set per column" fetch. Not a regression; just a
  ceiling on the win.
- **Two-phase fetch interacts with the L2 client's
  retry/backoff policy.** A retry on Phase B does not
  invalidate Phase A's parsed metadata; a retry on Phase A
  invalidates a cached `RecordBufferDirectory`. The
  invalidation rule is straightforward but new.
- **Synthetic-column overhead remains until projection
  reaches them.** ADR 042's `__row_hlc__` /
  `__row_key_id__` / `__row_routing_locality__` buffers
  are skipped only when no opcode references them. PBC
  programs that consult them (e.g. visibility-metadata
  folds) pay the same cost they did pre-044.
- **One more place where Arrow-version regressions can
  bite.** The IPC FlatBuffer parser used in Phase A is
  Arrow-version-specific; an upgrade that changes the
  buffer-vector encoding requires re-pinning the metadata
  parser. ADR 043's golden-fixtures discipline (semantic
  round-trip, not raw bytes) extends to ADR 044's
  metadata-parse golden tests.
- **Initial unsupported-Arrow-layout fallback.** The first
  partial materializer may not cover every Arrow type
  (nested structs, dictionaries, union types). Tables with
  unsupported layouts fall back to whole-record fetch
  uniformly via `unsupported_arrow_layout`. As partial
  materializer support broadens in Phase 4 follow-up work,
  fallback rate should drop. The metric
  `storage_l2_column_fetch_fallback_total{reason="unsupported_arrow_layout"}`
  surfaces what is uncovered.
- **No win on uncompressed records.** Per-buffer LZ4 from
  ADR 043 is what makes per-buffer-independent decode
  cheap. Uncompressed `MultiRow` tables (rare under ADR
  043's layout-driven default but possible for high-
  entropy payloads) still reduce cold-tier *bytes* but not
  *decompression* CPU. The planner runs uniformly; the
  density win is just smaller.

## Rejected alternatives

- **Extend ADR 022 footer with per-record column-buffer
  offset index.** Two sources of truth (footer index +
  IPC FlatBuffer); writer migration burden; HMAC scope
  growth; ~60 KiB extra footer for typical analytical
  schemas. Two-phase fetch achieves the same column-fetch
  capability without any of those costs and is the design
  the council should reach for unless a metric proves
  metadata round-trip is a real bottleneck.
- **`ColumnFetchPolicy` descriptor field
  (`{ Auto, Disabled }`).** Rejected in favour of the
  node-local `L2ColumnFetchConfig`. The descriptor field
  would churn the descriptor fingerprint for a read-side
  optimization with no logical-schema or physical-record
  change; it is too coarse for queries that vary in
  projection density; and it forces Toron to learn a knob
  that contributes nothing to query semantics. Node-local
  config makes the decision per-query, per-record, where
  it belongs.
- **Provider-native multi-range API in the `ObjectStore`
  trait.** S3 `GetObject` accepts multiple ranges; R2 /
  GCS support is uneven. Widening the canonical storage
  trait before measurement is premature. Bounded parallel
  `get_range` calls portably cover LocalFs, RustFS,
  S3-compatible, B2, and GCS backends; if a future
  bench shows a cost win from native multi-range, it lands
  as its own ADR.
- **Move Arrow IPC FlatBuffers into the cleartext slab
  footer.** Schema and metadata are confidential under ADR
  020. Promoting them to cleartext would leak schema
  shape to the provider for no operational gain
  (descriptor-fingerprint and per-record metadata are
  already authenticated under tenant KEK / per-slab DEK).
- **Always column-fetch on `Lz4Frame` `MultiRow` tables
  regardless of projection ratio.** Regresses queries that
  project most columns; unconditionally pays metadata
  round-trip cost. The planner branch is the relief valve.
- **Add a new "fetch projection" field to PBC programs or
  to the data-server wire frame.** Duplicates information
  already in `LOAD` + `PROJECT`. Toron integration burden
  with no new information conveyed.
- **Add a node capability bit
  (`NodeCapability::ColumnFetch`).** Mixed clusters serve
  column-fetched and whole-record-fetched results
  identically; no negotiation is needed.
- **Bump envelope `format_version`.** No on-disk shape
  change; nothing to signal.
- **Add a new reject-code family.** Existing AEAD-verify
  and Arrow-decode error paths cover all failure modes.
- **Land mini-page row pruning in V1.** Multi-quarter
  design surface; better as its own ADR (likely after
  Brainstorm 011 V2's page directories).
- **Pre-fetch metadata for all records in a slab into a
  single bulk GET.** Plausible optimization but adds a
  separate code path and unbounded buffering pressure for
  large slabs. Coalescing contiguous metadata reads gets
  most of the benefit without the unbounded buffer.
- **Cache `RecordBufferDirectory` across queries.** Adds a
  cache surface, eviction policy, and consistency story
  (records can be retired by retention sweep mid-cache).
  V1 does not cache; V2 may, when fleet metrics
  show repeat-record reads on cold tier.
- **Sparse IPC body with dummy null arrays for unfetched
  user columns.** Rejected because it creates a second
  interpretation of column presence (`batch.column(i)`
  returns nulls) parallel to the
  `ordinal_mapping`-driven first interpretation.
  Downstream callers who skip the mapping silently
  produce wrong results — a `MIN(col_x)` aggregator
  reading an all-null array yields `NULL` instead of
  flagging the column as unavailable. The
  `ProjectedRecordBatch` shape forbids dummy nulls; an
  architectural unit test rejects them.
- **Reuse Arrow's "file format" with random-access footer
  pointer instead of "stream format."** ADR 042 picked
  stream format for streaming write + recovery
  reconstruction. Switching is a bigger change than
  ADR 044 needs.
- **Per-column compression-policy descriptor field.**
  Arrow IPC's `BodyCompression` is per-buffer, but ADR 043
  §1 deferred per-column policy to LCE V2 explicitly. ADR
  044 is fetch-side only and does not touch policy.
- **Project-name-prefixed type names** (per
  `plans/conventions/no-project-name-in-protocols.md`).
- **`V1` / `V2` suffixes on new types** (per
  `plans/conventions/no-version-suffixes.md`).
- **Reserve enum slots for future fetch policies** (per
  `plans/conventions/no-premature-backward-compat.md`).

## Open questions

These are explicit decision points tracked for downstream
resolution — none are contract-level, so none block this
ADR. Resolution lands in Phase-11 benches, in
implementer-discovery during a specific phase, in
council/policy follow-up, or in a follow-on ADR.

1. **Whether `min_record_bytes` and
   `max_selected_fraction_micros` defaults are
   provider-aware.** S3, R2, GCS, B2, RustFS, LocalFs
   have different RTT and per-GET cost characteristics.
   V1 picks one cluster-wide default tuple
   (256 KiB / 0.67). A follow-up may need per-provider
   defaults if Phase-11 metrics show one dominant
   regime.
2. **Initial Arrow-layout coverage.** Should the V1
   partial materializer support every descriptor-supported
   Arrow type, or is `unsupported_arrow_layout` fallback
   acceptable for nested / dictionary / union columns in
   V1? Default V1 admits the fallback; council may revisit
   if Phase 4 / Phase 11 evidence argues for a stricter
   "support everything before merge" bar.
3. **Range-fraction threshold.** Default
   `max_selected_fraction_micros` of 670_000 (0.67) is
   middle-of-the-road. 500_000 (0.50) is more aggressive
   (more fallback). 750_000 (0.75) is more permissive
   (more columnar fetch even on wider projections).
   Bench-tunable.
4. **AGG short-circuit interaction.** When the CSB short-
   circuit fires for a slab, no record-body fetch happens
   at all. The column-fetch planner is downstream of CSB;
   no interaction in V1. Should be confirmed in
   benchmarks.
5. **Two-phase batching across records in the same slab.**
   Phase A for N admitted records can be one GET if their
   metadata-window bytes are contiguous on disk. Worth
   batching in this ADR, or defer to a follow-on ADR once
   round-trip-count metrics are in?
6. **Whether the column fetch plan should expose its
   chunk-run set to a future Lambda/serverless pushdown
   (Brainstorm 010).** Default keeps it storage-internal;
   the Brainstorm 010 ADR (when opened) may design the
   public surface then, or sooner if its scope arrives on
   near-term radar.
7. **Synthetic-column buffers in projection union.** When a
   PBC program touches `__row_hlc__` (visibility filter),
   that buffer joins the projected set. The column-to-
   buffers map needs to enumerate synthetic columns —
   confirm this is stable through ADR 042's Phase 8
   identity columns refactor.
8. **Access-pattern leakage threat-model statement.** ADR
   044 makes chunk-level access pattern more column-
   correlated than ADR 022 / 043 left it. Does this need
   a stronger threat-model note than ADR 022's existing
   range-read leakage acceptance, or is the
   "Consequences" entry sufficient?
9. **Whether the Arrow `StreamReaderBuilder::with_projection`
   API works with sparse backing bytes.** Phase 4 of the
   impl plan flags this as load-bearing-but-unverified:
   if Arrow's reader walks every buffer offset even when
   a projection is set, the implementation needs to
   bypass `StreamReader` and build arrays directly from
   selected buffers using `RecordBufferDirectory`.
   Resolution by Phase-4 path-discovery test.

## Forward-compatibility analysis

**Brainstorm 011 V2 (homerolled SIMD encodings).** ADR 044's
fetch planner consumes per-buffer Arrow IPC offsets; LCE V2's
homerolled encodings still expose buffers (or page-directories
that decompose to buffers). The planner surface stays.

**Brainstorm 011 V2 sub-piece (per-buffer page directory).**
Refines what "fetch a buffer" means inside a chunk. The
ADR 044 plan stays at chunk granularity; the page-directory
ADR refines decode-time. Composition is clean.

**Brainstorm 011 V3 (compute-over-compressed-data).** PBC
opcodes execute over encoded bytes directly; ADR 044's
projection set is the same; the decode call swaps for an
encoded-evaluation call. ADR 044 does not foreclose.

**Brainstorm 010 (serverless PBC L2 / node-pushdown).** A
Lambda function evaluating PBC at L2 needs the same
chunk-aligned authenticated range reads ADR 044 uses —
plus the per-slab DEK delivered to the Lambda environment
under ADR 020's key-custody story. ADR 044's fetch plan
helper is the natural seed.

**ADR 045 (PK-less append-only tables).** Append-only
tables almost always project few columns over wide
schemas. They are the workload column-fetch helps most.
ADR 044's planner runs unchanged on PK-less tables; the
synthetic-column ordinals just shift (ordinal → content
hash + monotonic ordinal per ADR 045's design).

**ADR 046 (secondary indexes).** Secondary index lookups
land on a `(slab_id, record_location, record_row_index)`
triple per ADR 046. ADR 044 handles the slab + record
lookup; row_index projection composes post-decode against
the projected batch.

**Future LSM compaction.** Compacted slabs are byte-
identical in shape to today's slabs. ADR 044's fetch
planner runs unchanged.

**Future ZSTD capacity-tier codec.** A new
`RecordBodyCompressionPolicy::Zstd` variant changes the
decompression call but not the buffer offsets.
ADR 044's planner is codec-agnostic.

**Wire-frame compression (separate transport ADR).**
Orthogonal; ADR 044 is storage-resident-read-side only.

**Logical / physical descriptor fingerprint split.** ADR
044 adds *no* descriptor fields at all, so the
fingerprint surface is unchanged. If the split ever
lands for other reasons, ADR 044 does not pre-empt it.

## Deferred to follow-up ADRs

1. **Mini-page row-group granularity for highly selective
   predicates.** Buffer-level fetch leaves "filter to 1%
   of rows still pulls 100% of buffer bytes" headroom.
2. **Per-buffer page directory for O(1) point-read
   decompression.** Brainstorm 011 V2 sub-piece; may land
   independently if point-read amplification metrics show
   real cost.
3. **`RecordBufferDirectory` cache.** Adds a cache surface
   that V1 does not need. Pulled when measured repeat-
   record cold-tier reads justify the eviction-policy
   complexity.
4. **Bulk-slab metadata pre-fetch.** Phase A across all
   admitted records in one GET. Pulled when metadata-
   round-trip count dominates cold-tier latency.
5. **Lambda / serverless PBC L2 pushdown.** Brainstorm 010.
   Inherits ADR 044's plan helper.
6. **Secondary-index integration.** ADR 046 calls into
   ADR 044 for the buffer-level fetch step.

## References

- ADR 020: L2 envelope encryption + chunked AEAD —
  authentication invariants, chunk-aligned range reads,
  `plan_tiered_range_read` primitive.
- ADR 022: L2 slab format, footer-indexed range reads —
  unencrypted footer + plaintext-byte-offset addressing.
- ADR 035: vectorized PBC executor — terminal `PROJECT`,
  filter+project pipeline.
- ADR 036: PBC pushdown contract — Toron-visible program
  surface unchanged.
- ADR 037: tenant table schemas — descriptor surface
  unchanged; ADR 044 adds no descriptor field.
- ADR 038: column-aware locality V1 — slab admission
  unchanged; column-fetch happens after admission.
- ADR 039: slab-footer column statistics — projection-ratio
  estimates can use stats; CSB short-circuit unchanged.
- ADR 040: aggregate pushdown — AGG accumulators consume
  only declared input columns; column fetch passes through.
- ADR 041: project-name decoupling — names follow brand-
  clean discipline.
- ADR 042: analytical multi-row slab fragments —
  `record_row_index` projection composes; synthetic columns
  enter projection union only when referenced.
- ADR 043: Arrow IPC body compression (LZ4) — per-buffer
  LZ4 is the load-bearing substrate; mixed-shape slab
  reads via IPC FlatBuffer authority.
- Brainstorm 010 — serverless PBC L2 (node-pushdown) — uses
  the same chunk-aligned authenticated range-read primitive.
- Brainstorm 011 — lightweight columnar encodings — V2/V3
  refines decode but not fetch planning.
- `plans/conventions/newtype-units.md` —
  `IpcBufferOffset`, `IpcBufferLength`,
  `EncryptedChunkIndex`, `EncryptedChunkCount`,
  `RecordBodyByteOffset`, `RecordBodyByteLen`,
  `IpcBufferIndex`, `ColumnProjection`.
- `plans/conventions/no-version-suffixes.md` — types are
  `RecordBufferDirectory`, `TieredMultiRangeReadPlan`,
  `ProjectedRecordBatch`, `L2ColumnFetchConfig`,
  `L2FetchStrategy`, no V1.
- `plans/conventions/no-project-name-in-protocols.md` —
  type names and metric segments stay brand-clean.
- `plans/conventions/no-premature-backward-compat.md` — no
  descriptor field; no reserved fallback-reason variants
  beyond the V1 set; no envelope version bump.
