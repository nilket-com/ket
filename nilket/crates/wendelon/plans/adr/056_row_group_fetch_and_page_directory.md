# ADR 056: Row-Group Fetch and Layered Page Directory

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/056_row_group_fetch_and_page_directory.md`

## Status

Draft — staged for council review. Layered hierarchy
framing, V1 substrate shape, directory shapes,
metadata key, strategy variant, and reject taxonomy
are council-converged across the three drafts in
`plans/wip/`.

The substrate evolution is committed as a layered
hierarchy mirroring Parquet / Vortex convergent
design:

1. **ADR 056 V1 (this ADR)** — Arrow IPC RecordBatch
   row groups + per-row-group min / max / null-count
   stats. Outer hierarchy layer; macro pruning for
   analytical scans.
2. **ADR 057 encoding cascades (future ADR)** — per-buffer pages +
   encoding cascades **inside each row group
   RecordBatch's encoded buffers**. Inner hierarchy
   layer; fine random access, per-column encoding
   choice (Vortex-style).
3. **COCD (future ADR)** — compute over encoded
   representation (COCD).

Row groups ship first as a construction-order choice,
not a priority ranking — the layers are equal
priority. Row groups establish the outer contract
(row partitioning, stats, fetch planner, secondary-
index mapping, LSM rebuild rules, metric and failure
taxonomies) under which per-buffer pages can safely
nest. Per-buffer pages ship next because they have a
stable place to attach inside each row-group buffer.

The council process: iters 1–5 oscillated between
shape choices (per-buffer mini-pages vs. RecordBatch
row groups) before recognizing both as layers of one
hierarchy rather than alternatives. Resolved 2026-05-05
by user direction. Iters 6–7 land the final converged
text.

## Where we are

ADR 060 sparse projection fetch narrowing is now
implemented and promoted to shipped. Its Phase 7
bench artifact at
`plans/impl/060_sparse_projection_fetch_narrowing_bench_results.md`
closes the encoded-fetch sparse follow-up on top of
ADR 056's row-group planner: exact-COCD survival
masks can narrow projection fetches to selected ADR
057 pages for clustered BitPacked / Dictionary
fixtures, while preserving ADR 056's public
`L2FetchStrategy::RowGroup` shape and fallback full-
row-group projection path.

ADR 060's gate revision records that ADR 056 / 044
chunk-aligned authenticated range reads bound
ciphertext-byte reduction in V1. ADR 060 therefore
promotes on selected plaintext projection-byte
reduction plus wall-clock improvement, with
ciphertext ratios recorded as evidence for a future
page-aligned authenticated chunk-layout ADR.

ADR 062 row-group sizing profile is now implemented and
promoted as an ADR 056 amendment. `RowGroupSizingProfile`
is a closed two-variant catalog: `PointRead` preserves
the ADR 056 V1 defaults byte-identically, while `Scan`
pins `target_row_group_rows = 65_536`,
`max_row_group_rows = 131_072`,
`min_paged_record_rows = 65_536`, and
`min_paged_record_bytes = 1 MiB` for warehouse scan
deployments. The amendment does not change the page
directory substrate, fetch planner, reject taxonomy,
descriptor / wire / capability surfaces, or `ObjectStore`
trait. Bench evidence is recorded at
`plans/impl/062_row_group_sizing_profile_bench_results.md`.

V1 substrate commitments (council-converged):

- substrate-only scope: per-record `RecordPageDirectory`
  carried in IPC schema custom metadata, plus a
  three-phase fetch planner that consumes it for
  row-group-grain selectivity;
- **row groups are Arrow IPC RecordBatches** — one slab
  record body becomes one IPC stream containing one
  schema message followed by N RecordBatch messages,
  each a dense row-group batch over the same physical
  schema (user columns + ADR 042 / 045 storage-only
  synthetic columns);
- **per-buffer-per-row-group page directory** records,
  for each `(row_group_index, ipc_column_ordinal,
  ipc_buffer_ordinal)` triple, the byte range within
  the record body — combined with per-row-group
  metadata range and body range, this is the
  addressing substrate for selective fetch and point
  reads;
- **row-group stats in V1**: directory carries
  per-row-group min / max / null-count for ADR
  039-supported column types; planner uses them in
  Phase B to skip provably-disjoint row groups before
  any output fetch;
- ADR 043's per-buffer LZ4_FRAME compression is
  unchanged — each row-group RecordBatch's buffers are
  ordinary LZ4_FRAME-compressed at the row-group's own
  buffer grain (no intra-buffer block layout, no LZ4
  frame flag change, no codec catalog extension, no
  custom partial-buffer materializer);
- writer side gated by node-local
  `RowGroupWriterConfig`; default off in initial
  rollout, flipped on after Phase 10 bench acceptance;
- reader side automatic on directory presence —
  pre-056 sealed records (single-RecordBatch IPC stream,
  no custom_metadata key) read transparently via ADR
  044 column fetch;
- fetch planner extends ADR 044's `L2FetchStrategy` to
  a four-leg dispatch:
  `RowGroup(RowGroupFetchPlan) | Columnar(plan) |
  WholeRecord | Fallback(reason)`;
- conservative PBC predicate classifier for stats-driven
  pruning: equality, range comparisons, IS NULL,
  IS NOT NULL; conjunction (AND) skips when **any**
  branch proves disjoint — unsupported sibling branches
  do not block the skip; disjunction (OR) skips only
  when **every** branch proves disjoint, so any
  unsupported branch keeps the group;
- closed 3-code `RowGroupRejectCode` taxonomy:
  `PageDirectoryParseError`,
  `RowGroupRangeOverflow`, `LayoutUnsupported`;
- bounded `storage_l2_row_group_fetch_*` metric family
  with `{strategy, reason, decision, kind,
  violation_kind}` enumerations; pinned by
  architectural test;
- pinned V1 defaults: `target_row_group_rows = 4_096`,
  `max_row_group_rows = 8_192`,
  `min_paged_record_rows = 8_192`,
  `min_paged_record_bytes = 512 KiB`,
  `max_selected_row_group_fraction_micros = 250_000`
  (0.25), `max_selected_byte_fraction_micros = 500_000`
  (0.50), `max_ranges_per_record = 16`,
  `max_parallel_range_gets = 4`;
- no new wire `MsgType`, no new `FRAME_FLAG_*`, no new
  `NodeCapability` bit, no Cargo feature, no new digest
  domain, no `ObjectStore` trait change, no envelope
  `format_version` bump, no ADR 022 footer extension;
- no descriptor surface change; no synthetic Arrow
  column for the directory.

## Context

Roadmap #14 sub-bundle. ADR 044's column-projected fetch
landed buffer-grain selectivity on the column axis. The
remaining axis is row selectivity within a record body
— a predicate matching 1% of rows in a 65,536-row
multi-row record still fetches and decompresses 100%
of the projected buffer bytes.

Two pressures make this the next ADR:

1. **Toron compute-layer pivot.** Toron's analytical
   workloads against multi-row tables are large records,
   narrow predicates, modest projection ratios.
   Settling row-grain selectivity now means Toron's
   fetch surface is stable on day one. Toron's plans
   also frequently include point-shaped sub-queries
   (joins on PK, secondary-index lookups, top-K
   re-fetch) — row-group fetch turns those into one-
   row-group decode.

2. **ADR 052 secondary-index lookups.** Index hits
   return `(SlabId, RecordLocation, RecordRowIndex)`
   triples; without row-group fetch, every hit pays
   whole-record decompression cost.

ADR 044's deferred items name both pieces; ADR 056
lands them as one substrate ADR because the directory
IS the addressing index and the fetch shape IS its
consumer.

The infrastructure is already in place:

- ADR 020 chunked AEAD covers any sub-record byte
  range.
- ADR 022 unencrypted footer + plaintext-byte-offset
  index addresses each record body without DEK access.
- ADR 042 multi-row record bodies are Arrow IPC
  streams, natively able to carry multiple
  RecordBatch messages under one schema.
- ADR 043 per-buffer LZ4_FRAME compression is
  unchanged; row-group RecordBatches inherit it at
  per-batch buffer grain.
- ADR 044 two-phase column fetch is the planner
  surface; ADR 056 extends it to three phases and the
  strategy enum to four legs.
- ADR 052 secondary-index sidecars are the upstream
  consumer for point reads.
- ADR 055 LSM compaction outputs go through the
  normal slab writer; row-group emission rides that
  path unchanged.

The constraints from ADR 003 (bifurcated writes), ADR
016 (control ring), ADR 020 (AEAD), ADR 022 (footer),
ADR 029 (Identity vs Locality), ADR 030 (deployment
profiles), ADR 035 / 036 (PBC contract), ADR 037
(descriptors), ADR 039 (CSB), ADR 042 (multi-row), ADR
043 (per-buffer LZ4), ADR 044 (column fetch), ADR 052
(secondary indexes), ADR 053 (wire compression), ADR
054 (CDC substrate), ADR 055 (LSM substrate) all hold.

## Architectural inextricability

Two design choices are physically inseparable here:

**1. Page directory + row-group fetch are one
substrate.** Splitting them lands an unused index or an
unimplementable plan. The directory IS the addressing
substrate; the fetch shape IS its consumer.

**2. Page directory lives in IPC custom metadata, not
the ADR 022 footer and not as a synthetic column.**
Two alternatives are physically rejected together (each
for distinct reasons):

- **Footer extension**: two sources of truth (footer
  index vs. IPC metadata) parallel to ADR 044 §3's
  rejected column-buffer-offset index; HMAC-scope
  growth; pre-056 migration story; cleartext workload-
  shape leakage to L2 provider (column count, row-
  group counts, buffer ratios).
- **Synthetic `__row_group_directory__` column**:
  directory has one logical value per record body, not
  per row. Forcing it into a column creates row-count
  mismatch (one-row metadata column on an N-row
  RecordBatch) or metadata repetition; pollutes
  column ordinal space that PBC / AGG / CSB consumers
  walk.

The IPC FlatBuffer's `Schema.custom_metadata` is the
existing slot for Arrow-adjacent metadata, encrypted
inside the record body, authenticated by ADR 020 chunk
AEAD. It is the only correct home.

## Identity-vs-Locality framing

Per `plans/philosophy.md`, every record has an Identity
and a Locality. Row-group fetch is **identity-axis**:

- The fetch unit is `(SlabId, RecordLocation,
  RowGroupIndex)` or `(SlabId, RecordLocation,
  RecordRowIndex)` — identity-bound coordinates.
- The `RecordPageDirectory` addresses byte ranges
  within a record body that is itself addressed by ADR
  022's plaintext byte offset (locality-axis).
- The Unified Ring rule applies: there is one storage-
  read surface; "fetch-plane" / "decode-plane" are
  forbidden framings.
- Readers (PBC executor, AGG, secondary-index lookup
  paths) consume dense `ProjectedRecordBatch` values
  regardless of fetch shape. Identity-axis output is
  invariant; locality-axis fetch shape is internal
  optimization.
- Cross-locality compaction does not apply — this is
  a per-record substrate, downstream of admission,
  upstream of ADR 055's per-locality compactor.

## Why row groups as RecordBatches, not intra-buffer mini-pages

The council oscillated four iterations on this. The
resolution:

**Stock Arrow decode.** Each row-group RecordBatch
decodes via the existing Arrow IPC reader. No custom
partial-buffer materializer; no offsets-buffer rebasing
for UTF-8; no bit-alignment for validity bitmaps; no
decompressed-length validation pass. Variable-width
columns "just work" because each RecordBatch is dense
and complete.

**No LZ4 frame change.** Each row-group's buffers are
ordinary ADR 043 per-buffer LZ4_FRAME at the
RecordBatch's own buffer grain. The codec catalog stays
unchanged; no LZ4 frame block-independence flag, no
new dependency.

**Row-group stats are natural.** Computing min / max /
null-count per RecordBatch at write time is cheap and
fits the stats per `(row_group_index, ipc_column_ordinal)`
shape directly. With intra-buffer mini-pages, stats
would have to live at sub-buffer grain — finer than
the predicate evaluation grain and harder to reason
about.

**ADR 057 encoding cascades layers cleanly on top.** Brainstorm 011 §3's
intra-buffer "Lance idea" SIMD pages are a refinement
INSIDE each row-group RecordBatch's buffers. ADR 056
V1 ships row-group RecordBatches; ADR 057 encoding cascades adds intra-
buffer SIMD pages with their own page directory inside
the encoded buffers. The two compose hierarchically:
record → row-group RecordBatch → buffer → intra-buffer
SIMD page.

**The roadmap's "per-buffer page directory" is
satisfied.** Directory entries are keyed on
`(row_group_index, ipc_column_ordinal, ipc_buffer_ordinal)`
— one entry per buffer per row group. The "per-buffer"
axis lives in the directory shape, not in the
sub-buffer slicing.

**Whole-record decode trivial.** A pre-056 reader of a
post-056 record sees one IPC stream with one schema
followed by RecordBatch messages — exactly what Arrow
IPC supports. Stream concatenation gives the whole-
record view for free.

**ADR 044 substrate continuity.** ADR 044 established
`RecordBufferDirectory` as the cold-read addressing
surface. ADR 056 extends it with optional
`page_directory: Option<RecordPageDirectory>`; the
dispatcher gains one new leg. Whether the
`RecordPageDirectory` records intra-buffer or
inter-RecordBatch ranges is internal; the planner /
executor surfaces are identical.

**Tradeoff acknowledged.** Row-group RecordBatches
require the writer to split a multi-row record body
into multiple RecordBatch messages — a moderate writer-
side change. Intra-buffer mini-pages would have been a
smaller writer change but a larger reader change
(custom materializer). The reader-side simplicity wins.

## V1 scope

ADR 056 ships **only** the row-group + page-directory
substrate:

1. Writer-side multi-RecordBatch IPC emission for
   `MultiRow` records above paging thresholds.
2. `RecordPageDirectory` carried in IPC
   `Schema.custom_metadata["storage.row_group_directory"]`
   with row-group entries, per-buffer page entries,
   and per-row-group column stats for ADR 039-supported
   types.
3. ADR 044 Phase A parser extension that reads the
   directory.
4. Phase B row-group admission: stats-driven skip,
   then predicate-column fetch + filter when stats are
   insufficient.
5. Phase C projected page fetch over surviving
   row-group set.
6. `L2FetchStrategy::RowGroup(RowGroupFetchPlan)`
   variant; planner ladder
   row-group → column-buffer (ADR 044) → whole-record.
7. ADR 052 secondary-index `fetch_indexed_rows`
   adapter that maps index hits to row-group point
   reads.
8. Bounded `storage_l2_row_group_fetch_*` metric
   family.
9. Architectural lockdown.

It deliberately does **not** ship:

- intra-buffer SIMD mini-pages or sub-row-group
  page directories (ADR 057 encoding cascades territory);
- variable per-buffer mini-page row count (ADR 057 encoding cascades);
- byte-budgeted (vs. row-aligned) row groups (encoding cascades
  V2);
- homerolled SIMD encodings (ADR 057 encoding cascades / V3);
- compute-over-compressed-data (COCD);
- a `RecordPageDirectory` cache across queries;
- bulk pre-fetch of all directories in a slab in one
  GET;
- footer extension or new on-disk envelope shape;
- descriptor field for paging or stats policy;
- per-table opt-in / opt-out;
- row-group fetch over wire-tier transport (ADR 053
  unchanged);
- public Toron-visible PBC opcode addition;
- partial-slab rehydration;
- compaction-time elision of stats (compaction
  recomputes; never copies input directories);
- LZ4 frame flag change.

## Non-goals (V1)

- **No descriptor field.** Row-group fetch is a
  node-local read/write strategy; identity-axis output
  is invariant. No descriptor-fingerprint churn.
- **No new envelope `format_version`.** On-disk shape
  is unchanged outside `Schema.custom_metadata`, an
  existing Arrow flatbuffer slot.
- **No ADR 022 footer extension.** Page directory
  lives in IPC custom metadata.
- **No `SlabRecordHeader` change.**
- **No new node capability bit.** Mixed-fleet rollout
  is byte-identical on logical results.
- **No new public HRFT frame family** and no new
  reject-code family on the wire.
- **No new wire-protocol surface to Toron.** ADR 036
  pushdown contract unchanged.
- **No `ObjectStore` trait change.**
- **No partial-slab rehydration.**
- **No new compression codec or LZ4 frame flag
  change.** ADR 043 catalog and frame shape
  unchanged; row-group RecordBatch boundaries
  provide independence.
- **No new cipher / nonce / AAD shape.** ADR 020
  byte-identical.
- **No new digest domain.** Visible-state digest
  (ADR 046) and per-version-entry digest (ADR 052)
  invariant under row-group boundaries.
- **No PBC opcode addition.** Predicate evaluation in
  Phase B uses existing PBC opcodes against fetched
  row-group batches.
- **No intra-buffer mini-pages or sub-row-group page
  directory.** ADR 057 encoding cascades territory.
- **No synthetic Arrow column for the directory.**
- **No sparse Arrow arrays with nulls for unfetched
  row groups.**
- **No CDC interaction.** CDC catchup reads full
  record bodies (ADR 054 §6); row-group directory
  is invisible.
- **No reserved enum slots, compatibility shims, or
  `V1` / `V2` Rust type suffixes.**
- **No project-name prefix on new symbols.**
- **No new Cargo feature or dependency.**
- **No SQL / DDL surface.**
- **No bulk slab-level metadata pre-fetch.** Same
  deferral as ADR 044.
- **No `RecordPageDirectory` cache across queries.**
  Same deferral as ADR 044.

## Decision

### 1. Row groups are Arrow IPC RecordBatches

For `RecordLayoutPolicy::MultiRow`, the storage writer
emits one Arrow IPC stream per record body containing:

```text
schema message
record batch message for row group 0
record batch message for row group 1
...
record batch message for row group N-1
```

Each row-group RecordBatch is a dense Arrow batch over
the same physical schema (user columns + ADR 042 / 045
storage-only synthetic columns). A full-record reader
decodes the stream as ordinary Arrow IPC and
concatenates the batches. A row-group-aware reader
fetches and decodes only selected RecordBatch
messages.

ADR 043 per-buffer LZ4_FRAME compression remains
ordinary Arrow IPC body compression at each row
group's buffer grain. There is no partial-buffer
reconstruction, no LZ4 frame flag change, and no codec
catalog extension.

V1 writer policy:

```text
target_row_group_rows  = 4_096
max_row_group_rows     = 8_192
min_paged_record_rows  = 8_192
min_paged_record_bytes = 512 KiB
```

A record is paged only when it meets **both**
thresholds — `row_count >= min_paged_record_rows`
**AND** estimated body bytes
`>= min_paged_record_bytes`. Records that fall below
either threshold use the existing single-batch
`MultiRow` encoding (ADR 042 / 043 path) — no
directory emitted. Single-row records are unchanged.

Storage-only synthetic columns (`__row_hlc__`,
`__row_key_id__`, `__row_routing_locality__`,
`__row_tombstone__` when present) appear in every
row-group batch and are stripped before user-facing
output, exactly per ADR 042 §6.

### 2. Page directory is metadata, not a column

Paged records carry a storage-private
`RecordPageDirectory` in the IPC schema message's
`custom_metadata` under the literal key
`storage.row_group_directory`. The directory is inside
the record body, encrypted and authenticated by the
same ADR 020 chunk AEAD as the rest of the IPC
stream. It is not added to the ADR 022 footer and is
not a physical Arrow column.

```rust
pub struct RecordPageDirectory {
    pub record_id: TieredRecordId,
    pub row_count: RowCount,
    pub row_groups: Vec<RowGroupDirectoryEntry>,
    pub buffer_pages: Vec<BufferPageDirectoryEntry>,
    pub row_group_stats: Vec<RowGroupColumnStats>,
    pub logical_schema_fingerprint: LogicalSchemaFingerprint,
}

pub struct RowGroupDirectoryEntry {
    pub row_group_index: RowGroupIndex,
    pub first_record_row_index: RecordRowIndex,
    pub row_count: RowGroupRowCount,
    pub metadata_range: RecordBodyByteRange,
    pub body_range: RecordBodyByteRange,
}

pub struct BufferPageDirectoryEntry {
    pub row_group_index: RowGroupIndex,
    pub ipc_column_ordinal: IpcColumnOrdinal,
    pub ipc_buffer_ordinal: IpcBufferIndex,
    pub range: RecordBodyByteRange,
}

pub struct RowGroupColumnStats {
    pub row_group_index: RowGroupIndex,
    pub ipc_column_ordinal: IpcColumnOrdinal,
    pub null_count: RowCount,
    pub min: Option<CanonicalScalar>,
    pub max: Option<CanonicalScalar>,
}

pub struct RecordBodyByteRange {
    pub start: RecordBodyByteOffset,
    pub len: RecordBodyByteLen,
}
```

The directory is the row-group analogue of ADR 044's
`RecordBufferDirectory`: an authenticated seek
accelerator and row-group admission aid. The Arrow IPC
FlatBuffer remains authoritative for each batch's
internal buffer layout; the directory addresses where
each RecordBatch's metadata and per-buffer bytes live
within the record body, plus per-row-group stats.

The "per-buffer page directory" wording from the
roadmap is satisfied by `BufferPageDirectoryEntry` —
one entry per `(row_group_index, ipc_buffer_ordinal)`
pair. The per-buffer axis lives in the directory shape;
intra-buffer slicing is ADR 057 encoding cascades territory.

**Layout invariants enforced at parse time:**

- row-group spans cover `0..row_count` exactly, with
  no gaps or overlap;
- `metadata_range` and `body_range` are inside the
  record body;
- buffer page ranges are inside their row group's
  `body_range`;
- buffer page entries are sorted and non-overlapping
  for the same `(row_group_index, ipc_buffer_ordinal)`;
- every `ipc_column_ordinal` and `ipc_buffer_ordinal`
  exists in the IPC schema;
- `logical_schema_fingerprint` matches the descriptor's
  current logical fingerprint (ADR 051);
- row-group stats are present only for ADR
  039-supported column types; absent (not opaque) for
  unsupported types.

A directory failing any invariant fails as
`PageDirectoryParseError` or `RowGroupRangeOverflow`
— **never** silent fallback to whole-record fetch
(ADR 044 §7 discipline).

### 3. Row-group stats inside the directory

Row-group stats carry per-row-group min / max /
null-count for columns whose types are supported by
ADR 039's canonical statistics rules (numeric,
fixed-width binary, UTF-8 with bounded width, boolean
— final supported set finalized in impl Phase 0 audit
against ADR 039).

Stats are encrypted with the record body and consulted
*after* slab-level CSB pruning (ADR 039) has admitted
the slab. CSB remains the slab-level skip surface;
row-group stats are an in-record fetch-planning
surface within an admitted record.

The pruning rule is **fail-closed**:

- if a row group's stats prove the predicate cannot
  match, skip the group;
- if stats are absent, unsupported, truncated, or
  insufficient for the predicate, the group cannot
  be skipped by stats — the planner either runs Phase
  B predicate fetch when early narrowing is
  economical, or carries the group into Phase C with
  predicate columns included;
- row-group stats never answer user-visible aggregates
  directly (an AGG that wanted to fold from stats
  would go through ADR 040 / ADR 039's slab-level
  CSB short-circuit, not through this surface).

Stats inclusion is what makes Phase B PBC-driven
pruning work; without them, every Phase B reduces to
"fetch predicate columns and evaluate full filter".
Once row groups are RecordBatches, computing stats per
batch at write time is cheap; the V1 cost-benefit is
clearly in their favor.

### 4. Three-phase fetch

ADR 044's two-phase plan extends to three phases when
predicates and selectivity warrant:

```text
Phase A: metadata + directory probe
  - fetch the IPC metadata window (ADR 044 mechanism);
  - parse the schema, RecordBatch headers, and
    RecordPageDirectory if present.
  - Output: RecordBufferDirectory (with optional
    page_directory).

Phase B: row-group admission
  - if page directory absent: jump to Phase C with all
    row groups (single-batch fallback, equivalent to
    ADR 044 column fetch);
  - else: classify each row group via stats —
      Skip:                predicate proven disjoint;
      Keep:                stats prove the group may
                           match and no early predicate
                           fetch is needed;
      NeedsPredicateFetch: stats absent / unsupported
                           / insufficient, AND fetching
                           predicate columns early is
                           economical;
  - if any NeedsPredicateFetch: fetch predicate-column
    pages for those row groups (ColumnFetchRequirement
    predicate columns + storage-only requirements);
    decode dense row-group batches; run PBC filter to
    derive surviving row mask;
  - output: surviving row-group set + per-group
    survival masks for predicate-fetched groups +
    decoded predicate batches retained for the Phase C
    zip step (the executor MUST hold these between
    phases — Phase 5 zips Phase B columns with Phase C
    columns rather than re-fetching or re-filtering).

Phase C: projected page fetch
  - for surviving row groups, fetch buffer pages for
    output / aggregate / storage-only columns
    referenced by the PBC program but not already
    fetched in Phase B;
  - decode dense row-group RecordBatches;
  - for each predicate-fetched group, zip Phase B
    columns + Phase C columns by IPC column ordinal,
    apply the per-group survival mask, emit a dense
    filtered batch — PBC is NOT re-evaluated here;
  - for stats-only Keep groups, emit Phase C's dense
    batch directly;
  - hand to PBC / AGG executor as a stream of dense
    ProjectedRecordBatch values, or concatenate when
    the caller wants a single batch.
```

The planner can bypass row-group mode before Phase B
with the cheap-bypass discipline ADR 044 §6
established:

```text
For each admitted record:
  if not config.enabled:                use whole-record (Disabled).
  if descriptor.is_single_row():        use whole-record (SingleRow).
  if required.is_full(schema):          use whole-record (FullProjection).
  if record.length < min_paged_record_bytes:
                                        use whole-record (SmallRecord).
  if record.row_count < min_paged_record_rows:
                                        use whole-record (TooFewRows).
  if rehydration_active(record):        use whole-record (RehydrationAvailable).

  // Phase A — fetch + decrypt metadata window.
  directory = parse_record_buffer_directory(...)
  if directory == MetadataProbeCap:     use whole-record (MetadataProbeCap).
  if directory.layout_unsupported:      use whole-record (UnsupportedArrowLayout).
  if directory.page_directory.is_none():
                                        use ADR 044 (UnpagedRecord).

  // Phase B — stats-driven prune, then predicate fetch if needed.
  pd = directory.page_directory.unwrap()
  classified = classify_row_groups(pd, pbc_predicate)
  if classified.all_skipped():          empty-result row-group plan (NoSurvivors).
  surviving = if classified.needs_predicate_fetch {
      execute_phase_b(classified)
  } else { classified.kept }

  // Phase C — projected fetch over surviving groups.
  plan = build_phase_c_plan(pd, surviving, required)
  if plan.selected_row_group_fraction
       > config.max_selected_row_group_fraction_micros / 1_000_000:
                                        use ADR 044 (TooManyRowGroups).
  if plan.selected_byte_fraction
       > config.max_selected_byte_fraction_micros / 1_000_000:
                                        use ADR 044 (TooManyBytes).
  if plan.chunk_runs.len() > config.max_ranges_per_record:
                                        use ADR 044 (TooManyRanges).

  use the row-group plan.
```

`NoSurvivors` is a successful row-group decision
(empty-result plan with no Phase C fetch), not a
fallback to whole-record fetch.

The strategy enum extends to four legs:

```rust
pub enum L2FetchStrategy {
    RowGroup(RowGroupFetchPlan),         // ADR 056
    Columnar(ColumnarRecordFetchPlan),   // ADR 044
    WholeRecord,
    Fallback(RowGroupFallbackReason),
}

pub enum RowGroupFallbackReason {
    Disabled,
    SingleRow,
    FullProjection,
    SmallRecord,
    TooFewRows,
    TooManyRowGroups,
    TooManyBytes,
    TooManyRanges,
    MetadataProbeCap,
    UnsupportedArrowLayout,
    RehydrationAvailable,
    UnpagedRecord,
}
```

`pick_l2_fetch_strategy` returns one of the four
`L2FetchStrategy` variants. Authentication / parse
failures are NOT variants — they route to the existing
storage-error path per §6.

The ADR 044 architectural test forbidding `_`
catch-all match arms extends to the new variant.

### 5. Point reads and secondary indexes use the same directory

For a point read with `RecordLocation.record_row_index`:

- map `record_row_index` to a `(row_group_index,
  row_within_group)` pair using
  `RowGroupDirectoryEntry.first_record_row_index +
  row_count` spans;
- fetch that row group's user columns plus storage
  columns required by the projection;
- decode the dense row-group RecordBatch;
- take the one row at `row_within_group`;
- strip storage-only columns before returning.

Without a page directory, point reads use the existing
ADR 044 column-fetch path (`UnpagedRecord` fallback).

ADR 052 secondary-index hits keep the same
`(SlabId, RecordLocation, RecordRowIndex)` identity.
ADR 056 only accelerates the fetch path behind those
hits via the `fetch_indexed_rows` adapter (§7).

### 6. Authentication failures never fall back

ADR 044 §7 discipline carries verbatim. AEAD verify
failure, footer-HMAC failure, IPC parse failure after
AEAD verify, page-directory parse failure, row-group
range overflow, layout invariant violation — all
surface as corruption / invalid-data errors on the
existing storage-error paths.

The `RowGroupRejectCode` taxonomy:

```rust
#[repr(u32)]
pub enum RowGroupRejectCode {
    PageDirectoryParseError = 1,
    RowGroupRangeOverflow   = 2,
    LayoutUnsupported       = 3,
}
```

Closed V1 taxonomy. Discriminants start at 1 so a
zero-init reject doesn't decode as valid. New codes
require a follow-up ADR.

`Fallback(...)` carries only **economic** /
**unsupported-layout** reasons. Architectural test
forbids authentication / decode errors from reaching
the Fallback branch.

### 7. ADR 052 secondary-index integration

ADR 052 §3 returns `(SlabId, RecordLocation,
RecordRowIndex)` triples. ADR 056 adds the adapter:

```rust
pub async fn fetch_indexed_rows(
    hits: &[SecondaryIndexHit],
    requirement: &ColumnFetchRequirement,
) -> Result<Vec<ProjectedRecordBatch>, FetchError>;
```

Behavior:

- group hits by `(SlabId, RecordLocation)`;
- for each record, derive a `RowGroupSet` from the
  hit `RecordRowIndex` values via the page directory;
- call `pick_l2_fetch_strategy` with that row-group
  set + requirement;
- `RowGroup(plan)`: fetch row-group batches, slice to
  the named rows, return dense
  `ProjectedRecordBatch`;
- `Columnar(plan)` (UnpagedRecord fallback): ADR 044
  column fetch, slice to the row;
- `WholeRecord` / `Fallback(reason)`: ADR 044
  whole-record fetch, slice to the row.

Hits across distinct records issue parallel fetches
bounded by `max_parallel_range_gets`. Hits within the
same record coalesce to one fetch.

Pre-056 records (no directory) are served via
fallback; identity-axis output is byte-identical.

### 8. ADR 042 multi-row composition

Multi-row records are the primary beneficiary. A
65,536-row record with 50 columns split into 16 row
groups of 4096 rows: a 1-row point read fetches 1 row
group = 1/16 of the projected buffer bytes; a 1024-row
contiguous range fetches 1 row group = 1/16; a
5%-selectivity predicate evenly distributed fetches
all 16 row groups in Phase B (predicate columns) then
projects on the surviving rows in Phase C.

`__row_hlc__`, `__row_key_id__`, `__row_routing_locality__`
synthetic columns appear in every row-group batch
and are projected only when the PBC program references
them (visibility-metadata folds, etc.) — ADR 042 §6
stays verbatim.

### 9. ADR 043 per-buffer LZ4 composition

Each row-group RecordBatch's buffers are compressed
via ADR 043's per-buffer LZ4_FRAME at the row-group's
own buffer grain. **No frame flag change. No codec
catalog extension. No partial-buffer materializer.**
Compression ratio per-buffer matches ADR 043 on
equivalent row-group input; aggregate ratio per-record
may degrade marginally vs. single-batch records
because row-group boundaries break long compression
runs (Phase 10 bench measures; expect 1–4% on integer
/ float, lower on UTF-8).

### 10. ADR 044 column-fetch composition

ADR 044 column fetch and ADR 056 row-group fetch are
the same dispatch family. The planner code path is one
function with three legs (RowGroup / Columnar /
WholeRecord) plus Fallback. Shared substrate:
`TieredMultiRangeReadPlan`, `RecordBufferDirectory`,
`L2ColumnFetchConfig` (extended into a sister
`L2RowGroupFetchConfig` for row-group-specific knobs
in §13).

A query with both projection narrowing AND row-group
narrowing flows through `RowGroup(plan)`: column
projection narrows the buffer set per row group;
row-group selection narrows the row-group set within
the record. Both wins compose multiplicatively.

### 11. ADR 055 LSM compaction integration

LSM compaction outputs go through the same writer.
Compaction worker:

- inherits `RowGroupWriterConfig` from the node
  (cluster-wide; per-node override forbidden V1);
- emits paged outputs with freshly-recomputed
  row-group stats (never copies input directories);
- preserves visible-state digest invariance (ADR 055
  §6) — row-group boundaries do not change visible
  row set;
- preserves per-version-entry digest (ADR 052) — row
  identity unchanged;
- preserves secondary-index `RecordLocation.record_row_index`
  references (sidecars still point to a physical
  record + row index; row-group directory accelerates
  fetch but does not change sidecar identity);
- preserves cross-replica logical equality (ADR 055
  §"Why deterministic compaction") — row-group
  boundaries are deterministic given identical input
  bytes + identical config.

Pre-056 → post-056 first-pass compaction produces
paged output even from unpaged input; this is the
graceful upgrade path.

### 12. CDC, recovery, rehydration are byte-neutral

- CDC catchup (ADR 054 §6) reads full record bodies
  and emits version entries in row-axis identity
  order; row-group directory is invisible.
  Architectural guard: CDC modules do not import
  `RecordPageDirectory` or `RowGroupFetchPlan`.
- Recovery from raw slab bytes (ADR 042 §3, ADR 043
  §8, ADR 044 §14) decodes Arrow IPC and rebuilds
  redb indexes. Recovery decodes the full IPC stream
  (concatenates row-group batches), so directory
  presence is transparent.
- Rehydration (ADR 006) installs whole slab bytes;
  subsequent L1 reads find the directory and use
  row-group fetch path automatically.

### 13. Node-local config, not descriptor policy

```rust
pub struct RowGroupWriterConfig {
    pub enabled: bool,
    pub target_row_group_rows: RowGroupRowCount,        // 4_096
    pub max_row_group_rows: RowGroupRowCount,           // 8_192
    pub min_paged_record_rows: RowCount,                // 8_192
    pub min_paged_record_bytes: RecordBodyByteLen,      // 512 KiB
    pub stats_supported_types: BTreeSet<DataType>,      // ADR 039 audited
}

pub struct L2RowGroupFetchConfig {
    pub enabled: bool,
    pub max_selected_row_group_fraction_micros: u32,    // 250_000
    pub max_selected_byte_fraction_micros: u32,         // 500_000
    pub max_ranges_per_record: EncryptedChunkCount,     // 16
    pub max_parallel_range_gets: EncryptedChunkCount,   // 4
}
```

Two configs because writer-side and reader-side
toggle independently during fleet rollout.
`RowGroupWriterConfig` is **deployment-wide** in V1
— applied uniformly across locality replicas to
preserve cross-replica byte-equality (Consequences
§"Cross-replica byte-equality").

Fraction fields use ADR 044's fixed-point denominator
(`1_000_000`). Validation rejects zero row-group
sizes, max below target, fractions ≥ 1_000_000, zero
range caps.

V1 deployment: both configs ship with `enabled =
false`; flipped on after Phase 10 bench acceptance.

### 14. Existing invariants stay fixed

- ADR 020 authentication unchanged.
- ADR 022 footer layout unchanged.
- ADR 039 CSB pruning runs before any record-body
  fetch; unchanged.
- ADR 040 AGG short-circuit runs before any record-
  body fetch (ADR 042 §11 / ADR 044 §10); unchanged.
- ADR 042 synthetic columns stay storage-only;
  stripped before user-visible output.
- ADR 043 codec catalog unchanged.
- ADR 044 `ColumnFetchRequirement` remains the
  source of required user / predicate / aggregate /
  storage columns.
- ADR 052 secondary-index sidecars retain identity-
  axis shape; row-group fetch is a faster path to
  the same rows.
- ADR 055 compaction rebuilds paged outputs out-of-
  place; directories recomputed, never patched in
  place.
- PBC opcode catalog (ADR 035) unchanged. Phase B
  predicate evaluation uses existing PBC opcodes
  against fetched row-group batches.

### 15. Capability and protocol surface

No new negotiated `NodeCapability`. ADR 056 is an
internal storage-side substrate. Toron-visible PBC
programs, ADR 036 pushdown contract, data-server /
control-server wire frames all unchanged.

A node either writes paged records (writer config
enabled) and reads them via three-phase planner
(reader config enabled) or does not. Mixed-fleet
rollout is byte-identical on logical results;
row-group fetch metrics will show two populations
during rollout.

### 16. Observability

Metrics under `storage_l2_row_group_fetch_*`:

```text
storage_l2_row_group_fetch_requests_total{strategy}
storage_l2_row_group_fetch_fallback_total{reason}
storage_l2_row_group_fetch_row_groups_total{decision}
storage_l2_row_group_fetch_bytes_total{kind}
storage_l2_row_group_fetch_plan_micros
storage_l2_row_group_fetch_selected_ranges
storage_l2_row_group_fetch_directory_bytes
storage_l2_row_group_fetch_invariant_violations_total{violation_kind}
```

Pinned label enumerations:

```text
strategy = row_group | columnar_buffer | whole_record
reason   = disabled | single_row | full_projection
         | small_record | too_few_rows
         | too_many_row_groups | too_many_bytes
         | too_many_ranges | metadata_probe_cap
         | unsupported_arrow_layout
         | rehydration_available | unpaged_record
decision = skipped_by_stats | predicate_fetched
         | projected_fetched | no_survivors
kind     = directory | predicate | projected
         | avoided
violation_kind = page_directory_parse
              | row_group_range_overflow
              | layout_unsupported
```

No tenant / table / column / row index / slab id /
error-string / path labels.

Audit events (`src/audit/events.rs`):

- `RowGroupFetchSampled { tier,
  row_groups_fetched, row_groups_total,
  predicate_fetched_groups, dominant_strategy }`
  (sampled at planner-config rate, debug-grade);
- `RowGroupInvariantViolation { kind }` (page-grade).

### 17. Architectural lockdown

`tests/architecture_test.rs` enforces:

1. exactly one `RecordPageDirectory`,
   `RowGroupDirectoryEntry`,
   `BufferPageDirectoryEntry`, `RowGroupColumnStats`
   definition;
2. `RowGroupIndex`, `RowGroupRowCount`,
   `IpcColumnOrdinal` are newtype-units;
3. exactly one `RowGroupFetchPlan` definition; not
   imported by CDC, PBC, recovery, or catalog
   modules;
4. `L2FetchStrategy::RowGroup` present; dispatch
   sites match all four variants explicitly (no `_`
   catch-all);
5. `RowGroupRejectCode` taxonomy closed to three V1
   codes;
6. `RowGroupFallbackReason` contains only economic /
   unsupported-layout reasons; no authentication /
   decode variants;
7. `storage_l2_row_group_fetch_*` metrics carry
   exactly the bounded labels;
8. no new `MsgType` containing `ROW_GROUP` or
   `PAGE_DIRECTORY`;
9. no new `FRAME_FLAG_*`;
10. no new `NodeCapability` bit;
11. no `ObjectStore` trait method addition;
12. no table descriptor field for paging or stats
    policy;
13. no synthetic Arrow column for the directory
    (no `SyntheticColumn::RowGroupDirectory` or
    `__row_group_directory__` literal);
14. no sparse Arrow array materializer for unfetched
    row groups (architectural test rejects null-fill
    paths);
15. authentication / parse failures cannot reach the
    `Fallback(...)` branch (carries ADR 044 §7
    discipline);
16. no envelope `format_version` bump; no ADR 022
    footer field addition;
17. row-group planner reuses ADR 044 / ADR 020
    multi-range planning (no parallel chunk
    arithmetic);
18. `Schema.custom_metadata` key for the directory
    is the literal string `storage.row_group_directory`;
19. writer reads from `RowGroupWriterConfig` only,
    not from a descriptor field;
20. compaction writer recomputes directories;
    never copies input directory bytes;
21. no `Wendelon`-prefixed row-group identifiers;
22. no `RowGroup*V[0-9]` /
    `PageDirectory*V[0-9]` Rust type suffixes;
23. CDC, recovery, rehydration code paths do not
    import `RecordPageDirectory` or
    `RowGroupFetchPlan`;
24. no LZ4 frame block-independence flag mutation;
    each row-group RecordBatch's buffers use ADR
    043's existing frame shape unchanged.

## Rationale

### Council convergence retrospective

The council oscillated four times across five
iterations between RecordBatch row groups and
per-buffer intra-buffer mini-pages before recognizing
that both shapes are layers of the same hierarchy
rather than alternatives. The convergent answer
matches Parquet / Vortex: row groups are the outer
macro-pruning unit; per-buffer pages are the inner
random-access / encoding unit. The layers are equal
priority; ordering is software-development driven.
ADR 056 V1 ships the outer layer first because row
groups establish the outer contract — row partition,
stats, fetch planner, index mapping, compaction /
recovery rules, and failure taxonomy — under which
ADR 057 encoding cascades's per-buffer pages can safely nest inside each
row-group RecordBatch's encoded buffers without
re-architecting V1. The arguments traded across
iterations are preserved in council git history;
they applied correctly to the shape question, but
the shape question was the wrong frame.

**Why row groups as Arrow RecordBatches.**
§"Why row groups as RecordBatches, not intra-buffer
mini-pages" gives the seven-axis argument: stock Arrow
decode (no custom materializer); no LZ4 frame change;
row-group stats are natural; ADR 057 encoding cascades layers cleanly on
top; per-buffer page directory wording satisfied via
`(row_group, buffer)` entries; whole-record decode
trivial; ADR 044 substrate continuity.

**Why page directory in IPC custom metadata.** Two
rejected alternatives, two distinct reasons:
- *Footer extension*: parallel source of truth;
  HMAC-scope growth; cleartext workload-shape leakage.
- *Synthetic column*: directory has one logical value
  per record body, not per row; column ordinal
  pollution.

IPC `Schema.custom_metadata` is the existing slot;
encrypted inside the record body; authenticated by
ADR 020 chunk AEAD. Authority discipline from ADR 043
§4 / ADR 044 §3 carries.

**Why row-group stats in V1.** Once row groups are
RecordBatches, computing min / max / null-count per
batch at write time is cheap. Excluding stats would
force every Phase B to fetch predicate columns and run
filter even when stats could have proved disjoint —
the dominant V1 win on selective predicates. Stats are
not CSB; CSB remains slab-level admission. They are an
in-record fetch-planning surface.

**Why three-phase fetch.** Phase A (metadata +
directory) + Phase B (stats prune, predicate fetch
when needed) + Phase C (projected pages over surviving
groups) captures the dominant analytical wins:
- selective predicates skip via stats — Phase B
  fetches little or nothing;
- selective predicates with insufficient stats fetch
  predicate columns + filter once — Phase B is still
  narrow;
- output projection narrows by columns AND row-group
  survival in Phase C.

**Why dense batches only (no sparse arrays).** Inherits
ADR 044's rejected-dummy-arrays argument verbatim. A
sparse array with nulls for unfetched row groups would
let downstream code skip the row-group ordinal mapping
and silently produce wrong aggregates (a `MIN` over an
all-null array yields `NULL`, not "this row group was
not fetched"). Dense `ProjectedRecordBatch` values per
fetched row group make the invariant crisp.

**Why no Toron wire change.** Toron sends PBC; storage
derives row-group selection from PBC's filter +
projection + row-group stats. New wire field would
duplicate information already in PBC.

**Why no descriptor field.** Row-group fetch is a
physical read/write strategy that doesn't change
logical schema, row identity, or visible row set.
Descriptor churn would leak a physical tuning knob
into logical schema identity.

**Why writer-side gated, reader-side automatic on
presence.** Symmetric with ADR 044 §15 / Phase 10
rollout discipline. Writer-side change ships behind a
config flag for fleet-rollout safety; reader-side
picks up directories the moment they appear.

**Why fail-closed on directory parse / range
overflow.** ADR 044 §7 discipline. Page directory
bytes that AEAD-verify but parse as invalid indicate
writer bug or tampering; silent fallback would mask
both.

**Why default `target_row_group_rows = 4096`,
`min_paged_record_rows = 8192`,
`min_paged_record_bytes = 512 KiB`.** Parquet
convention is 4096–8192 rows per page. ADR 042's
`max_record_bytes` is 1 MiB, so paging engages only
on records well above the single-batch sweet spot.
Writer's max is 8192 to ensure a paged record always
has at least 2 row groups (otherwise a single-batch
record is cheaper). All four defaults tunable; bench
Phase 10 picks.

**Why fraction thresholds 0.25 / 0.50.** A query that
selects more than 25% of row groups (or more than 50%
of bytes) likely doesn't beat ADR 044 column fetch
once Phase B + Phase C round trips are amortized.
Below those thresholds, row-group fetch wins.
Conservative initial defaults; bench tunes.

**Why `RowGroupWriterConfig` deployment-wide in V1.**
Cross-replica byte-equality matters for ADR 055
deterministic compaction outputs and for any operator
gossip-based corruption check. If two replicas in a
locality run different writer configs, admission-time
slabs differ in physical bytes (logically equivalent,
byte-different). Visible-state digest invariance still
holds, but byte-equality breaks. V1 keeps the config
uniform.

**Why `storage_l2_row_group_fetch_*` metric prefix.**
Parallel to ADR 044's `storage_l2_column_fetch_*`.

## Consequences

- **Row-axis read amplification drops sharply.** Point
  reads on a 65,536-row record fetch 1/16 (one row
  group) instead of 16/16 of projected buffer bytes.
  Highly selective scans skip row groups via stats.
- **Phase B round-trip cost.** Stats-driven skip is
  free (already in directory). Predicate-fetch when
  stats are insufficient adds one round trip; bounded
  by `max_ranges_per_record`.
- **Writer CPU cost.** Splitting a record into N
  RecordBatch messages and computing per-row-group
  stats is non-trivial. Bench Phase 10 measures.
- **Compression ratio degrades 1–4%.** Row-group
  boundaries break long compression runs. Bench
  measures.
- **Metadata window grows.** Directory is per-record;
  16 row groups × 50 columns × ~16 bytes per buffer
  page entry + stats ≈ 20 KiB per record. Fits
  within ADR 044's 64 KiB initial probe window with
  margin.
- **L2 GET count rises on selective workloads.** Each
  selected row group's projected buffers may live in
  a distinct chunk run. Mitigated by
  `max_ranges_per_record` cap; planner falls back to
  ADR 044 column fetch if exceeded.
- **Provider observes finer access pattern.** Chunk-
  level access leaks row-group correlation to the
  provider on top of ADR 044's column correlation.
  Same threat-model qualification as ADR 044's
  Consequences entry; tenants whose threat model
  treats row-range queries as confidential disable
  the writer config.
- **Cross-replica byte-equality requires uniform
  writer config.** Deployment-wide setting in V1; if
  operators flip per-node, admission slabs diverge
  in physical bytes.
- **Mixed legacy / new directory presence during
  rollout.** Pre-056 sealed records lack directories;
  post-056 may or may not. Logical results
  byte-identical; only fetch shape metrics differ.
- **LSM compaction output bytes-per-row may differ
  from input.** Compaction recomputes directories;
  first-pass pre-056 → post-056 compaction output is
  paged.
- **Recovery cost unchanged.** Whole-record decode
  concatenates row-group batches; identity-axis
  output byte-identical to single-batch records.
- **CDC / migration paths unaffected.**
- **Arrow-version regressions surface here.** IPC
  multi-RecordBatch parsing and `Schema.custom_metadata`
  reader are Arrow-version-specific.
- **No win on uncompressed buffers.** ADR 043
  layout-driven default uses `MultiRow → Lz4Frame`.
- **No win on small records.** Records below paging
  thresholds use single-batch encoding.
- **No win on full-projection / full-scan queries.**
  Planner falls back to ADR 044 column fetch.

## Rejected alternatives

- **Synthetic `__row_group_directory__` column.**
  Directory has one logical value per record, not
  per row.
- **Sparse Arrow arrays with nulls for unfetched row
  groups.** Inherits ADR 044's dummy-nulls
  rejection.
- **Intra-buffer LZ4 mini-pages with
  block-independence flag as the V1 substrate.**
  Required custom partial-buffer materializer
  (offsets-buffer rebasing for UTF-8, validity
  bitmap bit alignment, decompressed-length
  validation), LZ4 frame flag change, codec-
  catalog-adjacent surface. Intra-buffer SIMD pages
  are ADR 057 encoding cascades territory per brainstorm 011 §3 ("encoding cascades
  payload's internal Page Directory"); V1 substrate
  ADR ships RecordBatch-grain row groups, ADR 057 encoding cascades
  adds intra-buffer pages inside each RecordBatch's
  buffers.
- **Page directory in the ADR 022 unencrypted
  footer.** Two sources of truth; HMAC-scope growth;
  cleartext workload-shape leakage.
- **Page directory as a new Wendelon-private
  FlatBuffer block in the slab body.** Breaks ADR
  042's `[SlabRecordHeader] [Arrow IPC]` invariant;
  new on-disk marker; format-version handling.
- **Row-group stats deferred to a later ADR.** Once
  row groups are RecordBatches, stats computation
  is nearly free at write time. Excluding would
  force every Phase B to fetch predicate columns.
  Cheap V1 win.
- **Two-phase fetch only (no Phase B).** Forces
  every selective query through full predicate-
  column fetch. Phase B with stats prune skips most
  fetches entirely on selective workloads.
- **Three-tier mini-pages: row-group → mini-page →
  buffer (intra-buffer SIMD batch).** ADR 057 encoding cascades
  territory.
- **Variable per-buffer mini-page row count.** encoding cascades
  V2.
- **Byte-budgeted row groups.** Variable row count;
  ADR 057 encoding cascades.
- **PBC opcode for row-group fetch
  (`PROJECT_ROW_GROUPS`).** Storage derives selection
  from existing PBC opcodes + row-group stats. New
  opcode would duplicate information.
- **Descriptor-level row-group policy.** Read/write
  strategy doesn't change semantics; descriptor
  churn forbidden.
- **Always-on writer-side generation.** Forces every
  cluster onto the new path simultaneously; no
  fleet-rollout safety. Node-local config gates
  writer side.
- **Provider-native filtering.** Same as ADR 044:
  ADR 020 envelope encryption precludes object-
  store-side inspection.
- **Per-node `RowGroupWriterConfig` overrides in V1.**
  Cross-replica byte-equality breaks.
- **Bulk slab-level metadata pre-fetch.** ADR 044
  deferred follow-up.
- **`RecordPageDirectory` cache across queries.** ADR
  044 deferred follow-up.
- **Move IPC `Schema.custom_metadata` into the
  cleartext slab footer.** Workload shape leakage.
- **Project-name-prefixed type names** (per
  `plans/conventions/no-project-name-in-protocols.md`).
- **`V1` / `V2` suffixes on new Rust types** (per
  `plans/conventions/no-version-suffixes.md`).
- **Reserve enum slots for future fallback reasons**
  (per `plans/conventions/no-premature-backward-compat.md`).
- **Fall back silently to whole-buffer decompression
  on directory parse failure.** Masks corruption.
  ADR 044 §7 discipline.
- **New PBC opcode** for row-group fetch. Fetch
  shape is a storage-side planning decision.

## Open questions

These are explicit decision points; none block the
ADR. Bench-resolved questions land defaults at Phase
10; architectural questions defer to focused
follow-up ADRs.

**Bench-resolved (Phase 10):**

1. **Default `target_row_group_rows`.** 4096 (V1
   default, Parquet) vs. 8192 (Lance) vs. 1024
   (Vortex SIMD-aligned). Smaller maximizes future
   ADR 057 encoding cascades SIMD alignment but inflates directory
   size.
2. **Default `min_paged_record_rows` /
   `min_paged_record_bytes`.** 8192 / 512 KiB (V1,
   paging engages only on records ~2× single-batch
   sweet spot).
3. **Per-row-group concurrency in Phase B / C.**
   `max_parallel_range_gets = 4` per record; should
   row groups within a record fetch concurrently?
4. **L1 (NVMe) bypass.** L1 has no network round
   trip; only decompression / decode CPU win
   remains. Bench measures whether planner overhead
   justifies the win on L1.
5. **Cross-record metadata pre-fetch in Phase A.**
   ADR 044 noted coalescing; ADR 056's directory
   bytes are larger, calculus shifts.
6. **Predicate classifier coverage gap.** V1 covers
   equality, range, IS NULL, IS NOT NULL,
   conjunction (skip when any branch proves
   disjoint), and disjunction (skip only when every
   branch proves disjoint). Top-level LIKE / IN /
   BETWEEN-with-cast keep all groups; in conjunction
   they collapse to Keep without blocking sibling
   skips; in disjunction they keep the group. Bench
   measures the residual gap.

**Architectural (deferred to follow-up ADRs):**

7. **Stats-supported types.** ADR 039 defines the
   canonical set. Phase 0 audits and pins; if ADR
   039 evolves, ADR 056 inherits.
8. **Cross-replica byte-equality enforcement.**
   Currently only a deployment-wide convention.
   Should ADR 055 carry an explicit
   `LsmInvariantViolation` guard for divergent
   writer configs across replicas? Defer to a
   focused ADR 055 follow-up if operational evidence
   justifies.
9. **Phase B predicate fetch granularity.** Row-group
   buffers are smaller but multi-range; column-fetch
   buffer is larger but single-range. V1 uses
   row-group buffers; revisit if metrics show
   predicate-fetch cost dominating.
10. **`unsupported_branch_disjunction` keeps-all vs.
    fetch-and-evaluate.** V1 picks conservative
    keeps-all. Future ADR may admit
    fetch-and-evaluate when bench shows gap.
11. **Schema-fingerprint propagation through
    secondary-index hits.** ADR 052 hits should
    carry source slab's schema fingerprint so
    mismatched plans reject as corruption.
    Confirm in ADR 052 §3 follow-up.

## Forward-compatibility analysis

### Substrate sequencing

The substrate evolution is a **committed layered
hierarchy**, not just a forward-compatible direction.
Both axes ship; ADR 056 is the outer layer, ADR 057 encoding cascades is
the inner layer:

```text
record body
  └─ row-group RecordBatch                  (ADR 056 V1 — outer layer, this ADR)
       └─ encoded column buffer              (ADR 057 encoding cascades cascade — inner layer)
            └─ intra-buffer SIMD page         (ADR 057 encoding cascades page directory — inner layer)
                 └─ encoded run                (COCD COCD — innermost layer)
```

This mirrors Parquet's row-group + column-chunk + page
hierarchy and Vortex's chunked-encoded-arrays-with-pages
model — the convergent design every modern OLAP engine
arrives at. The layers are equal priority; ordering is
software-development driven. ADR 056 V1 lands the outer
layer first because row groups establish the outer
contract under which ADR 057 encoding cascades's per-buffer pages can
safely nest. ADR 057 encoding cascades will land the inner layer next,
providing fine random access, per-column encoding
cascades, and SIMD-friendly compute. The two compose
without rework: each row group RecordBatch is a complete
unit of the outer layer; ADR 057 encoding cascades's per-buffer pages and
SIMD blocks live inside its encoded column buffers.

### ADR 057 encoding cascades mechanics under this hierarchy

**ADR 057 encoding cascades (homerolled SIMD encodings).** ADR 057 encoding cascades's
encoding cascade (per brainstorm 011 §1: e.g.
`RLE(BitPacked(Dictionary(Strings)))`) is applied
per-column-per-record. Under ADR 056's RecordBatch
shape, ADR 057 encoding cascades applies the cascade to each
`(row_group, column)` pair the same way it would
apply to a single-batch record's column buffer — the
cascade decision and encoding mechanics are unchanged;
just the buffer instances multiply by row-group count.

**ADR 057 encoding cascades sub-piece (intra-buffer SIMD pages /
per-buffer pages).** Brainstorm 011 §3's "Lance
idea" — decompress only the 64-value SIMD block
containing a target row — lands inside each row-group
RecordBatch's encoded buffer as an intra-buffer page
directory. This is the inner layer of the hierarchy:
ADR 056 row groups outside, ADR 057 encoding cascades per-buffer pages
inside.

ADR 056's directory shape is forward-compatible.
ADR 057 encoding cascades may add a sibling
`buffer_simd_pages: Vec<...>` field on
`BufferPageDirectoryEntry` (or a sibling
custom_metadata key like `encoding.simd_page_directory.v1`)
pointing inside each encoded buffer.

**ADR 057 encoding cascades sub-piece (variable per-buffer mini-page row
count).** Would extend `BufferPageDirectoryEntry` to
carry per-buffer row counts. ADR 056's invariant
(uniform `row_count` per row group) relaxes per-buffer;
forward-compatible extension.

**COCD (compute-over-compressed-data).** PBC opcodes
that evaluate over encoded bytes consume row-group
RecordBatches; codec-agnostic at the buffer level.

**ADR 039 V2 (mini-page CSB).** Adds column statistics
per intra-buffer SIMD page (sub-row-group). ADR 056's
row-group stats stay at row-group grain; ADR 039 V2
adds parallel sub-row-group stats. Forward-compatible.

**zstd capacity-tier codec.** New compression variant.
Page directory mechanism is codec-agnostic at the
byte-range level. Forward-compatible.

**Brainstorm 010 (serverless PBC L2).** Lambda-like
runtimes consume the same chunk-aligned authenticated
range reads + page directory + row-group fetch plan.
ADR 056 surfaces the substrate Lambda inherits.

**ADR 052 expansion (composite secondary indexes,
per-row predicates).** Composite indexes returning
larger row sets benefit from row-group fetch.
Per-row predicates compose with row-group stats.
Forward-compatible.

**Future LSM tier-2 compaction (direct already-
tiered compaction).** Compaction outputs include
directories regardless of input tier; layer-agnostic.

## Deferred to follow-up ADRs

1. **Intra-buffer SIMD mini-pages (sub-row-group).**
   ADR 057 encoding cascades; brainstorm 011 §3 Lance idea ships inside
   the encoding-cascade payload.
2. **Variable per-buffer mini-page row count.** encoding cascades
   V2.
3. **`RecordPageDirectory` cache across queries.**
4. **Bulk slab-level page-directory pre-fetch.**
5. **Compute-over-compressed-data (COCD) at row-
   group grain.** COCD.
6. **Per-row-group DEK rotation.** Future ADR 020
   extension.
7. **Wire-tier row-group fetch (ADR 053
   extension).**
8. **Public Toron `PROJECT_ROWS` opcode** (or
   equivalent surface).
9. **Variable row-group byte budget (hybrid byte-
   and-row).** ADR 057 encoding cascades.
10. **Cross-record / cross-slab row-group
    abstractions.** LSM-level sort orders.
11. **Per-row-group encryption-domain isolation.**
12. **Row-group stats sketches (HLL, t-digest).**
13. **Adaptive row-group sizing per record based on
    schema width / cardinality.**

## References

- `plans/adr/003_stellarator_bifurcated_writes.md` —
  finalization is the publish gate.
- `plans/adr/006_tiered_storage_lazy_migration.md` —
  rehydration stays whole-slab.
- `plans/adr/020_l2_envelope_encryption_and_dek_lifecycle.md`
  — chunked AEAD; row-group bytes ride the same
  authenticator.
- `plans/adr/022_l2_slab_format_footer_indexed_range_reads.md`
  — unencrypted footer; ADR 056 declines to extend.
- `plans/adr/034_toron_wendelon_integration_contract.md`
  — Toron-visible surface unchanged.
- `plans/adr/035_vectorized_pbc_executor.md` — PBC
  consumes RecordBatch; row-group fetch returns
  ProjectedRecordBatch.
- `plans/adr/036_pbc_pushdown_contract.md` — Toron-
  visible PBC unchanged.
- `plans/adr/037_tenant_table_schemas.md` —
  descriptor surface unchanged.
- `plans/adr/039_slab_footer_column_statistics_v1.md`
  — slab-level CSB; row-group stats use same
  canonical type rules at different grain.
- `plans/adr/040_aggregate_pushdown.md` — AGG
  short-circuit unchanged.
- `plans/adr/042_analytical_multi_row_fragments.md`
  — multi-row record bodies; row-group RecordBatches
  compose naturally.
- `plans/adr/043_arrow_ipc_body_compression.md` —
  per-buffer LZ4 unchanged at row-group grain.
- `plans/adr/044_l2_columnar_fetch.md` — column-
  fetch planner extended to three phases; substrate
  types reused.
- `plans/adr/046_state_digest_substrate.md` —
  visible-state digest invariant under row-group
  boundaries.
- `plans/adr/051_descriptor_fingerprint_split.md` —
  logical schema fingerprint check on directory.
- `plans/adr/052_secondary_indexes.md` — primary V1
  consumer of row-group point reads.
- `plans/adr/053_wire_arrow_ipc_body_compression.md`
  — wire-tier compression independent of storage-
  tier page directory.
- `plans/adr/054_cdc_subscription_substrate.md` —
  CDC catchup runs whole-record; no row-group
  interaction.
- `plans/adr/055_lsm_leveled_compaction_substrate.md`
  — compaction outputs include directories under
  writer config; cross-replica logical equality
  preserved.
- `plans/brainstorms/011_encoding_cascades_vortex.md`
  §3 — "Lance idea" intra-buffer page directory; encoding cascades
  V2 territory, ADR 056 V1 ships row-group
  RecordBatches.
- `plans/conventions/newtype-units.md`,
  `plans/conventions/no-version-suffixes.md`,
  `plans/conventions/no-premature-backward-compat.md`,
  `plans/conventions/no-project-name-in-protocols.md`.
