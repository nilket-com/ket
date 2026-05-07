# Wendelon Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward.

**Update cadence.** The roadmap is rewritten at planning
sessions, not at implementation commits. The council operates on
a block-chain rhythm: plan → implement → plan → implement, and
this file is a snapshot of the most recent plan. Between
sessions the roadmap lags the code on purpose — an item can read
"implementation phases pending" while the implementation is
already merged on `main`, or "full 100k / 1M bench matrix
remains ahead" while a smoke run has already landed. That is
expected, not a discrepancy. Don't open a session by flagging
the gap as if it were news; check `git log` and the relevant
impl plan / bench-results file for actual status, and roll
roadmap edits into the next planning session along with the
forward-sequence rewrite.

## Where we are

ADR 058 (Compute Over Encoded Data, COCD)
council-converged across six iterations and design-promoted.
Implementation phases pending; full small / medium / large
bench evidence is the promotion-to-shipped gate. Innermost
layer of the storage hierarchy ADR 056 / ADR 057 established.
**Closed 6-variant predicate evaluator catalog**: `BitPacked`,
`RlePlain`, `RleBitPacked`, `DictionaryPlain`,
`DictionaryBitPacked`, `Delta` (full atom set, full-buffer
grain). `Plain`, `Lz4Frame`, `Fsst`, `Alp` cascades fall back
to materialize-then-evaluate. **Closed 8-kernel aggregate
catalog**: `count_star`, `count_non_null`, `min`, `max`,
`signed_sum`, `unsigned_sum`, `signed_avg`, `unsigned_avg`.
`min`/`max` target fixed-width integer / time / Boolean and
fixed-width Dictionary cascades; sum/avg are integer-only
(Int8/16/32/64 for signed, UInt8/16/32/64 for unsigned),
matching ADR 040 dense aggregate validation. Runs after
ADR 040 CSB direct-answer ordering. COCD kernels never
exceed ADR 040 dense validation; dense-supported shapes
that COCD does not yet fold (Utf8 / Binary `min`/`max`,
`APPROX_DISTINCT`, `QUANTILE_KLL`) fall back to dense AGG
via the all-or-nothing mixed-request policy. **No new public PBC opcodes** — Toron-visible PBC
contract (ADR 035 / ADR 036) byte-identical; COCD is internal
executor dispatch, not a Toron-facing opcode extension.
**Activation via node-local `EncodingComputeConfig`**
(deployment-wide for cross-replica determinism), sibling to
ADR 057's `EncodingWriterConfig` / `L2EncodingFetchConfig`;
no descriptor field, no `NodeCapability` bit. **Exact /
Prefilter / DenseFallback predicate plan modes**: Exact when
every leaf has an encoded kernel (PBC tree composes via mask
algebra; PBC executor not invoked post-Phase-B); Prefilter
when a conjunction has at least one encoded and one residual
leaf (encoded leaves drop rows; original PBC re-runs against
survivors); DenseFallback for unsupported shapes (existing
ADR 057 path). **Dual economic gates**: leaf-level
selectivity gate at 0.25 (every encoded predicate leaf
including Exact-mode requires CSB-estimated row-group
selectivity ≤ 0.25; selectivity > 0.25 routes to dense
materialize-then-evaluate, which wins by reusing one fetch as
predicate + projection input) plus secondary Prefilter
rejection gate at 0.10 (encoded leaves must reject ≥ 10% of
rows to amortize residual narrowing). Aggregate folding is
ungated by predicate selectivity. **`EncodedRowMask` tagged
enum** (`All { rows }` / `None { rows }` /
`Bitmap { rows, bytes }` / `Runs { rows, runs }`):
RLE-friendly fixtures stay compact, all/none short-circuits
remove O(N) work on common Dictionary cases, mask byte
allocation bounded by `max_mask_bytes_per_record`. **Page-
grain dispatch default** for page-emitting cascades
(`BitPacked`, `Rle { inner: BitPacked }`,
`Dictionary { inner: BitPacked }`); buffer-grain for
`Rle { inner: Plain }`, `Dictionary { inner: Plain }`;
full-buffer grain for `Delta` (no page emission in ADR 057).
**Closed 5-code `EncodingComputeRejectCode` taxonomy**
(`MaskLengthMismatch`, `EncodedValueOverflow`,
`KernelDecodedLengthMismatch`, `AggregateOverflowMismatch`,
`SelectionRangeOverflow`); authentication / parse / decode
failures route to storage errors, never silent fallback.
**Closed 9-variant `EncodingComputeFallbackReason` enum** for
planner metric labels. Bounded `storage_l2_encoding_compute_*`
metric family with `{decision, reason, kernel, cascade, kind,
aggregate, violation_kind}` enumerations. V1 defaults:
`max_dict_match_set_size = 4_096`,
`min_buffer_rows_for_encoded_eval = 256`,
`max_parallel_kernel_tasks = 4`,
`max_mask_bytes_per_record = 256 KiB`,
`max_encoded_pages_per_record = 256`,
`min_selectivity_for_encoded_eval_micros = 250_000`,
`min_prefilter_rejection_micros = 100_000`. **Reader-CPU
bench gate** as starting target: predicate-only Exact COCD
≥ 2× faster than ADR 057 dense PBC on representative
`BitPacked` / `Rle` / `Dictionary` fixtures at low
selectivity; aggregate kernels ≥ 2× faster than dense AGG on
representative `Rle` / `BitPacked` fixtures; dense-selection
overhead ≤ 5% vs. ADR 057 dense path when COCD economically
falls back. Provisional discipline applies (revisable on
Phase 8 evidence). ADR 052 secondary-index point reads gain a
COCD confirm-step for COCD-supported cascades. ADR 055 LSM
compaction, ADR 054 CDC subscription, recovery, and
rehydration are byte-neutral to COCD. **~27 architectural
lockdown guards** in `tests/architecture_test.rs`. ADR 057
directory shape unchanged — COCD reads the existing
`BufferEncoding` / `EncodingCascade` shape. No new wire
`MsgType`, no new `FRAME_FLAG_*`, no new `NodeCapability`
bit, no Cargo feature, no new digest domain, no `ObjectStore`
trait change, no envelope `format_version` bump, no ADR 022
footer extension, no descriptor surface change, no public
Toron-visible PBC opcode addition, no ADR 057 cascade catalog
extension. `Fsst` / `Alp` cascade encoded evaluation deferred
(own kernel proofs); aggregate push-into-encoded for
`approx_distinct` / KLL / HLL / `count_distinct` / float /
decimal / GROUP BY all deferred to follow-up ADRs. See
`plans/adr/058_compute_over_encoded_data.md` and
`plans/impl/058_compute_over_encoded_data.md`.

ADR 057 (Per-Buffer Pages and Encoding Cascades)
council-converged across five iterations and promoted
after implementation plus Phase 10 bench evidence. Inner layer of the
Parquet / Vortex-style storage hierarchy ADR 056
opened. **Closed 8-variant cascade catalog**: `Plain`,
`Lz4Frame`, `BitPacked`, `Rle`, `Dictionary`, `Delta`,
`Fsst`, `Alp` with one-level composition via
`Box<EncodingLeafCascade>` (`EncodingLeafCascade ∈ {Plain,
Lz4Frame, BitPacked}`); arbitrary-depth nesting
rejected. **Activation via node-local
`EncodingWriterConfig`** (deployment-wide for cross-replica
byte-equality), not via
`RecordBodyCompressionPolicy::EncodingCascade` descriptor variant;
Encoding cascades do not change logical schema or table semantics,
so descriptor churn is unwarranted.
**`EncodingDirectory` lives in IPC schema
`custom_metadata`** under literal key
`storage.encoding_directory` — sibling to ADR 056's
`storage.row_group_directory`, encrypted with the
record body, authenticated by ADR 020 chunk AEAD.
**Compact directory shape** is the council's iter
3→4 win: cascades deduped once in
`cascades: Vec<EncodingCascade>`; one `BufferEncoding`
per `(row_group_index, ipc_column_ordinal,
ipc_buffer_ordinal)` triple referencing a cascade by
`EncodingCascadeIndex`; page-grain bytes captured as
`page_end_offsets: Vec<EncodingPageByteOffset>` prefix
array per buffer — not as one serialized row per page
(per-page rows would explode metadata on wide
schemas; ADR 056 directories already use 22–26% of
ADR 044's 64 KiB initial probe).
`EncodingPageDescriptor` is transient planner-only —
expanded from prefix offsets in memory, never
serialized. **Buffer-relative byte offsets** mean Encoding
metadata is invariant under any compaction or rewrite
that shifts the buffer's absolute position but
preserves its internal encoded layout; ADR 056
locates the buffer, ADR 057 slices pages within it.
**Page emission required** for `BitPacked`, `Rle`,
`Dictionary`; other cascades carry empty
`page_end_offsets` and decode at full-buffer grain in
V1. **Phase D extension** to ADR 056's three-phase
plan (Phase A metadata → B stats prune → C projected
fetch → **D Encoding materialization**); internal to the
`L2FetchStrategy::RowGroup(plan)` leg, no fifth
strategy variant. **Custom partial-buffer
materializer** lives in `src/storage/encoding/`,
satisfying ADR 056's V1-scope guards 25–26 (the
sanctioned home for `partial_buffer_materialize` and
`mini_page_*` symbols ADR 056 explicitly kept out of
its surface). **Decode-only V1** — PBC continues to
evaluate against materialized dense Arrow batches;
compute-over-encoded-data is ADR 058 (COCD)
territory. **Closed 5-code `EncodingRejectCode`
taxonomy** (`EncodingDirectoryParseError`,
`BufferEncodingMismatch`, `PageRangeOverflow`,
`CascadeUnsupported`, `DecodedLengthMismatch`);
authentication / parse / decode failures route to
storage errors, never silent fallback. **Closed
7-variant `EncodingFallbackReason` enum** for planner
metric labels. Bounded `storage_l2_encoding_*` metric
family with `{cascade, decision, reason, kind,
violation_kind}` enumerations. V1 defaults:
`target_simd_page_values = 1_024`,
`cascade_sample_rows = 256`,
`min_buffer_bytes_for_encoding = 4_096`,
`max_simd_pages_per_record = 64`,
`max_parallel_materialize_tasks = 4`. **Writer-cost
gate ≤ 75%** as starting target on the largest legal
representative fixture, defended by mandatory
compression and read-side wins in the same bench
artifact; revisable on Phase 10 evidence per the
Provisional discipline ADR 056's ≤ 15% miss
codified. ADR 052 secondary-index point reads
accelerated via SIMD-page fast path; ADR 055 LSM
compaction recomputes cascades deterministically;
CDC, recovery, rehydration are byte-neutral. No new
wire `MsgType`, no new `FRAME_FLAG_*`, no new
`NodeCapability` bit, no Cargo feature, no new digest
domain, no `ObjectStore` trait change, no envelope
`format_version` bump, no ADR 022 footer extension,
no descriptor surface change, no public Toron-visible
PBC opcode addition, no Vortex / Lance / Parquet
external dependency. **~30 architectural lockdown
guards** in `tests/architecture_test.rs`. See
`plans/adr/057_per_buffer_pages_and_encoding_cascades.md`
and
`plans/impl/057_per_buffer_pages_and_encoding_cascades.md`
plus `plans/impl/057_per_buffer_pages_bench_results.md`.
Implementation and Phase 10 evidence have landed:
representative writer gate passes on repeated full
bench runs after bounding multi-page variable-width
Dictionary selection; typed compression targets pass;
point-read selected bytes are ≥16x smaller than the
ADR 056 row-group-buffer baseline; materialization is
faster than ADR 056-only on all representative fixtures;
metadata stays inside the 64 KiB initial probe; and
cross-replica determinism passes. ADR 058 is now
unblocked for drafting from concrete cascade-distribution
and page-grain evidence.

ADR 056 (Row-Group Fetch and Layered Page Directory)
is the immediate predecessor and the substrate ADR
057 nests inside. Promoted after Phase 10 accepted an
empirical writer-cost gate revision. Closes the
row-axis fetch-amplification shape question for
multi-row records and lays the outer layer of a
Parquet / Vortex-style storage hierarchy. **Layered
substrate sequencing committed**: ADR 056 is the outer
row-group + per-row-group stats layer; ADR 057 is the
inner per-buffer pages + encoding-cascades layer inside
each row-group RecordBatch's encoded buffers; ADR 058 is
compute over encoded representation (COCD). The layers are
equal priority; ordering is software-development
driven — row groups establish the outer contract
under which per-buffer pages can safely nest, so they
ship first.
Row groups are Arrow IPC RecordBatches: one
qualifying `MultiRow` record body becomes one IPC
stream with one schema message followed by N dense
row-group RecordBatch messages over the same physical
schema. **Paging engages only when both thresholds
are met**: `row_count >= min_paged_record_rows`
**AND** estimated bytes `>= min_paged_record_bytes`;
fall below either, single-batch encoding stands.
**`RecordPageDirectory` lives in IPC schema
`custom_metadata`** under literal key
`storage.row_group_directory` — encrypted with the
record body, authenticated by ADR 020 chunk AEAD; not
in the ADR 022 footer (cleartext leakage), not a
synthetic Arrow column (false row semantics).
Directory carries `RowGroupDirectoryEntry` per row
group, `BufferPageDirectoryEntry` per
`(row_group_index, ipc_column_ordinal,
ipc_buffer_ordinal)` triple — the V1 per-buffer
addressing surface — and `RowGroupColumnStats` per
row group for ADR 039-supported types.
**Three-phase fetch planner** extends ADR 044's two
phases: Phase A (metadata + directory probe), Phase
B (stats prune; predicate-column fetch when stats
insufficient → derive per-group survival masks),
Phase C (projected fetch over surviving groups,
zip Phase B columns + Phase C columns by IPC
ordinal, apply per-group mask, emit dense filtered
batch — PBC is **never re-evaluated** post-Phase-B).
**Conservative PBC predicate classifier** with
asymmetric AND/OR rule: conjunction skips when *any*
branch proves disjoint (unsupported sibling branches
do not block); disjunction skips only when *every*
branch proves disjoint; LIKE / IN / BETWEEN-with-cast
keep the group at top level, collapse to Keep
without blocking siblings under conjunction.
**`L2FetchStrategy`** four-leg dispatch:
`RowGroup(RowGroupFetchPlan) | Columnar(plan) |
WholeRecord | Fallback(reason)`. Closed 3-code
`RowGroupRejectCode` taxonomy
(`PageDirectoryParseError`,
`RowGroupRangeOverflow`, `LayoutUnsupported`);
authentication / parse / decode failures route to
storage errors, never the `Fallback(...)` branch.
Bounded `storage_l2_row_group_fetch_*` metric family
with `{strategy, reason, decision, kind,
violation_kind}` enumerations. V1 defaults:
`target_row_group_rows = 4_096`,
`max_row_group_rows = 8_192`,
`min_paged_record_rows = 8_192`,
`min_paged_record_bytes = 512 KiB`,
`max_selected_row_group_fraction_micros = 250_000`
(0.25), `max_selected_byte_fraction_micros = 500_000`
(0.50), `max_ranges_per_record = 16`,
`max_parallel_range_gets = 4`. Writer-side gated by
node-local `RowGroupWriterConfig` (default off,
opt-in after Phase 10 accepted the revised empirical
writer-cost gate); reader-side automatic on
directory presence — pre-056 sealed records (single-
RecordBatch IPC, no custom_metadata key) read
transparently via ADR 044 column fetch. ADR 052
secondary-index hits map `RecordRowIndex` → row group
via the directory; identity-axis sidecar shape
unchanged. ADR 055 LSM compaction inherits via
out-of-place sidecar rebuild and per-output-slab
directory recomputation under matching writer config
— cross-replica byte-equality holds, visible-state
digest invariant. CDC, recovery, rehydration are
byte-neutral (whole IPC stream decode). No new wire
`MsgType`, no new `FRAME_FLAG_*`, no new
`NodeCapability` bit, no Cargo feature, no new digest
domain, no `ObjectStore` trait change, no envelope
`format_version` bump, no ADR 022 footer extension,
no descriptor field, no synthetic Arrow column for
the directory, no LZ4 frame block-independence
mutation, no custom partial-buffer materializer
(stock Arrow IPC reader handles row-group decode).
**27 architectural lockdown guards** in
`tests/architecture_test.rs`, including two
ADR-057-scoped guards (no `partial_buffer_materialize`
symbols and no `mini_page_*` fields in ADR 056's
surface — those land in the encoding module)
and one Phase-5 guard against post-Phase-B PBC
re-evaluation. See
`plans/adr/056_row_group_fetch_and_page_directory.md`,
`plans/impl/056_row_group_fetch_and_page_directory.md`,
and
`plans/impl/056_row_group_fetch_bench_results.md`.
Implementation and Phase 10 bench evidence have
landed. The original `<= 15%` writer-cost target was
revised to an empirical `<= 25%` gate on the largest
legal representative fixture; wide threshold-margin
fixtures are documented as opt-in writer-cost
warnings. This bench artifact is now the input shape
for ADR 057.

ADR 055 (LSM-Style Leveled Compaction Substrate) is
the immediate predecessor and the substrate ADR 056
inherits for compaction-aware writers. Per-locality
compactor; four levels (`L0..L3`); HLC-disjoint
within each level; deterministic compaction so
replicas reproduce the same logical output set;
elision floor IS ADR 048's retention floor (no
separate compaction floor); manifest-snapshot pins
as the sole MVCC mechanism (no `relocated_reads`
shim); sidecar rebuild composes via ADR 052 §9
(replace-by-new); cold-rebuild visible-domain audit
over localized manifest views as the V1 install
gate; three V1 reject codes (`ManifestRace`,
`SidecarDigestMismatch`, `VisibleDigestInvariance`).
See `plans/adr/055_lsm_leveled_compaction_substrate.md`
and `plans/impl/055_lsm_leveled_compaction_substrate.md`.
Implementation phases pending; full small / medium /
large bench evidence is the promotion-to-shipped
gate.

## Prioritization philosophy

Wendelon is being sequenced as a complete-system launch rather
than as an incrementally exposed service. That changes the
roadmap constraint. In a live product, near-term user pain,
production evidence, and delivery commitments would rightly
pull high-impact access features forward. In this pre-launch
phase, those signals inform the design, but they do not have to
dictate the order of construction.

The ordering principle for this stretch is **software
composition**. The goal is to **minimize cumulative rework
across the whole tree before launch**: choose the order that
lets later ADRs build on settled foundations instead of
revisiting earlier work because a later ADR changed a core
shape. A feature wants to come *earlier* when it settles a
foundational type or invariant (`Identity`, `NodeId`, slab
footer, descriptor fingerprint) that many later features build
on. A feature wants to come *later* when it is a consumer or
refinement of those surfaces and composes cleanly once they are
stable.

This does not make LSM compaction, vector embeddings, or
encoding refinements less important. They remain central
warehouse capabilities. The sequencing claim is narrower: they
should consume the algebraic substrate (`StateDigest`),
lifecycle layer (tombstones, retention), topology decision,
identity / descriptor cleanup, access-pattern surface
(secondary indexes), wire-compression policy, and CDC substrate
rather than force those foundations to be retrofitted
afterward.

**Toron-pivot stretch (current).** Past the ADR 055 milestone the
roadmap turns toward compute-adjacent storage primitives in
preparation for the Toron compute layer. The ordering goal in
this stretch is to land the storage hierarchy that
analytical-scan workloads (BigQuery-killer territory) need
before opening Toron-side surface. Concretely: row-group fetch
+ page directory (ADR 056), then per-buffer pages + encoding
cascades (ADR 057), then compute-over-encoded-data
(ADR 058 / COCD). CDC follow-up ADRs and vector embeddings
are deliberately deferred behind the storage hierarchy in this
stretch — not because they are smaller, but because the Toron
pivot pulls compute-adjacent primitives ahead of OLTP-shaped
access surfaces (vectors, triggers, webhooks).

## Forward sequence (council-converged)

Decided across multiple iterations between Claude, Codex, and
Gemini. Steps 1–14 are promoted (in `plans/adr/`); step #14
is design-promoted with implementation phases pending. The
forward shape continues at #15.

1. **`StateDigest` substrate** — ADR 046 promoted.
2. **Row-level tombstones on append-only** — ADR 047 promoted.
3. **Per-table retention policy** — ADR 048 promoted.
4. **Algebraic topology gate** — closed 2026-04-29 with
   Outcome B (opaque `NodeId`); see brainstorm 015 closure.
   Splits to brainstorm 018 (FROST-WAL authority) and
   brainstorm 019 (Sybil-resistance threat model), both
   off the active queue.
5. **Content-hash identity (`AppendOnlyContentHash`)** —
   ADR 049 promoted.
6. **PK-keyed row-delete via supersession** — ADR 050
   promoted.
7. **Logical / physical descriptor fingerprint split** —
   ADR 051 promoted.
8. **Secondary indexes on non-clustering columns** —
   ADR 052 promoted.
9. **Wire-side Arrow IPC body compression** — ADR 053
   promoted.
10. **CDC subscription substrate** — ADR 054 promoted.
    Substrate-only; triggers, PBC filter pushdown,
    webhooks, and continuous historical-plus-live queries
    are explicit follow-up ADRs.
11. **LSM-style leveled compaction substrate** — ADR 055
    promoted. Substrate-only; size-tiered fallback,
    subcompaction, bloom filters, cross-replica
    deduplication, manual-trigger RPC, L4+ extension,
    and direct already-tiered compaction are explicit
    follow-up ADRs (each consumes the substrate).

12. **Row-group fetch and layered page directory** —
    ADR 056 promoted.
    Outer layer of the storage hierarchy: Arrow IPC
    RecordBatch row groups + per-row-group min/max/null-
    count stats + `RecordPageDirectory` in IPC custom
    metadata; three-phase fetch planner with Phase B/C
    survival-mask handoff; asymmetric AND/OR predicate
    classifier; `L2FetchStrategy::RowGroup` four-leg
    dispatch; ADR 052 secondary-index integration; ADR
    055 LSM compaction integration; 27 architectural
    lockdown guards. Phase 10 bench evidence accepted
    the empirical writer-cost gate and opens ADR 057.
    See *Where we are*.

13. **Per-buffer pages + encoding cascades inside
    row-group buffers** — ADR 057 promoted.
    Council-converged and implemented. Inner layer of the storage
    hierarchy. Closed 8-variant cascade catalog
    (`Plain`, `Lz4Frame`, `BitPacked`, `Rle`,
    `Dictionary`, `Delta`, `Fsst`, `Alp`) with
    one-level composition via `EncodingLeafCascade`;
    `EncodingDirectory` in IPC custom_metadata under
    key `storage.encoding_directory`, sibling to
    ADR 056's row-group directory; compact directory
    shape with cascade dedup and per-buffer
    `page_end_offsets` prefix array (no per-page
    serialized rows); `EncodingPageDescriptor` is transient
    planner-only; buffer-relative byte offsets;
    page emission required for `BitPacked`, `Rle`,
    `Dictionary`; Phase D materialization step internal
    to `L2FetchStrategy::RowGroup(plan)` leg; custom
    partial-buffer materializer in `src/storage/encoding/`;
    ADR 052 secondary-index integration via SIMD-page
    fast path; ADR 055 LSM compaction recomputes
    cascades deterministically; node-local
    `EncodingWriterConfig` activation (no
    `RecordBodyCompressionPolicy::EncodingCascade` descriptor
    variant); 5-code `EncodingRejectCode` taxonomy; ~30
    architectural lockdown guards. Decode-only V1;
    compute-over-encoded-data deferred to ADR 058.
    Phase 10 bench evidence accepted: per-cascade compression
    ratios on int / float / low-card UTF-8 / mixed,
    point-read latency reduction via SIMD-page fast
    path, full-buffer materialization cost vs. ADR 056,
    writer-cost defended by compression and read-side
    wins, cross-replica determinism, mixed legacy /
    Encoding slab read throughput, secondary-index page
    fast path, and CSB bypass evidence. This opens ADR
    058. See *Where we are*.

14. **Compute over encoded data (COCD)** — ADR 058
    promoted. Council-converged across six iterations and
    design-promoted; implementation phases pending.
    Innermost layer of the storage hierarchy. Closed
    6-variant predicate evaluator catalog (`BitPacked`,
    `RlePlain`, `RleBitPacked`, `DictionaryPlain`,
    `DictionaryBitPacked`, `Delta`) and closed 8-kernel
    aggregate catalog (`count_star`, `count_non_null`,
    `min`, `max`, `signed_sum`, `unsigned_sum`,
    `signed_avg`, `unsigned_avg`); no new public PBC
    opcodes (Toron contract byte-identical); node-local
    `EncodingComputeConfig` activation (no descriptor
    field, no `NodeCapability` bit); Exact / Prefilter /
    DenseFallback predicate plan modes; `EncodedRowMask`
    tagged enum (`All` / `None` / `Bitmap` / `Runs`);
    page-grain dispatch default for page-emitting
    cascades; dual economic gates (0.25 leaf-level
    selectivity + 0.10 Prefilter rejection); aggregate
    folding ungated by predicate selectivity and runs
    after ADR 040 CSB direct-answer ordering; 5-code
    reject taxonomy; ADR 052 secondary-index confirm-
    step; ADR 057 directory shape unchanged; ~27
    architectural lockdown guards. `Fsst` / `Alp`
    encoded evaluation deferred. Two aggregate-deferral
    classes: dense-supported but COCD-deferred (Utf8 /
    Binary `min`/`max`, `APPROX_DISTINCT`,
    `QUANTILE_KLL` — fall back to dense AGG, no ADR 040
    surface change required) vs. not-yet-in-ADR-040
    (exact `count_distinct`, float / decimal aggregates,
    time/timestamp sum/avg, GROUP BY — each requires an
    ADR 040 expansion first). Reader-CPU bench gate is
    the promotion-to-shipped gate. See *Where we are*.

15. **Capacity-tier compression + serverless / erasure-
    coded follow-ups (rest of the old #14 bundle).**
    Independent; each ships its own contract surface:
    - **zstd capacity-tier codec + `CompressionLevel`
      newtype.** Codec-catalog extension; ADR 053 / 043
      wire vs. body composition; new
      `CompressionLevel(u8)` newtype-unit. Composes
      orthogonally with the row-group / per-buffer-page
      hierarchy.
    - **Serverless PBC at L2** *(brainstorm 010.)*
      Lambda-like remote PBC executor; consumes the same
      chunk-aligned authenticated range-read substrate
      ADR 044 / 056 surfaces.
    - **L1 inline erasure coding** *(brainstorm 003.)*
      Reed-Solomon / Cauchy across L1 NVMe replicas;
      composes with the LSM-tier sweeper boundary
      (ADR 055 §"No StorageTier on the substrate").
    *Each is its own ADR; ordering opportunistic and
    independent of #13 / #14 sequencing.*

16. **CDC follow-up ADRs (consume ADR 054 substrate)** —
    deferred behind the storage hierarchy in the Toron-
    pivot stretch. Independent; each ships its own
    contract surface:
    - **PBC filter pushdown on subscriptions.** Trailing-
      section bytecode on `CDC_SUBSCRIBE_REQ`; new
      `CDC_WATERMARK` MsgType for low-selectivity streams
      (every emitted event advances the cursor at
      substrate, but watermarks become load-bearing once
      filters can drop events without advancing); filter
      reject taxonomy; budget accounting; no-match
      checkpoint advancement. Reuses ADR 036 PBC validator
      family.
    - **Continuous historical-plus-live queries.** Either
      composes externally (L2 read at HLC=T plus CDC
      subscription with cursor matching T) or introduces a
      dedicated streaming `QUERY_REQ` / `QUERY_RESP` shape.
    - **Post-commit PBC triggers.** New `TriggerCatalog`
      (mirroring ADR 052 `SecondaryIndexCatalog` shape),
      `SchemaObjectKind::Trigger`, trigger worker that runs
      PBC over CDC events and appends to descriptor-bound
      append-only sinks; sink-write idempotency; indirect
      cycle detection; pause-resume operator surface.
    - **Synchronous PBC triggers at admission.**
      Admission-time PBC hook with its own CPU budget and
      failure-domain rules; deterministic PBC requirement.
    - **Webhook worker + `__webhook_events__` system table
      (or equivalent).** Internal HTTP retry / DLQ
      semantics; webhook worker is a CDC consumer.
    - **Server-side durable cursor commit (consumer-group
      semantics).** New `CDC_CURSOR_COMMIT_REQ` / `_RESP`
      MsgType pair; substrate cursor unchanged.
    - **Exactly-once delivery contract.** Server-side
      cursor commit + per-frame ack RPC.
    - **Cross-replica cursor handoff (zero-reconnect
      failover).** Server-side durable cursor + replica
      synchronization.
    - **Multi-table / tenant-firehose subscription scope.**
      Cursor extends to carry `TableObjectId` explicitly;
      V1's two-field scope is forward-compatible.
    - **Before-image CDC payloads.** Adds `before_*`
      columns to the CDC event row schema; substrate
      cursor / digest domain unchanged.
    - **Event-history digest domain
      (`digest.cdc.event.v1`).** Adds prefix-membership
      proof when a consumer (e.g., downstream secondary-
      index replicator) needs it independent of visible
      state.
    - **Consumer-friendly higher-level `CdcEventKind`
      view.** Layered on top of substrate version-entries;
      derives insert / update / delete / resurrection
      semantics from row-axis synthetic columns.
    - **Per-table CDC opt-in / opt-out** (and per-table
      retention extension beyond source-table retention).
    *Each is its own ADR; ordering opportunistic.*

17. **Vector embeddings + vector-form identity / locality
    PBC opcodes** *(brainstorm 002.)* ANN as a first-class
    index family, composing on the secondary-index
    substrate (#8) and LSM (#11). **Set aside in the
    Toron-pivot stretch** — OLAP focus pulls
    compute-adjacent primitives ahead. Reopen after the
    storage hierarchy (ADR 056 / 057 / 058) is in
    production use and Toron compute-layer requirements
    surface concrete vector workload signals. *Large,
    multi-ADR.*

18. **Refinement plumbing (opportunistic, no ordering
    constraint).** AGG follow-ups (float canonical,
    Decimal SUM/AVG, GROUP BY *(medium-large)*,
    string/binary quantiles, exact COUNT(DISTINCT),
    `COUNT(DISTINCT __row_key_id__)` shortcut, CSB
    visibility-metadata consolidation). Hilbert locality
    variant. Append-only `SingleRow` low-latency path.
    `IntraBatchIndex` widening to u64. Provider-native
    multi-range API in `ObjectStore`.
    `RecordBufferDirectory` cache.

## What not to do next

- **Don't bundle the CDC follow-up ADRs into one
  mega-ADR.** Each consumer of the ADR 054 substrate
  (filters, triggers, webhooks, continuous queries,
  durable cursor commit, multi-table firehose, before-
  images, event-history digest) has its own contract
  surface; bundling defers real decisions behind a larger
  one. Land them independently against the substrate.
- **Don't introduce a separate compaction floor.** ADR
  055 §"Architectural inextricability" pins the elision
  floor as identical to ADR 048's retention floor. A
  parallel parameter would diverge under operational
  drift and create two reachability contracts.
  Operator-tunable retention is the lever; "compact
  more aggressively than retention permits" is not a
  shape V1 supports.
- **Don't add an LSM `relocated_reads` shim.** ADR 055
  §7 commits to manifest-snapshot pins as the sole MVCC
  mechanism; cached `RecordLocation` references retire
  with the old sidecars (rebuilt out-of-place during
  compaction). Reintroducing per-row relocation rows
  for LSM would add chain-management complexity without
  improving CDC catchup or iterator safety.
- **Don't replace the cold-rebuild visible-domain audit
  with a streaming-only check.** ADR 055 §6 / §9 commit
  to localized cold rebuild over input/output slab
  views as the V1 install gate — equivalent O(touched
  bytes) cost, strictly stronger bug-catching (catches
  materialization, not just elision). Streaming
  `visible_contrib(elision_set) == identity` is a debug
  / bench tool, not the gate.
- **Don't widen `CompactionMergeKey` with locality /
  tenant / table prefix.** Compaction is per-locality
  by construction; table ownership is read from row-
  axis synthetic columns at retention-floor lookup, not
  from the merge key. The narrow `(HLC, RowKeyId,
  RecordRowIndex)` matches the data-ring slab seek key
  exactly.
- **Don't put `StorageTier` on the LSM substrate.**
  Tier moves are an orthogonal sweeper concern. Coupling
  them would force the substrate to coordinate with the
  sweeper without buying capability.
- **Don't bundle the LSM follow-up ADRs.** Size-tiered
  picker, subcompaction, bloom filters, cross-replica
  deduplication, manual-trigger RPC, L4+ extension, and
  direct already-tiered compaction each consume the ADR
  055 substrate independently. Bundling defers real
  decisions behind a larger one.
- **Don't promote stabilization watch items into roadmap
  items.** Stabilization watch — observability metrics,
  architectural-test guards, post-merge dashboards — lives
  in runbooks and tests, not as a roadmap feature.
  Decision-enabling measurement (e.g., the CDC watermark
  hold-time bench at multiple heartbeat cadences) belongs
  inside the owning ADR.
- **Don't re-open the topology gate without citing the
  rework budget recorded in brainstorm 015's closure
  status.** Speculative threat models, aesthetic
  preferences, and the desire to reuse curve machinery
  are not sufficient grounds. A re-open requires concrete
  verifier requirements or operational evidence that
  opaque-handle plus membership-bound signing forces more
  rework than a `NodeId`-shape migration.
- **Don't add a `CdcEventKind` enum to the substrate.**
  Consumer-friendly insert / update / delete / resurrection
  semantics layer on top in a future higher-level view ADR
  (#12 sub-bullet). The substrate stays storage-axis-
  aligned with the version-entry grain.
- **Don't expose `LocalityId` on the CDC cursor.** The
  identity-axis cursor is the load-bearing decision; the
  safe-emission watermark plus heartbeat contract is what
  makes global cursor monotonicity work without it.
- **Don't bundle ADR 056 / 057 / 058 into one mega-ADR.**
  The storage hierarchy is a layered substrate by
  design; each layer is a substantial decision in its
  own right: ADR 056 is row groups + stats, ADR 057 is
  per-buffer pages + encoding cascades + custom partial-
  buffer materializer, and ADR 058 is compute over
  encoded data. Bundling them defers each layer's review
  behind the next, weakens the lockdown story (ADR 056's
  scoped guards depend on a clean ADR 057 boundary), and
  loses the Parquet / Vortex precedent of evolving inner
  layers across multiple specs. Each layer ships as its
  own ADR.
- **Don't reopen ADR 058 with public PBC opcodes
  (`EVAL_DICT`, `EVAL_RLE`, `EVAL_BITPACKED_RANGE`).**
  Brainstorm 011 §2 framed COCD as opcode extensions; the
  council convergence at iter 6 rejects this uniformly.
  ADR 036 §7 ("PBC capability negotiation becomes
  mandatory before new opcodes") and §11 ("targetability
  diagnostics belong on Toron expression nodes") together
  say: new public opcodes are a Toron contract concern,
  not a storage optimization concern. Toron does not need
  to know whether Wendelon evaluated against a dictionary
  or against a materialized array — the answer is byte-
  identical. The dispatch happens internally; the opcode
  does not. Reopening requires concrete Toron-side
  evidence that the dispatch decision needs to be
  controllable from the query session, plus full ADR 036
  capability-negotiation work.
- **Don't ship ADR 058 without aggregate folding in V1.**
  Codex iter-1 deferred aggregates and iter-2 retreated
  again, but the iter-3 reasoning carried: mask
  infrastructure is the load-bearing substrate, and the
  common exact AGG operations reuse the same encoded
  scans (`count_star` is mask popcount, `count_non_null`
  is mask plus validity, `min`/`max`/`sum`/`avg` walk the
  cascade once with the mask filtering). Splitting
  predicate and aggregate substrates would force a
  near-immediate ADR 040 follow-up that reopens the same
  mask and cascade correctness questions. Council
  convergence at iter 5 / iter 6: aggregates ship with the
  predicate substrate.
- **Don't ship ADR 058 with a single economic gate or no
  gate.** The iter-3 / iter-4 oscillation (blanket gate →
  no gate → Prefilter-only gate → dual gates) settled on
  dual gates because they handle distinct regression
  cases. The leaf-level selectivity gate at 0.25 defends
  against high-selectivity Exact regressions (encoded eval
  produces a mostly-true mask while the projection fetch
  still pulls most rows; dense materialize wins by reusing
  one fetch). The Prefilter rejection gate at 0.10 defends
  against unprofitable mixed-conjunction narrowing
  (encoded leaves must drop ≥ 10% of rows to amortize
  residual narrowing). Aggregate folding has neither gate
  — the mask is the input, folding is strictly cheaper
  than materializing then folding.
- **Don't open ADR 058 with descriptor-policy
  activation.** Same physical-deployment-knob argument
  ADR 057 §"Why node-local writer config" makes: COCD
  does not change logical rows, table semantics, PBC
  programs, or Toron contracts. A descriptor field would
  make a physical deployment knob look like logical
  schema policy. Deployment-wide `EncodingComputeConfig`
  is the council-converged shape.
- **Don't extend ADR 057's directory shape for COCD.**
  COCD reads existing `BufferEncoding` /
  `EncodingCascade` metadata; no new fields, no per-page
  prefix-sum side tables for Delta range, no page-level
  min/max/null stats. Those additions are follow-up ADR
  territory and require their own bench-driven
  motivation.
- **Don't ship ADR 057 with `Constant` or
  `FrameOfReference` cascade variants.** Iter 2 of
  the council ADR 057 process surfaced these as
  candidate variants; iter 3 rejected them as
  redundant. `Constant` is `Rle { run_count: 1, ... }`
  — same encoded bytes, one variant fewer in the
  catalog. `FrameOfReference` is subsumed by
  `BitPacked { bits, base }` where `base` is the
  Frame-of-Reference offset; the variant split was a
  category mistake. The 8-cascade catalog
  (`Plain`, `Lz4Frame`, `BitPacked`, `Rle`,
  `Dictionary`, `Delta`, `Fsst`, `Alp`) is the
  council-converged set; new variants require a
  follow-up ADR.
- **Don't open ADR 057 with descriptor-policy
  activation (`RecordBodyCompressionPolicy::EncodingCascade`).**
  The activation surface was the load-bearing fork
  across iters 1–4 of the ADR 057 council process.
  Iter 4 converged on node-local `EncodingWriterConfig`
  for principled reasons: encoding cascades do not change logical
  rows, table semantics, PBC programs, or Toron
  contracts; a descriptor field would make a physical
  deployment choice look like logical schema policy;
  cross-replica byte-equality is already enforced
  via deployment-wide config; readers auto-detect
  via directory presence. Reopening this with a
  `RecordBodyCompressionPolicy::EncodingCascade` variant requires
  a re-vote with documented evidence that the
  descriptor framing buys capability the node-local
  shape doesn't.

## Convergence summary

```text
StateDigest substrate                            (ADR 046)
  → tombstones (validate algebra @ deletion)     (ADR 047)
  → retention (validate algebra @ compaction)    (ADR 048)
  → topology gate CLOSED — Outcome B (opaque NodeId)
  → content-hash identity                        (ADR 049)
  → PK-keyed row delete via supersession         (ADR 050)
  → descriptor fingerprint split                 (ADR 051)
  → secondary indexes                            (ADR 052)
  → wire-side Arrow IPC body compression         (ADR 053)
  → CDC subscription substrate                   (ADR 054)
  → LSM-style leveled compaction substrate       (ADR 055)
  → row-group fetch + page directory             (ADR 056)
  → per-buffer pages + encoding cascades     (ADR 057)
  → compute over encoded data (COCD)             (ADR 058)
  → capacity-tier compression / serverless / EC  (rest of old #14 bundle)  ← here
  → CDC follow-up ADRs (deferred this stretch)
  → vector embeddings (set aside this stretch)
  → refinement plumbing
```

Design tip is past ADR 058: the storage hierarchy is
design-complete (ADR 056 row groups, ADR 057 per-buffer
pages + encoding cascades, ADR 058 compute over encoded
data). ADR 056 and ADR 057 are shipped with Phase 10 bench
evidence; ADR 058 is design-promoted with implementation
phases pending. The Toron-pivot stretch closes its primary
storage-hierarchy goal once ADR 058 implementation +
Phase 8 bench evidence land. **The council's lean on next
pickup:** ADR 058 implementation iterations against the
locked design contract — the substrate, dispatch table,
plan modes, and acceptance gates are all settled at the
council level; Phase 0 audit through Phase 9 architectural
lockdown should follow the impl plan's phase ordering. CDC
follow-ups (#16) and vector embeddings (#17) remain
deferred behind the storage hierarchy in this stretch.
