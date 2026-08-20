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

## Status

Storage hierarchy is shipped through ADR 069 (encoded-validity
sparse projection, 2026-05-12). ADR 069 is Shipped and promoted
with evidence at
`plans/impl/0069_encoded_validity_sparse_projection_bench_results.md`;
the reader now initializes `Enabled` encoded-validity sparse
projection coverage and pins the `RowGroupRowCountMismatch`
guard. ADR 070 (all-null page-skip for nullable sparse
projection) is implemented with evidence at
`plans/impl/0070_all_null_page_skip_sparse_projection_bench_results.md`
after four council iterations under `plans/wip/`; source plans
live at
`plans/adr/0070_all_null_page_skip_sparse_projection.md` and
`plans/impl/0070_all_null_page_skip_sparse_projection.md`. The
Toron-pivot stretch continues to pull compute-adjacent storage
primitives ahead of OLTP-shaped access surfaces.

**ADR 071 (`nilket wendelon inspect`) implemented — ops/observability
CLI composition, not a storage-engine continuation.** Planning and
implementation are complete
(`plans/adr/0071_nilket_wendelon_inspect_real_wiring.md` +
`plans/impl/0071_nilket_wendelon_inspect_real_wiring.md`; evidence in
`plans/impl/0071_nilket_wendelon_inspect_real_wiring_evidence.md`).
It adds a one-shot
operator CLI that dials Wendelon's existing mTLS `/ready` listener
(ADR 027 observability posture), composing over the Toron-sequence
cross-ring substrates ADR 047 `tls-pki` + ADR 048
`observability-http`. Drafted via the Toron operator-plane council
process (3 iterations, Codex+Claude 2-of-3) and relocated to the
Wendelon sequence at promotion because Wendelon owns the consumer
ADR. It completes the symmetric operator-plane
inspect surface (`nilket toron inspect` (Toron ADR 046) +
`nilket wendelon inspect` (this ADR)). Zero daemon-side change;
all code lands in `crates/wendelon`.

**ADR 072 (Table Descriptor Snapshot Producer) implemented — the
producer half of cross-ring descriptor liveness, not a
storage-engine continuation.** Plans at
`plans/adr/0072_table_descriptor_snapshot_producer.md` +
`plans/impl/0072_table_descriptor_snapshot_producer.md`; evidence at
`plans/impl/0072_table_descriptor_snapshot_producer_evidence.md`. A read-only
Control Ring RPC (`MsgType::TABLE_DESCRIPTOR_GET_REQ` 0x3C /
`_RESP` 0x3D) answering `(tenant_id, namespace, table_name)` →
latest `TableSchemaDescriptor` (or not-found) against the Raft
**leader's** applied `TableSchemaCatalogV1::latest_by_name`, with
the full descriptor Postcard-encoded in the response trailing
section. **Authz:** a descriptor-get helper whose V1 role matrix
delegates to existing `TenantAdminRead` admission for Toron's current
no-claim `Operator` Wendelon identity, plus a tenant-isolation overlay
via `claimed_tenant_id` (tenant-scoped principals read only their own
tenant; a no-claim cluster actor may target any). **Serializable-at-
leader**, bypasses the Raft log (read-only against applied state);
followers return `NOT_LEADER` + `leader_hint`. Reuses existing Control
Ring reason codes (`NOT_FOUND`, `AUTHORIZATION_FAILED`,
`INVALID_CONFIG`) and adds aggregate-only metrics. Seeded by Toron ADR
052 §D15; drafted via the Toron operator-plane council (3 iterations,
3-of-3) and relocated to the Wendelon sequence at promotion (ADR 071
precedent). **14 decisions, 16 architecture guards + 3 convention
guards, 12 required fixtures + 1 optional.** Read-only; no consensus
write; no Data Ring change; no Toron production change. The **Toron
bind-miss consumer** that calls this producer (in-place
`sync_descriptor_for_tenant` registry merge, not ADR 051's fresh
constructor) is the next **Toron** ADR (053), cross-referenced from
the Toron roadmap.

## Shipped foundations

One-line index. Full context in each ADR; bench evidence under
`plans/impl/<id>_*_bench_results.md`.

- **ADR 046 `StateDigest` substrate** — algebraic identity
  primitive.
- **ADR 047 Row-level tombstones on append-only.**
- **ADR 048 Per-table retention policy.**
- **(Topology gate closed 2026-04-29 — Outcome B opaque
  `NodeId`.)**
- **ADR 049 Content-hash row identity
  (`AppendOnlyContentHash`).**
- **ADR 050 PK-keyed row-delete via supersession.**
- **ADR 051 Logical / physical descriptor fingerprint split.**
- **ADR 052 Secondary indexes on non-clustering columns.**
- **ADR 053 Wire-side Arrow IPC body compression.**
- **ADR 054 CDC subscription substrate.**
- **ADR 055 LSM-style leveled compaction substrate.**
- **ADR 056 Row-group fetch + layered page directory.** Outer
  layer of the storage hierarchy.
- **ADR 057 Per-buffer pages + encoding cascades.** Inner
  layer; 8-variant cascade catalog (`Plain`, `Lz4Frame`,
  `BitPacked`, `Rle`, `Dictionary`, `Delta`, `FsstWholeBuffer`,
  `Alp`) plus ADR 067's `FsstRowSafe` extension.
- **ADR 058 Compute over encoded data (COCD).** Innermost
  layer; predicate + aggregate evaluator catalogs.
- **ADR 059 Native per-cascade COCD kernels.** Default-on for
  narrowed evaluator set (`BitPacked`, `DictionaryPlain`,
  `DictionaryBitPacked`, `Delta`).
- **ADR 060 Sparse projection fetch narrowing.** Composes ADR
  058 / 059 exact masks with ADR 057 page metadata.
- **ADR 061 Page-aligned authenticated chunks + macro-block
  read coalescing.** AEAD / network two-layer decoupling.
- **ADR 062 Row-group sizing profile.** `PointRead` / `Scan`
  closed two-variant catalog.
- **ADR 063 Page-level predicate stats + predicate-fetch
  narrowing.** Three-state classification
  (`Disjoint` / `ExactMatch` / `Maybe`); `enabled = false`
  writer default.
- **ADR 064 Nullable sparse projection.** Raw byte-addressable
  validity only; lifts ADR 060's
  `NullableProjectedColumn` decline for fixed-width
  projection.
- **ADR 065 Dense predicate-zip with sparse projection.**
  Manager-side three-case dispatch (Case A mask-only / Case B
  predicate-output overlap / Case C no-overlap dense
  fallback).
- **ADR 066 FSST predicate evaluator (Strategy A).** Whole-
  buffer FSST streaming-token decode for back-compat reads.
- **ADR 067 Row-canonical FSST framing + Strategy B predicate
  evaluator.** Decode-avoiding predicate path; new
  `FsstRowSafe` cascade variant; `ENCODING_DIRECTORY_FORMAT_VERSION`
  bump.
- **ADR 068 Sample-based `FsstRowSafe` writer estimator.**
  Exact-guard-preserving baseline on directory-writer column-
  cache path.
- **ADR 069 Encoded-validity sparse projection.** Narrows
  ADR 064's `EncodedValidityBufferUnsupported` decline to
  admit `Rle { inner: Plain }`; page-local emission with 1:1
  value/validity alignment. Shipped and promoted with bench
  evidence at
  `plans/impl/0069_encoded_validity_sparse_projection_bench_results.md`;
  reader config now initializes `Enabled`; guard coverage
  includes `RowGroupRowCountMismatch`.

## Prioritization philosophy

Wendelon is being sequenced as a complete-system launch rather
than as an incrementally exposed service. In a live product,
near-term user pain, production evidence, and delivery
commitments would pull high-impact access features forward. In
this pre-launch phase, those signals inform the design but do
not dictate the order of construction.

The ordering principle is **software composition**: minimize
cumulative rework across the whole tree before launch. A
feature wants to come earlier when it settles a foundational
type or invariant that many later features build on; later
when it is a consumer or refinement of stable surfaces. LSM
compaction, vector embeddings, and encoding refinements remain
central — the claim is that they should consume the algebraic
substrate, lifecycle layer, topology decision, identity /
descriptor cleanup, access-pattern surface, wire-compression
policy, and CDC substrate rather than force those foundations
to be retrofitted afterward.

**Toron-pivot stretch (current).** Past the ADR 055 milestone
the roadmap turns toward compute-adjacent storage primitives
in preparation for the Toron compute layer. Concretely: row-
group fetch + page directory (ADR 056), then per-buffer pages
+ encoding cascades (ADR 057), then COCD substrate (ADR 058),
then native per-cascade kernels (ADR 059), then sparse
projection fetch narrowing (ADR 060), and onward through the
nullable / dense-zip / FSST / page-stats / encoded-validity
follow-ups. CDC follow-up ADRs and vector embeddings are
deliberately deferred behind the storage hierarchy in this
stretch.

**Ring-product placement (cross-ring convention:
`../../../toron/plans/conventions/ring-product-placement.md`).**
Storage-engine ADRs are Exterior Substrate and stay in this
Wendelon sequence. Operator-facing Wendelon features (Exterior
Composition — e.g. `nilket wendelon inspect`, ADR 071) also live
here, even when drafted through the Toron operator-plane council:
placement follows code-locality + server-side parentage (ADR 071's
`/ready` target is Wendelon ADR 027), not which wip dir the council
ran in. Only workspace-shared Interior substrate (`tls-pki`,
`observability-http`) lives in the Toron aggregator sequence.

## Forward sequence

**Implemented — ADR 074 Table-Query Trace Continuation
(cross-ring trace-continuation producer).** The Wendelon-owned half of
Toron ADR 068: 068 instrumented Toron OTLP traces but **stopped at
`toron.storage.scan`** because `wendelon-client::TableQueryOptions` /
`TableQueryRequestMeta` had no trace carrier. 074 carries Toron's trace
context through `TABLE_QUERY_REQ` via a **fixed binary tail** widening
`TableQueryRequestMeta` (136 → 168 B, prefix offsets pinned; flags
`0x01` present / `0x02` sampled — the 073 no-extension/no-`*V1` posture;
no traceparent string, no `ComputeTraceContextV1` on the wire). A
**three-state** `TableQueryTraceContext` (`Absent` / `Unsampled` /
`Sampled{trace_id, parent_span_id}`) carries the ADR 068 worker
no-resample rule to the ring boundary: `Unsampled` is authoritative
(Wendelon must not root-sample), `Absent` keeps direct-client behavior.
Wendelon **derives** its child span id
(`blake3("wendelon.table_query.span.v1" ‖ trace_id ‖ parent_span_id ‖
request_id)[0..8]`, nonzero) and **consumes the ADR 068 explicit-context
seam** (`otel.{trace_id,parent_span_id,span_id,sampled}`, adopted +
stripped) — **no new `observability::telemetry` seam**. Privacy: a
**sanitized** `wendelon.table.query` linked span (bounded classes + raw
magnitudes, no SQL/tenant/table/request-id), while the existing raw
`data_ring.table_query.request` span is **preserved as a Wendelon-local,
unlinked** span (siblings in different traces, never nested — so raw ids
never enter Toron's trace). Malformed carriers **fail open** to
unsampled (never reject `TABLE_QUERY_REQ`); `Absent` is
behavior-equivalent (the unchanged Wendelon OTLP HTTP/gRPC fixtures are
the regression net). **Producer-only — no Toron production change.**
Drafted in the Toron cross-ring council (working "ADR 069"), relocated +
renumbered to the Wendelon sequence at promotion per
`ring-product-placement.md` + the ADR 071/072/073 precedent. Council:
**2-of-3 (Codex, Claude)** — on the producer/consumer split (072/073
precedent) and local-span preservation (the iter-3 red-team); Gemini's
parallel iter-4 held fold-the-Toron-change / sanitized-only replacement,
resolved against by precedent and the Wendelon-local-observability
regression. **14 decisions, 16 architecture + 3 convention/meta guards,
16 fixtures.** Plans: `plans/adr/0074_table_query_trace_continuation.md` +
`plans/impl/0074_table_query_trace_continuation.md`; evidence:
`plans/impl/0074_table_query_trace_continuation_evidence.md`. **Next consumer:**
the Toron telemetry consumer ADR (Toron sequence) — populate
`TableQueryOptions.trace_context` from `ScanRequest` with
`toron.storage.scan` as parent (cross-referenced, not owned here).
Gemini's `UPDATE`-without-`WHERE` is the parallel **Toron ADR 069**.

**Implemented — ADR 073 Table-Query Request Cancellation
Producer Contract (cross-ring cancellation producer).** The
deferred half of Toron ADR 059's honesty boundary: 059 made Toron
worker cancellation real but did **not** cancel already-issued
Wendelon storage reads. Because `handle_table_query` materializes
(concatenates) the result *before* writing `TABLE_QUERY_RESP`, a
dropped Toron client stream is observed too late — the scan
already ran. 073 adds an **explicit** primitive for
`TABLE_QUERY_REQ`: a Wendelon-owned, per-attempt
`TableQueryRequestId` grown into the fixed `TableQueryRequestMeta`
as a Pod-compatible u128 newtype with zero rejected by validation
(no extension/compat path — no-premature-backward-compat); a
data-server active-request registry keyed `(TenantId,
TableQueryRequestId)` with RAII removal + a bounded TTL/count
pending-cancel tombstone; `TABLE_QUERY_CANCEL_REQ = 0x3E` /
`TABLE_QUERY_CANCEL_RESP = 0x3F` on the existing bidi data ring
(not `0x1F`, not Control Ring); a `tokio_util` `CancellationToken`
checked at execution boundaries (planning / migration-pull / scan
/ row-group / concat / encode — not in FSST/decompression
kernels); `ReadStatus::QUERY_CANCELLED` (no Arrow body);
oracle-safe `ACCEPTED`/`TERMINAL` cancel response; RAII resource
release; aggregate-only metrics; no rollback (reads are
side-effect-free). Stop-on-client-drop retained only as a
fallback. **Carries the ADR 059 review lessons** onto the storage
ring: `CancellationToken` (not a hand-rolled lost-wakeup-prone
`Notify`), bounded tombstone, oracle-safety, RAII. **Producer-only
— no Toron production change.** Seeded by Toron ADR 059;
**relocated to / owned in the Wendelon sequence at promotion** (the
ADR 072 precedent). Council: **3-of-3 (Claude, Codex, Gemini)** —
the strongest-aligned promotion of the cancellation arc; **19
decisions, 22 architecture guards + 1 meta guard, 19 required
fixtures**. Plans: `plans/adr/0073_table_query_request_cancellation.md`
+ `plans/impl/0073_table_query_request_cancellation.md`; evidence:
`plans/impl/0073_table_query_request_cancellation_evidence.md`.
**Next consumer:** Toron ADR 060 — wires Toron ADR 059
cancellation into this primitive (Toron sequence, cross-referenced,
not owned here).

**Implemented — ADR 072 Table Descriptor Snapshot Producer
(cross-ring descriptor-liveness producer).** Read-only Control Ring
RPC (0x3C/0x3D) returning the latest `TableSchemaDescriptor` by
`(tenant, namespace, table)` from the Raft leader's applied catalog;
descriptor-get authorization helper + tenant-isolation; full
descriptor in the trailing section with a pinned `descriptor_len`.
Seeded by Toron ADR 052 §D15; relocated to the Wendelon sequence at
promotion (ADR 071 precedent). Status block above; plans at
`plans/adr/072_*` + `plans/impl/072_*`; evidence at
`plans/impl/0072_table_descriptor_snapshot_producer_evidence.md`.
**Next consumer:** the
Toron bind-miss ADR (Toron sequence, 053) — cross-referenced, not
owned here.

**Implemented — ADR 071 `nilket wendelon inspect`
(ops/observability CLI composition).** Off the
storage-engine track: a one-shot operator CLI dialing Wendelon's
existing mTLS `/ready` listener (ADR 027), composing over
ADR 047 `tls-pki` + ADR 048 `observability-http`. Mirrors Toron
ADR 046 to the storage ring; body-authoritative readiness; exit
codes 0/1/2; empty ALPN; `ObservabilityReady` admission; zero
daemon-side change. **15 decisions, 16 architecture guards + 1
convention guard (in `crates/wendelon/tests/architecture_test.rs`),
15 required fixtures + 2 V1-evidence optional.** Plans:
`plans/adr/0071_nilket_wendelon_inspect_real_wiring.md` +
`plans/impl/0071_nilket_wendelon_inspect_real_wiring.md`; evidence:
`plans/impl/0071_nilket_wendelon_inspect_real_wiring_evidence.md`.
Drafted via the Toron operator-plane council; relocated to the
Wendelon sequence at promotion (Wendelon owns the consumer ADR). The
storage-engine forward items below continue independently.

1. **All-null page-skip for nullable sparse projection** —
   ADR 070 implemented after four Claude / Codex / Gemini
   council iterations. Plans at
   `plans/adr/0070_all_null_page_skip_sparse_projection.md`
   and
   `plans/impl/0070_all_null_page_skip_sparse_projection.md`;
   bench-results artifact at
   `plans/impl/0070_all_null_page_skip_sparse_projection_bench_results.md`.
   **Metadata-only zero-RTT elision** via ADR 063 page
   stats: when
   `value_buf.page_stats[k].null_count == page_value_count[k]`
   for a selected nullable fixed-width value page, the
   planner tags `SelectedPageDisposition::AllNullElided` and
   omits both value and validity fetches; the materializer
   synthesizes canonical-null Arrow buffers locally. Lifts
   ADR 064's all-null deferral guard and removes ADR 069's
   pinned ADR-070 boundary guard. **Promotion oracle =
   canonical-null logical equality** for elided pages; byte-
   identical retained for non-elided. **Test-only drift
   sentinel** independently decodes underlying validity
   bytes in test fixtures and asserts observably all-zero
   over `page_value_count` bits, guarding the inherited
   ADR 063 trust contract without burdening production I/O.
   **V1 catalog = ADR 063 V1 ∩ ADR 060 / 064 sparse-
   projection value-column eligibility**: {`BitPacked`,
   `DictionaryPlain`, `DictionaryBitPacked`} ×
   {Int8-64, UInt8-64, Timestamp(ms/us/ns), Date32/64}.
   Boolean remains V2 because it is not in the current
   `fixed_width_value_bytes` sparse-projection value-column
   catalog. **Closed surface**: no new on-disk shape, no
   new `EncodingCascade` / `EncodingComputeEvaluatorKind` /
   `L2FetchStrategy` / reject / fallback variant, no IPC
   `custom_metadata` key, no
   `ENCODING_DIRECTORY_FORMAT_VERSION` bump, no wire /
   descriptor / PBC / capability / digest / `ObjectStore`
   trait change, no Cargo feature, no metric label, no
   operator-visible toggle. **No writer change** — consumes
   ADR 063's existing field. **ADR 063 enablement
   coupling**: V1 is operationally inert until ADR 063 is
   enabled deployment-wide; capability lands, engagement
   follows. **15 Phase 0 pins** (oracle contract, writer
   scope, eligibility catalog, dense null-slot audit,
   synthesis cost, multi-range coalescing, determinism,
   guard text, fixtures, bench home, disposition storage,
   ADR 063 enablement, Case B handoff, Boolean shape, per-
   column audit, drift sentinel pre-check, threshold
   calibration). **Promotion gates**: 12 hard (canonical-
   null oracle, byte-identical for non-elided, no value
   fetch, no validity fetch, no decoded-bit elision, per-
   column independence, non-nullable forbidden, ADR 063
   disabled control, mixed row group, determinism, drift
   sentinel, closed surface) plus 7 soft (elided byte
   fraction, mixed reduction, wall-clock speedup,
   synthesis cost, cross-validity parity, no-elision
   controls, range/GET count recorded with regressions
   justified by accompanying byte reduction).

2. **Capacity-tier compression + serverless / erasure-coded
   follow-ups (rest of the old #14 bundle).** Independent;
   each ships its own contract surface:
   - **zstd capacity-tier codec + `CompressionLevel`
     newtype.** Codec-catalog extension; ADR 053 / 043
     wire vs. body composition; new `CompressionLevel(u8)`
     newtype-unit. Composes orthogonally with the row-
     group / per-buffer-page hierarchy.
   - **Serverless PBC at L2** *(brainstorm 010.)* Lambda-
     like remote PBC executor; consumes the same chunk-
     aligned authenticated range-read substrate ADR 044 /
     056 surfaces; composes with ADR 061's per-slab
     `chunk_size_bytes` for sub-record decrypt grain.
   - **L1 inline erasure coding** *(brainstorm 003.)*
     Reed-Solomon / Cauchy across L1 NVMe replicas;
     composes with the LSM-tier sweeper boundary (ADR
     055 §"No StorageTier on the substrate").

3. **CDC follow-up ADRs (consume ADR 054 substrate)** —
   deferred behind the storage hierarchy in the Toron-
   pivot stretch. Independent; each ships its own contract
   surface:
   - **PBC filter pushdown on subscriptions.** Trailing-
     section bytecode on `CDC_SUBSCRIBE_REQ`; new
     `CDC_WATERMARK` MsgType for low-selectivity streams;
     filter reject taxonomy; budget accounting; no-match
     checkpoint advancement. Reuses ADR 036 PBC validator
     family.
   - **Continuous historical-plus-live queries.** Either
     composes externally (L2 read at HLC=T plus CDC
     subscription with cursor matching T) or introduces a
     dedicated streaming `QUERY_REQ` / `QUERY_RESP` shape.
   - **Post-commit PBC triggers.** New `TriggerCatalog`
     (mirroring ADR 052 `SecondaryIndexCatalog` shape),
     `SchemaObjectKind::Trigger`, trigger worker that
     runs PBC over CDC events and appends to descriptor-
     bound append-only sinks; sink-write idempotency;
     indirect cycle detection; pause-resume operator
     surface.
   - **Synchronous PBC triggers at admission.** Admission-
     time PBC hook with its own CPU budget and failure-
     domain rules; deterministic PBC requirement.
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
     proof when a consumer needs it independent of
     visible state.
   - **Consumer-friendly higher-level `CdcEventKind`
     view.** Layered on top of substrate version-entries;
     derives insert / update / delete / resurrection
     semantics from row-axis synthetic columns.
   - **Per-table CDC opt-in / opt-out** (and per-table
     retention extension beyond source-table retention).

4. **Vector embeddings + vector-form identity / locality
   PBC opcodes** *(brainstorm 002.)* ANN as a first-class
   index family, composing on the secondary-index
   substrate (ADR 052) and LSM (ADR 055). **Set aside in
   the Toron-pivot stretch** — OLAP focus pulls compute-
   adjacent primitives ahead. Reopen after the storage
   hierarchy is in production use and Toron compute-layer
   requirements surface concrete vector workload signals.
   *Large, multi-ADR.*

5. **Refinement plumbing (opportunistic, no ordering
   constraint).** AGG follow-ups (float canonical, Decimal
   SUM/AVG, GROUP BY *(medium-large)*, string/binary
   quantiles, exact COUNT(DISTINCT),
   `COUNT(DISTINCT __row_key_id__)` shortcut, CSB
   visibility-metadata consolidation). Hilbert locality
   variant. Append-only `SingleRow` low-latency path.
   `IntraBatchIndex` widening to u64. Provider-native
   multi-range API in `ObjectStore`. `RecordBufferDirectory`
   cache. ADR 057 `Constant` / `FrameOfReference` cascade
   variants are out of scope (subsumed by `Rle` and
   `BitPacked { base }`); a future ADR would need a
   separate evidence case.

6. **Storage-hierarchy V2 follow-ups (opportunistic).**
   - **Mixed-page all-null skip** via decoded validity +
     survival-mask intersection (ADR 070 V2).
   - **Variable-width nullable sparse projection** (ADR 064
     V2 boundary).
   - **Float / Time page-stat-backed elision** (ADR 063
     V2).
   - **Page stats on additional cascades** (`Rle`,
     `Delta`, `FsstRowSafe`, `Alp`).
   - **Sparse predicate-column fetch** (ADR 065 V2).
   - **Page-grain FSST** + **range / `LIKE` / regex /
     substring on FSST** + **FSST aggregate folds**
     (ADR 067 V2 family).
   - **ALP evaluator** + **RLE value-run format work**
     (ADR 058 / 059 V2 family).
   - **Cascade-field cleanup**: remove
     `symbol_table_byte_len` from `EncodingCascade::FsstWholeBuffer`
     (trivial follow-up; tracked from ADR 066 / 067).

7. **WAL body shape across a format-version boundary (named
   follow-up, ADR 085 family).** Raised by ADR 0087's
   `SNAPSHOT_FORMAT_VERSION` / `WAL_FORMAT_VERSION` bump 1 → 2 and
   the `fix: adr 0085 older versioned snapshot explains itself`
   repair. **Already handled:** WAL *framing* is length-delimited
   and version-gated — a record newer than this build refuses by
   name and says the segment is intact rather than corrupt, and a
   pre-versioning record still replays. **Not handled:** WAL record
   *bodies* carry serialized mutations, and ADR 0087 added a
   `serde(default)` tail field to `CtasSagaMutation::Provision`.
   Postcard is not self-describing, so a body written before that
   change has no bytes for the new field and deserializes into
   `UnexpectedEnd` exactly as the snapshot did — the framing hands
   the replayer a checksum-valid record whose payload this build
   cannot interpret. Did not surface in the local-profile incident
   because `nilket local --reset` discards the WAL with the
   snapshot, and because snapshot decode fails first. **Open
   questions:** does the replayer diagnose an old-shaped body the
   way `decode_snapshot_reader` now does (digest matched, artifact
   intact, older format version, no migrations), or a bare postcard
   error? Is per-record refusal the right granularity when a
   segment may mix records from either side of a bump? Is the
   snapshot's "no migrations" contract right at record level, where
   a record is replayable state rather than a whole state machine?
   Answering these decides WAL-level upgrade semantics and wants
   its own ADR, not a patch riding a fix commit.

## What not to do next

Enduring guardrails for upcoming work. Items tied to
specific shipped ADRs are recorded in those ADRs' §"What Not
To Do" sections.

- **Don't bundle the CDC follow-up ADRs into one mega-ADR.**
  Each consumer of the ADR 054 substrate has its own
  contract surface; bundling defers real decisions behind
  a larger one. Land them independently.
- **Don't bundle the LSM follow-up ADRs.** Size-tiered
  picker, subcompaction, bloom filters, cross-replica
  deduplication, manual-trigger RPC, L4+ extension, and
  direct already-tiered compaction each consume the ADR
  055 substrate independently.
- **Don't promote stabilization watch items into roadmap
  items.** Observability metrics, architectural-test
  guards, post-merge dashboards belong in runbooks and
  tests. Decision-enabling measurement belongs inside the
  owning ADR.
- **Don't re-open the topology gate without citing the
  rework budget recorded in brainstorm 015's closure
  status.** Speculative threat models, aesthetic
  preferences, and the desire to reuse curve machinery are
  not sufficient grounds.
- **Don't introduce operator-visible toggles for storage-
  hierarchy V2 follow-ups by default.** Wendelon V1 house
  style is gate-driven (matching ADR 062 deterministic
  profile emission, ADR 060 plan-time decline). Operator
  knobs require explicit operational-pressure evidence.
- **Don't add an `L2FetchStrategy` variant for an internal
  dispatch optimization.** ADR 060 / 064 / 065 / 069 / 070
  all stayed inside the existing `RowGroup` leg; a new
  top-level variant would split the planner's decision
  matrix and create path-dependent physical plans on the
  same logical query.
- **Don't surface internal planner decline reasons via
  metric labels or public fallback variants.** Plan-time
  routing facts live inside the planner module
  (`sparse_projection_fetch.rs` and siblings). They never
  become new fallback variants, metric labels, or escape
  via wire / descriptor / capability surface.
- **Don't widen the sparse projection type/cascade catalog
  outside the ADR pinning the eligibility intersection.**
  Each cascade × type extension wants its own evidence
  case (the ADR 063 → ADR 070 inheritance is the
  precedent).
- **Don't promise byte-identical output to dense fallback
  for null-slot value bytes on ADR 070 elided pages.**
  Canonical-null logical equality is the contract for
  elided pages; byte-identical applies only to non-elided
  pages.
- **Don't reopen ADR 058 with public PBC opcodes** (e.g.,
  `EVAL_DICT`, `EVAL_RLE`, `EVAL_BITPACKED_RANGE`). New
  public opcodes are a Toron contract concern, not a
  storage optimization concern. Reopening requires
  concrete Toron-side evidence plus full ADR 036
  capability-negotiation work.
- **Don't ship ADR 058 with a `simd_strategy` /
  `simd_width` / `kernel_dispatch_mode` config knob, a
  Cargo feature gate, or nightly rustc.** Closed-dispatch
  principle (ADR 058 §"Architectural inextricability"
  point 3).
- **Don't add new on-disk shape changes to ADR 057 for
  follow-up ADRs unless absolutely required.** ADR 063's
  `page_stats` field is the only post-ADR-057 additive on-
  disk change; ADR 067's `FsstRowSafe` cascade variant
  bumped the format version explicitly. Each new field /
  variant needs an explicit decision; default is no.
- **Don't put `StorageTier` on the LSM substrate.** Tier
  moves are an orthogonal sweeper concern. Coupling them
  would force the substrate to coordinate with the
  sweeper without buying capability.
- **Don't add an LSM `relocated_reads` shim.** ADR 055 §7
  commits to manifest-snapshot pins as the sole MVCC
  mechanism; cached `RecordLocation` references retire
  with the old sidecars (rebuilt out-of-place during
  compaction).
- **Don't add a `CdcEventKind` enum to the substrate.**
  Consumer-friendly insert / update / delete /
  resurrection semantics layer on top in a future higher-
  level view ADR. The substrate stays storage-axis-aligned
  with the version-entry grain.
- **Don't expose `LocalityId` on the CDC cursor.** The
  identity-axis cursor is the load-bearing decision; the
  safe-emission watermark plus heartbeat contract is what
  makes global cursor monotonicity work without it.
