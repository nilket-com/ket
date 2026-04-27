# Roadmap: from 043 forward (transient nav doc)

Author: Claude (AI Swarm Council)
Scope: transient navigation — replaces `roadmap-post-042.md`,
retires whenever `plans/wip/` is cleaned up again.
Written after ADRs 035, 036, 037, 038, 039, 040, 041, 042, 043
shipped.

## Where we are

The warehouse story now spans nine shipped ADRs:

- **ADR 035** — vectorized predicate-bytecode (PBC) executor.
- **ADR 036** — PBC as the versioned Toron→Wendelon predicate-
  pushdown contract.
- **ADR 037** — typed tenant tables, PK-derived row identity,
  `TableSpreadProjection`.
- **ADR 038** — column-aware locality V1 (Z-order),
  descriptor-authoritative.
- **ADR 039** — slab-footer column statistics V1.
- **ADR 040** — aggregate pushdown protocol (AGG); CSB
  short-circuit with `tombstone_metadata`.
- **ADR 041** — project-name decoupling from internal
  protocols and types.
- **ADR 042** — analytical multi-row slab fragments.
  `RecordLayoutPolicy::MultiRow` packs N-row Arrow batches
  into single slab records; storage-only synthetic columns
  carry per-row HLC, key identity, and routing locality.
  `SupersessionMetadata` extends the ADR 040 CSB short-circuit
  proof to cover MVCC version-hiding. Single coherent
  compaction (ADR 042 §13) is the convergence primitive
  every later ADR composes against.
- **ADR 043** — Arrow IPC body compression (LZ4_FRAME).
  Per-table opt-in storage compression policy,
  `RecordBodyCompressionPolicy::Lz4Frame`, defaulting to
  `Lz4Frame` on `MultiRow` tables and `Uncompressed` on
  `SingleRow`. Reuses Arrow IPC's native per-buffer
  `BodyCompression`; readers trust the IPC FlatBuffer for
  format authority, not the descriptor. Compress-then-
  encrypt order. Two-axis enforcement: `max_record_bytes`
  against the uncompressed IPC body (the decode-memory
  bound from ADR 042 §10), tenant quotas / stored bytes
  against the encoded body. No envelope `format_version`
  bump, no new node capability bit, no
  `SingleRowLayoutDescriptor` refactor, no reserved enum
  slots for future codecs. Single coherent compaction
  (ADR 042 §13) is the convergence primitive: descriptor
  policy edits do not run a dedicated migration sweep.

What's true now that wasn't true a week ago:

- `MultiRow` storage bytes shrink by 40–70% on the typical
  analytical schema. NVMe footprint, L2 object size, and
  L1↔L2 transfer bytes all fall by the same factor on
  admitted records.
- Per-buffer LZ4 means each Arrow column buffer compresses
  independently inside the IPC frame. Buffer independence
  is the load-bearing property the future L2 columnar
  fetch ADR depends on.
- The descriptor surface gains exactly one field
  (`record_body_compression_policy`) and one enum
  (`RecordBodyCompressionPolicy { Uncompressed, Lz4Frame }`).
  No reserved variants. Future codecs land in their own
  ADR with their own measured workload pull.
- Mixed-compression slabs are first-class: pre-043 sealed
  slabs read transparently alongside post-043 compressed
  records. Convergence happens through natural compaction,
  measured by the unlabeled
  `storage_compression_uncompressed_resident_bytes` gauge.
- Two storage-internal IPC encoders are explicit
  exclusions guarded by architectural tests:
  `encode_arrow_schema_ipc` (descriptor schema IPC must
  decode without the `ipc_compression` Cargo feature at
  boot) and the slab-footer CSB encoder (fixed-offset
  random access during admission). Wire-side IPCs
  (control / data servers, client) also stay on default
  options — ADR 043 is storage-resident only.

What 043 explicitly did *not* do, and why it matters for the
roadmap:

- **L2 cold-tier fetch granularity is unchanged.** A query
  touching 5 of 50 columns still Range-GETs whole admitted
  record bodies (chunk-aligned per ADR 020's 64 KiB AEAD
  chunks) and filters node-side. ADR 043 reduces *bytes
  per fetched record* by ~50%; it does not reduce the
  *unit of fetch*. The council's L2 pushdown concern stays
  open as ADR 044 (proposed): L2 columnar fetch (sovereign
  pushdown).
- **No provider-native filtering.** S3 Select and
  equivalents are precluded by ADR 020 envelope
  encryption: the storage provider sees only ciphertext.
  Switching L2 to Parquet plus provider-side encryption
  would be a storage architecture fork — its own ADR with
  its own zero-trust trade-off, not a substitute for 043.
- **No homerolled lightweight columnar encodings.**
  Brainstorm 011 V2 (FastLanes / ALP / FSST / RLE / page
  directories) and V3 (COCD) stay open; their pull
  argument depends on measured headroom that LZ4 leaves
  behind.
- **No descriptor-fingerprint split into logical and
  physical halves.** `record_body_compression_policy`
  joins `record_layout_policy` in the existing fingerprint;
  a split is a future ADR if enough physical-only fields
  accumulate.

## The near map

```
                          YOU ARE HERE
                              ▼
     ┌──────────────────────────────────────────────────────┐
     │ 035 / 036 / 037 / 038 / 039 / 040 / 041 / 042 / 043  │ ← shipped
     └────────────┬─────────────────────────────────────────┘
                  │
   ┌──────────────┼──────────────┐
   │              │              │
   ▼              ▼              ▼
 stabilization  ADR 044         ADR 045
 (1–2 weeks)   L2 columnar      PK-less
                fetch            append-only
                (sovereign       tables
                pushdown)
                                │
                                ▼
                            ADR 046
                         secondary indexes
                                │
                                ▼
                   ┌────────────┴────────────┐
                   │      post-046 menu      │
                   │ ordered by compound     │
                   │ value                   │
                   └────────────┬────────────┘
                                ▼
                AGG follow-ups → wire-frame compression
                  (when warranted) → Hilbert (when warranted)
                  → LSM leveled compaction (when warranted)
                  → LCE V2 / V3 / brainstorms
```

The renumbering from `roadmap-post-042.md`:

| Old number | New number | Scope |
|---|---|---|
| 043 (proposed) | **043 (shipped)** | Arrow IPC body compression (LZ4) |
| — | **044 (proposed)** | L2 columnar fetch (sovereign pushdown) — *new* |
| 044 (proposed) | **045 (proposed)** | PK-less append-only tables |
| 045 (proposed) | **046 (proposed)** | Secondary indexes on non-clustering columns |

ADR 042 references "ADR 043 (proposed): secondary indexes" in
its references / status table. The aggregation plan's
ratification commit (`plans/wip/aggregation_plan_043.md` §2)
fixes that to "ADR 046 (proposed)" in the same commit that
lands ADR 043 canonical.

## Immediate (1–2 weeks post-merge)

Let 043 settle. New watch items the LZ4 body compression
introduces, plus carryovers from 042 that remain relevant.

### New under 043

1. **Compression density.**
   `storage_compression_ratio{codec="lz4_frame"}`
   distribution. Target band: 0.3–0.6 (40–70% reduction)
   on the default analytical schema. A tenant whose ratio
   sits near or above 1.0 is a candidate for an opt-out
   back to `Uncompressed` — high-entropy payloads (already
   compressed blobs, encrypted columns, dense binary)
   produce LZ4 framing overhead with no density win.
2. **Decode CPU on admitted reads.**
   `storage_compression_decode_micros{codec="lz4_frame"}`
   distribution, paired with
   `pbc_slab_stats_pruned_total / pbc_slab_stats_consulted_total`.
   High decode + low stats prune ratio = a tenant whose
   admission gate is leaky and now pays decompress cost on
   slabs that contribute little to results. Operator
   signal to revisit clustering / stats coverage, not to
   disable compression cluster-wide.
3. **Encode CPU on writes.**
   `storage_compression_encode_micros{codec="lz4_frame"}`
   distribution. Multi-row write throughput should drop by
   the encode share — typically 10–20% on a write-bound
   node. ADR 043 §3's two-axis enforcement encodes once
   uncompressed (for `max_record_bytes`) and once
   compressed (for the actual write); this metric covers
   both sides of that record-level cost.
4. **Convergence backlog.**
   `storage_compression_uncompressed_resident_bytes` gauge
   — unlabeled by design (the gauge's filter is
   "descriptor-current = `Lz4Frame`", which makes
   "uncompressed codec" the only meaningful value). Should
   decay monotonically as natural compaction reaches each
   pre-043 sealed slab. A non-trivial steady-state value
   means slabs haven't been rewritten — operators may
   trigger a manual recompact via the existing
   `recluster_sealed_table_layout` path (ADR 042 Phase 7),
   not via a dedicated compression-only sweep.
5. **Decode error rate.**
   `storage_compression_decode_error_total{codec}` should
   remain at zero. Non-zero is a corruption /
   version-drift signal; pair with `decode_error_total`
   on the existing IPC decode paths to localize.
6. **Operator opt-out rate.** Count of tenants flipping
   `Lz4Frame → Uncompressed` after rollout. High rate
   means the layout-driven default is wrong for the
   shipped workload mix; revisit the
   `MultiRow → Lz4Frame` default before adding more
   codecs.
7. **Stored ciphertext quota deltas.** Tenants on
   `MultiRow` tables see headroom at policy flip;
   billing / quota dashboards should reflect the
   reduction. Watch for tenants whose quota counters
   *don't* drop after a flip — that's a compaction-stuck
   slab signal.
8. **Architectural-test enforcement.** New guards landed
   in 043 stay green:
   - Brand-prefix grep returns empty (character-class
     self-defending regex).
   - Envelope `format_version` did not bump.
   - `encode_arrow_schema_ipc` and slab-footer CSB
     encoder reject `IpcWriteOptions` with compression.
   - Writer call-site allowlist matches the storage-side
     IPC encoder set; new sites must route through
     `ipc_write_options_for`.
9. **Point-read amplification under compression.**
   `storage_multirow_point_read_amplification_bytes`
   distribution should be unchanged in shape from the
   pre-043 baseline (the metric measures the
   *uncompressed* axis per ADR 043 §3, which is the same
   axis ADR 042 §10 used). If this metric shifts with the
   043 deploy, that is a sign the enforcement axis is
   leaking compressed bytes into the cap.
10. **L2 cold-read fetched bytes per admitted record.**
    Should fall ~50% on `Lz4Frame` tables. Projected-
    column overfetch — fetching 50 columns to filter to
    5 — remains a separate signal. It does *not*
    decrease under ADR 043; that is the future ADR 044
    (proposed) input.

### Carryover from 042

Still relevant:

- **Layout selection sanity.**
  `storage_multirow_rows_per_record` distribution per
  tenant — informs whether the default
  `target_rows = 4096` matches real workloads. Compose
  with the new compression-ratio metric: tables with low
  rows/record may also see worse compression.
- **Compaction load.**
  `storage_multirow_recompact_total` rate vs. write rate.
  ADR 043 added compression migration to the same single
  coherent compaction primitive; sweeper should keep up
  without accumulating an uncompacted-row-records or an
  uncompressed-resident backlog.
- **Migration backlog drain.**
  `storage_multirow_uncompacted_row_records_total` and
  `storage_compression_uncompressed_resident_bytes` decay
  in parallel — both are "convergence under coherent
  compaction" signals.
- **Supersession-tracking adoption.**
  `agg_csb_short_circuit_skipped_supersession_metadata_untracked_total`.
- **Recovery correctness.** First post-043 crash on a
  node carrying mixed-compression multi-row slabs is the
  real test. Verify the rebuilt redb index matches a
  pre-crash snapshot byte-for-byte across both codecs.
- **Architectural-test enforcement.** 042's
  synthetic-column / brand-prefix / scalar-PBC-opcode
  guards stay green alongside the 043 additions.

### Carryover from 040 / 041

- **AGG leverage.** `agg_request_total` vs. row-shipping
  baseline, per-tenant.
- **CSB short-circuit ratio.**
  `agg_csb_short_circuit_total / agg_request_total`.
- **Capability fallback.**
  `agg_skipped_no_capability_total` should remain at zero.
- **Numeric overflow.** `agg_int_overflow_total` should
  remain at zero.
- **KLL state size.** `agg_partial_bytes` distribution.
- **TieredSlab `tombstone_metadata` adoption.**
- **Architectural guard against `Wendelon` references in
  protocol surfaces.** Stays green; ADR 043's
  brand-prefix grep is the latest reinforcement.

### Carryover from 038 / 039

- **Stats prune ratio.** `pbc_slab_stats_pruned_total /
  pbc_slab_stats_consulted_total` per tenant. Pair with
  the new `decode_micros` metric: stats fold over rows
  decoded from compressed buffers, but the fold itself is
  unchanged.
- **CSB size and decrypt latency** —
  `pbc_slab_stats_block_bytes`,
  `pbc_slab_stats_decrypt_micros`. The CSB itself stays
  uncompressed (ADR 043 §6).
- **Locality clustering effectiveness** —
  `pbc_locality_arcs_scanned_total`,
  `pbc_range_decomposition_overflow_total`.
- **Width-budget pressure on declared clustering columns.**

## ADR 044 (proposed) — L2 columnar fetch (sovereign pushdown)

**What:** Column-aware Range-GETs against encrypted L2
objects. Instead of fetching whole admitted record bodies
and filtering node-side, the L2 read path fetches only the
chunks containing the buffers the query projects (and, when
predicates are narrow, only the chunks containing the
predicate columns). Stays inside ADR 020's per-slab DEK and
64 KiB chunked AEAD discipline; granularity remains
chunk-aligned, not byte-aligned.

**Why next, why high-priority:** This is the cold-tier
bandwidth concern the council surfaced during the ADR 043
session and that ADR 043 deliberately did not solve. ADR
043's per-buffer LZ4 (over record-level zstd) is the
load-bearing substrate — each Arrow column buffer
decompresses independently from its containing 64 KiB AEAD
chunk — so this ADR can target buffer-level fetch without
re-encoding the format. For an analytical query touching
5 of 50 columns over a cold table, ADR 044 plus ADR 043
together approach Parquet-class pushdown bytes-fetched
without giving up ADR 020 zero-trust or forking L2 to
Parquet plus provider-side encryption.

**Design surface (sketch, not committed):**

1. Authenticated record-to-buffer offset directory.
   Either extend the ADR 022 unencrypted slab footer with
   a per-record column-buffer offset index, or define a
   two-phase fetch protocol that pulls the head 64 KiB
   chunk first to read the IPC FlatBuffer metadata, then
   issues a targeted chunk-aligned Range-GET for the
   chunks containing the target column buffers. Either
   path must remain authenticated (no oracle attacks via
   adversarial chunk splices).
2. Chunk-aligned encrypted range-fetch protocol changes.
   ADR 020's 64 KiB chunked AEAD already supports
   contiguous range reads; column projection turns those
   into a set of disjoint contiguous ranges. Need a
   protocol-level batching story (one S3 GET per
   contiguous chunk run vs. parallel Range-GETs).
3. PBC executor column projection (ADR 035). Decoded
   batches materialize only the projected columns;
   non-projected columns never decompress.
4. Selectivity-aware fallback. For predicates whose
   stats-pruning ratio is low *and* whose column footprint
   is small relative to the IPC body, column-projected
   fetch dominates whole-record fetch. The planner must
   pick correctly using ADR 039 stats, not at random.
5. Composes with ADR 042 multi-row records (column
   buffers are per-batch; `record_row_index` projection
   still works) and ADR 043 per-buffer LZ4 (each column
   buffer decompresses independently from its chunk).
6. **Non-goals.** No homerolled encodings (still
   Brainstorm 011 V2). No page-directory point reads
   (still Brainstorm 011 V2 sub-piece). No mini-page row
   granularity in V1 — fetch granularity stays at the
   buffer (≈column) level. No S3 Select / provider-native
   filtering — same ADR 020 envelope-encryption argument.
   No Parquet fork.

**Risk:** Authenticated chunked range reads under per-slab
DEK are a non-trivial protocol change. Done wrong, this is
where the zero-trust posture cracks.

Size estimate: medium-large. Council session: 4–6
iterations.

## ADR 045 (proposed) — PK-less append-only tables

**What:** ADR 037 §19 deferred this. Streams, event logs,
audit trails. Content-hash-as-identity, auto-assigned
ordinal, no dedupe.

**Composition with 042 / 043:**

- PK-less tables almost always opt into
  `RecordLayoutPolicy::MultiRow` — they're scan-shaped by
  construction. By ADR 043 the layout-driven default
  carries them onto `Lz4Frame` automatically.
- `__row_key_id__` semantics extend: instead of BLAKE3 of
  canonical PK, it becomes BLAKE3 of payload (content
  hash) or a synthesized ordinal. This is the open
  question 042 left for the PK-less ADR to close.
- Append-only is the workload that benefits *most* from
  the ADR 040 + 042 CSB short-circuit: every sealed slab
  is visibility-complete forever (no supersession
  possible without a PK to hide).
- Append-only is also the workload that benefits *most*
  from ADR 044's column-projected fetch: scan-heavy and
  rarely point-read.
- Natural bridge to brainstorm 004 (CDC).

Size estimate: medium. Council session: 3–5 iterations.

## ADR 046 (proposed) — Secondary indexes on non-clustering columns

**What:** Declared secondary indexes mapping `column_value
→ (slab, record_location)`. Composes naturally with 042's
`record_row_index` granularity — secondary indexes can
point at a specific row inside a multi-row record without
changing the multi-row format.

**Why third (was 045, demoted one further):** With
clustering (038), stats (039), aggregates (040), multi-row
layout (042), LZ4 body compression (043), L2 columnar
fetch (proposed 044), and PK-less append-only (proposed
045) in place, predicates not covered by clustering are
the remaining slow-path. Secondary indexes are the right
tool of last resort — their cost is real (write
amplification, separate index management), so they're
opt-in per column per table. Demoted because no Toron
pull has surfaced and the cold-tier bandwidth axis (which
is what motivated promoting 044) ranks ahead.

**Design surface:**

1. Index storage shape: separate slabs, embedded in
   primary slab footer, or redb-indexed.
2. Index granularity: row-level (uses `record_row_index`),
   record-level (cheaper to maintain), or slab-level
   (effectively a column-stat).
3. Maintenance: synchronous on write vs. eventually
   consistent via background indexer.
4. Query planner integration: when does the planner
   consult the secondary index vs. fall back to PBC over
   the locality range?
5. Tombstone / supersession composition: secondary index
   entries need the same visibility metadata discipline
   as the primary index.
6. Composition with ADR 043: the index entry's
   `(slab, record_location, record_row_index)` resolves
   to a chunk range under ADR 044; index lookup → ADR 044
   column-projected fetch is the natural cold-tier shape.

Size estimate: medium-large. Council session: 4–6
iterations.

## Post-046 menu (ordered by compound value)

### AGG follow-ups (multiple, individually small)

ADR 040 deferred several aggregate-protocol extensions.
Each is its own ADR when a workload pulls:

- **Float canonical encoding.** Resolves ADR 039 §15's
  deferral and unblocks `sum(float)` / `avg(float)`.
  Size: medium.
- **Decimal SUM/AVG.** Pinned `Decimal256` overflow and
  scale contract. Size: medium.
- **Grouped aggregation (`GROUP BY`).** Per-slab
  cardinality caps, hash-table strategies, group-key
  canonical encoding, spill behavior. Size: large
  (4–6 iterations).
- **String/Binary quantiles.** Either drop ADR 039's
  prefix-truncation for KLL inputs, or invent a
  "rank within prefix space" semantic. Size:
  small-medium.
- **Exact `COUNT(DISTINCT)`.** Size: medium.
- **CSB visibility metadata consolidation.** Both
  `tombstone_metadata` (040) and `supersession_metadata`
  (042) could be promoted to a unified
  visibility-metadata struct on the TieredSlab pointer.
  Size: small.

These aren't sequenced — pick whichever Toron pulls first.
Most are independent and can land in any order.

### Wire-frame compression (transport-side IPC)

ADR 043 is storage-resident only; control-server schema
fetch and data-server raw-batch responses still ship
default-options Arrow IPC. A future transport ADR (under
the ADR 011 transport family, not the storage family)
can extend wire IPCs to LZ4 when network egress dominates
node CPU. The decision space is bounded by ADR 011's
auto-adaptive transport logic rather than by descriptor
policy.

Size estimate: small-medium. Pulled when measured network
egress cost dominates encode CPU on the wire path.

### Vector-form identity/locality PBC opcodes

ADR 042 retired the scalar `LOAD_IDENTITY` /
`ROUTE_BY_LOCALITY` opcodes outright. When Toron's
planner first needs per-row identity or locality inside a
predicate, a follow-up ADR introduces vector-form opcodes
that consume the `__row_key_id__` and
`__row_routing_locality__` synthetic columns as Arrow
inputs directly.

Size: small-medium. Pulled on workload demand.

### Row-level tombstones

Currently deferred (ADR 042 §16). When row-level DELETE
lands, the natural shape adds `__row_tombstone__: Bool`
to the storage-only column set or uses a sentinel value
in `__row_hlc__`. The recompactor drops tombstones once
they're below all surviving versions in the segment. ADR
042 left this unallocated per
`no-premature-backward-compat.md`.

Likely lands with or shortly after ADR 045. Size: medium.

### Compression-level tunable / zstd capacity-tier codec

ADR 043 §10 explicitly does not introduce a
`CompressionLevel` newtype because LZ4_FRAME exposes no
useful V1 tuning knob. When and if measured L2 capacity
cost dominates L1 read bandwidth on real workloads, a
new `RecordBodyCompressionPolicy::Zstd` variant lands
with the level newtype it needs. Until then, no reserved
enum slot — same convention as ADR 043 §1.

Size: medium. Pulled by L2 capacity-cost evidence.

### Per-buffer page directory for O(1) point-read decompression

Brainstorm 011 V2 sub-piece. May land independently of
the rest of LCE V2 if point-read amplification metrics
on `Lz4Frame` tables surface a real cost.

Size: medium. Pulled by point-read amplification
evidence on compressed multi-row tables.

### Per-column compression policy

Arrow IPC's `BodyCompression` is per-buffer, so
per-column policy is expressible. ADR 043 deliberately
does not surface it because LCE V2 does column-aware
encoding cascades better — coherently with FastLanes /
ALP / FSST selection. Per-column policy lands inside the
LCE V2 ADR, not as an extension to ADR 043.

## Hilbert curve (deferred)

ADR 038 left `ProjectionFunctionV1::Hilbert` parsed-but-
rejected as `PBC_REJECT_PROJECTION_NOT_YET_SUPPORTED`. A
future Hilbert ADR (numbered at opening) lifts that
rejection.

Why deferred: Z-order is adequate for the common
(≤4-dim) analytical case. Hilbert's tighter bounding
boxes are incremental improvement on locality, while the
deferred AGG follow-ups, ADR 044 (L2 columnar fetch),
ADR 045 (PK-less), and ADR 046 (secondary indexes)
unlock entire query classes or whole bandwidth axes.

## LSM-style leveled compaction (deferred)

ADR 042's forward-compat analysis confirmed that 042's
synthetic-column convention, `SingleBucket` packing,
per-row HLC, descriptor-driven layout, supersession
metadata, and single-coherent-compaction primitive all
push toward LSM-friendly. ADR 043 added compression
policy migration to the same single coherent compaction
primitive — the LSM ADR opens when measured write-amp
pressure justifies it, with one more invariant
(compression policy) folded into the same pointer-swap.

Two ADR-042-incrementing forward-cost decisions to keep
in view (unchanged from `roadmap-post-042.md`):

- The global redb index gained 4 bytes/row for
  `record_row_index`. If a future ADR shifts to
  per-segment metadata, this becomes a sunk cost.
- ADR 042 permits within-bucket key-range overlap
  silently; classical leveled compaction (LevelDB /
  RocksDB style) requires non-overlap at L1+. The LSM
  ADR picks size-tiered (overlap allowed) or
  merge-promotes-overlapping-peers.

Vocabulary watch: "segment" and "slab" are the same
concept today. If LSM lands, segments may be composed of
multiple multi-row records per slab — vocabulary
diverges.

## Longer-range brainstorms (multi-ADR streams)

Each is its own council session when pulled by real
workload demand.

- **Brainstorm 002 — Vector embeddings as first-class.**
  ANN search integrated rather than sidecar. Composes
  with 038's locality (LSH variant), 039's stats (PQ
  centroids), 040's AGG (vector reduction), 042's
  multi-row layout (batch vector ops over fragment-
  shaped records), and 043's per-buffer LZ4 (vector
  buffers compress well).

- **Brainstorm 004 — Real-time CDC and triggers.**
  Native streaming from the Raft WAL. Composes with
  045's PK-less tables and 042's multi-row layout (CDC
  frames as multi-row records).

- **Brainstorm 010 — Serverless PBC L2.** Cold-storage
  compute. Composes with 042 (multi-row records are the
  right unit for cold-storage compute pushdown — large
  enough to amortize Lambda invocation, small enough to
  skip), 043 (compressed L2 objects mean Lambda fetches
  fewer bytes), and 044 (column-projected fetch from
  Lambda is the natural shape). Note: Brainstorm 010 is
  *node-pushdown* (do compute *at* L2 storage); ADR 044
  is *column pushdown* (fetch fewer columns *from* L2).
  They are siblings, not substitutes.

- **Brainstorm 011 — Lightweight Columnar Encodings
  (LCE).** The "Vortex" playbook. **V1.5 (Arrow IPC LZ4
  body compression) shipped as ADR 043.** The
  brainstorm's remaining scope is V2 (homerolled SIMD
  encodings — ALP, FastLanes, FSST, RLE, encoding
  cascades, page directories for O(1) point-read
  amplification) and V3 (COCD — `PbcExecutor` evaluates
  predicates directly on encoded bytes). Pulled when
  post-043 metrics show the remaining bandwidth /
  memory headroom is worth the multi-quarter design
  surface. **ADR 044 (proposed) is *not* part of
  Brainstorm 011's V2/V3 scope** — it is a sibling
  cold-tier ADR that uses 043's per-buffer LZ4 as
  substrate, not a replacement codec.

## Status at a glance

| ADR | Scope | Status |
|---|---|---|
| 035 | Vectorized PBC executor | ✓ shipped |
| 036 | PBC pushdown contract | ✓ shipped |
| 037 | Tenant table schemas, row identity, `TableSpreadProjection` | ✓ shipped |
| 038 | Column-aware locality V1, Z-order | ✓ shipped |
| 039 | Slab-footer column statistics V1 | ✓ shipped |
| 040 | Aggregate pushdown protocol | ✓ shipped |
| 041 | Project-name decoupling refactor | ✓ shipped |
| 042 | Analytical multi-row slab fragments + supersession metadata | ✓ shipped |
| 043 | Arrow IPC body compression (LZ4) | ✓ shipped |
| 044 (proposed) | L2 columnar fetch (sovereign pushdown) | high-priority post-043 pickup |
| 045 (proposed) | PK-less append-only tables | post-044 |
| 046 (proposed) | Secondary indexes on non-clustering columns | post-045 — no Toron pull yet |
| 047+ (candidate, unordered) | Wire-frame compression (transport-side IPC) | deferred — pull from network egress cost |
| 047+ (candidate, unordered) | Vector-form identity/locality PBC opcodes | deferred — pull from workload |
| 047+ (candidate, unordered) | Row-level tombstones | deferred — likely with 045 |
| 047+ (candidate, unordered) | Float canonical encoding | deferred — pull from workload |
| 047+ (candidate, unordered) | Decimal SUM/AVG | deferred — pull from workload |
| 047+ (candidate, unordered) | Grouped aggregation | deferred — pull from workload |
| 047+ (candidate, unordered) | String/Binary quantiles | deferred — pull from workload |
| 047+ (candidate, unordered) | Exact `COUNT(DISTINCT)` | deferred — pull from workload |
| 047+ (candidate, unordered) | CSB visibility metadata consolidation (tombstone + supersession) | deferred — small |
| 047+ (candidate, unordered) | Auto-triggered single→multi migration | deferred — pull from ops noise |
| 047+ (candidate, unordered) | zstd capacity-tier codec + `CompressionLevel` newtype | deferred — pull from L2 capacity cost |
| 047+ (candidate, unordered) | Per-buffer page directory for O(1) point-read decompression | deferred — pull from point-read amplification on `Lz4Frame` |
| 047+ (candidate, unordered) | Logical / physical descriptor fingerprint split | deferred — pull from physical-only field accumulation |
| N (future) | Column-Aware Locality V2, Hilbert variant | deferred behind AGG follow-ups |
| N (future) | LSM-style leveled compaction | deferred behind write-amp pressure |
| N (brainstorm 002) | Vector embeddings first-class | deferred, multi-ADR |
| N (brainstorm 004) | CDC + triggers | deferred, multi-ADR |
| N (brainstorm 010) | Serverless PBC L2 (node-pushdown) | deferred, multi-ADR — sibling to ADR 044 |
| N (brainstorm 011) | LCE V2/V3 (custom SIMD encodings + COCD; per-column policy lands here) | deferred, multi-ADR — V1.5 shipped as ADR 043 |

ADR numbers 044+ are candidate ordering; actual numbering
happens at opening.

## What to not do next

Carryovers from the 042-era roadmap, plus 043-specific
additions.

### From 042

- **Don't open the LSM ADR speculatively.** ADR 042's
  forward-compat work confirmed it's tractable, not that
  it's ready. Wait for measured write-amp pressure on
  real workloads. ADR 043 added one more invariant
  (compression policy) to the same single-coherent-
  compaction primitive — the LSM ADR inherits it, not a
  separate sweep.
- **Don't enable `MultiRow` cluster-wide.** It's a
  per-table opt-in for a reason. Point-heavy tables pay
  the amplification tax with no compensating scan
  benefit — and now also pay the LZ4 encode/decode cost
  with no compensating density benefit.
- **Don't auto-trigger `SingleRow → MultiRow` migration.**
  ADR 042 §13 is operator-driven only in V1.
- **Don't reintroduce scalar identity/locality PBC
  opcodes.** ADR 042 retired them under the day-14
  fresh-start license. Future identity/locality
  predicates use vector-form opcodes that consume the
  synthetic columns as Arrow inputs.
- **Don't open every brainstorm at once.** Each is its
  own council session.
- **Don't promote Hilbert ahead of AGG follow-ups or
  044/045/046.** Locality refinement, not warehouse
  infrastructure.
- **Don't expand ADR 040's catalog without an ADR.** New
  aggregate tags, new sketch types, new
  numerical-semantics guarantees each have their own
  forward-compat surface.
- **Don't reintroduce brand prefixes in protocol
  surfaces.** ADR 041's convention is enforced by the
  architectural test and reinforced by ADR 042's metric
  / reject / synthetic-column audit and by ADR 043's
  brand-prefix grep guard.
- **Don't piecemeal sweeper passes.** ADR 042 made
  single-coherent-compaction load-bearing. ADR 043
  reinforced it: compression policy migration joins the
  same atomic pointer-swap, not a separate sweep. Future
  metadata-tracking upgrades (tombstone, supersession,
  compression policy, future LSM levels, etc.) compose
  there.

### New under 043

- **Don't enable `Lz4Frame` on point-heavy `SingleRow`
  workloads cluster-wide.** ADR 043 §1 leaves
  `SingleRow + Lz4Frame` as validate-and-allow exactly
  for unusual workloads where the operator has measured a
  density win — the layout-driven default keeps it off
  precisely because LZ4_FRAME framing overhead dominates
  small per-row IPC payloads. If the
  `storage_compression_ratio{codec="lz4_frame"}`
  histogram shows ratios near or above 1.0 for a tenant
  on `SingleRow + Lz4Frame`, that tenant should flip
  back to `Uncompressed`.
- **Don't auto-flip compression policy based on per-table
  measured ratio.** ADR 043 rejected
  auto-tune-on-measured-ratio explicitly. The descriptor
  is the source of truth; observability is the operator
  feedback path. An auto-flip ADR opens later if and only
  if manual opt-out / opt-in proves operationally noisy
  at fleet scale — measure first.
- **Don't open the LCE V2 ADR before post-043 metrics
  saturate.** Brainstorm 011 V2/V3 is multi-quarter
  design surface. ADR 043's stabilization watch —
  `compression_ratio`, `decode_micros`, `encode_micros`,
  `uncompressed_resident_bytes` — exists to expose
  whether LZ4_FRAME captures the bandwidth/memory win
  Brainstorm 011 identified. Open V2 when those metrics
  show a real residual headroom, not on principle.
- **Don't pre-allocate `Zstd` / `Snappy` / `Vortex`
  enum slots in `RecordBodyCompressionPolicy`.** ADR
  043 §1 chose the enum exactly so future codecs add a
  variant rather than reshape a field, and chose to keep
  the variant set at exactly two so future codecs land
  with their own ADR (with their own measured workload
  pull, their own observability surface, their own
  compression-level newtype where needed). Convention
  violation per
  `plans/conventions/no-premature-backward-compat.md`.
- **Don't enforce `max_record_bytes` against compressed
  bytes.** ADR 043 §3 is load-bearing: the cap exists
  to bound *decompressed* point-read payload and
  decode-time memory peaks. Enforcing against the
  compressed axis would silently raise ADR 042's
  amplification ceiling and convert a stable point-read
  trade-off into a workload-shape lottery.
- **Don't bump the envelope `format_version`.** The
  Arrow IPC FlatBuffer's `BodyCompression` field is the
  format authority; a parallel envelope version is a
  second source of truth with no new information. If a
  future change needs an envelope-version bump, that is
  its own ADR.
- **Don't add a "compress everything" pass.** Two
  storage-internal IPC encoders are explicit exclusions
  guarded by architectural tests:
  `encode_arrow_schema_ipc` and the slab-footer CSB
  encoder. The descriptor schema IPC must decode without
  the `ipc_compression` Cargo feature at boot; the CSB
  needs O(1) random access during admission. Wire-side
  encoders are also out of scope (storage-resident only).
  Future "compress more" work opens its own ADR per
  surface.
- **Don't bundle L2 columnar Range-GET pushdown into a
  later ADR's scope opportunistically.** ADR 044
  (proposed) is the right ADR for column-aware fetch.
  Splicing column projection into AGG / PBC / secondary
  indexes ADRs out of order would create a half-built
  authenticated-offset story.
- **Don't fork L2 to Parquet plus provider-side
  encryption as a workaround for ADR 044.** That is a
  storage-architecture change with its own zero-trust
  trade-off (ADR 020), its own rehydration model (ADR
  042), and its own execution-engine consequences (PBC
  / AGG no longer the read path on cold tier). It is
  ADR-grade on its own merits, not a substitute for
  ADR 043 + ADR 044.
- **Don't pin golden-byte fixtures to raw LZ4 payload
  bytes.** ADR 043 Phase 0 set the discipline: golden
  tests pin *semantic round-trip* and *FlatBuffer-
  metadata presence* (`BodyCompression == LZ4_FRAME`),
  not raw payload bytes. LZ4 output may shift across
  `arrow` or `lz4_flex` upgrades even when decoded
  batches are byte-identical.
