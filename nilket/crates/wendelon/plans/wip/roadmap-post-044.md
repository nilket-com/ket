# Roadmap: from 044 forward (transient nav doc)

Author: Claude (AI Swarm Council)
Scope: transient navigation — replaces `roadmap-post-043.md`,
retires whenever `plans/wip/` is cleaned up again.
Written after ADRs 035, 036, 037, 038, 039, 040, 041, 042,
043, 044 shipped.

## Where we are

The warehouse story now spans ten shipped ADRs:

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
  against the uncompressed IPC body, tenant quotas / stored
  bytes against the encoded body.
- **ADR 044** — L2 columnar fetch (sovereign pushdown).
  Two-phase chunk-aligned authenticated read against
  encrypted L2 objects. Phase A pulls a 64 KiB metadata
  window, parses Arrow IPC FlatBuffer per-buffer offsets
  into a `RecordBufferDirectory`. Phase B issues
  chunk-aligned Range-GETs for the projected buffers'
  chunk runs through `TieredMultiRangeReadPlan` (a thin
  extension of the existing `plan_tiered_range_read`
  primitive). Node-local `L2ColumnFetchConfig` — *no*
  descriptor field. Per-record planner branch: cheap
  bypasses (`Disabled`, `SingleRow`, `FullProjection`,
  `SmallRecord`, `RehydrationAvailable`) run before Phase
  A; post-Phase-A fallbacks (`TooManyBytes`,
  `TooManyRanges`, `MetadataProbeCap`,
  `UnsupportedArrowLayout`) gate the columnar plan against
  no-win cases. `ProjectedRecordBatch { batch,
  ordinal_mapping }` resolves descriptor ordinals through
  a narrow physical batch — no dummy null arrays for
  unfetched columns. Authentication failures *never* fall
  back. Rehydration stays whole-slab. No new node
  capability bit, no envelope `format_version` bump, no
  `ObjectStore` trait break.

What ADR 044 commits the platform to (this commit is doc
promotion; the runtime targets below land phase-by-phase
per `plans/impl/044_l2_columnar_fetch.md`):

- **Egress target.** Once implementation lands, a
  5-of-50-column projection over a `MultiRow + Lz4Frame`
  cold table is targeted to move ~20% of whole-record
  bytes (4–5× egress reduction). For narrower projections
  the ratio scales with projection size, not record size —
  Parquet-class bandwidth without giving up ADR 020
  zero-trust or forking L2 to Parquet. Verified in Phase
  11 benches.
- **Load-bearing substrate, not just density.** Per-buffer
  LZ4 from ADR 043 is reclassified as load-bearing for
  044: each Arrow column buffer decompresses independently
  from its containing 64 KiB AEAD chunk, which is what
  lets 044 fetch and decompress only the chunks
  containing projected buffers. ADR 043's bytes don't
  change; its role does.
- **Per-record strategy selection contract.** The
  cold-tier read path will select per-record between
  whole-record and column-projected fetch using a
  conservative threshold tuple
  (`min_record_bytes` = 256 KiB,
  `max_selected_fraction_micros` = 670_000 = 0.67,
  `max_ranges_per_record` = 8). Tenants whose workloads
  don't fit the columnar regime fall back to whole-record
  fetch with byte-identical results. Defaults are revisable
  per Phase 11 benches; the contract — that the planner
  picks per-record and never overshoots whole-record cost —
  is fixed.
- **No new authentication surface.** The two-phase fetch
  protocol is two consecutive uses of the existing
  `plan_tiered_range_read` chunk-aligned authenticated
  range-read primitive. ADR 020's per-slab DEK + 64 KiB
  chunked AEAD discipline stays byte-identical; the AAD
  tuple, nonce derivation, and key-wrapping path are
  unchanged.
- **Range planning generalizes, single-range stays as a
  thin wrapper.** The `TieredMultiRangeReadPlan`
  abstraction will generalize the existing single-range
  planner; the single-range caller becomes a thin wrapper,
  with no behavior change to any non-ADR-044 caller.
- **Projection derived from PBC, not a new wire field.**
  `ColumnFetchRequirement` is derived from existing PBC
  `LOAD` + terminal `PROJECT` opcodes plus AGG accumulator
  inputs. No new wire-protocol field for Toron, no new PBC
  opcode, no new HRFT frame family.

What 044 explicitly did *not* do, and why it matters for
the roadmap:

- **No mini-page row-group pruning.** Selective predicates
  that hit 1% of rows in a 4096-row record still fetch
  the full projected buffer set. "Filter to 1% of rows
  still pulls 100% of buffer bytes" is residual headroom.
  Future ADR (likely after Brainstorm 011 V2's page
  directories) opens when measured.
- **No homerolled SIMD encodings.** Brainstorm 011 V2/V3
  remains its own multi-quarter design surface. ADR 044
  consumes ADR 043's per-buffer LZ4 as substrate; LCE V2
  is a sibling that swaps codecs, not a follow-on of 044.
- **No Lambda / serverless PBC at L2.** Brainstorm 010 is
  *node-pushdown* (do compute *at* L2 storage), separate
  from 044's *column pushdown* (fetch fewer columns
  *from* L2). Brainstorm 010 now has its substrate (the
  same chunk-aligned authenticated range-read primitive)
  but remains its own multi-ADR stream.
- **No descriptor-fingerprint split into logical and
  physical halves.** ADR 044 added zero descriptor fields,
  so the fingerprint surface is unchanged.
- **No `ObjectStore` trait break.** Provider-specific
  batch / multi-range APIs are uneven (S3 `GetObject`
  takes one range per request; some providers / SDK shims
  expose richer batch shapes). 044 stays portable by
  issuing bounded parallel single-range `get_range` calls
  on every backend. A future ADR could widen the trait if
  bench output justifies it on providers where richer
  shapes exist.
- **No partial-slab rehydration.** ADR 006's hot-tier
  rehydration still installs whole slab bytes. Column-
  projected reads count toward existing rehydration heat
  but do not change what rehydration installs.

## The near map

```
                          YOU ARE HERE
                              ▼
     ┌──────────────────────────────────────────────────────────┐
     │ 035 / 036 / 037 / 038 / 039 / 040 / 041 / 042 / 043 / 044│ ← shipped
     └────────────┬─────────────────────────────────────────────┘
                  │
   ┌──────────────┼──────────────┐
   │              │              │
   ▼              ▼              ▼
 stabilization  ADR 045         ADR 046
 (1–2 weeks)   PK-less          secondary
                append-only      indexes
                tables
                                │
                                ▼
                   ┌────────────┴────────────┐
                   │      post-046 menu      │
                   │ ordered by compound     │
                   │ value                   │
                   └────────────┬────────────┘
                                ▼
                AGG follow-ups → wire-frame compression
                  (when warranted) → mini-page row-group
                  fetch (when warranted) → Hilbert (when
                  warranted) → LSM leveled compaction (when
                  warranted) → LCE V2 / V3 / brainstorms
```

ADR numbering is unchanged from the post-043 roadmap: 044
shipped at the position previously labeled "044 (proposed) —
L2 columnar fetch (sovereign pushdown)." 045 (PK-less
append-only) and 046 (secondary indexes) stay at their
post-043 numbers.

## Immediate (1–2 weeks post-runtime-rollout)

Once the impl plan's phases land and the column-projected
fetch path is rolling out on the fleet, let 044 settle.
These are the new watch items the path introduces, plus
carryovers from earlier ADRs.

### New under 044

1. **Strategy distribution.**
   `storage_l2_column_fetch_requests_total{strategy}`.
   Healthy ratio depends on workload; alarming signal is
   `strategy="columnar"` at 0% on a fleet that should be
   using the new path (planner thresholds too
   conservative, or `enabled=false` left over from rollout
   gate).
2. **Fallback-reason distribution.**
   `storage_l2_column_fetch_fallback_total{reason}`. The
   dominant reason tells operators which knob to turn:
   - `too_many_bytes` dominates → workload is dense; not
     a regression, just not a fit for columnar fetch.
   - `too_many_ranges` dominates → coalescing is weak;
     consider tightening the chunk-merge gap budget or
     reducing `max_parallel_range_gets`.
   - `metadata_probe_cap` dominates → schemas are wider
     than the 256 KiB cap accommodates; bump
     `max_ipc_metadata_probe_bytes`.
   - `unsupported_arrow_layout` dominates → V1 partial
     materializer needs broader Arrow type coverage;
     promote a follow-up to widen the supported set.
   - `small_record` dominates → records are below
     `min_record_bytes` (256 KiB); not a regression on
     point-heavy workloads.
3. **Bytes saved.**
   `storage_l2_column_fetch_record_body_bytes_avoided_total
    / storage_l2_column_fetch_requests_total{strategy="columnar"}`.
   Headline cost-axis win. Target: ≥ 80% byte reduction on
   5-of-50-column analytical projections (Phase-11 bench
   target from impl plan).
4. **Phase A round-trip count.**
   `storage_l2_column_fetch_phase_a_round_trips_total /
    storage_l2_column_fetch_requests_total{strategy="columnar"}`.
   Should hover near 1.0; > 1.5 means metadata window is
   too small (tune `ipc_metadata_initial_window_bytes`
   closer to `max_ipc_metadata_probe_bytes`).
5. **Phase A skip ratio.**
   `storage_l2_column_fetch_phase_a_skipped_total /
    storage_l2_column_fetch_requests_total`. High ratio is
   *good* — cheap bypasses are catching no-win cases
   without paying the metadata round trip.
6. **Truncated-window refetch.**
   `storage_l2_column_fetch_metadata_truncated_refetch_total`.
   Should be near zero. Non-zero means
   `ipc_metadata_initial_window_bytes` (default 64 KiB)
   undersizes the workload's typical schema; bump up
   toward the cap.
7. **Decompression skip.**
   `storage_l2_column_fetch_decompressed_buffers_total{codec="lz4_frame"}` /
   total LZ4 buffers in admitted records. Ratio < 1 means
   upstream projection is correctly avoiding decompression
   of non-projected buffers. Persistent ratio = 1 is a
   regression candidate (the partial materializer
   accidentally decompressing everything).
8. **Auth-error rate.**
   `storage_l2_column_fetch_auth_error_total`. **Must stay
   zero.** Non-zero is a corruption / key drift / reader
   bug signal — never a tuning signal. Pair with
   `decode_error_total` on the existing IPC decode paths
   to localize.
9. **L2 GET-rate cost.** Object-store API call rate
   before/after. Operators on tenants where GET-rate cost
   dominates byte savings should flip
   `L2ColumnFetchConfig::enabled = false` cluster-wide.
   Surfaced in vendor bills and provider-side dashboards;
   Wendelon does not directly observe.
10. **Cold-tier latency p50 / p99.**
    On representative tenants, cold-read p50 / p99
    before/after. Network-savings should dominate
    Phase-A round-trip cost for projection-sparse
    workloads; opposite for projection-dense.
11. **Architectural-test enforcement.** New guards landed
    in 044 stay green:
    - No new node capability bit
      (`NodeCapability::*Column*` grep empty).
    - No descriptor field added (`column_fetch_policy`,
      `ColumnFetchPolicy` greps empty).
    - No slab-format-version bump.
    - No Parquet / ORC / S3 Select identifier introduced.
    - No `_` catch-all match arm for
      `ColumnarFallbackReason`.
    - No project-name-prefixed surfaces.
    - Multi-range plan-building reuses
      `plan_tiered_range_read`'s authentication exactly;
      no parallel chunk-math impl.
    - Partial materializer rejects dummy-null array
      construction (the unit-test guard from impl-plan
      Phase 4).
12. **Operator opt-out rate.** Count of operators flipping
    `L2ColumnFetchConfig::enabled = false` after rollout.
    High rate means the threshold defaults don't fit the
    fleet's workload mix; revisit per-provider defaults
    (Open Question #1) before adding more codec or layout
    surface.
13. **Phase A batching adoption** (Phase 8 of impl plan).
    `storage_l2_column_fetch_phase_a_batched_records_total`
    increases when contiguous admitted records share
    metadata-window chunks. Operationally healthy on
    sequential scans; near zero on random-access reads.
14. **Tenant ciphertext quota deltas.** Tenants on
    `MultiRow + Lz4Frame` tables should see *no* quota
    change from ADR 044 (column fetch is a read-side
    optimization; stored bytes are unchanged). A delta
    is a metric / accounting bug.

### Carryover from 043

Still relevant:

- **Compression density.**
  `storage_compression_ratio{codec="lz4_frame"}`.
  Target band 0.3–0.6 on typical analytical schemas;
  unchanged by ADR 044.
- **Decode CPU on admitted reads.**
  `storage_compression_decode_micros{codec="lz4_frame"}`.
  Should *fall* on column-projected reads — only projected
  buffers decompress. The interaction shows up in the
  ratio between the storage-compression and L2-column-fetch
  histograms.
- **Encode CPU on writes.**
  `storage_compression_encode_micros{codec="lz4_frame"}`.
  Unchanged by ADR 044 (writes are not column-projected).
- **Convergence backlog.**
  `storage_compression_uncompressed_resident_bytes` gauge
  decays as natural compaction reaches each pre-043 sealed
  slab. ADR 044 reads pre-043 sealed slabs identically to
  post-043 ones (compression is per-buffer; absent =
  uncompressed buffer; the partial materializer handles
  both).
- **Decode error rate.**
  `storage_compression_decode_error_total{codec}` should
  remain at zero. ADR 044's `auth_error_total` is the
  L2-fetch sibling.
- **Operator opt-out rate** for `Lz4Frame → Uncompressed`.
- **Stored ciphertext quota deltas.** ADR 044 doesn't
  change this axis.
- **Architectural-test enforcement.** ADR 043's
  brand-prefix grep, envelope-version-bump grep, encoder
  exclusion guards, writer call-site allowlist all stay
  green alongside ADR 044's new guards.
- **Point-read amplification under compression.**
  `storage_multirow_point_read_amplification_bytes`
  shape unchanged by ADR 044 (column fetch reads scan-
  shaped predicates; point reads still take the
  whole-record path via `read_from_l2`).
- **L2 cold-read fetched bytes per admitted record.**
  Now distributed across two metric families: ADR 043's
  `storage_compression_l2_record_body_bytes_fetched_total`
  (compression's view) and ADR 044's
  `storage_l2_column_fetch_record_body_bytes_fetched_total{strategy}`
  (column fetch's view). Both should fall on
  projection-sparse cold scans.

### Carryover from 042

Still relevant:

- **Layout selection sanity.**
  `storage_multirow_rows_per_record` distribution per
  tenant.
- **Compaction load.**
  `storage_multirow_recompact_total` rate vs. write rate.
- **Migration backlog drain.**
  `storage_multirow_uncompacted_row_records_total` and
  `storage_compression_uncompressed_resident_bytes` decay
  in parallel.
- **Supersession-tracking adoption.**
  `agg_csb_short_circuit_skipped_supersession_metadata_untracked_total`.
- **Recovery correctness.** ADR 044 keeps recovery on the
  whole-record path; the first post-044 crash on a node
  carrying mixed-compression multi-row slabs that have
  served column-projected reads is the real test.
- **Architectural-test enforcement.** 042's guards stay
  green alongside 044's.

### Carryover from 040 / 041

- **AGG leverage.** `agg_request_total` vs. row-shipping
  baseline, per-tenant. Column-projected fetch lowers
  cold-tier AGG cost without changing the per-row fold
  itself.
- **CSB short-circuit ratio.**
  `agg_csb_short_circuit_total / agg_request_total`.
  ADR 044 is downstream of CSB; pruned slabs issue zero
  record-body Range-GETs (column or whole-record).
- **Capability fallback.**
  `agg_skipped_no_capability_total` should remain at zero.
- **Numeric overflow.** `agg_int_overflow_total` zero.
- **KLL state size.** `agg_partial_bytes` distribution.
- **TieredSlab `tombstone_metadata` adoption.**
- **Architectural guard against `Wendelon` references in
  protocol surfaces.** Stays green.

### Carryover from 038 / 039

- **Stats prune ratio.** `pbc_slab_stats_pruned_total /
  pbc_slab_stats_consulted_total` per tenant. Pair with
  the new `storage_l2_column_fetch_*` metrics:
  CSB-pruned slabs never reach the column-fetch planner;
  surviving slabs are where the column-fetch win lands.
- **CSB size and decrypt latency** —
  `pbc_slab_stats_block_bytes`,
  `pbc_slab_stats_decrypt_micros`. Unchanged by ADR 044
  (CSB stays at fixed offsets, uncompressed, AEAD-sealed).
- **Locality clustering effectiveness** —
  `pbc_locality_arcs_scanned_total`,
  `pbc_range_decomposition_overflow_total`.
- **Width-budget pressure on declared clustering columns.**

## ADR 045 (proposed) — PK-less append-only tables

**What:** ADR 037 §19 deferred this. Streams, event logs,
audit trails. Content-hash-as-identity, auto-assigned
ordinal, no dedupe.

**Composition with 042 / 043 / 044:**

- PK-less tables almost always opt into
  `RecordLayoutPolicy::MultiRow` — they're scan-shaped by
  construction. By ADR 043 the layout-driven default
  carries them onto `Lz4Frame` automatically.
- ADR 044 column-projected fetch is the *natural read
  shape* for append-only: scan-heavy, rarely point-read,
  often projection-sparse (event-shape tables typically
  have wide schemas, narrow query projections).
- `__row_key_id__` semantics extend: instead of BLAKE3 of
  canonical PK, it becomes BLAKE3 of payload (content
  hash) or a synthesized ordinal. This is the open
  question 042 left for the PK-less ADR to close.
- Append-only is the workload that benefits *most* from
  the ADR 040 + 042 CSB short-circuit: every sealed slab
  is visibility-complete forever (no supersession
  possible without a PK to hide).
- Natural bridge to brainstorm 004 (CDC).

Size estimate: medium. Council session: 3–5 iterations.

## ADR 046 (proposed) — Secondary indexes on non-clustering columns

**What:** Declared secondary indexes mapping `column_value
→ (slab, record_location)`. Composes naturally with 042's
`record_row_index` granularity and 044's column-projected
fetch — secondary indexes can point at a specific row
inside a multi-row record without changing the multi-row
format, and the fetch from the indexed slab uses ADR 044's
column-projected path.

**Why third (was 046 in post-043, stays at 046):** With
clustering (038), stats (039), aggregates (040), multi-row
layout (042), LZ4 body compression (043), L2 columnar
fetch (044), and PK-less append-only (proposed 045) in
place, predicates not covered by clustering are the
remaining slow-path. Secondary indexes are the right tool
of last resort — their cost is real (write amplification,
separate index management), so they're opt-in per column
per table. No Toron pull has surfaced yet; pulled when
measured.

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
6. **Composition with ADR 044**: the index entry's
   `(slab, record_location, record_row_index)` triple
   resolves to a chunk range under the column-fetch
   planner. Index lookup → ADR 044 column-projected
   fetch → row-index projection post-decode is the
   natural cold-tier shape for an indexed predicate
   joined with a projection.

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

### Mini-page row-group fetch (sub-record row pruning)

ADR 044 V1 fetch granularity is the Arrow buffer (≈column)
level. Selective predicates that hit a small fraction of
rows inside a multi-row record still fetch the full
projected buffer set, then filter node-side.

A future ADR could add row-group granularity by either
(a) introducing an Arrow IPC layout extension that
fragments column buffers into mini-pages indexed by row
range, (b) homerolling lightweight columnar encoding with
page directories (Brainstorm 011 V2 sub-piece), or (c) a
secondary-index-driven point-read shape post-046.

Pulled when post-044 metrics show selective predicates
account for a meaningful fraction of cold-read latency.

Size estimate: medium-large. Multi-quarter design surface
for option (b).

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
they're below all surviving versions in the segment.

Likely lands with or shortly after ADR 045. Size: medium.

### Compression-level tunable / zstd capacity-tier codec

ADR 043 §10 explicitly does not introduce a
`CompressionLevel` newtype because LZ4_FRAME exposes no
useful V1 tuning knob. When and if measured L2 capacity
cost dominates L1 read bandwidth on real workloads, a
new `RecordBodyCompressionPolicy::Zstd` variant lands
with the level newtype it needs.

Size: medium. Pulled by L2 capacity-cost evidence.

### Per-buffer page directory for O(1) point-read decompression

Brainstorm 011 V2 sub-piece. May land independently of
the rest of LCE V2 if point-read amplification metrics
on `Lz4Frame` tables surface a real cost. Composes with
ADR 044's column-projected fetch (page-directory entries
become finer-grained Range-GET targets within a chunk).

Size: medium. Pulled by point-read amplification
evidence on compressed multi-row tables.

### Per-column compression policy

Arrow IPC's `BodyCompression` is per-buffer, so
per-column policy is expressible. ADR 043 deliberately
does not surface it because LCE V2 does column-aware
encoding cascades better — coherently with FastLanes /
ALP / FSST selection. Per-column policy lands inside the
LCE V2 ADR, not as an extension to ADR 043.

### Provider-specific batch / multi-range API in `ObjectStore`

ADR 044 §10 explicitly does not break the `ObjectStore`
trait. S3's `GetObject` takes one byte range per request,
so parallel single-range GETs are the portable shape on
S3; some providers / SDK shims expose richer batch
shapes. A future ADR could widen the trait if post-044
bench output shows a richer shape meaningfully reduces
L2 GET count or RTT on projection-sparse workloads on
providers where it exists. Until then, bounded parallel
`get_range` calls portably cover all backends.

Size: medium. Pulled by cross-provider bench evidence.

### `RecordBufferDirectory` cache

ADR 044 §3 explicitly does not cache parsed
`RecordBufferDirectory` across queries. A future ADR
adds a cache surface, eviction policy, and consistency
story (records can be retired by retention sweep mid-
cache) when fleet metrics show repeat-record reads on
cold tier.

Size: small-medium. Pulled by repeat-cold-read evidence.

## Hilbert curve (deferred)

ADR 038 left `ProjectionFunctionV1::Hilbert` parsed-but-
rejected as `PBC_REJECT_PROJECTION_NOT_YET_SUPPORTED`. A
future Hilbert ADR (numbered at opening) lifts that
rejection.

Why deferred: Z-order is adequate for the common
(≤4-dim) analytical case. Hilbert's tighter bounding
boxes are incremental improvement on locality, while the
deferred AGG follow-ups, ADR 045 (PK-less), and ADR 046
(secondary indexes) unlock entire query classes or whole
bandwidth axes.

## LSM-style leveled compaction (deferred)

ADR 042's forward-compat analysis confirmed that 042's
synthetic-column convention, `SingleBucket` packing,
per-row HLC, descriptor-driven layout, supersession
metadata, and single-coherent-compaction primitive all
push toward LSM-friendly. ADR 043 added compression
policy migration to the same single coherent compaction
primitive. ADR 044 adds nothing to the compaction surface
(it's a read-side optimization). The LSM ADR opens when
measured write-amp pressure justifies it.

Two ADR-042-incrementing forward-cost decisions to keep
in view (unchanged from `roadmap-post-042` /
`roadmap-post-043`):

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
  shaped records), 043's per-buffer LZ4 (vector buffers
  compress well), and 044's column-projected fetch
  (vector queries typically project a small set of
  columns alongside the embedding column).

- **Brainstorm 004 — Real-time CDC and triggers.**
  Native streaming from the Raft WAL. Composes with
  045's PK-less tables and 042's multi-row layout (CDC
  frames as multi-row records). ADR 044 is largely
  orthogonal — CDC is write-time, ADR 044 is read-time.

- **Brainstorm 010 — Serverless PBC L2.** Cold-storage
  compute. Now has its substrate after ADR 044: the
  same chunk-aligned authenticated range-read primitive
  that 044's planner uses. A Lambda function evaluating
  PBC at L2 needs the per-slab DEK delivered to the
  Lambda environment under ADR 020's key-custody story
  plus the `ColumnFetchPlan` shape (or its public
  successor) to know which chunks to fetch. Brainstorm
  010 is *node-pushdown*; ADR 044 is *column pushdown*.
  They are siblings, not substitutes. ADR 044 is the
  natural seed for Brainstorm 010's chunk-fetch step.

- **Brainstorm 011 — Lightweight Columnar Encodings
  (LCE).** The "Vortex" playbook. **V1.5 (Arrow IPC LZ4
  body compression) shipped as ADR 043.** ADR 044 is
  *not* part of Brainstorm 011's V2/V3 scope — it is a
  sibling cold-tier ADR that uses 043's per-buffer LZ4
  as substrate. The brainstorm's remaining scope is V2
  (homerolled SIMD encodings — ALP, FastLanes, FSST,
  RLE, encoding cascades, page directories for O(1)
  point-read amplification) and V3 (COCD —
  `PbcExecutor` evaluates predicates directly on
  encoded bytes). Pulled when post-044 metrics show the
  remaining bandwidth / memory headroom is worth the
  multi-quarter design surface.

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
| 044 | L2 columnar fetch (sovereign pushdown) | ✓ shipped |
| 045 (proposed) | PK-less append-only tables | next pickup |
| 046 (proposed) | Secondary indexes on non-clustering columns | post-045 — no Toron pull yet |
| 047+ (candidate, unordered) | Wire-frame compression (transport-side IPC) | deferred — pull from network egress cost |
| 047+ (candidate, unordered) | Mini-page row-group fetch (sub-record row pruning) | deferred — pull from selective-predicate latency |
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
| 047+ (candidate, unordered) | Provider-native multi-range API in `ObjectStore` | deferred — pull from cross-provider bench |
| 047+ (candidate, unordered) | `RecordBufferDirectory` cache | deferred — pull from repeat-cold-read evidence |
| 047+ (candidate, unordered) | Logical / physical descriptor fingerprint split | deferred — pull from physical-only field accumulation |
| N (future) | Column-Aware Locality V2, Hilbert variant | deferred behind AGG follow-ups |
| N (future) | LSM-style leveled compaction | deferred behind write-amp pressure |
| N (brainstorm 002) | Vector embeddings first-class | deferred, multi-ADR |
| N (brainstorm 004) | CDC + triggers | deferred, multi-ADR |
| N (brainstorm 010) | Serverless PBC L2 (node-pushdown) | deferred, multi-ADR — sibling to ADR 044, now has substrate |
| N (brainstorm 011) | LCE V2/V3 (custom SIMD encodings + COCD; per-column policy lands here) | deferred, multi-ADR — V1.5 shipped as ADR 043 |

ADR numbers 047+ are candidate ordering; actual numbering
happens at opening.

## What to not do next

Carryovers from the 043-era roadmap, plus 044-specific
additions.

### From 042 / 043

- **Don't open the LSM ADR speculatively.** Wait for
  measured write-amp pressure on real workloads. ADR 043
  added compression-policy migration to the single-coherent-
  compaction primitive; ADR 044 added nothing to that
  primitive. The LSM ADR inherits both, not a separate
  sweep.
- **Don't enable `MultiRow` cluster-wide.** It's a
  per-table opt-in for a reason. Point-heavy tables pay
  the amplification tax with no compensating scan
  benefit — and now also pay the LZ4 encode/decode cost
  with no compensating density benefit, while picking up
  no benefit from ADR 044's column-projected fetch path
  (which is scan-shaped).
- **Don't auto-trigger `SingleRow → MultiRow` migration.**
  ADR 042 §13 is operator-driven only in V1.
- **Don't reintroduce scalar identity/locality PBC
  opcodes.** ADR 042 retired them; future
  identity/locality predicates use vector-form opcodes
  that consume the synthetic columns as Arrow inputs.
- **Don't open every brainstorm at once.** Each is its
  own council session.
- **Don't promote Hilbert ahead of AGG follow-ups or
  045/046.** Locality refinement, not warehouse
  infrastructure.
- **Don't expand ADR 040's catalog without an ADR.**
- **Don't reintroduce brand prefixes in protocol
  surfaces.** Architectural test enforces.
- **Don't piecemeal sweeper passes.** Single-coherent-
  compaction stays load-bearing.
- **Don't enable `Lz4Frame` on point-heavy `SingleRow`
  workloads cluster-wide.** ADR 043 §1's layout-driven
  default keeps it off precisely because LZ4_FRAME framing
  overhead dominates small per-row IPC payloads.
- **Don't auto-flip compression policy based on per-table
  measured ratio.** ADR 043 rejected this; observability
  is the operator feedback path.
- **Don't pre-allocate `Zstd` / `Snappy` / `Vortex` enum
  slots in `RecordBodyCompressionPolicy`.** Convention
  violation per
  `plans/conventions/no-premature-backward-compat.md`.
- **Don't enforce `max_record_bytes` against compressed
  bytes.** ADR 043 §3 is load-bearing; the cap exists to
  bound *decompressed* point-read payload.

### New under 044

- **Don't add a `ColumnFetchPolicy` descriptor field.**
  ADR 044 explicitly rejects this — column fetch is a
  read-side optimization decided per-query and per-record,
  not a per-table policy. Adding the field would churn
  the descriptor fingerprint for no logical-schema or
  physical-record change. The node-local
  `L2ColumnFetchConfig` is the operator surface; if a
  per-table override is later justified, that's a
  follow-up ADR with its own threat-model and
  fingerprint-cost analysis.
- **Don't break the `ObjectStore` trait for native
  multi-range APIs in V1.** Provider support is uneven;
  bounded parallel `get_range` calls portably cover all
  current backends. A future ADR opens if cross-provider
  bench shows a measurable cost win.
- **Don't add a `RecordBufferDirectory` cache.** ADR 044
  V1 derives the directory from authenticated IPC bytes
  every read. A cache adds eviction-policy and
  consistency-story complexity (records can be retired by
  retention sweep mid-cache). Pulled by repeat-cold-read
  evidence, not on principle.
- **Don't fabricate dummy null arrays for unfetched
  columns.** Architectural unit test rejects this. A
  physical batch with null placeholders for columns that
  were never fetched would create a second interpretation
  of column presence parallel to the `ordinal_mapping`
  and silently produce wrong results downstream.
- **Don't let AEAD / footer-HMAC failures fall back to
  whole-record fetch.** Architectural test enforces. Auth
  failures surface as `CryptoError` and indicate
  corruption, key drift, or a reader bug — never a tuning
  signal. Fetching more bytes must not turn a corruption
  signal into a successful query.
- **Don't auto-tune `min_record_bytes` per record or per
  query.** It's a node-config knob for a reason: the
  right value depends on provider RTT and per-GET cost,
  which are deployment-fixed properties. Per-query
  auto-tuning would add planner-side state and a
  feedback loop with no measured benefit.
- **Don't pre-allocate fallback-reason enum slots beyond
  the V1 set.** Convention violation per
  `plans/conventions/no-premature-backward-compat.md`.
  `ColumnarFallbackReason` has exactly 9 variants;
  future variants land with their own ADR (with their
  own measured workload pull).
- **Don't open Brainstorm 010 (serverless PBC L2) before
  ADR 044 stabilization metrics are in.** ADR 044 is the
  substrate; Brainstorm 010 is the next layer. Opening
  the latter speculatively risks designing against
  unstable assumptions.
- **Don't bundle column-fetch into ADR 045 / 046's
  scopes opportunistically.** ADR 045 (PK-less append-
  only) is a logical-schema ADR; ADR 046 (secondary
  indexes) is an indexing ADR. Both compose with ADR 044
  through documented interfaces; they should not extend
  the column-fetch protocol or threshold surface.
- **Don't promote ADR 044's Open Questions to ADR scope
  before bench / implementation-discovery / council /
  follow-on-ADR resolution.** Of the 9 open questions:
  #1, #3, #4 are bench-resolvable; #2, #5, #7, #9 are
  implementation-discovery (resolved by the relevant impl
  phase); #6 defers to the Brainstorm 010 ADR if/when it
  opens; #8 (access-pattern leakage threat-model
  strength) is council/policy-resolvable. None are
  contract-level. Premature ADRs would freeze choices the
  bench, the implementer, or the council should make.
- **Don't put Arrow IPC FlatBuffer metadata in the
  cleartext slab footer.** ADR 044 §3 is load-bearing:
  schema and per-buffer offset shape leak workload
  information even if data buffers stay encrypted. The
  two-phase probe keeps IPC metadata encrypted under the
  same record chunks as the buffers it describes.
- **Don't introduce partial-slab rehydration.** ADR 006
  rehydration installs whole slab bytes. Column-projected
  reads count toward the existing heat counter; the
  promotion shape is unchanged.
- **Don't tighten `max_selected_fraction_micros` cluster-
  wide before Phase 11 metrics are in.** The 0.67 default
  is conservative; lowering it without bench evidence
  pushes more queries to the whole-record path with no
  cost-axis justification.

ADR 044 is meant to compose, not expand. The roadmap
beyond it is workload-pull, not feature-push.
