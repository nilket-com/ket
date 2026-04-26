# Roadmap: from 042 forward (transient nav doc)

Author: Claude (AI Swarm Council)
Scope: transient navigation — replaces `roadmap-post-040.md`,
retires whenever `plans/wip/` is cleaned up again.
Written after ADRs 035, 036, 037, 038, 039, 040, 041, 042
shipped.

## Where we are

The warehouse story now spans eight shipped ADRs:

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
  Bucket-pure packing preserves ADR 038 narrowing.
  `SupersessionMetadata` extends the ADR 040 CSB
  short-circuit proof to cover MVCC version-hiding (not just
  tombstones). ADR 035's scalar `LOAD_IDENTITY` /
  `ROUTE_BY_LOCALITY` opcodes were retired under the day-14
  fresh-start license. `RecordLocation` grew uniformly by
  4 bytes for `record_row_index`.

What's true now that wasn't true a week ago:

- A 65,536-row analytical write produces ~16 multi-row
  records (at default `target_rows = 4096`) instead of 65,536
  single-row records. ~4000× fewer `SlabRecordHeader`s, BLAKE3
  computations, and slab appends; ~4000× less per-record
  decode overhead on full scans.
- AGG folding visits ~244K records on a 1B-row table instead
  of 1B records. Combined with ADR 038 locality narrowing,
  ADR 039 stats pruning, and ADR 040 CSB short-circuit, the
  read path now compresses what has to leave storage at every
  layer.
- CSB direct answers are sound under MVCC, not just under
  append-only. The new `supersession_metadata` field closes
  a soundness gap that existed on single-row tables since
  ADR 040 and never showed up because no fragment-shaped
  forcing function exposed it.
- The PBC executor is smaller (two scalar-context opcodes
  removed) and cleaner — predicates see user columns only.

## The near map

```
                     YOU ARE HERE
                          ▼
     ┌──────────────────────────────────────────────────┐
     │ 035 / 036 / 037 / 038 / 039 / 040 / 041 / 042    │ ← shipped
     └──────────┬───────────────────────────────────────┘
                │
   ┌────────────┼────────────┐
   │            │            │
   ▼            ▼            ▼
 stabilization  ADR 043       ADR 044
 (1-2 weeks)   Arrow IPC     PK-less
                LZ4 body      append-only
                compression
                              │
                              ▼
                          ADR 045
                       secondary indexes
                              │
                              ▼
                 ┌────────────┴────────────┐
                 │      post-045 menu      │
                 │ ordered by compound     │
                 │ value                   │
                 └────────────┬────────────┘
                              ▼
              AGG follow-ups → Hilbert (when warranted) →
              LSM leveled compaction (when warranted) →
              L2 serverless / brainstorms
```

## Immediate (1–2 weeks post-merge)

Let 042 settle. New watch items the multi-row layout
introduces, plus carryovers from the 040/041 watch list that
remain relevant.

### New under 042

1. **Layout selection sanity.**
   `storage_multirow_rows_per_record` distribution per
   tenant — informs whether the default
   `target_rows = 4096` matches real workloads. Long tail
   below 100 rows/record signals a tenant who opted into
   multi-row on a workload that doesn't pack well; long tail
   above 50K signals tables hitting `max_rows` ceiling.
2. **Point-read amplification.**
   `storage_multirow_point_read_amplification_bytes`
   distribution. High amplification flags a tenant who set
   `RecordLayoutPolicy::MultiRow` on a table that takes
   point reads — operator signal to switch back to
   `SingleRow` or split the access pattern into sibling
   tables.
3. **Compaction load.**
   `storage_multirow_recompact_total` rate vs. write rate.
   Sweeper should keep up with multi-row backlog without
   accumulating uncompacted row records.
4. **Migration backlog drain.**
   `storage_multirow_uncompacted_row_records_total`. After a
   `SingleRow → MultiRow` descriptor edit, the conservative
   migration path appends row-records first; this metric
   should decay toward zero as compaction re-packs them.
5. **Supersession-tracking adoption.**
   `agg_csb_short_circuit_skipped_supersession_metadata_untracked_total`
   should decay toward zero as natural compaction migrates
   pre-042 sealed slabs through the single-coherent-rewrite
   path. Non-zero rate steady-state means a tenant has slabs
   that haven't been rewritten and are ineligible for CSB
   short-circuit; benign on cold tables.
6. **Recovery correctness.** First post-042 crash on a node
   carrying multi-row slabs is a real test of the
   descriptor-free recovery path. Verify the rebuilt redb
   index matches a pre-crash snapshot byte-for-byte.
7. **Architectural-test enforcement.** New guards landed in
   042: synthetic columns never appear in user output; metric
   / reject prefixes stay brand-clean; removed scalar
   PBC opcodes never reappear in executor or fuzz corpora.
   The test catalog should stay green.

### Carryover from 040/041

Still relevant, paired down to active items:

- **AGG leverage.** `agg_request_total` vs. row-shipping
  baseline, per-tenant. Should now show further compression
  on multi-row tables vs. the 040 baseline.
- **CSB short-circuit ratio.**
  `agg_csb_short_circuit_total / agg_request_total`. Should
  rise as more slabs gain tracked supersession metadata.
- **Capability fallback.**
  `agg_skipped_no_capability_total` should remain at zero
  post-rollout.
- **Numeric overflow.** `agg_int_overflow_total` should
  remain at zero.
- **KLL state size.** `agg_partial_bytes` distribution —
  whether default `k = 200` holds.
- **TieredSlab `tombstone_metadata` adoption.** Pre-040
  sealed slabs should continue migrating to tracked via
  natural compaction.
- **Architectural guard against `Wendelon` references in
  protocol surfaces.** Stays green.

### Carryover from 038

- **Stats prune ratio.** `pbc_slab_stats_pruned_total /
  pbc_slab_stats_consulted_total` per tenant.
- **CSB size and decrypt latency** —
  `pbc_slab_stats_block_bytes`, `pbc_slab_stats_decrypt_micros`.
- **Locality clustering effectiveness** —
  `pbc_locality_arcs_scanned_total`,
  `pbc_range_decomposition_overflow_total`.
- **Width-budget pressure on declared clustering columns.**

## ADR 043 — Arrow IPC body compression (LZ4)

**What:** Enable Arrow IPC's native `BodyCompression` (LZ4) on
the writer side; readers decompress transparently. No new wire
protocol, no synthetic-column changes, no PBC executor changes.
Composes orthogonally with ADR 042's multi-row fragments (LZ4
wraps the IPC payload bytes inside the existing
`[SlabRecordHeader] [Arrow IPC]` envelope) and with ADR 039's
stats path (stats are computed pre-compression; pruning still
operates on uncompressed metadata).

**Why next:** Memory and NVMe bandwidth are the dominant
variable cost as analytical scale grows. LZ4 BodyCompression
gives 60–80% on-disk density at memory-bandwidth decode speeds
(3–5 GB/s) — directly attacking the cost axis without forcing
the full custom-LCE / compute-over-compressed-data redesign
that Brainstorm 011 covers. The implementation surface is
small: Arrow already does the heavy lifting on the reader side,
so this is mostly a writer-side flag plus envelope/encryption
ordering verification.

Promoted out of Brainstorm 011 (originally bundled as the V1.5
step). V2 / V3 (homerolled SIMD encodings, page directories,
COCD opcodes) stay in the brainstorm until LZ4 ships and
metrics expose the remaining bandwidth/memory headroom.

**Design surface:**

1. Writer flag in `pack_record_batch_by_bucket` (ADR 042
   Phase 2): emit IPC with LZ4 `BodyCompression` enabled.
2. Reader: Arrow IPC handles decompress transparently; only
   wiring change is plumbing the buffer-compression metadata
   through the decode path.
3. Compose with ADR 020 envelope encryption: compress-then-
   encrypt is the right order; verify the existing envelope
   handles the new payload shape without fragmentation.
4. Compose with ADR 042 §10 point-read amplification: LZ4
   decompresses the whole IPC body, so the bounded
   amplification caveat still applies. Page-directory style
   O(1) point-read decompression is V2 territory
   (Brainstorm 011), not this ADR.
5. Per-table opt-in or descriptor default? Lean enabled-by-
   default on `MultiRow` tables (where the win is largest);
   `SingleRow` tables get an opt-in path because the
   compression-amortization story is weaker on single-row
   IPCs.
6. Stats writer (ADR 039): stats compute over decompressed
   rows; the slab-footer CSB itself is not compressed (lives
   at fixed offsets and is already encrypted).
7. Bench surface: scan latency on admitted slabs (decompress
   cost vs. raw IPC), CSB pruning effectiveness unchanged,
   full-system read latency from cold storage (the dominant
   win), write throughput impact from encode cost.

**Risk:** Decompress-before-PBC adds CPU cost on admitted
slabs. Tenants whose stats prune poorly may see scan latency
regression. Stabilization watch tracks
`pbc_slab_stats_pruned_total / consulted_total` per tenant
(carryover from 038) and the new
`storage_compression_decompress_micros` distribution. If a
tenant regresses, the per-table opt-out is the immediate
relief valve.

Size estimate: small-medium. Council session: 2–4 iterations.

## ADR 044 — PK-less append-only tables

**What:** ADR 037 §19 deferred this. Streams, event logs,
audit trails. Content-hash-as-identity, auto-assigned
ordinal, no dedupe.

**Composition with 042:**

- PK-less tables almost always opt into
  `RecordLayoutPolicy::MultiRow` — they're scan-shaped by
  construction.
- `__row_key_id__` semantics extend: instead of BLAKE3 of
  canonical PK, it becomes BLAKE3 of payload (content hash) or
  a synthesized ordinal. This is the open question 042 left
  for ADR 044 to close.
- Append-only is the workload that benefits *most* from the
  ADR 040 + 042 CSB short-circuit: every sealed slab is
  visibility-complete forever (no supersession possible
  without a PK to hide).
- Natural bridge to brainstorm 004 (CDC).

Size estimate: medium. Council session: 3–5 iterations.

## ADR 045 — Secondary indexes on non-clustering columns

**What:** Declared secondary indexes mapping `column_value →
(slab, record_location)`. Composes naturally with 042's
`record_row_index` granularity — secondary indexes can point
at a specific row inside a multi-row record without changing
the multi-row format.

**Why third (was 043, demoted):** With clustering (038), stats
(039), aggregates (040), multi-row layout (042), and LZ4 body
compression (043) in place, predicates not covered by
clustering are the remaining slow-path. Secondary indexes are
the right tool of last resort — their cost is real (write
amplification, separate index management), so they're opt-in
per column per table. Demoted behind 043/044 because no Toron
pull has surfaced yet.

**Design surface:**

1. Index storage shape: separate slabs, embedded in primary
   slab footer, or redb-indexed.
2. Index granularity: row-level (uses `record_row_index`),
   record-level (cheaper to maintain), or slab-level (effectively
   a column-stat).
3. Maintenance: synchronous on write vs. eventually consistent
   via background indexer.
4. Query planner integration: when does the planner consult
   the secondary index vs. fall back to PBC over the locality
   range?
5. Tombstone / supersession composition: secondary index
   entries need the same visibility metadata discipline as the
   primary index.

Size estimate: medium-large. Council session: 4–6 iterations.

## Post-045 menu (ordered by compound value)

### AGG follow-ups (multiple, individually small)

ADR 040 deferred several aggregate-protocol extensions. Each
is its own ADR when a workload pulls:

- **Float canonical encoding.** Resolves ADR 039 §15's
  deferral and unblocks `sum(float)` / `avg(float)`. Size:
  medium.
- **Decimal SUM/AVG.** Pinned `Decimal256` overflow and scale
  contract. Size: medium.
- **Grouped aggregation (`GROUP BY`).** Per-slab cardinality
  caps, hash-table strategies, group-key canonical encoding,
  spill behavior. Size: large (4–6 iterations).
- **String/Binary quantiles.** Either drop ADR 039's
  prefix-truncation for KLL inputs, or invent a "rank within
  prefix space" semantic. Size: small-medium.
- **Exact `COUNT(DISTINCT)`.** Size: medium.
- **CSB visibility metadata consolidation.** Both
  `tombstone_metadata` (040) and `supersession_metadata` (042)
  could be promoted to a unified visibility-metadata struct
  on the TieredSlab pointer. Size: small.

These aren't sequenced — pick whichever Toron pulls first.
Most are independent and can land in any order.

### Vector-form identity/locality PBC opcodes

ADR 042 retired the scalar `LOAD_IDENTITY` /
`ROUTE_BY_LOCALITY` opcodes outright. When Toron's planner
first needs per-row identity or locality inside a predicate, a
follow-up ADR introduces vector-form opcodes that consume the
`__row_key_id__` and `__row_routing_locality__` synthetic
columns as Arrow inputs directly.

Size: small-medium. Pulled on workload demand.

### Row-level tombstones

Currently deferred (ADR 042 §16). When row-level DELETE lands,
the natural shape adds `__row_tombstone__: Bool` to the
storage-only column set or uses a sentinel value in
`__row_hlc__`. The recompactor drops tombstones once they're
below all surviving versions in the segment. ADR 042 left this
unallocated per `no-premature-backward-compat.md`.

Likely lands with or shortly after ADR 044. Size: medium.

## Hilbert curve (deferred)

ADR 038 left `ProjectionFunctionV1::Hilbert` parsed-but-rejected
as `PBC_REJECT_PROJECTION_NOT_YET_SUPPORTED`. A future Hilbert
ADR (numbered at opening) lifts that rejection.

Why deferred: Z-order is adequate for the common (≤4-dim)
analytical case. Hilbert's tighter bounding boxes are
incremental improvement on locality, while the deferred AGG
follow-ups (float, decimal, GROUP BY) and ADR 044 (PK-less)
unlock entire query classes.

## LSM-style leveled compaction (deferred)

ADR 042's forward-compat analysis confirmed that 042's
synthetic-column convention, `SingleBucket` packing, per-row
HLC, descriptor-driven layout, supersession metadata, and
single-coherent-compaction primitive all push toward
LSM-friendly. The LSM ADR opens when measured write-amp
pressure justifies it.

Two ADR-042-incrementing forward-cost decisions to keep in
view:

- The global redb index gained 4 bytes/row for
  `record_row_index`. If a future ADR shifts to per-segment
  metadata, this becomes a sunk cost.
- ADR 042 permits within-bucket key-range overlap silently;
  classical leveled compaction (LevelDB / RocksDB style)
  requires non-overlap at L1+. The LSM ADR picks size-tiered
  (overlap allowed) or merge-promotes-overlapping-peers.

Vocabulary watch: "segment" and "slab" are the same concept
today. If LSM lands, segments may be composed of multiple
multi-row records per slab — vocabulary diverges.

## Longer-range brainstorms (multi-ADR streams)

Each is its own council session when pulled by real workload
demand.

- **Brainstorm 002 — Vector embeddings as first-class.** ANN
  search integrated rather than sidecar. Composes with 038's
  locality (LSH variant), 039's stats (PQ centroids), 040's
  AGG (vector reduction), and 042's multi-row layout (batch
  vector ops over fragment-shaped records).

- **Brainstorm 004 — Real-time CDC and triggers.** Native
  streaming from the Raft WAL. Composes with 044's PK-less
  tables and 042's multi-row layout (CDC frames as multi-row
  records).

- **Brainstorm 010 — Serverless PBC L2.** Cold-storage
  compute. Composes with 042 (multi-row records are the right
  unit for cold-storage compute pushdown — large enough to
  amortize Lambda invocation, small enough to skip).

- **Brainstorm 011 — Lightweight Columnar Encodings (LCE).**
  The "Vortex" playbook. V1.5 (Arrow IPC LZ4 BodyCompression)
  was promoted to ADR 043 candidate. The brainstorm's remaining
  scope is V2 (homerolled SIMD encodings — ALP, FastLanes,
  FSST, RLE, encoding cascades, page directories for O(1)
  point-read amplification) and V3 (COCD — `PbcExecutor`
  evaluates predicates directly on encoded bytes). Pulled when
  ADR 043 ships and metrics show the remaining bandwidth /
  memory headroom is worth the multi-quarter design surface.

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
| 043 (proposed) | Arrow IPC body compression (LZ4) | next council session |
| 044 (proposed) | PK-less append-only tables | post-043 |
| 045 (proposed) | Secondary indexes on non-clustering columns | post-044 — no Toron pull yet |
| 046+ (candidate, unordered) | Vector-form identity/locality PBC opcodes | deferred — pull from workload |
| 046+ (candidate, unordered) | Row-level tombstones | deferred — likely with 044 |
| 046+ (candidate, unordered) | Float canonical encoding | deferred — pull from workload |
| 046+ (candidate, unordered) | Decimal SUM/AVG | deferred — pull from workload |
| 046+ (candidate, unordered) | Grouped aggregation | deferred — pull from workload |
| 046+ (candidate, unordered) | String/Binary quantiles | deferred — pull from workload |
| 046+ (candidate, unordered) | Exact `COUNT(DISTINCT)` | deferred — pull from workload |
| 046+ (candidate, unordered) | CSB visibility metadata consolidation (tombstone + supersession) | deferred — small |
| 046+ (candidate, unordered) | Auto-triggered single→multi migration | deferred — pull from ops noise |
| N (future) | Column-Aware Locality V2, Hilbert variant | deferred behind AGG follow-ups |
| N (future) | LSM-style leveled compaction | deferred behind write-amp pressure |
| N (brainstorm 002) | Vector embeddings first-class | deferred, multi-ADR |
| N (brainstorm 004) | CDC + triggers | deferred, multi-ADR |
| N (brainstorm 010) | Serverless PBC L2 | deferred, multi-ADR |
| N (brainstorm 011) | LCE V2/V3 (custom SIMD encodings + COCD) | deferred, multi-ADR — V1.5 promoted to ADR 043 |

ADR numbers 043+ are candidate ordering; actual numbering
happens at opening.

## What to not do next

- **Don't open the LSM ADR speculatively.** ADR 042's
  forward-compat work confirmed it's tractable, not that it's
  ready. Wait for measured write-amp pressure on real
  workloads.
- **Don't enable `MultiRow` cluster-wide.** It's a per-table
  opt-in for a reason. Point-heavy tables pay the
  amplification tax with no compensating scan benefit.
- **Don't auto-trigger `SingleRow → MultiRow` migration.** ADR
  042 §13 is operator-driven only in V1. The auto-trigger
  ADR opens if and when manual descriptor-edit migration
  proves operationally noisy — measure first.
- **Don't reintroduce scalar identity/locality PBC opcodes.**
  ADR 042 retired them under the day-14 fresh-start license.
  Future identity/locality predicates use vector-form
  opcodes that consume the synthetic columns as Arrow inputs.
- **Don't open every brainstorm at once.** Each is its own
  council session.
- **Don't promote Hilbert ahead of AGG follow-ups or 043/044/045.**
  Locality refinement, not warehouse infrastructure.
- **Don't homeroll LCE / COCD before ADR 043 (LZ4) ships.**
  Brainstorm 011 V2/V3 is multi-quarter. LZ4 is small surface
  and likely captures most of the bandwidth/memory win on its
  own. Measure first; if metrics show LZ4 saturating, then the
  LCE/COCD design pulls.
- **Don't expand ADR 040's catalog without an ADR.** New
  aggregate tags, new sketch types, new numerical-semantics
  guarantees each have their own forward-compat surface.
- **Don't reintroduce brand prefixes in protocol surfaces.**
  ADR 041's convention is enforced by the architectural test
  and reinforced by 042's metric / reject / synthetic-column
  audit.
- **Don't piecemeal sweeper passes.** ADR 042 made
  single-coherent-compaction load-bearing. Future
  metadata-tracking upgrades (tombstone, supersession, future
  LSM levels, etc.) join the same atomic pointer-swap, not
  separate sweep passes.
