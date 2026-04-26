# Roadmap: from 040/041 to 042 and beyond (transient nav doc)

Author: Claude (AI Swarm Council)
Scope: transient navigation — replaces `roadmap-post-038.md`,
retires whenever `plans/wip/` is cleaned up again.
Written after ADRs 035, 036, 037, 038, 039, 040, 041 shipped.

## Where we are

The warehouse story now spans seven shipped ADRs:

- **ADR 035** — vectorized predicate-bytecode (PBC) executor.
  Arrow kernels, scalar oracle, benches.
- **ADR 036** — PBC as the versioned Toron→Wendelon predicate-
  pushdown contract.
- **ADR 037** — typed tenant tables, PK-derived row identity,
  `TableSpreadProjection` keeping tables on a bounded set of
  arcs.
- **ADR 038** — column-aware locality V1 (Z-order),
  descriptor-authoritative. Range queries on declared
  clustering columns map to a small set of contiguous ring
  arcs, capped at the planner's 1024-arc budget.
- **ADR 039** — slab-footer column statistics V1. Encrypted CSB
  between encrypted chunks and offset_index; locator in the
  formerly-reserved 32 bytes of `slab_footer`; always-on min/
  max/null/non-null per column; opt-in bloom/HLL/small-set;
  compaction recomputes; internal pruning invisible to Toron
  behind a `slab_stats_pruning_v1` capability bit.
- **ADR 040** — aggregate pushdown protocol (AGG). Sibling
  protocol to PBC. Ungrouped global aggregates (count, sum,
  min, max, avg, KLL quantiles, HLL distinct) over PBC-filtered
  Arrow batches. Coordinator-node fan-out with cross-bind
  merge. CSB short-circuit for COUNT/MIN/MAX under a
  visibility-completeness proof condition; depends on ADR 039's
  TieredSlab pointer carrying `first_tombstone_hlc:
  Option<HybridTimestamp>`.
- **ADR 041** — project-name decoupling from internal
  protocols and types. WPBC → PBC; brand-clean reject codes,
  metric prefixes, module names, struct prefixes. New
  convention `plans/conventions/no-project-name-in-protocols.md`
  codifies the rule going forward.

What's true now that wasn't true a week ago:

- `SELECT COUNT(*) FROM events WHERE region = 'us-west'`
  collapses to one merged number per query rather than
  shipping every matching row to Toron. Locality narrows the
  arc set, stats narrow the slab set, the predicate filter
  narrows rows within admitted slabs, AGG folds the survivors
  into a partial — and on append-only tables, the CSB short-
  circuit answers the aggregate from the slab tail without
  decrypting any chunks.
- The brand is no longer baked into wire formats, reject
  codes, metric prefixes, or module names. A future project
  rename (Wendelon → another name) is a Tier-3 metadata change,
  not a protocol break.
- ADR 036's `PushdownApprox` targetability label, idle since
  the contract was written, is now load-bearing for
  `quantile_kll` and `approx_distinct`.

## The near map

```
                     YOU ARE HERE
                          ▼
     ┌─────────────────────────────────────────┐
     │ 035 / 036 / 037 / 038 / 039 / 040 / 041 │ ← shipped
     └──────────┬──────────────────────────────┘
                │
   ┌────────────┼────────────┐
   │            │            │
   ▼            ▼            ▼
 stabilization  quick wins   ADR 042
 (1-2 weeks)   (small ADRs)  analytical
                              multi-row
                              fragments
                              │
                              ▼
                 ┌────────────┴────────────┐
                 │      post-042 menu      │
                 │ ordered by compound     │
                 │ value                   │
                 └────────────┬────────────┘
                              ▼
              secondary indexes → PK-less append-only →
              Hilbert (when warranted) → L2 serverless
```

## Immediate (1–2 weeks post-merge)

Let 040 and 041 settle together. Specifically:

1. **AGG leverage measurement.** `agg_request_total` vs.
   row-shipping baseline for the same query class. Per-tenant
   variance shows which workloads benefit most.
2. **CSB short-circuit value.**
   `agg_csb_short_circuit_total` / `agg_request_total` ratio.
   Append-only tenants should see high ratios; tenants with
   active tombstone churn should see near-zero. If a tenant
   that should benefit isn't, investigate whether `seal_hlc` /
   `first_tombstone_hlc` is correctly populated.
3. **Capability fallback visibility.**
   `agg_skipped_no_capability_total` should drop to zero once
   every node has rolled out. Non-zero reveals stuck rolling
   upgrades.
4. **Numeric overflow.** `agg_int_overflow_total` should be
   zero. Non-zero means a tenant is hitting i128/u128
   overflow — investigate workload shape (likely malformed
   data, possibly legitimate sum at the protocol boundary).
5. **KLL state size.** `agg_partial_bytes` distribution —
   informs whether default `k = 200` holds or workloads want
   higher accuracy at higher state cost.
6. **Coordinator fan-out cost.** `agg_merge_micros` under
   peer-count variation. Identifies whether the coordinator
   fan-out becomes a hot-spot under real load.
7. **TieredSlab `first_tombstone_hlc` adoption.** New sealed
   slabs populate the field; pre-040 sealed slabs carry `None`.
   Track the ratio over time as natural compaction migrates
   pre-040 slabs to the new schema.
8. **Architectural-test enforcement.** Verify that no new
   `Wendelon` references slip into protocol surfaces. The
   forbidden-words test in `tests/architecture_test.rs` should
   stay green.

Carryovers from the post-038 watch list (still relevant):

- Stats prune ratio: `pbc_slab_stats_pruned_total` /
  `pbc_slab_stats_consulted_total`. Per-tenant variance.
- CSB size and decrypt latency
  (`pbc_slab_stats_block_bytes`, `pbc_slab_stats_decrypt_micros`).
  Whether the `slab_payload_bytes / 256` cap is right and
  whether the local stats cache is hitting often enough.
- `pbc_slab_stats_pointer_drift_total` should stay at zero.
- Real clustering effectiveness:
  `pbc_locality_arcs_scanned_total`,
  `pbc_range_decomposition_overflow_total`.
- Width-budget pressure on declared clustering columns.

## ADR 042 — Analytical multi-row fragments

**Seed:** Originally numbered as candidate ADR 041 in
`roadmap-post-038.md`; shifted to 042 after the project-name
decoupling refactor took the 041 slot.

**What:** Relax ADR 037's "one slab record = one row"
invariant for analytical-shaped tables. A slab record carries
a packed N-row Arrow batch with a row-level PK index inside it.

**Why next after 040:** Single-row records are correct for
MVCC but wasteful for huge analytical tables. With clustering
(038), stats (039), and aggregates (040) in place, single-row
cost becomes the bottleneck on large analytical scans even
when the AGG path collapses the result.

**What 042 has to decide (the design surface):**

1. Slab record format: how to lay out an N-row Arrow batch
   with row-level PK index and MVCC visibility metadata.
2. Backward compatibility with ADR 037's single-row records.
   Per-table opt-in or whole-cluster?
3. CSB interaction: stats are still per-slab (per ADR 039),
   but row-level visibility (per `first_tombstone_hlc` from
   040) needs to extend to per-record-batch granularity.
4. Predicate execution: PBC operates on `RecordBatch`; this
   maps cleanly when records *are* batches. Verify no
   semantic regression.
5. Aggregate execution: AGG folds rows; same composition as
   today but with batch-shaped input that may admit a faster
   path than batch-then-fold.
6. Compaction story: merging row-shaped slabs into batch-
   shaped ones, and how that interacts with descriptor-
   evolution invariants.

Size estimate: large. Touches storage index, write path, read
path, AGG, PBC, and CSB all at once. Council session: 5–8
iterations.

## Post-042 menu (ordered by compound value)

### ADR 043: Secondary indexes on non-clustering columns

**What:** Declared secondary indexes mapping `column_value →
(slab, record_location)`. Needed for joins and for predicates
not covered by clustering.

**Why third:** With clustering + stats + aggregates, many
predicates are already fast. Secondary indexes are the tool of
last resort.

**Size:** Medium-large.

### ADR 044: PK-less append-only tables

**What:** ADR 037 §19 deferred this. Streams, event logs, audit
trails. Content-hash-as-identity, auto-assigned ordinal, no
dedupe.

**Why fourth:** Unlocks streaming/CDC/audit workloads without
forcing a synthetic PK. Natural bridge to brainstorm 004.
Append-only is also the workload that benefits *most* from
ADR 040's CSB short-circuit (every sealed slab is
visibility-complete forever).

**Size:** Medium.

### ADR 045+: AGG follow-ups (multiple, individually small)

ADR 040 deferred several aggregate-protocol extensions. Each
is its own ADR when a workload pulls:

- **Float canonical encoding.** Resolves ADR 039 §15's
  deferral and unblocks `sum(float)` / `avg(float)`. Size:
  medium (NaN propagation, Kahan vs. naive vs. ordered merge,
  signed zero, subnormals).
- **Decimal SUM/AVG.** Pinned `Decimal256` overflow and scale
  contract. Size: medium (smaller than float — overflow is the
  main design surface).
- **Grouped aggregation (`GROUP BY`).** Per-slab cardinality
  caps, hash-table strategies, group-key canonical encoding,
  spill behavior. Size: large (4–6 council iterations).
- **String/Binary quantiles.** Either drop ADR 039's
  prefix-truncation for KLL inputs (separate full-byte stat),
  or invent a "rank within prefix space" semantic. Size:
  small-medium.
- **Exact `COUNT(DISTINCT)`.** Either materialize all distinct
  values per slab and merge globally (memory-bounded variants
  only), or accept the linear-counting tail when approximate
  distinct is good enough. Size: medium.
- **CSB visibility metadata consolidation.** ADR 040 V1 owns
  `first_tombstone_hlc` on the TieredSlab pointer; a future
  039-followup could formalize it alongside other visibility
  metadata (`seal_hlc`, last-write-hlc, etc.). Size: small.

These aren't sequenced — pick whichever Toron pulls first.
Most are independent and can land in any order.

## Hilbert curve (deferred)

ADR 038 left `ProjectionFunctionV1::Hilbert` parsed-but-rejected
as `PBC_REJECT_PROJECTION_NOT_YET_SUPPORTED`. A future Hilbert
ADR (numbered at opening, expected post-044 unless workload
pull surfaces earlier) lifts that rejection.

Why deferred behind aggregates and follow-ups: Z-order is
adequate for the common (≤4-dim) analytical case. Hilbert's
tighter bounding boxes are incremental improvement on locality,
while the deferred AGG follow-ups (float, decimal, GROUP BY)
unlock entire query classes. Sequence by leverage, not by
adjacency.

## Longer-range brainstorms (multi-ADR streams)

Each is its own council session when pulled by real workload
demand.

- **Brainstorm 002 — Vector embeddings as first-class.** ANN
  search integrated rather than sidecar. Composes with 038's
  locality descriptor (LSH variant), 039's slab-footer stats
  (PQ centroids), and 040's AGG (vector reduction).

- **Brainstorm 004 — Real-time CDC and triggers.** Native
  streaming from the Raft WAL. Subscribe to a `Locality` arc
  over QUIC, receive new commits as they land. Composes with
  044's PK-less tables.

- **Brainstorm 010 — Serverless PBC L2.** Cold-storage
  compute. Ship a Wendelon runtime near object storage. Three
  targets: local materialization, provider-native translation
  (S3 Select), remote runtime (Lambda/Worker).

## Status at a glance

| ADR | Scope | Status |
|---|---|---|
| 035 | Vectorized PBC executor | ✓ shipped |
| 036 | PBC pushdown contract | ✓ shipped |
| 037 | Tenant table schemas, row identity, `TableSpreadProjection` | ✓ shipped |
| 038 | Column-aware locality V1, Z-order, `ColumnProjection` | ✓ shipped |
| 039 | Slab-footer column statistics V1 | ✓ shipped |
| 040 | Aggregate pushdown protocol | ✓ shipped |
| 041 | Project-name decoupling refactor | ✓ shipped |
| 042 (proposed) | Analytical multi-row fragments | next council session |
| 043 (candidate) | Secondary indexes on non-clustering columns | post-042 |
| 044 (candidate) | PK-less append-only tables | post-042 |
| 045+ (candidates, unordered) | Float canonical encoding | deferred — pull from workload |
| 045+ (candidates, unordered) | Decimal SUM/AVG | deferred — pull from workload |
| 045+ (candidates, unordered) | Grouped aggregation | deferred — pull from workload |
| 045+ (candidates, unordered) | String/Binary quantiles | deferred — pull from workload |
| 045+ (candidates, unordered) | Exact `COUNT(DISTINCT)` | deferred — pull from workload |
| 045+ (candidates, unordered) | CSB visibility metadata follow-up | deferred — small consolidation |
| N (future) | Column-Aware Locality V2, Hilbert variant | deferred behind AGG follow-ups |
| N (brainstorm 002) | Vector embeddings first-class | deferred, multi-ADR |
| N (brainstorm 004) | CDC + triggers | deferred, multi-ADR |
| N (brainstorm 010) | Serverless PBC L2 | deferred, multi-ADR |

ADR numbers 042+ are candidate ordering; actual numbering
happens at opening.

## What to not do next

- **Don't open every brainstorm at once.** Each is its own
  council session. Pick one when Toron actually pulls.
- **Don't promote Hilbert ahead of AGG follow-ups.** It is a
  locality refinement, not warehouse infrastructure.
- **Don't open the AGG follow-ups speculatively.** Float,
  decimal, GROUP BY, string quantiles, exact distinct — each
  is deferred until measured workload pressure surfaces. The
  protocol is designed to extend cleanly (append-only tag
  numbers); waiting costs nothing.
- **Don't let Toron depend on deferred items without explicit
  ADR.** Same discipline as post-037 / post-039.
- **Don't expand ADR 040's catalog without an ADR.** New
  aggregate tags, new sketch types, new numerical-semantics
  guarantees each have their own forward-compat surface.
- **Don't reintroduce brand prefixes in protocol surfaces.**
  ADR 041's convention is enforced by the architectural test;
  flagged in code review otherwise. New protocol names use
  functional acronyms (`AGG`, `PBC`, `CSB`, `HLC`); never
  brand-prefixed (`WAP`, `WPBC`, `WCSB`, `WHLC`).
