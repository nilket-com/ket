# Roadmap: from 038 to 039 and beyond (transient nav doc)

Author: Claude (AI Swarm Council)
Scope: transient navigation — retires whenever `plans/wip/` is cleaned up again.
Written after ADRs 035, 036, 037, 038 shipped.

## Where we are

The warehouse story is genuinely credible now:

- **ADR 035** — vectorized WPBC executor. Arrow kernels, scalar oracle, benches.
- **ADR 036** — WPBC as the versioned Toron→Wendelon pushdown contract.
- **ADR 037** — typed tenant tables, PK-derived row identity,
  `TableSpreadProjection` keeping tables on a bounded set of arcs.
- **ADR 038** — column-aware locality V1 (Z-order), descriptor-authoritative.
  A range query on declared clustering columns now maps to a small set of
  contiguous ring arcs, capped at the planner's 1024-arc budget.

What's true now that wasn't true a week ago:

- Range queries on declared clustering columns hit a contiguous arc set,
  not the whole ring.
- Per-column declared bit widths let tenants encode workload shape exactly.
- Migration semantics are explicit: `spread_factor` increase is a soft
  jump-hash delta migration; everything else is a full rewrite.

## The near map

```
              YOU ARE HERE
                   ▼
       ┌─────────────────────────┐
       │ 035 / 036 / 037 / 038   │ ← shipped
       └──────────┬──────────────┘
                  │
     ┌────────────┼────────────┐
     │            │            │
     ▼            ▼            ▼
 stabilization  quick wins   ADR 039
 (1-2 weeks)   (small ADRs)  slab-footer
                              column stats
                              │
                              ▼
                 ┌────────────┴────────────┐
                 │      post-039 menu      │
                 │ ordered by compound value│
                 └────────────┬────────────┘
                              ▼
              partial aggregates → multi-row fragments →
              secondary indexes → PK-less tables →
              Hilbert (when warranted) → L2 serverless
```

## Immediate (1–2 weeks post-merge)

Let 038 settle. Specifically:

1. **Real clustering effectiveness.** The `wpbc_locality_arcs_scanned_total`
   and `wpbc_range_decomposition_overflow_total` metrics show whether the
   1024-arc budget is right. Tune from real predicate shapes, not synthetic.
2. **Width-budget pressure.** A tenant declaring 4D clustering eats 48
   suffix bits with `prefix_bits=16`; if real workloads want tighter
   per-column widths, the descriptor reshape is a rewrite migration. Watch
   for early signal.
3. **Encode-cost under bulk write.** Z-order encoding is cheap; the BMI2
   path is bonus. The bench gates from Phase 8 should be checked under
   real ingest, not just synthetic batches.

Carries over from the post-037 watch list:
- Ingest-splitting CPU under bulk writes (037).
- Rebatching effectiveness (`runtime={vectorized,scalar_reference}`).

## ADR 039 — Slab-footer column statistics

**Seed:** post-037 roadmap §"Slab-footer column statistics" (highest leverage).

**What:** `min`, `max`, `null_count`, `distinct_count` for each column per
slab, plus optional bloom filters for equality predicates and dictionary
encoding for low-cardinality columns. Stored in the slab footer (ADR 022
territory).

**Why first after 038:** Smallest implementation, enormous planner benefit.
Toron's planner can skip entire slabs by consulting footer stats before
issuing any read. Combined with 038's column-aware clustering, an
`event_time > '2026-04-01'` predicate eliminates 99% of cold-storage
scan without reading any data bytes. This is how BigQuery/Snowflake
make billion-row scans fast.

**What 039 has to decide (the new design surface):**
1. Which stats to maintain by default vs. opt-in via descriptor.
2. Bloom filter bit budget per slab and false-positive target.
3. Dictionary-encoding threshold (cardinality cutoff).
4. Compaction interaction — stats are recomputed on compaction output;
   does the slab-footer carry per-input lineage or just merged stats?
5. Lazy stats computation vs. eager — if a slab is written without stats,
   can a background pass add them, and what does that mean for footer
   immutability?
6. Toron planner contract — how stats are surfaced over the
   Toron→Wendelon boundary (probably extends ADR 036).

Size estimate: similar to 037 but with less novel descriptor design.
Council session, 4–6 iterations, merge proposal, ratification.

## Post-039 menu (ordered by compound value)

### ADR 040: Partial aggregate pushdown sibling protocol

**What:** ADR 036 §10 reserved this. `COUNT`, `SUM`, `AVG`, `MIN`/`MAX`,
quantile sketches (t-digest, KLL), approximate `DISTINCT`. Computed at
the storage tier; only the merged sketch returns to Toron.

**Why second:** Once you have declared columns + clustering + stats, the
next obvious win is "compute partial aggregates beside storage." The
other half of what makes BigQuery fast.

**Size:** Medium-large. New protocol surface, careful numerical semantics,
Toron-side planner integration.

### ADR 041: Analytical multi-row fragments

**What:** Relax ADR 037's "one slab record = one row" invariant for
analytical-shaped tables. A slab record carries a packed N-row Arrow
batch with a row-level PK index inside it.

**Why third:** Single-row records are correct for MVCC but wasteful for
huge analytical tables. Once clustering (038), stats (039), and
aggregates (040) are in place, single-row cost becomes the bottleneck.

**Size:** Large. Touches storage index, write path, read path.

### ADR 042: Secondary indexes on non-clustering columns

**What:** Declared secondary indexes mapping `column_value → (slab,
record_location)`. Needed for joins and for predicates not covered by
clustering.

**Why fourth:** With clustering + stats, many predicates are already
fast. Secondary indexes are the tool of last resort.

**Size:** Medium-large.

### ADR 043: PK-less append-only tables

**What:** ADR 037 §19 deferred this. Streams, event logs, audit trails.
Content-hash-as-identity, auto-assigned ordinal, no dedupe.

**Why fifth:** Unlocks streaming/CDC/audit workloads without forcing a
synthetic PK. Natural bridge to brainstorm 004.

**Size:** Medium.

## Hilbert curve (deferred)

ADR 038 left `ProjectionFunctionV1::Hilbert` parsed-but-rejected as
`WPBC_REJECT_PROJECTION_NOT_YET_SUPPORTED`. A future Hilbert ADR
(numbered at opening, expected post-043) lifts that rejection.

Why deferred behind stats and aggregates: Z-order is adequate for the
common (≤4-dim) analytical case. Hilbert's tighter bounding boxes are
incremental improvement on locality, while stats and aggregates are
infrastructure that compose with any locality scheme. Sequence by
leverage, not by adjacency.

## Longer-range brainstorms (multi-ADR streams)

Each is its own council session when pulled by real workload demand.

- **Brainstorm 002 — Vector embeddings as first-class.** ANN search
  integrated rather than sidecar. Composes with 038's locality
  descriptor (LSH variant), 039's slab-footer stats (PQ centroids),
  and 040's partial aggregates (vector reduction).

- **Brainstorm 004 — Real-time CDC and triggers.** Native streaming
  from the Raft WAL. Subscribe to a `Locality` arc over QUIC, receive
  new commits as they land. Composes with 043's PK-less tables.

- **Brainstorm 010 — Serverless WPBC L2.** Cold-storage compute. Ship
  a Wendelon runtime near object storage. Three targets: local
  materialization, provider-native translation (S3 Select), remote
  runtime (Lambda/Worker).

## Status at a glance

| ADR | Scope | Status |
|---|---|---|
| 035 | Vectorized WPBC executor | ✓ shipped |
| 036 | WPBC pushdown contract | ✓ shipped |
| 037 | Tenant table schemas, row identity, `TableSpreadProjection` | ✓ shipped |
| 038 | Column-aware locality V1, Z-order, `ColumnProjection` | ✓ shipped |
| 039 (proposed) | Slab-footer column statistics | next council session |
| 040 (candidate) | Partial aggregate pushdown sibling protocol | post-039 |
| 041 (candidate) | Analytical multi-row fragments | post-039 |
| 042 (candidate) | Secondary indexes on non-clustering columns | post-039 |
| 043 (candidate) | PK-less append-only tables | post-039 |
| N (future) | Column-Aware Locality V2, Hilbert variant | deferred behind stats+aggregates |
| N (brainstorm 002) | Vector embeddings first-class | deferred, multi-ADR |
| N (brainstorm 004) | CDC + triggers | deferred, multi-ADR |
| N (brainstorm 010) | Serverless WPBC L2 | deferred, multi-ADR |

ADR numbers 040+ are candidate ordering; actual numbering happens at
opening.

## What to not do next

- **Don't open every brainstorm at once.** Each is its own council
  session. Pick one when Toron actually pulls.
- **Don't bundle 039 with bloom filters by default.** Min/max/null/distinct
  is the safe MVP; bloom filters and dictionary encoding earn their way
  in via measured planner pressure.
- **Don't promote Hilbert ahead of stats+aggregates.** It is a locality
  refinement, not warehouse infrastructure.
- **Don't let Toron depend on deferred items without explicit ADR.**
  Same discipline as post-037.
