# Roadmap: from 038 to 040 and beyond (transient nav doc)

Author: Claude (AI Swarm Council)
Scope: transient navigation — retires whenever `plans/wip/` is cleaned up again.
Written after ADRs 035, 036, 037, 038, 039 shipped.

## Where we are

The warehouse story now has end-to-end leverage:

- **ADR 035** — vectorized WPBC executor. Arrow kernels, scalar oracle, benches.
- **ADR 036** — WPBC as the versioned Toron→Wendelon pushdown contract.
- **ADR 037** — typed tenant tables, PK-derived row identity,
  `TableSpreadProjection` keeping tables on a bounded set of arcs.
- **ADR 038** — column-aware locality V1 (Z-order), descriptor-authoritative.
  A range query on declared clustering columns now maps to a small set of
  contiguous ring arcs, capped at the planner's 1024-arc budget.
- **ADR 039** — slab-footer column statistics V1. Encrypted CSB between
  `encrypted_chunks` and `offset_index`; locator in the formerly-reserved
  32 bytes of `slab_footer`; always-on min/max/null/non-null per column;
  opt-in bloom/HLL/small-set; compaction recomputes; internal pruning
  invisible to Toron behind a `slab_stats_pruning_v1` capability bit.

What's true now that wasn't true a week ago:

- Within a contiguous arc set, slabs whose `min`/`max` bounds preclude
  the predicate are skipped before any chunk-stream GET fires. ADR 038's
  locality narrowing × ADR 039's stats pruning compose multiplicatively.
- Stats are encrypted under the slab DEK; the footer HMAC covers only
  the locator (including a 16-byte ciphertext hash), so point reads that
  don't need stats don't pay for them.
- `TieredSlab` carries three small advisory fields
  (`stats_format_version`, `stats_block_len`, `stats_block_hash128`)
  so a planner can skip the stats fetch entirely when no predicate is
  representable.

## The near map

```
                       YOU ARE HERE
                            ▼
       ┌─────────────────────────────────┐
       │ 035 / 036 / 037 / 038 / 039     │ ← shipped
       └──────────┬──────────────────────┘
                  │
     ┌────────────┼────────────┐
     │            │            │
     ▼            ▼            ▼
 stabilization  quick wins   ADR 040
 (1-2 weeks)   (small ADRs)  partial aggregate
                              pushdown protocol
                              │
                              ▼
                 ┌────────────┴────────────┐
                 │      post-040 menu      │
                 │ ordered by compound value│
                 └────────────┬────────────┘
                              ▼
              multi-row fragments → secondary indexes →
              PK-less tables → Hilbert (when warranted) →
              L2 serverless
```

## Immediate (1–2 weeks post-merge)

Let 038 and 039 settle together. Specifically:

1. **Stats prune ratio.** `wpbc_slab_stats_pruned_total` /
   `wpbc_slab_stats_consulted_total` shows the fraction of candidate
   slabs that the CSB eliminates. Per-tenant variance reveals which
   workloads benefit and which pay the encryption tax for nothing.
2. **CSB size and decrypt latency.** `wpbc_slab_stats_block_bytes`
   and `wpbc_slab_stats_decrypt_micros` tell us whether the
   `min(256 KiB, slab_payload_bytes / 256)` cap is right and whether
   the local stats cache (Phase 9) is hitting often enough to amortize.
3. **`TieredSlab` drift.** `wpbc_slab_stats_pointer_drift_total`
   should stay at zero. Anything else means a control-ring/footer
   advisory mismatch — investigate before it confuses planners.
4. **Real clustering effectiveness.** Carryover from 038:
   `wpbc_locality_arcs_scanned_total` and
   `wpbc_range_decomposition_overflow_total` show whether the 1024-arc
   budget is right.
5. **Width-budget pressure.** Carryover from 038: a tenant declaring
   4D clustering eats 48 suffix bits with `prefix_bits=16`; if real
   workloads want tighter per-column widths, the descriptor reshape
   is a rewrite migration.
6. **Encode-cost under bulk write.** Carryover from 038: Z-order
   encoding is cheap; the BMI2 path is bonus. Bench gates should be
   checked under real ingest, not synthetic batches.

Carries over from the post-037 watch list:
- Ingest-splitting CPU under bulk writes (037).
- Rebatching effectiveness (`runtime={vectorized,scalar_reference}`).

## ADR 040 — Partial aggregate pushdown sibling protocol

**Seed:** ADR 036 §10 reserved this. Surfaced as next council session
now that ADR 039 has shipped.

**What:** `COUNT`, `SUM`, `AVG`, `MIN`/`MAX`, quantile sketches
(t-digest, KLL), approximate `DISTINCT`. Computed at the storage tier;
only the merged sketch returns to Toron.

**Why first after 039:** Declared columns + clustering + stats already
let Wendelon skip slabs that can't match. The next obvious leverage is
"compute partial aggregates beside storage" — collapse a billion-row
`SELECT COUNT(*)` to a constant before any rows leave the storage tier.
Quantile sketches were deliberately deferred from ADR 039 to this
sibling protocol.

**What 040 has to decide (the new design surface):**

1. Which aggregates ship in V1 vs. opt-in vs. deferred.
2. Sketch type for quantiles (t-digest vs. KLL vs. something else),
   merge semantics, and on-disk format.
3. Approximate-distinct: extend ADR 039's HLL infrastructure or
   introduce a new sketch?
4. Wire format for partial-result merge between storage nodes
   before the result reaches Toron.
5. WPBC integration — new opcodes, or a sibling bytecode dialect?
6. Numerical-semantics guarantees (associativity, NaN handling,
   overflow detection) that the protocol must promise to Toron.

Size estimate: medium-large. New protocol surface, careful numerical
semantics, Toron-side planner integration. Council session: 4–6
iterations.

## Post-040 menu (ordered by compound value)

### ADR 041: Analytical multi-row fragments

**What:** Relax ADR 037's "one slab record = one row" invariant for
analytical-shaped tables. A slab record carries a packed N-row Arrow
batch with a row-level PK index inside it.

**Why next after 040:** Single-row records are correct for MVCC but
wasteful for huge analytical tables. With clustering (038), stats
(039), and aggregates (040) in place, single-row cost becomes the
bottleneck.

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
| 039 | Slab-footer column statistics V1 | ✓ shipped |
| 040 (proposed) | Partial aggregate pushdown sibling protocol | next council session |
| 041 (candidate) | Analytical multi-row fragments | post-040 |
| 042 (candidate) | Secondary indexes on non-clustering columns | post-040 |
| 043 (candidate) | PK-less append-only tables | post-040 |
| N (future) | Column-Aware Locality V2, Hilbert variant | deferred behind stats+aggregates |
| N (future) | Float/decimal stat semantics | deferred — ADR 039 §15 type tags 0x09–0x0C reserved |
| N (brainstorm 002) | Vector embeddings first-class | deferred, multi-ADR |
| N (brainstorm 004) | CDC + triggers | deferred, multi-ADR |
| N (brainstorm 010) | Serverless WPBC L2 | deferred, multi-ADR |

ADR numbers 041+ are candidate ordering; actual numbering happens at
opening.

## What to not do next

- **Don't open every brainstorm at once.** Each is its own council
  session. Pick one when Toron actually pulls.
- **Don't promote Hilbert ahead of aggregates.** It is a locality
  refinement, not warehouse infrastructure.
- **Don't let Toron depend on deferred items without explicit ADR.**
  Same discipline as post-037.
- **Don't expand ADR 039's stats catalog without an ADR.** Quantile
  sketches, float min/max, and distinct cardinalities for high-card
  columns each have their own forward-compat surface; they earn their
  way in through 040 (quantiles), the float follow-up (floats), or
  measured workload pressure (distincts).
