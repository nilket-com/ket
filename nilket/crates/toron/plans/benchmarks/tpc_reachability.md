# TPC Benchmark Reachability: The Lighthouse Path

**Document tier:** Benchmark Lighthouse — a long-horizon
orientation document, not a commitment.

**Authors:** Gemini (initial draft); AI Swarm Council
extensions.

**Cadence.** Rewritten at planning sessions when an ADR
lands that unlocks a new level, when bench evidence
shifts a percentage, or when the TPC-side requirement
matrix changes. Between sessions the document lags the
code on purpose — same rhythm as `plans/wip/roadmap.md`.

## What this document is (and isn't)

Toron's planning system has tiers:

- `plans/adr/` — *what the architecture is*. Promoted
  contracts.
- `plans/impl/` — *how an ADR lands*. Migration plans
  and bench artifacts.
- `plans/wip/roadmap.md` — *what's next*. The near-term
  forward sequence, rewritten each planning round.
- `plans/brainstorms/` — *speculative architectures*.
  Far-horizon shapes the council is excited about; not
  committed scope.
- `plans/conventions/` — *how code should be written*.
  Recurring style and naming rules.

**This tier — Benchmark Lighthouse — sits between
brainstorms and the roadmap.** Unlike a brainstorm, it
is not speculative: TPC-H and TPC-DS are real,
externally-defined benchmark suites with byte-exact
expected output. Unlike an ADR, it commits to no
particular contract — the *path* to a publishable result
runs through many ADRs, but this document names the
*destination*. It orients ADR selection without forcing
the cadence: if no ADR is ready, no ADR ships. The
lighthouse just keeps the bow pointed.

Use this document when:
- evaluating whether a candidate ADR moves the lighthouse
  meaningfully closer;
- explaining to a newcomer where Toron sits in the
  industry's standard SQL-engine reachability ladder;
- deciding between two equally-evidenced ADRs where one
  unlocks a new lighthouse level and the other does not.

Do **not** use this document to:
- pull near-term scope forward (the roadmap and ADR-by-
  ADR cadence still drive that);
- bench against TPC at intermediate stages (publishable
  TPC results have strict provenance rules — see
  §Success definition).

## The completion-metric paradox

In a modern SQL engine, **90% of query volume is handled
by 10% of features, but the final 10% of TPC-DS (queries
like Q72, Q64) requires 90% of the architectural depth.**
Linear progress metrics — line count, ADR count, kernel
count — give a misleadingly optimistic picture. We
measure progress instead by **Query Reachability**:
*which TPC query, if any, can Toron run end-to-end
against the published answer set, on a multi-node
Wendelon-backed cluster, byte-for-byte?*

This reframe captures three asymmetries:

1. **Long-tail features dominate.** Window functions,
   correlated subqueries, recursive CTEs, advanced
   types, and CBO each lock out tens of queries until
   they exist.
2. **Composition multiplies, not adds.** A 3-way join
   plus aliased aggregate output plus date arithmetic
   is *one* query; missing any of the three blocks it.
3. **Infrastructure costs are part of the gap.** A
   correct query on a single laptop isn't publishable;
   the multi-node automation surface is itself a
   prerequisite.

## The Lighthouse Levels

Each level represents a fundamental capability jump.
Levels target **TPC-H first** (22 queries; the simpler
benchmark suite) before TPC-DS (99 queries; the modern
suite with substantially deeper requirements).

### Level 0 — Atomic Integrity (current)

- **Capability.** Single-table scans, filtered joins
  (Inner/Outer/Semi/Anti), the "Basic 5" aggregates
  (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`), `GROUP BY`,
  `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, scalar and
  grouped aggregate surfaces with multi-process
  digest parity.
- **Toron status.** In place. ADR 011 through ADR 020
  built the substrate; ADR 021 (catalog refactor) is
  promoted and implemented.
- **Bench target.** `SELECT region, AVG(amount) FROM
  sales GROUP BY region ORDER BY AVG(amount) DESC`.
  Not a TPC-H query — a unit benchmark for the
  substrate.

### Level 1 — Scan-Filter-Aggregate Baseline (TPC-H Q6, Q1)

- **Capability.** Decimal precision (V1-critical),
  Date/Interval arithmetic, `CASE WHEN` logic,
  per-column expressions in `SELECT`.
- **Why this is the first lighthouse query.** TPC-H Q6
  is famously the smallest surface area in the suite:
  one table (`LINEITEM`), date and amount filters, one
  aggregate. If Toron can't run Q6 end-to-end, no
  larger query can. Q1 follows directly — same table,
  grouped aggregate with `CASE WHEN`-derived buckets.
- **Prerequisite ADRs.**
  - Decimal numeric types ADR (in V1.x section of
    roadmap; the most-cited V1.x gap).
  - Date/Timestamp/Interval type ADR (cluster of
    follow-ons; Decimal precedent applies).
  - `CASE WHEN` SQL surface ADR.
  - Expression evaluation in `SELECT` (computed
    columns) — partially covered by ADR 023 (post-
    aggregate projection).
- **Wendelon prerequisite.** Canonical encoding for
  Decimal / Date / Timestamp / Interval (currently
  absent — see roadmap "Decimal numeric types" V1.x
  entry and ADR 014's encoded-type-widening seam).

### Level 2 — Relational Composition (TPC-H Q3)

- **Capability.** 3-way joins across distributed
  workers, aggregate aliasing, post-aggregate
  projection, multi-table query planning under hash
  partitioning.
- **Why Q3.** Customer / Orders / Lineitem 3-way join
  with grouped aggregate output and `ORDER BY` —
  exercises ADR 003/004 distributed substrate, ADR 008
  outer/anti/semi joins, ADR 011/015/016/017/018
  aggregate substrate, and post-aggregate projection
  in one query.
- **Prerequisite ADRs.**
  - ADR 023 — Post-Aggregate Projection (named in the
    roadmap forward sequence as ADR 021's runner-up).
  - Multi-way join planning (V1.x; requires cost
    model).
- **Wendelon prerequisite.** None new beyond Level 1
  type support.

### Level 3 — Optimizer Maturity (TPC-H Q8, Q9)

- **Capability.** Cost-based optimization, join
  reordering, subquery decorrelation beyond `EXISTS`
  (correlated scalar subqueries, `IN (SELECT ...)`),
  semi-join rewrites, build/probe-side selection by
  cost.
- **Why Q8 / Q9.** Q8 (National Market Share) is an
  8-table join with nested subqueries and a complex
  aggregate expression. Q9 (Product Type Profit
  Measure) requires correlated subquery flattening
  and pattern-match filters. Both expose the optimizer
  surface that V1 explicitly defers.
- **Prerequisite ADRs.**
  - Wendelon statistics surface (cross-ring; needed
    before Toron cost model has facts to work with).
  - Toron cost model ADR (V1.x — `BuildSide` field
    lands when this engages).
  - Correlated subquery decorrelation beyond
    `EXISTS`/`NOT EXISTS` (ADR 009 substrate
    extends).
  - Multi-way join planning (shares dependency with
    Level 2).
- **Wendelon prerequisite.** Statistics protocol
  surface — table-level cardinality, column-level
  distinct-counts / null-fractions / top-K / histograms.

### Level 4 — Modern Analytical Surface (TPC-DS subset)

- **Capability.** Window functions (`OVER`,
  `PARTITION BY`, frame specs), `GROUPING SETS` /
  `ROLLUP` / `CUBE`, recursive CTEs, advanced subquery
  shapes, larger-cardinality fact tables with stress
  on multi-node shuffle.
- **Why TPC-DS subset before full TPC-DS.** TPC-DS has
  99 queries; ~30 are reachable with window-function
  + grouping-set support without full CBO maturity.
  Lighting the first ~30 TPC-DS queries is a meaningful
  intermediate milestone — proves the modern analytical
  surface without the full optimizer story.
- **Prerequisite ADRs.**
  - Window function ADR (V1.x — listed in roadmap
    advanced-kernel-graduations section).
  - `GROUPING SETS` / `ROLLUP` / `CUBE` ADR.
  - Recursive CTE ADR.
  - Sort-merge join (V1.x; needed for some larger
    TPC-DS queries).
- **Wendelon prerequisite.** None new beyond Level 3
  statistics.

### Level 5 — Full TPC-DS + Publishable Provenance

- **Capability.** All 99 TPC-DS queries; multi-tenant
  governance for repeatable cluster runs;
  publish-grade provenance (load-time + run-time
  reproducibility); the harness automation from
  §Success definition below.
- **Why this is the terminal lighthouse.** TPC-DS
  publication is the industry's strongest objective
  proof of a SQL engine's maturity. Reaching this
  level means Toron + Wendelon together compete in
  the modern analytical-database arena on level
  ground with DuckDB, ClickHouse, Trino, and the
  established commercial systems.
- **Prerequisite.** Everything from Levels 0-4 plus
  multi-tenant governance ADR (Candidate A in
  roadmap), cloud-deployment automation (S3 / EKS /
  EC2), workload-management surface, and a deep
  reservoir of per-query optimization work that
  emerges only when running the full suite.

## Reachability matrix

Percentages are **rough estimates** — first-pass
calibration, not measured rates. Sharpen via TPC-H
attempt logs when Level 1 implementation begins.

| Dimension          | TPC requirement                           | Toron status | Gap                                      |
|--------------------|-------------------------------------------|--------------|------------------------------------------|
| **SQL surface**    | Full SQL-92/99 + analytical extensions    | ~15%         | Date/Interval, `CASE`, `IN`, windows     |
| **Type depth**     | Decimals, Timestamps, Intervals, Strings  | ~10%         | **Decimals** (critical), date/time       |
| **Executor**       | Window functions, sort-merge, spill       | ~15%         | Windows, large-scale Sort, sort-merge    |
| **Optimizer**      | CBO, join reordering, subquery rewrite    | <1%          | Effectively all of it (V1 explicitly defers) |
| **Aggregate**      | Basic + window + grouping sets            | ~40%         | Window aggregates, `GROUPING SETS`       |
| **Joins**          | Inner/Outer/Semi/Anti/Cross + non-equi    | ~50%         | Cross join, non-equi/theta, sort-merge   |
| **Distribution**   | Multi-node correctness + perf            | ~70%         | Skew mitigation, locality, pipelining   |
| **Infrastructure** | S3/EKS/EC2 orchestration, automation      | 0%           | Cloud connectors, harness automation     |

The strongest pillars (distribution, joins) reflect
Toron's V1 focus areas (ADR 003/004/008). The weakest
pillars (optimizer, infrastructure) are explicit V1
deferrals — the ADR cadence is honest about them.

## Brainstorm seams that compound at TPC scale

Three of the four file-backed brainstorms have
TPC-scale implications worth flagging:

- **Brainstorm 001 (Cranelift JIT).** TPC-H Q1 is
  notoriously dominated by per-row hash-bucket and
  aggregate-update overhead. A JIT'd inner-loop kernel
  closes the gap between Toron's interpreted execution
  and the bare-metal throughput DuckDB / ClickHouse
  / commercial systems publish.
- **Brainstorm 003 (Disaggregated Compute Roles).**
  TPC-DS at terabyte scale rewards storage-compute
  separation: elastic compute against a stateful
  Wendelon data plane. The disaggregation question
  becomes binding at Level 5, not earlier.
- **Brainstorm 004 (Memory Arenas).** Query-plan
  allocation overhead is a measurable fraction of
  wall-clock on dispatch-heavy TPC-DS queries. The
  ADR 021 catalog's `#[repr(u8)]` `StableAggregateKernel`
  tag is already compatible with the SoA plan-payload
  shape Brainstorm 004 would adopt — recorded in
  ADR 021 §SoA / SIMD forward compatibility.

Brainstorm 002 (WeakControlRing) is orthogonal to TPC
reachability — it's about coordinator HA, not analytical
throughput.

## Success definition: "publishable results"

A TPC result is publishable when:

1. **Correctness.** Query results match the TPC-
   provided answer sets byte-for-byte (normalized
   ordering where the TPC spec permits).
2. **Integrity of run.** Executed on a multi-node AWS
   cluster (EKS or EC2) with data hosted in
   Wendelon-native storage. Single-machine results
   don't count; an honest TPC publication requires
   network, disk, and coordinator costs at scale.
3. **Reproducibility.** The full bench harness —
   provision → load → run → tear-down → result
   capture — is automated. A future reviewer should
   be able to re-run the bench with one command and
   reproduce the result within statistical noise.
4. **Provenance.** The exact commit hashes (Toron,
   Wendelon, harness), cluster hardware spec, and
   data-load checksum are recorded alongside the
   result. The result is reproducible-by-anyone, not
   trust-the-publisher.

Currently, Toron is at **~0.02%** of a publishable
TPC-DS result — Level 0 of 5, with no infrastructure
automation. The number isn't meant to be precise; it's
meant to be honest. Each lighthouse level moves it by
~20 percentage points.

## How this document updates

- **When an ADR lands that unlocks a new level**, the
  level's "Toron status" line moves from gap to
  in-place, and any percentage in the reachability
  matrix shifts.
- **When bench evidence shifts a percentage**, the
  matrix updates with a note linking to the bench
  artifact.
- **When the TPC spec changes** or a new TPC variant
  becomes industry-relevant (TPC-DS v3, new TPCx-AI
  edition, etc.), the levels themselves are revisited.
- **When a brainstorm graduates** to an ADR or
  reverses, the §Brainstorm seams section updates.

The document does not update at every commit. Like the
roadmap, it lags the code on purpose — read alongside
`git log` and per-ADR bench artifacts for ground truth
between planning sessions.

## What this document does not contain

- Detailed query-by-query Toron-side analysis. A
  separate document (or per-query bench artifact in
  `plans/impl/`) would handle that once we're actually
  attempting TPC queries.
- Performance targets per level. The TPC suites have
  their own scaling factors and time-bound metrics
  (`tpcH@SF100`, `tpcDS-RT@SF1000`); naming targets
  before correctness is in hand is premature.
- A timeline. Lighthouses are direction, not commitment.
  The ADR cadence determines pace; this document
  determines bearing.
