# ADR 473 - the primitive ladder: establish the floor before optimizing the delta

Add six primitive queries to the gauge and measure them at the arc's working cell.
Every TPC-H query is a composite; the arc has spent twenty ADRs measuring *deltas*
against composites without ever establishing what a bare scan, decode, filter,
group, or join costs on this machine. Without a floor, no delta can be judged
against what is achievable — only against itself.

## What is missing today

q05 at sf=8 executes in ~21.6 s over a 6.8 GB lineitem. Nobody in this arc can say
whether that is 1.5× or 30× off the floor, because no floor exists: there is no
measurement of scan throughput, per-column decode cost, filter cost, grouping cost
at low or high cardinality, or a bare join. Each composite result has been
interpreted relative to its own arm, which is honest but blind.

## Decision — six primitives on the existing rail

Micro-queries enter the same registry the 22 use (`perf_query_sql`,
`persisted_query_table_names`), so they inherit the full apparatus for free: budget
coordinates and provenance, digests, retained samples, the stability gate, the
paired rail if ever needed. All read `lineitem` (the volume table) except the join
primitive.

| id | shape | isolates |
|---|---|---|
| `m01` | `SELECT COUNT(1) FROM lineitem` | scan floor, no column decode |
| `m02` | `SELECT SUM(l_quantity) FROM lineitem` | + one decimal column decode |
| `m03` | `SELECT SUM(l_extendedprice) FROM lineitem WHERE l_shipdate >= DATE '1995-09-01'` | + predicate evaluation, two columns |
| `m04` | `SELECT l_returnflag, COUNT(1) FROM lineitem GROUP BY l_returnflag` | low-cardinality grouping (3 groups) |
| `m05` | `SELECT l_orderkey, COUNT(1) FROM lineitem GROUP BY l_orderkey` | high-cardinality grouping (~12M groups at sf=8) — the spill-relevant primitive |
| `m06` | `SELECT COUNT(1) FROM lineitem, orders WHERE l_orderkey = o_orderkey` | one equi-join, no other work |

**Roofline arithmetic, stated before the numbers**: `lineitem` at sf=8 is
47,989,007 rows / 6,817,318,944 logical bytes. Each primitive's median yields
rows/s and logical GB/s; the ladder's differences yield per-stage marginal costs
(decode = m02 − m01, predicate = m03 − m02, grouping = m04/m05 − m01, join =
m06 − m01 scaled for the orders scan). Nothing here is a target; the point is a
denominator.

## Acceptance

- Six primitives registered, correct (digests stable across runs; the shapes are
  self-evidently checkable against row counts), and runnable through the standard
  perf rail with explicit budget coordinates.
- One measured pass at sf=8 (and sf=1 for the cheap ones) with retained samples;
  the receipt reports per-primitive median, rows/s, GB/s, and the marginal-cost
  differences, plus the composite comparison (q05's wall against the sum of its
  primitive parts).
- No optimization work in this cut. The ladder is an instrument; what it reveals is
  the next cut's problem.

## Guardrails

- **These are not benchmarks against other systems** — no external comparison, no
  claims about competitiveness; the denominator is this machine and this dataset.
- **The stability gate applies**: an unstable primitive is reported unstable, not
  re-run until pretty.
- **Marginal costs are differences of medians, not paired estimates** — stated as
  arithmetic on measurements, never as causal attributions.
- Standing gates green; no sealed verdict re-judged; the 22-query board untouched.

## Forward

The ladder's largest gap between a primitive and its composite is the arc's next
target, chosen by evidence instead of by which mechanism happened to be open.
