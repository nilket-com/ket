# ADR 474 - m02 diagnosis: locate the 20× in the per-row scalar path

ADR 473 established that Toron spends ~20× the best observed reference on a single
decimal column decode-and-sum, with no grouping, join, or filter involved. This cut
decomposes that primitive until the cost has an owner: Parquet decode, Arrow→scalar
conversion, decimal arithmetic, or per-row executor dispatch. Diagnosis only — the
fix is the next cut, chosen by what this one finds.

## Method — sub-primitives that subtract cleanly

Four additions to the ladder, all over `lineitem` at the same conditions, chosen so
that differences isolate one layer each:

| id | shape | isolates |
|---|---|---|
| `m07` | `SELECT COUNT(l_quantity) FROM lineitem` | decimal **decode** + null-check, no arithmetic |
| `m08` | `SELECT COUNT(l_orderkey) FROM lineitem` | Int64 **decode** + null-check |
| `m09` | `SELECT SUM(l_orderkey) FROM lineitem` | Int64 decode + **integer arithmetic** |
| `m02` | `SELECT SUM(l_quantity) FROM lineitem` (existing) | decimal decode + **decimal arithmetic** |

Subtractions, each a difference of medians (arithmetic on measurements, not causal
attribution):

- decimal decode = `m07` − `m01`; Int64 decode = `m08` − `m01`
- decimal arithmetic = `m02` − `m07`; integer arithmetic = `m09` − `m08`
- **decimal penalty** = (`m02` − `m07`) − (`m09` − `m08`)

`COUNT(col)` is deliberately used as the decode probe: it must touch the column
(unlike `COUNT(1)`, which ADR 473 proved is folded to metadata by every engine
including ours), while its accumulate step is a null-check and increment.

All four run against DuckDB and Polars under the ADR 473 conditions (same persisted
Parquet, one-thread references, recorded versions/threads/coordinates), so
each layer gets a ratio, not just a Toron-internal number.

## Rail 2 — the flame graph, now warranted

The ledger cannot decompose within a stage, which is exactly what is needed here;
this is the escalation ADR 473's memory note anticipated. `perf record` on a
release `m02` child (frame pointers as available), reported as the top self-cost
symbols with their share, and read jointly with the sub-primitive table: the
profile names functions, the subtractions name layers, and a claim requires both to
agree.

## Acceptance

- The four sub-primitives registered, output row counts agreeing across engines,
  and priced against both references.
- A layer table stating, per layer, Toron's cost, the reference's cost where
  separable, and the ratio; the decimal penalty isolated from the type-independent
  decode cost.
- A profile whose top symbols are consistent with the layer table; disagreement
  between the two is reported as unresolved, not reconciled by narrative.
- **A named owner for the 20×**, or an explicit statement that the evidence does
  not yet distinguish two candidates and what would.
- No optimization in this cut. No parallelism work: references ran on one thread
  and the scalar path must be understood before multi-core scaling can be priced.

## Guardrails

- **`COUNT(col)` may itself be optimized** by a reference engine using Parquet
  null-count metadata — if a reference returns implausibly fast, its decode number
  is void and is reported as such rather than used as a denominator.
- Subtractions are differences of medians; they are stated as such.
- Conditions recorded exactly as ADR 473 requires; anything unrecorded makes the
  ratio an artifact.

## Forward

The owner named here becomes the next cut's target. `m06`'s 18.6× (23.3 s versus
1.25 s for one equi-join) is the second target and likely shares this substrate —
if the scalar path explains m02, the join probe path is where it multiplies.
