# ADR 320 - HAVING scalar subquery recombine after aggregation (q11)

## Status
Proposed.

## Context

Atlas engine cut #3 - q11's measured wall (ADR 314): `unsupported_having_scalar_subquery@plan`. q11 is the last
measured reach red that looks like a **contained operator-composition gap**, not a larger predicate/decorrelation
family. The fix is the mechanism the code already names as deferred: **recombine an *uncorrelated, one-row,
global-aggregate* scalar subquery ABOVE the GROUP BY aggregate**, so the HAVING filter can compare each group's
value to it.

```sql
SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > (SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 FROM ... 'GERMANY')
ORDER BY value DESC
```

## The wall (grounded)
`sql_planner.rs:3811-3821`: ADR 257 pulls uncorrelated provably-scalar subqueries into scalar `Cross` joins, but
only **pre-aggregation** (WHERE). A HAVING (or aggregate/grouped-projection) scalar **must recombine *above* the
aggregate**. The frontend still rejects it with the deferred-mechanism comment that "the aggregate's group-keyed
exchange is not an exchange-free probe input for the broadcast scalar `Cross`."

That comment must be re-grounded before coding: current `binder/join.rs` already has a `ScalarBroadcastCross`
branch for a provably one-row right side (`row_upper_bound <= 1`), and its local comment says the scalar side is
broadcast-eligible even when composite. So the likely cut is frontend placement (collect and join the scalar after
aggregate lowering), not necessarily a binder admission change.

## Phase 0 - prove whether the exchange-free-probe objection is still real (before coding)
The scalar `(SELECT SUM(...) * 0.0001 ...)` is a **non-correlated global aggregate -> exactly one row**. Ground:
- Bind a synthetic `Aggregate(...) Cross Alias(global_aggregate_scalar)` shape directly. If it binds as
  `ScalarBroadcastCross`, the old frontend objection is stale and this cut should stay in `sql_planner.rs`
  placement/rewrite logic.
- If the synthetic post-aggregate Cross still rejects, identify the actual admission or fragmenter constraint before
  changing behavior; only then touch binder admission.
- Can ADR 240's scalar-local `Cross` (`row_upper_bound <= 1`) / the ADR 282 scalar-recombine machinery be placed
  **post-aggregate** (above the GROUP BY) instead of pre-aggregate?
- Choose the smallest placement and record it in the results doc: (i) frontend-only post-aggregate scalar collection
  if existing scalar-broadcast admission holds, or (ii) a narrowly-scoped binder/admission fix if Phase 0 proves the
  old objection is still real.

## Decision - recombine the 1-row global-aggregate scalar above the GROUP BY aggregate
A `crates/toron` frontend/binder cut, expected to be primarily SQL lowering unless Phase 0 proves an admission gap.
Instead of rejecting at the GROUP BY/HAVING scalar fence, collect the uncorrelated 1-row scalar, build the outer
aggregate, then recombine the scalar **after** the aggregate so HAVING references it. The HAVING lowers as a
post-aggregate Filter on the recombined scalar column. Reuse the existing scalar-recombine machinery (ADR 240
scalar-local `Cross` / ADR 282 scalar-recombine / `join_scalar_subplans`) at the smallest placement Phase 0 allows.

## Scope / boundaries - the safe first slice ONLY
**In scope:** uncorrelated, **one-row**, global-aggregate scalar subqueries in **HAVING**, recombined above the
outer aggregate output.

**Deferred - each a separate mechanism, explicitly NOT this cut (do not let it drift into generic HAVING-subquery
support):**
- **Correlated HAVING scalars** - a scalar correlated to the outer group/row (needs per-group decorrelation keyed by
  the grouped output). Stays rejected.
- **Multi-row / non-scalar subqueries** with a **cardinality assert** (a subquery that may return >1 row, asserted
  scalar at runtime) - a distinct assert mechanism. Stays rejected.
- **Non-aggregate scalar cardinality checks** (a non-global-aggregate "provably one row" subquery in HAVING) -
  separate provability path. Stays rejected.
- **Aggregate/grouped-projection scalars** (the sibling GROUP BY/HAVING scalar fence) - fold *only* if Phase 0 shows
  it is the exact same post-aggregate placement; otherwise note + defer.
- WHERE scalars untouched (already work, ADR 257).

## Acceptance - verify-the-flip (q11 may not fully green here)
- **q11 moves off `unsupported_having_scalar_subquery@plan`** - verify-first re-measure. **A deeper measured
  blocker is a good red row, not a failed cut:** q11's `ps_supplycost * ps_availqty` is a **Decimal x Int-column
  multiply** (the ADR-314-predicted strict wall, *behind* the HAVING-scalar). Expect **dev = pass@execute (or
  deeper), strict = `unsupported_decimal_arithmetic@bind`** -> names **ADR 321 (Decimal x Int multiply)**.
- **Correct recombine:** each group's `SUM` compares to the *single* global scalar (computed once, not
  re-aggregated per group); an engine test pins `HAVING agg > (SELECT global-agg ...)` vs an explicitly-recombined
  equivalent.
- **Keyed/tractable** at sf=0.01 (1-row broadcast scalar - no cartesian).
- Full `-p toron --no-fail-fast` + `--all-targets` + `architecture_test` green (scalar pull-up + recombine +
  admission are shared); **q15/q18/q19 + all other reach + correctness cells unchanged**.

## Risks
- **The exchange-placement is the crux** - Phase 0 must establish whether current scalar-broadcast admission already
  accepts a post-aggregate scalar Cross; wrong read = wrong fix.
- **Scope drift** - the deferred list above is load-bearing; keep this the uncorrelated-1-row-global-aggregate
  slice only.
- **Don't over-claim q11 green** - acceptance is "moves off the HAVING-scalar wall," naming ADR 321 if strict
  walls on the Decimal x Int multiply.
- **Shared scalar/recombine path** - full suite is the net; must not change WHERE-scalar behavior.

## Forward arc
q11 -> past the HAVING-scalar wall -> **ADR 321 (Decimal x Int-column multiply, the ADR-314 prediction)** -> q11
green. Atlas remaining after: q18 (grouped EXISTS-body, then re-measure the composite path against q15's A1
substrate), q21 (non-equi EXISTS). The deferred HAVING-subquery mechanisms above (correlated / multi-row-assert /
non-aggregate-cardinality) become their own ADRs only if a future query nominates them.

## Why toron-home
A `crates/toron` SQL-lowering/binder cut (scalar pull-up + post-aggregate recombine; admission only if Phase 0
proves it is needed). q11's SQL and fixtures stay unchanged; the harness expectation flips from the measured wall
to the new row. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. q11 = the last contained operator-composition reach red. Framed narrowly: HAVING scalar
recombine *after* aggregation, **uncorrelated one-row global-aggregate only**. Codex's deferrals made first-class
(correlated HAVING scalars / multi-row asserts / non-aggregate cardinality checks / generic HAVING-subquery support,
all separate mechanisms). Phase 0 proves the exchange-free-probe objection before coding; acceptance is movement
off the wall (a Decimal x Int strict red is a good row, -> ADR 321), not automatic q11 green.
