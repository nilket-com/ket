# ADR 512 - Reachable-key reduction for decorrelated scalar aggregates

ADR 511 removed q17's outer build envelope; the cell now runs about 11.6 s and its dominant cost is
the decorrelated inner aggregate: a per-partkey AVG folding all 47,989,007 lineitem rows at
9,533.8 ms — roughly 82% of the cell — before the LeftOuter recombine discards every group whose
partkey never survived the part filter. The engine computes averages for about 1.6 M partkeys and
then observes about 1,600 of them.

No pass today prunes an aggregate's input by join reachability: the grounded sweep found no
semi-join reduction, no filter pushdown through GROUP BY, and no key-domain propagation anywhere in
the binder or planner. The census harness hand-writes exactly this reduction to build its q17
oracle (`q17_census_partkey_predicate`, toron-tpch/src/lib.rs:227) — the engine should own what the
harness already proves.

This ADR introduces the reduction as a **semantic optimizer transformation with a declining
recognizer**, measured on q17 as the motivating case. It is not a query-specific patch: the rule
recognizes a plan shape, proves its own preconditions, and declines whenever any of them fails.

## The transformation

Both decorrelation paths produce one recombine shape (qualified correlation in
`join_scalar_subplans`, sql_planner.rs:6461; deferred unqualified correlation in
`bind_deferred_scalar_subquery`, binder.rs:2866-2913):

```text
LeftOuter(outer, Alias[key_0..key_n, value](Aggregate(group = k, agg, Exchange(k, inner))))
    on outer_key_i = alias.key_i        -- plain equality, null_safe_keys = false at both sites
```

The rewrite inserts a semi-join under the Exchange feeding the aggregate:

```text
Aggregate(group = k, agg, Exchange(k, LeftSemi(inner, source) on k = source_keys))
```

**Equivalence backbone — superset safety.** A group whose key never appears in the recombine's
outer key domain produces a scalar row the LeftOuter can never match; eliminating any subset of
those groups is unobservable in the query result, for **every** aggregate function. The COUNT
zero-versus-NULL distinction only exists for keys the outer side actually presents, and those
groups are never eliminated because the semi build is required to be a **superset** of the outer
key domain. Supersets are always safe; the rule never needs the exact domain.

**Nulls.** Both recombine sites emit `null_safe_keys: false` (verified: sql_planner.rs:7105,
binder.rs:2902-2913), so a null inner key forms a group that can never match the recombine. The
LeftSemi's plain equality drops null-key inner rows — consistent, and strictly a reduction of
never-matched groups. The recognizer **declines any shape whose recombine keys are null-safe**
(such constructors exist elsewhere: sql_planner.rs:3566, 7604); reducing a null-safe recombine
would require a null-safe semi and is out of scope.

**Duplicates.** LeftSemi emits each probe row at most once regardless of build-side duplication,
so the source needs no dedup for correctness; duplicate outer keys and duplicate inner keys are
both exercised in the pinned tests.

## Source derivation — the bounded first cut

The semi build must be provably a superset of the outer key domain and structurally cheap. For
each correlation key, walk the **outer** logical plan: if the outer key column traces through
equi-join pairs to a single base relation `R` carrying only local filters along the path, the
source is `project_keys(filter(R))`. All keys of one recombine must trace to the same relation, or
the rule declines. The traced scan is re-planned as a second scan of `R` — no sharing machinery,
no re-execution of the outer join tree in this cut.

q17 instantiates it as: outer key `l_partkey`, equi-bound to `p_partkey` of part filtered by
brand/container → source = the roughly 1,600 filtered part keys, a trivially small semi build
against the 48 M-row lineitem scan.

If derivation fails — no equi-chain to a single base relation, a non-local predicate on the path,
composite keys spanning relations — the plan is left byte-identical to today's.

## Decision

1. **One shared rewrite helper** applied at both recombine construction sites, operating on the
   logical plan before binding, gated by `PlannerConfig::scalar_aggregate_input_reduction`
   (compiled default **off** at the seal). The helper recognizes the shape, runs source
   derivation, checks `null_safe_keys == false`, and either returns the reduced inner or declines.
2. **Gauge control**: `TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION` with the loud "0"/"1" contract;
   every persisted row carries the value and provenance.
3. **Measure, then decide the default**: plan-distinct paired cohort on q17 at strict sf=8,
   explicit 2 GiB / 2 GiB, fixed 3,554 ms floor (inherited, not recomputed), 20 child rows = 10
   adjacent pairs, from the post-ADR-511 default tree. Census diff enumerates every engaged
   q01-q22 and m00-m10 cell; every changed cell runs its supported strict correctness rail in both
   arms. Default flips on iff the verdict is `activate` and no engaged cell prices a material
   regression — ADR 472's rule, unchanged.

## Acceptance

- **Equivalence battery** (both decorrelation paths, knob on vs off, identical results): keys
  present in outer, keys absent from outer, null inner keys, null outer keys (row keeps its NULL
  scalar and the outer row survives), duplicate keys on both sides, COUNT (zero for matched empty
  groups after residual filters vs NULL for unmatched keys), AVG/MIN, and wrapper arithmetic over
  the scalar. A discriminating test proves groups eliminated are exactly never-matched ones — it
  fails under a rule that reduces with an under-approximating (non-superset) source.
- **Decline pins**: null-safe recombine shapes never engage; failed source derivation leaves the
  plan byte-identical; knob off is byte-identical everywhere.
- **Placement proof**: the LeftSemi binds beneath the Exchange without changing the exchange count,
  partition keys, or aggregate grouping above it; engaged shapes pass a distributed
  `fragment_plan` bind proof in addition to the single-node rails.
- **Census**: fingerprint diff off/on across all 22 q-cells and 11 ladder cells with per-arm
  routes, admissions, and digests; the correlated-scalar cells (q02 and q20 on the qualified path,
  q17 on the deferred path) are the expected engagement set, and any additional engaged cell
  expands correctness coverage before measurement.
- **Paired verdict** against the fixed floor from the predeclared estimator; no post-hoc
  extension.

## Guardrails

- The recognizer **declines by default**: any unproven precondition leaves the plan unchanged.
- **No new estimators**; source admission is structural (filtered single base relation), not
  cost-based, in this cut.
- The knob-off arm remains byte-identical to the post-511 production plan on every cell.
- ADR 511's placement decision is untouched; the reduction composes with the swapped join0 but
  does not depend on it.

## Risks

- The source relation is scanned twice (once in the outer, once as the semi build). The build side
  is a filtered base-relation key projection by construction; the census prices the residual cost
  on every engaged cell before any default flip.
- q02 and q20 engage through multi-join inners and (for q20) composite keys inside an IN-subquery
  — wider shapes than q17. The equivalence battery covers both paths, and their oracle rails are
  the arbiters.
- Distributed fragmenting of a semi beneath an exchange rides the existing IN/EXISTS semi-join
  machinery; the fragment_plan proof gates it explicitly.

## Forward

- **Outer-execution sharing** (true magic sets): source the semi from the outer subplan itself
  when no cheap bounding relation exists — needs plan-sharing machinery, deferred and named.
- **Physical sideways reuse**: q17's post-511 join0 already holds an in-memory hash of exactly the
  filtered part keys; sharing that build with the aggregate's scan is a runtime-filter cut,
  deferred.
- **Runtime-filter generalization** past `Inner` and off the distributed path remains the
  alternate physical route.
