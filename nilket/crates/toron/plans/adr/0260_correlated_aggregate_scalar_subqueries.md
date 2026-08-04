# ADR 260 — Correlated aggregate scalar subqueries (grouped-aggregate decorrelation)

## Status

Proposed.

## Context

257 pulls an uncorrelated, provably-scalar (global-aggregate) subquery up via
`pull_up_scalar_subqueries` (`sql_planner.rs:5565`): it rewrites the subquery node to a
`__scalar_N.value` column reference, collects the row=1 subplan, and folds it onto the outer plan as a
**keyless `ScalarBroadcastCross`** (`cross_join_scalar_subplans`, `:5744`).
`validate_scalar_subquery_is_global_aggregate` (`:9155`) gates the shape and **rejects any outer
reference** — at `:9207` (the subquery projection / aggregate argument) and `:9212` (the subquery
WHERE).

A correlated aggregate scalar —

```sql
SELECT fact.pk,
       (SELECT MAX(dim.v) FROM dim WHERE dim.k = fact.k AND dim.active)  -- correlated on dim.k = fact.k
FROM fact
```

— cannot be a global aggregate (its value depends on the outer row). But it projects **exactly one
aggregate**, so the correlation decorrelates into a **grouped aggregate keyed on the correlation
key**, recombined with a **`LeftOuter` join**:

```sql
fact
LEFT JOIN (SELECT dim.k, MAX(dim.v) AS value      -- group by the correlation key
           FROM dim WHERE dim.active GROUP BY dim.k) __scalar_0
       ON fact.k = __scalar_0.k                    -- the correlation pair becomes the join `on`
-- the rewritten projection references __scalar_0.value
```

Two properties make this the correct lowering, with no runtime cardinality check:

- **GROUP BY the correlation key ⇒ structurally ≤1 row per outer row.** One group per key, so the
  `LeftOuter` matches at most one. The scalar-cardinality constraint holds by construction — this is
  exactly why the *aggregate* case needs no cardinality-assert mechanism and the non-aggregate case
  (which cannot group) does. The code already draws this line: a non-aggregate scalar rejects at the
  earlier `:9186` gate (`"non-aggregate scalar subqueries are not supported"`), so the
  `"correlated …"` string at `:9207`/`:9212` only ever fires for an *aggregate* scalar.
- **`LeftOuter` ⇒ correct empty-correlation semantics.** An outer row whose correlation key matches no
  inner group is **preserved** with a NULL aggregate value — what an empty scalar subquery returns for
  MAX/MIN/SUM/AVG. (An `Inner` join would wrongly *drop* that row.)

## Grounding

Everything the aggregate decorrelation needs is landed:

- **`JoinKind::LeftOuter`** is bindable and executable (`binder.rs:464`).
- **`LogicalPlan::Aggregate` with GROUP BY** + the full grouped-aggregate lowering, including the
  group-keyed `Exchange` insertion (`sql_planner.rs:3847`).
- **`classify_exists_conjunct`** (`:5519`; `ExistsConjunct::CorrelatedPair { outer, inner }` /
  `InnerResidual`, `:8783`) — the WHERE splitter 256/258/259 already reuse to separate the correlation
  equi-pair from the inner residual. 260 reuses it again.

## Decision — lift the WHERE-correlation reject, decorrelate via grouped aggregate + `LeftOuter`

In `pull_up_scalar_subqueries`, stop rejecting a **WHERE** outer reference; classify the subquery
WHERE through `classify_exists_conjunct` into correlation pairs and inner residuals, and:

1. **Inner residuals** stay a `Filter` on the inner plan, pre-aggregation (unchanged from the EXISTS
   path).
2. **Correlation pairs present** → build a grouped aggregate over the inner correlation keys
   (`Exchange(inner, key = inner_keys)` → `Aggregate { group_expr = inner_keys, aggr_expr = the one
   scalar agg }`), alias it `__scalar_N` exposing `[inner_keys…, value]`, and fold it with a
   **`LeftOuter`** join keyed `on = [(outer_i, __scalar_N.inner_i)…]`, **built directly** via
   `build_join_plan` (see Distribution).
3. **Correlation pairs empty** → 257's keyless `Cross` over the global aggregate, byte-identical
   (including COUNT — no extra wrapper on the uncorrelated path).
4. The scalar-expression rewrite to `__scalar_N.value` is unchanged **except** correlated COUNT
   (next section).

The **aggregate-argument** correlation reject (`:9207`, e.g. `MAX(dim.v + fact.x)`) stays — that case
is undecorrelatable by GROUP BY (the aggregate depends on the outer row per-row). A **non-equi** WHERE
correlation now rejects through the classifier (`"unsupported EXISTS subquery predicate"`), uniform
with EXISTS/IN.

### The COUNT empty-group rule (load-bearing)

The correlated `LeftOuter` yields NULL for a no-match outer row. Correct for `MAX/MIN/SUM/AVG`
(empty-set identity NULL); **wrong for `COUNT`** (empty-set identity `0`). So when a **correlated**
scalar aggregate is `CountAll`/`Count`, the rewritten value is `COALESCE(__scalar_N.value, 0)` (which
lowers to the existing `Expr::Case`, no new expr/op). This is per-aggregate empty-set identity — only
COUNT has a non-NULL one. The uncorrelated COUNT path remains 257's global aggregate result and is not
wrapped.

### Surface

Frontend (`sql_planner.rs`) only — `pull_up_scalar_subqueries`, its validation helper, the
`PulledScalar` collection, and the fold (`cross_join_scalar_subplans` → `join_scalar_subplans`).
**No IR node, execution op, fragmenter path, binder strategy, or new join kind.** Naming flows through
`Project.output_names` and the relation-level `Alias.columns`; there is **no `Expr::Alias`** in the IR.

## Distribution (verify-first gate — run and proven)

The recombine is a `LeftOuter` whose right input is a grouped `Aggregate`. The broadcast-over-aggregate
hazard does **not** apply, because a `LeftOuter` cannot take the broadcast path:
`fragment_broadcast_join_plan` rejects any non-inner kind (`fragment.rs:775`,
`BroadcastNonInner`), forcing the **hash** path. `fragment_hash_join_plan` requires each input to carry
an Exchange and yield `first_scan_metadata`; the grouped `Aggregate` already inserts its group-keyed
Exchange and `first_scan_metadata` descends through `Aggregate`.

A probe of the decorrelated shape binds and fragments as
`LeftOuter / HashPartitioned / 3 stages` (left producer + grouped-aggregate producer + join consumer).
The probe is kept as a permanent gate,
`distributed_process_test::adr260_verify_probe_left_outer_over_grouped_aggregate_fragments`, and rides
the implementation commit.

**Finding (confirms the cut; does not change it).** The SQL surface *rejects* an inline
`LEFT JOIN (subquery)` in a non-inner chain (`lower_select:3679`,
`"derived tables in joins are only supported in all-inner chains"` — the WHERE-pushdown path is wired
only for inner chains). 260 **never hits this guard**: it builds the `LeftOuter` node **directly** via
`build_join_plan`, not by lowering a SQL derived-table join. The probe expresses the shape via a CTE (a
named table factor bypasses the guard) as the faithful plan-shape proxy until the lowering lands.

## Scope

- **In**: a scalar subquery of one aggregate over one table — no GROUP BY/HAVING/DISTINCT — used as a
  value in a **non-aggregate** outer query's projection or WHERE (257's placements), correlated to the
  outer by one or more **equi-conjuncts** in its WHERE; inner residuals fold in; COUNT empty-group
  identity handled.
- **Out / deferred (mechanism boundaries)**:
  - **Aggregate-argument correlation** (`MAX(dim.v + fact.x)`, `:9207`) — undecorrelatable by GROUP
    BY; stays rejected `"correlated scalar subqueries are not supported"` (the surviving string).
  - **Non-aggregate correlated scalar** — needs the ≤1-row cardinality-assert mechanism (no
    `single_value`/assert-one aggregate exists); its own cut.
  - **Scalar in GROUP BY/HAVING / aggregate-query projection** (`:3767`,
    `"scalar subqueries in GROUP BY/HAVING are not supported"`) — 257's boundary, the forward-arc
    "HAVING/aggregate scalars" item.
  - Non-equi correlation; cross-type join-key coercion; multi-column scalar; **DISTINCT inside the
    scalar aggregate** (defer explicitly; do not reopen the core cut).

## Discriminators (each fails on `main`)

- **`correlated_aggregate_scalar_decorrelates_to_left_outer`** *(execute)* — `SELECT fact.pk, (SELECT
  MAX(dim.v) FROM dim WHERE dim.k = fact.k) FROM fact`; the per-row MAX differs from the uncorrelated
  global `MAX(dim.v)`, and an outer row with no matching dim group is **preserved with a NULL value**
  (locks `LeftOuter` vs `Inner`).
- **`correlated_scalar_recombines_as_keyed_left_outer`** *(plan-shape)* — the recombine is a
  `LeftOuter` with `on = [(fact.k, __scalar_0.k)]`, right input an `Aggregate` grouped on `dim.k`
  computing `MAX(dim.v)` — not a keyless `Cross`.
- **`correlated_count_scalar_empty_group_is_zero`** *(execute)* — an outer row with no matching inner
  group gets `COUNT = 0`, not NULL. Fails on `main` (rejected) **and** under a naive un-COALESCE'd
  `LeftOuter` — the test that locks the COUNT rule.
- **`correlated_aggregate_scalar_with_inner_residual`** *(execute or plan-shape)* — `… WHERE dim.k =
  fact.k AND dim.active` puts the residual in a `Filter` below the `Aggregate`, coexisting with the
  correlation key.
- **`uncorrelated_global_aggregate_scalar_unchanged`** *(regression)* — `… (SELECT MAX(dim.v) FROM
  dim)` keeps the empty `group_expr` + keyless `Cross` (the `correlation_pairs == []` path is
  byte-identical to 257).
- **`aggregate_arg_and_non_aggregate_correlated_scalar_stay_rejected`** *(reject)* —
  `(SELECT MAX(dim.v + fact.pk) FROM dim WHERE dim.k = fact.k)` rejects
  `"correlated scalar subqueries are not supported"` (`:9207`); a non-aggregate correlated scalar
  rejects `"non-aggregate scalar subqueries are not supported"`; a GROUP BY/HAVING scalar rejects
  `"scalar subqueries in GROUP BY/HAVING are not supported"`.

## Architecture guards

- `adr260_correlated_aggregate_scalar_decorrelates_via_grouped_left_outer` —
  `pull_up_scalar_subqueries` classifies the WHERE via `classify_exists_conjunct`, builds a grouped
  `Aggregate` keyed on the correlation, folds via `JoinKind::LeftOuter`; reuses existing nodes.
- `adr260_correlated_scalar_is_frontend_only` — no IR / executor / fragmenter / binder / join-kind
  surface.
- `adr260_aggregate_arg_and_non_aggregate_scalar_stay_deferred` — `"correlated scalar subqueries are
  not supported"` (aggregate-arg), `"non-aggregate scalar subqueries are not supported"`, and
  `"scalar subqueries in GROUP BY/HAVING are not supported"` all still present.

## Reconcile (lifting the WHERE-correlation reject)

The `"correlated scalar subqueries are not supported"` string **survives** (it still fires at `:9207`),
but its meaning narrows from "all correlated scalars" to "aggregate-argument-correlated scalars."
Reconcile each pin to that narrowed meaning:

- **`adr259_non_equi_and_scalar_correlation_stay_deferred`** (`subquery_predicates.rs:214`) — keep the
  string assertion, retarget its comment + the referenced behavioral-test pin (line 218) to the
  surviving aggregate-arg case.
- **`exists_decorrelation_test.rs:324`** (`non_equi_correlated_in_and_correlated_scalar_stay_rejected`)
  — its scalar case `MIN(dim.score) … WHERE dim.k = fact.k` **lowers now**; change it to
  `MIN(dim.score + fact.pk)` (aggregate-arg correlation) so it keeps rejecting the same string.
- **`group_by_test.rs:2688`** (`non_aggregate_and_correlated_scalar_subqueries_stay_rejected`) — its
  `AVG(amount) … WHERE sales.region = regions.region` case **lowers now**; drop it (keep the
  non-aggregate `(SELECT region FROM regions)` case) or convert to aggregate-arg correlation.
- A **non-equi WHERE correlation** inside a scalar now rejects via the classifier
  (`"unsupported EXISTS subquery predicate"`) instead of the scalar string — name this change.
- Grep `adr*` + the test corpus for `"correlated scalar"` pins and reconcile each (the 259 precedent).
  The string surviving is not evidence that all correlated scalar subqueries remain deferred.

## Deferred

Correlated **non-aggregate** scalar (≤1-row cardinality-assert mechanism); aggregate-argument
correlation; scalar in GROUP BY/HAVING / aggregate-query projection; non-equi correlation; cross-type
join-key coercion; multi-column scalar; DISTINCT inside the scalar aggregate; recursive-completion.

## Forward arc

260 correlated aggregate scalar → correlated **non-aggregate** scalar (build the ≤1-row
cardinality-assert mechanism) → HAVING/aggregate-query scalar placement (the gather/`terminal_exchange`
planner-rule review) → cross-type join/membership key coercion → recursive-completion → the standard
query surface is broadly complete. (Window `EXCLUDE` remains the lone window item, externally blocked
on a sqlparser bump.)
