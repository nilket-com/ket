# ADR 257 — Scalar Subqueries (provably-scalar / aggregate form)

## Status

Proposed.

## Context

After the predicate surface (256), a scalar subquery — a subquery used as a *value* — is the next gap:
`SELECT region, (SELECT AVG(amount) FROM sales) FROM regions`,
`… WHERE amount > (SELECT AVG(amount) FROM sales)`. Today scalar `SqlExpr::Subquery` rejects.

A scalar subquery that is a **global aggregate** is *provably ≤1 row* (`row_upper_bound <= 1`,
`binder/join.rs:607`), the most common scalar form. The natural lowering is to recombine the subplan
onto the outer with a `Cross` and reference its single column — reusing the ADR 239/240 scalar-`Cross`
substrate.

## Grounding (iter-cycle probe — the premise shifted)

The naive "frontend-only pull-up into a `Cross`" premise is **falsified**. Probe: `regions × (SELECT
AVG(amount) FROM sales)` built directly as a `Cross` **rejects at bind**:

> *"CROSS JOIN requires a broadcast-eligible build side under the broadcast cap; no eligible side fits and
> a keyless hash-partition would collapse the cartesian to a single bucket"*

`choose_cross_join_strategy` (`binder/join.rs:548`) handles **both**-scalar (ADR 239 `ScalarLocalCross`),
but a scalar *subquery* is **outer-many × scalar-one**: the aggregate side is composite, so the Auto path
finds no broadcast-eligible (scan-linear/base-relation) side and errors. So 257 is **not frontend-only** —
it needs a targeted binder extension.

## Decision

Scalar subqueries (provably-scalar / global-aggregate form) = a **binder extension** + a **frontend
pull-up**.

### Binder — a provably-scalar branch is broadcast-eligible

Extend `choose_cross_join_strategy`: when exactly **one** side is provably scalar (`row_upper_bound <= 1`),
broadcast that side. A 1-row branch broadcast to N outer partitions yields N×1 = N rows — no partition
multiplication, the same "cannot multiply partitions → broadcast-exempt" reasoning the both-scalar
`ScalarLocalCross` already relies on. So a scalar (1-row) side is broadcast-eligible **regardless of being
composite** (the scan-linear/base-relation requirement exists to bound broadcast *size*; a 1-row branch is
trivially bounded). This makes `outer × scalar` bind by broadcasting the scalar.

### Frontend — pull the scalar subquery up into a `Cross`

In SELECT-list projections and WHERE comparison operands, a scalar `SqlExpr::Subquery` that is an
uncorrelated global aggregate lowers by: lower the subquery to its (row=1) aggregate subplan; `Cross`-join
it onto the outer plan **with the `Auto` strategy intent** (so the binder's `ScalarBroadcastCross`
physicalizes the one-row broadcast); replace the subquery expression with a reference to the subplan's
single output column. Multiple scalar subqueries → multiple stacked scalar `Cross` joins. The `Cross` is
injected **below** the WHERE filter / aggregate (pre-aggregation), so the scalar value is in scope for the
WHERE predicate and the projection.

### Surface

Binder (`choose_cross_join_strategy`) + frontend (`lower_select` pull-up) only. The recombination reuses
the existing `Cross` / broadcast execution path — **no new IR node and no new execution op**; the binder
change picks the `Broadcast` strategy for a scalar side, which the executor already runs.

## Scope

- **In**: an **uncorrelated, provably-scalar (global-aggregate)** subquery used as a value in a SELECT-list
  projection and in WHERE comparison operands; recombined via a one-row broadcast scalar `Cross`; its
  single column referenced in place of the expression. The binder broadcast-eligibility extension for
  one-side-scalar `Cross`.
- **Out / deferred (mechanism boundaries)**: **HAVING / aggregate-query scalar subqueries** — a grouped
  aggregate sits behind a terminal (group-keyed) exchange, so a scalar in HAVING (or in an aggregate /
  grouped projection) must recombine *above* the aggregate; a broadcast `Cross` needs an exchange-free
  probe, so this is a separate pre-aggregation-value recombination cut. **Non-aggregate scalar
  subqueries** (`(SELECT x FROM t WHERE …)` — needs a runtime "at most one row, else error" cardinality
  assert). **Correlated** scalar subqueries (decorrelation). Scalar subqueries in ON / GROUP BY / ORDER BY
  positions; `NOT IN`; recursive-completion. *(A scalar in WHERE of an aggregate query still works — it
  crosses pre-aggregation and is consumed by the filter.)*

## Discriminators (each fails on `main`)

- **`scalar_aggregate_subquery_in_projection_attaches_value`** — `SELECT region, (SELECT AVG(amount) FROM
  sales) FROM regions` attaches the aggregate to every region row.
- **`scalar_aggregate_subquery_in_where_filters`** — `… WHERE id > (SELECT COUNT(*) FROM regions)`
  (Int-vs-Int; cross-type Int-vs-Float comparison across the scalar boundary is not coerced — a follow-on).
- **`scalar_aggregate_subquery_in_having_filters`** — `… GROUP BY … HAVING <agg> > (SELECT AVG(amount)
  FROM sales)` **stays rejected** (the deferred HAVING / aggregate-query boundary).
- **`one_side_scalar_cross_broadcasts_the_scalar`** — the binder extension: `outer × (global aggregate)`
  `Cross` binds by broadcasting the scalar side (the case that rejects on `main`).
- **`non_aggregate_and_correlated_scalar_subqueries_stay_rejected`** — the deferred boundaries.

## Architecture guards

- `adr257_one_side_scalar_cross_is_broadcast_eligible` — `choose_cross_join_strategy` broadcasts a
  provably-scalar (`row_upper_bound <= 1`) side; the both-scalar `ScalarLocalCross` path is unchanged.
- `adr257_scalar_subquery_pulls_up_into_cross` — a scalar `SqlExpr::Subquery` (aggregate) lowers to a
  `Cross` + a reference to the subplan column; the scalar-subquery reject is lifted for the provably-scalar
  case.
- `adr257_non_aggregate_and_correlated_scalar_stay_deferred` — the HAVING / aggregate-query,
  non-aggregate, and correlated scalar-subquery rejects remain; no new execution op (reuses the
  `Cross`/broadcast executor).

## Deferred

**HAVING / aggregate-query scalar subqueries** (recombine above the aggregate — a separate
pre-aggregation-value cut); non-aggregate scalar subqueries (cardinality assert); correlated scalar
subqueries; scalar subqueries in ON/GROUP BY/ORDER BY; cross-type (Int-vs-Float) scalar comparison
coercion; `NOT IN` (null-aware); recursive-completion.

## Forward arc

257 scalar subqueries (aggregate) → 258 non-aggregate / correlated scalar + `NOT IN` (null-aware) →
correlated `IN` → recursive-completion → the standard query surface is broadly complete. (Window
`EXCLUDE` remains the lone window item, externally blocked on a sqlparser bump.)
