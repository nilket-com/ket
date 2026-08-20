# ADR 573 - EXISTS and NOT EXISTS in projection position

**Status:** Proposed 2026-08-20.

## Context

`EXISTS` works as a predicate and nowhere else. Grounded by probe, all three
projected forms refuse today — correlated, negated, and uncorrelated — and
they refuse badly: `lower_expr` has no `SqlExpr::Exists` arm, so they fall to
a catch-all that prints the raw sqlparser AST as the operator-facing message.
The predicate form is fine and stays fine: `SELECT … WHERE EXISTS (…)` lowers
through `lower_exists_join` to a semi-join with the ADR 279/323 correlation
machinery behind it.

The reason the predicate path cannot simply be reused is the shape of what
each position needs. A semi-join *filters rows*; it never produces a value.
A projection needs one value per outer row, including for the outer rows that
have no inner match at all — precisely the rows a semi-join discards.

The decorrelated grouped aggregate already produces exactly that: ADR 260's
correlated-scalar path builds `COUNT` keyed on the correlation keys and
recombines with a `LeftOuter`, and `count_empty_group_coalesce` already
converts the unmatched-group `NULL` into `0`. Projected `EXISTS` is that
value, compared to zero. No new join type, no new kernel.

The seam that needs care: `pull_up_scalar_subqueries` runs over the WHERE
selection as well as the projection list. `SqlExpr::Exists` currently falls
through its catch-all untouched, which is exactly why the predicate path
still reaches `lower_exists_join`. A position-blind rewrite here would
silently re-plan every existing `WHERE EXISTS` onto the aggregate path.

## Decision

### 1. The rewrite is position-aware, and the position is a type

`pull_up_scalar_subqueries` takes a `SubqueryPosition` (`Projection` or
`Predicate`). `SqlExpr::Exists` rewrites only under `Projection`; under
`Predicate` it passes through unchanged, so every existing `WHERE EXISTS`
plan is byte-identical. The distinction is carried by a type rather than a
call-site convention because both positions share one traversal.

### 2. Projected EXISTS lowers to a decorrelated COUNT compared to zero

Correlated: classify the inner WHERE with the existing
`classify_exists_conjunct` splitter, build `COUNT(*)` grouped on the inner
correlation keys, recombine with `LeftOuter` on the outer keys, and rewrite
the projection expression to `count_empty_group_coalesce(value) > 0`.
Uncorrelated: the keyless global `COUNT(*)` under ADR 257's `Cross`, rewritten
to `value > 0`. `NOT EXISTS` is the same plan with `= 0` — negation is a
comparison flip, never a separate lowering.

Admitted subquery shapes are exactly the predicate form's: the same
`validate_exists_query_shape` / `validate_exists_select_shape` /
`validate_exists_projection` validators run, so a body that is legal in a
`WHERE` is legal in a `SELECT` and vice versa. A correlated residual the
splitter cannot classify refuses with the existing directive.

### 3. The result is two-valued by construction, and that is the boundary

`EXISTS` is TRUE or FALSE — never NULL — for every input, including a NULL
correlation key, an empty inner relation, and an inner relation of all-NULL
rows. The `COALESCE`-to-zero step is what makes that structural here: an
outer row with no matching group reads `0`, which compares to `FALSE`, not
`NULL`.

This is exactly why the same rewrite does **not** extend to projected `IN`
or the quantified comparisons of ADR 568. Those are three-valued: `x IN
(SELECT …)` with no match and a NULL present is UNKNOWN, not FALSE, and a
`COUNT`-based rewrite cannot distinguish "no match" from "no match, but a
NULL was there". They need a mark-join that carries a third state, which is
its own decision with its own record — deferred, not attempted here. Their
current refusals gain a directive naming that deferral.

### 4. The refusal for an inexpressible expression names itself

The `lower_expr` catch-all stops printing parser AST. An unsupported
expression refuses with its SQL-level construct named; the projected `IN` and
quantified forms name the mark-join deferral.

## Acceptance

- A correlated projected `EXISTS` returns, per outer row, the same verdict the
  equivalent `WHERE EXISTS` keeps — proven by pairing the two against one
  fixture rather than by asserting a literal list twice.
- `NOT EXISTS` is the exact complement of `EXISTS` on the same fixture: no row
  is TRUE or FALSE in both, and none is NULL in either.
- The discriminating control: an outer row whose correlation key is NULL, and
  an outer row whose key matches nothing, both read FALSE — and the projected
  column is non-nullable. This fails under any three-valued treatment.
- An empty inner relation yields FALSE for every outer row, not NULL and not
  zero rows.
- An uncorrelated projected `EXISTS` is constant across outer rows and does not
  fan them out; row count equals the outer's.
- Composition: projected `EXISTS` inside `CASE`, under `NOT`, and combined with
  `AND`/`OR` alongside an ordinary column.
- Duplicate correlation keys in the inner relation do not fan out the outer:
  the grouped aggregate emits one row per key.
- Predicate-position control: existing `WHERE EXISTS` / `WHERE NOT EXISTS`
  plans are unchanged, asserted structurally against the semi-join shape, so a
  position-blind regression fails here.
- Projected `IN` and quantified forms still refuse, with a directive naming the
  mark-join deferral rather than an AST dump.

## Guardrails

The rewrite reuses `classify_exists_conjunct`,
`count_empty_group_coalesce`, and the `validate_exists_*` shape validators —
a guard pins that the projected path introduces no second EXISTS shape
validator and no new `JoinKind`. Admitted-shape parity between the two
positions is asserted, not assumed.

## Risks and deferrals

Projected `IN`, projected quantified comparisons, and any construct needing a
third truth state are deferred to a mark-join ADR. `EXISTS` in `GROUP BY`,
`ORDER BY`, and `HAVING` positions are out of this cut. The aggregate path
costs a decorrelated group-by where the predicate path can short-circuit on
first match; that cost is accepted for a value-producing position and is not
a reason to re-plan the predicate form.

## Forward

The mark-join record picks up projected `IN` and quantified forms with the
third truth state this cut deliberately does not need.
