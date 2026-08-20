# ADR 575 - Projected IN and quantified comparisons

**Status:** Proposed 2026-08-20. Closes the family ADR 0573 deferred and ADR
0568 bounded: subquery membership and quantified comparison in projection
position, where the third truth state is a value rather than a filter verdict.

## Context

ADR 0573 gave `EXISTS` a projection form and explicitly refused the rest,
directing `IN` and the quantified comparisons to "the mark join that carries
its third truth value". This record closes that family — and, having ground
the predicate paths, does **not** build a mark join. The reasoning for that is
the decision below, not an omission.

What already exists, and why none of it produces a value:

- **`IN` / `NOT IN` in a predicate** lower to joins. ADR 256 gave `IN` a
  `LeftSemi`; ADR 258 gave `NOT IN` a null-aware desugaring into two
  anti-joins (a null-guard and a membership arm); ADR 259 extended both to
  correlated forms. That desugaring is correct **because a `WHERE` cannot
  distinguish UNKNOWN from FALSE** — both discard the row. A projection can,
  so the desugaring cannot be reused there.
- **Ordering-quantified comparisons** already compute the three-valued answer.
  ADR 0568's `lower_quantified_ordering` emits `[keys…, COUNT(*), COUNT(v),
  MIN|MAX(v)]` from one aggregate and builds the explicit truth table as a
  `CASE`. That `CASE` is already a value-shaped `Expr` — it is simply pushed
  into the WHERE residual list today. Nothing about it is predicate-specific.
- **ADR 0573** established the shape this record follows: a projected form
  needs a value for every outer row, including rows a semi-join discards, and
  a decorrelated aggregate recombined with a `LeftOuter` supplies it.

The distinction 0573 drew — `EXISTS` is two-valued, so counting suffices —
inverts cleanly. Membership needs one more fact than existence: whether the
set contained a NULL. That is also a count.

## Decision

### 1. No mark join. The third state is counted, not marked

`x IN (S)` is fully determined by three facts, each already producible by the
ADR 260 decorrelation this engine has:

| condition | result |
| --- | --- |
| `x` is NULL | NULL |
| some `s ∈ S` equals `x` | TRUE |
| no match, and `S` contains a NULL | NULL |
| no match, and `S` has no NULL | FALSE |

Two decorrelated subplans supply them. A **membership** aggregate grouped on
the subquery's value recombines with a `LeftOuter` on `x = value`, so a
matched outer row carries a non-null key and an unmatched one does not — one
inner row per distinct value, so the outer cannot fan out. A **null-presence**
aggregate carries `COUNT(*)` and `COUNT(value)`; their inequality is exactly
"the set contained a NULL". The projected expression is the table above, in
the order above — the NULL probe first, because it dominates.

A mark-join operator would be a new `JoinKind`, a new executor kernel, and a
new physical shape, to compute what two aggregates and a `CASE` already
compute. ADR 0573 made the same call for the two-valued case and it held. A
physical mark join remains available later as an **optimization** with its own
record and its own evidence; it is not a prerequisite for the semantics, and
this record deliberately does not spend a join variant on it.

Consequence to fix in the same cut: ADR 0573's refusal directives name a mark
join as the awaited decision. They are retargeted to name this record.

### 2. Quantified projection reuses ADR 0568's table verbatim

`lower_quantified_ordering` already returns a three-valued `Expr`. The
projected form uses that same expression as a projected value instead of a
WHERE residual. No second truth table is written — a second one could drift
from the first, and the two positions must agree by construction rather than
by review.

Equality quantifiers keep ADR 0568's routing: `= ANY` is membership and
`<> ALL` is its negation, so they take §1's path rather than the ordering
path, in projection exactly as in a predicate.

### 3. The position is the existing type, not a new fork

ADR 0573 introduced `SubqueryPosition` because one traversal serves both the
WHERE and the projection list. This record adds arms to that traversal under
`Projection` only. Predicate-position `IN`, `NOT IN`, and quantified
comparisons keep their current lowerings byte-for-byte — the join desugarings
are better where a row filter is what is wanted, and nothing here re-plans
them.

### 4. `NOT IN` in projection is negation, not a second lowering

`x NOT IN (S)` is `NOT (x IN (S))` with NULL propagating, evaluated from the
same two subplans. It is not the predicate form's anti-join pair, which
cannot yield UNKNOWN as a value.

## Acceptance

- The four-row truth table of §1 is exercised as four distinct outer rows in
  one query: a NULL probe, a match, a no-match against a NULL-bearing set, and
  a no-match against a NULL-free set — reading NULL, TRUE, NULL, FALSE. A
  two-valued implementation fails on row three, and only on row three, which
  is what makes the control discriminating rather than decorative.
- `NOT IN` is the complement of `IN` wherever `IN` is not NULL, and is NULL
  exactly where `IN` is NULL — asserted against the same fixture rather than
  by a second literal list.
- A NULL-free set makes the third row FALSE: the same query against a set with
  the NULL removed flips exactly that row, proving the null-presence count is
  load-bearing and not incidentally satisfied.
- Projected `= ANY` agrees with projected `IN` on every row of one fixture,
  and projected `> ANY` / `> ALL` agree with ADR 0568's predicate verdict
  wherever that verdict is not UNKNOWN.
- Duplicate values in the subquery do not fan out the outer: row count is
  preserved, as the grouped membership aggregate guarantees.
- Predicate-position control: existing `WHERE x IN (…)`, `WHERE x NOT IN (…)`,
  and quantified predicates lower to the same plans as before, asserted
  structurally, so a position-blind regression fails here.
- The projected column is nullable, and its nullability is asserted — the
  inverse of ADR 0573's control, where a non-nullable result was the property
  that mattered.

## Guardrails

One truth table serves both positions: a guard pins that the quantified
projection path consumes `lower_quantified_ordering`'s expression rather than
constructing a `CASE` of its own, and that no second membership truth table
exists in the planner. A guard also pins that no new `JoinKind` variant
appears — this record's claim is that the family closes without one, and that
claim should fail loudly if a later cut quietly adds one under its name.

## Risks and deferrals

A physical mark join, as an optimization, is deferred with its own record; it
would need evidence that the two-aggregate form is the bottleneck before
spending a join variant. Correlated projected membership follows the predicate
form's admitted shapes (ADR 259) and no further; a correlation the EXISTS
splitter cannot classify refuses with the existing directive, as in ADR 0573.
`IN` over an explicit value list is unrelated to this record and unchanged.

## Forward

With this the three-valued projection family is closed, and `SubqueryPosition`
has arms for every subquery form the engine admits. The remaining subquery
work is positional rather than semantic — `ORDER BY`, `GROUP BY`, and `HAVING`
are the positions no record covers yet.
