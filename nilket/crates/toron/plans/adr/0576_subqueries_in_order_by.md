# ADR 576 - Subqueries in ORDER BY

**Status:** Proposed 2026-08-21. Takes the first of the three positions ADR
0573 and 0575 left open, and explains why the other two are not in this cut.

## Context

`SubqueryPosition` now has arms for the predicate and the projection, and the
three-valued family is closed. What remains is positional: `ORDER BY`,
`GROUP BY`, and `HAVING`. Grounded by probe, they turn out to be three
different problems, and only one of them is a subquery problem.

**ORDER BY already admits arbitrary expressions.** `ORDER BY amount + 1`,
`ORDER BY CASE WHEN … END`, and `ORDER BY <output alias>` all plan today. The
only thing that refuses is a subquery — `EXISTS`, `IN`, quantified, or scalar.
So the gap here is exactly one shape, in a position that already accepts
computed keys.

**GROUP BY admits no expressions at all.** `GROUP BY amount + 1` and
`GROUP BY CASE WHEN … END` refuse with "GROUP BY keys must be simple column
references" — the same message a subquery gets. Admitting a subquery there
means first admitting computed group keys, which changes what the aggregate
keys on, what the exchange partitions by, and how a key relates to its output
column. That is a grouping decision that happens to have a subquery as one
special case, not a subquery decision. It is **out of this family** and wants
its own record.

**HAVING is partially done, and its remaining gap is decorrelation, not
position.** `HAVING COUNT(*) > (SELECT COUNT(*) FROM …)` already plans through
`pull_up_having_scalar_subqueries`. What refuses is `EXISTS` / `IN` there, and
— tellingly — a *correlated* scalar subquery, with "correlated scalar
subqueries are not supported". Correlating a HAVING subquery means keying the
decorrelation on the GROUP BY keys after aggregation, which is a different
problem from anything ADR 260 solved and the reason HAVING is its own cut
rather than a second half of this one.

## Decision

### 1. ORDER BY takes the projection's shape, not the predicate's

A sort key needs a value per row and must not change cardinality, which is the
projection's contract. `EXISTS` keeps ADR 0573's two-valued counting form,
membership and quantified comparisons keep ADR 0575's three-valued tables, and
a scalar subquery keeps ADR 257/260/571's value form. No new semantics: this
record is a position, not a meaning.

Sorting on a three-valued key sorts on a nullable boolean, with NULL ordering
governed by the existing `NULLS FIRST` / `NULLS LAST` rules. That falls out of
the type; it is not a special case.

### 2. The subplan joins before the sort, and the columns it adds are hidden

`ORDER BY` lowers last, against a plan that has already been projected. A
pulled-up subplan therefore joins *after* the projection and adds columns the
SELECT list does not name, which must not reach the caller.

This shape already exists: `add_hidden_sort_projection` does exactly it for the
aggregate case — sort by something that is not in the output, then project it
away. This record reuses that machinery rather than adding a second way to
hide a sort column. Output schema is unchanged by the presence of a subquery
in `ORDER BY`, and that is asserted rather than assumed.

### 3. One evaluation per subquery, and the alias case says so

`SELECT x IN (…) AS m FROM t ORDER BY m` already works — the sort references
the projected output column, and nothing new is pulled up. `ORDER BY x IN (…)`
with no corresponding SELECT item pulls the subquery up once. What this record
does **not** do is common-subexpression the two: an identical subquery written
in both places is evaluated twice. That is a cost, it is stated, and it is not
a correctness question.

## Acceptance

- Each admitted form sorts correctly as an `ORDER BY` key: `EXISTS`, `IN`,
  a quantified comparison, and a scalar subquery — each asserted against the
  row order the same expression produces in a projection, so the position
  cannot disagree with the meaning.
- A three-valued sort key orders its NULLs per `NULLS FIRST` / `NULLS LAST`,
  asserted both ways, so the UNKNOWN rows are placed rather than dropped.
- Cardinality and schema are preserved: the output has the same rows and the
  same columns as the identical query without the `ORDER BY` subquery. A
  leaked hidden column fails here.
- `ORDER BY <alias>` over a projected subquery still resolves to the projected
  column and pulls nothing up — asserted structurally, since the alternative
  silently doubles the work.
- `GROUP BY` and `HAVING` subqueries still refuse, each naming its own reason
  rather than a generic position message: computed group keys for the former,
  the correlated-decorrelation gap for the latter.

## Guardrails

A guard pins that the sort position reuses `add_hidden_sort_projection` and
introduces no second hidden-column mechanism, and that the admitted forms are
the projection's — no `ORDER BY`-only truth table, value form, or refusal
text.

## Risks and deferrals

`GROUP BY` subqueries are deferred behind computed group keys, which is their
actual blocker and its own record. `HAVING` subqueries are deferred to a
record that must first decide how a correlated HAVING subquery decorrelates
against its group keys. Repeated evaluation of an identical subquery written in
both `SELECT` and `ORDER BY` is accepted here and left to a common-subexpression
decision if it ever earns one.

## Forward

With `ORDER BY` closed, the remaining subquery positions are the two whose
blockers are named above, and neither is blocked on subquery machinery.
