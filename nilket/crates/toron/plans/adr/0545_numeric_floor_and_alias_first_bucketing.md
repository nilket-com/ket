# ADR 545 - Numeric `FLOOR` and alias-first bucketing

**Status:** Accepted and implemented 2026-08-11.

Polariton's next plotting candidates exposed a database boundary rather than a
missing renderer. A histogram is a bar plot over a relation of interval starts,
interval ends, and counts. The renderer must not invent that relation by
silently aggregating raw rows.

Stage 0 established a narrower prerequisite than expected. Toron already
executes grouping over a projected alias through a CTE or derived table:

```sql
WITH binned AS (
  SELECT revenue / 10 AS bin FROM default.demo_percent_bars
)
SELECT bin, COUNT(*) AS n
FROM binned
GROUP BY bin
ORDER BY bin
```

The equivalent direct expression remains deliberately refused:

```text
GROUP BY keys must be simple column references
```

What is missing is the rounding primitive that makes numeric intervals stable.
`sqlparser` parses `FLOOR(value)` as `SqlExpr::Floor`; Toron's expression
lowering has no corresponding logical or bound expression, so the live request
currently refuses as an unsupported expression. This record adds that one
scalar and blesses alias-first bucketing as the composition. It does not widen
`GROUP BY` and does not introduce a histogram feature.

## Decision

### 1. `FLOOR(expr)` is a stable scalar expression

Toron lowers `SqlExpr::Floor { expr, field: None }` into one new logical
`Expr::Floor` arm and one new bound-expression arm. It is accepted anywhere the
ordinary scalar-expression surface is accepted: projection, filter, computed
aggregate argument, ordering, CTE projection, and derived-table projection.

The evaluator implements mathematical floor, toward negative infinity:

```text
FLOOR( 2.9) =  2
FLOOR( 2.0) =  2
FLOOR(-2.1) = -3
FLOOR(-2.0) = -2
```

Null remains null. This cut's physical type matrix is deliberately closed:

| input | output |
|---|---|
| `Int32` | identical `Int32` value |
| `Int64` | identical `Int64` value |
| `Float32` | floored `Float32` value |
| `Float64` | floored `Float64` value |

Decimal128 and named logical types refuse at bind time. This record does not
silently strip a logical type or choose a decimal precision/scale rule. A
`FLOOR` AST carrying a datetime field also refuses; date truncation retains its
existing, separately named surface.

Non-finite floating-point values are refused by the expression evaluator with a
typed kernel error. They have no ordered numeric-bin meaning, and admitting
NaN as a grouping key would make equality, hashing, and ordering policy part of
this record by accident. The refusal names the offending `FLOOR` expression.

### 2. Bucketing is alias-first relation composition

The canonical equal-width numeric-bucket shape is:

```sql
WITH bucketed AS (
  SELECT
    FLOOR(value / 10.0) * 10.0 AS bin_start
  FROM measurements
  WHERE value IS NOT NULL
)
SELECT
  bin_start,
  bin_start + 10.0 AS bin_end,
  COUNT(*) AS bin_count
FROM bucketed
GROUP BY bin_start
ORDER BY bin_start
```

The projected alias is the group key. Toron already executes that shape; this
record pins it rather than expanding the SQL frontend to accept the direct
`GROUP BY FLOOR(value / 10.0)` spelling.

The intervals are half-open `[bin_start, bin_end)`. An exact boundary value
belongs to the interval beginning at that value. Negative values follow the
same rule because `FLOOR` rounds toward negative infinity: at width 10, `-0.1`
belongs to `[-10, 0)`, not `[0, 10)`.

Width validation remains authored SQL. A caller must supply a positive,
nonzero width; division by zero and other arithmetic errors retain their
existing typed behavior. Toron does not infer a width, a range, or a desired bin
count.

This composition is intentionally asymmetric with the existing user-facing
`TIME_BUCKET(interval, timestamp)` surface. `TIME_BUCKET` accepts a duration
string literal and an `Int64` timestamp expressed in milliseconds; those are
temporal semantics, not a generic numeric-width contract, and `TIME_BUCKET`
itself remains behind the experimental-kernel gate. Stabilizing `FLOOR` first is
therefore deliberate ordering, not an assertion that the two bucketing surfaces
have equivalent maturity. Numeric bucket width is an ordinary numeric
expression, and the arithmetic spelling keeps both the width and the bucket
origin explicit without introducing a second bucketing surface with its own
coercion, endpoint, and overflow rules.

### 3. Empty bins are absent rows

The query emits only occupied intervals. Filling holes between the minimum and
maximum interval is a relation-generation capability that does not exist in
Toron today. A consumer that needs explicit zero-count bins must provide an
interval relation and left join it, once Toron has a suitable series-generation
surface.

This is important for the contract: `FLOOR` plus `GROUP BY` is a sparse
histogram relation, not a promise of a continuous interval grid.

### 4. Direct computed `GROUP BY` stays refused

`LogicalPlan::Aggregate` and the bound executor already carry expression
programs for group keys. The remaining restriction is nevertheless not a
one-line frontend lift. Projection naming, aggregate output matching, and
expression identity currently assume simple group-column names. Removing only
`lower_group_by_key`'s refusal would expose an incompletely specified output
surface.

Accordingly, this exact refusal remains banked:

```text
GROUP BY keys must be simple column references
```

CTE and derived-table aliases are the supported escape. Direct computed keys
may receive their own record when their naming and structural identity are
decided independently of histogram work.

### 5. One expression producer serves every evaluator

Logical lowering, binding, unbound/local evaluation, and bound/distributed
evaluation receive exhaustive `Floor` arms. The arithmetic lives in one shared
typed helper; a second implementation in a planner constant-folder or one
execution path is rejected.

No PBC pushdown, storage predicate, optimizer fact, or secondary-index encoding
is added. `FLOOR` executes above the scan like existing scalar arithmetic.

### 6. This is database surface, not plot surface

The result is independently useful for numeric bucketing, cohorting, pricing
bands, and derived grouping. No `HISTOGRAM`, `BIN`, `WIDTH_BUCKET`, plot type,
renderer dependency, or Polariton file enters Toron.

The Polariton histogram record remains responsible for continuous interval
geometry, axis semantics, hover, and how absent intervals are shown. This
record supplies only the relation it can consume.

## Sequence

1. Bank the live failures for `FLOOR` and direct computed `GROUP BY`, and bank
   the already-green alias-first CTE control.
2. Add the logical and bound expression vocabulary plus exhaustive structural
   visitors.
3. Add the closed type matrix and shared evaluator.
4. Exercise projection, filtering, ordering, CTE, local, and distributed paths.
5. Run the complete Toron and workspace batteries without changing the direct
   computed-group refusal.

## Acceptance

1. **Live histogram relation.** A fixture containing negative values, exact
   boundaries, positive values, duplicates, and nulls produces exact
   `[bin_start, bin_end, bin_count]` rows through the canonical alias-first CTE.
   At least one negative value discriminates floor-toward-negative-infinity
   from truncation toward zero.
2. **Scalar matrix.** Int32, Int64, Float32, and Float64 inputs return the exact
   types and values specified in §1; nulls stay null. Decimal128 and a named
   logical numeric value refuse at bind time rather than being coerced.
3. **Non-finite refusal.** Float32 and Float64 NaN and infinities reach a real
   evaluation path and refuse with the typed `FLOOR` diagnostic. A finite
   sibling proves the fixture did not fail before evaluation.
4. **Alias route parity.** Equivalent CTE and inline-derived-table queries
   return identical schemas and rows. This proves the blessed composition is
   not peculiar to one wrapper.
5. **Direct-group negative control.** `GROUP BY FLOOR(value / width)` and
   `GROUP BY value / width` retain the exact simple-column refusal. The
   alias-first sibling passes in the same test.
6. **Expression reach.** Projection, filter, ordering, and computed aggregate
   argument each execute `FLOOR`; no path reconstructs it as a string or an AST
   function call after lowering.
7. **Local/distributed correspondence.** A value-discriminating fixture returns
   identical rows under local execution and the distributed posture. Existing
   group exchange remains keyed on the projected alias.
8. **Structural closure.** Every exhaustive expression visitor, column
   collector, ordinal rewriter, structural hash/equality visitor, and display
   path is updated by compiler force or an architecture gate. No wildcard arm
   silently drops `Floor`.
9. **Sparse semantics.** A fixture with an unoccupied middle interval emits no
   row for it. No implicit series generation or zero filling appears.
10. **No adjacent surface.** Source guards and dependency inspection find no
    `CEIL`, `ROUND`, `WIDTH_BUCKET`, histogram mark, general computed-group
    acceptance, or Polariton dependency introduced by this record.
11. **Full battery.** The final tree passes the complete Toron battery and the
    workspace checks required by the current engine cut, with no ignored or
    tolerated failures.

## Guardrails and stop conditions

- If `sqlparser`'s `SqlExpr::Floor` cannot be lowered without introducing a
  general scalar-function registry or accepting its datetime-field variant,
  halt and record that larger surface rather than hiding it here.
- If alias-first bucketing loses the projected name, type, or value before the
  aggregate, halt. Do not lift the direct computed-group refusal as a repair.
- If one evaluator cannot share the same typed implementation, halt rather than
  ship local/distributed `FLOOR` semantics that can drift.
- Decimal, logical-type, non-finite, gap-filling, and automatic-width requests
  reopen their named boundaries; they are not opportunistic additions.

## Rejected

- **A `histogram(...)` aggregate or SQL function.** It couples a plotting
  product to the database and duplicates ordinary relation composition.
- **Mark-side binning.** It makes the denominator and interval membership
  renderer-private and cannot compose with filtering, facets, export, or other
  relation consumers.
- **Direct `GROUP BY` expressions in this record.** The underlying IR can carry
  them, but the frontend's output naming and expression-identity contract is
  undecided. The already-working alias route is sufficient.
- **`WIDTH_BUCKET` instead of `FLOOR`.** It would also decide range endpoints,
  overflow buckets, bin count, and indexing convention. Those are materially
  larger semantics than the reusable primitive required here.
- **Overload `TIME_BUCKET` or add `NUMERIC_BUCKET`.** The existing
  `TIME_BUCKET` surface is typed around a duration literal and timestamp
  milliseconds. Widening it would conflate temporal and dimensionless numeric
  semantics; adding a parallel named operator would require new origin,
  coercion, endpoint, and overflow policy for arithmetic already expressible as
  `FLOOR(value / width) * width`.
- **Silent decimal or logical-type coercion.** A bucket boundary must retain a
  meaning the type system can state; stripping it for convenience is not one.

## Consequences

Toron gains one ordinary scalar and a documented way to build sparse numeric
interval counts. Histogram work becomes a Polariton geometry record over a
normal relation rather than an exception to the relation/mark boundary.

This record does not unblock box plots. It also does not make hexagons
viewport-aware; hexbin remains a separate planning problem.
