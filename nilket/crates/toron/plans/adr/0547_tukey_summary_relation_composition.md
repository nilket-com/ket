# ADR 547 - Tukey summary relation composition

**Status:** Proposed and blocked on ADR 546, 2026-08-10.

ADR 546 makes grouped approximate quartiles a stable database capability. It
does not by itself produce a conventional box summary. Tukey whiskers require a
second pass to find the most extreme observed values inside the 1.5×IQR fences,
and outliers are zero-or-more observations per category. That is a multi-stage
relation with variable cardinality, not a fixed five-column aggregate row.

Stage 0 assembled the required shape with stable aggregate stand-ins. Each
intermediate CTE executes:

- grouped summary values;
- a join of those values back to observations;
- computed lower and upper fences;
- conditional `MIN`/`MAX` for in-fence extremes.

A final-recombination failure is **reported** but not yet reproduced. The report
is that after nested CTEs and grouped summaries, a join projecting the original
observation returns an unknown qualified column even though that column is
visible when the source CTE or the immediately preceding CTE is queried alone.

This is recorded as a reported failure, not as banked Stage 0 evidence. Eleven
reconstructed variants — including this record's canonical composition in §3,
with CTE aliasing, the diamond where `classified` feeds both `summaries` and
`outliers`, alias reuse across scopes, `CASE WHEN` fences, `AVG`, `LEFT JOIN`,
and `ORDER BY` on qualified join columns — all bind cleanly against the standard
fixture. Reproducing it requires the exact failing query, the table schema it
ran against, and the frontend used (SQL or Rune).

Until it reproduces, whether the cause is a planner/binder column-lineage defect
is undetermined, and no lineage repair can be written red-first.

This record pins one ordinary flat relation capable of carrying Tukey box
semantics. It adds no box-plot function, array aggregate, renderer, or
plot-specific server type. Whether it also closes a composition boundary depends
on whether the reported failure reproduces: if it does not, there is no boundary
to close and this record delivers the relation alone.

Grouped quantiles alone are explicitly **not** an unblocker for the Polariton
box surface. The complete database prerequisite is this record's fence, whisker
and outlier gates, plus its lineage gates **if the reported failure
reproduces**. If it does not reproduce, the fence, whisker and outlier gates are
the whole of it and no lineage gate is owed.

## Decision

### 1. The output is one flat statistical relation

For one non-null categorical key and one non-null numeric observation, the
canonical output schema is:

```text
category
whisker_low
q1
median
q3
whisker_high
outlier          nullable
```

There is one row per outlier observation. A category with no outliers still
has exactly one row with `outlier = NULL`. Duplicate outlier values remain
duplicate rows; the relation preserves observation multiplicity and does not
silently deduplicate points.

All six summary values repeat for each outlier row in the category. This is
intentional denormalization at the query boundary: Arrow and Rune already carry
flat columns, while an array-valued outlier cell would require a new nested type
and a second consumption protocol.

Rows with null category or null observation are excluded in the first CTE. A
category with no non-null observations therefore has no box-summary row. This
record does not invent a visual empty-category sentinel.

Column aliases are authored by the query. The names above are the acceptance
fixture and interop contract, not reserved Toron keywords.

### 2. Tukey semantics are computed from one quartile row per category

For each category:

```text
q1     = APPROX_QUANTILE(value, 0.25)
median = APPROX_QUANTILE(value, 0.50)
q3     = APPROX_QUANTILE(value, 0.75)
iqr    = q3 - q1
lower_fence = q1 - 1.5 * iqr
upper_fence = q3 + 1.5 * iqr

whisker_low  = MIN(observed value WHERE value >= lower_fence)
whisker_high = MAX(observed value WHERE value <= upper_fence)
outlier      = each observed value below lower_fence or above upper_fence
```

Whiskers are observed values, never the mathematical fence positions. Points
exactly on a fence are inside. The IQR multiplier is exactly 1.5. An IQR of
zero is not special-cased: the fences coincide, equal observations define the
whiskers, and unequal observations are outliers.

The quartiles inherit ADR 546's method, configuration, and rank-error bound.
That method is randomized: Wendelon ADR 83 selects the fully mergeable KLL
construction, whose guarantee requires randomness independent of the data, so a
fixed seed is not available and the earlier "fixed seed, deterministic fences"
framing is withdrawn.

The honest contract is that quartiles, and therefore the fences derived from
them, are **stable within one query execution** — the same values feed fence
computation, whisker selection, and outlier classification in a single request —
with variation between executions governed by the declared rank bound. A box
rendered twice may differ slightly; it is never internally inconsistent. Toron
does not label the fences exact hinges or imply a value-distance error bound.

The same q1/median/q3 values used to compute fences appear in the final rows.
They are not recomputed independently in later CTEs, because separate compacted
sketches could choose different retained observations and make the displayed
box disagree with its outlier classification.

### 3. The canonical query is ordinary SQL composition

The accepted shape is four logical stages:

```sql
WITH samples AS (
  SELECT region AS category, revenue AS value
  FROM measurements
  WHERE region IS NOT NULL AND revenue IS NOT NULL
),
quartiles AS (
  SELECT
    category,
    APPROX_QUANTILE(value, 0.25) AS q1,
    APPROX_QUANTILE(value, 0.50) AS median,
    APPROX_QUANTILE(value, 0.75) AS q3
  FROM samples
  GROUP BY category
),
classified AS (
  SELECT
    s.category,
    s.value,
    q.q1,
    q.median,
    q.q3,
    q.q1 - 1.5 * (q.q3 - q.q1) AS lower_fence,
    q.q3 + 1.5 * (q.q3 - q.q1) AS upper_fence
  FROM samples s
  JOIN quartiles q ON s.category = q.category
),
summaries AS (
  SELECT
    category,
    MIN(CASE WHEN value >= lower_fence THEN value END) AS whisker_low,
    MIN(q1) AS q1,
    MIN(median) AS median,
    MIN(q3) AS q3,
    MAX(CASE WHEN value <= upper_fence THEN value END) AS whisker_high
  FROM classified
  GROUP BY category
),
outliers AS (
  SELECT category, value AS outlier
  FROM classified
  WHERE value < lower_fence OR value > upper_fence
)
SELECT
  s.category,
  s.whisker_low,
  s.q1,
  s.median,
  s.q3,
  s.whisker_high,
  o.outlier
FROM summaries s
LEFT JOIN outliers o ON s.category = o.category
ORDER BY s.category, o.outlier
```

`MIN(q1)`/`MIN(median)`/`MIN(q3)` do not recompute quantiles; every classified
row for a category carries the one joined quartile row, so the stable aggregate
projects that value through the grouped summary. This avoids expanding the
simple-column `GROUP BY` surface merely to group on all three summary values.

Toron may optimize or internally materialize the CTEs, but accepted behavior is
the result above. The implementation does not recognize this spelling and
replace it with a box-specific plan.

### 4. Column identity must survive nested CTEs, aggregation, and recombination

**This section is conditional on reproducing the reported failure, and is
removed with it if the artifact stays unavailable.**

The requirement it states is unconditional and already appears to hold: a column
selected by a CTE and visible in that CTE's schema remains bindable under its
current table alias after the CTE participates in a later join and after sibling
CTEs contain aggregates. What is conditional is that a *repair* is needed at
all. If the report reproduces, the fix belongs to the normal projection/scope
lineage; until then no defect in that lineage has been demonstrated.

A stand-in fixture substituting stable `MIN`/`AVG`/`MAX` for q1, median and q3
over the same join/conditional-summary/final-left-join shape would separate this
repair from ADR 546's new aggregate state and give it an oracle already
expressible today.

**This step is conditional and is not currently actionable.** The stand-in shape
binds cleanly, so there is no red to land first. It becomes a real step only
when the reported failure reproduces from the exact production query, schema and
frontend. If that artifact stays unavailable, the step is removed rather than
satisfied by a query that never failed, and §4 carries no repair.

The fix must not flatten table qualifiers into ambiguous bare names, expose a
column pruned from the projected schema, or widen wildcard projection. The
qualified name resolves because the relation really projects it.

### 5. One statement observes one database snapshot

The summary query is one SQL request. Every CTE and repeated reference observes
the same request snapshot under Toron's existing read semantics. The client
does not issue a quartile query, interpolate values into a second SQL string,
then issue an outlier query.

This gives fence classification and returned quartiles one observation moment.
It does not promise one physical table scan, CTE caching, or a performance
bound. Those are optimizer questions after correctness.

### 6. The relation is generic; interpretation belongs to its consumer

Toron adds no `BOX_PLOT`, `TUKEY`, `WHISKERS`, or `OUTLIERS` syntax. It already
has the scalar arithmetic, `CASE`, computed aggregate arguments, CTEs, joins,
and conditional `MIN`/`MAX` needed to state the query. This record makes that
composition executable and proves the result.

Polariton later owns mark arguments, orientation, category order, hover,
outlier geometry, export, and the authored whisker policy. The database output
is useful independently for anomaly reports and percentile summaries.

## Sequence

1. Obtain the exact production artifact for the reported final-recombination
   failure — query text, table schema, and frontend — and reproduce it. Eleven
   reconstructed variants bind cleanly, so no stand-in reproduces it today. **If
   it cannot be reproduced, remove steps 1 and 2 and §4 from this record** and
   proceed from step 3; do not substitute a query that never failed.
2. Only once reproduced: repair projection/scope lineage and land that query
   green without any quantile or plotting change.
3. After ADR 546 lands, run the full quantile query at q=0.25/0.50/0.75 and
   prove Tukey semantics end to end. If steps 1 and 2 were removed for want of a
   reproduction, this is the first executable step and there are no stand-ins to
   replace.
4. Add local/distributed and one-snapshot correspondence gates.
5. Run the complete database battery and publish an implementation receipt that
   repeats that grouped quantiles alone did not unblock the consumer.

## Acceptance

1. **Lineage repair, conditional on reproduction.** Once the reported failure in
   §Context is reproduced from the exact query, schema, and frontend, that query
   moves from the qualified-column binding error to exact expected rows. Each
   source and intermediate CTE has a passing direct projection control, so the
   test cannot be repaired by deleting the missing observation from the final
   output. If the report does not reproduce, this acceptance item is withdrawn
   rather than satisfied by a substitute query that never failed.
2. **Exact output schema.** The full query returns the seven columns in §1 with
   stable numeric types and a nullable outlier. No nested/list type or second
   result set is introduced.
3. **Whiskers versus fences.** A fixture places observations between each
   mathematical fence and the nearest in-fence datum. Returned whiskers equal
   those observed data values, not the fence coordinates and not raw min/max.
4. **Outlier multiplicity.** The fixture contains different low/high outliers
   and two identical outlier observations. The output contains every occurrence
   exactly once. A category with no outliers still emits one row with null
   outlier.
5. **Fence inclusivity and zero IQR.** Values exactly on both fences remain
   in-fence. A zero-IQR category produces coincident quartiles/whiskers and
   classifies unequal observations as outliers without division or empty-state
   failure.
6. **Canonical quartile correspondence.** Within each category, every output
   row repeats byte-identical q1/median/q3 values; recomputing a quantile in a
   downstream CTE or producing fence values from a different sketch is
   structurally absent.
7. **Null policy.** Null observations and null categories are excluded before
   aggregation. An all-null category is absent, not rendered as zeros or a
   fabricated row. A non-null sibling proves the query executed.
8. **Duplicate ordinary observations.** Repeated in-fence source values affect
   the quantile population and preserve outlier multiplicity rules without
   creating extra summary-only rows. No hidden `DISTINCT` appears.
9. **Single-request evidence.** A request-level integration gate proves all
   stages execute as one statement against one snapshot. No client loop or
   interpolated follow-up query is introduced.
10. **Local/distributed correspondence.** A value-discriminating multi-category
    fixture returns identical ordered rows locally and with at least two
    workers. ADR 531's global and grouped aggregate correctness posture remains
    intact.
11. **Genericity.** Production source contains no box-specific logical plan,
    aggregate function, schema type, or Polariton dependency. The same query
    shape works with neutral aliases (`key`, `value`, `low`, `high`).
12. **Complete prerequisite claim.** The receipt names grouped quartiles,
    fences, observed whiskers, variable outlier rows, and the final join as the
    delivered unit. It explicitly states that ADR 546 alone was not an
    unblocker.
13. **Full battery.** The final tree passes the complete Toron battery and
    current workspace checks, with no ignored or tolerated failures.

## Guardrails and stop conditions

- This record is blocked until ADR 546's stable grouped quantiles land, which in
  turn waits on Wendelon ADR 83. The lineage repair may be implemented first
  **only if the reported failure is first reproduced from the exact production
  artifact**; absent that, there is nothing to repair and the step is removed.
  The record cannot close on stand-ins either way.
- If the final relation requires a new logical-plan variant, nested Arrow type,
  multi-result protocol, or box-specific executor, halt and redraft that larger
  capability rather than hiding it as lineage repair.
- If separate uses of a CTE can observe different snapshots in one request,
  halt. Do not move the quartiles into client code as a workaround.
- If distributed planning splits one category's quantile or conditional
  summary population across unreconciled terminal states, halt at the database
  boundary rather than accepting locally correct Tukey rows.
- Exact quartiles, alternative multipliers, weighted samples, notched boxes,
  adjusted boxplots, and array-valued outliers are future contracts.

## Rejected

- **Ship grouped quantiles alone.** They do not identify observed in-fence
  whiskers or return any outlier observations.
- **Min/max whiskers with no outliers as the first box contract.** Although
  representable, a standalone exported box would conventionally be read as
  Tukey 1.5×IQR. Silent range semantics borrow that convention's credibility
  while meaning something else.
- **Method furniture in every artifact.** A temporary “range/no outliers” label
  is permanent visual tax followed by another pixel change when conventional
  semantics arrive. Waiting is the honest, cheaper path.
- **An opaque `BOX_SUMMARY` aggregate.** Zero-or-more outliers do not fit one
  scalar aggregate cell, and the ordinary relational composition is independently
  useful and testable.
- **Client-side multi-query assembly.** It creates a snapshot race and moves
  statistical truth out of the database.
- **Drop duplicate outliers or pack them into a list.** Both change the
  observation set; the latter also invents a nested interchange surface.

## Consequences

After ADRs 546 and 547, Toron can produce a conventional Tukey summary relation
with approximate, rank-bounded quartiles, observed in-fence whiskers, and every
outlier observation. That is the complete database handoff for a later
Polariton box-mark record.

Histogram does not wait on this chain; ADR 545 is its independent prerequisite.
Hexbin remains later because its current data-space radius does not define
regular screen geometry under unequal axis scales.
