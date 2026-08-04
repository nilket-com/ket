# Toron ADR 231: SELECT DISTINCT

**Status:** Promoted. **Migration:** `plans/impl/0231_select_distinct.md`.
**Category:** Toron core query surface - add top-level `SELECT DISTINCT` and a matching Rune
`distinct()` authoring handle through a bind-eliminated logical marker. **No new execution
operator, Wendelon pushdown, optimizer evidence/readout/receipt surface, durable key, daemon
flush, or system table.**

## Why this cut exists

ADR 230 intentionally closed the optimizer-instrumentation arc at a receipt-gated stopping point
and named DISTINCT as the next core-query direction. `SELECT DISTINCT` is a common SQL surface
that Toron still rejects, while the executor already has the essential primitive: grouped
aggregation keyed by `GroupValue`. With zero aggregate calls, grouped aggregation emits one row
per unique key tuple. This cut admits DISTINCT narrowly and proves the semantics without opening
set operations, windowing, subquery DISTINCT, or join DISTINCT in the same move.

## Grounding

- `SqlPlanner::lower_select` rejects `distinct.is_some()` before FROM lowering
  (`sql_planner.rs:3350`).
- User-authored grouped SQL rejects no-aggregate grouping with
  `"GROUP BY requires at least one aggregate call"` (`sql_planner.rs:3517`).
- The ordinary SELECT wildcard path does **not** expand `*` in `sql_planner.rs`; a single
  wildcard projection returns the source plan unchanged (`sql_planner.rs:3593-3597`). Therefore
  `SELECT DISTINCT *` needs a binder-altitude expansion point.
- Rune `RuneDataFrame` is lazy and schema-free: it carries a `LogicalPlan`, not a bound schema
  (`rune_engine.rs:191`). A no-argument `distinct()` cannot enumerate current columns before
  binding.
- The binder already sees the input output schema when lowering logical plans and already lowers
  `LogicalPlan::Aggregate` to `ExecutionPlan::Aggregate` (`binder.rs:1134-1164`).
- Aggregate output schema is group keys first, aggregate calls after (`binder/aggregate.rs:235`).
- The executor already dispatches non-empty `group_expr` to `bound_grouped_aggregate`
  (`exec.rs:759`), whose `BTreeMap<Vec<GroupValue>, ...>` key naturally dedups tuples, including
  `GroupValue::Null`.
- Derived tables and EXISTS independently reject DISTINCT via `validate_derived_select_shape` and
  `validate_exists_select_shape` (`sql_planner.rs:7074`, `7113`).

## Decisions

**D1 - Add a logical-only DISTINCT marker.** Add
`LogicalPlan::Distinct { input: Box<LogicalPlan> }`. SQL `SELECT DISTINCT ...` and Rune
`distinct()` both build this marker. The binder must eliminate it by lowering to the existing
aggregate path; `Distinct` must never reach `ExecutionPlan`, the fragmenter, executor, storage,
or wire.

**D2 - Empty aggregate remains the dedup substrate.** Binder lowering maps the marker to
`LogicalPlan::Aggregate` semantics with `group_expr = all input output columns` and
`aggr_expr = []`, then binds that to the existing `ExecutionPlan::Aggregate`. The group keys are
derived from bound output ordinals, not resolved again by output name, so duplicate projection
names remain correct. No new physical operator or Wendelon pushdown.

**D3 - Frontend-neutral first cut.** SQL and Rune share the same marker and therefore the same
binder lowering. The cardinal parity proof is that
`SELECT DISTINCT a, b FROM t` and
`ctx.table("t").project([col("a"), col("b")]).distinct()` produce the same bound plan shape and
rows.

**D4 - Public GROUP BY is not relaxed.** `SELECT a FROM t GROUP BY a` continues to reject without
an aggregate call. DISTINCT gets a separate marker path; it does not remove or bypass the public
GROUP BY guard for user-authored GROUP BY.

**D5 - Projection shape is the DISTINCT input shape.** SQL applies projection first, then wraps
that projection in `Distinct`. That makes aliases, computed expression names, duplicate output
names, and `SELECT DISTINCT *` follow the same visible output columns as the non-DISTINCT query.
Rune `distinct()` dedups the current frame, so callers compose `project(...).distinct()` when
they want SQL-like projection DISTINCT.

**D6 - Narrow fences are explicit.** Reject DISTINCT over joins, derived tables, EXISTS
subqueries, aggregate projections, GROUP BY/HAVING, DISTINCT ON, set operations, SELECT
modifiers, TOP, QUALIFY, and window functions with clear unsupported errors. DISTINCT over joins
and subqueries are named follow-ons.

**D7 - ORDER BY observes the post-DISTINCT projection.** ORDER BY after DISTINCT resolves against
the distinct output. Ordering by a non-projected source column is rejected rather than silently
using a hidden value.

**D8 - Nulls dedup as equal.** Multiple rows whose DISTINCT key tuple differs only by repeated
NULL values collapse to one output row, matching SQL result-set semantics and the existing
`GroupValue::Null` key behavior.

**D9 - Optimizer/evidence arc untouched.** No optimizer decision matrix, runtime evidence,
receipt-gate row, Rune optimizer readout handle, durable key, daemon flush, fragmenter, executor
policy, wire, or Wendelon surface changes.

## Semantics

Supported examples:

```sql
SELECT DISTINCT region FROM sales
SELECT DISTINCT region, product_id FROM sales WHERE amount > 0
SELECT DISTINCT region AS r FROM sales ORDER BY r LIMIT 10
SELECT DISTINCT amount + 1 AS adjusted FROM sales
SELECT DISTINCT * FROM sales
```

Rejected examples:

```sql
SELECT DISTINCT region FROM sales JOIN stores ON sales.store_id = stores.id
SELECT DISTINCT count(*) FROM sales
SELECT region FROM sales GROUP BY region
SELECT DISTINCT region FROM (SELECT region FROM sales) s
SELECT DISTINCT ON (region) region FROM sales
```

## Honesty Gates

- `select_distinct_single_and_multi_column_dedups_rows`.
- `select_distinct_nulls_collapse_to_one`.
- `select_distinct_star_groups_all_visible_columns`.
- `select_distinct_alias_and_expression_preserve_output_schema`.
- `select_distinct_duplicate_output_names_group_by_position`.
- `select_distinct_where_order_by_limit_compose_in_existing_tail_order`.
- `select_distinct_order_by_non_projected_column_rejects`.
- `rune_distinct_matches_sql_distinct_plan_and_rows`.
- `distinct_lowers_to_empty_aggregate_marker_no_new_execution_op`.
- `public_group_by_without_aggregate_still_rejects`.
- `distinct_over_join_is_explicit_unsupported`.
- `distinct_in_subquery_is_explicit_unsupported`.
- `distinct_with_aggregate_group_by_or_having_is_explicit_unsupported`.

## Architecture Guards

- `adr231_distinct_marker_lowers_to_empty_aggregate_in_binder`.
- `adr231_distinct_binder_group_keys_are_ordinal_safe`.
- `adr231_distinct_is_frontend_neutral_sql_and_rune_share_the_marker`.
- `adr231_distinct_adds_no_new_execution_plan_or_executor_op`.
- `adr231_distinct_first_cut_fences_joins_subqueries_and_aggregates`.
- `adr231_user_group_by_without_aggregate_still_rejected`.
- `adr231_distinct_leaves_optimizer_evidence_surfaces_unchanged`.

## Reconciled Prior Guards

- ADR 230 optimizer-rotation cockpit and dominance gate: unchanged.
- ADR 229 probe-linearity readout, ADR 228 Rune optimizer handles, ADR 217 receipt gates, and
  ADR 216 decision matrix: unchanged.
- Derived table and EXISTS shape guards retain their DISTINCT rejection.
- Public GROUP BY no-aggregate rejection remains, even though marker lowering internally creates
  an empty aggregate.

## Consensus

The council converged on SELECT DISTINCT as the next missing-feature cut after ADR 230. All drafts
agree on the empty-aggregate dedup substrate and on keeping the first cut Toron-local. The main
iteration fork was marker vs direct aggregate lowering. This draft adopts the marker because the
code confirms `SELECT *` is not expanded before binding and Rune has no schema, so a shared
binder-altitude lowering is the only design that keeps SQL `DISTINCT *` and Rune `distinct()`
frontend-neutral without adding a physical operator. It narrows Gemini's GROUP BY-relaxation
proposal: GROUP BY remains unchanged while DISTINCT gets its own marker path.

## Forward Arc

```text
231 SELECT DISTINCT
-> DISTINCT over joins
-> DISTINCT in derived tables / EXISTS where useful
-> set operations
-> window functions
```

Any future optimizer fence relaxation still requires ADR 230's dominance gate to be actionable
from production evidence.
