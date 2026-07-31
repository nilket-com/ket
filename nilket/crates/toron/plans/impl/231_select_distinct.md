# Impl Plan - Toron ADR 231: SELECT DISTINCT

Pairs with `plans/adr/231_select_distinct.md`. Add top-level SQL `SELECT DISTINCT` and a
matching Rune `distinct()` handle through a logical-only `LogicalPlan::Distinct` marker. The
binder eliminates the marker by lowering the bound input columns to the existing grouped
aggregate executor with `aggr_expr = []`. No new execution operator, optimizer evidence,
receipt gate, durable key, daemon flush, wire message, system table, or Wendelon surface.

## Touch Surface

- `src/ir.rs` - add `LogicalPlan::Distinct { input: Box<LogicalPlan> }` and recurse through it
  in logical-plan walkers.
- `src/sql_planner.rs` - replace the top-level blanket DISTINCT reject with a narrow classifier
  that builds the marker after ordinary projection lowering.
- `src/rune_engine.rs` - add no-argument `RuneDataFrame::distinct()` that wraps the current lazy
  frame in the same marker, then register it in the Toron Rune module.
- `src/binder.rs` and possibly `src/binder/aggregate.rs` - lower the marker from bound input
  schema to `ExecutionPlan::Aggregate` with ordinal-safe group keys and no aggregate calls.
- `src/binder/tests.rs`, `src/rune_engine.rs` tests, or nearby existing SQL execution tests -
  behavior and parity coverage.
- `tests/architecture/*` - ADR 231 source guards and any retargets from the old top-level
  DISTINCT rejection.

Expected no-change surfaces: `src/exec.rs`, `src/wire.rs`, `src/daemon.rs`,
`src/binder/runtime_evidence.rs`, `src/distributed`, `../wendelon/src`.

## Anchors

| Concern | Anchor |
|---|---|
| Current SQL reject | `SqlPlanner::lower_select`, `distinct.is_some()` (`sql_planner.rs:3350`) |
| Public GROUP BY no-aggregate reject | `"GROUP BY requires at least one aggregate call"` (`sql_planner.rs:3517`) |
| GROUP BY join fences | `"GROUP BY over joins is not supported"` (`sql_planner.rs:3420/3493`) |
| Ordinary wildcard behavior | single wildcard projection returns the source plan unchanged (`sql_planner.rs:3593-3597`) |
| Derived/EXISTS DISTINCT fences | `validate_derived_select_shape` / `validate_exists_select_shape` (`sql_planner.rs:7074/7113`) |
| Logical plan enum | `LogicalPlan` and existing `Aggregate` variant (`ir.rs`) |
| Binder aggregate lowering | `LogicalPlan::Aggregate` to `ExecutionPlan::Aggregate` (`binder.rs:1134-1164`) |
| Aggregate schema construction | group keys first, aggregate calls after (`binder/aggregate.rs:235`) |
| Executor aggregate dispatch | `ExecutionPlan::Aggregate` branch (`exec.rs:759`) |
| Rune lazy frame | `RuneDataFrame { plan: LogicalPlan }` (`rune_engine.rs:191`) |
| Rune transforms and module registration | `impl RuneDataFrame` / `create_toron_module` (`rune_engine.rs:230`, `1274+`) |

## Phase 0 - Red Tests

Add the behavior gates first:

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

The first group fails on `main` because top-level SQL DISTINCT is rejected and Rune has no
distinct authoring handle. The fence tests pin the boundary before any planner widening.

## Phase 1 - Logical Marker

Add:

```rust
LogicalPlan::Distinct {
    input: Box<LogicalPlan>,
}
```

Keep it logical-only:

- Update all exhaustive `LogicalPlan` matches and walkers to recurse into `input` where the
  surrounding code is pre-bind analysis.
- Reject or make unreachable in any post-bind-only surface where a marker would indicate binder
  leakage.
- Do not add `ExecutionPlan::Distinct`, executor branches, fragmenter handling, wire tags, or
  distributed worker cases.
- Add architecture coverage that scans for no physical DISTINCT operation and for binder-only
  elimination.

This marker is the load-bearing difference from a direct planner construction. SQL `*` is not
expanded before binding, and Rune frames do not carry schema. The binder is the first shared
altitude where "all visible columns" can be enumerated for both frontends.

## Phase 2 - SQL Planner Widening

Replace the blanket `distinct.is_some()` reject with a classifier:

- Accept only plain `DISTINCT`.
- Reject `DISTINCT ON` with a specific unsupported message.
- Reject when `GROUP BY`, `HAVING`, aggregate projections, set operations, SELECT modifiers,
  `TOP`, `QUALIFY`, or window expressions are present.
- Reject when the `FROM` shape contains joins.
- Keep derived-table and EXISTS DISTINCT rejects in their existing validation helpers.

Lowering order:

1. Lower `WHERE` and source relations exactly as today.
2. Lower the SELECT projection exactly as the non-DISTINCT path would.
3. If the projection is a single unqualified `*`, preserve today's source plan shape and wrap it
   in `LogicalPlan::Distinct`.
4. Otherwise wrap the ordinary projected plan in `LogicalPlan::Distinct`.
5. Apply ORDER BY, LIMIT, and other already-supported tail clauses after the marker.

The final output schema must match the non-DISTINCT SELECT output. ORDER BY after DISTINCT
resolves against that output; ordering by a non-projected source column rejects instead of using
hidden source data.

Do not relax the public `GROUP BY` no-aggregate rule. DISTINCT has a private marker path; user SQL
`SELECT a FROM t GROUP BY a` still fails.

## Phase 3 - Binder Lowering

Add binder support for `LogicalPlan::Distinct`:

1. Bind the `input` plan normally.
2. Read the bound input output schema.
3. Build one group-key expression/program entry per output ordinal (column references over the
   bound input schema by position).
4. Reuse the **existing** aggregate lowering rather than hand-rolling a physical aggregate:
   construct the `LogicalPlan::Aggregate` (or its bound equivalent) with those ordinal group keys
   and `aggr_expr = []` and route it through the existing `LogicalPlan::Aggregate` →
   `ExecutionPlan::Aggregate` path (`binder.rs:1134-1164`). This is ADR D2's intent and keeps
   DISTINCT on the tested substrate; do not re-implement `ExecutionPlan::Aggregate` construction
   for DISTINCT.
5. Output schema is ordinal-safe **for free**: `aggregate_schema` (`binder/aggregate.rs:235`)
   builds the output by extending the group-expr schema fields **in order** (no name
   re-resolution), so `SELECT a AS x, b AS x` keeps both `x` positions and dedups on both. The
   guard `adr231_distinct_binder_group_keys_are_ordinal_safe` pins this.

The group keys must be ordinal-safe — construct bound column references from the bound input
schema by position, never by output-name resolution. Because the reused `aggregate_schema` is
already ordinal/positional, no extra wrapper or final projection is needed to preserve duplicate
output names (add one only if implementation proves the reused path cannot).

The lowering produces the same physical aggregate substrate as explicit grouped aggregate, but it
must not call the public SQL `GROUP BY` path or remove the public no-aggregate reject — DISTINCT
reaches `LogicalPlan::Aggregate` through its own marker, not through the user `GROUP BY` lowering.

## Phase 4 - Rune Handle

Add a no-argument frame transform:

```rust
impl RuneDataFrame {
    pub fn distinct(self) -> Self {
        Self {
            plan: LogicalPlan::Distinct {
                input: Box::new(self.plan),
            },
        }
    }
}
```

Register `"distinct"` in the Toron Rune module. It dedups the current frame, equivalent to SQL
`SELECT DISTINCT *` over that frame. Projection DISTINCT remains compositional:

```text
ctx.table("sales").project([col("region"), col("product_id")]).distinct()
```

Do not add an expression-list Rune API in this cut. The whole point of the marker is that no-arg
Rune and SQL `DISTINCT *` share binder-altitude schema enumeration.

## Phase 5 - Architecture Guards

Add ADR 231 guards:

- `adr231_distinct_marker_lowers_to_empty_aggregate_in_binder` - pins that `LogicalPlan::Distinct`
  exists, is accepted from SQL/Rune, and is eliminated by binder lowering to empty aggregate.
- `adr231_distinct_binder_group_keys_are_ordinal_safe` - pins an ordinal-aware construction path
  and rejects name-resolution-only lowering.
- `adr231_distinct_is_frontend_neutral_sql_and_rune_share_the_marker` - SQL and Rune both emit
  the marker, not divergent ad hoc aggregate shapes.
- `adr231_distinct_adds_no_new_execution_plan_or_executor_op` - no physical DISTINCT op, executor
  branch, fragmenter case, wire tag, daemon path, or Wendelon production touch.
- `adr231_distinct_first_cut_fences_joins_subqueries_and_aggregates` - pins the deferred shapes
  and existing derived/EXISTS rejects.
- `adr231_user_group_by_without_aggregate_still_rejected` - public GROUP BY no-aggregate reject
  remains while DISTINCT gets the private marker path.
- `adr231_distinct_leaves_optimizer_evidence_surfaces_unchanged` - scans decision telemetry,
  receipt gates, runtime evidence, daemon flush, and Rune optimizer readout handles for no
  DISTINCT policy surface.

Retarget any existing guard that asserted `select.distinct.is_some()` is rejected at top level.
Keep the derived/EXISTS guard expectations intact.

## Phase 6 - Behavior Coverage

Use a small table with duplicate rows, duplicate tuples, and NULLs. Required assertions:

- Single-column DISTINCT dedups rows.
- Multi-column DISTINCT dedups whole tuples, not individual column values.
- Duplicate NULL tuples collapse to one result row.
- `SELECT DISTINCT *` groups all visible source columns.
- Aliases and computed expressions preserve the same output schema as non-DISTINCT projection.
- Duplicate output names group by position and preserve duplicate names.
- WHERE runs before dedup.
- ORDER BY and LIMIT run after dedup.
- ORDER BY non-projected source column rejects.
- DISTINCT over joins rejects with the explicit first-cut message.
- DISTINCT in derived tables and EXISTS remains rejected.
- DISTINCT with aggregate calls, GROUP BY, or HAVING rejects.
- Public `GROUP BY` without aggregate calls still rejects.
- Rune `project(...).distinct()` matches SQL `SELECT DISTINCT ...` plan shape and rows.
- Rune no-arg `distinct()` over a table matches SQL `SELECT DISTINCT *`.

## Verification Gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib select_distinct
scripts/cargo-cgroup test  -p toron --lib rune_distinct
scripts/cargo-cgroup test  -p toron --test architecture_test adr231
scripts/cargo-cgroup test  -p toron --test architecture_test adr102
scripts/cargo-cgroup test  -p toron --test architecture_test adr217
scripts/cargo-cgroup test  -p toron --test architecture_test adr230
scripts/cargo-cgroup test  -p toron --test architecture_test
scripts/cargo-cgroup test  -p toron --lib
scripts/cargo-cgroup fmt -p toron --check
git diff --check
```

Run a distributed execution fixture only if the implementation unexpectedly touches distributed
planning or exchange behavior. The intended implementation should not.

## Named Falsifications

1. Leaving the blanket SQL DISTINCT reject in place: every SQL behavior gate fails.
2. Implementing only SQL and not Rune: Rune parity tests and frontend-neutral guard fail.
3. Directly constructing aggregate in SQL planner: `SELECT DISTINCT *`, Rune no-arg distinct, and
   the marker-sharing guard fail.
4. Grouping by output name instead of ordinal: duplicate-output-name test and ordinal guard fail.
5. Relaxing public GROUP BY: `public_group_by_without_aggregate_still_rejects` and the guard fail.
6. Adding a new executor/operator: no-new-op guard fails.
7. Letting DISTINCT over joins or subqueries pass accidentally: fence tests fail.
8. Reusing a hidden source column for ORDER BY: non-projected ORDER BY test fails.
9. Treating NULLs as distinct values: null collapse test fails.
10. Wendelon/evidence/readout leakage: no-surface guard fails.

## Hygiene

- Keep new names semantic (`Distinct`, `distinct`, `empty_aggregate_dedup`), no version suffixes.
- Keep unsupported messages specific enough to identify the deferred shape.
- Do not add durable keys, system tables, readout handles, goldens, wire messages, or Wendelon
  production changes.
- Prefer existing binder projection/expression helpers over ad hoc expression printers.
- Keep marker elimination local to binder; do not let optimizer or executor code learn DISTINCT.

## Open For Implementation

1. Confirm the exact ordinal-aware binder helper available for "all input columns"; add the
   smallest helper if none exists.
2. Confirm sqlparser's pinned `Distinct` enum shape before choosing the exact `DISTINCT ON`
   unsupported branch.
3. Confirm current ORDER BY ordinal behavior. If `ORDER BY 1` is not already supported, do not
   introduce ordinal ORDER BY in this cut.
