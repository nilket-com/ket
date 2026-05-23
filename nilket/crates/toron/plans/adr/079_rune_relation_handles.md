# ADR 079: Rune Relation Handles — SQL as a Composable DataFrame Source

**Status:** Promoted (plan) — **genuine 3-of-3 (Codex, Gemini, Claude)**, fully
converged by iter-2, no fork.
**Category:** Toron frontend composition / Rune dataframe authoring / shared IR.
**Scope:** Toron-only. **No Wendelon wire / worker protocol / storage / PBC-opcode
/ telemetry / audit / dependency / `syn` change.**

## Summary

079 makes SQL **subordinate to Rune composition** without changing the Toron
execution model. `ctx.sql("<query>")` is a coordinator-side Rune authoring
primitive that parses one SQL query, lowers it into a lazy `LogicalPlan`, wraps it
as a `RuneDataFrame`, and lets the **existing** Rune dataframe methods (`.filter`,
`.aggregate`) add more IR above it. The final Rune script still returns one
`LogicalPlan`; the normal Toron binder, pushdown partitioners, placement, and
executors see the composed tree. Rune gains no local optimizer/binder/executor/
materialized result.

The proof fixture is small but load-bearing:

```rune
pub fn main(ctx) {
    ctx.sql("SELECT city, COUNT(1) AS cnt FROM customer GROUP BY city")
       .aggregate([], [sum(col("cnt"))])
}
```

It must construct one composed tree and **bind successfully** through the normal
binder, resolving outer `col("cnt")` against the SQL-derived output schema. The
assertion is **schema continuation across the SQL/Rune boundary**, not a brittle
adjacent-node shape.

## Doctrine

**SQL is a relation-producing frontend. Rune is the composition frontend. Toron IR
is the shared substrate.** SQL stays useful for compatibility/migration/concise
relational fragments; Rune is where larger programs compose them. Future SQL work
improves the shared relational substrate — it does not make SQL the highest-level
authoring model. *(A deliberate strategic direction, owned as a decision.)*

## Decisions (13, Promotion-Locked)

**D1 Lazy relation handle.** `RuneDataFrame` is a lazy carrier for a `LogicalPlan`
— not a `RecordBatch`, iterator, cursor, temp table, or materialized result.

**D2 Query-relation admission only.** `ctx.sql` accepts exactly one SQL statement
that lowers to a relation-producing query plan.

**D3 Explicit multi-statement rejection (grounded).** `ctx.sql` must reject parsed
SQL where `ast.len() != 1`. Required because `SqlPlanner::plan`
(`sql_planner.rs:864`) parses all statements but lowers only `ast[0]` — a Rune
relation helper must not silently ignore trailing statements such as
`SELECT ...; DROP TABLE ...` (which would otherwise plan the SELECT and drop the
DROP while passing the DDL/DML check).

**D4 Terminal SQL rejected in lazy composition.** `CREATE TABLE`/CTAS/`DROP TABLE`/
`INSERT`/`INSERT SELECT`/`DELETE`/`UPDATE` are terminal entry points, not Rune
relation sources (no side effects hiding in lazy dataframe graphs).

**D5 SQL plans are composable inputs.** Rune dataframe methods wrap a SQL-derived
plan with existing logical operators (`Filter`/`Project`/`Aggregate`). No SQL/Rune
materialization seam.

**D6 Final composed plan uses the normal Toron pipeline.** One `LogicalPlan`;
binding, pushdown, scan profiles, projection pruning, placement, and execution see
the whole composed tree. **No Rune-local optimizer, binder, executor, or pushdown
path.**

**D7 SQL aliases are Rune-addressable by name.** `col("cnt")` is the stable V1
interface; dot/method sugar (`.cnt.sum()`) is future polish that must lower to the
same named-column model.

**D8 Alias resolution is proven by binding.** Missing/duplicate/ambiguous names
from a SQL-derived relation fail in the normal binding path. **Promotion requires
a fixture that *binds* the SQL aggregate-alias continuation** (schema continuation,
typically via the SQL `Project` output names) — constructing a logical shape is not
enough.

**D9 Minimal aggregate continuation.** Add only `sum(expr)` to prove composition
over SQL output aliases. Broader dataframe ergonomics are future work.

**D10 Sandbox integrity.** SQL parse/planning errors surface as bounded frontend
errors; `ctx.sql` grants Rune **no** filesystem/network/process/env/time/random/
thread capability. ADR-007 sandbox invariants remain the safety floor.

**D11 No Rune execution actions.** No `collect`/`write`/`persist`/temp tables/
result handles/iterative orchestration; `RuneEngine::execute_script` still returns
a `LogicalPlan`. **Evidence may execute the returned plan through an existing test
harness** (to confirm the composed plan runs to a correct result), but Rune gains
no terminal action.

**D12 No worker-side Rune or row UDFs.** Rune is coordinator-side plan authoring;
worker UDF kernels / JIT / iterative dataframes / distributed actions are future
ADRs.

**D13 No SQL frontend expansion.** No SQL `FROM` subqueries, recursive CTEs, new
expression syntax, or new optimizer pass. SQL subqueries are the explanatory
analogy for the target IR shape, not an implementation dependency.

## Architecture Guards (14)

`adr079_rune_dataframe_remains_lazy_logical_plan_handle`,
`adr079_toron_context_exposes_sql_relation_source`,
`adr079_ctx_sql_rejects_multi_statement_input`,
`adr079_ctx_sql_rejects_ddl_and_dml`,
`adr079_ctx_sql_does_not_execute_or_materialize`,
`adr079_sql_derived_relation_can_be_wrapped_by_rune_aggregate`,
`adr079_sql_alias_can_feed_rune_column_expression`,
`adr079_nested_sql_aggregate_continuation_binds_inner_alias`,
`adr079_composed_rune_sql_plan_enters_normal_binder_path`,
`adr079_no_rune_local_optimizer_binder_or_executor`,
`adr079_no_worker_side_rune_udf_surface`,
`adr079_no_new_wendelon_wire_storage_pbc_opcode_or_dependency`,
`adr079_sqlparser_ast_remains_quarantined_to_sql_planner`,
`adr079_rune_sandbox_allow_list_remains_in_force`.

## Integration Fixtures (8)

`rune_ctx_sql_select_returns_lazy_dataframe`;
`rune_ctx_sql_rejects_multi_statement_input` (D3);
`rune_ctx_sql_rejects_create_insert_update_delete_drop`;
**`rune_sql_group_by_alias_can_feed_outer_sum_and_bind`** (the binding gate, D8);
`rune_sql_group_by_alias_outer_sum_executes_if_existing_harness_supports_it`
(the bounded execution check, D11);
`rune_sql_filter_then_rune_aggregate_composes_single_logical_plan`;
`rune_sql_composition_preserves_existing_pushdown_path`;
`rune_sql_parse_error_maps_to_frontend_error_without_capability_escape`. Existing
`rune_sandbox_test` + SQL group-by/aggregate/predicate-pushdown coverage stays green.

## Out of Scope

No `collect`/`write`/`persist`; no materialization; no distributed iteration; no
recursive CTE; no SQL `FROM` subquery; no dot-field syntax; no schema/type feedback
loop in the Rune compiler; no row-level Rune UDFs; no worker-side Rune; no JIT; no
new optimizer pass; no `syn`.

## Council History

iter-0: Codex seeded the seam (SQL → lazy relation handle, Rune composes more IR);
**Claude identified the proof hinge** — the SQL/Rune boundary must *bind* through
the normal schema path, not just construct a plan. iter-1: **Codex's grounded
multi-statement rejection** (confirmed `SqlPlanner::plan` lowers only `ast[0]`);
**Claude softened the "novel adjacent nested aggregate" risk** — the invariant is
schema continuation through SQL output names (often via `Project`), not any
physical node adjacency, so the binding hinge is a well-exercised path. iter-2:
**Gemini converged on the same 13-decision shape** (adopting the multi-statement
guard + binding-proof). Genuine 3-of-3, no fork.

## Promotion criteria met

D-set 13; 14 guards; 8 fixtures; behavior-additive (existing SQL + Rune unchanged);
Toron-only; no new dependency/wire/storage/PBC-opcode/worker-Rune/execution-action/
SQL-subquery/`syn`. The promotion gate is the **bound** SQL-alias continuation
fixture + multi-statement rejection. Migration:
`plans/impl/079_rune_relation_handles.md`.

## Conclusion

079 turns SQL from a terminal experience into one producer of a lazy relation
handle that Rune composes with existing dataframe IR — proven by **binding** an
aggregate over a SQL output alias (schema continuation), with explicit
multi-statement rejection, DDL/DML rejection, sandbox integrity, and no Rune
execution/optimizer. Small, sound seam; Rune is now genuinely above SQL in the
authoring stack, with the shared Toron IR unchanged underneath.
