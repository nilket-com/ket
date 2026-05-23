# Impl Plan 079: Rune Relation Handles — SQL as a Composable DataFrame Source

**Status:** Promoted, paired with `plans/adr/079_rune_relation_handles.md`.
**Posture:** Coordinator-side authoring seam. Reuse the existing `SqlPlanner` and
`RuneDataFrame { plan: LogicalPlan }`; add the smallest surface proving SQL can
produce a lazy relation and Rune can compose more IR over it. **Toron-only; no new
dependency/wire/opcode/`syn`; no Rune execution action.** Seven phases.

## Verified anchors (grounded)

- `RuneDataFrame { plan: LogicalPlan }` already has `.filter` (`rune_engine.rs:182`)
  and `.aggregate(group_expr, aggr_expr)` (`:202`); `ctx.table` (`:233`) builds one
  from `ScanNamed`; `linear_regress`/`approx_quantile` (`:262/269`) are the
  aggregate-helper pattern; `create_toron_module` (`:296`); `execute_script` returns
  a `LogicalPlan` (`:377`).
- `SqlPlanner::plan` (`sql_planner.rs:864`) parses all statements but `match
  &ast[0]` — **no `ast.len() == 1` check** (the basis for D3's explicit guard).
- SELECT aliases lower through projection `output_names` (plain SELECT projection
  handling and aggregate final projections), so a SQL-derived plan's **output
  schema names the aliases** — the binding boundary.

## Invariants carried in

- Rune constructs plans; it does not execute data work. `ctx.sql` accepts exactly
  one query/`SELECT`; **multi-statement + DDL/DML rejected**. The returned dataframe
  is indistinguishable from `ctx.table(...)` for later composition. The composed
  plan enters the **normal** binder/pushdown path (no Rune-local optimizer/
  executor). ADR-007 sandbox allow-list intact. No new dep/wire/opcode/`syn`.

## Phase 0 — Failing tests first (incl. the binding gate)

- `rune_ctx_sql_select_returns_lazy_dataframe`
- `rune_ctx_sql_rejects_multi_statement_input` (D3)
- `rune_ctx_sql_rejects_create_insert_update_delete_drop`
- **`rune_sql_group_by_alias_can_feed_outer_sum_and_bind`** — the load-bearing gate:
  the composed `aggregate([],[sum(col("cnt"))])` over `ctx.sql("… COUNT(1) AS cnt …
  GROUP BY city")` must **bind** (resolve `col("cnt")` against the SQL output
  schema), not merely construct.
- `rune_sql_parse_error_maps_to_frontend_error_without_capability_escape`

## Phase 1 — Relation-only SQL admission helper

Add a relation-only entry point on the SQL planner, not a second parser or
Rune-local planner:

```rust
impl SqlPlanner {
    pub fn plan_relation(&self, sql: &str) -> Result<LogicalPlan, FrontendError> {
        let statement = self.parse_one_statement(sql)?;
        let plan = self.lower_statement(statement)?;
        reject_terminal_plan_for_relation(&plan)?;
        Ok(plan)
    }
}
```

`parse_one_statement` uses the existing sqlparser dialect, rejects empty SQL, and
**rejects `ast.len() != 1`** (D3). `lower_statement` preserves the current
statement dispatch. `plan_relation` then **rejects terminal statement plans**
(`CreateTable`, `CreateTableAsSelect`, `DropTable`, `Insert`, `InsertSelect`,
`Delete`, `Update`). Everything else is a relation plan. Errors say "`ctx.sql`
accepts single query relations only"; no extra capability surface leaks into Rune.
Do not add SQL string classification.

## Phase 2 — `ToronContext::sql`

```rust
impl ToronContext {
    pub fn sql(&self, query: String) -> Result<RuneDataFrame, FrontendError> {
        Ok(RuneDataFrame {
            plan: SqlPlanner::new().plan_relation(&query)?,
        })
    }
}
```

Register beside `ToronContext::table` in `create_toron_module()`. Parse/planning
failure surfaces as a frontend error, not a host panic.

## Phase 3 — Minimal aggregate continuation

```rust
pub fn sum(expr: RuneExpr) -> RuneAggr {
    RuneAggr { func: AggregateFunction::Sum, args: vec![expr.expr] }
}
```

Register beside `linear_regress`/`approx_quantile`. Do not broaden the dataframe
API beyond the fixtures.

## Phase 4 — Composition + binding/pushdown regression

- Non-aggregate continuation fixture: `ctx.sql("SELECT id, amount FROM sales WHERE
  region = 'west'").filter(col("amount").gt(lit(100)))` → one logical tree
  (`Filter` over the SQL-derived plan).
- `rune_sql_composition_preserves_existing_pushdown_path` — the composed predicate
  reaches the **normal** binder/pushdown, not a Rune-specific path.
- `rune_sql_group_by_alias_outer_sum_executes_if_existing_harness_supports_it` —
  the bounded execution check (D11): run the composed plan through an existing
  harness to confirm it executes to the right result (Rune gains no terminal
  action).

## Phase 5 — Architecture guards

Add the 14 ADR-079 guards (lazy-handle, sql-relation-source, **reject-multi-
statement**, reject-DDL/DML, no-materialize, wrappable-by-aggregate, alias-feeds-
column, **nested-aggregate-continuation-binds-inner-alias**, enters-normal-binder,
no-Rune-optimizer/binder/executor, no-worker-Rune-UDF, no-new-dep/wire/opcode,
sqlparser-quarantined, sandbox-allow-list). Place them in the semantic architecture
buckets (077 layout), not a new ADR-number module.

## Phase 6 — Evidence

`plans/impl/079_rune_relation_handles_evidence.md`: `cargo fmt`; `cargo test -p
toron --test architecture_test adr079_`; `rune_sandbox_test`; `group_by_test`;
`predicate_widening_test`; `--lib`; feature-matrix `cargo check`; `clippy -D
warnings`; `git diff --check`. Record the bound-alias-continuation result + the
multi-statement rejection. Note any disk-limited partials.

## Watch-items

1. **Multi-statement rejection (D3)** — `ctx.sql` must enforce `ast.len()==1`; the
   existing planner silently uses `ast[0]`. The most likely soundness gap.
2. **Bind, don't just construct (D8)** — the gate fixture must route through the
   binder and resolve `col("cnt")` against the SQL output schema.
3. **Reject side effects (D4)** — DDL/DML through `ctx.sql` would make a lazy
   handle dishonest.
4. **No Rune optimizer/executor (D6/D11)** — the composed plan uses the existing
   pipeline; `execute_script` still returns a plan.
5. **ADR-007 sandbox (D10)** — adding SQL parsing must not loosen module allow-
   lists or capability denials.
6. **No SQL-parser creep (D13)** — `FROM` subqueries are the analogy, not a dep.

## Acceptance checklist

- [ ] `ctx.sql("SELECT …")` returns a lazy `RuneDataFrame`; rejects multi-statement
      + CREATE/CTAS/INSERT/UPDATE/DELETE/DROP.
- [ ] `sum(col("cnt"))` aggregate over a SQL alias **binds** (gate fixture).
- [ ] Rune-composed SQL plans bind + push down through the normal Toron path.
- [ ] 14 guards pin no materialization/no-Rune-executor/no-worker-UDF/no-new-
      dep-wire-opcode/sqlparser-quarantine/sandbox.
- [ ] Existing Rune-sandbox + SQL aggregate + pushdown tests stay green.
- [ ] Evidence captures the bound continuation + multi-statement rejection.
