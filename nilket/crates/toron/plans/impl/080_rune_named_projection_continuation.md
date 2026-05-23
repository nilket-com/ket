# Impl Plan 080: Rune Named Projection Continuation — Multi-Hop Schema Naming

**Status:** Promoted. Paired with
`plans/adr/080_rune_named_projection_continuation.md`. Genuine 3-of-3.
**Posture:** Narrow Rune authoring surface over existing IR. Expected production
change is localized to `crates/toron/src/rune_engine.rs`; tests and architecture
guards carry the rest.

## Implementation invariants

- Reuse `LogicalPlan::Project.output_names` (`ir.rs:225`) and
  `AggregateFunction::{CountAll,Count,Sum,Avg,Min,Max}`. **No new IR.**
- `RuneExpr` stays alias-free; `.project(Vec<RuneExpr>)` stays anonymous/unchanged.
- `.select(Vec<RuneNamedExpr>)` is all-named visible-schema shaping.
- No SQL grammar/join/worker-side Rune/execution action/materialization/new
  dependency/`syn`. ADR 079 `ctx.sql` behavior (relation-only, single-statement,
  DDL/DML-rejecting) preserved.

## Phase 0 — failing tests first (`tests/rune_relation_handles_test.rs`)

Reuse the 079 helpers (`rune_plan`, `catalog`, `run_logical`, `frontend_message`,
`int64_column_rows`); add a small string/int row-inspector if needed.

1. **`rune_named_sql_multi_hop_continuation_binds_and_executes` (gate)** — the ADR
   program. Assert visible bound schema `[bucket, gross, n]`; execute to the
   hand-verified single row **`bucket="east", gross=30, n=1`**.
2. `rune_named_select_sets_project_output_names` — `ctx.table("sales").select([
   col("region").named("bucket"), col("amount").named("net")])` →
   `Project.output_names == [Some("bucket"), Some("net")]`.
3. `rune_named_select_binds_visible_schema` — bind #2; visible `[bucket, net]`.
4. `rune_aggregate_helper_pack_binds_stable_outputs` — `aggregate([], [count_all(),
   count(col("amount")), sum(col("amount")), avg(col("amount")), min(col("amount")),
   max(col("amount"))])` binds; stable names `[count, count, sum, avg, min, max]`
   (intentional duplicate `count`; do not resolve here).
5. `rune_named_post_aggregate_select_renames_outputs` — `aggregate([],
   [sum(col("amount"))]).select([col("sum").named("gross")])` → visible `[gross]`.
6. `rune_duplicate_aggregate_output_name_is_ambiguous` — `aggregate([],
   [sum(col("amount")), sum(col("id"))]).select([col("sum").named("gross")])` →
   `BindingError::AmbiguousColumn { column: "sum" }`. Grounded by the SQL analog
   `grouped_aggregate_order_by_duplicate_generated_name_is_ambiguous`
   (`group_by_test.rs:1220`) — same binder path.
7. `rune_named_select_rejects_misused_named_expression` — `aggregate([],
   [sum(col("amount").named("net"))])` → bounded frontend error, **no capability
   leak**; do not pin exact Rune diagnostic text (the carrier's type-safety payoff).
8. `rune_named_projection_preserves_adr079_sql_relation_guards` — ADR 079
   multi-statement / DDL-DML rejection, SQL-alias continuation, sandbox, and
   normal-pushdown preservation stay green.

## Phase 1 — host type + `.named` (`rune_engine.rs`)

```rust
#[derive(Any, Debug, Clone)]
pub struct RuneNamedExpr { pub(crate) expr: Expr, pub(crate) name: String }

impl RuneExpr {
    pub fn named(self, name: String) -> RuneNamedExpr {
        RuneNamedExpr { expr: self.expr, name }
    }
}
```

`self`-by-value mirrors `RuneExpr::gt` / `RuneExpr::eq` and the existing
`RuneExpr→RuneAggr` helper pattern. Do **not** add `alias: Option<String>` to
`RuneExpr`.

## Phase 2 — `RuneDataFrame::select`

```rust
pub fn select(self, exprs: Vec<RuneNamedExpr>) -> Self {
    let mut plan_exprs = Vec::with_capacity(exprs.len());
    let mut output_names = Vec::with_capacity(exprs.len());
    for named in exprs { plan_exprs.push(named.expr); output_names.push(Some(named.name)); }
    Self { plan: LogicalPlan::Project { input: Box::new(self.plan), exprs: plan_exprs, output_names } }
}
```

`output_names.len() == exprs.len()` by construction → the `binder.rs:3349`
`debug_assert` cannot fire.

## Phase 3 — stable aggregate helper pack

```rust
pub fn count_all() -> RuneAggr { RuneAggr { func: AggregateFunction::CountAll, args: vec![Expr::column("count")] } }
pub fn count(expr: RuneExpr) -> RuneAggr { RuneAggr { func: AggregateFunction::Count, args: vec![expr.expr] } }
pub fn avg(expr: RuneExpr)   -> RuneAggr { RuneAggr { func: AggregateFunction::Avg,   args: vec![expr.expr] } }
pub fn min(expr: RuneExpr)   -> RuneAggr { RuneAggr { func: AggregateFunction::Min,   args: vec![expr.expr] } }
pub fn max(expr: RuneExpr)   -> RuneAggr { RuneAggr { func: AggregateFunction::Max,   args: vec![expr.expr] } }
```

Leave `RuneDataFrame::count()`, `sum`, `linear_regress`, and `approx_quantile`
untouched.

## Phase 4 — registration

```rust
m.ty::<RuneNamedExpr>()?;
m.function(["count_all"], count_all).build()?;
m.function(["count"], count).build()?;
m.function(["avg"], avg).build()?;
m.function(["min"], min).build()?;
m.function(["max"], max).build()?;
m.associated_function("select", RuneDataFrame::select)?;
m.associated_function("named", RuneExpr::named)?;
```

No default Rune modules; no new capability-bearing module. **Watch:** if
`RuneExpr::named -> RuneNamedExpr` won't register cleanly, fall back to a global
`named(expr, name)` and record why — **never** switch to alias-on-`RuneExpr` (that
silently reopens the closed fork).

## Phase 5 — architecture guards (`tests/architecture/rune_frontend.rs`, 077 layout)

The 15 ADR-080 guards. Behaviorally load-bearing:
- `adr080_rune_expr_does_not_carry_alias_state` — `struct RuneExpr` has `expr: Expr`
  and **not** `alias`/`output_name`/`Option<String>` (encodes the resolved fork).
- `adr080_count_all_helper_uses_placeholder_count_arg` — `count_all` body has
  `AggregateFunction::CountAll` + `Expr::column("count")`.
- `adr080_named_select_lowers_to_existing_project_output_names` — `select` body has
  `LogicalPlan::Project` + `output_names.push(Some(` and no new IR variant.
- `adr080_no_logical_aggregate_output_names_or_alias_field` — `LogicalPlan::Aggregate`
  still only `input/group_expr/aggr_expr`; `RuneAggr` has no name/alias field.
- `adr080_dataframe_count_shortcut_remains_separate`,
  `adr080_anonymous_project_surface_remains_available`,
  `adr080_aggregate_helper_pack_uses_existing_stable_functions`,
  `adr080_duplicate_aggregate_output_names_use_existing_ambiguous_error`,
  `adr080_named_continuation_binds_through_normal_binder` (fixture exists + calls
  `bind(`; no new binder-like fn in `rune_engine.rs`).
- Carry ADR-079 no-Rune-local-optimizer/executor, no-worker-Rune, no-new-wire/PBC/
  dep, sqlparser-quarantine, sandbox guards under ADR-080 names.

If `rune_frontend.rs` grows too large, follow the ADR 077 semantic-submodule
convention rather than an ADR-numbered module.

## Verification

```
cargo fmt
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test architecture_test adr080_
cargo test -p toron --test architecture_test adr079_
cargo test -p toron --test rune_sandbox_test
cargo test -p toron --test group_by_test          # incl. the :1220 ambiguity anchor
cargo clippy -p toron -- -D warnings
git diff --check
cargo test -p toron                                # final full-package pass
```

## Evidence doc (`plans/impl/080_rune_named_projection_continuation_evidence.md`)

Record: files touched (expect `rune_engine.rs` only for production); the
carrier-vs-alias 3-of-3 resolution; the gate's visible schema + `east,30,1` row; the
duplicate-name `AmbiguousColumn` result with its SQL-analog anchor; commands +
counts; and — if any latent binder bug surfaces — its **pure-SQL/non-Rune**
regression fixture (079 precedent).

## Acceptance checklist

- [ ] `RuneNamedExpr` exists; `RuneExpr` alias-free.
- [ ] `.select` → existing `Project.output_names`; `.project` unchanged.
- [ ] `count_all/count/avg/min/max` registered + bind; `count_all` uses placeholder.
- [ ] Multi-hop `ctx.sql` continuation binds + executes to `east,30,1`.
- [ ] Duplicate aggregate names → existing `AmbiguousColumn`.
- [ ] Misused `RuneNamedExpr` → bounded frontend error, no capability leak.
- [ ] ADR 079 relation-handle + sandbox tests green.
- [ ] 15 guards pin no-alias-field / no-aggregate-alias / no-join / no-SQL-expansion /
      no-new-capability-dep-wire.
