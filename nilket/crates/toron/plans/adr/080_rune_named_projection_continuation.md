# ADR 080: Rune Named Projection Continuation — Multi-Hop Schema Naming

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)**, converged
by iter-2 — the iter-1 alias-on-`RuneExpr` fork resolved to the `RuneNamedExpr`
carrier (existing `RuneExpr→RuneAggr` idiom + type safety + parallel-vector
invariant by construction); `alias: Option<String>` recorded as the rejected
alternative. Consensus verified against the latest peer drafts before promotion.
**Category:** Toron frontend composition / Rune dataframe naming / schema
continuation over shared IR.

## Scale Boundary

Toron-only, coordinator-side Rune authoring. ADR 080 adds Rune host surface that
lowers to existing Toron IR (`LogicalPlan::Project.output_names` and stable
`AggregateFunction` variants). It does not change Wendelon wire, worker protocol,
storage, PBC opcodes, telemetry, audit, dependencies, or `syn`. Rune does not
execute, collect, persist, materialize, or run on workers. Production code change
is expected to stay localized to `crates/toron/src/rune_engine.rs`.

## Summary

ADR 080 makes schema names first-class at the Rune composition layer. ADR 079 made
SQL a lazy relation source under Rune and proved one SQL alias could feed a Rune
aggregate; 080 extends that proof across a **multi-hop** chain by adding a named
projection carrier (`RuneNamedExpr`), a `.select(...)` dataframe method lowering to
**existing** `LogicalPlan::Project.output_names`, and the stable aggregate helper
pack (`count_all`, `count`, `avg`, `min`, `max`). No IR variant, no binder seam, no
engine change.

The promotion gate is one composed Rune program that **binds and executes**:

```rune
pub fn main(ctx) {
    ctx.sql("SELECT region, amount FROM sales WHERE status = 'open'")
       .filter(col("amount").gt(lit(10)))
       .select([col("region").named("bucket"), col("amount").named("net")])
       .aggregate([col("bucket")], [sum(col("net")), count_all()])
       .select([col("bucket").named("bucket"), col("sum").named("gross"), col("count").named("n")])
       .filter(col("gross").gt(lit(20)))
}
```

SQL output names feed Rune filters; Rune `.select` defines new visible names; Rune
aggregate consumes those + emits existing kernel names; post-aggregate `.select`
renames them; the final filter binds the renamed schema. One `LogicalPlan` enters
the normal Toron binder and executor.

## Doctrine

**Rune is the composition frontend; names are the connective tissue.** Schema
continuation across composition boundaries is explicit and bindable. Toron IR
remains the shared substrate; SQL remains a relation-producing frontend under Rune.

## Grounding Ledger (every load-bearing claim, verified)

- **`LogicalPlan::Project { exprs, output_names: Vec<Option<String>> }`**
  (`ir.rs:225–228`, ADR 023) — `.select` lowers straight into it; no IR variant.
- **Parallel-vector invariant** enforced by `debug_assert_eq!(root_count,
  output_names.len())` (`binder.rs:3349`); the carrier's required `name: String`
  guarantees one `Some(name)` per expr, so it cannot fire from Rune.
- **Continuation names resolve.** `aggregate_schema` (`binder.rs:3330`) names
  group-key fields by their column expr + aggregate fields by the kernel catalog
  `output_name` (`aggregate_kernel.rs`: `Sum→"sum"`, `Count/CountAll→"count"`,
  `Min/Max/Avg`). After `.aggregate([col("bucket")], [sum(col("net")),
  count_all()])` the schema is `["bucket","sum","count"]`.
- **Carrier is the existing idiom** — the current `sum`, `linear_regress`, and
  `approx_quantile` helpers already transform `RuneExpr → RuneAggr`; `RuneExpr →
  RuneNamedExpr` via `.named()` is the same carrier pattern. An alias field on
  `RuneExpr` would be a pattern used nowhere.
- **`count_all()` placeholder parity** — `vec![Expr::column("count")]` in both
  the SQL planner's `COUNT(*)` lowering and existing `RuneDataFrame::count()`;
  binder discards CountAll args (`binder.rs:2534`).
- **Duplicate-name behavior is grounded by an existing test, not assumed** —
  `grouped_aggregate_order_by_duplicate_generated_name_is_ambiguous`
  (`group_by_test.rs:1220`): `SELECT region, COUNT(*), COUNT(amount) … ORDER BY
  count` → `BindingError::AmbiguousColumn { column: "count" }`. The Rune
  `[sum(a), sum(b)]` → `col("sum")` case is the same binder path.
- **`lit` is i32-only** — the gate stays within it (SQL
  supplies `status='open'`; Rune compares with integer lits). String/float Rune
  literals are deferred.

**Hand-verified gate output** against the `sales` fixture
(region=[east,west,east,east,west,NULL], amount=[10,20,NULL,30,NULL,7],
status=[open,open,closed,open,open,open]): `WHERE status='open'` drops id3;
`amount>10` keeps id2(west,20)+id4(east,30); group→{west:(20,1), east:(30,1)};
`gross>20` keeps east only → single row **`bucket="east", gross=30, n=1`**.

## Decisions (15, promotion-locked)

**D1 Named projection carrier.** `RuneNamedExpr { expr: Expr, name: String }` — Rune
host type, no standalone IR semantics, no binder behavior.
**D2 Method naming surface.** `RuneExpr::named(name: String) -> RuneNamedExpr`;
canonical `col("amount").named("net")`. Global `named(expr, name)` fallback allowed
only if Rune method registration fails and the reason is recorded.
**D3 Named dataframe select.** `RuneDataFrame::select(Vec<RuneNamedExpr>) ->
RuneDataFrame` → `Project { exprs, output_names: all Some(name) }`.
**D4 Anonymous project remains.** `.project(Vec<RuneExpr>)` stays the anonymous
surface (all `None`); not renamed/removed.
**D5 No alias state on `RuneExpr`.** Expression carrier only — aliases never flow
through filters, aggregate args, comparisons, or future combinators. Guarded.
**D6 Stable aggregate helper pack.** `count_all/count/avg/min/max`; `sum` stays from
079; experimental `linear_regress`/`approx_quantile` unchanged.
**D7 CountAll placeholder parity.** `count_all()` → `(CountAll,
vec![Expr::column("count")])`.
**D8 Dataframe count stays separate.** `RuneDataFrame::count()` remains the scalar
whole-frame shortcut; `count_all()` is a `RuneAggr` for use inside `.aggregate`.
**D9 Aggregate outputs renamed by post-projection.** No aggregate aliases, no
`RuneAggr::named`, no `output_names` on `LogicalPlan::Aggregate`.
**D10 Duplicate aggregate output names remain ambiguous** — resolve to the existing
`AmbiguousColumn`; no new disambiguation. Pinned by a negative fixture.
**D11 Names proved by binding** — the gate binds (and executes); construction is not
sufficient evidence.
**D12 Normal Toron pipeline only** — no Rune-local schema checker/binder/optimizer/
executor/pushdown/materialization.
**D13 Sandbox unchanged** — new host fns/types only in `create_toron_module`; no
allow-list widening; ADR-007 floor holds.
**D14 No SQL expansion** — no subqueries, grammar work, aggregate-alias machinery,
or SQL optimizer change.
**D15 No join** — no `LogicalPlan::Join` / relation alias / qualified-column surface.

## Architecture Guards (15)

`adr080_rune_named_expr_is_host_surface_only`,
`adr080_rune_expr_does_not_carry_alias_state`,
`adr080_named_select_lowers_to_existing_project_output_names`,
`adr080_anonymous_project_surface_remains_available`,
`adr080_aggregate_helper_pack_uses_existing_stable_functions`,
`adr080_count_all_helper_uses_placeholder_count_arg`,
`adr080_dataframe_count_shortcut_remains_separate`,
`adr080_no_logical_aggregate_output_names_or_alias_field`,
`adr080_duplicate_aggregate_output_names_use_existing_ambiguous_error`,
`adr080_named_continuation_binds_through_normal_binder`,
`adr080_no_rune_local_schema_binder_optimizer_or_executor`,
`adr080_no_join_surface_in_named_projection_cut`,
`adr080_no_sql_planner_grammar_or_subquery_expansion`,
`adr080_no_new_wendelon_wire_worker_storage_pbc_opcode_or_dependency`,
`adr080_rune_sandbox_allow_list_remains_toron_authoring_only`.

## Integration Fixtures (8)

`rune_named_sql_multi_hop_continuation_binds_and_executes` (gate: visible schema
`bucket,gross,n`, row `east,30,1`); `rune_named_select_sets_project_output_names`;
`rune_named_select_binds_visible_schema`;
`rune_aggregate_helper_pack_binds_stable_outputs`;
`rune_named_post_aggregate_select_renames_outputs`;
`rune_duplicate_aggregate_output_name_is_ambiguous`;
`rune_named_select_rejects_misused_named_expression` (passing `RuneNamedExpr` to
`sum`/`.filter` is a bounded frontend error, not a silent alias drop — the carrier's
type-safety payoff); `rune_named_projection_preserves_adr079_sql_relation_guards`.

**079 discipline carried:** any latent binder/schema bug surfaced on this path is
fixed only when it's the same schema-continuation path *and* pinned by a **pure-SQL/
non-Rune** regression fixture.

## Out of Scope

Joins, relation aliases, qualified-column sugar, dot-field access,
`alias: Option<String>` on `RuneExpr`, mixed named/unnamed `.select`, aggregate
aliases / `RuneAggr::named` / `LogicalPlan::Aggregate.output_names`, duplicate
disambiguation, string/float/bool Rune literal helpers, collection/write/
materialization, worker-side Rune, row UDFs, JIT, SQL grammar/subquery expansion,
new optimizer pass, new dependency, `syn`.

## Council History

iter-0: Codex seeded named projection; **Claude sharpened the proof hinge to
binding, not construction.** iter-1: Gemini proposed alias-on-`RuneExpr`; **Codex +
Claude resolved to the `RuneNamedExpr` carrier** on the `RuneAggr` idiom + type
safety; **Claude grounded `count_all()` placeholder parity and the `AmbiguousColumn`
boundary.** iter-2: **Gemini conceded the carrier → genuine 3-of-3.** iter-3: final
dual; consensus verified against the latest peer drafts; D10 anchored to the
existing ambiguity test (`group_by_test.rs:1220`) and the gate row hand-verified.

## Conclusion

ADR 080 makes naming first-class in Rune dataframe authoring via a named projection
surface (`.select`) and the stable aggregate helper pack, proving multi-hop schema
continuation over `ctx.sql` binds and executes through the normal Toron pipeline —
with the IR substrate unchanged. Migration:
`plans/impl/080_rune_named_projection_continuation.md`.
