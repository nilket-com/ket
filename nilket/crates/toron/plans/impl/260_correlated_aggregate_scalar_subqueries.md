# Implementation Plan — ADR 260: Correlated aggregate scalar subqueries

## Summary

Frontend-only change in `sql_planner.rs`. `pull_up_scalar_subqueries` stops rejecting a **WHERE**
outer reference; it classifies the subquery WHERE through `classify_exists_conjunct`, and when
correlation pairs exist it builds a **grouped aggregate keyed on the correlation** and folds it with a
**`LeftOuter`** join (built directly) instead of 257's keyless `Cross`. The aggregate-argument
correlation reject (`:9207`) stays; COUNT gets a `COALESCE(., 0)` wrap. No IR / executor / fragmenter /
binder / join-kind change; no `Expr::Alias`.

## Verify-first probe — DONE (gate passed, kept as a permanent test)

Ran before promotion. The decorrelated shape binds and fragments as
`LeftOuter / HashPartitioned / 3 stages` (left producer + grouped-aggregate producer + join consumer).
Broadcast is impossible for non-inner (`fragment.rs:775`); scan metadata descends through `Aggregate`
(`first_scan_metadata`). The "frontend-only, distributes on the hash path" claim is proven, not
assumed. Probe: `distributed_process_test::adr260_verify_probe_left_outer_over_grouped_aggregate_fragments`
(rides this impl commit, not the plan commit).

**Finding (already reflected in Phase 4):** an inline SQL `LEFT JOIN (subquery)` in a non-inner chain
rejects at `lower_select:3679` (`"derived tables in joins are only supported in all-inner chains"`).
260 sidesteps it by constructing the `LeftOuter` LogicalPlan node via `build_join_plan`, never lowering
a SQL derived-table join. **Do not** route 260's recombine through SQL derived-table-in-join lowering.

## Phase 1 — represent pulled scalars (`sql_planner.rs`)

Replace the bare collection vector with a struct carrying the recombine intent:

```rust
struct PulledScalar {
    plan: LogicalPlan,
    on: Vec<(Expr, Expr)>,            // empty => 257 keyless Cross; non-empty => 260 keyed LeftOuter
}
```

Thread it through `pull_up_scalar_subqueries`, `rewrite_select_item_scalar_subqueries`, the WHERE
scalar pull-up in `lower_select`, and `cross_join_scalar_subplans` (renamed `join_scalar_subplans`).
`--all-targets` catches any missed caller.

## Phase 2 — narrow validation (`pull_up_scalar_subqueries`, ~`:5573`)

Rename `validate_scalar_subquery_is_global_aggregate` → `validate_aggregate_scalar_subquery_shape`.

- **Keep**: one FROM item, no joins, no GROUP BY/HAVING/DISTINCT, exactly one projected aggregate, and
  the **projection / aggregate-argument** outer-ref reject (`:9207`,
  `"correlated scalar subqueries are not supported"`).
- **Lift**: the **WHERE** outer-ref reject (`:9212`). WHERE correlation is now classified and
  decorrelated. A non-equi WHERE correlation rejects downstream via the classifier
  (`"unsupported EXISTS subquery predicate"`).

## Phase 3 — classify the WHERE, build the inner plan

In the `SqlExpr::Subquery(query)` arm:

1. Validate shape; extract the single aggregate projection item.
2. Lower the scalar subquery's one table; compute `outer_qualifiers` (outer plan) and `inner_qualifiers`
   (inner plan).
3. Split `select.selection` with `split_top_level_and` + `classify_exists_conjunct` into
   `correlation_pairs: Vec<(Expr, Expr)>` (the `(outer, inner)` halves) and
   `inner_residuals: Vec<Expr>`.
4. Apply residuals as `LogicalPlan::Filter { input: inner_plan, predicate }` before aggregation.
5. **`correlation_pairs.is_empty()`** → keep 257: `lower_query_scoped(query)`, `Alias{ columns:
   ["value"] }`, push `PulledScalar { on: vec![], .. }`. This path stays byte-equivalent to 257,
   including COUNT (the global aggregate already returns `0` for empty input).

## Phase 4 — build the correlated grouped aggregate (the one real seam)

For non-empty correlation pairs, build the right side from existing logical nodes — **do not hand-roll
aggregate lowering, exchange insertion, or `Expr::Alias`** (the IR names outputs via
`Project.output_names` and `Alias.columns`):

- `aggr_expr` from `lower_aggregate_function(function, AggregateSqlMode::Grouped)` for the single scalar
  aggregate (reject `distinct` for this cut unless the existing distinct-aggregate rewrite is proven
  cheap at impl);
- `group_expr` = the inner halves of the correlation pairs;
- `Exchange { input: filtered_inner, key: group_expr.clone(), partition_count:
  DEFAULT_AGGREGATE_PARTITION_COUNT }` under `LogicalPlan::Aggregate`;
- name `[key_0, …, key_n, value]` via `LogicalPlan::Project { output_names }`, then wrap in
  `LogicalPlan::Alias { alias: __scalar_N, columns: [...] }`;
- join pairs: `(outer_i, Expr::qualified_column(TableRef::new("__scalar_N"), format!("key_{i}")))`.

> The aggregate argument (`dim.v`) must lower against the inner plan's schema — assert this at impl.
> An alternative route (synthesize a grouped `Query` and ride `lower_query_scoped`) needs a SqlExpr
> WHERE split; the classifier route above keeps the non-equi reject uniform and avoids that
> duplication. Pick whichever keeps the diff smaller and the aggregate type-check on the existing path.

## Phase 5 — fold + COUNT wrap

`cross_join_scalar_subplans` → `join_scalar_subplans`:

```rust
for PulledScalar { plan: subplan, on, .. } in collected {
    plan = if on.is_empty() {
        build_auto_join_plan(plan, subplan, JoinKind::Cross, vec![])                 // 257, byte-identical
    } else {
        build_join_plan(plan, subplan, JoinKind::LeftOuter, on, /* HashPartitioned, Direct */ ...)?
    };
}
```

The uncorrelated path stays byte-equivalent to 257 (keyless `Cross`, scalar side on the right). The
COUNT wrap applies only to the **correlated** `LeftOuter` path: if the aggregate is
`AggregateFunction::CountAll | Count`, the rewritten scalar expression should be
`COALESCE(__scalar_N.value, 0)` (or the equivalent lowered `Expr::Case`) with an `Int64(0)` else-arm;
otherwise use the bare `__scalar_N.value`. Do not wrap uncorrelated COUNT, and do not wrap
MAX/MIN/SUM/AVG.

## Phase 6 — tests, guards, reconcile, verification, seal

### Behavioral gates (`tests/exists_decorrelation_test.rs` — fact/dim fixture, the correlation home)

`correlated_aggregate_scalar_decorrelates_to_left_outer` *(execute)*;
`correlated_scalar_recombines_as_keyed_left_outer` *(plan-shape)*;
`correlated_count_scalar_empty_group_is_zero` *(execute)*;
`correlated_aggregate_scalar_with_inner_residual` *(execute or plan-shape)*;
`uncorrelated_global_aggregate_scalar_unchanged` *(regression)*;
`aggregate_arg_and_non_aggregate_correlated_scalar_stay_rejected` *(reject)*.

### Architecture guards (`tests/architecture/subquery_predicates.rs` — extend the 256/258/259 module)

`adr260_correlated_aggregate_scalar_decorrelates_via_grouped_left_outer`,
`adr260_correlated_scalar_is_frontend_only`,
`adr260_aggregate_arg_and_non_aggregate_scalar_stay_deferred`.

### Reconcile (the load-bearing housekeeping)

- `adr259_non_equi_and_scalar_correlation_stay_deferred` (`subquery_predicates.rs:214`) — keep the
  string assertion (survives at `:9207`), retarget the comment + line-218 test pin to the aggregate-arg
  case.
- `exists_decorrelation_test.rs:324` — change its scalar case to `MIN(dim.score + fact.pk)`
  (aggregate-arg) so it keeps rejecting; the non-equi-IN case is unchanged.
- `group_by_test.rs:2688` — drop or convert the `AVG(amount) … WHERE sales.region = regions.region`
  case (it lowers now); keep the non-aggregate reject.
- Grep `adr*` + the test corpus for `"correlated scalar"`; note the non-equi-WHERE string change
  (`"correlated scalar subqueries are not supported"` → `"unsupported EXISTS subquery predicate"`).

### Verification gate (every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test exists_decorrelation_test
scripts/cargo-cgroup test  -p toron --test group_by_test
scripts/cargo-cgroup test  -p toron --test architecture_test adr260
scripts/cargo-cgroup test  -p toron --test architecture_test            # FULL (259 reconcile + new adr260 guards)
scripts/cargo-cgroup test  -p toron --test distributed_process_test     # includes the permanent 260 probe
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

### Seal

`impl: adr 260 correlated aggregate scalar subqueries`,
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes

- **Phase 4 is the only non-trivial code** — everything else (LeftOuter fold, COUNT COALESCE, the
  uncorrelated fallback) is reuse. Keep aggregate lowering on the existing path; no `Expr::Alias`.
- **`SELECT *` hidden-column gate** (impl-time): the aggregate side exposes its correlation keys
  (`__scalar_N.key_i`); non-`*` projections drop them via the final Project. Confirm `SELECT *`
  expansion excludes the synthetic `__scalar_N` qualifier (already latent for 257's WHERE-placed
  scalar — 260 doesn't worsen it); if it leaks, scope the keys as hidden.
- **COUNT scope**: only `CountAll`/`Count` get the COALESCE; MAX/MIN/SUM/AVG correctly stay NULL.
- **Outer-column scope** across the fold: the correlation `outer` exprs stay bound through the
  recombine (the EXISTS/membership chain precedent).
