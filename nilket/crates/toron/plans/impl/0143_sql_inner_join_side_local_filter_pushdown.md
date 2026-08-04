# Impl Plan — ADR 143: SQL Inner-Join Side-Local Filter Pushdown

**Migration for** `plans/adr/0143_sql_inner_join_side_local_filter_pushdown.md`.
**Scope:** SQL planner logical-shape rewrite only (`crates/toron/src/sql_planner.rs`).
**Non-scope:** binder/chooser/NDV-consumer change; executor/fragmenter/replay
change; Wendelon change; derived-table-in-join; outer/semi/anti pushdown;
mixed-chain pushdown; `JOIN ON` widening; any new IR variant.

## Pre-flight anchors (grounded against `main`)
| Concern | Anchor |
|---|---|
| Join chain built before WHERE | `sql_planner.rs:2782–2802` (`lower_select`) |
| Join operator → kind/on | `sql_planner.rs:2985` `lower_join_operator` |
| Conjunct splitter | `sql_planner.rs:5957` `split_top_level_and` |
| Side-scope classifier | `sql_planner.rs:6176` `collect_scope_refs` (+ `resolve_column_scope`) |
| Classifier flags | `sql_planner.rs:5952` `struct ScopeRefs { outer, unsupported }` → **+`inner`** |
| Per-side qualifiers | `sql_planner.rs:6283` `plan_qualifiers` (recurses through `Filter`) |
| Residual WHERE / EXISTS (unchanged) | `sql_planner.rs:3677` `lower_where`, `lower_exists_join` |
| Expr lowering / recombine | `lower_expr`, `combine_and_exprs` |
| Join builders | `build_auto_join_plan` (`:4241`), `build_join_plan` (`:4195`), `join_strategy_intent_for_hint` |
| Guard to retarget | `tests/architecture/optimizer.rs:1802` `adr141_no_sql_join_filter_pushdown_in_this_cut` |
| Binder win unlocked | `filter_equality_ndv_estimated_row_count` (`binder.rs:3773`), `derive_estimated_byte_size` (`:289`), chooser (`:1420`), left build candidate (`:4268`) |

## Phase 0 — failing gates first
Add behavior tests in `binder.rs` (141-style NDV fixtures for `customers.region`),
headline failing on `main`:
- `sql_equality_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate`.
- `sql_outer_join_chain_disables_filter_pushdown` (the D2 row-level counterexample).
- `sql_filter_pushdown_digest_matches_above_join_filter`.

## Phase 1 — `ScopeRefs` gains `inner`
`collect_scope_refs` no-ops on an inner ref (`:6189`). Add `inner: bool`, set on
`Ok(ColumnScope::Inner) => refs.inner = true`. Purely additive — the EXISTS caller
reads only `outer`/`unsupported`. The `inner` flag separates a right-local
conjunct from a qualifier-free constant (both have `!outer`).

## Phase 2 — `combine_and_sql_exprs`; keep `lower_where` untouched
```rust
fn combine_and_sql_exprs(conjuncts: &[&SqlExpr]) -> Option<SqlExpr>
//   mirror of combine_and_exprs, folding with SqlExpr::BinaryOp { op: BinaryOperator::And }.
```
Residuals recombine into one `SqlExpr` and flow through the **unchanged**
`lower_where` (EXISTS routing verbatim).

## Phase 3 — `lower_select` join section (replaces `:2782–2802`)
```rust
let mut plan = self.lower_table_factor(&table_with_joins.relation)?;

// One implementation shape: lower each join operator once → authoritative (kind, on).
let lowered_joins: Vec<(JoinKind, Vec<(Expr, Expr)>)> = table_with_joins.joins
    .iter().map(|j| self.lower_join_operator(&j.join_operator)).collect::<Result<_, _>>()?;

// D2: pushdown only when the whole chain is inner.
let all_inner = lowered_joins.iter().all(|(k, _)| *k == JoinKind::Inner);

let mut pending: Vec<&SqlExpr> = match (all_inner, selection) {
    (true, Some(sel)) => split_top_level_and(sel),
    _ => Vec::new(),
};

for (i, join) in table_with_joins.joins.iter().enumerate() {
    let mut right = self.lower_table_factor(&join.relation)?;
    let (kind, on) = lowered_joins[i].clone();

    if all_inner {
        let right_q = plan_qualifiers(&right);
        if i == 0 {                                   // leftmost base still lone here (D3)
            let left_q = plan_qualifiers(&plan);
            plan  = self.push_side_local(plan,  &mut pending, &left_q, &right_q, Side::Left)?;
        }
        let left_q = plan_qualifiers(&plan);
        right = self.push_side_local(right, &mut pending, &left_q, &right_q, Side::Right)?;
    }

    let strategy_intent = join_strategy_intent_for_hint(&mut broadcast_hint, &plan, &right, kind)?;
    plan = match strategy_intent {
        JoinStrategyIntent::Auto => build_auto_join_plan(plan, right, kind, on),
        JoinStrategyIntent::Pinned { strategy, source } =>
            build_join_plan(plan, right, kind, on, strategy, source)?,
    };
}
reject_unused_broadcast_hint(&broadcast_hint)?;

// Residuals above the chain (whole WHERE when !all_inner).
plan = match (all_inner, selection) {
    (true, _)          => match combine_and_sql_exprs(&pending) {
                              Some(sel) => self.lower_where(plan, &sel)?, None => plan },
    (false, Some(sel)) => self.lower_where(plan, sel)?,
    (false, None)      => plan,
};
```

`push_side_local(target, pending, left_q, right_q, side)`: partition `pending` — a
conjunct matches `side` iff `collect_scope_refs(c, left_q, right_q)` returns `Ok`
with, for `Side::Right`, `inner && !outer && !unsupported`; for `Side::Left`,
`outer && !inner && !unsupported`. Lower matches with `self.lower_expr`, combine
with `combine_and_exprs`, wrap `target` in `LogicalPlan::Filter` only if ≥1
matched, leave non-matches in `pending`. A lower failure surfaces the same error
today's `lower_where` would.

Grounded notes:
- **Hint resolution intact:** `plan`/`right` may be `Filter`-wrapped;
  `plan_qualifiers` recurses through `Filter` (`:6287`) so `/*+ BROADCAST(c) */`
  resolves; the pin sits above the pushed `Filter`.
- **Multi-dim ordering:** `regions.name` at `i==0` (regions not in scope) →
  `unsupported` for that step → stays pending → pushes at its own step.
- **EXISTS:** classifies `unsupported` (`:6193`) → never pushed → flows to
  `lower_where` → `lower_exists_join`, verbatim.
- **`lower_join_operator` lowered once**, reused — no double validation.
  If preserving today's exact right-table-factor-before-`ON` error precedence turns
  out to matter, use a guarded kind-only pre-scan for `all_inner` and keep
  authoritative `lower_join_operator` lowering inside the existing loop.

## Phase 4 — guards
Retarget `adr141_no_sql_join_filter_pushdown_in_this_cut` → the six `adr143_*`
guards (ADR §"Architecture guards"): planner-local, all-inner-chain only,
tightest-single-input, residual-preserving, no binder/chooser/NDV change, no
derived-table-join widening.

## Phase 5 — verification
```sh
cargo test -p toron
cargo test -p toron --test architecture_test adr143   # file stem, NOT `architecture`
cargo test -p toron --test architecture_test adr141   # confirm retarget green
cargo check -p toron --all-targets                    # ScopeRefs field add
git diff --check
```
([[feedback_toron_architecture_test_target]], [[feedback_verify_all_targets_on_new_ir_field]].)
129–142 regressions expected green (no chooser/fact change).

## Risk register
- **`ScopeRefs` field add** — additive, `default()`-constructed; `--all-targets` confirms.
- **`combine_and_sql_exprs` synthetic AST** — folds already-parsed conjuncts with
  `And` only; digest gate covers it; keeps `lower_where`/EXISTS untouched.
- **Fence correctness** — the row-level `sql_outer_join_chain_disables_filter_pushdown`
  test is the live guard; all-inner gating is a single `.all(== Inner)`.
- **Predicate drop/dup** — pending-list partition removes pushed, recombines the
  rest; digest gate + EXISTS tests cover it.
