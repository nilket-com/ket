# Impl Plan — ADR 108: SQL `CASE`

**Migration for** `plans/adr/108_sql_case.md`. **Scope:** `ir.rs`, `sql_planner.rs`, `ir/execution.rs`,
`binder.rs`, `exec.rs`, `pbc.rs` + behavior tests + guard retargets. **Non-scope:** no `LogicalPlan`/
`ExecutionPlan` variant, no Wendelon/storage/coordinator surface, no PBC opcode, no per-row lazy eval, no
sugar family (`IF`/`COALESCE`/`NULLIF` → ADR 109). The first conditional-expression substrate.

## Phase 0 — Inventory (the two-sided ripple)

```sh
rg -n "SqlExpr::Case|CaseWhen" crates/toron/src/sql_planner.rs
rg -n "Expr::Like|Expr::TimeBucket" crates/toron/src                     # logical-side visitor sweep
rg -n "BoundExprNode::Like|BoundExprNode::TimeBucket" crates/toron/src   # bound-side walker sweep
rg -n "adr074_similar_rlike_case|adr031_bound_expr_node|unsupported_pattern_predicates" crates/toron/tests
```

## Phase 1 — Logical IR (`ir.rs`)

`Expr::Case { when_then: Vec<(Box<Expr>, Box<Expr>)>, else_expr: Option<Box<Expr>> }`. Add `Self::Case { .. }
=> false` to `is_pbc_pushdown_eligible` (residual group, beside `Like`). `is_deterministic` stays `true`.

## Phase 2 — Planner (`sql_planner.rs`): `lower_case` + logical-visitor recursion

Replace the `SqlExpr::Case` rejection with `lower_case(operand, conditions, else_result)`: reject empty
`conditions`; per branch `cond = match operand { Some(op) => Expr::Eq(lower(op), lower(c.condition)), None =>
lower(c.condition) }`, `then = lower(c.result)`; `else_expr = else_result.map(lower)`.

**Logical-side visitor recursion (D3 — do not skip):** add a `Case` arm to the planner `SqlExpr`/`Expr`
visitors — `reject_nested_aggregate`, aggregate/HAVING reject helpers, subquery/window detection,
`expr_contains_qualified_column`, hidden-sort-key validation, non-aggregate column-ref detection, and
`hash_expr_structural`/`expr_structural_eq` (new stable tag after `SqlLogicalCast`; hash/compare branch count
+ ordered `(cond,result)` pairs + else). `grep Expr::Like` enumerates the sites.

## Phase 3 — Bound IR (`ir/execution.rs`)

`BoundExprNode::Case { when_then: Vec<(BoundExprId, BoundExprId)>, else_input: BoundExprId }` +
`BoundExprProgramBuilder::append_case`. `Case` arm in every match (`grep BoundExprNode::Like`):
`validate`/`node_children` (children = each cond, each then, then `else_input`), `node_nullable` (any then
nullable OR else_input nullable), `node_logical_type` (NAKED — leave the `append_node` default, don't
override), clone/remap, structural-eq, pretty/display.

## Phase 4 — Binder (`binder.rs`): `bind_case`

1. Reject empty `when_then`. 2. Bind each cond → `require_boolean(builder, "CASE WHEN", id)`. 3. Bind each
result + explicit else. 4. Common type:

```rust
fn case_common_type(types: &[DataType]) -> Result<DataType, BindingError> {
    let mut iter = types.iter();
    let mut common = iter
        .next()
        .cloned()
        .ok_or_else(|| BindingError::TypeMismatch {
            message: "CASE requires at least one result branch".to_string(),
        })?;
    for t in iter {
        if common == *t { continue; }
        match arithmetic_output_type(&common, t) {
            Some(out) => common = out,
            None => return Err(BindingError::TypeMismatch {
                message: format!("CASE result branches cannot be unified: {common:?} and {t:?}"),
            }),
        }
    }
    Ok(common)
}
```

5. `cast_if_needed` each branch to `common`. 6. No else → append typed-NULL `Literal` of `common`
(`ScalarValue::{Boolean,Int32,Int64,Float32,Float64,Utf8}(None)`; other → reject). 7. `append_case`.

Binder walker sites (Codex's list): `collect_logical_cast_names`,
`reject_storage_synthetic_returning_reference`, `clone_bound_subgraph`,
`bound_subgraphs_structurally_equal_inner`, `bound_node_children`, `offset_bound_node`.

## Phase 5 — Executor (`exec.rs`): zip eval + the required doc-comment

```rust
/// SQL CASE is nominally lazy; this evaluates **every** branch array eagerly and `zip`s by the WHEN mask
/// (branchless / vectorized). A hard-erroring kernel (integer overflow, strict CAST) in a branch fails the
/// query even for rows that branch isn't selected for. Integer div-by-zero is safe (ADR 084 → NULL).
/// Per-branch masked (lazy) execution is a deferred follow-on; do not treat CASE as a lazy error fence.
fn evaluate_case(when_then: &[(ArrayRef, ArrayRef)], else_array: ArrayRef) -> Result<ArrayRef, ExecutionError> {
    let mut acc = else_array;
    for (cond, then) in when_then.iter().rev() {           // reverse → first WHEN wins
        let mask = as_boolean(cond, "CASE WHEN condition")?;
        acc = arrow::compute::kernels::zip::zip(mask, then, &acc)?;
    }
    Ok(acc)
}
```

Add the `Case` arm to `evaluate_bound_program_without_storage` **and** the unbound `evaluate_expr`. Confirm
`zip` takes falsy on false-or-NULL mask (SQL `WHEN NULL` no-match).

## Phase 6 — PBC decline (`pbc.rs`): both sites

`compile_node` → `ExpressionShapeUnsupported("CASE is not PBC-eligible")`; `emit_node` → unsupported-node
arm. No opcode.

## Phase 7 — Tests + guard retargets

Behavior gates (per ADR §Behavior gates; `predicate_widening_test.rs` style with `plan_sql`/`run_logical`),
including the two eager-eval pins:
- `case_divide_guard_is_safe_via_null` — `CASE WHEN x<>0 THEN 100/x ELSE 0` over a row with `x=0` executes
  (x=0 → ELSE; proves div0→NULL safety).
- `case_branches_eval_eagerly_masked_hard_error_fails` — a CASE whose masked branch hard-errors (integer
  overflow or strict `CAST`) **fails the query**, pinning the documented eager contract.

Retarget: `unsupported_pattern_predicates_and_case_subquery_in_remain_unsupported` (drop only the CASE
fixture); `adr074_similar_rlike_case_subquery_in_remain_unsupported` (drop `SqlExpr::Case`);
`adr031_bound_expr_node_has_no_boxes` (9→10 + assert `Case`); add `adr108_case_is_residual_bound_expression`
(pins `Expr::Case`, `BoundExprNode::Case`, `else_input: BoundExprId`, `require_boolean`,
`arithmetic_output_type` unification, typed-NULL synthesis, `node_nullable`/walker arms, `zip::zip` in exec,
`pbc.rs` decline + no opcode, no Wendelon/storage `Case` refs).

## Phase 8 — Verification

```sh
cargo fmt
cargo test -p toron case_
cargo test -p toron --test architecture_test adr108
cargo test -p toron --test architecture_test adr074
cargo test -p toron --test architecture_test adr031
cargo test -p toron
git diff --check
```

## Acceptance

Searched + simple CASE bind/execute anywhere `lower_expr` runs; `arithmetic_output_type` unification (mismatch
rejects at bind); missing ELSE → typed NULL; NULL condition → no-match; first-true-wins; residual (no PBC),
naked result; **both** the logical-side `SqlExpr`/`Expr` visitor ripple and the bound-side `BoundExprNode`
walker ripple complete; eager-eval documented (ADR + `evaluate_case` doc-comment) and pinned (the two gates);
074 / pattern-predicates / 031 guards retargeted + `adr108_*` added; Toron suite green.
