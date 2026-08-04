# Impl Plan — ADR 114: Contextless `NULL` Projection Substrate

**Migration for** `plans/adr/0114_contextless_null_projection.md`. **Representation: B —
`BoundExprNode::NullLiteral`** (result-only bound artifact; `ScalarValue` stays typed-only). **Scope:**
`ir/execution.rs` (bound null node + `append_null()`), `binder.rs` (result-position null policy + all-null CASE +
the result-null helper), `exec.rs` (bound `NullArray`), `sql_planner.rs` (`NVL2` desugar + lift one-arg
`COALESCE(NULL)` reject), tests + guards. **Non-scope:** no `ScalarValue::Null`, no executable `Expr::UntypedNull`,
no PBC opcode / Wendelon / storage persistence, no lazy CASE, no Rune `nvl2`.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Untyped marker (kept) | `Expr::UntypedNull` (`ir.rs`); `Value::Null => Ok(Expr::UntypedNull)` (`sql_planner.rs`) |
| Bound arena (add leaf) | `BoundExprNode`, `BoundExprProgramBuilder`, `node_nullable`/`node_children`/`pretty_node` (`ir/execution.rs`) |
| Generic reject (kept) | `bind_expr_into_program` `Expr::UntypedNull => Err` (`binder.rs`) |
| Result-projection sites (shared path) | `Project` (`binder.rs:151`, covers top-level and derived SELECT outputs), `RETURNING` (`:850`); Rune `.project`/`.project_named` → `LogicalPlan::Project` (`rune_engine.rs:224`/`:240`) |
| Non-result callers (untouched) | predicate `:132`/`:802`, GROUP BY `:167`, sort `:191`, UPDATE source projection `:666`, aggregate args `:4307` |
| All-null CASE reject (→ Null) | `bind_case` `result_types.is_empty()` (`binder.rs:4862`) |
| Typed-null helper (kept narrow) | `typed_null_for_data_type` / `resolve_untyped_null` (`binder.rs`) |
| Bound execution | `evaluate_bound_program_without_storage`; `new_null_array` already at `exec.rs:1018` |
| CTAS boundary | `is_supported_ctas_column_type` / `infer_ctas_schema` (`binder.rs`) |
| Conditional sugar | `ConditionalSugarFunction`, `conditional_sugar_function_name`, `lower_coalesce`/`lower_nullif` (`sql_planner.rs`) |
| 112 guard (holds) | `adr112_explicit_null_is_transient_binder_consumed` (`!scalar_enum.contains("Null")`) |

## Phase 1 — Bound null node (`ir/execution.rs`)

Add `NullLiteral` (leaf) to `BoundExprNode`. `append_null(&mut self) -> BoundExprId` →
`append_node(BoundExprNode::NullLiteral, DataType::Null)` (binder-only). Thread the leaf arm: `node_nullable →
true`, `node_children → []`, `pretty_node → "NULL"`, `node_logical_type → NAKED`, no literal-slot validation; PBC
eligibility/compile treat it as unrepresentable (decline, no opcode). **Do not** add `ScalarValue::Null`.

## Phase 2 — Result-position null policy (`binder.rs`)

Keep `bind_expr_into_program`'s `Expr::UntypedNull => Err` unchanged. Add an opt-in:
```rust
enum NullRootPolicy { Reject, MaterializeResultNull }

fn bind_expr_program_with_null_policy(exprs, input_schema, ctx, policy) -> Result<BoundExprProgram> {
    // per root: if MaterializeResultNull && is_untyped_null(expr) { builder.append_null() }
    //           else bind_expr_into_program(...); push_root
}
```
Existing `bind_expr_program` = `Reject`. Add `bind_projection_expr_program` (= `MaterializeResultNull`) and use it
at the result-projection sites only: SELECT `Project` (`:151`, which also covers derived SELECT outputs) and
`RETURNING` (`:850`). **Rune projection inherits** (it lowers to `LogicalPlan::Project`). Do **not** touch
predicate/GROUP BY/sort/aggregate-arg/assignment binding or the UPDATE source projection (`:666`) — they stay
`Reject` or explicitly target-typed (112).

## Phase 3 — All-null CASE + result-null helper (`binder.rs`)

```rust
fn materialize_null_for_output_type(builder, output_type) -> Result<BoundExprId> {
    if output_type == &DataType::Null { Ok(builder.append_null()) }
    else { resolve_untyped_null(builder, output_type) }
}
```
`type_case_result` uses it for `CaseResultInput::UntypedNull`. At `binder.rs:4862`:
```rust
let (output_type, output_logical_type) = if result_types.is_empty() {
    (DataType::Null, LogicalTypeId::NAKED)
} else {
    let t = case_common_type(&result_types)?;
    (t.clone(), case_output_logical_type(builder, &typed_result_ids, &t)?)
};
```
Mixed typed/null CASE is unchanged (`result_types` non-empty → null arms materialize at the typed output).
`DataType::Null` is **not** added to `arithmetic_output_type`/comparison coercion → `COALESCE(NULL,NULL) + 1`
rejects (D9); `NULLIF(NULL, amount)` → Null (comparison typed by `amount`), `NULLIF(NULL,NULL)` rejects.

## Phase 4 — Bound executor (`exec.rs`)

`evaluate_bound_program_without_storage`: `BoundExprNode::NullLiteral => new_null_array(&DataType::Null,
batch.num_rows())` (one per row, incl. zero rows). No `literal_array` change (`ScalarValue` untouched). Keep the raw
`Expr::UntypedNull` evaluator error as a defensive backstop.

## Phase 5 — `NVL2` + one-arg `COALESCE(NULL)` (`sql_planner.rs`)

Add `Nvl2` to `ConditionalSugarFunction` + `"NVL2"` classifier; `lower_nvl2` (3 positional args) → `Expr::Case {
when_then: [(IsNotNull(a), b)], else_expr: Some(c) }`. Lift the one-arg `COALESCE(NULL)` planner reject so it lowers
to `Expr::UntypedNull` (projection binding handles it); keep arity/positional checks.

## Phase 6 — CTAS boundary (`binder.rs`)

Leave `is_supported_ctas_column_type` excluding `DataType::Null`. Pin both: `CREATE TABLE … AS SELECT NULL AS n` →
existing unsupported-output-type error (cast guidance); `CREATE TABLE … AS SELECT CAST(NULL AS BIGINT) AS n` →
nullable `Int64` persists (no special handling — source column is already `Int64`).

## Phase 7 — Tests + guards

Behavior (ADR §gates): `bare_select_null_projects_null_array`, `select_null_respects_empty_input_row_count`,
`sugar_family_all_null_projects_null`, `case_all_null_branches_project_null`,
`nullif_first_explicit_null_operand_projects_null`, `nullif_two_contextless_nulls_still_rejects`,
`null_result_is_not_a_coercion_participant`, `mixed_typed_null_behavior_unchanged`, `nvl2_desugars_and_executes`,
`nvl2_all_null_results_project_null`, `nvl2_arity_rejects`, `nvl2_inherits_case_identity_and_aggregate_rejection`,
`update_returning_contextless_null_projects_null_column`, `rune_lit_null_projection_materializes_null_column`,
`ctas_select_null_rejects`, `ctas_cast_null_to_concrete_type_succeeds`, `untyped_null_still_non_executable`. Flip 112
negatives to positive result-position coverage. Schema assertions: `data_type() == DataType::Null`, nullable, NAKED,
`len()` == input rows.

Guards: `adr114_bound_null_projection_substrate`, `adr114_untyped_null_stays_binder_only`,
`adr114_projection_policy_is_the_only_raw_untyped_null_escape`, `adr114_bound_executor_materializes_null_array`,
`adr114_all_null_case_resolves_to_null_type`; retarget 109 (+`NVL2`/`lower_nvl2`, no `OP_NVL2`) + 112 contextless
negatives (→ positive, keeping the generic-reject + PBC-residual + transient-marker assertions). 112's
`!scalar_enum.contains("Null")` **holds unchanged**.

## Phase 8 — Verification

```sh
cargo fmt
cargo test -p toron --test architecture_test adr114
cargo test -p toron --test architecture_test adr112 && cargo test -p toron --test architecture_test adr109
cargo test -p toron --test sql_case_test && cargo test -p toron --test update_test
cargo test -p toron --test ctas_test && cargo test -p toron --test rune_relation_handles_test
cargo test -p toron
git diff --check
```

## Acceptance

`SELECT NULL` / all-null CASE/COALESCE/IFNULL/NVL/NVL2 / `RETURNING NULL` / Rune `lit_null()` projection project a
naked `DataType::Null` row-count-correct `NullArray`; 112 typed-null + 110 identity unchanged; `DataType::Null`
never coerces; `NVL2` is CASE sugar only; generic `Expr::UntypedNull` still rejects outside result opt-in;
`ScalarValue`/PBC/Wendelon/storage free of a null carrier; CTAS null-typed output rejects, `CAST(NULL AS T)`
persists; `adr114_*` + reconciled 109/112 guards hold; Toron suite green.
