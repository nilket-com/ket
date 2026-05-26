# Impl Plan — ADR 109: SQL conditional-sugar family

**Migration for** `plans/adr/109_conditional_sugar_family.md`. **Scope:** `sql_planner.rs` (six name-matched
desugars onto `Expr::Case` + explicit-NULL-arg → placeholder) + **one `bind_case` hook** in `binder.rs` +
tests + two guards. **Non-scope:** no `Expr`/`BoundExprNode` variant, executor kernel, PBC opcode,
Wendelon/storage, general explicit `NULL`, `NVL2`, procedural `IF`.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Function dispatch | `SqlExpr::Function` arm of `lower_expr` (`sql_planner.rs:3780`, TIME_BUCKET precedent); args via `FunctionArguments::List` |
| Explicit-NULL rejection | `Value::Null => Err("explicit NULL literal not supported")` (stays for bare NULL) |
| Untyped-NULL placeholder convention | `Expr::Literal(ScalarValue::Utf8(None))` + `typed_update_null_for_target` (`binder.rs:1074`) |
| CASE binder (reuse + hook) | `bind_case`, `case_common_type`, `typed_null_for_case`, `cast_if_needed` (`binder.rs`) |

## Phase 1 — Planner desugars (`sql_planner.rs`, `SqlExpr::Function` arm near `:3780`)

Case-insensitive name-matched arms beside `TIME_BUCKET`: `COALESCE`→`lower_coalesce`, `IF`/`IIF`→`lower_if`,
`NULLIF`→`lower_nullif`, `IFNULL`/`NVL`→`lower_ifnull_nvl`. `NVL2` **not** matched. A small value/result
arg-lowering helper maps a literal-`NULL` arg to the placeholder (D6) instead of rejecting it:

```rust
fn lower_conditional_value_arg(&self, e: &SqlExpr) -> Result<Expr, FrontendError> {
    if expr_is_null_literal(e) {
        return Ok(Expr::Literal(ScalarValue::Utf8(None)));   // untyped-NULL placeholder (sugar-arg only)
    }
    self.lower_expr(e)
}

fn function_positional_args<'a>(f: &'a Function, name: &str) -> Result<Vec<&'a SqlExpr>, FrontendError> {
    if f.uses_odbc_syntax
        || !matches!(f.parameters, FunctionArguments::None)
        || f.filter.is_some()
        || f.null_treatment.is_some()
        || f.over.is_some()
        || !f.within_group.is_empty()
    {
        return Err(unsupported(format!("{name} does not support function modifiers")));
    }
    let FunctionArguments::List(list) = &f.args else { return Err(unsupported(format!("{name} requires positional arguments"))); };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return Err(unsupported(format!("{name} does not support argument clauses")));
    }
    list.args.iter().map(|a| match a {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
        _ => Err(unsupported(format!("{name} supports only plain positional arguments"))),
    }).collect()
}

fn lower_if(&self, f: &Function) -> Result<Expr, FrontendError> {
    let [c, t, e] = function_positional_args(f, "IF/IIF")?.as_slice() else {
        return Err(unsupported("IF/IIF requires exactly three arguments"));
    };
    Ok(Expr::Case {
        when_then: vec![(Box::new(self.lower_expr(c)?), Box::new(self.lower_conditional_value_arg(t)?))],
        else_expr: Some(Box::new(self.lower_conditional_value_arg(e)?)),
    })
}

fn lower_coalesce(&self, f: &Function) -> Result<Expr, FrontendError> {
    let args = function_positional_args(f, "COALESCE")?;
    let (last, init) = args.split_last().ok_or_else(|| unsupported("COALESCE requires at least one argument"))?;
    if init.is_empty() {
        if expr_is_null_literal(last) {
            return Err(unsupported("COALESCE requires at least one typed argument"));
        }
        return self.lower_expr(last);                                      // COALESCE(a) ≡ a
    }
    let when_then = init.iter().map(|a| {
        let lowered = self.lower_conditional_value_arg(a)?;
        Ok((Box::new(Expr::IsNotNull(Box::new(lowered.clone()))), Box::new(lowered)))
    }).collect::<Result<Vec<_>, FrontendError>>()?;
    Ok(Expr::Case { when_then, else_expr: Some(Box::new(self.lower_conditional_value_arg(last)?)) })
}

fn lower_ifnull_nvl(&self, f: &Function) -> Result<Expr, FrontendError> {
    let [a, b] = function_positional_args(f, "IFNULL/NVL")?.as_slice() else {
        return Err(unsupported("IFNULL/NVL requires exactly two arguments"));
    };
    let a = self.lower_conditional_value_arg(a)?;                      // = 2-arg COALESCE
    Ok(Expr::Case {
        when_then: vec![(Box::new(Expr::IsNotNull(Box::new(a.clone()))), Box::new(a))],
        else_expr: Some(Box::new(self.lower_conditional_value_arg(b)?)),
    })
}

fn lower_nullif(&self, f: &Function) -> Result<Expr, FrontendError> {
    let [a, b] = function_positional_args(f, "NULLIF")?.as_slice() else {
        return Err(unsupported("NULLIF requires exactly two arguments"));
    };
    let a = self.lower_expr(a)?;
    Ok(Expr::Case {
        when_then: vec![(
            Box::new(Expr::Eq(Box::new(a.clone()), Box::new(self.lower_expr(b)?))),
            Box::new(Expr::Literal(ScalarValue::Utf8(None))),         // NULLIF's THEN NULL
        )],
        else_expr: Some(Box::new(a)),
    })
}
```

(Confirm sqlparser 0.62 `Value`/`FunctionArg`/`FunctionArgExpr` shapes at impl.)

## Phase 2 — The `bind_case` placeholder hook (`binder.rs`, the only engine change)

Extend 108's `bind_case`: a result branch binding to literal `Utf8(None)` is the placeholder.

```rust
fn is_null_placeholder(builder: &BoundExprProgramBuilder, id: BoundExprId) -> bool {
    matches!(builder.literal(id), Some(ScalarValue::Utf8(None)))
}
```

In `bind_case`, after binding result ids (then-results + optional else), before unifying:
1. `typed` = result types of non-placeholder branches; if empty → reject (`"conditional has no typed
   (non-NULL) result branch"` — covers all-NULL `COALESCE`).
2. `output_type = case_common_type(&typed)`.
3. Materialize each branch result: placeholder → `builder.append_literal(typed_null_for_case(&output_type)?)`;
   else → `cast_if_needed(builder, id, &output_type)`. Apply to each `(_, result)` in `bound_when_then` and
   the bound else.

Doc-comment the safety invariant (literal `Utf8(None)` in a CASE result is the planner sentinel only while
normal `lower_expr` rejects explicit `NULL`; revisit if general NULL literals land). Shared `bind_case`;
conditional sugar creates the sentinel only for CASE result/value positions.

## Phase 3 — Behavior tests (`tests/sql_case_test.rs` or new `sql_conditional_sugar_test.rs`)

The ADR §Behavior gates, via the 108 `plan_sql`/`run_logical` harness (all execute through 108 CASE) —
including `coalesce_explicit_null_argument_uses_typed_fallback`, `if_null_result_branch_uses_typed_case_null`,
`if_explicit_null_condition_remains_unsupported`, `nullif_explicit_null_operand_remains_unsupported`,
`ifnull_and_nvl_match_two_arg_coalesce`, `ifnull_nvl_arity_not_two_rejects`, `nvl2_remains_unsupported`,
`bare_select_null_remains_unsupported`, and `conditional_sugar_inherits_case_eager_hard_error`.

## Phase 4 — Architecture guards (`tests/architecture/sql_planner_surface.rs`)

- `adr109_conditional_sugar_desugars_to_case` — `lower_coalesce`/`lower_if`/`lower_nullif`/`lower_ifnull_nvl`
  + `lower_conditional_value_arg` exist; the `SqlExpr::Function` arm dispatches `COALESCE`/`IF`/`IIF`/`NULLIF`/
  `IFNULL`/`NVL`; builds `Expr::Case` (+`IsNotNull`/`Eq`); `NVL2` and `Statement::If` **not** matched; **no**
  `Expr::Coalesce`/`NullIf`, `BoundExprNode` variant, executor kernel, or PBC opcode.
- `adr109_case_binder_types_untyped_null_sentinel` — `bind_case` detects literal `Utf8(None)`, excludes from
  `case_common_type`, rejects all-placeholder, replaces via `typed_null_for_case`.

## Phase 5 — Verification

```sh
cargo fmt
cargo test -p toron coalesce_ && cargo test -p toron nullif_ && cargo test -p toron if_ && cargo test -p toron iif_ && cargo test -p toron ifnull_ && cargo test -p toron nvl
cargo test -p toron --test architecture_test adr109 && cargo test -p toron --test architecture_test adr108
cargo test -p toron
git diff --check
```

## Acceptance

All six conditionals bind + execute through `Expr::Case`; arity/shape rejects clean (`IFNULL`/`NVL` strict-2);
`NULLIF` + explicit-NULL value/result sugar args get typed NULLs via the shared `bind_case` hook; all-placeholder rejects;
bare `SELECT NULL`, `NVL2`, procedural `IF` stay rejected; eager-error inherited + pinned; **no new
IR/bound/executor/PBC/Wendelon surface**; two 109 guards added, 108 retained; Toron suite green.
