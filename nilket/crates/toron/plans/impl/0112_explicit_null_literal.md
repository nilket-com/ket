# Impl Plan — ADR 112: Explicit `NULL` Literal

**Migration for** `plans/adr/0112_explicit_null_literal.md`. **Scope:** `ir.rs` (`Expr::UntypedNull` transient
leaf) + `sql_planner.rs` (lift `:3690`; retire both placeholders; lift IN-member reject; leaf walker arms) +
`binder.rs` (reject backstop; one `typed_null_for_data_type`; resolve `UntypedNull` at CAST physical+logical /
CASE / UPDATE / Boolean/null-test / comparison / arithmetic / IN; delete the bound sentinel) + `rune_engine.rs`
(`lit_null`) + tests + guards. **Non-scope:** no `BoundExprNode`/executor-kernel/PBC/Wendelon/storage substrate
change, except residual `evaluate_in_list` adopting Kleene OR for IN-with-NULL 3VL correctness; no contextless
`SELECT NULL` (needs `DataType::Null`/`NullArray`); no `NVL2`. `UntypedNull` is binder-consumed (the
`SqlLogicalCast` precedent) → the bound arena is untouched.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Typeless marker | `Expr` enum (`ir.rs:496`); transient precedent `SqlLogicalCast` (`:503`) |
| Lift general reject | `sql_planner.rs:3690` |
| Sentinel producers (delete) | `update_null_placeholder` (`:797`), `conditional_null_placeholder` (`:5467`) |
| IN-member reject (lift) / LIKE (keep) | `:4012` / `:3981` (`expr_is_null_literal`) |
| Reject backstop | `bind_expr_into_program` (`:4496`) exhaustive `Expr` match |
| Sibling seam | `bind_comparison` (`:5119`, mismatch `:5138`), `bind_arithmetic` (`:4717`, `:4743`), `bind_in_list` |
| Physical CAST seam | `bind_explicit_cast` (`:4919`) + try-cast path |
| Logical CAST seam (D11) | `SqlLogicalCast` bind + 092/093 async spec resolution; `reconcile_identity` (087/088) |
| CASE loop / typer (delete sentinel) | `bind_case` (`:4748`); `is_case_null_placeholder` (`:4818`), `typed_null_for_case` (`:4905`), `typed_update_null_for_target` (`:1074`) |
| PBC residual (pin) | `emit_literal` catch-all → `NullLiteralUnrepresentable` (`pbc.rs:231`) |
| Rune free-fn | `create_toron_module` (`rune_engine.rs:602`) |
| INSERT NULL (untouched) | `:908` `InsertLiteral::Null` |

## Phase 1 — `Expr::UntypedNull` (`ir.rs`)

Add `UntypedNull,` to `Expr` (leaf). `is_pbc_pushdown_eligible => false` (conservative; never survives binding).

## Phase 2 — Planner (`sql_planner.rs`)

- `:3690` `Value::Null => Ok(Expr::UntypedNull)`.
- **Delete** `update_null_placeholder` / `conditional_null_placeholder`; UPDATE-assign + sugar desugars emit
  `Expr::UntypedNull` (sugar NULL args route through normal `lower_expr`; `NULLIF`'s synthetic THEN-NULL emits
  `Expr::UntypedNull` directly).
- Lift the IN-member reject (`:4012`) so `IN (NULL, …)` lowers (members carry `UntypedNull`); **keep** the
  LIKE-pattern NULL reject (`:3981`).
- Add the trivial leaf `Expr::UntypedNull` arm to the logical-`Expr` visitors 108 touched (mirror `Literal`).

## Phase 3 — Binder (`binder.rs`)

**(a) Typer + helpers (D3, D2).** `typed_null_for_data_type(&DataType) -> Result<ScalarValue, BindingError>` (six
arms; supersedes `typed_null_for_case` + the inline `typed_update_null_for_target` match). Add
`is_untyped_null(&Expr)` and `resolve_untyped_null(builder, &DataType) -> Result<BoundExprId, BindingError>` (append
`typed_null_for_data_type`).

**(b) Reject backstop (D2).** `bind_expr_into_program`:
```rust
Expr::UntypedNull => Err(BindingError::UnsupportedExpression {
    message: "NULL literal has no type in this position; wrap it in CAST(NULL AS <type>)".into(),
}),
```

**(c) Physical CAST (D4).** `bind_explicit_cast`/try-cast: if `is_untyped_null(inner)` →
`resolve_untyped_null(target)` (no inner bind, no kernel).

**(d) Logical CAST (D11).** In the `SqlLogicalCast` bind path, if `is_untyped_null(inner)`: take the resolved spec's
physical (092/093 async resolution already in hand), `resolve_untyped_null(physical)`, then stamp identity via the
existing `reconcile_identity` + `set_node_logical_type` (operand identity is naked → `reconcile(naked, T) = add`).
`try_cast` → nullable parity. No new resolution machinery — reuse the 092/093 spec + reconcile.

**(e) CASE / Boolean / null tests (D4, D6, D9).** In `bind_case`, detect `UntypedNull` THEN/ELSE at the Expr level:
skip binding, mark slot; compute common-type/identity over non-null bound results (logic unchanged, Expr-level
filter replaces `is_case_null_placeholder`); fill slots via `resolve_untyped_null(output_type)`. Add a
boolean-context binder helper: `UntypedNull` in CASE/IF conditions and `AND`/`OR`/`NOT` operands resolves to
`Boolean(None)`, then normal `require_boolean` applies to other expressions. `IS NULL` / `IS NOT NULL` over
`UntypedNull` should also resolve the input to a typed NULL (Boolean is sufficient) before appending the null-test
unary node; this keeps `COALESCE(NULL, amount)` green after the sentinel is deleted. **Delete**
`is_case_null_placeholder`.

**(f) Comparison / arithmetic / IN (D5, D7).** `bind_comparison`/`bind_arithmetic`: if one operand
`is_untyped_null`, `resolve_untyped_null(peer_type)` before the `:5138` mismatch / `cast_if_needed`; both null →
reject. `bind_in_list`: resolve `UntypedNull` members to the target type; if the target is `UntypedNull`, infer one
compatible type from the non-null members and materialize the target from that type; all-null target/list rejects.
Negated keeps native 3VL (`NOT IN (NULL,…)` → empty). In the executor, switch residual `evaluate_in_list` from
strict `or` to `or_kleene`; otherwise `x IN (NULL, v)` can incorrectly return `UNKNOWN` even when `x = v` is TRUE.
This is a residual evaluator correctness fix, not a new executor kernel or NULL carrier.

**(g) UPDATE (D4).** `typed_update_null_for_target` matches `Expr::UntypedNull` → `Expr::Literal(
typed_null_for_data_type(field.data_type())?)`.

## Phase 4 — Rune `lit_null` (`rune_engine.rs`, D10)

```rust
pub fn lit_null() -> RuneExpr { RuneExpr { expr: Expr::UntypedNull } }
```
Register `m.function(["lit_null"], lit_null).build()?;` beside `lit`/`col`. No Rune-side checks.

## Phase 5 — Tests + guards

All ADR §Behavior gates (incl. `cast_null_as_logical_type_carries_identity`, `not_in_null_member_returns_zero_rows`,
`null_is_null_and_is_not_null_bind_without_sentinel`, `comparison_with_null_is_3vl_residual`,
`rune_lit_null_in_case_and_comparison`, sugar/UPDATE regressions). Guards:
`adr112_explicit_null_is_transient_binder_consumed`, `adr112_null_sentinel_retired`,
`adr112_untyped_null_rejects_without_context`, `adr112_null_predicates_stay_residual`; reconcile 109/110 guards +
the Rune surface-name guard (admit `lit_null`).

## Phase 6 — Verification

```sh
cargo fmt
cargo test -p toron --test architecture_test adr112
cargo test -p toron --test architecture_test adr108 && cargo test -p toron --test architecture_test adr109 && cargo test -p toron --test architecture_test adr110 && cargo test -p toron --test architecture_test adr111 && cargo test -p toron --test architecture_test adr092
cargo test -p toron
git diff --check
```

## Acceptance

Explicit NULL lowers as `Expr::UntypedNull`, materializes in every typed context (CAST physical+logical /
assignment / CASE / Boolean/null-test / comparison / arithmetic / IN); contextless NULL rejects pointing at `CAST(NULL AS
T)`; the `Utf8(None)` sentinel is gone (one marker + one `typed_null_for_data_type`); sugars + `SET col = NULL`
regress; `NOT IN (NULL) → empty` + 3VL comparisons pinned residual; 110 identity + all-NULL reject hold; `CAST(NULL
AS CustomerId)` carries identity; Rune `lit_null()` binds by the same rules; `adr112_*` guards green; **no**
bound-arena/executor-kernel/PBC/Wendelon/storage substrate change, except `evaluate_in_list` using Kleene OR for
3VL IN-with-NULL; Toron suite green.
