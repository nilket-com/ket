# ADR 082: Rune Typed Literals

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)**: the final
WIP drafts ratify the narrow typed-literals cut and the two implementation groundings
(no `Float64->Int32` coercion arm, so `lit_f64` needs same-type fixtures; `lit_i64`
is included). Minor guard/fixture-granularity divergences resolve to the lean set:
5 guards, 4 behavior fixtures, and 1 plan-only wide-integer fixture.
**Category:** Toron frontend composition / Rune expression authoring over shared IR.
**Scale boundary:** Toron-only; production change localized to `rune_engine.rs`
(4 free functions + registration). Lowers to existing `Expr::Literal` + `ScalarValue`.
No IR/binder/executor/SQL/wire/worker/dependency/`syn`/sandbox change; purely additive.

## Summary

Rune's literal surface is `lit(i32)` only, blocking same-type filters like
`col("status").eq(lit_str("open"))`. ADR 082 adds `lit_str`/`lit_f64`/`lit_bool`/
`lit_i64`, lowering to existing `ScalarValue` and immediately usable through the
existing `.eq`/`.gt`. No operator expansion (deferred to ADR 083).

## Grounding ledger (verified)

- `ScalarValue::{Utf8,Float64,Boolean,Int64}` exist (`ir.rs:31-36`); the binder binds
  them via the normal `append_literal` path; the SQL `WHERE status='open'` (080 gate)
  proves Utf8-literal compare end-to-end.
- **No cross-type `lit_f64`**: `coerce_literal_value` (`binder.rs`) has
  `Int32->{Int64,Float32,Float64}`, `Int64->{Float32,Float64}`, `Float64->Float32`, and
  a no-op fallthrough, but **no `Float64->Int32`**. So `col("amount"/*Int32*/).gt(
  lit_f64(15.5))` won't coerce; every `lit_f64`/`lit_bool` gate must be **same-type**
  (a Float64 / Boolean column).
- **`lit_i64` INCLUDE**: `sales.id` is `Int64`, Rune's native integer is `i64`, and
  `lit(i32)` truncates wide integers at the host boundary. `lit_i64` is the correct
  same-type ID literal, not optional polish.
- Fixture columns + expected ids **hand-verified** (incl. NULL `price` at id3 failing
  `>15.5` under 3VL): str->[1,2,4,5,6], f64->[2,4], bool->[1,3,4,6], i64->[4].

## Decisions (8, promotion-locked)

**D1 Typed literal helpers**: free fns `lit_str(String)->Utf8`, `lit_f64(f64)->
Float64`, `lit_bool(bool)->Boolean`, `lit_i64(i64)->Int64`.
**D2 Lower to existing IR**: each builds `Expr::Literal(ScalarValue::...)`; no new
`ScalarValue`/`Expr`/bound-expr/executor/PBC representation.
**D3 Keep `lit(i32)`** unchanged (079-081 back-compat).
**D4 `lit_i64` INCLUDE**: the correct same-type Int64 literal (grounded above).
**D5 Explicit helpers, no polymorphic `lit`**: matches the statically-typed host
idiom; no `rune::Value` runtime dispatch.
**D6 Register as free functions** in `create_toron_module` (not `RuneExpr` methods).
**D7 ADR 081 naming discipline**: names are keyword-free; no aliases, source
rewriting, or shims.
**D8 No operator expansion**: no `.lt/.lte/.gte/.ne`, `.and/.or/.logical_not`,
`.is_null/.is_not_null`, arithmetic, or keyword-colliding `.not` alias in this cut
(ADR 083 candidate).

## Behavior gate (same-type execution + wide-plan assertion)

Extend the rune `sales` fixture with two nullable test columns:
`price: Float64 = [10.5, 20.25, null, 30.75, 5.5, 7.25]`,
`active: Boolean = [true, false, true, true, false, true]`.

1. `lit_str`: `filter(col("status").eq(lit_str("open")))` -> ids **[1,2,4,5,6]**.
2. `lit_f64`: `filter(col("price").gt(lit_f64(15.5)))` -> ids **[2,4]** (Float64
   column, not `amount: Int32`).
3. `lit_bool`: `filter(col("active").eq(lit_bool(true)))` -> ids **[1,3,4,6]**.
4. `lit_i64`: `filter(col("id").eq(lit_i64(4)))` -> id **[4]**.
5. **Plan-only**: `filter(col("id").eq(lit_i64(3000000000)))` asserts the predicate
   carries `ScalarValue::Int64(Some(3000000000))` (pins the `lit_i64` rationale, with
   no i32 truncation and no huge fixture row).

## Architecture Guards (5)

`adr082_rune_typed_literal_helpers_lower_to_existing_scalar_values`,
`adr082_rune_typed_literal_helpers_are_registered_in_toron_module`,
`adr082_typed_literal_names_avoid_rune_keywords`,
`adr082_typed_literal_cut_does_not_expand_operator_surface` (asserts no
`lt/lte/gte/ne/and/or/logical_not/not/is_null/is_not_null/add/sub/mul/div`
registration; pins the literals-only scope),
`adr082_typed_literals_do_not_change_ir_binder_executor_or_sandbox`. Carry the ADR
079/080/081 guards, especially `adr079_` because the added fixture columns perturb
`SELECT *` paths, plus `adr080_` named-projection guards and `adr081_` no-source-
rewrite / keyword guards.

## Out of Scope

Operator expansion (ADR 083), `Like`/`InList`, `Cast`, typed-null helpers,
polymorphic `lit`, dot-field sugar, joins, sort/limit, SQL grammar, source rewriting,
sandbox changes.

## Council History

iter-0: Claude + Gemini -> expression-surface topic; **Claude flagged missing
booleans/null-predicates**. The council narrowed to **typed literals**. iter-1: Claude +
Gemini locked literals; **Claude grounded the `Float64->Int32` gap** (same-type fixtures)
and **`lit_i64` INCLUDE**; Codex held the broad surface. iter-2: **Codex conceded the
narrow scope and adopted both Claude groundings**; the council added an `active:
Boolean` column (real `lit_bool` test) and a wide-integer plan fixture. Genuine
3-of-3. Gemini's iter-1 cross-type `lit_f64` fixture superseded by the same-type one.

## Promotion criteria

4 helpers lower to existing `ScalarValue`; `lit(i32)` unchanged; registered as free
fns; 4 same-type behavior fixtures bind + execute; the wide-int fixture inspects the
logical predicate; `lit_f64` tested against `price: Float64` (not `amount`); no
operator surface; no production change outside `rune_engine.rs`; ADR 079/080/081 +
sandbox + group-by + clippy + full package green. Migration:
`plans/impl/0082_rune_typed_literals.md`.
