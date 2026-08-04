# ADR 085: Rune Cast / TryCast Expression Surface

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified
against the latest peer drafts (Codex iter-2, Gemini iter-2). Gemini conceded its
iter-1 string-target proposal to the typed carrier; no divergence outstanding.
**Category:** Toron frontend composition / Rune expression authoring over shared IR.
**Scope:** Toron-only; production change localized to `crates/toron/src/rune_engine.rs`
(a `RuneDataType` carrier, 4 numeric constructors, 2 `RuneExpr` methods, registration).
Lowers to **existing** `Expr::Cast`/`Expr::TryCast` (ADR 024). No IR/binder/executor/
SQL/PBC/dependency/`syn`/sandbox change.

## Summary

After 084's arithmetic, the remaining numeric-composition gap is explicit type
control. ADR 085 adds `.cast(type)` / `.try_cast(type)` — thin constructors over the
existing ADR 024 cast IR — so Rune can choose output width and strict-vs-nullable
failure behavior. Target types are a **typed carrier** (`RuneDataType` + `type_i32/
i64/f32/f64()`), not strings. Like arithmetic, casts are PBC-ineligible → the proof is
computed projection + residual filter execution.

## Doctrine

**Typed carriers over stringly APIs.** Type metadata is a host-side typed token, not
a runtime-parsed string — authoring-time feedback (a typo is an unknown-function
compile error, not a runtime bind error), a guardable fixed domain, and no
type-name parser in the frontend. (The same carrier-over-stringly-typed doctrine as
ADR 080's `RuneNamedExpr`; `time_bucket("1h")`'s silent zero-duration mapping is the
cautionary counter-example, not a precedent.)

## Grounding ledger (verified)

- `Expr::Cast(Box<Expr>, DataType)` / `TryCast` exist; `bind_explicit_cast` /
  `bind_try_cast` gate the numeric lattice `{Int32,Int64,Float32,Float64}` with a
  bounded reject; `evaluate_cast` / `evaluate_try_cast` implement the split.
- **Strict vs lenient pre-settled (ADR 024):** `cast_i64_to_i32` errors on narrowing
  overflow (`cast_failed` → `ExecutionError::Kernel "cast failed"`); `try_cast_*`
  return `None` (NULL) — verified incl. `try_cast_f64_to_f32` nulling finite
  `> f32::MAX`. Rune invents no policy; null input stays null.
- **PBC-ineligible → residual** — `is_pbc_pushdown_eligible` declines Cast/TryCast
  (`"arithmetic, Cast, and TryCast are not PBC-eligible"`); a cast predicate binds
  `EvaluateInToron`.
- **Two guards forbid cast/try_cast today** and must both be reconciled (discipline
  #4, see Guards): `adr082_typed_literal_cut_does_not_expand_operator_surface`
  (forbidden list) **and** `adr084_rune_arithmetic_methods_are_registered`
  (forbidden-registration list `m.associated_function("cast"…)`).
- `cast`/`try_cast`/`type_*` are keyword-free; no `should_implement_trait` clippy lint.

## Decisions (10, promotion-locked)

**D1 Typed carrier** — `RuneDataType { data_type: DataType }`, host-surface only.
**D2 Numeric constructors** — `type_i32/type_i64/type_f32/type_f64()` →
`DataType::{Int32,Int64,Float32,Float64}`. No `type_utf8`/`type_bool`/parser.
**D3 Two methods** — `.cast(RuneDataType)` → `Expr::Cast`; `.try_cast(RuneDataType)`
→ `Expr::TryCast`.
**D4 Numeric-only target domain** — binder `is_supported_explicit_numeric_cast` is the
backstop; unsupported targets are unrepresentable from the 4-constructor surface.
**D5 ADR 024 semantics inherited** — `cast` strict (`Kernel "cast failed"`);
`try_cast` lenient (NULL); null input → null. No Rune-local evaluation.
**D6 Residual** — cast predicates bind `EvaluateInToron`, no scan PBC.
**D7 Computed columns via `.named`** (ADR 081 shape); cast methods don't alias.
**D8 No `safe_cast`** — one spelling (`try_cast`), one-to-one with the engine model.
**D9 Reconcile BOTH inverted guards (discipline #4)** — see Guards. Retargeting only
the 082 guard would leave the stale 084 forbidden-registration guard contradicting
the decision (Codex's catch).
**D10 Sandbox intact** — host carrier + methods construct IR only.

## Behavior gate (exact values verified on the 082–084 `sales` fixture)

1. **Cast projection** — `project_named([col("amount").cast(type_i64()).named("amount_i64"),
   col("id").cast(type_i32()).named("id_i32"), col("price").cast(type_f32()).named("price_f32"),
   col("amount").cast(type_f64()).named("amount_f64")])`:
   `amount_i64=[10,20,NULL,30,NULL,7]`, `id_i32=[1,2,3,4,5,6]`,
   `price_f32=[10.5,20.25,NULL,30.75,5.5,7.25]`, `amount_f64=[10.0,20.0,NULL,30.0,NULL,7.0]`.
   Assert visible schema types + rows.
2. **TryCast nulls data-domain failures** —
   `col("id").add(lit_i64(3000000000)).try_cast(type_i32())` → all NULL;
   `col("price").mul(lit_f64(1e40)).try_cast(type_f32())` → all NULL (finite
   `> f32::MAX`); source NULL → NULL.
3. **Strict cast failure → Kernel** —
   `col("id").add(lit_i64(3000000000)).cast(type_i32())` → `ExecutionError::Kernel`
   containing `"cast failed"` (not a Rune parse/bind/sandbox error).
4. **Cast predicate is residual** — `filter(col("amount").cast(type_i64()).gt(lit_i64(10)))`
   → ids **[2,4]**; bind asserts `EvaluateInToron` + no scan PBC.
5. **Aggregate-argument composition** — `aggregate([], [sum(col("amount").try_cast(type_i64()))])`
   → scalar **[Some(67)]**.

## Architecture Guards

- **Retarget `adr082_typed_literal_cut_does_not_expand_operator_surface`**:
  `cast`/`try_cast` → **required**; forbidden remains `{not, like, ilike, in_list,
  safe_cast}`.
- **Retarget `adr084_rune_arithmetic_methods_are_registered`**: remove
  `m.associated_function("cast"…)` / `"try_cast"…` from its **forbidden-registration**
  list (keep `mod`/`neg` forbidden). *(Both retargets are required; #4.)*
- `adr085_rune_datatype_is_host_surface_only` (RuneDataType only in `rune_engine.rs`).
- `adr085_rune_cast_target_helpers_are_numeric_only` (exactly 4; **no string parser /
  `FromStr` / `match …as_str()` / SQL-planner call**; no `type_utf8`/`type_bool`).
- `adr085_rune_cast_methods_lower_to_existing_expr_variants` (`Expr::Cast`/`TryCast`).
- `adr085_rune_cast_methods_are_registered` (2 methods + 4 constructors + carrier; no
  `safe_cast`).
- `adr085_rune_cast_reuses_existing_substrate` (no binder/executor/PBC markers in
  `rune_engine.rs`).
- `adr085_rune_cast_remains_pbc_ineligible`.
- Carry 079–084 guards + `group_by_test` + `rune_sandbox_test`.

## Out of Scope

`Like`/`InList` (086/087), `Utf8`/`Boolean`/`Decimal`/`Date` casts, `safe_cast`,
string type-name parser, type aliases, unary negation, modulo, dot-field sugar,
joins, sort/limit, SQL grammar, worker-side Rune.

## Council History

iter-0: Gemini → Like/InList; Codex + Claude → Cast/TryCast; Claude proposed a string
target, Codex the typed carrier. iter-1: topic resolved to Cast 2-of-3; **Claude
conceded string → carrier** (no parser, compile-time typos, guardable; the 080
carrier doctrine + the `time_bucket` footgun). iter-2: **Gemini conceded the carrier
→ genuine 3-of-3**; **Codex caught the second stale guard** (`adr084_…are_registered`
forbids cast registration) — both guards retarget. Fixtures + exact values verified.

## Promotion criteria

Carrier + 4 numeric constructors + `.cast`/`.try_cast` → existing `Expr`;
numeric-only, no string parser, no `safe_cast`; 5 gates pass (strict `Kernel` vs
`try_cast` NULL, residual `EvaluateInToron`); **both** the 082 and 084 guards
reconciled; no production change outside `rune_engine.rs`; 079–084 + `group_by_test`
+ `rune_sandbox_test` + clippy + full package green. Migration:
`plans/impl/0085_rune_cast_try_cast.md`.
