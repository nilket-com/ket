# Impl Plan 085: Rune Cast / TryCast Expression Surface

**Status:** Promoted. Paired with `plans/adr/0085_rune_cast_try_cast.md`. Genuine
3-of-3. Builds on the landed ADR 084 feat. **Posture:** pure additive surface in
`rune_engine.rs` — `RuneDataType` carrier + 4 constructors + 2 `RuneExpr` methods +
registration; ADR 024 owns all cast semantics.

## Invariants

- Lower to existing `Expr::Cast(Box<Expr>, DataType)` / `Expr::TryCast(...)`. No new
  IR/binder/executor/PBC.
- Numeric-only targets `{Int32,Int64,Float32,Float64}`; **no string parser**, no
  Utf8/Boolean target, no `safe_cast`.
- Keep 079–084 behavior intact. No change outside `rune_engine.rs` + tests.

## Phase 0 — tests first (`tests/rune_relation_handles_test.rs`)

Reuse the 082–084 `sales` fixture (`amount: Int32`, `price: Float64`, `id: Int64`).
All bind + execute; values verified.

- `rune_cast_projection_executes` (gate) —
  `project_named([col("amount").cast(type_i64()).named("amount_i64"),
  col("id").cast(type_i32()).named("id_i32"), col("price").cast(type_f32()).named("price_f32"),
  col("amount").cast(type_f64()).named("amount_f64")])` →
  `amount_i64=[10,20,NULL,30,NULL,7]`, `id_i32=[1,2,3,4,5,6]`,
  `price_f32=[10.5,20.25,NULL,30.75,5.5,7.25]`, `amount_f64=[10.0,20.0,NULL,30.0,NULL,7.0]`.
  Assert visible schema types + per-column rows incl. NULL slots.
- `rune_try_cast_nulls_data_domain_failures` —
  `col("id").add(lit_i64(3000000000)).try_cast(type_i32())` → all NULL;
  `col("price").mul(lit_f64(1e40)).try_cast(type_f32())` → all NULL; source NULL → NULL.
- `rune_strict_cast_domain_failure_is_kernel_error` —
  `col("id").add(lit_i64(3000000000)).cast(type_i32())` → `ExecutionError::Kernel`
  containing `"cast failed"` (match the variant; `panic!` on any other → never NULL).
- `rune_cast_predicate_is_residual` — bind
  `filter(col("amount").cast(type_i64()).gt(lit_i64(10)))`; assert ids `[2,4]`,
  `FilterDisposition::EvaluateInToron`, `scan_pbc(scan).is_none()`.
- `rune_cast_feeds_aggregate_argument` —
  `aggregate([], [sum(col("amount").try_cast(type_i64()))])` → scalar `[Some(67)]`.

## Phase 1 — production (`rune_engine.rs`; add `use arrow::datatypes::DataType` if needed)

```rust
#[derive(Any, Debug, Clone)]
pub struct RuneDataType { pub(crate) data_type: DataType }

pub fn type_i32() -> RuneDataType { RuneDataType { data_type: DataType::Int32 } }
pub fn type_i64() -> RuneDataType { RuneDataType { data_type: DataType::Int64 } }
pub fn type_f32() -> RuneDataType { RuneDataType { data_type: DataType::Float32 } }
pub fn type_f64() -> RuneDataType { RuneDataType { data_type: DataType::Float64 } }

impl RuneExpr {
    pub fn cast(self, target: RuneDataType) -> RuneExpr {
        RuneExpr { expr: Expr::Cast(Box::new(self.expr), target.data_type) }
    }
    pub fn try_cast(self, target: RuneDataType) -> RuneExpr {
        RuneExpr { expr: Expr::TryCast(Box::new(self.expr), target.data_type) }
    }
}
```

Register in `create_toron_module`:
```rust
m.ty::<RuneDataType>()?;
m.function(["type_i32"], type_i32).build()?;
m.function(["type_i64"], type_i64).build()?;
m.function(["type_f32"], type_f32).build()?;
m.function(["type_f64"], type_f64).build()?;
m.associated_function("cast", RuneExpr::cast)?;
m.associated_function("try_cast", RuneExpr::try_cast)?;
```

No `safe_cast`, no string-target method, no Utf8/Boolean constructors.

## Phase 2 — guards (`tests/architecture/rune_frontend.rs`)

**Two retargets (discipline #4 — both, per Codex's catch):**
- `adr082_typed_literal_cut_does_not_expand_operator_surface` (its forbidden list at
  ~`:696`): move `cast`/`try_cast` to **required**; keep `not, like, ilike, in_list,
  safe_cast` forbidden.
- `adr084_rune_arithmetic_methods_are_registered` (its forbidden-registration list at
  ~`:800`): **remove** `m.associated_function("cast"…)` / `"try_cast"…` (keep
  `"mod"`/`"neg"` forbidden). Leaving this stale would fail the moment 085 registers
  the methods.

New 085 guards:
- `adr085_rune_datatype_is_host_surface_only` — `RuneDataType` only in `rune_engine.rs`.
- `adr085_rune_cast_target_helpers_are_numeric_only` — exactly 4 `type_*`, each
  `DataType::{Int32,Int64,Float32,Float64}`; assert **no** `FromStr` / `match
  …as_str()` type parser, no `type_utf8`/`type_bool`/`type_decimal`/`type_date`.
- `adr085_rune_cast_methods_lower_to_existing_expr_variants` — `.cast`→`Expr::Cast`,
  `.try_cast`→`Expr::TryCast`.
- `adr085_rune_cast_methods_are_registered` — 2 methods + 4 constructors + carrier;
  no `safe_cast`.
- `adr085_rune_cast_reuses_existing_substrate` — no
  `BindingError`/`BoundExpr`/`ExecutionPlan`/`normalize_`/`evaluate_` markers in
  `rune_engine.rs`.
- `adr085_rune_cast_remains_pbc_ineligible` — `is_pbc_pushdown_eligible` false for
  Cast/TryCast (source-read `ir.rs`) + PBC rejection text.

## Verification

```
cargo fmt
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test architecture_test adr085_
cargo test -p toron --test architecture_test adr084_   # retargeted registration guard green
cargo test -p toron --test architecture_test adr083_
cargo test -p toron --test architecture_test adr082_   # retargeted scope guard green
cargo test -p toron --test architecture_test adr081_
cargo test -p toron --test architecture_test adr080_
cargo test -p toron --test architecture_test adr079_
cargo test -p toron --test group_by_test               # ADR-024 cast net intact
cargo test -p toron --test rune_sandbox_test
cargo clippy -p toron -- -D warnings
git diff --check
cargo test -p toron
```

## Evidence doc (`plans/impl/0085_rune_cast_try_cast_evidence.md`)

Record: `rune_engine.rs` + two test files as only touched; the 5 fixture results
(projection rows / try_cast→NULL / strict→Kernel "cast failed" / residual `[2,4]` /
agg `67`); **both** retargeted guards (082 + 084); confirmation
`binder.rs`/`exec.rs`/`ir.rs` unchanged and no string parser / `safe_cast` /
non-numeric target; command counts.

## Watch items

1. **Two guards, not one** — both the 082 scope guard and the 084 registration guard
   must be retargeted; running `adr084_` alone catches the stale one.
2. **Carrier, not string** — 4 `type_*()` constructors; the guard pins "no string
   parser". Do not add `.cast("Int64")`.
3. **try_cast → NULL, cast → Kernel** — assert the different outcomes for the same op.
4. **Residual, not pushed** — cast predicate asserts `EvaluateInToron` + no PBC.

## Acceptance checklist

- [ ] `RuneDataType` + `type_i32/i64/f32/f64` + `.cast`/`.try_cast` → existing `Expr`.
- [ ] Numeric-only; no string parser; no `safe_cast`.
- [ ] 5 fixtures green (projection / try_cast NULL / strict Kernel / residual `[2,4]` / agg `67`).
- [ ] Cast predicate asserts `EvaluateInToron`.
- [ ] **Both** the 082 and 084 guards retargeted; new 085 guards green.
- [ ] No production change outside `rune_engine.rs`; 079–084 guards intact.
- [ ] Full package and clippy clean.
