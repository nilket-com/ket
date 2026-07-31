# Implementation Plan: ADR 026 — `TRY_CAST(x AS T)` / `SAFE_CAST(x AS T)` — Error-Tolerant Numeric Primitive Cast

**Companion ADR:** `plans/adr/026_try_cast_numeric_primitive_lattice.md`

## Goal

Expose error-tolerant SQL `TRY_CAST(expr AS type)` and
`SAFE_CAST(expr AS type)` for Toron's four primitive
numeric types (`Int32`, `Int64`, `Float32`, `Float64`)
under the ADR 025 16-cell conversion lattice with
null-on-failure semantics for the five documented
data-domain failure modes while preserving:

- ADR 025's strict Cast contract verbatim.
- ADR 024's aggregate-argument column-only rule.
- ADR 025's PBC-residual contract (now widened to
  TryCast).
- ADR 022's loud-failure / sticky-NaN philosophy (for
  inner Cast errors propagated through TryCast).
- ADR 020 / 022 / 024 / 025's `ExecutionError::Kernel`
  error-variant discipline.

Six-phase rollout: baseline review → IR variants + metadata
→ SQL lowering → binder policy → execution kernels → PBC +
architecture guards → fixtures + verification.

## Touchpoint sweep (code-grounded, current `main`)

| File | Anchor | Phase |
|---|---|---|
| `crates/toron/src/ir.rs` | `Expr` enum + **`is_pbc_pushdown_eligible()`** helper | 1 |
| `crates/toron/src/ir/execution.rs` | `ExecutionExpr` enum + `data_type()` + `nullable()` + `pbc_eligible()` | 1 |
| `crates/toron/src/sql_planner.rs::lower_expr` | `SqlExpr::Cast { kind, .. }` arm | 2 |
| `crates/toron/src/sql_planner.rs::expr_contains_cast` | rename → `expr_contains_cast_family`; match `Cast | TryCast` | 2 |
| `crates/toron/src/sql_planner.rs::sort_needs_hidden_projection` | update gate call to renamed helper | 2 |
| `crates/toron/src/sql_planner.rs` (planner walkers) | confirm kind-agnostic recursion (already covers all CastKind variants via `SqlExpr::Cast { expr, .. }` patterns) | 2 (no-op confirm) |
| `crates/toron/src/sql_planner.rs::validate_aggregate_argument` | preserved (catchall rejects computed CAST/TRY_CAST) | 2 (no-op) |
| `crates/toron/src/binder.rs::bind_expr` | new `Expr::TryCast` arm dispatches to `bind_try_cast` | 3 |
| `crates/toron/src/binder.rs::bind_try_cast` | NEW helper (parallel to `bind_explicit_cast`) | 3 |
| `crates/toron/src/binder.rs::collect_expr_columns` | add `TryCast` arm (combined with Cast) | 3 |
| `crates/toron/src/binder.rs::rewrite_expr_ordinals` | add `TryCast` arm | 3 |
| `crates/toron/src/exec.rs::evaluate_expr` (unbound) | new `Expr::TryCast` arm | 4 |
| `crates/toron/src/exec.rs::evaluate_bound_expr` | new `ExecutionExpr::TryCast` arm — `?` propagates child errors (D11) | 4 |
| `crates/toron/src/exec.rs::evaluate_try_cast` | NEW evaluator (parallel to `evaluate_cast`) | 4 |
| `crates/toron/src/exec.rs::try_cast_*` | 6 NEW per-pair fail-to-null kernels | 4 |
| `crates/toron/src/pbc.rs::PbcCompiler::compile_expr` | widen Cast decline arm to also decline TryCast | 5 |
| `crates/toron/tests/architecture_test.rs::adr024_scalar_arithmetic_guards_are_narrow` | bump variant lists + PBC diagnostic string + `is_pbc_pushdown_eligible` TryCast arm assertion | 5 |
| `crates/toron/tests/architecture_test.rs::adr025_cast_syntax_variants_are_rejected` | rename → `adr026_cast_syntax_doublecolon_and_format_remain_rejected`; drop TRY/SAFE sub-assertions | 5 |
| `crates/toron/tests/architecture_test.rs` | 7 NEW ADR 026 guards + 3 NEW integration tests | 5 |
| `crates/toron/tests/group_by_test.rs` | positive + null-on-failure + preserved-reject + identity + D10 + D11 + D3 alias fixtures | 6 |

## Phase 0 — Baseline Review

Before edits, inspect to confirm starting state:

- `crates/toron/src/ir.rs` — `Expr` enum and
  `is_pbc_pushdown_eligible()` exhaustive body.
- `crates/toron/src/ir/execution.rs` — `ExecutionExpr`
  enum, `data_type()`, `nullable()`, `pbc_eligible()`.
- `crates/toron/src/sql_planner.rs` — `lower_expr`,
  `lower_cast_target_type`, `expr_contains_cast`,
  `sort_needs_hidden_projection`, planner walkers.
- `crates/toron/src/binder.rs` — `bind_expr`,
  `bind_explicit_cast`, `cast_if_needed`,
  `is_widening_cast`, `is_supported_explicit_numeric_cast`,
  `collect_expr_columns`, `rewrite_expr_ordinals`.
- `crates/toron/src/exec.rs` — `evaluate_cast`,
  `cast_failed`, `cast_*` strict helpers,
  `F64_I32_UPPER`, `F64_I64_UPPER`.
- `crates/toron/src/pbc.rs` — Cast decline arm.
- `crates/toron/tests/architecture_test.rs` — ADR 024 +
  ADR 025 guards.
- `crates/toron/tests/group_by_test.rs` — existing CAST
  fixtures.

**Preserve verbatim (ADR 025 contract):**
- `bind_explicit_cast` strict policy + no identity
  elision.
- `cast_if_needed` + `is_widening_cast` 4-cell ADR 024
  widening lattice.
- `evaluate_cast` + `cast_*` strict kernels +
  `cast_failed` Kernel-error prefix.
- `F64_I32_UPPER` / `F64_I64_UPPER` exclusive upper-
  bound checks.
- ADR 025 hidden-projection helpers' overall mechanism
  shape (`add_hidden_sort_projection`,
  `visible_output_names`, `unique_hidden_sort_name`,
  `__toron_order_key_{idx}` naming) — only the gate
  helper renames at D10; the mechanism proper stays
  untouched.

## Phase 1 — IR Variants + Metadata

Goal: add `Expr::TryCast` and `ExecutionExpr::TryCast`
twin variants with correct metadata. Update
`Expr::is_pbc_pushdown_eligible()` for exhaustiveness.

**Files:**
- `crates/toron/src/ir.rs`
- `crates/toron/src/ir/execution.rs`

**Tasks:**

1. **Add `Expr::TryCast(Box<Expr>, DataType)`** to the
   `Expr` enum, positioned after `Cast`.

2. **Update `Expr::is_pbc_pushdown_eligible()`** for
   exhaustiveness:
   ```rust
   impl Expr {
       pub fn is_pbc_pushdown_eligible(&self) -> bool {
           match self {
               // ... existing arms ...
               Self::Cast(_, _) => false,
               Self::TryCast(_, _) => false,  // NEW
               Self::Add(_, _) | Self::Sub(_, _) | Self::Mul(_, _) | Self::Div(_, _) => false,
               // ...
           }
       }
   }
   ```

3. **Add `ExecutionExpr::TryCast(Box<ExecutionExpr>,
   DataType)`** to the `ExecutionExpr` enum, after `Cast`.

4. **Update `ExecutionExpr::data_type()`** with combined
   arm:
   ```rust
   Self::Cast(_, target) | Self::TryCast(_, target) => target.clone(),
   ```

5. **Update `ExecutionExpr::nullable()`** (D5):
   ```rust
   Self::Cast(inner, _) => inner.nullable(),
   Self::TryCast(_, _) => true,  // NEW — always nullable
   ```

   The TryCast arm returns `true` unconditionally — do
   NOT use `inner.nullable() || _failure_possible(source,
   target)`. The contract is "this is the null-on-failure
   operator; downstream operators must handle null."

6. **Update `ExecutionExpr::pbc_eligible()`** (D9):
   ```rust
   Self::Cast(_, _) | Self::TryCast(_, _) => false,
   ```

7. **Compiler-exhaustiveness pass.** Run `cargo check -p
   toron` after adding the variants to expose remaining
   match sites. Expected catches: binder helpers
   (Phase 3), executor dispatch (Phase 4), PBC dispatch
   (Phase 5).

**Tests (Phase 1):**

Unit tests in `ir.rs` / `ir/execution.rs`:
```rust
#[test]
fn try_cast_is_not_pbc_pushdown_eligible() {
    let inner = Expr::Column(ColumnRef::unqualified("amount"));
    let try_cast = Expr::TryCast(Box::new(inner), DataType::Int64);
    assert!(!try_cast.is_pbc_pushdown_eligible());
}

#[test]
fn try_cast_execution_nullable_is_always_true() {
    let inner = ExecutionExpr::Column(ResolvedColumn {
        name: ColumnName("amount".into()),
        ordinal: ColumnOrdinal(0),
        data_type: DataType::Int32,
        nullable: false,
        pbc_eligible: true,
    });
    let try_cast = ExecutionExpr::TryCast(Box::new(inner), DataType::Int32);
    assert!(try_cast.nullable());  // even though inner Int32 column is non-nullable
}

#[test]
fn try_cast_execution_pbc_eligible_is_false() {
    let inner = ExecutionExpr::Column(ResolvedColumn {
        name: ColumnName("amount".into()),
        ordinal: ColumnOrdinal(0),
        data_type: DataType::Int32,
        nullable: false,
        pbc_eligible: true,
    });
    let try_cast = ExecutionExpr::TryCast(Box::new(inner), DataType::Int64);
    assert!(!try_cast.pbc_eligible());
}
```

## Phase 2 — SQL Lowering

Goal: wire `TRY_CAST` and `SAFE_CAST` to `Expr::TryCast`;
preserve `::` and `FORMAT` rejects; extend hidden-sort-key
gate to cast family (D10).

**Files:**
- `crates/toron/src/sql_planner.rs`

**Tasks:**

1. **Update `lower_expr`'s `SqlExpr::Cast` arm:**
   ```rust
   SqlExpr::Cast { kind, expr, data_type, format } => {
       if format.is_some() {
           return Err(unsupported("CAST FORMAT clause is not supported"));
       }
       match kind {
           CastKind::Cast => Ok(Expr::Cast(
               Box::new(self.lower_expr(expr)?),
               lower_cast_target_type(data_type)?,
           )),
           CastKind::TryCast | CastKind::SafeCast => Ok(Expr::TryCast(
               Box::new(self.lower_expr(expr)?),
               lower_cast_target_type(data_type)?,
           )),
           CastKind::DoubleColon => Err(unsupported(
               "unsupported cast syntax: ::; only CAST(... AS ...) is supported",
           )),
       }
   }
   ```

2. **Rename `expr_contains_cast` → `expr_contains_cast_family`**
   and widen match (D10):
   ```rust
   fn expr_contains_cast_family(expr: &Expr) -> bool {
       match expr {
           Expr::Cast(_, _) | Expr::TryCast(_, _) => true,
           Expr::Add(left, right)
               | Expr::Sub(left, right)
               | Expr::Mul(left, right)
               | Expr::Div(left, right)
               | Expr::Eq(left, right)
               | Expr::Neq(left, right)
               | Expr::Lt(left, right)
               | Expr::Lte(left, right)
               | Expr::Gt(left, right)
               | Expr::Gte(left, right)
               | Expr::And(left, right)
               | Expr::Or(left, right) => {
               expr_contains_cast_family(left) || expr_contains_cast_family(right)
           }
           Expr::Not(inner)
               | Expr::IsNull(inner)
               | Expr::IsNotNull(inner)
               | Expr::TimeBucket(_, inner) => expr_contains_cast_family(inner),
           Expr::Column(_) | Expr::Literal(_) => false,
       }
   }
   ```

   Update all recursive calls inside the body and the
   single call site in `sort_needs_hidden_projection`.

3. **Walker recursion audit (no-op confirmation).** ADR
   025 walkers match `SqlExpr::Cast { expr, .. }`
   kind-agnostically. TRY_CAST + SAFE_CAST inherit
   recursion automatically. Sites to verify during patch
   review:
   - `rewrite_grouped_aggregate_calls`
   - `discover_aggregate_calls`
   - `rewrite_aggregate_calls_for_projection`
   - `reject_qualified_having_column_references`
   - `reject_having_alias_references`
   - `reject_qualified_aggregate_projection_references`
   - `expr_contains_aggregate_call`
   - `expr_contains_nonaggregate_column_reference`
   - `collect_scope_refs`

4. **`validate_aggregate_argument` no-op.** TRY_CAST /
   SAFE_CAST inside aggregate argument continues to
   reject via existing catchall (D6).

5. **Single-caller audit of `expr_contains_cast` before
   rename.** Confirm exactly one caller
   (`sort_needs_hidden_projection`). Mechanical rename.

**Tests (Phase 2):**

Positive lowering:
- `SELECT TRY_CAST(amount AS BIGINT) FROM sales` —
  lowers to `Expr::TryCast`.
- `SELECT SAFE_CAST(amount AS BIGINT) FROM sales` —
  lowers to same `Expr::TryCast` (alias).
- `SELECT region, SUM(amount) FROM sales GROUP BY region
  ORDER BY TRY_CAST(SUM(amount) AS FLOAT32) DESC` —
  lowers + uses hidden projection (D10).

Negative (preserved rejects):
- `TRY_CAST(amount AS VARCHAR)` → ADR 025 target diagnostic.
- `TRY_CAST(amount AS FLOAT)` → ADR 025 ambiguous-FLOAT.
- `amount::BIGINT` → `::` reject.
- `TRY_CAST(amount AS BIGINT FORMAT '999')` → FORMAT reject.
- `SUM(TRY_CAST(amount AS BIGINT))` → ADR 024 column-only diagnostic.
- `SUM(SAFE_CAST(amount AS BIGINT))` → same.

## Phase 3 — Binder Policy

Goal: add `bind_try_cast` parallel to `bind_explicit_cast`;
reuse `is_supported_explicit_numeric_cast` lattice;
preserve identity TryCast nodes (D7); update ancillary
binder helpers.

**Files:**
- `crates/toron/src/binder.rs`

**Tasks:**

1. **Add `Expr::TryCast` arm to `bind_expr`:**
   ```rust
   Expr::TryCast(inner, target) => bind_try_cast(input_schema, config, inner, target),
   ```

2. **Add `bind_try_cast` helper:**
   ```rust
   fn bind_try_cast(
       input_schema: &SchemaRef,
       config: &PlannerConfig,
       inner: &Expr,
       target: &DataType,
   ) -> Result<ExecutionExpr, BindingError> {
       let inner = bind_expr(inner, input_schema, config)?;
       let source = inner.data_type();
       if !is_supported_explicit_numeric_cast(&source, target) {
           return Err(BindingError::TypeMismatch {
               message: format!(
                   "unsupported TRY_CAST/SAFE_CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported"
               ),
           });
       }
       // INTENTIONALLY NO identity elision here.
       // Same PBC-boundary reasoning as ADR 025 D8.
       // Eliding identity TryCast would let user-written CAST predicates
       // collapse to bare column refs and become PBC-pushable, violating
       // the cast-family residual contract (D7).
       Ok(ExecutionExpr::TryCast(Box::new(inner), target.clone()))
   }
   ```

3. **Update `collect_expr_columns`** with combined arm:
   ```rust
   ExecutionExpr::Cast(inner, _) | ExecutionExpr::TryCast(inner, _) => {
       collect_expr_columns(inner, columns)
   }
   ```

4. **Update `rewrite_expr_ordinals`** with TryCast arm:
   ```rust
   ExecutionExpr::TryCast(inner, target) => Ok(ExecutionExpr::TryCast(
       Box::new(rewrite_expr_ordinals(*inner, remap)?),
       target,
   )),
   ```

5. **Preserve untouched** (ADR 025 contract):
   - `bind_explicit_cast` strict body + no identity
     elision.
   - `cast_if_needed` + `is_widening_cast` 4-cell ADR
     024 widening.
   - `is_supported_explicit_numeric_cast` 16-cell
     whitelist.

6. **Architecture-guard contract requirement.** The
   `bind_try_cast` body must:
   - Call `is_supported_explicit_numeric_cast(&source, target)`.
   - Return `Ok(ExecutionExpr::TryCast(Box::new(inner), target.clone()))`.
   - **Not contain** `if source == *target` or `if source == target`.

**Tests (Phase 3):**

- `TRY_CAST(int_col AS Int32)` where `int_col` is `Int32`
  → binds to `ExecutionExpr::TryCast(int_col, Int32)`
  (NOT elided).
- `TRY_CAST(int_col AS Int64)` where `int_col` is `Int32`
  → binds.
- `TRY_CAST(big_amount AS REAL)` where `big_amount` is
  `Int64` → binds.
- `TRY_CAST(name AS BIGINT)` where `name` is `Utf8` →
  rejects with `"unsupported TRY_CAST/SAFE_CAST: Utf8 to Int64; ..."`.
- `SAFE_CAST(amount AS BIGINT)` produces the same bound
  expression as `TRY_CAST(amount AS BIGINT)` (D3 alias
  verification).

## Phase 4 — Execution Kernels

Goal: add `evaluate_try_cast`; 6 new fail-to-null kernels
for fallible cells; 6 infallible cells reuse strict
kernels; ensure child-error propagation (D11) via natural
`?` operator dispatch.

**Files:**
- `crates/toron/src/exec.rs`

**Tasks:**

1. **Add unbound `Expr::TryCast` arm to `evaluate_expr`:**
   ```rust
   Expr::TryCast(inner, target) => {
       let arr = self.evaluate_expr(inner, batch)?;  // child errors propagate (D11)
       evaluate_try_cast(&arr, target)
   }
   ```

2. **Add bound `ExecutionExpr::TryCast` arm to
   `evaluate_bound_expr`:**
   ```rust
   ExecutionExpr::TryCast(inner, target) => {
       let arr = self.evaluate_bound_expr(inner, batch)?;  // child errors propagate (D11)
       evaluate_try_cast(&arr, target)
   }
   ```

   **Critical:** the `?` operator on the inner call is
   what enforces D11. If the child errors, the error
   propagates without TryCast ever seeing it.

3. **Add `evaluate_try_cast`** parallel to
   `evaluate_cast`:
   ```rust
   fn evaluate_try_cast(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ExecutionError> {
       if array.data_type() == target {
           return Ok(array.clone());  // identity short-circuit
       }
       match (array.data_type(), target) {
           // 6 infallible cells — reuse ADR 025 strict kernels directly
           (DataType::Int32, DataType::Int64) => cast_i32_to_i64(array),
           (DataType::Int32, DataType::Float32) => cast_i32_to_f32(array),
           (DataType::Int32, DataType::Float64) => cast_i32_to_f64(array),
           (DataType::Int64, DataType::Float32) => cast_i64_to_f32(array),
           (DataType::Int64, DataType::Float64) => cast_i64_to_f64(array),
           (DataType::Float32, DataType::Float64) => cast_f32_to_f64(array),
           // 6 fallible cells — new fail-to-null kernels
           (DataType::Int64, DataType::Int32) => try_cast_i64_to_i32(array),
           (DataType::Float64, DataType::Float32) => try_cast_f64_to_f32(array),
           (DataType::Float32, DataType::Int32) => try_cast_f32_to_i32(array),
           (DataType::Float32, DataType::Int64) => try_cast_f32_to_i64(array),
           (DataType::Float64, DataType::Int32) => try_cast_f64_to_i32(array),
           (DataType::Float64, DataType::Int64) => try_cast_f64_to_i64(array),
           // Unreachable — binder gate is exhaustive
           (source, target) => Err(cast_failed(
               source, target,
               "unsupported combination (binder gate failure — file a bug)",
           )),
       }
   }
   ```

4. **`try_cast_i64_to_i32` (checked range, null on
   overflow):**
   ```rust
   fn try_cast_i64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let array = as_int64(array, "TryCast source")?;
       let values = array
           .iter()
           .map(|value| -> Option<i32> {
               let value = value?;  // null → null
               if value < i32::MIN as i64 || value > i32::MAX as i64 {
                   return None;
               }
               Some(value as i32)
           })
           .collect::<Vec<_>>();
       Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
   }
   ```

5. **`try_cast_f64_to_f32` (preserve NaN/Inf, null on
   finite overflow):**
   ```rust
   fn try_cast_f64_to_f32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let array = as_float64(array, "TryCast source")?;
       let values = array
           .iter()
           .map(|value| -> Option<f32> {
               let value = value?;
               if value.is_finite() && value.abs() > f32::MAX as f64 {
                   return None;
               }
               // NaN and ±Inf flow through `as f32` per IEEE 754
               Some(value as f32)
           })
           .collect::<Vec<_>>();
       Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
   }
   ```

6. **`try_cast_f64_to_i32` (NaN/Inf null, exclusive
   bound, truncate-toward-zero):**
   ```rust
   fn try_cast_f64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let array = as_float64(array, "TryCast source")?;
       let values = array
           .iter()
           .map(|value| -> Option<i32> {
               let value = value?;
               if value.is_nan() { return None; }
               if value.is_infinite() { return None; }
               let trunc = value.trunc();
               if !(F64_I32_LOWER <= trunc && trunc < F64_I32_UPPER) {
                   return None;
               }
               Some(trunc as i32)
           })
           .collect::<Vec<_>>();
       Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
   }
   ```

7. **`try_cast_f32_to_i32` (convert to f64 for range
   check):**
   ```rust
   fn try_cast_f32_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let array = as_float32(array, "TryCast source")?;
       let values = array
           .iter()
           .map(|value| -> Option<i32> {
               let value = value?;
               if value.is_nan() { return None; }
               if value.is_infinite() { return None; }
               // Convert to f64 BEFORE range check (ADR 025 D9)
               let trunc = f64::from(value).trunc();
               if !(F64_I32_LOWER <= trunc && trunc < F64_I32_UPPER) {
                   return None;
               }
               Some(trunc as i32)
           })
           .collect::<Vec<_>>();
       Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
   }
   ```

8. **`try_cast_f64_to_i64`, `try_cast_f32_to_i64`** —
   analogous, with `F64_I64_LOWER` / `F64_I64_UPPER`.

9. **Architecture-guard contract requirement** for the 6
   `try_cast_*` kernels:
   - **Absence of `cast_failed(`** (negative source-read).
   - **Absence of `ExecutionError::Kernel`** construction
     in non-catchall paths.
   - **Presence of `return None`** for failure conditions.
   - **Presence of F64_I32_UPPER / F64_I64_UPPER** in
     float-to-int kernels.
   - **Do not call `evaluate_cast`** from
     `evaluate_try_cast`.
   - **Do not catch `ExecutionError::Kernel`** and convert
     to null.

**Tests (Phase 4):**

Per-pair fixtures (parallel to ADR 025 strict fixtures):

Integer narrowing:
- `try_cast_i64_to_i32_in_range_succeeds`.
- `try_cast_i64_to_i32_overflow_nulls`.

Float narrowing:
- `try_cast_f64_to_f32_preserves_nan`.
- `try_cast_f64_to_f32_preserves_inf`.
- `try_cast_f64_to_f32_finite_overflow_nulls`.

Float → Int:
- `try_cast_f64_to_i32_truncates_toward_zero`.
- `try_cast_f64_to_i32_nan_nulls`.
- `try_cast_f64_to_i32_inf_nulls`.
- `try_cast_f64_to_i32_overflow_nulls`.
- `try_cast_f64_to_i32_boundary_excludes_upper`.
- `try_cast_f32_to_i32_boundary_at_i32_max`.

Null handling:
- `try_cast_null_input_stays_null`.

Identity (D7 enforcement at execution):
- `try_cast_i32_to_i32_identity_short_circuit`.

## Phase 5 — PBC + Architecture Guards

Goal: widen Cast decline diagnostic; rename ADR 025
syntax guard; add 7 new ADR 026 guards (including the
cast-family extension guard for the hidden-projection
mechanism) + 3 new integration tests.

**Files:**
- `crates/toron/src/pbc.rs`
- `crates/toron/tests/architecture_test.rs`

**Tasks:**

1. **PBC source update** (D9):
   ```rust
   ExecutionExpr::Cast(_, _)
   | ExecutionExpr::TryCast(_, _)  // NEW
   | ExecutionExpr::Add(_, _) | /* other arith */ => {
       Err(PbcCompileError::Unsupported {
           message: "arithmetic, Cast, and TryCast are not PBC-eligible".to_string(),
       })
   }
   ```

2. **Adjust `adr024_scalar_arithmetic_guards_are_narrow`:**
   - Bump Expr variant list to include
     `TryCast(Box<Expr>, DataType)`.
   - Bump ExecutionExpr variant list to include
     `TryCast(Box<ExecutionExpr>, DataType)`.
   - Update PBC diagnostic string assertion:
     `"arithmetic, Cast, and TryCast are not PBC-eligible"`.
   - Add `is_pbc_pushdown_eligible` exhaustiveness:
     ```rust
     let logical = std::fs::read_to_string(...).expect("read logical IR source");
     let helper_body = brace_blocks_after_marker(&logical, "fn is_pbc_pushdown_eligible")
         .into_iter().next().expect("is_pbc_pushdown_eligible body").1;
     assert!(helper_body.contains("Self::TryCast(_, _) => false"));
     ```

3. **Rename `adr025_cast_syntax_variants_are_rejected →
   adr026_cast_syntax_doublecolon_and_format_remain_rejected`:**
   - Drop TRY_CAST + SAFE_CAST reject sub-assertions.
   - Keep `::` + FORMAT rejects.
   - Add positive assertion that TRY_CAST + SAFE_CAST now
     lower to `Expr::TryCast`.

4. **Add `adr026_try_cast_ir_surface_is_narrow`:**
   ```rust
   #[test]
   fn adr026_try_cast_ir_surface_is_narrow() {
       let logical_source = std::fs::read_to_string(manifest_dir().join("src/ir.rs"))
           .expect("read logical IR source");
       assert!(logical_source.contains("TryCast(Box<Expr>, DataType)"));

       let pbc_eligible_body = brace_blocks_after_marker(&logical_source, "fn is_pbc_pushdown_eligible")
           .into_iter().next().expect("is_pbc_pushdown_eligible body").1;
       assert!(pbc_eligible_body.contains("Self::TryCast(_, _) => false"));

       let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
           .expect("read execution IR source");
       assert!(execution_source.contains("TryCast(Box<ExecutionExpr>, DataType)"));

       let nullable_body = brace_blocks_after_marker(&execution_source, "fn nullable")
           .into_iter().next().expect("nullable body").1;
       assert!(nullable_body.contains("TryCast(_, _) => true"));

       let pbc_eligible_exec_body = brace_blocks_after_marker(&execution_source, "fn pbc_eligible")
           .into_iter().next().expect("pbc_eligible body").1;
       assert!(pbc_eligible_exec_body.contains("TryCast(_, _)") && pbc_eligible_exec_body.contains("false"));
   }
   ```

5. **Add `adr026_try_cast_lowers_via_planner`:**
   ```rust
   #[test]
   fn adr026_try_cast_lowers_via_planner() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");
       let lower_expr = brace_blocks_after_marker(&planner, "fn lower_expr")
           .into_iter().next().expect("lower_expr body").1;
       assert!(lower_expr.contains("CastKind::TryCast | CastKind::SafeCast"));
       assert!(lower_expr.contains("Expr::TryCast(Box::new(self.lower_expr(expr)?)"));
       assert!(lower_expr.contains("lower_cast_target_type(data_type)?"));
   }
   ```

6. **Add `adr026_try_cast_policy_is_separate` (D7
   negative assertions critical):**
   ```rust
   #[test]
   fn adr026_try_cast_policy_is_separate() {
       let binder = std::fs::read_to_string(manifest_dir().join("src/binder.rs"))
           .expect("read binder source");
       let bind_try_cast_body = brace_blocks_after_marker(&binder, "fn bind_try_cast")
           .into_iter().next().expect("bind_try_cast body").1;

       assert!(bind_try_cast_body.contains("is_supported_explicit_numeric_cast"));
       assert!(bind_try_cast_body.contains("Ok(ExecutionExpr::TryCast(Box::new(inner), target.clone()))"));
       assert!(bind_try_cast_body.contains("unsupported TRY_CAST/SAFE_CAST"));

       assert!(
           !bind_try_cast_body.contains("if source == *target"),
           "bind_try_cast must not elide identity TryCast (ADR 026 D7)"
       );
       assert!(
           !bind_try_cast_body.contains("if source == target"),
           "bind_try_cast must not elide identity TryCast (ADR 026 D7)"
       );
   }
   ```

7. **Add `adr026_try_cast_supports_numeric_primitive_lattice`:**
   ```rust
   #[test]
   fn adr026_try_cast_supports_numeric_primitive_lattice() {
       let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs"))
           .expect("read exec source");
       let evaluate_try_cast = brace_blocks_after_marker(&exec, "fn evaluate_try_cast")
           .into_iter().next().expect("evaluate_try_cast body").1;

       assert!(evaluate_try_cast.contains("if array.data_type() == target"));

       for arm in [
           "(DataType::Int32, DataType::Int64) => cast_i32_to_i64(array)",
           "(DataType::Int32, DataType::Float32) => cast_i32_to_f32(array)",
           "(DataType::Int32, DataType::Float64) => cast_i32_to_f64(array)",
           "(DataType::Int64, DataType::Float32) => cast_i64_to_f32(array)",
           "(DataType::Int64, DataType::Float64) => cast_i64_to_f64(array)",
           "(DataType::Float32, DataType::Float64) => cast_f32_to_f64(array)",
       ] {
           assert!(evaluate_try_cast.contains(arm), "missing strict-reuse {arm}");
       }

       for arm in [
           "(DataType::Int64, DataType::Int32) => try_cast_i64_to_i32(array)",
           "(DataType::Float64, DataType::Float32) => try_cast_f64_to_f32(array)",
           "(DataType::Float32, DataType::Int32) => try_cast_f32_to_i32(array)",
           "(DataType::Float32, DataType::Int64) => try_cast_f32_to_i64(array)",
           "(DataType::Float64, DataType::Int32) => try_cast_f64_to_i32(array)",
           "(DataType::Float64, DataType::Int64) => try_cast_f64_to_i64(array)",
       ] {
           assert!(evaluate_try_cast.contains(arm), "missing fail-to-null {arm}");
       }
   }
   ```

8. **Add `adr026_try_cast_nulls_only_data_domain_failures`:**
   ```rust
   #[test]
   fn adr026_try_cast_nulls_only_data_domain_failures() {
       let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs"))
           .expect("read exec source");

       for kernel in [
           "fn try_cast_i64_to_i32", "fn try_cast_f64_to_f32",
           "fn try_cast_f32_to_i32", "fn try_cast_f32_to_i64",
           "fn try_cast_f64_to_i32", "fn try_cast_f64_to_i64",
       ] {
           let body = brace_blocks_after_marker(&exec, kernel)
               .into_iter().next().unwrap_or_else(|| panic!("{kernel} body")).1;

           assert!(!body.contains("cast_failed("),
               "{kernel} must not emit cast_failed errors (ADR 026 D8)");
           assert!(!body.contains("ExecutionError::Kernel"),
               "{kernel} must not construct Kernel errors directly");
           assert!(body.contains("return None"),
               "{kernel} must emit None on failure");
       }

       for kernel in [
           "fn try_cast_f32_to_i32", "fn try_cast_f32_to_i64",
           "fn try_cast_f64_to_i32", "fn try_cast_f64_to_i64",
       ] {
           let body = brace_blocks_after_marker(&exec, kernel)
               .into_iter().next().unwrap().1;
           assert!(
               body.contains("trunc < F64_I32_UPPER") || body.contains("trunc < F64_I64_UPPER"),
               "{kernel} must use ADR 025 D9 exclusive upper bound"
           );
       }
   }
   ```

9. **Add `adr026_try_cast_pbc_declines`:**
   ```rust
   #[test]
   fn adr026_try_cast_pbc_declines() {
       let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs"))
           .expect("read PBC source");
       assert!(pbc.contains("ExecutionExpr::TryCast(_, _)"));
       assert!(pbc.contains("arithmetic, Cast, and TryCast are not PBC-eligible"));
   }
   ```

10. **Add `adr026_try_cast_order_by_uses_hidden_sort_projection`
    (D10 — Cast-to-Cast-family hidden-sort-key extension):**
    ```rust
    #[test]
    fn adr026_try_cast_order_by_uses_hidden_sort_projection() {
        let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
            .expect("read SQL planner source");

        assert!(planner.contains("fn expr_contains_cast_family"));
        assert!(!planner.contains("fn expr_contains_cast("),
            "expr_contains_cast renamed to expr_contains_cast_family (ADR 026 D10)");

        let family_body = brace_blocks_after_marker(&planner, "fn expr_contains_cast_family")
            .into_iter().next().expect("expr_contains_cast_family body").1;
        assert!(family_body.contains("Expr::Cast(_, _) | Expr::TryCast(_, _)"));

        let sort_gate_body = brace_blocks_after_marker(&planner, "fn sort_needs_hidden_projection")
            .into_iter().next().expect("sort_needs_hidden_projection body").1;
        assert!(sort_gate_body.contains("expr_contains_cast_family"));
    }
    ```

11. **Add 3 new integration tests:**
    - `pbc_declines_try_cast_filter` — non-identity TryCast
      predicate residual.
    - `pbc_declines_identity_try_cast_filter` — identity
      TryCast residual (D7 end-to-end).
    - `order_by_try_cast_aggregate_uses_hidden_projection`
      — D10 end-to-end.

## Phase 6 — Fixtures + Verification

Goal: exhaustive positive / null-on-failure / preserved-
reject coverage; composition fixtures across ADR 022 /
023 / 024 / 025 surfaces; D3 alias equivalence; D7
identity preservation; D11 inner-error propagation; plan-
digest parity for the no-TryCast corpus.

**Files:**
- `crates/toron/tests/group_by_test.rs`
- (optional) `crates/toron/tests/try_cast_test.rs` for
  per-pair execution tests if size warrants.

**Tasks:**

1. **Positive SELECT fixtures:**
   - `SELECT TRY_CAST(amount AS BIGINT) FROM sales`
   - `SELECT SAFE_CAST(amount AS BIGINT) FROM sales`
   - `SELECT TRY_CAST(id AS INT) FROM sales`
   - `SELECT TRY_CAST(price AS REAL) FROM sales`
   - `SELECT TRY_CAST(rating32 AS DOUBLE) FROM sales`

2. **Composition with ADR 024 arithmetic:**
   - `SELECT amount + TRY_CAST(price AS Int64) FROM sales`
   - `SELECT TRY_CAST(amount * 100 AS Int32) FROM sales`
     (rows that overflow → null; query succeeds)

3. **Post-aggregate TryCast (composes with ADR 023):**
   - `SELECT TRY_CAST(SUM(amount) AS DOUBLE) FROM sales`
   - `SELECT region, TRY_CAST(SUM(amount) AS Int32) FROM
     sales GROUP BY region`
   - `SELECT region FROM sales GROUP BY region HAVING
     TRY_CAST(SUM(amount) AS Int32) IS NOT NULL`

4. **D10 ORDER BY with TryCast:**
   - `SELECT region, SUM(amount) FROM sales GROUP BY
     region ORDER BY TRY_CAST(SUM(amount) AS FLOAT32) DESC`

5. **WHERE with TryCast (PBC-residual):**
   - `SELECT * FROM sales WHERE TRY_CAST(amount AS BIGINT)
     IS NOT NULL`
   - `SELECT * FROM sales WHERE TRY_CAST(amount AS BIGINT)
     > 1000`

6. **Null-on-failure fixtures:**
   - `TRY_CAST(i64::MAX literal AS Int32)` → null.
   - `TRY_CAST(price * 1e40 AS REAL)` overflow row → null;
     in-range row → cast value.
   - `TRY_CAST(score AS INT)` where `score = NaN` → null.
   - `TRY_CAST(price / 0.0 AS INT)` ±Inf row → null.
   - `TRY_CAST(special_float AS REAL)` preserves NaN/Inf.

7. **Preserved rejects:**
   - `SUM(TRY_CAST(amount AS BIGINT))` → ADR 024
     diagnostic.
   - `TRY_CAST(region AS BIGINT)` → binder reject.
   - `TRY_CAST(amount AS VARCHAR)` → unsupported target.
   - `TRY_CAST(amount AS FLOAT)` → ambiguous-FLOAT.
   - `TRY_CAST(amount AS DECIMAL(10,2))` → unsupported.
   - `TRY_CAST(amount AS BIGINT FORMAT '999')` → FORMAT
     reject.
   - `amount::BIGINT` → `::` reject.

8. **Identity preservation fixtures (D7):**
   - `SELECT TRY_CAST(int_col AS Int32) FROM sales` —
     bound expression shows `ExecutionExpr::TryCast` node
     (not elided).
   - `SELECT * FROM sales WHERE TRY_CAST(id AS INT) = 1`
     — residual filter (integration test from Phase 5).

9. **D11 inner-Cast-error propagation fixture:**
   ```rust
   #[tokio::test]
   async fn try_cast_over_strict_cast_propagates_inner_error() {
       let err = execute_sql_err(
           "SELECT TRY_CAST(CAST(amount * 100000 AS INT) AS BIGINT) FROM sales"
       ).await;
       match err {
           ExecutionError::Kernel { message } => {
               assert!(
                   message.starts_with("arithmetic overflow:") || message.starts_with("cast failed:"),
                   "inner error should propagate; got {message}"
               );
           }
           other => panic!("expected Kernel error, got {other:?}"),
       }
   }
   ```

10. **D3 alias-equivalence fixture:**
    ```rust
    #[tokio::test]
    async fn safe_cast_is_alias_for_try_cast() {
        let try_results = run_logical(&plan_sql("SELECT TRY_CAST(amount AS BIGINT) FROM sales")).await;
        let safe_results = run_logical(&plan_sql("SELECT SAFE_CAST(amount AS BIGINT) FROM sales")).await;
        assert_eq!(scalar_int64_column_rows(&try_results, 0),
                   scalar_int64_column_rows(&safe_results, 0));
    }
    ```

11. **Plan-digest parity check.** Spot-check 5 queries
    from `group_by_test.rs` against pre-patch plan
    digests. Diff should be empty for non-TryCast queries.

12. **Verification:**
    ```sh
    cargo fmt --check
    cargo test -p toron
    ```

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Plan digest regression for no-TryCast queries | Phase 6 task 11 spot-checks 5 queries. |
| R2 | ADR 025 syntax guard breaks | Phase 5 task 3 renames + drops sub-assertions. |
| R3 | Per-pair `try_cast_*` kernel duplication | Only 6 fallible cells; 6 infallible reuse strict. |
| R4 | Always-nullable surprises users | D5 documented + diagnostic fixtures. |
| R5 | Aggregate-argument over-rejection | Preserved column-only rule. |
| R6 | New IR variant breaks pattern exhaustiveness | Phase 1 audit + explicit Phase 3 `collect_expr_columns` / `rewrite_expr_ordinals` arms; compiler exhaustiveness catches at build time. |
| R7 | PBC diagnostic widening breaks ADR 024 guard | `adr024_scalar_arithmetic_guards_are_narrow` string assertion updated. |
| R8 | `evaluate_try_cast` divergence from `evaluate_cast` | Per-pair fixtures pair each strict failure with corresponding tolerant null. |
| R9 | Identity TryCast PBC leak | D7 + `adr026_try_cast_policy_is_separate` negative source-read + integration test. |
| R10 | D11 inner error swallowed by outer TryCast | D11 explicit + fixture + structural `?` operator pattern. |
| R11 | ADR 025 hidden-sort-key gate doesn't recognize TryCast | D10 + new architecture guard + integration test. |
| R12 | Spelling-neutral diagnostic loses debugging context | `TRY_CAST/SAFE_CAST` form covers both. |
| R13 | Walker recursion misses `CastKind::TryCast` | Audit confirms ADR 025 walkers kind-agnostic. |
| R14 | `null_on_failure: bool` flag drift in future refactor | Architecture guard for `try_cast_*` absence of `cast_failed(` prevents accidental regression. |
| R15 | `SAFE_CAST` non-numeric semantics surprise | D3 + D7 scope explicit. |
| R16 | TryCast over TryCast composition | Natural recursion via binder + evaluator; works structurally. |
| R17 | `bind_try_cast` accidentally elides identity | Negative source-read in `adr026_try_cast_policy_is_separate`. |
| R18 | `expr_contains_cast` callers outside `sort_needs_hidden_projection` | Single-caller audit before rename (Phase 2 task 5). |
| R19 | `Expr::is_pbc_pushdown_eligible` arm missed | Phase 1 task 2 explicit; compiler exhaustiveness catches. |
| R20 | `collect_expr_columns` / `rewrite_expr_ordinals` arms missed | Phase 3 tasks 3-4 explicit; compiler exhaustiveness catches. |
| R21 | Hidden-projection mechanism guard naming confusion | `adr026_try_cast_order_by_uses_hidden_sort_projection` is a NEW ADR 026 extension guard, not a rename of ADR 025's existing Cast-only guard. |

## Definition of Done

- [ ] `Expr::TryCast(Box<Expr>, DataType)` and
      `ExecutionExpr::TryCast(Box<ExecutionExpr>,
      DataType)` added with metadata: `data_type()` →
      target, `nullable()` → always `true` (D5),
      `pbc_eligible()` → `false` (D9).
- [ ] `Expr::is_pbc_pushdown_eligible()` arm
      `Self::TryCast(_, _) => false` added.
- [ ] `TRY_CAST` and `SAFE_CAST` lower to `Expr::TryCast`
      (D3); `::` and `FORMAT` remain rejected (D4).
- [ ] `bind_try_cast` reuses ADR 025 lattice via
      `is_supported_explicit_numeric_cast`; spelling-
      neutral diagnostic.
- [ ] Identity TryCast preserved as
      `ExecutionExpr::TryCast` at bind time (D7).
      Identity short-circuit lives in `evaluate_try_cast`
      only.
- [ ] `collect_expr_columns` and `rewrite_expr_ordinals`
      have TryCast arms.
- [ ] `evaluate_try_cast` has 12-arm dispatch + identity
      short-circuit: 6 infallible cells reuse strict
      kernels, 6 fallible cells use `try_cast_*` fail-to-
      null kernels (D8).
- [ ] All 6 `try_cast_*` kernels emit `None` (not
      `cast_failed`) for the 5 documented data-domain
      failure modes. Float-to-int kernels reuse ADR 025
      D9 exclusive upper bound.
- [ ] Inner strict Cast errors propagate through outer
      TryCast (D11) via natural `?` operator dispatch.
- [ ] Computed aggregate arguments containing TryCast
      still reject with ADR 024 column-only diagnostic.
- [ ] Cast in WHERE evaluates as Toron residual (ADR 025
      preserved); TryCast in WHERE also stays residual
      (D9). Identity TryCast in WHERE stays residual
      (D7).
- [ ] PBC compiler diagnostic widened to `"arithmetic,
      Cast, and TryCast are not PBC-eligible"`.
- [ ] `expr_contains_cast` renamed to
      `expr_contains_cast_family`; body matches
      `Cast | TryCast`; `sort_needs_hidden_projection`
      calls renamed helper (D10).
- [ ] 7 new ADR 026 architecture guards pass; 2 existing
      guards adjusted (1 renamed;
      `adr026_try_cast_order_by_uses_hidden_sort_projection`
      is **NEW** — extends ADR 025's Cast-only hidden-
      projection guard coverage to the cast family).
- [ ] 3 new integration tests pass:
      `pbc_declines_try_cast_filter`,
      `pbc_declines_identity_try_cast_filter`,
      `order_by_try_cast_aggregate_uses_hidden_projection`.
- [ ] Plan digests for no-TryCast queries are byte-
      identical pre/post-ADR-026.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.

## Verification commands

```sh
# During development — catch missed exhaustive match sites
cargo check -p toron

# Pre-PR checks
cargo fmt --check
cargo test -p toron

# Optional when workspace deps change
cargo build -p toron
```
