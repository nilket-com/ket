# Implementation Plan: ADR 025 — SQL `CAST(x AS T)` Syntax — Numeric Primitive Lattice

**Companion ADR:** `plans/adr/025_sql_cast_numeric_primitive_lattice.md`

## Goal

Expose standard SQL `CAST(expr AS type)` for explicit
numeric casts among Toron's four primitive numeric types
(`Int32`, `Int64`, `Float32`, `Float64`) under the ADR 025
16-cell lattice while preserving:

- ADR 024's widening-only binder-internal arithmetic Cast
  policy (4-cell widening lattice).
- ADR 024's aggregate-argument column-only rule.
- ADR 024's PBC ineligibility for Cast.
- ADR 020 / 022 / 024's `ExecutionError::Kernel` error-
  variant discipline.
- ADR 022's loud-failure / sticky-NaN philosophy.

Five-phase rollout: SQL lowering → binder policy split →
execution kernels → PBC + architecture guards → fixtures +
verification.

## Touchpoint sweep (code-grounded, current `main`)

| File | Anchor | Phase |
|---|---|---|
| `crates/toron/src/sql_planner.rs::lower_expr` | `SqlExpr::Cast` in unsupported-expression catchall | Phase 1 |
| `crates/toron/src/sql_planner.rs` (module level) | `lower_cast_target_type` (NEW helper) | Phase 1 |
| `crates/toron/src/sql_planner.rs::validate_aggregate_argument` | preserved | Phase 1 (no-op) |
| `crates/toron/src/sql_planner.rs` (scalar-expression walkers) | aggregate-call detection, HAVING qualifier, nonaggregate column-reference detection, post-aggregate scope filter | Phase 1 (walker audit) |
| `crates/toron/src/binder.rs::bind_cast` | refactor into `bind_explicit_cast` | Phase 2 |
| `crates/toron/src/binder.rs::cast_if_needed` | preserved | Phase 2 (no-op) |
| `crates/toron/src/binder.rs::is_widening_cast` | preserved | Phase 2 (no-op) |
| `crates/toron/src/binder.rs::is_supported_explicit_numeric_cast` | NEW helper | Phase 2 |
| `crates/toron/src/exec.rs::evaluate_cast` | extend 4 → 16 cells | Phase 3 |
| `crates/toron/src/exec.rs::cast_failed` | NEW Kernel-error constructor | Phase 3 |
| `crates/toron/src/exec.rs` (float-to-int boundary constants) | NEW `F64_I32_UPPER` / `F64_I64_UPPER` | Phase 3 |
| `crates/toron/src/pbc.rs::PbcCompiler::compile_expr` | preserved Cast decline arm | Phase 4 (no-op) |
| `crates/toron/tests/architecture_test.rs::adr024_cast_is_binder_only_widening` | rename + drop SQL-lowering-absence sub-assertion | Phase 4 |
| `crates/toron/tests/architecture_test.rs` | 7 NEW ADR 025 guards | Phase 4 |
| `crates/toron/tests/group_by_test.rs` | post-aggregate / aggregate-argument fixtures | Phase 5 |
| `crates/toron/tests/non_aggregate_projection_accounting_test.rs` | non-aggregate SELECT / WHERE fixtures | Phase 5 |
| (optional) `crates/toron/tests/cast_test.rs` | per-pair execution fixtures if size warrants | Phase 5 |

## Phase 1 — SQL Lowering

Goal: wire SQL `CAST` syntax to the `Expr::Cast` IR with
target-type normalization, explicit rejection of out-of-
scope kinds / types, and walker recursion through CAST
nodes.

Tasks:

1. **Add `SqlExpr::Cast` arm to `lower_expr`:**
   ```rust
   SqlExpr::Cast { kind, expr, data_type, format } => {
       match (kind, format) {
           (CastKind::Cast, None) => {
               let inner = self.lower_expr(expr)?;
               let target = lower_cast_target_type(data_type)?;
               Ok(Expr::Cast(Box::new(inner), target))
           }
           (CastKind::TryCast, _) => Err(FrontendError::Unsupported(
               "unsupported cast syntax: TRY_CAST; only CAST(... AS ...) is supported".into(),
           )),
           (CastKind::SafeCast, _) => Err(FrontendError::Unsupported(
               "unsupported cast syntax: SAFE_CAST; only CAST(... AS ...) is supported".into(),
           )),
           (CastKind::DoubleColon, _) => Err(FrontendError::Unsupported(
               "unsupported cast syntax: ::; only CAST(... AS ...) is supported".into(),
           )),
           (CastKind::Cast, Some(_)) => Err(FrontendError::Unsupported(
               "CAST FORMAT clause is not supported".into(),
           )),
       }
   }
   ```

2. **Add `lower_cast_target_type` helper:**
   ```rust
   fn lower_cast_target_type(
       data_type: &sqlparser::ast::DataType,
   ) -> Result<DataType, FrontendError> {
       use sqlparser::ast::{DataType as SqlDt, ExactNumberInfo};
       match data_type {
           SqlDt::Int(_) | SqlDt::Integer(_) | SqlDt::Int4(_) | SqlDt::Int32 => {
               Ok(DataType::Int32)
           }
           SqlDt::BigInt(_) | SqlDt::Int64 => Ok(DataType::Int64),
           SqlDt::Real | SqlDt::Float4 | SqlDt::Float32 => Ok(DataType::Float32),
           SqlDt::Double(ExactNumberInfo::None)
               | SqlDt::DoublePrecision
               | SqlDt::Float8
               | SqlDt::Float64 => Ok(DataType::Float64),
           SqlDt::Float(_) => Err(FrontendError::Unsupported(
               "unsupported CAST target type: FLOAT (precision-parameter ambiguous; use REAL, DOUBLE PRECISION, FLOAT32, or FLOAT64)".into(),
           )),
           other => Err(FrontendError::Unsupported(
               format!("unsupported CAST target type: {other}"),
           )),
       }
   }
   ```

3. **Walker-recursion audit.** Every scalar-expression
   walker in `sql_planner.rs` recurses through
   `SqlExpr::Cast`. Sites to audit during patch:
   - `qualified_having_reference_walker`
   - HAVING-alias resolver
   - aggregate-projection reference detector
   - non-aggregate column-reference detector
   - aggregate-call detector (`contains_aggregate_call`)
   - post-aggregate scope filter (ADR 023 derived from
     `AggregateOutputCatalog`)

   Audit pattern:
   ```rust
   // Add Cast recursion alongside other binary-op recursions
   SqlExpr::Cast { expr, .. } => walker(expr),
   SqlExpr::BinaryOp { left, right, .. } => {
       walker(left)?;
       walker(right)?;
       Ok(())
   }
   ```

4. **`validate_aggregate_argument` no-op.** `SqlExpr::Cast`
   is not `SqlExpr::Identifier` or `CompoundIdentifier`;
   the existing catchall rejects with the column-only
   diagnostic. Verify `SUM(CAST(amount AS BIGINT))`
   rejects in Phase 5 fixtures.

5. **`SMALLINT` / `TINYINT` rejection** falls through to
   the catchall arm with `format!("unsupported CAST target
   type: {other}")`.

Tests (Phase 1):

Positive lowering:
- `SELECT CAST(amount AS BIGINT) FROM sales`
- `SELECT CAST(id AS INT) FROM sales`
- `SELECT CAST(price AS REAL) FROM sales`
- `SELECT CAST(price AS DOUBLE PRECISION) FROM sales`
- `SELECT amount + CAST(price AS BIGINT) FROM sales`
- `SELECT CAST(SUM(amount) AS DOUBLE) FROM sales`
- `SELECT region, CAST(SUM(amount) AS DOUBLE) FROM sales GROUP BY region`
- `SELECT * FROM sales WHERE CAST(amount AS BIGINT) > 1000`

Negative (reject diagnostics):
- `CAST(amount AS VARCHAR)` → `"unsupported CAST target type: VARCHAR"`
- `CAST(amount AS FLOAT)` → `"unsupported CAST target type: FLOAT (precision-parameter ambiguous; ...)"`
- `CAST(amount AS DECIMAL(10,2))` → `"unsupported CAST target type: ..."`
- `CAST(amount AS SMALLINT)` → `"unsupported CAST target type: SMALLINT"`
- `CAST(amount AS BOOLEAN)` → `"unsupported CAST target type: BOOLEAN"`
- `TRY_CAST(amount AS BIGINT)` → `"unsupported cast syntax: TRY_CAST; ..."`
- `SAFE_CAST(amount AS BIGINT)` → `"unsupported cast syntax: SAFE_CAST; ..."`
- `amount::BIGINT` → `"unsupported cast syntax: ::; ..."`
- `CAST(amount AS BIGINT FORMAT '...')` → `"CAST FORMAT clause is not supported"`
- `SUM(CAST(amount AS BIGINT))` → ADR 024 `"aggregate arguments must be simple column references"`

## Phase 2 — Binder Policy Split

Goal: cleanly separate ADR 024's binder-internal widening
Cast policy from ADR 025's explicit numeric Cast policy.
**No identity elision in `bind_explicit_cast`** (D8).

Tasks:

1. **Preserve `cast_if_needed` and `is_widening_cast`
   exactly as ADR 024 left them.** 4-cell widening
   lattice: `Int32→Int64`, `Int32→Float64`,
   `Int64→Float64`, `Float32→Float64`.

2. **Add `is_supported_explicit_numeric_cast`:**
   ```rust
   fn is_supported_explicit_numeric_cast(
       source: &DataType,
       target: &DataType,
   ) -> bool {
       use DataType::*;
       matches!(
           (source, target),
           (Int32, Int32) | (Int32, Int64) | (Int32, Float32) | (Int32, Float64)
           | (Int64, Int32) | (Int64, Int64) | (Int64, Float32) | (Int64, Float64)
           | (Float32, Int32) | (Float32, Int64) | (Float32, Float32) | (Float32, Float64)
           | (Float64, Int32) | (Float64, Int64) | (Float64, Float32) | (Float64, Float64)
       )
   }
   ```

3. **Add `bind_explicit_cast` — NO identity elision
   (D8):**
   ```rust
   fn bind_explicit_cast(
       input_schema: &SchemaRef,
       config: &PlannerConfig,
       inner: &Expr,
       target: &DataType,
   ) -> Result<ExecutionExpr, BindingError> {
       let bound_inner = bind_expr(inner, input_schema, config)?;
       let source = bound_inner.data_type();
       if !is_supported_explicit_numeric_cast(&source, target) {
           return Err(BindingError::TypeMismatch {
               message: format!(
                   "unsupported CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported",
               ),
           });
       }
       // INTENTIONALLY NO identity elision here.
       // ExecutionExpr::Cast is the PBC-ineligibility boundary; preserving the
       // node for source == target prevents user-written CAST predicates from
       // becoming PBC-pushable. See ADR 025 D5 / D8.
       Ok(ExecutionExpr::Cast(Box::new(bound_inner), target.clone()))
   }
   ```

   The comment block is part of the architecture-guard
   contract — `adr025_explicit_cast_policy_separate`
   source-reads for the absence of `if source == *target`
   in `bind_explicit_cast`'s body.

4. **Route the binder's `Expr::Cast` arm** to
   `bind_explicit_cast`. The `bind_arithmetic` path that
   calls `cast_if_needed` for operand promotion is
   untouched.

5. **Preserve `ExecutionExpr::Cast(Box<ExecutionExpr>,
   DataType)`** shape and `nullable() == inner.nullable()`
   passthrough.

Tests (Phase 2):

- `CAST(int_col AS Int32)` where `int_col` is `Int32` →
  binds to `ExecutionExpr::Cast(int_col, Int32)` (NOT elided).
- `CAST(int_col AS Int64)` where `int_col` is `Int32` →
  binds to `ExecutionExpr::Cast(int_col, Int64)`.
- `CAST(big_amount AS REAL)` where `big_amount` is `Int64`
  → binds (lossy precision is explicit user intent).
- `CAST(name AS BIGINT)` where `name` is `Utf8` → rejects.
- `CAST(flag AS Int32)` where `flag` is `Boolean` → rejects.
- Binder-injected arithmetic casts remain widening-only
  (architecture guard).

## Phase 3 — Execution Kernels

Goal: extend `evaluate_cast` from 4 ADR 024 widening paths
to the full 16-cell numeric matrix. Manual per-(source,
target) kernels with stable Kernel errors and exclusive
upper bound for float-to-int range checks (D9).

Tasks:

1. **Add `cast_failed` helper:**
   ```rust
   fn cast_failed(
       source: &DataType,
       target: &DataType,
       cause: impl fmt::Display,
   ) -> ExecutionError {
       ExecutionError::Kernel {
           message: format!("cast failed: {source:?} to {target:?}: {cause}"),
       }
   }
   ```

2. **Define float-to-int boundary constants (D9):**
   ```rust
   // Exclusive upper bounds: i32::MAX as f32 = 2147483648.0 (one above
   // i32::MAX due to f32 rounding); same trap for i64/f64. See ADR 025 D9.
   const F64_I32_LOWER: f64 = i32::MIN as f64;        // -2147483648.0
   const F64_I32_UPPER: f64 = 2147483648.0;           // i32::MAX + 1, exact in f64
   const F64_I64_LOWER: f64 = i64::MIN as f64;        // -9223372036854775808.0
   const F64_I64_UPPER: f64 = 9223372036854775808.0;  // i64::MAX + 1, exact in f64
   ```

3. **Extend `evaluate_cast`:**
   ```rust
   fn evaluate_cast(
       inner: &ArrayRef,
       target: &DataType,
   ) -> Result<ArrayRef, ExecutionError> {
       // Identity short-circuit — this is the ONLY identity elision; the
       // binder preserves the ExecutionExpr::Cast node (ADR 025 D8).
       if inner.data_type() == target {
           return Ok(inner.clone());
       }
       let source = inner.data_type();
       match (source, target) {
           (DataType::Int32, DataType::Int64) => cast_i32_to_i64(inner),
           (DataType::Int32, DataType::Float32) => cast_i32_to_f32(inner),
           (DataType::Int32, DataType::Float64) => cast_i32_to_f64(inner),
           (DataType::Int64, DataType::Int32) => cast_i64_to_i32(inner),
           (DataType::Int64, DataType::Float32) => cast_i64_to_f32(inner),
           (DataType::Int64, DataType::Float64) => cast_i64_to_f64(inner),
           (DataType::Float32, DataType::Int32) => cast_f32_to_i32(inner),
           (DataType::Float32, DataType::Int64) => cast_f32_to_i64(inner),
           (DataType::Float32, DataType::Float64) => cast_f32_to_f64(inner),
           (DataType::Float64, DataType::Int32) => cast_f64_to_i32(inner),
           (DataType::Float64, DataType::Int64) => cast_f64_to_i64(inner),
           (DataType::Float64, DataType::Float32) => cast_f64_to_f32(inner),
           _ => Err(cast_failed(
               source, target,
               "unsupported combination (binder gate failure — file a bug)",
           )),
       }
   }
   ```

4. **`cast_i64_to_i32` (checked range):**
   ```rust
   fn cast_i64_to_i32(inner: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let src = inner.as_primitive::<Int64Type>();
       let mut builder = Int32Array::builder(src.len());
       for i in 0..src.len() {
           if src.is_null(i) {
               builder.append_null();
               continue;
           }
           let v = src.value(i);
           if v < i32::MIN as i64 || v > i32::MAX as i64 {
               return Err(cast_failed(
                   &DataType::Int64, &DataType::Int32,
                   format!("value {v} does not fit"),
               ));
           }
           builder.append_value(v as i32);
       }
       Ok(Arc::new(builder.finish()))
   }
   ```

5. **`cast_f64_to_f32` (preserve NaN/Inf, reject finite
   overflow):**
   ```rust
   fn cast_f64_to_f32(inner: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let src = inner.as_primitive::<Float64Type>();
       let mut builder = Float32Array::builder(src.len());
       for i in 0..src.len() {
           if src.is_null(i) {
               builder.append_null();
               continue;
           }
           let v = src.value(i);
           if v.is_finite() && v.abs() > f32::MAX as f64 {
               return Err(cast_failed(
                   &DataType::Float64, &DataType::Float32,
                   format!("finite value {v} overflows Float32"),
               ));
           }
           // NaN and ±Inf flow through `as f32` correctly per IEEE 754.
           builder.append_value(v as f32);
       }
       Ok(Arc::new(builder.finish()))
   }
   ```

6. **`cast_f64_to_i32` (NaN/Inf reject, exclusive bound,
   truncate-toward-zero — D9):**
   ```rust
   fn cast_f64_to_i32(inner: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let src = inner.as_primitive::<Float64Type>();
       let mut builder = Int32Array::builder(src.len());
       for i in 0..src.len() {
           if src.is_null(i) {
               builder.append_null();
               continue;
           }
           let v = src.value(i);
           if v.is_nan() {
               return Err(cast_failed(
                   &DataType::Float64, &DataType::Int32,
                   "NaN has no integer image",
               ));
           }
           if v.is_infinite() {
               return Err(cast_failed(
                   &DataType::Float64, &DataType::Int32,
                   "Inf has no integer image",
               ));
           }
           let trunc = v.trunc();
           if !(F64_I32_LOWER <= trunc && trunc < F64_I32_UPPER) {
               return Err(cast_failed(
                   &DataType::Float64, &DataType::Int32,
                   format!("value {v} truncated to {trunc} does not fit Int32"),
               ));
           }
           builder.append_value(trunc as i32);
       }
       Ok(Arc::new(builder.finish()))
   }
   ```

7. **`cast_f32_to_i32` (convert to f64 for range check —
   D9):**
   ```rust
   fn cast_f32_to_i32(inner: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let src = inner.as_primitive::<Float32Type>();
       let mut builder = Int32Array::builder(src.len());
       for i in 0..src.len() {
           if src.is_null(i) {
               builder.append_null();
               continue;
           }
           let v = src.value(i);
           if v.is_nan() {
               return Err(cast_failed(&DataType::Float32, &DataType::Int32, "NaN has no integer image"));
           }
           if v.is_infinite() {
               return Err(cast_failed(&DataType::Float32, &DataType::Int32, "Inf has no integer image"));
           }
           // Convert to f64 BEFORE range check to avoid f32 rounding traps.
           let trunc = (v as f64).trunc();
           if !(F64_I32_LOWER <= trunc && trunc < F64_I32_UPPER) {
               return Err(cast_failed(
                   &DataType::Float32, &DataType::Int32,
                   format!("value {v} truncated to {trunc} does not fit Int32"),
               ));
           }
           builder.append_value(trunc as i32);
       }
       Ok(Arc::new(builder.finish()))
   }
   ```

8. **`cast_f64_to_i64`, `cast_f32_to_i64`** — analogous to
   tasks 6/7 with `F64_I64_LOWER` / `F64_I64_UPPER`.

9. **`cast_i32_to_f32`, `cast_i64_to_f32` (lossy direct
   cast):**
   ```rust
   fn cast_i64_to_f32(inner: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       // Precision loss is explicit user intent. i64 values above ~2^24 lose
       // mantissa precision in Float32, but every i64 value remains finite
       // because the i64 range is far below f32::MAX (ADR 025 D2).
       let src = inner.as_primitive::<Int64Type>();
       let mut builder = Float32Array::builder(src.len());
       for i in 0..src.len() {
           if src.is_null(i) {
               builder.append_null();
               continue;
           }
           builder.append_value(src.value(i) as f32);
       }
       Ok(Arc::new(builder.finish()))
   }
   ```

10. **Null mask propagation.** Each kernel iterates over
    the source's null bitmap; errors raised only on non-
    null source values.

11. **Prefix discipline.** All error returns from cast
    kernels go through `cast_failed`, guaranteeing the
    `"cast failed: "` prefix.

Tests (Phase 3) per-pair fixtures:

Integer narrowing:
- `cast_i64_to_i32_in_range_succeeds` — `100i64 → 100i32`.
- `cast_i64_to_i32_overflow_errors` — `i32::MAX as i64 + 1`
  → `cast failed: Int64 to Int32: value 2147483648 does not fit`.

Float narrowing:
- `cast_f64_to_f32_preserves_nan` — `f64::NAN → f32::NAN`.
- `cast_f64_to_f32_preserves_pos_inf` — `f64::INFINITY → f32::INFINITY`.
- `cast_f64_to_f32_preserves_neg_inf` — `f64::NEG_INFINITY → f32::NEG_INFINITY`.
- `cast_f64_to_f32_finite_overflow_errors` — `1e40f64` → error.
- `cast_f64_to_f32_boundary_at_f32_max` — `f32::MAX as f64`
  succeeds; `(f32::MAX as f64) * 1.000001` errors.

Float → Int (D9 boundary fixtures):
- `cast_f64_to_i32_truncates_toward_zero` — `3.7 → 3`,
  `-3.7 → -3`.
- `cast_f64_to_i32_nan_errors`.
- `cast_f64_to_i32_pos_inf_errors`.
- `cast_f64_to_i32_neg_inf_errors`.
- `cast_f64_to_i32_overflow_errors` — `1e20f64`.
- `cast_f64_to_i32_boundary_excludes_upper` — exact
  `2147483648.0f64` errors (D9 fixture).
- `cast_f64_to_i32_just_below_upper_succeeds` —
  `2147483647.0f64` → `i32::MAX`.
- `cast_f32_to_i32_boundary_at_i32_max` — `f32` value
  parsed from `2147483647_f32` (rounds to `2147483648.0f32`)
  errors (D9 fixture).
- `cast_f32_to_i32_just_below_upper_succeeds` —
  `2147483520_f32` → `2147483520_i32`.

Int → Float lossy:
- `cast_i32_to_f32_succeeds` — `100i32 → 100.0f32`.
- `cast_i64_to_f32_succeeds_large_value` —
  `1_000_000_000_000i64 → ~1e12f32` (precision loss).
- `cast_i64_to_f32_at_i64_max_succeeds` — `i64::MAX as
  f32` produces a finite f32 with expected precision loss.

Identity preservation (D8):
- `cast_i32_to_i32_identity_short_circuit` —
  `evaluate_cast` returns `Ok(inner.clone())`.
- `bind_explicit_cast_preserves_identity_node` —
  `bind_explicit_cast(int_col, Int32)` returns
  `ExecutionExpr::Cast(_, Int32)`, NOT the bare `inner`.

Null handling:
- `cast_null_input_stays_null` — null source element →
  null output, no error.

## Phase 4 — PBC + Architecture Guards

Goal: preserve Cast's PBC ineligibility; rename ADR 024
guards; add seven new ADR 025 guards.

Tasks:

1. **PBC source no-op.** `PbcCompiler::compile_expr`
   continues to decline `ExecutionExpr::Cast` with the
   existing ADR 024 diagnostic `"arithmetic and Cast are
   not PBC-eligible"`.

2. **Rename + adjust `adr024_cast_is_binder_only_widening`:**
   - New name: `adr025_binder_arithmetic_cast_remains_widening_only`.
   - Drop the source-read for "no `SqlExpr::Cast` lowering
     site" (now false).
   - Keep source-reads for `cast_if_needed` + `is_widening_cast`
     bodies (4 cells only).

3. **Add `adr025_sql_cast_lowers_via_planner`:**
   ```rust
   #[test]
   fn adr025_sql_cast_lowers_via_planner() {
       let src = read_to_string("src/sql_planner.rs").unwrap();
       assert!(src.contains("SqlExpr::Cast { kind, expr, data_type, format }"));
       assert!(src.contains("(CastKind::Cast, None) =>"));
       assert!(src.contains("lower_cast_target_type"));
   }
   ```

4. **Add `adr025_sql_cast_target_type_normalization`:**
   ```rust
   #[test]
   fn adr025_sql_cast_target_type_normalization() {
       let src = read_to_string("src/sql_planner.rs").unwrap();
       for spelling in ["SqlDt::Int(_)", "SqlDt::Integer(_)", "SqlDt::Int4(_)", "SqlDt::Int32"] {
           assert!(src.contains(spelling), "missing {spelling}");
       }
       assert!(src.contains("SqlDt::BigInt(_)"));
       assert!(src.contains("SqlDt::Int64"));
       for spelling in ["SqlDt::Real", "SqlDt::Float4", "SqlDt::Float32"] {
           assert!(src.contains(spelling));
       }
       for spelling in ["SqlDt::Double(ExactNumberInfo::None)", "SqlDt::DoublePrecision", "SqlDt::Float8", "SqlDt::Float64"] {
           assert!(src.contains(spelling));
       }
       assert!(src.contains("SqlDt::Float(_) =>"));
       assert!(src.contains("unsupported CAST target type: FLOAT"));
   }
   ```

5. **Add `adr025_explicit_cast_policy_separate`
   (D8 negative assertions critical):**
   ```rust
   #[test]
   fn adr025_explicit_cast_policy_separate() {
       let src = read_to_string("src/binder.rs").unwrap();
       let widening = extract_fn_body(&src, "is_widening_cast");
       for cell in ["(Int32, Int64)", "(Int32, Float64)", "(Int64, Float64)", "(Float32, Float64)"] {
           assert!(widening.contains(cell), "missing widening cell {cell}");
       }
       for absent in ["(Int64, Int32)", "(Float64, Float32)", "(Float64, Int32)", "(Float32, Int32)"] {
           assert!(!widening.contains(absent), "is_widening_cast leaked {absent}");
       }
       let explicit = extract_fn_body(&src, "is_supported_explicit_numeric_cast");
       for src_ty in ["Int32", "Int64", "Float32", "Float64"] {
           for tgt_ty in ["Int32", "Int64", "Float32", "Float64"] {
               let cell = format!("({src_ty}, {tgt_ty})");
               assert!(explicit.contains(&cell), "missing explicit cell {cell}");
           }
       }
       let bind_explicit = extract_fn_body(&src, "bind_explicit_cast");
       assert!(
           !bind_explicit.contains("if source == *target"),
           "bind_explicit_cast must not elide identity Cast — preserves PBC boundary (ADR 025 D8)"
       );
       assert!(
           !bind_explicit.contains("if source == target"),
           "bind_explicit_cast must not elide identity Cast — preserves PBC boundary (ADR 025 D8)"
       );
   }
   ```

6. **Add `adr025_cast_supports_numeric_primitive_lattice`:**
   ```rust
   #[test]
   fn adr025_cast_supports_numeric_primitive_lattice() {
       let src = read_to_string("src/exec.rs").unwrap();
       let body = extract_fn_body(&src, "evaluate_cast");
       let expected = [
           "(DataType::Int32, DataType::Int64)",
           "(DataType::Int32, DataType::Float32)",
           "(DataType::Int32, DataType::Float64)",
           "(DataType::Int64, DataType::Int32)",
           "(DataType::Int64, DataType::Float32)",
           "(DataType::Int64, DataType::Float64)",
           "(DataType::Float32, DataType::Int32)",
           "(DataType::Float32, DataType::Int64)",
           "(DataType::Float32, DataType::Float64)",
           "(DataType::Float64, DataType::Int32)",
           "(DataType::Float64, DataType::Int64)",
           "(DataType::Float64, DataType::Float32)",
       ];
       for arm in expected {
           assert!(body.contains(arm), "missing match arm {arm}");
       }
       assert!(body.contains("if inner.data_type() == target {"));
   }
   ```

7. **Add `adr025_cast_failure_returns_kernel_error_with_prefix`:**
   ```rust
   #[test]
   fn adr025_cast_failure_returns_kernel_error_with_prefix() {
       let src = read_to_string("src/exec.rs").unwrap();
       let body = extract_fn_body(&src, "cast_failed");
       assert!(body.contains(r#""cast failed: {source:?} to {target:?}: {cause}""#));
       assert!(body.contains("ExecutionError::Kernel"));
   }
   ```

8. **Add `adr025_try_cast_safe_cast_doublecolon_reject`:**
   ```rust
   #[test]
   fn adr025_try_cast_safe_cast_doublecolon_reject() {
       let src = read_to_string("src/sql_planner.rs").unwrap();
       assert!(src.contains("CastKind::TryCast"));
       assert!(src.contains("unsupported cast syntax: TRY_CAST"));
       assert!(src.contains("CastKind::SafeCast"));
       assert!(src.contains("unsupported cast syntax: SAFE_CAST"));
       assert!(src.contains("CastKind::DoubleColon"));
       assert!(src.contains("unsupported cast syntax: ::"));
       assert!(src.contains("CAST FORMAT clause is not supported"));
   }
   ```

9. **Add `adr025_float_to_int_uses_exclusive_upper_bound`
   (D9):**
   ```rust
   #[test]
   fn adr025_float_to_int_uses_exclusive_upper_bound() {
       let src = read_to_string("src/exec.rs").unwrap();
       assert!(src.contains("F64_I32_UPPER: f64 = 2147483648.0"));
       assert!(src.contains("F64_I64_UPPER: f64 = 9223372036854775808.0"));
       for kernel in ["cast_f64_to_i32", "cast_f32_to_i32", "cast_f64_to_i64", "cast_f32_to_i64"] {
           let body = extract_fn_body(&src, kernel);
           assert!(
               body.contains("trunc < F64_I32_UPPER") || body.contains("trunc < F64_I64_UPPER"),
               "{kernel} must use exclusive upper bound (ADR 025 D9)"
           );
       }
   }
   ```

10. **PBC integration tests:**
    - `pbc_declines_cast_filter` — `WHERE CAST(amount AS
      BIGINT) > 10` → Toron residual, not PBC.
    - **NEW** `pbc_declines_identity_cast_filter` — `WHERE
      CAST(id AS INT) = 1` where `id` is already `Int32`
      → Toron residual (proves D8 holds end-to-end).

## Phase 5 — Fixtures + Verification

Goal: exhaustive positive / negative / error coverage
across the 16-cell lattice; composition coverage across
ADR 022 / 023 / 024 surfaces; plan-digest parity for the
no-CAST corpus.

Tasks:

1. **Supported SQL type-name spellings.** Positive lowering
   fixtures for every spelling in the D3 table:
   - `INT`, `INTEGER`, `INT4`, `INT32`
   - `BIGINT`, `INT64`
   - `REAL`, `FLOAT4`, `FLOAT32`
   - `DOUBLE`, `DOUBLE PRECISION`, `FLOAT8`, `FLOAT64`

2. **Clause placement coverage:**
   - `SELECT CAST(amount AS BIGINT) FROM sales`
   - `SELECT * FROM sales WHERE CAST(amount AS BIGINT) > 1000`
   - `SELECT region, SUM(amount) FROM sales GROUP BY region HAVING CAST(SUM(amount) AS Int32) > 100`
   - `SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY CAST(SUM(amount) AS Float32) DESC`
   - `SELECT CAST(SUM(amount) AS DOUBLE) FROM sales` (composes with ADR 023)

3. **Composition with ADR 024 arithmetic:**
   - `SELECT amount + CAST(price AS Int64) FROM sales`
   - `SELECT CAST(amount AS Float64) / price FROM sales`
   - `SELECT CAST(SUM(amount) AS Float64) / CAST(COUNT(*) AS Float64) FROM sales`
     (the manual-average-via-CAST pattern, working around
     ADR 024's integer-truncation Div).

4. **Preserved rejects:**
   - `SUM(CAST(amount AS BIGINT))` → ADR 024 column-only diagnostic.
   - `CAST(amount AS VARCHAR)` → unsupported target.
   - `CAST(amount AS FLOAT)` → unsupported target (precision ambiguous).
   - `CAST(name AS BIGINT)` (Utf8 source) → binder reject.
   - `TRY_CAST(amount AS BIGINT)` → unsupported syntax.
   - `amount::BIGINT` → unsupported syntax.

5. **Runtime error fixtures:**
   - `CAST(big_amount AS INT)` over an `Int64` fixture row containing
     `i64::MAX` → `cast failed: Int64 to Int32: value 9223372036854775807 does not fit`.
   - `CAST(2147483648.0 AS Int32)` → `cast failed: Float64 to Int32: ...` (D9 boundary).
   - Executor-level `Float64Array` fixture containing `f64::NAN`, cast to
     `Int32` → `cast failed: Float64 to Int32: NaN has no integer image`.
   - Executor-level `Float64Array` fixture containing `f64::INFINITY`, cast to
     `Int64` → `cast failed: Float64 to Int64: Inf has no integer image`.
   - `CAST(1e40 AS Float32)` → `cast failed: Float64 to Float32: finite value 1e40 overflows Float32`.
   - Executor-level `Float64Array` fixture containing `f64::NAN`, cast to
     `Float32` → succeeds, preserves NaN.

6. **Identity preservation fixtures (D8):**
   - `SELECT CAST(int_col AS Int32) FROM sales` — plan
     digest shows `ExecutionExpr::Cast` node (not elided).
   - `SELECT * FROM sales WHERE CAST(id AS INT) = 1` —
     residual filter, not PBC.

7. **Plan-digest parity check for no-CAST corpus.**
   Spot-check 5 queries from `group_by_test.rs` + 5 from
   `non_aggregate_projection_accounting_test.rs` against
   pre-patch plan digests. Non-CAST corpus diff should be
   empty.

8. **Verification:**
   ```sh
   cargo fmt --check
   cargo test -p toron
   ```
   If workspace deps / feature flags change:
   ```sh
   cargo build -p toron
   ```

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Plan digest regression for no-CAST queries | Phase 5 task 7 spot-checks 10 queries. |
| R2 | ADR 024 `cast_is_binder_only_widening` guard breaks | Phase 4 task 2 renames + drops sub-assertion. |
| R3 | Float → Int overflow check per-row branch cost | Acceptable; future SoA / SIMD vectorizes. |
| R4 | NaN / Inf → Int halts query with bad data | Documented; `TRY_CAST` follow-on provides null-on-failure. |
| R5 | SQL type-name normalization misses an alias | Fixtures enumerate supported spellings; extend additively. |
| R6 | Aggregate-argument guard over-rejects | Guard preserves column-only rule. |
| R7 | Cast IR variant proliferation | Single `Cast(Box<Expr>, DataType)` accommodates any target. |
| R8 | `is_widening_cast` accidentally widened by patch | `adr025_explicit_cast_policy_separate` uses positive + negative source-reads. |
| R9 | PBC compiler arms need updating | No update needed; `pbc_eligible() = false` for all Cast. |
| R10 | `exec.rs` LOC inflation | Co-locate in `exec/cast.rs` if needed. |
| R11 | Float64 → Float32 finite-overflow check non-obvious | Comment + boundary fixtures. |
| R12 | Identity Cast PBC-pushdown leak (D8) | Negative source-read in guard + `pbc_declines_identity_cast_filter` integration test. |
| R13 | Float-to-int off-by-one at `i32::MAX as f32` (D9) | Exclusive upper bound; `adr025_float_to_int_uses_exclusive_upper_bound` guard + boundary fixtures. |
| R14 | `sqlparser` `CastKind::DoubleColon` availability | Verify pinned version; catchall handles absence with same diagnostic. |
| R15 | Walker recursion misses a `SqlExpr::Cast` site | Phase 1 task 3 enumerates walkers; integration tests exercise paths. |
| R16 | Architecture guard count growth | Co-locate ADR 025 guard changes in a contiguous block in `architecture_test.rs` for review. |
| R17 | `Int64 → Float32` precision loss is easy to overstate as overflow | Phase 3 task 9 comment + fixture documents that all `i64` values remain finite while precision loss is accepted. |

## Definition of Done

- [ ] Numeric `CAST(x AS T)` lowers, binds, and executes
      across the 16-cell primitive lattice in `SELECT`,
      `WHERE`, `HAVING`, `ORDER BY`, and post-aggregate
      projection.
- [ ] Binder-internal arithmetic Cast policy
      (`cast_if_needed` / `is_widening_cast`) remains
      ADR 024-pristine (4-cell widening lattice).
- [ ] Explicit Cast policy (`bind_explicit_cast` /
      `is_supported_explicit_numeric_cast`) exposes the
      16-cell numeric lattice.
- [ ] Identity Cast preserved as `ExecutionExpr::Cast` at
      bind time (D8). Identity short-circuit lives in
      `evaluate_cast` only.
- [ ] Float-to-int kernels use exclusive upper bound
      (D9). `Float32 → Int*` kernels convert to f64 before
      range check.
- [ ] Computed aggregate arguments containing CAST still
      reject with ADR 024 column-only diagnostic.
- [ ] `TRY_CAST`, `SAFE_CAST`, `::`, `FORMAT` reject with
      stable per-kind diagnostics.
- [ ] All numeric Cast runtime failures return
      `ExecutionError::Kernel` with the prefix `"cast
      failed: {Source} to {Target}: "`.
- [ ] Cast in WHERE evaluates as Toron residual filter;
      PBC declines. Identity Cast in WHERE also stays
      residual.
- [ ] Seven new ADR 025 architecture guards pass; two
      ADR 024 guards adjusted (one renamed).
- [ ] Plan digests for no-CAST queries are byte-identical
      pre/post-ADR-025.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
