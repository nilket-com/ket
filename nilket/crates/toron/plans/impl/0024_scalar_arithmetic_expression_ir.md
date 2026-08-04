# Implementation Plan: ADR 024 — Scalar Arithmetic Expression IR

**Companion ADR:**
`plans/adr/0024_scalar_arithmetic_expression_ir.md`

## Status

Promoted.

Seven-phase incremental migration. Each phase compiles green
and keeps the existing test suite green except where ADR
024's contract changes are intentional.

The contract is **additive at the SQL surface**: arithmetic
in SELECT (top-level and post-aggregate), HAVING, and WHERE
all becomes accepted; computed aggregate arguments stay
rejected via an explicit shape guard. The IR adds five
variants (Cast + four arithmetic) to both `Expr` and
`ExecutionExpr`; no new `ExecutionError` variant; no new
`TaskFailureKind` variant. `Cast` is binder-only and
widening-only.

**Target evaluator surface: 20 paths.** 4 widening Cast
evaluator paths + 16 same-type arithmetic kernels (4 ops ×
4 types). Binder injects Cast nodes for operand promotion;
arithmetic kernels see matched-type operands exclusively.

Implementation rules:

> Per-(op, output-type) explicit arithmetic kernels; no row-
> path trait dispatch. ADR 021's SoA-readiness invariants
> preserved; Cast IR strengthens JIT seam shape.

> Checked integer overflow returns `ExecutionError::Kernel`
> with stable message format `"arithmetic overflow:
> {Op}({Type})"`. ADR 020 SUM precedent extended.

> Integer zero divisor returns NULL; integer divide overflow
> remains checked overflow; float division-by-zero follows IEEE
> 754. Per-type policy matches the representation's value lattice.

> Aggregate-argument shape guard MUST land alongside or
> before the arithmetic lowering arms. Without it,
> `SUM(amount + tax)` accidentally starts binding once
> `lower_expr` learns arithmetic.

> Cast IR is binder-only and widening-only. SQL `CAST(x AS
> T)` rejects through the existing unsupported-expression
> path. Narrowing Cast rejects at bind time. Architecture
> guards codify.

## Goal

Land scalar arithmetic + binder-only widening Cast IR across
`ir.rs`, `ir/execution.rs`, `sql_planner.rs`, `binder.rs`,
`exec.rs`, `pbc.rs`, and the architecture / test suite
without:

- Adding new IR variants beyond five (Cast + four
  arithmetic ops).
- Exposing SQL `CAST(x AS T)` syntax.
- Introducing narrowing Cast.
- Adding a new `ExecutionError` variant.
- Adding a new `TaskFailureKind` variant.
- Changing `AggState`, `STABLE_AGGREGATE_KERNELS`,
  `AggregateOutputCatalog`, `AggregateOutputRule`,
  `GroupedAggregateSignature`, or any other ADR 021/022/023
  contract.
- Removing `AggState: Copy`.
- Introducing row-path trait dispatch.
- Changing `ExecutionPlan` digests for queries currently
  accepted (existing queries have no arithmetic; additive
  surface).
- Changing the Wendelon PBC bytecode or AGG protocol.
- Adding a fragmenter detector or recognition site.

## Substrate

### Modified files

- `crates/toron/src/ir.rs` — Phase 1.
- `crates/toron/src/ir/execution.rs` — Phase 1.
- `crates/toron/src/sql_planner.rs` — Phase 2 (lowering) +
  Phase 3 (aggregate-argument guard).
- `crates/toron/src/binder.rs` — Phase 4.
- `crates/toron/src/exec.rs` — Phase 5.
- `crates/toron/src/pbc.rs` (or wherever `PbcCompiler` lives)
  — Phase 6.
- `crates/toron/tests/architecture_test.rs` — Phase 7.
- `crates/toron/tests/group_by_test.rs` — Phase 7.
- `crates/toron/tests/non_aggregate_projection_accounting_test.rs`
  — Phase 7.
- `crates/toron/tests/aggregate_projection_accounting_test.rs`
  — Phase 7.
- `crates/toron/tests/distributed_process_test.rs` — Phase 7.

### New files

Optionally: `crates/toron/src/arithmetic.rs` or a private
module inside `exec.rs` for the 20 evaluator paths. Either
shape works; the touchpoint sweep below assumes inline in
`exec.rs`.

### Code-grounded touchpoint sweep

| Site | File:Line | Phase | Change |
|---|---|---|---|
| `Expr` enum | `ir.rs` (existing variants) | 1 | Add `Cast(Box<Expr>, DataType)`, `Add(Box<Expr>, Box<Expr>)`, `Sub(...)`, `Mul(...)`, `Div(...)` |
| `ExecutionExpr` enum | `ir/execution.rs` | 1 | Add five symmetric variants |
| `ExecutionExpr::data_type()` | `ir/execution.rs:135` | 1 | Cast arm returns target type; arithmetic arms return left.data_type() (same-type post-promotion) |
| `ExecutionExpr::nullable()` | `ir/execution.rs` | 1 | Cast: passthrough; arithmetic: either-operand-nullable. Integer Div: also nullable because /0 → NULL |
| `ExecutionExpr::pbc_eligible()` | `ir/execution.rs:178` | 1 | Five new arms returning `false` |
| `SqlPlanner::lower_expr` | `sql_planner.rs:1004` | 2 | Four `BinaryOperator` arms (Plus, Minus, Multiply, Divide); `SqlExpr::Cast` continues to reject |
| `SqlPlanner::lower_count_aggregate` | `sql_planner.rs:754` | 3 | Add aggregate-argument shape guard via `validate_aggregate_argument(arg, true)` |
| `SqlPlanner::lower_sum_aggregate` | `sql_planner.rs:772` | 3 | `validate_aggregate_argument(arg, false)` |
| `SqlPlanner::lower_min_aggregate` | sibling | 3 | Same |
| `SqlPlanner::lower_max_aggregate` | sibling | 3 | Same |
| `SqlPlanner::lower_avg_aggregate` | sibling | 3 | Same |
| `validate_aggregate_argument` | `sql_planner.rs` (new fn) | 3 | Rejects non-column arguments with diagnostic |
| `arithmetic_output_type` helper | `ir/execution.rs` or shared module | 4 | 16-cell lattice; shared between binder and `ExecutionExpr::data_type()` |
| `bind_expr` arithmetic arms | `binder.rs` | 4 | Four arms calling `bind_arithmetic` |
| `bind_expr` Cast arm | `binder.rs` | 4 | Validates widening-only; produces `ExecutionExpr::Cast` |
| `bind_arithmetic` helper | `binder.rs` (new fn) | 4 | Promotion lattice + Cast injection + nullability |
| Cast evaluator paths | `exec.rs` (4 functions) | 5 | `cast_int32_to_int64`, `cast_int32_to_float64`, `cast_int64_to_float64`, `cast_float32_to_float64` |
| Arithmetic kernel functions | `exec.rs` (16 functions) | 5 | 4 ops × 4 types; same-type inputs; checked overflow; integer /0 → NULL; integer divide overflow → Kernel; float /0 → IEEE 754 |
| Per-op dispatch helper | `exec.rs` (new fn) | 5 | `evaluate_arithmetic(op, left, right, output_type)` dispatches to kernels |
| `Executor::evaluate_expr` | `exec.rs:687` | 5 | Five arithmetic arms (unbound path) |
| `Executor::evaluate_bound_expr` | `exec.rs:833` | 5 | Five arithmetic arms (bound path) |
| `PbcCompiler::compile_expr` | `pbc.rs` (TBD line) | 6 | Five explicit decline branches for Cast + arithmetic variants |
| Architecture guards | `tests/architecture_test.rs` | 7 | Seven new guards |
| `group_by_test.rs` fixtures | `tests/group_by_test.rs` | 7 | ADR-023-deferred forms flip from reject to accept; aggregate-argument guard fixtures preserve reject with new diagnostic |
| `non_aggregate_projection_accounting_test.rs` | `tests/...` | 7 | New positive fixtures: `SELECT amount + id` etc. |
| `aggregate_projection_accounting_test.rs` | `tests/...` | 7 | Positive fixtures for post-aggregate arithmetic |
| `distributed_process_test.rs` composition | `tests/...` | 7 | One composition fixture across 2/4 workers |

## Phases

### Phase 1 — IR extension

**Goal.** Add five variants to `Expr` and `ExecutionExpr`;
extend `data_type()`, `nullable()`, `pbc_eligible()`
accordingly. Build green; existing tests pass (new variants
unreachable through any current code path).

**Tasks.**

1. In `ir.rs`, extend `Expr`:
   ```rust
   pub enum Expr {
       // ... existing variants ...
       Cast(Box<Expr>, DataType),
       Add(Box<Expr>, Box<Expr>),
       Sub(Box<Expr>, Box<Expr>),
       Mul(Box<Expr>, Box<Expr>),
       Div(Box<Expr>, Box<Expr>),
   }
   ```

2. In `ir/execution.rs`, extend `ExecutionExpr`:
   ```rust
   pub enum ExecutionExpr {
       // ... existing variants ...
       Cast(Box<ExecutionExpr>, DataType),
       Add(Box<ExecutionExpr>, Box<ExecutionExpr>),
       Sub(Box<ExecutionExpr>, Box<ExecutionExpr>),
       Mul(Box<ExecutionExpr>, Box<ExecutionExpr>),
       Div(Box<ExecutionExpr>, Box<ExecutionExpr>),
   }
   ```

3. Add the `arithmetic_output_type` helper in
   `ir/execution.rs` (shared with `binder.rs`):
   ```rust
   pub fn arithmetic_output_type(left: &DataType, right: &DataType) -> Option<DataType> {
       use DataType::*;
       match (left, right) {
           (Int32, Int32) => Some(Int32),
           (Int32, Int64) | (Int64, Int32) | (Int64, Int64) => Some(Int64),
           (Float32, Float32) => Some(Float32),
           (Int32 | Int64, Float32) | (Float32, Int32 | Int64) => Some(Float64),
           (Float64, Int32 | Int64 | Float32 | Float64)
           | (Int32 | Int64 | Float32, Float64) => Some(Float64),
           _ => None,
       }
   }
   ```

4. Extend `ExecutionExpr::data_type()` (`ir/execution.rs:135`):
   ```rust
   Self::Cast(_, target) => target.clone(),
   Self::Add(left, right)
   | Self::Sub(left, right)
   | Self::Mul(left, right)
   | Self::Div(left, right) => {
       debug_assert_eq!(left.data_type(), right.data_type());
       left.data_type()
   }
   ```

5. Extend `ExecutionExpr::nullable()`:
   ```rust
   Self::Cast(inner, _) => inner.nullable(),
   Self::Add(left, right)
   | Self::Sub(left, right)
   | Self::Mul(left, right) => left.nullable() || right.nullable(),
   Self::Div(left, right) => {
       let integer = matches!(
           left.data_type(),
           DataType::Int32 | DataType::Int64
       );
       left.nullable() || right.nullable() || integer
   }
   ```

6. Extend `ExecutionExpr::pbc_eligible()` (`ir/execution.rs:178`):
   ```rust
   Self::Cast(_, _)
   | Self::Add(_, _)
   | Self::Sub(_, _)
   | Self::Mul(_, _)
   | Self::Div(_, _) => false,
   ```

7. Run `cargo build -p toron`. Compiler error: existing
   `match expr` sites are non-exhaustive. Add
   `unreachable!()` arms for the new variants as Phase 1
   placeholders.

8. Run `cargo test -p toron`. Existing tests pass (new
   variants unreachable through any current SQL surface).

**Definition of Phase 1 done.**
- `Expr` and `ExecutionExpr` each have five new variants.
- `data_type()`, `nullable()`, `pbc_eligible()` extended.
- `unreachable!()` placeholders compile.
- Existing test suite green.

### Phase 2 — SQL lowering

**Goal.** Add four `BinaryOperator` arms in `lower_expr`.
Arithmetic SQL like `a + b` lowers to `Expr::Add(...)`.
Phase 2 alone is intentionally a broken state until Phase 3
lands the aggregate-argument guard; Phase 2 + Phase 3 must
bundle.

**Tasks.**

1. In `sql_planner.rs::lower_expr` (`sql_planner.rs:1004`),
   extend the `SqlExpr::BinaryOp` match:
   ```rust
   SqlExpr::BinaryOp { left, op, right } => {
       let left = self.lower_expr(left)?;
       let right = self.lower_expr(right)?;
       match op {
           // ... existing comparison/boolean arms ...
           BinaryOperator::Plus => Ok(Expr::Add(Box::new(left), Box::new(right))),
           BinaryOperator::Minus => Ok(Expr::Sub(Box::new(left), Box::new(right))),
           BinaryOperator::Multiply => Ok(Expr::Mul(Box::new(left), Box::new(right))),
           BinaryOperator::Divide => Ok(Expr::Div(Box::new(left), Box::new(right))),
           _ => Err(unsupported(/* existing reject for unknown ops */)),
       }
   }
   ```
   No `SqlExpr::Cast` arm — it continues to reject through
   the existing catch-all.

2. Run `cargo build -p toron`. Build green.

**Definition of Phase 2 done.**
- Four `BinaryOperator` arms present.
- Arithmetic SQL lowers to `Expr::Add` etc.
- `SqlExpr::Cast` still rejects.
- Phase 3 bundle-required.

### Phase 3 — Aggregate-argument shape guard

**Goal.** Critical guard. Preserve the existing column-only
aggregate argument invariant before Phase 2's arithmetic
lowering accidentally widens it.

**Tasks.**

1. Add `validate_aggregate_argument` helper in
   `sql_planner.rs`:
   ```rust
   fn validate_aggregate_argument(
       arg: &FunctionArg,
       allow_wildcard: bool,
   ) -> Result<&FunctionArg, FrontendError> {
       match arg {
           FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if allow_wildcard => Ok(arg),
           FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
               match expr {
                   SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => Ok(arg),
                   _ => Err(unsupported(
                       "aggregate arguments must be simple column references"
                   )),
               }
           }
           _ => Err(unsupported(
               "aggregate arguments must be simple column references"
           )),
       }
   }
   ```

2. Call `validate_aggregate_argument` in each
   `lower_*_aggregate` helper BEFORE `lower_arg`:
   - `lower_count_aggregate` (`sql_planner.rs:754`):
     `validate_aggregate_argument(arg, true)` — wildcard ok.
   - `lower_sum_aggregate` (`sql_planner.rs:772`):
     `validate_aggregate_argument(arg, false)`.
   - `lower_min_aggregate`, `lower_max_aggregate`,
     `lower_avg_aggregate`: same as SUM.

3. Run `cargo test -p toron`. Computed aggregate arguments
   reject with the new diagnostic.

**Definition of Phase 3 done.**
- Aggregate-argument shape guard rejects computed args.
- Phase 2 + Phase 3 bundle to land as one commit.

### Phase 4 — Binder Cast + arithmetic

**Goal.** `bind_expr` handles arithmetic via `bind_arithmetic`
helper; the helper computes the lattice common type, injects
Cast nodes for operand promotion. Cast arm validates
widening-only conversions.

**Tasks.**

1. Add `bind_arithmetic` helper in `binder.rs`:
   ```rust
   fn bind_arithmetic(
       input_schema: &SchemaRef,
       config: &PlannerConfig,
       left: &Expr,
       right: &Expr,
       constructor: fn(Box<ExecutionExpr>, Box<ExecutionExpr>) -> ExecutionExpr,
   ) -> Result<ExecutionExpr, BindingError> {
       let left = bind_expr(left, input_schema, config)?;
       let right = bind_expr(right, input_schema, config)?;
       let output_type = arithmetic_output_type(&left.data_type(), &right.data_type())
           .ok_or_else(|| BindingError::UnsupportedExpression {
               message: "arithmetic operands must be Int32, Int64, Float32, or Float64".to_string(),
           })?;
       let left = if left.data_type() != output_type {
           ExecutionExpr::Cast(Box::new(left), output_type.clone())
       } else {
           left
       };
       let right = if right.data_type() != output_type {
           ExecutionExpr::Cast(Box::new(right), output_type.clone())
       } else {
           right
       };
       Ok(constructor(Box::new(left), Box::new(right)))
   }
   ```

2. Replace `unreachable!()` placeholders in `bind_expr`:
   ```rust
   Expr::Add(left, right) => bind_arithmetic(
       input_schema, config, left, right, ExecutionExpr::Add,
   ),
   Expr::Sub(left, right) => bind_arithmetic(
       input_schema, config, left, right, ExecutionExpr::Sub,
   ),
   Expr::Mul(left, right) => bind_arithmetic(
       input_schema, config, left, right, ExecutionExpr::Mul,
   ),
   Expr::Div(left, right) => bind_arithmetic(
       input_schema, config, left, right, ExecutionExpr::Div,
   ),
   Expr::Cast(inner, target) => {
       let inner = bind_expr(inner, input_schema, config)?;
       let source = inner.data_type();
       let valid = matches!(
           (&source, target),
           (DataType::Int32, DataType::Int64)
           | (DataType::Int32, DataType::Float64)
           | (DataType::Int64, DataType::Float64)
           | (DataType::Float32, DataType::Float64)
       );
       if !valid {
           return Err(BindingError::UnsupportedExpression {
               message: format!("Cast from {source:?} to {target:?} is not a supported widening conversion"),
           });
       }
       Ok(ExecutionExpr::Cast(Box::new(inner), target.clone()))
   }
   ```

   Note: SQL CAST never produces `Expr::Cast`. The
   `Expr::Cast` arm exists for logical-IR symmetry and
   defense-in-depth if manually constructed plans reach the
   binder.

3. Run `cargo test -p toron`. Arithmetic binds; Cast nodes
   are visible in the bound `ExecutionExpr` tree.

**Definition of Phase 4 done.**
- `bind_arithmetic` helper present; four arithmetic arms in
  `bind_expr`.
- Cast arm validates widening-only conversions.
- Architecture guard
  `adr024_arithmetic_promotion_lattice_is_narrow` source-
  reads the lattice helper.

### Phase 5 — Executor evaluation

**Goal.** 4 Cast evaluator paths + 16 same-type arithmetic
kernels = 20 evaluator paths. Checked integer overflow;
integer /0 → NULL; integer divide overflow →
`ExecutionError::Kernel`; float /0 → IEEE 754. Both bound and
unbound executor paths covered.

**Tasks.**

1. Cast evaluator paths (4 functions):
   ```rust
   fn cast_int32_to_int64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let input = array.as_any().downcast_ref::<Int32Array>()
           .ok_or(/* internal type mismatch */)?;
       let result: Int64Array = input
           .iter()
           .map(|opt| opt.map(i64::from))
           .collect();
       Ok(Arc::new(result))
   }
   // Symmetric: cast_int32_to_float64, cast_int64_to_float64, cast_float32_to_float64
   ```

2. Arithmetic kernels (16 functions; same-type inputs after
   binder Cast injection):
   ```rust
   fn add_int32(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       let l = left.as_any().downcast_ref::<Int32Array>().ok_or(/* */)?;
       let r = right.as_any().downcast_ref::<Int32Array>().ok_or(/* */)?;
       let mut builder = Int32Array::builder(l.len());
       for i in 0..l.len() {
           if l.is_null(i) || r.is_null(i) {
               builder.append_null();
           } else {
               let result = l.value(i).checked_add(r.value(i))
                   .ok_or_else(|| ExecutionError::Kernel {
                       message: "arithmetic overflow: Add(Int32)".to_string(),
                   })?;
               builder.append_value(result);
           }
       }
       Ok(Arc::new(builder.finish()))
   }
   // Symmetric for sub_int32, mul_int32, div_int32
   // (explicit zero check, then checked_div).
   // Float kernels use direct +/-/*/ (IEEE 754).
   ```

3. Division kernels (special /0 handling):
   ```rust
   fn div_int64(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       // ... null check ...
       if r.value(i) == 0 {
           builder.append_null();
           continue;
       }
       match l.value(i).checked_div(r.value(i)) {
           Some(v) => builder.append_value(v),
           None => {
               return Err(ExecutionError::Kernel {
                   message: "arithmetic overflow: Div(Int64)".to_string(),
               });
           }
       }
   }

   fn div_float64(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
       // ... null check ...
       builder.append_value(l.value(i) / r.value(i));  // IEEE 754
   }
   ```

4. Per-op dispatch helper:
   ```rust
   fn evaluate_arithmetic(
       op: ArithmeticOp,
       left: &ArrayRef,
       right: &ArrayRef,
       output_type: &DataType,
   ) -> Result<ArrayRef, ExecutionError> {
       match (op, output_type) {
           (ArithmeticOp::Add, DataType::Int32) => add_int32(left, right),
           (ArithmeticOp::Add, DataType::Int64) => add_int64(left, right),
           (ArithmeticOp::Add, DataType::Float32) => add_float32(left, right),
           (ArithmeticOp::Add, DataType::Float64) => add_float64(left, right),
           // ... 12 more arms (Sub, Mul, Div × 4 types) ...
           _ => Err(ExecutionError::InvalidExpression {
               message: format!("ADR 024: unreachable (op, output_type) pair: {op:?}, {output_type:?}"),
           }),
       }
   }
   ```

5. Cast dispatch helper:
   ```rust
   fn evaluate_cast(
       array: &ArrayRef,
       target: &DataType,
   ) -> Result<ArrayRef, ExecutionError> {
       match (array.data_type(), target) {
           (DataType::Int32, DataType::Int64) => cast_int32_to_int64(array),
           (DataType::Int32, DataType::Float64) => cast_int32_to_float64(array),
           (DataType::Int64, DataType::Float64) => cast_int64_to_float64(array),
           (DataType::Float32, DataType::Float64) => cast_float32_to_float64(array),
           _ => Err(ExecutionError::InvalidExpression {
               message: format!("ADR 024: unreachable Cast: {:?} → {target:?}", array.data_type()),
           }),
       }
   }
   ```

6. Replace `unreachable!()` placeholders in
   `Executor::evaluate_expr` (`exec.rs:687`) and
   `Executor::evaluate_bound_expr` (`exec.rs:833`):
   ```rust
   ExecutionExpr::Cast(inner, target) => {
       let inner_array = self.evaluate_bound_expr(inner, batch)?;
       evaluate_cast(&inner_array, target)
   }
   ExecutionExpr::Add(left, right)
   | ExecutionExpr::Sub(left, right)
   | ExecutionExpr::Mul(left, right)
   | ExecutionExpr::Div(left, right) => {
       let left_array = self.evaluate_bound_expr(left, batch)?;
       let right_array = self.evaluate_bound_expr(right, batch)?;
       debug_assert_eq!(left_array.data_type(), right_array.data_type());
       evaluate_arithmetic(
           arithmetic_op_from_variant(expr),
           &left_array,
           &right_array,
           left.data_type(),
       )
   }
   ```

   The bound `ExecutionExpr` path is the canonical path. If
   the legacy unbound `Expr` path remains reachable for logical
   plan execution, mirror binder normalization at array
   evaluation time: evaluate both children, compute the common
   type from their array data types, run `evaluate_cast` on
   mismatched arrays, and then dispatch to the same 16 same-type
   kernels. Do not add mixed-type per-cell arithmetic kernels.

7. Run `cargo test -p toron`. All positive arithmetic
   fixtures bind + execute correctly; error fixtures
   produce expected results.

**Definition of Phase 5 done.**
- 16 arithmetic kernel functions + 4 Cast evaluator paths.
- Same-type operand handling (no per-cell type dispatch).
- Checked integer overflow returns `ExecutionError::Kernel`.
- Integer /0 → NULL; integer divide overflow →
  `ExecutionError::Kernel`; float /0 → IEEE 754.
- Both bound and unbound executor paths covered.

### Phase 6 — PBC compiler decline

**Goal.** PBC compiler explicitly declines arithmetic AND
Cast variants.

**Tasks.**

1. Locate `PbcCompiler::compile_expr`. Add five decline
   branches:
   ```rust
   ExecutionExpr::Cast(_, _)
   | ExecutionExpr::Add(_, _)
   | ExecutionExpr::Sub(_, _)
   | ExecutionExpr::Mul(_, _)
   | ExecutionExpr::Div(_, _) => Err(PbcCompileError::Unsupported {
       message: "ADR 024: arithmetic and Cast are not PBC-eligible".to_string(),
   }),
   ```

2. Verify the WHERE-with-arithmetic test path: filter falls
   through to Toron residual via existing
   `FilterDisposition::EvaluateInToron`.

3. Run `cargo test -p toron`. WHERE arithmetic succeeds via
   Toron residual.

**Definition of Phase 6 done.**
- PBC compiler explicitly declines arithmetic + Cast with
  stable error.
- Architecture guard
  `adr024_pbc_compiler_declines_arithmetic_and_cast`
  source-reads the decline arms.

### Phase 7 — Architecture guards + tests + bench

**Goal.** Seven new architecture guards; positive +
negative test fixture suite; distributed-process-test
composition fixture.

**Tasks.**

1. Architecture guards (`tests/architecture_test.rs`):
   - `expr_variant_count_includes_arithmetic_and_cast`:
     source-reads `ir.rs` for the five new variants.
   - `execution_expr_variant_count_includes_arithmetic_and_cast`:
     symmetric for `ir/execution.rs`.
   - `adr024_arithmetic_promotion_lattice_is_narrow`:
     source-reads `arithmetic_output_type` body for the
     16-cell table; absence of any cell or addition of
     out-of-lattice arms fails.
   - `adr024_arithmetic_in_sql_planner_lowers_to_ir`:
     source-reads `lower_expr` for the four
     `BinaryOperator::Plus/Minus/Multiply/Divide` arms.
   - `adr024_integer_overflow_returns_kernel_error`:
     source-reads kernel module for `checked_add`,
     `checked_sub`, `checked_mul`, `checked_div`, explicit
     zero-divisor handling before division, AND the stable
     `"arithmetic overflow: "` prefix.
   - `adr024_aggregate_arguments_remain_column_only`:
     source-reads `validate_aggregate_argument` body and the
     diagnostic.
   - `adr024_cast_is_binder_only_widening`: source-reads
     `sql_planner.rs::lower_expr` for absence of
     `SqlExpr::Cast` arm; source-reads `binder.rs::bind_expr`
     Cast arm for the four widening cases and the rejection
     branch.
   - `adr024_pbc_compiler_declines_arithmetic_and_cast`:
     source-reads PBC compiler for five decline branches.

2. Positive fixtures in `group_by_test.rs`:
   - The four ADR-023-deferred forms flip from reject to
     accept:
     ```sql
     SELECT SUM(amount) + 1 FROM sales
     SELECT region, SUM(amount) * 1.1 FROM sales GROUP BY region
     SELECT region, SUM(amount) + COUNT(*) FROM sales GROUP BY region
     SELECT amount + tax AS gross FROM sales
     ```
   - HAVING arithmetic:
     ```sql
     SELECT region FROM sales GROUP BY region HAVING SUM(amount) + COUNT(*) > 10
     SELECT region FROM sales GROUP BY region HAVING SUM(amount) / COUNT(*) > 10
     ```
   - Division semantics:
     ```sql
     SELECT SUM(amount) / COUNT(*) AS ratio FROM sales
     ```
   - Mixed-type lattice exercises:
     ```sql
     SELECT int32_col + int64_col FROM sales
     SELECT int32_col + float32_col FROM sales
     SELECT float32_col + float64_col FROM sales
     ```

3. Positive fixtures in
   `non_aggregate_projection_accounting_test.rs`:
   ```sql
   SELECT amount + id AS gross FROM sales
   SELECT amount - tax AS net FROM sales
   SELECT amount * 2 AS doubled FROM sales
   SELECT amount / 2 AS half FROM sales
   ```

4. Negative fixtures in `group_by_test.rs`:
   - Computed aggregate arguments still reject:
     ```sql
     SELECT SUM(amount + tax) FROM sales
     SELECT COUNT(amount + tax) FROM sales
     SELECT region, SUM(amount + tax) FROM sales GROUP BY region
     SELECT MIN(amount * 2) FROM sales
     SELECT AVG(price * quantity) FROM sales
     ```
   - Out-of-scope ops still reject:
     ```sql
     SELECT -amount FROM sales
     SELECT amount % 2 FROM sales
     SELECT CAST(amount AS Float64) FROM sales
     ```
   - Out-of-scope domains:
     ```sql
     SELECT region + amount FROM sales
     SELECT amount + 'x' FROM sales
     ```

5. Error-path fixtures:
   ```rust
   // Integer overflow
   assert_runtime_error(
       "SELECT 2147483647 + 1 FROM sales",
       "arithmetic overflow: Add(Int32)",
   );
   // Integer divide-by-zero → NULL
   let result = run_query("SELECT amount / 0 FROM sales").await;
   assert!(result.column(0).is_null(0));
   // Integer divide overflow → ExecutionError::Kernel
   // Use fixture columns because SQL unary negation is out of scope.
   assert_runtime_error(
       "SELECT min_i32 / neg_one FROM arithmetic_edges",
       "arithmetic overflow: Div(Int32)",
   );
   // Float divide-by-zero → IEEE 754
   let result = run_query("SELECT 1.0 / 0.0 FROM sales").await;
   assert!(result.column(0).value(0).is_infinite());
   ```

6. Distributed-process-test composition fixture:
   ```rust
   #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
   async fn adr024_arithmetic_post_aggregate_composition_is_stable_across_worker_counts() {
       let sql = "SELECT val AS k, SUM(val) + COUNT(*) AS score \
                   FROM fixture GROUP BY val \
                   HAVING SUM(val) > 0 \
                   ORDER BY score DESC LIMIT 5";
       let two_workers = run_process_query(sql, 2, 3_000, 0).await;
       let four_workers = run_process_query(sql, 4, 3_100, 1).await;
       assert_eq!(two_workers.row_count, 5);
       assert_eq!(four_workers.row_count, 5);
       assert_eq!(two_workers.digest, four_workers.digest);
   }
   ```

7. Run focused suites:
   ```sh
   cargo fmt --check
   cargo test -p toron --test architecture_test
   cargo test -p toron --test group_by_test
   cargo test -p toron --test non_aggregate_projection_accounting_test
   cargo test -p toron --test aggregate_projection_accounting_test
   cargo test -p toron --test sort_ordering_test
   cargo test -p toron --test distributed_process_test
   cargo test -p toron
   ```

**Definition of Phase 7 done.**
- Seven new architecture guards green.
- Existing ADR 015 / 020 / 022 / 023 guards green.
- Positive + negative + error fixture suites green.
- Distributed-process-test composition fixture green at 2 +
  4 workers.
- `cargo fmt --check` clean; `cargo test -p toron` green.

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Plan digest regression for queries currently accepted | Currently-accepted queries have no arithmetic; additive surface; no regression possible. |
| R2 | Phase 2 + Phase 3 land separately, breaking computed-aggregate-argument reject | Bundle Phase 2 + Phase 3 as one landing commit OR feature-branch them together. |
| R3 | Cast-injection logic produces Cast nodes for narrowing | Lattice ensures `output_type` is always at least as wide as either input; `bind_expr` Cast arm rejects narrowing if any future code attempts it. |
| R4 | `arithmetic_output_type` location creates circular dependency | Place in `ir/execution.rs` (or shared module); both binder and `data_type()` reference it. |
| R5 | Per-(op, type) kernel count (16 + 4 Cast = 20) inflates source | Acceptable. Future SoA / SIMD ADR can vectorize via Arrow kernels without changing dispatch shape. JIT-friendly. |
| R6 | Integer division `MIN_VALUE / -1` is accidentally treated like /0 | Explicit zero-divisor branch appends NULL before `checked_div`; `checked_div` returning `None` after that maps to `ExecutionError::Kernel`. Test fixture verifies both cases. |
| R7 | Cast IR enabling future SQL `CAST(x AS T)` without governance | Architecture guard `adr024_cast_is_binder_only_widening` asserts no SQL Cast lowering site. Any future SQL CAST ADR explicitly authorizes. |
| R8 | Existing tests hitting `lower_expr`'s unsupported-expression catch-all for arithmetic now succeed unexpectedly | Phase 7 fixture updates explicit; grep for "unsupported" before commit. |
| R9 | `validate_aggregate_argument` accidentally rejects `COUNT(*)` | Phase 3 explicitly allows `Wildcard` when `allow_wildcard == true` for COUNT. Test fixture verifies. |
| R10 | Float32 inputs to integer-output kernels (unreachable per lattice) reach a kernel | The unreachable `(op, output_type)` arm in `evaluate_arithmetic` returns `ExecutionError::InvalidExpression`. Same for Cast. Architecture guard pins the lattice. |
| R11 | New IR variants break serde wire compatibility | `Expr` is coordinator-internal; `ExecutionExpr` is wire-serialized in `ExecutionPlan` task payload. New variants are additive; ADR 003's reject-on-mismatch posture handles single-workspace upgrade. |
| R12 | `pbc_eligible()` returning false for Cast accidentally affects column-only sub-expressions inside Cast | `pbc_eligible()` is transitive AND for composite expressions; arithmetic and Cast force the whole expression non-eligible. Matches existing behavior. |
| R13 | Cast IR proliferation in future ADRs (SQL CAST, CASE, COALESCE) | Future ADRs reuse the Cast substrate cleanly; ADR 024's binder-only widening doesn't constrain future surface. |
| R14 | `ExecutionExpr::nullable()` for integer Div uses run-time type check | Acceptable; type-check is O(1) and SoA-friendly. |
| R15 | The 20 evaluator paths inflate module file | Acceptable; co-locate in `exec.rs` or new `arithmetic.rs` module. Future SoA / Arrow refactor compresses per-type loops. |
| R16 | Division output type dissent (Codex iter-2 preferred always-Float64) re-surfaces | 3/3 by iter-4 ratified per-lattice; Codex iter-3/4 retracted the dissent. Documented in ADR §Recorded dissent. |

## Test plan

Existing tests that must stay green:

- `tests/architecture_test.rs` — adjusted + 7 new guards.
- `tests/group_by_test.rs` — extended positive + negative
  suite; ADR-023-deferred fixtures flip from reject to
  accept.
- `tests/non_aggregate_projection_accounting_test.rs` —
  positive fixtures for top-level arithmetic.
- `tests/aggregate_projection_accounting_test.rs` —
  positive fixtures for post-aggregate arithmetic.
- `tests/sort_ordering_test.rs` — preserved (ADR 010
  output-schema-only sort key rule unchanged).
- `tests/distributed_process_test.rs` — 1 new composition
  fixture.
- `tests/clause_accounting_test.rs` — preserved.
- `tests/wendelon_storage_execution_test.rs` — preserved.

New test files: none. Consolidated in existing files.

Bench target: optional; no new bench rows required.

## Definition of done

The migration lands when:

1. `Expr` and `ExecutionExpr` each have five new variants.
2. `ExecutionExpr::data_type()` returns the cast target /
   lattice output; `nullable()` propagates correctly;
   `pbc_eligible()` returns false for all five.
3. `sql_planner.rs::lower_expr` has four `BinaryOperator`
   arms; `SqlExpr::Cast` continues to reject.
4. Aggregate-argument shape guard rejects computed args.
5. `binder.rs::bind_arithmetic` helper applies the 16-cell
   lattice; injects Cast nodes for operand promotion. Cast
   arm validates widening-only.
6. `exec.rs`: 4 Cast evaluator paths + 16 same-type
   arithmetic kernels (20 paths total).
7. `PbcCompiler::compile_expr` declines arithmetic AND Cast
   explicitly.
8. Seven new architecture guards green.
9. Surface matrix tested.
10. Distributed-process-test composition fixture green at 2
    + 4 workers.
11. Plan digests for currently-accepted queries byte-
    identical.
12. `cargo fmt --check` clean; `cargo test -p toron` green.

## Phase ordering rationale

Phase 1 (IR extension) lands first as the substrate. Phases
2 + 3 must bundle: Phase 2 adds arithmetic lowering which
makes `SUM(amount + tax)` reach the aggregate binder
successfully; Phase 3 adds the column-only aggregate-
argument guard that preserves the existing reject. Splitting
creates an intermediate state where computed aggregate
arguments accidentally bind.

Phase 4 (binder arithmetic + Cast) is independent — can land
after Phase 3 bundle. Phase 5 (executor evaluation) requires
Phase 4. Phase 6 (PBC compiler decline) can land anytime
after Phase 1. Phase 7 (architecture guards + tests) is the
final correctness gate.

Bundle structure:
- **Bundle 1: Phase 1.** IR extension. Compile-only.
- **Bundle 2: Phase 2 + Phase 3.** Lowering + aggregate-
  argument guard. Critical bundle.
- **Bundle 3: Phase 4 + Phase 5 + Phase 6.** Binder +
  executor + PBC. End-to-end arithmetic surface.
- **Bundle 4: Phase 7.** Architecture guards + tests + bench.

Each bundle lands as a separate commit on the same branch /
landing PR.

## Migration command sequence

```sh
# Bundle 1: IR extension
git checkout -b adr-024-bundle-1
# edit ir.rs, ir/execution.rs; add 5 variants + arithmetic_output_type helper
# add unreachable!() placeholders in sql_planner.rs, binder.rs, exec.rs
cargo build -p toron
cargo test -p toron
git commit -m "feat: adr 024 phase 1 — Expr/ExecutionExpr cast + arithmetic variants"

# Bundle 2: Lowering + aggregate-argument guard
git checkout -b adr-024-bundle-2
# edit sql_planner.rs: lower_expr arithmetic arms + validate_aggregate_argument
cargo build -p toron
cargo test -p toron
git commit -m "feat: adr 024 phase 2-3 — SQL arithmetic lowering + aggregate-argument guard"

# Bundle 3: Binder + executor + PBC
git checkout -b adr-024-bundle-3
# edit binder.rs, exec.rs, pbc.rs
cargo build -p toron
cargo test -p toron
git commit -m "feat: adr 024 phase 4-6 — binder cast injection + executor + PBC"

# Bundle 4: Architecture guards + tests + bench
git checkout -b adr-024-bundle-4
# edit tests/architecture_test.rs + test fixture files
cargo fmt --check
cargo test -p toron
git commit -m "feat: adr 024 phase 7 — architecture guards + surface matrix tests"
```

Bundles land as separate commits within the same branch /
landing PR.
