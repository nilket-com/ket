# Implementation Plan: ADR 028 — Computed Aggregate Arguments

**Companion ADR:** `plans/adr/028_computed_aggregate_arguments.md`

## Goal

Lift `validate_aggregate_argument`'s column-only catch-
all reject for the stable aggregate kernel family
(`CountAll` plus one-arg `Sum`, `Count`, `Avg`, `Min`,
`Max`), allowing
computed `Expr` arguments to flow through aggregate calls
while preserving:

- ADR 022 sticky-NaN policy.
- ADR 024 arithmetic loud-failure / D8 div/0 null.
- ADR 025 strict Cast `cast_failed` Kernel error.
- ADR 026 TryCast fail-to-null + D11 inner-Cast-error
  propagation.
- ADR 027 hidden-projection mechanism.
- PBC ineligibility for arithmetic / Cast / TryCast
  (computed-arg aggregates stay Toron-residual).
- Plan-digest byte-identity for column-only-argument
  queries.
- Experimental kernels' exclusion from grouped aggregate
  signature matching through `stable_kernel_for_function`
  returning `None`.

No new IR variant; no new operator; no new kernel; no
fragmenter detector change; no PBC opcode change; no
cross-ring engagement; no `AggState` shape change; no
hidden pre-aggregate `Project` node; **no parallel key
types**; no public `Eq + Hash` on `AggregateFunction` /
`Expr` / `ScalarValue`.

Seven-phase rollout: baseline → signature visitors →
SQL-walk validator → wire into catalog + lowering →
row-layer / binder preservation audit → architecture
guards → fixtures + final verification.

## Touchpoint sweep (code-grounded, current `main`)

| File | Anchor | Phase |
|---|---|---|
| `crates/toron/src/sql_planner.rs::validate_aggregate_argument` (~line 38) | replace with `reject_nested_aggregate(sql_expr)` wrapper | 2 |
| `crates/toron/src/sql_planner.rs::validate_simple_aggregate_arg` (~line 31) | preserve `FunctionArg` adapter | 2 (no-op) |
| `crates/toron/src/sql_planner.rs::reject_nested_aggregate` | NEW SQL-walk-time detector | 2 |
| `crates/toron/src/sql_planner.rs::aggregate_function_name` | reuse existing name-resolution helper for nested aggregate detection | 2 |
| `crates/toron/src/sql_planner.rs::GroupedAggregateSignature` (~line 1613) | replace finite enum with struct `{ kernel: StableAggregateKernel, args: Vec<Expr> }` + custom `PartialEq` / `Eq` / `Hash` impls | 1 |
| `crates/toron/src/sql_planner.rs::hash_expr_structural` | NEW exhaustive visitor | 1 |
| `crates/toron/src/sql_planner.rs::expr_structural_eq` | NEW exhaustive pair-symmetric visitor | 1 |
| `crates/toron/src/sql_planner.rs::hash_scalar_structural` | NEW visitor — bit-pattern for float arms | 1 |
| `crates/toron/src/sql_planner.rs::scalar_structural_eq` | NEW visitor — bit-pattern compare for float arms | 1 |
| `crates/toron/src/sql_planner.rs::hash_data_type_structural` | NEW visitor — primitive arms + `format!("{other:?}")` fallback | 1 |
| `crates/toron/src/sql_planner.rs::data_type_structural_eq` | NEW — `left == right` reflexive (P3 correctness) | 1 |
| `crates/toron/src/aggregate_kernel.rs::stable_kernel_for_function` | preserve existing stable-kernel mapping — `None` for experimental | 1 |
| `crates/toron/src/sql_planner.rs::grouped_aggregate_signature` (~line 1962) | rewrite via `stable_kernel_for_function` + arity-check; `AggregateArity::Zero` produces `args: Vec::new()` regardless of input | 3 |
| `crates/toron/src/sql_planner.rs::lower_count_aggregate` (~line 833) | **preserve verbatim** — synthetic `Expr::column("count")` placeholder NOT modified | 3 |
| `crates/toron/src/sql_planner.rs::lower_sum_aggregate` / `lower_min_aggregate` / `lower_max_aggregate` / `lower_avg_aggregate` | preserve shape; relaxed validator accepts computed args | 3 |
| `crates/toron/src/sql_planner.rs::AggregateOutputCatalog::resolve_for_*` (~line 1437) | preserve — `&GroupedAggregateSignature` comparison via custom impls composes unchanged | 3 (no-op verify) |
| `crates/toron/src/sql_planner.rs::aggregate_output_name` | preserve — kernel-name-only | 3 (no-op verify) |
| `crates/toron/src/binder.rs` | preservation audit | 4 (no-op) |
| `crates/toron/src/exec.rs` | preservation audit | 4 (no-op) |
| `crates/toron/tests/architecture_test.rs` | 11 NEW ADR 028 guards + 2 adjusted guards | 5 |
| `crates/toron/tests/group_by_test.rs` | positive + negative + matching/ambiguity + regression + plan-digest + compositional fixtures | 6 |

## Phase 0 — Baseline Confirmation

Before edits, confirm starting state via grep:

```sh
rg -n "validate_aggregate_argument|validate_simple_aggregate_arg|GroupedAggregateSignature|grouped_aggregate_signature|stable_kernel_for_function|lower_count_aggregate|lower_sum_aggregate|aggregate_output_name" crates/toron/src/sql_planner.rs
rg -n "evaluate_expr\(&args\[0\]|evaluate_bound_expr\(&call\.args\[0\]" crates/toron/src/exec.rs | head -20
rg -n "bind_aggregate_call|bind_stable_aggregate_call|stable_output_data_type" crates/toron/src/binder.rs
rg -n "AggregateArity|StableAggregateKernel" crates/toron/src/aggregate_kernel.rs
rg -n "GroupedAggregateSignature::(Sum|Count|Min|Max|Avg|CountAll)" crates/toron/src/ crates/toron/tests/
```

Verify:

- `StableAggregateKernel` already derives `Eq + Hash`.
- `AggregateArity` (`Zero` / `One`) exists in catalog
  metadata.
- `validate_aggregate_argument` currently rejects with
  column-only diagnostic.
- `lower_count_aggregate` synthesizes `Expr::column("count")`
  for `COUNT(*)`.
- `bind_aggregate_call` strips `CountAll` bound args to
  `Vec::new()`.
- Aggregate execution uses
  `self.evaluate_expr(&args[0], batch)?` /
  `self.evaluate_bound_expr(&call.args[0], batch)?`.
- `AggregateOutputCatalog` stores
  `entries: Vec<AggregateOutputEntry>` (linear scan).

**Preserve verbatim:**

- `StableAggregateKernel` rows + `#[repr(u8)]`;
  `AggState` shape.
- `AggregateOutputCatalog::natural_output_names`.
- `bind_expr` / `evaluate_expr` / `evaluate_bound_expr`
  aggregate-argument paths.
- `aggregate_output_name(&function)` return values
  (`"sum"` / `"count"` / `"min"` / `"max"` / `"avg"`).
- ADR 022 sticky-NaN; ADR 024 arithmetic Kernel-error;
  ADR 025 / 026 Cast / TryCast policies.
- ADR 027 hidden-projection mechanism.
- ADR 023 HAVING-alias / hidden-aggregate-key /
  qualified-reference rejects.
- `lower_count_aggregate`'s synthetic placeholder.

## Phase 1 — Signature Visitors

**Goal.** Add `GroupedAggregateSignature` struct +
custom `PartialEq + Eq + Hash` impls + 6 visitor helper
functions. No new types beyond the struct itself.

**Files:** `crates/toron/src/sql_planner.rs`

**Tasks:**

1. **Replace `GroupedAggregateSignature` enum** with
   struct + custom impls:

   ```rust
   use std::hash::{Hash, Hasher};

   #[derive(Debug, Clone)]
   struct GroupedAggregateSignature {
       kernel: StableAggregateKernel,
       args: Vec<Expr>,
   }

   impl PartialEq for GroupedAggregateSignature {
       fn eq(&self, other: &Self) -> bool {
           self.kernel == other.kernel
               && self.args.len() == other.args.len()
               && self.args.iter().zip(&other.args)
                   .all(|(a, b)| expr_structural_eq(a, b))
       }
   }

   impl Eq for GroupedAggregateSignature {}

   impl Hash for GroupedAggregateSignature {
       fn hash<H: Hasher>(&self, state: &mut H) {
           self.kernel.hash(state);
           // P4: length-hash prevents prefix collisions
           self.args.len().hash(state);
           for arg in &self.args {
               hash_expr_structural(arg, state);
           }
       }
   }
   ```

2. **Add `hash_expr_structural` visitor** (exhaustive
   over current `Expr`):

   ```rust
   fn hash_expr_structural(expr: &Expr, state: &mut impl Hasher) {
       match expr {
           Expr::Column(c) => { 0u8.hash(state); c.hash(state); }
           Expr::Literal(s) => { 1u8.hash(state); hash_scalar_structural(s, state); }
           Expr::Cast(inner, ty) => {
               2u8.hash(state);
               hash_expr_structural(inner, state);
               hash_data_type_structural(ty, state);
           }
           Expr::TryCast(inner, ty) => {
               3u8.hash(state);
               hash_expr_structural(inner, state);
               hash_data_type_structural(ty, state);
           }
           Expr::Add(l, r) => { 4u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Sub(l, r) => { 5u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Mul(l, r) => { 6u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Div(l, r) => { 7u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Eq(l, r)  => { 8u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Neq(l, r) => { 9u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Lt(l, r)  => { 10u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Lte(l, r) => { 11u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Gt(l, r)  => { 12u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Gte(l, r) => { 13u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::And(l, r) => { 14u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Or(l, r)  => { 15u8.hash(state); hash_expr_structural(l, state); hash_expr_structural(r, state); }
           Expr::Not(inner) => { 16u8.hash(state); hash_expr_structural(inner, state); }
           Expr::IsNull(inner) => { 17u8.hash(state); hash_expr_structural(inner, state); }
           Expr::IsNotNull(inner) => { 18u8.hash(state); hash_expr_structural(inner, state); }
           Expr::TimeBucket(dur, inner) => {
               19u8.hash(state);
               dur.hash(state);
               hash_expr_structural(inner, state);
           }
       }
   }
   ```

   Discriminator bytes (0-19) prevent collisions across
   variants. Match exhaustiveness enforced at compile
   time.

3. **Add `expr_structural_eq` visitor** (exhaustive,
   pair-symmetric):

   ```rust
   fn expr_structural_eq(a: &Expr, b: &Expr) -> bool {
       match (a, b) {
           (Expr::Column(x), Expr::Column(y)) => x == y,
           (Expr::Literal(x), Expr::Literal(y)) => scalar_structural_eq(x, y),
           (Expr::Cast(xi, xt), Expr::Cast(yi, yt))
           | (Expr::TryCast(xi, xt), Expr::TryCast(yi, yt)) => {
               expr_structural_eq(xi, yi) && data_type_structural_eq(xt, yt)
           }
           (Expr::Add(xl, xr), Expr::Add(yl, yr))
           | (Expr::Sub(xl, xr), Expr::Sub(yl, yr))
           | (Expr::Mul(xl, xr), Expr::Mul(yl, yr))
           | (Expr::Div(xl, xr), Expr::Div(yl, yr))
           | (Expr::Eq(xl, xr), Expr::Eq(yl, yr))
           | (Expr::Neq(xl, xr), Expr::Neq(yl, yr))
           | (Expr::Lt(xl, xr), Expr::Lt(yl, yr))
           | (Expr::Lte(xl, xr), Expr::Lte(yl, yr))
           | (Expr::Gt(xl, xr), Expr::Gt(yl, yr))
           | (Expr::Gte(xl, xr), Expr::Gte(yl, yr))
           | (Expr::And(xl, xr), Expr::And(yl, yr))
           | (Expr::Or(xl, xr), Expr::Or(yl, yr)) => {
               expr_structural_eq(xl, yl) && expr_structural_eq(xr, yr)
           }
           (Expr::Not(xi), Expr::Not(yi))
           | (Expr::IsNull(xi), Expr::IsNull(yi))
           | (Expr::IsNotNull(xi), Expr::IsNotNull(yi)) => {
               expr_structural_eq(xi, yi)
           }
           (Expr::TimeBucket(xd, xi), Expr::TimeBucket(yd, yi)) => {
               xd == yd && expr_structural_eq(xi, yi)
           }
           _ => false,
       }
   }
   ```

4. **Add `hash_scalar_structural` + `scalar_structural_eq`**
   helpers — bit-pattern for `Float32` / `Float64`:

   ```rust
   fn hash_scalar_structural(value: &ScalarValue, state: &mut impl Hasher) {
       match value {
           ScalarValue::Boolean(b) => { 0u8.hash(state); b.hash(state); }
           ScalarValue::Int32(i)   => { 1u8.hash(state); i.hash(state); }
           ScalarValue::Int64(i)   => { 2u8.hash(state); i.hash(state); }
           ScalarValue::Float32(f) => {
               3u8.hash(state);
               f.map(f32::to_bits).hash(state);
           }
           ScalarValue::Float64(f) => {
               4u8.hash(state);
               f.map(f64::to_bits).hash(state);
           }
           ScalarValue::Utf8(s) => { 5u8.hash(state); s.hash(state); }
           // ... exhaustive over current ScalarValue arms
       }
   }

   fn scalar_structural_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
       match (a, b) {
           (ScalarValue::Float32(x), ScalarValue::Float32(y)) => {
               x.map(f32::to_bits) == y.map(f32::to_bits)
           }
           (ScalarValue::Float64(x), ScalarValue::Float64(y)) => {
               x.map(f64::to_bits) == y.map(f64::to_bits)
           }
           (ScalarValue::Boolean(x), ScalarValue::Boolean(y)) => x == y,
           (ScalarValue::Int32(x), ScalarValue::Int32(y))     => x == y,
           (ScalarValue::Int64(x), ScalarValue::Int64(y))     => x == y,
           (ScalarValue::Utf8(x), ScalarValue::Utf8(y))       => x == y,
           // ... exhaustive ...
           _ => false,
       }
   }
   ```

5. **Add `data_type_structural_eq` + `hash_data_type_structural`**
   — **P3 reflexivity correctness:**

   ```rust
   // P3 correctness: reflexive equality via Arrow's PartialEq.
   // DO NOT use `matches!` with arm restrictions — would violate
   // Eq reflexivity for hand-built non-lattice DataType values.
   fn data_type_structural_eq(left: &DataType, right: &DataType) -> bool {
       left == right
   }

   fn hash_data_type_structural(data_type: &DataType, state: &mut impl Hasher) {
       match data_type {
           DataType::Int32   => 0u8.hash(state),
           DataType::Int64   => 1u8.hash(state),
           DataType::Float32 => 2u8.hash(state),
           DataType::Float64 => 3u8.hash(state),
           other => {
               // Defensive fallback for hand-built non-lattice values
               // (keeps Hash consistent with the reflexive Eq).
               255u8.hash(state);
               format!("{other:?}").hash(state);
           }
       }
   }
   ```

6. **Use the existing aggregate-kernel mapping**
   (`aggregate_kernel::stable_kernel_for_function`):

   ```rust
   // Already in aggregate_kernel.rs; preserve this boundary.
   pub(crate) fn stable_kernel_for_function(
       function: &AggregateFunction,
   ) -> Option<StableAggregateKernel>
   ```

   The grouped signature path should call the catalog
   helper rather than adding a second local mapping in
   `sql_planner.rs`.

**Verification:**

```sh
cargo check -p toron
```

Expected: compile errors at every existing
`GroupedAggregateSignature::CountAll` / `Sum(ColumnRef)`
/ etc. constructor and matcher site (Phase 3 fixes
these).

## Phase 2 — SQL-Walk Validator (D5)

**Goal.** Lift column-only reject; add nested-aggregate
detection at SQL-walk time.

**Files:** `crates/toron/src/sql_planner.rs`

**Tasks:**

1. **Reuse the existing aggregate-function name resolver.**
   `sql_planner.rs` already has `aggregate_function_name`;
   use it for nested aggregate detection to preserve the
   current one-part-name policy and stable / experimental
   name catalog.

2. **Add `reject_nested_aggregate`** walker:

   ```rust
   fn reject_nested_aggregate(expr: &SqlExpr) -> Result<(), FrontendError> {
       match expr {
           SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
               Err(unsupported(
                   "aggregate arguments must not contain aggregate calls",
               ))
           }
           SqlExpr::BinaryOp { left, right, .. } => {
               reject_nested_aggregate(left)?;
               reject_nested_aggregate(right)
           }
           SqlExpr::UnaryOp { expr, .. } => reject_nested_aggregate(expr),
           SqlExpr::Cast { expr, .. } => reject_nested_aggregate(expr),
           SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => {
               reject_nested_aggregate(inner)
           }
           SqlExpr::Nested(inner) => reject_nested_aggregate(inner),
           SqlExpr::Function(function) => {
               if let FunctionArguments::List(list) = &function.args {
                   for arg in &list.args {
                       match arg {
                           FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))
                           | FunctionArg::Named {
                               arg: FunctionArgExpr::Expr(arg_expr),
                               ..
                           }
                           | FunctionArg::ExprNamed {
                               arg: FunctionArgExpr::Expr(arg_expr),
                               ..
                           } => reject_nested_aggregate(arg_expr)?,
                           FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                           | FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {}
                           _ => {}
                       }
                   }
               }
               Ok(())
           }
           _ => Ok(()),
       }
   }
   ```

3. **Rewrite `validate_aggregate_argument`:**

   ```rust
   fn validate_aggregate_argument(expr: &SqlExpr) -> Result<(), FrontendError> {
       reject_nested_aggregate(expr)?;
       Ok(())
   }
   ```

4. **Drop the column-only diagnostic** string from
   `sql_planner.rs`.

5. **Preserve `validate_simple_aggregate_arg`**
   adapter — routes `FunctionArg` →
   `validate_aggregate_argument`.

**Verification:** `cargo check -p toron`.

## Phase 3 — Wire New Signature into Catalog + Lowering

**Goal.** Update `grouped_aggregate_signature`, audit
per-kernel lowering helpers, audit
`AggregateOutputCatalog` resolver.

**Files:** `crates/toron/src/sql_planner.rs`

**Tasks:**

1. **Rewrite `grouped_aggregate_signature`:**

   ```rust
   fn grouped_aggregate_signature(
       function: &AggregateFunction,
       args: &[Expr],
   ) -> Option<GroupedAggregateSignature> {
       let kernel = aggregate_kernel::stable_kernel_for_function(function)?;
       let spec = aggregate_kernel::stable_spec(kernel);
       if !spec.grouped_supported {
           return None;
       }
       match spec.arity {
           AggregateArity::Zero => Some(GroupedAggregateSignature {
               kernel,
               args: Vec::new(),  // ignore input args (D6)
           }),
           AggregateArity::One if args.len() == 1 => {
               Some(GroupedAggregateSignature {
                   kernel,
                   args: vec![args[0].clone()],
               })
           }
           _ => None,
       }
   }
   ```

   **Critical:** `AggregateArity::Zero` produces
   `args: Vec::new()` regardless of input — decouples
   `CountAll`'s signature from `lower_count_aggregate`'s
   synthetic placeholder.

2. **Preserve `lower_count_aggregate` verbatim.**
   `COUNT(*)`'s synthetic `Expr::column("count")`
   placeholder stays in the `LogicalPlan` shape (D6 +
   D10).

3. **Preserve `lower_sum_aggregate` / `lower_min_aggregate`
   / `lower_max_aggregate` / `lower_avg_aggregate`**
   shape — they still call
   `validate_simple_aggregate_arg(&args[0])?`, which
   routes through the relaxed validator.

4. **Audit `AggregateOutputCatalog::resolve_for_*`:**
   - `resolve_for_projection` / `resolve_for_having` /
     `resolve_for_order_by` / `resolve_natural` already
     compare `&GroupedAggregateSignature` via `==`.
   - Custom `impl PartialEq` composes unchanged.
   - Confirm no `match` on old `Sum(ColumnRef)` / etc.
     enum variants exists in resolver code.

5. **Verify `aggregate_output_name`** returns kernel-
   name-only strings; no `format!`.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron --test architecture_test  # expect failures until Phase 5
```

## Phase 4 — Row-Layer + Binder Preservation Audit

**Goal.** Verify the existing aggregate-argument paths
handle computed arguments correctly. **No code change
expected.**

**Files:** `crates/toron/src/binder.rs`,
`crates/toron/src/exec.rs`

**Tasks:**

1. **Binder audit:**
   - `bind_aggregate_call` / `bind_stable_aggregate_call`
     calls `bind_expr` per `Expr` argument.
   - `bind_expr` handles full ADR 024 / 025 / 026 / 027
     `Expr` surface.
   - Per-kernel type-domain validation (D3) compares
     bound argument `data_type()` against kernel input
     domain.
   - `bind_aggregate_call` strips `CountAll` bound args
     to `Vec::new()`.

2. **Executor audit:**
   - All aggregate-kernel paths use
     `self.evaluate_expr(&args[0], batch)?` /
     `self.evaluate_bound_expr(&call.args[0], batch)?`.
   - Evaluator handles full `Expr` / `ExecutionExpr`
     surface.

3. **No code change in Phase 4.** Pure audit.

## Phase 5 — Architecture Guards (11 new + 2 adjusted)

**Files:** `crates/toron/tests/architecture_test.rs`

**Tasks:**

1. **Adjust ADR 024 D6 column-only-rule guard:**
   - Drop column-only sub-assertion.
   - Add positive absence assertion for the column-only
     diagnostic string.

2. **Adjust ADR 020 AVG signature-shape guard:**
   - Stop looking for removed `GroupedAggregateSignature::Avg(ColumnRef)`.
   - Pin `StableAggregateKernel::Avg` + natural output
     name `"avg"`.

3. **Add `adr028_aggregate_arguments_accept_computed_expressions`:**

   ```rust
   #[test]
   fn adr028_aggregate_arguments_accept_computed_expressions() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");
       assert!(
           !planner.contains("aggregate arguments must be simple column references"),
           "ADR 024 D6 column-only diagnostic must be removed (ADR 028 D1)"
       );
       assert!(planner.contains(
           "aggregate arguments must not contain aggregate calls"
       ));
       let validator = brace_blocks_after_marker(&planner, "fn validate_aggregate_argument")
           .into_iter().next().expect("validate_aggregate_argument body").1;
       assert!(validator.contains("reject_nested_aggregate"));
   }
   ```

4. **Add `adr028_aggregate_signature_uses_planner_private_visitors`:**

   ```rust
   #[test]
   fn adr028_aggregate_signature_uses_planner_private_visitors() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");

       let sig_body = brace_blocks_after_marker(&planner, "struct GroupedAggregateSignature")
           .into_iter().next().expect("GroupedAggregateSignature body").1;
       assert!(sig_body.contains("kernel: StableAggregateKernel"));
       assert!(sig_body.contains("args: Vec<Expr>"));

       assert!(planner.contains("impl PartialEq for GroupedAggregateSignature"));
       assert!(planner.contains("impl Hash for GroupedAggregateSignature"));

       // P4: length-hash present
       let hash_impl = brace_blocks_after_marker(&planner, "impl Hash for GroupedAggregateSignature")
           .into_iter().next().expect("Hash impl body").1;
       assert!(
           hash_impl.contains("self.args.len().hash(state)"),
           "P4: signature Hash must include args.len() length-hash"
       );

       // 6 visitor helpers present
       assert!(planner.contains("fn hash_expr_structural"));
       assert!(planner.contains("fn expr_structural_eq"));
       assert!(planner.contains("fn hash_scalar_structural"));
       assert!(planner.contains("fn scalar_structural_eq"));
       assert!(planner.contains("fn hash_data_type_structural"));
       assert!(planner.contains("fn data_type_structural_eq"));

       // No parallel key types (iter-4 refinement boundary)
       assert!(!planner.contains("enum AggregateArgKey"));
       assert!(!planner.contains("enum ScalarKey"));
       assert!(!planner.contains("enum DataTypeKey"));
       assert!(!planner.contains("DurationKey"));
   }
   ```

5. **Add `adr028_aggregate_signature_visitors_are_exhaustive`**
   (P4 dual coverage):

   ```rust
   #[test]
   fn adr028_aggregate_signature_visitors_are_exhaustive() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");

       let hash_body = brace_blocks_after_marker(&planner, "fn hash_expr_structural")
           .into_iter().next().expect("hash_expr_structural body").1;
       let eq_body = brace_blocks_after_marker(&planner, "fn expr_structural_eq")
           .into_iter().next().expect("expr_structural_eq body").1;

       for arm in ["Expr::Column", "Expr::Literal", "Expr::Cast", "Expr::TryCast"] {
           assert!(hash_body.contains(arm), "hash visitor missing {arm}");
           assert!(eq_body.contains(arm), "eq visitor missing {arm}");
       }
       for op in ["Add", "Sub", "Mul", "Div"] {
           let arm = format!("Expr::{op}");
           assert!(hash_body.contains(&arm), "hash visitor missing {arm}");
           assert!(eq_body.contains(&arm), "eq visitor missing {arm}");
       }
       for op in ["Eq", "Neq", "Lt", "Lte", "Gt", "Gte", "And", "Or"] {
           let arm = format!("Expr::{op}");
           assert!(hash_body.contains(&arm), "hash visitor missing {arm}");
           assert!(eq_body.contains(&arm), "eq visitor missing {arm}");
       }
       for op in ["Not", "IsNull", "IsNotNull"] {
           let arm = format!("Expr::{op}");
           assert!(hash_body.contains(&arm), "hash visitor missing {arm}");
           assert!(eq_body.contains(&arm), "eq visitor missing {arm}");
       }
       assert!(hash_body.contains("Expr::TimeBucket"));
       assert!(eq_body.contains("Expr::TimeBucket"));
   }
   ```

6. **Add `adr028_aggregate_signature_visitors_preserve_float_bits`:**

   ```rust
   #[test]
   fn adr028_aggregate_signature_visitors_preserve_float_bits() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");

       let hash_scalar = brace_blocks_after_marker(&planner, "fn hash_scalar_structural")
           .into_iter().next().expect("hash_scalar_structural body").1;
       let eq_scalar = brace_blocks_after_marker(&planner, "fn scalar_structural_eq")
           .into_iter().next().expect("scalar_structural_eq body").1;

       assert!(hash_scalar.contains("f32::to_bits"));
       assert!(hash_scalar.contains("f64::to_bits"));
       assert!(eq_scalar.contains("f32::to_bits"));
       assert!(eq_scalar.contains("f64::to_bits"));

       for arm in [
           "ScalarValue::Boolean",
           "ScalarValue::Int32",
           "ScalarValue::Int64",
           "ScalarValue::Float32",
           "ScalarValue::Float64",
       ] {
           assert!(hash_scalar.contains(arm), "hash_scalar missing {arm}");
           assert!(eq_scalar.contains(arm), "eq_scalar missing {arm}");
       }
   }
   ```

7. **Add `adr028_data_type_structural_eq_is_reflexive`**
   (P3 correctness boundary):

   ```rust
   #[test]
   fn adr028_data_type_structural_eq_is_reflexive() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");

       let body = brace_blocks_after_marker(&planner, "fn data_type_structural_eq")
           .into_iter().next().expect("data_type_structural_eq body").1;

       // Reflexive via PartialEq delegation
       assert!(
           body.contains("left == right") || body.contains("left.eq(right)"),
           "P3 correctness: data_type_structural_eq must be reflexive via PartialEq delegation"
       );

       // Must NOT use matches! with arm restrictions
       assert!(
           !body.contains("matches!"),
           "P3 correctness: data_type_structural_eq must not use matches! arm-restriction (Eq reflexivity)"
       );

       // hash_data_type_structural keeps explicit primitive arms +
       // defensive fallback
       let hash_body = brace_blocks_after_marker(&planner, "fn hash_data_type_structural")
           .into_iter().next().expect("hash_data_type_structural body").1;
       for arm in [
           "DataType::Int32",
           "DataType::Int64",
           "DataType::Float32",
           "DataType::Float64",
       ] {
           assert!(hash_body.contains(arm), "hash_data_type missing {arm}");
       }
       assert!(
           hash_body.contains("format!"),
           "hash_data_type_structural must have a defensive fallback arm"
       );
   }
   ```

8. **Add `adr028_aggregate_signature_count_all_is_zero_arg`:**

   ```rust
   #[test]
   fn adr028_aggregate_signature_count_all_is_zero_arg() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");

       let sig_body = brace_blocks_after_marker(&planner, "fn grouped_aggregate_signature")
           .into_iter().next().expect("grouped_aggregate_signature body").1;
       assert!(
           sig_body.contains("AggregateArity::Zero")
               && (sig_body.contains("args: Vec::new()") || sig_body.contains("args: vec![]")),
           "AggregateArity::Zero branch must produce zero-arg signature"
       );

       // lower_count_aggregate placeholder preserved
       let lower_count = brace_blocks_after_marker(&planner, "fn lower_count_aggregate")
           .into_iter().next().expect("lower_count_aggregate body").1;
       assert!(
           lower_count.contains("count")
               && (lower_count.contains("Expr::column") || lower_count.contains("Expr::Column")),
           "lower_count_aggregate must preserve synthetic placeholder (ADR 028 D6)"
       );
   }
   ```

9. **Add `adr028_nested_aggregate_rejects_at_sql_walk`:**

   ```rust
   #[test]
   fn adr028_nested_aggregate_rejects_at_sql_walk() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");
       let walker = brace_blocks_after_marker(&planner, "fn reject_nested_aggregate")
           .into_iter().next().expect("reject_nested_aggregate body").1;

       for arm in [
           "SqlExpr::Function",
           "SqlExpr::BinaryOp",
           "SqlExpr::UnaryOp",
           "SqlExpr::Cast",
           "SqlExpr::IsNull",
           "SqlExpr::IsNotNull",
           "SqlExpr::Nested",
       ] {
           assert!(walker.contains(arm), "missing walker arm: {arm}");
       }
       assert!(planner.contains(
           "aggregate arguments must not contain aggregate calls"
       ));
   }
   ```

10. **Add `adr028_experimental_kernels_excluded_from_grouped_signatures`:**

    ```rust
    #[test]
    fn adr028_experimental_kernels_excluded_from_grouped_signatures() {
        let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
            .expect("read SQL planner source");
        let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
            .expect("read aggregate kernel catalog source");
        let mapping = brace_blocks_after_marker(&catalog, "fn stable_kernel_for_function")
            .into_iter().next().expect("stable_kernel_for_function body").1;

        assert!(
            mapping.contains("ApproxQuantile(_) => None")
                || mapping.contains("ApproxQuantile(_,) => None"),
            "ApproxQuantile must return None"
        );
        assert!(
            mapping.contains("LinearRegress => None"),
            "LinearRegress must return None"
        );

        let sig_body = brace_blocks_after_marker(&planner, "fn grouped_aggregate_signature")
            .into_iter().next().expect("grouped_aggregate_signature body").1;
        assert!(sig_body.contains("aggregate_kernel::stable_kernel_for_function(function)?"));
    }
    ```

11. **Add `adr028_aggregate_output_names_stay_kernel_names`:**

    ```rust
    #[test]
    fn adr028_aggregate_output_names_stay_kernel_names() {
        let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
            .expect("read SQL planner source");
        let names = brace_blocks_after_marker(&planner, "fn aggregate_output_name")
            .into_iter().next().expect("aggregate_output_name body").1;

        for name in ["\"sum\"", "\"count\"", "\"min\"", "\"max\"", "\"avg\""] {
            assert!(names.contains(name), "aggregate_output_name missing {name}");
        }
        assert!(!names.contains("format!"), "no expression-derived naming (D7)");
    }
    ```

12. **Add `adr028_binder_executor_aggregate_args_preserved`:**

    ```rust
    #[test]
    fn adr028_binder_executor_aggregate_args_preserved() {
        let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs"))
            .expect("read exec source");
        assert!(
            exec.contains("self.evaluate_expr(&args[0]")
                || exec.contains("self.evaluate_bound_expr(&call.args[0]"),
            "aggregate-arg evaluation must use existing evaluator path"
        );

        let binder = std::fs::read_to_string(manifest_dir().join("src/binder.rs"))
            .expect("read binder source");
        assert!(binder.contains("bind_expr"));
    }
    ```

13. **Add `adr028_public_ir_remains_non_eq_non_hash`:**

    ```rust
    #[test]
    fn adr028_public_ir_remains_non_eq_non_hash() {
       let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs"))
           .expect("read IR source");
       for ty in ["AggregateFunction", "Expr", "ScalarValue"] {
           let before_ty = ir
               .split(&format!("pub enum {ty}"))
               .next()
               .expect("type marker");
           let derives = before_ty.rsplit("#[derive").next().unwrap_or("");
           let derive_items = derives
               .split(')')
               .next()
               .and_then(|line| line.split('(').nth(1))
               .unwrap_or("");
           let has_forbidden_derive = derive_items
               .split(',')
               .map(str::trim)
               .any(|item| item == "Eq" || item == "Hash");
           assert!(
               !has_forbidden_derive,
               "ADR 028 D2: {ty} must not derive Eq or Hash"
           );
           assert!(
               !ir.contains(&format!("impl Hash for {ty}")),
               "ADR 028 D2: {ty} must not implement Hash"
            );
            assert!(
                !ir.contains(&format!("impl Eq for {ty}")),
                "ADR 028 D2: {ty} must not implement Eq"
            );
        }
    }
    ```

**Verification:**

```sh
cargo test -p toron --test architecture_test
```

## Phase 6 — Fixtures

**Goal.** Exhaustive positive / negative / matching-
ambiguity / regression / plan-digest / compositional
coverage.

**Files:** `crates/toron/tests/group_by_test.rs`

**Tasks:**

1. **Positive — scalar aggregates with computed args:**
   ```sql
   SELECT SUM(amount + 1) FROM sales
   SELECT COUNT(amount + 1) FROM sales
   SELECT AVG(price * 1.1) FROM sales
   SELECT MIN(amount + id), MAX(amount + id) FROM sales
   SELECT SUM(TRY_CAST(amount AS BIGINT)) FROM sales
   SELECT COUNT(TRY_CAST(score AS INT)) FROM sales
   SELECT SUM(CAST(amount AS BIGINT)) FROM sales
   SELECT COUNT(SAFE_CAST(id AS BIGINT)) FROM sales
   ```

2. **Positive — grouped aggregates with computed args:**
   ```sql
   SELECT region, SUM(amount + tax) FROM sales GROUP BY region
   SELECT region, AVG(price * 1.5) FROM sales GROUP BY region
   SELECT region, SUM(TRY_CAST(amount AS BIGINT)) FROM sales GROUP BY region
   ```

3. **Positive — HAVING + ORDER BY composition:**
   ```sql
   SELECT region, SUM(amount + tax)
     FROM sales GROUP BY region
     HAVING SUM(amount + tax) > 100
   SELECT region, SUM(amount + tax)
     FROM sales GROUP BY region
     ORDER BY SUM(amount + tax) DESC
   SELECT region, SUM(amount + tax)
     FROM sales GROUP BY region
     HAVING SUM(amount + tax) > 100
     ORDER BY SUM(amount + tax) DESC
   ```

4. **Positive — error/null-propagation composition:**
   - `SELECT SUM(TRY_CAST(amount AS INT)) FROM sales`
     with rows that fail TryCast → null per ADR 026;
     SUM applies null-handling.
   - `SELECT MIN(price * 1.1) FROM sales` with NaN
     price → ADR 022 sticky-NaN through `*` to MIN.

5. **Signature behavior fixtures (D4 + D6):**
   - Identical computed aggregate calls deduplicate.
   - `SUM(amount + tax)` ≠ `SUM(tax + amount)`
     (structurally distinct).
   - `COUNT(*)` ≠ `COUNT(amount)` ≠ `COUNT(amount + 1)`.

6. **Negative — nested aggregate rejects (D5):**
   ```sql
   SELECT SUM(COUNT(*)) FROM sales
   SELECT SUM(amount + SUM(tax)) FROM sales
   SELECT COUNT(AVG(price)) FROM sales
   ```
   Expects: `"aggregate arguments must not contain
   aggregate calls"`.

7. **Negative — nested aggregate wrapped in expression
   families:**
   ```sql
   SELECT SUM(CAST(SUM(x) AS BIGINT)) FROM sales
   ```
   Same diagnostic.

8. **Negative — binder type-domain rejects (D3):**
   ```sql
   SELECT SUM(amount > 0) FROM sales       -- Boolean not SumNumeric
   SELECT SUM(CAST(region AS BIGINT)) FROM sales  -- inner Cast fails at bind
   ```
   Existing binder TypeMismatch diagnostic, not the
   ADR 028 planner-side diagnostic.

9. **Negative — grouped experimental aggregate queries:**
   ```sql
   SELECT region, APPROX_QUANTILE(amount + 1, 0.5) FROM sales GROUP BY region
   SELECT region, LINEAR_REGRESS(amount + 1, id) FROM sales GROUP BY region
   ```
   Reject via the existing grouped-experimental
   boundary. ADR 028 does not make experimental
   functions grouped-signature eligible.

10. **Matching / ambiguity (D7):**
    ```sql
    SELECT SUM(amount + 1), SUM(amount + 2)
    FROM sales
    HAVING SUM(amount + 1) > 0
    ```

11. **Regression — column-only-arg queries preserved:**
    ```sql
    SELECT SUM(amount) FROM sales
    SELECT region, SUM(amount) FROM sales GROUP BY region
    SELECT region, SUM(amount) FROM sales GROUP BY region
      ORDER BY SUM(amount)
    SELECT region, SUM(amount) FROM sales GROUP BY region
      HAVING SUM(amount) > 0
    SELECT COUNT(*) FROM sales
    ```

12. **Plan-digest backward compat (D10):**
    Spot-check 5+ column-only-arg queries against
    pre-ADR-028 baseline digests. Plan digests
    byte-identical.

13. **Compositional regressions — ADR 025 / 026 / 027:**
    ```sql
    SELECT region, SUM(amount + tax) FROM sales
      GROUP BY region
      ORDER BY CAST(SUM(amount + tax) AS FLOAT32) DESC
    SELECT region, SUM(TRY_CAST(amount AS BIGINT))
      FROM sales GROUP BY region
      ORDER BY SUM(TRY_CAST(amount AS BIGINT)) DESC
    ```

**Verification:**

```sh
cargo test -p toron --test group_by_test
```

## Phase 7 — Final Verification

```sh
cargo fmt --check
cargo check -p toron
cargo test -p toron --test architecture_test
cargo test -p toron --test group_by_test
cargo test -p toron
```

Optional when workspace deps / feature flags change:

```sh
cargo build -p toron
```

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Plan-digest regression for column-only-arg queries | D10 automatic via `LogicalPlan` shape preservation; D6 preserves `COUNT(*)` placeholder; Phase 6 task 12 spot-checks. |
| R2 | Visitor drift if `Expr` adds variants | Exhaustive `match`; `adr028_aggregate_signature_visitors_are_exhaustive` guard pins dual coverage. |
| R3 | Nested-aggregate detector misses SQL-walk arm | Phase 2 audit; `adr028_nested_aggregate_rejects_at_sql_walk` guard. |
| R4 | Computed argument over aggregate accidentally accepted | Phase 6 task 7 fixture (`SUM(CAST(SUM(x) AS BIGINT))`). |
| R5 | Aggregate-arg expression error propagation diverges | Phase 4 audit confirms single-path reuse. |
| R6 | Experimental-kernel surprises | D2 type segregation; Phase 5 task 10 guard. |
| R7 | Wide Eq/Hash surface complicates Brainstorm 001 / 004 | D2 visitor approach; Phase 5 task 13 guard. |
| R8 | ADR 022 sticky-NaN composition gap | Phase 6 task 4 fixture. |
| R9 | PBC residual contract breaks | D9; no PBC change. |
| R10 | Architecture-guard adjustment cascade | Phase 5 tasks 1-2 (2 adjusted guards). |
| R11 | `CountAll` placeholder hashing leak | D6; Phase 5 task 8 guard. |
| R12 | Output-name policy drift | D7; Phase 5 task 11 guard. |
| R13 | `lower_count_aggregate` placeholder accidentally removed | Phase 5 task 8 guard asserts placeholder preservation. |
| R14 | `ScalarValue` / scalar visitor mirror drift | Compiler exhaustiveness on `hash_scalar_structural` / `scalar_structural_eq`. |
| R15 | `data_type_structural_eq` becomes non-reflexive (P3 correctness) | **Phase 5 task 7 `adr028_data_type_structural_eq_is_reflexive` guard pins `left == right` and absence of `matches!` arm-restriction.** |
| R16 | `hash_data_type_structural` fast-path drifts vs Eq | Defensive `format!("{other:?}")` fallback. |
| R17 | Resolver `match` on old `GroupedAggregateSignature` enum variants | Phase 3 task 4 audit. |
| R18 | Walker recursion misses `SqlExpr::Function` non-aggregate args | Phase 2 task 2 explicit `Function` arm recurses. |
| R19 | `validate_aggregate_argument` callers outside lower_* helpers | Phase 0 baseline audit. |
| R20 | Implementer accidentally widens public IR derives | Phase 5 task 13 guard. |
| R21 | Visitor walk cost per probe | Catalog is `Vec<AggregateOutputEntry>` linear scan; cost negligible. `Hash` impl preserved for forward-compat. |
| R22 | Hash/Eq discriminator-order drift | Phase 5 task 5 dual-coverage guard. |
| R23 | `args.len()` not hashed → prefix collision | Phase 5 task 4 length-hash assertion. |

## Definition of Done

- [ ] `validate_aggregate_argument` accepts lowered
      `Expr` surface; column-only diagnostic removed.
- [ ] `reject_nested_aggregate` SQL-walk walker added;
      stable diagnostic present; exhaustive over SQL
      expression families.
- [ ] Existing `aggregate_function_name` helper reused
      for nested-aggregate detection.
- [ ] `GroupedAggregateSignature` struct
      `{ kernel: StableAggregateKernel, args: Vec<Expr> }`
      with planner-private `impl PartialEq` + `impl
      Eq` + `impl Hash`.
- [ ] **No `AggregateArgKey` / `ScalarKey` /
      `DataTypeKey` / `DurationKey` types in
      `sql_planner.rs`.**
- [ ] 6 visitor helpers present:
      `hash_expr_structural`, `expr_structural_eq`,
      `hash_scalar_structural`, `scalar_structural_eq`,
      `hash_data_type_structural`,
      `data_type_structural_eq`.
- [ ] `hash_expr_structural` + `expr_structural_eq`
      exhaustive over current `Expr` (20 variants).
- [ ] `hash_scalar_structural` + `scalar_structural_eq`
      exhaustive; `f32::to_bits()` / `f64::to_bits()`
      for float arms.
- [ ] `data_type_structural_eq` body is `left == right`
      (P3 reflexive); no `matches!` arm-restriction.
- [ ] `hash_data_type_structural` primitive arms +
      `format!("{other:?}")` fallback.
- [ ] `args.len().hash(state)` in signature Hash impl
      (P4 length-hash).
- [ ] `stable_kernel_for_function` returns `Some(...)`
      for stable kernels, `None` for experimental.
- [ ] `grouped_aggregate_signature`'s
      `AggregateArity::Zero` branch produces `args:
      Vec::new()` regardless of input.
- [ ] `lower_count_aggregate`'s synthetic placeholder
      preserved verbatim.
- [ ] Per-kernel lowering helpers accept computed
      `Expr` args.
- [ ] `aggregate_output_name` returns kernel-name-only
      strings; no expression-derived naming.
- [ ] `AggregateOutputCatalog::resolve_for_*` composes
      unchanged.
- [ ] Binder per-kernel type-domain validation
      preserved verbatim.
- [ ] Row-layer executor seam unchanged; no
      `LogicalPlan::Project` between input and
      `Aggregate`.
- [ ] PBC declines aggregates with computed args.
- [ ] Column-only-argument plan digests byte-identical
      pre/post-ADR-028.
- [ ] Experimental kernels remain excluded from grouped
      aggregate signature matching via D2 type
      segregation.
- [ ] Public `AggregateFunction` / `Expr` /
      `ScalarValue` remain non-`Eq` / non-`Hash`.
- [ ] **11** new ADR 028 architecture guards pass.
- [ ] **2** existing guards adjusted (ADR 024 D6 +
      ADR 020 AVG).
- [ ] Positive / negative / matching-ambiguity /
      regression / plan-digest / compositional
      fixtures pass.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.

## Verification commands

```sh
# Development cadence — catch exhaustive-match gaps
cargo check -p toron

# Pre-PR checks
cargo fmt --check
cargo test -p toron --test architecture_test
cargo test -p toron --test group_by_test
cargo test -p toron

# Optional when workspace deps change
cargo build -p toron
```
