# Impl Plan 084: Rune Arithmetic Expression Surface

**Status:** Promoted. Paired with `plans/adr/084_rune_arithmetic_expressions.md`.
Genuine 3-of-3. Builds on the landed ADR 083 feat. **Posture:** pure additive surface
in `rune_engine.rs` — 4 `RuneExpr` methods + registration; tests + guards carry the
rest. ADR 024 already owns all arithmetic semantics.

## Invariants

- Lower to existing `Expr::{Add,Sub,Mul,Div}`. No new IR/binder/executor/PBC.
- Keep `lit*`, `.gt`/`.eq`, the 083 predicate methods, `.named`, `.project_named`,
  aggregate helpers, 079–083 behavior intact. No `Cast`/`TryCast`, modulo, unary
  negation, or operator overloading.
- No change outside `rune_engine.rs` + tests; no shim; no Rune-local evaluation.

## Phase 0 — tests first (`tests/rune_relation_handles_test.rs`)

Reuse the 082/083 `sales` fixture (`amount: Int32`, `price: Float64`, `id: Int64`).
All bind + execute via `rune_plan`/`run_logical`; values hand-verified.

- `rune_arithmetic_computed_projection_executes` (gate):
  ```rune
  ctx.table("sales").project_named([
      col("amount").add(lit(1)).named("amount_plus_one"),
      col("amount").div(lit(2)).named("amount_half"),
      col("price").mul(lit_f64(2.0)).named("price_x2"),
      col("id").sub(lit_i64(1)).named("zero_based_id"),
      col("amount").add(col("price")).named("amount_plus_price"),
  ])
  ```
  → `amount_plus_one=[11,21,NULL,31,NULL,8]`, `amount_half=[5,10,NULL,15,NULL,3]`
  (truncating), `price_x2=[21.0,40.5,NULL,61.5,11.0,14.5]`,
  `zero_based_id=[0,1,2,3,4,5]`, `amount_plus_price=[20.5,40.25,NULL,60.75,NULL,14.25]`
  (Int32+Float64→Float64). Assert visible schema + per-column values incl. NULL slots.
- `rune_arithmetic_residual_filter_disposition_is_evaluate_in_toron` — bind
  `filter(col("amount").add(lit(1)).gt(lit(20)))`; assert ids `[2,4]` via execute,
  `FilterDisposition::EvaluateInToron`, and `scan_pbc(scan).is_none()` (inverse of
  083's FullyPushed; reuse `find_filter`/`scan_pbc`).
- `rune_arithmetic_aggregate_argument_executes` —
  `aggregate([], [sum(col("amount").add(lit(1)))])` → scalar `[Some(71)]` (grounded by
  SQL `SUM(amount+1)`, `group_by_test:317`).
- `rune_arithmetic_integer_division_by_zero_is_null` —
  `project_named([col("amount").div(col("amount").sub(col("amount"))).named("q")])`
  → `q` all NULL.
- `rune_arithmetic_integer_overflow_is_kernel_error` —
  `project_named([lit_i64(9223372036854775807).add(lit_i64(1)).named("overflow")])`
  → execution returns `ExecutionError::Kernel` containing `"arithmetic overflow"`
  (assert the kernel error, never NULL — pins the corrected inheritance).

## Phase 1 — production methods (`rune_engine.rs`, in `impl RuneExpr` after the 083 predicate methods)

```rust
pub fn add(self, other: RuneExpr) -> RuneExpr { RuneExpr { expr: Expr::Add(Box::new(self.expr), Box::new(other.expr)) } }
pub fn sub(self, other: RuneExpr) -> RuneExpr { RuneExpr { expr: Expr::Sub(Box::new(self.expr), Box::new(other.expr)) } }
pub fn mul(self, other: RuneExpr) -> RuneExpr { RuneExpr { expr: Expr::Mul(Box::new(self.expr), Box::new(other.expr)) } }
pub fn div(self, other: RuneExpr) -> RuneExpr { RuneExpr { expr: Expr::Div(Box::new(self.expr), Box::new(other.expr)) } }
```

Register after `is_not_null`:
```rust
m.associated_function("add", RuneExpr::add)?;
m.associated_function("sub", RuneExpr::sub)?;
m.associated_function("mul", RuneExpr::mul)?;
m.associated_function("div", RuneExpr::div)?;
```

No `Cast`/`TryCast`, modulo, unary negation, or operator overloading.

## Phase 2 — guards (`tests/architecture/rune_frontend.rs`)

- **Retarget `adr082_typed_literal_cut_does_not_expand_operator_surface`**
  (discipline #4): move `add/sub/mul/div` to **required**; keep `not, like, ilike,
  in_list, cast, try_cast` **forbidden**.
- `adr084_rune_arithmetic_methods_lower_to_existing_expr_variants` — each body builds
  its `Expr::{Add|Sub|Mul|Div}`.
- `adr084_rune_arithmetic_methods_are_registered` — 4 registered; no overload/shim/cast.
- `adr084_rune_arithmetic_reuses_adr024_substrate` — no
  `BindingError`/`BoundExpr`/`ExecutionPlan`/`normalize_`/`evaluate_expr` in
  `rune_engine.rs`; `ir`/`binder`/`exec` already hold `bind_arithmetic` /
  `arithmetic_output_type` / `evaluate_unbound_arithmetic`.
- `adr084_rune_arithmetic_remains_pbc_ineligible` — assert `is_pbc_pushdown_eligible`
  false for `Add/Sub/Mul/Div` (source-read `ir.rs`) + the PBC rejection text.

## Verification

```
cargo fmt
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test architecture_test adr084_
cargo test -p toron --test architecture_test adr083_
cargo test -p toron --test architecture_test adr082_   # retargeted guard green
cargo test -p toron --test architecture_test adr081_
cargo test -p toron --test architecture_test adr080_
cargo test -p toron --test architecture_test adr079_
cargo test -p toron --test group_by_test               # ADR-024 net intact
cargo test -p toron --test rune_sandbox_test
cargo clippy -p toron -- -D warnings
git diff --check
cargo test -p toron
```

## Evidence doc (`plans/impl/084_rune_arithmetic_expressions_evidence.md`)

Record: `rune_engine.rs` + two test files as only touched; the 5 fixture results
(computed columns / `[2,4]`+EvaluateInToron / sum `71` / div-zero NULL / overflow
Kernel); the retargeted scope guard; confirmation `binder.rs`/`exec.rs`/`ir.rs`
unchanged and no `Cast`/`TryCast`/modulo registered; command counts.

## Watch items

1. **Residual, not pushed** — the arithmetic-predicate fixture must assert
   `EvaluateInToron` + no scan PBC. `FullyPushed` would be a finding.
2. **Overflow is `Kernel`, not NULL** — the overflow fixture asserts `Kernel` (the
   iter-0 slip); never NULL.
3. **Truncating int division** — `amount_half` 7//2 = 3; `Int32` same-type.
4. **NULL propagation** — NULL `amount` rows stay NULL through arithmetic (incl.
   `amount_plus_price` at the NULL-amount rows); assert exact per-row values.

## Acceptance checklist

- [ ] `.add/.sub/.mul/.div` exist, lower to existing `Expr`; no Cast/modulo/negation.
- [ ] Registered as `RuneExpr` associated functions.
- [ ] 5 fixtures green (computed projection / residual `[2,4]` / sum `71` / div-zero
      NULL / overflow Kernel).
- [ ] Arithmetic predicate asserts `EvaluateInToron` (not pushed).
- [ ] Scope guard retargeted (arithmetic required; cast/like/in_list still forbidden).
- [ ] No production change outside `rune_engine.rs`; 079–083 guards intact; full package + clippy clean.
