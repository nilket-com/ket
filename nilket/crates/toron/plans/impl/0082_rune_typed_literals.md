# Impl Plan 082: Rune Typed Literals

**Status:** Promoted. Paired with `plans/adr/0082_rune_typed_literals.md`. Genuine
3-of-3. Builds on the landed ADR 081 feat. States the converged shape with the
hand-verified fixtures and two watch-items.
**Posture:** Pure additive surface in `rune_engine.rs` (4 free fns + registration);
everything else is test work.

## Invariants

- Lower to existing `ScalarValue::{Utf8,Float64,Boolean,Int64}` + `Expr::Literal`.
- Keep `lit(i32)`, `.eq`, `.gt`, `.project`/`.project_named`, aggregate helpers, and
  079-081 behavior intact.
- No change outside `rune_engine.rs` + tests; no operator methods; no shim.

## Phase 0 - tests first (`tests/rune_relation_handles_test.rs`)

Extend `sales_schema()` and `sales_batch()` with two nullable test columns:

```rust
Field::new("price", DataType::Float64, true),
Field::new("active", DataType::Boolean, true),
// price : [10.5, 20.25, None, 30.75, 5.5, 7.25]   (Float64Array)
// active: [true, false, true, true, false, true]  (BooleanArray)
```

Behavior fixtures (bind + execute via `rune_plan`/`run_logical`; `id` stays column 0
so `int64_column_rows(&batches, 0)` reads ids) - **all same-type, hand-verified:**

- `rune_lit_str_filters_utf8_column_executes` -
  `filter(col("status").eq(lit_str("open")))` -> `[1,2,4,5,6]`.
- `rune_lit_f64_filters_float64_column_executes` -
  `filter(col("price").gt(lit_f64(15.5)))` -> `[2,4]`. **Float64 column, not
  `amount: Int32`** (no `Float64->Int32` arm).
- `rune_lit_bool_filters_boolean_column_executes` -
  `filter(col("active").eq(lit_bool(true)))` -> `[1,3,4,6]`.
- `rune_lit_i64_filters_int64_column_executes` -
  `filter(col("id").eq(lit_i64(4)))` -> `[4]`.
- `rune_lit_i64_preserves_wide_integer_in_logical_plan` (plan-only) -
  `filter(col("id").eq(lit_i64(3000000000)))` asserts the predicate carries
  `ScalarValue::Int64(Some(3000000000))`.

## Phase 1 - production helpers (`rune_engine.rs`, beside `lit`)

```rust
pub fn lit_str(value: String) -> RuneExpr { RuneExpr { expr: Expr::Literal(ScalarValue::Utf8(Some(value))) } }
pub fn lit_f64(value: f64)    -> RuneExpr { RuneExpr { expr: Expr::Literal(ScalarValue::Float64(Some(value))) } }
pub fn lit_bool(value: bool)  -> RuneExpr { RuneExpr { expr: Expr::Literal(ScalarValue::Boolean(Some(value))) } }
pub fn lit_i64(value: i64)    -> RuneExpr { RuneExpr { expr: Expr::Literal(ScalarValue::Int64(Some(value))) } }
```

Register each helper after `lit`:

```rust
m.function(["lit_str"], lit_str).build()?;
m.function(["lit_f64"], lit_f64).build()?;
m.function(["lit_bool"], lit_bool).build()?;
m.function(["lit_i64"], lit_i64).build()?;
```

No `RuneExpr` methods in this ADR.

## Phase 2 - architecture guards (`tests/architecture/rune_frontend.rs`)

The 5 `adr082_*` guards from the ADR - lower-to-existing-ScalarValue,
registered-in-module, names-avoid-keywords, no-operator-expansion (assert none of
`lt/lte/gte/ne/and/or/logical_not/not/is_null/is_not_null/add/sub/mul/div`
registered), and
no-IR/binder/exec/sandbox-change (assert `compile_script` still has
`Source::new("script", script)`, no `normalize_` fn, no `BindingError`/`ExecutionPlan`
markers in `rune_engine.rs`). ADR 079/080/081 guards stay green.

## Watch items

1. **`bool` host-fn arg has no in-repo precedent** - `String`/`f64` are proven
   (`col`, `approx_quantile`), `bool` is not. Verify `m.function(["lit_bool"],
   lit_bool)` registers cleanly (Rune has native `bool`; low risk). If it doesn't,
   that's a real finding; surface it, don't paper over.
2. **Shared-fixture perturbation** - adding `price`+`active` to `sales` changes the
   079 `rune_sql_composition_preserves_existing_pushdown_path` fixture's `SELECT *`
   expansion (now 6 columns). It asserts `FilterDisposition`/scan-PBC, not a column
   set, so it should stay green; the full-package run must confirm.

## Verification

```
cargo fmt
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test architecture_test adr082_
cargo test -p toron --test architecture_test adr081_
cargo test -p toron --test architecture_test adr080_
cargo test -p toron --test architecture_test adr079_   # SELECT * fixture path
cargo test -p toron --test rune_sandbox_test
cargo test -p toron --test group_by_test
cargo clippy -p toron -- -D warnings
git diff --check
cargo test -p toron                                     # full package
```

## Evidence doc (`plans/impl/0082_rune_typed_literals_evidence.md`)

Record: `rune_engine.rs` + the two test files as the only touched files; the
`price`/`active` fixture data; the 4 behavior outputs (`[1,2,4,5,6]`/`[2,4]`/
`[1,3,4,6]`/`[4]`) + the wide-int plan assertion; the `bool`-arg registration result;
confirmation the 079 `SELECT *` fixture stayed green; confirmation `binder.rs`/
`exec.rs`/`ir.rs` unchanged; command counts.

## Acceptance checklist

- [ ] `lit_str/lit_f64/lit_bool/lit_i64` exist, lower to matching `ScalarValue`.
- [ ] Registered as free fns; `lit(i32)` unchanged.
- [ ] 4 same-type behavior fixtures + wide-int plan fixture green.
- [ ] `lit_f64` tested against `price: Float64`, not `amount`.
- [ ] No operator methods; no shim; no production change outside `rune_engine.rs`.
- [ ] 5 `adr082_*` guards green; 079/080/081 guards intact; full package + clippy clean.
