# ADR 084 Evidence: Rune Arithmetic Expression Surface

Date: 2026-05-23

## Implementation summary

- Added four Rune arithmetic-authoring methods on `RuneExpr` in `rune_engine.rs`:
  `add`, `sub`, `mul`, and `div`.
- Registered the four methods as `RuneExpr` associated functions in
  `create_toron_module`.
- Each method lowers directly to an existing ADR 024 `Expr` variant:
  `Add`, `Sub`, `Mul`, or `Div`.
- Kept `.eq`, `.gt`, ADR 083 predicate methods, `.named`, `lit*`, dataframe
  projection, aggregate helpers, and `ctx.sql` unchanged.
- Did not add casts, modulo, unary negation, operator overloading, source rewriting,
  Rune-local evaluation, sandbox capability changes, or PBC arithmetic.
- Production code change is localized to `crates/toron/src/rune_engine.rs`. Test
  changes are in `tests/rune_relation_handles_test.rs` and
  `tests/architecture/rune_frontend.rs`.
- No IR, binder, executor, SQL planner, Wendelon, PBC opcode, dependency, or `syn`
  production changes were made.

## Load-bearing evidence

- `rune_arithmetic_computed_projection_executes` proves all four methods bind and
  execute through `.project_named(...)`:
  - `amount_plus_one = [11, 21, NULL, 31, NULL, 8]`
  - `amount_half = [5, 10, NULL, 15, NULL, 3]`
  - `price_x2 = [21.0, 40.5, NULL, 61.5, 11.0, 14.5]`
  - `zero_based_id = [0, 1, 2, 3, 4, 5]`
  - `amount_plus_price = [20.5, 40.25, NULL, 60.75, NULL, 14.25]`
- `rune_arithmetic_residual_filter_disposition_is_evaluate_in_toron` proves
  `col("amount").add(lit(1)).gt(lit(20))` executes to ids `[2, 4]`, binds
  `FilterDisposition::EvaluateInToron`, and sends no scan PBC.
- `rune_arithmetic_aggregate_argument_executes` proves
  `sum(col("amount").add(lit(1)))` executes as scalar `[Some(71)]`, matching the
  existing SQL arithmetic aggregate path.
- `rune_arithmetic_integer_division_by_zero_is_null` proves integer divide-by-zero
  returns NULL for every row.
- `rune_arithmetic_integer_overflow_is_kernel_error` proves the literal-only
  overflow shape `lit_i64(9223372036854775807).add(lit_i64(1))` returns
  `ExecutionError::Kernel` containing `"arithmetic overflow"`.
- The Rune relation test fixture now preserves row count for empty storage
  projections, so literal-only computed projections execute instead of failing before
  expression evaluation.
- `adr082_typed_literal_cut_does_not_expand_operator_surface` is retargeted in place
  for the post-ADR084 scope: arithmetic is required; `not`, `like`, `ilike`,
  `in_list`, `cast`, and `try_cast` remain forbidden.
- `adr084_rune_arithmetic_methods_lower_to_existing_expr_variants` proves each new
  method body builds only the intended existing `Expr` variant.
- `adr084_rune_arithmetic_methods_are_registered` proves the four methods are
  registered and no cast/modulo/negation surface was registered.
- `adr084_rune_arithmetic_reuses_adr024_substrate` proves the Rune frontend did not
  gain binder/executor/source-normalizer markers and that the existing IR, binder,
  executor, and PBC substrate already carry arithmetic semantics.
- `adr084_rune_arithmetic_remains_pbc_ineligible` proves arithmetic remains outside
  PBC pushdown via `Expr::is_pbc_pushdown_eligible` and the stable PBC rejection
  string `"arithmetic, Cast, and TryCast are not PBC-eligible"`.

## Commands

- `cargo fmt`
- `cargo test -p toron --test rune_relation_handles_test`
  - 30 passed.
- `cargo test -p toron --test architecture_test adr084_`
  - 4 passed.
- `cargo test -p toron --test architecture_test adr083_`
  - 4 passed.
- `cargo test -p toron --test architecture_test adr082_`
  - 5 passed.
- `cargo test -p toron --test architecture_test adr081_`
  - 2 passed.
- `cargo test -p toron --test architecture_test adr080_`
  - 15 passed.
- `cargo test -p toron --test architecture_test adr079_`
  - 14 passed.
- `cargo test -p toron --test group_by_test`
  - 125 passed.
- `cargo test -p toron --test rune_sandbox_test`
  - 8 passed.
- `cargo clippy -p toron -- -D warnings`
- `git diff --check`
- `cargo test -q -p toron`
  - Full package suite passed; one pre-existing Wendelon admin fixture remained
    ignored.
