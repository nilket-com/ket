# ADR 085 Evidence: Rune Cast / TryCast Expression Surface

## Summary

Implemented ADR 085 as the typed numeric Rune Cast/TryCast cut.

Production change is localized to `crates/toron/src/rune_engine.rs`:

- added `RuneDataType { data_type: DataType }`
- added `type_i32`, `type_i64`, `type_f32`, and `type_f64`
- added `RuneExpr::cast` lowering to existing `Expr::Cast`
- added `RuneExpr::try_cast` lowering to existing `Expr::TryCast`
- registered the carrier, four helpers, and two expression methods in the Toron Rune module

No production edits were made to `binder.rs`, `exec.rs`, `ir.rs`, `pbc.rs`, SQL
planner code, storage, wire, or Wendelon paths. Cast semantics remain owned by the
existing ADR 024 substrate.

## Files Changed

- `crates/toron/src/rune_engine.rs`
- `crates/toron/tests/rune_relation_handles_test.rs`
- `crates/toron/tests/architecture/rune_frontend.rs`
- `crates/toron/plans/impl/0085_rune_cast_try_cast_evidence.md`

## Behavior Fixtures

Added five Rune behavior gates in `rune_relation_handles_test.rs`.

- `rune_cast_projection_executes`
  - `project_named` over casts to `Int64`, `Int32`, `Float32`, and `Float64`
  - asserts schema names and target data types
  - asserts exact rows:
    - `amount_i64 = [10, 20, NULL, 30, NULL, 7]`
    - `id_i32 = [1, 2, 3, 4, 5, 6]`
    - `price_f32 = [10.5, 20.25, NULL, 30.75, 5.5, 7.25]`
    - `amount_f64 = [10.0, 20.0, NULL, 30.0, NULL, 7.0]`
- `rune_try_cast_nulls_data_domain_failures`
  - `id + 3000000000` try-cast to `Int32` yields all NULL
  - `price * 1e40` try-cast to `Float32` yields all NULL
- `rune_strict_cast_domain_failure_is_kernel_error`
  - same narrowing overflow with strict `cast(type_i32())` returns `ExecutionError::Kernel`
  - message contains `cast failed`
- `rune_cast_predicate_is_residual`
  - `filter(col("amount").cast(type_i64()).gt(lit_i64(10)))`
  - selected ids are `[2, 4]`
  - disposition is `FilterDisposition::EvaluateInToron`
  - scan PBC is absent
- `rune_cast_feeds_aggregate_argument`
  - `aggregate([], [sum(col("amount").try_cast(type_i64()))])`
  - output is `[Some(67)]`

## Architecture Guards

Retargeted existing guards:

- `adr082_typed_literal_cut_does_not_expand_operator_surface`
  - now requires `cast` and `try_cast`
  - still forbids `not`, `like`, `ilike`, `in_list`, and `safe_cast`
- `adr084_rune_arithmetic_methods_are_registered`
  - no longer forbids `cast` and `try_cast`
  - still forbids `mod` and `neg`

Added ADR 085 guards:

- `adr085_rune_datatype_is_host_surface_only`
- `adr085_rune_cast_target_helpers_are_numeric_only`
- `adr085_rune_cast_methods_lower_to_existing_expr_variants`
- `adr085_rune_cast_methods_are_registered`
- `adr085_rune_cast_reuses_existing_substrate`
- `adr085_rune_cast_remains_pbc_ineligible`

The guards pin:

- typed carrier, not string target parsing
- exactly four numeric target helpers
- no `safe_cast`
- no `type_utf8`, `type_bool`, `type_decimal`, or `type_date`
- direct lowering to existing IR variants
- no new binder, executor, planner, or PBC production machinery in Rune
- cast predicates remain PBC-ineligible

## Verification

Commands run from `/home/me/work/nilket`:

```sh
cargo fmt
cargo test -p toron --test architecture_test adr085_
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test architecture_test adr084_
cargo test -p toron --test architecture_test adr083_
cargo test -p toron --test architecture_test adr082_
cargo test -p toron --test architecture_test adr081_
cargo test -p toron --test architecture_test adr080_
cargo test -p toron --test architecture_test adr079_
cargo fmt --check
cargo test -p toron --test group_by_test
cargo test -p toron --test rune_sandbox_test
cargo clippy -p toron -- -D warnings
cargo test -p toron
git diff --check
```

Observed results:

- `architecture_test adr085_`: 6 passed
- `rune_relation_handles_test`: 35 passed
- `architecture_test adr084_`: 4 passed
- `architecture_test adr083_`: 4 passed
- `architecture_test adr082_`: 5 passed
- `architecture_test adr081_`: 2 passed
- `architecture_test adr080_`: 15 passed
- `architecture_test adr079_`: 14 passed
- `group_by_test`: 125 passed
- `rune_sandbox_test`: 8 passed
- `cargo clippy -p toron -- -D warnings`: passed
- `cargo test -p toron`: passed; final observed tail included
  `wendelon_storage_execution_test` with 11 passed and 1 ignored, plus doc-tests
  with 0 tests
- `git diff --check`: passed

## Residual Scope

Out of scope remains unchanged from the ADR:

- string target parsing such as `.cast("Int64")`
- `safe_cast`
- non-numeric target helpers
- scan PBC lowering for casts
- new cast kernels or semantic changes
- SQL grammar changes
- Rune `Like` or `InList`
