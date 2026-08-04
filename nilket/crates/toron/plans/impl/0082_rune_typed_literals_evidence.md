# ADR 082 Evidence: Rune Typed Literals

Date: 2026-05-23

## Implementation summary

- Added Rune free functions `lit_str`, `lit_f64`, `lit_bool`, and `lit_i64` in
  `rune_engine.rs`.
- Registered the four helpers in `create_toron_module` immediately after the
  existing `lit(i32)` registration.
- Each helper lowers directly to `Expr::Literal` with the existing
  `ScalarValue::{Utf8, Float64, Boolean, Int64}` variants.
- Kept `lit(i32)`, `.eq`, `.gt`, `.project`, `.project_named`, and aggregate
  helpers unchanged.
- Extended the Rune relation-handle `sales` fixture with test-only `price:
  Float64` and `active: Boolean` columns.
- Updated the `FixtureScan` test harness to execute pushed PBC predicates before
  projection, matching the production storage-scan ordering and making fully
  pushed Rune filters observable in execution fixtures.
- Added five ADR 082 architecture guards in `tests/architecture/rune_frontend.rs`.
- No IR, binder, executor, SQL planner, Wendelon, worker, wire, storage, PBC,
  dependency, `syn`, sandbox policy, source-normalization, or operator-surface
  production changes were made.

## Load-bearing evidence

- `rune_lit_str_filters_utf8_column_executes` proves
  `col("status").eq(lit_str("open"))` binds and executes to ids
  `[1, 2, 4, 5, 6]`.
- `rune_lit_f64_filters_float64_column_executes` proves
  `col("price").gt(lit_f64(15.5))` binds and executes to ids `[2, 4]` against a
  same-type `Float64` column, not `amount: Int32`.
- `rune_lit_bool_filters_boolean_column_executes` proves `lit_bool(true)`
  registers as a native Rune bool host argument and executes against
  `active: Boolean` to ids `[1, 3, 4, 6]`.
- `rune_lit_i64_filters_int64_column_executes` proves
  `col("id").eq(lit_i64(4))` binds and executes against an `Int64` column to id
  `[4]`.
- `rune_lit_i64_preserves_wide_integer_in_logical_plan` proves
  `lit_i64(3000000000)` remains `ScalarValue::Int64(Some(3000000000))` in the
  logical predicate and does not route through the existing i32 helper.
- `rune_sql_composition_preserves_existing_pushdown_path` and
  `rune_named_projection_preserves_adr079_sql_relation_guards` stayed green after
  the shared `sales` fixture grew `price` and `active`.
- `adr082_rune_typed_literal_helpers_lower_to_existing_scalar_values` proves the
  helper bodies lower to existing `ScalarValue` variants.
- `adr082_rune_typed_literal_helpers_are_registered_in_toron_module` proves the
  helpers are registered as Rune free functions and `lit(i32)` remains present.
- `adr082_typed_literal_names_avoid_rune_keywords` proves the new names do not
  collide with the ADR 081 keyword set.
- `adr082_typed_literal_cut_does_not_expand_operator_surface` proves ADR 082 did
  not register deferred operator methods such as `lt`, `and`, `logical_not`,
  `not`, `is_null`, or arithmetic helpers.
- `adr082_typed_literals_do_not_change_ir_binder_executor_or_sandbox` proves the
  implementation remains local to Rune authoring and keeps direct
  `Source::new("script", script)` compilation without a source normalizer.

## Commands

- `cargo fmt`
- `cargo test -p toron --test rune_relation_handles_test`
  - 21 passed.
- `cargo test -p toron --test architecture_test adr082_`
  - 5 passed.
- `cargo test -p toron --test architecture_test adr081_`
  - 2 passed.
- `cargo test -p toron --test architecture_test adr080_`
  - 15 passed.
- `cargo test -p toron --test architecture_test adr079_`
  - 14 passed.
- `cargo test -p toron --test rune_sandbox_test`
  - 8 passed.
- `cargo test -p toron --test group_by_test`
  - 125 passed.
- `cargo clippy -p toron -- -D warnings`
- `git diff --check`
- `cargo test -p toron`
  - Full package suite passed; one pre-existing Wendelon admin fixture remained
    ignored.
