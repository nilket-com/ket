# ADR 083 Evidence: Rune Predicate Operator Surface

Date: 2026-05-23

## Implementation summary

- Added nine Rune predicate-authoring methods on `RuneExpr` in `rune_engine.rs`:
  `lt`, `lte`, `gte`, `ne`, `and`, `or`, `logical_not`, `is_null`, and
  `is_not_null`.
- Registered the nine methods as `RuneExpr` associated functions in
  `create_toron_module`.
- Each method lowers directly to an existing `Expr` variant:
  `Lt`, `Lte`, `Gte`, `Neq`, `And`, `Or`, `Not`, `IsNull`, or `IsNotNull`.
- Kept `.eq`, `.gt`, `.named`, `lit*`, dataframe projection, aggregate helpers, and
  `ctx.sql` unchanged.
- Did not add `.not`, arithmetic, `LIKE`/`IN`, casts, source rewriting, Rune-local
  evaluation, or sandbox capability changes.
- Production code change is localized to `crates/toron/src/rune_engine.rs`. Test
  changes are in `tests/rune_relation_handles_test.rs` and
  `tests/architecture/rune_frontend.rs`.
- No IR, binder, executor, SQL planner, Wendelon, PBC opcode, dependency, or `syn`
  production changes were made.

## Load-bearing evidence

- `rune_predicate_operator_compound_filter_executes` proves the compound Rune
  predicate binds and executes to ids `[1, 2, 3]`, exercising `.lt`, `.gte`, `.lte`,
  `.ne`, `.and`, `.or`, `.is_null`, and `.is_not_null`.
- `rune_predicate_operators_preserve_pushdown_path` binds the same compound
  predicate and proves `FilterDisposition::FullyPushed` with scan PBC present.
- `rune_logical_not_executes_against_boolean_column` proves `.logical_not()` executes
  against `active: Boolean` to ids `[2, 5]`.
- `rune_null_predicate_executes_against_nullable_column` proves `.is_null()` executes
  against `price: Float64` to id `[3]`.
- `adr082_typed_literal_cut_does_not_expand_operator_surface` is retargeted in
  place for the post-ADR083 scope: predicate operators are required; `.not`,
  arithmetic, `like`/`ilike`, `in_list`, `cast`, and `try_cast` remain forbidden.
- `adr083_rune_predicate_operators_lower_to_existing_expr_variants` proves each new
  method body builds only the intended existing `Expr` variant.
- `adr083_rune_predicate_operators_are_registered` proves the nine methods are
  registered and `"not"` is not registered.
- `adr083_logical_not_avoids_keyword_collision_with_not` proves the negation surface
  is `logical_not` and no Rune keyword name is registered.
- `adr083_predicate_operator_cut_does_not_change_engine_substrate` proves the Rune
  frontend did not gain binder/executor/source-normalizer markers and that the
  existing IR, binder, executor, and PBC substrate already carry the predicate ops.

## Commands

- `cargo fmt`
- `cargo test -p toron --test rune_relation_handles_test`
  - 25 passed.
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
- `cargo test -p toron --test rune_sandbox_test`
  - 8 passed.
- `cargo test -p toron --test group_by_test`
  - 125 passed.
- `cargo clippy -p toron -- -D warnings`
- `git diff --check`
- `cargo test -p toron`
  - Full package suite passed; one pre-existing Wendelon admin fixture remained
    ignored.
