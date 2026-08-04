# ADR 086 Evidence: Rune InList Membership Surface

## Summary

Implemented `.in_list` and `.not_in_list` in the Rune authoring surface as
`VmResult`-returning `RuneExpr` methods. The methods lower directly to existing
`Expr::InList { expr, list, negated }` and validate the SQL frontend bound in Rune:
empty lists reject with `"IN list must not be empty"` and lists longer than 32 reject
with `"IN list exceeds 32 items"`.

Production change is localized to `crates/toron/src/rune_engine.rs`: one local
`RUNE_MAX_IN_LIST_ITEMS` constant, one shared `build_in_list` helper, two methods, and
two module registrations. No IR, binder, executor, PBC opcode, SQL grammar, storage,
wire, dependency, or sandbox change.

## Behavior Evidence

- `rune_in_list_pushes_to_scan_pbc`: `id IN [1,2]` returns ids `[1,2]`, binds
  `FilterDisposition::FullyPushed`, reaches scan PBC, and bytecode contains
  `OP_IN_LIST`.
- `rune_not_in_list_pushes_to_scan_pbc`: `status NOT IN ["open"]` returns `[3]`, binds
  `FullyPushed`, reaches scan PBC, and bytecode contains `OP_IN_LIST` and `OP_NOT`.
- `rune_not_in_list_excludes_null_target`: `region NOT IN ["east"]` returns `[2,5]`;
  the NULL `region` row is excluded by inherited 3VL behavior.
- `rune_in_list_literal_coercion_executes`: `id IN [lit(1), lit_i64(2)]` returns
  `[1,2]`, proving binder coercion is inherited.
- `rune_in_list_computed_member_is_residual`: computed members
  `amount - 9` and `amount - 18` return `[1,2]`, bind `EvaluateInToron`, and do not
  attach scan PBC.
- `rune_in_list_empty_list_panics`: `.in_list([])` fails in the Rune frontend with
  `"IN list must not be empty"`.
- `rune_in_list_over_limit_panics`: 33 members fail in the Rune frontend with
  `"IN list exceeds 32 items"`.
- `rune_in_list_mismatch_is_binding_error`: `status IN [lit(1)]` reaches the existing
  binder type mismatch path containing `"IN operands"`.

## Guard Evidence

- Retargeted `adr082_typed_literal_cut_does_not_expand_operator_surface` to require
  `in_list` and `not_in_list` while keeping exact bare `not`, `like`, `ilike`, and
  `safe_cast` forbidden.
- Added six ADR 086 architecture guards:
  lower to existing `Expr::InList`; validate Rune frontend bounds; pin
  `RUNE_MAX_IN_LIST_ITEMS == MAX_IN_LIST_ITEMS`; assert both methods are registered
  without exact exported `in`/`not`; assert pushed behavior is guarded; and assert
  the implementation reuses existing IR/binder/executor/PBC substrate.
- Test bytecode checks import `OP_IN_LIST` and `OP_NOT` from public
  `wendelon::schema::pbc`; Toron's `pbc.rs` imports those constants privately.

## Verification

- `cargo fmt` — pass.
- `cargo test -p toron --test rune_relation_handles_test` — 43 passed, including 8 new
  ADR 086 fixtures.
- `cargo test -p toron --test architecture_test adr086_` — 6 passed.
- `cargo test -p toron --test architecture_test adr082_` — 5 passed.
- `cargo test -p toron --test architecture_test` — 1023 passed.
- `cargo test -p toron --test predicate_widening_test` — 28 passed.
- `cargo test -p toron --test group_by_test` — 125 passed.
- `cargo test -p toron --test rune_sandbox_test` — 8 passed.
- `cargo clippy -p toron -- -D warnings` — pass.
- `git diff --check` — pass.
- `cargo test -p toron` — pass; package-wide test run green with the pre-existing
  ignored Wendelon admin fixture unchanged.
