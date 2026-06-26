# ADR 354 Results - expression evaluator submodule

## Coupling map outcome

`exec.rs` now declares `mod expr;` and `src/exec/expr.rs` owns the expression evaluator as a split `impl Executor`
block. The existing call shapes were preserved:
- external callers still use `Executor::evaluate_bound_program_without_storage`,
  `executor.evaluate_bound_program(...)`, and `executor.filter_batches_with_bound_predicate(...)`;
- sibling executor modules still call methods on `Executor`;
- the only root free-function re-export is `pub(crate) use self::expr::evaluate_cast;`, preserving
  `coordinator/write.rs`.

Visibility changes were limited to the cross-module seams the coupling map identified:
- `evaluate_bound_single_root` and `filter_batch_with_bound_predicate` are `pub(in crate::exec)` for aggregate/join
  siblings;
- `ArithmeticOp`, `literal_array`, and `evaluate_unbound_arithmetic` are `pub(in crate::exec)` for rooted window frame
  helpers;
- evaluator helper functions remain private inside `expr.rs`.

System-table, recursive-CTE, materialization, dispatch, scan, exchange, and window execution stayed rooted. The local
decimal/cast evaluator tests moved with the evaluator; mixed harness tests stayed rooted.

## Decomposition guard

Line-count ratchet:
- before: `exec.rs` 9,772 lines;
- after fmt: `exec.rs` 7,315 lines, `exec/expr.rs` 2,468 lines;
- guard: `EXEC_ROOT_AFTER_EXPR_LINE_LIMIT = 7_500`.

`exec_decomposition.rs` now checks that:
- `mod expr;` is declared;
- `src/exec/expr.rs` exists;
- carved evaluator markers are absent from the root;
- executor modules contain no query-number references, including comments.

Older architecture guards that asserted executor evaluator invariants were retargeted to the combined executor source
corpus via `exec_source()`, matching the earlier decomposition pattern.

## De-branding

Removed query-number comments from executor modules:
- Decimal multiplication/division/add-sub evaluator comments in the moved expression cluster;
- Decimal aggregate comments in `exec/aggregate.rs`.

`rg -n "Q[0-9]|q[0-9]" crates/toron/src/exec.rs crates/toron/src/exec` returns no matches.

## Verification

All commands were run through `scripts/cargo-cgroup`:
- `fmt`
- `fmt -- --check`
- `check -p toron --all-targets`
- `check -p toron-tpch --all-targets`
- `test -p toron --lib` - 1160/0
- `test -p toron --test architecture_test` - 2079/0
- `test -p toron --no-fail-fast` - green
- `test -p toron-tpch` - green
- `run -p toron-tpch -- correctness-matrix` - 22/22 `pass@compare`
- `run -p toron-tpch -- matrix` - 22/22 dev and strict `pass@execute`

No behavior, budget, streaming, or cost-model logic was changed.
