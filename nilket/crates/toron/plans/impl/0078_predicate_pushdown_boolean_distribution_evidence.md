# ADR 078 Implementation Evidence: Predicate Pushdown Boolean Distribution

## Summary

ADR 078 is implemented as a Toron-local binder partitioning change. The old
ADR 075 top-level `AND` split is preserved, with a bounded recursive
`split_boolean_tree` layer that:

- pushes whole subtrees first when `compile_pbc` accepts them;
- preserves minimal residuals for top-level `AND`;
- extracts only structurally common PBC factors from mixed `OR`;
- extracts only compilable branch negations from mixed `NOT(OR ...)`;
- keeps the original predicate as residual for new mixed OR/NOT shapes;
- falls back to the ADR 075 top-level splitter when factoring exceeds budget.

No SQL syntax, PBC opcode, Wendelon protocol, storage wire, telemetry, audit, or
dependency surface was changed.

## Scope Note

The SQL planner still does not lower explicit `NOT (...)` syntax. The ADR 078
`NOT(OR)` and fully-pushable `NOT` behavior is therefore tested through direct
logical-plan fixtures using the existing `Expr::Not` surface. This keeps the ADR
syntax-neutral as planned.

## Verification

All commands below were run from the repository root.

```text
cargo fmt --package toron
```

Completed with no changes required after formatting.

```text
cargo test -p toron --test architecture_test adr078_
```

Result: 17 passed.

```text
cargo test -p toron --test architecture_test adr075_
cargo test -p toron --test architecture_test adr074_
cargo test -p toron --test architecture_test
```

Results:

- ADR 075 guards: 3 passed.
- ADR 074 guards: 17 passed.
- Full architecture corpus: 967 passed.

```text
cargo test -p toron --test predicate_widening_test
cargo test -p toron --test delete_test
cargo test -p toron --test update_test
cargo test -p toron --test runtime_filter_pushdown_test
cargo test -p toron --test wendelon_storage_execution_test
```

Results:

- Predicate widening / pushdown behavior: 28 passed.
- DELETE behavior: 22 passed.
- UPDATE behavior: 34 passed.
- Runtime filter pushdown: 7 passed.
- Wendelon storage execution: 11 passed, 1 ignored.

```text
cargo test -p toron --lib
cargo test -p toron
```

Results:

- Toron lib tests: 353 passed.
- Full Toron package: green.

```text
cargo check -p toron
cargo check -p toron --features telemetry
cargo check -p toron --features otlp-http
cargo check -p toron --features otlp-grpc
```

Result: all four feature checks passed.

```text
cargo clippy -p toron --all-targets -- -D warnings
git diff --check
```

Result: clippy passed with warnings denied; diff check passed.

## Behavior Evidence

The new fixtures cover the promoted ADR surface:

- `where_fully_pushable_or_pushes_whole_or`
- `where_fully_pushable_not_pushes_whole_not`
- `where_mixed_or_branch_does_not_push_partial_branch`
- `where_common_pushable_conjunct_factors_out_of_or_and_preserves_rows`
- `where_common_pk_conjunct_across_or_uses_point_read_profile`
- `where_structurally_different_equivalent_terms_do_not_factor`
- `where_not_or_mixed_extracts_negated_pushable_branch_and_preserves_rows`
- `where_not_and_mixed_does_not_push_negated_single_side`
- `boolean_factoring_over_budget_falls_back_without_wrong_results`
- `predicate_pushdown_3vl_property_random_rows_including_nulls`
- `predicate_pushdown_emitted_splits_preserve_kleene_3vl`
- `delete_common_or_factor_uses_pushed_candidate_plus_original_residual`
- `update_not_or_extraction_uses_pushed_source_scan_plus_original_residual`

The `NOT(OR)` bytecode-shape fixture verifies the pushed branch negation reaches
PBC and ends with the existing `OP_NOT`; common-factor fixtures verify a PBC
program is attached while the original mixed predicate remains the Toron
residual.
