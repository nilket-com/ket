# ADR 352 Results - join streaming submodule

## Summary

ADR 352 performed exactly one carve: the join streaming orchestration layer moved from
`crates/toron/src/exec/join.rs` to `crates/toron/src/exec/join/streaming.rs`.

The Phase 0 coupling map proved the move-now bucket was separable through a split `impl Executor` block plus one-way
calls into the join root, aggregate APIs, generic filter/project/exchange helpers, and ADR 351's spill API. No aggregate
helper, expression evaluator, generic hash join, or executor dispatch code moved.

This is behavior-neutral source structure work. No budget values, cost-model choices, streaming algorithms, spill formats,
or runtime semantics changed.

## Coupling map and decision

### Move now - streaming-specific orchestration

These symbols moved to `exec/join/streaming.rs`:

- Shape/runtime structs and enums:
  `ReorientedInnerJoinStage`, `ReorientedInnerJoinRuntime`, `StreamingSemiAntiStage`,
  `StreamingSemiAntiExistence`, `StreamingExistenceOp`, `StreamingSemiAntiRuntime`,
  `StreamingLeftOuterRecombineRuntime`, and `StreamingSemiAntiChain`.
- Streaming shape extractors and plan walkers:
  `extract_reoriented_inner_chain`, `filter_input`, `exchange_input`, `decompose_streaming_existence_plan`, and
  `extract_streaming_inner_prefix_semi_anti_chain`.
- Split `impl Executor` streaming methods:
  `try_streaming_join0_filter`, `try_streaming_join0_filter_exchange`,
  `try_streaming_correlated_scalar_left_outer_global_aggregate`,
  `try_visit_streaming_join0_filter_chunks`, `try_streaming_collapsing_global_aggregate`,
  `try_streaming_projected_left_semi_grouped_aggregate`,
  `fold_projected_streamed_left_semi_grouped_aggregate`, `fold_projected_left_semi_refs`,
  `try_streaming_reoriented_inner_chain_grouped_aggregate`,
  `try_streaming_inner_prefix_semi_anti_grouped_aggregate`, `prepare_reoriented_inner_join_runtime`,
  `prepare_streaming_semi_anti_runtime`, `write_streaming_semi_anti_existence`,
  `try_visit_streaming_reoriented_inner_chain_base`, `probe_streaming_semi_anti`,
  `probe_streaming_left_outer_recombine`, and `probe_reoriented_inner_join`.
- Streaming-local residual helper:
  `matching_preserved_rows_in_memory`.

The split `impl Executor` approach was the key separability mechanism: these methods still call root/shared APIs, but no
root logic has to move with them.

### Stay root - shared APIs, not streaming ownership

These stayed in `exec/join.rs` because they are shared with non-streaming hash join paths, aggregate work planned for ADR
353, or generic project/materialization utilities:

- Aggregate-column and ordinal utilities:
  `collect_program_column_ordinals`, `collect_program_node_column_ordinals`, `grouped_aggregate_input_columns`,
  `grouped_aggregate_is_order_insensitive`, and `remap_column_ordinals`.
- Projection/materialization utilities:
  `projected_schema_for_columns`, `project_batch_columns`, `default_array_for_unused_column`,
  `materialize_join_selection`, `materialize_reoriented_inner_selection`, and `materialize_preserved_selection`.
- Generic hash join execution:
  `execute_hash_join`, `execute_hash_join_into_global_aggregate`, in-memory build/probe helpers, join key helpers,
  join output materialization byte accounting, and the generic streamed-spilled / partitioned spilled join helpers.
- Telemetry and budget helpers shared by all join paths:
  `record_join_runtime_observation`, `record_task_telemetry`, `duration_micros`, `budget_exhausted`, and byte estimators.

Aggregate folding/finalization functions remain in the executor root and are called as APIs from `streaming.rs`; the move
did not require private aggregate internals. Therefore the Phase 0 gate resolved to **carve streaming now**, not
aggregate-first.

### Defer / re-scope

- No symbol required a forced aggregate-first re-order.
- No test module was extracted. The streaming tests stay in the existing mixed `exec::tests` harness because they share
  broad executor fixtures with the rest of `exec.rs`.
- Further join splitting is deliberately deferred; this cut does not move generic hash join, materialization, or aggregate
  helpers.

## API surface

`exec/join.rs` declares `mod streaming;`. Production dispatch reaches moved methods through inherent `Executor` methods
with `pub(in crate::exec)` visibility, matching the prior `pub(super)` reach from `exec/join.rs`.

Only the existing mixed root tests need direct names from the streaming submodule. Those are re-exported under
`#[cfg(test)]`:

- `StreamingSemiAntiStage`
- `StreamingSemiAntiExistence`
- `StreamingSemiAntiRuntime`
- `extract_streaming_inner_prefix_semi_anti_chain`

No `pub(crate)` streaming API was introduced.

## Line counts and guards

- Before ADR 352:
  - `exec/join.rs`: 5,801 lines.
  - `exec/join/spill.rs`: 1,375 lines.
  - `exec.rs`: 11,611 lines.
- After ADR 352 and fmt:
  - `exec/join.rs`: 3,685 lines.
  - `exec/join/streaming.rs`: 2,120 lines.
  - `exec/join/spill.rs`: 1,375 lines.
  - `exec.rs`: unchanged at 11,611 lines.

`tests/architecture/exec_decomposition.rs` now guards:

- `exec/join.rs` declares `mod streaming;`.
- `exec/join/streaming.rs` exists.
- streaming orchestration markers are absent from the join root.
- `exec/join.rs` stays at or below 3,800 lines.
- the existing no-query-number executor scan covers `streaming.rs` through the recursive `exec_source_files()` helper.

`tests/architecture/binder_decomposition.rs` now allows `exec/join/streaming.rs` as part of the accepted executor
decomposition surface.

## Verification

Seal gate completed:

- `scripts/cargo-cgroup fmt --all`
- `scripts/cargo-cgroup fmt -- --check`
- `scripts/cargo-cgroup check -p toron --all-targets`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron --lib` - 1160 passed.
- `scripts/cargo-cgroup test -p toron --test architecture_test` - 2071 passed.
- `scripts/cargo-cgroup test -p toron --no-fail-fast` - passed.
- `scripts/cargo-cgroup test -p toron-tpch` - passed.
- `scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix` - 22/22 strict pass@compare.
- `scripts/cargo-cgroup run -p toron-tpch -- matrix` - 22/22 dev/strict pass@execute.
- `git diff --check`
