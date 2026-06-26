# ADR 351 Results - join spill submodule

## Summary

ADR 351 performed exactly one carve: the join spill storage layer moved from `crates/toron/src/exec/join.rs` to
`crates/toron/src/exec/join/spill.rs`. Streaming, in-memory hash join logic, aggregate folding, expression evaluation,
window execution, and context/runtime configuration stayed in place.

This is behavior-neutral source structure work. No budget values, cost-model choices, streaming paths, spill formats, or
runtime algorithms changed.

## Boundary map

Moved to `exec/join/spill.rs`:

- `SpilledJoinBuild`, `SpilledInnerJoinBuild`, `SpilledSemiAntiExistenceBuild`.
- Spill row-reference types: `SpilledBatchRowRef`, `SpilledInnerJoinMatch`, `SpilledExistenceRowRef`.
- Spill page/cache types: `JoinSpillPage`, `CachedJoinSpillPage`.
- Spill page read/write, IPC encode/decode, encryption nonce/tag, and plain IO helpers.
- Spill resident/index byte helpers:
  `join_spill_index_bytes`, `spilled_batch_join_index_bytes`, `semi_anti_existence_spill_index_bytes`.
- Spill-only constants:
  `JOIN_SPILL_PAGE_SIZE`, `JOIN_SPILL_CACHE_TARGET_BYTES`, `NEXT_JOIN_SPILL_ID`.

Kept in the join root:

- Streaming orchestration and shape extraction from ADRs 338-349.
- In-memory hash join paths and shared materializers.
- Shared helpers used by spill and non-spill join paths:
  `JoinKey`, `BuildRowList`, `JoinKeyPlan`, `key_arrays`, `join_key_at`, `project_batch_columns`,
  `budget_exhausted`, `join_output_materialization_memory_cap`, and
  `JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER`.

## Spill API surface

The parent join module imports/re-exports the build/ref types that already appeared in `pub(super)` join signatures:

- `SpilledJoinBuild`
- `SpilledInnerJoinBuild`
- `SpilledSemiAntiExistenceBuild`
- `SpilledBatchRowRef`
- `SpilledInnerJoinMatch`

The visibility remains `pub(in crate::exec)` for those types and their existing impl methods because the pre-existing
mixed `exec::tests` harness instantiates `SpilledInnerJoinBuild` / observes `SpilledSemiAntiExistenceBuild` through
`super::join::*`. Storage-local page/cache/IPC/nonce/index helpers are private inside `spill.rs`.

No `pub(crate)` spill API was introduced.

## Tests and fixtures

No test module was extracted in this cut. The spill-focused unit tests stay in the existing mixed `exec::tests` harness
because they depend on broad executor fixtures and shape helpers. Test extraction remains a possible later cleanup, but
the ADR 351 production ratchet is the spill storage module itself.

## Line counts and guards

- Before ADR 351: `exec/join.rs` was 7,143 lines.
- After ADR 351 and fmt:
  - `exec/join.rs`: 5,801 lines.
  - `exec/join/spill.rs`: 1,375 lines.
  - `exec.rs`: unchanged at 11,611 lines.
- `tests/architecture/exec_decomposition.rs` now guards:
  - `exec/join.rs` declares `mod spill;`.
  - `exec/join/spill.rs` exists.
  - spill storage markers are absent from the join root.
  - `exec/join.rs` stays at or below 5,900 lines.

## Verification

Seal gate completed:

- `scripts/cargo-cgroup fmt`
- `scripts/cargo-cgroup fmt -- --check`
- `scripts/cargo-cgroup check -p toron --all-targets`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron --lib` - 1160 passed.
- `scripts/cargo-cgroup test -p toron --test architecture_test` - 2067 passed.
- `scripts/cargo-cgroup test -p toron --no-fail-fast` - passed.
- `scripts/cargo-cgroup test -p toron-tpch` - passed.
- `scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix` - 22/22 strict pass@compare.
- `scripts/cargo-cgroup run -p toron-tpch -- matrix` - 22/22 dev/strict pass@execute.
- `git diff --check`
