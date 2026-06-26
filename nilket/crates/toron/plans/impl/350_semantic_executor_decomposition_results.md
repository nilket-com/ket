# ADR 350 Results - semantic executor decomposition

## Scope

ADR 350 created the decomposition framework and performed exactly one carve:

- `crates/toron/src/exec.rs` remains the executor root.
- `crates/toron/src/exec/join.rs` now owns the join substrate.
- No `context`, `expr`, `aggregate`, `window`, or broad test-module carve was opened.

This is a behavior-neutral source move. No query logic, budget constant, cost model, or TPC-H harness logic was intentionally changed.

## Phase 0 - boundary map

Moved into `exec/join.rs`:

- `execute_hash_join` and join execution variants.
- Join output materialization helpers.
- In-memory join key/build/probe helpers.
- Cross-join match-all materialization.
- Spilled join builds, streamed-spilled inner/semi/anti builds, IPC page helpers, and spill byte accounting helpers.
- ADR 338-349 streaming/grace-partitioned join paths.
- Join runtime observation and telemetry helpers tied to join execution.

Kept in the root for this carve:

- `Executor`, `ExecutionContext`, and the public executor-facing configuration.
- Expression evaluation and aggregate state helpers.
- Exchange partitioning root logic.
- Window execution.
- `materialize_scalar_rows`.
- The existing inline `exec::tests` module.

The test harness was not extracted in this ADR. Join tests still live in the existing mixed executor test module and import the moved join module through `super::join::*`. That keeps the first carve focused on production source structure; extracting the mixed executor tests should be its own ratcheted follow-up if the council wants the ADR 176 test-module shape mirrored exactly.

## Visibility

The production dispatch path needs `execute_hash_join` from the root. The existing inline root tests also need access to many moved helpers, so this carve uses `pub(super)` inside `exec/join.rs` rather than `pub(crate)`. That keeps the widened surface limited to the parent `exec` module, but it is intentionally broader than the final desired surface until the tests are extracted or the helper tests move with a later join sub-split.

## Line-count ratchet

Measured after `fmt`:

- Before: `exec.rs` was 18,711 lines.
- After: `exec.rs` is 11,611 lines.
- New join module: `exec/join.rs` is 7,143 lines.
- Combined total: 18,754 lines.

The architecture guard sets `EXEC_ROOT_LINE_LIMIT = 11_700`, leaving 89 lines of slack for module declarations/import churn while failing if join logic drifts back into the root. Follow-up carves should lower the threshold.

## Architecture guard

Added `crates/toron/tests/architecture/exec_decomposition.rs` and registered it in the architecture harness.

The guard checks:

- root declares `mod join;`;
- `src/exec/join.rs` exists;
- the root excludes carved join clusters such as `execute_hash_join`, `materialize_join_selection`, `SpilledJoinBuild`, `SpilledInnerJoinBuild`, `JoinOutputRow`, `JoinKeyPlan`, `join_spill_index_bytes`, and `record_join_runtime_observation`;
- root line count stays under the ratchet threshold;
- executor modules have no `qNN` / `QNN` query-number identifiers;
- the public surface remains rooted (`Executor`, `ExecutionContext`, `materialize_scalar_rows`, and `pub mod exec`).

I also added shared architecture-test source helpers for the executor module tree and retargeted existing guards that intentionally inspect executor join code to read the concatenated executor source instead of only `exec.rs`.

## Verification

Script-wrapped checks:

- `scripts/cargo-cgroup fmt -- --check`
- `scripts/cargo-cgroup check -p toron --all-targets`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron --lib` - 1,160 passed.
- `scripts/cargo-cgroup test -p toron --test architecture_test` - 2,063 passed.
- `scripts/cargo-cgroup test -p toron --no-fail-fast` - passed.
- `scripts/cargo-cgroup test -p toron-tpch --no-fail-fast` - passed.
- `scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix` - 22/22 `pass@compare`.
- `scripts/cargo-cgroup run -p toron-tpch -- matrix` - 22/22 `pass@execute`.

## Extra sf=1 refresh finding

I attempted an additional current-binary sf=1 reachability refresh for the dev/strict matrix. That run stopped at `q08 --mode dev --sf 1` with:

`memory_budget_exhausted`, producer `join output materialization`, requested `1,735,851,439`, available `1,073,741,824`.

I compared the exact same command in a detached worktree at the ADR 350 plan baseline (`3a1f5ff`) and got the same failure and byte counts. This is not an ADR 350 decomposition regression.

The canonical `matrix` renderer is scale-agnostic and chooses the latest row per `(query, mode)`, so a later default-scale q08 dev row restores the documented 22/22 matrix view. The sf=1 q08 dev finding should be treated as a separate scale/matrix-hygiene issue before using `matrix` as a scale-specific seal.

## Outcome

ADR 350's one-carve decomposition is complete:

- join code is out of the root;
- the root is bounded by an architecture ratchet;
- the executor module tree is de-branded from TPC-H query numbers;
- the public executor surface remains available;
- broad Toron and TPC-H suites pass after the move.

Next decomposition ratchets can proceed as separate ADRs: join sub-split, aggregate, expr, then context/root cleanup.
