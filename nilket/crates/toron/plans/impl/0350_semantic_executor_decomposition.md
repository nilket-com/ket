# ADR 350 - Implementation plan: Semantic Executor Decomposition (join-first)

Pairs with `plans/adr/0350_semantic_executor_decomposition.md`. A pure `crates/toron` source-structure cut: convert
`exec.rs` (18,711 lines) into a thin bounded root + `src/exec/join.rs`, add `tests/architecture/exec_decomposition.rs`
(mirroring `binder_decomposition.rs`), **behavior-neutral, exactly one carve (join), no perf/logic/budget change.**

## ADR granularity
This is **not** a multi-phase umbrella ADR. ADR 350 has only the framework + `join` carve. D2 is the guard for that
carve, not permission to start the next module. Subsequent carves (`join` sub-split, aggregate, expr, window, context)
are separate ADRs with their own measured boundaries, ratchets, guards, and behavior-neutral proof.

## Phase 0 - map the join boundary + set the threshold
- **Enumerate the join-substrate symbols** + their private-helper dependencies: `execute_hash_join` + variants,
  join-into-global-aggregate, `materialize_join_selection`, `build_in_memory_join_rows`, the 340-349 streamed/spilled/
  partitioned paths, `SpilledJoinBuild`/`SpilledInnerJoinBuild` + IPC page helpers, `join_spill_index_bytes` /
  `join_take_index_bytes` / key-bytes / bucketing, join unit tests.
- **Identify the cross-module surface:** which helpers must become `pub(crate)` to cross the line (minimize - prefer
  private inside `join`); which symbols are shared with aggregate/expr and **stay in the root** for now (clean boundary,
  not a tangle).
- **Confirm pure move:** the carve compiles with no logic edits; nothing relocated except join.
- **Threshold plan:** the exact root line-count threshold is measured **after** the move (post-carve root lines + a
  small fixed slack for module declarations / import churn), set below 18,711 and close enough to fail on join
  drift-back. Record the number + a comment in the guard.

## D1 - the carve (pure move)
- Create `src/exec/join.rs`; move the join-substrate symbols there verbatim (no logic change); declare `mod join;` in
  the root; re-export / `pub(crate)` only what the boundary requires.
- Move join unit tests with the module (scoped `exec/join/tests.rs` or inline `#[cfg(test)]` in `join.rs`).
- **No other module is created or moved** (no expr/aggregate/window/context relocation - the exactly-one-carve fence).
- `Executor`, `ExecutionContext`, `materialize_scalar_rows`, `pub mod exec;` remain reachable as before.

## D2 - the guard (`tests/architecture/exec_decomposition.rs`, mirror 176)
- `exec_root_declares_join_module` - root contains `mod join;`.
- `exec_root_excludes_carved_join_clusters` - the moved join symbols are no longer in the root (the carve happened).
- `exec_join_module_exists` - `src/exec/join.rs` exists.
- `exec_root_is_bounded` - root line count <= the Phase-0 threshold (below 18,711, fails on join drift-back); comment
  tracks the ratchet.
- `exec_modules_have_no_query_number_identifiers` - no `qNN` / TPC-H query-number symbols in `src/exec/` modules.
- `exec_public_surface_unchanged` - `Executor`, `ExecutionContext`, `materialize_scalar_rows` reachable (a
  reference/compile assertion).
- Register the new guard file in the `architecture_test` harness.

## Tests / safety net (behavior-neutral is the whole game)
- **The sf=1 board is the tripwire:** `correctness-matrix` 22/22 + `matrix` 22/22 stay green and **bit-identical**
  (no behavior change from a pure move).
- Full script-wrapped suite: `scripts/cargo-cgroup test -p toron --lib`,
  `scripts/cargo-cgroup test -p toron --test architecture_test` (incl. the new guard),
  `scripts/cargo-cgroup test -p toron --no-fail-fast`, and `scripts/cargo-cgroup test -p toron-tpch` all green.
- `scripts/cargo-cgroup check -p toron --all-targets` + `scripts/cargo-cgroup check -p toron-tpch --all-targets` (the
  move must not break benches/examples).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # incl. exec_decomposition.rs
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix        # 22/22 unchanged (behavior-neutral)
scripts/cargo-cgroup run -p toron-tpch -- matrix                    # 22/22 unchanged
git diff --check
#   exec.rs -> thin root + exec/join.rs; one carve; root bounded < 18,711; sf=1 board bit-identical
```
- Run the architecture_test AFTER fmt (fmt can mutate scanned source - the seal order is edit -> fmt -> fmt --check ->
  full architecture_test -> seal).

## Results doc
`crates/toron/plans/impl/0350_semantic_executor_decomposition_results.md`: the Phase-0 join boundary map (symbols moved,
the `pub(crate)` surface widened, symbols kept in root); the measured root line count before/after + the guard
threshold + slack; confirmation exactly one carve (only join moved); the `exec_decomposition.rs` checks; the
behavior-neutral proof (sf=1 22/22 bit-identical + full suite green); the ratchet's next steps (351 sub-split, 352
aggregate, 353 expr, context).

## Risk / blast radius
- **Exactly one carve** - only join moves; the guard's carved-clusters check + a results-doc statement bound the scope.
- **Behavior-neutral** - pure move; the sf=1 tripwire + `--no-fail-fast` catch any drift.
- **Minimal surface** - `pub(crate)` only where required; shared symbols stay in root (no premature split).
- **Guard from day one** - line-count ratchet fails on join drift-back; no backsliding.
- **No perf/logic/budget/cost-model change.**

## Seal
`impl: adr 350 semantic executor decomposition`. Do not commit wip drafts.
