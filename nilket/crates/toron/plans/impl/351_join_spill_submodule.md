# ADR 351 - Implementation plan: carve exec/join/spill.rs (spill storage only)

Pairs with `plans/adr/351_join_spill_submodule.md`. A pure `crates/toron` source-structure cut: carve the spill
**storage** layer out of `exec/join.rs` (7,143 lines) into `src/exec/join/spill.rs`, extend
`tests/architecture/exec_decomposition.rs`. **Behavior-neutral, exactly one carve (spill storage), streaming deferred to
ADR 352.**

## ADR granularity
ADR 351 carves spill storage **only**. Streaming (`exec/join/streaming.rs`) is ADR 352, deliberately separate so the
spill API surface is compiler-proven first. No aggregate/expr/window/context here.

## Phase 0 - map the spill boundary + the API surface
- **Enumerate the spill storage symbols:** `SpilledJoinBuild`, `SpilledInnerJoinBuild`,
  `SpilledSemiAntiExistenceBuild` + impls and row-reference structs, IPC page read/write + cache, encryption/tag
  helpers, spill-index resident helpers (`join_spill_index_bytes`, `spilled_batch_join_index_bytes`,
  `semi_anti_existence_spill_index_bytes`), and spill-only constants (`JOIN_SPILL_PAGE_SIZE`,
  `JOIN_SPILL_CACHE_TARGET_BYTES`, `NEXT_JOIN_SPILL_ID`).
- **Confirm clean bottom layer:** grep the spill symbols for any upward dependency (streaming/hash/aggregate). There
  should be none; if one exists, pull the shared helper into the join root, not into `spill.rs`.
- **Map the spill API surface:** the symbols streaming/hash call into spill -> the `pub(in crate::exec::join)` API of
  `spill.rs`. Minimize it; record it (the streaming ADR builds on it).
- **Classify shared helpers:** `JoinKey`, `BuildRowList`, `JoinKeyPlan`, `key_arrays`, `join_key_at`,
  `project_batch_columns`, `budget_exhausted`, `join_output_materialization_memory_cap`, and
  `JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER` stay in the join root unless Phase 0 proves an item is spill-only.
- **Threshold:** measure join-root + `spill.rs` line counts after the move; set the bounded `exec/join` root threshold
  below 7,143 with churn slack, close enough to fail on spill drift-back.

## D1 - the carve (pure move)
- Create `src/exec/join/spill.rs`; move the spill storage symbols there verbatim (no logic change); declare `mod spill;`
  in the join root; re-export / `pub(in crate::exec::join)` only the API streaming/hash require.
- Move spill-storage unit tests with the structs only when they move cleanly (inline `#[cfg(test)]` in `spill.rs` or a
  scoped `spill/tests.rs`). If a test depends on the mixed root executor harness or broad shared fixtures, leave it in
  the existing root test module and document the deferral in the results.
- **No other code moves** (streaming/hash/aggregate stay put - the exactly-one-carve fence).
- `Executor`, `ExecutionContext`, `materialize_scalar_rows`, `pub mod exec;` reachable as before.

## D2 - extend the guard (`exec_decomposition.rs`)
- `exec_join_root_declares_spill_module` - the join root contains `mod spill;`.
- `exec_join_spill_module_exists` - `src/exec/join/spill.rs` exists.
- `exec_join_root_excludes_spill_storage` - the spill storage symbols are no longer in the join root, including
  `SpilledJoinBuild`, `SpilledInnerJoinBuild`, `SpilledSemiAntiExistenceBuild`, `JoinSpillPage`,
  `write_join_spill_plaintext_page`, and `encode_record_batch_ipc`.
- `exec_join_root_is_bounded` - the join root line count <= the Phase-0 threshold (below 7,143, fails on spill
  drift-back); comment tracks the ratchet.
- extend the no-qNN scan to cover `exec/join/spill.rs`; public-surface assertion unchanged.

## Tests / safety net (behavior-neutral is the whole game)
- **The sf=1 board is the tripwire:** `correctness-matrix` 22/22 + `matrix` 22/22 stay green and **bit-identical**.
- Full `--lib`, `--test architecture_test` (incl. the extended guard), `--no-fail-fast`, `toron-tpch` green.
- `check -p toron --all-targets` + `-p toron-tpch --all-targets` (the move must not break benches/examples/tests).

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
#   exec/join.rs -> join root + exec/join/spill.rs; one carve; join root bounded < 7,143; sf=1 board bit-identical
```
- Run architecture_test AFTER fmt (the edit -> fmt -> fmt --check -> full architecture_test -> seal order).

## Results doc
`crates/toron/plans/impl/351_join_spill_submodule_results.md`: the Phase-0 spill boundary map (symbols moved, the
`pub(in crate::exec::join)` spill API surface, any shared helper kept in the root, and whether spill tests moved or
remained in the mixed root test harness); join-root + `spill.rs` line counts before/after + the guard threshold;
confirmation exactly one carve (only spill storage moved, streaming untouched); the `exec_decomposition.rs` extension;
the behavior-neutral proof (sf=1 22/22 bit-identical + full suite); the spill API surface recorded for ADR 352
(streaming).

## Risk / blast radius
- **Exactly the spill carve** - only spill storage moves; the guard's exclude-spill-storage check + a results-doc
  statement bound the scope; streaming is 352.
- **Behavior-neutral** - pure move; the sf=1 tripwire + `--no-fail-fast` catch any drift.
- **Spill API minimal** - `pub(in crate::exec::join)`, not `pub(crate)`; record its width (informs 352).
- **Test extraction local only** - do not move broad executor fixtures to satisfy a test-location ideal; local spill
  tests may move, mixed-harness tests may stay and be documented.
- **No perf/logic/budget/cost-model change.**

## Seal
`impl: adr 351 join spill submodule`. Do not commit wip drafts.
