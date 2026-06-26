# ADR 353 - Implementation plan: carve exec/aggregate.rs (aggregate executor/state cluster)

Pairs with `plans/adr/353_aggregate_executor_submodule.md`. A pure `crates/toron` source-structure cut, **gated on the
coupling map**: carve the **full aggregate executor/state cluster** out of the `exec.rs` root (11,611 lines) into
`src/exec/aggregate.rs`, with a deliberate `exec::aggregate` API for the three callers. Behavior-neutral, exactly one
carve, dispatch stays rooted. Kernels already separate (`aggregate_kernel.rs`, 021).

## Phase 0 - the coupling map (the go/no-go; produce BEFORE moving)
- **Enumerate the full cluster:** global (`bound_global_aggregate`, `fold/init/finalize_global_aggregate_state`),
  grouped-bound (`bound_grouped_aggregate`, `fold_bound_grouped_aggregate_batch`,
  `finalize_bound_grouped_aggregate`), state+group (`AggState`, `init_bound_states`, state update/finalize helpers,
  `GroupValue`, `group_value`, group-materialization helpers), aggregate executor tests. Treat
  `#[cfg(test)] grouped_aggregate` as a legacy/test helper: move it only if it stays local to aggregate tests; otherwise
  keep it rooted/mixed-harness and document why.
- **Enumerate ALL call sites** (not just streaming): `exec.rs` (root dispatch arm), `exec/join.rs` (hash-join-into-
  aggregate), `exec/join/streaming.rs` (`try_streaming_*` aggregate methods). Design the `exec::aggregate`
  `pub(in crate::exec)` API surface these three use - one deliberate surface.
- **Per symbol/call site bucket:** move-now (cluster, one-way deps) / stay-root (dispatch, the legacy/unbound aggregate
  branch, expr internals 354, scan/exchange, mixed-harness tests) / defer (drags dispatch or private expr internals).
- **expr discriminator:** stable API -> proceed; private expr internals -> stop, recommend expr-first (354).
- **Threshold:** measure exec-root + `aggregate.rs` line counts after the move; set the exec-root ratchet below 11,611
  with churn slack, close enough to fail on aggregate drift-back.

## D1 - the carve (full cluster; only if map-proven)
- Create `src/exec/aggregate.rs`; move the full executor/state cluster verbatim (no logic change), free fns + a split
  `impl Executor` block for the methods; declare `mod aggregate;` in the exec root.
- **Dispatch stays rooted:** root `execute` / `execute_bound` routing stays in the root. The bound aggregate dispatch
  re-points to the aggregate module API; the legacy/unbound logical aggregate branch, including its inline global mock,
  stays rooted unless Phase 0 extracts a tiny deliberate helper that sharpens the API. This is not a dispatch refactor.
- **Re-point the three callers** (`exec.rs`, `exec/join.rs`, `exec/join/streaming.rs`) to the `exec::aggregate` API
  (path/visibility only - behavior-neutral).
- Kernels stay in `aggregate_kernel.rs`; aggregate.rs calls the kernel module.
- Local aggregate tests move; mixed-harness tests stay (documented). `#[cfg(test)] grouped_aggregate` follows this
  locality rule rather than forcing the legacy/unbound branch into `aggregate.rs`.
- `Executor`, `ExecutionContext`, `materialize_scalar_rows`, `pub mod exec;` reachable as before.
- **No-go branch:** if Phase 0 shows private-expr-internals coupling, do not create `aggregate.rs` or a fake guard;
  the results doc records the map + the expr-first (354) recommendation.

## D2 - extend the guard (`exec_decomposition.rs`)
- **If carved:** exec root declares `mod aggregate;`; `src/exec/aggregate.rs` exists; the moved cluster symbols are no
  longer in the exec root; exec-root line count ratcheted below 11,611 (fails on aggregate drift-back); no qNN in
  `aggregate.rs`; public-surface assertion unchanged.

## Tests / safety net (behavior-neutral is the whole game)
- **The sf=1 board is the tripwire:** `correctness-matrix` 22/22 + `matrix` 22/22 **bit-identical**.
- Full `--lib`, `--test architecture_test` (incl. the extended guard), `--no-fail-fast`, `toron-tpch` green.
- `check -p toron --all-targets` + `-p toron-tpch --all-targets`.
- **LSP diagnostics advisory** - stale errors during the move are expected (350/351/352); authoritative = `scripts/
  cargo-cgroup check` + the architecture guard + the board commands. Ground on those.

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
#   exec.rs -> root + exec/aggregate.rs; full cluster; dispatch rooted; root ratcheted < 11,611; board bit-identical
```
- Run architecture_test AFTER fmt (edit -> fmt -> fmt --check -> full architecture_test -> seal).

## Results doc
`crates/toron/plans/impl/353_aggregate_executor_submodule_results.md`: the three-bucket coupling map (full cluster +
ALL three call sites + the `exec::aggregate` API surface); the go/no-go decision (carved vs expr-first) + one-way-
dependency evidence; if carved: exec-root + `aggregate.rs` line counts before/after + the guard threshold, the
aggregate-module API surface, the dispatch-stayed-rooted confirmation, the legacy/unbound aggregate branch disposition,
which callers re-pointed, which tests moved vs stayed; the behavior-neutral proof (sf=1 22/22 bit-identical + full
suite); confirmation the LSP errors during the move were stale (`cargo-cgroup check` clean).

## Risk / blast radius
- **Full cluster** - nothing stranded; `AggState`/`GroupValue`/helpers move with the methods.
- **Dispatch rooted** - `execute` / `execute_bound` routing stays; the legacy/unbound aggregate branch is not forced
  through the aggregate module; not a dispatch cleanup.
- **Deliberate API** - one `exec::aggregate` surface for the three callers, not incidental scatter.
- **Behavior-neutral** - pure move; sf=1 tripwire + `--no-fail-fast`.
- **Map-gated** - expr-first if private-expr-internals coupling; no forced carve.
- **No perf/logic/budget/cost-model change.**

## Seal
`impl: adr 353 aggregate executor submodule` (if carved), or a planning-only outcome recommending expr-first. Do not
commit wip drafts.
