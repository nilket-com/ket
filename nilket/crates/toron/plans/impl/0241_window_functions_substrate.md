# Impl Plan — Toron ADR 241: Window Functions Substrate

Pairs with `plans/adr/0241_window_functions_substrate.md`. A new `Window` logical + physical node for
ranking functions (`ROW_NUMBER`/`RANK`/`DENSE_RANK`) over `OVER (PARTITION BY ... ORDER BY ...)`. The
first new analytic operator in the 234→240 arc. **No frames, aggregate-window, optimizer/durable/Wendelon
surface.**

## Touch surface

- `ir.rs` — add `pub enum WindowFunction { RowNumber, Rank, DenseRank }`; add
  `LogicalPlan::Window { input: Box<LogicalPlan>, partition_by: Vec<Expr>, order_by: Vec<SortExpr>,
  functions: Vec<WindowFunction> }`. Compile-driven: add `Window` arms to every exhaustive `LogicalPlan`
  match (the 235/237/239 lesson).
- `ir/execution.rs` — add `ExecutionPlan::Window { input, partition_by: BoundExprProgram, order_by:
  Vec<BoundSortExpr>, functions: Vec<WindowFunction>, output_schema }`; add the `schema()` arm and every
  exhaustive `ExecutionPlan` match arm.
- `sql_planner.rs` — lower SELECT-projection window functions, evaluated **post-`HAVING`/pre-final-
  projection** (the standard SQL order): detect via the existing `expr_contains_window_function`; extract
  `Function.over = WindowType::WindowSpec`; lower `partition_by` + `order_by` (reuse the ORDER BY lowering
  → `SortExpr`); build `LogicalPlan::Window` over an inserted `Exchange(key = partition_by)` (empty
  partition → `partition_count = 1`). Reject `window_frame: Some(_)`, `WindowType::NamedWindow`,
  non-ranking functions, multiple **distinct** `OVER` specs, and `GROUP BY`/aggregate + window in one
  query. Keep the UPDATE/RETURNING/DISTINCT, derived-table, and the WHERE/GROUP BY/HAVING window rejects.
- `binder.rs` — bind `LogicalPlan::Window` → `ExecutionPlan::Window`: bind `partition_by` as an exchange
  key program and `order_by` as bound sort exprs; output schema = input schema + one `Int64` column per
  function (declared/alias name); facts = composite (row-preserving → carry input row bound).
- `exec.rs` — `execute_window`: over the co-located input, order each partition **internally** by
  `order_by` (reuse `execute_sort`/the sort kernel — NOT a separate `Sort` node, which the fragmenter
  treats as a global gather, fragment.rs:1189), then stream rows assigning ranks and **resetting** the
  counter on each partition-key change (`ROW_NUMBER` unconditional; `RANK` ties+gap; `DENSE_RANK` ties
  no-gap), emitting input columns + rank columns row-for-row. Add the dispatch arm in `execute_bound`.
- `distributed/fragment.rs` — `Window` rides the **node-agnostic** single-exchange-boundary path
  (`replace_exchange_with_scan`); no new fragmenter arm. Add `Window` only to the node-walking matches it
  trips: `find_exchange`, `count_exchanges`, `first_scan_metadata`, `replace_exchange_with_scan`, and any
  schema/child walkers.
- `coordinator.rs` / `server/mod.rs` / walkers — `Window` arms where `LogicalPlan`/`ExecutionPlan` are
  matched (compile-driven).
- tests: `tests/window_test.rs` (behavior) + `tests/distributed_process_test.rs` (value-discriminating) +
  `tests/architecture/window_functions.rs` (nine `adr241_*` guards).

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| OVER AST | `Function.over: Option<WindowType>` / `WindowSpec { partition_by, order_by, window_frame }` (sqlparser 0.62) |
| Window detection (reuse) | `expr_contains_window_function` (sql_planner.rs) |
| ORDER BY template | `SortExpr` (ir.rs:921), `LogicalPlan::Sort` (ir.rs:998) |
| Per-partition sort kernel (reuse) | `execute_sort` (exec.rs:832) |
| Grouped-aggregate exchange shape (model) | grouped `Aggregate(Exchange(key=group))` lowering (sql_planner.rs ~3667) |
| Node-agnostic exchange-boundary fragmenting | `replace_exchange_with_scan` (fragment.rs:377) |
| One-exchange-boundary limit (grounds single-spec) | `MultipleExchangeBoundaries` (fragment.rs) |

## Phase 0 — Red discriminators

`row_number_restarts_and_orders_within_partition`; `rank_and_dense_rank_differ_on_ties`;
`window_preserves_input_row_count`; `distributed_window_rank_is_value_discriminating`. Red on `main`
(window in SELECT and derived tables is rejected; no `Window` node).

## Phase 1 — `Window` IR nodes

Add `WindowFunction` + `LogicalPlan::Window` + `ExecutionPlan::Window`; add `Window` arms to all
exhaustive matches (compile-driven). `--all-targets` (new enum variants → exhaustive matches + benches).

## Phase 2 — Lowering

Lower SELECT-projection ranking window functions → `Window(Exchange(partition_by, input))`; reuse ORDER
BY lowering. Reject frames / named window / aggregate-window / multiple distinct specs / GROUP BY+window.
Lift only the SELECT-projection window reject; leave derived tables fenced in this cut.

## Phase 3 — Binder

Bind `Window` → `ExecutionPlan::Window` with output schema = input + rank columns (row-preserving).
Validate a single `OVER` spec; reuse the exchange-key binding for `partition_by`. Behavior:
`window_functions_evaluated_post_having` (a window over a `HAVING`-filtered result).

## Phase 4 — Executor

`execute_window`: partition the co-located rows, per-partition sort (reuse the sort kernel), sequential
rank, row-preserving emit. In-memory; budget the per-partition buffer.

## Phase 5 — Distributed

Confirm `Window(Exchange)` fragments via the generic boundary (probe `bind` + `fragment_plan` first —
the ADR 240 discipline). `distributed_window_rank_is_value_discriminating`: a partition scan-shared
across producers; ranks correct only after the partition exchange co-locates + orders it (236/238).

## Phase 6 — Guards + verification

Add the nine `adr241_*` guards. Reconcile the general new-`ExecutionPlan`/`LogicalPlan`-variant guards
(enum-shape/variant-count, distributed `g17` family). Run the FULL `architecture_test` before sealing (a
new node trips source-scanning guards `--all-targets` does NOT catch — the new-node checklist).

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test window
scripts/cargo-cgroup test  -p toron --test architecture_test adr241
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL (new-node reconciliations)
scripts/cargo-cgroup test  -p toron --test distributed_process_test   # FULL
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Ranks not per-partition / not ordered — `row_number_restarts_and_orders_within_partition` fails.
2. Ties mishandled — `rank_and_dense_rank_differ_on_ties` fails (1,1,3 vs 1,1,2).
3. Window collapses rows like an aggregate — `window_preserves_input_row_count` fails.
4. Non-deterministic tie/NULL order — the worker-count digest gate fails.
5. Empty `PARTITION BY` not a single ordered partition — `empty_partition_by_orders_whole_input` fails.
6. Two functions over one spec fan into two stages — `shared_over_spec_uses_one_window_node` fails.
7. Distributed ranks reachable from one producer — the distributed gate fails (236/238).
8. A deferred form (frame / aggregate-window / multiple specs) silently plans — the reject gate fails.

## Hygiene

- No m-word; no version-suffix idiom; a new *node* (window functions are not composable from existing
  ops — the honest exception to "reuse primitives"); goldens limited to ranked result rows.
- `--all-targets` (new enum variants); cargo via `scripts/cargo-cgroup`; FULL `architecture_test`.

## Open for the implementing session (verify-first)

1. Probe that `Window(Exchange(partition_by, scan))` binds AND `fragment_plan` succeeds via the generic
   boundary (a throwaway bind+fragment probe — the ADR 240 grounding discipline) before building the
   executor.
2. Confirm empty `PARTITION BY` → a single-partition exchange that co-locates all rows for one ordered
   partition.
3. Confirm `RANK`/`DENSE_RANK`/`ROW_NUMBER` tie + NULL ordering is total and deterministic across worker
   counts (reuse the existing sort policy).
4. Confirm the output schema is row-preserving (input columns + rank columns) and the binder/exec agree
   on column order.
5. Confirm a second *distinct* `OVER` spec is rejected (one-exchange-boundary limit) — and that two
   functions sharing one spec produce a single `Window` node.
