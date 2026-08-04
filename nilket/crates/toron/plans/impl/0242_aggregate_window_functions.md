# Impl Plan — Toron ADR 242: Aggregate Window Functions

Pairs with `plans/adr/0242_aggregate_window_functions.md`. Extend the ADR 241 `Window` node + operator to
carry aggregate calls (`SUM/AVG/COUNT/MIN/MAX(x) OVER (...)`) on the SQL default frame (`RANGE UNBOUNDED
PRECEDING AND CURRENT ROW`), reusing the existing aggregate kernels. **No explicit-frame syntax, no new
operator, no optimizer/durable/Wendelon surface.**

## Touch surface

- `ir.rs` — extend `WindowFunction` with `Aggregate(AggregateFunction)`; extend `WindowFunctionCall` with
  `args: Vec<Expr>` (ranking: empty; aggregate: one). Add `Aggregate` arms to the `WindowFunction`
  matches (`window_function_default_name`, etc.).
- `ir/execution.rs` — `ExecutionPlan::Window.functions` becomes `Vec<BoundWindowCall>` where
  `BoundWindowCall { function: WindowFunction, arg: Option<BoundExprProgram> }` (aggregate arg bound;
  `None` for ranking). Keep `output_schema`.
- `sql_planner.rs` — `extract_window_call` accepts `COUNT(*)` (no argument) and `COUNT/SUM/MIN/MAX/AVG`
  with exactly one argument in addition to the ranking functions; carry the arg(s) in
  `WindowFunctionCall.args` (`COUNT(*)` → empty). Multiple window calls sharing one `OVER` spec stay a
  single `Window` node. Keep the explicit-frame (incl. bounded `ROWS`/`RANGE`), named-window,
  DISTINCT-window-aggregate, multiple-spec, and WHERE/GROUP BY/HAVING/derived-table rejects. **No
  `WindowFrame` IR payload in this cut** — the default frame is implicit in the operator's peer loop.
- `binder.rs` — in the `Window` arm: bind each aggregate call's arg to a `BoundExprProgram`
  (`bind_expr_program_scoped`); compute each window-aggregate output column's type via the existing
  aggregate result-type logic (e.g. `aggregate_schema`/the per-call result type). Set output-column
  nullability per aggregate: `COUNT(*)`/`COUNT(x)` → non-null `Int64`; `SUM`/`AVG`/`MIN`/`MAX` → nullable.
  Ranking columns stay non-null `Int64`. Row-preserving facts unchanged.
- `exec.rs` — extend `execute_window`: after the partition sort + peer detection, for each aggregate call
  evaluate its arg and maintain a running accumulator (Sum/Count/Min/Max/Avg) over the sorted partition,
  finalizing per **peer group** (RANGE default frame) and assigning that value to every row in the group;
  whole-partition is the single-peer-group case. Reuse the existing scalar-aggregate accumulators/kernels;
  NULL/empty semantics match the existing aggregates.
- `distributed/fragment.rs` — no change: aggregate-window rides the 241 `Window(Exchange(partition_by))`
  node-agnostic boundary.
- tests: `tests/window_test.rs` + `tests/distributed_process_test.rs` + `tests/architecture/window_functions.rs`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Window node + call | `WindowFunction` / `WindowFunctionCall` (ir.rs); `ExecutionPlan::Window` (ir/execution.rs) |
| Window lowering (extend) | `fn extract_window_call` / `fn lower_select_window` (sql_planner.rs) |
| Window bind (extend) | the `LogicalPlan::Window` arm in `bind_plan_with_trace` (binder.rs) |
| Partition sort + peer detection (reuse) | `fn execute_window` peer loop (exec.rs) |
| Aggregate result type / kernels (reuse) | `AggregateFunction`, `aggregate_schema`, scalar-aggregate accumulators |
| Arg program evaluation | `Executor::evaluate_bound_program_without_storage` |

## Phase 0 — Red discriminators

`whole_partition_aggregate_window`; `running_sum_window_is_cumulative`;
`running_window_range_ties_share_value`; `count_star_window_counts_frame_rows`;
`aggregate_window_multiple_functions_share_one_window_node`;
`distributed_window_aggregate_is_value_discriminating`. Red on `main` (aggregate-window rejected).

## Phase 1 — Extend the call representation

Add `WindowFunction::Aggregate(AggregateFunction)` + `WindowFunctionCall.args`; make
`ExecutionPlan::Window.functions` carry bound aggregate args (`BoundWindowCall`). `--all-targets` for the
exhaustive `WindowFunction`/`Window` matches.

## Phase 2 — Lowering

`extract_window_call` accepts `COUNT(*)` (arg-less) and the five aggregates with one arg. Reject explicit
frames / DISTINCT-window / multiple specs (unchanged).

## Phase 3 — Binder + verify-first probe

Bind aggregate args + window-aggregate output column types. **VERIFY-FIRST:** probe that
`COUNT(*) OVER (PARTITION BY p)` and `agg(x) OVER (PARTITION BY p [ORDER BY o])` bind AND
`fragment_plan` succeeds (extends the 241 probe) before building the running executor.

## Phase 4 — Executor

Peer-aware running accumulator per aggregate call, finalized per peer group (RANGE), assigned to all
rows in the group; whole-partition = one peer group. Reuse the scalar-aggregate accumulators.

## Phase 5 — Tests, guards, verify, seal

Behavior gates (whole-partition, running cumulative, RANGE-ties-share, `COUNT(*)` frame count,
multiple-functions-share-one-node, row-preservation, AVG null, deferred rejects); the distributed
value-discriminating gate; the eight `adr242_*` guards; reconcile the
ADR 241 ranking-only guards + any `WindowFunction` variant-count pins. Full verification gate. Commit
`impl: adr 242 aggregate window functions`.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test window
scripts/cargo-cgroup test  -p toron --test architecture_test adr242
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL (reconciled 241 + variant pins)
scripts/cargo-cgroup test  -p toron --test distributed_process_test   # FULL
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Whole-partition not per-row total — `whole_partition_aggregate_window` fails.
2. Running not cumulative / not per-partition — `running_sum_window_is_cumulative` fails.
3. Naive `ROWS` running fold instead of `RANGE` — `running_window_range_ties_share_value` fails (tied
   rows would get different values).
4. Window aggregate collapses rows — `window_aggregate_preserves_row_count` fails.
5. AVG/SUM over empty/all-NULL not NULL — `running_avg_window_null_and_empty_semantics` fails.
6. A deferred form (explicit frame / DISTINCT-window) silently plans — the reject gate fails.
7. Distributed aggregate reachable from one producer — the distributed gate fails (236/238).
8. `COUNT(*)` window unsupported or miscounted — `count_star_window_counts_frame_rows` fails.
9. Multiple functions over one spec fan into multiple `Window` nodes —
   `aggregate_window_multiple_functions_share_one_window_node` fails.

## Hygiene

- No m-word; no version-suffix idiom; reuse the `Window` node + aggregate kernels (no new operator);
  goldens limited to window result rows.
- `--all-targets` (the `WindowFunction` variant change); cargo via `scripts/cargo-cgroup`; FULL
  `architecture_test` before sealing.

## Open for the implementing session (verify-first)

1. Probe `COUNT(*) OVER (PARTITION BY p)` and `agg(x) OVER (PARTITION BY p ORDER BY o)` bind +
   `fragment_plan` succeeds before the executor.
2. Confirm the default frame is RANGE: tied `ORDER BY` rows share the cumulative-through-peer-group value
   (not a row-by-row ROWS fold).
3. Confirm whole-partition (no ORDER BY) = one peer group → partition total on every row.
4. Confirm window-aggregate output column types AND nullability match the aggregate (AVG→nullable Float64,
   COUNT→non-null Int64, etc.) and the row count is preserved.
5. Confirm empty/all-NULL aggregate-window semantics (SUM/AVG → NULL, COUNT → 0).
6. Confirm `COUNT(*)` (arg-less) and `COUNT(x)` map onto the existing aggregate kernels and that
   `COUNT(*)` window counts frame rows (running count through the peer group under `ORDER BY`).
