# Implementation Plan — ADR 251: Rune Window Builder

## Summary

Add a `RuneDataFrame::window(partition_by, order_by, calls)` verb that builds the same
`LogicalPlan::Window`-over-`Exchange` node SQL lowers to, via a shared frontend-agnostic
`window_over_exchange` helper extracted from `lower_select_window`. Reuse the existing public IR
`WindowFunctionCall` / `WindowFrame` types as the Rune-facing constructor vocabulary. Rune-frontend
only — no IR / binder / executor / fragmenter change.

## Verify-first probe

1. Confirm Rune has no window verb today (no `window` in the `m.associated_function(...)` block;
   `rune_engine.rs` has zero `LogicalPlan::Window`).
2. Hand-build a `LogicalPlan::Window`-over-`Exchange` in a scratch Rune-side test and run it through
   the same bind/execute path Rune queries use — it must bind and execute (proves the mechanism is
   frontend-agnostic, so the verb only needs to construct the node).
3. Confirm `lower_select_window`'s per-group node build (`Exchange` then `Window`) is a
   self-contained block extractable without behavior change (the 248/250 window tests are the
   regression net).

## Phase 1 — Extract the shared node-build helper

Pull the `Exchange`+`Window` construction out of `lower_select_window` into a frontend-neutral
helper:

```
fn window_over_exchange(input: LogicalPlan, partition_by: Vec<Expr>, order_by: Vec<SortExpr>,
                        functions: Vec<WindowFunctionCall>) -> LogicalPlan
```

placed where both `sql_planner` and `rune_engine` can call it (e.g. `ir.rs` or a planner-common
module; ensure `DEFAULT_AGGREGATE_PARTITION_COUNT` is reachable — move/re-export if needed).
`lower_select_window`'s stacking loop becomes `node = window_over_exchange(node, group.partition_by,
group.order_by, group.functions)`. Behavior-neutral: the full `window_test` + multi-spec distributed
gates must stay green (this is the proof the extraction didn't shift SQL behavior).

## Phase 2 — The `.window` verb + call/frame methods

In `rune_engine.rs`:

- `WindowFunctionCall` builder methods `frame(self, WindowFrame) -> Self` and
  `named(self, String) -> Self`.
- `WindowFrame` convenience constructors for `rows(start, end)` / `range(start, end)` /
  `groups(start, end)` and the bound helpers.
- `RuneDataFrame::window(self, partition_by: Vec<RuneExpr>, order_by: Vec<RuneSortKey>, calls:
  Vec<WindowFunctionCall>) -> Self` → `window_over_exchange(self.plan, partition_by exprs, order_by
  SortExprs, calls)`. Empty `calls` panics (like `sort` on empty keys).

## Phase 3 — The constructor vocabulary + registration

Free functions / methods returning `WindowFunctionCall`:

- ranking: `rank()`, `row_number()`, `dense_rank()`.
- aggregate: `RuneAggr::over(self) -> WindowFunctionCall` (reuses `sum`/`count`/`count_all`/`min`/
  `max`/`avg`; maps `RuneAggr { func, args }` → `WindowFunction::Aggregate(func)` + args).
- value: `lag(expr, offset, default)`, `lead(expr, offset, default)`, `first_value(expr)`,
  `last_value(expr)`, `nth_value(expr, n)`.
- NTILE: `ntile(n)`.
- frames: `rows(start, end)` / `range(start, end)` / `groups(start, end)` + bound constructors
  `unbounded_preceding()` / `preceding(n)` / `current_row()` / `following(n)` /
  `unbounded_following()` → `WindowFrame`.

Register all of these + the `window` verb + `over`/`frame`/`named` in the Rune module
(`m.associated_function(...)`). Default output names (when `.named` is omitted) follow
`window_function_default_name` for parity with SQL.

The frame-validity rules (ranking/NTILE reject a frame; RANGE single ORDER BY key; GROUPS needs
order) already live in the binder/IR — a malformed Rune frame errors there, no new fence.

## Phase 4 — Tests, guards, verification, seal

### Behavioral gates (Rune integration tests)

Build via the Rune engine and assert parity with the SQL spelling (reuse a window-friendly fixture):
- `rune_window_ranking_matches_sql`
- `rune_window_aggregate_with_frame_matches_sql`
- `rune_chained_windows_stack` (two `.window` calls → two stacked nodes)
- `rune_window_value_and_ntile_reach_ir`
- `rune_window_node_matches_sql_node` (a Rune single-spec plan equals the SQL-lowered plan — proves
  the shared helper)

### Architecture guards (`tests/architecture/`)

- `adr251_rune_has_window_verb` (rune module registers `window` + the constructors)
- `adr251_window_node_build_is_frontend_agnostic` (`window_over_exchange` is the single node-build,
  called by `lower_select_window` and the Rune verb; no duplicated `Exchange`+`Window` build)
- `adr251_rune_window_adds_no_ir_or_executor_surface` (only `rune_engine.rs` + the helper change; no
  new IR variant / binder arm / executor arm / fragmenter path)

### Verification gate (route every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test            # SQL behavior unchanged by the extraction
scripts/cargo-cgroup test  -p toron <rune integration test>
scripts/cargo-cgroup test  -p toron --test architecture_test adr251
scripts/cargo-cgroup test  -p toron --test architecture_test      # FULL
scripts/cargo-cgroup test  -p toron --test distributed_process_test # FULL (extraction regression)
scripts/cargo-cgroup test  -p toron --lib                         # no IR/wire change
cargo fmt -p toron -- --check
git diff --check
```

### Seal

Commit the implementation as `impl: adr 251 rune window builder` with the
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.

## Risk notes

- **Extraction is behavior-neutral**: `window_over_exchange` must reproduce `lower_select_window`'s
  node build exactly (same `partition_count` rule); the full `window_test` + multi-spec distributed
  gates are the regression net.
- **Helper placement / `DEFAULT_AGGREGATE_PARTITION_COUNT`**: the const must be reachable from the
  helper's new home; re-export rather than duplicate the value.
- **Constructor surface size**: the vocabulary is broad but each constructor is a thin IR map; keep
  them uniform (all yield `WindowFunctionCall`) so the verb takes one `Vec<WindowFunctionCall>`.
- **No new fences**: frame-validity stays in the binder/IR; the Rune verb adds no parallel validation
  (a bad frame errors identically to the SQL path).
