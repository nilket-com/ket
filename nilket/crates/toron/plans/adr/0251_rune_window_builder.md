# ADR 251 — Rune Window Builder

## Status

Proposed.

## Context

The window arc (241–250) is reachable only through `df.sql("...")`. `rune_engine.rs` has no window
surface — no `.window` verb, no `WindowFunction`/`LogicalPlan::Window`. For the primary, prescriptive
Rune interface that is the SQL-only default the Rune-primary discipline forbids. ADR 241 deferred a
"Rune window builder" by design, building the substrate frontend-agnostic and keeping 242–250's
`no_new_..._rune_surface` guards so it lands once over the finished surface. This is that follow-on.

## Grounding — a Rune-frontend cut over a complete mechanism

`RuneDataFrame { plan: LogicalPlan }` builds the IR directly via verbs; `.aggregate(group_expr,
aggr_expr)` (`rune_engine.rs:388`) builds `LogicalPlan::Aggregate`, with aggregates as free-function
constructors (`sum(expr)` → `RuneAggr { func, args }`). The window verb is the exact parallel: it
builds the **same** `LogicalPlan::Window`-over-`Exchange` node SQL lowers to. No IR / binder /
executor / fragmenter change — one verb plus a call/frame vocabulary reaches all of 241–250
(ranking, aggregate, frames, value, NTILE).

SQL must *group* window calls by their inline `OVER` spec (the ADR 248 machinery). Rune's
`.window(partition, order, calls)` makes the spec explicit per verb call, so **chaining
`.window(...).window(...)` stacks** — each call is one node and stacking is the user's composition.
The Rune builder therefore needs none of the 248 grouping; it reuses only the single-spec node build,
which is **extracted to a frontend-agnostic helper** shared with `lower_select_window`. Named windows
(250) are moot in Rune — a host `let w = (...)` gives spec reuse.

## Decision

Add `RuneDataFrame::window(partition_by: Vec<RuneExpr>, order_by: Vec<RuneSortKey>, calls:
Vec<WindowFunctionCall>) -> Self`, building one `Window`-over-`Exchange` node via a shared helper:

```
fn window_over_exchange(input, partition_by: Vec<Expr>, order_by: Vec<SortExpr>,
                        functions: Vec<WindowFunctionCall>) -> LogicalPlan
```

extracted from `lower_select_window`'s per-group node build (`Exchange{key, partition_count =
empty ? 1 : DEFAULT_AGGREGATE_PARTITION_COUNT}` then `Window`) and reused by both frontends. SQL's
loop calls it once per spec group; Rune's verb calls it once. Multiple specs in Rune = chain the verb.

`WindowFunctionCall` values, built by the full vocabulary:

- **ranking** — `rank()`, `row_number()`, `dense_rank()` (no args, no frame).
- **aggregate** — `sum(expr).over()` / `count_all().over()` / etc.: a `RuneAggr::over()` adapter →
  `WindowFunction::Aggregate`, reusing the existing aggregate constructors (no name collision with the
  GROUP BY ones).
- **value** — `lag(expr, offset, default)`, `lead(...)`, `first_value(expr)`, `last_value(expr)`,
  `nth_value(expr, n)` → `WindowFunction::Value`.
- **NTILE** — `ntile(n)` → `WindowFunction::Ntile { buckets }`.
- **frames** — `rows(start, end)` / `range(start, end)` / `groups(start, end)` with bound constructors
  (`unbounded_preceding()`, `preceding(n)`, `current_row()`, `following(n)`, `unbounded_following()`),
  attached via `WindowFunctionCall.frame`. The existing frame-validity rules (ranking/NTILE reject a
  frame; RANGE single-key; GROUPS needs order) are enforced in the binder/IR, unchanged.
- **output name** — `WindowFunctionCall.output_name`.

The verb + constructors register in the Rune module (`m.associated_function(...)` block).

## Consequences

- The window surface is first-class on both frontends; the Rune-primary gap across 241–250 closes in
  one cut.
- A resolved Rune window plan is byte-identical to the SQL one, so binding, execution, and
  fragmentation are already covered — no new distributed gate (the 241/248 cardinals apply).
- The shared `window_over_exchange` helper removes the duplicated node-build, so future window-node
  changes touch one place.

## Discriminators (each fails on `main` — Rune has no window verb)

- **`rune_window_ranking_matches_sql`** — `df.window([col(grp)], [col(score).asc()],
  [rank().named("r")])` yields the same plan + result as `RANK() OVER (PARTITION BY grp ORDER BY
  score)`.
- **`rune_window_aggregate_with_frame_matches_sql`** — `sum(col(score)).over().frame(rows(preceding(1),
  current_row())).named("s")` matches the SQL ROWS-frame equivalent (frames + 243 reach Rune).
- **`rune_chained_windows_stack`** — `.window(specA).window(specB)` builds two stacked Window nodes
  (the Rune analog of 248, by composition).
- **`rune_window_value_and_ntile_reach_ir`** — `lag(...)` / `ntile(n)` build the 246/249 variants and
  execute.
- **`rune_window_node_matches_sql_node`** — a Rune-built single-spec window plan equals the
  SQL-lowered plan for the same spec (proves the shared helper, not a parallel build).

## Architecture guards

- `adr251_rune_has_window_verb` — `RuneDataFrame::window` + the call/frame constructors are registered
  in the Rune module.
- `adr251_window_node_build_is_frontend_agnostic` — `window_over_exchange` is the single node-build,
  called by both `lower_select_window` and the Rune verb (no Rune-specific Window lowering, no
  duplicated `Exchange`+`Window` construction).
- `adr251_rune_window_adds_no_ir_or_executor_surface` — the cut touches only `rune_engine.rs` (+ the
  extracted helper); no new IR variant, binder arm, executor arm, or fragmenter path.

## Deferred / out of scope

- Window inheritance/extension, IGNORE NULLS, EXCLUDE — deferred at the IR/SQL level; not a Rune
  concern until the mechanism lands.

## Forward arc

251 Rune window builder (this) → the window surface is complete on both frontends; ready to branch.
