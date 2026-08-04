# Toron ADR 241: Window Functions Substrate

**Status:** Draft (iter-1 of the adr/impl dual). **Migration:** `plans/impl/0241_window_functions_substrate.md`.
**Category:** Toron core query surface — the analytic (window) function primitive. **The first new
analytic operator in the 234→240 arc:** a `Window` logical + physical node for ranking functions
(`ROW_NUMBER`/`RANK`/`DENSE_RANK`) over `OVER (PARTITION BY ... ORDER BY ...)`. No frames,
aggregate-window, optimizer-evidence, durable key, or Wendelon change.

## Why this cut exists

Window functions are detection-only and fenced everywhere — there is no substrate:
`expr_contains_window_function` (sql_planner.rs) only *rejects* them (UPDATE assignment :1451,
RETURNING :2587, DISTINCT-with-window :3517), `named_window` is also fenced in SELECT
(`WINDOW is not supported`), `unsupported derived table projection` still fences windows in derived
tables, and there is **no `Window` node** in `ir.rs`/`ir/execution.rs`/`exec.rs`. So this is a
fresh cut, not the completion of a partial path.

**The load-bearing departure.** ADRs 234–240 all *composed existing primitives*. A ranking function is
a **stateful sequential** computation over an *ordered partition* — "assign 1,2,3… within each
partition in ORDER BY order" cannot be expressed by composing `Aggregate`/`Sort`/`Project`. So this cut
introduces a genuine new operator. (Note: window functions, unlike `Aggregate`, are **row-preserving** —
N input rows → N output rows, each gaining the rank column.)

## Grounding (verified)

- `OVER (...)` is fully lowerable: `Function.over: Option<WindowType>`, `WindowType::WindowSpec(WindowSpec
  { partition_by: Vec<Expr>, order_by: Vec<OrderByExpr>, window_frame: Option<WindowFrame>, .. })`
  (sqlparser 0.62). Reject `window_frame: Some(_)`, `NamedWindow`, and non-ranking functions.
- `SortExpr { expr, descending, nulls_first }` and `LogicalPlan::Sort` (ir.rs:921/:998) are the template
  for the window ORDER BY; `execute_sort` (exec.rs:832) is reusable inside the operator for per-partition
  ordering.
- **Distributed is mostly free.** The fragmenter's single-exchange-boundary path is *node-agnostic*:
  `replace_exchange_with_scan(plan, ...)` wraps whatever sits above the exchange (fragment.rs:377). So
  `Window(Exchange(input, key = partition_by))` fragments exactly like a grouped `Aggregate(Exchange(...))`
  — producer shuffles on the partition key, the consumer runs the `Window` operator per co-located
  partition. No fragmenter `Window` arm is needed.
- The seed fragmenter rejects **>1 exchange boundary** in a non-join pipeline
  (`MultipleExchangeBoundaries`). Two *distinct* `OVER` specs (different partition keys) would need two
  exchange stages — grounds D6.

## Decisions (promotion-locked)

**D1 — New `Window` logical + physical node + a `WindowFunction` enum.** `LogicalPlan::Window { input,
partition_by: Vec<Expr>, order_by: Vec<SortExpr>, functions: Vec<WindowFunction> }` and the bound
`ExecutionPlan::Window { input, partition_by, order_by, functions, output_schema }`. `WindowFunction`
is `RowNumber | Rank | DenseRank` (ranking functions take no arguments). The existing
`AggregateFunction` stays untouched — window functions are their own surface, not an aggregate flag.

**D2 — Lower SELECT-projection `OVER (PARTITION BY p ORDER BY o)` ranking functions → `Window` over an
`Exchange` on the partition key, evaluated post-`HAVING`/pre-final-projection.** Window functions are
extracted from the SELECT list and evaluated logically *after* `WHERE`/`GROUP BY`/`HAVING` and *before*
the final projection and global `ORDER BY` (the standard SQL evaluation order); the planner inserts
`Exchange(partition_count, key = partition_by)` beneath the `Window` node (co-locating each partition,
exactly like a grouped aggregate). Empty `PARTITION BY` (`OVER (ORDER BY o)`) → a single-partition
gather (`partition_count = 1`). Window functions in `WHERE`/`GROUP BY`/`HAVING` stay rejected.

**D3 — Window output is row-preserving: input columns + one rank column per function.** Output schema =
the input schema followed by one `Int64` column per window function (its declared/alias name). N input
rows → N output rows. This is the anti-`Aggregate` property and is pinned.

**D4 — The `Window` operator orders each co-located partition internally, then streams with a
partition-boundary reset.** Within the operator: order each co-located partition by `ORDER BY` (reusing
the sort kernel), then stream rows assigning ranks and **resetting** the counter whenever the partition
key changes. `ROW_NUMBER` = 1,2,3… unconditionally; `RANK` = ties share a rank, then a gap; `DENSE_RANK`
= ties share a rank, no gap. `ORDER BY` ties/NULLs are broken deterministically (total order, via the
existing sort policy) so the result and its canonical digest are stable across worker counts. The
ordering is done **inside** the operator, *not* via a separate `Sort` node: Toron's `Sort` is recognized
by the fragmenter only as a global-gather barrier (`Sort(Exchange(partition_count=1))`, fragment.rs:1189),
which would re-gather every partition to one worker and defeat the partition-by exchange.

**D5 — Distributed = partition-by hash-`Exchange` + per-partition window eval; reuse the generic
exchange-boundary fragmentation.** The whole partition must be co-located (window funcs need every row
of the partition), like a grouped aggregate. Empty partition → single-partition gather. No new
fragmenter code — `Window(Exchange(...))` rides the node-agnostic boundary path.

**D6 — One distinct `OVER` spec per query in this cut; multiple functions may share it.** `ROW_NUMBER()
OVER (w), RANK() OVER (w)` (same partition/order) → **one** `Window` node computing both. **Multiple
distinct `OVER` specs** (different partition/order) need a stack of `Window` nodes = multiple exchange
stages, which the seed fragmenter's one-boundary limit does not support — **deferred** (a real
mechanism boundary, not a feature nerf).

**D7 — Deferred, stays fenced (named):** explicit frames (`ROWS`/`RANGE BETWEEN ...`); aggregate-window
(`SUM()/AVG()/COUNT() OVER` — needs frame semantics); `GROUP BY` + window in one query; named `WINDOW`
clause; `DISTINCT`-with-window; `LAG/LEAD/NTILE/FIRST_VALUE/...`. Each is a separate follow-on; the
existing rejects for these remain.

**D8 — Frontend-agnostic node; no optimizer / durable / Wendelon surface.** `LogicalPlan::Window` is
frontend-agnostic — Rune can emit it directly (a Rune window builder is a near-term follow-on, not a
SQL-only default per the Rune-primary discipline). Window adds no optimizer-evidence and no cross-ring
change.

## Semantics

```sql
SELECT region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM sales
SELECT id, RANK() OVER (PARTITION BY region ORDER BY amount) AS r,
           DENSE_RANK() OVER (PARTITION BY region ORDER BY amount) AS dr FROM sales  -- shared OVER spec
SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM sales                          -- empty PARTITION BY
-- still rejected (deferred):
SELECT SUM(x) OVER (PARTITION BY p ORDER BY o ROWS BETWEEN ...) FROM t                -- frames / agg-window
SELECT ROW_NUMBER() OVER (PARTITION BY a), RANK() OVER (PARTITION BY b) FROM t        -- multiple distinct specs
```

## Honesty gates (discriminating)

- **`row_number_restarts_and_orders_within_partition`** — ranks restart at 1 per partition and follow
  `ORDER BY` (fails if the operator ranks globally or ignores the partition).
- **`rank_and_dense_rank_differ_on_ties`** — a tie makes `RANK` skip (1,1,3) and `DENSE_RANK` not (1,1,2)
  — discriminates the two (and proves tie handling, not naive row numbering).
- **`window_preserves_input_row_count`** — N input rows → N output rows (the anti-`Aggregate` gate;
  fails if window collapses like an aggregate).
- **`window_order_by_ties_and_nulls_are_deterministic`** — digest stable across worker counts.
- **`empty_partition_by_orders_whole_input`** — `OVER (ORDER BY o)` ranks all rows in one partition.
- **`window_functions_evaluated_post_having`** — a window over a `HAVING`-filtered result computes on the
  post-`HAVING` rows (proves the post-`HAVING`/pre-projection placement; window in `WHERE` stays rejected).
- **`shared_over_spec_uses_one_window_node`** — two functions, one `OVER` → a single `Window` node
  (plan-shape; fails if it fans into two exchange stages).
- **`distributed_window_rank_is_value_discriminating`** — a partition's rows scan-shared across producers
  before the partition exchange; the ranks are correct only once the partition is co-located + ordered
  (236/238 discipline — single-producer-unreachable).
- **`frames_aggregate_window_named_window_and_multiple_specs_stay_rejected`** — the deferred set.

## Architecture guards (promotion-locked)

- `adr241_adds_window_logical_and_physical_node`
- `adr241_window_function_enum_is_ranking_only`
- `adr241_over_spec_lowers_to_window_over_partition_exchange`
- `adr241_window_is_row_preserving_not_an_aggregate`
- `adr241_window_reuses_generic_exchange_boundary_fragmentation`
- `adr241_single_over_spec_per_query_multiple_specs_deferred`
- `adr241_frames_and_aggregate_window_stay_rejected`
- `adr241_distributed_window_gate_is_value_discriminating`
- `adr241_window_node_is_frontend_agnostic_no_optimizer_or_wendelon_surface`

**Reconciled prior guards (named):** the window-function *rejects* (`expr_contains_window_function` in
UPDATE/RETURNING/DISTINCT) stay for those clauses; only the SELECT-projection window path is lifted. A
new `ExecutionPlan`/`LogicalPlan` variant trips the general enum-shape/variant guards (e.g. the
distributed `g17` single-Join-variant family, enum-count pins) — retarget per the new-node checklist.

## Consensus (transparent)

All three converged on a window-function ranking substrate; the differences were scope and the
execution shape, reconciled by grounding (not headcount):

- **Function set.** Gemini proposed `ROW_NUMBER` only (execution-risk management); Codex included
  windowed aggregates (`SUM() OVER`); Claude proposed the ranking trio. Promoted: **`ROW_NUMBER`/`RANK`/
  `DENSE_RANK`** — the complete *ranking* surface, all sharing the same streaming-counter-over-ordered-
  partition substrate (and `RANK` vs `DENSE_RANK` gives a discriminating tie gate). `ROW_NUMBER`-alone is
  too granular; **windowed aggregates need frame semantics** and are the genuine next mechanism (D7).
- **Execution shape.** Gemini proposed the planner insert an explicit `Sort([partition, order])` before a
  streaming `Window` kernel. Grounding overruled the *separate `Sort` node*: Toron's `Sort` is a
  global-gather barrier (fragment.rs:1189), so it would collapse the partition-by exchange. Promoted:
  the `Window` operator orders each **co-located** partition internally, then uses Gemini's
  partition-boundary-reset streaming (D4). Gemini's post-`HAVING`/pre-projection placement is adopted (D2).
- **Distributed + scope bounds (Claude, grounded).** The node-agnostic exchange-boundary fragmentation
  makes distribution mostly free (D5); the seed fragmenter's one-exchange-boundary limit bounds the cut
  to one distinct `OVER` spec per query (D6). Row-preservation (D3) was unanimous.

No residual divergence after grounding.

## Forward arc

```text
241 window ranking substrate (this)
-> window frames (ROWS/RANGE) + aggregate-window (SUM/AVG/... OVER)
-> LAG/LEAD/NTILE/FIRST_VALUE/LAST_VALUE; named WINDOW clause; multiple distinct OVER specs
-> GROUP BY + window composition; Rune window builder
```

## Deferred (named)

Explicit frames; aggregate-window; `LAG/LEAD/NTILE/FIRST_VALUE/LAST_VALUE`; named `WINDOW`; window
expressions inside derived tables; multiple
distinct `OVER` specs in one query; `GROUP BY` + window in one query; `DISTINCT`-with-window; a Rune
window builder API; optimizer evidence for window cardinality.
