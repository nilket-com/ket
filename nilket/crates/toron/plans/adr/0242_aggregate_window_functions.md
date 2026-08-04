# Toron ADR 242: Aggregate Window Functions

**Status:** Draft (iter-1 of the adr/impl dual). **Migration:** `plans/impl/0242_aggregate_window_functions.md`.
**Category:** Toron core query surface — aggregate window functions on the SQL default frame. **Extends
the ADR 241 `Window` node + operator to carry aggregate calls (`SUM/AVG/COUNT/MIN/MAX(x) OVER (...)`),
reusing the existing `AggregateFunction` kernels.** No explicit-frame syntax, no new physical operator,
no optimizer/durable/Wendelon/Rune surface.

## Why this cut exists

ADR 241 landed the `Window` node, the partition-exchange + internal per-partition ordered streaming, and
ranking functions. Aggregate-window is the arc's named next item and the highest-value window extension
(`score / SUM(score) OVER (PARTITION BY grp)` = percent-of-total; `SUM(x) OVER (ORDER BY t)` = running
total). It is fenced today, no partial impl:

- `"only ROW_NUMBER, RANK, and DENSE_RANK window functions are supported"` (sql_planner.rs:3899);
- `"explicit window frames are not supported"` (:3909);
- no `WindowFrame`/running-aggregate handling in `src/` (grep empty).

This cut composes two landed pieces — the **241 `Window` operator** (partition exchange + internal
ordered streaming with peer detection) and the **existing `AggregateFunction` kernels** — so it is an
operator extension, not a new operator.

**The intuition (for the humans in the loop).** An aggregate window function is "the aggregate over a
*frame* of rows, emitted on every row" (row-preserving), versus `GROUP BY` which collapses. The SQL
**default frame** (when you write `OVER (... ORDER BY o)` with no explicit `ROWS`/`RANGE`) is `RANGE
BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — a *cumulative* aggregate where "current row" in `RANGE`
means *the whole peer group*, so tied rows share one cumulative value. With no `ORDER BY`, there is one
peer group = the whole partition, and every row gets the partition total. So one mechanism — peer-aware
cumulative over the sorted partition — covers both whole-partition and running aggregates.

## Grounding (verified)

- The 241 operator already detects partition boundaries and **peer groups** (`execute_window`'s
  RANK/DENSE_RANK loop), which is exactly the unit the RANGE default frame aggregates over.
- The `Window` node carries `functions: Vec<WindowFunctionCall>` (ranking, no args); extending the call
  to carry an aggregate function + one argument is the change.
- `AggregateFunction` (Count/Sum/Min/Max/Avg) kernels exist; window aggregates reuse them.
- sqlparser supplies `WindowFrame`/`WindowFrameUnits`/`WindowFrameBound` for the deferred explicit-frame
  follow-on; this cut keeps rejecting `spec.window_frame.is_some()`.
- Distribution is free: aggregate-window rides the same partition-by `Exchange` + the node-agnostic
  fragmentation the 241 `Window` already uses.

## Decisions (promotion-locked)

**D1 — Extend the `Window` node to carry aggregate window calls; reuse the kernels.** A unified window
call carries a function (ranking or `Aggregate(AggregateFunction)`) plus arguments (ranking: none;
aggregate: one). One operator pass computes ranking and aggregate calls over the same sorted partition.
The user-facing `AggregateFunction` enum is unchanged; window-ness is a property of the `Window` node,
not the function.

**D2 — Lower `agg(x) OVER (PARTITION BY ... [ORDER BY ...])` to the `Window` node.** `extract_window_call`
accepts `COUNT(*)` (no argument) and `COUNT/SUM/MIN/MAX/AVG` with one argument, in addition to the
ranking functions. Multiple window calls (ranking and/or aggregate) sharing one `OVER` spec lower to a
**single** `Window` node, computed in one operator pass. WHERE/GROUP BY/HAVING/derived-table window
rejects and the explicit-frame reject remain.

**D3 — Default frame only: `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`; it unifies whole-partition and
running.** No `ORDER BY` → one peer group = the whole partition → every row gets the partition aggregate
(percent-of-total). `ORDER BY` → peer-aware cumulative: all rows in an `ORDER BY` peer group share the
value accumulated through the *end* of that peer group (`RANGE` semantics, not row-by-row `ROWS`).
The 241 operator **already computes these peer-group boundaries** (`exec.rs` peer loop: "a new peer
group starts at a partition boundary or when the `ORDER BY` value changes"), so the RANGE default frame
needs no new frame IR payload. **Explicit `ROWS`/`RANGE BETWEEN` frame syntax stays rejected** — and so
does the `WindowFrame` IR payload that would carry it. Its load-bearing home is **ADR 243 (rows window
frames)**, where per-row sliding re-aggregation over physical row offsets is a genuinely new executor
mechanism (bounded `RANGE`/`GROUPS` value/peer offsets follow in **244 range window frames**); this cut
defers there, at that mechanism boundary, not to stay small.

**D4 — Row-preserving (unchanged from 241).** Output schema = input columns + one column per window call;
each aggregate-window column's type is the aggregate's result type over its argument (e.g. `AVG → Float64`,
`SUM(Int64) → Int64`, `MIN/MAX → arg type`). Output-column nullability follows the aggregate:
`COUNT(*)`/`COUNT(x)` → non-null `Int64`; `SUM`/`AVG`/`MIN`/`MAX` → **nullable** (an all-NULL or empty
frame yields NULL); ranking columns stay non-null `Int64`. N input rows → N output rows.

**D5 — Executor: peer-aware cumulative over the sorted partition.** Reuse the 241 partition sort + peer
detection. Maintain a running accumulator per aggregate call; at each peer-group boundary finalize the
accumulator and assign that value to every row in the peer group (so ties share it). Whole-partition is
the one-peer-group case. NULL/empty semantics follow the existing aggregates (e.g. `SUM`/`AVG` over no
non-null values → NULL; `COUNT` → 0).

**D6 — Distributed reuses the 241 partition-exchange fragmentation; value-discriminating gate.** No new
fragmenter code. The distributed gate (236/238) places a partition's rows scan-shared across producers so
the running/whole-partition aggregate is correct only once the partition exchange co-locates and orders
them.

**D7 — Deferred, stays fenced (named).** Explicit `ROWS`/`RANGE BETWEEN` frames; `DISTINCT` inside a
window aggregate (`SUM(DISTINCT x) OVER`); `APPROX_QUANTILE`/`LinearRegress` over windows;
`LAG/LEAD/NTILE/FIRST_VALUE/LAST_VALUE`; multiple distinct `OVER` specs; named `WINDOW`; `GROUP BY` +
window in one query.

**D8 — No new physical / optimizer / durable / Wendelon / Rune surface.** Reuse the `Window` node + the
aggregate kernels; window aggregates add no optimizer-evidence and no cross-ring change.

## Semantics

```sql
SELECT grp, SUM(score) OVER (PARTITION BY grp) AS total FROM t          -- whole-partition (per-row total)
SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY t) AS running FROM t  -- running (RANGE default)
SELECT AVG(score) OVER (ORDER BY t) AS running_avg FROM t               -- empty PARTITION BY, running
-- still rejected (deferred):
SELECT SUM(x) OVER (ORDER BY t ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t  -- explicit frame
SELECT SUM(DISTINCT x) OVER (PARTITION BY g) FROM t                     -- DISTINCT window aggregate
```

## Honesty gates (discriminating)

- **`whole_partition_aggregate_window`** — `SUM(x) OVER (PARTITION BY g)` gives every row its partition
  total (percent-of-total).
- **`count_star_window_counts_frame_rows`** — `COUNT(*) OVER (...)` counts rows in the frame (running
  count through the peer group under `ORDER BY`; partition row-count otherwise).
- **`aggregate_window_multiple_functions_share_one_window_node`** — multiple window calls (ranking and/or
  aggregate) over one `OVER` spec lower to a single `Window` node and compute in one operator pass
  (anti-fan-out).
- **`running_sum_window_is_cumulative`** — `SUM(x) OVER (PARTITION BY g ORDER BY o)` is cumulative and
  restarts per partition.
- **`running_window_range_ties_share_value`** — tied `ORDER BY` rows share the value cumulative through
  the *whole* peer group (`RANGE` default), NOT a row-by-row `ROWS` running fold — the discriminator that
  proves the default frame is `RANGE`.
- **`window_aggregate_preserves_row_count`** — N input rows → N output rows (anti-`Aggregate`).
- **`running_avg_window_null_and_empty_semantics`** — `AVG`/`SUM` over no non-null values → NULL.
- **`explicit_frames_and_distinct_window_aggregate_stay_rejected`** — the deferred set.
- **`distributed_window_aggregate_is_value_discriminating`** — a partition scan-shared across producers;
  the aggregate is correct only after the partition exchange co-locates + orders it (236/238).

## Architecture guards (promotion-locked)

- `adr242_window_call_carries_ranking_or_aggregate_function`
- `adr242_aggregate_window_lowers_to_window_node_reusing_kernels`
- `adr242_default_frame_is_range_unbounded_preceding`
- `adr242_window_aggregate_is_row_preserving`
- `adr242_explicit_frames_and_distinct_window_aggregate_stay_rejected`
- `adr242_window_aggregate_reuses_partition_exchange_fragmentation`
- `adr242_distributed_window_aggregate_gate_is_value_discriminating`
- `adr242_no_new_physical_optimizer_durable_wendelon_or_rune_surface`

**Reconciled prior guards (named):** the ADR 241 ranking-only guards
(`adr241_window_function_enum_is_ranking_only`, the `extract_window_call` reject) are retargeted —
aggregate functions are now accepted in `OVER`; only the named-deferred forms stay rejected. A
`WindowFunction` variant change may bump the enum/variant-count guards.

## Consensus (transparent)

All three agreed on the substrate (extend the 241 `Window` node + reuse the `AggregateFunction` kernels,
not a parallel operator) and on deferring multiple distinct `OVER` specs (a second exchange boundary).

The load-bearing fork was the **frame surface**. Codex and Gemini both leaned *frames-first*: carry a
`WindowFrame` IR payload on the `Window` node and accept explicit `ROWS` frames now, deferring `RANGE`/
`GROUPS` units. Claude leaned *default-frame-only*: no explicit frame syntax, no frame payload.

Grounding resolves it toward default-frame-only. The SQL default frame **is** `RANGE` (with `ORDER BY`,
`RANGE UNBOUNDED PRECEDING AND CURRENT ROW`; without, the whole partition) — so a cut that accepts
explicit `ROWS` while deferring `RANGE` leaves the *most common* running form, `SUM(x) OVER (ORDER BY t)`
with no explicit frame, in a semantic gap: its default frame is exactly the deferred unit. The 241
operator already computes the peer-group boundaries the RANGE default aggregates over (grounded in
`exec.rs`), so default-frame delivers the marquee percent-of-total and running-total cases correctly and
for free. The `WindowFrame` payload earns its keep only once **ADR 243 (rows window frames)** lands
(physical row offsets, sliding re-aggregation) — the genuine next mechanism — so this cut defers explicit
frame syntax there, at the mechanism boundary. The good ideas from the frames-first drafts are folded: `COUNT(*)` support, the
single-`Window`-node-per-`OVER`-spec gate, and explicit output-column nullability.

Operator steer is open: fold explicit *cumulative* `ROWS`/`RANGE UNBOUNDED PRECEDING AND CURRENT ROW`
syntax + the frame payload into this cut, or hold at the default frame. Lean: hold — the payload is dead
weight until bounded frames make the bounds load-bearing.

## Forward arc

```text
241 window functions substrate (ranking)
-> 242 aggregate window functions, default frame (this)
-> 243 rows window frames (explicit ROWS BETWEEN: physical row offsets, sliding re-aggregation +
   WindowFrame IR payload; moving averages, trailing/centered windows)
-> 244 range window frames (bounded RANGE value offsets; GROUPS)
-> LAG/LEAD/NTILE/FIRST_VALUE/LAST_VALUE; named WINDOW; multiple distinct OVER specs; GROUP BY + window
```

## Deferred (named)

Explicit `ROWS` frames (→ **243 rows window frames**) and bounded `RANGE`/`GROUPS` frames (→ **244 range
window frames**); the `WindowFrame` IR payload (lands in 243); `DISTINCT` window aggregates;
`APPROX_QUANTILE`/`LinearRegress` windows; `LAG/LEAD/NTILE/FIRST_VALUE/LAST_VALUE`; multiple distinct
`OVER` specs; named `WINDOW`; `GROUP BY` + window; a Rune window builder.
