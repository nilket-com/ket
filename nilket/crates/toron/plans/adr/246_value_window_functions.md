# Toron ADR 246: Value Window Functions

**Status:** Draft (iter-1 of the adr/impl dual). **Migration:** `plans/impl/246_value_window_functions.md`.
**Category:** Toron core query surface — value/navigation window functions (`LAG`/`LEAD`/`FIRST_VALUE`/
`LAST_VALUE`/`NTH_VALUE`). **Extends the 241–245 `Window` operator with "return an expr's value from
another row of the ordered partition" — positional for `LAG`/`LEAD`, frame-boundary for the value
functions.** No new physical operator, no `NTILE`, no `IGNORE NULLS`, no optimizer/durable/Wendelon/Rune
surface.

## Why this cut exists

The window arc has the analytic primitives (ranking 241, aggregate 242, the explicit frame family
243–245), but the **value-function family is still fenced** — the marquee gap for time-series and
"compare to another row" analytics. They split into two sub-shapes over one user-facing mechanism ("the
value of `expr` at some other row in the ordered partition"):

- **Navigation** — `LAG(expr, n)` / `LEAD(expr, n)`: the value `n` rows *back/ahead by position*
  (frame-independent). `price - LAG(price) OVER (ORDER BY day)` = day-over-day delta.
- **Frame value** — `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE(expr, n)`: the value at the *first/last/nth row
  of the active frame* — cashing in the 243–245 frame slice.

This is the coherent cut between the frame family and `NTILE` (bucketing — a different rule, deferred).

**The intuition (for the humans in the loop).** A window aggregate *folds* the frame to one number; a
value function *picks one row* instead. `FIRST_VALUE(x)` is "x at the start of my frame"; `LAG(x, 1)` is
"x one row before me." The subtle, load-bearing bit: `LAG`/`LEAD` ignore the frame (they count *position*
from the current row), while `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` are *defined by* the frame — so
`LAST_VALUE(x) OVER (ORDER BY t)` over the SQL default `RANGE` frame returns the last value in the
current peer group, not the partition's last (you need `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING` for that; on a unique ORDER BY key, the default frame's end happens to be the current row).
The frame machinery from 243–245 already computes the `[lo, hi]` slice; the value functions just *take
one row* of it instead of aggregating.

## Grounding (verified)

- The 241 `Window` operator already materializes + orders each co-located partition (`sorted_order`) and
  is row-preserving (N→N). `LAG`/`LEAD` are a positional `take` over that order; `FIRST_VALUE`/`LAST_VALUE`/
  `NTH_VALUE` are a single `take` from the frame slice the 243–245 executors already compute.
- `WindowFunction` today is `RowNumber | Rank | DenseRank | Aggregate(AggregateFunction)`. Value functions
  are a **new function arm**, not a new node; the call already carries `arg: Option<BoundExprProgram>`
  (the value expr, reused from 242) and `frame: Option<WindowFrame>` (243–245).
- `sqlparser 0.62` parses `LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE(args) OVER (…)` as `Function` with
  `over` — the shape `extract_window_call` already dispatches.
- Distribution is free: value functions ride the same partition-by `Exchange` + node-agnostic
  fragmentation. No new operator/fragmenter arm.

## Decisions (promotion-locked)

**D1 — One shared `WindowFunction::Value(ValueFunction)` arm** (resolving Codex's iter-1 open question).
`ValueFunction` is `Lag | Lead | FirstValue | LastValue | NthValue`, carrying its constant parameters
(`Lag`/`Lead`: `offset: u64` (default 1) + `default: ScalarValue` (default NULL); `NthValue`: `n: u64`;
`FirstValue`/`LastValue`: none). The value `expr` rides on the call's existing `arg`. One arm keeps the
executor dispatch and planner validation localized, and the whole family shares the output-type rule.

**D2 — `LAG`/`LEAD` are positional and frame-independent.** For row `i` in the sorted partition, return
`expr` at `i − offset` (`LAG`) / `i + offset` (`LEAD`), clamped to the partition; outside the partition →
the `default` (or NULL). An explicit frame on `LAG`/`LEAD` is **ignored** (SQL semantics) — they count
position from the current row, not frame rows.

**D3 — `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` are frame-aware.** They reuse the 243–245 frame slice
`[lo, hi]` (default frame = `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`): `FIRST_VALUE` = `expr` at row
`lo`, `LAST_VALUE` = at row `hi`, `NTH_VALUE(expr, n)` = at row `lo + n − 1` (NULL if past `hi`). An empty
frame → NULL.

**D4 — Output type = the `expr`'s type, nullable.** Value functions return one cell of the value column;
edges/defaults/empty frames produce NULL, so the column is nullable. `LAG`/`LEAD` require `ORDER BY`
(positional navigation needs an order), like ranking.

**D5 — Per-call, row-preserving, distributed reuse (all unchanged).** Value functions coexist with
ranking/aggregate calls in one `Window` node over a shared `OVER` spec; N rows → N rows; same exchange
fragmentation. No new optimizer/durable/Wendelon/Rune surface.

**D6 — Deferred, fenced (named).** `NTILE` (bucketing — a partition-wide rank rule, no value expr);
`IGNORE NULLS`/`RESPECT NULLS`; expression (non-constant) `LAG`/`LEAD` defaults; `NTH_VALUE … FROM LAST`;
`FROM FIRST`/`FROM LAST` and `RESPECT`/`IGNORE NULLS` modifiers.

## Semantics

```sql
SELECT grp, LAG(score) OVER (PARTITION BY grp ORDER BY id) AS prev FROM t            -- prior row, NULL at edge
SELECT grp, score - LAG(score, 1, 0) OVER (ORDER BY id) AS delta FROM t              -- delta, default 0
SELECT grp, LEAD(score, 2) OVER (ORDER BY id) AS two_ahead FROM t
SELECT grp, FIRST_VALUE(score) OVER (PARTITION BY grp ORDER BY id) AS first_so_far FROM t
SELECT grp, LAST_VALUE(score) OVER (PARTITION BY grp ORDER BY id
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS grp_last FROM t
SELECT grp, NTH_VALUE(score, 2) OVER (PARTITION BY grp ORDER BY id) AS second FROM t
-- one OVER spec, a navigation and an aggregate in one Window node (per-call, from 242/244):
SELECT LAG(x) OVER (ORDER BY t), SUM(x) OVER (ORDER BY t) FROM t
-- still rejected (deferred):
SELECT NTILE(4) OVER (ORDER BY score) FROM t                                          -- bucketing → follow-on
SELECT LAG(x) OVER (PARTITION BY g) FROM t                                            -- LAG needs ORDER BY
SELECT LAG(x) IGNORE NULLS OVER (ORDER BY t) FROM t                                   -- null treatment
```

## Honesty gates (discriminating)

- **`lag_returns_prior_row_value`** / **`lead_returns_next_row_value`** — positional access over the order.
- **`lag_offset_and_default_at_partition_edge`** — `LAG(x, 2, -1)`: rows past the partition start → the
  default `-1` (not NULL, not a leak from the prior partition).
- **`lag_lead_restart_per_partition`** — no value leaks across a `PARTITION BY` boundary.
- **`lag_lead_ignore_frame`** — the discriminator: `LAG(x) OVER (… ROWS 1 PRECEDING)` equals
  `LAG(x) OVER (…)` (the frame is ignored), proving `LAG` is positional, not frame-defined.
- **`first_last_value_respect_frame`** — the dual discriminator: `LAST_VALUE(x)` over the default frame =
  the last value in the current `RANGE` peer group, but over `ROWS … UNBOUNDED FOLLOWING` = the
  partition's last — proving the value functions *are* frame-defined.
- **`nth_value_uses_frame_ordinal_and_null_past_end`** — `NTH_VALUE(x, 2)` = the 2nd frame row; NULL when
  the frame has fewer than `n` rows.
- **`value_functions_preserve_row_count`** and **`value_functions_share_one_window_node`** — N→N; a
  navigation + an aggregate over one spec in one node.
- **`value_functions_distributed_gate_is_value_discriminating`** — a prior-row/frame value across a
  scan-shared partition is correct only once the exchange co-locates + orders the rows (236/238).

## Architecture guards (promotion-locked)

- `adr246_window_function_has_value_function_arm`
- `adr246_lag_lead_are_positional_and_ignore_the_frame`
- `adr246_first_last_nth_value_reuse_the_frame_slice`
- `adr246_value_function_output_is_expr_typed_and_row_preserving`
- `adr246_ntile_null_treatment_and_ranking_value_forms_stay_rejected`
- `adr246_value_functions_reuse_partition_exchange_fragmentation`
- `adr246_value_functions_distributed_gate_is_value_discriminating`
- `adr246_no_new_physical_optimizer_durable_wendelon_or_rune_surface`

**Reconciled prior guards (named):** the 242 `adr242_window_call_carries_ranking_or_aggregate_function`
pin and the `WindowFunction` variant-count guards are retargeted for the added `Value` arm; a
`WindowFunction` shape change re-pins postcard goldens.

## Consensus (transparent)

The council converged at iter-0/1 on the **value-function family** — `LAG`/`LEAD` (positional) +
`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` (frame-aware) — as one coherent mechanism ("return an `expr` value
from another row of the ordered partition"), with `NTILE` deferred as the odd-one-out (bucketing, no value
expr). I resolve Codex's one open question toward a **single `Value(ValueFunction)` arm** (D1): it
localizes the executor dispatch and planner validation and shares the output-type rule across the family,
and the frame-aware members reuse the 243–245 frame slice rather than a parallel path. Gemini's iter-0
folded `NTILE` in; grounding splits it out (bucketing is a rank rule, not a value lookup) — a clean
follow-on.

## Forward arc

```text
243/244/245 explicit frame family (ROWS/RANGE/GROUPS)
-> 246 value window functions (this): LAG/LEAD (positional) + FIRST_VALUE/LAST_VALUE/NTH_VALUE (frame)
-> NTILE (bucketing); IGNORE/RESPECT NULLS; named WINDOW; multiple distinct OVER specs;
   invertible O(1) sliding accumulators; GROUP BY + window; frame EXCLUDE
```

## Deferred (named)

`NTILE`; `IGNORE NULLS`/`RESPECT NULLS`; non-constant `LAG`/`LEAD` defaults; `NTH_VALUE … FROM LAST`;
named `WINDOW` clauses; multiple distinct `OVER` specs; `GROUP BY` + window; frame `EXCLUDE`; invertible
O(1) sliding accumulators; optimizer evidence for window cardinality; a Rune window builder.
