# Toron ADR 243: Rows Window Frames

**Status:** Draft (iter-2 of the adr/impl dual). **Migration:** `plans/impl/243_rows_window_frames.md`.
**Category:** Toron core query surface — explicit `ROWS BETWEEN …` physical-offset frames on aggregate
window functions. **Extends the ADR 242 aggregate-window path with a per-call frame payload and a sliding
re-aggregation executor.** No `RANGE`/`GROUPS` units, no named `WINDOW`, no new physical operator, no
optimizer/durable/Wendelon/Rune surface.

## Why this cut exists

ADR 242 landed aggregate windows on the SQL **default** frame (`RANGE UNBOUNDED PRECEDING AND CURRENT
ROW` — peer-aware cumulative; whole-partition with no `ORDER BY`). The remaining load-bearing window
semantic is the **explicit `ROWS` frame**: moving sums, trailing/centered averages, and any
bounded row-count window — `AVG(x) OVER (ORDER BY t ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` (7-row
moving average) — that the default cumulative frame cannot express.

This is the genuine next *mechanism*, the boundary 242 named: `ROWS` counts **physical rows** in the
ordered partition, so the executor materializes a **sliding frame per output row** rather than reuse the
default peer-group accumulator. The frame `WindowFrame` IR payload — deliberately omitted in 242 — lands
here.

**The intuition (for the humans in the loop).** The 242 default frame answers "the aggregate from the
start of the partition through my peer group." A `ROWS` frame answers "the aggregate over the *k* physical
rows around me" — a window that slides as the row pointer advances. `ROWS BETWEEN 1 PRECEDING AND 1
FOLLOWING` is a 3-row sliding window; at the partition edges it clamps (the first row's frame is just
itself + the next). Because it counts physical rows, the chosen discriminator fixture can make tied
`ORDER BY` rows produce different values under `ROWS` (unlike the 242 `RANGE` default, where ties share)
— that difference is the discriminator that proves the new mechanism.

## Grounding (verified)

- `sqlparser 0.62` exposes `WindowFrame { units: WindowFrameUnits, start_bound, end_bound: Option<…> }`
  (`end_bound: None` = the `ROWS n PRECEDING` shorthand ≡ `CURRENT ROW`) and
  `WindowFrameBound::{CurrentRow, Preceding(Option<Expr>), Following(Option<Expr>)}` (`…(None)` =
  `UNBOUNDED`).
- ADR 242 gives a `Window` operator that materializes + sorts each co-located partition (random access),
  a per-call `BoundWindowCall { function, arg }`, and the partition-exchange fragmentation — exactly the
  substrate a sliding frame needs. The buffer is already there; this cut adds frame-bound arithmetic +
  slice re-aggregation, not new buffering.
- `extract_window_call` still rejects every explicit frame (`"explicit window frames are not supported"`).
  This cut lifts that fence for `ROWS` only and keeps `RANGE`/`GROUPS` rejected.

## Decisions (promotion-locked)

**D1 — The frame is a per-call `Option<WindowFrame>`, not a node property.** `WindowFunctionCall`
(logical) and `BoundWindowCall` (physical) carry `frame: Option<WindowFrame>` (`None` = the 242 default
frame). The `Window` node is unchanged. **Per-call, not per-node:** two aggregate windows over the *same*
`OVER` spec can carry *different* frames — a moving `SUM(x) OVER (… ROWS BETWEEN 2 PRECEDING AND CURRENT
ROW)` beside a running `AVG(x) OVER (…)` (default) — and both compute in one `Window` node over one
exchange + one sort. `window_specs_match` still compares only partition + order; the frame is not part of
the spec match.

**D2 — Accept explicit `ROWS BETWEEN <start> AND <end>` on aggregate windows.** Bounds: `UNBOUNDED
PRECEDING`, `n PRECEDING`, `CURRENT ROW`, `n FOLLOWING`, `UNBOUNDED FOLLOWING`, with `n` a non-negative
integer literal. The `ROWS <start>` shorthand (`end_bound = None`) means `… AND CURRENT ROW`. Frames on
**ranking** functions stay rejected (ranking is frame-independent).

**D3 — `RANGE` and `GROUPS` units stay fenced (→ ADR 244 range window frames).** Only `WindowFrameUnits::
Rows` is accepted; `Range`/`Groups` and the `EXCLUDE` clause are rejected. The 242 default-frame semantics
(an absent frame) are untouched.

**D4 — Validate frame-bound ordering.** Reject `start = UNBOUNDED FOLLOWING`, `end = UNBOUNDED PRECEDING`,
and any `start` that comes after `end` (mapping `UNBOUNDED PRECEDING < n PRECEDING < CURRENT ROW < n
FOLLOWING < UNBOUNDED FOLLOWING`), e.g. `ROWS BETWEEN CURRENT ROW AND 1 PRECEDING`.

**D5 — Executor: sliding re-aggregation per row, clamped to the partition.** For each row in its sorted
partition, compute the physical frame slice `[lo, hi]` (offsets clamped to the partition's first/last
row), aggregate the rows in that slice with the existing kernels, and emit one value per row. An empty
frame (`lo > hi`, e.g. `ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING` on the last row) yields the empty
aggregate (`SUM`/`AVG`/`MIN`/`MAX` → NULL, `COUNT` → 0). The default frame (`None`) keeps the 242
peer-group cumulative path unchanged. First cut: a straightforward slice re-aggregation (O(rows × frame));
an invertible sliding accumulator (O(1) for `SUM`/`COUNT`/`AVG`) is a named follow-on, not this cut.

**D6 — Row-preserving (unchanged).** The frame changes values, not row count: N input rows → N output
rows; output schema = input columns + one column per call (aggregate result types as in 242).

**D7 — Distributed reuses the 242/241 partition-exchange fragmentation.** No new fragmenter arm; the frame
payload rides the same `Window(Exchange(partition_by))` boundary. A value-discriminating cross-producer
gate (236/238) proves a moving frame is correct only once the partition exchange co-locates and orders the
rows.

**D8 — No new physical / optimizer / durable / Wendelon / Rune surface.** Reuse the `Window` node + the
aggregate kernels; the frame adds no optimizer-evidence and no cross-ring change.

## Semantics

```sql
SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY day
                             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trailing3 FROM t
SELECT grp, AVG(score) OVER (PARTITION BY grp ORDER BY day
                             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS centered3 FROM t
SELECT grp, SUM(score) OVER (ORDER BY day ROWS UNBOUNDED PRECEDING) AS running FROM t  -- ≡ … AND CURRENT ROW
SELECT grp, COUNT(*) OVER (PARTITION BY grp ORDER BY day
                          ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS lookahead FROM t  -- forward window
-- one OVER spec, two different frames, one Window node (D1):
SELECT SUM(x) OVER (PARTITION BY g ORDER BY t ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
       AVG(x) OVER (PARTITION BY g ORDER BY t)                                          FROM t
-- still rejected (deferred):
SELECT SUM(x) OVER (ORDER BY t RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t        -- RANGE → 244
SELECT SUM(x) OVER (ORDER BY t GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t       -- GROUPS → 244
SELECT RANK() OVER (ORDER BY t ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t         -- frame on ranking
```

## Honesty gates (discriminating)

- **`rows_frame_running_does_not_share_ties`** — on the chosen discriminator fixture,
  `SUM(x) OVER (ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` gives a *row-by-row*
  cumulative where tied `ORDER BY` rows produce **different** values, unlike the 242 `RANGE` default
  (where ties share). Fails if the default peer path is reused.
- **`rows_frame_moving_window_edges_respect_offsets`** — `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`
  computes only the 3-row window, clamped at partition boundaries (first/last row see 2 rows).
- **`rows_frame_current_row_and_following`** — `CURRENT ROW`/`n FOLLOWING` look-ahead bounds are applied.
- **`rows_frame_empty_window_is_null`** — a frame entirely outside the partition (`2 FOLLOWING AND 3
  FOLLOWING` on the last row) → NULL (`SUM`/`AVG`) / 0 (`COUNT`).
- **`rows_frame_multiple_functions_can_differ_in_frame`** — two aggregate windows over one `OVER` spec
  with **different** frames compute in one `Window` node (the per-call-frame discriminator; FAILS under a
  node-level frame).
- **`default_frame_aggregate_window_unchanged`** — an absent frame still behaves as the 242 `RANGE`
  default (tied rows share) — a regression guard for the 242 path.
- **`explicit_range_groups_and_ranking_frames_stay_rejected`** — `RANGE`/`GROUPS` units, `EXCLUDE`, and
  frames on ranking functions, plus an invalid bound ordering, stay rejected.
- **`rows_frame_distributed_gate_is_value_discriminating`** — a moving frame across a scan-shared
  partition is correct only after the exchange co-locates + orders it (236/238).

## Architecture guards (promotion-locked)

- `adr243_window_call_carries_optional_rows_frame`
- `adr243_rows_frame_lowers_with_bounds_reusing_window_node`
- `adr243_rows_frame_executor_is_sliding_not_peer_default`
- `adr243_rows_frame_window_is_row_preserving`
- `adr243_range_groups_and_ranking_frames_stay_rejected`
- `adr243_rows_frame_reuses_partition_exchange_fragmentation`
- `adr243_rows_frame_distributed_gate_is_value_discriminating`
- `adr243_no_new_physical_optimizer_durable_wendelon_or_rune_surface`

**Reconciled prior guards (named):** the 242 `adr242_default_frame_is_range_unbounded_preceding` and
`adr242_explicit_frames_and_distinct_window_aggregate_stay_rejected` guards are retargeted — explicit
`ROWS` frames are now accepted; only `RANGE`/`GROUPS`/named/DISTINCT/ranking-frame forms stay rejected. A
`BoundWindowCall`/`WindowFunctionCall` field add may shift postcard goldens (re-pin in this cut).

## Consensus (transparent)

The council converged on the mechanism: 242 handled the default frame; explicit `ROWS` is the next
mechanism (sliding physical-row offsets over the materialized partition); `RANGE`/`GROUPS` stay deferred
to 244; an `Option<WindowFrame>` payload (`None` = the 242 default); naive slice re-aggregation first cut.

The one design point — **frame placement** — resolved by grounding to **per-call**. SQL frames are
per-function: two aggregate windows over the same partition/order can carry different frames (a moving
`SUM` beside a running `AVG`); a node-level frame would force those into a rejected "multiple distinct
`OVER` specs," a regression from 242's surface. Codex's iter-1/2 placed the frame on the **Window node**;
**Gemini's iter-2 concurred with per-call** (`Option<WindowFrame>` on `WindowFunctionCall`/
`BoundWindowCall`). Per-call wins by grounding and the 2-of-3 majority, generalizing 242's per-call
`BoundWindowCall`; `window_specs_match` keeps comparing only partition + order. It is locked by
`rows_frame_multiple_functions_can_differ_in_frame`, which fails under a node-level frame. (This also
answers Codex's open impl question — the frame does need to ride `WindowFunctionCall`.)

## Forward arc

```text
242 aggregate window functions, default frame
-> 243 rows window frames (this): explicit ROWS BETWEEN, per-call frame, sliding re-aggregation
-> 244 range window frames (RANGE/GROUPS value- and peer-count frames; WindowFrameUnit)
-> invertible sliding accumulators (O(1) SUM/COUNT/AVG); LAG/LEAD/NTILE/FIRST_VALUE/LAST_VALUE;
   named WINDOW; multiple distinct OVER specs; GROUP BY + window
```

## Deferred (named)

`RANGE` and `GROUPS` frames + the `WindowFrameUnit` (→ **244 range window frames**); `EXCLUDE` clauses;
frames on ranking functions; invertible O(1) sliding accumulators; named `WINDOW` clauses; `DISTINCT`
window aggregates; `LAG/LEAD/NTILE/FIRST_VALUE/LAST_VALUE`; multiple distinct `OVER` specs; `GROUP BY` +
window; optimizer evidence for window cardinality; a Rune window builder.
