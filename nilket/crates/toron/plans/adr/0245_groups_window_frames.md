# Toron ADR 245: Groups Window Frames

**Status:** Draft (iter-2 of the adr/impl dual). **Migration:** `plans/impl/0245_groups_window_frames.md`.
**Category:** Toron core query surface — explicit `GROUPS BETWEEN …` peer-count frames on aggregate window
functions. **Extends ADR 244's per-call frame with a `Groups` unit + an integer group-count offset, sliced
over the peer-group boundaries already computed.** **Completes the explicit frame family
(`ROWS`/`RANGE`/`GROUPS`).** No new physical operator, no `EXCLUDE`, no named `WINDOW`, no
optimizer/durable/Wendelon/Rune surface.

## Why this cut exists

ADR 243 landed `ROWS` (physical row offsets), 244 landed `RANGE` (value-distance offsets). `GROUPS` is the
last explicit frame unit: offsets counted in **peer groups**. `ROWS` asks "how many physical rows around
me?", `RANGE` "which rows are within a value distance of me?", `GROUPS` "how many peer *groups* around
mine?" On data with ties the three genuinely diverge, and `GROUPS` is the one 244 deliberately deferred as
a distinct mechanism. After this cut the explicit frame surface is complete.

**The intuition (for the humans in the loop).** Sort a partition into peer groups `[g1, g2, g3]` where
`g2` holds several tied rows. `GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW` over any row in `g2` covers all
of `g1` *and* all of `g2` — **whole groups**, no matter how many rows each holds or how far apart their
values are. That is exactly what `ROWS` (counts rows) and `RANGE` (counts value distance) cannot express.

## Grounding (verified)

- `sqlparser 0.62` exposes `WindowFrameUnits::{Rows, Range, Groups}` and the same `WindowFrameBound` shape
  243/244 lower. The AST already names `Groups`; this cut lifts the fence.
- ADR 244 carries `frame: Option<WindowFrame>` per call and a first-class `WindowFrameUnit`, so `GROUPS`
  adds a variant, not a node-shape change.
- **The executor already computes peer-group boundaries** (`is_new_peer_group`, `peer_start[]`/
  `peer_end[]` in `execute_window`). `GROUPS` is a peer-group **count** offset over that structure — no
  value arithmetic, no `RowConverter`, no type constraints. It is the cheapest of the three frame units.
- A peer group never crosses a partition (a partition boundary is also a peer-group boundary), so a global
  peer-group index is partition-aligned and clamping a group offset to the partition is a group-id clamp.

## Decisions (promotion-locked)

**D1 — `WindowFrameUnit::Groups` + `WindowFrameDelta::Groups(u64)`.** The `GROUPS` offset is a **peer-group
count** (a non-negative integer), reusing the integer-literal parsing `ROWS` uses — *not* a value
`ScalarValue` (that is `RANGE`). A new `Groups(u64)` delta keeps the delta kind matching the frame unit;
lowering validates `GROUPS` frames carry `Groups` deltas.

**D2 — `GROUPS` bounds count peer groups.** `CURRENT ROW` = the current peer group (all its rows);
`n PRECEDING`/`n FOLLOWING` = `n` peer groups earlier/later in sort order (inclusive at the group
boundary); `UNBOUNDED` = the partition edge. The frame is the contiguous rows spanning peer groups
`[g − start, g + end]`, clamped to the partition's first/last group.

**D3 — `GROUPS` binds on *any* ORDER BY key, including multi-key.** Peer groups are defined by peer
equality on the order keys, which is well-defined for every type and any number of keys — so `GROUPS`
works where a value-offset `RANGE` cannot (multi-key, `Utf8`). A group-count offset (`n PRECEDING`/
`FOLLOWING`) requires **an** `ORDER BY` (peer groups need an order; with none, the partition is one group
and the offset is degenerate) — rejected without `ORDER BY`. `UNBOUNDED`/`CURRENT ROW` `GROUPS` need no
offset and bind on any spec, like the peer-based `RANGE` forms.

**D4 — Executor: peer-group-id indexing + group-span slicing.** Reuse the peer arrays: assign each row a
partition-aligned peer-group id, record each group's `[start_row, end_row]`, then resolve each bound to a
group id (clamped to the partition's group range) and translate to row bounds. Fold the existing aggregate
kernels over the span; an empty span (a group offset entirely outside the partition) → the empty aggregate
(`SUM`/`AVG`/`MIN`/`MAX` → NULL, `COUNT` → 0). Direction-agnostic: the offset is positional in group space
(`PRECEDING` = earlier groups = lower row indices), so there is no ASC/DESC value flip. Default frame
(`None`) stays 242; `ROWS` stays 243; `RANGE` stays 244.

**D5 — Per-call, row-preserving, distributed reuse (all unchanged).** The frame rides the call (244), N
rows → N rows, and the window rides the existing partition-exchange fragmentation. No new
optimizer/durable/Wendelon/Rune surface.

## Semantics

```sql
SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score
                             GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS g FROM t   -- prev group + mine
SELECT id, COUNT(*) OVER (ORDER BY day GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t
SELECT id, AVG(v) OVER (ORDER BY a, b GROUPS UNBOUNDED PRECEDING) FROM t               -- multi-key, peers on (a,b)
-- one OVER spec, a ROWS frame and a GROUPS frame in one Window node (per-call, from 244):
SELECT SUM(x) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
       SUM(x) OVER (ORDER BY v GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)  FROM t
-- still rejected (deferred):
SELECT SUM(x) OVER (ORDER BY t GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t -- EXCLUDE
SELECT SUM(x) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t                                -- no ORDER BY
SELECT RANK() OVER (ORDER BY t GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t                     -- ranking
```

## Honesty gates (discriminating)

- **`groups_offset_differs_from_rows_and_range`** — the three-way discriminator. On the tie fixture the
  same `1 PRECEDING AND CURRENT ROW` bound gives a **different** value under `GROUPS` (whole prior group),
  `ROWS` (one prior row), and `RANGE` (value-distance window) — proving the peer-count rule is its own
  mechanism, not a reskin of either.
- **`groups_frame_includes_whole_peer_group`** — a peer group of N tied rows enters the frame wholesale
  (all N), regardless of row count.
- **`groups_frame_tied_rows_share_value`** — every row in a peer group gets the same frame value.
- **`groups_frame_binds_on_multikey_or_utf8_key`** — `GROUPS` over a multi-key or `Utf8` `ORDER BY` binds
  and executes (where a value-offset `RANGE` is rejected).
- **`groups_frame_empty_window_is_null`** — a group offset entirely outside the partition → empty aggregate.
- **`groups_frame_multiple_functions_can_differ_in_frame`** — a `ROWS` frame and a `GROUPS` frame over one
  `OVER` spec compute in one `Window` node (per-call, from 244).
- **`groups_count_offset_without_order_by_rejected`** — a group-count offset needs an `ORDER BY`.
- **`groups_frame_distributed_gate_is_value_discriminating`** — a peer-count window across a scan-shared
  partition is correct only once the exchange co-locates + orders the rows (236/238).

## Architecture guards (promotion-locked)

- `adr245_frame_unit_enum_completes_with_groups`
- `adr245_groups_frame_lowers_with_peer_group_count_offset`
- `adr245_groups_frame_executor_is_peer_group_count_not_row_or_value`
- `adr245_groups_frame_window_is_row_preserving`
- `adr245_exclude_no_order_by_and_ranking_frames_stay_rejected`
- `adr245_groups_frame_reuses_partition_exchange_fragmentation`
- `adr245_groups_frame_distributed_gate_is_value_discriminating`
- `adr245_no_new_physical_optimizer_durable_wendelon_or_rune_surface`

**Reconciled prior guards (named):** the 243/244 `WindowFrameUnit`/`WindowFrameDelta` pins
(`adr244_frame_unit_is_first_class_with_typed_value_delta`) are retargeted for the added `Groups`
variant(s); a `WindowFrameDelta`/`WindowFrameUnit` add re-pins postcard goldens.

## Consensus (transparent)

The council split is clean across the three units: `ROWS` = physical rows (243), `RANGE` = value distance
(244), `GROUPS` = peer-group count (this). The substrate converged early: reuse the per-call frame payload,
add the `Groups` unit + `Groups(u64)` delta, and slice via peer-group indexing over the already-computed
boundaries.

Three groundings sharpened this cut, and **both peers adopted all three at iter-2** (Codex iter-2 carries
them in D1–D5; Gemini iter-1 in its rationale): (1) **`GROUPS` binds on any key, incl. multi-key/`Utf8`** —
strictly more permissive than value-offset `RANGE`, because it needs only peer-group definition, not value
arithmetic; (2) a group-count offset **requires `ORDER BY`** (peer groups need an order; otherwise one
group); (3) the executor is **direction-agnostic** (group offsets are positional, no ASC/DESC value flip),
so it is the simplest of the three frame paths. No residual divergence.

## Forward arc

```text
242 aggregate window functions, default frame
-> 243 rows window frames (physical row offsets)
-> 244 range window frames (value-distance offsets)
-> 245 groups window frames (this): peer-group-count offsets — EXPLICIT FRAME FAMILY COMPLETE
-> invertible O(1) sliding accumulators; LAG/LEAD/NTILE/FIRST_VALUE/LAST_VALUE; named WINDOW;
   multiple distinct OVER specs; GROUP BY + window; frame EXCLUDE
```

## Deferred (named)

`EXCLUDE` clauses; frames on ranking functions; invertible O(1) sliding accumulators; named `WINDOW`
clauses; `DISTINCT` window aggregates; `LAG/LEAD/NTILE/FIRST_VALUE/LAST_VALUE`; multiple distinct `OVER`
specs; `GROUP BY` + window; optimizer evidence for window cardinality; a Rune window builder.
