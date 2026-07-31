# Toron ADR 244: Range Window Frames

**Status:** Draft (iter-2 of the adr/impl dual). **Migration:** `plans/impl/244_range_window_frames.md`.
**Category:** Toron core query surface — explicit `RANGE BETWEEN …` value-distance frames on aggregate
window functions. **Extends ADR 243's per-call frame with a first-class `WindowFrameUnit` and a typed
value-offset bound, computing `key ± offset` with Toron's existing typed arithmetic — so RANGE covers
every ordered scalar type Toron has (numeric *and* temporal).** No `GROUPS`, no multi-key RANGE, no
`EXCLUDE`, no named `WINDOW`, no new physical operator, no optimizer/durable/Wendelon/Rune surface.

## Why this cut exists

ADR 243 landed physical-row `ROWS` frames. The remaining explicit frame mechanism is `RANGE`: a frame
whose bounds move in the **ORDER BY value space**, not the row-index space. `ROWS BETWEEN 2 PRECEDING AND
CURRENT ROW` is "the two prior rows and me"; `RANGE BETWEEN 2 PRECEDING AND CURRENT ROW` is "every row
whose ORDER BY value is within 2 of mine." On data with gaps these differ, and that difference — value
distance vs row count — is the new executor rule that justifies the `WindowFrameUnit` split 243 deferred.

**The intuition (for the humans in the loop).** Sort a partition by `score` with values `[10, 12, 20]`.
For the row at `12`: `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` sums the previous row + me (`10 + 12`);
`RANGE BETWEEN 5 PRECEDING AND CURRENT ROW` sums every row whose score is in `[7, 12]` (`10 + 12`, because
`20` is out of range) — but at the row `20`, `RANGE 5 PRECEDING` sees only `[15, 20] = {20}` while `ROWS 1
PRECEDING` still sees `{12, 20}`. RANGE follows the values; ROWS follows positions.

## Grounding (verified)

- `sqlparser 0.62` exposes `WindowFrameUnits::{Rows, Range, Groups}` and the same `WindowFrameBound`
  shape ADR 243 lowers. The AST already carries the unit; this cut lifts the `Range` fence.
- ADR 243 carries `frame: Option<WindowFrame>` on `WindowFunctionCall`/`BoundWindowCall` (per-call) and
  materializes + sorts each partition with random access — so RANGE reuses the per-call payload and the
  sorted buffer; the value interval is a scan/search over the already-sorted ORDER BY key.
- **The 243 bound is `Preceding(u64)` — a row *count*.** A RANGE offset is a *value* in the key domain,
  so it cannot reuse the row-count `u64`. This cut generalizes the bound offset to a typed value (see D2).
- **Toron's ordered scalar types are exactly `{Int32, Int64, Float32, Float64, Date32, Timestamp}`** —
  there is **no `Decimal`, `Fraction`, or `Rational`** in `ScalarValue`, so the RANGE offset domain is
  small and closed, not open-ended.
- **`key ± offset` is already computed by Toron's existing typed arithmetic** — `evaluate_arithmetic` +
  `evaluate_temporal_arithmetic` (`Timestamp/Date32 ± Interval`) + `evaluate_calendar_arithmetic`
  (`± CalendarMonthInterval`). So the RANGE bound reuses that machinery for *every* ordered key type;
  there is no reason to hand-roll numeric subtraction or carve temporal out.

## Decisions (promotion-locked)

**D1 — `WindowFrameUnit { Rows, Range }` is first-class on the frame.** `WindowFrame` carries `unit`
alongside `start`/`end`. `ROWS` keeps the 243 physical-offset rule; `RANGE` uses value-distance matching.
(`Groups` is *not* added as a variant in this cut — it is a distinct peer-count mechanism, deferred.)

**D2 — The frame bound offset generalizes from a row count to a *typed* value delta.** A `ROWS` bound
carries a row count (integer); a `RANGE` bound carries a **typed `ScalarValue` offset** matching the
ORDER BY key — a number for a numeric key, an `Interval`/`CalendarMonthInterval` for a temporal key. The
recommended representation is `WindowFrameDelta { Rows(u64), Range(ScalarValue) }`; lowering validates
that `ROWS` carries a row count and `RANGE` carries a value whose type is `key − offset`–compatible. The
delta is *not* a hand-picked numeric subset — `ScalarValue` already spans Toron's whole ordered domain.

**D3 — RANGE value offsets require a single ORDER BY key whose `± offset` arithmetic Toron defines.**
That is any of the six ordered scalar types (`Int32/64`, `Float32/64`, `Date32`, `Timestamp`); the offset
literal's type must match (numeric offset for a numeric key; `INTERVAL …` for a temporal key) and is
checked at lowering against the existing arithmetic output rules. Multi-key RANGE is rejected (value
distance over a tuple is undefined). RANGE bounds with **no value offset** (`UNBOUNDED PRECEDING`,
`CURRENT ROW`, `UNBOUNDED FOLLOWING`) are peer/value based and work for **any** single ORDER BY key
(`RANGE … CURRENT ROW` = the current row's value-peers; the 242 default made explicit).

**D4 — Executor: select the contiguous value interval, computing the bound with the existing arithmetic.**
For each row, the low/high bounds are `key − start` / `key + end`, evaluated by the **same**
`evaluate_arithmetic`/`evaluate_temporal_arithmetic` kernels Toron uses for `col ± interval` (so numeric
and temporal keys are one code path, not two). The frame is the contiguous run of rows whose key value
lies in `[low, high]` (direction-aware for `DESC`; `UNBOUNDED`/`CURRENT ROW` resolve to the partition
edge / value-peer boundary), found by scan/binary-search over the sorted key. Aggregate the slice with the
existing kernels; an empty interval yields the empty aggregate (`SUM`/`AVG`/`MIN`/`MAX` → NULL, `COUNT` →
0). NULL ORDER BY values form their own peer group (no value arithmetic). Default frame (`None`) stays the
242 peer path; `ROWS` stays the 243 path.

**D5 — Deferred, fenced (named): `GROUPS`, multi-key RANGE.** `GROUPS` (peer-count offsets) is a distinct
mechanism and stays deferred; multi-key RANGE stays deferred. There is **no** `Decimal`/`Fraction`/
`Rational` deferral because Toron has no such types — the RANGE domain is exactly the six ordered scalar
types, all covered here. (`CalendarMonthInterval` month/year offsets ride the existing calendar
arithmetic; their non-uniform width is a documented edge, not a separate mechanism.)

**D6 — Per-call frame, row-preserving, distributed reuse (all unchanged).** The frame rides the call
(243), N input rows → N output rows, and the window rides the existing partition-exchange fragmentation.
No new optimizer/durable/Wendelon/Rune surface.

## Semantics

```sql
SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score
                             RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS r FROM t   -- value window
SELECT grp, COUNT(*) OVER (ORDER BY score RANGE UNBOUNDED PRECEDING) AS running FROM t -- ≡ default peers
SELECT grp, AVG(amount) OVER (ORDER BY amount RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING) FROM t
SELECT id, SUM(qty) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) FROM t -- temporal
-- one OVER spec, a ROWS frame and a RANGE frame in one Window node (per-call, from 243):
SELECT SUM(x) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
       SUM(x) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)  FROM t
-- still rejected (deferred):
SELECT SUM(x) OVER (ORDER BY t GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t                -- GROUPS
SELECT SUM(x) OVER (ORDER BY a, b RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t              -- multi-key
SELECT SUM(x) OVER (ORDER BY label RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t             -- Utf8 key, no ± arith
SELECT RANK() OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t                 -- ranking
```

## Honesty gates (discriminating)

- **`range_offset_differs_from_rows_offset`** — on a value-gap fixture, `RANGE BETWEEN n PRECEDING AND
  CURRENT ROW` and `ROWS BETWEEN n PRECEDING AND CURRENT ROW` give **different** values (RANGE follows
  value distance, ROWS follows row count) — the discriminator that proves the value-based rule.
- **`range_offset_extends_across_peer_groups`** — a value bound pulls in rows from neighbouring peer
  groups when their values fall inside the interval (and excludes peers that fall outside).
- **`range_frame_shares_value_peers`** — value-equal rows (same ORDER BY value) always share one frame
  value (RANGE is value-defined).
- **`range_frame_empty_window_is_null`** — an interval entirely outside the partition → empty aggregate.
- **`range_frame_multiple_functions_can_differ_in_frame`** — a ROWS frame and a RANGE frame over one
  `OVER` spec compute in one `Window` node (per-call, from 243).
- **`temporal_range_interval_offset_windows_by_value`** — `RANGE BETWEEN INTERVAL '<n>' DAY PRECEDING
  AND CURRENT ROW` over a `Timestamp`/`Date32` key selects rows within the time distance, reusing the
  existing temporal arithmetic (proves numeric and temporal are one path).
- **`range_non_arithmetic_key_groups_and_multikey_stay_rejected`** — a value offset on a key whose
  `± offset` arithmetic Toron doesn't define (e.g. `Utf8`), an offset whose type mismatches the key,
  `GROUPS`, multi-key RANGE, and frames on ranking functions stay rejected; `UNBOUNDED`/`CURRENT ROW`
  RANGE still bind on any single key.
- **`range_frame_distributed_gate_is_value_discriminating`** — a value-window across a scan-shared
  partition is correct only once the exchange co-locates + orders the rows (236/238).

## Architecture guards (promotion-locked)

- `adr244_frame_unit_is_first_class_with_typed_value_delta`
- `adr244_range_frame_lowers_with_typed_offsets_matched_to_key`
- `adr244_range_frame_bound_reuses_existing_typed_arithmetic`
- `adr244_range_frame_window_is_row_preserving`
- `adr244_non_arithmetic_value_offset_keys_groups_multikey_and_ranking_frames_stay_rejected`
- `adr244_range_frame_reuses_partition_exchange_fragmentation`
- `adr244_range_frame_distributed_gate_is_value_discriminating`
- `adr244_no_new_physical_optimizer_durable_wendelon_or_rune_surface`

**Reconciled prior guards (named):** the 243 guards that pin `WindowFrameBound` as a bare `u64`
(`adr243_window_call_carries_optional_rows_frame`, `adr243_rows_frame_lowers_with_bounds_reusing_window_node`)
are retargeted for the generalized value delta. A `WindowFrame` shape change re-pins postcard goldens.

## Consensus (transparent)

The council agrees on the boundary: 243 owns physical `ROWS`; 244 owns value-based `RANGE`; the per-call
frame and the unit discriminator (not overloaded `ROWS` semantics) carry it.

The load-bearing reframe, grounded past Codex's iter-1 (and the operator's "Decimal? Temporal? Fraction?
Rational?" steer): **the RANGE offset domain is not an open-ended numeric question — it is exactly
Toron's six ordered scalar types**, and `key ± offset` is *already* computed by the existing typed
arithmetic. So:

- Codex's "keep `Preceding(u64)`, integer literals only" silently restricts RANGE to **integer keys** —
  the one domain where RANGE is least used. Rejected: the bound offset generalizes to a typed
  `ScalarValue`.
- Splitting "numeric in / temporal out" is an **arbitrary carve-out**: `evaluate_temporal_arithmetic`
  already does `Timestamp − Interval`, so temporal RANGE is the *same* code path, not a new mechanism.
  Both land here.
- There is **no `Decimal`/`Fraction`/`Rational`** in `ScalarValue`, so there is nothing to scope or
  defer there — the domain is closed at the six ordered types.

`GROUPS` (peer-count) and multi-key RANGE stay deferred — genuine distinct mechanisms.

**Gemini's iter-1 independently converged on this reframe** (the `WindowFrameDelta`, the
`evaluate_arithmetic`/`evaluate_temporal_arithmetic` reuse, temporal in-scope, the closed six-type
domain). Codex's iter-1/2 kept the narrower "value-based `RANGE`" framing without grounding the offset
type, so it neither commits the delta representation nor the temporal reuse. The reframed design carries
by grounding and the 2-of-3 majority.

## Forward arc

```text
242 aggregate window functions, default frame
-> 243 rows window frames: physical row offsets
-> 244 range window frames (this): value-distance frames over all ordered key types (numeric + temporal)
-> GROUPS (peer-count frames); invertible O(1) sliding accumulators; LAG/LEAD/NTILE; named WINDOW;
   multiple distinct OVER specs; GROUP BY + window
```

## Deferred (named)

`GROUPS` frames; multi-key RANGE; `EXCLUDE` clauses; frames on ranking functions; invertible O(1) sliding
accumulators; named `WINDOW` clauses; `DISTINCT` window aggregates; `LAG/LEAD/NTILE/FIRST_VALUE/
LAST_VALUE`; multiple distinct `OVER` specs; `GROUP BY` + window; optimizer evidence for window
cardinality; a Rune window builder. (No `Decimal`/`Fraction`/`Rational` RANGE — Toron has no such types.)
