# Impl Plan — Toron ADR 244: Range Window Frames

Pairs with `plans/adr/0244_range_window_frames.md`. Extend ADR 243's per-call frame with a first-class
`WindowFrameUnit` and a **typed** value-offset bound, plus a value-interval `RANGE` executor that computes
`key ± offset` with Toron's **existing typed arithmetic** — so RANGE covers all six ordered scalar types
(numeric + temporal) on one code path. **`GROUPS`, multi-key RANGE, `EXCLUDE`, named windows, and
ranking-frames stay rejected. No new operator, no optimizer/durable/Wendelon/Rune surface.**

## Touch surface

- `ir.rs` — add `pub enum WindowFrameUnit { Rows, Range }`; add `unit: WindowFrameUnit` to `WindowFrame`.
  Generalize the bound offset: `WindowFrameBound::{Preceding, Following}` carry a `WindowFrameDelta` where
  `WindowFrameDelta { Rows(u64), Range(ScalarValue) }` (ROWS = row count; RANGE = value offset). Keep the
  `Unbounded*`/`CurrentRow` variants. `WindowFunctionCall.frame` unchanged in shape.
- `ir/execution.rs` — `BoundWindowCall.frame` carries the unit-bearing frame (serialized). No new fields.
- `sql_planner.rs` — `extract_window_call`: accept `WindowFrameUnits::Range` alongside `Rows`; build
  `WindowFrame { unit, .. }`. ROWS bound → `WindowFrameDelta::Rows(u64)` (the 243 path, rewrapped). RANGE
  value bound → `WindowFrameDelta::Range(ScalarValue)` by `lower_expr`-ing the offset literal (a number or
  an `INTERVAL …`) to a `ScalarValue`, then validating its type is `key − offset`–compatible against the
  single ORDER BY key via the existing arithmetic-output rules (reuse `arithmetic_output_type`). Reject
  multi-key RANGE (`order_by.len() != 1` with a value offset), a value offset on a key with no `± offset`
  arithmetic (e.g. `Utf8`), `Groups`, `EXCLUDE`, and ranking-function frames. Keep the 243 start≤end
  ordering validation. RANGE `UNBOUNDED`/`CURRENT ROW` bounds need no value offset and still bind on any
  single key.
- `binder.rs` — thread the unit-bearing `call.frame` into `BoundWindowCall` (unchanged plumbing). The
  RANGE key ordinal: the executor reads the single ORDER BY key already in `order_by` (ExecutionSortKey).
- `exec.rs` — `execute_window_aggregate` dispatches on `frame.unit`: `Rows` → `execute_window_aggregate_rows`
  (243, reading the `Rows(u64)` delta); `Range` → new `execute_window_aggregate_range`. The RANGE path:
  build `low_bounds = key_array − start_offset` and `high_bounds = key_array + end_offset` **once per
  partition** via the existing `evaluate_arithmetic`/`evaluate_temporal_arithmetic` kernels (broadcast the
  offset `ScalarValue`; numeric and `Timestamp/Date32 ± Interval` are the same call), then for each row
  find the contiguous `[lo, hi]` row interval whose sorted key value lies in `[low_bounds[i],
  high_bounds[i]]` (scan or binary search, direction-aware for DESC), fold the kernels over the slice
  (reuse `window_agg_add`/`window_agg_finalize`), empty → empty aggregate. NULL key values: their own peer
  group (no arithmetic). `UNBOUNDED`/`CURRENT ROW` bounds resolve to the partition edge / value-peer
  boundary (reuse the peer arrays). RANGE forms without a value offset still bind on any single key.
- `distributed/fragment.rs` / walkers — no change.
- tests: `tests/window_test.rs` + `tests/distributed_process_test.rs` + `tests/architecture/window_functions.rs`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Frame AST | `WindowFrame { units, start_bound, end_bound }`, `WindowFrameUnits::{Rows,Range}` (sqlparser 0.62) |
| Frame IR (extend) | `WindowFrame` / `WindowFrameBound` (ir.rs, ADR 243) |
| Window lowering (extend) | `fn extract_window_call` / `fn lower_rows_window_frame` (sql_planner.rs) |
| ROWS sliding path (reuse/dispatch) | `fn execute_window_aggregate_rows` + the `fn execute_window_aggregate` dispatcher (exec.rs) |
| Sorted ORDER BY key values | `sorted_order` + `order_arrays` in `fn execute_window` (exec.rs) |
| Aggregate fold helpers (reuse) | `window_agg_add` / `window_agg_finalize` / `numeric_value` (exec.rs) |
| `key ± offset` bound arithmetic (reuse) | `evaluate_arithmetic` / `evaluate_temporal_arithmetic` / `evaluate_calendar_arithmetic` (exec.rs) |
| Offset type ↔ key type validation | `arithmetic_output_type` (ir.rs); `lower_expr` for the offset literal (sql_planner.rs) |

## Phase 0 — Red discriminators

`range_offset_differs_from_rows_offset`; `range_offset_extends_across_peer_groups`;
`range_frame_shares_value_peers`; `temporal_range_interval_offset_windows_by_value`;
`range_frame_empty_window_is_null`; `range_frame_multiple_functions_can_differ_in_frame`;
`range_frame_distributed_gate_is_value_discriminating`. Red on `main` (RANGE rejected).

## Phase 1 — IR payload + delta generalization

Add `WindowFrameUnit`; thread `unit` into `WindowFrame`; generalize the bound offset to `WindowFrameDelta`.
Rewrap the 243 ROWS lowering/executor to read `Rows(u64)`. `--all-targets` + serde round-trips.

## Phase 2 — Lowering

Accept `Range`; `lower_expr` the offset literal (number or `INTERVAL …`) to a `ScalarValue` and validate
its type against the single ORDER BY key via `arithmetic_output_type`; reject keys with no `± offset`
arithmetic for value-bearing RANGE (e.g. `Utf8`), multi-key RANGE, `Groups`, `EXCLUDE`, ranking-frames;
keep ordering validation. `UNBOUNDED`/`CURRENT ROW` RANGE still bind on any single key.

## Phase 3 — Binder + verify-first probe

Thread the unit-bearing frame. **VERIFY-FIRST:** probe `SUM(x) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING
AND CURRENT ROW)` (numeric `v`) binds AND `fragment_plan` succeeds before the range evaluator.

## Phase 4 — Executor

`execute_window_aggregate_range`: per-row value interval over the sorted key (direction-aware; NULL peers;
UNBOUNDED/CURRENT ROW via peer edges), fold kernels over the slice, empty → empty aggregate. Reuse the
243 add/finalize helpers. Default frame keeps 242; ROWS keeps 243.

## Phase 5 — Tests, guards, verify, seal

Behavior gates (ROWS-vs-RANGE same-offset differ on a value-gap fixture, cross-peer extension, value-peer
sharing, empty→NULL, per-call ROWS+RANGE in one node, deferred rejects); distributed value-discriminating
gate; eight `adr244_*` guards; reconcile the 243 `u64`-bound guards; re-pin postcard goldens the frame
shape shifts. Full verification gate. Commit `feat: adr 244 range window frames`.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test window
scripts/cargo-cgroup test  -p toron --test distributed_process_test range
scripts/cargo-cgroup test  -p toron --test architecture_test adr244
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL (reconciled 243 pins)
scripts/cargo-cgroup test  -p toron --test distributed_process_test   # FULL
scripts/cargo-cgroup test  -p toron --lib                             # serde/postcard round-trips
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. RANGE reuses the ROWS row-count rule — `range_offset_differs_from_rows_offset` fails.
2. Value bound stays inside the peer group — `range_offset_extends_across_peer_groups` fails.
3. Value-equal rows get different frame values — `range_frame_shares_value_peers` fails.
4. Empty interval not NULL/0 — `range_frame_empty_window_is_null` fails.
5. Frame forced onto the node — `range_frame_multiple_functions_can_differ_in_frame` fails.
6. A deferred form (GROUPS, multi-key, or a key with no `± offset` arithmetic for a value-bearing RANGE)
   silently plans — a reject gate fails.
7. Distributed value-window reachable from one producer — the distributed gate fails (236/238).

## Hygiene

- No m-word ("strictly increasing"); no version-suffix idiom; reuse the `Window` node + aggregate kernels.
- `--all-targets` (the generalized delta touches the 243 ROWS construction sites); cargo via
  `scripts/cargo-cgroup`; FULL `architecture_test` before sealing; re-pin postcard goldens if the
  `WindowFrame`/`WindowFrameBound` shape shifts a preimage.

## Open for the implementing session (verify-first)

1. Probe `SUM(x) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)` binds + `fragment_plan` ok.
2. Confirm on a value-gap fixture that RANGE n PRECEDING ≠ ROWS n PRECEDING (value distance vs row count).
3. Confirm value-equal rows share one frame value, and the interval extends across peer groups by value.
4. Confirm DESC ordering reverses the interval direction correctly; NULL key values form their own group.
5. Confirm the empty interval → NULL/0, and an absent/`ROWS` frame are byte-for-byte unchanged (242/243).
6. Confirm the `WindowFrameDelta` representation serializes cleanly (re-pin postcard goldens if needed).
7. Decide the value-interval search: linear scan first (simple, O(rows × window)); binary search only if a
   gate shows the need (the partition is already sorted by the key).
