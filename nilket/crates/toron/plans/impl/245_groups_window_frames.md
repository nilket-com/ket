# Impl Plan — Toron ADR 245: Groups Window Frames

Pairs with `plans/adr/245_groups_window_frames.md`. Extend ADR 244's per-call frame with a
`WindowFrameUnit::Groups` + `WindowFrameDelta::Groups(u64)` and a peer-group-count executor path.
**Completes the explicit frame family. `EXCLUDE`, named windows, ranking-frames, and group offsets without
`ORDER BY` stay rejected. No new operator, no optimizer/durable/Wendelon/Rune surface.**

## Touch surface

- `ir.rs` — add `Groups` to `WindowFrameUnit`; add `WindowFrameDelta::Groups(u64)` (a peer-group count).
  `WindowFrameBound`/`WindowFrame`/`WindowFunctionCall.frame` unchanged in shape.
- `ir/execution.rs` — `BoundWindowCall.frame` already carries the unit-bearing frame (serialized); no
  change.
- `sql_planner.rs` — `lower_window_frame` accepts `WindowFrameUnits::Groups`; `lower_frame_delta` maps a
  `GROUPS` offset to `WindowFrameDelta::Groups(window_frame_offset(expr)?)` (the non-negative integer parse,
  like `ROWS`). Extend `window_frame_bound_rank`/`frame_delta_magnitude` for the `Groups` delta. In
  `extract_window_call`, require an `ORDER BY` when a `GROUPS` frame has a count offset (`spec.order_by`
  non-empty); keep the `EXCLUDE`/ranking-frame rejects. **No key-type check** (unlike `RANGE`) — a group
  count is type-free and works on multi-key/`Utf8`.
- `binder.rs` — thread the frame through `BoundWindowCall` unchanged; `validate_range_frame_offsets` only
  applies to `Range` (skips `Groups`). No peer-group binding needed.
- `exec.rs` — `execute_window_aggregate` dispatches `Groups` → new `execute_window_aggregate_groups`. Build
  (once per call, from the existing `is_new_peer_group`): `group_id[row]` (partition-aligned peer-group
  ordinal), `group_start_row[]`/`group_end_row[]` per group id, and each partition's `[first_group,
  last_group]` id range. Per row in group `g`: resolve the start bound to a group id (`UNBOUNDED PRECEDING`
  → partition first group; `CURRENT ROW` → `g`; `Preceding(n)`/`Following(n)` → `g ∓ n`) and the end bound
  similarly, clamp to `[first_group, last_group]`; if the span is entirely outside → empty. Translate
  `[lo_group, hi_group]` → `[group_start_row[lo_group], group_end_row[hi_group]]` and fold
  `window_agg_add`/`window_agg_finalize` over the slice. No `RowConverter`/arithmetic (direction-agnostic).
- `distributed/fragment.rs` / walkers — no change.
- tests: `tests/window_test.rs` + `tests/distributed_process_test.rs` + `tests/architecture/window_functions.rs`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Frame AST | `WindowFrameUnits::{Rows,Range,Groups}` (sqlparser 0.62) |
| Frame IR (extend) | `WindowFrameUnit` / `WindowFrameDelta` (ir.rs, ADR 244) |
| Lowering (extend) | `fn lower_window_frame` / `fn lower_frame_delta` / `fn window_frame_bound_rank` (sql_planner.rs) |
| Peer-group boundaries (reuse) | `is_new_peer_group` / `peer_start` / `peer_end` in `fn execute_window` (exec.rs) |
| Dispatch + slice fold (reuse) | `fn execute_window_aggregate` / `window_agg_add` / `window_agg_finalize` (exec.rs) |
| Group offset (non-negative int) | `fn window_frame_offset` (sql_planner.rs) |

## Phase 0 — Red discriminators

`groups_offset_differs_from_rows_and_range`; `groups_frame_includes_whole_peer_group`;
`groups_frame_tied_rows_share_value`; `groups_frame_binds_on_multikey_or_utf8_key`;
`groups_frame_empty_window_is_null`; `groups_frame_multiple_functions_can_differ_in_frame`;
`groups_frame_distributed_gate_is_value_discriminating`. Red on `main` (GROUPS rejected).

## Phase 1 — IR payload

Add `WindowFrameUnit::Groups` + `WindowFrameDelta::Groups(u64)`; update the exhaustive delta/unit matches.
`--all-targets` + serde round-trips.

## Phase 2 — Lowering

Accept `Groups`; map the offset to `Groups(u64)`; extend the ordering-rank magnitude; require `ORDER BY`
for a group-count offset; reject `EXCLUDE` and ranking frames. No key-type check.

## Phase 3 — Binder + verify-first probe

Thread the frame. **VERIFY-FIRST:** probe `SUM(x) OVER (ORDER BY o GROUPS BETWEEN 1 PRECEDING AND CURRENT
ROW)` binds AND `fragment_plan` succeeds before the group-span executor.

## Phase 4 — Executor

`execute_window_aggregate_groups`: peer-group indexing + group→row mapping; resolve bounds in group space,
clamp to the partition's group range, empty span → empty aggregate; reuse the 243/244 fold helpers.

## Phase 5 — Tests, guards, verify, seal

Behavior gates (three-way GROUPS≠ROWS≠RANGE, whole-peer-group inclusion, tied-rows share, multi-key/Utf8
binds, empty→NULL, per-call ROWS+GROUPS in one node, no-ORDER-BY + EXCLUDE + ranking rejects); the
distributed value-discriminating gate; the eight `adr245_*` guards; reconcile the 244 unit/delta pins;
re-pin postcard goldens the unit/delta add shifts. Full verification gate. Commit `impl: adr 245 groups
window frames`.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test window
scripts/cargo-cgroup test  -p toron --test distributed_process_test groups
scripts/cargo-cgroup test  -p toron --test architecture_test adr245
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL (reconciled 244 pins)
scripts/cargo-cgroup test  -p toron --test distributed_process_test   # FULL
scripts/cargo-cgroup test  -p toron --lib                             # serde/postcard round-trips
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. GROUPS reuses the ROWS row-count or RANGE value-distance rule — `groups_offset_differs_from_rows_and_range` fails.
2. A peer group enters the frame partially — `groups_frame_includes_whole_peer_group` fails.
3. Tied rows get different values — `groups_frame_tied_rows_share_value` fails.
4. GROUPS rejects a multi-key/Utf8 ORDER BY — `groups_frame_binds_on_multikey_or_utf8_key` fails.
5. Empty group span not NULL/0 — `groups_frame_empty_window_is_null` fails.
6. Frame forced onto the node — `groups_frame_multiple_functions_can_differ_in_frame` fails.
7. A deferred form (EXCLUDE, no-ORDER-BY group offset, ranking frame) silently plans — a reject gate fails.
8. Distributed group window reachable from one producer — the distributed gate fails (236/238).

## Hygiene

- No m-word ("strictly increasing"); no version-suffix idiom; reuse the `Window` node + aggregate kernels.
- `--all-targets` (the new unit/delta variants); cargo via `scripts/cargo-cgroup`; FULL `architecture_test`
  before sealing; re-pin postcard goldens if the `WindowFrameUnit`/`WindowFrameDelta` shape shifts a preimage.

## Open for the implementing session (verify-first)

1. Probe `SUM(x) OVER (ORDER BY v GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)` binds + `fragment_plan` ok.
2. Confirm the three-way split on the tie fixture: GROUPS (whole prior group) ≠ ROWS (one row) ≠ RANGE (value).
3. Confirm a whole peer group enters the frame; tied rows share one value; spans clamp at partition edges.
4. Confirm GROUPS binds + executes over a multi-key and a `Utf8` ORDER BY (where RANGE value offsets reject).
5. Confirm an out-of-range group span → empty aggregate, and `GROUPS UNBOUNDED PRECEDING` is peer-cumulative.
6. Confirm serde/postcard round-trips for the extended `WindowFrameUnit`/`WindowFrameDelta` (re-pin goldens).
