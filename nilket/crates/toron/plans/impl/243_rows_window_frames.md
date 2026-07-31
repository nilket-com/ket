# Impl Plan — Toron ADR 243: Rows Window Frames

Pairs with `plans/adr/243_rows_window_frames.md`. Extend the ADR 242 aggregate-window path with a
**per-call** `Option<WindowFrame>` and a sliding re-aggregation executor for explicit `ROWS BETWEEN …`
frames. **`ROWS` only — `RANGE`/`GROUPS` units, `EXCLUDE`, named windows, and ranking-frames stay
rejected. No new operator, no optimizer/durable/Wendelon/Rune surface.**

## Touch surface

- `ir.rs` — add `pub enum WindowFrameBound { UnboundedPreceding, Preceding(u64), CurrentRow,
  Following(u64), UnboundedFollowing }` and `pub struct WindowFrame { start, end }` (ROWS unit implicit
  this cut; the unit enum lands in 244). Add `frame: Option<WindowFrame>` to `WindowFunctionCall`
  (`None` = the 242 default frame).
- `ir/execution.rs` — add `frame: Option<WindowFrame>` to `BoundWindowCall`. `WindowFrame`/
  `WindowFrameBound` derive `Serialize`/`Deserialize` (BoundWindowCall is wire-serialized).
- `sql_planner.rs` — in `extract_window_call`, lower `spec.window_frame` when present: accept
  `WindowFrameUnits::Rows` only (reject `Range`/`Groups`/`EXCLUDE`); map each `WindowFrameBound`
  (`Preceding(None)`→`UnboundedPreceding`, `Preceding(Some(lit))`→`Preceding(n)`, `CurrentRow`,
  `Following(…)` similarly; `end_bound = None` → `CurrentRow`); require `n` a non-negative integer literal;
  validate bound ordering (D4). Reject a frame on a ranking function. Carry `frame` on `ExtractedWindow`
  and the pushed `WindowFunctionCall`. `RANGE`/`GROUPS` keep the existing reject message family.
- `binder.rs` — in the `Window` arm, thread `call.frame` into each `BoundWindowCall` (the frame is
  constant offsets — no expression binding). Output schema / row facts unchanged from 242.
- `exec.rs` — `execute_window_aggregate` dispatches on `frame`: `None` → the 242 peer-group cumulative
  path (unchanged); `Some(WindowFrame)` → sliding slice. Compute partition ranges from `is_new_partition`;
  for each row compute `[lo, hi]` by physical offset clamped to `[p_start, p_end]`; fold a fresh `AggState`
  (or i64 count) over `[lo, hi]` and finalize; empty (`lo > hi`) → empty aggregate (NULL / 0). Reuse
  `sum_state_for_data_type`/`add_sum_value`/`numeric_value`/`update_min_state`/… ; assemble the column.
- `distributed/fragment.rs` / walkers — no change: rides the 242 `Window(Exchange(partition_by))` boundary.
- tests: `tests/window_test.rs` + `tests/distributed_process_test.rs` + `tests/architecture/window_functions.rs`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Frame AST | `WindowFrame { units, start_bound, end_bound: Option }`, `WindowFrameBound`, `WindowFrameUnits::Rows` (sqlparser 0.62) |
| Window call / bound call | `WindowFunctionCall` (ir.rs); `BoundWindowCall` (ir/execution.rs) |
| Window lowering (extend) | `fn extract_window_call` / `fn lower_select_window` (sql_planner.rs) |
| Window bind (extend) | the `LogicalPlan::Window` arm in `bind_plan_with_trace` (binder.rs) |
| Default-frame peer path (reuse) | `fn execute_window_aggregate` peer-group branch (exec.rs) |
| Partition boundaries | `is_new_partition` in `fn execute_window` (exec.rs) |
| Integer-literal extraction | the existing aggregate/limit literal lowering in sql_planner.rs |

## Phase 0 — Red discriminators

`rows_frame_running_does_not_share_ties`; `rows_frame_moving_window_edges_respect_offsets`;
`rows_frame_empty_window_is_null`; `rows_frame_multiple_functions_can_differ_in_frame`;
`default_frame_aggregate_window_unchanged`; `rows_frame_distributed_gate_is_value_discriminating`. Red on
`main` (explicit frames rejected).

## Phase 1 — IR payload

Add `WindowFrameBound` + `WindowFrame`; add `frame: Option<WindowFrame>` to `WindowFunctionCall` and
`BoundWindowCall`. `--all-targets` for the exhaustive `Window`/call constructions.

## Phase 2 — Lowering

Lower `spec.window_frame` (Rows only) with bound mapping + ordering validation; reject `RANGE`/`GROUPS`/
`EXCLUDE`/ranking-frame. Keep DISTINCT / multiple-spec / placement rejects.

## Phase 3 — Binder + verify-first probe

Thread `frame` into `BoundWindowCall`. **VERIFY-FIRST:** probe that `SUM(x) OVER (PARTITION BY p ORDER BY
o ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)` binds AND `fragment_plan` succeeds (extends the 242 probe)
before building the sliding executor.

## Phase 4 — Executor

Per-row sliding slice clamped to the partition; reuse the scalar-aggregate state helpers; default frame
keeps the 242 peer path. Empty frame → empty aggregate. Naive slice re-aggregation (optimize later).

## Phase 5 — Tests, guards, verify, seal

Behavior gates (ROWS-vs-RANGE ties discriminator, moving-window edges, current/following, empty→NULL,
per-call-differing-frames, 242-default-unchanged regression, deferred rejects); the distributed
value-discriminating gate; the eight `adr243_*` guards; reconcile the 242 default-frame + explicit-frame
guards; re-pin any postcard goldens the `BoundWindowCall` field shifts. Full verification gate. Commit
`impl: adr 243 rows window frames`.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test window
scripts/cargo-cgroup test  -p toron --test architecture_test adr243
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL (reconciled 242 + any pins)
scripts/cargo-cgroup test  -p toron --test distributed_process_test   # FULL
scripts/cargo-cgroup test  -p toron --lib                             # serde/postcard round-trips
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. On the discriminator fixture, ROWS still behaves like RANGE (ties share) — `rows_frame_running_does_not_share_ties` fails.
2. Sliding bounds ignored / not clamped — `rows_frame_moving_window_edges_respect_offsets` fails.
3. Empty frame not NULL/0 — `rows_frame_empty_window_is_null` fails.
4. Frame forced onto the node (functions can't differ) — `rows_frame_multiple_functions_can_differ_in_frame` fails.
5. 242 default-frame path regressed — `default_frame_aggregate_window_unchanged` fails.
6. A deferred unit (`RANGE`/`GROUPS`) or invalid ordering silently plans — the reject gate fails.
7. Distributed moving frame reachable from one producer — the distributed gate fails (236/238).

## Hygiene

- No m-word ("strictly increasing"); no version-suffix idiom; reuse the `Window` node + aggregate kernels.
- `--all-targets` (the new IR fields); cargo via `scripts/cargo-cgroup`; FULL `architecture_test` before
  sealing; re-pin postcard goldens if the `BoundWindowCall` shape shifts a preimage.

## Open for the implementing session (verify-first)

1. Probe `SUM(x) OVER (… ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)` binds + `fragment_plan` succeeds.
2. Confirm `ROWS` ≠ `RANGE`: each row gets its own frame end (so on the discriminator fixture tied
   `ORDER BY` rows get distinct cumulative values, where the `RANGE` default shares one).
3. Confirm frame slices clamp at partition boundaries and an out-of-range frame → empty aggregate.
4. Confirm two functions over one `OVER` spec with different frames stay one `Window` node.
5. Confirm the 242 default-frame (absent frame) path is byte-for-byte unchanged.
6. Confirm `lib`/postcard round-trips for the extended `BoundWindowCall` (re-pin goldens if needed).
