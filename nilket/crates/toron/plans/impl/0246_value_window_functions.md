# Impl Plan — Toron ADR 246: Value Window Functions

Pairs with `plans/adr/0246_value_window_functions.md`. Extend the 241–245 `Window` operator with a
`WindowFunction::Value(ValueFunction)` arm: positional `LAG`/`LEAD` + frame-aware `FIRST_VALUE`/
`LAST_VALUE`/`NTH_VALUE`, returning an expr's value at a target row. **`NTILE`, `IGNORE NULLS`, expression
defaults, and `NTH_VALUE … FROM LAST` stay rejected. No new operator, no optimizer/durable/Wendelon/Rune
surface.**

## Touch surface

- `ir.rs` — add `pub enum ValueFunction { Lag { offset: u64, default: ScalarValue }, Lead { offset: u64,
  default: ScalarValue }, FirstValue, LastValue, NthValue { n: u64 } }` and a `WindowFunction::Value(
  ValueFunction)` variant. The value `expr` rides on `WindowFunctionCall.args`/`BoundWindowCall.arg`
  (reused from 242). Update the `WindowFunction` serializer/exhaustive matches and re-pin the postcard
  goldens if the new variant shifts the shape.
- `ir/execution.rs` — `BoundWindowCall` already carries `function`/`arg`/`frame`; no new fields.
- `sql_planner.rs` — in `extract_window_call`, accept `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`
  (via `function_name_eq`): extract the value expr (arg 0) → `args`; parse `LAG`/`LEAD` `offset` (arg 1,
  non-negative int literal, default 1) + `default` (arg 2, constant literal → `ScalarValue`, default
  NULL); parse `NTH_VALUE` `n` (arg 1, positive int literal). Build `WindowFunction::Value(…)`. Require
  `ORDER BY` for `LAG`/`LEAD`. Reject `NTILE`, `IGNORE/RESPECT NULLS`, non-constant defaults,
  `NTH_VALUE … FROM LAST`, frames on `LAG`/`LEAD`? (no — frame is *ignored*, not rejected, per D2; accept
  but drop it). Keep ranking/aggregate paths unchanged.
- `binder.rs` — in the `Window` arm: for a `Value` call, bind the value `expr` (`bind_expr_program_scoped`)
  to `arg`; output field = the expr's bound type, **nullable**; coerce/validate the `LAG`/`LEAD` `default`
  ScalarValue against the expr type (reuse the cast/coercion path). `validate_range_frame_offsets` already
  only fires for `Range` frames. Row facts unchanged.
- `exec.rs` — extend the `WindowFunction` match in `execute_window`: a `Value` call computes its column
  per row. `Lag`/`Lead`: `take` the value expr at `i ∓ offset` within the partition (`partition_start`/
  `partition_end`), else the `default` (built via `literal_array`/scalar). `FirstValue`/`LastValue`/
  `NthValue`: resolve the frame slice `[lo, hi]` exactly as the aggregate frame path (`None` → 242 default
  peer slice; `Some(frame)` → the 243/244/245 ROWS/RANGE/GROUPS resolution — factor the per-row `[lo,hi]`
  out of the aggregate executors into a shared helper), then `take` the value at `lo` / `hi` / `lo+n-1`
  (NULL past `hi`). Reuse `Executor::evaluate_bound_program_without_storage` + `take`. The value column is
  assembled by `take` over an index vector (target row per output row, or a NULL sentinel).
- `distributed/fragment.rs` / walkers — no change.
- tests: `tests/window_test.rs` + `tests/distributed_process_test.rs` + `tests/architecture/window_functions.rs`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Window function enum + call | `WindowFunction` / `WindowFunctionCall` (ir.rs); `BoundWindowCall` (ir/execution.rs) |
| Window lowering (extend) | `fn extract_window_call` / `fn lower_select_window` (sql_planner.rs) |
| Window bind (extend) | the `LogicalPlan::Window` arm in `bind_plan_with_trace` (binder.rs) |
| Sorted partition + arg eval | `sorted_order` + `Executor::evaluate_bound_program_without_storage` in `fn execute_window` (exec.rs) |
| Frame slice `[lo,hi]` (reuse/extract) | `execute_window_aggregate_rows`/`_range`/`_groups` + the default peer path (exec.rs) |
| Integer-literal extraction | `fn window_frame_offset` (sql_planner.rs) |

## Phase 0 — Red discriminators

`lag_returns_prior_row_value`; `lead_returns_next_row_value`; `lag_offset_and_default_at_partition_edge`;
`lag_lead_ignore_frame`; `first_last_value_respect_frame`; `nth_value_uses_frame_ordinal_and_null_past_end`;
`value_functions_distributed_gate_is_value_discriminating`. Red on `main` (value functions rejected).

## Phase 1 — IR

Add `ValueFunction` + `WindowFunction::Value`; `--all-targets` for the exhaustive `WindowFunction` matches
(default-name, lowering, binder, executor) + the variant-count architecture guards.

## Phase 2 — Lowering

Accept the five functions; extract expr + offset/default/n; require `ORDER BY` for LAG/LEAD; reject NTILE /
null-treatment / non-constant default / FROM LAST. Frame on LAG/LEAD is dropped (ignored), not rejected.

## Phase 3 — Binder + verify-first probe

Bind the value expr; nullable expr-typed output; coerce the LAG/LEAD default to the expr type.
**VERIFY-FIRST:** probe that `LAG(x) OVER (ORDER BY o)` and `FIRST_VALUE(x) OVER (ORDER BY o)` bind AND
`fragment_plan` succeeds before building the executor.

## Phase 4 — Executor

Factor the per-row frame slice `[lo,hi]` out of the aggregate executors into a shared helper; LAG/LEAD use
a positional target; FIRST/LAST/NTH use the frame slice; assemble each value column by `take` (target-row
index per output row, NULL sentinel for edges/empty/past-end).

## Phase 5 — Tests, guards, verify, seal

Behavior gates (prior/next value, offset+default at edge, per-partition restart, LAG-ignores-frame,
FIRST/LAST-respect-frame, NTH ordinal + NULL-past-end, row-preservation, share-one-node, deferred
rejects); the distributed value-discriminating gate; the eight `adr246_*` guards; reconcile the 242
`WindowFunction` pins + variant-count guards; re-pin postcard goldens the `WindowFunction` add shifts. Full
verification gate. Commit `impl: adr 246 value window functions`.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test window
scripts/cargo-cgroup test  -p toron --test distributed_process_test value
scripts/cargo-cgroup test  -p toron --test architecture_test adr246
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL (reconciled 242 + variant pins)
scripts/cargo-cgroup test  -p toron --test distributed_process_test   # FULL
scripts/cargo-cgroup test  -p toron --lib                             # serde/postcard round-trips
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. LAG/LEAD off-by-one or leak across partitions — `lag_returns_prior_row_value`/`lag_lead_restart_per_partition` fail.
2. Default not used at the partition edge — `lag_offset_and_default_at_partition_edge` fails.
3. LAG/LEAD honor the frame (should ignore it) — `lag_lead_ignore_frame` fails.
4. FIRST/LAST/NTH ignore the frame (should respect it) — `first_last_value_respect_frame` fails.
5. NTH past the frame end not NULL — `nth_value_uses_frame_ordinal_and_null_past_end` fails.
6. A deferred form (NTILE, IGNORE NULLS, non-constant default) silently plans — a reject gate fails.
7. Distributed value lookup reachable from one producer — the distributed gate fails (236/238).

## Hygiene

- No m-word ("strictly increasing"); no version-suffix idiom; reuse the `Window` node + the frame slice.
- `--all-targets` (the new `WindowFunction` variant); cargo via `scripts/cargo-cgroup`; FULL
  `architecture_test` before sealing; re-pin postcard goldens if the `WindowFunction` shape shifts a preimage.

## Open for the implementing session (verify-first)

1. Probe `LAG(x) OVER (ORDER BY o)` + `FIRST_VALUE(x) OVER (ORDER BY o)` bind + `fragment_plan` succeed.
2. Confirm LAG/LEAD ignore an explicit frame; FIRST/LAST/NTH respect it (the dual discriminators).
3. Confirm LAG/LEAD default + partition clamping at edges; per-partition restart.
4. Confirm NTH_VALUE ordinal + NULL past the frame end; the value column nullability.
5. Confirm the frame-slice helper extraction leaves the 243/244/245 aggregate behavior byte-for-byte unchanged.
6. Confirm serde/postcard round-trips for the extended `WindowFunction` (re-pin goldens if needed).
