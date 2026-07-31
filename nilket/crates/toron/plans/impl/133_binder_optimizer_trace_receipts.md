# Impl Plan — ADR 133: Binder Optimizer Trace Receipts

**Migration for** `plans/adr/133_binder_optimizer_trace_receipts.md`.
**Scope:** `crates/toron/src/binder.rs` (trace types + `bind_with_trace` + `&mut`
recording + `pub(crate)` bumps), guards, tests. **Non-scope:** `sql_planner` (no
EXPLAIN statement — that's ADR 134), exec/ir/wire/distributed/Wendelon, any 129–132
strategy logic.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| reason types module-private (must bump to `pub(crate)`) | bare `enum` `binder.rs:193` JoinPhysicalPlacement / `:199` BroadcastBuildSide / `:205` JoinStrategyDecisionReason / `:219` HashByDefaultReason |
| decision computed then reason discarded | `binder.rs:1118` `choose_join_strategy`; `:1163-1180` keep only strategy/placement |
| `bind` entry + Binder ctor / `bind_join` recursion (&mut threading) | grep `pub async fn bind(` / `fn bind_join` |
| no-wire precedent to preserve | the 130/131/132 `..._no_…_wire…` guards |

## Phase 0 — failing gate first
`trace_records_broadcast_inference_with_rejected_magnitude` calls the new
`bind_with_trace` and asserts the receipt — fails to compile/pass until the seam
exists. `bind_remains_structurally_identical_with_and_without_trace` passes after.

## Phase 1 — types + visibility
Add `JoinTraceTarget { join_index: usize }`, `DecisionReceipt`, `OptimizerTrace`
(all `pub(crate)`). Bump `JoinStrategyDecisionReason` + `HashByDefaultReason` +
`BroadcastBuildSide` + `JoinPhysicalPlacement` to `pub(crate)` (grep refs;
`--all-targets` catches misses). Derive `Clone` on the receipt; reason is already
`Copy`/`Clone` for its variants.

## Phase 2 — `&mut` accumulator + `bind_with_trace`
Thread `&mut OptimizerTrace` through the recursive binding path (no `RefCell`, no
`Binder` field). In `bind_join`, after `choose_join_strategy`, push
`DecisionReceipt { target: JoinTraceTarget { join_index: next }, strategy,
placement, reason: decision.reason }` — record auto/hash/pinned alike; assign
`join_index` in deterministic bind-traversal order. Add `bind_with_trace(...) ->
(ExecutionPlan, OptimizerTrace)`; make `bind` a one-line wrapper that calls it and
drops the trace. Always-collect.

## Phase 3 — guards + tests
Five `adr133_*` guards (binder-local; `pub(crate)`-not-`pub`; absent from
exec/wire/distributed; `bind` delegates; no substrate change). Behavior gates per
ADR. The structural-equivalence gate binds the same logical plan both ways and
compares the post-projection plan shape. No canonical plan digest exists today, so
use a focused structural helper or stable debug representation rather than adding a
new digest substrate.

## Phase 4 — verification
`cargo test -p toron` (trace gates + structural-equivalence) · `cargo test -p toron
--test architecture_test -- adr133` (+ re-run adr129 adr130 adr131 adr132) · `cargo
check -p toron --all-targets` (visibility-bump ripple is the backstop —
[[feedback_verify_all_targets_on_new_ir_field]]) · `git diff --check`.

## Risk register
- **Visibility-bump ripple:** `pub(crate)` on the reason enums may surface refs;
  `--all-targets` is the backstop. Keep `pub(crate)` not `pub` (guarded — no public
  API / wire leak).
- **&self vs &mut self:** thread the accumulator through the recursive bind path; if
  the binder borrows immutably somewhere, pass `&mut OptimizerTrace` as a param
  rather than adding interior mutability. Ground before coding.
- **Side-effect creep:** the trace push must not alter control flow / plan;
  `bind_remains_structurally_identical_with_and_without_trace` is the backstop.
- **Scope creep into EXPLAIN:** no `sql_planner`/result-set/`SystemTable` change this
  ADR — that's 134. (No EXPLAIN statement handling in the diff.)
- **`join_index` stability:** assign in a deterministic traversal; pin with
  `trace_indexes_each_join_deterministically` (self-join-safe).

## Acceptance
Deterministic per-join receipts (broadcast/hash/pinned) with full 129–132 reason +
`rejected_side_bytes`; `bind` structurally identical to `bind_with_trace`; trace +
reason types `pub(crate)`, off `ExecutionPlan`/wire; no
sql_planner/exec/wire/distributed/Wendelon change; five `adr133_*` + 129–132 guards
green.
