# Impl Plan — Toron ADR 227: Probe-Linearity Fence Receipts

Pairs with `plans/adr/227_probe_linearity_fence_receipts.md`. Replace the lossy
`broadcast_probe_is_scan_linear -> bool` with a typed exhaustive `broadcast_probe_linearity ->
ProbeLinearity`, render the blocking cause in both fence consumers' EXPLAIN receipts, change
**no plan / fragmenter / durable evidence / decision matrix**, re-pin EXPLAIN goldens in-cut.

Touch surface:
- `binder.rs` — compute typed left/right probe linearity once and carry it into
  `JoinInputOptimization` and join-order edge evidence.
- `binder/join.rs` — `ProbeLinearity` / `ProbeLinearityFence` enums;
  `broadcast_probe_linearity(plan)` (exhaustive over `ExecutionPlan`, no wildcard);
  `broadcast_probe_is_scan_linear` becomes a `matches!(..., Linear)` wrapper;
  `JoinInputOptimization` and `HashByDefaultReason::ProbeNotScanLinear` carry typed causes.
- `binder/explain.rs` — render the typed cause at the two `ProbeNotScanLinear` sites (2383
  hash-by-default; 2742/2771 star decline).
- `binder/join_order.rs` — join-order edge evidence and star decline carry the cause to render
  receipts via the shared helper (no second classifier).
- `binder/tests.rs` — discriminators + byte-identical-plan net; re-pin EXPLAIN goldens.
- `tests/architecture/{optimizer,join_order,optimizer_accrual,rune_frontend,optimizer_memo_admission}.rs`
  as needed — the `adr227_*` guards and retargeted render/source-scan assertions.

No `runtime_evidence.rs` / `decision_log.rs` (no new reason), daemon, executor, fragmenter,
storage, wire, or Wendelon change.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Bool to retire | `broadcast_probe_is_scan_linear` (join.rs:703); `Filter`/`Project` recurse, eligible inner `Broadcast` join recurses on `right`/probe |
| Carrier to widen | `left_probe_scan_linear` / `right_probe_scan_linear` in binder.rs:2190-2244 feed `JoinInputOptimization` and `JoinOrderEdgeEvidence` |
| Consumer 1 (hash fallback) | `choose_auto_join_strategy` filters `left_viable`/`right_viable` by `probe_scan_linear` and can emit `HashByDefaultReason::ProbeNotScanLinear` (join.rs:538-612) |
| Consumer 2 (star decline) | star fence (join_order.rs:4216) + branch authorization (4557) |
| Render sites | explain.rs:2383 / 2742 / 2771 |
| Matrix to keep | `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` |
| 226 locks | `adr226_*` (all-zero contract, zero admissible) |

## Phase 0 — Red discriminators

`probe_linearity_project_and_filter_are_transparent`,
`probe_linearity_limit_sort_aggregate_exchange_are_distinct_causes`,
`probe_linearity_nested_broadcast_propagates_inner_cause`,
`hash_join_probe_not_scan_linear_renders_typed_cause`,
`hash_join_probe_not_scan_linear_uses_carried_input_cause`,
`star_probe_not_scan_linear_renders_same_cause_via_shared_helper`. Red on `main`.

## Phase 1 — Typed classifier (join.rs)

```rust
pub(super) enum ProbeLinearity {
    Linear,
    Blocked(ProbeLinearityFence),
}
pub(super) enum ProbeLinearityFence {
    ScanRowLimit, Take, Sort, Aggregate, Exchange, NonBroadcastJoin, NonScanLeaf,
}
pub(super) fn broadcast_probe_linearity(plan: &ExecutionPlan) -> ProbeLinearity { … }
pub(super) fn broadcast_probe_is_scan_linear(plan: &ExecutionPlan) -> bool {
    matches!(broadcast_probe_linearity(plan), ProbeLinearity::Linear)
}
```

Exhaustive `match` over `ExecutionPlan` — **no wildcard**. `Filter`/`Project` recurse
(transparent). The eligible inner `Broadcast` join recurses on `right`/probe and **propagates the
inner cause** (D2). Bool-only call sites can remain via the wrapper; receipt-producing carriers
migrate in Phase 2.

## Phase 2 — Carry the typed cause (binder.rs + join.rs + join_order.rs)

- In `binder.rs`, replace `left_probe_scan_linear` / `right_probe_scan_linear` with typed
  `left_probe_linearity` / `right_probe_linearity`, computed by
  `broadcast_probe_linearity(&left_plan.plan)` and
  `broadcast_probe_linearity(&right_plan.plan)`.
- In `JoinInputOptimization`, replace `probe_scan_linear: bool` with typed
  `probe_linearity: ProbeLinearity`; viability checks use `probe_linearity.is_linear()`.
- Change `HashByDefaultReason::ProbeNotScanLinear` to
  `ProbeNotScanLinear(ProbeLinearityFence)`, not `&'static str`.
- When build candidates exist but neither side is viable, select the fence from the blocked
  probe side tied to the candidate: left-build candidate uses the right input as probe;
  right-build candidate uses the left input as probe. If both are blocked, keep the existing
  deterministic build-side preference and pin it in the test.
- In `JoinOrderEdgeEvidence` and `SnowflakeEdgeStrategy`, replace `probe_scan_linear: bool`
  with typed `probe_linearity: ProbeLinearity`; analyzer checks pass the blocked fence into
  `StarOrderDeclineReason::ProbeNotScanLinear(fence)`.
- The star actuation decline calls the same `broadcast_probe_linearity` helper and passes the
  fence through `accrue_star_order_decline` and `render_star_order_decline`.
- `optimizer_decision_decline_reason` maps
  `StarOrderDeclineReason::ProbeNotScanLinear(_)` to the existing durable
  `OptimizerDecisionDeclineReason::ProbeNotScanLinear`.

Do not store the cause as `&'static str` before rendering.

## Phase 3 — Render the cause (explain.rs)

Add `ProbeLinearityFence::label() -> &'static str` (snake_case:
`scan_row_limit`/`take`/`sort`/`aggregate`/`exchange`/`non_broadcast_join`/`non_scan_leaf`).
Append the cause to the existing reason token at 2383/2742/2771 (e.g.
`probe_not_scan_linear:sort`) so the **durable reason string and the matrix stay untouched** —
the cause lives only in the render layer. Keep `declined_probe_not_scan_linear` coarse; put
the fence in the reason/detail token.

## Phase 4 — Goldens + guards

Re-pin every EXPLAIN golden referencing `probe_not_scan_linear` / `scan_linear`
(`tests/architecture/{optimizer,join_order,optimizer_accrual,rune_frontend}.rs`,
`binder/tests.rs`). Add the six `adr227_*` guards:

- `adr227_probe_linearity_is_typed_exhaustive_no_wildcard`
- `adr227_probe_linearity_cause_is_not_stringly_typed`
- `adr227_both_consumers_share_single_linearity_classifier`
- `adr227_no_plan_fragmenter_or_durable_change`
- `adr227_no_new_decision_reason_matrix_stays_140_220`
- `adr227_226_zero_admissible_and_no_generic_search_intact`

Confirm `adr216` (140/220) and the `adr226_*` locks stay green untouched.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib probe_linearity
scripts/cargo-cgroup test  -p toron --lib scan_linear
scripts/cargo-cgroup test  -p toron --test architecture_test adr227
scripts/cargo-cgroup test  -p toron --test architecture_test adr226
scripts/cargo-cgroup test  -p toron --test architecture_test adr216
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL — golden re-pins participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. A wildcard `_ =>` in `broadcast_probe_linearity`: `adr227_…_exhaustive_no_wildcard` fails.
2. String payload causes (`Blocked(&'static str)` or `ProbeNotScanLinear(&'static str)`):
   `adr227_probe_linearity_cause_is_not_stringly_typed` fails.
3. A second classifier in the star path:
   `adr227_both_consumers_share_single_linearity_classifier` + `star_…_renders_same_cause…` fail.
4. Keeping `JoinInputOptimization` bool-only:
   `hash_join_probe_not_scan_linear_uses_carried_input_cause` fails.
5. Any bound-plan change: `probe_linearity_receipts_do_not_change_bound_plan` fails.
6. New `OptimizerDecisionOutcome` / matrix off 140/220:
   `adr227_no_new_decision_reason_matrix_stays_140_220` + `adr216` fail.
7. Skipped golden re-pin: the FULL architecture binary fails on stale EXPLAIN goldens.

## Hygiene

- No version-suffix idiom; no memo engine / Cascades / `src/optimizer.rs` implementation hooks.
- No new durable keys; EXPLAIN render goldens re-pinned in-cut; decision matrix untouched.
- `--all-targets` after the enum addition.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Render shape — appended token (`probe_not_scan_linear:sort`) vs a separate receipt field;
   lean appended token to keep the durable reason and matrix untouched. Either way, re-pin.
2. Confirm both consumers (hash fallback + star decline) reach the *same* cause-label fn.
