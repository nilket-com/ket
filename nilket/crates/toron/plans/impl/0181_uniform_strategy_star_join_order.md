# Impl Plan - ADR 181: Uniform-Strategy Star Join Order

Pairs with `plans/adr/181_*.md`.

Extend 178's order eligibility from broadcast-only to **any uniform-strategy** star
(all-broadcast OR all-hash). Uniformity is proven by declared-order and selected-order
preflights over the binder's actual `DecisionReceipt`s; only matching uniform strategies
actuate. Binder-only: reuse 178's reorder + N-segment restore and 179/180 measure/verdict
unchanged. Mixed and snowflake decline (deferred).

## Anchors

| Concern | Anchor |
|---|---|
| Order eligibility / decline | `extract_star_join_candidate`; broadcast-only preflight decline in `try_bind_star_order_actuation` |
| Strategy basis | `choose_join_strategy`; `DecisionReceipt { strategy, placement, reason }` |
| Uniformity proof | declared and selected preflight decision slices |
| Reorder + restore (reuse) | 178 `actuate` + N-segment `restore_declared_join_order_project` |
| Measure/verdict (reuse, no change) | 179 measured map + 180 verdict (per-edge replay-id-keyed) |
| Hash execution (already works) | `execute_hash_join` (`exec.rs:2465`) — untouched |
| Tests | `binder/tests.rs`, `tests/explain_analyze_test.rs`; `tests/architecture/join_order.rs` |

## Phase 1 - Uniform-strategy classification

Add a helper that classifies a preflight decision slice:

```rust
fn uniform_star_order_strategy(decisions: &[DecisionReceipt]) -> Option<StarOrderStrategy>
```

Rules:

- `Broadcast` → every decision is `JoinStrategy::Broadcast` with
  `JoinPhysicalPlacement::SwappedRightBuild` (the dimension is the build side; 178
  unchanged).
- `HashPartitioned` → every decision is `JoinStrategy::HashPartitioned`.
- otherwise `None`.

Add `MixedStrategyStar` or `NonUniformStrategyStar` as the new decline reason. Keep the
existing fences (single probe, inner-equi, left-deep, scan-linear probe, leaf dims).

## Phase 2 - Preflight declared and selected orders

Where the order path currently preflights only the selected order and returns
`NotBroadcastStar` unless every selected decision is broadcast, run two no-render
preflights:

- declared order: `0..dimensions.len()`;
- selected order: the ADR 178 preferred order when the confidence/margin gate reorders,
  otherwise declared.

Classify both decision slices. Eligible only if both classify to the same
`StarOrderStrategy`. Decline mixed/non-uniform slices or strategy drift between declared
and selected. This is the safety proof that 181 remains order actuation, not hidden
order+strategy co-optimization.

## Phase 3 - Reorder over the proven uniform strategy (reuse 178)

`actuate` reorders by the 178 cost rule + confidence gate regardless of the uniform
strategy. Because both preflights proved the same strategy class, the selected order is
strategy-invariant for this cut. Wrap with the existing N-segment
`restore_declared_join_order_project` (strategy-agnostic) → output columns restored and
normalized row results identical. **No new reorder/restore code.**

## Phase 4 - Measure/verdict (verify, do not extend)

179's per-edge measured capture and 180's verdict key by `JoinReplayId`, strategy-agnostic;
they cover hash-star edges with no new surface. Add tests; change no code.

## Phase 5 - Behavioral tests

Add an all-hash star fixture (dims larger than the broadcast threshold) with a non-optimal
declared order; reuse 178's broadcast fixtures for regression.

- `uniform_hash_star_reorders_on_robust_win` - all-hash star reorders to cost-preferred;
  receipt shows hash + reordered.
- `hash_star_reorder_is_results_identical` - output schema/column order restored and
  normalized row multiset identical to declared (N>2 dims).
- `hash_star_reorder_preserves_hash_strategy` - each edge stays `HashPartitioned` with
  natural placement post-reorder.
- `declared_and_selected_preflights_share_uniform_strategy` - declared and selected
  preflights classify to the same strategy before actuation.
- `mixed_strategy_star_declines` - broadcast+hash star → `MixedStrategyStar`, declared kept.
- `broadcast_star_order_unchanged_from_178` - pure-broadcast regression.
- `hash_star_measured_verdict_extends` - 179 measured + 180 verdict for a hash star.
- `hash_star_order_is_binder_only` - no `fragment.rs`/executor diff.

## Phase 6 - Architecture guards

In `tests/architecture/join_order.rs`:

- `adr181_uniform_strategy_recognizer` - eligibility all-broadcast OR all-hash; mixed
  declines.
- `adr181_uniform_strategy_preflights_declared_and_selected` - declared and selected
  decision slices are both classified and must match before actuation.
- `adr181_reorder_is_strategy_invariant` - reorder reuses the 178 restore and only acts
  after matching uniform preflights.
- `adr181_hash_star_order_is_binder_only` - no executor/`fragment.rs` change.
- `adr181_reuses_178_restore_and_179_180_net` - reuses restore + measure/verdict; no second
  estimator.
- `adr181_no_optimizer_shell` - 177's no-cathedral guard holds.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib uniform_hash_star_reorders_on_robust_win
scripts/cargo-cgroup test -p toron --lib hash_star_reorder_is_results_identical
scripts/cargo-cgroup test -p toron --lib mixed_strategy_star_declines
scripts/cargo-cgroup test -p toron --test explain_analyze_test hash_star_measured_verdict_extends
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test adr181
scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

## Risks

- **Mixed must decline.** Strategy-invariance + the 178 cost rule only hold for *uniform*
  stars; mixed needs strategy-aware cost (deferred). Decline firmly; pin
  `mixed_strategy_star_declines`.
- **Selected-order-only preflight is insufficient.** A selected order can look uniform while
  the declared order was mixed, which would turn 181 into implicit strategy selection. Pin
  `declared_and_selected_preflights_share_uniform_strategy` +
  `hash_star_reorder_preserves_hash_strategy`.
- **Results-identical.** Reuse the 178 N-segment restore; pin restored output schema/column
  order and normalized row multiset equality at N>2 for the hash case.
- **No execution creep.** Binder-only; do **not** touch the fragmenter (that's snowflake).
  Pin `hash_star_order_is_binder_only`.
- **Net reuse, not re-build.** 179/180 cover hash edges by replay id; verify, don't fork.

## Deferred

Mixed-strategy star order (strategy-aware cost model); **broadcast snowflake order
(execution cliff: relax `rewrite_broadcast_star_probe` scan-build → published-sub-plan
build)**; multi-probe/galaxy; outer-join order; verdict-driven acts once verdicts accrue.
