# Impl Plan - ADR 192: Broadcast-Snowflake Uniformity Across Composite-Build Placement

Pairs with `plans/adr/192_broadcast_snowflake_actuation_baseline.md`.

This is a Toron binder cut unless the headline distributed proof shows otherwise. The fragmenter already publishes composite broadcast builds; the real blockers are the ADR 181 local-star preflight rules when a snowflake root dimension is a composite branch build.

## Anchors

| Concern | Anchor |
|---|---|
| Flat-star uniformity | `uniform_star_order_strategy` in `binder/join_order.rs` |
| Local actuator preflight | `preflight_star_order_strategy` and `bind_local_star_actuation` |
| Current snowflake decline fixture | `mixed_or_broadcast_snowflake_root_declines_actuation` |
| Branch construction | `bind_snowflake_dimension`, `bind_snowflake_root_dimensions`, `bind_declared_star_children` |
| Composite build publication | `broadcast_composite_build_fragments_to_single_build_producer` in `distributed/fragment.rs` |
| Probe-linearity fence | `BroadcastProbeNotStarLinear` and nested-probe rejection |
| Render exclusivity | `root_is_snowflake` / old `if !force_hash_root` behavior |

## Phase 0 - Restore the discriminating fixture first

Recreate the canonical multi-child broadcast snowflake and prove it fails on main before changing the preflight:

```sql
SELECT *
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN regions r ON c.region_id = r.id
JOIN segments g ON c.segment_id = g.id
JOIN stores s ON o.store_id = s.id
```

Fixture requirements:

- `orders` is the large root probe;
- `customers` is a multi-child branch with `regions` and `segments`;
- `stores` is a root leaf child;
- all dimension/build sides are under the broadcast cap;
- estimates make the selected root order broadcast-uniform and preferred;
- main declines root actuation through preflight, not through execution.

The headline test is `broadcast_snowflake_root_actuates_and_executes_distributed`: after implementation it must emit a root `join_order` receipt with broadcast strategy and distributed results must digest-match the hash baseline.

## Phase 1 - Keep typed authorization, but stop treating it as the unlock

Keep the regression-neutral substrate:

```rust
pub(super) enum StarOrderStrategyAuthorization {
    AnyUniform,
    HashOnlyCompositeProbe,
}
```

Root actuation uses `AnyUniform`; the old render suppression stays as a separate `root_is_snowflake` predicate. Remove comments that say `AnyUniform` alone makes broadcast snowflakes actuate. It only permits the later snowflake-specific preflight to make a decision.

## Phase 2 - Add snowflake-specific uniformity

Do not relax `uniform_star_order_strategy`; it is ADR 181's flat-star fence.

Add a helper on the snowflake path, for example:

```rust
fn uniform_snowflake_order_strategy(
    decisions: &[DecisionReceipt],
    composite_build_positions: &BTreeSet<usize>,
) -> Option<StarOrderStrategy>
```

Rules:

- empty decisions -> `None`;
- all `(HashPartitioned, Natural)` -> `Some(HashPartitioned)`;
- all `Broadcast` -> `Some(Broadcast)` only when every placement is authenticated:
  - `SwappedRightBuild` is valid for normal leaf builds;
  - `Natural` is valid only for a position known to be a composite branch build;
- any mixed strategy, unauthenticated `Natural` broadcast, or other placement -> `None`.

The authentication source must come from the built snowflake root dimensions, not from labels in SQL text. A practical shape is to carry a marker on `BuiltSnowflakeDimension` / `StarChildDimension` indicating whether that dimension's plan was built from child dimensions and is being published as a composite build. Keep this marker binder-local.

## Phase 3 - Fence the selected-broadcast upgrade

In the snowflake root actuator path, use the snowflake preflight for root dimensions. The flat-star path continues to use `preflight_star_order_strategy` and declared-vs-selected equality.

Snowflake root acceptance:

- selected preflight is `Some(Broadcast)` under `uniform_snowflake_order_strategy`;
- selected root dimensions are structurally authenticated as leaf or composite build placements;
- then allow actuation even if the declared preflight is `Some(HashPartitioned)` or `None`.

Decline cases:

- selected preflight is `None`;
- selected preflight is hash while declared and selected disagree;
- any mixed broadcast/hash strategy;
- unauthenticated `Broadcast + Natural`;
- non-inner/non-equi/probe-shape declines already handled by existing gates.

This addresses the traced Gate 1 (placement strictness) and Gate 2 (declared-vs-selected drift) without weakening ADR 181 globally.

## Phase 4 - Branch policy

Keep branch-local actuation conservative. A branch can use `AnyUniform` only when its probe is scan-linear and the branch output will be consumed as a published composite build by the root. Otherwise it stays `HashOnlyCompositeProbe`. The root snowflake preflight still authenticates any `Natural` broadcast placement as composite before accepting it.

## Phase 5 - Fragmenter decision

Run the headline distributed fixture after the binder changes.

- If it passes, keep ADR 192 binder-only and add guards proving the existing composite-build publication path is reused.
- If it fails because the stage graph needs nested dependencies, add the smallest worklist/`VecDeque` traversal in `distributed/fragment.rs`; do not add recursive broadcast-fragmenter traversal; keep `BroadcastProbeNotStarLinear`.

## Tests

Behavior:

- `broadcast_snowflake_root_actuates_and_executes_distributed` - canonical multi-child snowflake fails on main, actuates after ADR 192, executes distributed, digest-matches hash baseline.
- `snowflake_uniformity_accepts_composite_build_natural_broadcast` - `(Broadcast, Natural)` is accepted only for authenticated composite build positions.
- `snowflake_non_composite_natural_broadcast_declines` - prevents a flat-star or bogus leaf natural broadcast from entering through the snowflake helper.
- `flat_star_uniformity_remains_strict_swapped_right_build` - original helper still returns `None` for `(Broadcast, Natural)`.
- `snowflake_selected_broadcast_upgrade_is_fenced` - declared hash/none plus selected broadcast is accepted only on the snowflake root path.
- `mixed_strategy_snowflake_still_declines_actuation`.
- `nested_broadcast_probe_still_rejects_fragmentation`.
- `broadcast_snowflake_root_decline_render_state_stays_exclusive`.

Regression filters:

- `mixed_or_broadcast_snowflake_root_declines_actuation` retargeted to the genuinely mixed case;
- `hash_snowflake_root_reorders_on_robust_win`;
- `hash_snowflake_branch_emits_edge_owned_star_receipt`;
- `deep_snowflake_actuates_root_and_keeps_unary_branches_receiptless`;
- `three_way_broadcast_star_digest_matches_hash_over_tcp`;
- `broadcast_composite_build_fragments_to_single_build_producer`.

## Architecture Guards

- `adr192_snowflake_uniformity_is_isolated_from_flat_star` - original flat-star uniformity function is unchanged and the snowflake helper is separate.
- `adr192_selected_broadcast_upgrade_is_snowflake_specific` - declared-vs-selected relaxation appears only in snowflake root actuation.
- `adr192_composite_natural_broadcast_is_authenticated` - `Natural` broadcast is accepted only with a binder-owned composite marker.
- `adr192_no_raw_force_hash_boolean` - `force_hash: bool` is gone from `bind_local_star_actuation`.
- `adr192_root_authorization_does_not_own_render_suppression` - render suppression uses `root_is_snowflake` or equivalent, not strategy authorization.
- `adr192_fragmenter_probe_linearity_is_preserved`.
- `adr192_no_storage_wendelon_or_wire_surface`.
- `adr192_no_memo_cascades_or_cost_model`.

Retarget ADR 186's `adr186_root_hash_gate_commits_or_discards_branch_receipts` to the surviving invariant: branch receipts are committed only with an actuated root and discarded on root decline. It must not assert the old `force_hash_root` boolean.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib broadcast_snowflake_root_actuates_and_executes_distributed
scripts/cargo-cgroup test -p toron --lib snowflake_uniformity_accepts_composite_build_natural_broadcast
scripts/cargo-cgroup test -p toron --lib snowflake_non_composite_natural_broadcast_declines
scripts/cargo-cgroup test -p toron --lib flat_star_uniformity_remains_strict_swapped_right_build
scripts/cargo-cgroup test -p toron --lib snowflake_selected_broadcast_upgrade_is_fenced
scripts/cargo-cgroup test -p toron --lib mixed_strategy_snowflake_still_declines_actuation
scripts/cargo-cgroup test -p toron --lib nested_broadcast_probe_still_rejects_fragmentation
scripts/cargo-cgroup test -p toron --lib broadcast_snowflake_root_decline_render_state_stays_exclusive
scripts/cargo-cgroup test -p toron --test architecture_test adr186
scripts/cargo-cgroup test -p toron --test architecture_test adr192
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one test filter per command.

## Risks

- **Flat-star blast radius.** Do not relax `uniform_star_order_strategy`; isolate the snowflake helper.
- **Over-broad selected-wins rule.** Selected broadcast can bypass declared drift only after snowflake-specific structural authentication.
- **False composite authentication.** The composite marker must be binder-owned and derived from the constructed snowflake tree, not user text.
- **Fragmenter assumption.** The execution gate decides whether binder-only is real.
- **Regression-neutral substrate.** Typed authorization alone is not ADR 192's deliverable; avoid comments or tests claiming otherwise.

## Deferred

Mixed-strategy snowflakes; cost model for broadcast/hash placement choice; arbitrary nested broadcast probes; galaxy/bridge topologies; outer joins; full optimizer memo.
