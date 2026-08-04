# ADR 192: Broadcast-Snowflake Uniformity Across Composite-Build Placement

**Status:** Promoted. **Migration:** `plans/impl/0192_broadcast_snowflake_actuation_baseline.md`.

ADR 181 made ordered flat stars executable. ADRs 185/186 made hash snowflakes executable. The next rung is still the broadcast snowflake, but the diagnostic trace changed the mechanism: this is not a fragmenter rewrite, and not merely the old `force_hash_root` authorization gate. The blocking surface is ADR 181's local star uniformity preflight when one "dimension" is already a composite branch build.

## Grounding

The fragmenter already publishes composite broadcast builds. `broadcast_composite_build_fragments_to_single_build_producer` fragments a broadcast join whose build side is an `Exchange(partition_count=1)` over a join tree into a single producer stage consumed by the root probe stage. The published input is identified by exchange id, not by re-reading a base table. So the distributed primitive exists.

The canonical multi-child snowflake still declines before authorization becomes relevant:

```sql
orders o
JOIN customers c ON o.customer_id = c.id
JOIN regions r ON c.region_id = r.id
JOIN segments g ON c.segment_id = g.id
JOIN stores s ON o.store_id = s.id
```

with `customers` as the multi-child branch and all dimension sides under the broadcast cap.

The traced decline chain is:

- Gate 1: placement strictness. The root preflight sees all broadcast edges, but placements differ: `(Broadcast, Natural)` for the composite branch build and `(Broadcast, SwappedRightBuild)` for the leaf build. ADR 181's `uniform_star_order_strategy` only accepts flat-star broadcast as all `SwappedRightBuild`, so it returns `None`.
- Gate 2: order-variant strategy drift. If Gate 1 is experimentally relaxed, the declared order preflights as `HashPartitioned` while the selected order preflights as `Broadcast`, so `declared_strategy != selected_strategy` still declines as `MixedStrategyStar`.

The old `force_hash_root` boolean still deserves cleanup because it mixed strategy authorization with render-state suppression, but replacing it alone is regression-neutral. ADR 192's behavioral cut is the fenced snowflake uniformity rule plus the selected-broadcast upgrade, not the typed authorization by itself.

## Decision

Unlock the first broadcast snowflake root whose selected plan is structurally a broadcast-uniform composite-build plan. Keep flat-star ADR 181 invariants unchanged.

**D1 - Keep flat-star uniformity strict.** `uniform_star_order_strategy` remains the flat-star recognizer. It continues to require broadcast flat stars to be all `(Broadcast, SwappedRightBuild)` and hash flat stars to be all `(HashPartitioned, Natural)`. No flat-star behavior is relaxed.

**D2 - Add fenced snowflake uniformity.** Add a snowflake-specific preflight helper, e.g. `uniform_snowflake_order_strategy`, used only by snowflake root actuation. It may classify a plan as broadcast only when every decision is `JoinStrategy::Broadcast` and each placement is one of:

- `SwappedRightBuild` for normal leaf broadcast builds;
- `Natural` for authenticated composite branch builds that are already published as a single build side.

Any hash/broadcast mixture, unknown placement, non-composite `Natural` broadcast, or unauthenticated shape still returns `None`.

**D3 - Selected broadcast may upgrade declared hash only inside the snowflake fence.** ADR 181's declared-vs-selected equality rule stays for flat stars. For snowflake roots, a selected plan that passes D2 as broadcast may actuate even when the declared preflight is `HashPartitioned` or `None`, because the selected plan is the executable broadcast shape being proven. This is not a blanket "selected wins" rule: if the selected snowflake preflight is not authenticated broadcast-uniform, decline.

**D4 - Typed strategy authorization remains substrate.** Replace `force_hash: bool` with `StarOrderStrategyAuthorization::{AnyUniform, HashOnlyCompositeProbe}`. The root uses `AnyUniform`; branch-local composite probes remain fenced by the same structural checks. Keep render-decline suppression separate as `root_is_snowflake` so the old boolean's render behavior is not inferred from strategy authorization.

**D5 - Preserve probe-linearity and fragmenter scope.** `BroadcastProbeNotStarLinear` remains. ADR 192 authorizes a root probe over published composite build sides; it does not authorize arbitrary nested broadcast probes or a general broadcast DAG. If the headline distributed fixture proves the existing composite-build publication works, do not change `fragment.rs`. If it fails for dependency traversal, add only an explicit worklist/`VecDeque` stage traversal, not recursive fragmenter descent.

**D6 - Result identity is the safety fence.** The headline gate must fail on pre-192 code, actuate after the fenced uniformity change, execute distributed over multiple workers, and match the hash or declared-order baseline by schema and row digest.

## Recursion vs. Worklist

Existing binder recursion over `SnowflakeStarTree` is acceptable because the input is a tree and ADR 192 is still a local actuation cut. New stage/dependency traversal is different: any new fragmenter traversal must be a queue/worklist with explicit upstream stage ids. ADR 192 should not add recursive graph traversal.

## Headline Gate

`broadcast_snowflake_root_actuates_and_executes_distributed`

- canonical multi-child branch snowflake: `orders` root probe, `customers` composite branch, `regions` and `segments` branch children, `stores` root leaf child;
- on main: root declines before authorization through ADR 181 preflight gates;
- after ADR 192: selected root preflight authenticates broadcast-uniform composite-build placement, emits a root `join_order` receipt, and records broadcast strategy;
- distributed execution succeeds without replay and digest-matches the hash baseline.

## Supporting Gates

- `snowflake_uniformity_accepts_composite_build_natural_broadcast`;
- `flat_star_uniformity_remains_strict_swapped_right_build`;
- `snowflake_selected_broadcast_upgrade_is_fenced`;
- `snowflake_non_composite_natural_broadcast_declines`;
- `mixed_strategy_snowflake_still_declines_actuation`;
- `nested_broadcast_probe_still_rejects_fragmentation`;
- `broadcast_snowflake_receipts_are_edge_owned`;
- `broadcast_snowflake_root_decline_render_state_stays_exclusive`.

## Architecture Guards

- `adr192_snowflake_uniformity_is_isolated_from_flat_star`;
- `adr192_selected_broadcast_upgrade_is_snowflake_specific`;
- `adr192_composite_natural_broadcast_is_authenticated`;
- `adr192_no_raw_force_hash_boolean`;
- `adr192_root_authorization_does_not_own_render_suppression`;
- `adr192_fragmenter_probe_linearity_is_preserved`;
- `adr192_no_storage_wendelon_or_wire_surface`;
- `adr192_no_memo_cascades_or_cost_model`.

## Forward Arc

`186 hash-snowflake actuation -> 192 broadcast-snowflake composite-build uniformity -> mixed-strategy snowflake cost model -> galaxy / bridge -> memo when receipts show local decisions fighting`.

## Deferred

Mixed broadcast/hash snowflakes; cost-based broadcast/hash placement choice; all arbitrary nested broadcast probes; galaxy/bridge topology; outer joins; full stage-graph worklist unless execution proves it is required here; memo/Cascades.
