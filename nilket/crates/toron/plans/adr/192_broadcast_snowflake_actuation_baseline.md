# ADR 192: Broadcast-Snowflake Actuation Baseline

**Status:** Promoted. **Migration:** `plans/impl/192_broadcast_snowflake_actuation_baseline.md`.

ADR 181 climbed the ordered star to execution; 185/186 made the **hash** snowflake actuatable (binder-only — the executor already shuffles multi-level shapes). Every ADR from 181 to 191 deferred the same named item: the **broadcast-snowflake execution cliff**. ADR 192 climbs that rung — the *minimal executable* broadcast snowflake, as 181 was the minimal executable ordered star. Recognition, ordering, evidence, and the margin machinery (182-191) are untouched; this ADR makes a shape the binder already recognizes actually run.

## The cliff is a binder authorization cliff, not a fragmenter cliff

The arc named this deferral as "relax the fragmenter" (`rewrite_broadcast_star_probe` scan-build → published-sub-plan build). Grounding the premise corrects it: **the fragmenter already publishes a composite (join-tree) build as a single-partition producer.** `broadcast_composite_build_fragments_to_single_build_producer` (`fragment.rs:1698`) fragments a broadcast join whose build side is an `Exchange(partition_count=1)` over a *join* into two stages — a producer stage that **contains the join** and a probe consumer whose `upstream_stages` is that producer. `first_scan_metadata` recurses through joins (`fragment.rs:1154`) and the consumer binds the published input by a synthetic exchange-id table id (`shuffle_input_table_id(e) = TableId(u64::MAX - e)`, `task.rs:190`), not by re-reading a catalog table — so the build's internal shape is already irrelevant to publication.

The real blocker is upstream, in the binder:

- `force_hash_root = tree.has_branch_dimensions()` (`join_order.rs:1513`), then `if force_hash && selected_strategy != HashPartitioned { Declined(MixedStrategyStar) }` (`join_order.rs:~2004`). Any branchy snowflake whose edges resolve to broadcast is forced to hash or declined; the binder **never emits a broadcast-snowflake plan** for the fragmenter to handle.
- `mixed_or_broadcast_snowflake_root_declines_actuation` pins today's state: an all-broadcast branchy snowflake is *observed* (a `join_order_snowflake` row) but never *actuated* as a root `join_order` receipt.

So this is a **binder authorization cut**, gated by an end-to-end execution proof that the already-recognized shape truly executes. The executor needs no new primitive; the fragmenter is changed only if — and exactly as much as — the execution proof demands.

## Decision

Unlock the **uniform broadcast snowflake root** by replacing the hash-forcing boolean with typed authorization, lifting the root gate for the strategy-invariant all-broadcast shape, and proving it executes distributed. Keep multi-child branches conservative.

**D1 — Typed strategy authorization replaces `force_hash: bool`.** Add a typed local-actuation authorization in `binder/join_order.rs`:

```rust
pub(crate) enum StarOrderStrategyAuthorization {
    AnyUniform,
    HashOnlyCompositeProbe,
}
```

The boolean goes; the call site declares whether all uniform strategies are allowed or a composite probe must force hash. `bind_local_star_actuation` and `try_bind_star_order_actuation` consume the type. Keep the existing `MixedStrategyStar` decline label for 192 unless the implementation deliberately updates the explain labels, decision-log mapping, and guards as a separate user-surface choice; the behavioral cut is authorization, not new receipt vocabulary.

**D2 — Root broadcast snowflakes actuate only when strategy-invariant.** For the snowflake root, preflight declared and selected order exactly as today; the root may actuate as broadcast only when **both** resolve to `Some(StarOrderStrategy::Broadcast)` (reuse `uniform_star_order_strategy`). Hash snowflakes continue through the 185/186 path. `None` or mismatched strategies still decline `MixedStrategyStar`. Strategy uniformity, not depth, is the deferral seam: "should this edge be broadcast or hash?" is a cost-model question (191-adjacent, separate); "can a recognized all-broadcast snowflake run?" is this rung.

The old `force_hash_root` also suppresses flat-star render declines for snowflake-root scratch failures (`if !force_hash_root`). That is a separate render-state behavior, not part of strategy authorization. Preserve it with an explicit `root_is_snowflake` / render-suppression predicate so lifting broadcast authorization does not reintroduce duplicate `join_order` decline rows beside an observed or actuated snowflake.

**D3 — Branch policy stays conservative.** ADR 192's user-visible unlock is the **root fact-probe** broadcast snowflake. Recursive multi-child branch stars keep `HashOnlyCompositeProbe`: a branch acts as a nested probe for its own children, and the fragmenter's `rewrite_broadcast_star_probe` rejects nested broadcast probes (`BroadcastProbeNotStarLinear`). Keeping branches hash-built (then published once as a composite build) keeps the first broadcast unlock inside the fragmenter's proven safe shape. All-broadcast multi-child branches are deferred until an execution proof requires and validates a broader stage traversal.

**D4 — The execution proof drives fragmenter scope.** Start the implementation with a discriminating *distributed* execution fixture (Phase 0). If typed binder authorization alone yields a broadcast snowflake that fragments and executes correctly over multiple workers, 192 stays **binder-only** (plus tests/guards). If the fixture shows the fragmenter needs new nested-producer dependency traversal, add the **smallest worklist-based** change. `broadcast_composite_build_fragments_to_single_build_producer` is a fragment-*shape* proof, not an end-to-end *result* proof — so the fixture, not the assumption, decides.

**D5 — Probe linearity is preserved.** `BroadcastProbeNotStarLinear` and the nested-probe rejection remain. 192 authorizes composite *build* sides, not arbitrary bushy broadcast *probes*.

**D6 — Reuse composite build publication; no new surface.** 192 reuses the existing single-partition `ShuffleProducer` / partition-0 `ShuffleConsumer` shape. No new task-input variant, control protocol, catalog, storage, Wendelon, or wire surface. The published-build identity is the exchange id; the branch's joins (with their `replay_id` / `replay_eligibility`, ADR 188) live in the **producer sub-stage**, so the parent consumer sees a replay-neutral shuffle input — replay stays deterministic, no collision, no new replay surface.

**D7 — Honest actuation, locked by distributed execution.** The headline gate executes a uniform broadcast snowflake across a multi-worker cluster and asserts (a) a root `join_order` actuation receipt (not merely an observed snowflake row), (b) the selected root strategy is broadcast, (c) distributed execution succeeds, (d) the result digest matches the hash / declared-order baseline. It must **fail on pre-192 code** (root is observation-only / hash-forced). Fragmentation success ≠ correctness — a fragment-only assertion would pass for the wrong reason.

## On recursion vs. worklist traversal

Queue/worklist traversal is the right long-run shape for stage graphs: galaxy, bridge, multi-fact, cycles, and memo all become graph/DAG traversal, where a queue plus visited-set generalizes and recursive tree-walking does not. But 192 should not churn every recursive helper. The binder snowflake code is tree-local and already recursive over an *already-built* `SnowflakeStarTree`; rewriting it during an authorization cut would be infrastructure with no independent behavior. The rule for 192:

- keep existing binder recursion when the cut only changes authorization over the built tree;
- do **not** add new recursive fragmenter/stage traversal;
- if (and only if) D4's execution proof shows the fragmenter needs new dependency traversal, implement it as an explicit worklist/`VecDeque`, never another recursive descent, with a guard that producers precede consumers via explicit upstream stage ids.

So recursion is acceptable as a *reused, tree-local* implementation detail; a worklist is *mandatory* for any newly-created stage/dependency traversal.

## Forward Arc

`186 hash-snowflake actuation → 192 uniform broadcast-snowflake baseline (this) → mixed-strategy snowflake (strategy-aware cost model) → galaxy / bridge → memo (only when receipts show local decisions fighting)`. 192 is the last cheap rung *inside* the broadcast snowflake shape: it reuses recognition (182-186), the broadcast-star fragmenter, and every execution primitive — lifting one binder gate behind a typed authorization, behind an execution proof. The worklist question becomes load-bearing only when topology stops being a tree; 192 must not pretend that future has arrived, but must not add recursion to the stage-graph side either.

## Deferred (named)

Mixed-strategy snowflake (broadcast + hash edges — strategy-aware cost model); all-broadcast multi-child branch stars (until proven by execution); broadcast/hash placement *choice* tuning (191-adjacent); full stage-graph worklist if D4 doesn't require it now; galaxy / bridge / multi-fact topology; measured-cardinality → optimizer-facts feedback; outer-join order; binder join-depth recursion (pre-existing, all-joins); memo / Cascades.
