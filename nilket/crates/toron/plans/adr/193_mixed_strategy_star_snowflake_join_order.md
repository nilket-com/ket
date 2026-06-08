# ADR 193: Mixed-Strategy Star/Snowflake Join Order

**Status:** Promoted. **Migration:** `plans/impl/193_mixed_strategy_star_snowflake_join_order.md`.

ADR 177–192 climbed the **uniform-strategy** tree: all-hash and all-broadcast stars and snowflakes recognize, order, actuate, execute, and self-tune. The remaining gap is the realistic warehouse query — one **fat** dimension that wants `HashPartitioned` and several **small** ones that want `Broadcast` at the same local star level. Today that declines: the actuator collapses the per-edge strategies to one uniform summary and rejects the mixture as `MixedStrategyStar`. ADR 193 makes the baseline mixed root star/snowflake **actuate and execute**, closing the next tree-shaped rung without claiming arbitrary mixed-tree coverage.

## Grounding (the cost model is a red herring; the fragmenter is the cliff)

Iter-0 proposed a "strategy-aware cost model." All three council seats grounded that as wrong, and the correction is load-bearing:

- **Per-edge strategy is already chosen.** `choose_auto_join_strategy` (`binder/join.rs:430`) picks hash vs broadcast per edge by the byte cap during each `bind_scoped_join` preflight. 193 invents no strategy selection.
- **Row-only ordering remains the first invariant.** `order_cost` (`join_order.rs:1351`) is `Σ(len-idx)·rows`; it ranks dimension selectivity regardless of each edge's final strategy. The `adr181_reorder_is_strategy_invariant` guard (`tests/architecture/join_order.rs:653`) pins that `rank_star_order` / `order_cost` / `classify_star_order_action` never read `JoinStrategy`. **193 preserves that invariant, it does not invert it.** It does not assert row-only ranking is the final global mixed-strategy cost model; that remains evidence-driven future work.
- **The binder already builds mixed plans.** When star actuation declines, `bind_plan_with_trace` falls back to recursive `bind_join`, constructing the declared-order tree with each edge's strategy chosen per-edge (`binder.rs:1070-1087`, `:1933-1992`). The in-process executor already evaluates that nested mixed tree (`snowflake_order_evidence_is_observational`).
- **Distributed fragmentation is the cliff.** `fragment_join_plan` (`distributed/fragment.rs:374`) dispatches on the *single topmost* join's strategy and assumes one exchange boundary: hash fragmentation requires both inputs to be a bare `Exchange(scan)` (`extract_exchange` → `JoinInputNotExchanged`, `:1028`); broadcast fragmentation rewrites only an all-broadcast linear probe chain (`BroadcastProbeNotStarLinear`, `:623`); the base path rejects `count > 1` boundaries (`MultipleExchangeBoundaries`, `:286`). A mixed plan — `Join(Broadcast, probe=Join(Hash,…))` — matches none.
- **The receipt cannot tell the truth.** `StarOrderEvidence.strategy` is a single `Broadcast | HashPartitioned` (`:311`). Observed snowflake evidence already carries per-edge `SnowflakeEdgeStrategy` (`:356`); acted receipts need the same plus an explicit `Mixed` summary.

**The consequence sets the cut's shape:** relaxing only the binder gate turns a clean observed decline into an actuated plan that **crashes at distributed `fragment_plan` — strictly worse than today.** So the fragmenter capability is *mandatory*, and the honesty gate is end-to-end distributed execution. The binder gate is a few edits; the fragmenter is the ADR.

## Decision

Make mixed hash/broadcast **root** local-star levels (flat stars and snowflake roots) actuatable and executable in the baseline shape, with the distributed fragmenter learning to stage a heterogeneous plan. Branch-local mixed stays deferred.

**D1 — "Strategy-aware" means per-edge-aware, not strategy-costed.** 193 consumes the preflight `DecisionReceipt` vector instead of collapsing it to a uniform strategy. `order_cost`, `rank_star_order`, and soft-margin classification stay row-only — no `strategy_aware_order_cost`, no network-cost term, no cost-based strategy choice. If later verdicts show row-only mixed ordering misranks, that is a separate cost-model ADR.

**D2 — Explicit `Mixed` classification after the strict uniform recognizers.** `uniform_star_order_strategy` / `uniform_snowflake_order_strategy` remain the strict ADR-181/192 fast paths, unchanged. A new classifier runs after them: all-broadcast (valid placements) → `Broadcast`; all-hash → `HashPartitioned`; a valid hash/broadcast mixture → `Mixed`; invalid placement, missing decision, non-inner/non-equi, or an **unauthenticated** snowflake `Broadcast + Natural` → decline. ADR 192's `composite_build_positions` authentication still gates any `Broadcast + Natural`; mixed classification must not bypass that fence.

**D3 — Represent local strategy as a `Mixed` summary plus per-edge decisions.** Add `Mixed` to the strategy summary (extend `StarOrderStrategy` or an equivalent local summary), and carry the preflighted per-edge strategy/placement list on the acted receipt. A mixed receipt renders `strategy=mixed` with detail `strategies=[c:hash_partitioned/natural,s:broadcast/swapped_right_build]`. Render/telemetry only.

**D4 — Authorize `Mixed` only at root local-star levels, with a per-edge fingerprint.** Flat-star and snowflake-root levels may act on `Mixed` only when **declared and selected** preflights both classify as `Mixed` *and* their per-dimension strategy fingerprint matches: the same dimension labels resolve to the same hash/broadcast strategy in both orders. Placement may differ only where the existing ADR 192 composite-build fence explicitly authenticates it. ADR 192's selected-broadcast upgrade stays broadcast-specific — it does **not** become a general "selected wins" rule for mixed summaries. Recursive **branch-local** mixed stays declined through the hash-only branch authorization (`HashOnlyCompositeProbe`), keeping nested-broadcast-probe risk out of branch composites until a fragmenter proof says otherwise.

**D5 — Distributed execution is part of the cut (fragmenter staging, worklist, baseline).** The headline runs a mixed star/snowflake through distributed execution and digest-matches an all-hash / declared-order baseline. The fragmenter gains the smallest mixed-staging capability the discriminating fixture requires:
- a join input may be a **fragmented upstream subtree**, not only a bare `Exchange(scan)`;
- broadcast over a hash-shuffled probe subtree for the baseline left-deep mixed root; hash over a published broadcast subtree only if the fixture requires it;
- new stage-dependency traversal is an explicit **worklist/`VecDeque`** (ADR-192 discipline), never a new recursive graph descent, with producers preceding consumers via `upstream_stages`;
- existing uniform hash and uniform broadcast fragmentation stay **byte-identical**.
The arbitrary-bushy general case is deferred; the execution proof, not assumption, fixes the exact scope.

**D6 — `MixedStrategyStar` survives for still-deferred shapes.** It remains the decline reason for branch-local mixed, unsupported bushy mixed plans, invalid placements, non-scan-linear probes, and any mixed plan the fragmenter cannot prove safe. The baseline boundary is pinned by a still-declining fixture.

**D7 — No topology expansion or optimizer shell.** No galaxy, bridge, many-to-many, cycles, outer joins, memo, Cascades, `Optimizer`, or broad `PlanProperties`. 193 consumes the existing flat-star and single-fact snowflake recognizer outputs.

**D8 — Ring/runtime boundaries.** Toron-owned. Fragmenter changes are in-scope because distributed mixed execution is the load-bearing proof. No executor kernel, storage, Wendelon, PBC, SQL grammar, or wire change.

## Honesty gate

`mixed_strategy_snowflake_root_actuates_and_executes_distributed`: the canonical mixed snowflake (`orders ⋈ customers ⋈ {regions, segments}` + `stores`, one dim over-cap → hash, the rest under-cap → broadcast) stops rendering observe-only, emits a root `join_order` receipt with `strategy=mixed`, executes distributed across multiple workers, and digest-matches the all-hash baseline. It must **fail on pre-193** (observed-only and/or fragmenter rejection). Fragment-shape success ≠ correctness; lock by execution. Because a binder-gate-only change would *regress* (actuate→crash), the test proving the fragmenter capability is the design lock.

## Forward Arc

`192 broadcast-snowflake uniformity → 193 mixed-strategy star/snowflake order (this; the fragmenter learns to stage the baseline heterogeneous root) → galaxy evidence → bridge evidence → bounded topology actuation → memo when receipts show local decisions fighting`. 193 closes the root-level mixed tree rung; branch-local mixed and arbitrary bushy mixed trees remain explicit follow-ons. The heterogeneous-stage fragmenter is also the substrate galaxy/bridge will need, paid for once here.

## Deferred (named)

Strategy-aware cost model; cost-based strategy choice; branch-local mixed actuation; arbitrary bushy mixed plans; galaxy/bridge topology; outer joins; memo/Cascades; durable/persisted optimizer evidence.
