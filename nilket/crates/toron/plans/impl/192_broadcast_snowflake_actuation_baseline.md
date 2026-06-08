# Impl Plan — ADR 192: Broadcast-Snowflake Actuation Baseline

Pairs with `plans/adr/192_broadcast_snowflake_actuation_baseline.md`.

Unlock all-broadcast snowflake **root** actuation by replacing the hash-forcing boolean with typed strategy authorization, proving the broadcast snowflake executes distributed, and preserving the fragmenter's probe-linearity guard. Existing binder recursion stays unless the execution proof forces new fragmenter dependency traversal; any new fragmenter traversal must be worklist-based. The fragmenter already publishes composite builds (`broadcast_composite_build_fragments_to_single_build_producer`), so the production change is binder-first.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Hash-only root gate | `force_hash_root = tree.has_branch_dimensions()` — `join_order.rs:1513`, in `try_bind_star_order_actuation` (`:1428`) |
| Raw boolean gate | `bind_local_star_actuation(..., force_hash: bool, ...)` — `join_order.rs:1931/1936`; decline at `:2004` |
| Branch builders | `bind_snowflake_dimension` (`:1771`), `bind_snowflake_root_dimensions` (`:1741`), `bind_declared_star_children` (`:1850`) |
| Strategy recognizer | `uniform_star_order_strategy(decisions)` — `join_order.rs:1258` |
| Current decline fixture | `mixed_or_broadcast_snowflake_root_declines_actuation` (`binder/tests.rs`) |
| Composite build already fragments | `broadcast_composite_build_fragments_to_single_build_producer` — `fragment.rs:1698` |
| Probe-linearity guard | `BroadcastProbeNotStarLinear` (`fragment.rs:88`); nested-probe rejection |
| Shuffle-input identity | `shuffle_input_table_id(e) = TableId(u64::MAX - e)` — `task.rs:190`; `first_scan_metadata` recurses joins (`fragment.rs:1154`) |
| Distributed digest baseline (template) | `three_way_broadcast_star_digest_matches_hash_over_tcp` — `lib.rs:809` |
| ADR 186 guard to retarget | `adr186_root_hash_gate_commits_or_discards_branch_receipts` — `tests/architecture/join_order.rs:1350` |

## Phase 0 — Write the discriminating execution fixture first

Before changing authorization, add the headline test and confirm it **fails on current code**. This fixture, not an assumption, decides whether 192 is binder-only or needs fragmenter work — `broadcast_composite_build_fragments_to_single_build_producer` is a fragment-shape proof, not an end-to-end result proof.

- a small `orders ⋈ customers ⋈ regions` snowflake (`customers` is the branch);
- every build side under the broadcast cap;
- estimates make a different root order preferred than declared;
- compare distributed result digest against the hash / declared-order baseline (model on `three_way_broadcast_star_digest_matches_hash_over_tcp`);
- assert a root `join_order` actuation receipt is emitted and the strategy detail is broadcast.

## Phase 1 — Typed local strategy authorization

In `binder/join_order.rs`, add near `StarOrderStrategy`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StarOrderStrategyAuthorization {
    AnyUniform,
    HashOnlyCompositeProbe,
}
```

Change `bind_local_star_actuation`'s `force_hash: bool` to `authorization: StarOrderStrategyAuthorization`, and replace the decline at `:2004`:

```rust
// before
if force_hash && selected_strategy != StarOrderStrategy::HashPartitioned {
    return Ok(LocalStarActuationAttempt::Declined(StarOrderDeclineReason::MixedStrategyStar));
}
// after
match authorization {
    StarOrderStrategyAuthorization::AnyUniform => {}
    StarOrderStrategyAuthorization::HashOnlyCompositeProbe
        if selected_strategy != StarOrderStrategy::HashPartitioned =>
    {
        return Ok(LocalStarActuationAttempt::Declined(StarOrderDeclineReason::MixedStrategyStar));
    }
    StarOrderStrategyAuthorization::HashOnlyCompositeProbe => {}
}
```

Keep the existing declared-vs-selected strategy equality check before this match (that is the strategy-invariant gate). The boolean must be gone — the call site, not `bind_local_star_actuation`, decides whether a composite probe forces hash.

## Phase 2 — Root authorization

In `try_bind_star_order_actuation`, replace the root actuator's strategy gate with a typed authorization of `AnyUniform` (a helper such as `snowflake_root_authorization(tree) -> StarOrderStrategyAuthorization` keeps the call site readable). The root then relies on `bind_local_star_actuation`'s preflight: declared strategy uniform, selected strategy uniform, the two equal. All-broadcast proceeds; hash continues; mixed still declines `MixedStrategyStar`.

Do not let the old boolean's two responsibilities collapse into the new type. `force_hash_root` currently feeds the scratch and real root actuator calls at `:1542/:1591`, but it also controls render-trace decline suppression in the `if !force_hash_root` arms at `:1529/:1549`. Preserve that render behavior with a separate explicit predicate, e.g. `let root_is_snowflake = tree.has_branch_dimensions();`, so a failed snowflake scratch attempt does not emit duplicate flat-star decline rows. The branch-local actuator call is the literal `true` at `:1813`; Phase 3 changes that call to `HashOnlyCompositeProbe`.

## Phase 3 — Branch policy stays conservative

In `bind_snowflake_dimension`, a branch dimension with `child_dimensions.len() >= 2` continues to pass `StarOrderStrategyAuthorization::HashOnlyCompositeProbe` for that local branch star — branch stars acting as nested probes stay hash-only until a fragmenter worklist proves deeper all-broadcast branch execution. Unary branch dimensions (`bind_declared_star_children`) may still be part of a root composite broadcast build. This keeps emitted shapes inside the fragmenter's proven composite-build-with-scan-linear-probe subset and respects `BroadcastProbeNotStarLinear`.

## Phase 4 — Fragmenter scope decision (driven by Phase 0)

Run the Phase 0 fixture after Phases 1-3.

- **If it passes:** keep the production change binder-only; do not touch `fragment.rs` except tests/guards; add guards proving the composite-build fragment shape was reused.
- **If it fails specifically because nested producer dependencies are needed:** implement the **smallest worklist/`VecDeque`** in `fragment.rs` modelling broadcast sub-problems as queue entries (not stack frames); preserve the scan-build fast path and `BroadcastProbeNotStarLinear`; add a stage-order guard proving producers precede consumers via explicit `upstream_stages`. Do **not** add recursive broadcast-fragmenter traversal.

## Phase 5 — Behavior tests

- `broadcast_snowflake_root_actuates_and_executes_distributed` — the Phase 0 headline distributed-digest test (multi-worker; digest matches baseline; root actuation receipt; broadcast strategy). **Fails on `main`.**
- `broadcast_snowflake_branch_build_is_published_once` — the composite branch build is a single-partition producer consumed by each root probe task (partition-0 `ShuffleConsumer`).
- `mixed_strategy_snowflake_still_declines_actuation` — retarget `mixed_or_broadcast_snowflake_root_declines_actuation`: the **mixed** case still declines (`MixedStrategyStar`); the **uniform-broadcast** case now actuates.
- `nested_broadcast_probe_still_rejects_fragmentation` — keep the fragmenter's nested-probe rejection green.
- `broadcast_snowflake_receipts_are_edge_owned` — root receipt + 186 edge-owned branch receipts render without duplicating/losing branch evidence.
- `broadcast_snowflake_root_decline_render_state_stays_exclusive` — a declined snowflake root still renders either observed snowflake evidence or committed star receipts, never an extra flat-star decline row from the scratch preflight.
- `broadcast_snowflake_replay_ids_remain_deterministic` — replay targets stable across two binds and EXPLAIN ANALYZE.
- `rune_broadcast_snowflake_matches_sql` — Rune-authored snowflake reaches the same bind path and result digest (Rune is the primary interface).

Regression filters to keep green: `three_way_broadcast_star_digest_matches_hash_over_tcp`, `broadcast_composite_build_fragments_to_single_build_producer`, `hash_snowflake_root_reorders_on_robust_win`, `hash_snowflake_branch_emits_edge_owned_star_receipt`, `deep_snowflake_actuates_root_and_keeps_unary_branches_receiptless`.

## Phase 6 — Architecture guards

Add `adr192_*` guards in `tests/architecture/join_order.rs` (plus fragment/scheduler guards where those live):

- `adr192_retargets_adr186_hash_only_root_gate` — no raw `force_hash: bool`; root authorization is typed `StarOrderStrategyAuthorization`.
- `adr192_root_broadcast_authorization_is_strategy_invariant` — source shows declared and selected strategy must be equal before `AnyUniform` actuates.
- `adr192_root_authorization_does_not_own_render_suppression` — the old `force_hash_root` render-suppression behavior survives as an explicit snowflake-root predicate rather than being inferred from strategy authorization.
- `adr192_branch_policy_remains_explicit` — recursive multi-child branch actuation uses `HashOnlyCompositeProbe` unless the worklist fragmenter path lands.
- `adr192_fragmenter_probe_linearity_is_preserved` — `BroadcastProbeNotStarLinear` and the nested-probe rejection remain.
- `adr192_reuses_existing_composite_build_publication` — the existing single-partition producer / partition-0 consumer path is still the publication mechanism; no new task-input variant.
- `adr192_new_stage_traversal_is_worklist_not_recursion` — only if `fragment.rs` changes traversal: require `VecDeque`/worklist, forbid a new self-recursive broadcast-fragmenter descent.
- `adr192_no_storage_wendelon_or_wire_surface` — no storage, Wendelon, catalog, or wire markers.
- `adr192_no_memo_cascades_or_cost_model` — no optimizer shell / memo / mixed-strategy cost-model surface.

**Retarget ADR 186:** `adr186_root_hash_gate_commits_or_discards_branch_receipts` (`join_order.rs:1350`) currently asserts `force_hash_root = tree.has_branch_dimensions()` / `force_hash_root,` / `if !force_hash_root`; retarget to the typed authorization plus the branch-receipt commit/discard semantics (the commit-or-discard atomicity is the surviving 186 invariant, not the boolean).

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib broadcast_snowflake_root_actuates_and_executes_distributed
scripts/cargo-cgroup test -p toron --lib broadcast_snowflake_branch_build_is_published_once
scripts/cargo-cgroup test -p toron --lib mixed_strategy_snowflake_still_declines_actuation
scripts/cargo-cgroup test -p toron --lib nested_broadcast_probe_still_rejects_fragmentation
scripts/cargo-cgroup test -p toron --lib broadcast_snowflake_replay_ids_remain_deterministic
scripts/cargo-cgroup test -p toron --lib rune_broadcast_snowflake_matches_sql
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --lib broadcast
scripts/cargo-cgroup test -p toron --test architecture_test adr186
scripts/cargo-cgroup test -p toron --test architecture_test adr192
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one test filter per command.

## Risks

- **Binder-only assumption may be false.** The composite-build fragment test is not an execution proof. Phase 0 prevents shipping an authorization that fragments but fails distributed execution.
- **Over-broad broadcast unlock.** Keep `BroadcastProbeNotStarLinear` and mixed-strategy declines — this cut authorizes a root snowflake shape, not arbitrary broadcast DAGs.
- **Recursive debt.** No new recursion on the fragmenter side; existing binder recursion is tolerated because the domain is a tree and the cut's behavior is authorization.
- **Branch strategy leakage.** Keep multi-child branch actuation hash-only unless a worklist fragmenter path is proven by execution tests.
- **Receipt / replay drift.** Root/branch receipt and replay-id behavior must stay stable across rebind and EXPLAIN ANALYZE; pin with the receipt + replay gates.

## Deferred

Mixed-strategy snowflake; all-broadcast multi-child branch stars if not proven in 192; broadcast/hash cost model; full stage-graph worklist; galaxy/bridge topology; outer-join order; memo/Cascades.
