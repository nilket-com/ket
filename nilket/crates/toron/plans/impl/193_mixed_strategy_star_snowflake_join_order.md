# Impl Plan — ADR 193: Mixed-Strategy Star/Snowflake Join Order

Pairs with `plans/adr/193_mixed_strategy_star_snowflake_join_order.md`.

Make a mixed broadcast+hash root star/snowflake actuate **and execute distributed**. The binder gate relaxation, the `Mixed` summary, and the per-edge receipt are thin; the load-bearing work is the distributed fragmenter learning to stage a heterogeneous plan. Ordering/cost is untouched: row-only ranking remains ADR 181's preserved invariant, not a new claim that mixed network cost is solved. Execution-proof-driven: write the failing distributed test first; it fixes the exact fragmenter scope.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Fragmenter dispatch on single topmost strategy | `fragment_join_plan` — `distributed/fragment.rs:374` |
| Hash path needs bare `Exchange(scan)` inputs | `fragment_hash_join_plan` `:401`; `extract_exchange` `:1028` → `JoinInputNotExchanged` |
| Broadcast probe-chain must be all-broadcast | `rewrite_broadcast_star_probe` `:603-655` → `BroadcastProbeNotStarLinear` `:623` |
| Single-stage assumption | `MultipleExchangeBoundaries` `:286` |
| Uniformity gate + decline | `uniform_star_order_strategy`/`uniform_snowflake_order_strategy` `join_order.rs:1274/1300`; `MixedStrategyStar` decline `:322`, `:2066`, `:2090` |
| Strategy summary enum | `StarOrderStrategy` `join_order.rs:269` |
| Per-edge strategy carrier (already exists) | `SnowflakeEdgeStrategy {strategy, placement, ..}` `:356`; `SnowflakeOrderEvidence.edge_strategies` `:346` |
| Acted receipt strategy (single → +Mixed/per-edge) | `StarOrderEvidence.strategy` `:311`; render `binder/explain.rs:~1464` |
| Row-only ordering invariant (KEEP) | `order_cost` `:1351`; `rank_star_order` `:1017`; guard `adr181_reorder_is_strategy_invariant` `tests/architecture/join_order.rs:653` |
| Per-edge strategy choice (unchanged) | `choose_auto_join_strategy` `binder/join.rs:430` |
| Mixed-declines tests to retarget | `mixed_strategy_star_declines` `tests.rs:8578` (`declined_mixed_strategy_star`); snowflake mixed `:8409` |
| Distributed digest baseline (template) | `three_way_broadcast_star_digest_matches_hash_over_tcp` `lib.rs:809` |

## Phase 0 — Discriminating distributed fixture first

Add `mixed_strategy_snowflake_root_actuates_and_executes_distributed` and confirm it **fails on `main`**. Minimal shape: one over-cap dimension (→ hash) and small dims (→ broadcast), e.g. the canonical snowflake with `customers` (or a fat dim) over the broadcast cap and `regions`/`segments`/`stores` under it. Run distributed, digest-match the all-hash baseline. **Confirm the failing signal:** today the binder-built mixed plan reaches distributed `fragment_plan` and crashes (`BroadcastProbeNotStarLinear` / `JoinInputNotExchanged` / `MultipleExchangeBoundaries`) — the over-cap→hash *demotion* keeps single-broadcast plans uniform but does not rescue a genuinely mixed plan. Capture the exact crash to scope Phase 3.

## Phase 1 — Mixed classification + gate relaxation (binder, small)

In `join_order.rs`, add a classifier that runs **after** the strict `uniform_*_order_strategy` fast paths over the preflight `DecisionReceipt` vector:
- all-broadcast valid placements → `Broadcast`; all-hash → `HashPartitioned`; valid hash/broadcast mixture → `Mixed`; else decline.
- snowflake-root `Broadcast + Natural` still requires `composite_build_positions` authentication (192 fence).

Return a preflight result that includes both the summary and a per-dimension strategy fingerprint:

```rust
struct StarOrderPreflight {
    strategy: StarOrderStrategy,
    edges: Vec<JoinOrderEdgeStrategy>,
}
```

The fingerprint must be keyed by dimension label, not only by preflight position, so reordered plans can prove that the same logical edge stayed hash or broadcast. Add `Mixed` to the strategy summary (`StarOrderStrategy::Mixed` or an equivalent local summary). In `bind_local_star_actuation` (`:2057-2092`), stop returning `Declined(MixedStrategyStar)` when the classifier yields an authorized `Mixed`; authorize mixed actuation only when **declared and selected both classify as `Mixed` and their per-dimension strategy fingerprints match**. Placement may differ only where the ADR 192 composite-build fence authenticates `Broadcast + Natural`. Keep 192's `CompositeBuildSnowflakeRoot` selected-broadcast upgrade broadcast-specific. Ordering (`selected_indices` by `estimated_rows`) and `order_cost` unchanged.

Branch authorization (`HashOnlyCompositeProbe`) is unchanged — branch-local mixed stays declined.

## Phase 2 — Per-edge `Mixed` receipt (binder/render, small)

Carry the per-edge strategy/placement list on the acted `StarOrderEvidence`. Prefer a generic carrier such as `JoinOrderEdgeStrategy`; do not make acted star evidence depend on a snowflake-only type name just because `SnowflakeOrderEvidence.edge_strategies` already exists. EXPLAIN renders `strategy=mixed` + `strategies=[…]`. Update `binder/explain.rs` labels; leave `decision_log` unchanged unless implementation actually adds strategy labels to accumulated decision rows. Render/telemetry only — no executor/storage surface.

## Phase 3 — Fragmenter heterogeneous staging (the ADR; worklist; driven by Phase 0)

Run Phase 0 after Phases 1-2.

- **If the minimal mixed plan already fragments+executes:** 193 is binder+receipt only; add guards proving the fragmenter shape was reused. (Unlikely per grounding.)
- **If it crashes (expected):** generalize fragmentation so a stage input may be a **fragmented upstream subtree**, not only a bare `Exchange(scan)`:
  - when a hash join's input is itself a `Join`, fragment it into its own stage and consume its published output (relax `extract_exchange`/`JoinInputNotExchanged` for that case);
  - when a broadcast probe-chain input is a hash-shuffled subtree, fragment it as an upstream stage instead of `BroadcastProbeNotStarLinear`;
  - lift `MultipleExchangeBoundaries` for the mixed shape;
  - implement the new stage-dependency traversal as an explicit **worklist/`VecDeque`**, never recursive descent; guard that each upstream producer precedes its consumer via `upstream_stages`;
  - preserve uniform hash and uniform broadcast fragmentation **byte-identically** — the mixed path is strictly additive.

Scope to the **minimal** shape the headline needs; defer arbitrary bushy mixed plans. No executor-kernel change (the in-process executor already evaluates nested mixed trees).

## Phase 4 — Tests

- `mixed_strategy_snowflake_root_actuates_and_executes_distributed` — Phase 0 headline; fails on `main`; root `join_order` receipt `strategy=mixed`; digest-matches all-hash baseline.
- `mixed_strategy_flat_star_actuates_and_executes_distributed` — flat star, one over-cap + one under-cap dim; mixed receipt + distributed digest match. (Support if the snowflake headline already forces the full fragmenter scope.)
- Retarget `mixed_strategy_star_declines` (`tests.rs:8578`) and the snowflake mixed decline (`:8409`): the baseline mixed case **actuates**; keep a still-deferred shape (branch-local mixed / bushy / non-scan-linear) that **still declines** (`MixedStrategyStar`), pinning the baseline boundary.
- `mixed_strategy_receipt_renders_per_edge_strategies`; `mixed_strategy_keeps_row_only_ordering`; `mixed_strategy_declared_selected_must_both_be_mixed`; `mixed_strategy_requires_declared_selected_edge_fingerprint_match`; `mixed_branch_still_declines_or_binds_declared`.
- `mixed_snowflake_fragment_producers_precede_consumers` — worklist stage-ordering assertion.
- Regression (byte-identical): `uniform_hash_star_unchanged`, `uniform_broadcast_star_unchanged`, `broadcast_snowflake_root_actuates_and_executes_distributed` still passes, `nested_broadcast_probe_still_rejects_unsupported_shape`.
- `rune_mixed_strategy_snowflake_matches_sql` — Rune parity (primary interface).

## Phase 5 — Architecture guards

- `adr193_strategy_summary_has_mixed_state` — acted receipts can represent `Mixed`.
- `adr193_receipt_carries_per_edge_strategies` — acted star evidence owns per-edge strategy/placement.
- `adr193_ordering_remains_row_only` — `order_cost`/`rank_star_order`/`classify_star_order_action` still don't read `JoinStrategy` (preserves `adr181_reorder_is_strategy_invariant`, does not invert it).
- `adr193_mixed_classification_uses_preflight_decisions` — mixed is derived from the preflight decisions that prove executability.
- `adr193_mixed_authorization_uses_edge_strategy_fingerprint` — mixed root authorization compares declared/selected per-dimension strategy fingerprints, not only the `Mixed` summary.
- `adr193_snowflake_mixed_preserves_composite_authentication` — snowflake-root mixed still checks `composite_build_positions`.
- `adr193_branch_mixed_strategy_stays_declined` — branch composites remain hash-only.
- `adr193_fragmenter_mixed_subtree_staging_is_worklist_driven` — new mixed-stage traversal uses a `VecDeque`/worklist + explicit `upstream_stages`, no new recursive fragmenter descent.
- `adr193_uniform_fragmentation_paths_preserved` — uniform hash/broadcast fragmentation unchanged.
- `adr193_no_topology_memo_or_planproperties`; `adr193_no_storage_wendelon_pbc_wire_or_executor_kernel_change`.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib mixed_strategy_snowflake_root_actuates_and_executes_distributed
scripts/cargo-cgroup test -p toron --lib mixed_strategy_flat_star_actuates_and_executes_distributed
scripts/cargo-cgroup test -p toron --lib mixed_strategy
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --lib broadcast
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr181
scripts/cargo-cgroup test -p toron --test architecture_test adr192
scripts/cargo-cgroup test -p toron --test architecture_test adr193
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one test filter per command.

## Risks

- **Binder-only regresses.** Relaxing the gate without the fragmenter turns a clean decline into actuate-then-crash. Phases 1-2 must not ship without the Phase 3 capability proven by the headline.
- **Fragmenter blast radius.** The mixed path must be strictly additive — uniform broadcast/hash fragmentation byte-identical. Pin with the two byte-identical regressions.
- **181 invariant inversion.** Do not let `order_cost`/ranking read strategy; the `adr181_reorder_is_strategy_invariant` guard must stay green.
- **Demotion masking.** If a mixed plan ever demotes to all-hash, the headline must assert the broadcast edges survive (per-edge receipt + no all-hash replay), else it passes for the wrong reason.
- **Recursive debt.** New stage traversal is a worklist (galaxy/bridge inherits it).

## Deferred

Strategy-aware cost model; cost-based strategy choice; branch-local mixed actuation; arbitrary bushy mixed plans; galaxy/bridge topology; outer joins; memo/Cascades; durable/persisted optimizer evidence.
