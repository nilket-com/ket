# Impl Plan — ADR 193: Mixed-Strategy Star/Snowflake Join Order

Pairs with `plans/adr/193_mixed_strategy_star_snowflake_join_order.md`.

Make a mixed broadcast+hash root star/snowflake actuate **and execute distributed**. As built, the entire cut is **binder-only**: the `Mixed` classification, the selected-preflight gate relaxation, and the per-edge receipt. Implementation **falsified the planned fragmenter work** — the binder's cost-optimal order is broadcast-first/hash-last, a left-deep shape the existing `fragment_join_plan` already stages, so the distributed headline passes with zero fragmenter change. Ordering/cost is untouched: row-only ranking remains ADR 181's preserved invariant, not a new claim that mixed network cost is solved.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Per-edge strategy choice (unchanged) | `choose_auto_join_strategy` `binder/join.rs:430` — broadcast requires a scan-linear probe (`right_viable = … filter(\|_\| left_input.probe_scan_linear)`) |
| Existing fragmenter (reused unchanged) | `fragment_join_plan` `distributed/fragment.rs`; broadcast-first/hash-last fragments through the existing flat path |
| Uniformity gate + decline | `uniform_star_order_strategy`/`uniform_snowflake_order_strategy`; `MixedStrategyStar` decline |
| Strategy summary enum (+`Mixed`) | `StarOrderStrategy` `join_order.rs:269` |
| Acted receipt per-edge carrier (new) | `StarOrderEvidence.edge_strategies` `join_order.rs:319`; render `binder/explain.rs:1153` |
| Selected-preflight gate | `bind_local_star_actuation` `join_order.rs:2052`; `preflight_star_order_strategy` `:2278`; `order_strategy_fingerprint` `:1356` |
| Row-only ordering invariant (KEEP) | `order_cost`; `rank_star_order`; guard `adr181_reorder_is_strategy_invariant` |
| Distributed digest baseline (template) | `three_way_broadcast_star_digest_matches_hash_over_tcp` `lib.rs` |

## Phase 0 — Discriminating distributed fixture first

`mixed_strategy_star_actuates_and_executes_distributed` (`lib.rs`): a flat mixed star — `orders ⋈ small` (under-cap → broadcast) and `orders ⋈ big` (over-cap → hash), broadcast-first declared order. Run distributed, digest-match the all-hash baseline (built with `force_hash_join_ids`). **The failing-on-`main` signal is a decline, not a crash:** pre-193 the actuator collapses the per-edge strategies and returns `Declined(MixedStrategyStar)` — it never reaches the fragmenter. (The promoted plan predicted a `fragment_plan` crash; implementation falsified that — see Phase 3.)

## Phase 1 — Mixed classification + selected-preflight gate (binder; the load-bearing cut)

In `join_order.rs`:
- Add `StarOrderStrategy::Mixed`.
- `preflight_star_order_strategy` returns `PreflightStrategy { summary, fingerprint }`. `order_strategy_fingerprint` validates placements (`(Broadcast, SwappedRightBuild)` valid; `(Broadcast, Natural)` valid only at an authenticated composite position; `(HashPartitioned, Natural)` valid; else `None` → decline). The strict `uniform_*_order_strategy` recognizers stay the fast path; a valid non-uniform set is `.unwrap_or(StarOrderStrategy::Mixed)`.
- In `bind_local_star_actuation`, **the selected physical order is authoritative.** Typed authorization:
  - `AnyUniform` (flat-star root): `selected.summary == Mixed || fingerprint_match`.
  - `CompositeBuildSnowflakeRoot` (snowflake root): `selected.summary == Broadcast || selected.summary == Mixed || fingerprint_match`.
  - `HashOnlyCompositeProbe` (branch): `selected.summary == HashPartitioned && fingerprint_match` — branch-local mixed stays declined.

  The fingerprint is **not** an authorization gate. The cost reorder legitimately flips a dim's strategy (hash→broadcast once it moves ahead of the hash join), so a declared==selected match would block exactly this reorder. `fingerprint_match` is demoted to a soft disjunct (uniform stability) and to receipt honesty. An invalid selected placement (`order_strategy_fingerprint` → `None`) still declines `MixedStrategyStar` before any tier.

Ordering (`selected_indices` by `estimated_rows`) and `order_cost` unchanged.

## Phase 2 — Per-edge `Mixed` receipt (binder/render)

Add a generic `edge_strategies: Vec<(String, JoinStrategy, JoinPhysicalPlacement)>` to `StarOrderEvidence` (not snowflake-named). `bind_local_star_actuation` populates it from the selected fingerprint in applied order; threaded through `recognized_star_order_outcome`. EXPLAIN renders `strategy=mixed; strategies=[c:hash_partitioned/natural,s:broadcast/swapped_right_build]` (`binder/explain.rs`). Render/telemetry only.

## Phase 3 — Fragmenter: reused unchanged (planned cliff falsified)

The promoted plan scoped heterogeneous fragmenter staging as "the ADR." Implementation proved it unnecessary:
- `choose_auto_join_strategy` only broadcasts a scan-linear probe, so a hash join earlier in the order forces later edges to hash. The row-only cost optimum therefore emits **broadcast-first / hash-last** — `Join(Hash, probe=Join(Broadcast, …, scan))` — a clean left-deep shape.
- That shape is exactly what `fragment_join_plan` already stages: the broadcast-first probe subtree fragments through the existing broadcast path, leaving the topmost hash join with bare `Exchange(scan)` inputs.
- An exploratory fragmenter change broke `overestimated_inner_join_broadcast_over_cap_replays_as_hash_and_matches_digest` (a nested broadcast-in-hash-input already runs flat with replay) and was reverted. `distributed/fragment.rs` is **unchanged**.

Guard pins it: `adr193_mixed_classification_is_binder_only_no_fragmenter_change` asserts `fragment.rs` never learns `StarOrderStrategy`/`Mixed`. A heterogeneous-stage fragmenter is deferred until a discriminating execution fixture forces it.

## Phase 4 — Tests (as built)

- `mixed_strategy_star_actuates_and_executes_distributed` (`lib.rs`) — Phase 0 headline; declines on `main`; root `join_order` receipt `strategy=mixed`; trace carries both Broadcast and HashPartitioned; distributed digest-matches the `force_hash_join_ids` baseline.
- `mixed_strategy_snowflake_root_actuates_with_mixed_receipt` (`tests.rs`, inverted from `mixed_strategy_snowflake_still_declines_actuation`) — the reordering mixed snowflake root actuates; `strategy=mixed` + per-edge `strategies=[…]` with a broadcast and a hash edge; no observe-only verdict. This is the case where selected ≠ declared, so **selected-preflight authority** (not fingerprint match) is what authorizes it.
- Observe-only retargets: every valid scan-linear root now actuates, so the 13 observation tests (`snowflake_order_evidence_is_observational`, `…_is_explain_gated`, `root_decline_keeps_observed_snowflake_state`, the margin/Rune/runtime/verdict tests, etc.) were retargeted to a **non-scan-linear probe** (`observed_snowflake_order_sql` — fact table via `LIMIT`; Rune via `.sort(...)`). A non-scan-linear probe forces all edges to hash, so `snowflake_evidence_records_per_edge_strategy_without_gating` asserts the observed all-hash recording with `scan_linear=false` (the broadcast-edge receipt is proven on the actuated path).
- Decision-log retargets: `optimizer_decisions_accrue_across_queries` and `optimizer_decisions_record_committed_not_scratch` updated — the mixed snowflake actuates (`snowflake_root/kept_optimal` + `snowflake_branch/reordered`); the declined-root case is the non-scan-linear probe (commit-discard atomicity at the decision-log level).

## Phase 5 — Architecture guards (as built)

- `adr193_selected_preflight_is_the_authorization_gate` — selected-Mixed actuates; fingerprint is a soft disjunct (`Mixed || fingerprint_match`), not a precondition; invalid selected placement declines.
- `adr193_mixed_classification_is_binder_only_no_fragmenter_change` — `distributed/fragment.rs` never learns `StarOrderStrategy`/`Mixed`; the binder owns `StarOrderStrategy::Mixed`.
- `adr193_reorder_cost_stays_row_only` — `order_cost` weighs only `estimated_rows`; no strategy/placement/byte terms.
- `adr193_branch_mixed_stays_declined` — `HashOnlyCompositeProbe` requires uniform hash; no Broadcast/Mixed acceptance.
- `adr193_receipt_carries_per_edge_strategy_detail` — generic `edge_strategies` carrier; explain renders `strategy={}; strategies=[{}]`.
- Refactor-travels updates: `adr181_uniform_strategy_recognizer` (now 3 variants, +`Mixed`), `adr181_uniform_strategy_preflights_declared_and_selected` (renamed `declared`/`selected` locals, fingerprint-match expresses the strict flat-star equality), `adr181_reuses_178_restore_and_179_180_net` (format string gained `strategies=[{}]`), `adr192_composite_natural_broadcast_is_authenticated` + `adr192_selected_broadcast_upgrade_is_snowflake_specific` (post-refactor strings; snowflake-specificity preserved).

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib mixed_strategy_star_actuates_and_executes_distributed
scripts/cargo-cgroup test -p toron --lib mixed
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --lib optimizer_decisions
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr181
scripts/cargo-cgroup test -p toron --test architecture_test adr192
scripts/cargo-cgroup test -p toron --test architecture_test adr193
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one test filter per command.

## Risks (as resolved)

- **Binder-only regresses.** *Falsified.* The binder emits broadcast-first/hash-last, which fragments through the existing path — no actuate-then-crash. Proven by the passing distributed headline.
- **181 invariant inversion.** `order_cost`/ranking never read strategy; `adr181_reorder_is_strategy_invariant` + `adr193_reorder_cost_stays_row_only` stay green.
- **Demotion masking.** The headline asserts both Broadcast and HashPartitioned survive in the trace and digest-matches the all-hash baseline, so it cannot pass on an all-hash demotion.
- **Observe-only collapse.** Post-193 every valid scan-linear root actuates; the observe-only path is genuinely reachable only for shapes outside the actuator's domain (non-scan-linear probe, branch-local mixed, bushy), and is pinned by the non-scan-linear-probe fixtures.

## Deferred

Strategy-aware cost model; cost-based strategy choice; branch-local mixed actuation; arbitrary bushy mixed plans; heterogeneous-stage fragmenter; galaxy/bridge topology; outer joins; memo/Cascades; durable/persisted optimizer evidence.
