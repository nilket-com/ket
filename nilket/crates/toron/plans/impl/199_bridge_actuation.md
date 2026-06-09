# Impl Plan — ADR 199: Bridge Actuation

Pairs with `plans/adr/199_bridge_actuation.md`. Actuate the reachable 3-relation many-to-many bridge by
re-rooting to the selective dimension when the bridge is real, the **sharpened relation-rooted** cost
proves the reroot is cheaper, and the re-rooted plan still fragments + digest-matches. Sequencing mirrors
198: **Phase 0 (the distributed falsification gate) ships and passes before any actuation code.**

## Anchors (verified against the tree)

| Concern | Anchor |
|---|---|
| Bridge recognizer + observe gauge | `analyze_bridge` (`join_order.rs:1239`); `bridge_order_recommendation` (`:1408`); `BridgeLink` (`:428`) |
| Role-neutral reroot to reuse | `reroot_galaxy_join_tree` (`join_order.rs:579`) — `Join(Join(F,B),D) → Join(Join(D,B),F)` |
| Galaxy actuation seam to mirror | `try_bind_galaxy_actuation` (`join_order.rs:2050`); `set_applied_galaxy_root` (`explain.rs:146`) |
| Selected-preflight gate (193/198) | `preflight_star_order_strategy` (`join_order.rs:2697`) |
| Output restoration (178/186) | `restore_declared_star_order_project` (`join.rs:692`) |
| Edge-cost helper (galaxy) | `edge_estimate_between`; `JoinOrderEdgeEvidence.estimated_rows` |
| Recognition-no-pollute | `without_decision_recording()` (the ZST no-op recorder) |
| Bridge fixture | `bridge_join_order_catalog` (`tests.rs:14106`); `bridge_chain_order_sql` (`:14169`) |
| Distributed digest precedent | `galaxy_exec_catalog`/`galaxy_exec_storage`/`galaxy_reroot_query_digest_matches_ast_order_distributed` (`lib.rs:2777/2849/2892`); `canonical_arrow_ipc_digest` (`determinism.rs:51`) |

## Phase 0 — Distributed falsification gate (DO FIRST; no actuation code)

**Goal:** prove a hand-written re-rooted bridge with a *selective predicate on D* executes distributed and
digest-matches the AST baseline, and that the no-predicate twin shows no cost gap. If this fails the cut
re-scopes (gate-exists-declines / escalate to a fragmenter cut) — the 193/198 discipline.

1. Add `bridge_exec_catalog()` + `bridge_exec_storage()` in `lib.rs`, mirroring `galaxy_exec_*`: a
   `MultiTableStorageScan` over `orders`(F) / `order_items`(B, fans out M:N) / `products`(D), FK-complete,
   with a filterable `products` column (e.g. `category`) for the selective variant.
2. `bridge_reroot_query_digest_matches_ast_order_distributed` (sibling of the 198 test):
   - AST order: `… FROM orders JOIN order_items … JOIN products … WHERE products.category = '…'`.
   - Re-rooted order: the **hand-written** `(products ⋈ order_items) ⋈ orders` form — independent of any
     actuation code (this proves the *shape* is valid, not the actuator).
   - **Both queries project the same column order** (the digest sorts rows; column order must match — the
     198 lesson). Assert `canonical_arrow_ipc_digest(&results, SortPolicy::FullRowLex)` equal.
   - **PASS is the gate to proceed.** Also assert (binder evidence test) the *selective* fixture yields
     `outgoing.estimated_rows < incoming.estimated_rows` (reroot) while the *no-predicate* twin yields `≈`
     (kept) — proving the cost signal discriminates before any actuator exists.

## Phase 1 — D1: sharpen the bridge order recommendation (the real new content)

`join_order.rs`:
- `BridgeLink`: add `incoming_edge_rows: Option<RowCount>` + `outgoing_edge_rows: Option<RowCount>`
  (`F⋈B` / `B⋈D` join-result estimates). Populate in `analyze_bridge` from `incoming.estimated_rows` /
  `outgoing.estimated_rows` — both already in hand at the link-build site (`:1321`).
- Rewrite `bridge_order_recommendation`: `preferred_root = D` iff `outgoing_edge_rows < incoming_edge_rows`,
  else `F`; `action = Reordered` when `preferred_root != F`. **Return `None` (kept) when either estimate
  is absent.** `preferred_root`/`declared_root`/`costs` become **relation**-keyed. Route the comparison
  through `edge_estimate_between` (or a row-only twin) so `adr197_graph_order_cost_is_row_only` pins it.
- Keep it row-only (no `JoinStrategy`/byte/placement).

`explain.rs`: the bridge order row renders the **relation** root; keep the 197 fan-out **ratio** as a
derived gauge detail (single source of truth = the relation). `tests.rs`: reconcile the 197 bridge order
tests that asserted `preferred_order = "filter_first"` to assert the relation root + `action` (the
receipt sharpened — reconciled, not deleted).

## Phase 2 — D2: `try_bind_bridge_actuation` (reuse, don't rebuild)

`join_order.rs`:
- `async fn try_bind_bridge_actuation(&self, plan, trace, render_trace) -> GalaxyActuationResult` (reuse
  the 198 result enum / a shared `GraphActuationResult`): recognize the bridge on the **no-op-recorder**
  binder (`without_decision_recording()`), run `analyze_bridge` + the sharpened `bridge_order_recommendation`;
  gate on `action == Reordered` AND the D3 soft margin; then `reroot_galaxy_join_tree(plan)`,
  `bind_plan_with_trace(&rerooted)`, and `render_trace.set_applied_bridge_root(D)`. Return `NotCandidate`
  for any non-bridge / missing-estimate / thin-margin / preflight-cliff case.

`binder.rs`: wire `try_bind_bridge_actuation` into the `LogicalPlan::Join` arm **alongside**
`try_bind_galaxy_actuation`. Order: galaxy, bridge, star actuation, then the plain `bind_join`
fall-through (recognizers are mutually exclusive by their gates).

No fragmenter/executor/staging changes — the re-rooted snowflake rides the existing path.

## Phase 3/4 — D3 soft margin · D4 selected-preflight decline

- D3: gate the reroot on the 190/191 soft margin over the edge-estimate costs (reuse the binder's
  resolved `join_order_soft_margin_step`).
- D4: actuate only if the re-rooted plan's own `preflight_star_order_strategy` proves valid + fragmentable;
  strategy drift / nested-probe cliff → `NotCandidate` → fall through to the AST-order `bind_join`,
  byte-identical, observe-only `would_reorder`.

## Phase 5 — D5 receipt/verdict + guards

- `explain.rs`: `OptimizerExplainRenderTrace` gains `applied_bridge_root: Option<String>` +
  `set_applied_bridge_root` (mirror `applied_galaxy_root`). Bridge order detail flips to
  `action=reordered; applied_root={D}` when set; 197's `join_order_graph_verdict` judges the applied
  bridge order (verify OverEager/MisRanked via an `EXPLAIN ANALYZE` fixture).
- Guards (`tests/architecture/join_order.rs`):
  - **Retarget** `adr196_bridge_is_observe_only` → `adr199_bridge_recognizer_is_pure` (keep the
    `analyze_bridge` no-actuation / no-`StarReceipts` asserts + `BridgeOrderEvidence` no-`verdict`).
  - `adr197_graph_order_is_observe_only` **unchanged**; `adr197_graph_order_cost_is_row_only` += assert
    `bridge.contains("estimated_rows")`.
  - **Add**: `adr199_bridge_actuation_reuses_snowflake_machinery` (calls `reroot_galaxy_join_tree` + the
    existing staging; defines no new reassociation), `…respects_soft_margin`,
    `…recognition_is_recursion_free` (recognizes via `without_decision_recording().bind_*`; has
    `set_applied_bridge_root`; no re-entry flag), `…is_binder_only` (markers absent from
    fragment/exec/storage/pbc/wire/wendelon), `…reroot_is_bounded_three_relation`,
    `adr199_bridge_recommendation_sharpens_beyond_fanout_proxy` (the actuation cost consumes
    `estimated_rows`, not just `bridge_rows > fact_rows`).

## Test plan (discriminators that lock the design)

| test | locks | fails under |
|---|---|---|
| `bridge_reroots_to_selective_dimension_and_executes_distributed` | actuation fires + digest-safe | `main` (197 observe-only) |
| `bridge_fans_out_but_dimension_not_selective_is_kept` | **D1 costs effective rows, not fan-out** | the endpoint-row proxy |
| `bridge_already_optimal_order_kept` | decides from evidence, not a constant | a constant-reorder |
| `bridge_declines_reorder_on_fragmenter_cliff` | D4 decline → AST, byte-identical | unconditional actuation |
| `bridge_nested_restoration_preserves_output_schema` | 178/186 restoration | dropped restoration |
| `bridge_reorder_respects_soft_margin` | D3 thin-win keeps declared | no margin |
| `rune_bridge_reorder_matches_sql` | Rune parity (primary interface) | SQL-only |
| `bridge_overeager_verdict_faults_applied_root` (ANALYZE) | D5 applied-order verdict | observe-only verdict |
| `snowflake_galaxy_and_star_evidence_byte_identical` | no collateral drift | any non-bridge change |

## Verification gate (one filter per command)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib bridge_reroot_query_digest_matches_ast_order_distributed
scripts/cargo-cgroup test  -p toron --lib bridge
scripts/cargo-cgroup test  -p toron --lib galaxy
scripts/cargo-cgroup test  -p toron --lib snowflake
scripts/cargo-cgroup test  -p toron --test architecture_test adr199
scripts/cargo-cgroup test  -p toron --test architecture_test adr196
scripts/cargo-cgroup test  -p toron --test architecture_test adr197
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check ; git diff --check
```

## Risks

- **Phase 0 is the cut's life-or-death.** If the selective-D reroot does not fragment + digest-match,
  actuation is a fragmenter ADR, not a binder reorder — re-scope to decline-only.
- **Sharpened-cost correctness.** The actuation gate must never fall back to the blunt fan-out proxy when
  the `B⋈D` estimate is missing or D is non-selective — pin with the discriminator + the byte-identical
  regression.
- **No regression when declined.** A bridge that cannot safely reroot behaves exactly as 197 (observe-only).
