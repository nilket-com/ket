# Impl Plan — ADR 198: Bounded Graph Topology Actuation (Galaxy Root-Swap)

Pairs with `plans/adr/198_bounded_graph_topology_actuation.md`.

Actuate a 2-fact galaxy root-swap by reassociating the inner-join tree to lead with the cost-optimal
fact and binding the re-rooted **snowflake** through the existing actuator + fragmenter. **Phase 0 is
the distributed digest fixture — it gates everything:** no actuation code ships until the re-rooted
plan is proven to fragment and digest-match. This is the 193 falsification discipline, sharpest here.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Galaxy recognizer + cost model (the trigger) | `analyze_galaxy`; `galaxy_order_recommendation` (`action=Reordered`, `preferred_root`) `join_order.rs` |
| Star actuation seam to mirror (reorder + restore) | `try_bind_star_order_actuation`; `bind_star_order_sequence`; `restore_declared_star_order_project` (178/186) |
| Root is FROM-fixed (the gap to fill) | `extract_star_join_candidate` `join_order.rs:535` |
| Selected-preflight / fragment gate discipline (193) | `bind_local_star_actuation` preflight; `fragment_join_plan` `distributed/fragment.rs` |
| Soft-margin discipline (190/191) | `classify_star_order_action` margin; `auto_broadcast_config` |
| Distributed digest proof template (193) | `run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal` `lib.rs`; `mixed_strategy_star_actuates_and_executes_distributed` |
| Galaxy execution fixture (new) | model on `snowflake_order_execution_fixture` / `write_star_order_table` / storage batches; the hand-written re-rooted query from 197 is `galaxy_optimal_order_sql` |

## Phase 0 — Distributed re-root proof (DO FIRST; the gate)

Build a galaxy **execution** fixture (catalog + storage for `sales`/`product`/`returns`, `sales` large
and `returns` small so the cost-optimal root = `returns`). Confirm on `main`:
- the AST-order query (`FROM sales …`) and the hand-written re-rooted query (`galaxy_optimal_order_sql`,
  `FROM returns …`) **execute distributed and digest-match** — proving the re-rooted plan is correct +
  fragmentable today as a plain snowflake, independent of actuation.
- the AST-order query does **not** re-root (197: only `would_reorder`).

If the re-rooted query does not fragment/digest-match, **STOP** — actuation is a fragmenter cut, not a
binder reorder; re-scope to "gate exists, declines" or escalate. (This is the falsification step that
saved 193/194. Everything below assumes Phase 0 passes.)

## Phase 1 — Bounded logical re-root (binder)

`reroot_galaxy_join_tree(plan, preferred_root, conformed_dim) -> Option<LogicalPlan>`: for the
recognized 2-fact galaxy `Join(Join(F1, D), F2)` (or its mirror), produce `Join(Join(preferred, D),
other)` by inner-join reassociation/commutation. Bounded to the 2-fact shape; returns `None` for any
shape it cannot cleanly reassociate (→ decline, AST order).

## Phase 2 — `try_bind_galaxy_actuation` (binder; reuse snowflake downstream)

A sibling of `try_bind_star_order_actuation`, invoked at the same join-order decision point:
- recognize the galaxy (`analyze_galaxy`) and compute `galaxy_order_recommendation`;
- if `action == Reordered` AND the preferred-vs-declared cost clears the soft margin (190/191):
  - `reroot_galaxy_join_tree` → the re-rooted logical plan;
  - bind it through the **existing** binder/staging path on the re-rooted tree so it stages via the
    existing fragmenter;
  - **preflight gate (193):** if the re-rooted plan's own preflight is invalid or it would not fragment,
    **decline** → AST order;
  - restore output ordinals to the AST schema (`restore_declared_star_order_project`);
- else: AST order (observe-only, exactly 197).

No new fragmenter, no executor change. Reuses the 192/193 actuator + fragmenter on the re-rooted shape.

## Phase 3 — Receipt + verdict reflect the applied re-root

The `join_order_galaxy` order detail shows `action=reordered; applied_root={preferred}` when actuation
fires (vs `would_reorder`). The 197 `join_order_graph_verdict` now judges the applied order — newly
exercising `OverEager` / `MisRanked` (the run reordered; the measurement vindicates or faults it).

## Phase 4 — Tests

- `galaxy_reorders_to_cheaper_root_and_executes_distributed` — headline; re-rooted execution
  digest-matches AST baseline; receipt `action=reordered; applied_root=returns`; fails on `main`.
- `galaxy_in_optimal_order_does_not_reorder` — FROM root already cheapest → no re-root, AST order.
- `galaxy_declines_reorder_on_fragmenter_cliff` / `galaxy_reorder_declines_on_strategy_preflight_failure`
  — a re-root that cannot stage falls back to AST order, byte-identical, observe-only `would_reorder`.
- `galaxy_reorder_results_byte_identical` + `galaxy_nested_restoration_preserves_output_schema` — AST vs
  actuated results + schema identical.
- `galaxy_reorder_respects_soft_margin` — a thin win keeps the declared order.
- `bridge_actuation_remains_deferred`; the 194–197 suites green where no actuation fires.
- `rune_galaxy_reorder_matches_sql` — Rune parity.

## Phase 5 — Architecture guards

- `adr198_galaxy_actuation_reuses_snowflake_machinery` — no new fragmenter; `distributed/fragment.rs`
  unchanged; the re-rooted plan binds through `try_bind_star_order_actuation`.
- `adr198_galaxy_reroot_is_bounded_two_fact` — `reroot_galaxy_join_tree` handles only the 2-fact galaxy;
  returns `None` otherwise (no general search / memo / Cascades).
- `adr198_galaxy_actuation_gated_on_preflight` — re-root actuates only when its own preflight proves a
  valid, fragmentable plan; declines to AST order otherwise (193 discipline).
- `adr198_galaxy_reorder_restores_ast_schema` — uses `restore_declared_star_order_project`; results
  byte-identical.
- `adr198_galaxy_actuation_respects_soft_margin` — reuses 190/191 margin; no reorder on a thin win.
- `adr198_no_change_to_executor_or_fragmenter` — no executor/scheduler/storage/Wendelon/PBC/wire/grammar.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib galaxy_reorders_to_cheaper_root_and_executes_distributed
scripts/cargo-cgroup test -p toron --lib galaxy
scripts/cargo-cgroup test -p toron --lib bridge
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr198
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one test filter per command.

## Risks

- **Phase 0 is the cut's life-or-death.** If the re-rooted snowflake does not fragment/digest-match,
  actuation is a fragmenter ADR, not a binder reorder — stop and re-scope (the 193/194 falsification
  discipline). Everything downstream assumes Phase 0 passes.
- **Reassociation correctness.** The logical re-root must produce a plan that binds + executes
  identically; pin with `galaxy_reorder_results_byte_identical`.
- **No regression when declined.** A galaxy that cannot safely re-root must behave exactly as 197
  (observe-only); pin with the byte-identical regression + the cliff-decline test.

## Deferred

Bridge actuation; n-fact / multi-hop graph actuation; general join-order search / memo/Cascades;
cross-shape joint actuation; catalog PK/FK; outer joins; durable evidence.
