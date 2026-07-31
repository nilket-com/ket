# ADR 198: Bounded Graph Topology Actuation (Galaxy Root-Swap)

**Status:** Promoted. **Migration:** `plans/impl/198_bounded_graph_topology_actuation.md`.

ADR 194–197 took the non-tree graph topologies through observe → measure → judge. 197 emits
`would_reorder` to the cost-optimal root but never acts. ADR 198 is the **actuate** capstone: when a
recognized 2-fact galaxy's cost-optimal root differs from the FROM root, and the reorder is proven safe
and fragmentable, the binder **executes the re-rooted plan** — byte-identical results, distributed
digest-proven. The first time graph evidence changes the executed plan. Deliberately narrow: a single
2-fact galaxy root swap, gated.

## Grounding (what is reachable, what is new, what must be proven)

- **A re-rooted galaxy is a snowflake — so we reuse the engine, not rebuild it.** `sales ⋈ product ⋈
  returns` rooted at `returns` is `(returns ⋈ product) ⋈ sales` — a snowflake (returns root, product
  composite-build branch, sales leaf). 197 already proves the FROM-`returns` form
  (`galaxy_optimal_order_sql`) is a valid snowflake today. The actuated shape is one the existing
  binder/staging path and fragmenter already stage — mirroring how 193's mixed star reused the
  fragmenter rather than extending it.
- **The new capability is a bounded logical re-root.** The actuator's reorder today is *within a star
  level* (`bind_star_order_sequence` over `selected_indices`); the root is FROM-fixed
  (`extract_star_join_candidate`, `join_order.rs:535`). Re-rooting a 2-fact galaxy is a single, bounded
  inner-join reassociation: `Join(Join(F1, D), F2) → Join(Join(F2, D), F1)` where `D` is the conformed
  dimension. Semantically safe (inner joins are commutative/associative); the output ordinals shuffle
  and restore to the AST schema by the **178/186 restoration** projection the star actuator already
  uses.
- **Correctness is a theorem; fragmentation must be *proven* (the 193 lesson).** The reorder preserves
  results by inner-join algebra. But whether the *re-rooted plan fragments and digest-matches* cannot be
  assumed — it is the load-bearing gate, proven by a distributed execution fixture (impl Phase 0,
  before any actuation code ships). If the re-rooted plan hits a fragmenter cliff (strategy drift,
  nested-probe), **decline** — the 185/192 fences.

## Decision

**D1 — Actuate only the bounded 2-fact galaxy root swap, on cost + margin.** Actuate iff
`galaxy_order_recommendation` returns `action=Reordered` (cost-optimal root ≠ FROM root) AND the
preferred root beats the declared root by the soft margin (reuse the 190/191 discipline — no reorder on
a thin win). First cut: the reachable 2-fact galaxy only. Bridge actuation, n-fact galaxies, multi-hop:
deferred.

**D2 — Re-root by reassociation, then bind through the existing staging path.** A new
`try_bind_galaxy_actuation` (sibling of `try_bind_star_order_actuation`): on a recognized galaxy whose
preferred root differs, reassociate the inner-join tree to lead with the preferred fact and bind it
through the **existing** binder/staging path, which already feeds the fragmenter. No new fragmenter, no
executor change — the re-rooted snowflake is staged by the path 192/193 built.

**D3 — Restore the AST-declared schema.** Re-rooting shuffles output ordinals; reuse
`restore_declared_star_order_project` (178/186) so the output columns line up with the AST schema again.
Results are **byte-identical** to the AST-order baseline; upstream operators see no change.

**D4 — Decline on any fragmenter/strategy cliff (selected-preflight discipline, from 193).** Actuation
is gated on the re-rooted plan's own preflight proving it is valid and fragmentable — the exact 193
discipline (the selected physical order is authoritative *if its own preflight proves a valid,
fragmentable plan*). Strategy drift or a nested-probe cliff → decline, fall back to the AST order
(observe-only `would_reorder`, exactly 197). No regression: a galaxy that cannot safely re-root behaves
exactly as 197.

**D5 — Receipt and verdict reflect the *applied* re-root.** The `join_order_galaxy` receipt's order
detail changes `action=would_reorder` (197, observe) to `action=reordered; applied_root={preferred}`
when actuation fires. The 197 `join_order_graph_verdict` now judges the **applied** graph order — which
newly enables the `OverEager` / `MisRanked` classes for galaxies (the run reordered, and the
measurement can vindicate or fault that decision), not only `MissedWin`.

**D6 — Boundaries.** Toron binder plus the existing snowflake staging path. No new fragmenter
capability, no executor kernel, scheduler, storage, Wendelon, PBC, wire, or grammar change. Binder-only
plus the existing distributed staging.

## Honesty gate

`galaxy_reorders_to_cheaper_root_and_executes_distributed`: the 2-fact galaxy whose FROM-root (`sales`)
is not cost-optimal **executes distributed** with `returns` as the driving root, **digest-matches** the
AST-order baseline (the 193 proof shape), and emits a `join_order_galaxy` receipt with
`action=reordered; applied_root=returns`. Must **fail on `main`** (197 only recommends; the executed
plan is the AST order). Discriminating partners: `galaxy_in_optimal_order_does_not_reorder` (no
spurious re-root when the FROM root is already cheapest) and
`galaxy_declines_reorder_on_fragmenter_cliff` (a re-root that does not fragment falls back to AST order,
byte-identical, observe-only). Plus `galaxy_nested_restoration_preserves_output_schema` and the
soft-margin partner. Paired regression: the 194–197 galaxy/bridge/snowflake suites byte-identical where
no actuation fires.

## Consensus (transparent)

3-of-3 (Claude, Codex, Gemini). All three converged on: the first cut is a bounded 2-fact galaxy root
swap (bridge actuation deferred); the actuated shape is snowflake-shaped so the existing snowflake
actuator/fragmenter stages it (no new fragmenter/executor); the re-root is a bounded logical
reassociation; output restoration reuses the 178/186 projection; the actuation gate declines on any
invalid preflight or fragmenter cliff (the 193 selected-preflight discipline); and the distributed
digest is the load-bearing proof. All three frame the fragment feasibility as the **Phase 0 gate to be
proven, not assumed** — if the re-rooted snowflake cannot be made to fragment + digest-match, the cut
re-scopes (gate-exists-declines, or escalates to a fragmenter cut), the same falsification discipline
that corrected 193/194.

## Forward arc

`197 graph verdict → 198 bounded galaxy root-swap actuation (this) → bridge actuation → n-fact /
multi-hop graph ordering + actuation → memo only when receipts show local decisions fighting`. 198 is
the first plan-changing graph cut; it stays narrow (2-fact galaxy) and gated by a distributed proof.

## Deferred (named)

Bridge actuation; n-fact galaxy / multi-hop bridge actuation; general join-order search / memo/Cascades;
cross-shape joint actuation; catalog PK/FK classification; outer joins; durable optimizer evidence.
