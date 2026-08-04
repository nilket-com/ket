# ADR 201: Graph Verdict Accrual

**Status:** Promoted. **Migration:** `plans/impl/0201_graph_verdict_accrual.md`.

ADR 200 wired graph **decisions** (galaxy/bridge reorder/keep/decline) into the 187 decision log and
split verdict accrual to this follow-on. ADR 201 finishes the telemetry loop: graph **order verdicts**
(`Vindicated` / `MissedWin` / …) flow into the 188 sampled runtime-verdict log, so
`information_schema.optimizer_runtime_verdicts` and the 189/191 margin advisory cover galaxies/bridges,
not just stars/snowflakes. Side-effect-only — plans and EXPLAIN receipts byte-identical.

## Grounding (verified against the code)

- **The seam and what's already done.** `measured_verdicts_from_summaries` (explain.rs) — the sampled
  path's verdict producer — handles only `ObservedSnowflake` and `StarReceipts`; there are **no
  galaxy/bridge arms**. But `star_order_runtime_replay_ids` (binder.rs) **already unions galaxy + bridge
  edge targets**, so the sampled path already *collects* the graph runtime observations — no new replay
  plumbing. The render path already computes the verdict inline in `graph_order_verdict_row`, and the
  per-shape measured-cost assembly exists (`galaxy_measured_fact_cost`; the bridge inline in
  `bridge_analyze_rows`). `coordinator.rs` (the 188 sampled driver) already loops
  `measured_explain_analyze_verdicts(...)` into `record_verdict(verdict.kind, verdict.verdict)` — so a
  new `MeasuredLevelVerdict { kind: GalaxyRoot|BridgeRoot, … }` flows to `optimizer_runtime_verdicts`
  with **no coordinator change**. The mechanical work is a bounded extraction + two arms.

- **The load-bearing design question — graph-vs-tree verdict coexistence (the wrinkle that split D3).**
  A recognized galaxy/bridge *also* rides a snowflake/star tree, so `render_trace.join_order` carries a
  tree receipt that `measured_verdicts_from_summaries` turns into a tree verdict. ADR 200's decision
  accrual is **asymmetric**, and the verdict accrual must mirror it or it lies:
  - **Actuated graph:** 200 binds the re-rooted plan on the no-op recorder, so the rerooted snowflake's
    tree *decisions are suppressed* — the decision log holds only `galaxy_root`/`bridge_root` reordered.
    The render trace still carries the rerooted tree receipt, so emitting its tree *verdict* would be an
    **orphan** (a verdict with no matching decision) — exactly the unfaithfulness the 200 no-op
    suppression existed to prevent.
  - **Kept graph:** the FROM-order tree actually executes, and its tree decisions *do* accrue (the
    `bind_join` fall-through) alongside the `galaxy_root` kept decision. Here a tree verdict + a graph
    verdict legitimately **coexist** — both have matching decisions, and they judge *different* things
    (root choice vs in-tree dimension order).

- **Anchor correction.** The applied-root marker is a field on the **render trace**
  (`render_trace.applied_galaxy_root` / `applied_bridge_root`), *not* on `GalaxyOrderEvidence` /
  `BridgeOrderEvidence`. The actuation check is `render_trace.applied_galaxy_root.is_some() || …`, not
  `galaxy.applied_root` (which does not exist). The current binder treats galaxy and bridge as XOR
  shapes, so that single `graph_actuated` gate covers the one active graph path.

## Decision

**D1 — Extract `measured_graph_order_verdict`.** Lift the inline Vindicated/MissedWin/Indeterminate logic
out of `graph_order_verdict_row` into `measured_graph_order_verdict(rec: &GraphOrderRecommendation,
measured: &[(String, Option<u128>)]) -> StarOrderVerdict`; rewrite the render row builder to call it.
Render and accrual share one authoritative computation (the 200 "one source of truth" discipline).

**D2 — Add galaxy/bridge arms to `measured_verdicts_from_summaries`.** When `render_trace.galaxy_order`
is `Some`, assemble the measured costs via `galaxy_measured_fact_cost` (the galaxy_analyze_rows logic)
and push `MeasuredLevelVerdict { kind: GalaxyRoot, verdict: runtime_verdict_class_from_star(
measured_graph_order_verdict(rec, measured)) }`; same for `bridge_order` → `BridgeRoot`, reusing the
bridge measured assembly (fact-root = measured incoming `output_rows`; dim-root = `outgoing_edge_rows`).
The already-collected summaries supply the measurements; the 188 `record_verdict` loop consumes the new
rows unchanged.

**D3 — Mirror the decision asymmetry (the faithfulness gate, the cut's real content).** Compute
`graph_actuated = render_trace.applied_galaxy_root.is_some() || render_trace.applied_bridge_root.is_some()`.
Gate the **tree** verdicts (the `ObservedSnowflake` arm and the `StarReceipts` loop) on `!graph_actuated`,
then always append the graph verdict for any recognized graph. Result: the verdict log mirrors the
decision log — **graph-only for an actuated graph** (no orphan tree verdict), **tree + graph for a kept
graph** (both have matching decisions). Because the current binder treats galaxy and bridge as XOR
shapes, that single gate is the right asymmetry discriminator for the graph path.

**D4 — Boundaries / purity.** Toron binder + explain + the decision/verdict log. The coordinator path is
reused unchanged. Side-effect-only: execution plans and EXPLAIN receipts stay byte-identical to `main`.
No fragmenter, executor, scheduler, storage, Wendelon, PBC, wire, or grammar change.

## Guards reconciled (discipline #4)

`adr200_graph_decisions_accrue_to_shared_log` survives. New `adr201_*`:
`…graph_verdicts_accrue_to_sampled_runtime_log` (the `galaxy_order`/`bridge_order` arms in
`measured_verdicts_from_summaries`, `GalaxyRoot`/`BridgeRoot` reaching the verdict report),
`…verdict_computation_is_shared_between_render_and_accrual` (`graph_order_verdict_row` calls
`measured_graph_order_verdict`), `…actuated_graph_suppresses_orphan_tree_verdict` (the `graph_actuated`
gate on the tree arms), `…verdict_accrual_is_side_effect_only` (the 188 binder/coordinator-only sweep).

## Honesty gate

`graph_actuation_accrues_runtime_verdict`: under the **normal** sampled path (`sample_rate = 1`, no
EXPLAIN), a reordered galaxy and a reordered bridge each emit an `optimizer_runtime_verdicts` row
(`galaxy_root` / `bridge_root`), and the executed re-rooted plan emits **no** orphan `snowflake_root` /
`flat_star` verdict. Must **fail on `main`** (no graph arms today).

**Load-bearing discriminator — `kept_graph_emits_both_graph_and_tree_verdict`:** an already-optimal
galaxy / clean-FK bridge emits the graph verdict **and** its FROM tree's verdict (both have matching
decisions). This **fails under a blanket suppression** and passes only with the actuated-vs-kept
asymmetry — locking D3 as the real decision, not a constant. Plus
`graph_verdict_indeterminate_on_missing_measurement` and the 194–200 EXPLAIN suites byte-identical.

## Consensus (transparent)

**Topic: 3-of-3 (Claude, Gemini, Codex).** Codex's iter-0 preferred n-fact / multi-hop evidence but
conceded by iter-2 to graph verdict accrual — the explicitly-named split-off from 200 and the
prerequisite that scopes whether n-fact is worth building.

**Resolved divergence — the orphan-suppression asymmetry (D3).** Claude + Gemini (grounded 2-of-3) make
the actuated-vs-kept asymmetry the cut's core (suppress the tree verdict when a graph actuated). Codex's
iter-2 is **pure-additive** — it adds the graph arms but omits the suppression, which would emit an
orphan snowflake verdict for every actuated graph (a verdict with no matching decision, inconsistent
with 200's decision log). Codex did not *refute* the asymmetry — he omitted it — and the wrinkle is
grounded directly in 200's no-op-recorder suppression (which I implemented). Resolved with the
asymmetry. The shared verdict-extraction insight is 3-of-3; the faithfulness gate is the 2-of-3 grounded
addition, surfaced here rather than papered over.

## Forward arc

`200 graph decision accrual → 201 graph verdict accrual (this; closes the graph telemetry loop) → n-fact
/ multi-hop graph ordering + actuation (scoped by these receipts) → memo only when receipts show local
decisions fighting`.

## Deferred (named)

n-fact galaxy / multi-hop bridge ordering + actuation; general join-order search / memo / Cascades;
cross-shape joint actuation; catalog PK/FK structural classification; outer joins; durable optimizer
evidence.
