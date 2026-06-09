# ADR 197: Galaxy / Bridge Cost Model and Ordering

**Status:** Promoted. **Migration:** `plans/impl/197_graph_order_evidence.md`.

ADR 194–196 made galaxy and bridge graphs visible and measured. They are recognized *as snowflakes*,
and the snowflake actuator orders that tree — but its root is fixed by FROM order
(`extract_star_join_candidate`), and it only reorders dimensions *within* a star level. It never
chooses **which fact roots the conformed dimension**, the one graph-shaped ordering decision. ADR 197
adds a shape-aware cost model that computes the cost-optimal graph order and surfaces it as observe-only
evidence + a measured order verdict — the **judge** rung of the graph arc. No actuation.

## Grounding (the new decision is real, reachable, discriminating — probed)

Probed on the existing galaxy fixture `sales ⋈ product ⋈ returns`: `GalaxyEdgeEvidence` carries
`sales→product est = 100000` and `product→returns est = 5000`. **FROM-order roots `sales`** (the larger
fact-to-dimension join); the **cost-optimal root is `returns`** (its `returns ⋈ product` join is the
smaller intermediate). FROM-root ≠ cost-optimal root, and the discriminating signal is **already
captured** on the galaxy edge estimates — no new substrate, no re-bind. This is a genuine new decision,
not a re-derivation of the snowflake verdict.

## Decision

A shape-aware cost model over the recognized galaxy/bridge evidence: a **preferred graph order**
recommendation (observe-only) and a **measured order verdict** (the proof). Reuses the tree cost/verdict
seams; no actuation.

**D1 — Galaxy order cost = root the most-selective fact-to-conformed-dimension join.** For conformed
dimension `D` shared by facts `F1…Fn`, the cost of rooting `Fi` is its `Fi ⋈ D` edge estimate (on
`GalaxyEdgeEvidence`). `preferred_root = argmin`; `declared_root` = the fact that is never built as a
dimension (the FROM root). `action = would_reorder` if they differ, else `kept_optimal`. The graph
analog of the row-based star ordering heuristic (ADR 177/181), applied to the shared-dimension shape.
First cut: the reachable 2-fact galaxy.

**D2 — Bridge order cost = don't materialize the M:N fan-out before the selective dimension.** For
`F ⋈ B ⋈ D`, the bridge candidate compares early bridge fan-out (196) against delaying the many-to-many
expansion until the selective `D` has been applied; the cost model prefers the order that avoids
materializing the fan-out early. The candidate set is the small shape-implied set in the bridge
receipt, not a search. First cut: the single 3-relation bridge.

**D3 — Order evidence row (observe-only).** Extend the `join_order_galaxy` / `join_order_bridge` cockpit
rows with `declared_order`, `preferred_order`, per-candidate `estimated_cost`, and a non-acting
`action` (`kept_optimal` | `would_reorder`), mirroring the star plan evidence. The receipt *says* what
the model prefers; it does not rewrite the plan. Executed plan and results stay byte-identical.

**D4 — Measured order verdict (the proof, reusing 195/196).** Under EXPLAIN ANALYZE, substitute the
measured fact-to-dimension cardinalities (195/196 runtime) into the graph scorer and emit a single
unified `join_order_graph_verdict` row with the 180/184 verdict vocabulary
(`Vindicated` | `MissedWin` | `OverEager` | `Indeterminate`) and a recommendation string
(`WOULD_REORDER_ROOT_FACT`, etc.). `Indeterminate` when runtime rows are missing — no verdict without
measurement. Reuses `measured_star_order_verdict`'s comparison shape and `StarOrderVerdict`.

**D5 — No actuator; shape-aware; bounded.** The model enumerates only the small candidate set the
recognized shape admits (the root-fact choices of a 2-fact galaxy; the bridge join sequence) — **not**
memo/Cascades/general join reordering (deferred). Cost stays row-only (cf. ADR 181), framed as a
selectivity heuristic with the measured verdict as the reality check, *not* a global optimality claim.
No actuation (the next rung). Binder/explain/analyze only; reuses the star-order verdict vocabulary
and measured-verdict shape (`StarOrderAction` / `StarOrderVerdict`), while the graph scorer itself is
new; no fragmenter, executor, storage, Wendelon, PBC, wire, or grammar change.

## Honesty gate

`galaxy_order_evidence_recommends_cheaper_root`: the 2-fact galaxy whose FROM-root (`sales`, 100000) is
not cost-optimal emits a galaxy order receipt with `action=would_reorder`, `preferred_root=returns`, and
per-fact `estimated_cost`, while results + physical plan stay byte-identical. Must **fail on `main`**.
Discriminating partner `galaxy_already_optimal_order_kept`: a fixture whose FROM-root *is* the smallest
fact-to-dimension join emits `kept_optimal` — proving the model decides from evidence, not a constant.
Verdict gates: `galaxy_verdict_recommends_reordering_suboptimal_root_fact` (EXPLAIN ANALYZE →
`MissedWin` + `WOULD_REORDER_ROOT_FACT`), `graph_verdict_indeterminate_on_missing_measurement`,
`graph_verdicts_are_observational_only`. Bridge analog:
`bridge_order_evidence_recommends_delaying_massive_fanout`. Paired regression:
`bridge_order_snowflake_galaxy_and_star_observe_byte_identical`.

## Consensus (transparent)

3-of-3 (Claude, Codex, Gemini). All three converged on: the cost model consumes 194–196 evidence (no
new substrate); galaxy cost = root the smallest fact-to-dimension edge estimate; bridge cost = delay the
fan-out past the selective dimension; the order receipt carries `declared/preferred/estimated_cost/
action`; a unified `join_order_graph_verdict` reuses the 180/184 verdict vocabulary
(`Vindicated`/`MissedWin`/`OverEager`/`Indeterminate`) over 195/196 runtime; strictly shape-aware (not
a search engine / memo / Cascades); observe-only with byte-identical plans. The load-bearing decision —
which fact roots the conformed dimension, outside the FROM-fixed snowflake actuator — was probed
against the real substrate before promotion (`sales` FROM-root vs `returns` cost-optimal root).

## Forward arc

`196 bridge evidence → 197 graph order evidence + measured verdict (this; observe-only) → bounded graph
topology actuation (reorder the recognized graph when a costed choice beats main) → memo only when
receipts show local decisions fighting`. 197 makes the graph CBO's preference visible and proven by
measurement before anything is steered.

## Deferred (named)

Graph topology actuation; n-fact galaxy / multi-hop bridge ordering; general join-order search /
memo/Cascades; cross-shape joint ordering; catalog PK/FK classification; outer joins; durable optimizer
evidence.
