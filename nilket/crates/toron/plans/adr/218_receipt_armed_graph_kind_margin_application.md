# Toron ADR 218: Receipt-Armed Graph-Kind Margin Application

**Status:** Promoted. **Migration:** `plans/impl/218_receipt_armed_graph_kind_margin_application.md`.
**Category:** Toron optimizer actuation — the **first receipt-gated behavior
rotation**, the first consumer of the ADR 217 rotation cockpit that changes a
plan. No durable evidence-format change, no new counter/key/table/`SystemTableKind`,
no new config mode, no wire / executor / fragmenter / Wendelon change.

## Why this cut exists

207→216 made the optimizer's evidence durable; 217 turned it into a checkable
rotation cockpit (`information_schema.optimizer_receipt_gates`) but rotated
nothing. 218 takes the cockpit's first behavior-ready actuator candidate —
`graph_margin_divergence` — and lets it **arm** a real behavior: graph join-order
soft-margin application becomes **per graph kind** (`GalaxyRoot`, `BridgeRoot`,
`BridgeChainRoot`) instead of one uniform graph-family step, but **only when the
217 gate is `actionable` at runtime**. Otherwise it is byte-identical to ADR 215.

This is "receipt-armed": the actuator is **production-dormant** (cannot fire
without an actionable receipt) yet **test-exercised** (a seeded
two-divergent-kinds fixture drives the armed path every CI run) — exactly the 215
`ApplyAdvisory` precedent, opt-in and evidence-derived. Self-arming dissolves the
planning-time cold-ledger problem (the council need not hold production evidence
to ship a dormant, receipt-gated actuator) while honoring 217's discipline by
construction: the behavior rotates iff the receipt justifies it.

## Grounding (verified)

- `ApplyAdvisory` is the existing opt-in application mode; `Manual` is the
  byte-identical base (join_order.rs:305). 218 adds **no new mode/bool/flag**.
- The 217 gate already exists as reusable pieces: `graph_margin_divergence_gate`
  (join_order.rs:465), `graph_kind_gate_signals` (:505), `GraphKindGateSignal {
  kind, recommendation, recommended_step, sample_size }` (:429), floor
  `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE = 20`.
- **The actuation seam (Codex iter-2, grounded — this corrects the recognizer
  thread-point earlier drafts proposed):** the keep/actuate margin is read in
  `try_bind_graph_actuation` at join_order.rs:3017 —
  `let margin = self.graph_join_order_soft_margin_step.margin();`. That single
  line is the behavioral application point; `shape.decision_kind()` (:2842) names
  the graph kind there. If this line still reads the family step, 218 has not
  applied graph-kind margins, however much the resolver diverges.
- `Binder` carries the scalar `graph_join_order_soft_margin_step` (binder.rs:457),
  threaded through the clone/no-op binders (:483/499/514); a single-graph-step
  resolver `effective_graph_join_order_soft_margin_step` exists (:332). These
  become the typed bundle + resolver. No durable surface is touched.

## Decisions (promotion-locked)

**D1 — Receipt-armed, graph-only, inside `ApplyAdvisory`.** Under `ApplyAdvisory`,
graph-kind steps engage **iff** the shared `graph_margin_divergence` gate is
`actionable`; otherwise every graph kind uses the ADR 215 graph-family step.
`Manual` is byte-identical. Tree-family application is byte-identical.

**D2 — Existing opt-in only.** No new config enum variant, boolean, env flag, or
session switch.

**D3 — One shared typed gate result (the honesty pin — Codex iter-2, adopted).**
Promote the gate to a typed `GraphMarginDivergenceGate { status, ready:
Vec<GraphKindGateSignal> }` consumed by **both** the 217 `optimizer_receipt_gates`
readout row builder **and** the 218 actuator. Sharing only the status enum is
insufficient — the actuator needs the same ready set + recommended steps, or the
threshold can drift between what the cockpit reports and what the planner applies.
One predicate, one ready set, one set of recommended steps.

**D4 — Typed graph-step bundle.** Replace the scalar graph step with
`EffectiveGraphJoinOrderSoftMarginSteps { family, galaxy, bridge, bridge_chain }`
plus `step_for(kind: OptimizerDecisionKind)` (non-graph kinds rejected by
type/debug-assert, not silently routed).

**D5 — Global arming, per-kind readiness.** When the gate is `actionable`: each
**sample-ready** graph kind uses its own recommended step; a kind **without**
sample-ready evidence falls back to the graph-family step (so two diverging kinds
never force a thin-evidence third onto a noisy per-kind step).

**D6 — Actuation seam is `try_bind_graph_actuation` via `shape.decision_kind()`.**
Replace the family-scalar read at join_order.rs:3017 with
`self.graph_join_order_soft_margin_steps.step_for(shape.decision_kind()).margin()`.
This is the load-bearing behavioral line; a resolver that returns distinct fields
without this retarget is inert.

**D7 — Single-snapshot discipline (215 continuity).** `effective_join_order_soft_margin_steps`
takes **one** runtime-evidence snapshot at bind entry and derives tree, graph
family fallback, the gate status, and the per-kind ready steps from it. The
actuator does **not** read the `information_schema` table; report and policy never
take separate snapshots in one bind.

**D8 — Read-only continuity; zero new surface.** The other four 217 gates remain
non-consumers; `windowing_decay_pressure` stays `inactive`-only,
`counter_handoff_readiness` stays `blocked_external_signal`-only. No durable
evidence-format change, no new counter key/lane/table/`SystemTableKind`, no wire
or Wendelon change.

## Honesty gates (discriminating)

- `per_kind_graph_margin_inert_until_gate_actionable` — cold ledger / one ready
  kind / converged ready kinds → `ApplyAdvisory` byte-identical to family-uniform
  (the self-arming pin).
- `actionable_graph_divergence_resolves_kind_specific_steps` — seed ≥2 divergent
  ready kinds → the resolver produces distinct per-kind steps from the shared gate
  result.
- `graph_kind_margin_reaches_graph_actuation_decision` — **plan-level proof** that
  the selected kind step reaches `try_bind_graph_actuation` and changes the
  committed graph actuation (a resolver-field test can pass while the optimizer
  still uses the family step — this is the test that catches that, D6).
- `graph_kind_without_ready_evidence_uses_family_step` — D5 fallback.
- `manual_ignores_graph_kind_divergence`; `tree_margin_application_byte_identical_under_graph_kind_divergence`.
- `graph_actuator_and_receipt_table_share_gate_result` — recompute the ready set
  in the actuator and this fails (D3).
- `rune_graph_kind_margin_matches_sql` — Rune/SQL parity through the shared binder.

## Architecture guards (promotion-locked)

- `adr218_graph_actuator_and_receipt_table_share_gate_result` (D3)
- `adr218_graph_actuation_uses_step_for_shape_decision_kind` (D6 — fails if
  try_bind_graph_actuation reads the family scalar)
- `adr218_graph_actuation_armed_only_by_graph_margin_divergence`
- `adr218_tree_and_manual_margin_byte_identical_under_graph_divergence`
- `adr218_no_new_durable_key_counter_table_or_system_table_kind`
- `adr218_other_four_receipt_gates_stay_non_consumers`
- `adr218_graph_kind_domain_is_galaxy_bridge_bridgechain_only`

**Reconciled prior guards (named):**
- `adr217_receipt_gates_are_explicit_five_row_system_table` → the static
  five-row manifest stays, but the graph row may be rendered by a wrapper around
  the shared `GraphMarginDivergenceGate` result instead of by a row-builder-only
  `graph_margin_divergence_gate` function.
- `adr217_graph_gate_uses_ready_kind_divergence_not_family_rollup` → retarget to
  the shared typed gate result; the ready-kind divergence predicate, floor, and
  three graph-kind domain stay identical.
- `adr217_readout_is_read_only_no_policy_consumers` → allow **exactly** the
  `graph_margin_divergence` consumer; the row is still *reported* by the readout,
  not replaced by policy; the other four gates stay non-consumers.
- `adr217_windowing_and_counter_handoff_are_substrate_blocked` → unchanged.
- ADR 213/215 graph readout + family-application guards → 218 refines graph
  application *within* `ApplyAdvisory`; the readout and tree path are unchanged.

## Consensus (transparent)

3-of-3. Claude proposed the self-armed graph actuator (gating on the receipt by
construction to avoid a desire-chosen actuator) and conceded the original
"per-kind margins now" framing into it. Codex retracted its windowing default on
Claude's grounding (no durable-age substrate → windowing is the heavy path), then
contributed the two load-bearing locks adopted here: the **full shared gate
result** (D3) and the **`try_bind_graph_actuation` seam** (D6, correcting the
recognizer thread-point in the iter-1 drafts). Gemini synthesized the decision set
and the single-snapshot continuity. No residual divergence — graph-only,
receipt-armed, durable-surface-free.

## Forward arc

`218 receipt-armed graph-kind margins (this) → the next gate that goes actionable:
preserved-outer safe-side ordering, or a decision-conflict detail readout →
windowing/decay **durable-age substrate** when the readout shows cumulative
staleness → deployment elasticity signal for counter handoff → memo when
decision-conflict is legible`.

## Deferred (named)

Preserved-outer safe-side actuation; decision-conflict detail readout → memo;
windowing/decay durable-age substrate (named honestly — re-opens the counter
format); deployment elasticity signal (external); mixed-chain pushdown; memo/Cascades.
