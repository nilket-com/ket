# ADR 200: Graph Decision & Verdict Accrual

**Status:** Promoted. **Migration:** `plans/impl/0200_graph_decision_and_verdict_accrual.md`.

The galaxy/bridge actuators 198/199 shipped **actuate**, but record **0×** into the 187/188
`OptimizerDecisionLog` (the star actuator records 8×) — so every graph reorder/keep/decline is invisible
to `information_schema.optimizer_decisions` and `…optimizer_runtime_verdicts`. ADR 200 closes the join-
order arc's last telemetry gap: wire graph decisions + verdicts into that loop, through a single
**unified graph actuator** that also pays down the duplication 199 left. Side-effect-only — plans and
EXPLAIN receipts stay byte-identical.

## Grounding (verified against the pre-200 code)

This grounding describes the **pre-200** state that motivates the cut. D1 then unifies the two actuators
named below into `try_bind_graph_actuation`, so post-implementation `try_bind_galaxy_actuation` /
`try_bind_bridge_actuation` no longer exist (see the impl plan's as-built anchor table).

- **The gap is exact.** Pre-200, `try_bind_galaxy_actuation` / `try_bind_bridge_actuation` contain **0**
  `accrue_*` calls; `try_bind_star_order_actuation` contains **8**. Graph decisions never reach the log.
- **The duplication 199 left.** The two actuators are ~87 lines and differ **only** in the function name
  and three references — `analyze_galaxy`/`analyze_bridge`, `galaxy_order_recommendation`/
  `bridge_order_recommendation`, `set_applied_galaxy_root`/`set_applied_bridge_root`. The scratch-bind,
  the soft-margin gate, `reroot_galaxy_join_tree`, and the bind-rerooted tail are byte-identical; the
  recommendation type (`GraphOrderRecommendation`) and the reroot are already shared. Wiring accrual into
  two byte-identical copies is the smell the unify removes — before n-fact would triple it.
- **The verdict seam (load-bearing scope).** `measured_verdicts_from_summaries` (explain.rs) handles only
  `ObservedSnowflake` and `StarReceipts` — **no galaxy/bridge arms**, so graph verdicts are absent from
  the sampled (normal-execution) path. **But** `star_order_runtime_replay_ids` (binder.rs) **already
  unions galaxy + bridge edge targets** — the sampled path already *collects* the graph observations;
  only the verdict-class *computation* is missing. So verdict accrual is a **bounded extraction** (lift
  the inline logic from `graph_order_verdict_row` into a shared helper, add two arms), not new
  measurement plumbing.
- **Anchor correction.** Earlier drafts named `compute_join_order_verdicts`, `score_galaxy_order`,
  `score_bridge_order` — **none exist**. The real seam is `measured_verdicts_from_summaries` /
  `measured_explain_analyze_verdicts`; the recommenders are `*_order_recommendation`; the render-time
  verdict is `graph_order_verdict_row`.

## Decision

**D1 — Unify into `try_bind_graph_actuation`.** One function over a `GraphShape { Galaxy, Bridge }` enum
dispatching the 3-point seam: recognize → `Option<GraphOrderRecommendation>`, the applied-root setter,
and the accrual kind. The shared scratch-bind / margin gate / reroot / bind-rerooted body is
deduplicated. Reuse the existing result enum (`GalaxyActuationResult` → `GraphActuationResult`). Behavior-
neutral; the unify travels with this ADR (discipline #6 — accrual must edit both actuators anyway),
pinned by the 194–199 suites staying byte-identical *before* any accrual is added. n-fact later extends
the same enum.

**D2 — Graph decision accrual.** In the unified actuator, record the committed outcome via the typed
`OptimizerDecisionRecorder` (mirroring the star actuator's 8 sites): `Reordered` on fire,
`KeptAlreadyOptimal` on the already-optimal path, `KeptThinMargin` on the soft-margin keep path, and
`Declined(reason)` on missing-estimate / preflight-cliff. This needs a graph-specific result surface
(`GraphActuationResult`) rather than the current two-way galaxy enum so the keep/decline split stays
faithful to the committed outcome. Add `OptimizerDecisionKind::GalaxyRoot` / `BridgeRoot` — provenance
is what the operator reads `information_schema` for; a re-rooted graph *is* a snowflake structurally, but
"a galaxy reordered" is the fact worth surfacing. Recognition stays on the no-op recorder, so scratch
never accrues (the 199 discipline).

**D3 — Graph verdict accrual (folded).** Extract `measured_graph_order_verdict(rec, measured) ->
StarOrderVerdict` from the inline logic in `graph_order_verdict_row` (render and accrual share one
computation), and add galaxy/bridge arms to `measured_verdicts_from_summaries` emitting
`MeasuredLevelVerdict { kind: GalaxyRoot|BridgeRoot, verdict }`. The 188 sampled `record_verdicts` loop
consumes them unchanged → graph verdicts land in `…optimizer_runtime_verdicts`, and the 189/191 margin
advisory sees graph `MissedWin`s for free. **Escape hatch:** the replay-id union already supplies the
graph observations; if the galaxy measured-cost assembly in the sampled path exceeds what
`bridge_analyze_rows` already computes, ship D1+D2 as ADR 200 and split D3 to ADR 201 — D2 + unify is a
complete rung on its own. Name the split in the seal if taken.

**D4 — Boundaries / purity.** Accrual and unification are side-effect-only: execution plans and EXPLAIN
receipts stay byte-identical to `main`. Toron binder + decision/verdict log. No executor, fragmenter,
scheduler, storage, Wendelon, PBC, wire, or grammar change.

## Guards reconciled (discipline #4)

Unifying renames the actuators, so `adr198_galaxy_actuation_*` and `adr199_bridge_actuation_*` (which pin
`try_bind_galaxy_actuation` / `try_bind_bridge_actuation` by name) **retarget** to
`try_bind_graph_actuation`, preserving every invariant (reuses-snowflake-machinery, respects-soft-margin,
recognition-recursion-free via `without_decision_recording`, binder-only; the `GraphShape` arms keep the
two-shape bound and `…reroot_is_bounded`). New `adr200_*`: `…unified_actuator_replaces_duplicates` (no
`try_bind_galaxy_actuation`/`try_bind_bridge_actuation` symbols remain), `…graph_decisions_accrue_to_shared_log`
(the actuator calls the `accrue_*` sink; `GalaxyRoot`/`BridgeRoot` reach the report),
`…graph_verdicts_accrue_to_sampled_runtime_log` (the `galaxy_order`/`bridge_order` arms in
`measured_verdicts_from_summaries`), `…accrual_is_side_effect_only` (the 187/188 binder-only sweep).

## Honesty gate

`graph_actuation_accrues_committed_decisions_and_verdicts`: under the **normal** sampled path
(`sample_rate = 1`, no EXPLAIN), a reordered galaxy and a reordered bridge each emit an
`information_schema.optimizer_decisions` row (`galaxy_root` / `bridge_root`, `reordered`) **and** an
`…optimizer_runtime_verdicts` row. Must **fail on `main`** (0× today). Discriminator
`kept_graph_accrues_kept_not_reordered`: an already-optimal galaxy / clean-FK bridge accrues a *kept*
decision, not reordered — proving accrual records the committed decision, not actuation-fired. Regression:
`unified_actuator_preserves_198_199_behavior` (physical plans byte-identical to `main` on the 198/199
fixtures) + the 194–199 EXPLAIN suites byte-identical.

## Consensus (transparent)

**Grounded 2-of-3 (Claude, Gemini)** on graph decision & verdict accrual via the unified actuator.
**Codex dissents (iter-2), holding n-fact / multi-hop graph ordering evidence** as the next cut.
Resolved against the dissent by grounding, not headcount:

- Accrual closes a **verified** gap (graph decisions 0× vs the star's 8×); n-fact, by Codex's own words,
  is to "learn *whether* breadth introduces a real ordering problem" — a hypothetical this ADR's receipts
  are what answer. Codex's own acceptance ("decide whether verdict accrual or memo are warranted later")
  **depends on** the workload telemetry accrual provides. So accrual is the **prerequisite** for n-fact,
  not its rival.
- Accrual is equally a "smallest honest observe-only step" — it changes no plan or receipt, satisfying
  the dissent's bar while closing a present gap rather than opening a new (search-adjacent) frontier
  mid-arc.

**n-fact / multi-hop is the named follow-on**, to be scoped by these receipts. If the dissent later
grounds that breadth must precede telemetry, it re-opens cheaply (promotion is reversible). The verdict-
extraction insight (the replay-id union already collects graph observations, so D3 is a bounded
extraction) is shared 2-of-3 and is what made the full 187+188 capstone foldable rather than a second ADR.

## Forward arc

`199 bridge actuation → 200 graph decision & verdict accrual (this; closes the arc's telemetry loop) →
n-fact / multi-hop graph ordering + actuation (scoped by these receipts) → memo only when receipts show
local decisions fighting`.

## Deferred (named)

n-fact galaxy / multi-hop bridge ordering + actuation; general join-order search / memo / Cascades;
cross-shape joint actuation; catalog PK/FK structural classification; outer joins; durable optimizer
evidence.
