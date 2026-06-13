# ADR 215: Graph-Family Margin Advisory Application

**Status:** Promoted. **Migration:** `plans/impl/215_graph_family_margin_advisory_application.md`.
**Category:** Toron optimizer policy — the application half of the graph-family margin loop, closing
the 189→191 replay that 213's readout opened. The 213 application gate was re-priced by operator steer
(recorded in Consensus). No new config modes, durable keys, tables, recognizer, executor, fragmenter,
or Wendelon change.

## Why this cut exists

213 built the graph-family readout and deliberately left the knob read-only behind a written
gate ("apply only if the readout shows actionable signal"). The operator re-priced that gate on
the structural argument the council made explicit this cycle: the application is **self-arming
but evidence-inert** — `recommend_soft_margin` returns the configured base below
`JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE` and hysteresis holds `keep` below threshold, so a wired
graph resolver moves the knob exactly when, and only when, the graph knob row's own condition fires organically (sample over the floor, recommendation past hysteresis),
behind the double opt-in of explicit `ApplyAdvisory` plus the evidence floor. This replays
189→191→(208 split)→213→**this** for the graph family: the loop the tree family already runs.

## Decisions

**D1 — No new mode; ApplyAdvisory gains its second actionable family (Codex).** `Manual` =
configured base for both families, byte-identical. `ApplyAdvisory` = tree sites consume the tree
recommendation (existing), graph sites consume the graph recommendation (new). No graph-specific
mode, bool, or config field.

**D2 — Parallel family policy, one bind-entry snapshot.** Add the graph-family resolver by
mirroring the tree policy, but binder construction derives both tree and graph effective steps
from one shared `verdict_snapshot()` read. Tree keeps the existing `TreeFamily` recommendation;
graph uses the existing `GraphFamily` scope filter and `recommend_soft_margin(...).recommended_step`
under `ApplyAdvisory`; both return the configured base under `Manual`. The 208 field split now
carries two *resolved* values; low-level tree/graph gates read those fields and never read
runtime evidence.

**D3 — Scope purity both directions, third family excluded.** The graph resolver consumes only
`GalaxyRoot | BridgeRoot | BridgeChainRoot` rows (the existing 213/214 partition);
tree evidence cannot steer graph, graph cannot steer tree, and `PreservedOuterOrder` steers
neither (214-D6 continuity, now load-bearing at an application site).

**D4 — The inertness contract is a promoted decision, not an implementation accident (the
steer).** Under `ApplyAdvisory` with graph evidence below the floor or within hysteresis, the
graph gate binds byte-identical to `Manual`. Pinned by its own gate — this is the re-priced 213
gate living on as a runtime invariant instead of a planning condition.

**D5 — The 213 report tells the new truth.** The `graph_join_order` knob row's
`application_mode`/`applied_margin` render row-effectively (no longer hardcoded `manual`): under
`ApplyAdvisory` they show the applied graph step. Kind-detail rows remain diagnostic and
non-actionable (unchanged). Report rendering uses the report's single snapshot and row advisory;
it must not call a bind-entry resolver and take a second evidence read. No schema change — the
213 columns were built for this.

**D6 — Boundaries.** No per-kind knobs (still gated on the kind-detail rows showing divergence);
no outer-family application (still gated on material `preserved_outer_order` MissedWin counts);
no new durable keys, tables, or config; no recognizer/executor/fragmenter/Wendelon change. Counter
handoff: **named deferral** absent a deployment elasticity signal (the steer's explicit disposition).

## Honesty gates

- **Red on main:** `graph_evidence_changes_near_margin_graph_decision_while_tree_evidence_does_not`
  — the test 213 pre-named at promotion, finally honest under the re-priced gate: seed
  graph `MissedWin` ≥ floor under `ApplyAdvisory`, a near-margin galaxy/bridge/chain actuation
  flips; the same seeds as tree evidence flip nothing.
- `apply_advisory_with_insufficient_graph_evidence_binds_byte_identical` — **the steer's gate**
  (D4): below-floor and within-hysteresis seedings bind identical to Manual.
- `manual_mode_byte_identical_for_both_families` — continuity.
- `tree_and_graph_evidence_do_not_cross_steer` — both directions in one fixture (extends 208's
  discriminator to the application site).
- `preserved_outer_evidence_steers_neither_family` — 214-D6 at the application site.
- `graph_knob_row_renders_row_effective_application` — D5.
- 208/213/214 suites pass; the named retargets below are the only edits.

## Architecture guards

`adr215_graph_application_shares_apply_advisory_mode_no_new_mode`,
`adr215_bind_margin_steps_share_one_snapshot`,
`adr215_graph_resolver_is_scope_pure`,
`adr215_insufficient_graph_evidence_is_byte_identical` (the inertness pin),
`adr215_kind_detail_rows_remain_non_actionable`,
`adr215_no_outer_family_application`,
`adr215_no_new_keys_tables_or_config`.
**Named retargets (discipline #4):** 213's `adr213_graph_readout_does_not_drive_graph_gate`
**inverts** → "the graph gate consumes only the floor-gated graph recommendation under
ApplyAdvisory"; the 213 `graph_readout_is_read_only_no_plan_change` behavioral gate retargets to
Manual + insufficient-evidence byte-identity; 208's binder-construction guard strings retarget to
the shared bind-entry resolver; 213's D4 hardcoded-manual rendering assertion retargets to
row-effective.

## Consensus (transparent — the steer, recorded)

The council split 2-vs-1 on timing (Codex + Gemini: apply now; Claude: hold the written 213
gate). Claude's iter-1 made both readings explicit — one treating the gate as a justification
requirement, the other treating it as structurally preserved by the evidence floor — and put the
re-pricing where the 214 precedent says it belongs: with the operator. **The operator re-priced
the gate on the evidence-floor reading**: the graph resolver is self-arming but evidence-inert
until the graph knob row shows actionable signal, because it sits behind both explicit
`ApplyAdvisory` and the existing MIN_SAMPLE + hysteresis policy; Manual remains byte-identical;
and Wendelon counter handoff preempts only on a live elasticity signal, which is absent — it
stays a named deferral.
Design consensus is 3-of-3 (Codex's graph-application sketch with the shared-snapshot
implementation sharpening folded in; Claude's D4/D5 elevations; Gemini aligned).

## Forward arc

`215 graph advisory application (this) → per-kind graph knobs (gated: detail-row divergence) →
safe-side outer ordering (gated: preserved-outer MissedWin counts) → windowing/decay (gated: counter saturation — now with two
applied advisories reading cumulative counters, the staleness question sharpens) → counter
handoff (elasticity signal) → mixed-chain pushdown (named shape) → memo (receipts fighting)`.

## Deferred (named)

Per-kind graph knobs; outer-family application; windowing/decay; counter locality handoff
(deployment-gated, the steer's disposition); per-decision provenance; exactly-once flush;
mixed-chain pushdown; memo/Cascades.
