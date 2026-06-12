# ADR 213: Graph-Family Margin Advisory Readout

**Status:** Promoted. **Migration:** `plans/impl/213_graph_family_margin_advisory_readout.md`.
**Category:** Toron optimizer policy/reporting — the rotation cycle's cut, and the readout half of the
graph-family margin loop. No executor, fragmenter, storage, wire, Wendelon, outer-join-demotion,
durable-key, cost-model, or graph-actuation behavior change.

## Why this cut exists

The 207/208/076 arc made optimizer verdicts durable, replicated, and family-scoped — and then the
outer-join arc (209–212) consumed four cycles while the receipts accumulated. The rotation rule written
into 212's forward arc is "by receipts," and this cut is what makes that rule operational: graph verdicts
(`GalaxyRoot`, `BridgeRoot`, `BridgeChainRoot`) accrue durably but **nothing reads them as policy** — the
189/191 advisory machinery is tree-scoped end-to-end (208's deliberate fence), and the graph margin knob
that gates real actuation (`graph_join_order_soft_margin_step`, read at the thin-margin gate) has no
feedback surface at all. This ADR exposes what the graph verdicts would recommend, **without applying
it** — the proven 189-before-191 sequence, replayed for the graph family. After it lands, every
receipts-gated condition on the council's ledger is checkable with a query instead of a hand-derivation.

**The organizing rule (the cycle's design contribution): recommendations attach to knobs; diagnostics
attach to kinds.** There are exactly two margin knobs, so exactly two rows can ever become actionable. The
208-D6 question — do galaxy, bridge, and chain want *different* margins, i.e. is one graph knob the right
long-term shape? — is diagnostic, answered by per-kind margin math that no existing surface provides.

## Grounding (verified against the code)

- `recommend_soft_margin(snapshot, current)` (join_order.rs:103-165) is family-neutral verdict-class math
  over a provided slice — reused untouched. `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE = 20` (:7) applies per
  scope.
- `is_tree_margin_advisory_kind` / `tree_margin_advisory_input` are 208's boundary; graph evidence appears
  only as the tree row's `excluded_sample_size` today.
- `graph_join_order_soft_margin_step` (:231) returns the configured base under every application mode;
  graph actuation reads it at :2538. **This cut does not change that.**
- `GraphShape::decision_kind` covers the graph partition exactly (Galaxy → GalaxyRoot, Bridge →
  BridgeRoot, Chain → BridgeChainRoot — the 206 partition-completeness inherited).
- The durable 35-key matrix already carries all seven non-overflow kinds × five classes — no evidence-store
  widening.
- `information_schema.optimizer_margin_advisory` already carries `evidence_scope` and
  `excluded_sample_size` (208) — the multi-row extension is what that column anticipated, and it requires
  **no new `SystemTableKind` variant** (the adr102 finite-family guard does not bump — the dividend that
  settled extension over a sibling table).
- The "evidence-driven outer ordering" thread is currently **inexpressible**, not merely unfired: durable
  keys record the resulting decision kind (a demoted star accrues `flat_star`); no outer-specific kind or
  counterfactual receipt exists. Its next step is an evidence-design cut, gated on this readout.

## Decisions (promotion-locked)

**D1 — Graph advisory scope is exact and exhaustive.** A graph-scope predicate accepts exactly
`GalaxyRoot | BridgeRoot | BridgeChainRoot`; every tree kind and `Overflow` excluded. Tree and graph scope
helpers are exhaustive matches; a unit test pins the partition (every real kind in exactly one family,
`Overflow` in neither).

**D2 — One snapshot, independent scoped rows.** The report reads `runtime_evidence.verdict_snapshot()`
once and computes every row from it — the 191/208 resolve-once discipline applied to reporting; no per-row
races against a live refresh. A source guard pins that the snapshot read happens at the report-builder
entry, not inside the per-scope row helper.

**D3 — Scope-keyed five-row report; knob rows vs kind-detail rows.**
`information_schema.optimizer_margin_advisory` becomes scope-keyed: two `knob_advisory` rows
(`tree_join_order`, `graph_join_order`) and three `kind_detail` rows (`galaxy_root`, `bridge_root`,
`bridge_chain_root`), distinguished by a `row_kind` column **appended at the tail** (every 189/191/208
column index preserved). Kind-detail rows run the same math scoped to one kind — the D6-answering
diagnostic — and are structurally non-actionable. `excluded_sample_size` is symmetric and consistent:
every row counts the sampled verdicts outside its own scope. Rows emit in fixed order:
`tree_join_order`, `graph_join_order`, `galaxy_root`, `bridge_root`, `bridge_chain_root`; behavioral tests
still select by `evidence_scope` so semantics do not depend on position.

**D4 — Application rendering is row-effective and honest.** The tree knob row keeps current semantics
(recommendation from tree evidence; `applied_margin` follows `ApplyAdvisory` for that row only). The graph
knob row and all detail rows are read-only in this ADR: they render `application_mode = manual` and
`applied_margin = current_margin` — the report states the truth that nothing applies them.

**D5 — Read-only, structurally and behaviorally.** `graph_join_order_soft_margin_step` keeps returning the
configured base under every mode — zero application paths added. 208's
`apply_advisory_does_not_steer_graph_actuation` is re-pinned as an explicit 213 continuity gate: if it
fails, the readout became policy.

**D6 — Existing reports stay complete; boundaries.** `optimizer_runtime_verdicts` (all 35 keys) and the
decision report untouched; the durable store untouched; no PlannerConfig change; SQL and Rune share the
surface (parity gate); no executor / fragmenter / storage / wire / Wendelon / outer-join / cost-model
change.

## Honesty gates

Red on `main`:
- `graph_margin_advisory_reports_graph_scope` — graph `MissedWin` seeds ≥ MIN_SAMPLE produce a
  `graph_join_order` knob row recommending loosen (today no graph row exists anywhere; the counts show
  only as tree excluded evidence).
- `per_kind_detail_rows_aggregate_independently` — galaxy-loosen + chain-tighten seeds produce diverging
  detail rows (the 208-D6 signal, finally observable).

**Load-bearing discriminators:**
- `graph_margin_advisory_ignores_tree_verdicts` + the tree-row mirror — the 208 boundary, both directions.
- `graph_readout_is_read_only_no_plan_change` — byte-identical bind under any verdict seeding × every
  application mode (fails under accidental application — the named falsification).
- `kind_detail_rows_are_not_actionable` — `row_kind` pins the structural distinction (fails if collapsed).
- `tree_row_selected_by_scope_matches_208_contract` — the retarget safety net: the tree row's values are
  byte-identical to the pre-213 single row once selected by scope.
- `insufficient_evidence_renders_per_scope` — thin/empty scopes report honestly (the day-one state).
- `rune_queries_graph_margin_advisory` — parity.
- `apply_advisory_does_not_steer_graph_actuation` — 208 continuity, re-pinned under 213.
- `adr213_margin_advisory_reads_evidence_snapshot_once` — source guard for D2's single-snapshot read.

## Architecture guards (promotion-locked)

- `adr213_graph_advisory_scope_accepts_only_graph_kinds`
- `adr213_margin_advisory_reads_evidence_snapshot_once`
- `adr213_margin_advisory_report_is_scope_keyed_knob_and_detail_rows`
- `adr213_graph_advisory_reuses_family_agnostic_margin_math`
- `adr213_graph_readout_does_not_drive_graph_gate`
- `adr213_runtime_verdict_report_remains_complete`
- `adr213_no_executor_fragmenter_storage_wire_wendelon_or_outer_join_surface`

**Reconciled prior guards (named):** the 208 report-shape tests retarget from one-row/row-0 to
select-by-scope (the tree row's contract intact — pinned by its own gate above); 208's scoped-helper guard
text acknowledges the second consumer (which its wording anticipated); `adr102`'s finite-family count is
**unchanged** (no new variant — assert it in review). All other prior guards pass unchanged.

## Rejected alternatives

**Sibling table** — two tables named "margin advisory," a new SystemTableKind variant, and the per-kind
rows would have made it multi-row anyway. **One aggregated graph row only** — leaves 208-D6 unanswered;
the per-kind divergence is the readout's diagnostic payload. **Graph advisory application now** — repeats
the pre-208 mistake in miniature: policy before the boundary is legible; the application cut follows only
if this readout shows actionable signal, with its one red test pre-named (graph evidence changes a
near-margin graph decision while tree evidence does not). **Outer-ordering evidence first** — currently
inexpressible (grounding above); its design cut is gated on this readout.

## Consensus (transparent)

3-of-3 on the spine (readout-not-application, exact graph scope, math/surface reuse, honest application
rendering). The convergence trail: Codex's iter-0 set the frame and the Candidate-C inexpressibility
analysis (adopted verbatim into the forward arc); Claude's iter-1 contributed the knob-vs-diagnostic rule
resolving the row-shape fork and the adr102 no-bump observation that settled the table fork; Codex's
iter-1 adopted both and tightened twice (`row_kind` at the tail for column-index stability; row-effective
application rendering — both folded into D3/D4); Gemini converged on the knob subset (his two-row shape
predates the kind-detail resolution — the five-row report is the grounded 2-of-3, surfaced per discipline)
and contributed the symmetric `excluded_sample_size` rule (adopted into D3).

## Forward arc

`213 graph readout (this) → graph advisory application ONLY if the readout shows actionable signal (one
red test, pre-named) → outer-ordering evidence design (first decide what outer evidence is: demoted-inner
vs true-outer vs a new receipt family) → mixed-chain pushdown (named demanding shape required) → memo only
when receipts show local decisions fighting`.

## Deferred (named)

Graph advisory application; per-kind graph margin knobs (only if the detail rows prove divergence);
outer-ordering evidence receipts; durable decision-lane readout; advisory windowing/decay over cumulative
counters (a real future question the readout will make visible); mixed-chain pushdown; memo/Cascades.
