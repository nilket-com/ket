# Toron ADR 227: Probe-Linearity Fence Receipts

**Status:** Promoted. **Migration:** `plans/impl/227_probe_linearity_fence_receipts.md`.
**Category:** Toron optimizer instrumentation — replace the lossy boolean broadcast
probe-linearity fence with a typed, inspectable cause rendered in EXPLAIN receipts. **Read-only
instrumentation:** no physical-planning / fragmenter / execution change (bound plans
byte-identical), no new durable key / decision reason / `OptimizerDecisionOutcome`, no decision
matrix change (stays 140/220), no system table, no `optimizer_memo_admission` change; ADR 226's
all-zero / zero-admissible / no-generic-search locks stay green.

## Why this cut exists

ADR 226 proved `ProbeNotScanLinear` is a real *physical execution fence* with a production home
and a valid hash fallback — not a missing candidate. The post-226 discipline is to make a
fence legible **before** arguing about relaxing it. Today the fence is opaque:
`broadcast_probe_is_scan_linear(plan) -> bool` (join.rs:703) collapses every blocking cause —
`LIMIT`, `Take`, `Sort`, `Aggregate`, `Exchange`, a non-broadcast/ineligible `Join`, a nested
broadcast join whose right probe is itself non-linear, and non-scan leaves — into a single
`false`. The durable lane records only the coarse `probe_not_scan_linear` reason; EXPLAIN shows
only `scan_linear=false`. A later ADR that wants to relax any part of the fence cannot know
which cause-class it is lifting, or which of the two consumers sees it. This cut replaces the
boolean with a typed cause and renders it, so the next plan-changing ADR relaxes exactly one
named cause with production evidence — instead of guessing.

## Grounding (verified)

- `broadcast_probe_is_scan_linear` (join.rs:703): `Scan{row_limit: Some}` / `Take` / `Sort` /
  `Aggregate` / `Exchange` / non-broadcast or ineligible `Join` / non-scan leaves
  (`EmptyRelation` / `SystemTable` / DDL / DML) → `false`; `Filter` / `Project` recurse
  (transparent); an eligible inner `Broadcast` `Join` with `terminal_exchange(left) ==
  Some((1, _))` recurses on `right`.
- Two consumers depend on it through different paths. The ordinary hash-strategy path first
  captures `left_probe_scan_linear` / `right_probe_scan_linear` into `JoinInputOptimization`
  and `JoinOrderEdgeEvidence` (binder.rs:2190-2244), then
  `choose_auto_join_strategy` can emit
  `hash_join_decision(HashByDefaultReason::ProbeNotScanLinear)` (join.rs:612). The
  star/snowflake actuation path calls the helper directly at join_order.rs:4216, with branch
  authorization at 4557. 226 Gate 3 proved the star decline reaches the durable lane with a
  valid hash fallback. This carrier path is why the cut must carry typed linearity through the
  structs, not just wrap the helper.
- The reason is rendered at explain.rs:2383 (`probe_not_scan_linear`), 2742
  (`declined_probe_not_scan_linear`), 2771 (`probe_not_scan_linear`).
- The decision matrix is `7 × 10 × 2 = 140` / combined 220 (optimizer_accrual.rs); this cut
  adds no outcome, so it stays.

## Decisions (promotion-locked)

**D1 — Typed, exhaustive linearity readout.** Introduce `broadcast_probe_linearity(plan) ->
ProbeLinearity`, where `ProbeLinearity` is `Linear` or `Blocked(ProbeLinearityFence)`, and
`ProbeLinearityFence` is the exhaustive set of named blockers: `ScanRowLimit`, `Take`, `Sort`,
`Aggregate`, `Exchange`, `NonBroadcastJoin`, `NonScanLeaf`. The `match` over `ExecutionPlan`
has **no `_ =>` wildcard**, so a future execution node forces a deliberate linearity decision
(the ADR 225 `decline_blocker_class` discipline). A thin wrapper
`broadcast_probe_is_scan_linear(p) = matches!(broadcast_probe_linearity(p), Linear)` stays
available for bool-only continuity, but every receipt-producing carrier stores the typed
result.

**D2 — Cause propagation; `Filter`/`Project` transparent.** `Filter`/`Project` recurse and
pass linearity through. The eligible inner `Broadcast` join recurses on its right/probe side
and **propagates the inner cause** rather than masking it with a generic nested-join blocker —
a `Sort` buried under a valid nested broadcast reads `sort`, so the receipt names the node an
operator would actually fix.

**D3 — Single shared classifier for both consumers.** Both the hash-strategy fallback
(`HashByDefaultReason::ProbeNotScanLinear`) and the star/snowflake decline
(`StarOrderDeclineReason::ProbeNotScanLinear`) read the exact same `broadcast_probe_linearity`
result, carried through `JoinInputOptimization`, `JoinOrderEdgeEvidence`, and
`SnowflakeEdgeStrategy` as needed. No raw string payloads (`&'static str`) before the render
edge, and no second classifier, so the definition cannot drift between ordinary joins and star
graphs.

**D4 — Rendered receipt, not a durable decision.** The blocking cause is a diagnostic EXPLAIN
detail, appended to the rendered receipt token (e.g. `probe_not_scan_linear:sort`). **No new
`OptimizerDecisionOutcome`; the durable decision reason stays the coarse `probe_not_scan_linear`;
the decision matrix stays 140/220** (the ADR 226 over-expansion mistake is not repeated). No
durable key, no system table.

**D5 — Zero plan impact.** Strictly read-only instrumentation; bound plans are byte-identical
before and after. No physical-planning, fragmenting, or execution behavior change; no new
broadcast shape is admitted through the fragmenter.

**D6 — EXPLAIN goldens re-pinned in-cut.** Rendering the cause enriches the EXPLAIN receipt
text at explain.rs:2383/2742/2771, so EXPLAIN goldens referencing `probe_not_scan_linear` /
`scan_linear` will shift; they are re-pinned in this same cut (render-text growth does not
self-stabilize). The re-pin is also the falsification signal that the cause reaches the
receipt.

## Honesty gates (discriminating)

- `probe_linearity_project_and_filter_are_transparent` — `Project(Filter(Scan))` → `Linear`.
- `probe_linearity_limit_sort_aggregate_exchange_are_distinct_causes` — each blocked class
  renders its own cause, not one generic `false`. (Fails on `main`.)
- `probe_linearity_nested_broadcast_propagates_inner_cause` — a `Sort` under an eligible
  broadcast join reads `sort` (locks D2). (Fails on `main`.)
- `hash_join_probe_not_scan_linear_renders_typed_cause` — the ordinary fallback exposes it.
  (Fails on `main`.)
- `hash_join_probe_not_scan_linear_uses_carried_input_cause` — the ordinary fallback proves
  the cause survived `JoinInputOptimization`, rather than being recomputed loosely or replaced
  with a string.
- `star_probe_not_scan_linear_renders_same_cause_via_shared_helper` — the star path uses the
  one classifier (fails if a second classifier is introduced).
- `probe_linearity_receipts_do_not_change_bound_plan` — representative fixtures bind
  byte-identically before/after (locks D5).
- EXPLAIN goldens re-pinned and green.

## Architecture guards (promotion-locked)

- `adr227_probe_linearity_is_typed_exhaustive_no_wildcard`
- `adr227_probe_linearity_cause_is_not_stringly_typed`
- `adr227_both_consumers_share_single_linearity_classifier`
- `adr227_no_plan_fragmenter_or_durable_change`
- `adr227_no_new_decision_reason_matrix_stays_140_220`
- `adr227_226_zero_admissible_and_no_generic_search_intact`

**Reconciled prior guards (named):**
- ADR 216 decision matrix → **unchanged at 140/220** (no new outcome/reason).
- ADR 226 all-zero contract / zero-admissible / no-generic-search → **unchanged**.
- ADR 220/222/225 decision/window/blocker surfaces → unchanged (cause is render-only).
- ADR 221/197 no-memo fences → unchanged.

## Consensus (transparent)

3-of-3. Council selected the instrument-first framing over a direct fence relaxation (Claude
iter-0 had ranked broadcast physicalization as cost-dependent/speculative; Codex iter-0 reframed
it as a receipt cut — retire the bool, render the cause, change no plan). Codex grounded the
shared-classifier requirement (the fence is used by both ordinary join selection and star
actuation) and preferred the narrow scan-linearity name over a broader "fragmentability"
taxonomy (mixed-strategy is already typed at its authorization site). Claude contributed the
exhaustive-no-wildcard guard, the nested-broadcast cause propagation, and the EXPLAIN-golden
re-pin discipline. Gemini synthesized. No residual divergence.

## Forward arc

```text
226 capability-lane coverage audit
-> 227 probe-linearity fence receipts (this; the fence is now legible per cause)
-> targeted fragmenter relaxation for one named cause, only if it dominates in production
   and clears the production-home test (e.g. probe physicalization to enable broadcast) —
   or the next receipt-gated topic / rotate out of the optimizer.
```

## Deferred (named)

Any fragmenter relaxation / new broadcast shape; probe physicalization (sort/repartition to
enable broadcast); `MixedStrategyStar` authorization relaxation (already typed at its site);
folding mixed-strategy into a broader fragmentability-fence taxonomy; counter-handoff
elasticity signal (external/deployment); mixed-chain predicate motion through bridge junctions.
