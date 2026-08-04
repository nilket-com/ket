# ADR 189: Soft-Margin Tuning Advisory

**Status:** Promoted. **Migration:** `plans/impl/0189_soft_margin_tuning_advisory.md`.

187/188 made the optimizer's join-order decisions and their measured verdicts legible across the
workload. 189 is the **feedback** rung, taken observe-first: it reads the accrued 188 verdicts,
computes a **recommended** soft-margin for 178's reorder gate, and surfaces it read-only as
`information_schema.optimizer_margin_advisory`. It changes **no decision and no plan** — 178's
margin const is untouched; the recommendation is an analytic the operator (or a later auto-apply,
190) can act on.

This is the arc's observe-before-act discipline applied to tuning itself: recommend the margin
from evidence and surface it before any cut wires it into the decision. The load-bearing substance
is the tuning **policy** (verdict distribution → recommended margin, bounded + hysteresis +
min-sample); building it as a read-only advisory validates the policy before it is trusted to act.

## Grounding

- **The margin and its direction.** 178's soft reorder fires iff `preferred_cost / declared_cost
  ≤ STAR_ORDER_SOFT_MARGIN_NUMERATOR/DENOMINATOR` (= 4/5; a ≥20% win)
  (`classify_star_order_action`, `binder/join_order.rs:1056`). A **looser** margin is a larger
  ratio (reorders fire more); **tighter** is smaller (fewer reorders).
- **Which verdict means which mis-calibration.** From the 188 log: `MissedWin` (kept declared,
  measured says reorder) ⇒ the margin is too **tight** ⇒ recommend **looser**. `OverEager`
  (reordered, measured says keep) ⇒ too **loose** ⇒ recommend **tighter**. `Vindicated` confirms
  calibration. `MisRanked` is a *cost-model* error (reordered to the wrong order), not a margin
  error — excluded. `Indeterminate` is no signal — excluded.
- **The input already exists.** 188's `OptimizerDecisionLog::verdict_snapshot()` gives counts per
  `(decision_kind, verdict_class)`; the advisory aggregates the margin-relevant classes across
  kinds (the margin is one global constant).
- **The report path already exists.** `information_schema.*` binds via
  `bind_information_schema_relation`; the advisory is a sibling table, no new mechanism.
- **Read-only keeps it safe.** The advisory mutates nothing — not the const, not config, not the
  plan. Like 187/188 it is side-effect-free; applying the recommendation (config + manual/auto)
  is the deferred 190 act.

## Decisions

**D1 - The tuning policy (the substance).** `recommend_soft_margin(verdict_snapshot,
current_margin) -> MarginAdvisory`. Aggregate `missed_win`, `over_eager`, `vindicated` across
kinds. `decisive = missed_win + over_eager + vindicated`.

- `decisive < MIN_SAMPLE` → `InsufficientEvidence` (recommended = current).
- else `net = missed_win - over_eager`; if `net` exceeds a hysteresis threshold (relative to
  `decisive`) → recommend one step **looser**; if below `-threshold` → one step **tighter**;
  else `Keep` (well-calibrated).

`MisRanked` and `Indeterminate` are excluded from `decisive` and from `net`.

**D2 - A bounded, clamped margin ladder.** Recommendations move **one step** along a fixed safe
ladder of ratios (e.g. `3/5, 7/10, 4/5, 9/10, 19/20`), clamped at the ends. Represent the
effective margin as a ladder step (or a private-field ratio value produced only by ladder
constants), not an arbitrary public numerator/denominator pair. One-step + ladder gives natural
bounding and hysteresis; invalid ratio states, runaway values, and arbitrary learned ratios are
unrepresentable.

**D3 - Read-only; no decision or plan change.** The advisory reads the verdict log and the current
margin const; it mutates nothing. 178's `classify_star_order_action` and `STAR_ORDER_SOFT_MARGIN_*`
are unchanged; the bound `ExecutionPlan` is byte-identical. Pinned by the full suites staying green
plus a guard that no decision reads the advisory.

**D4 - Surface via `information_schema.optimizer_margin_advisory`.** Bind it through the existing
information-schema path into a `SystemTable` with columns: `current_margin`, `recommended_margin`,
`recommendation` (`loosen`/`tighten`/`keep`/`insufficient_evidence`), `missed_win`, `over_eager`,
`vindicated`, `mis_ranked`, `indeterminate`, `sample_size`. One row (the global recommendation).
SQL and Rune share the path. `mis_ranked` and `indeterminate` are exposed for operator visibility
but excluded from the tuning pressure.

**D5 - Margin-relevant verdicts only.** The signal uses `MissedWin`/`OverEager` (margin
calibration) and `Vindicated` (sample weight); `MisRanked` (cost model) and `Indeterminate` (no
data) are excluded — surfaced as counts if useful but never drive the recommendation.

**D6 - Reuse 188's log; no new accrual.** The advisory consumes `verdict_snapshot()`; it adds no
capture, no new log, no execution change.

**D7 - Application is deferred (190).** Making the margin configurable and applying the
recommendation (manually via config, then auto) is the named follow-on. 189 only recommends.

**D8 - No decision-path/executor/fragmenter/storage/Wendelon change.** The cut is the policy + the
read-only report. No `classify_star_order_action` change, no const change, no executor,
fragmenter, distributed scheduler, storage, PBC, Wendelon, cost model, optimizer shell, memo,
Cascades, or broad `PlanProperties` contract.

## Headline Gates

- `margin_advisory_recommends_looser_on_missed_wins` - a verdict log dominated by `MissedWin`
  (past the hysteresis threshold, above `MIN_SAMPLE`) yields `recommendation=loosen` and a
  `recommended_margin` one step looser than current.
- `margin_advisory_recommends_tighter_on_overeager` - an `OverEager`-dominated log yields
  `recommendation=tighten`, one step tighter.
- `margin_advisory_is_read_only_no_plan_change` - a populated verdict log (the advisory's input)
  does not change the bound `ExecutionPlan` or 178's reorder decision — byte-identical with an
  empty vs seeded log; the margin const is unchanged (the verdict log is read-only for planning).

## Supporting Gates

- `margin_advisory_keeps_when_well_calibrated` - a `Vindicated`-dominant / balanced log →
  `recommendation=keep`, recommended = current.
- `margin_advisory_insufficient_evidence_below_min_sample` - a small log → `insufficient_evidence`,
  recommended = current.
- `margin_advisory_excludes_misranked_and_indeterminate` - a log of only `MisRanked`/`Indeterminate`
  → `insufficient_evidence`; the report still shows the excluded counts, but they do not count as
  decisive.
- `margin_advisory_clamps_to_safe_range` - an extreme imbalance recommends at most the ladder end,
  never beyond.
- `margin_advisory_has_hysteresis` - a small `net` (below threshold) → `keep`, no flip.
- `rune_queries_margin_advisory` - Rune reads the advisory.

## Architecture Guards

- `adr189_advisory_is_read_only` - `recommend_soft_margin` and the report are read-only over the
  verdict log + margin const; no mutation; no decision reads the advisory.
- `adr189_advisory_does_not_change_margin_or_decision` - `STAR_ORDER_SOFT_MARGIN_*` and
  `classify_star_order_action` are unchanged by 189.
- `adr189_advisory_reuses_188_verdict_log` - the advisory reads `verdict_snapshot()`; it adds no
  new accrual or capture.
- `adr189_advisory_excludes_misranked_indeterminate` - the policy excludes those classes from the
  decisive count and the net while still surfacing their counts in the report.
- `adr189_report_reuses_information_schema_path` - the advisory binds via
  `bind_information_schema_relation`.
- `adr189_no_executor_fragmenter_storage_or_wendelon_change` - no executor, fragmenter,
  distributed scheduler, storage, PBC, Wendelon change.
- `adr102_system_table_kind_has_finite_introspection_family` - retarget the finite system-table
  variant guard for the new `InformationSchemaOptimizerMarginAdvisory` member instead of leaving
  a stale count failure.
- `adr189_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.

## Rejected Alternatives

**Auto-apply the recommended margin now.** The control-loop risk (a mis-tuned margin degrades
plans) plus deployment-dependent data argue for observe-first; the advisory validates the policy,
and 190 applies it once trusted.

**Make the margin configurable in this cut.** Configurability is the first step of *application*
(manual apply); it touches the decision path. 189 stays purely observational (recommend); 190 owns
configurability + apply, keeping a clean observe/act split.

**A continuous computed ratio.** A fixed clamped ladder with one-step moves is safer and
self-hysteretic; a continuous ratio invites oscillation and runaway.

**Tune on MisRanked.** MisRanked means the reorder picked the wrong order — a cost-model problem,
not a margin problem. Tuning the margin on it would mis-correct.

**A bespoke command.** `information_schema` gives a composable, frontend-shared, `SELECT`-able
advisory.

## Non-Goals

- no margin application / mutation / configurability (that is 190);
- no decision-path change (178 unchanged);
- no per-kind or per-query margins (one global recommendation);
- no cost-model tuning (MisRanked excluded);
- no persistence, executor, storage, PBC, wire, Wendelon, or catalog change;
- no cost model, optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Consensus Trail

189 closes the robustness loop on the observable side: it reads 188's measured verdicts, computes
a recommended soft margin via a bounded, hysteretic, min-sampled policy, and surfaces it read-only
as `information_schema.optimizer_margin_advisory`. Observe-before-act applied to tuning — recommend
from evidence, change nothing — so the policy is validated before 190 wires it into the decision.
After 189 the optimizer can *say* whether its reorder gate is mis-calibrated and which way; acting
on it is the earned next step.

## Forward Arc

`187 decision accrual -> 188 measured verdict accrual -> 189 margin tuning advisory -> 190 margin
application (configurable + manual, then auto-apply) -> [loop closed]`. After 189 the optimizer
robustness arc is complete on the observe side; the remaining topology/executor bets are best
ordered by deployed accrual evidence — a natural chapter boundary.

## Deferred (named)

Margin application (190: configurable margin + manual/auto apply); per-kind margins; cost-model
tuning (MisRanked); persistence; the broadcast-snowflake cliff; mixed-strategy cost model;
galaxy/bridge topology.
