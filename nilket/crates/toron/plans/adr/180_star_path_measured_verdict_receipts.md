# ADR 180: Star-Path Measured Verdict Receipts

**Status:** Promoted. **Migration:** `plans/impl/180_*.md`.

The council chose the conservative observe-side over the reversion/act draft: don't act on
179's measurement yet — *judge* the decision with it. ADR 180 turns 179's raw
estimated-vs-actual numbers into a **verdict** on 178's order decision: if ADR 178's same
order model had seen the measured rows, would it have applied the order that actually ran?
It is **pure interpretation** of data 178/179 already produce — no new capture, no acting,
observational. It accrues the verdict that *orders the next act* (adaptive re-order, margin
auto-tuning, or fact feedback) by evidence rather than faith.

### Why a verdict (not reversion)

iter-1 explored adaptive revert-on-spill (acting). The council pulled back: we have the
measurement *tool* (179) but no *accrued judgment* of how often 178 is right or wrong. A
verdict is the disciplined next step — it converts each `EXPLAIN ANALYZE` run into a
decision judgment, so the eventual act (revert / re-order / auto-tune / feedback) is chosen
by what the verdicts show, not guessed. Observe → act → measure → **judge**.

## Grounding

- **All inputs already exist.** 178's `StarOrderEvidence { declared_order,
  cost_preferred_order, applied_order, action, confidence }` + per-edge `estimated_rows`
  (178) + 179's per-edge `actual_rows` (the `EXPLAIN ANALYZE` measured record). 180 reads
  these and renders a verdict — **no new execution capture**.
- **The measured decision is derivable by reusing 178.** Substitute 179's measured
  `actual_rows` into the same `JoinOrderDimEvidence.estimated_rows` field, then run
  `recognized_star_order_outcome` / `classify_star_order_action` / `order_cost` with the
  original confidence. This preserves 178's soft-margin semantics instead of judging
  against a raw sort.
- **The measured applied order must be explicit.** `recognized_star_order_outcome(...,
  None, ...)` records declared order as applied. 180 derives the measured applied order
  from the measured action and passes it as the override before comparing to the original
  `StarOrderEvidence.applied_order`.
- **Observational, rides 179's analyze surface.** 180 appends a summary verdict row to the
  `EXPLAIN ANALYZE` analyze render (`binder/explain.rs`) and preserves 179's per-edge
  runtime rows. It touches no executor, no plan, no decision.

## Decisions

**D1 - Verdict from 178's decision + 179's measurement, interpretation-only.** Per evaluated
broadcast star, combine 178's `applied_order`/`action` + per-edge `estimated_rows` with
179's per-edge `actual_rows`. The measured input is the same `actual_rows` ADR 179 renders
for each edge: `JoinRuntimeObservationSummary.output_rows`. No new capture, no re-plan, no
behavior change.

**D2 - Recompute 178 under measured rows.** Clone 178's dimension evidence, replace only
`estimated_rows` with 179's measured output rows, and run the existing 178 star-order model.
Derive the measured applied order exactly like 178 actuation:

- measured `Reordered` → `measured.cost_preferred_order`;
- measured `KeptAlreadyOptimal` / `KeptInsufficientMargin` → `measured.declared_order`.

Then record that measured applied order in the measured evidence and compare it to the
original `StarOrderEvidence.applied_order`.

**D3 - The verdict classes.**

- **Vindicated** — original `applied_order == measured.applied_order`.
- **MisRanked** — 178 reordered to one order, but the measured 178 model would reorder to a
  different order.
- **MissedWin** — 178 kept declared, but the measured 178 model would reorder.
- **OverEager** — 178 reordered, but the measured 178 model would keep declared.
- **Indeterminate** — missing actuals or otherwise incomplete measurement.

Render recommendation labels are non-acting: `KEEP_APPLIED`, `WOULD_REORDER`,
`WOULD_KEEP_DECLARED`, and `NO_RECOMMENDATION`.

**D4 - Per-dimension estimate accuracy.** For each edge, report `actual_rows` vs
`estimated_rows` with a ratio and a label (accurate / over-estimate / under-estimate),
sourced from 179. This is the *why* behind the verdict.

**D5 - Render in the `EXPLAIN ANALYZE` cockpit.** Preserve 179's `join_order_runtime`
per-edge rows. Add one `join_order_verdict` summary row with the verdict class,
measured-applied order, measured action, recommendation, and per-dim accuracy detail. SQL
+ Rune share it. Do not widen the analyze schema unless the implementation proves it needs
new typed columns.

**D6 - Observational; the verdict does not act.** No re-plan, no revert, no margin change,
no fact write consumes the verdict. Results, plan, and 178's decision are unchanged. The
verdict is read-only output. (Distinct from the deferred adaptive/feedback acts.)

**D7 - The verdict orders the next act (named, not built).** Accrued verdicts tell us which
robustness rung to take: many **MissedWin** → the 178 margin is too conservative; many
**OverEager** → the margin is too permissive; many **MisRanked** → the estimate's ranking is
unreliable; catastrophic cases → adaptive revert. 180 produces the signal; acting on it is
deferred.

**D8 - Scope + reuse.** Broadcast inner star that 178 evaluated (reordered or kept), under
`EXPLAIN ANALYZE`. Reuses 178's `StarOrderEvidence` + 179's measured record + the analyze
cockpit. New: the verdict classification + accuracy + summary row. **Deferred:** acting on
the verdict; persisting/accruing verdicts across queries (always-on); hash/non-broadcast +
general-graph verdicts.

**D9 - No execution/fragment/storage surface.** Pure binder/render interpretation on 179's
existing capture. No executor, `fragment.rs`, Wendelon, storage, PBC, or wire change; no new
optimizer fact.

## Headline Gates

- `verdict_vindicated_when_measured_178_would_apply_same_order` - a star whose
  `applied_order` matches the measured 178 applied order → `Vindicated`.
- `verdict_misranked_when_reorder_picks_wrong` - a star 178 reordered whose
  `applied_order` differs from a measured 178 reorder → `MisRanked`, with the differing
  dims shown.
- `verdict_missed_win_when_margin_declined_a_real_win` - a star 178 kept declared, but the
  measured 178 model would reorder → `MissedWin` (the discriminating conservative-gate
  case).
- `verdict_overeager_when_measurement_would_keep_declared` - a star 178 reordered, but the
  measured 178 model would keep declared → `OverEager`.
- `verdict_is_interpretation_only` - results, bound plan, and 178's decision are byte-identical
  with and without verdict rendering; no new execution capture.

## Supporting Gates

- `verdict_reports_per_dim_estimate_accuracy` - each edge shows actual vs estimated + ratio +
  over/under label.
- `verdict_indeterminate_on_missing_actuals` - absent measurement → `Indeterminate`, no false
  verdict.
- `verdict_reuses_179_measured_and_178_decision` - no second estimator / no new capture; inputs
  are 178's evidence + 179's measured record.
- `verdict_uses_output_rows_not_build_or_probe_rows` - the measured 178 input is ADR 179's
  `actual_rows` / `output_rows`.
- `verdict_does_not_act` - no re-plan / revert / margin / fact mutation consumes the verdict.
- `cockpit_renders_verdict` - the analyze view shows verdict + measured order + accuracy;
  SQL + Rune parity.

## Architecture Guards

- `adr180_verdict_reuses_178_order_model_no_second_estimator` - the measured verdict calls
  the 178 star-order model and does not call cardinality/fact estimators.
- `adr180_measured_helper_sets_applied_order_override` - the measured evidence explicitly
  records the measured applied order instead of relying on the default declared-order
  override behavior.
- `adr180_verdict_compares_star_evidence_applied_order` - verdict comparison is original
  `applied_order` vs measured `applied_order`.
- `adr180_verdict_uses_runtime_output_rows` - measured scoring uses 179's `output_rows`,
  not build/probe rows.
- `adr180_verdict_is_interpretation_only` - the verdict path reads 178 evidence + 179
  measured and renders; it adds no execution capture and mutates no plan/decision.
- `adr180_verdict_does_not_act` - no re-plan/revert/margin/fact code consumes the verdict.
- `adr180_runtime_correlation_remains_replay_id_keyed` - actuals are correlated through
  `JoinReplayId`, not render order.
- `adr180_no_exec_or_fragment_change` - no executor/`fragment.rs`/Wendelon/storage/PBC
  change.
- `adr180_rune_and_sql_share_analyze_renderer` - Rune does not grow a separate verdict
  implementation.
- `adr180_no_optimizer_shell` - 177's no-cathedral guard still holds.

## Non-Goals

- **no acting on the verdict** — no adaptive re-order/revert, no margin auto-tune, no fact
  feedback (each is a named follow-on the verdict will justify);
- no persisting/accruing verdicts across queries (always-on telemetry — deferred);
- no hash/non-broadcast or general-graph verdicts;
- no new optimizer fact, executor, `fragment.rs`, or wire surface.

## Consensus Trail

(iter-2, council-selected.) The council chose the verdict (observe) over the iter-1
reversion (act): we have 179's measurement tool but no accrued judgment of 178's accuracy,
so the disciplined rung is to *judge* the decision before acting on it. 180 is pure
interpretation — substitute measured actuals into the existing 178 model, classify 178's
decision (Vindicated / MisRanked / MissedWin / OverEager), report per-dim accuracy —
observational, reusing 178's evidence + 179's measurement + the analyze cockpit. The
accrued verdicts then choose the next robustness act by evidence.

## Forward Arc

`179 measure -> 180 measured verdict -> {the verdict picks the act: margin auto-tune
(MissedWin/OverEager) / fact feedback or adaptive re-order (MisRanked) / revert
(catastrophic)} -> hash-non-broadcast star -> snowflake/chain -> bounded enumeration ->
outer-join -> relax left-deep -> bridges -> cycles -> memo`.

After 180 the loop is fully legible: 178 decides, 179 measures, 180 judges. The next act is
no longer a guess — the verdicts say which one the data demands.

## Deferred (named)

Acting on the verdict (margin auto-tune, fact feedback, adaptive re-order/revert); persisting
verdicts across queries (always-on accrual); hash/non-broadcast + general-graph verdicts.
