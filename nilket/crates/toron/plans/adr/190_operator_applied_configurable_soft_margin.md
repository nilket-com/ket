# ADR 190: Operator-Applied Configurable Soft Margin

**Status:** Promoted. **Migration:** `plans/impl/190_operator_applied_configurable_soft_margin.md`.

189 made the optimizer *recommend* a soft margin from accrued verdicts (read-only). 190 makes the
margin **applicable**: it promotes `STAR_ORDER_SOFT_MARGIN_*` from a const to a `PlannerConfig`
value (a bounded ladder step, default 4/5), threads it into the reorder decision, records the
applied margin in ranking/evidence receipts, and reflects it in the advisory. The 189 advisory
recommends; the **operator applies** by setting the config. Plans stay **reproducible**
(operator-controlled, like any setting); the optimizer does not self-modify -- auto-apply is the
deferred 191.

Behavior-neutral by default: the config defaults to the 4/5 step, so 177-189 plans are
byte-identical. This is the reproducible "act" on 189's advisory, fully validatable in-council.

## Grounding

- **A single margin gate, reached by a bounded path.** The margin ratio is applied in exactly
  one place -- `classify_star_order_action` (`binder/join_order.rs`) -- called from
  `rank_star_order`, itself used by star actuation, explain-only star/snowflake observation, and
  the measured verdict re-rank. One gate means threading a margin parameter is bounded, but every
  classifier consumer must receive the configured step.
- **The ladder already bounds safe values.** 189 defined `SoftMarginLadderStep` (`3/5 ... 19/20`) +
  `SoftMargin`. Making the config a `SoftMarginLadderStep` means the operator can only select a
  *safe* margin -- no arbitrary ratios -- and the advisory recommends in the same units.
- **Public config needs a public step type.** `PlannerConfig` is a public API. A public field
  cannot expose a crate-private ladder type. 190 must make the ladder step public/re-exported (or
  introduce a public alias) while keeping the raw `SoftMargin` numerator/denominator private.
- **Render has no config; the receipt must carry the margin.** `render_explain_analyze(binding,
  observations)` takes no config, and the 188 accrual verdict path computes verdicts from the
  binding alone. So the applied margin must be **recorded in the evidence/receipt** at decision
  time, and the verdict re-rank + the displayed margin read it from there -- keeping
  decision/verdict/display consistent without threading config into render.
- **Snowflake evidence stores rankings directly.** `SnowflakeOrderEvidence` owns a
  `StarOrderRanking`, not a `StarOrderEvidence`. If only flat-star evidence carries the applied
  margin, observed snowflake detail and snowflake measured verdicts can drift back to the old
  const. The applied margin must live on `StarOrderRanking` too.
- **189's advisory already reads a "current" margin.** With 190, "current" is the applied config
  margin (was the const); the advisory recommends a step relative to it, and the operator applies
  by setting the config to the recommended step.
- **189's const-only guard intentionally inverts.** `adr189_advisory_does_not_change_margin_or_decision`
  currently asserts the classifier still reads `STAR_ORDER_SOFT_MARGIN_*`. 190 must retarget that
  guard to the new configured-margin invariant rather than leaving a stale architecture failure.

## Decisions

**D1 - The margin becomes a bounded config value.** Add
`join_order_soft_margin_step: SoftMarginLadderStep` (or a public alias with that meaning) to
`PlannerConfig`, default the 4/5 step (behavior-neutral). The operator sets it to any ladder step;
off-ladder ratios are not representable. The ladder step is public/re-exported for config; the
raw `SoftMargin` ratio stays private.

**D2 - Thread the resolved margin into every classifier consumer.** `classify_star_order_action`
takes the derived private `SoftMargin`; `rank_star_order`, `recognized_star_order_outcome`,
`analyze`, and `analyze_snowflake` take the configured ladder step and derive the private margin
inside `join_order.rs`. Star actuation and explain-only observation pass
`config.join_order_soft_margin_step`. The `STAR_ORDER_SOFT_MARGIN_*` consts remain only as the
baseline/default definition, not read in the decision path.

**D3 - Record the applied margin in ranking and receipt.** `StarOrderRanking` and
`StarOrderEvidence` carry the applied `SoftMargin`. `recognized_star_order_outcome` copies it from
the ranking; `SnowflakeOrderEvidence` gets it through its ranking. The measured verdict re-rank
reads it (so the verdict uses the same margin the decision used), and explain detail displays it
(was the const). Render needs no config.

**D4 - The advisory reflects the applied margin.** 189's `recommend_soft_margin` takes the applied
config ladder step as "current"; the advisory's `current_margin` is the live config value and
`recommended_margin` is a step relative to it. The operator closes the loop by setting the config
to the recommendation.

**D5 - Behavior-neutral default.** Default = the 4/5 step, so the margin value is identical and 177-189 plans
and verdicts byte-identical. Pinned by the full suites staying green.

**D6 - Operator-applied, reproducible; not auto.** The margin changes only when the operator sets
the config: plans are reproducible and history-independent. No accrued verdict feeds the margin;
the optimizer does not self-modify. Auto-apply (the optimizer setting its own margin from the 189
advisory) is the named, opt-in, deployment-gated follow-on (191).

**D7 - No executor/storage/Wendelon change.** The cut is the config value + decision threading +
the receipt field + the advisory/display reflecting the applied margin. No `fragment.rs`,
executor, storage, PBC, Wendelon, cost model, optimizer shell, memo, Cascades, or broad
`PlanProperties` contract.

## Headline Gates

- `operator_set_margin_changes_reorder_threshold` - a borderline star that reorders at the default
  4/5 step keeps declared under a tighter step (3/5) and still reorders under a looser step (9/10);
  the config drives the decision.
- `default_margin_is_behavior_neutral` - with the default config, the bound `ExecutionPlan` and the
  178/181/185/186 actions are byte-identical to pre-190 (the const value unchanged).
- `applied_margin_recorded_and_displayed` - ranking/evidence carry the applied margin, explain
  detail shows it (not the const), and the measured verdict re-rank uses it.

## Supporting Gates

- `operator_margin_bounded_to_ladder` - `join_order_soft_margin_step` accepts only ladder steps; an
  off-ladder ratio is not constructible.
- `verdict_uses_applied_margin` - a query decided under a tighter applied margin produces a verdict
  consistent with that margin (the re-rank reads the receipt's margin, not the const).
- `advisory_current_is_applied_margin` - the 189 advisory's `current_margin` equals the config
  margin, and `recommended_margin` is relative to it.
- `observed_snowflake_uses_configured_margin` - explain-only snowflake analysis records and renders
  the configured margin through `StarOrderRanking`.
- `rune_respects_operator_margin` - Rune honors the configured margin identically to SQL.

## Architecture Guards

- `adr190_margin_is_config_not_const` - `classify_star_order_action`/`rank_star_order` take the
  margin as a parameter; the decision path does not read `STAR_ORDER_SOFT_MARGIN_*` directly (only
  the default definition does).
- `adr190_applied_margin_recorded_in_ranking_and_evidence` - `StarOrderRanking` and
  `StarOrderEvidence` carry the applied margin; the verdict re-rank and explain detail read it,
  not the const.
- `adr190_observed_paths_receive_configured_margin` - `bind_with_explain_trace` passes the config
  margin into both `analyze` and `analyze_snowflake`.
- `adr190_advisory_current_is_configured_margin` - the advisory bind passes
  `self.config.join_order_soft_margin_step`, not `CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP`.
- `adr190_margin_bounded_to_ladder` - the config margin is a public/re-exported
  `SoftMarginLadderStep` (safe range), not raw ratio fields.
- `adr190_operator_applied_not_auto` - the margin is set only from config; no accrual/verdict path
  writes `join_order_soft_margin_step` (auto-apply is 191).
- `adr190_retargets_adr189_const_guard` - 189's const-only classifier guard is replaced with the
  configured-margin invariant.
- `adr190_no_executor_storage_or_wendelon_change` - no executor, `fragment.rs`, storage, PBC, or
  Wendelon change.
- `adr190_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.

## Rejected Alternatives

**Auto-apply now.** Introduces history-dependent plans and a deployment-gated payoff; 190 keeps
plans reproducible (operator-set) and validatable in-council, and de-risks 191.

**Arbitrary (non-ladder) margin ratios.** A free ratio lets the operator set an unsafe margin and
breaks the advisory's one-step semantics. The `SoftMarginLadderStep` bounds operator and advisory
to the same safe ladder.

**Thread config into render.** `render_explain_analyze` has no config, and the accrual verdict path
computes from the binding alone; recording the applied margin in the receipt keeps the verdict and
display consistent without a new config thread into render.

**Per-kind margins.** One global margin keeps the operator model simple and matches 181/186's
shared gate; per-kind tuning is a later refinement if evidence warrants.

## Non-Goals

- no auto-apply / self-tuning (191, opt-in, deployment-gated);
- no off-ladder margin values;
- no per-kind or per-query margins;
- no executor, fragment, storage, PBC, wire, Wendelon, or catalog change;
- no cost model, optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Consensus Trail

190 is the reproducible act on 189's advisory: the soft margin becomes a bounded config value
(`SoftMarginLadderStep`, default 4/5), threaded into the reorder decision and recorded in the
receipt so the verdict and display stay consistent without threading config into render. The
advisory recommends; the operator applies by setting the config. Plans stay reproducible and
history-independent; the optimizer does not self-modify. Behavior-neutral by default, fully
validatable in-council. Auto-apply is the earned, opt-in, deployment-gated 191.

## Forward Arc

`187 decisions -> 188 verdicts -> 189 advisory -> 190 operator-applied margin -> 191 auto-apply
(opt-in, history-dependent, deployment-gated)`. After 190 the robustness loop is closed on the
operator-controlled side; 191 (auto) and the topology/executor bets want deployed evidence - a
natural chapter boundary.

## Deferred (named)

Auto-apply self-tuning (191); per-kind margins; off-ladder margins; the broadcast-snowflake cliff;
mixed-strategy cost model; galaxy/bridge topology.
