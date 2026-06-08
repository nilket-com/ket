# ADR 191: Opt-In Advisory-Applied Soft Margin

**Status:** Promoted. **Migration:** `plans/impl/191_opt_in_advisory_applied_soft_margin.md`.

ADR 190 made the join-order soft margin an operator-applied ladder step. ADR 191 adds the smallest
safe self-tuning rung: an explicit application mode where the optimizer may apply the ADR 189
advisory recommendation as the effective soft margin for a bind.

The cut is default-dark. Manual mode remains the default and preserves ADR 190 behavior.
Advisory application is opt-in, bounded to the one-step recommendation already produced by
`recommend_soft_margin`, resolved once at bind entry, and reported visibly as configured,
recommended, and applied margin values.

## Grounding

- `PlannerConfig` already owns `join_order_soft_margin_step: SoftMarginLadderStep`, defaulting to
  `CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP`.
- `recommend_soft_margin(snapshot, current_step)` already computes `recommended_step` internally
  from ADR 189's min-sample and hysteresis rules. It currently stores only the rendered
  `SoftMargin` values in `MarginAdvisory`.
- Star actuation reads `self.config.join_order_soft_margin_step.margin()` in
  `binder/join_order.rs`; observed EXPLAIN paths pass `config.join_order_soft_margin_step` into
  `analyze_snowflake` and `analyze`; the advisory system table calls
  `recommend_soft_margin(&self.config.decision_log.verdict_snapshot(),
  self.config.join_order_soft_margin_step)`.
- `OptimizerDecisionLog` is shared state. An auto-applied bind must not take multiple live
  `verdict_snapshot()` reads and risk using different margins in different join-order seams.
- `information_schema.optimizer_margin_advisory` currently exposes `current_margin`,
  `recommended_margin`, and sample counters. Under 191 there are two distinct "current" concepts:
  the configured baseline and the margin applied to this bind/report.
- ADR 190's `adr190_operator_applied_not_auto` guard intentionally forbids self-derived margins.
  ADR 191 must retarget that guard instead of deleting it.

## Decisions

**D1 - Typed application mode, default Manual.** Add a public config enum:

```rust
pub enum JoinOrderSoftMarginApplication {
    Manual,
    ApplyAdvisory,
}
```

Add `join_order_soft_margin_application: JoinOrderSoftMarginApplication` to `PlannerConfig`.
Default is `Manual`. Do not use a bool or stringly mode.

**D2 - Advisory input remains the configured baseline.** `recommend_soft_margin` continues to take
the operator-configured base step as `current`. `MarginAdvisory` exposes both `current_step` and
`recommended_step`, while preserving the existing `current` and `recommended` margin fields.

This avoids the double-step bug: with baseline `4/5` and a loosen recommendation, the report must
show `current_margin=4/5`, `recommended_margin=9/10`, and `applied_margin=9/10`, not recompute from
`9/10` and render `19/20`.

**D3 - Effective step is resolved once at bind entry.** Add a pure resolver:

```text
Manual:
  effective = config.join_order_soft_margin_step

ApplyAdvisory:
  advisory = recommend_soft_margin(verdict_snapshot, config.join_order_soft_margin_step)
  effective = advisory.recommended_step
```

`bind_with_trace` and `bind_with_explain_trace` construct the binder with that resolved step before
planning starts. Later join-order seams in the same bind read the stored effective step, not the
live config field and not another verdict snapshot.

**D4 - All join-order decision and observation paths consume the effective step.** Star actuation,
observed star evidence, observed snowflake evidence, and measured ranking/verdict classification
use the same resolved `SoftMarginLadderStep`. Manual mode makes that value equal to the configured
ADR 190 step.

**D5 - The advisory report shows configured, recommended, mode, and applied.** Append columns to
`information_schema.optimizer_margin_advisory`:

- `application_mode` (`manual` or `apply_advisory`);
- `applied_margin`.

Existing columns keep their meaning: `current_margin` is the configured baseline supplied to the
advisory; `recommended_margin` is the ADR 189 recommendation from that baseline. In Manual mode,
`applied_margin = current_margin`. In ApplyAdvisory mode,
`applied_margin = recommended_margin`.

**D6 - Bounded, derived, and non-mutating.** ApplyAdvisory applies exactly the advisory
`recommended_step`, which is at most one ladder step from the configured baseline. It does not
mutate `PlannerConfig`, does not write verdict rows or margin state, and does not persist a tuned
step.

**D7 - Default reproducibility, explicit history dependence.** Manual default preserves 190's
history-independent planning. ApplyAdvisory explicitly accepts that the same query may bind
differently as runtime verdict history changes.

**D8 - Retarget 190's not-auto guard.** `adr190_operator_applied_not_auto` becomes: no in-place
assignment to `join_order_soft_margin_step`, no decision-log-owned margin state, and the only
allowed derived margin is the opt-in effective-step resolver.

**D9 - No new optimizer surface.** This is binder/config/report/test work only. No executor,
fragmenter, storage, PBC, Wendelon, memo, Cascades, optimizer shell, or `PlanProperties` contract.

## Headline Gates

- `auto_applied_margin_uses_advisory_recommendation` - with base `Baseline` and a
  `MissedWin`-dominant verdict log, ApplyAdvisory uses the `Loose` recommendation and reorders a
  borderline soft star that Manual baseline keeps.
- `manual_application_mode_is_190_byte_identical` - with the same verdict log, Manual mode
  produces the same bound plan and receipts as ADR 190.
- `advisory_report_does_not_double_step_auto_application` - with base `4/5` and a loosen
  recommendation, the advisory row shows `current_margin=4/5`, `recommended_margin=9/10`,
  `application_mode=apply_advisory`, `applied_margin=9/10`, and never `19/20`.

## Supporting Gates

- `auto_application_insufficient_evidence_uses_configured_margin` - below
  `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE`, ApplyAdvisory applies the configured base.
- `auto_application_hysteresis_keep_uses_configured_margin` - balanced verdicts inside the
  hysteresis band apply the configured base.
- `auto_application_tightens_on_overeager` - an `OverEager`-dominant log applies the tighter
  recommended step.
- `auto_application_is_bounded_to_one_step` - even an extreme log moves at most one ladder step
  from the configured base.
- `auto_application_does_not_mutate_config_or_log` - resolving/applying the margin leaves
  `PlannerConfig.join_order_soft_margin_step` unchanged and does not write verdict rows or margin
  state. A normal bind may still record ordinary optimizer decisions.
- `observed_snowflake_uses_auto_applied_margin` - EXPLAIN-only snowflake observation renders the
  auto-applied soft margin when ApplyAdvisory is enabled.
- `rune_respects_auto_applied_margin` - Rune-authored plans use the same planner config path and
  therefore the same effective margin.

## Architecture Guards

- `adr191_margin_application_mode_is_typed` - `PlannerConfig` uses
  `JoinOrderSoftMarginApplication`, default `Manual`, with no `join_order_margin_auto_tune: bool`.
- `adr191_effective_margin_resolved_once_at_bind_entry` - binder construction stores a resolved
  `SoftMarginLadderStep`; join-order code reads that field instead of repeatedly reading
  `self.config.join_order_soft_margin_step` or `verdict_snapshot()`.
- `adr191_auto_apply_reuses_189_policy_only` - the resolver calls `recommend_soft_margin` and
  returns `recommended_step` under `ApplyAdvisory`; there is no new margin policy.
- `adr191_report_preserves_configured_recommended_and_applied_margins` - the advisory schema keeps
  existing columns and appends `application_mode` and `applied_margin`.
- `adr191_retargets_190_operator_applied_not_auto` - no reassignment to
  `join_order_soft_margin_step`; the single opt-in resolver is the only allowed derived path.
- `adr191_default_manual_preserves_190` - default config is Manual and the default step remains
  `CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP`.
- `adr191_no_executor_fragmenter_storage_or_wendelon_change` - new markers are absent from
  executor, distributed fragmenter/scheduler, storage, PBC, and Wendelon.
- `adr191_no_optimizer_shell_or_planproperties_contract` - no memo, Cascades, optimizer shell, or
  broad property contract appears.

## Rejected Alternatives

**Boolean `join_order_margin_auto_tune`.** Mechanically sufficient, but it bakes a two-state API
into a topic that is explicitly about operator modes. A typed enum makes invalid future states less
representable and keeps later modes honest.

**Make `current_margin` mean the auto-applied margin.** This hides the operator baseline and can
lead to a double-step advisory report if the recommendation is recomputed from the already-applied
step. Keep configured, recommended, and applied separate.

**Mutable live tuning state.** An `Arc<Atomic>` or background coordinator would introduce cadence,
rollback, staleness, and persistence policy. 191 is a pure per-bind resolution.

**Iterative convergence.** Repeatedly walking the ladder can drift away from the operator baseline.
191 applies the one-step recommendation from the configured base only.

**Always-on advisory application.** History-dependent planning is an explicit tradeoff, not the
default.

## Non-Goals

- no always-on self-tuning;
- no multi-step or persistent ratchet;
- no tuned-step persistence across restart;
- no per-kind margins;
- no new runtime verdict policy;
- no executor, fragmenter, scheduler, storage, PBC, Wendelon, or catalog surface;
- no cost model, memo, Cascades, optimizer shell, or `PlanProperties`;
- no broadcast-snowflake execution unlock.

## Consensus Trail

ADR 191 completes the first feedback loop without making adaptive plans the default:

```text
187 decisions -> 188 runtime verdicts -> 189 advisory -> 190 operator-applied margin
              -> 191 opt-in advisory-applied margin
```

The optimizer can now consume its own measured advisory, but only under an explicit typed mode,
with the effective margin resolved once per bind, bounded to one ladder step, and visible in the
cockpit.

## Forward Arc

The margin loop is closed after 191. The next optimizer work should be selected from workload or
execution pressure, not from an unfinished margin rung. The strongest named candidate remains the
broadcast-snowflake execution cliff, followed by mixed-strategy cost modeling and broader topology
work when receipts show local decisions fighting.

## Deferred (named)

Broadcast-snowflake execution baseline; mixed-strategy cost model; galaxy/bridge topology;
multi-step auto-convergence; tuned-step persistence; per-kind margin policies.
