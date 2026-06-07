# ADR 184: Snowflake-Path Measured Verdict Receipts

**Status:** Promoted. **Migration:** `plans/impl/184_snowflake_path_measured_verdict_receipts.md`.

182 made a snowflake's join order visible from estimates. 183 measured the actual output rows
for those declared snowflake edges under `EXPLAIN ANALYZE`. 184 judges those measurements:
for each snowflake level, rerank the level using measured rows, decide whether the declared
order was vindicated or left a measured win, and roll the level verdicts up to one snowflake
receipt.

The design principle for this cut: make impossible snowflake states unrepresentable. A
snowflake has not been actuated, so it cannot be `MisRanked` or `OverEager` in the ADR 180
star sense. Those variants should not exist in the snowflake verdict type.

## Grounding

- **183 already produces the measurements.** The snowflake analyze branch emits one
  `join_order_snowflake_runtime` row per snowflake edge, correlated by
  `JoinReplayId`/`JoinTraceTarget`. The same loop can accumulate
  `measured_rows_by_dimension` from `JoinRuntimeObservationSummary.output_rows`.
- **182 already gives level-local estimated rankings.** `SnowflakeOrderEvidence` is recursive:
  each level has a `StarOrderRanking` plus `edge_strategies` and child branches. The verdict
  can walk that tree depth-first and judge each level independently.
- **The reusable part of 180 is measured reranking, not star verdict classification.** ADR 180
  substitutes measured rows into dimension evidence and reruns the 178 order model. That
  measured-rerank step is shared. ADR 180's final classifier compares against an applied star
  order, so it is star-specific.
- **Snowflake is observe-only.** No snowflake reorder has run. The actual order is declared
  order at every level. Therefore the snowflake question is only: measured rows keep declared,
  measured rows would reorder, or measurement is incomplete.
- **Measurements are declared-edge outputs.** ADR 183 did not execute counterfactual isolated
  subplans. ADR 184 is an interpretation of measured declared-edge outputs, not proof that a
  hypothetical reordered snowflake would have exactly those intermediate rows.

## Decisions

**D1 - Extract a shared measured-rerank helper.** Factor the measured-substitution work out of
`measured_star_order_verdict`: given a `StarOrderRanking` and a
`dimension -> measured_output_rows` map, build per-dimension accuracy, replace estimates with
actuals, rerun `rank_star_order`, and derive the measured applied order from the measured
action. The existing flat-star helper keeps its ADR 180 behavior by calling this helper and
then applying the star-specific classifier.

**D2 - Snowflake gets its own verdict type.** Add a snowflake verdict enum with only the
valid observe-only outcomes:

- `Vindicated` - measured rows keep declared order.
- `MissedWin` - measured rows would reorder away from declared order.
- `Indeterminate` - required measurement is missing.

Do not reuse `StarOrderVerdict` for snowflake receipts. `MisRanked` and `OverEager` require
an applied optimizer order and cannot be represented for ADR 184.

**D3 - Per-level verdicts are mandatory.** Emit one level verdict for the root level and each
branch level. A snowflake can have a vindicated root and a missed-win branch, or the reverse.
The level is keyed by the level probe label and carries the original 182 estimated ranking,
the measured rerank, the real per-edge strategies, and the snowflake verdict.

**D4 - Aggregate verdict is worst-case meaningful.** Emit one aggregate
`join_order_snowflake_verdict` row:

- any `Indeterminate` level -> aggregate `Indeterminate`;
- else any `MissedWin` level -> aggregate `MissedWin`;
- else aggregate `Vindicated`.

Recommendations are snowflake-specific too: `KEEP_DECLARED`, `WOULD_REORDER`, and
`NO_RECOMMENDATION`.

**D5 - Receipt rows are additive and snowflake-specific.** Preserve ADR 182's
`join_order_snowflake` row and ADR 183's `join_order_snowflake_runtime` rows. Append
`join_order_snowflake_verdict` rows. Do not emit the flat-star `join_order_verdict` kind for
snowflakes.

**D6 - Render real snowflake evidence.** Per-level detail renders:

- `estimated_action` from the 182 ranking;
- declared order and estimated preferred order;
- measured action, measured applied order, and measured costs;
- recommendation and accuracy;
- real per-edge snowflake strategies from `SnowflakeEdgeStrategy`.

No coerced adapter action or fake uniform strategy exists to render.

**D7 - Observational invariant.** No reorder, no recursive binder build, no plan mutation, no
margin change, no fact write, and no execution capture change. The verdict reads ADR 182
evidence and ADR 183 measurements and renders receipts.

**D8 - Frontend-shared and binder/render scoped.** SQL `EXPLAIN ANALYZE` and Rune `analyze()`
share the same optimizer analyze renderer. No executor, `fragment.rs`,
`rewrite_broadcast_star_probe`, storage, PBC, Wendelon, catalog, optimizer shell, memo,
Cascades, or broad `PlanProperties` contract.

## Headline Gates

- `snowflake_verdict_reports_per_level_and_aggregate` - a snowflake whose branch has a
  measured reorder opportunity emits a branch `MissedWin`, a root `Vindicated`, and an
  aggregate `MissedWin`.
- `measured_snowflake_order_verdict_aggregates_worst_case` - the aggregation unit test: all
  measured-keep levels roll up to `Vindicated`, any measured-reorder level to `MissedWin`, any
  missing measurement to `Indeterminate` (with the matching recommendation each time).
- `snowflake_verdict_type_excludes_star_actuation_only_outcomes` - snowflake verdict code has
  no `MisRanked` or `OverEager` state to emit.

## Supporting Gates

- `snowflake_level_estimate_wanted_reorder_judges_against_declared` - a level whose estimate
  wanted reorder still judges against declared execution: measured keep -> `Vindicated`,
  measured reorder -> `MissedWin`.
- `snowflake_verdict_detail_keeps_estimate_and_measurement_separate` - a level with
  `estimated_action=reordered` renders that estimate separately from measured action and
  measured applied order.
- `snowflake_verdict_missed_win_recommends_would_reorder` - a missed-win level and aggregate
  carry `WOULD_REORDER`.
- `snowflake_explain_analyze_emits_plan_runtime_and_verdict` - snowflake analyze output
  contains `join_order_snowflake`, `join_order_snowflake_runtime`, and
  `join_order_snowflake_verdict`, and emits **no** flat `join_order_verdict`/`join_order_runtime`
  rows (the no-flat-star-verdict boundary).
- the ADR 180 star verdict suite (`verdict_vindicated_when_measured`,
  `verdict_overeager_when_measurement_would_keep_declared`, `verdict_misranked_when_reorder_picks_wrong`,
  `verdict_missed_win_when_margin_declined_a_real_win`, `verdict_indeterminate_on_missing_actuals`)
  stays green after the shared-rerank extraction (flat-star verdicts unchanged).
- `rune_analyze_shows_snowflake_verdict` - Rune parity through the shared renderer.

## Architecture Guards

- `adr184_measured_rerank_is_shared_but_classifiers_are_separate` - star and snowflake share
  the measured-rerank helper; snowflake does not call `measured_star_order_verdict`.
- `adr184_snowflake_verdict_enum_has_only_observe_outcomes` - the snowflake enum contains only
  `Vindicated`, `MissedWin`, and `Indeterminate`.
- `adr184_snowflake_verdict_uses_183_output_rows` - snowflake measured scoring uses
  `JoinRuntimeObservationSummary.output_rows`, not build/probe rows.
- `adr184_snowflake_verdict_reuses_182_tree` - verdict recursion walks
  `SnowflakeOrderEvidence.branches`; no second topology recognizer.
- `adr184_snowflake_verdict_detail_renders_real_evidence` - detail uses 182 estimated action
  and `SnowflakeEdgeStrategy`, not flat-star detail rendering.
- `adr184_snowflake_verdict_is_observational` - no reorder; verdict code is read-only over
  render trace and summaries.
- `adr184_no_executor_fragment_costmodel_or_wendelon_change` - no executor, `fragment.rs`,
  `rewrite_broadcast_star_probe`, cost model, storage, PBC, wire, or Wendelon change.
- `adr184_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.
- `adr184_cockpit_is_frontend_shared` - SQL and Rune render through the same path.

## Rejected Alternatives

**Coerce `StarOrderEvidence` into an un-actuated adapter and call `measured_star_order_verdict`.**
Rejected. It is smaller, but it creates an intentionally distorted value: the estimate may
have wanted reorder while the adapter action says Kept. That violates the "make invalid states
unrepresentable" principle and risks leaking adapter fields into receipts or future helpers.

**Call `measured_star_order_verdict` per level unchanged.** Rejected. Its classifier assumes
an applied star decision. Snowflake has no applied optimizer reorder, so star-only
`MisRanked`/`OverEager` outcomes are semantically invalid.

**Invent a new cost model.** Rejected. The measured judgement must reuse the 178 ranking and
soft-margin semantics so the verdict compares like with like.

**Aggregate-only verdict.** Rejected. It hides which snowflake level earned the next act.

**Fold in actuation.** Rejected. ADR 184 exists to justify or refuse ADR 185 with measured
evidence.

## Non-Goals

- no snowflake actuation/reorder;
- no broadcast-snowflake execution relaxation;
- no galaxy / bridge / cyclic recognition;
- no always-on verdict accrual / cross-query margin self-tuning;
- no new optimizer fact, observation type, second estimator, or cost model;
- no executor capture, fragment, storage, PBC, wire, catalog, or Wendelon change;
- no optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Consensus Trail

Judgement Day is the snowflake sub-arc's verdict rung: 182 evidence, 183 measurements, 184
judgement, then 185 actuation only if measured MissedWins justify it. The load-bearing
correction is not to fake an un-actuated star. Snowflake gets its own observe-only verdict
type, while sharing the measured-rerank mechanics with ADR 180. That makes the impossible
states impossible instead of guarded by convention.

## Forward Arc

`182 plan evidence -> 183 runtime evidence -> 184 measured verdict -> 185 hash-snowflake order
actuation (binder-only recursive build, earned by 184's measured MissedWins) ->
broadcast-snowflake execution cliff (rewrite_broadcast_star_probe) -> galaxy/multi-fact ->
bridge/many-to-many -> cyclic caps -> memo only when receipts show decisions fighting`.

## Deferred

Hash-snowflake order actuation (185); the broadcast-snowflake execution cliff
(`rewrite_broadcast_star_probe`); galaxy / bridge / cyclic recognition; always-on verdict
accrual / cross-query margin self-tuning; strategy-aware cost model.
