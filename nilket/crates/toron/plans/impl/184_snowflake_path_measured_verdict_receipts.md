# Impl Plan - ADR 184: Snowflake-Path Measured Verdict Receipts

Pairs with `plans/adr/184_snowflake_path_measured_verdict_receipts.md`.

Judge what ADR 183 measured without faking an actuated star. Extract the measured-rerank
mechanics from ADR 180, keep the flat-star classifier for stars, add snowflake-specific
observe-only verdict types, and render per-level + aggregate `join_order_snowflake_verdict`
rows. No reorder, no new measurement substrate, no executor / fragment / Wendelon change.

## Anchors

| Concern | Anchor |
|---|---|
| Star measured verdict | `measured_star_order_verdict` (`binder/join_order.rs`) |
| Shared ranking model | `rank_star_order`, `classify_star_order_action`, `order_cost` |
| Snowflake evidence | `SnowflakeOrderEvidence { ranking, edge_strategies, branches }` |
| Runtime rows | snowflake branch of `optimizer_analyze_rows` (`binder/explain.rs`) |
| Runtime correlation | `join_replay_id_for_trace_target(edge.target)` + `JoinRuntimeObservationSummary.output_rows` |
| Rendering helpers | `star_order_action_label`, `star_order_dim_accuracy_label`, strategy labels |
| Tests | `tests/explain_analyze_test.rs`; `binder/tests.rs`; `tests/architecture/join_order.rs` |

## Phase 1 - Extract measured rerank (`binder/join_order.rs`)

Factor the measured-substitution half of `measured_star_order_verdict` into a helper that
does not classify a verdict:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MeasuredOrderRerank {
    pub(super) measured_applied_order: Vec<String>,
    pub(super) accuracy: Vec<StarOrderDimAccuracy>,
    pub(super) measured_ranking: Option<StarOrderRanking>,
}

pub(super) fn measured_order_rerank(
    ranking: &StarOrderRanking,
    measured_rows_by_dimension: &BTreeMap<String, RowCount>,
) -> MeasuredOrderRerank { ... }
```

Rules:

- Populate `accuracy` exactly like ADR 180: original estimated rows and optional measured rows.
- If any dimension is missing measurement, return `measured_ranking = None`,
  `measured_applied_order = []`, and the accuracy list.
- Otherwise replace each dimension's `estimated_rows` with measured `output_rows`, preserving
  declared position, source, and confidence.
- Call `rank_star_order(ranking.probe.clone(), measured_dimensions, ranking.confidence)`.
- Derive `measured_applied_order` from measured action:
  - measured `Reordered` -> measured `cost_preferred_order`;
  - measured keep -> measured `declared_order`.

Keep `measured_star_order_verdict` behavior byte-identical by adapting the new helper back to
`StarOrderEvidence` and then applying the existing ADR 180 star classifier. The helper is the
shared mechanism; each caller derives its own recommendation from its own verdict semantics.

## Phase 2 - Add snowflake verdict types (`binder/join_order.rs`)

Add types that cannot represent star-only outcomes:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SnowflakeOrderVerdictKind {
    Vindicated,
    MissedWin,
    Indeterminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SnowflakeOrderRecommendation {
    KeepDeclared,
    WouldReorder,
    NoRecommendation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeOrderVerdict {
    pub(super) levels: Vec<SnowflakeLevelVerdict>,
    pub(super) aggregate: SnowflakeOrderVerdictKind,
    pub(super) recommendation: SnowflakeOrderRecommendation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeLevelVerdict {
    pub(super) probe: String,
    pub(super) estimated_ranking: StarOrderRanking,
    pub(super) strategies: Vec<SnowflakeEdgeStrategy>,
    pub(super) rerank: MeasuredOrderRerank,
    pub(super) verdict: SnowflakeOrderVerdictKind,
    pub(super) recommendation: SnowflakeOrderRecommendation,
}

pub(super) fn measured_snowflake_order_verdict(
    evidence: &SnowflakeOrderEvidence,
    measured_rows_by_dimension: &BTreeMap<String, RowCount>,
) -> SnowflakeOrderVerdict { ... }
```

Per-level classification:

- `rerank.measured_ranking.is_none()` -> `Indeterminate` / `NoRecommendation`;
- measured ranking action `Reordered` -> `MissedWin` / `WouldReorder`;
- measured ranking action keeps declared -> `Vindicated` / `KeepDeclared`.

Aggregate rollup:

- any `Indeterminate` -> aggregate `Indeterminate` / `NoRecommendation`;
- else any `MissedWin` -> aggregate `MissedWin` / `WouldReorder`;
- else `Vindicated` / `KeepDeclared`.

Walk the evidence tree depth-first: root first, then branches in existing evidence order.

## Phase 3 - Emit verdict rows (`binder/explain.rs`)

Update the snowflake branch of `optimizer_analyze_rows`:

- preserve `optimizer_analyze_snowflake_order_row(evidence)`;
- preserve ADR 183 runtime rows;
- while producing runtime rows, accumulate `measured_rows_by_dimension` from
  `summary.output_rows`, mirroring the flat-star branch;
- call `measured_snowflake_order_verdict(evidence, &measured_rows_by_dimension)`;
- append one per-level `join_order_snowflake_verdict` row and one aggregate row;
- return before the flat-star branch.

Do not emit `join_order_verdict` for snowflakes.

## Phase 4 - Render snowflake-specific details (`binder/explain.rs`)

Add helpers:

```rust
fn snowflake_order_verdict_label(verdict: SnowflakeOrderVerdictKind) -> &'static str { ... }
fn snowflake_order_recommendation_label(recommendation: SnowflakeOrderRecommendation) -> &'static str { ... }

fn optimizer_analyze_snowflake_verdict_row(
    level: &SnowflakeLevelVerdict,
) -> Result<Vec<ScalarValue>, BindingError> { ... }

fn optimizer_analyze_snowflake_aggregate_verdict_row(
    verdict: &SnowflakeOrderVerdict,
) -> Vec<ScalarValue> { ... }
```

Per-level row shape, using the existing 10-column analyze schema:

- `decision_kind`: `join_order_snowflake_verdict`
- `target`: `level=<probe>`
- `outcome`: `vindicated`, `missed_win`, or `indeterminate`
- `detail`: `estimated_action`, `measured_action`, `recommendation`,
  declared order, estimated preferred order, measured applied order, estimated costs,
  measured costs, accuracy list, and real per-edge snowflake strategies
- typed metric columns: `None`

Aggregate row:

- `decision_kind`: `join_order_snowflake_verdict`
- `target`: `snowflake`
- `outcome`: aggregate verdict label
- `detail`: recommendation plus `levels=[<probe>:<verdict>, ...]`

Do not call `star_order_verdict_detail_label` for snowflake rows.

## Phase 5 - Retarget ADR 183 gates

Retarget tests that asserted "runtime without verdict":

- `snowflake_explain_analyze_shows_runtime_without_verdict` becomes
  `snowflake_explain_analyze_emits_plan_runtime_and_verdict`.
- Keep the assertion that flat `join_order_runtime` and `join_order_verdict` rows are absent.
- Assert `join_order_snowflake`, `join_order_snowflake_runtime`, and
  `join_order_snowflake_verdict` are present.

Apply this to both lib-level binder tests and integration tests if both carry the old
boundary assertion.

## Phase 6 - Behavioral tests

Add or retarget:

- `snowflake_verdict_reports_per_level_and_aggregate` - root measured keep, branch measured
  reorder -> root `Vindicated`, branch `MissedWin`, aggregate `MissedWin`.
- `snowflake_verdict_vindicates_measured_declared_order` - all levels measured keep declared
  -> all `Vindicated`.
- `snowflake_level_estimate_wanted_reorder_judges_against_declared` - estimated action may be
  reorder, but measured keep -> `Vindicated` and measured reorder -> `MissedWin`.
- `snowflake_verdict_detail_keeps_estimate_and_measurement_separate` - detail shows
  `estimated_action` and measured action as separate fields.
- `snowflake_verdict_missed_win_recommends_would_reorder` - level and aggregate missed wins
  carry `WOULD_REORDER`.
- `snowflake_verdict_indeterminate_on_missing_measurement` - omit one measured edge; owning
  level and aggregate are `Indeterminate`.
- `snowflake_analyze_emits_no_flat_star_verdict` - no `join_order_verdict` row.
- `star_verdict_unchanged` - flat-star verdict row byte-identical after helper extraction.
- `rune_analyze_shows_snowflake_verdict` - Rune parity.

Unit tests:

- `measured_order_rerank_preserves_180_measured_order_semantics`;
- `measured_snowflake_order_verdict_aggregates_worst_case`;
- `snowflake_verdict_type_excludes_star_actuation_only_outcomes`.

## Phase 7 - Architecture guards

In `tests/architecture/join_order.rs`:

- `adr184_measured_rerank_is_shared_but_classifiers_are_separate` - `measured_star_order_verdict`
  and `measured_snowflake_order_verdict` both call `measured_order_rerank`; snowflake does not
  call `measured_star_order_verdict`.
- `adr184_snowflake_verdict_enum_has_only_observe_outcomes` - source block for
  `SnowflakeOrderVerdictKind` contains `Vindicated`, `MissedWin`, `Indeterminate`, and not
  `MisRanked`/`OverEager`.
- `adr184_snowflake_verdict_uses_183_output_rows` - snowflake analyze branch inserts
  `summary.output_rows` into the measured map, not build/probe rows.
- `adr184_snowflake_verdict_reuses_182_tree` - helper recurses over
  `SnowflakeOrderEvidence.branches` and does not call `analyze_snowflake`.
- `adr184_snowflake_verdict_detail_renders_real_evidence` - snowflake verdict rows use
  snowflake-specific detail helpers and include `estimated_action` + `SnowflakeEdgeStrategy`.
- `adr184_snowflake_verdict_is_observational` - verdict helpers/renderers contain no
  `try_bind_star_order_actuation`, `bind_star_order_sequence`, `choose_join_strategy`,
  `broadcast_cap_replay_trigger`, `PlannerConfig`, or `ExecutionPlan`.
- `adr184_no_executor_fragment_costmodel_or_wendelon_change` - markers
  `join_order_snowflake_verdict`, `SnowflakeOrderVerdict`, and
  `measured_snowflake_order_verdict` are absent from executor, fragmenter, storage, PBC,
  catalog, coordinator, SQL planner, distributed modules, and Wendelon.
- `adr184_no_optimizer_shell_or_planproperties_contract` - call the existing ADR 177/179
  guard.
- `adr184_cockpit_is_frontend_shared` - renderer path is shared; Rune contains no snowflake
  verdict implementation strings.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test explain_analyze_test snowflake_verdict
scripts/cargo-cgroup test -p toron --test explain_analyze_test snowflake_explain_analyze_emits_plan_runtime_and_verdict
scripts/cargo-cgroup test -p toron --test explain_analyze_test star_verdict_unchanged
scripts/cargo-cgroup test -p toron --lib measured_order_rerank
scripts/cargo-cgroup test -p toron --lib measured_snowflake_order_verdict
scripts/cargo-cgroup test -p toron --lib rune_analyze_shows_snowflake_verdict
scripts/cargo-cgroup test -p toron --test architecture_test adr184
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --all-targets
git diff --check
```

## Risks

- **Star regression from helper extraction.** Keep `measured_star_order_verdict` behavior
  byte-identical with focused star verdict tests.
- **Classifier drift.** Snowflake cannot grow star-only verdict states. Pin with the enum
  architecture guard and behavior tests.
- **Overclaiming counterfactuals.** The measured rows are declared-edge outputs. Keep detail
  labels clear: this is a measured verdict over the observed declared execution.
- **Missing measurements.** Partial observation must be `Indeterminate`, never a panic or a
  false `Vindicated`.
- **Level-local signal loss.** Aggregate-only verdict would hide which branch earned ADR 185.
  Keep per-level rows primary.

## Deferred

Hash-snowflake order actuation (185); broadcast-snowflake execution cliff
(`rewrite_broadcast_star_probe`); galaxy / bridge / many-to-many / cyclic recognition;
always-on verdict accrual; strategy-aware snowflake cost model; memo only when receipts show
local decisions fighting.
