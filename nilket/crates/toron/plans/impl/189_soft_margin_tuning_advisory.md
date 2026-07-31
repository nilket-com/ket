# Impl Plan - ADR 189: Soft-Margin Tuning Advisory

Pairs with `plans/adr/189_soft_margin_tuning_advisory.md`.

Read 188's accrued verdicts, compute a recommended soft margin via a bounded/hysteretic/min-sampled
policy, and surface it read-only as `information_schema.optimizer_margin_advisory`. No decision or
plan change; the 178 margin const and `classify_star_order_action` are untouched. Application is 190.

## Anchors

| Concern | Anchor |
|---|---|
| Margin const + gate (read, unchanged) | `STAR_ORDER_SOFT_MARGIN_NUMERATOR/DENOMINATOR`, `classify_star_order_action` (`binder/join_order.rs:5,1056`) |
| Verdict input (reused) | `OptimizerDecisionLog::verdict_snapshot()`; `OptimizerRuntimeVerdictClass` (`binder/decision_log.rs`) |
| Log handle | shared `Arc<OptimizerDecisionLog>` via `PlannerConfig` (187) |
| Report path | `bind_information_schema_relation` (`binder.rs:656`); `INFORMATION_SCHEMA_*` consts; sibling tables `optimizer_decisions` (187), `optimizer_runtime_verdicts` (188) |
| Tests | `binder/tests.rs`; `tests/architecture/optimizer_accrual.rs` |

## Phase 1 - The tuning policy (`binder/join_order.rs`)

```rust
pub(crate) enum SoftMarginLadderStep { Tightest, Tight, Baseline, Loose, Loosest }
pub(crate) struct SoftMargin { numerator: u128, denominator: u128 } // private fields
// ascending = looser; gate fires iff pref/decl <= numerator/denominator.
// values: 3/5, 7/10, 4/5, 9/10, 19/20
const CURRENT_MARGIN_STEP: SoftMarginLadderStep = SoftMarginLadderStep::Baseline;
const MIN_SAMPLE: u64 = …;            // documented; e.g. 20 decisive verdicts
const HYSTERESIS_NUM/DEN: …;          // |net| must exceed this fraction of decisive

pub(crate) enum MarginRecommendation { Loosen, Tighten, Keep, InsufficientEvidence }
pub(crate) struct MarginAdvisory {
    current: SoftMargin, recommended: SoftMargin, recommendation: MarginRecommendation,
    missed_win: u64, over_eager: u64, vindicated: u64,
    mis_ranked: u64, indeterminate: u64, sample_size: u64,
}
pub(crate) fn recommend_soft_margin(
    snapshot: &[OptimizerRuntimeVerdictRow],
    current: SoftMarginLadderStep,
) -> MarginAdvisory { … }
```

`OptimizerDecisionLog` stays a log; do not move policy into it. Keep the margin policy near the
margin semantics in `join_order.rs`, and have report materialization call the pure helper.
Invalid ratios should be unrepresentable: callers choose a ladder step, not a public
`(num, den)`.

Policy:
- aggregate `missed_win`/`over_eager`/`vindicated` across kinds; count `MisRanked`/`Indeterminate`
  for visibility only; `decisive = missed_win + over_eager + vindicated`.
- `decisive < MIN_SAMPLE` → `InsufficientEvidence`, recommended = current.
- `net = missed_win - over_eager`; `net*HYS_DEN > decisive*HYS_NUM` → one step looser (clamp);
  `< -…` → one step tighter (clamp); else `Keep`.
- step = move one enum step along the ladder from the current step, clamped to the enum endpoints.
- `MisRanked`/`Indeterminate` never enter `decisive` or `net`.

Pure function over the snapshot — no I/O, no mutation.

## Phase 2 - The read-only report (`binder/explain.rs`, `binder.rs`, `ir/execution.rs`)

- Add `SystemTableKind::InformationSchemaOptimizerMarginAdvisory` in `ir/execution.rs`.
- Add the `information_schema.optimizer_margin_advisory` name/table consts in `binder.rs`.
- Add schema/row helpers in `binder/explain.rs` with columns
  (`current_margin` Utf8 e.g. "4/5", `recommended_margin` Utf8, `recommendation` Utf8,
  `missed_win`/`over_eager`/`vindicated`/`mis_ranked`/`indeterminate`/`sample_size` Int64).
- Bind it in `bind_information_schema_relation`:
  `recommend_soft_margin(config.decision_log.verdict_snapshot(), CURRENT_MARGIN_STEP)` → one row.
- Update the unknown `information_schema` relation diagnostic to list the new report.
- Reject DDL/writes like the other information-schema relations.

## Phase 3 - Behavioral tests (`binder/tests.rs`)

Drive `recommend_soft_margin` directly (unit) and via a seeded log + the report query (integration):

- `margin_advisory_recommends_looser_on_missed_wins` - seed `MissedWin`-dominant (≥ MIN_SAMPLE,
  past hysteresis) → `loosen`, recommended one step looser.
- `margin_advisory_recommends_tighter_on_overeager` - `OverEager`-dominant → `tighten`.
- `margin_advisory_keeps_when_well_calibrated` - `Vindicated`-dominant / balanced → `keep`.
- `margin_advisory_insufficient_evidence_below_min_sample` - small sample → `insufficient_evidence`.
- `margin_advisory_excludes_misranked_and_indeterminate` - only `MisRanked`/`Indeterminate` →
  `insufficient_evidence`, with those counts visible in the report.
- `margin_advisory_clamps_to_safe_range` - extreme imbalance at the ladder end stays at the end.
- `margin_advisory_has_hysteresis` - small `net` below threshold → `keep`.
- `margin_advisory_is_read_only_no_plan_change` - bind a reordering star with an empty vs a
  populated verdict log; the `ExecutionPlan` and the 178 action are byte-identical (the verdict
  log is read-only for planning).
- `rune_queries_margin_advisory` - Rune `SELECT`s the report.

## Phase 4 - Architecture guards (`tests/architecture/optimizer_accrual.rs`)

- `adr189_advisory_is_read_only` - `recommend_soft_margin` is a pure fn over the snapshot + margin;
  the report bind only reads; no planning decision references the advisory.
- `adr189_advisory_does_not_change_margin_or_decision` - `STAR_ORDER_SOFT_MARGIN_*` and
  `classify_star_order_action` bodies are unchanged by 189 (source-match).
- `adr189_advisory_reuses_188_verdict_log` - the advisory calls `verdict_snapshot()`; no new log.
- `adr189_advisory_excludes_misranked_indeterminate` - the policy source excludes those classes
  from `decisive`/`net`, while the row helper renders their counts.
- `adr189_report_reuses_information_schema_path` - binds via `bind_information_schema_relation`.
- `adr189_no_executor_fragmenter_storage_or_wendelon_change` - marker sweep: advisory symbols
  absent from `exec.rs`, `fragment.rs`, storage, `distributed/`, Wendelon.
- Retarget `adr102_system_table_kind_has_finite_introspection_family` for the additional
  `InformationSchemaOptimizerMarginAdvisory` variant and assert the new member explicitly.
- `adr189_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib margin_advisory
scripts/cargo-cgroup test -p toron --lib optimizer_decision optimizer_runtime_verdict   # 187/188 unchanged
scripts/cargo-cgroup test -p toron --lib star_order snowflake hash_snowflake     # 177-186 unchanged
scripts/cargo-cgroup test -p toron --test explain_analyze_test                   # verdicts unchanged
scripts/cargo-cgroup test -p toron --test architecture_test adr189
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

## Risks

- **Read-only.** The advisory must never mutate the margin/config/plan; `recommend_soft_margin` is
  pure. Pin with `margin_advisory_is_read_only_no_plan_change` + the guard.
- **Policy correctness.** Direction (MissedWin→looser, OverEager→tighter), min-sample, hysteresis,
  clamp must all hold. Pin with the per-case unit gates.
- **Exclusions.** MisRanked/Indeterminate must not count as decisive. Pin with the exclusion gate.
- **Finite introspection guard.** Adding a new information-schema report adds one
  `SystemTableKind` variant; retarget ADR102's finite-family guard in the same cut.
- **No new field needed.** The log handle already rides in `PlannerConfig` (187); no new
  `PlannerConfig` field, so no `--all-targets` breadth risk this cut.
- **Determinism.** The report is a single deterministic row from a snapshot.

## Deferred

Margin application (190: configurable margin + manual/auto apply); per-kind margins; cost-model
tuning; persistence; broadcast cliff; mixed-strategy cost model; galaxy/bridge.
