# Impl Plan - ADR 180: Star-Path Measured Verdict Receipts

Pairs with `plans/adr/180_*.md`.

Pure-interpretation cut: combine 178's `StarOrderEvidence` (declared/preferred/applied
order + per-edge `estimated_rows`) with 179's per-edge `actual_rows`, recompute 178's same
star-order model under the measured rows, classify the decision (Vindicated / MisRanked /
MissedWin / OverEager / Indeterminate), report per-dim accuracy, and render it in the
`EXPLAIN ANALYZE` cockpit. No new execution capture, no acting, no plan change.

## Anchors

| Concern | Anchor |
|---|---|
| 178 decision record | `StarOrderEvidence { declared_order, cost_preferred_order, applied_order, action, confidence }` + per-edge `estimated_rows` (`binder/join_order.rs`) |
| 179 measured record | per-edge `actual_rows` keyed by replay id (`JoinRuntimeObservationSummary.output_rows`, `binder/explain.rs`) |
| 178 model reuse | `recognized_star_order_outcome`, `classify_star_order_action`, `order_cost` (`binder/join_order.rs`) |
| Analyze render | `optimizer_analyze_plan`, `optimizer_analyze_rows`, `optimizer_analyze_join_order_row` (`binder/explain.rs`) |
| Tests | `tests/explain_analyze_test.rs`; `tests/architecture/join_order.rs` |

## Phase 1 - Verdict types (in `binder/join_order.rs`)

```rust
pub(super) enum StarOrderVerdict {
    Vindicated,
    MisRanked,     // 178 reordered; measured 178 would reorder differently
    MissedWin,     // 178 kept declared; measured 178 would reorder
    OverEager,     // 178 reordered; measured 178 would keep declared
    Indeterminate, // missing actuals / incomplete measured input
}
pub(super) enum StarOrderVerdictRecommendation {
    KeepApplied,
    WouldReorder,
    WouldKeepDeclared,
    NoRecommendation,
}
pub(super) struct DimAccuracy {
    dimension: String, estimated_rows: RowCount, actual_rows: RowCount, // ratio at render
}
pub(super) struct StarOrderVerdictReceipt {
    measured_applied_order: Vec<String>,
    verdict: StarOrderVerdict,
    recommendation: StarOrderVerdictRecommendation,
    accuracy: Vec<DimAccuracy>,
    measured_evidence: Option<StarOrderEvidence>,
}
```

## Phase 2 - Classify (interpretation-only)

`fn verdict(evidence: &StarOrderEvidence, measured: &[edge actual_rows]) ->
StarOrderVerdictReceipt`:

- pair each dim's `estimated_rows` (178) with its `actual_rows` (179) by replay id/dim;
- clone each `JoinOrderDimEvidence`, replacing only `estimated_rows` with 179's
  `actual_rows` / `output_rows`;
- call `recognized_star_order_outcome` with the measured dimensions and original
  confidence;
- derive the measured applied order from the measured action:
  - `Reordered` → measured `cost_preferred_order`;
  - kept → measured `declared_order`;
- record that measured applied order in measured evidence by passing it as the
  `applied_order_override`;
- classify:
  - missing/insufficient actuals → `Indeterminate`;
  - `evidence.applied_order == measured.applied_order` → `Vindicated`;
  - original action kept-declared and measured action reordered → `MissedWin`;
  - original action reordered and measured action kept-declared → `OverEager`;
  - original action reordered and measured action reordered differently → `MisRanked`;
- build `accuracy` per dim (estimated, actual; ratio + over/under label at render).

Read-only; no mutation of evidence, plan, or decision.

## Phase 3 - Render in EXPLAIN ANALYZE

Preserve 179's per-edge `join_order_runtime` rows. Append one summary
`join_order_verdict` row containing `measured_applied_order`, `measured_action`, `verdict`
label, and per-dim `accuracy` detail beside 178's declared/preferred/applied context. SQL
+ Rune share the path. Only emitted under `EXPLAIN ANALYZE` (rides 179's analyze gate);
ordinary EXPLAIN/execution unaffected. Prefer keeping costs/accuracy in `detail` over
widening the analyze schema.

## Phase 4 - Behavioral tests

Reuse 179's analyze fixtures; add known-miss fixtures where measured actuals make the same
178 model pick a different applied order, including the conservative and over-eager margin
cases.

- `verdict_vindicated_when_measured_178_would_apply_same_order` - applied == measured
  applied → Vindicated.
- `verdict_misranked_when_reorder_picks_wrong` - reordered, applied != measured applied →
  MisRanked, differing dims shown.
- `verdict_missed_win_when_margin_declined_a_real_win` - kept declared, declared !=
  measured applied → MissedWin.
- `verdict_overeager_when_measurement_would_keep_declared` - reordered, measured 178 would
  keep declared → OverEager.
- `verdict_reports_per_dim_estimate_accuracy` - actual vs estimated + ratio + over/under.
- `verdict_indeterminate_on_missing_actuals` - absent measurement → Indeterminate.
- `verdict_uses_output_rows_not_build_or_probe_rows` - measured scoring substitutes
  `output_rows`.
- `verdict_is_interpretation_only` - results + bound plan + 178 decision byte-identical with/without
  verdict; no new capture.
- `verdict_does_not_act` - no re-plan/revert/margin/fact mutation consumes the verdict.
- `cockpit_renders_verdict` - analyze view shows verdict + measured order + accuracy; Rune parity.

## Phase 5 - Architecture guards

In `tests/architecture/join_order.rs`:

- `adr180_verdict_reuses_178_order_model_no_second_estimator` - measured classification calls
  `recognized_star_order_outcome` / `order_cost` and does not call join/cardinality/fact
  estimators.
- `adr180_measured_helper_sets_applied_order_override` - measured evidence explicitly sets
  the measured applied order rather than relying on the default declared-order override.
- `adr180_verdict_compares_star_evidence_applied_order` - original `applied_order` is
  compared to measured `applied_order`.
- `adr180_verdict_uses_runtime_output_rows` - measured scoring uses `summary.output_rows`,
  not `build_rows` / `probe_rows`.
- `adr180_verdict_is_interpretation_only` - the verdict reads 178 evidence + 179 measured and
  renders; adds no execution capture; mutates no plan/decision.
- `adr180_verdict_does_not_act` - no re-plan/revert/margin/fact code consumes the verdict.
- `adr180_runtime_correlation_remains_replay_id_keyed` - actuals are mapped through
  `JoinReplayId`.
- `adr180_no_exec_or_fragment_change` - no executor/`fragment.rs`/Wendelon/storage/PBC change.
- `adr180_rune_and_sql_share_analyze_renderer` - Rune has no separate verdict
  implementation.
- `adr180_no_optimizer_shell` - 177's no-cathedral guard holds.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test explain_analyze_test verdict_misranked_when_reorder_picks_wrong
scripts/cargo-cgroup test -p toron --test explain_analyze_test verdict_missed_win_when_margin_declined_a_real_win
scripts/cargo-cgroup test -p toron --test explain_analyze_test verdict_overeager_when_measurement_would_keep_declared
scripts/cargo-cgroup test -p toron --test explain_analyze_test verdict_uses_output_rows_not_build_or_probe_rows
scripts/cargo-cgroup test -p toron --test explain_analyze_test verdict_is_interpretation_only
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr180
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

## Risks

- **Interpretation-only.** The verdict must not act or capture; reuse 179's measured map +
  178's evidence and render. Pin with `verdict_is_interpretation_only` + `verdict_does_not_act`.
- **Correlation.** Pair estimated↔actual by replay id/dim, not position (distributed merge).
- **Soft-margin fidelity.** A raw actual-row sort is not enough; run the existing 178 model
  under measured rows so conservative and over-eager margin outcomes are classified
  correctly.
- **Default applied-order trap.** `recognized_star_order_outcome(..., None, ...)` records
  declared order as applied. The measured pass must set the measured applied-order override.
- **No second estimator.** Use 178's evidence and 179's `output_rows` as-is; do not call
  cardinality or fact estimators.
- **Scope.** Broadcast inner star 178 evaluated, under EXPLAIN ANALYZE; no acting, no
  cross-query accrual, no hash/general graphs.

## Deferred

Acting on the verdict (margin auto-tune / fact feedback / adaptive re-order / revert);
persisting + accruing verdicts across queries (always-on); hash/non-broadcast + general-graph
verdicts.
