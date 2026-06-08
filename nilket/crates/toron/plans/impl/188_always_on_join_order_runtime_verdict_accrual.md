# Impl Plan - ADR 188: Always-On Join-Order Runtime Verdict Accrual

Pairs with `plans/adr/188_always_on_join_order_runtime_verdict_accrual.md`.

For a sampled fraction of normal queries, the coordinator binds via the explain-analyze binding,
executes the query plan with the existing `MeasureJoins` capture (returning user rows), computes
the 180/184 measured verdict via an extracted shared helper, and accrues per-level verdict classes
into 187's log — surfaced as `information_schema.optimizer_runtime_verdicts`. No executor change;
bounded by a sample rate (default off); side-effect-only.

## Anchors

| Concern | Anchor |
|---|---|
| Capture mode (reused, unchanged) | `JoinRuntimeObservationMode::{Disabled, MeasureJoins}` (`distributed/task.rs`); `exec.rs:2523+` produces observations |
| ANALYZE path to mirror | `execute_explain_analyze_distributed` (`coordinator.rs:720`); local variant (`:2410`) |
| Binding (reused) | `bind_explain_analyze` → `ExplainAnalyzeBinding { plan, replay_ids, trace, render_trace }` |
| Verdict computation (extract) | `render_explain_analyze` → `optimizer_analyze_plan` → per-receipt `measured_*_verdict` (`binder/join_order.rs`, `binder/explain.rs`) |
| Observation collection | `join_observations_from_task_telemetry` / `..._from_local_telemetry` (`coordinator.rs:1440/1449`) |
| Normal execute path | `execute_query_with_context` / `execute_query_with_admission` (`coordinator.rs:516/750`) |
| Accrual log (extend) | `OptimizerDecisionLog` (`binder/decision_log.rs`); shared via `PlannerConfig` |
| Report path | `bind_information_schema_relation` (`binder.rs:656`); `INFORMATION_SCHEMA_*` consts |
| Tests | coordinator tests; `binder/tests.rs`; `tests/architecture/optimizer_accrual.rs` |

## Phase 1 - Extract the verdict computation (`binder/explain.rs`)

Factor the per-level verdict out of the analyze-row rendering:

```rust
pub(crate) enum RuntimeVerdictClass { Vindicated, MissedWin, OverEager, MisRanked, Indeterminate }
pub(crate) struct MeasuredLevelVerdict { kind: OptimizerDecisionKind, verdict: RuntimeVerdictClass }
pub(crate) fn measured_verdicts(
    binding: &ExplainAnalyzeBinding,
    observations: &[JoinRuntimeObservation],
) -> Vec<MeasuredLevelVerdict> { … }
```

It runs the same per-receipt `measured_star_order_verdict` / `measured_snowflake_order_verdict`
the renderer uses, returning the verdict classes. **Do not rewrite `optimizer_analyze_rows`'s
rendering** — only extract the shared `measured_rows_for_receipt(receipt/observed, summaries)`
rows-construction helper so the renderer and `measured_verdicts` build the same
`measured_rows_by_dimension`; both then call the existing `measured_*_verdict` classifiers. Render
output stays byte-identical (pin with the 180/184 explain-analyze tests); the accrued class equals
the rendered verdict (`measured_verdict_reuses_180_184_computation`).

## Phase 2 - The sample gate (`binder` config + coordinator)

- Add `pub measured_accrual_sample_rate: u32` to `PlannerConfig` (default 0). **Run
  `cargo check -p toron --all-targets`** (new field breadth, like 187).
- Add a coordinator-scoped `Arc<AtomicU64>` query counter (alongside the shared log).
- `fn should_measure(rate: u32, counter: &AtomicU64) -> bool`: if `rate == 0`, false; otherwise
  `let ordinal = counter.fetch_add(1, Ordering::Relaxed).saturating_add(1); ordinal %
  u64::from(rate) == 0`. This matches the gate wording: `sample_rate=3` instruments queries 3, 6,
  9, ...

## Phase 3 - The sampled measured path (`coordinator.rs`, inside the admission path)

`execute_query_with_admission` (`:750`) already runs
`execute_bound_distributed_attempt_with_telemetry` (`:1173`) → `output.task_telemetry`. The mode
is `Disabled` today; sampling just sets it. When `should_measure(...)` for a read query:

```rust
let binding = bind_explain_analyze(plan, catalog, &config).await?;       // plan == bind(plan).plan (177 invariant)
let context = context.with_join_runtime_observation_mode(
    analyze_observation_mode(binding.replay_ids.clone()),                 // MeasureJoins
);
// existing admission/budget execute, unchanged:
let output = self.execute_bound_distributed_attempt_with_telemetry(binding.plan.clone(), &context, budget, handle, 0).await?;
let observations = join_observations_from_task_telemetry(&output.task_telemetry);
for v in measured_verdicts(&binding.render_trace, &observations) {
    config.decision_log.record_verdict(v.kind, v.verdict);               // accrue, do not render
}
return Ok(output.batches);                                               // user rows; admission/budget intact
```

Reuse the existing telemetry executor (no new executor capture plumbing); only the bind (analyze vs
normal) and the mode differ from the unsampled path, plus the verdict accrual. Keep the local
(`execute_*_local`) variant symmetric if the deployment is non-distributed.

Non-sampled / non-read / when off: the unchanged normal path (`bind` + execute), 187 decisions
only. Reuse the ANALYZE path's execute-with-mode plumbing (refactor the shared bit out of
`execute_explain_analyze_distributed`).

## Phase 4 - Extend the log + report (`binder/decision_log.rs`, `binder.rs`)

- `OptimizerDecisionLog`: add `verdict_counts: Mutex<BTreeMap<(OptimizerDecisionKind, VerdictClass), u64>>`
  + `record_verdict` + `verdict_snapshot`.
- Add `SystemTableKind::InformationSchemaOptimizerRuntimeVerdicts`, the
  `information_schema.optimizer_runtime_verdicts` name/table consts, schema (`decision_kind`,
  `verdict`, `count`), and bind it in `bind_information_schema_relation` from
  `verdict_snapshot()`.

## Phase 5 - Behavioral tests (coordinator tests, `binder/tests.rs`)

- `measured_verdicts_accrue_for_sampled_queries` - `sample_rate=1`, a measured-MissedWin snowflake
  → `optimizer_runtime_verdicts` shows `(snowflake_*, missed_win)`; a vindicated query →
  `vindicated`.
- `measured_accrual_off_by_default_is_zero_overhead` - `sample_rate=0` → empty verdict report;
  results/plan identical to pre-188.
- `sampled_query_returns_user_data_not_analyze_table` - sampled query returns the same user rows
  as unsampled.
- `measured_accrual_samples_every_nth_query` - `sample_rate=3` instruments queries 3, 6, 9.
- `measured_verdict_reuses_180_184_computation` - accrued verdict class == the EXPLAIN ANALYZE
  verdict for the same query.
- `measured_verdicts_record_per_level_for_recursive_snowflake` - bushy snowflake → root + branch
  verdicts.
- `optimizer_runtime_verdicts_report_aggregates_by_kind_and_class` - repeated identical sampled
  queries bump one row's count.
- `rune_queries_optimizer_runtime_verdicts` - Rune `SELECT`s the report.

## Phase 6 - Architecture guards (`tests/architecture/optimizer_accrual.rs`)

- `adr188_measured_accrual_reuses_capture_and_verdict` - sampled path references
  `MeasureJoins`/`bind_explain_analyze`/`measured_*_verdict`; defines no new capture mode or
  classifier.
- `adr188_no_executor_capture_change` - `exec.rs`/`distributed` `JoinRuntimeObservation`
  production unchanged.
- `adr188_sampled_path_returns_user_data` - the sampled branch returns query batches, not the
  analyze system table.
- `adr188_measured_accrual_is_side_effect_only` - the verdict log is write-from-execution,
  read-for-report only; no planning input.
- `adr188_default_off_zero_overhead` - `measured_accrual_sample_rate` defaults to 0; the measured
  branch is guarded by `should_measure`.
- `adr188_report_reuses_information_schema_path` - verdict report binds via
  `bind_information_schema_relation`.
- `adr188_no_storage_or_wendelon_change` - marker sweep: no storage/PBC/Wendelon.
- `adr188_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets        # PlannerConfig new-field breadth
scripts/cargo-cgroup test -p toron --lib optimizer_runtime_verdict measured_accrual
scripts/cargo-cgroup test -p toron --test explain_analyze_test   # 179/180/183/184 render byte-identical
scripts/cargo-cgroup test -p toron --lib optimizer_decision      # 187 decisions unchanged
scripts/cargo-cgroup test -p toron --lib star_order snowflake hash_snowflake  # 177-186 unchanged
scripts/cargo-cgroup test -p toron --test architecture_test adr188
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

## Risks

- **Verdict-extraction must not change render.** The extracted `measured_verdicts` must produce
  the same classes the renderer shows; refactor render to call it. Pin with green
  `explain_analyze` + `measured_verdict_reuses_180_184_computation`.
- **`PlannerConfig` field breadth.** New `sample_rate` field breaks literal constructions
  (E0063, `--all-targets`). Default 0; run `--all-targets`.
- **Side-effect-only.** Sampled execution must return identical user rows; capture/verdict never
  change results. Pin with `sampled_query_returns_user_data_not_analyze_table` +
  `measured_accrual_off_by_default_is_zero_overhead`.
- **Double bind cost.** The sampled path binds via explain-analyze (replacing the normal bind),
  not in addition; do not bind twice.
- **Render-evidence overhead.** `bind_explain_analyze` may build structured render evidence and
  predicate strings that accrual does not display. Keep the first cut correct by reusing the
  existing binding, but do not parse rendered rows; defer a lighter measurement binding until the
  sampled overhead is measured.
- **Concurrency / counter.** The `AtomicU64` counter and log `Mutex` must stay lock-local; never
  hold across execution.
- **Read-only scope.** Only measure read queries (the ANALYZE path is read-only); writes/DDL take
  the normal path.

## Deferred

Margin self-tuning (189); persistence; scan/filter measured accrual; unsampled always-on
measurement; broadcast cliff; mixed-strategy cost model; galaxy/bridge.
