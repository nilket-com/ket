# Impl Plan - ADR 179: Star-Path Join Order Runtime Evidence

Pairs with `plans/adr/179_star_path_join_order_runtime_evidence.md`.

Implement `EXPLAIN ANALYZE` as a run-and-measure surface: execute the inner plan with an
analyze-gated, replay-id-filtered capture; record generic actual join cardinality keyed by
`JoinReplayId`; correlate the star-order subset to 178's per-edge estimate; and render
estimated-vs-actual. Observational — measures, never re-plans or changes results.
Always-on telemetry and general operator profiling are deferred.

## Anchors

| Concern | Anchor |
|---|---|
| EXPLAIN ANALYZE parse (already exists, stubbed) | `lower_explain_statement(..., analyze, ..)` (`sql_planner.rs:2204`); rejection at `:2218` |
| Explain/analyze logical node | `LogicalPlan::Explain { input }` (`ir.rs:982`); add an analyze form |
| Capture hook | `ExecutionPlan::Join { replay_id, .. }` arm (`exec.rs:798`) → `execute_hash_join` (`:2424`); `batch.num_rows()` |
| Capture-mode threading | `ExecutionContext`, `TaskSpec` / `TaskExecutionContext`, worker task execution |
| Distributed merge by id (precedent) | `collect_replayable_broadcast_join_ids` (`coordinator.rs:1384`) |
| Estimated side (178) | per-edge `estimated_rows`/`estimate_source` + `DecisionReceipt` by replay id (`binder/join_order.rs`, `explain.rs`) |
| Render | `optimizer_explain_*` / new analyze renderer (`binder/explain.rs` or adjacent) |
| Tests | `binder/tests.rs`, exec measured fixtures; `tests/architecture/join_order.rs` |

## Phase 1 - Implement the EXPLAIN ANALYZE surface

- Replace `sql_planner.rs:2218` `Err(unsupported("EXPLAIN ANALYZE is not supported"))` with
  lowering to an explicit analyze logical form. Either add `analyze: bool` to
  `LogicalPlan::Explain { input }` or add a sibling `LogicalPlan::ExplainAnalyze { input }`.
- Existing callers of `LogicalPlan::Explain` stay analyze=false / bind-time. Rune adds
  `RuneDataFrame::analyze()` beside `.explain()` and lowers to the same analyze form.
- Keep normal `bind_explain` as the plan-time renderer for ordinary EXPLAIN. The analyze
  form is intercepted by a bind-and-run analysis helper before ordinary binder rendering,
  because actual rows require execution.

## Phase 2 - Thread an analyze-capture mode through the execution context

Add a `JoinRuntimeObservationMode` to `ExecutionContext` and to the Toron task
request/context used for distributed execution:

```rust
pub enum JoinRuntimeObservationMode {
    Disabled,
    MeasureJoins { replay_ids: BTreeSet<JoinReplayId> },
}
```

Off by default. Set only by the `EXPLAIN ANALYZE` / Rune `.analyze()` run path after the
bind-and-run helper has extracted 178's star-order replay ids. The executor checks replay-id
membership only; it does not receive star-order model types.

Do not gate on `task_telemetry.is_some()`: worker tasks already allocate telemetry for
ordinary execution. The explicit mode is the hot-path fence.

Add the structured observation stream to `TaskTelemetry`:

```rust
pub struct JoinRuntimeObservation {
    replay_id: JoinReplayId,
    strategy: JoinStrategy,
    build_rows: RowCount,
    probe_rows: RowCount,
    output_rows: RowCount,
}

pub struct TaskTelemetry {
    // existing labels/counters/phases
    join_observations: Vec<JoinRuntimeObservation>,
}
```

Put the vector on `TaskTelemetry`, not `TaskTelemetryCounters`; it is structured evidence,
not an aggregate counter. The vector defaults empty.

## Phase 3 - Cheap per-join capture at exec (analyze-gated)

At the `ExecutionPlan::Join` arm (`exec.rs:798`), destructure `replay_id` and `strategy`
and pass both into `execute_hash_join`. When the context capture mode is on and contains the
join's replay id, record one generic observation per physical join:

```rust
struct JoinRuntimeObservation {
    replay_id: JoinReplayId,
    strategy: JoinStrategy,
    build_rows: RowCount,
    probe_rows: RowCount,
    output_rows: RowCount,
}
```

- `build_rows` comes from the physical left/build batch after binder-side swaps;
- `probe_rows` comes from the physical right/probe batch;
- `output_rows` is the sum of returned join output batches;
- **no buffering, no batch clone/mutation/reorder**;
- worker-local observations merge by `replay_id` on flow-back;
- mode off -> no per-join observation retained.

Keep executor vocabulary generic. `exec.rs` must not import star-order model types.

## Phase 4 - Run-and-measure + correlate + render

Add a bind-and-run analysis helper at the coordinator/API boundary. It should:

- bind the input with `bind_with_explain_trace` so 178's star-order action and estimates
  are available;
- derive the replay-id allowlist from the 178 star-order evidence;
- execute the bound input once with `MeasureJoins { replay_ids }`;
- collect `TaskTelemetry.join_observations` from local or distributed execution;
- discard the user's result rows after counting/digesting as needed;
- join measured ↔ 178's plan-time `join_order` evidence by `JoinReplayId`:

```rust
struct StarOrderMeasured {
    replay_id: JoinReplayId, dimension: RelationKey,
    estimated_rows: RowCount,
    actual_rows: RowCount,
    build_rows: RowCount,
    probe_rows: RowCount,
    estimate_source: JoinOrderEstimateSource,
}
```

- render an estimated-vs-actual analysis view: estimated, actual, ratio, source, build
  rows, and probe rows per measured star edge. Correlate by id.

Aggregate partial observations by replay id with fixed semantics:

- `actual_rows` / `output_rows`: sum across workers;
- `probe_rows`: sum across workers;
- `build_rows`: physical build input rows observed by measured join tasks. For replicated
  broadcast builds this may be larger than logical dimension cardinality; do not present it
  as a distinct logical row count.

Do not serialize binder-only `OptimizerTrace` or `OptimizerExplainRenderTrace` into a
distributed `ExecutionPlan`; keep the trace coordinator-local and execute only the bound
input plan.

## Phase 5 - Observational safety

- Bound input plan + result rows byte-identical with and without the capture flag (capture
  is counting only).
- No re-plan / margin change consumes the measured map (distinct from
  `broadcast_cap_replay_trigger`). Acting deferred.

## Phase 6 - Behavioral tests

Reuse 178's star-order SQL/catalog shape where possible. If `star_join_order_catalog` is
catalog-only and not executable, add a small in-memory star fixture with matching
optimizer facts. Add a known-wrong-estimate fixture (157-style: clustered/biased dimension
whose NDV-driven estimate misses actual by a known factor).

- `explain_analyze_runs_and_captures_star_join_actuals` - `EXPLAIN ANALYZE <star>` records
  actual generic per-join rows by `replay_id`.
- `explain_analyze_was_previously_rejected_now_implemented` - `EXPLAIN ANALYZE` no longer
  errors unsupported.
- `measured_vs_estimated_record_carries_both_sides` - 178 estimate + runtime actual + ratio
  + source, same replay id.
- `known_miss_star_runtime_evidence_shows_divergence` - known-wrong fixture under analyze →
  `actual_rows ≠ estimated_rows` by the known factor.
- `runtime_measurement_is_observational` - results + bound plan identical with/without analyze.
- `ordinary_execution_captures_nothing` - non-analyze run captures no actuals even though
  task telemetry exists.
- `non_star_join_is_not_captured_by_star_analyze_allowlist` - a non-star join in the same
  analyzed plan is not recorded unless its replay id is requested.
- `measured_record_correlates_by_replay_id` - actual maps to the right dim/estimate by id.
- `cockpit_renders_estimated_vs_actual` - analyze view shows both sides; SQL + Rune parity.
- `spilled_join_records_runtime_observation_when_analyze_enabled` - low-memory spill path
  records one observation, not two.
- `star_order_reorder_preserves_schema_and_rows_by_execution_diff` - reordered and
  declared-order stars match by exact schema and canonical row multiset.

## Phase 7 - Architecture guards

In `tests/architecture/join_order.rs`:

- `adr179_runtime_evidence_is_observational` - capture/emit mutates no plan/result/decision.
- `adr179_measures_does_not_act` - no re-plan / margin mutation consumes the measured signal.
- `adr179_actual_correlates_via_replay_id` - measured↔estimated by `JoinReplayId`.
- `adr179_reuses_178_estimate_no_second_estimator` - estimated side is 178's value.
- `adr179_capture_is_cheap_and_analyze_gated` - exec capture counts rows, no buffer/mutation,
  only under the explicit replay-id capture mode, not merely because telemetry exists.
- `adr179_capture_allowlist_is_replay_id_only` - the capture context carries
  `JoinReplayId`s, not star-order model types.
- `adr179_explain_analyze_is_implemented_not_rejected` - the `:2218` rejection is gone.
- `adr179_no_star_order_vocabulary_in_exec` - executor records generic join observations
  without `StarOrder*`, `JoinOrderEdgeEvidence`, or `join_order` imports.
- `adr179_no_fragment_or_wendelon_storage_pbc_change` - no `fragment.rs:80` topology,
  Wendelon, storage descriptor, catalog, or PBC change. Toron task/context payload may
  carry the explicit capture mode.
- `adr179_no_optimizer_shell` - 177's no-cathedral guard holds.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib known_miss_star_runtime_evidence_shows_divergence
scripts/cargo-cgroup test -p toron --lib runtime_measurement_is_observational
scripts/cargo-cgroup test -p toron --lib ordinary_execution_captures_nothing
scripts/cargo-cgroup test -p toron --lib star_order_reorder_preserves_schema_and_rows_by_execution_diff
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test adr179
scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

## Risks

- **First exec touch.** Capture is a pure counter at the join output — no buffer/reorder/
  clone; pin with cheap-capture + identical-results guards. Keep it minimal.
- **Analyze path in the wrong layer.** Binder can render plan-time receipts, but it cannot
  execute. Keep runtime analysis at a bind-and-execute boundary.
- **Observational.** No re-plan/margin change on the measured signal — the line vs
  `broadcast_cap_replay_trigger`.
- **Distributed merge.** Worker-local capture merges by `replay_id` at the coordinator
  (the same id-keyed pattern as `collect_replayable_broadcast_join_ids`); mis-merge
  mis-attributes — correlate strictly by id; pin with `measured_record_correlates_by_replay_id`.
- **Replicated build rows.** Broadcast build input may be observed once per worker. Treat
  `build_rows` as physical work evidence, not logical dimension cardinality.
- **Analyze-gated.** Ordinary execution captures nothing despite task telemetry; pin with
  `ordinary_execution_captures_nothing`.
- **Surface scope.** Implement only `EXPLAIN ANALYZE` join-order measurement; do not widen
  to general operator metrics or always-on telemetry this cut.
- **Executor vocabulary leak.** Executor records physical join observations only; star
  analysis stays in binder/coordinator rendering.
- **Fidelity + known-miss honesty.** Estimated side is 178's emitted value; the miss fixture
  is deterministic/RNG-free (157 discipline).

## Deferred

Acting on the measurement (adaptive re-plan, soft-margin auto-tuning from measured
estimate-vs-actual); always-on production measured telemetry; hash/non-broadcast +
general-graph measurement; per-operator metrics beyond star-join cardinality.
