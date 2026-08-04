# ADR 179: Star-Path Join Order Runtime Evidence

**Status:** Promoted. **Migration:** `plans/impl/0179_star_path_join_order_runtime_evidence.md`.

178 reorders broadcast stars on a *soft* estimate with a *hardcoded* margin, both
unvalidated against reality. ADR 179 closes the loop: under `EXPLAIN ANALYZE`, execute the
query, capture **actual** per-join row counts, correlate the star-order subset to the
estimate that drove 178's order decision (by `JoinReplayId`), and render
**measured-vs-estimated**. It **measures, it does not act** — no re-plan, no margin change,
no result change. The first *robustness-first* rung: knowing where the estimate misses, on
real executions, is the direct answer to the beyond-star fear.

### What's new and what stays safe

This is the **first arc cut to touch execution** — 177/178 were binder-only. "Runtime
evidence" needs a runtime measurement, so 179 adds a cheap generic row-count capture at
physical join outputs, **gated to `EXPLAIN ANALYZE` and a replay-id allowlist**. Star-order
analysis supplies 178's replay ids, then consumes the captured rows later by replay id; the
executor does not know about star-order vocabulary.
The safety line: it is **observational** — it counts rows and emits a record; it changes
no plan, no result, no decision. Acting on the measurement (adaptive re-plan, margin
auto-tuning) is the named, deferred contract.

## Grounding

- **`EXPLAIN ANALYZE` is already parsed and stubbed.** `lower_explain_statement(..., analyze,
  ..)` (`sql_planner.rs:2204`) currently `Err(unsupported("EXPLAIN ANALYZE is not
  supported"))` at `:2218`. 179 **implements** it: flip that rejection and lower to an
  explicit analyze logical form. No new parser.
- **The correlation key exists.** `JoinReplayId` ties a runtime join event to its plan join
  (`ExecutionPlan::Join { replay_id, .. }`); `broadcast_cap_replay_trigger` /
  `collect_replayable_broadcast_join_ids` (`coordinator.rs:1350/1384`) already aggregate a
  runtime join signal by replay id. 179 reuses that key and follows the same id-keyed
  aggregation pattern for observations.
- **The capture hook is precise.** The `ExecutionPlan::Join` arm (`exec.rs:798`) currently
  ignores `replay_id`, calls `execute_hash_join`, and gets output batches whose row counts
  can be summed. No per-join output metric exists today — 179 adds a cheap one.
- **Telemetry presence is not a gate.** Worker task execution already constructs
  `TaskTelemetry`. 179 needs an explicit join-observation mode in the execution/task
  request, similar in spirit to existing per-query context threading, so ordinary
  execution does not accidentally collect the new stream. The mode carries replay ids,
  not star-order model types.
- **178/177 carry the estimate** (`binder/join_order.rs` per-edge `estimated_rows`/
  `estimate_source`, `DecisionReceipt` by replay id) — the estimated side; 179 supplies the
  measured side and the comparison.
- **154→157 discipline**: build the measurement; name the future contract (adaptive loop /
  auto-tuned margin).

## Decisions

**D1 - Implement `EXPLAIN ANALYZE` as the run-and-measure surface.** Flip the
`sql_planner.rs:2218` rejection and lower to a distinct analyze logical form (either an
`analyze` flag on `LogicalPlan::Explain` or a sibling `LogicalPlan::ExplainAnalyze`). Rune
gets `.analyze()` beside `.explain()`. The analyze path **binds and executes** the inner
plan with the capture flag set, collects the measured map, and renders
estimated-vs-actual. Ordinary `EXPLAIN` stays plan-time; ordinary query execution captures
nothing.

**D2 - Capture generic per-join cardinality at execution, keyed by `JoinReplayId`.** At
the `ExecutionPlan::Join` arm (`exec.rs:798`), when the analyze-capture mode is set and the
join's `replay_id` is in the requested set, record one physical join observation:
`{replay_id, strategy, build_rows, probe_rows, output_rows}`. It is a row-counting record,
**no buffering, no batch mutation, no reordering**. Worker-local observations merge by
`replay_id` on flow-back. The executor records generic join facts; star-order analysis
happens later.

**D3 - Thread an explicit capture mode through execution.** Add a small join-observation
mode to `ExecutionContext` and, for distributed execution, the Toron task request/context:
`Disabled` by default, or `MeasureJoins { replay_ids }` for analyze. Ordinary execution
does not collect per-join observations even though task telemetry exists. `EXPLAIN ANALYZE`
and Rune `.analyze()` derive the replay-id set from 178's star-order evidence and turn it
on. A future general operator profile can widen the set under a new ADR.

**D4 - Correlate actual ↔ estimate by `JoinReplayId`; emit measured-vs-estimated.** Join
the measured map to 178's per-edge estimate by replay id:
`{replay_id, dimension, estimated_rows, actual_rows, build_rows, probe_rows,
estimate_source}` (+ ratio at render). Correlate by id, never by position.
Distributed merge semantics are explicit: `actual_rows/output_rows` and `probe_rows` sum
across workers; `build_rows` is reported as physical build input rows observed by the
measured join tasks, not as a de-duplicated logical dimension cardinality.

**D5 - Measure, don't act.** No re-plan, no margin change, no plan/result mutation for the
analyzed query — explicitly distinct from `broadcast_cap_replay_trigger` (which re-plans).
Acting is deferred.

**D6 - Render as runtime evidence.** The `EXPLAIN ANALYZE` output uses a dedicated
analyze renderer or analyze-only cockpit rows with estimated, actual, ratio, source, build
rows, and probe rows per measured star edge. The plan-time 177/178 `EXPLAIN` evidence is
unchanged and ordinary `optimizer_explain_plan` remains bind-time. SQL and Rune share the
analysis renderer.

**D7 - Observational safety.** The bound input plan, disposition, strategy, order, and
result rows are identical with join-observation mode on or off. The capture is row-counting
only. Pinned by an identical-results/plan gate with capture on, and by
`ordinary_execution_captures_nothing`.

**D8 - Scope + reuse.** Broadcast inner star (178 shape) is the star-order consumer, while
the executor observation is generic replay-id-filtered join cardinality. Reuses replay-id
correlation, 178's estimate, the explain/cockpit vocabulary, and the parsed `EXPLAIN
ANALYZE`. New surface: the cheap exec row-count capture (first arc `exec` touch), the
explicit capture mode in Toron execution/task context, and the measured record.
**Deferred:** acting on the signal; hash/non-broadcast + general-graph estimated-vs-actual
analysis; always-on production telemetry. No
`fragment.rs:80` topology, Wendelon, storage descriptor, catalog, or PBC change.

## Headline Gates

- `explain_analyze_runs_and_captures_star_join_actuals` - `EXPLAIN ANALYZE <star>` executes
  and records actual generic per-join rows keyed by `JoinReplayId`.
- `measured_vs_estimated_record_carries_both_sides` - the record has 178's `estimated_rows`
  and the runtime `actual_rows` for the same replay id + ratio + source.
- `known_miss_star_runtime_evidence_shows_divergence` - a deterministic known-wrong-estimate
  star, executed under analyze, yields `actual_rows ≠ estimated_rows` by the known factor.
- `runtime_measurement_is_observational` - the bound input plan and result rows are
  byte-identical with/without analyze capture; no re-plan.

## Supporting Gates

- `explain_analyze_was_previously_rejected_now_implemented` - `EXPLAIN ANALYZE` no longer
  returns unsupported; it runs and measures.
- `ordinary_execution_captures_nothing` - non-analyze run captures no actuals even though
  task telemetry exists (hot path).
- `non_star_join_is_not_captured_by_star_analyze_allowlist` - a non-star join in the same
  analyzed plan is not recorded unless its replay id is in the explicit set.
- `measured_record_correlates_by_replay_id` - actual maps to the right dimension/estimate by
  id, not position (distributed merge correctness).
- `measurement_does_not_act` - the measured signal triggers no re-plan / margin change.
- `cockpit_renders_estimated_vs_actual` - the analyze view shows both sides; SQL + Rune parity.

## Architecture Guards

- `adr179_runtime_evidence_is_observational` - capture/emit mutates no plan/result/decision.
- `adr179_measures_does_not_act` - no re-plan / margin mutation consumes the measured signal
  (distinct from `broadcast_cap_replay_trigger`).
- `adr179_actual_correlates_via_replay_id` - measured↔estimated joined by `JoinReplayId`,
  reusing the replay correlation/aggregation.
- `adr179_reuses_178_estimate_no_second_estimator` - estimated side is 178's value.
- `adr179_capture_is_cheap_and_analyze_gated` - the exec capture counts rows (no buffer/
  mutation) and only when the analyze replay-id mode is set.
- `adr179_capture_allowlist_is_replay_id_only` - the capture context carries
  `JoinReplayId`s, not star-order model types.
- `adr179_no_star_order_vocabulary_in_exec` - executor code records generic join
  observations without `StarOrder*`, `JoinOrderEdgeEvidence`, or `join_order` imports.
- `adr179_no_fragment_or_wendelon_storage_pbc_change` - no `fragment.rs:80` topology,
  Wendelon, storage descriptor, catalog, or PBC change. Toron task/context payload may
  carry the explicit observation mode.
- `adr179_no_optimizer_shell` - 177's no-cathedral guard still holds.

## Non-Goals

- **no acting on the measurement** — no adaptive re-plan, no margin auto-tuning (deferred);
- no hash/non-broadcast or general-graph estimated-vs-actual analysis (178 broadcast star
  edges only this cut);
- no always-on production telemetry beyond the `EXPLAIN ANALYZE` record;
- no new optimizer fact or second estimator;
- no `fragment.rs:80` topology or executor behavior change beyond the cheap
  analyze-gated capture;
- no Wendelon, storage descriptor, catalog, or PBC wire change. Toron task/context payload
  may carry the explicit observation mode.

## Consensus Trail

The council chose *runtime* evidence over a static miss fixture. Grounding made it
tractable and safe: `EXPLAIN ANALYZE` is already parsed (just stubbed), `JoinReplayId`
ties runtime actuals to plan estimates, the exec capture hook is precise, and worker task
telemetry already returns runtime counters. Codex's review tightened two details: analysis
must be a bind-and-execute surface rather than a normal `bind_explain` render, and capture
must be gated by an explicit replay-id mode rather than by telemetry presence. The cut
only *measures* — the replay path's *acting* is the deferred contract.

## Forward Arc

`178 actuation -> 179 runtime evidence (EXPLAIN ANALYZE) -> {act on it: adaptive re-plan /
auto-tuned margin from measured miss} / hash-non-broadcast star order -> snowflake/chain ->
bounded enumeration -> outer-join safety -> relax left-deep -> bridges -> cycles -> memo`.

After 179 the loop is closed observationally: 178 acts, 179 measures whether the act was
right. Acting on that measurement (adaptive) is the next big robustness rung, now justified
by real data.

## Deferred (named)

Acting on the measurement: adaptive re-plan on measured miss, soft-margin auto-tuning from
measured estimate-vs-actual; always-on production measured telemetry; hash/non-broadcast +
general-graph estimated-vs-actual analysis; per-operator metrics beyond star-join
cardinality.
