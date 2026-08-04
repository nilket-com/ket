# ADR 188: Always-On Join-Order Runtime Verdict Accrual

**Status:** Promoted. **Migration:** `plans/impl/0188_always_on_join_order_runtime_verdict_accrual.md`.

187 accrued the optimizer's join-order **decisions** (what it did) across the workload. 188
accrues the **measured outcomes** (whether it was right): for a sampled fraction of normal
queries, capture the runtime join observations the executor already knows how to produce,
compute the 180/184 measured verdict, and accrue per-level `Vindicated / MissedWin / OverEager /
MisRanked / Indeterminate` counts into the 187 log — surfaced as a sibling
`information_schema.optimizer_runtime_verdicts` report. This is the outcome half of the grounding
loop; the data that justifies self-tuning (189) and orders the topology/executor bets.

The grounded surprise: **no executor change is needed.** The capture machinery
(`JoinRuntimeObservationMode::MeasureJoins`) and the verdict logic (180/184) already exist for
`EXPLAIN ANALYZE`; 188 reuses them on a *sampled normal* coordinator path, returning the user's
data while accruing the verdict instead of rendering it. The new surface is the coordinator's
sampled-measurement path, bounded by a sample rate (default off).

## Grounding

- **Capture is mode-gated and already implemented.** `JoinRuntimeObservationMode` is `Disabled`
  by default and `MeasureJoins { replay_ids }` under EXPLAIN ANALYZE; the executor produces
  `JoinRuntimeObservation`s for the moded joins (`exec.rs:2523+`), flowing back via
  `TaskTelemetry.join_observations`. A sampled normal query sets `MeasureJoins` for its actuated
  joins — no executor change.
- **`bind_explain_analyze` already yields plan + instrumentation.** It returns
  `ExplainAnalyzeBinding { plan (user data), replay_ids, trace, render_trace (evidence) }`. The
  `plan` returns the user's rows; `replay_ids` + `render_trace` are the metadata the verdict
  needs. A sampled query executes `plan` (user data) with `MeasureJoins(replay_ids)` and computes
  the verdict from `render_trace` + observations.
- **The verdict logic is reusable; only its sink changes.** `render_explain_analyze` →
  `optimizer_analyze_plan` computes per-level 180/184 verdicts and renders rows. 188 extracts the
  verdict *computation* (the per-receipt `measured_*_verdict` calls) and accrues the verdict
  classes instead of rendering — same logic, different sink.
- **187's log is the accrual home.** The coordinator already holds a shared
  `Arc<OptimizerDecisionLog>` via config. 188 extends that shared log with a measured-verdict count
  lane keyed by `(decision_kind, verdict_class)`, surfaced through the same information-schema path.
- **The existing execute paths already have telemetry homes.** The distributed admission path
  (`execute_query_with_admission`, `coordinator.rs:750`) already executes via
  `execute_bound_distributed_attempt_with_telemetry` (`:1173`), returning `TaskTelemetry`; the local
  ANALYZE path already uses `Arc<Mutex<TaskTelemetry>>`. The sampled path sets `MeasureJoins` and
  reads the telemetry those paths already produce; **no new executor capture plumbing**,
  admission/budget semantics intact.
- **`bind_explain_analyze`'s plan equals the normal bind.** The render trace is observational
  (the 177 byte-identical-plan invariant), so executing `binding.plan` returns the same user rows
  as `bind(plan)`. Sampling changes overhead, never results.
- **The query still returns user data.** 188 executes the normal query plan (not the ANALYZE
  system table); the verdict is a side-accrual. Sampled queries pay capture + verdict overhead;
  the sample rate bounds it.

## Decisions

**D1 - A sample-rate gate (default off).** Add a `measured_accrual_sample_rate: u32` to
`PlannerConfig` (0 = off, N = measure every Nth query) plus a coordinator-scoped counter
(`Arc<AtomicU64>`, alongside the log). Default 0: zero overhead, behavior identical to pre-188.
`N=1` measures every query; large `N` samples rarely.

**D2 - A sampled measured path inside the normal read path.** When the gate fires for a normal
read query, the distributed admission path binds via `bind_explain_analyze` (plan + replay_ids +
render_trace), sets `MeasureJoins(replay_ids)` on the execution context, runs its existing
`execute_bound_distributed_attempt_with_telemetry` (admission/budget unchanged), reads
`output.task_telemetry` observations, accrues the per-level verdict classes, and returns
`output.batches` (user rows). The local path mirrors the same bind/mode/verdict flow using its
existing telemetry context. Non-sampled / non-read / off queries take the unchanged path (187
decision accrual only). No new executor capture plumbing.

**D3 - Reuse capture + verdict; render stays byte-identical.** 188 reuses the executor capture (no
change), `bind_explain_analyze`, and the 180/184 `measured_*_verdict` classifiers. Add a parallel
`measured_verdicts(render_trace, observations) -> Vec<(kind, verdict_class)>` that builds
`measured_rows_by_dimension` (sharing the `measured_rows_for_receipt` helper with the renderer)
and calls the same `measured_*_verdict` classifiers — returning classes instead of rendering rows.
`optimizer_analyze_rows` is untouched except for sharing the rows helper, so EXPLAIN ANALYZE output
is byte-identical; the accrued class equals the rendered verdict (pinned).

**D4 - Extend the accrual log + a runtime verdict report.** Add measured-verdict counts keyed by
`(decision_kind, verdict_class)` to the log, surfaced as
`information_schema.optimizer_runtime_verdicts` (columns `decision_kind`, `verdict`, `count`) via
the existing information-schema path. `optimizer_decisions` (187) is unchanged. The
`verdict_class` set spans both classifiers: the 5-variant star verdict (`Vindicated`/`MissedWin`/
`OverEager`/`MisRanked`/`Indeterminate`) for actuated `StarReceipts` levels and the 3-variant
snowflake verdict (`Vindicated`/`MissedWin`/`Indeterminate`) for `ObservedSnowflake` levels —
whichever the query's render state carries. A sampled query with no recognized join-order level
accrues nothing (the bind is slightly heavier for no gain — a bounded sampling waste, acceptable).

**D5 - Side-effect-only; user results unchanged.** A sampled query returns the same rows as the
unsampled path; capture and verdict computation never change the result or the plan. Pinned by
the sampled-vs-unsampled results being identical and a guard that the verdict log never feeds
planning.

**D6 - Bounded overhead by sampling; conservative default.** Default rate 0 (off). The capture
cost is paid only on sampled queries; the operator chooses the rate. No always-on cost when off.

**D7 - Coordinator-scoped, in-memory; decisions+verdicts only.** The log lives with the
coordinator (no persistence; deferred). 188 accrues join-order measured verdicts only — the
outcome counterpart to 187's decisions. Self-tuning (consuming these) is 189.

**D8 - No executor/fragment/storage/Wendelon change.** The capture machinery exists; the cut is
the coordinator sampled path + the log/report extension + the verdict-extraction refactor. No
`exec.rs`/`fragment.rs` capture change, no storage, PBC, Wendelon, cost model, optimizer shell,
memo, Cascades, or broad `PlanProperties` contract.

## Headline Gates

- `measured_verdicts_accrue_for_sampled_queries` - with `sample_rate=1`, run a snowflake whose
  declared order loses on measured rows; `SELECT … FROM
  information_schema.optimizer_runtime_verdicts` shows a `(snowflake_root|snowflake_branch,
  missed_win)` count, and a vindicated query shows `vindicated`.
- `measured_accrual_off_by_default_is_zero_overhead` - with `sample_rate=0`, no query is
  instrumented and the verdict report is empty; the executed results and plan are identical to
  pre-188.
- `sampled_query_returns_user_data_not_analyze_table` - a sampled query returns the same user
  rows as the unsampled query; only the verdict log differs.

## Supporting Gates

- `measured_accrual_samples_every_nth_query` - `sample_rate=3` instruments queries 3, 6, 9 …;
  the others are uninstrumented.
- `measured_verdict_reuses_180_184_computation` - the accrued verdict class equals the
  `EXPLAIN ANALYZE` verdict for the same query (same logic, different sink).
- `measured_verdicts_record_per_level_for_recursive_snowflake` - a bushy snowflake accrues a
  `snowflake_root` and a `snowflake_branch` verdict.
- `optimizer_runtime_verdicts_report_aggregates_by_kind_and_class` - repeated identical sampled
  queries bump one row's count.
- `rune_queries_optimizer_runtime_verdicts` - Rune reads the report.

## Architecture Guards

- `adr188_measured_accrual_reuses_capture_and_verdict` - the sampled path uses
  `JoinRuntimeObservationMode::MeasureJoins`, `bind_explain_analyze`, and the 180/184
  `measured_*_verdict`; no new capture mode or verdict classifier.
- `adr188_no_executor_capture_change` - `exec.rs`/`distributed` observation production is
  unchanged; 188 only sets the existing mode from the coordinator.
- `adr188_sampled_path_returns_user_data` - the sampled path executes the query plan and returns
  user rows; it does not return the analyze system table.
- `adr188_measured_accrual_is_side_effect_only` - the verdict log is write-only from execution and
  read-only for the report; no planning/execution decision reads it.
- `adr188_default_off_zero_overhead` - `sample_rate` defaults to 0; the measured path is not
  entered when off.
- `adr188_report_reuses_information_schema_path` - the verdict report binds via
  `bind_information_schema_relation`; no new system-table machinery.
- `adr188_no_storage_or_wendelon_change` - in-memory log; no storage/PBC/Wendelon change.
- `adr188_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.

## Rejected Alternatives

**Change the executor to capture always-on.** Unnecessary — the executor already captures under
`MeasureJoins`; the coordinator sets that mode on the sampled path. No executor change, no
always-on cost.

**Measure every query (no sampling).** Pays capture + verdict cost on every query. A sample rate
(default off) bounds it; `N=1` is the measure-every-query special case for the operator to choose.

**Re-render the ANALYZE table and parse verdicts.** Wasteful and fragile. Extract the verdict
*computation* into a shared helper; render and accrue both call it.

**A new verdict type for accrual.** Duplicative — 180/184 already classify; accrual records their
classes.

**Persist the verdict log.** Durable accrual opens the storage/Wendelon surface; in-memory
process-scoped answers the question first (matches 187).

## Non-Goals

- no margin self-tuning (consumes these verdicts; that is 189);
- no executor/fragment observation-capture change (mode set from the coordinator);
- no persistence / cross-restart durability (in-memory, process-scoped);
- no always-on (unsampled) measurement by default (sample rate, default off);
- no scan/filter accrual (join-order verdicts only);
- no storage, PBC, wire, Wendelon, or catalog-trait change;
- no cost model, optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Consensus Trail

The council took the outcome half of the grounding loop. 188 reuses the executor's existing
capture, `bind_explain_analyze`, and the 180/184 verdict logic on a sampled normal coordinator
path — returning the user's data while accruing the measured verdict into 187's log, surfaced as
`information_schema.optimizer_runtime_verdicts`. No executor change; bounded by a sample rate
(default off); side-effect-only. The report now answers not just how often the optimizer acts
(187) but whether it was right, measured — the data that justifies 189 self-tuning and orders the
topology bets.

## Forward Arc

`187 decision accrual -> 188 measured verdict accrual -> 189 margin self-tuning (consumes the
accrued verdicts) -> [evidence-ordered] broadcast-snowflake cliff / mixed-strategy cost model /
galaxy-bridge topology`.

After 188 the grounding loop is closed on the read side: the optimizer's decisions and their
measured outcomes are both legible at workload scale — the evidence base for every subsequent
bet.

## Deferred (named)

Margin self-tuning (189); persistence/durability; scan/filter measured accrual; unsampled
always-on measurement; the broadcast-snowflake cliff; mixed-strategy cost model; galaxy/bridge.
