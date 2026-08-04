# ADR 187: Optimizer Decision Accrual

**Status:** Promoted. **Migration:** `plans/impl/0187_optimizer_decision_accrual.md`.

177–186 built ten rungs of join-order instruments, all `EXPLAIN ANALYZE`-gated and per-query.
187 opens the **telemetry** surface: it accrues the optimizer's join-order **decisions**
(reordered/kept/declined-and-why, per level) across the whole workload into a coordinator-scoped
log, surfaced as a queryable `information_schema.optimizer_decisions` table. It reads the
instruments we built — at scale, not one manual EXPLAIN at a time — so the next (bigger,
surface-opening) bet is grounded in data instead of speculation.

It is **decisions-only**: the actuation decisions the binder already computes during normal
binding, recorded always-on with no execution-capture change. Always-on *measured*-verdict
accrual (runtime rows) is the deferred follow-on; this cut needs no executor or storage change.

## Grounding

- **The decision is already computed always-on; only the receipt is EXPLAIN-gated.**
  `try_bind_star_order_actuation` reorders or declines for every query (the plan is actually
  reordered), but the receipt lands in `render_trace` only under EXPLAIN. Accrual records the
  decision the binder already makes, independent of `render_trace`.
- **Record committed decisions, not the scratch preflight.** 186 plans branch + root in a
  scratch pass, then commits a real pass only if the root actuates (commit-or-discard). The
  decline returns (`Ok(None)`) and the real `LocalStarActuationAttempt::Actuated(real_root)` +
  `branch_receipts` are the **committed** decisions; the scratch attempt is exploration. Accrual
  records at the committed points only, so a discarded branch is never counted.
- **The level kind is in the tree.** `SnowflakeStarTree::has_branch_dimensions()` distinguishes a
  flat star (no branches → `FlatStar`) from a snowflake root (`SnowflakeRoot`); recursive branch
  receipts are `SnowflakeBranch`.
- **A queryable report path already exists.** `information_schema.*` tables bind through
  `bind_information_schema_relation` (`binder.rs:656`) into a `SystemTable` plan. The report is a
  sibling `information_schema.optimizer_decisions`, no new system-table mechanism.
- **The report-read forces the log into the binder; config is the lowest-churn thread.** The
  report rows come from the log and the binder binds the report, so the log must reach the
  binder regardless of where writes happen. `PlannerConfig` is already borrowed into every bind
  and into the ~12 coordinator bind sites, so a shared `Arc<OptimizerDecisionLog>` handle there
  serves both read and write with no new Binder field and no coordinator bind-site change. It is
  a sink **handle**, not a planning setting; the side-effect-only guard keeps it out of planning.
- **Session is coordinator-process-scoped.** `SessionId` is effectively constant, so accrual is
  per-coordinator-process — exactly "this deployment's workload."

## Decisions

**D1 - A coordinator-scoped shared decision log.** Add `OptimizerDecisionLog` (interior-mutable,
in-memory, process-scoped), constructed by `QueryCoordinator`, shared as
`Arc<OptimizerDecisionLog>`. It aggregates counts keyed by `(decision_kind, outcome)`:
`decision_kind ∈ {StarCandidate, FlatStar, SnowflakeRoot, SnowflakeBranch}`,
`outcome ∈ {Reordered, KeptOptimal, KeptThinMargin, Declined(reason)}`. `StarCandidate` is only
for pre-classification declines where relation labels / tree shape are unavailable; once the
tree is classified, use the precise flat/root/branch kind.

**D2 - Thread the handle via `PlannerConfig`.** A shared `Arc<OptimizerDecisionLog>` rides in
`PlannerConfig` (defaulted to a fresh empty log). The coordinator constructs one and stores it in
its config; bind sites are unchanged. It is documented as the accrual sink handle, not a setting.
The dedicated-bind-context alternative is rejected (more churn, same field-breadth risk, and the
report-read needs the log in the binder anyway).

**D3 - Record only committed decisions.** In `try_bind_star_order_actuation`, record to the log:
at the **decline returns** one `Declined(reason)` for the committed candidate; at the **real
`Actuated(real_root)` commit** the root outcome plus one `SnowflakeBranch` entry per committed
`branch_receipt`. The scratch/preflight pass records nothing. `decision_kind` is `StarCandidate`
for pre-tree declines, then `SnowflakeRoot` if `tree.has_branch_dimensions()` else `FlatStar`
after classification. Here "committed" means the **real (non-scratch) pass** within a bind — the
186 commit-or-discard distinction; it is not cross-query dedup. A broadcast-cap force-hash replay
re-binds, so a replayed query accrues both its pre-cap and forced-hash real decisions; refining
replay dedup is a noted minor follow-on, not this cut's concern. Recording is independent of
`render_trace`.

**D4 - A queryable `information_schema.optimizer_decisions` report.** Bind it through the existing
information-schema relation path into a `SystemTable` whose rows are the log's aggregated counts —
one row per `(decision_kind, outcome, decline_reason, count)`, sorted for determinism. SQL and
Rune share the path.

**D5 - Decisions-only; no runtime capture.** The log records plan-time actuation decisions, not
runtime measurements. Always-on *measured*-verdict accrual needs the deferred execution-capture
surface and is a named follow-on.

**D6 - Side-effect-only invariant (non-report queries).** Recording a decision never changes
planning: a non-report query's bound `ExecutionPlan` and results are byte-identical with and
without the log. The report table itself intentionally reflects live accrual — its plan (a
`SystemTable` scan) is fixed but its rows are the current snapshot — so the side-effect-only gate
is asserted on a non-report query. Binding the report only *reads* the log; querying the report
contains no join, so it accrues nothing (no self-accrual). Pinned by the full suites staying
green plus a guard that no planning decision reads the log.

**D7 - In-memory, bounded, process-scoped; join-order only.** The log lives as long as the
coordinator and is capped by number of aggregate keys; when the cap is exhausted, it increments a
deterministic overflow row instead of growing unboundedly. There is no storage/Wendelon
persistence; durability is deferred. 187 accrues star/snowflake actuation decisions only — the
foundation; scan/filter/strategy-receipt accrual is a later extension. A branch that locally
declines inside a committed root has no receipt and is not separately accrued (a noted minor gap,
extensible later).

**D8 - No executor/fragment/storage/Wendelon change.** The cut is the log type + binder recording
+ the information-schema report. No `fragment.rs`, executor, PBC, storage, Wendelon, cost model,
optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Headline Gates

- `optimizer_decisions_accrue_across_queries` - bind a reordered star, a kept-declared star, and
  a declined snowflake through one coordinator, then `SELECT … FROM
  information_schema.optimizer_decisions`: the rows show the accrued counts per
  `(decision_kind, outcome)` summed across the queries.
- `optimizer_decision_accrual_is_always_on` - a normal (non-EXPLAIN) query's actuation decision
  is recorded; accrual is not gated on `render_trace`/EXPLAIN.
- `optimizer_decision_accrual_is_side_effect_only` - a **non-report** query's bound
  `ExecutionPlan` and results are identical with and without the decision log; recording changes
  no plan.

## Supporting Gates

- `optimizer_decisions_record_committed_not_scratch` - a bushy snowflake whose root commits
  records one `SnowflakeRoot` + one `SnowflakeBranch` per committed branch — not the scratch
  preflight; a root-declined snowflake records exactly one `Declined` root entry, no branch
  entries.
- `optimizer_decisions_report_aggregates_by_kind_and_outcome` - repeated identical queries
  increment one row's count, not new rows.
- `optimizer_decisions_records_decline_reasons` - a mixed/broadcast-root snowflake records a
  `Declined(mixed_strategy_star)` row.
- `optimizer_decisions_records_unclassified_candidate_declines` - a pre-tree decline (for
  example missing labels) records as `StarCandidate`, not a fake `FlatStar` or `SnowflakeRoot`.
- `optimizer_decisions_records_per_level_for_recursive_snowflake` - a bushy snowflake records a
  `SnowflakeRoot` and a `SnowflakeBranch` decision (the 186 edge-owned levels).
- `optimizer_decisions_log_is_bounded` - distinct keys beyond the cap increment a deterministic
  overflow row instead of growing the map.
- `rune_queries_optimizer_decisions` - Rune reads the report through the shared path.

## Architecture Guards

- `adr187_decision_log_is_coordinator_scoped_shared` - the log is an `Arc` held by
  `QueryCoordinator` and threaded via config; not per-query Binder state.
- `adr187_accrual_recording_is_always_on_not_explain_gated` - the binder records the decision
  independent of `render_trace`.
- `adr187_accrual_records_committed_not_scratch` - recording is at the real commit / decline
  return, not the scratch preflight pass.
- `adr187_accrual_is_side_effect_only` - the log is write-only from planning and read-only for the
  report; no planning decision reads it.
- `adr187_report_reuses_information_schema_path` - the report binds via
  `bind_information_schema_relation`; no new system-table machinery beyond the variant.
- `adr187_decisions_only_no_runtime_capture` - the log has no `JoinRuntimeObservation`/output-row
  field; recording is plan-time only.
- `adr187_decision_log_is_bounded` - the log has a key cap and overflow counter/row; it is not an
  unbounded workload sink.
- `adr187_no_executor_fragment_storage_or_wendelon_change` - in-memory log; no `fragment.rs`,
  executor, PBC, storage, or Wendelon change.
- `adr187_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.

## Rejected Alternatives

**A dedicated bind context instead of the config handle.** More threading churn and the same
new-field breadth risk, and the report-read needs the log in the binder regardless. The config
handle is the lowest total churn; the smell is mitigated by it being a sink handle plus the
side-effect-only guard.

**Record from the scratch/preflight pass.** Over-counts discarded branches (commit-or-discard).
Record only committed decisions.

**Persist the log to storage.** Durable accrual opens the storage/Wendelon surface; the in-memory
process-scoped log answers the workload-evidence question first.

**Accrue measured runtime verdicts now.** Needs always-on execution capture (beyond EXPLAIN
ANALYZE) — a bigger build. Decisions-first grounds whether it is worth it.

**A bespoke command instead of a queryable table.** `information_schema` already gives a
composable, frontend-shared, `SELECT`-able report.

## Non-Goals

- no always-on measured/runtime verdict accrual (needs execution capture);
- no persistence / cross-restart durability (in-memory, process-scoped);
- no margin self-tuning (consumes the accrual; deferred);
- no scan/filter/strategy-receipt accrual (join-order decisions only this cut);
- no executor, fragment, storage, PBC, wire, Wendelon, or catalog-trait change;
- no cost model, optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Consensus Trail

The arc reached its bounded envelope; 187 opens the lightest new surface — telemetry — to ground
the next bet. It records the join-order decisions the binder already computes (always-on, no
execution change, committed-only so commit-or-discard is not over-counted) into a
coordinator-scoped log threaded via config, surfaced as a queryable
`information_schema.optimizer_decisions` report reusing the existing information-schema path.
Decisions-only, side-effect-only, in-memory. The report tells us — across the real workload — how
often the optimizer acts vs declines and why, ordering the broadcast-cliff, mixed-strategy, and
galaxy/bridge bets.

## Forward Arc

`187 decision accrual -> always-on measured/runtime verdict accrual (execution capture) ->
margin self-tuning (consumes accrual) -> [evidence-ordered] broadcast-snowflake cliff /
mixed-strategy cost model / galaxy-bridge topology`.

After 187 the optimizer's behavior is legible across the workload, not one EXPLAIN at a time — the
grounding that turns the next surface-opening bet from a guess into a measured choice.

## Deferred (named)

Always-on measured/runtime verdict accrual; persistence/durability; margin self-tuning;
scan/filter/strategy-receipt accrual; the broadcast-snowflake execution cliff; mixed-strategy cost
model; galaxy/bridge topology.
