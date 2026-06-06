# Impl Plan - ADR 174: Scan/Filter Decision Receipts

Pairs with `plans/adr/174_scan_filter_decision_receipts.md`.

Extend the join-only cockpit with a **cheap, structured** scan/filter receipt
stream. The SELECT Filter arm preserves selectivity evidence, the pushdown helper
returns pushdown evidence, and the Filter arm assembles/pushes the final receipt
where the trace lives. Render through a unified EXPLAIN schema using stable
ids/ordinals, not per-query strings. Observational: no plan/disposition/estimate/
join change. SELECT read-path only; DML/aggregate/names/predicate-text deferred.

## Anchors

| Concern | Anchor |
|---|---|
| Trace built-then-discarded | `bind` (`binder.rs:116`, `let (plan, _) = bind_with_trace`); EXPLAIN renders (`910-911`) |
| Trace structs | `OptimizerTrace` (`binder.rs:153`), `DecisionReceipt` (`:145`, `Copy`) |
| SELECT Filter emission site (has trace) | `LogicalPlan::Filter` arm (`binder.rs:613`), calls `plan_filter_with_predicate_pushdown` at `641` |
| Pushdown helper (return evidence here) | `plan_filter_with_predicate_pushdown` (`binder.rs:2154`) → `(ExecutionPlan, QueryProfileHint)`; `AlwaysReject`→`EmptyRelation` branch `~2160`; `predicate_pushdown_plan` (`:3030`) |
| Disposition (binary — derive richer outcome) | `FilterDisposition { FullyPushed, EvaluateInToron }` (`ir/execution.rs:194`) |
| Selectivity estimate | SELECT Filter arm computes equality/IN-list NDV + range estimates before pushdown (`binder.rs:628-633`); `filter_range_selectivity_estimated_row_count` (`:5300`); 173 `converted_linear_range_atom` |
| EXPLAIN render | `optimizer_explain_schema/_plan/_row` (`6992-7060`), `SystemTableKind::OptimizerExplain`; render receives `OptimizerTrace` only |
| No-trace DML callers (ignore receipt) | `bind_delete`; `bind_update` (`binder.rs:1629` caller), no `trace` param |
| Frontend | `LogicalPlan::Explain`; Rune `explain()` (`rune_engine.rs:267`) |
| Tests | optimizer-explain test module; new `tests/architecture/cockpit.rs` |

## Phase 1 - Cheap receipt types

In `binder.rs`, beside the join receipt types (keep `Copy` where possible):

```rust
pub(crate) struct ScanTraceTarget {
    pub scan_index: usize,
    pub table_id: Option<TableId>,
}
pub(crate) enum FilterEstimateSource {
    Range,
    ConvertedRange,
    NdvEquality,
    NdvInList,
    HeavyHitterTopK,
}
pub(crate) enum ScanFilterOutcome {
    FullyPushed, PartiallyPushed, ResidualOnly, StaticElided, PointRead,
}
pub(crate) struct ScanFilterReceipt {
    pub target: ScanTraceTarget,
    pub outcome: ScanFilterOutcome,
    pub converted_pushdown: bool,
    pub profile_hint: QueryProfileHint,
    pub estimated_rows: Option<RowCount>,
    pub spent_fact: Option<FilterEstimateSource>,
    pub spent_column: Option<ColumnOrdinal>,
}
pub(crate) struct OptimizerTrace {
    pub join_decisions: Vec<DecisionReceipt>,
    pub scan_filter_decisions: Vec<ScanFilterReceipt>,   // new
}
```

No owned `String`/`BoundExprProgram` in the receipt. `table_id` and `spent_column`
render as stable ids/ordinals (`table_id=N`, `col[N]`); friendly names are deferred
because `optimizer_explain_plan(trace)` has no catalog or source-plan context.

## Phase 2 - Preserve selectivity evidence in the Filter arm

The SELECT `LogicalPlan::Filter` arm already computes the candidate estimates before
calling pushdown. Change the estimate helpers or wrap their results so the chosen
estimate carries its source:

```rust
pub(crate) struct FilterEstimateEvidence {
    pub rows: RowCount,
    pub source: FilterEstimateSource,
    pub column: Option<ColumnOrdinal>,
}
```

- equality NDV / heavy-hitter equality → `NdvEquality` or `HeavyHitterTopK`;
- IN-list NDV / heavy-hitter IN-list → `NdvInList` or `HeavyHitterTopK`;
- plain range atom → `Range`;
- 173 converted range atom → `ConvertedRange`.

Keep the existing estimate semantics exactly: the Filter arm still chooses the
minimum candidate row count, but now carries along the evidence for the winning
estimate. If no estimate wins, the receipt fields are null. Capturing the evidence
must not alter the estimate or its planning effect.

## Phase 3 - Return pushdown evidence from the pushdown helper

Add a cheap helper return type:

```rust
pub(crate) struct ScanFilterPushdownEvidence {
    pub outcome: ScanFilterOutcome,
    pub converted_pushdown: bool,
    pub profile_hint: QueryProfileHint,
    pub table_id: Option<TableId>,
}
```

In `plan_filter_with_predicate_pushdown` (`binder.rs:2154`) derive the outcome from
values it already computes:

- `AlwaysReject` → `EmptyRelation` branch (`~2160`) → `StaticElided`;
- `FilterDisposition::FullyPushed` → `FullyPushed`;
- `EvaluateInToron` with a PBC program present → `PartiallyPushed`;
- `EvaluateInToron`, no PBC → `ResidualOnly`;
- profile hint point-read → `PointRead` (precedence per the cockpit's intent; a
  point-read scan with a pushed predicate may render `PointRead` with the push noted
  — pick one precedence and pin it in a gate).

Also return `converted_pushdown` (whether 173's converted derivation fired — a bool
the pushdown step already knows), `profile_hint`, and optional table id when the
input is a scan. Change the trace-bearing path to return
`(ExecutionPlan, QueryProfileHint, ScanFilterPushdownEvidence)`. DML can use a
compatibility wrapper or ignore the third value. The helper must not mutate
`OptimizerTrace`.

## Phase 4 - Assemble and push at the trace-bearing caller

In the Filter arm (`binder.rs:641`), currently
`let (plan, _) = plan_filter_with_predicate_pushdown(...)`:

- preserve the chosen `FilterEstimateEvidence` while computing `estimated_row_count`;
- `let (plan, _profile, pushdown) = plan_filter_with_predicate_pushdown(...);`
- build `ScanFilterReceipt` from `pushdown` + selected estimate evidence;
- assign `target.scan_index = trace.scan_filter_decisions.len()`;
- `trace.scan_filter_decisions.push(receipt);`

The `bind_update` caller (`1629`) and DML pushdown sites have no trace — they drop
the pushdown evidence. `scan_index` is assigned at push time so it is stable and
trace-local.

## Phase 5 - Unified EXPLAIN schema + render

Generalize `optimizer_explain_schema`:

```text
decision_kind Utf8, target Utf8, outcome Utf8, detail Utf8?,
estimated_rows Int64?, spent_fact Utf8?, spent_column Utf8?,
build_side Utf8?, build_bytes Int64?, threshold_bytes Int64?, rejected_side_bytes Int64?
```

- `optimizer_explain_plan`: render `join_decisions` then `scan_filter_decisions`.
- `optimizer_explain_row` (join): `decision_kind="join"`, `target="join[idx]"`,
  `outcome=strategy`, `detail=reason`, build columns; scan columns null.
- `optimizer_explain_scan_filter_row` (new): `decision_kind="scan_filter"`,
  `target` = `scan[index]` plus optional `table_id=N`, `outcome` =
  `ScanFilterOutcome` label (with a converted marker when `converted_pushdown`),
  `detail` = profile-hint label, `estimated_rows`, `spent_fact` label,
  `spent_column` = `col[N]`; join columns null.

Add `scan_filter_outcome_label`, `spent_fact_label`, `scan_filter_target_label`,
and `column_ordinal_label`. Do not add table/column name resolution in this cut:
the renderer has only `OptimizerTrace`, and storing names in the trace would violate
the cheap receipt invariant.

## Phase 6 - Behavioral tests

In the optimizer-explain test module (reuse the join-EXPLAIN harness; add a scanned
fixture with range facts + a `Mile`/`Kilometer` unit column + a `region` column):

- `scan_filter_receipt_records_pushdown_outcome` - `WHERE region = 'us'` →
  `scan_filter` row with the push outcome.
- `scan_filter_receipt_marks_converted_pushdown` - `WHERE CONVERT(distance_mile,
  Kilometer) > 10` → converted marker set.
- `scan_filter_receipt_records_selectivity_estimate` - range filter →
  `estimated_rows` set, `spent_fact=range`, `spent_column=col[1]`.
- `scan_filter_receipt_records_residual_only` / `_static_elision` / `_point_read`
  (the last pins the PointRead-vs-push precedence).
- `explain_plans_unchanged_under_receipts` - representative queries' bound
  `ExecutionPlan` / `FilterDisposition` / PBC presence match the 173-era
  expectations (observational invariant).
- `join_receipts_unchanged_under_unified_schema` - join rows preserve columns
  (129/130 regression).
- `rune_explain_renders_scan_filter_receipts` - Rune `explain()` parity.

The existing pushdown/selectivity suites staying green is itself the primary
observational-invariant evidence (D4).

## Phase 7 - Architecture guards

New `tests/architecture/cockpit.rs`:

- `adr174_optimizer_trace_carries_scan_filter_receipts` - field + pushdown evidence
  return from `plan_filter_with_predicate_pushdown` + final assembly/push in the
  Filter arm.
- `adr174_scan_filter_receipt_is_observational` - decision/selectivity/pushdown code
  does not read `scan_filter_decisions`.
- `adr174_pushdown_helper_does_not_mutate_trace` - `plan_filter_with_predicate_pushdown`
  returns evidence and has no `OptimizerTrace` parameter.
- `adr174_scan_filter_receipt_is_cheap` - `ScanFilterReceipt` contains no
  `String`/`BoundExprProgram` fields (ids/ordinals/`Copy` enums only).
- `adr174_explain_schema_is_unified_decision_kind` - `decision_kind` + both column
  groups + both row builders.
- `adr174_join_receipts_preserved` - join row builder emits join columns.
- `adr174_no_runtime_storage_or_producer_surface` - no executor/PBC/storage/
  Wendelon/fact-producer change.
- `adr174_cockpit_is_frontend_shared` - render via `optimizer_explain_plan`.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib scan_filter_receipt
scripts/cargo-cgroup test -p toron --lib explain_plans_unchanged_under_receipts
scripts/cargo-cgroup test -p toron --test architecture_test adr174
scripts/cargo-cgroup test -p toron --test architecture_test optimizer
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib   # pushdown/selectivity stay green = observational proof
git diff --check
```

## Risks

- **Observational invariant.** Emitting receipts must not change any plan/estimate;
  build from finalized values, never read receipts in planning. The green pushdown/
  selectivity suite is the proof.
- **Outcome granularity.** `FilterDisposition` is binary; derive `ScanFilterOutcome`
  from disposition + pbc presence + profile + elision branch; pin the PointRead-vs-
  push precedence in a gate.
- **Receipt cost.** Keep the receipt cheap (no strings/clones); the trace is built
  on every query and usually discarded. Pin with `adr174_scan_filter_receipt_is_cheap`.
- **Estimate capture coupling.** Return the estimate; do not recompute or alter it.
- **Wrong assembly site.** Selectivity evidence and pushdown evidence are split in
  the current code. Assemble the final receipt in the Filter arm, not inside the
  pushdown helper.
- **Schema change to EXPLAIN.** Join schema generalizes; reconcile join-EXPLAIN
  tests in the same cut, behavior-neutral.
- **Render context.** `optimizer_explain_plan` has only the trace. Render stable
  ids/ordinals in 174; do not smuggle names into the hot-path receipt.
- **One receipt per scan.** Push exactly once per SELECT filtered scan; ensure
  re-planning (`optimize_for_required`) does not double-emit.
- **DML scope.** DML callers lack the trace; confirm they drop the receipt (no
  partial DML cockpit rows).

## Deferred

DML scan/filter receipts; aggregate/projection/join-input receipts; EXPLAIN-gated
predicate-text and friendly-name rendering; interactive cockpit (overrides/gauges/
what-if); per-operator cost surfacing; affine + equality converted pushdown; unit
display.
