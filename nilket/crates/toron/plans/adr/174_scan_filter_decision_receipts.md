# ADR 174: Scan/Filter Decision Receipts

**Status:** Promoted. **Migration:** `plans/impl/174_scan_filter_decision_receipts.md`.

The optimizer cockpit exists but is **join-only**: `DecisionReceipt`
(`binder.rs:145`) records join strategy/placement/reason, emitted once
(`binder.rs:1528`), rendered by `optimizer_explain_*` (`6992-7060`) and
`SystemTableKind::OptimizerExplain`. The **read path is invisible** — nothing shows
what predicate pushed to PBC vs stayed residual (173), which range fact was spent
and the selectivity it produced (139-166), or the profile hint. ADR 174 extends the
cockpit to the scan/filter path, **purely observationally** (records what the
planner already decided; changes no plan).

### Why (intuition)

The 139-166 fact spine and 173's pushdown do real, opaque work. 174 makes the same
EXPLAIN surface that already shows "join 0: broadcast, build 2KB < 8MB" also show
"scan[0] table_id=7: fully PBC-pushed, point-read, est 1.2k rows via range
col[1]." This is the Rune-primary inspection surface the cockpit doctrine names.

## Grounding

- `bind` discards the trace (`binder.rs:116`, `let (plan, _) = bind_with_trace(…)`);
  EXPLAIN renders it (`910-911`). **The trace is built on every query and thrown
  away unless explained.** So receipts must be **cheap** — the join receipt is a
  `Copy` struct of enums (no strings/clones), and the scan/filter receipt must match
  that, or every query pays formatting it will discard.
- The SELECT `LogicalPlan::Filter` arm (`binder.rs:613`) holds `&mut OptimizerTrace`
  and calls `plan_filter_with_predicate_pushdown` (`641`, free fn at `2154`,
  returning `(ExecutionPlan, QueryProfileHint)`).
- `FilterDisposition` (`ir/execution.rs:194`) is **binary**
  (`FullyPushed` | `EvaluateInToron`) — too coarse for the cockpit, which also needs
  static-elision, point-read, and partial-push. So the receipt derives a dedicated
  cheap outcome enum from the finalized plan (disposition + pbc presence + profile
  hint + the `AlwaysReject`→`EmptyRelation` elision branch at `~2160`), rather than
  storing `FilterDisposition` alone.
- The selectivity estimates are produced in the SELECT Filter arm before pushdown:
  equality/IN-list NDV (`filter_equality_ndv_estimated_row_count`) and range
  selectivity (`filter_range_selectivity_estimated_row_count`, including 173's
  converted range atom). Pushdown/static/profile evidence is produced by
  `plan_filter_with_predicate_pushdown`. The final receipt assembly belongs in the
  Filter arm, where both evidence streams and the trace are visible.
- DML pushdown: `bind_delete` and `bind_update` (the second
  `plan_filter_with_predicate_pushdown` caller, `binder.rs:1629`) **carry no trace**,
  so they cannot emit receipts without new threading → DML is out of scope this cut.
- Rendering: `optimizer_explain_schema/_plan/_row` (`6992-7060`); both SQL EXPLAIN
  and Rune `explain()` (`rune_engine.rs:267`) lower to `LogicalPlan::Explain`.
  `optimizer_explain_plan(trace)` receives the trace only, not the catalog or source
  plan, so 174 renders stable ids/ordinals (`scan[0]`, `table_id=N`, `col[1]`)
  rather than pretending it can resolve table/column names from cheap receipts.

## Decisions

**D1 - `OptimizerTrace` gains a cheap scan/filter receipt stream.** Add
`scan_filter_decisions: Vec<ScanFilterReceipt>`. The receipt stores **structured,
cheap** data — no owned predicate strings, no plan clones — with a dedicated
outcome enum derived from the finalized plan:

```rust
struct ScanTraceTarget { scan_index: usize, table_id: Option<TableId> }
enum FilterEstimateSource {
    Range,
    ConvertedRange,
    NdvEquality,
    NdvInList,
    HeavyHitterTopK,
}
enum ScanFilterOutcome {
    FullyPushed,        // FilterDisposition::FullyPushed, all in PBC
    PartiallyPushed,    // pbc present + Toron residual
    ResidualOnly,       // EvaluateInToron, no pbc
    StaticElided,       // AlwaysReject → EmptyRelation
    PointRead,          // point-read profile hint
}
struct ScanFilterReceipt {
    target: ScanTraceTarget,
    outcome: ScanFilterOutcome,
    converted_pushdown: bool,              // 173 converted derivation present
    profile_hint: QueryProfileHint,
    estimated_rows: Option<RowCount>,
    spent_fact: Option<FilterEstimateSource>,
    spent_column: Option<ColumnOrdinal>,   // source ordinal, rendered as col[N]
}
```

All `Copy`/cheap, mirroring the join receipt precedent, so the always-built-then-
discarded trace stays inexpensive. The receipt stores ids/ordinals only; friendly
names and predicate text are deferred to an EXPLAIN-gated render path that has the
catalog/source context to resolve them.

**D2 - Built where the data lives; pushed where the trace lives.**
The SELECT Filter arm preserves the chosen selectivity evidence while computing the
row estimate. `plan_filter_with_predicate_pushdown` (and its `EmptyRelation` elision
branch) returns cheap pushdown evidence from the finalized plan: outcome,
converted-derivation marker, profile hint, and optional table id. The trace-holding
Filter arm combines the estimate evidence + pushdown evidence into a
`ScanFilterReceipt` and pushes it. Non-trace callers (DML) either ignore the
pushdown evidence or use a compatibility wrapper. The receipt is **derived from the
finalized plan**, never consulted by planning.

**D3 - Generalize the EXPLAIN schema to host both receipt kinds.** Restructure
`optimizer_explain_schema` to a unified decision table with a `decision_kind`
discriminator (`"join"` | `"scan_filter"`) and shared columns:

```text
decision_kind, target, outcome, detail,
estimated_rows, spent_fact, spent_column,   -- scan/filter (null for join)
build_side, build_bytes, threshold_bytes, rejected_side_bytes  -- join (null for scan)
```

Join rows map onto `{outcome=strategy, detail=reason, build_*}`; scan/filter rows map
onto `{outcome=ScanFilterOutcome label (+ "converted" marker), detail=profile hint,
estimated_rows, spent_fact, spent_column=col[N]}`. `target` renders as
`scan[index]` with optional `table_id=N`, not a resolved table name. In-flight schema
change to an observational system table (no backward-compat scaffolding); it travels
with 174. Predicate **text** and friendly names are intentionally not in the receipt
this cut (they would force per-query string formatting/clones or extra render
context for the discarded trace) — the outcome + spent-fact ordinal + estimate are
the legible core; full predicate text/name resolution is a deferred EXPLAIN-gated
follow-on.

**D4 - Observational invariant: no plan changes.** 174 records + renders only. The
chosen plan, `FilterDisposition`, PBC program, estimate, and join strategy for any
query are unchanged. Pinned by (a) the entire existing pushdown/plan/selectivity
test suite staying green (plans byte-identical), and (b) an architecture guard that
decision code (`plan_filter_with_predicate_pushdown`'s decisions, `choose_*`,
selectivity) never *reads* `scan_filter_decisions`. (The trace is always built, so
there is no "with/without receipts" plan to compare — the invariant is
plans-unchanged, proven by the green suite.)

**D5 - Scope: SELECT read-path scan/filter this cut.** Receipts cover the
trace-bearing SELECT Filter path: outcome (pushed/partial/residual/elided/point-read),
profile hint, and the selected NDV/range selectivity estimate. **Deferred:** DML
(delete/update) receipts (no trace threading), aggregate/projection/join-input
receipts, friendly name resolution, and predicate-text rendering. Join receipts are
unchanged (only re-hosted under the unified schema).

**D6 - Frontend-shared.** SQL EXPLAIN and Rune `explain()` render through the same
`LogicalPlan::Explain` → `optimizer_explain_plan` path. No frontend-specific code.

**D7 - Binder/system-table only.** No executor, PBC, storage, Wendelon, descriptor,
optimizer-fact-producer, or wire change.

## Headline Gates

- `scan_filter_receipt_records_pushdown_outcome` - `WHERE region = 'us'` produces a
  `scan_filter` EXPLAIN row whose `outcome` reflects the push outcome
  (fully/partial/residual) for the scanned table.
- `scan_filter_receipt_marks_converted_pushdown` - `WHERE CONVERT(distance_mile,
  Kilometer) > 10` (173) produces a row with the converted marker set.
- `scan_filter_receipt_records_selectivity_estimate` - a range-filtered scan over a
  column with range facts shows `estimated_rows`, `spent_fact = range`, and
  `spent_column = col[1]` (the source ordinal).
- `explain_plans_unchanged_under_receipts` - the bound `ExecutionPlan` /
  `FilterDisposition` / PBC presence for representative queries match the pre-174
  pushdown expectations (the observational invariant via the existing suite).

## Supporting Gates

- `scan_filter_receipt_records_residual_only` - a non-pushable predicate →
  `ResidualOnly`.
- `scan_filter_receipt_records_static_elision` - a refinement-elided predicate
  (`EmptyRelation`) → `StaticElided`.
- `scan_filter_receipt_records_point_read` - a point-read profile → `PointRead`.
- `join_receipts_unchanged_under_unified_schema` - join rows preserve
  strategy/placement/reason/build columns (129/130 regression).
- `explain_unified_schema_has_decision_kind` - join and scan/filter rows coexist,
  correct null columns per kind.
- `rune_explain_renders_scan_filter_receipts` - Rune `explain()` parity.
- `scan_filter_receipt_is_cheap_structured` - the receipt holds no owned predicate
  string / cloned program (a type-shape assertion that it stays cheap).

## Architecture Guards

- `adr174_optimizer_trace_carries_scan_filter_receipts` - `OptimizerTrace` has
  `scan_filter_decisions`; pushdown evidence returned by
  `plan_filter_with_predicate_pushdown`, final receipt assembled and pushed in the
  Filter arm.
- `adr174_scan_filter_receipt_is_observational` - planning/decision code does not
  read `scan_filter_decisions`; the receipt is built from the finalized
  outcome/estimate.
- `adr174_pushdown_helper_does_not_mutate_trace` -
  `plan_filter_with_predicate_pushdown` returns evidence and has no `OptimizerTrace`
  parameter.
- `adr174_scan_filter_receipt_is_cheap` - `ScanFilterReceipt` stores ids/ordinals
  and `Copy` enums, not predicate strings or programs.
- `adr174_explain_schema_is_unified_decision_kind` - schema has `decision_kind` +
  both column groups; both row builders exist.
- `adr174_join_receipts_preserved` - join rows still carry the join columns.
- `adr174_no_runtime_storage_or_producer_surface` - no executor/PBC/storage/
  Wendelon/fact-producer change.
- `adr174_cockpit_is_frontend_shared` - render via `optimizer_explain_plan`; no
  Rune-specific cockpit code.

## Non-Goals

- no change to any plan, disposition, estimate, or join decision (observational);
- no DML (delete/update) receipts (no trace there);
- no aggregate/projection/join-input receipts;
- no predicate-text rendering (deferred; would cost the discarded trace);
- no friendly table/column name rendering (deferred; render has trace only);
- no interactive cockpit / overrides / gauges (later arc);
- no new optimizer fact, producer, PBC, storage, or wire surface.

## Consensus Trail

After the unit arc, the council pivoted to optimizer legibility. Grounding corrected
a false greenfield (the cockpit/`DecisionReceipt`/`optimizer_explain` exist but are
join-only), a cost trap (the trace is built-then-discarded on every query, so
receipts must be cheap, not string-formatted), a render-context trap
(`optimizer_explain_plan` has only the trace, so cheap receipts render ids/ordinals,
not names), and a granularity gap (`FilterDisposition` is binary, so the receipt
derives a dedicated outcome enum). So 174 is a cheap, structured scan/filter
receipt — selectivity evidence preserved in the Filter arm, pushdown evidence
returned by the pushdown helper, final receipt pushed where the trace lives — with a
unified EXPLAIN schema and a hard observational invariant. DML, aggregate, friendly
name, and predicate-text receipts are deferred.

## Forward Arc

`173 converted pushdown -> 174 scan/filter decision receipts -> DML + aggregate +
join-input receipts / predicate-text (EXPLAIN-gated) / interactive Rune cockpit
(gauges, overrides)`.

After 174 the read-path decisions are inspectable through the same cockpit as join
strategy; richer coverage and the interactive cockpit are the named follow-ons.

## Deferred (named)

DML scan/filter receipts; aggregate/projection/join-input receipts; EXPLAIN-gated
predicate-text and friendly-name rendering; interactive cockpit (overrides, gauges,
what-if); per-operator cost surfacing; affine + equality converted pushdown; unit
display.
