# Impl Plan - ADR 175: EXPLAIN-Gated Cockpit Readability

Pairs with `plans/adr/0175_explain_gated_cockpit_readability.md`.

Add an EXPLAIN-only render sidecar for scan/filter receipt readability. The sidecar
captures predicate text and source-safe column names at the binder sites where that
data is still available. 174's cheap `ScanFilterReceipt` remains string-free and
`Copy`. Render rows prefer the sidecar readability fields when present and fall
back to 174 ids/ordinals. Observational; binder/system-table only.

## Anchors

| Concern | Anchor |
|---|---|
| EXPLAIN entry | `bind_explain` -> `bind_with_trace` + `optimizer_explain_plan(trace)` |
| Cheap trace | `OptimizerTrace`, `ScanFilterReceipt`, `adr174_scan_filter_receipt_is_cheap` |
| SELECT receipt emission | SELECT `LogicalPlan::Filter` arm; final receipt assembled after `plan_filter_with_predicate_pushdown_evidence` |
| Pushdown split | `PredicatePushdownPlan { pushed_predicate, residual_predicate, ... }`; pushed text must be captured before PBC-only lowering |
| Predicate renderer | `BoundExprProgram::pretty` and `pretty_node`; debug-flavored, includes column names |
| Stable target | `scan_table_id` extracts `TableId`; no general `TableId -> TableRef` reverse lookup in `Catalog` |
| EXPLAIN render | `optimizer_explain_schema`, `optimizer_explain_plan`, `optimizer_explain_scan_filter_row` |
| Frontend | `LogicalPlan::Explain`; Rune `.explain()` |
| Tests | optimizer-explain tests; `tests/architecture/cockpit.rs` |

## Phase 1 - Keep receipts cheap; add render sidecar

Leave `ScanFilterReceipt` unchanged in shape: no `String`, no
`BoundExprProgram`, no plan clone, and keep `Copy`.

Add binder-local render structs, for example:

```rust
#[derive(Debug, Clone, Default)]
struct OptimizerExplainRenderTrace {
    scan_filters: Vec<ScanFilterRenderReceipt>,
}

#[derive(Debug, Clone, Default)]
struct ScanFilterRenderReceipt {
    target: ScanTraceTarget,
    predicate_text: Option<String>,
    pushed_predicate_text: Option<String>,
    residual_predicate_text: Option<String>,
    spent_column_name: Option<String>,
}
```

The sidecar is not part of normal `OptimizerTrace` and is never read by planning.

## Phase 2 - Add an EXPLAIN-only bind/render path

Keep `bind_with_trace` as the cheap default for existing callers. Add either:

```rust
bind_with_explain_trace(...) -> Result<(ExecutionPlan, OptimizerTrace, OptimizerExplainRenderTrace), BindingError>
```

or a private lower-level helper that takes `Option<&mut OptimizerExplainRenderTrace>`.

`bind_explain` is the only caller that passes `Some(render_trace)` and then calls:

```rust
optimizer_explain_plan(trace, Some(render_trace))
```

Normal `bind` and `bind_with_trace` pass no render trace and do not build strings.

## Phase 3 - Capture predicate text before it is lost

At the SELECT Filter arm:

- keep the original bound predicate available for render capture;
- call the pushdown helper in a way that can expose EXPLAIN-only render text, or let
  the helper append to the render sidecar under `Some(render_trace)`;
- assign render entries with the same `scan_index` used by the cheap receipt.

Capture:

- `predicate_text = original_predicate.pretty()`;
- `pushed_predicate_text = pushed_predicate.pretty()` when a pushed program exists;
- `residual_predicate_text = residual_predicate.pretty()` when a residual program
  exists;
- static elision's original predicate before returning `EmptyRelation`.

Do not reconstruct pushed text from the final `ExecutionPlan`; partial pushdown may
only leave a `PbcProgram` plus residual plan.

## Phase 4 - Capture source-safe column names

Keep the 174 `spent_column` ordinal field. Add only a render-side
`spent_column_name`.

Recommended implementation:

- when a selected estimate has `spent_column`, and render capture is enabled, walk
  the original bound predicate for a `ResolvedColumn` with that source ordinal;
- use that column's `name` as the render label;
- if no unique source column is found, leave `spent_column_name = None`.

This avoids guessing through projected or derived plan shapes. It also avoids
requiring a table name or render-time catalog lookup.

## Phase 5 - Extend EXPLAIN schema and rows

Add nullable scan-only columns to `optimizer_explain_schema`:

```text
predicate_text Utf8?
pushed_predicate_text Utf8?
residual_predicate_text Utf8?
spent_column_name Utf8?
```

`optimizer_explain_plan` should render join rows and scan/filter rows using the
optional render trace. Match scan/filter cheap receipts to sidecar entries by
`ScanTraceTarget`/`scan_index`.

For scan/filter rows:

- keep `target` as the 174 stable label (`scan[N] table_id=M`);
- keep `spent_column` as `col[N]`;
- fill `spent_column_name` when the sidecar has a proven label;
- fill predicate-text columns when the sidecar has strings.

Join rows set the new scan-only columns to null.

## Phase 6 - Behavioral tests

Reuse the 174 optimizer-explain fixtures.

- `explain_scan_filter_renders_original_pushed_and_residual_predicates` - a
  partially pushed filter renders original, pushed, and residual text.
- `explain_scan_filter_static_elision_renders_original_predicate` - an elided
  contradiction still shows the original predicate.
- `explain_scan_filter_renders_spent_column_name` - fact spend renders
  `spent_column = col[N]` and `spent_column_name = amount`.
- `normal_bind_does_not_materialize_explain_render_strings` - cheap
  `bind_with_trace` has no render sidecar and leaves 174 receipts unchanged.
- `explain_plans_unchanged_under_render_capture` - cheap and EXPLAIN-rich binding
  produce identical execution plans.
- `converted_pushdown_predicate_text_shows_source_column` - converted pushdown
  text names the source unit column.
- `spent_column_name_declines_when_source_ordinal_is_ambiguous` - ambiguous
  mapping renders null.
- `join_rows_keep_scan_readability_columns_null` - join rows preserve 174 behavior.
- `rune_explain_renders_readable_scan_filter_receipts` - Rune parity.

## Phase 7 - Architecture guards

In `tests/architecture/cockpit.rs`:

- `adr175_render_trace_is_explain_only` - only `bind_explain` constructs/passes the
  render sidecar; normal `bind_with_trace` does not.
- `adr175_scan_filter_receipt_remains_cheap` - `ScanFilterReceipt` has no
  `String`, `BoundExprProgram`, or plan clone and remains `Copy`.
- `adr175_partial_pushdown_text_is_captured_before_lowering` - render capture sees
  `pushed_predicate` before PBC-only lowering.
- `adr175_predicate_text_uses_bound_program_pretty` - no SQL pretty-printer/parser
  dependency.
- `adr175_spent_column_name_is_source_ordinal_safe` - label lookup uses proven
  source ordinal evidence and fails open.
- `adr175_no_table_name_reverse_lookup_claim` - no required render-time
  `table_id -> TableRef` lookup through `Catalog`.
- `adr175_cockpit_is_frontend_shared` - SQL and Rune share `bind_explain` render.
- `adr175_no_runtime_storage_or_producer_surface` - no executor/PBC/storage/
  Wendelon/fact-producer/wire change.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib explain_scan_filter_renders_original_pushed_and_residual_predicates
scripts/cargo-cgroup test -p toron --lib explain_scan_filter_renders_spent_column_name
scripts/cargo-cgroup test -p toron --lib normal_bind_does_not_materialize_explain_render_strings
scripts/cargo-cgroup test -p toron --lib explain_plans_unchanged_under_render_capture
scripts/cargo-cgroup test -p toron --lib scan_filter_receipt
scripts/cargo-cgroup test -p toron --test rune_relation_handles_test rune_explain
scripts/cargo-cgroup test -p toron --test architecture_test adr175
scripts/cargo-cgroup test -p toron --test architecture_test adr174
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

## Risks

- **Cheap hot path.** Do not add strings to `ScanFilterReceipt`; render strings only
  under the EXPLAIN sidecar.
- **Partial pushdown.** Capturing after final plan construction can miss pushed
  text. Capture before lowering to `PbcProgram` or while `PredicatePushdownPlan`
  still has `pushed_predicate`.
- **Column label correctness.** A wrong friendly label is worse than null. Fail
  open when the source ordinal cannot be proven.
- **Renderer stability.** `pretty()` is debug-flavored. Tests should assert stable
  substrings, not pretend this is SQL serialization.
- **Observational invariant.** Capture/render only; existing pushdown and
  selectivity gates stay green.
- **Schema churn.** Existing EXPLAIN rows gain nullable columns in place. Retarget
  174 schema guards and Rune parity tests.

## Deferred

SQL-flavored predicate rendering and prefix trimming; friendly table-name render
map; interactive cockpit (read-path overrides, gauges); DML/aggregate/join-input
receipts; join-receipt enrichment; full plan-tree pretty-printer; multi-table name
disambiguation; affine + equality converted pushdown; unit display.
