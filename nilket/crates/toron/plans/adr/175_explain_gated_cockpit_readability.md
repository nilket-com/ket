# ADR 175: EXPLAIN-Gated Cockpit Readability

**Status:** Promoted. **Migration:** `plans/impl/175_explain_gated_cockpit_readability.md`.

ADR 174 made the read-path cockpit **visible** but still **cryptic**:
scan/filter rows render stable ids and ordinals such as
`scan[0] table_id=7` and `spent_column=col[1]`, with no predicate text.
That was the correct hot-path cut: the optimizer trace is built for every
normal bind and discarded unless the query is explained, so receipts must
stay cheap and structured.

ADR 175 makes those rows readable through an **EXPLAIN-only render sidecar**.
It adds predicate text and source-safe column names when explaining, while
keeping 174's `ScanFilterReceipt` cheap, string-free, and `Copy`.

### Why not just render from the final plan?

The final bound plan is useful but insufficient. It can show a residual-only
predicate, and it can show a fully pushed predicate because the filter node still
carries a `BoundExprProgram`. But for a partially pushed predicate the final plan
has a `PbcProgram` on the scan plus a Toron residual filter above it; the pushed
`BoundExprProgram` may no longer be available as text. Reconstructing it from the
final plan would be lossy or would require correlating trace receipts back to plan
nodes by traversal order.

So the readable data is captured where the binder already has it: the SELECT
Filter emission path, around the pushdown split and receipt assembly. Normal binds
do not build this sidecar. EXPLAIN does.

## Grounding

- `bind_explain` currently calls `bind_with_trace(input, self.catalog,
  self.config)` and then `optimizer_explain_plan(trace)`. The bound input plan is
  thrown away; EXPLAIN is the trace's only public render path.
- `bind_with_trace` constructs `OptimizerTrace::default()` and has many direct
  test callers. Keeping it cheap avoids touching every receipt test and preserves
  174's invariant.
- `BoundExprProgram::pretty` already exists and renders stable internal predicate
  text with column names embedded, for example `root[0] = Gt(distance_mile#2,
  Float64(Some(...)))`. It is debug-flavored, not SQL round-trip text; a
  SQL-flavored renderer is deferred.
- `ResolvedColumn` nodes carry `name` and `ordinal`, so predicate text can include
  column names without a render-time catalog lookup.
- `ExecutionPlan::Scan` carries table ids/object ids and schema, but not a
  user-facing `TableRef`. `TableBinding` also does not store the original table
  name. Therefore 175 must not promise a general `table_id -> name` reverse lookup.
  Friendly table names require an explicit EXPLAIN-only name map and are deferred.
- 174's `ScanFilterReceipt` is `Copy` and string-free, pinned by
  `adr174_scan_filter_receipt_is_cheap`. 175 keeps that receipt cheap and adds
  render-only strings outside it.

## Decisions

**D1 - Cheap receipts stay cheap.** `ScanFilterReceipt` remains the 174 structured
receipt: target, outcome, converted marker, profile hint, estimated rows, spent
fact, and spent column. It does not gain `String`, `BoundExprProgram`, plan nodes,
or table-name fields. It remains `Copy`.

**D2 - Add an EXPLAIN-only render sidecar.** Introduce a binder-local render trace,
for example:

```rust
struct OptimizerExplainRenderTrace {
    scan_filters: Vec<ScanFilterRenderReceipt>,
}

struct ScanFilterRenderReceipt {
    target: ScanTraceTarget,
    predicate_text: Option<String>,
    pushed_predicate_text: Option<String>,
    residual_predicate_text: Option<String>,
    spent_column_name: Option<String>,
}
```

The exact type names are not load-bearing; the ownership boundary is. This sidecar
is built only for `bind_explain`, is not part of normal `OptimizerTrace`, and is
not consulted by planning.

**D3 - Capture predicate text before pushdown text is lost.** The SELECT Filter arm
already has the original bound predicate. The pushdown helper sees the
`PredicatePushdownPlan` with `pushed_predicate` and `residual_predicate`. Under the
EXPLAIN render mode, capture:

- `predicate_text` from the original predicate's `pretty()`;
- `pushed_predicate_text` from the pushed program before it is lowered to PBC;
- `residual_predicate_text` from the residual program when present.

Static elision also renders the original predicate because the Filter arm has it
before `AlwaysReject -> EmptyRelation`.

**D4 - Render source-safe column labels, not table names.** Add
`spent_column_name` beside the existing stable `spent_column = col[N]`. Resolve the
friendly label only when the binder can prove the source ordinal maps to a named
column, for example from the bound predicate column node or source scan schema. If
projection/derived shape makes the label ambiguous, render null and keep the stable
ordinal.

Do not make friendly table names part of 175. Keep `target` as the stable 174
label (`scan[N] table_id=M`). A later ADR can add table names by carrying an
explicit EXPLAIN-only scan-name map through named scan and alias binding.

**D5 - Render schema grows in place.** Extend the optimizer EXPLAIN schema with
nullable scan-only columns:

```text
predicate_text, pushed_predicate_text, residual_predicate_text, spent_column_name
```

Join rows leave these columns null. Scan/filter rows keep the 174 fields and fill
the new columns from the render sidecar when present.

**D6 - Observational invariant.** 175 records and renders only. It changes no plan,
filter disposition, PBC program, selectivity estimate, join strategy, storage
request, or fact producer. Normal `bind` and `bind_with_trace` do not build
render strings.

**D7 - Frontend-shared.** SQL `EXPLAIN` and Rune `.explain()` both lower to
`LogicalPlan::Explain` and use the same `bind_explain`/`optimizer_explain_plan`
path. No Rune-specific cockpit code.

**D8 - Binder/system-table only.** No executor, PBC, storage, Wendelon,
descriptor, optimizer-fact-producer, or wire change.

## Headline Gates

- `explain_scan_filter_renders_original_pushed_and_residual_predicates` -
  a partially pushed filter renders original, pushed, and residual predicate text.
- `explain_scan_filter_static_elision_renders_original_predicate` - a statically
  elided filter still shows the original predicate in the EXPLAIN row.
- `explain_scan_filter_renders_spent_column_name` - a range/NDV fact spend shows
  both `spent_column = col[N]` and `spent_column_name = amount`.
- `normal_bind_does_not_materialize_explain_render_strings` - normal bind paths do
  not allocate or populate render strings.
- `explain_plans_unchanged_under_render_capture` - cheap and EXPLAIN-rich binding
  produce the same execution plan.

## Supporting Gates

- `converted_pushdown_predicate_text_shows_source_column` - a 173 converted row's
  predicate text names the source unit column.
- `join_rows_keep_scan_readability_columns_null` - join rows preserve 174 behavior
  with the new scan-only columns null.
- `rune_explain_renders_readable_scan_filter_receipts` - Rune `.explain()` renders
  the same readable scan/filter columns as SQL EXPLAIN.
- `spent_column_name_declines_when_source_ordinal_is_ambiguous` - derived or
  projection shapes fail open to null, not a guessed name.
- `scan_filter_receipt_is_still_cheap_structured` - retargets the 174 cheap guard:
  the cheap receipt stays string-free and `Copy`; strings live in the render
  sidecar.

## Architecture Guards

- `adr175_render_trace_is_explain_only` - only `bind_explain` constructs or passes
  the render sidecar; ordinary `bind`/`bind_with_trace` do not.
- `adr175_scan_filter_receipt_remains_cheap` - `ScanFilterReceipt` contains no
  `String`, `BoundExprProgram`, or plan clone and remains `Copy`.
- `adr175_partial_pushdown_text_is_captured_before_lowering` - the render capture
  sees `pushed_predicate` before it becomes only a `PbcProgram`.
- `adr175_predicate_text_uses_bound_program_pretty` - no SQL round-trip formatter
  or parser dependency is introduced.
- `adr175_spent_column_name_is_source_ordinal_safe` - friendly labels fail open
  when source ordinal mapping is not proven.
- `adr175_no_table_name_reverse_lookup_claim` - no required render-time
  `table_id -> TableRef` lookup through `Catalog`.
- `adr175_cockpit_is_frontend_shared` - SQL and Rune share the same EXPLAIN render
  path.
- `adr175_no_runtime_storage_or_producer_surface` - no executor/PBC/storage/
  Wendelon/fact-producer/wire change.

## Non-Goals

- no optimizer decision changes;
- no always-on rich capture;
- no strings or programs on `ScanFilterReceipt`;
- no SQL-flavored predicate rendering;
- no friendly table-name guarantee;
- no join-receipt enrichment, DML EXPLAIN, aggregate/projection/join-input
  receipts, or full plan-tree printer;
- no new optimizer fact, producer, PBC, storage, or wire surface.

## Consensus Trail

174 left the cockpit visible but hard to read and named EXPLAIN-gated predicate
text/friendly names as the next legibility follow-on. The council converged on
readability before control. Grounding corrected two tempting overclaims: the final
plan alone cannot recover the pushed half of a partially pushed predicate, and the
current catalog/scan binding does not provide a general table-id-to-name reverse
lookup. The promoted 175 therefore keeps the cheap receipt untouched and adds an
EXPLAIN-only render sidecar for predicate text and source-safe column names.

## Forward Arc

`174 scan/filter receipts -> 175 EXPLAIN-gated readability -> interactive cockpit
(read-path overrides) / DML + aggregate receipts / SQL-flavored predicate rendering`.

After 175 the read-path cockpit is a usable inspection tool: predicate text, source
column names, outcome, and estimate. Controls and broader coverage follow.

## Deferred (named)

SQL-flavored predicate rendering; friendly table-name render map; interactive
cockpit (read-path overrides, gauges); DML/aggregate/join-input receipts;
join-receipt enrichment; full plan-tree pretty-printer; multi-table name
disambiguation; affine + equality converted pushdown; unit display.
