# ADR 080 Evidence: Rune Named Projection Continuation

Date: 2026-05-23

## Implementation summary

- Added the `RuneNamedExpr { expr, name }` host carrier and
  `RuneExpr::named(name) -> RuneNamedExpr`; `RuneExpr` remains alias-free.
- Added `RuneDataFrame::select(Vec<RuneNamedExpr>)` lowering to existing
  `LogicalPlan::Project { output_names: Vec<Option<String>> }`; anonymous
  `.project(Vec<RuneExpr>)` remains unchanged.
- Added and registered the stable aggregate helper pack:
  `count_all`, `count`, `avg`, `min`, `max`; `sum`, `linear_regress`, and
  `approx_quantile` remain unchanged.
- Preserved aggregate output naming as binder/kernel behavior only: no aggregate
  aliases, no `RuneAggr::named`, and no `LogicalPlan::Aggregate.output_names`.
- Preserved ADR 079 `ctx.sql` relation-handle behavior, sandbox posture, normal
  binder/executor path, and no worker/wire/storage/PBC/dependency surface.

## Implementation note

Rune 0.14 tokenizes `select` as a language keyword, so direct field access
`df.select(...)` is rejected by Rune before host method lookup. The authored ADR
080 surface remains `.select(...)`; `rune_engine.rs` normalizes that exact method
token outside strings/comments to an internal non-keyword host registration
`select_named` before Rune compilation. The real lowering still lives in
`RuneDataFrame::select`, and the architecture guard pins the normalization and
dual registration.

## Load-bearing evidence

- `rune_named_sql_multi_hop_continuation_binds_and_executes` proves the gate:
  `ctx.sql(...) -> filter -> select -> aggregate -> select -> filter` binds with
  schema `[bucket, gross, n]` and executes to `bucket="east", gross=30, n=1`.
- `rune_named_select_sets_project_output_names` proves `.select` constructs a
  `Project` with `[Some("bucket"), Some("net")]`.
- `rune_named_select_binds_visible_schema` and
  `rune_named_post_aggregate_select_renames_outputs` prove names are visible only
  through normal binding.
- `rune_aggregate_helper_pack_binds_stable_outputs` proves
  `[count, count, sum, avg, min, max]` from existing aggregate kernels.
- `rune_duplicate_aggregate_output_name_is_ambiguous` proves duplicate generated
  aggregate names use the existing `BindingError::AmbiguousColumn { column:
  "sum" }` path; the SQL analog remains pinned by
  `grouped_aggregate_order_by_duplicate_generated_name_is_ambiguous`.
- `rune_named_select_rejects_misused_named_expression` proves `RuneNamedExpr` is
  not silently accepted as `RuneExpr` in aggregate arguments and does not leak
  sandbox details.
- `named_select_surface_normalizes_keyword_method_only_in_code` pins the Rune
  keyword compatibility shim and verifies strings/comments are not rewritten.
- The 15 ADR 080 architecture guards pin host-only carrier scope, no alias state,
  existing Project lowering, aggregate helper registration, CountAll placeholder
  parity, no aggregate aliases, duplicate ambiguity, normal binder path, no join,
  no SQL planner expansion, no new worker/wire/storage/PBC/dependency, and the
  unchanged sandbox allow-list.

## Commands

- `cargo fmt`
- `cargo test -p toron --test rune_relation_handles_test`
  - 16 passed.
- `cargo test -p toron --lib named_select_surface_normalizes_keyword_method_only_in_code`
  - 1 passed.
- `cargo test -p toron --test architecture_test adr080_`
  - 15 passed.
- `cargo test -p toron --test architecture_test adr079_`
  - 14 passed.
- `cargo test -p toron --test rune_sandbox_test`
  - 8 passed.
- `cargo test -p toron --test group_by_test`
  - 125 passed.
- `cargo clippy -p toron -- -D warnings`
- `cargo test -p toron`
  - Full package suite passed; one pre-existing ignored Wendelon admin fixture
    remained ignored.
- `git diff --check`
