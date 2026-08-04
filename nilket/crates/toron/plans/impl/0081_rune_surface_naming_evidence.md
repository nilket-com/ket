# ADR 081 Evidence: Rune Surface Naming

Date: 2026-05-23

## Implementation summary

- Renamed the authored named-projection Rune surface from `.select(...)` to
  `.project_named(...)`.
- Renamed `RuneDataFrame::select(Vec<RuneNamedExpr>)` to
  `RuneDataFrame::project_named(Vec<RuneNamedExpr>)`; the body still lowers to the
  existing `LogicalPlan::Project { output_names }` path.
- Deleted the ADR 080 source-text shim:
  `normalize_rune_named_select_surface`, `skip_rune_quoted`,
  `skip_rune_line_comment`, `skip_rune_block_comment`,
  `is_ascii_ident_continue`, the `compile_script` normalization call, and the
  shim unit test `named_select_surface_normalizes_keyword_method_only_in_code`.
- Replaced the dual `"select"` / `"select_named"` registrations with the single
  `"project_named"` registration.
- Respelled the ADR 080 relation-handle fixtures and renamed stale
  `named_select` test names to `project_named` / `named_expr` names.
- No IR, binder, executor, SQL planner, Wendelon, worker, wire, storage, PBC,
  dependency, or `syn` changes were made.

## Load-bearing evidence

- `rune_project_named_sql_multi_hop_continuation_binds_and_executes` proves the
  behavior gate remains intact:
  `ctx.sql(...) -> filter -> project_named -> aggregate -> project_named -> filter`
  binds with schema `[bucket, gross, n]` and executes to
  `bucket="east", gross=30, n=1`.
- `rune_project_named_sets_project_output_names` proves named projection still
  constructs `Project.output_names` as `[Some("bucket"), Some("net")]`.
- `rune_project_named_binds_visible_schema` and
  `rune_project_named_post_aggregate_renames_outputs` prove names remain visible
  through the normal binder.
- `rune_duplicate_aggregate_output_name_is_ambiguous` still proves duplicate
  generated aggregate names use the existing `BindingError::AmbiguousColumn {
  column: "sum" }` path.
- `rune_named_expr_rejects_misused_named_expression` still proves `RuneNamedExpr`
  is not silently accepted as a `RuneExpr` aggregate argument and does not leak
  sandbox details.
- The retargeted `adr080_named_projection_lowers_to_existing_project_output_names`
  guard proves named projection still lowers through existing
  `LogicalPlan::Project.output_names` without joins, aggregates, or a source
  normalizer dependency.
- The retargeted `adr080_rune_sandbox_allow_list_remains_toron_authoring_only`
  guard proves the sandbox allow-list registers `project_named` and no longer
  registers `select` or `select_named`.
- `adr081_rune_frontend_does_not_rewrite_user_source` proves `compile_script`
  passes the authored script directly into `Source::new("script", script)` and
  the normalizer/scanner helpers are gone.
- `adr081_authored_rune_methods_avoid_reserved_keywords` proves no authored Rune
  method/function registration uses the `select` or `as` keyword names and no
  `select_named` compatibility shim remains.

## Commands

- `cargo fmt`
- `cargo test -p toron --test rune_relation_handles_test`
  - 16 passed.
- `cargo test -p toron --test architecture_test adr080_`
  - 15 passed.
- `cargo test -p toron --test architecture_test adr081_`
  - 2 passed.
- `cargo test -p toron --test rune_sandbox_test`
  - 8 passed.
- `cargo test -p toron --test group_by_test`
  - 125 passed.
- `cargo clippy -p toron -- -D warnings`
- `git diff --check`
- `cargo test -p toron`
  - Full package suite passed; one pre-existing Wendelon admin fixture remained
    ignored.
