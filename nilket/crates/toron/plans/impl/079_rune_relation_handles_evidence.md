# ADR 079 Evidence: Rune Relation Handles

Date: 2026-05-23

## Implementation summary

- Added `SqlPlanner::plan_relation` with `parse_one_statement` (`ast.len() == 1`)
  and terminal-plan rejection for `CREATE TABLE` / CTAS / `DROP TABLE` /
  `INSERT` / `INSERT SELECT` / `DELETE` / `UPDATE`.
- Added `ToronContext::sql(query)` returning a lazy `RuneDataFrame { plan }`,
  plus the minimal Rune `sum(expr)` aggregate helper.
- Preserved Rune as coordinator-side plan authoring only: no Rune-local binder,
  optimizer, executor, materialization, storage path, worker UDF surface, PBC
  opcode, wire change, dependency, or `syn`.
- Fixed the latent user-visible SQL correctness bug surfaced by the ADR 079 gate:
  aggregate-only grouped projections such as
  `SELECT COUNT(*) FROM sales GROUP BY region` need hidden group keys for
  aggregation while exposing only aggregate outputs. Requested `Project` roots are
  now isolated before child pruning, and aggregate output remaps no longer claim
  grouped aggregate output was pruned when it was not.

## Load-bearing evidence

- `rune_ctx_sql_rejects_multi_statement_input` rejects
  `SELECT id FROM sales; DROP TABLE sales` with the single-statement diagnostic.
- `rune_sql_group_by_alias_can_feed_outer_sum_and_bind` binds:
  `ctx.sql("SELECT region, COUNT(1) AS cnt FROM sales GROUP BY region")
     .aggregate([], [sum(col("cnt"))])`
  through the normal binder, resolving `cnt` against the SQL output schema.
- `rune_sql_group_by_alias_outer_sum_executes_if_existing_harness_supports_it`
  executes the returned plan through the existing executor harness and returns
  `SUM(cnt) = 6`.
- `rune_sql_composition_preserves_existing_pushdown_path` binds a Rune filter
  over `ctx.sql("SELECT * FROM sales")` with `FilterDisposition::FullyPushed`
  and a scan PBC program.
- `grouped_aggregate_can_project_only_aggregates_and_drop_group_keys` pins the
  pure-SQL one-key and two-key variants independent of Rune.

## Commands

- `cargo fmt`
- `cargo test -p toron --test rune_relation_handles_test`
  - 8 passed.
- `cargo test -p toron --test architecture_test adr079_`
  - 14 passed.
- `cargo test -p toron --test rune_sandbox_test`
  - 8 passed.
- `cargo test -p toron --test group_by_test`
  - 125 passed.
- `cargo test -p toron --test predicate_widening_test`
  - 28 passed.
- `cargo test -p toron --lib`
  - 353 passed.
- `cargo test -p toron --test architecture_test`
  - 981 passed.
- `cargo test -p toron`
  - Full package suite passed; one pre-existing ignored Wendelon admin fixture
    remained ignored.
- `cargo check -p toron`
- `cargo check -p toron --features telemetry`
- `cargo check -p toron --features otlp-http`
- `cargo check -p toron --features otlp-grpc`
- `cargo clippy -p toron -- -D warnings`
- `git diff --check`

All commands completed successfully.
