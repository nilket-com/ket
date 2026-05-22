# Impl Evidence 073: sqlparser 0.62 Migration

**Date:** 2026-05-22
**Base:** `024fe95 plan: adr 073 sqlparser 062 migration`

## Summary

ADR 073 is implemented as a contained parser-foundation refresh. Toron now pins
`sqlparser = "0.62"` in `crates/toron/Cargo.toml`, with `sqlparser 0.62.0` in
`Cargo.lock`.

The migration stayed at the sqlparser-to-Toron-IR anti-corruption boundary:
`sqlparser` imports remain quarantined to `crates/toron/src/sql_planner.rs`, and
no sqlparser AST type was added to Toron IR, SQL wire, storage, or Wendelon
surfaces.

The planner was updated for the 0.62 AST shapes without widening SQL capability:

- `ObjectNamePart` and `ValueWithSpan` extraction now goes through local helpers.
- `Query.limit_clause` preserves the ordered `LIMIT/OFFSET` contract and rejects
  unordered limit/offset, `LIMIT BY`, comma-limit syntax, `FETCH`, locks,
  settings, `FORMAT`, and pipe operators.
- `OrderByKind::All`, `SelectItem::ExprWithAliases`, `ColumnOption::Srid`,
  `ColumnOption::Invisible`, `TableObject::TableQuery`, multi-table INSERT,
  `CAST AS ARRAY`, and new CREATE TABLE dialect fields are named and rejected.
- `CreateTable`, `Insert`, `Delete`, `Update`, `Query`, `Select`, and table-factor
  lowering destructure the lowered external AST fields explicitly. No compat
  layer or second sqlparser version was added.
- ADR 063 and ADR 064 0.54 pin guards were retargeted to the 0.62 migration.

## Probe

After the manifest bump and `cargo update -p sqlparser --precise 0.62.0`, the
first `cargo check -p toron --all-targets` probe produced 62 errors, all in
`crates/toron/src/sql_planner.rs`. The errors matched the planned categories:
object-name parts, value/literal wrappers, `Query.limit_clause`, `OrderByKind`,
`Select` field churn, `Expr::Cast` array field, CREATE TABLE field churn, INSERT
field churn, DELETE field churn, UPDATE field churn, and table/column constraint
shape changes.

This confirmed containment: the parser bump did not force changes outside the
planner boundary until tests and architecture guards were intentionally updated.

## Verification

- `cargo fmt` - run.
- `cargo check -p toron --all-targets` - passed.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `cargo test -p toron --test architecture_test` - passed: 919 guards, including
  ADR 073 guards and retargeted ADR 063/064 guards.
- `cargo test -p toron --test clause_accounting_test` - passed: 6 tests.
- `cargo test -p toron --test create_table_test` - passed: 33 tests.
- `cargo test -p toron --test insert_test` - passed: 30 tests.
- `cargo test -p toron --test delete_test` - passed: 20 tests.
- `cargo test -p toron --test ctas_test` - passed: 28 tests.
- `cargo test -p toron --test sort_ordering_test` - passed: 12 tests.
- `cargo test -p toron --test group_by_test` - passed: 124 tests.
- `cargo test -p toron --test sql_listener_test` - passed: 21 tests.
- `cargo test -p toron --test update_test` - passed: 32 tests.
- `cargo test -p toron --lib` - passed: 349 tests.
- `git diff --check` - passed.

## SQL Corpus

| Surface | Fixture / SQL | Result |
| --- | --- | --- |
| SELECT | `SELECT id FROM t WHERE id > 10` | Still lowers to project/filter shape. |
| ORDER BY | `SELECT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 1` | Still lowers to terminal `Sort` with limit `2`, offset `1`. |
| LIMIT | `SELECT id FROM t LIMIT 1` | Still rejected: unordered limit/offset remains unsupported. |
| LIMIT | `SELECT id FROM t ORDER BY id LIMIT 1, 2` | Rejected through 0.62 `LimitClause::OffsetCommaLimit`. |
| FETCH | `SELECT id FROM t ORDER BY id FETCH FIRST 1 ROWS ONLY` | Still rejected. |
| INSERT VALUES | `INSERT INTO readings VALUES (NULL, -1)` | Still lowers to `InsertLiteral::Null` and signed integer literal. |
| INSERT VALUES | `INSERT INTO t (a) VALUES (1) ORDER BY a` | Rejected: VALUES source must stay undecorated. |
| INSERT VALUES | `INSERT INTO t (a) VALUES (1) LIMIT 1` | Rejected: VALUES source must stay undecorated. |
| INSERT target | `INSERT INTO catalog.public.t (a) VALUES (1)` | Still rejected: only one- or two-part target names. |
| INSERT columns | `INSERT INTO t (catalog.a) VALUES (1)` | Rejected: qualified INSERT column list. |
| INSERT SELECT | `INSERT INTO dst (b, a) SELECT x, y FROM src` | Still lowers to `LogicalPlan::InsertSelect`. |
| CREATE TABLE | `CREATE TABLE "App"."Sales" ("Id" INT NOT NULL)` | Quoted object-name parts preserve values. |
| CREATE TABLE | `CREATE TABLE t (a INT INVISIBLE)` | Rejected through explicit 0.62 `ColumnOption::Invisible` guard. |
| CREATE TABLE | `CREATE TABLE t (a INT) WITH (foo = 'bar')` | Rejected through explicit 0.62 table-options guard. |
| CTAS | `CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people` | Still accepted. |
| DELETE | `DELETE FROM sales` | Still lowers to delete-all shape. |
| UPDATE | `UPDATE people SET note = 'new'` | Still lowers to update-all shape after ADR 070. |

`ORDER BY ALL` is pinned by source-level guard on `OrderByKind::All(_)`. Under
the current `GenericDialect`, the concrete text `ORDER BY ALL` parses as an
ordinary identifier named `ALL`, so the behavior fixture is not meaningful for
that parser variant.

## Notes

- A non-gating broad `cargo test -p toron --tests` sweep was attempted once and
  stopped in the lib binary on two distributed budget rate-limit timing tests:
  `burst_capacity_admits_then_rejects` and
  `configured_tenant_rate_bucket_isolated_from_other`. Both passed immediately
  when run individually, and the required `cargo test -p toron --lib` rerun
  passed 349/349. No parser, SQL, architecture, or ADR 073 tests failed.
- The migration intentionally does not widen SQL grammar. New 0.62 AST surface
  is either lowered only where it represents the existing Toron contract, or
  rejected with bounded unsupported-planner errors.
