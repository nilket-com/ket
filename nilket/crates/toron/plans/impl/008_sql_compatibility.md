# Implementation Plan: M8 — SQL Compatibility

Author: AI Swarm Council (Claude / Codex / Gemini)
Primary draft: Gemini

## 1. Goal

Implement a SQL compatibility surface for Toron that lowers a subset of analytical SQL into Toron's handwritten `LogicalPlan`.

## 2. Implementation Contract

- **Compatibility Surface:** SQL is provided for familiarity; `toron-ir` remains the authoritative engine contract.
- **Parser:** Use `sqlparser-rs` to parse SQL strings.
- **Analytical Subset (V0):**
    - `SELECT COUNT(*)` and `SELECT col1, col2`.
    - `FROM table_name` (mapped to `TableId`).
    - `WHERE expression` supporting existing IR operators.
- **Lowering:** The `SqlPlanner` will recursively traverse the SQL AST and construct the corresponding `LogicalPlan`.

## 3. Phase Plan

### 3.1. Infrastructure (Completed)
- [x] Add `sqlparser` dependency to `Cargo.toml`.
- [x] Initialize `src/sql_planner.rs`.
- [x] Register `pub mod sql_planner` in `src/lib.rs`.

### 3.2. Expression Lowering (Completed)
- [x] Implement `SqlPlanner::lower_expr(sql_expr: &sqlparser::ast::Expr) -> Result<Expr, String>`.
- [x] Map SQL binary operators (`=`, `<>`, `<`, `<=`, `>`, `>=`) to IR variants.
- [x] Map SQL logical operators (`AND`, `OR`, `NOT`) to IR variants.
- [x] Map SQL `IS NULL` and `IS NOT NULL` to IR variants.
- [x] Map SQL identifiers to `Expr::Column`.
- [x] Map SQL literals (numbers, strings, booleans) to `Expr::Literal`.

### 3.3. Plan Lowering (Completed)
- [x] Implement `SqlPlanner::plan(sql: &str) -> Result<LogicalPlan, String>`.
- [x] Support `Query` -> `Select` -> `From` mapping.
- [x] Handle `FROM` clause: map table names to `TableId` (using a mock catalog for V0).
- [x] Handle `WHERE` clause: wrap the plan in a `LogicalPlan::Filter`.
- [x] Handle `SELECT` list:
    - If `COUNT(*)`, wrap in `LogicalPlan::Aggregate`.
    - Otherwise, wrap in `LogicalPlan::Project`.

### 3.4. Verification & Testing (Completed)
- [x] **Lowering Tests:** Verify that various SQL strings (e.g., `SELECT * FROM sales WHERE amount > 100`) produce the correct `LogicalPlan`.
- [x] **Error Handling:** Verify that unsupported SQL features (e.g., `JOIN`, `UNION`, complex subqueries) return helpful error messages.
- [x] **End-to-End Test:** A test that takes a SQL string, plans it, and executes it through the M6 `Executor` against an in-memory storage.


## 4. Design Details: SQL to IR Mapping

```sql
SELECT count(*) FROM sales WHERE amount > 100
```
Lowering:
1. `Scan(TableId(1))` (mapped from "sales").
2. `Filter(input: Scan, predicate: Gt(Column("amount"), Literal(100)))`.
3. `Aggregate(input: Filter, aggr_expr: [(CountAll, Column("count"))])`.

## 5. Risks and Mitigations

- **Risk:** `sqlparser` AST is complex and may lead to "switch-exhaustion" or verbose code.
  - **Mitigation:** Focus strictly on the V0 analytical subset; explicitly fail fast on unsupported variants.
- **Risk:** Dialect differences.
  - **Mitigation:** Use `GenericDialect` for V0.

## 6. Definition of Done

M8 is complete when a user can pass a SQL string to the `SqlPlanner`, receive a valid `LogicalPlan`, and execute it successfully against Toron's storage/execution layers.
