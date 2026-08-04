# Implementation Plan: ADR 041 CREATE OR REPLACE TABLE

**Status:** Promoted (paired with `plans/adr/0041_create_or_replace_table.md`)

## Implementation Thesis

A pure SQL composition cycle. Lift two `unsupported` planner rejects, plumb a single `or_replace: bool` flag through the IR / binder / executor, extract one shared DROP helper, and ensure CTAS materializes its source before the replacement drop. No new IR variants. No new Wendelon protocol. No new substrate.

## Phase 0: Touched-Surface Audit

No Plan A / Plan B split. Before editing, confirm the exact touched surfaces and current rejects:

- `crates/toron/src/sql_planner.rs`
  - Schema-only `or_replace` reject (line 671-672).
  - CTAS `or_replace` reject (line 904-905).
  - CTAS `IF NOT EXISTS` reject (line 897) — **must remain**.
- `crates/toron/src/ir.rs` (or wherever the variant lives)
  - `LogicalPlan::CreateTable`
  - `LogicalPlan::CreateTableAsSelect`
- `crates/toron/src/exec.rs` / `ir/execution.rs`
  - `ExecutionPlan::CreateTable`
  - `ExecutionPlan::CreateTableAsSelect`
- `crates/toron/src/binder.rs`
  - Logical-to-execution pass-through (lines ~203, ~212).
  - Required-output / optimizer match arms.
- `crates/toron/src/coordinator.rs`
  - `execute_catalog_plan` (CREATE TABLE, DROP TABLE).
  - `execute_write_plan_with_source` (CTAS).
  - Existing `DropTableStorageRequest` construction site (~line 347).
- Tests:
  - `crates/toron/tests/architecture_test.rs`
  - `crates/toron/tests/create_table_test.rs`
  - `crates/toron/tests/ctas_test.rs`
  - `crates/toron/tests/drop_table_test.rs`
  - `crates/nilket/tests/cli_integration.rs` (for the reservation-continuity fixture)

## Phase 1: IR Flags

Add `or_replace: bool` to:

- `LogicalPlan::CreateTable`
- `LogicalPlan::CreateTableAsSelect`
- `ExecutionPlan::CreateTable`
- `ExecutionPlan::CreateTableAsSelect`

Update all pattern matches and constructors to compile with the new field. **Do not** add CTAS `if_not_exists` plumbing. **Do not** add Sequence IR.

## Phase 2: Planner Lowering

In `lower_create_table` (`crates/toron/src/sql_planner.rs` ~line 626):

1. Check `*or_replace && *if_not_exists` **before** either modifier is handled. Return the stable diagnostic `CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS`.
2. Remove the schema-only `unsupported CREATE TABLE clause: OR REPLACE` reject at line 671-672.
3. Pass `or_replace: *or_replace` into `LogicalPlan::CreateTable`.

In `lower_create_table_as_select` (~line 851):

1. Check `*or_replace && *if_not_exists` first; return the same stable mutual-exclusion diagnostic.
2. **Preserve** the existing `CTAS does not support IF NOT EXISTS in v1` rejection at line 897 for `if_not_exists` when `or_replace` is false.
3. Preserve the explicit-column-list reject and all other existing unsupported-clause rejects.
4. Remove only the CTAS `unsupported CREATE TABLE clause: OR REPLACE` reject at line 904-905.
5. Pass `or_replace: *or_replace` into `LogicalPlan::CreateTableAsSelect`.

## Phase 3: Binder and Optimizer Pass-Through

In `crates/toron/src/binder.rs`:

1. Pass `or_replace` from `LogicalPlan::CreateTable` into `ExecutionPlan::CreateTable` (line ~203).
2. Extend `bind_create_table_as_select` (line ~212) to accept and forward `or_replace` into `ExecutionPlan::CreateTableAsSelect`.
3. Preserve `or_replace` in any `optimize_for_required` / required-output rewrite arms that destructure the two DDL variants.
4. Update any schema/qualifier helper matches that destructure the DDL variants.

No binding-time validation should duplicate planner validation.

## Phase 4: Shared DROP Helper

In `crates/toron/src/coordinator.rs`, extract a private helper:

```rust
async fn drop_existing_table_with_storage_admin(
    catalog: &dyn Catalog,
    admin: &dyn StorageAdmin,
    context: &ExecutionContext,
    name: &TableRef,
    if_exists: bool,
) -> Result<bool, QueryError>;
```

Behavior:

- Resolve the table.
- Missing + `if_exists = true` → `Ok(false)`.
- Missing + `if_exists = false` → `QueryError::Catalog(...)`.
- Present → build one `DropTableStorageRequest`, call `admin.drop_table_storage`, then call `catalog.drop_table(name, false)`. Return `Ok(true)`.

Refactor the existing `ExecutionPlan::DropTable` execution path to call this helper. The replace path must call the same helper with `if_exists = true`. Guard 7 (`adr041_replace_reuses_storage_admin_drop`) pins the single-call-site invariant.

## Phase 5: CREATE Execution

In `execute_catalog_plan` for `ExecutionPlan::CreateTable`:

1. If `or_replace` is true, call `drop_existing_table_with_storage_admin(..., if_exists = true)` before `catalog.create_table`.
2. Then call `catalog.create_table(name, schema.clone(), if_not_exists)`.
3. Mutual exclusion from the planner guarantees `if_not_exists` is false when `or_replace` is true — do not invent a second interpretation in the executor.

## Phase 6: CTAS Execution (Source-First)

Widen `execute_write_plan_with_source` to accept `admin: &dyn StorageAdmin` (or thread it via the existing context type). Update all call sites in both distributed and local coordinators.

For `ExecutionPlan::CreateTableAsSelect`:

1. **Execute and materialize the source first.** `let source_batches = source_executor.execute_read_plan(source).await?;` — any error from this step propagates up *before* any catalog or admin mutation.
2. If `or_replace` is true, call `drop_existing_table_with_storage_admin(..., if_exists = true)` only after source materialization succeeds.
3. Create the new table via `catalog.create_table(table, schema.clone(), false)`.
4. Write the materialized payload through the existing CTAS write path.

**The Phase 1 → Phase 2 ordering is load-bearing.** Do not resolve / drop the target before source materialization for CTAS replace. Fixture 7 (`ctas_or_replace_source_failure_preserves_existing_table`) is the runtime witness; guard 8 (`adr041_ctas_replace_materializes_source_before_drop`) is the source-level witness.

## Phase 7: Architecture Guards

Add the ten ADR 041 guards to `crates/toron/tests/architecture_test.rs`:

1. `adr041_schema_create_or_replace_no_longer_rejected` — the schema-only `or_replace` block no longer returns `"unsupported CREATE TABLE clause: OR REPLACE"`.
2. `adr041_ctas_or_replace_no_longer_rejected` — the CTAS `or_replace` block no longer returns the same message.
3. `adr041_ctas_if_not_exists_remains_rejected` — the `CTAS does not support IF NOT EXISTS in v1` reject is still present in `sql_planner.rs`.
4. `adr041_or_replace_if_not_exists_is_explicitly_rejected` — source contains the literal `CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS` diagnostic.
5. `adr041_no_statement_sequence_ir` — no `LogicalPlan::Sequence` or `ExecutionPlan::Sequence` exists in the workspace.
6. `adr041_no_wendelon_replace_request` — Toron and Wendelon sources do not introduce `TableSchemaReplaceRequest`, `submit_table_replace`, or `drop_table_with_replacement`.
7. `adr041_replace_reuses_storage_admin_drop` — replacement path calls `drop_existing_table_with_storage_admin` rather than constructing a second `DropTableStorageRequest`.
8. `adr041_ctas_replace_materializes_source_before_drop` — narrow source-block extraction around the CTAS execution arm asserts source materialization precedes the helper call.
9. `adr041_toron_cli_reservations_preserved` — `start`, `sql`, `inspect` remain reserved in `toron::cli` with the literal ADR 040 diagnostic text.
10. `adr041_replace_uses_fresh_table_object_id` — the replace path does not preserve `TableObjectId` across the drop/create boundary.

Prefer source-read assertions over broad semantic greps. For guard 8, use narrow source-block extraction around the CTAS execution arm (the same technique used for ADR 038's brace-block guards).

## Phase 8: Integration Fixtures

Add the eleven ADR 041 fixtures in the existing test files:

- `crates/toron/tests/create_table_test.rs`:
  - `create_or_replace_table_creates_when_absent`
  - `create_or_replace_table_replaces_existing_schema`
  - `create_or_replace_table_rejects_if_not_exists_combo`
  - `create_or_replace_table_produces_fresh_object_id` — capture pre-replace binding, run replacement, assert post-replace `TableObjectId` and fingerprint changed.

- `crates/toron/tests/ctas_test.rs`:
  - `create_or_replace_table_as_select_creates_when_absent`
  - `create_or_replace_table_as_select_replaces_existing_table`
  - `ctas_or_replace_source_failure_preserves_existing_table` — inject a failing SELECT; assert the existing target survives unchanged.
  - `ctas_if_not_exists_remains_rejected`

- `crates/toron/tests/drop_table_test.rs` (or a local coordinator fixture file):
  - `or_replace_uses_storage_admin_drop_before_catalog_removal`
  - `wendelon_dependent_index_reject_propagates_through_replace`

- `crates/nilket/tests/cli_integration.rs`:
  - `nilket_toron_sql_start_inspect_still_reserved` — assert ADR 040 reservation diagnostics are byte-identical post-ADR-041.

## Phase 9: Verification

Minimum verification:

```text
cargo fmt --all
cargo check -p toron -p nilket
cargo test -p toron --test architecture_test adr041
cargo test -p toron --test create_table_test
cargo test -p toron --test ctas_test
cargo test -p toron --test drop_table_test
cargo test -p nilket
git diff --check
```

If the implementation touches shared coordinator execution broadly, also run:

```text
cargo test -p toron
cargo clippy --workspace -- -D warnings
```

## Phase 10: Evidence Record

Before promotion / merge, the implementation evidence record should affirm:

- [ ] Two `OR REPLACE` rejects lifted (schema + CTAS).
- [ ] CTAS `IF NOT EXISTS` reject preserved.
- [ ] Mutual-exclusion diagnostic emitted for `OR REPLACE` + `IF NOT EXISTS` combo.
- [ ] No `Sequence` IR added.
- [ ] No Wendelon replacement request / proof carrier / stage orchestration added.
- [ ] Standard DROP path and replacement path share one storage-admin drop helper.
- [ ] CTAS replace materializes source before any replacement drop (fixture 7 + guard 8).
- [ ] Replacement creates fresh `TableObjectId` / schema generation / fingerprint (fixture 4 + guard 10).
- [ ] Toron CLI reservations remain ADR 040 byte-identical (fixture 11 + guard 9).
- [ ] All 10 architecture guards green; all 11 integration fixtures green.

## Acceptance Checklist

- [ ] Phase 0 surface audit recorded.
- [ ] Phase 1 IR flags added; workspace compiles.
- [ ] Phase 2 planner lowering: two rejects removed, mutual-exclusion added, CTAS `IF NOT EXISTS` reject preserved.
- [ ] Phase 3 binder pass-through wired.
- [ ] Phase 4 shared DROP helper extracted; standalone DROP path refactored to use it.
- [ ] Phase 5 CREATE replacement: `or_replace` triggers helper before catalog create.
- [ ] Phase 6 CTAS replacement: source materialized before helper; payload write follows fresh create.
- [ ] Phase 7 ten architecture guards added and green.
- [ ] Phase 8 eleven integration fixtures added and green.
- [ ] Phase 9 verification commands run; evidence captured.
- [ ] No regression in pre-existing fixture pass counts (byte-identical invariant).

## Non-Goals

- `LogicalPlan::Sequence` / `ExecutionPlan::Sequence` IR.
- Cross-ring transaction or atomic Wendelon-side replacement protocol.
- `TableSchemaReplaceRequest` or any new Wendelon admin RPC.
- CTAS `IF NOT EXISTS` lift.
- Cascading secondary-index drop on replace.
- Toron daemon substrate (`nilket toron start` lift).
- Public Toron SQL listener (`nilket toron sql` lift).
- In-place schema evolution on `OR REPLACE` (must be destructive fresh-identity).
