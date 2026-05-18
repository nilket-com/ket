# Implementation Plan: ADR 033 — `CREATE TABLE AS SELECT`

**Companion ADR:** `plans/adr/033_create_table_as_select.md`

## Goal

Add local coordinator support for `CREATE TABLE name AS
<query>` by composing ADR 029 catalog identity (the
existing `Catalog::create_table` path) with ADR 032
materialization (`materialize_insert_select_batch`) and
schema inference.

The implementation must keep the target table identity
out of the bound CTAS plan. Binding validates and infers
the target schema from the source. The local coordinator
creates the table, executes the source, materializes
exactly one payload batch (or one zero-row batch if the
source is empty), writes it, and returns no result rows.

Preserve:

- All ADR 029 catalog metadata-only invariants and
  storage-synthetic-name reject discipline.
- All ADR 030 VALUES INSERT architecture guards;
  `ExecutionPlan::Insert` stays expression-free.
- All ADR 031 `BoundExprProgram` invariants.
- All ADR 032 InsertSelect architecture guards;
  `materialize_insert_select_batch` still returns
  `Ok(None)` for zero rows.
- ADR 015 cycle properties: narrow-detector architecture
  guards; named-reject sites; no silent acceptance.

No new SQL syntax beyond bare `CREATE TABLE name AS
<query>`; no new IR expression variant; no new aggregate
kernel; no PBC opcode change; no fragmenter detector
change for SELECT paths; no cross-ring engagement; no
Wendelon-side change; no new `StorageWrite` overload; no
row-count or RETURNING result; no `blake3` dependency;
no external allocator crate; no source-to-target type
coercion (target schema IS source schema by
construction).

**Eight-phase patch order.** Phases 1-6 are mechanical;
Phase 7 is functional tests; Phase 8 is architecture
guards. Phase 9 promotion-time housekeeping is out of
implementation PR scope.

## Implementation constraints (Codex iter-0 R1-R7 + Codex iter-1 R1-R3 + Claude iter-1 C1/C2/C3/C4 + Claude iter-2 A1/A2/A4)

- **R1 (iter-0):** `ExecutionPlan::CreateTableAsSelect`
  must NOT carry `TableId`, `TableObjectId`, or
  `PhysicalDescriptorFingerprint` — only `table`,
  `schema`, `source`.
- **R2 (iter-0):** Binder binds source SELECT first,
  then infers target schema, then emits ExecutionPlan.
  Coordinator performs `catalog.create_table` only
  after binding succeeds.
- **R3 (iter-0):** SQL planner rejects CTAS with
  non-empty `columns` (sqlparser `CreateTable.columns`
  field) with stable diagnostic.
- **R4 (iter-0):** SQL planner rejects CTAS with
  `if_not_exists` with stable diagnostic.
- **R5 (iter-0):** Binder validates inferred schema:
  rejects duplicate output names; rejects ADR 029
  storage synthetic names; rejects types outside the
  six-type lattice.
- **R6 (iter-0):** Empty source SELECT must create a
  queryable empty table via zero-row `RecordBatch`
  write.
- **R7 (iter-0):** Non-atomicity (catalog success +
  write failure = phantom table) is documented in the
  ADR; impl makes no rollback attempt.
- **R1 (iter-1):** Coordinator calls `create_table`
  BEFORE source execution (fails fast on table-exists
  / admin-unavailable). The load-bearing safety
  invariant is source BINDING before catalog mutation
  (D5 / R2 iter-0), NOT source EXECUTION before
  catalog mutation.
- **R2 (iter-1):** Wendelon synthetic constants imported
  directly into `binder.rs` (or factored through a
  neutral Toron helper). No forked `__row_*__` string
  literals (Claude iter-2 A1 strict guard wording).
- **R3 (iter-1):** No source-to-target type coercion
  in CTAS. Identity `ColumnOrdinal` target columns
  passed to `materialize_insert_select_batch`. Users
  spell `CAST` in SELECT if they want type
  adjustments.
- **C1 (Claude iter-1):** Add private sibling helper
  `materialize_empty_record_batch(schema) ->
  Result<RecordBatch, ExecutionError>` in
  `coordinator/write.rs`. Do NOT modify
  `materialize_insert_select_batch`.
- **C2 (Claude iter-1):** Architecture guards pin two
  ordering invariants via byte-position comparison:
  bind-source-before-infer-schema (binder), create-
  table-before-execute-source (coordinator).
- **C3 (Claude iter-1) + A1 (Claude iter-2):** Storage
  synthetic name reject site is the binder's schema-
  inference path; architecture guard uses negative-
  pattern enforcement (asserts absence of forked
  `__row_*__` string literals).
- **C4 (Claude iter-1):** Four required end-to-end
  composition fixtures: SUM grouped aggregate, INNER
  JOIN, empty-source initialization, failure-before-
  catalog regression.
- **A2 (Claude iter-2):** New test file
  `crates/toron/tests/ctas_test.rs` (do not extend
  `insert_test.rs`).
- **A4 (Claude iter-2):** Variant-count guard pairing —
  bump existing
  `architecture_guards_unchanged_plus_sort_variant`
  alongside new ADR-033 count guards.

## Touchpoint sweep

| File | Phase |
|---|---|
| `crates/toron/src/ir.rs` | 1 |
| `crates/toron/src/ir/execution.rs` | 1, 6 (walkers) |
| `crates/toron/src/sql_planner.rs` | 2 |
| `crates/toron/src/binder.rs` | 3 |
| `crates/toron/src/coordinator/write.rs` | 4 |
| `crates/toron/src/coordinator.rs` | 5 |
| `crates/toron/src/distributed/fragment.rs` | 6 |
| `crates/toron/src/distributed/scheduler.rs` | 6 |
| `crates/toron/src/server/mod.rs` | 6 |
| `crates/toron/src/exec.rs` | 6 |
| `crates/toron/tests/ctas_test.rs` (NEW) | 7 |
| `crates/toron/tests/architecture_test.rs` | 8 |

## Helper reuse summary (Claude iter-2 A3)

| Helper | Source | CTAS use |
|---|---|---|
| `Catalog::create_table` | ADR 029 | Called by coordinator; returns `TableBinding` for `WriteRequest` |
| `StorageWrite::write` | ADR 030 | Called once per CTAS with payload from materialization |
| `WriteRequest` struct shape | ADR 030 | Reused unchanged |
| `Executor::execute_bound(source, None)` | existing | Source executes with `None` morsel (mirrors ADR 030 D12 + ADR 032 carry-forward) |
| `materialize_insert_select_batch` | ADR 032 | Called with identity target ordinals; unchanged `Ok(None)` zero-row contract |
| `typed_null_array(data_type, 0, column)` | ADR 032 (private) | Reused inside new `materialize_empty_record_batch` |
| Wendelon synthetic constants | wendelon crate (per ADR 029 D12) | Imported directly in `binder.rs` or exposed by a neutral helper; no forked literals (R2 iter-1) |
| `materialize_empty_record_batch` | **NEW** (Phase 4) | Called when `materialize_insert_select_batch` returns `Ok(None)` |
| `is_supported_ctas_column_type` | **NEW** (Phase 3) | Binder-side six-type lattice check |
| `validate_ctas_column_name` | **NEW** (Phase 3) | Binder-side duplicate + synthetic name check |
| `infer_ctas_schema` | **NEW** (Phase 3) | Binder-side schema inference orchestrator |
| `lower_create_table_as_select` | **NEW** (Phase 2) | SQL planner-side CTAS lowerer |
| `bind_create_table_as_select` | **NEW** (Phase 3) | Binder-side CTAS arm entry |
| `FragmentError::CreateTableAsSelectNotFragmentable` | **NEW** (Phase 6) | Fragmenter-side reject variant |

Three new SQL/binder/coordinator helpers + one new
error variant + one telemetry label `"create_table_as_select"`.
Net new code is bounded; heavy lifting is helper-
composition.

## Phase 0 — Preflight audit

```sh
# Confirm CreateTableAsSelect is unused
rg -n "CreateTableAsSelect" crates/toron/src crates/toron/tests
# Expected: zero hits

# Confirm ADR 029 / 030 / 031 / 032 baseline green
cargo test -p toron --test architecture_test
cargo test -p toron --test create_table_test
cargo test -p toron --test insert_test

# Confirm ADR 029 lower_create_table rejects query.is_some()
rg -n "fn lower_create_table|query.is_some|AS SELECT" \
  crates/toron/src/sql_planner.rs

# Confirm InsertSelect helper currently returns None for zero rows
rg -n "fn materialize_insert_select_batch" \
  crates/toron/src/coordinator/write.rs
rg -n "return Ok\(None\)" \
  crates/toron/src/coordinator/write.rs

# Confirm typed_null_array helper exists and handles rows = 0
rg -n "fn typed_null_array" crates/toron/src/coordinator/write.rs

# Confirm ADR 029 Wendelon synthetic constants source
rg -n "STORAGE_SYNTHETIC|__row_hlc__|validate_storage_synthetic" \
  crates/toron/src

# Confirm no shadow dependencies
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" \
  crates/toron/Cargo.toml
# Expected: zero hits
```

Expected baseline:

- No existing `CreateTableAsSelect` hits in production
  source or tests.
- `lower_create_table` rejects `query.is_some()` with
  `"unsupported CREATE TABLE clause: AS SELECT (CTAS)"`.
- `materialize_insert_select_batch` returns `Ok(None)`
  for zero source rows.
- Distributed and executor paths have InsertSelect
  reject patterns to mirror.

Preserve verbatim:

- All `Catalog` trait methods + semantics (ADR 029).
- All ADR 030 `StorageWrite` / `InMemoryTableStorage` /
  `WendelonStorageWrite` paths.
- All ADR 031 `BoundExprProgram` shape and derives.
- All ADR 032 `materialize_insert_select_batch`
  semantics including `Ok(None)` zero-row return.
- All distributed / server / scheduler / fragmenter
  reject patterns for `Insert` / `InsertSelect`.

## Phase 1 — IR shape (Codex iter-0 R1)

**Files:** `crates/toron/src/ir.rs`,
`crates/toron/src/ir/execution.rs`.

**Tasks:**

1. Add to `LogicalPlan`:

   ```rust
   CreateTableAsSelect {
       table: TableRef,
       source: Box<LogicalPlan>,
   }
   ```

   No `columns` field (R3 rejects non-empty column
   lists at the planner; the logical IR doesn't carry
   what cannot exist).

2. Add to `ExecutionPlan`:

   ```rust
   CreateTableAsSelect {
       table: TableRef,
       schema: SchemaRef,
       source: Box<ExecutionPlan>,
   }
   ```

   **No `TableId`, `TableObjectId`, or
   `PhysicalDescriptorFingerprint` fields** (Codex
   iter-0 R1). The coordinator obtains these from
   `catalog.create_table` after binding succeeds.

3. Update `ExecutionPlan::schema()` to return
   `Arc::new(Schema::empty())` for
   `CreateTableAsSelect`, matching `Insert`,
   `InsertSelect`, and `CreateTable`.

4. Update any exhaustive `match` sites the compiler
   flags (executor, fragmenter, scheduler, server,
   walkers). Walkers wire up real leaf behavior in
   Phase 6.

**Verification:**

```sh
cargo check -p toron
```

## Phase 2 — SQL lowering (Codex iter-0 R3 + R4)

**File:** `crates/toron/src/sql_planner.rs`.

**Tasks:**

1. Refactor `lower_create_table` so
   `CreateTable.query.is_some()` routes to a new
   `lower_create_table_as_select` instead of the
   current `"unsupported CREATE TABLE clause: AS SELECT
   (CTAS)"` reject. Keep the exhaustive `CreateTable`
   destructure without a `..` rest pattern (ADR 029 D2
   carry-forward).

2. Implement `lower_create_table_as_select(&self,
   create_table: &CreateTable) -> Result<LogicalPlan,
   FrontendError>`:

   - reject `if_not_exists == true` with stable
     diagnostic `"CTAS does not support IF NOT EXISTS
     in v1"` (Codex iter-0 R4);
   - reject non-empty `columns` with stable diagnostic
     `"CTAS does not support explicit column lists in
     v1; use SELECT aliases for column names"` (Codex
     iter-0 R3);
   - reject any other ADR 029 unsupported clauses
     (constraints, `OR REPLACE`, `LIKE`, `CLONE`,
     storage options, table_options, `with_options`,
     `engine`, `comment`, partitioning, tags, etc.) —
     reuse existing `lower_create_table` reject path or
     shared helper;
   - extract the table name via existing
     `lower_create_table_name` (or analogous DDL-specific
     helper from ADR 029);
   - lower the query body via `self.lower_query(query)`;
   - emit `LogicalPlan::CreateTableAsSelect { table:
     name, source: Box::new(source) }`.

3. Add walker leaves in `plan_contains_join` and
   `plan_qualifiers`:

   ```rust
   LogicalPlan::CreateTable { .. }
   | LogicalPlan::Insert { .. }
   | LogicalPlan::InsertSelect { .. }
   | LogicalPlan::CreateTableAsSelect { .. } => false,  // or Vec::new()
   ```

   CTAS is a statement boundary, like InsertSelect. Do
   not let CTAS source qualifiers leak into the outer
   planner scope.

**Verification:**

```sh
cargo test -p toron sql_planner::tests
cargo test -p toron --test ctas_test  # once Phase 7 lands
```

## Phase 3 — Binder + schema inference (Codex iter-0 R1 + R2 + R5 + Codex iter-1 R2)

**File:** `crates/toron/src/binder.rs`.

**Tasks:**

1. Add a `LogicalPlan::CreateTableAsSelect` arm in
   `Binder::bind_plan` (or equivalent top-level
   dispatch) that calls a dedicated helper:

   ```rust
   LogicalPlan::CreateTableAsSelect { table, source } => {
       self.bind_create_table_as_select(table, source).await
   }
   ```

2. Implement `async fn bind_create_table_as_select(&self,
   table: &TableRef, source: &LogicalPlan) ->
   Result<ExecutionPlan, BindingError>`:

   Order is load-bearing (Codex iter-0 R2):

   - bind `source` with `self.bind_plan(source).await?`
     (BEFORE schema inference; BEFORE any catalog
     mutation);
   - read `source.schema()`;
   - call `infer_ctas_schema(&source_schema)` →
     `Result<SchemaRef, BindingError>`;
   - emit `ExecutionPlan::CreateTableAsSelect { table:
     table.clone(), schema, source: Box::new(source) }`.

   Do NOT call `Catalog::create_table` or
   `Catalog::resolve_table` from this helper. The
   coordinator owns catalog mutation.

3. Add three new private helpers:

   ```rust
   fn infer_ctas_schema(
       source_schema: &SchemaRef,
   ) -> Result<SchemaRef, BindingError>

   fn validate_ctas_column_name(name: &str) -> Result<(), BindingError>

   fn is_supported_ctas_column_type(data_type: &DataType) -> bool
   ```

   `is_supported_ctas_column_type` body:

   ```rust
   matches!(
       data_type,
       DataType::Boolean
           | DataType::Int32
           | DataType::Int64
           | DataType::Float32
           | DataType::Float64
           | DataType::Utf8
   )
   ```

   Functionally identical to `is_supported_insert_select_target`
   in `coordinator/write.rs`. Do NOT share — they live
   on different sides of the binder/coordinator
   boundary and may evolve independently.

4. `infer_ctas_schema` responsibilities:

   - preserve source field names, types, and
     nullability verbatim;
   - reject duplicate output names with stable
     diagnostic;
   - reject empty output names if reachable;
   - reject Wendelon storage synthetic names via
     `validate_ctas_column_name`;
   - reject types outside the ADR 029 table lattice via
     `is_supported_ctas_column_type`.

5. `validate_ctas_column_name` synthetic-name
   ownership (Codex iter-1 R2):

   - import the five Wendelon storage synthetic
     constants directly into `binder.rs`; or
   - factor a small Toron-private helper into a
     neutral module (re-exporting the Wendelon
     constants).

   **Do NOT fork the five `__row_*__` literals** in
   `binder.rs`. Architecture guard #7 enforces this
   via negative-pattern check (Claude iter-2 A1).

6. Add walker leaves in `collect_qualifiers`,
   `terminal_exchange`, and `optimize_for_required`:

   - `collect_qualifiers`: CTAS is a leaf (its source
     qualifiers are collected during source binding
     before this point);
   - `terminal_exchange`: CTAS is not a terminal
     exchange (return `None`);
   - `optimize_for_required`: recurse into the CTAS
     source with `required_output = None` (mirrors
     ADR 032 D11 binder-optimizer exception); preserve
     the inferred schema and return empty `OrdinalRemap`.

**Verification:**

```sh
cargo test -p toron binder::tests
cargo test -p toron --test ctas_test  # once Phase 7 lands
```

## Phase 4 — Coordinator helpers (Claude iter-1 C1)

**File:** `crates/toron/src/coordinator/write.rs`.

**Tasks:**

1. Add private sibling helper:

   ```rust
   pub(super) fn materialize_empty_record_batch(
       schema: &SchemaRef,
   ) -> Result<RecordBatch, ExecutionError> {
       let columns = schema
           .fields()
           .iter()
           .map(|field| typed_null_array(field.data_type(), 0, field.name()))
           .collect::<Result<Vec<ArrayRef>, ExecutionError>>()?;
       RecordBatch::try_new(schema.clone(), columns)
           .map_err(ExecutionError::from)
   }
   ```

2. Reuse the existing private `typed_null_array(data_type,
   rows, column)` helper with `rows = 0`. The helper
   already handles all six lattice types and the
   `rows = 0` case produces an empty typed array. No
   new builder code paths.

3. Do NOT modify `materialize_insert_select_batch` —
   its `Ok(None)` zero-row contract is the load-bearing
   signal ADR 032 D7 + coordinator routing use to skip
   the writer for InsertSelect. Architecture guard #17
   enforces.

**Verification:**

```sh
cargo test -p toron coordinator::write::tests
```

## Phase 5 — Coordinator routing (Codex iter-0 R2 + Codex iter-1 R1)

**File:** `crates/toron/src/coordinator.rs`.

**Tasks:**

1. In `QueryCoordinator::execute_query`, extend the
   write-statement interception arm to also match
   `LogicalPlan::CreateTableAsSelect`:

   ```rust
   if matches!(
       plan,
       LogicalPlan::Insert { .. }
           | LogicalPlan::InsertSelect { .. }
           | LogicalPlan::CreateTableAsSelect { .. }
   ) {
       let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
       return self.execute_write_plan(&bound).await;
   }
   ```

   The intercept must remain BEFORE the SELECT
   descriptor-evolution retry loop (ADR 030 D12 + ADR
   032 + ADR 033 D11 carry-forward).

2. Extend `execute_write_plan` for
   `ExecutionPlan::CreateTableAsSelect`:

   ```rust
   ExecutionPlan::CreateTableAsSelect {
       table,
       schema,
       source,
   } => {
       // R1 (iter-1) + D6: catalog.create_table BEFORE source execution.
       let binding = self
           .catalog
           .create_table(table, schema.clone(), false)
           .await?;
       // D13 + ADR 030/032 carry-forward: source executes with None morsel.
       let source_batches = self.executor.execute_bound(source, None).await?;
       // R3 (iter-1): identity ordinals; no coercion.
       let target_columns: Vec<ColumnOrdinal> = (0..schema.fields().len())
           .map(|index| ColumnOrdinal(index as u32))
           .collect();
       let payload = match write::materialize_insert_select_batch(
           schema,
           &target_columns,
           &source_batches,
       )? {
           Some(payload) => payload,
           // D8 + Claude iter-1 C1: zero-row source creates queryable empty table.
           None => write::materialize_empty_record_batch(schema)?,
       };
       let context = self.executor.context();
       self.writer
           .write(WriteRequest {
               session_id: context.session_id,
               query_id: context.query_id,
               tenant_id: context.tenant_id,
               // R1 (iter-0): table identity from create_table binding, not from bound plan.
               table_id: binding.legacy_table_id,
               table_object_id: binding.table,
               physical_descriptor_fingerprint: binding.fingerprint,
               payload,
               profile_hint: context.profile_hint,
               trace_context: context.trace_context.clone(),
           })
           .await?;
       Ok(Vec::new())
   }
   ```

   The `create_table` call intentionally happens
   BEFORE source execution (Codex iter-1 R1). The
   source was already bound (so missing source tables
   and invalid source expressions already failed
   before catalog mutation per Codex iter-0 R2), and
   table-exists / admin-unavailable failures should
   also fail before source execution cost.

3. In `DistributedQueryCoordinator::execute_query`,
   reject `ExecutionPlan::CreateTableAsSelect` before
   `fragment_plan` with stable diagnostic `"distributed
   CREATE TABLE AS SELECT execution is not wired to a
   write-context-bearing coordinator"`.

**Verification:**

```sh
cargo test -p toron coordinator::tests
cargo test -p toron --test ctas_test  # once Phase 7 lands
```

## Phase 6 — Defensive rejects + walker leaf treatment

**Files:**

- `crates/toron/src/exec.rs`
- `crates/toron/src/distributed/fragment.rs`
- `crates/toron/src/distributed/scheduler.rs`
- `crates/toron/src/server/mod.rs`

**Tasks:**

1. **Executor** (`exec.rs`):

   - `LogicalPlan::CreateTableAsSelect` arm in unbound
     reject path: `ExecutionError::NotImplemented {
     feature: "unbound CREATE TABLE AS SELECT
     execution" }`;
   - `ExecutionPlan::CreateTableAsSelect` arm in bound
     reject path: `ExecutionError::NotImplemented {
     feature: "CREATE TABLE AS SELECT execution is
     coordinator-only" }`.

2. **Fragmenter** (`distributed/fragment.rs`):

   - Add `FragmentError::CreateTableAsSelectNotFragmentable`
     variant with error string `"CREATE TABLE AS
     SELECT is a coordinator-only data-plane
     operation"`;
   - Reject in `fragment_plan` BEFORE
     `contains_join(&plan)` (mirrors ADR 032
     InsertSelect reject ordering);
   - Walker leaves in `first_join`, `find_exchange`,
     `count_exchanges`, `first_scan_metadata`,
     `replace_exchange_with_scan`, `replace_first_join`:
     return None / 0 / unchanged.

3. **Scheduler** (`distributed/scheduler.rs`):

   - Walker leaf in `first_scan_fingerprint`: return
     `None`.

4. **Server** (`server/mod.rs`):

   - Walker leaves in `join_strategy_label`,
     `contains_join`, `contains_broadcast_join`,
     `scan_schema_for_table_id`: return None / false /
     None;
   - `operator_kind_label` returns
     `"create_table_as_select"` for the new variant
     (distinct from `"catalog"` for plain `CreateTable`
     and `"insert_select"` for InsertSelect; preserves
     telemetry distinguishability).

**Verification:**

```sh
cargo test -p toron --test distributed_process_test
cargo test -p toron
```

## Phase 7 — Functional tests

**File:** new `crates/toron/tests/ctas_test.rs` (Claude
iter-2 A2).

**Required composition fixtures (Definition of Done):**

### Fixture A — Grouped aggregate source

```sql
CREATE TABLE src (id INT NOT NULL, amount BIGINT NOT NULL);
INSERT INTO src VALUES (1, 100), (1, 200), (2, 50);
CREATE TABLE totals AS
  SELECT id, SUM(amount) AS total FROM src GROUP BY id;
SELECT id, total FROM totals ORDER BY id;
-- Expected: (1, 300), (2, 50)
```

### Fixture B — INNER JOIN source

```sql
CREATE TABLE left_table (id INT NOT NULL, label TEXT);
CREATE TABLE right_table (left_id INT NOT NULL, score BIGINT NOT NULL);
INSERT INTO left_table VALUES (1, 'a'), (2, 'b');
INSERT INTO right_table VALUES (1, 100), (2, 200);
CREATE TABLE joined AS
  SELECT id, label, score
  FROM left_table
  INNER JOIN right_table ON left_table.id = right_table.left_id;
SELECT id, label, score FROM joined ORDER BY id;
-- Expected: (1, 'a', 100), (2, 'b', 200)
```

### Fixture C — Empty-source initialization

```sql
CREATE TABLE src (id INT NOT NULL);
CREATE TABLE empty_copy AS SELECT id FROM src WHERE id > 100;
SELECT id FROM empty_copy;
-- Expected: empty result set; query MUST SUCCEED (no "table not found")
```

The empirical verification of D8 / Codex iter-0 R6.
Without `materialize_empty_record_batch`, the final
SELECT fails with `"table not found"` after the empty
WHERE filter.

### Fixture D — Failure-before-catalog (regression test for D5 / R2)

```sql
CREATE TABLE dst AS SELECT missing FROM src;
```

Expected: error references missing column; the
destination table `dst` must NOT be resolvable in
catalog after the failed CTAS.

### Additional functional coverage

**Planner-level:**

- Planner accepts `CREATE TABLE dst AS SELECT id,
  amount FROM src`.
- Planner rejects `CREATE TABLE dst (a, b) AS SELECT
  ...` with R3 diagnostic.
- Planner rejects `CREATE TABLE IF NOT EXISTS dst AS
  SELECT ...` with R4 diagnostic.
- Planner rejects `CREATE OR REPLACE TABLE dst AS
  SELECT ...` (reuse existing ADR 029 reject path).
- Existing CREATE TABLE (no AS) tests remain green.

**Binder-level:**

- Binder infers schema verbatim from source SELECT
  output (names + types + nullability).
- Binder rejects duplicate output names (`SELECT id
  AS x, amount AS x FROM src`) with stable diagnostic.
- Binder rejects synthetic name aliases (`SELECT id
  AS __row_hlc__ FROM src`) with stable diagnostic.
- Binder rejects unsupported source output types if
  any source path can produce them.
- Binder binds source BEFORE inferring schema
  (regression test for D5 / R2 ordering).

**Coordinator-level:**

- CTAS over already-existing target rejects with
  stable diagnostic (matches ADR 029 D14 reject
  behavior; `catalog.create_table` reject propagates).
- Source binding failure (`CREATE TABLE x AS SELECT *
  FROM missing_table`) does NOT leave a phantom
  catalog entry (Fixture D regression test for D5 /
  R2).
- Write failure after catalog success leaves the
  catalog entry intact (D7 / R7 acknowledged behavior;
  test asserts the documented non-atomicity).
- Total source row count over `TABLE_WRITE_MAX_ROWS`
  fails before write (reuses ADR 032 helper).

**Distributed reject:**

- Distributed coordinator rejects
  `ExecutionPlan::CreateTableAsSelect` before
  `fragment_plan` with the D11 diagnostic.

**Defensive rejects:**

- Executor rejects direct CTAS execution with
  NotImplemented diagnostic.
- Fragmenter rejects direct CTAS fragmentation with
  `FragmentError::CreateTableAsSelectNotFragmentable`.

**Verification:**

```sh
cargo test -p toron --test ctas_test
```

## Phase 8 — Architecture guards

**File:** `crates/toron/tests/architecture_test.rs`.

**18 ADR-033-specific guards + 1 paired adjustment to
the existing `architecture_guards_unchanged_plus_sort_variant`
catch-all = 19 architecture guard touchpoints.**

Each guard names ADR 033 + its specific decision /
refinement in the diagnostic string (per the ADR 015
cycle narrow-detector pattern).

| # | Guard | Pins |
|---|---|---|
| 1 | `adr033_logical_variant_count_is_13` | `LogicalPlan` 12 → 13 |
| 2 | `adr033_execution_variant_count_is_11` | `ExecutionPlan` 10 → 11 |
| 3 | `adr033_execution_ctas_has_no_precreate_identity` | Body has `table`, `schema`, `source`; absence of `TableId` / `TableObjectId` / `PhysicalDescriptorFingerprint` |
| 4 | `adr033_planner_routes_only_query_create_table_to_ctas` | `lower_create_table` routes `query.is_some()` to `lower_create_table_as_select` |
| 5 | `adr033_planner_rejects_ctas_column_list_and_if_not_exists` | Stable diagnostics for both rejects |
| 6 | `adr033_binder_binds_source_before_inferring_schema` | Byte-position comparison in `bind_create_table_as_select` body |
| 7 | `adr033_schema_inference_rejects_synthetic_names` | Negative-pattern enforcement: absence of forked `"__row_hlc__"` / `"__row_key_id__"` / `"__row_routing_locality__"` / `"__row_table_object_id__"` / `"__row_tombstone__"` literals; positive presence of validator helper (Claude iter-2 A1) |
| 8 | `adr033_schema_inference_rejects_duplicate_names` | Duplicate-name reject in inference path |
| 9 | `adr033_schema_inference_uses_adr029_type_lattice` | Six-type admissibility helper present |
| 10 | `adr033_local_coordinator_intercepts_before_retry` | CTAS intercept precedes SELECT retry loop |
| 11 | `adr033_coordinator_creates_table_before_source_execution` | Byte-position comparison: `catalog.create_table` < `executor.execute_bound` in CTAS arm |
| 12 | `adr033_coordinator_uses_create_table_binding_for_write` | `WriteRequest` table identity from `binding.legacy_table_id` / `binding.table` / `binding.fingerprint` |
| 13 | `adr033_empty_source_ctas_writes_zero_row_batch` | CTAS arm matches `None` → calls `materialize_empty_record_batch` → writes |
| 14 | `adr033_distributed_coordinator_rejects_before_fragment` | Byte-position comparison in distributed coordinator |
| 15 | `adr033_fragmenter_rejects_before_join_or_exchange` | Byte-position comparison: `CreateTableAsSelectNotFragmentable` < `contains_join` / `count_exchanges` |
| 16 | `adr033_distributed_walkers_treat_ctas_as_leaf` | Enumerated walker arms across fragmenter / scheduler / server; telemetry label `"create_table_as_select"` |
| 17 | `adr033_does_not_modify_insert_select_empty_contract` | `materialize_insert_select_batch` body contains `return Ok(None)` for zero rows |
| 18 | `adr033_does_not_add_plan_arena_dependency` | `Cargo.toml` absence of `bumpalo` / `typed-arena` / `slab` / `generational-arena` |

Paired adjustment (Claude iter-2 A4):

| Existing guard | Adjustment |
|---|---|
| `architecture_guards_unchanged_plus_sort_variant` | `LogicalPlan` count assert bumps 12 → 13 + add `CreateTableAsSelect` content assert; `ExecutionPlan` count assert bumps 10 → 11 + add `CreateTableAsSelect` content assert |

Sample guard #7 (Claude iter-2 A1 strict pin):

```rust
#[test]
fn adr033_schema_inference_rejects_synthetic_names() {
    let binder = read_to_string(
        manifest_dir().join("src/binder.rs")
    ).unwrap();
    let body = brace_blocks_after_marker(
        &binder, "async fn bind_create_table_as_select"
    ).into_iter().next().expect("bind_create_table_as_select body").1;
    // Negative pattern: binder must NOT fork the five Wendelon literals anywhere.
    for literal in [
        "\"__row_hlc__\"",
        "\"__row_key_id__\"",
        "\"__row_routing_locality__\"",
        "\"__row_table_object_id__\"",
        "\"__row_tombstone__\"",
    ] {
        assert!(
            !binder.contains(literal),
            "ADR 033 R2 (iter-1): binder must not fork Wendelon synthetic name literal {literal}"
        );
    }
    let constants = [
        "STORAGE_ROW_HLC_COLUMN",
        "STORAGE_ROW_KEY_ID_COLUMN",
        "STORAGE_ROW_ROUTING_LOCALITY_COLUMN",
        "STORAGE_ROW_TABLE_OBJECT_ID_COLUMN",
        "STORAGE_ROW_TOMBSTONE_COLUMN",
    ];
    let imports_wendelon_constants =
        constants.iter().all(|constant| binder.contains(constant));
    let calls_neutral_helper =
        binder.contains("is_storage_synthetic_column_name")
            || binder.contains("is_storage_synthetic_column");
    assert!(
        imports_wendelon_constants || calls_neutral_helper,
        "ADR 033 R2 (iter-1): binder must reuse Wendelon synthetic constants directly or through a neutral helper"
    );
    // Positive pattern: validator helper must exist.
    assert!(
        body.contains("validate_ctas_column_name")
            || body.contains("infer_ctas_schema"),
        "ADR 033 R5 (iter-0): binder must call CTAS schema validator"
    );
}
```

**Verification:**

```sh
cargo test -p toron --test architecture_test
```

## Phase 9 — Promotion-time housekeeping (out of PR scope)

1. Roadmap bump: move ADR 033 from candidate to
   promoted; advance active section to ADR 034.
2. Archive WIP iteration docs per established
   convention.
3. Update bench-results pointer if applicable.

## Locked patch order

1. **Phase 0** — Preflight audit.
2. **Phase 1** — IR shape (`ir.rs` + `ir/execution.rs`
   variants).
3. **Phase 2** — SQL lowering (`sql_planner.rs`
   `lower_create_table_as_select` + walker leaves).
4. **Phase 3** — Binder + schema inference
   (`binder.rs` `bind_create_table_as_select` +
   `infer_ctas_schema` + `validate_ctas_column_name`
   + `is_supported_ctas_column_type` + walker
   leaves).
5. **Phase 4** — Coordinator helpers
   (`coordinator/write.rs::materialize_empty_record_batch`).
6. **Phase 5** — Coordinator routing
   (`coordinator.rs` local intercept + CTAS arm +
   distributed reject).
7. **Phase 6** — Defensive rejects + walkers
   (`exec.rs` + `fragment.rs` + `scheduler.rs` +
   `server/mod.rs` leaf-treatment + defensive
   rejects + telemetry label).
8. **Phase 7** — Functional tests
   (`tests/ctas_test.rs`: four required fixtures +
   planner / binder / coordinator / reject
   coverage).
9. **Phase 8** — Architecture guards
   (`tests/architecture_test.rs`: 18 new + 1
   paired adjustment).

Mechanical ordering: Phases 1-2 precede Phase 3
(binder consumes IR). Phase 4 gates Phase 5
(coordinator calls the helper). Phase 5 gates Phase 6
(walker rejects become real when coordinator wiring is
complete). Phases 7-8 are verification.

## Definition of Done

- [ ] `LogicalPlan::CreateTableAsSelect { table,
      source }` defined (12 → 13).
- [ ] `ExecutionPlan::CreateTableAsSelect { table,
      schema, source }` defined (10 → 11); **no
      TableId / TableObjectId / fingerprint fields**
      (R1 iter-0).
- [ ] `SqlPlanner` routes `CreateTable.query.is_some()`
      to `lower_create_table_as_select`; exhaustive
      `CreateTable` destructure preserved.
- [ ] `lower_create_table_as_select` rejects non-empty
      column list (R3 iter-0) + `if_not_exists` (R4
      iter-0) with stable diagnostics.
- [ ] `Binder::bind_create_table_as_select` binds
      source BEFORE inferring schema (R2 iter-0).
- [ ] `infer_ctas_schema` + `validate_ctas_column_name`
      + `is_supported_ctas_column_type` helpers
      present.
- [ ] Inferred schema validated against ADR 029
      lattice (six types + no duplicates + no
      synthetic names) (R5 iter-0).
- [ ] Binder uses Wendelon synthetic constants
      directly or neutral helper; **no forked
      `__row_*__` literals** (R2 iter-1).
- [ ] `materialize_empty_record_batch` helper present
      in `coordinator/write.rs` (C1 + R6 iter-0).
- [ ] `materialize_insert_select_batch` unchanged —
      still returns `Ok(None)` for zero rows (guard
      #17).
- [ ] `QueryCoordinator::execute_query` intercepts
      `LogicalPlan::CreateTableAsSelect` next to
      Insert / InsertSelect, before SELECT retry loop.
- [ ] `execute_write_plan` CTAS arm sequence:
      `catalog.create_table` → `execute_bound(source,
      None)` → `materialize_insert_select_batch`
      (Some) or `materialize_empty_record_batch`
      (None) → single `writer.write` → `Ok(Vec::new())`
      (R1 iter-1 + D6).
- [ ] `WriteRequest` table identity from
      `create_table` binding (guard #12).
- [ ] `DistributedQueryCoordinator::execute_query`
      rejects CTAS before `fragment_plan` with D11
      diagnostic.
- [ ] `FragmentError::CreateTableAsSelectNotFragmentable`
      reject fires before `contains_join` /
      `count_exchanges`.
- [ ] Distributed / scheduler / server walkers treat
      CTAS as leaf; binder `optimize_for_required`
      is documented exception (descends with `None`).
- [ ] Telemetry label `"create_table_as_select"`.
- [ ] Defensive `Executor::execute_bound` reject for
      uncoordinated CTAS.
- [ ] **All 18 ADR-033-specific architecture guards
      pass.**
- [ ] **Paired adjustment to
      `architecture_guards_unchanged_plus_sort_variant`
      passes.**
- [ ] **Fixture A (SUM grouped aggregate) passes
      end-to-end.**
- [ ] **Fixture B (INNER JOIN source) passes end-to-
      end.**
- [ ] **Fixture C (empty-source initialization)
      passes end-to-end — empty SELECT must SUCCEED
      on the newly-created table.**
- [ ] **Fixture D (failure-before-catalog) passes —
      missing source column fails without creating
      destination table.**
- [ ] All existing CREATE TABLE / VALUES INSERT /
      INSERT SELECT tests green.
- [ ] All existing SELECT-only plan-digest fixtures
      green (D14 re-stabilization).
- [ ] ADR 029 / 030 / 031 / 032 invariants preserved.
- [ ] No `blake3` dependency added.
- [ ] No external allocator crate added.
- [ ] Roadmap moves ADR 033 from candidate to
      promoted.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.

## Verification commands

```sh
# Development cadence
cargo check -p toron

# Pre-PR checks
cargo fmt --check
cargo test -p toron --test ctas_test
cargo test -p toron --test architecture_test
cargo test -p toron --test create_table_test
cargo test -p toron --test insert_test
cargo test -p toron --test group_by_test
cargo test -p toron --test distributed_process_test
cargo test -p toron

# Post-PR audits
rg -n "CreateTableAsSelect" crates/toron/src crates/toron/tests
rg -n "fn lower_create_table_as_select" crates/toron/src/sql_planner.rs
rg -n "fn bind_create_table_as_select" crates/toron/src/binder.rs
rg -n "fn infer_ctas_schema|fn validate_ctas_column_name|fn is_supported_ctas_column_type" \
  crates/toron/src/binder.rs
rg -n "fn materialize_empty_record_batch" crates/toron/src/coordinator/write.rs
rg -n "CreateTableAsSelectNotFragmentable" crates/toron/src/distributed

# Verify ADR 032 helper untouched (C1)
rg -A 3 "fn materialize_insert_select_batch" \
  crates/toron/src/coordinator/write.rs | grep -q "Ok(None)" \
  || echo "ADR 033 guard #17 violation: materialize_insert_select_batch zero-row contract changed"

# Verify Wendelon synthetic constants discipline (R2 iter-1)
rg -n "__row_hlc__|__row_key_id__|__row_routing_locality__|__row_table_object_id__|__row_tombstone__" \
  crates/toron/src/binder.rs
# Expected: zero hits IF binder imports Wendelon constants directly;
# OR hits only inside a neutral helper module that re-exports them.

# Verify evaluate_cast still pub(crate) and unconsumed by CTAS path (R3 iter-1)
rg -n "evaluate_cast" crates/toron/src/coordinator/write.rs
# Expected: zero hits in CTAS arm (CTAS doesn't coerce)

# Verify no shadow dependencies
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits
```

## Implementation risks

| ID | Risk | Mitigation |
|---|---|---|
| R1 | VALUES INSERT path silently regresses through `lower_create_table` refactor | Phase 2 routes only on `query.is_some()`; non-CTAS CREATE TABLE path byte-identical. |
| R2 | Distributed CTAS accidentally fragments | Phases 5 + 6 add three independent reject sites; Phase 8 guards #14 + #15 + #16 pin them. |
| R3 | Source binding happens after catalog mutation | Phase 3 task 2 explicit ordering; Phase 8 guard #6 byte-position comparison. |
| R4 | Source execution happens before catalog mutation | Phase 5 task 2 explicit ordering; Phase 8 guard #11 byte-position comparison. |
| R5 | Empty-source CTAS leaves storage uninitialized | Phase 4 helper + Phase 5 fallback + Phase 7 Fixture C + Phase 8 guard #13. |
| R6 | ADR 032 `materialize_insert_select_batch` modified | Phase 4 task 3 explicit non-modification; Phase 8 guard #17. |
| R7 | Binder forks Wendelon synthetic name literals | Phase 3 task 5 explicit constants-or-neutral-helper rule; Phase 8 guard #7 negative-pattern enforcement. |
| R8 | Pre-create table identity leaks into bound variant | Phase 1 task 2 explicit field set; Phase 8 guard #3. |
| R9 | `WriteRequest` constructed from binder-side fields instead of catalog binding | Phase 5 task 2 explicit `binding.*` field usage; Phase 8 guard #12. |
| R10 | Walkers grow broad CTAS traversal | Phase 6 task 3 leaf-treatment; Phase 8 guard #16. |
| R11 | `Insert` / `InsertSelect` VALUES guards weaken via shared helper drift | No shared helper between CTAS and InsertSelect binder paths (Phase 3 task 3 explicit non-share). |
| R12 | Plan-digest invariant accidentally re-broken | Phase 8 verifies SELECT-only fixtures unchanged. |
| R13 | Source-to-target coercion sneaks into CTAS path | R3 iter-1 + Phase 5 identity ordinals + no `evaluate_cast` import in CTAS arm. |
| R14 | Test file growth in `insert_test.rs` muddles ADR scope | New `crates/toron/tests/ctas_test.rs` (A2 iter-2). |
| R15 | Variant-count guards not bumped | Phase 8 paired adjustment to `architecture_guards_unchanged_plus_sort_variant` (A4 iter-2). |
| R16 | Empty-source helper accidentally re-implemented inline | Phase 4 helper extracted; Phase 8 guard #13 pins the call path. |
