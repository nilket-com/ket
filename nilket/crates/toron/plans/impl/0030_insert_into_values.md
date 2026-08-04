# Implementation Plan: ADR 030 — Literal `INSERT INTO ... VALUES`

**Companion ADR:** `plans/adr/0030_insert_into_values.md`

## Goal

Add SQL:

```sql
INSERT INTO [schema.]table_name [(column_name, ...)]
VALUES (literal, ...), ...
```

Route through planner → binder → coordinator →
`StorageWrite`. `WendelonStorageWrite` uses the high-
level `wendelon_client::Client::table_write` wrapper
with `WriteOptions { await_finalization: true,
..Default::default() }`. `InMemoryTableStorage`
provides the test backend. Distributed INSERT returns
a stable unsupported diagnostic.

Preserve:

- All ADR 029 architecture guards (15 + 2 adjusted).
- Existing SELECT query corpus plan digests byte-
  identical (D4 / D17).
- `QueryCoordinator`'s descriptor-evolution retry loop
  semantics for SELECT (ADR 029 D15 + D12 carry-
  forward).
- `Catalog` trait as metadata-only (D6).
- `InMemoryStorageScan` shape for existing read-only
  fixtures (D11).
- All cross-ADR invariants from ADR 022 / 023 / 024 /
  025 / 026 / 027 / 028 / 029.

No new IR expression variant; no new aggregate kernel;
no fragmenter detector change for SELECT paths; no PBC
opcode change; no cross-ring metadata-write
engagement; no Wendelon row-identity construction; no
`blake3` dependency; no `RecordBatch` inside any
`ExecutionPlan` variant; no `Catalog` write method; no
distributed INSERT execution.

Ten-phase rollout: baseline audit → IR shape → SQL
planner → binder → storage write boundary → coordinator
materialization + routing → executor/fragmenter
guardrails → architecture guards → behavioral tests →
final verification.

## Touchpoint sweep (code-grounded, current `main`)

| File | Anchor | Phase |
|---|---|---|
| `crates/toron/src/ir.rs` | `pub enum InsertLiteral`; `LogicalPlan::Insert { table: TableRef, columns: Option<Vec<String>>, rows: Vec<Vec<InsertLiteral>> }` leaf variant | 1 |
| `crates/toron/src/ir/execution.rs` | `ExecutionPlan::Insert { local_table_id: TableId, table: TableObjectId, fingerprint: PhysicalDescriptorFingerprint, schema: SchemaRef, rows: Vec<Vec<ScalarValue>> }`; `schema()` returns `Arc::new(Schema::empty())` | 1 |
| `crates/toron/src/sql_planner.rs::plan` | `Statement::Insert` arm; widened catch-all diagnostic | 2 |
| `crates/toron/src/sql_planner.rs::lower_insert` | NEW; exhaustive `sqlparser::ast::Insert` destructure (19 fields) without `..` | 2 |
| `crates/toron/src/sql_planner.rs::lower_insert_target_name` | NEW; 1- or 2-part names; rejects 3-part + table functions | 2 |
| `crates/toron/src/sql_planner.rs::lower_insert_literal` | NEW; 6-variant `InsertLiteral` lattice; rejects expressions | 2 |
| `crates/toron/src/binder.rs::bind_plan` | New `LogicalPlan::Insert` arm → `bind_insert` | 3 |
| `crates/toron/src/binder.rs::bind_insert` | NEW; target resolve, column-list resolution, row cap, NULL fill, coercion | 3 |
| `crates/toron/src/binder.rs::coerce_insert_literal` | NEW; per-field `InsertLiteral → ScalarValue` over ADR 029 six-type lattice | 3 |
| `crates/toron/src/storage.rs::WriteRequest` / `WriteOutcome` / `StorageWrite` | NEW trait + supporting types | 4 |
| `crates/toron/src/storage.rs::InMemoryTableStorage` | NEW; `StorageScan` + `StorageWrite` over `Mutex<HashMap<TableId, InMemoryTableData>>` | 4 |
| `crates/toron/src/storage.rs::WendelonStorageWrite` | NEW; mirrors `WendelonStorageScan` shape; uses `wendelon_client::Client::table_write` wrapper | 4 |
| `crates/toron/src/coordinator.rs` | `QueryCoordinator` gains `writer: Arc<dyn StorageWrite>`; private `UnsupportedStorageWrite` default; `with_writer` constructor; `LogicalPlan::Insert` intercept BEFORE retry loop; `DistributedQueryCoordinator` reject BEFORE `fragment_plan`; `mod write;` declaration | 5 |
| `crates/toron/src/coordinator/write.rs` (NEW) | `materialize_insert_batch(schema, rows)` private helper; user-columns only | 5 |
| `crates/toron/src/exec.rs::Executor::execute` / `execute_bound` | Defensive `Insert` arms returning `NotImplemented` | 6 |
| `crates/toron/src/exec.rs::Executor::context` | NEW pub accessor `pub fn context(&self) -> &ExecutionContext` | 5 |
| `crates/toron/src/distributed/fragment.rs` | `FragmentError::InsertNotFragmentable`; `fragment_plan` top-level reject; walker `Insert` arms | 6 |
| `crates/toron/src/distributed/scheduler.rs` + `crates/toron/src/server/mod.rs` | Walker `Insert` arms (`None` / `false` / `"insert"`) | 6 |
| `crates/toron/tests/architecture_test.rs` | 12 new ADR 030 guards + 2 adjusted ADR 029 guards | 7 |
| `crates/toron/tests/insert_test.rs` (NEW) | Planner positive/negative, binder positive/negative, coordinator local + distributed, e2e `CREATE+INSERT+SELECT`, defensive rejects | 8 |
| `crates/toron/tests/create_table_test.rs` | Unchanged — ADR 029 fixtures continue to pass | — |
| `crates/toron/tests/wendelon_storage_execution_test.rs::assert_scan_has_no_pbc` | `Insert` arm with `panic!("INSERT has no scan")` | 6 |

## Phase 0 — Baseline Audit

Before edits, confirm starting state:

```sh
rg -n "Statement::CreateTable|ExecutionPlan::CreateTable|execute_catalog_plan" crates/toron/src
rg -n "pub enum LogicalPlan|pub enum ExecutionPlan|fn schema" crates/toron/src/ir.rs crates/toron/src/ir/execution.rs
rg -n "pub trait StorageScan|InMemoryStorageScan|WendelonStorageScan" crates/toron/src/storage.rs
rg -n "table_write|WriteOptions|TableWriteReceipt|TableWriteRequest" crates/wendelon crates/toron/src
rg -n "From<wendelon_client::ClientError>" crates/toron/src/error.rs
rg -n "STORAGE_ROW_HLC_COLUMN|STORAGE_ROW_KEY_ID_COLUMN" crates/toron/src
rg -n "blake3" crates/toron crates/toron/Cargo.toml
rg -n "fragment_plan|count_exchanges|contains_join|execute_bound|execute" crates/toron/src
```

Verify five **load-bearing baseline facts**:

1. **Toron imports the `wendelon-client` wrapper
   crate.** `crates/toron/Cargo.toml:25`:
   `wendelon-client = { path = "../wendelon/crates/wendelon-client" }`.
   `WendelonStorageScan` at `storage.rs:159` already
   uses `Arc<wendelon_client::Client>`. ADR 030 uses
   the same wrapper. The low-level
   `wendelon::client::WendelonClient::table_write(TableWriteRequest)`
   at `crates/wendelon/src/client/core.rs:447` is NOT
   the call site.
2. **`impl From<wendelon_client::ClientError> for
   StorageError` exists** (`error.rs:48`) with
   complete variant coverage including
   `TableWriteRejected`, `WriteFailed`, `QuotaExceeded`,
   `TenantSuspended`, `TenantUnavailable`, `Io`,
   `ReadFailed`, `TableDescriptorEvolved`,
   `SchemaUpdateRejected`, `AuthorizationFailed`. No
   new mapping required.
3. **`ExecutionPlan` derives `Serialize + Deserialize`**
   (`ir/execution.rs:24`). The `Vec<Vec<ScalarValue>>`
   shape (vs `RecordBatch`) preserves serde
   derivation.
4. **`TenantId` conversion mirror exists.** Toron
   `pub struct TenantId(pub u64)` (`storage.rs:16`)
   maps to Wendelon via
   `wendelon::schema::TenantId::new(request.tenant_id.0)`
   — already used by `WendelonStorageScan` at
   `storage.rs:206`.
5. **`InMemoryStorageScan` callers are read-only.**
   ADR 029 `create_table_test.rs::empty_storage()`
   helper depends on the current per-instance
   immutable shape. Phase 4 `InMemoryTableStorage` is
   additive; `InMemoryStorageScan` does NOT change.

Verify `blake3` is **not** declared in
`crates/toron/Cargo.toml` — ADR 030 must not add it.

**Preserve verbatim:**

- All existing `Catalog` trait methods + their
  semantics.
- All existing `LogicalPlan` / `ExecutionPlan` variants
  + their walkers (DML is additive).
- `Executor::execute` / `execute_bound` SELECT and
  CREATE TABLE paths.
- `fragment_plan` SELECT paths.
- `QueryCoordinator`'s descriptor-evolution retry
  semantics.
- All cross-ADR contracts (022 / 023 / 024 / 025 /
  026 / 027 / 028 / 029).

## Phase 1 — IR Shape

**Goal.** Add `InsertLiteral` enum + `LogicalPlan::Insert`
+ `ExecutionPlan::Insert` leaf variants.

**Files:** `crates/toron/src/ir.rs`,
`crates/toron/src/ir/execution.rs`.

**Tasks:**

1. **Add `InsertLiteral`:**

   ```rust
   #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
   pub enum InsertLiteral {
       Null,
       Boolean(bool),
       Int64(i64),
       UInt64(u64),
       Float64(f64),
       Utf8(String),
   }
   ```

2. **Add `LogicalPlan::Insert`** (leaf):

   ```rust
   LogicalPlan::Insert {
       table: TableRef,
       columns: Option<Vec<String>>,
       rows: Vec<Vec<InsertLiteral>>,
   }
   ```

   `columns: None` means "all columns in schema
   order".

3. **Add `ExecutionPlan::Insert`** (leaf):

   ```rust
   ExecutionPlan::Insert {
       local_table_id: TableId,
       table: TableObjectId,
       fingerprint: PhysicalDescriptorFingerprint,
       schema: SchemaRef,
       rows: Vec<Vec<ScalarValue>>,
   }
   ```

4. **`ExecutionPlan::schema()` arm:**

   ```rust
   ExecutionPlan::Insert { .. } => Arc::new(Schema::empty()),
   ```

5. **Update exhaustive `LogicalPlan` walkers**
   (`sql_planner.rs::plan_qualifiers`,
   `plan_contains_join`). For `Insert`: leaf —
   `Vec::new()` / `false`.

6. **Update exhaustive `ExecutionPlan` walkers** in
   `binder.rs`, `fragment.rs`, `scheduler.rs`,
   `server/mod.rs`, and
   `wendelon_storage_execution_test.rs`:

   - `contains_join` → `false`
   - `contains_broadcast_join` → `false`
   - `count_exchanges` → `0`
   - `find_exchange` → `None`
   - `first_join` → `None`
   - `first_scan_metadata` / `first_scan_fingerprint`
     → `None`
   - `join_strategy_label` → `None`
   - `operator_kind_label` → `"insert"`
   - `replace_exchange_with_scan` /
     `replace_first_join` → `plan` (pass-through)
   - `scan_schema_for_table_id` → `None`
   - `assert_scan_has_no_pbc` → `panic!("INSERT has no
     scan")`

**Verification:**

```sh
cargo check -p toron
```

Expected: compile errors at every existing exhaustive
match site. Fix them as leaf / `None` arms.

## Phase 2 — SQL Planner

**Goal.** Add `Statement::Insert` arm + DML-specific
helpers.

**Files:** `crates/toron/src/sql_planner.rs`.

**Tasks:**

1. **Update `SqlPlanner::plan`:**

   ```rust
   match &ast[0] {
       Statement::Query(query) => self.lower_query(query),
       Statement::CreateTable(create_table) => self.lower_create_table(create_table),
       Statement::Insert(insert) => self.lower_insert(insert),
       _ => Err(unsupported(
           "unsupported SQL statement; only SELECT, CREATE TABLE, and INSERT are supported",
       )),
   }
   ```

2. **Implement `lower_insert` with exhaustive 19-field
   destructure (no `..`):**

   ```rust
   fn lower_insert(&self, insert: &Insert) -> Result<LogicalPlan, FrontendError> {
       let Insert {
           or,
           ignore,
           into,
           table,
           table_alias,
           columns,
           overwrite,
           source,
           assignments,
           partitioned,
           after_columns,
           has_table_keyword,
           on,
           returning,
           replace_into,
           priority,
           insert_alias,
           settings,
           format_clause,
       } = insert;

       if or.is_some() {
           return Err(unsupported("unsupported INSERT clause: OR (REPLACE/IGNORE/...)"));
       }
       if *ignore {
           return Err(unsupported("unsupported INSERT clause: IGNORE"));
       }
       if !*into {
           return Err(unsupported("unsupported INSERT syntax: missing INTO keyword"));
       }
       if *has_table_keyword {
           return Err(unsupported("unsupported INSERT clause: TABLE keyword"));
       }
       if table_alias.is_some() {
           return Err(unsupported("unsupported INSERT clause: target alias"));
       }
       if *overwrite {
           return Err(unsupported("unsupported INSERT clause: OVERWRITE"));
       }
       if !assignments.is_empty() {
           return Err(unsupported("unsupported INSERT clause: SET assignments"));
       }
       if partitioned.is_some() {
           return Err(unsupported("unsupported INSERT clause: PARTITION"));
       }
       if !after_columns.is_empty() {
           return Err(unsupported("unsupported INSERT clause: AFTER columns"));
       }
       if on.is_some() {
           return Err(unsupported("unsupported INSERT clause: ON CONFLICT / ON DUPLICATE KEY"));
       }
       if returning.is_some() {
           return Err(unsupported("unsupported INSERT clause: RETURNING"));
       }
       if *replace_into {
           return Err(unsupported("unsupported INSERT clause: REPLACE INTO"));
       }
       if priority.is_some() {
           return Err(unsupported("unsupported INSERT clause: priority"));
       }
       if insert_alias.is_some() {
           return Err(unsupported("unsupported INSERT clause: insert alias"));
       }
       if settings.is_some() {
           return Err(unsupported("unsupported INSERT clause: SETTINGS"));
       }
       if format_clause.is_some() {
           return Err(unsupported("unsupported INSERT clause: FORMAT"));
       }

       let table_ref = lower_insert_target_name(table)?;
       let column_list = lower_insert_column_list(columns)?;
       let rows = lower_insert_source(source.as_deref())?;

       Ok(LogicalPlan::Insert {
           table: table_ref,
           columns: column_list,
           rows,
       })
   }
   ```

3. **Implement `lower_insert_target_name`:**

   ```rust
   fn lower_insert_target_name(target: &TableObject) -> Result<TableRef, FrontendError> {
       match target {
           TableObject::TableName(name) => match name.0.as_slice() {
               [table] => Ok(TableRef::new(table.value.clone())),
               [schema, table] => Ok(TableRef::with_schema(
                   schema.value.clone(),
                   table.value.clone(),
               )),
               _ => Err(unsupported(
                   "INSERT supports only one- or two-part table names",
               )),
           },
           TableObject::TableFunction(_) => Err(unsupported(
               "INSERT target must be a table name; table functions are not supported",
           )),
       }
   }
   ```

   **Critical:** does NOT delegate to
   `table_ref_from_object_name` (same anti-delegation
   discipline as ADR 029 D9).

4. **Implement `lower_insert_column_list`:**

   ```rust
   fn lower_insert_column_list(columns: &[Ident]) -> Result<Option<Vec<String>>, FrontendError> {
       if columns.is_empty() {
           return Ok(None);
       }
       Ok(Some(columns.iter().map(|ident| ident.value.clone()).collect()))
   }
   ```

   Duplicate detection happens at bind time with
   schema context.

5. **Implement `lower_insert_source`:**

   ```rust
   fn lower_insert_source(source: Option<&Query>) -> Result<Vec<Vec<InsertLiteral>>, FrontendError> {
       let query = source.ok_or_else(|| unsupported("INSERT requires a VALUES clause"))?;
       // Reject query-level features (WITH, ORDER BY, LIMIT, OFFSET, FETCH, FOR_CLAUSE).
       if query.with.is_some()
           || query.order_by.is_some()
           || query.limit.is_some()
           || !query.limit_by.is_empty()
           || query.offset.is_some()
           || query.fetch.is_some()
           || !query.locks.is_empty()
           || query.for_clause.is_some()
           || query.settings.is_some()
           || query.format_clause.is_some()
       {
           return Err(unsupported("unsupported INSERT source: must be VALUES only"));
       }
       let values = match query.body.as_ref() {
           SetExpr::Values(values) => values,
           _ => return Err(unsupported("INSERT source must be VALUES (INSERT ... SELECT not supported)")),
       };
       if values.explicit_row {
           return Err(unsupported("unsupported INSERT clause: explicit ROW keyword"));
       }
       if values.rows.is_empty() {
           return Err(unsupported("INSERT VALUES must contain at least one row"));
       }
       values.rows.iter()
           .map(|row| row.iter().map(lower_insert_literal).collect::<Result<Vec<_>, _>>())
           .collect()
   }
   ```

6. **Implement `lower_insert_literal`:**

   ```rust
   fn lower_insert_literal(expr: &SqlExpr) -> Result<InsertLiteral, FrontendError> {
       match expr {
           SqlExpr::Value(Value::Null) => Ok(InsertLiteral::Null),
           SqlExpr::Value(Value::Boolean(b)) => Ok(InsertLiteral::Boolean(*b)),
           SqlExpr::Value(Value::Number(text, _)) => parse_numeric_literal(text, false),
           SqlExpr::UnaryOp { op: UnaryOperator::Minus, expr: inner } => {
               match inner.as_ref() {
                   SqlExpr::Value(Value::Number(text, _)) => parse_numeric_literal(text, true),
                   _ => Err(unsupported("INSERT VALUES must be literals; complex expressions not supported")),
               }
           }
           SqlExpr::Value(Value::SingleQuotedString(s))
               | SqlExpr::Value(Value::NationalStringLiteral(s)) => Ok(InsertLiteral::Utf8(s.clone())),
           _ => Err(unsupported("INSERT VALUES must be literals; complex expressions not supported")),
       }
   }

   fn parse_numeric_literal(text: &str, negate: bool) -> Result<InsertLiteral, FrontendError> {
       // Prefer Int64 > UInt64 > Float64; respect negate.
       if let Ok(v) = text.parse::<i64>() {
           return Ok(InsertLiteral::Int64(if negate { -v } else { v }));
       }
       if !negate {
           if let Ok(v) = text.parse::<u64>() {
               return Ok(InsertLiteral::UInt64(v));
           }
       }
       text.parse::<f64>()
           .map(|v| InsertLiteral::Float64(if negate { -v } else { v }))
           .map_err(|_| unsupported(format!("INSERT VALUES literal not representable: {text}")))
   }
   ```

   **Critical:** does NOT delegate to `lower_expr`
   (same anti-delegation discipline as ADR 029 D11).
   `lower_expr` rejects bare `NULL` and parses
   integers as `i32`-first.

**Verification:** `cargo check -p toron`.

## Phase 3 — Binder

**Goal.** Add `LogicalPlan::Insert` bind arm with
target resolution, column-list resolution, typed-NULL
fill, target-type coercion, and row cap enforcement.

**Files:** `crates/toron/src/binder.rs`.

**Tasks:**

1. **Add `TABLE_WRITE_MAX_ROWS` constant:**

   ```rust
   pub const TABLE_WRITE_MAX_ROWS: usize = 65_536;
   ```

2. **Add `bind_plan` arm:**

   ```rust
   LogicalPlan::Insert { table, columns, rows } =>
       self.bind_insert(table, columns.as_deref(), rows).await,
   ```

3. **Implement `bind_insert`:**

   1. `let binding = self.catalog.resolve_table(table).await?;`
   2. Reject `rows.is_empty()` →
      `BindingError::UnsupportedExpression { message: "INSERT VALUES must contain at least one row" }`.
   3. Reject `rows.len() > TABLE_WRITE_MAX_ROWS` →
      `BindingError::UnsupportedExpression { message: format!("INSERT row count {} exceeds maximum {TABLE_WRITE_MAX_ROWS}", rows.len()) }`.
   4. **Column resolution:**
      - `None` → expected_arity =
        `binding.schema.fields().len()`; target fields
        = `binding.schema.fields()` in order; mapping =
        identity.
      - `Some(names)`:
        - Detect duplicates → `BindingError::UnsupportedExpression { message: format!("duplicate INSERT target column: {name}") }`.
        - Resolve each `name` to a field index
          (case-sensitive against schema field names);
          unknown → `BindingError::UnknownColumn { column: name.clone() }`.
        - Build per-position target field reference.
   5. **Per-row arity check** → `BindingError::TypeMismatch { message: format!("INSERT row {ordinal} has {actual} values; expected {expected}") }`.
   6. **Per-cell coercion** via
      `coerce_insert_literal(literal, field) -> Result<ScalarValue, BindingError>`.
   7. **Schema-order assembly:** for `columns =
      Some(names)`, place coerced values at their
      target field index; fill unmentioned fields with
      typed `ScalarValue::<Field::data_type>(None)`;
      reject unmentioned NOT NULL fields with
      `BindingError::TypeMismatch { message: format!("INSERT omits non-nullable column: {column}") }`.
   8. Return `ExecutionPlan::Insert { local_table_id:
      binding.local_table_id, table: binding.table,
      fingerprint: binding.fingerprint, schema:
      binding.schema.clone(), rows: bound_rows }`.

4. **Implement `coerce_insert_literal`:**

   Lattice covers ADR 029 D11 six types:

   - `Null`: typed `ScalarValue::<target>(None)`;
     reject if `!field.is_nullable()`.
   - `Boolean(b)`: only into `DataType::Boolean`.
   - `Int64(v)` / `UInt64(v)`:
     - `Int32`: `try_into::<i32>()`; overflow rejects.
     - `Int64`: direct (Int64 → Int64; UInt64 →
       `try_into::<i64>()` if positive).
     - `Float32`: `as f32` cast; document precision
       loss is tolerated.
     - `Float64`: `as f64` cast.
     - Reject `Utf8`, `Boolean`.
   - `Float64(v)`:
     - `Float32`: `as f32`.
     - `Float64`: direct.
     - Reject `Int32`, `Int64`, `Boolean`, `Utf8`.
     - NaN / infinity permitted (mirror Arrow native
       semantics).
   - `Utf8(s)`: only into `DataType::Utf8`.

   Diagnostics include `field.name()` and target
   `DataType`.

5. **Update exhaustive walkers** in `binder.rs`
   (`collect_qualifiers`, `terminal_exchange`,
   `optimize_for_required`). `Insert` is leaf — no
   recursion, no metadata.

**Verification:**

```sh
cargo check -p toron
```

## Phase 4 — Storage write boundary

**Goal.** Add `StorageWrite` trait, `InMemoryTableStorage`
backend, and `WendelonStorageWrite` backend.

**Files:** `crates/toron/src/storage.rs`.

**Tasks:**

1. **Add `WriteRequest` / `WriteOutcome` /
   `StorageWrite`:**

   ```rust
   pub struct WriteRequest {
       pub session_id: SessionId,
       pub query_id: QueryId,
       pub tenant_id: TenantId,
       pub table_id: TableId,
       pub table_object_id: TableObjectId,
       pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
       pub payload: RecordBatch,
       pub profile_hint: Option<QueryProfileHint>,
       pub trace_context: Option<TraceContext>,
   }

   #[non_exhaustive]
   pub struct WriteOutcome {
       pub rows: usize,
   }

   #[async_trait::async_trait]
   pub trait StorageWrite: Send + Sync {
       async fn write(&self, request: WriteRequest)
           -> Result<WriteOutcome, StorageError>;
   }
   ```

2. **Add `InMemoryTableStorage`:**

   ```rust
   struct InMemoryTableData {
       table_object_id: TableObjectId,
       physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
       batch: RecordBatch,
   }

   pub struct InMemoryTableStorage {
       tables: Mutex<HashMap<TableId, InMemoryTableData>>,
   }

   impl InMemoryTableStorage {
       pub fn new() -> Self { Self { tables: Mutex::new(HashMap::new()) } }
   }

   #[async_trait::async_trait]
   impl StorageWrite for InMemoryTableStorage {
       async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError> {
           let mut tables = self.tables.lock().expect("InMemoryTableStorage lock poisoned");
           match tables.entry(request.table_id) {
               Entry::Vacant(entry) => {
                   // First write seeds the table.
                   let rows = request.payload.num_rows();
                   entry.insert(InMemoryTableData {
                       table_object_id: request.table_object_id,
                       physical_descriptor_fingerprint: request.physical_descriptor_fingerprint,
                       batch: request.payload,
                   });
                   Ok(WriteOutcome { rows })
               }
               Entry::Occupied(mut entry) => {
                   let data = entry.get_mut();
                   if data.table_object_id != request.table_object_id
                       || data.physical_descriptor_fingerprint != request.physical_descriptor_fingerprint
                   {
                       return Err(StorageError::DescriptorEvolved);
                   }
                   if data.batch.schema() != request.payload.schema() {
                       return Err(StorageError::Internal {
                           message: "INSERT payload schema does not match existing table schema".to_string(),
                       });
                   }
                   let rows = request.payload.num_rows();
                   let batches = [data.batch.clone(), request.payload];
                   let concatenated =
                       arrow::compute::concat_batches(&data.batch.schema(), batches.iter())?;
                   data.batch = concatenated;
                   Ok(WriteOutcome { rows })
               }
           }
       }
   }

   #[async_trait::async_trait]
   impl StorageScan for InMemoryTableStorage {
       async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
           let batch = {
               let tables = self.tables.lock().expect("InMemoryTableStorage lock poisoned");
               let data = tables.get(&request.table_id).ok_or_else(|| {
                   StorageError::TableNotFound { table: request.table_id }
               })?;
               if data.table_object_id != request.table_object_id
                   || data.physical_descriptor_fingerprint
                       != request.physical_descriptor_fingerprint
               {
                   return Err(StorageError::DescriptorEvolved);
               }
               data.batch.clone()
           };
           // Apply projection/runtime-filter using existing scan helpers; see
           // InMemoryStorageScan::scan for the canonical body to mirror.
           // The mutex guard is dropped before scan filtering/projection work.
           // ...
       }
   }
   ```

   `scan` should reuse the existing
   `InMemoryStorageScan::scan` body shape for
   projection / morsel / runtime-filter handling; the
   only difference is that `table_data` is cloned from
   the `HashMap` lookup under lock before the scan
   filtering/projection work starts.

3. **Add `WendelonStorageWrite`:**

   ```rust
   pub struct WendelonStorageWrite {
       client: Arc<wendelon_client::Client>,
       descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptor>,
   }

   impl WendelonStorageWrite {
       pub fn new(
           client: Arc<wendelon_client::Client>,
           descriptors: impl IntoIterator<Item = (TableId, wendelon_client::TableSchemaDescriptor)>,
       ) -> Self {
           Self { client, descriptors: descriptors.into_iter().collect() }
       }
   }

   #[async_trait::async_trait]
   impl StorageWrite for WendelonStorageWrite {
       async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError> {
           let descriptor = self.descriptors
               .get(&request.table_id)
               .ok_or(StorageError::TableNotFound { table: request.table_id })?;

           if descriptor.table_object_id != request.table_object_id
               || descriptor.physical_descriptor_fingerprint
                   != request.physical_descriptor_fingerprint
           {
               return Err(StorageError::DescriptorEvolved);
           }

           let descriptor_schema = descriptor.arrow_schema().map_err(|error| {
               StorageError::Internal {
                   message: format!("descriptor arrow_schema: {error}"),
               }
           })?;
           if request.payload.schema().as_ref() != &descriptor_schema {
               return Err(StorageError::Internal {
                   message: "INSERT payload schema does not match descriptor".to_string(),
               });
           }

           // ADR 030 does not pass a client_hlc from Toron. The wrapper supplies
           // a default; read-your-writes/session-HLC advancement is a future ADR.
           let tenant_id = wendelon::schema::TenantId::new(request.tenant_id.0);
           let receipt = self.client
               .table_write(
                   tenant_id,
                   descriptor,
                   request.payload,
                   wendelon_client::WriteOptions {
                       await_finalization: true,
                       ..Default::default()
                   },
               )
               .await
               .map_err(StorageError::from)?;

           Ok(WriteOutcome { rows: receipt.rows })
       }
   }
   ```

**Verification:**

```sh
cargo check -p toron
```

## Phase 5 — Coordinator routing + materialization

**Goal.** Wire INSERT through `QueryCoordinator` with
materialization helper. Preserve descriptor-evolution
retry semantics for SELECT (ADR 029 D15).

**Files:** `crates/toron/src/coordinator.rs`,
`crates/toron/src/coordinator/write.rs` (NEW),
`crates/toron/src/exec.rs`.

**Tasks:**

1. **In `coordinator.rs`, declare submodule** at the
   top of the file:

   ```rust
   mod write;
   ```

   No directory split required; the submodule is a
   sibling file `coordinator/write.rs`.

2. **Create `crates/toron/src/coordinator/write.rs`:**

   ```rust
   use std::sync::Arc;

   use arrow::array::{ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder};
   use arrow::datatypes::{DataType, SchemaRef};
   use arrow::record_batch::RecordBatch;

   use crate::error::StorageError;
   use crate::ir::ScalarValue;

   pub(super) fn materialize_insert_batch(
       schema: &SchemaRef,
       rows: &[Vec<ScalarValue>],
   ) -> Result<RecordBatch, StorageError> {
       let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
       for (field_index, field) in schema.fields().iter().enumerate() {
           columns.push(build_array(field.data_type(), rows, field_index)?);
       }
       RecordBatch::try_new(schema.clone(), columns)
           .map_err(StorageError::Arrow)
   }

   fn build_array(
       data_type: &DataType,
       rows: &[Vec<ScalarValue>],
       field_index: usize,
   ) -> Result<ArrayRef, StorageError> {
       match data_type {
           DataType::Int32 => {
               let mut builder = Int32Builder::with_capacity(rows.len());
               for row in rows {
                   match &row[field_index] {
                       ScalarValue::Int32(Some(v)) => builder.append_value(*v),
                       ScalarValue::Int32(None) => builder.append_null(),
                       other => return Err(StorageError::Internal {
                           message: format!("materialize_insert_batch type mismatch at field {field_index}: expected Int32, got {other:?}"),
                       }),
                   }
               }
               Ok(Arc::new(builder.finish()))
           }
           DataType::Int64 => { /* analogous */ }
           DataType::Float32 => { /* analogous */ }
           DataType::Float64 => { /* analogous */ }
           DataType::Utf8 => { /* StringBuilder */ }
           DataType::Boolean => { /* BooleanBuilder */ }
           other => Err(StorageError::Internal {
               message: format!("materialize_insert_batch unsupported target type: {other}"),
           }),
       }
   }
   ```

   **Architecture invariant:** no synthetic-column
   construction; the produced batch's schema is
   exactly the bound `schema` (which is the user-
   visible descriptor schema).

3. **Add `Executor::context()` accessor**
   (`exec.rs`):

   ```rust
   impl Executor {
       pub fn context(&self) -> &ExecutionContext {
           &self.context
       }
   }
   ```

4. **`QueryCoordinator` field addition:**

   ```rust
   pub struct QueryCoordinator {
       // ... existing fields ...
       writer: Arc<dyn StorageWrite>,
   }

   struct UnsupportedStorageWrite;

   #[async_trait::async_trait]
   impl StorageWrite for UnsupportedStorageWrite {
       async fn write(&self, _request: WriteRequest) -> Result<WriteOutcome, StorageError> {
           Err(StorageError::Internal {
               message: "INSERT execution requires a configured StorageWrite".to_string(),
           })
       }
   }

   impl QueryCoordinator {
       pub fn new(catalog: Arc<dyn Catalog>, executor: Executor, config: PlannerConfig) -> Self {
           Self::with_writer(catalog, executor, config, Arc::new(UnsupportedStorageWrite))
       }

       pub fn with_writer(
           catalog: Arc<dyn Catalog>,
           executor: Executor,
           config: PlannerConfig,
           writer: Arc<dyn StorageWrite>,
       ) -> Self { /* ... */ }
   }
   ```

5. **`QueryCoordinator::execute_query` intercept
   BEFORE retry loop:**

   ```rust
   pub async fn execute_query(
       &self,
       plan: &LogicalPlan,
       morsel: Option<(MorselId, u32)>,
   ) -> Result<Vec<RecordBatch>, QueryError> {
       if matches!(plan, LogicalPlan::CreateTable { .. }) {
           let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
           return self.execute_catalog_plan(&bound).await;
       }
       if matches!(plan, LogicalPlan::Insert { .. }) {
           let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
           return self.execute_write_plan(&bound).await;
       }

       let mut retried_descriptor_evolution = false;
       loop {
           let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
           // ... existing SELECT path retry logic ...
       }
   }
   ```

6. **Add private `execute_write_plan`:**

   ```rust
   async fn execute_write_plan(
       &self,
       bound: &ExecutionPlan,
   ) -> Result<Vec<RecordBatch>, QueryError> {
       match bound {
           ExecutionPlan::Insert {
               local_table_id,
               table,
               fingerprint,
               schema,
               rows,
           } => {
               let payload = write::materialize_insert_batch(schema, rows)
                   .map_err(QueryError::from)?;
               let context = self.executor.context();
               let request = WriteRequest {
                   session_id: context.session_id,
                   query_id: context.query_id,
                   tenant_id: context.tenant_id,
                   table_id: *local_table_id,
                   table_object_id: *table,
                   physical_descriptor_fingerprint: *fingerprint,
                   payload,
                   profile_hint: context.profile_hint.clone(),
                   trace_context: context.trace_context.clone(),
               };
               self.writer.write(request).await?;
               Ok(Vec::new())
           }
           other => Err(QueryError::Internal {
               message: format!("execute_write_plan called with non-DML plan: {other:?}"),
           }),
       }
   }
   ```

7. **`DistributedQueryCoordinator::execute_query`
   reject** (same file):

   ```rust
   let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
   if let ExecutionPlan::CreateTable { /* ... */ } = &bound { /* existing ADR 029 path */ }
   if matches!(&bound, ExecutionPlan::Insert { .. }) {
       return Err(QueryError::Internal {
           message: "distributed INSERT execution is not wired to a write-context-bearing coordinator".to_string(),
       });
   }
   let stages = fragment_plan(bound, /* ... */)?;
   ```

**Verification:**

```sh
cargo check -p toron
cargo test -p toron --lib coordinator
```

## Phase 6 — Executor + fragmenter guardrails

**Goal.** Defensive rejects mirroring ADR 029 D4
pattern.

**Files:**

- `crates/toron/src/exec.rs`
- `crates/toron/src/distributed/fragment.rs`
- `crates/toron/src/distributed/scheduler.rs`
- `crates/toron/src/server/mod.rs`

**Tasks:**

1. **`Executor::execute` (unbound)** add arm:

   ```rust
   LogicalPlan::Insert { .. } => Err(ExecutionError::NotImplemented {
       feature: "unbound INSERT execution",
   }),
   ```

2. **`Executor::execute_bound`** add arm:

   ```rust
   ExecutionPlan::Insert { .. } => Err(ExecutionError::NotImplemented {
       feature: "INSERT execution is coordinator-only",
   }),
   ```

3. **`FragmentError::InsertNotFragmentable`
   variant:**

   ```rust
   #[derive(thiserror::Error, Debug)]
   pub enum FragmentError {
       // ... existing variants ...
       #[error("CREATE TABLE is a coordinator-only catalog operation")]
       CreateTableNotFragmentable,
       #[error("INSERT is a coordinator-only data-plane operation")]
       InsertNotFragmentable,
   }
   ```

4. **`fragment_plan` top-level reject:**

   ```rust
   if matches!(plan, ExecutionPlan::CreateTable { .. }) {
       return Err(FragmentError::CreateTableNotFragmentable);
   }
   if matches!(plan, ExecutionPlan::Insert { .. }) {
       return Err(FragmentError::InsertNotFragmentable);
   }
   // ... existing logic ...
   ```

5. **Recursive walkers** in fragmenter, scheduler,
   and server — add `Insert` arms per Phase 1 task 6
   (leaf / `None` / `false` / `"insert"`).

**Verification:**

```sh
cargo check -p toron
cargo test -p toron --lib fragment
```

## Phase 7 — Architecture Guards (12 new + 2 adjusted)

**Files:** `crates/toron/tests/architecture_test.rs`.

**Tasks:**

1. **Adjust** `architecture_guards_unchanged_plus_sort_variant`:

   ```rust
   assert_eq!(enum_variant_count(logical_plan), 11);  // was 10
   assert!(logical_plan.contains("Insert {"));
   // ... existing CreateTable check ...

   assert_eq!(enum_variant_count(execution_plan), 9);  // was 8
   assert!(execution_plan.contains("Insert {"));
   ```

2. **Adjust** existing
   `adr029_sql_planner_accepts_only_select_and_create_table`
   — update catch-all diagnostic assertion to include
   `"and INSERT"`; assert all three of
   `Statement::Query`, `Statement::CreateTable`,
   `Statement::Insert` arms; assert absence of the
   pre-ADR-030 string.

3. **Add 12 new ADR 030 guards** per the locked list
   in `plans/adr/0030_insert_into_values.md`
   §"Architecture guard adjustments":

   - `adr030_insert_destructure_is_exhaustive`
     (includes explicit checks for `into` and
     `has_table_keyword`)
   - `adr030_logical_plan_insert_uses_insert_literal`
   - `adr030_execution_plan_insert_has_no_record_batch`
     (pins field shape:
     `Insert {\n\t\tlocal_table_id: TableId,\n\t\ttable: TableObjectId,\n\t\tfingerprint: PhysicalDescriptorFingerprint,\n\t\tschema: SchemaRef,\n\t\trows: Vec<Vec<ScalarValue>>,`)
   - `adr030_catalog_has_no_insert_method`
   - `adr030_no_storage_synthetic_column_construction_in_toron`
     (source-reads `coordinator/write.rs`)
   - `adr030_no_blake3_in_toron_insert_path`
     (source-reads `crates/toron/Cargo.toml` +
     `coordinator/write.rs`)
   - `adr030_coordinator_insert_uses_wendelon_client_wrapper`
   - `adr030_write_options_set_await_finalization_true`
   - `adr030_wendelon_storage_write_mirrors_scan_error_mapping`
   - `adr030_local_coordinator_intercepts_insert_before_retry_loop`
   - `adr030_distributed_coordinator_rejects_insert_before_fragment_plan`
   - `adr030_executor_and_fragmenter_reject_insert`

Total: **12 new + 2 adjusted** architecture guards.

**Verification:**

```sh
cargo test -p toron --test architecture_test
```

## Phase 8 — Behavioral Tests

**Goal.** Comprehensive fixture coverage. The
signature deliverable is the
`CREATE TABLE; INSERT; SELECT` round-trip lifting ADR
029 D14.

**Files:** `crates/toron/tests/insert_test.rs` (NEW).

**Tasks:**

1. **Planner positives:**

   - `INSERT INTO t VALUES (1, 1, 1.0, 1.0, 'a', TRUE)` — six-type lattice.
   - `INSERT INTO t VALUES (1, 'a'), (2, 'b')` — multi-row.
   - `INSERT INTO app.sales VALUES (1, 100)` — schema-qualified target.
   - `INSERT INTO t (b, a) VALUES ('x', 1)` — reordered column list.
   - `INSERT INTO t (a) VALUES (NULL)` — explicit NULL.
   - `INSERT INTO t (a) VALUES (-1)` — negative numeric literal.

2. **Planner negatives** (each with stable diagnostic
   assertion):

   - `INSERT INTO t SELECT 1` — `"INSERT source must be VALUES"`.
   - `INSERT INTO t (a) VALUES (1) RETURNING a` — `"RETURNING"`.
   - `INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING` — `"ON CONFLICT"`.
   - `INSERT OR REPLACE INTO t (a) VALUES (1)` — `"OR"`.
   - `INSERT IGNORE INTO t (a) VALUES (1)` — `"IGNORE"`.
   - `INSERT t (a) VALUES (1)` — `"missing INTO"`.
   - `INSERT INTO TABLE t (a) VALUES (1)` — `"TABLE keyword"`.
   - `REPLACE INTO t (a) VALUES (1)` — `"REPLACE INTO"`.
   - `INSERT OVERWRITE TABLE t (a) VALUES (1)` — `"OVERWRITE"`.
   - `INSERT INTO t (a) VALUES ()` — empty values list.
   - `INSERT INTO t (a) VALUES (1 + 2)` — `"must be literals"`.
   - `INSERT INTO t (a) VALUES (now())` — `"must be literals"`.
   - `INSERT INTO catalog.public.t (a) VALUES (1)` — `"only one- or two-part"`.

3. **Binder positives:**

   - Column reorder produces schema-order scalar
     rows.
   - Omitted nullable column filled with typed NULL.
   - All six ADR 029 target types coerce from
     supported literals.

4. **Binder negatives:**

   - Duplicate target column.
   - Unknown target column.
   - Row arity mismatch (include row ordinal in
     diagnostic).
   - Omitted NOT NULL column.
   - NULL into NOT NULL column.
   - INT overflow: `INSERT INTO t (a) VALUES (99999999999)` for `a INT`.
   - Float into Utf8 column.
   - Row cap exceeded (programmatically construct
     >65536 rows).

5. **Storage tests:**

   - `InMemoryTableStorage` seeds on first write.
   - Subsequent writes append.
   - Schema mismatch rejected (`StorageError::Internal`).
   - `table_object_id` mismatch rejected
     (`StorageError::DescriptorEvolved`).
   - `physical_descriptor_fingerprint` mismatch
     rejected (`StorageError::DescriptorEvolved`).
   - Scan with no prior write returns
     `StorageError::TableNotFound`.

6. **Coordinator tests:**

   - `QueryCoordinator::with_writer(...).execute_query(INSERT)`
     → `Ok(Vec::new())` + subsequent scan returns
     inserted rows.
   - `QueryCoordinator::new(...).execute_query(INSERT)`
     (default unsupported writer) → stable diagnostic.
   - `DistributedQueryCoordinator::execute_query(INSERT)`
     → stable unsupported diagnostic + no fragmenter
     dispatch (use `FailingTransport` from ADR 029
     `create_table_test.rs`).

7. **End-to-end signature fixture** (ADR 030's
   defining test):

   ```rust
   #[tokio::test]
   async fn create_table_insert_select_round_trips() {
       let catalog = Arc::new(MockCatalog::new());
       let storage = Arc::new(InMemoryTableStorage::new());
       let executor = Executor::new(storage.clone());
       let coordinator = QueryCoordinator::with_writer(
           catalog.clone(), executor, PlannerConfig::default(), storage.clone(),
       );
       let planner = SqlPlanner::new();

       coordinator.execute_query(
           &planner.plan("CREATE TABLE t (id INT, name TEXT)").unwrap(), None,
       ).await.unwrap();

       coordinator.execute_query(
           &planner.plan("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap(), None,
       ).await.unwrap();

       let results = coordinator.execute_query(
           &planner.plan("SELECT id, name FROM t ORDER BY id").unwrap(), None,
       ).await.unwrap();

       let schema = results[0].schema();
       let batch = arrow::compute::concat_batches(&schema, &results).unwrap();
       assert_eq!(batch.num_rows(), 2);
       // ... assert column values ...
   }
   ```

8. **Defensive tests:**

   - `Executor::execute_bound(&ExecutionPlan::Insert { ... })`
     → coordinator-only `NotImplemented` error.
   - `fragment_plan(ExecutionPlan::Insert { ... }, ...)`
     → `FragmentError::InsertNotFragmentable`.

9. **Catch-all preservation:** existing ADR 029
   `planner_preserves_non_create_table_statement_rejects`
   fixture should be extended (or paralleled) to
   assert that `UPDATE`, `DELETE`, `ALTER`, `DROP`,
   `CREATE INDEX`, `CREATE VIEW` continue to reject
   with the widened diagnostic
   `"only SELECT, CREATE TABLE, and INSERT are supported"`.

**Verification:**

```sh
cargo test -p toron --test insert_test
```

## Phase 9 — Final Verification

```sh
cargo fmt --check
cargo build -p toron --tests
cargo test -p toron --test architecture_test
cargo test -p toron --test create_table_test
cargo test -p toron --test insert_test
cargo test -p toron
```

If formatting diff: `cargo fmt && cargo fmt --check`.

Expected: all green; ADR 029 fixtures + guards still
pass; existing SELECT corpus plan digests byte-
identical.

## Implementation risks

| ID | Risk | Mitigation |
|---|---|---|
| R1 | INSERT slips into SELECT retry loop | Phase 5 intercept BEFORE retry loop; ADR 029 `adr029_select_retry_loop_is_preserved` carries forward. |
| R2 | Toron-side construction of `__row_*__` columns | Phase 0 verify no `blake3` dep; `coordinator/write.rs` references no `STORAGE_ROW_*` constants; Phase 7 architecture guards. |
| R3 | Wrong Wendelon API call (low-level `WendelonClient::table_write` vs wrapper) | Phase 0 explicit verification of `Cargo.toml` dep; Phase 7 guard asserts no `TableWriteRequest` construction in toron source. |
| R4 | `RecordBatch` in `ExecutionPlan::Insert` | Phase 7 guard `adr030_execution_plan_insert_has_no_record_batch` source-checks variant body. |
| R5 | `await_finalization: false` causes read-after-write race | Phase 4 `WendelonStorageWrite::write` explicitly overrides; Phase 7 guard pins the literal `await_finalization: true`. |
| R6 | Distributed INSERT dispatches to fragmenter | Phase 5 distributed coordinator rejects before `fragment_plan`; Phase 6 top-level fragmenter reject. |
| R7 | `Catalog` grows insert/write method during refactor | Phase 7 guard `adr030_catalog_has_no_insert_method`. |
| R8 | sqlparser dependency upgrade silently accepts new Insert fields | Phase 2 exhaustive destructure without `..`; Phase 7 guard. |
| R9 | Coordinator constructor churn breaks server wiring | Phase 5 default `UnsupportedStorageWrite` keeps existing callers compiling; only INSERT-aware tests use `with_writer`. |
| R10 | `coordinator.rs` becomes a single huge file | Phase 5 splits materialization into `coordinator/write.rs` — `coordinator.rs` declares `mod write;` without directory restructure. |
| R11 | Plan-digest regression for SELECT corpus | `LogicalPlan` / `ExecutionPlan` additive variants. |
| R12 | `InMemoryTableStorage` lock contention surprises tests | Single `Mutex` over `HashMap`; tests sequential. Document; revisit if fixtures get parallelism. |
| R13 | Integer overflow on `INT` target accepted silently | Phase 3 `coerce_insert_literal` uses `i64.try_into::<i32>()`; overflow rejects with stable diagnostic + column name. |
| R14 | Float NaN/inf inconsistency vs Cast lattice | Mirror Arrow native semantics in v1 (no finite-only check); future ADR may add. |
| R15 | Architecture guard count growth | 12 new + 2 adjusted; substrate-touching but narrower than ADR 029's 15 + 1. |
| R16 | Row cap divergence between in-memory and Wendelon backends | Phase 3 bind-time enforcement of `TABLE_WRITE_MAX_ROWS = 65_536`; matches Wendelon constant. |
| R17 | `InMemoryStorageScan` callers broken by storage refactor | Phase 4 additive — `InMemoryStorageScan` untouched. |
| R18 | `wendelon_storage_execution_test.rs::assert_scan_has_no_pbc` not updated | Phase 1 task 6 includes the `Insert` arm; Phase 0 baseline audit catches missing arms via compile errors. |

## Definition of Done

Per ADR 030 §"Definition of Done" — full checklist
applies. Critical implementation gates summary:

- [ ] `LogicalPlan::Insert` + `InsertLiteral` enum +
      `ExecutionPlan::Insert` leaf variants;
      `schema()` returns empty.
- [ ] `StorageWrite` trait + `WriteRequest` +
      `WriteOutcome { rows: usize }` (`#[non_exhaustive]`).
- [ ] `InMemoryTableStorage` implements both
      `StorageScan` and `StorageWrite`; multi-table.
- [ ] `WendelonStorageWrite` uses
      `wendelon_client::Client::table_write` with
      `WriteOptions { await_finalization: true,
      ..Default::default() }`; error mapping mirrors
      `WendelonStorageScan`.
- [ ] No `TableWriteRequest` construction in
      `crates/toron/src/`.
- [ ] No `blake3` declaration in
      `crates/toron/Cargo.toml`.
- [ ] No `STORAGE_ROW_*` references in
      `coordinator/write.rs`.
- [ ] `Statement::Insert` arm + `lower_insert`
      exhaustive 19-field destructure + DML-specific
      helpers (no SELECT helper delegation).
- [ ] Binder `bind_insert` with column resolution,
      typed-NULL fill, row-cap enforcement
      (`TABLE_WRITE_MAX_ROWS = 65_536`), target-type
      coercion.
- [ ] `QueryCoordinator` intercepts INSERT BEFORE
      retry loop; returns `Ok(Vec::new())` on success.
- [ ] `DistributedQueryCoordinator` returns stable
      unsupported diagnostic before `fragment_plan`.
- [ ] `coordinator/write.rs::materialize_insert_batch`
      private helper; user-columns only.
- [ ] `Executor::context()` accessor added.
- [ ] `Executor` defensive arms (unbound + bound).
- [ ] `FragmentError::InsertNotFragmentable` +
      `fragment_plan` top-level reject.
- [ ] Walker arms updated in `binder.rs`,
      `fragment.rs`, `scheduler.rs`,
      `server/mod.rs`, and test helpers.
- [ ] **12 new** ADR 030 architecture guards pass.
- [ ] **2 adjusted** ADR 029 guards still pass
      (catch-all + variant counts).
- [ ] All ADR 029 retained architecture guards still
      pass.
- [ ] Existing SELECT corpus plan digests
      byte-identical.
- [ ] Planner + binder + storage + coordinator +
      defensive fixtures.
- [ ] **End-to-end `CREATE TABLE; INSERT; SELECT`
      round-trip fixture** — ADR 030's signature
      deliverable.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.

## Verification commands

```sh
# Development cadence
cargo check -p toron

# Pre-PR checks
cargo fmt --check
cargo test -p toron --test architecture_test
cargo test -p toron --test create_table_test
cargo test -p toron --test insert_test
cargo test -p toron

# Optional when workspace deps change
cargo build -p toron
```
