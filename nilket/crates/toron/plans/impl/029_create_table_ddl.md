# Implementation Plan: ADR 029 — SQL `CREATE TABLE`

**Companion ADR:** `plans/adr/029_create_table_ddl.md`

## Goal

Add SQL:

```sql
CREATE TABLE [IF NOT EXISTS] [schema.]table_name (
    column_name TYPE [NULL | NOT NULL],
    ...
)
```

Route through planner → binder → coordinator → catalog.
DDL never reaches data executor or fragmenter (defensive
rejects only). Catalog mutation succeeds via
`MockCatalog`; `WendelonCatalog::create_table` returns
`AdminUnavailable` until a cross-ring metadata-write
ADR engages.

Preserve:

- Existing SELECT query corpus plan digests byte-
  identical (D15).
- `QueryCoordinator`'s descriptor-evolution retry loop
  semantics for SELECT (R1).
- All cross-ADR invariants from ADR 022 / 023 / 024 /
  025 / 026 / 027 / 028.

No new IR expression variant; no new aggregate kernel;
no fragmenter detector change for SELECT paths; no PBC
opcode change; no cross-ring engagement; no Wendelon
persistent descriptor writes.

Nine-phase rollout: baseline → IR shape → catalog
contract + errors + identity helper → SQL planner →
binder → coordinator execution → executor/fragmenter
guardrails → architecture guards → behavioral tests →
final verification.

## Touchpoint sweep (code-grounded, current `main`)

| File | Anchor | Phase |
|---|---|---|
| `crates/toron/src/ir.rs` | `LogicalPlan::CreateTable { name, schema, if_not_exists }` leaf variant | 1 |
| `crates/toron/src/ir/execution.rs` | `ExecutionPlan::CreateTable { ... }` twin variant + `schema()` returns `Arc::new(Schema::empty())` | 1 |
| `crates/toron/src/error.rs` | `CatalogError::TableAlreadyExists` / `SchemaMismatch` / `AdminUnavailable`; reuse existing `FingerprintResolution` | 2 |
| `crates/toron/src/catalog.rs` | `Catalog::create_table` async trait method; `MockCatalog` → `RwLock<MockCatalogInner>` refactor; `mock_table_identity` helper; `WendelonCatalog::create_table` → `AdminUnavailable` | 2 |
| `crates/toron/src/sql_planner.rs::plan` (~line 121) | `Statement::CreateTable` arm; widened catch-all diagnostic | 3 |
| `crates/toron/src/sql_planner.rs::lower_create_table` | NEW; exhaustive `sqlparser::ast::CreateTable` destructure without `..` | 3 |
| `crates/toron/src/sql_planner.rs::lower_create_table_name` | NEW; 1- or 2-part names only | 3 |
| `crates/toron/src/sql_planner.rs::lower_create_table_data_type` | NEW; DDL-specific 6-type lattice | 3 |
| `crates/toron/src/sql_planner.rs::lower_create_table_column` | NEW; `Field` construction + storage-synthetic-name reject (D12) | 3 |
| `crates/toron/src/sql_planner.rs::lower_create_table_column_nullability` | NEW; NULL/NOT NULL only | 3 |
| `crates/toron/src/binder.rs::bind_plan` | New `LogicalPlan::CreateTable` arm — straight pass-through | 4 |
| `crates/toron/src/coordinator.rs::QueryCoordinator::execute_query` | DDL interception BEFORE descriptor-evolution retry loop | 5 |
| `crates/toron/src/coordinator.rs::DistributedQueryCoordinator::execute_query` | DDL interception AFTER binding, BEFORE `fragment_plan` (**single file** — both coordinators here) | 5 |
| `crates/toron/src/exec.rs::Executor::execute` and `execute_bound` | Defensive `ExecutionPlan::CreateTable` arms returning `NotImplemented` | 6 |
| `crates/toron/src/distributed/fragment.rs` | `FragmentError::CreateTableNotFragmentable` + `fragment_plan` top-level reject | 6 |
| `crates/toron/src/distributed/fragment.rs` (recursive walkers) | `contains_join`, `count_exchanges`, `find_exchange`, etc. — `CreateTable` arms as leaf / `None` | 6 |
| `crates/toron/tests/architecture_test.rs` | 15 NEW ADR 029 guards + 1 adjusted statement-catch-all guard | 7 |
| `crates/toron/tests/create_table_test.rs` (NEW) | Positive + negative + coordinator + catalog + guardrail fixtures | 8 |

## Phase 0 — Baseline Audit

Before edits, confirm starting state:

```sh
rg -n "pub fn plan|unsupported SQL statement|Statement::Query" crates/toron/src/sql_planner.rs
rg -n "pub trait Catalog|struct MockCatalog|struct WendelonCatalog|register_descriptor|register_table" crates/toron/src/catalog.rs
rg -n "pub enum LogicalPlan|pub enum ExecutionPlan|fn schema" crates/toron/src/ir.rs crates/toron/src/ir/execution.rs
rg -n "pub async fn bind|fn bind_plan|terminal_exchange" crates/toron/src/binder.rs
rg -n "execute_query|execute_bound|fragment_plan|FragmentError" crates/toron/src/coordinator.rs crates/toron/src/exec.rs crates/toron/src/distributed/fragment.rs
rg -n "table_ref_from_object_name|lower_cast_target_type|lower_data_type" crates/toron/src/sql_planner.rs
rg -n "table_name|TableId|TableObjectId|PhysicalDescriptorFingerprint|TableSchemaDescriptor|RowIdentityPolicy|LocalityPolicy|RecordLayoutPolicy" crates/toron/src/catalog.rs
rg -n "STORAGE_ROW_HLC_COLUMN|STORAGE_ROW_KEY_ID_COLUMN|STORAGE_ROW_ROUTING_LOCALITY_COLUMN|STORAGE_ROW_TABLE_OBJECT_ID_COLUMN|STORAGE_ROW_TOMBSTONE_COLUMN" crates/wendelon/src
```

Verify three **load-bearing baseline hazards**:

1. `QueryCoordinator::execute_query` binds **inside**
   the descriptor-evolution retry loop. DDL must
   branch **before** the loop so SELECT retry
   semantics stay byte-identical (R1).
2. `MockCatalog` stores plain `HashMap`s. The async
   `&self` trait method requires `RwLock` or
   equivalent interior mutability.
3. `PhysicalDescriptorFingerprint::from_bytes` is
   `pub(crate)` inside Wendelon. Toron cannot
   fabricate fingerprints directly; the only public
   path is via `TableSchemaDescriptor::new_with_layout`.

Also verify:

- `TableId` is `pub struct TableId(pub u64)` — tuple
  struct, no `new` constructor.
- `TableRef` does NOT derive `Display`. Existing
  crate-private `table_name(&TableRef) -> String`
  helper is the diagnostic formatter.
- Both `QueryCoordinator` and
  `DistributedQueryCoordinator` live in
  `crates/toron/src/coordinator.rs` (single file).

**Preserve verbatim:**

- All existing `Catalog` trait read methods + their
  semantics.
- All existing `LogicalPlan` / `ExecutionPlan` variants
  + their walkers (DDL is additive).
- `Executor::execute` / `execute_bound` SELECT paths.
- `fragment_plan` SELECT paths.
- `QueryCoordinator`'s descriptor-evolution retry
  semantics.
- All cross-ADR contracts (022 / 023 / 024 / 025 / 026
  / 027 / 028).

## Phase 1 — IR Shape

**Goal.** Add `LogicalPlan::CreateTable` +
`ExecutionPlan::CreateTable` leaf variants.

**Files:**

- `crates/toron/src/ir.rs`
- `crates/toron/src/ir/execution.rs`

**Tasks:**

1. **Add `LogicalPlan::CreateTable`:**

   ```rust
   LogicalPlan::CreateTable {
       name: TableRef,
       schema: arrow::datatypes::SchemaRef,
       if_not_exists: bool,
   }
   ```

   Leaf — no `Box<LogicalPlan>` input.

2. **Add `ExecutionPlan::CreateTable`** (twin shape):

   ```rust
   ExecutionPlan::CreateTable {
       name: TableRef,
       schema: arrow::datatypes::SchemaRef,
       if_not_exists: bool,
   }
   ```

3. **`ExecutionPlan::schema()` arm:**

   ```rust
   ExecutionPlan::CreateTable { .. } => Arc::new(Schema::empty()),
   ```

   Empty schema is metadata for the control-plane
   result; NOT a promise of empty-batch row output.

4. **Update exhaustive `LogicalPlan` walkers**
   throughout `sql_planner.rs` (`plan_qualifiers` and
   any other walkers). For `CreateTable`, the natural
   arm is "leaf — no recursion / no metadata".

5. **Update exhaustive `ExecutionPlan` walkers** in
   `binder.rs`, `fragment.rs`, `scheduler.rs` (if any),
   server helpers, and tests:

   - `contains_join` → `false`
   - `contains_broadcast_join` → `false`
   - `count_exchanges` → `0`
   - `find_exchange` → `None`
   - `first_scan_metadata` / `first_scan_fingerprint` → `None`
   - `join_strategy_label` → `None`

**Verification:**

```sh
cargo check -p toron
```

Expected: compile errors at every existing exhaustive
match site. Fix them as leaf / `None` arms.

## Phase 2 — Catalog Mutation Contract

**Goal.** Extend `Catalog` trait; refactor `MockCatalog`
to interior-mutable shape; implement `mock_table_identity`
helper for deterministic-real fingerprint; implement
`WendelonCatalog::create_table` as admin-unavailable.

**Files:**

- `crates/toron/src/error.rs`
- `crates/toron/src/catalog.rs`

**Tasks:**

1. **Add `CatalogError` variants:**

   ```rust
   #[derive(thiserror::Error, Debug)]
   pub enum CatalogError {
       // ... existing variants ...

       #[error("table already exists: {table}")]
       TableAlreadyExists { table: String },

       #[error("existing table schema does not match CREATE TABLE IF NOT EXISTS: {table}")]
       SchemaMismatch { table: String },

       #[error("catalog admin operation unavailable: {message}")]
       AdminUnavailable { message: String },
   }
   ```

   `CatalogError::FingerprintResolution` already exists
   and is reused for mock identity descriptor construction
   failures. Do not add a duplicate variant.

2. **Extend `Catalog` trait:**

   ```rust
   #[async_trait::async_trait]
   pub trait Catalog: Send + Sync {
       // ... existing read methods (unchanged) ...

       async fn create_table(
           &self,
           name: &TableRef,
           schema: SchemaRef,
           if_not_exists: bool,
       ) -> Result<TableBinding, CatalogError>;
   }
   ```

3. **Refactor `MockCatalog` to interior-mutable shape:**

   ```rust
   #[derive(Default)]
   pub struct MockCatalog {
       inner: std::sync::RwLock<MockCatalogInner>,
   }

   struct MockCatalogInner {
       by_ref: HashMap<TableRef, TableId>,
       by_id: HashMap<TableId, TableBinding>,
       next_table_id: u64,
   }

   impl Default for MockCatalogInner {
       fn default() -> Self {
           Self {
               by_ref: HashMap::new(),
               by_id: HashMap::new(),
               next_table_id: 1,
           }
       }
   }
   ```

   - `resolve_table` / `resolve_table_id` take read
     lock via `self.inner.read().unwrap()`.
   - `register_descriptor(&mut self, ...)` /
     `register_table(&mut self, ...)` setup helpers
     stay `&mut self` and use `self.inner.get_mut()`
     to avoid lock overhead in test setup paths. Both
     helpers bump `next_table_id` past the registered
     id to keep `create_table`'s allocator monotone.
   - `create_table(&self, ...)` takes write lock.

4. **Implement `mock_table_identity` helper (D7):**

   ```rust
   /// Produce a deterministic-yet-real `TableObjectId` +
   /// `PhysicalDescriptorFingerprint` pair for the mock catalog.
   ///
   /// `PhysicalDescriptorFingerprint::from_bytes` is crate-private
   /// to Wendelon, so the only public path is via descriptor
   /// construction. Determinism is provided by stamping
   /// `local_table_id.0` into the `TableObjectId` payload and
   /// using fixed policy choices.
   fn mock_table_identity(
       name: &TableRef,
       schema: &Schema,
       local_table_id: TableId,
   ) -> Result<(TableObjectId, PhysicalDescriptorFingerprint), CatalogError> {
       let mut object_id = [0_u8; 16];
       object_id[8..].copy_from_slice(&local_table_id.0.to_be_bytes());
       let table_object_id = TableObjectId::new(object_id);

       let descriptor = TableSchemaDescriptor::new_with_layout(
           table_object_id,
           1, // schema_generation
           name.schema.as_deref().unwrap_or("default"),
           &name.table,
           schema,
           RowIdentityPolicy::AppendOnly,
           LocalityPolicy::default_table_spread(),
           RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
           SchemaEvolutionPolicyV1::AdditiveOnly,
       )
       .map_err(|error| CatalogError::FingerprintResolution {
           message: error.to_string(),
       })?;

       Ok((table_object_id, descriptor.physical_descriptor_fingerprint))
   }
   ```

   **Critical:** uses the public
   `TableSchemaDescriptor::new_with_layout` API, NOT
   the crate-private
   `PhysicalDescriptorFingerprint::from_bytes`. No
   Wendelon persistence implied; this is catalog
   identity bookkeeping for test paths.

5. **Implement `MockCatalog::create_table` (D10
   alias-no-overwrite):**

   ```rust
   async fn create_table(
       &self,
       name: &TableRef,
       schema: SchemaRef,
       if_not_exists: bool,
   ) -> Result<TableBinding, CatalogError> {
       let mut inner = self.inner.write().expect("MockCatalog lock poisoned");

       if let Some(existing_id) = inner.by_ref.get(name).cloned() {
           let existing = inner
               .by_id
               .get(&existing_id)
               .expect("MockCatalog by_ref/by_id consistency invariant")
               .clone();
           if if_not_exists {
               if existing.schema.as_ref() == schema.as_ref() {
                   return Ok(existing);
               }
               return Err(CatalogError::SchemaMismatch {
                   table: table_name(name),
               });
           }
           return Err(CatalogError::TableAlreadyExists {
               table: table_name(name),
           });
       }

       // Allocate deterministic test-only TableId; skip ids that
       // collide with previously-registered descriptors so the
       // by_id invariant is preserved.
       while inner.by_id.contains_key(&TableId(inner.next_table_id)) {
           inner.next_table_id = inner
               .next_table_id
               .checked_add(1)
               .expect("TableId overflow");
       }
       let local_table_id = TableId(inner.next_table_id);
       inner.next_table_id = inner
           .next_table_id
           .checked_add(1)
           .expect("TableId overflow");

       let (table_object_id, fingerprint) =
           mock_table_identity(name, schema.as_ref(), local_table_id)?;
       let binding = TableBinding::new(
           local_table_id,
           table_object_id,
           fingerprint,
           schema.clone(),
       );

       // D10: exact ref always; unqualified alias only if vacant.
       inner.by_ref.insert(name.clone(), local_table_id);
       if name.schema.is_some() {
           inner
               .by_ref
               .entry(TableRef::new(name.table.clone()))
               .or_insert(local_table_id);
       }
       inner.by_id.insert(local_table_id, binding.clone());

       Ok(binding)
   }
   ```

   Three iter-3 precision items visible:

   - `TableId(inner.next_table_id)` — tuple-struct
     construction, NOT `TableId::new(...)`.
   - `table_name(name)` — diagnostic formatter helper,
     NOT `name.to_string()`.
   - `entry(...).or_insert(...)` — alias path never
     overwrites an existing entry.

6. **Implement `WendelonCatalog::create_table`:**

   ```rust
   async fn create_table(
       &self,
       _name: &TableRef,
       _schema: SchemaRef,
       _if_not_exists: bool,
   ) -> Result<TableBinding, CatalogError> {
       Err(CatalogError::AdminUnavailable {
           message:
               "CREATE TABLE is not wired to Wendelon catalog administration"
                   .to_string(),
       })
   }
   ```

   Cross-ring metadata-write contract deferred to a
   future ADR. Do NOT fake persistent descriptor writes.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron --lib catalog
```

## Phase 3 — SQL Planner

**Goal.** Add `Statement::CreateTable` arm + DDL-
specific helpers + storage-synthetic-name reject (D12).

**Files:** `crates/toron/src/sql_planner.rs`

**Tasks:**

1. **Update `SqlPlanner::plan`:**

   ```rust
   match &ast[0] {
       Statement::Query(query) => self.lower_query(query),
       Statement::CreateTable(create_table) => {
           self.lower_create_table(create_table)
       }
       _ => Err(unsupported(
           "unsupported SQL statement; only SELECT and CREATE TABLE are supported",
       )),
   }
   ```

   Narrows the existing catch-all diagnostic by one
   intentional edit.

2. **Implement `lower_create_table` with exhaustive
   destructure (no `..`):**

   ```rust
   fn lower_create_table(
       &self,
       create_table: &sqlparser::ast::CreateTable,
   ) -> Result<LogicalPlan, FrontendError> {
       // Destructure every field — compile-time guard against
       // sqlparser dependency upgrades introducing new clauses.
       let sqlparser::ast::CreateTable {
           or_replace,
           temporary,
           external,
           global,
           if_not_exists,
           transient,
           volatile,
           name,
           columns,
           constraints,
           hive_distribution,
           hive_formats,
           table_properties,
           with_options,
           file_format,
           location,
           query,
           without_rowid,
           like,
           clone,
           engine,
           comment,
           auto_increment_offset,
           default_charset,
           collation,
           on_commit,
           on_cluster,
           primary_key,
           order_by,
           partition_by,
           cluster_by,
           clustered_by,
           options,
           strict,
           copy_grants,
           enable_schema_evolution,
           change_tracking,
           data_retention_time_in_days,
           max_data_extension_time_in_days,
           default_ddl_collation,
           with_aggregation_policy,
           with_row_access_policy,
           with_tags,
       } = create_table;

       // Reject every unsupported clause with specific diagnostics
       if *or_replace { return Err(unsupported("unsupported CREATE TABLE clause: OR REPLACE")); }
       if *temporary { return Err(unsupported("unsupported CREATE TABLE clause: TEMPORARY")); }
       if *external { return Err(unsupported("unsupported CREATE TABLE clause: EXTERNAL")); }
       if global.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: GLOBAL/LOCAL")); }
       if *transient { return Err(unsupported("unsupported CREATE TABLE clause: TRANSIENT")); }
       if *volatile { return Err(unsupported("unsupported CREATE TABLE clause: VOLATILE")); }
       if !constraints.is_empty() { return Err(unsupported("unsupported CREATE TABLE clause: table-level constraints")); }
       if hive_formats.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: HIVE FORMATS")); }
       if !table_properties.is_empty() { return Err(unsupported("unsupported CREATE TABLE clause: TBLPROPERTIES")); }
       if !with_options.is_empty() { return Err(unsupported("unsupported CREATE TABLE clause: WITH options")); }
       if file_format.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: STORED AS")); }
       if location.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: LOCATION")); }
       if query.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: AS SELECT (CTAS)")); }
       if *without_rowid { return Err(unsupported("unsupported CREATE TABLE clause: WITHOUT ROWID")); }
       if like.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: LIKE")); }
       if clone.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: CLONE")); }
       if engine.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: ENGINE")); }
       if comment.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: COMMENT")); }
       if auto_increment_offset.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: AUTO_INCREMENT")); }
       if default_charset.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: DEFAULT CHARSET")); }
       if collation.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: COLLATE")); }
       if on_commit.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: ON COMMIT")); }
       if on_cluster.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: ON CLUSTER")); }
       if primary_key.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: table-level PRIMARY KEY")); }
       if order_by.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: ORDER BY")); }
       if partition_by.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: PARTITION BY")); }
       if cluster_by.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: CLUSTER BY")); }
       if clustered_by.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: CLUSTERED BY")); }
       if options.as_ref().is_some_and(|options| !options.is_empty()) {
           return Err(unsupported("unsupported CREATE TABLE clause: OPTIONS"));
       }
       if *strict { return Err(unsupported("unsupported CREATE TABLE clause: STRICT")); }
       if *copy_grants { return Err(unsupported("unsupported CREATE TABLE clause: COPY GRANTS")); }
       if enable_schema_evolution.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: ENABLE SCHEMA EVOLUTION")); }
       if change_tracking.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: CHANGE TRACKING")); }
       if data_retention_time_in_days.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: DATA RETENTION")); }
       if max_data_extension_time_in_days.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: MAX DATA EXTENSION")); }
       if default_ddl_collation.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: DEFAULT DDL COLLATION")); }
       if with_aggregation_policy.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: AGGREGATION POLICY")); }
       if with_row_access_policy.is_some() { return Err(unsupported("unsupported CREATE TABLE clause: ROW ACCESS POLICY")); }
       if with_tags.as_ref().is_some_and(|tags| !tags.is_empty()) {
           return Err(unsupported("unsupported CREATE TABLE clause: WITH TAGS"));
       }

       if !matches!(hive_distribution, sqlparser::ast::HiveDistributionStyle::NONE) {
           return Err(unsupported("unsupported CREATE TABLE clause: Hive distribution"));
       }

       if columns.is_empty() {
           return Err(unsupported("CREATE TABLE requires at least one column"));
       }

       let table_ref = lower_create_table_name(name)?;
       let fields = columns.iter()
           .map(lower_create_table_column)
           .collect::<Result<Vec<_>, _>>()?;
       check_unique_column_names(&fields)?;

       let schema = Arc::new(arrow::datatypes::Schema::new(fields));

       Ok(LogicalPlan::CreateTable {
           name: table_ref,
           schema,
           if_not_exists: *if_not_exists,
       })
   }
   ```

   Note: actual `sqlparser::ast::CreateTable` field set
   is verified at Phase 0 baseline; the destructure
   list adjusts to the dependency-version field set
   but the pattern (no `..`) stays.

3. **Implement `lower_create_table_name`:**

   ```rust
   fn lower_create_table_name(
       name: &ObjectName,
   ) -> Result<TableRef, FrontendError> {
       match name.0.as_slice() {
           [table] => Ok(TableRef::new(table.value.clone())),
           [schema, table] => Ok(TableRef::with_schema(
               schema.value.clone(),
               table.value.clone(),
           )),
           _ => Err(unsupported(
               "CREATE TABLE supports only one- or two-part table names",
           )),
       }
   }
   ```

   **Critical:** does NOT call `table_ref_from_object_name`.

4. **Implement `lower_create_table_column` with
   storage-synthetic-name reject (D12):**

   ```rust
   /// Wendelon storage synthetic column names — these collide
   /// with descriptor construction in `mock_table_identity`
   /// (or shadow legitimate storage metadata on the production
   /// path). Reject at planner time with a stable diagnostic.
   use wendelon::schema::{
       STORAGE_ROW_HLC_COLUMN,
       STORAGE_ROW_KEY_ID_COLUMN,
       STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
       STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
       STORAGE_ROW_TOMBSTONE_COLUMN,
   };

   const STORAGE_SYNTHETIC_COLUMN_NAMES: &[&str] = &[
       STORAGE_ROW_HLC_COLUMN,
       STORAGE_ROW_KEY_ID_COLUMN,
       STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
       STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
       STORAGE_ROW_TOMBSTONE_COLUMN,
   ];

   fn lower_create_table_column(
       column: &ColumnDef,
   ) -> Result<arrow::datatypes::Field, FrontendError> {
       let column_name = &column.name.value;

       if matches!(&column.data_type, sqlparser::ast::DataType::Unspecified) {
           return Err(unsupported("CREATE TABLE column type is required"));
       }

       // D12: storage synthetic name reject.
       if STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&column_name.as_str()) {
           return Err(unsupported(format!(
               "CREATE TABLE column name is reserved for storage metadata: {column_name}"
           )));
       }

       if column.collation.is_some() {
           return Err(unsupported(
               "unsupported CREATE TABLE column option: COLLATE",
           ));
       }

       let data_type = lower_create_table_data_type(&column.data_type)?;
       let nullable = lower_create_table_column_nullability(&column.options)?;

       Ok(arrow::datatypes::Field::new(
           column_name.clone(),
           data_type,
           nullable,
       ))
   }
   ```

5. **Implement `lower_create_table_column_nullability`:**

   ```rust
   fn lower_create_table_column_nullability(
       options: &[ColumnOptionDef],
   ) -> Result<bool, FrontendError> {
       let mut nullable: Option<bool> = None;
       for option_def in options {
           if option_def.name.is_some() {
               return Err(unsupported(
                   "unsupported CREATE TABLE column option: named constraint",
               ));
           }
           let new_nullable = match &option_def.option {
               ColumnOption::Null => true,
               ColumnOption::NotNull => false,
               ColumnOption::Default(_) => {
                   return Err(unsupported("unsupported CREATE TABLE column option: DEFAULT"));
               }
               ColumnOption::Unique { .. } => {
                   return Err(unsupported("unsupported CREATE TABLE column option: UNIQUE / PRIMARY KEY"));
               }
               ColumnOption::ForeignKey { .. } => {
                   return Err(unsupported("unsupported CREATE TABLE column option: REFERENCES"));
               }
               ColumnOption::Check(_) => {
                   return Err(unsupported("unsupported CREATE TABLE column option: CHECK"));
               }
               ColumnOption::Generated { .. } => {
                   return Err(unsupported("unsupported CREATE TABLE column option: GENERATED"));
               }
               ColumnOption::Materialized(_)
               | ColumnOption::Ephemeral(_)
               | ColumnOption::Alias(_)
               | ColumnOption::DialectSpecific(_)
               | ColumnOption::CharacterSet(_)
               | ColumnOption::Comment(_)
               | ColumnOption::OnUpdate(_)
               | ColumnOption::Options(_)
               | ColumnOption::Identity(_)
               | ColumnOption::OnConflict(_)
               | ColumnOption::Policy(_)
               | ColumnOption::Tags(_) => {
                   return Err(unsupported(
                       "unsupported CREATE TABLE column option",
                   ));
               }
           };
           if nullable.is_some() {
               return Err(unsupported(
                   "CREATE TABLE column nullability is specified more than once",
               ));
           }
           nullable = Some(new_nullable);
       }
       Ok(nullable.unwrap_or(true))  // default nullable per SQL convention
   }
   ```

6. **Implement `lower_create_table_data_type`:**

   ```rust
   fn lower_create_table_data_type(
       data_type: &sqlparser::ast::DataType,
   ) -> Result<arrow::datatypes::DataType, FrontendError> {
       use arrow::datatypes::DataType as AT;
       use sqlparser::ast::DataType as SDT;

       match data_type {
           SDT::Int(_) | SDT::Integer(_) | SDT::Int4(_) | SDT::Int32 => Ok(AT::Int32),
           SDT::BigInt(_) | SDT::Int8(_) | SDT::Int64 => Ok(AT::Int64),

           SDT::Real | SDT::Float4 | SDT::Float32 => Ok(AT::Float32),
           SDT::Double(_) | SDT::DoublePrecision | SDT::Float8 | SDT::Float64
               => Ok(AT::Float64),

           SDT::Text
           | SDT::String(_)
           | SDT::Varchar(_)
           | SDT::Char(_)
           | SDT::Character(_)
           | SDT::CharVarying(_)
           | SDT::CharacterVarying(_)
               => Ok(AT::Utf8),

           SDT::Boolean | SDT::Bool => Ok(AT::Boolean),

           // Reject bare/parameterized FLOAT for precision ambiguity (matches ADR 025 D4).
           SDT::Float(_) => Err(unsupported(
               "unsupported CREATE TABLE column type: FLOAT (precision ambiguous; use REAL or DOUBLE)",
           )),

           other => Err(unsupported(format!(
               "unsupported CREATE TABLE column type: {other}"
           ))),
       }
   }
   ```

   **Critical:** does NOT delegate to
   `lower_cast_target_type`.

7. **Implement `check_unique_column_names`:**

   ```rust
   fn check_unique_column_names(
       fields: &[arrow::datatypes::Field],
   ) -> Result<(), FrontendError> {
       let mut seen = HashSet::new();
       for field in fields {
           if !seen.insert(field.name()) {
               return Err(unsupported(
                   "CREATE TABLE column names must be unique",
               ));
           }
       }
       Ok(())
   }
   ```

**Verification:**

```sh
cargo check -p toron
```

## Phase 4 — Binder

**Goal.** Add `LogicalPlan::CreateTable` arm to
`bind_plan` — straight pass-through to
`ExecutionPlan::CreateTable`.

**Files:** `crates/toron/src/binder.rs`

**Tasks:**

1. **Add bind arm:**

   ```rust
   LogicalPlan::CreateTable {
       name,
       schema,
       if_not_exists,
   } => Ok(ExecutionPlan::CreateTable {
       name: name.clone(),
       schema: schema.clone(),
       if_not_exists: *if_not_exists,
   }),
   ```

   No expression binding; no column-ordinal rewriting;
   no input plan recursion.

2. **Update exhaustive `LogicalPlan` / `ExecutionPlan`
   match sites** in `binder.rs` (`terminal_exchange`,
   PBC rewrites, ordinal rewriters). For `CreateTable`,
   natural arm is leaf / pass-through / `None`.

**Verification:** `cargo check -p toron`.

## Phase 5 — Coordinator Execution

**Goal.** Wire DDL through `QueryCoordinator` and
`DistributedQueryCoordinator`. Preserve descriptor-
evolution retry semantics for SELECT (R1).

**Files:** `crates/toron/src/coordinator.rs` (single
file housing both coordinators).

**Tasks:**

1. **`QueryCoordinator::execute_query` — DDL branch
   BEFORE retry loop:**

   ```rust
   pub async fn execute_query(
       &self,
       plan: &LogicalPlan,
       morsel: Option<(MorselId, u32)>,
   ) -> Result<Vec<RecordBatch>, QueryError> {
       // DDL branch — intercept BEFORE the descriptor-evolution
       // retry loop so SELECT retry semantics stay unchanged.
       if matches!(plan, LogicalPlan::CreateTable { .. }) {
           let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
           return self.execute_catalog_plan(&bound).await;
       }

       let mut retried_descriptor_evolution = false;
       loop {
           let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
           // ... existing SELECT path retry logic ...
       }
   }
   ```

2. **Add private `execute_catalog_plan`:**

   ```rust
   async fn execute_catalog_plan(
       &self,
       bound: &ExecutionPlan,
   ) -> Result<Vec<RecordBatch>, QueryError> {
       match bound {
           ExecutionPlan::CreateTable { name, schema, if_not_exists } => {
               self.catalog
                   .create_table(name, schema.clone(), *if_not_exists)
                   .await?;
               Ok(Vec::new())
           }
           other => Err(QueryError::Internal {
               message: format!(
                   "execute_catalog_plan called with non-DDL plan: {other:?}"
               ),
           }),
       }
   }
   ```

   `QueryError` already has
   `Catalog(#[from] CatalogError)`, so `?` propagates
   catalog failures without wrapping.

3. **`DistributedQueryCoordinator::execute_query` —
   DDL branch BEFORE `fragment_plan`** (same file):

   ```rust
   let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
   if let ExecutionPlan::CreateTable { name, schema, if_not_exists } = &bound {
       self.catalog
           .create_table(name, schema.clone(), *if_not_exists)
           .await?;
       return Ok(Vec::new());
   }

   let stages = fragment_plan(bound, /* ... */)?;
   // ... existing distributed execution path ...
   ```

**Verification:**

```sh
cargo check -p toron
cargo test -p toron --lib coordinator
```

## Phase 6 — Executor and Fragmenter Guardrails

**Goal.** Defensive rejects. DDL should never reach
these surfaces in practice; the guards exist to fail
loud if the coordinator interception is bypassed.

**Files:**

- `crates/toron/src/exec.rs`
- `crates/toron/src/distributed/fragment.rs`

**Tasks:**

1. **`Executor::execute` (unbound)** add arm:

   ```rust
   LogicalPlan::CreateTable { .. } => Err(ExecutionError::NotImplemented {
       feature: "unbound CREATE TABLE execution",
   }),
   ```

2. **`Executor::execute_bound`** add arm:

   ```rust
   ExecutionPlan::CreateTable { .. } => Err(ExecutionError::NotImplemented {
       feature: "CREATE TABLE execution is coordinator-only",
   }),
   ```

3. **`FragmentError::CreateTableNotFragmentable`
   variant:**

   ```rust
   #[derive(thiserror::Error, Debug)]
   pub enum FragmentError {
       // ... existing variants ...

       #[error("CREATE TABLE is a coordinator-only catalog operation")]
       CreateTableNotFragmentable,
   }
   ```

4. **`fragment_plan` top-level reject:**

   ```rust
   pub fn fragment_plan(
       plan: ExecutionPlan,
       // ...
   ) -> Result<Vec<StagePlan>, FragmentError> {
       if matches!(plan, ExecutionPlan::CreateTable { .. }) {
           return Err(FragmentError::CreateTableNotFragmentable);
       }
       // ... existing fragmenter logic ...
   }
   ```

   Top-level reject runs BEFORE any exchange counting
   or plan walking.

5. **Recursive fragmenter walkers** — add `CreateTable`
   arms:

   - `contains_join` → `false`
   - `contains_broadcast_join` → `false`
   - `count_exchanges` → `0`
   - `find_exchange` → `None`
   - `first_scan_metadata` → `None`
   - `first_scan_fingerprint` → `None`
   - replacement helpers → no-op / pass-through

6. **Scheduler / server helpers** — same leaf-arm
   pattern. `join_strategy_label`, etc., return `None`.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron --lib fragment
```

## Phase 7 — Architecture Guards (15 new + 1 adjusted)

**Files:** `crates/toron/tests/architecture_test.rs`

**Tasks:**

1. **Adjust** existing "unsupported SQL statement"
   catch-all guard: allow `CREATE TABLE` lowering;
   preserve all other statement rejects.

2. **Add `adr029_sql_planner_accepts_only_select_and_create_table`:**
   source-reads `Statement::Query` + `Statement::CreateTable`
   arms + the narrowed catch-all diagnostic.

3. **Add `adr029_create_table_destructure_is_exhaustive`:**
   source-reads `lower_create_table` body for
   representative rejected fields (`query`, `like`,
   `clone`, `primary_key`, `partition_by`, `with_tags`)
   + absence of `..` rest pattern.

4. **Add `adr029_create_table_ir_surface_is_narrow`:**
   source-reads `LogicalPlan::CreateTable` +
   `ExecutionPlan::CreateTable` for `SchemaRef`;
   asserts absence of `ColumnSpec` / `CreateTableColumn`
   parallel IR type.

5. **Add `adr029_create_table_name_lowering_is_strict`:**
   source-reads `lower_create_table_name` for 1- or
   2-part match arms + 3-part reject diagnostic;
   asserts absence of `table_ref_from_object_name` in
   DDL path.

6. **Add `adr029_create_table_type_lattice_is_ddl_specific`:**
   source-reads `lower_create_table_data_type` for the
   6-type lattice; asserts bare-`FLOAT` + `DECIMAL`
   reject; asserts absence of `lower_cast_target_type`
   in DDL path.

7. **Add `adr029_create_table_column_options_are_narrow`:**
   source-reads `lower_create_table_column_nullability`
   for `ColumnOption::Null` + `ColumnOption::NotNull`
   arms + representative reject arms (`Default`,
   `Unique`, `Check`, `ForeignKey`, `Generated`) +
   named-constraint reject.

8. **Add `adr029_create_table_rejects_storage_synthetic_column_names`:**
   source-reads `STORAGE_SYNTHETIC_COLUMN_NAMES` for
   the five Wendelon constants + reserved-name
   diagnostic + reject branch in
   `lower_create_table_column`.

9. **Add `adr029_catalog_create_table_returns_binding`:**
   source-reads `Catalog::create_table` signature
   returning `Result<TableBinding, CatalogError>` +
   new `TableAlreadyExists` / `SchemaMismatch` /
   `AdminUnavailable` variants and reused
   `FingerprintResolution`.

10. **Add `adr029_mock_catalog_is_interior_mutable`:**
    source-reads `RwLock<MockCatalogInner>` +
    `next_table_id` + setup helpers bumping
    `next_table_id` via `get_mut()`.

11. **Add `adr029_mock_catalog_identity_uses_descriptor_fingerprint`:**
    source-reads `mock_table_identity` for
    `TableObjectId::new` + `TableSchemaDescriptor::new_with_layout`
    + `physical_descriptor_fingerprint`. Asserts
    absence of `PhysicalDescriptorFingerprint::from_bytes`
    in `crates/toron/src/catalog.rs`.

12. **Add `adr029_mock_catalog_reference_insertion_does_not_overwrite`:**
    source-reads `MockCatalog::create_table` for
    exact-ref `insert` + unqualified-alias
    `entry(...).or_insert(...)` path. Asserts absence
    of a second unconditional `by_ref.insert` on the
    alias path.

13. **Add `adr029_wendelon_catalog_create_table_is_admin_unavailable`:**
    source-reads `WendelonCatalog::create_table`
    `AdminUnavailable` return.

14. **Add `adr029_coordinators_intercept_create_table`:**
    source-reads `QueryCoordinator::execute_query`
    branch BEFORE descriptor-evolution retry loop +
    `DistributedQueryCoordinator::execute_query` branch
    BEFORE `fragment_plan` (both in
    `crates/toron/src/coordinator.rs`) + `Ok(Vec::new())`
    return + `?` propagation of `CatalogError` into
    `QueryError::Catalog`.

15. **Add `adr029_executor_and_fragmenter_reject_create_table`:**
    source-reads Executor coordinator-only diagnostic +
    `FragmentError::CreateTableNotFragmentable` +
    fragmenter top-level reject before exchange counting.

16. **Add `adr029_select_retry_loop_is_preserved`:**
    source-reads `QueryCoordinator::execute_query` for
    SELECT bind INSIDE descriptor-evolution retry loop.

Total: **15 new + 1 adjusted** architecture guards.

**Verification:**

```sh
cargo test -p toron --test architecture_test
```

## Phase 8 — Behavioral Tests

**Goal.** Comprehensive fixture coverage. **Per D14,
NO SELECT-after-CREATE test.**

**Files:** `crates/toron/tests/create_table_test.rs`
(NEW file)

**Tasks:**

1. **Planner positives:**

   ```sql
   CREATE TABLE sales (id INT, amount BIGINT, price DOUBLE, name TEXT, active BOOLEAN)
   CREATE TABLE IF NOT EXISTS app.sales (id INT NOT NULL, note VARCHAR(128) NULL)
   CREATE TABLE metrics (score REAL, active BOOLEAN)
   CREATE TABLE names (name VARCHAR(128))
   ```

   Assert `LogicalPlan::CreateTable` with expected
   `TableRef`, Arrow schema field names, data types,
   nullability.

2. **Planner negatives** (each with stable diagnostic
   assertion):

   ```sql
   CREATE TABLE t ()                              -- empty schema
   CREATE TABLE t (a INT, a BIGINT)               -- duplicate column
   CREATE TABLE catalog.public.t (a INT)          -- 3-part name
   CREATE TEMPORARY TABLE t (a INT)               -- TEMPORARY clause
   CREATE TABLE t AS SELECT 1                     -- CTAS
   CREATE TABLE t LIKE other                      -- LIKE clone
   CREATE TABLE t (a INT, PRIMARY KEY(a))         -- table-level PK
   CREATE TABLE t (a INT DEFAULT 1)               -- DEFAULT
   CREATE TABLE t (a INT PRIMARY KEY)             -- column-level PK
   CREATE TABLE t (a INT CHECK (a > 0))           -- CHECK
   CREATE TABLE t (a INT REFERENCES x(b))         -- REFERENCES
   CREATE TABLE t (a INT CONSTRAINT nn NOT NULL)  -- named constraint
   CREATE TABLE t (a DECIMAL(10,2))               -- DECIMAL
   CREATE TABLE t (a FLOAT)                       -- bare FLOAT
   CREATE TABLE t (a TIMESTAMP)                   -- temporal
   CREATE TABLE t (a INT NULL NOT NULL)           -- nullability conflict
   CREATE TABLE t (__row_hlc__ BIGINT)            -- D12 storage synthetic
   CREATE TABLE t (__row_key_id__ INT)            -- D12 storage synthetic
   CREATE TABLE t (__row_routing_locality__ INT)  -- D12 storage synthetic
   CREATE TABLE t (__row_table_object_id__ INT)   -- D12 storage synthetic
   CREATE TABLE t (__row_tombstone__ BOOLEAN)     -- D12 storage synthetic
   ```

   Assert the storage-synthetic rejects each carry
   the `"reserved for storage metadata"` diagnostic
   substring.

3. **Catalog tests** (against `MockCatalog`):

   - `MockCatalog::create_table` creates → resolve
     succeeds with expected schema + binding.
   - `CREATE TABLE IF NOT EXISTS` with existing
     identical schema → no-op success, returns
     existing binding.
   - `CREATE TABLE IF NOT EXISTS` with existing
     differing schema → `SchemaMismatch` (diagnostic
     uses `table_name` helper output).
   - `CREATE TABLE` without `IF NOT EXISTS` on
     existing table → `TableAlreadyExists`.
   - `WendelonCatalog::create_table` →
     `AdminUnavailable`.
   - **D7:** binding from `create_table` carries
     `TableObjectId` whose last 8 bytes equal
     `local_table_id.0.to_be_bytes()` and a
     `PhysicalDescriptorFingerprint` consistent with
     `TableSchemaDescriptor::new_with_layout` (NOT
     a synthesized zero-fingerprint placeholder).
   - **D10:** create `app.sales` first; then register
     a separate `default.sales` via setup helper.
     Verify that creating `other_schema.sales` later
     does NOT overwrite the existing unqualified
     `sales` alias. Test exercises the
     `entry(...).or_insert(...)` no-overwrite branch.

4. **Coordinator tests:**

   - `QueryCoordinator::execute_query(CREATE TABLE)` →
     `Ok(Vec::new())` and subsequent
     `catalog.resolve_table(&name)` succeeds.
   - `DistributedQueryCoordinator::execute_query(CREATE TABLE)`
     → `Ok(Vec::new())`; use a transport / scheduler
     test double that would fail if called (proves no
     fragmenter dispatch).
   - Preserve existing descriptor-evolution retry test
     for SELECT — confirm SELECT path unchanged.

5. **Guardrail tests:**

   - Direct `Executor::execute_bound(&ExecutionPlan::CreateTable { ... })`
     → coordinator-only `NotImplemented` error.
   - Direct `fragment_plan(ExecutionPlan::CreateTable { ... }, ...)`
     → `CreateTableNotFragmentable`.

6. **Non-goal explicitly excluded (D14):**

   Do NOT include any test of the form:

   ```sql
   CREATE TABLE t (a INT);
   SELECT a FROM t;
   ```

   Catalog visibility is not storage population. ADR
   029 proves catalog-side correctness only.

7. **Other rejects (catch-all preserved):**

   ```sql
   INSERT INTO t VALUES (1)        -- reject; ADR 030 territory
   DROP TABLE t                    -- reject
   ALTER TABLE t ADD COLUMN b INT  -- reject
   CREATE INDEX i ON t (a)         -- reject
   CREATE VIEW v AS SELECT 1       -- reject
   ```

   Assert the catch-all diagnostic:
   `"unsupported SQL statement; only SELECT and CREATE TABLE are supported"`.

**Verification:**

```sh
cargo test -p toron --test create_table_test
```

## Phase 9 — Final Verification

```sh
cargo fmt --check
cargo build -p toron --tests
cargo test -p toron --test architecture_test
cargo test -p toron --test create_table_test
cargo test -p toron
```

If formatting diff: `cargo fmt && cargo fmt --check`.

## Implementation risks

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Descriptor-evolution retry regression for SELECT | DDL branch BEFORE the loop; `adr029_select_retry_loop_is_preserved` guard. |
| R2 | `MockCatalog` ID fabrication implies persistence | D7 keeps deterministic IDs confined to test path; descriptor-construction-derived fingerprint is real but test-only. |
| R3 | DDL name lowering collapses multipart names silently | Separate `lower_create_table_name`; guard pins separation. |
| R4 | DDL type lattice drifts with cast syntax policy | Separate `lower_create_table_data_type`; guard pins separation. |
| R5 | sqlparser dependency upgrade introduces new fields silently | Exhaustive destructure without `..`; compile-time guard. |
| R6 | SELECT-after-CREATE test expectations create false confidence | D14 explicit; fixtures via `catalog.resolve_table`, not `StorageScan`. |
| R7 | DDL reaches data executor or fragmenter | Defensive arms + guards + top-level fragmenter reject. |
| R8 | `Catalog::create_table` returning `()` blocks tests + `IF NOT EXISTS` | Returns `TableBinding` (D5). |
| R9 | Wendelon-side persistence ambiguity | `WendelonCatalog::create_table` explicit `AdminUnavailable`. |
| R10 | Coordinator-branching duplication (local + distributed) | Both coordinator paths covered in Phase 5; tests cover both in Phase 8; shared private `execute_catalog_plan` helper. |
| R11 | Plan-digest regression for SELECT corpus | `LogicalPlan` / `ExecutionPlan` additive variant. |
| R12 | `MockCatalog` lock contention | Read locks for resolve; write lock for create; tests sequential. |
| R13 | Schema equality false positive on metadata mismatch | Arrow `Schema::eq` strictness intentional (D8). |
| R14 | Multi-statement script surprise | Single-statement preservation; `SqlPlanner::plan` handles `ast[0]` only. |
| R15 | DDL atomicity / partial-failure | Out of scope; storage-admin contract deferred to cross-ring ADR. |
| R16 | Architecture guard count growth | 15 new + 1 adjusted; substrate-introducing scope. |
| R17 | sqlparser `CreateTable` field set drift between versions | Exhaustive destructure forces compile-time reconsideration. |
| R18 | `ColumnOption` enum drift in sqlparser | Compiler-enforced exhaustive match in `lower_create_table_column_nullability`. |
| R19 | Mock fingerprint construction fails for user names colliding with Wendelon storage policy | D12 planner-time reject preempts descriptor-build error; `FingerprintResolution` is the catch-all. |
| R20 | Silent alias overwrite in `MockCatalog` | D10 `entry(...).or_insert(...)`; guard. |
| R21 | `PhysicalDescriptorFingerprint::from_bytes` is crate-private; direct fabrication won't compile | D7 derives fingerprint via `TableSchemaDescriptor::new_with_layout`; guard. |
| R22 | `TableId::new(...)` confusion | `TableId(value)` tuple-struct construction. |
| R23 | `TableRef::to_string()` won't compile (no `Display`) | `table_name(&TableRef)` crate-private helper. |
| R24 | Wrong file location for distributed coordinator | Both coordinators live in `crates/toron/src/coordinator.rs`. |

## Definition of Done

- [ ] `LogicalPlan::CreateTable` leaf variant.
- [ ] `ExecutionPlan::CreateTable` twin variant with
      `schema()` returning empty Arrow schema.
- [ ] `CatalogError::TableAlreadyExists` /
      `SchemaMismatch` / `AdminUnavailable` variants;
      existing `FingerprintResolution` reused for mock
      descriptor construction failures.
- [ ] `FragmentError::CreateTableNotFragmentable`
      variant.
- [ ] `Catalog::create_table` async trait method
      returning `Result<TableBinding, CatalogError>`.
- [ ] `MockCatalog` refactored to
      `RwLock<MockCatalogInner>` interior mutability.
- [ ] `mock_table_identity` helper derives
      `PhysicalDescriptorFingerprint` via
      `TableSchemaDescriptor::new_with_layout`;
      `TableObjectId` deterministic from
      `local_table_id.0` 8-byte suffix.
- [ ] `MockCatalog::create_table` allocates fresh
      `TableId(value)` via tuple-struct construction
      (NOT `TableId::new`); skips existing keys; bumps
      `next_table_id` monotonically.
- [ ] `MockCatalog::create_table` implements IF NOT
      EXISTS schema-equality semantics.
- [ ] MockCatalog reference insertion (D10): exact
      `TableRef` always inserted; unqualified alias
      via `entry(...).or_insert(...)`.
- [ ] `WendelonCatalog::create_table` returns
      `AdminUnavailable`.
- [ ] `Statement::CreateTable` arm in `SqlPlanner::plan`.
- [ ] `lower_create_table` destructures
      `sqlparser::ast::CreateTable` exhaustively
      without `..`.
- [ ] `lower_create_table_name` accepts 1- or 2-part
      names; rejects 3-part.
- [ ] `lower_create_table_data_type` DDL-specific
      6-type lattice; does NOT delegate to
      `lower_cast_target_type`.
- [ ] `lower_create_table_column` validates column
      options narrowly (NULL / NOT NULL only); rejects
      DEFAULT / UNIQUE / CHECK / REFERENCES / GENERATED
      / named-constraint / nullability-conflict.
- [ ] Storage synthetic column name reject (D12) at
      planner level for the 5 Wendelon constants.
- [ ] `check_unique_column_names` detects duplicate
      column names.
- [ ] Empty schemas reject with stable diagnostic.
- [ ] Binder arm:
      `LogicalPlan::CreateTable` →
      `ExecutionPlan::CreateTable` pass-through.
- [ ] `QueryCoordinator::execute_query` intercepts DDL
      BEFORE descriptor-evolution retry loop; returns
      `Ok(Vec::new())`.
- [ ] `DistributedQueryCoordinator::execute_query`
      intercepts DDL AFTER bind, BEFORE `fragment_plan`;
      returns `Ok(Vec::new())` (same file —
      `crates/toron/src/coordinator.rs`).
- [ ] `Executor` defensive arms (unbound + bound)
      return coordinator-only `NotImplemented`.
- [ ] `fragment_plan` top-level reject.
- [ ] Recursive fragmenter / scheduler / server
      walkers handle `CreateTable` as leaf.
- [ ] Diagnostic strings use crate-private
      `table_name(&TableRef)` helper for
      `TableAlreadyExists.table` /
      `SchemaMismatch.table`.
- [ ] **15** new architecture guards pass.
- [ ] **1** adjusted statement-catch-all guard.
- [ ] Planner positive fixtures (6-type lattice; IF
      NOT EXISTS; NULL/NOT NULL; 1-part and 2-part
      names; VARCHAR(n) / CHAR(n) advisory length).
- [ ] Planner negative fixtures (empty schema,
      duplicate column, 3-part name, CTAS, LIKE,
      TEMPORARY, PRIMARY KEY, CHECK, REFERENCES,
      named constraint, DEFAULT, DECIMAL, bare
      FLOAT, TIMESTAMP, NULL/NOT NULL conflict,
      all 5 storage synthetic column names).
- [ ] Catalog fixtures (create + resolve; IF NOT
      EXISTS match + mismatch; TableAlreadyExists;
      WendelonCatalog AdminUnavailable;
      fingerprint-from-descriptor;
      schema-qualified create does not overwrite
      existing unqualified alias).
- [ ] Coordinator fixtures (local + distributed;
      `Ok(Vec::new())` + resolvable binding; no
      scheduler dispatch for distributed DDL).
- [ ] Guardrail fixtures (Executor + fragmenter
      defensive rejects).
- [ ] Catch-all preservation (INSERT / DROP / ALTER /
      CREATE INDEX / CREATE VIEW continue to reject).
- [ ] Existing SELECT query corpus plan digests
      byte-identical.
- [ ] Roadmap commits ADR 030 = INSERT, ADR 031 =
      BoundExprProgram.
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
cargo test -p toron

# Optional when workspace deps change
cargo build -p toron
```
