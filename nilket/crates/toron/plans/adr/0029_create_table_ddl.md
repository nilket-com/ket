# ADR 029: SQL `CREATE TABLE` — Narrow DDL Surface

## Status

Promoted. Three-way council convergence (Claude / Codex /
Gemini) reached across four iterations. Iter-0 topic
split — Codex iter-0's substantial `BoundExprProgram`
(Brainstorm 004 narrow realization) draft vs Claude +
Gemini's `CREATE TABLE` framing — resolved at iter-1
via explicit sequencing commitment (ADR 030 = INSERT,
ADR 031 = `BoundExprProgram`). Substantive shape locked
at iter-2 (D1-D15) after Codex iter-2's four code-
evidence corrections (coordinator-owns-DDL, MockCatalog
interior mutability, DDL-specific name + type helpers,
exhaustive sqlparser destructure without `..`). Iter-3
folded in five additional Codex code-evidence
precision items (descriptor-derived fingerprint, alias-
no-overwrite, storage-synthetic-name reject, TableId
tuple-struct construction, TableRef diagnostic
formatter helper, single coordinator-file location).

**First DDL ADR.** **First ADR to integrate
substantively with Wendelon-defined types** (`TableObjectId`,
`PhysicalDescriptorFingerprint`, `TableSchemaDescriptor`,
`RowIdentityPolicy`, `LocalityPolicy`, etc.) —
established a new cycle pattern that ADRs touching
cross-ring storage types need 2+ iterations of code-
evidence Codex passes to firm up implementation
contracts. Eleventh instance of the ADR 015 cycle (after
016 / 017 / 018 / 019 / 022 / 023 / 024 / 025 / 026 / 027 /
028).

## Scale boundary

**Primary:** Toron SQL planner + binder + coordinator +
catalog. Surface widens the statement layer in
`SqlPlanner::plan` to handle `Statement::CreateTable`;
adds `LogicalPlan::CreateTable` + `ExecutionPlan::CreateTable`
leaf variants; extends the `Catalog` trait with an
`async fn create_table`; intercepts DDL in both
`QueryCoordinator` and `DistributedQueryCoordinator`
BEFORE their respective downstream stages.

In scope:

- `SqlPlanner::plan` widens to handle
  `Statement::CreateTable`.
- New `LogicalPlan::CreateTable` / `ExecutionPlan::CreateTable`
  leaf variants carrying `arrow::datatypes::SchemaRef`.
- New `lower_create_table` / `lower_create_table_name`
  / `lower_create_table_data_type` /
  `lower_create_table_column` /
  `lower_create_table_column_nullability` helpers in
  `sql_planner.rs` — all DDL-specific, not delegating
  to existing read-side helpers.
- New `Catalog::create_table` async trait method
  returning `Result<TableBinding, CatalogError>`.
- New `MockCatalog` interior-mutability shape
  (`RwLock<MockCatalogInner>`) + `mock_table_identity`
  helper deriving `PhysicalDescriptorFingerprint`
  through `TableSchemaDescriptor::new_with_layout`.
- `WendelonCatalog::create_table` returns
  `CatalogError::AdminUnavailable`.
- Coordinator interception in `QueryCoordinator` +
  `DistributedQueryCoordinator` (both in
  `crates/toron/src/coordinator.rs`).
- Defensive `Executor` + `Fragmenter` rejects.
- 15 new architecture guards + 1 adjusted statement-
  level catch-all guard.
- Comprehensive fixture set (planner positives /
  planner negatives / catalog / coordinator /
  guardrail / catch-all preservation).

Out of scope:

- `INSERT` / `UPDATE` / `DELETE` / `ALTER` / `DROP` /
  `CREATE INDEX` / `CREATE VIEW` / `CREATE TABLE AS
  SELECT` (CTAS).
- Table-level or column-level constraints, except the
  narrow column-level `NOT NULL` nullability marker
  (`PRIMARY KEY` / `FOREIGN KEY` / `CHECK` / `UNIQUE`
  remain out of scope).
- `DEFAULT` / `GENERATED` / identity / collation /
  comments / partitioning / table options /
  clustering / `STORED AS` / `LOCATION` / `LIKE` /
  `CLONE` / `TEMPORARY` / `EXTERNAL` / `GLOBAL` /
  `LOCAL` / `TRANSIENT` / `VOLATILE` / `STRICT` /
  `COPY GRANTS` / Snowflake-specific clauses.
- Multi-statement scripts.
- Wendelon-side persistent descriptor writes (ADR
  defers cross-ring metadata-write contract).
- Rune DDL API.
- `SELECT * FROM newly_created_table` storage-readback
  path — ADR 029 proves catalog visibility only.
- Decimal / temporal / binary / array / struct / map /
  unsigned-integer column types.

## Context

Pre-ADR-029 Toron is `SELECT`-only at the statement
layer. `SqlPlanner::plan` matches `Statement::Query`
and rejects every other statement with the catch-all
diagnostic:

```text
unsupported SQL statement; only SELECT is supported
```

This produces a chicken-and-egg problem for analytical
workflows: users cannot create tables in Toron-native
SQL — they must register tables out-of-band via
`MockCatalog::register_descriptor` setup helpers (test
path) or rely on Wendelon-side descriptor population
(production path). The user's selection-round framing —
*"we should be able to create tables before going super
deep into approximate quantiles"* — surfaces the
priority gap after five consecutive expression-IR cycles
(ADR 024 / 025 / 026 / 027 / 028) widened the read
path without touching the DDL surface.

ADR 029 is the **narrowest possible** DDL widening that
gives users an in-band path to register table schemas.
It is paired with a sequencing commitment to ADR 030
(`INSERT`) — `CREATE TABLE` alone is unusable without
a way to populate the table. The Codex iter-0 substrate
fallback (`BoundExprProgram`, Brainstorm 004's narrow
realization) is sequenced to ADR 031 (D15 below).

The load-bearing decisions are: **where does DDL
execute** (coordinator-only vs executor), **what
catalog mutation contract is offered** (return type +
mutability shape), **how do existing Wendelon types
factor in** (deterministic fingerprint construction
without crate-private API access), and **what column
shapes are accepted** (six-type lattice vs ADR 025
cast lattice). The council converged on coordinator-
only execution with descriptor-derived fingerprint
identity and a DDL-specific helper family that does
NOT delegate to existing read-side lowering helpers.

## Decision

### D1 — Narrow SQL `CREATE TABLE` shape

The accepted SQL surface:

```sql
CREATE TABLE [IF NOT EXISTS] [schema.]table_name (
    column_name TYPE [NULL | NOT NULL],
    ...
)
```

All other DDL, DML, constraints, indexes, and table
options are out of scope for v1. The sequencing
commitment carries `INSERT INTO ... VALUES (...)` as
ADR 030 and Brainstorm 004's `BoundExprProgram` narrow
realization as ADR 031.

### D2 — Statement planner widens with exhaustive sqlparser destructure

`SqlPlanner::plan`'s top-level match adds a
`Statement::CreateTable` arm and narrows the existing
catch-all diagnostic to
`"unsupported SQL statement; only SELECT and CREATE TABLE
are supported"`. The DDL helper `lower_create_table`
destructures `sqlparser::ast::CreateTable` **exhaustively
without `..`** — every field of the upstream AST struct
is named in the destructure and explicitly accepted or
rejected with a stable diagnostic. This is the compile-
time guard against silent acceptance of new clauses
when `sqlparser` upgrades introduce additional fields.

### D3 — Logical IR carries `SchemaRef`

`LogicalPlan::CreateTable` and `ExecutionPlan::CreateTable`
are additive leaf variants carrying
`arrow::datatypes::SchemaRef` directly. No parallel
`ColumnSpec` or `CreateTableColumn` IR type is
introduced — Toron's IR is Arrow-native throughout, and
DDL inherits the same convention.

```rust
LogicalPlan::CreateTable {
    name: TableRef,
    schema: arrow::datatypes::SchemaRef,
    if_not_exists: bool,
}

ExecutionPlan::CreateTable {
    name: TableRef,
    schema: arrow::datatypes::SchemaRef,
    if_not_exists: bool,
}
```

`ExecutionPlan::CreateTable.schema()` returns
`Arc::new(Schema::empty())` — the empty schema is
metadata for the control-plane result, NOT a promise
of empty-batch row output.

### D4 — DDL is coordinator-only

`Executor` owns `storage: Arc<dyn StorageScan>` and has
no catalog handle; DDL is a control-plane operation
that must NOT cross the data-plane boundary.

- `QueryCoordinator::execute_query` intercepts
  `LogicalPlan::CreateTable` **before** the
  descriptor-evolution retry loop. SELECT retry
  semantics stay byte-identical.
- `DistributedQueryCoordinator::execute_query`
  intercepts `ExecutionPlan::CreateTable` **after**
  binding, **before** `fragment_plan`. DDL never
  enters the distributed task graph.
- `Fragmenter` explicitly rejects
  `ExecutionPlan::CreateTable` at the top level with
  `FragmentError::CreateTableNotFragmentable`.
- `Executor::execute_bound` adds a defensive arm
  returning `ExecutionError::NotImplemented` with
  message `"CREATE TABLE execution is coordinator-only"`.

The Executor defensive arm is a guardrail, not the
primary execution path — its existence pins the
architectural invariant.

### D5 — `Catalog::create_table` returns `TableBinding`

The new trait method signature:

```rust
async fn create_table(
    &self,
    name: &TableRef,
    schema: SchemaRef,
    if_not_exists: bool,
) -> Result<TableBinding, CatalogError>
```

Returning `TableBinding` (NOT `()`) enables clean
`IF NOT EXISTS` semantics (return the existing binding
on match) and supports test paths that need to verify
the created identity.

New `CatalogError` variants:

- `TableAlreadyExists { table: String }` — duplicate
  name without `IF NOT EXISTS`.
- `SchemaMismatch { table: String }` — `IF NOT EXISTS`
  with a differing existing schema.
- `AdminUnavailable { message: String }` —
  `WendelonCatalog` cannot service DDL today.

Existing `CatalogError::FingerprintResolution` is reused
for mock identity descriptor construction failures (rare;
planner-time D12 reject preempts in normal flow).

### D6 — `MockCatalog` becomes interior-mutable

Because `Catalog::create_table` takes `&self` (trait
methods cannot require `&mut self` through `Arc<dyn
Catalog>`), `MockCatalog` wraps its internal state in
an `RwLock`:

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

Read methods take read locks; `create_table` takes a
write lock. Setup helpers (`register_descriptor`,
`register_table`) stay `&mut self` and use
`self.inner.get_mut()` to avoid lock overhead in test
setup paths; both bump `next_table_id` past the
registered id to keep `create_table`'s allocator
monotone.

### D7 — Mock identity uses real descriptor fingerprint

`TableBinding::new` requires
`(TableId, TableObjectId, PhysicalDescriptorFingerprint,
SchemaRef)`. `PhysicalDescriptorFingerprint::from_bytes`
is **crate-private to Wendelon** — Toron cannot
fabricate fingerprint bytes directly.

The clean path: construct a real `TableSchemaDescriptor`
with deterministic policy choices and extract its
`physical_descriptor_fingerprint`. A new
`mock_table_identity` helper in `catalog.rs` produces
deterministic-yet-real `(TableObjectId,
PhysicalDescriptorFingerprint)` pairs:

```rust
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

`TableObjectId` is deterministic via
`local_table_id.0.to_be_bytes()` stamped into the
trailing 8 bytes. This is catalog identity bookkeeping
for test paths only — no Wendelon persistence implied.

### D8 — `IF NOT EXISTS` uses strict Arrow `Schema` equality

If a table already exists at the requested name and
`if_not_exists` is true, success is conditional on
**exact Arrow `Schema` equality** (including metadata,
field nullability, field ordering, and data types).
Mismatches return `CatalogError::SchemaMismatch`.
Arrow's derived `Schema::eq` is the source of truth;
the strictness is intentional.

### D9 — DDL-specific name lowering

A new `lower_create_table_name` helper accepts 1- or
2-part names and rejects 3-part names with a stable
diagnostic:

```rust
fn lower_create_table_name(name: &ObjectName)
    -> Result<TableRef, FrontendError>
{
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

**Critical:** this helper does NOT call
`table_ref_from_object_name`, which silently collapses
multi-part names — wrong for DDL where ambiguity must
surface as an explicit error.

### D10 — `MockCatalog` reference insertion uses alias-no-overwrite

When `MockCatalog::create_table` inserts a new table
binding:

- Always insert the **exact requested** `TableRef`
  into `by_ref`.
- For schema-qualified names (e.g., `app.sales`), also
  install the unqualified alias (`sales`) **only if
  the unqualified slot is currently vacant**. Use
  `entry(...).or_insert(...)`, NOT unconditional
  insert.
- Never overwrite an existing `by_ref` entry as a
  side effect of creating another table.

```rust
inner.by_ref.insert(name.clone(), local_table_id);
if name.schema.is_some() {
    inner
        .by_ref
        .entry(TableRef::new(name.table.clone()))
        .or_insert(local_table_id);
}
```

Preserves convenient unqualified lookup for the simple
case while preventing silent overwrites when two
schemas share a table name in test setups.

### D11 — DDL-specific type lattice

A new `lower_create_table_data_type` helper covers a
six-type lattice:

| sqlparser shape | Arrow type |
|---|---|
| `INT` / `INTEGER` / `INT4` | `Int32` |
| `BIGINT` / `INT8` / `INT64` | `Int64` |
| `REAL` / `FLOAT4` / `FLOAT32` | `Float32` |
| `DOUBLE` / `DOUBLE PRECISION` / `FLOAT8` / `FLOAT64` | `Float64` |
| `TEXT` / `VARCHAR` / `CHAR` / `STRING` (and length-parameterized variants) | `Utf8` |
| `BOOLEAN` / `BOOL` | `Boolean` |

Bare `FLOAT` is rejected for precision ambiguity (mirrors
ADR 025 D4). `DECIMAL`, temporal types (`DATE` /
`TIMESTAMP` / `TIME`), binary, array, struct, map, and
unsigned-integer types are rejected with stable
diagnostics.

**Critical:** this helper does NOT delegate to ADR 025's
`lower_cast_target_type`, which is numeric-only and
rejects `Utf8` / `Boolean`. The DDL type lattice is a
separate decision domain from the Cast lattice;
architecture guard pins the separation.

### D12 — Narrow column options + storage-synthetic-name reject

Only `NULL` and `NOT NULL` column options are accepted.
All other `ColumnOption` variants reject with stable
diagnostics:

- Named constraints (`CONSTRAINT foo ...`)
- `DEFAULT`
- `UNIQUE` / `PRIMARY KEY` (column-level)
- `REFERENCES` (foreign key)
- `CHECK`
- `GENERATED`
- `COLLATE`

Additionally, **storage synthetic column names** are
rejected at planner time. Wendelon's storage layer uses
synthetic columns for row-identity metadata:

- `__row_hlc__`
- `__row_key_id__`
- `__row_routing_locality__`
- `__row_table_object_id__`
- `__row_tombstone__`

User schemas trying to define these names would either
collide with descriptor construction (causing the D7
helper to fail) or shadow legitimate storage metadata.
Planner-time reject with stable diagnostic:

```text
CREATE TABLE column name is reserved for storage metadata: {name}
```

Architecture guard
(`adr029_create_table_rejects_storage_synthetic_column_names`)
pins the five-string list.

### D13 — DDL success returns empty batches

Both coordinators return `Ok(Vec::new())` on successful
DDL execution. Empty `Vec<RecordBatch>` is the wire-
shape for "operation succeeded; no rows to return" —
matches existing `QueryError::Catalog` propagation
shape via `?` operator dispatch.

### D14 — `SELECT * FROM newly_created_table` is NOT promised

ADR 029 proves catalog **visibility**, not storage
**population**. After `CREATE TABLE t (...)`,
`catalog.resolve_table(&t)` succeeds — but
`SELECT * FROM t` requires a storage-write companion
ADR (ADR 030 = `INSERT`). Test fixtures verify catalog
resolution; **no fixture queries `SELECT` against a
freshly-created table**.

### D15 — Existing SELECT plan digests stay byte-identical

The ADR is purely additive at the IR level — new
variants, no existing variant shape change. Existing
SELECT query corpus plan digests remain byte-identical
pre/post-ADR-029. SELECT bind stays inside
`QueryCoordinator`'s descriptor-evolution retry loop;
the DDL interception branches **before** the loop.

### Sequencing commitment

The roadmap is committed to:

- **ADR 030 = `INSERT INTO ... VALUES (...)`** —
  paired with this cycle; `CREATE TABLE` alone is
  unusable without `INSERT`.
- **ADR 031 = `BoundExprProgram`** (Codex iter-0's
  Brainstorm 004 narrow realization) — the substrate-
  introducing expression-IR work that was the iter-0
  topic alternative.

## Reject removal (ADR 015 cycle's eleventh instance)

The existing statement-level catch-all diagnostic
narrows:

```text
unsupported SQL statement; only SELECT is supported
```

becomes

```text
unsupported SQL statement; only SELECT and CREATE TABLE are supported
```

Structural rejects preserved or added:

- `INSERT` / `UPDATE` / `DELETE` / `ALTER` / `DROP` /
  `CREATE INDEX` / `CREATE VIEW` / `CREATE TABLE AS
  SELECT` continue to reject via the narrowed catch-
  all (D1 + adjusted guard).
- Per-clause CREATE TABLE rejects (D2 exhaustive
  destructure): every unsupported sqlparser field
  rejects with a specific diagnostic.
- 3-part name reject (D9):
  `"CREATE TABLE supports only one- or two-part table names"`.
- Type rejects (D11): bare `FLOAT`, `DECIMAL`,
  temporal, binary, array, struct, map, unsigned-
  integer types.
- Column-option rejects (D12): named constraints,
  `DEFAULT`, `UNIQUE` / `PRIMARY KEY`, `REFERENCES`,
  `CHECK`, `GENERATED`, `COLLATE`.
- Storage synthetic name reject (D12 iter-3
  elaboration): the five `__row_*__` names.
- Duplicate column name reject + empty schema reject
  (D12 / D11 implicit).
- Nullability conflict reject (D12).

New errors added:

- `CatalogError::TableAlreadyExists` /
  `SchemaMismatch` / `AdminUnavailable`.
- Existing `CatalogError::FingerprintResolution` is reused
  for mock descriptor construction failures.
- `FragmentError::CreateTableNotFragmentable`.

## Architecture guard adjustments

| Guard | Status |
|---|---|
| Existing statement-level "unsupported SQL statement" catch-all guard | **Adjusted.** Allow `CREATE TABLE` lowering; preserve all other statement-level rejects. |
| **NEW** `adr029_sql_planner_accepts_only_select_and_create_table` | Source-reads `Statement::Query` + `Statement::CreateTable` arms + the narrowed catch-all diagnostic. |
| **NEW** `adr029_create_table_destructure_is_exhaustive` | Source-reads `lower_create_table` body for representative rejected fields (`query`, `like`, `clone`, `primary_key`, `partition_by`, `with_tags`) + absence of `..` rest pattern. |
| **NEW** `adr029_create_table_ir_surface_is_narrow` | Source-reads `LogicalPlan::CreateTable` + `ExecutionPlan::CreateTable` for `SchemaRef`; asserts absence of `ColumnSpec` / `CreateTableColumn` parallel IR type. |
| **NEW** `adr029_create_table_name_lowering_is_strict` | Source-reads `lower_create_table_name` for 1- or 2-part match arms + 3-part reject diagnostic; asserts absence of `table_ref_from_object_name` in DDL path. |
| **NEW** `adr029_create_table_type_lattice_is_ddl_specific` | Source-reads `lower_create_table_data_type` for the 6-type lattice; asserts bare-`FLOAT` + `DECIMAL` reject; asserts absence of `lower_cast_target_type` in DDL path. |
| **NEW** `adr029_create_table_column_options_are_narrow` | Source-reads `lower_create_table_column_nullability` for `ColumnOption::Null` + `ColumnOption::NotNull` arms + representative reject arms (`Default`, `Unique`, `Check`, `ForeignKey`, `Generated`) + named-constraint reject. |
| **NEW** `adr029_create_table_rejects_storage_synthetic_column_names` | Source-reads `STORAGE_SYNTHETIC_COLUMN_NAMES` for the five Wendelon constants + reserved-name diagnostic + reject branch in `lower_create_table_column`. |
| **NEW** `adr029_catalog_create_table_returns_binding` | Source-reads `Catalog::create_table` signature returning `Result<TableBinding, CatalogError>` + new catalog error variants; verifies existing `FingerprintResolution` is reused for mock descriptor construction failures. |
| **NEW** `adr029_mock_catalog_is_interior_mutable` | Source-reads `RwLock<MockCatalogInner>` + `next_table_id` + setup helpers bumping `next_table_id` via `get_mut()`. |
| **NEW** `adr029_mock_catalog_identity_uses_descriptor_fingerprint` | Source-reads `mock_table_identity` for `TableObjectId::new` + `TableSchemaDescriptor::new_with_layout` + `physical_descriptor_fingerprint`; asserts absence of `PhysicalDescriptorFingerprint::from_bytes` in Toron catalog code. |
| **NEW** `adr029_mock_catalog_reference_insertion_does_not_overwrite` | Source-reads `MockCatalog::create_table` for exact-ref `insert` + unqualified-alias `entry(...).or_insert(...)` path; asserts absence of a second unconditional `by_ref.insert` on the alias path. |
| **NEW** `adr029_wendelon_catalog_create_table_is_admin_unavailable` | Source-reads `WendelonCatalog::create_table` `AdminUnavailable` return. |
| **NEW** `adr029_coordinators_intercept_create_table` | Source-reads `QueryCoordinator::execute_query` branch BEFORE descriptor-evolution retry loop + `DistributedQueryCoordinator::execute_query` branch BEFORE `fragment_plan` (both in `crates/toron/src/coordinator.rs`) + `Ok(Vec::new())` return. |
| **NEW** `adr029_executor_and_fragmenter_reject_create_table` | Source-reads Executor coordinator-only diagnostic + `FragmentError::CreateTableNotFragmentable` + fragmenter top-level reject before exchange counting. |
| **NEW** `adr029_select_retry_loop_is_preserved` | Source-reads `QueryCoordinator::execute_query` for SELECT bind INSIDE descriptor-evolution retry loop. Pins the SELECT-path-preservation mitigation. |

Total: **15 new + 1 adjusted** architecture guards.

## SoA-readiness check

ADR 021's four invariants preserved:

1. Catalog stays pure metadata for kernels; new
   table-schema entries don't affect kernel state.
2. `#[repr(u8)]` on `StableAggregateKernel` unchanged.
3. Centralized dispatch unchanged — DDL doesn't engage
   per-row dispatch.
4. No row-path dyn dispatch.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Untouched — DDL
  doesn't engage the row-layer kernel path.
- **Brainstorm 002 (WeakControlRing).** Touches the
  seam preserved; coordinator-only execution (D4)
  keeps WeakControlRing consensus-ownership story open
  without baking topology assumptions.
- **Brainstorm 003 (Disaggregated Compute).** Touches
  the seam preserved; coordinator-only execution +
  fragmenter rejection preserves `NodeRoleSet` routing
  semantics.
- **Brainstorm 004 (Memory Arenas).** Untouched at the
  expression-IR level. New IR variant uses `SchemaRef`
  + `TableRef` + `bool` — no `Box<T>` chain. ADR 031
  sequencing commitment (D1 / D15) keeps Brainstorm
  004 near-term.

No seam closes.

## What Not To Do

- Don't put DDL in `Executor::execute_bound` as the
  primary execution site (D4 — defensive arm only).
- Don't return `()` from `Catalog::create_table` (D5).
- Don't leave `MockCatalog` with non-interior-mutable
  storage (D6).
- Don't try to construct `PhysicalDescriptorFingerprint`
  directly via byte fabrication (D7); `from_bytes` is
  crate-private to Wendelon.
- Don't reuse ADR 025's `lower_cast_target_type` for
  DDL (D11).
- Don't reuse `table_ref_from_object_name` for DDL
  (D9).
- Don't destructure `sqlparser::ast::CreateTable` with
  `..` (D2).
- Don't promise `SELECT ... FROM newly_created_table`
  in ADR 029 fixtures (D14).
- Don't unconditionally insert the unqualified alias
  when creating a schema-qualified table (D10); use
  `entry(...).or_insert(...)`.
- Don't accept user-defined column names that match
  Wendelon storage synthetic-column-name constants
  (D12).
- Don't expand to `INSERT` / `UPDATE` / `DELETE` /
  `ALTER` / `DROP` / `CREATE INDEX` / `CREATE VIEW` /
  CTAS at v1.
- Don't add constraints / primary keys / foreign keys /
  unique / check / defaults / generated columns /
  identity / collation / comments / partitioning /
  table options at v1.
- Don't bundle multi-statement script support.
- Don't introduce HAVING alias visibility / hidden-
  aggregate `ORDER BY` / Utf8 `CAST` in this cycle.
- Don't move binder out of `QueryCoordinator`'s
  descriptor-evolution retry loop for SELECT (D15).
- Don't break plan-digest backward compat for the
  SELECT query corpus.
- Don't bake `WendelonCatalog::create_table`
  persistence semantics — return `AdminUnavailable`
  until a cross-ring metadata-write ADR engages.
- Don't use `TableId::new(...)` — `TableId` is a
  tuple-struct `TableId(pub u64)`; use tuple
  construction `TableId(value)`.
- Don't use `TableRef::to_string()` for diagnostics —
  `TableRef` has no `Display` impl; use the existing
  crate-private `table_name(&TableRef) -> String`
  helper for `TableAlreadyExists.table` /
  `SchemaMismatch.table` field values.

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Descriptor-evolution retry regression for SELECT | DDL branch BEFORE the loop; `adr029_select_retry_loop_is_preserved` guard. |
| R2 | MockCatalog ID fabrication implies persistence | D7 keeps deterministic IDs confined to the test path; descriptor-construction-derived fingerprint is real but test-only. |
| R3 | DDL name lowering collapses multi-part names silently | D9 separate helper; guard. |
| R4 | DDL type lattice drifts with cast syntax policy | D11 separate helper; guard. |
| R5 | sqlparser dependency upgrade introduces new fields silently | D2 exhaustive destructure without `..`; compile-time guard. |
| R6 | SELECT-after-CREATE test expectations create false confidence | D14 explicit; fixtures via `catalog.resolve_table` only. |
| R7 | DDL reaches data executor or fragmenter | Defensive arms + guards + top-level fragmenter reject. |
| R8 | `Catalog::create_table` returning `()` blocks tests + `IF NOT EXISTS` | D5 returns `TableBinding`. |
| R9 | Wendelon-side persistence ambiguity | `WendelonCatalog::create_table` explicit `AdminUnavailable`. |
| R10 | Coordinator-branching duplication (local + distributed) | Phase 5 covers both paths; shared private `execute_catalog_plan` helper. |
| R11 | Plan-digest regression for SELECT corpus | `LogicalPlan` / `ExecutionPlan` additive variant. |
| R12 | MockCatalog lock contention | Read lock for resolve; write lock for create; tests sequential. |
| R13 | Schema equality false positive on metadata mismatch | Arrow `Schema::eq` strictness intentional (D8). |
| R14 | Multi-statement script surprise | Single-statement preservation; out of scope explicit. |
| R15 | DDL atomicity / partial-failure undefined | Out of scope; storage-admin contract defers to cross-ring ADR. |
| R16 | Architecture guard count growth | 15 new + 1 adjusted; substrate-introducing scope warrants. |
| R17 | sqlparser `CreateTable` field set drift between dependency versions | D2 exhaustive destructure forces compile-time reconsideration. |
| R18 | `ColumnOption` enum drift in sqlparser | Exhaustive match in `lower_create_table_column_nullability`; compiler enforces. |
| R19 | Mock fingerprint construction fails for user names colliding with Wendelon storage policy | D12 planner-time reject for storage synthetic names preempts descriptor-build error. |
| R20 | Silent alias overwrite in `MockCatalog` | D10 `entry(...).or_insert(...)` for unqualified-alias path. |
| R21 | `PhysicalDescriptorFingerprint::from_bytes` is crate-private; direct fabrication would not compile | D7 derives fingerprint via `TableSchemaDescriptor::new_with_layout`. |
| R22 | `TableId` tuple-struct construction confusion | Use `TableId(value)`, not `TableId::new(value)`. |
| R23 | `TableRef` diagnostic formatting | Use crate-private `table_name(&TableRef) -> String` helper; `TableRef` has no `Display`. |

## Recorded dissent

**None at iter-3.** All iter-0 / iter-1 / iter-2 open
questions resolved.

Two structural patterns recorded:

- **iter-0 topic split (BoundExprProgram vs CREATE TABLE):**
  Codex iter-0 proposed `BoundExprProgram` as
  Brainstorm 004's narrow realization. User-evidence
  framing ("create tables before going deep on
  quantiles") and 2-of-3 council weight selected
  CREATE TABLE. Codex conceded at iter-1 with an
  explicit ADR 031 sequencing ask — third consecutive
  instance of the topic-concession-compresses-next-
  cycle pattern (ADR 020 → 021; ADR 027 → 028;
  ADR 029 → 031).

- **Gemini iter-1 → iter-2 retraction streak (fourth
  consecutive cycle):** Gemini iter-2 formally
  retracted four iter-1 framings (D4 ownership, D5
  return type, D9 type helper, D14 SELECT-after-
  CREATE). Matches the documented pattern from
  ADR 025 / 026 / 028 — Gemini's iter-1 broader-scope
  framings reliably narrow at iter-2 once Codex's
  code-evidence pass surfaces.

## Process observations

### Substrate-introducing cadence with zero internal flip-flops

ADR 029 took 4 iterations (iter-0 through iter-3) with
2 topic-selection events (iter-0 split → iter-1
resolution) and **zero substantive flip-flops** on
D1-D15 content after iter-2 council convergence. Iter-3
was pure code-evidence precision-folding.

| ADR | Substrate-introducing? | Iterations | Flip-flops |
|---|---|---|---|
| ADR 023 | Yes | 4 + consolidation | 1 |
| ADR 024 | Yes | 4+ | Multi |
| ADR 028 | Yes | 5 | 2 (user-evidence iter-4 reopen) |
| **ADR 029** | **Yes (statement layer)** | **4** | **0 (topic concession at iter-1 resolved cleanly)** |

The substrate-introducing cadence holds with a
narrower flip-flop profile than ADR 023 / 024 / 028.
ADR 029's narrowness (single new statement type;
clear coordinator/catalog ownership; pre-existing
Wendelon types) constrained the design space more
than ADRs touching expression IR or aggregate
signatures.

### Code-evidence Codex pattern matures at iter-2 + iter-3 (sixth consecutive cycle)

| ADR | Codex correctness contribution | Surface iter |
|---|---|---|
| ADR 024 | Aggregate-argument shape guard | iter-1 |
| ADR 025 | Identity-Cast PBC + float-to-int bound | iter-2 |
| ADR 026 | Inner-Cast-error propagation (D11) | iter-1 |
| ADR 027 | Visible-output validator (D2) | iter-1 |
| ADR 028 | Planner-private signature key + `COUNT(*)` placeholder + reflexivity fix | iter-0 + iter-3 + iter-5 |
| **ADR 029** | **Coordinator/catalog ownership (iter-2 D4) + MockCatalog interior mutability (iter-2 D6) + DDL-specific helpers (iter-2 D9 + D11) + descriptor-derived fingerprint (iter-3 D7) + alias-no-overwrite (iter-3 D10) + storage-synthetic-name reject (iter-3 D12)** | **iter-2 + iter-3** |

ADR 029 has the most code-evidence-grounded Codex
contributions of any cycle: six precision items across
iter-2 and iter-3, all surfaced from actual codebase
inspection rather than abstract design reasoning. New
documented cycle invariant: **substrate-introducing ADRs
that integrate with real existing Wendelon-defined types
reliably need 2+ iterations of code-evidence Codex
passes to firm up implementation contracts.**

### Gemini iter-1 → iter-2 retraction streak (four consecutive cycles)

| ADR | Gemini iter-1/2 position | Iter-2/3 retraction |
|---|---|---|
| ADR 025 | Arrow-kernel-delegation (broader scope) | Manual per-pair kernels |
| ADR 026 | `null_on_failure: bool` flag on strict kernels | Separate `try_cast_*` kernels |
| ADR 028 | Public manual `Eq + Hash` on IR | Planner-private signature key |
| **ADR 029** | **iter-1 D4 (Executor owns DDL) + D5 (`()` return) + D9 (reuse `lower_data_type`) + D13 (SELECT-after-CREATE in fixtures)** | **iter-2: four explicit "I retract my Iteration 1" framings adopting Codex code-evidence corrections** |

Now a 4-of-4 confirmed pattern. ADR 029 is the most
explicit retraction cycle (Gemini iter-2 prominently
labels each correction). Stable process feature.

### Topic-concession-compresses-next-cycle pattern at 3-of-3

ADR 020 → 021 (computed kernel-catalog substrate);
ADR 027 → 028 (computed-aggregate-args); ADR 029 → 031
(`BoundExprProgram`). Codex's iter-0 ADR 029
`BoundExprProgram` doc seeds the ADR 031 iter-0 cycle
the same way Gemini iter-0 ADR 020 seeded ADR 021 and
Claude iter-0 ADR 027 seeded ADR 028. Stable invariant.

### New process pattern — pre-existing-Wendelon-types ADRs

ADR 029 is the first ADR to **integrate substantively
with Wendelon-defined types** (`TableObjectId`,
`PhysicalDescriptorFingerprint`, `TableSchemaDescriptor`,
`RowIdentityPolicy`, `LocalityPolicy`, etc.). The iter-3
precision items (D7 fingerprint construction; R21
`from_bytes`-not-public discovery) are all rooted in
"Wendelon doesn't expose what we initially assumed".

This is a **new cycle pattern**: ADRs that touch
storage-ring types need a code-evidence pass on the
Wendelon-side API surface, not just Toron-side. ADR 030
(`INSERT`) and the eventual cross-ring AGG consumer
wiring ADR will share this characteristic. Recorded:
**future cross-ring-touching ADRs should budget an
explicit Wendelon-API-discovery pass at iter-2.**

## Future work

ADR 030+ candidates:

- **ADR 030: `INSERT INTO ... VALUES (...)`.** Paired
  with this cycle — `CREATE TABLE` alone is unusable
  without `INSERT`. Sequencing committed in D1 / D15.
- **ADR 031: Bound Expression Program** (Codex's
  narrow Brainstorm 004 realization). Sequencing
  committed in D1 / D15. Codex iter-0 doc seeds the
  cycle.
- `INSERT INTO ... SELECT ...` (composes 029 + 030 +
  read-side ADRs).
- `DROP TABLE`.
- `CREATE TABLE AS SELECT (CTAS)`.
- `ALTER TABLE`.
- `CREATE INDEX`.
- Constraints: `PRIMARY KEY` / `FOREIGN KEY` / `CHECK`
  / column-level `UNIQUE`.
- `DEFAULT` clause.
- `CREATE VIEW`.
- Multi-statement scripts.
- Hidden-aggregate `ORDER BY` keys.
- Non-aggregate computed `ORDER BY` (ADR 027 D10).
- `DISTINCT` aggregate arguments.
- `FILTER (WHERE ...)` clause on aggregate calls.
- Window aggregates (`OVER`).
- Utf8 / Boolean / Decimal / Date `CAST` + `TRY_CAST`.
- HAVING alias visibility (ADR 023 D7).
- Sibling ADR 024 cycles: unary negation, modulo,
  `CASE`, `COALESCE` / `IFNULL` / `NULLIF`.
- Wendelon AGG consumer wiring (cross-ring; PBC
  partial-pushdown).
- Kernel graduations (`LinearRegress`,
  `ApproxQuantile`).
- Cross-ring metadata-write contract (Wendelon
  catalog admin — needed to lift
  `WendelonCatalog::create_table` from
  `AdminUnavailable`).
- Temporal types (DATE, TIMESTAMP) — `CREATE TABLE`
  type-lattice extension.
- DECIMAL — type-lattice extension.

## Definition of Done

- [ ] `Statement::CreateTable` arm in `SqlPlanner::plan`.
- [ ] `lower_create_table` exhaustively destructures
      `sqlparser::ast::CreateTable` without `..`.
- [ ] `lower_create_table_name` accepts 1- or 2-part
      names; rejects 3-part with stable diagnostic.
- [ ] `lower_create_table_data_type` DDL-specific
      6-type lattice; rejects bare FLOAT / DECIMAL /
      temporal / etc.
- [ ] `lower_create_table_column_nullability` validates
      NULL / NOT NULL only; rejects named constraints,
      DEFAULT, UNIQUE, CHECK, REFERENCES, GENERATED,
      COLLATE.
- [ ] Storage synthetic column name reject (D12) at
      planner level for the 5 Wendelon constants.
- [ ] Duplicate column names + empty schemas reject.
- [ ] `LogicalPlan::CreateTable { name, schema,
      if_not_exists }` leaf variant.
- [ ] `ExecutionPlan::CreateTable` twin variant +
      `schema()` returns `Arc::new(Schema::empty())`.
- [ ] `Catalog::create_table` async trait method
      returning `Result<TableBinding, CatalogError>`.
- [ ] `CatalogError::TableAlreadyExists` /
      `SchemaMismatch` / `AdminUnavailable` variants;
      existing `FingerprintResolution` reused for mock
      descriptor construction failures.
- [ ] `MockCatalog` shape: `RwLock<MockCatalogInner>`.
- [ ] `mock_table_identity` helper derives
      `PhysicalDescriptorFingerprint` via
      `TableSchemaDescriptor::new_with_layout`;
      `TableObjectId` deterministic from
      `local_table_id.0` 8-byte suffix.
- [ ] `MockCatalog::create_table` allocates fresh
      `TableId(value)` (tuple-struct construction, NOT
      `TableId::new`); skips existing keys; bumps
      `next_table_id` monotonically.
- [ ] MockCatalog reference insertion (D10): exact
      `TableRef` always inserted; unqualified alias
      via `entry(...).or_insert(...)` only.
- [ ] `WendelonCatalog::create_table` returns
      `AdminUnavailable`.
- [ ] `QueryCoordinator::execute_query` intercepts DDL
      BEFORE descriptor-evolution retry loop; returns
      `Ok(Vec::new())`.
- [ ] `DistributedQueryCoordinator::execute_query`
      intercepts DDL AFTER bind, BEFORE `fragment_plan`;
      returns `Ok(Vec::new())`.
- [ ] `FragmentError::CreateTableNotFragmentable`
      variant + top-level fragmenter reject.
- [ ] `Executor::execute_bound` defensive arm returns
      `NotImplemented`.
- [ ] Diagnostic strings use crate-private
      `table_name(&TableRef)` helper for `TableRef`
      formatting.
- [ ] **15** new ADR 029 architecture guards pass.
- [ ] **1** adjusted statement-level catch-all guard.
- [ ] Planner positive fixtures (6-type lattice; IF
      NOT EXISTS; NULL/NOT NULL; 1-part + 2-part
      names; VARCHAR(n) / CHAR(n) advisory length).
- [ ] Catalog fixtures (create + resolve; IF NOT
      EXISTS match + mismatch; TableAlreadyExists;
      WendelonCatalog AdminUnavailable; fingerprint-
      from-descriptor consistency; schema-qualified
      create does not overwrite existing unqualified
      alias).
- [ ] Coordinator fixtures (local + distributed;
      `Ok(Vec::new())` + resolvable binding; no
      scheduler dispatch for distributed DDL).
- [ ] Guardrail fixtures (Executor + fragmenter
      defensive rejects).
- [ ] Planner negative fixtures (empty schema,
      duplicate column, 3-part name, CTAS, LIKE,
      TEMPORARY, PRIMARY KEY, CHECK, REFERENCES,
      named constraint, DEFAULT, DECIMAL, bare
      FLOAT, TIMESTAMP, NULL/NOT NULL conflict,
      all 5 storage synthetic column names).
- [ ] Catch-all preservation (INSERT / DROP / ALTER /
      CREATE INDEX / CREATE VIEW continue to reject).
- [ ] Existing SELECT query corpus plan digests
      byte-identical pre/post-ADR-029.
- [ ] Roadmap commits ADR 030 = INSERT, ADR 031 =
      BoundExprProgram.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
