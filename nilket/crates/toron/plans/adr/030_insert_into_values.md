# ADR 030: SQL `INSERT INTO ... VALUES (...)` — Literal-Only DML

## Status

Promoted. Three-way council convergence (Claude / Codex
/ Gemini) reached across four iterations. Iter-0
selected the literal-INSERT topic per ADR 029's
D1 / D15 sequencing commitment. Iter-1 settled the
load-bearing shape decisions (`StorageWrite` separate
from `StorageScan`; `InsertLiteral` IR carrier; binder-
owned schema coercion; coordinator-resident INSERT
execution; distributed reject). Iter-2 corrected two
substrate items (Codex iter-2 self-correction:
`ExecutionPlan::Insert` carries `Vec<Vec<ScalarValue>>`
not `RecordBatch` — preserves serde derivation; Claude
iter-2 retraction: the `wendelon-client` wrapper crate
provides the friendlier `table_write` signature and
existing `From<ClientError> for StorageError`
conversion that iter-1 wrongly disputed). Iter-3
ratified convergence + locked architecture guard set.

**First DML ADR** (after ADR 029 = first DDL).
**Second ADR to integrate substantively with Wendelon-
defined types** (`wendelon_client::Client::table_write`,
`WriteOptions`, `TableWriteReceipt`, `TableSchemaDescriptor`).
Twelfth instance of the ADR 015 cycle (after 016 /
017 / 018 / 019 / 022 / 023 / 024 / 025 / 026 / 027 /
028 / 029) and second consecutive statement-layer
widening.

## Scale boundary

**Primary:** Toron SQL planner + binder + coordinator +
new `StorageWrite` boundary. Surface widens the
statement layer in `SqlPlanner::plan` to handle
`Statement::Insert`; adds `LogicalPlan::Insert` +
`ExecutionPlan::Insert` leaf variants; introduces
`StorageWrite` trait beside `StorageScan`; intercepts
DML in `QueryCoordinator` BEFORE the SELECT
descriptor-evolution retry loop, and rejects in
`DistributedQueryCoordinator` BEFORE `fragment_plan`.

In scope:

- `SqlPlanner::plan` widens to handle `Statement::Insert`.
- New `LogicalPlan::Insert` + `ExecutionPlan::Insert`
  leaf variants. Logical carries
  `Vec<Vec<InsertLiteral>>`; execution carries
  `Vec<Vec<ScalarValue>>` plus descriptor identity.
- New `pub enum InsertLiteral` carrier (`Null`,
  `Boolean(bool)`, `Int64(i64)`, `UInt64(u64)`,
  `Float64(f64)`, `Utf8(String)`).
- New `lower_insert` / `lower_insert_target_name` /
  `lower_insert_literal` helpers in
  `sql_planner.rs` — all DML-specific, not delegating
  to existing SELECT helpers.
- New `bind_insert` + `coerce_insert_literal` helpers
  in `binder.rs`.
- New `StorageWrite` trait + `WriteRequest` +
  `WriteOutcome` in `storage.rs`.
- New `InMemoryTableStorage` (additive; existing
  `InMemoryStorageScan` untouched) implementing both
  `StorageScan` and `StorageWrite`.
- New `WendelonStorageWrite` parallel to
  `WendelonStorageScan`; uses
  `wendelon_client::Client::table_write` wrapper.
- New `coordinator/write.rs` submodule with
  `materialize_insert_batch(schema, rows) ->
  Result<RecordBatch, StorageError>` private helper.
- `QueryCoordinator` gains `writer: Arc<dyn StorageWrite>`
  field with default `UnsupportedStorageWrite` sentinel;
  new `with_writer` constructor.
- `DistributedQueryCoordinator` rejects bound INSERT
  with stable diagnostic before `fragment_plan`.
- Defensive `Executor::execute_bound` +
  `fragment_plan` rejects.
- 12 new architecture guards + 2 adjusted ADR 029
  guards.
- `TABLE_WRITE_MAX_ROWS = 65_536` bind-time row cap.

Out of scope:

- `INSERT INTO ... SELECT ...`.
- `RETURNING`.
- `ON CONFLICT` / `UPSERT` / `INSERT OR REPLACE` /
  `INSERT IGNORE` / `REPLACE INTO` / `INSERT OVERWRITE`.
- `DEFAULT` value expressions; `GENERATED` columns;
  named constraints; dialect-specific clauses
  (assignments, partitions, settings, format clauses,
  priorities, aliases).
- Non-literal expressions in `VALUES` (arithmetic,
  casts, function calls, identifiers, placeholders,
  arrays, structs, intervals, dates).
- Multi-statement scripts.
- `UPDATE` / `DELETE` (DML siblings).
- Distributed INSERT execution (returns stable
  unsupported diagnostic; future ADR lifts after
  distributed-write context plumbing).
- Read-your-writes session HLC advancement
  (`WriteOutcome` discards Wendelon's `commit_hlc`;
  future ADR engages).
- Per-statement `durability` override (uses wrapper
  default `DurabilityClass::QUORUM`).
- Row-count or command-tag result batches (returns
  `Ok(Vec::new())`).
- Decimal / temporal / binary / array / struct / map /
  unsigned-integer target columns (bounded by ADR 029
  D11 six-type lattice).
- Idempotency / write-retry semantics across
  descriptor evolution.

## Context

ADR 029 added the `CREATE TABLE` statement but left a
chicken-and-egg situation: users could declare table
schemas in Toron-native SQL but could not populate
them. ADR 029 D14 explicitly forbade
`SELECT * FROM newly_created_table` fixtures for that
reason, deferring readback proofs to ADR 030. The
user-evidence framing ("create tables before going
super deep into approximate quantiles") only delivers
value when `CREATE TABLE; INSERT; SELECT` becomes
testable end-to-end.

ADR 030 is the **paired companion** to ADR 029 and the
**narrowest possible** DML widening. Like ADR 029, it
preserves SELECT plan-digest bytes (additive IR
variants), keeps the descriptor-evolution retry loop
SELECT-only, and intercepts the new statement at the
coordinator before downstream stages.

The load-bearing decisions are: **where do data-plane
writes execute** (new `StorageWrite` boundary vs
`Catalog` extension vs `Executor` injection), **what IR
shape carries values** (insert-specific `InsertLiteral`
vs `Expr` reuse vs Arrow `RecordBatch`), **who owns
Wendelon row identity** (Toron-side `__row_*__`
construction vs server-side Wendelon admission), **how
the distributed coordinator handles INSERT in v1**
(threading new write context vs stable unsupported
reject), and **what guarantees the SQL `INSERT`
statement gives about visibility** (`await_finalization`
default vs explicit override).

The council converged on a metadata/data-plane
separation that strictly preserves ADR 029's catalog-
is-metadata-only stance: `StorageWrite` is a new
trait beside `StorageScan`; `Catalog` gains no write
method; Wendelon owns row identity end-to-end (Toron
never constructs `__row_*__` columns and never depends
on `blake3`); the `wendelon-client` wrapper crate's
high-level `table_write` is the single Wendelon-side
call site; `await_finalization: true` makes the SQL
`INSERT` return only after commit confirmation (so the
signature `CREATE → INSERT → SELECT` fixture has read-
after-write semantics); distributed INSERT returns the
same kind of stable unsupported diagnostic ADR 029
used for `WendelonCatalog::create_table`.

## Decision

### D1 — Narrow SQL `INSERT` shape

The accepted SQL surface:

```sql
INSERT INTO [schema.]table_name [(column_name, ...)]
VALUES (literal, ...), ...
```

"Literal" means `NULL`, boolean, signed/unsigned
integer, floating-point number, single-quoted string,
or a simple unary-minus over a numeric literal. All
other DML forms, source shapes, target shapes, and
dialect extensions reject with stable diagnostics.
The `INTO` keyword is required; Hive-style
`INSERT INTO TABLE ...` is not part of ADR 030.

### D2 — Statement planner widens with exhaustive sqlparser destructure

`SqlPlanner::plan`'s top-level match adds a
`Statement::Insert` arm and narrows the existing
catch-all diagnostic to:

```text
unsupported SQL statement; only SELECT, CREATE TABLE, and INSERT are supported
```

The DML helper `lower_insert` destructures
`sqlparser::ast::Insert` **exhaustively without `..`**
— all 19 fields of the sqlparser 0.54 AST struct are
named. Unsupported fields reject with specific
diagnostics. This is the same compile-time guard
discipline ADR 029 D2 established for `CreateTable`
destructuring.

The `into` and `has_table_keyword` fields are named
and checked: `into` must be true, and
`has_table_keyword` must be false.

### D3 — Logical IR uses insert-specific `InsertLiteral`

`LogicalPlan::Insert` is an additive leaf variant. The
literal carrier is purpose-built; `Expr` is NOT reused
(`lower_expr` rejects bare `NULL`, parses integer
literals as `i32`-first, and is shaped for SELECT
predicates).

```rust
pub enum InsertLiteral {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
}

LogicalPlan::Insert {
    table: TableRef,
    columns: Option<Vec<String>>,   // None = all columns in schema order
    rows: Vec<Vec<InsertLiteral>>,
}
```

Same anti-delegation discipline as ADR 029 D11
(DDL type lattice did not delegate to
`lower_cast_target_type`).

### D4 — `ExecutionPlan::Insert` is serializable

`ExecutionPlan::Insert` is an additive leaf variant
carrying `Vec<Vec<ScalarValue>>`, NOT
`arrow::record_batch::RecordBatch`. Reason:
`ExecutionPlan` derives `Serialize + Deserialize`
(`ir/execution.rs:24`); `RecordBatch` is not serde-
derivable. The coordinator write helper materializes
the Arrow batch from `(schema, rows)` immediately
before `StorageWrite::write`.

```rust
ExecutionPlan::Insert {
    legacy_table_id: TableId,
    table: TableObjectId,
    fingerprint: PhysicalDescriptorFingerprint,
    schema: SchemaRef,
    rows: Vec<Vec<ScalarValue>>,
}
```

`ExecutionPlan::CreateTable.schema()` precedent
applies: `ExecutionPlan::Insert.schema()` returns
`Arc::new(Schema::empty())` — empty schema is metadata
for the control-plane result, NOT a promise of empty-
batch row output.

### D5 — `StorageWrite` is a new boundary beside `StorageScan`

The metadata/data-plane separation ADR 029 D4 / D5
established (catalog mutations on `Catalog`; row reads
on `StorageScan`) extends to: row writes on
`StorageWrite`. New trait:

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

Single-method trait; no `begin_write` / `append` /
`commit` streaming API in v1. `WriteOutcome` is
`#[non_exhaustive]` to keep the additive door open
(e.g., future `commit_hlc` for read-your-writes).

### D6 — `Catalog` stays metadata-only

`Catalog` gains no `insert_rows` / `insert` / `write_*`
method. ADR 029 D5 made catalog mutation the DDL
boundary; data-plane writes use `StorageWrite`. An
architecture guard pins the absence.

### D7 — Wendelon synthetic columns stay server-owned

Toron MUST NOT construct any of the five Wendelon
storage synthetic columns
(`__row_hlc__`, `__row_key_id__`,
`__row_routing_locality__`, `__row_table_object_id__`,
`__row_tombstone__`) for INSERT payloads. The
materialized `RecordBatch` contains only the
descriptor's user-visible Arrow schema. Wendelon's
admission and packing layer stamps storage metadata
server-side (see `wendelon/src/storage/multirow.rs`
`pack_admitted_record_batch_by_bucket*`).

Toron MUST NOT import or depend on `blake3` for row
identity. Wendelon's multi-row packer owns row identity
derivation per Wendelon ADR 045.

Architecture guards source-check
`coordinator/write.rs` for absence of `STORAGE_ROW_*`
references and `crates/toron/Cargo.toml` for absence of
`blake3` declaration.

### D8 — `WendelonStorageWrite` uses the `wendelon-client` wrapper

The high-level wrapper `wendelon_client::Client::table_write`
(`crates/wendelon/crates/wendelon-client/src/lib.rs:574`)
takes `(tenant_id, descriptor, payload, options) ->
Result<TableWriteReceipt, ClientError>` and constructs
the low-level `TableWriteRequest` + `TableWriteRequestMeta`
internally (handling `parent_write_id`, `client_hlc`,
`expected_epoch`, `schema_generation`, row-count
encoding, etc.).

Toron uses ONLY the wrapper. Architecture guard
asserts absence of any `TableWriteRequest {`
construction in `crates/toron/src/`.

`WendelonStorageWrite` mirrors `WendelonStorageScan`
shape:

```rust
pub struct WendelonStorageWrite {
    client: Arc<wendelon_client::Client>,
    descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptor>,
}
```

Write call:

```rust
self.client
    .table_write(
        wendelon::schema::TenantId::new(request.tenant_id.0),
        descriptor,
        request.payload,
        wendelon_client::WriteOptions {
            await_finalization: true,
            ..Default::default()
        },
    )
    .await
    .map_err(StorageError::from)?
```

`TenantId::new(request.tenant_id.0)` is the exact
conversion `WendelonStorageScan` uses
(`crates/toron/src/storage.rs:206`).

### D9 — `await_finalization: true` for SQL INSERT

The `wendelon-client` `WriteOptions::default()` sets
`await_finalization: false`. Toron's SQL INSERT
overrides to `true`. Reason: the user-evidence payoff
fixture is `CREATE TABLE; INSERT; SELECT`. With
`await_finalization: false`, the INSERT returns before
Wendelon confirms commit; a subsequent SELECT could
race against the leader's replication. SQL `INSERT`
semantics require commit confirmation; the override
makes that contract explicit.

Architecture guard pins the literal
`await_finalization: true` in
`WendelonStorageWrite::write`.

### D10 — Error mapping mirrors `WendelonStorageScan`

`WendelonStorageWrite::write` error mapping:

- Missing `TableId` in descriptor map →
  `StorageError::TableNotFound`.
- `table_object_id` or
  `physical_descriptor_fingerprint` mismatch →
  `StorageError::DescriptorEvolved`.
- `descriptor.arrow_schema()` decoder failure →
  `StorageError::Internal { message: ... }`.
- Payload schema does not match descriptor Arrow
  schema → `StorageError::Internal { message: ... }`.
- `wendelon_client::ClientError` propagation via the
  existing `impl From<wendelon_client::ClientError>
  for StorageError` (`crates/toron/src/error.rs:48`)
  — no new mapping required.

### D11 — `InMemoryTableStorage` additive

The existing `pub struct InMemoryStorageScan {
table_data: RecordBatch }` (`storage.rs:96`) is read-
only and per-instance, used by ADR 029 and earlier
fixtures. ADR 030 does NOT refactor it. Instead, a new
`InMemoryTableStorage` implements both `StorageScan`
and `StorageWrite` over shared interior state:

```rust
struct InMemoryTableData {
    table_object_id: TableObjectId,
    physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
    batch: RecordBatch,
}

pub struct InMemoryTableStorage {
    tables: Mutex<HashMap<TableId, InMemoryTableData>>,
}
```

First write seeds a table; subsequent writes verify
identity + schema and append via
`arrow::compute::concat_batches`. Scan with no prior
write returns `StorageError::TableNotFound`. Multi-
table support is required for the
`CREATE TABLE; INSERT; SELECT` signature fixture.

### D12 — Local `QueryCoordinator` owns INSERT execution

`QueryCoordinator::execute_query` intercepts
`LogicalPlan::Insert` **before** the descriptor-
evolution retry loop. SELECT retry semantics stay
byte-identical.

Flow:

1. Bind `LogicalPlan::Insert` → `ExecutionPlan::Insert`.
2. `coordinator/write.rs::materialize_insert_batch(schema, rows)`
   produces a user-schema `RecordBatch`.
3. Build `WriteRequest` from
   `executor.context()` (new `Executor::context()`
   accessor) + plan descriptor identity.
4. `self.writer.write(request).await?`.
5. Return `Ok(Vec::new())`.

`QueryCoordinator` gains `writer: Arc<dyn StorageWrite>`.
Default constructor wires a private
`UnsupportedStorageWrite` sentinel that returns
`StorageError::Internal { message: "INSERT execution
requires a configured StorageWrite" }`. New
`with_writer` constructor for tests + server wiring.

INSERT does NOT retry on descriptor evolution in v1;
write-retry semantics defer to a future ADR.

### D13 — Distributed INSERT returns stable unsupported diagnostic

`DistributedQueryCoordinator::execute_query` binds
INSERT (so normal frontend / binding diagnostics
surface) then returns a stable unsupported error
**before** `fragment_plan`. Mirrors ADR 029's
`WendelonCatalog::create_table → AdminUnavailable`
pattern.

Distributed INSERT cannot reach `fragment_plan`,
exchange counting, scheduler dispatch, or executor
fragment execution.

### D14 — Defensive executor + fragmenter rejects

Even with coordinator interception, lower layers fail
closed:

- `Executor::execute_bound` rejects
  `ExecutionPlan::Insert` with
  `ExecutionError::NotImplemented { feature: "INSERT
  execution is coordinator-only" }`. The test-only
  unbound `Executor::execute` helper, where present,
  also rejects `LogicalPlan::Insert`.
- `FragmentError::InsertNotFragmentable` variant added;
  `fragment_plan` top-level reject runs BEFORE
  exchange counting or join detection.
- Plan walkers in `fragmenter`, `scheduler`,
  `server/mod.rs`, and test helpers add `Insert` arms
  as leaf / `None` / `false` / `"insert"`.

### D15 — `TABLE_WRITE_MAX_ROWS = 65_536` bind-time cap

Toron rejects `VALUES` lists with more than 65,536
rows at bind time, matching Wendelon's
`wendelon/src/schema/table.rs:46` `TABLE_WRITE_MAX_ROWS`
constant. Bind-time enforcement keeps in-memory and
Wendelon-backed semantics aligned; a stable SQL
diagnostic surfaces before any payload
materialization. Toron does not invent a smaller cap.

Empty `VALUES` lists also reject at bind time.

### D16 — Materialization in `coordinator/write.rs`

Scalar-row-to-Arrow conversion lives in a new private
submodule:

```text
crates/toron/src/coordinator/write.rs
```

Declared in `coordinator.rs` via `mod write;`. The
private `materialize_insert_batch(schema: &SchemaRef,
rows: &[Vec<ScalarValue>]) -> Result<RecordBatch,
StorageError>` helper builds one Arrow array per
schema field, preserves table column order, and emits
ONLY user columns (no synthetic). Arrow failures map
through `StorageError`.

Rationale for not putting it in `storage.rs`: the
coordinator write path already has the
`ExecutionContext` needed to construct `WriteRequest`,
while `storage.rs` should stay shape-neutral and deal
only in storage requests, streams, and errors.

## Reject removal (ADR 015 cycle's twelfth instance)

The existing statement-level catch-all narrows:

```text
unsupported SQL statement; only SELECT and CREATE TABLE are supported
```

becomes

```text
unsupported SQL statement; only SELECT, CREATE TABLE, and INSERT are supported
```

Structural rejects preserved or added:

- `UPDATE` / `DELETE` / `ALTER` / `DROP` /
  `CREATE INDEX` / `CREATE VIEW` /
  `CREATE TABLE AS SELECT` continue to reject via the
  narrowed catch-all (ADR 029 carry-forward).
- Per-clause INSERT rejects (D2 exhaustive
  destructure): every unsupported sqlparser field
  rejects with a specific diagnostic
  (`RETURNING`, `ON CONFLICT`, `REPLACE`,
  `INSERT IGNORE`, `OVERWRITE`, omitted `INTO`,
  Hive-style `TABLE` keyword, table function targets,
  partitions, assignments, settings, format clauses,
  priority, aliases, etc.).
- 3-part name reject (D3 helper mirrors ADR 029 D9).
- Source-shape rejects (D2): only
  `Some(Query)` with `SetExpr::Values`,
  `!values.explicit_row`, non-empty rows.
- Literal-shape rejects (D3): identifiers, function
  calls, arithmetic, casts, placeholders, arrays,
  structs, intervals, dates.
- Binder rejects (D3 / D15): duplicate target
  columns, unknown target columns, row arity
  mismatches, NULL into NOT NULL columns, omitted NOT
  NULL columns without default, type-mismatch
  coercions, integer overflow on target type, empty
  `VALUES`, row count > 65,536.
- Distributed INSERT reject (D13).
- Defensive `Executor` + fragmenter rejects (D14).

New errors added:

- `FragmentError::InsertNotFragmentable`.

No new `CatalogError` variants (`Catalog` is
metadata-only per D6).

No new `StorageError` variants (existing
`TableNotFound`, `DescriptorEvolved`, `Internal`,
`Arrow`, `Client { source }`, and the existing
`From<wendelon_client::ClientError>` impl cover all
write-path failures).

## Architecture guard adjustments

| Guard | Status |
|---|---|
| ADR 029 `adr029_sql_planner_accepts_only_select_and_create_table` | **Adjusted.** Catch-all diagnostic widens to `"only SELECT, CREATE TABLE, and INSERT are supported"`; both `Statement::Query`, `Statement::CreateTable`, `Statement::Insert` arms required. |
| ADR 029 `architecture_guards_unchanged_plus_sort_variant` | **Adjusted.** `LogicalPlan` variant count 10 → 11 with `Insert {` assertion; `ExecutionPlan` variant count 8 → 9 with `Insert {` assertion. |
| **NEW** `adr030_insert_destructure_is_exhaustive` | Source-reads `lower_insert` body for representative rejected fields (`into`, `has_table_keyword`, `source`, `returning`, `on`, `replace_into`, `overwrite`, `partitioned`, `assignments`, `format_clause`) + absence of `..` rest pattern. |
| **NEW** `adr030_logical_plan_insert_uses_insert_literal` | Source-reads `LogicalPlan::Insert` for `rows: Vec<Vec<InsertLiteral>>` field; asserts absence of `Vec<Vec<Expr>>` parallel shape. |
| **NEW** `adr030_execution_plan_insert_has_no_record_batch` | Source-reads `ExecutionPlan::Insert` variant body for the full field shape (`legacy_table_id`, `table`, `fingerprint`, `schema`, `rows: Vec<Vec<ScalarValue>>`); asserts absence of `RecordBatch` and `payload`. |
| **NEW** `adr030_catalog_has_no_insert_method` | Source-reads `Catalog` trait body for absence of `insert` / `write_` method names. |
| **NEW** `adr030_no_storage_synthetic_column_construction_in_toron` | Source-reads `coordinator/write.rs` for absence of any of the five `STORAGE_ROW_*` constants (the planner-side ADR 029 D12 reject in `sql_planner.rs` is the only legitimate reference). |
| **NEW** `adr030_no_blake3_in_toron_insert_path` | Source-reads `crates/toron/Cargo.toml` for absence of `blake3` dependency declaration; source-reads `coordinator/write.rs` for absence of `blake3` references. |
| **NEW** `adr030_coordinator_insert_uses_wendelon_client_wrapper` | Source-reads `WendelonStorageWrite::write` for `self.client.table_write(`; source-reads `crates/toron/src/` for absence of `TableWriteRequest {` construction. |
| **NEW** `adr030_write_options_set_await_finalization_true` | Source-reads `WendelonStorageWrite::write` for literal `await_finalization: true`. |
| **NEW** `adr030_wendelon_storage_write_mirrors_scan_error_mapping` | Source-reads `WendelonStorageWrite::write` for both `StorageError::TableNotFound` (missing descriptor) and `StorageError::DescriptorEvolved` (identity mismatch). |
| **NEW** `adr030_local_coordinator_intercepts_insert_before_retry_loop` | Source-reads `QueryCoordinator::execute_query` for `LogicalPlan::Insert` branch BEFORE `let mut retried_descriptor_evolution`. |
| **NEW** `adr030_distributed_coordinator_rejects_insert_before_fragment_plan` | Source-reads `DistributedQueryCoordinator::execute_query` for `ExecutionPlan::Insert` reject BEFORE `fragment_plan(`. |
| **NEW** `adr030_executor_and_fragmenter_reject_insert` | Source-reads `Executor::execute_bound` coordinator-only diagnostic + `FragmentError::InsertNotFragmentable` + `fragment_plan` top-level reject before exchange counting. |

Total: **12 new + 2 adjusted** architecture guards.

ADR 029's `adr029_select_retry_loop_is_preserved`
guard remains in force without modification — it pins
the same invariant ADR 030 D12 relies on.

## SoA-readiness check

ADR 021's four invariants preserved:

1. Catalog stays pure metadata for kernels; INSERT
   writes do not engage kernel state.
2. `#[repr(u8)]` on `StableAggregateKernel` unchanged.
3. Centralized dispatch unchanged — DML doesn't engage
   per-row dispatch.
4. No row-path dyn dispatch.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Untouched — DML
  doesn't engage the row-layer kernel path.
- **Brainstorm 002 (WeakControlRing).** Touches the
  seam preserved; coordinator-only execution +
  distributed-reject (D12 / D13) keeps WeakControlRing
  consensus-ownership story open without baking
  topology assumptions.
- **Brainstorm 003 (Disaggregated Compute).** Touches
  the seam preserved; coordinator-only execution +
  fragmenter rejection preserves `NodeRoleSet` routing
  semantics.
- **Brainstorm 004 (Memory Arenas).** Untouched at the
  expression-IR level. New IR variants use
  `Vec<Vec<InsertLiteral>>` (logical) and
  `Vec<Vec<ScalarValue>>` (execution) — no `Box<T>`
  chain. ADR 031 sequencing commitment (ADR 029 D1 /
  D15) keeps Brainstorm 004 near-term.

No seam closes.

## What Not To Do

- Don't put row writes on `Catalog` (D6).
- Don't append `__row_*__` synthetic columns in Toron
  (D7); Wendelon owns row identity end-to-end.
- Don't import or depend on `blake3` in Toron INSERT
  code (D7).
- Don't construct `TableWriteRequest` /
  `TableWriteRequestMeta` in Toron (D8); use the
  `wendelon-client` wrapper.
- Don't set `WriteOptions::default()` for SQL INSERT
  (D9); override `await_finalization: true`.
- Don't put materialization in `storage.rs` (D16);
  use `coordinator/write.rs`.
- Don't put `RecordBatch` in any `ExecutionPlan`
  variant (D4); breaks serde derivation.
- Don't reuse `Expr` for INSERT VALUES (D3); use
  `InsertLiteral`.
- Don't reuse SELECT-side `lower_expr` for VALUES
  lowering (D3); same anti-delegation pattern as ADR
  029 D11.
- Don't refactor `InMemoryStorageScan` callers (D11);
  add `InMemoryTableStorage`.
- Don't bundle distributed INSERT support in this
  cycle (D13); future cross-ring-write-context ADR.
- Don't retry INSERT on descriptor evolution (D12).
- Don't consume Wendelon's `commit_hlc` to advance
  session read bounds (out of scope; future read-
  your-writes ADR).
- Don't expose `commit_hlc` through Toron's
  `HlcBound(pub u64)` (D5 — lossy; future ADR may
  define a richer Hlc type).
- Don't expand to `INSERT ... SELECT` /
  `RETURNING` / `ON CONFLICT` / `UPDATE` / `DELETE`
  at v1.
- Don't accept user-defined column names that match
  Wendelon storage synthetic-column-name constants
  (D7); ADR 029 D12 reject is layered protection.
- Don't break plan-digest backward compat for the
  SELECT query corpus (D4 additive).
- Don't move binder out of `QueryCoordinator`'s
  descriptor-evolution retry loop for SELECT (ADR
  029 D15 carry-forward).
- Don't change `ExecutionContext.read_bound` to a
  write-side name (out-of-scope rename; comment in
  `WendelonStorageWrite` covers the dual-use).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | INSERT slips into SELECT retry loop | D12 branch BEFORE the loop; `adr029_select_retry_loop_is_preserved` carries forward. |
| R2 | Toron-side construction of Wendelon `__row_*__` columns | D7 + architecture guard `adr030_no_storage_synthetic_column_construction_in_toron` + `adr030_no_blake3_in_toron_insert_path`. |
| R3 | `wendelon-client` wrapper vs low-level `WendelonClient` confusion | D8 architecture guard `adr030_coordinator_insert_uses_wendelon_client_wrapper`; impl-phase Phase 0 audit verifies. |
| R4 | `ExecutionPlan` becomes non-serializable | D4 + architecture guard `adr030_execution_plan_insert_has_no_record_batch` pins field shape. |
| R5 | Read-after-write race in CREATE/INSERT/SELECT fixture | D9 `await_finalization: true`; architecture guard pins literal. |
| R6 | Distributed INSERT reaches fragmenter | D13 distributed coordinator reject before `fragment_plan`; D14 fragmenter top-level reject. |
| R7 | Catalog grows write methods | D6 + architecture guard `adr030_catalog_has_no_insert_method`. |
| R8 | sqlparser dependency upgrade introduces new Insert fields silently | D2 exhaustive 19-field destructure without `..`; compile-time guard. |
| R9 | Coordinator constructor churn breaks existing call sites | D12 default `UnsupportedStorageWrite` sentinel keeps existing callers compiling. |
| R10 | INSERT into nonexistent table surfaces confusing diagnostic | Binder `Catalog::resolve_table` propagates the existing `TableNotFound` class. |
| R11 | Row cap divergence between in-memory and Wendelon backends | D15 bind-time enforcement of `TABLE_WRITE_MAX_ROWS = 65_536`; matches Wendelon constant. |
| R12 | `InMemoryStorageScan` callers broken by storage refactor | D11 additive — `InMemoryStorageScan` untouched. |
| R13 | Coercion errors lose column context | Binder `coerce_insert_literal` carries `Field` reference; diagnostics include column name. |
| R14 | Integer overflow on `INT` target accepted silently | Binder `coerce_insert_literal` uses `i64.try_into::<i32>()`; overflow rejects with stable diagnostic. |
| R15 | Float NaN/inf inconsistency vs Cast lattice | Mirror Arrow native semantics in v1 (no finite-only check); future ADR may add. |
| R16 | Architecture guard count growth | 12 new + 2 adjusted; substrate-touching but narrower than ADR 029. |
| R17 | Plan-digest regression for SELECT corpus | `LogicalPlan` / `ExecutionPlan` additive variants. |
| R18 | `RecordBatch`-in-IR drift on future maintenance | D4 + architecture guard pins field-by-field shape (not just absence of `RecordBatch`). |
| R19 | `await_finalization: false` slips back in via refactor | D9 architecture guard pins literal in source. |
| R20 | `Catalog::insert_*` slips back in via refactor | D6 architecture guard. |
| R21 | Wendelon-side identity stamping diverges silently | D8 wrapper + descriptor-pinned identity verification in `WendelonStorageWrite`. |

## Recorded dissent

**None at iter-3.**

Gemini iter-1 / iter-2 proposed `Catalog::insert_rows`
(D5), Toron-side `__row_*__` construction (D6), and
BLAKE3 row identity derivation (D7). Gemini iter-3
formally retracted all three. This is the **fifth
consecutive cycle** where Gemini's iter-1 / iter-2
broader framings retracted at a later iter
(extending the 4-of-4 streak from ADR 025 / 026 / 028
/ 029 to 5-of-5).

Claude iter-1 C1 and C4 incorrectly disputed Codex
iter-0 E2 — Claude iter-1 had read the low-level
`wendelon::client::WendelonClient::table_write`
(`crates/wendelon/src/client/core.rs:447`) rather
than the `wendelon-client` wrapper Toron actually
imports (`crates/wendelon/crates/wendelon-client/src/lib.rs:574`).
Claude iter-2 retracted both. Recording as the first
cycle where Claude iter-1 substantive code-evidence
items needed iter-2 corrections.

## Process observations

### Substrate-touching cadence with narrow flip-flop profile

ADR 030 took 4 iterations with **one substantive
flip-flop** (Codex iter-1 self-correction:
`RecordBatch` → `Vec<Vec<ScalarValue>>` in
`ExecutionPlan::Insert`) and **two iter-2 retractions
from Claude** (C1 wrapper crate path, C4 existing
`From<ClientError>` impl).

| ADR | Substrate-touching? | Iterations | Flip-flops |
|---|---|---|---|
| ADR 023 | Yes | 4 + consolidation | 1 |
| ADR 024 | Yes | 4+ | Multi |
| ADR 028 | Yes | 5 | 2 |
| ADR 029 | Yes (statement layer) | 4 | 0 |
| **ADR 030** | **Yes (statement layer, DML)** | **4** | **1 (Codex iter-1 self-correction)** |

ADR 030's narrowness held the iter count to 4 despite
being the cycle's first DML and second Wendelon-types-
integrating ADR. Topic-concession-compresses-next-cycle
pattern predicted by ADR 029's process notes confirmed:
because ADR 029 D1 / D15 sequenced ADR 030 explicitly,
the topic question never reopened.

### Pre-existing-Wendelon-types ADR pattern (second instance)

ADR 029 was the first ADR to integrate substantively
with Wendelon-defined types
(`TableObjectId`, `PhysicalDescriptorFingerprint`,
`TableSchemaDescriptor`, `RowIdentityPolicy`,
`LocalityPolicy`). ADR 030 is the second
(`wendelon_client::Client::table_write`,
`WriteOptions`, `TableWriteReceipt`,
`TableSchemaDescriptor::arrow_schema`).

**New cycle property:** cross-ring ADRs need **two-pass
Wendelon-side verification** — at least one iteration
with surface-reading the relevant API and at least one
peer iter to catch wrong-file or wrong-import errors.
ADR 029 had this with iter-3 surfacing
`PhysicalDescriptorFingerprint::from_bytes` is crate-
private. ADR 030 had this with Claude iter-2
retracting C1 / C4 (wrong source file). Future cross-
ring ADRs should budget the wrong-import-discovery
peer iter explicitly.

### Gemini iter-1/2 retraction streak now 5-of-5

| ADR | Gemini iter-1/2 position | Iter-2/3 retraction |
|---|---|---|
| ADR 025 | Arrow-kernel-delegation (broader scope) | Manual per-pair kernels |
| ADR 026 | `null_on_failure: bool` flag on strict kernels | Separate `try_cast_*` kernels |
| ADR 028 | Public manual `Eq + Hash` on IR | Planner-private signature key |
| ADR 029 | Executor owns DDL + `Catalog::create_table` returns `()` + reuse SELECT helpers | Coordinator owns DDL + `TableBinding` return + DDL-specific helpers |
| **ADR 030** | **`Catalog::insert_rows` + Toron `__row_*__` construction + Toron BLAKE3 identity** | **iter-3: three explicit retractions; metadata/data-plane boundary preserved** |

Five-of-five confirmed pattern. Stable cycle property:
Gemini iter-1/2 broader framings surface the
alternate-design space the council needs to reject
explicitly. Useful for the council's collective
understanding but should not be load-bearing for
promotion.

### Iter-3 final-pass discipline

Both Codex iter-3 and Claude iter-3 documents were
ratification + small precision additions
(WendelonStorageWrite error mapping mirror; field-
shape pin tightening; Cargo.toml blake3 guard;
variant-count guard updates). No new substrate. Iter-3
final-pass pattern stable: at least one iter where
documents converge on promotion shape, with
remaining work being source anchors and housekeeping.

## Future work

ADR 031+ candidates:

- **ADR 031: Bound Expression Program** (Codex's
  narrow Brainstorm 004 realization). Sequencing
  committed in ADR 029 D1 / D15.
- `INSERT INTO ... SELECT ...` (composes 029 + 030 +
  read-side ADRs).
- `RETURNING` clause.
- `UPDATE` / `DELETE`.
- `ON CONFLICT` / `UPSERT`.
- `DROP TABLE` / `ALTER TABLE` / `CREATE INDEX` /
  `CREATE VIEW` / CTAS — DDL siblings.
- Constraints (PRIMARY KEY / FOREIGN KEY / CHECK /
  column-level UNIQUE) / DEFAULT / GENERATED columns
  — DDL siblings to ADR 029.
- Temporal column types (DATE, TIMESTAMP) / DECIMAL —
  ADR 029 type-lattice extension.
- Cross-ring metadata-write contract ADR — needed to
  lift `WendelonCatalog::create_table` from
  `AdminUnavailable`.
- Distributed-write-context ADR — needed to lift
  ADR 030 D13's distributed-INSERT reject.
- Read-your-writes session HLC advancement ADR —
  defines whether coordinators consume
  `TableWriteReceipt.commit_hlc`.
- Richer Toron `Hlc` type — needed for read-your-
  writes ADR.
- Idempotency / write-retry-after-descriptor-evolution
  ADR.
- Per-statement `durability` override.
- Wendelon AGG consumer wiring (cross-ring; PBC
  partial-pushdown).
- Hidden-aggregate `ORDER BY` keys; non-aggregate
  computed `ORDER BY` (ADR 027 D10).
- HAVING alias visibility (ADR 023 D7).
- `DISTINCT` aggregate arguments; `FILTER (WHERE ...)`
  clause on aggregate calls; window aggregates (`OVER`).
- Utf8 / Boolean / Decimal / Date `CAST` + `TRY_CAST`.
- Sibling ADR 024 cycles: unary negation, modulo,
  `CASE`, `COALESCE` / `IFNULL` / `NULLIF`.
- Kernel graduations (`LinearRegress`,
  `ApproxQuantile`).

## Definition of Done

- [ ] `Statement::Insert` arm in `SqlPlanner::plan`.
- [ ] `lower_insert` exhaustively destructures
      `sqlparser::ast::Insert` (19 fields) without
      `..`.
- [ ] `lower_insert_target_name` accepts 1- or 2-part
      names; rejects 3-part with stable diagnostic;
      rejects `TableObject::TableFunction`.
- [ ] `lower_insert_literal` accepts the 6
      `InsertLiteral` variants; rejects identifiers,
      function calls, arithmetic, casts, placeholders,
      arrays, structs, intervals, dates.
- [ ] `pub enum InsertLiteral` with 6 variants.
- [ ] `LogicalPlan::Insert { table, columns:
      Option<Vec<String>>, rows: Vec<Vec<InsertLiteral>> }`
      leaf variant.
- [ ] `ExecutionPlan::Insert { legacy_table_id, table,
      fingerprint, schema, rows: Vec<Vec<ScalarValue>> }`
      leaf variant; `schema()` returns
      `Arc::new(Schema::empty())`.
- [ ] `bind_insert` resolves target, validates row
      cap, enforces column-list resolution, fills
      typed NULL, rejects omitted NOT NULL, coerces
      literals.
- [ ] `coerce_insert_literal` covers ADR 029 six-type
      lattice with overflow + type-mismatch
      diagnostics including column name.
- [ ] `TABLE_WRITE_MAX_ROWS = 65_536` bind-time row
      cap.
- [ ] `pub struct WriteRequest`,
      `#[non_exhaustive] pub struct WriteOutcome { pub rows: usize }`,
      `pub trait StorageWrite` in `storage.rs`.
- [ ] `pub struct InMemoryTableStorage` implements
      both `StorageScan` and `StorageWrite`;
      multi-table support.
- [ ] `pub struct WendelonStorageWrite` parallel to
      `WendelonStorageScan`; uses
      `wendelon_client::Client::table_write` wrapper.
- [ ] `WriteOptions { await_finalization: true,
      ..Default::default() }` literal in
      `WendelonStorageWrite::write`.
- [ ] `WendelonStorageWrite::write` error mapping
      mirrors `WendelonStorageScan` (TableNotFound
      missing-descriptor, DescriptorEvolved
      identity-mismatch, Internal Arrow/schema
      failure).
- [ ] No `TableWriteRequest` construction in
      `crates/toron/src/`.
- [ ] No `blake3` declaration in
      `crates/toron/Cargo.toml`.
- [ ] No `STORAGE_ROW_*` references in
      `coordinator/write.rs`.
- [ ] `coordinator/write.rs::materialize_insert_batch`
      private helper; user-columns only.
- [ ] `coordinator.rs` declares `mod write;`.
- [ ] `Executor::context()` accessor added.
- [ ] `QueryCoordinator` gains `writer: Arc<dyn StorageWrite>`
      with private `UnsupportedStorageWrite` default;
      new `with_writer` constructor; intercepts
      `LogicalPlan::Insert` BEFORE descriptor-
      evolution retry loop; returns `Ok(Vec::new())`.
- [ ] `DistributedQueryCoordinator` binds INSERT and
      returns stable unsupported diagnostic BEFORE
      `fragment_plan`.
- [ ] `Executor::execute` + `execute_bound` defensive
      arms return `NotImplemented`.
- [ ] `FragmentError::InsertNotFragmentable` variant +
      `fragment_plan` top-level reject.
- [ ] Plan walkers updated in `binder.rs`,
      `fragment.rs`, `scheduler.rs`,
      `server/mod.rs`, and test helpers.
- [ ] **12 new** ADR 030 architecture guards pass.
- [ ] **2 adjusted** ADR 029 guards still pass
      (catch-all diagnostic, variant counts).
- [ ] Existing 15 ADR 029 architecture guards still
      pass.
- [ ] Planner positive fixtures (full-column, multi-
      row, schema-qualified target, reordered column
      list, NULL literal, negative numeric literal).
- [ ] Planner negative fixtures (INSERT ... SELECT,
      RETURNING, ON CONFLICT, REPLACE, INSERT IGNORE,
      OVERWRITE, table function target, 3-part name,
      expression in VALUES, function call in VALUES,
      unsupported literal type, empty VALUES).
- [ ] Binder positive fixtures (column reorder,
      typed-NULL fill, all 6 ADR 029 target types).
- [ ] Binder negative fixtures (duplicate column,
      unknown column, arity mismatch, omitted NOT
      NULL, NULL into NOT NULL, integer overflow,
      type mismatch, row cap exceeded).
- [ ] Storage fixtures (`InMemoryTableStorage` seed,
      append, schema-mismatch reject, identity-
      mismatch reject, missing-table scan).
- [ ] Coordinator fixtures (local INSERT with writer,
      default unsupported writer diagnostic,
      distributed INSERT reject).
- [ ] Guardrail fixtures (Executor + fragmenter
      defensive rejects).
- [ ] **End-to-end `CREATE TABLE; INSERT; SELECT`
      round-trip fixture** — ADR 030's signature
      deliverable; lifts ADR 029 D14.
- [ ] Catch-all preservation (UPDATE / DELETE /
      ALTER / DROP / CREATE INDEX / CREATE VIEW
      continue to reject).
- [ ] Existing SELECT query corpus plan digests
      byte-identical pre/post-ADR-030.
- [ ] Roadmap updates ADR 030 entry from "candidate"
      to "promoted"; rewrites the stale candidate text
      that mentions `Catalog::insert_rows` and
      Toron-side `__row_*__` population.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
