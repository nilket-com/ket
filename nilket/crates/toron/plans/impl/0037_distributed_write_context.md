# Implementation Plan: ADR 037 — Distributed Write Context

**Companion ADR:** `plans/adr/0037_distributed_write_context.md`

## Goal

Implement the distributed-write-context substrate
by lifting four distributed-DML rejects plus the
Wendelon CREATE admin reject when Plan A lands, and wiring
the distributed coordinator to call the same
mutation driver as the local coordinator while
remaining the sole component that mutates catalog
or storage:

- distributed `INSERT ... VALUES` (no source read);
- distributed `INSERT ... SELECT` (source plan
  fragmented; one coordinator-side write);
- distributed `CREATE TABLE AS SELECT` (source
  plan fragmented; catalog create + one
  coordinator-side write);
- distributed `DELETE` (read-only candidate plan
  fragmented; one coordinator-side `delete_rows`);
- Wendelon `WendelonCatalog::create_table` via
  `Client::submit_table_schema_update` (Plan A) or
  preserved-as-`AdminUnavailable` (Plan B
  contingency).

Substrate adds:

- `DistributedQueryCoordinator` becomes context-
  bearing;
- `TaskExecutionContext` (4 fields:
  `session_id`, `tenant_id`, `read_bound`,
  `trace_context`) carried in every `TaskSpec`;
- `ExecutionPlan::Scan.row_identity:
  RowIdentityProjection` field (Append for internal
  DELETE candidate plans; None for SQL scans);
- private `MutationSourceExecutor` async trait
  seam with local + distributed implementations;
- shared private mutation driver invoked by both
  local and distributed coordinators (byte-
  identical refactor invariant pre / post for ADR
  030 / 032 / 033 / 036 fixtures);
- `DistributedQueryCoordinator` explicit
  writer/deleter sink wiring (unsupported adapters
  in read-only deployments).

Substrate preserves:

- `WriteRequest` / `DeleteRowsRequest` storage
  request shapes unchanged;
- SQL-invisible row identity (`__row_key_id__` /
  `__row_hlc__` synthetic-name rejects from ADR
  029 D12 + ADR 030 D7 + ADR 035 D11 + ADR 036
  guard #22 remain green);
- fragmenter defensive `*NotFragmentable` rejects
  for raw DML / catalog mutation plans;
- bind-once / mutate-once invariant: task retry
  never becomes write retry;
- non-transactional CTAS partial-failure semantics
  (ADR 033 D7 carry-forward; no compensation);
- Wendelon `DROP TABLE` AdminUnavailable (deferred
  to ADR 038 per D14).

No new SQL surface; no new aggregate kernel; no
PBC opcode change; no new `StorageWrite` /
`StorageDeleteRows` trait method; no new
`StorageError` variant; no new public field on
`WriteRequest` or `DeleteRowsRequest`; no Wendelon
storage reclaim trait or helper.

**Nine-phase patch order.** Phase 0 is preflight
audit + reference fixture pass-count capture for
byte-identical refactor verification; Phase 1
makes the distributed coordinator context-bearing
with explicit writer/deleter sinks; Phase 2 adds
`TaskExecutionContext` + serde + fragmenter
stamping + worker `ExecutionContext` construction;
Phase 3 extends `ExecutionPlan::Scan` with
`row_identity`; Phase 4 extracts the shared
mutation driver behind `MutationSourceExecutor`
async trait with byte-identical local-path
preservation; Phase 5 routes distributed
`INSERT VALUES` / `INSERT SELECT` / CTAS through
the shared driver; Phase 6 wires distributed
DELETE candidate reads + identity extraction;
Phase 7 lands Wendelon CREATE Plan A or preserves
Plan B; Phase 8 adds the 25 ADR-037 architecture
guards; Phase 9 is full verification.

## Implementation constraints

- **D1:** Workers are read/shuffle executors
  only. The distributed coordinator is the only
  caller of `Catalog::create_table`,
  `StorageWrite::write`, and
  `StorageDeleteRows::delete_rows`.
- **D2:** `DistributedQueryCoordinator` is
  context-bearing in production. Test / read-only
  construction may use an explicit test helper or
  unsupported sinks but must not pass through the
  production execution path.
- **D3:** `TaskExecutionContext` has exactly four
  fields: `session_id: SessionId`, `tenant_id:
  TenantId`, `read_bound: HlcBound`,
  `trace_context: Option<storage::TraceContext>`.
  No
  receipt / client-write / durability /
  finalization / retry / runtime-filter / row-
  identity / table-pin / telemetry / spill fields.
- **D3 (worker construction):** worker
  `ExecutionContext.query_id =
  spec.identity.query_id`. Production worker
  paths do not call `ExecutionContext::local_test()`.
- **D4:** `WriteRequest` and `DeleteRowsRequest`
  field counts unchanged. No `client_write_id` /
  `durability` / `await_finalization` public
  fields added.
- **D5:** `DistributedQueryCoordinator` has
  writer/deleter fields + `with_writer` /
  `with_writer_and_deleter` constructors. Read-
  only sinks reuse the local coordinator's
  unsupported-adapter strings:
  `"INSERT execution requires a configured
  StorageWrite"` / `"DELETE execution requires a
  configured StorageDeleteRows"`.
- **D6:** Shared mutation driver lives behind
  `MutationSourceExecutor` async trait (not a
  closure parameter). Local and distributed
  coordinators both invoke the shared driver.
  Local path is byte-identical pre / post for ADR
  030 / 032 / 033 / 036 fixtures (reference pass-
  count check).
- **D7:** Distributed `INSERT ... VALUES` does not
  call `fragment_plan`. Payload materialization +
  write happen coordinator-side.
- **D8:** Distributed `INSERT ... SELECT` and
  CTAS fragment only the SELECT source. CTAS
  ordering: source read → catalog create →
  materialize payload → write.
- **D9:** `ExecutionPlan::Scan` carries
  `row_identity: RowIdentityProjection`. Normal
  scan constructors set `None`. Only the internal
  DELETE candidate plan builder sets `Append`. SQL
  binder / planner synthetic-name rejects remain.
- **D10:** DELETE candidate execution uses
  `runtime_filter: None` at the scan-input
  boundary. Positive + negative guard patterns
  (Claude C11 + A3 carry-forward; task-context-
  aware variant added).
- **D11:** Fragmenter `*NotFragmentable` rejects
  remain defensive for raw `Insert` /
  `InsertSelect` / `CreateTableAsSelect` /
  `Delete` / `CreateTable` / `DropTable` plans.
- **D12:** Wendelon CREATE Plan A only if
  `submit_table_schema_update` accepts a real
  `TableSchemaDescriptor::new_with_layout` and a
  defensible `table_object_id` allocation path
  exists. Otherwise Plan B (preserve
  `AdminUnavailable`, defer to ADR 038).
- **D13:** Wendelon-known DROP remains
  `AdminUnavailable`. No `WendelonCatalog::drop_table`
  change.
- **D14:** ADR 038 = Storage-Admin / Reclaim
  Substrate (Wendelon DROP + physical reclaim +
  catalog refresh after admin mutation +
  remaining Wendelon CREATE object-id work if
  Plan B).
- **Claude iter-1 A1 carry-forward:** capture
  reference fixture pass counts at Phase 0;
  verify identical pass counts after Phase 4
  extraction.
- **Claude iter-1 A2 carry-forward:** Plan A / B
  contingency is hard. Do not invent
  `table_object_id` allocation policy inside ADR
  037; defer.
- **Claude iter-1 A3 carry-forward:** DELETE
  candidate execution `runtime_filter: None` has
  both positive (literal) and negative (no
  `self.context.runtime_filter` /
  `context.runtime_filter` /
  `spec.context.runtime_filter` inheritance)
  architecture guard patterns.

## Touchpoint sweep

| File | Phase |
|---|---|
| `crates/toron/src/coordinator.rs` | 1 (context-bearing + sinks), 4 (shared driver wiring), 5 (distributed INSERT VALUES/SELECT + CTAS), 6 (distributed DELETE) |
| `crates/toron/src/coordinator/write.rs` | 4 (shared driver extraction) |
| `crates/toron/src/coordinator/delete.rs` | 4 (shared driver extraction), 6 (distributed DELETE candidate path) |
| `crates/toron/src/distributed/context.rs` (NEW) | 2 (`TaskExecutionContext` struct + serde) |
| `crates/toron/src/distributed/mod.rs` | 2 (module wiring) |
| `crates/toron/src/distributed/task.rs` | 2 (`TaskSpec.context` field + serde) |
| `crates/toron/src/distributed/fragment.rs` | 2 (`FragmentPlanConfig` carries context; stamps every task) |
| `crates/toron/src/server/mod.rs` | 2 (worker `ExecutionContext` construction from `spec.context` + `spec.identity`) |
| `crates/toron/src/ir/execution.rs` | 3 (`ExecutionPlan::Scan.row_identity` field), 6 (`Filter` arm preserves trailing identity column) |
| `crates/toron/src/binder.rs` | 3 (`bind_scan` defaults to `None`; synthetic-name rejects preserved) |
| `crates/toron/src/exec.rs` | 3 (`ScanRequest` field plumb), 4 (defensive arms unchanged) |
| `crates/toron/src/storage.rs` | 3 (`RowIdentityProjection` re-export location decision; `ScanRequest.row_identity` plumb) |
| `crates/toron/src/catalog.rs` | 7 (Wendelon CREATE Plan A or Plan B preservation) |
| `crates/toron/tests/distributed_write_context_test.rs` (NEW) | 5, 6, 7 (functional fixtures) |
| `crates/toron/tests/architecture_test.rs` | 8 (25 new guards + paired adjustment) |
| `crates/toron/tests/wendelon_storage_execution_test.rs` | 7 (optional Wendelon CREATE Plan A fixtures) |

## Helper reuse summary

| Helper | Source | ADR 037 use |
|---|---|---|
| `materialize_insert_values_batches` | ADR 030 | Reused unchanged via shared driver |
| `materialize_insert_select_batch` | ADR 032 | Reused unchanged via shared driver |
| `materialize_empty_record_batch` | ADR 033 | Reused unchanged via shared driver |
| `materialize_delete_candidate_batches` | ADR 036 | Reused unchanged via shared driver |
| `extract_delete_row_key_ids` | ADR 035 / 036 | Reused unchanged via shared driver |
| `execute_delete_with_candidates` | ADR 035 | Reused unchanged via shared driver |
| `filter_batches_with_bound_predicate` | ADR 036 | Reused unchanged for residual filtering of DELETE candidates |
| `delete_count_batch` | ADR 036 | Reused unchanged (coordinator-side result batch) |
| `lower_table_ref_object_name` | ADR 036 | Reused unchanged (no SQL surface change) |
| `Executor::execute_bound` | Existing | Reused unchanged by local `MutationSourceExecutor` impl |
| `fragment_plan` + scheduler + collector | Existing | Reused unchanged by distributed `MutationSourceExecutor` impl for source / candidate read plans |
| `submit_table_schema_update` | Wendelon `Client` | Plan A: real call site for Wendelon CREATE |
| `TableSchemaDescriptor::new_with_layout` | Wendelon | Plan A: real descriptor construction (no ad-hoc fingerprint) |
| `MutationSourceExecutor` (NEW) | Phase 4 | Private async trait seam |
| `LocalMutationSourceExecutor` (NEW) | Phase 4 | Local impl wraps `Executor::execute_bound` |
| `DistributedMutationSourceExecutor` (NEW) | Phase 4 | Distributed impl wraps fragment + schedule + collect |
| `TaskExecutionContext` (NEW) | Phase 2 | 4-field serializable task context |
| `derive_delete_candidate_plan` (NEW) | Phase 6 | Constructs read-only candidate plan from `ExecutionPlan::Delete` with `row_identity: Append`; candidate execution supplies `runtime_filter: None` at the scan-input boundary |
| `DistributedQueryCoordinator::with_writer` (NEW) | Phase 1 | Mirrors `QueryCoordinator::with_writer` |
| `DistributedQueryCoordinator::with_writer_and_deleter` (NEW) | Phase 1 | Mirrors local pattern |

Six new types/trait/items (`TaskExecutionContext`,
`MutationSourceExecutor` trait + 2 impls,
`derive_delete_candidate_plan`, `with_writer` /
`with_writer_and_deleter` constructors). Zero new
public storage request fields. Zero new
`StorageError` variants.

## Phase 0 — Preflight audit + reference fixture capture

```sh
# Confirm distributed DML rejects currently in place
rg -n "distributed (INSERT|INSERT INTO \\.\\.\\. SELECT|CREATE TABLE AS SELECT|DELETE) execution is not wired|ExecutionPlan::(Insert|InsertSelect|CreateTableAsSelect|Delete)" crates/toron/src/coordinator.rs
# Expected: at least four reject sites; capture exact strings for Phase 5/6 removal verification

# Confirm fragmenter NotFragmentable variants
rg -n "FragmentError::.*NotFragmentable" crates/toron/src/distributed/fragment.rs
# Expected: variants for Insert / InsertSelect / CreateTableAsSelect / Delete / CreateTable / DropTable

# Confirm worker context construction site
rg -n "ExecutionContext::local_test|ExecutionContext \{" crates/toron/src/server/mod.rs
# Expected: at least one local_test() call in production execute_task body (to be removed in Phase 2)

# Confirm Executor::execute_bound builds ScanRequest from ExecutionContext
rg -n "ScanRequest \{|ExecutionContext" crates/toron/src/exec.rs | head -30
# Expected: ScanRequest construction reads session_id / tenant_id / read_bound from &ExecutionContext

# Confirm ExecutionPlan::Scan current shape
rg -n "ExecutionPlan::Scan|Scan \{" crates/toron/src/ir/execution.rs | head -10
# Expected: 8-field Scan variant (local_table_id, table, fingerprint, schema, projection, pbc_program, profile_hint, primary_key_columns); no row_identity yet

# Confirm DistributedQueryCoordinator construction shape
rg -n "DistributedQueryCoordinator" crates/toron/src/coordinator.rs | head -20
# Expected: read-only construction with catalog / registry / memory / transport; no writer/deleter fields

# Confirm local coordinator write/delete sink pattern (template for D5)
rg -n "with_writer|with_writer_and_deleter|StorageWrite|StorageDeleteRows" crates/toron/src/coordinator.rs | head -20
# Expected: local with_writer / with_writer_and_deleter pattern from ADR 030 / 035 / 036

# Confirm Wendelon catalog admin surface
rg -n "create_table|drop_table|submit_table_schema_update|AdminUnavailable" crates/toron/src/catalog.rs
# Expected: WendelonCatalog::create_table returns AdminUnavailable; WendelonCatalog::drop_table returns AdminUnavailable

# Confirm storage trace context vs distributed trace context types
rg -n "TraceContext" crates/toron/src/storage.rs crates/toron/src/distributed/task.rs
# Expected: two distinct TraceContext types; Phase 2 keeps storage trace explicit and does not reuse distributed task trace

# Capture reference fixture pass counts for byte-identical refactor verification
cargo test -p toron --test insert_test -- --nocapture 2>&1 | tail -5
cargo test -p toron --test insert_select_test -- --nocapture 2>&1 | tail -5
cargo test -p toron --test ctas_test -- --nocapture 2>&1 | tail -5
cargo test -p toron --test delete_test -- --nocapture 2>&1 | tail -5
# Expected: capture "N passed; 0 failed" line for each; reference for Phase 4 verification

# Confirm no shadow dependencies
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits

# Confirm ADR 030 / 032 / 033 / 035 / 036 architecture guards green (baseline)
cargo test -p toron --test architecture_test adr030
cargo test -p toron --test architecture_test adr032
cargo test -p toron --test architecture_test adr033
cargo test -p toron --test architecture_test adr035
cargo test -p toron --test architecture_test adr036
# Expected: all green
```

Expected baseline:

- Distributed `INSERT` / `INSERT SELECT` / CTAS /
  `DELETE` rejects present in
  `DistributedQueryCoordinator::execute_query`
  body with captured reject strings.
- Fragmenter `*NotFragmentable` variants present
  for all DML / catalog mutation plans.
- Worker `execute_task` calls
  `ExecutionContext::local_test()` in production
  paths.
- `Executor::execute_bound` builds `ScanRequest`
  from `ExecutionContext.{session_id,
  tenant_id, read_bound}` — confirms iter-3
  TaskExecutionContext shape correction.
- `ExecutionPlan::Scan` has 8 fields (no
  `row_identity` yet).
- `DistributedQueryCoordinator` construction:
  catalog + registry + memory + transport; no
  writer / deleter fields.
- `QueryCoordinator::with_writer` /
  `with_writer_and_deleter` patterns exist from
  ADR 030 / 035 / 036 (template for D5).
- `WendelonCatalog::create_table` returns
  `AdminUnavailable`; `WendelonCatalog::drop_table`
  returns `AdminUnavailable`.
- Storage `TraceContext` and distributed
  `TraceContext` are distinct types; Phase 2
  keeps the storage trace type explicit and does
  not reuse distributed task trace.
- All reference fixture pass counts captured:
  `insert_test`, `insert_select_test`,
  `ctas_test`, `delete_test`.
- All ADR 030 / 032 / 033 / 035 / 036
  architecture guards green.

Preserve verbatim:

- All ADR 029 / 030 / 031 / 032 / 033 / 034 / 035
  / 036 invariants.
- All existing SELECT / INSERT / INSERT SELECT /
  CREATE TABLE / CTAS / DROP / DELETE substrate
  test fixtures.

## Phase 1 — Context-bearing `DistributedQueryCoordinator` + explicit sinks

**Files:** `crates/toron/src/coordinator.rs`.

**Tasks:**

1. Add execution context to
   `DistributedQueryCoordinator`. Recommended
   shape:

   ```rust
   pub struct DistributedQueryCoordinator {
       catalog: Arc<dyn Catalog>,
       registry: WorkerRegistry,
       memory: MemoryConfig,
       transport: Arc<dyn DistributedTransport>,
       context: ExecutionContext,  // NEW
       writer: Option<Arc<dyn StorageWrite>>,  // NEW
       deleter: Option<Arc<dyn StorageDeleteRows>>,  // NEW
       // ... existing fields ...
   }
   ```

2. Constructor pattern (mirrors
   `QueryCoordinator`):

   ```rust
   impl DistributedQueryCoordinator {
       pub fn new(
           catalog: Arc<dyn Catalog>,
           registry: WorkerRegistry,
           memory: MemoryConfig,
           transport: Arc<dyn DistributedTransport>,
           context: ExecutionContext,
       ) -> Self { /* writer/deleter: None */ }

       pub fn with_writer(
           mut self,
           writer: Arc<dyn StorageWrite>,
       ) -> Self { /* installs writer */ }

       pub fn with_writer_and_deleter(
           mut self,
           writer: Arc<dyn StorageWrite>,
           deleter: Arc<dyn StorageDeleteRows>,
       ) -> Self { /* installs both */ }
   }
   ```

3. Update construction call sites:
   - production / integration test sites pass real
     `ExecutionContext`;
   - existing read-only tests pass an explicit
     test context (e.g., `ExecutionContext::local_test()`
     stays available for test construction only).

4. Use `self.context` for `WriteRequest` /
   `DeleteRowsRequest` field population in any
   coordinator-side mutation path. Use one
   `query_id` consistently:
   - `FragmentPlanConfig.query_id =
     self.context.query_id`;
   - `TaskSpec.identity.query_id =
     self.context.query_id`;
   - `WriteRequest.query_id = self.context.query_id`;
   - `DeleteRowsRequest.query_id =
     self.context.query_id`.

5. Read-only sinks (writer / deleter `None`)
   produce diagnostics matching local coordinator
   strings when DML is attempted:
   - `"INSERT execution requires a configured
     StorageWrite"`;
   - `"DELETE execution requires a configured
     StorageDeleteRows"`.

   Defer the actual reject points until Phase 5
   removes the old top-level reject strings.

**Guards (added in Phase 8):**

- `adr037_distributed_coordinator_is_context_bearing`
  — `DistributedQueryCoordinator` body contains
  `context: ExecutionContext` field.
- `adr037_distributed_write_requests_use_real_context`
  — `WriteRequest` / `DeleteRowsRequest`
  construction sites under
  `DistributedQueryCoordinator` source fields
  from `self.context`, not from `local_test()`.
- `adr037_distributed_coordinator_has_explicit_write_sinks`
  — `DistributedQueryCoordinator` body contains
  `writer` / `deleter` fields or
  `with_writer` / `with_writer_and_deleter`
  constructors.

## Phase 2 — `TaskExecutionContext` substrate + worker construction

**Files:**
- `crates/toron/src/distributed/context.rs` (NEW);
- `crates/toron/src/distributed/mod.rs`;
- `crates/toron/src/distributed/task.rs`;
- `crates/toron/src/distributed/fragment.rs`;
- `crates/toron/src/server/mod.rs`.

**Tasks:**

1. Create `crates/toron/src/distributed/context.rs`
   with:

   ```rust
   use serde::{Deserialize, Serialize};
   use crate::ir::SessionId;
   use crate::storage::{HlcBound, TenantId, TraceContext as StorageTraceContext};

   #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
   pub struct TaskExecutionContext {
       pub session_id: SessionId,
       pub tenant_id: TenantId,
       pub read_bound: HlcBound,
       pub trace_context: Option<StorageTraceContext>,
   }
   ```

   Add or confirm `Serialize` / `Deserialize`
   derives for `SessionId`, `TenantId`,
   `HlcBound`, and storage `TraceContext` as
   needed. Do not reuse
   `distributed::task::TraceContext` here: that
   type remains task/wire tracing, while this
   field is the storage trace context that feeds
   `ScanRequest`, `WriteRequest`, and
   `DeleteRowsRequest`.

2. Wire module:

   ```rust
   // crates/toron/src/distributed/mod.rs
   pub mod context;
   pub use context::TaskExecutionContext;
   ```

3. Add `TaskSpec.context: TaskExecutionContext`:

   ```rust
   pub struct TaskSpec {
       pub identity: TaskIdentity,
       pub budget: TaskBudget,
       pub context: TaskExecutionContext,  // NEW
       pub plan: ExecutionPlan,
       pub inputs: Vec<TaskInputBinding>,
       // ... existing fields ...
   }
   ```

   Derive `Serialize` / `Deserialize` for
   `TaskSpec` (already present); compile-time
   check confirms the new field round-trips.

4. Extend `FragmentPlanConfig`:

   ```rust
   pub struct FragmentPlanConfig {
       pub query_id: QueryId,
       pub task_context: TaskExecutionContext,  // NEW
       // ... existing fields ...
   }
   ```

   Fragmenter stamps every emitted `TaskSpec.context
   = config.task_context.clone()`.

5. `DistributedQueryCoordinator` builds
   `FragmentPlanConfig` from `self.context`:

   ```rust
   let config = FragmentPlanConfig {
       query_id: self.context.query_id,
       task_context: TaskExecutionContext {
           session_id: self.context.session_id,
           tenant_id: self.context.tenant_id,
           read_bound: self.context.read_bound.clone(),
           trace_context: self.context.trace_context.clone(),
       },
       // ...
   };
   ```

6. Worker `execute_task` constructs
   `ExecutionContext` through the public
   constructor and setters:

   ```rust
   // crates/toron/src/server/mod.rs
   let mut exec_context = ExecutionContext::new(
       spec.context.session_id,
       spec.identity.query_id,  // from identity, not context
       spec.context.tenant_id,
       spec.context.read_bound,
   )
   .with_task_budget(spec.budget)
   .with_task_telemetry(telemetry.clone())
   .with_runtime_filter(scan_runtime_filter(spec)); // from TaskInputBinding::Scan
   exec_context.trace_context = spec.context.trace_context.clone();
   exec_context.profile_hint = scan_profile_hint(spec); // from ExecutionPlan::Scan
   ```

   Remove `ExecutionContext::local_test()` call
   from production `execute_task` body. The helper
   may remain available behind `#[cfg(test)]` or
   similar gate; production code paths must use
   the task-context-derived construction.

7. Validate via grep that
   `Executor::execute_bound` reads
   `ExecutionContext.{session_id, tenant_id,
   read_bound}` for `ScanRequest` construction.
   Worker now feeds those from
   `spec.context.{session_id, tenant_id,
   read_bound}` so distributed scans observe real
   context (the iter-3 correction).

**Guards (added in Phase 8):**

- `adr037_task_spec_carries_task_execution_context`
  — `TaskSpec` body contains
  `context: TaskExecutionContext`.
- `adr037_worker_no_longer_uses_local_test_context`
  — `crates/toron/src/server/mod.rs` production
  body does not call `ExecutionContext::local_test()`.
- `adr037_worker_context_uses_task_identity_query_id`
  — worker `ExecutionContext.query_id =
  spec.identity.query_id` literal pattern present.
- `adr037_worker_context_uses_task_context_session_tenant_read_bound`
  — worker `ExecutionContext.{session_id,
  tenant_id, read_bound}` derived from
  `spec.context.{session_id, tenant_id,
  read_bound}`.
- `adr037_task_context_field_set_is_minimal`
  — `TaskExecutionContext` body contains exactly
  4 fields; negative pattern against receipt /
  client-write / durability / finalization /
  retry / runtime-filter / row-identity / table-
  pin / telemetry / spill literals.
- `adr037_task_context_runtime_filter_via_task_input_binding`
  — task execution sources `runtime_filter` from
  `TaskInputBinding::Scan` (existing path); no
  `runtime_filter` field on `TaskExecutionContext`.

## Phase 3 — `ExecutionPlan::Scan.row_identity` field

**Files:**
- `crates/toron/src/ir/execution.rs`;
- `crates/toron/src/binder.rs`;
- `crates/toron/src/exec.rs`;
- `crates/toron/src/storage.rs`;
- existing scan-construction tests.

**Tasks:**

1. Decide `RowIdentityProjection` location. If
   importing from `storage.rs` into
   `ir/execution.rs` creates poor coupling, move
   the enum to a neutral module
   (`crates/toron/src/row_identity.rs` or
   similar) and re-export from `storage.rs` for
   ADR 035 `ScanRequest.row_identity` continuity.

2. Add `row_identity` field to
   `ExecutionPlan::Scan`:

   ```rust
   ExecutionPlan::Scan {
       local_table_id: TableId,
       table: TableObjectId,
       fingerprint: PhysicalDescriptorFingerprint,
       schema: SchemaRef,
       projection: Option<Vec<ColumnOrdinal>>,
       pbc_program: Option<PbcProgram>,
       profile_hint: QueryProfileHint,
       primary_key_columns: Vec<ColumnOrdinal>,
       row_identity: RowIdentityProjection,  // NEW
   }
   ```

3. Default all normal `ExecutionPlan::Scan`
   constructors to `RowIdentityProjection::None`:
   - `bind_scan` (binder);
   - any rewrite paths emitting `Scan`;
   - any test constructors.

4. Update `ExecutionPlan::schema()`:

   ```rust
   Self::Scan { schema, row_identity, .. } => match row_identity {
       RowIdentityProjection::None => schema.clone(),
       RowIdentityProjection::Append => {
           append_row_key_id_column(schema)
       }
   },
   ```

   `append_row_key_id_column` constructs a new
   `SchemaRef` that appends the
   `__row_key_id__: UInt64` (or whatever the ADR
   035 layout dictates) column. Import the
   synthetic name constant from Wendelon (not
   duplicated locally).

5. Plumb the field into `ScanRequest` in
   `Executor::execute_bound`:

   ```rust
   ExecutionPlan::Scan { row_identity, .. } => {
       let request = ScanRequest {
           // ... existing fields ...
           row_identity: *row_identity,
       };
       // ...
   }
   ```

6. Preserve SQL synthetic-name rejects in
   `binder.rs` and `sql_planner.rs` (ADR 029 D12 +
   ADR 030 D7 + ADR 035 D11 + ADR 036 guard #22
   carry-forward).

7. Update existing exhaustive matches over
   `ExecutionPlan::Scan` fields (no `..`):
   destructure the new field where appropriate
   (executor defensive arms, walker leaves,
   fragmenter, scheduler).

**Guards (added in Phase 8):**

- `adr037_execution_scan_has_row_identity_field`
  — `ExecutionPlan::Scan` body contains
  `row_identity: RowIdentityProjection` field.
- `adr037_normal_scan_constructors_default_to_none`
  — non-DELETE-candidate scan constructions set
  `row_identity: RowIdentityProjection::None`.
- `adr037_delete_candidate_only_sets_append` —
  only the internal DELETE candidate plan builder
  constructs `Scan { row_identity:
  RowIdentityProjection::Append }` in production
  source.
- `adr037_sql_synthetic_row_key_invisible` —
  planner / binder synthetic-name rejects for
  `__row_key_id__` / `__row_hlc__` remain (ADR
  035 / 036 carry-forward).
- `adr037_execution_scan_rest_patterns_prohibited`
  — paired adjustment: no `..` rest pattern on
  `ExecutionPlan::Scan` destructure (forces
  exhaustive match on new field).

## Phase 4 — Shared mutation driver via `MutationSourceExecutor`

**Files:**
- `crates/toron/src/coordinator.rs`;
- `crates/toron/src/coordinator/write.rs`;
- `crates/toron/src/coordinator/delete.rs`.

**Tasks:**

1. Add `MutationSourceExecutor` trait (private to
   the `coordinator` module tree):

   ```rust
   #[async_trait::async_trait]
   pub(crate) trait MutationSourceExecutor: Send + Sync {
       async fn execute_read_plan(
           &self,
           plan: &ExecutionPlan,
       ) -> Result<Vec<RecordBatch>, QueryError>;
   }
   ```

2. Implement for local path:

   ```rust
   pub(crate) struct LocalMutationSourceExecutor<'a> {
       executor: &'a Executor,
       context: &'a ExecutionContext,
   }

   #[async_trait::async_trait]
   impl<'a> MutationSourceExecutor for LocalMutationSourceExecutor<'a> {
       async fn execute_read_plan(
           &self,
           plan: &ExecutionPlan,
       ) -> Result<Vec<RecordBatch>, QueryError> {
           self.executor.execute_bound(plan, self.context).await
       }
   }
   ```

3. Implement for distributed path:

   ```rust
   pub(crate) struct DistributedMutationSourceExecutor<'a> {
       coordinator: &'a DistributedQueryCoordinator,
   }

   #[async_trait::async_trait]
   impl<'a> MutationSourceExecutor for DistributedMutationSourceExecutor<'a> {
       async fn execute_read_plan(
           &self,
           plan: &ExecutionPlan,
       ) -> Result<Vec<RecordBatch>, QueryError> {
           // fragment + schedule + collect for the read-only plan
           let fragments = self.coordinator.fragment_plan(plan)?;
           let tasks = self.coordinator.schedule(fragments).await?;
           self.coordinator.collect(tasks).await
       }
   }
   ```

4. Extract shared mutation driver helpers. Each
   helper takes `&dyn MutationSourceExecutor` (or a
   typed generic + `async_trait`-friendly bound)
   and an explicit storage sink:

   - `execute_insert_via_driver(plan: &ExecutionPlan::Insert,
     source: &dyn MutationSourceExecutor, writer:
     &dyn StorageWrite, context: &ExecutionContext)`
   - `execute_insert_select_via_driver(...)`
   - `execute_ctas_via_driver(...)`
   - `execute_delete_via_driver(...)`

   Each helper:
   - reuses existing materializers
     (`materialize_insert_values_batches`,
     `materialize_insert_select_batch`,
     `materialize_empty_record_batch`,
     `materialize_delete_candidate_batches`,
     `extract_delete_row_key_ids`);
   - reuses existing validation
     (`validate_not_null` etc.);
   - preserves existing result batch shape
     (`delete_count_batch` for DELETE; empty for
     INSERT / CTAS).

5. Refactor local `QueryCoordinator::execute_write_plan`
   to construct a `LocalMutationSourceExecutor`
   and call the shared driver helpers.

6. **Byte-identical refactor verification.** Rerun
   reference fixtures captured at Phase 0:

   ```sh
   cargo test -p toron --test insert_test
   cargo test -p toron --test insert_select_test
   cargo test -p toron --test ctas_test
   cargo test -p toron --test delete_test
   ```

   Pass counts must match Phase 0 capture. Behavior
   (result shapes, error messages, empty-batch
   contracts, CTAS ordering, DELETE residual
   filtering) must be byte-identical.

**Guards (added in Phase 8):**

- `adr037_local_and_distributed_call_shared_mutation_driver`
  — both local
  `QueryCoordinator::execute_write_plan` and
  `DistributedQueryCoordinator` write-path
  bodies invoke shared driver helpers; no
  copy-pasted materializer / validation bodies.
- `adr037_mutation_source_executor_is_async_trait_seam`
  — `MutationSourceExecutor` trait exists with
  `async fn execute_read_plan(&self, plan:
  &ExecutionPlan) -> ...` signature; no
  closure-based `Box<dyn Fn(...) -> ...>`
  source executor parameter.
- `adr037_shared_write_driver_preserves_local_byte_identical`
  — refactor invariance check (fixture pass-
  count guard or reference behavior test;
  verified at impl Phase 4).

## Phase 5 — Distributed INSERT VALUES / INSERT SELECT / CTAS routing

**Files:**
- `crates/toron/src/coordinator.rs`;
- `crates/toron/tests/distributed_write_context_test.rs`
  (NEW).

**Tasks:**

1. Remove old top-level distributed DML reject
   strings for:
   - `ExecutionPlan::Insert`;
   - `ExecutionPlan::InsertSelect`;
   - `ExecutionPlan::CreateTableAsSelect`.

   (DELETE removal lands in Phase 6.) Capture
   exact strings from Phase 0 audit for guard
   #11 absence pattern.

2. Distributed `INSERT ... VALUES` routing
   (D7 — no fragmentation):

   ```rust
   ExecutionPlan::Insert { .. } => {
       let writer = self.writer.as_ref().ok_or(
           QueryError::CapabilityRequired(
               "INSERT execution requires a configured StorageWrite",
           ),
       )?;
       // No source plan; call shared driver with
       // a trivial source executor (or bypass the
       // source executor entirely for INSERT VALUES).
       execute_insert_via_driver(
           plan,
           writer.as_ref(),
           &self.context,
       ).await
   }
   ```

   Distributed `INSERT VALUES` **must not call
   `fragment_plan`**.

3. Distributed `INSERT ... SELECT` routing
   (D8 — fragment source only):

   ```rust
   ExecutionPlan::InsertSelect { source, .. } => {
       let writer = self.writer.as_ref().ok_or(...)?;
       let source_executor = DistributedMutationSourceExecutor {
           coordinator: self,
       };
       execute_insert_select_via_driver(
           plan,
           source.as_ref(),       // fragments the SELECT source
           &source_executor,
           writer.as_ref(),
           &self.context,
       ).await
   }
   ```

   Only the `source` plan reaches
   `fragment_plan`. The outer `InsertSelect`
   variant is not fragmentable (defensive
   rejected by fragmenter per D11).

4. Distributed CTAS routing (D8 — fragment source
   only; D1 + ADR 033 ordering preserved):

   ```rust
   ExecutionPlan::CreateTableAsSelect { source, table, .. } => {
       let writer = self.writer.as_ref().ok_or(...)?;
       let source_executor = DistributedMutationSourceExecutor {
           coordinator: self,
       };
       execute_ctas_via_driver(
           plan,
           source.as_ref(),  // fragments the SELECT source
           &source_executor,
           self.catalog.as_ref(),
           writer.as_ref(),
           &self.context,
       ).await
       // Order inside the helper: source read -> catalog create
       // -> materialize payload (or empty batch via
       // materialize_empty_record_batch) -> StorageWrite::write
   }
   ```

   CTAS non-transactional partial-failure
   behavior preserved verbatim (ADR 033 D7
   carry-forward).

5. Functional fixtures (new test file
   `crates/toron/tests/distributed_write_context_test.rs`):

   - `distributed_insert_values` — `INSERT INTO t
     VALUES (...)`; subsequent SELECT shows
     inserted row.
   - `distributed_insert_select_with_projection_reorder`
     — `INSERT INTO t (col_b, col_a) SELECT b, a
     FROM source`; SELECT shows reordered values.
   - `distributed_ctas_from_aggregate_source` —
     `CREATE TABLE t AS SELECT col, SUM(val)
     FROM source GROUP BY col`; SELECT shows
     aggregated rows.
   - `distributed_ctas_from_join_source` — CTAS
     over a JOIN; coordinator fragments the JOIN
     source, then writes once.
   - `read_only_distributed_coordinator_rejects_dml_at_storage_boundary`
     — `DistributedQueryCoordinator::new` without
     `with_writer` / `with_writer_and_deleter`;
     INSERT / CTAS return capability error (not
     the old coordinator-level reject string).

**Guards (added in Phase 8):**

- `adr037_distributed_dml_reject_strings_absent`
  — old reject strings for distributed INSERT /
  INSERT SELECT / CTAS not present in
  `DistributedQueryCoordinator::execute_query`
  body (DELETE absence verified after Phase 6).
- `adr037_insert_values_does_not_fragment` —
  distributed INSERT path body does not call
  `fragment_plan`.
- `adr037_insert_select_fragments_source_only` —
  distributed INSERT SELECT path fragments only
  the SELECT source plan.
- `adr037_ctas_fragments_source_only_preserves_create_before_source`
  — distributed CTAS path fragments only source;
  catalog create call site precedes write call
  site in helper body.
- `adr037_fragmenter_dml_defensive_rejects_remain`
  — `FragmentError::*NotFragmentable` variants
  preserved for all DML / catalog mutation
  plans.

## Phase 6 — Distributed DELETE candidate reads

**Files:**
- `crates/toron/src/coordinator/delete.rs`;
- `crates/toron/src/coordinator.rs`;
- `crates/toron/tests/distributed_write_context_test.rs`.

**Tasks:**

1. Add `derive_delete_candidate_plan` helper:

   ```rust
   pub(crate) fn derive_delete_candidate_plan(
       delete: &ExecutionPlan::Delete,
   ) -> ExecutionPlan {
       // Pushdown-only predicate:
       //   Scan { row_identity: Append, pbc_program: Some(pushed),
       //          ... }
       // Residual predicate:
       //   Filter { predicate: residual, input: Box<Scan {
       //     row_identity: Append, pbc_program: None,
       //     ...
       //   }> }
       // No predicate:
       //   Scan { row_identity: Append, pbc_program: None,
       //          ... }
   }
   ```

   The candidate plan body does not gain a
   runtime-filter field. Candidate execution MUST
   set or preserve `runtime_filter: None` at the
   scan-input / `ScanRequest` construction
   boundary. The candidate scan MUST NOT inherit
   any ambient runtime filter from
   `self.context.runtime_filter`,
   `context.runtime_filter`, or
   `spec.context.runtime_filter`.

2. Remove the distributed `Delete` reject string
   from
   `DistributedQueryCoordinator::execute_query`.

3. Distributed `ExecutionPlan::Delete` routing:

   ```rust
   ExecutionPlan::Delete { .. } => {
       let deleter = self.deleter.as_ref().ok_or(
           QueryError::CapabilityRequired(
               "DELETE execution requires a configured StorageDeleteRows",
           ),
       )?;
       let candidate_plan = derive_delete_candidate_plan(plan);
       let source_executor = DistributedMutationSourceExecutor {
           coordinator: self,
       };
       execute_delete_via_driver(
           plan,
           &candidate_plan,
           &source_executor,
           deleter.as_ref(),
           &self.context,
       ).await
   }
   ```

   The shared driver:
   - calls
     `source_executor.execute_read_plan(&candidate_plan)`;
   - calls `extract_delete_row_key_ids` on the
     returned batches;
   - if the row-key set is empty,
     short-circuits via
     `execute_delete_with_candidates`;
   - otherwise constructs `DeleteRowsRequest`
     from `context` + extracted ids and calls
     `deleter.delete_rows(...)` once.

4. The candidate scan output schema includes the
   trailing `__row_key_id__` column (`row_identity:
   Append`). The optional `Filter` wrapper
   preserves it because
   `filter_batches_with_bound_predicate` (ADR
   036 D6) preserves all input columns.

5. Functional fixtures (extend
   `crates/toron/tests/distributed_write_context_test.rs`):

   - `distributed_delete_with_pushdown_predicate`
     — `DELETE FROM t WHERE id > 2` (PBC-pushable);
     subsequent SELECT shows remaining rows;
     deleted count matches.
   - `distributed_delete_with_residual_predicate`
     — `DELETE FROM t WHERE amount + 1 > 40`
     (non-PBC arithmetic); deleted count matches
     **only the filter result**. Critical R1
     correctness fixture carry-forward from ADR
     036; proves residual filter applies before
     identity extraction.
   - `distributed_delete_ignores_ambient_runtime_filter`
     — coordinator context (or upstream
     distributed JOIN setup) carries a runtime
     filter; the DELETE candidate scan does NOT
     inherit it. Delete count matches
     predicate-only, not predicate-and-filter
     intersection.
   - `worker_scan_request_uses_real_context` —
     distributed SELECT (or DELETE candidate
     scan) execution observes real
     `session_id` / `tenant_id` / `read_bound`
     from `TaskExecutionContext`, not
     `local_test()` defaults. Verify by recording
     `ScanRequest.session_id` on the worker side
     in a test-only spy.
   - `read_only_distributed_coordinator_rejects_dml_at_storage_boundary`
     — extend Phase 5 fixture: DELETE on
     read-only coordinator returns capability
     error.

**Guards (added in Phase 8):**

- `adr037_delete_fragments_derived_candidate_only`
  — distributed DELETE path body fragments only
  the derived candidate plan; no raw
  `ExecutionPlan::Delete` reaches
  `fragment_plan`.
- `adr037_delete_candidate_scan_no_runtime_filter`
  — positive: `runtime_filter: None` literal in
  candidate execution / scan-input construction,
  not as an `ExecutionPlan::Scan` field; negative: no
  `runtime_filter: self.context.runtime_filter`
  / `runtime_filter: context.runtime_filter` /
  `runtime_filter: spec.context.runtime_filter`
  inheritance patterns.
- `adr037_distributed_dml_reject_strings_absent`
  — verify DELETE reject string absent (final
  pass after Phase 6).

## Phase 7 — Wendelon CREATE Plan A or Plan B preservation

**Files:**
- `crates/toron/src/catalog.rs`;
- `crates/toron/tests/wendelon_storage_execution_test.rs`
  (optional Plan A fixtures).

**Preflight (D12 hard contingency):**

```sh
# Confirm Wendelon submit_table_schema_update signature
rg -n "submit_table_schema_update|TableSchemaDescriptor::new_with_layout" crates/wendelon/src
# Look for: real descriptor accepted; what does table_object_id allocation require?

# Confirm table_object_id allocation path
rg -n "TableObjectId|allocate_table_object_id|next_table_id" crates/wendelon/src crates/toron/src
# If allocation requires policy invention or admin consensus we don't have, switch to Plan B.
```

**Plan A path (Wendelon CREATE lands):**

1. Keep `WendelonCatalog::from_descriptors`
   read-only.

2. Add admin-capable construction:

   ```rust
   impl WendelonCatalog {
       pub fn with_admin_client(
           descriptors: Arc<DescriptorMaps>,
           admin: Arc<dyn WendelonAdminClient>,
       ) -> Self { /* ... */ }
   }
   ```

3. Add interior mutability for descriptor maps if
   `create_table(&self, ...)` must update local
   resolution (e.g., `RwLock<DescriptorMaps>` or
   similar).

4. Implement `WendelonCatalog::create_table`:

   ```rust
   fn create_table(
       &self,
       descriptor: &TableDescriptor,
       if_not_exists: bool,
   ) -> Result<TableBinding, CatalogError> {
       // Build real TableSchemaDescriptor::new_with_layout
       let schema_descriptor = TableSchemaDescriptor::new_with_layout(
           descriptor.schema.clone(),
           descriptor.physical_layout.clone(),
       );
       // Submit schema update
       let submit_result = self.admin.submit_table_schema_update(
           schema_descriptor,
           if_not_exists,
       )?;
       // Insert into local maps only after admin success
       self.descriptors.write().insert(...)?;
       Ok(submit_result.binding)
   }
   ```

5. Keep `WendelonCatalog::drop_table` returning
   `AdminUnavailable` (D13).

6. Optional Plan A fixtures (in
   `crates/toron/tests/wendelon_storage_execution_test.rs`
   or new file):
   - `wendelon_catalog_admin_create_table_round_trip`
     — `WendelonCatalog::create_table` succeeds
     via `submit_table_schema_update`; subsequent
     `resolve_table` returns new binding.
   - `wendelon_catalog_admin_create_table_if_not_exists_idempotent`
     — duplicate create with `if_not_exists`
     returns existing binding.
   - `wendelon_catalog_admin_create_table_rejection`
     — Wendelon rejects schema update; Toron
     surfaces `CatalogError::SchemaUpdateRejected`
     (not `AdminUnavailable`).

**Plan B path (preserve `AdminUnavailable`):**

1. Leave `WendelonCatalog::create_table` returning
   `AdminUnavailable` verbatim.

2. Document deferral in ADR 037 (already in
   D12); record Wendelon CREATE alongside DROP in
   ADR 038 scope.

3. Distributed CTAS still works against
   `MockCatalog` and local storage paths for
   fixtures #3 and #4. Wendelon-backed CTAS
   surfaces `AdminUnavailable` deterministically.

4. Preserve all ADR 029 D6
   `WendelonCatalog::create_table` invariants.

**Guards (added in Phase 8):**

- `adr037_wendelon_drop_still_unavailable` —
  `WendelonCatalog::drop_table` for known table
  returns `AdminUnavailable`. Plan A guard: if
  Wendelon CREATE lands, uses
  `TableSchemaDescriptor::new_with_layout` and
  not ad-hoc fingerprints. Plan B guard:
  `WendelonCatalog::create_table` preserves
  `AdminUnavailable` verbatim.

## Phase 8 — Architecture guards

**File:** `crates/toron/tests/architecture_test.rs`.

Add the 25 ADR-037 guards (per promoted ADR
§Architecture Guard Set) plus the paired
adjustment to any existing variant-count
catch-all guard if `ExecutionPlan::Scan` field
addition affects a generic variant-body
source-check.

Guard naming convention (ADR 015 cycle pattern
carry-forward):

- positive shape guard for new fields / traits;
- negative literal pattern guard against context
  inheritance / forbidden field literals;
- absence guard for removed reject strings;
- defensive-arm preservation guard for fragmenter
  / executor / walker leaves.

Verify each guard fails first against a known
violating shape (write the violation, observe
red, revert, observe green) for the most subtle
guards:

- guard #7 (task context minimal field set);
- guard #21 (delete candidate runtime filter
  positive + negative);
- guard #22 (shared mutation driver dispatch).

## Phase 9 — Full verification

```sh
cargo fmt --check
cargo test -p toron
cargo test -p toron --test architecture_test adr037
cargo test -p toron --test distributed_write_context_test
cargo test -p toron --test insert_test
cargo test -p toron --test insert_select_test
cargo test -p toron --test ctas_test
cargo test -p toron --test delete_test
cargo test -p toron --test architecture_test adr029
cargo test -p toron --test architecture_test adr030
cargo test -p toron --test architecture_test adr032
cargo test -p toron --test architecture_test adr033
cargo test -p toron --test architecture_test adr034
cargo test -p toron --test architecture_test adr035
cargo test -p toron --test architecture_test adr036
```

Expected end state:

- distributed `INSERT VALUES`, `INSERT SELECT`,
  CTAS, DELETE work against mock + local +
  (Plan A) Wendelon storage;
- worker scan requests use real `session_id` /
  `tenant_id` / `read_bound` from
  `TaskExecutionContext`;
- production worker `execute_task` does not call
  `ExecutionContext::local_test()`;
- `DistributedQueryCoordinator` reports
  capability error (not old coordinator-level
  reject) when DML is attempted without
  writer/deleter sinks;
- fragmenter still rejects raw
  `ExecutionPlan::Insert` / `InsertSelect` /
  `CreateTableAsSelect` / `Delete` /
  `CreateTable` / `DropTable`;
- local DML fixtures (ADR 030 / 032 / 033 / 036)
  pass with identical pass counts to Phase 0
  capture (byte-identical refactor invariant);
- ADR 035 substrate guards remain green;
- ADR 036 SQL DELETE fixtures remain green;
- ADR 015 reject-string absence guard (#11)
  passes;
- 25 ADR-037 architecture guards green;
- Wendelon CREATE either implemented via real
  admin path (Plan A) or preserved as
  `AdminUnavailable` (Plan B; recorded in ADR
  038 scope);
- Wendelon DROP remains `AdminUnavailable`
  (deferred to ADR 038 per D13 + D14).

## Risks

R1. **Task context under-carries scan-request
fields.** Mitigation: architecture guard #6
checks worker `ExecutionContext.{session_id,
tenant_id, read_bound}` derivation from
`spec.context`; reference fixture
`worker_scan_request_uses_real_context` verifies
non-test values flow through. This is the load-
bearing iter-3 correction.

R2. **Shared async seam becomes lifetime-heavy.**
Mitigation: use private `#[async_trait]` trait
(not generic async closure parameters). Lifetime
parameters on `LocalMutationSourceExecutor<'a>`
and `DistributedMutationSourceExecutor<'a>` are
acceptable; closures returning
`Box<dyn Future<Output = ...>>` are not.

R3. **Local write behavior drifts during helper
extraction.** Mitigation: capture reference
fixture pass counts at Phase 0; require identical
counts after Phase 4. ADR 035 + ADR 036
byte-identical refactor precedent applies.

R4. **DELETE candidate reads inherit runtime
filters.** Mitigation: candidate execution sets
or preserves `runtime_filter: None` at the
scan-input / `ScanRequest` construction boundary;
guard #21 checks both positive literal and negative
`self.context` / `context` / `spec.context`
inheritance patterns. Functional fixture
`distributed_delete_ignores_ambient_runtime_filter`
verifies behavioral non-inheritance.

R5. **Wendelon CREATE needs object-id policy
invention.** Mitigation: hard Plan A / Plan B
preflight at Phase 7 start. If
`table_object_id` allocation requires admin
consensus or policy not present in current
Wendelon admin surface, switch to Plan B
verbatim. Do not invent policy inside ADR 037.

R6. **Storage trace context conflicts with
existing distributed trace type.** Mitigation:
derive serde for the storage trace type and keep
`distributed::task::TraceContext` separate. Do not
silently coerce between the two types. Guard #7
keeps `trace_context` as the storage trace field.

R7. **Fragmenter accepts derived candidate plan
but a future refactor accidentally fragments raw
`ExecutionPlan::Delete`.** Mitigation: guard #16
preserves `FragmentError::DeleteNotFragmentable`
for raw `Delete`; guard #15 asserts the
distributed DELETE path fragments only the
derived candidate plan body.

R8. **CTAS partial-failure recovery seems to want
ADR 037 work.** Mitigation: ADR 033 D7
non-transactional behavior is explicit
carry-forward. ADR 037 does not introduce
compensation logic. Document in code comment
referencing ADR 033 D7 + ADR 037 D8 if a reader
might assume otherwise.

R9. **Worker spy fixture (#8
`worker_scan_request_uses_real_context`) requires
test-only `ScanRequest` recording surface.**
Mitigation: add minimal `#[cfg(test)]` recording
hook in the worker scan code path, or use an
existing test storage adapter that records
`ScanRequest` fields. Do not add a production
recording surface.

R10. **`MutationSourceExecutor` trait is private
but architecture guard #23 needs to see it.**
Mitigation: trait is `pub(crate)`; architecture
guard inspects source under
`crates/toron/src/coordinator*` rather than
public re-exports.

R11. **Existing exhaustive `ExecutionPlan::Scan`
destructure sites use `..` rest pattern, so new
`row_identity` field doesn't break compile but
isn't seen.** Mitigation: paired adjustment
guard #25 (no `..` on `Scan` destructure)
forces every destructure to bind the new field
or `_`, ensuring intent is explicit. Inventory
destructure sites during Phase 3.

R12. **Plan A descriptor construction depends on
Wendelon-side helpers that may have moved.**
Mitigation: confirm
`TableSchemaDescriptor::new_with_layout` signature
at Phase 7 preflight; if mismatched, switch to
Plan B rather than refactoring Wendelon-side
within ADR 037.

R13. **Read-only distributed coordinator
diagnostic differs subtly from local coordinator
diagnostic.** Mitigation: reuse exact strings
`"INSERT execution requires a configured
StorageWrite"` / `"DELETE execution requires a
configured StorageDeleteRows"` from local
coordinator. Architecture guard pair: read-only
distributed surfaces both strings.

R14. **Fixture for `distributed_delete_with_residual_predicate`
proves correctness only if the residual filter
is actually applied before identity extraction.**
Mitigation: pick a predicate where the
filtered row count is strictly smaller than the
table row count (e.g., `amount + 1 > 40` on a
table where row count = 5 but only 2 satisfy
`amount + 1 > 40`); assert `deleted == 2`, not
`deleted == 5`. ADR 036 fixture
`coordinator_deletes_rows_with_residual_predicate`
is the reference shape.

R15. **`derive_delete_candidate_plan` reads
runtime filter from `ExecutionPlan::Delete` if a
field is added there in the future.**
Mitigation: ADR 037 does not add a runtime
filter field to `ExecutionPlan::Delete`. Guard
#21 negative pattern covers task / coordinator /
worker contexts; if a future ADR adds a runtime
filter source, that ADR must update guard #21
or document the inheritance shape explicitly.
