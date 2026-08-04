# Implementation Plan: ADR 038 — Storage-Admin / Reclaim Substrate

**Companion ADR:** `plans/adr/0038_storage_admin_reclaim_substrate.md`

## Goal

Implement the storage-admin / reclaim substrate by lifting two
distributed-DML carry-forward rejects and naming a third for
ADR 039:

- **L1** — Toron-local DROP physical reclaim via the new
  `StorageAdmin` trait + `InMemoryTableStorage` impl +
  coordinator routing helper + strict admin sink. Lifts
  ADR 034 D9 + ADR 035 D9 "no storage reclaim guarantee."
- **L2** — Wendelon `CREATE TABLE` Plan A via UUID-derived
  `TableObjectId` + `RowIdentityPolicy::AppendOnly` +
  `Client::submit_table_schema_update`. Lifts ADR 029 D6 /
  ADR 037 D12 Plan B carry-forward.
- **L3** — Wendelon DROP migration. **Explicitly deferred to
  ADR 039** at promotion time (ADR 038 D13 + D14). The
  α-reclaim-completion-token vs β-empty-table-precondition
  decision remains the ADR 039 primary topic.

Substrate adds:

- `StorageAdmin` trait + `DropTableStorageRequest` +
  `DropTableStorageOutcome` in `crates/toron/src/storage.rs`;
- `InMemoryTableStorage` implements `StorageAdmin` (whole-
  entry removal under the existing `Mutex<HashMap>`);
- `UnsupportedStorageAdmin` + `NoopStorageAdmin` (test
  helper) sinks;
- `QueryCoordinator` + `DistributedQueryCoordinator`
  storage-admin sink field + `with_storage_admin` /
  `with_writer_deleter_and_admin` constructors;
- shared `WendelonDescriptorRegistry` in
  `crates/toron/src/catalog.rs` (or sibling) backing
  catalog + 3 storage adapters;
- `WendelonCatalog::create_table` Plan A wiring (UUID id,
  `AppendOnly` policy, `submit_table_schema_update` call,
  registry mutation on success);
- DROP routing helper extension to `execute_catalog_plan`:
  resolve → admin → catalog;
- 24 ADR-038-specific architecture guards + 1 paired
  variant-count adjustment.

Substrate preserves:

- `ExecutionPlan::DropTable` identity-free (no field added);
- ADR 034 fragmenter `DropTableNotFragmentable` reject;
- ADR 034 catalog-only DROP fixtures (migrated to explicit
  `NoopStorageAdmin`);
- bound `TaskSpec` / `ExecutionPlan::Scan` stale-failure
  semantics (D10);
- `WendelonCatalog::drop_table` `AdminUnavailable` arm
  (D13 carry-forward to ADR 039).

No new SQL surface; no new IR variant; no new aggregate
kernel; no PBC opcode change; no new `LogicalPlan` /
`ExecutionPlan` variant; no new public field on
`WriteRequest` / `DeleteRowsRequest` / `ScanRequest`; no
Wendelon-side change beyond the existing CREATE client call
path; no `blake3` dependency; no external allocator crate.

**Ten-phase patch order.** Phase 0 is preflight audit +
reference fixture pass-count capture (byte-identical refactor
invariant baseline); Phase 1 adds `StorageAdmin` types + trait;
Phase 2 implements `InMemoryTableStorage` admin impl + the two
sentinel sinks (`UnsupportedStorageAdmin`,
`NoopStorageAdmin`); Phase 3 wires the coordinator admin sink
(strict default); Phase 4 rewrites the DROP routing helper for
storage-first / catalog-second ordering; Phase 5 introduces the
shared `WendelonDescriptorRegistry` and migrates storage
adapters off independent descriptor maps; Phase 6 lands
Wendelon CREATE Plan A; Phase 7 adds Toron functional fixtures
(8 required); Phase 8 adds 24 architecture guards; Phase 9
captures Plan A / Plan B landed-state per lift (Phase 11.5 from
Claude iter-0 promoted); Phase 10 is full verification.

## Implementation constraints

- **D1:** `StorageAdmin` trait has exactly one method,
  `drop_table_storage`. Request shape is context-only (8
  fields: `session_id`, `query_id`, `tenant_id`, `table_id`,
  `table_object_id`, `physical_descriptor_fingerprint`,
  `profile_hint`, `trace_context`).
- **D2:** `ExecutionPlan::DropTable` body stays
  `{ name, if_exists }`. No identity fields added.
- **D3:** Routing helper sequence: resolve → IF-EXISTS
  short-circuit → storage admin → catalog drop.
- **D4:** `InMemoryTableStorage::drop_table_storage` validates
  both `table_object_id` and `physical_descriptor_fingerprint`
  before removal; mismatch → `StorageError::DescriptorEvolved`.
- **D5:** `WendelonDescriptorRegistry` is the single source
  for catalog binding + storage adapter descriptors. No
  standalone `HashMap<TableId, TableSchemaDescriptor>` field
  in any Wendelon storage adapter post-Phase-5.
- **D6:** `WendelonCatalog::create_table` Plan A uses UUID v4
  for `TableObjectId`. No `blake3`, no name-hash, no
  fingerprint-hash.
- **D7:** All SQL-created Wendelon tables use
  `RowIdentityPolicy::AppendOnly`. Matches
  `mock_table_identity`.
- **D8:** `QueryCoordinator::new()`, `with_writer()`,
  `with_writer_and_deleter()` install
  `UnsupportedStorageAdmin` as default. Same for
  `DistributedQueryCoordinator` constructors.
- **D9:** No `invalidate_table` / `refresh_catalog` /
  `evict_descriptor` helper exists. Registry mutation IS the
  catalog refresh.
- **D10:** DROP paths in `catalog.rs`, `storage.rs`,
  `coordinator.rs` reference no plan-cache or bound-plan
  symbol. Stale-bound-plan failures surface at storage
  descriptor check.
- **D11:** `Catalog::drop_table` has exactly one production
  call site, inside the coordinator routing helper.
- **D12:** ADR 034's `FragmentError::DropTableNotFragmentable`
  unchanged.
- **D13:** `WendelonCatalog::drop_table` body unchanged
  (`AdminUnavailable` for Wendelon-known tables).
  Architecture guard 13 enforces.
- **D14:** ADR 039 = Wendelon DROP Migration Substrate.
  Phase 9 capture must record L3 as deferred (not Plan B —
  *explicitly deferred*).
- **Byte-identical refactor invariant** (carry-forward from
  ADR 035 D5 + ADR 036 D6 + ADR 037 D6): Phase 5 registry
  migration must preserve ADR 029 / 030 / 032 / 033 / 034 /
  036 / 037 fixture pass counts. Phase 0 captures the
  reference counts; Phase 10 verifies them.
- **Claude C1 carry-forward:** Plan A / Plan B lift naming
  must appear in the promoted ADR text (done — see ADR Status)
  AND in Phase 9 capture per-lift evidence.
- **Claude C2 carry-forward:** Do not invent
  `RowIdentityPolicy` mid-cycle. Use AppendOnly verbatim.
- **Claude C5 carry-forward:** Strict admin sink default.
  `NoopStorageAdmin` is a *test helper*, not a production
  constructor default.

## Touchpoint sweep

| File | Phase |
|---|---|
| `crates/toron/src/storage.rs` | 1 (StorageAdmin trait + request/outcome types), 2 (InMemoryTableStorage admin impl + Unsupported/Noop sinks), 5 (WendelonDescriptorRegistry — if it lives in storage.rs) |
| `crates/toron/src/coordinator.rs` | 3 (admin sink field + constructors), 4 (DROP routing helper) |
| `crates/toron/src/error.rs` | 6 (`CatalogError::AdminFailed { message }` for opaque Wendelon CREATE admin failures) |
| `crates/toron/src/catalog.rs` | 5 (WendelonDescriptorRegistry — if it lives in catalog.rs), 6 (WendelonCatalog::create_table Plan A) |
| `crates/toron/tests/drop_table_test.rs` | 7 (functional fixtures 1-7) |
| `crates/toron/tests/storage_admin_reclaim_test.rs` (NEW, optional) | 7 (focused ADR 038 fixtures if drop_table_test grows too large) |
| `crates/toron/tests/architecture_test.rs` | 8 (24 new guards + paired adjustment) |
| `crates/toron/tests/common/mod.rs` (or similar) | 2 (NoopStorageAdmin test helper) |
| `crates/toron/tests/wendelon_storage_execution_test.rs` | 6 (optional Wendelon CREATE Plan A fixtures) |

Modules created or extended:

- `crates/toron/src/storage.rs` — new types: `StorageAdmin`,
  `DropTableStorageRequest`, `DropTableStorageOutcome`,
  `UnsupportedStorageAdmin`.
- `crates/toron/src/catalog.rs` — new private type:
  `WendelonDescriptorRegistry` (or moved to
  `crates/toron/src/wendelon_descriptor.rs` if size
  warrants).

No new modules under `crates/toron/src/distributed/` — DROP
routing stays at the top-level coordinator and `execute_query`
distributed-routing decisions don't change.

## Helper reuse summary

| Helper | Source | ADR 038 use |
|---|---|---|
| `execute_catalog_plan` | ADR 029 → extended in ADR 034 | Phase 4: extended to accept admin sink + context, route DROP through storage-first/catalog-second sequence |
| `mock_table_identity` | ADR 029 | Phase 6: structural template for `WendelonCatalog::create_table` (same `RowIdentityPolicy::AppendOnly`, same `LocalityPolicy::default_table_spread`, same `RecordLayoutPolicy::MultiRow`, same `SchemaEvolutionPolicyV1::AdditiveOnly`) |
| `Client::submit_table_schema_update` | Wendelon `wendelon-client` | Phase 6: real call site for Wendelon CREATE Plan A (mirrors ADR 037 Phase 7 Plan A intent — this time landing) |
| `TableSchemaDescriptor::new_with_layout` | Wendelon | Phase 6: real descriptor construction (no ad-hoc fingerprint synthesis) |
| `TableSchemaUpdateRequest::create` | Wendelon `wendelon-client` | Phase 6: encodes the Create mutation |
| `Uuid::new_v4` | existing `uuid` crate (transitive via Wendelon) | Phase 6: `TableObjectId` allocation |
| `InMemoryTableStorage` `Mutex<HashMap>` shape | existing | Phase 2: whole-entry `tables.remove(&request.table_id)` |
| `StorageError::TableNotFound` / `DescriptorEvolved` | existing | Phase 2: reused for InMemoryTableStorage admin reject paths |
| `StorageError::Internal { message }` | existing | Phase 2: `UnsupportedStorageAdmin` capability-boundary diagnostic |
| `CatalogError::AdminFailed { message }` (NEW) | Phase 6 | Opaque Wendelon CREATE admin failures; keeps `AdminUnavailable` reserved for deferred DROP |
| `StorageAdmin` (NEW) | Phase 1 | One-method trait |
| `DropTableStorageRequest` (NEW) | Phase 1 | 8-field context-only request |
| `DropTableStorageOutcome` (NEW) | Phase 1 | `{ reclaimed_tables: usize }` |
| `UnsupportedStorageAdmin` (NEW) | Phase 2 | Default coordinator admin sink |
| `NoopStorageAdmin` (NEW, test-only) | Phase 2 | ADR 034 catalog-only fixtures install this explicitly |
| `WendelonDescriptorRegistry` (NEW) | Phase 5 | Shared `Arc<RwLock<WendelonDescriptorState>>` |
| `QueryCoordinator::with_storage_admin` (NEW) | Phase 3 | Builder for real admin sink |
| `QueryCoordinator::with_writer_deleter_and_admin` (NEW) | Phase 3 | Combined builder |
| `DistributedQueryCoordinator::with_storage_admin` (NEW) | Phase 3 | Mirrors local pattern |
| `DistributedQueryCoordinator::with_writer_deleter_and_admin` (NEW) | Phase 3 | Mirrors local pattern |

Eleven named additions: six types / traits (one test-only),
four constructors, and one `CatalogError` variant. Zero new
public storage request fields. Zero new `StorageError`
variants (reuses existing). Zero new `LogicalPlan` /
`ExecutionPlan` variants.

## Phase 0 — Preflight audit + reference fixture capture

Audit commands:

```sh
rg -n "AdminUnavailable|CREATE TABLE is not wired|DROP TABLE is not wired" crates/toron/src/catalog.rs
rg -n "TableSchemaUpdateRequest|submit_table_schema_update|SchemaMutationKindV1::Drop" crates/wendelon/crates/wendelon-client/src/lib.rs
rg -n "table schema drops require|apply_table_schema_update|TableSchemaCatalogV1" crates/wendelon/src
rg -n "struct WendelonStorageScan|struct WendelonStorageWrite|struct WendelonStorageDeleteRows" crates/toron/src/storage.rs
rg -n "struct InMemoryTableStorage|InMemoryTableData|deleted_row_key_ids" crates/toron/src/storage.rs
rg -n "StorageError::Internal|UnsupportedStorageWrite|UnsupportedStorageDeleteRows" crates/toron/src/storage.rs crates/toron/src/coordinator.rs
rg -n "HashMap<TableId, TableSchemaDescriptor>" crates/toron/src/
rg -n "Uuid::new_v4|uuid::Uuid" crates/toron/src/ crates/wendelon/
```

Expected findings (confirmed at ADR 038 iter-0):

- `WendelonCatalog::{create_table, drop_table}` both return
  `AdminUnavailable` at `crates/toron/src/catalog.rs:349-377`.
- Wendelon client `TableSchemaUpdateRequest::envelope`
  rejects `SchemaMutationKindV1::Drop`.
- Wendelon consensus `apply_table_schema_update` rejects
  table Drop.
- Three Wendelon storage adapters each own a
  `HashMap<TableId, TableSchemaDescriptor>` copy.
- `InMemoryTableStorage` data lives in
  `Mutex<HashMap<TableId, InMemoryTableData>>`.
- `StorageError::Internal { message }` is the existing
  unsupported-capability surface used by ADR 037's
  `UnsupportedStorageWrite` / `UnsupportedStorageDeleteRows`.
- `uuid` crate transitively available via Wendelon
  dependency chain.

Reference fixture pass-count capture (for Phase 10 byte-
identical refactor verification):

```sh
cargo test -p toron --test drop_table_test 2>&1 | tee /tmp/adr038_phase0_drop_table.txt
cargo test -p toron --test ctas_test 2>&1 | tee /tmp/adr038_phase0_ctas.txt
cargo test -p toron --test distributed_write_context_test 2>&1 | tee /tmp/adr038_phase0_distributed_write_context.txt
cargo test -p toron --test delete_test 2>&1 | tee /tmp/adr038_phase0_delete.txt
cargo test -p toron --test insert_test 2>&1 | tee /tmp/adr038_phase0_insert.txt
cargo test -p toron --test architecture_test 2>&1 | tee /tmp/adr038_phase0_architecture.txt
cargo test -p toron 2>&1 | tail -20 | tee /tmp/adr038_phase0_all.txt
```

Capture the "test result" line for each. Phase 10 reruns and
asserts byte-identical pass counts for the ADR 029 / 030 / 032
/ 033 / 034 / 036 / 037 fixtures.

## Phase 1 — `StorageAdmin` trait + request/outcome types

**File:** `crates/toron/src/storage.rs`.

Add (near existing `StorageWrite` / `StorageDeleteRows`):

```rust
#[derive(Debug, Clone)]
pub struct DropTableStorageRequest {
    pub session_id: SessionId,
    pub query_id: QueryId,
    pub tenant_id: TenantId,
    pub table_id: TableId,
    pub table_object_id: TableObjectId,
    pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
    pub profile_hint: Option<QueryProfileHint>,
    pub trace_context: Option<TraceContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropTableStorageOutcome {
    pub reclaimed_tables: usize,
}

#[async_trait::async_trait]
pub trait StorageAdmin: Send + Sync {
    async fn drop_table_storage(
        &self,
        request: DropTableStorageRequest,
    ) -> Result<DropTableStorageOutcome, StorageError>;
}
```

Negative scope (architecture guard 17 enforces):

- No `client_write_id`.
- No `durability` enum.
- No `await_finalization` bool.
- No retry-counter field.
- No `runtime_filter` field.
- No generic admin-command enum (single trait method only).

Do not add a new `StorageError` variant. The strict admin sink
uses the existing `StorageError::Internal { message }`
capability-boundary surface, matching ADR 037's unsupported
write/delete adapters.

## Phase 2 — `InMemoryTableStorage` reclaim + sentinel sinks

**File:** `crates/toron/src/storage.rs`.

`InMemoryTableStorage` admin impl:

```rust
#[async_trait::async_trait]
impl StorageAdmin for InMemoryTableStorage {
    async fn drop_table_storage(
        &self,
        request: DropTableStorageRequest,
    ) -> Result<DropTableStorageOutcome, StorageError> {
        let mut tables = self.tables.lock().expect("InMemoryTableStorage poisoned");
        let data = tables.get(&request.table_id).ok_or_else(|| {
            StorageError::TableNotFound { /* request.table_id */ }
        })?;
        if data.table_object_id != request.table_object_id {
            return Err(StorageError::DescriptorEvolved { /* identifying fields */ });
        }
        if data.physical_descriptor_fingerprint
            != request.physical_descriptor_fingerprint
        {
            return Err(StorageError::DescriptorEvolved { /* identifying fields */ });
        }
        tables.remove(&request.table_id);
        Ok(DropTableStorageOutcome { reclaimed_tables: 1 })
    }
}
```

`UnsupportedStorageAdmin`:

```rust
pub struct UnsupportedStorageAdmin;

#[async_trait::async_trait]
impl StorageAdmin for UnsupportedStorageAdmin {
    async fn drop_table_storage(
        &self,
        _request: DropTableStorageRequest,
    ) -> Result<DropTableStorageOutcome, StorageError> {
        Err(StorageError::Internal {
            message: "DROP TABLE physical reclaim requires a \
                      configured StorageAdmin".to_string(),
        })
    }
}
```

`NoopStorageAdmin` lives in a test-only module (e.g.,
`crates/toron/tests/common/mod.rs` or `crates/toron/src/testing.rs`
with `#[cfg(any(test, feature = "test-helpers"))]` gate). It
returns `Ok(DropTableStorageOutcome { reclaimed_tables: 0 })`
unconditionally and exists solely for ADR 034 catalog-only DROP
fixtures that intentionally test alias cleanup without exercising
physical reclaim.

Unit tests near existing in-memory storage tests:

- `inmem_admin_removes_existing_table`;
- `inmem_admin_unknown_table_returns_table_not_found`;
- `inmem_admin_object_id_mismatch_returns_descriptor_evolved`;
- `inmem_admin_fingerprint_mismatch_returns_descriptor_evolved`;
- `inmem_admin_drop_clears_tombstone_set`.

## Phase 3 — Coordinator admin sink (strict default)

**File:** `crates/toron/src/coordinator.rs`.

Add `admin: Arc<dyn StorageAdmin>` to both coordinators.

`QueryCoordinator`:

```rust
pub struct QueryCoordinator {
    catalog: Arc<dyn Catalog>,
    writer: Arc<dyn StorageWrite>,
    deleter: Arc<dyn StorageDeleteRows>,
    admin: Arc<dyn StorageAdmin>,
    // ... existing fields
}

impl QueryCoordinator {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            catalog,
            writer: Arc::new(UnsupportedStorageWrite),
            deleter: Arc::new(UnsupportedStorageDeleteRows),
            admin: Arc::new(UnsupportedStorageAdmin),
            // ...
        }
    }

    pub fn with_writer(mut self, writer: Arc<dyn StorageWrite>) -> Self {
        self.writer = writer;
        self
    }

    pub fn with_writer_and_deleter(
        mut self,
        writer: Arc<dyn StorageWrite>,
        deleter: Arc<dyn StorageDeleteRows>,
    ) -> Self {
        self.writer = writer;
        self.deleter = deleter;
        self
    }

    // NEW
    pub fn with_storage_admin(mut self, admin: Arc<dyn StorageAdmin>) -> Self {
        self.admin = admin;
        self
    }

    // NEW (test ergonomics)
    pub fn with_writer_deleter_and_admin(
        mut self,
        writer: Arc<dyn StorageWrite>,
        deleter: Arc<dyn StorageDeleteRows>,
        admin: Arc<dyn StorageAdmin>,
    ) -> Self {
        self.writer = writer;
        self.deleter = deleter;
        self.admin = admin;
        self
    }
}
```

Mirror the same pattern on `DistributedQueryCoordinator`.

Architecture guard 14 source-asserts that every
`QueryCoordinator::new` / `DistributedQueryCoordinator::new` /
`with_writer*` constructor sets `admin` to
`Arc::new(UnsupportedStorageAdmin)` by default.

## Phase 4 — DROP routing helper (storage-first, catalog-second)

**File:** `crates/toron/src/coordinator.rs`.

Extend `execute_catalog_plan` (or its renamed equivalent) to
accept the admin sink + execution context:

```rust
async fn execute_catalog_plan(
    catalog: &dyn Catalog,
    admin: &dyn StorageAdmin,
    context: &ExecutionContext,
    bound: &ExecutionPlan,
) -> Result<Vec<RecordBatch>, QueryError>
```

For `ExecutionPlan::CreateTable`:
- behavior unchanged (catalog-only); admin sink unused.

For `ExecutionPlan::DropTable { name, if_exists }`:

```rust
ExecutionPlan::DropTable { name, if_exists } => {
    let binding = match catalog.resolve_table(name).await {
        Ok(binding) => binding,
        Err(CatalogError::TableNotFound { .. }) if *if_exists => {
            return Ok(Vec::new());
        }
        Err(other) => return Err(QueryError::from(other)),
    };

    let request = DropTableStorageRequest {
        session_id: context.session_id,
        query_id: context.query_id,
        tenant_id: context.tenant_id,
        table_id: binding.table,
        table_object_id: binding.table_object_id,
        physical_descriptor_fingerprint: binding.physical_descriptor_fingerprint,
        profile_hint: context.profile_hint,
        trace_context: context.trace_context,
    };

    admin.drop_table_storage(request).await?;
    catalog.drop_table(name, false).await?;
    Ok(Vec::new())
}
```

Notes:

- `if_exists` short-circuit at step 2 ensures admin is never
  called for unknown tables. Architecture guard 4 source-
  asserts the ordering (resolve before admin).
- Step 5 calls `catalog.drop_table(name, false)` — the
  `false` (not `*if_exists`) is intentional: the resolve at
  step 1 already proved the table exists, so the catalog
  call should not silently no-op.
- Architecture guards 6 + 12 source-assert the storage-first
  / catalog-second sequence.
- Architecture guard 24 source-asserts exactly one
  `.drop_table(` call site in `coordinator.rs` (this one).

Distributed routing in `DistributedQueryCoordinator::execute_query`
preserves the existing pre-`fragment_plan` DROP routing
introduced by ADR 034 — distributed DROP routes through
`execute_catalog_plan` before reaching the fragmenter. Phase 4
amendment: distributed routing also threads admin + context
through.

## Phase 5 — Shared `WendelonDescriptorRegistry`

**File:** `crates/toron/src/catalog.rs` (or new sibling
`crates/toron/src/wendelon_descriptor.rs` if module size warrants).

Type sketch:

```rust
pub(crate) struct WendelonDescriptorRegistry {
    inner: Arc<RwLock<WendelonDescriptorState>>,
}

struct WendelonDescriptorState {
    by_ref: HashMap<TableRef, TableId>,
    by_id: HashMap<TableId, TableBinding>,
    descriptors: HashMap<TableId, TableSchemaDescriptor>,
    next_table_id: u64,
}

impl WendelonDescriptorRegistry {
    pub(crate) fn from_descriptors<I>(iter: I) -> Result<Self, CatalogError>
    where I: IntoIterator<Item = (TableId, TableSchemaDescriptor)> { /* ... */ }

    pub(crate) fn resolve_table(&self, name: &TableRef)
        -> Result<TableBinding, CatalogError> { /* read lock */ }

    pub(crate) fn resolve_table_id(&self, id: TableId)
        -> Result<TableBinding, CatalogError> { /* read lock */ }

    pub(crate) fn descriptor_for_table_id(&self, id: TableId)
        -> Result<TableSchemaDescriptor, CatalogError> { /* read lock */ }

    pub(crate) fn insert_descriptor(
        &self,
        name: TableRef,
        binding: TableBinding,
        descriptor: TableSchemaDescriptor,
    ) -> Result<(), CatalogError> { /* write lock; duplicate-name reject */ }

    pub(crate) fn remove_table_id(
        &self,
        id: TableId,
    ) -> Result<Option<TableBinding>, CatalogError> { /* write lock; ADR 039 use */ }
}
```

Migration:

- `WendelonCatalog::from_descriptors` constructs the shared
  registry once; returns both the catalog handle and a
  cloneable `Arc<WendelonDescriptorRegistry>` for storage
  adapters.
- `WendelonStorageScan`, `WendelonStorageWrite`,
  `WendelonStorageDeleteRows` constructors accept the
  registry handle and read through it instead of carrying
  independent `HashMap` copies. Existing constructors with
  `HashMap<TableId, TableSchemaDescriptor>` parameter
  deprecate; replace call sites with the registry-aware
  constructors.
- Architecture guard 8 source-asserts no standalone
  `HashMap<TableId, TableSchemaDescriptor>` field exists in
  Wendelon storage adapters after Phase 5.

This is the byte-identical refactor invariant phase. Phase 0
captured ADR 029 / 030 / 032 / 033 / 034 / 036 / 037 fixture
pass counts; Phase 10 re-runs and asserts identical counts.

## Phase 6 — Wendelon `CREATE TABLE` Plan A

**File:** `crates/toron/src/catalog.rs`.

`WendelonCatalog::create_table`:

```rust
async fn create_table(
    &self,
    name: &TableRef,
    schema: SchemaRef,
    if_not_exists: bool,
) -> Result<TableBinding, CatalogError> {
    // 1. Duplicate-name handling under registry write lock.
    if let Some(existing) = self.registry.try_resolve(name) {
        if if_not_exists {
            // ADR 029 IF NOT EXISTS equal-schema short-circuit.
            return Ok(existing);
        }
        return Err(CatalogError::TableAlreadyExists {
            table: format!("{}.{}",
                name.schema.as_deref().unwrap_or("default"),
                name.table),
        });
    }

    // 2. Allocate UUID-derived TableObjectId.
    let table_object_id = TableObjectId::new(Uuid::new_v4().into_bytes());

    // 3. Allocate legacy TableId from registry counter.
    let table_id = self.registry.allocate_table_id();

    // 4. Build TableSchemaDescriptor with AppendOnly policy.
    let descriptor = TableSchemaDescriptor::new_with_layout(
        table_object_id,
        1, // initial generation
        name.schema.as_deref().unwrap_or("default"),
        &name.table,
        &schema,
        RowIdentityPolicy::AppendOnly,                     // ← D7
        LocalityPolicy::default_table_spread(),
        RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
        SchemaEvolutionPolicyV1::AdditiveOnly,
    )
    .map_err(|error| CatalogError::FingerprintResolution {
        message: error.to_string(),
    })?;

    // 5. Submit to Wendelon.
    let request = TableSchemaUpdateRequest::create(
        self.tenant_id,
        descriptor.clone(),
    );
    self.client
        .submit_table_schema_update(request)
        .await
        .map_err(|e| map_wendelon_admin_error(e))?;

    // 6. Mutate registry on success.
    let binding = TableBinding {
        table: table_id,
        table_object_id,
        physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
    };
    self.registry.insert_descriptor(name.clone(), binding.clone(), descriptor)?;
    Ok(binding)
}
```

Open question for impl: `map_wendelon_admin_error` —
specifically, does Wendelon's `submit_table_schema_update` return
a structured error that includes duplicate-name and duplicate-
object-id signals? If yes, map to
`CatalogError::TableAlreadyExists` where appropriate. If no
(opaque error), surface as
`CatalogError::AdminFailed { message }`. Do not reuse
`AdminUnavailable`; ADR 038 reserves that signal for the
still-deferred Wendelon DROP not-wired path.

The Wendelon CREATE path now lifts:

- `crates/toron/src/catalog.rs:349-358` `AdminUnavailable` arm
  → removed. Architecture guard 9 source-asserts zero
  `AdminUnavailable` hits inside the `create_table` body.

`WendelonCatalog::drop_table` body remains unchanged
(D13 carry-forward; architecture guard 13 asserts).

## Phase 7 — Toron functional fixtures (8 required)

**File:** extend `crates/toron/tests/drop_table_test.rs` OR
add focused `crates/toron/tests/storage_admin_reclaim_test.rs`
(decision: extend if drop_table_test stays under ~600 lines
post-additions; otherwise new file). Claude iter-1 A3 carry-
forward — fixture-file location is a code-evidence decision,
not an ADR decision.

### Fixture 1: `drop_table_reclaims_in_memory_storage`

```rust
#[tokio::test]
async fn drop_table_reclaims_in_memory_storage() {
    let catalog = Arc::new(MockCatalog::new());
    let storage = Arc::new(InMemoryTableStorage::new());
    let coordinator = QueryCoordinator::new(catalog.clone())
        .with_writer_deleter_and_admin(
            storage.clone(), storage.clone(), storage.clone(),
        );

    execute_sql(&coordinator, "CREATE TABLE t (id Int64)").await.unwrap();
    execute_sql(&coordinator, "INSERT INTO t VALUES (1), (2), (3)").await.unwrap();

    // Confirm storage has the table.
    assert_eq!(storage.table_count(), 1);

    execute_sql(&coordinator, "DROP TABLE t").await.unwrap();

    // Confirm physical reclaim.
    assert_eq!(storage.table_count(), 0);

    // Confirm catalog reachability removed.
    let result = execute_sql(&coordinator, "SELECT * FROM t").await;
    assert!(matches!(result, Err(QueryError::CatalogError(
        CatalogError::TableNotFound { .. }
    ))));
}
```

### Fixture 2: `drop_table_if_exists_missing_does_not_call_storage_admin`

```rust
let spy = Arc::new(SpyStorageAdmin::new());
let coordinator = QueryCoordinator::new(catalog)
    .with_storage_admin(spy.clone());

execute_sql(&coordinator, "DROP TABLE IF EXISTS missing").await.unwrap();

assert_eq!(spy.call_count(), 0);
```

`SpyStorageAdmin` lives in the test helpers module.

### Fixture 3: `drop_table_storage_failure_preserves_catalog_reachability`

```rust
let failing = Arc::new(AlwaysFailStorageAdmin);
let coordinator = QueryCoordinator::new(catalog.clone())
    .with_writer_deleter_and_admin(
        storage.clone(), storage.clone(), failing,
    );

execute_sql(&coordinator, "CREATE TABLE t (id Int64)").await.unwrap();
let result = execute_sql(&coordinator, "DROP TABLE t").await;
assert!(matches!(result, Err(QueryError::StorageError(
    StorageError::Internal { .. } // or whichever AlwaysFail returns
))));

// Catalog still has the table.
let binding = catalog.resolve_table(&TableRef::unqualified("t")).await;
assert!(binding.is_ok());
```

### Fixture 4: `drop_table_stale_scan_returns_table_not_found`

```rust
let coordinator = /* wired with InMemoryTableStorage admin */;
execute_sql(&coordinator, "CREATE TABLE t (id Int64)").await.unwrap();
execute_sql(&coordinator, "INSERT INTO t VALUES (1)").await.unwrap();

// Bind a SELECT plan against the live table.
let bound = bind(&coordinator, "SELECT id FROM t").await.unwrap();

// First execution succeeds.
let first = coordinator.execute_bound(&bound).await.unwrap();
assert_eq!(first.len(), 1);

// Drop.
execute_sql(&coordinator, "DROP TABLE t").await.unwrap();

// Re-execute the same bound plan — stale-bound-plan failure mode.
let stale = coordinator.execute_bound(&bound).await;
assert!(matches!(stale, Err(QueryError::StorageError(
    StorageError::TableNotFound { .. }
))));
```

### Fixture 5: `distributed_drop_table_routes_storage_admin_before_fragment`

```rust
let spy_admin = Arc::new(SpyStorageAdmin::new());
let spy_fragmenter = Arc::new(SpyFragmenter::new());
let coordinator = DistributedQueryCoordinator::new(catalog, /* ... */)
    .with_storage_admin(spy_admin.clone())
    .with_fragmenter(spy_fragmenter.clone());

execute_sql(&coordinator, "CREATE TABLE t (id Int64)").await.unwrap();
execute_sql(&coordinator, "DROP TABLE t").await.unwrap();

assert_eq!(spy_admin.call_count(), 1);
assert_eq!(spy_fragmenter.fragment_plan_count(), 0);
```

### Fixture 6: `drop_with_no_admin_sink_returns_capability_diagnostic`

```rust
let coordinator = QueryCoordinator::new(catalog); // no .with_storage_admin
execute_sql(&coordinator, "CREATE TABLE t (id Int64)").await.unwrap();
let result = execute_sql(&coordinator, "DROP TABLE t").await;
assert!(matches!(result, Err(QueryError::StorageError(
    StorageError::Internal { message }
)) if message == "DROP TABLE physical reclaim requires a configured StorageAdmin"));
```

### Fixture 7: `drop_admin_failure_then_retry_succeeds`

```rust
let toggle = Arc::new(ToggleStorageAdmin::new(/* fail first, succeed second */));
let coordinator = QueryCoordinator::new(catalog.clone())
    .with_writer_deleter_and_admin(
        storage.clone(), storage.clone(), toggle.clone(),
    );

execute_sql(&coordinator, "CREATE TABLE t (id Int64)").await.unwrap();

// First DROP fails at storage admin step.
let first = execute_sql(&coordinator, "DROP TABLE t").await;
assert!(first.is_err());

// Table still in catalog (Codex iter-1 D3 partial-failure semantics).
let binding = catalog.resolve_table(&TableRef::unqualified("t")).await;
assert!(binding.is_ok());

// Second DROP succeeds (toggle now succeeds; routing helper re-runs storage admin first).
execute_sql(&coordinator, "DROP TABLE t").await.unwrap();

// Now both gone.
let binding = catalog.resolve_table(&TableRef::unqualified("t")).await;
assert!(matches!(binding, Err(CatalogError::TableNotFound { .. })));
```

### Fixture 8: `wendelon_create_uses_append_only_row_identity_policy` (source-read)

```rust
#[test]
fn wendelon_create_uses_append_only_row_identity_policy() {
    let source = std::fs::read_to_string("src/catalog.rs").unwrap();
    let body = extract_function_body(
        &source,
        "impl Catalog for WendelonCatalog",
        "async fn create_table",
    );
    assert!(body.contains("RowIdentityPolicy::AppendOnly"));
    assert!(!body.contains("RowIdentityPolicy::PrimaryKey"));
    assert!(!body.contains("RowIdentityPolicy::ByOrdinal"));
}
```

This fixture doubles as architecture guard 21 (the source-
read assertion fits the architecture_test.rs idiom).

### Optional Wendelon Plan A fixtures (Phase 6 conditional)

If the Wendelon client test harness supports admin operations:

- `wendelon_create_submits_table_schema_update_request` —
  intercepts the client call; asserts payload contains a
  real `TableSchemaDescriptor`.
- `wendelon_create_duplicate_name_maps_to_table_already_exists`
  — submits two `CREATE TABLE foo`; second returns
  `CatalogError::TableAlreadyExists`.

These live in `crates/toron/tests/wendelon_storage_execution_test.rs`
and gate on the existing Wendelon test fixture availability
(matches ADR 037 optional Wendelon fixture pattern).

## Phase 8 — Architecture guards

**File:** `crates/toron/tests/architecture_test.rs`.

Add 24 guards (matrix in ADR §"Architecture Guard Set"). Each
guard is a `#[test] fn adr038_*` with source-read assertions.
Group at the end of the existing `mod adr037 { /* ... */ }` /
`mod adr038 { /* ... */ }` module hierarchy.

Paired adjustment:
`adr038_execution_plan_variant_count_unchanged` — extend the
existing variant-count catch-all to assert `LogicalPlan` and
`ExecutionPlan` variant counts unchanged in this cycle (15
`LogicalPlan` variants, 13 `ExecutionPlan` variants — verify
at Phase 0).

Implementation sketch for guard 4
(`adr038_coordinator_resolves_drop_identity_at_execution`):

```rust
#[test]
fn adr038_coordinator_resolves_drop_identity_at_execution() {
    let source = read_to_string("src/coordinator.rs").unwrap();
    let helper = extract_function_body(&source, "async fn execute_catalog_plan");
    let drop_arm = extract_match_arm(&helper, "ExecutionPlan::DropTable");
    // resolve_table must appear before admin.drop_table_storage
    let resolve_pos = drop_arm.find("resolve_table").expect("resolve missing");
    let admin_pos = drop_arm.find("drop_table_storage").expect("admin missing");
    assert!(resolve_pos < admin_pos);
}
```

Implementation sketch for guard 8
(`adr038_wendelon_descriptor_registry_shared_by_all_adapters`):

```rust
#[test]
fn adr038_wendelon_descriptor_registry_shared_by_all_adapters() {
    let storage = read_to_string("src/storage.rs").unwrap();
    // None of the three Wendelon storage adapter structs may
    // own a standalone descriptor HashMap.
    for adapter in &[
        "struct WendelonStorageScan",
        "struct WendelonStorageWrite",
        "struct WendelonStorageDeleteRows",
    ] {
        let body = extract_struct_body(&storage, adapter);
        assert!(!body.contains("HashMap<TableId, TableSchemaDescriptor>"),
            "{} should read descriptors through WendelonDescriptorRegistry",
            adapter);
        assert!(body.contains("WendelonDescriptorRegistry"),
            "{} should reference WendelonDescriptorRegistry",
            adapter);
    }
}
```

Remaining guard implementations follow the same source-read
pattern from ADR 037 guard set.

## Phase 9 — Plan A / Plan B landed-state capture

After Phases 1-8 land, capture the actual landed state per
lift. This is the **Claude C1 carry-forward** — ADR 037
shipped without it and the roadmap drifted from the code.

Capture script:

```sh
# L1 — Toron-local DROP physical reclaim
rg -n "impl StorageAdmin for InMemoryTableStorage" crates/toron/src/storage.rs
rg -n "tables.remove\(&request.table_id\)" crates/toron/src/storage.rs

# L2 — Wendelon CREATE Plan A
rg -n "AdminUnavailable" crates/toron/src/catalog.rs
rg -n "submit_table_schema_update" crates/toron/src/catalog.rs
rg -n "Uuid::new_v4" crates/toron/src/catalog.rs
rg -n "RowIdentityPolicy::AppendOnly" crates/toron/src/catalog.rs

# L3 — Wendelon DROP (must remain AdminUnavailable; deferred to ADR 039)
rg -n "DROP TABLE is not wired" crates/toron/src/catalog.rs
```

Expected post-implementation evidence:

| Lift | Plan A | Plan B | Deferred | Landed evidence |
|------|--------|--------|----------|-----------------|
| L1 | ✓ | — | — | `impl StorageAdmin for InMemoryTableStorage` at `storage.rs:NNN`; `tables.remove(&request.table_id)` present |
| L2 | ✓ | — | — | `submit_table_schema_update` call at `catalog.rs:NNN`; `Uuid::new_v4` call at `catalog.rs:NNN`; `RowIdentityPolicy::AppendOnly` at `catalog.rs:NNN`; ZERO `AdminUnavailable` hits inside `create_table` body |
| L3 | — | — | ✓ ADR 039 | `WendelonCatalog::drop_table` body unchanged from ADR 034 — `AdminUnavailable` arm at `catalog.rs:NNN` preserved verbatim |

If any landed-state row does not match the expected pattern,
**the implementation PR is not Phase-9 complete** until either:

- the code is brought in line with the promoted ADR, OR
- the promoted ADR text is updated to reflect the actual
  landed state (rare; should be a council decision, not a
  silent commit).

## Phase 10 — Full verification

```sh
cargo fmt --check
cargo test -p toron --test architecture_test adr038
cargo test -p toron --test drop_table_test
cargo test -p toron --test storage_admin_reclaim_test  # if separate file
cargo test -p toron --test ctas_test
cargo test -p toron --test distributed_write_context_test
cargo test -p toron --test delete_test
cargo test -p toron --test insert_test
cargo test -p toron
```

Byte-identical refactor invariant verification:

```sh
cargo test -p toron --test drop_table_test 2>&1 | tee /tmp/adr038_phase10_drop_table.txt
cargo test -p toron --test ctas_test 2>&1 | tee /tmp/adr038_phase10_ctas.txt
cargo test -p toron --test distributed_write_context_test 2>&1 | tee /tmp/adr038_phase10_distributed_write_context.txt
cargo test -p toron --test delete_test 2>&1 | tee /tmp/adr038_phase10_delete.txt
cargo test -p toron --test insert_test 2>&1 | tee /tmp/adr038_phase10_insert.txt
diff /tmp/adr038_phase0_drop_table.txt /tmp/adr038_phase10_drop_table.txt
diff /tmp/adr038_phase0_ctas.txt /tmp/adr038_phase10_ctas.txt
diff /tmp/adr038_phase0_distributed_write_context.txt /tmp/adr038_phase10_distributed_write_context.txt
diff /tmp/adr038_phase0_delete.txt /tmp/adr038_phase10_delete.txt
diff /tmp/adr038_phase0_insert.txt /tmp/adr038_phase10_insert.txt
```

The diffs must report identical pass counts (only the *new*
ADR 038 fixtures may change the totals in `drop_table_test`).

Architecture guard verification:

```sh
cargo test -p toron --test architecture_test 2>&1 | grep "adr038_"
```

Must show 24 ADR-038-specific guards green + paired
variant-count adjustment green.

Wendelon-side verification — none required for ADR 038. ADR
039 owns Wendelon-side changes.

Optional Plan A Wendelon fixture verification:

```sh
cargo test -p toron --test wendelon_storage_execution_test
```

## Risks

| ID | Risk | Mitigation in plan |
|----|------|---------------------|
| R1 | Wendelon CREATE Plan A `submit_table_schema_update` returns an unanticipated error shape | Phase 6 maps known error patterns (duplicate-name, duplicate-object-id) to `CatalogError` variants; opaque errors surface as new `CatalogError::AdminFailed`. Phase 0 audit verifies the client surface shape. |
| R2 | `WendelonDescriptorRegistry` `RwLock` introduces measurable read-path contention | Phase 5 bench captures `cargo bench` for any existing Wendelon scan benchmarks pre/post; abort registry migration if read-path regression exceeds 1%. |
| R3 | Phase 5 byte-identical refactor breaks an ADR 029-037 fixture | Phase 0 captures reference counts; Phase 10 diffs verify; rollback path is to keep adapter-local maps and route registry through a `Deref`-like façade until convergence. |
| R4 | Phase 3 strict-admin default breaks ADR 034 catalog-only DROP fixtures noisily | `NoopStorageAdmin` test helper migration is part of Phase 2; ADR 034 fixtures wire it explicitly. If migration scope exceeds 30 fixture touchpoints, escalate to council for in-cycle scope review. |
| R5 | Phase 6 UUID v4 collision in test suite (statistically improbable but reproducibly bad if seeded) | Tests do not seed RNG; `Uuid::new_v4` uses OS entropy. If a test deliberately uses a fixed UUID, mark with a doc comment and architecture guard 18 exception list. |
| R6 | Phase 9 capture reveals L2 landed as Plan B (CREATE still returns `AdminUnavailable`) | Phase 6 includes a hard test gate: `adr038_wendelon_create_table_admin_unavailable_removed` is a required guard, not optional. If it fails at Phase 8/10, the implementation is incomplete — escalate to council. |
| R7 | Phase 4 routing helper extension breaks the ADR 034 distributed-DROP catalog-only path | Phase 4 preserves the pre-`fragment_plan` distributed DROP routing introduced by ADR 034; only the helper signature widens. Phase 7 Fixture 5 source-asserts the pre-fragment routing. |
| R8 | Phase 5 registry move-in fails for `WendelonStorageDeleteRows` (e.g., needs special construction order) | Phase 5 is a single-commit migration covering all three adapters; if one adapter resists registry migration, the whole phase deferred to a follow-up — ADR 038 ships L1 + Wendelon CREATE Plan A but skips Phase 5; Phase 9 capture records the partial landed state and the council decides whether to ship or rework. |
