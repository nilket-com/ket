# Implementation Plan: ADR 035 — Row Identity / Predicate-Delete Substrate

**Companion ADR:** `plans/adr/035_row_identity_predicate_delete_substrate.md`

## Goal

Introduce the private substrate that ADR 036 (SQL
`DELETE WHERE`) will compose over:

- `RowIdentityProjection` enum on `ScanRequest`
  (Toron-side) with a parallel definition on
  Wendelon's table-query metadata path and
  `wendelon-client` options;
- typed `DeleteRowsRequest` carrying `Vec<Identity>`
  at the trait boundary;
- four-counter `DeleteRowsOutcome`;
- dedicated `StorageDeleteRows` trait separate from
  `StorageWrite`;
- in-memory tombstones with deterministic hash-free
  identity layout;
- privileged identity-projection path through
  Wendelon `TableQueryRequestMeta` /
  `TableQueryRequest` / `TableQueryOptions`;
- `crates/toron/src/coordinator/delete.rs`
  materializer extracting `Vec<Identity>` from a
  candidate scan;
- explicit reject for combining identity projection
  with user projection;
- Wendelon delete adapter calling the high-level
  `wendelon_client::Client::table_delete` wrapper
  with `await_finalization: true`.

This is a substrate ADR. **No SQL `DELETE WHERE`
syntax**, no `Statement::Delete` /
`LogicalPlan::Delete` / `ExecutionPlan::Delete` IR
variants, no distributed DELETE coordinator routing,
no SQL row count, no RETURNING. All of that lands in
ADR 036.

Preserve:

- All ADR 029 catalog metadata-only invariants and
  storage-synthetic-name reject discipline.
- All ADR 030 VALUES INSERT architecture guards;
  `ExecutionPlan::Insert` stays expression-free.
- All ADR 030 D7 prohibition on Toron-side synthetic
  column construction (extended: Toron mock identity
  uses opaque-not-cryptographic layout; no `blake3`;
  Wendelon-side identity generation untouched).
- All ADR 031 `BoundExprProgram` invariants.
- All ADR 032 InsertSelect architecture guards;
  `materialize_insert_select_batch` still returns
  `Ok(None)` for zero rows.
- All ADR 033 CTAS architecture guards;
  `materialize_empty_record_batch` sibling helper.
- ADR 034 catalog `drop_table` + `lower_ddl_table_name`
  helper + 17 architecture guards.
- ADR 015 cycle properties: narrow-detector
  architecture guards; named-reject sites; no silent
  acceptance.

No new SQL syntax in this ADR; no new IR variants;
no new aggregate kernel; no PBC opcode change; no
fragmenter detector change for SELECT paths; no
distributed coordinator routing for delete; no new
`StorageError` variant; no new `RequiredColumnAxis`
variant on Wendelon; no new Wendelon wire RPC or
identity-specific table-query method; no `blake3`
dependency; no external allocator crate; no storage
reclaim trait or helper.

**Nine-phase patch order.** Phases 1-3 are Toron-
side storage substrate; Phase 4 is the cross-client
delete wrapper + Toron adapter; Phases 5-7 are Toron
coordinator wiring and fixtures; Phase 8 is
architecture guards; Phase 9 is Wendelon table-query
projection + full verification. Promotion-time
housekeeping (Phase 10) is out of implementation PR
scope.

## Implementation constraints (Codex iter-0 + iter-1 + Claude iter-0/1 + iter-2 wording)

- **D1:** `RowIdentityProjection` enum with `None` +
  `Append` variants. Toron-side definition in
  `crates/toron/src/storage.rs`; Wendelon-side
  parallel definition in `crates/wendelon/src/client/core.rs`
  (or wherever the existing `TableQueryRequest`
  lives). **No cross-crate re-export.**
- **D2:** No SQL DELETE syntax / IR variants in this
  ADR.
- **D3:** Identity column is transient; validated and
  stripped in the materializer; never reaches result
  paths or SQL projection resolution.
- **D4 (Claude iter-1 C7):** Wendelon table-query
  path carries `row_identity: RowIdentityProjection`
  through the public `wendelon-client::TableQueryOptions`,
  internal `wendelon::client::TableQueryRequest`, and
  wire-visible `TableQueryRequestMeta` byte. PBC
  PROJECT storage-column reject preserved verbatim
  for ordinary callers.
- **D5:** `DeleteRowsRequest` carries
  `row_key_ids: Vec<wendelon::schema::Identity>`. No
  `RecordBatch` payload.
- **D6:** `DeleteRowsOutcome` preserves four delete
  accounting fields: `requested` from the Toron
  request length plus Wendelon's three response
  counters (`deleted` / `already_deleted` /
  `not_found`). Do not reuse `WriteOutcome`.
- **D7:** Dedicated `StorageDeleteRows` trait;
  separate from `StorageWrite`. `StorageWrite` trait
  shape byte-identical.
- **D8:** Empty candidate set short-circuits in
  coordinator delete-path helper without calling
  `StorageDeleteRows::delete_rows`. Wendelon rejects
  empty `row_key_ids`.
- **D9:** `InMemoryTableData` extended with
  tombstones (HashSet<Identity>). No physical
  removal.
- **D10 (Claude iter-0 C3):** Deterministic hash-
  free mock identity layout `[table_object_id; 16] +
  [zero; 8] + [ordinal_be; 8]`. No `blake3` / `hash`
  / `sha`.
- **D11 (Claude iter-0 C4 + iter-1 C8 + iter-2 pin):**
  Storage adapters reject
  `row_identity == Append && projected_columns.is_some()`
  with `StorageError::Internal { message: "scan with
  row_identity=Append cannot be combined with
  projected_columns" }`. Reuse existing variant;
  do not add `StorageError::InvalidScanRequest`.
- **D12 (Claude iter-0 C1):** Candidate materializer
  ships in ADR 035 via `crates/toron/src/coordinator/delete.rs`
  submodule.
- **D13:** Wendelon delete adapter calls the
  high-level `wendelon_client::Client::table_delete`
  wrapper (singular, not `table_delete_rows`) with
  `await_finalization: true`. Toron must not
  construct `TableDeleteRowsRequestMeta` directly.
- **D14:** PBC and runtime filters remain advisory.
- **D15 (Claude iter-0 C6):** ADR 036 sequencing pin
  for SQL `DELETE WHERE`.
- **Claude iter-1 C9 (iter-2 mechanism pin):**
  Architecture guard #17 cross-ring boundary check
  reads Wendelon source via `wendelon_manifest_dir()`
  helper (Toron-side architecture test convention).
- **Claude iter-1 A3 carry-forward from ADR 034:**
  New test file `crates/toron/tests/delete_substrate_test.rs`
  (one substrate cycle, one test file).
- **No `bumpalo` / `typed-arena` / `slab` /
  `generational-arena` / `blake3` in Toron
  `Cargo.toml`.**
- **No storage reclaim trait or helper introduced.**

## Touchpoint sweep

| File | Phase |
|---|---|
| `crates/toron/src/storage.rs` | 1, 2, 3, 4, 5 |
| `crates/toron/src/coordinator.rs` (or `coordinator/mod.rs` if migrated) | 6 |
| `crates/toron/src/coordinator/delete.rs` (NEW) | 6 |
| `crates/toron/src/exec.rs` | 1 (ScanRequest construction sites) |
| `crates/toron/tests/delete_substrate_test.rs` (NEW) | 7 |
| `crates/toron/tests/architecture_test.rs` | 8 |
| `crates/wendelon/crates/wendelon-client/src/lib.rs` | 4, 9 (`Client::table_delete` wrapper; `TableQueryOptions.row_identity`) |
| `crates/wendelon/src/client/core.rs` | 9 (TableQueryRequest field + RowIdentityProjection enum) |
| `crates/wendelon/src/schema/table.rs` | 9 (`TableQueryRequestMeta` wire-visible row-identity byte) |
| `crates/wendelon/src/network/data_server.rs` | 9 (decode/use row-identity metadata; privileged identity projection) |
| Wendelon table-query handler/storage helpers (additional paths determined at Phase 9 audit) | 9 (privileged identity projection) |
| `crates/wendelon/tests/` (path determined at Phase 9) | 9 (3 fixtures) |

## Helper reuse summary

| Helper | Source | ADR 035 use |
|---|---|---|
| `wendelon::schema::Identity` | Wendelon (existing) | Carried as typed `Vec<Identity>` at delete trait boundary |
| `wendelon::schema::STORAGE_ROW_KEY_ID_COLUMN` constant (`"__row_key_id__"`) | Wendelon (existing) | Used by materializer for column-name validation; used by InMemoryTableStorage scan-append |
| `TableObjectId::as_bytes()` | Wendelon (existing) | Used in `in_memory_row_identity` for prefix bytes |
| `Identity::new([u8; 32])` | Wendelon (existing) | Used in `in_memory_row_identity` to construct identity |
| `wendelon::client::WendelonClient::table_delete` / `TABLE_DELETE_REQ` | Wendelon (existing) | Existing internal wire delete path wrapped by `wendelon-client::Client::table_delete` |
| `wendelon-client::Client::table_delete` | NEW (Phase 4 wrapper) | Toron delete adapter invokes this high-level method |
| `TableDeleteRowsRequest { row_key_ids }` | Wendelon (existing internal) | Built inside `wendelon-client`, not inside Toron |
| `TableDeleteRowsResponseMeta { deleted_count, already_deleted_count, not_found_count }` | Wendelon (existing) | Three response counters mapped into `DeleteRowsOutcome`; `requested` comes from request length |
| `wendelon_manifest_dir()` test helper | Toron architecture test infrastructure (existing or new at Phase 8) | Used by guard #17 to read Wendelon source |
| `RowIdentityProjection` | NEW (Phase 1; Toron) | Enum on `ScanRequest` |
| `RowIdentityProjection` | NEW (Phase 9; Wendelon) | Enum on `TableQueryOptions` / `TableQueryRequest` (parallel definition) |
| `DeleteRowsRequest` | NEW (Phase 2; Toron) | Typed delete request |
| `DeleteRowsOutcome` | NEW (Phase 2; Toron) | Four-counter outcome |
| `StorageDeleteRows` | NEW (Phase 2; Toron) | Trait |
| `in_memory_row_identity` | NEW (Phase 3; Toron) | Mock identity helper |
| `materialize_delete_row_key_ids` | NEW (Phase 6; Toron) | Coordinator materializer |
| `WendelonStorageDeleteRows` | NEW (Phase 5; Toron) | Wendelon delete adapter |

One Wendelon-side enum + one table-query metadata
byte + one client-option field + one high-level
delete wrapper + Toron-side new enum + four new
types + one new trait + one new helper + one new
submodule + one new adapter. Net new code is bounded.

## Phase 0 — Preflight audit

```sh
# Confirm Delete / RowIdentityProjection symbols are unused
rg -n "RowIdentityProjection|DeleteRowsRequest|DeleteRowsOutcome|StorageDeleteRows|materialize_delete_row_key_ids|in_memory_row_identity" crates/toron/src crates/toron/tests
# Expected: zero hits

# Confirm coordinator/delete.rs does not yet exist
ls crates/toron/src/coordinator/ 2>/dev/null
# Expected: directory does not exist, or contains only existing files

# Confirm ADR 029 / 030 / 031 / 032 / 033 / 034 baseline green
cargo test -p toron --test architecture_test
cargo test -p toron --test create_table_test
cargo test -p toron --test insert_test
cargo test -p toron --test ctas_test
cargo test -p toron --test drop_table_test

# Confirm no Toron-side blake3 / hash imports for identity
rg -n "use.*Identity::blake3|Identity::blake3|use.*blake3|::sha2|hash_identity" crates/toron/src
# Expected: zero hits (Toron must not construct identities via hashes)

# Confirm Wendelon TableQueryRequest does not yet have row_identity field
rg -n "row_identity|RowIdentityProjection" \
  crates/wendelon/src/client/core.rs \
  crates/wendelon/src/schema/table.rs \
  crates/wendelon/crates/wendelon-client/src/lib.rs
# Expected: zero hits in the table-query surfaces

# Confirm Wendelon insert_required_column reject string is currently in place
rg -n "cannot appear in PBC PROJECT output" crates/wendelon/src/storage/columnar.rs
# Expected: one hit (the reject string we must preserve)

# Confirm Wendelon Client::table_delete (singular) is the current method name
rg -n "pub async fn table_delete\b|pub async fn table_delete_rows\b" crates/wendelon/src/client/core.rs
# Expected: table_delete present; table_delete_rows absent (helper only, not method)

# Confirm the public wendelon-client wrapper does not yet expose table_delete
rg -n "pub async fn table_delete\b|TableDeleteRowsReceipt|TableDeleteReceipt" crates/wendelon/crates/wendelon-client/src/lib.rs
# Expected: zero hits; ADR 035 adds this high-level wrapper over existing TABLE_DELETE_REQ

# Confirm Wendelon Identity is [u8; 32]
rg -n "pub struct Identity\(\[u8; 32\]\)" crates/wendelon/src/schema/units.rs
# Expected: one hit

# Confirm Wendelon TableObjectId is [u8; 16]
rg -n "pub struct TableObjectId\(\[u8; 16\]\)" crates/wendelon/src/schema/table.rs
# Expected: one hit

# Confirm no shadow dependencies
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits

# Confirm StorageError current variants
rg -n "pub enum StorageError|InvalidScanRequest" crates/toron/src/error.rs
# Expected: enum present; no InvalidScanRequest variant
```

Expected baseline:

- No existing `RowIdentityProjection` /
  `DeleteRowsRequest` / `StorageDeleteRows` symbols.
- `coordinator/delete.rs` submodule does not exist.
- No Toron-side `Identity::blake3*` usage.
- Wendelon table-query request / metadata / public
  options surfaces have no `row_identity` field yet.
- Wendelon `insert_required_column` reject string in
  place.
- Internal `wendelon::client::WendelonClient::table_delete`
  (singular) is the current wire method name; the
  public `wendelon-client` wrapper does not yet expose
  a matching high-level method.
- `Identity = [u8; 32]`; `TableObjectId = [u8; 16]`.
- No new `StorageError::InvalidScanRequest` variant
  exists (we reuse `Internal { message }`).

Preserve verbatim:

- All existing `StorageScan` / `StorageWrite` /
  `Catalog` / `Executor` / `Coordinator` semantics.
- All existing Wendelon-side `insert_required_column`
  PBC PROJECT reject discipline.
- All ADR 029 / 030 / 031 / 032 / 033 / 034
  invariants.
- All ADR 015 cycle properties.

## Phase 1 — Scan-request enum field

**Files:** `crates/toron/src/storage.rs`,
`crates/toron/src/exec.rs`, plus any other
`ScanRequest { ... }` construction sites the compiler
flags.

**Tasks:**

1. Add to `crates/toron/src/storage.rs`:

   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
   pub enum RowIdentityProjection {
       None,
       Append,
   }

   impl Default for RowIdentityProjection {
       fn default() -> Self {
           Self::None
       }
   }
   ```

2. Add the field to `ScanRequest`:

   ```rust
   pub struct ScanRequest {
       // ... existing fields ...
       pub runtime_filter: Option<RuntimeFilterDescriptor>,
       pub profile_hint: Option<QueryProfileHint>,
       pub trace_context: Option<TraceContext>,
       pub morsel: Option<(MorselId, u32)>,
       pub row_identity: RowIdentityProjection,
   }
   ```

3. Update every existing `ScanRequest { ... }`
   construction site to set
   `row_identity: RowIdentityProjection::None`.
   The compiler will flag every site (struct is not
   `#[non_exhaustive]` for callers in-crate). Expected
   sites:

   - `crates/toron/src/exec.rs` (scan plan execution);
   - `crates/toron/src/coordinator.rs` (scan plan
     bootstrap);
   - `crates/toron/src/storage.rs` (any in-test
     constructions);
   - any architecture-test fixture that constructs a
     `ScanRequest` literal.

4. Add the D11 reject at the in-memory scan boundary:

   - if `RowIdentityProjection::None`: existing
     behavior unchanged;
   - if `RowIdentityProjection::Append &&
     request.projected_columns.is_some()`: return
     `Err(StorageError::Internal { message: "scan
     with row_identity=Append cannot be combined with
     projected_columns".to_string() })` (D11);
   - if `RowIdentityProjection::Append` without an
     identity context (the legacy bare
     `InMemoryStorageScan { table_data }` helper):
     return `StorageError::Internal` with a stable
     diagnostic such as `"row identity projection
     requires table storage identity context"`.
     The real append path lands in Phase 3 on
     `InMemoryTableStorage`, where row ids exist.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron storage::tests
```

Expected interim state: the legacy bare
`InMemoryStorageScan` keeps existing SELECT behavior
for `RowIdentityProjection::None` and rejects
`Append` because it has no stored row ids. The
compiler enforces every construction site update.

## Phase 2 — Delete request / outcome types + trait

**File:** `crates/toron/src/storage.rs`.

**Tasks:**

1. Add `DeleteRowsRequest`:

   ```rust
   #[derive(Debug)]
   pub struct DeleteRowsRequest {
       pub session_id: SessionId,
       pub query_id: QueryId,
       pub tenant_id: TenantId,
       pub table_id: TableId,
       pub table_object_id: TableObjectId,
       pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
       pub row_key_ids: Vec<wendelon::schema::Identity>,
       pub profile_hint: Option<QueryProfileHint>,
       pub trace_context: Option<TraceContext>,
   }
   ```

   **No `RecordBatch` field.** Mirrors `WriteRequest`
   target-metadata shape sans `payload`.

2. Add `DeleteRowsOutcome`:

   ```rust
   #[non_exhaustive]
   #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
   pub struct DeleteRowsOutcome {
       pub requested: usize,
       pub deleted: usize,
       pub already_deleted: usize,
       pub not_found: usize,
   }
   ```

   All four counters required. **Do not reuse
   `WriteOutcome`.**

3. Add the trait:

   ```rust
   #[async_trait::async_trait]
   pub trait StorageDeleteRows: Send + Sync {
       async fn delete_rows(
           &self,
           request: DeleteRowsRequest,
       ) -> Result<DeleteRowsOutcome, StorageError>;
   }
   ```

   Separate from `StorageWrite`. `StorageWrite` shape
   unchanged.

**Verification:**

```sh
cargo check -p toron
```

No call sites yet; the trait + types compile but are
unused outside the public surface until Phases 3 + 5.

## Phase 3 — In-memory storage tombstones + identity layout

**File:** `crates/toron/src/storage.rs`.

**Tasks:**

1. Extend `InMemoryTableData`:

   ```rust
   struct InMemoryTableData {
       table_object_id: TableObjectId,
       physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
       batch: RecordBatch,
       row_key_ids: Vec<wendelon::schema::Identity>,
       deleted_row_key_ids: std::collections::HashSet<wendelon::schema::Identity>,
       next_row_key_ordinal: u64,
   }
   ```

   - `row_key_ids` is parallel to `batch` row order;
     length matches `batch.num_rows()`.
   - `deleted_row_key_ids` is a HashSet of
     tombstoned identities.
   - `next_row_key_ordinal` is a monotonic counter
     for identity generation.

2. Add the identity helper:

   ```rust
   fn in_memory_row_identity(
       table: TableObjectId,
       ordinal: u64,
   ) -> wendelon::schema::Identity {
       let mut bytes = [0_u8; 32];
       bytes[0..16].copy_from_slice(table.as_bytes());
       bytes[24..32].copy_from_slice(&ordinal.to_be_bytes());
       wendelon::schema::Identity::new(bytes)
   }
   ```

   Bytes 16..24 remain zero (reserved namespace).
   **No `blake3` / `hash` / `sha`.**

3. Update `InMemoryTableStorage::write` (the existing
   `StorageWrite` impl) to:

   - assign identities `in_memory_row_identity(table_object_id,
     next_row_key_ordinal++)` for each new row;
   - append to `row_key_ids` parallel to `batch` rows;
   - leave `deleted_row_key_ids` untouched.

   The `WriteOutcome { rows }` return shape stays
   identical.

4. Refactor the in-memory scan helper so the identity
   append path receives row ids explicitly. A
   reasonable shape is a private helper that accepts
   the batch plus an optional identity slice /
   tombstone set; `InMemoryTableStorage::scan`
   supplies real `row_key_ids` and
   `deleted_row_key_ids`, while the legacy
   `InMemoryStorageScan { table_data }` supplies no
   identity context and therefore only supports
   `RowIdentityProjection::None`.

   The `InMemoryTableStorage::scan` path must:

   - reject identity scan combined with user
     projection per D11 (Phase 1 stub now becomes
     real);
   - filter tombstoned rows from `batch` using the
     parallel `row_key_ids` (skip row `i` if
     `deleted_row_key_ids.contains(&row_key_ids[i])`);
   - if `row_identity == RowIdentityProjection::Append`:
     append a final `FixedSizeBinary(32)` column
     named `STORAGE_ROW_KEY_ID_COLUMN` (`"__row_key_id__"`)
     containing the surviving identities;
   - if `row_identity == RowIdentityProjection::None`:
     existing behavior — return the filtered batch
     without identity column.

   The column-append uses Arrow's
   `FixedSizeBinaryArray::try_from_iter` or
   equivalent stable constructor; the resulting batch
   `schema()` must include the identity field as the
   final field.

5. Implement `StorageDeleteRows` for
   `InMemoryTableStorage`:

   ```rust
   #[async_trait::async_trait]
   impl StorageDeleteRows for InMemoryTableStorage {
       async fn delete_rows(
           &self,
           request: DeleteRowsRequest,
       ) -> Result<DeleteRowsOutcome, StorageError> {
           let requested = request.row_key_ids.len();
           let mut tables = self.tables.lock().await; // or whatever the existing lock pattern is
           let data = tables
               .get_mut(&request.table_id)
               .ok_or(StorageError::TableNotFound { table: request.table_id })?;
           if data.table_object_id != request.table_object_id
               || data.physical_descriptor_fingerprint != request.physical_descriptor_fingerprint
           {
               return Err(StorageError::DescriptorEvolved);
           }
           let live: std::collections::HashSet<&wendelon::schema::Identity> =
               data.row_key_ids.iter().collect();
           let mut deleted = 0;
           let mut already_deleted = 0;
           let mut not_found = 0;
           for id in &request.row_key_ids {
               if !live.contains(id) {
                   not_found += 1;
               } else if data.deleted_row_key_ids.contains(id) {
                   already_deleted += 1;
               } else {
                   data.deleted_row_key_ids.insert(*id);
                   deleted += 1;
               }
           }
           Ok(DeleteRowsOutcome {
               requested,
               deleted,
               already_deleted,
               not_found,
           })
       }
   }
   ```

   - `not_found` precedence over `already_deleted` is
     correct because tombstoned ids are still in
     `row_key_ids` (we never remove from the parallel
     vec).
   - Mutation discipline: hold the lock for the
     duration; the descriptor pin matches the
     existing `StorageWrite` pattern.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron storage::tests
```

Phase 7 fixtures will exercise this end-to-end. Phase
3 compiles and unit-level satisfies the
`StorageDeleteRows` impl.

## Phase 4 — Wendelon client wrapper + delete adapter

**Files:** `crates/wendelon/crates/wendelon-client/src/lib.rs`,
`crates/toron/src/storage.rs` (Wendelon adapter
section, alongside `WendelonStorageWrite`).

**Tasks:**

1. Add the missing high-level `wendelon-client`
   wrapper over Wendelon's existing internal
   `WendelonClient::table_delete` / `TABLE_DELETE_REQ`
   path. The wrapper should mirror `table_write`'s
   metadata ownership: allocate `ClientWriteId` from
   `next_sequence`, set `client_hlc`, `expected_epoch`,
   descriptor identity fields, `row_count`,
   durability, and `await_finalization`, then call
   the internal client method.

   Shape:

   ```rust
   #[derive(Clone, Copy, Debug, PartialEq, Eq)]
   pub struct TableDeleteRowsReceipt {
       pub table_object_id: TableObjectId,
       pub schema_generation: u64,
       pub requested: usize,
       pub deleted_count: u32,
       pub already_deleted_count: u32,
       pub not_found_count: u32,
       pub duplicate: bool,
       pub requested_durability: DurabilityClass,
       pub log_index: u64,
       pub term: u64,
       pub epoch: u64,
       pub commit_hlc: HybridTimestamp,
       pub finalization: Option<FinalizationMarker>,
   }

   pub async fn table_delete(
       &self,
       tenant_id: TenantId,
       descriptor: &TableSchemaDescriptor,
       row_key_ids: Vec<Identity>,
       options: WriteOptions,
   ) -> Result<TableDeleteRowsReceipt, ClientError> {
       // Build wendelon::client::TableDeleteRowsRequest here.
   }
   ```

   This is not a new wire RPC. It is a public wrapper
   for an existing Wendelon client operation, analogous
   to `table_write`.

2. Add `WendelonStorageDeleteRows`:

   ```rust
   pub struct WendelonStorageDeleteRows {
       client: std::sync::Arc<wendelon_client::Client>,
       descriptors: std::collections::HashMap<
           TableId,
           wendelon_client::TableSchemaDescriptor,
       >,
   }

   impl WendelonStorageDeleteRows {
       pub fn new(
           client: std::sync::Arc<wendelon_client::Client>,
           descriptors: std::collections::HashMap<
               TableId,
               wendelon_client::TableSchemaDescriptor,
           >,
       ) -> Self {
           Self { client, descriptors }
       }
   }

   #[async_trait::async_trait]
   impl StorageDeleteRows for WendelonStorageDeleteRows {
       async fn delete_rows(
           &self,
           request: DeleteRowsRequest,
       ) -> Result<DeleteRowsOutcome, StorageError> {
           let requested = request.row_key_ids.len();
           let descriptor = self
               .descriptors
               .get(&request.table_id)
               .ok_or(StorageError::TableNotFound {
                   table: request.table_id,
               })?;
           if request.table_object_id != descriptor.table_object_id {
               return Err(StorageError::DescriptorEvolved);
           }
           if request.physical_descriptor_fingerprint
               != descriptor.physical_descriptor_fingerprint
           {
               return Err(StorageError::DescriptorEvolved);
           }

           let tenant_id = wendelon::schema::TenantId::new(request.tenant_id.0);
           let receipt = self
               .client
               .table_delete(
                   tenant_id,
                   descriptor,
                   request.row_key_ids,
                   wendelon_client::WriteOptions {
                       await_finalization: true,
                       ..Default::default()
                   },
               )
               .await
               .map_err(StorageError::from)?;
           Ok(DeleteRowsOutcome {
               requested,
               deleted: receipt.deleted_count as usize,
               already_deleted: receipt.already_deleted_count as usize,
               not_found: receipt.not_found_count as usize,
           })
       }
   }
   ```

   Toron validates descriptor identity like
   `WendelonStorageWrite`, but it does **not** build
   `TableDeleteRowsRequestMeta`. That metadata stays
   inside `wendelon-client`.

3. **Do not call `Client::table_delete_rows`** (D13
   / guard #16) — that is not the current method
   name. The Wendelon helper `table_delete_rows_batch`
   is internal wire encoding.

4. Wire `Arc<dyn StorageDeleteRows>` into the local
   `QueryCoordinator` builder alongside `Arc<dyn
   StorageScan>` and `Arc<dyn StorageWrite>`. Field
   addition pattern matches the existing trait wiring.

**Verification:**

```sh
cargo check -p wendelon-client
cargo check -p toron
```

Phase 7 fixture #10 exercises the adapter against a
stub Wendelon client.

## Phase 5 — Coordinator delete-path helper

**Files:** `crates/toron/src/coordinator.rs` (or
`coordinator/mod.rs` if already migrated),
`crates/toron/src/coordinator/delete.rs` (NEW).

**Tasks:**

1. Create the submodule. If `coordinator.rs` is
   currently a single file, migrate to
   `coordinator/mod.rs` per ADR convention or add
   the submodule via `#[path = "..."]` attribute.
   Either path is acceptable; the architecture guard
   #13 source-checks the file's existence and the
   helper symbol.

2. In `coordinator/delete.rs`:

   ```rust
   use arrow::array::{Array, FixedSizeBinaryArray};
   use arrow::datatypes::DataType;
   use arrow::record_batch::RecordBatch;
   use futures::StreamExt;

   use crate::error::StorageError;
   use crate::storage::{
       DeleteRowsOutcome, DeleteRowsRequest, RowIdentityProjection, ScanRequest,
       StorageDeleteRows, StorageScan,
   };
   use wendelon::schema::{Identity, STORAGE_ROW_KEY_ID_COLUMN};

   pub(crate) async fn materialize_delete_row_key_ids(
       storage: &dyn StorageScan,
       scan_request: ScanRequest,
   ) -> Result<Vec<Identity>, StorageError> {
       debug_assert!(matches!(
           scan_request.row_identity,
           RowIdentityProjection::Append
       ));
       debug_assert!(scan_request.projected_columns.is_none());

       let mut stream = storage.scan(scan_request).await?.into_batches();
       let mut identities = Vec::new();
       while let Some(batch) = stream.next().await {
           let batch = batch?;
           extract_identity_column(&batch, &mut identities)?;
       }
       Ok(identities)
   }

   fn extract_identity_column(
       batch: &RecordBatch,
       identities: &mut Vec<Identity>,
   ) -> Result<(), StorageError> {
       let num_cols = batch.num_columns();
       if num_cols == 0 {
           return Err(StorageError::Internal {
               message: "delete candidate batch has no columns".to_string(),
           });
       }
       let last_idx = num_cols - 1;
       let field = batch.schema().field(last_idx).clone();
       if field.name() != STORAGE_ROW_KEY_ID_COLUMN {
           return Err(StorageError::Internal {
               message: format!(
                   "delete candidate trailing column is {}, expected {}",
                   field.name(),
                   STORAGE_ROW_KEY_ID_COLUMN
               ),
           });
       }
       if field.data_type() != &DataType::FixedSizeBinary(32) {
           return Err(StorageError::Internal {
               message: format!(
                   "delete candidate identity column has type {:?}, expected FixedSizeBinary(32)",
                   field.data_type()
               ),
           });
       }
       let column = batch.column(last_idx);
       let array = column
           .as_any()
           .downcast_ref::<FixedSizeBinaryArray>()
           .ok_or_else(|| StorageError::Internal {
               message: "delete candidate identity column downcast failed".to_string(),
           })?;
       for row in 0..array.len() {
           if array.is_null(row) {
               return Err(StorageError::Internal {
                   message: "delete candidate identity column has null entry".to_string(),
               });
           }
           let bytes: &[u8] = array.value(row);
           let mut copied = [0_u8; 32];
           copied.copy_from_slice(bytes);
           identities.push(Identity::new(copied));
       }
       Ok(())
   }

   pub(crate) async fn execute_delete_with_candidates(
       storage_delete: &dyn StorageDeleteRows,
       request_template: DeleteRowsRequest,
   ) -> Result<DeleteRowsOutcome, StorageError> {
       if request_template.row_key_ids.is_empty() {
           return Ok(DeleteRowsOutcome {
               requested: 0,
               deleted: 0,
               already_deleted: 0,
               not_found: 0,
           });
       }
       storage_delete.delete_rows(request_template).await
   }
   ```

   - The materializer never returns identity batches
     to user paths; it converts to `Vec<Identity>`.
   - Empty short-circuit (D8) lives in
     `execute_delete_with_candidates`; storage trait
     impls do not need defensive empty checks.
   - The helper is `pub(crate)` — not exposed beyond
     the coordinator submodule. ADR 036 will use it
     for SQL DELETE execution.

3. Declare the submodule in `coordinator.rs` (or
   `coordinator/mod.rs`):

   ```rust
   mod delete;
   ```

4. **Do not wire SQL DELETE routing.** No
   `LogicalPlan::Delete` arm in `execute_query`. No
   `ExecutionPlan::Delete` interception. ADR 036
   owns SQL DELETE.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron coordinator::delete::tests  # if helper has unit tests
```

Phase 7 fixtures exercise the materializer end-to-
end with `InMemoryTableStorage`, because the legacy
bare `InMemoryStorageScan` has no row-identity
context.

## Phase 6 — `Arc<dyn StorageDeleteRows>` wiring

**File:** `crates/toron/src/coordinator.rs`.

**Tasks:**

1. Add `storage_delete: Arc<dyn StorageDeleteRows>`
   to the `QueryCoordinator` struct alongside
   existing `storage_scan` / `storage_write`.

2. Update the constructor to accept the new
   parameter:

   ```rust
   pub fn new(
       config: Arc<Config>,
       catalog: Arc<dyn Catalog>,
       storage_scan: Arc<dyn StorageScan>,
       storage_write: Arc<dyn StorageWrite>,
       storage_delete: Arc<dyn StorageDeleteRows>,
   ) -> Self { ... }
   ```

3. Update every existing call site that constructs
   `QueryCoordinator` to pass a `StorageDeleteRows`
   impl:

   - in-process tests: pass
     `Arc::new(InMemoryTableStorage::new())` — same
     impl as `storage_scan` / `storage_write` (in-
     memory storage implements all three traits
     after Phase 3).
   - Wendelon-enabled call sites: pass
     `Arc::new(WendelonStorageDeleteRows::new(
     client.clone(), descriptors.clone()))`, matching
     the descriptor map already supplied to
     `WendelonStorageScan` / `WendelonStorageWrite`.

4. **Do not** route any SQL statement through
   `storage_delete` in this ADR. The field exists for
   ADR 036 to use.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron
```

All existing SELECT / INSERT / CREATE TABLE / CTAS /
DROP TABLE tests must remain green.

## Phase 7 — Functional tests

**File:** new
`crates/toron/tests/delete_substrate_test.rs`
(Claude iter-1 A3 file-naming convention carry-
forward from ADR 034).

10 required Toron functional fixtures:

### Fixture 1 — `in_memory_storage_scan_appends_stable_row_identity`

```rust
#[tokio::test]
async fn in_memory_storage_scan_appends_stable_row_identity() {
    // create InMemoryTableStorage; write three rows
    // scan with RowIdentityProjection::Append
    // assert final column is "__row_key_id__", FixedSizeBinary(32)
    // assert three rows, deterministic prefix-suffix layout:
    //   bytes[0..16] == table_object_id.as_bytes()
    //   bytes[16..24] == [0; 8]
    //   bytes[24..32] == [0, 0, 0, 0, 0, 0, 0, 1], [0, 0, 0, 0, 0, 0, 0, 2], [0, 0, 0, 0, 0, 0, 0, 3]
    // scan again; assert byte-identical identities (stable across repeated scans)
}
```

### Fixture 2 — `in_memory_storage_normal_scan_hides_row_identity`

```rust
#[tokio::test]
async fn in_memory_storage_normal_scan_hides_row_identity() {
    // create InMemoryTableStorage; write three rows
    // scan with RowIdentityProjection::None
    // assert no "__row_key_id__" column in result batches
    // assert result schema matches user schema exactly (field count, names, types)
}
```

### Fixture 3 — `in_memory_storage_delete_rows_filters_future_scans`

```rust
#[tokio::test]
async fn in_memory_storage_delete_rows_filters_future_scans() {
    // write five rows; capture identities via Append scan
    // delete two specific identities
    // scan with None: assert three rows (the live ones)
    // scan with Append: assert three rows + the three remaining identities (byte-identical to the original five minus the deleted two)
}
```

### Fixture 4 — `in_memory_storage_delete_rows_reports_all_counters`

```rust
#[tokio::test]
async fn in_memory_storage_delete_rows_reports_all_counters() {
    // write five rows; capture identities
    // delete two (first delete call): assert outcome { requested: 2, deleted: 2, already_deleted: 0, not_found: 0 }
    // delete five identities: two live (rows 3 & 4), two already-tombstoned (rows 1 & 2), one never-seen (synthetic identity)
    // assert outcome { requested: 5, deleted: 2, already_deleted: 2, not_found: 1 }
}
```

### Fixture 5 — `delete_rows_empty_candidate_set_short_circuits`

```rust
#[tokio::test]
async fn delete_rows_empty_candidate_set_short_circuits() {
    // construct a PanickingDeleteAdapter that panics on any delete_rows call
    // call execute_delete_with_candidates with empty row_key_ids
    // assert returns DeleteRowsOutcome { requested: 0, deleted: 0, already_deleted: 0, not_found: 0 }
    // assert panicking adapter never called
}
```

### Fixture 6 — `identity_extraction_rejects_wrong_name_type_width_or_null`

```rust
#[tokio::test]
async fn identity_extraction_rejects_wrong_name_type_width_or_null() {
    // construct synthetic candidate batches:
    //   case A: trailing column named "wrong_name" → error mentions both names
    //   case B: trailing column type FixedSizeBinary(16) → error mentions expected/actual type
    //   case C: trailing column type Utf8 → error mentions expected/actual type
    //   case D: trailing column has a null entry → error mentions null
    //   case E: batch has zero columns → error mentions no columns
    // each case returns StorageError::Internal { message } with a stable diagnostic shape
    // never panics
}
```

### Fixture 7 — `identity_scan_rejects_user_projection`

```rust
#[tokio::test]
async fn identity_scan_rejects_user_projection() {
    // construct ScanRequest { row_identity: Append, projected_columns: Some(vec![ColumnOrdinal(0)]), .. }
    // call InMemoryTableStorage::scan after writing a small batch
    // assert err matches StorageError::Internal { message }
    // assert message == "scan with row_identity=Append cannot be combined with projected_columns"
}
```

### Fixture 8 — `coordinator_materializes_delete_row_key_ids_from_filtered_scan`

```rust
#[tokio::test]
async fn coordinator_materializes_delete_row_key_ids_from_filtered_scan() {
    // create InMemoryTableStorage; write five rows
    // construct ScanRequest with row_identity: Append + runtime_filter selecting two specific rows
    // call materialize_delete_row_key_ids
    // assert returned Vec<Identity> has exactly two entries (matching the filter)
    // call StorageDeleteRows::delete_rows with those identities
    // assert outcome { requested: 2, deleted: 2 }
    // re-scan: assert three rows remain
}
```

### Fixture 9 — `storage_synthetic_column_names_still_rejected`

```rust
#[test]
fn storage_synthetic_column_names_still_rejected() {
    // SQL `SELECT __row_key_id__ FROM t` rejected at planner or binder
    // SQL `SELECT * FROM t` returns only user columns (no __row_key_id__)
    // proves ADR 030 D7 invariant preserved + identity column does not become SQL-visible
}
```

### Fixture 10 — `wendelon_delete_adapter_maps_table_delete_counters`

```rust
#[tokio::test]
async fn wendelon_delete_adapter_maps_table_delete_counters() {
    // build a stub wendelon_client::Client that:
    //   - asserts table_delete receives row_key_ids and await_finalization: true
    //   - returns a synthetic high-level delete receipt with deleted_count=2, already_deleted_count=1, not_found_count=3
    // call WendelonStorageDeleteRows::delete_rows with three row identities
    // assert outcome { requested: 3, deleted: 2, already_deleted: 1, not_found: 3 }
    // (the requested counter comes from Toron's pre-call count; the other three from the stub response)
}
```

Each fixture must execute without `unwrap()` on
unrelated paths; all expected errors must be matched
via `match` or `assert!(matches!(err, ...))` against
the documented diagnostic shape.

**Verification:**

```sh
cargo test -p toron --test delete_substrate_test
```

All 10 fixtures green.

## Phase 8 — Architecture guards

**File:** `crates/toron/tests/architecture_test.rs`.

Add 18 new ADR-035-specific guard functions matching
the table in the promoted ADR §"Architecture Guard
Set". Each guard's diagnostic string names ADR 035 +
the specific decision it enforces.

Key implementation notes:

1. **Guard #17 (`adr035_wendelon_projection_change_is_privileged`)**
   reads Wendelon source files. Use a
   `wendelon_manifest_dir()` helper that resolves
   the Wendelon crate path relative to Toron's
   `CARGO_MANIFEST_DIR`:

   ```rust
   fn wendelon_manifest_dir() -> std::path::PathBuf {
       let toron_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
       toron_dir
           .parent()
           .expect("crates/toron has parent crates/")
           .join("wendelon")
   }
   ```

   If the helper already exists in
   `architecture_test.rs` for prior cross-ring
   guards (e.g. ADR 030), reuse it; otherwise add it
   in Phase 8.

   Guard #17 body:

   ```rust
   #[test]
   fn adr035_wendelon_projection_change_is_privileged() {
       let columnar = std::fs::read_to_string(
           wendelon_manifest_dir().join("src/storage/columnar.rs")
       ).expect("read wendelon columnar.rs");

       // Positive: PBC PROJECT storage-column reject preserved verbatim
       assert!(
           columnar.contains("cannot appear in PBC PROJECT output"),
           "ADR 035 D4: Wendelon must preserve PBC PROJECT storage-column rejection"
       );

       // Positive: insert_required_column body still gates StorageColumn
       let body = brace_block_after(&columnar, "fn insert_required_column")
           .expect("insert_required_column body");
       assert!(
           body.contains("StorageColumn"),
           "ADR 035 D4: insert_required_column must still gate storage columns"
       );

       let wendelon_dir = wendelon_manifest_dir();
       let client_core = std::fs::read_to_string(
           wendelon_dir.join("src/client/core.rs")
       ).expect("read wendelon client core.rs");

       // Positive: TableQueryRequest carries row_identity field
       let table_query_request = struct_body_after(&client_core, "pub struct TableQueryRequest")
           .expect("TableQueryRequest body");
       assert!(
           table_query_request.contains("row_identity"),
           "ADR 035 D4 / Claude iter-1 C7: TableQueryRequest must carry the privileged identity projection field"
       );
       assert!(
           table_query_request.contains("RowIdentityProjection"),
           "ADR 035 D4: TableQueryRequest.row_identity must be typed RowIdentityProjection"
       );

       let table_schema = std::fs::read_to_string(
           wendelon_dir.join("src/schema/table.rs")
       ).expect("read wendelon schema table.rs");
       let table_query_meta = struct_body_after(&table_schema, "pub struct TableQueryRequestMeta")
           .expect("TableQueryRequestMeta body");
       assert!(
           table_query_meta.contains("row_identity"),
           "ADR 035 D4: TableQueryRequestMeta must carry row identity on the wire"
       );

       let client_wrapper = std::fs::read_to_string(
           wendelon_dir.join("crates/wendelon-client/src/lib.rs")
       ).expect("read wendelon-client lib.rs");
       let table_query_options = struct_body_after(&client_wrapper, "pub struct TableQueryOptions")
           .expect("TableQueryOptions body");
       assert!(
           table_query_options.contains("row_identity"),
           "ADR 035 D4: wendelon-client TableQueryOptions must expose row_identity"
       );
       assert!(
           client_wrapper.contains("row_identity: options.row_identity"),
           "ADR 035 D4: wendelon-client must copy TableQueryOptions.row_identity into TableQueryRequest"
       );
   }
   ```

   The `brace_block_after` / `struct_body_after`
   helpers may already exist in
   `architecture_test.rs` for prior structural
   guards. If not, add minimal versions in Phase 8.

2. **Guard #15 (`adr035_select_scan_uses_row_identity_none`)**
   audits every Toron-side `ScanRequest { ... }`
   construction outside the materializer. The
   simplest implementation:

   - read every Toron source file;
   - find every `ScanRequest {` occurrence;
   - for each, verify the brace-block contains
     `row_identity: RowIdentityProjection::None` or
     `row_identity: RowIdentityProjection::Append`
     (latter only in `coordinator/delete.rs`).

3. **Guard #16 (`adr035_wendelon_adapter_calls_table_delete`)**
   reads Toron's `storage.rs` and:

   - positive-asserts `.table_delete(` literal
     present;
   - negative-asserts `.table_delete_rows(` literal
     absent.

4. **Guard #9 (`adr035_no_blake3_or_hash_identity_generation_in_toron`)**
   reads Toron's `storage.rs`, `coordinator/delete.rs`,
   and `Cargo.toml`:

   - negative-asserts `blake3`, `Identity::blake3`,
     `Identity::blake3_tenant`, `Identity::blake3_keyed`,
     `sha2`, `sha3`, `hash_identity` literals
     absent from identity construction paths.

5. **Architecture guards for ADR 029 / 030 / 031 /
   032 / 033 / 034 invariants** remain unchanged.

**Verification:**

```sh
cargo test -p toron --test architecture_test
```

All 18 new guards green; all existing guards
(including ADR 030 D7 + ADR 034 17 guards) remain
green.

## Phase 9 — Wendelon-side patch + verification

**Files:** `crates/wendelon/src/client/core.rs`,
`crates/wendelon/src/schema/table.rs`,
`crates/wendelon/crates/wendelon-client/src/lib.rs`,
`crates/wendelon/src/network/data_server.rs`,
Wendelon table-query/storage helpers (additional
paths determined at audit), 3 new Wendelon fixtures.

**Tasks:**

1. Add `RowIdentityProjection` enum to
   `wendelon::client` (or wherever
   `TableQueryRequest` lives):

   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
   pub enum RowIdentityProjection {
       #[default]
       None,
       Append,
   }
   ```

   Parallel definition to Toron's; no cross-crate
   re-export.

2. Add `row_identity: RowIdentityProjection` to the
   public `wendelon-client::TableQueryOptions` and
   to internal `wendelon::client::TableQueryRequest`:

   ```rust
   pub struct TableQueryRequest {
       // ... existing fields ...
       pub row_identity: RowIdentityProjection,
   }

   pub struct TableQueryOptions {
       // ... existing fields ...
       pub row_identity: RowIdentityProjection,
   }
   ```

   Default `RowIdentityProjection::None` for backward-
   compatible existing callers. Update every existing
   `TableQueryRequest { ... }` and `TableQueryOptions {
   ... }` construction site (compiler flags) to set
   `RowIdentityProjection::None`. In
   `wendelon-client`, copy `options.row_identity` into
   the internal `TableQueryRequest`.

3. Make the setting wire-visible. `TableQueryRequest`
   itself is not serialized; `table_query_once`
   writes `TableQueryRequestMeta` plus trailing
   PBC/runtime-filter bytes. Add a metadata field,
   preferably by replacing one byte of the existing
   padding while preserving the current metadata
   size:

   ```rust
   #[repr(C)]
   pub struct TableQueryRequestMeta {
       // ... existing fields ...
       pub consistency: crate::schema::Consistency,
       pub row_identity_projection: u8,
       pub _pad: [u8; 6],
       // ... existing fields ...
   }
   ```

   Encode `RowIdentityProjection::None` / `Append`
   with stable byte values and reject unknown values
   as `QUERY_INVALID_PROGRAM` or an equivalent
   table-query invalid-request status. Update table-
   query test metadata constructors to initialize
   `row_identity_projection` to `None`.

4. Audit the Wendelon table-query handler path that
   materializes `TableQueryResult.batches`. The exact
   file may be:

   - `crates/wendelon/src/server/...` (server-side
     handler);
   - `crates/wendelon/src/storage/columnar.rs`
     (storage-layer composer);
   - or a combination.

   Add the privileged identity-projection logic:

   - if decoded row identity projection ==
     `RowIdentityProjection::Append`:
     append `StorageColumn::RowKeyId` to the output
     batches' final column (`__row_key_id__`,
     `FixedSizeBinary(32)`);
   - identity values come from the actual row
     identities already available during table-query
     scanning (for example the `key_identity` yielded
     by `scan_as_of_with_identity_and_locality`), not
     from user-row-derived recomputation in Toron;
   - append after predicate/runtime-filter row
     selection so the identity column's row order and
     row count match the returned user-visible rows;
   - if the projected fast path cannot carry the
     parallel identity vector safely in v1, it may
     fall back to the whole-record path for
     `Append`; correctness and PBC PROJECT reject
     preservation are more important than preserving
     the fast path for this internal mode.

5. **Do not modify `insert_required_column`** at
   `crates/wendelon/src/storage/columnar.rs`. The
   PBC PROJECT storage-column reject must remain
   verbatim:

   ```text
   "storage column {storage_column:?} cannot appear in PBC PROJECT output"
   ```

   The privileged identity projection is carried
   outside ordinary PBC output requirements via the
   new query option / metadata field.

6. **Do not add a new `RequiredColumnAxis` variant**.
   The axis enum stays frozen.

7. **Do not add a separate
   `table_query_with_identities` RPC method**. Single
   new query option / metadata field is the minimum-
   surface change.

8. Add 3 Wendelon fixtures (in Wendelon's existing
   client / storage test suite — exact paths
   determined at implementation time):

   ### Wendelon fixture 1 — privileged identity projection

   ```rust
   #[tokio::test]
   async fn wendelon_table_query_with_privileged_identity_projection_appends_row_key_id() {
       // call wendelon-client Client::table_query with
       // TableQueryOptions { row_identity: RowIdentityProjection::Append, .. }
       // assert response batches end with "__row_key_id__" column, FixedSizeBinary(32)
       // assert identities match the rows' actual storage identities
   }
   ```

   ### Wendelon fixture 2 — ordinary PBC PROJECT still rejects

   ```rust
   #[tokio::test]
   async fn wendelon_ordinary_pbc_project_storage_columns_still_rejects() {
       // call Client::table_query (or query_pbc equivalent) with PBC PROJECT requesting a storage column
       // assert error fires with the existing "cannot appear in PBC PROJECT output" reject string
       // proves D4 negative discipline: privileged identity projection does NOT relax PBC PROJECT
   }
   ```

   ### Wendelon fixture 3 — identity projection respects filters and tombstones

   ```rust
   #[tokio::test]
   async fn wendelon_identity_projection_respects_predicate_filtering_and_tombstones() {
       // write rows
       // delete some (via the public Client::table_delete wrapper)
       // apply a predicate via Client::table_query with row_identity: Append + predicate
       // assert returned batches contain only the live + matching rows
       // assert identities are stable across calls
   }
   ```

**Verification:**

```sh
cargo check -p wendelon-client
cargo check -p wendelon
cargo test -p wendelon
cargo check -p toron
cargo test -p toron --test architecture_test
```

All Wendelon-side tests green (including the 3 new
fixtures + the existing PBC PROJECT reject); Toron
architecture guard #17 green; all Toron tests green.

Full end-to-end:

```sh
cargo fmt --check
cargo test -p toron
cargo test -p wendelon-client
cargo test -p wendelon
```

## Definition of Done

- [ ] **Phase 1:** `RowIdentityProjection` enum +
      `ScanRequest::row_identity` field; all
      existing call sites default to
      `RowIdentityProjection::None`.
- [ ] **Phase 2:** `DeleteRowsRequest` /
      `DeleteRowsOutcome` types + `StorageDeleteRows`
      trait; `StorageWrite` shape byte-identical.
- [ ] **Phase 3:** `InMemoryTableData` extended
      with `row_key_ids` / `deleted_row_key_ids` /
      `next_row_key_ordinal`; `in_memory_row_identity`
      helper; `scan_in_memory_batch` honors
      `RowIdentityProjection`; `StorageDeleteRows`
      impl with four-counter semantics; D11 reject
      fires for `Append + projected_columns`.
- [ ] **Phase 4:** `wendelon-client::Client::table_delete`
      high-level wrapper added; `WendelonStorageDeleteRows`
      adapter calls it with `await_finalization: true`;
      `requested` comes from request length and the
      three Wendelon response counters are mapped.
- [ ] **Phase 5:** `coordinator/delete.rs`
      submodule with `materialize_delete_row_key_ids`
      + `execute_delete_with_candidates`; empty
      short-circuit (D8); column-shape validation
      with stable diagnostics.
- [ ] **Phase 6:** `Arc<dyn StorageDeleteRows>`
      wired into `QueryCoordinator`; no SQL DELETE
      routing.
- [ ] **Phase 7:** All 10 Toron fixtures pass
      (`crates/toron/tests/delete_substrate_test.rs`).
- [ ] **Phase 8:** All 18 ADR-035-specific
      architecture guards pass.
- [ ] **Phase 9:** Wendelon-side
      `RowIdentityProjection` enum +
      `TableQueryOptions.row_identity` +
      `TableQueryRequest.row_identity` +
      `TableQueryRequestMeta` wire byte added;
      privileged identity-projection handler in place;
      ordinary PBC PROJECT reject preserved verbatim;
      all 3 Wendelon fixtures pass; no new
      `RequiredColumnAxis` variant; no new wire RPC
      or identity-specific query method.
- [ ] All existing CREATE TABLE / VALUES INSERT /
      INSERT SELECT / CTAS / DROP TABLE tests
      green.
- [ ] All existing SELECT-only plan-digest
      fixtures green (additive substrate cycle
      carry-forward).
- [ ] ADR 029 catalog metadata-only preserved.
- [ ] ADR 030 `ExecutionPlan::Insert` expression-
      free preserved.
- [ ] ADR 030 D7 Toron-side synthetic-column
      construction prohibition preserved (extended:
      Toron uses opaque-not-cryptographic identity
      layout).
- [ ] ADR 031 `BoundExprProgram` `PartialEq`-only
      preserved.
- [ ] ADR 032 InsertSelect invariants preserved
      (`materialize_insert_select_batch` `Ok(None)`
      zero-row contract).
- [ ] ADR 033 CTAS invariants preserved
      (`materialize_empty_record_batch` sibling
      helper).
- [ ] ADR 034 catalog `drop_table` + 17
      architecture guards preserved; `lower_ddl_table_name`
      helper preserved.
- [ ] `StorageWrite` trait shape unchanged.
- [ ] `StorageScan` trait signature unchanged
      (only `ScanRequest` body extended with
      `row_identity` field).
- [ ] No `blake3` dependency added to Toron
      `Cargo.toml`.
- [ ] No external allocator crate added.
- [ ] No new `StorageError` variant added.
- [ ] No new `RequiredColumnAxis` variant added to
      Wendelon.
- [ ] No new Wendelon wire RPC or identity-specific
      table-query method. The high-level
      `wendelon-client::Client::table_delete` wrapper
      over existing `TABLE_DELETE_REQ` is in scope.
- [ ] No SQL DELETE syntax / IR variants added.
- [ ] No distributed DELETE coordinator routing
      added.
- [ ] No predicate-to-storage delete API on
      Wendelon side.
- [ ] No storage reclaim trait / helper introduced.
- [ ] Roadmap moves ADR 035 from candidate to
      promoted; advances active section to ADR 036
      (SQL `DELETE WHERE` per D15 sequencing).
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
- [ ] `cargo test -p wendelon-client` green.
- [ ] `cargo test -p wendelon` green.

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Boolean projection flag leaks into Toron source | Guard #2 source-checks enum; guard #3 negative-patterns `include_row_key_id` literal. |
| R2 | `RecordBatch` payload added to `DeleteRowsRequest` | Guard #5 negative-pattern source check; Phase 2 explicit struct definition. |
| R3 | `WriteOutcome` reused for delete | Guard #6 source-checks all four counters; Phase 2 explicit struct definition. |
| R4 | `StorageWrite::delete_rows` overload added | Guards #7 + #8 enforce trait separation; Phase 2 + Phase 3 separate impls. |
| R5 | `blake3` / hash dependency creeps into Toron identity construction | Guard #9 negative-pattern source check; Phase 0 preflight + Phase 3 explicit no-hash layout. |
| R6 | Mock identity layout drifts to non-deterministic | Guard #10 source-checks layout helper; fixture #1 asserts hex-grep-able pattern. |
| R7 | Physical row removal added to in-memory storage | Guard #11 source-checks `deleted_row_key_ids` HashSet field; fixture #3 asserts identity stability across delete. |
| R8 | New `StorageError` variant added for D11 reject | D11 wording pins reuse of `Internal { message }`; Phase 1 + Phase 3 explicit error literal; fixture #7 asserts exact diagnostic. |
| R9 | Identity column leaks into SQL result paths | Guards #14 + #18 + fixture #9 catch leakage. |
| R10 | Wendelon PBC PROJECT storage-column reject relaxed globally | Guard #17 positive-asserts reject string preserved; Phase 9 task #5 explicit "do not modify" directive; Wendelon fixture #2 asserts behavior preserved. |
| R11 | Row-identity setting lands only on `TableQueryRequest` and never reaches the server | Guard #17 positive-asserts internal request, public options, and wire metadata surfaces; Phase 9 task #3 pins `TableQueryRequestMeta`. |
| R12 | Materializer extracts identity from wrong column position / type / width | Guard #14 source-checks shape validation; fixture #6 asserts each wrong shape returns stable error. |
| R13 | Coordinator skips empty-set short-circuit and calls Wendelon (which rejects empty) | Fixture #5 panicking-mock adapter proves short-circuit. |
| R14 | `Client::table_delete_rows` referenced (wrong method name) | Guard #16 positive-asserts `table_delete` literal; negative-asserts `table_delete_rows`. |
| R15 | ADR 036 reopens substrate questions (e.g. WriteOutcome reuse) | D15 sequencing pin explicit; D6 counter shape locked at substrate level. |
| R14a | Toron constructs `TableDeleteRowsRequestMeta` directly and bypasses `wendelon-client` metadata discipline | Phase 4 pins the high-level wrapper; adapter code uses `client.table_delete(tenant_id, descriptor, row_key_ids, WriteOptions { ... })`. |
| R16 | Plan Arena pressure increases | ADR 035 adds no IR variants; new substrate is types + trait + module + Wendelon query metadata/client fields; zero plan-spine pressure. |
| R17 | Distributed DELETE smuggled in via ADR 035 path | Out-of-scope explicit; no distributed routing wired for `StorageDeleteRows`. |
| R18 | Stale-bound-plan invalidation becomes implicit after delete | Tombstone scans return stable schemas; identity layout stable; future stale-bound-plan ADR engages. |
| R19 | ScanRequest construction site not updated to set `row_identity` | Phase 1 task #3 explicitly enumerates known sites; compiler flags any missed site (struct is not `#[non_exhaustive]`). |
| R20 | Wendelon-side TableDeleteRowsRequestMeta field shape drifts during implementation | Phase 4 keeps that metadata construction inside `wendelon-client`, alongside `table_write`; Toron adapter is insulated from field drift. |
| R21 | Toron-side architecture test cannot resolve Wendelon manifest path | Phase 8 task #1 explicit `wendelon_manifest_dir()` helper; reused from prior ADR 030 cross-ring guard if present. |
| R22 | Wendelon-side privileged identity projection handler path not yet identified | Phase 9 task #4 explicit "audit determines exact handler path"; implementation PR may add additional guards if surprises surface. |
