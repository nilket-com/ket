# Implementation Plan: ADR 039 — Wendelon DROP Migration Substrate

**Companion ADR:** `plans/adr/039_wendelon_drop_migration_substrate.md`

## Goal

Lift the deferred L3 from ADR 038 by implementing
Wendelon-internal 3-stage fenced DROP protocol with
raft-log-index receipts, exposed to Toron via a single
high-level RPC call:

```rust
client.drop_table_schema(TableSchemaDropRequest::current(...)).await
```

Wendelon owns the entire internal choreography (prepare/fence,
physical reclaim + reclaim-completion commit, finalize).
Toron's side is a thin client wrapper.

The promoted ADR names two sub-lifts:

- **L1** — Wendelon-internal complete protocol: typed payload
  types (`TableSchemaDropRequest` public; `TableDropPrepareRequest`
  / `TableReclaimCompletionRequest` Wendelon-private), three
  internal consensus apply paths, lifecycle drop-state +
  `TableSchemaCatalogV1::unbind_latest_name` primitive,
  structured `WendelonAdminError` variants, raft-entry-index
  propagation into apply handlers, **server-side reclaim
  orchestrator**, high-level `Client::drop_table_schema` surface,
  L1b data-path admission consulting the fence on
  write/delete/scan/index-maintenance/CDC paths.
- **L2** — Toron-side thin wrapper:
  `WendelonStorageAdmin::drop_table_storage` (~10 lines —
  one `client.drop_table_schema(...)` call + outcome mapping +
  structured error mapping + finalized-drop marker),
  `WendelonCatalog::drop_table` lift
  (remove `AdminUnavailable`; consume finalized-drop marker
  before registry removal),
  `TableBinding.schema_generation: u64` widening,
  `DropTableStorageRequest.schema_generation: u64` widening,
  `map_wendelon_admin_error` rewrite against structured
  variants (replaces ADR 038 substring matching).

**Plan A (default)**: L1 + L2 in one cycle. ADR 038 D13's
`WendelonCatalog::drop_table` `AdminUnavailable` safety block
is fully discharged.

**Plan B (correctness-preserving fallback)**: substrate-only.
Wendelon ships typed payloads + `WendelonAdminError` variants
+ raft-entry-index plumbing. **No server-side orchestrator.
No L1b data-path admission. No Toron-side wrapper.**
`WendelonCatalog::drop_table` safety block **preserved**.
Toron's `TableBinding.schema_generation` / `DropTableStorageRequest.schema_generation`
field additions MAY land as additive preparation (they don't
enable Wendelon DROP on their own).

Plan A / B selected at **Phase 0 implementation audit**.
**Toron does not ship code that orchestrates a Wendelon
admin protocol** — if Wendelon can't own the full internal
protocol, Toron doesn't try to fill the gap (D13).

Substrate preserves:

- ADR 038 `StorageAdmin` trait unchanged (no new methods;
  no `DropTableStorageOutcome.proof` field).
- ADR 038 `Catalog` trait **unchanged** (no
  `drop_table_with_proof` method — the iter-2 design item
  deleted at iter-3 redirection).
- ADR 038 D2 `ExecutionPlan::DropTable` identity-free.
- ADR 038 D3 storage-first / catalog-second routing.
- ADR 038 D9 registry mutation IS the catalog refresh.
- ADR 038 D10 stale-bound-plan invariant.
- ADR 038 D11 single-call-site invariant for
  `catalog.drop_table` — preserved verbatim (no trait
  widening).

No new SQL surface; no new IR variant; no new
`ExecutionPlan` / `LogicalPlan` variant; no new aggregate
kernel; no PBC opcode change; no `blake3` dependency; no
external allocator crate.

**Nine-phase patch order**. Phase 0 is preflight audit +
reference fixture pass-count capture + L1 sub-lift sizing
audit + Plan A/B selection. Phases 1-4 implement Wendelon-
internal substrate (L1). Phase 5 implements L1b data-path
admission (skipped under Plan B). Phase 6 implements Toron-
side thin wrapper (L2; skipped under Plan B). Phase 7 adds
architecture guards. Phase 8 captures per-lift landed-state.
Phase 9 is full verification.

## Implementation constraints

- **D1**: Wendelon-internal 3-stage protocol (prepare/fence →
  reclaim + completion → finalize). Toron never spells the
  stages.
- **D2**: All Wendelon apply paths (prepare, completion,
  finalize) are pure committed-metadata. Architecture guard
  6 source-asserts no I/O symbols in apply bodies.
- **D3**: `fence_log_index` and `reclaim_completion_log_index`
  are raft log indexes (`entry.log_id.index`).
  Wendelon-internal; not surfaced to Toron. No cryptographic
  / HMAC / PKI material.
- **D4**: `TableSchemaUpdateRequest::envelope` Drop reject
  remains; ADR 039 adds dedicated typed payloads on a
  separate control-plane surface.
- **D5**: Root types unsuffixed. `TableSchemaDropRequest`
  Toron-facing; `TableDropPrepareRequest` /
  `TableReclaimCompletionRequest` Wendelon-private. All use
  sum-type variant + `::current(fields)` constructor.
- **D6**: `DropOperationId { tenant_id, table_object_id,
  schema_generation }`. Per-stage identity is
  `(DropOperationId, DropStage)` (Wendelon-internal).
  `query_id` not in derivation.
- **D7**: `TableBinding.schema_generation: u64` explicit
  field. `DropTableStorageRequest.schema_generation: u64`
  explicit field. No `schema_generation_from_fingerprint`
  helper.
- **D8**: Name unbinds only at Wendelon-internal finalize
  stage.
- **D9**: Drop-state separate from `TableRetentionPolicy`.
  Decision at Phase 1 audit.
- **D10**: Wendelon write/delete/scan/index/CDC paths
  reject fenced tables (L1b sub-lift; skip under Plan B).
- **D11**: 6 structured `WendelonAdminError` variants
  replace ADR 038 substring matching.
- **D12**: Conservative reject when dependent secondary
  indexes exist.
- **D13**: Sub-lift Plan A / B settled at Phase 0. Plan B
  is substrate-only + feature-gated; Toron-side wrapper
  does not ship under Plan B. Per-lift landed-state
  captured at Phase 8.
- **D14**: **Wendelon owns the entire DROP choreography.**
  Toron makes one client call. Architecture guards 15-18.
- **Byte-identical refactor invariant**: ADR 029-038
  fixtures byte-identical pre/post-ADR-039. Phase 0
  captures reference counts; Phase 9 verifies.

## Touchpoint sweep

### Wendelon side (L1)

| File | Phase |
|---|---|
| `crates/wendelon/crates/wendelon-client/src/lib.rs` | 1 (Toron-facing `TableSchemaDropRequest` + `::current` constructor + `Client::drop_table_schema` method + Wendelon-private internal stage types if exposed for tests) |
| `crates/wendelon/crates/wendelon-client/src/error.rs` (or wherever errors live) | 1 (6 `WendelonAdminError` variants) |
| `crates/wendelon/src/schema/table.rs` (or sibling) | 1 (`TableSchemaCatalogV1::unbind_latest_name` primitive) |
| `crates/wendelon/src/integration/compute.rs` | 1 (lifecycle drop-state extension OR `TableDropFenceCatalog` sibling) |
| `crates/wendelon/src/consensus/raft_storage.rs` (or wherever apply lives) | 2-4 (3 new apply paths: prepare, completion, finalize; entry log index propagation) |
| `crates/wendelon/src/server/...` (server-side handler for `drop_table_schema` RPC + reclaim orchestrator) | 1-4 (server-side handler + orchestrator) |
| `crates/wendelon/src/storage/*.rs` (write / delete / scan paths) | 5 (L1b fence admission — skip under Plan B) |
| `crates/wendelon/tests/control_ring_rpc.rs` + `architecture_test.rs` | 1-5 (Wendelon-side fixtures: 12 required) |

### Toron side (L2 — skipped under Plan B)

| File | Phase |
|---|---|
| `crates/toron/src/storage.rs` | 6 (`WendelonStorageAdmin` struct + thin `drop_table_storage` impl + finalized-drop marker + `DropTableStorageRequest.schema_generation` field) |
| `crates/toron/src/catalog.rs` | 6 (`TableBinding.schema_generation` field widening + `WendelonDescriptorRegistry` finalized-drop marker + `WendelonCatalog::drop_table` lift — consume marker before registry removal) |
| `crates/toron/src/coordinator.rs` | UNCHANGED from ADR 038 (no trait widening, no proof threading) |
| `crates/toron/src/error.rs` | UNCHANGED (`CatalogError::AdminFailed` from ADR 038 suffices) |
| `crates/toron/tests/drop_table_test.rs` + `wendelon_storage_execution_test.rs` | 6 (Toron-side fixtures: 10 required) |
| `crates/toron/tests/architecture_test.rs` | 7 (~25 ADR-039 guards + 1 paired ADR 038 adjustment) |

## Helper reuse summary

| Helper | Source | ADR 039 use |
|---|---|---|
| `execute_catalog_plan` | ADR 029 → 034 → 038 | **UNCHANGED**. Routing helper still calls `admin.drop_table_storage(...)` then `catalog.drop_table(name, false)`. No proof threading. |
| `InMemoryTableStorage::drop_table_storage` | ADR 038 | UNCHANGED. Returns existing `DropTableStorageOutcome { reclaimed_tables }` shape. |
| `WendelonDescriptorRegistry` | ADR 038 D5 | Phase 6: `mark_finalized_drop` + `consume_finalized_drop` handoff helpers used by `WendelonStorageAdmin` / `WendelonCatalog::drop_table` (Plan A), plus `remove_table`. |
| `Catalog::drop_table` (trait method) | ADR 034 → 038 | **UNCHANGED** trait shape. `WendelonCatalog::drop_table` body rewritten (Plan A) to remove `AdminUnavailable`, consume a finalized-drop marker, then remove registry state. |
| `Catalog::drop_table_with_proof` (proposed in iter-2) | — | **NOT INTRODUCED**. Iter-3 redirection deleted this. |
| `DropTableStorageOutcome.proof` (proposed in iter-2) | — | **NOT INTRODUCED**. Iter-3 redirection deleted this. |
| `DropTableStorageProof` (proposed in iter-2) | — | **NOT INTRODUCED**. Iter-3 redirection deleted this. |
| `WendelonStorageAdmin::drop_table_storage` (Plan A only) | Phase 6 | Thin client wrapper: one `client.drop_table_schema(...)` call + outcome mapping. |
| `Client::drop_table_schema` (NEW, public) | Phase 1 | Toron-facing single high-level RPC. |
| `TableSchemaDropRequest` + `TableSchemaDropFields` + `::current` | Phase 1 | Toron-facing root payload + constructor. |
| `TableDropPrepareRequest` (Wendelon-private) | Phase 1 | Internal stage payload. Not in Toron. |
| `TableReclaimCompletionRequest` (Wendelon-private) | Phase 1 | Internal stage payload. Not in Toron. |
| `DropOperationId` | Phase 1 (wendelon-client) | `(tenant_id, table_object_id, schema_generation)`. Toron passes in `TableSchemaDropRequest`. Wendelon uses internally for idempotency. |
| `DropStage` (Wendelon-internal) | Phase 1 | `{ Prepare, ReclaimCompletion, Finalize }`. |
| Server-side reclaim orchestrator (NEW) | Phases 2-4 | Observes fence in raft state, drives physical reclaim, commits completion entry. |
| `TableSchemaCatalogV1::unbind_latest_name` (NEW) | Phase 1 | Called at internal finalize apply. |
| `TableLifecycleCatalog` extension OR `TableDropFenceCatalog` | Phase 1 | Drop-state availability lifecycle (D9). |
| `WendelonAdminError` (NEW enum, 6 variants) | Phase 1 | Replaces ADR 038 substring matching in Toron. |
| `map_wendelon_admin_error` | ADR 038 catalog.rs | Phase 6: rewritten against structured variants. |
| Existing Wendelon `reclaim_tombstoned_ranges` / `prune_versions_for_retention` / `prepare_tiered_table_retention_rewrite` | Wendelon storage | Phases 2-3: wired into server-side reclaim orchestrator. |
| `TableBinding.schema_generation` (NEW field) | Phase 6 | Additive widening. Set from `descriptor.schema_generation` in `from_descriptor`. |
| `DropTableStorageRequest.schema_generation` (NEW field) | Phase 6 | Additive widening. Set from `binding.schema_generation` in coordinator routing helper. |

## Phase 0 — Preflight audit + sub-lift sizing + Plan A/B selection

### Audit commands

```sh
# ADR 038 substrate state
rg -n "AdminUnavailable" crates/toron/src/catalog.rs
rg -n "WendelonDescriptorRegistry" crates/toron/src/storage.rs crates/toron/src/catalog.rs

# Wendelon admin RPC surface gaps
rg -n "drop_table_schema|table_schema_drop" crates/wendelon/crates/wendelon-client/src/
rg -n "table schema drops require|apply_table_schema_update" crates/wendelon/src
rg -n "TableSchemaCatalogV1|unbind_latest_name" crates/wendelon/src
rg -n "TableLifecycleCatalog" crates/wendelon/src

# Wendelon raft-entry-index propagation gap
rg -n "entry\.log_id\.index|apply_request" crates/wendelon/src/consensus

# Wendelon server-side self-submission pattern (R9 risk)
rg -n "RaftClient|submit_request|control_ring" crates/wendelon/src/server

# Toron-side substrate
rg -n "schema_generation" crates/toron/src/catalog.rs crates/toron/src/storage.rs
rg -n "query_id" crates/toron/src/storage.rs crates/toron/src/coordinator.rs

# L1b sub-lift sizing
rg -n "table_query_with_runtime_filter|table_write|table_delete" crates/wendelon/src
rg -n "secondary_index|index_maintenance" crates/wendelon/src
```

### Reference fixture pass-count capture

```sh
for suite in drop_table_test ctas_test create_table_test distributed_write_context_test delete_test insert_test architecture_test; do
    cargo test -p toron --test $suite 2>&1 | tee /tmp/adr039_phase0_${suite}.txt
done
cargo test -p toron 2>&1 | tail -3 | tee /tmp/adr039_phase0_all.txt
cargo test -p wendelon 2>&1 | tail -3 | tee /tmp/adr039_phase0_wendelon.txt
cargo test -p wendelon-client 2>&1 | tail -3 | tee /tmp/adr039_phase0_wendelon_client.txt
```

### Plan A vs Plan B selection

Estimate **L1 implementation effort** including the
server-side reclaim orchestrator. Key audit checks:

1. **Wendelon server-side self-submission pattern**: does
   Wendelon's server already support submitting consensus
   requests from server context (not from client RPC)? If
   YES → server-side orchestrator can commit
   reclaim-completion + finalize internally. If NO →
   server-side orchestrator scope grows significantly
   (R9 mitigation); strongly consider Plan B.
2. **L1b data-path admission**: estimate touched files +
   admission-call sites for write/delete/scan/index/CDC
   paths. If ≤ ~5 files and admission can be expressed as a
   single fence-state lookup → Plan A. If > ~5 files OR
   admission requires raft-state-aware refactor → Plan B.
3. **`TableLifecycleCatalog` extension feasibility**: can
   the existing catalog carry availability state cleanly?
   If yes → extend. If no → sibling
   `TableDropFenceCatalog`. Either way, doesn't block
   Plan A.

If Plan B selected, ADR 040 = Wendelon Server-Side DROP
Orchestrator + L1b + Final Enablement. Document Plan B
blockers in Phase 0 audit notes; reference them in ADR
040's iter-0 audit.

**Default to Plan A**. Plan B is the correctness-
preserving fallback when the cycle can't fit.

## Phase 1 — Wendelon-internal substrate (L1 base)

### Public Toron-facing root payload in `wendelon-client`

```rust
pub enum TableSchemaDropRequest {
    V1 {
        tenant_id: TenantId,
        table_object_id: TableObjectId,
        expected_generation: SchemaGeneration,
        operation_id: DropOperationId,
    },
}

pub struct TableSchemaDropFields {
    pub tenant_id: TenantId,
    pub table_object_id: TableObjectId,
    pub expected_generation: SchemaGeneration,
    pub operation_id: DropOperationId,
}

impl TableSchemaDropRequest {
    pub fn current(fields: TableSchemaDropFields) -> Self {
        Self::V1 {
            tenant_id: fields.tenant_id,
            table_object_id: fields.table_object_id,
            expected_generation: fields.expected_generation,
            operation_id: fields.operation_id,
        }
    }
}
```

### Public client method

```rust
impl Client {
    pub async fn drop_table_schema(
        &self,
        request: TableSchemaDropRequest,
    ) -> Result<(), ClientError> {
        // RPC to Wendelon server's drop_table_schema handler.
        // Returns after server-side protocol completes finalize.
    }
}
```

### Wendelon-private internal stage types

`TableDropPrepareRequest` + `TableReclaimCompletionRequest`
follow the same unsuffixed + `::current` pattern, but are
defined inside the wendelon crate (not wendelon-client) or
behind a `pub(crate)` boundary so Toron cannot import them.

```rust
// crates/wendelon/src/server/drop_protocol.rs (or sibling)
pub(crate) enum TableDropPrepareRequest {
    V1 { /* fields */ },
}

pub(crate) enum TableReclaimCompletionRequest {
    V1 { /* fields */ },
}
```

### `DropOperationId` + `DropStage` in `wendelon-client`

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DropOperationId {
    pub tenant_id: TenantId,
    pub table_object_id: TableObjectId,
    pub schema_generation: SchemaGeneration,
}

pub enum DropStage {
    Prepare,
    ReclaimCompletion,
    Finalize,
}
```

`DropStage` is only used Wendelon-internally for identifying
stage receipts; Toron references `DropOperationId` only.

### `TableSchemaCatalogV1::unbind_latest_name`

```rust
impl TableSchemaCatalogV1 {
    pub(crate) fn unbind_latest_name(&mut self, name: &str) -> Result<(), SchemaError> {
        // Remove latest-by-name binding only; preserve by_id history (D8).
    }
}
```

### Lifecycle drop-state (D9)

Decision at Phase 1: extension vs sibling catalog. Default
to extension if `TableLifecyclePolicy` can carry an
availability-state field cleanly.

### Structured `WendelonAdminError` (D11)

6 variants in `wendelon-client::error`:
`TableMissing`, `GenerationMismatch`, `TableAlreadyDropping`,
`TableAlreadyDropped`, `DependentIndexesPresent`,
`Transport`. Internal failure modes
(`ReclaimEvidenceInvalid`, `ReclaimNotComplete`,
`FenceNotFound`) stay inside Wendelon as
`(crate)` enum variants and are not exposed externally —
Wendelon's internal orchestrator handles them
deterministically.

## Phase 2 — Wendelon internal Stage 1 apply (Prepare/Fence)

**File:** `crates/wendelon/src/consensus/...` (wherever
`apply_table_schema_update` lives).

Plumb `entry.log_id.index` into apply handlers (D3):
modify `apply_request(state, req)` signature to accept
the current entry's log index.

Add `apply_table_drop_prepare`:

```rust
fn apply_table_drop_prepare(
    state: &mut WendelonState,
    request: TableDropPrepareRequest,
    log_index: u64,
) -> Result<TableDropPrepareReceipt, WendelonAdminErrorInternal> {
    let TableDropPrepareRequest::V1 {
        tenant_id, table_object_id, expected_generation, operation_id,
    } = request;

    let descriptor = state.table_catalog.latest_by_id(table_object_id)
        .ok_or(WendelonAdminError::TableMissing { table_object_id })?;
    if descriptor.schema_generation != expected_generation {
        return Err(WendelonAdminError::GenerationMismatch {
            expected: expected_generation,
            committed: descriptor.schema_generation,
        }.into());
    }

    // Idempotency
    if let Some(existing) = state.lifecycle.get_fence(table_object_id) {
        if existing.operation_id == operation_id {
            return Ok(TableDropPrepareReceipt { fence_log_index: existing.fence_log_index });
        }
        return Err(WendelonAdminError::TableAlreadyDropping {
            table_object_id,
            existing_operation_id: existing.operation_id,
        }.into());
    }

    // Dependent index check (D12)
    let indexes = state.indexes.active_indexes_for(table_object_id);
    if !indexes.is_empty() {
        return Err(WendelonAdminError::DependentIndexesPresent { indexes }.into());
    }

    state.lifecycle.mark_fenced(table_object_id, log_index, operation_id);
    Ok(TableDropPrepareReceipt { fence_log_index: log_index })
}
```

`state.lifecycle.mark_fenced(...)` is pure metadata mutation;
no I/O. Architecture guard 6.

## Phase 3 — Wendelon internal Stage 2 (Reclaim + Completion)

### Server-side reclaim orchestrator

New component:
`crates/wendelon/src/server/drop_orchestrator.rs` (or
sibling). Responsibilities:

1. Listen for `prepare_fence` events (e.g., post-apply
   notification, or polling raft state for new fences).
2. For each fenced `table_object_id`, drive physical
   reclaim through existing primitives
   (`reclaim_tombstoned_ranges` etc.).
3. After reclaim completes, submit
   `TableReclaimCompletionRequest` to consensus (via
   server-side self-submission — requires the pattern
   audited at Phase 0).
4. Maintain a server-side state machine per table
   (Fenced → Reclaiming → ReclaimCompleted →
   Finalizing → Finalized).

### Reclaim-completion apply

```rust
fn apply_table_reclaim_completion(
    state: &mut WendelonState,
    request: TableReclaimCompletionRequest,
    log_index: u64,
) -> Result<TableReclaimCompletionReceipt, WendelonAdminErrorInternal> {
    let TableReclaimCompletionRequest::V1 {
        table_object_id, fence_log_index, operation_id,
    } = request;

    let fence = state.lifecycle.get_fence(table_object_id)
        .ok_or(WendelonAdminErrorInternal::FenceNotFound { table_object_id })?;
    if fence.fence_log_index != fence_log_index || fence.operation_id != operation_id {
        return Err(WendelonAdminErrorInternal::FenceMismatch { table_object_id });
    }

    if let Some(existing) = state.lifecycle.get_completion(table_object_id) {
        if existing.operation_id == operation_id {
            return Ok(TableReclaimCompletionReceipt {
                reclaim_completion_log_index: existing.reclaim_completion_log_index,
            });
        }
    }

    state.lifecycle.mark_reclaim_complete(table_object_id, log_index, operation_id);
    Ok(TableReclaimCompletionReceipt { reclaim_completion_log_index: log_index })
}
```

## Phase 4 — Wendelon internal Stage 3 apply (Finalize) + RPC handler

### Finalize apply

```rust
fn apply_table_schema_drop_finalize(
    state: &mut WendelonState,
    table_object_id: TableObjectId,
    fence_log_index: u64,
    reclaim_completion_log_index: u64,
    operation_id: DropOperationId,
) -> Result<(), WendelonAdminErrorInternal> {
    // Generation revalidation
    let descriptor = state.table_catalog.latest_by_id(table_object_id)?;
    let fence = state.lifecycle.get_fence(table_object_id)?;
    let completion = state.lifecycle.get_completion(table_object_id)?;

    if fence.fence_log_index != fence_log_index
        || completion.reclaim_completion_log_index != reclaim_completion_log_index
        || fence.operation_id != operation_id
        || completion.operation_id != operation_id
    {
        return Err(WendelonAdminErrorInternal::FinalizeReceiptMismatch);
    }

    let name = descriptor.table_name.clone();
    state.table_catalog.unbind_latest_name(&name)?;
    state.lifecycle.remove(table_object_id);
    Ok(())
}
```

`by_id` history preserved per D8 / Codex iter-1 ADR 038 D8.

### Server-side `drop_table_schema` RPC handler

```rust
async fn handle_drop_table_schema(
    server: &WendelonServer,
    request: TableSchemaDropRequest,
) -> Result<(), WendelonAdminError> {
    let TableSchemaDropRequest::V1 {
        tenant_id, table_object_id, expected_generation, operation_id,
    } = request;

    // Submit internal prepare
    let prepare_receipt = server.submit_consensus(
        TableDropPrepareRequest::current(TableDropPrepareFields {
            tenant_id, table_object_id, expected_generation, operation_id,
        }),
    ).await?;

    // Wait for server-side reclaim orchestrator to complete
    // (driven asynchronously). Server-side state machine
    // commits TableReclaimCompletionRequest when reclaim
    // succeeds.
    let completion_receipt = server.await_reclaim_completion(
        table_object_id, prepare_receipt.fence_log_index, operation_id,
    ).await?;

    // Submit finalize
    server.submit_consensus(
        TableSchemaDropFinalizePayload {
            table_object_id,
            fence_log_index: prepare_receipt.fence_log_index,
            reclaim_completion_log_index: completion_receipt.reclaim_completion_log_index,
            operation_id,
        },
    ).await?;

    Ok(())
}
```

The whole protocol runs inside Wendelon's server. Toron's
client RPC completes once finalize commits.

## Phase 5 — Wendelon data-path fence admission (L1b)

**Skip entirely under Plan B.**

For each Wendelon data-path entry point
(`table_query_with_runtime_filter`, `table_write`,
`table_delete`, secondary-index maintenance, CDC),
add fence-state lookup before main work:

```rust
async fn handle_table_write(&self, request: TableWriteRequest) -> Result<TableWriteOutcome, ClientError> {
    if let Some(fence) = self.state.lifecycle.get_fence(request.table_object_id) {
        return Err(ClientError::Admin(WendelonAdminError::TableAlreadyDropping {
            table_object_id: request.table_object_id,
            existing_operation_id: fence.operation_id,
        }));
    }
    // ... existing write logic
}
```

Mirror across delete / scan / index-maintenance / CDC.
Architecture guards 8, 9, 10.

## Phase 6 — Toron-side thin wrapper (L2)

**Skip entirely under Plan B.**

### `TableBinding.schema_generation` widening (D7)

**File:** `crates/toron/src/catalog.rs`.

```rust
pub struct TableBinding {
    pub local_table_id: TableId,
    pub table: TableObjectId,
    pub schema_generation: u64,                  // NEW
    pub fingerprint: PhysicalDescriptorFingerprint,
    pub schema: SchemaRef,
}

impl TableBinding {
    pub fn from_descriptor(
        local_table_id: TableId,
        descriptor: &wendelon_client::TableSchemaDescriptor,
    ) -> Result<Self, CatalogError> {
        Ok(Self {
            local_table_id,
            table: descriptor.table_object_id,
            schema_generation: descriptor.schema_generation,  // NEW
            fingerprint: descriptor.physical_descriptor_fingerprint,
            schema: build_schema_from_descriptor(descriptor)?,
        })
    }
}
```

### `DropTableStorageRequest.schema_generation` widening (D7)

**File:** `crates/toron/src/storage.rs`.

```rust
#[derive(Debug, Clone)]
pub struct DropTableStorageRequest {
    pub session_id: SessionId,
    pub query_id: QueryId,
    pub tenant_id: TenantId,
    pub table_id: TableId,
    pub table_object_id: TableObjectId,
    pub schema_generation: u64,                    // NEW
    pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
    pub profile_hint: Option<QueryProfileHint>,
    pub trace_context: Option<TraceContext>,
}
```

### `WendelonStorageAdmin` (NEW; Plan A only)

**File:** `crates/toron/src/storage.rs`.

```rust
pub struct WendelonStorageAdmin {
    client: Arc<wendelon_client::Client>,
    registry: Arc<WendelonDescriptorRegistry>,
}

impl WendelonStorageAdmin {
    pub fn new(
        client: Arc<wendelon_client::Client>,
        registry: Arc<WendelonDescriptorRegistry>,
    ) -> Self {
        Self { client, registry }
    }
}

#[async_trait::async_trait]
impl StorageAdmin for WendelonStorageAdmin {
    async fn drop_table_storage(
        &self,
        request: DropTableStorageRequest,
    ) -> Result<DropTableStorageOutcome, StorageError> {
        let operation_id = DropOperationId {
            tenant_id: request.tenant_id.into(),
            table_object_id: request.table_object_id,
            schema_generation: SchemaGeneration::new(request.schema_generation),
        };
        self.client
            .drop_table_schema(TableSchemaDropRequest::current(TableSchemaDropFields {
                tenant_id: request.tenant_id.into(),
                table_object_id: request.table_object_id,
                expected_generation: SchemaGeneration::new(request.schema_generation),
                operation_id,
            }))
            .await
            .map_err(map_wendelon_admin_error_to_storage)?;
        self.registry
            .mark_finalized_drop(request.table_id, operation_id)
            .map_err(|error| StorageError::Internal {
                message: error.to_string(),
            })?;
        Ok(DropTableStorageOutcome { reclaimed_tables: 1 })
    }
}
```

That's the entire orchestration. ~15 lines plus the local
finalized-drop handoff marker. No multi-stage decomposition.
Architecture guards 16-17 enforce.

Add registry helpers:

```rust
impl WendelonDescriptorRegistry {
    fn mark_finalized_drop(
        &self,
        table: TableId,
        operation_id: DropOperationId,
    ) -> Result<(), CatalogError> { /* write-lock marker insert */ }

    fn consume_finalized_drop(
        &self,
        table: TableId,
        operation_id: DropOperationId,
    ) -> Result<(), CatalogError> { /* remove matching marker */ }
}
```

The marker is not a distributed fence or cache-invalidation
mechanism. It is a same-execution guard that prevents
`WendelonCatalog::drop_table` from locally deleting a live
descriptor if called without the prior successful Wendelon
admin operation.

### `WendelonCatalog::drop_table` lift (Plan A)

**File:** `crates/toron/src/catalog.rs`.

```rust
async fn drop_table(
    &self,
    name: &TableRef,
    if_exists: bool,
) -> Result<Option<TableBinding>, CatalogError> {
    let binding = match self.registry.try_resolve_table(name) {
        Some(b) => b,
        None if if_exists => return Ok(None),
        None => return Err(CatalogError::TableNotFound {
            table: table_name(name),
        }),
    };
    let operation_id = DropOperationId {
        tenant_id: self.tenant_id,
        table_object_id: binding.table,
        schema_generation: SchemaGeneration::new(binding.schema_generation),
    };
    self.registry
        .consume_finalized_drop(binding.local_table_id, operation_id)?;
    self.registry.remove_table(binding.local_table_id)?;
    Ok(Some(binding))
}
```

Under Plan B, preserve ADR 038's `AdminUnavailable` arm
verbatim.

### `map_wendelon_admin_error` rewrite

**File:** `crates/toron/src/catalog.rs` (or `storage.rs`
where the helper currently lives — verify at Phase 0).

```rust
fn map_wendelon_admin_error(name: &TableRef, error: ClientError) -> CatalogError {
    match error {
        ClientError::Admin(WendelonAdminError::TableMissing { .. }) =>
            CatalogError::TableNotFound { table: table_name(name) },
        ClientError::Admin(WendelonAdminError::GenerationMismatch { .. }) =>
            CatalogError::AdminFailed {
                message: format!("table {} schema generation changed during drop", table_name(name)),
            },
        ClientError::Admin(WendelonAdminError::TableAlreadyDropping { existing_operation_id, .. }) =>
            CatalogError::AdminFailed {
                message: format!(
                    "table {} drop already in progress under operation {:?}",
                    table_name(name), existing_operation_id,
                ),
            },
        ClientError::Admin(WendelonAdminError::TableAlreadyDropped { .. }) =>
            CatalogError::TableNotFound { table: table_name(name) },
        ClientError::Admin(WendelonAdminError::DependentIndexesPresent { indexes }) =>
            CatalogError::AdminFailed {
                message: format!(
                    "table {} has {} active dependent indexes; drop them first",
                    table_name(name), indexes.len(),
                ),
            },
        _ => CatalogError::AdminFailed { message: error.to_string() },
    }
}
```

**No `.contains(` substring matches** (whitelist: transport
fallback). Architecture guard 13.

### Coordinator routing helper — UNCHANGED from ADR 038

```rust
ExecutionPlan::DropTable { name, if_exists } => {
    let binding = match catalog.resolve_table(name).await {
        Ok(b) => b,
        Err(CatalogError::TableNotFound { .. }) if *if_exists => return Ok(Vec::new()),
        Err(error) => return Err(QueryError::Catalog(error)),
    };
    let outcome = admin
        .drop_table_storage(DropTableStorageRequest {
            session_id: context.session_id,
            query_id: context.query_id,
            tenant_id: context.tenant_id,
            table_id: binding.local_table_id,
            table_object_id: binding.table,
            schema_generation: binding.schema_generation,  // NEW field
            physical_descriptor_fingerprint: binding.fingerprint,
            profile_hint: context.profile_hint,
            trace_context: context.trace_context,
        })
        .await?;
    catalog.drop_table(name, false).await?;  // UNCHANGED — ADR 038 D11 single-call-site
    let _ = outcome.reclaimed_tables;  // outcome unused beyond success
    Ok(Vec::new())
}
```

No `drop_table_with_proof`. No proof threading. ADR 038
`Catalog` trait shape preserved.

### Toron functional fixtures (Phase 6)

10 fixtures listed in promoted ADR §"Required functional
fixtures (Toron-side)". Reuse ADR 038's `SpyStorageAdmin`,
`AlwaysFailStorageAdmin`, `ToggleStorageAdmin` helpers.
Add `MockWendelonClient` test double that records
`drop_table_schema` calls and returns success / failure
deterministically.

## Phase 7 — Architecture guards

**File:** `crates/toron/tests/architecture_test.rs`.

Add ~25 ADR-039 guards (full list in promoted ADR
§"Architecture Guard Set"). Group at the end of the existing
`adr038` module hierarchy.

Key new guards:

- `adr039_toron_does_not_orchestrate_wendelon_drop_stages`
  — source-asserts `WendelonStorageAdmin::drop_table_storage`
  body contains exactly one client call
  (`drop_table_schema`) and no `submit_table_drop_prepare`
  / `submit_table_reclaim_completion` /
  `submit_table_schema_drop` references.
- `adr039_toron_does_not_import_wendelon_internal_stage_types`
  — source-read across `crates/toron/**/*.rs` asserts no
  `TableDropPrepareRequest` or `TableReclaimCompletionRequest`
  symbol.
- `adr039_no_catalog_drop_table_with_proof_method` —
  source-asserts `Catalog` trait body contains no
  `drop_table_with_proof` method (negative assertion of
  the iter-2 trait widening that iter-3 deleted).
- `adr039_no_drop_table_storage_proof_type` —
  source-asserts `crates/toron/**/*.rs` contains no
  `DropTableStorageProof` symbol.
- `adr039_wendelon_catalog_drop_requires_finalized_marker`
  — source-asserts `WendelonCatalog::drop_table` consumes a
  finalized-drop marker before `remove_table`, and the
  missing-marker branch maps to a catalog admin error.
- `adr039_wendelon_drop_unavailable_only_removed_under_plan_a`
  — conditional: if L1b guards 8-10 are green AND
  Wendelon orchestrator presence indicator is green →
  `WendelonCatalog::drop_table` body has no
  `AdminUnavailable` arm; otherwise the arm remains.

Paired ADR 038 adjustment:

- ADR 038 guard 13
  (`adr038_wendelon_drop_remains_admin_unavailable_until_adr_039`)
  → renamed `adr039_wendelon_drop_admin_unavailable_removed`
  under Plan A; preserved verbatim under Plan B.

ADR 038 guard 24 (single-call-site for `catalog.drop_table`)
**unchanged** — no trait widening means the existing call
site stays. This is a structural advantage of the iter-3
redirection: ADR 038 D11 invariant survives without
modification.

## Phase 8 — Plan A / Plan B landed-state capture

After Phases 1-7, capture actual landed state per sub-lift:

```sh
# L1 Wendelon-internal substrate (consensus ops + payload types)
rg -n "submit_table_drop_prepare|submit_table_reclaim_completion" crates/wendelon/src
rg -n "Client::drop_table_schema|fn drop_table_schema" crates/wendelon/crates/wendelon-client/src/

# L1 server-side orchestrator
rg -n "drop_orchestrator|reclaim_orchestrator|await_reclaim_completion" crates/wendelon/src/server

# L1b data-path fence admission
rg -n "get_fence|is_fenced|TableAlreadyDropping" crates/wendelon/src/storage crates/wendelon/src/integration

# L2 Toron-side thin wrapper
rg -n "WendelonStorageAdmin" crates/toron/src/storage.rs
rg -n "AdminUnavailable" crates/toron/src/catalog.rs  # should be zero under Plan A

# Negative: no Toron-side orchestration
rg -n "submit_table_drop_prepare|submit_table_reclaim_completion" crates/toron/
rg -n "TableDropPrepareRequest|TableReclaimCompletionRequest" crates/toron/
rg -n "DropTableStorageProof|drop_table_with_proof" crates/toron/
# All four greps must return ZERO hits
```

Capture table:

| Lift | Plan A | Plan B | Landed | Evidence |
|------|--------|--------|--------|----------|
| L1 typed payloads + structured errors | ✓ | ✓ | ? | wendelon-client grep results |
| L1 internal consensus apply paths | ✓ | ✓ (substrate-only) | ? | apply_table_drop_prepare + apply_table_reclaim_completion + apply_table_schema_drop_finalize present |
| L1 server-side reclaim orchestrator | ✓ | — (Plan B blocker) | ? | drop_orchestrator module present |
| L1 high-level `Client::drop_table_schema` | ✓ | — (Plan B blocker) | ? | client method present + wires to server-side handler |
| L1b data-path fence admission | ✓ | — | ? | fence consultation in write/delete/scan paths |
| L2 `TableBinding.schema_generation` | ✓ | optional | ? | field present in TableBinding |
| L2 `WendelonStorageAdmin` thin wrapper | ✓ | — | ? | impl present; one client call; no stage decomposition |
| L2 `WendelonCatalog::drop_table` lift | ✓ | — | ? | AdminUnavailable removed; body consumes finalized-drop marker before registry removal |
| ADR 038 D13 safety block | removed | preserved | ? | grep AdminUnavailable in catalog.rs |
| Negative: no Toron orchestration | ✓ | ✓ | ? | zero grep hits for Wendelon internal stage symbols |
| Negative: no trait widening | ✓ | ✓ | ? | zero grep hits for drop_table_with_proof / DropTableStorageProof |

**Critical D13 invariant**: under Plan B, the
`AdminUnavailable` reject **MUST remain**, and Toron-side
`WendelonStorageAdmin` MUST NOT ship. If grep shows
`WendelonStorageAdmin` present but L1 orchestrator absent,
the implementation is in an invalid intermediate state —
either complete Plan A or revert to Plan B's substrate-only
scope.

If Plan B lands, ADR 040 = Wendelon Server-Side DROP
Orchestrator + L1b + Final Enablement promoted next cycle.

## Phase 9 — Full verification

```sh
cargo fmt --check
cargo test -p toron --test architecture_test adr039
cargo test -p toron --test drop_table_test
cargo test -p toron --test ctas_test
cargo test -p toron --test create_table_test
cargo test -p toron --test distributed_write_context_test
cargo test -p toron --test delete_test
cargo test -p toron --test insert_test
cargo test -p toron
cargo test -p wendelon-client
cargo test -p wendelon
```

Byte-identical refactor invariant verification:

```sh
for suite in drop_table_test ctas_test create_table_test distributed_write_context_test delete_test insert_test; do
    cargo test -p toron --test $suite 2>&1 | tee /tmp/adr039_phase9_${suite}.txt
    diff /tmp/adr039_phase0_${suite}.txt /tmp/adr039_phase9_${suite}.txt
done
```

ADR 029-038 fixture pass counts must be byte-identical
(only new ADR 039 fixtures add to totals).

Architecture guard verification:

```sh
cargo test -p toron --test architecture_test 2>&1 | grep "adr039_" | grep -c " ok$"
# Expected: ~25 if Plan A, ~10-15 if Plan B (guards 4, 8-10, 16-17, 25 skipped/inverted)
```

Under Plan B, document explicitly which guards skipped and
why in the promoted impl artifact.

## Risks

| ID | Risk | Mitigation |
|----|------|------------|
| R1 | Wendelon server-side orchestrator scope is larger than the cycle can carry | Plan B fallback (D13); ship substrate-only; defer orchestrator to ADR 040; safety block preserved |
| R2 | Wendelon doesn't currently support server-side self-submission to consensus (R9 from ADR) | Phase 0 audit verifies the pattern; if absent, scope into L1 or fall back to Plan B |
| R3 | `TableLifecycleCatalog` extension conflicts with retention semantics | Phase 1: fall back to sibling `TableDropFenceCatalog` |
| R4 | Wendelon `entry.log_id.index` plumbing breaks existing apply handler signatures | Additive: `_log_index` parameter ignored by existing handlers |
| R5 | Plan A removal of safety block exposes regression if `WendelonStorageAdmin` not wired in a fixture | `WendelonCatalog::drop_table` requires a finalized-drop marker; if `WendelonStorageAdmin` didn't run first, the marker is absent and the catalog path fails instead of deleting local descriptor state |
| R6 | `TableBinding.schema_generation` widening breaks existing `from_descriptor` call sites | Additive field; all call sites updated in Phase 6; `mock_table_identity` updated to set explicit generation |
| R7 | Plan B substrate-only landing creates abandoned Wendelon-internal payload types | All Plan B additions (typed payloads + structured errors + plumbing) are pure additions; activated by ADR 040's orchestrator + Toron-side wrapper |
| R8 | Idempotency confusion if two operators issue concurrent `DROP TABLE` | Target-derived `DropOperationId` → both converge to same Wendelon-internal protocol instance; second operator gets `TableAlreadyDropping` with the existing operation_id (matches their own, so they wait or retry) |
| R9 | Single Toron client call blocks until full Wendelon protocol completes; user-perceived latency long for large reclaim | Acceptable for DDL; future async-completion API can be added if latency proves problematic; not in ADR 039 scope |
| R10 | Wendelon server-side reclaim orchestrator deadlocks if server-side raft submission has fairness issues with client RPCs | Phase 1 implementation audit verifies submission pattern; if risky, add per-table reclaim queue with bounded concurrency |
| R11 | Plan B preserves the safety block but the substrate types may accidentally become reachable through some Wendelon-internal test path | Plan B test suite includes a negative test: `client.drop_table_schema` (if exposed under Plan B at all) returns a clear "not yet enabled" error; preferred: don't expose the client method under Plan B at all (gate behind `cfg(feature = "wendelon-drop")` or equivalent) |
