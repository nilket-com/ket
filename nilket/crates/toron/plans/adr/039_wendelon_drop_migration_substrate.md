# ADR 039: Wendelon DROP Migration Substrate

## Status

Promoted. Three-way council convergence (Claude / Codex /
Gemini) reached in three iterations on a 17-decision D-set,
then user-directed iter-3 architectural redirection moved
the 3-stage DROP protocol entirely server-side into Wendelon.
Final promoted D-set: **14 decisions**.

**Cross-ring admin orchestration boundary pinned**: Toron is
the compute ring; Wendelon is the storage ring; Wendelon admin
choreography lives inside Wendelon. ADR 039 matches the
single-call pattern established by ADR 038 D6 (Wendelon CREATE
Plan A via `submit_table_schema_update`): Toron makes one
high-level RPC per admin op; Wendelon owns the internal
multi-stage correctness protocol.

**One reject fully lifted (Plan A) or substrate landed +
reject preserved (Plan B)**:

- **Lifted (Plan A)**: ADR 034 D7 / ADR 037 D13 / ADR 038
  D13 `WendelonCatalog::drop_table` `AdminUnavailable`
  carry-forward. Replaced by a single high-level
  `client.drop_table_schema(TableSchemaDropRequest::current(...))`
  call. Wendelon's server-side orchestrator drives the
  3-stage fenced reclaim protocol with raft-log-index
  receipts.
- **Substrate-only (Plan B fallback)**: Wendelon-side
  typed payloads + structured admin errors + raft-log-
  index plumbing land; **no server-side orchestrator, no
  L1b data-path admission, no Toron-side wrapper.**
  `WendelonCatalog::drop_table` `AdminUnavailable` reject
  **preserved**. ADR 040 = Wendelon Server-Side DROP
  Orchestrator + L1b + Final Enablement. **Toron does
  not ship code that orchestrates a Wendelon admin
  protocol** — if Wendelon can't own the full protocol,
  Toron does not try to fill the gap.

**First cycle in council history with a user-directed
architectural redirection at promotion drafting.** ADR 037 /
ADR 038 user touchpoints were process directives (lift
naming, no-V1-suffix convention). ADR 039 iter-3 is the
first cycle where a user direction reshaped a load-bearing
architectural decision (Toron-orchestrated → Wendelon-
orchestrated). Recorded as a unique cycle artifact.

**Twelfth consecutive cycle of the Codex correctness-
clarification pattern.** Three load-bearing iter-N catches
this cycle (iter-0 fence requirement, iter-2 generation
type-confusion, iter-2 query-id lifecycle-confusion). Plus
the user-directed iter-3 redirection that restores the
Wendelon-orchestrated direction Gemini iter-1 originally
pointed at (and that Claude iter-1 C5 incorrectly overrode).

**Gemini iter-1 retraction streak advances to 13-of-15**
after ADR 038's 11-of-13 mark. Two clean Gemini iter-N
retractions: iter-0 cryptographic-token framing → iter-1
(Claude C1 raft-state reference); iter-2 fingerprint-to-
generation answer → iter-3 (Codex iter-2 D5 explicit
field).

**Claude iter-1 → iter-2 substantive retractions: two**,
both from the Toron orchestration pseudocode block. **Claude
iter-3 retraction: one major**, the entire Toron-orchestrated
stage-decomposition structure, restored to the Wendelon-
orchestrated direction Gemini iter-1 originally proposed.

**Topic-concession-compresses-next-cycle pattern advances
to 8-of-10** if Plan B lands (D13 → ADR 040 seed). Stays at
7-of-9 if Plan A lands.

Twenty-first instance of the ADR 015 cycle.

## Scale boundary

**Primary:** Wendelon control plane (three new internal
consensus ops + dedicated payload types + lifecycle drop-
state + `unbind_latest_name` primitive + structured errors +
**server-side reclaim orchestrator** + high-level
`drop_table_schema` client surface); Wendelon data path
(admission consults fence on write/delete/scan/index paths —
L1b sub-lift); Toron `WendelonStorageAdmin` (thin client
wrapper); `WendelonCatalog::drop_table` (local registry
refresh after Wendelon success).

In scope (Plan A):

- **Wendelon-internal 3-stage protocol** (D1): prepare/
  fence → physical reclaim + reclaim-completion commit →
  finalize. **All four sub-stages run server-side inside
  Wendelon.** Toron makes one client call.
- **Wendelon high-level client surface**:

  ```rust
  impl wendelon_client::Client {
      pub async fn drop_table_schema(
          &self,
          request: TableSchemaDropRequest,
      ) -> Result<(), ClientError>;
  }
  ```

  Returns after Wendelon's internal protocol completes
  finalize (name unbound, lifecycle removed, raft-state
  durable).

- **One Toron-facing public root payload type** in
  `wendelon-client`:

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
          Self::V1 { /* destructure */ }
      }
  }
  ```

  Toron call sites use `TableSchemaDropRequest::current(...)`.
  No `*V1` suffix in Toron sources.

- **Wendelon-private internal stage types**:
  `TableDropPrepareRequest`, `TableReclaimCompletionRequest`
  (with `::current(...)` constructors). These live inside
  Wendelon's server-side orchestrator and consensus apply
  paths. **Toron must not import or reference these
  symbols.** Architecture guard 16.

- **Wendelon server-side reclaim orchestrator**: new
  component. Observes the fence in raft state, drives
  physical reclaim through existing primitives
  (`reclaim_tombstoned_ranges`, `prune_versions_for_retention`,
  `prepare_tiered_table_retention_rewrite`), commits the
  reclaim-completion consensus entry from server context,
  triggers finalize commit.

- **Wendelon consensus apply paths** (all three) pass
  current `entry.log_id.index` into request-specific apply
  handlers. Receipt linkages (fence_log_index +
  reclaim_completion_log_index) live in Wendelon's
  lifecycle catalog. **Receipts are Wendelon-internal**;
  not surfaced to Toron.

- **Wendelon lifecycle drop-state**: extension to
  `TableLifecycleCatalog` carrying availability state
  alongside retention policy, OR sibling
  `TableDropFenceCatalog`. Decision at implementation audit.
  Not encoded as `TableRetentionPolicy::Disabled`.

- **Wendelon `TableSchemaCatalogV1::unbind_latest_name`
  primitive**: called at the internal finalize stage.

- **Wendelon data-path admission** (L1b sub-lift): table
  writes, deletes, scans, secondary-index maintenance, and
  CDC surfaces reject fenced `table_object_id`s
  immediately.

- **Structured `WendelonAdminError`** in `wendelon-client`:

  ```rust
  pub enum WendelonAdminError {
      TableMissing { table_object_id: TableObjectId },
      GenerationMismatch {
          expected: SchemaGeneration,
          committed: SchemaGeneration,
      },
      TableAlreadyDropping {
          table_object_id: TableObjectId,
          existing_operation_id: DropOperationId,
      },
      TableAlreadyDropped { table_object_id: TableObjectId },
      DependentIndexesPresent { indexes: Vec<IndexId> },
      Transport { message: String },
  }
  ```

  Variant count smaller than iter-2's 8: with Wendelon
  owning the protocol internally, the
  `ReclaimEvidenceInvalid` / `ReclaimNotComplete`
  variants are Wendelon-internal failure modes, not
  surfaced to Toron. Toron sees external failure modes
  only.

- **Toron `WendelonStorageAdmin`** (new) in
  `crates/toron/src/storage.rs` — thin client wrapper
  with a finalized-drop handoff marker into the shared
  registry:

  ```rust
  #[async_trait::async_trait]
  impl StorageAdmin for WendelonStorageAdmin {
      async fn drop_table_storage(
          &self,
          request: DropTableStorageRequest,
      ) -> Result<DropTableStorageOutcome, StorageError> {
          let operation_id = DropOperationId {
              tenant_id: request.tenant_id.into(),
              table_object_id: request.table_object_id,
              schema_generation: SchemaGeneration::new(
                  request.schema_generation,
              ),
          };
          self.client
              .drop_table_schema(TableSchemaDropRequest::current(
                  TableSchemaDropFields {
                      tenant_id: request.tenant_id.into(),
                      table_object_id: request.table_object_id,
                      expected_generation: SchemaGeneration::new(
                          request.schema_generation,
                      ),
                      operation_id,
                  },
              ))
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

  No `DropTableStorageProof`. No multi-stage decomposition.
  ADR 038's `DropTableStorageOutcome` shape unchanged.

- **Toron `WendelonCatalog::drop_table`** lift (Plan A):
  removes the `AdminUnavailable` arm; body consumes the
  finalized-drop marker recorded by `WendelonStorageAdmin`
  before removing the local descriptor. This keeps the
  existing `Catalog::drop_table` trait shape while preventing
  a direct local-only descriptor delete:

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
          schema_generation: SchemaGeneration::new(
              binding.schema_generation,
          ),
      };
      self.registry.consume_finalized_drop(
          binding.local_table_id,
          operation_id,
      )?;
      self.registry.remove_table(binding.local_table_id)?;
      Ok(Some(binding))
  }
  ```

- **`WendelonDescriptorRegistry` finalized-drop marker**
  (Plan A): a local handoff marker recorded after the
  Wendelon-owned `drop_table_schema` RPC returns success
  and consumed by `WendelonCatalog::drop_table`. This is not
  a distributed fence or cache-invalidation substrate; it
  only prevents the local catalog method from bypassing the
  Wendelon-authoritative DROP.

- **ADR 038 `Catalog` trait UNCHANGED**. No
  `drop_table_with_proof` method. No `DropTableStorageOutcome`
  widening. No trait churn.

- **`TableBinding.schema_generation: u64`** explicit field
  added (D6 — required so Toron can pass it to Wendelon).
- **`DropTableStorageRequest.schema_generation: u64`**
  explicit field added (D6).
- **Coordinator routing helper unchanged from ADR 038**.
  Same `resolve → admin → catalog` sequence.

- **~18 ADR-039-specific architecture guards** + ADR 038
  guard 13 paired adjustment (rename / inversion under
  Plan A; preservation under Plan B).

- **~22 required functional fixtures** across Toron and
  Wendelon test surfaces (down from iter-2's 25; Toron-
  side surface is thinner so requires fewer fixtures).

Out of scope:

- **Toron-side multi-stage decomposition.** D14: Wendelon
  owns the choreography. Toron makes one client call.
- **Trait widening (`Catalog::drop_table_with_proof`,
  `DropTableStorageOutcome.proof`).** Removed at iter-3
  redirection.
- **Toron registry transient fenced state.** Rejected as
  anti-pattern at iter-5 (Gemini disjoint-hardware
  framing). Not in promoted scope; **not deferred to a
  future ADR either**. Toron and Wendelon run on disjoint
  hardware; a local fenced map on one Toron node does not
  protect other Toron nodes in the compute ring from
  routing concurrent queries to the dropping table.
  Distributed cache invalidation across Toron compute
  nodes for transient Wendelon admin state is an
  anti-pattern. Concurrent queries during a drop route
  to Wendelon and are rejected there with
  `WendelonAdminError::TableAlreadyDropping` (or
  equivalent); Toron surfaces the structured error to
  the user.
  The Plan A finalized-drop handoff marker is explicitly not
  a transient fenced-state cache: it is a same-execution,
  post-success guard so `WendelonCatalog::drop_table` cannot
  remove a live descriptor when invoked without the preceding
  Wendelon admin call.
- **Cryptographic / HMAC / PKI receipts.** D3: raft log
  indexes only, Wendelon-internal.
- **`DropTableStorageProof` enum.** Removed at iter-3.
- **Cascading secondary-index DROP.** D12: conservative
  reject (Wendelon-internal). Future "Cascading Index
  DROP Substrate" ADR.
- **Schema history garbage collection.** ADR 039 preserves
  `by_id` history after `unbind_latest_name`; future
  retention ADR may garbage-collect.
- **`CREATE OR REPLACE TABLE`.** Composes once Plan A
  lands; named-but-not-committed candidate.
- **Stale-bound-plan invalidation.** ADR 038 D10
  invariant preserved. Future ADR.
- **`DROP TABLE ... PURGE` / `TRUNCATE` / multi-table
  drop.** Out of ADR 036 D1 SQL scope.
- **Transactional DDL.** ADR 033 D7 + ADR 037 D8 + ADR
  038 D3 non-transactional discipline preserved.
- **Read-your-writes / receipt surfacing through Toron.**
  ADR 037 D4 + ADR 038 carry-forward.
- **`blake3` / external allocator crate.**
- **Plan B widens to enable Wendelon DROP.** D13: Plan
  B is substrate-only + feature-gated. **Toron-side
  wrapper does not ship under Plan B.** Enablement waits
  until Wendelon owns the full internal protocol safely
  (ADR 040).

## Context

ADR 038 lifted L1 (Toron-local DROP physical reclaim) and
L2 (Wendelon CREATE Plan A) of the three-lift Storage-Admin
/ Reclaim Substrate scope. L3 — Wendelon DROP migration —
was explicitly deferred per ADR 038 D13 + D14 because the
consensus migration precondition decision was too rich to
compress alongside L1+L2 in one cycle.

ADR 039 owns the L3 lift. Three-iteration convergence on a
17-decision D-set produced a Toron-orchestrated 3-stage
protocol with explicit proof carrier between
`StorageAdmin` and `Catalog`. At iter-3 promotion drafting,
the user surfaced a load-bearing architectural redirection:

> "Toron should not choreograph Wendelon's DROP protocol.
> Toron should submit one high-level Wendelon admin
> request... Wendelon owns: prepare/fence, physical
> reclaim, reclaim-completion commit, final schema drop."

The redirection is consistent with positions the council
had partially proposed and then narrowed away:

- **Gemini iter-1 Stage 2** had "Wendelon storage *itself*
  submits `submit_table_reclaim_completion`." Claude iter-1
  C5 overrode this for Toron-orchestrated submission. The
  iter-3 redirection retracts the override.
- **Codex iter-1 D6** proposed putting all stages inside
  Toron's `WendelonStorageAdmin` ("Then `WendelonCatalog::drop_table`
  is a Toron-local registry convergence step"). Codex iter-2
  D4 retracted in favor of the proof-carrier model. The
  iter-3 redirection re-retracts Codex iter-2 D4, but goes
  further: not just `WendelonStorageAdmin`-orchestrated (A),
  but Wendelon-server-orchestrated (B). Toron makes one
  client call.

The architectural argument is clean: **Toron is the compute
ring; DDL operations against Wendelon's authoritative schema
state belong to Wendelon**. The single-call pattern matches
ADR 038 D6 (Wendelon CREATE Plan A — Toron makes one
`submit_table_schema_update` call; Wendelon owns the
internal consensus apply).

**Disjoint-hardware rationale** (Gemini iter-5 clarification):
Toron and Wendelon are designed to run on disjoint hardware
— they form one algebraic ring over various hardware sets,
but their roles are strictly decoupled across the network
boundary. Toron scales horizontally across many compute
nodes; Wendelon scales horizontally across many storage
nodes. Any Toron-orchestrated state for Wendelon admin
operations (transient fenced markers, raft-log-index proof
carriers, fence-aware registry caches) can't be shared
across Toron nodes without a cross-Toron-ring cache
invalidation scheme — and that is an anti-pattern. The only
authoritative state lives in Wendelon's consensus.

This sharpens D13/D14:

- Toron does not orchestrate Wendelon admin choreography
  (D14).
- Toron does not maintain transient local caches that
  mirror Wendelon's authoritative admin state — neither
  in this cycle nor in future cycles. Concurrent queries
  during a drop route to Wendelon and get rejected there
  with structured errors; Toron surfaces the rejection.

If Wendelon cannot own the full internal protocol in one
cycle, the fallback is substrate-only + feature-gated, **not
pushing the choreography back into Toron**. Enablement waits
until Wendelon is ready.

### Code evidence (Codex iter-0 audit + Codex iter-2 type review)

1. `wendelon-client::TableSchemaUpdateRequest::envelope`
   rejects `SchemaMutationKindV1::Drop` (verified ADR 038
   iter-0).
2. Wendelon consensus `apply_table_schema_update` rejects
   table schema drop with
   `"table schema drops require an explicit data-rewrite
   migration"` (verified ADR 038 iter-0).
3. `TableSchemaCatalogV1` has no removal / unbind primitive
   today. ADR 039 adds `unbind_latest_name`.
4. `TableLifecycleCatalog::remove` exists; used at internal
   finalize.
5. Wendelon raft log entries expose `entry.log_id.index`
   (Codex iter-1 audit). `apply_request(state, req)` does
   not currently propagate the entry index into request
   handlers; ADR 039 plumbs the index through.
6. `PhysicalDescriptorFingerprint` is a 32-byte BLAKE3
   digest — opaque, non-invertible. Not a generation
   carrier (Codex iter-2 D5 catch on Claude iter-1's
   `schema_generation_from_fingerprint`).
7. `query_id` is execution-trace identity, changes per SQL
   execution. Not stable across retries (Codex iter-2 D6
   catch).
8. Wendelon storage primitives
   (`reclaim_tombstoned_ranges`,
   `prune_versions_for_retention`,
   `prepare_tiered_table_retention_rewrite`) already exist.
   Wendelon's server-side orchestrator wires these
   primitives.

## Decision

Fourteen numbered decisions D1-D14. D1-D4 pin the 3-stage
protocol + raft-log-index receipts + naming convention.
D5-D7 pin the idempotency + generation product types. D8-D10
pin the lifecycle + data-path semantics. D11-D12 pin the
structured errors + index policy. D13-D14 pin the sub-lift
discipline + the Wendelon-owned choreography boundary.

### D1 — Wendelon-Internal 3-Stage Protocol

Wendelon's `drop_table_schema` RPC drives a 3-stage internal
protocol:

1. **Prepare / fence**: server validates request and records
   a drop fence in committed lifecycle metadata.
2. **Physical reclaim + reclaim-completion**: server-side
   reclaim orchestrator drives physical reclaim through
   existing Wendelon storage primitives, then commits a
   reclaim-completion consensus entry linking the
   fence_log_index + reclaim evidence.
3. **Finalize**: server commits the finalize consensus
   entry, unbinds the latest name, removes the lifecycle
   entry. Server returns success to client.

**All four sub-stages run server-side inside Wendelon.**
Toron makes one client call.

### D2 — Metadata-Only Consensus Apply

All three apply paths (prepare, reclaim-completion,
finalize) perform committed-metadata transitions only. They
MUST NOT open storage handles, inspect LSM state, delete
files, or wait on data-plane work *during apply*. (The
reclaim-completion apply commits a receipt; the physical
reclaim work runs in a separate server-side task, not in
apply.) Architecture guard 6 source-asserts no `lsm.*`,
`storage_io.*`, `read_block`, or file-system call symbols
in any of the three apply bodies.

### D3 — Raft Log Index Receipts (Wendelon-Internal)

`fence_log_index` and `reclaim_completion_log_index` are
raft log indexes of committed entries. **They live entirely
inside Wendelon's lifecycle catalog** — not surfaced to
Toron, not in any Toron-facing request or response type.
Wendelon plumbs `entry.log_id.index` into apply handlers
(D3 plumbing) and uses the indexes internally to link the
three stages.

**No cryptographic, HMAC, PKI, or shared-secret material is
introduced.** Architecture guard 5 enforces.

### D4 — Dedicated DROP Request Surface

Table DROP does not go through
`TableSchemaUpdateRequest::envelope`. The existing
create/alter envelope remains scoped to its existing
protocol.

ADR 039 adds:

- One Toron-facing root type: `TableSchemaDropRequest` (the
  request Toron submits).
- Two Wendelon-private root types:
  `TableDropPrepareRequest`,
  `TableReclaimCompletionRequest` (used by Wendelon's
  internal protocol). Toron does not import or reference
  these.

### D5 — Unsuffixed Root Types + `::current` Constructor

Public root request type names are unsuffixed; versioning
lives inside the type via sum-type variant + `::current`
constructor.

```rust
pub enum TableSchemaDropRequest {
    V1 { /* fields */ },
}

impl TableSchemaDropRequest {
    pub fn current(fields: TableSchemaDropFields) -> Self {
        Self::V1 { /* destructure */ }
    }
}
```

Toron call sites use `TableSchemaDropRequest::current(...)`.
**`crates/toron` contains no `*V1`-suffixed root request
symbol.** Architecture guards 14-15 enforce.

The Wendelon-private internal types
(`TableDropPrepareRequest`,
`TableReclaimCompletionRequest`) follow the same
convention internally (each is a sum type with a `V1`
variant + `::current` constructor) — but Toron never
imports them. Architecture guard 16 source-asserts the
non-import invariant.

### D6 — Target-Derived Idempotency

DROP idempotency is target-derived:

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

Toron passes `DropOperationId` in
`TableSchemaDropRequest`; Wendelon uses
`(DropOperationId, DropStage)` internally to identify each
stage's idempotency receipt. A new `DROP TABLE t` retry
after a crash derives the same `DropOperationId` and
resumes from where Wendelon's prior internal progress
recorded.

**`query_id` MUST NOT appear in `DropOperationId`
derivation.** `query_id` retains its trace-only role.
Architecture guard 11.

### D7 — Explicit Schema Generation Carrier

Add `schema_generation: u64` to `TableBinding` and
`DropTableStorageRequest`:

```rust
pub struct TableBinding {
    pub local_table_id: TableId,
    pub table: TableObjectId,
    pub schema_generation: u64,
    pub fingerprint: PhysicalDescriptorFingerprint,
    // existing fields...
}

pub struct DropTableStorageRequest {
    // existing 8 fields from ADR 038 D1...
    pub schema_generation: u64,
}
```

**`schema_generation` is NEVER derived from
`PhysicalDescriptorFingerprint`.** The fingerprint is an
opaque BLAKE3 digest. Architecture guard 10 forbids any
helper named or shaped like
`schema_generation_from_fingerprint`.

### D8 — Name Stays Bound Until Internal Finalize

Wendelon-internal prepare and reclaim-completion stages
leave the latest-by-name binding intact. Same-name
`CREATE TABLE` is rejected with `TableAlreadyExists`
(or the structured equivalent) until the internal
finalize stage completes.

**Finalize is the only stage that calls
`TableSchemaCatalogV1::unbind_latest_name`.**

### D9 — Drop State Is Not Retention Policy

The fence lives in an availability / drop lifecycle
state, either by extending `TableLifecycleCatalog` with
an availability field alongside `TableRetentionPolicy`
OR by adding a sibling committed `TableDropFenceCatalog`.
Decision at implementation Phase 1 audit.

**The fence MUST NOT be encoded as
`TableRetentionPolicy::Disabled` or any retention-policy
overload.** Architecture guard 7.

### D10 — Data Paths Reject Fenced Tables (L1b sub-lift)

All Wendelon table data paths consult committed fence
state and reject fenced tables immediately:

- Table writes.
- Table deletes.
- Table scans / queries.
- Secondary-index maintenance.
- CDC / live-read surfaces.

Read rejection is immediate after the internal prepare
stage commits. **This is the L1b sub-lift; required for
Plan A enablement (D13).**

### D11 — Structured Wendelon Admin Errors

Six external `WendelonAdminError` variants exposed via
`wendelon-client`:

```rust
pub enum WendelonAdminError {
    TableMissing { table_object_id: TableObjectId },
    GenerationMismatch {
        expected: SchemaGeneration,
        committed: SchemaGeneration,
    },
    TableAlreadyDropping {
        table_object_id: TableObjectId,
        existing_operation_id: DropOperationId,
    },
    TableAlreadyDropped { table_object_id: TableObjectId },
    DependentIndexesPresent { indexes: Vec<IndexId> },
    Transport { message: String },
}
```

Wendelon-internal failure modes (reclaim-evidence
mismatch, fence-not-found-during-completion, etc.) stay
inside Wendelon and don't surface to Toron — Wendelon's
internal orchestrator retries or fails atomically.

Toron-side mapping (in `WendelonStorageAdmin` /
`WendelonCatalog`) consumes the structured variants;
ADR 038's substring matching for Wendelon admin errors
is removed. Architecture guard 12.

### D12 — Conservative Secondary-Index Reject

ADR 039 rejects table DROP while dependent secondary
indexes are active. Wendelon's internal prepare apply
checks for active indexes; if any exist, returns
`DependentIndexesPresent { indexes }`. Cascading index
DROP is **out of scope**; future ADR.

### D13 — Sub-Lift Discipline + Plan B Fallback

ADR 039 names two sub-lifts at promotion time:

- **L1** — Wendelon-internal complete protocol: 3 consensus
  ops + payload types (`TableSchemaDropRequest` public,
  prepare/completion private) + lifecycle drop-state +
  `unbind_latest_name` primitive + structured admin errors
  + **server-side reclaim orchestrator** + high-level
  `drop_table_schema` client surface + **L1b data-path
  fence admission** on write/delete/scan/index paths.
- **L2** — Toron-side thin wrapper: `WendelonStorageAdmin`
  impl (~10-line method body — one client call + outcome
  mapping + finalized-drop marker) +
  `WendelonCatalog::drop_table` lift (remove
  `AdminUnavailable`; consume finalized-drop marker before
  registry removal) + `TableBinding.schema_generation` field +
  `DropTableStorageRequest.schema_generation` field +
  structured error mapping replacing ADR 038 substrings.

**Plan A (default)**: L1 + L2 in one cycle.
`WendelonCatalog::drop_table`'s `AdminUnavailable` safety
block is removed.

**Plan B fallback**: **substrate-only**. Ship Wendelon's
typed payloads + `WendelonAdminError` variants + raft-
log-index plumbing. **No server-side orchestrator. No L1b
data-path admission. No Toron-side wrapper.**
`WendelonCatalog::drop_table` `AdminUnavailable` reject
**preserved**. Toron's `DropTableStorageRequest.schema_generation`
+ `TableBinding.schema_generation` MAY land as additive
preparation, but `WendelonStorageAdmin` is not shipped.
ADR 040 = Wendelon Server-Side DROP Orchestrator + L1b +
Final Enablement.

**Plan B asymmetry is sharper than ADR 038's**: under
ADR 038, Plan B fallback would have shipped a subset of
the substrate while preserving the safety block; under
ADR 039, Plan B does not ship Toron-side wiring at all.
The principle: **Toron does not ship code that
orchestrates a Wendelon admin protocol; if Wendelon can't
own the full internal protocol, Toron doesn't try to fill
the gap.**

Plan A / B settled at implementation Phase 0 audit. Impl
Phase 8 captures per-lift landed-state (ADR 037 / ADR 038
lessons-learned discipline).

### D14 — Wendelon Owns the DROP Choreography

**Toron makes one client call per `DROP TABLE`**:

```rust
client.drop_table_schema(
    TableSchemaDropRequest::current(TableSchemaDropFields { ... }),
).await
```

Wendelon owns the internal 3-stage protocol end-to-end.

Toron does NOT:

- Submit `TableDropPrepareRequest` from any production code
  path.
- Submit `TableReclaimCompletionRequest` from any
  production code path.
- Coordinate the timing between Wendelon's internal stages.
- Hold or thread Wendelon-internal receipts
  (fence_log_index, reclaim_completion_log_index) through
  Toron-side state.
- Define a `DropTableStorageProof` carrier between
  `StorageAdmin` and `Catalog`. The proof stays inside
  Wendelon.

`WendelonStorageAdmin::drop_table_storage` body is a thin
wrapper: construct `TableSchemaDropRequest`, call
`client.drop_table_schema(...)`, return success. The
existing ADR 038 `DropTableStorageOutcome` shape (one
`reclaimed_tables: usize` field) is unchanged.

`WendelonCatalog::drop_table` body becomes a guarded local
registry refresh: it consumes the finalized-drop marker
recorded by `WendelonStorageAdmin`, then removes the
descriptor. ADR 038 D11 single-call-site invariant preserved
verbatim — the routing helper calls `catalog.drop_table(name,
false)` exactly once. **No
`Catalog::drop_table_with_proof` trait widening.** ADR
038 `Catalog` trait is byte-identical pre/post-ADR-039.

Architecture guards 16-18 source-assert the boundary.

## Reject removal (ADR 015 cycle's twenty-first instance)

ADR 039 (Plan A) removes:

1. `WendelonCatalog::drop_table` `AdminUnavailable` arm
   (ADR 034 D7 / ADR 037 D13 / ADR 038 D13 multi-cycle
   carry-forward).
2. ADR 038's substring matching in
   `map_wendelon_admin_error`.
3. Wendelon consensus `apply_table_schema_update`'s
   table-drop reject (by adding a new
   `apply_table_schema_drop` internal apply path).

Added (narrower):

1. `WendelonStorageAdmin::drop_table_storage` reject when
   `Client::drop_table_schema` returns
   `WendelonAdminError`. Mapped to existing
   `StorageError` variants where possible; new
   `StorageError::AdminFailed { message }` if needed.
2. New `CatalogError::TableDropping` (optional;
   only if registry-side local fast-fail is added — not
   in promoted scope).
3. Six new `WendelonAdminError` variants replacing ADR
   038's substring-matched rejects.
4. Wendelon prepare apply reject for active dependent
   secondary indexes (D12 conservative reject).

Under Plan B, the `WendelonCatalog::drop_table`
`AdminUnavailable` reject is preserved. ADR 039
substrate ships (Wendelon-internal typed payloads +
structured errors + plumbing); enablement defers.

## Architecture Guard Set

~18 ADR-039-specific guards + 1 paired ADR 038 guard 13
adjustment.

| # | Guard | Asserts |
|---|-------|---------|
| 1 | `adr039_drop_table_schema_client_method_exists` | `wendelon-client` exposes `Client::drop_table_schema(TableSchemaDropRequest) -> Result<(), ClientError>` |
| 2 | `adr039_schema_drop_request_root_type_exists` | `wendelon-client` exposes `TableSchemaDropRequest` as a sum type with a `V1` variant + `::current` constructor |
| 3 | `adr039_wendelon_internal_stage_types_exist` | Wendelon-internal `TableDropPrepareRequest` + `TableReclaimCompletionRequest` exist as sum types (Wendelon-private; not in `wendelon-client` public API or Toron) |
| 4 | `adr039_wendelon_owns_drop_choreography` | Wendelon's `Client::drop_table_schema` (or server-side handler) body contains the internal stage submissions OR delegates to a server-side orchestrator that does |
| 5 | `adr039_no_crypto_drop_token` | No `hmac::`, `Hmac`, `Signature`, `secret_key`, `private_key` symbols in drop / reclaim paths |
| 6 | `adr039_consensus_drop_apply_no_storage_io` | Wendelon-internal apply bodies (prepare, completion, finalize) contain no `lsm.*`, `storage_io.*`, `read_block`, file-system call symbols |
| 7 | `adr039_lifecycle_drop_state_not_retention_policy` | Fence representation is not `TableRetentionPolicy::Disabled` overload |
| 8 | `adr039_table_write_rejects_fenced_table` | Wendelon write path body references the fence state |
| 9 | `adr039_table_delete_rejects_fenced_table` | Wendelon delete path body references the fence state |
| 10 | `adr039_table_query_rejects_fenced_table` | Wendelon scan/query path body references the fence state |
| 11 | `adr039_finalize_unbinds_latest_name` | Internal finalize apply body calls `TableSchemaCatalogV1::unbind_latest_name` |
| 12 | `adr039_finalize_removes_lifecycle_entry` | Internal finalize apply body calls `TableLifecycleCatalog::remove` |
| 13 | `adr039_wendelon_admin_error_is_structured` | `map_wendelon_admin_error` body in `crates/toron/src/catalog.rs` and `crates/toron/src/storage.rs` contains no `.contains(` substring matches (transport-fallback whitelist exception) |
| 14 | `adr039_drop_rejects_when_dependent_indexes_present` | Wendelon prepare apply body contains dependent-index check + `DependentIndexesPresent` error variant |
| 15 | `adr039_no_table_schema_drop_request_v1_in_toron` | `crates/toron/**/*.rs` contains no `TableSchemaDropRequestV1` symbol |
| 16 | `adr039_toron_does_not_import_wendelon_internal_stage_types` | `crates/toron/**/*.rs` contains no `TableDropPrepareRequest` or `TableReclaimCompletionRequest` symbol |
| 17 | `adr039_toron_does_not_orchestrate_wendelon_drop_stages` | `WendelonStorageAdmin::drop_table_storage` body contains no `submit_table_drop_prepare`, `submit_table_reclaim_completion`, or `submit_table_schema_drop` symbols; only `drop_table_schema` |
| 18 | `adr039_toron_uses_drop_request_current_constructor` | Toron call sites use `TableSchemaDropRequest::current(...)`, not `::V1 { ... }` |
| 19 | `adr039_no_schema_generation_from_fingerprint` | No helper named or shaped like `schema_generation_from_fingerprint` |
| 20 | `adr039_drop_storage_request_carries_schema_generation` | `DropTableStorageRequest` body contains `schema_generation: u64` field |
| 21 | `adr039_drop_operation_id_is_target_derived` | `DropOperationId` body contains only `(tenant_id, table_object_id, schema_generation)`; no UUID / random / query-id fields |
| 22 | `adr039_query_id_remains_trace_only_for_drop` | `WendelonStorageAdmin::drop_table_storage` body + `DropOperationId` derivation contain no `query_id` reference |
| 23 | `adr039_wendelon_catalog_drop_requires_finalized_marker` | `WendelonCatalog::drop_table` consumes a finalized-drop marker before `remove_table`; missing marker maps to an admin error |
| 24 | `adr039_no_catalog_drop_table_with_proof_method` | `Catalog` trait body contains no `drop_table_with_proof` method (negative assertion of the iter-2-but-deleted trait widening) |
| 25 | `adr039_no_drop_table_storage_proof_type` | `crates/toron/**/*.rs` contains no `DropTableStorageProof` symbol |
| 26 | `adr039_wendelon_drop_unavailable_only_removed_under_plan_a` | If L1b guards 8-10 + Wendelon orchestrator presence are green → `WendelonCatalog::drop_table` has no `AdminUnavailable` arm; if any are absent → the arm remains |

ADR 038 paired adjustment:

- ADR 038 guard 13
  (`adr038_wendelon_drop_remains_admin_unavailable_until_adr_039`)
  → under Plan A, renamed `adr039_wendelon_drop_admin_unavailable_removed`;
  under Plan B, preserved verbatim (with text updated to
  reference ADR 040).

ADR 038 guard 24
(`adr038_catalog_drop_table_only_called_from_routing_helper`)
**stays unchanged** — Toron's `Catalog::drop_table` trait
shape is unchanged at ADR 039, so the single-call-site
invariant is preserved without renaming or updating.

## Required functional fixtures

**Toron-side (~10)**:

1. `wendelon_drop_table_end_to_end_via_admin_sink` —
   end-to-end through `WendelonStorageAdmin` →
   `client.drop_table_schema` (against test fixture or
   simulated Wendelon).
2. `wendelon_drop_table_storage_failure_preserves_catalog_reachability`
   — mirror of ADR 038 fixture against Wendelon admin.
3. `wendelon_drop_table_retry_after_failure_converges`
   — second `DROP TABLE t` re-submits same
   `TableSchemaDropRequest`; Wendelon-internal idempotency
   resumes.
4. `wendelon_drop_table_storage_admin_returns_minimal_outcome`
   — assert `DropTableStorageOutcome.reclaimed_tables == 1`
   on success (no proof field).
5. `drop_idempotency_id_is_target_derived_not_query_id_derived`
   — issue two `DROP TABLE t` statements with distinct
   `query_id`s; assert both succeed (Wendelon-internal
   convergence; Toron-side semantics opaque to retry).
6. `schema_generation_in_binding_is_explicit_field` —
   runtime test on `TableBinding.schema_generation`.
7. `mock_catalog_drop_table_unchanged_from_adr_038`
   — assert ADR 038's `MockCatalog::drop_table` behavior
   byte-identical pre/post-ADR-039.
8. `wendelon_catalog_drop_table_requires_finalized_marker`
   — assert `WendelonCatalog::drop_table` body
   consumes a finalized-drop marker before registry remove;
   missing marker fails; no `client.drop_table_schema` call
   inside.
9. `structured_wendelon_admin_errors_replace_substring_matching`
   — source-read assertion that
   `map_wendelon_admin_error` has no `.contains(` outside
   transport-fallback whitelist.
10. `drop_table_request_root_type_unsuffixed_in_toron`
    — source-read; no `TableSchemaDropRequestV1` or
    `TableDropPrepareRequest` / `TableReclaimCompletionRequest`
    in Toron sources.

**Wendelon-side (~12)**:

11. `client_drop_table_schema_public_method_exists`
12. `wendelon_internal_drop_protocol_runs_three_stages`
13. `consensus_drop_prepare_records_fence_and_returns_log_index`
14. `consensus_drop_prepare_idempotent_on_same_operation_id`
15. `server_side_reclaim_orchestrator_observes_fence`
16. `consensus_reclaim_completion_links_fence_log_index`
17. `consensus_finalize_validates_completion_then_unbinds_name`
18. `consensus_finalize_removes_lifecycle_entry`
19. `consensus_finalize_preserves_by_id_history`
20. `data_path_write_rejects_fenced_table`
21. `data_path_scan_rejects_fenced_table`
22. `consensus_prepare_rejects_dependent_indexes`

**Total: ~22 required fixtures** (down from 25 under
iter-2's Toron-orchestrated shape; Wendelon-side count
grew, Toron-side count shrank).

## SoA-readiness check

No row-path expression evaluation changes. No new
`AggregateOutputRule`, `AggState`, `StableAggregateKernel`
catalog changes. SoA-readiness invariants from ADR 021
preserved.

## Brainstorm-seam touch check

- **001 Cranelift JIT compilation.** Untouched.
- **002 IR-arena migration.** Untouched.
  `ExecutionPlan` / `LogicalPlan` variant counts unchanged
  (`ExecutionPlan::DropTable` body unchanged per ADR 038
  D2 carry-forward).
- **003 Ring-router asymmetry / `WeakControlRing`
  coordinator API.** Touched additively — ADR 039 sharpens
  the cross-ring admin orchestration boundary (Wendelon
  owns admin choreography). No trait widening on the
  asymmetric capability surfaces.
- **004 Plan Arena / index-based plan IR.** Untouched.
  Eighth consecutive cycle preserving Plan Arena as
  named-debt-not-yet-pressured.

## What Not To Do

- **Don't orchestrate Wendelon's DROP protocol from
  Toron.** D14: Wendelon owns the choreography. Architecture
  guards 16-18.
- **Don't import Wendelon-internal stage types into
  Toron.** `TableDropPrepareRequest` and
  `TableReclaimCompletionRequest` are Wendelon-private.
  Architecture guard 16.
- **Don't introduce a `Catalog::drop_table_with_proof`
  trait method.** Architecture guard 23 (negative
  assertion).
- **Don't introduce a `DropTableStorageProof` carrier
  type.** Architecture guard 24.
- **Don't use cryptographic / HMAC / PKI receipts.** D3:
  raft-log-index receipts only (Wendelon-internal).
- **Don't allow consensus apply to do I/O.** All three
  apply paths are pure committed-metadata. D2.
- **Don't encode the fence as
  `TableRetentionPolicy::Disabled`.** D9.
- **Don't widen `TableSchemaUpdateRequest::envelope` to
  carry Drop.** Dedicated typed payloads (D4).
- **Don't suffix root request types with `V1`.** D5.
- **Don't derive `schema_generation` from the
  fingerprint.** D7. Architecture guard 19.
- **Don't derive DROP idempotency from `query_id`.** D6.
  Architecture guard 22.
- **Don't enable Wendelon DROP under Plan B.** D13: Plan
  B is substrate-only + feature-gated; Toron-side wrapper
  does not ship. Architecture guard 25.
- **Don't unbind the name during prepare or reclaim.**
  Only Wendelon-internal finalize calls
  `unbind_latest_name`. D8.
- **Don't cascade dependent secondary indexes** in ADR
  039. Conservative reject (D12).
- **Don't reuse `TableObjectId` after drop** (ADR 038 D6
  / Codex iter-1 ADR 038 D8 carry-forward).
- **Don't add `blake3` or hash-derived `TableObjectId`s.**
- **Don't add background GC / scheduler threads to
  Toron.** Wendelon's server-side reclaim orchestrator is
  a Wendelon-internal component; Toron stays synchronous.
- **Don't expose Wendelon-internal failure modes
  (reclaim-evidence mismatch, fence-not-found-during-
  completion) through Toron's structured error surface.**
  Wendelon's internal orchestrator retries or fails
  atomically; Toron sees only external failure variants.
- **Don't promise Plan A and silently land Plan B.** Impl
  Phase 8 per-lift landed-state capture is the evidence
  record.
- **Don't push the choreography back into Toron when
  Wendelon can't own it.** D13: substrate-only / feature-
  gated fallback. Enablement waits until Wendelon is
  ready.
- **Don't maintain transient Toron-side caches that
  mirror Wendelon admin state.** Disjoint-hardware
  framing: Toron scales across many compute nodes; a
  local fenced map on one node does not protect others.
  Cross-Toron cache invalidation for transient Wendelon
  admin state is an anti-pattern. The authoritative state
  lives in Wendelon's consensus; concurrent queries route
  to Wendelon and get rejected there. Toron surfaces the
  structured error.

## Risk register

| ID | Risk | Mitigation |
|----|------|------------|
| R1 | Wendelon server-side reclaim orchestrator is larger than the cycle can carry (new server component + async reclaim driver + completion-commit-from-server context) | Plan B fallback (D13); ship substrate-only; defer orchestrator to ADR 040; safety block preserved |
| R2 | Wendelon's existing reclaim primitives (`reclaim_tombstoned_ranges` etc.) don't compose cleanly into a fence-observing orchestrator | Phase 1 implementation audit verifies; if primitives need new APIs, defer to ADR 040 |
| R3 | `TableSchemaCatalogV1::unbind_latest_name` primitive interaction with concurrent schema-update operations | Wendelon-internal generation check at apply time; structured `GenerationMismatch` error |
| R4 | One client RPC blocking on the entire 3-stage Wendelon-internal protocol creates user-perceived latency | Wendelon's server-side reclaim is the slow part; the consensus stages are cheap; total latency is dominated by physical reclaim. Acceptable for DDL operations. Alternative: async-completion API (client gets a handle, polls) — deferred to future ADR if latency proves problematic. |
| R5 | UUID-derived `TableObjectId` collision with retired ids | ADR 038 D6 carry-forward; v4 122-bit random space; consensus duplicate-object-id reject as safety net |
| R6 | Dependent-secondary-index reject (D12) blocks legitimate user DROPs | Future cascading-index-drop ADR; current contract returns structured `DependentIndexesPresent` with index list so users know to drop indexes first |
| R7 | Plan B's substrate-only landing creates abandoned Wendelon-internal payload types until ADR 040 activates them | `WendelonAdminError` variants and request types are pure additions; no orphaned client surface (since `Client::drop_table_schema` itself ships only under Plan A) |
| R8 | Idempotency confusion if two operators issue concurrent `DROP TABLE` against the same table | `TableAlreadyDropping` structured error returns existing `operation_id`; target-derived IDs make concurrent operators converge to the same protocol instance |
| R9 | The Wendelon-internal completion-commit-from-server-context pattern requires new infrastructure (server submitting to its own raft) that doesn't exist today | Phase 1 implementation audit verifies whether Wendelon already has the pattern (control-plane self-submission); if not, scope this into L1 or fall back to Plan B |

## Recorded dissent

- **Gemini iter-0 cryptographic-token framing** — retracted
  at iter-1.
- **Gemini iter-2 #2 (derive generation from
  fingerprint)** — retracted at iter-3 (Codex iter-2 D5).
- **Codex iter-1 D6 (Toron's `WendelonStorageAdmin`
  performs all stages)** — retracted at Codex iter-2 D4
  in favor of explicit proof carrier; that retraction
  then re-retracted at user-directed iter-3 redirection
  in favor of Wendelon-server-orchestrated.
- **Claude iter-0 C2 (two-stage consensus, no fence)** —
  retracted at iter-1.
- **Claude iter-1 C5
  `schema_generation_from_fingerprint`** — retracted at
  iter-2.
- **Claude iter-1 C5
  `derive_idempotency_key(query_id, ...)`** — retracted at
  iter-2.
- **Claude iter-1 C5 Toron-orchestrated 3-stage sketch
  (Stages 1 + 2 inside `WendelonStorageAdmin`; Stage 3
  inside `Catalog::drop_table_with_proof`)** — retracted at
  user-directed iter-3 redirection.
- **Claude iter-2 D11 (explicit Toron proof carrier)** —
  retracted at iter-3.
- **Claude iter-2 D12 (Wendelon catalog requires Wendelon
  proof)** — retracted at iter-3 (no proof crosses into
  Toron).
- **Claude iter-2 D13 (registry transient fenced state)**
  — demoted at iter-3 (no longer load-bearing; optional
  future addition).
- **Gemini iter-4 D2 (retain a thin proof receipt on
  `DropTableStorageOutcome` for observability)** —
  recorded but not adopted. The user's iter-3 direction
  was explicit: "No `Catalog::drop_table_with_proof`. No
  `DropTableStorageProof`." Gemini iter-4 otherwise
  ratifies the Wendelon-orchestrated direction and
  retracts iter-3 endorsement of Toron-side orchestration.
  The promoted ADR uses `DropTableStorageOutcome {
  reclaimed_tables: 1 }` unchanged from ADR 038; future
  observability needs are deferred (could be served by
  tracing spans or a separate audit ADR).
- **Gemini iter-5 ratification** — Gemini iter-5 closes
  the convergence with two clarifying signals: (1) the
  **disjoint-hardware framing** — Toron and Wendelon run
  on disjoint hardware, sharpening the architectural
  rationale for D14; (2) **formal retraction of the local
  fenced-state cache as an anti-pattern** — distributed
  cache invalidation across Toron compute nodes for
  transient Wendelon admin state is not just deferred,
  it is rejected. Both signals are folded into the
  Context and What-Not-To-Do sections of this promoted
  ADR. Gemini iter-5 also retracts the iter-4 D2 proof
  receipt position implicitly (Toron-side cache of any
  kind, including proof receipts, is the same
  anti-pattern under disjoint-hardware framing).

No outstanding dissents at promotion.

## Process observations

### First cycle with user-directed architectural redirection at promotion drafting

ADR 037 / ADR 038 user touchpoints were process directives
(naming conventions, lift discipline at promotion). ADR
039 iter-3 is the first time a user direction reshaped a
load-bearing architectural decision (Toron-orchestrated
choreography → Wendelon-orchestrated choreography). The
redirection deleted 3 of 17 decisions (D11/D12/D13 from
iter-2 D-set), restructured 2 (D14/D16 → D13/D14),
shrunk the architecture guard set by ~7 guards, shrunk
the Toron-side fixture set, grew the Wendelon-side
fixture set, and clarified the cross-ring admin
orchestration boundary in writing.

Recorded as a unique cycle artifact. Cycle-property
hypothesis at 1 data point: user-directed architectural
redirections at promotion drafting *concentrate* council
positions — pulling the council toward a structurally
cleaner shape that earlier iterations had partial signals
for (Gemini iter-1, Codex iter-1 D6) but had narrowed
away.

### Three-iteration partial-scope substrate cadence confirmed at 2 data points

ADR 038 = 2 iters; ADR 039 = 3 iters. Both partial-scope
substrate cycles compress below the full-scope 4-iter
floor. Refined cycle property: **partial-scope cross-
ring substrate cycles converge in 2-3 iterations.**

### Twelfth consecutive Codex correctness-clarification cycle

Three load-bearing iter-N corrections (iter-0 fence,
iter-2 type-confusion, iter-2 lifecycle-confusion).
Density consistent with ADR 037.

### Claude substrate-cycle retractions concentrate in pseudocode-sketch sections (2 data points)

ADR 039 iter-1's pseudocode block had two type/lifecycle
errors (retracted at iter-2). ADR 039 iter-1 / iter-2
pseudocode also encoded the wrong choreography ownership
(retracted at iter-3). Pattern hypothesis confirmed at 2
data points: **Claude substrate-cycle pseudocode sketches
introduce errors at higher rate than protocol-design
prose**. Future cycles should treat pseudocode as
contestable artifact, not authoritative.

### Cross-ring admin orchestration boundary now in writing

ADR 038 D6 established the single-call pattern for
Wendelon CREATE; ADR 039 D14 explicitly extends it as a
general principle: **Wendelon admin operations are
choreographed by Wendelon, not by Toron**. The boundary
is now memorialized in roadmap + ADR text + ADR 039
architecture guards. Future Wendelon admin substrate
cycles (ALTER, RENAME, etc.) inherit the principle.

## Future work

ADR 039 commits **ADR 040 by name under Plan B only**
(D13). Under Plan A, ADR 040 is named-but-not-committed.

Beyond ADR 040:

- **Cascading index DROP substrate ADR.** Lifts D12
  conservative reject once Wendelon's index-lifecycle
  audit confirms reclaim ownership.
- **`CREATE OR REPLACE TABLE` composition.** Lowers to
  `DROP IF EXISTS x; CREATE TABLE x AS SELECT ...`.
  Composes once Plan A lands.
- **Schema history retention policy ADR.** Future
  garbage-collection of preserved `by_id` history.
- **Read-your-writes / receipt surfacing.** ADR 037 /
  038 / 039 carry-forward.
- **Transactional DDL.** Lifts non-transactional
  partial-failure mode.
- **Stale-bound-plan invalidation.** ADR 038 D10
  invariant preserved.
- **`UPDATE WHERE` substrate.** Carry-forward from ADR
  037.
- **Future Wendelon admin substrates (ALTER TABLE,
  RENAME TABLE).** Inherit ADR 039 D14 single-call
  pattern: Toron submits one high-level request;
  Wendelon owns internal choreography.

## Definition of Done

Phase 9 verification (impl plan) green; ~22 fixtures
green (modulo Plan B fixture skips); ~25 architecture
guards green (modulo Plan B guard 25 inversion);
ADR 029-038 fixtures byte-identical pre/post-ADR-039
(**fifth byte-identical refactor invariant** after ADR
035 D5 + ADR 036 D6 + ADR 037 D6 + ADR 038 byte-
identical); promoted impl artifact captures L1 + L2
landed-state explicitly (Phase 8); roadmap updated with
ADR 039 Status entry + Forward sequence transition;
`plans/wip/*_iter_*_adr_039.md` retained in `plans/wip/`
through implementation.
