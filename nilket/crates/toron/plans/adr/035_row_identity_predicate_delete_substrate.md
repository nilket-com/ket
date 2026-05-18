# ADR 035: Row Identity / Predicate-Delete Substrate

## Status

Promoted. Three-way council convergence (Claude /
Codex / Gemini) reached in four iterations —
**first cross-ring substrate cycle since ADR 030**,
matching the four-iter cross-ring substrate-cycle
floor (ADR 029 + 030 prior data points). Iter-0
produced complete substrate sketch (Codex iter-0 D1-
D12 + E1-E4 code-evidence audit; Claude iter-0
endorsed wholesale + added six contributions C1-C6
that resolved four of Codex's five iter-0 open
questions; Gemini iter-1 produced four substrate-
shape disagreements). Iter-1 settled the load-
bearing shape via Codex iter-1's D1-D15 absorbed
wholesale by Claude iter-1 plus three additional
code-evidence audits (E5 / E6 / E7); Gemini iter-2
retracted three of four iter-1 wrong positions
(bool → enum; RecordBatch trait boundary →
`Vec<Identity>`; single counter → four counters;
blake3 → deterministic-no-hash). Iter-2 was
promotion-drafting with four iter-2 wording precision
items (D1 enum location, D4 Wendelon field shape,
D11 `StorageError` variant pin, guard #17 mechanism).

**Eighth consecutive cycle of the Codex correctness-
clarification pattern.** Codex iter-0 was unusually
complete (12 D-level + four code-evidence audits +
five open questions); Codex iter-1 added three more
code-evidence audits (E5 / E6 / E7) and tightened
three Wendelon-side decisions (D4 privileged-not-
general projection mode, D11 storage-error variant
pin, D13 Wendelon delete adapter method name —
`Client::table_delete`, not `table_delete_rows`).

**Gemini iter-1/2 retraction streak restored at
8-of-10** after the 7-of-9 ADR 034 mark. Gemini
iter-1 produced four substrate-shape wrong defaults
(boolean projection flag, RecordBatch trait boundary,
`WriteOutcome` reuse for delete, `blake3` mock
identities). Gemini iter-2 retracted all four after
engaging Codex iter-1 evidence pressure. Updated
cycle property: substrate-cycle wrong defaults
retract reliably when Codex + Claude joint iter-1
evidence consolidates.

**First cross-ring substrate cycle to hit 4-iter
cadence with iter-2 as promotion.** ADR 029 was 4
iters with iter-3 promotion. ADR 030 was 4 iters
with one flip-flop. ADR 035 is 4 iters with iter-2
promotion-drafting — faster cross-ring substrate
cycle than ADR 029 / 030. Cycle property: cross-
ring substrate cycles can hit the 4-iter floor when
iter-1 produces both substrate-lock and cross-ring
audit simultaneously.

**Claude iter-0 / iter-1 combined 10-of-10
contribution promotion rate.** Highest combined
promotion rate in council history. iter-0 C1-C6 all
promoted to Codex iter-1 D-level decisions; iter-1
C7-C10 all promoted to ADR 035 D4 + D11 + guard #17
+ cycle-property data point. Pattern: explicit
resolution of Codex iter-0 / iter-1 open questions
paired with code-evidence grounding promotes near-
100%.

Seventeenth instance of the ADR 015 cycle (after
016 / 017 / 018 / 019 / 022 / 023 / 024 / 025 / 026
/ 027 / 028 / 029 / 030 / 031 / 032 / 033 / 034).

## Scale boundary

**Primary:** Toron storage boundary + `coordinator/`
submodule + Wendelon table-query metadata /
`wendelon-client` options / `Client::table_delete`
data-plane surface. Surface introduces the end-to-end
substrate required for ADR 036 (SQL `DELETE WHERE`)
to remain a narrow composition cycle. **Cross-ring
substrate cycle** with one narrow Wendelon scan-
projection addition, one high-level client-wrapper
delete method over the existing wire delete request,
and a complete Toron substrate.

In scope:

- New `RowIdentityProjection` enum in
  `crates/toron/src/storage.rs` (Toron-side); parallel
  `wendelon::client::RowIdentityProjection` with
  identical variants.
- `ScanRequest::row_identity: RowIdentityProjection`
  field added.
- New `DeleteRowsRequest` struct in
  `crates/toron/src/storage.rs` carrying
  `row_key_ids: Vec<wendelon::schema::Identity>` (no
  `RecordBatch` payload).
- New `DeleteRowsOutcome` struct in
  `crates/toron/src/storage.rs` preserving delete
  accounting: `requested` from the Toron request
  length plus Wendelon's three response counters
  (`deleted` / `already_deleted` / `not_found`).
- New `StorageDeleteRows` trait in
  `crates/toron/src/storage.rs`, separate from
  `StorageWrite`.
- `InMemoryTableStorage` extended with deterministic
  hash-free identity layout
  (`[table_object_id; 16] + [zero; 8] + [ordinal; 8]`)
  + tombstone `HashSet<Identity>`. Tombstones, not
  physical removal.
- `InMemoryTableStorage` implements
  `StorageDeleteRows`; normal scans filter tombstones;
  `RowIdentityProjection::Append` scans append the
  identity column after tombstone filtering.
- New `WendelonStorageDeleteRows` adapter, with the
  same descriptor map discipline as `WendelonStorageWrite`,
  calling `wendelon_client::Client::table_delete`
  with `await_finalization: true`.
- New `crates/toron/src/coordinator/delete.rs`
  submodule with private
  `materialize_delete_row_key_ids` helper that:
  - executes a candidate scan/filter plan with
    `RowIdentityProjection::Append`;
  - validates the final column name is
    `STORAGE_ROW_KEY_ID_COLUMN`
    (`__row_key_id__`);
  - validates type is `DataType::FixedSizeBinary(32)`;
  - rejects nulls or malformed values;
  - extracts `Vec<Identity>`;
  - never returns identity columns in user result
    batches.
- Coordinator delete-path helper (private; routed by
  ADR 036, not by ADR 035 SQL surface):
  - empty candidate set short-circuits with zero
    `DeleteRowsOutcome` (does not call storage —
    Wendelon rejects empty `row_key_ids`).
- `Arc<dyn StorageDeleteRows>` wired into local
  `QueryCoordinator` (no SQL DELETE routing in this
  ADR).
- Wendelon-side change: add a parallel
  `wendelon::client::RowIdentityProjection`, expose
  it through `wendelon-client::TableQueryOptions`,
  carry it in `wendelon::client::TableQueryRequest`,
  and make it wire-visible through
  `TableQueryRequestMeta` (for example by replacing
  one padding byte with a `row_identity_projection:
  u8` field while preserving the metadata size).
  The `insert_required_column` PBC PROJECT storage-
  column reject remains preserved verbatim for
  ordinary projection paths.
- Storage adapters MUST reject scans where
  `row_identity == RowIdentityProjection::Append` and
  `projected_columns.is_some()` with
  `StorageError::Internal { message: "scan with
  row_identity=Append cannot be combined with
  projected_columns" }`.
- 18 architecture guards + 10 Toron functional
  fixtures + 3 Wendelon functional fixtures.

Out of scope:

- SQL `DELETE WHERE` syntax. Deferred to ADR 036 as
  narrow composition over ADR 035 substrate.
- `Statement::Delete`, `LogicalPlan::Delete`,
  `ExecutionPlan::Delete` — no IR variants in this
  ADR.
- SQL `UPDATE`. Future ADR after ADR 036.
- Physical storage reclaim (in-memory map cleanup,
  Wendelon descriptor deletion). Future storage-
  admin ADR.
- Distributed DELETE coordinator routing. ADR 036
  will mirror ADR 030 D13 distributed-write reject
  pattern.
- Identity column visibility to SQL (planner /
  binder / projection resolution). Identity column
  is transient, validated, and extracted before
  control returns to result paths.
- Identity column visibility through ordinary PBC
  PROJECT. The Wendelon-side change is a privileged
  table-query metadata / options field, not a
  general storage-column relaxation.
- New `RequiredColumnAxis` variant on Wendelon side.
  Privileged identity projection is carried outside
  ordinary PBC output requirements via the new query
  option / metadata field.
- New Wendelon wire RPC or identity-specific query
  RPC. ADR 035 may add the missing high-level
  `wendelon-client::Client::table_delete` wrapper
  over existing `TABLE_DELETE_REQ`; it must not add
  a new wire message or a separate
  `table_query_with_identities` method.
- Cross-crate enum re-export. Toron and Wendelon
  define parallel `RowIdentityProjection` enums; the
  Wendelon adapter maps at the wire boundary.
- `blake3` or any hash dependency in Toron.
- New external allocator crate (`bumpalo` /
  `typed-arena` / `slab` / `generational-arena`).
- New `StorageError` variant. D11 reject reuses
  existing `Internal { message }` variant with stable
  diagnostic.
- Toron-side construction of `__row_key_id__` for
  write-time identity (server-side identity
  generation remains storage-owned per ADR 030 D7 +
  Codex iter-1 E7).
- Predicate-to-storage delete API on Wendelon side.
  ADR 035 uses scan-then-tombstone-write via typed
  `Vec<Identity>`; runtime filters and PBC may
  reduce candidate batches but correctness is the
  Toron predicate result paired with attached
  identity.
- Stale-bound-plan invalidation. Identity layout is
  stable across repeated scans; delete operations
  do not invalidate prior plan handles. Future
  stale-bound-plan ADR engages.
- Plan Arena. Substrate cycle adds new types /
  trait / module / Wendelon query metadata and
  client-option fields;
  zero plan-spine pressure.

## Context

After ADR 029 → 034, the local DDL/DML composition
loop is complete (CREATE TABLE + INSERT VALUES +
INSERT SELECT + CTAS + DROP TABLE). Two gaps
remain:

1. **No row-deletion primitive.** ADR 034 added
   `DROP TABLE` but only catalog reachability; there
   is no SQL-surface path to delete individual rows.
2. **No row-identification substrate.** Toron scans
   return user-schema `RecordBatch` values; synthetic
   storage columns are hidden and Toron-side
   construction is forbidden by ADR 030 D7.

ADR 034 D15 sequenced ADR 035 as the row-identity /
predicate-delete substrate cycle that must settle
before SQL `DELETE WHERE` can be a narrow composition
cycle (ADR 036).

**Codex iter-0 E1-E4 code-evidence audit was
decisive.** Wendelon's delete API is row-key-id-
based (`wendelon::client::TableDeleteRowsRequest
{ row_key_ids: Vec<Identity> }`), not predicate-
based. Toron's `StorageScan::scan` returns user-
schema batches with no storage synthetic identity
column. ADR 030 D7 forbids Toron-side construction
of `__row_*__` synthetic columns. Wendelon's
`Identity` is the correct opaque carrier (32-byte
`FixedSizeBinary`).

**Codex iter-1 E5-E7 audit added cross-ring
precision.** Wendelon's
`crates/wendelon/src/storage/columnar.rs` rejects
storage columns on the `RequiredColumnAxis::Output`
axis with the stable diagnostic `"storage column
{storage_column:?} cannot appear in PBC PROJECT
output"`. ADR 035 cannot simply relax this reject;
the Wendelon-side change must be a privileged
table-query option that appends row identity for
Toron delete candidate materialization while
preserving the existing PBC PROJECT rejection.
Wendelon's delete method is `Client::table_delete`
(singular), not `table_delete_rows`. Wendelon's
append-only identity generation remains storage-
owned and uses BLAKE3 internally — Toron's in-
memory mock storage does not need to reproduce that;
it needs only opaque, stable, unique local
identities for test storage and coordinator
semantics.

The load-bearing decisions are: **how the row-
identity projection mode is shaped at the scan
request boundary** (D1: enum, not bool; D3:
transient identity column never SQL-visible),
**how the delete trait boundary is typed** (D5:
`Vec<Identity>`, not `RecordBatch`), **how the
delete outcome carries Wendelon's four counters**
(D6: dedicated struct, not `WriteOutcome` reuse),
**how the storage delete trait is separated from
the write trait** (D7: dedicated `StorageDeleteRows`),
**how in-memory deletion semantics work** (D9:
tombstones, not physical removal), **how mock
identities are generated without `blake3`** (D10:
deterministic prefix-suffix layout), **how the
identity scan/projection rejection is enforced**
(D11: `StorageError::Internal` with stable
diagnostic), **where the materializer lives**
(D12: `coordinator/delete.rs`), **how Wendelon
exposes identity for table queries** (D4:
privileged `row_identity` field on
`TableQueryRequest`, not general PBC relaxation),
**whether ADR 035 ships the materializer** (D12:
yes, so ADR 036 stays composition-only), **how SQL
row count maps to outcome counters** (D6 / ADR
036: `deleted` alone).

The council converged in four iterations matching
the cross-ring substrate floor. Iter-0 produced
Codex full sketch + Claude wholesale endorsement +
Gemini iter-1 substrate disagreements. Iter-1
settled load-bearing shape via Codex iter-1 D1-D15
+ Claude iter-1 C7-C10 cross-ring precision. Iter-2
was promotion-drafting with four wording precision
items.

## Decision

### D1 — `RowIdentityProjection` enum

Define in `crates/toron/src/storage.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowIdentityProjection {
    None,
    Append,
}
```

Add `pub row_identity: RowIdentityProjection,` to
`ScanRequest`.

Normal SELECT scans set `RowIdentityProjection::None`.
Delete candidate scans set `RowIdentityProjection::Append`.

**Do not** use `include_row_key_id: bool`. The enum
leaves room for future discovery modes (e.g.
`OnlyIdentity`) and makes architecture guards
clearer.

**Module location.** Defined in `toron::storage`
(Toron-side). Wendelon defines its own parallel
`wendelon::client::RowIdentityProjection` with
identical variants, and `wendelon-client` exposes it
through `TableQueryOptions` for callers. **No Toron
storage-surface re-export.** Toron's Wendelon scan
adapter maps Toron `RowIdentityProjection::Append`
to the Wendelon client option at the query boundary.
This avoids `wendelon-client` becoming part of
`toron::storage`'s public row-identity enum.

### D2 — No SQL DELETE syntax in this ADR

ADR 035 ships no `Statement::Delete`,
`LogicalPlan::Delete`, or `ExecutionPlan::Delete`
variants. ADR 036 owns SQL `DELETE WHERE`.

### D3 — Row identity is transient and not SQL-visible

When `RowIdentityProjection::Append` is used,
storage returns the row identity as the final
`__row_key_id__` `FixedSizeBinary(32)` column of
candidate batches.

The identity column is:

- not part of `ExecutionPlan::schema()`;
- not accessible by SQL column resolution;
- stripped / extracted before control returns to
  normal query result paths;
- guarded by existing planner / binder synthetic-
  name rejects (ADR 029 D12 + ADR 030 D7 carry-
  forward).

### D4 — Wendelon privileged identity projection

Wendelon-side change: extend the table-query path
end to end:

```rust
pub struct TableQueryRequest {
    // ... existing fields ...
    pub row_identity: RowIdentityProjection,
}

pub struct TableQueryOptions {
    // ... existing fields ...
    pub row_identity: RowIdentityProjection,
}

#[repr(C)]
pub struct TableQueryRequestMeta {
    // ... existing fields ...
    pub row_identity_projection: u8,
    pub _pad: [u8; 6],
    // ... existing fields ...
}
```

with `RowIdentityProjection::None` as the default
for existing callers. `TableQueryRequest` is not
itself the wire payload; `table_query_once` serializes
`TableQueryRequestMeta` plus trailing PBC/runtime-
filter bytes. Therefore the projection setting must
be copied from `wendelon-client::TableQueryOptions`
into `TableQueryRequest`, encoded in
`TableQueryRequestMeta`, decoded by the data server,
and then carried into table-query planning. Only
Toron's Wendelon scan adapter sets
`RowIdentityProjection::Append`.

The Wendelon table-query handler appends
`StorageColumn::RowKeyId` / `__row_key_id__` to
table-query output for this internal mode only, after
predicate/runtime-filter row selection so the
identity column has the same row order and row count
as the user-visible output columns.

**Do not** globally allow arbitrary user PBC PROJECT
output to include storage columns. The existing
`insert_required_column` guard at
`crates/wendelon/src/storage/columnar.rs` is correct
for normal projection and must be preserved verbatim.

**Do not** add a new `RequiredColumnAxis` variant or
a separate `table_query_with_identities` RPC method.
Option A is the minimum-surface change: one
privileged query option carried through the existing
table-query request metadata.

### D5 — Delete trait boundary uses typed identities

```rust
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

**No `RecordBatch` payload at the Toron trait
boundary.** Arrow batch encoding remains inside
Wendelon's client wire path
(`table_delete_rows_batch` builds the single-column
`FixedSizeBinary(32)` batch internally).

### D6 — Dedicated `DeleteRowsOutcome` with four counters

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

**Do not reuse `WriteOutcome { rows }`.** That shape
is append-shaped and loses delete response semantics.
`requested` is derived from
`DeleteRowsRequest.row_key_ids.len()` before the call;
`deleted`, `already_deleted`, and `not_found` map from
Wendelon's `TableDeleteRowsResponseMeta`.

ADR 036 maps `DeleteRowsOutcome.deleted` alone to
SQL row count. The other three counters remain
available for telemetry and retry reasoning.

### D7 — Dedicated `StorageDeleteRows` trait

```rust
#[async_trait::async_trait]
pub trait StorageDeleteRows: Send + Sync {
    async fn delete_rows(
        &self,
        request: DeleteRowsRequest,
    ) -> Result<DeleteRowsOutcome, StorageError>;
}
```

Separate from `StorageWrite`. Mirrors the
distinction between append payloads and tombstone
requests. Keeps ADR 030 / 032 / 033 / 034 write
contracts byte-identical.

### D8 — Empty candidate sets short-circuit in coordinator

Wendelon rejects empty `row_key_ids`. The
coordinator delete path returns a zero
`DeleteRowsOutcome` without calling
`StorageDeleteRows::delete_rows` when the
materialized candidate identity set is empty.

This lives in the coordinator helper, not as a
burden on every backend implementation.

### D9 — In-memory storage uses tombstones

Extend `InMemoryTableData` (or successor struct) in
`crates/toron/src/storage.rs` with:

- `row_key_ids: Vec<Identity>` — parallel to
  existing `batch` row order, assigned at write
  time;
- `deleted_row_key_ids: HashSet<Identity>` — set
  of tombstoned identities;
- `next_row_key_ordinal: u64` — monotonic counter
  for identity generation.

Normal scans (`RowIdentityProjection::None`) filter
tombstoned rows. Identity scans
(`RowIdentityProjection::Append`) filter tombstones
then append the identity column after filtering.

Deleting an id:

- increments `deleted` if it was live and is now
  tombstoned;
- increments `already_deleted` if it was already
  tombstoned;
- increments `not_found` if the table never had
  that id.

`requested` is always `request.row_key_ids.len()`.

No physical removal of identity bytes; no `Vec`
shrinking. Storage reclaim is a future ADR.

### D10 — Deterministic hash-free in-memory identity layout

```rust
fn in_memory_row_identity(
    table: TableObjectId,
    ordinal: u64,
) -> Identity {
    let mut bytes = [0_u8; 32];
    bytes[0..16].copy_from_slice(table.as_bytes());
    bytes[24..32].copy_from_slice(&ordinal.to_be_bytes());
    Identity::new(bytes)
}
```

Bytes 0..16 carry the 16-byte `TableObjectId`. Bytes
16..24 remain zero as a reserved namespace. Bytes
24..32 carry the big-endian row ordinal.

Required properties:

- stable across repeated scans;
- unique within a table across appends;
- no reuse after delete (monotonic `next_row_key_ordinal`);
- no `blake3`, `hash`, `sha`, or user-row-derived
  identity logic in Toron;
- hex-grep-able in test traces (e.g.
  `...0000000000000001` for the first row);
- not visible to planner, binder, or SQL.

### D11 — Identity scan rejects user projection

Storage adapters MUST return:

```rust
Err(StorageError::Internal {
    message: "scan with row_identity=Append cannot be combined with projected_columns".to_string(),
})
```

if `row_identity == RowIdentityProjection::Append`
and `projected_columns.is_some()`.

**Reuse the existing `StorageError::Internal { message }`
variant.** Source audit of `crates/toron/src/error.rs`
confirms `StorageError` variants are `TableNotFound`,
`DescriptorEvolved`, `ReadBoundUnavailable`,
`InvalidPbcForDescriptor`, `InvalidPbcProgram { message }`,
`PermissionDenied`, `Unavailable`, `Client`, `Arrow`,
`Internal { message }`. No `InvalidScanRequest`
variant; `InvalidPbcProgram` is semantically about
PBC compilation rejection; `Internal { message }` is
the right catch-all. Adding a new variant for one
use case bloats the enum.

The stable diagnostic is the architecture-guard
anchor. Identity-discovery scans are scan/filter
shaped; user projection happens after identity
extraction in the materializer.

### D12 — Candidate materializer ships in ADR 035

Add `crates/toron/src/coordinator/delete.rs` with a
private helper:

```rust
pub(crate) async fn materialize_delete_row_key_ids(
    storage: &dyn StorageScan,
    scan_request: ScanRequest,
) -> Result<Vec<Identity>, StorageError> {
    // assumes scan_request.row_identity == RowIdentityProjection::Append
    // assumes scan_request.projected_columns == None
    // collects the candidate stream, validates the trailing identity
    // column shape, extracts identities, returns them.
}
```

The helper:

- executes a candidate scan/filter plan with
  `RowIdentityProjection::Append`;
- validates the final column name is
  `STORAGE_ROW_KEY_ID_COLUMN` (`__row_key_id__`);
- validates type is `DataType::FixedSizeBinary(32)`;
- rejects nulls or malformed values;
- extracts `Vec<Identity>`;
- never returns identity columns in user result
  batches.

This keeps ADR 036 narrow: parse/bind DELETE, call
the existing materializer, call
`StorageDeleteRows`, return empty results plus
row-count semantics.

### D13 — Wendelon delete adapter

Implement `WendelonStorageDeleteRows` that:

- validates descriptor identity like
  `WendelonStorageWrite`;
- stores the same `HashMap<TableId, TableSchemaDescriptor>`
  descriptor map as `WendelonStorageWrite`;
- calls the high-level
  `wendelon_client::Client::table_delete` wrapper with
  `row_key_ids` from `DeleteRowsRequest`;
- uses `await_finalization: true`;
- calls `client.table_delete(...)` (singular
  `table_delete`, **not** `table_delete_rows`);
- maps `requested` from the pre-call request length
  and maps the three response counters from Wendelon
  into `DeleteRowsOutcome`.

The implementation must not call
`Client::table_delete_rows`; that is not the current
method name in `crates/wendelon/src/client/core.rs`.
It must also not make Toron construct
`TableDeleteRowsRequestMeta` directly; that metadata
belongs inside the Wendelon client wrapper, next to
the existing `table_write` metadata construction.

### D14 — PBC and runtime filters remain advisory

Runtime filters and PBC may reduce candidate batches
during identity-projection scans, but correctness is
the Toron predicate result paired with the attached
identity. No predicate-to-storage delete API in ADR
035; ADR 036 will use scan-then-tombstone-write.

### D15 — ADR 036 sequencing pin

ADR 036 = SQL `DELETE WHERE` as a narrow composition
ADR over ADR 035 substrate:

- `LogicalPlan::Delete { table, predicate }` (or
  equivalent narrow leaf);
- binder validates predicate against target schema;
- coordinator builds the scan/filter candidate path
  using ADR 035 materializer;
- `StorageDeleteRows::delete_rows` executes local
  tombstones or Wendelon `table_delete`;
- local coordinator only in v1; distributed
  coordinator rejects before `fragment_plan`
  (mirrors ADR 030 D13 distributed-INSERT pattern);
- SQL row count maps to `DeleteRowsOutcome.deleted`.

ADR 035 does not pre-implement these IR variants.

## Reject removal (ADR 015 cycle's seventeenth instance)

ADR 035 is a substrate-introducing cycle, not a
reject-lifting cycle. The ADR 015 cycle properties
apply differently:

- **No existing SQL/Toron reject lifted:** ADR 035
  adds private substrate only. Wendelon's
  `insert_required_column` reject for storage columns
  on `RequiredColumnAxis::Output` is preserved
  verbatim; the privileged identity append path sits
  outside ordinary PBC PROJECT output requirements
  (D4 + guard #17).
- **One new reject added:** storage adapters
  reject `row_identity == Append &&
  projected_columns.is_some()` with stable
  `StorageError::Internal` diagnostic (D11 + guard
  #12).

The pattern is preserved: each reject change has a
named-detector architecture guard; no reject is
silently widened or narrowed.

## Architecture Guard Set

**18 ADR-035-specific guards** in
`crates/toron/tests/architecture_test.rs`.

| # | Guard | Pins | File anchor |
|---|---|---|---|
| 1 | `adr035_no_sql_delete_yet` | No `Statement::Delete` / `LogicalPlan::Delete` / `ExecutionPlan::Delete` in Toron source | `sql_planner.rs`, `ir.rs`, `ir/execution.rs` |
| 2 | `adr035_scan_request_uses_row_identity_enum` | `ScanRequest` body contains `pub row_identity: RowIdentityProjection,` | `storage.rs` |
| 3 | `adr035_no_include_row_key_id_bool` | No `include_row_key_id: bool` literal anywhere in Toron source | `storage.rs` (negative) |
| 4 | `adr035_delete_request_uses_vec_identity` | `DeleteRowsRequest` body contains `pub row_key_ids: Vec<` + `Identity>` | `storage.rs` |
| 5 | `adr035_delete_request_has_no_record_batch_payload` | `DeleteRowsRequest` body contains no `RecordBatch` field | `storage.rs` (negative) |
| 6 | `adr035_delete_outcome_preserves_four_counters` | `DeleteRowsOutcome` body contains all four fields: `requested`, `deleted`, `already_deleted`, `not_found` | `storage.rs` |
| 7 | `adr035_storage_delete_rows_trait_is_separate` | `pub trait StorageDeleteRows` exists; signature returns `Result<DeleteRowsOutcome, StorageError>` | `storage.rs` |
| 8 | `adr035_storage_write_trait_unchanged` | `pub trait StorageWrite` body still returns `Result<WriteOutcome, StorageError>`; no new methods | `storage.rs` |
| 9 | `adr035_no_blake3_or_hash_identity_generation_in_toron` | No `blake3` / `Identity::blake3` / `Identity::blake3_tenant` / `Identity::blake3_keyed` / `sha` / `hash` literals in Toron-side identity construction paths | `storage.rs`, `coordinator/delete.rs`, `Cargo.toml` (negative) |
| 10 | `adr035_in_memory_identity_layout_is_prefix_suffix` | `in_memory_row_identity` (or successor name) sets bytes 0..16 from `TableObjectId::as_bytes()`; bytes 24..32 from `ordinal.to_be_bytes()`; bytes 16..24 untouched (zero) | `storage.rs` |
| 11 | `adr035_in_memory_storage_tracks_row_ids_and_tombstones` | `InMemoryTableData` body contains `row_key_ids`, `deleted_row_key_ids`, `next_row_key_ordinal` fields | `storage.rs` |
| 12 | `adr035_identity_scan_rejects_user_projection` | Storage scan path contains explicit `row_identity == Append && projected_columns.is_some()` reject; uses `StorageError::Internal`; diagnostic literal contains `"scan with row_identity=Append cannot be combined with projected_columns"` | `storage.rs` |
| 13 | `adr035_coordinator_delete_module_exists` | `crates/toron/src/coordinator/delete.rs` exists; module declared in `coordinator.rs` (or `coordinator/mod.rs` if migrated); `materialize_delete_row_key_ids` function exists | `coordinator.rs`, `coordinator/delete.rs` |
| 14 | `adr035_materializer_validates_identity_column_shape` | `materialize_delete_row_key_ids` body source-checks `STORAGE_ROW_KEY_ID_COLUMN` name + `DataType::FixedSizeBinary(32)` + null reject | `coordinator/delete.rs` |
| 15 | `adr035_select_scan_uses_row_identity_none` | Every Toron-side `ScanRequest { ... }` construction outside the materializer sets `row_identity: RowIdentityProjection::None`; or grep-equivalent default-arm assertion | `coordinator.rs`, `coordinator/scan.rs`, `exec.rs` |
| 16 | `adr035_wendelon_adapter_calls_table_delete` | Wendelon delete adapter source contains `.table_delete(` literal; no `.table_delete_rows(` literal | `storage.rs` (Wendelon adapter section) |
| 17 | `adr035_wendelon_projection_change_is_privileged` (Claude iter-1 C9; iter-2 mechanism pin) | Reads `crates/wendelon/src/storage/columnar.rs` via `wendelon_manifest_dir()` helper. Positive: `"cannot appear in PBC PROJECT output"` literal preserved; `insert_required_column` body still contains `StorageColumn` gate. Reads `crates/wendelon/src/client/core.rs`, `crates/wendelon/src/schema/table.rs`, and `crates/wendelon/crates/wendelon-client/src/lib.rs`. Positive: `TableQueryRequest` body contains `row_identity`; `TableQueryRequestMeta` carries a wire-visible row-identity byte; `TableQueryOptions` exposes `row_identity` and the wrapper copies it into the internal request | `wendelon/src/storage/columnar.rs`, `wendelon/src/client/core.rs`, `wendelon/src/schema/table.rs`, `wendelon/crates/wendelon-client/src/lib.rs` (read via Toron-side architecture test) |
| 18 | `adr035_synthetic_names_remain_rejected_in_sql` | Planner / binder synthetic-name rejects for `__row_key_id__` / `__row_hlc__` literals remain at the SQL surface; identity column never reaches SQL projection resolution | `sql_planner.rs`, `binder.rs` |

Each guard's diagnostic string names ADR 035 + the
specific decision it enforces (per the ADR 015 cycle
narrow-detector pattern).

## Required functional fixtures

**10 Toron fixtures + 3 Wendelon fixtures** required
for promotion. Toron fixtures live in
`crates/toron/tests/delete_substrate_test.rs` (one
substrate cycle, one test file — Claude iter-1 A3
naming convention carry-forward from ADR 034).
Wendelon fixtures live in Wendelon's existing client
/ storage test suites at locations the implementation
plan pins.

### Toron fixtures

1. `in_memory_storage_scan_appends_stable_row_identity`
   — write three rows; scan with
   `RowIdentityProjection::Append`; assert final
   column is `__row_key_id__`,
   `FixedSizeBinary(32)`, three rows with
   deterministic prefix-suffix layout
   (`table_object_id || zero(8) || ordinal_be(8)`);
   scan again; assert byte-identical identities.
2. `in_memory_storage_normal_scan_hides_row_identity`
   — write three rows; scan with
   `RowIdentityProjection::None`; assert no
   `__row_key_id__` column in result batches; result
   schema matches user schema exactly.
3. `in_memory_storage_delete_rows_filters_future_scans`
   — write five rows; delete two by identity;
   subsequent scan (`None`) returns three rows;
   identity scan (`Append`) returns three rows + the
   three remaining identities.
4. `in_memory_storage_delete_rows_reports_all_counters`
   — request delete of five ids: two live, two
   already-tombstoned (from prior delete), one
   never-seen; assert outcome
   `requested = 5, deleted = 2, already_deleted = 2,
   not_found = 1`.
5. `delete_rows_empty_candidate_set_short_circuits`
   — coordinator delete-path helper called with
   empty `Vec<Identity>`; assert returns
   `DeleteRowsOutcome { requested: 0, deleted: 0,
   already_deleted: 0, not_found: 0 }` without
   calling `StorageDeleteRows::delete_rows` (use a
   panicking mock adapter to prove no call).
6. `identity_extraction_rejects_wrong_name_type_width_or_null`
   — materializer fed synthetic candidate batches
   with wrong final column name / wrong type /
   wrong byte width / null entry; each case returns
   `StorageError::Internal` with a stable
   shape-mismatch diagnostic; never panics.
7. `identity_scan_rejects_user_projection` —
   `ScanRequest { row_identity: Append,
   projected_columns: Some(...), .. }` returns
   `StorageError::Internal { message: "scan with
   row_identity=Append cannot be combined with
   projected_columns" }` exactly.
8. `coordinator_materializes_delete_row_key_ids_from_filtered_scan`
   — write five rows; coordinator helper runs scan
   with a runtime filter selecting two rows;
   materializer returns exactly two identities;
   subsequent delete returns
   `requested = 2, deleted = 2`; final scan returns
   three rows.
9. `storage_synthetic_column_names_still_rejected`
   — planner / binder rejects of `SELECT
   __row_key_id__ FROM t` remain green; identity
   column does not become SQL-visible via the new
   substrate.
10. `wendelon_delete_adapter_maps_table_delete_counters`
    — adapter unit-test against a stub Wendelon
    client wrapper that returns a synthetic table-
    delete receipt; assert `DeleteRowsOutcome`
    mapping uses the request length for `requested`
    and Wendelon's three response counters for the
    remaining fields; assert `await_finalization: true`;
    assert `Client::table_delete` (not
    `table_delete_rows`) call site.

### Wendelon fixtures

1. `wendelon_table_query_with_privileged_identity_projection_appends_row_key_id`
   — call `Client::table_query` with new
   `row_identity: RowIdentityProjection::Append`;
   assert response batches end with
   `__row_key_id__` column,
   `FixedSizeBinary(32)`.
2. `wendelon_ordinary_pbc_project_storage_columns_still_rejects`
   — call ordinary `Client::table_query` (or
   `query_pbc` equivalent) with PBC PROJECT
   requesting a storage column; assert the existing
   `"cannot appear in PBC PROJECT output"` reject
   string fires with the same shape as today.
3. `wendelon_identity_projection_respects_predicate_filtering_and_tombstones`
   — Wendelon test write rows + delete some +
   apply a predicate; assert identity projection
   returns only the live rows matching the
   predicate, with stable identities.

## SoA-readiness check

ADR 021's four SoA-readiness invariants preserved:

1. **Catalog stays pure metadata for kernels.** ADR
   035 touches no catalog API; metadata-only
   discipline preserved.
2. **`#[repr(u8)]` on `StableAggregateKernel`
   unchanged.** No aggregate kernel touched.
3. **Centralized dispatch unchanged.** Delete-path
   helper is a private coordinator submodule, not a
   new dispatch site for kernels.
4. **No row-path dyn dispatch.** Identity extraction
   is column-shape validation + byte copy; no `dyn`
   added on the row path.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Untouched.
- **Brainstorm 002 (WeakControlRing).** Untouched.
- **Brainstorm 003 (Disaggregated Compute).** Touched
  but preserved — Wendelon-side gets one new
  privileged data-plane field; no fragmentation
  work added in this ADR.
- **Brainstorm 004 (Memory Arenas).** Untouched —
  ADR 035 adds new types / trait / module but no
  plan-spine pressure (no boxed children, no new
  IR variants). Substrate cycle preserving Plan
  Arena as named-debt-not-yet-pressured (extends
  the composition-trilogy pattern to additive
  substrate cycles).

No seam closes.

## What Not To Do

- **Don't use `include_row_key_id: bool`** (D1 — enum
  shape; future discovery modes need room to grow).
- **Don't define `RowIdentityProjection` in
  `wendelon::schema` and re-export to Toron** (D1 —
  parallel definitions; Toron-Wendelon enum mapping
  happens at the wire boundary).
- **Don't pass `RecordBatch` at the
  `StorageDeleteRows` trait boundary** (D5 — typed
  `Vec<Identity>`; wire encoding stays inside
  Wendelon's client).
- **Don't reuse `WriteOutcome` for delete** (D6 —
  delete preserves four counters; append shape loses
  semantic).
- **Don't extend `StorageWrite` with a delete method**
  (D7 — dedicated `StorageDeleteRows` trait
  preserves ADR 030 / 032 / 033 / 034 write
  contracts byte-identical).
- **Don't physically remove rows in InMemoryTableStorage**
  (D9 — tombstone-only; storage reclaim is a future
  ADR).
- **Don't use `blake3` / `Identity::blake3` /
  `Identity::blake3_tenant` / `Identity::blake3_keyed`
  / `sha` / `hash` for Toron identity construction**
  (D10 + guard #9 — deterministic prefix-suffix
  layout only; Toron carries the ADR 030 D7 "no
  Toron-side synthetic column construction"
  discipline forward by using opaque-not-cryptographic
  identities for mock storage).
- **Don't add a new `StorageError::InvalidScanRequest`
  variant** (D11 — reuse `StorageError::Internal
  { message }` with stable diagnostic; one-use
  variant bloats the enum).
- **Don't relax the Wendelon `insert_required_column`
  PBC PROJECT storage-column reject globally** (D4 +
  guard #17 — privileged identity projection is a
  table-query option / metadata field, not a PBC
  PROJECT axis relaxation).
- **Don't add a new `RequiredColumnAxis` variant** on
  Wendelon side (D4 — single new query option /
  metadata field is the minimum-surface change; axis
  enum stays frozen).
- **Don't add a separate `table_query_with_identities`
  RPC method** on Wendelon side (D4 — same reason).
- **Don't make Toron construct
  `TableDeleteRowsRequestMeta` directly** (D13 —
  the high-level `wendelon-client::Client::table_delete`
  wrapper owns write-id, epoch, durability, row-count,
  and finalization metadata the same way
  `table_write` does).
- **Don't combine `RowIdentityProjection::Append`
  with `projected_columns.is_some()`** (D11 + guard
  #12 — identity-discovery plans are scan/filter
  shaped; projection happens after extraction).
- **Don't return identity bytes in user-facing
  result batches** (D3 + D12 — identity column is
  validated and stripped in the materializer; never
  reaches result paths).
- **Don't make `__row_key_id__` SQL-visible** (D3 +
  guard #18 — planner / binder synthetic-name
  rejects remain green).
- **Don't call `Client::table_delete_rows` from the
  Wendelon adapter** (D13 + guard #16 — the current
  method name is `Client::table_delete` per Codex
  iter-1 E6 audit).
- **Don't pre-implement SQL DELETE IR variants in
  this ADR** (D2 + D15 + out-of-scope — ADR 036
  owns SQL DELETE).
- **Don't add a predicate-to-storage delete API on
  Wendelon side** (D14 — runtime filters / PBC
  remain advisory; correctness is Toron predicate
  paired with attached identity).
- **Don't add `bumpalo` / `typed-arena` / `slab` /
  `generational-arena`** (out-of-scope; substrate
  cycle preserves Plan Arena as named debt).
- **Don't pin a `StorageReclaim` / `StorageTombstone`
  contract** (D9 + out-of-scope — tombstones are
  in-memory; future storage-admin ADR engages).
- **Don't bundle the materializer into ADR 036**
  (D12 — substrate ADR ships the complete private
  substrate so ADR 036 stays composition-only).
- **Don't bundle DELETE substrate coverage into
  existing `insert_test.rs` / `ctas_test.rs` /
  `drop_table_test.rs`** (file-naming convention
  from ADR 034 A3 — one substrate cycle, one test
  file: `delete_substrate_test.rs`).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Boolean projection flag leaks back in | Guard #2 source-checks `RowIdentityProjection` enum; guard #3 negative-patterns `include_row_key_id` literal. |
| R2 | `RecordBatch` payload added to `DeleteRowsRequest` for "convenience" | Guard #5 negative-pattern source check. |
| R3 | `WriteOutcome` reused for delete | Guard #6 source-checks all four counters in `DeleteRowsOutcome`. |
| R4 | `StorageWrite::delete_rows` overload added | Guard #7 + #8 enforce trait separation. |
| R5 | `blake3` / hash dependency creeps into Toron identity construction | Guard #9 negative-pattern source check; `Cargo.toml` audit; matches ADR 030 D7 + ADR 031 + ADR 032 + ADR 033 + ADR 034 carry-forward invariant. |
| R6 | Mock identity layout drifts to non-deterministic | Guard #10 source-checks `bytes[0..16]` + `bytes[24..32]` + reserved zero range; fixture #1 asserts hex-grep-able pattern. |
| R7 | Physical row removal added to in-memory storage | Guard #11 source-checks `deleted_row_key_ids` HashSet field; fixture #3 asserts identity stability across delete. |
| R8 | New `StorageError` variant added for D11 reject | D11 wording pins reuse of `Internal { message }`; impl plan Phase 5 fixture asserts exact diagnostic literal. |
| R9 | Identity column leaks into SQL result paths | Guard #14 + guard #18 + fixture #9 catch leakage at materializer-output and SQL-surface levels. |
| R10 | Wendelon-side PBC PROJECT storage-column reject relaxed globally | Guard #17 reads Wendelon source via `wendelon_manifest_dir()`; positive-asserts reject string preserved; Wendelon fixture #2 asserts behavior preserved. |
| R11 | Row-identity query setting lands only on `TableQueryRequest` and never reaches the server | Guard #17 positive-asserts all three surfaces: internal `TableQueryRequest`, wire `TableQueryRequestMeta`, and public `wendelon-client::TableQueryOptions`. |
| R12 | Materializer extracts identity from wrong column position / type / width | Guard #14 source-checks shape validation; fixture #6 asserts each wrong shape returns stable error. |
| R13 | Coordinator skips empty-set short-circuit and calls Wendelon (which rejects empty) | Fixture #5 panicking-mock adapter proves short-circuit. |
| R14 | `Client::table_delete_rows` referenced (wrong method name) | Guard #16 positive-asserts `table_delete` literal; negative-asserts `table_delete_rows`. |
| R14a | Toron bypasses `wendelon-client` and constructs delete wire metadata directly | D13 pins a high-level wrapper; fixture #10 should exercise the wrapper-shaped adapter rather than request-meta construction in Toron. |
| R15 | ADR 036 reopens substrate questions (e.g. WriteOutcome reuse) | D15 sequencing pin explicit; D6 counter shape locked at substrate level. |
| R16 | Plan Arena pressure increases (new boxed children added later for delete IR) | ADR 035 adds no IR variants; ADR 036 will add `Delete` leaf without boxed children mirroring ADR 034 R1 carry-forward. |
| R17 | Distributed DELETE smuggled in via ADR 035 path | Out-of-scope explicit; no distributed routing wired for `StorageDeleteRows`; ADR 036 will own the distributed reject mirroring ADR 030 D13. |
| R18 | Stale-bound-plan invalidation after delete becomes implicit | Tombstone scans return stable schemas; identity layout stable; stale bound plans remain a future ADR (carry-forward from ADR 034 D9). |

## Recorded dissent

**None at iter-2.**

Gemini iter-1 substrate-shape disagreements (boolean
projection flag; `RecordBatch` trait boundary;
`WriteOutcome` reuse for delete; `blake3` mock
identities) all retracted at iter-2 after engaging
Codex iter-0 + Codex iter-1 evidence pressure (D1
enum shape; E1 typed-identity carrier; E1+E2
four-counter delete response; E7 storage-owned
identity generation). Retraction was clean — no
iter-3 reopening attempt.

Four Gemini iter-2 wording-level items
(`Client::table_delete_rows` → `Client::table_delete`;
bytes 0..15 → bytes 0..16; "allow `StorageColumn::RowKeyId`
in PBC PROJECT output" → "privileged identity
projection"; `StorageError::InvalidScanRequest` →
`StorageError::Internal { message }`; unspecified
guard mechanism → Toron-side architecture test via
`wendelon_manifest_dir()`) folded into this
promoted ADR; they were never dissent — they were
mechanical wording fixes.

Claude iter-0 had no wrong defaults this cycle.
All six iter-0 contributions (C1-C6) and all four
iter-1 contributions (C7-C10) promoted. Worth
recording as cycle property: composition-trilogy
stabilization extended through the first cross-ring
substrate cycle after the trilogy — iter-0 design
instincts continued improving as the substrate
landscape matured.

## Process observations

### Fourth cross-ring substrate cycle data point

| ADR | Shape | Iters | Flip-flops | Notes |
|---|---|---|---|---|
| ADR 029 | Cross-ring substrate (DDL) | 4 | 0 | Iter-3 promotion |
| ADR 030 | Cross-ring substrate (DML) | 4 | 1 | Iter-3 promotion; one substrate flip |
| **ADR 035** | **Cross-ring substrate (delete substrate)** | **4** | **0** | **Iter-2 promotion-drafting; first 4-iter cross-ring substrate cycle with iter-2 promotion** |

The 4-iter cross-ring substrate cycle cadence is
firmly established. ADR 035 hit the floor exactly
with no flip-flops and with iter-2 as promotion-
drafting — faster than ADR 029 / 030 because iter-1
produced both substrate-lock (Codex iter-1 D1-D15)
and cross-ring audit (E5 / E6 / E7) simultaneously.

Recorded cycle property: **cross-ring substrate
cycles can hit the 4-iter floor with iter-2
promotion when iter-1 produces substrate-lock and
cross-ring audit jointly.** (Ratifies prior data
point ADR 029 where iter-1 was lock-only; ADR 030
where iter-1 had one substrate flip; ADR 035 is the
faster shape.)

### Eighth consecutive Codex correctness-clarification cycle

Codex iter-0 contributed 12 D-level sketches + four
code-evidence audits (E1-E4) + five open questions;
Codex iter-1 added three more code-evidence audits
(E5 / E6 / E7) and tightened three Wendelon-side
decisions. The pattern is firmly established across
ADR 024 / 025 / 026 / 028 / 029 / 030 / 031 / 032
/ 033 / 034 / 035.

### Trilogy alternation continues

ADR 029 → 030 → 031 was the substrate trilogy (DDL
+ DML + expression-IR-replacing).
ADR 032 → 033 → 034 was the composition trilogy
(INSERT SELECT + CTAS + DROP TABLE) over that
substrate.
ADR 035 starts a new substrate phase with the row-
identity / predicate-delete cycle. ADR 036 will
likely be the first composition cycle in the new
phase (SQL `DELETE WHERE` per D15 sequencing).

Trilogy alternation pattern at 7 data points;
hypothesis test continues with ADR 036+.

### Claude iter-0/1 combined 10-of-10 promotion rate

| Claude contribution | Promoted to |
|---|---|
| iter-0 C1 ADR 035 implements materializer + `coordinator/delete.rs` location | D12 |
| iter-0 C2 `deleted` alone as SQL row count | D6 (ADR 036 hook) |
| iter-0 C3 deterministic hash-free mock identity layout | D10 (Codex iter-1 explicitly cited Claude C3) |
| iter-0 C4 reject `projected_columns.is_some()` with `Append` | D11 |
| iter-0 C5 Gemini iter-2 retraction prediction | Confirmed verbatim |
| iter-0 C6 ADR 036 sequencing pin | D15 |
| iter-1 C7 Wendelon `TableQueryRequest` field shape (Option A) | D4 + impl Phase 9 |
| iter-1 C8 pin `StorageError` variant for D11 reject | D11 + impl Phase 5 |
| iter-1 C9 architecture guard #17 cross-ring boundary | Guard #17 + impl Phase 8 |
| iter-1 C10 4-iter cross-ring cadence call | Cycle-property data point |

10-of-10 — highest combined iter-0/1 promotion rate
in council history.

### Substrate-cycle property catalog after first 035 data point

After ADR 035 (first substrate cycle since the
composition trilogy):

1. **Cross-ring substrate cycles have a 4-iter
   floor.** (ADR 029, 030, 035 — three data points)
2. **Cross-ring substrate cycles can hit iter-2
   promotion-drafting when iter-1 produces joint
   substrate-lock + cross-ring audit.** (ADR 035 new
   data point; revises the older ADR 029 / 030 iter-3
   promotion shape)
3. **Substrate cycles can be zero-existing-invariant
   break when additive (new trait / new types / new
   request fields), not substrate-replacing.** (ADR
   035 first data point; contrasts with ADR 031
   `BoundExprProgram` substrate-replacing)
4. **Two-pass Wendelon-API discovery budget applies
   to cross-ring substrate cycles.** (ADR 029 / 030
   / 035 — three data points)
5. **Composition cycles seed substrate cycles via
   explicit sequencing decisions in their final
   iteration.** (ADR 034 D15 → ADR 035; pattern data
   point #1 in the new sequencing-pin shape)

## Future work

ADR 036+ candidates carry forward the ADR 029 → 035
arc:

- **ADR 036 — SQL `DELETE WHERE`** (firm sequencing
  commitment per D15). Composition cycle over ADR
  035 substrate; expected 3-iter composition-cycle
  cadence.
- **`UPDATE WHERE`** — composition over ADR 035 row-
  identity substrate + new partial-write contract.
  Sequence: ADR 035 → 036 (DELETE) → later UPDATE
  ADR.
- **`CREATE OR REPLACE TABLE`** — lower to `DROP IF
  EXISTS x; CREATE TABLE x AS SELECT ...` once
  ADR 036 lands. Composition over ADR 029 / 033 /
  034 / 036.
- **`INSERT ... RETURNING` / `CTAS ... RETURNING`
  / `DELETE ... RETURNING`.**
- **Cross-ring distributed-write-context ADR.**
  Lifts ADR 030 D13 + ADR 032 D1 + ADR 033 D11 +
  ADR 035 (no distributed DELETE in this ADR; ADR
  036 will mirror) distributed write rejects.
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint
  substrate first.
- **Streaming / multi-batch `StorageWrite::write`
  / `StorageDeleteRows::delete_rows`** — lifts ADR
  032 D5 + D6 + ADR 033 D8 + ADR 035 single-batch
  cap.
- **Cross-ring metadata-write contract ADR.** Lifts
  `WendelonCatalog::create_table` /
  `WendelonCatalog::drop_table` `AdminUnavailable`;
  unblocks transactional CTAS + atomic DROP.
- **Storage reclaim ADR.** Physical file deletion,
  in-memory storage map cleanup, Wendelon
  descriptor deletion, tombstone collection; lifts
  D9 carry-forward + ADR 034 D9 carry-forward.
- **Stale-bound-plan invalidation ADR.**
- **Read-your-writes session HLC advancement ADR.**
- **Per-statement durability override.**
- **Hidden-aggregate `ORDER BY` keys ADR.**
- **HAVING alias visibility ADR.**
- **Non-aggregate computed `ORDER BY` ADR.**
- **Typed expression tape / opcode interpreter.**
- **Plan Arena / index-based plan IR.**
- **CSE + literal deduplication.**
- **Cranelift JIT compilation.**
- **Logical `Expr` arena migration.**
- **PBC partial-pushdown of computed-arg aggregates.**
- **Temporal types (DATE, TIMESTAMP).**
- **DECIMAL.**
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`ALTER TABLE` / `CREATE INDEX` / `CREATE VIEW`
  / `DROP VIEW` / `DROP SCHEMA` / `DROP INDEX`** —
  DDL siblings using the renamed
  `lower_ddl_table_name` helper pattern from ADR
  034 D4.
- **Constraints** (PRIMARY KEY / FOREIGN KEY /
  CHECK / column-level UNIQUE) / DEFAULT /
  GENERATED.
- **Wendelon AGG consumer wiring.**
- **Kernel graduations.**

## Definition of Done

- [ ] `RowIdentityProjection` enum defined in
      `crates/toron/src/storage.rs` with `None` +
      `Append` variants (D1).
- [ ] `ScanRequest::row_identity:
      RowIdentityProjection` field added; all
      existing call sites updated to
      `RowIdentityProjection::None` (D1 / guard #15).
- [ ] `wendelon::client::RowIdentityProjection`
      parallel enum defined with identical variants
      (D1 / D4).
- [ ] `wendelon::client::TableQueryRequest`,
      `wendelon::schema::TableQueryRequestMeta`, and
      `wendelon_client::TableQueryOptions` carry the
      row-identity projection setting, defaulting
      `None` for existing callers (D4 / guard #17).
- [ ] Wendelon table-query handler appends
      `StorageColumn::RowKeyId` for the privileged
      `RowIdentityProjection::Append` mode while
      preserving the existing
      `insert_required_column` PBC PROJECT reject
      verbatim (D4 / guard #17 / Wendelon fixtures
      #1 + #2).
- [ ] `DeleteRowsRequest` defined with typed
      `row_key_ids: Vec<wendelon::schema::Identity>`;
      no `RecordBatch` payload (D5 / guards #4 + #5).
- [ ] `DeleteRowsOutcome` defined with all four
      counters (`requested`, `deleted`,
      `already_deleted`, `not_found`) (D6 / guard
      #6).
- [ ] `StorageDeleteRows` trait defined separately
      from `StorageWrite`; signature returns
      `Result<DeleteRowsOutcome, StorageError>` (D7
      / guards #7 + #8).
- [ ] `StorageWrite` trait shape byte-identical
      pre/post-ADR-035 (guard #8).
- [ ] `InMemoryTableData` extended with `row_key_ids:
      Vec<Identity>` + `deleted_row_key_ids:
      HashSet<Identity>` + `next_row_key_ordinal:
      u64` (D9 / guard #11).
- [ ] `in_memory_row_identity` helper implements the
      deterministic prefix-suffix layout
      `[table_object_id; 16] + [zero; 8] +
      [ordinal_be; 8]` with no `blake3` / hash
      (D10 / guards #9 + #10).
- [ ] `InMemoryTableStorage::scan` filters
      tombstoned ids; identity scan appends the
      identity column after filtering (D9 / fixtures
      #1 + #2 + #3).
- [ ] `InMemoryTableStorage` implements
      `StorageDeleteRows` with four-counter
      semantics (D9 / fixture #4).
- [ ] Storage adapters reject
      `row_identity == Append &&
      projected_columns.is_some()` with
      `StorageError::Internal { message: "scan with
      row_identity=Append cannot be combined with
      projected_columns" }` (D11 / guard #12 /
      fixture #7).
- [ ] `crates/toron/src/coordinator/delete.rs`
      submodule exists; declared in
      `coordinator.rs` (or `coordinator/mod.rs`
      after migration) (D12 / guard #13).
- [ ] `materialize_delete_row_key_ids` function
      defined; validates final column name +
      type + null/width; extracts `Vec<Identity>`
      (D12 / guards #13 + #14 / fixture #6).
- [ ] Empty candidate set short-circuits in
      coordinator delete-path helper without
      calling `StorageDeleteRows::delete_rows` (D8
      / fixture #5).
- [ ] `WendelonStorageDeleteRows` adapter calls
      the high-level `wendelon_client::Client::table_delete`
      wrapper (singular; not `table_delete_rows`); uses
      `await_finalization: true`; maps `requested`
      from the pre-call request length and maps the
      three Wendelon response counters (D13 / guard
      #16 / fixture #10).
- [ ] `Arc<dyn StorageDeleteRows>` wired into local
      `QueryCoordinator`; no SQL DELETE routing in
      this ADR (D2 / guard #1).
- [ ] Identity column never reaches user-facing
      result batches (D3 / D12 / guard #14 /
      fixture #9).
- [ ] Planner / binder synthetic-name rejects for
      `__row_key_id__` remain green (D3 / guard
      #18 / fixture #9).
- [ ] **All 18 ADR-035-specific architecture
      guards pass.**
- [ ] **All 10 Toron functional fixtures pass.**
- [ ] **All 3 Wendelon functional fixtures pass.**
- [ ] All existing CREATE TABLE / VALUES INSERT /
      INSERT SELECT / CTAS / DROP TABLE tests green.
- [ ] All existing SELECT-only plan-digest
      fixtures green.
- [ ] ADR 029 / 030 / 031 / 032 / 033 / 034
      invariants preserved (including ADR 029
      catalog metadata-only, ADR 030 D7 Toron-side
      synthetic-column prohibition, ADR 030 D13
      distributed-write reject, ADR 032
      `materialize_insert_select_batch` `Ok(None)`
      zero-row contract, ADR 033
      `materialize_empty_record_batch`, ADR 034
      `lower_ddl_table_name` helper).
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
- [ ] Roadmap moves ADR 035 from candidate to
      promoted; advances active section to ADR 036
      (SQL `DELETE WHERE` per D15 sequencing
      commitment).
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
- [ ] `cargo test -p wendelon-client` green.
- [ ] `cargo test -p wendelon` green (Wendelon-side
      fixtures + existing test suite preserved).
