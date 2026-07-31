# ADR 038: Storage-Admin / Reclaim Substrate

## Status

Promoted. Three-way council convergence (Claude /
Codex / Gemini) reached in **two iterations** — the fastest
convergence for a cross-ring substrate cycle so far, attributed
to L3 deferral (Wendelon DROP migration substrate explicitly
re-sequenced to ADR 039 at promotion time, avoiding the α-vs-β
consensus-precondition decision compression that would have
expanded the cycle past the 4-iter cross-ring substrate floor).

**Two rejects fully lifted + one explicitly deferred to ADR
039**:

1. **Lifted**: ADR 034 D9 + ADR 035 D9 physical storage reclaim
   after DROP TABLE — Toron-local `InMemoryTableStorage` and
   the new coordinator-owned `StorageAdmin` trait close the
   data-leak path.
2. **Lifted**: ADR 029 D6 / ADR 037 D12 Plan B carry-forward
   for `WendelonCatalog::create_table` `AdminUnavailable` —
   Wendelon CREATE Plan A lands here with UUID-derived
   `TableObjectId` and `RowIdentityPolicy::AppendOnly`
   (matching `mock_table_identity`).
3. **Deferred**: ADR 034 D7 / ADR 037 D13
   `WendelonCatalog::drop_table` `AdminUnavailable` — deferred
   to ADR 039 Wendelon DROP Migration Substrate per D14
   sequencing commitment. The Wendelon-side consensus migration
   precondition (α reclaim-completion-token vs β empty-table-
   precondition) is the ADR 039 primary decision.

**First cycle with an explicit Plan A / Plan B lift-naming
discipline at promotion time.** Drawn from ADR 037 lessons-
learned, where the roadmap claimed "Plan A landed" but the code
at `crates/toron/src/catalog.rs:349-358` showed Plan B. ADR 038
names three lift scopes (L1 Toron-local reclaim, L2 Wendelon
CREATE Plan A, L3 Wendelon DROP — deferred) and pins them in
D13/D14 so the promoted artifact and the landed code cannot
drift.

**Eleventh consecutive cycle of the Codex correctness-
clarification pattern.** Codex iter-0 surfaced the load-bearing
correction: `MockCatalog::drop_table` cannot reclaim storage
inline because `MockCatalog` has no storage handle and must not
get one. Reclaim is a coordinator-owned storage-admin action,
not a catalog side effect. Codex iter-0 also named the exact
Wendelon-side blockers (`TableSchemaUpdateRequest::envelope`
rejecting `SchemaMutationKindV1::Drop`; `apply_table_schema_update`
rejecting table Drop in consensus) that drove the L3 deferral
decision.

**Gemini iter-1 retraction streak advances to 11-of-13** after
the 10-of-12 ADR 037 mark. Two clean Gemini iter-0 retractions
at iter-1: D1 inline `MockCatalog` reclaim (retracted in favor
of Codex's storage-impl shape); D4 separate cache invalidation
(retracted in favor of Claude C3 / Codex D5 — registry mutation
IS the catalog refresh).

**Claude iter-0 / iter-1 combined contribution promotion rate
4-of-4** with one substantive retraction. C2 → D7 (AppendOnly);
C3 → D9 (registry-is-the-refresh); C4 → D10 (stale-bound-plan
invariant); C5 → D8 (strict admin sink); C7 candidate-α/β →
ADR 039 sequencing commitment.

**Topic-concession-compresses-next-cycle pattern advances to
7-of-9** — ADR 038 D14 → ADR 039 generates the Wendelon DROP
migration substrate seed. Pattern: substrate-defer ADRs seed
the next cycle (ADR 034 D15 → ADR 035; ADR 035 D15 → ADR 036;
ADR 036 D14 → ADR 037; ADR 037 D14 → ADR 038; ADR 038 D14 →
ADR 039).

Twentieth instance of the ADR 015 cycle (after 016 / 017 / 018
/ 019 / 022 / 023 / 024 / 025 / 026 / 027 / 028 / 029 / 030 /
031 / 032 / 033 / 034 / 035 / 036 / 037).

## Scale boundary

**Primary:** Toron `QueryCoordinator` +
`DistributedQueryCoordinator` admin sink wiring; `Catalog`
trait + `WendelonCatalog::create_table` Plan A;
`InMemoryTableStorage` reclaim impl; new
`WendelonDescriptorRegistry` shared across catalog + storage
adapters; new `StorageAdmin` trait beside `StorageWrite` /
`StorageDeleteRows`.

In scope:

- **New `StorageAdmin` trait** in `crates/toron/src/storage.rs`
  with exactly one method, `drop_table_storage`:

  ```rust
  #[async_trait::async_trait]
  pub trait StorageAdmin: Send + Sync {
      async fn drop_table_storage(
          &self,
          request: DropTableStorageRequest,
      ) -> Result<DropTableStorageOutcome, StorageError>;
  }
  ```

  Request shape mirrors ADR 037's context-only discipline:
  `session_id`, `query_id`, `tenant_id`, `table_id`,
  `table_object_id`, `physical_descriptor_fingerprint`,
  `profile_hint`, `trace_context`. No `client_write_id`,
  durability override, retry counter, or finalization flag.
  Outcome shape:
  `DropTableStorageOutcome { reclaimed_tables: usize }`.

- **`InMemoryTableStorage` implements `StorageAdmin`**: removes
  the `InMemoryTableData` entry keyed by `TableId` after
  validating `table_object_id` and
  `physical_descriptor_fingerprint`. Removes batch, row
  identities, `deleted_row_key_ids`, and `next_row_key_ordinal`
  together. No granular tombstone reaping for whole-table
  drop.

- **`QueryCoordinator` + `DistributedQueryCoordinator` gain
  storage-admin sink**: `with_storage_admin` constructor +
  combined `with_writer_deleter_and_admin`. Default sink is
  `UnsupportedStorageAdmin`, which returns the stable
  diagnostic `"DROP TABLE physical reclaim requires a
  configured StorageAdmin"`. Mirrors ADR 037's `with_writer` /
  `with_writer_and_deleter` capability-boundary pattern.

- **DROP routing helper** (extension of ADR 029's
  `execute_catalog_plan` shape): resolves table identity via
  catalog, short-circuits `IF EXISTS` for unknown tables
  before any admin call, then storage-first / catalog-second:
  `admin.drop_table_storage(...)` → `catalog.drop_table(name,
  false)`. Storage failure leaves catalog reachability intact;
  catalog failure after storage success surfaces the catalog
  error (table physically gone but catalog-reachable —
  non-transactional v1 contract).

- **Shared `WendelonDescriptorRegistry`** in
  `crates/toron/src/catalog.rs` (or sibling module). Replaces
  the independent `HashMap<TableId, TableSchemaDescriptor>`
  copies owned by `WendelonCatalog`, `WendelonStorageScan`,
  `WendelonStorageWrite`, and `WendelonStorageDeleteRows`.
  Shape:

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
  ```

  Storage adapters read through the shared registry; catalog
  mutates through the shared registry. Registry mutation under
  the write lock IS the catalog refresh — no separate
  invalidation / eviction helper exists.

- **`WendelonCatalog::create_table` Plan A**: allocate UUID-
  derived 128-bit `TableObjectId` (collision-rejected by
  Wendelon consensus); build descriptor via
  `TableSchemaDescriptor::new_with_layout` with
  `RowIdentityPolicy::AppendOnly`,
  `LocalityPolicy::default_table_spread()`,
  `RecordLayoutPolicy::MultiRow(...)`, and
  `SchemaEvolutionPolicyV1::AdditiveOnly`; submit via
  `Client::submit_table_schema_update` Create mutation;
  update shared registry on success.

- **Stable diagnostics for the strict admin sink**:
  `"DROP TABLE physical reclaim requires a configured
  StorageAdmin"`. Capability-boundary failure (matches ADR 037
  precedent for `StorageWrite` / `StorageDeleteRows`
  unconfigured-sink path).

- **ADR 034 catalog-only DROP fixture migration**: explicit
  `NoopStorageAdmin` test helper installed in fixtures that
  intentionally test the catalog-only DROP path (alias
  cleanup, `IF EXISTS` short-circuit, etc.) without exercising
  storage reclaim. Production constructors continue to install
  `UnsupportedStorageAdmin`; fixture explicitness preserves
  ADR 034 intent.

- **24 ADR-038-specific architecture guards** in
  `crates/toron/tests/architecture_test.rs` + 1 paired
  adjustment to the existing variant-count catch-all.

- **8 required functional fixtures** in
  `crates/toron/tests/drop_table_test.rs` (extending ADR 034's
  test file) or a new sibling
  `storage_admin_reclaim_test.rs`.

Out of scope:

- **L3 Wendelon DROP** — deferred to ADR 039 Wendelon DROP
  Migration Substrate. The Wendelon-side `TableSchemaDropRequestV1`
  payload + consensus apply path + α-reclaim-token-vs-β-empty-
  table-precondition decision is the ADR 039 primary topic.
  ADR 038 does NOT widen `TableSchemaUpdateRequest::envelope`,
  does NOT remove the `apply_table_schema_update` Drop
  reject, and does NOT add a Wendelon-side schema-drop
  client helper.
- **Background reclaim / GC threads.** Coordinator stays
  synchronous and request-driven.
- **Per-row tombstone reaping outside whole-table drop.**
  Future tombstone-compaction ADR.
- **Transactional DDL rollback** (DROP after CREATE failure,
  CREATE after DROP failure). Non-atomicity preserved per ADR
  033 D7 + ADR 037 D8.
- **`CREATE OR REPLACE TABLE`** composition. Named in roadmap;
  composes over L1+L2+(future L3) once they land.
- **SQL `PURGE` / `TRUNCATE`.** Each is its own surface ADR.
- **Stale-bound-plan invalidation.** ADR 038 explicitly
  preserves the existing stale-bound-plan failure mode:
  bound `TaskSpec` / `ExecutionPlan::Scan` carrying a now-
  removed `TableId` fail at the storage descriptor check
  (`StorageError::TableNotFound` / `DescriptorEvolved`).
  Future stale-bound-plan invalidation ADR (named in roadmap
  substrate candidates) owns the lift.
- **Read-your-writes / receipt surfacing / `client_write_id`
  exposure.** ADR 037 D4 carry-forward.
- **Per-statement durability / client-HLC overrides.** ADR
  037 D4 carry-forward.
- **Wendelon AGG consumer wiring; PBC partial-pushdown of
  computed-arg aggregates; kernel graduations.** Not engaged.
- **Plan Arena.** No `LogicalPlan` / `ExecutionPlan` variant
  added (zero new variants in this cycle). Sixth consecutive
  cycle preserving Plan Arena as named-debt-not-yet-pressured.
- **New SQL surface.** ADR 038 touches no user-visible SQL.
- **New `StorageWrite` / `StorageDeleteRows` method.** L1
  reclaim lives on the *new* `StorageAdmin` trait, not on the
  existing write/delete traits.
- **New `StorageError` variant.** Not added. Reuses
  `TableNotFound`, `DescriptorEvolved`, and
  `StorageError::Internal { message }` for the unsupported
  admin sink, matching ADR 037's unsupported write/delete
  adapters.
- **`blake3` / external allocator crate.** UUID-derived
  object IDs use the existing `uuid` dependency path Wendelon
  already exposes; no new crate.

## Context

ADR 037 closed the cross-ring substrate that lets the
distributed coordinator own DML execution end-to-end. It also
named two pieces of substrate debt it did not lift:

- **`WendelonCatalog::create_table` Plan B fallback landed in
  code.** The promoted ADR 037 D12 said Plan A would land *if*
  `submit_table_schema_update` accepts a real
  `TableSchemaDescriptor::new_with_layout` and a defensible
  `table_object_id` allocation path exists. The
  implementation found the `table_object_id` allocation
  question rich enough to warrant deferral; Plan B preserved
  `AdminUnavailable` verbatim in `WendelonCatalog::create_table`
  at `crates/toron/src/catalog.rs:349-358`. The roadmap
  Status section was written before the implementation
  evidence landed; the language is "Plan A" but the code is
  Plan B.

- **`WendelonCatalog::drop_table` `AdminUnavailable` remains
  unaddressed.** ADR 034 D7 (Wendelon-known DROP rejects with
  `AdminUnavailable`) carried forward through ADR 037 D13
  unchanged.

Additionally, two long-deferred items from the composition
trilogy remain open:

- **Physical storage reclaim after DROP.** ADR 034 D9 + ADR
  035 D9 explicitly punted "no storage reclaim guarantee" to
  a future substrate ADR. SQL `DROP TABLE` against `MockCatalog`
  removes the catalog binding but leaves `InMemoryTableData`
  intact; the table is unreachable but the bytes remain
  allocated until process exit.

- **Catalog refresh after admin mutation.** The shipped
  Wendelon catalog + storage adapters carry independent
  `HashMap<TableId, TableSchemaDescriptor>` copies (audited at
  Codex iter-0 against the four call sites). For read-only
  fixtures, this is acceptable. Once admin mutation widens
  the descriptor set, divergence becomes a correctness
  hazard: `WendelonCatalog::create_table` could succeed while
  `WendelonStorageWrite` still rejects the new `TableId`.

The minimum substrate cycle that closes the first three of
these debts and lets ADR 039 absorb the fourth (Wendelon DROP)
without expanding scope is the natural ADR 038 shape. The
discipline drawn from ADR 037's Plan A / Plan B drift:

- Name the lift scopes explicitly at promotion time (L1 / L2
  / L3).
- Pin which lifts land in this cycle (L1 + L2) and which
  defer to a named next cycle (L3 → ADR 039).
- Capture the actual landed state per-lift in the promoted
  impl artifact (Phase 11.5 in impl plan).

This makes the promoted ADR and the landed code mechanically
checkable against each other.

### Code evidence (Codex iter-0 audit)

1. `crates/toron/src/catalog.rs:349-358` —
   `WendelonCatalog::create_table` returns
   `CatalogError::AdminUnavailable` with the literal message
   `"CREATE TABLE is not wired to Wendelon catalog
   administration"`.

2. `crates/toron/src/catalog.rs:360-377` —
   `WendelonCatalog::drop_table` returns
   `CatalogError::AdminUnavailable` for tables present in
   `by_ref` with `"DROP TABLE is not wired to Wendelon catalog
   administration"`; for missing tables, returns
   `Ok(None)` (if `IF EXISTS`) or `TableNotFound`.

3. `wendelon_client::TableSchemaUpdateRequest::envelope`
   rejects `SchemaMutationKindV1::Drop` before wire encoding
   with `"table schema drops require a rewrite migration and
   are not supported by this helper"` — proves L3 is a
   Wendelon-client substrate gap, not just a Toron wrapper
   gap.

4. Wendelon consensus `apply_table_schema_update` rejects
   table schema drop with `"table schema drops require an
   explicit data-rewrite migration"` — proves L3 requires a
   consensus-side migration precondition, blocking
   single-cycle delivery.

5. `TableSchemaCatalogV1` has `upsert`, `get`,
   `latest_by_name`, `latest_by_id`, tenant listing — no
   table-removal / name-unbind primitive. L3 needs a new
   helper (`unbind_latest_name` or equivalent).

6. `TableLifecycleCatalog::remove` exists — lifecycle cleanup
   has a natural hook once schema drop lands.

7. Toron Wendelon storage adapters (`WendelonStorageScan`,
   `WendelonStorageWrite`, `WendelonStorageDeleteRows`) each
   own a `HashMap<TableId, TableSchemaDescriptor>` copy.
   Verified by source grep at iter-0.

8. `InMemoryTableStorage` data lives in `Mutex<HashMap<TableId,
   InMemoryTableData>>`. Whole-entry removal under the lock
   reclaims `batch`, `row_key_ids`, `deleted_row_key_ids`,
   and `next_row_key_ordinal` together — no per-field
   bookkeeping needed.

9. ADR 029 SQL `CREATE TABLE` syntax has no PRIMARY KEY
   declaration. Existing `mock_table_identity` at
   `crates/toron/src/catalog.rs:402-415` uses
   `RowIdentityPolicy::AppendOnly`. Reusing the policy
   verbatim in Wendelon CREATE avoids inventing policy inside
   ADR 038 (mirrors ADR 037 D12's "do not invent inside this
   ADR" discipline).

## Decision

Fourteen numbered decisions D1-D14. D1-D6 ratify Codex iter-1
D1-D6; D7-D11 promote Claude iter-0 C2/C5/C3/C4/C8 to D-level;
D12 ratifies the ADR 034 fragmenter-reject carry-forward;
D13-D14 record the L3 deferral + ADR 039 sequencing commitment.

### D1 — Coordinator-Owned Storage Admin

Add a narrow `StorageAdmin` trait beside `StorageWrite` and
`StorageDeleteRows`, not inside `Catalog`. Exactly one method:
`drop_table_storage`. Request fields carry the same context
shape as the existing write / delete request types: session,
query, tenant, table identity (id + object id + fingerprint),
profile hint, trace context. No write receipts, durability
override, retry policy, or `ClientWriteId`.

The trait stays a separate-responsibility surface. Future
admin operations (table truncation, descriptor evolution
migration, etc.) get their own trait method only when their
own ADR pins the contract — not pre-emptively.

### D2 — `ExecutionPlan::DropTable` Stays Identity-Free

ADR 034's identity-free `LogicalPlan::DropTable` /
`ExecutionPlan::DropTable` leaf remains correct. The coordinator
resolves table identity (binding, object id, fingerprint)
through the catalog at execution time, immediately before the
storage-admin call. This preserves stale-bound-plan behavior:
old bound plans carrying a stale `TableId` reach storage
adapters and fail there, not at binder time.

ADR 038 does not introduce plan invalidation. The future
stale-bound-plan invalidation ADR owns that lift.

### D3 — Storage-First, Catalog-Second DROP Ordering

For Toron-local storage, the coordinator routing helper for
`DropTable`:

1. `catalog.resolve_table(name).await`;
2. if not found and `if_exists` → return `Ok(Vec::new())`
   without calling admin;
3. `admin.drop_table_storage(DropTableStorageRequest { ... }).await`;
4. `catalog.drop_table(name, false).await`;
5. return `Ok(Vec::new())`.

Failure modes:

- Step 3 fails → catalog still bound; subsequent DROP retry
  re-enters the routing helper. **DROP is naturally
  retryable** if storage admin is the failing step.
- Step 4 fails after step 3 succeeded → catalog still bound
  but storage gone. Subsequent reads of the catalog-bound
  table fail at storage descriptor check
  (`StorageError::TableNotFound`). Surfacing the catalog
  error to the caller acknowledges the asymmetric partial
  failure; v1 contract is non-transactional DDL (ADR 033 D7
  precedent).

The inverse ordering (catalog-first, storage-second) would
leak storage on the catalog-success / storage-failure case —
exactly the data-leak path ADR 038 exists to close.

### D4 — In-Memory Reclaim Drops the Whole Entry

`InMemoryTableStorage::drop_table_storage`:

```rust
async fn drop_table_storage(
    &self,
    request: DropTableStorageRequest,
) -> Result<DropTableStorageOutcome, StorageError> {
    let mut tables = self.tables.lock().expect("InMemoryTableStorage poisoned");
    let data = tables.get(&request.table_id).ok_or(StorageError::TableNotFound {
        table_id: request.table_id,
    })?;
    if data.table_object_id != request.table_object_id {
        return Err(StorageError::DescriptorEvolved { /* ... */ });
    }
    if data.physical_descriptor_fingerprint != request.physical_descriptor_fingerprint {
        return Err(StorageError::DescriptorEvolved { /* ... */ });
    }
    tables.remove(&request.table_id);
    Ok(DropTableStorageOutcome { reclaimed_tables: 1 })
}
```

`InMemoryTableData` removal drops batch, identities,
`deleted_row_key_ids`, and `next_row_key_ordinal` together.
No partial compaction. No per-tombstone reaping. Future
tombstone-compaction ADR (named in roadmap) owns granular
reaping.

### D5 — Shared Wendelon Descriptor Registry

Introduce `WendelonDescriptorRegistry` as the single
authoritative source for Wendelon table descriptors used by:

- `WendelonCatalog`;
- `WendelonStorageScan`;
- `WendelonStorageWrite`;
- `WendelonStorageDeleteRows`.

Interior mutability via `Arc<RwLock<WendelonDescriptorState>>`.
Read paths (storage adapter scan/write/delete request
fingerprint validation) acquire the read lock; admin paths
(catalog create / future drop) acquire the write lock and
mutate.

This is a **correctness invariant**, not cleanup: without it,
`WendelonCatalog::create_table` could succeed (registering
the new table in the catalog's by_ref / by_id maps) while
`WendelonStorageWrite` continues to reject the new `TableId`
as unknown (because its independent descriptor map was not
told). Gemini iter-0 D4 called this "cache invalidation";
this ADR explicitly rejects that framing — there is no
separate cache. Registry mutation under the write lock IS
the catalog refresh.

### D6 — Wendelon CREATE Plan A

`WendelonCatalog::create_table` implementation:

1. Acquire registry write lock; check duplicate-name reject
   conditions (`TableAlreadyExists` for non-`IF NOT EXISTS`
   path; equal-schema short-circuit for `IF NOT EXISTS`).
2. Allocate `TableObjectId` from UUID-derived 128-bit bytes
   (`Uuid::new_v4().into_bytes()`). Do NOT hash the table
   name, schema, or fingerprint. Do NOT add `blake3`.
3. Build `TableSchemaDescriptor` via
   `TableSchemaDescriptor::new_with_layout(...)` with the
   policy choices pinned in D7.
4. Construct `TableSchemaUpdateRequest::create(...)` and
   submit via `Client::submit_table_schema_update(...)`.
5. On Wendelon success: insert the new descriptor + binding
   + name alias into the registry under the write lock;
   release lock; return `TableBinding`.
6. On Wendelon failure (duplicate name, duplicate object id,
   consensus reject): map to `CatalogError::TableAlreadyExists`
   where the Wendelon error preserves the duplicate-name
   signal; otherwise surface as
   `CatalogError::AdminFailed { message }`. Do not reuse
   `AdminUnavailable`; ADR 038 reserves that signal for the
   still-deferred Wendelon DROP not-wired path.

UUID-based allocation is the object-id policy ADR 037 D12
refused to invent. Wendelon consensus remains the authority
for collision rejection. v4 UUID space (122 random bits) makes
collisions cosmologically improbable; consensus's
duplicate-object-id reject is the safety net, not the primary
mechanism.

### D7 — `RowIdentityPolicy::AppendOnly` for SQL-Created Wendelon Tables

ADR 029 SQL `CREATE TABLE` has no PRIMARY KEY declaration.
Wendelon `TableSchemaDescriptor::new_with_layout` requires a
`RowIdentityPolicy`.

**Pin `RowIdentityPolicy::AppendOnly` for all SQL-created
Wendelon tables in ADR 038.**

Rationale:

- It matches `mock_table_identity` at
  `crates/toron/src/catalog.rs:402-415`, the existing policy
  for the SQL CREATE path.
- It does not invent policy inside ADR 038 (mirrors ADR 037
  D12's "do not invent inside this ADR" discipline that bit
  Wendelon CREATE the first time around).
- It composes with ADR 035's row-identity substrate (the
  `RowIdentityProjection::Append` candidate-scan path).

A future PRIMARY KEY constraint ADR (named in roadmap
Constraints carried-forward) widens to
`RowIdentityPolicy::PrimaryKey(...)` once the SQL surface
exists.

### D8 — Strict `StorageAdmin` Sink Default

`QueryCoordinator::new()`, `with_writer()`,
`with_writer_and_deleter()`, and the equivalent
`DistributedQueryCoordinator` constructors install
`UnsupportedStorageAdmin` as the default admin sink.

`UnsupportedStorageAdmin::drop_table_storage` returns the
stable diagnostic:

```
"DROP TABLE physical reclaim requires a configured StorageAdmin"
```

`with_storage_admin()` and combined
`with_writer_deleter_and_admin()` install a real admin sink.

DROP execution does NOT silently skip storage reclaim when no
admin is configured; it fails at the storage capability
boundary. This mirrors ADR 037's `StorageWrite` /
`StorageDeleteRows` capability-boundary pattern.

ADR 034 catalog-only fixtures migrate via an explicit
`NoopStorageAdmin` test helper in
`tests/common/mod.rs` (or sibling location). The catalog-only
DROP semantic is *intentionally* preserved for fixtures
testing alias cleanup in isolation; production constructors
continue to install `UnsupportedStorageAdmin`.

### D9 — Registry Mutation Is the Catalog Refresh

There is no separate cache-invalidation step after admin
mutation. The shared `WendelonDescriptorRegistry` (D5) is the
only authoritative source for catalog bindings and storage-
adapter descriptors. Registry mutation under the write lock
serves both roles atomically.

No `invalidate_table`, `refresh_catalog`, `evict_descriptor`,
or equivalent helper exists. ADR 038 architecture guard 22
source-asserts against introducing one. Future admin
operations follow the same pattern: write-lock-mutate the
registry; release.

### D10 — Stale-Bound-Plan Invariant Preserved

ADR 038 explicitly does NOT invalidate bound plans on DROP.
Bound `TaskSpec` / `ExecutionPlan::Scan` / `Insert` / `Delete`
variants carrying a `TableId` + `TableObjectId` +
`PhysicalDescriptorFingerprint` triple that was dropped fail
at storage-adapter descriptor validation, surfacing
`StorageError::TableNotFound` (after reclaim) or
`StorageError::DescriptorEvolved` (if the new table reused the
slot — which D6 + D8 of Codex iter-1 explicitly prevent).

This is the *intended* failure mode. It preserves the design
space for the future stale-bound-plan invalidation ADR (named
in roadmap substrate candidates). ADR 038 would close that
seam by silently invalidating plans; D10 explicitly keeps it
open.

Architecture guard 23 source-asserts that DROP paths in
`catalog.rs`, `storage.rs`, and `coordinator.rs` reference no
plan-cache or bound-plan invalidation symbol.

### D11 — `Catalog::drop_table` Only Called From Routing Helper

Pin that `Catalog::drop_table` has exactly one production call
site: the coordinator routing helper. Without this guard, a
future feature could call `catalog.drop_table` directly and
skip storage reclaim — silently re-introducing the data-leak
path ADR 038 exists to close.

Architecture guard 24 enforces via source-read: exactly one
`.drop_table(` call site in `crates/toron/src/coordinator.rs`,
inside the routing helper's function span (located by
fn-header proximity).

Test code (fixtures, helpers, architecture tests) is excluded
from the constraint.

### D12 — Fragmenter `DropTableNotFragmentable` Reject Remains

ADR 034's `FragmentError::DropTableNotFragmentable` defensive
reject in `fragment_plan` stays unchanged. Distributed DROP
TABLE continues to route through the coordinator's
`execute_query` before reaching the fragmenter. ADR 038 does
not loosen the defensive reject; the design philosophy is
"raw DDL nodes never reach the fragmenter."

### D13 — L3 Deferred to ADR 039

ADR 038 ships:

- **L1** — Toron-local DROP physical reclaim via
  `StorageAdmin` + `InMemoryTableStorage` impl + coordinator
  routing helper + strict admin sink (D1-D4 + D8-D11).
- **L2** — Wendelon CREATE Plan A via shared registry +
  UUID object IDs + `AppendOnly` policy (D5-D7).

ADR 038 does NOT ship:

- **L3** — Wendelon DROP migration. The Wendelon-side
  blockers identified at iter-0 (consensus reject + client
  envelope reject + missing schema-catalog name-unbind
  primitive) make L3 a substrate cycle of its own. The α-vs-β
  precondition decision (reclaim-completion-token vs
  empty-table-precondition) is rich enough to warrant
  council convergence in ADR 039 rather than compression
  into ADR 038's body.

`WendelonCatalog::drop_table` continues to return
`AdminUnavailable` for Wendelon-known tables verbatim from
ADR 034 D7 / ADR 037 D13.

Architecture guard 13:
`adr038_wendelon_drop_remains_admin_unavailable_until_adr_039`
— source-asserts the unchanged reject body.

This is the **first cycle in council history with an
explicit Plan A / Plan B lift-naming discipline at promotion
time.** ADR 037's lessons-learned: the roadmap claimed Plan
A but the code landed Plan B. ADR 038 pre-empts that drift by
naming L3 as deferred in the promoted ADR, not in a footnote
or in iter-N hedging.

### D14 — ADR 039 Sequencing Commitment

ADR 039 = **Wendelon DROP Migration Substrate**.

ADR 039 lifts:

1. ADR 034 D7 / ADR 037 D13 / ADR 038 D13
   `WendelonCatalog::drop_table` `AdminUnavailable`.
2. Wendelon-side `TableSchemaDropRequestV1` payload design
   (not a widened `TableSchemaUpdateRequest::envelope`).
3. Wendelon consensus `apply_table_schema_update` migration
   precondition — primary decision: α reclaim-completion-
   token vs β empty-table-precondition vs (new) γ candidate
   discovered during ADR 039 iter-0 audit.
4. Wendelon `TableSchemaCatalogV1::unbind_latest_name` (or
   equivalent) primitive.
5. Wendelon `TableLifecycleCatalog::remove` invocation
   sequencing in the drop consensus apply path.
6. Toron-side wiring: `WendelonStorageAdmin` impl that calls
   the new Wendelon DROP client helper; registry mutation
   sequencing after Wendelon success.

ADR 039 inherits the cross-ring substrate cadence (expected
4 iterations) and the iter-0 Wendelon-API discovery budget
pattern from ADR 029 / 030 / 035 / 037.

**Topic-concession-compresses-next-cycle pattern advances
to 7-of-9.** Pattern: substrate-defer ADRs seed the next
cycle (ADR 034 D15 → ADR 035; ADR 035 D15 → ADR 036; ADR 036
D14 → ADR 037; ADR 037 D14 → ADR 038; ADR 038 D14 → ADR 039).

## Reject removal (ADR 015 cycle's twentieth instance)

ADR 038 removes two `Err(CatalogError::AdminUnavailable)`
arms and adds back two narrower rejects:

**Removed:**

1. `WendelonCatalog::create_table` `AdminUnavailable` arm at
   `crates/toron/src/catalog.rs:349-358`.
2. The implicit "storage is silently leaked on DROP" failure
   mode in coordinator `execute_catalog_plan` for `DropTable`
   (not an explicit `Err`, but a silent gap — ADR 038 closes
   the gap by requiring `StorageAdmin` invocation).

**Added (narrower):**

1. `UnsupportedStorageAdmin::drop_table_storage` returns
   `StorageError::Internal { message }` with the strict admin-sink
   diagnostic. Narrower than the removed catalog admin reject
   because it admits DROP for any coordinator configured with
   a real admin sink (`InMemoryTableStorage`, or future
   `WendelonStorageAdmin`).
2. `WendelonCatalog::drop_table` `AdminUnavailable` arm at
   `catalog.rs:360-377` preserved verbatim (ADR 038 D13)
   pending ADR 039 lift.

`CatalogError::AdminFailed { message }` for attempted Wendelon
CREATE admin mutations is not a preserved not-wired reject; it
is the runtime failure mapping for a Plan A call that reached
Wendelon and failed there. Guard 9 keeps the `create_table`
body free of `AdminUnavailable`.

The pattern matches ADR 015's "remove one reject, add a
narrower reject, preserve architectural invariant" cycle
shape. Twentieth instance.

## Architecture Guard Set

Twenty-four ADR-038-specific guards in
`crates/toron/tests/architecture_test.rs` + 1 paired
adjustment to the existing variant-count catch-all.

| # | Guard | Asserts |
|---|-------|---------|
| 1 | `adr038_storage_admin_trait_has_one_method` | `StorageAdmin` trait body contains exactly one `async fn` |
| 2 | `adr038_drop_table_storage_request_shape_is_context_only` | `DropTableStorageRequest` field set matches D1 (8 fields) |
| 3 | `adr038_execution_drop_table_remains_identity_free` | `ExecutionPlan::DropTable` body is `{ name, if_exists }` only |
| 4 | `adr038_coordinator_resolves_drop_identity_at_execution` | Routing helper contains `catalog.resolve_table(...)` before `admin.drop_table_storage(...)` |
| 5 | `adr038_in_memory_storage_admin_removes_table_entry` | `InMemoryTableStorage::drop_table_storage` calls `tables.remove(&request.table_id)` |
| 6 | `adr038_drop_table_storage_first_catalog_second` | Routing helper calls `admin.drop_table_storage` before `catalog.drop_table` |
| 7 | `adr038_fragmenter_drop_table_defensive_reject_remains` | `FragmentError::DropTableNotFragmentable` arm unchanged from ADR 034 |
| 8 | `adr038_wendelon_descriptor_registry_shared_by_all_adapters` | No standalone `HashMap<TableId, TableSchemaDescriptor>` field in Wendelon storage adapters; all read through `WendelonDescriptorRegistry` |
| 9 | `adr038_wendelon_create_table_admin_unavailable_removed` | Zero `AdminUnavailable` hits inside `WendelonCatalog::create_table` body |
| 10 | `adr038_wendelon_create_uses_table_schema_update_request_create` | `WendelonCatalog::create_table` body references `TableSchemaUpdateRequest::create` (or `submit_table_schema_update`) |
| 11 | `adr038_wendelon_create_uses_uuid_object_id_not_fingerprint` | `WendelonCatalog::create_table` body references `Uuid::new_v4` (or `uuid::Uuid::new_v4`) and contains no `blake3`, `xxh3`, or `Sha2` reference |
| 12 | `adr038_drop_route_uses_storage_admin_sink_when_configured` | Routing helper calls `self.admin.drop_table_storage(...)` |
| 13 | `adr038_wendelon_drop_remains_admin_unavailable_until_adr_039` | `WendelonCatalog::drop_table` body contains the unchanged `AdminUnavailable` arm |
| 14 | `adr038_strict_admin_sink_default_is_unsupported_storage_admin` | All `QueryCoordinator::new*` constructors install `UnsupportedStorageAdmin` by default |
| 15 | `adr038_no_background_gc_or_scheduler` | No `tokio::spawn` / `std::thread::spawn` in DROP or registry paths |
| 16 | `adr038_no_create_or_replace_sql_surface` | No `CreateOrReplaceTable` IR variant or `CREATE OR REPLACE` lowering arm |
| 17 | `adr038_no_write_receipt_or_read_your_writes_fields` | `DropTableStorageRequest` / `DropTableStorageOutcome` field set contains no receipt / `client_write_id` / durability / `await_finalization` fields |
| 18 | `adr038_no_blake3_or_name_hash_object_ids` | No `blake3` in `Cargo.toml` or source; no `Sha2`/`Sha3`/`xxh3` in `catalog.rs` |
| 19 | `adr038_no_table_object_id_reuse` | `WendelonDescriptorRegistry` insertion sites generate a fresh UUID per call; no path reads an `object_id` from a prior dropped entry |
| 20 | `adr038_in_memory_admin_validates_object_id_and_fingerprint` | `InMemoryTableStorage::drop_table_storage` body contains both `table_object_id` and `physical_descriptor_fingerprint` comparisons |
| 21 | `adr038_wendelon_create_uses_append_only_row_identity_policy` | `WendelonCatalog::create_table` body references `RowIdentityPolicy::AppendOnly` and not `RowIdentityPolicy::PrimaryKey` or `ByOrdinal` |
| 22 | `adr038_no_separate_catalog_cache_after_admin` | No `invalidate_table` / `refresh_catalog` / `evict_descriptor` helper exists in `catalog.rs` |
| 23 | `adr038_drop_does_not_invalidate_bound_plans` | DROP paths in `catalog.rs`, `storage.rs`, `coordinator.rs` reference no `TaskSpec`, `ExecutionPlan`, `plan_cache`, or `invalidate_plan` symbol |
| 24 | `adr038_catalog_drop_table_only_called_from_routing_helper` | Exactly one `.drop_table(` call site in `crates/toron/src/coordinator.rs` production code, inside the routing helper span |

Paired adjustment:
`adr038_execution_plan_variant_count_unchanged` — the existing
variant-count catch-all asserts `ExecutionPlan` and
`LogicalPlan` variant counts unchanged (no new variant in ADR
038).

## Required functional fixtures

Eight required fixtures in
`crates/toron/tests/drop_table_test.rs` (extending ADR 034's
test file) or a new sibling
`crates/toron/tests/storage_admin_reclaim_test.rs`:

1. **`drop_table_reclaims_in_memory_storage`** — wire
   `QueryCoordinator` with `Arc::new(InMemoryTableStorage::new())`
   as both writer and storage admin; `CREATE TABLE t`;
   `INSERT INTO t VALUES (...)`; `DROP TABLE t`; assert
   subsequent `SELECT * FROM t` returns the standard
   table-not-found path (catalog reject after DROP catalog
   unbind).
2. **`drop_table_if_exists_missing_does_not_call_storage_admin`** —
   wire spy admin; `DROP TABLE IF EXISTS missing`; assert
   admin call count == 0 and result == `Ok(Vec::new())`.
3. **`drop_table_storage_failure_preserves_catalog_reachability`** —
   wire failing admin; `DROP TABLE t`; assert error surfaced;
   catalog `resolve_table` still succeeds; retry of the
   same DROP fails at the admin step again.
4. **`drop_table_stale_scan_returns_table_not_found`** —
   bind a `SELECT * FROM t` plan; execute it once
   successfully; `DROP TABLE t`; re-execute the still-bound
   plan; assert `StorageError::TableNotFound` (D10
   stale-bound-plan invariant).
5. **`distributed_drop_table_routes_storage_admin_before_fragment`** —
   wire spy admin on `DistributedQueryCoordinator`;
   `DROP TABLE t`; assert admin call count == 1 and
   `fragment_plan` not invoked.
6. **`drop_with_no_admin_sink_returns_capability_diagnostic`** —
   build coordinator via `QueryCoordinator::new(catalog)`
   (no admin); execute `DROP TABLE t`; assert
   `Err(StorageError::Internal { message: m })` with
   `m == "DROP TABLE physical reclaim requires a configured
   StorageAdmin"`.
7. **`drop_admin_failure_then_retry_succeeds`** — wire admin
   that fails once then succeeds; `DROP TABLE t`; assert
   first call surfaces error; second `DROP TABLE t` succeeds
   end-to-end (idempotency at the routing-helper layer).
8. **`wendelon_create_uses_append_only_row_identity_policy`** —
   source-read assertion; `WendelonCatalog::create_table`
   body uses `RowIdentityPolicy::AppendOnly` (matches D7 +
   architecture guard 21).

Plan A optional Wendelon fixtures (Phase 6 impl, conditional
on Wendelon client availability):

- `wendelon_create_submits_table_schema_update_request` —
  intercept `Client::submit_table_schema_update` calls;
  assert one call per `CREATE TABLE` execution; assert
  payload contains a real `TableSchemaDescriptor`.
- `wendelon_create_duplicate_name_maps_to_table_already_exists`
  — submit two `CREATE TABLE foo`; second returns
  `CatalogError::TableAlreadyExists`.

## SoA-readiness check

No row-path expression evaluation changes. No new
`AggregateOutputRule` variant. No `AggState` change. The
shared `WendelonDescriptorRegistry` is a control-plane
structure with read-mostly access from storage adapters; no
hot-path dispatch.

`AggState: Copy` invariant preserved. `StableAggregateKernel`
catalog untouched. Centralized dispatch points
(`init_bound_state_for_call`, `finalize_scalar_for_call`)
untouched.

SoA-readiness invariants from ADR 021 preserved.

## Brainstorm-seam touch check

- **001 Cranelift JIT compilation.** Untouched. No expression
  IR changes.
- **002 IR-arena migration.** Untouched. `ExecutionPlan` /
  `LogicalPlan` variant counts unchanged.
- **003 Ring-router asymmetry / `WeakControlRing` coordinator
  API.** Touched — ADR 038 sharpens the
  `DistributedQueryCoordinator` API surface by adding the
  storage-admin sink. The new builder method
  (`with_storage_admin`) preserves the asymmetry pattern (one
  sink per capability, not a coalesced `with_storage_caps`).
- **004 Plan Arena / index-based plan IR.** Untouched. ADR
  035 + ADR 037 added zero walker arms; ADR 038 adds zero
  walker arms (DROP routing is leaf-level, not walker-level).
  Sixth consecutive cycle preserving Plan Arena as named-debt-
  not-yet-pressured.

## What Not To Do

- **Don't put storage handles into `MockCatalog`.** Gemini
  iter-0 D1 retracted; `MockCatalog` is a metadata trait.
  Storage reclaim is a storage-impl responsibility.
- **Don't make `Catalog` a data-plane trait.** New admin
  operations live on `StorageAdmin` (or future siblings),
  not on `Catalog`.
- **Don't add a generic storage-admin command enum.**
  `StorageAdmin` is a separate-method trait; widening
  happens one named method per ADR.
- **Don't introduce `CREATE OR REPLACE TABLE` in ADR 038.**
  Named in roadmap; composes over L1+L2+L3 once they land.
- **Don't add background Toron GC.** Coordinator stays
  synchronous and request-driven.
- **Don't bypass the Wendelon client wrapper.** Toron code
  must not reach into Wendelon raft or quic internals.
  Production code paths consume `wendelon::schema::{pbc,
  *types}`, `wendelon_client::*`,
  `wendelon::network::pool::*`, `wendelon::storage::file_ops::*`
  per the ADR 003 / 004 import allowlist.
- **Don't remove ADR 034 fragmenter defensive rejects.**
  `FragmentError::DropTableNotFragmentable` stays; D12.
- **Don't expose synthetic row identity columns to SQL.**
  ADR 029 D12 + ADR 030 D7 + ADR 035 D11 + ADR 037 D9 carry-
  forward. ADR 038 introduces no SQL surface.
- **Don't add `blake3` or hash-derived object ids in Toron.**
  UUID v4 only; consensus is the collision authority.
- **Don't widen `TableSchemaUpdateRequest::envelope` to
  accept Drop.** The dedicated Wendelon table-drop payload
  is ADR 039 scope; ADR 038 preserves the existing reject.
- **Don't weaken the existing Wendelon consensus Drop
  reject.** `apply_table_schema_update` continues to reject
  table Drop verbatim. ADR 039 adds the migration
  precondition + payload type that lets consensus accept
  Drop conditionally.
- **Don't silently catalog-only-DROP when no admin sink is
  configured.** D8 strict-admin default; failures surface at
  the capability boundary with the stable diagnostic.
- **Don't invalidate bound plans on DROP.** D10 stale-bound-
  plan invariant preserved; future stale-bound-plan ADR
  owns the lift.
- **Don't add a separate cache-invalidation step after admin
  mutation.** D9; registry mutation IS the refresh.
- **Don't call `catalog.drop_table` outside the routing
  helper.** D11; architecture guard 24 enforces.
- **Don't promise L3 and land Plan B silently.** D13 names
  the deferral in the promoted ADR text; the impl plan's
  Phase 9 (Plan A / Plan B capture) verifies the landed
  state.
- **Don't reuse `TableObjectId` after DROP.** Even if ADR 039
  lands DROP, table-object-id reuse stays out of scope per
  ADR 038 D6 + ADR 039 expected D-set (Codex iter-1 D8).
- **Don't introduce `StorageAdmin::create_table_storage`.**
  CREATE creates metadata; storage data appears through the
  existing write path (ADR 030 / 032 / 033). Future ADR may
  add a `prepare_table_storage` if substrate evidence pressures
  it; not preemptive.

## Risk register

| ID | Risk | Mitigation |
|----|------|------------|
| R1 | Wendelon CREATE Plan A `submit_table_schema_update` rejects the new UUID `TableObjectId` for an unanticipated reason (e.g., descriptor field validation gap) | Phase 0 preflight executes a real `Client::submit_table_schema_update` against a Wendelon test fixture before Phase 6 catalog wiring; failure modes mapped to `CatalogError` variants at iter-1 |
| R2 | `WendelonDescriptorRegistry` shared-mutable contention degrades read-mostly storage-adapter throughput | `RwLock` (not `Mutex`); admin paths are infrequent vs scan paths; Phase 5 bench captures no read-path regression |
| R3 | Strict admin sink D8 breaks ADR 034 catalog-only DROP fixtures | `NoopStorageAdmin` test helper migrates ADR 034 fixtures with intentional catalog-only scope; production constructors install `UnsupportedStorageAdmin` (failures are loud, not silent) |
| R4 | D3 partial-failure mode (storage success + catalog failure) leaves stale catalog bindings until manual recovery | Documented as non-transactional v1 contract; ADR 033 D7 + ADR 037 D8 precedent; future transactional DDL ADR owns the lift |
| R5 | `RowIdentityPolicy::AppendOnly` choice (D7) doesn't compose cleanly with a future PRIMARY KEY SQL surface | Future PRIMARY KEY constraint ADR widens `WendelonCatalog::create_table` to derive policy from the parsed PRIMARY KEY constraint; AppendOnly is the well-defined default for the no-PK case |
| R6 | ADR 039 implementation reveals a consensus migration precondition that requires altering ADR 038's registry shape | Registry is `pub(crate)` and `Arc<RwLock<...>>`; field changes are non-breaking; ADR 039 can extend `WendelonDescriptorState` if needed without breaking ADR 038 invariants |
| R7 | UUID v4 collision in `TableObjectId` allocation | Consensus rejection is the safety net; v4 122-bit random space (3.4e36) makes practical collisions cosmologically improbable; no application-level retry path needed |
| R8 | D11 single-call-site guard breaks under legitimate refactor (e.g., splitting the routing helper) | Guard checks call-site count + fn-span proximity, not specific function name; future refactor can rename or split the helper as long as the call site stays inside the helper's span |

## Recorded dissent

- **Codex iter-1 D10 preference for including L3 in-cycle.**
  Retracted at Claude iter-1 / Codex iter-2 ratification:
  α-vs-β consensus migration precondition decision is too
  rich to compress alongside L1+L2. Recorded; ADR 039 is the
  agreed home.

- **Gemini iter-0 D1 inline `MockCatalog` reclaim.**
  Retracted at iter-1 in favor of Codex iter-0's storage-impl
  shape. `MockCatalog` has no storage handle and must not
  get one.

- **Gemini iter-0 D4 separate cache invalidation.**
  Retracted at iter-1 in favor of D9 (registry mutation IS
  the refresh).

No outstanding dissents at promotion.

## Process observations

### Two-iteration convergence (substrate cycles can shrink when one lift defers)

ADR 038 reaches convergence in two iterations — faster than
the established 4-iter cross-ring substrate floor (ADR 029,
030, 035, 037 all converged at iter-3 → iter-4). The cycle
shape that drove the speed:

- iter-0 surfaced the full substrate (Codex iter-0 +
  Claude iter-0 + Gemini iter-0 covered L1 + L2 + L3
  candidates).
- iter-1 named L3 as deferred (Claude C7 / Codex iter-1
  D10 fallback), removing the α-vs-β decision compression
  that would have driven a third iteration.
- iter-2 would have been promotion-drafting only; folded
  into iter-1 closing positions.

**New cycle property (1 data point):** substrate cycles that
defer one of their lifts to the next cycle compress to 2-3
iterations. The underlying 4-iter floor for *full-scope*
cross-ring substrate cycles remains intact (5 data points at
029, 030, 035, 037, 039-pending).

### Eleventh consecutive Codex correctness-clarification cycle

Codex iter-0 named the load-bearing Wendelon-side blockers
(`TableSchemaUpdateRequest::envelope` Drop reject,
`apply_table_schema_update` consensus Drop reject, missing
`TableSchemaCatalogV1` unbind primitive) that drove the L3
deferral decision. Without that iter-0 audit, ADR 038 might
have promised L3 in body language and discovered the
substrate gap during implementation — exactly the ADR 037
Plan A / Plan B drift this ADR exists to avoid.

Pattern: Codex's code-evidence-driven iter-0 reframing has
correctness consequences that compound across cycles. 11
consecutive instances (027 → 037 → 038).

### Gemini iter-1 retraction streak at 11-of-13

Two clean Gemini iter-0 retractions at iter-1 (D1 inline
`MockCatalog` reclaim; D4 separate cache invalidation).
Formal "I retract Iteration 0 position" wording (or
equivalent) at iter-1.

Streak: 11-of-13 (was 10-of-12 at ADR 037 close).

### First explicit Plan A / Plan B lift-naming discipline

ADR 037 promoted with "Plan A landed" language in body and
"Plan B may fallback" in conditional clauses; the
implementation landed Plan B but the promoted ADR did not
flag this clearly enough to update the roadmap. ADR 038
introduces the **three-lift discipline at promotion time**:

- L1 / L2 / L3 are named in the Status section.
- D13 pins which lifts land and which defer.
- D14 commits the next ADR by name for the deferred lift.
- Impl Phase 9 captures the landed-state per lift before
  the implementation artifact is written.

This discipline is now the precedent for future cross-ring
substrate cycles that may defer subsets of their scope.

### Topic-concession-compresses-next-cycle pattern at 7-of-9

ADR 038 D14 → ADR 039 generates the Wendelon DROP migration
substrate seed. Pattern data point: substrate-defer ADRs
seed the next cycle reliably when the defer is named in the
promoted ADR text. 7-of-9.

### Cross-ring substrate-cycle properties at 5 data points

After ADR 038:

1. **Full-scope cross-ring substrate cycles converge in 4
   iterations.** 4 data points (029, 030, 035, 037). ADR
   038 is a partial-scope cycle (L3 deferred) and converges
   in 2 — refinement, not break.
2. **Cross-ring substrate cycles add 0-1 fields to a single
   trait-request struct.** ADR 038: new `StorageAdmin` trait
   with one request struct; consistent with the pattern.
3. **Cross-ring substrate cycles attract Codex iter-0 audit-
   shaped reframings.** ADR 038: Wendelon-side blocker audit
   drove L3 deferral. Continues the pattern.
4. **Cross-ring substrate cycles permit Codex self-
   retraction.** ADR 037: two self-retractions on
   `TaskExecutionContext` shape. ADR 038: zero. Cycle
   property "permits but does not require"; data point at 1
   instance.

## Future work

ADR 038 commits ADR 039 by name (D14). Beyond ADR 039:

- **Stale-bound-plan invalidation ADR.** Substrate candidate
  carried forward from earlier roadmap rounds. Engages once
  composition pressure surfaces concrete cases where stale
  failures are user-confusing (the current contract is
  "stale plans fail at storage descriptor check" — clean
  but not user-friendly).
- **`CREATE OR REPLACE TABLE` composition.** Lower to
  `DROP IF EXISTS x; CREATE TABLE x AS SELECT ...`. Waits
  on ADR 039 reclaim substrate before promotion.
- **Tombstone reaping / per-row compaction.** Future
  substrate ADR; ADR 038 explicitly out of scope (D4 +
  Out-of-scope).
- **`StorageAdmin::truncate_table_storage`** (or sibling).
  Pre-emptive widening rejected; pinned by future evidence-
  driven ADR.
- **PRIMARY KEY / UNIQUE constraint substrate.** Widens
  `RowIdentityPolicy::AppendOnly` (D7) to
  `RowIdentityPolicy::PrimaryKey(...)` once the SQL surface
  exists.
- **Transactional DDL.** Lifts the D3 non-transactional
  partial-failure mode. Substrate ADR with its own decision
  matrix.
- **`UPDATE WHERE` substrate.** Carry-forward from ADR 037;
  needs partial-write contract. Likely a two-ADR sequence
  (partial-write substrate + SQL UPDATE composition).
- **`StorageWrite` / `StorageDeleteRows` streaming /
  multi-batch.** Lifts ADR 032 D5 + D6 + ADR 033 D8 + ADR
  035 single-batch cap. Substrate ADR.

## Definition of Done

Phase 10 verification (impl plan) green; eight Toron
fixtures green; 24 architecture guards green; ADR 029 / 030
/ 031 / 032 / 033 / 034 / 035 / 036 / 037 fixtures byte-
identical pre/post-ADR-038 (fourth byte-identical refactor
invariant); promoted impl artifact captures L1 + L2 + L3
landed-state explicitly (Phase 9); roadmap updated with ADR
038 Status entry + ADR 039 Forward sequence commitment;
`plans/wip/*_iter_*_adr_038.md` /
`plans/wip/*_iter_*_impl_038.md` retained in `plans/wip/`
through implementation; roadmap reflects ADR 038 promotion
and ADR 039 sequencing.
