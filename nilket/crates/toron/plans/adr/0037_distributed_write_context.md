# ADR 037: Distributed Write Context — Cross-Ring Substrate for Distributed DML

## Status

Promoted. Three-way council convergence (Claude /
Codex / Gemini) reached in **four iterations** —
matching the cross-ring substrate-cycle floor
established by ADR 029 + 030 + 035. **First cross-
ring substrate cycle with two Codex self-retractions
on the same field** (`TaskExecutionContext` shape:
iter-1 4-field → iter-2 2-field narrowing → iter-3
4-field correction after deeper call-graph audit).

**Five rejects fully lifted + one partially lifted
+ one explicitly deferred to ADR 038** — highest
reject-lift count in council history:

1. **Lifted**: ADR 029 D6
   `WendelonCatalog::create_table` `AdminUnavailable`
   (Plan A path; Plan B contingency preserves
   `AdminUnavailable` if iter-2 audit reveals
   table-object-id allocation needs policy
   invention).
2. **Lifted**: ADR 030 D13 distributed INSERT
   reject.
3. **Lifted**: ADR 032 D1 distributed INSERT
   SELECT reject.
4. **Lifted**: ADR 033 D11 distributed CTAS
   reject.
5. **Lifted**: ADR 036 D10 distributed DELETE
   reject.
6. **Partially lifted**: ADR 034 D12 distributed
   DROP TABLE — catalog-routing path was already
   lifted by ADR 034 itself; ADR 037 confirms.
   Wendelon-known DROP still rejects with
   `AdminUnavailable` (ADR 034 D7 carry-forward).
7. **Deferred**: ADR 034 D7
   `WendelonCatalog::drop_table` `AdminUnavailable`
   — deferred to ADR 038 storage-admin / reclaim
   substrate per D14 sequencing commitment.

**Tenth consecutive cycle of the Codex correctness-
clarification pattern.** Codex iter-0 reframed the
substrate from "coordinator-local writer" to
"coordinator-owned mutations + distributed
candidate reads" (the load-bearing iter-0 finding).
Codex iter-2 + iter-3 self-retracted twice on
`TaskExecutionContext` shape; the iter-3 audit
through `Executor::execute_bound`'s call graph
caught a substrate failure that the iter-2 type-
shape audit missed.

**Three substantive Claude retractions across the
cycle** (first cycle in council history with
retractions across three iterations):
- iter-0 C2 (coordinator-local writer) retracted
  at iter-1 in favor of Codex iter-0 D1 + D7.
- iter-0 D4 (Wendelon DROP wiring attempt)
  retracted at iter-1 in favor of Codex iter-0
  D10 (Wendelon DROP deferred).
- iter-2 C15 (2-field `TaskExecutionContext`
  endorsement) retracted at iter-3 in favor of
  Codex iter-3 D3 4-field shape after the call-
  graph audit.

**Gemini iter-1/2/3 retraction pattern complex.**
Gemini iter-1 + iter-2 held the 4-field shape
(correct); Gemini iter-3 retracted toward Codex
iter-2's 2-field narrowing (wrong) — **one-beat-
late retraction trap** as Codex iter-2 → iter-3
oscillated. Final Gemini iter-3 D2 is a wording
correction at promotion drafting.

**Highest reject-lift count in council history**
(5 + 1 partial + 1 deferred). Five composition
cycles (030 / 032 / 033 / 034 / 036) deferred
substrate work across 6 cycles, accumulating into
this single substrate lift cycle.

Nineteenth instance of the ADR 015 cycle (after
016 / 017 / 018 / 019 / 022 / 023 / 024 / 025 /
026 / 027 / 028 / 029 / 030 / 031 / 032 / 033 /
034 / 035 / 036).

## Scale boundary

**Primary:** Toron `DistributedQueryCoordinator` +
`TaskSpec` + `ExecutionPlan::Scan` + local /
distributed coordinator write paths +
`WendelonCatalog::create_table` (Plan A/B). The
substrate is mostly Toron-side composition over
existing Wendelon RPCs; the only Wendelon-side
discovery is `submit_table_schema_update` (already
wired in `wendelon-client` per ADR 037 iter-0
audit).

In scope:

- **`DistributedQueryCoordinator` becomes context-
  bearing**: new explicit execution context
  argument (or `with_context` constructor) so the
  distributed coordinator no longer relies on
  `local_test()` defaults for production
  execution.
- **`TaskExecutionContext` substrate**: new
  serializable type in
  `crates/toron/src/distributed/context.rs`
  carrying scan-context fields:

  ```rust
  pub struct TaskExecutionContext {
      pub session_id: SessionId,
      pub tenant_id: TenantId,
      pub read_bound: HlcBound,
      pub trace_context: Option<storage::TraceContext>,
  }
  ```

  `query_id` derived from `TaskSpec.identity.query_id`
  (not duplicated); `profile_hint` from
  `ExecutionPlan::Scan` (already there);
  `runtime_filter` from `TaskInputBinding::Scan`
  (already there). The task context MUST NOT
  carry write receipts, client write IDs,
  durability policy, finalization policy, retry
  counters, row identity policy, runtime filters,
  table pins, task telemetry, or join spill
  configuration (Codex iter-3 D3 + Claude C23).
- **`TaskSpec` extended** with `context:
  TaskExecutionContext`.
- **`FragmentPlanConfig` extended** so every
  emitted task gets stamped with the context.
- **Production worker execution** constructs
  `ExecutionContext` via `ExecutionContext::new`
  and the existing `with_*` setters from
  `spec.identity.query_id` +
  `spec.context.{session_id, tenant_id, read_bound,
  trace_context}` + `spec.budget` +
  `scan_runtime_filter(spec)` + task telemetry,
  **NOT from `ExecutionContext::local_test()`**.
- **`ExecutionPlan::Scan` gains `row_identity:
  RowIdentityProjection`** field. Normal scan
  constructors default `None`. Internal DELETE
  candidate plan builder may set `Append`. SQL
  binder / projection paths must not allow user
  construction of synthetic row-key names
  (ADR 029 D12 + ADR 030 D7 + ADR 035 D11 carry-
  forward).
- **Shared `MutationSourceExecutor` async trait
  seam** in coordinator path; local coordinator
  wraps `Executor::execute_bound`; distributed
  coordinator wraps fragment + schedule + collect
  for read-only source plans.
- **Shared private mutation driver** invoked by
  both local and distributed coordinators for:
  - INSERT VALUES literal materialization;
  - INSERT SELECT source execution + write;
  - CTAS source execution + catalog create +
    write;
  - DELETE candidate extraction + identity
    extraction + delete_rows;
  - non-null validation + empty-batch handling.
- **Distributed coordinator gains optional write
  sinks**: `with_writer` + `with_writer_and_deleter`
  constructors mirroring local coordinator
  pattern from ADR 030 / 035 / 036.
- **Distributed INSERT VALUES**: shared driver,
  no fragmentation.
- **Distributed INSERT SELECT / CTAS**: shared
  driver with distributed source executor;
  fragmenter fragments only the SELECT source
  plan. CTAS ordering preserved (source read,
  catalog create, row write).
- **Distributed DELETE**: shared driver with
  distributed candidate-read source executor;
  internal candidate plan is `Scan { row_identity:
  Append }` optionally wrapped in
  `ExecutionPlan::Filter` for residual predicates;
  `runtime_filter: None` enforced; coordinator
  extracts identities and calls `delete_rows`
  once.
- **Old distributed DML reject strings removed
  from `DistributedQueryCoordinator::execute_query`**:
  the four lifted rejects (INSERT / INSERT SELECT
  / CTAS / DELETE) no longer fire at the
  coordinator dispatch level. Unsupported sinks
  fail at the storage capability boundary
  instead.
- **Fragmenter defensive rejects stay**: raw
  `Insert` / `InsertSelect` / `CreateTableAsSelect`
  / `Delete` / `CreateTable` / `DropTable` still
  rejected by `fragment_plan` so accidental
  routing fails fast.
- **`WendelonCatalog::create_table` Plan A**:
  build `TableSchemaDescriptor` via
  `TableSchemaDescriptor::new_with_layout`;
  submit via `Client::submit_table_schema_update`
  Create mutation; update local descriptor maps
  only after admin success.
- **`WendelonCatalog::create_table` Plan B**
  (R3 fallback): if iter-2 audit reveals object-
  id allocation requires policy invention, leave
  `AdminUnavailable` preserved verbatim; record
  Wendelon CREATE alongside DROP in ADR 038
  storage-admin scope. The implementation PR
  picks A or B based on Phase 0 preflight finding.
- **Wendelon DROP stays `AdminUnavailable`** for
  known tables (ADR 034 D7 carry-forward).
  Deferred to ADR 038.
- **25 ADR-037-specific architecture guards** in
  `crates/toron/tests/architecture_test.rs`.
- **9-10 required functional fixtures** + 2-3
  optional Plan A Wendelon fixtures.

Out of scope:

- Worker-side `StorageWrite::write` /
  `StorageDeleteRows::delete_rows` execution.
  Mutations stay coordinator-owned.
- Multi-worker write fanout (e.g., parallel
  INSERT SELECT source-side shuffling, sharded
  INSERT VALUES). Future ADR.
- Atomic CTAS / DROP. Non-atomicity preserved
  per ADR 033 D7 + ADR 034 D9.
- Mutation retry after partial success.
- Idempotency via Toron-side `ClientWriteId`.
  Wendelon's existing `Client::table_write` /
  `table_delete` handle idempotency internally
  through client UUID + sequence number.
- Write receipts surfacing through Toron
  `WriteOutcome` / `DeleteRowsOutcome`.
- Read-your-writes / HLC advancement from write
  receipts. Future read-your-writes ADR engages.
- Per-statement durability / client-HLC overrides.
- Wendelon `DROP TABLE` admin wiring + physical
  storage reclaim + descriptor-evolution
  migration. **Deferred to ADR 038 storage-admin
  / reclaim substrate per D14.**
- `UPDATE WHERE`. Needs partial-write substrate
  first.
- `RETURNING` clause. Future RETURNING substrate
  ADR.
- Subqueries in DELETE WHERE clause. Future
  correlated-subquery substrate ADR.
- Plan Arena. `ExecutionPlan::Scan` gains one
  field (additive); no boxed-child variants.
  Fifth consecutive cycle preserving Plan Arena
  as named-debt-not-yet-pressured (after ADR 032
  / 033 / 034 + extending to ADR 035 / 036).
- New SQL surface. ADR 037 is pure substrate.
- New `StorageWrite` / `StorageDeleteRows` trait
  method.
- New `StorageError` variant.
- `blake3` / external allocator crate.

## Context

After ADR 036 promotion, the local CRUD substrate
is complete and ADR 036 D14 firmly committed ADR
037 to lift the accumulated distributed-write
rejects. The deferral history:

| Cycle | Reject deferred |
|---|---|
| ADR 030 | distributed INSERT (D13) |
| ADR 032 | distributed INSERT SELECT (D1) |
| ADR 033 | distributed CTAS (D11) |
| ADR 034 | distributed DROP TABLE (D12); Wendelon DROP `AdminUnavailable` (D7) |
| ADR 036 | distributed DELETE (D10) |

Five cycles of substrate deferral consolidated
into ADR 037's reject-lift count.

**Key code-evidence findings across iter-0/1/2/3:**

1. **Wendelon-side surface is light.**
   `wendelon_client::Client::submit_table_schema_update`
   already exists at `wendelon-client/src/lib.rs:454`
   with full control-ring leader-refresh + retry
   budget + `SchemaUpdateReceipt` shape. Toron-
   side admin wiring is mostly composition over
   existing Wendelon RPCs.

2. **Wendelon DROP TABLE is rejected by consensus
   state machine, not just by the client wrapper.**
   The diagnostic `"table schema drops require an
   explicit data-rewrite migration"` comes from
   Wendelon's consensus layer. Bundling DROP into
   ADR 037 would widen scope into storage-admin /
   reclaim work; deferred to ADR 038.

3. **`Executor::execute_bound(ExecutionPlan::Scan)`
   builds `ScanRequest` from `ExecutionContext`.**
   This is the load-bearing iter-3 finding: if
   workers receive only `query_id` + `profile_hint`,
   they still execute storage scans with
   `ExecutionContext::local_test()` defaults for
   `session_id` / `tenant_id` / `read_bound`. The
   task context must carry these fields.

4. **Workers must remain read/shuffle executors.**
   Coupling task retry to write retry would create
   an idempotency problem the storage layer
   doesn't yet solve. Coordinator-owned mutations
   preserve ADR 030 / 032 / 033 / 036 "bind once,
   mutate once" invariants.

5. **DELETE candidate scans benefit from
   distribution.** A coordinator-local approach
   (Claude iter-0 C2) would leave the expensive
   candidate scan un-distributed. Codex iter-0 D7
   route — extend `ExecutionPlan::Scan` with
   `row_identity`, distribute candidate scans via
   the existing fragmenter / worker machinery,
   extract identities at the coordinator — gets
   the distribution benefit without violating
   coordinator-owned mutations.

The council converged in four iterations with one
substrate flip-flop (Codex iter-1 → iter-2 → iter-3
on D2 shape) and three substantive Claude
retractions (C2 iter-0, D4 iter-0, C15 iter-2).
Iter-3 audit settled the final substrate shape.

## Decision

### D1 — Coordinator-Owned Mutations

Distributed writes remain coordinator-owned.
Remote workers may execute read-only source /
candidate fragments, but only the distributed
coordinator calls:

- `Catalog::create_table`;
- `StorageWrite::write`;
- `StorageDeleteRows::delete_rows`.

This preserves the ADR 030 / 032 / 033 / 036 "bind
once, mutate once" invariant. Task retry never
becomes write retry; mutation idempotency stays
inside Wendelon's existing `Client::table_write` /
`table_delete` paths (client UUID + sequence
number).

### D2 — Distributed Coordinator Becomes Context-Bearing

`DistributedQueryCoordinator` gains an explicit
execution context argument (or `with_context`
constructor; impl plan picks the cleanest shape).
Production construction passes real context; test
/ read-only construction may use an explicit test
helper.

The context provides the fields required for write
/ delete requests and for the remote scan context:

- `session_id`;
- `query_id`;
- `tenant_id`;
- `read_bound`;
- optional `profile_hint`;
- optional storage `trace_context`.

The coordinator uses one query id consistently for
fragment plan config, task identities, write
requests, and delete requests.

### D3 — `TaskExecutionContext` Substrate

Add `TaskExecutionContext` in
`crates/toron/src/distributed/context.rs` and
include it in every `TaskSpec`:

```rust
pub struct TaskExecutionContext {
    pub session_id: SessionId,
    pub tenant_id: TenantId,
    pub read_bound: HlcBound,
    pub trace_context: Option<storage::TraceContext>,
}
```

`query_id` for a task comes from
`TaskSpec.identity.query_id` (not duplicated; an
architecture guard asserts the worker's
`ExecutionContext.query_id` equals
`spec.identity.query_id`).

`profile_hint` for a scan comes from
`ExecutionPlan::Scan` (already there).

`runtime_filter` for a scan comes from
`TaskInputBinding::Scan` (already there).

**The task context MUST NOT carry**:

- write receipts;
- client write IDs;
- durability policy;
- finalization policy;
- retry counters;
- row identity policy;
- runtime filters;
- table pins;
- task telemetry;
- join spill configuration.

Architecture guard #7 enforces this negative
invariant.

Worker construction of `ExecutionContext` uses the
public constructor and setters because the struct
has private spill fields:

```rust
let mut context = ExecutionContext::new(
    spec.context.session_id,
    spec.identity.query_id,
    spec.context.tenant_id,
    spec.context.read_bound,
)
.with_task_budget(spec.budget)
.with_task_telemetry(telemetry.clone())
.with_runtime_filter(scan_runtime_filter(spec));
context.trace_context = spec.context.trace_context.clone();
context.profile_hint = scan_profile_hint(spec);
```

Production worker execution NO LONGER calls
`ExecutionContext::local_test()`. The
`local_test()` helper may stay for test-only
construction behind a `#[cfg(test)]` gate or
similar discipline, but production code paths
must use the task-context-derived construction.

### D4 — No Storage Request Shape Change

Keep `WriteRequest` and `DeleteRowsRequest` byte-
identical pre/post-ADR-037. Do not add:

- `client_write_id`;
- `durability`;
- `await_finalization`;
- public receipt state.

Existing fields suffice: session, query, tenant,
table identity, descriptor fingerprint, payload /
row_key_ids, profile hint, trace.

Wendelon's `Client::table_write` /
`table_delete` own `ClientWriteId` generation
internally (client UUID + sequence number) and
already pass `await_finalization: true` by default
through the existing adapter discipline (ADR 030
+ ADR 035).

### D5 — Distributed Coordinator Gains Explicit Write Sinks

Mirror local coordinator constructor pattern from
ADR 030 / 035 / 036:

- `DistributedQueryCoordinator::new(...)` keeps
  read-only behavior with `UnsupportedStorageWrite`
  / `UnsupportedStorageDeleteRows` adapters
  (or `None` sinks); construction succeeds; DML
  fails at mutation boundary with capability
  error.
- `DistributedQueryCoordinator::with_writer(...)`
  installs a real `StorageWrite`.
- `DistributedQueryCoordinator::with_writer_and_deleter(...)`
  installs both `StorageWrite` and
  `StorageDeleteRows`.

Unsupported sink diagnostics reuse local
coordinator strings:

- `"INSERT execution requires a configured StorageWrite"`;
- `"DELETE execution requires a configured StorageDeleteRows"`.

Read-only deployments remain explicit.

### D6 — Shared Mutation Driver via `MutationSourceExecutor`

Refactor local `QueryCoordinator::execute_write_plan`
into a private shared driver invoked by both
local and distributed coordinators. The shared
driver handles:

- INSERT literal batching;
- INSERT SELECT source execution + write;
- CTAS source execution + catalog create + write;
- DELETE candidate extraction + identity
  extraction + delete_rows;
- non-null validation + target-column reorder +
  empty-batch handling.

The only intended variation between local and
distributed paths is the **source-read executor**:

```rust
#[async_trait::async_trait]
trait MutationSourceExecutor {
    async fn execute_read_plan(
        &self,
        plan: &ExecutionPlan,
    ) -> Result<Vec<RecordBatch>, QueryError>;
}
```

- **Local source executor** wraps
  `Executor::execute_bound` directly.
- **Distributed source executor** wraps fragment
  + schedule + collect for read-only source plans.

Use a private async trait seam (not a generic
closure parameter). The async trait approach
preserves `Send + Sync` cleanly via
`#[async_trait]` and avoids the `for<'a>` lifetime
noise of closure-based source executors.

**Refactor invariant**: the local source-executor
path must produce byte-identical behavior pre /
post-refactor for:

- empty INSERT SELECT `Ok(Vec::new())` zero-row
  contract;
- CTAS create-before-source ordering;
- CTAS empty-source write of an empty batch via
  `materialize_empty_record_batch`;
- DELETE residual filter + identity extraction;
- DELETE result batch (`deleted: Int64`).

Reference fixture pass-count check is mandatory:
all existing ADR 030 / 032 / 033 / 036 fixtures
must remain green with identical pass counts
pre / post-refactor.

This is the third byte-identical refactor
invariant in council history (after ADR 035 D5
typed delete trait + ADR 036 D6
`filter_batches_with_bound_predicate` helper
extraction).

### D7 — INSERT VALUES Does Not Fragment

Distributed `ExecutionPlan::Insert` has no source
read. The distributed coordinator materializes the
payload locally and calls the shared mutation
driver with a trivial source executor (or
bypasses the source executor entirely). It MUST
NOT call `fragment_plan`.

This is the simplest lift of ADR 030 D13.

### D8 — INSERT SELECT and CTAS Fragment Only Source Reads

Distributed `ExecutionPlan::InsertSelect` runs its
source plan through the distributed
`MutationSourceExecutor::execute_read_plan`
(fragment + schedule + collect), then the shared
mutation driver performs one coordinator-side
`StorageWrite::write`.

Distributed `ExecutionPlan::CreateTableAsSelect`
preserves ADR 033 local ordering:

1. source read (distributed via
   `MutationSourceExecutor::execute_read_plan`);
2. catalog create (`Catalog::create_table`,
   coordinator-side; Wendelon Plan A or local
   `MockCatalog`);
3. materialize payload (shared driver, including
   ADR 033 `materialize_empty_record_batch` for
   empty source);
4. coordinator-side `StorageWrite::write`.

If source execution or write fails after catalog
creation, preserve existing local non-atomic CTAS
behavior (ADR 033 D7 carry-forward; manual cleanup
via DROP per ADR 034). **Do not sell ADR 037 as
transactional CTAS.**

### D9 — DELETE Uses Internal Row Identity Candidate Reads

`ExecutionPlan::Scan` gains a `row_identity:
RowIdentityProjection` field.

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

Normal scan constructors default to
`RowIdentityProjection::None`. The internal DELETE
candidate plan builder may set
`RowIdentityProjection::Append`. The output
schema of `Scan` with `Append` includes the
internal `__row_key_id__` column appended after
the user-schema columns.

DELETE candidate planning shape:

- **Pushdown-only predicate**: candidate plan is
  `Scan { row_identity: Append, pbc_program:
  Some(pushed), ... }`; candidate execution
  supplies no runtime filter (D10).
- **Residual predicate**: candidate plan is
  `Filter { predicate, input: Box<Scan { row_identity:
  Append, pbc_program: None, ... }> }`. The
  trailing `__row_key_id__` column
  survives `Filter` because
  `filter_batches_with_bound_predicate` (ADR 036
  D6) preserves all input columns.
- **No predicate**: candidate plan is `Scan {
  row_identity: Append, pbc_program: None, ... }`.

Runtime filters are deliberately not modeled as an
`ExecutionPlan::Scan` field. Runtime-filter
exclusion for DELETE candidate reads is pinned by
D10 at the candidate execution / scan-input
construction boundary.

After distributed execution returns candidate
batches to the coordinator, the shared mutation
driver extracts row identities via the existing
ADR 035 / 036 helper
`extract_delete_row_key_ids` and calls
`StorageDeleteRows::delete_rows` once.

**SQL invisibility of identity column** carry-
forward (ADR 029 D12 + ADR 030 D7 + ADR 035 D11
+ ADR 036 guard #22):

- SQL planner / binder synthetic-name rejects
  remain.
- Normal `bind_scan` emits
  `RowIdentityProjection::None`.
- Only the internal DELETE candidate plan builder
  emits `RowIdentityProjection::Append`.
- `ExecutionPlan::schema()` only appends the
  identity field when `row_identity == Append`.

### D10 — Runtime Filters Stay Read-Only

DELETE candidate scan execution MUST set or
preserve `runtime_filter: None` at the scan-input
/ `ScanRequest` construction boundary and MUST
NOT inherit ambient runtime filters from the
coordinator context, task context, or worker
context.

Positive guard: literal `runtime_filter: None` in
the candidate execution path, generated scan input,
or `ScanRequest` construction. It is not an
`ExecutionPlan::Scan` field.

Negative guards (Claude C11 + A3 carry-forward):

- no `runtime_filter: self.context.runtime_filter`
  pattern;
- no `runtime_filter: context.runtime_filter`
  pattern;
- no `runtime_filter: spec.context.runtime_filter`
  pattern (NEW for task context).

Runtime filters are read accelerators for join
performance only; using them to narrow mutation
candidates would risk silent data loss.

### D11 — Fragmenter Rejects Stay Defensive

`fragment_plan` continues to reject raw `Insert`
/ `InsertSelect` / `CreateTableAsSelect` /
`Delete` / `CreateTable` / `DropTable` plans
with the existing `FragmentError::*NotFragmentable`
variants. ADR 037 routes DML around the
fragmenter; only derived read-only source /
candidate plans reach the fragmenter.

This preserves ADR 030 / 032 / 033 / 034 / 036
defensive fragmenter discipline.

### D12 — Wendelon CREATE Plan A / Plan B

Wendelon `WendelonCatalog::create_table` lands in
ADR 037 only if the implementation can use the
existing Wendelon schema-update path and a
defensible table-object-id allocation path.

**Plan A (preferred)**:

1. Keep `WendelonCatalog::from_descriptors`
   read-only.
2. Add admin-capable construction or interior
   mutability for descriptor maps.
3. Build real descriptors through
   `TableSchemaDescriptor::new_with_layout`.
4. Submit schema updates through
   `Client::submit_table_schema_update` Create
   mutation.
5. Insert the descriptor into local maps only
   after admin call succeeds.
6. Map `ClientError::SchemaUpdateRejected` into
   appropriate `CatalogError` variants.
7. Handle `if_not_exists`: pre-flight resolve
   or map Wendelon's `TABLE_ALREADY_EXISTS`
   reject.

**Plan B (R5 fallback)**: if Phase 0 preflight
audit reveals no defensible table-object-id
allocation API or policy, leave
`WendelonCatalog::create_table` returning
`CatalogError::AdminUnavailable`. Distributed
CTAS still works against `MockCatalog`; Wendelon-
backed CTAS still rejects. Record Wendelon
CREATE alongside DROP in ADR 038 storage-admin
scope.

Hard preflight discipline: **do not invent table-
object-id allocation policy inside ADR 037**.

### D13 — Wendelon DROP TABLE Remains Deferred

`WendelonCatalog::drop_table` for known tables
continues to return `CatalogError::AdminUnavailable`
(ADR 034 D7 carry-forward). Wendelon's consensus
state machine rejects `SchemaMutationKindV1::Drop`
by design; lifting it requires storage-admin work
(data-rewrite migration, physical reclaim,
descriptor refresh) that belongs in ADR 038.

Distributed DROP routing through
`execute_catalog_plan` (already wired by ADR 034)
remains functional for `MockCatalog`-backed
distributed DROP execution.

### D14 — ADR 038 Sequencing Commitment

ADR 038 = **Storage-Admin / Reclaim Substrate**.

Lifts:

- Wendelon `WendelonCatalog::drop_table`
  `AdminUnavailable` (ADR 034 D7 / ADR 037 D13
  deferral);
- Wendelon CREATE Plan B fallback if ADR 037
  takes that path;
- Physical storage reclaim (ADR 034 D9 + ADR 035
  D9 + ADR 036 D8 tombstone collection carry-
  forward);
- Catalog refresh after admin mutation;
- Data-rewrite migration for Wendelon DROP
  (consensus-required precondition).

Read-your-writes and partial-write recovery
substrate may follow ADR 038 depending on
post-ADR-037 evidence.

ADR 039+ then becomes a composition candidate:
- `CREATE OR REPLACE TABLE` (small composition
  over ADR 029 / 033 / 034 / 037 / 038 once
  Wendelon DROP works);
- UPDATE WHERE (composition after partial-write
  substrate, which may overlap with ADR 038);
- DELETE / INSERT / CTAS RETURNING (after
  RETURNING substrate ADR).

This restores the topic-concession-compresses-
next-cycle pattern at **6-of-8** — substrate-
defer ADRs seed the next cycle (ADR 035 D15 →
036 → ADR 036 D14 → 037 → ADR 037 D14 → 038).

## Reject removal (ADR 015 cycle's nineteenth instance)

ADR 037 is the largest reject-lift cycle in
council history. Each removed reject preserves
ADR 015 narrow-detector + named-reject discipline:

**Lifted (distributed coordinator)**:

- ADR 030 D13 distributed INSERT reject string —
  removed.
- ADR 032 D1 distributed INSERT SELECT reject
  string — removed.
- ADR 033 D11 distributed CTAS reject string —
  removed.
- ADR 036 D10 distributed DELETE reject string —
  removed.

**Lifted (catalog admin)**:

- ADR 029 D6 `WendelonCatalog::create_table`
  `AdminUnavailable` — lifted via Plan A
  (`Client::submit_table_schema_update`) OR
  preserved verbatim via Plan B (deferred to
  ADR 038).

**Preserved as defensive**:

- All `FragmentError::*NotFragmentable` variants
  stay; raw DML / catalog mutation plans still
  reject at the fragmenter level. Coordinator-
  level rejects removed; fragmenter-level rejects
  defensive only.

**Preserved as carry-forward**:

- ADR 034 D7 `WendelonCatalog::drop_table`
  `AdminUnavailable` — preserved verbatim;
  deferred to ADR 038.

ADR 035 + 036 substrate types / trait shapes
unchanged; substrate carry-forward preserved.

## Architecture Guard Set

**25 ADR-037-specific guards** in
`crates/toron/tests/architecture_test.rs`.

| # | Guard | Pins |
|---|---|---|
| 1 | `adr037_distributed_coordinator_is_context_bearing` | `DistributedQueryCoordinator` has explicit context constructor / argument; production construction uses real context |
| 2 | `adr037_distributed_write_requests_use_real_context` | distributed write/delete requests source session/query/tenant/profile/trace from coordinator context, not from `local_test()` defaults |
| 3 | `adr037_task_spec_carries_task_execution_context` | `TaskSpec` body contains `context: TaskExecutionContext` field |
| 4 | `adr037_worker_no_longer_uses_local_test_context` | production worker execution paths do not call `ExecutionContext::local_test()`; test-only construction may keep it |
| 5 | `adr037_worker_context_uses_task_identity_query_id` | worker `ExecutionContext.query_id` is derived from `spec.identity.query_id`; no duplicated query id field on task context |
| 6 | `adr037_worker_context_uses_task_context_session_tenant_read_bound` | worker `ExecutionContext.{session_id, tenant_id, read_bound}` derived from `spec.context.{...}` |
| 7 | `adr037_task_context_field_set_is_minimal` | `TaskExecutionContext` body contains only 4 fields (`session_id`, `tenant_id`, `read_bound`, `trace_context`); negative pattern against receipt / client-write / durability / finalization / retry / runtime-filter / row-identity / table-pin / telemetry / spill literals |
| 8 | `adr037_task_context_runtime_filter_via_task_input_binding` | task execution sources `runtime_filter` from `TaskInputBinding::Scan`, not from `TaskExecutionContext` |
| 9 | `adr037_storage_request_shapes_unchanged` | `WriteRequest` + `DeleteRowsRequest` field counts unchanged from ADR 035; no `client_write_id` / `durability` / `await_finalization` literals added |
| 10 | `adr037_distributed_coordinator_has_explicit_write_sinks` | `DistributedQueryCoordinator` body contains writer/deleter fields or `with_writer` / `with_writer_and_deleter` constructors |
| 11 | `adr037_distributed_dml_reject_strings_absent` | old reject strings for distributed INSERT / INSERT SELECT / CTAS / DELETE not present in `DistributedQueryCoordinator::execute_query` body |
| 12 | `adr037_insert_values_does_not_fragment` | distributed INSERT path body does not call `fragment_plan` |
| 13 | `adr037_insert_select_fragments_source_only` | distributed INSERT SELECT path fragments only the SELECT source plan |
| 14 | `adr037_ctas_fragments_source_only_preserves_create_before_source` | distributed CTAS path fragments only source; catalog create precedes write |
| 15 | `adr037_delete_fragments_derived_candidate_only` | distributed DELETE path fragments only derived read-only candidate plan |
| 16 | `adr037_fragmenter_dml_defensive_rejects_remain` | `FragmentError::*NotFragmentable` variants preserved for all DML / catalog mutation plans |
| 17 | `adr037_execution_scan_has_row_identity_field` | `ExecutionPlan::Scan` body contains `row_identity: RowIdentityProjection` field |
| 18 | `adr037_normal_scan_constructors_default_to_none` | non-DELETE-candidate scan constructions set `row_identity: RowIdentityProjection::None` |
| 19 | `adr037_delete_candidate_only_sets_append` | only DELETE candidate plan builder constructs `Scan { row_identity: RowIdentityProjection::Append }` in production source |
| 20 | `adr037_sql_synthetic_row_key_invisible` | planner / binder synthetic-name rejects for `__row_key_id__` / `__row_hlc__` remain green (carry-forward from ADR 035 / 036) |
| 21 | `adr037_delete_candidate_scan_no_runtime_filter` | positive: `runtime_filter: None` in candidate execution / scan-input construction, not as an `ExecutionPlan::Scan` field; negative: no `self.context.runtime_filter` / `context.runtime_filter` / `spec.context.runtime_filter` inheritance |
| 22 | `adr037_local_and_distributed_call_shared_mutation_driver` | both local `QueryCoordinator::execute_write_plan` and distributed coordinator's write path call the same shared driver (no copy-pasted bodies) |
| 23 | `adr037_mutation_source_executor_is_async_trait_seam` | `MutationSourceExecutor` trait exists with `async fn execute_read_plan` signature; no closure-based source executor parameter |
| 24 | `adr037_shared_write_driver_preserves_local_byte_identical` | local source-executor path produces byte-identical behavior; reference fixture pass counts match pre/post-refactor (verified at impl Phase 4) |
| 25 | `adr037_wendelon_drop_still_unavailable` | `WendelonCatalog::drop_table` for known table returns `AdminUnavailable`; Plan A guard: if Wendelon CREATE lands, uses `TableSchemaDescriptor::new_with_layout` and not ad-hoc fingerprints; Plan B guard: `WendelonCatalog::create_table` preserves `AdminUnavailable` verbatim |

Plus paired adjustment to existing variant-count
catch-all guard if `ExecutionPlan::Scan` field
addition affects the variant-body source-check.

## Required functional fixtures

**9 fixtures required for promotion + 2-3
optional Plan A Wendelon fixtures.**

Required (in new
`crates/toron/tests/distributed_write_context_test.rs`
or extension of existing distributed test file):

1. `distributed_insert_values` — `INSERT INTO t
   VALUES (...)` via `DistributedQueryCoordinator`;
   subsequent SELECT shows the inserted row.
2. `distributed_insert_select_with_projection_reorder`
   — `INSERT INTO t (col_b, col_a) SELECT b, a
   FROM source` via distributed coordinator;
   subsequent SELECT shows reordered values.
3. `distributed_ctas_from_aggregate_source` —
   `CREATE TABLE t AS SELECT col, SUM(val) FROM
   source GROUP BY col`; subsequent SELECT shows
   aggregated rows.
4. `distributed_ctas_from_join_source` — CTAS
   over a JOIN; coordinator fragments the JOIN
   source, then writes once.
5. `distributed_delete_with_pushdown_predicate` —
   `DELETE FROM t WHERE id > 2`; deleted count
   matches; subsequent SELECT shows remaining
   rows.
6. `distributed_delete_with_residual_predicate` —
   `DELETE FROM t WHERE amount + 1 > 40` (non-PBC
   arithmetic); deleted count matches only the
   filter result. Critical R1 correctness fixture
   carry-forward from ADR 036.
7. `distributed_delete_ignores_ambient_runtime_filter`
   — coordinator context has a runtime filter set
   (from prior distributed JOIN); distributed
   DELETE candidate scan does NOT inherit it;
   delete count matches predicate-only.
8. `worker_scan_request_uses_real_context` —
   distributed SELECT execution observes real
   `session_id` / `tenant_id` / `read_bound`
   from `TaskExecutionContext`, not `local_test()`
   defaults.
9. `read_only_distributed_coordinator_rejects_dml_at_storage_boundary`
   — `DistributedQueryCoordinator` without
   writer/deleter sinks; INSERT/DELETE/CTAS
   return storage-capability errors (not the old
   coordinator-level reject strings).

Optional Plan A (if Wendelon CREATE lands):

10. `wendelon_catalog_admin_create_table_round_trip`
    — `WendelonCatalog::create_table` succeeds
    via `submit_table_schema_update`; subsequent
    `resolve_table` returns the new binding.
11. `wendelon_catalog_admin_create_table_if_not_exists_idempotent`
    — duplicate create with `if_not_exists`
    returns existing binding.
12. `wendelon_catalog_admin_create_table_rejection`
    — Wendelon rejects schema update; Toron
    surfaces `CatalogError::SchemaUpdateRejected`
    (not `AdminUnavailable`).

## SoA-readiness check

ADR 021's four SoA-readiness invariants preserved:

1. **Catalog stays pure metadata for kernels.**
   ADR 037 adds Wendelon CREATE wiring but no
   kernel additions.
2. **`#[repr(u8)]` on `StableAggregateKernel`
   unchanged.** No aggregate kernel touched.
3. **Centralized dispatch unchanged.** Shared
   mutation driver is a coordinator-internal
   helper; no kernel dispatch sites added.
4. **No row-path dyn dispatch.** The
   `MutationSourceExecutor` async trait is invoked
   once per mutation, not per row.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Untouched.
- **Brainstorm 002 (WeakControlRing).** **Engaged.**
  ADR 037 is the cross-ring metadata-write
  contract Brainstorm 002 names. Implementation
  uses the existing `submit_table_schema_update`
  RPC matching the WeakControlRing brainstorm's
  coordinator API sketch.
- **Brainstorm 003 (Disaggregated Compute).**
  **Partially engaged.** Distributed write
  routing is now wired (coordinator-owned
  mutations + distributed candidate reads);
  multi-worker write fanout remains future work.
- **Brainstorm 004 (Memory Arenas).** Untouched
  — ADR 037 adds one field to `ExecutionPlan::Scan`
  + one new struct (`TaskExecutionContext`) + one
  new module (`distributed/context.rs`). No
  boxed-child variants; Plan Arena preserved.

**Brainstorm 002 closes** with this cycle (cross-
ring metadata-write contract delivered via Plan A;
or partially delivered via Plan B fallback).
**Brainstorm 003 stays open** for multi-worker
write fanout.

## What Not To Do

- **Don't execute writes on workers** (D1 —
  coordinator-owned mutations; task retry must
  never become write retry).
- **Don't call `fragment_plan` on write plans**
  (D11 — fragmenter sees only derived read-only
  source / candidate plans).
- **Don't implement coordinator-local source
  reads for `INSERT SELECT` / CTAS / DELETE and
  call it "distributed"** (D6-D9 — true
  distribution via `MutationSourceExecutor`).
- **Don't add `client_write_id` / `durability` /
  `await_finalization` fields to Toron request
  structs** (D4 — Wendelon's existing client
  handles these internally).
- **Don't advance session HLC from write
  receipts** (out-of-scope — future read-your-
  writes ADR).
- **Don't include Wendelon `DROP TABLE`** (D13 —
  deferred to ADR 038).
- **Don't add UPDATE / RETURNING / MERGE /
  UPSERT** (out-of-scope).
- **Don't add Plan Arena / storage reclaim / new
  SQL syntax / `blake3` / external allocator
  crate** (out-of-scope).
- **Don't carry write receipts / client write
  IDs / durability policy / runtime filters /
  row identity policy in `TaskExecutionContext`**
  (D3 — minimal-field discipline; guard #7
  enforces).
- **Don't duplicate `query_id` between
  `TaskSpec.identity` and `TaskSpec.context`** (D3
  — single source from `spec.identity.query_id`;
  guard #5 enforces).
- **Don't inherit ambient `runtime_filter` from
  task / coordinator / worker context into
  DELETE candidate scans** (D10 — silent data
  loss hazard; guard #21 positive + negative
  pattern).
- **Don't break `ExecutionPlan::Filter` SELECT
  WHERE behavior** during Phase 3 shared driver
  extraction (D6 — byte-identical refactor
  invariant; guard #24).
- **Don't make CTAS transactional** (D8 / ADR
  033 D7 carry-forward — non-atomic CTAS
  preserved; manual cleanup via DROP).
- **Don't invent table-object-id allocation
  policy in ADR 037** (D12 — Plan A uses
  existing API; Plan B defers to ADR 038).
- **Don't change `StorageWrite` /
  `StorageDeleteRows` trait signatures** (D4 —
  trait shapes byte-identical pre/post-ADR-037).
- **Don't introduce a new public `WriteContext`
  struct at the storage-trait boundary** (D4 —
  existing request structs carry context fields
  already; `TaskExecutionContext` is for
  TaskSpec / worker boundary only).
- **Don't use generic async closure source
  executors** (D6 — `MutationSourceExecutor`
  async trait seam preserves `Send + Sync`
  lifetimes cleanly).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Task context under-carries scan-request fields, leaving workers on `local_test()` defaults silently (the load-bearing iter-3 correction this ADR captures) | D3 4-field shape; guards #2 + #6 + #4 enforce |
| R2 | Shared async helper lifetimes get noisy with generic closures | D6 + guard #23 `MutationSourceExecutor` async trait seam |
| R3 | Local write behavior drifts during shared driver extraction | D6 byte-identical refactor invariant + guard #24 reference fixture pass-count check |
| R4 | DELETE candidate reads inherit runtime filters from task / coordinator / worker context | D10 + guard #21 positive + negative pattern (three negative patterns including `spec.context.runtime_filter`) |
| R5 | Wendelon CREATE Plan A requires object-id allocation policy that does not exist yet | D12 Plan B fallback explicit; guard #25 reflects chosen path |
| R6 | Storage trace context conflicts with existing distributed trace type | Phase 0 + Phase 2 audit; keep `storage::TraceContext` and `distributed::task::TraceContext` distinct, add serde to the storage type if needed, and do not silently coerce between them |
| R7 | `TaskExecutionContext` accumulates fields post-promotion (drifts back toward 5+ fields) | D3 + guard #7 minimal-field discipline |
| R8 | `query_id` accidentally duplicated between `TaskSpec.identity` and `TaskSpec.context` | D3 + guard #5 single-source enforcement |
| R9 | Workers receive `runtime_filter` via task context instead of via `TaskInputBinding::Scan` | D3 + guard #8 source enforcement |
| R10 | CTAS partial failure leaves a created table without rows | D8 + ADR 033 D7 carry-forward — non-atomic CTAS behavior preserved and documented; no compensation in ADR 037 |
| R11 | `ExecutionPlan::Scan` `row_identity` field defaults to `Append` accidentally during refactor | D9 + guards #18 + #19 default-to-`None` + DELETE-candidate-only-`Append` |
| R12 | `RowIdentityProjection` enum location creates module coupling between `storage.rs` and `ir/execution.rs` | Impl Phase 3 note (move to neutral module if needed; do not duplicate enum) |
| R13 | Worker-side write paths added accidentally | D1 + guard `adr037_no_worker_side_storage_write_calls` (Codex iter-3 guard #18 carry-forward) |
| R14 | Mutation retry loops added accidentally during distributed write integration | D1 + ADR 030 D12 + ADR 036 D11 carry-forward |
| R15 | Read-your-writes HLC advancement added accidentally from write receipts | D4 + guard `adr037_no_read_your_writes_hlc_advancement` (Codex iter-1 guard carry-forward) |

## Recorded dissent

**None substantive at iter-3.**

Three Claude retractions across the cycle (iter-0
C2 + iter-0 D4 + iter-2 C15) — all absorbed
cleanly through iter-1 / iter-3.

Two Codex self-retractions on the same field
(`TaskExecutionContext` shape: iter-1 → iter-2 →
iter-3) — first cycle in council history with two
Codex self-retractions. Final shape converges at
iter-3 wider form after the call-graph audit.

Gemini iter-3 D2 retracted toward the wrong (2-
field) shape — first time a Gemini retraction has
moved away from the eventual settle shape. The
2-field shape was the iter-2 Codex/Claude
position that iter-3 reversed; Gemini lagged one
beat. Folded at promotion drafting (wording
correction).

## Process observations

### Fourth 4-iter cross-ring substrate cycle

| ADR | Iters | Flip-flops | Notes |
|---|---|---|---|
| ADR 029 | 4 | 0 | DDL substrate |
| ADR 030 | 4 | 1 | DML substrate; one flip-flop |
| ADR 035 | 4 | 0 | Row-identity substrate; iter-2 promotion-drafting |
| **ADR 037** | **4** | **1** | **Distributed-write-context; one D2 flip-flop across iter-1→2→3** |

Cross-ring substrate cycle floor firmly **4 iters
at 4 data points**. ADR 037 confirms after Claude
iter-2 3-iter compression prediction was wrong.
Pattern: cross-ring substrate cycles compress to
3 iters only when both Wendelon-side AND Toron-
side substrate shapes are pre-audited by iter-2.
ADR 037's Toron-side `TaskExecutionContext` shape
was the unaudited piece that forced iter-3.

### Tenth consecutive Codex correctness-clarification cycle

Codex iter-0 reframed the substrate from
"coordinator-local writer" to "coordinator-owned
mutations + distributed candidate reads."
Codex iter-3 caught the substrate failure that
iter-2's narrowing would have introduced. Pattern
established across 13 cycles (ADR 024 / 025 / 026
/ 028 / 029 / 030 / 031 / 032 / 033 / 034 / 035 /
036 / 037).

### Two Codex self-retractions in one cycle (first in council history)

| Iter | Codex D2 |
|---|---|
| iter-1 | 4-field (`session_id`, `tenant_id`, `read_bound`, `trace_context`) |
| iter-2 | 2-field (`query_id`, `profile_hint`) — first self-retraction |
| iter-3 | 4-field (`session_id`, `tenant_id`, `read_bound`, optional `trace_context`) — second self-retraction |

Pattern observation: **Codex self-retracts on
substrate-shape decisions that need deeper call-
graph audit than initial type-shape framing.**
The iter-2 narrowing was a type-shape audit
("those fields are already in `ScanRequest`");
the iter-3 audit was a call-graph audit ("those
fields in `ScanRequest` come from `ExecutionContext`,
which workers build from `local_test()` without
the task context").

### Three substantive Claude retractions in one cycle (first in council history)

iter-0 C2 (coordinator-local writer) retracted at
iter-1.
iter-0 D4 (Wendelon DROP wiring attempt) retracted
at iter-1.
iter-2 C15 (2-field context endorsement) retracted
at iter-3.

Pattern: **Claude follows Codex confident
narratives into framing errors more often in
substrate cycles than composition cycles.**

| Cycle | Type | Claude retractions |
|---|---|---|
| ADR 035 | Substrate (cross-ring) | 0 |
| ADR 036 | Composition | 1 (correctness — C3 PBC-only) |
| **ADR 037** | **Substrate (cross-ring)** | **3 (framing — C2 / D4 / C15)** |

ADR 035's pure-substrate audit (Codex iter-0
complete) allowed Claude to ratify cleanly. ADR
036's composition cycle had one correctness audit
to defer to. ADR 037's substrate cycle with mid-
cycle Codex oscillation propagated framing errors
to Claude.

### Highest reject-lift count + 4-iter cadence

ADR 037 lifts 5 fully + 1 partially + 1 deferred
= effectively 6 lifts in one cycle. Previous max
was 1 per cycle. Pattern: **deferred substrate
debt accumulates linearly across composition
cycles; substrate-lift cycles consolidate it.**
Five composition cycles (030 / 032 / 033 / 034 /
036) deferred distributed-write work; ADR 037
consolidates in one substrate cycle.

The cycle ran 4 iters despite the high lift count
because most lifts are mechanical once the
substrate (task context + Scan field + shared
driver) is in place. The complexity is in the
substrate shape, not the lift count.

### Trilogy alternation hypothesis update

Substrate trilogy (029 → 030 → 031) → composition
trilogy (032 → 033 → 034) → substrate (035) →
composition (036) → substrate (037) → **substrate
(038 = storage-admin / reclaim, committed per
D14)**.

ADR 038 will be **two consecutive substrate
cycles** (037 + 038), which **breaks the 1-cycle
alternation pattern** that held at 3 data points
(035 / 036 / 037).

Updated hypothesis: **substrate work accumulates
in pairs when deferred-debt is sufficiently
backed up.** ADR 037 + 038 are both consequences
of ADR 029-036 substrate deferrals. ADR 039+ may
return to composition or continue substrate
depending on post-ADR-038 evidence.

### Topic-concession-compresses-next-cycle pattern at 6-of-8

ADR 037 D14 commits ADR 038 = storage-admin /
reclaim substrate. Pattern: substrate-defer ADRs
seed the next cycle (ADR 035 D15 → 036; ADR 036
D14 → 037; ADR 037 D14 → 038). Pattern stable
across last three cycles.

## Future work

ADR 038+ candidates carry forward the ADR 029 →
037 arc:

- **ADR 038 — Storage-Admin / Reclaim Substrate**
  (firm commitment per D14). Lifts Wendelon DROP
  `AdminUnavailable` + physical reclaim
  (in-memory storage map cleanup + Wendelon
  descriptor deletion + tombstone collection) +
  Wendelon CREATE Plan B fallback (if ADR 037
  took that path) + descriptor-evolution
  migration. Pure substrate cycle (no SQL surface).
- **ADR 039+ candidates** (post-ADR-038):
  - `CREATE OR REPLACE TABLE` — small composition
    over ADR 029 / 033 / 034 / 037 / 038 once
    Wendelon DROP works;
  - UPDATE WHERE — composition over partial-write
    substrate (may bundle with ADR 038 or follow
    as separate substrate);
  - DELETE / INSERT / CTAS RETURNING — after
    RETURNING substrate ADR;
  - Multi-worker write fanout — parallel INSERT
    SELECT source-side, sharded INSERT VALUES,
    etc. Brainstorm 003 broader work.
- **Read-your-writes session HLC advancement
  ADR** — surfaces Wendelon receipts (`commit_hlc`
  / finalization) through Toron `WriteOutcome` /
  `DeleteRowsOutcome`; advances session read bound.
- **Subqueries in WHERE / DELETE clauses** —
  correlated-subquery substrate ADR.
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint
  substrate first.
- **Streaming / multi-batch write paths** — lifts
  ADR 032 D5 + D6 + ADR 033 D8 + ADR 035 single-
  batch cap.
- **Stale-bound-plan invalidation ADR.**
- **Idempotency / write-retry-after-descriptor-
  evolution ADR.**
- **`USING` clause / `ORDER BY` + `LIMIT` /
  table aliases / `RETURNING` / `TRUNCATE` in
  DELETE / UPDATE** — DML extensions.
- **Typed expression tape / opcode interpreter.**
- **Plan Arena / index-based plan IR.**
- **CSE + literal deduplication.**
- **Cranelift JIT compilation.**
- **Logical `Expr` arena migration.**
- **PBC partial-pushdown of computed-arg
  aggregates** — cross-ring.
- **Temporal types (DATE, TIMESTAMP).**
- **DECIMAL.**
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`ALTER TABLE` / `CREATE INDEX` / `CREATE VIEW`
  / `DROP VIEW` / `DROP SCHEMA` / `DROP INDEX`**
  — DDL siblings using `lower_ddl_table_name`
  helper pattern.
- **Constraints** (PRIMARY KEY / FOREIGN KEY /
  CHECK / column-level UNIQUE) / DEFAULT /
  GENERATED.
- **Wendelon AGG consumer wiring** — cross-ring;
  PBC partial-pushdown.
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).

## Definition of Done

- [ ] `DistributedQueryCoordinator` becomes
      context-bearing; production construction
      uses real execution context, not
      `local_test()` defaults.
- [ ] `TaskExecutionContext` defined in
      `crates/toron/src/distributed/context.rs`
      with the 4-field shape (`session_id`,
      `tenant_id`, `read_bound`, optional
      `trace_context`).
- [ ] `TaskSpec` extended with
      `context: TaskExecutionContext`.
- [ ] `FragmentPlanConfig` stamps every emitted
      task with the context.
- [ ] Production worker `ExecutionContext`
      construction uses `spec.identity.query_id` +
      `spec.context.{session_id, tenant_id,
      read_bound, trace_context}` + `spec.budget`
      + `scan_runtime_filter(spec)` +
      `scan_profile_hint(spec)` + task telemetry;
      does NOT call `ExecutionContext::local_test()`.
- [ ] `ExecutionPlan::Scan` extended with
      `row_identity: RowIdentityProjection` field;
      normal constructors default `None`; output
      schema appends identity column for `Append`.
- [ ] `RowIdentityProjection` enum location chosen
      (neutral module if needed); not duplicated
      between `storage.rs` and `ir/execution.rs`.
- [ ] SQL binder / projection paths cannot
      request synthetic row-key columns
      (carry-forward).
- [ ] `MutationSourceExecutor` async trait seam
      defined in coordinator module; not a generic
      closure parameter.
- [ ] Local `QueryCoordinator::execute_write_plan`
      refactored to call shared mutation driver
      with local source-executor impl wrapping
      `Executor::execute_bound`.
- [ ] **Byte-identical refactor invariant
      verified**: reference fixture pass counts
      match pre/post-refactor for ADR 030 / 032 /
      033 / 036 fixtures (insert_test, ctas_test,
      delete_test, delete_substrate_test).
- [ ] `DistributedQueryCoordinator` gains
      writer/deleter sinks; `with_writer` +
      `with_writer_and_deleter` constructors;
      read-only construction wires unsupported
      adapters; old distributed DML reject
      strings absent.
- [ ] Distributed INSERT VALUES path uses shared
      driver without calling `fragment_plan`.
- [ ] Distributed INSERT SELECT path uses shared
      driver with distributed source executor;
      fragments only SELECT source.
- [ ] Distributed CTAS path uses shared driver
      with distributed source executor; CTAS
      ordering preserved (source read, catalog
      create, write).
- [ ] Distributed DELETE path uses shared driver
      with distributed candidate-read source
      executor; candidate plan is `Scan { Append }
      + optional Filter`; `runtime_filter: None`
      enforced at the scan-input boundary.
- [ ] Fragmenter defensive `*NotFragmentable`
      variants preserved.
- [ ] Either Plan A (Wendelon CREATE wired
      through `submit_table_schema_update` with
      real `TableSchemaDescriptor::new_with_layout`)
      OR Plan B (Wendelon CREATE deferred;
      `AdminUnavailable` preserved); guard
      reflects chosen path.
- [ ] Wendelon DROP `AdminUnavailable` preserved
      verbatim.
- [ ] **All 25 ADR-037-specific architecture
      guards pass.**
- [ ] **All 9 required functional fixtures pass.**
- [ ] (Plan A only) 2-3 optional Wendelon
      catalog admin fixtures pass.
- [ ] All existing CREATE TABLE / VALUES INSERT
      / INSERT SELECT / CTAS / DROP TABLE / DELETE
      / DELETE substrate tests green.
- [ ] All existing SELECT corpus plan-digest
      fixtures green.
- [ ] All existing SELECT WHERE corpus byte-
      identical (D6 refactor invariant).
- [ ] ADR 029 / 030 / 031 / 032 / 033 / 034 /
      035 / 036 invariants preserved.
- [ ] `StorageWrite` / `StorageScan` /
      `StorageDeleteRows` trait shapes byte-
      identical.
- [ ] `WriteRequest` / `DeleteRowsRequest` field
      counts unchanged.
- [ ] No new `StorageError` variant added.
- [ ] No new Wendelon RPC method added (Plan A
      uses existing `submit_table_schema_update`).
- [ ] No `blake3` dependency added.
- [ ] No external allocator crate added.
- [ ] No storage reclaim trait / helper
      introduced.
- [ ] No new SQL surface added.
- [ ] Roadmap moves ADR 037 from candidate to
      promoted; advances active section to ADR 038
      (storage-admin / reclaim substrate per D14
      sequencing).
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
