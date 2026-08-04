# ADR 037: Tenant Table Schemas, Row Identity, and Table-Scoped Locality

## Status
Accepted

## Context

ADR 005 committed Wendelon to pushdown bytecode (PBC) as the compute/storage boundary. ADR 035 shipped a vectorized PBC executor that runs those programs directly over Arrow `RecordBatch`es, and ADR 036 pinned PBC as Toron's storage-side physical pushdown target (Rune/SQL → `toron-ir` → PBC). Together those ADRs gave Wendelon a fast columnar filter engine.

The engine is only as useful as the columns it can see. Today's write RPC (see `plans/schema/schemas.md`'s `write_payload_schema`) pins every incoming batch to a two-column `{key: LargeBinary, value: LargeBinary}` shape, so predicates like `age >= 18` cannot reach columns hidden inside the opaque `value` blob. Wendelon therefore cannot yet serve the warehouse workload Toron is being designed for: real tables with declared columns, scans that stay on a bounded arc of the ring, MVCC-clean updates by primary key.

This ADR closes that gap. It defines a unified tenant-table contract on top of ADR 032's `SchemaUpdate` envelope, ships a minimal table-scoped locality primitive that prevents tables from scattering uniformly across the ring, and specifies the write/read machinery needed to exploit vectorized execution over real columnar data.

Three addressing concepts are kept orthogonal, consistent with ADR 029's newtype discipline:

- **Primary key** — a declared table column; drives row identity for MVCC and point reads.
- **Row identity (`key_identity`)** — `BLAKE3(domain_tag || tenant_id || table_object_id || canonical_pk_bytes)`. Storage primitive; never user-facing.
- **Locality** — ring coordinate; derived by the table's locality policy. V1 implements `TableSpreadProjection` (default for typed tables) and `IdentityProjection` (default for legacy KV). Multidimensional / column-aware clustering is reserved for ADR 038.

This ADR does not change ADR 035, ADR 036, or `plans/schema/compute_ast.md`. Legacy `{key, value}` `WRITE_REQ` stays stable; typed table writes use a new `TableWriteRequestMeta` HRFT frame. One logical model, two wire shapes during the compatibility window. `plans/schema/schemas.md` continues to describe the legacy KV write payload unchanged.

## Decision

Wendelon adopts a unified tenant-table schema contract for the write path and table-scoped locality for the placement path.

1. **One logical model: tenant table schemas.**
   Every writable object is a table descriptor registered through ADR 032's `SchemaUpdate` envelope. Legacy `{key, value}` writes map to a default KV table descriptor, not a separate storage model.

2. **Every table has a stable `TableObjectId`.**
   `namespace` and `table_name` are operator labels. `TableObjectId([u8; 16])` is the durable tenant-scoped object identity assigned at table create. It survives future renames and is included in row-identity and locality preimages.

3. **Primary key is a declared table constraint.**
   V1 supports one non-null scalar Arrow column as the primary key. The descriptor uses `columns: Vec<String>` for future composite-PK support but enforces `len() == 1` in V1.

4. **`key_identity` is table- and PK-derived.**

   ```text
   key_identity = BLAKE3(
       "wendelon.table-row.v1" ||
       tenant_id ||
       table_object_id ||
       canonical_pk_value
   )
   ```

   This encodes table identity in the storage row key without leaking Wendelon metadata into the user-facing primary key. The domain tag prevents hash confusion with other BLAKE3 uses.

5. **Whole-record hashes are separate.**
   `payload_hash = BLAKE3(canonical_arrow_payload)` may support integrity, finalization diagnostics, or future dedup hints. It does not drive MVCC row identity.

6. **PK canonical encoding is narrow in V1.**
   Supported: signed/unsigned integers as little-endian fixed-width bytes; `Utf8`/`LargeUtf8` as raw UTF-8 (no collation normalization); `Binary`/`LargeBinary` as raw bytes; `Timestamp(ms, _)` as little-endian UTC milliseconds (any timezone normalized to UTC before canonicalization).

   Deferred or rejected: floats (NaN-equality trap), decimals, nested/struct/list PKs, nullable PKs, composite PKs.

7. **Default typed-table locality is `TableSpreadProjection`.**
   V1 typed table creation defaults to `TableSpreadProjection { spread_factor: 4, prefix_bits: 16 }`. A full table scan targets a bounded set of table arcs instead of scattering across the whole ring.

8. **`IdentityProjection` remains explicit opt-in.**
   `Locality::from(key_identity)` remains supported for pure load-balanced KV and point-heavy workloads. The default legacy KV descriptor uses it so existing KV clients see no behavior change.

9. **`ColumnProjection` / z-order is reserved for ADR 038.**
   ADR 037 defines the descriptor slot but rejects unsupported `ColumnProjection` descriptors with a stable reject code until ADR 038 lands.

10. **The write RPC accepts multi-row Arrow batches with an admission cap.**
    Public table writes accept `RecordBatch` payloads up to **65,536 rows** plus the existing byte-size limits. Larger writes must be chunked by the client or Toron.

11. **V1 storage splits into row-keyed slab records.**
    Ingest validates the incoming batch once, rejects duplicate PKs, then expands each row into a single-row Arrow IPC payload. Each child record gets its own `key_identity`, locality, child write identity, and slab append.

12. **Duplicate PKs inside one write batch are rejected.**
    V1 does not define last-write-wins inside one batch. Clients that intend multiple versions send separate writes.

13. **Read-side rebatching is mandatory query-plan work.**
    Scans decode row-keyed payloads, group compatible records by `(table_object_id, schema_generation, output_projection)`, concatenate into vector-sized `RecordBatch` chunks, then run PBC. Without rebatching, ingest splitting would throw away ADR 035's vectorization win.

14. **Scalar identity/locality PBC context disables rebatching in V1.**
    ADR 035's `LOAD_IDENTITY` and `ROUTE_BY_LOCALITY` opcodes consume scalar per-batch context. Programs using those opcodes run per-record until per-row metadata columns or typed metadata-column opcodes land.

15. **Schema generation lives in persisted write metadata.**
    `SlabRecordHeader` is not expanded. `TableWriteRequestMeta` carries `table_object_id`, `schema_generation`, parent write id, row count, and child-id material.

16. **Hot write frames carry `schema_generation`, not `schema_fingerprint`.**
    `schema_fingerprint = BLAKE3(canonical_descriptor_bytes)` lives on the descriptor and is checked when a descriptor loads — defense-in-depth against replicated descriptor corruption. It is not repeated per row-keyed child frame. At the 65,536-row admission cap, that saves 2 MB of redundant integrity bytes per max-sized bulk write.

17. **Typed table writes use a new HRFT frame; legacy `WRITE_REQ` stays stable.**
    The existing bytemuck `WriteRequestMeta` is fixed-layout and heavily exercised by tests; it is not modified. Typed table writes use a new `TableWriteRequestMeta` carrying table/schema fields explicitly. One logical model, two wire frames during the compatibility window. `plans/schema/schemas.md` continues to describe the legacy KV payload unchanged.

18. **Schema evolution is additive-only in V1.**
    Allowed: add nullable column, add column with default, relax `nullable: false -> true`. Rejected without an explicit rewrite migration: type changes, renames, drops, non-additive reorders, PK changes, and locality-policy changes.

19. **Every V1 table has a PK.**
    PK-less append-only table shapes are a future ADR with their own identity/index contract. Ingest splitting, MVCC, and point reads all depend on this constraint.

20. **Locality is baked at write time.**
    A future descriptor generation can change locality policy only for new writes. Existing rows move only through an explicit re-clustering migration with its own audit trail.

## Descriptor shape

```rust
struct TableSchemaDescriptorV1 {
    contract_version: u16,
    table_object_id: TableObjectId,
    schema_generation: u64,
    namespace: String,
    table_name: String,
    arrow_schema_ipc: Vec<u8>,
    primary_key: PrimaryKeyDescriptorV1,
    locality_policy: LocalityPolicyV1,
    schema_fingerprint: [u8; 32],
    evolution_policy: SchemaEvolutionPolicyV1,
}

struct TableObjectId([u8; 16]);

struct PrimaryKeyDescriptorV1 {
    columns: Vec<String>,
    canonical_encoding: PrimaryKeyEncodingV1,
}

enum LocalityPolicyV1 {
    TableSpreadProjection(TableSpreadLocalityDescriptorV1),
    IdentityProjection,
    ColumnProjection(ColumnLocalityDescriptorV1),
}

struct TableSpreadLocalityDescriptorV1 {
    spread_factor: u16,
    prefix_bits: u8,
}

struct ColumnLocalityDescriptorV1 {
    columns: Vec<String>,
    projection: ProjectionFunctionV1,
    spread_factor: u16,
    prefix_bits: u8,
}

enum ProjectionFunctionV1 {
    RandomHash,
    ZOrder,
}
```

### V1 constraints

- **`primary_key.columns.len() == 1`** — composite PK deferred.
- **PK column is scalar and non-nullable** — per §6.
- **`TableSpreadProjection` and `IdentityProjection` are implemented.**
- **`ColumnProjection` is parsed and rejected as unsupported** — reserved for ADR 038.
- **`spread_factor` must be in `1..=256`** — lower bound 1 = one arc; upper bound 256 prevents accidental approach to random scatter.
- **`prefix_bits` defaults to `16`** — recommended to remain `16` until a re-clustering ADR defines a safe migration.
- **Typed-table default:** `TableSpreadProjection { spread_factor: 4, prefix_bits: 16 }`.
- **Legacy KV default:** `IdentityProjection`.

## TableSpreadProjection

`TableSpreadProjection` gives table-local scan shape without solving full multidimensional clustering.

```text
bucket      = jump_consistent_hash(key_identity, spread_factor)
table_hash  = hash64("wendelon.table-locality.v1" || tenant_id || table_object_id || bucket)
row_hash    = hash64(key_identity)
prefix_mask = high prefix_bits of u64
suffix_mask = remaining low bits
locality    = (table_hash & prefix_mask) | (row_hash & suffix_mask)
```

### Properties

- A full table scan targets up to `spread_factor` table arcs instead of the whole ring.
- A hot table can be spread over multiple arcs without losing table-local scan shape.
- Increasing `spread_factor` under jump-consistent hashing (Lamping & Veach 2014) migrates exactly `1/(new_factor)` of rows — bounded, not catastrophic.
- `prefix_bits = 16` yields 65,536 possible table/bucket prefixes with 48 bits of row spread inside each prefix; at `spread_factor = 4` the default covers four prefixes per table.

### Range planning

- The planner derives the table's bucket prefixes from the descriptor and issues one locality-range scan per unique prefix.
- Each prefix range is `[prefix, prefix | suffix_mask]`.
- `IdentityProjection` tables use the existing arbitrary locality-range scan behavior.

## Rationale

**Why table identity belongs in storage row identity, not user-facing PK.** A primary key is unique inside a table, not across a tenant. Wendelon's storage row identity must therefore carry table identity so that `customers.id = 5` and `orders.id = 5` don't collide in the `(tenant_id, key_identity)` storage index. Encoding this via `table_object_id` in the `key_identity` hash preimage keeps the ergonomic property that a user declares `PK = customer_id` and never sees Wendelon metadata in their logical schema.

**Why PK-derived identity (not whole-record hash).** Wendelon's MVCC model treats successive versions of a row as sharing a `key_identity`. If `key_identity = BLAKE3(whole_record_bytes)`, then `UPDATE row SET age = 2` produces a new logical row instead of a new version, breaking compaction, point reads, and ADR 035's `LOAD_IDENTITY` / `ROUTE_BY_LOCALITY` semantics. Whole-record hashes still have value as `payload_hash` for integrity/dedup, but not as the storage key.

**Why `TableSpreadProjection` in V1, and not full z-order.** `IdentityProjection` gives beautiful point-balance but scatters tables uniformly across the ring — any full-table scan hits every node. That defeats the OLAP scan goal the stack is being built for. The minimal fix is table-scoped spread: keep one table in a bounded number of arcs so scans stay sequential on a known working set. Hot tables don't collapse to a single arc because `spread_factor` spreads them across several via jump-consistent hash. The full z-order story (multi-dimensional clustering by declared columns, time bucketing, bit interleaving order, policy migration under spread-factor change, compaction interaction) is substantial additional design space that belongs in ADR 038. Shipping `TableSpreadProjection` now delivers the 80% scan benefit without committing to those decisions.

**Why multi-row at the wire, single-row in the slab.** Arrow is designed for bulk; forcing 10,000 RPCs for 10,000 rows throws away the main efficiency Arrow exists for. The V1 storage index has one `key_identity` per slab record, which is load-bearing for MVCC and point reads. Accepting multi-row batches at the edge and splitting at ingest preserves Arrow's wire efficiency without rewriting the storage index. The cost is paid at ingest (N BLAKE3 computations, N index inserts per N-row batch) and at read time (rebatching before PBC). Analytical multi-row fragments are deferred to a follow-up ADR when the cost profile warrants row-level secondary indexes.

**Why read-side rebatching is mandatory, not opportunistic.** Single-row slab records are correct for MVCC but destroy ADR 035's vectorization win if PBC is invoked per record. Reassembling compatible records into vector-sized batches before PBC reclaims the win. The scalar-context exception (programs using `LOAD_IDENTITY` / `ROUTE_BY_LOCALITY`) is honest: those opcodes take one context per batch, and mixing records would misapply the first record's context to the rest.

**Why fingerprint on descriptor only.** Including `schema_fingerprint` on every child frame costs 32 bytes × up to 65,536 rows = 2 MB per bulk write. The fingerprint defends against replicated descriptor corruption, which is a descriptor-load concern, not a per-write concern. Descriptor-load-time validation closes the same threat at near-zero hot-path cost.

## Consequences (the negatives)

- **V1 storage does single-row slab records under the hood.** A 65,536-row analytical write produces 65,536 `SlabRecordHeader`s, 65,536 BLAKE3 computations, and 65,536 index inserts. Mitigated by batching appends into one `io_uring` submission.
- **Read-side rebatching has two modes.** Predicate-only programs rebatch; identity/locality-context programs don't. Planner must distinguish. Small branch cost in the hot read path.
- **Jump-consistent hash computation per write.** Cheap (a few adds and multiplications per bucket step) but non-zero. Deterministic — cacheable for repeated writes to the same PK.
- **`TableSpreadProjection` parameters are a one-way commitment.** Changing `prefix_bits` on an existing descriptor requires a full data migration. Changing `spread_factor` via jump-consistent hash migrates `1/new_factor` of rows — still requires a rewrite pass. Choose defaults carefully for tables that will grow.
- **HRFT frame type zoo grows.** Legacy `WriteRequestMeta` stays; `TableWriteRequestMeta` is new. Two frame types for the parser and tests.
- **`plans/schema/schemas.md` describes the legacy KV payload, not the typed-table one.** That file remains accurate for the legacy `{key, value}` path. The typed-table payload is specified by this ADR's `TableWriteRequestMeta` + `arrow_schema_ipc` in the descriptor. Operators referring to `schemas.md` should read ADR 037 alongside it.
- **Error surface grows.** New reject codes listed in Appendix A.

## Rejected alternatives

- **Separate dispatch paths for raw KV and typed tables.** Rejected. Two storage models for the same substrate doubles the bug surface and contradicts the physics-grade unification. KV as a degenerate two-column table is strictly simpler.
- **`key_identity = BLAKE3(whole_record_bytes)`.** Rejected. Breaks MVCC; every update would produce a new logical row. Whole-record hashes kept as `payload_hash` for the jobs that actually need them.
- **Force single-row write RPCs at the wire.** Rejected. Throws away Arrow's bulk-throughput primitive.
- **`IdentityProjection` as the V1 default for typed tables.** Rejected. Scans would scatter uniformly across the ring; warehouse workloads would be abysmal.
- **`TableSpreadProjection` with fixed, non-configurable parameters.** Rejected. Hot tables and small tables want different spread factors; configurability via the descriptor is cheap and forward-compatible.
- **Composite PKs in V1.** Deferred. The `columns: Vec<String>` shape keeps the door open for a later ADR without a descriptor-format break.
- **Row-level secondary indexes in V1.** Deferred. Point reads under V1 go through the `TableSpreadProjection` arc plus PBC filter; sufficient for the initial workload shape. A PK-to-row secondary index ADR opens if point-read latency requires it.
- **Bundling column-aware clustering (z-order, time bucketing) into 037.** Rejected — opens substantial additional design surface (bit layout, type support, migration under policy change, compaction) that deserves its own ADR.

## Implementation staging

### Phase A: descriptor and schema state

- Add `TableSchemaDescriptorV1` encode/decode helpers near the compute/storage schema envelope.
- Assign stable `table_object_id` at table create.
- Resolve descriptors by `(tenant_id, table_object_id, schema_generation)` and by `(tenant_id, namespace, table_name)`.
- Create default descriptors for legacy KV and typed table tests.
- Validate PK, locality policy, spread factor, prefix bits, and descriptor fingerprint.

### Phase B: typed table write frame

- Add `TableWriteRequestMeta`.
- Include `table_object_id`, `schema_generation`, parent write id, row count, and child-id material.
- Enforce the 65,536-row admission cap and byte caps.
- Validate incoming Arrow schema against the resolved descriptor.
- Reject duplicate PKs inside the batch.
- Canonicalize PK values.

### Phase C: split append

- For each row, compute `key_identity`.
- Compute locality from the descriptor's locality policy.
- Encode the row as a single-row Arrow IPC payload.
- Append each child payload as its own slab record.
- Derive child write identity from `(parent_write_id, row_ordinal, key_identity)`.
- Report one parent write with N child records in finalization and audit surfaces.

### Phase D: query planning and rebatching

- For `TableSpreadProjection`, map table scans to unique table-bucket prefix ranges.
- Decode candidate row-keyed Arrow payloads.
- Group compatible records by `(table_object_id, schema_generation, output_projection)` and PBC scalar-context safety.
- Concatenate compatible records into vector-sized batches before PBC.
- Fall back to per-record execution for identity/locality-context programs.

### Phase E: column locality follow-up (out of scope for this ADR)

ADR 038 will define `ColumnProjection` and cover z-order bit layout, supported column types, time bucketing, spread-factor change migration, and compaction interaction.

## Implementation follow-ups

1. **Mixed schema generations on read.**
   Lean: the query planner names a target schema generation; V1 rejects incompatible mixes; a later read-time projection layer can map additive older generations forward.

2. **Per-row identity/locality in PBC.**
   Rebatching and scalar context remain in tension. Per-row metadata columns or typed metadata-column opcodes are a future ADR.

3. **Large-write chaining above 65,536 rows.**
   The per-frame cap is pinned. The multi-frame parent-write chaining protocol is implementation design.

## Appendix A: new PBC reject codes

ADR 036 Deferred Decision #1 anticipated that the reject-code enum would extend during implementation. ADR 037 introduces the following stable codes, all consistent with ADR 036's "no free-form error strings across the boundary" rule:

- `PBC_REJECT_SCHEMA_STALE` — `expected_schema_version` in the write frame does not match the current descriptor generation.
- `PBC_REJECT_SCHEMA_UNKNOWN` — `table_object_id` is unknown to the tenant's schema state, or the schema has not yet replicated.
- `PBC_REJECT_COLUMN_TYPE_MISMATCH` — the incoming Arrow schema does not match the descriptor at the named generation.
- `PBC_REJECT_PK_MISSING` — the PK column declared in the descriptor is absent from the incoming batch.
- `PBC_REJECT_PK_NULL` — a row's PK value is null (nullable PKs are rejected in V1).
- `PBC_REJECT_PK_TYPE_UNSUPPORTED` — PK type is outside the V1 canonicalization set (§6).
- `PBC_REJECT_DUPLICATE_PK_IN_BATCH` — two or more rows in the batch share a PK value.
- `PBC_REJECT_LOCALITY_UNSUPPORTED` — descriptor declares `ColumnProjection`, which is reserved for ADR 038.
- `PBC_REJECT_BATCH_TOO_LARGE` — batch exceeds the 65,536-row admission cap or the byte-size limit.

## References

- ADR 005: Decoupling Compute via Pushdown Bytecode (PBC)
- ADR 029: Identity vs. Locality
- ADR 032: Schema Evolution And Control-Ring Contract
- ADR 034: Toron-Wendelon Integration Contract
- ADR 035: Vectorized PBC Executor
- ADR 036: PBC Pushdown Contract — 037 extends its reject-code enum per its Deferred Decision #1.
- ADR 038: Column-Aware Locality V1 (Z-Order) — fills the `ColumnProjection` slot reserved here.
- `plans/schema/compute_ast.md` — PBC V1 spec, unchanged.
- `plans/schema/schemas.md` — legacy `write_payload_schema` for the default KV descriptor, unchanged.
- `src/schema/core.rs` — `SlabRecordHeader` and `WriteRequestMeta` layouts that this ADR does not modify.
