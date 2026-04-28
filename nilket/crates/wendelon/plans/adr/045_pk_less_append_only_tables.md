# ADR 045: PK-less Append-Only Tables

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan: `plans/impl/045_pk_less_append_only_tables.md`

## Status

Accepted

## Context

ADR 037 §19 punts PK-less append-only tables to a "future ADR
with its own identity/index contract." ADR 042 §16 / §19
narrows the deferral: a PK-less table cannot populate
`__row_key_id__` from a PK, so ADR 045 must pick an
alternative identity. The post-044 roadmap reaffirms:
append-only tables "almost always opt into
`RecordLayoutPolicy::MultiRow`" and are "the natural read
shape for column-projected fetch."

The motivating workloads:

- **Event streams.** Application events, structured-log
  ingest, metrics with per-emit identity. No natural
  unique key; the *event itself* is the identity.
- **Audit trails.** Tamper-evident records (ADR 025-style)
  where every emission is a new row by construction.
- **Time-series ingest.** Sensor / telemetry / metric
  streams. `(device_id, t)` is *almost* a PK but two
  emissions at the same HLC are valid — silent dedupe via
  PK is a data-loss bug.
- **CDC sinks** (brainstorm 004). CDC frames are
  intrinsically append-only.

The semantic constraint that drives the design: **identical
payloads at distinct ingest events must remain distinct
rows.** Two heartbeats with identical payload, two audit
events of the same shape, two sensor readings reporting
the same value — each is its own fact. A payload-hash-only
identity collapses them into a single
`(tenant_id, key_identity)` version chain and breaks
append-only semantics under MVCC.

The composition with the substrate ADR 042 / 043 / 044
already laid:

- **ADR 042 multi-row.** Append-only is the canonical
  multi-row shape — wide schemas, scan-heavy, batch-shaped
  writes. Layout policy is hard-required to be multi-row.
- **ADR 043 LZ4 body compression.** Multi-row default →
  `Lz4Frame` default. No special case.
- **ADR 044 column-projected fetch.** Append-only event
  tables have wide schemas and narrow projections; this is
  the workload 044's planner thresholds were tuned for.
- **ADR 040 AGG + CSB short-circuit.** Append-only is the
  workload that benefits *most* from CSB pruning — every
  sealed slab is visibility-complete forever. The
  supersession axis (042 §11) collapses to the trivial
  case.

ADR 045 is therefore a small ADR by the substrate's
standards. It adds an identity policy enum and a write-
context-derived `__row_key_id__` derivation. It does not
introduce a new synthetic column, a new replicated state
surface, or a new wire frame.

## Non-goals (V1)

- **No row-level `DELETE` / `__row_tombstone__` column.**
  Append-only tables are insert-only in V1. Row delete
  pulls in retention floors, AGG visibility, secondary-
  index cleanup, compaction-GC rules, and a delete-by-key-
  identity wire shape — that is a real ADR, not a §-of-045.
  The follow-up shape is sketched in *Forward-compatibility
  analysis* below. Until then, retention is at the
  table / range / slab lifecycle level (ADR 042 §1).
- **No content-hash identity.** Two identical payloads at
  distinct ingest events must remain distinct rows. A
  payload-hash-only identity collapses them; rejected
  categorically, not deferred.
- **No idempotent-ingest mode.** Future ADR territory.
  Pulled by exactly-once / dedupe-on-write workloads.
- **No replicated table-local append counter.** Identity
  is synthesized from authenticated write context; no
  per-table counter, no replicated state surface, no
  recovery reconciliation.
- **No new storage-only synthetic column.** The ADR 042
  trio (`__row_hlc__`, `__row_key_id__`,
  `__row_routing_locality__`) is sufficient. Recovery
  rebuilds indexes from those alone.
- **No public point-read API by any append cursor.** The
  write-context tuple is storage-internal. Exposing an
  HLC-rooted or identity-rooted cursor has API
  consequences (ordering, resume, schema-evolution
  interaction) that belong to the CDC ADR.
- **No vector-form `LOAD_IDENTITY` / `ROUTE_BY_LOCALITY`
  PBC opcodes.** ADR 042 already deferred these.
- **No CDC streaming protocol.** Brainstorm 004.
- **No new `RecordLayoutPolicy` variant.** Append-only is
  hard-required to be `MultiRow`.
- **No new locality policy variant.** All three existing
  ADR 037 / 038 variants compose unchanged with append-
  only.
- **No new node capability bit.** Append-only is descriptor-
  driven; mixed clusters serve both shapes identically.
- **No new HRFT frame family.** No new reject-code family.
- **No `ObjectStore` trait change.**
- **No envelope `format_version` bump.** On-disk slab shape
  is unchanged.
- **No project-name-prefixed names.**
- **No `V1` / `V2` suffixes** on new Rust types.
- **No reserved-for-V2 enum slots, no `#[serde(default)]`
  shims, no "future ADR will pin this" hedging.**
- **No descriptor-fingerprint split into logical / physical
  halves.** ADR 045 changes the logical schema; a
  fingerprint break is appropriate here.

## Decision

### 1. `RowIdentityPolicy` replaces the mandatory PK field

The descriptor's `primary_key: PrimaryKeyDescriptor` field is
replaced by:

```rust
pub enum RowIdentityPolicy {
    PrimaryKey(PrimaryKeyDescriptor),
    AppendOnly,
}
```

`PrimaryKey(...)` preserves ADR 037 semantics. `AppendOnly`
means:

- rows are assigned storage identity by the engine, not
  derived from a user column;
- writes are inserts only — no update / upsert /
  delete-by-key in V1;
- no duplicate-PK check runs;
- no MVCC supersession by key occurs;
- point reads by user payload columns are unavailable.

This is a logical-schema change, so `RowIdentityPolicy`
enters the descriptor fingerprint. No `#[serde(default)]`
shim, no `Option<PrimaryKeyDescriptor>` coexistence field —
per `no-premature-backward-compat.md`, just change the
type.

### 2. Append-only requires `MultiRow`

`RowIdentityPolicy::AppendOnly` requires
`RecordLayoutPolicy::MultiRow`. The descriptor validator
rejects `AppendOnly + SingleRow` outright. Reasons:

- Append-only is intrinsically scan-shaped; single-row
  records defeat ADR 042's amortization.
- A single-row append-only path would need a second
  recovery / packing branch for no workload win.
- Tenants who want single-row writes already have PK-keyed
  tables.

A one-row append is still encoded as a one-row multi-row
record if flushing requires it.

### 3. Identity is synthesized from write context

Append-only identity is coordination-free. Each row
identity is derived from the authenticated writer, the
row HLC, and the row's intra-batch position:

```rust
pub struct WriterNodeId([u8; 16]);
pub struct IntraBatchIndex(u32);
```

`WriterNodeId` is bound from the authenticated mTLS
identity of the node admitting the write (ADR 018);
`IntraBatchIndex` advances within a single write request,
0-indexed. `IntraBatchIndex` overflow rejects the entire
write before any slab append — no wraparound, no
collision-resolution path. Allocation is local to the
admitting node and the request scope; no replicated state
is consulted.

For a batch of N rows, row `i` receives:

```text
writer_node_id = authenticated writer identity
row_hlc        = the row's assigned HLC (ADR 012)
intra_batch    = i (zero-indexed)
```

### 4. `__row_key_id__` is write-context-rooted

For append-only rows:

```text
row_payload_hash = BLAKE3(canonical admitted Arrow row bytes)

key_identity = BLAKE3(
    "table.append-row"
    || tenant_id              // 16 B
    || table_object_id        // 16 B
    || writer_node_id         // 16 B
    || row_hlc                // 16 B
    || intra_batch_index_be   // u32 big-endian, 4 B
    || row_payload_hash       // 32 B
)
```

The uniqueness root is the
`(writer_node_id, row_hlc, intra_batch_index)` tuple. The
payload hash binds the identity to the admitted row bytes
for diagnostics and defense-in-depth — it is not a dedupe
signal. Identical payloads at distinct write contexts
produce distinct identities. Whether to keep the payload
hash in the preimage is bench-tunable (Open question 1).

The domain tag `"table.append-row"` separates this preimage
from ADR 037's PK-derived domain so a constructed PK can
never collide with an append-only key identity at the
BLAKE3 input layer. `intra_batch_index_be` is the big-
endian `u32` representation; this is the wire/format
contract and is unambiguous across implementations.
`row_payload_hash` is computed through the structured
Arrow row canonicalizer, not ad hoc byte slicing.

For PK-keyed tables, ADR 037's PK-derived identity remains
unchanged.

### 5. No new synthetic column is required

Append-only multi-row records use the ADR 042 synthetic
columns unchanged:

```text
user columns...
__row_hlc__               FixedSizeBinary(16)
__row_key_id__            FixedSizeBinary(32)
__row_routing_locality__  UInt64
```

The write-context components (`writer_node_id`,
`intra_batch_index`) are not stored as separate columns.
Recovery does not need them: it reads `__row_key_id__`,
`__row_hlc__`, and `__row_routing_locality__` directly
from the IPC body and rebuilds the same redb indexes
ADR 042 already requires. Compaction preserves those
synthetic values exactly per row and never recomputes
append-only identity for surviving rows.

### 6. Locality policy is unchanged; default is table spread

Append-only tables use existing `LocalityPolicy` variants
(ADR 037 / 038):

- **`TableSpreadProjection`** uses the synthesized
  `key_identity` as its salt input, keeping full-table
  scans bounded to table arcs (ADR 037 design intent).
  This is the V1 default when the descriptor omits
  locality, matching ADR 037's typed-table default
  (`spread_factor: 4, prefix_bits: 16`).
- **`ColumnProjection`** is the recommended shape for
  event-time, tenant, region, or other scan-relevant
  clustering columns. Tenants whose append-only table has
  a natural time-locality column should declare it
  explicitly.
- **`IdentityProjection`** is valid for write-spread-heavy
  streams but scatters full scans; explicit opt-in only.

The synthesized `key_identity` is a valid salt input for
`TableSpreadProjection` — there is no PK dependency to
resolve. Defaulting append-only locality to the same
table-spread shape as PK-keyed tables keeps operator-
visible defaults consistent across identity policies.

The default is a *create-time resolution rule*, not a
`#[serde(default)]` compatibility shim: when an `AppendOnly`
descriptor omits locality, the create-table path writes
the resolved `TableSpreadProjection { spread_factor: 4,
prefix_bits: 16 }` value into the persisted descriptor
before fingerprinting. Serialized descriptors carry an
explicit locality policy on the wire and at rest. Schema-
load decode never silently fills in defaults.

No `AppendOrderProjection` is introduced. Tenants who want
time-range locality declare an event-time clustering
column under `ColumnProjection`.

### 7. Deterministic packing keeps ADR 042 order

ADR 042's deterministic multi-row sort order is the
physical packing rule for append-only tables, unchanged:

```text
(__row_routing_locality__, __row_key_id__, __row_hlc__, original_ordinal)
```

The `__row_key_id__` tie-breaker is deterministic and
unique with overwhelming probability (32-byte BLAKE3
output over a unique write-context tuple). This ADR does
not promise physical ingest order inside a record; CDC
ordering remains future work.

Compaction may move rows between physical records but
must preserve `__row_key_id__` and per-row HLC exactly per
row.

### 8. Version chains contain one version per identity

The existing primary index shape remains:

```text
(tenant_id, key_identity) -> Vec<RecordLocation sorted by hlc>
```

For append-only tables, every generated `key_identity`
should have exactly one `RecordLocation`. A second
location for the same identity is an internal identity-
collision / corruption error, not an update or dedupe
path. The probability of an actual BLAKE3 collision over
the V1 preimage is cryptographically negligible; an
observed collision is a code bug.

`supersession_metadata` for append-only slabs is always
populated as `tracked None` once the slab is sealed —
append-only rows do not hide older versions. Existing
`tombstone_metadata` (ADR 040) rules still apply to
table / range / slab lifecycle operations.

### 9. CSB short-circuit composition

For sealed append-only slabs, CSB direct-answer
visibility uses the existing checks:
`seal_hlc <= as_of_hlc`, tracked tombstone metadata, and
tracked supersession metadata. Append-only slabs satisfy
the supersession side by construction (always `tracked
None`). The reclaim loop skips supersession-driven
reclaim for append-only segments; they reclaim only on
slab lifecycle events (table drop, range sweep, future
retention ADR).

### 10. Query semantics are scan-first

Append-only tables support the existing table scan, PBC,
AGG, CSB, multi-row, compression, and L2 column-fetch
paths. They do not support:

- primary-key point lookup;
- update / upsert by key;
- row-level delete (deferred to follow-up ADR);
- duplicate-key rejection;
- dedupe by content hash.

`as_of_hlc` filtering continues to use per-row
`__row_hlc__`. The synthesized identity is not a timestamp
substitute.

### 11. PBC composition unchanged

PBC opcodes operate over user-visible columns. The ADR 042
synthetic columns are storage-only and stripped before
PBC's user-facing output, exactly as ADR 042 / 044 already
mandate. No new PBC opcode.

### 12. AGG composition unchanged

AGG accumulators consume only declared input columns.
`count_star` over an append-only table is exact (no
supersession to subtract; no row tombstones in V1).
`min(event_time)` / `max(event_time)` against an event-
time clustering column compose with CSB short-circuit
straightforwardly — append-only sealed slabs are direct-
answer eligible without supersession checks.

### 13. ADR 044 column-projected fetch composition

Append-only is the canonical 044 workload. The column-
fetch planner picks columnar fetch on every typical
analytical projection. The `ColumnFetchRequirement` is
unchanged from ADR 044 — the same three synthetic columns
enter the requirement only when an opcode references
them.

### 14. Time-travel composition (ADR 013)

`lookup_as_of(t)` over an append-only table returns rows
where `__row_hlc__ <= t`. Strictly simpler than PK-keyed
time-travel because there are no version chains to walk —
each row is its own independent fact.

### 15. Recovery is descriptor-free for append-only

Recovery from raw slab bytes already reads synthetic
columns directly (ADR 042 §3 / 043 §8). For append-only:

- `__row_key_id__` is read directly (no PK
  canonicalization);
- `__row_routing_locality__` is read directly (no
  recomputation);
- no append-counter state to reconcile (because none
  exists).

Recovery is, if anything, *easier* than PK-keyed because
no PK schema knowledge is needed.

### 16. Toron-side surface

Toron's `CREATE TABLE` syntax adds an `APPEND ONLY`
clause:

```sql
CREATE TABLE events (
    event_time   TIMESTAMP,
    payload      BINARY,
    ...
) APPEND ONLY
  CLUSTER BY (event_time);
```

`APPEND ONLY` translates to
`RowIdentityPolicy::AppendOnly` on the descriptor.
`PRIMARY KEY` plus `APPEND ONLY` is a parse error.
`APPEND ONLY` without an explicit `CLUSTER BY` falls back
to the V1 default (`TableSpreadProjection { spread_factor:
4, prefix_bits: 16 }`).

Update / upsert / delete-by-key against an append-only
table are parse errors. V1 ships *insert-only*; row delete
lands with the follow-up ADR.

ADR 034's `CreateTable` wire frame extends with
`row_identity_policy: RowIdentityPolicy`. No new frame, no
new HRFT family.

### 17. Write API stays row-batch-shaped

The client sends an Arrow batch plus the existing table-
write metadata. For `AppendOnly` descriptors, the storage
node:

1. validates the Arrow batch against the descriptor's
   user schema;
2. rejects user columns named `__row_*` (defense-in-depth
   against synthetic-column collisions);
3. skips PK presence / null / duplicate-in-batch
   validation;
4. binds `WriterNodeId` from the authenticated request
   context, assigns per-row HLCs (ADR 012) and
   `IntraBatchIndex` values;
5. computes per-row payload hashes (under the V1
   preimage) and key identities;
6. computes locality from the descriptor;
7. appends the ADR 042 synthetic columns;
8. packs rows into bucket-pure multi-row records sorted
   by the ADR 042 deterministic order.

No new public write-frame family. Audit metadata records
the batch's row count and writer identity, not every row
identity.

## Rationale

**Why write-context-rooted identity, not content-hash-only.**
Append-only does not mean unique-by-content. Logs and
audit trails must preserve repeated identical events. A
payload-hash-only identity collapses repeated events into
a single version chain and makes the second append look
like a superseding row version — wrong model. The
`(writer_node_id, hlc, intra_batch_index)` tuple makes
distinct appends distinct rows.

**Why no table-local replicated counter.** A per-table
counter would be a new replicated serialization point on
the hot write path. Append-only is precisely the workload
that wants high-throughput parallel ingest; serializing
on a counter cuts against that. Recovery would also need
`next_append_ordinal = max(catalog, max_recovered + 1)`
reconciliation, with the attendant risk of
catalog-vs-recovered drift on partial-failure paths. The
write-context shape uses information the write path
already has: authenticated writer identity (ADR 018),
HLC (ADR 012), row position inside the batch. No new
state surface. The motivating benefit a counter would
provide (CDC cursor exposure) is recoverable from
`(__row_hlc__, __row_key_id__)` if a future CDC ADR
needs it.

**Why include the payload hash in the identity preimage.**
The write-context tuple is sufficient for uniqueness
alone. The payload hash binds identity to admitted row
bytes, giving debugging tools a stable way to explain
"same write context, different bytes" if request replay
or HLC assignment ever misbehaves. It is not a dedupe
signal — identical payloads at distinct write contexts
still produce distinct identities. The BLAKE3-on-row
cost is non-trivial for wide rows (Open question 1,
bench-resolvable).

**Why no new storage-only synthetic column.** Once
identity does not depend on a mutable table counter,
recovery does not need to reconstruct write-context
material. The ADR 042 trio is sufficient. Adding a
column for diagnostic-only purposes would inflate
steady-state storage cost on every append-only row for
no operational benefit.

**Why `MultiRow` is hard-required.** Append-only's read-
side optimizations (CSB short-circuit, ADR 044 column
fetch) materially require multi-row. Single-row append-
only would need a second recovery / packing branch for
no workload win. Tenants who want single-row writes have
PK-keyed tables.

**Why `TableSpreadProjection` is the default locality.**
ADR 037's typed-table default is
`TableSpreadProjection { spread_factor: 4,
prefix_bits: 16 }`; append-only inherits the same
default. The synthesized `key_identity` is a valid salt
input regardless of identity policy — the spread
machinery doesn't care whether the salt came from a
canonical PK or a write-context tuple. Applying the same
default keeps operator-visible behavior consistent across
identity policies. `IdentityProjection` would scatter
scans across the whole ring, which is the wrong default
for a table shape that is overwhelmingly scan-read.

**Why no descriptor-fingerprint split.** ADR 045 changes
the *logical* schema (a new way of identifying rows).
Fingerprint break is correct here. The split (separate
logical / physical fingerprints) is its own ADR
motivated by physical-only churn — and 045 is not
physical-only churn.

**Why fold `PrimaryKeyDescriptor` into a
`RowIdentityPolicy` variant rather than coexisting.**
Coexistence (`primary_key: Option<PrimaryKeyDescriptor>`
plus `row_identity_policy`) creates two sources of truth
and a validation rule pair. Folding gives a single
exhaustive enum the validator handles in one match.

**Why no `Ordinal` qualifier on the variant name.** The
identity is write-context-rooted, not derived from a
stored ordinal counter, so an `Ordinal` qualifier would
overstate the mechanism. It would also pre-allocate
naming space for a hypothetical content-hash variant,
which `no-premature-backward-compat.md` discourages even
at the naming level. `AppendOnly` names the table
semantics directly. Future variants
(`AppendOnlyContentHash` or anything else) land on their
own merits.

**Why defer row-level tombstones.** Tombstones drag in
retention floors, AGG visibility composition, secondary-
index cleanup, compaction-GC rules, and a delete-by-key
wire shape. Each is a real design surface. Bundling them
with append-only doubles the V1 scope. Append-only is
operationally complete without row delete: V1 ingest +
table-level retention covers the canonical workloads;
audit-trail / GDPR sweeps land with the follow-up ADR.

## Consequences (the negatives)

- **Descriptor fingerprint break.** The
  `RowIdentityPolicy` refactor changes the descriptor
  shape. Existing tables re-fingerprint on schema-load.
  No deployed external state to preserve — bookkeeping
  cost.
- **Identity changes on retry after a committed failure.**
  A client retry that is actually applied as a second
  write receives a later HLC and therefore a different
  identity. This is correct append-only behavior but is
  not idempotent ingest. Tenants who need idempotent
  ingest wait for the future content-hash policy.
- **HLC-derived identity leaks timing to raw-storage
  readers.** Identity generation is tied to write time;
  the BLAKE3 preimage is not visible, but the per-row
  HLC column (`__row_hlc__`) is the visible timing
  carrier. Tenants who need timing-independent identity
  use the future content-hash policy.
- **Payload hash CPU cost on every row.** The write path
  computes a canonical row hash in addition to the final
  `key_identity`. For wide event rows (10s of KB) this is
  measurable. Open question 1 surfaces whether the hash
  remains in V1.
- **No low-latency single-row fast path.** V1 forces
  multi-row layout even for one-row flushes. This is the
  right trade for V1; tenants who want low-latency
  single-row writes have PK-keyed tables.
- **No physical ingest-order guarantee inside records.**
  Packing is deterministic by routing locality and
  identity, not by append arrival order. CDC ordering
  uses HLC / log order if a future CDC ADR exposes it.
- **No content dedupe.** Tenants with duplicate-heavy
  event streams store all duplicates. That's the
  semantic contract, not an optimization miss.
- **No PK point reads.** Workloads that need lookup /
  update by a business key use `PrimaryKey(...)` tables.
  ADR 046 (secondary indexes, deferred) will fill the
  gap for index-by-non-PK-column queries on append-only
  tables.
- **`SchemaEvolutionPolicy` interaction.** ADR 037 §17's
  schema evolution rules apply unchanged to append-only
  tables — appending columns is fine, renaming /
  removing columns has the same constraints as PK-keyed.
  Append-only doesn't relax evolution.
- **Mixed PK / append-only fleet.** A node serves both
  shapes simultaneously. Code paths that branch on
  `RowIdentityPolicy` exist in: writer (identity
  synthesis), Toron parser. Each is small but the
  branches are spread.
- **Without row delete, GDPR sweeps require partition-
  level retention or wait for the follow-up ADR.**
  Operators with immediate row-delete needs use
  PK-keyed tables in V1.
- **Identity uniqueness depends on `WriterNodeId`
  being authenticated and unique.** ADR 018 already
  guarantees this via mTLS identity; ADR 045 does not
  add a new authentication surface, but it does newly
  rely on the property. A node identity collision would
  be a key-management bug visible across more than just
  ADR 045.

## Rejected alternatives

- **Content-hash-only identity** (`key_identity =
  BLAKE3(payload)`). Rejected on correctness grounds:
  collapses identical events into one version chain,
  breaks append-only semantics. Idempotent-ingest
  workloads that *want* dedupe get a future ADR with
  its own canonicalization spec.
- **Replicated table-local `AppendOrdinal` counter.**
  Rejected for V1 because it introduces a per-table
  replicated serialization point, recovery
  reconciliation, and snapshot state for no V1 benefit
  the CDC ADR cannot supply later from `__row_hlc__` /
  `__row_key_id__` alone.
- **`key_identity = BLAKE3(writer_node_id || row_hlc ||
  intra_batch_index)` only** (no payload hash). Kept the
  payload hash for V1 diagnostics; if Phase-7 benches
  show BLAKE3-on-row dominates ingest CPU, this is the
  cleanest simplification to revisit.
- **Reuse PK-keyed tables with a synthetic UUID column.**
  Forces users to invent identity at write time; doubles
  row width; doesn't address supersession-doesn't-apply.
- **Append-only via a read-path-only flag (no descriptor
  change).** Writer needs to know to synthesize identity;
  CSB needs to know to skip supersession. Logical schema,
  not a read-time hint.
- **Bundle row-level tombstones into 045.** Rejected:
  doubles the V1 scope, drags in retention / AGG
  visibility / compaction-GC / secondary-index-cleanup.
- **`__row_append_ordinal__` synthetic column.** Without
  a counter, no column to reconstruct. Adds 8 bytes / row
  + Arrow overhead for no operational benefit.
- **Allow append-only `SingleRow` records.** Rejected:
  spends implementation surface on the least-useful
  shape.
- **Add an `AppendOrderProjection` locality variant.**
  Rejected: `ColumnProjection` over an event-time column
  already covers it.
- **Default locality = `IdentityProjection`.** Rejected:
  scatters scans across the whole ring, defeats ADR 037's
  table-arc-bounded-scan property when the tenant
  intended a scan-shaped workload.
- **Explicit-required locality / no default.** Rejected:
  ADR 037's `TableSpreadProjection` default applies
  cleanly via the synthesized `key_identity` salt; making
  append-only an exception introduces an inconsistency
  with PK-keyed tables that doesn't earn its keep.
- **Forbid `TableSpreadProjection` on append-only.**
  Rejected: the synthesized `key_identity` is a defined
  salt input; the spread machinery works unchanged.
- **Add a node capability bit
  `NodeCapability::AppendOnly`.** Mixed clusters serve
  both shapes; nothing to negotiate.
- **Bump envelope `format_version`.** No on-disk shape
  change.
- **Add a new HRFT write-frame family.** The descriptor
  tells the storage node which path applies; row-batch
  shape unchanged.
- **Pre-allocate `AppendOnlyContentHash` enum slot** (per
  `no-premature-backward-compat.md`).
- **Pre-allocate `__row_tombstone__` synthetic-column
  slot** (per same convention).
- **Project-name-prefixed names** (per
  `no-project-name-in-protocols.md`).
- **`V1` / `V2` suffixes** on `RowIdentityPolicy`,
  `WriterNodeId`, `IntraBatchIndex` (per
  `no-version-suffixes.md`).

## Open questions

These defer to bench / implementer-discovery / council /
follow-up ADR. None are contract-level.

1. **Should the payload hash remain in the identity
   preimage?** Write-context-only identity is cheaper and
   still correct; the hash binds identity to row bytes
   for diagnostics. Bench-resolvable — if BLAKE3-on-row
   CPU dominates ingest on representative wide-row
   workloads, drop the hash.
2. **Should `IntraBatchIndex` be `u32` or `u64`?** `u32`
   gives 4 G rows per writer per HLC tick — comfortable
   for any plausible workload but a hard ceiling.
   Synthetic stress tests may prefer the wider unit.
   Default `u32`; council/policy decision.
3. **Should CDC expose an HLC-derived cursor?** Likely
   yes, but belongs to brainstorm 004 / CDC ADR. ADR 045
   does not preempt the choice.
4. **Should append-only `SingleRow` ever be allowed?**
   V1 says no; council can reopen if a low-latency
   stream workload pulls.
5. **Identity-collision response.** A second
   `RecordLocation` for the same `key_identity` is a
   corruption signal; the V1 implementation panics or
   returns a hard error (TBD in Phase 2). Soft-recovery
   modes would require a council-level decision on
   acceptable behavior under cryptographic collision.

## Forward-compatibility analysis

**Future row-delete ADR.** Concrete shape sketch (so the
follow-up has a starting point):

- Add `__row_tombstone__: Bool` synthetic column on
  append-only tables only.
- Add `has_tombstones: bool` reserved bit in the ADR 022
  footer (HMAC scope unchanged — the byte is already
  inside the HMAC range).
- Add per-tenant `TombstoneRetentionFloor` config.
- Add `DeleteRows { row_key_ids: Vec<RowKeyId> }` request
  shape (extends existing write frame).
- Visibility filter: build a per-scan
  `HashSet<RowKeyId>` from rows where
  `__row_tombstone__ = true`; drop both the tombstone
  marker and matching originals.
- Compaction GC: drop tombstones + matching originals
  once HLC drops below the retention floor.
- ADR 044 column-fetch consults `has_tombstones` to skip
  the tombstone buffer on steady-state slabs.

This shape composes with 045 without changing 045's
contract — the synthetic-column extension appends.

**ADR 046 (secondary indexes).** Append-only tables are
prime secondary-index targets — wide schemas, no PK,
scan-heavy lookups. ADR 046's index-entry triple
`(slab_id, record_location, record_row_index)` works
identically against append-only and PK-keyed slabs.

**Brainstorm 004 (CDC).** Append-only is the canonical
CDC sink shape. The 045 write-context identity scheme
gives CDC pipelines a natural per-event identity.
Future CDC may expose a cursor derived from
`(__row_hlc__, __row_key_id__)`.

**Future content-hash identity ADR.** Extends
`RowIdentityPolicy` with a third variant alongside
`PrimaryKey` and `AppendOnly` (concrete name to be
chosen by that ADR with its canonicalization spec).
Existing append-only tables don't migrate; new tables
opt in.

**Future per-table retention ADR.** Pairs naturally with
append-only: `RETENTION = '90 DAYS'` drops rows where
`__row_hlc__ < now - 90d` at compaction time. ADR 042's
single-coherent-compaction primitive carries it.

**ADR 044 column-projected fetch.** Composes unchanged.
Synthetic columns enter the fetch requirement only when
storage-internal paths require them.

**Future LSM compaction.** Append-only segments are LSM-
clean by construction (no overlapping keys, no version
chains). LSM hits append-only first, easiest case.

**Future vector-form identity opcodes** (ADR 042
deferred follow-up). Vector-form `LOAD_IDENTITY` over
`__row_key_id__` works identically on PK-keyed and
append-only sources.

**Logical / physical fingerprint split.** ADR 045 is a
logical change; the split would land the new field on
the logical side. ADR 045 doesn't preempt it.

## Deferred to follow-up ADRs

1. **Row-level tombstones / `DELETE` on append-only
   tables.** Sketch above in *Forward-compatibility
   analysis*. Pulled by GDPR / audit-trail row-delete
   needs.
2. **Content-hash identity (`AppendOnlyContentHash`).**
   Idempotent ingest with deterministic dedupe-on-write.
3. **PK-keyed row delete via supersession.** Different
   semantics; reuses the row-tombstone shape under
   supersession discipline.
4. **Per-table retention policy.** Append-only's natural
   companion.
5. **Public CDC cursor derived from `__row_hlc__` /
   `__row_key_id__`** — brainstorm 004 / CDC ADR
   territory.
6. **Vector-form identity / locality PBC opcodes.**
7. **Append-only `SingleRow` low-latency path** —
   pulled by workload demand.
8. **`COUNT(DISTINCT __row_key_id__)` short-circuit.**
   Trivially `count(*)` for append-only; AGG follow-up.

## References

- ADR 005 / 035 / 036: PBC predicate protocol — composes
  unchanged.
- ADR 012: bounded clock drift — `__row_hlc__` rides ADR
  012 monotonicity. The append-only identity preimage
  consumes the per-row HLC.
- ADR 013: time-travel queries — append-only is
  naturally time-travel-friendly.
- ADR 016: durability model — unchanged.
- ADR 018: dual-CA mTLS identity — `WriterNodeId` is
  bound from the authenticated mTLS node identity.
- ADR 020: L2 envelope encryption — unchanged.
- ADR 022: L2 slab format — unchanged in V1 (the future
  row-delete ADR adds one footer bit).
- ADR 025: audit event model — append-only is the
  natural audit-trail shape.
- ADR 034: Toron-Wendelon integration contract —
  `CreateTable` extends with `row_identity_policy`.
- ADR 037: tenant table schemas — `primary_key` field
  folds into `RowIdentityPolicy::PrimaryKey`; §19
  deferral closes here.
- ADR 038: column-aware locality — composes unchanged.
- ADR 039: slab-footer column statistics — composes
  unchanged.
- ADR 040: aggregate pushdown — CSB short-circuit
  simplifies for append-only sealed slabs.
- ADR 041: project-name decoupling — names follow brand-
  clean discipline.
- ADR 042: analytical multi-row slab fragments —
  synthetic-column trio carries append-only identity
  unchanged; layout policy hard-required;
  supersession axis collapses.
- ADR 043: Arrow IPC body compression — `MultiRow +
  Lz4Frame` is the append-only default by composition.
- ADR 044: L2 columnar fetch — append-only is the
  canonical workload.
- Brainstorm 004 (CDC) — natural sink shape.
- `plans/conventions/newtype-units.md` —
  `WriterNodeId`, `IntraBatchIndex`.
- `plans/conventions/no-version-suffixes.md` — types
  are `RowIdentityPolicy`, `WriterNodeId`,
  `IntraBatchIndex`.
- `plans/conventions/no-project-name-in-protocols.md` —
  metric segments stay brand-clean.
- `plans/conventions/no-premature-backward-compat.md` —
  no `Option<PrimaryKeyDescriptor>` shim; no reserved
  `RowIdentityPolicy` variants; no
  `__row_tombstone__` pre-allocation; no envelope
  version bump.
