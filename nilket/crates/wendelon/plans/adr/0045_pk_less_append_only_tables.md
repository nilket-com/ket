# ADR 045: PK-less Append-Only Tables

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan: `plans/impl/0045_pk_less_append_only_tables.md`

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
introduce a replicated append state surface or a new wire
frame. Phase 8.5 adds one storage-only synthetic column,
`__row_table_object_id__`, as a descriptor-isolation stamp
on multi-row records; it is not append state and is not part
of identity synthesis.

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
- **No stored write-context columns.** `writer_node_id` and
  `intra_batch_index` are consumed by the identity preimage
  and are not stored separately. Recovery rebuilds indexes
  from the stored synthetic identity / HLC / locality
  columns; `__row_table_object_id__` is only the
  descriptor-isolation stamp for matchers and projected
  fetch.
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
- **No new locality policy variant.** AppendOnly V1 uses the
  existing `TableSpreadProjection` shape only. Existing
  `IdentityProjection` and `ColumnProjection` remain valid
  for PK-keyed tables, but are rejected for AppendOnly until
  a future ADR proves retry-stable routing semantics for
  them.
- **No new node capability bit.** Append-only is descriptor-
  driven; mixed clusters serve both shapes identically.
- **No new HRFT frame family.** No new reject-code family.
- **No `ObjectStore` trait change.**
- **No envelope `format_version` bump.** The multi-row IPC
  storage schema gains `__row_table_object_id__` under the
  existing envelope; no slab-envelope version or HRFT frame
  changes.
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

`WriterNodeId` is bound from the **server's** authenticated
mTLS `NodeId` (ADR 018) at admission time, threaded
explicitly into the admission and packing helpers. It is
not read from `meta.proxy_source` (which is zero on direct
SDK writes by wire contract). `IntraBatchIndex` advances
within a single write request, 0-indexed. `IntraBatchIndex`
overflow rejects the entire write before any slab append —
no wraparound, no collision-resolution path. Allocation is
local to the admitting node and the request scope; no
replicated state is consulted.

For a batch of N rows, row `i` receives:

```text
writer_node_id = authenticated writer identity
row_hlc        = leader-merged commit_hlc (ADR 012);
                 the same HLC stored in __row_hlc__
intra_batch    = i (zero-indexed)
```

The leader merges the request's `client_hlc` into its
local HLC driver before admission; that result is the
`commit_hlc` used for both identity synthesis and the
stored `__row_hlc__` column. Because
`HybridLogicalClockDriver::merge` is strictly monotonic
per leader, two distinct batches admitted by the same
node always receive distinct `row_hlc`s — which is the
load-bearing invariant for §4 uniqueness without a
payload hash term.

### 4. `__row_key_id__` is write-context-rooted

For append-only rows:

```text
key_identity = BLAKE3(
    "table.append-row"
    || tenant_id              // 16 B
    || table_object_id        // 16 B
    || writer_node_id         // 16 B
    || row_hlc                // 16 B  (= leader-merged commit_hlc)
    || intra_batch_index_be   // u32 big-endian, 4 B
)
```

The uniqueness root is the
`(writer_node_id, row_hlc, intra_batch_index)` tuple, where
`row_hlc` is the leader-merged commit HLC the admission
path receives from the leader (the same HLC stored in the
row's `__row_hlc__` synthetic column). The HLC driver's
`merge` is strictly monotonic per leader, so two distinct
batches from the same admitting node always receive
distinct `row_hlc`s; identical payloads at distinct write
contexts therefore produce distinct identities without any
payload-hash contribution. Open question 1 (whether to
include the payload hash in the preimage) was resolved
during Phase 7 — see Rationale below.

The domain tag `"table.append-row"` separates this preimage
from ADR 037's PK-derived domain so a constructed PK can
never collide with an append-only key identity at the
BLAKE3 input layer. `intra_batch_index_be` is the big-
endian `u32` representation; this is the wire/format
contract and is unambiguous across implementations.

For PK-keyed tables, ADR 037's PK-derived identity remains
unchanged.

### 4a. Append-only locality is request-derived

Append-only locality is **decoupled from per-row identity**.
Every row in a single write request receives the same
`Locality`, computed once from retry-stable write context:

```text
append_locality_seed = BLAKE3(
    "table.append-locality.v1"
    || tenant_id              // 16 B
    || table_object_id        // 16 B
    || parent_write_id        // 32 B
)

bucket = jump_consistent_hash(seed_u64, spread_factor)
prefix = table_spread_prefix_hash(tenant_id, table_object_id, bucket)
suffix = seed_u64 & suffix_mask
locality = (prefix & prefix_mask) | suffix
```

Why decoupled: identity must depend on `commit_hlc` for
uniqueness, but `commit_hlc` is only known to the leader.
Client-side routing must therefore depend on something the
client and server both see in the request meta —
`parent_write_id` is the natural choice (32 bytes,
retry-stable by definition, already the request's
idempotency key). With locality computed identically on both
sides, client preview and server admission converge by
construction; the lease gate cannot trip on structural
client/server disagreement.

A consequence: each AppendOnly batch lands on exactly one
replica group. Bifurcation across groups is structurally
impossible for AppendOnly. Per-row distribution within the
spread bucket comes from the `suffix` term and from the
spread of distinct `parent_write_id`s across requests.

The stored `__row_routing_locality__` column carries this
per-request locality on every row. Query scan ranges for
AppendOnly table-spread continue to cover the full set of
prefixes returned by `table_spread_prefix_ranges(tenant_id,
table_object_id, spread)` — queries do not know
`parent_write_id`s, so pin no narrower than the spread
prefix space (exactly the existing behavior).

`AppendOnly + LocalityPolicyV1::IdentityProjection` is
forbidden for V1: locality = identity is not retry-stable
under HLC-derived identity. `AppendOnly +
LocalityPolicyV1::ColumnProjection` is also forbidden for
V1 — its retry-stable routing semantics under append-only
identity have not been worked out, and the V1 contract
restricts it accordingly. Descriptor validation rejects
both combinations.

PK-keyed tables retain ADR 037's per-row locality semantics
unchanged.

### 5. Multi-row synthetic-column set adds `__row_table_object_id__`

Every multi-row record (PK + AppendOnly) carries the
ADR 042 synthetic-column trio plus the table-isolation
stamp:

```text
user columns...
__row_hlc__               FixedSizeBinary(16)
__row_key_id__            FixedSizeBinary(32)
__row_routing_locality__  UInt64
__row_table_object_id__   FixedSizeBinary(16)
```

Single-row records continue to carry no synthetic columns
(ADR 042 / 044 contract); descriptor-filtered queries
against single-row records use the whole-record matcher
fallback. The schema validator forbids users from declaring
a column with any of the synthetic names regardless of
identity policy.

`__row_table_object_id__` carries the descriptor's
`table_object_id` stamped on every row of a multi-row
record. The all-or-none stamping invariant is enforced by
the encoder: either every row in a record carries the
column with the same value, or no row does.

The descriptor-match helpers
(`table_record_matches_descriptor`,
`descriptor_matches_tiered_row`) verify this column equals
the candidate descriptor's `table_object_id`. For
AppendOnly the verification is the only descriptor binding
(synthesized identity is HLC-derived, so re-derivation
cannot rule out a sibling AppendOnly table with the same
schema and a colliding table-spread prefix). For PK the
column is an early-fail before the (more expensive) PK
re-derivation. The projected fast path uses a uniform
byte-compare against this column for both identity
policies, avoiding PK re-derivation cost for
descriptor-filtered queries on the projected path.

Without this column, query, reclustering, stats, and LZ4
candidate-detection paths on AppendOnly tables would let
same-tenant same-shape descriptors with overlapping
table-spread prefixes leak rows across descriptors.

The write-context components (`writer_node_id`,
`intra_batch_index`) are not stored as separate columns.
Recovery does not need them: it reads the four synthetic
columns directly from the IPC body and rebuilds the same
redb indexes ADR 042 already requires. Compaction
preserves those synthetic values exactly per row and never
recomputes append-only identity for surviving rows.

### 6. Append-only locality is table-spread only in V1

Append-only tables use existing `LocalityPolicy` machinery,
but V1 accepts only `LocalityPolicyV1::TableSpreadProjection`.
The descriptor validator rejects `AppendOnly +
IdentityProjection` and `AppendOnly + ColumnProjection`.

Reason: append-only identity is rooted in the leader-merged
`commit_hlc`, which the client cannot know during routing.
Any locality policy that depends on per-row identity or on a
row-local computation risks client-preview / server-admission
divergence. `TableSpreadProjection` composes with §4a because
the spread seed is the retry-stable request identity
(`parent_write_id`) and every row in the request receives the
same locality.

The default remains a *create-time resolution rule*, not a
`#[serde(default)]` compatibility shim: when an `AppendOnly`
descriptor omits locality, the create-table path writes the
resolved `TableSpreadProjection { spread_factor: 4,
prefix_bits: 16 }` value into the persisted descriptor before
fingerprinting. Serialized descriptors carry an explicit
locality policy on the wire and at rest. Schema-load decode
never silently fills in defaults.

No `AppendOrderProjection` is introduced. Event-time locality
for append-only tables is future ADR territory; V1 does not
silently infer it from column names or `CLUSTER BY` syntax.

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
structurally unchanged from ADR 044. Phase 8.5 adds
`__row_table_object_id__` to descriptor-filtered projected
fetch requirements so the fast path can verify table
isolation before returning; other synthetic columns enter
the requirement only when storage-internal paths require
them.

### 14. Time-travel composition (ADR 013)

`lookup_as_of(t)` over an append-only table returns rows
where `__row_hlc__ <= t`. Strictly simpler than PK-keyed
time-travel because there are no version chains to walk —
each row is its own independent fact.

### 15. Recovery uses stored synthetic columns

Recovery from raw slab bytes already reads synthetic
columns directly (ADR 042 §3 / 043 §8). For append-only:

- `__row_key_id__` is read directly (no PK
  canonicalization);
- `__row_routing_locality__` is read directly (no
  recomputation);
- `__row_table_object_id__` is read directly when matcher /
  maintenance paths need descriptor isolation;
- no append-counter state to reconcile (because none
  exists).

Recovery is, if anything, *easier* than PK-keyed because
no PK schema knowledge is needed to rebuild the identity and
locality indexes.

### 16. Toron-side surface

Toron's `CREATE TABLE` syntax adds an `APPEND ONLY`
clause:

```sql
CREATE TABLE events (
    event_time   TIMESTAMP,
    payload      BINARY,
    ...
) APPEND ONLY;
```

`APPEND ONLY` translates to
`RowIdentityPolicy::AppendOnly` on the descriptor.
`PRIMARY KEY` plus `APPEND ONLY` is a parse error.
`APPEND ONLY` uses the V1 default table-spread locality
(`TableSpreadProjection { spread_factor: 4, prefix_bits:
16 }`). `CLUSTER BY` / `ColumnProjection` for append-only is
rejected in V1 unless a future ADR proves retry-stable
routing for it.

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
   context, receives the leader-merged request HLC (ADR 012),
   and assigns per-row `IntraBatchIndex` values;
5. computes per-row key identities under the V1
   preimage (no payload-hash term);
6. computes request-level table-spread locality from
   canonical `parent_write_id`;
7. appends the ADR 042 synthetic trio plus
   `__row_table_object_id__`;
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

**Why the identity preimage excludes the row payload
hash (resolved during Phase 7).** Earlier drafts folded
a canonical row payload hash into the BLAKE3 preimage as
defence-in-depth. Phase 7 benches showed payload hashing
dominated wide-row admission CPU: per-row admission gap
between hashed and unhashed paths was ~0.011 µs at 0 B,
~0.013 µs at 32 B, ~0.229 µs at 256 B, and ~0.879 µs at
1024 B; identity-synthesis stayed flat at ~0.127 µs.
Linear scaling with payload size proved BLAKE3-on-payload
dominated, while the hash itself contributed nothing to
uniqueness — uniqueness is fully rooted in
`(writer_node_id, row_hlc, intra_batch_index)` because
the leader-merged commit HLC is strictly monotonic per
admitting node (ADR 012). The "diagnostic / replay
detection" motivation for keeping the hash was outweighed
by the cost on hot-path ingest, and the same diagnostic
information is available out-of-band via stored row bytes
when needed. Open question 1 is closed.

**Why one new storage-only synthetic column
(`__row_table_object_id__`).** Once identity does not
depend on a mutable table counter, recovery does not need
to reconstruct write-context material — the ADR 042 trio
suffices for that. Phase 8 added one additional column
strictly for descriptor isolation against cross-table
table-spread prefix collisions. Without it, AppendOnly
identity (HLC-derived) cannot be re-bound to a descriptor
at query time, and PK descriptor-filtered projected
queries pay the cost of full PK re-derivation per row.
Stamping the column on every multi-row record (PK +
AppendOnly) lets the projected fast path verify
table-isolation with a 16-byte byte-compare. Constant-
column compression keeps steady-state storage cost
negligible (one value repeated across all rows of a
record). Single-row records carry no synthetic columns
and fall back to the whole-record matcher.

**Why `MultiRow` is hard-required.** Append-only's read-
side optimizations (CSB short-circuit, ADR 044 column
fetch) materially require multi-row. Single-row append-
only would need a second recovery / packing branch for
no workload win. Tenants who want single-row writes have
PK-keyed tables.

**Why `TableSpreadProjection` is the only allowed locality
for AppendOnly V1.** ADR 037's typed-table default is
`TableSpreadProjection { spread_factor: 4, prefix_bits: 16 }`;
append-only inherits the same defaults. AppendOnly identity
is `commit_hlc`-derived and therefore not known to the
client at routing time, so per-row identity-derived locality
(`IdentityProjection`, `ColumnProjection`) cannot give
client-side routing a stable answer. AppendOnly resolves
this by computing locality from retry-stable write context
(`parent_write_id`) under §4a — a value the client and
server both see in the request meta. Descriptor validation
rejects `AppendOnly + IdentityProjection` and
`AppendOnly + ColumnProjection` for V1; their retry-stable
routing semantics under HLC-derived identity are out of
scope for this ADR.

**Why request-level locality, not per-row.** AppendOnly
ingest is batch-shaped and scan-read; clustering rows of
one logical write together is the natural shape. The
spread bucket varies per `parent_write_id`, so distinct
writes still distribute across the table-spread prefix
space. Per-row locality variation would re-introduce the
client/server divergence the §4a contract exists to
eliminate.

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
  A secondary-index follow-up candidate can fill the gap
  for index-by-non-PK-column queries on append-only tables
  if council prioritizes that path.
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
- **`key_identity = BLAKE3(... || row_payload_hash)`**
  (earlier draft kept a canonical row payload hash in
  the preimage as defence-in-depth). Phase 7 benches
  showed BLAKE3-on-payload dominated wide-row admission
  CPU and the hash contributed nothing to uniqueness;
  the preimage now omits it. See Rationale and Open
  question 1.
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
  Rejected: AppendOnly V1 intentionally has one
  retry-stable table-spread locality shape. Event-time
  clustering for append-only needs a future ADR that proves
  client/server routing convergence.
- **Default locality = `IdentityProjection`.** Rejected:
  scatters scans across the whole ring, defeats ADR 037's
  table-arc-bounded-scan property when the tenant
  intended a scan-shaped workload.
- **Explicit-required locality / no default.** Rejected:
  ADR 037's `TableSpreadProjection` default applies
  cleanly via the request-derived `parent_write_id` salt;
  making append-only an exception introduces an
  inconsistency with PK-keyed tables that doesn't earn its
  keep.
- **Forbid `TableSpreadProjection` on append-only.**
  Rejected: §4a defines a retry-stable request-derived
  spread salt; the spread machinery works without using
  HLC-derived identity for client-side routing.
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

1. ~~**Should the payload hash remain in the identity
   preimage?**~~ **Resolved (Phase 7, 2026-04):** dropped.
   Phase 7 benches showed BLAKE3-on-payload dominated
   wide-row admission CPU (per-row gap scaled linearly
   with payload size: ~0.011 µs at 0 B → ~0.879 µs at
   1024 B), while identity-synthesis stayed flat at
   ~0.127 µs. Payload hash contributed nothing to
   uniqueness (which is rooted in `(writer_node_id,
   row_hlc, intra_batch_index)` given monotonic leader
   commit HLC), so it was removed from the preimage.
   `canonical_row_payload_hash` and the
   `storage_append_only_payload_hash_micros` metric were
   both deleted. See §4 and Rationale.
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

**Secondary indexes.** Append-only tables are prime
secondary-index targets — wide schemas, no PK, scan-heavy
lookups. The index-entry triple
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
