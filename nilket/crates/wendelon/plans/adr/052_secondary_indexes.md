# ADR 052: Secondary Indexes on Non-Clustering Columns

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/052_secondary_indexes.md`

## Status

Accepted

## Context

Wendelon today supports exactly one access path on a table:
locator lookup by `__row_key_id__` (the PK index for
`PrimaryKey` tables; the content-derived identity index for
`AppendOnlyContentHash`; the write-context-derived identity
index for `AppendOnly`). Predicate evaluation (PBC) and
aggregates fall back to scanning every slab that PBC bloom (if
any) does not exclude, with CSB direct-answer and direct-merge
as the only cross-slab pruning.

Roadmap #8 opens the second access path: a secondary index
keyed on a non-`__row_key_id__` column.

The roadmap places this immediately after ADR 051 on purpose:

- **From ADR 051:** index entries bind to the table's
  `LogicalSchemaFingerprint`. A physical retune (compression,
  layout, stats policy) does not invalidate index segments.
- **From ADR 050:** the row-axis version chain plus
  `lookup_as_of(K, T)` is the authoritative supersession
  resolver. The index trusts it; the index does not duplicate
  that discipline on its own axis.
- **From ADR 049:** content-hash tables are first-class index
  citizens. Their `__row_key_id__` is fingerprint-free and
  stable across additive evolution.
- **From ADR 048:** index sidecars respect the table's
  retention floor.
- **From ADR 047:** tombstones live in the row chain; the
  index axis observes them through `lookup_as_of`, not
  through its own marker.
- **From ADR 046:** index sidecars carry a `StateDigest` from
  day one under the reserved `digest.secondary_index.entry.v1`
  domain. Replica reconciliation per sidecar is an O(1) point
  comparison.

Three concerns shape the surface:

- **A second access path exists.** Equality, range, and
  IN-list predicates on indexed columns avoid full slab
  scans for the covered fraction of the table.
- **The index is a derived accelerator.** Backfill failure
  does not break write admission. Partial coverage does not
  break correctness — uncovered slabs fall through to the
  existing row path.
- **The index is sharded, replicated, time-travel-capable,
  and compaction-aware.** It composes with every lifecycle
  and identity discipline already shipped.

The constraints from `no-premature-backward-compat.md`,
`newtype-units.md`, `no-version-suffixes.md`,
`no-project-name-in-protocols.md`,
`capability-vs-feature-naming.md`, and ADR 046 §7 all hold.

## Identity-vs-Locality framing at the index axis

Per `plans/philosophy.md`, every record has an Identity (what
it IS) and a Locality (where it LIVES). The PK index gives a
row a locality on its identity axis: `__row_key_id__` → slab
placement.

V1 secondary indexes do **not** give the row a second locality
on the ring. The sidecar model places index entries at the
source slab's locality; range scans iterate sidecars across
slab localities. Routing is by the row's existing locality
(the slab's), not by the indexed value. A future ADR may
introduce a true second locality (range-sharded by indexed
value via a range-aware ring projection); V1 stays in the
existing hash-by-row-locality projection.

The roadmap's "physical locator entries vs logical index
membership" distinction survives the sidecar model intact, with
three concepts:

- A **physical locator entry** is the
  `(record_location, record_row_index)` pointer on a sidecar
  entry (the source slab's `slab_id` is implicit in the
  sidecar binding). It is the fetch primitive. Compaction
  rewrites locators independently of any logical fact;
  locators are excluded from the digest preimage so replicas
  reconcile across independent rewrites.
- A **logical version-entry** is the tuple
  `(indexed_value, row_key_id, row_hlc)` (under a fixed
  `(tenant_id, table_object_id, index_object_id,
  logical_schema_fingerprint, column_ordinal)`). One per
  data-row admission. The `StateDigest` is over these.
- A **visible membership** at time `T` is the projection of
  version-entries through `lookup_as_of(K, T)` plus
  indexed-value recheck plus PBC recheck. V1 does **not**
  make visible membership a durable artifact; it computes it
  at read time. (A future "visible-membership" digest is a
  follow-up consumer; ADR 052 commits to the version-entry
  digest only.)

The two-concept distinction the roadmap brief calls out is
preserved: the version-entry digest is logical (compaction-
stable; replica-comparable); locators are physical (compaction-
mutable; per-replica). What V1 does **not** ship is a
visible-membership digest — that requires a synchronous index
mutation discipline the lazy model deliberately avoids. ADR
050's row-axis visible-membership digest plus mandatory
read-time projection is sufficient for V1 correctness; a
follow-up may add a visible-only index digest if anti-entropy
demands it.

A row with N data versions on the table produces N version-
entries across (up to N) sidecars. At time `T`, at most one of
those N entries projects to a visible answer. Read-time
validation collapses the N candidates to the one visible
answer.

## Non-goals (V1)

- **No multi-column composite indexes.** Composite preimage,
  prefix planning, and mixed range constraints are their own
  ADR.
- **No expression / functional indexes.** No `lower(col)`-
  style indexed expressions; the indexed value is the
  column's stored value under the existing canonical
  encoding.
- **No partial / filtered indexes** (`WHERE` predicate on the
  index).
- **No covering indexes.** Index entries do not store
  non-indexed columns. Index lookup yields a candidate
  locator; the reader fetches the row through the existing
  data path.
- **No unique-constraint enforcement.** Uniqueness needs
  cross-replica admission ordering beyond an access-path
  ADR.
- **No full-text / inverted indexes.**
- **No ANN / vector indexes.** Roadmap #12.
- **No cost-based optimizer.** V1 ships predicate extraction
  with a hybrid threshold; cost-based optimizer is a
  follow-up.
- **No indexing on the declared PK column** of a `PrimaryKey`
  table.
- **No indexing on storage synthetic columns** (`__row_*__`).
- **No float / decimal / dictionary / nested / list / struct
  / map / union indexed columns** in V1.
- **No locale collation / case folding / NFC / total-order
  float encoding** in V1. Binary ordering on already-
  canonicalized bytes only.
- **No NULL-included mode in V1.** Rows whose indexed
  column is NULL emit no sidecar entry. `IS NULL`
  predicates route to scan. A future variant ADR may define
  NULL ordering (NULL-first vs NULL-last) when a consumer
  demands it.
- **No visible-membership digest on the index axis in V1.**
  ADR 052 ships a logical version-entry digest; visible-
  membership reconciliation is a follow-up consumer if
  anti-entropy demands it.
- **No synchronous index updates blocking write admission.**
  Active-slab buffer append is best-effort; failure marks
  the slab uncovered without rolling back the data write.
- **No global ready-state replication.** Coverage is a per-
  replica fact; cluster-wide ready semantics are a follow-up.
- **No DDL parser.**
- **No new HRFT frame family.** Index create / drop reuses
  `SchemaUpdateEnvelopeV1` with `SchemaObjectKind::Index`.
  Index lookup is internal; PBC predicate-driven query
  frames are unchanged.
- **No new node capability bit.**
- **No `ObjectStore` trait change.**
- **No bump on any existing `format_version`** (envelope,
  data-slab footer, CSB header, table descriptor, or any
  other pre-ADR-052 artifact). The new per-source-slab
  index sidecar is a *fresh* artifact family and carries
  its own `format_version` field in its header (§4); that
  field is new, not a bump on an existing one.
- **No table descriptor field for indexes.** Index metadata
  lives in a separate replicated `SecondaryIndexCatalog`.
- **No table `schema_generation` bump** on index create /
  drop.
- **No new descriptor-axis fingerprint domain.** Index
  entries bind directly to the table's
  `LogicalSchemaFingerprint`.
- **No separate index logical-fingerprint newtype.** No
  stacked-preimage construction. The descriptor cross-bind
  is `(index_object_id, table_object_id,
  logical_schema_fingerprint, key descriptor)`.
- **No new digest domain beyond
  `digest.secondary_index.entry.v1`** (ADR 046 reservation).
- **No durable digest placement on data slabs.**
- **No reserved enum slots, compatibility shims, or `V1` /
  `V2` Rust type suffixes** for any new ADR 052 type.
- **`Alter` rejected.** Drop + create with new
  `IndexObjectId`.
- **No append-only-style aggressive pair-GC** on index
  sidecars.
- **No full-chain key-retirement proof.**
- **No anti-entropy reader / fanout consumer.**
- **No CDC index-axis event surface.**

## Decision

### 1. Index descriptors are separate schema objects

Secondary indexes are created and dropped through the existing
`SchemaUpdateEnvelopeV1`. ADR 052 introduces four index-axis
mutation variants on `SchemaObjectKind::Index`:

```text
object.kind = SchemaObjectKind::Index
mutation    = Create
            | Drop
            | IndexBackfillComplete
            | IndexDropGcComplete
```

`Create` and `Drop` are operator-initiated; the catalog
state machine accepts them on the schema-mutation reply
path. `IndexBackfillComplete` and `IndexDropGcComplete` are
internal lifecycle-advancing mutations that replicas submit
themselves to advance catalog state through the linear
lifecycle (see below). They reuse the
`SchemaUpdateEnvelopeV1` carrier — same Raft path, same
audit shape — instead of inventing a parallel mutation
family. `Alter` is rejected; see §13.

The payload for `Create` is a new postcard-serializable
descriptor:

```rust
pub struct SecondaryIndexDescriptor {
    pub index_object_id:           IndexObjectId,
    pub table_object_id:           TableObjectId,
    pub namespace:                 String,
    pub index_name:                String,
    // Provenance only. Planner compatibility binds on
    // logical_schema_fingerprint.
    pub table_schema_generation:   u64,
    pub logical_schema_fingerprint: LogicalSchemaFingerprint,
    pub key:                       SecondaryIndexKeyDescriptor,
}

pub struct SecondaryIndexKeyDescriptor {
    pub column_name:    String,
    pub column_ordinal: u32,
    pub arrow_type_tag: IndexArrowTypeTag,
    pub encoding:       IndexKeyEncoding,
}
```

`IndexObjectId([u8; 16])` is a distinct newtype per
`newtype-units.md`; not a `TableObjectId` alias.

`SecondaryIndexCatalog` is replicated in the control-ring
state machine beside `TableSchemaCatalogV1` and
`TableLifecycleCatalog`, keyed by
`(tenant_id, index_object_id)` and indexed by
`(tenant_id, namespace, index_name)` and by
`(tenant_id, table_object_id)`. Catalog rows carry a
lifecycle state:

```rust
pub enum SecondaryIndexState {
    Building,
    Ready,
    Dropping,
}
```

`Building` descriptors exist; admission emits to the active-
slab buffer; backfill processes historical slabs; planner
**ignores** `Building` descriptors (no read use until
backfill completes). `Ready` descriptors are eligible for
query planning subject to local coverage. `Dropping`
descriptors reject new admissions and reads while sidecars
are reclaimed. The absent state is represented by no catalog
row.

Lifecycle transitions are linear:

```text
absent -> Building -> Ready -> Dropping -> absent
```

Backfill completion is a replicated `IndexBackfillComplete`
transition from `Building` to `Ready`. Drop-GC completion
removes the catalog row. There is no `Ready -> Building`
transition.

`Alter` is rejected. A changed index definition is drop +
create with a new `IndexObjectId`. The reject lives at the
schema-mutation layer (see §13).

`table_schema_generation` is provenance from the create-time
descriptor snapshot. Planner compatibility binds on
`logical_schema_fingerprint`.

Index create validates:

- referenced table exists;
- descriptor's `table_schema_generation` and
  `logical_schema_fingerprint` match the current table
  descriptor at create time;
- column exists at `column_ordinal` and matches the declared
  `arrow_type_tag`;
- column is a user column, not a `__row_*__` synthetic
  column;
- on `RowIdentityPolicy::PrimaryKey`, the column is not the
  declared PK column;
- Arrow type is in the V1 supported set
  (`IndexKeyEncoding::OrderedScalar`);
- index name is unique within `(tenant_id, namespace)`.

Index create / drop does **not** mutate
`TableSchemaDescriptorV1`, **does not** change either
descriptor fingerprint, and **does not** bump table
`schema_generation`.

### 2. V1 scope: one ordered scalar non-PK non-synthetic column

The first index shape is a non-unique ordered scalar index
over one column. Supported indexed Arrow types:

- Boolean.
- Signed and unsigned integers (`Int8`…`Int64`,
  `UInt8`…`UInt64`).
- `Date32` / `Date64` and `Timestamp(*)` integer-backed
  types (already normalized by the table schema).
- `Utf8` / `LargeUtf8` (binary UTF-8 byte ordering).
- `Binary` / `LargeBinary` (raw bytes).

Unsupported types reject at descriptor validation with
`INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE`. Float / decimal /
dictionary / nested types are deferred.

NULL is excluded from the V1 index. A row whose indexed
column is NULL produces no sidecar entry. Predicates such as
`IS NULL` use the existing scan path. A future NULL-included
variant can define sentinel ordering explicitly when a
consumer demands it.

V1 uses binary ordering only. Locale collation, case folding,
NFC normalization, decimal scale rules, and float total-order
semantics are follow-up work.

### 3. Order-preserving canonical key encoding

`IndexKeyEncoding::OrderedScalar` converts each supported
non-NULL Arrow value to a byte string whose lexicographic
order equals the storage comparison order:

```text
bool                    : 0x00 false, 0x01 true
unsigned integer        : big-endian bytes
signed integer / date / timestamp
                        : big-endian bytes with sign bit flipped
UTF-8 / binary          : raw bytes, length-prefixed by
                          segment-reader framing
```

NULL values produce no sidecar entry; no sentinel byte is
added in V1. `IS NULL` predicates fall through to the scan
path.

The segment sort key per entry is:

```text
(encoded_key, row_key_id, row_hlc, record_location, record_row_index)
```

The locator tail is tie-breaker only and is **excluded**
from the digest preimage (§5).

### 4. Index sidecars are per-source-slab

Index segments are immutable per-source-slab sidecars built
per:

```text
(tenant_id, table_object_id, index_object_id,
 logical_schema_fingerprint, source_slab_id)
```

One sidecar per source slab per index. The conceptual
locator triple from the roadmap brief is preserved at the
contract level: `(slab_id, record_location, record_row_index)`.
`slab_id` is stored once in the sidecar header rather than
repeated per entry, since a V1 sidecar is bound to one source
slab.

`record_row_index` widens to `u32`. **If the existing
`RecordLocation` already carries `record_row_index`**
(current `src/storage/manager.rs` shape), ADR 052 widens
that field in place rather than duplicating it on the
segment entry. **If `RecordLocation` does not carry the
field**, the segment entry adds
`record_row_index: RecordRowIndex` directly. In both cases
the per-entry digest excludes the locator fields (§5), and
the segment header binds the sidecar to the source slab.

Segment header records:

- segment magic and `format_version`;
- tenant id, table id, index id, source slab id;
- `table_schema_generation` (provenance) and
  `logical_schema_fingerprint` (binding);
- indexed `column_ordinal` and `arrow_type_tag`;
- entry count;
- min / max encoded key;
- segment `StateDigest` (over logical version-entries);
- segment payload hash.

Entries are stored sorted by §3's sort key. No bytes are
added to the slab footer. No CSB header field is reused.
Sidecars live beside the source slab in L1 and may tier to
L2 through existing object-store primitives. The local
persistent index maps
`(tenant, table, index, fingerprint, slab_id) → segment
pointer`.

The sidecar model implies:

- Range scan over indexed_value iterates sidecars across
  slabs (one per slab), pruning by per-sidecar (min_key,
  max_key).
- Point-equal lookup also iterates sidecars; per-sidecar
  bloom or min/max filtering is the V1 pruning mechanism.
- Compaction is automatic when the source slab compacts:
  the sidecar regenerates from the compacted slab's
  surviving rows.
- Tiering is automatic when the source slab tiers.
- Recovery is automatic when the source slab replays.

### 5. Per-segment `StateDigest` over logical version-entries

Domain: `digest.secondary_index.entry.v1` (ADR 046
reservation; `DOMAIN_SECONDARY_INDEX_ENTRY` already exists at
`src/hash/state_digest.rs:19`).

Element preimage:

```text
"digest.secondary_index.entry.v1"
  || tenant_id                  (u64 LE)
  || table_object_id            (16 bytes)
  || index_object_id            (16 bytes)
  || logical_schema_fingerprint (32 bytes)
  || column_ordinal             (u32 LE)
  || encoded_key_len            (u64 LE)
  || encoded_key                (bytes)
  || row_key_id                 (32 bytes)
  || row_hlc.physical_ms        (u64 LE)
  || row_hlc.logical            (u32 LE)
  || row_hlc.node_id            (u32 LE)
```

Excluded:

- `slab_id`, `record_location`, `record_row_index`. Replicas
  rewrite locators independently.
- `table_schema_generation`. Per ADR 051: generation is the
  ordering primitive; logical fingerprint is the structural
  binding.

This is intentionally a **logical version-entry digest**, not
a visible-membership digest. A PK upsert that keeps the
indexed value unchanged still writes a new data version with
a new payload locator; if the index did not add a version
entry for that write, an index lookup after the upsert could
find only the old locator and would have no direct pointer
to the latest row payload — a silent miss. V1 keeps direct
locators and solves supersession by read-time validation
plus compaction cleanup. A future ADR may add a visible-only
membership digest as a separate consumer; ADR 052 does not
allocate that domain.

### 6. Lazy supersession via mandatory candidate revalidation

The index is a **candidate source**, not a visibility
authority. Tombstones produce no index entry. Older data
versions and updates with changed indexed values produce
additional version-entries; the index does NOT subtract
anything synchronously.

Every candidate from an index range scan is revalidated:

1. Segment `logical_schema_fingerprint` must equal the
   current query descriptor's
   `logical_schema_fingerprint`.
2. Fetch candidate row through `(slab_id, record_location,
   record_row_index)`.
3. Decoded row's `(__row_key_id__, __row_hlc__)` must match
   `(row_key_id, row_hlc)` from the index entry.
4. `lookup_as_of(tenant_id, row_key_id, as_of_hlc)` must
   return the candidate's exact `(row_key_id, row_hlc)` and
   not be a tombstone.
5. The fetched row's *current indexed value* (under the
   original canonical encoding) must equal the entry's
   `encoded_key`. If the row updated the indexed value, the
   candidate is stale.
6. The full PBC predicate must hold on the fetched row.

Step 4 closes the supersession case (data → data; data →
tombstone; tombstone → data resurrection). Step 5 closes
the indexed-value-mutation case.

Worked examples:

- **Insert.** Row K at hlc=10 with col=foo. Sidecar for
  K's slab: `(foo, K, 10, locator_v1)`. Range over
  col='foo' at any T ≥ 10: returns locator_v1.
- **Indexed-value-changing upsert.** Row K updated at
  hlc=20 with col=bar. Sidecar (or new sidecar):
  `(foo, K, 10, locator_v1)` and `(bar, K, 20, locator_v2)`.
  Range over col='foo' at as_of=now: candidate v1 → step 4
  returns v=20 row → step 4 mismatch (hlc 10 ≠ 20) →
  reject. Range over col='bar' at as_of=now: candidate
  v2 → step 4 returns v=20 row → step 5 col=bar matches →
  accept.
- **Unchanged-value upsert.** Row K updated at hlc=20 with
  col=foo. Sidecar: `(foo, K, 10, locator_v1)` and
  `(foo, K, 20, locator_v2)`. Range over col='foo' at
  as_of=now: candidate v1 → step 4 returns v=20 row → hlc
  mismatch → reject. Candidate v2 → step 4 returns v=20 →
  step 5 col=foo matches → accept. **The new version-entry
  is load-bearing**: without it, the index has no pointer
  to the v=20 row and the query misses K entirely.
- **Delete.** Row K deleted at hlc=20 (tombstone in row
  chain; no index entry). Range over col='foo' at
  as_of=now: candidate v1 → step 4 returns tombstone →
  reject.
- **Resurrection.** Row K resurrected at hlc=30 with
  col=foo. Sidecar: new entry `(foo, K, 30, locator_v3)`.
  Range over col='foo' at as_of=now: candidate v3 → step 4
  returns v=30 → step 5 col=foo matches → accept.

The version-entry digest accounts for every data-row write
exactly once (no de-dup of unchanged-value upserts). The
trade-off vs visible-membership: more storage amplification
on update-heavy workloads, paid back when the source slab
compacts (compaction reads survivor data rows; obsolete
versions are naturally omitted; the rebuilt sidecar is
purged of stale candidates).

### 7. Coverage is a per-replica fact

Each replica tracks coverage locally for each
`(tenant_id, table_object_id, index_object_id,
logical_schema_fingerprint)`:

- covered sealed slab ids with valid sidecars;
- active-slab buffered entries (in-memory until seal);
- known uncovered slab ids (backfill pending or failed);
- last backfill error.

The query planner uses an index only when the descriptor is
in `Ready` state AND the slab is covered locally. Uncovered
slabs route to the existing row path. Hybrid plan:

```text
indexed candidates from covered sidecars + active-slab buffer
  + row scan over uncovered slabs / active-slab tail
  → step 1 fingerprint match
  → step 2 fetch
  → step 3 row-metadata equality
  → step 4 lookup_as_of
  → step 5 indexed-value recheck
  → step 6 PBC recheck
  → result
```

If the uncovered fraction is large, the planner may choose a
full scan. This is a cost decision, not a correctness
decision.

Index create does **not** drain table writes. Backfill is an
asynchronous per-replica worker that scans existing sealed
slabs and emits sidecars. **Backfill iterates every retained
non-tombstone data version in the slab**, not just the rows
visible at backfill time. This matches the steady-state
admission rule (one entry per data-row write) applied
retroactively to history, so that an `as_of_hlc` query
reaching a covered slab finds the version-entry that was
visible at the requested time — equivalent to what the index
would have produced if it had existed when the data was first
written.

Active-slab admission appends to an in-memory buffer bounded
by `max_index_buffer_bytes` (operator tunable); on slab seal,
the buffer flushes into a sidecar.

If buffering fails for any reason — overflow against
`max_index_buffer_bytes`, encode error, allocation failure
— the buffer is dropped, the slab is marked uncovered, and
the planner scans that slab. **Data writes succeed
regardless of index buffer state**; correctness is
preserved by scan fallback.

Backfill must coordinate with compaction: the worker
acquires a read-lease on the source slab during build. If
compaction claims the slab before backfill seals the
sidecar, backfill aborts and the slab is re-enqueued
against the compaction output. This avoids orphaned
sidecars whose source slab no longer exists.

Replicas converge coverage independently.

### 8. Schema evolution invalidates by logical fingerprint

An index descriptor binds to one
`logical_schema_fingerprint`. Physical-only descriptor
mutations (layout / compression / stats policy) do not
change the table's logical fingerprint, so existing
sidecars stay usable — the property ADR 051 §3 set up.

Logical mutations (column add / type change / row-identity
change) produce a different `logical_schema_fingerprint`.
Existing sidecars no longer match the table descriptor; the
planner ignores them. A new index descriptor with the new
logical fingerprint must be created (drop + create) and
backfilled. V1 does not attempt per-column survival across
additive evolution; that is deferred to a future per-column
or structural fingerprint.

### 9. Compaction, retention, tiering, and coverage healing

Compaction rebuilds index sidecars from survivor data rows
after tombstone, supersession, and retention filtering.
Because obsolete entries (tombstoned or overwritten rows)
are naturally omitted from the survivor data rows, the
rebuilt sidecar is automatically purged of stale
candidates.

Rules:

- **Append-only / content-hash pair-GC** (ADR 047 / 049):
  rebuilt sidecar excludes pair-GC'd data entries.
- **PK prefix retention** (ADR 013 / 050): rebuilt sidecar
  indexes the retained data boundary if the boundary is
  data; excludes it if the boundary is a tombstone.
  ADR 048's `RETENTION_REJECTED_PK_DISALLOWED` remains in
  force.
- **Older superseded PK data versions** retained for time
  travel above the floor remain in sidecars; revalidation
  decides visibility at `as_of`.

**Coverage healing through compaction.** When compaction
processes an *uncovered* source slab (e.g., because the
active-slab buffer overflowed during admission), the
rebuilt output slab automatically gets a freshly built
sidecar. Compaction does not need to know whether the
input slab was covered; it always emits a sidecar for
the output. This makes uncovered slabs self-healing on
the natural compaction cadence, without requiring a
separate backfill retry path.

**Compaction rebuilds for `Building` and `Ready` indexes
alike.** A `Building` index's backfill may lose its
read-lease race against compaction (§7); without
compaction-time rebuild for `Building` indexes, the
output slab would stay uncovered until backfill's next
pass — racing the compactor again. Compaction-time
emission for both states closes the race cleanly:
either backfill seals the input slab's sidecar, or
compaction seals the output slab's sidecar. Either way
the slab ends up covered without retry loops.
`Dropping` indexes do not get a rebuild — Phase 7 GC
reclaims their bytes instead.

Rebuilt sidecar digest is verified against a cold rebuild
before retiring the old sidecar. Tiering moves sidecars
alongside source slabs.

### 10. Two-phase drop GC

Drop is a two-phase replicated process to prevent
crash-leaks on the L1/L2 sidecar bytes:

1. **Drop mutation accept** transitions the catalog row
   to `Dropping`. Effects:
   - new admissions stop emitting for the dropped
     `index_object_id`;
   - reads against the index reject through fingerprint
     mismatch;
   - in-progress backfill aborts.
2. **Background GC pass** scans local sidecar metadata
   for `Dropping` descriptors and deletes the
   corresponding L1 bytes; emits tier-aware deletes for
   L2 bytes. The pass is idempotent and crash-safe — a
   crash mid-GC leaves the catalog row in `Dropping`,
   and the next GC pass resumes.
3. **Per-replica `IndexDropGcComplete` confirmations.**
   Once a replica's local GC completes, it submits an
   `IndexDropGcComplete` mutation. The catalog row goes
   absent only after **every** replica has confirmed
   local GC by replicating its own
   `IndexDropGcComplete`. A single replica's
   confirmation is not enough — if the catalog were
   retired on the originating replica's confirmation,
   slow or partitioned replicas might still hold
   sidecar bytes when the descriptor goes absent,
   reintroducing the crash-leak the two-phase design is
   meant to prevent.

Without the explicit two-phase shape, a single-mutation
drop that removes the catalog row first could leak
sidecar bytes if the GC pass crashes; the all-replica
confirmation requirement keeps the `Dropping` marker in
the replicated catalog until reclaim is provably done
on every replica.

### 11. Planner integration (minimal V1)

The planner inspects the PBC program and the local index
catalog. It may choose a secondary index when the PBC
program contains a single-column constraint on an indexed
column expressible as:

- equality (`col = v`);
- inclusive / exclusive range (`col ≥ a AND col < b`);
- IN-list (`col ∈ {v_1, …, v_k}`).

The index narrows candidates only. The full PBC program
runs after fetch.

Planner selection is heuristic in V1:

- no `Ready` descriptor or no extractable predicate → full
  scan;
- coverage below the local threshold → full scan or hybrid
  plan based on uncovered-slab fraction;
- estimated predicate selectivity below the runbook
  threshold → indexed or hybrid plan;
- otherwise → full scan.

Thresholds are benchmark-calibrated operator tunables, not
a cost-based optimizer.

No query wire field is added. Clients express predicates
through PBC.

### 12. Wire and lookup surface

Index create / drop reuses `SchemaUpdateEnvelopeV1` with
`SchemaObjectKind::Index`. No new HRFT frame family.

Internal typed lookup executor (used by the planner, not
exposed as a client surface in V1):

```rust
pub struct TableLookupByIndexRequest {
    pub tenant_id:                  TenantId,
    pub table_object_id:            TableObjectId,
    pub index_object_id:            IndexObjectId,
    pub logical_schema_fingerprint: LogicalSchemaFingerprint,
    pub query:                      IndexQuery,
    pub as_of_hlc:                  HybridTimestamp,
}

pub enum IndexQuery {
    PointEqual { encoded_key: Vec<u8> },
    Range      { lower: IndexBound, upper: IndexBound },
    InList     { encoded_keys: Vec<Vec<u8>> },
}
```

The executor enforces `Ready`-state gating, applies the
six-step revalidation, returns surviving rows to the PBC
data path. Classification on the response side increments
metrics:

- `visible_count`: passed all revalidation steps.
- `superseded_count`: rejected at step 4
  (`lookup_as_of` returned a different version or a
  tombstone).
- `stale_indexed_value_count`: rejected at step 5.

PBC residual rejections flow through existing PBC counters.

### 13. Reject-code taxonomy

Lookup-time and descriptor-validation rejects are a typed
`INDEX_REJECT_*` family with four codes. Schema-mutation-
time rejects (specifically the `Alter` mutation) are a
separate typed reject at the schema-update layer; they are
deliberately outside the `INDEX_REJECT_` family because the
call-path, operator audience, and surfacing channel
(replicated schema-mutation reply, not query response) all
differ.

**Lookup / descriptor-validation rejects (`INDEX_REJECT_*`):**

- `INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH` — query
  against an index descriptor whose logical fingerprint
  does not equal the current table descriptor's logical
  fingerprint. Operator action: re-fetch table descriptor
  and reissue.
- `INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE` — descriptor
  validation rejects unsupported Arrow types; mirrored on
  read-time defensive paths.
- `INDEX_REJECT_PK_COLUMN` — descriptor validation rejects
  indexing the declared PK column.
- `INDEX_REJECT_SYNTHETIC_COLUMN` — descriptor validation
  rejects indexing `__row_*__` columns.

**Schema-mutation-layer reject:**

- `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED` —
  `SchemaUpdateEnvelopeV1` with `SchemaObjectKind::Index`
  and `mutation = Alter`. Returned at the schema-mutation
  path (the same channel as other `SchemaUpdateEnvelopeV1`
  rejects). A changed index definition is drop + create
  with a new `IndexObjectId`.

No `INDEX_REJECT_NULL_INDEXED_VALUE_LOOKUP` (NULL is
excluded; `IS NULL` routes to scan).
No `INDEX_REJECT_NOT_FOUND` (empty results are successful).
Dropped-index lookup routes through fingerprint mismatch.
`Building` and `Dropping` descriptors also route through
fingerprint mismatch (planner refuses to use them; no
client-visible "wrong state" code).

### 14. Composition with existing ADRs

- **ADR 037**: index descriptors live in
  `SecondaryIndexCatalog`, outside
  `TableSchemaDescriptorV1`.
- **ADR 045 / 049**: identity policies hash into the table's
  `LogicalSchemaFingerprint` per ADR 051. Content-hash row
  identity preimage stays fingerprint-free.
- **ADR 046**: digest substrate consumed via
  `digest.secondary_index.entry.v1`. Per-segment durable
  placement satisfies §7 deferral test.
- **ADR 047 / 050**: tombstones live in the row chain.
  Index observes them through `lookup_as_of`. No index
  Tombstone kind.
- **ADR 048**: retention floor inherited; PK gate remains
  in force.
- **ADR 051**: index entries bind to
  `LogicalSchemaFingerprint`; physical retunes leave
  sidecars valid.

## Rationale

**Why now.** Roadmap #8. Every prerequisite is settled.

**Why distinguish version-entry from visible-membership in
the digest.** A row with N versions has N version-entries.
At `T`, at most one projects to a visible answer. V1's
direct-locator entry shape requires emitting on every data-
row write (including unchanged-value upserts) so the index
has a pointer to the latest payload. A visible-membership
digest would skip unchanged-value upserts and lose that
pointer. The version-entry digest is the right invariant
for the V1 entry shape.

**Why the index emits on every data-row write, including
unchanged-value upserts.** Without it, an unchanged-value
upsert leaves the index with only the older locator; an
indexed lookup post-upsert finds the older locator, fetches
the older row, fails step 4 (`lookup_as_of` returns the
newer hlc), and rejects — silent miss of a row that is
actually visible at the requested indexed value. The fix is
to emit a new version-entry on every data-row admission;
read-time revalidation collapses N candidates to one
visible answer.

**Why exclude NULL in V1.** Sentinel-byte encoding forces
a `NULL-first` versus `NULL-last` ordering decision before
any V1 consumer demands it. The V1 base index stays narrow:
non-NULL rows emit entries, NULL rows route to scan, and a
future variant ADR can define sentinel ordering when a real
consumer emerges. Per `no-premature-backward-compat`,
deferring an ordering choice without a load-bearing
consumer is the right discipline.

**Why bound the active-slab buffer.** Without a memory
bound, a runaway write workload on a wide indexed column
could drive the active-slab buffer to OOM and crash the
replica. `max_index_buffer_bytes` makes the failure
operational: overflow drops the buffer, marks the slab
uncovered, and lets the data write proceed. Correctness
is preserved by scan fallback; coverage is restored
naturally when the source slab compacts (§9).

**Why the backfill compaction race lease.** Without the
read-lease, backfill and compaction could both produce
sidecars for the same source slab, with the backfill's
sidecar pointing at locators in a slab that compaction
has just retired. The lease forces one of the two to
abort; the natural choice is backfill (compaction's
output is on the critical retention/correctness path,
backfill's output is a derived accelerator).

**Why two-phase drop GC.** A single-mutation drop that
removes the catalog row first risks leaking L1/L2 sidecar
bytes if GC crashes mid-pass. The `Dropping` marker keeps
the replicated catalog state load-bearing through GC: the
descriptor cannot transition to absent until reclaim
finishes. This is the same crash-safety discipline ADR 048
uses for retention policy mutations.

**Why recheck the indexed value (revalidation step 5).**
`lookup_as_of` proves the row version is visible at
`as_of_hlc`, but it does not prove the row's current
indexed value still matches the candidate's `encoded_key`.
A row that was at `col=foo` at hlc=10 may be visible at
`as_of=now` while having been updated to `col=bar` at
hlc=20; the index entry at `(foo, K, 10)` is now stale even
though `lookup_as_of` returns a non-tombstone row. Step 5
re-encodes the fetched row's indexed value and compares —
the only step that catches indexed-value mutation without
synchronous index deletion. Step 5 also defends against
stale segments, schema drift, and planner shortcuts before
PBC.

**Why distinguish logical version-entries from physical
locators in the digest preimage.** Replicas rewrite
locators independently during compaction. Including
`slab_id` / `record_location` / `record_row_index` in the
preimage would turn logically-equivalent-but-physically-
rewritten sidecars into spurious digest mismatches.

**Why still store physical locators in entries.** The query
path needs direct fetch pointers. Without locators, the
index degrades into a row-key set requiring point lookups +
scans to recover payloads.

**Why the sidecar-per-source-slab model.** A separately-
sharded index slab family requires its own locality
decision, retention discipline, compaction discipline, and
recovery path divorced from data slabs. Sidecars inherit
all four from the source slab automatically. Point lookup
loses O(1) routing — sidecars are iterated with min/max
pruning — but that cost is manageable and clears entirely
once range-sharded ring projection ships (a future ADR).

**Why lazy supersession.** Synchronous index updates on
every upsert / delete create write amplification
proportional to the number of indexes. The row chain plus
`lookup_as_of` already authoritatively answers visibility;
duplicating that on the index axis is duplicate work. The
trade-off is read amplification, bounded by source-slab
compaction cadence.

**Why mandatory candidate revalidation.** The index is a
candidate source. PBC and row-chain visibility are the
storage-side truths. Revalidation protects against stale
sidecars, schema drift, planner bugs, and indexed-value
mutation without a synchronous two-emission rule.

**Why no Tombstone kind on the index axis.** Tombstones
live in the row chain; the index observes them through
`lookup_as_of`. Adding a Tombstone kind would double
admission cost, require synchronous indexed-value recovery,
and duplicate supersession discipline. None buys
correctness.

**Why no synchronous index update on write.** Write
admission must not block on derived-accelerator failures.
A failing index buffer marks the slab uncovered and lets
the data write proceed.

**Why coverage is per-replica.** Each replica builds
independently. Replicating ready-state requires cross-
replica coordination on a derived artifact; V1 keeps the
coordination surface narrow.

**Why the catalog carries lifecycle state but writes don't
gate on it.** `Building` is a *planner* signal: descriptor
exists, admission already emits to the active-slab buffer,
but the index is not yet eligible for query use because
historical slabs are still being indexed. Reads against a
`Building` index would only see partial data; the planner
refuses to use it. Once backfill completes, a transition to
`Ready` makes the index eligible. `Dropping` is the
operator signal that a drop is in progress; writes stop
emitting and reads reject.

**Why split the reject taxonomy across `INDEX_REJECT_*` and
`SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`.** Lookup-
time and descriptor-validation rejects fire on the query
response path; the schema-mutation reject fires on the
replicated schema-update reply path. Operators reading
query metrics and operators reading schema-update audit
trails consult different surfaces. Encoding that in the
prefix keeps each surface's namespace coherent and avoids
the false impression that `Alter` rejection is a query-time
concern.

**Why reuse `SchemaUpdateEnvelopeV1`.** Adding a new typed
mutation duplicates Raft replication boilerplate the schema
envelope already provides.

**Why no table `schema_generation` bump.** Generation
orders *table* mutations. Index create / drop does not
change the table's logical or physical descriptor; bumping
generation would invalidate aggregate caches, CSB merges,
and replica mutation gates for no reason.

**Why reject PK column indexing.** The row-identity index
already provides exact PK lookup.

**Why reject Alter.** Index definition changes are not
safely mutation-compatible with existing sidecars. Drop +
create with a new `IndexObjectId` is the cleaner discipline.

**Why defer float / decimal / nested types.** NaN, negative
zero, decimal scale evolution, and nested-type canonical
encoding each deserve their own discipline.

**Why minimal planner integration in V1.** A cost-based
optimizer needs selectivity estimates from CSB / column
stats / runtime feedback. V1 ships predicate extraction
with a heuristic threshold; the optimizer ADR consumes it
later.

## Consequences

- **Operators get a second access path.** Equality, range,
  and IN-list reads on indexed columns avoid full scans for
  the covered fraction of the table.
- **Index entries survive table physical retunes.** ADR
  051's promise cashed in.
- **Per-segment digest reconciliation is O(1).** Anti-entropy
  ADR's substrate is in place.
- **Read-path cost grows on candidate-dense workloads**
  (lazy supersession + per-version emission). Bounded by
  source-slab compaction cadence.
- **Storage amplification: stale entries persist** until
  the source slab compacts. Update-heavy workloads see
  `secondary_index_entries_indexed_total` outpace visible-
  row count.
- **Index compaction overhead.** Sidecars regenerate on
  every source-slab compaction; the compaction scheduler
  gains a per-index work term proportional to indexed-
  column count.
- **Hybrid plans preserve correctness across partial
  coverage.** Uncovered slabs scan; covered use the index.
- **Index admission failure does not break write
  admission.** The affected slab is marked uncovered.
- **Unchanged-value PK upserts emit new version-entries.**
  Necessary so the index has a pointer to the latest
  payload.
- **PK and content-hash and append-only tables share the
  same index path.**
- **`record_row_index` widens to `u32`.** Widened in place
  on `RecordLocation` if the field already lives there;
  otherwise the segment entry adds it directly. Resolves
  roadmap #14 implicitly.
- **No on-disk shape change for data slabs.** Sidecars are
  a fresh artifact family.
- **No bump on any existing `format_version`.** The new
  sidecar header carries its own `format_version` on a
  fresh artifact family; pre-existing `format_version`
  fields are unchanged.
- **No new HRFT frame family.**
- **No new digest domain.**
- **No table `schema_generation` bump on index create /
  drop.**
- **Reject taxonomy partitioned across two namespaces:**
  four `INDEX_REJECT_*` codes (`LOGICAL_FINGERPRINT_MISMATCH`,
  `UNSUPPORTED_COLUMN_TYPE`, `PK_COLUMN`, `SYNTHETIC_COLUMN`)
  for lookup / descriptor-validation, plus a single
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED` at the
  schema-mutation layer. The two surfaces are deliberately
  separate.
- **Active-slab buffer is memory-bounded.** Operators
  expose `max_index_buffer_bytes`; overflow degrades to
  scan fallback for the affected slab.
- **Drop is two-phase replicated.** `Dropping` marker
  persists in the catalog until L1/L2 reclaim finishes.

## Rejected alternatives

- **Sharded index slab family routed by indexed-value
  locality.** Rejected. Requires its own locality /
  retention / compaction / recovery discipline divorced
  from data slabs. Sidecars inherit all four.
- **Index Tombstone entry kind with synchronous
  two-emission rule on indexed-value-changing upsert.**
  Rejected. Doubles admission cost; duplicates row-chain
  supersession.
- **Visible-membership digest in V1 ("emit only when
  visible state changes; skip unchanged-value upserts").**
  Rejected. Under V1's direct-locator entry shape, an
  unchanged-value upsert produces a new payload locator;
  if the index does not emit, the lookup post-upsert
  silently misses the row. The version-entry digest is
  the load-bearing invariant.
- **Synchronous-drain backfill at create time.** Rejected.
  Async derived-accelerator with hybrid fallback is
  operationally far better.
- **New typed `CreateSecondaryIndex` / `DropSecondaryIndex`
  mutations.** Rejected. `SchemaUpdateEnvelopeV1` with
  `SchemaObjectKind::Index` is the natural carrier.
- **`IndexLogicalFingerprint` newtype with stacked
  preimage.** Rejected. No V1 consumer for the
  separation.
- **Two new digest domains.** Rejected. ADR 046 reserved
  `digest.secondary_index.entry.v1`; ADR 052 owns the
  preimage.
- **Inline index entries into data slabs.** Rejected.
  Range scan would require reconstructing the index from
  every data slab.
- **Cover the index entry with the row's full bytes.**
  Rejected. Doubles storage.
- **Bind index entries to the table's physical
  fingerprint.** Rejected. Defeats ADR 051's split.
- **Online concurrent backfill in V1.** Rejected. Build-
  then-swap is a separate ADR.
- **Composite (multi-column) indexes in V1.** Rejected.
- **Expression / functional indexes in V1.** Rejected.
- **Unique-constraint enforcement.** Rejected. Cross-
  replica admission ordering is a semantic-constraint ADR.
- **Float / decimal / nested-type indexed columns in V1.**
  Rejected. Canonical encoding semantics deferred.
- **NULL-included V1 indexes with sentinel-byte encoding.**
  Rejected. Sentinel encoding forces a NULL-first vs
  NULL-last ordering decision before any V1 consumer
  demands it. `IS NULL` predicates scan in V1; a future
  variant ADR defines sentinel ordering when a consumer
  emerges.
- **Bump table `schema_generation` on index create /
  drop.** Rejected.
- **Embed `SecondaryIndexDescriptor` inside
  `TableSchemaDescriptorV1`.** Rejected.
- **Synchronous deletion of old index entries on upsert /
  tombstone.** Rejected.
- **Index tombstones for tombstone rows.** Rejected.
- **Reject reads while backfill is incomplete.** Rejected.
- **Make indexes authoritative (skip PBC recheck).**
  Rejected.
- **`Alter` mutation.** Rejected.
- **DDL parser for `CREATE INDEX … ON …`.** Rejected.
- **New query wire field for index hints.** Rejected.
- **Cross-table indexes.** Rejected.
- **Range-sharded ring projection in V1.** Rejected.
- **Drop boundary-tombstone preservation on the index
  axis.** Rejected.
- **Bundle full-chain key retirement into ADR 052.**
  Rejected.
- **Replicate the table's `PhysicalDescriptorFingerprint`
  into the index header for completeness.** Rejected.
- **Cost-based optimizer in V1.** Rejected.
- **Global ready-state replication.** Rejected.
- **Fold `Alter` rejection into `INDEX_REJECT_*` as a
  fifth code.** Rejected. Lookup-time and schema-mutation-
  time rejects fire on different paths with different
  operator audiences; conflating them under one prefix
  obscures the call-site split. ADR 052 keeps lookup
  rejects under `INDEX_REJECT_*` and `Alter` rejection at
  the schema-mutation layer under
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`.
- **Drop the catalog lifecycle state field; treat all
  catalog rows as `Ready`.** Rejected. Without `Building`,
  the planner cannot distinguish "index exists, backfill
  complete" from "index exists, backfill in progress."
  Reads against a still-building index would return partial
  results; the lifecycle state is the planner's signal.
- **Unbounded active-slab index buffer.** Rejected.
  Memory-unbounded buffer crashes the replica under
  wide-column write storms. The bounded buffer with scan
  fallback preserves correctness without pinning replica
  memory.
- **Single-phase drop (catalog row removed atomically with
  the drop mutation).** Rejected. Risks leaking sidecar
  bytes on a mid-GC crash. Two-phase drop keeps the
  `Dropping` marker as the reclaim watermark.
- **Backfill without compaction lease.** Rejected.
  Concurrent backfill and compaction on the same source
  slab can produce orphaned sidecars pointing into a
  retired slab. The read-lease forces one to abort
  cleanly.

## Resolved questions

V1 commitments — each can be revisited in a follow-up ADR
without on-disk surface change.

1. Single-column V1 indexes; one index per
   `(table, column)` allowed.
2. Index descriptors live in `SecondaryIndexCatalog`, not
   in `TableSchemaDescriptorV1`.
3. `SchemaUpdateEnvelopeV1` with `SchemaObjectKind::Index`
   is the create / drop carrier. `Alter` rejected.
4. Catalog rows carry
   `SecondaryIndexState ∈ {Building, Ready, Dropping}`.
   State gates planner use, not active-slab admission.
5. Index descriptors bind to the table's
   `LogicalSchemaFingerprint`. No separate index-axis
   fingerprint newtype.
6. Index sidecars are per-source-slab; locator triple
   `(slab_id, record_location, record_row_index)` with
   `record_row_index: u32`; `slab_id` implicit in the
   sidecar binding.
7. Per-segment `StateDigest` under reserved
   `digest.secondary_index.entry.v1`; preimage excludes
   physical locators and `schema_generation`; includes
   `column_ordinal` and `tenant_id`.
8. **Logical version-entry digest, not visible-membership
   digest.** Every data-row write emits a version-entry,
   including unchanged-value upserts.
9. V1 indexed types: bool, signed/unsigned ints,
   date/timestamp, Utf8/LargeUtf8, Binary/LargeBinary.
   Float / decimal / nested deferred.
10. **NULL excluded from V1 index.** No sentinel byte; NULL
    rows emit no entry. `IS NULL` predicates fall through
    to scan. Future variant ADR defines sentinel ordering.
11. Lazy supersession: no Tombstone kind, no synchronous
    deletion. Mandatory `lookup_as_of` + indexed-value
    recheck + PBC recheck on every candidate.
12. Coverage is per-replica; backfill is asynchronous;
    index admission failure marks the slab uncovered
    without rolling back the data write.
13. Hybrid plans (indexed candidates over covered + scan
    over uncovered) preserve correctness across partial
    coverage.
14. Compaction rebuilds sidecars from survivor data rows;
    obsolete entries naturally omitted.
15. PK retention stays cluster-MVCC prefix per ADR 050;
    `RETENTION_REJECTED_PK_DISALLOWED` remains in force.
16. PK column rejected at descriptor validation.
17. `__row_*__` synthetic columns rejected at descriptor
    validation.
18. No covering / unique / expression / multi-column /
    full-text / vector / partial / NULL-included indexes
    in V1.
19. **Reject taxonomy partitioned across two namespaces.**
    Lookup / descriptor-validation rejects use four
    `INDEX_REJECT_*` codes:
    `INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH`,
    `INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE`,
    `INDEX_REJECT_PK_COLUMN`,
    `INDEX_REJECT_SYNTHETIC_COLUMN`. The schema-mutation-
    layer reject is the separate
    `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`,
    surfaced on the schema-update reply path.
20. No SQL parser; reuse `SchemaUpdateEnvelopeV1`.
21. No new HRFT frame family; `TableLookupByIndexRequest`
    is internal.
22. No table `schema_generation` bump on index create /
    drop.
23. Index entries do not bind to the table's
    `PhysicalDescriptorFingerprint`.
24. Minimal planner integration: equality, range, IN-list
    with hybrid fallback. Cost-based optimizer deferred.
25. **Active-slab buffer is bounded by
    `max_index_buffer_bytes`** (operator tunable);
    overflow drops the buffer and marks the slab
    uncovered. Data writes succeed regardless.
26. **Backfill acquires a read-lease on each source slab**
    during build; compaction wins on contention and
    backfill aborts/retries against the compaction
    output.
27. **Two-phase replicated drop GC**: (1) drop mutation
    transitions catalog row to `Dropping`; (2) background
    GC removes L1/L2 sidecars; (3) replicated removal
    mutation removes the catalog row only after local GC
    completes.
28. **Compaction over an uncovered source slab heals
    coverage automatically** by emitting a sidecar for
    the output slab.

## Open questions

V1 picks defaults; each can be revisited without on-disk
surface change.

1. **Cross-slab key retirement under index evidence.**
   ADR 050 §8 deferred full-chain key retirement, citing
   secondary indexes as the natural place to surface
   cross-slab evidence. ADR 052 ships the access path; the
   proof itself is a separate discipline.
2. **Cluster-wide ready-state replication.** V1 is per-
   replica.
3. **Cost-threshold calibration.** Hybrid-plan vs full-
   scan threshold calibrated by Phase 9 bench results.
4. **Sidecar storage path convention.** L1 path and
   optional L2 URI shape finalize during Phase 3 against
   existing object-store helpers.
5. **Visible-membership digest as a follow-up consumer.**
   ADR 052 commits to the version-entry digest; if
   anti-entropy or unique-constraint enforcement demands
   visible-only reconciliation, a follow-up may add a
   separate domain.
6. **Storage amplification under unchanged-value upsert
   workloads.** V1 trades storage for write simplicity;
   bench results inform whether a future "skip
   unchanged-value upsert" emission rule pays off in
   conjunction with a separate visible-only digest.
7. **Indexed-value pruning cadence.** V1 prunes stale
   entries at source-slab compaction granularity. Update-
   heavy indexed columns may want an earlier rebuild
   trigger (e.g., a per-sidecar staleness ratio that
   forces rebuild before the source slab compacts).
   Benchmark Phase 9 should quantify whether the V1
   cadence is sufficient or whether a separate trigger
   is warranted.

## Forward-compatibility analysis

**Roadmap #9 wire-frame compression.** Index lookup
responses inherit transport-side compression. No ADR 052
contract change.

**Roadmap #10 CDC.** CDC cursors bind to the table's
logical fingerprint plus HLC. Index changes flow as
`__row_*__`-bearing data events the CDC stream already
covers. Index-assisted CDC catch-up is a follow-up.

**Roadmap #11 LSM compaction.** Sidecars compact under the
same LSM discipline as their source slabs. Because the
digest excludes physical locators, LSM rewrites verify
logical equivalence before retiring input segments.

**Roadmap #12 vector embeddings.** ANN indexes are a
distinct family with their own segment shape and access
path. ADR 052 settles the secondary-index *substrate*; ANN
composes by reusing the catalog mutation shape, the
locator triple, and the per-segment digest discipline.

**Future composite / multi-column index ADR.** Adds a
canonical preimage for `(value_1, …, value_k)`, reuses
sidecar shape and digest binding, allocates its own type
tag.

**Future NULL-included index variant ADR.** V1 excludes
NULL; `IS NULL` falls through to scan. A future variant
ADR introduces an `IndexNullAdmission` enum (or parallel
`IndexKeyEncoding` variant) and commits to a NULL-first
or NULL-last ordering when a consumer demands it.

**Future visible-membership index digest ADR.** Allocates
a separate domain (e.g.,
`digest.secondary_index.visible.v1`) and introduces a
synchronous emission discipline for consumers that need
visible-only reconciliation. ADR 052 stays version-entry
only.

**Future expression / functional index ADR.** Defines
admission-time vs read-time evaluation, reuses the
locator triple and digest discipline.

**Future unique-constraint ADR.** Composes by checking
membership digest at admission; may consume the future
visible-membership digest.

**Future online concurrent backfill ADR.** Defines a
build-then-swap protocol; reuses the descriptor catalog
mutation shape.

**Future range-locality ring projection.** Adds a true
second locality for indexes (range-sharded by indexed
value). Composes by replacing the sidecar model with a
sharded index family for range workloads; sidecar model
remains valid for hash workloads.

**Future full-chain key retirement.** Uses sidecars as
cross-slab evidence; reuses the per-segment digest.

**Future per-column structural fingerprint** (ADR 051
deferral). Allows sidecars to survive additive evolution
that changes the table's logical fingerprint but not the
indexed column.

**Future global ready-state replication.** Adds replicated
build watermarks; reuses descriptor catalog and per-
segment digest.

**Future float / decimal / nested-type index ADR.**
Defines canonical encoding under NaN / -0 / decimal-scale-
evolution semantics.

## Deferred to follow-up ADRs

1. Cost-based optimizer for index access path selection.
2. Online concurrent index backfill.
3. Composite / multi-column indexes.
4. Expression / functional indexes.
5. Covering indexes.
6. Unique-constraint enforcement.
7. Partial / filtered indexes.
8. Full-text / inverted indexes.
9. ANN / vector indexes.
10. Range-sharded ring projection.
11. Per-column structural fingerprint.
12. Anti-entropy reader / fanout consumer.
13. Full-chain key retirement using sidecar cross-slab
    evidence.
14. CDC index-axis event surface.
15. Cluster-wide ready-state replication.
16. Float / decimal / dictionary / nested-type indexed
    columns.
17. NULL-included index variant.
18. Visible-membership digest as a separate domain
    (`digest.secondary_index.visible.v1` or similar).
19. Index-assisted joins.

## References

- **ADR 013** — cluster MVCC retention.
- **ADR 037** — `tenant_table_schemas`; the catalog shape
  this ADR mirrors for `SecondaryIndexCatalog`.
- **ADR 040** — aggregate pushdown.
- **ADR 042** — analytical multi-row fragments; segment
  seal discipline parallel.
- **ADR 045** — PK-less append-only tables.
- **ADR 046** — `StateDigest` substrate; reserved
  `digest.secondary_index.entry.v1` consumed here.
- **ADR 047** — row-level tombstones; index observes
  through `lookup_as_of`.
- **ADR 048** — per-table retention.
- **ADR 049** — content-hash row identity.
- **ADR 050** — PK delete via supersession.
- **ADR 051** — logical / physical descriptor fingerprint
  split.
- `plans/conventions/newtype-units.md`.
- `plans/conventions/no-version-suffixes.md`.
- `plans/conventions/no-premature-backward-compat.md`.
- `plans/conventions/no-project-name-in-protocols.md`.
- `plans/conventions/capability-vs-feature-naming.md`.
- `plans/philosophy.md` — Identity-vs-Locality framing
  applied to the index axis (sidecar preserves the row's
  single locality).
