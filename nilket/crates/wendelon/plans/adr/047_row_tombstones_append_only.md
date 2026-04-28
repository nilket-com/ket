# ADR 047: Row-Level Tombstones on Append-Only Tables

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan: `plans/impl/047_row_tombstones_append_only.md`

## Status

Accepted

## Context

ADR 045 made PK-less append-only tables real: each append-only
row has a durable `__row_key_id__`, `__row_hlc__`,
`__row_routing_locality__`, and `__row_table_object_id__`, with
no PK canonicalization, no duplicate-key rejection, and no
row-delete surface. ADR 046 then added the `StateDigest`
algebraic substrate but deliberately left lifecycle semantics
and durable placement to consumer ADRs.

The next lifecycle step is row-level deletion for append-only
tables. The motivating cases are narrow but important:

- event streams that must retract a bad event without dropping a
  whole slab or table;
- audit/operational streams where delete authority exists but
  must preserve time-travel behavior until the retention floor
  passes;
- CDC sinks that need a stable representation for source-side
  delete events while still treating append rows as immutable
  facts.

Three pressures shape the design:

- **Compliance pull.** GDPR / right-to-be-forgotten and PII
  correction workflows need a row-level logical delete that does
  not require table rebuilds.
- **Algebra validation.** ADR 046 shipped add / subtract
  machinery; tombstones are the first non-speculative consumer
  that proves visible-state subtraction against real storage
  rows.
- **Read-path fast path.** Most append-only slabs will never
  contain tombstone marker rows. The design should keep that
  steady-state path cheap.

The constraint from ADR 045 still holds: append-only rows are
not updated in place and are not superseded by later payload
versions. A row delete must therefore be represented as another
storage fact, not as a mutation of the original row bytes.

## Non-goals (V1)

- **No PK-keyed row-delete via supersession.** Roadmap #6.
- **No per-table retention policy.** Roadmap #3. The composition
  rule is settled here so retention slots in cleanly; the surface
  (`RETENTION = '90 DAYS'`, retention-floor GC, time-driven
  reclaim) is not part of this ADR.
- **No content-hash identity.** Roadmap #5.
- **No CDC cursor surface.** Roadmap #10.
- **No secondary-index tombstone propagation.** Roadmap #8.
- **No anti-entropy gossip.** Tombstone digest is computable;
  exchange protocol is a follow-up ADR.
- **No durable `StateDigest` in slab footer / sidecar / catalog
  / wire.** ADR 046 §7 holds. The visible digest is helper-
  computable from slab contents on demand.
- **No `Identity` replacement.** ADR 046 §1 holds.
- **No new node capability bit, no new HRFT frame family, no
  new reject-code family, no `ObjectStore` trait change.**
  `DeleteRows` is a new MsgType pair inside the existing
  typed-table write family.
- **No envelope `format_version` bump.** A single reserved bit
  in the ADR 022 footer (already inside the HMAC range) is the
  entire on-disk shape change.
- **No descriptor format-version bump.** The synthetic column
  is derived, not declared.
- **No predicate delete, range delete, table truncate, or
  retention syntax.**
- **No update / upsert semantics.**
- **No tolerant decoder, no compat shim, no reserved-for-V2
  field.** Per `no-premature-backward-compat.md`.

## Decision

### 1. Append-only row delete is a tombstone fact

The codebase adds an append-only table operation:

```rust
pub struct DeleteRows {
    pub tenant_id: TenantId,
    pub table_object_id: TableObjectId,
    pub schema_generation: u64,
    pub parent_write_id: ClientWriteId,
    pub row_key_ids: Vec<Identity>,
}
```

This is the logical request shape. The wire encoding uses a
fixed metadata struct plus a `FixedSizeBinary(32)` Arrow
payload, inside the typed-table write family as
`TABLE_DELETE_REQ` / `TABLE_DELETE_RESP`. Council ratifies the
exact MsgType numbers from the typed-table allocation table;
`0x2E` / `0x2F` is the current candidate (see *Open
questions*).

The logical response shape:

```rust
pub struct DeleteRowsResponse {
    pub write: WriteResponseMeta,
    pub deleted_count: u32,
    pub already_deleted_count: u32,
    pub not_found_count: u32,
}
```

Missing and already-deleted row IDs are successful no-ops
reported through counts, not whole-request failures.

`DeleteRows` is accepted only for `RowIdentityPolicy::AppendOnly`.
The same request against a PK-keyed table is rejected; PK-keyed
delete is not silently mapped onto append-only tombstones.

Each committed delete emits one tombstone storage row per
target row that is currently visible at the delete HLC. A
tombstone row uses:

```text
__row_key_id__              = target row key (shared with target)
__row_hlc__                 = delete_hlc
__row_routing_locality__    = target row routing locality (inherited)
__row_table_object_id__     = target table_object_id
__row_tombstone__           = true
```

The original data row remains unchanged.

### 2. Multi-row storage gains `__row_tombstone__`

The storage-only synthetic-column set for multi-row records
becomes:

```text
__row_hlc__                 FixedSizeBinary(16)
__row_key_id__              FixedSizeBinary(32)
__row_routing_locality__    UInt64
__row_table_object_id__     FixedSizeBinary(16)
__row_tombstone__           Boolean
```

Data rows store `__row_tombstone__ = false`. Tombstone rows
store `true`.

Tombstone storage records may be **tombstone-only records**:
they carry the synthetic columns above and no user columns.
Query, stats, AGG, PBC, and column-fetch paths must treat
them as storage visibility facts, not user rows. User-facing
reads never surface `__row_tombstone__` or any other storage
synthetic column.

This avoids inventing placeholder values for non-null user
columns and avoids copying deleted row payloads into delete
records.

### 3. Tombstone presence metadata is an optimization, not authority

Each sealed slab records whether it contains any row-tombstone
marker rows. The footer flags carry:

```text
has_row_tombstone_records = any(__row_tombstone__ == true)
```

The bit is read-pruning metadata. It lets ADR 044 projected
fetch skip fetching `__row_tombstone__` from slabs that contain
no tombstone rows, and it lets maintenance paths avoid
tombstone-only decoding in the common case.

**The bit is read-pruning metadata, not visibility authority.**
It answers exactly one question: *does this slab contain
tombstone marker rows that have to be decoded at read time?*
It does **not** answer *can this slab's data rows be hidden by
a committed tombstone?* — that question is owned by ADR 040
`tombstone_metadata` (§7), and its answer can be yes even when
the presence bit is false (a later slab can tombstone an
earlier slab's data row).

This distinction matters for two reasons:

1. The no-delete fast path requires *both* signals (§8).
2. Tombstone-aware compaction may produce slabs whose
   `has_row_tombstone_records` is false but whose
   `tombstone_metadata` still records a relevant earliest-
   tombstone HLC. The bit and the metadata are not redundant.

**Roadmap shorthand vs final symbol.** ADR 045's forward-compat
sketch and some peer references use `has_tombstones`. This ADR
uses the longer name for the final symbol because the bit
records marker-row presence only — it is not the existing
`tombstone_metadata` visibility authority. Treat any peer-draft
or roadmap reference to `has_tombstones` as shorthand for the
same bit.

The presence bit is an **optimization** that can be dropped if
footer-slot allocation forces it. ADR 022's footer has finite
reserved-bit space; the implementation plan Phase 0 audit
confirms whether a clean HMAC-covered flag slot exists. If not,
the bit drops and the read path falls back to per-slab
metadata + index lookup (slightly more work in the steady-
state-no-deletes case, but correctness-equivalent). See *Open
questions*.

### 4. Tombstones share the target row identity

The primary index shape stays:

```text
(tenant_id, key_identity) -> Vec<RecordLocation sorted by hlc>
```

The local and persistent index entry for each location gains
an `is_tombstone` bit. The lookup key does not change; the bit
is metadata about the indexed location so admission and reads
can decide latest-as-of visibility without re-reading the
record body just to classify it.

ADR 045 said append-only identities should have exactly one
location. ADR 047 narrows that invariant:

- a live append-only row has one data location;
- a deleted append-only row has one data location and one
  tombstone location;
- more than one tombstone for the same row is an idempotency
  or corruption bug and is treated as a no-op on admission or a
  recovery error, depending on where it is observed.

This keeps lookup local to existing row-key indexing. A row's
latest location at or before an `as_of_hlc` decides visibility:

```text
latest <= as_of  is data row       -> visible
latest <= as_of  is tombstone row  -> hidden
no location <= as_of               -> not visible
```

The tombstone's `__row_hlc__` is the delete commit HLC. It is
not a replacement for the original row HLC; time-travel reads
before the delete HLC still see the original row.

### 5. Delete admission resolves targets before emitting tombstones

Delete admission performs these steps:

1. Validate tenant, table, descriptor generation, and
   `RowIdentityPolicy::AppendOnly`.
2. Deduplicate repeated `row_key_ids` inside the request.
3. Compute one leader-merged `delete_hlc` for the request.
4. For each key, consult the row index for the latest location
   at or before `delete_hlc`.
5. Emit a tombstone only when the latest location is a data
   row.
6. Treat missing keys and already tombstoned keys as successful
   no-ops and report counts.

The tombstone inherits the target row's stored routing
locality. That keeps the delete fact in the same replica group
as the data row and lets the existing table-write bifurcation
machinery group multi-row tombstone records by locality.

If a `DeleteRows` request spans multiple replica groups, the
accepting node splits tombstone children by inherited locality
and uses the same durable quorum / finalization discipline as
table writes. The request is all-or-nothing for the emitted
tombstone set: no client-visible success is returned until the
emitted delete facts meet the requested durability boundary.

The "latest is tombstone → no-op" rule (step 5 read negatively)
is the load-bearing idempotency invariant. It preserves
multiset digest discipline: ADR 046's substrate gives multiset
semantics, and `subtract_element` on an already-subtracted
target would over-subtract, putting the visible digest into a
state no logical row set produces. One-tombstone-per-row at
the admission boundary is the cheapest possible enforcement.

### 6. Idempotency is logical, not duplicate tombstone rows

The existing `parent_write_id` idempotency cache handles
ordinary retries. If a retry bypasses the cache after restart
or expiry, admission still consults current row state and sees
the row already tombstoned. It therefore emits no second
tombstone.

This deliberately preserves the earliest delete HLC. A later
delete of the same row is also a no-op; it does not move the
tombstone HLC forward. This is the only model that keeps
time-travel behavior and `StateDigest` set semantics stable.

### 7. CSB visibility metadata extends to row tombstones

ADR 040's `tombstone_metadata` remains the visibility guard
for CSB direct answers. ADR 047 extends its meaning from
migrated range tombstones to any committed tombstone that can
hide a row in the slab.

When a row tombstone commits, every data slab containing a
target row records the earliest applicable tombstone HLC:

```text
slab.tombstone_metadata =
    merge_earliest_tombstone(slab.tombstone_metadata, delete_hlc)
```

For L1 slabs this is local manager state. For L2 slabs the
existing `TieredSlabPointer.tombstone_metadata` field is
updated. No new pointer field is required.

CSB direct-answer eligibility remains:

```text
seal_hlc <= as_of_hlc
AND tombstone_metadata is tracked
AND no tombstone at or before as_of_hlc
AND supersession_metadata is tracked
AND no supersession at or before as_of_hlc
```

An append-only slab with row tombstones at or before
`as_of_hlc` falls back to row-level filtering. Tombstone-only
records never produce user-column CSBs (they have no user
columns).

This composition is the reason ADR 047 reuses
`tombstone_metadata` instead of inventing a new field. Range
tombstones and row tombstones share the same proof shape:
*earliest HLC at which a visibility-deleting fact could affect
the slab's row set.*

### 8. Query paths filter tombstones before user output

Append-only scans, PBC, AGG, and ADR 044 column-fetch must
apply row-tombstone visibility before returning or aggregating
user rows.

For each `__row_key_id__` in scope, the scan executor consults
the index for the latest location at or before `as_of_hlc`:

```text
latest <= as_of  is data row       -> visible
latest <= as_of  is tombstone row  -> hidden
no location <= as_of               -> not visible (yet)
```

Tombstone rows themselves are never returned to user output.
Tombstone-only records contribute zero user rows by definition.

For projected reads, the planner must include the storage
columns needed for visibility filtering:

```text
__row_key_id__
__row_hlc__
__row_table_object_id__
__row_tombstone__
```

`__row_routing_locality__` is included when the existing
locality or descriptor-matching path needs it. These columns
are stripped before user-visible output.

**ADR 044 fast-path rule.** Projected fetch may skip
`__row_tombstone__` (and the post-fetch visibility filter)
only when **both** signals agree:

```text
has_row_tombstone_records == false
AND tombstone_metadata has no tombstone <= as_of_hlc
```

The first signal says: this slab has no tombstone marker rows
to decode. The second says: this slab's data rows are not
hidden by a committed tombstone. Either alone is insufficient
— a steady-state-no-marker slab can still have its data rows
tombstoned by a later slab; conversely, a slab with marker
rows visible AT the as-of HLC may have all its targeted data
rows belonging to other slabs. Both checks are cheap (footer
flag bit + metadata predicate).

PBC, AGG, and CSB never count tombstone rows as user rows.
AGG `count_star`, `min`, `max` ignore tombstoned rows. AGG /
CSB short-circuits drop slabs whose `tombstone_metadata`
indicates a tombstone at or before `as_of_hlc`.

A scan must fail-closed when a record is missing the synthetic
columns required for visibility classification.

### 9. Compaction preserves history until the retention floor

Compaction may regroup data rows and tombstone rows, but it
must preserve time-travel behavior for every supported
`as_of_hlc`.

For a deleted append-only row:

- before `delete_hlc`, the original data row is visible;
- at and after `delete_hlc`, the row is hidden;
- once the retention floor is greater than `delete_hlc`, both
  the data row and its tombstone row may be dropped together.

If the original data HLC is below the retention floor but the
tombstone HLC is not, compaction must keep both rows.
Otherwise a read at an as-of time between the retention floor
and the tombstone HLC would incorrectly lose the pre-delete
row.

Windowed compaction can reclaim a data / tombstone pair only
when the history window no longer requires the pre-delete
row:

- **In-window pair** (data row and its tombstone both in the
  current compaction window): the pair is **eligible** for
  joint drop because the visible digest is invariant under
  the algebra (the pair's `+visible_row_element` and
  `-visible_row_element` already cancelled). Joint drop is
  **only safe when the history window no longer requires the
  pre-delete row** — i.e., `retention_floor > delete_hlc`,
  or an equivalent partition-scope history-window proof
  exists. Otherwise the data row must be retained
  (regrouped into the output) so that `AS OF T < delete_hlc`
  reads continue to see it. Dropping both physical rows
  before the retention floor passes `delete_hlc` would
  satisfy the digest algebra but break time-travel
  visibility.
- **Tombstone without data row in the current window** (data
  row in an older slab outside the window): retain the
  tombstone in the output unless retention or a partition-
  scope scan can prove the data row has already been safely
  reclaimed.
- **Data row without its tombstone in the current window**
  (tombstone in a newer slab outside the window): retain the
  data row. The rebuilt index plus external
  `tombstone_metadata` continues to hide it from visibility-
  filtering reads; physical removal would break time-travel
  for `as_of_hlc < delete_hlc`.

The output slab's `has_row_tombstone_records` bit reflects
whether any tombstone marker rows survived. The output slab's
`tombstone_metadata` is recomputed from survivors.

ADR 047 uses the existing MVCC retention floor as the safe
history-GC boundary for tombstoned pairs. Per-table retention
policy remains the next lifecycle ADR.

### 10. StateDigest composition uses visible-domain subtraction

ADR 047 uses ADR 046's `digest.table.row.visible.v1` domain
directly, with no new domain.

```text
data row admission:    accumulator.add_element(visible_row_element(t, T, k))
tombstone admission:   accumulator.subtract_element(visible_row_element(t, T, k))
```

Because the tombstone shares the target's `row_key_id`, the
two elements are byte-for-byte identical and cancel
algebraically. The visible digest of a slab is the digest of
all logically visible rows — no separate physical or event-
history digest required.

The crucial invariant:

> Compaction can drop a `(data, tombstone)` pair without
> changing the visible digest. The pair's add and subtract
> already cancelled; removing both physical rows from storage
> leaves the digest invariant.

That is testable: pack a slab, take the visible digest, run
pair GC, take the visible digest again, assert equality. The
implementation plan Phase 8 asserts it.

The visible digest is **computable on demand** from slab
contents. This ADR does **not** persist it. ADR 046 §7 defers
durable placement to a consumer with a concrete reader,
authentication boundary, and invalidation rule — anti-entropy
is that consumer, and anti-entropy has its own ADR.

**In-memory cache rule.** Seal-time or recovery-time code may
compute the visible digest once and cache it **in memory**
(per-slab, per-partition, or per-replica) as an optimization.
That cache is not a contract: it must not be persisted to the
slab footer, the `TieredSlabPointer`, the catalog, the local
index, the durability log, or any wire frame. If a workload
pulls on durable cross-replica comparison, that pull belongs
to the anti-entropy ADR — not this one.

A separate `digest.table.row.tombstone.v1` event-history
domain is **not** used in V1. The substrate has the byte tag
reserved (ADR 046 §4) and a future audit / anti-entropy ADR
may pull on it, but row-tombstone visibility itself is fully
captured by visible-domain subtraction.

**Cold-path digest cost is unchanged from ADR 046's single-
domain measurement.** Each row contributes one visible-row
point — added on data, subtracted on tombstone — and that is
the entire digest work this ADR does on the cold path. No
physical-row digest, no tombstone-event digest, no retention-
eviction digest is introduced here.

### 11. Recovery rebuilds tombstone visibility from storage rows

Recovery reads data rows and tombstone rows from multi-row
storage records. For each tombstone row it rebuilds the same
`(tenant_id, key_identity) -> Vec<RecordLocation>` chain as
live admission, with the `is_tombstone` bit on the indexed
location.

Recovery also recomputes per-slab tombstone metadata:

- a slab containing only tombstone rows is not a CSB direct-
  answer source;
- a slab containing data rows hidden by tombstones records the
  earliest applicable tombstone HLC;
- corrupted chains with multiple tombstones for one row are
  rejected or repaired by keeping the earliest tombstone and
  reporting a corruption metric, depending on the local
  recovery policy chosen by implementation.

The implementation plan should prefer fail-closed behavior for
any ambiguous visibility state.

## Rationale

**Why tombstone rows instead of mutating original rows.** L1
and L2 slabs are append / seal objects. Mutating a stored row
would either break slab integrity or require a side index that
becomes the real source of visibility truth. A tombstone row
is an ordinary storage fact and composes with existing durable
quorum and recovery logic.

**Why share the target `__row_key_id__`.** Reuses the existing
version-chain lookup wholesale. Index-level visibility is a
one-bit classification on the latest-as-of entry. Digest
cancellation is automatic — both sides project the same
`visible_row_element`, so add and subtract operate on the same
point. Retry-after-cache-loss is naturally idempotent: once
the latest indexed location is a tombstone, a repeated delete
emits nothing.

**Why not give tombstones their own unique row identity plus a
target column.** That model preserves a one-identity-per-
append reading of ADR 045 and gives CDC a distinct tombstone
event identity. The cost is an extra target column, scan-time
tombstone-set unions instead of index lookups, separate
idempotency logic for re-deletes, and either a separate
`tombstone.v1` digest domain or per-element subtraction that
doesn't naturally cancel. This ADR chooses the storage-index
interpretation: a tombstone is the next version of the target
row identity, and the `is_tombstone` bit classifies that
version. The CDC "every row is an event" property is
recoverable at the CDC layer — the tombstone marker row IS a
unique storage event with its own physical record location,
even when it shares logical identity with its target.

**Why one new column, not two.** With shared identity, a
second `__row_tombstone_target__` column would be redundant —
the target identity is already on `__row_key_id__`. The
remaining `__row_tombstone__` column is a 1-bit boolean with
high compression; ADR 044 column-fetch can consult
`has_row_tombstone_records` (combined with
`tombstone_metadata`) and skip even that one bit on steady-
state slabs.

**Why tombstone-only records.** Delete records do not have
user payload. Forcing placeholder values into non-null user
columns would pollute stats, compression, and validation.
Keeping tombstone rows synthetic-only makes their storage role
explicit at the record schema level.

**Why one tombstone per row.** Multiple tombstones for the
same row do not add information and break multiset digest
discipline. The earliest tombstone is the semantic delete
time; later attempts are no-ops.

**Why subtract from visible, not three monotone domains.**
The audit / anti-entropy / time-travel arguments for a
separate event-history digest are real, but they are deferred-
ADR territory. ADR 046 §6 explicitly named this as a future
consumer call. Pinning event-history domains here would
triple cold-path digest cost and exceed the row-tombstone
ADR's charter. If the anti-entropy or audit ADR later needs
event-history granularity, it pulls on
`digest.table.row.tombstone.v1` (substrate already reserved)
at that time.

**Why not persist a footer `StateDigest` now.** Tombstones
validate the algebra, but they do not by themselves define who
reads a persisted point, how it is authenticated across
sidecar / footer placement, or how stale points are
invalidated through compaction. ADR 046 §7's deferral test
(concrete reader, authentication boundary, invalidation rule)
is not satisfied by tombstones alone. Anti-entropy, secondary-
index segments, or retention can own that placement when one
of them needs it.

**Why reuse `tombstone_metadata`.** CSB direct-answer
soundness only needs to know whether a slab's row set may have
been hidden by a tombstone at or before `as_of_hlc`. Range
tombstones and row tombstones have the same proof shape at
this layer: earliest visibility-deleting HLC. Reusing the
existing field avoids inventing a parallel mechanism for the
same predicate.

**Why keep a row-tombstone presence bit.**
`tombstone_metadata` answers "can this data slab's visible
rows be hidden by a committed tombstone?" The presence bit
answers "does this slab contain tombstone marker rows that
have to be decoded?" They are different questions. The bit
keeps the common no-delete slab path cheap without becoming
visibility authority.

**Why the bit is named `has_row_tombstone_records` and not the
shorter `has_tombstones`.** Two different concepts in this
codebase use the word "tombstone": range tombstones (ADR 040)
carry visibility authority via `tombstone_metadata`; row
tombstone marker rows (this ADR) drive decode-cost
optimization. The presence-vs-authority distinction is load-
bearing throughout the design (§3, §8 fast-path rule, §9
compaction discipline, §11 recovery rebuild). The long name
encodes that distinction at the symbol level — anyone reading
`if has_row_tombstone_records { … }` can tell the bit is about
marker-row presence without consulting the spec. The short
name `has_tombstones` reads naturally too, but in a codebase
that already has `tombstone_metadata` it risks being misread
as a presence bit *for that field's authority*. The §3
"roadmap shorthand vs final symbol" paragraph carries the
shorthand-equivalence rule.

**Why a `DeleteRows` MsgType, not a generic write payload.**
The payload is a list of row IDs, not a record batch with
user columns. A typed request avoids per-row Arrow user schema
overhead for what is fundamentally an ID list. The new
MsgType lives inside the existing typed-table write family —
no new HRFT family, no new node capability bit.

## Consequences

- Query paths must carry storage visibility columns more often
  for append-only tables with row tombstones.
- Tombstone-only records add a second multi-row storage shape.
  Every decoder that assumes user columns are present must be
  audited.
- Deletes can create small tombstone records unless batching
  groups them by locality.
- Deleted row payload bytes remain stored until the retention
  floor crosses the tombstone HLC.
- CSB direct-answer rates decrease on slabs with row deletes.
- The primary index invariant for append-only rows changes
  from "exactly one location" to "one data location plus
  optional tombstone location."
- Cold-path digest cost stays on the ADR 046 single-domain
  path. This ADR does not pay the cost of a separate physical-
  row or tombstone-event digest.
- `StateDigest` visible-state equality alone does not prove
  historical event equality. The tombstone rows remain the
  history source of truth until a future event-history digest
  has a concrete consumer.
- The tombstone-presence bit consumes one HMAC-covered flag
  slot if Phase 0 confirms such a slot. If the only clean
  implementation requires a wider footer change, the presence-
  bit optimization drops and reads fall back to metadata +
  index lookup.

## Rejected alternatives

- **Tombstones with their own unique `__row_key_id__` and a
  `__row_tombstone_target__` column.** Strong CDC / audit-
  trail story — every appended row is its own physical event.
  Rejected because: (a) it forces a separate scan-time
  tombstone-set union pass instead of reusing the existing
  latest-as-of index lookup; (b) idempotency needs separate
  admission logic to prevent duplicate tombstone events for
  the same target (the shared-identity model gets it for free
  via "latest is tombstone → no-op"); (c) it splits digest
  cancellation across two elements (target add cancels via
  target subtract; tombstone add doesn't cancel anything)
  instead of one. The CDC "every row is an event" property is
  recoverable at the CDC layer: the tombstone marker row IS a
  unique storage event with its own physical record location.

- **Three monotone digest domains: `physical.v1` +
  `tombstone.v1` + derived `visible.v1`.** Audit / anti-
  entropy / time-travel-composition strengths are real but
  target *deferred* ADRs that have not yet pulled on those
  properties. Triples cold-path digest cost. ADR 046 §4
  explicitly reserved the byte tag
  `digest.table.row.tombstone.v1` for future-consumer
  ownership; pinning preimage and storage shape here would
  pre-allocate surface that the deferred consumers may want
  to revise.

- **Pin `digest.table.row.physical.v1` preimage here.**
  Withdrawn for the same reason — the canonical row-fact
  consumer (content-hash identity ADR, roadmap #5) owns that
  preimage's shape.

- **Durable `StateDigest` in slab footer.** Rejected. ADR 046
  §7 reserves durable placement for a consumer with a
  concrete reader, authentication boundary, and invalidation
  rule. Anti-entropy is that consumer; anti-entropy has its
  own ADR. The footer's 256-byte budget and HMAC scope are
  deliberately constrained.

- **Pre-allocating `digest.retention.gc.v1`.** The retention
  ADR owns its own preimage once it defines policy and GC
  authority. Per `no-premature-backward-compat.md`.

- **Subtract-from-visible without the one-tombstone-per-row
  admission check.** Multiset digest discipline forbids over-
  subtraction. Earliest-wins is the cheapest enforcement
  boundary.

- **Tombstones as full multi-row rows with placeholder NULL
  user columns.** Rejected — pollutes stats / compression /
  validation. Tombstone-only records are the right shape.

- **One synthetic column carrying `Option<Identity>` (none on
  data, target id on tombstone).** Rejected on column-fetch
  and stats grounds.

- **Tombstones in a sidecar block, not as multi-row records.**
  Rejected — sidecar requires a new HMAC-bounded surface, new
  compaction rule, new catalog field. The row-shaped approach
  reuses end-to-end.

- **Always fetch tombstone columns; no presence bit.**
  Rejected on column-projected-fetch grounds — most append-
  only tables in production never emit a single tombstone.

- **Reject `DeleteRows` for non-existent rows.** Rejected —
  forces the hot path to validate row existence, dragging in
  payload hashing or replicated state. SQL-shaped no-op
  semantics handle missing keys cleanly.

- **Allow `DeleteRows` against PK-keyed tables (silently
  remap to supersession).** Rejected — different identity
  policy, different semantics. PK-keyed delete is roadmap #6
  with its own surface.

- **Scan-time tombstone-set only (no index `is_tombstone`
  bit).** Rejected as the primary visibility mechanism
  because cross-slab tombstone-set growth is unbounded until
  retention. The row index plus `tombstone_metadata` provides
  the local latest-as-of visibility shape.

## Open questions

1. **MsgType allocation.** `0x2E` / `0x2F` is the current
   candidate for `TABLE_DELETE_REQ` / `TABLE_DELETE_RESP`.
   Council ratifies the exact values from the typed-table
   write family allocation table.

2. **Tombstone-only decoder audit.** Council should review
   the implementation audit list for decoders that currently
   assume every multi-row storage record has user columns.

3. **Recovery duplicate policy.** For multiple tombstones on
   one row, should recovery fail closed or keep the earliest
   tombstone and emit a corruption metric? This ADR prefers
   fail-closed unless implementation evidence shows repair is
   materially safer.

4. **Cross-window orphan cleanup certainty.** This ADR keeps
   orphaned tombstones and orphaned data rows unless retention
   or a partition-scope scan proves safe reclaim. Council
   should decide whether that is enough before the retention
   ADR lands.

5. **Footer presence bit allocation.** If the current footer
   has no clean HMAC-covered flag slot, should ADR 047
   reshape a reserved byte as a flag field, or should it drop
   the presence bit and rely on row-index / tombstone-
   metadata pruning only? Dropping the bit is correctness-
   equivalent; it only costs read-path work on steady-state
   slabs.

6. **Visible digest cache.** Should slab seal compute an in-
   memory visible digest cache, or should every consumer
   derive it on demand until anti-entropy defines a durable
   placement?

## Forward-compatibility analysis

**Per-table retention (roadmap #3).** Adds
`digest.retention.gc.v1` domain, subtracts retention
evictions from visible. Tombstones whose data row has been
retention-evicted are dropped by retention GC; the visible
digest is invariant under the algebra. The pair-reclaim rule
from §9 is symmetric — retention is another monotone
subtraction from visible at compaction time.

**Content-hash identity (roadmap #5).** Adds the third
`RowIdentityPolicy` variant. May extend the visible-row
preimage to include canonical payload bytes (or add a sibling
`digest.table.row.content.v1` domain). This ADR's
`__row_tombstone__` column composes unchanged: content-hash
rows are still rows, and tombstones still share the target's
`__row_key_id__`.

**PK-keyed row-delete (roadmap #6).** Reuses the
`__row_tombstone__` synthetic column and
`has_row_tombstone_records` footer bit. Layers PK
supersession discipline on top: PK-keyed delete may produce
multiple tombstones for the same logical PK across versions,
which interacts with PK supersession's existing version-chain
logic. The PK delete ADR settles that.

**Logical / physical descriptor fingerprint split (roadmap
#7).** This ADR adds one synthetic column on the logical
side. The fingerprint split places it cleanly.

**Secondary indexes (roadmap #8).** The future secondary-
index ADR consumes the visible-row filtering rule but does
not redefine it. Index segments at compaction time are
rebuilt against post-tombstone visible rows. The index ADR's
`secondary_index.entry.v1` digest domain composes
orthogonally — tombstones do not directly appear in the
index-entry digest, because the index ADR's preimage is
index-entry-shaped, not row-shaped.

**Wire-frame compression (roadmap #9).** Tombstone records
ride compression like any other.

**CDC (roadmap #10).** Tombstones are CDC events. The CDC
ADR exposes a cursor surface and change-set frame; both
consume tombstone rows directly (they are physically present
in the slab until GC). The CDC cursor surface is the CDC
ADR's call. This ADR pins one CDC-relevant property:
tombstones do **not** compress into a CDC `delete` event with
the prior payload. Append-only does not carry update / upsert
payloads through CDC; consumers see a `tombstone-emitted`
event with the target row key and the delete HLC. Workloads
that need the prior payload scan `AS OF delete_hlc - 1`
before retention-floor GC drops the original.

**LSM compaction (roadmap #11).** Pair GC extends to level
merges.

**Anti-entropy gossip (deferred from ADR 046).** Compares
visible-domain digests directly between replicas. Divergence
localizes to row-key-identity differences. The substrate
supports this directly; durable placement is the anti-entropy
ADR's call.

## Deferred to follow-up ADRs

1. Per-table retention policy and floor.
2. Anti-entropy gossip protocol.
3. CDC cursor surface and change-set frame.
4. PK-keyed row-delete via supersession.
5. Content-hash identity / canonical-row-fact encoding.
6. Secondary-index tombstone propagation rules.
7. LSM tombstone GC across levels.
8. Tombstone-density alarms / introspection metrics.
9. Durable `StateDigest` placement (anti-entropy ADR).
10. Event-history digest domain
    (`digest.table.row.tombstone.v1`) if anti-entropy / audit
    pulls on it.

## References

- **ADR 022** — slab footer
  (`has_row_tombstone_records` bit consumes one reserved-bit
  slot inside the existing HMAC scope).
- **ADR 040** — CSB visibility metadata
  (`tombstone_metadata` reused for row-tombstone visibility
  authority).
- **ADR 042** — single-coherent compaction (the GC rule
  applies under).
- **ADR 044** — column-projected fetch (consults the footer
  bit and `tombstone_metadata` for the no-delete fast path).
- **ADR 045** — append-only identity, write-context
  derivation, synthetic columns (`__row_*`), forward-compat
  sketch this ADR ratifies.
- **ADR 046** — `StateDigest` substrate, OQ#3 lifecycle
  model.
- **Brainstorm 014** — algebraic record identity / ECMH.
- `plans/conventions/{newtype-units, no-premature-backward-compat,
  no-version-suffixes, no-project-name-in-protocols}.md`.
