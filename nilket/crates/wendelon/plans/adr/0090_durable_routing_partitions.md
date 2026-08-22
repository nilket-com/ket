# ADR 0090 - Durable routing partitions

**Status:** Proposed 2026-08-22. Predecessor to ADR 0089, which is blocked on it.
It decides what a record may group and who owns the result; it does not pack
anything, change the record format, or alter the response protocol.

## Context

ADR 0089 set out to amortize the dominant scan cost — one physical read plus one
full decode per row, together 98.6% of scan time — by grouping rows into shared
records. Grounding the implementation found that the grouping key it proposed,
the locality prefix, cannot be made sound, and that the obstacle is placement
rather than packing.

**Ownership is an arc of the raw locality space, and arcs move.** Production
wires the vnode ring: each physical node is inserted `VNODES_PER_NODE` times at
BLAKE3-derived positions, a replica set is the clockwise walk filtered by the
data capability, and a background updater rebuilds and atomically swaps the ring
on every membership epoch. `RingState::primary_ranges_for` returns a node's owned
arcs as raw `(u64, u64)` locality ranges. (`StaticReplicaRouter`, which resolves
`locality % nodes.len()`, is the documented bring-up and test fallback, not the
production router — but it has the same defect for this purpose.)

Rows sharing a 16-bit locality prefix differ across the remaining 48 bits, so
they span an enormous arc of the ring and are owned by many different replica
sets. A record built from them has no coherent owner. Worse, arc boundaries move
when membership changes, so even a grouping that is co-owned at write time can
later straddle an ownership boundary — and a durable record cannot be re-grouped
without rewriting it. Single-node deployments hide all of this, which is the
failure shape to avoid: it would test clean and break in a cluster.

**Packed rows are also unindexed today.** The multi-row write branch emits one
`BifurcatedTableChildWrite` per record, carrying the first row's `key_identity`
and `record_row_index: ZERO`, and ingest registers exactly one key-index and
locality-index entry per child. With one row per record that is correct. Pack `R`
rows and rows `2..R` receive no index entry at all: invisible to range scans and
to point lookup. The capability to derive per-row entries from a stored frame
exists — `storage_multirow_index_entries_from_frame`, used by the sweeper and
retention paths — but it is not wired at ingest.

What is already settled and does not need revisiting: the locality index is
row-granular and gives exact interval semantics, verified for intervals crossing,
starting inside, ending inside, and wholly inside a locality span; and row
resolution inside a shared record is identity-based, matching on key identity,
row HLC, record row index, and tombstone rather than on position.

## Decision

### 1. A durable partition identity

A **partition id** is derived from a row's routing locality by a fixed,
membership-independent function over a fixed partition cardinality. It is a pure
function of the locality: the same locality yields the same partition in every
membership epoch, on every node, forever. It is stamped durably on the record.

Partition cardinality is a durable constant, not a tuning knob. Changing it
re-maps every row and is a format and migration decision.

### 2. Placement is defined on the partition, never on the raw locality

Replica placement resolves a partition to a replica set. The ring assigns whole
partitions to nodes, so a partition is indivisible with respect to ownership and
**no ownership boundary ever falls inside a partition**. Raw-locality placement
is what makes grouping unsound; once partitions exist, computing placement from a
raw locality is a defect.

This subsumes the `StaticReplicaRouter` stand-in: `locality % nodes.len()` must
not survive as a production path.

### 3. A record never spans a partition

The partition is the grouping key ADR 0089 needs. A record holds rows from
exactly one partition, so it has exactly one owner at every epoch, and grouping
decided at write time stays valid for the life of the record.

### 4. Every row of a shared record is individually indexed

Ingest registers one index entry per row — its own key identity, its own full
routing locality, and its `record_row_index` — all resolving to the one physical
record. The locality index stays row-granular, which is what preserves exact
interval semantics.

A shared record must never be registered by its first row alone. That is the
present behaviour and it is safe only while records hold one row; it becomes
silent row loss the moment they do not.

### 5. Membership change moves whole partitions

Rebalancing transfers partitions, and therefore whole records. A record is never
split, never rewritten to change its grouping, and never partially owned. During
a transfer every row of a record moves together, and a concurrent read returns
each row exactly once.

The transfer state rides the existing Raft-persisted Migration Ledger, not a new
machine. A partition's span — its prefix followed by every suffix — is a
contiguous locality range, so a ledger entry can carry a whole partition without
a schema change, and the range-based pull machinery enumerates exactly the
partition's rows. A partition is covered by an entry whose range contains its
representative position (the partition value, which is also the one ring
position the whole partition routes by).

**Exactly one owner serves, structurally.** `PartitionTransfer` carries a single
`serving` node, so "both owners" and "neither owner" are unrepresentable rather
than guarded: an in-flight entry's `logical_owner` serves alone — route primacy
and write fan-out both consult the transfer before the ring — and a completed or
absent entry hands serving back to the ring, which names one owner. Two
in-flight entries claiming one partition with different owners is a malformed
ledger and is refused rather than arbitrated: a broken ledger fails closed, it
does not pick a side.

**The epoch seeder derives partition spans, and it is load-bearing.** Without
it, a membership change re-owns a partition the moment the ring swaps, before
any byte transfers — the new owner serves misses. The seeder therefore emits,
per qualifying moved intersection and per live partition width from the schema
catalog, one span-shaped entry covering the contiguous run of moved partitions:
range `[first_representative, last_representative | suffix_mask]`, owner and
sources taken from the same ring diff as the segment seeds. Lattice runs are
merged so cardinality stays one entry per intersection per width rather than one
per partition. The trigger is `partitioned_prefix_width`, the same single
definition ingress uses, so seeding and writing cannot disagree about which
tables are partitioned.

Seed derivation is replay-critical — a later leader reconstructs the expected
batch — and this change is versioned **by construction of the ids**: partition
seeds only ADD entries whose deterministic ids (span-shaped ranges) are disjoint
from segment ids, and reconciliation converges by committing what is missing.
Nothing existing is reinterpreted; an upgrade mid-transition commits the
partition seeds the older derivation failed to plan, which is the repair, not a
corruption.

**`logical_owner` is the serving primary and anchor, not the whole replica
set.** A transfer names the one node primacy is judged against; the remaining
replicas come from the ring's own walk at the configured replication factor, so
a transfer never silently collapses replicated durability to one node.

### 6. Existing raw-locality records stay readable, and placement semantics are durable

Every record carries a durable marker for the placement semantics it was written
under. A record written before partitions keeps raw-locality placement for its
lifetime or until it is explicitly migrated; a partitioned record resolves
through its partition. The marker is stamped and read, never inferred from the
value's shape and never implied by a parse failure — absence of a stamp means
legacy, and that default is written down rather than discovered.

**An existing location is never reinterpreted as a partition id.** The two share
a 64-bit space and a legacy locality can be numerically indistinguishable from a
valid partition id, so reading one as the other misroutes silently — the failure
this decision exists to make impossible. Placement resolution consults the
record's placement semantics first and the value second.

Both transition paths are supported and neither is optional: a legacy record may
be migrated to partitioned placement, or it may be read through the legacy path
indefinitely. Migration is never a precondition for reading. A deployment that
never migrates keeps working, and a deployment mid-migration holds both kinds at
once — that mixed state is a supported steady state, not a window to be closed
quickly.

Both kinds coexist under membership change. A rebalance with both present moves
legacy records by arc and partitioned records by partition, in the same epoch
swap, without either kind losing or duplicating a row.

### 7. Every carrier uses its own existing version mechanism, and none uses a default field

Placement semantics and partitions are durable, so before anything is stamped the
carriers that would have to hold them are enumerated with the compatibility
mechanism each already has. No carrier gains a new scheme, and none relies on a
defaulted field to survive an old reader.

| carrier | what it holds | mechanism it already has |
|---|---|---|
| Record frame per-row metadata | `__row_key_id__`, `__row_routing_locality__`, `__row_hlc__`, `__row_tombstone__`, `__row_table_object_id__` | Arrow IPC is self-describing; readers probe `column_by_name`. `__row_table_object_id__` was added exactly this way |
| Persistent record index (redb `slab_records_v1`) | routing locality, record row index, flags, table object id | fixed-length value, **length-dispatched**: V1 = 4, V2 = 12, V3 = 20, V4 = 36; the decoder accepts every earlier length |
| Raft snapshot (`SmState`) | applied log id, membership, epoch history, leases | `SNAPSHOT_FORMAT_VERSION` in the snapshot header; no migrations, and a version mismatch is explained rather than reported as damage |
| Raft WAL record | log entries, snapshot markers | `WALRecordHeader.format_version`, carved from reserved padding under ADR 0085; `PRE_VERSIONING_FORMAT` means shape unverifiable and decoded as before |
| Bifurcated write wire meta | `TableWriteRequestMeta` and siblings | fixed C layout with named `_pad` bytes; size is the contract |
| CDC cursor | `(row_hlc, row_key_id, record_row_index)` | fixed layout with `_pad`; already addresses a row inside a shared record and needs no placement |
| Secondary index segment | segment payloads | `SECONDARY_INDEX_SEGMENT_FORMAT_VERSION` |

Three rules follow, and they are not negotiable:

**A missing postcard tail is not a default.** postcard is not self-describing: a
decoder that runs out of bytes fails, and `#[serde(default)]` cannot rescue it.
Any field added to a postcard-encoded durable structure is a format-version bump
under ADR 0085, with no migration, and the older artifact is explained rather
than silently reinterpreted.

**Absence must be decodable, never inferred from failure.** A record without a
placement stamp is `RawLocality` because its carrier says so — a shorter index
value, an absent Arrow column, an older declared format version — and never
because a parse failed. A parse failure stays an error.


**Length dispatch alone is not sufficient, and the index is not self-validating.**
A V5 index value truncated back to thirty-six bytes is byte-indistinguishable
from a genuine V4 value, so length would read it as legacy and route a
partitioned record by raw locality — silent misrouting, which is exactly the
outcome the rules above forbid. Length dispatch therefore covers V1–V4 only.

V5 is validated against **independent durable evidence**: the record frame. Arrow
IPC is self-describing, so a partitioned record carries
`__row_partition__` and an older one does not, and that presence cannot be
truncated away without destroying the frame. Placement is read from both carriers
and they must agree:

| index reads | frame declares | outcome |
|---|---|---|
| raw-locality | no partition | legacy record, reads |
| partitioned | same partition | partitioned record, reads |
| raw-locality | a partition | **refused** — the index value is truncated or damaged, not older |
| partitioned | no partition | **refused** — index claims a partition the record does not carry |
| partitioned | a different partition | **refused** — the carriers disagree |

The third row is the one that matters: it is the only way a truncated V5 value is
distinguishable from a genuine V4 value, and it is why placement is never decided
by the index alone.

Frame presence is **per row**, not per record: the column holds a value at rows
written under partitioned placement and a null at rows that were not, so
partition zero stays a legitimate partition and the requested row — never a
neighbour — governs. One central resolver performs this cross-check, wired after
every read-side record decode; an architecture guard holds the census of decode
sites equal to resolver calls and confines `PlacementKey` construction to the
modules that own placement.

**A fixed-layout struct's size is its contract.** Reserved padding may absorb a
new field only where the padding already exists and the size is unchanged;
anything larger is a protocol version bump, never a silent widening.
`TableWriteRequestMeta` carries three padding bytes, which is not room for a
partition, so a partition does not ride the write meta: it is derived per row at
the write site, where the descriptor and its `prefix_bits` are known.

### 8. Which writes are partitioned

Ingress derives the partition at the descriptor-aware write boundary — the
multirow packer, where the descriptor and its `prefix_bits` are in scope — and
only there. A table writes partitioned rows iff its record layout is `MultiRow`
and its locality policy carries a prefix (table-spread or column projection).
`SingleRow` tables, identity-projection localities, KV records, and
secondary-index segment rows stay raw-locality: partitioned placement exists to
let records group, and those either cannot group or carry no prefix to group on.

A tombstone is placed with the rows it deletes, so delete ingress derives the
same partition. The packer asserts that no packed record spans partitions rather
than assuming bucketing keeps it true. Fan-out routes every child by its
placement key; the frame column is emitted only when at least one row is
partitioned, so legacy frames stay byte-identical to before this record.

## Acceptance

- The partition of a locality is identical across differing membership epochs,
  node counts, and replication factors. The control varies the membership and
  asserts equality, so a partition function that quietly depends on topology
  fails.
- No ownership boundary falls inside a partition, over a generated set of
  memberships including single-node, RF larger than the node count, and node
  join and departure. The control asserts every partition resolves to one replica
  set, not that boundaries "usually" align.
- Every row of a shared record is discoverable by range scan and resolvable by
  point lookup, including rows that are not the record's first. This fails on the
  present ingest, which registers only the first row.
- After a membership change, every row of a record resolves to the same owner.
  The control performs an actual epoch swap rather than asserting a property of
  the mapping in isolation.
- A read concurrent with a partition transfer returns each row exactly once —
  neither dropped nor duplicated.
- Interval semantics are unchanged: intervals crossing, starting inside, ending
  inside, and wholly inside a span still yield exactly the rows inside them.
- A record written under raw-locality placement remains readable after partitions
  land, with no rewrite and no migration step. The control writes under legacy
  semantics, brings partitions up, and reads.
- A scan spanning legacy and partitioned records returns every row exactly once,
  in index order, with each record resolved by its own placement semantics.
  Mixed state is a supported steady state, so this is a permanent control.
- A legacy locality whose numeric value is also a valid partition id still routes
  by legacy rules. This is the discriminating control for decision 6: an
  implementation that reinterprets the value instead of consulting the stamp
  passes every other test here and fails this one.
- A membership change with both legacy and partitioned records present relocates
  each by its own rule in one epoch swap, and every row still resolves
  afterwards — none dropped, none duplicated.
- Every carrier that gains a placement stamp decodes artifacts written before it
  did, by its own mechanism: a V4 index value, an Arrow frame without the
  column, a snapshot at the previous format version. Each has a control reading
  a genuine older artifact, not a synthesized one.
- No durable structure gains a `#[serde(default)]` field as its compatibility
  story. A control asserts that a truncated postcard body fails to decode rather
  than yielding a defaulted value, so the mechanism cannot regress into an
  assumption.
- A V5 index value truncated to a V4 length, paired with a partitioned record
  frame, is refused rather than read as legacy. This is the control that
  distinguishes truncation from an older record; without it the two are
  byte-identical and length dispatch reports the wrong one.
- A stamp-absent artifact decodes as `RawLocality` through a successful decode
  path. A corrupt artifact still errors: absence and damage never share an
  outcome.
## Guardrails

Placement semantics are read from the record, not inferred from its values. A
code path that decides how to route by inspecting a locality or partition value,
rather than the durable marker, is the defect decision 6 forbids.

Placement is computed from the partition. A call that resolves replicas from a
raw locality, once this record lands, is a defect and not an optimization.

Partition cardinality is durable. It may not be tuned per table, per workload, or
per deployment without a migration record.

A grouping key must be provable to hold more than one row and to be stable under
membership change. Both properties need their own controls; the first is what
ADR 0089's prefix grouping failed, the second is what this record exists for.

## Risks and deferrals

Partition cardinality is a real trade-off left to implementation: too few
partitions skew load and cap parallelism, too many inflate placement metadata and
transfer bookkeeping. The choice needs evidence, not a default.

Existing records carry no stamped partition. Because the partition is a pure
function of the routing locality, existing rows can be classified without a
rewrite; whether the stamp is backfilled during compaction or derived on read is
an implementation decision this record does not make.

Not addressed: rebalancing mechanics and their throughput, the ring's vnode count
and its load-skew behaviour, and whether `spread_factor` of four remains the right
placement spread. This record fixes what a partition is and who owns it, not how
fast a cluster can move one.

## Forward

ADR 0089 resumes once partitions exist, with the partition id as its grouping key
and its per-record read coalescing unchanged. The point-lookup and interval
discoverability gates written for 0089 apply here without modification and should
land with this record, since per-row indexing of shared records is decided here.
