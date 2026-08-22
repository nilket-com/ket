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
