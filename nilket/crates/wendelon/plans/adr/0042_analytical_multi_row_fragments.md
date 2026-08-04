# ADR 042: Analytical Multi-Row Slab Fragments

Author: AI Swarm Council (Claude / Codex / Gemini)

## Status

Accepted

## Context

ADR 037 §11 pinned single-row slab records as the V1 storage shape:
each row of every multi-row write expands to its own
`SlabRecordHeader` (72 bytes), its own Arrow IPC payload, its own
4 KiB-aligned slab append, and its own redb index entry. ADR 037
§"Consequences" explicitly accepted the cost ("65,536
`SlabRecordHeader`s, 65,536 BLAKE3 computations, 65,536 index inserts"
on a max-sized bulk write) on the explicit promise that *analytical
multi-row fragments are deferred to a follow-up ADR when the cost
profile warrants*.

That follow-up is now overdue on a different axis. ADRs 038–040 turned
the read path into a real warehouse engine:

- 038 narrowed scans to a bounded arc set via column-aware locality.
- 039 made slab-footer column statistics the gate that lets the read
  path skip whole slabs without decryption.
- 040 collapsed predicate-filtered reads into single merged
  aggregates, with a CSB short-circuit that answers
  `MIN`/`MAX`/`COUNT` from one slab tail per slab.

Each layer compresses what has to leave storage. The remaining
floor is the *per-row constant* baked into ADR 037: 72 bytes of
header, one BLAKE3, one O\_DIRECT-aligned record, and one redb row
per logical row. For a billion-row analytical table, this is the
dominant component of both write cost and on-disk overhead — and
it sits *below* every optimization 038/039/040 added.

This ADR relaxes ADR 037 §11 for analytical-shaped tables: a slab
record may carry a packed N-row Arrow batch with a row-level
identity index inside it. Single-row records are not deprecated —
they remain correct and they remain the right shape for
point-heavy / OLTP-shaped tables.

## Non-goals (V1)

- **No PK-less append-only tables.** ADR 045 territory.
- **No row-level DELETE / row-tombstone model.** Defers to ADR 045
  or a sibling.
- **No per-fragment CSB catalog.** Stats stay per-slab (ADR 039
  unchanged).
- **No PBC identity/locality metadata opcodes in V1.** ADR 035's
  scalar `LOAD_IDENTITY` and `ROUTE_BY_LOCALITY` are removed
  entirely (§8); future vector-form replacements need a pulled
  workload and a follow-up ADR.
- **No grouped AGG work.** ADR 040 follow-up territory.
- **No new public HRFT frame family.** ADR 042 is a storage-layout
  ADR, not a wire-protocol ADR.
- **No project-name-prefixed protocol acronyms, reject codes,
  metric prefixes, module names, or helper types** (per
  `plans/conventions/no-project-name-in-protocols.md`).
- **No new `V1` / `V2` suffixes** on Rust types, descriptors,
  capabilities, or helper structs (per
  `plans/conventions/no-version-suffixes.md`).
- **No full LSM-tree adoption.** Forward-compat analysis confirms
  ADR 042 leaves room; the LSM ADR opens later under its own
  forcing function.

## Decision

### 1. Layout choice is a per-table descriptor field

Add to `TableSchemaDescriptor`:

```rust
enum RecordLayoutPolicy {
    SingleRow,                              // ADR 037 default
    MultiRow(MultiRowLayoutDescriptor),
}

struct MultiRowLayoutDescriptor {
    target_rows: TargetRowCount,            // newtype, see §12
    max_rows: MaxRowCount,                  // hard cap per record
    max_record_bytes: ByteCount,            // hard cap per record
}
```

V1 ships exactly two variants: `SingleRow` (default for typed tables
under ADR 037, default for legacy KV) and `MultiRow`. The
locality-bucket discipline (§6) is enforced unconditionally for
`MultiRow` — no `BucketStrategy` enum because single-bucket is the
only V1 strategy and per
`plans/conventions/no-premature-backward-compat.md` we don't reserve
the enum for hypothetical alternatives.

The name is record-level (`RecordLayoutPolicy`) rather than
table-level: layout is a property of how records pack rows, and
the descriptor field selects which policy that table's records
use.

Layout policy is set at table creation; changing it on an existing
table requires an explicit re-cluster migration (same discipline as
ADR 037 §20 for locality-policy changes).

`target_rows` is the write-path target; `max_rows` is the hard
ceiling that compaction outputs must also obey. `max_record_bytes`
is a parallel byte-shaped ceiling (a wide schema may hit the byte
cap before the row cap).

**V1 defaults and bounds:**

- `target_rows = 4096`.
- `max_rows = 65_536` (matches ADR 037's existing write cap).
- `max_record_bytes = 1 MiB` default.
- `max_record_bytes` hard ceiling = 8 MiB; descriptor validator
  rejects any value above this. The ceiling sits at the
  intersection of "NVMe / object-store readers prefer larger
  reads" and "point-read amplification stays bounded."

ADR 042 is a *storage-layout* ADR, not a wire-protocol ADR. The
public table write frame and peer replication surfaces remain
row-semantic — the storage payload replicated to peers is bytes
plus row-index metadata, not a new HRFT frame family.

### 2. Slab record physical shape

A multi-row slab record reuses the existing slab preamble:

```text
[SlabRecordHeader : 72B] [ Arrow IPC bytes : frame_len ] [ zero pad -> 4K ]
```

`SlabRecordHeader` is unchanged — it remains the torn-write and
checksum boundary for the whole record. For multi-row records the
header's `key_identity` is a *physical record identity* (e.g.,
BLAKE3 of the IPC payload), not a logical row identity; the
header's `hlc` is the maximum row HLC inside the record.

A reader distinguishes single-row vs. multi-row by the IPC schema
contents (presence of storage-only synthetic columns, §3) — not by
a header flag, and not by a magic byte at the payload start. The
descriptor is the authority for which shape the IPC carries.

The IPC schema is the only schema-evolution surface. Future shapes
extend by appending storage-only columns, never by reserving slots
or version bytes outside Arrow.

### 3. Per-row identity via storage-only synthetic columns

Every row in a multi-row record carries its identity material as
three storage-only synthetic columns appended to the user's
declared schema *only on the storage side*:

```
__row_hlc__               FixedSizeBinary(16)   // HLC bytes per row
__row_key_id__            FixedSizeBinary(32)   // key_identity per row
__row_routing_locality__  UInt64                // descriptor-derived locality
```

The user's Arrow schema as declared in the descriptor stays exactly
as ADR 037 defined it. The storage path appends these three columns
at write/compact time and strips them on read before the data leaves
the storage layer. They never appear in PBC programs, AGG inputs,
query responses, or point-read responses. They are never accepted
from clients.

The synthetic-column names follow
`plans/conventions/no-project-name-in-protocols.md` — `__row_*__`,
not `__wendelon_row_*__`. The names are part of the on-disk
storage contract; brand prefixes would leak into recovery,
diagnostics, and any external tool that inspects raw IPC.

Recovery from raw slab bytes decodes the Arrow IPC, reads the
synthetic columns, and rebuilds the redb key-index and locality-
index entries — *without descriptor context*. That's the load-
bearing reason `__row_routing_locality__` is mandatory rather than
recomputable: recomputing locality requires PK canonicalization,
which requires descriptor context that recovery cannot rely on.

The header's `hlc` becomes `max(__row_hlc__)` for the record. For
single-row records, the synthetic columns are absent and the
existing decode path applies unchanged. A reader distinguishes
single-row vs. multi-row by sniffing for synthetic-column presence
in the IPC schema — the descriptor is the authoritative source.

### 4. Index entry granularity

`RecordLocation` extends uniformly with a row-position field:

```rust
struct RecordLocation {
    slab_id: SlabId,
    offset: u64,
    length: u32,
    hlc: HybridTimestamp,
    record_row_index: RecordRowIndex,    // newtype around u32; 0 for SingleRow
}
```

For `SingleRow` tables, `record_row_index` is always
`RecordRowIndex::ZERO`. For `MultiRow` tables, it names the row's
ordinal in the IPC batch.

The MVCC version-chain invariant from ADR 037 is preserved:

```text
(tenant_id, key_identity) -> Vec<RecordLocation sorted by hlc>
```

For multi-row index entries, `RecordLocation.hlc` is the row HLC
from `__row_hlc__`, not the physical record header's max HLC. That
preserves the existing version-chain and `lookup_as_of` behavior.

The redb persistent index gains a 4-byte field per row across the
board. Acceptable cost (see Consequences); the unification keeps
single-row vs. multi-row a field value rather than a second
addressing abstraction.

### 5. Point read decode flow

For point reads at a specific `as_of_hlc`, the storage layer:

1. Resolves `(tenant_id, key_identity)` → `Vec<RecordLocation>`
   sorted by `hlc` ascending, exactly as ADR 037 does today.
2. Binary-searches for the latest version `<= as_of_hlc`.
3. Reads the slab record at `(slab_id, offset, length)`.
4. For `record_row_index == 0` on a single-row record: returns
   the existing single-row Arrow IPC payload unchanged. For a
   multi-row record: decodes the IPC, projects via
   `take(&[record_row_index])`, strips synthetic columns,
   returns a single-row batch shaped like ADR 037's typed-table
   payload.

Step 3 is the only one that grows for multi-row reads — the read
returns more bytes than the caller needs. Mitigation in §10.

### 6. Locality is bucket-pure within a record

A multi-row record contains *only* rows that share a locality
bucket under the table's `LocalityPolicy`. The
`SingleBucket` strategy enforces this at write/compact time.

This is the discipline that keeps ADR 038's locality narrowing
working: a scan of locality range `R` admits a slab only if `R`
intersects the slab's covered locality buckets. Multi-row records
that mix buckets would force admission whenever *any* contained
row falls in `R`, which dilutes ADR 038's whole point.

The cost is that a write batch spanning multiple buckets fans out
into multiple multi-row records (one per bucket present in the
batch). For tables with `TableSpreadProjection {
spread_factor: 4 }`, the worst case is 4 records per write batch
instead of 1. Still far better than 65,536.

### 7. PBC composes natively

PBC operates on `RecordBatch`. Multi-row records *are* batches
already, so the read path becomes:

1. Slab admission (ADR 038 locality + ADR 039 CSB pruning).
2. Decode the Arrow IPC of each admitted record.
3. **Strip synthetic columns** before passing to PBC.
4. Run PBC on the resulting batch.
5. (For aggregate queries) Fold survivors into AGG accumulators
   per ADR 040.

ADR 037 §13's mandatory rebatch step still exists but its scope
shrinks: it only fires for `RowRecords` tables and for
cross-record concatenation when an individual fragment record
falls below the vectorization threshold.

The query path also gains a new logical step for multi-row tables:
**group selected multi-row locations by enclosing physical record**
before decoding, so each record is decoded at most once per query
even when many of its rows survive predicate selection.

### 8. PBC scalar identity/locality opcodes are removed

ADR 035's `LOAD_IDENTITY` and `ROUTE_BY_LOCALITY` opcodes consume
one scalar per batch — semantics that don't compose cleanly with
multi-row records. Per
`plans/conventions/no-premature-backward-compat.md`, the opcodes
ship in dev code only with no deployed PBC base to protect; the
right answer is to delete their executor implementations as part
of ADR 042. The predicate language gets simpler and the executor
surface shrinks.

PBC sees user columns only. Identity and routing locality stay
storage-internal — carried by the synthetic columns, stripped
before the batch reaches PBC, and unavailable to predicates until
a future ADR introduces vector-form metadata opcodes.

When Toron's planner first needs per-row identity or locality
inside a predicate, that follow-up ADR introduces vector-form
opcodes that consume the `__row_key_id__` and
`__row_routing_locality__` synthetic columns as Arrow inputs
directly. Until then, the predicate language is smaller and
faster. This is the same day-14 fresh-start license ADR 041 used
at the identifier layer, applied at the opcode layer.

### 9. AGG protocol unchanged

ADR 040's wire format and accumulator catalog are entirely
unchanged. The CSB short-circuit semantics (§5 of ADR 040) are
unchanged at the protocol layer because they operate on per-slab
visibility metadata (`tombstone_metadata`, `seal_hlc`); the proof
extends in §11 below to add `supersession_metadata`, but the
external AGG contract does not change.

The performance-relevant change is internal: the storage path
that folds survivors into accumulators no longer pays the
per-record decode/header/index cost for every logical row. For a
1B-row analytical table with `target_rows = 4096`, AGG-folding
visits ~244K records instead of 1B.

### 10. Read amplification for point reads is bounded

A point read on a multi-row table reads the entire enclosing
physical record. With `max_record_bytes` capped per descriptor
(default 1 MiB, hard ceiling 8 MiB), worst-case overhead is
bounded but real.

For warehouse-shaped point reads (rare; analytical tables don't
optimize for them), this cost is acceptable. For point-heavy
tables, the descriptor sets `RecordLayoutPolicy::SingleRow` and
the cost doesn't apply. The
`storage_multirow_point_read_amplification_bytes` histogram
exposes this to operators so a misconfigured layout choice
surfaces in metrics.

### 11. CSB direct-answer soundness gains a supersession proof

ADR 040's CSB short-circuit (§5) requires the slab be
*visibility-complete* at `as_of_hlc`: `seal_hlc ≤ as_of_hlc` AND
no tombstone applies before `as_of_hlc`. That proof is correct
for tombstones but **incomplete under multi-version writes**:
when a row is updated (not deleted), the new version lives in a
newer slab; the older slab's CSB still contains the *old* value's
contribution to `MIN`/`MAX`/`COUNT`. A point read at
`as_of_hlc ≥ update_hlc` resolves to the new version, but a CSB
direct answer over the old slab returns a stale aggregate.

This soundness gap is **not introduced by ADR 042** — it exists
today on `RowRecords` tables — but ADR 042 makes it more visible
because fragment slabs are larger and more attractive for direct
answers. The proof needs an additional field on the TieredSlab
pointer:

```rust
enum SupersessionMetadata {                  // mirrors TombstoneMetadata
    Untracked,                               // pre-amendment pointer
    None,                                    // no superseding write known
    Earliest(HybridTimestamp),               // earliest superseding HLC
}
```

The CSB short-circuit visibility check from ADR 040 §5 extends:

```text
slab.seal_hlc ≤ as_of_hlc
AND slab.tombstone_metadata is tracked
   AND no tombstone at or before as_of_hlc
AND slab.supersession_metadata is tracked
   AND no superseding write at or before as_of_hlc
```

`supersession_metadata` is maintained by four well-defined hooks:

- **On append of a new row version**: if the previous latest
  version lives in a sealed or tiered slab, record the new
  version's HLC on that slab's `supersession_metadata`. The
  state-machine apply observer (same mechanism ADR 040 uses for
  tombstones) fires the update.
- **On active-slab seal**: seal-time computation derives the
  earliest internal supersession HLC for the new slab from the
  row version chains it just sealed.
- **On retention / tombstone-aware compaction**: the rewrite
  preserves or recomputes `supersession_metadata` on output
  pointers from the survivor row HLCs.
- **Sweeper backlog convergence**: a backlog pass over committed
  writes ensures pre-042 sealed slabs that have been superseded
  transition `Untracked → tracked` once the sweeper reconciles.

`Untracked` fails closed, matching ADR 040's tombstone discipline.
A new metric mirrors the tombstone one:
`agg_csb_short_circuit_skipped_supersession_metadata_untracked_total`.

ADR 042 carries this change because it is the natural forcing
function — fragments make the per-row vs. per-slab distinction
explicit — and because the impl work composes cleanly with the
row-locator index that 042 introduces.

### 12. New newtypes (per `plans/conventions/newtype-units.md`)

```rust
struct TargetRowCount(u32);     // 1..=max_rows
struct MaxRowCount(u32);        // 1..=65_536, descriptor hard cap
struct RecordRowIndex(u32);     // 0..=max_rows-1
```

`MaxRowCount` upper bound matches ADR 037's existing 65,536-row
write cap; future LSM-style large-segment layouts (see Forward-
compatibility analysis) would extend this in their own ADR.

### 13. Compaction modes and ordering

Compaction shapes ADR 042 enables:

- `SingleRow → SingleRow` (today, unchanged).
- `MultiRow → MultiRow` (recompaction): merge two multi-row
  records whose combined rows fit `max_rows` and combined bytes
  fit `max_record_bytes`. Per-row HLCs preserved via
  `__row_hlc__`. New record's header HLC = max of inputs.
- `SingleRow → MultiRow` and `MultiRow → SingleRow` (descriptor
  policy change): explicit re-cluster migration only, never
  implicit.

Every rewrite recomputes CSB and visibility metadata from
survivor rows. Rewrites preserve logical row identity exactly;
physical record offsets and row indices may change.

**Single coherent compaction.** When the sweeper rewrites a slab,
it performs **all** applicable upgrades in one atomic pass on the
output pointer:

- multi-row recompaction (this ADR);
- ADR 040 `tombstone_metadata` upgrade `Untracked → tracked`;
- ADR 042 `supersession_metadata` upgrade `Untracked → tracked`
  (§11);
- (future) LSM level promotion when that ADR opens.

A single pointer-swap commits all of them together. Piecemeal
sweeper passes — one for re-pack, another for tombstone tracking,
another for supersession — would multiply write amplification and
fragment the atomicity story. The sweeper's existing
"read N inputs, write M outputs, atomically swap pointer"
primitive (used by retention rewrite and ADR 040's tombstone-aware
reclaim) is the right unit; ADR 042 extends what gets recomputed
inside that unit without splitting it.

**Deterministic row ordering** inside multi-row records. Both
write-time and rewrite-time records sort rows by:

```text
(__row_routing_locality__, __row_key_id__, __row_hlc__, original_ordinal)
```

The synthetic columns make this sort directly expressible in
Arrow. Query execution must not depend on this order for
correctness, but the determinism is useful for: (a) byte-exact
write/compact tests, (b) future leveled compactor merge runs,
(c) operator diagnostics. Write-time and rewrite-time use the
same sort to keep the byte-stable property uniform.

Compaction must preserve `__row_key_id__` exactly per row so
redb index relocations stay sound.

### 14. Migration stays row-oriented

Migration (Plan 019) remains logically row-oriented. The source
side materializes selected multi-row records into the existing
single-row migration record stream; the destination side appends
according to *its* descriptor's layout (which may differ from
the source's). A conservative first pass on the destination can
append `SingleRow` even on a multi-row-table descriptor;
compaction re-packs later. The
`storage_multirow_uncompacted_row_records_total` metric exposes
the unconsolidated backlog to operators.

This keeps the migration wire shape unchanged and avoids
streaming whole multi-row records when only a subset of rows
actually move.

### 15. Stats are still per-slab

ADR 039's CSB stats are computed at slab seal time over all rows
in the slab. Multi-row records change *how rows are packed*, not
*which rows the slab contains*. Stats computation iterates the
union of rows across all records in the slab and produces the
same `min/max/null/non-null` it would have produced under
single-row layout.

The ADR 039 stats writer needs one small adjustment: it currently
iterates `SlabRecordHeader`s and decodes one IPC per header.
Under multi-row layout, each header refers to a record whose IPC
contains many rows, and the stats writer strips synthetic columns
before folding rows into the per-column stat builders. The
per-row iteration shape is otherwise the same.

### 16. Tombstone metadata composes unchanged

`MigratedRangeTombstone` ranges are locality-based, not row-based.
A multi-row record's locality bucket (guaranteed single-bucket
per §6) falls within or outside a tombstoned range; per-row
breakdown is irrelevant. ADR 040's `tombstone_metadata` field
stays at slab granularity.

When row-level deletes (DELETE ... WHERE) eventually land — likely
ADR 045 (PK-less append-only) or its sibling — they will need
per-row visibility metadata. The natural fit: extend the
synthetic-column set with `__row_tombstone__: Bool` (or use a
sentinel value in `__row_hlc__`) so deleted rows can be carried
through compaction until a retention floor lets them be dropped.
*This ADR deliberately does not pre-allocate that space*; per
`plans/conventions/no-premature-backward-compat.md`. The future
ADR designs the shape with the row-delete semantics it
introduces.

## Rationale

**Why per-table opt-in, not cluster-wide.** Different tables have
different access shapes. ADR 037 already established the descriptor
as the per-table policy carrier. Adding `RecordLayoutPolicy` is
the additive shape. Cluster-wide forced multi-row would penalize
point-heavy workloads without recourse.

**Why a descriptor field rather than a slab-format flag in
`SlabRecordHeader`.** The header is already the torn-write and
checksum boundary; a header flag for "this is a fragment" carries
no information that the envelope magic doesn't already carry, and
creates a place where the disk and the descriptor can disagree.
Sniffing the magic in the payload bytes is cheap and keeps
recovery descriptor-free.

**Why synthetic Arrow columns rather than an explicit row
directory.** The deciding arguments:

- *One binary format, not two.* A row directory introduces a
  parallel encoder/decoder alongside Arrow IPC. Synthetic
  columns let every read path work entirely in Arrow — strip,
  decode, project, return.
- *Recovery cost is small.* Arrow IPC decode is fast (sub-
  millisecond per record at 256 KiB) and recovery is rare.
  The "row directory avoids Arrow decode" argument optimizes
  the wrong axis.
- *`__row_routing_locality__` makes recovery descriptor-free* —
  no PK canonicalization needed during disaster recovery.
- *Storage cost is identical.* 56 bytes/row of identity material
  whether it lives in a fixed-width directory entry or three
  fixed-width Arrow columns.

The synthetic columns are *storage-only*: appended at write/
compact time, stripped at every read path before data leaves
the storage layer. They never appear in user-visible output,
never accept client input, and never appear in PBC programs.

**Why three synthetic columns, not two.** `__row_routing_locality__`
exists specifically so recovery from raw slab files can rebuild
the locality index without descriptor context. Recomputing
locality from PK values requires PK canonicalization, which
requires schema knowledge — a circular dependency in disaster
recovery scenarios. Persisting the locality value sidesteps it.

**Why per-row HLC rather than reusing a write-time per-batch
HLC.** Compaction mixes rows from different writes. A multi-row
record that started homogeneous (all rows at HLC `t1`) becomes
heterogeneous once recompaction merges in rows from HLC `t2`.
Per-row HLC means MVCC visibility resolution is local to the
synthetic-column read; no out-of-band metadata required.

**Why the locality `SingleBucket` discipline.** ADR 038 narrows
scans to bounded arc sets specifically so cold-storage slabs that
can't be locality-relevant are skipped. Mixing buckets in one
record would force the planner to admit that record on any of its
constituent buckets — the worst-case admit set is the *union*,
not the *intersection*. That destroys the locality narrowing for
mixed-bucket records and leaks complexity into the read planner.
Restricting packing to single-bucket keeps locality semantics
hermetic.

**Why a flat `record_row_index` rather than a `RowSelector`
discriminated union.** The codebase already treats
`RecordLocation` as the address stored in version chains; adding
`record_row_index` keeps the existing type and makes single-row
vs. multi-row a field value rather than a second addressing
abstraction. The 4 bytes/row across all index entries is real
cost, but the alternative forces a `Vec<RecordLocation>` →
`Vec<RowLocation>` refactor across every consumer. Smaller
refactor wins; the storage delta is bounded.

**Why scalar PBC opcodes are removed entirely rather than
degraded or rejected.** ADR 035's `LOAD_IDENTITY` and
`ROUTE_BY_LOCALITY` carry one scalar context per batch; multi-row
records have N. The day-14 fresh-start license codified in
`no-premature-backward-compat.md` applies cleanly: no deployed
base depends on these opcodes, so the simplest resolution is to
delete them. The executor surface shrinks; future vector-form
opcodes (when Toron pulls) consume `__row_key_id__` and
`__row_routing_locality__` directly. KISS wins.

**Why a single coherent compaction operation.** Each tracking-
metadata upgrade (tombstone, supersession, future LSM levels)
has its own "earliest event HLC" semantics, but the *primitive*
they all use is the same: rewrite output, swap pointer. Splitting
those upgrades across separate sweeper passes would multiply
both write amplification and the atomicity surface area
(separate pointer-swaps mean separate windows where partial
state is observable). Bundling them into one rewrite keeps the
existing primitive load-bearing and lets compaction be the
universal "fix all the things" operation.

**Why no envelope-level versioning beyond `format_version`.**
The envelope carries `format_version: u16` because it's a
real on-disk format that may evolve. Per
`plans/conventions/no-premature-backward-compat.md`, V1 sets it
to 1 and ships. No reserved slots, no `RecordFormatVersion` enum
at the Rust type level (per `no-version-suffixes.md`).

**Why default `target_rows`, `max_rows`, and `max_record_bytes`
are tunables and not pinned constants.** Workload-driven. A
column-rich analytical table (50 columns, 16 bytes/value) hits
1 MiB at ~1310 rows; a column-sparse table (3 columns, 4
bytes/value) hits the same byte cap at ~87K rows. Both are
valid; neither is the "right" default. Stabilization-watch
metrics (impl plan) inform default tuning post-merge.

**Why this is ADR 042 and not part of an ADR 037 amendment.** The
descriptor field, the IPC content discipline, the index extension,
the read-path simplification, and the compaction modes together
form a coherent unit. Bolting any one onto 037 in isolation makes
the rest of the design illegible. A new ADR keeps the design
self-contained.

## Consequences (the negatives)

- **`RecordLocation` and the redb record row grow by 4 bytes
  per logical row.** `record_row_index` is uniformly present
  on every index entry, even single-row tables that derive no
  benefit. At ~1B rows, ~4 GB additional index footprint;
  fixed fractional growth.
- **Write batches spanning multiple locality buckets fan out
  into multiple multi-row records.** The promised "1 record
  per write" only holds when the write batch is bucket-pure.
  Mixed writes pay the bucketing overhead; this is the cost
  of keeping ADR 038's locality narrowing sound.
- **Point reads on multi-row tables read the whole enclosing
  record.** Bounded by `max_record_bytes` but real. Operators
  who set `RecordLayoutPolicy::MultiRow` on a point-heavy
  table are accepting this trade.
- **Memory spike on multi-row decode.** Reading and decoding a
  multi-MiB Arrow batch peaks at the record's payload size in
  resident memory until the row of interest is extracted. The
  cap on `max_record_bytes` is the operator's tool to bound
  this. Streaming decode would help on the IPC side but adds
  complexity; defer.
- **Compaction code grows.** Three new shapes (multi-row →
  multi-row recompact, single→multi migrate, multi→single
  migrate). The migrate forms are explicit operator actions
  with their own audit trail; recompact is a sweeper extension.
- **Recovery decodes Arrow IPC to rebuild multi-row index
  entries.** Slower than the existing 72-byte-header-only path
  but still single-pass and bounded by IPC decode speed
  (sub-millisecond per record).
- **Synthetic columns appear in the on-disk Arrow schema for
  multi-row tables.** An external observer reading the raw IPC
  sees `__row_hlc__`, `__row_key_id__`, and
  `__row_routing_locality__`. They are intentional system
  columns; their presence is documented by this ADR.
- **A new TieredSlabPointer field (`supersession_metadata`).**
  Same shape and discipline as ADR 040's `tombstone_metadata`,
  with parallel apply-observer wiring and reclaim-loop
  convergence. Adds ~16 bytes per pointer when populated; one
  CAS per superseding write.
- **Stats writer iterates a slightly different shape.** ADR 039
  stats path needs one focused change to strip synthetic
  columns and fold per-row stats; the abstraction (rows in,
  stats out) is preserved.
- **PBC loses scalar identity/locality opcodes.** Future
  metadata predicates need vector-form opcodes (deferred
  follow-up ADR). Until that ADR opens, predicates over row
  identity or routing locality are unavailable inside PBC.
- **The descriptor surface grows.** `RecordLayoutPolicy` is a
  new variant set Toron must understand. Negotiation between
  Toron and Wendelon at table create remains under ADR 032's
  `SchemaUpdate` envelope.

## Rejected alternatives

- **Discriminator bit in `SlabRecordHeader`.** The descriptor
  is the authority; sniffing for synthetic columns in the IPC
  schema is a sufficient secondary signal for any path that
  needs to dispatch without a descriptor lookup.
- **Explicit row directory in a fragment envelope before the
  Arrow IPC blob.** Introduces a parallel encoder/decoder; the
  recovery-speed benefit is small relative to the code-organization
  cost. Synthetic columns let every read path stay single-format.
- **Cluster-wide forced multi-row layout.** Penalizes point-heavy
  tables; contradicts the per-table descriptor discipline ADR 037
  established.
- **No per-row HLC; rely on a single record-level HLC.** Breaks
  under compaction once two multi-row records from different
  writes merge. Either the design forbids cross-write compaction
  (which kills the optimization) or it carries per-row HLCs
  (this ADR).
- **`RowSelector { WholeRecord, FragmentRow { ordinal } }`
  discriminated union plus a parallel `RowLocation` type.**
  Trades 3 bytes/row on single-row tables for a `Vec<RecordLocation>`
  → `Vec<RowLocation>` refactor across every consumer. The flat
  `record_row_index` field is the smaller change.
- **Recompute routing locality from PK during recovery instead
  of persisting it.** Requires descriptor context during
  recovery, which disaster-recovery scenarios may not have.
  Persisting `__row_routing_locality__` makes recovery
  descriptor-free.
- **Reserve a `RecordFormatVersion` field for future variants.**
  Convention violation per `no-premature-backward-compat.md`.
  The IPC schema's column set is the format signal; future
  variants append columns.
- **Allow multi-bucket multi-row records.** Destroys ADR 038's
  locality narrowing for any record straddling the boundary.
  The fan-out cost of bucket-pure packing is acceptable.
- **Keep scalar PBC opcodes alive (degrade per-row or refuse
  with new reject code).** Both shapes preserve executor cost
  for opcodes with no deployed users. The day-14 fresh-start
  license per `no-premature-backward-compat.md` favors deletion;
  future vector-form opcodes consume the synthetic columns when
  Toron pulls.
- **Vector identity/locality PBC opcodes in V1.** Doubles the
  PBC executor surface for a workload class that hasn't been
  pulled. Refuse loudly now; add the opcodes when Toron's planner
  has a query that wants them.
- **Make `record_row_index` an `Option<RecordRowIndex>` for
  single-row tables.** Forces a branch in every redb encoder /
  decoder for a 4-byte savings per single-row index entry.
  Unification wins.
- **Per-table tunable `BucketStrategy` with a `Mixed` option for
  workloads that want fewer fan-out records.** Trades ADR 038
  soundness for write throughput; the right way to reduce fan-out
  is to widen `target_rows` or shrink `spread_factor`, not to
  weaken locality semantics.
- **A "multi-row light" mode that packs only writes already
  arriving as a single bucket-pure batch.** Half the design with
  none of the read-side wins; still pays per-row index cost on
  recompactions that span batches.
- **Allow CSB direct answers based only on `tombstone_metadata`
  for multi-row slabs.** Newer row versions hide older rows
  without being tombstones; the proof needs supersession metadata
  too (§11).
- **Bundle a full LSM tree into ADR 042.** Fragment format and
  row-locator indexing are already a large storage change. LSM
  levels, run metadata, read merge, and compaction scheduling
  belong in a dedicated ADR once fragments are available as the
  physical unit.

## Forward-compatibility analysis: LSM-style compaction

This ADR does **not** adopt LSM-tree compaction. Wendelon today is
not an LSM (no memtable, no levels, no per-segment bloom filters,
no manifest-driven version sets); ADR 042 does not change that.

This section asks a narrower question: *if* a future ADR pulls
Wendelon toward LSM-style leveled compaction, do the choices in
ADR 042 admit that move cleanly, or do they ossify the substrate
in a hostile direction? Surfacing this now is cheaper than
discovering an incompatibility after 042 ships.

The headline answer: most of ADR 042 is LSM-friendly by accident
of good factoring. A few specific choices have forward-compat
implications worth flagging.

### Forward-friendly properties

- **Synthetic columns (§3)** carry the per-row identity,
  locality, and HLC an LSM compactor needs for MVCC merge —
  self-describing inside the IPC, no descriptor lookup required
  during merge.
- **`SingleBucket` packing (§6)** partitions records by locality
  bucket, the shape leveled compaction wants for
  non-overlapping-within-level invariants.
- **`tombstone_metadata` (ADR 040) + `supersession_metadata` (§11)**
  give the earliest-relevant-event-HLC analysis a future
  compactor needs to decide what's safe to drop on merge.
- **Variable-sized records bounded by `max_record_bytes` (§10)**
  match LSM segment shape; nothing hardcodes uniform sizes.
- **Compaction sort order (§13)** —
  `(__row_routing_locality__, __row_key_id__, __row_hlc__,
  original_ordinal)` — lets a future leveled compactor run
  merge-runs over already-sorted inputs.
- **Pointer-swap compaction primitive.** Retention rewrite, ADR
  040 tombstone-aware reclaim, and §11 supersession upgrade all
  share the "read N, write M, atomic pointer swap" primitive LSM
  compaction is built on. The single-coherent-compaction
  discipline (§13) makes this composability explicit.

### Forward-compat concerns to flag (not resolved here)

1. **Within-bucket key-range overlap.** `SingleBucket` does not
   guarantee non-overlapping key ranges within a bucket — two
   records in the same bucket can both contain key `K` at
   different HLCs. Correct under MVCC, but classical leveled
   compaction (LevelDB / RocksDB) requires non-overlap at L1+.
   A future LSM ADR picks size-tiered (overlap allowed) or adds
   merge-promotes-overlapping-peers logic; ADR 042 leaves the
   choice open.
2. **Global redb index vs. per-segment.** ADR 042 increments
   the global-index cost (4 bytes/row for `record_row_index`).
   If a future LSM moves to per-segment metadata + segment bloom
   filters, redb becomes a manifest-only structure. The
   `record_row_index` field migrates cleanly because the ordinal
   is already record-relative. The forward-cost note: writing
   both global and segment metadata would be real write
   amplification.
3. **`target_rows` cap vs. LSM segment sizes.** ADR 042 caps
   `target_rows` at 65,536; LSM segments often hold millions.
   Future LSM either raises the cap or composes leveled
   segments from multiple records per slab — the latter
   diverges "segment" from "slab" vocabulary, forward-compatible
   but worth flagging.
4. **Per-row tombstones for row-DELETE.** §16 defers them; the
   natural shape is `__row_tombstone__: Bool` joining the
   storage-only column set. The row-delete ADR (whenever it
   lands) should be designed with multi-row recompaction in
   mind so tombstones can be dropped once below all surviving
   versions in the segment.
5. **Stats footer + filter-cache promotion.** ADR 039 places CSB
   per-slab. A future LSM may want per-level filter caches in
   memory, consulted before any segment read. The 039 footer is
   compatible with hoisting filters into a memory cache, but the
   cache eviction strategy isn't designed for LSM read-path
   frequency. That promotion belongs in the LSM ADR, not a 039
   amendment.

### Net assessment

ADR 042 does not bake in LSM hostility. Synthetic columns,
`SingleBucket` packing, per-row HLC, descriptor-driven layout,
and supersession metadata all push in directions that make a
future LSM ADR easier. The two choices most worth keeping in
view: (1) within-bucket key-range overlap remains permitted, and
(2) the global redb index incurs a per-row cost that becomes a
sunk cost if a future ADR shifts to per-segment metadata.

## Deferred to follow-up ADRs

1. **Vector-form identity/locality PBC opcodes.** ADR 042 removes
   ADR 035's scalar-context `LOAD_IDENTITY` /
   `ROUTE_BY_LOCALITY`; their replacements consume
   `__row_key_id__` and `__row_routing_locality__` as Arrow
   column inputs when Toron's planner pulls.
2. **Auto-triggered single→multi migration.** Adopt when manual
   descriptor-edit migration proves operationally noisy.
3. **Row-level tombstones via synthetic columns.** When row-level
   DELETE lands (likely ADR 045 or sibling), the natural shape
   adds `__row_tombstone__: Bool` to the storage-only column set.
4. **PK-less append-only identity.** ADR 045 territory. A PK-less
   table can't populate `__row_key_id__` from a PK; ADR 045
   either picks an alternative identity (content hash, ordinal)
   or extends `__row_key_id__` semantics to non-PK identity.
5. **LSM-style leveled compaction.** Forward-compat analysis
   above confirms 042 doesn't preclude it; the LSM ADR opens
   when measured write-amp pressure justifies it.

## References

- ADR 005 / 035 / 036: PBC predicate protocol — composes natively
  with multi-row records via batch-shaped input.
- ADR 037: tenant table schemas, single-row slab records — this
  ADR relaxes §11 for `MultiRow` tables only.
- ADR 038: column-aware locality V1 — the bucket-pure packing
  discipline preserves its narrowing semantics.
- ADR 039: slab-footer column statistics V1 — stats remain
  per-slab; writer strips synthetic columns before folding rows.
- ADR 040: aggregate pushdown — composes unchanged at the
  protocol layer; major performance beneficiary; §11 of this ADR
  extends ADR 040's CSB short-circuit proof with
  `supersession_metadata`.
- ADR 041: project-name decoupling — synthetic-column names and
  any metric prefixes follow the brand-clean discipline.
- `plans/conventions/newtype-units.md` — `TargetRowCount`,
  `MaxRowCount`, `RecordRowIndex`.
- `plans/conventions/no-premature-backward-compat.md` — no
  reserved-for-V2 fields in the IPC schema; future shapes
  extend by appending columns.
- `plans/conventions/no-version-suffixes.md` — types are
  `RecordLayoutPolicy`, `MultiRowLayoutDescriptor`,
  `SupersessionMetadata`, no V1 suffixes.
- `plans/conventions/no-project-name-in-protocols.md` —
  synthetic-column names, metric segments, and reject prefixes
  are functional, not brand-prefixed.
- ADR 043: Arrow IPC body compression (LZ4) — `Lz4Frame` is
  the layout-driven default for `MultiRow`; coherent
  compaction (§13) is the convergence primitive for the
  compression policy migration; the `max_record_bytes`
  enforcement axis (uncompressed bytes) extends §10.
- ADR 044 (proposed): L2 columnar fetch (sovereign pushdown)
  — column-aware Range-GETs preserving zero-trust;
  composes with `record_row_index` granularity.
- ADR 045 (proposed): PK-less append-only — natural composition
  with multi-row format on append-only event streams.
- ADR 046 (proposed): secondary indexes — composes with
  `record_row_index` granularity.
