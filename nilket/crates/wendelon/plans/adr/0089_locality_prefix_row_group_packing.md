# ADR 0089 - Row-group packing on the durable partition

**Status:** Proposed 2026-08-22. Responds to the per-location phase profile taken
against the live deployment. It changes which rows share a stored record; it does
not change the record format, the response protocol, or the encryption boundary.

> **Unblocked and implemented on ADR 0090's partitions.** The grouping key is
> the DURABLE `PartitionId` carried on each source row — derived once at the
> descriptor-aware write boundary and never recomputed at pack time — so packing
> and placement structurally agree. Rows without a partition keep their full
> locality as the bucket key: legacy behaviour, unchanged. The scan coalesces
> reads and decodes on the canonical physical record identity
> `(slab_id, offset)` through a bounded decoded-record cache, preserving
> row-index order, interval membership, limits, and cancellation; a per-request
> `scan_decode_audit` makes "one read/decode per distinct record" checkable.

## Context

Scan cost on the daemon path is per location, and a location is one row. The
per-location phase profile, taken with a request-local accumulator inside the
scan and reported on every exit path, is stable across query sizes:

| scan | locations | read+decrypt | decode | materialize | push | prune |
|---|---|---|---|---|---|---|
| bounded | 8,193 | 57.9% | 40.8% | 0.3% | 0.2% | 0.1% |
| bounded | 12,289 | 57.3% | 41.2% | 0.3% | 0.3% | 0.1% |
| full | 13,000 | 57.5% | 41.2% | 0.3% | 0.3% | 0.1% |

**98.6% of the work is one physical read plus one full decode, per row.** Bounded
and full scans are equivalent in shape, so this is not a limit-path artifact and
the remedy does not need a bounded-read special case. The absolute microseconds
from that run are not load-bearing here — the instrumented build is slower than
stock, and the totals it reports are drop-time rather than scan-time. The shares
are the evidence; they are ratios of phase timers accumulated inside one scan.

Everything else has been excluded by measurement rather than argument: response
framing (ADR 0088), client-return work, SQL/Arrow encoding, the QUIC hop, client
decode, the table's physical history, PBC and stats pruning, and kernel/IPC
handoff — the last measured at 60 ms of a 14.1 s request.

The decode figure is the tell. Roughly 1.3 ms to decode a **one-row** batch is
fixed per-decode overhead — framing and schema handling — not per-value work.
That independently explains why `SELECT x` and `SELECT x, y` cost the same: cost
never depended on how many values a row held.

### The cause is not a missing feature

Multi-row packing is already built and already selected. `RecordLayoutPolicy`
has a `MultiRow(MultiRowLayoutDescriptor)` variant with `target_rows = 4096`; the
table write path branches on it and calls the packing routines; compaction can
already encode multirow records from survivors; and the read path already
materializes a row out of a packed record by `record_row_index`. Toron's
`create_table_descriptor` and `provision_ctas` both request `MultiRow`. The table
measured above is a `MultiRow` table.

It packs one row per record anyway, because packing buckets on `Locality`:

```rust
let mut buckets: BTreeMap<Locality, Vec<MultiRowSourceRow>> = BTreeMap::new();
```

and under the default `TableSpreadProjection`, `Locality` is unique per row by
construction:

```rust
Locality::new((table_hash & prefix_mask) | (row_hash & suffix_mask))
```

The 16-bit prefix carries the table/spread bucket and takes only `spread_factor`
distinct values — four by default. The 48-bit suffix is the row's own key
identity. Two rows therefore share a `Locality` only if they share a key, so
every bucket holds exactly one row and `target_rows` can never bind. The
`storage_multirow_uncompacted_row_records_total` metric exists to observe
precisely this state.

So the defect is a grouping-key choice, not an absent capability: packing is
keyed on a value that is deliberately unique per row.

## Decision

### 1. Rows group by their durable partition, not the whole locality

Packing buckets on the placement-bearing part of the locality — the prefix — and
not on the per-row suffix. Rows sharing a prefix already share ring placement, so
grouping them into one record does not move data between owners. The suffix
remains what identifies a row inside its record.

`target_rows`, `max_rows`, and `max_record_bytes` keep their existing meaning and
finally bind, because a bucket can now hold more than one row.

### 2. The scan reads and decodes each physical record once

Packing alone changes nothing. Scan cost is charged per **index entry**, not per
record: the loop calls `slab.read(location)` and `decode_batch` for every entry
it walks, so a record holding `R` in-range rows is read and decoded `R` times no
matter how its rows were grouped. Grouping without coalescing is a no-op, and
the phase profile above would not move at all.

The scan therefore coalesces on the physical record identity — the
`(slab_id, offset)` pair carried by `RecordLocation` — reading and decoding a
record once and materializing each of its in-range rows from that single decode.

Row selection and order stay index-driven. The loop still walks locality index
entries in index order and still emits one row per entry, selected by that
entry's `record_row_index`. Coalescing replaces the physical read; it never
changes which rows are emitted or in what order. Cancellation, deadline, budget,
and row-limit checks stay exactly where they are, evaluated per entry.

Entries for one record are **not** contiguous. Rows in a record share a prefix,
but the index orders on the full locality, so a prefix block interleaves entries
from every record carrying that prefix. A cache holding only the previous record
would thrash, so coalescing retains a bounded set of decoded records and evicts.
The bound is on retained decoded bytes and record count: this is a cache, and a
scan must never grow its memory with the size of the scan.

### 3. A packed record is addressed by its prefix, and rows by suffix within it

A record spanning many suffixes cannot be addressed by any one of them. A packed
record's stored locality is its prefix, and locality-range scans enumerate prefix
ranges — the machinery for that already exists as `table_spread_prefix_ranges`.

Point lookup by key identity must resolve key → prefix → record → row-within, and
must return exactly the row asked for. This is the load-bearing correctness
change in this record and it needs its own controls, not an assurance: a lookup
that returns a neighbouring row from the same record is the failure this must
make impossible.

### 4. Read compatibility is a requirement, not a migration step

Single-row and packed records coexist permanently. A reader must never assume a
location holds one row, and must never assume a table's records are uniformly
packed — a table will hold both for as long as its unpacked records survive.
The read path already carries this shape; this record makes it a contract.

No format version changes. A packed record is the existing multirow record; this
alters only which rows are placed in one.

### 5. Migration is by compaction, never by rewrite

Existing tables are not rewritten in place and no operator step is required.
Compaction already encodes multirow records from survivors; it packs on the same
prefix rule, so existing one-row records merge as they are compacted normally.
Until a region is compacted its rows keep their present cost — the improvement
arrives with compaction rather than at deploy.

New writes pack immediately. This means a table's read cost improves in two
stages, and a measurement taken between them is neither the old nor the final
number; anything reporting a rate must say which state it observed.

### 6. Encryption and authentication scope is unchanged

Encryption and authentication are per sealed slab, single-tenant, via the
encrypted slab footer. A record is not the encryption unit, so grouping rows
into one record neither widens nor narrows the cryptographic boundary, and no
key, footer, or tenancy invariant changes.

One consequence does change and must be stated rather than discovered:
`payload_hash` is per record. A packed record's digest covers every row it holds,
so a corrupt record invalidates that group rather than a single row. Integrity
detection is unaffected; integrity **attribution** coarsens to the record. Any
diagnostic that reports a corrupt row must report the containing record's row
range instead of implying a single-row fault.

## Acceptance

- **Per-record amortization, exact.** Scanning `N` rows packed at `R` rows per
  record reads and decodes `ceil(N / R)` **distinct records**, not `N`. The
  control counts distinct `(slab_id, offset)` reads and decodes, because
  locality entries examined stay equal to rows by design — the index is
  row-granular and stays that way — so counting entries would show no change and
  prove nothing. It fails on the pre-change scan, where reads equal rows.
- **No duplicate rows.** Every row is emitted exactly once even when many index
  entries resolve to the same physical record. A control asserts the emitted row
  set and its order match the index order, with no repeats.
- **Bounded coalescing memory.** A scan far larger than the cache bound retains
  no more than the configured decoded bytes and record count. The control drives
  a scan whose working set exceeds the bound and asserts retention stays under
  it, so the cache cannot silently become a whole-scan buffer.
- **Cancellation and limits are unchanged by coalescing.** A row limit stops the
  scan at the same row it stopped at before, and cancellation and deadline exits
  return their existing typed outcomes. A cache hit must not skip the per-entry
  checks; the control asserts a limit satisfied mid-record stops there rather
  than emitting the rest of the decoded record.
- Row count, values, order within a locality, and schema are identical before and
  after packing for the same written rows. Packing changes storage grouping, never
  query results.
- A point lookup by key identity against a packed record returns exactly that row.
  A control covers a record holding many rows and asserts the neighbours are not
  returned, since returning a neighbour is the specific failure this design admits.
- A table holding both packed and unpacked records reads correctly and returns
  each row once. This is the steady state during migration, not a transient.
- The phase shares move as predicted: with `R` rows per record, per-row
  read+decrypt and decode fall by approximately `R`. Reported as a measurement
  with its binary identified, not as a CI gate.
- Compaction of existing one-row records produces packed records without a format
  change, and the pre-compaction and post-compaction reads return identical rows.
- A corrupt packed record is reported against its row range rather than as a
  single-row fault.

## Guardrails

The locality index stays row-granular. Exact interval semantics depend on it:
each row is discoverable through its own `(routing_locality, key_identity)`
entry, so a scan starting inside, ending inside, or crossing a record's locality
span yields exactly the rows inside the interval. Coarsening the index to the
record would break that and is not an optimization this record permits.

Packing without coalescing is a no-op, and coalescing without packing has
nothing to coalesce. Neither half may land alone claiming a rate improvement.

Packing groups on the locality prefix only. Grouping on any value that is unique
per row silently degenerates to one row per record — the present defect — so a
future grouping key needs a control proving a bucket can hold more than one row.

A location is not a row. Tests must not assert location counts as a proxy for row
counts, in either direction.

Bounded and full scans share one phase profile; a remedy that helps only one of
them has not addressed this record's evidence.

## Risks and deferrals

Larger records mean a bounded read may materialize more rows than it returns: a
`LIMIT 1` will decode a whole record. That is the same amortization working in
reverse, and it is bounded by `max_record_bytes`. ADR 0579 pushes a top-level
`LIMIT` into the scan so the producer stops at the limit, which bounds how many
records are read but not how many rows a single record holds.

Read amplification for point lookups rises for the same reason. `target_rows`
and `max_record_bytes` are the tuning surface, and a workload dominated by single
point reads may want a smaller target; this record does not retune per workload.

Not addressed: a new end-to-end attribution at the post-packing operating
point. Physical read and decryption are still inseparable at the scan boundary,
but the old 57% read+decrypt share was measured before one physical record was
shared across thousands of index entries; it is not evidence that a read/decrypt
split owns the new budget. Also unaddressed: whether `spread_factor` of four is
the right placement spread, which is a distribution question, not a packing one.

## Forward

Re-attribute the complete request at the post-packing operating point, on a
table whose records are known packed, and state which migration state was
observed. The pre-0089 exclusion controls were priced against a ~2.08 ms/row
request, so their percentage shares do not carry over to a ~13 us/row request.
Instrument the per-index-entry tail beyond the whole-record decode cache
(`resolve_record_placement`, descriptor matching, materialization, PBC,
accumulation, and index-entry production), then re-run the client/transport
controls. Optimize only the component the new attribution identifies.

## Measured

2026-08-23, optiplex, live daemon, binary `c2a23d7f72bc9adb` at `9e773b28`,
warmup + cold restart + two measured passes, all stable.

Both migration states were measured separately: `ctrl_plain` (rows written
before ADR 0090, raw-locality records, no placement stamps) and `ctrl_packed`
(fresh `CREATE TABLE AS` through the partitioned write path). The per-request
scan-decode audit pinned the premise first: 13,000 rows resolve to
`distinct_records = 4` — the per-partition `ceil(rows / 4096)` — with
`decodes + hits == rows` at every rung and `max_resident <= 4`.

| rows | pre-0089 baseline | + 0579 only | `ctrl_plain` | `ctrl_packed` |
|---|---|---|---|---|
| 2,000 | 8.84 / 8.88 s | 4.27 / 4.30 s | 0.06–0.07 s | 0.04–0.05 s |
| 5,000 | 17.66 / 17.88 s | 10.84 / 10.72 s | 0.08–0.09 s | 0.08 s |
| 13,000 | 26.93 / 27.05 s | ~27.1 s | 0.18 s | 0.16–0.17 s |

The headline result: **13,000 rows in ~0.18 seconds** (~76,000 rows/s against
the ~481 rows/s baseline). The 2k/5k rungs combine ADR 0579's limit push-down
with this cut (1 and 2 records decoded respectively — producer work bounded to
exactly the limit); the 13k rung is the pure full-scan comparison.

The audit also settled the migration-state question in an unexpected direction:
`ctrl_plain` shows the identical audit shape. Its rows already lived in four
shared physical records — append-only batch locality packed them at write time
— so the historic per-row cost was the scan re-reading and re-decoding the same
record once per row, exactly the 57% read+decrypt / 41% decode phase profile.
On this fixture the entire win is the read-side coalescing; compaction-driven
migration contributed nothing, and the two states legitimately converge. The
write-side packing delta remains unmeasured and needs a fixture whose writes
genuinely produce one-row records.

The ~29,000-row statement-timeout ceiling no longer binds at this rate. An
isolated single-node heavy-churn exhibit on the same implemented engine,
recorded in `crates/toron/plans/wip/claude_iter_14_heavy_churn_verified.md`,
then read seven terminal-route cardinalities from 49,999 through 1,000,001
rows. It confirmed the expected scaling: read throughput stayed flat at
~72,000–76,000 rows/s (13.31–13.67 s for 1,000,000 rows). The approximately
4.5M-row statement-timeout figure is therefore demonstrated as an extrapolated
budget at the measured rate, rather than awaiting a cardinality check. That
exhibit also confirmed its requested and reopened row counts and reported no
maintenance, slab-GC, or cleanup failures; it is a one-pass, single-node
happy-path result, not a substitute for fault-injection or replica acceptance
arms.
