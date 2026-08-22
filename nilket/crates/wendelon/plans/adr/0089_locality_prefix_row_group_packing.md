# ADR 0089 - Row-group packing on the locality prefix

**Status:** Proposed 2026-08-22. Responds to the per-location phase profile taken
against the live deployment. It changes which rows share a stored record; it does
not change the record format, the response protocol, or the encryption boundary.

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

### 1. Rows group by the locality prefix, not the whole locality

Packing buckets on the placement-bearing part of the locality — the prefix — and
not on the per-row suffix. Rows sharing a prefix already share ring placement, so
grouping them into one record does not move data between owners. The suffix
remains what identifies a row inside its record.

`target_rows`, `max_rows`, and `max_record_bytes` keep their existing meaning and
finally bind, because a bucket can now hold more than one row.

### 2. A packed record is addressed by its prefix, and rows by suffix within it

A record spanning many suffixes cannot be addressed by any one of them. A packed
record's stored locality is its prefix, and locality-range scans enumerate prefix
ranges — the machinery for that already exists as `table_spread_prefix_ranges`.

Point lookup by key identity must resolve key → prefix → record → row-within, and
must return exactly the row asked for. This is the load-bearing correctness
change in this record and it needs its own controls, not an assurance: a lookup
that returns a neighbouring row from the same record is the failure this must
make impossible.

### 3. Read compatibility is a requirement, not a migration step

Single-row and packed records coexist permanently. A reader must never assume a
location holds one row, and must never assume a table's records are uniformly
packed — a table will hold both for as long as its unpacked records survive.
The read path already carries this shape; this record makes it a contract.

No format version changes. A packed record is the existing multirow record; this
alters only which rows are placed in one.

### 4. Migration is by compaction, never by rewrite

Existing tables are not rewritten in place and no operator step is required.
Compaction already encodes multirow records from survivors; it packs on the same
prefix rule, so existing one-row records merge as they are compacted normally.
Until a region is compacted its rows keep their present cost — the improvement
arrives with compaction rather than at deploy.

New writes pack immediately. This means a table's read cost improves in two
stages, and a measurement taken between them is neither the old nor the final
number; anything reporting a rate must say which state it observed.

### 5. Encryption and authentication scope is unchanged

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

- **Per-row amortization, exact.** Writing `N` rows to a spread-locality table and
  scanning them examines `ceil(N / target_rows)` locations, not `N`. The control
  asserts the exact expected count against `clocks.progress_marks`-style producer
  work, over a source larger than one `target_rows` group, and fails on the
  pre-change packer where locations equals rows. "Fewer locations" is not the
  claim and would pass while packing two rows per record.
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

Not addressed: the read+decrypt phase's internal split — physical read and
decryption are inseparable at the scan boundary, and at 57% a split is worth
having once packing has amortized it. Also unaddressed: whether `spread_factor`
of four is the right placement spread, which is a distribution question, not a
packing one.

## Forward

Re-measure the daemon path after packing, on a table whose records are known
packed, and state which migration state was observed. If per-row cost falls by
roughly the rows-per-record factor, the scan-cost arc that began with the
scatter-threshold slice is closed and the ~29,000-row statement-timeout ceiling
moves with it. Then split read from decrypt, which will be the largest remaining
per-record cost.
