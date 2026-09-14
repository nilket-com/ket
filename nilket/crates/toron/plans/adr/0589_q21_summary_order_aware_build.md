# ADR 589 - q21 existence-summary build: ordering census, then an order-aware build

**Status:** PROPOSED. Successor to ADR 588. Two phases under one record.
Phase 1 measures the order of the batches the existence summary actually
consumes, per partition stream, and attributes every recurrence; it opens
or closes phase 2 on that evidence alone. Phase 2, if opened, is a build
that appends under a per-partition monotone precondition checked on every
row, with an exact fallback that preserves all prior summary state, and
that builds the hash index once at the end instead of probing and
regrowing it during the build. Phase 2 is retained only on digest parity
and the ADR 588 release-pair rule; otherwise it is reverted and closed.


## Context

ADR 587 resolved the row loop outside growth as the owner of q21's semi
insert (1,122.8 median-scaled ms, 8.75%), with growth at 450.5 ms below the
floor, and its counters said what the loop does: on the semi alias,
12,004,370 lookups met an existing key 4,370 times and appended 12,000,000
times, with 93% of the row loop's sampled CPU in hashing and probing. ADR
588 removed the second walk on a miss: all five pairs were faster, median
182.2 ms, against a 353.0 ms bar; not established, reverted. What remains
is the first walk, twelve million random bucket reads into a 134 MB array
plus fifteen rehashes, to confirm the absence of keys that a sorted source
already implies are absent.

The 4,370 found lookups prove that keys recur after the consecutive-key
cache has lost them. They do not yet say why. Two mechanisms could produce
them and they have different consequences. The lineitem Parquet source is
ordered by order key, and each key's rows are contiguous; if the summary
consumed the source in that order, a key could recur only where the source
itself is out of order. But the summary does not consume the source in
that order. The existence plan's Exchange op routes every row of each
source batch into one of four partition batches by a hash of the key, and
the writer inserts the four partition batches of one source batch in
sequence, then the four of the next. Within one partition, rows keep the
source's order, so each partition's stream is non-decreasing across
batches; but the consumed stream as a whole restarts four times per source
batch, and a key whose rows straddle a source-batch boundary lands in the
same partition of two consecutive source batches with the other three
partitions' batches consumed in between. The cache has moved on; the key is
looked up and found. 5,859 source batches give 5,858 boundaries, and a key
straddles one with probability near three quarters at four rows per key:
about 4,400 recurrences, which is the count observed. That is a hypothesis
with the right arithmetic, not a measurement; whether the consumed stream
is non-decreasing within every partition, whether every found lookup is a
boundary straddle, and whether any key ever arrives below its partition's
running maximum are counts the summary can take exactly, and phase 1 takes
them. Nothing in phase 2 may rest on the source's order; it rests on the
order of what the summary consumes, measured.

The reason order matters is what a probe is for. In a linear-probed table
the walk on a miss does two things at once: it confirms the key is absent
and it finds the empty slot. If the key's absence were known without the
walk, the slot could be found later, for all keys at once, by sorting
twelve million (bucket, entry) pairs and filling the array sequentially,
which is streaming work rather than random access; and the array could be
sized once from the final count instead of growing fifteen times. A
per-partition monotone stream gives exactly that knowledge: a key strictly
greater than every key that partition has delivered so far cannot be in
the table, because the routing is a fixed function of the key and would
have delivered it through the same partition. That is the whole
precondition, and it is checked with one comparison per row against a
running maximum, not assumed.

Two facts bound the design. The writer today discards the partition index
the Exchange produced; phase 2 needs it and the writer will pass it. And
ADR 586's repartition candidate (613 ms gross, net unpriced) interacts with
this record: removing the Exchange would make the consumed stream the
source stream, globally non-decreasing with no boundary straddles at all;
phase 1's census is also the measurement of what the Exchange does to
order, and the record reports it so that a later decision on the Exchange
is made on counts. This record does not remove the Exchange.


## Decision

### Phase 1: the ordering census

**Instrument, additive, on the ADR 587 profiled path.** The writer passes
each partition batch's partition index into `write_batch_with`. The
instrumented loop keeps, per partition, `max[p]`, the running maximum of
every key delivered on that partition so far, represented as `Option<i64>`
so that an unseen partition is `None` and a first key of `i64::MIN` is
above it rather than equal to a sentinel; and the key that ended the
previous batch on that partition. It counts on every non-cache-hit row:

```
above_max[p]        key > max[p]: a new key under the monotone precondition
equal_max[p]        key == max[p]: a recurrence the cache lost, by construction
                    the first row of a batch on p equal to the last of the
                    previous batch on p (a boundary straddle) or not
below_max[p]        key < max[p]: an ordering violation on stream p
boundary_straddles  equal_max rows that were the first row of their batch
found_at_straddle   lookup_found rows that were boundary straddles
found_elsewhere     lookup_found rows that were not
```

with `max[p]` raised to `key` after an `above_max` row and left alone
otherwise. Per source batch the receipt also
records, for each partition batch, its first key, its last key, and
whether its first key was below the previous batch's last key on the same
partition, as a compact per-batch string like ADR 587's windows. The
counters publish through ADR 587's windows diagnostic; the census runs
under ADR 587's driver (`adr587-q21-insert`, the same phase 0, pairs, gate,
and sampler) on a `profiling` build, so the row-loop terms and the sampled
classes are re-reported beside the census at no extra cost, and ADR 587's
line classes are re-pinned for the tree. No engine behaviour changes in
phase 1.

**What the census decides, stated before it runs.** Phase 2 opens only if,
on both summaries and in every pair, `below_max` is zero on every
partition and `found_elsewhere` is zero: every recurrence is a boundary
straddle and every partition stream is non-decreasing. If `below_max` is
non-zero anywhere, the precondition does not hold on the consumed stream
and phase 2 closes on this record without being built; the record then
reports what order the stream does have and hands the row loop forward
with that. If `found_elsewhere` is non-zero with `below_max` zero, the
recurrence mechanism is not the straddle and the record names what it is
before phase 2 may open. Partial results do not open phase 2.

### Phase 2: the order-aware build

**Admission: equal keys must stay in one partition, proved from the plan,
not assumed.** The fast path's inference, "a key above `max[p]` is absent",
is sound only if the routing is a pure function of the summary's existence
key, so that every row of one key reaches one partition. The existing
decomposition accepts any Exchange key program above any Project chain,
and nothing today ties the Exchange's key to the summary's. The order-aware
build is therefore admitted per summary at preparation time, only when:
the decoded op chain (stored scan-first, as `decompose_streaming_existence_plan`
returns it) contains exactly one Exchange; its key program is a single
root that is a plain column reference; and the two ordinals resolve to
one scan column when each is traced toward the scan through exactly the
Projects that lie between it and the scan, in reverse execution order.
The Exchange's key ordinal is a column of the Exchange's input, so it is
traced through the Projects that precede the Exchange in the op list and
no others; the existence key is a column of the chain's output, so it is
traced through every Project in the chain. Each step maps an ordinal
through that Project's `root_columns`, as the key-count hint already
walks ordinals, and a Project whose root at that ordinal is not a plain
column ends the trace as not admitted. Admission holds only if both
traces reach the scan and name the same scan column. Any other shape, a
computed key, a multi-column key, a key that traces to a different scan
column, no Exchange, or more than one, uses the existing hashed build
unchanged. Admission is recorded in the receipt beside the ops list.

Tests of admission trace ordinals, not names, with the mappings spelled
out. Let the scan produce `(orderkey, suppkey)` at ordinals `(0, 1)`. A
Project before the Exchange alone cannot make the Exchange-input ordinal
and the summary-output ordinal differ, because the Exchange's output is
the summary's input; a Project after the Exchange is what changes the
output ordinal. So: (a) Project `(suppkey, orderkey)` after the Exchange,
Exchange keyed on input ordinal 0; the existence key is output ordinal 1,
which traces through the Project to scan column 0, and the Exchange key
traces through nothing to scan column 0: admitted although the ordinals
differ (1 against 0). (b) Project `(suppkey, orderkey)` before the
Exchange and Project `(suppkey, orderkey)` after it: the first swap puts
`orderkey` at ordinal 1, so the Exchange is keyed on input ordinal 1; the
second swap puts it back at ordinal 0, so the existence key is output
ordinal 0. The existence key traces 0 → 1 (through the second Project)
→ 0 (through the first) to scan column 0; the Exchange key traces 1 → 0
through the first Project only, to scan column 0: admitted although the
ordinals differ (0 against 1). (c) The decoy: Project `(suppkey, orderkey)` after the
Exchange, Exchange keyed on input ordinal 0 (`orderkey`), existence key
output ordinal 0, which is `suppkey` after the swap: the ordinals
coincide and the scan columns differ; not admitted, and the hashed build
runs with the fast path never taken. (d) A Project that computes the key
column: not admitted. (e) No Exchange: not admitted. Without tracing, a
comparison of raw ordinals would wrongly refuse (a) and (b), whose
ordinals differ, and wrongly admit (c), whose ordinals coincide; that is
why these are the tests.

**The cut.** The summary build gains an ordered fast path and keeps the
hashed path as its exact fallback. Per partition stream `p` the summary
holds `max[p]: Option<i64>`, the running maximum of every key delivered on
`p`, `None` until the first row. On a row that is not a cache hit:

- `max[p]` is `None` or `key > max[p]`: the fast path. The entry is
  appended to `entries` with no probe, no bucket write, and no growth of
  the bucket array; `max[p]` becomes `Some(key)`; the entry's index is
  pushed onto `ordered[p]`, the partition's list of fast-path entries,
  which is therefore strictly increasing by key.
- otherwise: the fallback. The key may exist. It is looked up exactly
  without the main index: a binary search of `ordered[p]` by key finds it
  if it was appended on the fast path; if not there, an overflow map
  (`HashMap<i64, u32>` from key to entry index, expected tiny) is consulted;
  found either way, the value is observed exactly as today and the last-key
  cache is set; not found, the entry is appended and recorded in the
  overflow map, not in `ordered[p]`, so `ordered[p]` stays sorted. `max[p]`
  is not changed by a fallback row.

Every prior summary state, entries, representative values, the multi-key
bitset, `input_rows`, `multi_keys`, is preserved by both paths, because
both paths are the same append and the same `observe_value`; the fallback
differs from today's hashed path only in how it finds an existing entry.
If the stream were badly ordered the overflow map would grow and the build
would degrade toward a hash-map build, still exact.

**The index, built once, with collisions and wraparound stated.** When the
build ends, before the drive, the bucket array is allocated once, empty, at
twice the next power of two above the final entry count, the same load
bound the growing build ends at. An array of `(home_bucket, entry_index)`
pairs is built over all entries and sorted by home bucket, entry index
ascending within a bucket. The fill walks the sorted pairs with one
unwrapped cursor `next_free: u64` over the range `0..2N` where `N` is the
array length, so that it is monotone even after passing the array's end:
slot indices are `cursor mod N`, and a cursor at or above `N` means the
fill has wrapped. For each pair in order, `cursor = max(home_bucket,
cursor)` is taken only while the cursor is below `N`; once the cursor has
wrapped it is never reset to a home bucket again, because every remaining
pair's home bucket is at or below the position the cursor already passed
in unwrapped terms, and its correct slot is the first empty slot at or
after the cursor. The entry is written at the first empty slot at or
after `cursor mod N`, advancing the cursor past occupied slots, and the
cursor becomes that position plus one. Two invariants follow and are
checked: the cursor is monotone over the whole fill, so the fill's work is
one pass over the array plus one over the pairs, linear; and every entry
lands at the first empty slot at or after its home bucket in the probe's
wraparound order with no empty slot between, which is what `find_bucket_in`
relies on, that a lookup meets no empty slot before its key. Entries whose
home buckets sit near the array's end spill past it into low slots that
low-home entries already hold; the wrapped cursor moves past those and
places them after, where a wrapping probe finds them. A wrapped cluster
that reached back to its own origin would need more than `N` occupied
slots in one run, which a load factor of at most one half rules out and a
debug assertion checks by refusing a cursor at or beyond `2N`. The probe
side (`matches`, `lookup_entry_index`) is unchanged and sees a table with
the same invariant it sees today. The growth policy and `resize_capacity`
are not used by the build any more; `entries` still grows as a `Vec`, by
doubling, guarded as below.

Tests of the index: deliberate collisions on one home bucket near the
array's end that must wrap into occupied low buckets, then every one of
those keys found and every absent key's probe terminating at an empty
slot; a cluster spanning the wrap with an absent key whose home bucket
lies inside it; several groups of collisions on consecutive high home
buckets whose combined run continues through the one wrap into low slots
already occupied by low-home entries, at a load under one half so the
cursor stays below `2N`, asserting both that every key is found and that
the fill's slot visits, counted by a test hook, are at most the array
length plus the pair count, so the linear claim is checked rather than
stated; an empty input building an empty
table that finds nothing; a randomised comparison against the growing build in which every
key present in one is found in the other and every absent key is absent
in both, with the probe-step count of the once-built table reported
beside the grown table's.

**Memory guards, before each allocation, at actual capacities, overlap
included.** The order-aware build allocates more things than the hashed
build and allocates some of them while others are still live, and the
existing `guard_allocation` pattern, request against `baseline +
accounted + allocation` before the allocation and re-check at the
capacity actually obtained, is applied to each of them: every doubling of
`entries` (old and new arrays overlap during the copy, so the request is
the new capacity while the old is still accounted); every doubling of an
`ordered[p]` list and of the overflow map's table (the map's growth is
guarded by reserving explicitly before an insert that would grow it, at
the capacity the map reports afterwards); the `(home_bucket, entry_index)`
array, twelve million pairs at `size_of::<Pair>()` for the representation
chosen, padding included (a `(usize, u32)` pair is 16 bytes on this
target; a packed `(u32, u32)` would be 8 and bounds the bucket index to
`u32`, which the array length already is), live together with `entries`,
`multi`, and `ordered`; the sort's scratch, bounded by the sort chosen (an
in-place unstable sort, so none beyond the pairs array) and stated; the
bucket array, allocated while the pairs array is still live; and the drop
of the pairs array and the `ordered` lists after the fill, which is the
build's peak point and the last thing the receipt records. Peak combined
bytes are observed at every step as today. A test drives the fallback
path's growth into a cap that refuses it and asserts the budget error
with the summary intact; another drives the final index construction into
a cap that admits the entries but not the pairs array, and another one
that admits the pairs but not the bucket array, and asserts the budget
error at the right allocation each time, before it is made.

**Value gate.** Digest parity, as ADR 588 wrote it: every row of every
arm byte-equal to the pinned sealed q21 digest.

**Tests at the summary, each failing if the cut were wrong.** Recurrence
across batches: a key whose rows end one partition batch and begin the
next on the same partition is found by the fallback and observed, not
appended, with `entries` unchanged. Recurrence across partitions: two keys
routed to different partitions each advance their own `max` and neither
sees the other's; a key repeated on its own partition after the other
partition advanced is still found. Descending keys: a key below `max[p]`
takes the fallback, is appended to the overflow map and not to
`ordered[p]`, `ordered[p]` stays sorted, a later lookup of that key finds
it, and `max[p]` is unchanged. Repeated keys with differing supplier
values: the multi-key bit is set exactly as the hashed build sets it, on
the cache path, on the straddle path, and on the overflow path, and
`multi_keys` agrees. Equivalence: for randomised inputs of mixed order the
order-aware build and the hashed build produce entry sets, representative
values, multi-key bits, and probe results that are equal, and the
existing residual-twin probe tests pass untouched. Index: the once-built
index finds every entry `find_bucket` would, and the probe side's results
are unchanged.

**The work moved after ingestion gets its own ledger terms.** There is no
continuous insert interval to enclose the finish in: ADR 586's
`semi_write_insert` is an accumulation of one interval per source batch
around that batch's insertion passes, ADR 585's `runtime_prep_semi_write`
is likewise accumulated per source batch around the transform-and-write,
and ADR 587's `insert_rows` windows close inside each batch. Work done
after the last batch falls outside all three, so a build that defers its
hashing would report shorter write and insert terms by moving the hashing
out from under their clocks. Those terms keep their meaning and their
per-batch accumulation unchanged. The instrumented build adds, on the
profiled path only, inclusive lifecycle terms for every phase that runs
after the last batch: `index_pairs` (building the pairs array),
`index_sort`, `index_alloc` (the bucket array's allocation and zeroing),
`index_fill`, and `build_cleanup` (dropping the pairs array and `ordered`
lists), each a window with the thread it ran on, summed into
`insert_finish`, and a named total `insert_total = <summary>_write_insert +
insert_finish`. The finish is accounted for in the enclosing lifecycle
reconciliation: it runs inside the summary's preparation, which ADR 585
brackets as `runtime_prep_semi_write` per batch plus a residual under
`runtime_prep`, so `insert_finish` is published as its own runtime-prep
child and the runtime-prep band becomes `inner_1 + inner_2 + semi_read +
semi_write + semi_finish + anti_read + anti_write + anti_finish +
runtime_prep_residual = runtime_prep_inclusive`, exactly, with the
existing 5% completeness bound applied to the residual as before. A test
drives a multi-batch build and asserts that band, that `insert_total`
equals the sum, and that the five finish windows are disjoint, ordered,
and sum to `insert_finish`; it does not assert containment in any insert
interval, since none exists. In the mechanism check a row-loop or write
term that fell by more than `insert_finish` rose is a movement worth
naming; one that fell by less merely moved. Neither is the gain: retention
rests on the release pairs alone. The unprofiled build runs the same
finish with no windows.

**Measurement, as ADR 588 wrote it.** Two `release` executables on one
recipe, control from the tree with phase 1's instrument and the driver,
treatment from that tree plus the cut alone; the ADR 588 driver with its
identity gate, its one validator on every child, and the pinned digest;
phase 0 on the control against ADR 588's control median 12,967.4 ms; five
alternating unprofiled pairs at the sealed coordinate. **Retained only if
the median gain exceeds `2 × range_control` and every pair is strictly
positive; a zero pair fails.** Otherwise reverted in the closing commit
and closed. Then ADR 587's driver on a `profiling` build of the treatment
as the mechanism check, reported and not gating: the census counters
should show every fast-path row as `above_max`, every straddle as a
fallback found, `growth_reprobes` zero, `insert_finish` and its five
windows reconciled in the runtime-prep band with `insert_total` stated,
and the row-loop term and its sampled classes beside ADR 587's, never
netted into the gain.

**What this record does not do.** It does not remove or change the
Exchange; it reports what the Exchange does to order. It does not change
the probe side, the recognizer, or any route. It does not open phase 2 on
the source's order, on the arithmetic in the Context, or on a partial
census. It does not retain a gain the rule does not establish.


## Acceptance

- Phase 1 lands first: the writer passes the partition index; the census
  counters and per-batch order strings ride the ADR 587 receipt with
  paired tests (a straddle counted as `equal_max` and `found_at_straddle`,
  a descending key as `below_max`, a key on another partition leaving this
  partition's `max` alone, per-batch first/last keys and the restart flag
  exact); ADR 587's line classes re-pinned with the anchor test passing;
  ADR 587's driver reports the census per summary and pair beside the
  existing terms.
- The census result stated per summary and pair: `below_max` per
  partition, `found_at_straddle` against `lookup_found`, `found_elsewhere`;
  the opening rule applied as written, phase 2 opened or closed in one
  sentence; the Exchange's effect on order stated in counts.
- If phase 2 opens: the admission gate lands with its tests (projection
  reordering on each side of the Exchange traced correctly, the
  coinciding-ordinal decoys refused, a computed key and no Exchange
  refused);
  the cut lands with the six summary tests, the index tests (end-of-array
  collisions wrapping into occupied low buckets, a cluster spanning the
  wrap with an absent key inside it, absent-key probes, empty input,
  randomised agreement with the grown table), the three budget tests
  (fallback growth refused; pairs array refused; bucket array refused,
  each before the allocation), the finish-window tests (five disjoint
  ordered windows summing to `insert_finish`, the runtime-prep band
  reconciling exactly with `semi_finish` and `anti_finish`, `insert_total`
  equal to its sum), the ADR 587
  counter tests re-derived (no growth, no reprobes, `above_max` on the
  fast path), and the summary equivalence tests unchanged; two `release`
  executables staged on one recipe with the identity gate satisfied; the
  ADR 588 driver run with every row validated; per-pair control,
  treatment, gain, and digest; the rule applied unchanged with the verdict
  in one sentence; the mechanism check reported beside ADR 587's terms.
- The verdict's consequence in the same commit: retained, or reverted and
  closed; the arc adds no stamp-only commit.
- Results cite every tree id, executable hash, recipe, the dataset
  manifest, and the artifact directories, for phase 1 and, if run, phase 2.


## Guardrails

- Ordering is measured on the batches the summary consumes, per
  partition, never inferred from the source's sort order or the Context's
  arithmetic; phase 2 does not open on a non-zero `below_max` anywhere.
- The precondition is checked on every row; a violation takes the exact
  fallback; no row is ever appended on the fast path without `key >
  max[p]` having held for its partition.
- The fallback preserves every prior state and finds an existing key
  exactly; `ordered[p]` is never made unsorted; the overflow map is the
  only home of a below-max new key.
- The fast path is admitted only when the Exchange key is provably the
  existence key column; every other shape takes the hashed build.
- The once-built index preserves the probe invariant (no empty slot
  between a key's home bucket and its slot, in wraparound order); the
  probe side is unchanged and sees the same table.
- Every allocation is guarded before it is made at the capacity actually
  obtained, overlapping allocations counted together; the peak is
  observed at the fill.
- Work moved after the batch loop is clocked in its own terms and
  reconciled in the runtime-prep band; the per-batch write and insert
  terms keep their meaning; a shorter row loop or write term is read only
  beside `insert_finish` and `insert_total`, and never as the gain.
- Digest parity on every row is a precondition of any verdict; the
  release-pair rule is fixed before the run and not revisited; the
  mechanism check never enters the gain.
- The Exchange is not removed under this record.
- Every earlier executable is measured by nothing here.


## Risks

- The census may find non-zero `below_max`: an out-of-order source region,
  a partition routing that is not a pure function of the key, or a batch
  the reader delivers out of order. Phase 2 then closes before it is
  built, which is the intended outcome of asking first.
- The sort-based index build over twelve million pairs may not be cheaper
  than the probes it replaces; the release pairs decide, and a gain under
  twice the control range reverts the cut.
- `entries` still doubles as a `Vec`, so the copy half of growth remains
  (7.5% of ADR 587's growth term); only the rehash half is removed, and
  the pairs array and sort add work of their own that `insert_finish`
  prices.
- The build's peak memory is higher than the hashed build's at the fill,
  when entries, the pairs array, and the bucket array are all live; the
  guards refuse before an allocation, and the receipt reports the peak,
  which the operator weighs against the 2 GiB coordinate.
- The overflow map could grow large on a badly ordered stream, degrading
  the build toward a hash-map build; exact, but not the gain measured
  here, and the census would have said so first.
- The per-partition maximum and sorted lists add memory proportional to
  the entry count; the same cap guards them.


## Forward

If phase 1 finds the consumed stream monotone per partition and every
recurrence a boundary straddle, the Exchange's only effect on the build is
to create those straddles, and ADR 586's repartition candidate becomes a
question of whether a single-partition order-aware build is cheaper still;
that is a later record. If phase 2 retains, the row loop's remaining cost
is the sort-based index build and the anti's date filter, and the next
owner is chosen from ADR 587's terms again. If phase 2 is not established,
the row loop closes as measured and the arc's next owner is
`runtime_prep_inner_1` at 1,654 ms or `cleanup` at 567 ms.
