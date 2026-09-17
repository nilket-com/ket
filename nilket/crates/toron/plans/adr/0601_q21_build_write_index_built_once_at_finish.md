# ADR 601 - q21's replay-1 build write: the compact index built once, at finish

**Status:** PROPOSED. Successor to ADR 600, which retained the compact
resident index on the spilled branch (median paired gain 559.7 ms,
6.23% of q21) and measured its index child at 303.9 median-scaled ms on
the treatment's mechanism check, with `inner_1_build_write` at 688.7.
Those figures are what the child spans on that run; they are context
for this record, not a savings estimate, and ADR 600's replay-1 lookup
counts remain unknown.


## Context

**What the index child does now, at `edc19bf1`.** With the compact
wrapper enabled, `write_page_batch_inner` (`exec/join/spill.rs`)
constructs `SpilledBatchIndex::for_key_plan` on the first page —
`PrimitiveSpilledBatchIndex::with_capacity(0)`: one bucket, an empty
entry vector — and then, per page, drains the chunk's keyed rows into
`PrimitiveSpilledBatchIndex::insert`: `find_bucket` (an `ahash`
`RandomState` hash of the key, then a linear probe over `u32` buckets until
an empty slot or the equal key), and on an empty slot the resize check
`(entries + 1) × 2 > buckets` — which doubles the bucket array and
rehashes every entry into it (`resize_buckets`: the new array is
allocated while the old one lives; the accounting moves after) — then
a 24-byte entry `{key, first ref}` pushed onto the entry vector (the
vector doubles as `Vec` does; the capacity delta is accounted after
the push); on an equal key, the ref goes to that entry's overflow list
(`SpilledBatchRefList::push`: a `Box<Vec>` of one on the first
duplicate, then `Vec` growth). After the page's inserts the guard runs
once (`guard_resident_state_after_write`) against the retained cap,
16 × the task memory (`JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER`,
`exec/join.rs:121`; 32 GiB at the sealed 2 GiB), on the accounted
figure — capacity, not the transient.

**What that is on q21, from the mechanism check.** 5,845,938 rows kept
and indexed, and every key distinct: the resident peak of 268,506,120
bytes is 2²⁴ buckets × 4 + 2²³ entries × 24 (268,435,456) plus the
5,128-byte writer peak plus the 65,536-byte page reserve, leaving
nothing for an overflow list, so no key repeated. Starting from one
bucket, that is 24 bucket doublings (1 → 2²⁴), each rehashing every
entry present — 0 + 1 + 2 + … + 2²² ≈ 8.4 million hash-and-probe
re-insertions over the write — and about 22 entry-vector doublings
(capacity 4 → 2²³), moving ≈ 8.4 million entries, ≈ 200 MB, in the
worst case where no reallocation extends in place. The transients are
outside the guard: at the last bucket doubling the old 33.6 MB and the
new 67.1 MB arrays coexist; at the last entry reallocation the old
100.7 MB and the new 201.3 MB vectors may. The accounted peak carries
2²³ − 5,845,938 = 2,542,670 entries of unused capacity (61.0 MB) and
2²⁴ buckets, the smallest power of two at or above twice the entries.
What remains of the child after those: one hash and one probe per row
(5.85 million), the entry push, and the per-row accounting.

**Finish today.** `SpilledInnerJoinBuild::finish` (`spill.rs:3035`) is
`set_len`, `fdatasync`, and the resident guard; no index work. ADR
600's `build_write_finish` child, 53.9 median-scaled ms and
12.9–72.4 ms raw across the pairs, spans those three together and
isolates none of them. A build that moves the index into finish
therefore needs its own term, or the file work and the index would
read as one.

**Where the clocks are.** The branch clocks `build_write_create`
around `create_with_resident_baseline` and closes it before ADR 600's
activation call (`streaming.rs:24113–24120`); the write's children are
inside `write_streaming_scan_inner_build_batches`; `build_write_finish`
opens around `spilled.finish` and closes after it; the band's residual
is `inner_1_build_write` less the children's sum, and the band refuses
a child with time and no call and a once-per-build child with calls
other than one. An allocation made at the activation call today would
land in the residual, and an index built inside `spilled.finish` would
land inside the finish clock: both places are wrong for a term of its
own.

**The probe side.** Replay 1's probe reads this index:
`visit_streaming_spilled_inner_probe_batch` (`exec/join/streaming.rs`)
and its siblings take one `spilled.lookup(&key)` per probe row —
`find_bucket` again (hash and probe), then `SpilledBatchRefList::refs`
(`spill.rs:959`), which allocates a `Vec` for the hit's refs even when
there is one — before the pages are read and the rows materialized. On
the mechanism check's on arm the `inner_replay_1` stage spans
2,313 ms over 23,436 calls, the largest replay stage of the
invocation (runtime prep spans 3,910); how
many lookups it makes, and how many hit, is what ADR 600 asked for and
did not get; the closing tree publishes them, and this record's
mechanism check will read them. The index's layout is what the probe
walks, so the paired runs price the whole query — build and probe —
and nothing in this record prices the build alone.

**Duplicates and nulls.** Under the primitive plans a null key is
never a key: `join_key_at` (`exec/join.rs:2243–2258`) returns `None`
for a null `Int32`/`Int64` and the row is not in the scratch refs the
index drains; a null-safe plan is `Generic` and takes the `Hash`
variant, outside the primitive index. A once-built index sees exactly
the keyed rows the incremental one sees. Duplicates, absent on q21's
build, are the general case the index must keep: the same key's refs
in one entry's list, in insertion order, the lookup returning all of
them.

**What the write already knows.** The sizing pass that precedes the
write reports `inner_1_sizing_rows` (`compact_facts.rows` at the
spilled branch; 5,845,938 on q21, equal to the rows the write kept),
so the entry vector's capacity can be obtained once, before the first
page, from a row count the branch has in hand — and accounted before
the allocation, not after.

**A second construction site, out of scope.** The deferred-replay path
(`rebuild_compact_index_and_pin_pages`, `spill.rs:2695`) rebuilds the
same index by re-reading pages; the measured chain does not take it
and this record leaves it as it is, incremental.


## Decision

**Build the primitive index once, at finish, from an entry vector the
write appends to without hashing — measured as a paired cut under ADR
599's procedure with the retention rule unchanged, the whole query
priced, build and probe together.**

*Admission, then activation.* The branch decides the form before any
clock opens: `finish_built_index_admitted(key_plan, null_safe_keys)`
— true for an `Int32`/`Int64` plan that is not null-safe, the same
admission `for_key_plan` makes for the primitive variants. Not
admitted (a `Generic` plan or a null-safe one): the branch makes ADR
600's call, `use_compact_resident_index()`, and nothing else —
incremental, the `HashMap` as it is, `index_build = incremental`, no
reserve clock, no finalization call, both new children at zero calls
and zero time — required, not incidental, and fixtured. Admitted: the
branch opens the reserve clock, calls
`use_compact_resident_index_built_at_finish(expected_rows)`, closes
it, and the primitive index enters the *collecting* state,
`index_build = finish`; at the end it calls `finalize_index` once,
under its own clock, before `finish`. Reserve once, finalize once,
both clocked when the handle exists.

*Reservation, in its own term.* A new child, `build_write_reserve`,
opened by the branch around the admitted activation call and closed
after it, gated on the handle like the others: the entry vector is obtained at
`expected_rows` capacity — the sizing pass's rows — after a guard on
the planned bytes (`expected_rows × 24`, checked arithmetic, refused
by name on overflow) against the retained cap, and accounted at the
capacity *obtained* (`Vec::capacity` after the allocation), not the
planned figure. Calls 1 when `index_build = finish`, else 0 with zero
time.

*Collecting.* Each page appends one `{key, first ref}` entry per keyed
row — no bucket, no hash, no probe, no resize — inside the per-page
`build_write_index` span, which keeps clocking the loop. A row past
the obtained capacity grows the vector as `Vec` grows it, the delta
accounted after from the obtained capacities and counted
(`index_capacity_growths`); during that growth the old and new vectors
coexist, a transient the accounting does not carry, exactly as today's.
A lookup in the collecting state is a kernel error, by name; the write
path never probes, and the deferred path is unchanged.

*Finalization, in its own term.* A new build method,
`finalize_index(budget)`, called by the branch before `spilled.finish`
and clocked by a new child `build_write_finish_index` that closes
before `build_write_finish` opens — disjoint spans, both under the
parent, neither nested. It moves the index from *collecting* to
*finalized*: the bucket count is the smallest power of two at or above
`2 × entries.len()` (checked; `entries.len()` must be below the `u32`
sentinel and the count representable, else refused by name before any
allocation), guarded on the planned bucket bytes against the retained
cap — a refusal here leaves the build cleanable with the vector still
accounted — then allocated and accounted as obtained; then one pass in
insertion order with a write cursor `w`: hash and probe entry `i`
against the buckets, which only ever point below `w`; an empty slot
takes `w`, `entries[w] = entries[i]` (a move downward, `w ≤ i`), `w +=
1`; an equal key at `entries[j]`, `j < w`, receives `entries[i]`'s ref
on its overflow list — accounted from the list's obtained capacity, an
allocation the transient rule does not cover — and `i` is dropped,
its slot free for a later move. Refs on a key stay in insertion order:
the first occurrence's ref inline, the later ones appended in scan
order. The vector is truncated to `w`; its capacity is kept and stays
accounted as obtained. Calls 1 when `index_build = finish`, else 0
with zero time. A second `finalize_index`, a write after
finalization, or a `finish` on a collecting index are kernel errors,
by name. The guard in `spilled.finish` runs as today, on the whole.

*Facts and rules.* `index_build` (`incremental` / `finish`) beside
`index_mode` and `index_variant`; `finish_index_entries`,
`finish_index_duplicates`, `finish_index_buckets`,
`index_capacity_growths`, `index_expected_rows`; the rules `finish ⇒
compact ∧ variant ∈ {int32, int64}`, `finish_index_entries +
finish_index_duplicates = index_entries`, `finish_index_buckets` a
power of two `≥ 2 × finish_index_entries` (and `≥ 1`), `reserve` and
`finish_index` calls 1 iff `index_build = finish` and 0 with zero time
otherwise, the residual as before over thirteen children, parent
reconciliation unchanged.

*Zero and degenerate inputs.* No keyed rows — zero rows, or every key
null under a primitive plan — finalize to an empty vector and one
bucket; every lookup misses; the facts read zero entries, zero
duplicates, one bucket. An estimate short of the rows grows the
vector; an estimate long of them leaves capacity that stays accounted;
a duplicate-heavy input lands most rows on overflow lists and truncates
the vector to the distinct keys. The representability limits are
fixtured on the sizing arithmetic alone, with no allocation.

*What the accounting guarantees, and what it does not.* When the
estimate covers the rows and no key repeats, every byte the index
holds is obtained after a guard on its planned size and accounted at
its obtained size, and nothing is allocated outside the two terms.
Growth on a short estimate is allocated in the write loop, inside the
per-page index span, and accounted after from the obtained capacity,
as today's growth is; overflow lists on repeated keys are allocated
during finalization, inside `build_write_finish_index`, and accounted
from their obtained capacities as the pass makes them. Both carry the
old-and-new overlap of a growing `Vec` uncarried — scoped so, and
counted.

*The probe.* Unchanged in code: `lookup` walks the same buckets and
entries. Its cost is in the pairs, and the mechanism check reads
replay 1's lookup hits and misses through the collection ADR 600's
closing tree publishes.

*The arithmetic, not a measurement.* On q21's counts — the estimate
exact, no key repeated — the index holds 5,845,938 × 24 = 140.3 MB of
entries at the sizing capacity and 2²⁴ × 4 = 67.1 MB of buckets —
207.4 MB accounted against 268.4 MB — with no rehash, no vector
reallocation, and under those two conditions no transient outside the
accounting; the hash-and-probe work is once per row at finish instead
of once per row plus 8.4 million rehashes over the write. Whether that
clears a bar of twice the day's control range is the measurement's to
say.

*The experiment.* Control: this record's tree with the incremental
compact index (ADR 600's call). Treatment: the same tree with the one
call replaced by the finish-built form. Two release executables on one
recipe, ADR 588's manifest gate, the twelve-row driver, ADR 592's
validator with the arm's executable and the sealed digest on every row,
ADR 593's readings and cutoff, ADR 595's separated launch and
precedence, ADR 599's gate procedure with `--gate-settle-seconds 5`
(threshold and load bound unchanged), one attempt, and **ADR 588's
rule: retained only if the median paired gain exceeds `2 ×
range_control` and every pair is strictly positive** — all by reference
to ADRs 597, 599, and 600, unchanged. Every outcome other than
`retained` reverts the call in the closing commit. The mechanism check
(ADR 596's driver on the treatment, the same procedure) follows a
verdict, never a refusal, and reports the index facts, the reserve
and both finish terms, the resident peak, and replay 1's lookup
counts.

**What this record does not do.** It does not change the probe, the
page format, the sizing passes, the Exchange handling, the hash
function, or the fingerprint; it does not touch the deferred-replay
rebuild or the `Hash` variant; it does not read the 303.9 ms child or
the 2,313 ms replay stage as a bound on the gain; it does not retain a
gain the rule does not establish.


## Acceptance

- The finish-built form on the spilled branch in the treatment tree,
  asserted by diff; the admission ahead of every clock; a `Generic` or
  null-safe plan taking ADR 600's call and the incremental `Hash`
  index unchanged, with `build_write_reserve` and
  `build_write_finish_index` both at zero calls and zero time on the
  ledger and the receipt, fixtured.
- Value parity, the whole scope: the same complete rows for every key
  through the build's own `lookup` — incremental against finish-built
  for `Int64` and `Int32`, keys repeated within and across batches
  with the refs in insertion order, nulls under the primitive plans
  (never indexed on either), an absent key, growth across pages —
  each asserting `index_build` and the variant.
- The finalization pass under a seeded hasher: keys chosen to collide,
  to probe past the array's end and wrap, and to repeat after their
  first occurrence's slot has been moved down and after a dropped slot
  has been reused by a later key; every lookup after the pass returning
  the right refs in order, the buckets pointing only below the cursor.
- The states: a lookup while collecting refused by name; a write after
  finalization refused; a second `finalize_index` refused; `finish` on
  a collecting index refused; cleanup after each refusal.
- Accounting: the reservation guarded on the planned bytes and
  accounted at the obtained capacity; a short estimate growing the
  vector with the growth counted; a long estimate leaving the capacity
  accounted after truncation; a duplicate-heavy input accounted from
  the overflow lists' obtained capacities and truncated to the distinct
  keys; zero rows and all-null input finalizing to one bucket; the
  bucket allocation refused before it happens on a budget the guard
  rejects, the build still cleanable; the sizing arithmetic refusing
  an entry count at the `u32` sentinel and a bucket count that cannot
  be represented, without allocating; the accounted peak equal to the
  obtained capacities × their sizes plus the overflow heap.
- The ledger: thirteen children — `build_write_reserve` and
  `build_write_finish_index` — with disjoint spans, the reserve around
  the activation call, `finish_index` closed before `finish` opens,
  both gated on the handle; calls 1 iff `index_build = finish`, else 0
  with zero time; the residual and the parent reconciliation over the
  thirteen; the facts and rules above on the receipt and in the tpch
  bands; the sixth band's lookup counts read from the collection.
- Two executables built on bigfoot, both ladders green, staged and
  hash-verified on strix; one run under the declared procedure with
  twelve rows, readings, separation and window verified, the control
  range and the bar, the gain per pair and its median, the verdict,
  the entry and gate readings reported as observations; or the refusal.
- After a verdict, the mechanism check: `index_build`, the finish
  facts, the reserve, `finish_index`, `finish`, and the index child
  beside ADR 600's, the resident peak, replay 1's lookup hits and
  misses.
- Plan commit, then one implementation/results/closure commit; every
  outcome other than `retained` reverts the call in that commit.


## Guardrails

- The retention rule, the twelve-row contract, the gate procedure, and
  the threshold are ADR 597/599/600's and ADR 593's verbatim.
- Build on bigfoot, measure on strix, the separated launch, nothing
  else on strix in the window; no relaunch under this record.
- The probe path and the hash function are not changed under this
  record; a probe-side cut is its own record.


## Risks

- The finish pass does the same 5.85 million hashes and probes the
  write does now, in one burst over a 67 MB bucket array instead of
  interleaved with page writes; the pairs may price that burst above
  the rehashes it removes.
- A sizing estimate short of the rows costs a reallocation the design
  avoids; the fact counts it, and the arithmetic above assumes q21's
  exact estimate.
- The existing finish term (set_len, fdatasync, the guard together)
  sits beside the new one; the two are disjoint on the receipt, and
  neither is netted against the other.
- The reserve is a term of its own so that a 140 MB allocation is
  never a residual; if the sizing rows are ever absent the branch
  reserves at zero and the growth counter says so.


## Forward

- Retained: the probe side — `refs` allocating per hit, the hash
  function — becomes the next candidate on replay 1, grounded on the
  lookup counts this record's mechanism check reads.
- Not established: the incremental index stays; an ordering census of
  this build's keys (ADR 589's phase 1) would say whether a probe-free
  append exists here at all.
