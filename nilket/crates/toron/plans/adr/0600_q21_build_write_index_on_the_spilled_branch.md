# ADR 600 - q21's replay-1 build write: the index child, grounded, and the resident index the spilled branch never enables

**Status:** CLOSED, RETAINED. One paired attempt on strix under the
declared gate procedure (results in
`plans/impl/0600_q21_build_write_index_on_the_spilled_branch_results.md`):
the gate admitted after the 5 s wait (entry 46 °C, gate 38 °C, load
0.00), separation and window verified, digest parity on all twelve
rows, every pair positive — median gain 559.7 ms (6.23%) — against a
bar of 191.8 ms (2 × the 95.9 ms control range): `retained` under ADR
588's rule; the one call stays. The mechanism check on the treatment
read `index_mode = compact`, `index_variant = int64`, 5,845,938
entries, a resident peak of 268,506,120 bytes, the index child at
303.9 median-scaled ms and `inner_1_build_write` at 688.7; replay 1's
lookup counts were absent on that check — the publication sat on a
handler the measured chain does not route through, and the receipt
read the absence as disabled — and the closing tree carries the fix
(one collection per chain, a three-valued reading, fixtures through
the real path); the counts stay unmeasured under this record.
Successor to ADR 596, which decomposed
`inner_1_build_write` and left `build_write_index` as its leading child
(482.4 median-scaled ms, below the floor), and to ADR 599, which closed
the Exchange-skip candidate `not_established` and measured the index
child again on the treatment at 489.6 ms. Those figures are what the
child spans on those runs; they are context for this record, not a
savings estimate.


## Context

**What the index child contains, at `c1ab6ab3`.** ADR 596 clocks
`build_write_index` per emitted page from the instant after the page's
seal and write until after the resident-state guard, in
`write_page_batch_inner` (`exec/join/spill.rs`). Inside the span, in
order: on the first page, the index's construction (`SpilledBatchIndex::for_key_plan`
when the compact index is enabled, else `self.index.clear()`); the
insert loop over the chunk's keyed rows — for each `(key, row)` drained
from the scratch refs, `append_spilled_batch_row_ref` on the legacy
path (`HashMap<JoinKey, Vec<SpilledBatchRowRef>>::entry`: a hash of
the key, a probe, and on a vacant entry a `Vec` of one 16-byte ref
allocated on the heap and the 64-byte base accounted) or
`SpilledBatchIndex::insert` on the compact path, and `self.rows += 1`;
`try_compact_near_cap_resident_index` (a no-op unless the test-only
rescue is enabled); the page bookkeeping (`next_offset`, `page_rows`,
`pages`, the indexed-flush counters); and `guard_resident_state_after_write`
(a `resident_state()` total against the cap). On q21's path every row
is keyed and `o_orderkey` is unique, so the loop is 5,845,938 vacant
inserts — 5,845,938 heap allocations of a one-element `Vec` and as
many `HashMap` entries — over 1,465 pages (ADR 599's treatment) or
5,860 (ADR 596's split). The ledger's `index_mode` fact on both runs
is `legacy`.

**Why the legacy index, and what the tree already has.** The spilled
build carries two indexes: the legacy `HashMap<JoinKey, Vec<SpilledBatchRowRef>>`,
and the compact `SpilledBatchIndex` — `PrimitiveSpilledBatchIndex<i32|i64>`
for a single non-null-safe `Int32`/`Int64` key, an open-addressing
bucket array of `u32` entry indices (power-of-two size, at least twice
the entry count, resized by doubling with a rehash of every entry) over
an `entries: Vec<{key, refs: SpilledBatchRefList}>` in which the first
ref is inline (8 bytes: `page_index: u32, row: u32`) and further refs
for a repeated key go to a boxed overflow `Vec`; `Hash` otherwise. It
is enabled by `use_compact_resident_index()`, which three paths call:
`prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline`
(the build-from-batches fallback, when the estimate exceeds the
materialization cap) and the two streamed spilled builds in
`exec/join/streamed_spill.rs` (`try_execute_streamed_spilled_left_semi_join`
at line 60 and `try_create_streamed_spilled_inner_build` at line 992). The streamed-scan spilled branch —
q21's path — creates its build with `create_with_resident_baseline`
and never calls it, so it runs the legacy index. The compact index is
tested in `spill.rs` (insert, lookup, resize, accounting, the rescue
that converts a legacy index into it) and is the `IndexAtFlush` mode's
index on the coalesced writer. Nothing in the tree says why the spilled
branch was left on the legacy index; the record does not guess. The
call enables the compact *wrapper*: which variant the build gets is
`for_key_plan`'s choice at the first page — `Int32` or `Int64` for a
single non-null-safe integer key, `Hash` for a generic key or a
null-safe plan — and the two are different facts.

**What a replacement removes, what it keeps.** Switching the spilled
branch to the compact index removes, per row, the `HashMap` entry
machinery and the one-element `Vec` allocation, and replaces them with
a hash, a linear probe into the bucket array, and a push onto
`entries` (amortised doubling of the `Vec`, with a copy) plus, when the
load factor passes one half, a bucket resize with a rehash of every
entry so far — from a one-slot table to 2^24 slots, 24 bucket resizes
and about 23 entry-vector doublings for 5.85 million entries, the last
resize rehashing 4.2 million. Index construction therefore remains:
one hash per row at insert, the entry pushes, and the resizes; it moves
none of it after the last batch, so ADR 596's clocks keep their
meaning and the index child is measured where it is. A once-built
index (ADR 589's pattern on the semi summary: collect `(key, ref)`
pairs during the write, size the table once at finish, fill it in one
pass) would remove the resizes and the per-insert probing too, but it
moves the hashing after the last batch, needs its own ledger term so
the write's clocks are not merely emptied, and is a second design;
this record grounds it and does not build it.

**Memory.** The legacy index's accounted bytes on ADR 599's treatment
run: `resident_state_peak_bytes` 514,525,240 — the accounting's 64
bytes per key plus 16 per ref, over 5,845,938 entries, plus the 64 KB
reserve and the 17 KB plaintext peak; the allocator's true footprint
of 5.85 million small `Vec`s is above it and unrecorded. The compact
index, by its own accounting (`spilled_primitive_index_base_bytes`:
bucket bytes plus `entries.capacity() × size_of::<entry>()`): at
5,845,938 entries, buckets of 16,777,216 × 4 bytes = 67.1 MB and an
entry vector of at most 8,388,608 × 24 = 201.3 MB of capacity — about
**268 MB of estimated retained capacity** after the last doubling. That
is not a peak. `resize_buckets` allocates the new bucket array and
rehashes every entry into it before the old array is replaced and
dropped, so both arrays are live during a resize (the last: 33.6 MB
old beside 67.1 MB new); the entry vector's own reallocation likewise
holds the old and the new capacity together while it copies (the last:
100.7 MB beside 201.3 MB); and the resident-state guard runs once per
page after that page's insert loop, not after each allocation, so it
observes neither overlap. The accounting itself (`accounted_bytes`)
counts the new bucket array from the resize and the entry capacity
from the push; the guard judges that figure at the page's end. These
are arithmetic from the structures' definitions, not measurements; the
attempt records `resident_state_peak_bytes` on the treatment and the
ledger's `index_mode` and `index_variant`, and a fixture asserts the
accounted bytes at a known entry count from the obtained capacities
(`buckets.len() × size_of::<u32>()` plus `entries.capacity() ×
size_of::<PrimitiveSpilledBatchIndexEntry<K>>()`), and that a build
whose accounted index crosses the cap is refused at the page's guard
with its spill file removed by the branch's cleanup and no runtime
returned, on the compact path as on the legacy one.

**The probe side.** `SpilledInnerJoinBuild::lookup(&JoinKey)` returns
a `Vec<SpilledBatchRowRef>` on both paths: the legacy path clones the
entry's `Vec` (one allocation per hit), the compact path builds one
from the ref list (one allocation per hit); a miss allocates nothing on
either. The drive's `inner_replay_1` stage — 2,084 ms over 23,436
calls on ADR 599's treatment profile — is where any change in lookup
cost lands, and the paired gain is net of it by construction. How many
lookups replay 1 makes is not on record (the stage table carries no row
counts; the probe input is replay 0's output, not a scan). This record
adds a lookup count with a stated lifecycle: **enabled** on the spilled
build at its creation in the spilled branch only when the ADR 594
lifecycle handle is present (`count_lookups`, off on the unprofiled
arm, so the release pairs do no counter work); **incremented** inside
`SpilledInnerJoinBuild::lookup`, hits (a non-empty result) and misses
(an empty one) separately, on that build alone; **published** once,
after the drive, in the chain's cleanup loop that already collects
each runtime's lifecycle facts per replay id, as the diagnostic
`inner_replay_lookups` `{replay_id, counted, hits, misses}` — so the
count is the whole drive's, not a build-time snapshot, and replay 1's
build is told apart from the other replays' by its id. The receipt
surfaces replay 1's as `inner_1_lookup_hits` / `inner_1_lookup_misses`
beside the sixth band's facts. A fixture drives lookups through the
real path against a build with counting enabled and one without, and
asserts the published counts after the runtime's cleanup.


## Decision

**The candidate is the compact resident index on the spilled
scan-build branch — one call the branch does not make today —
measured as a paired cut under ADR 599's procedure with the retention
rule unchanged.** Control: this record's tree with the spilled branch
as it is. Treatment: the same tree with `spilled.use_compact_resident_index()`
after `create_with_resident_baseline` in the spilled branch of
`prepare_reoriented_inner_join_runtime_with_scan_build`, and nothing
else — the call the from-batches fallback already makes; for a key
plan that is not `Int32`/`Int64` or is null-safe, `for_key_plan` yields
the `Hash` variant and the receipt's `index_mode` says so, which is a
fact, not a refusal. Two release executables on one recipe, ADR 588's
manifest gate, the twelve-row driver, ADR 592's validator with the
arm's executable and the sealed digest on every row, ADR 593's
readings, journal, scope, and 4% cutoff on the control range, ADR
595's separated launch and precedence, the declared gate procedure
with `--gate-settle-seconds 5` as ADR 599 ran it (threshold and load
bound unchanged), one attempt, and **ADR 588's rule: retained only if
the median paired gain exceeds `2 × range_control` and every pair is
strictly positive** — all by reference to ADRs 597 and 599, unchanged.
Every outcome other than `retained` reverts the call in the closing
commit. The mechanism check (ADR 596's driver on the treatment, the
same procedure) follows a verdict, never a refusal, and never reopens
it; it reports `index_mode = compact`, `index_entries`, the resident
peak, the index child's reading beside ADR 596's and ADR 599's, and
the new lookup count.

**Value parity, over the whole scope the call activates.** The call
enables the compact wrapper for every admitted spilled scan build, so
the engine fixtures cover every variant it can take: ADR 597's parity
fixture — the same complete rows for every key through the build's
own `lookup`, keys repeated within and across batches, an absent key —
run with the legacy index on one build and the compact wrapper on the
other, for an `Int64` key, an `Int32` key, and a generic key (`Utf8`,
the `Hash` variant), each with nulls under a non-null-safe plan (not
indexed on either) and under a null-safe plan (the `Hash` variant,
indexed on both), and with enough rows over several batches to force
bucket resizes and entry-vector growth across pages; each fixture
asserts the variant the build reports. On the board, the sealed digest
on every row of both arms.

**Facts and accounting.** The ledger already records `index_mode`
(`legacy` / `compact` — whether the wrapper was enabled),
`index_entries`, `index_compactions`, and the resident peak. This
record adds `index_variant` (`hash` / `int32` / `int64`, the variant
`for_key_plan` chose — a different fact from the mode, recorded
separately), the lookup counts above, and the accounting fixture from
obtained capacities and `size_of` with cleanup verified on a budget
refusal.

**What this record does not do.** It does not build the once-built
index; it does not change the probe side, the page format, the sizing
passes, the Exchange handling, or the fingerprint; it does not read the
482–490 ms child as a bound on the gain; it does not retain a gain the
rule does not establish.


## Acceptance

- The one call on the spilled branch in the treatment tree, asserted
  by diff; the parity fixtures legacy/compact for `Int64`, `Int32`, and
  a generic key with duplicates, nulls under both null-safety settings,
  absent keys, and growth across pages, each asserting the variant;
  the accounting fixture from obtained capacities and `size_of` with
  cleanup verified on a budget refusal; `index_variant` on the ledger
  and the receipt; the lookup counts enabled only under the handle,
  counted in the real lookup path, published at cleanup per replay id,
  with a fixture on the published counts.
- Two executables built on bigfoot, both ladders green, both staged and
  hash-verified on strix; the manifest gate admitting the pair.
- One run on strix under the declared gate procedure: twelve rows,
  readings, separation and window verified, the control range and the
  bar stated, the gain per pair and its median, the verdict, the entry
  and gate readings and the wait reported as observations; or the
  retained refusal or closure.
- After a verdict, the mechanism check: `index_mode`, `index_variant`,
  entries, resident peak, replay 1's lookup hits and misses, the
  seventh band's terms beside the historical ones.
- Plan commit, then one implementation/results/closure commit; every
  outcome other than `retained` reverts the call in that commit.


## Guardrails

- The retention rule, the twelve-row contract, the gate procedure, and
  the threshold are ADR 597/599's and ADR 593's verbatim.
- Build on bigfoot, measure on strix, the separated launch, nothing
  else on strix in the window; no relaunch under this record.


## Risks

- The compact index's per-insert cost is a hash and a probe like the
  legacy's; what it removes is allocation and entry overhead, and
  whether that clears a bar of twice this run's control range is the
  measurement's to say — ADR 599's every-pair-positive 139.6 ms did
  not clear 467.4.
- The resize rehashes and the entry-vector reallocations are inside
  the index child and are counted in its time; their overlaps of old
  and new allocations are not observed by the guard, which runs once
  per page, as recorded; the attempt runs under a 2 GiB task budget
  whose retained-footprint cap the estimated capacity is far under.


## Forward

- Retained: the once-built index (ADR 589's pattern) becomes the next
  candidate on this child, with its own ledger term for the finish.
- Not established: the parameter stays as it was; the next candidate
  on this path is chosen from the mechanism check's readings.
