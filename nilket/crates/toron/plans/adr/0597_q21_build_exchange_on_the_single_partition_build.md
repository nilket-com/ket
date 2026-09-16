# ADR 597 - q21's replay-1 build write: the Exchange feeding the single-partition build

**Status:** PROPOSED. Successor to ADR 596, which decomposed
`inner_1_build_write` below the floor and named two shapes for a
successor: the legacy index (kept separate, its own record, not
preferred here — its 482.4 ms child is below the floor too and a
replacement still constructs an index) and the Exchange of four
partitions feeding a one-partition build (this record).


## Context

ADR 596 established, on every pair, that q21's replay-1 build input is
`Exchange { partition_count: 4, key: o_orderkey } → Scan(orders)` with
the status predicate below the writer, that the spilled branch writes
the rows as the scan delivers them (no writer-side projection; 4.6
plaintext bytes per row) to one file, and that the writer's `repartition` child —
one `repartition_exchange_batch` per batch, 1,465 calls — costs 64.7
median-scaled ms, 0.72% of q21 and 7.4% of the build write. It also
recorded that the split precedes the per-batch chunking, so the 1,465
batches become 5,860 pieces, chunks, and pages of about a thousand
rows. This record grounds what that Exchange does on the path and what
skipping it would and would not change, then runs the scoped
experiment: the write alone stops honoring it, measured as a paired cut
under ADR 588's retention rule at the strix coordinate.

**Where the Exchange comes from.** The binder wraps both sides of every
`HashPartitioned` join in `exchange_for_join_keys(plan, keys,
DEFAULT_AUTO_JOIN_PARTITION_COUNT = 4)` (`binder.rs`, the
non-local-hash-join arm; `binder/join.rs::exchange_for_join_keys`). It
is the plan's distribution shape — the fragment boundary a partitioned
executor would split at — and it is part of the bound plan that the
distributed fingerprint covers. The local executor does not materialize
it: `decompose_streaming_scan_probe_input` folds `Exchange → [Filter →]
Scan` into a `StreamingScanProbeInput { scan, filter_predicate,
exchange }`, and each consumer of that input decides what to do with
`exchange`.

**What the build side does with it, at `a6de019b`.** Three passes read
the build input under
`prepare_reoriented_inner_join_runtime_with_scan_build`: the sizing scan
(`estimate_streaming_scan_inner_build_with_facts`), the partition-sizing
pass (`estimate_streaming_scan_inner_build_partition_bytes`, once per
sizing attempt), and the spilled branch's write
(`write_streaming_scan_inner_build_batches`). Each, when `exchange` is
present, calls `repartition_exchange_batch(&batch, 4, key)` on every
batch and iterates the pieces as `(_, partitioned)`: the partition index
is discarded in all three. The sizing scan sums per-piece byte
estimates; the partition-sizing pass hashes each piece's rows into the
build's own candidate partition count; the write hands each non-empty
piece to the chunk writer, which slices it into 8,192-row chunks (one
chunk, since the pieces are about a thousand rows) and writes one page
per chunk to the single spill file. The partitioned branch's probe and
the materialized-exchange accumulators on other routes do consume the
index (`partition_batches[partition]`, `next.push((routed, …))`); the
single-file spilled branch does not, on either side — the probe path
for a `Spilled` build (`visit_streaming_spilled_inner_scan_probe_chunks`)
iterates its probe pieces as `(_, partitioned)` too and probes the whole
index. On replay 1 the probe input is replay 0's output, not a scan, so
the probe side of this join is not on this record's path and is not
grounded here.

**What `repartition_exchange_batch` does to a batch.** It evaluates the
key program, hashes every row's key with `partition_bucket_with_scratch`
(a null key is encoded and hashed like any other; no row is dropped),
collects row indices per bucket, and `take`s each non-empty bucket into
its own batch. Every row of the input lands in exactly one output piece;
the pieces together are a permutation of the input, grouped by bucket
and in input order within a bucket. Membership and values are therefore
preserved by construction whether the pieces or the whole batch are
written; the only things that change are the order of rows within the
file and the page boundaries.

**What the write does not depend on.** The spilled build's index is
`HashMap<JoinKey, Vec<SpilledBatchRowRef>>` (the legacy mode ADR 596
recorded) or the compact index; neither has an order-dependent fast
path (ADR 589's consecutive-key cache is the semi-join summary's, not
this build's). Lookup is by key; page order and page boundaries do not
enter membership or values. The order in which several matches for one
probe row are emitted follows the `Vec` order, which is page order —
for `orders` on `o_orderkey` every key is unique, and in general the
join's output order is not a contract: q21's grouped chain aggregates
the matches, the final sort is total, and the harness's digest is
`SortPolicy::FullRowLex`. Page boundaries enter only the drive-side
page cache (pages decoded and retained by `page_index`, under
`cache_cap`), the resident-state guard through `writer_peak` (the
largest plaintext page, about 18.5 KB for a 3,990-row batch at the
measured 4.6 bytes per row against 4.6 KB now, far under any cap), and the per-page work of encode, page
write, and index bookkeeping.

**What skipping it in the write would and would not change.** A write
that ignored `exchange` on the single-file spilled branch would poll,
filter, and chunk each batch whole: about 1,465 pages of about 3,990
rows instead of 5,860 of about a thousand, the same rows, the same index
entries, the same ciphertext bytes to within IPC and tag framing, and no
repartition child. The sizing passes are untouched by this record: they
feed the branch decision through the estimate (per-piece `max(rows ×
120, memory)` sums), so changing them could move `partition_count`, and
their own repartition shares are a separate, unmeasured question that
does not enter this candidate's price. The partitioned branch
(`partition_count > 1`) is untouched too: it never ran on q21 at this
coordinate, and its probe reads a partition index.

**The price, stated as what it is.** The measured gross work is the
write's repartition child, 64.7 median-scaled ms (0.72% of q21). That
is not a bound on the cut in either direction: fewer, larger pages
change the per-page share of encode, page write, and index bookkeeping
(inside the 51.4, 33.7, and 482.4 ms children, which ADR 596 could not
separate into per-page and per-row parts) and the drive side's cache
behaviour with pages four times larger, and none of those is priced.
Net is what the paired run returns.


## Decision

**A scoped, write-only experiment: the spilled single-file build stops
honoring the Exchange, measured as a paired cut.** The writer takes an
`ExchangeHandling` — `Split` (today's behaviour: `repartition_exchange_batch`
per batch, each non-empty piece to the chunk writer) or `PassThrough`
(the batch whole to the chunk writer; the ledger's `repartition` child
takes no calls) — as a parameter of
`write_streaming_scan_inner_build_batches`, and exactly one call site
passes `PassThrough`: the spilled branch of
`prepare_reoriented_inner_join_runtime_with_scan_build`. Every other
caller — the partitioned branch's drive-side write, the left-outer
grouped write with preserved rows, the sizing passes, the probe paths,
the materialized-exchange accumulators on other routes — passes `Split`
and is unchanged.

**Admission boundary, explicit and recorded.** Skipping the split
also skips evaluating the Exchange's key program and hashing its
values, and both can fail under `Split`: `evaluate_bound_program` on a
computed key, `partition_bucket_with_scratch` on a key type the
partition encoder refuses (`unsupported exchange partition key type`),
and a `partition_count` of zero (`exchange partition_count must be
greater than zero`). A write that omitted them would turn those errors
into successes, an observable change. This first cut therefore admits
`PassThrough` only for a shape whose evaluation cannot fail, and
falls back to `Split` — today's loop, its errors included — for every
other: all of (a) the runtime is the spilled branch with
`partition_count == 1` (the sizing passes, unchanged, chose one
partition), `retain_unkeyed_rows` false, `join_partition` `None`; (b)
the scan input carries an Exchange with `partition_count >= 1`; (c) the
Exchange's key program has a single root that is a plain column
reference (`single_root()` and `BoundExprNode::Column`, as ADR 589's
admission reads it), whose ordinal is inside the scan input's output
schema and whose data type is `Int32` or `Int64` — the fixed-width
partition hash, which takes no fallible step on any value, null
included; the scan's own column fetch is the only evaluation such a
program does. Nothing else about the key is inspected, because nothing
on the path reads the index: the write discards it and the `Spilled`
probe does too. The decision and its reason are recorded in the ADR
596 ledger as facts: `exchange_handling` (`split` / `pass_through` /
`none` when there is no Exchange) and `pass_through_refusal` (`null`
when taken; else one of `computed_key`, `multi_root_key`,
`key_ordinal_out_of_schema`, `key_type_not_fixed_width`,
`zero_partition_count`, `branch_not_admitted`). The band's rules read
them — `pass_through` requires zero repartition calls and zero
repartition time and `batches_written = batches_admitted −
batches_filtered_empty`; `split` keeps ADR 596's rules — so a profiled
run says on every pair which path the write took and why. Fixtures
outside the admitted shape, each through the real writer with the
treatment's argument: a computed key (an arithmetic program), a
two-root key, an ordinal past the schema, a `Utf8` key, a
`partition_count` of zero — each takes `Split`, records its refusal,
and behaves exactly as the control (the zero count still errors, the
computed key is still evaluated); and the admitted shape with a null
`Int64` key takes `PassThrough`.

**Value parity, at two levels.** In the engine, a fixture through the
real writer path writes the same batches under `Split` and
`PassThrough`, finishes both builds, and then, through the build's own
`lookup` and page read as the probe uses them, looks up every distinct
key of the input plus keys not present and compares the complete
returned rows — every column's values, as a multiset per key — between
the two builds: equal for unique keys, for keys repeated within and
across batches (several rows returned, the same multiset, the order not
asserted), for null keys under a non-null-safe plan (not indexed on
either build, `NoMatch` on both) and under a null-safe plan (indexed and
returned on both), and `NoMatch` on both for an absent key; the page
count is asserted to differ (one page per batch under `PassThrough`
against more under `Split`) so the fixture cannot pass by the two paths
being the same. On the board, ADR 592's validator already requires
every child's q21 digest to equal the sealed digest; both arms of every
pair must pass it, control and treatment alike.

**Memory: what the cut changes, what the guards actually account, and
what is recorded.** The cut changes the size of the writer's per-chunk
buffers, not their number: `scratch_plaintext` holds one chunk's IPC
encoding and, after `seal_in_place_append_tag`, the ciphertext plus
its 16-byte tag in the same buffer; `scratch_refs` holds one chunk's
`(key, row)` pairs. A chunk is bounded by `CROSS_JOIN_OUTPUT_CHUNK_ROWS`
(8,192 rows) in every case, but the plaintext bytes per chunk are not
a constant: at q21's measured 4.6 bytes per row a whole 3,990-row
batch is about 18.5 KB against about 4.6 KB now, and variable-width
columns make a chunk's size a function of its content. What the
existing guards account (`resident_state()`): `resident_baseline_bytes`,
`resident_index_bytes`, `writer_peak_bytes` — the high-water mark of
plaintext page *length* (`page.plaintext_len`), not the capacity a
`Vec` obtained, not the tag, and not `scratch_refs` at all — a fixed
`page_reserve_bytes` of `JOIN_SPILL_PAGE_SIZE` (64 KB), and
`pinned_decoded_bytes`. `guard_resident_state_after_write` runs after
each page is written and pushed, releasing the scratch first if the
total is over the cap, then refusing; `finish` guards once more. So the
accounting is a high-water mark plus a fixed reserve, and the cut's
transient peaks lie outside it in two places: the obtained capacities
of `scratch_plaintext` (at least the largest ciphertext, possibly more
by the allocator's growth) and `scratch_refs` (at least
`8,192 × size_of::<(JoinKey, u32)>`), and, on the drive side, the page
cache, which inserts a decoded page *before* evicting to `cache_cap`
(`cache_peak_bytes` records the transient, `cache_cap + one page`), so
pages four times larger raise that transient by the page's growth. The
record does not claim these are guarded; it records them. The ADR 596
ledger gains, under the profiling handle only: `scratch_plaintext_capacity_peak`,
`scratch_refs_capacity_peak`, `writer_peak_bytes`, and
`resident_state_peak_bytes` (the largest `resident_state().total()`
seen at any guard site), and the mechanism check reports them beside
`cache_peak_bytes` from the residency snapshot; the results state the
obtained capacities and peak residency on the treatment against the
control's. A fixture drives a whole-batch write with variable-width
rows (a `Utf8` payload whose length makes one batch's page larger than
the cap admits while the split's pages are admitted) and asserts the
budget error — `BudgetDimension::Memory` with the write guard's site —
returned from the write; "intact" here means precisely what the branch
guarantees, not that state is unchanged: the page was written and
pushed before the guard refused, the error propagates out of
`write_streaming_scan_inner_build_batches`, the branch's error path
runs `cleanup` on the build, and no runtime is returned; the fixture
asserts the error, the page count at refusal, and that the spill file
is gone after cleanup. Admission (`partition_count`, the sizing
estimate) is unchanged by construction, since the sizing passes are.

**Measurement, as ADR 588 wrote it, under the strix protocol.** Two
`release` executables on one recipe: control from the tree with the
driver, the fixtures, and the `ExchangeHandling` parameter at `Split`
everywhere; treatment from that tree plus the one call site at
`PassThrough`. ADR 588's manifest gate (same recipe digest, different
trees, different executables, no untracked build inputs). The driver
(`adr597-q21-exchange-skip`) is ADR 596's lifecycle driver with two
admitted executables instead of a profiling arm: ADR 591's coordinate;
ADR 592's binding by content; ADR 593's start gate, readings, journal,
scope, and the 4% cutoff applied to the control arm's range; ADR 595's
separated launch with its two verified intervals. No phase 0: the gate
and the cutoff are the strix protocol's replacement for ADR 588's
tripwire.

**The twelve-row contract.** The run is exactly twelve children in this
order: row 0 the control's unprofiled warmup, row 1 the treatment's
unprofiled warmup, then five pairs of two rows alternating
control-treatment (pair 0), treatment-control (pair 1), and so on; each
row carries `adr597_arm` (`control` / `treatment`), `adr597_phase`
(`warmup` / `pairs`), `adr597_pair` and `adr597_pair_position` on pair
rows, `warmup: true` on the two warmups, the launched manifest and
executable, and the ADR 593/595 stamps (`driver_launch_unix_ms`,
`driver_return_unix_ms`, the readings). Every child, the warmups
included, is validated the moment it returns and before the next is
launched — ADR 592's validator (status, q21 digest equal to the sealed
digest, coordinate, affinity, dataset, launch manifest and hash) with
the launched executable required to be the row's arm's — and a failing
row is journaled with its refusal, the run stops, and the summary
records the refusal; a warmup that fails stops the run exactly as a
pair row does. The re-evaluation's window validator, which ADR 595
fixed at eleven rows, is generalised to the driver's expected row list:
for this driver it requires twelve rows with the arms and order above,
every row's launch and return finite with launch before return, no
overlap, and the first and last timestamps present — a missing or
malformed stamp on any row, the first and last included, is refused,
never narrowed around. Fixtures: a failing control warmup and a failing
treatment warmup each stop the run with the refusal journaled; a
missing first-row launch, a missing last-row return, an out-of-order
arm, and a wrong pair position are each refused by the validator.

**Retained only if the median paired gain exceeds `2 × range_control`
and every pair is strictly positive; a zero or negative pair fails.**
Otherwise `not_established`. ADR 595's precedence applies: a separation
or window verdict other than `verified` withholds the verdict
(`withheld_launch_separation_*`, `withheld_window_not_clean_*`) with the
pairs and the rule's outcome beside it; a control range above the 4%
cutoff closes on variability before the rule is read; a gate refusal
or a validator refusal closes on that refusal. **Every outcome other
than `retained` — `not_established`, `closed_on_variability`, a
withheld launch, a gate or validator refusal — reverts the treatment
call site in the closing commit**, leaving the parameter, the facts,
the fixtures, and the driver. The bar is whatever the run's control
range makes it — 2 × 45.6 ms was ADR 595's range, 2 × 324.1 ms ADR
596's — and the record states it before reading the gain; a candidate
whose known gross part is 64.7 ms is measured against it, not assumed
either side of it.

**Mechanism check: after a verdict, explanatory only.** When the
retention run has produced a verdict — `retained` or
`not_established` — one run of ADR 596's driver
(`adr596-q21-build-write`, its own off/on arms, separated launch) on
the treatment executable follows, under its own gate and one attempt:
the seventh band should show `exchange_handling = pass_through`, zero
repartition calls, about 1,465 pages, `batches_written =
batches_admitted`, the capacity and residency facts above, and the
other children's movement beside ADR 596's readings — named, never
netted into the gain. It runs after a `not_established` verdict as
much as after a retained one, to say what the treatment did; it does
not run when the retention run closed without a verdict (a refusal, a
variability closure, a withheld launch), because there is no verdict
to explain. In no case does it reopen the verdict or stand in for the
release pairs: retention rests on those alone, and the closing commit
reverts or keeps the call site on the release verdict before the
mechanism check is read.

**What this record does not do.** It does not touch the sizing passes,
the partitioned branch, the probe side, the binder's Exchange
placement, or the fingerprint's scope. It does not price the legacy
index or fold it in. It does not retain a gain the rule does not
establish, and it does not keep an unestablished cut in the tree.


## Acceptance

- `ExchangeHandling` on the writer, `PassThrough` at the one call site
  under the stated admission (branch, count, single plain `Int32`/`Int64`
  column root inside the schema), `Split` everywhere else; the ledger's
  `exchange_handling` and `pass_through_refusal` facts with the band's
  conditional rules and fixtures for each (`pass_through` with a
  repartition call, with repartition time, with `batches_written`
  above the admitted; `split` unchanged); the refusal fixtures
  (computed key, two-root key, ordinal past the schema, `Utf8` key,
  zero count) each taking `Split` with the control's behaviour, and
  the null-key admitted case taking `PassThrough`.
- The value-parity fixture through the real writer and the build's
  own `lookup`: unique keys, duplicate keys within and across batches,
  null keys under both null-safety settings, an absent key; complete
  returned rows compared as multisets per key; page counts differing.
- The memory fixture with variable-width rows: the larger page refused
  at the write's guard under a cap that admits the split's pages, the
  error's dimension and site asserted, the page count at refusal
  stated, the file gone after the branch's cleanup; the four capacity
  and residency facts on the ledger, with a fixture that they are
  non-zero on the profiled path and absent on the unarmed one.
- The two-executable driver: ADR 588's manifest gate, the twelve-row
  contract with the arm's executable and the digest validated on every
  row as it returns, refusals journaled, the generalised window
  validator, the cutoff on the control range, ADR 595's precedence, the
  retention rule; fixtures returning `retained`, `not_established` (a
  zero pair; a median under the bar), `closed_on_variability`, each
  withholding, a failing warmup on either arm, and each window refusal.
- One run on strix: twelve rows admitted, readings present, separation
  and window verified, the control range and the bar stated, the gain
  per pair and its median, the verdict; or the retained refusal or
  closure. Then, on a verdict, the mechanism check, reported.
- Plan commit, then one implementation/results/closure commit; every
  outcome other than `retained` reverts the call site in that commit
  and leaves the parameter, the facts, the fixtures, and the driver.


## Guardrails

- The cut is one call site's argument; the writer's `Split` path is
  today's loop, errors included, and every refusal falls back to it.
- Build on bigfoot, measure on strix, the separated launch, nothing
  else on strix in either window; the two executables staged and
  hash-verified on strix before the launch.
- The parity fixture reads through `lookup` and the page reader, not
  files or IPC bytes, which legitimately differ.


## Risks

- The bar depends on the run's control range; on a run like ADR 596's
  it is 648 ms and no plausible net gain from this cut clears it, and
  the record then says `not_established` at that bar, which is a fact
  about the run, not a bound on the cut. The cutoff refuses a range
  above 4% before the rule is read.
- Larger pages could cost more on the drive side than the write saves;
  the paired gain is net of that by construction, and the mechanism
  check names the movement.
- The writer's transient peaks (obtained capacities, the cache's
  insert-before-evict) are recorded, not guarded; a result that
  retained the cut would carry those numbers into the successor that
  decides whether the guards should account them.


## Forward

- Retained: the successor decides whether the sizing passes' handling
  of the same Exchange, and the partitioned branch's, follow, each
  under its own paired record.
- Not established: the parameter and the fact stay for the next
  candidate on this path; the legacy index's own record is next either
  way.
