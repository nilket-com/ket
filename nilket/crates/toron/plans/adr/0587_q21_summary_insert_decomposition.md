# ADR 587 - q21 existence-summary insert decomposition

**Status:** PROPOSED. Successor to ADR 586 at the same sealed ADR 583
coordinate, on the symbol-bearing `profiling` build profile with its own
paired baseline. Decomposes the owning child, `semi_write_insert`, into
growth, the row loop, and its residual by exact ledger clocks, counts the row
loop's paths exactly, and attributes CPU time inside the ledger's windows
with a time-based sampling profiler. No engine route, admission, or writer
changes under this record; no attribution-only code shaping.


## Context

ADR 586 resolved `semi_write_insert` as the owner of q21's semi write:
1,610.7 median-scaled ms, 12.27% of the 13,128.5 ms unprofiled execute
median, with `anti_write_insert` at 1,365.8 ms beside it on the same code
path; 2,976.5 ms, 22.7% of the query. The repartition child cleared the
floor at 636.0 ms as gross removable work with its net unpriced, because
the order it imposes is what the insert's consecutive-key cache sees.
Neither summary carries a key-program op on this route and the semi
carries no filter; each summary saw 5,859 source batches.

`semi_write_insert` is one clock pair per source batch around the whole
`for batch in batches { summary.write_batch(..) }` loop. That bracket holds
more than `NeqExistenceSummary::insert`: `write_batch` first downcasts and
null-checks the key and value columns, then runs the row loop, and the
batch is dropped when the loop moves on. The enclosing clock stays as it is
under this record, so the term ADR 586 measured keeps its meaning, and the
decomposition lands inside it with an explicit residual.

The row loop's shape is known from the code. `insert` compares the key
against the last key inserted; on a match it updates that entry's
representative and multi-key bit without touching the table. Otherwise it
hashes the key with `ahash`, walks a linear-probed bucket array at load
factor at most one half until it meets an empty bucket or a matching entry,
and on a miss hashes and walks again through `find_bucket` before appending
the entry and writing its bucket. When the entry array is full it doubles:
a new entry array is allocated and copied, the multi-key bitset is copied,
and every entry is rehashed into a fresh bucket array. The summary reserves
a capacity up front only when the recognizer supplies a key-count hint from
column NDV. A SF=1 run of the same route on 2026-09-14 recorded
`key_count_hint: null` for both summaries, an entry capacity of 2,097,152
for 1,500,000 unique keys over 6,001,215 rows on the semi alias, and
1,375,365 unique keys over 3,793,296 filtered rows on the anti. Doubling
from 1,024 to that capacity is eleven growth events at SF=1; the same shape
predicts fourteen at SF=8 for some twelve million unique keys over 48
million rows, four rows per key, so roughly three quarters of the rows take
the cache path and one quarter the hash-and-probe path, each miss walking
the table twice. The Parquet lineitem source is ordered by order key, so a
miss is most likely a key the table has never seen: the probe would then be
confirming absence before an append, and a positive lookup would be rare.
Whether that is so is a count, and the counters below report it exactly.

What none of this says is where the time goes: in the fourteen rehashes,
in the random bucket reads of twelve million absence probes over a bucket
array of 134 MB and an entry array of 268 MB, or in the cache path's 36
million cheap updates. A clock per row is not a way to find out: two
`Instant` reads per row over 48 million rows cost about two seconds
against a 1.6 s term. A clock per key change is twelve million clocks. The
attribution has to come from sampling, and sampling has to be verified
before it is trusted.

It was verified on this host on 2026-09-14, after the operator lowered
`kernel.perf_event_paranoid` from 4 to 2, which permits user-space sampling
of one's own processes and nothing else. The sealed `release` profile
strips symbols under fat LTO, so the check ran on the existing `profiling`
profile (`Cargo.toml`, ADR 474: inherits `release`, `debug = 1`,
`strip = false`), staged through `toron-bench build --profile profiling`
as executable `c452f6c5…`. `perf record -e task-clock:u` on q21 at SF=1
lost zero samples at 4 kHz (13,874 samples) and at 10 kHz (34,861); the
run's elapsed time under 10 kHz sampling was 1,285.8 ms against 1,236.7 and
1,278.6 ms unprofiled, inside single-run noise. Symbols resolve:
`NeqExistenceSummary::find_bucket_in` is its own symbol even under fat LTO,
and the copies of it inlined into `lookup_entry_index`, `insert`, and
`write_batch` resolve to their source lines through DWARF inline
information, so a sample on the cache-hit compare, the row loop, or the
bucket load lands on that line. Two limits were found the same way. Call
chains do not work: the out-of-line `find_bucket_in`, which serves both the
miss's second walk and the rehash, unwinds to nothing because `panic =
"abort"` leaves it no unwind tables, and DWARF stack copies of 4 KB and 64
KB both failed; so growth cannot be separated from row-loop probes by
caller. And the profiler's clock is `task-clock`, CPU time of the sampled
thread, not wall time; blocked or descheduled intervals produce no samples.
Both limits shape the method: the ledger supplies exact wall-clock windows,
the profiler attributes CPU time inside them, and the two are reconciled
rather than conflated. Recording with `-k CLOCK_MONOTONIC` was checked to
place sample timestamps on the same clock a process reads through
`clock_gettime`, which is what makes the windows usable.

Two further facts bound what a sample can say. Partition parallelism 1 is
a statement about partition work, not about operating-system threads: the
harness runs every query on a Tokio multi-thread runtime with four workers
(`toron-tpch/src/main.rs`), so the thread executing the insert is one of
several, may differ between runs, and a window is only meaningful for the
thread that opened it. And source lines do not separate the two row paths
cleanly: every row executes the last-key comparison, and `observe_value`
runs after a cache hit and after a lookup that found its key alike, so a
sample on those lines belongs to neither path by itself. The classes below
are defined with both facts in view.


## Decision

**Own baseline on the `profiling` profile.** The instrumented executable is
built with `toron-bench build --profile profiling`; the driver requires
that profile. Its codegen is not assumed identical to `release`: both arms
of every pair run on it, the off arm unprofiled, so the attribution is
internally paired, and the off median is reported beside ADR 586's
13,128.5 ms as a cross-profile distance, never netted. ADR 586's `03daafed`,
ADR 585's `21192726`, and the sealed `b436a843` are reported for identity
and run under nothing here.

**Phase 0, the host tripwire, inherited.** One unprofiled warmup and one
unprofiled sample on the staged executable, compared to ADR 586's off
median passed explicitly as the reference; more than 10% away stops the run
with the finding recorded. Because the profile differs, the tripwire here
guards against a gross host shift only; a few percent of distance is the
expected cross-profile cost and is reported, not acted on.

**The enclosing clock is preserved; three exact terms land inside it, as
windows.** `semi_write_insert` and `anti_write_insert` keep their clocks,
boundaries, and names. Inside each, when the stage handle is present:

```
semi_write_insert                        unchanged: the write_batch loop
  insert_rows        one window per write_batch around the row loop only
    insert_grow      one window per growth event, nested inside the rows
  insert_residual    column downcast and null check, batch disposal, loop
                     overhead: checked subtraction
anti_write_insert
  the same three
```

Each window is a half-open interval `[start_ns, end_ns)` of integer
`CLOCK_MONOTONIC` nanoseconds taken through `clock_gettime`, the same clock
`Instant` uses on Linux and the clock the profiler is told to stamp samples
with, and it carries the PID and TID of the thread that opened it, read
once per batch through `gettid`. The ledger sums the windows into the exact
terms (`insert_rows` from 5,859 pairs, `insert_grow` from at most fourteen)
and also publishes the window list itself, so the receipt carries both the
sums and the intervals, with their thread, that produced them. The receipt
refuses a window whose TID differs from the enclosing rows window's, and
reports the set of TIDs the summary's windows used; the expectation is one
per summary, and more than one is a finding, not a defect. `insert_grow` is
reported as its own term and subtracted from `insert_rows` to give
`insert_rows_excluding_grow`; each growth event records its ordinal, the
capacity it reached, and its micros, so the geometric series is visible.

**Exact per-row counters, compiled only into the instrumented loop.** The
row loop becomes generic over a `const INSTRUMENTED: bool`; the off arm
calls `write_batch::<false>`, which is the loop as it exists today, and the
on arm calls `write_batch::<true>`, which accumulates in locals and
publishes once per batch:

```
cache_hits          rows that matched the last key
lookups             rows that hashed and walked the table
lookup_found        of those, walks that met a matching entry
appended            of those, walks that met an empty bucket and appended
probe_steps         bucket steps beyond the home bucket, both walks
grow_events         doublings
```

with `entries_final`, `entry_capacity_final`, `buckets_final`, and the
recognizer's `key_count_hint` recorded at close. `lookup_found` against
`appended` says whether the table is ever consulted positively during the
build; `probe_steps` per lookup says whether a walk is one bucket or a
chain. Four increments per row are the instrument's whole per-row cost.

**CPU-time attribution inside the windows, by thread and source line,
from the profiler.** The driver launches every on-arm child under
`perf record -e task-clock:u -F 10000 -k CLOCK_MONOTONIC` (user space only,
no call chains, inheritance left on so every thread the runtime creates is
sampled; `--no-inherit` would drop threads created after exec, which is
where the query runs) and keeps the `perf.data` beside the node profile.
After the run it converts each profile with `perf script --ns -F
pid,tid,time,period,ip,sym,srcline`, so timestamps are integer
nanoseconds, and bins every sample by thread, window, and source line. A
sample is inside a window only if its PID and TID equal the window's and
`start_ns <= t < end_ns`; a sample on another thread is never inside any
window, whatever its time. The driver requires that the thread each
summary's windows name appears in the profile, and reports per summary how
many samples that thread produced in and out of the windows.

Windows nest and attribution is exclusive: a sample inside an
`insert_grow` window belongs to growth and to nothing else; a sample inside
an `insert_rows` window and outside every growth window belongs to the
rows-excluding-growth classes. The line classes are fixed before the run
from the measured tree and recorded in the receipt as line ranges:

```
hash_and_probe    the hash, both walks (the inlined lines and the
                  out-of-line find_bucket_in symbol), and the append:
                  lines only a miss executes
shared            the last-key compare, the row loop's own lines, and
                  observe_value: lines every row, or both paths, execute
growth            everything inside an insert_grow window, split further
                  into copy, rehash, and other_growth lines
other             any line inside a rows window not in a class, and any
                  sample whose line or symbol did not resolve
```

There is no `cache_hit` class, because no line is executed by the cache
path alone: the compare runs on every row and `observe_value` runs after a
hit and after a found lookup alike. `shared` is reported as shared work
and is not allocated between the paths: `lookup_found` counts how often
the found path ran, not what share of `observe_value`'s time it took, and
a count cannot subtract CPU time. The only statement the record makes
about the cache path is a classification bound on the sampled
attribution: every sample the path could have produced lies in `shared`
or `other`, so its sampled share is at most `shared + other`. That bounds
what the samples attribute, not the path's actual CPU cost, to which skid
and sampling uncertainty still apply; the bound is reported as such, with
both classes' sample counts. If
the measured instruction ranges inside the compiled loop turn out to
separate the paths, the results say which addresses and why, and a
`cache_hit` class may be added there with its range shown; the plan does
not assume it. Growth is attributed by
window, never by caller, which is what the unwind limit requires; a sample
of the out-of-line `find_bucket_in` inside a rows window and outside every
growth window is a miss's second walk, and inside a growth window it is
the rehash. Samples outside every window, on any thread, are not the
insert and are reported only as totals per thread.

**Coverage: sampled periods inside a window against the window's wall,
reported as an estimate.** For each summary and pair, the sum of sample
periods inside the `insert_rows` windows on the window's thread is
reported beside the ledger's `insert_rows` micros as an estimated coverage
ratio. It is an estimate because a period is assigned by its sample's
timestamp and can straddle a window edge, because timer sampling has a
phase, and because the kernel may throttle the sampler; the difference
between the sum and the wall term is reported as unexplained discrepancy,
and the record attributes none of it to descheduling, page faults, or
kernel time. Attributing the discrepancy would need an independent CPU-time
account for the thread over the same windows, which this record does not
take; a successor that wants it reads the thread's `CLOCK_THREAD_CPUTIME_ID`
at each window edge, and the plan names that as the instrument rather than
inferring it from the sampler. The attribution shares are stated as shares
of sampled CPU time within the window class, with `other` as the explicit
residual; nothing is scaled to force the shares to sum to the wall clock.

**Timing algebra, recorded.** Beside ADR 585's three bands and ADR 586's
fourth, per summary:

```
insert_rows + insert_residual                        = <summary>_write_insert
insert_grow + insert_rows_excluding_grow             = insert_rows

rows_inclusive          = sampled periods in insert_rows windows, own thread
rows_inclusive          = rows_excluding_growth + growth
rows_excluding_growth   = hash_and_probe + shared + other
growth                  = copy + rehash + other_growth
rows_inclusive / insert_rows                          estimated coverage
insert_rows - rows_inclusive                          unexplained discrepancy
```

The first two by checked subtraction with `instrument_defect` on
underflow; the sampled identities hold exactly by construction, because
every sample inside a rows window lands in exactly one class, and a
sample inside a growth window lands in a growth class only. Completeness for pair i: `insert_residual_i` at most `overhead_i
+ 0.05 * <summary>_write_insert_i`; every exact residual non-negative; ADR
585 and 586 bounds still apply above. A sample outside a bound is reported
and withholds the owner.

**Arms, pairs, gate: inherited.** Off arm with no diagnostic and no
profiler; on arm with `TORON_NODE_ATTRIBUTION_PROFILE_PATH` and the
profiler together, so the perturbation gate prices the counters, the
windows, and the sampler as one instrument. Phase 0, one unprofiled
warmup, five adjacent alternating pairs. q21 alone, strict SF=8, persisted
Parquet, explicit 2 GiB task memory and 2 GiB spill reserve, partition
parallelism 1, empty dedicated spill scratch, deployment stopped for the
run and restarted after it, full-hash dataset verification, fresh output
directory. The driver refuses to start unless `perf` is present,
`perf_event_paranoid` is at most 2, and a one-second self-check recording
of the harness itself completes with zero lost samples; it records the
`perf` version and the exact command line in the summary. It refuses a
profile whose header reports lost samples, or whose summary threads are
absent from the profile. The sampled classes are reported simply: each
class's period-weighted share of its window total, per pair and across
all five pairs, with its sample count. A ranking that holds in every
pair is described as a consistent observation; a ranking that changes
between pairs, or a class whose samples the binner could not place, is
reported as unresolved. No confidence interval is attached: in frequency
mode the kernel adjusts the sampling period as it runs, samples are
correlated in time, and skid is systematic, so no simple error bound
describes them, and the record claims no statistical significance for
any share. Sampling alone authorizes no mechanism; it narrows what the
counts and the exact wall terms already say. Perturbation
gate as ADR 584 wrote it: reject when the median of `|m_i|` exceeds 0.25.

**Statistic and owner rule, over the semi insert's exact terms.** For term t
and pair i, `scaled_t_i = term_t_on_i / (execute_on_i * 1000) *
execute_off_i`; median over five pairs; `range_off = max(execute_off) -
min(execute_off)`; a term is a candidate owner only if `median(scaled_t) >
max(500 ms, 2 * range_off)`; ties within `range_off` unresolved; a leading
residual is an incomplete instrument; no term clearing the floor is the
below-floor outcome with no mechanism opened. The candidate universe is
`insert_grow`, `insert_rows_excluding_grow`, and `insert_residual`, wall
terms from the ledger. The sampled classes are not owner candidates: they
decide the successor within the owning term, as measured CPU shares with
their sample counts stated. The anti insert's terms and classes are
reported beside the semi's, not ranked. ADR 586's five semi-write children
and ADR 585's thirteen terms are re-reported from this run's receipts as
consistency checks against their prior medians, stated in words as a
cross-profile comparison, never netted.

**Removable pricing, as ADR 586 wrote it, applied to the owning term.** For
`insert_grow` the term is priced as gross removable work, exactly as the
repartition child was: growth exists only because the hint is absent, and
a hint from the scan's row count or NDV would replace fourteen doublings
with one allocation, but that allocation still has to be made and its
bucket array initialised, its first touch still page-faults, and the copy
and rehash it removes are only part of what the term holds. The gross term
clearing the floor opens the measurement, not the cut: a successor prices
the replacement allocation, demonstrates net execution improvement in
paired off/on runs, and carries a digest value gate and a peak-memory
accounting check, since a reservation commits the peak early. For `insert_rows_excluding_grow` nothing is removable as such;
the successor is chosen from the counts and the sampled classes: a
near-zero `lookup_found` with a `hash_and_probe`-dominated class points at
the absence probe, whose candidates are the repeated walk on a miss and a
build that exploits the source order; a `probe_steps`-per-lookup well above
zero points at the hash or the load factor; a `shared`-dominated class
would say the rows' common work is not cheap and, with `lookup_found`
small, would leave the cache path's sampled share bounded by `shared +
other` and name the compare and
`observe_value` for a closer look. The record
names the candidate and states that any cut must show net execution
improvement in paired off/on runs beside value parity, as the repartition
candidate must.

**What this record does not do.** It does not change the summary, its
growth policy, its hashing, the recognizer's hint, the repartition, or any
route. It adds no `#[inline(never)]`, no frame pointers, no unwind tables,
and no other shaping of the code for the sake of attribution; the sampled
lines are the lines the optimizer produced. It does not add a clock per row
or per key change. It does not decompose `cleanup` or the replay-1 build.


## Acceptance

- The instrument lands first with paired tests: the off-arm loop is the
  uninstrumented monomorphization and the on-arm loop the instrumented one,
  shown by a test that drives both over the same batches and asserts equal
  summaries with counters and windows only on the second; a multi-batch
  source with repeated consecutive keys yields exact `cache_hits`,
  `lookups`, `lookup_found`, `appended`, and `grow_events` against
  hand-computed values; a source that forces growth records one
  `insert_grow` window per doubling, nested inside its rows window, with
  its capacity; windows are monotone, non-overlapping within a level, and
  sum to the published terms; the receipt computes both insert residuals by
  checked subtraction and reports `instrument_defect` on underflow; ADR
  585's and 586's bands and ADR 507's exclusive figures are unchanged,
  shown by their tests passing untouched.
- The sample binner is tested on a synthetic `perf script --ns` text and
  window list: a sample at `start_ns` is inside and one at `end_ns` is
  outside; a sample on another TID at a time inside a window is outside
  every window; a sample inside a nested growth window lands in growth and
  is absent from the rows-excluding-growth classes; the sampled identities
  hold exactly; a resolved line in no class, and an unresolved line or
  symbol, land in `other`; samples outside every window are counted only
  as per-thread totals.
- The window recorder is tested for the thread binding: windows carry the
  opening thread's PID and TID, a nested growth window inherits and is
  checked against its rows window's TID, and the receipt lists the TIDs
  per summary.
- The driver checks `perf` presence, the paranoid level, and a zero-loss
  self-check before phase 0; applies the phase-0 tripwire against an
  explicit reference; requires one `inner_prefix_semi_anti_grouped` receipt
  per on-arm sample carrying both summaries' three exact terms, window
  lists, counters, and growth events, plus a `perf.data` whose header
  reports zero lost samples and whose samples include each summary's
  window thread; applies the three-term owner rule with all three
  outcomes and the class report with its consistent-or-unresolved
  ranking; each refusal, each outcome, and an unresolved ranking are
  tested.
- One instrumented executable, staged once on the `profiling` profile,
  reported beside `03daafed`, `21192726`, and `b436a843`; the recipe and
  profile distance from ADR 586 stated; the phase-0 sample and the
  five-pair off median and range reported against 13,128.5 ms and 172.2 ms
  as a cross-profile comparison.
- Perturbation gate result stated; a rejected profile reported as rejected.
- Per-pair tables of every band with the new terms, counters, growth
  events with capacities and micros, the window threads, sample counts and
  periods per class, the estimated coverage ratio and unexplained
  discrepancy per summary, and the residuals; medians and spreads per arm.
- ADR 586's five semi-write medians and ADR 585's thirteen re-reported
  beside their prior values with the cross-profile consistency statement.
- The owner rule applied over the three exact terms with `range_off` and
  every median shown; one owner, an unresolved tie, or below-floor; the
  anti insert's terms and classes reported beside it.
- The owning term priced as gross removable work pairwise, with what a
  replacement would still cost named; the successor candidate named from
  the counts and classes in one sentence, with the net-improvement
  requirement stated; the cache path's sampled share reported as at most
  `shared + other`, a classification bound and not a bound on its actual
  cost, never as a class, unless the results show the instruction ranges
  that separate it.
- Results cite the source tree id, executable hash, recipe, profile,
  dataset manifest, `perf` version, sampling command, and artifact
  directory; the arc adds no stamp-only commit.


## Guardrails

- `b436a843`, `21192726`, and `03daafed` are measured by nothing under this
  record.
- Attribution runs never touch the sealed board directory or its scratch.
- The phase-0 sample enters no statistic; a tripped phase 0 ends the run.
- `semi_write_insert` and `anti_write_insert` keep their clocks and
  boundaries; the new terms accumulate inside them.
- The off arm executes the uninstrumented monomorphization under no
  profiler; no counter, window, or sample is taken there.
- No per-row or per-key-change clock. No code shaping for attribution.
- Sampled classes are CPU-time shares inside ledger windows on the
  window's own thread; the owner rule reads wall terms only. Nothing is
  scaled to make samples sum to a wall clock, and no discrepancy between
  the two is attributed to a cause this record did not measure.
- Ambiguous work is `shared` and unresolved samples are `other`; no class
  is inferred from a line both paths execute, and no count subtracts time.
- Sampled shares carry no significance claim and authorize no mechanism
  on their own.
- No mechanism opens on a containing term or on a sampled class alone; a
  removable subterm's pairwise median must clear the floor, and a cut must
  show net improvement paired with value parity.
- The local deployment is stopped for the run and restarted after it.
  Partition parallelism stays 1 for every child.


## Risks

- The counters, windows, and sampler together may perturb more than ADR
  586's 0.47%; the smoke test suggests a few percent at most; the gate
  decides.
- Timer-based sampling skids: a sample of a stalled load tends to land on
  the instruction after it, which can be the next line and can be a line
  of another class at a class boundary. The classes are line ranges so
  that most skid stays inside a class, but nothing guarantees it; a sample
  that resolves to a boundary line the results cannot place is `other`;
  the rest is a stated limit of line-level attribution, not corrected for.
- The estimated coverage may fall well below one inside growth windows,
  where the first touch of the grown arrays page-faults; the record reports
  the discrepancy there and names the thread CPU-time account as the
  instrument that would explain it, without asserting the cause.
- The runtime may migrate the query between worker threads across pairs
  or, in principle, inside a summary; per-window TIDs make that visible
  and the receipt reports it rather than assuming one thread.
- The `profiling` profile may run measurably slower or faster than
  `release`; the paired design absorbs it, and the cross-profile distance
  is reported, never netted.
- Growth may own outright with a hint-shaped successor, leaving the probe
  question to a later record; or the row loop may own with growth below the
  floor, closing the hint candidate on this evidence.
- The anti summary's filter changes its row mix (3.8 M of 6.0 M rows at
  SF=1), so its counts and classes will differ from the semi's; both are
  reported and neither is inferred from the other.


## Forward

If `insert_grow` owns and clears the floor as gross removable work, the
successor is a reservation from a row-count or NDV hint, which must price
the replacement allocation and its first touch, show net execution
improvement in paired runs, and carry value parity and peak-memory
accounting. If `insert_rows_excluding_grow` owns,
the successor is the cut the counts and classes select: removing the second
walk on a miss if `lookup_found` is near zero and `hash_and_probe`
dominates, or an order-aware build if the source order makes the table's
positive lookups unnecessary during the build, either measured for net
execution improvement in paired runs beside value parity. The repartition
candidate from ADR 586 stays open at 636.0 ms gross with net unpriced, and
the same order question governs it: a build that no longer depends on the
cache would also change what the Exchange is worth. `cleanup` at 573.1 ms
and `runtime_prep_inner_1` at 1,711.3 ms stay open as their own candidates.
The sampler-in-windows method, once it has produced one accepted profile,
is reusable for the next in-drive owner without a new instrument.
