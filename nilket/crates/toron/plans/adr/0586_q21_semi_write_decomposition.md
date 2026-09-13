# ADR 586 - q21 semi-summary write decomposition

**Status:** PROPOSED. Successor to ADR 585 at the same sealed ADR 583
coordinate. Decomposes the owning term, `runtime_prep_semi_write`, into its
four activities before any writer or route changes. No engine route,
admission, or writer changes under this record.


## Context

ADR 584 named one owner for q21 at the sealed coordinate: `outside_drive`,
7,887.0 median-scaled ms, 55.16% of execute. That term was the whole
preparation-and-lifecycle envelope, everything `execute_ms` holds that the
seven drive stages do not; it was never a claim about the partitioned inner
builds alone. ADR 585 split the envelope with an additive inclusive lifecycle
receipt and resolved a single owner inside it: `runtime_prep_semi_write`,
2,323.1 median-scaled ms, 16.64% of the 13,958.6 ms unprofiled execute
median, leading `runtime_prep_anti_write` (1,954.0) by 369.1 ms, more than
the 247.4 ms off-arm range. `runtime_prep_inner_1` (1,810.2) and `cleanup`
(586.8) also cleared the 500 ms floor; the shared-source candidate closed
below it (pairwise removable read 386.2 ms) and does not reopen here.

The owning term is a bracket, not a mechanism. In
`write_streaming_recognized_neq_existence_summary` the semi summary's source
is one lineitem alias opened as a scan stream. Each batch the stream yields
is charged to the read half at the `next` boundary; everything after it,
until the next `next`, is the write half. That write half does four things
in sequence, and the sequence is decoded from the plan rather than fixed in
code: `decompose_streaming_existence_plan` walks the existence plan down to
its scan and returns the ops above it, `Filter` when the predicate's
disposition is evaluate-in-Toron, `Project` for the key program, `Exchange`
for the repartition. The write half applies each op to every batch the
previous op produced, then inserts every surviving batch into the not-equal
existence summary row by row through `write_batch`. The summary is an
open-addressed table keyed by an `Int64` order key with a representative
supplier and a multi-key flag. An insert first compares the key against
the last key inserted and, on a match, updates that entry without hashing
or probing; only a key that differs from its predecessor is hashed through
an `ahash::RandomState` hasher and probed in the bucket array. How often
the consecutive-key cache hits therefore depends on the order rows arrive
in, and that order is what the `Exchange` op produces.

Two facts about that loop are visible in the code and are what the
decomposition tests. First, the insert loop discards the partition index the
`Exchange` op produced but consumes its output batches; on this route the
repartition survives as an ordering of rows into the table, and because of
the consecutive-key cache that ordering is not free of consequence for the
insert. Second, `repartition_exchange_batch` has no single-partition short
circuit: it evaluates the key program, routes every row into a bucket, and
takes every column into a fresh batch regardless of the partition count.
Whether that work is the owner, or whether the hash and probe of the inserts
the cache does not absorb is, or the key program's own copy, is exactly what
is unmeasured.
The anti summary's write half is the same function with a different alias
and, presumably, a Toron-evaluated date predicate in front of the key
program; its 1,954.0 ms is the material secondary term and the same
instrument prices it at no extra cost.

The host has not been measured under this coordinate since 2026-09-02. ADR
585's off median was 6.67% faster than the sealed ADR 583 row on the same
recipe, reported as drift and never netted. Eleven days later, a gross shift
(a throttled clock, a background job, a changed page cache) would make the
five pairs' noise floor and every scaled term meaningless, and it is cheaper
to find that out with one query than with twelve.


## Decision

**Phase 0, a host tripwire before the pairs.** The driver runs, on the
staged instrumented executable at the exact sealed coordinate, one unprofiled
warmup and then one unprofiled q21 sample. The sample is compared to ADR
585's unprofiled off median, 13,958.6 ms, passed to the driver explicitly as
the reference. If the sample lies more than 10% from the reference, the
driver stops before the pairs, records the sample under
`adr586_phase = "host_check"` with the tripwire result, and the run is a host
finding: the host is investigated (load, frequency scaling, memory pressure,
dataset verification) and phase 0 is repeated after remediation. Within the
tripwire the driver proceeds; the sample is reported as phase 0 and is used
for nothing else. The threshold is set above ADR 585's 6.67% same-recipe
distance and well above its 1.77% off-arm range on purpose: one sample can
catch a gross shift and cannot establish drift, so the tripwire is coarse
and the pairs are the only basis for the noise floor and the attribution.
The drift statement itself is the five-pair off median and range against
13,958.6 ms and 247.4 ms, reported after the run.

**A fourth band in the inclusive receipt, additive, beside ADR 585's three.**
The write half's clock, its boundary, and its name do not change, so
`runtime_prep_semi_write` under this record is the same quantity ADR 585
measured. Inside it, the loop accumulates four child terms per summary and
the receipt records their residual by checked subtraction:

```
runtime_prep_semi_write
  semi_write_filter        every Filter op pass, evaluate-in-Toron predicates
  semi_write_key_program   every Project op pass, the key program
  semi_write_repartition   every Exchange op pass, key eval + routing + take
  semi_write_insert        write_batch over every surviving batch
  semi_write_residual      batch unwrap, row-limit slice, loop overhead
runtime_prep_anti_write
  anti_write_filter, anti_write_key_program, anti_write_repartition,
  anti_write_insert, anti_write_residual
```

One clock pair per op pass per source batch, and one around the insert loop
per source batch, so each source batch costs `ops + 1` extra pairs rather
than one per partitioned piece. At roughly 23,000 source batches per summary
and three ops that is about 184,000 additional `Instant` reads per summary,
some 370,000 per query. Their cost at this coordinate is not established
and this record does not claim it is sub-millisecond. The child clocks and
their counters are diagnostic-only: they run only when the stage handle is
present, which is only the on arm, so the off arm executes the write loop
exactly as ADR 585's off arm did and the paired measurement prices the
instrument. ADR 585's existing read and write clocks keep their documented
unconditional behaviour and are not touched. The receipt also records each
summary's decoded op list in plan order and, for each child, the number of
passes it accumulated over, so a zero child is read with its count: an op
absent from the list, an op present that no batch reached because an
earlier filter emptied the stream, and a present op whose passes rounded to
zero microseconds are three different facts and the receipt distinguishes
them.

**Timing algebra, four bands, all recorded.** ADR 585's three sums stand
unchanged; this record adds, per summary:

```
sum(write children) + write_residual = runtime_prep_<summary>_write
```

computed at receipt time with `instrument_defect` on underflow, as the other
residuals are. Completeness for pair i: each write residual may be at most
`overhead_i + 0.05 * runtime_prep_<summary>_write_i`, with `overhead_i` as
ADR 585 defined it; every residual non-negative; ADR 585's bounds still
apply to every band above. A sample outside any bound is reported and
withholds the owner.

**Executable, arms, pairs, gate: inherited.** One instrumented executable
built and staged once through `toron-bench build` in release, run for phase 0
and both arms of every pair, reported beside ADR 585's `21192726` and the
sealed `b436a843`; neither of those runs under this record. Off arm with no
diagnostic; on arm with `TORON_NODE_ATTRIBUTION_PROFILE_PATH` alone. One
unprofiled warmup after phase 0, then five adjacent pairs alternating
off-on, on-off, off-on, on-off, off-on. q21 alone, strict SF=8, persisted
Parquet, explicit 2 GiB task memory and 2 GiB spill reserve, partition
parallelism 1, own empty spill scratch, deployment quiescent, full-hash
dataset verification, fresh output directory. Perturbation gate as ADR 584
wrote it: reject the profile when the median of `|m_i|` exceeds 0.25.

**Statistic and owner rule, over the semi write's five terms.** For term t
and pair i, `scaled_t_i = term_t_on_i / (execute_on_i * 1000) *
execute_off_i`; the statistic is the median over five pairs; `range_off =
max(execute_off) - min(execute_off)`; a term is a candidate owner only if
`median(scaled_t) > max(500 ms, 2 * range_off)`; ties within `range_off`
are unresolved; a residual that leads is an incomplete instrument, not an
owner. If no term clears the floor the outcome is below-floor: the write
half's cost is spread across activities none of which alone carries a
mechanism, the record says so, no mechanism opens, and the successor is
named from the secondaries rather than from this boundary. The candidate
universe is the semi write's four children and its residual. The anti write's five terms are scaled and reported the same way
beside the semi's, as the priced secondary, but are not ranked against the
semi's terms and cannot be named owner here. The thirteen ADR 585 terms are
re-reported from this run's receipts as a consistency check against ADR
585's medians, stated in words within the off-arm range or as a named
finding, never netted.

**The mechanism floor is applied to the removable subterm, priced pairwise.**
As ADR 585 wrote it, no cut opens on the strength of a containing term. The
record names, for the owning child, what a mechanism would remove and
whether its pairwise-scaled median clears `max(500 ms, 2 * range_off)`. For
`repartition` the child is priced as gross removable work: the writer does
not use the partition index, so the op's own cost could be skipped, but the
order it imposes feeds the insert's consecutive-key cache, and removing it
may make the insert dearer by an amount this record cannot price. A
repartition cut is therefore a candidate whose successor must measure net
execution improvement at this coordinate, paired off/on with the cut, beside
value parity; the gross child clearing the floor opens the measurement, not
the cut. For `insert` the child is not itself removable and the successor is
a further split (cache hits, hash and probe, growth) before any writer
change. For
`key_program` and `filter` the removable subterm is whatever the successor
can show is redundant with work the scan or the summary already does; this
record does not presume one.

**What this record does not do.** It does not change the summary writer, the
existence-plan decomposition, the repartition kernel, the partitioned inner
build, or any route. It does not decompose `cleanup`, which stays a separate
secondary candidate priced at 586.8 ms with its own successor if the
operator opens one. It does not reopen shared-source fusion. It does not run
the timing atlas or the distributed coordinate.


## Acceptance

- The instrument lands first with paired tests: a synthetic multi-batch
  source through a Filter, Project, and Exchange chain yields four non-zero
  children and a residual that reconcile exactly to the write term; the same
  source with no Filter op yields a zero filter child, an op list that
  says so, and a zero pass count, while a Filter op that empties the stream
  leaves the downstream children at zero with zero passes and the op still
  listed; the receipt computes both write residuals by checked subtraction
  and reports `instrument_defect` on underflow; ADR 585's three bands and
  ADR 507's exclusive figures are unchanged, shown by their tests passing
  untouched.
- The driver applies the phase-0 tripwire against an explicit reference
  before the pairs, refuses to proceed when tripped, and records the sample
  either way; the tripwire arithmetic is tested at both sides of the
  threshold.
- The driver requires one `inner_prefix_semi_anti_grouped` receipt per
  on-arm sample carrying all eight write children, both residuals, and both op
  lists, and withholds the owner otherwise; the five-term owner rule, the
  floor, ties, and residual-owner withholding are tested.
- One instrumented executable, staged once, reported beside `21192726` and
  `b436a843`; the phase-0 sample and the five-pair off median and range
  reported against 13,958.6 ms and 247.4 ms.
- Perturbation gate result stated; a rejected profile reported as rejected.
- A per-pair table of the four bands with every write child, its residual,
  the completeness bounds, and whether each sample met them; medians and
  spreads per arm; the decoded op list and per-child pass counts per
  summary.
- The ADR 585 thirteen-term medians re-reported from this run beside ADR
  585's, with the consistency statement.
- The owner rule applied over the semi write's five terms with `range_off`
  and every median shown; one owner, an unresolved tie, or below-floor
  with no mechanism opened; the driver emits each of the three outcomes
  and is tested on all three; the anti write's five terms reported beside
  it.
- The owning child's removable subterm named and priced pairwise against the
  floor as gross work; for `repartition` the record states that net
  improvement is unpriced here; the successor named in one sentence.
- The off arm runs no child clock and no child counter; the on arm's
  instrument cost is read from the pairs, and the perturbation gate is the
  only claim made about it.
- Results cite the source tree id, executable hash, recipe, dataset manifest,
  and artifact directory; the arc adds no stamp-only commit.


## Guardrails

- `b436a843`, `a173e825`, and `21192726` are measured by nothing under this
  record.
- Attribution runs never touch the sealed board directory or its scratch.
- The phase-0 sample enters no statistic; a tripped phase 0 ends the run.
- `runtime_prep_semi_write` keeps its clock and boundary; the children are
  accumulated inside it, never re-derived from a second timer around it.
- No mechanism opens on a containing term; only a removable subterm's
  pairwise median may clear the floor.
- The local deployment stays quiescent. Partition parallelism stays 1 for
  every child.


## Risks

- The extra clocks may perturb more than ADR 585's 1.16%; the gate decides,
  and a rejection is a finding about instrument cost at this coordinate.
- The semi summary may carry no Toron-evaluated filter, making the
  decomposition three-way; the op list makes that a stated fact rather than
  a surprise.
- Two children may tie within `range_off` (repartition and insert are the
  likely pair); the record then reports both and the successor decomposes
  the larger, not a mechanism.
- Phase 0 may trip on an ordinary bad minute; the remedy is a second phase 0
  after a look at the host, never a widened threshold.
- Host drift since ADR 585 may move the off median by several percent
  without tripping; that is reported as drift and the attribution's scaled
  shares stay comparable to ADR 585's because both are shares of their own
  off arm.


## Forward

If `repartition` owns and its gross median clears the floor, the successor
is a route-local candidate that skips the Exchange op for recognized
existence summaries, measured for net execution improvement in paired
off/on runs at this coordinate with a digest value gate; the consecutive-key
cache is why gross and net can differ. If `insert` owns, the successor
splits it into cache hits, hash and probe, and growth before touching the
writer. If nothing clears the floor, the successor comes from the
secondaries. Either way the anti write's decomposition, reported
here, tells the successor whether the same mechanism prices the 1,954.0 ms
secondary as well. `cleanup` at 586.8 ms and `runtime_prep_inner_1` at
1,810.2 ms stay open as their own candidates. SF=10 still waits for a
measured mechanism and a fixture.
