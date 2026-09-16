# ADR 594 - q21's replay-1 preparation on strix: lifecycle decomposition of `runtime_prep_inner_1`

**Status:** CLOSED ON VARIABILITY. One run on strix through the gate
(results in `plans/impl/0594_q21_inner_1_preparation_decomposition_results.md`):
off range 799.1 ms, 8.90% of the median, above the 4% cutoff, so no
owner is read. The band reconciled on every pair and established the
facts: q21's replay-1 preparation at SF=8 under 2 GiB takes the spilled
branch (one partition, one sizing pass, 5,845,938 rows), and on the two
steady pairs the term is 1.34 s of which the build write is ~880 ms, the
partition sizing ~266 ms, and the sizing scan ~200 ms. A login-session
teardown (the launcher's, ended by the workstation) fell inside the
window before the slow samples; not attributed. Successor: the same
decomposition launched with no session event in its window. Successor to
ADR 593, which resolved q21's owner on strix to `runtime_prep_inner_1`
and named its decomposition as the next record.


## Context

ADR 593 read `runtime_prep_inner_1` at 1,345.0 median-scaled ms, 14.84%
of a 9,064.7 ms off median, on strix under ADR 591's coordinate; ADR 592
had read it at 1,334.5 and slim's ADR 589 at 1,671.4. Those readings are
historical context for this record, not its control: the instrument
below is a new executable and its own unprofiled arm is the control it
is read against.

What the term contains, at `8ef864c6`. ADR 585 clocks
`runtime_prep_inner_1` around one call in the chain handler
(`streaming.rs`, the reoriented-inner-stage loop): from the instant
before `prepare_reoriented_inner_join_runtime_with_streamed_scan_build`
is entered for `JoinReplayId(1)` to the instant it returns, recorded with
`record_inclusive_lifecycle_micros`. Everything before the loop (the
chain's required-output computation, the projection marking, the
summary-pair recognition) is outside the term and stays in ADR 585's
`runtime_prep_residual`. Inside the call, for a build that decomposes to
a streaming scan input (q21's `orders` scan with its `o_orderstatus`
filter), the path is:

1. **Sizing scan.** `estimate_streaming_scan_inner_build_with_facts`
   opens the build's scan, and for every batch applies the filter
   predicate, projects to the retained columns, and accumulates row
   count and decoded bytes into `CompactBuildSizingFacts`. A full pass
   over the filtered build input, producing an estimate and no rows.
2. **Partition sizing.** `choose_streaming_scan_inner_build_partitioning`
   derives a partition count from the estimate and the budgets, then in
   a loop calls `estimate_streaming_scan_inner_build_partition_bytes`,
   which opens the scan again, filters, hashes every row's build key to
   a partition, and accumulates bytes per partition; if the per-partition
   spill sizing refuses, the count is raised and the pass repeats. One or
   more further full passes, producing counts and no rows.
3. **Then one of two branches.** With `partition_count > 1`, the runtime
   is assembled as `PartitionedScan` with no build loaded
   (`loaded_builds` empty, `loaded_build_attempted` false): the build is
   performed later, during the drive, partition by partition, and that
   time lands in ADR 584's `inner_replay_1` stage, not in this term.
   With `partition_count == 1`, a `SpilledInnerJoinBuild` is created and
   `write_streaming_scan_inner_build_batches` opens the scan a third
   time, filters, and writes the hashed build, then `finish`es it: the
   build itself, inside this term.
4. **Assembly.** Key plan, projection, resident guards, and the runtime
   struct: small, but real.

Which branch q21 takes at SF=8 under 2 GiB budgets, and how many
partition-sizing passes it makes, are not on record; ADR 584's
`inner_replay_1` drive stage at 2,101.4 ms and the 27,242,808 spill
bytes on every q21 row are consistent with either. The instrument below
records the branch and the pass count as facts, so the decomposition
reads against what actually ran and the successor is chosen against a
named path, not a guessed one.

The candidate the shape suggests, stated and not acted on: the sizing
scan and each partition-sizing pass read and filter the whole build
input before any build row is written, so the preparation performs
`1 + partition_sizing_passes` full scans, plus one build-write scan on
the spilled branch; at least two scans on the partitioned branch and at
least three on the spilled one, before the drive reads the input again.
Whether those passes own the term is what this record measures.


## Decision

**A sixth band in the inclusive receipt, additive, beside ADR 585's
three, ADR 586's fourth, and ADR 587's fifth.** Inside the `JoinReplayId(1)`
call, the handler records three inclusive children, each one wall-clock
span from entry to return of the one call it names, taken once per call
and never nested in another child, into the same profile under the same
path, and a residual computed at close:

```
runtime_prep_inner_1                                   (ADR 585's clock, unchanged)
  inner_1_sizing_scan          one span: estimate_streaming_scan_inner_build_with_facts
  inner_1_partition_sizing     one span: choose_streaming_scan_inner_build_partitioning,
                               entered once and clocked once, its retry loop inside the span
  inner_1_build_write          one span on the spilled branch: create, write batches, finish;
                               zero calls on the partitioned branch
  inner_1_residual             runtime_prep_inner_1 − sum of the three, computed at close
```

The partition-sizing clock is unambiguous by construction: it brackets
the single call to `choose_streaming_scan_inner_build_partitioning`,
whose retry loop runs inside that call, so a retry can never be
accumulated twice; the passes are counted separately, one increment per
call of `estimate_streaming_scan_inner_build_partition_bytes`, as the
fact `inner_1_partition_sizing_passes`. Each child is exclusive of the
others by construction, since the three spans are disjoint call sites,
and `inner_1_residual` is everything else inside the term: the
projection and key-plan work, the resident guards, the runtime assembly,
and the instrument's own cost. The residual is computed by subtraction
when the term's span closes and written as its own field; a negative
residual is an instrument defect the receipt reports.

**The instrument is gated on the profiling handle.** ADR 585's parent
clock is unchanged. The child clocks, the pass counter, and the fact
publication run only when the stage handle is present, which is the on
arm; on the off arm the preparation path executes exactly as it does at
`8ef864c6`, with no new reads, no counters, and no fact assembly. A
fixture runs the preparation without a handle and proves the receipt
carries no band and the path's counters are untouched.

Beside the terms, the band records facts, not times, each required when
the band is present: `inner_1_branch` (`partitioned` or `spilled`),
`inner_1_partition_count`, `inner_1_group_count`,
`inner_1_partition_sizing_passes`, `inner_1_estimated_build_bytes`,
`inner_1_sizing_rows`, `inner_1_sizing_scan_calls`,
`inner_1_partition_sizing_calls`, `inner_1_build_write_calls`, and on the
spilled branch `inner_1_build_rows` and `inner_1_build_spill_bytes`. The
band's fields are absent, not zero, on a profile that never entered
replay 1's preparation. If the call takes the non-streaming fallback
(the build does not decompose to a scan input, and the runtime is
prepared from executed build batches), the band records
`inner_1_branch = unsupported` with the parent clock only, and the
driver reports the pair as an unsupported path rather than reading it
through this band.

**Timing algebra, six bands, all recorded.** ADR 585's identities stand
unchanged, and the band adds one:

```
inner_1_sizing_scan + inner_1_partition_sizing + inner_1_build_write + inner_1_residual = runtime_prep_inner_1
```

reconciled on every on row with the same tolerance ADR 585 applies to
its bands.

**Completeness is consistency of the facts, then the bound.** A pair's
band is complete only if every required field is present and the facts
agree with the branch: `inner_1_sizing_scan_calls = 1` and
`inner_1_partition_sizing_calls = 1` with
`inner_1_partition_sizing_passes ≥ 1` on either branch; on the
`partitioned` branch `inner_1_partition_count > 1` and
`inner_1_build_write_calls = 0`; on the `spilled` branch
`inner_1_partition_count = 1`, `inner_1_build_write_calls = 1` with the
write completed (`inner_1_build_rows` and `inner_1_build_spill_bytes`
present). A missing child, a missing fact, or a call count that
contradicts the branch makes the pair incomplete even when the omitted
time would fit inside the residual allowance: the allowance covers what
the children cannot name, not what they failed to record. Then, for
pair i, with `overhead_i = max(0, execute_on_i − execute_off_i) × 1000`
microseconds, the residual may be at most
`overhead_i + 0.05 × runtime_prep_inner_1_i`; above that the children do
not cover the term and the pair is incomplete. A negative residual is an
instrument defect. A profile whose band is absent while
`runtime_prep_inner_1` is non-zero is incomplete; an `unsupported` branch
is reported as such and is not an owner reading. A sample outside any
bound is reported, not dropped.

**Executable, arms, pairs, gates, telemetry, and the driver: inherited
from ADRs 584-593.** One instrumented release executable built on bigfoot
from this tree runs both arms of every pair; the off arm with every
diagnostic disabled, the on arm with the profiler and the ledger enabled;
one unprofiled warmup, five adjacent pairs alternating off-on and on-off;
ADR 584's perturbation gate (median `|m_i| > 0.25` rejects the profile);
ADR 592's binding to ADR 591's sealed manifest by content and its child
validator on every row; ADR 593's host readings on every child, the
journal collected once afterwards and joined by state, the driver run as
a transient user service with lingering enabled, its scope recorded, the
start gate (package at or below 45 °C, load below 0.3, read before any
work of the driver's own), and the variability cutoff
`range_off ≤ 0.04 × median_off`. The polling that watches the run opens
no login session on strix. The driver is `adr594-q21-inner-1`, ADR 586's
lifecycle-pair driver under its own kind and band, with the ADR 593
additions factored into the shared paired-children path so both the ADR
585 and this driver carry them.

**The control is this instrument's own off arm.** The off median is
reported as an observed distance from the sealed q21 median (9,126.9 ms)
and from ADR 593's off median (9,064.7 ms), carrying host and time
variability as well as the instrument's changes; nothing is netted
against ADR 593's 1,345.0 ms, which appears beside the new
`runtime_prep_inner_1` reading as context.

**Statistic and owner rule, over the band's four terms.** For term t and
pair i, `scaled_t_i = term_t_on_i / (execute_on_i × 1000) × execute_off_i`;
the statistic is the median over five pairs; `range_off = max(execute_off)
− min(execute_off)`; a term is a candidate owner only if
`median(scaled_t) > max(500 ms, 2 × range_off)`. The outcomes, each
named in the summary: **resolved** (one candidate leads the next term by
more than `range_off`); **unresolved tie** (the lead is inside
`range_off`); **below floor** (no term clears the floor: the preparation
has no single owner at this floor and the finding is the whole term's
shape); **incomplete** (a residual leads, or a completeness bound fails
on any pair: an instrument finding, not an owner); and **closed on
variability** (the cutoff exceeded before the rule is applied). The rule
is applied once, to the one run, and the outcome is the record's.

**Attribution only.** No change to the sizing passes, the partitioning,
the build writer, the scan, or any route under this record. If the
sizing passes own the term, the successor record prices the removable
subterm pairwise as ADR 585 fixed (the passes a single-pass sizing or a
carried estimate would not read) before any cut opens; if the build
write owns it, ADR 586's decomposition pattern applies to the write; if
the residual leads, the instrument is the finding.


## Acceptance

- The receipt carries the sixth band with its four terms, its facts, and
  its identity reconciled on a fixture profile; a negative residual is
  reported as a defect; an absent band beside a non-zero
  `runtime_prep_inner_1` is reported as incomplete; the partitioned branch
  yields `inner_1_build_write` at zero with zero calls and is accepted;
  the bands of ADRs 585-587 are unchanged (their tests still pass).
- Fact consistency is enforced: fixtures with a missing child, a missing
  fact, `partitioned` with a build-write call, `spilled` with
  `partition_count = 2` or without a completed write, and a sizing-scan
  call count other than one are each reported incomplete with the field
  named, even where the residual bound would have passed; an
  `unsupported` branch is reported as an unsupported path.
- The partition-sizing clock is counted once: a fixture that drives a
  retry (the first partition count refused, the second accepted) yields
  `inner_1_partition_sizing_calls = 1`, `inner_1_partition_sizing_passes = 2`,
  and a partition-sizing span that is not the sum of two overlapping
  spans; the pass count increases with each retry.
- The off arm is untouched: a fixture runs the preparation without a
  profiling handle and proves no band, no new reads, and no counters,
  while ADR 585's parent clock behaves as before.
- The band's spans lie inside the `runtime_prep_inner_1` span on a
  fixture that enters the call and are absent on one that does not; the
  facts name the branch that ran, the partition count, and the number of
  partition-sizing passes.
- The driver refuses a profile that trips the perturbation gate, reports
  a pair whose residual exceeds its bound as incomplete, and returns each
  of the five outcomes on fixtures built to produce them (resolved, tie,
  below floor, incomplete, closed on variability).
- The ADR 593 additions (scope, gate, launch and return stamps, journal
  collection after the last child, variability cutoff) are shared by the
  ADR 585 and ADR 594 drivers through one path, with the ADR 593 tests
  still passing.
- One run on strix, eleven rows, every row admitted by the ADR 592
  validator, the readings present on every row, the journal state and
  per-child coverage reported, the six identities reconciled on every on
  row, the four-term table with the facts and the verdict, and the
  historical readings beside it labelled as such.
- Plan commit, then one implementation/results/closure commit; the
  measured tree named; nothing else under this number.


## Guardrails

- The children are the code boundaries named above at `8ef864c6`; if the
  path has moved by the time the instrument is written, the record is
  re-grounded before the band is named, not after.
- One run; a refused or rejected run is retained and closes the record
  on its finding.
- Build on bigfoot (the build host since mini's outage), measure on
  strix, nothing else on strix in the window; slim runs nothing.
- Readings and journal entries are evidence beside the samples, not
  attribution; the record claims a cause only where they support it.


## Risks

- The term's owner may be the branch, not a child: a partitioned
  preparation whose time is two sizing passes has a different successor
  from a spilled one whose time is the build write. The facts decide
  which is being read, and the results say so before the table.
- The instrument adds four spans and a handful of counters inside a term
  that ADR 593 read at 1.3 s; the perturbation gate covers it.
- The band may reconcile within tolerance and still leave a residual that
  clears the floor; that is the incomplete outcome and the next
  instrument change, not an owner.


## Forward

- The successor is named by the outcome: the sizing passes priced
  pairwise for a single-pass sizing candidate; ADR 586's pattern on the
  build write; or the instrument if the residual leads.
- The band's facts (`branch`, `partition_count`, `passes`) belong on the
  q21 row of every future board on strix as coordinate context, once
  they have shown their worth here.
