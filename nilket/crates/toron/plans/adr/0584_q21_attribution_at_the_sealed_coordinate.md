# ADR 584 - q21 attribution at the sealed coordinate

**Status:** CLOSED. Implemented and measured on source tree
`407488d022bfffa88336f96a083ec4ced76eb12b`; results are recorded in
`plans/impl/0584_q21_attribution_at_the_sealed_coordinate_results.md`. The
accepted profile resolves `outside_drive` as the sole owner and opens its
pre-drive lifecycle decomposition. No engine route or admission changed.


## Context

ADR 583's sealed board names q21 the next owner by absolute gap: 14,956.0 ms
against DuckDB's 2,970.0 ms, a gap of 11,986.0 ms, at strict SF=8, one task,
2 GiB task memory and 2 GiB spill reserve, on the staged executable
`b436a843`. The 521 table had q21 at 12,646.0 ms; that delta is not established
as a regression, because both engines ran slower on this host by about the
board's known drift.

The sealed q21 row carries wall time and three local hash-join admission
verdicts, one admitted at an estimated 9.6 MB build and two declined with no
build row bound, and nothing else. No receipt is declined as not inner, so all
three verdicts belong to inner joins; the semi and anti replays run the
specialized chain path, which never reaches local hash-join admission and never
concatenates input. The public bind entry strips the join target from each
receipt and surfaces only the verdict, so bind order alone cannot say which
inner join declined, and nothing in the sealed evidence says where the 14.96 s
goes.

Two prior records bound what attribution must answer. ADR 514 last grounded q21
as a 5.7 s runtime-build term plus a 9.9 s, 898 MB reservoir spill-replay
envelope, at an earlier coordinate with different budgets; its post-activation
figure is not comparable to the sealed row without re-derivation. ADR 580
closed with a multi-batch input-materialization residual it could not place and
a rule: attribute that residual among the shapes the batch-local route
excludes before widening admission. Its q21 ledger row, 425.1 ms of right-side
multi-batch time, came from another binary on the distributed atlas coordinate.
q21 is one of the places that residual may live. Which of its replays carries
any multi-batch charge is unknown, and this record begins by discovering that
rather than assuming the non-Inner stages own it.

The instrument for the drive exists. The chain-stage profiler records
microseconds, calls, batches, and rows for seven stages, base scan, three inner
replays, the semi replay, the anti replay, and the grouped fold, plus the
unattributed in-drive residual, against `total_drive_micros`. It is switched on
by an environment variable the executor reads, so it runs under the board's own
coordinate. It starts after runtime preparation, so it does not see the whole
of a sample's execute time. It flushes on every base batch; ADR 426 measured
39 to 74 microseconds per snapshot, which projects to roughly 1.8 to 3.5 s over
the SF=8 base batch count, a material perturbation on a 15 s query that must be
gated, not assumed away.

The timing atlas carries the per-node input ledger 554 and 580 named, but it
refuses a single-query run and measures on the distributed coordinate, so it
cannot supply a same-boundary q21 ledger.


## Decision

**Timing algebra, with two residuals.** The profile reconciles to the drive,
not to the sample wall:

```
sum(stages) + in_drive_residual        = total_drive_micros
total_drive_micros + outside_drive_micros = execute_ms * 1000
```

`outside_drive` is runtime preparation and everything else before and after the
drive, and is a named term in its own right; ADR 514's runtime-build term lives
there. The row's `total_ms` and the parent process wall stay as separate outer
boundaries and are reported, never folded in.

**Executable identities.** `b436a843` is the sealed baseline only. The
instrument changes below produce one new executable; it is built and staged
once through `toron-bench build`, and that single hash runs BOTH arms of every
pair. The record reports the sealed identity and the instrumented identity side
by side and reads attribution only from the instrumented one. Its unprofiled
arm is also the control that says how far the instrumented build sits from
the sealed row.

**The two arms, defined completely.** The off arm runs with the chain profiler
and the diagnostic ledger both disabled. The on arm runs with both enabled; the
perturbation gate therefore covers every enabled diagnostic, not the profiler
alone. Pairs are adjacent, five of them after one unprofiled warmup, and their
order alternates, off-on, on-off, off-on, on-off, off-on, so instrumentation is
not confounded with second-run cache or thermal effects. Every on-arm child
receives a fresh, unique receipt path, and the harness records that path in the
sample row, so a profile is paired to its sample by an explicit reference and
never by JSONL order. Recording the path is the first instrument change if the
row lacks it.

**Perturbation gate, executable as written.** For pair i, movement is
`m_i = (execute_on_i - execute_off_i) / execute_off_i`. The profile is rejected
when the median of `|m_i|` exceeds 0.25, and the profiler's flush cadence is
the next instrument change before any attribution is read. Stage shares are
computed on stage-exclusive microseconds; the snapshot cost itself is expected
in the in-drive residual.

**Completeness bounds, stated before the run.** For pair i the overhead
allowance in microseconds is
`overhead_i = max(0, execute_on_i - execute_off_i) * 1000`, and the in-drive
residual of the on sample may be at most
`overhead_i + 0.05 * total_drive_micros_i`; above that the stages do not cover
the drive and that sample's profile is reported incomplete.
`outside_drive_i = execute_on_i * 1000 - total_drive_micros_i` must be
non-negative; a negative value is an instrument defect. A sample outside either
bound is reported, not dropped.

**Statistic and owner rule, one formula.** The candidate universe is the seven
stages, the in-drive residual, and `outside_drive`, nine terms, so runtime
preparation can own the query if it does. For term t and pair i, the scaled
value in milliseconds is
`scaled_t_i = term_t_on_i / (execute_on_i * 1000) * execute_off_i`. The
statistic is the median of `scaled_t_i` over the five pairs. The off-arm
absolute range is `range_off = max(execute_off) - min(execute_off)` in
milliseconds, reported alongside the relative spread `range_off / median_off`.
A term is a candidate owner only if
`median(scaled_t) > max(500 ms, 2 * range_off)`. The 500 ms is ADR 580's
authorization threshold, so a successor opens only for a term that would have
cleared 580's own gate. Ties within `range_off` are reported as unresolved,
not broken.

**Trace-target decline evidence is a third instrument change.** No existing
artifact exposes replay identity with its admission verdict: the public bind
entry discards the optimizer trace target and surfaces only the verdict. The
perf row therefore gains, under the same diagnostic flag, the replay identity
derived from each trace target alongside its verdict, with a paired test that a
stripped receipt is refused by the diagnostic. Each replay is then mapped to
its join, build side, and the planner fact the build-row bound needed. Whether an
estimate would change a route is recorded as a question with the evidence
required, not as a proposal. ADR 513's finding that semi and anti placement is
pinned by the author stands unless this evidence contradicts it, and then the
contradiction is the finding.

**ADR 580's residual: same-boundary diagnostic, historical row as context.**
The 425.1 ms atlas figure is historical, non-arithmetic context: another
binary, another coordinate, never subtracted from or netted against this
profile. A full atlas is not paid for under this record, because it would still
be the other coordinate. Instead the join input-materialization ledger, by side,
shape, and batch histogram, is exposed per replay in the perf sample row under
a diagnostic flag that is off by default and voids candidate authority when on.
That is the second instrument change. With it, the record discovers which
replay, if any, carries multi-batch input time at the board's own boundary.

**Reconcile with ADR 514 by coordinate, or say it cannot be.** If the sealed
coordinate reproduces 514's envelope shape, the term that moved is named; if it
cannot without a budget change, the record says so and stops.

**Name the successor, or the plan-shape question.** The record ends with one
owner term, its share, the mechanism that would reduce it, and the 500 ms
floor, or, if the largest term is parity Toron's plan shape cannot reach, the
plan-shape question in their place.


## Acceptance

- The three instrument changes land first, each with a paired test: the sample
  row records the profile receipt path; the diagnostic flag exposes the
  per-replay input ledger and trace-target-derived admission receipts while
  voiding candidate authority; and the diagnostic refuses a stripped receipt.
- One instrumented executable staged once and used for both arms of every
  pair, reported beside the sealed `b436a843` identity; five adjacent pairs at
  the sealed coordinate in alternating order, with the perturbation gate
  applied as written and its result stated; a rejected profile is reported as
  rejected.
- A seven-stage table with `total_drive_micros`, the in-drive residual, the
  completeness bound and whether each sample met it, `outside_drive`, and
  `execute_ms`, medians and spreads per arm.
- Each of the three verdicts mapped from trace-target-derived evidence to its
  join, its replay identity, and the missing planner fact.
- The per-replay multi-batch input ledger from the same-boundary diagnostic,
  with batch histograms, and a statement of which replay, if any, carries the
  charge; the 580 row cited as context only.
- ADR 514's envelope reproduced with the moved term named, or declared
  non-reproducible without a budget change.
- One successor owner with share, mechanism, and the 500 ms floor, or a
  plan-shape finding in their place; the owner rule applied as written over
  all nine terms, with `range_off` and the per-term medians shown.
- Results cite the source tree id and executable hash; diagnostic runs are
  unsealed and say so; the arc adds no stamp-only commit.


## Guardrails

- `b436a843` is measured by nothing under this record; it is the sealed
  baseline identity the instrumented executable is reported against. Both arms
  of every pair run the one instrumented hash.
- Attribution runs never touch the sealed board directory or its scratch.
- No admission is widened and no route is flipped on the strength of a parent
  total whose distribution has not been shown.
- The local deployment stays offline. The query is in-process.


## Risks

- The perturbation may exceed the gate at SF=8. That is a finding about the
  instrument and opens a cadence change, not a reason to read a perturbed
  profile.
- Five pairs at about 30 s each bound the profile's noise but not host drift.
  The owner rule's spread test is what keeps drift from naming an owner.
- The declines may be routine, an estimate the plan has never had, in which
  case they are a planner fact and not an owner.
- The same-boundary ledger may show no multi-batch charge on q21 at all. Then
  580's residual lives elsewhere and this record says so.


## Forward

`outside_drive` owns 7,887.0 median-scaled ms, 55.16% of q21's unprofiled
execute median. The successor first decomposes the surviving pre-drive
lifecycle: both inner runtime builds, recognized semi and anti preparation,
base preparation, and finalization/cleanup. Replay 1's partitioned build is in
this envelope and may be larger than either membership-source scan. A shared
lineitem-source pass feeding both summary builders is only the first candidate
mechanism; that subterm must independently clear the inherited 500 ms floor
before a fusion cut opens. SF=10 still waits for the measured mechanism and a
fixture.
