# ADR 585 - q21 outside-drive lifecycle decomposition

**Status:** CLOSED. Implemented and measured on source tree
`f761c601735a26eed95bf270b014ed7297875964`; results are recorded in
`plans/impl/0585_q21_outside_drive_lifecycle_decomposition_results.md`. The
accepted profile resolves `runtime_prep_semi_write` as the sole owner and
closes the shared-source candidate below the inherited floor. No engine route
or admission changed.


## Context

ADR 584 closed on the sealed ADR 583 coordinate with one owner for q21:
`outside_drive`, 7,887.0 median-scaled ms, 55.16% of the unprofiled execute
median of 14,298.5 ms, on instrumented executable `a173e825` against sealed
baseline `b436a843`. The chain-stage profiler that produced that result starts
its clock after runtime preparation, so `outside_drive` is an envelope, not a
term: it is everything `execute_ms` holds that the seven drive stages do not.
ADR 584 handed it forward as the enclosing owner, not as a claim that all of
it is removable, and named one candidate mechanism, a shared lineitem-source
pass feeding both membership summaries, while pricing a full filtered lineitem
read at only 629.6 scaled ms in `base_scan`. That price sits just above the
500 ms mechanism floor ADR 580 set and 584 inherited. Whether any part of the
envelope can carry a mechanism is exactly what is unknown.

The envelope has a known shape. q21 runs the inner-prefix semi/anti grouped
aggregate handler, and `execute_ms` brackets the whole `execute_bound` call
around it. Before the drive the handler prepares, in this order and
sequentially: the inner runtimes for replays 1 and 2 (filtered `orders`, then
`nation`, each through the streamed scan build that ADR 584's trace showed
declining local hash-join admission for want of a build-row bound), then the
recognized not-equal existence summary for the semi stage, then the same for
the anti stage. Each summary preparation opens a scan of one lineitem alias,
filters it, evaluates the key program, repartitions, and writes the summary.
The drive function then prepares the base join, replay 0's `supplier` build,
before the chain profiler's clock starts. After the drive come finalization
of the grouped aggregate, cleanup of every runtime, and the plan nodes above
the handler: the sort and limit over 31,809 groups and result collection.

An instrument already brackets this handler, but in the wrong domain for a
lifecycle question. The ADR 507 node-attribution profile, switched on by
`TORON_NODE_ATTRIBUTION_PROFILE_PATH`, runs in-process under `perf-run-one`,
attaches to the perf row, and reconciles the root span to `execute_ms` within
a declared 10 ms outer boundary, failing closed. The handler's declared
vocabulary for the direct-replay route requires eleven stages: `runtime_prep`,
`base_prep`, the seven drive stages, `finalize`, and `cleanup`. Every handler
span and every stage span in that profile is exclusive by design: at close the
profiler subtracts the nested `execute_bound` children the span drove, so the
figure sits in the same domain as a node's exclusive cost. That arithmetic is
ADR 507's contract and is correct for ranking nodes. It is wrong for pricing a
lifecycle phase, because the work a phase exists to do is precisely the
nested scan and build it drives: an exclusive `runtime_prep` excludes the
`orders` scan-build and both lineitem summary scans, and the difference
between the root span and the handler's exclusive span is not "the nodes
above the handler" but every descendant the handler executed. Read as
lifecycle terms, the expensive work would collect in a residual instead of
the phase that owns it. ADR 507 priced the profile's overhead as within noise
on its pilot; ADR 508 could not resolve it on a noisier pair. Neither ran at
the sealed coordinate, so the cost here is unmeasured.

Three further gaps sit between that instrument and an owner. `runtime_prep`
is one bracket around four sequential preparations, so even an inclusive
reading cannot separate the partitioned `orders` build from either summary,
or say what share of a summary is reading its source versus writing the
summary, which is the only split that prices the shared-source candidate.
The node profile is not among the diagnostics that void candidate authority,
so a row measured with it on reads as `diagnostics_disabled = true`. And the
row does not record the profile's path, so a profile is joined to its sample
by environment rather than by an explicit reference.

ADR 514's 5.7 s runtime-build term was measured at other budgets on another
binary and stays historical context, never subtracted from anything here.


## Decision

**An additive inclusive lifecycle receipt, beside ADR 507's exclusive
arithmetic, not instead of it.** The node profile keeps every exclusive
figure it reports today; nothing in its per-node or per-stage accounting
changes. The handler additionally emits, into the same profile file under the
same path, an inclusive lifecycle receipt: wall time from open to close with
nothing subtracted, at each of these boundaries.

```
handler_inclusive_micros
  runtime_prep_inclusive
    runtime_prep_inner_1        replay 1 runtime preparation (filtered orders)
    runtime_prep_inner_2        replay 2 runtime preparation (nation)
    runtime_prep_semi_read      semi summary: scan stream reads of the lineitem alias
    runtime_prep_semi_write     semi summary: filter, key program, repartition, summary write
    runtime_prep_anti_read      anti summary: scan stream reads
    runtime_prep_anti_write     anti summary: filter, key program, repartition, summary write
    runtime_prep_residual
  base_prep_inclusive
  drive_inclusive
  finalize_inclusive
  cleanup_inclusive
  lifecycle_residual
```

Each residual is computed by subtraction at the moment its enclosing span
closes and written into the receipt as its own term, so a reader never
re-derives it and a negative value is an instrument defect the receipt itself
reports. The read/write split inside a summary is measured per batch around
the stream `next` call and the transform-and-write that follows it, and
accumulated per call the way the chain-stage profiler accumulates a stage;
the two summaries share nothing at this cut, so each alias is read once and
charged once.

**One clock, not two.** The handler span and every existing lifecycle stage
already take a single elapsed sample when they close, before the profiler
subtracts children to produce the exclusive figure. The inclusive receipt
reuses that same sample for `handler_inclusive` and for the `runtime_prep`,
`base_prep`, `finalize`, and `cleanup` terms; it does not start a second
timer beside the first, which would introduce slightly different boundaries
and its own perturbation. Only the seven `runtime_prep` children and the
`drive` bracket need new samples, because no existing span closes at those
boundaries.

**Bound to the winning invocation by identity.** A profile can hold several
aggregate invocations, and one handler family can take two of them in the
same query, so route name and completion order cannot say which receipt is
q21's. The receipt is keyed by the frame's stable `invocation_id`, the same
identity ADR 508 uses to match a handler profile to its invocation, and is
published on that invocation's entry in the profile. The driver requires
exactly one receipt whose invocation carries the
`inner_prefix_semi_anti_grouped` route; zero or more than one is an
instrument defect that withholds the owner.

**Timing algebra, three bands, all recorded.** With
`execute_bound_span_micros` from ADR 507's root frame:

```
execute_bound_span_micros + outer_residual_micros         = execute_ms * 1000
handler_inclusive_micros + outside_handler_span_micros    = execute_bound_span_micros
sum(inclusive lifecycle terms) + lifecycle_residual       = handler_inclusive_micros
sum(inclusive runtime_prep children) + runtime_prep_residual = runtime_prep_inclusive
```

`outside_handler_span` is all root-span time outside the winning handler's
inclusive interval. Its constituents are the plan nodes above the handler,
the dispatcher's own work, and every streaming arm that probed and declined
before the winning handler took the node; a losing arm never closes its span,
so its time can live nowhere else. Nothing the winning handler drove is in
it. It is computed once at close and recorded in the receipt like every other
residual. `outer_residual` keeps its 10 ms boundary.

**The instrument contract, three parts, each with a paired test.** First, the
inclusive receipt, with two tests. A discriminating accounting test opens a
lifecycle stage, drives a synthetic nested `execute_bound` child of known
duration inside it, and closes both: the inclusive lifecycle term must retain
the child's time while ADR 507's exclusive figure for the same stage must
have subtracted it. That test fails if the receipt is built from the
existing exclusive substages, which a non-zero check alone would not catch.
A reconciliation test then drives the handler with the profile on and
asserts every term above is present, keyed by the winning `invocation_id`,
that the three sums reconcile exactly, and that the read and write halves of
one summary are both non-zero for a multi-batch source. Second, authority:
`diagnostics_disabled` is false whenever `TORON_NODE_ATTRIBUTION_PROFILE_PATH`
is set, tested at the predicate. Third, the reference: the sample row
records the node profile's path, the driver gives every on-arm child a fresh
unique path and refuses a reuse, and a profile is joined to its sample by
that reference and never by JSONL order, tested as ADR 584 tested the
chain-profile path.

**Executable identities and arms, inherited from ADR 584.** The instrument
contract produces one new executable, built and staged once through
`toron-bench build`; that single hash runs both arms of every pair. The record
reports it beside `a173e825` and `b436a843`. The off arm runs with no
diagnostic set. The on arm runs with the node profile alone; the chain-stage
profiler and the replay-input ledger stay off, because `drive_inclusive`
already brackets the drive and a second clock inside it would only add
perturbation. Five adjacent pairs after one unprofiled warmup, alternating
off-on, on-off, off-on, on-off, off-on. q21 alone, strict SF=8, persisted
Parquet, explicit 2 GiB task memory and 2 GiB spill reserve, partition
parallelism 1, own empty spill scratch, deployment offline. Partition
parallelism 1 is load-bearing: the node ledger is one process-global stack
and cannot separate concurrent workers, and the inclusive receipt inherits
that limit.

**Perturbation gate, as 584 wrote it.** `m_i = (execute_on_i - execute_off_i)
/ execute_off_i`; the profile is rejected when the median of `|m_i|` exceeds
0.25. A rejection is a finding about the node profile's cost at this
coordinate and opens a cadence question, not a reason to read the profile.

**Completeness bounds, stated before the run.** For pair i,
`overhead_i = max(0, execute_on_i - execute_off_i) * 1000`. The outer residual
must satisfy ADR 507's 10 ms boundary. `lifecycle_residual_i` and
`runtime_prep_residual_i` may each be at most `overhead_i + 0.05 *
handler_inclusive_micros_i`. Every residual must be non-negative. The
vocabulary's static 1.5 s exclusive boundary stays as the engine's own check
on the exclusive domain and is not the bound this record reads. A sample
outside any bound is reported, not dropped, and withholds the owner.

**Cross-instrument consistency, reported, never netted.** `drive_inclusive`,
scaled as below, is reported beside ADR 584's `total_drive_micros` medians,
and the sum of every non-drive term beside 584's `outside_drive` 7,887.0 ms.
They come from different runs and different clocks; agreement within the
off-arm range is stated as consistency, and disagreement beyond it is a
finding about one of the two instruments, named.

**Statistic and owner rule, one formula, over the envelope's terms.** The
candidate universe is thirteen terms: the seven `runtime_prep` children
including their residual, `base_prep_inclusive`, `finalize_inclusive`,
`cleanup_inclusive`, `outside_handler_span`, `lifecycle_residual`, and
`outer_residual`. `drive_inclusive` is reported for the consistency check but
is not a candidate; ADR 584 already ranked its stages. For term t and pair i,
`scaled_t_i = term_t_on_i / (execute_on_i * 1000) * execute_off_i`, the
statistic is the median over five pairs, `range_off = max(execute_off) -
min(execute_off)`, and a term is a candidate owner only if
`median(scaled_t) > max(500 ms, 2 * range_off)`. Ties within `range_off` are
reported unresolved. If a residual term is the largest, the finding is an
incomplete instrument, not an owner.

**The 500 ms mechanism floor is applied to the subterm a mechanism would
remove, priced pairwise.** For the shared-source candidate that subterm is the
read half of the alias a shared pass would no longer read. For each pair i,
`removable_i = min(semi_read_i, anti_read_i)`, scaled exactly as any term is
scaled in that pair; the statistic is the median of `removable_i` over the
five pairs. A minimum of two medians is not this quantity and is not used. A
fusion cut may open only if that median clears `max(500 ms, 2 * range_off)`.
If it does not, the candidate is closed here as below the floor and the
record says so. The same rule binds any other mechanism a term suggests: the
record names the removable subterm, its pairwise-scaled median, and whether
it clears.

**What this record does not do.** It does not change the partitioned build,
the summary writer, the sort, or any route. It does not alter ADR 507's
exclusive accounting or its consumers. It does not run the timing atlas or
the distributed coordinate. It does not re-measure with `b436a843` or
`a173e825`.


## Acceptance

- The three-part instrument contract lands first with its paired tests: a
  synthetic nested child is retained by the inclusive lifecycle term and
  subtracted by the exclusive one; the receipt is keyed by the winning
  `invocation_id`, reuses the close-time elapsed sample for every existing
  span, and reconciles exactly at all three sums with non-zero read and
  write halves on a multi-batch summary; the node profile voids candidate
  authority; the sample row records a unique node-profile path per on-arm
  child and the driver refuses a reuse.
- The driver requires exactly one `inner_prefix_semi_anti_grouped` receipt
  per on-arm sample and withholds the owner otherwise.
- ADR 507's exclusive figures are unchanged by the cut, shown by its existing
  tests passing untouched.
- One instrumented executable staged once and used for both arms of every
  pair, reported beside `a173e825` and `b436a843`, with the off-arm distance
  to the sealed 14,956.0 ms row stated.
- Five adjacent alternating pairs at the sealed coordinate; the perturbation
  gate applied as written and its result stated; a rejected profile reported
  as rejected.
- A per-pair table of the three bands and every inclusive term with its
  recorded residual, the completeness bounds, and whether each sample met
  them; medians and spreads per arm.
- The cross-instrument consistency of `drive_inclusive` and the non-drive sum
  against ADR 584's medians, stated in words.
- The owner rule applied as written over all thirteen terms with `range_off`
  and every per-term median shown; one owner or an unresolved tie.
- The shared-source candidate priced by the pairwise removable read half
  against the floor, opened or closed in one sentence; any other candidate
  priced the same way.
- Results cite the source tree id, the executable hash, and the artifact
  directory; diagnostic runs are unsealed and say so; the arc adds no
  stamp-only commit.


## Guardrails

- `b436a843` and `a173e825` are measured by nothing under this record.
- Attribution runs never touch the sealed board directory or its scratch.
- No mechanism opens on the strength of the containing term; only the
  removable subterm's pairwise median may clear the floor.
- The exclusive node and stage figures keep their names, arithmetic, and
  consumers; the inclusive receipt is an addition that never feeds the
  exclusive reconciliation.
- The local deployment stays offline. The query is in-process. Partition
  parallelism stays 1 for every child.


## Risks

- The node profile's per-node and per-stage clocks, plus the inclusive
  receipt's own reads, may perturb a 14 s query more than the chain profiler
  did. The gate decides; a rejection names the instrument's cost at this
  coordinate.
- The read/write split inside a summary is measured around a stream boundary
  where Parquet decode may already have happened on a background task; if
  so, the read half undercounts and the shared-source candidate is priced
  conservatively low, which is the safe direction for a floor test.
- The partitioned `orders` build may own the envelope outright. Then the
  successor is a build question, not a source question, and the shared-source
  candidate closes on this record's evidence.
- Five pairs bound the profile's noise but not host drift; the owner rule's
  spread test is what keeps drift from naming an owner.


## Forward

`runtime_prep_semi_write` owns 2,323.1 median-scaled ms, 16.64% of q21's
unprofiled execute median, above the 500 ms floor. It contains filtering, key
evaluation, repartition, and summary insertion; this record does not separate
those mechanisms. The successor therefore decomposes that write boundary
before proposing a change. The anti write and replay-1 build remain material
secondary terms at 1,954.0 ms and 1,810.2 ms. The proposed shared-source pass
closes here: its pairwise removable-read median is only 386.2 ms, below the
floor. SF=10 still waits for a measured mechanism and a fixture.
