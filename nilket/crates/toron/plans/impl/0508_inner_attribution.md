# impl 508 step 2 — inner attribution: the bridge and the first stage vocabulary

ADR 508 step 2 extends attribution one layer beneath the plan node, into the fused streaming handler.
This receipt covers the two pieces that had to exist before any handler could be decomposed — the
**bridge** that ties a handler profile to the node it claims to explain, and the **stage contract**
that a vocabulary reconciles through — and the first vocabulary built on them, `scan_grouped`.

## The chain, and where each link fails closed

ADR 508 requires the full chain to close:

```
inner stages ⊆ handler-exclusive ⊆ matched invocation exclusive ⊆ execute_bound span ⊆ execute_ms
```

Each link is reconciled separately and each fails closed on its own axis.

| link | reconciles | failure |
|---|---|---|
| refinements ⊆ parent stage | `attributed ≤ parent_micros` | `refinement_exceeds_parent_stage`, invocation fails |
| stages ⊆ handler-exclusive | `attributed ≤ handler_micros` | `stages_exceed_handler_span`, invocation fails |
| handler ⊆ invocation exclusive | `checked_sub` both axes | `HandlerAttribution::Failed`, no share published |
| invocation ⊆ node kind total | per-cell sum on time and spill | invocation named uncovered |
| root span ⊆ `execute_ms` | outer boundary, 10 ms | perf row carries the failure |

**The handler link is exclusive-vs-exclusive, not span-vs-span.** A handler span measured wall-to-wall
would contain its nested children, and those children are already attributed to their own nodes; the
stage residual would then be silently negative-or-clamped. Stage tokens subtract the children that ran
inside them (`children_micros_at_open`), so what remains is the handler's own work and the residual is
dispatch, not double-counted subtree.

**Residual is published, never absorbed.** Every link emits its residual as a number with a declared
boundary. Observed dispatch residual between invocation-exclusive and handler-exclusive is 1–6 µs —
materially negligible, but nonzero, and reported rather than rounded to zero.

## Stage recording is bound to an invocation identity, not to order

A stage token carries the `invocation_id` of the frame it opened on, assigned at frame-open and stable
regardless of completion order. `open_stage` refuses if the executing frame is not the one that
declared the vocabulary; `close_stage` re-verifies before accumulating. A stage that closes after its
invocation has already exited is recorded as an **orphan** and fails that invocation, rather than
attaching to whichever frame happens to be on top.

The caller cannot supply a duration. Earlier drafts let a caller pass elapsed micros, which meant a
stage that had accidentally included a child's time could still fit inside a large residual and pass —
the arithmetic cannot detect a contamination it was handed. Tokens remove the capability: the only way
to record a stage is to open and close one on a live frame.

## The `scan_grouped` vocabulary

Read the handler's actual lifecycle before naming stages. `try_streaming_scan_grouped_aggregate`
pulls batches from a scan stream, optionally applies scan-input ops, optionally repartitions through
an exchange, folds each batch into the grouped table, and finalizes. That is what the vocabulary
names — two levels:

**Outer phases** (reconcile to handler-exclusive): `stream_pull`, `projection`, `repartition`,
`fold`, `finalize`.

**Fold refinements** (reconcile to `fold`'s own micros, not to the handler — so a part and its whole
are never summed together): `group_expr_eval`, `arg_eval_and_readers`, `row_lookup_update`.

### Applicability is declared, not assumed

A required-stage list that names a phase the query does not have would make absence look like missing
coverage; a list that omits a phase the query does have would let real work hide in the residual.
The vocabulary is therefore selected at declare time from two observable facts — whether the scan
input has ops, and whether an exchange is active and unbypassed:

| variant | required stages |
|---|---|
| `scan_grouped_generic` | stream_pull, projection, fold, finalize |
| `scan_grouped_generic_no_projection` | stream_pull, fold, finalize |
| `scan_grouped_generic_exchange` | stream_pull, projection, repartition, fold, finalize |
| `scan_grouped_generic_exchange_no_projection` | stream_pull, repartition, fold, finalize |

Exchange-active is a **separate vocabulary**, not a flag on the generic one: its repartition work is a
required stage there, so it cannot be absorbed into residual, and the non-exchange variants assert the
bypass rather than assuming it.

The vocabulary is declared **after** the primitive admissions decline. A cell that takes a primitive
fast path is therefore recorded as having no stage attribution — an explicit missing-attribution
verdict — rather than borrowing a lifecycle it did not execute. Primitive fast paths stay out of scope
for this step.

### Residual boundary

`500_000` µs, disclosed on every profile. The justification is **~2.5× the largest observed
unexplained residual (202 ms)**. It is *not* "below the smallest named phase" — q17's `finalize` is
468 ms, so that rationale would have been wrong.

## Measurement resolution: an explicit floor, not a migration to nanoseconds

Stage durations are whole microseconds (`Instant::elapsed().as_micros()` truncates). A per-batch stage
costing well under a microsecond therefore records **zero on nearly every call**. Two fold refinements
do exactly this: `group_expr_eval` and `arg_eval_and_readers` accumulate 0–31 µs across 5,859 calls,
which is the timer's floor showing through, not a measurement of their cost.

Zero is published as *below resolution*, never as free. Each stage carries:

- `calls` — how many times it opened and closed,
- `resolution_micros: 1`,
- `below_resolution: true` when a stage observed calls but measured exactly zero,
- `quantization_dominated: true` when a stage averaged under one tick per call,
- `quantization_loss_upper_bound_micros` — `calls`. Each call loses strictly under one tick to
  truncation, so this bounds the **loss**, not the total: a stage's true cost is bounded by
  `micros + calls`. For an exactly-zero stage the two coincide.

**Two flags, because the zero test alone is not enough.** The live profiles show stages that are not
exactly zero and are still pure quantization noise: q20's `arg_eval_and_readers` recorded **5 µs
across 5,859 calls**, and q17's `group_expr_eval` **31 µs across 5,859 calls**. A reader consulting
only `below_resolution` would treat those as measured. `quantization_dominated` (`micros < calls`)
covers both the exact-zero case and the few-calls-crossed-a-tick case.

**The contract was not migrated to nanoseconds, because the residual already bounds the loss tightly
enough to answer the question that matters.** The unmeasured work cannot exceed the refinement
residual:

| cell | fold | refinement residual | share of fold | `row_lookup_update` if the entire residual belonged to the two zero stages |
|---|---:|---:|---:|---:|
| q20 inv 18 | 2,762.7 ms | 11.4 ms | 0.41% | ≥ 99.59% |
| q17 inv 4 | 10,452.3 ms | 7.0 ms | 0.07% | ≥ 99.93% |

Assigning **all** of the residual to group-expression and argument evaluation still leaves
`row_lookup_update` at more than 99.5% of fold. The zero-valued stages are not proof of zero cost, but
they are proof of not-the-target: no sharpening of their timer can move the ranking. Nanosecond
timing would answer a question nobody is asking.

**Banked finding:** *batch-level expression and setup work in the grouped fold is below the
microsecond resolution floor and collectively bounded by the refinement residual — under 0.41% of fold
on q20 and under 0.07% on q17. The compute-front premise is inside `row_lookup_update`.*

That is where the unresolved split now lives: key materialization, hash-table lookup/insertion, and
aggregate-state update are still one number. The next grounding is the existing generic grouped-fold
profile and its probe/growth counters; finer or sampled instrumentation is justified only if those
counters cannot separate the lookup side from the state-update side.

## Measured lifecycle, strict sf=8

Strict, sf=8, parquet, single measured run per arm, profiler on. Build identity
`b2e64dd602e2-dirty` (the WIP this receipt seals; the clean board is step 3's, from a pushed commit).

**q20 inv 18 — `scan_grouped_generic`**, handler-exclusive 5,645.3 ms. All verdicts pass.

| stage | ms | % handler | calls |
|---|---:|---:|---:|
| stream_pull | 885.9 | 15.7% | 5,860 |
| projection | 142.1 | 2.5% | 5,859 |
| **fold** | **2,762.7** | **48.9%** | 5,859 |
| finalize | 1,670.1 | 29.6% | 1 |
| *residual* | *184.5* | *3.3%* | — |

| fold refinement | ms | % fold |
|---|---:|---:|
| group_expr_eval | 0 (below resolution, < 5.9 ms) | — |
| arg_eval_and_readers | 0.005 (quantization-dominated, < 5.9 ms) | — |
| **row_lookup_update** | **2,751.4** | **99.59%** |
| *residual* | *11.4* | *0.41%* |

**q17 inv 4 — `scan_grouped_generic_no_projection`**, handler-exclusive 11,372.1 ms. All verdicts
pass. The applicability variant fired on its own: q17's scan input has no ops, so `projection` is not
a required stage there and its absence is not counted as missing coverage.

| stage | ms | % handler | calls |
|---|---:|---:|---:|
| stream_pull | 396.8 | 3.5% | 5,860 |
| **fold** | **10,452.3** | **91.9%** | 5,859 |
| finalize | 481.5 | 4.2% | 1 |
| *residual* | *41.5* | *0.4%* | — |

| fold refinement | ms | % fold |
|---|---:|---:|
| group_expr_eval | 0.031 (quantization-dominated, < 5.9 ms) | — |
| arg_eval_and_readers | 0.004 (quantization-dominated, < 5.9 ms) | — |
| **row_lookup_update** | **10,445.2** | **99.93%** |
| *residual* | *7.0* | *0.07%* |

**q08's `reoriented_inner_chain_grouped` (7,576.4 ms exclusive) emits no vocabulary** and is published
as an explicit missing-attribution verdict — `passed: false`, no shares — rather than as a covered
invocation. Same for q17 inv 1 (`correlated_scalar_left_outer_global`, 45,096.2 ms). Both are step 2's
remaining instrumentation.

**Cell wall times in this receipt are not comparable to the ADR 507 board.** These runs clear the join
spill scratch before every arm and run under a different memory ceiling; q17 in particular measures
57.8 s here against 35.7 s there. That does not affect the pricing below, where both arms are treated
identically within a rep, and none of these numbers is a baseline.

## Pricing the instrument

The stage shares above come from a profiled run, so the profiler's own cost had to be measured before
they could be banked. **Same binary, two arms** — profiler env unset vs set — three reps per cell,
arms interleaved with the leading arm alternating by rep so drift cannot land systematically on one
side. `q08` is the control: its handler has no stage vocabulary, so it prices the node layer alone.

| cell | rep 1 | rep 2 | rep 3 | paired mean | sd (pp) | paired mean (ms) |
|---|---:|---:|---:|---:|---:|---:|
| q20 (node + tokens) | −5.24% | +5.84% | +8.71% | **+3.10%** | 7.37 | +344 |
| q08 (node only) | +0.33% | −2.41% | −0.17% | **−0.75%** | 1.46 | −61 |
| q17 (node + tokens) | −2.17% | +0.99% | −0.00% | **−0.40%** | 1.62 | −235 |

**The pair does not resolve an overhead.** Every cell's standard deviation exceeds its mean (both in
percentage points, as quoted), the sign
disagrees across cells and across reps within a cell, and two of the three point estimates are
negative. q20's +3.10% carries a 95% interval of roughly [−15%, +21%] at n=3. Reporting +3.10% as
"the profiler costs 3%" would be reading noise as a measurement; the honest statement is that the
instrument's wall-clock cost is **below what three reps can distinguish** at 2–10% run-to-run spread.

**A tighter bound comes from where the cost is charged, not from the wall clock.** Token bookkeeping
is architecturally outside the measured span: `open_stage_inner` starts the clock *after* the ledger
lock and spill read, and `close_stage` reads elapsed *before* taking the lock. Per-call token cost is
therefore charged to the enclosing scope — it lands in the residual, never in a stage's own share. So
the refinement residual bounds the **sum** of unmeasured work and token overhead: 11.4 ms across
**17,577 open/close pairs** on q20 — 5,859 batches x 3 refinements, i.e. 35,154 individual endpoint
operations — which is ≤ **646 ns per pair** (≤ 323 ns per endpoint) if it were all token cost. q17's
is 7.0 ms on the same call count. Whichever way that sum splits, `row_lookup_update` stays above 99.5%
of fold.

**Why the shares are bankable — and in which direction they are biased.** Bookkeeping sits outside a
refinement's own numerator but *inside* its parent's span, so it inflates `fold` while leaving
`row_lookup_update` untouched. That does not cancel: it **depresses** the measured share. The bias is
one-directional and bounded by the refinement residual, so the measured 99.59% and 99.93% are
**conservative lower bounds** on `row_lookup_update`'s true share of fold — the correction can only
move them up. What the noisy pair genuinely fails to bound is second-order perturbation — cache and
TLB effects of the instrument — which no on/off pair at this spread can resolve. That is stated as a
limit, not resolved.

## Grounding `row_lookup_update` on the existing grouped-fold profile

Before adding any finer timer, the pre-existing `GenericGroupedFoldProfile`
(`TORON_GENERIC_GROUPED_FOLD_PROFILE_DIR`) was run on both compute-front cells. It already carries
per-phase probe counters, per-growth-epoch counters, and residency — no new instrument was needed to
ask the question.

| | q20 | q17 |
|---|---:|---:|
| input rows | 7,280,281 | 47,989,007 |
| groups | 4,352,465 | 1,600,000 |
| occupied / vacant lookups | 2.93 M / **4.35 M (59.8% vacant)** | **46.39 M (96.7% occupied)** / 1.60 M |
| bucket visits | 29,502,268 (**4.05/row**) | 68,561,914 (**1.43/row**) |
| — primary_occupied | 3.89 M (13%) | **60.53 M (88%)** |
| — primary_vacant | 7.91 M (27%) | 2.79 M (4%) |
| — vacant_placement | 7.91 M (27%) | 2.79 M (4%) |
| — **rehash** | **9.79 M (33%)** | 2.45 M (3.6%) |
| collision mismatches | 9.48 M | 16.53 M (23.4% of occupied equalities) |
| max probe depth | 55 | 38 |
| growths completed | 14 | 12 |
| peak resident | **1.78 GB** | 441 MB |
| aggregate updates | 7,280,281 (1/row) | 47,989,007 (1/row) |

**The two cells do not share a premise.** q20 is *insertion-dominated*: 60% of its rows create a new
group, the vacant path costs two bucket scans (probe then placement) and rehash alone is a third of
all bucket visits, against a 1.78 GB resident table. q17 is *hit-path-dominated*: 96.7% of lookups
find an existing entry in 1.43 visits, rehash is negligible at 3.6%, and the cost concentrates in
60.5 M equality invocations carrying a 23.4% collision-mismatch rate. A remedy aimed at one is not
aimed at the other.

**Two corrections to what the counters appear to say.** `key_allocations` is not a measurement — it is
assigned `rows`, so it counts key *materializations*, not heap allocations; the key buffer is
allocated once per batch and reused via `clear()`. And `fold_micros` from this run is **inflated by
the counter instrument itself** (a `record_lookup` plus a histogram write per lookup): q17's fold
reads 12.28 s here against 10.45 s under node attribution alone, about +17.5%. The *counts* are exact
and deterministic; the *times* from this run are not comparable to the stage profile.

### What the counters settle, and what they do not

They separate the operation classes decisively **by count**, and they rank the cells. They do not
**price** the split, because every counter is a count and the only time is the whole fold.

A two-point fit against the unperturbed stage-profile fold times is suggestive but cannot settle it:

| | ns/row | bucket visits/row |
|---|---:|---:|
| q20 | 379.5 | 4.052 |
| q17 | 217.8 | 1.429 |

Solving `ns/row = a + b x visits/row` gives **b ≈ 61.6 ns per bucket visit** and **a ≈ 130 ns/row**
fixed — putting lookup+growth at ~66% on q20 and ~40% on q17, with key materialization plus
aggregate-state update in the remainder. **This is two points and two unknowns: zero degrees of
freedom, and one unshared confound** — q20 groups on two columns to q17's one, so its true fixed term
is the larger of the two, which biases `b` upward. It is a plausible range, not a measured share.

**Verdict: the probe/growth evidence cannot separate table lookup and growth from aggregate-state
update in time.** It establishes the classes, the per-cell ranking, and a plausible range; the
successor ADR needs a measured share. The minimal instrument that closes it is a **sampled** per-row
split — bracketing `entry_or_init` against the state-update loop on every Nth row — because
bracketing every row would cost two clock reads per row across 48 M rows and swamp the arithmetic it
measures. That is the fallback the ADR pre-authorized, and it is now justified rather than assumed.

## Splitting `row_lookup_update`: a failed estimator, then external CPU sampling

### The sampled estimator, and why it is rejected and removed

**It is not in the production tree.** A rejected diagnostic that can still be switched on is a
liability: this one emits `separated: true` whenever its three sample-shape rules pass, without the
joint-covariance acceptance this receipt concludes it would need — a fail-open diagnostic. What
survives in the code is the growth census (`TORON_FOLD_GROWTH_CENSUS=1`) and the classifier; the
estimator's design and its failure survive here, as the reason the split is measured from outside.


A stratified sampled estimator was built to the design the composed-gap review specified: deterministic
hash sampling at 1/512 keyed by invocation, batch and row (not a stride — a stride at 512 over
512-row batches draws the same ordinal from every batch, and there is a test for that); strata by
lookup outcome with **exact** populations; growth **censused**, every event timed, and any growth
nested inside a sampled lookup subtracted from that sample by `checked_sub` so the two accountings
stay disjoint. Nanoseconds are local to the estimator; the exact contract keeps its microseconds.

Sample counts landed as predicted — q20 14,092, q17 93,855, smallest stratum 3,070 — and it satisfied
every usability rule that had been declared in advance. It still produced a decomposition summing to
**119.4%** of its parent on q20 and **202.6%** on q17.

**This run is the pilot that exposed the missing gate.** A fourth rule — attributed total within ±10%
of the exact parent — was added *after* seeing the disagreement, so it is a **hard rejection ceiling,
not a positive validity criterion**: passing it would not make an estimate trustworthy. If this
estimator is ever reused, positive acceptance additionally requires the exact parent to be compatible
with a **joint** interval for the component total, including the covariance between components
measured on the same row — which the current per-component intervals do not carry.

**Why it fails is not settled.** Four clock reads per sampled row on the generic path at a calibrated
10.6–10.8 ns each is ~43 ns, far below the ~240 ns/row overshoot, so direct clock cost is excluded.
Lost memory-level parallelism is the leading explanation — a random probe into a 441 MB (q17) or
1.78 GB (q20) table overlaps across rows, and bracketing serialises it — but the measurement does not
distinguish that from compiler barriers around the clock reads, or from cache effects of the sampling
branch itself. It is a plausible mechanism, not a proved one.

### Growth: censused, but not unaffected

Growth never entered the sample, but it was measured *inside a loop that row sampling was perturbing*.
A census-only run (growth timed, row sampling and its per-lookup bookkeeping disabled) rebases it:

| | row loop, census-only | row loop, sampled | growth ms | **growth share of the row loop** |
|---|---:|---:|---:|---:|
| q20 | 2,867.1 ms | 3,280.3 ms (+14.4%) | 714.1 vs 693.1 | **24.9%** (not 21.1%) |
| q17 | 10,129.5 ms | 11,221.1 ms (+10.8%) | 90.6 vs 91.6 | **0.9%** |

The absolute growth cost barely moves; the *share* does, because row sampling inflated the
denominator by 10.8–14.4%. **q20 spends a quarter of its row loop rehashing** — banked from the
census-only run.

### External CPU sampling

Throughput is preserved by measuring from outside: `perf record -F 999 -e cpu-clock` on the
symbol-rich profiling build, estimator and histogram profiler both off. Five runs per cell,
**zero lost samples in all ten**.

**perf's own cost was priced on the same profiling binary**, recorded against not-recorded, arms
interleaved, three pairs per cell. On `execute_ms` the paired means are −0.29% (q20, sd 1.67 pp) and
−0.69% (q17, sd 2.06 pp) — noise, as before. On the quantity the shares are
divided by — the `row_lookup_update` stage itself — the paired means are **+0.19%** (q20, sd 0.52 pp)
and **+0.18%** (q17, sd 0.87 pp). Those are **point estimates, not upper bounds**: at n = 3 their
approximate 95% intervals are **q20 [−1.1%, +1.5%]** and **q17 [−2.0%, +2.3%]**. The correct
statement is that **pricing resolved no material perturbation of the stage** — not that perf costs
under 0.2%.

Classification is **period-weighted, not sample-counted**. `perf record -F 999` is adaptive — it
retunes the period to hold the rate — so samples are not equal slices of time. Each sample's `period`
is its CPU nanoseconds; classes are summed by period and divided by the exact wall-time
`row_lookup_update`. What is left over is genuine **shortfall**: off-CPU time inside the stage plus
work the symbol table cannot attribute. Period-weighting reproduced the sample-count ranking closely
(q20 lookup 79.66% against 79.7%; q17 49.12% against 49.1%), which is a check on the method rather
than a licence to have skipped it.

The classifier is committed as `scripts/classify_fold_perf.py`, with the symbol rules, the
period-weighting, and the rule-of-three bound in one place; the table below is reproducible from the
recorded `.data` files and their sibling node-attribution profiles.

Classification is by leaf **(symbol, source line)**. Neither dwarf nor LBR unwinding puts
`fold_bound_grouped_aggregate_batch` on a stack — it is inlined into its async caller — but the leaf
symbol is exactly what disambiguates lines shared between phases: `GroupValue`'s derive line resolves
under `find_bucket_in` (its `Hash`/`Eq`, the probe) and separately under `quicksort` (its `Ord`, which
is finalize, not the row loop). The denominator is **not** the classified total: each class is the
sum of its samples' CPU periods, divided by the exact wall-time `row_lookup_update` stage. Whatever
the symbol table cannot attribute — and any off-CPU time inside the stage — appears as a bounded
shortfall instead of vanishing.

| share of the exact row loop | q20 | q17 |
|---|---:|---:|
| **lookup + growth** | **79.66%** (78.66–80.30) | **49.12%** (47.80–50.64) |
| **state update** | **2.95%** (2.70–3.42) | **41.73%** (40.34–42.44) |
| key preparation | **0 classified samples** (95% UB 0.021%) | 0.01% (0.00–0.04) |
| fold-body unattributed | 6.75% (6.22–7.16) | 7.70% (7.35–8.09) |
| shortfall | 10.64% (10.02–11.43) | 1.44% (1.21–1.71) |
| row-loop samples, 5 runs | 13,075 | 50,247 |

Key preparation on q20 is **zero classified samples, not a measured zero**: with none observed, three
events is the 95% upper limit, which at that run's mean period is 0.021% of the row loop. q17 observed
seven samples across five runs, so it carries a share rather than a bound.

**The two cells do not share a premise inside the row loop either**, and each is stated at the
strength the evidence carries:

- **q20 — lookup and growth is at least ~80% and decisively dominates; state update is at least 3%.**
  Both are lower bounds: the 6.75% fold-body remainder and the 10.64% shortfall are unassigned, and
  either could belong to either class.
- **q17 — lookup and state update are both first-class costs, and their ordering is unresolved.** The
  observed lead is 49.12% against 41.73%, a gap of 7.4 points, which the 7.70% fold-body remainder
  could erase entirely if it belonged to the update side. That lead is not bankable as a ranking.
- **q20's ~55% non-growth lookup is an approximate cross-run subtraction** — the perf lookup share
  minus the census growth share — and the two are measured not only in different runs but on
  **different binaries**: the census on the release build (row loop 2,831–2,855 ms), the perf
  classification on the profiling build (2,895–2,938 ms). The profiling runs **read about 2–3%
  higher**; nothing here attributes that to the binary, which was not the variable under test. It is
  a sketch of where the 80% sits, not a reconciled decomposition of it.

Key preparation is bounded near zero by an instrument that never touched the loop, independently
confirming what the exact stage could only report as below its resolution floor.

Two honest limits. `fold_body_unattributed` (~7–8%) is core-library glue inlined into the loop body —
comparison and iterator lines — that the line table cannot assign to a phase. And q20's 10.6%
unclassified is *plausibly* the allocator: allocator symbols are 12.4% of that whole run against 6.0%
on q17, and q20 performs 4.35 M insertions each allocating a per-group `Vec<AggState>`. Without call
graphs that attribution is unproven, and it is reported as a shortfall rather than assigned.

### What this does and does not establish about the pointer chase

The state table is `Vec<Vec<AggState>>`: on a hit, the probe finds the entry and the update then
chases a pointer into a separately allocated buffer — a second dependent random access. That is
structural evidence from the code, and q17's measured 41.7% state-update share is consistent with it.
The rejected estimator establishes nothing here; its state-update numbers were part of the same
invalid decomposition. Whether flattening that structure is the remedy is a **mechanism question and
belongs to the successor ADR**, not to this attribution cut.

## `reoriented_inner_chain_grouped`: the existing profiler was failing the query

The handler map recorded this handler as having no inner attribution. It actually has a nine-leaf
`GroupedChainProfile` — RuntimePrepBuild, BaseAccumulation, ReservoirPartitionSpillWrite, BaseConcat,
ReservoirReadProbe, SuffixJoin, GroupedFold, Finalization, Cleanup — so the absence needed explaining,
and the explanation was an instrumentation-correctness bug rather than missing coverage.

| q08, profile env | status | digest | node-attribution route |
|---|---|---|---|
| off | pass | `ArrowIpcDigest([28,117,148,208,…` | `reoriented_inner_chain_grouped` |
| on (before the fix) | **fail** | **none** | `uncovered_generic_fold` |

**The profiler did not make the handler decline — it turned a valid admission into an error.** Two
fences did it, both asserting the generic grouped-reservoir lifecycle as though it were a correctness
invariant of execution:

1. An unconsumed runtime-prep timer raised `Kernel { "admitted chain did not commit the generic
   grouped reservoir path" }`. q08 commits through an earlier branch, so it never reaches
   `try_streaming_reoriented_inner_chain_generic_base_grouped` and the timer stays active.
2. With that removed, `persist` then failed on `required(... "task spill at runtime-preparation end")`
   — the same class of assertion one layer down, because every required field belongs to the
   generic-reservoir path.

`uncovered_generic_fold` was a misreading on my part: it is node attribution's label for an
**unstamped** invocation — the handler errored before `record_route` — not evidence that a generic
fallback ran. And the wrapper's exit code was 0 while the row carried `status: fail`, which is why the
first reading of this looked like an environment-sensitive planner decision. It is not: routing is
identical with the profile on and off.

**Fix — fail-open for execution, fail-closed for attribution.** An unsupported path publishes an
explicit receipt naming the vocabulary that does not describe it, and the handler then **takes the
profiler out entirely**. Marking a flag was not enough: the handler still goes on to record stages,
open finalization and cleanup timers, record final group rows, `finish_root` and `persist`, every one
of which asserts the generic grouped-reservoir lifecycle and could still fail a passing query.
Dropping the profile is what makes that unreachable, rather than trusting each site to check.

The receipt write is **best-effort by construction** — it returns nothing and swallows its own I/O
failure. If a diagnostic never decides whether the query succeeds, the write that *reports*
inapplicability cannot be the thing that fails it either.

And nothing timed is published: the receipt carries `timings_published: false` and no snapshot, so an
inapplicable vocabulary leaves no payload a reader could mistake for a valid decomposition.

Two tests pin it: one drives the whole caller-shaped tail — final group rows, finalization and cleanup
timers, `finish_root`, `persist` — with not one required field ever recorded, and asserts no stage
snapshot is emitted alongside the receipt; the other points the profile directory at a regular file so
the receipt cannot be written, and asserts the query still does not fail.

**Acceptance, profile on against off** — `status` pass in both arms, **byte-identical digest**,
identical output schema fingerprint, identical route (`reoriented_inner_chain_grouped` both ways),
`execute_ms` 7,966 vs 7,988. Only after that
does any timing from this profiler become usable.

**The nine-leaf vocabulary is generic-reservoir-specific and is not reused for this cell.** q08
commits before that path, so its stage names describe a lifecycle q08 does not execute.

**Which branch q08 commits through is now a durable routing fact, stamped by the instrument itself:
`scan_probe_base`** — carried in the receipt as `committed_branch`, alongside
`inapplicable_vocabulary: generic_grouped_reservoir`. It borrows no lifecycle names and times nothing.
Reading that branch's actual lifecycle is the next step, before any vocabulary is named for it.

### Lifecycle grounding for the `scan_probe_base` branch (64 MiB task budget)

Both cells commit through `scan_probe_base` (stamped by the receipt), one aggregate invocation each,
and profile on/off preserves status, digest and route on both.

| | q08 | q09 |
|---|---|---|
| structural chain stages | 6 | 4 |
| aggregate exclusive | 7,668.1 ms | 14,326.2 ms |
| aggregate exclusive spill | 0 | **83.2 MB** |
| nested children (exclusive) | scan 144.4 ms/7, exchange 153.1 ms/7, filter 45.9 ms/2 | scan 140.0 ms/5, exchange 472.7 ms/5, **no filter** |

**The existing `phases.build_micros` / `probe_micros` counters are corroboration, not stages.**
`build_micros` times only `build_in_memory_join_rows` — it excludes the left child's execution,
concat, key extraction, sizing and key-plan construction. `probe_micros` is the opposite error: one
envelope beginning **after the scan stream has been opened**, covering pulls, filtering, base probe,
materialization, repartition, every suffix join and the grouped fold — precisely the split being asked
for. Both accumulate
task-wide with `saturating_add` and carry no invocation identity, so ingesting either would reintroduce
caller-supplied durations and unchecked accumulation into a contract just hardened against both. They
are used as cross-checks: a new base-hash-build stage should approximately reproduce the build
increment, and the **loop** stages plus their explicit residual should reconcile to the probe envelope.
`scan_open` sits outside that envelope — the stream is already open when `probe_micros` starts — so it
must be excluded from the comparison rather than counted into it.

**Two corrections to the lifecycle map.** Grouped-fold growth does **not** spill — it reallocates and
rehashes resident vectors under a memory guard — so the earlier claim that the fold's growth is a
spill writer was wrong. q09's bytes can instead arise during suffix-runtime preparation before
`scan_probe_base` commits, or lazily while probing a partitioned or spilled runtime; runtime
preparation therefore belongs inside the measured lifecycle, and aggregate-level spill equality does
not localize the bytes to the probe loop. And **"spill reachable" is not a vocabulary selector**: one
Flow domain should legitimately read zero on q08 and 83.2 MB on q09. Conditional required stages
follow structural facts — filters, runtime variants, present replay stages — never whether bytes
happened to be written. Chain length likewise becomes replay-ID-bound repeated stages, not a separate
vocabulary, unless it changes the lifecycle itself.

**Spill gate.** q09's 83.2 MB is byte-identical with the profile on and off (87,201,312 both arms), so
it is not a profiling artifact **at this coordinate**. The later coordinate audit found that these
runs used the 64 MiB per-task / 512 MiB reserve defaults, not the board's 2 GiB / 2 GiB ladder
budgets. It therefore does not contradict the zero-spill board row and does not establish tree
movement; the exact-coordinate correction is recorded below.

The original response was to leave q09 front-unassigned until the clean board. The exact-coordinate
re-grounding later restores zero spill and assigns it to the join-probe premise, never the spill front.

### The stage-contract refactor `scan_probe_base` requires first

Two gaps block wiring this branch, and they want the same refactor rather than one retrofitting the
other.

**Replay identity in the key.** `HandlerStage` merges on `(name, parent)` and coverage asks only
whether a name appeared, so one `suffix_probe` record would satisfy a six-replay chain with five
replays invisible. The key becomes `(name, parent, replay_id)`, requirements are stated per replay
identity, and profiles keep per-replay records even where totals are also aggregated.

**The identity must flow through refinements too.** A replay-scoped key on top-level stages alone is
not enough: refinement reconciliation looks its parent up by name, so children from several suffix
replays would all reconcile against the first matching parent. Parent lookup and refinement
reconciliation must both be replay-scoped, or the arithmetic silently mixes replays.

**Declaration order.** Runtime preparation begins before `scan_probe_base` wins, but the current API
declares one fixed vocabulary. The shape is two-phase:

```
predeclare(variants, expected_replay_ids) -> handle
   ... record runtime preparation and every other stage ...
commit(handle, observed_variant)
```

**`expected_replay_ids` is captured at predeclaration, not at commit.** Supplying them at commit would
select the coverage requirement *after* runtime preparation had already been measured, which is the
precise property the split exists to preserve. Commit may only choose among definitions already fixed
before any measurement.

**Requirements become an owned-or-static enum.** Chain length is runtime data — 6 for q08, 4 for q09,
read from the extracted chain — so a `&'static [&'static str]` requirement list cannot express it. An
enum keeps the existing static path allocation-free for the four `scan_grouped` variants while
allowing runtime-derived replay requirements.

Acceptance for the refactor:

- six expected replay IDs with one observed fails, naming the five exact missing IDs;
- all expected IDs observed out of completion order passes;
- an unknown, missing, or conflicting variant commit fails closed;
- an attempted branch that returns `None` is never committed;
- repeated stages merge only within the same replay ID;
- refinements reconcile against the matching replay-scoped parent;
- Flow spill still sums across every replay instance;
- the existing static `scan_grouped` declarations keep their current behaviour;
- dynamic requirement construction allocates nothing when attribution is disabled.

**The contract is landed, all nine green** (`node_attribution.rs` unit tests, names one-to-one with
the list above; the missing-replay and refinement criteria are discriminating — under the prior
name-keyed lookup both would pass). The observed lifecycle commits through
`predeclare(variants) → commit(handle, observed_variant)`: every definition a commit may select,
including its replay-scoped requirements, is fixed inside the predeclare builder before any stage is
measured, and commit only chooses among them — the property the two-phase split exists to preserve.
Stages recorded while a declaration is pending are buffered and spliced in at commit; an attempt that
declines publishes nothing. Requirements are the owned-or-static enum: the four `scan_grouped`
variants keep their allocation-free `&'static` lists and bare-string reporting, and the builder never
runs when attribution is disabled.

Only with that contract in place does `scan_probe_base` instrumentation go in — runtime preparation,
base preparation and hash build, scan open, scan pull, optional filters, base probe and
materialization, repartition, replay-ID-bound suffix probe and filter, grouped fold, finalization,
cleanup — under one Flow spill domain, letting the measurement say whether q09's bytes are preparation
or suffix probing. The generic-reservoir leaves stay explicitly inapplicable.

## The `scan_probe_base` lifecycle, measured

The contract above is wired into the branch. Strict, sf=8, parquet, one measured run per arm, release
build `e70eaec6edf5-dirty`, join-spill scratch cleared before every arm.

**Acceptance, profile on against off** — status `pass` in both arms on both cells, **byte-identical
digest**, identical output schema fingerprint, identical route. `execute_ms` 7,952.9 vs 7,889.2 (q08)
and 14,619.5 vs 14,588.7 (q09). As with every prior on/off pair in this receipt, a single pair at
2–10% run-to-run spread **resolves no overhead**; it is not evidence that the instrument costs 0.8%.

**The full chain closes on both cells for the first time.** Every link passes on its own axis: stages
⊆ handler-exclusive (residual 130.0 ms of 7,604.7 on q08, 113.2 ms of 14,010.7 on q09, both inside the
declared 500 ms boundary); handler ⊆ invocation exclusive (bridge reconciled); invocation ⊆ node-kind
total (one invocation, equal to the `aggregate` total exactly); root span ⊆ `execute_ms` (outer
residual 228 µs and 191 µs against a 10 ms boundary); in-tree residual **0**; and zero interleaved
frames, bridge mismatches or stage mismatches. Coverage is complete: every replay identity the chain
extracted reported its `suffix_probe`.

| stage | q08 ms | % handler | q09 ms | % handler |
|---|---:|---:|---:|---:|
| runtime_prep | 429.5 | 5.6% | **2,592.8** | **18.5%** |
| base_prep | 0.1 | 0.0% | 0.1 | 0.0% |
| base_hash_build | 1.0 | 0.0% | 1.0 | 0.0% |
| scan_open | 1.7 | 0.0% | 1.7 | 0.0% |
| scan_pull | 709.8 | 9.3% | 805.4 | 5.7% |
| base_probe | 777.7 | 10.2% | 801.3 | 5.7% |
| base_materialize | 342.8 | 4.5% | 387.9 | 2.8% |
| repartition | 780.4 | 10.3% | 876.2 | 6.3% |
| **suffix_probe** (all replays) | **4,031.7** | **53.0%** | **6,954.8** | **49.6%** |
| suffix_filter (all replays) | 306.7 | 4.0% | 539.1 | 3.8% |
| fold | 50.0 | 0.7% | 250.9 | 1.8% |
| finalize | 0.0 | 0.0% | 0.0 | 0.0% |
| cleanup | 43.5 | 0.6% | 686.5 | 4.9% |
| *residual* | *130.0* | *1.7%* | *113.2* | *0.8%* |

Per replay, the chain is front-loaded rather than evenly spread — q08's replay 1 is 3,773.6 ms of its
4,031.7 ms suffix total (93.6%), the remaining five replays 36.9–74.9 ms each; q09's replay 1 is
3,972.0 ms of 6,954.8 (57.1%), then 1,769.7 / 1,072.0 / 141.1. Replay-scoped keys are what make that
visible: merged on name alone it would have been one `suffix_probe` number.

### At 64 MiB, q09's 83.2 MB is runtime preparation, not suffix probing

**All 87,201,312 bytes are on `runtime_prep`. Every probe stage reads zero.** This is enforced rather
than observed: the vocabulary declares `Flow`, so the stage spills are reconciled against the
handler's flow delta at boundary zero — they sum exactly or the invocation fails.

That settles the question this cut was built to answer, and it settles it against the reading the
aggregate-level figure invited. The bytes are written while the suffix runtimes are being built,
before `scan_probe_base` commits — which is why they were measurable only after the two-phase split
let runtime preparation be recorded under a declaration that had not yet chosen its lifecycle.

**One limit, stated because it is the obvious misreading.** `counters.spill_bytes` accumulates at page
*write* time. "Every probe stage reads zero" therefore means those stages **wrote** nothing — not that
they performed no spill I/O. Reading a spilled runtime back costs time that lands inside
`suffix_probe`, and this counter cannot see it. Localizing the *write* is not localizing the *cost*.

### The compute-front label pointed at the wrong operator

q08 and q09 entered the board as compute-front cells because the `aggregate` node carries 96% and 95%
of their time. Inside that node, **the grouped fold is 0.7% and 1.8%**. The time is the join chain
fused into the aggregate handler: suffix probing alone is ~50% on both.

**The probe-pipeline subtotal — base probe, materialization, repartition and suffix probe — is 78.0%
(q08) and 64.4% (q09). That is a lower bound on join work, not a measure of it**, because two further
stages are unequivocally join-associated: `runtime_prep` builds the suffix join runtimes, and
`suffix_filter` applies each stage's post-filter. Counting those, join work is **87.7% (q08) and 86.7%
(q09)** — and the gap between the cells nearly closes. The 78-vs-64 spread was an artifact of which
stages the subtotal happened to include: q09 spends 18.5% of its handler in runtime preparation
against q08's 5.6%, so excluding it charged q09 for a difference in where its join work sits rather
than in how much of it there is. What is left over on both cells is small and non-join: `scan_pull`
(9.3% / 5.7%), `cleanup` (0.6% / 4.9%), the fold, and the sub-0.05% base setup stages.

So "the aggregate operator dominates" was true and "the aggregate fold dominates" does not follow.
Whatever remedy these two cells motivate is a **join-probe** remedy, and it is a different mechanism
from the `row_lookup_update` finding that the same phrase names on q17 and q20. That is a correction
to this receipt's own framing, not to the ADR 507 attribution that produced it.

This localizes the low-budget bytes; it does not promote q09 to the spill front. The later 2 GiB
re-grounding records zero spill and keeps the join-probe shape.

**Not yet cross-checked against the ledger.** The `probe_micros` envelope comparison the section above
specifies — loop stages plus residual against the ledger envelope, with `scan_open` excluded — needs
the task-telemetry phases, which the perf row does not carry. The loop stages that would compose it
sum to 6,999.0 ms (q08) and 10,615.4 ms (q09); the comparison itself is still owed.

## Lifecycle grounding for `correlated_scalar_left_outer_global`

Read-only grounding, before any stage is named. The handler
(`try_streaming_correlated_scalar_left_outer_global_aggregate`) recognizes
`Filter(LeftOuter(Exchange(join0), scalar))`, builds an in-memory hash of the scalar side, drives the
preserved side through `try_visit_streaming_join0_chunks` accumulating repartitioned batches, then
probes the recombine and folds a **global** aggregate — no group table, so the fold here is
`fold_global_aggregate_state`, not the grouped fold this receipt decomposes elsewhere.

### Spill writers: nine leaf routes, and the dispatcher is two levels deep

**`try_visit_streaming_join0_chunks` is itself a dispatcher, and so is the helper it calls first.**
Two earlier drafts of this section undercounted — the first listed three routes and **omitted the one
q17 actually uses**, because it grepped a preselected list of helper names instead of enumerating
every call; the second still missed the estimate-over-cap fallback. The count is now derived from the
success returns themselves rather than from a search for writers:

| leaf route | writer | cadence |
|---|---|---|
| `try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks` | `group_spill_bytes` | **per completed group** |
| `try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks` | `group_spill_bytes` | **per completed group** |
| `try_visit_partitioned_streamed_inner_materialized_build_probe_chunks` | `group_spill_bytes` | **per completed group** |
| `try_create_streamed_spilled_inner_scan_build` (2 sites in the scan-probe helper) | `spilled.spill_bytes()` | one whole-build increment |
| `try_create_streamed_spilled_inner_build` (join0 fallback) | `spilled.spill_bytes()` | one whole-build increment |
| estimate-over-cap → `visit_spilled_hash_join_filtered_chunks` | `spilled.spill_bytes()` | one whole-build increment |
| scan-probe in-memory tail, dispatcher in-memory tail | none | two zero-spill leaves |

`try_visit_streaming_inner_join_scan_probe_chunks` calls the two partitioned scan-probe helpers
*before* either `try_create_*` fallback, which is where the first omission came from.

**q17's actual route is `partitioned_streaming_scan_build`** — one of the per-group ones — carrying
all 618,187,648 B at replay 0, read from `join_spill_observations` in its own artifact. The route this
handler's vocabulary most needs to describe was the one missing from the first draft.

### A declined route cannot mutate the counter — verified, not assumed

The "exactly one writer fires" premise would break if a route could write bytes and *then* decline,
leaving its increment on the counter while a later route commits. **It cannot, at any of the six
increment sites.** Every increment sits inside a block that reaches `return Ok(Some(...))` before any
`return Ok(None)` becomes reachable: 5124→commits 5181 (next decline 5186), 5254→5311 (5319),
8127→8187 (8249), and the three per-group routes have no decline anywhere after their first increment.
So handler-exclusive spill is attributable to the one committed route, and a stage bracketing the
drive captures exactly that route's bytes.

### Counter cadence is route-specific, and no route is page-level

An earlier draft claimed a blanket "one jump after the whole build". That holds only for the two
monolithic `try_create_*` fallbacks. **The three partitioned routes increment once per completed
partition group**, inside their `for group in 0..group_count` loop.

What is true of every route is the negative: **no route moves the task counter page by page.** The
consequence for the vocabulary is therefore weaker than the earlier draft claimed but still binding —
a sub-stage drawn *inside a group* reads a structural zero and must not publish one, while a
**group-enclosing stage can honestly attribute that group's bytes**. On a partitioned route, per-group
spill attribution is available; on a monolithic route it is not. That difference is itself a reason
the variants must be distinguishable at commit rather than collapsed into one dispatcher-level name.

### Commit the winning leaf — and `JoinSpillPath` could not supply it

The vocabulary must commit **the exact winning leaf route, not the enclosing dispatcher** — otherwise
a per-group route and a whole-build route publish under one name with incompatible spill granularity,
and the zero-spill in-memory leaf is indistinguishable from a spilling one that happened to write
nothing.

An earlier draft proposed deriving the variant set from the existing `JoinSpillPath` telemetry. **That
was wrong on three counts**, and checking it is what turned up a leaf this receipt had also missed:

- **Five of the nine leaves never stamp a path at all** — the partitioned materialized scan-probe and
  materialized probe routes, the estimate-over-cap fallback, the dispatcher's spilled-build fallback,
  and its in-memory tail all return success without calling `record_join_spill_observation`.
- **The estimate-over-cap fallback is a ninth leaf and a spilling one**, writing through
  `visit_spilled_hash_join_filtered_chunks`; the "six routes" count above it was still short.
- **`record_join_spill_observation` returns nothing.** It appends to a shared task-telemetry vector, so
  recovering a route means reading state back — incomplete where unstamped, and ambiguous when one
  task holds several observations for the same replay. `EstimateSpillFallback` and `BuildSpillFallback`
  are also emitted by a different join path entirely, so the variants are not even in correspondence.

**The dispatcher now returns the leaf synchronously instead.** `try_visit_streaming_join0_chunks`
returns `Result<Option<Join0ChunkRoute>, ExecutionError>`, with one variant per leaf, and each leaf
returns its own. `Join0ChunkRoute::spill_cadence()` states whether that leaf increments the counter
per completed group, once after the whole build, or never — the property that bounds how finely bytes
may be attributed. The mapping is pinned by test so a new leaf cannot be added without choosing a
cadence. `JoinSpillPath` telemetry is kept as-is; the two answer different questions.

| leaf | cadence |
|---|---|
| `partitioned_scan_build` · `partitioned_materialized_scan_probe` · `partitioned_materialized_probe` | per completed group |
| `spilled_scan_build` · `spilled_build_scan_probe` · `spilled_build` · `estimate_spill_fallback` | whole build |
| `scan_probe_in_memory` · `in_memory` | no spill writer |

**The refactor is behavior-neutral, verified rather than asserted** — it changes live dispatch code, so
q17 and q08 were re-run against their pre-refactor artifacts: identical digest, identical output
schema fingerprint, identical row count, identical plan fingerprint, identical spill bytes
(618,187,648 and 0), and identical `join_spill_observations` including q17's
`partitioned_streaming_scan_build`. `execute_ms` moved 54,857.2 → 53,941.0 and 7,952.9 → 7,683.9,
which is single-run variation at this spread and is not read as a speedup.

No stage is wired yet, by design: the route value exists first so the vocabulary has something honest
to commit against.

### Write is not read, and only write is counted

`counters.spill_bytes` accumulates at page-write time. The read-back path —
`visit_streamed_spilled_inner_join_chunks` and its filtered twin, which stream the spilled partitions
back during probing — **never touches the counter**. Its cost is real and lands in probe time. So for
this handler, as for q09, a Flow domain that closes tells us where bytes were *written* and is silent
on where the spill *cost* was paid. Both must be reported, or the profile invites the same misreading
this receipt already had to correct once.

### Nested children and the fold

The handler drives exactly two nested `execute_bound` subtrees: the scalar side (`right`) directly,
and the preserved side inside `try_visit_streaming_join0_chunks`. Both are ordinary child frames, so
the stage tokens subtract them and their spill is attributed to those child nodes rather than to this
handler — which is what makes handler-exclusive spill the right whole for the Flow domain.
`probe_streaming_left_outer_recombine` and the global fold contain no spill writer at all.

### Measured: the two invocations are nested, not siblings

One profiled run, strict sf=8, build `e70eaec6edf5-dirty`, status `pass`, `execute_ms` 54,857.2.

| | invocation id | route | exclusive | exclusive spill |
|---|---:|---|---:|---:|
| outer | **1** | `correlated_scalar_left_outer_global` | 44,524.2 ms | **618,187,648 B** |
| inner | **4** | `scan_grouped` | 10,298.2 ms | 0 |

**The `scan_grouped` invocation runs *inside* the correlated-scalar one.** Ids are assigned at frame
open and records land in completion order, so a record at position 0 carrying id 4 while id 1 sits at
position 1 means id 1 opened first and closed last. That is the nesting, read straight off the
identity discipline the step-1 contract exists to provide — and it is not visible in a per-kind total,
which is why the step-1 receipt could only say "q17 inv 1, 69%".

The inference depends on frames not having interleaved, since concurrent children would make
open/close order say nothing about containment. The artifact reports **`interleaved_frames: 0`** (with
`bridge_mismatches: 0` and `stage_mismatches: 0`), so the LIFO reading holds.

The inner invocation is the `part_avg` grouped aggregate — q17's correlated subquery is
`SELECT l_partkey, AVG(l_quantity) ... GROUP BY l_partkey`, and the handler executes it as the scalar
side (`execute_bound(right)`) before driving the preserved side. Two consequences for the vocabulary:
the scalar-preparation stage will **bracket a 10.3-second nested child**, so its recorded exclusive
value will be a small fraction of its wall span — correct, but the kind of number that reads as a bug
if the nesting is not documented; and the whole 44.5 s the outer invocation carries is already net of
it, so there is no double count between the two rows above.

Every byte of the query's spill is on the outer invocation (root delta 618,187,648, exchange and the
remaining nodes zero), and its bridge reconciles on both axes. The step-1 figure of "623 MB" is
superseded here by **618,187,648 B measured on this tree**; the two are different trees, not a
discrepancy to explain.

Its stage verdict today is `stage_attribution_missing`, `passed: false` — a routed handler that owes
attribution it has not provided. That is the honest red this cut is meant to clear, and it is already
being reported without anyone having to remember to look.

### Measured: the correlated-scalar vocabulary, wired and closing

Strict sf=8, build `cb6a8a1b`-descended working tree, status `pass`, digest identical to every
pre-wiring run. **Both invocations pass** — `in_tree_residual_micros: 0`, outer residual 238 µs against
a 10 ms boundary, zero interleaved frames, bridge and stage mismatches zero, and a 3 µs dispatch
residual. q08 and q09 already closed their chains under `scan_probe_base`; what is new here is that
this is the first **multi-invocation** cell where *every* invocation passes, so the two rows are
jointly complete rather than one covered and one uncovered.

**Committed variant: `correlated_scalar_partitioned_scan_build`.** This is the acceptance that
mattered — the vocabulary commits the leaf `try_visit_streaming_join0_chunks` *returned*, and it
agrees with the `JoinSpillObservation` path `partitioned_streaming_scan_build` **independently**,
having never read telemetry to get there.

Handler-exclusive 45,133.2 ms. Flow closes exactly: stage spill 618,187,648 against handler spill
618,187,648, `spill_error: None`, boundary zero.

| stage | ms | % handler | calls |
|---|---:|---:|---:|
| **`join0_build`** (spill 618,187,648) | **38,892.9** | **86.2%** | 16 |
| `join0_partition_plan` | 3,517.4 | 7.8% | 1 |
| `join0_probe` | 1,330.6 | 2.9% | 16 |
| `join0_group_cleanup` | 247.8 | 0.5% | 16 |
| `scalar_hash_build` | 42.8 | 0.1% | 1 |
| `join0_group_open` | 22.8 | 0.1% | 16 |
| `recombine_filter` | 5.2 | 0.0% | 1,519 |
| `recombine_probe` | 4.0 | 0.0% | 1,519 |
| `scalar_prep` | 3.1 | 0.0% | 1 |
| `repartition_accum` | 2.9 | 0.0% | 1,519 |
| `global_fold` · `finalize` | 0.0 | 0.0% | 1,436 · 1 |
| *residual* | *1,063.8* | *2.36%* | — |

**The spill front is a partitioned-build front — an envelope, not a writer.** `join0_build` is 86.2%
of the handler and owns all 618,187,648 spill-write bytes, and `join0_probe` is 2.9%. Everything after
the join0 drive — recombine probe, filter, global fold, finalize — is under 16 ms combined against a
45-second handler.

**What that does not establish.** `join0_build` brackets the whole build envelope, not the write:
`write_partitioned_streaming_scan_inner_build_batches` scans and partitions the build input and
serializes pages, then each partition is `finish()`ed, its write scratch released, and its index
**rebuilt from pages**. Scanning, partitioning, serialization, finish work and index reconstruction
are all still combined inside that 86.2%. Owning the bytes proves *write ownership*; it does not prove
the time is spent writing. `join0_probe` is symmetrically wide — the full probe-side lifecycle, not
only reads — so "read-back is 2.9%" would also be too narrow a reading of it.

**The bankable premise is therefore: the `partitioned_scan_build` route is build-envelope dominated at
86.2%, against a 2.9% probe envelope.** Separating scan, partition, serialize, write, finish and index
rebuild is a successor's job, and it has to happen *before* a mechanism is chosen — which keeps this
ADR inside its measured-premise, no-remedy-selection scope.

**`scalar_prep` reads 3.1 ms and that is correct.** It brackets a nested `execute_bound` that is the
`scan_grouped` invocation measured at 10,322.0 ms in the same profile; the token subtracts the child,
so the stage reports the handler's own work in that span. The nesting is what makes the two rows
additive rather than double-counted.

**Partition planning is 7.8% in a single call** — `choose_streaming_scan_inner_build_partitioning`,
3,517.4 ms before any build work starts. It was inside the residual until this cut named it.

### The first boundary was wrong, and failing loudly is what found the stages

The vocabulary initially declared the 500 ms residual boundary the other two vocabularies use, and
**overran it at 4,886.2 ms — `passed: false` on an invocation whose arithmetic and spill both
reconciled**. Coverage failed alone, which is the split doing its job: the numbers added up and still
did not explain the handler.

Tracing that 4.9 s named three unattributed regions, now stages: `join0_partition_plan`,
`join0_group_open`, `join0_group_cleanup` — required **only** on per-group routes, keyed off
`spill_cadence() == PerGroup`, so a monolithic route is not charged with stages it structurally cannot
have. They recovered 3,821.6 ms of the gap.

The boundary was then **re-derived rather than widened to fit**: 3 s, ~2.8x the largest observed
unexplained remainder (1,061 ms), the same rule the `scan_grouped` boundary uses. The remainder is
**unattributed, not explained** — diffuse per-group and per-batch scaffolding over 16 groups and ~1,500
batch visits — and it reproduced at 1,063.8 ms on the confirming run, so it is stable rather than
noise. Being an absolute figure, it is a proportionally weaker check on a smaller query using this
handler; that is stated in the declaration itself.

**The boundary is grounded for `PartitionedScanBuild` only.** Every variant currently declares the
same 3 s, but it was derived from one route on one cell. For the other eight it is an inherited
guess — the same status the 500 ms figure had here before it failed — so those variants are
**ungrounded as well as incomplete**, and the first cell to commit one should expect to re-derive it.

### Only one of nine variants is exercised

q17 commits `partitioned_scan_build`. **The other eight variants are declared but unexercised**, and
their `join0_build`/`join0_probe` stages are not placed in the monolithic routes or the two remaining
partitioned ones. A query committing one of those will publish an honest incomplete-coverage red
rather than a wrong number. That is the intended failure mode — the variants were declared complete
from the start rather than narrowed to the single route that could be tested — but it means this
vocabulary is verified for one leaf, not nine.

### What the vocabulary needs, and what is still owed

The lifecycle to declare: scalar-side preparation and hash build; preserved-side drive (with the
route-specific build and its spill increments, per group or whole-build by route); the repartition
accumulation; the recombine probe; the recombine filter; the global fold; finalization. Variants are
selected by **which leaf route commits**, taken from the `Join0ChunkRoute` the dispatcher now returns
and including both zero-spill leaves — predeclared before the preserved side runs, committed once a
leaf wins, exactly as `scan_probe_base` does.

**Declare it complete and from `Flow`.** A route whose bytes do not close fails the invocation
honestly and names the gap — but that failure is a **trigger to investigate, not a justification for
`Subset`**. The failure says the declared lifecycle is incomplete; the usual cause is a missing stage
or a mis-drawn boundary, and the fix is to name the writer. `Subset` becomes right only when a
*deliberately excluded, differently scoped writer* has been positively identified, and the declaration
then has to name where that wider-domain remainder is accounted. Reaching for it on a red is exactly
how a permissive domain gets substituted for a measurement that was trying to tell the truth.

**`Flow` held.** The measurement below closes every byte at boundary zero on the first route wired, so
`Subset` was never reached for, and still has no vocabulary declaring it.

Still owed and not blocking: the `probe_micros` cross-check for `scan_probe_base`, which needs
task-telemetry phases the perf row does not export. It is corroboration, not a gate.

## Provenance and reproduction

Shared by every group: dataset verification `full_hash`; coordinate strict, sf=8, persisted parquet,
8 GiB cgroup ceiling. The rows also record the harness defaults
`memory_per_task = 67,108,864` and `spill_bytes_reserved = 536,870,912`; these are **not** the
2 GiB / 2 GiB ladder budgets. That omission in the original receipt is corrected by the
post-seal coordinate audit below.

**Build identity is per artifact group, not per receipt.** This work spans several WIP commits, so one
blanket identity would let a later group inherit an earlier one — each row below carries the
`build_commit` recorded in its own artifacts.

| artifact group | build identity | contents |
|---|---|---|
| perf classification | `bca8f34f788e-dirty` | `target/tpch/adr508/perf/` — 5 runs per cell, `.data` plus sibling `_node.json` |
| perf pricing pairs | `bca8f34f788e-dirty` | `target/tpch/adr508/perfprice/` — 3 interleaved pairs per cell |
| growth census repetitions | `bca8f34f788e-dirty` | `target/tpch/adr508/census2/` — 3 per cell |
| rejected estimator pilot | `7761021e6f65-dirty` | `target/tpch/adr508/estimator/` |
| grouped-fold counter profile | `b2e64dd602e2-dirty` | `target/tpch/adr508/foldprofile/` |
| scan_probe_base lifecycle | `e70eaec6edf5-dirty` | `target/tpch/adr508/probebase/` — on/off pair per cell, `_node.json` plus rows |
| correlated-scalar grounding | `e70eaec6edf5-dirty` | `target/tpch/adr508/q17probe/` — one profiled q17 run |
| join0 route refactor neutrality | `2258fd8bd6ca-dirty` | `target/tpch/adr508/routecheck/` — q17 and q08 against the two groups above |
| correlated-scalar vocabulary | `cb6a8a1b`-descended | `target/tpch/adr508/corrscalar/` — three q17 runs: first boundary overrun, after naming three stages, confirming |

The `scan_grouped` lifecycle tables earlier in this receipt are the `b2e64dd602e2-dirty` group. Cross-
group time comparisons are therefore cross-binary and are not made; where one was unavoidable (q20's
~55% non-growth lookup, census against perf) the receipt says so at the point of use.

Every group is measured from a working tree, not a sealed commit — these are attribution facts, and
ADR 508 gates the clean 22/22 board on a pushed, unamended commit instead.

Recording, one run per cell:

```
TORON_NODE_ATTRIBUTION_PROFILE_PATH=<out>/<cell>_<run>_node.json \
  perf record -F 999 -e cpu-clock -o <out>/<cell>_<run>.data -- \
  target/profiling/toron-tpch perf-run-one --engine toron --query <cell> \
    --mode strict --sf 8 --data-source parquet --data-dir target/tpch/data
```

Classification, with the line ranges naming the key-preparation and state-update regions of
`fold_bound_grouped_aggregate_batch` at this build:

```
scripts/classify_fold_perf.py q20 2165-2168 2173-2226 target/tpch/adr508/perf/q20_[1-5].data
scripts/classify_fold_perf.py q17 2165-2168 2173-2226 target/tpch/adr508/perf/q17_[1-5].data
```

The ranges are line numbers, so they are only valid for this build identity; a later tree must
re-derive them before re-running the classifier.

## Post-seal coordinate audit and the q21 bridge

The first clean-board attempt exposed two receipt defects before any result was promoted.

**The step-2 inner profiles were not on the ladder memory coordinate.** Their rows record
`memory_per_task = 67,108,864` and `spill_bytes_reserved = 536,870,912`; the sf=8 board is
2,147,483,648 / 2,147,483,648. The earlier conclusion that q09's 83.2 MB spill represented tree
movement was therefore unsupported. A clean, full-hash re-grounding at the ladder coordinate on
`b6e37cf9fe60` records q09 at 13,421.8 ms and zero spill; the clean 22-query board records a
13,349.9 ms median and zero spill. The handler remains join-probe dominated at the corrected
coordinate, but the old 83.2 MB runtime-preparation result is a **64 MiB budget fact**, not a clean
baseline fact. The same audit applies to the fine scan-grouped census and external CPU classifier:
their counts and shares are cross-budget grounding until repeated at 2 GiB, not the final
baseline-coordinate premise.

**q21's existing `ChainStageProfiler` did not close ADR 508's invocation-level chain.** A clean node
profile correctly emitted `stage_attribution_missing`: the separate profiler explained part of the
drive, but had no identity-bound vocabulary connecting it to handler-exclusive time and spill. The
corrective vocabulary is predeclared before runtime preparation, commits either the direct or
partition-at-a-time lifecycle only after admission, and measures every published duration with
`StageToken`; it imports none of the legacy profiler's caller-supplied elapsed values.

The exercised partition-at-a-time route now closes:

- handler 17,392.7 ms; top-level attributed 17,351.9 ms; residual 40.8 ms inside a predeclared
  1,500 ms boundary;
- stage Flow 897,713,384 B against the same handler delta, residual zero;
- runtime preparation 5,755.9 ms; base scan 637.1 ms; replay 0 1,479.2 ms; deferred replay
  9,445.9 ms; finalization plus cleanup 32.9 ms;
- the deferred envelope's token-measured refinements sum to 9,373.2 ms with 72.7 ms residual:
  reservoir buffering 3,987.4 ms, reservoir sealing 325.1 ms, replay 1 read/probe 4,371.9 ms,
  suffix/semi/anti/fold 688.7 ms, and release/accounting below timer resolution. The 873,395,520 B
  carried by the release/accounting stage is where the task counter advances, not a claim that the
  bytes were written in its measured microsecond;
- shared grouped-fold refinements now open only when the committed vocabulary promises a top-level
  `fold` parent,
  preventing a helper from publishing children under another handler's lifecycle.

The validation artifact is working-tree grounding (`b6e37cf9fe60-dirty`, full hash, exact 2 GiB /
2 GiB budgets) under `target/tpch/adr508_q21_stage_wip3/`. The correction subsequently sealed and
pushed at `632777908780f17bf15d1d7d43125b6e28f67d99`. The final clean 22/22 board and five-cell
attribution set were rerun from that immutable identity and are recorded in
`impl/0508_clean_sf8_baseline.md`; they supersede the WIP timing figures above.

## Forward

**All four dominant-handler lifecycles are now bridged** — `scan_grouped`, `scan_probe_base`
(`reoriented_inner_chain_grouped` on both its cells), and `correlated_scalar_left_outer_global`. On
q17 both aggregate invocations pass in one profile: the first multi-invocation cell that is jointly
complete. The fourth is `inner_prefix_semi_anti_grouped`: its pre-existing chain profile remains a
diagnostic, while the new token vocabulary closes the invocation-level contract.

**Three premises are handed to the successor, none of them a remedy.** On the final 2 GiB coordinate:

- q08/q09 are join-probe dominated, with an 87.9%/85.1% join-associated lower bound and only
  0.65%/1.91% in grouped fold;
- q20 and q17's inner `scan_grouped` invocation put 49.0%/91.3% of their handlers in grouped fold,
  and more than 99.5% of each fold in the still-combined row lookup/update loop;
- q17's outer `partitioned_scan_build` route puts 79.4% in `join0_build`, while q21 splits between
  runtime preparation and the deferred reservoir/replay envelope.

Each names *where* the cost is, and each still needs its dominant component split before a mechanism
can be chosen — `join0_build` into scan/partition/serialize/write/finish/index-rebuild,
`suffix_probe` into lookup/materialize/read-back, and the grouped row loop into lookup versus state
update. q09's 83.2 MB runtime-preparation spill remains a **64 MiB-only** fact; the clean board and
attribution run both record zero spill. Selecting remedies is the successor ADR's decision.

What remains on this handler is coverage of the other eight leaf routes: only
`partitioned_scan_build` is exercised, and the monolithic and remaining partitioned routes have no
`join0_build`/`join0_probe` placement yet. Each will report incomplete coverage honestly when first
committed. Finding a query that commits one is the way to close them; guessing at the placements
without a cell to verify against is how the first boundary went wrong.

**Start it from `Flow`.** An earlier draft of this section reserved `SpillCounterKind::Subset` for it
as "the first spilling handler to be decomposed" — that was wrong. q09's low-budget run had already
shown a spilling handler closing under `Flow` with every byte attributed, and the exact-coordinate
q21 bridge does the same at 897,713,384 B. Spilling does not imply subset semantics. `Subset` is
justified only if lifecycle
grounding finds a writer inside the handler span that is *deliberately excluded* from the stage
vocabulary, and the declaration then has to name where that remainder is accounted. Declaring it
pre-emptively would substitute a permissive domain for the one measurement that can fail honestly.

Two things the measurement above hands forward. The `probe_micros` cross-check is owed. And the
join-probe finding needs one more attribution layer before it can name a remedy: `suffix_probe`
brackets a whole runtime's probe of a batch set, so it does not yet separate hash lookup from
materialization from spilled-runtime read-back — the last of which this write-flow counter cannot see
at all. That refinement is replay-scoped by construction (`open_replay_substage` exists and is
deliberately unused), so it does not need another contract change.

Step 3 is complete: the final clean 22/22 sf=8 baseline was measured only after the corrective q21
bridge sealed and pushed, per the ADR's commit-boundary rule.
