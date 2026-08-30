# ADR 580 - join input concatenation, measured by side and shape

Pairs with `plans/adr/0580_multi_batch_join_input_consumption.md`. First measurement under
that record. No behaviour changed: this is the instrument reading its own parent.

Artifact `adr580_input_shape_sf8.jsonl`, build `08e06751a431`, binary mtime 1788048955,
SF 8, strict, 7 pairs, two workers. Per-cell medians over pairs on the distributed arm.

## Gates, in the order the record fixed

**1. Authority.** 21/21 finalized, `replay_violations: []`, `timing_authority: true`,
`join_selection_locality_probe: false`. Stamp matches the pin exactly.

**2. The four children partition their parent.** `left_single + left_multi + right_single +
right_multi == join_input_materialization_ms` on **all 1,323 nodes**, zero violations. 604
nodes carry input time and 623 carry a shape record; **no node has input time without a
shape record**, so nothing charged time while escaping the distribution.

**3. The histogram reconciles.** `concats_at_least[0] + empty_concats == concats` on every
side of every node, zero violations.

Comparability against the `e45f5e75` baseline: mean 0.917, range 0.825-0.996 — this board
ran uniformly faster, so the parent's move from 7,100.6 to 6,559.2 ms is drift and is not
read as a change.

## The answer: probe-heavy, multi-batch

| child | board ms | share |
|---|---:|---:|
| **right (probe) multi-batch** | **6,424.8** | **97.9%** |
| left (build) multi-batch | 134.3 | 2.0% |
| right single-batch | 0.02 | ~0 |
| left single-batch | 0.16 | ~0 |
| parent | 6,559.2 | |

Against ADR 580's gate: the **probe side clears 500 ms by 12.8x** and its time is
genuinely multi-batch. The **build side does not clear it** — 134.3 ms, a quarter of the
threshold. Only the probe side is authorized.

**The single-batch premise is confirmed on the board, not just in a bench.** Both
single-batch children together carry **0.18 ms** out of 6,559.2. Arrow's one-batch path
costs what the bench said it costs: nothing worth naming. Had the instrument shipped
without the shape split, that fact would have been invisible and 0.18 ms of wrapper work
would have been indistinguishable from 6.4 s of copying.

### Ownership is not frequency

Per pair, each side runs 106 concatenations; only 34 (left) and 35 (right) are
multi-batch. A **minority of invocations carries essentially all of the time**, which is
exactly what the per-invocation counters were repaired to show. On the cells that matter
the concatenations are uniformly four batches wide - `concats_at_least` reads
`[n, n, n, 0, 0, 0]` on every hot node, so every concat there has 4-7 batches and none has
8.

| cell | parent ms | left multi | right multi | concats (multi) | batches L/R |
|---|---:|---:|---:|---|---|
| q09 | 2,159.4 | 91.8 | **2,067.6** | 8 (4) | 20 / 20 |
| q08 | 1,615.4 | 5.0 | **1,610.4** | 10 (6) | 28 / 28 |
| q17 | 839.4 | 0.2 | **839.2** | 6 (2) | 12 / 12 |
| q05 | 613.5 | 9.2 | **604.3** | 7 (3) | 16 / 16 |
| q21 | 429.4 | 4.3 | **425.1** | 6 (2) | 12 / 12 |
| q02 | 396.7 | 7.7 | **389.0** | 10 (6) | 28 / 28 |

The largest single node is q09 stage 0 task 0: 4 concatenations, 16 batches,
114,378,014 rows, **7.48 GB** of slice bytes copied, 2,083.2 ms.

### The two sides are the same shape and 48x apart in cost

Per pair, both sides run the same number of concatenations, and their batch counts are
close - yet:

| side | concats | batches | rows | slice GB | ms | GB/s |
|---|---:|---:|---:|---:|---:|---:|
| left (build) | 106 | 208 | 59,019,068 | 3.72 | 134.3 | 27.7 |
| right (probe) | 106 | 354 | 588,190,221 | 24.73 | 6,424.8 | 3.85 |

The probe side moves 6.6x the bytes and 10x the rows, but costs **48x** the time. Its
effective copy rate is 3.85 GB/s against the build side's 27.7 GB/s.

**That gap is not explained here.** Column type mix (the probe side is the fact table and
carries strings), allocation cost at gigabyte scale, cache residency of a small build side
- all hypotheses, none measured.

**It also does not gate the decision, and an earlier draft of this section said it did.**
The 6,424.8 ms is spent inside `concat_batches`. A design that does not call
`concat_batches` does not spend it, whatever it is made of — payload copying, allocation,
string offsets, validity handling. The gross removable term is the whole figure. What is
genuinely unknown is what a per-batch path costs **in its place**, and no decomposition of
the copy answers that. The 3.85 GB/s asymmetry stays recorded as a curiosity worth a
sentence, not as a precondition.

## What this authorizes, and what it does not

**Authorized:** the probe-side project - consuming probe batches directly rather than
concatenating them.

**Not authorized:** the build side (below threshold), and any implementation at all until
ADR 580's clause 4 is satisfied.

**And the design to price is smaller than this record first assumed.**
`execute_hash_join` already returns `Vec<RecordBatch>` and
`probe_in_memory_join_rows` already takes one probe batch with batch-local `u32` rows, so
probing the 4-7 right-side batches one at a time needs no `(batch, row)` addressing and
leaves the selection vocabulary and the slice rule alone. Only the left-preserving kinds need anything join-wide — `LeftOuter`, `FullOuter`,
`LeftSemi`, `LeftAnti` — where `build_match_bitmap` is hoisted out of the per-probe call
and unmatched build rows are emitted after the last batch.

The bench prices that pipeline end to end against the current one — concat, keys, probe,
gather or fold — at q09/q08/q17 shapes, reporting phase times, output batch count and peak
memory. Not index arithmetic: the last forecast this line took from a component bench was
falsified in direction.

## The batch-local pipeline, priced - `f34bd6f8`

ADR 580 clause 4 required the whole replacement path priced before any code. Bench
`join_input_pipeline_shape`, release, host settled with `nilket-local.service` stopped,
three interleaved replicates per arm, `ArrayData` equality checked outside the timed region
on the first replicate of every case.

Shapes are ONE INVOCATION of the hot cells - 4 batches x 4.2 M / 7.15 M / 12 M rows - not a
task total the engine never concatenates in one call.

| shape | kind | arm | concat | probe | gather | total | out | peak MB |
|---|---|---|---:|---:|---:|---:|---:|---:|
| q08-like | inner | current | 204.6 | 428.6 | 290.7 | **923.9** | 1 | 1,666 |
| q08-like | inner | candidate | 0.0 | 296.1 | 275.8 | **572.0** | 4 | 497 |
| q09-like | inner | current | 340.6 | 629.1 | 502.9 | **1,472.7** | 1 | 2,566 |
| q09-like | inner | candidate | 0.0 | 463.4 | 461.7 | **925.1** | 4 | 778 |
| q17-like | inner | current | 563.8 | 1,059.9 | 805.7 | **2,429.5** | 1 | 4,430 |
| q17-like | inner | candidate | 0.0 | 766.3 | 801.6 | **1,567.9** | 4 | 1,336 |
| q09-like | leftouter | current | 343.1 | 661.1 | 502.2 | **1,506.4** | 1 | 2,566 |
| q09-like | leftouter | candidate | 0.0 | 487.3 | 458.3 | **945.6** | 4 | 778 |

(q08 and q17 `leftouter` behave as q09's does: 939.4 -> 584.7 and 2,479.8 -> 1,624.9.)

**The candidate wins on every shape and both kinds: -35% to -38% of pipeline time and
-70% of peak memory.** The join-wide-state case costs it 10-20 ms more than `Inner` -
the hoisted bitmap and its deferred finalization are cheap, measured rather than argued.

### Where the win comes from, and one part of it is not the concat

Removing the concatenation is 204.6-563.8 ms of it. The rest - and it is the larger
share - is the **probe phase, 31% faster in the candidate on every shape**. That is not a
locality claim: the current arm grows ONE selection vector to the full output length while
the candidate reuses a per-batch vector across four passes, so much of the difference is
allocation growth, and the bench does not separate the two. The engine's real path has the
same asymmetry, so the effect is real; its cause is not established here.

Gather is unchanged (within 5%), as it must be - the same rows are gathered either way.

### What this does NOT establish

- **Model, not engine.** The join internals are `pub(super)`; both arms share a faithful
  model of the path. Absolute milliseconds are model numbers and the delta is the decision
  input, not a board forecast.
- **Key derivation is priced at zero.** `derive_keys` is a column clone here, so the
  candidate's four key derivations cost nothing. Real `key_arrays` work - multi-column
  keys, null-safe handling - would be paid four times instead of once, and this bench says
  nothing about it.
- **The wider output is not consumed.** The candidate returns 4 batches where the current
  path returns 1. Nothing downstream runs here, so the cost of that shape change is
  recorded as a fact and not as a price.
- **The board term is the concat only.** `join_input_materialization_micros` names 6,424.8
  ms; the probe and gather phases live in other counters. A board claim from this bench
  would be that removing the concatenation removes up to that term - not the 35% total the
  model shows, which spans phases this record does not own.
