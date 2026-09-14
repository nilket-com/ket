# ADR 589 - phase 1: the ordering census, results

Measured 2026-09-14 at the sealed ADR 583 coordinate under ADR 587's driver
on a `profiling` build of the census tree. Every gate passed. **The
opening rule opens phase 2**: on both summaries, in every pair, on every
one of the four partition streams, `below_max` is zero, `found_elsewhere`
is zero, and every found lookup is a boundary straddle, 4,370 on the semi
alias and 3,155 on the anti, equal to `lookup_found` exactly. The
consumed stream is non-decreasing within each partition and restarts three
times per source batch across partitions, 17,577 restarts in 23,435 batch
transitions, which is the Exchange's four-way traversal and nothing else.
The Context's hypothesis is now a measurement. No engine behaviour
changed.

## Identity and coordinate

| identity | value |
|---|---|
| sealed baseline executable | `b436a843b107b2f385d5e7c2486c41b1d1f89acfdb4af75f8a4793c823a05ee4` |
| ADR 587 instrumented executable | `3f3a5e2c786d18a8142f4cb8477fd1360566b62bbb23e65eb5fd177620e59c36` |
| census source tree | `e4d3d0f9…` (commit `a30daff6`, amended with this record before closure) |
| census executable | `997a3d12…` (profile `profiling`, recipe `a1fad89d…`, ADR 587's) |
| dataset manifest | `fnv1a64:0fc5c06d19c5ed23` |
| profiler | `perf version 7.0.14`, `perf_event_paranoid = 2`, self-check 9,000 samples, none lost |

Coordinate as ADR 587's: q21 alone, strict SF=8 over persisted Parquet,
explicit 2 GiB task memory and spill reserve, partition parallelism 1,
full-hash dataset verification, empty dedicated spill scratch
(`target/tpch/adr589-join-spill`, empty after the run), the deployment
stopped for the run and restarted after it, on a quiet host (load 0.74,
package 50 °C at start). Artifacts: `target/tpch/adr589-q21-census/`
(`samples.jsonl`, `summary.json`, `profiles/pair-NN-on.json`,
`pair-NN.perf.data`, `pair-NN.bins.json`). Unsealed, no candidate
authority.

## Instrument

The existence writer now carries each batch's Exchange partition through
the op chain as `(partition, batch)` and hands it to the instrumented
`write_batch`; the unprofiled path carries the index and ignores it. The
instrumented loop keeps, per partition, the running maximum `max[p]` as
`Option<i64>` and the last key of the previous batch on that partition,
and classifies every row the consecutive-key cache does not absorb as
`above_max`, `equal_max`, or `below_max`, attributing each found lookup to
a boundary straddle (first row of a batch equal to the previous batch's
last key on the same partition) or to `found_elsewhere`. Per consumed
batch it records the partition, first key, last key, and a restart flag,
encoded as one delta string per summary (249 KB for 23,436 batches). The
node profile grew to 1.0 MB and the outer residual to 0.99–1.09 ms,
inside ADR 507's 10 ms boundary in every pair.

## Phase 0, perturbation, and the ADR 587 terms

Phase 0: 12,996.1 ms against ADR 587's 12,827.1 ms, +1.32%, not tripped.
Five pairs: off 12,901.3–13,030.2 ms (median **13,008.9**, range 128.9 ms),
on 13,465.4–13,575.0 ms (median 13,498.5, range 109.6 ms); median absolute
movement **4.18%**, accepted; every profile complete; 150,000 samples per
pair, none lost. The census adds one comparison per row and a small
per-batch record on top of ADR 587's instrument, and the on-arm cost rose
from ADR 587's 3.33% to 4.18%; the pairs price it.

ADR 587's terms, re-reported: `semi_insert_rows_excluding_grow` 1,240.9
ms (9.54%; ADR 587: 1,122.8 at 8.75%), `semi_insert_grow` 446.0 (450.5),
`anti_insert_rows_excluding_grow` 987.6 (907.6), `anti_insert_grow` 442.3
(440.7); the row loop is still the owner; sampled classes 92.6%
hash-and-probe, 89.1% rehash, consistent in every pair; coverage 0.903
semi, 0.892 anti. The row-loop term is about 10% higher than ADR 587's on
an off median 1.4% slower (13,008.9 against 12,827.1 ms); the census's
per-row comparison sits inside the `insert_rows` windows and is the
plausible cause, reported not netted.
ADR 586's five terms and ADR 585's thirteen re-reported within their
prior ranges (`semi_write_insert` 1,702.8; `runtime_prep_semi_write`
2,321.6).

## The census

Identical in every pair, as counts must be:

| | semi | anti |
|---|---:|---:|
| partitions consumed | 4 | 4 |
| batches per partition | 5,859 | 5,859 |
| `above_max` (sum) | 12,000,000 | 11,002,698 |
| `equal_max` (sum) | 4,370 | 3,155 |
| `below_max` (every partition) | **0** | **0** |
| restarts within a partition | **0** | **0** |
| `boundary_straddles` | 4,370 | 3,155 |
| `found_at_straddle` | 4,370 | 3,155 |
| `found_elsewhere` | **0** | **0** |
| `lookup_found` (ADR 587 counter) | 4,370 | 3,155 |
| `appended` | 12,000,000 | 11,002,698 |

Per partition on the semi (pair 0): `above_max` 3,000,723 / 3,000,430 /
2,999,835 / 2,999,012, `equal_max` 1,118 / 1,087 / 1,080 / 1,085, running
maxima 47,999,973 / 47,999,970 / 48,000,000 / 47,999,975, `below_max` 0 and
restarts 0 on each. The anti's four partitions likewise, with `equal_max`
808 / 795 / 781 / 771.

Decoding the consumed-batch string: 4,370 semi batches open on the key
their partition's previous batch ended with (the straddles, by batch, equal
to the row count above because a straddle is one row); and 17,577 of the
23,435 batch-to-batch transitions open below the previously consumed
batch's last key across partitions, exactly three per source batch (5,859
× 3), which is the Exchange's traversal of partitions 0, 1, 2, 3 restarting
the key range each time. Within a partition the stream never restarts.

## The opening rule, applied as written

Phase 2 opens only if, on both summaries and in every pair, `below_max` is
zero on every partition and `found_elsewhere` is zero. Ten censuses were
checked, no violation was found: **phase 2 opens.** Every recurrence the
cache lost is a boundary straddle under the Exchange's partition
traversal, and every partition stream is non-decreasing; the fast path's
premise, that a key above its partition's running maximum is absent, holds
on the consumed stream at this coordinate, subject to phase 2's admission
gate proving the routing a pure function of the existence key from the
plan.

## What the Exchange does to order

The measured finding, for ADR 586's repartition candidate: every one of
the 17,577 observed global restarts occurs at a partition transition, and
every one of the 4,370 + 3,155 found lookups occurs at a same-partition
boundary straddle. No restart and no recurrence was observed anywhere
else. What the stream would look like without the Exchange is not
measured here: per-partition monotonicity says nothing about the order the
Exchange's input arrived in before it was split, and source-batch
boundaries would still split a key's rows across batches, so a build
without the Exchange remains an unmeasured counterfactual. That decision
is not taken here.

## Implementation and verification

Engine: the partition carried through the op chain; `PartitionOrderCensus`
and `ConsumedBatch` on the instrument with the classification in the
instrumented row loop; the consumed-batch encoding and its test-only
decoder; four tests (a boundary straddle counted as `equal_max` and
`found_at_straddle` with the other partition's maximum untouched;
descending keys as `below_max` with a recurrence elsewhere and a batch
restart; `i64::MIN` as a first key above an unseen partition's `None` with
the per-batch records exact through the encoding; the write loop
attributing batches to their Exchange partition). Driver: the census
reported per summary and pair beside ADR 587's terms and the opening rule
applied by `adr589_opening_rule`, with a test opening on a clean census,
closing on a single `below_max` or `found_elsewhere` anywhere, and refusing
a missing census; ADR 587's line classes re-pinned to the census tree with
the anchor test passing. Under review the census evaluator was tightened
so that incomplete evidence cannot open phase 2: it decodes the
consumed-batch string and requires every partition it names to be in the
census and every census partition to have consumed at least one batch;
requires the census batch total, the decoded batch count, and the rows
window count to agree, per partition and in sum; requires the decoded
restarts to equal the census's; and requires the two accounting
identities `above_max + equal_max + below_max = lookups` and
`found_at_straddle + found_elsewhere = lookup_found`, with
`found_at_straddle <= boundary_straddles`. Tests cover an empty partition
list, a partition dropped from the census while the consumed batches name
it, a batch total disagreeing with the rows windows, a per-partition batch
count disagreeing with the consumed batches, classes not summing to the
lookups, found attributions not summing to the found lookups, and a
consumed string decoding to the wrong count. The ten retained censuses
were re-evaluated under the tightened checks and every one passes them
(all four partitions covered, 5,859 batches each, 23,436 batches on every
count, both identities exact, 17,577 global restarts in each); the opening
verdict stands on the retained evidence and no run was repeated. Ladder (`RUST_MIN_STACK=67108864`): fmt clean;
Toron library 1,788; architecture 2,151; integration 3,649; toron-tpch 589
passed, 6 ignored.

Phase 2 is not implemented under this commit; it opens on this evidence
and proceeds under the record's phase 2 contracts.
