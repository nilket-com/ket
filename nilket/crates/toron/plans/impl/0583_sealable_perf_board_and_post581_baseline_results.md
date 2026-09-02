# ADR 583 - Sealable perf board and post-581 baseline results

ADR 583's canonical Toron-and-DuckDB board sealed with final authority. The
coordinate is strict SF=8 persisted Parquet, q01 through q22 in order, Toron
then DuckDB per query, 1 warmup plus 5 measured samples per cell, explicit 2
GiB task memory and 2 GiB spill reserve, Toron partition parallelism 1, and
DuckDB v1.5.4 at one thread. All 264 samples passed, all 44 summaries were
stable, the deployment was quiescent at both observations, and the join-spill
scratch was empty before and after.

Every row carries an empty partition-work concurrency observation list. That is
the expected shape of the one-task coordinate, which dispatches no partition
work: the at-or-below-one condition is met by the absence of dispatch, not by
observed values, and the gate refuses any observation above one if a future
board records any. It is stated here so an empty list is never read as a
measured one.

## Sealed identity

The results identify the measured content, not the commit that later records
this document.

| identity | value |
|---|---|
| source tree | `9e29278d107cb08566d394edcba16d8d67178941` |
| evidence tag | `evidence/9e29278d107c-100b880d2f90` |
| artifact SHA-256 | `100b880d2f9001c97e2ec6a3a27cbd0a432dd7972e61104f819e092507932992` |
| harness/Toron SHA-256 | `b436a843b107b2f385d5e7c2486c41b1d1f89acfdb4af75f8a4793c823a05ee4` |
| DuckDB SHA-256 | `c478c0f49f6755100d55dee32de485c0f00f3729b40b62658a83b0666b684707` |
| build recipe SHA-256 | `fcdf9ca12816d9da39a63948e6f24d6f08e6a55c923e337638b3f2634f0609ab` |
| dataset manifest fingerprint | `fnv1a64:0fc5c06d19c5ed23` |

The sealed manifest archives and rehashes all three executable roles; harness
and Toron are the same staged binary for this in-process board. Its
`final_authority` is true. The tag resolves on `origin` to the annotated tag
object for the measured tree.

## Comparison with the committed ADR 521 table

This is necessarily table-to-table. ADR 521's artifact is gone and its DuckDB
executable hash is unknown, so the comparison inherits that document's stated
version and one-thread control rather than claiming executable-identity
continuity. The old aggregates below are recomputed from its committed rounded
per-cell rows; that gives 80.612 s and 19.027 s rather than adding precision to
the displayed 80.6 s and 19.0 s.

| aggregate | ADR 521 | ADR 583 | change |
|---|---:|---:|---:|
| median per-cell Toron/DuckDB ratio | 4.26x | 4.55x | +0.29x (+6.7%) |
| sum of Toron cell medians | 80.612 s | 91.584 s | +10.972 s (+13.6%) |
| sum of DuckDB cell medians | 19.027 s | 20.982 s | +1.955 s (+10.3%) |
| ratio of summed medians | 4.237x | 4.365x | +0.128x (+3.0%) |

The answer is therefore **worse on this board**, not better: both engines are
slower than the committed 521 table, but Toron slowed more, so both reported
relative aggregates regress. This is one board on a host whose prior atlas
drift reached 10 percent. The board reports the result as observed; it does not
turn the difference into an engine-causality claim.

Rows are ranked by ADR 583's absolute Toron-minus-DuckDB gap. `gap delta` is the
new absolute gap minus the ADR 521 table's gap.

| cell | ADR 583 Toron (ms) | ADR 583 DuckDB (ms) | ratio | gap (ms) | ADR 521 Toron (ms) | ADR 521 DuckDB (ms) | gap delta (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| q21 | 14,956.0 | 2,970.0 | 5.04x | 11,986.0 | 12,646.0 | 2,693.0 | +2,033.0 |
| q09 | 10,211.5 | 2,192.0 | 4.66x | 8,019.5 | 8,602.7 | 2,020.0 | +1,436.8 |
| q05 | 7,329.0 | 861.0 | 8.51x | 6,468.0 | 6,452.3 | 734.0 | +749.7 |
| q18 | 7,691.3 | 2,024.0 | 3.80x | 5,667.3 | 7,534.1 | 1,765.0 | -101.8 |
| q01 | 6,445.4 | 1,302.0 | 4.95x | 5,143.4 | 5,165.9 | 1,141.0 | +1,118.5 |
| q07 | 5,507.0 | 818.0 | 6.73x | 4,689.0 | 4,710.6 | 736.0 | +714.4 |
| q08 | 4,555.8 | 906.0 | 5.03x | 3,649.8 | 3,899.4 | 804.0 | +554.4 |
| q04 | 4,248.0 | 623.0 | 6.82x | 3,625.0 | 3,359.2 | 553.0 | +818.8 |
| q17 | 4,178.0 | 776.0 | 5.38x | 3,402.0 | 4,273.8 | 768.0 | -103.8 |
| q12 | 2,797.9 | 650.0 | 4.30x | 2,147.9 | 2,460.8 | 618.0 | +305.1 |
| q13 | 3,413.3 | 1,539.0 | 2.22x | 1,874.3 | 3,059.8 | 1,419.0 | +233.5 |
| q02 | 2,024.9 | 177.0 | 11.44x | 1,847.9 | 1,928.5 | 157.0 | +76.4 |
| q03 | 2,702.1 | 902.0 | 3.00x | 1,800.1 | 2,328.1 | 797.0 | +269.0 |
| q16 | 1,947.9 | 271.0 | 7.19x | 1,676.9 | 1,576.8 | 234.0 | +334.1 |
| q10 | 3,134.8 | 1,502.0 | 2.09x | 1,632.8 | 3,022.4 | 1,369.0 | -20.6 |
| q20 | 2,277.9 | 690.0 | 3.30x | 1,587.9 | 2,034.3 | 641.0 | +194.6 |
| q15 | 1,905.3 | 429.0 | 4.44x | 1,476.3 | 1,631.5 | 396.0 | +240.8 |
| q19 | 2,367.5 | 895.0 | 2.65x | 1,472.5 | 2,098.2 | 821.0 | +195.3 |
| q11 | 1,178.5 | 140.0 | 8.42x | 1,038.5 | 1,383.2 | 132.0 | -212.7 |
| q22 | 871.2 | 304.0 | 2.87x | 567.2 | 801.8 | 280.0 | +45.4 |
| q06 | 996.3 | 494.0 | 2.02x | 502.3 | 837.2 | 444.0 | +109.1 |
| q14 | 844.3 | 517.0 | 1.63x | 327.3 | 805.3 | 505.0 | +27.0 |

Sixteen DuckDB summaries retain the same informational `schema_mismatch` flag
documented by ADR 521: the engines present equivalent outputs with different
physical type fingerprints. Every row-count comparison passes. The gate treats
the former as disclosure and the latter as correctness.

## Rejected rehearsal and next owner

The first full sweep exposed a false refusal in the new gate: it treated ADR
521's known informational schema-presentation flag as a correctness failure.
That sweep remained unsealed. The gate was corrected and locked by paired tests
that still reject a row-count mismatch, the source tree changed, the release
binary was rebuilt, and the canonical board was rerun from fresh output and
scratch directories. No result from the rejected tree entered the sealed board.

q21 remains the next owner by the decided absolute-gap rule, at 11,986.0 ms.
That opens an attribution cut, not wider admission: ADR 580's remaining
multi-batch term still has to be assigned among the excluded shapes (including
non-Inner, spilled and aggregate work) before any successor broadens the
batch-local route.
