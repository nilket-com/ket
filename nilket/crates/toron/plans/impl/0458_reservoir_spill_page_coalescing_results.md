# ADR 458 implementation receipt

ADR 458 coalesced the q05 grouped-reservoir writer's per-partition fragments into
bounded 8,192-row IPC pages. At the selected cell `(64 MiB memory_per_task, 2 GiB
spill reserve, sf=8)`, the page ledger moved as designed and the paired re-verdict is
**detectable and material**.

## Implementation and safety rails

- Only the **unindexed** reservoir writer coalesces. Indexed spill writers retain
  their direct page writes because they publish page-index row references as they go.
- Each `SpilledInnerJoinBuild` buffers a single partition's eligible fragments until
  the existing `CROSS_JOIN_OUTPUT_CHUNK_ROWS = 8,192` cap, then writes one composed
  Arrow IPC page. The streaming writer flushes all partitions at its existing
  all-builds barrier; `finish()` refuses an unflushed partial buffer.
- Pending-buffer bytes are included in the writer high-water resident-footprint
  accounting before the buffer can grow further. Empty-key fragments retain the old
  skip behavior rather than becoming new physical rows.
- `unindexed_spill_coalesces_fragments_and_flushes_the_partial_page` proves that
  multiple same-partition fragments yield one page, a partial buffer cannot be
  finished silently, and its decoded rows equal the concatenation.

## Correctness and gates

- Strict persisted-Parquet q05 correctness at sf=8 passed against the independent
  oracle in **29,332 ms** with the explicit `(64 MiB, 2 GiB)` coordinates.
- Architecture tests: **2,091 passed**. Full `toron --no-fail-fast`: **passed**.
- Fresh gate zero passed: OFF `reservoir_build_rows = 47,989,007` equals the
  scale-parametric oracle; ON `= 11,742,021` is strictly smaller; output digests are
  identical. The fresh cohort is at
  `target/tpch/adr458_cohort_sf8/`; it does not reuse a pre-coalescing journal.

## Page-shape ledger (before any wall interpretation)

| arm | reservoir rows | pages | spill bytes | rows/page | bytes/row |
|---|---:|---:|---:|---:|---:|
| ON | 11,742,021 | 1,442 | 164,814,256 | 8,143 | 14.04 |
| OFF | 47,989,007 | 5,890 | 668,759,344 | 8,148 | 13.94 |

The pre-fix sf=8 gate-zero ledgers recorded 735,285 ON pages and 749,840 OFF pages.
The new page counts therefore track row volume rather than fragment arrival, and the
two arms' byte density is now at parity. `physical_write_calls`, `physical_read_calls`,
and `decode_count` equal page count in each arm; read amplification remains 1.0.

## Paired re-verdict

The release cohort completed **21/21** journal rows (one warmup, 20 timed children),
with ten contiguous ON/OFF pairs, no `BudgetExhausted`, and explicit budget provenance
in every record. The pooled summary is flagged `unstable` because it deliberately
combines two distinct treatment distributions; the predeclared paired estimator is the
admissible measure.

- All **10/10 OFF−ON differences are positive**.
- Hodges–Lehmann OFF−ON shift: **4,639 ms**; exact paired Wilcoxon 95% interval:
  **[4,395, 4,904] ms**. The interval excludes zero, so the effect is detectable.
- OFF median: **26,225 ms**; the 10% OFF-baseline materiality floor is **2,623 ms**.
  The point estimate is 17.7% of the OFF median and 1.77× the floor, so it is
  material by the sealed dual criterion.

## What this establishes

At this one post-format cell, page-granularity overhead was the reason the runtime
filter's 4× reservoir-row reduction did not translate into material wall reduction.
Coalescing removes that overhead mechanically and makes the existing bridge material
again. This is a within-cell causal result only: ADR 451/456/457 remain sealed
measurements of the pre-coalescing format and are not ranked against this receipt.

The immediate residual owner is no longer the tiny-page spill writer. Future
reorientation work should begin from this post-coalescing ledger; a flame graph is
appropriate only if a later committed-cell result underdelivers after this structural
page result, not as a replacement for the ledger that identified it.
