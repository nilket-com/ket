# ADR 457 implementation receipt

The paired ablation at the isolating cell `(64 MiB memory_per_task, 2 GiB spill
reserve, sf=8)` completed cleanly. Its dual-criterion verdict is **detectable but
immaterial**: the runtime-filter bridge helps, but its Hodges–Lehmann point estimate
does not clear the predeclared 10%-of-OFF-median floor.

Together with ADR 456, this is the planned one-coordinate comparison: both cells hold
the scale and spill reserve at `(sf=8, 2 GiB)` and differ only in `memory_per_task`
(64 MiB versus 128 MiB). Reducing that budget does not restore materiality at sf=8.

## Cohort integrity

- Single detached release parent; the durable journal has **21/21** rows (one warmup
  and 20 timed children), all passing. Timed rows have contiguous run indices 0–19,
  ten ON and ten OFF arms, paired by the recorded `(arm, run_index)` identity.
- Gate zero passed in-run at sf=8: OFF reservoir rows **47,989,007** equal the
  scale-parametric `generate_strict_lineitem(8.0)` oracle, ON rows **11,742,021** are
  strictly less, and both output digests are identical.
- The manifest and every child record the explicit cell coordinates:
  `memory_per_task: 67,108,864` and `spill_bytes_reserved: 2,147,483,648`.
  No child reported `BudgetExhausted`; the predeclared 4 GiB rerun was neither needed
  nor used.

## Gate-zero oracle hardening

The cohort did not require a harness change. Its post-run cgroup verification exposed
an avoidable test-cost defect in ADR449's cardinality oracle: the test materialized the
full sf=10 strict `LINEITEM` Arrow batch solely to call `num_rows()`, and systemd-oomd
killed that test at the wrapper's 12 GiB cap. `strict_lineitem_row_count` now traverses
the same `LineItemGenerator` without materializing columns; the gate-zero OFF
expectation remains exact and scale-parametric. The ADR449 test now uses that path for
its 0.01/1/10 scale checks and additionally verifies the 0.01 count against the
materialized strict batch. No cohort samples or cohort verdict changed.

## The verdict (mandatory order, within-cell only)

- Pairing is complete and **10/10 OFF−ON differences are positive**, ranging from
  100 ms to 36,068 ms.
- **HL OFF−ON shift: 4,256 ms**, with a 95% Wilcoxon interval of
  **[2,314, 18,885] ms**. The interval excludes zero: the effect is **detectable**.
- OFF-arm median: **46,414 ms**. The predeclared 10% materiality floor is
  **4,641 ms**. The HL point is 91.7% of that floor, so the verdict is
  **immaterial** under the dual criterion.
- The pooled summary is flagged `unstable`, driven in part by an anomalously slow
  OFF child (91,212 ms). That is not a baseline verdict and does not replace the
  predeclared paired estimator: all signed pairs remain positive and the interval
  excludes zero. The wide interval and near-floor point are retained rather than
  smoothed into a stronger claim.

## The four observed cells

| ADR | memory per task | spill reserve | scale | result |
|---|---:|---:|---:|---|
| 451 | 64 MiB | 512 MiB | 4 | material; HL 8,411 ms, 25.7% of OFF median |
| 454 | 256 MiB | 512 MiB | 4 | non-committing resident path; no paired cohort |
| 456 | 128 MiB | 2 GiB | 8 | detectable, immaterial; HL 4,273 ms, ~9.8% |
| 457 | 64 MiB | 2 GiB | 8 | detectable, immaterial; HL 4,256 ms, ~9.2% |

## What this seals

ADR 456 and this result isolate the task-budget coordinate at sf=8 with a 2 GiB
non-binding reserve. The bridge remains detectable at both measured sf=8 cells but is
immaterial at each, so the 64 MiB task budget **does not restore materiality across
that twofold budget step**.

This does not attribute the 451-to-sf=8 difference to scale alone: ADR 451's 512 MiB
reserve binds at sf≥7, so its reserve cannot be held against the sf=8 cells. It is
therefore evidence against the task-budget explanation at sf=8, not a complete map of
the three-coordinate frontier. There is no cross-cell wall-clock ranking or
extrapolation beyond these observed cells.

With paired evidence from the two sf=8 cells, chain reorientation remains re-opened
for a new mechanism decision. The ADR 446 spill-scope gap, engine-side `Default`
retarget, `MAX_FILTER_BYTES` cap raise, ADR 132 comparator, q17 remeasurement, q21
residual, q19 re-diagnosis plus 438 widening, q13, Cut A (439 mirror), and the generic
O(1) LRU rewrite remain queued.
