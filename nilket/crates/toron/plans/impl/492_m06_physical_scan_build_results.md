# ADR 492 implementation receipt — physical scan build honored

The fused two-scan inner-join visitor was undoing the binder's physical placement:
it tried the right input as build first whenever both inputs were scans. On m06's
`SwappedRightBuild` plan that rebuilt the 48M-row lineitem probe, selected the
partitioned streaming scan-build path, and produced all `88,559,536` residual spill
bytes attributed by ADR 491.

The visitor now preserves the physical contract: left builds and right probes. When
the scan-derived build estimate fits the task budget, it lets the caller materialize
that physical build and re-enters the existing in-memory build plus streaming-probe
path. Larger builds retain the established partitioned spill path and guards.

## Matched alternating A/B

Coordinate: 2 GiB task, 2 GiB spill reserve, sf=8, strict parquet, release. The same
binary (`build_commit=0ad6184102e0-dirty`, identical binary mtime) alternated control
and candidate three times. `TORON_TPCH_PHYSICAL_SCAN_BUILD=0` restores the previous
right-build-first dispatch; `=1` is the default candidate.

| arm | execute (3 runs) | median | peak RSS | spill owner |
|---|---|---:|---:|---|
| previous fused dispatch | 14,951 / 14,957 / 15,054 ms | 14,957 ms | ~466 MiB | replay 0 `partitioned_streaming_scan_build`, 88,559,536 B |
| physical build honored | 3,526 / 3,545 / 3,545 ms | 3,545 ms | ~1,017 MiB | replay 0 `in_memory`, 0 B |

**Effect: -76.3% execute median against the ADR 490 local plan.** All six rows have
the same `01b38b33108cf032` fingerprint, admitted 1.44 GB / 2 GiB binder receipt,
standing digest, and one-row output. The plan is unchanged; only the executor's
runtime orientation/path differs. Candidate RSS rises by about 551 MiB but remains
well below the explicit 2 GiB task coordinate and the 12 GiB process ceiling.

Rows are preserved under `target/tpch/adr492_ab_{control,candidate}_{1,2,3}/`.

## Rails

- A focused two-scan regression proves the physical left side builds, the physical
  right side streams batch-by-batch, output order remains correct, spill is zero,
  and the typed runtime receipt is `in_memory`.
- The A/B switch changes only scan/scan orientation; it does not change task memory,
  planner admission, the plan fingerprint, or executor spill thresholds.
- Builds whose scan-derived estimate exceeds task memory still enter the existing
  partitioned streaming scan-build path.

## Forward

The admitted path currently scans the fitting build once to estimate it and again to
materialize it. Eliminating that duplicate read is the next bounded opportunity, but
it should be priced separately; ADR 492's large win comes from honoring physical
placement and avoiding the wrong-side spill, not from scan-count tuning.
