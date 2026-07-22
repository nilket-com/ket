# ADR 493 implementation receipt — retain fitting scan build

ADR 492's in-memory physical-build path read a fitting scan twice: once to derive
its runtime memory estimate, then again when the caller materialized the build. The
scan estimator now retains batches only while its running estimate remains within
the task-memory cap and passes those batches directly to the existing in-memory
hash-build path. If the estimate crosses the cap, the accumulator is dropped and
estimation continues without retaining an oversized build; the established
partitioned/spilled path is unchanged.

## Matched alternating A/B

Coordinate: 2 GiB task, 2 GiB spill reserve, sf=8, strict parquet, release. The same
binary (`build_commit=f5add54495f2-dirty`, `binary_modified_unix=1784696593`)
alternated control and candidate three times. `TORON_TPCH_RETAIN_FITTING_SCAN_BUILD=0`
restores ADR 492's estimate-then-rescan behavior; `=1` is the default candidate.

| arm | execute (3 runs) | median | peak RSS median | spill |
|---|---|---:|---:|---:|
| estimate, then rescan | 3,419.2 / 3,424.4 / 3,439.1 ms | 3,424.4 ms | 1,041,708 KiB | 0 B |
| retain fitting scan | 3,372.0 / 3,372.5 / 3,360.5 ms | 3,372.0 ms | 1,041,808 KiB | 0 B |

**Effect: -1.5% execute median.** All six rows have plan fingerprint
`01b38b33108cf032`, the same Arrow IPC digest, admitted 1.44 GB / 2 GiB binder
receipt, typed replay-0 `in_memory` observation, and one-row output. RSS is flat
within run noise and remains below the explicit task coordinate.

Rows are preserved under timestamped `target/tpch/adr493_ab_{control,candidate}_*/`
directories.

## Rails

- A focused scan/scan join regression proves a fitting physical build is requested
  once and the probe once, with the standing output and zero-spill receipt.
- A direct accumulator regression uses a one-byte cap and proves that crossing the
  cap returns no retained batches after completing the estimate.
- The A/B switch changes only whether fitting batches are retained during the
  estimate. It does not change planning, task memory, join orientation, spill
  thresholds, or output semantics.

## Forward

With the wrong-side spill removed and the fitting build read once, the remaining
m06 time is dominated by the in-memory join/probe and downstream aggregate rather
than redundant build I/O. The next cut should profile those CPU paths before
selecting another candidate.
