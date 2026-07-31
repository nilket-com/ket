# ADR 362 results - scan-streaming consumer extension

## Summary

ADR 362 removes q21's remaining sf=10 `scan output accumulator` wall from ADR 361 by widening the existing
shape-named streaming surface, not by adding a query-specific path.

The cut is green-or-next-wall. It landed as a next-wall result:

- q21 no longer fails at the core `scan output accumulator`.
- q21 now reaches a named spill-volume wall in the streamed inner build writer:
  `spill_budget_exhausted`, producer `streamed inner join build spill writer`.
- Peak RSS stays under the fixed 4 GB cap at sf=10.

## Phase 0 - edge provenance

The bound-plan probe named the remaining materialized edge before implementation:

| table | bound-plan position | consumer | why ADR 361 missed it |
| --- | --- | --- | --- |
| orders, `TableId(3)` | `JoinReplayId(1).right` through `Exchange(Scan)` | reoriented inner-chain build side prepared by `prepare_reoriented_inner_join_runtime` | ADR 361 streamed the base scan/probe edge and semi/anti existence scans, but later reoriented inner build sides still used `execute_bound(stage.build)` |

The full relevant path was:

```text
Aggregate
  -> Exchange
  -> LeftAnti(4).left
  -> Exchange
  -> LeftSemi(3).left
  -> Exchange
  -> Filter
  -> Inner Join(2).left
  -> Exchange
  -> Filter
  -> Inner Join(1).right
  -> Exchange
  -> Scan orders
```

Decision: same-family / fence-too-tight. The implementation widens the reoriented inner runtime to stream scan-like
build sides into a spilled inner build. It does not add a q21-specific edge.

## Implementation

- `ReorientedInnerJoinRuntime` now stores either an in-memory build or a spilled build.
- The inner-prefix semi/anti streaming path prepares later inner build sides with
  `prepare_reoriented_inner_join_runtime_with_streamed_scan_build`.
- Scan-like build sides reuse the existing `bound_scan_request -> ScanStream` path and
  `write_streaming_scan_inner_build_batches`, so the build is written batch-by-batch and dropped instead of
  materialized with `execute_bound -> Vec -> concat_or_empty`.
- The probe path reuses the existing spilled-inner probe materializer and restores the observable probe-first column
  order.
- Runtimes clean up spill files after the grouped aggregate finishes.

Core symbols and comments remain topology-named; no `qNN` / `QNN` references were added under `crates/toron/src/exec*`.

## Regression test

Added:

- `reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order`

The test covers the widened edge with an `Exchange(Scan)` build side:

- proves the scan build is written as separate batches, not concatenated;
- compares streamed-spilled output to the materialized in-memory path;
- covers both `Float64` and `Decimal128`;
- verifies probe-first column order restoration.

## sf=10 live result

Command:

```text
systemd-run --user --scope -p MemoryMax=4G -p MemorySwapMax=0 \
  /home/me/work/nilket/target/release/toron-tpch perf \
  --sf 10 --mode strict --runs 1 --warmup 0 --query q21 --engines toron \
  --data-source parquet --data-dir /home/me/work/nilket/target/tpch/data \
  --output-dir /home/me/work/nilket/target/tpch/perf_adr362_q21
```

Observed raw row:

| query | status | reason | producer | requested | available | execute_ms | peak_rss_kib |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| q21 | fail | `spill_budget_exhausted` | `streamed inner join build spill writer` | 536872016 | 536870912 | 669610.649 | 1804796 |

The old `scan output accumulator` producer is gone. The next wall is spill volume, 1,104 bytes over the 512 MiB spill
budget. Memory stayed bounded under the fixed 4 GB cap.

## Correctness

- `scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q21 --sf 0.01` -> pass
- `scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q21 --sf 1` -> pass
- Dev-style parity is covered by the focused `Float64` unit test above; the TPC-H correctness command is not used for
  dev parity.

## Verification

- `scripts/cargo-cgroup fmt -- --check` -> pass
- `scripts/cargo-cgroup check -p toron --all-targets` -> pass
- `scripts/cargo-cgroup test -p toron reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order --lib` -> pass
- `scripts/cargo-cgroup test -p toron --lib` -> 1165 passed
- `scripts/cargo-cgroup test -p toron --test architecture_test` -> 2079 passed
- `scripts/cargo-cgroup build -p toron-tpch --release` -> pass
- `scripts/cargo-cgroup test -p toron-tpch` -> pass
- no `qNN` / `QNN` references in `crates/toron/src/exec*`

`scripts/cargo-cgroup test -p toron --no-fail-fast` is not green in this tree, but the failing targets reproduce at
the clean HEAD baseline (`9e3710e`) without this implementation:

- `ctas_test::query_coordinator_empty_source_ctas_initializes_queryable_table`
- `delete_test::coordinator_deletes_all_rows_without_where`
- `distributed_process_test::select_distinct_join_dedups_across_partitions`

The full current run also reported additional failures in `distributed_process_test` with the same empty-batch schema
pattern. These are pre-existing integration failures, not ADR 362 regressions.

## Forward

q21 has moved past the remaining scan-materialization wall and now exposes a spill-byte wall in the streamed inner
build writer. The next cut should classify the 1,104-byte spill excess the same way ADR 348 handled q18: reduce it
without a cap bump if the excess is projection/layout slack, or name it as an irreducible sf=10 spill-volume wall if
the bytes are already minimal.
