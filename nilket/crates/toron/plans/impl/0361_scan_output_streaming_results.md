# ADR 361 results - scan-output streaming

## Summary

ADR 361 removed the persisted TPC-H scan-output accumulator as the first sf=10 engine wall for the targeted scan edges.
The cut is green-or-next-wall, and it landed that way:

- q14 strict sf=10 now passes under the fixed 4 GB cap.
- q21 strict sf=10 no longer fails at the persisted adapter accumulator; it advances to the core
  `scan output accumulator`, a named memory wall from materializing a different scan edge.

The broader materialize-each model is still present. This ADR only streams the matched scan edge into the streaming
join consumer.

## Gate 1 - shared ScanRequest builder

Added `Executor::bound_scan_request`, used by both production bound-scan paths:

- the `ExecutionPlan::Scan` arm in `exec.rs`;
- the existing direct-scan existence path in `exec/join/streaming.rs`.

The drift test `bound_scan_request_carries_every_bound_scan_field` asserts all request fields are preserved:
projection, PBC predicate, runtime filter, row identity, profile hint, morsel, row limit, trace context, downstream
cancellation, and identity/session/table fields.

The row-identity architecture guard now checks the shared builder, so future scan-request changes do not silently
bypass this boundary.

## Gate 2 - edge map

The persisted sf=10 bound shape reached by the cut was:

- q14: global aggregate over `Filter(Join(Exchange(Scan lineitem), Exchange(Scan part)))`.
- q21: an initial scan-like edge feeding the existing streaming join chain; after this cut the remaining wall is a
  later materialized scan accumulator, not the persisted adapter accumulator.

The implementation supports the observed scan-like edge forms needed here: `Scan`, `Filter(Scan)`, and
`Exchange(Scan | Filter(Scan))`. The direct scan path is still shape-fenced to the streaming join consumer; this is not
a general scan-child recognizer.

## Implementation notes

- The TPC-H persisted Parquet adapter no longer owns a cumulative `persisted parquet scan output accumulator` guard.
- Core scan materialization now has its own budgeted producer, `scan output accumulator`, so materialized scans still
  fail honestly when an unmatched edge accumulates too much output.
- Streaming join can consume a scan-like probe batch-by-batch and can write a scan-like build side to the spilled inner
  build without first calling `execute_bound` or `concat_or_empty`.
- Swapped physical orientation restores the observable output order before downstream predicates consume the joined
  batch.
- A probe-only post-join filter is pushed onto the probe batch before the join only when the predicate references the
  probe prefix. That is what makes the q14 edge selective before the join.
- The direct scan path is disabled for tiny task-memory tests (< 64 MiB), preserving existing wall-discriminator tests
  that intentionally expect downstream join/filter budget producers.

Core symbols and comments are topology-named; query identifiers remain in the TPC-H harness and planning docs, not in
`crates/toron/src/exec*`.

## sf=10 live results

Commands used the built release binary under a fixed 4 GB systemd scope and the ADR 360 persisted dataset:

```text
systemd-run --user --scope -p MemoryMax=4G -p MemorySwapMax=0 \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
  --query q14 --engines toron --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf/adr361_q14

systemd-run --user --scope -p MemoryMax=4G -p MemorySwapMax=0 \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
  --query q21 --engines toron --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf/adr361_q21
```

Observed rows:

| query | status | producer | requested | available | execute_ms | peak_rss_kib | spill_bytes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| q14 | pass | none | n/a | n/a | 70639.510706 | 465476 | 134225536 |
| q21 | fail | `scan output accumulator` | 1073762000 | 1073741824 | 570.118836 | 1059504 | 0 |

The old producer `persisted parquet scan output accumulator` is gone for both targeted probes.

## Correctness

The TPC-H `correctness` command is strict-only for these queries. I verified strict correctness for both affected
queries:

- q14 strict sf=0.01: pass.
- q21 strict sf=0.01: pass.
- q14 strict sf=1: pass.
- q21 strict sf=1: pass.

The unit tests cover the request/parity and shape mechanics at the engine level:

- `bound_scan_request_carries_every_bound_scan_field`
- `streaming_inner_join_scan_probe_visits_probe_batches_without_materializing_scan`
- `streaming_inner_join_left_scan_probe_restores_original_column_order`

## Verification

- `scripts/cargo-cgroup fmt`
- `scripts/cargo-cgroup test -p toron --test architecture_test` -> 2079 passed
- `scripts/cargo-cgroup test -p toron --lib` -> 1164 passed
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `git diff --check`
- no `qNN` / `QNN` references in `crates/toron/src/exec*` or `crates/toron/src/coordinator.rs`
- no ADR/temp probe markers left in `crates/toron/src` or `crates/toron-tpch/src/lib.rs`

One full lib attempt hit the existing optimizer evidence window race in
`binder::tests::rune_optimizer_readout_handles_are_read_only`; the focused rerun passed, and the subsequent full lib
rerun passed.

## Forward

q14 is green at sf=10 on this edge, but its 70s runtime and 128 MiB spill are now real perf data for the sf=10 rail.
q21 advanced to the next materialized scan accumulator. The next cut should stream that later scan edge into its
consumer or otherwise extend the scan-streaming surface to the semi/anti branch that still materializes.
