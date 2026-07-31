# ADR 368 results - scan-to-grouped-fold streaming

Base: `84e7962 plan: adr 368 scan to grouped fold streaming`

## Summary

ADR 368 added a shape-fenced scan-to-grouped-fold path: `ScanStream` batches flow through the existing scan
filter/project operations, optionally through exchange partitioning, and then directly into the grouped aggregate fold.
The path bypasses `materialize_scan_stream -> Vec` for the matched grouped-aggregate edge.

The canary passed:

- `q01` is green at sf=10 under the fixed 4 GB cap.
- `q01` persisted correctness passes at sf=10.
- The synthetic grouped parity test proves streamed `Scan -> Filter -> Exchange -> GroupedAggregate` matches the
  materialized path for Float64 and Decimal, preserving exchange partition visitation and final group output order.

Measured B2 coverage is 3 new greens and 3 unchanged scan-output walls. The cut claims only the measured coverage.

## Coverage contract

| query | bucket | sf=10 result | verdict |
| --- | --- | --- | --- |
| q01 | B2-exchange, canary | pass | covered by scan -> exchange -> grouped fold |
| q13 | B2/B3 mixed | fail: scan output accumulator | unchanged, named next subset |
| q15 | mixed revenue grouped path | pass | covered by live remeasure |
| q18 | B2/B3 mixed | fail: scan output accumulator | unchanged, named next subset |
| q20 | mixed grouped path | pass | covered by live remeasure |
| q22 | B2/B3 mixed | fail: scan output accumulator | unchanged, named next subset |

The unchanged cells still fail cleanly at the core scan-output accumulator:
`requested=1073762000`, `available=1073741824`. They are not claimed as advanced or green.

## Implementation

- `try_streaming_scan_grouped_aggregate` runs before the materializing grouped-aggregate fallback.
- The direct scan path uses the shared `bound_scan_request`, so ScanRequest parity stays covered by the existing drift
  test.
- `apply_streaming_scan_input_ops` is shared with the global scan-fold path so scan-side filter/project behavior remains
  identical.
- Exchange handling partitions each streamed batch with `repartition_exchange_batch` and folds each partitioned batch
  immediately.
- The exchange matcher is conservative: if the exchange key does not match the group key, it only streams
  order-insensitive grouped aggregates.
- Empty inputs fold a schema-bearing empty batch so grouped output schema behavior stays aligned with the materialized
  path.

## Live sf=10 remeasure

All runs used the persisted Parquet data surface, strict mode, and a fixed 4 GB systemd memory cap.

| query | status | producer | requested | available | execute_ms | peak_rss_kib | spill_bytes | correctness |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| q01 | pass | n/a | n/a | n/a | 23702.9 | 24120 | 0 | pass |
| q13 | fail | scan output accumulator | 1073762000 | 1073741824 | 952.7 | 1345872 | 0 | not required |
| q15 | pass | n/a | n/a | n/a | 4418.9 | 125436 | 0 | pass |
| q18 | fail | scan output accumulator | 1073762000 | 1073741824 | 882.0 | 1346728 | 0 | not required |
| q20 | pass | n/a | n/a | n/a | 84070.5 | 3479648 | 323398632 | pass |
| q22 | fail | scan output accumulator | 1073762000 | 1073741824 | 1130.9 | 1370504 | 0 | not required |

Existing sf=10 green controls stayed green:

| query | status | execute_ms | peak_rss_kib | spill_bytes |
| --- | --- | ---: | ---: | ---: |
| q06 | pass | 1207.6 | 22556 | 0 |
| q14 | pass | 75821.6 | 465740 | 134225536 |
| q16 | pass | 45804.5 | 827352 | 145425912 |

## Correctness

- `q01` strict correctness passes at sf=0.01.
- `q01` strict correctness passes at sf=1.
- `q01`, `q15`, and `q20` persisted strict correctness pass at sf=10.
- `streaming_scan_exchange_grouped_aggregate_matches_materialized_float64_and_decimal_order` passes for Float64 and
  Decimal.

## Verification

All Rust commands were run through `scripts/cargo-cgroup`.

```
scripts/cargo-cgroup fmt -- --check
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --test scan_global_aggregate_streaming_test
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --lib
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --test architecture_test
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron-tpch
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q01 --sf 0.01 --mode strict
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q01 --sf 1 --mode strict
git diff --check
```

Fixed-cap persisted sf=10 commands were run with:

```
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
  --query <query> --engines toron --data-source parquet --data-dir target/tpch/data

systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness --query <query> --sf 10 --mode strict \
  --data-source parquet --data-dir target/tpch/data
```

Additional hygiene:

- `git diff --check` passed.
- `rg -n "[qQ][0-9]{2}" crates/toron/src/exec.rs crates/toron/src/exec/aggregate.rs crates/toron/tests/scan_global_aggregate_streaming_test.rs`
  returned no matches.
- `crates/toron/src/exec.rs` is 7,496 lines, below the decomposition guard threshold.

## Next subset

The remaining scan-output cells in this measured subset are `q13`, `q18`, and `q22`. They are mixed grouped/join or
exchange shapes that still materialize a scan output accumulator. The next cut should classify those remaining consumers
before changing code, using the same coverage-contract discipline as ADR 367 and ADR 368.
