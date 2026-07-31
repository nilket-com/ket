# ADR 369 results - scan-to-join streaming

Base: `237ee1d plan: adr 369 scan to join streaming`

## Summary

ADR 369 added a shape-fenced scan-to-join-to-grouped-fold path for the B3 canary. The new path streams the preserved
scan side into a projected spilled LeftOuter build, streams the probe scan batch-by-batch, applies the join residual per
candidate, folds matched rows into the grouped aggregate, then folds unmatched preserved rows with null-filled probe
columns.

The canary passed:

- `q13` is green at sf=10 under the fixed 4 GB cap.
- `q13` persisted correctness passes at sf=10.
- The synthetic LeftOuter grouped parity test proves streamed `Scan -> LeftOuter(+residual) -> GroupedAggregate ->
  GroupedAggregate` matches the materialized path, including residual filtering and unmatched-row null-fill.

Measured B3 coverage is one new green, six unchanged scan-output walls, and one no-row long-running probe. The cut claims
only the measured coverage.

## Coverage contract

| query | bucket | sf=10 result | verdict |
| --- | --- | --- | --- |
| q04 | B3-semi/anti-or-exists | fail: scan output accumulator | unchanged, named next subset |
| q07 | B3-inner-chain | fail: scan output accumulator | unchanged, named next subset |
| q08 | B3-inner-chain | fail: scan output accumulator | unchanged, named next subset |
| q09 | B3-inner-chain | fail: scan output accumulator | unchanged, named next subset |
| q13 | B3-left-outer-simple, canary | pass | covered by scan -> LeftOuter -> grouped fold |
| q17 | B3-correlated | no row in bounded probe window | not claimed green; needs separate probe |
| q18 | B3-semi/anti-or-exists | fail: scan output accumulator | unchanged, named next subset |
| q22 | B3-semi/anti-or-exists | fail: scan output accumulator | unchanged, named next subset |

The unchanged cells still fail cleanly at the core scan-output accumulator:
`requested=1073762000`, `available=1073741824`. They are not claimed as advanced or green.

`q17` did not produce a JSON row in the bounded probe window and was stopped. It is recorded as unclassified by bytes for
this cut, not as a pass or a named memory wall.

## Implementation

- `try_streaming_left_outer_grouped_aggregate` matches the B3-left-outer-simple shape before the grouped aggregate
  materializes its child.
- The preserved scan side is streamed directly through the shared `bound_scan_request` path into
  `SpilledInnerJoinBuild`, avoiding a full preserved-side scan accumulator.
- `SpilledInnerJoinBuild::write_projected_preserved_batch_chunks` writes only retained preserved columns while keeping
  unmatched and null-key rows available for LeftOuter null-fill.
- The probe scan is streamed batch-by-batch, including scan-side filter and exchange handling, then folded directly into
  grouped aggregate state.
- Unmatched preserved rows are read back from spill pages in chunks, null-filled for the probe side, and folded without a
  full join output `Vec<RecordBatch>`.
- `try_streaming_grouped_aggregate` centralizes the grouped streaming fanout in `exec/aggregate.rs`, keeping the executor
  root below the decomposition line-count guard.

## Live sf=10 remeasure

All runs used the persisted Parquet data surface, strict mode, and a fixed 4 GB systemd memory cap.

| query | status | producer | requested | available | execute_ms | peak_rss_kib | spill_bytes | correctness |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| q04 | fail | scan output accumulator | 1073762000 | 1073741824 | 591.5 | 1059044 | 0 | not required |
| q07 | fail | scan output accumulator | 1073762000 | 1073741824 | 591.2 | 1059184 | 0 | not required |
| q08 | fail | scan output accumulator | 1073762000 | 1073741824 | 1093.3 | 1522052 | 0 | not required |
| q09 | fail | scan output accumulator | 1073762000 | 1073741824 | 4816.8 | 3111176 | 0 | not required |
| q13 | pass | n/a | n/a | n/a | 186271.1 | 1018068 | 9152992 | pass |
| q17 | no row | stopped long-running probe | n/a | n/a | n/a | n/a | n/a | not claimed |
| q18 | fail | scan output accumulator | 1073762000 | 1073741824 | 901.8 | 1346548 | 0 | not required |
| q22 | fail | scan output accumulator | 1073762000 | 1073741824 | 1142.7 | 1370940 | 0 | not required |

Existing sf=10 green controls stayed green:

| query | status | execute_ms | peak_rss_kib | spill_bytes |
| --- | --- | ---: | ---: | ---: |
| q01 | pass | 23625.6 | 23908 | 0 |
| q06 | pass | 1146.2 | 22620 | 0 |
| q14 | pass | 75437.6 | 465588 | 134225536 |
| q15 | pass | 4543.0 | 120436 | 0 |
| q16 | pass | 45339.8 | 820188 | 145425912 |
| q20 | pass | 84177.1 | 3479980 | 323398632 |

## Correctness

- `q13` strict correctness passes at sf=0.01.
- `q13` strict correctness passes at sf=1.
- `q13` persisted strict correctness passes at sf=10.
- `streaming_scan_left_outer_grouped_aggregate_matches_materialized_residual_and_null_fill` passes and covers
  residual application, null-fill, and the two grouped levels.

## Verification

All Rust commands were run through `scripts/cargo-cgroup`.

```
scripts/cargo-cgroup fmt -- --check
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --test scan_global_aggregate_streaming_test
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --test architecture_test
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron-tpch
git diff --check
```

Fixed-cap persisted sf=10 commands used:

```
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
  --query <query> --engines toron --data-source parquet --data-dir target/tpch/data

systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness --query q13 --sf 10 --mode strict \
  --data-source parquet --data-dir target/tpch/data
```

Additional hygiene:

- `git diff --check` passed.
- `rg -n "[qQ][0-9]{2}" crates/toron/src/exec.rs crates/toron/src/exec/aggregate.rs crates/toron/src/exec/join/spill.rs crates/toron/src/exec/join/streaming.rs`
  returned no matches.
- `crates/toron/src/exec.rs` is 7,460 lines, below the decomposition guard threshold.
- No memory or spill budget cap was raised.

## Next subset

The B3-left-outer-simple canary is green, but the broader B3 family did not move as a group. The next cut should classify
the remaining scan-output accumulator producers by consumer shape:

- inner-chain cells: `q07`, `q08`, `q09`
- semi/anti-or-exists cells: `q04`, `q18`, `q22`
- correlated cell: `q17`, which needs a bounded/no-row probe before claiming either a memory or time wall

The recurring producer is still the scan-output accumulator, not join-output materialization. That makes the next
high-leverage work a measured extension to those remaining scan-to-consumer edges rather than the q02 join-output family.
