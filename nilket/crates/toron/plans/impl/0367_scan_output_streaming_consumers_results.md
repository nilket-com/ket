# ADR 367 results - scan-output streaming consumers

ADR 367 implements the first scan-output consumer cut from the ADR 366 sf=10 atlas: stream a bounded scan directly into
the global aggregate fold instead of materializing the scan output into the `scan output accumulator`.

## Runtime setup

| item | value |
| --- | --- |
| base commit | `6f6647c` (`plan: adr 367 scan output streaming consumers`) |
| dataset | `target/tpch/data/strict/sf10/manifest.json` |
| cap | `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512` for live sf=10 probes |
| mode / sf | `strict`, `10` |
| runs / warmup | `1` / `0` |

## Phase 0 - coverage contract

Bound-plan shape probe over the persisted sf=10 catalog:

| query | bucket | relevant shape | ADR 367 verdict |
| --- | --- | --- | --- |
| q01 | B2 | `GroupedAgg(Exchange(Filter(Scan(lineitem))))` | grouped aggregate, not q06 canary |
| q04 | B3 | `GroupedAgg(... LeftSemi(... Filter(Scan(lineitem))))` | scan under semi/join/exchange |
| q06 | B1 | `Project(GlobalAgg(Filter(Scan(lineitem))))` | canary, implement here |
| q07 | B3 | `GroupedAgg(... Filter(Join(... Scan(lineitem) ...)))` | scan under join chain |
| q08 | B3 | `GroupedAgg(... Filter(Join(... Scan(lineitem) ...)))` | scan under join chain |
| q09 | B3 | `GroupedAgg(... Join(... Scan(lineitem) ...))` | scan under join chain |
| q13 | B2/B3 | nested `GroupedAgg` over `LeftOuter(... Filter(Scan(orders)))` | grouped/join consumer |
| q15 | B1+B3 | one global aggregate over lineitem plus a joined grouped revenue source | mixed; q06 mechanism may cover only the B1 branch |
| q17 | B3 | `GlobalAgg(Filter(LeftOuter(... Scan(lineitem) ... GroupedAgg(Scan(lineitem)))))` | correlated join consumer |
| q18 | B2/B3 | `GroupedAgg(... LeftSemi(... GroupedAgg(Scan(lineitem))))` | grouped/semi consumer |
| q20 | B2/B3 | `LeftSemi(... LeftOuter(... GroupedAgg(Filter(Scan(lineitem)))))` | grouped/semi consumer |
| q22 | B1+B3 | global aggregate over customer plus anti-join consumer | mixed; q06 mechanism may cover only the B1 branch |

Conclusion: q06 is the only pure B1 canary. ADR 367 implements B1 only and requires a live family remeasure before any
coverage claim.

## Implementation

`ExecutionPlan::Aggregate` now tries `try_streaming_scan_global_aggregate` before the materializing fallback for global
aggregates. The helper:

- decomposes `Scan`, `Filter(Scan)`, `Project(Scan|Filter(Scan))`, and `Filter(Project(Scan|Filter(Scan)))`;
- builds the scan request through the existing `bound_scan_request` path;
- reads `ScanStream` batches directly, applies any executor-side filter/project in plan order, folds each batch into the
  global aggregate state, then drops it;
- finalizes through the same global aggregate finalizer as the materialized path.

No budget constants changed.

## Live sf=10 family remeasure

Fixed-cap command shape:

```text
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf \
    --sf 10 --mode strict --runs 1 --warmup 0 --query <query> \
    --engines toron --data-source parquet --data-root target/tpch \
    --output-dir target/tpch/perf/adr367-systemd-<query>
```

| query | status | producer / result | requested | available | execute ms | RSS KiB | verdict |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| q01 | fail | `scan output accumulator` | 1073753200 | 1073741824 | 603.4 | 1089932 | unchanged |
| q04 | fail | `scan output accumulator` | 1073762000 | 1073741824 | 614.8 | 1058652 | unchanged |
| q06 | pass | n/a | n/a | n/a | 1180.3 | 22700 | green |
| q07 | fail | `scan output accumulator` | 1073762000 | 1073741824 | 620.7 | 1058576 | unchanged |
| q08 | fail | `scan output accumulator` | 1073762000 | 1073741824 | 1193.4 | 1521028 | unchanged |
| q09 | fail | `scan output accumulator` | 1073762000 | 1073741824 | 5367.6 | 3111208 | unchanged |
| q13 | fail | `scan output accumulator` | 1073762000 | 1073741824 | 960.2 | 1346468 | unchanged |
| q15 | fail | `scan output accumulator` | 1073747200 | 1073741824 | 536.4 | 1085872 | unchanged |
| q17 | fail | `scan output accumulator` | 1073742336 | 1073741824 | 482.4 | 1087328 | unchanged |
| q18 | fail | `scan output accumulator` | 1073762000 | 1073741824 | 930.6 | 1346364 | unchanged |
| q20 | fail | `scan output accumulator` | 1073748096 | 1073741824 | 1718.4 | 1373724 | unchanged |
| q22 | fail | `scan output accumulator` | 1073762000 | 1073741824 | 1143.9 | 1370860 | unchanged |

Measured coverage: 1 of the 12 scan-output rows. This is expected from the Phase 0 bucket map: ADR 367 covers the pure
global aggregate canary and names the grouped/join/exchange consumers as follow-up work.

## Correctness

q06 passed all required correctness gates:

```text
toron-tpch correctness --query q06 --sf 0.01 --mode strict
tpch_correctness query=q06 sf=0.01 mode=strict phase=compare status=pass

toron-tpch correctness --query q06 --sf 1 --mode strict
tpch_correctness query=q06 sf=1 mode=strict phase=compare status=pass

systemd-run ... target/release/toron-tpch correctness --query q06 --sf 10 --mode strict --data-source parquet
tpch_correctness query=q06 sf=10 mode=strict phase=compare status=pass
```

Focused unit coverage:

```text
scripts/cargo-cgroup test -p toron --test scan_global_aggregate_streaming_test
```

The focused test compares streamed scan->filter->global aggregate against materialize-then-aggregate for both Float64
and Decimal inputs across multiple scan batches.

## Named next subset

The next high-leverage subset is B2: scan streams into grouped aggregate consumers. It covers q01 directly and appears
inside q13/q18/q20, and it is the natural extension of the q06 fold path from global state to grouped state. B3 remains
join/exchange consumer work and must keep the wall-mover/green distinction from ADR 367.

## Verification

Commands run:

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test scan_global_aggregate_streaming_test
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q06 --sf 0.01 --mode strict
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q06 --sf 1 --mode strict
systemd-run ... target/release/toron-tpch perf --query q06 --sf 10 --data-source parquet
systemd-run ... target/release/toron-tpch correctness --query q06 --sf 10 --data-source parquet
systemd-run ... target/release/toron-tpch perf --query q01/q04/q06/q07/q08/q09/q13/q15/q17/q18/q20/q22 --sf 10 --data-source parquet
```

All verification commands completed successfully.
