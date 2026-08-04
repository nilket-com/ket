# ADR 408 results - q03 sf10 time-cell diagnostic

Status: diagnostic + instrumentation. No execution optimization is shipped.

ADR 408 asked whether the q03 sf=10 timeout was the same materialized work as sf=1 scaled up,
a planner/Rune divergence, or a structural execution loop. The answer is structural: q03 sf=10 is
CPU-bound on repeated `lineitem` scan/repartition work in the partitioned streamed-inner
scan-build path. The query rereads the projected `lineitem` build once per active partition group;
in a 180s partial run it opened 31 `lineitem` scans: 30 full passes plus one partial pass before a
row was emitted.

The implementation change in this ADR is only timeout-safe diagnostic plumbing:

- `TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=<path>` appends Parquet scan start/batch progress JSONL;
- `TORON_JOIN_BUILD_PROFILE_PATH=<path>` appends completed join-build profiles JSONL.

Both are off by default and do not change execution semantics. They exist because the existing
in-memory ledgers are lost when a scoped sf=10 run is killed by the timeout.

## Clean rebuild

ADR 407 left a warning that `target/release/toron-tpch` had been built from the rejected radix
candidate. ADR 408 rebuilt the release binary from clean source before probing:

```bash
CARGO_CGROUP_BACKEND=affinity CARGO_CGROUP_JOBS=12 \
  scripts/cargo-cgroup build -p toron-tpch --features polars-sql-reference --release
```

Release binary timestamp:

```text
2026-07-09 01:39:21 -0500 target/release/toron-tpch
```

The profiling binary was built separately with debug symbols:

```bash
CARGO_TARGET_DIR=target/adr408_profile CARGO_BUILD_JOBS=4 \
  CARGO_PROFILE_RELEASE_DEBUG=true \
  CARGO_PROFILE_RELEASE_STRIP=false \
  CARGO_PROFILE_RELEASE_LTO=false \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
  cargo build -p toron-tpch --features polars-sql-reference --release
```

`file`, `readelf -S`, and `nm -C` confirmed debug info, `.symtab`, and resolved
`build_in_memory_join_rows`, `probe_in_memory_join_rows`, `execute_bound`,
`ParquetHarnessScan::scan`, and `execute_bound_profile_iteration`.

## Plan proof

Command:

```bash
target/release/toron-tpch q03-plan-proof --sf 10 --mode strict \
  --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr408_q03_plan_proof_sf10.json
```

Control:

```bash
target/release/toron-tpch q03-plan-proof --sf 1 --mode strict \
  --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr408_q03_plan_proof_sf1.json
```

| scale | SQL fingerprint | Rune fingerprint | plans differ? | verdict |
|---:|---|---|---|---|
| sf=1 | `666e0ac028036f70` | `666e0ac028036f70` | false | 402 closure holds |
| sf=10 | `666e0ac028036f70` | `666e0ac028036f70` | false | same timed path |

The compact shape is the same at sf=1 and sf=10. The hot shape is:

```text
Sort
  Exchange(parts=1)
    Aggregate(groups=[l_orderkey,o_orderdate,o_shippriority])
      Join(HashPartitioned)
        left: Exchange(parts=4)
          Join(HashPartitioned)
            left: Exchange(parts=4) Filter(customer c_mktsegment = BUILDING)
            right: Exchange(parts=4) Filter(orders o_orderdate < 1995-03-15)
        right: Exchange(parts=4) Filter(lineitem l_shipdate > 1995-03-15)
```

So ADR 408 is not a SQL-vs-Rune planner-shape problem. Both frontends drive the same executor plan.

## Fresh timeout probes

The first systemd `RuntimeMaxSec` probe confirmed no row but did not preserve `/usr/bin/time`
output. The durable probes used an inner `timeout`:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  /usr/bin/time -v timeout -k 10s 360s \
    target/release/toron-tpch perf-run-one \
      --engine toron \
      --query q03 \
      --mode strict \
      --sf 10 \
      --run-index 0 \
      --data-source parquet \
      --data-dir target/tpch/data \
  > target/tpch/adr408_q03_toron_sf10_timeout_probe.json \
  2> target/tpch/adr408_q03_toron_sf10_timeout_probe.err
```

Same command with `--engine rune` for the same-executor control.

| engine | stdout row | exit | wall | user | sys | CPU | max RSS | verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| toron SQL | 0 bytes | 124 | 6:00.20 | 318.15s | 42.44s | 100% | 1,615,864 KB | time/no-row |
| rune | 0 bytes | 124 | 6:00.11 | 319.02s | 41.90s | 100% | 1,616,880 KB | time/no-row |

Classification from this row:

- not a 4 GiB memory wall: RSS is about 1.54 GiB;
- not reclaim throttling: CPU is 100%;
- not SQL lowering: Rune is identical;
- not sf=1 substrate linear scaling: sf=1 is sub-second while sf=10 burns six CPU-minutes with no row.

## Perf attribution

Two delayed profiles were collected. The first used default cycles and produced usable symbols but
was noisy on hybrid PMU events. The cleaner directional sample used `cpu-clock`:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  timeout -k 10s 150s \
    perf record -e cpu-clock -D 30000 -F 199 --call-graph dwarf \
      -o target/tpch/adr408_q03_sf10_toron_cpuclock.perf.data -- \
      target/adr408_profile/release/toron-tpch perf-run-one \
        --engine toron \
        --query q03 \
        --mode strict \
        --sf 10 \
        --run-index 0 \
        --data-source parquet \
        --data-dir target/tpch/data
```

The capture recorded 23,336 samples and lost 13.00%, so it is directional rather than a final
micro-optimization profile. It is still sufficient for classifying the wall because the top frames
agree with the scan-progress artifact.

Top relevant self frames:

| frame | self share | interpretation |
|---|---:|---|
| `VecDeque::retain` | 9.10% | spill LRU/cache churn during repeated work |
| `partition_bucket_if_non_null_with_scratch` | 8.89% | repartition/key bucketing |
| `partition_bucket_with_scratch` | 8.27% | repartition/key bucketing |
| `_int_malloc` | 5.03% | allocation churn |
| `RleDecoder::get_batch_with_dict` | 4.17% + 1.48% | repeated Parquet decode |
| `__memset_avx2_unaligned_erms` | 3.75% | allocation/materialization |
| `encode_partition_key_value` | 2.49% | repartition key encoding |
| `Executor::repartition_exchange_batch` | 2.23% | exchange repartition |
| `partition_streaming_join_batch_active_group_with_buckets` | 1.21% | partitioned streamed join routing |

The stack is not a single hash-table hot spot like ADR 405, nor a one-time decode floor. It is
repartitioning plus repeated scan/decode/allocation work.

## Timeout-safe progress evidence

The progress run used the new off-by-default JSONL hooks and a shorter timeout:

```bash
rm -f target/tpch/adr408_q03_sf10_scan_progress.jsonl \
      target/tpch/adr408_q03_sf10_join_build_profiles.jsonl

systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr408_q03_sf10_scan_progress.jsonl \
  --setenv=TORON_JOIN_BUILD_PROFILE_PATH=target/tpch/adr408_q03_sf10_join_build_profiles.jsonl \
  /usr/bin/time -v timeout -k 10s 180s \
    target/release/toron-tpch perf-run-one \
      --engine toron \
      --query q03 \
      --mode strict \
      --sf 10 \
      --run-index 0 \
      --data-source parquet \
      --data-dir target/tpch/data
```

Timeout row:

| wall | user | sys | CPU | max RSS | exit |
|---:|---:|---:|---:|---:|---:|
| 3:00.17 | 130.74s | 48.70s | 99% | 1,614,188 KB | 124 |

Progress summary from `target/tpch/adr408_q03_sf10_scan_progress.jsonl`:

| table | scan starts | batches | raw rows | output rows | raw bytes | output bytes |
|---|---:|---:|---:|---:|---:|---:|
| customer | 1 | 1,465 | 1,500,000 | 300,276 | 37,415,164 | 2,542,848 |
| orders | 1 | 1 | 1,024 | 1,024 | 24,960 | 24,960 |
| lineitem | 30 full + 1 partial | 1,806,380 | 1,849,706,360 | 1,849,706,360 | 82,081,050,880 | 82,081,050,880 |

The first 30 `lineitem` scans each read the full projected table; the last `lineitem` scan was
partial when the timeout killed the process:

| scan | table | batches | raw rows | output rows | read columns | pbc |
|---:|---|---:|---:|---:|---|---|
| 1 | lineitem | 58,581 | 59,986,052 | 59,986,052 | `[1,2,3,8]` | none |
| 2 | lineitem | 58,581 | 59,986,052 | 59,986,052 | `[1,2,3,8]` | none |
| 3 | lineitem | 58,581 | 59,986,052 | 59,986,052 | `[1,2,3,8]` | none |
| ... | lineitem | 58,581 | 59,986,052 | 59,986,052 | `[1,2,3,8]` | none |
| last | lineitem | 48,950 | 50,124,800 | 50,124,800 | `[1,2,3,8]` | none |

No `TORON_JOIN_BUILD_PROFILE_PATH` file was produced. That is expected for this path: q03 is in
the partitioned streamed-inner scan-build path, which writes a spilled build per group rather than
completing an in-memory `build_in_memory_join_rows` profile before the timeout.

## Classification

ADR 408 classified the wall as case (a): over-cap partitioned scan-build routing with a
superlinear source re-scan loop.

Evidence:

- SQL and Rune are shape-identical at sf=10 (`666e0ac028036f70`).
- Both frontends time out at six minutes with 100% CPU and about 1.54 GiB RSS.
- The delayed profile is dominated by partition bucketing, exchange repartition, repeated decode,
  and allocation churn.
- The timeout-safe progress hook proves `lineitem` is read repeatedly: about 30.8 passes in 180s,
  1.85B rows, and 82 GB of projected Parquet/Arrow data from a 60M-row table.
- The current plan's `lineitem` side is `Exchange(parts=4)` under the top hash-partitioned join,
  and the executor's partitioned streamed-inner scan-build machinery scans the build source once per
  active partition group to respect the spill/resident budget.

Rejected alternatives:

- **Memory wall / reclaim throttle:** RSS is well below 4 GiB and CPU is 99-100%.
- **SQL planner shape:** Rune is identical and times out identically.
- **One-time decode/materialization floor:** the scan-progress artifact shows tens of full source
  reads, not one.
- **Small hash-table substrate strike:** ADR 407 already closed the isolated build-only partitioning
  route; this wall appears before any in-memory join-build profile completes.

## Stop-line and named follow-on

No bounded execution fix is shipped in ADR 408.

The obvious fix is not another sf=1 hash-table cleanup. ADR 409 should first decide between two
execution-side forks:

1. **Adaptive build-side decision.** The repeated source is the top join's `lineitem` side, but the
   other side is the derived `(customer ⋈ orders)` result. If that derived side is actually small
   enough at runtime to fit the build accumulator, the cheapest fix is to build it once in memory
   and stream/probe `lineitem` once. Phase 0 must measure that derived-side size and explain why
   today's reoriented scan-build path chose `lineitem` as the repeatedly scanned build source
   (unknown cardinality at routing time, conservative estimate, scan-shaped-build precondition, or
   a real size constraint).
2. **One-pass partition reservoir.** If the derived side cannot safely become the build side, then
   read/filter/partition the `lineitem` build source once, persist or otherwise route the active
   partition data, and process partition groups without rereading the original Parquet source once
   per group.

Either design must preserve:

- the 4 GiB memory cap and the 512 MiB spill budget semantics;
- group/partition determinism and q03 order correctness;
- the existing q19/q08 spill/re-read wins;
- probe-row output order and matched-build semantics if the probe side is also partitioned;
- generic behavior for the q03/q05/q10/q12 streamed-inner family, not a q03-branded shortcut.

Named next ADR: q03 streaming-completion fork: adaptive derived-side build if measured safe, else
one-pass partition reservoir for partitioned streamed-inner scan-build. q03 is the driver;
q05/q10/q12 are the likely family coverage set; q08/q19/q04 remain controls.

## Verification

Commands run:

```text
cargo fmt --all
CARGO_CGROUP_BACKEND=affinity CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup build -p toron-tpch --features polars-sql-reference --release
CARGO_BUILD_JOBS=12 cargo check -p toron --lib
CARGO_BUILD_JOBS=12 cargo check -p toron-tpch --features polars-sql-reference
CARGO_BUILD_JOBS=12 cargo test -p toron-tpch --features polars-sql-reference --lib --no-fail-fast
CARGO_BUILD_JOBS=12 cargo test -p toron build_rows --lib --no-fail-fast
CARGO_BUILD_JOBS=12 cargo test -p toron --no-fail-fast
CARGO_BUILD_JOBS=12 cargo test -p toron-tpch --features polars-sql-reference --no-fail-fast
CARGO_BUILD_JOBS=12 cargo test -p toron --test architecture_test --no-fail-fast
```

Test results:

```text
toron-tpch lib: 26 passed; 0 failed
toron build_rows: 15 passed; 0 failed
toron full: passed; 0 failed
toron-tpch full with polars-sql-reference: passed; 0 failed
toron architecture_test: 2079 passed; 0 failed
```

The join-build JSONL path was smoke-tested on q03 sf=0.01:

```bash
TORON_JOIN_BUILD_PROFILE_PATH=target/tpch/adr408_join_profile_smoke.jsonl \
  target/release/toron-tpch perf-run-one --engine rune --query q03 \
    --mode strict --sf 0.01 --run-index 0 \
    --data-source parquet --data-dir target/tpch/data
```

It wrote a profile line for replay `1` with `strategy=HashPartitioned`, `key_plan=int64`,
`build_rows=32260`, `unique_keys=8277`, and `max_rows_per_key=7`.

The result is diagnostic and off-by-default instrumentation only. No correctness-bearing execution
rewrite was made in ADR 408.

Artifacts:

- `target/tpch/adr408_q03_plan_proof_sf{1,10}.json`;
- `target/tpch/adr408_q03_{toron,rune}_sf10_timeout_probe.err`;
- `target/tpch/adr408_q03_sf10_toron_cpuclock.perf.data` and rendered reports;
- `target/tpch/adr408_q03_sf10_scan_progress.jsonl`;
- `target/tpch/adr408_q03_sf10_progress_probe.err`.

The `.perf.data` and JSONL artifacts are under `target/` and are not staged.
