# ADR 404 results - post-402 q03 substrate precision strike

Status: diagnostic sealed, no engine change.

ADR 404 re-profiled the post-402 q03 path after SQL and Rune converged to the same bound shape
(`666e0ac028036f70`). Phase 0 found a real Toron-owned residual in the materialized join-build path, but the narrow
candidate strikes available inside that producer did not produce a bankable four-way median improvement. Per the ADR
stop-line, no source change is shipped from this round.

## Phase 0 baseline

Command:

```bash
rm -rf target/tpch/perf_adr404_q03_sf1_baseline && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
  target/release/toron-tpch perf \
    --sf 1 \
    --mode strict \
    --runs 7 \
    --warmup 1 \
    --query q03 \
    --engines toron,rune,polars_sql,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr404_q03_sf1_baseline \
  > target/tpch/adr404_q03_sf1_baseline.out
```

Artifact: `target/tpch/perf_adr404_q03_sf1_baseline/latest_summary.jsonl`.

| engine | median | spread | fingerprint | rows | spill | flags |
|---|---:|---:|---|---:|---:|---|
| toron | 373.105 ms | 0.018 | `666e0ac028036f70` | 11,620 | 0 | none |
| rune | 372.137 ms | 0.025 | `666e0ac028036f70` | 11,620 | 0 | none |
| polars_sql | 102.107 ms | 0.153 | `fnv1a64:a3b5c15847833ac3` | 11,620 | 0 | none |
| duckdb | 46.000 ms | 0.739 | `fnv1a64:73b142150e59739f` | 11,620 | 0 | `unstable,schema_mismatch` |

Ratios: SQL/Rune `1.003x`; Toron/Polars `3.654x`; Rune/Polars `3.645x`.

## Profiling method

The symbol-retaining build succeeded:

```bash
cargo clean -p toron-tpch -p toron --release
CARGO_CGROUP_BACKEND=affinity CARGO_CGROUP_JOBS=4 \
  scripts/cargo-cgroup build \
    --config 'profile.release.debug=2' \
    --config 'profile.release.strip="none"' \
    -p toron-tpch \
    --features polars-sql-reference \
    --release
```

The symbol gate passed (`readelf -S target/release/toron-tpch` showed `.debug_*` and `.symtab`; `nm -C` resolved
`run_rune_q03_profile_loop`, `execute_bound`, and `build_in_memory_join_rows`). After
`kernel.perf_event_paranoid=1`, `perf record` succeeded. `kernel.kptr_restrict=1` kept some kernel callers unresolved,
but the user-space Toron/Arrow/Parquet frames resolved.

Two samples were used:

- an instrumented sample with `TORON_JOIN_BUILD_PROFILE=1` and `TORON_TPCH_PARQUET_SCAN_LEDGER=1` for cardinality and
  scan-ledger evidence;
- a clean sample with only `TORON_TPCH_PROFILE_LOOP=16` for runtime attribution, so the join-build telemetry did not
  appear as profile overhead.

Instrumented command:

```bash
rm -f target/tpch/adr404_q03_loop_perf_enabled.perf.data && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_PROFILE_LOOP=16 \
  --setenv=TORON_JOIN_BUILD_PROFILE=1 \
  --setenv=TORON_TPCH_PARQUET_SCAN_LEDGER=1 \
  perf record -D 1000 -F 999 --call-graph dwarf \
    -o target/tpch/adr404_q03_loop_perf_enabled.perf.data \
    -- target/release/toron-tpch perf-run-one \
      --engine rune \
      --query q03 \
      --mode strict \
      --sf 1 \
      --run-index 0 \
      --data-source parquet \
      --data-dir target/tpch/data \
  > target/tpch/adr404_q03_loop_perf_enabled.out
perf report --stdio -i target/tpch/adr404_q03_loop_perf_enabled.perf.data \
  --no-children --sort symbol \
  > target/tpch/adr404_q03_loop_perf_enabled_flat.txt
```

Clean sample command:

```bash
rm -f target/tpch/adr404_q03_loop_perf_clean.perf.data && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_PROFILE_LOOP=16 \
  perf record -D 1000 -F 999 --call-graph dwarf \
    -o target/tpch/adr404_q03_loop_perf_clean.perf.data \
    -- target/release/toron-tpch perf-run-one \
      --engine rune \
      --query q03 \
      --mode strict \
      --sf 1 \
      --run-index 0 \
      --data-source parquet \
      --data-dir target/tpch/data \
  > target/tpch/adr404_q03_loop_perf_clean.out
perf report --stdio -i target/tpch/adr404_q03_loop_perf_clean.perf.data \
  --no-children --sort symbol \
  > target/tpch/adr404_q03_loop_perf_clean_flat.txt
```

Both profiled paths were the intended materialized path: `materialized_path=true`, fingerprint
`666e0ac028036f70`, rows `11,620`, spill `0`. The clean sample captured 5,338 samples; the instrumented sample
captured 6,154 samples.

## Cause ledger

Clean profile-loop median: `348.451 ms` across 16 iterations. Instrumented profile-loop median:
`389.503 ms`; the difference is expected because the instrumented run records join-build cardinality and scan-batch
ledgers.

Clean `cpu_core/cycles` flat profile, top resolved user-space self frames:

| frame | self |
|---|---:|
| `hashbrown::rustc_entry::<impl ...>::rustc_entry` | 10.80% |
| `toron::exec::join::build_in_memory_join_rows` | 10.61% |
| `parquet::encodings::rle::RleDecoder::get_batch_with_dict` | 5.89% |
| `__memmove_avx_unaligned_erms` | 5.40% |
| `hashbrown::map::HashMap<K,V,S,A>::get` | 3.19% |
| `parquet::arrow::array_reader::primitive_array::coerce_i64` | 1.51% |
| `parquet::util::bit_pack::unpack32` | 1.45% |
| `arrow_select::filter::filter_native` | 1.22% |

The instrumented sample showed the same shape, but also included
`toron::exec::join::build_profile::record_join_build_profile` at 3.01%; that is measurement overhead, not a runtime
target.

Join-build profile:

| replay | strategy | key plan | build rows | unique keys | duplicate keys | max rows/key | heap overflow | build time |
|---:|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `HashPartitioned` | `int64` | 3,241,776 | 829,958 | 700,918 | 7 | 0 bytes | 126,484 us |

Scan ledger:

| table | projected/read columns | output rows | output bytes | note |
|---|---:|---:|---:|---|
| lineitem | 4 / 4 | 6,001,215 | 266,309,472 | no pbc predicate |
| orders | 4 / 4 | 1,500,000 | 36,566,400 | no pbc predicate |
| customer | 1 / 2 | 30,142 | 255,248 | pbc predicate reads `c_mktsegment` |

Classification: the 397 projection pushdown held. The dominant sampled Toron-owned residual is the join-build/hash
path: `build_in_memory_join_rows` plus hashbrown entry/probe work accounts for roughly a quarter of user-space self
samples. Parquet RLE/dictionary decode and copy/filter materialization remain meaningful peer residuals, but the scan
ledger did not show a missing projection/predicate pushdown gap comparable to 397. The join-build residual is not a
single obvious allocator bug after 396/399; it is mixed hash-table entry/probe work over 3.24M inserted rows.

## Rejected candidate strikes

Two behavior-neutral candidates were tested locally and removed before seal because they did not clear the ADR 404
acceptance bar.

### Candidate A - precompute join-build row-limit

Change tested: precompute the linear `join_build_memory_fits` row limit once per build instead of recomputing the row
width for each inserted row.

Profile-loop result:

| metric | phase0 | candidate A |
|---|---:|---:|
| loop median | 371.086 ms | 366.296 ms |
| join-build time | 126,484 us | 122,344 us |

Four-way result (`target/tpch/perf_adr404_q03_sf1_post2/latest_summary.jsonl`):

| engine | median | spread | fingerprint | flags |
|---|---:|---:|---|---|
| toron | 377.784 ms | 0.027 | `666e0ac028036f70` | none |
| rune | 378.428 ms | 0.029 | `666e0ac028036f70` | none |
| polars_sql | 105.881 ms | 0.144 | `fnv1a64:a3b5c15847833ac3` | none |
| duckdb | 38.000 ms | 0.395 | `fnv1a64:73b142150e59739f` | `unstable,schema_mismatch` |

Verdict: small local loop/profile improvement, but no bankable absolute median improvement against the fresh baseline.
Not shipped.

### Candidate B - primitive no-null build fast path

Change tested: for primitive Int32/Int64 build keys with `null_count()==0` and `rows <= u32::MAX`, skip the per-row
null check and fallible row-index conversion.

Profile-loop result:

| metric | phase0 | candidate B |
|---|---:|---:|
| loop median | 371.086 ms | 376.825 ms |
| join-build time | 126,484 us | 129,960 us |

Four-way result (`target/tpch/perf_adr404_q03_sf1_post_fastpath/latest_summary.jsonl`):

| engine | median | spread | fingerprint | flags |
|---|---:|---:|---|---|
| toron | 393.489 ms | 0.022 | `666e0ac028036f70` | none |
| rune | 374.666 ms | 0.028 | `666e0ac028036f70` | none |
| polars_sql | 101.231 ms | 0.201 | `fnv1a64:a3b5c15847833ac3` | `unstable` |
| duckdb | 36.000 ms | 0.556 | `fnv1a64:73b142150e59739f` | `unstable,schema_mismatch` |

Verdict: regression. Not shipped.

## Final outcome

No engine source changes are part of ADR 404. The banked result is the diagnostic:

- the post-402 q03 substrate residual remains around `3.6x` Polars in the fresh sf=1 artifact;
- SQL and Rune are still shape-equivalent (`666e0ac028036f70`) and move together;
- sampled profiles identify join-build/hash-table construction as the leading Toron-owned residual, but the narrow
  row-limit/null-check strikes do not move the four-way scorecard enough to bank;
- the next credible cut needs a stronger cause-level mechanism than per-row guard cleanup, likely a deeper hash-table
  representation/probe change or the broader execution-model work.

## Verification

Final source state: no `crates/toron/src` changes.

Checks run:

```text
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup fmt -- --check
```

Focused candidate tests were run while evaluating Candidate A, then removed with the unbanked source change. No
`.perf.data` is staged or committed; perf artifacts remained under ignored `target/` only during the local
investigation.
