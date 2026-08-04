# ADR 020 AVG Aggregate Kernel Bench Results

Date: 2026-05-16

## Command

```text
cargo bench -p toron --bench storage_execution -- --quick --storage-execution-only
```

The command completed successfully. The benchmark harness checks semantic
parity by digesting baseline execution against the PBC-enabled execution path;
it fails if digests diverge. ADR 020 adds no PBC aggregate pushdown, so
`pbc_engaged=false` is expected for aggregate rows.

## Environment

- Commit: `b984d57`
- rustc: `1.93.0 (254b59607 2026-01-19)`
- cargo: `1.93.0 (083ac5135 2025-12-15)`
- Arrow: `58.3.0`

## Fixture

Harness: `crates/toron/benches/storage_execution.rs`

Quick run rows: 8,192

Relevant columns:

- `amount`: nullable `Int32`
- `id`: non-null `Int64`
- `float64_amount`: nullable `Float64`
- `all_null_i32`: nullable `Int32`, all NULL
- `region`: nullable `Utf8` grouping key

## Results

```text
case=avg_nullable_i32 rows=8192 baseline_ms=0.090 engaged_ms=0.090 speedup=1.00 bytes=34184 cols=1 digest=1f692ff7dcfca323
case=avg_i64 rows=8192 baseline_ms=0.132 engaged_ms=0.134 speedup=0.99 bytes=66888 cols=1 digest=775305045ba18983
case=avg_float64 rows=8192 baseline_ms=0.201 engaged_ms=0.216 speedup=0.93 bytes=66952 cols=1 digest=79946368c99693cc
case=avg_all_null_i32 rows=8192 baseline_ms=0.028 engaged_ms=0.028 speedup=1.03 bytes=34184 cols=1 digest=7b18325bf610cc23
case=group_avg rows=8192 baseline_ms=0.442 engaged_ms=0.379 speedup=1.17 bytes=97608 cols=2 digest=96e2f595c5460b9a
```

Nearby comparison rows from the same run:

```text
case=sum_nullable rows=8192 baseline_ms=0.081 engaged_ms=0.081 speedup=0.99 digest=1bec982d6b355b2a
case=min_nullable_i32 rows=8192 baseline_ms=0.047 engaged_ms=0.046 speedup=1.02 digest=df21dee1d8312f25
case=max_i64 rows=8192 baseline_ms=0.054 engaged_ms=0.053 speedup=1.00 digest=31be27935397d59b
case=group_sum rows=8192 baseline_ms=0.401 engaged_ms=0.374 speedup=1.07 digest=30c3f2476114ef59
case=group_min_max rows=8192 baseline_ms=0.452 engaged_ms=0.366 speedup=1.23 digest=63be2e17a9c628fc
```

## Readout

- Scalar AVG over nullable `Int32`, non-null `Int64`, nullable `Float64`, and
  all-NULL `Int32` executes through the storage-backed benchmark harness with
  digest parity.
- Grouped AVG executes with digest parity over the same grouping fixture as
  the nearby `group_sum` and `group_min_max` rows.
- Throughput is in the same range as adjacent SUM / MIN / MAX aggregate rows.
  No minimum-speedup gate is attached to ADR 020; the artifact is a correctness
  and characterization record for the newly stable AVG kernel.
