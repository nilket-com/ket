# ADR 021 Aggregate Kernel Catalog Refactor Bench Results

Date: 2026-05-16

## Command

```text
cargo bench -p toron --bench storage_execution -- --quick --storage-execution-only
```

The command completed successfully. The benchmark harness checks semantic
parity by digesting baseline execution against the PBC-enabled execution path;
it fails if digests diverge. ADR 021 adds no PBC aggregate pushdown, so
`pbc_engaged=false` is expected for aggregate rows.

The command was run twice after compilation. The first sample had two noisy
scalar aggregate rows beyond the investigation threshold; the immediate rerun
kept aggregate rows within normal quick-run variance. The rows below are from
the second run.

## Environment

- Source revision: base HEAD `ecc1b89` with ADR 021 working-tree changes
- rustc: `1.93.0 (254b59607 2026-01-19)`
- cargo: `1.93.0 (083ac5135 2025-12-15)`
- Arrow: `58.3.0`

## Fixture

Harness: `crates/toron/benches/storage_execution.rs`

Quick run rows: 8,192

Relevant columns:

- `id`: non-null `Int64`
- `amount`: nullable `Int32`
- `float64_amount`: nullable `Float64`
- `all_null_i32`: nullable `Int32`, all NULL
- `region`: nullable `Utf8` grouping key

## Results

```text
case=count_all rows=8192 baseline_ms=0.005 engaged_ms=0.002 speedup=2.94 bytes=200 cols=0 digest=b585b7acef38f2b1
case=count_nullable rows=8192 baseline_ms=0.012 engaged_ms=0.011 speedup=1.15 bytes=34184 cols=1 digest=1332fc6a1cf7072d
case=sum_nullable rows=8192 baseline_ms=0.080 engaged_ms=0.078 speedup=1.03 bytes=34184 cols=1 digest=1bec982d6b355b2a
case=avg_nullable_i32 rows=8192 baseline_ms=0.095 engaged_ms=0.095 speedup=1.01 bytes=34184 cols=1 digest=1f692ff7dcfca323
case=avg_i64 rows=8192 baseline_ms=0.107 engaged_ms=0.105 speedup=1.02 bytes=66888 cols=1 digest=775305045ba18983
case=avg_float64 rows=8192 baseline_ms=0.204 engaged_ms=0.207 speedup=0.99 bytes=66952 cols=1 digest=79946368c99693cc
case=min_nullable_i32 rows=8192 baseline_ms=0.039 engaged_ms=0.041 speedup=0.95 bytes=34184 cols=1 digest=df21dee1d8312f25
case=max_i64 rows=8192 baseline_ms=0.052 engaged_ms=0.052 speedup=1.00 bytes=66888 cols=1 digest=31be27935397d59b
case=sum_all_null rows=8192 baseline_ms=0.025 engaged_ms=0.024 speedup=1.04 bytes=34184 cols=1 digest=080471c3f6f0ea45
case=avg_all_null_i32 rows=8192 baseline_ms=0.025 engaged_ms=0.025 speedup=1.01 bytes=34184 cols=1 digest=7b18325bf610cc23
case=min_all_null_i32 rows=8192 baseline_ms=0.025 engaged_ms=0.024 speedup=1.03 bytes=34184 cols=1 digest=bbf0d5ae542fe00e
case=group_sum rows=8192 baseline_ms=0.385 engaged_ms=0.375 speedup=1.03 bytes=97608 cols=2 digest=30c3f2476114ef59
case=group_avg rows=8192 baseline_ms=0.380 engaged_ms=0.358 speedup=1.06 bytes=97608 cols=2 digest=96e2f595c5460b9a
case=group_min_max rows=8192 baseline_ms=0.438 engaged_ms=0.382 speedup=1.15 bytes=164232 cols=3 digest=63be2e17a9c628fc
case=group_null_key rows=8192 baseline_ms=0.265 engaged_ms=0.295 speedup=0.90 bytes=63688 cols=1 digest=50b794f755c6adf9
```

## Readout

- Stable aggregate rows preserve digest parity across baseline and
  PBC-enabled execution.
- Timings remain in the same range as ADR 019 / ADR 020 aggregate rows. The
  catalog refactor has no minimum-speedup gate; this artifact records
  correctness and quick-run characterization.
- No SQL behavior, plan digest, IR shape, or PBC engagement changed for the
  aggregate cases.
