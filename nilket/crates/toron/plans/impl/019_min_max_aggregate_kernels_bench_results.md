# ADR 019 MIN / MAX Aggregate Kernels Bench Results

Date: 2026-05-16

## Commands

```text
cargo bench -p toron --bench storage_execution --no-run
cargo bench -p toron --bench storage_execution -- --quick --storage-execution-only
```

Both commands completed successfully. The benchmark harness checks semantic
parity by digesting baseline execution against the PBC-enabled execution path;
it fails if digests diverge. ADR 019 adds no PBC aggregate pushdown, so
`pbc_engaged=false` is expected for all aggregate rows.

## Fixture

Harness: `crates/toron/benches/storage_execution.rs`

Quick run rows: 8,192

Relevant columns:

- `id`: non-null `Int64`
- `amount`: nullable `Int32`
- `region`: nullable `Utf8` grouping key
- `all_null_i32`: nullable `Int32`, all NULL

## Results

```text
case=sum_nullable rows=8192 baseline_ms=0.077 engaged_ms=0.076 speedup=1.01 baseline_columns=1 engaged_columns=1 digest=1bec982d6b355b2a
case=min_nullable_i32 rows=8192 baseline_ms=0.042 engaged_ms=0.039 speedup=1.08 baseline_columns=1 engaged_columns=1 digest=df21dee1d8312f25
case=max_i64 rows=8192 baseline_ms=0.046 engaged_ms=0.043 speedup=1.06 baseline_columns=1 engaged_columns=1 digest=31be27935397d59b
case=sum_all_null rows=8192 baseline_ms=0.022 engaged_ms=0.022 speedup=1.01 baseline_columns=1 engaged_columns=1 digest=080471c3f6f0ea45
case=min_all_null_i32 rows=8192 baseline_ms=0.022 engaged_ms=0.022 speedup=1.02 baseline_columns=1 engaged_columns=1 digest=bbf0d5ae542fe00e
case=group_sum rows=8192 baseline_ms=0.402 engaged_ms=0.347 speedup=1.16 baseline_columns=2 engaged_columns=2 digest=30c3f2476114ef59
case=group_min_max rows=8192 baseline_ms=0.430 engaged_ms=0.425 speedup=1.01 baseline_columns=3 engaged_columns=3 digest=63be2e17a9c628fc
```

## Readout

- Scalar nullable `Int32` MIN and scalar `Int64` MAX execute through the
  storage-backed benchmark harness with digest parity.
- All-NULL `Int32` MIN executes with digest parity, covering the no-non-NULL
  state shape used by ADR 019.
- Grouped MIN+MAX executes with digest parity over the same grouping fixture
  as the nearby `group_sum` row.
- Throughput is in the same range as adjacent SUM rows. No minimum-speedup gate
  is attached to ADR 019; the artifact is a correctness and characterization
  record for newly stable kernels.
