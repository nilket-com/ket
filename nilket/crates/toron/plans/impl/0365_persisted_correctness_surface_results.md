# ADR 365 results - persisted correctness surface

ADR 365 added persisted Parquet data-source support to `toron-tpch correctness`.

The implementation is harness-only:

- `correctness --data-source memory|parquet|persisted --data-dir ...`
- memory remains the default and delegates to the existing `check_strict_q*` functions;
- persisted correctness reuses the ADR 360 persisted spec (`build_persisted_perf_query_spec`) and executes through the
  metadata/storage path;
- the returned batches are compared with the existing strict per-query oracles;
- no `crates/toron/src` engine code changed.

## Seam Table

All persisted rows use the same strict SQL and table-family mapping as the perf rail:
`perf_query_sql` + `persisted_query_table_names` + `build_persisted_perf_query_spec`.

| query | memory path | persisted execution input | comparator/oracle | persisted support | policy |
| --- | --- | --- | --- | --- | --- |
| q01 | `check_strict_q1` | strict SQL + persisted tables | `strict_q1_oracle` / `compare_strict_q1` | yes | normal |
| q02 | `check_strict_q02` | strict SQL + persisted tables | `strict_q02_oracle` / `compare_strict_q02` | yes | normal |
| q03 | `check_strict_q3` | strict SQL + persisted tables | `strict_q3_oracle` / `compare_strict_q3` | yes | normal |
| q04 | `check_strict_q04` | strict SQL + persisted tables | `strict_q04_oracle` / `compare_strict_q04` | yes | normal |
| q05 | `check_strict_q5` | strict SQL + persisted tables | `strict_q5_oracle` / `compare_strict_q5` | yes | normal |
| q06 | `check_strict_q6` | strict SQL + persisted tables | `strict_q6_oracle` / `compare_strict_q6` | yes | normal |
| q07 | `check_strict_q07` | strict SQL + persisted tables | `strict_q07_oracle` / `compare_strict_q07` | yes | normal |
| q08 | `check_strict_q08` | strict SQL + persisted tables | `strict_q08_oracle` / `compare_strict_q08` | yes | normal |
| q09 | `check_strict_q09` | strict SQL + persisted tables | `strict_q09_oracle` / `compare_strict_q09` | yes | normal |
| q10 | `check_strict_q10` | strict SQL + persisted tables | `strict_q10_oracle` / `compare_strict_q10` | yes | normal |
| q11 | `check_strict_q11` | strict SQL + persisted tables | `strict_q11_oracle` / `compare_strict_q11` | yes | normal |
| q12 | `check_strict_q12` | strict SQL + persisted tables | `strict_q12_oracle` / `compare_strict_q12` | yes | normal |
| q13 | `check_strict_q13` | strict SQL + persisted tables | `strict_q13_oracle` / `compare_strict_q13` | yes | normal |
| q14 | `check_strict_q14` | strict SQL + persisted tables | `strict_q14_oracle` / `compare_strict_q14` | yes | normal |
| q15 | `check_strict_q15` | strict SQL + persisted tables | `strict_q15_oracle` / `compare_strict_q15` | yes | normal |
| q16 | `check_strict_q16` | strict SQL + persisted tables | `strict_q16_oracle` / `compare_strict_q16` | yes | normal |
| q17 | `check_strict_q17` | strict SQL + persisted tables | `strict_q17_oracle` / `compare_strict_q17` | yes | `q17_sf10_oracle_policy_gap` outside `Q17_CORRECTNESS_SCALE` |
| q18 | `check_strict_q18` | strict SQL + persisted tables | `strict_q18_oracle` / `compare_strict_q18` | yes | normal |
| q19 | `check_strict_q19` | strict SQL + persisted tables | `strict_q19_oracle` / `compare_strict_q19` | yes | normal |
| q20 | `check_strict_q20` | strict SQL + persisted tables | `strict_q20_oracle` / `compare_strict_q20` | yes | normal |
| q21 | `check_strict_q21` | strict SQL + persisted tables | `strict_q21_oracle` / `compare_strict_q21` | yes | normal |
| q22 | `check_strict_q22` | strict SQL + persisted tables | `strict_q22_oracle` / `compare_strict_q22` | yes | normal |

The helper shape is `check_strict_query_with_data_source(query, sf, data_source, data_root)`.
It returns the same `CorrectnessAttempt` telemetry semantics as the existing correctness path.

## q17 Policy

q17 remains a named policy gap for sf=10 correctness:

```text
q17_sf10_oracle_policy_gap: q17 correctness requires --sf 0.02; got 10
```

ADR 365 does not silently reuse a lower-scale q17 row as sf=10 evidence.

## Live sf=10 Probes

Dataset:

```text
target/tpch/data/strict/sf10/manifest.json
manifest_fingerprint=fnv1a64:8920cf8f2e991230
lineitem row_count=59986052
```

All probes used the fixed cap:

```text
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness \
    --query <query> --sf 10 --data-source parquet --data-dir target/tpch/data
```

| query | result | interpretation |
| --- | --- | --- |
| q06 | `fail@execute`, `memory_budget_exhausted`, 4273.6 ms | Reaches the persisted execute path and exposes a named engine wall, not the old in-memory fixture wall. |
| q14 | `pass@compare`, 77469.5 ms | Persisted correctness proves the sf=10 q14 answer on the shared Parquet data. |
| q21 | `fail@execute`, `spill_budget_exhausted`, 122915.2 ms | Reaches the current q21 wall from ADR 364, not fixture generation. |

The q06 acceptance expectation in the ADR was stale. A follow-up perf probe identified the producer:

```text
BudgetExhausted { dimension: Memory, producer: "scan output accumulator",
  requested: ByteCount(1073766272), available: ByteCount(1073741824) }
```

q06 is 24,448 bytes over the 1 GiB scan-output accumulator cap. This is a current engine wall for ADR 366, not an ADR 365
plumbing failure. ADR 365's key guarantee is that persisted correctness reaches live execution and records the real wall
instead of dying at the ADR 359 fixture surface.

## Verification

Commands run:

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch adr365_persisted_correctness_surface -- --nocapture
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup build -p toron-tpch --release
git diff --check
```

Live fixed-cap probes:

```text
correctness --query q06 --sf 10 --data-source parquet --data-dir target/tpch/data
correctness --query q14 --sf 10 --data-source parquet --data-dir target/tpch/data
correctness --query q21 --sf 10 --data-source parquet --data-dir target/tpch/data
```

Additional q06 diagnostic probe:

```text
perf --sf 10 --mode strict --runs 1 --warmup 0 --query q06 \
  --engines toron --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf_adr365_q06_debug
```

## Result

ADR 365 is a behavior-neutral correctness-surface cut. It makes persisted sf=10 correctness evidence possible and leaves
newly exposed engine walls to the atlas and later engine ADRs.
