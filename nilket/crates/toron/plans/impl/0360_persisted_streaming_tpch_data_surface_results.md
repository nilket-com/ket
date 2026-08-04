# ADR 360 results - persisted/streaming TPC-H data surface

## Summary

Implemented the gauge-local persisted data surface in `crates/toron-tpch`:

- shared dataset under `target/tpch/data/<mode>/sf<N>/`,
- streamed `tpchgen` -> Parquet generation in bounded chunks,
- manifest with table row counts, logical bytes, schema signatures/fingerprints, file hashes, and dataset fingerprint,
- `ParquetHarnessScan` that streams Parquet batches and preserves `HarnessScan`'s PBC-before-projection semantics,
- persisted perf source wired through `--data-source parquet --data-dir ...` for both Toron and DuckDB.

No Toron engine code changed.

## Dataset

Command:

```bash
systemd-run --user --scope -p MemoryMax=4G -p MemorySwapMax=0 \
  target/release/toron-tpch prepare \
  --data-source parquet --mode strict --sf 10 --data-dir target/tpch/data
```

Result:

```text
prepared persisted tpch mode=strict sf=10 root=target/tpch/data/strict/sf10 manifest=target/tpch/data/strict/sf10/manifest.json tables=8
```

Manifest fingerprint: `fnv1a64:8920cf8f2e991230`.

Table summary:

| table | rows | logical bytes | parquet size |
| --- | ---: | ---: | ---: |
| customer | 1,500,000 | 403,845,128 | 246 MB |
| lineitem | 59,986,052 | 8,520,711,296 | 2.2 GB |
| nation | 25 | 1,049,272 | 1.6 KB |
| orders | 15,000,000 | 2,014,523,432 | 1.2 GB |
| part | 2,000,000 | 320,528,552 | 98 MB |
| partsupp | 8,000,000 | 288,176,256 | 183 MB |
| region | 5 | 524,632 | 852 B |
| supplier | 100,000 | 22,049,280 | 16 MB |

Dataset size on disk: `3.9G`.

This closes ADR 359's first wall: sf=10 data generation no longer builds the full in-memory `lineitem` fixture before a query can start.

## Scan Semantics

`ParquetHarnessScan` now:

- uses Parquet projection for non-PBC scans,
- compiles PBC bytecode to collect referenced columns,
- reads `projection union pbc_columns` for PBC scans,
- inflates missing untouched columns as null arrays only for original-ordinal PBC compatibility,
- appends the requested projection to the PBC program, matching `HarnessScan`'s PBC-before-projection behavior,
- streams row groups/batches through `ScanStream::new`.

The persisted scan has a gauge-local output safety guard at the same 16x effective memory budget used by the streaming output guards: if a scan would yield more than 1 GiB of batches to the materializing executor, it returns a named `BudgetExhausted { dimension: Memory, producer: "persisted parquet scan output accumulator", ... }` row instead of letting the process OOM. This is not an engine fix; it makes the sf=10 scan/materialization wall observable.

## Tests

Passed:

```bash
scripts/cargo-cgroup fmt -p toron-tpch -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch adr360_persisted_data_surface -- --nocapture
git diff --check
```

Focused tests:

- persisted manifest is shared and idempotent,
- persisted q06 matches the in-memory digest at sf=0.01,
- DuckDB export uses the shared dataset manifest.

## Pilot

All commands ran release binaries under fixed 4 GB via `systemd-run --user --scope -p MemoryMax=4G -p MemorySwapMax=0`.

| query | engine | status | reason / note | execute ms | peak RSS KiB |
| --- | --- | --- | --- | ---: | ---: |
| q06 | toron | pass | persisted Parquet path, digest recorded | 2066.8 | 3,127,172 |
| q06 | duckdb | pass | shared Parquet dataset | 88.0 | 194,208 |
| q14 | toron | fail | `memory_budget_exhausted`, producer `persisted parquet scan output accumulator`, requested 1,073,790,848 / available 1,073,741,824 | 645.0 | 1,077,544 |
| q14 | duckdb | pass | shared Parquet dataset | 122.0 | 402,856 |
| q21 | toron | fail | `memory_budget_exhausted`, producer `persisted parquet scan output accumulator`, requested 1,073,762,000 / available 1,073,741,824 | 612.1 | 1,058,628 |
| q21 | duckdb | pass | shared Parquet dataset | 545.0 | 729,968 |

Before the scan-output guard, q14/q21 Toron were OOM-killed with no row at 4 GB. After the guard, both emit named memory rows. That satisfies the 360 "measurable, not necessarily green" bar and names the first sf=10 engine-facing wall for 359-redux.

## Follow-Up

ADR 360 is surface-only. The next sf=10 atlas should start from these named facts:

- the persisted data surface is valid and shared across engines,
- q06 Toron reaches green at sf=10 under 4 GB,
- q14/q21 Toron now fail cleanly at the scan-output/materialize boundary,
- DuckDB passes q06/q14/q21 from the same files, so the remaining Toron reds are engine/executor work, not data parity or generation.

