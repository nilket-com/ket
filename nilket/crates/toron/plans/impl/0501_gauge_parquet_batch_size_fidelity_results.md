# impl 501 — gauge parquet batch-size fidelity re-pin

ADR 501 changes the **TPC-H Parquet harness coordinate**, not the Toron engine. The
new default is **8,192 rows**, labeled **"Parquet-optimal proxy for production's
large-batch contract"**. It is not production-equal: production scan backends emit
whole/large stored batches, whereas Parquet decoding has its own U-curve and does
not efficiently admit one giant batch.

## Change

`ParquetHarnessScan` now passes 8,192 to `ParquetRecordBatchReaderBuilder` when
`TORON_TPCH_PARQUET_BATCH_SIZE` is unset. A positive value in that environment
variable remains an explicit override for future fidelity sweeps. No planner,
executor, or query-specific behavior changed.

## Phase 1 — coordinate selection

Strict sf=8 persisted-Parquet Toron ladders used 2 GiB task memory, 2 GiB spill
reserve, one warmup, and three measured runs per rung. Raw artifacts are retained
under `target/tpch/adr501_phase1/` for 1,024, 4,096, 8,192, and 16,384 rows.

| rung | 1,024 ms | 8,192 ms | change |
|---|---:|---:|---:|
| m00 | 400.5 | 240.1 | -40.0% |
| m01 | 352.8 | 197.7 | -43.9% |
| m02 | 438.8 | 271.0 | -38.2% |
| m03 | 616.9 | 417.0 | -32.4% |
| m04 | 911.9 | 679.2 | -25.5% |
| m05 | 2,585.1 | 2,423.3 | -6.3% |
| m06 | 2,289.6 | 2,162.7 | -5.5% |
| m07 | 392.8 | 231.0 | -41.2% |
| m08 | 434.9 | 272.4 | -37.4% |
| m09 | 483.8 | 309.4 | -36.0% |
| m10 | 661.9 | 474.9 | -28.2% |

8,192 is not a special m04 setting: it improves every ladder rung relative to the
old 1,024 coordinate. The 16,384 arm remains below the old coordinate on all rungs,
but is slower than 8,192 on ten of eleven; m06 is the sole exception (2,079.0 ms vs
2,162.7 ms). That does not justify a second coordinate: 8,192 is the broad
Parquet-optimal compromise, while the gauge intentionally does not tune per query.

## Phase 2 — re-pinned ladder

The fresh 8,192-row ladder used the same strict sf=8 persisted-Parquet, 2 GiB/2 GiB,
one-warmup/three-measured coordinate. DuckDB v1.5.4 was pinned to one thread and
Polars v0.54.4 to `POLARS_MAX_THREADS=1`. Raw rows: `target/tpch/adr501_repin_8192/ladder.jsonl`.

| rung | Toron 1,024 | Toron 8,192 | DuckDB 1t | Polars 1t |
|---|---:|---:|---:|---:|
| m00 | 400.5 | 240.1 | 24.0 | 0.9 |
| m01 | 352.8 | 197.7 | 11.0 | 1.0 |
| m02 | 438.8 | 271.0 | 82.0 | 247.8 |
| m03 | 616.9 | 417.0 | 253.0 | 350.4 |
| m04 | 911.9 | 679.2 | 183.0 | 689.6 |
| m05 | 2,585.1 | 2,423.3 | 3,809.0 | 2,574.3 |
| m06 | 2,289.6 | 2,162.7 | 1,277.0 | 2,920.7 |
| m07 | 392.8 | 231.0 | 12.0 | 208.2 |
| m08 | 434.9 | 272.4 | 12.0 | 140.0 |
| m09 | 483.8 | 309.4 | 99.0 | 157.2 |
| m10 | 661.9 | 474.9 | 196.0 | 1,355.9 |

The reference engines read the identical persisted Parquet files and ran freshly at
the same sf/thread/memory coordinate. Their internal reader batch sizes are not
controlled by `TORON_TPCH_PARQUET_BATCH_SIZE`, so this is not a claim that they use
8,192-row batches. The old/new Toron column is therefore a **gauge-coordinate
effect**, never an engine delta; any resulting ranking movement has the same meaning.

All 33 fresh rows have `failure: null`. Across all 11 Toron rungs, the 8,192 output
has the identical 1,024 digest and cardinality and retains zero spill. The reference
rows agree on output cardinality and zero spill; their harnesses do not emit Toron's
Arrow IPC digest.

## Gates

- `cargo fmt --check` passed before the feature build.
- Release harness build passed with `--features polars-sql-reference`.
- `cargo test -p toron-tpch --features polars-sql-reference` passed (51 library
  tests, 38 binary tests, plus its harness tests).
- Phase 1: four complete 11-rung Toron ladders; no 8,192 regression against 1,024.
- Phase 2: complete 33-row Toron/DuckDB/Polars ladder, no failure rows; correctness
  invariants above hold.
- `cargo test --workspace --no-fail-fast` had one unrelated Wendelon timing
  assertion failure: `dispatcher_backpressures_cross_tenant_read_when_actor_limit_is_saturated`
  observed 1101 rather than 1089 under suite contention. The exact Wendelon test
  passed 3/3 in isolation. This receipt does not call the workspace suite green.

The retained override is diagnostic evidence, not a per-query tuning interface. ADR
500's conclusion remains intact: this re-pin removes a Parquet-gauge artifact from
the reported coordinate and does not claim a production-engine optimization.
