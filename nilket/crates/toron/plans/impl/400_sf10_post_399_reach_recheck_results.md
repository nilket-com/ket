# ADR 400 results - sf=10 post-399 reach re-check

## Verdict

ADR 400 is measurement-only. The post-399 sf=10 reach board did **not** move the
primary affected cells:

- `q03/q05/q10` remain `time_no_row` in the bounded 360-second probe.
- `q08` remains the same named `reoriented join build accumulator` wall.
- `q19` remains green and persisted-correct.

Fork verdict: **switch to the SQL/Rune planner-shape lever**. ADR 399's sf=1
substrate strike was real, but it did not move sf=10 reach for the affected
multi-table cells. The residual user-visible gap on the default SQL path
(`SQL/Rune ~= 2.9x` in ADR 399) is now the higher-return next lever than another
immediate sf=1 Rune substrate lap.

## Conditions

All Toron rows are live persisted sf=10 under the fixed 4 GiB scope. No matrix or
correctness-matrix cache was used.

The long Toron cells were bounded `perf-run-one` probes with
`RuntimeMaxSec=360s`:

| query | service | result | wall | CPU | memory peak | JSON row |
|---|---|---|---:|---:|---:|---|
| q03 | `adr400-q03-toron` | timeout | 6min 116ms | 5min 55.897s | 4G | none |
| q05 | `adr400-q05-toron` | timeout | 6min 330ms | 5min 57.837s | 4G | none |
| q10 | `adr400-q10-toron` | timeout | 6min 267ms | 5min 58.609s | 4G | none |

`q08` and `q19` were measured through the perf rail with `runs=1, warmup=0`.
DuckDB reference rows for `q03/q05/q10` were rerun live as context.

## Delta Table

| query | 398_status | 398_toron_ms | 400_status | 400_toron_ms | delta | producer old vs new | correctness | verdict |
|---|---|---:|---|---:|---|---|---|---|
| q03 | time_no_row | >331s | time_no_row | >360s | unchanged; longer window still no row | no row -> no row | n/a | unchanged time-bound |
| q05 | time_no_row | >327s | time_no_row | >360s | unchanged; longer window still no row | no row -> no row | n/a | unchanged time-bound |
| q10 | time_no_row | >348s | time_no_row | >360s | unchanged; longer window still no row | no row -> no row | n/a | unchanged time-bound |
| q08 | fail | 1233.725 | fail | 1145.911 | -87.814 ms to same wall | `reoriented join build accumulator` -> same producer | n/a | unchanged named wall |
| q19 | pass | 81479.078 | pass | 79864.948 | -1614.130 ms | none -> none | persisted sf=10 pass | green control, no regression |

No new pass row appeared. No non-pass row is green by implication.

## Reference Context

DuckDB reference rows, measured live under the same fixed cap:

| query | duckdb status | duckdb ms | peak RSS KiB | rows |
|---|---|---:|---:|---:|
| q03 | pass | 238.0 | 429976 | 114003 |
| q05 | pass | 178.0 | 343740 | 5 |
| q10 | pass | 756.0 | 857972 | 381105 |
| q08 | pass | 271.0 | 408064 | 2 |
| q19 | pass | 179.0 | 419784 | 1 |

Polars SQL context where available:

| query | polars_sql status | polars_sql ms | note |
|---|---|---:|---|
| q08 | not_attempted | 0.0 | explicit adapter rejects the current SQL shape |
| q19 | pass | 328.840 | peak RSS 1764088 KiB |

Ratios for completing Toron rows:

| query | Toron / DuckDB | Toron / Polars |
|---|---:|---:|
| q19 | 446.17x | 242.86x |

`q08` reaches only a wall, so its `1145.911 ms / 271.0 ms = 4.23x` is time to
failure, not a completion ratio.

## Correctness Provenance

Only `q19` completed in ADR 400. It was verified through persisted sf=10
correctness:

```text
tpch_correctness query=q19 sf=10 mode=strict phase=compare status=pass reason=none elapsed_ms=103269.5
```

`q03/q05/q10/q08` did not pass, so their correctness provenance is `n/a`.

## Old-vs-New Read

- **q03/q05/q10:** unchanged. The compounded q03 sf=1 substrate wins from ADR
  396/397/399 did not pull the sf=10 multi-table cells into the completion
  window. The probes ran at least as long as ADR 398's windows and still emitted
  no row.
- **q08:** unchanged named wall. It still reaches
  `memory_budget_exhausted` at `reoriented join build accumulator`, requested
  `1.8 GiB` / available `1 GiB`.
- **q19:** no regression. It remains green, persisted-correct, and slightly
  faster than ADR 398's control row.

## Commands

Long Toron probe pattern:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  target/release/toron-tpch perf-run-one \
    --engine toron \
    --query q03 \
    --mode strict \
    --sf 10 \
    --run-index 0 \
    --data-source parquet \
    --data-dir target/tpch/data \
  > target/tpch/adr400_q03_toron_sf10.json \
  2> target/tpch/adr400_q03_toron_sf10.err
```

The same command shape was used for `q05` and `q10` with query-specific output
paths and service names.

Perf rail for quick/control rows:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
  target/release/toron-tpch perf \
    --sf 10 \
    --mode strict \
    --runs 1 \
    --warmup 0 \
    --query q19 \
    --engines toron,polars_sql,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr400_q19_sf10 \
  > target/tpch/adr400_q19_sf10_perf.out
```

Correctness for the pass row:

```bash
target/release/toron-tpch correctness \
  --query q19 \
  --sf 10 \
  --mode strict \
  --data-source parquet \
  > target/tpch/adr400_q19_sf10_correctness.out
```

## Hygiene

- Measurement-only; no source files changed.
- Live persisted sf=10 runs only; no matrix cache.
- No cap bump.
- No `.perf.data` committed.

## Forward

ADR 400 resolves the fork: reach did not move after ADR 399. The next plan should
target the SQL/Rune planner-shape lever on q03, because the default SQL path is
still about `2.9x` slower than the hand-authored Rune shape while the immediate
Rune substrate curve has flattened. Keep `q08`'s `reoriented join build
accumulator` as a named reach-board follow-up, not the next fork-driving cut.
