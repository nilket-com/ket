# ADR 392 results - substrate-ceiling measurement at completing scale

## Summary

ADR 392 reused the ADR 391 Rune q03 perf instrument to ask whether a completing Rune-authored q03 can put a useful
number on Toron's execution substrate ceiling against DuckDB. It was diagnostic-only: no `crates/toron/src` engine code
changed.

The answer is still reach-limited, not substrate-measured:

- At sf=1 persisted, SQL and Rune complete and are stable, but DuckDB is too fast/noisy for a trustworthy ratio.
- The planned sf=3 escalation gives a stable DuckDB denominator, but Rune fails at `scan output accumulator` before it
  can produce a correct row.
- Therefore ADR 392 does **not** produce a trustworthy `Rune/DuckDB` substrate ceiling. It proves the next arc is still
  reach-first for this Rune q03 shape: fix the scan-output accumulator before using Rune/DuckDB as the substrate ratio.

## Setup

| item | value |
| --- | --- |
| commit | `c71afa2` (`plan: adr 392 substrate ceiling completing scale`) |
| binary | `target/release/toron-tpch`, release + debuginfo build |
| DuckDB | `v1.5.4 (Variegata) 08e34c447b` |
| cap | `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512` |
| sf=1 data | `target/tpch/data/strict/sf1/manifest.json` |
| sf=3 data | `target/tpch/data/strict/sf3/manifest.json` |
| primary command | `perf --sf 1 --runs 7 --warmup 1 --query q03 --engines toron,rune,duckdb --data-source parquet` |
| escalation command | `perf --sf 3 --runs 7 --warmup 1 --query q03 --engines rune,duckdb --data-source parquet` |

The attempted full sf=3 three-way run was stopped after the Toron SQL warmup remained active for about nine minutes.
That row is treated as censored for this diagnostic rather than allowed to block the Rune/DuckDB escalation.

## Correctness Gate

SQL q03 passed the persisted sf=1 oracle/order correctness command before ratios were interpreted:

```text
tpch_correctness query=q03 sf=1 mode=strict phase=compare status=pass reason=none elapsed_ms=4517.9
```

Rune q03's sf=1 perf raw rows reached the ADR 391 oracle/order gate:

```text
engine=rune phase=compare status=pass output_row_count=11620
digest=ArrowIpcDigest([244, 108, 71, 138, 105, 116, 66, 111, 240, 157, 107, 220, 177, 28, 213, 242, 108, 204, 245, 76, 45, 103, 140, 194, 168, 149, 193, 61, 46, 22, 218, 182])
```

At sf=3, Rune does not reach compare:

```text
engine=rune phase=execute status=fail reason=memory_budget_exhausted
diagnostic_message=BudgetExhausted { dimension: Memory, producer: "scan output accumulator", requested: ByteCount(1073790848), available: ByteCount(1073741824) }
```

So there is no sf=3 Rune correctness row and no sf=3 substrate ratio.

## sf=1 Measurement

Release, persisted Parquet, fixed 4 GB, runs=7, warmup=1:

| engine | status | execute_ms median | min | max | spread | correctness | producer / profile |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| sql | pass | 3027.4 | 2987.3 | 3065.7 | 0.026 | oracle/order pass | release, spill 0 |
| rune | pass | 1580.6 | 1576.3 | 1596.3 | 0.013 | oracle/order pass | release, spill 0 |
| duckdb | pass | 37.0 | 35.0 | 58.0 | 0.622 | reference | `unstable`, `schema_mismatch` |

Ratios from sf=1 medians:

| ratio | value | verdict |
| --- | ---: | --- |
| SQL / Rune | 1.92x | trustworthy: both rows are stable and correct |
| Rune / DuckDB | 42.72x | not trustworthy: DuckDB row is unstable |
| SQL / DuckDB | 81.82x | not trustworthy: DuckDB row is unstable |

The sf=1 run answers the logical-shape question but not the substrate-ceiling question. The Rune-authored logical shape
is about 1.9x faster than the SQL-produced shape at this scale, but the DuckDB denominator is below the stable
measurement floor for q03.

## sf=3 Escalation

sf=3 was run because the sf=1 DuckDB row was unstable. The full three-way run was impractical because the SQL warmup
did not complete in about nine minutes, so the escalation was narrowed to the headline denominator pair: Rune and DuckDB.

Release, persisted Parquet, fixed 4 GB, runs=7, warmup=1:

| engine | status | execute_ms median | min | max | spread | correctness | producer / profile |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| rune | fail | 1486.4 | 1463.1 | 1492.0 | 0.019 | n/a | `scan output accumulator`, requested 1,073,790,848 / 1,073,741,824 |
| duckdb | pass | 93.0 | 89.0 | 104.0 | 0.161 | reference | stable, spill 0 |

The sf=3 DuckDB row is stable enough to be a reference denominator, but Rune fails before compare. The
`1486.4 / 93.0 = 15.98x` value is only time-to-memory-failure over DuckDB, not a substrate ratio.

## Verdict

**Verdict: reach-first. The q03 substrate ceiling remains unmeasured.**

ADR 392 was designed to produce a stable `Rune/DuckDB` number if Rune completed at a meaningful scale. It did not:

- sf=1 completes, but the DuckDB denominator is unstable.
- sf=3 gives a stable DuckDB denominator, but Rune hits the scan-output accumulator.

The strongest measured numbers are therefore:

| signal | value | meaning |
| --- | ---: | --- |
| sf=1 SQL / Rune | 1.92x | SQL logical shape is materially slower than the Rune-authored shape at a completing scale |
| sf=1 Rune / DuckDB | 42.72x | provisional only; denominator unstable |
| sf=3 Rune / DuckDB | n/a | Rune is reach-blocked, not slow-and-correct |

The next arc should fix the Rune q03 scan-output reach wall before reusing the Rune/DuckDB ratio as a substrate ceiling.
If a later Rune q03 shape completes at sf=3 or sf=10, rerun the same three-way table and interpret `Rune/DuckDB` as the
substrate number. Until then, the substrate-vs-DuckDB question is still blocked by reach, not answered by performance.

No symbol-retaining profile was run for Rune in ADR 392. The profile branch is for a slow, correct Rune row; sf=3 Rune is
a fast named memory failure, and sf=1's reference denominator is unstable.

## Verification

```text
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
target/tpch/data/strict/sf1/manifest.json present
target/release/toron-tpch prepare --data-source parquet --mode strict --sf 3 --data-dir target/tpch/data
target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 --engines toron,rune,duckdb --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf_adr392_q03
target/release/toron-tpch perf --sf 3 --mode strict --runs 7 --warmup 1 --query q03 --engines rune,duckdb --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf_adr392_q03_sf3_rune_duckdb
target/release/toron-tpch correctness --query q03 --sf 1 --data-source parquet --data-dir target/tpch/data
git diff --check
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron --no-fail-fast
```
