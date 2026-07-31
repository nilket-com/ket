# ADR 356 follow-up results - post-432 article performance baseline

## Verdict

The post-432 repeated-run rail now has a retained classification for every planned engine/cell:

- Toron: 22/22 cells have stable seven-run samples.
- DuckDB: 21/22 cells have stable seven-run samples. q11 remains timing-unstable after the one allowed retry and is
  reported as such; it is not presented as a clean baseline point.
- Polars SQL: all five capability-pass cells have stable seven-run samples. q03 remains value-validated by its
  dedicated ADR 394 gate; q01, q06, q14, and q19 remain reach+schema-only peer timings.
- All 49 retained samples pass the raw-row identity and invariance audit. All 22 Toron plan fingerprints match the
  post-432 ADR 415 redux, and all five Polars fingerprints match the ADR 394 capability census.
- Four cells exercised the one-retry protocol. Toron q11, Toron q18, and DuckDB q02 stabilized. DuckDB q11 did not.

This is the first article-grade repeated-run baseline for the sealed post-432 board. It is not timing-comparable with
ADR 415's one-shot, unpinned correctness census and makes no regression claim from those rows.

## Conditions and provenance

| item | value |
|---|---|
| engine seal commit | `7488ddff01b6dd9c04f2dd9eec0223b65f022529` |
| committed measurement plan | `30748cc927b9eed6b49ea5a7b458dfbb61848b97` |
| measurement window | 2026-07-13 13:16:15-16:13:44 CDT |
| binary SHA-256 | `cca277b41ca6bf87dc401e0bb97cbdaff896d7cf751bd4859ac5539eda7a02ac` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| DuckDB | `v1.5.4 (Variegata) 08e34c447b` |
| Polars | `0.54.4` |
| host | 12th Gen Intel Core i9-12900K, 16 cores / 24 threads, 61 GiB RAM |
| kernel | Linux `7.0.0-27-generic` |
| swap | none |
| frequency governor | `powersave`; Intel HWP may still vary clocks and turbo, so the spread rail remains authoritative |
| CPU placement | `AllowedCPUs=0-1`; CPUs 0 and 1 are SMT siblings on physical core 0 |
| CPU quota | `CPUQuota=150%` |
| memory | `MemoryHigh=infinity`, `MemoryMax=infinity` |
| remaining scope | `TasksMax=512`, `RuntimeMaxSec=3600` |
| data | persisted strict sf=10 Parquet at `target/tpch/data` |
| join-spill scratch | `target/tpch/join_spill` on ext4; empty before and after every attempt |
| evidence root | `target/tpch/article_perf_post432/` |
| disk preflight | 117 GiB free |

The release binary was built once from the sealed engine tree and reused throughout. The newer HEAD contains only the
committed measurement plan. Each of the 49 planned engine/cells ran in a distinct user systemd scope; four allowed
retries bring the persisted attempt count to 53.

Each attempt used one discarded cache-conditioning warmup and seven measured fresh child processes. `execute_ms` is
adapter-specific: Toron and DuckDB measure execute plus collect, while Polars measures `lazy.collect()`. Registration,
lowering, digest work, and parent process overhead are excluded. Peak RSS is the maximum child-process RSS reported
by the perf rail and `/usr/bin/time`; spill bytes are Toron engine telemetry.

Spread is exactly `(max - min) / median`; `unstable` means spread is greater than `0.20`. Ratios divide the Toron
median by the peer median, so a value above 1 means Toron took longer. No attempt is averaged with another attempt.

## Throughput board

`T/D` is Toron divided by DuckDB; `T/P` is Toron divided by Polars. The starred q11 ratio uses DuckDB's persistently
unstable retry and is contextual only, not a clean baseline ratio. The 17 Polars omissions are the measured
dialect-unsupported set from the capability census and were not re-run here.

| query | Toron median ms | T spread/flag | DuckDB median ms | D spread/flag | Polars median ms | P spread/flag | T/D | T/P | Polars validation |
|---|---:|---|---:|---|---:|---|---:|---:|---|
| q01 | 22864.838 | 0.0081 / stable | 1022.000 | 0.0323 / stable | 7984.157 | 0.0856 / stable | 22.37x | 2.86x | reach+schema-only |
| q02 | 8080.589 | 0.1799 / stable | 129.000 | 0.1938 / stable | n/a | n/a | 62.64x | n/a | n/a (dialect) |
| q03 | 14559.086 | 0.1004 / stable | 765.000 | 0.0327 / stable | 8810.723 | 0.0331 / stable | 19.03x | 1.65x | value-validated |
| q04 | 4730.243 | 0.0793 / stable | 488.000 | 0.0594 / stable | n/a | n/a | 9.69x | n/a | n/a (dialect) |
| q05 | 177026.577 | 0.1193 / stable | 654.000 | 0.0459 / stable | n/a | n/a | 270.68x | n/a | n/a (dialect) |
| q06 | 1149.535 | 0.0156 / stable | 379.000 | 0.0449 / stable | 845.871 | 0.0492 / stable | 3.03x | 1.36x | reach+schema-only |
| q07 | 12452.118 | 0.0242 / stable | 647.000 | 0.0603 / stable | n/a | n/a | 19.25x | n/a | n/a (dialect) |
| q08 | 17826.301 | 0.0104 / stable | 712.000 | 0.0492 / stable | n/a | n/a | 25.04x | n/a | n/a (dialect) |
| q09 | 28325.413 | 0.0457 / stable | 1834.000 | 0.0229 / stable | n/a | n/a | 15.44x | n/a | n/a (dialect) |
| q10 | 11740.176 | 0.0754 / stable | 1450.000 | 0.0441 / stable | n/a | n/a | 8.10x | n/a | n/a (dialect) |
| q11 | 6214.859 | 0.0345 / stable | 111.000 | 0.3333 / unstable | n/a | n/a | 55.99x* | n/a | n/a (dialect) |
| q12 | 7135.600 | 0.0301 / stable | 501.000 | 0.0778 / stable | n/a | n/a | 14.24x | n/a | n/a (dialect) |
| q13 | 134484.731 | 0.0401 / stable | 1382.000 | 0.0174 / stable | n/a | n/a | 97.31x | n/a | n/a (dialect) |
| q14 | 4800.140 | 0.0223 / stable | 446.000 | 0.0291 / stable | 530.552 | 0.0917 / stable | 10.76x | 9.05x | reach+schema-only |
| q15 | 4704.639 | 0.0162 / stable | 363.000 | 0.0413 / stable | n/a | n/a | 12.96x | n/a | n/a (dialect) |
| q16 | 4468.177 | 0.0868 / stable | 246.000 | 0.0732 / stable | n/a | n/a | 18.16x | n/a | n/a (dialect) |
| q17 | 58709.875 | 0.0940 / stable | 690.000 | 0.0203 / stable | n/a | n/a | 85.09x | n/a | n/a (dialect) |
| q18 | 222182.319 | 0.0639 / stable | 1948.000 | 0.0303 / stable | n/a | n/a | 114.06x | n/a | n/a (dialect) |
| q19 | 83450.371 | 0.0556 / stable | 717.000 | 0.0474 / stable | 935.916 | 0.1226 / stable | 116.39x | 89.16x | reach+schema-only |
| q20 | 22495.943 | 0.0191 / stable | 567.000 | 0.0441 / stable | n/a | n/a | 39.68x | n/a | n/a (dialect) |
| q21 | 85418.597 | 0.0221 / stable | 2549.000 | 0.0314 / stable | n/a | n/a | 33.51x | n/a | n/a (dialect) |
| q22 | 3650.206 | 0.0546 / stable | 278.000 | 0.0360 / stable | n/a | n/a | 13.13x | n/a | n/a (dialect) |

Among the 21 cells with stable Toron and DuckDB samples, Toron/DuckDB ranges from 3.03x on q06 to 270.68x on q05.
Across the five stable Polars peers, Toron/Polars ranges from 1.36x on q06 to 89.16x on q19. Only q03's Polars
comparison is value-validated; its ratio is 1.65x. These are baseline observations, not producer diagnoses.

## Detailed retained metrics

### Toron

| query | retained | min ms | median ms | max ms | spread | peak RSS KiB | spill bytes | flags |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| q01 | 1 | 22783.807 | 22864.838 | 22968.489 | 0.0081 | 28812 | 0 | none |
| q02 | 1 | 7524.488 | 8080.589 | 8977.945 | 0.1799 | 1448224 | 234983488 | none |
| q03 | 1 | 14085.657 | 14559.086 | 15547.134 | 0.1004 | 532440 | 233338304 | none |
| q04 | 1 | 4653.025 | 4730.243 | 5027.928 | 0.0793 | 1233136 | 0 | none |
| q05 | 1 | 159634.479 | 177026.577 | 180758.808 | 0.1193 | 3056564 | 5353985328 | none |
| q06 | 1 | 1141.112 | 1149.535 | 1159.018 | 0.0156 | 28064 | 0 | none |
| q07 | 1 | 12390.579 | 12452.118 | 12691.929 | 0.0242 | 984188 | 134732480 | none |
| q08 | 1 | 17750.648 | 17826.301 | 17936.144 | 0.0104 | 1080264 | 0 | none |
| q09 | 1 | 28261.558 | 28325.413 | 29555.714 | 0.0457 | 2818828 | 108992256 | none |
| q10 | 1 | 11628.159 | 11740.176 | 12513.393 | 0.0754 | 1701108 | 215703848 | none |
| q11 | 2 | 6116.699 | 6214.859 | 6330.811 | 0.0345 | 1055924 | 166552400 | none |
| q12 | 1 | 7046.412 | 7135.600 | 7261.367 | 0.0301 | 1435684 | 159510976 | none |
| q13 | 1 | 133222.889 | 134484.731 | 138617.240 | 0.0401 | 960044 | 9152992 | none |
| q14 | 1 | 4747.006 | 4800.140 | 4854.171 | 0.0223 | 288704 | 36464000 | none |
| q15 | 1 | 4657.174 | 4704.639 | 4733.462 | 0.0162 | 105772 | 0 | none |
| q16 | 1 | 4423.961 | 4468.177 | 4811.597 | 0.0868 | 763216 | 60078400 | none |
| q17 | 1 | 54611.847 | 58709.875 | 60131.871 | 0.0940 | 1113756 | 961584 | none |
| q18 | 2 | 214358.851 | 222182.319 | 228565.845 | 0.0639 | 1261608 | 4380793312 | none |
| q19 | 1 | 80839.702 | 83450.371 | 85482.071 | 0.0556 | 295932 | 51471552 | none |
| q20 | 1 | 22442.594 | 22495.943 | 22872.732 | 0.0191 | 3362088 | 256104392 | none |
| q21 | 1 | 85111.530 | 85418.597 | 87001.335 | 0.0221 | 846088 | 2286556056 | none |
| q22 | 1 | 3621.911 | 3650.206 | 3821.237 | 0.0546 | 694064 | 31509620 | none |

### DuckDB

| query | retained | min ms | median ms | max ms | spread | peak RSS KiB | spill bytes | flags |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| q01 | 1 | 997.000 | 1022.000 | 1030.000 | 0.0323 | 201600 | 0 | none |
| q02 | 2 | 124.000 | 129.000 | 149.000 | 0.1938 | 97484 | 0 | none |
| q03 | 1 | 752.000 | 765.000 | 777.000 | 0.0327 | 252228 | 0 | none |
| q04 | 1 | 483.000 | 488.000 | 512.000 | 0.0594 | 231468 | 0 | none |
| q05 | 1 | 641.000 | 654.000 | 671.000 | 0.0459 | 205888 | 0 | none |
| q06 | 1 | 375.000 | 379.000 | 392.000 | 0.0449 | 130104 | 0 | none |
| q07 | 1 | 635.000 | 647.000 | 674.000 | 0.0603 | 314208 | 0 | none |
| q08 | 1 | 696.000 | 712.000 | 731.000 | 0.0492 | 236464 | 0 | none |
| q09 | 1 | 1827.000 | 1834.000 | 1869.000 | 0.0229 | 1346860 | 0 | none |
| q10 | 1 | 1423.000 | 1450.000 | 1487.000 | 0.0441 | 788688 | 0 | none |
| q11 | 2 | 106.000 | 111.000 | 143.000 | 0.3333 | 83016 | 0 | unstable |
| q12 | 1 | 495.000 | 501.000 | 534.000 | 0.0778 | 192916 | 0 | none |
| q13 | 1 | 1371.000 | 1382.000 | 1395.000 | 0.0174 | 212064 | 0 | none |
| q14 | 1 | 441.000 | 446.000 | 454.000 | 0.0291 | 288404 | 0 | none |
| q15 | 1 | 359.000 | 363.000 | 374.000 | 0.0413 | 167656 | 0 | none |
| q16 | 1 | 232.000 | 246.000 | 250.000 | 0.0732 | 217080 | 0 | none |
| q17 | 1 | 680.000 | 690.000 | 694.000 | 0.0203 | 141872 | 0 | none |
| q18 | 1 | 1911.000 | 1948.000 | 1970.000 | 0.0303 | 1260216 | 0 | none |
| q19 | 1 | 702.000 | 717.000 | 736.000 | 0.0474 | 332360 | 0 | none |
| q20 | 1 | 546.000 | 567.000 | 571.000 | 0.0441 | 168216 | 0 | none |
| q21 | 1 | 2527.000 | 2549.000 | 2607.000 | 0.0314 | 541916 | 0 | none |
| q22 | 1 | 274.000 | 278.000 | 284.000 | 0.0360 | 83480 | 0 | none |

### Polars SQL

| query | retained | min ms | median ms | max ms | spread | peak RSS KiB | spill bytes | flags |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| q01 | 1 | 7907.691 | 7984.157 | 8591.059 | 0.0856 | 6785536 | 0 | none |
| q03 | 1 | 8716.137 | 8810.723 | 9007.950 | 0.0331 | 8193680 | 0 | none |
| q06 | 1 | 831.887 | 845.871 | 873.481 | 0.0492 | 905988 | 0 | none |
| q14 | 1 | 506.266 | 530.552 | 554.926 | 0.0917 | 1465180 | 0 | none |
| q19 | 1 | 913.455 | 935.916 | 1028.220 | 0.1226 | 1587260 | 0 | none |

The Polars q01 and q03 process peaks are approximately 6.47 GiB and 7.81 GiB. They would not be valid throughput
samples under the separate 4 GiB correctness envelope; this is why the profile was explicitly memory-unbounded.

## Polars validation levels

The perf adapter emits no generic digest for Polars. The validation level therefore remains the one established by
the ADR 394 capability census; timing success does not silently strengthen it.

| query | SQL source | capability | validation level | reference |
|---|---|---|---|---|
| q01 | canonical `export.sql` | pass | reach+schema-only | fingerprint matches capability census |
| q03 | dedicated `polars_sql_text` | pass | value-validated | 114003 rows; dedicated oracle max difference 267 <= 1000 |
| q06 | canonical `export.sql` | pass | reach+schema-only | fingerprint matches capability census |
| q14 | dedicated `polars_sql_text` | pass | reach+schema-only | fingerprint matches capability census |
| q19 | dedicated `polars_sql_text` | pass | reach+schema-only | fingerprint matches capability census |

## Raw-row admission and fingerprint audit

Every retained sample has one warmup row and exactly seven measured rows with run indices 0-6. All measured rows are
strict sf=10 release passes. `invariant` below means all seven measured rows agree on the named fields; row counts are
shown explicitly. DuckDB and Polars digests are null by design.

| engine | query | retained | measured | status/profile | plan/schema/rows | digest | external fingerprint | named exception |
|---|---|---:|---:|---|---|---|---|---|
| toron | q01 | 1 | 7/7 | pass/release | invariant (rows=4) | invariant | redux match | none |
| toron | q02 | 1 | 7/7 | pass/release | invariant (rows=4667) | invariant | redux match | none |
| toron | q03 | 1 | 7/7 | pass/release | invariant (rows=114003) | invariant | redux match | none |
| toron | q04 | 1 | 7/7 | pass/release | invariant (rows=5) | invariant | redux match | none |
| toron | q05 | 1 | 7/7 | pass/release | invariant (rows=5) | invariant | redux match | none |
| toron | q06 | 1 | 7/7 | pass/release | invariant (rows=1) | invariant | redux match | none |
| toron | q07 | 1 | 7/7 | pass/release | invariant (rows=4) | invariant | redux match | none |
| toron | q08 | 1 | 7/7 | pass/release | invariant (rows=2) | invariant | redux match | none |
| toron | q09 | 1 | 7/7 | pass/release | invariant (rows=175) | invariant | redux match | none |
| toron | q10 | 1 | 7/7 | pass/release | invariant (rows=381105) | invariant | redux match | none |
| toron | q11 | 2 | 7/7 | pass/release | invariant (rows=8685) | invariant | redux match | none |
| toron | q12 | 1 | 7/7 | pass/release | invariant (rows=2) | invariant | redux match | none |
| toron | q13 | 1 | 7/7 | pass/release | invariant (rows=46) | invariant | redux match | none |
| toron | q14 | 1 | 7/7 | pass/release | invariant (rows=1) | invariant | redux match | none |
| toron | q15 | 1 | 7/7 | pass/release | invariant (rows=1) | invariant | redux match | none |
| toron | q16 | 1 | 7/7 | pass/release | invariant (rows=27840) | invariant | redux match | none |
| toron | q17 | 1 | 7/7 | pass/release | invariant (rows=1) | invariant | redux match | none |
| toron | q18 | 2 | 7/7 | pass/release | invariant (rows=100) | invariant | redux match | none |
| toron | q19 | 1 | 7/7 | pass/release | invariant (rows=1) | invariant | redux match | none |
| toron | q20 | 1 | 7/7 | pass/release | invariant (rows=1804) | invariant | redux match | none |
| toron | q21 | 1 | 7/7 | pass/release | invariant (rows=100) | invariant | redux match | none |
| toron | q22 | 1 | 7/7 | pass/release | invariant (rows=7) | invariant | redux match | none |
| duckdb | q01 | 1 | 7/7 | pass/release | invariant (rows=4) | N/A (null) | N/A | none |
| duckdb | q02 | 2 | 7/7 | pass/release | invariant (rows=4667) | N/A (null) | N/A | none |
| duckdb | q03 | 1 | 7/7 | pass/release | invariant (rows=114003) | N/A (null) | N/A | none |
| duckdb | q04 | 1 | 7/7 | pass/release | invariant (rows=5) | N/A (null) | N/A | none |
| duckdb | q05 | 1 | 7/7 | pass/release | invariant (rows=5) | N/A (null) | N/A | none |
| duckdb | q06 | 1 | 7/7 | pass/release | invariant (rows=1) | N/A (null) | N/A | none |
| duckdb | q07 | 1 | 7/7 | pass/release | invariant (rows=4) | N/A (null) | N/A | none |
| duckdb | q08 | 1 | 7/7 | pass/release | invariant (rows=2) | N/A (null) | N/A | none |
| duckdb | q09 | 1 | 7/7 | pass/release | invariant (rows=175) | N/A (null) | N/A | none |
| duckdb | q10 | 1 | 7/7 | pass/release | invariant (rows=381105) | N/A (null) | N/A | none |
| duckdb | q11 | 2 | 7/7 | pass/release | invariant (rows=8685) | N/A (null) | N/A | timing unstable after retry |
| duckdb | q12 | 1 | 7/7 | pass/release | invariant (rows=2) | N/A (null) | N/A | none |
| duckdb | q13 | 1 | 7/7 | pass/release | invariant (rows=46) | N/A (null) | N/A | none |
| duckdb | q14 | 1 | 7/7 | pass/release | invariant (rows=1) | N/A (null) | N/A | none |
| duckdb | q15 | 1 | 7/7 | pass/release | invariant (rows=1) | N/A (null) | N/A | none |
| duckdb | q16 | 1 | 7/7 | pass/release | invariant (rows=27840) | N/A (null) | N/A | none |
| duckdb | q17 | 1 | 7/7 | pass/release | invariant (rows=1) | N/A (null) | N/A | none |
| duckdb | q18 | 1 | 7/7 | pass/release | invariant (rows=100) | N/A (null) | N/A | none |
| duckdb | q19 | 1 | 7/7 | pass/release | invariant (rows=1) | N/A (null) | N/A | none |
| duckdb | q20 | 1 | 7/7 | pass/release | invariant (rows=1804) | N/A (null) | N/A | none |
| duckdb | q21 | 1 | 7/7 | pass/release | invariant (rows=100) | N/A (null) | N/A | none |
| duckdb | q22 | 1 | 7/7 | pass/release | invariant (rows=7) | N/A (null) | N/A | none |
| polars_sql | q01 | 1 | 7/7 | pass/release | invariant (rows=4) | N/A (null) | capability-census match | none |
| polars_sql | q03 | 1 | 7/7 | pass/release | invariant (rows=114003) | N/A (null) | capability-census match | none |
| polars_sql | q06 | 1 | 7/7 | pass/release | invariant (rows=1) | N/A (null) | capability-census match | none |
| polars_sql | q14 | 1 | 7/7 | pass/release | invariant (rows=1) | N/A (null) | capability-census match | none |
| polars_sql | q19 | 1 | 7/7 | pass/release | invariant (rows=1) | N/A (null) | capability-census match | none |

The external fingerprint audit compared actual values, not merely counts: all 22 Toron values equal the redux values,
and all five Polars values equal the capability-census values. DuckDB has no prior external fingerprint rail, so its
requirement is within-sample invariance only.

## Retry provenance

| engine | query | attempt 1 | attempt 2 | retained classification |
|---|---|---|---|---|
| Toron | q11 | 5253.285 ms, spread 0.2373, unstable | 6214.859 ms, spread 0.0345, stable | attempt 2 clean |
| Toron | q18 | 225776.635 ms, spread 0.2795, unstable; operator reported concurrent 100 GiB editor/file I/O during the sample | 222182.319 ms, spread 0.0639, stable on the quiet host | attempt 2 clean; attempt 1 excluded |
| DuckDB | q02 | 127.000 ms, spread 0.2047, unstable | 129.000 ms, spread 0.1938, stable | attempt 2 clean |
| DuckDB | q11 | 113.000 ms, spread 0.2655, unstable | 111.000 ms, spread 0.3333, unstable | attempt 2 reported unstable; no clean median banked |

The q18 attempt-1 outlier was measured run 3 at 279322.902 ms; the other attempt-1 rows were 216221.979-239195.927
ms. The clean retry's seven rows were 214358.851-228565.845 ms. No third attempt was run for any cell.

## Hygiene and verification

- All 53 attempt scopes report exit status 0 and successful completion.
- Scratch was empty before and after every attempt and is empty after the rail.
- Every retained raw file has one warmup plus seven measured rows; identity and invariance audit errors: 0.
- No `.perf.data` was created under this rail or during the measurement window, and none is tracked. Pre-existing
  ignored profiling captures outside this artifact were left untouched.
- No engine, adapter, or harness source changed. The only committed change after the engine seal is the measurement
  plan, committed before this uncommitted results document.
- `scripts/cargo-cgroup fmt -- --check`: pass.
- `git diff --check` plus the untracked-file whitespace check: pass.
- `git status --short`: only this uncommitted results document.

## Forward use

This baseline is the article's throughput rail. The ADR 415 redux remains the 4 GiB/no-swap correctness and survival
rail; the two timing profiles must not be mixed. Any optimization work should select a decision-worthy stable gap,
name its producer, and compare a future run under this exact compute profile using the regression band
`max(0.10, 2 * max(current spread, baseline spread))`. DuckDB q11 first needs a stable peer sample before it can serve
as a clean per-cell comparison point.
