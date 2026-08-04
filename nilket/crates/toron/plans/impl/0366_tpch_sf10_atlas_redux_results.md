# ADR 366 results - TPC-H SF=10 atlas redux

ADR 366 ran the live sf=10 atlas from the ADR 360 persisted Parquet data surface. No engine code or budget constants
changed.

## Runtime Setup

| item | value |
| --- | --- |
| release commit | `6dec656` (`plan: adr 366 tpch sf10 atlas redux`) |
| dataset | `target/tpch/data/strict/sf10/manifest.json` |
| manifest fingerprint | `fnv1a64:8920cf8f2e991230` |
| cap | `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512` |
| mode / sf | `strict`, `10` |
| runs / warmup | `1` / `0` |
| DuckDB | `v1.5.4 (Variegata) 08e34c447b` |

Runner shape:

```text
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf \
    --sf 10 --mode strict --runs 1 --warmup 0 --query <query> \
    --engines toron,duckdb \
    --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr366/<query>
```

The combined Toron+DuckDB runner preserved both rows when Toron failed. q19 is the only exception: Toron was still
CPU-bound after 33m51s with stable RSS (~406 MiB) and no raw row, so the active child was terminated. DuckDB q19 was
then run separately and passed. This is recorded as a time-wall/methodology artifact, not a memory wall.

## Full Board

| query | Toron status | classification | producer | requested | available | Toron ms | RSS KiB | spill bytes | correctness provenance | DuckDB |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| q01 | fail | scan-output materialization | `scan output accumulator` | 1073753200 | 1073741824 | 546.5 | 1090404 | 0 | not required - red | pass / 207.0 ms / none |
| q02 | fail | join-output materialization | `join output materialization` | 1778000000 | 1073741824 | 1180.6 | 1204596 | 0 | not required - red | pass / 69.0 ms / none |
| q03 | fail | spill-volume | `streamed inner join build spill writer` | 536871224 | 536870912 | 163200.7 | 349828 | 0 | not required - red | pass / 255.0 ms / none |
| q04 | fail | scan-output materialization | `scan output accumulator` | 1073762000 | 1073741824 | 562.1 | 1058788 | 0 | not required - red | pass / 130.0 ms / none |
| q05 | fail | spill-volume | `streamed inner join build spill writer` | 536871224 | 536870912 | 164300.6 | 351240 | 0 | not required - red | pass / 190.0 ms / none |
| q06 | fail | scan-output materialization | `scan output accumulator` | 1073766272 | 1073741824 | 438.4 | 1082792 | 0 | not required - red | pass / 87.0 ms / none |
| q07 | fail | scan-output materialization | `scan output accumulator` | 1073762000 | 1073741824 | 590.1 | 1059152 | 0 | not required - red | pass / 210.0 ms / none |
| q08 | fail | scan-output materialization | `scan output accumulator` | 1073762000 | 1073741824 | 1085.7 | 1521960 | 0 | not required - red | pass / 230.0 ms / none |
| q09 | fail | scan-output materialization | `scan output accumulator` | 1073762000 | 1073741824 | 4710.6 | 3111236 | 0 | not required - red | pass / 423.0 ms / none |
| q10 | fail | spill-volume | `streamed inner join build spill writer` | 536871224 | 536870912 | 168395.8 | 350120 | 0 | not required - red | pass / 776.0 ms / none |
| q11 | pass | correctness-oracle gap | n/a | n/a | n/a | 52248.4 | 3846076 | 734405536 | fail@compare: oracle non-vacuous guard failed (`rows=0`) | pass / 48.0 ms / `schema_mismatch` |
| q12 | fail | spill-volume | `streamed inner join build spill writer` | 536876880 | 536870912 | 119987.1 | 321780 | 0 | not required - red | pass / 191.0 ms / none |
| q13 | fail | scan-output materialization | `scan output accumulator` | 1073762000 | 1073741824 | 862.5 | 1346264 | 0 | not required - red | pass / 342.0 ms / none |
| q14 | pass | correctness-green | n/a | n/a | n/a | 73047.4 | 465496 | 134225536 | pass@compare | pass / 121.0 ms / `schema_mismatch` |
| q15 | fail | scan-output materialization | `scan output accumulator` | 1073747200 | 1073741824 | 505.8 | 1086400 | 0 | not required - red | pass / 112.0 ms / none |
| q16 | pass | correctness-green | n/a | n/a | n/a | 45846.4 | 846988 | 145425912 | pass@compare | pass / 113.0 ms / none |
| q17 | fail | scan-output materialization | `scan output accumulator` | 1073742336 | 1073741824 | 463.0 | 1086848 | 0 | `q17_sf10_oracle_policy_gap` if reached | pass / 204.0 ms / none |
| q18 | fail | scan-output materialization | `scan output accumulator` | 1073762000 | 1073741824 | 886.9 | 1346556 | 0 | not required - red | pass / 345.0 ms / none |
| q19 | no row | time wall / methodology gap | n/a | n/a | n/a | >2031s active CPU | ~406500 | n/a | not run - no Toron row | pass / 161.0 ms / none |
| q20 | fail | scan-output materialization | `scan output accumulator` | 1073748096 | 1073741824 | 1635.4 | 1374032 | 4192 | not required - red | pass / 146.0 ms / none |
| q21 | fail | semi/anti existence-set spill | `semi/anti existence-set spill writer` | 536876880 | 536870912 | 122432.8 | 320972 | 3168 | not required - red | pass / 581.0 ms / none |
| q22 | fail | scan-output materialization | `scan output accumulator` | 1073762000 | 1073741824 | 1086.4 | 1370932 | 0 | not required - red | pass / 146.0 ms / none |

## Correctness Provenance

Persisted correctness was run for every Toron pass row:

| query | correctness result | note |
| --- | --- | --- |
| q11 | fail@compare | The query returns zero rows for both Toron and DuckDB, but the strict oracle's non-vacuity guard fails at sf=10 (`rows=0`, `total_german_parts=304774`, `threshold_fail_groups=304774`). This is an oracle/data-policy gap, not a correctness green. |
| q14 | pass@compare | Live persisted sf=10 correctness green. |
| q16 | pass@compare | Live persisted sf=10 correctness green. |

q17 was checked for the expected policy behavior:

```text
q17_sf10_oracle_policy_gap: q17 correctness requires --sf 0.02; got 10
```

## Common-Wall Ranking

| rank | producer / gap | dimension | count | queries | representative requested / available |
| ---: | --- | --- | ---: | --- | --- |
| 1 | `scan output accumulator` | Memory | 12 | q01, q04, q06, q07, q08, q09, q13, q15, q17, q18, q20, q22 | 1073762000 / 1073741824 |
| 2 | `streamed inner join build spill writer` | SpillBytes | 4 | q03, q05, q10, q12 | 536871224 / 536870912 |
| 3 | `join output materialization` | Memory | 1 | q02 | 1778000000 / 1073741824 |
| 4 | `semi/anti existence-set spill writer` | SpillBytes | 1 | q21 | 536876880 / 536870912 |
| 5 | q11 oracle non-vacuity gap | correctness provenance | 1 | q11 | n/a |
| 6 | q19 unbounded runtime artifact | time / methodology | 1 | q19 | >2031s active CPU, no Toron row |

## Recommendation

The next high-leverage engine ADR should target the scan-output accumulator.

Reason:

- It affects 12 measured Toron reds, far more than continuing the q21-only spill path.
- The overages are small but recurring: most scan accumulator requests are just over the 1 GiB cap.
- q06, the seed red from ADR 365, is part of this family. That validates the plumbing-first call and turns the q06
  finding into the top shared wall rather than a one-off.

Likely next cut shape:

- stream scan output into the consuming operator instead of materializing `Scan` into the `scan output accumulator`;
- start with the simplest scan-to-collapse consumer (q06 scan-to-global-aggregate is the canary);
- preserve the ADR 361 parity discipline: shared `ScanRequest` builder, digest-identical small-scale checks, fixed-cap
  sf=10 green-or-next-wall acceptance;
- do not raise the accumulator cap.

Secondary cuts:

- `streamed inner join build spill writer` is the next repeated family after scan streaming.
- q11 needs an oracle/data-policy decision before it can be counted as correctness-green at sf=10.
- q19 needs an explicit time-wall methodology (for example a planned runtime cap with recorded no-row classification)
  before future atlases can treat long CPU-bound runs consistently.

## Verification

Commands run:

```text
test -f target/tpch/data/strict/sf10/manifest.json
/home/me/.duckdb/cli/latest/duckdb --version
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run ... toron-tpch perf --query q01..q22 --engines toron,duckdb --data-source parquet
systemd-run ... toron-tpch perf --query q19 --engines duckdb --data-source parquet
systemd-run ... toron-tpch correctness --query q11 --sf 10 --data-source parquet
systemd-run ... toron-tpch correctness --query q14 --sf 10 --data-source parquet
systemd-run ... toron-tpch correctness --query q16 --sf 10 --data-source parquet
systemd-run ... toron-tpch correctness --query q17 --sf 10 --data-source parquet
```

No `crates/toron/src` files changed.
