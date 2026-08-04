# ADR 386 results - SF=10 post-385 mini-atlas

## Summary

ADR 386 refreshed the cells moved by the 374-385 perf arc under the persisted sf=10 data surface. It is measurement-only:
no engine code, harness code, or budget constants changed.

Main findings:

- q19 is reconfirmed green and correct at sf=10 after ADR 385.
- q17 now reaches a row at sf=10, but remains a correctness-oracle policy gap (`q17_sf10_oracle_policy_gap`), so it is
  not counted as green.
- q03/q05/q08/q10 are current live scan-output accumulator walls. q08 validates the carried-forward inner-chain
  memory-tail assumption.
- q12 remains the partition-group sizing residual.
- q21 remains the small semi/anti existence-set resident wall, 712 bytes over the 1 GiB retained-footprint cap.
- q07 remains a time/no-row cell under the 360s bound.

The DuckDB reference was collected for every target query. The results include `Toron/DuckDB` multiples so time hot spots
can be ranked in units of the reference engine, not only by raw seconds.

## Setup

| item | value |
| --- | --- |
| commit | `8e8efe1` (`plan: adr 386 sf10 post 385 mini atlas`) |
| dataset | `target/tpch/data/strict/sf10/manifest.json` |
| manifest sha256 | `0647a57fbd6c34839edbfe83297b717dd014c24a9b55535b7e7b8c2a066e446f` |
| cap | `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512` |
| Toron command | isolated `target/release/toron-tpch perf-run-one`, one query per fixed-cap scope |
| DuckDB command | isolated `target/release/toron-tpch perf --engines duckdb`, one query per fixed-cap scope |
| timeout | Toron target rows: 360s; DuckDB rows: 120s |
| DuckDB | `v1.5.4 (Variegata) 08e34c447b` |

The target set was:

```text
q03 q04 q05 q06 q07 q08 q10 q12 q14 q17 q19 q21
```

`q08` was added as the memory-tail control for the generic streamed-inner wiring touched by ADR 385.

## Target Board

| query | previous class | current class | Toron execute ms | DuckDB ms | Toron/DuckDB | Toron evidence | correctness |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| q03 | moderate reread/time control | scan-output memory wall | 78,450.6 | 269.0 | 291.6x | `memory_budget_exhausted`, producer `scan output accumulator`, 1,073,790,848 / 1,073,741,824, spill 1,434,880,384 | n/a |
| q04 | green after resident accounting | green_correct | 10,788.1 | 132.0 | 81.7x | pass, 5 rows, spill 0 | pass@compare sf=10 |
| q05 | partitioned inner-build family/time-no-row | scan-output memory wall | 112,510.0 | 207.0 | 543.5x | `memory_budget_exhausted`, producer `scan output accumulator`, 1,073,790,848 / 1,073,741,824, spill 1,434,880,384 | n/a |
| q06 | scan-output seed red, later global-agg green | green_correct | 1,161.4 | 99.0 | 11.7x | pass, 1 row, spill 0 | pass@compare sf=10 |
| q07 | time/no-row | time_no_row | >360,000 wall | 180.0 | >2000.0x | timeout rc=124 after 361s, no row | n/a |
| q08 | carried-forward scan-output memory tail | scan-output memory wall | 1,080.5 | 233.0 | 4.6x | `memory_budget_exhausted`, producer `scan output accumulator`, 1,073,762,000 / 1,073,741,824, spill 0 | n/a |
| q10 | partitioned inner-build family/time-no-row | scan-output memory wall | 71,495.9 | 781.0 | 91.5x | `memory_budget_exhausted`, producer `scan output accumulator`, 1,073,790,848 / 1,073,741,824, spill 1,434,880,384 | n/a |
| q12 | partition-group sizing residual | spill wall | 5,860.0 | 133.0 | 44.1x | `spill_budget_exhausted`, producer `streamed inner join build partition group sizing`, 8,396,405,047 / 536,870,912 | n/a |
| q14 | known persisted green | green_correct | 14,154.8 | 178.0 | 79.5x | pass, 1 row, spill 134,225,536 | pass@compare sf=10 |
| q17 | time cluster / oracle-policy expected | correctness_gap | 149,874.5 | 175.0 | 856.4x | pass, 1 row, spill 134,225,536 | `q17_sf10_oracle_policy_gap` |
| q19 | ADR 385 green canary | green_correct | 111,297.7 | 149.0 | 747.0x | pass, 1 row, spill 134,225,536 | pass@compare sf=10 |
| q21 | small existence-set resident wall | memory wall | 11,140.7 | 573.0 | 19.4x | `memory_budget_exhausted`, producer `semi/anti existence-set spill resident state`, 1,073,742,536 / 1,073,741,824, spill 3,168 | n/a |

Notes:

- For failing Toron rows, the `Toron/DuckDB` multiple is time-to-wall divided by DuckDB time, not time-to-correct-answer.
- q07 has no Toron JSON row; the ratio is a lower bound from the 360s timeout.
- q17 reaches and emits a digest, but the persisted correctness command rejects sf=10 by policy. It is not counted as
  correctness-green.

## Correctness Provenance

Live persisted correctness was run for every Toron pass row:

| query | result |
| --- | --- |
| q04 | pass@compare, 21,309.6 ms |
| q06 | pass@compare, 8,291.4 ms |
| q14 | pass@compare, 21,384.0 ms |
| q17 | `q17_sf10_oracle_policy_gap: q17 correctness requires --sf 0.02; got 10` |
| q19 | pass@compare, 134,391.3 ms |

Therefore ADR 386 counts q04/q06/q14/q19 as `green_correct`; q17 is a reach/pass row with a correctness-oracle policy
gap.

## Changed-Cell Read

### q19

q19 stayed green after ADR 385:

- Toron execute: 111.3s;
- DuckDB execute: 149ms;
- ratio: 747x;
- persisted correctness: pass.

This confirms 385 did not regress and that q19's next problem is performance, not reach/correctness.

### q03/q05/q10/q12

The partitioned spilled-inner family no longer presents as a single class:

- q03/q05/q10 now hit `scan output accumulator` after tens of seconds and 1.43 GiB of spill writes;
- q12 hits the `streamed inner join build partition group sizing` spill-budget stop-line quickly.

The high-leverage next memory producer is therefore not more spill decode/re-read work; it is the scan-output
accumulator across q03/q05/q10 plus the memory-tail control q08.

### q08 Control

q08 stayed in the carried-forward scan-output class:

```text
producer: scan output accumulator
requested: 1,073,762,000
available: 1,073,741,824
```

This validates the carried-forward memory-tail assumption for q09/q18/q22 as provenance-labeled rows: ADR 385's generic
streamed-inner page-loader wiring did not silently move the inner-chain control. The carried-forward rows remain
carried-forward, not fresh evidence.

### q07/q17/q21

- q07 remains the largest time/no-row outlier: >360s against DuckDB 180ms, >2000x lower-bound.
- q17 now reaches in 149.9s against DuckDB 175ms, but its sf=10 correctness oracle is still a policy gap.
- q21 remains a tiny resident-state memory wall: 712 bytes over cap.

## Ranking

Fresh mini-atlas ranking:

| rank | candidate | fresh affected queries | evidence | read |
| ---: | --- | --- | --- | --- |
| 1 | scan-output accumulator | q03, q05, q08, q10 | 4 live target rows, all current; q09/q18/q22 remain carried-forward scan-output candidates | highest fresh count; likely next memory/reach cut |
| 2 | time/perf cluster | q07, q17, q19 | q07 >2000x lower-bound, q17 856x with oracle gap, q19 747x but correct | highest DuckDB-normalized perf gap, but less reach-oriented |
| 3 | q12 partition-group sizing | q12 | 8.40 GiB requested vs 512 MiB spill budget | single-cell spill-policy/algorithm residual |
| 4 | q21 existence-set resident | q21 | 712 bytes over retained cap | small single-cell memory quick win |

Recommendation: next ADR should target the **scan-output accumulator family**. It has the strongest fresh count and q08
validated that at least one carried-forward memory-tail control is still in that class. The likely shape is another
scan-streaming consumer extension for the remaining mixed/join consumers surfaced by q03/q05/q08/q10, with live
coverage-contract discipline as in ADRs 367-371.

The time/perf cluster is real and severe, especially in DuckDB-normalized units, but the mini-atlas shows a repeated
reach/memory producer still blocking several cells. Finish that producer before returning to q07/q17/q19 performance.

## Carried-Forward Rows

These rows were not rerun by ADR 386 and must stay provenance-labeled:

| query | carried-forward class | provenance |
| --- | --- | --- |
| q02 | join-output materialization | ADR 366 |
| q09 | scan-output accumulator | ADR 366, validated indirectly by q08 control only |
| q18 | scan-output accumulator | ADR 366, validated indirectly by q08 control only |
| q22 | scan-output accumulator | ADR 366, validated indirectly by q08 control only |
| q01/q11/q13/q15/q16/q20 | prior greens / oracle-gap mix | not part of this mini-atlas |

The q08 control supports the assumption that the memory-tail scan-output family did not silently move under ADR 385, but
it does not convert q09/q18/q22 into fresh rows.

## Verification

| command | result |
| --- | --- |
| `test -f target/tpch/data/strict/sf10/manifest.json` | pass |
| `CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release` | pass |
| isolated fixed-cap Toron target set | complete for all 12 target queries |
| isolated fixed-cap DuckDB target set | complete for all 12 target queries |
| live persisted correctness for pass rows | q04/q06/q14/q19 pass; q17 policy gap |

No source files changed.

## Outcome

ADR 386 succeeded as a mini-atlas:

- the post-385 board is refreshed for the changed cells;
- q19 is reconfirmed green/correct;
- q17's reach improvement is recorded without overclaiming correctness;
- q08 validates the carried-forward memory-tail assumption;
- DuckDB-normalized ratios are now part of the ranking;
- the next high-leverage memory cut is the scan-output accumulator family.
