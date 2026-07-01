# ADR 398 results - sf=10 post-397 mini-atlas

## Verdict

ADR 398 is measurement-only. The post-397 sf=10 board did **not** turn the primary
multi-table cells (`q03/q05/q10`) green, but it did expose useful movement:

- `q03/q05/q10` remain `time_no_row` within the bounded window.
- `q08` moved from `time/no-row` to a named memory wall:
  `reoriented join build accumulator`.
- `q04` and `q19` remain green and improved materially versus the prior control timings.
- `q12` remains time/no-row; no spill producer surfaced inside the window.

397 therefore did not reprioritize the next substrate strike away from q03. It did, however,
confirm that generic join projection pushdown improved the control rows and can expose downstream
producers on adjacent inner-chain shapes.

## Conditions

All rows are live persisted sf=10 under the fixed 4 GiB scope. No matrix or correctness-matrix
cache was used.

Reference rows were split by engine because `polars_sql` q03 OOM-killed under the 4 GiB cap before
the combined reference run could write raw rows. DuckDB rows were then collected separately and
completed under the cap for all seven queries.

Toron time/no-row cells were bounded `perf-run-one` probes, stopped after roughly the same
360-second mini-atlas window:

| query | wall window | peak memory | row |
|---|---:|---:|---|
| q03 | 5m31s | 2.7 GiB | no JSON row |
| q05 | 5m27s | 2.9 GiB | no JSON row |
| q10 | 5m48s | 2.7 GiB | no JSON row |
| q12 | 5m50s | 1.4 GiB | no JSON row |

The completing / quick-fail Toron cells (`q08/q19/q04`) were rerun through the full perf rail
with `runs=3, warmup=1`.

## Pinned Board

| query | pre397_class | post397_status | post397_producer | correctness | toron_ms | duckdb_ms | polars_ms | toron/duckdb | toron/polars | verdict |
|---|---|---|---|---|---:|---:|---:|---:|---:|---|
| q03 | time/no-row (390) | time_no_row | no row before bounded window; peak 2.7 GiB | n/a | >331s | 241.0 | n/a (`polars_sql` OOM under 4 GiB) | n/a | n/a | unchanged time-bound |
| q05 | time/no-row (390) | time_no_row | no row before bounded window; peak 2.9 GiB | n/a | >327s | 175.0 | n/a (`polars_sql` not_attempted) | n/a | n/a | unchanged time-bound |
| q10 | time/no-row (390) | time_no_row | no row before bounded window; peak 2.7 GiB | n/a | >348s | 766.0 | n/a (`polars_sql` not_attempted) | n/a | n/a | unchanged time-bound |
| q12 | partition-group spill -> time (389) | time_no_row | no row before bounded window; peak 1.4 GiB | n/a | >350s | 112.0 | n/a (`polars_sql` not_attempted) | n/a | n/a | unchanged time-bound |
| q08 | time/no-row (387) | fail | `memory_budget_exhausted`: `reoriented join build accumulator`, requested 1.8 GiB / available 1 GiB | n/a | 1233.725 | 215.0 | n/a (`polars_sql` not_attempted) | 5.74x to wall | n/a | moved: time -> named memory wall |
| q19 | green (385) | pass | none; spill 51,471,552 B | persisted sf=10 pass | 81479.078 | 152.0 | 196.944 | 536.05x | 413.71x | green control improved, no regression |
| q04 | green (374) | pass | none; spill 0 | persisted sf=10 pass | 4828.684 | 125.0 | n/a (`polars_sql` not_attempted) | 38.63x | n/a | green control improved, no regression |

## Reference Rows

DuckDB completed all target rows under the fixed cap:

| query | duckdb median | spread | peak RSS |
|---|---:|---:|---:|
| q03 | 241.0 ms | 0.166 | 433,592 KiB |
| q05 | 175.0 ms | 0.029 | 341,640 KiB |
| q10 | 766.0 ms | 0.023 | 870,176 KiB |
| q12 | 112.0 ms | 0.027 | 279,420 KiB |
| q08 | 215.0 ms | 0.060 | 406,648 KiB |
| q19 | 152.0 ms | 0.033 | 419,632 KiB |
| q04 | 125.0 ms | 0.024 | 288,400 KiB |

Polars SQL rows:

| query | polars_sql status | median | note |
|---|---|---:|---|
| q03 | no row | n/a | OOM-killed under 4 GiB before raw rows were written |
| q05 | not_attempted | n/a | current Rust adapter does not attempt this query |
| q10 | not_attempted | n/a | current Rust adapter does not attempt this query |
| q12 | not_attempted | n/a | current Rust adapter does not attempt this query |
| q08 | not_attempted | n/a | current Rust adapter does not attempt this query |
| q19 | pass | 196.944 ms | stable, peak RSS 1,817,808 KiB |
| q04 | not_attempted | n/a | current Rust adapter does not attempt this query |

## Correctness Provenance

Only `q04` and `q19` had post-397 Toron pass rows. Both were verified through persisted sf=10
correctness:

| query | correctness |
|---|---|
| q04 | pass, `elapsed_ms=15706.1` |
| q19 | pass, `elapsed_ms=99551.3` |

No other Toron target completed, so no other cell is green by implication.

## Old-vs-New Read

- **q03/q05/q10:** unchanged at the board level. They remain CPU/time-bound beyond the
  mini-atlas window. 397 reduced q03 sf=1 materialization cost dramatically, but that did not
  translate into sf=10 completion for these multi-table cells.
- **q12:** unchanged time-bound anchor. The previous partition-group spill family remains past the
  reach milestone, but no new producer surfaced inside this bounded probe.
- **q08:** moved materially. Before 397 it was a time/no-row inner-chain control; after 397 it
  reaches a named `reoriented join build accumulator` memory wall in about 1.23s. This is a real
  reach-board reclassification.
- **q04/q19:** green controls are unregressed. Both pass persisted correctness and are faster than
  the prior public control numbers (`q04` now ~4.8s; `q19` now ~81.5s).

## 399 Steer

398 does **not** shift the substrate arc away from q03. The primary cells did not green or expose a
new shared producer. The right next measurement remains:

1. **ADR 399:** post-397 q03 substrate re-profile from the 3.97x Rune/Polars residual.
2. Keep `q08`'s newly named `reoriented join build accumulator` as a reach-board follow-up, not the
   next substrate strike.
3. Preserve q04/q19 as controls; they are useful evidence that 397's generic projection pruning
   helped beyond q03 without regressing correctness.

## Hygiene

- Measurement-only; no source files changed.
- Live persisted sf=10 runs only; no matrix cache.
- No cap bump.
- No `.perf.data` committed.
