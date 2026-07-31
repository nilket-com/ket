# ADR 398 - Implementation plan: sf=10 post-397 mini-atlas

Pairs with `plans/adr/398_sf10_post_397_mini_atlas.md`. Measurement-only: live persisted sf=10 board for q03/q05/q10 +
q12 + controls q08/q19/q04, pinned pre397 class, correctness-provenance per pass row, old-vs-new producer class. No
engine change.

## Phase 1 - live persisted sf=10 board
```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch \
  --features polars-sql-reference --release
# per query in {q03,q05,q10,q12,q08,q19,q04}: live persisted sf=10, fixed 4 GB, perf rail
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 3 --warmup 1 --query <q> \
    --engines toron,duckdb,polars_sql --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr398_<q>_sf10
```
- Record status (green / advanced / time-no-row / wall), the producer (old-vs-new class), execute_ms, and ratios where
  DuckDB/Polars rows exist. Polars only where the SQL subset runs.
- **Not the matrix/correctness-matrix cache** - live rows only.

## Phase 2 - correctness provenance for pass rows (365 path)
For any post-397 **pass**/completed row:
```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness --query <q> --sf 10 --data-source parquet --data-dir target/tpch/data
```
- Pass -> record the oracle row in the `correctness` column. Oracle cannot validate -> `reach_only_no_oracle`. **No green
  by implication.**

## Deliverable (results doc)
`crates/toron/plans/impl/398_sf10_post_397_mini_atlas_results.md`:
- the pinned board table: `query | pre397_class | post397_status | post397_producer | correctness | toron_ms | duckdb_ms
  | polars_ms | toron/duckdb | toron/polars | verdict`;
- per cell: old-vs-new producer class (moved/cleared/unchanged), correctness provenance (365 oracle or
  `reach_only_no_oracle`);
- the ranked verdict: did 397 reprioritize reach? which cells moved, controls unregressed;
- the 399 steer (if q03/q05/q10 moved, the next substrate cut may shift; else continue from 3.97x);
- confirmation: measurement-only, no engine change, live not cached, no committed `.perf.data`.

## Verification
```text
# measurement-only ADR; no code change to gate. Confirm:
git diff --check                 # only the two plan docs + results doc
git status --short               # no crates/toron/src change
# the board rows are live persisted sf=10 (perf + correctness), not matrix cache
```

## Risk / blast radius
- **Reading matrix cache as truth** -> live perf/correctness only.
- **Green by implication** -> 365 oracle or `reach_only_no_oracle`.
- **Bundling a strike** -> 398 is measurement-only; the strike is 399.
- **Control regression** -> q19/q04 recorded; a regression there is a 397 finding, surfaced not hidden.

## Seal
`plan: adr 398 sf10 post 397 mini atlas` (the dual). The board results seal as `impl: adr 398 sf10 post 397 mini atlas`.
Do not commit `.perf.data`.
