# ADR 400 - Implementation plan: sf=10 post-399 reach re-check

Pairs with `plans/adr/0400_sf10_post_399_reach_recheck.md`. Run a focused
measurement-only sf=10 mini-board after ADR 399 and decide whether reach moved
or the next cut should pivot to the SQL/Rune planner-shape lever.

## Phase 0 - prepare

1. Confirm the release binary includes ADR 399 and the determinism note.
2. Use the persisted sf=10 Parquet data under the fixed 4 GB cap.
3. Record ADR 398's pinned baseline rows for q03/q05/q10/q08/q19.

## Phase 1 - live target measurements

Run the perf rail for:

- q03;
- q05;
- q10;
- q08;
- q19.

Use isolated per-query runs or a focused perf run, but keep the results live and
persisted:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  target/release/toron-tpch perf \
    --sf 10 \
    --mode strict \
    --runs 1 \
    --warmup 0 \
    --query q03,q05,q10,q08,q19 \
    --engines toron,rune,polars_sql,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr400_sf10_post399
```

If the comma query form is not supported by the local CLI, run the five queries
one at a time and write the same combined results table. DuckDB/Polars rows are
contextual; Toron/Rune reach classification is the point.

## Phase 2 - correctness provenance

For every pass row, run or cite persisted sf=10 correctness through the ADR 365
surface. If a row reaches but no oracle policy exists, mark it
`reach_only_no_oracle`; do not classify it as green.

Non-pass rows get `n/a`.

## Phase 3 - result artifact

Create:

`crates/toron/plans/impl/0400_sf10_post_399_reach_recheck_results.md`

Include:

- the filled delta table against ADR 398;
- old-vs-new producer class for each row;
- correctness provenance per pass row;
- timing and ratio context where available;
- the fork verdict.

## Verification

Because this is measurement-only, no full engine gate is required unless source
files are changed. Still run:

```text
git diff --check
```

If any source or harness code changes, run the relevant `fmt`, `check`, and
`--no-fail-fast` gates before sealing.

## Seal

Commit as:

`plan: adr 400 sf10 post 399 reach recheck`

The implementation/result commit should be:

`impl: adr 400 sf10 post 399 reach recheck`

Do not commit `.perf.data`, raw target output, or generated Parquet data.
