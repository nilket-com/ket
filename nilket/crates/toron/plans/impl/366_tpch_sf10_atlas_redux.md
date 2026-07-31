# ADR 366 implementation plan - TPC-H SF=10 atlas redux

Pairs with `plans/adr/366_tpch_sf10_atlas_redux.md`.

This is a measurement ADR. It produces a live sf=10 board from the persisted Parquet data surface and classifies the
walls. ADR 365's persisted correctness plumbing is a prerequisite. This ADR must not change `crates/toron` engine
behavior or budget constants.

Known entering board from ADR 365:

- q14: persisted correctness-green at sf=10.
- q06: persisted perf and correctness both fail at `scan output accumulator`
  (`memory_budget_exhausted`, 24,448 bytes over the 1 GiB accumulator cap). This is a confirmed engine red, not a
  correctness-path divergence.
- q21: persisted correctness reaches `spill_budget_exhausted`, matching the current q21 frontier from ADR 364.
- q17: persisted correctness remains a named `q17_sf10_oracle_policy_gap`.
- The other 18 queries are unknown until this live atlas runs.

## Phase 0 - artifact and runner sanity

Verify the dataset:

```bash
test -f target/tpch/data/strict/sf10/manifest.json
```

If missing, prepare it outside the query measurement run:

```bash
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch prepare \
    --data-source parquet --mode strict --sf 10 --data-dir target/tpch/data
```

Build the release binary before classification:

```bash
scripts/cargo-cgroup build -p toron-tpch --release
```

Smoke the anchors, each in its own fixed-cap scope:

- q06: named `scan output accumulator` red expected.
- q14: Toron pass expected.
- q21: expected named next wall at `semi/anti existence-set spill writer` unless newer work changed it.
- q17 correctness: named `q17_sf10_oracle_policy_gap` expected.

Use the canonical wrapper:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf \
    --sf 10 --mode strict --runs 1 --warmup 0 --query <query> \
    --engines toron,duckdb \
    --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr366/<query>
```

Gate: inspect `latest_raw.jsonl` for the anchors and confirm rows contain the required fields:

- `engine`
- `query`
- `status`
- `reason`
- `diagnostic_message`
- `execute_ms`
- `peak_rss_kib`
- `spill_bytes`
- `plan_fingerprint`
- `flags`

If a combined Toron+DuckDB invocation loses rows after one engine fails, switch the full board to separate
per-query/per-engine invocations and record that in the results.

Treat q06 as a seed red when validating the runner. If q06 passes, that is a new finding; if it fails at the same
producer, the runner is behaving as expected.

## Phase 1 - full board

Run all 22 queries, isolated by query. Default shape:

```bash
for q in q01 q02 q03 q04 q05 q06 q07 q08 q09 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22; do
  rm -rf "target/tpch/perf_adr366/${q}"
  mkdir -p "target/tpch/perf_adr366/${q}"
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
    target/release/toron-tpch perf \
      --sf 10 --mode strict --runs 1 --warmup 0 --query "${q}" \
      --engines toron,duckdb \
      --data-source parquet --data-dir target/tpch/data \
      --output-dir "target/tpch/perf_adr366/${q}"
done
```

If a query produces no row, do not rerun it with a larger cap. Record:

- systemd/cgroup outcome if visible;
- whether any raw JSONL exists;
- last visible phase if the rail reports one;
- whether the failure happened before Toron, before DuckDB, or in one engine only.

## Phase 2 - correctness provenance

For each Toron `pass` row, run live sf=10 correctness through ADR 365's persisted correctness path:

```bash
target/release/toron-tpch correctness \
  --query <query> --sf 10 --data-source parquet --data-dir target/tpch/data
```

Do not use `matrix` or `correctness-matrix` for sf=10 evidence.

If a query still cannot use the persisted correctness route, record that as an atlas blocker. Do not replace it with a
cached, sf=1, or sf=0.01 correctness result.

q17 is already expected to land here as `q17_sf10_oracle_policy_gap` unless its oracle policy is deliberately extended
in a separate cut. Do not count q17 as correctness-green from its lower-scale oracle.

## Phase 3 - classify and rank walls

Create `crates/toron/plans/impl/366_tpch_sf10_atlas_redux_results.md` with:

1. Runtime setup:
   - release commit,
   - fixed cap,
   - dataset manifest fingerprint,
   - DuckDB version,
   - run count and warmup.
2. Full table:
   - query,
   - Toron status/reason/producer/requested/available,
   - Toron execute_ms/RSS/spill bytes/plan fingerprint,
   - DuckDB status/execute_ms/RSS/flags,
   - correctness provenance.
3. Common-wall ranking:
   - producer,
   - count of queries,
   - affected queries,
   - dimension,
   - representative requested/available bytes.
4. Next-cut recommendation:
   - one highest-leverage repeated producer,
   - why it beats continuing q21-only,
   - acceptance shape for the next ADR.

Classification rules:

- A red row with `diagnostic_message` is classified by producer/dimension/bytes.
- A no-row cgroup kill is classified as `no_row_oom` unless the rail provides a last known phase.
- A structural-only expected downstream wall is not a measured classification.
- A DuckDB schema or stability flag is recorded, not dropped.

## Verification

The ADR is measurement-only, so the main verification is artifact integrity rather than full engine tests.

Required:

```bash
scripts/cargo-cgroup build -p toron-tpch --release
git diff --check
```

If any harness code changes are needed, add:

```bash
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron --no-fail-fast
```

No `crates/toron/src` changes are expected. If an engine change becomes tempting, stop and write a follow-up ADR from
the atlas result.

## Seal

Commit as:

```text
impl: adr 366 tpch sf10 atlas redux
```

Only commit the results and any measurement-only harness plumbing. Do not commit engine fixes under ADR 366.
