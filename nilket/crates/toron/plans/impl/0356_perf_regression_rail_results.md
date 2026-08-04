# ADR 356 Results - perf/regression rail

## Implementation

`crates/toron-tpch` now has a dedicated Toron-only `perf` command:

```text
toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query all
```

The visible driver spawns a fresh child process per warmup/measured run through the hidden `perf-run-one` mode. The
child emits exactly one JSON record to stdout; the parent records parent-observed wall time and child peak RSS via
`wait4`/`ru_maxrss` on Unix. The `engine` column is present and currently always `toron`; live DuckDB execution is
deferred.

The raw per-run rows use `suite = "tpch_perf"` and include:
- `engine`, `query`, `scale_factor`, `mode`, `run_index`, `warmup`, `profile`;
- phase timings: `load_ms`, `plan_ms`, `bind_ms`, `execute_ms`, `digest_ms`, `total_ms`;
- `parent_wall_ms`, `peak_rss_kib`, `spill_bytes`, `plan_fingerprint`, `digest`;
- `status`, `reason`, and `diagnostic_message`.

The summary rows use `suite = "tpch_perf_summary"` and record measured-run median/min/max/spread for `execute_ms`,
max RSS, max spill bytes, digest, plan fingerprint, and recorded flags. Flags are non-fatal:
- `debug_build`;
- `unstable`;
- `plan_changed`;
- `slower_than_baseline`.

`latest_raw.jsonl` and `latest_summary.jsonl` are always written. A baseline is admitted only when every summary row is
release, passing, and stable; debug, failing, or unstable samples are recorded but cannot silently become the regression
reference.

`elapsed_ms` remains reachability telemetry only. It is not emitted in perf rows.

## Phase Timing And Counters

The shared TPC-H load -> plan -> bind -> execute helper now has additive phase timers. Reachability and correctness
still call the same substrate through the existing wrapper; perf calls the timed variant and then separately times digest
generation.

`TaskTelemetry` is installed on the `ExecutionContext` for perf runs and read after execution/failure, so the row reports
engine spill bytes without changing normal reachability/correctness behavior.

Plan fingerprints are deterministic physical-plan hashes over the bound plan representation. They are used to separate
same-plan timing regression flags from plan-change flags.

## First Finding - live sf=1 disagreed with cached matrix rows

The rail's first all-query smoke found that live release `q08`/`q09` at sf=1 failed with a `join output
materialization` `memory_budget_exhausted` wall:

```text
producer: "join output materialization"
requested: ByteCount(1975900039)
available: ByteCount(1073741824)
reason: memory_budget_exhausted
```

Direct release `run --query q08 --mode strict --sf 1` and `run --query q09 --mode strict --sf 1` confirmed this was
not a perf-driver artifact. The existing `matrix`/`correctness-matrix` commands render latest cached rows from JSONL;
they do not live-rerun the board and do not filter by requested scale. The historical "sf=1 22/22" claim was therefore
weaker than presented: the only live all-query sf=1 measurement before the follow-up fix was the perf rail, and it
correctly surfaced q08/q09 as red.

ADR 356 intentionally keeps the engine unchanged. The q08/q09 streaming generalization is split to ADR 357 so the
behavior change has its own decision, tests, and review.

## Baseline Status

The rail can produce a full recorded baseline, but a clean all-22 sf=1 baseline is deferred until ADR 357 because the
rail correctly exposed q08/q09 as live failures under the pre-357 engine.

Smoke commands run for ADR 356:

```text
scripts/cargo-cgroup run -p toron-tpch --release -- perf --sf 0.01 --mode strict --runs 1 --warmup 0 --query q06 --output-dir target/tpch/perf-smoke-release
scripts/cargo-cgroup run -p toron-tpch -- perf --sf 0.01 --mode strict --runs 1 --warmup 0 --query q06 --output-dir target/tpch/perf-smoke
scripts/cargo-cgroup run -p toron-tpch --release -- perf --sf 1 --mode strict --runs 1 --warmup 0 --query all --output-dir target/tpch/perf-sf1-smoke
```

The release q06 smoke produced one passing release row with phase timings, RSS, digest, spill bytes, and no
`elapsed_ms`. The debug q06 smoke produced a passing row with `debug_build`, proving debug measurements cannot silently
become a release baseline. The sf=1 all-query smoke produced raw/summary rows and surfaced q08/q09 as live failures.

## Verification

All commands were run through `scripts/cargo-cgroup`.

- `fmt -- --check`
- `check -p toron-tpch --all-targets`
- `test -p toron-tpch`
- release `perf --sf 0.01 --mode strict --runs 1 --warmup 0 --query q06`
- debug `perf --sf 0.01 --mode strict --runs 1 --warmup 0 --query q06`
- release `perf --sf 1 --mode strict --runs 1 --warmup 0 --query all` - rail completed and surfaced q08/q09 failures
- direct release `run --query q08 --mode strict --sf 1` - confirmed pre-357 live failure
- direct release `run --query q09 --mode strict --sf 1` - confirmed pre-357 live failure
- `git diff --check`

## Carry-Forward

Follow-ups made explicit by this rail:
- ADR 357 owns the q08/q09 reoriented-inner-chain streaming generalization and the first clean all-22 sf=1 baseline.
- Add live-rerun support to reach/correctness matrices, or clearly label them as cache renders, so cached rows cannot
  certify a stale scale board.
- Add progress output to the perf parent on stderr; the full fresh-process baseline is intentionally quiet until final
  summary emission, which is correct for JSON hygiene but poor operator feedback during hour-scale runs.
