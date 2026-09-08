# Toron Bench Scripts

## Runtime Filter Perft

`run_runtime_filter_perft.sh` bootstraps the ADR 006/007 selective runtime-filter
bench evidence. It runs the hash-partitioned `distributed_inner_equi_join`
matrix across runtime-filter and Wendelon-filter modes, captures environment
metadata, extracts structured rows, and writes a Markdown summary.

```bash
crates/toron/scripts/bench/run_runtime_filter_perft.sh --mode smoke
crates/toron/scripts/bench/run_runtime_filter_perft.sh --mode selective
crates/toron/scripts/bench/run_runtime_filter_perft.sh --mode full
crates/toron/scripts/bench/run_runtime_filter_perft.sh --mode smoke --samply
```

Outputs are written under `target/toron-perft/<mode>-<timestamp>` unless
`--out-dir` or `TORON_PERFT_OUT_DIR` is provided:

- `environment.txt` records revision, dirty status, toolchain, CPU governor,
  and selected bench dimensions.
- `command.txt` records the exact cargo command.
- `raw.log` stores full bench output.
- `rows.csv` stores only `adr004_distributed_inner_equi_join` rows.
- `summary.md` reports digest equality, probe-row reduction, shuffle-byte
  reduction, bytes-cluster reduction, wall-time reduction, and implied Bloom
  false-positive rate.
- `samply.json.gz` is written when `--samply` or `TORON_PERFT_SAMPLY=1` is set.
- `samply-build.log` is written before a `samply` run while locating the bench
  executable.

On Linux, `samply` needs access to perf events. If
`/proc/sys/kernel/perf_event_paranoid` is too restrictive, the runner preserves
the failure in `raw.log` and exits nonzero. Use `enable_profiling.sh` (below)
to flip the kernel gates on a dev machine.

## Profiler Permission Bootstrap

`enable_profiling.sh` flips Linux kernel knobs that gate `samply` / `perf` /
`gdb attach` on dev machines. Closes the recurring blocker recorded in ADR 005
and ADR 006 bench artifacts (`perf_event_paranoid=4`, `ptrace_scope=1`).

```bash
crates/toron/scripts/bench/enable_profiling.sh --verify   # show current state + samply probe
crates/toron/scripts/bench/enable_profiling.sh --apply    # record prior state, set dev targets (sudo)
crates/toron/scripts/bench/enable_profiling.sh --revert   # restore prior state from state file
```

Default target values (override via `TORON_PERFT_PERF_PARANOID` /
`TORON_PERFT_PTRACE_SCOPE`):

- `/proc/sys/kernel/perf_event_paranoid` → `1` (allow user-space events)
- `/proc/sys/kernel/yama/ptrace_scope` → `0` (allow same-user ptrace)

The `--apply` mode records prior values to
`target/toron-perft/profiling-state.txt` so `--revert` is mechanical. Changes
are runtime-only (no `/etc/sysctl.d/` write) and do not survive reboot. Not
for production hosts.

Modes:

- `smoke`: 2 workers, 2K build rows, 8K probe rows, 32K probe key domain.
- `selective`: 8 workers, 100K build rows, 10M probe rows, 10M probe key domain.
- `full`: 8 workers, 100K build rows, 100M probe rows, 10M probe key domain.
- `custom`: dimensions come from `TORON_PERFT_WORKERS`,
  `TORON_PERFT_BUILD_ROWS`, `TORON_PERFT_PROBE_ROWS`, and
  `TORON_PERFT_PROBE_KEY_DOMAIN`.

`TORON_PERFT_WORKERS` may be a comma-separated worker-count list.

The runner validates digest equality and requires at least 50% probe-row and
shuffle-byte reduction by default. `selective` and `full` also require at least
50% reduction in the ADR bytes-movement cluster; `smoke` skips that timing gate
because the fixture is too small for stable millisecond deltas. Override the
thresholds with `TORON_PERFT_MIN_PROBE_REDUCTION_PCT`,
`TORON_PERFT_MIN_SHUFFLE_REDUCTION_PCT`, and
`TORON_PERFT_MIN_CLUSTER_REDUCTION_PCT`. Set
`TORON_PERFT_MIN_CLUSTER_REDUCTION_PCT=` to disable the timing gate.

Existing logs can be summarized directly:

```bash
crates/toron/scripts/bench/summarize_runtime_filter_perft.py \
  crates/toron/plans/impl/0006_runtime_filter_pushdown_bench_results.md
```

A Rune port of that summarizer sits beside it and prints the same report:

```bash
rnx run crates/toron/scripts/bench/summarize_runtime_filter_perft.rn \
  crates/toron/plans/impl/0006_runtime_filter_pushdown_bench_results.md
```

Given no path, both read standard input, so either can sit in a pipeline
behind a bench run. The two are kept in step by
`fixtures/summarize_runtime_filter_perft/compare.sh`, which runs both over
nineteen cases and checks that the report, the exit code, and the diagnostics
all agree, with no exceptions.
