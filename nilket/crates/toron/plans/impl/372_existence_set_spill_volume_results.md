# ADR 372 results - semi/anti existence-set spill-volume

## Summary

ADR 372 implemented the measured-reducible lever for the common `semi/anti existence-set spill writer` wall:
project the existence-set spill payload to the columns required by keys and residuals, and keep no-residual membership
shapes keys-only. The 512 MiB spill budget was not changed.

The cut removed the immediate spill-volume stop for both target shapes, but it did **not** produce sf=10 pass rows within
the fixed-cap measurement window. Both q04 and q21 advanced into bounded CPU-bound execution with no memory/spill row
before manual cutoff. That is an honest wall move, not a green claim.

## Phase 0

The producer was the same for both target cells before the cut:

| query | producer before | requested | available | over |
| --- | --- | ---: | ---: | ---: |
| q04 | semi/anti existence-set spill writer | 536,881,184 | 536,870,912 | 10,272 |
| q21 | semi/anti existence-set spill writer | 536,876,880 | 536,870,912 | 5,968 |

Required existence columns:

| query | shape | written columns before | required columns | lever classification |
| --- | --- | --- | --- | --- |
| q04 | semi EXISTS, no correlated residual | full existence payload | join key only | projection + membership-only key index |
| q21 | semi/anti with correlated non-key residual | full existence payload | join key + residual payload | projected spill payload |

The q04 residual-free shape does not need page payloads at all: membership is determined by key presence. The q21 shape
does need projected pages because the non-key residual must be evaluated per candidate.

## Implementation

- Added projection support to `SpilledSemiAntiExistenceBuild`.
- Added projected page writes for residual-bearing semi/anti existence builds.
- Restored projected pages to the full logical schema before residual evaluation, so expression ordinals remain stable.
- Added a membership-only writer for no-residual semi/anti existence builds. It indexes keys and writes no spill pages.
- Routed streaming semi/anti runtime setup through the projected or membership-only path according to the measured stage.

q18 and q22 are not claimed by this ADR; both were still scan-output misses when ADR 372 started.

## Fixed-Cap sf=10 Probe

Command shape:

```text
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
    --query <query> --engines toron --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr372_<query>
```

Observed outcome:

| query | result |
| --- | --- |
| q04 | no row before manual cutoff; child stayed CPU-bound and bounded in memory, observed at ~9m with ~429 MiB RSS |
| q21 | no row before manual cutoff; child stayed CPU-bound and bounded in memory, observed at ~9m with ~828 MiB RSS |

No `spill_budget_exhausted` row was emitted for the old producer in these post-cut runs. Because neither row completed,
there is no persisted sf=10 correctness claim for q04 or q21 in this ADR. The next wall is a time/perf path, not a
memory/spill budget row.

## Tests

Focused tests:

```text
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron \
  membership_only_projected_semi_anti_existence_writes_no_pages -- --nocapture
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron \
  projected_semi_anti_existence_build_keys_only_matches_unprojected -- --nocapture
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron \
  projected_semi_anti_existence_build_retains_residual_columns -- --nocapture
```

All focused tests passed. They cover:

- no-residual membership writes zero pages and zero spill bytes;
- keys-only projected membership matches the unprojected spill path;
- residual-bearing projected pages retain required residual columns and match the unprojected path.

Regression and harness gates:

```text
scripts/cargo-cgroup fmt -- --check
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron-tpch
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch -- correctness --query q04 --sf 0.01
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
```

Results:

- `toron --no-fail-fast`: pass.
- `toron-tpch`: pass.
- q04 sf=0.01 strict correctness: pass.
- q21 sf=0.01 strict correctness: pass.
- release build: pass.

## Guardrails

- No cap bump: `spill_bytes_reserved_per_query` remains `512 * 1024 * 1024`.
- No qNN or ADR372 references in executor core.
- No temporary instrumentation remains.
- q18/q22 remain out of scope.

## Forward

ADR 372 removes the just-over-budget existence-set spill-volume producer, but q04/q21 now expose a bounded CPU/time path
at sf=10. The next decision is whether to treat q04/q21 as part of the time/perf arc, or to add a query-runtime cutoff to
the atlas so these long bounded executions produce a formal time-wall row instead of requiring manual interruption.
