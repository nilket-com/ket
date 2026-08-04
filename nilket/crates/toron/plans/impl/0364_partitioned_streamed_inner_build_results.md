# ADR 364 results - partitioned streamed inner-build spill

## Summary

ADR 364 landed as a narrow partitioned streamed-inner-build path. It removes q21's streamed inner-build resident-state
wall at sf=10 under the fixed 4 GB cap and advances the query to a different named wall:
`semi/anti existence-set spill writer` `spill_budget_exhausted`.

This is the acceptance branch "named next wall", not q21 green.

## Baseline wall

From ADR 363, q21 at sf=10 failed on the streamed inner-build resident path:

- producer: `streamed inner join build spill resident state`
- reason: `memory_budget_exhausted`
- requested: `1,073,753,480`
- available: `1,073,741,824`
- over: `11,656` bytes

That is the 349 resident-index family on the reoriented streamed inner build.

## Implementation

The fix keeps the ADR scope narrow:

- reuses `streamed_spill_partition_count` and `select_join_partition_batch`;
- keeps 363's projected-write, key-remap, and schema-restore materialization;
- does not reuse the LeftOuter partitioned visitor;
- does not introduce a shared partitioned-spill substrate.

The partition topology is the simple bounded path: estimate the streamed scan build, compute the partition count against
`join_spill_retained_footprint_memory_cap(task_budget.memory.get())`, then re-scan once per active partition. Each active
partition builds one `SpilledInnerJoinBuild`, probes matching rows, materializes, and drops the partition before the next.
Only the active partition's build input, build map, and spill index are resident.

The implementation also preserves the streaming projection path: partitioned writes use the same retained-column and
build-key remap machinery as the single-spill path.

## Result at sf=10

Command:

```text
CARGO_CGROUP_BACKEND=affinity TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
    --query q21 --engines toron --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr364
```

Raw result:

- status: `fail`
- reason: `spill_budget_exhausted`
- diagnostic: `BudgetExhausted { dimension: SpillBytes, producer: "semi/anti existence-set spill writer", requested: ByteCount(536876880), available: ByteCount(536870912) }`
- over: `5,968` bytes
- execute_ms: `118028.686`
- peak_rss_kib: `320912`
- flags: none

Interpretation: the old resident-state wall is gone. q21 now reaches the semi/anti existence-set spill writer and fails
on spill byte volume, 5,968 bytes over the unchanged 512 MiB spill budget. That is a different producer and a different
budget dimension.

## Tests and verification

Formatting / compile:

- `scripts/cargo-cgroup fmt -- --check`
- `scripts/cargo-cgroup check -p toron --all-targets`

Focused tests:

- `scripts/cargo-cgroup test -p toron streamed_inner_build_partition_count_uses_effective_resident_cap -- --nocapture`
- `scripts/cargo-cgroup test -p toron partitioned_reoriented_inner_runtime_matches_single_spill_projected_path -- --nocapture`

Broader gates:

- `scripts/cargo-cgroup test -p toron --lib`
- `scripts/cargo-cgroup test -p toron --test architecture_test`
- `scripts/cargo-cgroup test -p toron --no-fail-fast`
- `scripts/cargo-cgroup test -p toron-tpch`

Correctness:

- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01` -> `pass@compare`
- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 1` -> `pass@compare`

Release / sf=10:

- `scripts/cargo-cgroup build -p toron-tpch --release`
- fixed-cap q21 sf=10 perf command above -> named next wall, no cgroup kill

Other guards:

- `git diff --check`
- no qNN references in `crates/toron/src/exec` / `crates/toron/src/exec.rs`
- spill budget unchanged at `512 * 1024 * 1024`
- retained-footprint multiplier unchanged at `16`

## Forward

q21's next wall is the semi/anti existence-set spill writer, 5,968 bytes over the 512 MiB spill budget. The next cut is
another spill-volume reduction/name-the-excess step on that producer, not a memory resident/index fix.
