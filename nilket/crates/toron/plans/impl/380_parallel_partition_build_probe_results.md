# ADR 380 results - parallel per-partition build/probe

## Summary

ADR 380 implements the first executor parallelism cut for the 377/378 partitioned streamed inner-build path. The scan,
filter, repartition, and spill-write stages remain sequential/grouped. Parallelism starts only after a partition's build
pages have been read into an owned work item; the `spawn_blocking` closure rebuilds the index and probes in CPU-only
code.

The implementation is behavior-gated and deterministic. It does not claim an sf=10 speedup yet: the q03 release smoke
did not produce a row inside the smoke window and was interrupted, so the results record no banked perf win from 380.

## Ownership map

The implementation uses a new `LoadedPartitionWork` snapshot rather than moving `SpilledInnerJoinBuild` into the
blocking task.

Owned by `LoadedPartitionWork`:
- preloaded build pages (`Vec<RecordBatch>`);
- the partition probe batch;
- probe/build key ordinal vectors;
- `JoinKeyPlan`, null-safe flag, output schemas, projection metadata, and the memory-cap snapshot.

Kept out of `spawn_blocking`:
- `&self`;
- `&mut F` visitors;
- borrowed `StreamingScanProbeInput<'_>`;
- async `AsyncFileOps` page reads.

`SpilledInnerJoinBuild::read_all_batch_pages` runs before task launch, then the blocking task uses a loaded-page
`LoadedSpilledInnerBuild` to rebuild the index and materialize matches without async reads. A focused test compile-proves
the moved work item is `Send + 'static`.

If a later shape requires lazy async spill reads inside the task, that remains the named stop-line. This cut does not
hide async work in `spawn_blocking`.

## K bound

`ExecutionContext::partition_parallelism` controls requested partition parallelism. The default is
`available_parallelism()`, and `TORON_PARTITION_PARALLELISM=N` can force a value for reproducibility; `1` takes the
sequential fallback.

For each active partition group, the implementation computes K from the active group's maximum estimated resident peak,
not from the first partition:

- loaded page bytes from spill metadata;
- rebuilt index estimate from row count and key plan;
- writer/page overhead;
- partition probe-batch bytes;
- ordered output-slot backlog guarded at runtime by `parallel streamed inner join ordered output slots`.

The runtime semaphore is sized to:

```
min(configured_parallelism, available_cores, retained_footprint_budget / active_group_resident_peak)
```

`K < 2` falls back to the existing sequential per-partition path. The output-slot term is additionally enforced with a
runtime memory guard before partition-indexed results are retained for ascending-order emit.

## Determinism

Parallel tasks return `(partition_index, output_batches, probe_rows, output_rows)`. Results are collected into
partition-indexed slots and emitted in ascending partition order, independent of completion order.

The focused determinism test compares the forced sequential path (`partition_parallelism=1`) against the default
parallel path on the same partitioned scan-build fixture:

- identical probe row count;
- identical output row count;
- identical batches;
- identical canonical digest.

## Perf note

The release q03 sf=10 smoke command was started with the persisted dataset:

```
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- perf \
  --sf 10 --mode strict --runs 1 --warmup 0 --query q03 --engines toron \
  --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf/adr380
```

The release build completed, but the q03 run exceeded the intended smoke window and was interrupted. This result is not
banked as a speedup or green. The honest 380 performance outcome is: implementation landed with deterministic parallel
mechanics, but no sf=10 speedup claim is recorded yet.

A bounded q03 sample was then captured for 90 seconds:

```
timeout 90s perf record -F 99 -g --call-graph dwarf \
  -o target/tpch/perf/adr380/q03_sample.perf.data -- \
  target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 --query q03 \
    --engines toron --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf/adr380
```

The sample timed out as expected and captured 8,692 samples. The report was coarse because the rebuilt release binary did
not retain Rust function symbols, but it still names the next bottleneck class:

- allocator/zero/copy pressure (`__memset_avx2_unaligned_erms`, `_int_malloc`, `__memmove_avx_unaligned_erms`,
  `__libc_malloc2`, `free`);
- shmem spill reads (`pread64` -> `shmem_file_read_iter` / `_copy_to_iter`);
- worker coordination / wait overhead (`futex` / schedule stacks).

So 380 does not bank a speedup. The next perf evidence is a profiled class-level bottleneck: parallel partition work is
now limited by spill read/copy/allocation and worker coordination. A future symbol-retaining profile should name the
exact Toron frame before optimizing further.

## Verification

Passed:

```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron loaded_partition_work_is_send_static --lib
scripts/cargo-cgroup test -p toron parallel_partition --lib
scripts/cargo-cgroup test -p toron parallel_partitioned_scan_build_matches_sequential_ordered_emit --lib
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01
```

Focused results:
- `loaded_partition_work_is_send_static`: pass.
- `parallel_partition_plan_uses_active_group_max_not_first_partition`: pass.
- `parallel_partitioned_scan_build_matches_sequential_ordered_emit`: pass.
- q07 strict sf=0.01 correctness: `status=pass`.
- q03 strict sf=0.01 correctness: `status=pass`.

Additional checks:
- no qNN references added under `crates/toron/src/exec.rs` or `crates/toron/src/exec/join`;
- no cap/budget constants changed;
- scan build/probe scans remain sequential/grouped.

## Verdict

The mechanics cut is implemented and guarded: owned CPU work, async preload, bounded K, deterministic ordered emit, K=1
fallback, and full-suite regression coverage. No sf=10 speedup is claimed. The bounded sample names the next
class-level bottleneck as spill read/copy/allocation plus worker coordination; an exact Toron-frame optimization should
start from a symbol-retaining profile.
