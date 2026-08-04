# ADR 380 - Implementation plan: parallel per-partition build/probe (bounded spawn_blocking)

Pairs with `plans/adr/0380_parallel_partition_build_probe.md`. Parallelize the **owned post-repartition per-partition
build/probe** (not the scan loop) via bounded `spawn_blocking` + K-semaphore (379). Determinism = acceptance line 1; K
bounds all resident terms; K=1 sequential fallback is valid.

## Phase 0 - ownership mapping + owned work item (before code)
- At `visit_partitioned_streaming_scan_probe_group_chunks` (streaming.rs:3423), draw the boundary: scan/read/filter/
  repartition stays sequential/grouped; the CPU build/probe of an **owned** partition unit is what parallelizes.
- **Define `LoadedPartitionWork` (or compile-prove the moved item Send+'static):** owns preloaded build pages, index
  inputs, owned probe rows, `key_plan` (Copy), Arc schemas, Arc/owned predicates, owned budget snapshot, telemetry sink
  (Arc or per-task accumulator). **Confirm `SpilledInnerJoinBuild` (or the snapshot) is `Send + 'static`** - if not,
  carry the loaded pages + index inputs in the snapshot instead of the builder.
- **Preload async spill reads** into `LoadedPartitionWork` before `spawn_blocking`; the closure is pure CPU. If a shape
  needs lazy reads -> **named stop-line, K=1 for it** (record; do not bury async in the blocking task).
- **K terms:** enumerate the per-partition resident peak = loaded pages + rebuilt index + cache/writer/page overhead +
  the ordered-emit output slot. K = min(configured_parallelism, cores, floor(parallel_memory_budget / that peak)).
- **Skew-safe resident calibration:** do not compute K from an arbitrary first partition. Use the active group's
  conservative maximum (metadata/loaded-page measurements for all active partitions, or calibrate the largest
  metadata-estimated partition before launch). If only a non-representative single-partition measurement is available,
  use K=1 or name the stop-line.

## D1 - the parallel build/probe
- Keep the sequential scan/read/filter/repartition producing owned `LoadedPartitionWork` per partition (pages
  preloaded).
- Launch up to K `spawn_blocking` CPU tasks (build index + probe), gated by a `tokio::sync::Semaphore` sized to K. Each
  task returns `(partition_index, Vec<RecordBatch>)`.
- Collect into partition-indexed slots; **emit in ascending partition order** after the group; no executor-frame borrow.
- `configured_parallelism` knob (default `available_cores`, `=1` => sequential path). K<2 => today's sequential loop.
- Keep budget/producers/accounting correct; assert resident (all terms) under `parallel_memory_budget`. Topology-named;
  no qNN; scan not parallelized.

## Tests / safety net
- **Determinism (line 1):** parallel output digest == sequential output digest (strict + dev) for the q07/q03-shaped
  inner chain (multi-batch, multi-partition, null/residual keys); **`configured_parallelism=1` byte-reproduces** the
  pre-380 sequential output.
- **Send + 'static:** the moved work item compiles as `Send + 'static` (a `fn assert_send_static<T: Send + 'static>()`
  bound, or `LoadedPartitionWork` proven); no borrowed ref crosses.
- **K-bound all terms:** a synthetic case asserts the resident peak (pages + index + overhead + output slots) under K
  stays < `parallel_memory_budget`; K computed from the active-group conservative all-term peak.
- **Skew guard:** a small-first / large-later partition case must not over-admit K; the largest active partition (or a
  conservative bound for it) controls K.
- **K<2 fallback:** forced K=1 path == today's sequential (digest + behavior).
- **Ordered emit:** ascending partition order regardless of task completion order (stress with skewed task durations).
- **q03/q07 small-scale strict `pass@compare`** preserved.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01   # strict pass@compare
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01
# sf=10 measure (persisted, fixed 4G), q07 sampled speedup + q03/q05/q10 re-measure:
#   ... perf --sf 10 --query q07,q03,q05,q10 --engines toron --data-source parquet --data-dir target/tpch/data ...
#   determinism: run with configured_parallelism=1 vs default; digests equal
git diff --check
#   determinism digest parity (=1 reproduces); Send+'static proven; K all-term bounded; K<2 fallback; ordered emit;
#   q07 speedup/next-bottleneck + q03/q05/q10 re-measured; no cap bump; --no-fail-fast green; no qNN; scan not parallel
```

## Results doc
`crates/toron/plans/impl/0380_parallel_partition_build_probe_results.md`:
- the ownership map (LoadedPartitionWork / Send+'static proof; what was converted from borrowed; preload vs lazy-read
  stop-line);
- the K terms (pages + index + overhead + output slots) + the active-group resident calibration (including skew
  handling) + computed K;
- determinism results (digest parity strict/dev; `=1` reproduces sequential);
- K<2 fallback + ordered-emit test results;
- **sf=10:** q07 sampled speedup or named next bottleneck; q03/q05/q10 re-measure (green/advanced/unchanged);
- confirmation resident bounded under K, no cap bump, scan not parallelized, `--no-fail-fast` green, no qNN.

## Risk / blast radius
- **K under-counts terms or calibrates from a small first partition** -> peak regression; assert active-group all-term
  resident under K.
- **Borrowed ref crosses** -> compile error / snapshot; the Send+'static test gates it.
- **Hidden async in spawn_blocking** -> preload or named stop-line.
- **Nondeterminism** -> partition-indexed ascending emit + sequential reduction + fixed seeds; `=1` reproduces.
- **K=1 forced** -> valid named sequential outcome.
- **Regression** -> `--no-fail-fast`.

## Seal
`impl: adr 380 parallel partition build probe`. Do not commit wip drafts or large `.perf.data`.
