# ADR 380 - first parallel cut: owned per-partition build/probe via bounded spawn_blocking

## Context

Implementation of the 379 design (mechanism + invariants settled). Parallelize the **owned per-partition build/probe
work** of the partitioned spill/join family (q07/q03/q05/q10) via **bounded `spawn_blocking` + a K-semaphore** (379's
chosen mechanism; `tokio::spawn` rejected on 135 ms tick-gap starvation). **Determinism is acceptance line 1.**

**Precision on the seam:** `visit_partitioned_streaming_scan_probe_group_chunks` (streaming.rs:3423) is **still a scan
loop** - its build/probe **scans stay sequential/grouped** for this cut. **Parallelism begins only *after* a probe batch
is read / filtered / repartitioned and the partition work is owned.** The build scan stays 378's one-pass. If first-
partition measurement forces K=1, that is a **valid named outcome**, not a failure.

## Decision

### Phase 0 (load-bearing) - ownership mapping + the owned work item
- **No borrowed/executor-frame ref crosses the task boundary** - not `&self`, not `&mut F`, not the borrowed
  `StreamingScanProbeInput<'_>`, not borrowed `BoundExprProgram` predicates.
- **Either compile-prove the exact moved work item is `Send + 'static`, or introduce a smaller owned
  `LoadedPartitionWork` snapshot** carrying everything the CPU build/probe needs: the **preloaded** partition build
  pages, the (to-be-)rebuilt index inputs, the owned probe rows, `key_plan` (Copy), Arc schemas, Arc/owned predicates,
  an owned budget snapshot, and a telemetry sink (Arc or per-task accumulator merged after). `SpilledInnerJoinBuild`
  moving in cleanly is **not assumed** - prove it or snapshot it.
- **Async reads are preloaded, never hidden in the blocking task.** The spill page reads (async `AsyncFileOps`) happen
  **before** `spawn_blocking`, populating `LoadedPartitionWork`; the `spawn_blocking` closure is **pure CPU**. If the
  probe path genuinely needs lazy async reads that cannot be preloaded, that is a **named stop-line** (don't bury async
  in the blocking task) - record it, ship K=1 for that shape.
- **K-semaphore** acquired before launching a partition's CPU task, released on completion.
- **Resident calibration** -> compute K. Do **not** trust an arbitrary first partition under skew: use a conservative
  `measured_partition_resident_peak` for the active group (max over active partitions from metadata/loaded-page
  measurements, or calibrate the largest metadata-estimated partition before launch). If the implementation can only
  measure one non-representative partition, that shape falls back to K=1 or names the stop-line.

### The K-bound must cover EVERY concurrently resident term
K is **not** index-only and is **not first-partition-only**. `measured_partition_resident_peak` is the conservative
active-group maximum of **all** terms live during a partition's `spawn_blocking` work:
- the **loaded page batches**, the **rebuilt index**, **cache/writer/page overhead**, and
- the **partition-indexed output slots waiting for ordered emit** (the ordered-emit backlog is resident too).

```
K = min(configured_parallelism, available_cores,
        floor(parallel_memory_budget / measured_partition_resident_peak))   // peak = ALL terms above
```

K<2 -> sequential fallback, no speedup claim. Parallelism must not silently reintroduce the peak 378 bounded, including
when later partitions are larger than the first active partition.

### The cut
- Keep the scan/read/filter/repartition loop sequential/grouped (378). After a partition's work is **owned**
  (`LoadedPartitionWork`, pages preloaded), launch up to K CPU build/probe tasks via `spawn_blocking`, K-semaphore
  bounded.
- Each task returns its output; collect into **partition-indexed slots**; **emit in ascending partition order**.
- K from the active-group resident bound (all terms); `configured_parallelism` knob (default cores, `=1` for
  determinism).
- Topology-named; no qNN in core; scan not parallelized.

## Acceptance
- **Line 1 - determinism:** **parallel output digest == sequential output digest** (strict + dev), and
  **`configured_parallelism=1` reproduces today's sequential path exactly** (partition-indexed slots, ascending emit,
  sequential per-partition reduction, fixed hash seeds).
- **Ownership proven:** the moved work item is `Send + 'static` (compile-proven) or a `LoadedPartitionWork` snapshot;
  no borrowed/executor-frame ref crosses; async reads preloaded (or the lazy-read stop-line is named).
- **K bounds all resident terms** (pages + index + cache/writer/page + ordered-emit output slots); **resident peak
  under `parallel_memory_budget` asserted**; skewed active partitions cannot under-calibrate K; **no cap bump.**
- **K<2 sequential fallback tested** (== today; no fake speedup).
- **Perf:** q07 sampled speedup (373 method) **or a named next bottleneck**; q03/q05/q10 re-measured.
- **`--no-fail-fast` required**; de-branded; prior greens unchanged; **scan not parallelized.**

## Guardrails
- **Determinism is acceptance line 1** - parallel == sequential digest; `=1` reproduces today.
- **Nothing borrowed crosses the task** - owned `LoadedPartitionWork` / compile-proven Send+'static; no `&self`/`&mut F`/
  borrowed input/predicates.
- **Async reads preloaded, not hidden** - spawn_blocking is pure CPU; lazy-read need => named stop-line, K=1.
- **K bounds every resident term** - active-group max of pages + index + overhead + ordered-emit slots; resident <=
  budget; no cap bump.
- **Scan stays sequential/grouped** - parallelism only on owned post-repartition work.
- **K<2 = sequential fallback (valid); --no-fail-fast; de-branded; no qNN.**

## Risks
- **K under-counts terms or trusts a small first partition** -> memory peak regresses past 378; the peak must include
  pages + index + overhead + output slots, asserted under K for the active group's conservative maximum.
- **Work item not Send+'static** -> compile-prove or snapshot; no borrowed ref crosses.
- **Hidden async in spawn_blocking** -> preload, or name the lazy-read stop-line; don't bury it.
- **Nondeterminism leak** -> partition-indexed ascending emit + sequential per-partition reduction + fixed seeds.
- **K=1 forced** -> valid named outcome (sequential), not a failure.
- **Regression** -> `--no-fail-fast`.

## Forward arc
380 (q07/q03/q05/q10 owned per-partition parallel) -> measure speedup vs DuckDB -> extend to q17/q19/q21 + other shapes
-> memory-tail cells (q02/q08/q09/q18/q22/q21). The K-bound (all terms) + determinism gate ride forward unchanged.
