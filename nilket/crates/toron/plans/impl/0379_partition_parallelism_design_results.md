# ADR 379 results - partition parallelism design

## Summary

ADR 379 is complete as a design ADR. No engine behavior ships in this cut.

The runtime-boundary spike chooses **bounded `tokio::task::spawn_blocking`** for ADR 380:

> 380 uses `spawn_blocking` because `tokio::spawn` starves async workers during CPU-heavy partition probe work, while
> `spawn_blocking` keeps the runtime responsive without adding a second executor dependency. Rayon stayed responsive in
> the spike, but it introduces a separate pool plus a tokio handoff; keep that as a fallback if real 380 measurements
> show `spawn_blocking` overhead dominates.

ADR 380's first target remains the partitioned spill/join family: q07 plus q03/q05/q10.

## Runtime-boundary spike

The spike was throwaway code under `target/adr379_spike`, removed before seal. It used:

- existing multi-thread tokio runtime (`worker_threads = 4`);
- Wendelon's real `AsyncFileOps` surface through `BlockingFileOps`;
- 4 synthetic partition tasks;
- 8 MiB spill write + read per partition through `write_plain_at` / `read_plain_at`;
- 1,500,000-row synthetic hash build/probe per partition;
- a runtime monitor task ticking every 2 ms to expose async-worker starvation;
- 3 release-mode repeats per mechanism.

The checksum matched across every run, so the three mechanisms performed equivalent work.

| mechanism | median elapsed | min | max | spread | median runtime tick gap | verdict |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `tokio::spawn` | 153.868 ms | 149.658 ms | 167.192 ms | 11.4% | 135.573 ms | rejected: CPU spans starve async workers |
| `spawn_blocking` | 150.057 ms | 138.936 ms | 180.594 ms | 27.8% | 3.170 ms | chosen: responsive, runtime-native, no new executor |
| rayon | 154.009 ms | 150.844 ms | 163.494 ms | 8.2% | 3.142 ms | fallback: responsive, but adds pool + async handoff |

The decisive signal is not median elapsed time; all three are close. The decisive signal is runtime responsiveness:
`tokio::spawn` blocks all four async workers during the CPU phase, delaying the monitor task by about 135 ms. That is the
failure mode ADR 379 needed to rule out before putting CPU-heavy partition work on the async runtime.

## Chosen runtime boundary for 380

ADR 380 should run each partition's build-index/probe work in a `spawn_blocking` task, gated by a semaphore sized from
the operational K-bound. The blocking task may use `Handle::block_on` for the existing async spill I/O boundary in the
first implementation; 380 must measure the real path afterward. If nested blocking I/O becomes the dominant overhead,
rayon or a split async-I/O/CPU design becomes a named follow-up rather than an implicit dependency in the first cut.

Required 380 implementation constraints:

- spawn no more than `K` partition tasks at once;
- hold a permit for the whole partition resident lifetime;
- collect partition outputs into partition-indexed slots;
- emit slots in ascending partition order;
- keep per-partition build/probe order sequential.

## Operational K-bound

ADR 380 uses:

```text
K = min(configured_parallelism,
        available_cores,
        floor(parallel_memory_budget / measured_partition_resident_peak))
```

Design choices:

- `configured_parallelism`: new executor/perf knob, default `available_cores`, overrideable to `1` for deterministic
  debugging and exact sequential fallback.
- `available_cores`: `std::thread::available_parallelism()`, clamped to at least `1`.
- `parallel_memory_budget`: a reservation carved from the existing resident budget, not a cap bump. For 380, reserve at
  most half of the effective resident budget for concurrent partition resident state unless Phase 0 proves a tighter
  query-local bound.
- `measured_partition_resident_peak`: measured from the first completed partition's real resident accounting on the
  target path: `resident_index_bytes` plus writer/page/cache resident terms already counted by the spill guards.
- Conservative startup: process the first partition sequentially, record its resident peak, compute `K`, then run the
  remaining partitions with at most `K` concurrent permits.
- Safety margin: use a pessimistic multiplier on the first-partition peak if partition skew is possible. If 380 cannot
  establish a skew-safe bound, keep `K = 1`.
- `K < 2`: disable the parallel path, use the current sequential path, and make no speedup claim.

This keeps the 377/378 memory invariant intact: parallelism may multiply resident partition state only within the
existing budget reservation.

## Determinism gate for 380

The first acceptance line for ADR 380:

> Parallel output digest equals sequential output digest.

The mechanism:

- each partition writes output batches into its own partition-indexed slot;
- completion order is ignored;
- slots are emitted in ascending partition order;
- per-partition reduction/probe order remains sequential;
- `configured_parallelism = 1` reproduces today's sequential path exactly.

This is required for strict and dev parity because the existing correctness oracle compares exact output.

## Named 380 cut

ADR 380: **K-bounded partition parallelism for the partitioned spill/join family**.

Scope:

- q07 canary;
- q03/q05/q10 family coverage;
- partitioned inner-build spill path only;
- no q12 sizing residual, q17/q19/q21, or memory-tail cells.

Acceptance shape for 380:

- runtime boundary: `spawn_blocking` with K semaphore;
- K-bound measured from the real first partition and resident budget;
- deterministic partition-indexed output with ascending emit;
- strict/dev digest parity vs sequential;
- q07 and q03/q05/q10 measured for speedup or named next bottleneck;
- no cap bump; no qNN; `--no-fail-fast` required.

## Verification

| item | result |
| --- | --- |
| Runtime-boundary spike | complete; numbers above |
| Spike code | throwaway under `target/adr379_spike`, removed before seal |
| Engine behavior | no `crates/toron` source changes |
| Chosen 380 mechanism | bounded `spawn_blocking` |
| Operational K-bound | specified |
| Determinism gate | specified as first 380 acceptance line |

Run final seal checks before commit:

| command | result |
| --- | --- |
| `cargo fmt --check` | pass |
| `git diff --check` | pass |
| `scripts/cargo-cgroup check -p toron --all-targets` | pass |
| no-qNN executor scan | pass |
| `scripts/cargo-cgroup test -p toron --no-fail-fast` | pass |

## Seal notes

- Design only; no engine behavior.
- No cap bump.
- No qNN references in executor code.
- Rayon remains a named fallback if real 380 measurements show `spawn_blocking` overhead is unacceptable.
