# impl 496 — probe_key decomposition (grounding results)

The decomposition ADR 496 required before any kernel choice. No engine change is made
here; this records the targeted perf pass over the fused m06 probe and what it proves.

## Method

Same coordinate as the phase ledger (strict, sf=8, persisted parquet, 2 GiB task /
2 GiB spill reserve), `profiling` cargo profile (identical codegen to release, symbols
kept — the ADR 474 instrument), direct `perf-run-one` child under `perf record`
(cycles, F=999) and a separate `cache-misses` record. Two independent cycles runs;
instruction shares reproduced within ~1 point. Artifacts:
`m06_cyc.data` / `m06_cyc2.data` / `m06_miss.data` in the session scratchpad, plus
whole-run `perf stat` counters.

## Result: probe_key is memory-bound; the hash is free

Instruction-level annotate of `PrimitiveBuildTable<K>::get` (the probe_key owner —
23.1% of whole-process cycles, 25.7% of whole-process cache-misses):

| component | share of `get` cycles (run 1 / run 2) |
|---|---:|
| bucket-array load — `movq (%r9,%rdx,8), %rdi` | 66.0% / 67.0% |
| key-equality load — `cmpq %rsi, 0x28(%rax,%rdi)` | 26.6% / 25.3% |
| hash (multiply/xor/rotate sequence) | ≤2% |
| everything else (branch, chain step, empty check) | ~5% |

Whole-run confirmation: core IPC ≈ 1.04; 74.3M LLC-load-misses against 48.0M probes.
`JoinBuildRows::insert` (the build bucket) shows the same signature — 20.6% of
whole-process cache-misses — so a layout change moves both ledger buckets at once,
as ADR 496 anticipated.

The mechanism is the layout: `buckets: Vec<usize>` indexes into
`entries: Vec<PrimitiveBuildEntry { key, rows }>` (48 B entries, key at offset 40).
Every probe pays two *dependent* out-of-cache loads — bucket slot (~128 MB array),
then entry key (~576 MB array) — with no memory-level parallelism, ≈43 ns/probe.

## Consequence for the kernel choice (the deciding ADR's input)

- **Ruled out:** an Int64-specialized hash kernel (the ADR 495 analogy). Hashing is
  ≤2% of probe_key; its ceiling is noise.
- **In scope (ranked by expected MLP/miss reduction):**
  1. Batched probe with software prefetch — hash a batch of probe keys, prefetch all
     bucket slots, then resolve; overlaps the dependent-miss latency without
     changing layout or contracts.
  2. Key-adjacent bucket layout — store the key (or key fragment) in the bucket slot
     so a probe resolves hit/miss in one load; the entry load remains only for the
     matched row list.
  3. Dense range-directed table — TPC-H orderkeys are bounded-range integers; a
     direct-indexed table over the key range would delete the chase entirely, but
     needs bind-time min/max evidence and a density guard. Design-heavier; only if
     1–2 underdeliver.

## Harness observation (not engine, not this arc)

`toron_tpch::stable_file_hash` owns 41% of whole-process cycles (load window, outside
`execute_ms`). It does not contaminate the ledger or the decomposition, but the gauge
spends more CPU hashing input files than executing m06; worth a harness cut someday.
