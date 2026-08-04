# ADR 497 implementation receipt — batched probe with software prefetch

The kernel ADR 497 selected from the decomposition (`240190f5`): the Int32/Int64
probe arms of `probe_in_memory_join_rows` now stage a `PROBE_BATCH_WINDOW = 64`-key
window — hash + bucket-slot prefetch for the whole window, then entry prefetch, then
resolution — so the two dependent out-of-cache loads of independent probes overlap
instead of each probe paying them serially. A key whose first probed slot neither
hits nor is empty falls to the scalar linear-probe tail, so every key resolves to
exactly the bucket the unbatched `find_bucket` would choose. Prefetch is
`_mm_prefetch` behind `cfg(target_arch = "x86_64")`; elsewhere the staged loads
alone provide the memory-level parallelism and results are byte-identical. No
layout, contract, or result change; the Generic key plan is untouched.

`TORON_BATCHED_PROBE_PREFETCH=0` restores the historical one-key-at-a-time lookup
(the true prior code path, not a window of one).

## Matched alternating A/B on the sealed code (ledger armed)

Coordinate: strict, sf=8, persisted parquet, release, 2 GiB task / 2 GiB spill
reserve; same binary (`build_commit=4da06f31ea10-dirty` — the dirty delta is this
cut), three alternating pairs, `TORON_LOCAL_JOIN_PHASE_PROFILE_PATH` armed:

| arm | execute median | probe_key median | probe_key share |
|---|---:|---:|---:|
| scalar probe (control) | 3,676 ms | 1,974 ms | 53.7% |
| batched + prefetch | 2,482 ms | 799 ms | 32.2% |

**Effect: execute −32.5%; probe_key −59.5%.** One distinct
(digest, output_rows=1, matched_rows=47,989,007, spill=0) tuple across all six
rows — the acceptance gate's correctness legs all hold. The untargeted ledger
buckets are unmoved (build 797 vs 799 ms, probe_scan 324 vs 326 ms in the pre-seal
pricing run), so the win sits entirely in the targeted bucket: not a confound.

## MLP engagement proof (the 446 rail)

`perf stat` on the profiling build (identical codegen), whole process, control vs
candidate: cycles 32.30e9 → 26.51e9 with instructions flat (34.2e9 → 34.6e9) —
**IPC 1.06 → 1.31**; LLC-loads 98.5M → 82.7M and LLC-load-misses 73.6M → 58.7M
(prefetched lines arrive before their demand loads). The memory behavior moved with
the clock, as the plan required. Artifacts preserved under
`target/tpch/adr497_final_{control,candidate}_{1,2,3}/` and the ADR 496
decomposition artifacts remain the baseline profile.

## Rails

- A collision-dense unit test proves batched lookups equal scalar lookups across
  collisions, nulls, misses, duplicates, and window boundaries.
- The ablation default-on contract is pinned by test.
- Both probe arms share one `probe_primitive_key_rows` driver, keeping
  `exec/join` under its ADR 352 root bound (3,798 ≤ 3,800) with per-row match
  handling identical to the replaced loops.

## m06 lineage price

23.3 s (pre-490 ladder) → 3.54 s (ADRs 490–493) → 2.48 s (this cut) at the same
coordinate; DuckDB 1-thread reference is 1.30 s.

## Forward (unchanged from the plan)

- Key-adjacent bucket layout (also moves the untouched `build` bucket, now the
  largest ledger share at ~800 ms) and the dense range-directed table — held; open
  only if the refreshed ladder still ranks m06 first.
- `toron_tpch::stable_file_hash` harness cut — out of scope, still recorded.
