# impl 503 — m06 batched-prefetch insert results

Sealed implementation of ADR 503: the ADR 497 batched-prefetch kernel applied to
`JoinBuildRows::insert`, the probe's memory-bound twin (ADR 502). `PrimitiveBuildTable::insert_batch`
stages a window of build keys — hash + bucket-slot prefetch, then entry-line prefetch for occupied
slots — then a serial insert-resolve that re-walks `buckets[]` fresh from each home slot, so the
result is byte-identical to the one-key-at-a-time path. `TORON_BATCHED_BUILD_PREFETCH=0` restores the
true scalar insert (window=1) as the faithful control arm.

## Acceptance rail (8,192 coordinate, strict sf=8, 2 GiB task / 2 GiB spill, ledger armed)

Three alternating control(`=0`)/candidate pairs, medians:

| | control (scalar) | candidate (batched) | Δ |
|---|---:|---:|---:|
| ledger `build` bucket | 842 ms | 500 ms | **−342 ms (−40.6%)** |
| execute | 2,165 ms | 1,819 ms | **−346 ms (−16.0%)** |

The build reduction lands in the −300–450 ms ceiling, and −342 ms build ≈ −346 ms wall — the recovered
build time maps directly to the clock (build is not pipelined). **Digest identical, 1 output row,
47,989,007 matched rows, zero spill — invariant across both arms in every pair.**

## MLP check (perf stat, cpu_core, 99.7% coverage) — the 446 confound rail

| | control (scalar) | candidate (batched) | Δ |
|---|---:|---:|---:|
| instructions | 31.96 B | 31.61 B | ~flat (−1.1%) |
| cycles | 25.64 B | 23.93 B | −6.7% |
| IPC | 1.247 | 1.321 | **+5.9%** |
| LLC-load-misses | 58.2 M | 45.6 M | **−21.7%** |

Instructions flat, cycles down, IPC up, LLC-load-misses down 21.7% — the identical signature ADR 497's
probe kernel produced. The insert's two dependent misses overlap instead of serializing; the win is
memory-level parallelism, not a clock artifact. (Diluted to whole-run since build is ~40% of the wall;
the build bucket itself dropped 40.6%.)

## Gates

- build_rows unit tests: 19 passed, including `adr503_batched_insert_matches_scalar_across_collisions_and_duplicates`
  (batched insert byte-identical to scalar across collisions, duplicates, cross-window) and
  `adr503_batched_insert_ablation_defaults_on`. Run under `timeout -s KILL` — 0.00 s, no hang.
- ADR 352 line bound: fixed by **relocation, not a bump** — the `build_primitive_key_rows` driver moved
  to `build_rows.rs` (next to `insert_batch`); `exec/join` root back to 3,790 lines (≤ 3,800).
- `cargo test -p toron --test architecture_test`: 2,093 passed.
- `cargo test --workspace --no-fail-fast`: **exit 0** — the runner's explicit all-pass verdict under
  `--no-fail-fast` (default features; it runs past failures but exits non-zero if any failed). No
  failures or panics in output; the tail shows the final suites all `ok`. The aggregate `N passed`
  total is not quoted (the launch piped through `tail -30`, truncating it) — green rests on the exit-0
  verdict, not a summed count. This cut touches only `build_rows.rs`/`join.rs` (no feature-gated
  target), so the ADR 491 feature-gated-breakage class is not in play.

## m06 lineage and forward

m06 at the 8,192 coordinate: 2,163 ms (ADR 501, probe-only) → **1,819 ms** (ADR 503, build+probe both
batched-prefetched) vs DuckDB-1t 1,277 ms — **1.42×**. `build` (778→500 ms) and `probe_key` (both
halves of the Int64 hash-join machinery) are now both prefetch-optimized.

The layout kernels stay held as the deeper, higher-ceiling follow-up: #2 key-adjacent bucket slots
collapse miss #2 for probe *and* insert; #3 dense range-directed table deletes the chase entirely.
Neither was foreclosed. The next fork — accept ~1.4× and move on, or open the layout cut that moves
both buckets at once — should be decided against a fresh read of the corrected board.
