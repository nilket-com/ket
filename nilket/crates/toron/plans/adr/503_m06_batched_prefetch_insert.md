# ADR 503 - m06 build bucket: batched-prefetch insert (mirror of ADR 497)

ADR 502's build-bucket decomposition proved `JoinBuildRows::insert` is the probe's memory-bound twin:
its cost is the same two serialized dependent **read** misses (bucket array ~256 MB → entry-key
compare ~576 MB), stores absorbed by store buffers (~5%), hash ~1.6%. At the 8,192 coordinate the
build bucket is 778 ms (37% of m06's wall). This cut applies the ADR 497 batched-prefetch kernel — the
one that paid −59.5% on the identical probe signature — to the insert path.

## Decision

Add `PrimitiveBuildTable::insert_batch`, mirroring `get_batch` (ADR 497): stage a window of build keys
— hash + bucket-slot prefetch, then entry-line prefetch for occupied slots — then a **serial**
insert-resolve that re-walks `buckets[]` fresh from each key's home slot. Because the resolve re-reads
the live table (the prefetch is only a hint), a within-window key that collides with an earlier
same-window insert re-walks and resolves **exactly** as the scalar `find_bucket`+insert would, and the
keys are processed in slot order — so the result is byte-identical to the one-key-at-a-time path. The
build loop (`build_in_memory_join_rows`) windows Int32/Int64 keys through it via a shared driver that
mirrors `probe_primitive_key_rows`; the generic (bytes-keyed) path is unchanged.

Ablation: `TORON_BATCHED_BUILD_PREFETCH=0` restores the **true one-key-at-a-time** insert (window=1,
scalar `insert`, not a window-of-one) — the faithful control arm, exactly as ADR 497 did for probe.
`_mm_prefetch` reuses the existing cfg(x86_64) helper with a byte-identical non-x86 fallback.

## Scope

- Int32/Int64 primitive build tables only (the join-key fast path); generic bytes keys untouched.
- No layout change — this is the low-risk, no-layout move. The **held** kernels (ADR 497 forward:
  #2 key-adjacent bucket slots collapse miss #2 for probe *and* insert; #3 dense range-directed table
  deletes the chase) remain candidates; batched-prefetch insert forecloses neither and opens first.

## Acceptance rail

On the 8,192 coordinate (strict, sf=8, 2 GiB task / 2 GiB spill), armed:
- The ledger's **`build` bucket shrinks** materially (ceiling −300–450 ms of the 778 ms), candidate vs
  `=0` control in alternating pairs.
- `digest`, 12,000,000 matched rows, and zero spill **invariant** across control and candidate.
- **MLP engaged on the insert path** — the miss-profile / cycles check must show the insert misses
  overlapped (IPC up, LLC-miss latency amortized), not just a lower clock. A wall win whose insert
  miss profile is unchanged is a confound, not the cut (the 446 rail).
- Byte-identical output with the ablation on/off; a unit test asserts batched insert == scalar insert.

## Forward

If the build bucket shrinks as the probe did, m06 → ~1.65–1.75 s vs DuckDB-1t 1,277 ms (inside 1.4×),
and `build` + `probe_key` — the two halves of the hash-join machinery — are both prefetch-optimized.
The layout kernels (#2/#3) then become the deeper, higher-ceiling follow-up, decided on their own
grounding, targeting the miss that survives prefetch (the dependent chase itself).
