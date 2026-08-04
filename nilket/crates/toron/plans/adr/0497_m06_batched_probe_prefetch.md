# ADR 497 - m06 probe-key: batched probe with software prefetch (overlap the dependent misses)

ADR 496 identified `probe_key` (~55% of m06's wall) as the target and refused to pick a kernel
until a decomposition separated compute-bound from memory-bound. The decomposition (sealed
`240190f5`) is decisive: `probe_key` is **memory-bound**, and the hash is free. This cut decides the
kernel that follows from that evidence — and rules out the one the ADR 495 analogy suggested.

## Grounded boundary

The probe owner is `PrimitiveBuildTable<K>::get` → `find_bucket` (build_rows.rs:235/263). Each of
m06's ~48M lineitem probes is a serialized chase of **two out-of-cache dependent loads**:

1. `buckets[bucket]` — the open-addressing bucket array, ~128 MB (miss #1).
2. `entries[entry_index].key` for equality, then `entries[entry_index].rows` — the ~576 MB entries
   array, 48 B entries, key at offset 40 (miss #2).

The second load address depends on the first's result, so they cannot overlap; and the scalar
per-key loop resolves one probe fully before the next, so the out-of-order window has nothing to
overlap across probes either — **zero memory-level parallelism**. The decomposition receipt
(`impl/0496_probe_key_decomposition_results.md`, artifacts under
`target/tpch/adr496_probe_decomposition/`) measured:

| component of `probe_key` | share of cycles |
|---|---:|
| bucket-array load (dependent miss #1) | 66–67% |
| key-equality load (dependent miss #2) | 25–27% |
| the hash | ≤2% |

Corroboration: whole-run IPC ≈ 1.04, and 74.3M LLC-load-misses against 48.0M probes — ≈43 ns/probe,
the signature of two serialized cache misses with no overlap. `JoinBuildRows::insert` shows the same
signature (20.6% of process cache-misses), so the build ledger bucket has the same root cause.

## Decision: batch the probe and software-prefetch, creating memory-level parallelism

Restructure the fused inner-join probe (`probe_in_memory_join_rows` → `PrimitiveBuildTable`) from
one-key-at-a-time to **batched**: for a window of probe keys, compute all hashes and bucket indices,
prefetch every `buckets[bucket]` slot, then resolve them; prefetch the resulting `entries[idx]`
slots, then resolve equality. Independent misses from different keys in the window are now in flight
together — the ~43 ns latency is amortized across the window instead of paid serially. **No layout
change, no join contract change, no result change.**

This is the kernel the evidence selects, and it is the safest of the ranked three: it moves only the
probe loop's shape. The **ADR 495 analogy — an Int64-specialized hash — is ruled out**: the hash is
≤2% of `probe_key`, so a faster hash cannot move the wall. The cost is memory latency, not
arithmetic.

Two implementation notes the plan must carry, not bury:

- **Prefetch portability.** Explicit prefetch is `core::arch::x86_64::_mm_prefetch` (stable, but
  arch-gated). Batching alone (software pipelining that hands the OOO engine several independent
  loads before it must consume any) already recovers MLP on most cores; the explicit prefetch is an
  additive tightening behind `cfg(target_arch)`, with a scalar fallback that is byte-identical.
- **Collisions.** `find_bucket` linear-probes on collision. The batched kernel handles the common
  1–2-load case and continues any key whose chain is unresolved on a scalar tail — never changing
  which bucket a key resolves to.

## Acceptance instrument (arm the change; a wall win alone is not enough)

The ADR 496 phase ledger is the gate. The cut is accepted only if, on the same coordinate (strict,
sf=8, parquet, release, 2 GiB task / 2 GiB spill):

- the `probe_key` share **shrinks** in the ledger, and
- `digest` and the 12,000,000 matched-row count are **unchanged**, and spill stays **zero**.

And — the 446 harness-confound lesson — the *memory behavior* must move, not just the clock: the
decomposition's `cache-misses`/`cycles` perf pass must show MLP engaged (LLC-miss latency overlapped,
cycles-per-probe down, IPC up). A wall improvement whose miss profile is unchanged is a confound to
be explained, not banked.

## Forward

Held unless this cut underdelivers, in the receipt's ranked order:

- **Key-adjacent bucket layout** — store the key in the bucket slot so hit/miss resolves in one
  load; the entry load then survives only for matched rows. This also moves the `build`/`insert`
  bucket, which batched-probe-prefetch does not — keep them jointly measured if this is opened.
- **Dense range-directed table** — orderkeys are bounded-range; direct indexing deletes the chase
  entirely, but requires bind-time min/max evidence and a density guard. Highest ceiling, highest
  risk; hold until 1–2 are measured.

Out of scope, recorded so it is not lost: `toron_tpch::stable_file_hash` owns ~41% of whole-process
cycles in the load window — the gauge spends more CPU hashing input files than executing m06. It
does not touch `execute_ms` or the ledger; it is a cheap **harness** cut, not an engine one, for a
separate commit.
