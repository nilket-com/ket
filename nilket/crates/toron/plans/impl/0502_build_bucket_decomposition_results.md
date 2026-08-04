# impl 502 — build-bucket decomposition (grounding results)

The decomposition the ADR 502 refresh required before the build kernel is chosen. No
engine change is made here.

## Method

Same instrument as the ADR 496 probe decomposition: `profiling` cargo profile at the
sealed `e6014f28` tip, direct `perf-run-one` m06 at the 8,192 gauge coordinate
(strict, sf=8, 2 GiB task / 2 GiB spill), two independent cycles records (F=999),
instruction-level annotate of `JoinBuildRows::insert` (10.7% of whole-process
cycles; the largest engine symbol at this coordinate). Artifacts:
`target/tpch/adr502_build_decomposition/`.

## Result: insert is the probe's memory-bound twin; writes and hash are free

| component of `insert` cycles | run 1 / run 2 |
|---|---:|
| bucket-array load — `movq (%rcx,%rdx,8)` (read miss #1) | 49.6% / 51.0% |
| entry-key equality load — `cmpq %r13, 0x28(%rax,%rdi)` (read miss #2) | 37.3% / 33.8% |
| entry/key/row stores | ~5% |
| hash (mulq/rolq) | ~1.6% |

The build's cost is the same two serialized dependent **read** misses the pre-497
probe paid — chase `buckets[]` (~256 MB at the 32M-bucket build size), then compare
against `entries[].key` (~576 MB) — while the actual writes are absorbed by store
buffers (~5%) and the hash is noise. The entry-compare share (~35%) tracks the
~37.5% bucket occupancy of the 12M-entry / 32M-slot table: orders keys are unique,
so a compare fires only when the first probed slot is already occupied. The table is
pre-sized (`with_capacity` reserves both arrays), so there is no growth/realloc
component to attribute.

## Consequence for the kernel choice

- **The ADR 497 batched-prefetch kernel mirrors directly onto insert.** Stage a
  window of build keys, prefetch their bucket slots (and entry lines for occupied
  slots), then run the *serial* insert resolve re-walking from each staged bucket —
  identical safety shape to the probe kernel (mutation stays sequential; a window
  key that collides with an earlier same-window insert re-walks and resolves
  exactly as the scalar path would). Probe_key dropped 59.5% on this signature;
  the insert-side ceiling is comparable, worth roughly −300–450 ms of the 778 ms
  build bucket.
- **The held layout kernels remain the deeper cut** (key-adjacent bucket slots
  would collapse miss #2 for both probe and insert; dense range-directed would
  delete the chase entirely) — but batched-prefetch insert is the same
  low-risk/no-layout move that already paid once, and it does not foreclose either
  layout kernel later.

Recommendation to the deciding ADR: open with batched-prefetch insert (mirror of
497), hold the layout kernels behind its measured result, acceptance gated on the
ledger's `build` bucket shrinking with digest / matched-rows / zero-spill unchanged
and the miss-profile check showing MLP engaged on the insert path.
