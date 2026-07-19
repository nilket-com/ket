# ADR 465 implementation receipt

Two results, both earned by the rails as planned.

## Rail 1 caught — and reverted — a sealed regression

The correctness rail failed before any timing: q17 strict parquet at its
special-cased scale returned `avg_yearly row 0 is NULL`. Probe isolation confirmed
ADR 464's hash smaller-build placement as the cause (comparator disabled → pass;
enabled → fail; reproducible), and a narrower constraint to the fully-eligible
above-threshold terminal **still failed** — the hash-strategy swap+restore path is
not output-invariant for q17's shape, independent of where the comparator is
consulted. 464's implementation is reverted in full (`Revert "impl: adr 464..."`,
with its receipt rewritten as a falsification record); board correctness and the
full architecture_test re-verified green post-revert. The capability re-enters the
queue gated on a validity diagnosis, with the cheap failing fixture named.

Why 464's evidence missed it: its 22-query sweep ran on the memory source, which
carries no optimizer byte facts — the swap could never engage. Banked as a standing
rule: invariance evidence must run on a source that arms the change.

## Rail 2 — the remeasurement (release, sf=1, strict parquet, 64 MiB / 2 GiB explicit)

- Correctness green post-revert; then 20 timed runs + 1 warmup, `perf_exit=0`,
  zero `BudgetExhausted`.
- **Median 2,513.9 ms, min 2,413.4, max 2,613.0, spread ratio 0.079** — well inside
  the 0.20 stability gate; **no flags**.
- Sample skewness (adjusted Fisher–Pearson over the 20 retained samples):
  **+0.16** — the pre-447 baseline's +4.56 does not reproduce under the current
  apparatus. No cross-era ratio is computed; the old figure is era-bound
  (pre-retention, pre-format, pre-provenance) and simply retires.
- The run met the release/passing/stable recordability gate and **recorded the
  fresh baseline** (`target/tpch/adr465_q17/baseline_sf1_strict.jsonl`) with full
  coordinates and retained samples.

## What retires

The q17 queue item closes: an admissible, stable, coordinate-stamped baseline
exists, and the instability that motivated the item is gone under honest
measurement conditions. q17 needs no wall diagnosis on current evidence.
