# ADR 496 - m06 join CPU attribution: probe-key is the target, materialization is not

The ADR 473 ladder re-pin (post-495) put m06 first by recoverable time: `SELECT
COUNT(l_orderkey) FROM lineitem INNER JOIN orders ON l_orderkey = o_orderkey`, ~3.67 s,
zero spill, in-memory build. The prior forward note guessed the residual was probe/aggregate
CPU. This cut replaces the guess with a per-phase measurement and decides, from that
evidence, exactly where the next optimization works — and where it must not.

## Grounded boundary

m06 runs fully fused (ADR 496 impl grounding): each of ~48M lineitem probe batches is
scanned, keyed and probed against the ~12M-row `orders` Int64 hash table, reoriented-
materialized, and folded into the COUNT before the next batch — there is no 48M-row
materialized `Vec`. The build is resident (`admitted`, `estimate=1,440,000,000` ≤ the
2 GiB task budget), spill zero. This is CPU-only work; the memory policy is already settled
by ADR 490.

The `local_join_phase_ledger` (`TORON_LOCAL_JOIN_PHASE_PROFILE_PATH`) splits the fused pass
five ways. Medians over 5 sf=8 runs, `build_commit=e96d6a32`, ~7.4% residual:

| phase | share | absolute |
|---|---:|---:|
| **probe_key** — hash + bucket lookup + Int64 equality, 48M probes | **~55%** | ~2,090 ms |
| build — 12M-row Int64 hash table (120 B/row ≈ 1.44 GB) | ~22.7% | ~878 ms |
| probe_scan — lineitem decode | ~9.8% | ~389 ms |
| output_materialize | 3.9% | ~151 ms |
| aggregate_fold | 1.2% | ~48 ms |

## Decision: the target is probe_key (with build second); materialization is out of scope

The recoverable CPU is the Int64 hash-join machinery — `probe_key` (~55%) plus `build`
(~22.7%) ≈ 78% of the wall. The ADR commits the next implementation to that hot path and
**rules output-materialization and aggregate-fold out of scope**: at 3.9% and 1.2%, the
"COUNT does not need the materialized payload" hypothesis is measured-false — eliding it
recovers ~5% at most and is not worth the correctness surface.

The acceptance instrument is the ledger itself: any m06 change must show the `probe_key`
(and/or `build`) share shrink on the same coordinate, with the digest and 12M-matched-row
count unchanged and spill still zero. A wall improvement whose ledger does not move the
targeted bucket is a confound, not a win (the ADR 446 harness-confound lesson).

## The fix is not yet chosen — one decomposition gates it

`probe_key` at ~55% has two candidate causes with different fixes, and the evidence does not
yet separate them:

- **Compute-bound** (hashing 48M Int64 keys + equality) → a specialized Int64 join-key
  path, the direct parallel to ADR 495's compact Int64 group path.
- **Memory-bound** (cache-missing chase through a 1.44 GB, 120 B/row table) → a layout or
  probe-ordering change, where an Int64-specialized hash does little.

The implementation ADR's first step is to decompose `probe_key` into hash / bucket-chase /
equality (an extended ledger split or a targeted perf pass over the isolated probe), then
choose the kernel. This ADR deliberately does not embed that choice.

## Forward

- Decompose `probe_key`; pick the kernel from the decomposition, not from the ADR 495 analogy.
- `build`'s 120 B/row footprint (ADR 490's `estimated_build_hash_bytes = rows*(112+8)`) is
  the same table the probe chases; a build-layout change may move both buckets at once —
  keep them jointly measured.
- The ledger's residual (~7.4%) is the orders build-side scan on the physical-scan-build
  branch; if the decomposition needs it attributed, extend the build bucket to cover that
  `execute_bound` span rather than leaving it un-bucketed.
