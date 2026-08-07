# impl 0538 — the timing atlas published board and the no-work dominance verdict

`target/tpch/adr538_timing_atlas/timing_atlas_cad04fc4.jsonl`, sealed `timing_authority=true`
with zero withheld reasons and zero replay violations, from clean binary `cad04fc4fe79` on a
quiescent host. sf=8 strict, 2 workers over loopback TCP, 7 measured pairs per cell after 2
discarded warmups, q15 excluded by construction. Header fingerprint `fnv1a64:b9b384385fad4af5`
is the per-query export manifest (identical across all five runs); the dataset manifest is
`fnv1a64:0fc5c06d19c5ed23`, full-hash verified — two names for two objects, not drift.

## Board — 21 cells finalized

**15 distinguished-slower, 4 distinguished-faster, 2 indistinguishable.** The timing shape
replicated across three quiescent runs on three binaries (runs 2, 4, 5): the core cells hold
their categories and magnitudes; six near-zero cells (q02, q04, q11, q13, q16, q17, q22)
trade categories at the threshold, which is what near-zero medians under a strict 7/7 sign
rule are expected to do.

Distributed-faster: q17 (−812 ms, 0.80×), q02 (−217 ms, 0.91×), q04 (−180 ms, 0.95×),
q22 (−166 ms, 0.75×). Indistinguishable: q13, q16.

## Dominance fork — no component crosses the threshold

Per the frozen contract: a component leads a cell when its receipt-backed paired contribution
reaches 50% of the cell's median paired delta; a branch is selected when one component leads
at least ⌈2/3·|S|⌉ cells of S. |S| = 15, threshold = 10.

| cell | delta ms | ratio | scan% | operator% | shuffle% | transport% | leads |
|---|---|---|---|---|---|---|---|
| q01 | 16,664 | 4.15 | 6.6 | 0.0 | 57.6 | 42.5 | shuffle |
| q03 | 4,194 | 2.98 | 127.1 | −18.3 | 6.2 | 1.1 | scan |
| q05 | 8,323 | 2.34 | 1.3 | 96.1 | 5.1 | 0.4 | operator |
| q06 | 375 | 1.42 | 159.8 | 0.0 | 22.1 | 11.2 | scan |
| q07 | 23,186 | 6.18 | 3.5 | 53.9 | 23.2 | 11.5 | operator |
| q08 | 5,695 | 2.40 | 15.6 | −37.8 | 68.4 | 0.2 | shuffle |
| q09 | 7,354 | 1.89 | 13.1 | −55.1 | 76.9 | 10.8 | shuffle |
| q10 | 712 | 1.28 | 1.4 | −0.7 | 54.7 | 64.6 | shuffle, transport |
| q11 | 384 | 1.46 | −14.1 | 109.6 | 10.7 | 1.3 | operator |
| q12 | 5,514 | 3.48 | 79.0 | −5.1 | 11.4 | 5.3 | scan |
| q14 | 437 | 1.53 | 168.1 | −29.0 | 33.4 | 15.0 | scan |
| q18 | 3,128 | 1.45 | 157.1 | 0.3 | −30.0 | 0.0 | scan |
| q19 | 489 | 1.24 | 361.0 | −71.6 | 45.5 | 19.1 | scan |
| q20 | 381 | 1.23 | −0.7 | 112.7 | −10.7 | 0.4 | operator |
| q21 | 10,910 | 1.86 | 11.6 | 89.4 | 34.4 | 1.0 | operator |

Leading counts: **scan 6, operator 5, shuffle 4** (transport co-leads q10 only; dispatch is
ineligible by construction). No component reaches 10 of 15. **The evidence selects the
no-work branch: no single-component substrate decision is licensed.** The distributed
overhead is not one thing — it is three things, cleanly separated by cell family:

- **scan-led** (q03, q06, q12, q14, q18, q19): distributed scans cost more than the entire
  local-vs-distributed delta on four of six cells (shares above 100%), offset by *negative*
  operator contributions — the join work itself is cheaper split across workers, and
  re-reading the base relations is what erases the win.
- **operator-led** (q05, q07, q11, q20, q21): join build/probe/spill cost more under the
  distributed envelope; q07, the widest cell on the board (6.18×, +23.2 s), sits here.
- **shuffle-led** (q01, q08, q09, q10): partition, publication and consumption of exchange
  data dominates; q09 pairs 76.9% shuffle with −55.1% operator — the same
  cheaper-when-parallel effect paid for twice over in data movement.

Each family is large enough to name a follow-on question and none is large enough to decide
one alone. That is a finding about the engine, not a failure of the instrument.

## What the apparatus refused before it published

Three authoritative attempts were withheld by the apparatus's own vetoes, each exposing a
real defect the fix for which is now guarded by tests:

1. **Run 3 (`1ead103f`)**: `operator` exceeded its own task wall by up to 32% — eighteen
   executor sites fed one `build_started` interval into both `build_micros` and
   `spill_micros`. Fixed at `f98fde7d`; the architecture guard now rejects any timer feeding
   two phase counters.
2. **Run 4 (`f98fde7d`)**: attribution covered 100.03% of q22's measured elapsed — the
   dispatcher's decode of streamed result batches, measured on the coordinator's thread,
   was folded into the worker task's phases: 81 nodes across ten cells carried the same
   defect wherever the remainder was thick enough to hide it. Fixed at `cad04fc4`: phase
   counters are same-timeline intervals; dispatcher decode is non-phase evidence; worker-side
   pull decode split into `batch_decode_micros`; replay verifies the partition per node.
3. Both classes had always existed — `recompute_framework_overhead`'s saturating subtraction
   silently absorbed them. The atlas's additivity rule is what made them visible.

## Artifacts

Published: `timing_atlas_cad04fc4.jsonl` (canonical, replayable). Preserved partials:
`e6d925a3` (double-setup abort), `e53bf1ee` (complete data, pre-fix seal logic), `0e5f8a11`
(interrupted), `1ead103f` (operator overflow evidence), `f98fde7d` (transport timeline
evidence). The run-2 artifact `timing_atlas_0e5f8a11_run2.jsonl` remains what its correction
record states: preliminary timing deltas only.
