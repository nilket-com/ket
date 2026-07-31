# impl 524 - partsupp lifecycle attribution plan

Attribution only: close the partsupp composite-key lifecycle boundary in the ADR 516
instrument so one coherent envelope — partsupp prep + replay-2 probe + partsupp cleanup — can
be priced against the 3,554 ms floor. No remedy, no planner change, no new knob.

## Immutable inputs

- tree: post-523 main (`8e927b92` lineage), all activated knobs default on;
- exact coordinate: strict sf=8 persisted Parquet (format 2, full-hash), budgets 2 GiB /
  2 GiB explicit; fixed floor 3,554 ms;
- fresh grounding (this cycle, `q09_ground_8e927b92`): span 8.842 s attributed; replay-2
  partsupp parent 1,585 ms opaque (`coverage_complete=false` honestly reported); runtime_prep
  1,639 ms unsplit across four builds (part 87,125 / partsupp 6,400,000 / orders 12,000,000 /
  nation 25; 3.8 MB / 204.8 MB / 144.0 MB / 697 B); cleanup 690 ms unsplit; suffix-1 split
  lookup 568 / expand 78 / take 123 / residual 103; per-probe 607 ns (partsupp) vs 37 ns
  (orders) at equal probe counts;
- ADR 516 contracts inherited verbatim: batch-scoped tokens, reconciliation-or-fail,
  locality proxies stated not interpreted, perturbation gate (5 interleaved off/on pairs, 3%
  median bound, rejected-not-corrected, two consecutive rejections close red);
- the 092e574a lesson: profiler facts constructed once per runtime, profiler-gated.

## Step 1 - instrument extensions

On the reoriented-chain vocabulary in exec/join/streaming.rs:

- `runtime_prep` gains per-replay children (`prep_build`, one per suffix identity) with rows
  and retained bytes, parent-reconciled with explicit residual;
- the generic composite-key suffix path gains the same `suffix_lookup` /
  `suffix_match_expand` / `suffix_payload_take` children and flow counters the specialized
  path carries, batch-scoped;
- `cleanup` gains per-replay children, parent-reconciled;
- every build (generic and specialized) records representation facts: key width bytes,
  capacity, load factor, retained bytes, hash/index type name — recorded fields only.

Profiler-off byte-identity; once-per-runtime construction.

## Step 2 - tests and gates

Structural: profiler-off identity; once-per-runtime construction regression; reconciliation
failure on a missing child; q08 vocabulary unchanged except prep/cleanup splits. The full
battery (fmt x2, lib, FULL architecture_test, --no-fail-fast suite, toron-tpch, three
--all-targets checks, git diff --check) with direct exit statuses; seal
`impl: adr 524 partsupp lifecycle attribution`, push, release build.

## Step 3 - perturbation gate and the envelope

Five interleaved profiler-off/on q09 pairs at the exact coordinate from the sealed release
binary; median perturbation ≤ 3% or reject (two consecutive rejections close red);
fingerprints, digests, and route facts identical across arms; q08 spot pair within bound.
Then one profiled q09 run supplies the envelope:

```text
E = prep_build(partsupp) + suffix_probe(replay 2, attributed + residual) + cleanup(partsupp)
```

## Step 4 - receipt and the predeclared decision

`perf:` receipt with the full per-build prep/cleanup split, the replay-2 sub-stage split with
flow counters, representation facts for all four builds, the perturbation-gate table, and E
against 3,554 ms:

- E < floor → **q09 parked**, with the split recorded so no successor re-derives it;
- E ≥ floor → the compact composite-key lifecycle becomes a legitimate single-envelope
  successor target; remedy selection remains outside this ADR.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
