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
  and retained bytes, parent-reconciled with explicit residual; each timer brackets the
  complete stage-specific decision/admission/pre-scan plus runtime construction path;
- the generic composite-key suffix path gains the same `suffix_lookup` /
  `suffix_match_expand` / `suffix_payload_take` children and flow counters the specialized
  path carries, batch-scoped;
- `cleanup` gains per-replay children, parent-reconciled;
- every build (generic and specialized) records O(1) representation facts: exact fixed-width
  key/retained bytes when provable, otherwise `unknown` plus reason; capacity, load factor,
  and hash/index type name are recorded fields only, with no entry walk.

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

Cleanup is reported as order-sensitive lifecycle ownership and contributes only to this
maximum removable upper bound; it is not asserted as independently recoverable wall.

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

## Measured outcome - attribution rejected

The mechanism sealed as `f0e9ff25` and passed the complete repository battery, but the live
exact-coordinate evidence rejected the instrument before it could price `E`. No lifecycle
number from the profiler-on arms has attribution authority.

### Perturbation gate

The five adjacent q09 pairs all passed query correctness with one fingerprint
(`3d202b6e227b3a8b`), one digest, 175 rows, and zero spill. Their relative movements were:

| pair | profiler off (ms) | profiler on (ms) | `(on - off) / off` |
|---:|---:|---:|---:|
| 1 | 9,927.890117 | 9,293.211530 | -6.392885% |
| 2 | 10,070.984153 | 9,252.140018 | -8.130726% |
| 3 | 10,018.539358 | 9,146.304981 | -8.706203% |
| 4 | 9,874.506213 | 9,198.904684 | -6.841877% |
| 5 | 9,901.023854 | 9,189.012488 | -7.191290% |

The signed median is **-7.191290%**; ADR 516 defines the gate in absolute value, so the
perturbation is **7.191290%** and the q09 run set is rejected. Acceleration is not free
instrumentation: the sealed diff staged generic keys and hash lookups in 64-row windows while
the profiler-off arm retained the historical scalar loop. That execution-schedule difference is
a direct confound, whether or not it explains the whole measured movement.

The required q08 sanity check independently rejected twice:

| attempt | profiler off (ms) | profiler on (ms) | movement |
|---:|---:|---:|---:|
| 1 | 4,260.620470 | 4,426.569500 | +3.894950% |
| 2 | 4,305.746352 | 4,513.158760 | +4.817107% |

Both q08 attempts held one fingerprint (`5e07510a975325d2`), one digest, two rows, and zero
spill. Two consecutive breaches satisfy the predeclared close-red condition; the run set is not
selectively extended or corrected.

### Reconciliation failure

The q09 profiler artifacts also fail the stronger receipt contract. Every profile reports
`coverage_complete=true` but `refinements_reconciled=false` and `stages.passed=false`.
`runtime_prep` and `cleanup` are whole-invocation parents with `replay_id=null`, while each new
`prep_build` and `cleanup_build` child is replay-scoped. The existing parent-instance reconciler
therefore emits `parent_stage_not_reconciled` for all four prep children and all four cleanup
children. The suffix-probe refinements reconcile, including replay 2, but they cannot repair the
failed lifecycle boundary.

### Decision

ADR 524 closes **red on instrumentation validity**. `E` is not computed, q09 is neither parked
nor promoted to a compact-composite successor, and no remedy is selected. The receipt removes
the rejected profiler changes and restores the post-523 implementation surface; only the plan
and this evidence remain.

A future attribution cut must preserve the generic probe's execution order while placing
batch-scoped clocks around that same path, and must either publish replay-scoped prep/cleanup
parents or teach the receipt contract explicitly how replay children partition a global parent.
Those are instrument-design prerequisites, not q09 performance findings.

The restored tree re-ran green after removal: both fmt legs; 1,456 library tests; all 2,095
architecture tests; the full `--no-fail-fast` suite and `toron-tpch` with direct exit 0 and no
failures; the Toron telemetry, base TPC-H, Polars-reference TPC-H, and Wendelon all-targets
checks; and both staged and unstaged `git diff --check`.
