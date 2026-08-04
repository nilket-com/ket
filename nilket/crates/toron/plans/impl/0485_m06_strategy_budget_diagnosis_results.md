# ADR 485 implementation receipt — m06 strategy/budget diagnosis (no code shipped)

The question was whether m06's ~88 MB build can avoid the spill/repartition regime
under an explicitly chosen budget. **Grounding falsifies the budget premise**: the
strategy is bound at plan time and moved by neither the task budget nor the broadcast
threshold. The spilled partitioned join is triple-locked. No code shipped; a probe
knob was built, shown inert (clamped by a pinned ceiling), and reverted.

## Finding 1 — the budget knob does not move m06

m06 at task budgets 64 / 128 / 256 / 512 MiB (spill reserve 2 GiB): identical every
time — spill **88,559,536 bytes**, identical plan fingerprint `144a330ac4a17555`,
wall ~20.6-20.9 s. The strategy is chosen at bind time, not adapted to runtime memory.

## Finding 2 — the "spill" is the repartition shuffle, not memory eviction

The inner-join build memory cap is `memory_cap × 16`
(`JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER`, `join.rs:56`): 1 GiB at
64 MiB, 8 GiB at 512 MiB. The 88 MB build is far under both, yet spills. So the spill
is not memory-pressure eviction — it is the partitioned exchange materializing each
partition to a spill file (the shuffle). That is inherent to the hash-partitioned
strategy, which repartitions **both** sides (48M probe + 12M build).

## Finding 3 — m06 cannot broadcast; the ceiling is pinned below its build

A gauge broadcast-threshold knob (`TORON_TPCH_BROADCAST_MAX_BUILD_BYTES`, wired into
the perf bind at `lib.rs:3709`) set to 512 MB did **not** flip m06's plan — and was
inert for q05/q10/q14/q19 too. Cause: `effective_auto_broadcast_threshold`
(`binder/join.rs:1225`) is `min(config threshold, default_auto_broadcast_cap_ceiling)`,
and the ceiling is `QueryBudget::test_default().broadcast_build_bytes_per_task / 4`
= **16 MB** — pinned by the `adr138_default_threshold_unchanged` architecture guard.
m06's 88 MB build exceeds 16 MB, so it is broadcast-ineligible, and the PlannerConfig
knob is clamped below the point where it could matter. The knob was reverted (no
gauge value while clamped).

## Conclusion — the lever is a missing strategy, not a budget

m06's spilled-repartition is triple-locked: (1) budget-independent, (2) the spill is
the shuffle not eviction, (3) the broadcast ceiling is contract-pinned at 16 MB,
below the 88 MB build. Toron has exactly two inner-join strategies — broadcast (tiny
builds, ≤16 MB) and hash-partitioned-with-spill (everything else). m06's 88 MB build
falls in the gap: too big to broadcast, small enough to fit in memory (≪ the 1 GiB
build cap), but partitioned-and-spilled anyway because **no in-memory,
non-partitioned, single-build hash-join strategy exists**.

Pricing the in-memory alternative therefore requires *building* that strategy — a
binder/executor capability cut (a medium-build in-memory hash join, or a
budget-derived broadcast ceiling that the ADR 138 guard currently pins), each with
real blast radius — not choosing a budget. That is the honest scope of the m06 lever,
and it is a deliberate strategy decision for its own cut, not a tonight micro-op.
This diagnosis is the grounding for it.
