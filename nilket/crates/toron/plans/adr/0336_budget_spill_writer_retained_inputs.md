# ADR 336 - budget the spill-writer retained footprint (q18; observability-first)

## Status
Proposed.

## Context

The memory arc's **last observability cut** - q18 is the only remaining silent OOM at sf=1. ADR 334 named it: q18
dies inside `SpilledJoinBuild::write_inner` while building pages/index state, **before** the resident check at
exec.rs:5538 can run - and that check would not catch it anyway. After 336, every memory cell is a named
`memory_budget_exhausted` row; the streaming executor is then the standing real-fit arc.

### The omitted term (grounded)
- The resident check is `resident_without_cache = index_bytes + writer_peak + page_size > task_budget.memory`
  (exec.rs:5535-5542) - it counts **only the spill writer's own state.**
- The dominant retained footprint at spill-write time is **two unaccounted allocations, both live while write_inner
  runs:**
  1. the **concatenated input batches** - `concat_or_empty(left/right)` at exec.rs:2956-2957, built at the top of
     `execute_hash_join` and held through build + spill + write (ADR 334 q18: `left 452 MB` + `right 805 MB`);
  2. the **fallback build map** - `HashMap<Vec<u8>, Vec<u32>>` (exec.rs:3915-3932), resident while the writer builds
     pages/index (ADR 334 q18: 1.5M keys).
- Neither is in `resident_without_cache`, so the check never fires; q18 reaches the 4G cgroup ceiling first.

## Decision - a spill-entry resident-footprint guard (observability, not fit)

Add a budget guard in `execute_spilled_hash_join`, **after the fallback build map exists and before
`SpilledJoinBuild::write(...)`**, that accounts for the full spill-entry resident footprint:
- **retained inputs:** exact `RecordBatch::get_array_memory_size()` on the concat'd left/right batches (they already
  exist - a measurement, not an estimate). Count **both** concat'd inputs for every spilled hash join kind; this is
  retained-memory accounting, not ADR 335's output-side accounting;
- **fallback build map:** an estimate of the `HashMap<Vec<u8>, Vec<u32>>` resident bytes (keys, vec headers/capacity,
  row-index capacity, and hash-table overhead);
- (the existing writer term stays where it is for the q04/q12/q20 path).
- If the footprint exceeds the effective spill cap, `return budget_exhausted(BudgetDimension::Memory, ...)` before
  `write` builds page/index state.

Result: q18 -> clean `fail(memory_budget_exhausted@execute)` instead of `oom-kill`.

## The cap - measured from the gap, not assumed (the load-bearing discipline)
**Do not reuse raw `task_budget.memory` (it false-trips, the 335 lesson) or 335's 16x multiplier blindly.** Phase 0
**measures** and sets the cap from the actual gap:
- Which sf=1 joins reach the **spill path** and **complete** (build doesn't fit -> spill, then pass)? Among the greens
  (q01/q03/q05/q06/q10/q13/q16/q22), do any spill-complete, and what is their spill-entry footprint
  (inputs + build map)?
- q14/q17/q19 already spill (ADR 334) but are caught by 335's materialize guard; 336 may fire first (same outcome,
  earlier). The real risk is a **green spill-completer.**
- Set the cap in the measured gap: largest green spill-completer footprint < cap < q18's footprint (~1.26 GB+). **If
  no green spills-complete, the headroom is large and the cut is simpler** (a generous fixed cap below q18).

## Acceptance - exactly the observability scope
- **q18 @ sf=1 returns `fail(memory_budget_exhausted@execute)`** instead of `oom-kill` (default budget + 4G cap); the
  retained-input + build-map term is what makes the check fire.
- **No green spill-completer false-trips** (discriminating, measured; cap in the gap). Green controls
  (q01/q03/q05/q06/q10/q13/q16/q22) stay `pass@execute`.
- **The full memory family is observable** - q04/q07/q12/q14/q17/q19/q20/q21 + q18 all emit named
  `memory_budget_exhausted`; **no silent OOM remains at sf=1.**
- The **streaming-executor wall** is named as the real fit arc; 336 does **not** make q18 fit.
- sf=0.01 boards unchanged.

## Risks
- **Discrimination (measured)** - the cap must catch q18 yet spare green spill-completers; Phase 0 measures both
  before setting it (don't assume raw budget or 335's 16x). A discriminating unit test is mandatory.
- **Count both terms** - inputs *and* the fallback build map are resident; counting only inputs may under-fire.
- **Placement** - after the fallback map, before `write`; the guard must precede page/index construction (the kill
  point), not sit at the existing post-construction resident check.
- **Observability, not fit** - q18 becomes a named fail, not a pass; the streaming executor is the fit.

## Forward arc
After 336 every memory wall fails cleanly + named. The **streaming/pipelined executor** is then the substantive
scale arc - turn the named fails into passes - now the single biggest lever, sized against fully-observable walls.

## Why this is its own cut (not 335)
335 guards the **output** materialization (estimated from the selection vector, pre-take). 336 guards the **retained
input + fallback build map** on the **spill** path (measured from the concat'd batches, at spill entry). Different
terms, different placement, different kill point. Shared piece is byte-counting batches
(`get_array_memory_size`) - reuse if clean, no broad join-operator refactor.

## Why toron-home
Engine cut in `crates/toron` (exec.rs `execute_spilled_hash_join` / `SpilledJoinBuild`); driven via `toron-tpch`
reach at sf=1. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude (convergent: observability-first, q18 the last silent OOM). Codex's tightenings folded:
count the **full spill-entry resident footprint** (retained inputs *and* the fallback build map, not inputs alone);
**measure** green spill-completers and set the cap from the actual gap (don't reuse raw budget or 335's 16x);
guard in `execute_spilled_hash_join` after the fallback map, before `write`, using exact `get_array_memory_size` for
inputs + a build-map estimate. Observability-only; the streaming executor stays the real fit arc.
