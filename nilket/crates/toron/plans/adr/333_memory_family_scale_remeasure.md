# ADR 333 - memory-family scale re-measure (release + diagnostic retention)

## Status
Proposed.

## Context

The memory arc's first cut, and a **grounding probe** - the failure-atlas method, third time (after the reach atlas
and the scale atlas). The scale board after ADR 331/332:
- **sf=1 cartesians:** q08/q09/q02 - *fixed* (ADR 331).
- **sf=1 memory:** q04/q12/q20 named clean `memory_budget_exhausted` (ADR 332); **q07/q14/q17/q18/q19/q21** still
  classified `memory (oom-kill)` from the ADR 330 atlas - but that was the **debug** build, **before** diagnostic
  retention.

So six cells are classified on stale evidence. Two things changed that make a re-measure decisive: the **release
build** (debug overstated cost - q02 went 13s -> 1.4s; lower per-op overhead may shrink footprints) and **ADR 332
diagnostic retention** (every failed row now carries its cause, so a clean budget-fail is distinguishable from a
silent OOM). Measure before fixing - predicting the memory fix now would repeat the q08/q09 mistake (framing
overturned twice by grounding).

### The guard (grounded)
`BudgetExhausted{dimension: Memory}` maps to "tenant memory budget exhausted" on both the direct execution-error
and distributed task-failure SQL paths (`daemon.rs`). ADR 332 now preserves the raw diagnostic row and classifies
that debug message as `memory_budget_exhausted`, while still keeping the reason as `fail`, not `blocked`.

That only helps when the process emits a row. If the cgroup kills the process first, there may be no
`diagnostic_message` at all. This probe therefore distinguishes row-emitted budget failures from systemd kills:
a tracked allocation path **clean-fails** (`memory_budget_exhausted`); an untracked path still **OOM-kills** with
no guard fire.

## Decision - re-measure the six on release+retention, classify each cell, name the wall

### D1 - the re-measure (probe; no engine fix)
Build release; run each of q07/q14/q17/q18/q19/q21 strict @ sf=1 from the built binary under the ADR 330 caps
(`MemoryMax=4G`, `RuntimeMaxSec=300s`), one query at a time. Capture `status`, `reason`, and `diagnostic_message`
when a row is emitted; otherwise record the systemd result/kill signal, peak RSS, wall time, and that no row was
emitted.

### D2 - the classification split (the deliverable)
Each cell -> exactly one, by fixed rule:
- **now-pass** - finishes within the memory and wall caps with `status=pass`: a **free win** (release shrank the
  footprint, as q02 was for 331). The memory family shrinks.
- **clean `memory_budget_exhausted`** - a row is emitted whose diagnostic contains `BudgetExhausted` and
  `dimension: Memory`: the **cost-bound / streaming / spill gap** - the engine knows it is over budget but cannot
  stream/spill to fit.
- **OOM-kill (no clean guard fail)** - the cgroup ceiling killed it with no `BudgetExhausted`: a **budget-guard-
  coverage gap** - the budget is not tracking that query's allocation path. Distinct and more urgent.
- **time** - wall cap, not memory: a perf wall, named separately (not mis-bucketed as memory).
- **other row-fail** - a row is emitted with a non-memory reason (including `unrecognized` with a diagnostic
  message): record the reason + diagnostic and route it separately. Do not force it into the memory family.

Precedence is row first, kill result second: if the process emits a `memory_budget_exhausted` row, it is a clean
guard failure even though the process exits non-zero. If no row is emitted, the systemd result distinguishes
OOM-guard-gap from time.

### D3 - route from the measured split (names the next arc, does not fix it)
- **now-pass** -> bank; update the atlas.
- **OOM-guard-gap** -> the **first** fix arc: extend the guard's coverage so these clean-fail/named, not OOM. A
  precondition - you cannot size a streaming fix for a wall you cannot observe cleanly.
- **clean `memory_budget_exhausted`** -> the **cost-bound streaming/spill** arc (the substantive engine work),
  scoped by how many cells land here and their operator shapes (hash-join build / aggregate state / sort).
- **time** -> performance wall; route separately from memory.
- **other row-fail** -> route by the emitted reason + diagnostic; this ADR's deliverable is the named wall, not
  forcing every candidate cell to remain in the memory family.

## Acceptance - the memory atlas is the deliverable
- A **memory atlas** for q07/q14/q17/q18/q19/q21 @ sf=1 (release + retention): each cell classified
  (now-pass / clean `memory_budget_exhausted` / OOM-guard-gap / time / other row-fail) with per-cell RSS + wall +
  named reason, plus the emitted `diagnostic_message` or the recorded absence of a row. Honest - greens *and*
  walls.
- The memory wall(s) **named** and the next fix arc(s) **scoped** (guard-coverage first if any OOM-gap, then
  streaming/spill). **No engine fix in this ADR.**
- sf=0.01 boards unchanged; q04/q12/q20's named cells unchanged; q02 @ sf=10/100 still banked (sf=10+ later arc).

## Risks
- **Probe discipline** - measure and name only; resist fixing a wall mid-probe (that's the next ADR, scoped from the
  split). An honest `memory_budget_exhausted@execute` cell is the deliverable, not a red to rush-fix.
- **Don't mis-bucket** - a `time` cell is a perf wall, not memory; an OOM-without-guard-fire is a coverage gap, not
  a streaming gap; a non-memory emitted row is an other row-fail. The split must be by the fixed rule, not
  after-the-fact judgment.
- **Row vs kill evidence** - ADR 332 can only retain diagnostics for rows the harness emits. A cgroup kill may have
  no row; record the service result instead of inventing a missing `diagnostic_message`.
- **Cost** - sf=1 x 6 on release is bounded but real; `MemoryMax=4G` + `RuntimeMaxSec=300s` mandatory; built binary
  direct (a `cargo-cgroup` rebuild looks like a hang - ADR 282).
- **Release ≠ different data** - memory is data-driven; release lowers per-op overhead, not data size, so a genuine
  unbounded plan still walls (that's the signal).

## Forward arc
The split decides the memory arc's shape: free wins bank, guard-coverage gaps get the budget guard extended first
(so all memory walls fail cleanly+named), then the cost-bound streaming/spill arc does the real work - each fix ADR
scoped to the measured cells. Best case, release free-wins shrink the family before any engine cut.

## Why toron-home
The probe + classification live in `toron-tpch` (the release run + the ADR 332 retention); the budget guard and any
fix are `crates/toron` (daemon.rs / the budget mechanism). This ADR only *measures*. No Wendelon / cross-ring
surface.

## Council provenance
Operator + Codex + Claude (convergent): re-measure q07/q14/q17/q18/q19/q21 in release with diagnostic retention,
then split clean `memory_budget_exhausted` from true OOM / guard-coverage gaps, time walls, or other row-emitted
failures. The measure-don't-predict discipline that ran the reach, correctness, and scale arcs, applied to the
memory family - name the wall before scoping the cure.
