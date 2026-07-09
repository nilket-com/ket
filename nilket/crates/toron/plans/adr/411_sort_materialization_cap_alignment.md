# ADR 411 - Sort materialization cap alignment (q10's last wall)

## Context

410 routed q10's over-cap materialized build through the existing spill writer; q10 now does ~110 s of correct work and
fails at its **last** wall: `sort materialization`, requested 76,797,659 / available 67,108,864 - reproduced twice with
identical bytes (and identical spill 215,703,848).

**The grounded asymmetry:** every task defaults to 64 MiB (`memory_per_task`, `distributed/budget.rs:95`), and the
engine's materializing surfaces apply *multipliers* over it - **except sort**:

| surface | effective cap | multiplier |
|---|---:|---|
| join build materialization (`join_output_materialization_memory_cap`) | 1 GiB | x16 |
| spill retained footprint (`join_spill_retained_footprint_memory_cap`, `exec/join.rs:33`) | 1 GiB | x16 |
| **sort materialization** (`execute_sort`, `exec.rs:1799`) | **64 MiB** | **x1 - raw task budget** |

q10 needs 76.8 MB - barely over the raw task budget, far under the 1 GiB materialization-family envelope. This cut is
**per-operator policy alignment, not a cap bump**: the 4 GB scope, the cgroup, and the 64 MiB per-task default are all
untouched; sort simply joins the same materialization budget policy its sibling surfaces already use. Top-k is not an
alternative here: the bundled q10 carries no LIMIT (`toron-tpch/src/lib.rs:510` doc comment), and re-cutting the
benchmark query is out of bounds.

**What this cut is not (honesty guard):** it is **not** an external-sort mechanism. A sort that exceeds the
materialization-family cap must still fail with the same named producer and route to a future external-sort ADR. This
ADR moves one operator onto the existing family policy; it does not make sort unbounded.

## Decision

### Phase 0 - prove and quantify (before code)
1. **Prove sort is q10's final wall** on the current seal: reproduce the wall (already twice-reproduced in 410; a fresh
   probe on the 411 worktree confirms) and record requested bytes / input rows / batch count at the failure point.
2. **Quantify against both policies:** requested (76,797,659) vs raw task budget (67,108,864) vs the family cap
   (x16 = 1,073,741,824). **Stop-line check:** if q10's sort in fact needs more than the family cap, there is no policy
   change to make - seal the diagnostic; external sort is required.

### The cut - named sort materialization cap
- Introduce `sort_materialization_memory_cap(task_memory)` and use it in `execute_sort`'s estimate check in place of
  the raw task budget. Implement it as a **named sort-specific multiplier constant set equal to the materialization
  family's x16**, with a comment tying it to the family policy (explicit, greppable, and independently adjustable if
  sort ever needs to diverge). Reusing the join constant directly is acceptable if the code reads better; the policy
  must be *named* either way.
- No other budget, scope, or operator changes. Generic: no query/table branch.

### Stop-lines
- **Over-family-cap sort** (Phase 0 or any future query): fail with the same producer; external-sort mechanism is its
  own ADR - not smuggled in here, not "fixed" by a bigger multiplier.
- **No scope/budget change:** 4 GB cgroup, 64 MiB `memory_per_task`, and all sibling caps untouched.

## Acceptance
- **Phase 0 recorded:** the reproduced wall + requested/rows quantification vs raw and family caps, before code.
- **Primary: q10 sf=10 moves off `sort materialization`** under the 398 scope: persisted correctness `pass@compare`,
  **or a named + reproduced next wall** (reach-board honesty; the sort was the last *known* wall).
- **Discriminating tests:** a sort just over the raw task budget but under the family cap **completes** (fails on
  pre-411 code); a sort over the family cap **still fails** with `sort materialization` (locks the honesty guard - the
  cap moved to the family policy, it did not disappear).
- **Controls:** q03/q04/q08/q19 sf=10 pass@compare; q03 sf=1 four-way within band (fingerprint `666e0ac028036f70`,
  spill 0). Optional fence evidence: q12's wall unchanged (67,728 over the retained cap - a different producer this cut
  must not touch).
- **No budget drift:** no 4 GB / cgroup / `memory_per_task` / sibling-cap change; the diff touches the sort cap policy
  and its tests only.
- **Full engine gate:** `--no-fail-fast` both crates + architecture_test after fmt; de-branded; no committed
  `.perf.data`.
- **Scope honesty:** this aligns one operator's cap policy; external sort remains a named future mechanism for
  over-family-cap sorts; q12 (412, component breakdown) and q05 (reservoir) remain the next cuts.

## Guardrails
- **Policy alignment, not mechanism** - the explicit framing; a future over-family-cap sort fails and routes to an
  external-sort ADR.
- **Named cap** - `sort_materialization_memory_cap` with a named multiplier tied to the family policy; no inline magic
  constant.
- **Stop-line over stretch** - if q10 needs more than the family cap, no policy change lands.
- **Won ground is a control** - q03 (both scales), q04/q08/q19; q12's distinct wall untouched.

## Risks
- **A next wall behind the sort** -> reach-board honesty: name + reproduce (q10 was time/no-row two ADRs ago; walls
  behind walls are the pattern, though the sort is the last currently visible).
- **The multiplier invites unbounded-sort drift** -> the over-family-cap test locks the ceiling; external sort stays a
  named mechanism.
- **Hidden coupling** (another surface reads the raw budget via sort's check) -> the diff is a named function swap at
  one call site; full gate + controls catch drift.
- **q10 completes but slowly** -> record honestly; execution speed is a later thread (q08's is already named).

## Forward Arc
411 landing likely completes q10 (76.8 MB sorts trivially under the family cap after ~110 s of already-correct join
work) - the second heavy time-cell to go green. Then 412: q12's retained-footprint shedding policy (component breakdown
first), and the q05 one-pass reservoir as the capstone. External sort joins the named-mechanism backlog alongside q08
execution speed and the decode residual. Article-grade: the sf=10 board's walls are now falling in order of size.

## Grounding evidence (file:line)
Sort wall: `exec.rs:1799` (`execute_sort` - estimate = input bytes + 4 B/row, checked vs raw `task_budget.memory`),
producer at `exec.rs:1821`; q10 76,797,659 / 67,108,864 reproduced twice, spill 215,703,848
(`plans/impl/410_q10_reoriented_build_accumulator_completion_results.md`). Task default: `memory_per_task` 64 MiB
(`distributed/budget.rs:95`). Family multipliers: `JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER = 16`
(`exec/join.rs:33`), `join_output_materialization_memory_cap` (`exec/join.rs:1024`; 1 GiB effective per the
403/410 wall reports). No LIMIT in bundled q10: `toron-tpch/src/lib.rs:510`. No external/spilling sort exists in the
engine (grep). q10 execute ~110-114 s to the sort wall (410 results).
