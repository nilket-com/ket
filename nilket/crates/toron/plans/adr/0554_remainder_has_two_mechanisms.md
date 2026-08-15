# ADR 554 - The remainder has two mechanisms, not one

**Status:** Proposed 2026-08-15. No implementation.

## Context

ADR 551 recorded its verdict in the weak form — "no component dominates" — because
the four additive components did not reconcile to the cell delta, and named the
shortfall `remainder_ms`: *"large, it sits inside the measured envelope, and the
current taxonomy does not name it. Any future component verdict that wants a
mechanistic reading needs it accounted for first."*

The decomposition receipt (`a16ab3e6`) split that scalar by **where the time was**,
using counters the engine already produced and the artifact previously collapsed:

- `telemetry_lock_ms` — a measured activity.
- `framework_overhead_ms` — **a residual the worker computes**,
  `task_wall - accounted` in `server/mod.rs`. It is not an activity timer. It
  locates time inside a task without saying what the task was doing.
- `in_task_unattributed_ms` — those two, summed over the same path nodes the
  components are summed over.
- `unnamed_residual_ms` — `remainder_ms` minus that. Time no task named at all,
  so necessarily **outside every task wall**.

A fresh SF=8 run on a clean build produced an authoritative artifact — 21
finalized cells, no replay violations, no withheld reasons — held outside the
repository at `/home/me/atlas-stage2-remainder-clean-rerun/timing_atlas_remainder.jsonl`
(build `7dce25b6a1ef`). Medians over 7 pairs per cell, for the four cells where
ADR 551 found no component leading at all:

| cell | remainder ms | in-task ms | unnamed residual ms |
|---|---:|---:|---:|
| q06 | 613.4 | 516.9 | 96.4 |
| q13 | 2,887.4 | 2,592.1 | 291.2 |
| q14 | 445.3 | 172.3 | 273.2 |
| q19 | 552.2 | 2.3 | 550.0 |

`telemetry_lock_ms` is zero in all four, so the in-task figure is entirely the
worker's own residual.

**This disproves a single-mechanism story.** q06 and q13 are overwhelmingly
unbracketed work *inside* task walls. q14 is split roughly evenly. q19 is the
opposite of q06: within its tasks everything is accounted, and 550 of its 552 ms
sits **between or around** task walls. One instrument cannot see both.

## Decision

### 1. The remainder is investigated as two independent branches

Not one question with one answer. Each branch gets its own instrument, its own
evidence, and its own record. Neither result licenses a conclusion about the
other.

### 2. Branch A — in-task unbracketed work, as a named remainder subterm

Test fused aggregate compute by bracketing it, and report the result as a
**named subterm of the remainder**. Never as an ADR 538 component.

`framework_overhead` holds *every* unbracketed in-task activity. Its size is
therefore not evidence that aggregates are in it. A positive result must come
from a bracket that attributes aggregate time specifically, and must be read as
"this much of the in-task residual is aggregate compute" — not as a new component
and not as an account of the whole bucket.

ADR 538's frozen phase-to-component mapping is not reopened here. ADR 550 already
withdrew one component verdict for scope reasons, and this record does not spend
that lesson again.

### 3. Branch B — the outside-task residual, split into counted slack and dark lifecycle

A read-only audit of the sealed artifact narrowed this branch before any
instrument was built, and corrected the premise this record was drafted on.

**The gap is structural, not scattered.** `unnamed_residual_ms` equals
`measured_elapsed_ms - path_wall_ms` to within 0.1 ms in all four cells. Per-node
sums reconcile exactly — q19's path carries 2,148.236 ms of wall against
2,314.996 ms of dispatch — so `dispatch` is additive per task along the path, not
nested. The residual therefore splits into two parts with entirely different
standing:

| cell | unnamed residual | `dispatch - wall` (counted) | outside dispatch (uncounted) |
|---|---:|---:|---:|
| q06 | 96.4 | 95.8 — 99% | 0.5 |
| q13 | 291.2 | 277.9 — 95% | 15.3 |
| q14 | 273.2 | 126.4 — 46% | 140.1 |
| q19 | 550.0 | 167.0 — 30% | 382.9 — 70% |

**B1 — counted coordinator slack: `dispatch_wait - task_wall`.** Already
observable via `dispatch_wait_micros`, which the scheduler has recorded all
along; ADR 538 retains it raw and never eligible because the local arm has no
dispatch equivalent. **This is not grounds for new timing.** It was hidden by
aggregation, not missing, and the receipt now exposes it.

**q06 and q13 are overwhelmingly B1** — 99% and 95%. Their outside-task residue
is coordinator slack that already has a name, so they are not evidence cells for
new instrumentation.

**B2 — the uncounted outer lifecycle: before first dispatch and after final
completion and drain.** This is the real instrumentation target. The measured
envelope brackets `SqlPlanner::plan(...)` **and** `execute_query_with_context(...)`,
so logical planning, binding, fragmentation, scheduling, and the result-stream
drain are all inside it — while `dispatch_started` opens at the dispatcher call,
leaving everything before the first dispatch and after the last completion
outside every existing window. No timer covers it: all ten `Instant::now()` uses
in `coordinator.rs` are deadlines and intervals, none durations.

**q14 and q19 are B2's evidence cells** — 51% and 70% outside dispatch. The
original framing named q19 alone; q14 belongs with it.

**Dispatch slack identifies waiting, not its cause.** `dispatch_wait - task_wall`
mixes queueing, transport, and worker-start latency into one figure. It says the
coordinator waited longer than the worker ran; it does not say why, and it must
not be read as though it did.

**Any new timer must separate pre-dispatch from post-completion.** One lifecycle
bucket spanning both would repeat exactly the defect this branch exists to
correct: `framework_overhead` was a single opaque residual, and the only reason
this record exists is that splitting it revealed a disjunction. A B2 instrument
that lands as one number has learned nothing from that.

**B2 is three sequential figures plus a residual, none of them components:**

| figure | owner | span |
|---|---|---|
| `sql_planning_ms` | harness | around `SqlPlanner::plan` |
| `pre_dispatch_ms` | coordinator | planning/binding handoff → the dispatcher call; covers bind, fragment, schedule |
| `post_completion_drain_ms` | coordinator | final task completion → result completion and return |
| `lifecycle_residual_ms` | derived | what the three do not cover |

**Planning is not folded into `pre_dispatch_ms`.** They have different owners —
the harness times planning outside the coordinator entirely — and the two arms'
declared timing boundaries differ: the local arm's `execute_ms` starts after
binding by declaration (`NODE_ATTRIBUTION_OUTER_BOUNDARY_MICROS`), while the
distributed envelope contains it. Merging them would produce a figure whose
meaning changes with the arm.

**Both arms' planning boundary is reported explicitly, and no equivalence is
forced.** The receipt states where each arm's timer opens. It does not pair them,
does not difference them, and does not promote either into ADR 538's component
mapping. A figure that exists on one arm and not the other is a fact about the
instruments, not a component with a missing local side.

**The residual stays until coverage is demonstrated.** `lifecycle_residual_ms` is
carried explicitly and reported even when small, until the three spans are shown
to account for the gap. Dropping it the moment three plausible timers exist would
replace one opaque number with three confident-looking but incomplete ones —
which is this record's own failure mode, committed a second time with better
vocabulary.

### 4. Neither branch may be promoted to a component on envelope share alone

This is the record's central guardrail. A term that occupies a large share of the
measured envelope has demonstrated **where cost sits, not what it is**. Promotion
to an ADR 538 component requires the pairing, scope and fusion-invariance
arguments that record demands, established on their own evidence.

A subterm that grows large is a better-located unknown. It is not a finding about
mechanism, and a receipt that reports it must not read as one.

### 5. Sequencing: the lifecycle audit is done; B2 instrumentation is next

The audit in §3 is complete and read-only. It answered its question — **q19's gap
is 30% counted and 70% dark** — and it did so from the sealed artifact and the
source, without adding a timer. That ordering is the point: an instrument built
before the audit would have measured B1 all over again, since B1 is where q06 and
q13 put almost all of their residue.

Next is a B2 instrument on q14 and q19, split per §3. Branch A follows
independently, on its own cells.

## Acceptance

- **The two branches are separable in evidence.** A cell's in-task and
  outside-task figures move independently across the branches' instruments;
  if one instrument moves both, the split in §1 was wrong and the record is
  falsified rather than adjusted.
- **Branch A attributes by bracket, not by residue.** The aggregate subterm is
  the bracketed measurement; the control asserts that the residual bucket
  shrinks by what the bracket claims, so an unbracketed activity cannot be
  silently credited to aggregates.
- **Branch B accounts for q14 and q19 in the artifact**, not in an argument: the
  B1 slack already reported plus the B2 intervals a new instrument names sum,
  within the declared tolerance, to each cell's `unnamed_residual_ms`.
- **A B2 instrument reports its three spans separately**, with
  `sql_planning_ms` distinct from `pre_dispatch_ms`. The control asserts distinct
  figures; one combined lifecycle number fails it, because that is the
  opaque-bucket defect this record was written to correct.
- **`lifecycle_residual_ms` is present in the receipt**, including when it is
  near zero. Asserted as presence: a receipt that omits it once the three spans
  look convincing has stopped measuring coverage and started assuming it.
- **Neither arm's planning figure is paired with the other's.** Asserted as the
  absence of a differenced or paired planning term, since the arms' boundaries
  are declared differently and an equivalence would be manufactured.
- **B1 gains no new timer.** Asserted as absence: the coordinator slack is
  reported from `dispatch_wait_micros`, which already exists.
- **No component is added by either branch.** Asserted as the absence of a new
  entry in ADR 538's mapping, so the guardrail is a check rather than an
  intention.
- Every figure stays re-derivable from the artifact's nodes, as the replay
  validator already requires of the decomposition.

## Guardrails

Envelope share is not component share. This applies to `framework_overhead`
today and to whatever Branch A and Branch B name tomorrow.

Naming is not explaining. `framework_overhead` is a named term that explains
nothing: it is what the worker could not attribute. A subterm that merely
relabels it has added a word, not a mechanism.

Neither branch's result may be used to weaken ADR 551's recorded verdict. That
verdict stands on its own artifact, and this record exists to account for what
it declined to claim.

## Risks

Branch A may find aggregates are a small part of the in-task residual, leaving
q06 and q13 as located-but-unexplained. That is an honest outcome and better than
the current single scalar; it is not a reason to widen the bracket until
something large is captured.

Branch B may find the lifecycle boundary is simply not instrumented, in which
case the work is instrumentation rather than analysis, and the cost of that
instrument is its own decision.

The two branches may interact through the critical path: time that moves from
outside a task to inside one, or the reverse, changes both figures. The
separability gate in Acceptance exists to detect exactly that.

## Forward

Branch B's audit is done and recorded in §3. A B2 instrument on q14 and q19 —
`sql_planning_ms`, `pre_dispatch_ms`, `post_completion_drain_ms`, and an explicit
`lifecycle_residual_ms` — is the next cut. Branch A follows.

Neither is a perf change. Both are attribution work, and a perf claim built on
either needs the pairing discipline ADR 538 and ADR 551 already established.

ADR 551's `remainder_ms` question is now two questions with a measurement behind
the split. That is the whole content of this record: the scalar was hiding a
disjunction, and the receipt found it before an instrument was built for the
wrong one.
