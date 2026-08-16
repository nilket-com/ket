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

**B2 — measured, and it is not the outer lifecycle.** This section originally
named the outer lifecycle — before first dispatch, after final completion — as
the instrumentation target. **That was wrong, and the instrument disproved it.**

Three sequential spans were added and run at SF=8 on a clean build
(`5af71d0c0f26`, authoritative, no replay violations). Medians over 7 pairs:

| cell | sql planning | pre-dispatch | producer finalization |
|---|---:|---:|---:|
| q06 | 0.09 ms | 0.22 ms | ~0 |
| q13 | 0.10 ms | 0.39 ms | ~0 |
| q14 | 0.12 ms | 0.35 ms | ~0 |
| q19 | 0.21 ms | 0.43 ms | ~0 |

**The outer lifecycle is negligible** — bind, fragment, schedule and SQL planning
together total under 1 ms against q19's ~590 ms gap. Any instinct to optimise the
planner or scheduler setup would have been chasing 0.15% of the cost.

**The missing time is inside the dispatch loop, outside the selected path's task
windows.** Adding a `dispatch_loop_wall_ms` envelope and differencing it against
the path's dispatch sum accounts for the residual almost exactly:

| cell | unnamed gap | B1 per-task slack | `loop_non_path_ms` | unexplained |
|---|---:|---:|---:|---:|
| q06 | 92.5 | 91.9 (99%) | 0.1 | 0.54 |
| q13 | 295.7 | 276.8 (94%) | 17.3 | 0.88 |
| q14 | 322.9 | 125.8 (39%) | **196.1 (61%)** | 0.89 |
| q19 | 592.9 | 166.8 (28%) | **425.4 (72%)** | 1.05 |

Reconciliation to within **0.5–1.1 ms** on every cell, computed within one
artifact. The B1 cells show near-zero `loop_non_path_ms`, so the figure is
specific to the B2 cells rather than a general artefact.

**The name is deliberately neutral.** `loop_non_path_ms` is
`dispatch_loop_wall - Σ(selected-path dispatch)`. That subtraction **cannot
distinguish** scheduler or dependency-release cost from off-path sibling tasks
doing real work, because the critical path carries one task per stage and a stage
may have many. An earlier draft called it in-loop coordination; that asserted a
mechanism the arithmetic cannot see.

**q14 and q19 are consistent with off-path sibling execution under two workers,
and that is as far as the evidence goes.** q19's path is
`(0,0)→(1,0)→(2,3)→(3,0)` while stage 2 holds four sibling tasks at ~184 ms
each; the artifact reports `worker_count: 2`. Two rounds of stage 2 predicts a
loop wall of ~2,749 ms against ~2,873 ms measured — much nearer that than either
fully parallel (~2,565 ms) or serial (~2,968 ms). `max_in_flight_tasks` is set to
the maximum stage width, so the scheduler cap is not the constraint.

**Superseded by measurement — and the two-worker reading was wrong.** `worker_id`
was known to the scheduler at completion but discarded before
`TaskTelemetryRecord`. Retaining it (`237fb575`) let a further authoritative SF=8
run answer the question directly, and the answer rejects the round-structure
explanation above rather than confirming it.

**Observed in that run, and stated no wider than observed:**

- Every distributed cell's tasks were assigned to **one real worker**. Across all
  21 cells and 7 attempts each, no single attempt used more than one worker.
- Placement was **stable across all seven attempts** of a given cell — a query
  kept the same worker on every repetition rather than rotating.
- **No nil identity appeared.** Every distributed task carried a real worker, so
  no default value leaked into the classification.
- Both workers were in use across the cohort, so the pool was live; the
  single-worker result is about placement within a query, not a dead worker.
- Cells with 11 and 15 tasks were affected equally, so this is not a small-plan
  effect.

**This is one authoritative run, not a scheduler guarantee.** The placement policy
has not been audited, so "every query lands on one worker" is an observation about
this artifact and must not be restated as engine behaviour until that audit
exists.

**Within that scope, `loop_non_path_ms` is explained.** Stage siblings assigned to
a single worker cannot have executed concurrently, so the figure is **serialized
off-path sibling execution** — real work the critical path cannot see — and not a
two-worker scheduling artefact. q19's seven task dispatch windows sum to
~3,013 ms against a ~2,972 ms loop wall: essentially serial.

Note what this did **not** require. Task telemetry carries durations only, with no
start or end instants, so temporal ordering is unobservable. Assignment alone was
sufficient here: siblings sharing one worker cannot overlap. Had the tasks been
spread across both workers, round structure would have remained undecidable on
this evidence.

The consequence is about method rather than engine: work done by off-path siblings
is real execution that critical-path attribution cannot see, so part of the
"unattributed remainder" is a property of the attribution model.

**q14 and q19 are B2's evidence cells** — 51% and 70% outside dispatch. The
original framing named q19 alone; q14 belongs with it. Both are now measured
above, and the outside-dispatch time is `loop_non_path_ms`, not outer lifecycle.

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

The B2 instrument is built, run, and reported above; `worker_id` retention landed
in `237fb575` and settled the mechanism. B2 is closed as an attribution question.

What it opened is a placement question, and that is the next cut: **a read-only
audit of why a scheduler reporting two-worker capacity places every query's tasks
on a single worker.** Read-only first — the residency and placement path is
unaudited, and an instrument built before that audit would repeat this branch's
own mistake of measuring before understanding what is being measured.

Branch A follows independently, on its own cells.

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
- **A derived loop-minus-path figure is emitted only when the path's dispatch
  windows are contained in the loop wall.** Otherwise both raw figures stand and
  the subtraction is refused — never clamped to zero, which would read as a
  measurement.
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

Branch B is measured, reconciled, and mechanistically explained in §3. Its
successor is not an attribution question but a placement one: why two-worker
capacity yields single-worker query placement. Branch A follows.

Neither is a perf change. Both are attribution work, and a perf claim built on
either needs the pairing discipline ADR 538 and ADR 551 already established.

ADR 551's `remainder_ms` question is now two questions with a measurement behind
the split. That is the whole content of this record: the scalar was hiding a
disjunction, and the receipt found it before an instrument was built for the
wrong one.
