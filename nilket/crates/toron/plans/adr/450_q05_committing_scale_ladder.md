# ADR 450 - q05 committing-scale ladder probe

Probe the smallest scale at which q05 commits the generic grouped-reservoir path, and make
that rung the sole fixture scale for ADR 449 gate zero and all future q05 ablations. A
diagnosis rail: no paired cohort, no filter-effect measurement, no wall/spill claim.

## Why this exists

ADR 449's gate zero stopped at sf=1 — q05 takes the resident path, so the ADR 446 bridge
(inside `try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir`,
`streaming.rs:12350`; bridge at `:12433`) never fires. The paired ablation cannot run until
a committing scale is known, and assuming sf=10 costs ~180 s per arm. This cut probes the
smallest scale at which q05 commits the reservoir path and makes it the sole fixture scale
for 449 and future ablations.

## The threshold is metadata-driven, and pinned

- `memory_per_task` = 64 MiB (`distributed/budget.rs:95`)
- retained cap = `memory_cap × 16` = 1 GiB (`exec/join.rs:56,3669`)
- build estimate carries a 64× safety multiplier (`streaming.rs:10`)

The reservoir commits once the 64×-inflated build estimate exceeds 1 GiB — so the crossover
is at a raw build well below 1 GiB and cannot be computed by hand. It must be probed. q05
accepts any scale (only q11 is scale-constrained, `lib.rs:453`; `parse_scale_factor` takes
any finite positive float, `:10562`; dataset prep is scale-parametric), so intermediate
rungs are runnable.

## The outcome taxonomy — three states, two of them non-committing

This surface has **two** commit guards, both firing only under profiling:

- `:10038` — "admitted chain did not commit the **generic grouped reservoir path**": the
  resident path ran; the generic grouped path was never entered. (What 449 hit at sf=1.)
- `:12222` — "generic grouped path did not commit the **partitioned-scan reservoir**":
  the generic grouped path *was* entered, but
  `try_process_..._partitioned_scan_reservoir` returned `Ok(false)` — the reservoir
  declined and a different non-reservoir handler would have run.

**The 446 bridge is inside the function that returns false at `:12222`.** So on *both*
diagnostics the bridge did not fire — both are non-committing. Classifying only `:10038` as
a legitimate non-commit and treating `:12222` as a fault would abort the ladder on what is a
legitimate non-commit, not a fault.

Taxonomy, per rung:

| outcome | signal | ladder action |
|---|---|---|
| **committed** | exactly one `grouped_chain_*.json` with the reservoir profile present | **stop — this is the answer** |
| **non-committing** | child exits with `:10038` **or** `:12222`, recorded by which | climb higher |
| **hard error** | anything else — spawn failure, panic, missing/duplicate profile, any other `Kernel` message, non-zero exit without a known diagnostic | **abort the probe** — never silently "resident" |

Recording *which* non-commit diagnostic fired is diagnostically load-bearing: a rung that
reaches `:12222` (reservoir declined despite entering the generic grouped path) is evidence
the reservoir may never commit for q05 at *any* scale — which is the no-commit outcome
arriving early, and a stronger signal than `:10038`.

## Decision — the ladder as a diagnosis rail

**Rail 1 — linear ascending ladder.** Binary search is rejected for the first pass:
it can bracket but cannot establish the *smallest* committing rung without a final linear
refinement and a monotonicity check, and monotonicity of the 64×-inflated estimate in sf is
not established. Ascend rung by rung; the first **committed** outcome is the answer; stop
there.

**Rail 2 — per-rung isolation.** Each rung prepares and uses its **own
scale-matched persisted dataset** and **fresh profile and spill directories**. Stale sf=1
data must never be reused for sf>1; a reused profile dir would carry a prior `0000.json` and
read as a false commit. Empty-dir assertion before each rung; dataset fingerprint checked
against the rung's scale.

**Rail 3 — one profiled child per rung, single arm.** Filter at default (ON). The probe
asks *does the path commit*, not *does the filter help*, so one arm suffices. Report
`{scale, outcome, which_diagnostic?, execute_ms, reservoir_build_rows?}`.

**Rail 4 — sf=10 is an untrusted upper control.** sf=10 is *assumed* committing
from the 448 timeout and prior qualitative divergence, but neither is a profile. sf=10 does
not become a trusted control until a probed rung there produces an actual reservoir profile.
Until then it bounds the ladder but proves nothing.

**Rail 5 — the selected rung is the single source of truth.** The smallest committing scale
is emitted as the canonical q05 ablation fixture scale, consumed by 449 gate zero and every
future q05 ablation. No downstream ADR re-guesses it; a hard-coded fixture scale elsewhere
is the defect this rail prevents.

## Predeclared outcomes, all admissible

- **Cheap commit** (a low rung commits with acceptable runtime): it becomes the fixture
  scale; 449 reruns there; the arc proceeds.
- **No commit across the ladder** (every rung is `:10038`/`:12222` up to sf=10, or sf=10
  itself declines): the 446 bridge is unreachable at any practical scale on the current
  budget — the bridge is inert in the gauge and its place is reopened. A result.
- **Cost-too-high** (smallest committing rung is near sf=10): the probe reports the paired
  ablation costs ~N×180 s per cohort and hands the room an explicit affordability decision.
  This is where "has q05 earned a fifth measurement" is answered with a number.

Predeclaring all three keeps the probe from being run hoping for a cheap commit.

## Acceptance

- Linear ascending ladder; stop on first committed rung.
- Per rung: own scale-matched dataset, fresh empty profile+spill dirs (asserted empty
  first), one profiled ON child.
- Outcome classified into exactly {committed, non-committing(`:10038`|`:12222`), hard
  error}; **any non-known-diagnostic failure is a hard error, never "resident"**. A test
  asserts a fabricated unknown error is not classified non-committing.
- A test asserts `:12222` classifies as non-committing (climb), not hard error — pins the
  correction.
- sf=10 is labelled untrusted until it produces a real reservoir profile.
- The selected scale is emitted once and consumed by 449; a test asserts 449's fixture
  scale reads from this source, not a literal.
- No paired cohort, no filter-effect measurement, no wall/spill/materiality claim.

## Guardrails and risks

- **Two non-commit diagnostics, both mean climb.** Recording which one fired is the signal
  that separates "too small yet" (`:10038`) from "may never commit" (`:12222`).
- **Fresh dataset and dirs per rung.** Stale sf=1 data reused at sf>1 is silent corruption;
  a reused profile dir reads as a false commit.
- **sf=10 unproven until profiled.** A timeout is not a commit.
- **Monotonicity unestablished** — hence linear, not binary. If a later cut wants binary
  search, it owns proving commit-vs-scale monotonic first.
- **Sixth q05 cut** (444/447/448/449/450 apparatus, 446 mechanism). Five produced no
  throughput. The cost-too-high branch is the explicit stop mechanism; that is a feature.
- **Do not re-judge sealed reds.** 443/444/446/448/449 stand.

## Forward arc

**Commit found** → 449 gate zero reruns at the selected scale; if the arms then differ, the
paired cohort runs; the arc reaches its first clean throughput number or first earned null.

**No commit / cost-too-high** → the 446 bridge's reachability is the question; chain
reorientation (demoted) is re-examined as the path that does not depend on the reservoir
committing.

The ADR 446 12x spill-scope gap remains open. Query-budget realism (`memory_per_task` via
`test_default()` — directly relevant, since the 64 MiB budget sets the commit threshold this
ladder probes), `MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17 remeasurement, q18
strict one-pass, q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439 mirror), and
the generic O(1) LRU rewrite remain queued.
