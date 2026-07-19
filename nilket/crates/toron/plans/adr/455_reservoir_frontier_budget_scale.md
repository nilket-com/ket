# ADR 455 - the reservoir frontier: commit scale as a function of budget

Map where the grouped-reservoir regime begins in the budget×scale plane for q05, by
testing a predeclared linear prediction rather than surveying blindly. One ladder per
probed budget, rungs chosen around the predicted crossover; every record carries its
budget. A confirmed law turns 450/454's point observations into a predictive frontier;
a miss localizes an unmodeled term in the estimate. Both are results.

## Context and the prediction under test

- The reservoir commits when the 64×-inflated build estimate exceeds the retained cap
  of 16 × `memory_per_task` (ADR 450's threshold anatomy; `exec/join.rs:56`
  multipliers).
- Measured points: at 64 MiB the q05 crossover is sf=4 (450, reconfirmed as 454's
  control); at 256 MiB and 1 GiB, sf=4 does not commit (454).
- The build estimate is cardinality-driven and lineitem's cardinality is linear in sf,
  so the predicted crossover scale is linear in budget:

  **s\*(B) ≈ 4 × (B / 64 MiB)** — s\*(96 MiB) ≈ 6, s\*(128 MiB) ≈ 8.

Predeclaring the prediction keeps the probe honest: rungs are placed to bracket the
predicted crossovers, and the outcome is judged against the prediction, not curve-fit
after the fact.

## Decision — three rails

**Rail 1 — budget-stamped ladder records.** The 450 ladder's rung and terminal records
gain `memory_per_task` + provenance from the 454 plumbing, so a frontier artifact is
self-describing. (The rung children already record it; the ladder summary must too.)

**Rail 2 — bracketed ladders at two budgets.**

- 96 MiB (`TORON_TPCH_MEMORY_PER_TASK=100663296`): rungs sf 5, 6, 7 — bracketing
  s\* ≈ 6.
- 128 MiB (134217728): rungs sf 7, 8, 9 — bracketing s\* ≈ 8.
- Each ladder in its own output dir; per-rung fresh profile dirs and cleared spill
  scratch (450 rails); datasets prepared per rung by the existing ladder machinery.
- 256 MiB predicts s\* ≈ 16 — beyond the gauge's sf=10 bound; predeclared out of range
  and not probed. Recording that bound is part of the result.

**Rail 2b — isolate the variable under test (added when execution surfaced it).** The
first 128 MiB bracket run walled at sf=7 on `BudgetExhausted(SpillBytes)`: the
512 MiB `spill_bytes_reserved_per_query` test default exhausted before the reservoir
threshold was reached. A frontier probe must vary one budget dimension with the others
out of the way, so the gauge gains the same explicit knob for the spill reserve
(`TORON_TPCH_SPILL_BYTES_RESERVED`, provenance-recorded like the task budget), and the
128 MiB bracket re-runs with the reserve raised (2 GiB, explicit). The walled first
attempt is preserved as evidence that the dimensions interact; it is not the frontier
answer.

**Rail 3 — judgment, predeclared.**

- **Confirmed** (each probed budget's smallest committing rung within ±1 of
  prediction): the retained-cap crossover follows the linear relation at the observed
  cells, conditional on a non-binding spill reserve (Rail 2b's demonstrated third
  coordinate). Future cells are stated as (memory_per_task, spill_reserve, scale) with
  the reserve predeclared non-binding; extrapolation beyond observed cells is a
  hypothesis to bracket, not a placement.
- **Shifted or absent** (crossover off by >1 rung, or no commit through the bracket):
  the estimate model has budget- or scale-dependent terms the anatomy misses; the
  deviation's direction and size become the next diagnosis target. No re-probing
  beyond the bracket inside this cut — a miss seals as a miss.
- Non-commit diagnostics recorded per rung (Guard A vs B vs zero-profile), since which
  guard fires near the frontier is itself diagnostic.

No paired cohorts, no materiality claims, no wall-clock claims — reachability
classification only, in the 450/454 mold.

## Acceptance

- Ladder records carry `memory_per_task` + provenance; a test pins the field's
  presence in the ladder record.
- Both ladders run with the 450 isolation rails; every rung classified by the existing
  taxonomy; commit rungs report `reservoir_build_rows`.
- The results doc states, per budget: predicted s\*, observed smallest committing rung
  (or bracket exhaustion), and the confirmed/shifted verdict.
- No sealed verdict edited; wall clocks reported as context only, never compared
  across instruments.

## Guardrails and risks

- **Prediction before probe.** Rungs are placed by the predeclared law; moving the
  bracket after seeing results is curve-fitting and forbidden inside this cut.
- **Dataset cost is real.** sf 5–9 datasets are prepared on demand and kept (the
  ladder already fingerprints them); disk is ample, but prep time is part of the
  probe's cost, not a hang.
- **A missed prediction is not a defect hunt.** The miss seals; diagnosing the
  unmodeled term is the follow-on cut's job.
- **Do not re-judge sealed verdicts.** 443–454 stand, each in its regime.

## Forward arc

Confirmed → future ablation or capability cells are stated as (memory_per_task,
spill_reserve, scale) with the reserve predeclared non-binding, and the 452 instrument
can be pointed at an observed committing cell of a larger-budget regime; cells beyond
the observed brackets are bracketed first, not placed by extrapolation. Miss → the
estimate anatomy (64× multiplier, cap derivation) gets a diagnosis cut. The engine-side `Default` retarget, the ADR 446
12x spill-scope gap, `MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17
remeasurement, q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439 mirror),
and the generic O(1) LRU rewrite remain queued.
