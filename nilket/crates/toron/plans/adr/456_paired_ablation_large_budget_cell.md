# ADR 456 - paired ablation at a large-budget committing cell

Run the full 449/451 paired-ablation protocol at the observed committing cell
(memory_per_task = 128 MiB, spill_reserve = 2 GiB, sf = 8) and seal whether the 446
bridge's materiality survives outside the test-budget regime. This is the question ADR
454 left standing and ADR 455's forward arc pointed at; both answers are results.

## Context

- 451 sealed the bridge as material in the 64 MiB regime at sf=4 (HL 8.4 s, 25.7% of
  the OFF median), and 454 scoped that regime: at the probed larger budgets, sf=4
  never enters the reservoir at all.
- 455 observed a committing cell of a larger regime: (128 MiB, 2 GiB reserve, sf=8),
  reservoir 11,742,021 rows, with the spill reserve proven to be a third coordinate.
- Whether the bridge's filter effect is material *in that regime* is unknown — the
  materiality could be regime-specific (a property of the 64 MiB test constant) or
  regime-robust. Either sealed answer completes the arc's central question.

## Decision — three rails

**Rail 0 — budget-locked cohort identity.** The paired-ablation manifest gains
`memory_per_task` and `spill_bytes_reserved` (values from the 454/455 gauge knobs, the
same ones every child row already records). A resume across different budget
coordinates refuses exactly like a scale or dataset mismatch — after 454, a cohort's
regime is part of its identity. Pre-456 manifests lack the fields and refuse via the
byte-identical rule, never grandfathered.

**Rail 1 — gate zero at the cell.** 449's gate zero runs with the cell's env set:
OFF reservoir must equal `generate_strict_lineitem(8.0)`'s count (the oracle is
scale-parametric by construction), ON strictly less, output digests identical. Failure
seals as a defect/inapplicability report, no cohort.

**Rail 2 — the cohort, unchanged protocol at the cell.** Release-only, single detached
parent, `--sf 8 --runs 20 --warmup 1`, durable fsynced journal, fingerprint- and
budget-locked manifest, gate zero in-run, receipt in mandatory order, materiality
floored at 10% of the OFF-arm median with baseline and floor emitted. The 2 GiB
reserve is predeclared as required non-binding: any `BudgetExhausted` in any child
halts the cohort as apparatus-inconclusive (wrong wall), not as a verdict.

## Predeclared outcomes, all admissible

- **Material at the cell** → the bridge's value is regime-robust: it earns its place
  wherever the reservoir regime engages, and the 451 result generalizes beyond the
  test budget.
- **Immaterial or undetectable at the cell** → the bridge's materiality is specific to
  the 64 MiB regime; combined with 454, the bridge helps only in a regime that the
  probed larger budgets avoid at these scales — reorientation of the 446 line comes
  back into view, this time with two clean regime measurements behind it.
- **Gate zero fails at the cell** → the switch or the oracle's q05-shaped equality
  does not transfer to sf=8; seals as a defect report with the evidence.

No cross-regime wall-clock ranking is claimed in any outcome: the cell's HL verdict is
within-regime, exactly like 451's.

## Acceptance

- Manifest carries both budget coordinates; a budget-coordinate mismatch refuses
  resume (journal test extended); the cohort receipt shows both with `explicit`
  provenance.
- Gate zero passed at sf=8 with the oracle-derived OFF expectation; 21/21 journal
  lines; `(arm, run_index)` pairing complete; HL point + Wilcoxon interval +
  dual-criterion verdict in mandatory order.
- Any `BudgetExhausted` child → cohort halts, sealed apparatus-inconclusive.
- Standing gates green (architecture_test, targeted tpch suites); no sealed verdict
  re-judged.

## Guardrails and risks

- **The reserve must stay out of the way.** 455 proved the spill wall can silhouette
  the phenomenon; the halt-on-BudgetExhausted rail keeps this cohort's verdict about
  the bridge, not the reserve.
- **~25–40 min serial cohort** (children ~40–90 s at sf=8): detached single parent
  with the durable journal — the 451 discipline, not the pre-451 gamble.
- **OFF-arm cost is unknown at sf=8.** If OFF children are far slower than ON, the
  cohort still completes; cost is context, never a reason to trim runs mid-flight.
- **Do not re-judge sealed verdicts.** 443–455 stand, each in its regime.

## Forward arc

Material → the bridge graduates to a regime-robust mechanism; the 446 12x spill-scope
gap becomes the next bridge-line item. Immaterial → chain reorientation re-enters with
two regimes of clean evidence. Either way the paired instrument has now measured both
sides of the budget frontier. The engine-side `Default` retarget, `MAX_FILTER_BYTES`
cap raise, ADR 132's comparator, q17 remeasurement, q21 residual, q19 re-diagnosis +
438 widening, q13, Cut A (439 mirror), and the generic O(1) LRU rewrite remain queued.
