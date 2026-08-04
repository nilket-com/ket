# ADR 456 - paired ablation at a large-budget committing cell

Run the full 449/451 paired-ablation protocol at the observed committing cell
(memory_per_task = 128 MiB, spill_reserve = 2 GiB, sf = 8) and seal whether the 446
bridge is material at the observed larger-budget cell. This is the question ADR 454
left standing and ADR 455's forward arc pointed at; both answers are results.

## Context

- 451 sealed the bridge as material at the (64 MiB, 512 MiB, sf=4) cell (HL 8.4 s,
  25.7% of the OFF median), and 454 showed that at the probed larger budgets, sf=4
  never enters the reservoir at all.
- 455 observed a larger-budget committing cell: (128 MiB, 2 GiB reserve, sf=8),
  reservoir 11,742,021 rows, with the spill reserve proven to be a third coordinate.
- Whether the bridge's filter effect is material at that cell is unknown. The two cells
  differ in task budget, spill reserve, and scale, so this experiment can establish
  materiality across measured cells, not attribute any difference to one coordinate.

## Decision — three rails

**Rail 0 — budget-locked cohort identity.** The paired-ablation manifest gains
`memory_per_task` and `spill_bytes_reserved` (values from the 454/455 gauge knobs, the
same ones every child row already records). A resume across different budget
coordinates refuses exactly like a scale or dataset mismatch — after 454, a cohort's
cell coordinates are part of its identity. Pre-456 manifests lack the fields and refuse via the
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

- **Material at the cell** → the bridge is material across both measured cells. This
  supports neither a universal reservoir claim nor attribution to one coordinate.
- **Immaterial or undetectable at the cell** → the bridge is not material at the
  observed cell. Combined with 454, reorientation of the 446 line comes back into
  view with paired evidence from two cells behind it.
- **Gate zero fails at the cell** → the switch or the oracle's q05-shaped equality
  does not transfer to sf=8; seals as a defect report with the evidence.

No cross-cell wall-clock ranking is claimed in any outcome: the cell's HL verdict is
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
- **Do not re-judge sealed verdicts.** 443–455 stand, each at its measured cell.

## Forward arc

Material → the bridge is material across two measured cells; the 446 12x spill-scope
gap becomes the next bridge-line item. Immaterial → chain reorientation re-enters with
paired evidence from two cells. Either way the paired instrument has now measured both
cells. The engine-side `Default` retarget, `MAX_FILTER_BYTES`
cap raise, ADR 132's comparator, q17 remeasurement, q21 residual, q19 re-diagnosis +
438 widening, q13, Cut A (439 mirror), and the generic O(1) LRU rewrite remain queued.
