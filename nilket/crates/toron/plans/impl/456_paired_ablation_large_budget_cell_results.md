# ADR 456 implementation receipt

The paired ablation at the observed larger-budget cell (memory_per_task = 128 MiB,
spill_reserve = 2 GiB, sf = 8) completed cleanly and its dual-criterion verdict is
**detectable but immaterial**: the 446 bridge measurably helps at this cell, but below
the predeclared 10% materiality floor. The bridge is material at the (64 MiB, 512 MiB,
sf=4) cell (451: 25.7% of OFF median) and immaterial at this one (~9.8%); its
materiality is **not regime-robust across the two measured cells**.

## Cohort integrity

- Single detached release parent; 21/21 journal lines; zero `BudgetExhausted` in any
  child (the 2 GiB reserve stayed non-binding, as Rail 2 required); no summary flags.
- Gate zero passed at sf=8: OFF reservoir 47,989,007 rows == the scale-parametric
  oracle (`generate_strict_lineitem(8.0)`), ON strictly less, output digests identical
  across arms.
- Manifest carries and locks all cohort coordinates including both budget dimensions
  (`memory_per_task: 134217728`, `spill_bytes_reserved: 2147483648`, both `explicit`),
  and a resume across budget coordinates refuses — pinned in the extended
  `adr449_paired_journal_is_durable_and_resume_config_locked`.

## The verdict (mandatory order, within-regime only)

- Pairing complete: 10/10 pairs positive, OFF−ON range 3.24–6.17 s.
- **HL OFF−ON shift: 4,273 ms**, 95% interval **[3,598, 5,068] ms** — excludes zero:
  **detectable**.
- Baseline (OFF-arm median): 43,748 ms; predeclared floor (10%): 4,375 ms. HL point
  4,273 ms < floor: **immaterial** under the dual criterion. Verdict: the bridge does
  not earn materiality at this cell.
- Near-threshold character, stated for honesty and not as a re-judgment: the point
  estimate sits at 97.7% of the floor and the interval spans it. The predeclared rule
  judges the point estimate; the verdict is immaterial. Anyone re-litigating the floor
  is re-litigating 449's predeclaration, not this receipt.
- Arm medians: ON 39,520 ms, OFF 43,748 ms. No cross-regime wall-clock ranking is
  claimed; both this verdict and 451's are within-regime.

## What this seals

The bridge is material at the (64 MiB task, 512 MiB reserve, sf=4) cell and detectable
but immaterial at the observed larger-budget cell (128 MiB, 2 GiB, sf=8): **its
materiality is not regime-robust across these two measured cells.** The effect itself
is real at both (both cohorts detectable, all 20 pairs across both positive).

Causal scope, stated precisely: the two cells differ in all three frontier
coordinates — task budget, spill reserve, and scale — so this evidence does not
attribute the materiality change to any single axis. "The effect shrinks as budget
grows" is one hypothesis among several (scale and reserve are live alternatives);
isolating the axis would take a cell that varies one coordinate against a measured
cohort, and no such cell has been run. With 454's separate finding that the probed
larger budgets avoid the regime entirely at sf=4, what is established is only: the
bridge's materiality holds at one measured cell and fails at the other.

Chain reorientation (demoted in 446's confounded era) re-enters the queue with paired
evidence from two cells behind the decision — exactly the position 449's
predeclaration designed for.
