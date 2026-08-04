# ADR 457 - the isolating cell: task budget as the single varied axis

Run the paired-ablation protocol at (memory_per_task = 64 MiB, spill_reserve = 2 GiB,
sf = 8) — a cell differing from ADR 456's measured cell in exactly one coordinate, the
task budget. Whatever the verdict, combining it with the sealed 451/456 cells yields a
single-axis attribution that the two-cell picture could not: 456 established that the
bridge's materiality is not robust across cells; 457 asks which axis moves it.

## Context — the measured cells and the confound

| cell | verdict |
|---|---|
| (64 MiB, 512 MiB, sf=4) — 451 | material (25.7% of OFF median) |
| (128 MiB, 2 GiB, sf=8) — 456 | detectable, immaterial (~9.8%) |

The 451/456 cells differ in all three coordinates, so 456 sealed only "not robust
across the measured cells." The proposed cell shares sf=8 and the 2 GiB reserve with
456's cell and varies the task budget alone (128 → 64 MiB, the same 2× step the
frontier law was confirmed over). It is deep inside the 64 MiB regime's committing
region (frontier at sf=4), so gate zero is expected to pass; its failure would itself
be a finding.

## Decision — one rail of execution, predeclared readings

**The cohort.** Identical protocol to 456 at the new cell: release-only, single
detached parent, `--sf 8 --runs 20 --warmup 1`, durable journal, fingerprint- and
budget-locked manifest (the 456 Rail 0 machinery, no harness changes), gate zero
in-run with the scale-parametric oracle, receipt in mandatory order, 10%-of-OFF-median
floor. `BudgetExhausted` in any child halts as apparatus-inconclusive; if the 2 GiB
reserve binds, one predeclared re-run with the reserve raised to 4 GiB (explicit) is
permitted — the 455 Rail 2b precedent, predeclared this time rather than improvised.

**Predeclared readings, all admissible:**

- **Material at (64 MiB, 2 GiB, sf=8)** → with scale and reserve held against 456's
  cell, the materiality difference between the two sf=8 cells attributes to the
  task-budget axis. (No claim about the axis's shape beyond these cells.)
- **Detectable but immaterial** → the budget axis does not restore materiality at
  sf=8 across the 2× step; scale becomes the leading suspect for the 451→456 change,
  with the reserve axis explicitly noted as still partially confounded against 451
  (512 MiB reserve provably binds at sf≥7 under these budgets — 455's walled bracket —
  so it cannot be held at sf=8; that residual confound is stated, not resolved).
- **Undetectable** (interval includes zero) → stronger than immaterial: the bridge's
  effect itself, not just its materiality, fails to transfer; same attribution
  reading as immaterial, stated at the stronger grade.
- **Gate zero fails** → the committing region's interior does not behave like its
  frontier; seals as a defect/diagnosis report, no cohort.

No cross-cell wall-clock ranking; every verdict is within-cell, per the standing
wording guard ("observed cell", never "realistic").

## Acceptance

- Gate zero at (64 MiB, 2 GiB, sf=8): OFF == `generate_strict_lineitem(8.0)` count,
  ON strictly less, digests identical.
- 21/21 journal lines, complete pairing, zero `BudgetExhausted` (or the one
  predeclared 4 GiB re-run, recorded), receipt with both budget coordinates
  `explicit`.
- Results doc states the attribution reading in the three-coordinate vocabulary and
  carries the four-cell table (451, 456, 457, plus 454's non-committing sf=4 row) so
  the attribution logic is auditable in one place.
- Standing gates green; no sealed verdict re-judged; no harness changes expected — if
  one proves necessary it must be documented as a deviation.

## Guardrails and risks

- **Cohort cost is the largest yet**: 64 MiB at sf=8 means the deepest measured
  reservoir (expected OFF ~2× 456's, so roughly 60–90 min serial). Detached, durable,
  single parent; cost is context, never a reason to trim runs mid-flight.
- **The reserve may bind** at this deeper spill point; the predeclared 4 GiB re-run
  bounds the improvisation space in advance.
- **Attribution is between measured cells only.** Three cells cannot map a surface;
  they can attribute one difference to one axis. No interpolation, no extrapolation.
- **Do not re-judge sealed verdicts.** 443–456 stand, each at its measured cell.

## Forward arc

Whichever axis the attribution lands on, the reorientation decision (re-entered by
456) inherits it as its first grounding fact. The engine-side `Default` retarget, the
ADR 446 12x spill-scope gap, `MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17
remeasurement, q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439 mirror),
and the generic O(1) LRU rewrite remain queued.
