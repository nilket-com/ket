# ADR 449 - paired q05 ablation of the 446 bridge, gate-zero gated

Run the paired ablation of the ADR 446 runtime-filter bridge behind a hard gate-zero
checkpoint. Four executable rails, gate zero first; the cohort is contingent on gate zero
inside the cut. No wall or spill result is admissible without profiled cardinality
evidence.

## Context

448 shipped the ablation switch (`TORON_ABLATE_LOCAL_RUNTIME_FILTER`, default-on,
`streaming.rs:12433`), the per-child arm driver (`paired_ablation_arm`, `main.rs:2907`;
env per child `:922-927`; row tags `:932`), and the estimator (`paired_median_shift`
`:2709`) — but left the tagged samples **unpaired** (estimator called only from tests) and
never set `GROUPED_CHAIN_PROFILE_DIR` on the perf child, so the timing sweep yields no
`reservoir_build_rows`. Gate zero is a separate profiled run.

`reservoir_build_rows` is the discriminator that correctness parity cannot provide:
identical output appears whether the switch works or does nothing. Gate zero measures
whether the arms actually differ before any cohort spends an hour per cell.

## Decision — four executable rails, gate zero first

**Rail 1 — gate zero: per-arm profiled cardinality, oracle-derived expectation.**

- Each q05 arm runs as a fresh child with **its own fresh empty `GROUPED_CHAIN_PROFILE_DIR`**.
- After each arm, assert the dir holds **exactly one** `grouped_chain_*.json` (the
  profile-id counter is process-global from 0, so a fresh child's first profile is `0000`
  — `grouped_chain_profile.rs:16,361,1191`); a second file (`0001`) is ambiguous —
  **fail, do not pick the first**. Parse `reservoir_build_rows` (`:916`).
- **Derive the OFF expectation from the input oracle, not a constant.** The OFF arm's
  reservoir input equals q05's full lineitem build cardinality —
  `generate_strict_lineitem(scale_factor).num_rows()` (`lib.rs:907`), the exact batch the
  run scans. **The assertion is scale-parametric: OFF == that count, ON strictly less.**
  59,986,052 was the sf=10 value and must not be hard-coded.
- **State the equality's reason, or the gate mis-fires on other queries.** OFF equals
  lineitem cardinality *because q05's reservoir build side is unfiltered lineitem* (ADR
  446's finding). Gate zero asserts `off_rows == reservoir_full_input_cardinality(q05,
  sf)`, defined as lineitem's count for this query — **not** a generic "reservoir input ==
  a table's row count", which would wrongly fail a query whose build side is itself
  filtered. This gate is q05-shaped by construction and says so.
- Require exactly one ON and one OFF; emit `{on_rows, off_rows, expected_off,
  off_matches_expected, on_strictly_less}`. Fail loud on any violation.
- **Blocks everything downstream.** Violation → the ADR seals as a switch-defect report on
  448, not a cohort.

**Rail 2 — pairing completeness (no order inference).** Pair by **(arm, run_index)**, not
array position:
- Validate every row's `ablation_arm` matches the parity of its `run_index`
  (`even→on, odd→off`); a mismatch is a driver fault, not a datum.
- Require run indices `0..2n` contiguous, each present once; **reject duplicate or missing
  `(arm, run_index)`**. A dropped child must be rejected, never silently realigned.
- Pair the k-th `on` with the k-th `off` in run-index order → n differences.

**Rail 3 — sign, pinned.** `diff = off − on`. A helping filter makes OFF slower → positive
diff → positive HL estimate → detectable-and-material in the improvement direction. A
known-sign fixture (OFF>ON → HL > 0) is mandatory: **a sign inversion flips the verdict
silently with full statistical confidence** — the one error the estimator cannot
self-catch.

**Rail 4 — cohort admissibility.** No HL or timing result is admissible unless the receipt
carries the Rail-1 gate-zero record **first**, with both identities passed. Receipt order:
gate-zero identities → n=10 paired samples (arm, run index) → HL point estimate + Wilcoxon
interval → dual-criterion verdict (detectable **and** ≥ 10% material). A receipt missing
gate zero is inadmissible, not weaker. Enforced by the emitter, not by convention.

**Order is mandatory.** Rail 4 before Rail 1 is the exact mistake all of 448 existed to
prevent.

## Both outcomes are results (predeclared)

- **Material** (interval excludes zero, HL ≥ 10% of baseline median): the 446 bridge earns
  its complexity; the arc's first clean throughput win.
- **Immaterial or undetectable**: the bridge reduces the reservoir (proven by gate zero)
  but does not move the wall — reservoir volume is **not** the q05 wall, the conclusion ADR
  446 reached through a confounded measurement and withdrew, here earned.

Predeclaring both keeps Rail 4 honest; a cohort hoping for a win is how the 446 null got
over-read.

## Acceptance

- **Gate zero:** per-arm fresh profile dirs; exactly one `grouped_chain_*.json` per arm;
  exactly one ON and one OFF; `off_rows == generate_strict_lineitem(sf).num_rows()`, ON
  strictly less. Tested by a fixture that **fails if both arms are full cardinality** (the
  no-op switch) and one that **fails if OFF is hard-compared to 59,986,052 at a non-10
  scale** (the oracle-derivation regression).
- **Pairing:** `(arm, run_index)` completeness; duplicate/missing/parity-mismatch rejected;
  no array-order inference. Dropped-child fixture rejected.
- **Sign:** `off − on`; known-sign fixture OFF>ON → HL > 0.
- **Admissibility:** a receipt without a passed gate-zero record cannot carry an HL block
  (tested).
- Every q05 figure non-comparable to any pre-Rail-1 baseline (447's 110,158.488554 ms
  included).
- **Cohort contingent:** gate-zero failure → no cohort; ADR seals as a switch-defect report.

## Guardrails and risks

- **Gate zero is the whole point** — a cohort on arms not shown to differ is this arc's
  recurring failure. The hard stop is not ceremony.
- **The OFF expectation is q05-shaped and derived.** Oracle-parametric on scale; equality
  justified by q05's unfiltered lineitem build side, not asserted as a generic reservoir
  law.
- **The sign is silent if wrong** — only the fixture catches it.
- **Fresh dirs, always** — a reused profile dir carries a stale `0000.json` and the
  identity read is of the prior arm. Empty-dir assertion before each arm.
- **Profiling must not perturb the identity.** `reservoir_build_rows` is a row count; gate
  zero runs profiled, the cohort unprofiled. Confirm the count is identical profiled vs not
  — if it differs, the profiler is not inert and that is its own defect.
- **Fifth cut on q05.** 444/447/448 apparatus; 446 a mechanism whose evidence apparatus
  destroyed. 449 is the first that can produce a clean number — the argument to finish —
  and equally the moment to ask whether q05 has earned it. Gate zero makes that cheap: one
  profiled pair resolves whether there is anything to measure.
- **Do not re-judge sealed reds.** 443/444/446 stand.

## Forward arc

**Gate zero fails** → the 446 bridge is inert or the switch miswired; a defect cut, and
reorientation's premise weakens further.

**Cohort immaterial** → reservoir volume is not the q05 wall; 447's raw-sample retention
makes "then what is" answerable; chain reorientation (demoted) re-examined under the same
paired instrument.

**Cohort material** → first throughput win of the arc; the paired instrument becomes the
template for other spill-heavy cells (q18, q21).

The ADR 446 12x spill-scope gap remains open. Query-budget realism (`memory_per_task` via
`test_default()`), `MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17 remeasurement,
q18 strict one-pass, q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439
mirror), and the generic O(1) LRU rewrite remain queued.
