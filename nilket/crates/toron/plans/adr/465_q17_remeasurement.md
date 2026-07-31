# ADR 465 - q17 remeasurement under the current apparatus

Retire the stale q17 queue item by measuring it properly: the pre-447 baseline was
left-skewed (+4.56) and predates raw-sample retention, the stability gate, budget
provenance, the 458 page format, the 461 filter sizing, and the 464 placement change
— it is uninterpretable and non-comparable by every standard the arc has since built.
One clean measured run either records a fresh admissible baseline or seals an honest
instability finding.

## Context

- 448's deferred list carries the item verbatim: "q17 remeasurement — itself
  left-skewed (+4.56) in the pre-447 baseline." No artifact of that baseline
  survives under today's admissibility rules (no samples, no budget coordinates, no
  stability flag).
- The apparatus is complete for exactly this: retained `execute_ms_samples`, the
  release/passing/stable recordability gate, explicit budget knobs, and the standing
  wording that a figure without coordinates is not produceable.

## Decision — measurement only, in the 450/454 mold

**Rail 1 — correctness first.** q17 answer-check against its oracle at
`q17_correctness_scale` (the query's special-cased scale), strict parquet, explicit
coordinates. A red halts the cut as a defect report.

**Rail 2 — the measured run.** Release, single detached parent, q17 strict parquet
at sf=1 (the board's standard perf scale), explicit `(64 MiB, 2 GiB)` coordinates,
`--runs 20 --warmup 1`. No ablation arms — this is a plain perf rail; the paired
apparatus stays out.

**Rail 3 — predeclared readings.**

- **Stable** (spread within the standing 0.20 gate): the run records the fresh q17
  baseline (release+passing+stable is exactly the recordability gate), with sample
  skewness reported from the retained samples (moment skewness, formula stated) as
  context. The queue item retires.
- **Unstable or skewed again**: sealed as an honest instability finding with the
  retained samples as the diagnosis substrate — q17's wall shape becomes a named
  future diagnosis target; no baseline is forced past the gate.
- Either way: the pre-447 figure is cited as era-bound and non-comparable; no
  cross-era ratio is computed.

## Acceptance

- Correctness green at the special-cased scale before any timing.
- 21/21 journal-equivalent (20 timed + warmup in `latest_raw.jsonl`), zero
  `BudgetExhausted`, coordinates explicit in every row.
- Receipt: median, min/max, spread ratio, gate verdict, sample skewness with
  formula, and the recorded-or-refused baseline outcome.
- No engine or harness changes expected; a needed one is a documented deviation.

## Guardrails and risks

- **Do not chase stability mid-cut.** If q17 is unstable, that is the result;
  tuning or rerunning until stable is the anti-pattern the gate exists to stop.
- **Skewness is context, not a gate.** The recordability gate is spread-based; the
  skew number informs the future diagnosis, it does not veto the baseline.
- **Do not re-judge sealed verdicts or eras.**

## Forward arc

Stable → q17 has an admissible baseline for the article set. Unstable → q17 wall
diagnosis joins the queue with samples in hand. Behind it: q21 residual, q19
re-diagnosis + 438 widening, q13, Cut A (439 mirror), the O(1) LRU rewrite, the
budget-derivation gauge, and pricing a 464 swap cell remain queued.
