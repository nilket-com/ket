# ADR 464 implementation receipt — FALSIFIED AND REVERTED

The hash smaller-build placement shipped, its receipt claimed output-invariance, and
ADR 465's Rail 1 falsified it within hours: q17 strict parquet correctness fails
(`avg_yearly row 0 is NULL`) with the comparator engaged and passes with it
disabled. The implementation commit is reverted in full; this document is the
record of why.

## The falsification chain

- 464's invariance evidence was a 22-query sweep on the **memory** data source —
  which carries no optimizer byte facts, so the swap could never engage. The sweep
  was blind precisely where the change lived. The q05 sf=8 identity check passed for
  the same reason (no placement flip there).
- 465's correctness rail ran q17 on **parquet** (byte facts present): fail.
  Comparator neutralized via probe: pass. Reproducible both ways.
- A narrower hypothesis — constrain the swap to the fully-eligible
  `BuildByteUpperBoundAboveThreshold` terminal (every broadcast eligibility check
  passed except size) — was implemented and probed: **q17 still fails**. The swap
  machinery is not output-invariant for a shape reaching even that terminal on the
  parquet path. The defect is in (or interacts through) the swap+restore path under
  hash strategy, not merely in where the comparator was consulted.

## Disposition

- `git revert` of the implementation commit; board correctness restored (q17
  parquet green post-revert). The plan (sealed) stands as the intent record.
- The capability returns to the queue **gated on a validity diagnosis**: why the
  hash-strategy swap breaks q17's shape where the broadcast-strategy swap
  (validated since 130) does not. The failing fixture is cheap and in hand
  (`q17 strict parquet, sf=0.02`, engages on any build with the comparator).
- Standing lesson, banked: **output-invariance evidence must run on a source that
  arms the change.** A sweep that cannot engage the mechanism proves nothing about
  it — the 456 causal-scope lesson, one layer down the stack.
