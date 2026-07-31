# ADR 525 - Partsupp lifecycle bounds, prep and cleanup only (q09)

ADR 524 closed red on instrumentation validity, and its confound draws this cut's boundary:
the generic probe's lookup-vs-expand split cannot be measured with batch-scoped clocks without
per-row timers or reordering the work — the exact schedule change the perturbation gate
rejected. That split is also unnecessary for the decision at hand. ADR 516 already prices the
replay-2 parent (1,585 ms, reconciled as a parent), and the open question is only whether the
partsupp **lifecycle** — prep + probe + cleanup — clears the 3,554 ms floor. The straddling
interval [1,585, 3,914] is arbitrated entirely by the prep and cleanup shares.

This is therefore the minimum sufficient attribution cut:

## Decision

1. **Generic probing completely untouched** — no clocks, no staging, no code motion anywhere
   in the probe path. The replay-2 number in E is ADR 516's existing parent stage, as-is.
2. **Per-replay prep and cleanup only**: batch-scoped timers around the *existing* lifecycle
   regions — each build's preparation and teardown — emitting one `prep_build` and one
   `cleanup_build` child per suffix replay identity, with rows and retained bytes. No region
   is moved, split, or reordered; the clocks bracket code that already runs as a unit.
3. **One new fail-closed reconciliation rule**: replay-scoped children may partition an
   invocation-scoped parent iff — unique replay IDs; complete required coverage (every
   declared replay present); summed child time ≤ parent time; and exactly one explicit global
   residual carrying the remainder. Any violation fails the profile; nothing is inferred.
4. **The envelope**: `E = prep_build(partsupp) + suffix_probe(replay 2, existing parent) +
   cleanup_build(partsupp)`, computed only from a profile that passes both gates below.

## Acceptance

1. **Perturbation gates, both cells, no shortcut**: five interleaved profiler-off/on pairs at
   the exact coordinate for **q09 and q08 each**; absolute median bound 3% per cell; a breach
   is rejected, not corrected.
2. **Two-strike parking, predeclared**: any two rejections — perturbation or reconciliation,
   in any combination across the two cells' run sets — park q09 attribution permanently and
   the board moves directly to the shared-fold family. No third instrument attempt.
3. **Reconciliation**: the new partition rule holds for `runtime_prep` and `cleanup` families
   (unique IDs, complete coverage, sum ≤ parent, one global residual); replay-2's existing
   refinement continues to reconcile untouched; profiles report the partition verdict
   explicitly.
4. **Identity**: fingerprints, digests, row counts, route facts, and spill identical across
   every off/on pair; profiler-off byte-identity; once-per-runtime construction pinned.
5. **The decision, predeclared**: E < 3,554 ms → q09 **parked** with the split recorded;
   E ≥ 3,554 ms → a compact composite-key lifecycle becomes a legitimate single-envelope
   successor target. Remedy selection stays outside this ADR either way.

## Guardrails

- No planner or executor behavior change; no knob beyond the existing profiler env; census
  not applicable (identity asserted per pair).
- Representation facts from ADR 524's plan are dropped with the probe split — this cut
  measures time and rows only, the minimum E requires.
- Cleanup remains order-sensitive lifecycle ownership: E is a maximum removable upper bound,
  not a claim that teardown wall is independently recoverable.
- ADR 511-523 surfaces untouched; the 64-row-window observation from 524's confound remains
  an untested lead and is not consulted.

## Risks

- Even bracket-only clocks may perturb; the gates are the authority and the two-strike rule
  bounds total spend at one narrow attempt.
- E may land below the floor — that parks q09 honestly, the outcome the interval already
  half-predicts.

## Forward

- E ≥ floor: successor compact composite-key lifecycle ADR (build layout + probe + teardown
  as one mechanism, priced against this E).
- E < floor, or two strikes: q09 parked; next board unit is the shared-fold family (q18's
  2.75 + 2.28 s folds, q13's 3.16 s preaggregation residual, q20's route as the third
  tenant).
