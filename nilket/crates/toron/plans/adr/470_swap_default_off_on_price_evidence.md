# ADR 470 - smaller-build swap: default off on the six-cell price evidence

The 469 price census forces the default decision, and the evidence decides it:
`hash_smaller_build_placement` defaults **off**. Two cells win materially (q02
+17.1%, q11 +22.5%); two regress materially (q19 −77.5%, q14 −195% — three times
slower); one regresses detectably (q17 −2.4%); one is null (q20). A blind
estimate-driven swap is net-harmful at its worst far beyond its best, and the worst
cases are exactly the ones a default inflicts on users who never asked.

## Decision

- Default off. The capability remains fully plumbed (466), contract-coherent (467),
  activation-proven (468), and one config flag away for any session or workload
  where it wins.
- The 469 artifacts stand unchanged: census and prices ran explicit arms and are
  default-independent.
- Re-activation is gated on diagnosing the regressing cells and pricing the swap
  decision itself (a costing rule the wins and losses both feed — the leading
  mechanism hypothesis, recorded for that cut: the restore `Project` over a swapped
  join may block downstream pushdown/recognition, which would explain regressions
  disproportionate to any build-size effect).

## Acceptance

- Default flipped; the capability tests opt in explicitly (they test the mechanism,
  not the default); the 469 knob-contract test pins the new default provenance.
- Standing gates green; q17 parquet sentinel green; no sealed verdict re-judged.

## Forward

The regression diagnosis (q14 as the fixture — cheapest, largest signal) and
placement-aware costing own re-activation. Queue otherwise unchanged.
