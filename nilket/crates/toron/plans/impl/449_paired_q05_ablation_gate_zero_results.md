# ADR 449 implementation receipt

The executable rails are implemented, and the gate-zero stop is active.

## Implementation

- `adr449-gate-zero` launches fresh ON/OFF q05 children with per-child ablation
  and profile-directory environments.
- Each arm requires exactly one `grouped_chain_*.json`; the receipt parses
  `reservoir_build_rows` and compares OFF against the scale-parametric
  `generate_strict_lineitem(sf).num_rows()` oracle.
- Timing `--paired-ablation` runs gate zero first and refuses to start a cohort
  unless both cardinality identities pass.
- Samples are paired by `(run_index / 2, arm)`, with parity, contiguity,
  duplicate, and missing-arm rejection. Differences are `off - on` and feed the
  existing deterministic Hodges–Lehmann estimator.

## Gate-zero result

The real strict persisted q05 sf=1 check stopped at the declared boundary. The
ON child failed with:

`grouped-chain profile: admitted chain did not commit the generic grouped reservoir path`

No profile was accepted and no timing cohort ran. This is not treated as a
successful cardinality gate or as a performance result. The implementation
therefore fails loud rather than silently proceeding with an unprofiled or
wrong-path comparison. The plan's resolve-first scale assumption is disproven
on the current tree; a future run must select a scale that actually commits the
generic grouped-reservoir path and rerun gate zero before any cohort.

## Verification

- ADR 449 pairing, parity, oracle, and profile-cardinality tests pass.
- ADR 448 architecture guards remain green.
- No engine behavior was changed; no wall, spill, or materiality claim is made.
