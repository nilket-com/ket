# ADR 460 - the reorientation decision: retire it; the filter-cap raise succeeds it

Chain reorientation (lineitem as base probe, build on the filtered side — 446's
"structurally correct answer," demoted since) is **retired** for this arc. The
post-458/459 ledger shows its addressable share is bounded and its largest projected
cost remains, while a already-queued one-knob successor (`MAX_FILTER_BYTES` cap raise)
targets the same bytes at a fraction of the risk. Decided on evidence, not vote count.

## The evidence (post-458 format, measured cells)

ON-arm stage attribution at the (64 MiB, 2 GiB, sf=8) cell — 21.12 s root — with the
sf=10 probe agreeing within a few points:

| stage | share | responds to reorientation? |
|---|---:|---|
| `runtime_prep_build` (prefix-stage builds) | 36.7% | **no** — prefix joins are built under any chain shape; reorientation *adds* a built filtered side |
| `reservoir_partition_spill_write` (scan 48M rows + filter + route + encode 11.7M) | 30.8% | partially — the scan component survives (lineitem must be read regardless); only the route/encode share is addressable |
| `base_accumulation` | 18.0% | no — becomes the built side under reorientation |
| `reservoir_read_probe` | 9.1% | yes |
| `suffix_join` + rest | ~5% | no |

Reorientation's best measured-cell ceiling is therefore the reservoir write/read minus
the irreducible lineitem scan — plausibly well under 40% of an ON wall that the
bridge already made material — purchased with the board-wide grouped-geometry reshape
that ADR 442 named as the risk and that made 446 demote it in the first place. Since
459, this ledger is complete: no hidden spill producer might change the picture.

## The decision

1. **Retire chain reorientation** as a mechanism for this arc. Not demoted — retired:
   the resident-path fact (454: larger budgets simply bypass the regime) plus the
   materialized ledger says the reservoir regime's remaining costs do not justify a
   board-wide reshape. Re-opening it requires new evidence of a cell where the
   addressable share clears the reshape risk, stated in this ledger's vocabulary.
2. **Promote the `MAX_FILTER_BYTES` cap raise (1 MiB → ~2.7 MiB) as the successor**,
   contingent-on-446-landing in its original queue entry — a condition now met twice
   (451 material at sf=4; 458 material at sf=8). 446's FPR analysis (17.3% → ~1%)
   projects, applied to the measured sf=8 cell: ON reservoir 11,742,021 rows ≈ true
   matches (~3.4M, inferred as 11.74M − 0.173 × 48.0M) + false positives; at ~1% FPR
   the reservoir drops to ~3.9M rows — roughly a further 3× reservoir cut for a cap
   constant, with the paired instrument and gate zero standing ready to verify it.
   The inference is ledger-derived and stated as an estimate; the successor cut's
   gate zero measures the real count.

## Acceptance

- This ADR is the decision record; its impl receipt carries the ledger tables and the
  FPR arithmetic with assumptions named. No code changes in this cut.
- The queue entry "chain reorientation (demoted)" is replaced by this retirement;
  `MAX_FILTER_BYTES` moves to the queue head with its predicted effect attached.
- No sealed verdict re-judged; no wall/materiality claims beyond citations of sealed
  receipts.

## Guardrails and risks

- **A retirement is falsifiable.** The re-opening condition is stated (a cell whose
  addressable share clears the reshape risk); this is a decision under the measured
  cells, not a theorem.
- **The successor's projection is an estimate.** The ~3× reservoir cut assumes 446's
  FPR figures transfer to sf=8's key distribution; gate zero at the successor cut is
  the measurement, and an estimate miss is a result, not a defect.
- **DuckDB's 206 MB** (446's structural argument for reorientation) remains true and
  unexplained-in-full; it is recorded as retained context, not a live mandate.

## Forward arc

The successor cut (`MAX_FILTER_BYTES` raise + paired re-verdict at a measured cell)
is next. Behind it: the engine-side `Default` retarget, ADR 132's comparator, q17
remeasurement, q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439
mirror), and the generic O(1) LRU rewrite remain queued.
