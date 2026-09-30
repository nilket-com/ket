# ADR 613 - SF8 board re-seal after the grouped-fold cuts

**Status:** CLOSED, SEALED. One board on strix (results in
`plans/impl/0613_sf8_board_reseal_after_the_grouped_fold_cuts_results.md`;
artifact `2f029448…`, evidence tag
`evidence/906b879e526f-2f02944827b6`). Toron 61.387 s against DuckDB
15.579 s, 3.940×. Toron is 30.229 s above 2× this board's DuckDB total,
and 30.021 s above the fixed 31.366 s threshold. Every digest equals
ADR 608's, and every cell is stable. No cross-board attribution.
Successor to ADR 612. Main has changed since ADR
608 sealed the current-tree board (Toron 62.706 s against DuckDB
15.600 s, 4.020×):

| ADR | change | status |
|---|---|---|
| 609 | borrowed multi-key group lookup | retained: q01 −713.4 ms paired |
| 610 | two-phase grouped fold with typed kernels | retained: q01 −754.9 ms paired; q13 +122.4 ms, a regression pending |
| 611 | an eligibility rule | reverted to `AllTyped`, so behaviour is ADR 610's |
| 612 | a diagnostic | no code change |

Only q01, q09, and q13 have paired evidence for the changes since ADR
608. ADR 610's gate named 17
cells folding two-phase, and ADR 609's named six on the borrowed path.
Most of those carry the changes unmeasured. This record re-seals the
full 22-cell board on main's current tree, at the anchor coordinate, to
establish:
- the campaign's current total and ratio;
- a current-tree baseline, with a sealed manifest, for the next paired
  records.

It is a measurement, not a cut. It makes no retain or revert decision,
and it attributes no cross-board change.


## Decision

The procedure is ADR 608's, verbatim, except for this record's paths.

*The measured tree.* Main at this plan's commit. Its code equals
`artifact/adr611-control` (`1dddc7fe`); the trees differ only in
`plans/`. The ladder (Toron and toron-tpch, `--no-fail-fast`) runs on
that tree on bigfoot. Then one release executable is built under recipe
`9af51cbc…` with DuckDB `c478c0f4…`, recorded in
`target/tpch/evidence/build/adr613-measured.manifest`, and staged and
hash-verified on strix.

*The run.* One step, `perf --engines toron,duckdb --query all --runs 5
--warmup 1`, at the ADR 603 and 608 coordinate:
- SF8 strict parquet;
- task memory and spill reserve 2 GiB each;
- partition parallelism 1, DuckDB threads 1;
- affinity `0-15`;
- no DuckDB memory limit.

It uses ADR 595's separated launch, with the read-out's after-snapshot
taken only after the completion datagram. Host scripts are generated
from ADR 608's, with this record's paths, and grep-checked for other
ADR numbers.

*Sealing.* On `candidate_authority` true: `seal-evidence` with
`--remote seal`. The evidence tag is fetched to the workstation and
pushed with the closing commit. On a refusal, the artifact and its
reasons are retained unsealed. There is one attempt.

*The report*, as in ADR 608:
- **Per cell:** Toron and DuckDB medians, the ratio, the gap, and each
  engine's change from both ADR 608 and ADR 603.
- **Totals:** the ratio, and the target arithmetic named twice:
  - the distance to 2× this board's DuckDB total;
  - separately, the distance to the fixed 31.366 s threshold.
- **Ranking and stability:** the gap ranking, and per-cell stability
  under the gate.
- **Peak RSS and spill:** peak RSS with its parent high-water and its
  `above_parent_high_water` or `ambiguous` label (an ambiguous reading
  is not stated as a peak), and spill per Toron cell.
- **DuckDB's change:** its per-cell change reported as drift, for
  context only.


## Acceptance

- The ladder green on the measured tree. The manifest staged and
  verified.
- The artifact, with `candidate_authority` and its reasons. If sealed:
  the sealed manifest's hash, the evidence tag, and the archived
  executables.
- The report above.
- Plan commit, then one implementation commit with the results and the
  closure. No code change.


## Guardrails

- **No attribution.** Per-cell changes from ADR 608 are not credited to
  ADR 609 or ADR 610, or apportioned between them. The paired records
  (q01, q09, q13) are the only token evidence. Cells that changed
  without a pair are reported as observed, with no causal claim. That
  includes the 17 cells now folding two-phase.
- ADR 603 remains the historical anchor. This board becomes the
  current-tree baseline if sealed.
- One attempt, no relaunch.


## Risks

- **Cells that got slower.** A cell carrying the unmeasured two-phase
  or borrowed path may be slower than on ADR 608. The board reports it
  as observed. Only a paired record can attribute it.
- **Drift.** Host drift is shown by the DuckDB column, as context.


## Forward

- The next owner is chosen by absolute gap on this board.
- q13's open regression, and any cell observed slower here, are
  candidates for paired records. So is a follow-up to ADR 612 with
  demonstrated stage attribution.
