# ADR 527 - The 1/2/4/8-thread scaling atlas (attribution only)

The single-thread board is closed at `53f18ebc` with its 3,554 ms floor frozen. The next
doctrinal question — whether morsel-driven scheduling would ever be evidence-backed — has a
predeclared prerequisite from the standing architecture doctrine: a parallel scaling board
must exist before any scheduler is judged. This ADR builds that atlas. It is **attribution
only**: no morsel implementation, no executor change, no scheduling remedy. Its deliverable
is measurements and a verdict per cell under a clean decision tree.

## Contract

1. **Tree and cells**: the current default-on tree; all of q01-q22.
2. **Engines and thread counts**: Toron and DuckDB at explicitly matched thread counts —
   1, 2, 4, and 8 — Toron via a pinned `TORON_PARTITION_PARALLELISM`, DuckDB via
   `TORON_TPCH_DUCKDB_THREADS`, both persisted in every row (the pin-persistence fix covers
   DuckDB; the atlas extends row provenance to the Toron pin).
3. **Fixed aggregate memory**: total query memory stays 2 GiB at every thread count — the
   per-task budget is 2 GiB divided by the thread count, explicitly, so parallelism never
   silently multiplies available memory; spill reservation follows the same rule; both
   provenances recorded per row.
4. **Identity facts persisted per row**: thread pins, CPU affinity/topology of the run,
   binary identity, dataset identity (format-2 full-hash), peak RSS, and spill bytes.
5. **Step-zero honesty fact**: the closed board's rows never recorded Toron's effective
   parallelism (`TORON_PARTITION_PARALLELISM` defaults to machine parallelism). The atlas's
   first measurement pins what the historical coordinate actually ran with, and the 1-thread
   arm either reproduces it or the difference is stated — the era's "one-task" language is
   verified, not assumed.
6. **Per-cell statistics**: medians and spread from 5 runs + 1 warmup per (cell, engine,
   thread count); speedup and parallel efficiency versus the same engine's 1-thread arm.
   **No architectural credit from summed board time** — per-cell verdicts only.
7. **Partition-level attribution**: per-partition work and completion times, exchange
   volumes, and the max/median straggler ratio per cell at each thread count, from existing
   telemetry where it exists; where partition completion is not observable today, a
   batch-scoped, reconciliation-gated recorder is added under the ADR 516 perturbation
   discipline (5 interleaved off/on pairs, absolute 3% median, rejected-not-corrected) —
   the 524/525 lesson applied from the start.
8. **Plan identity across thread counts**: fingerprints recorded per arm; if plans change
   with thread count, the cell is flagged **optimizer-variant** and excluded from execution
   verdicts until the optimizer behavior is isolated — thread-dependent planning is its own
   finding, not an execution fact.

## The decision tree (predeclared)

Per cell, at 8 threads unless stated:

- **Good scaling** (efficiency above the predeclared band): morsels remain unnecessary for
  that cell.
- **Poor scaling + uneven partition completion** (straggler ratio above its band): morsel
  scheduling becomes evidence-backed for that cell class.
- **Poor scaling + balanced partitions**: the residual is per-tuple efficiency or shared
  contention, not scheduling — morsels would not help; named accordingly.
- **Thread-dependent plan changes**: optimizer isolation first; no execution verdict.

The bands (efficiency threshold, straggler threshold) are predeclared in the impl plan before
any measurement, from the 1-thread arm's noise characteristics — not fitted afterward.

## Acceptance

1. The atlas artifact: per-cell tables (median, spread, speedup, efficiency, straggler
   ratio, RSS, spill, plan fingerprint) at all four thread counts, both engines, with every
   identity fact persisted in the rows.
2. Any new recorder passes the perturbation gate on q09 and q08 at 1 and 8 threads before
   its facts are trusted; two strikes close the recorder red and the atlas ships with the
   observable subset, gaps named.
3. Digest identity across all arms and thread counts per cell (results must not vary with
   parallelism); any variance is a correctness finding that halts the atlas for that cell.
4. A verdict row per cell from the decision tree, plus the machine topology record.
5. No remedy, no knob beyond pins and the recorder env, no default change anywhere.

## Guardrails

- The frozen 3,554 ms floor is not consulted — this atlas measures scaling, not materiality;
  any future remedy ADR born from its verdicts declares its own economics.
- The single-thread board's receipts stay canonical for the closed epoch; atlas rows live in
  their own artifact namespace.
- Reference-engine rows remain timing/capability evidence (digest="none" class), as the
  board snapshot's evidence-limits doctrine established.

## Risks

- Memory-per-task division may shift spill behavior at higher thread counts; that is a
  finding (the atlas records it), not a defect — the fixed-aggregate rule is what makes
  thread counts comparable.
- Partition-completion observability may be thinner than expected; the recorder is the
  bounded fix and its perturbation gate bounds the spend.

## Forward

- The atlas verdicts feed the standing doctrine ledger (morsel gate, per-thread ratio band).
- If a cell class lands in "poor scaling + uneven completion", a successor scheduling ADR
  may cite the atlas; if everything lands in "balanced but inefficient", the per-tuple
  efficiency track continues under the existing bounded-rule regime at whatever floor a new
  epoch declares.
