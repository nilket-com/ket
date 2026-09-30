# ADR 614 - stage breakdown of the four largest gaps

**Status:** CLOSED (results in
`plans/impl/0614_stage_breakdown_of_the_four_largest_gaps_results.md`).
All 20 runs passed their gates, and every profile reconciles at its
outer boundary. The stages are top-level reconciled for q21, q09, and
q05; q09 and q05 are strict-incomplete, missing nested sub-stages.
- **Coverage:** top-level stage coverage was 98.5% for q21, 97.9% for
  q09, and 99.4% for q05. q18 has none: it has no stage vocabulary.
- **Owner:** join preparation (`runtime_prep`) is the largest recurring
  observed owner, 9.2 s across q05, q21, and q09. It is led by q05's
  replay-1 `prep_build` at 3.8 s.

No cut. Successor to ADR 613. **A diagnostic record: no
token, no pair, no cut.**

On the current-tree board (ADR 613, sealed: Toron 61.387 s against
DuckDB 15.579 s, 3.940×), four cells hold about 19.8 s of the 45.8 s
total gap:

| cell | Toron ms | DuckDB ms | gap ms |
|---|---|---|---|
| q21 | 7,972.7 | 2,205.0 | 5,768 |
| q09 | 6,978.4 | 1,629.0 | 5,349 |
| q18 | 5,976.7 | 1,479.0 | 4,498 |
| q05 | 4,752.0 | 591.0 | 4,161 |

This record measures where each cell's Toron time goes, by exact stage
durations, to choose the next owner. It projects no saving.


## Context

**What exists.** Node attribution (`TORON_NODE_ATTRIBUTION_PROFILE_PATH`)
records three things:
- per plan-node kind: exclusive time, calls, and spill;
- per aggregate invocation: its route and, where the route's handler
  has a stage vocabulary, reconciled stage durations;
- whether each invocation reconciles.

On ADR 604's pre-609 executable, three of the four cells' dominant
invocations reconciled, and the fourth did not:
- **q21:** `inner_prefix_semi_anti_grouped`, including `runtime_prep`
  3.68 s and `inner_replay_1` 2.12 s.
- **q09:** `reoriented_inner_chain_grouped`, including `runtime_prep`
  1.46 s, `repartition` 0.72 s, and `scan_pull` 0.67 s.
- **q05:** `reoriented_inner_chain_grouped`, including `prep_build`
  2.28 s and `runtime_prep` 2.29 s.
- **q18's** invocations had no stages: `scan_grouped` reported
  `stage_attribution_missing`, and an `uncovered_generic_fold` had no
  handler span. Its time sat at node level: aggregate 3.65 s, exchange
  1.24 s, and join 0.93 s.

Those figures predate ADRs 606 and 609–611. This record re-reads them on
the current tree.

**Why node attribution, not `perf`.** ADR 612 showed that whole-process
samples cannot be assigned to a stage without window or call-stack
selection. Node attribution's stage durations are measured inside each
stage: they are exact spans, not samples. This record uses them only,
and states where they do not reach.


## Decision

*The executable.* ADR 613's measured release executable
(`9a84d5c4…`, manifest `adr613-measured.manifest`), unchanged, run
through `toron-bench run` so that peak RSS is recorded.

*The runs.* On strix, one at a time, with the host quiet, at the ADR
613 coordinate. **Five runs of each of q21, q09, q18, and q05** with
node attribution. Each run is gated:
- its digest and plan fingerprint equal ADR 613's sealed cell;
- the status is pass;
- the spill scratch is empty after the run.

A run that fails is excluded and named.

*Per run, first.* Each run is reconciled on its own before anything is
summarized:
- the node exclusives sum to that run's execution within the profile's
  outer boundary;
- each reconciled invocation's stages sum to its handler time within
  the stage boundary.

**Coverage** is computed per run from disjoint spans only. *Clarified
before the read-out:* the numerator is only the top-level stage spans of
reconciled invocations. The uncovered remainder is the sum of:
- node kinds outside the aggregate invocations;
- unreconciled invocations' exclusive time;
- each reconciled invocation's time not in a top-level stage;
- the outer residual (execution minus node exclusives).

Per run, the numerator plus the uncovered remainder must equal
execution. The identity gap is reported.

The DuckDB panel's D1 result hash is also checked, read-only, against
ADR 613's sealed DuckDB cell (`duckdb_result_sha256` of its samples).
On a mismatch, the panel is labelled historical and unmatched.

Invocations are exclusive of their children in node attribution, so
they do not overlap. If a run shows an invocation
nested inside another's reconciled span, or any other overlap, then:
- that run's combined coverage percentage is not computed;
- per-invocation coverage is reported instead.

*The report, per cell, as the median and range over the five
per-run-reconciled values:*
- **The observed difference:** `execute_ms` median and range beside ADR
  613's board median. This is an observed difference between the
  diagnostic runs and the board. It may include attribution overhead
  and host drift, and it is not apportioned between them.
- **Node kinds:** the node-kind table (exclusive, calls, spill).
- **Invocations:** each aggregate invocation's route and reconciliation
  status. For a reconciled invocation, its ordered stages. A repeated
  stage name (for example `prep_build` once per chain stage) is compared
  across runs only when the five runs match:
  - the invocation topology: count, routes, and invocation order;
  - the full stage-name sequence.

  Where they match, stages are summarized by ordinal. Where they do
  not, the unmatched stages are marked as such and not summarized by
  position.
- **Coverage:** the per-run coverage's median and range. The remainder
  is named by node kind or by unreconciled invocation, as
  **uncovered**, and is not decomposed further. q18's missing stage
  vocabulary, if still missing, stays uncovered.
- **DuckDB, as context only:** ADR 604's D1 operator profiles for these
  cells (`DUCKDB_OPERATOR_PROFILE_SETTINGS`: operator timing and
  cardinality), cited from ADR 604's census artifacts
  (`adr604-census/<cell>/structural_census.json`). ADR 604's identity
  table records each cell's D1 digest as a match against the ADR 603
  anchor's DuckDB digest. The DuckDB
  executable (`c478c0f4…`), the dataset, and the settings are unchanged
  since. The panel is labelled with that provenance. It is not a
  like-for-like comparison: the engines' operators and boundaries
  differ. Only per-operator timings, the root total, and cardinalities
  are shown, with no ratio against a Toron stage. No new DuckDB run is
  made. `duckdb-profile-sweep` collects no operator timing, and is not
  used.

*The synthesis.* Across the four cells, a ranking of **observed stage
owners** by median time. Each is labelled with its route and cell, and whether it recurs
across cells. For example, `runtime_prep` appears in three of the
routes. The ranking names candidate owners for the next record. It projects no
recoverable time, chooses no mechanism, and projects no saving. A candidate's
mechanism is established by its own plan, from code and targeted
evidence.


## Acceptance

- Twenty gated runs (5 × 4 cells), or the excluded runs named.
- The per-cell report and the synthesis, as above.
- The DuckDB context per cell, cited from ADR 604 with its provenance,
  labelled non-comparable.
- The per-run reconciliation results, and the topology and stage-sequence
  correspondence verdict per cell.
- Where attribution does not reach (for example q18's missing stage
  vocabulary), the uncovered time named, with its size.
- Plan commit, then one implementation commit carrying the results (and
  any read-out script) and the closure. No production code change. If
  stage vocabulary is missing where it is needed, adding it is a
  separate, planned record.


## Guardrails

- No token, no pair, no cut, no saving projected.
- Stage times are the only per-stage evidence. No `perf` sample is
  assigned to a stage.
- Cross-engine figures are context only.
- Runs are diagnostics. They are not a board and not paired latency.


## Risks

- Attribution may add overhead, and the host may drift. The observed
  difference against ADR 613's board is stated, not apportioned.
- A route's stage vocabulary may lump several operations into one stage
  (for example `runtime_prep`). The record reports the stage as named,
  and a candidate's internals are a later record's question.
- q18's attribution may still be missing on the current tree. Then its
  breakdown is node-level only, and the record says so.
