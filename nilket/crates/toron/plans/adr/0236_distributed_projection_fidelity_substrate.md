# Toron ADR 236: Distributed Projection-Fidelity Substrate

**Status:** Promoted. **Migration:** `plans/impl/0236_distributed_projection_fidelity_substrate.md`.
**Category:** Toron distributed **test substrate** — a hardening cut. Codify the invariant that
process-backed distributed fixtures honor the scan-request fields the fragment under test depends on
(projection now), and formalize the cross-producer cardinal discipline, so a passing distributed
cardinal *proves the intended fragment ran*. **Test-only: no production storage/wire/planner/Wendelon
design change unless the work surfaces a real bug. No new SQL surface.**

## Why this cut exists

The ADR 235 review surfaced a distributed-test fidelity gap: `toron_fixture_worker` did not honor
`ScanRequest.projected_columns`, so a set-op test selecting column 1 received column 0 until
projection handling was added. That is not a semantic bug — it is a substrate bug that lets
distributed tests **pass for the wrong reason**. Every follow-on distributed cardinal (set-op
multiplicity, window functions) depends on positional correctness through the scan-share → exchange →
union → join → aggregate → distinct paths. Before adding the next feature, make the substrate honest
("eat the vegetables", per Codex).

## Grounding (verified)

- **Projection is honored post-235, unevenly elsewhere.** The process worker
  (`toron_fixture_worker.rs:139`) honors `projected_columns` + `morsel`; the in-process `FixtureScan`
  (group_by_test) honors `projected_columns`. Several test scan mocks honor nothing — some
  legitimately (no projection pushed). 236 turns "honors it" into a guarded invariant, not an
  accident.
- **Cross-producer cardinals rest on a weak proxy.** Every distributed cardinal
  (`select_distinct_join_dedups_across_partitions`, `group_by_join_real_aggregate_merges...`,
  `count_distinct_over_join...`, `union_distinct_dedups...`, `intersect_null_safe_matching...`) asserts
  `unique_selected_worker_count == 2` — which only proves two workers ran *somewhere*, not that the
  group/row under test was produced by >1 producer. Correctness rests entirely on hand-designed
  fixtures; there is no durable rule.
- **Predicate is the live sibling gap.** The process worker honors projection + morsel but **drops
  `pbc_predicate`** (its `scan()` never reads it). It is the next member of the same confounder
  family — named here, scoped to the next ADR (Decision D6).

## Decisions (promotion-locked)

**D1 — Codify the projection-fidelity invariant + guard it.** The durable rule: *a process-backed
distributed fixture must apply `ScanRequest.projected_columns` (before morsel slicing) before tests
use it as evidence.* An architecture guard pins the worker's projection handling, behavioral-leaning
(observe the projected result; don't overfit fixture text). The handling itself already landed in
235; 236 makes it a regression-pinned invariant rather than an accident.

**D2 — Distributed projection regression.** A process query selecting a **non-zero** projected column
through the scan-share path, whose expected rows are wrong if the ordinal projection is ignored (the
exact 235 symptom, now a pin).

**D3 — Nullable projected columns stay nullable.** A nullable source column remains nullable after
projection through the distributed path (the 235 `set_left`/`set_right` fixtures exercise this).

**D4 — Cross-producer cardinal rule (durable discipline).** A distributed cardinal that claims
cross-producer behavior must satisfy:
- its **expected value is unreachable from a single producer / a non-merging consumer** (the result
  itself discriminates — e.g. `intersect → [None, 9]` requires values contributed across fixture
  producers and merged by the distributed plan);
- `unique_selected_worker_count > 1` is **supporting evidence, not the proof**;
- the fixture places at least one discriminating key/row-group on **>1 producer** (a key on ≥2
  partitions).
Documented as a checklist/helper beside the distributed tests; existing cardinals should be annotated
against it rather than relying on worker count alone.

**D5 — Test-only.** Guards, regressions, and the discipline doc. No production storage/wire/planner
design change, no Wendelon change, unless the fidelity work uncovers an actual production bug. The
guardable invariant is "no new feature surface" (no SQL/IR/operator expansion), not a brittle
commit-diff claim about which files were touched.

**D6 — Predicate-fidelity is the named sibling, not folded.** `pbc_predicate` fidelity is real and
grounded, but folding it here would turn a narrow substrate fix into a broad scan-contract audit. It
is the immediate follow-on ADR (Forward arc).

**D7 — Set-op `ALL` multiplicity is explicitly out of scope.** No `INTERSECT ALL`/`EXCEPT ALL`/
`MINUS ALL`, no `Repeat` operator, no multiplicity kernel in 236 (deferred — see Consensus).

## Honesty gates (discriminating)

- **`distributed_projection_fidelity_selects_nonzero_column`** — a process query projecting a
  non-zero column returns that column's values, not column 0 (fails if the worker ignores
  `projected_columns`). *Cardinal.*
- `distributed_projection_fidelity_preserves_nullable_column` — nullable projected column stays
  nullable / preserves NULLs across the shuffle.
- `distributed_cross_producer_cardinal_is_value_discriminating` — uses the D4 checklist/helper to
  pin that a representative cardinal's expected value requires data from >1 producer (the result,
  not just the worker count).
- `adr236_fixture_worker_honors_projected_columns` (guard) — the worker's `scan()` applies
  `projected_columns` before returning the morsel.

## Architecture guards (promotion-locked)

- `adr236_fixture_worker_honors_projected_columns` (behavioral-leaning)
- `adr236_distributed_projection_fidelity_tests_exist`
- `adr236_cross_producer_cardinal_rule_documented`
- `adr236_no_new_sql_ir_or_operator_surface`

**Reconciled prior guards (named):** none inverted — this cut adds guards, it does not change an
existing invariant. The 235 distributed set-op guards / `fragment_union_plan` guard are reused
unchanged.

## Consensus (transparent)

2-of-3 (Codex + Claude), plus the operator's explicit steer ("testing for this next ADR before
coming back to set operations multiplicity"). Codex iter-0/2 proposed and scoped the
projection-fidelity substrate; Claude iter-1 grounded the fidelity landscape, elevated the
cross-producer cardinal rule to the durable artifact, and named the predicate sibling — Codex
converged on both. **Gemini iter-1 diverged**, proposing a *dual-purpose* cut that folds in set-op
`ALL` multiplicity + an `ExecutionPlan::Repeat` primitive. That is the feature the operator deferred;
resolved against here — a testing-infrastructure repair should not also carry a SQL feature. Gemini's
desugar-to-aggregate-join-`Repeat` design is **not discarded** — it is the grounded seed for the
set-op `ALL` multiplicity ADR that follows the predicate-fidelity sibling. Gemini's fixture-fidelity
half converges with this cut.

## Forward arc

```text
236 distributed projection-fidelity substrate (this)
-> distributed predicate-fidelity substrate (pbc_predicate — the named sibling)
-> set-op ALL multiplicity (Gemini's aggregate-join + ExecutionPlan::Repeat design)
-> multi-dedup distinct aggregates (234 deferred) -> window functions
```

## Deferred (named)

Predicate (`pbc_predicate`) fidelity (immediate sibling); `runtime_filter` / `row_identity` fixture
fidelity; set-op `ALL` multiplicity + the `Repeat` operator; multi-dedup distinct aggregates; window
functions. A stronger-than-worker-count partition-level "produced-by-N-producers" assertion (instead
of the documented discipline) is a possible future enhancement.
