# ADR 282 — binder-aware correlated scalar subquery resolution (q17 unqualified outer ref)

## Status
Proposed.

## Context
The gauge has seven reach cells dev+strict green and three correctness cells (q01/q03/q06). ADR 281 measured
q17 at `fail(unrecognized@bind)` with the raw wall `UnknownColumn { column: "p_partkey" }`. This is the **engine
cut that wall names**: the scalar-subquery analogue of ADR 279's unqualified correlated-EXISTS handoff. The
decorrelation arc stands at: correlated EXISTS ✓ (pre-256, plus 279 for unqualified refs), correlated IN ✓
(259), uncorrelated scalar ✓ (257), **bare** correlated aggregate scalar ✓ (260). The gap q17 forces: an
**unqualified** outer ref inside a correlated scalar subquery, with the aggregate **wrapped** in arithmetic
(`0.2 * AVG(...)`). A real Toron engine cut (full `-p toron` gate), not a harness cut.

## The measured wall + mechanism (two ordered walls)
The scalar pull-up is `sql_planner::pull_up_scalar_subqueries`. q17 fails in two ordered, qualifier-driven
places (the same class 279 fixed for EXISTS):

1. **Unqualified outer ref not recognized (the measured wall).** Correlation detection
   (`sql_expr_references_outer_qualifier`) matches only a qualified `CompoundIdentifier` (`part.p_partkey`).
   q17's bare `p_partkey` reads as not-correlated → the subquery takes the **uncorrelated** branch
   (`lower_query_scoped`) → `WHERE l_partkey = p_partkey` is lowered in the inner `lineitem` scope → the binder
   correctly errors `UnknownColumn { column: "p_partkey" }`.
2. **Projection is not a bare aggregate (strictly behind wall 1).** Once correlation is recognized,
   `scalar_subquery_bare_aggregate` accepts `AVG(l_quantity)` but rejects `0.2 * AVG(l_quantity)` (a `BinaryOp`).
   Only reached after the correlated path is taken; must be **re-measured** mid-implementation.

The SQL planner is catalog-free, so (like 279) it cannot resolve who owns the bare `p_partkey` — resolution
must happen in the binder.

## Decision
Make correlated scalar subquery resolution **binder-aware** for the unqualified case, reusing ADR 279's
ownership machinery and ADR 260's decorrelation construction. Five load-bearing parts:

1. **Frontend defers only the unclassifiable case.** The planner stays catalog-free. Qualified correlated
   scalar aggregates ADR 260 can classify keep their grouped-aggregate + `LeftOuter` lowering; uncorrelated
   global aggregates keep ADR 257's keyless scalar `Cross`. When a correlated-shaped scalar WHERE carries
   unqualified refs the frontend cannot safely classify, lower a **transient deferred scalar handoff** instead
   of guessing.
2. **Binder resolves by ownership.** Classify each handoff predicate with the ADR 279
   `column_side`/`RelationScope` machinery under the safe-resolution invariant — a correlated equality becomes
   a key pair only when one side is uniquely outer-owned and the other is uniquely inner-owned; inner-only
   predicates stay inner residuals; both/neither, outer-only, or non-equi correlations **reject** (no
   name-shape guessing). Then build ADR 260's existing shape: a grouped aggregate keyed on the inner correlation
   keys, recombined via a hash-partitioned `LeftOuter` exposing `__scalar_N.value`.
3. **The handoff preserves the scalar value expression** (the council's wall-2 resolution — no bespoke
   evaluator). Carry the lowered aggregate call **plus** the projection's value expression with the aggregate
   replaced by an internal placeholder. The binder builds `Aggregate(group by inner keys, aggr = raw
   AVG(l_quantity)) → Project(keys…, 0.2 * raw_avg AS value)`, keeping the wrapper out of the aggregate operator
   and on existing expression/binder/executor paths. So wall 2 folds generically rather than special-casing
   `0.2 * AVG`.
4. **Scratch trace for probe binds.** The scope-probe binds used only for classification run on a throwaway
   `OptimizerTrace` / render-trace; only the final bind records optimizer decisions. (Directly the ADR 279
   review's medium bug — copy the fix.)
5. **Guard reconciliation, explicit.** Retarget `adr260_correlated_scalar_is_frontend_only` and
   `adr257_scalar_subquery_pull_up_is_frontend_lowering_only`: the frontend-only invariant remains true for the
   already-classifiable 257/260 paths; unqualified correlated scalars now have a transient IR/binder handoff.

## Watch-items (operator) → guards
- **Trace cleanliness** — scratch trace/render for probe binds (the 279 footgun); a test pins trace stability.
- **Guard retarget** — the two named frontend-only guards reconciled, not deleted; run the FULL
  `architecture_test` AFTER fmt.
- **LogicalPlan variant blast radius** — if the handoff is a new variant, budget the full checklist: exhaustive
  match arms (audit/exec/binder/set-op/join-order + tests), the ~5 variant-count guards, the `adr176` binder-root
  cap, and the `--lib`/`--all-targets` phasing (`check` alone misses `cfg(test)`/integration matches).
- **Do not allowlist `UnknownColumn`** — ADR 281 correctly left q17 `fail(unrecognized@bind)`; the engine fix
  *removes* that generic error, it does not teach the harness to call it blocked.
- **Official q17 stays unqualified** — a qualified `part.p_partkey` form is a useful control test, not a
  replacement for the official TPC-H shape.

## Acceptance (tiered + measured — do not promise green before Phase 0)
- **Required:** q17 moves past `UnknownColumn { column: "p_partkey" }` in both modes (the unqualified outer ref
  resolves).
- **Target:** with the value-preserving handoff, q17 reaches `pass@execute` (the wrapped `0.2 * AVG` folds via
  the `Project`).
- **Defer only a precisely-named third wall:** if a strict-mode Decimal typing wall on `0.2`/`7.0` surfaces
  after the wrapper clears, name it from the measured row and hand it to ADR 283 rather than bloat this cut.
- **Engine tests:** an unqualified **bare-aggregate** correlated scalar resolves to the same plan/digest as its
  qualified bare-aggregate control (fails under the old qualifier-driven path — the discriminating test); a
  q17-style wrapped unqualified scalar exercises the value-preserving handoff; an ambiguous unqualified ref
  rejects; trace stability holds. The q17 harness row flips from `fail(unrecognized@bind)` to its measured
  outcome.
- **Gate:** full `-p toron` (fmt → check --all-targets → lib → full architecture_test after fmt → --no-fail-fast)
  + `-p toron-tpch` (the seven reach + three correctness cells unchanged).

## Forward arc
q17 reaching execute opens the correlated-scalar family: q02 (adds `partsupp`) and q22 (substring + correlated
`> AVG`) become natural reach probes. A precisely-named wrapper/Decimal wall → ADR 283. q05 correctness stays
available on the rail, independent of this engine arc.

## Why toron-home
The binder/frontend correlated-scalar resolution + the `toron-tpch` q17 row it flips. No cross-ring surface.
