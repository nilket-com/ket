# ADR 098: Top-Level-AND Decomposition for Static Elision

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — verified against the final
drafts: identical design (atom/wrapper split; top-level/nested `AND` only; **unify 095 + 097** proofs
per conjunct; the **eager-error fence** with the safe-sibling allowlist; 097 receipt gates preserved;
095's no-recursive-`AND` guard retargeted). **Predecessors:** 094 (`collect_top_level_bound_and`
decomposition), 095 (`EmptyRelation` + closed-fold/structural-null), 097 (refinement contradiction).
**Toron-only, binder-local.** Makes the 095/097 elision substrate fire on the universal compound-`WHERE`
case it currently misses.

## Doctrine — proof combiner, not short-circuit evaluation

`A AND B` is never-TRUE if `A` is never-TRUE (3VL), so the *row set* of eliding is correct. But Toron
evaluates conjuncts **eagerly** — `id = -5 AND CAST('x' AS INT) = 0` errors today — so eliding because
`id = -5` is `AlwaysReject` would **skip `B` and mask its error**, the exact violation 095's fence
forbids. Decomposition is a *proof combiner*: declare the filter empty only when one conjunct is
statically never-TRUE **and every other conjunct cannot raise**.

## Grounding

- `static_filter_class` (`binder.rs:1181`) runs whole-root proofs only — `structural_non_nullable_is_null`
  (`single_root()`), `refinement_static_filter_class` (`stored_scan_refinement_atom`, `single_root`),
  `closed_root_filter_class` (whole-program eval). So `1 = 0 AND active` and `id = -5 AND active` do
  **not** elide.
- 094's `collect_top_level_bound_and` (`:2170`, recurses nested `AND`) + `isolate_bound_subgraph` /
  `combine_bound_conjuncts` isolate a conjunct into a standalone single-root sub-program — no new IR.
- 095's error fence is pinned by `where_closed_erroring_expression_does_not_elide_and_still_errors`.

## Decisions (promotion-locked)

**D1 — Atom/wrapper split (reuse, don't rewrite).** Extract `static_filter_atom_class(input,
&conjunct_program, logical_types)` = the existing whole-root proof order (structural non-null `IS NULL`
→ 097 durable refinement contradiction → closed column-free eval), **internals unchanged**.
`static_filter_class` becomes the wrapper: `AND` root → `collect_top_level_bound_and` →
`isolate_bound_subgraph` each conjunct → run the atom helper on each. A non-`AND` root is the
1-conjunct case (today's behavior, byte-identical). **D2 — Top-level/nested `AND` only.** Flatten nested
`AND`; do **not** enter `OR`/`NOT`/`IN`/`LIKE`/casts/arithmetic/functions as boolean algebra. **D3 —
Unify 095 + 097 per conjunct.** A conjunct rejects via any existing proof: closed column-free eval to
`FALSE`/`NULL` *without evaluator error*, structural `non_nullable_col IS NULL`, or 097 durable
stored-scan refinement contradiction. No new proof source; no `AlwaysPass`/filter-removal. **D4 —
Error-free siblings (the fence).** `AlwaysReject` iff (∃ conjunct `AlwaysReject`) ∧ (**every
non-rejecting sibling is proven error-free**); else `Unknown`. **D5 — Safe-sibling allowlist.** A
closed (column-free) sibling is safe iff the closed-root classifier returns `AlwaysPass` or
`AlwaysReject`; `Unknown` (including evaluator `Err`) is unsafe. An open sibling is safe iff its
subgraph is only: direct column, literal (including NULL), comparison over columns/literals, `IS NULL`
/ `IS NOT NULL`, nested `AND` of those. **Excluded → unsafe →
`Unknown`:** `Cast`/`TryCast`, arithmetic, division, functions, `InList`, `Like`, `OR`, `NOT`, any
unknown node. **D6 — Preserve 097 receipt gates.** A refinement rejecting conjunct still proves only
through the durable registry and a direct stored scan column on `ExecutionPlan::Scan { projection: None }`
— no pass-through projection, scan-projection remap, sidecar-only receipt, seed-only lookup, or
CAST-ascription receipt. **D7 — Bounded.** Explicit conjunct/node budget (reuse 094's); over-budget/
malformed → `Unknown`. Not CNF/DNF/SMT/cross-column constant folding/residual rewrite. **D8 — Stays
before PBC.** `plan_filter_with_predicate_pushdown` still calls the classifier before pushdown; on
`Unknown` the existing PBC partitioner + residual path run unchanged. **D9 — No surface change.** No
SQL grammar / logical-IR / execution-IR / executor / PBC / storage / Wendelon / wire / client / Rune.

## Behavior gate

`refinement_and_safe_conjunct_elides_to_empty_relation` (`id = -5 AND active`, zero scan);
`refinement_nested_and_safe_conjunct_elides` (`A AND (B AND id = -5)`);
**`static_false_and_safe_sibling_elides`** (`1 = 0 AND active` — the 095-unify proof);
`non_nullable_is_null_and_safe_sibling_elides`;
`refinement_and_overlapping_conjunct_does_not_elide` (`id = 5 AND active`);
**`refinement_and_strict_cast_sibling_does_not_elide`** (`id = -5 AND CAST('x' AS INT) = 0` → assert
**no** `EmptyRelation`); **`static_null_and_strict_cast_sibling_does_not_elide_and_still_errors`** (the
case whose non-elided residual path errors today — assert the error is preserved);
`refinement_or_not_root_does_not_elide`;
`and_with_unrefined_or_non_scan_conjunct_does_not_elide` (097 provenance inherited per conjunct);
`count_star_over_and_elided_filter_returns_single_zero` (095 cardinality fence). *(Testing nuance:
assert plan-shape `no-EmptyRelation` as primary; assert runtime error only where the non-elided plan
actually errors. `FALSE AND strict_cast` can be suppressed by the existing PBC false path, so the
error-preservation fixture uses `NULL AND strict_cast`.)*

## Architecture guards

**Retarget (discipline #4):** 095's no-`BoundBinaryOp::And` classifier guard → **"`AND` appears only in
the bounded static-elision decomposition wrapper"** (never in `closed_root_filter_class`/recursive
folding); 097's whole-root `stored_scan_refinement_atom` guard stays valid (the wrapper feeds it
isolated single-root conjuncts). **Add:** `adr098_and_decomposition_runs_unified_atom_classifier` (095
closed-fold/null **and** 097 refinement apply per conjunct, one shared helper);
`adr098_and_elision_requires_error_free_siblings` (the allowlist; `Cast`/`TryCast`/arith/`InList`/`Like`/
`OR`/`NOT` excluded); `adr098_and_decomposition_inherits_097_durable_registry_and_scan_provenance`;
`adr098_and_elision_reuses_095_empty_relation_no_alwayspass`; `adr098_no_boolean_algebra_beyond_and`;
`adr098_no_wendelon_pbc_executor_storage_or_wire_change`.

## Scale boundary & seam check

Binder-local; reuses 094's `collect_top_level_bound_and`/`isolate_bound_subgraph` + the 095/097
classifiers + 095's `EmptyRelation`. New machinery is only the wrapper + the bounded error-free walk.
No new IR/exec/wire/PBC/Wendelon surface, no scale axis, no load-bearing seam.

## What to do next

- **Pass-through-projection provenance** (097's other deferral) — the strong separate follow-on; needs
  its own lineage contract for `Project` / `Scan { projection: Some(..) }` ordinal remap. Don't bundle
  with proof composition.
- Carried: `information_schema.logical_types`; `DROP`/`ALTER LOGICAL TYPE`; Rune `Like`/`ILike`; PK/FK;
  the Wendelon `*V1` de-versioning sweep; Wendelon authority-side validation.

## Council history

095 substrate → 096 receipt → 097 payoff; 098 widens the payoff to compound `WHERE`. iter-0: 3-of-3 on
AND-decomposition; **Claude grounded the unify-095+097 scope + the eager-error fence**; **Codex
independently grounded the fence** + the atom/wrapper split + the 095-guard retarget; Gemini ratified.
iter-1: unification accepted 3-of-3. iter-2 (dual): consolidated; folded the closed-sibling-eval safety,
the guard retarget, and the plan-shape testing nuance. iter-3/final: verified consensus and explicitly
pinned `NOT` as unsafe. Migration:
`plans/impl/098_and_decomposition_static_elision.md`.

## Promotion criteria

`id = -5 AND active` / `1 = 0 AND active` / nested-`AND` elide to `EmptyRelation` (zero scan);
overlapping conjunct / `OR`/`NOT` root / unrefined or non-scan conjunct / **strict-cast sibling** do
**not** elide (the cast-sibling stays a Filter; error preservation is pinned where the existing
non-elided path evaluates it); the wrapper feeds isolated conjuncts to one shared atom classifier
(095 + 097); 095's no-recursive-`AND` guard
retargeted in-change; 095 cardinality + error fences + 097 provenance hold; no surface change.
Migration: `plans/impl/098_and_decomposition_static_elision.md`.
