# Impl Plan — ADR 098: Top-Level-AND Decomposition for Static Elision

**Migration for** `plans/adr/0098_and_decomposition_static_elision.md`. **Toron-only, binder-local.**
Wrap the existing whole-root classifier in an `AND`-decomposition that runs the unified 095/097 atom
proofs per conjunct, gated by the eager-error fence. Reuses 094's `AND` collection + isolation + 095's
`EmptyRelation`. No new IR/exec/wire/PBC/Wendelon surface.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| Classifier (whole-root today) | `static_filter_class(input, predicate, logical_types)` `binder.rs:1181` |
| 095 atom proofs | `structural_non_nullable_is_null` `:1201`; `closed_root_filter_class` `:1225` (whole-program eval, 1-row unit batch) |
| 097 atom proof | `refinement_static_filter_class` `:1280` → `stored_scan_refinement_atom` (`single_root` + `Scan { projection: None }` + durable registry) |
| AND collect + isolate (094) | `collect_top_level_bound_and(program, id, &mut Vec)` `:2170` (recurses nested AND); `isolate_bound_subgraph(source, root) -> Result<BoundExprProgram, BooleanSplitError>` |
| Reuse target | `ExecutionPlan::EmptyRelation` (095) |
| 095 guard to retarget | the architecture guard asserting the classifier has no `BoundBinaryOp::And` |

## Phase 1 — Extract the atom classifier (no behavior change)

Rename the body of `static_filter_class` to `static_filter_atom_class(input: &ExecutionPlan,
predicate: &BoundExprProgram, logical_types: &LogicalTypeRegistry) -> StaticFilterClass` — the existing
095 structural-null + 097 refinement + 095 closed-fold proofs, **internals unchanged** (still operate
on `predicate.single_root()`). This is the per-conjunct proof.

## Phase 2 — The AND-decomposition wrapper

```rust
fn static_filter_class(input, predicate, logical_types) -> StaticFilterClass {
    let Some(root) = predicate.single_root() else { return Unknown; };
    let mut conjuncts = Vec::new();
    collect_top_level_bound_and(predicate, root, &mut conjuncts);   // [root] when not an AND
    if conjuncts.len() == 1 {
        return static_filter_atom_class(input, predicate, logical_types);   // today's path, byte-identical
    }
    if conjuncts.len() > STATIC_ELISION_MAX_AND_CONJUNCTS { return Unknown; } // bounded
    let mut found_reject = false;
    for c in &conjuncts {
        let Ok(sub) = isolate_bound_subgraph(predicate, *c) else { return Unknown; };
        match static_filter_atom_class(input, &sub, logical_types) {
            AlwaysReject => found_reject = true,
            _ => if !conjunct_is_error_free(predicate, *c) { return Unknown; },  // sibling fence
        }
    }
    if found_reject { AlwaysReject } else { Unknown }
}
```

The rejecting conjunct is inherently runtime-safe (097 atoms/structural-null don't evaluate; the
closed-fold returns `Unknown` on `Err`), so only **non-rejecting** conjuncts need the error-free check.
`static_filter_atom_class` gets the **same `input`** (the shared `Scan`) per conjunct, so 097's
provenance/durable-registry gate applies to each conjunct's column unchanged.

Define `STATIC_ELISION_MAX_AND_CONJUNCTS` locally (or reuse an explicitly named 094 cap) rather than
leaving the budget implicit.

## Phase 3 — The error-free check (the soundness fence)

```rust
fn conjunct_is_error_free(program: &BoundExprProgram, node: BoundExprId) -> bool
```
- **Closed (column-free) conjunct:** safe iff the closed-root classifier returns `AlwaysPass` or
  `AlwaysReject` on the isolated program. `Unknown` (including evaluator `Err`) is unsafe.
- **Open conjunct:** bounded structural walk — safe iff every node is `Column` / `Literal` / comparison
  `Binary { Eq|Neq|Lt|Lte|Gt|Gte }` / `Unary { IsNull|IsNotNull }` / `Binary { And }`. **Any**
  `Cast`/`TryCast`/arithmetic (`Add/Sub/Mul/Div`)/function/`InList`/`Like`/`Or`/`Not`/other → unsafe.

## Phase 4 — Behavior fixtures (`predicate_widening_test.rs` + `logical_type_ddl_test.rs`)

`refinement_and_safe_conjunct_elides_to_empty_relation` (`id = -5 AND active`, spy scan 0);
`refinement_nested_and_safe_conjunct_elides`; `static_false_and_safe_sibling_elides` (`1 = 0 AND active`);
`non_nullable_is_null_and_safe_sibling_elides`; `refinement_and_overlapping_conjunct_does_not_elide`;
**`refinement_and_strict_cast_sibling_does_not_elide`** (assert **no** `EmptyRelation`, Filter kept);
**`static_null_and_strict_cast_sibling_does_not_elide_and_still_errors`** (assert runtime error
preserved where the existing non-elided path evaluates the sibling);
`refinement_or_not_root_does_not_elide`; `and_with_unrefined_or_non_scan_conjunct_does_not_elide`;
`count_star_over_and_elided_filter_returns_single_zero`.

## Phase 5 — Architecture guards

**Retarget:** 095's no-`BoundBinaryOp::And` guard → "`AND` allowed only in the decomposition wrapper
(`static_filter_class`), never in `closed_root_filter_class`/recursive folding"; keep 097's whole-root
`stored_scan_refinement_atom` guard (it still receives single-root isolated conjuncts).
**Add:** `adr098_and_decomposition_runs_unified_atom_classifier`;
`adr098_and_elision_requires_error_free_siblings`;
`adr098_and_decomposition_inherits_097_durable_registry_and_scan_provenance`;
`adr098_and_elision_reuses_095_empty_relation_no_alwayspass`; `adr098_no_boolean_algebra_beyond_and`;
`adr098_no_wendelon_pbc_executor_storage_or_wire_change`.

## Phase 6 — Verification

```
cargo test -p toron --test predicate_widening_test --test logical_type_ddl_test   # AND elision + non-elision + fence
cargo test -p toron --test update_test                                            # source-filter AND elision
cargo test -p toron --test architecture_test                                      # adr098_* + retargeted 095 guard
cargo test -p toron                                                               # full suite; 095/097 green, cardinality, no regression
```

**Acceptance:** compound `AND` with a never-TRUE conjunct + error-free siblings elide to
`EmptyRelation` (zero scan); strict-cast sibling / overlapping / `OR`/`NOT` / unrefined / non-scan do
**not** elide (cast-sibling keeps the Filter; error preservation is asserted where the existing
non-elided path evaluates it); the wrapper feeds isolated conjuncts to one shared atom classifier
(095 + 097); 095's no-recursive-`AND` guard
retargeted (not red); 095 cardinality + error fences + 097 provenance hold; full suite green.

## Risks / notes

- **The eager-error fence is the soundness linchpin** — a never-TRUE conjunct must not erase a
  sibling's runtime error. Closed siblings are safe iff the closed-root classifier returns
  `AlwaysPass`/`AlwaysReject`; open siblings iff in the structural allowlist. Pin with the
  strict-cast-sibling fixtures (plan-shape + error-preservation on the `NULL AND strict_cast` path;
  `FALSE AND strict_cast` may already be suppressed by the existing PBC false path after non-elision).
- **Reuse, don't rewrite** — atom classifier internals unchanged (single-root); the wrapper isolates
  conjuncts via 094's helpers; 097's durable-registry + `Scan { projection: None }` provenance inherited
  per conjunct.
- **Bounded** — cap conjunct count (094's budget); over-budget/malformed → `Unknown`.
- **No scope creep** — top-level/nested `AND` only; no `OR`/`NOT`/CNF/commutation (094 owns residual);
  no `AlwaysPass`; no pass-through-projection provenance (separate follow-on).
