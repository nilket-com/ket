# ADR 094: Predicate Pushdown Residual Minimization (factored OR / NOT)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — verified against the iter-3
drafts (Codex adr, Gemini adr+impl): all three carry the identical shape (extraction helpers return
pushed + reconstructed minimal residual; common-OR strip → `R1∨R2`; NOT(OR) → `¬R`; structural-identity
only; bounded with 078 fallback; the existing 3VL property gate auto-validates). Converged after a
cross-over the operator broke (council history below). **Predecessor:** ADR 078 — this is its
explicitly-deferred follow-on. **Toron-only, `binder.rs`-localized.**
**Scope:** `split_boolean_tree` + its extraction/combine helpers in `binder.rs`; tests. **No** IR
variant, SQL syntax, PBC opcode, Wendelon change, wire change, executor change, or dependency.

## Doctrine — push once, don't re-evaluate the pushed term

Once a conjunct is proven necessary and pushed to Wendelon, Toron must not re-evaluate it in the
residual when a 3VL-preserving smaller residual exists. ADR 078 deliberately chose safety
(`residual = original`) for the new factored OR/NOT shapes; 094 is the planned second pass that makes
the residual minimal. The invariant is **exact**, over SQL three-valued (Kleene) logic incl. NULLs:

```text
eval(original) == eval(pushed AND residual)
```

```text
(P ∧ R1) ∨ (P ∧ R2)   push P → 078 residual = (P∧R1)∨(P∧R2)   094 residual = R1 ∨ R2
NOT(P ∨ R)             push ¬P → 078 residual = NOT(P∨R)        094 residual = ¬R
```

Execution model: Wendelon applies `pushed` (PBC), Toron applies `residual` to survivors. Since on
survivors `pushed=TRUE`, `P ∧ original_residual ≡ P ∧ minimal_residual` — identical results, less
Toron work. The win is on Toron's **primary axis** (pushdown — the wire carries results, not bytes).

## Grounding (the facts that fixed the cut)

- **078 deferred this in its own text** — the decisive evidence-pressure provenance:
  `plans/adr/078_*.md:58` "**original** predicate as residual — no residual reconstruction/minimization
  this [ADR]"; `:146` "no residual minimization for the new shapes"; `:155` "the **residual-is-original**
  simplification." A prior ADR naming its own deferred follow-on is the strongest scope provenance.
- **The defect is two lines.** `split_boolean_tree` (`binder.rs:1208`) sets the OR-arm residual to
  `isolate_bound_subgraph(source, root)` (the whole original) at `:1255`, and the NOT-arm residual the
  same at `:1271`. Those two are the change.
- **The extraction already finds the factors.** `try_extract_common_or_factors` (`:1310`) collects OR
  branches (`collect_bound_or_branches`) + each branch's AND terms (`collect_top_level_bound_and`),
  identifies the structurally-common, PBC-compilable factors (`bound_subgraphs_structurally_equal`),
  and returns the pushed program. `try_extract_not_or_branch_negations` (`:1370`) builds & tests each
  branch negation. **Both have everything needed to also build the residual** — they just don't today.
- **075 already minimizes the AND path** (`078_*.md:36`: "075-minimal residual for top-level `AND`,
  original residual for new mixed"). So 094 extends minimal-residual to exactly the OR/NOT factored
  shapes 078 left as original — **clean seam, no overlap** with the AND path.
- **The 3VL gate already drives `partition_bound_predicate`.** `predicate_pushdown_emitted_splits_preserve_kleene_3vl`
  (`binder.rs:3814`) calls `partition_bound_predicate` and asserts `k3_and(eval(pushed),
  eval(residual)) == eval(original)` over all 27 {T,F,N}³ rows — so the moment 094 changes the
  residual, this test *automatically* validates the minimal shape. Soundness is enforced by the
  existing harness, not asserted.

## Decisions (promotion-locked)

**D1 — Common-OR factor stripping.** `(P∧R1) ∨ (P∧R2)` → `pushed=P`, **`residual = R1 ∨ R2`**.
Generalize to N identical factors × M branches: `(P∧Q∧R1)∨(P∧Q∧R2)∨(P∧Q∧R3)` → `pushed=P∧Q`,
`residual=R1∨R2∨R3`. The residual is the **OR of (each branch's terms minus the extracted common
factors)**. **D2 — Branch-strips-to-TRUE.** If stripping empties a branch, that branch is `TRUE`;
since `… ∨ TRUE = TRUE` in K3 (even `NULL ∨ TRUE = TRUE`), the whole residual is `None`. **D3 —
NOT(OR) minimization.** `NOT(P∨R1∨R2)` → push every compilable branch negation; **`residual = AND of
NOT(unpushed branch)`** (i.e. `¬R1 ∧ ¬R2` — the De Morgan equivalent of `NOT(R1∨R2)`, chosen because
`build_negated_subgraph` already yields per-branch negations, so the `AND of ¬branch` form is the
direct reconstruction); if all branches push, `residual = None`. De Morgan `NOT(OR)` only — **`NOT(AND)`
stays out** (yields an OR with no individually-necessary term). **D4 — Structural identity only.** No
commutation, algebraic-equivalence proving, range reasoning, constant folding, or CNF/DNF — only
removal of the *exact* extracted factors. **D5 — Bounded, with 078 fallback.** Reconstruction stays
inside ADR 078's existing extraction budgets (`BOOLEAN_FACTOR_MAX_*`). If reconstruction fails after
a safe pushed predicate has been identified, fall back to ADR 078 behavior for that shape: push the
necessary predicate and keep the original residual. If extraction exceeds budget before a safe split
exists, keep the existing conservative fallback. `compile_pbc`-acceptance remains the sole authority
for the pushed side. **D6 — Binder-local, zero new surface.** Changes live in `split_boolean_tree` +
extraction/combine helpers; reuses the existing isolate/clone/combine machinery; adds one
`combine_bound_programs_as_or` helper and a residual return on the two extractors. No
IR/SQL/PBC/Wendelon/wire/dep change.

## Behavior gate

`where_common_pushable_conjunct_factors_out_with_minimal_residual` (plan-shape: residual is the OR of
branch-specifics only, not the whole tree); `where_common_pk_conjunct_minimal_residual_keeps_point_read_profile`
(scan profile still from the pushed PK equality; residual no longer repeats it — the measurable win);
`where_multiple_common_factors_strip_from_each_or_branch`;
`where_common_factor_branch_strips_to_true_removes_residual_when_safe` (D2 → residual `None`);
`where_not_or_minimal_residual_keeps_only_unpushed_branch_negations` (`NOT(P∨R)` → residual `¬R`);
`where_not_or_all_branch_negations_push_removes_residual` (D3 → `None`, likely helper/unit coverage
because ordinary small all-PBC `NOT(OR)` predicates are caught by the existing whole-subtree fast path);
**`predicate_pushdown_emitted_splits_preserve_kleene_3vl` extended** with multi-branch /
branch-strips-to-TRUE / partial-NOT(OR) program generators (the correctness linchpin — same 27-row
check, new shapes); reconciled DML `delete_common_or_factor_uses_pushed_candidate_plus_minimal_residual`
+ `update_not_or_extraction_uses_pushed_source_scan_plus_minimal_residual`.

## Architecture guards (discipline #4 — the promotion risk)

ADR 078 *intentionally pinned* original residuals, so the primary risk is a red suite. **Invert
in-change** (verified to exist): the DML fixtures
`delete_common_or_factor_uses_pushed_candidate_plus_original_residual` (`delete_test.rs:149`) +
`update_not_or_extraction_uses_pushed_source_scan_plus_original_residual` (`update_test.rs:341`),
and the source guards referencing them (`tests/architecture/predicate_pushdown.rs:887,891`) — retarget
"original residual" → "minimal residual." **Add:** `adr094_common_or_factor_reconstructs_minimal_residual`;
`adr094_not_or_reconstructs_remaining_branch_negations` (`NOT(AND)` stays out);
`adr094_branch_strips_to_true_removes_common_or_residual`;
`adr094_minimal_residual_preserves_078_kleene_gate` (the property fixture must include the new shapes);
`adr094_no_new_ir_sql_pbc_wendelon_surface`. **Keep green:** ADR 074/075/078 whole-subtree pushdown,
structural-identity, bounded-extraction, compile-attempt authority, shared SELECT/UPDATE/DELETE path.

## Scale boundary

Binder-local, single-predicate; no distributed/wire/Wendelon/executor/PBC/IR surface, no new scale
axis (it *shrinks* the residual program the existing path already builds). Touches no load-bearing seam.

## What to do next

*(Per the roadmap convention: this section is the next-topic source.)*

- **EmptyRelation + durable refinement contracts → ADR 095** (the deepen-on-identity tier the council
  forked over; Codex's `ExecutionPlan::EmptyRelation`-only + conservative-3VL-classifier shape is the
  ready substrate half — incl. the closed-literal dummy-batch caveat + the scalar-aggregate
  cardinality fence from the iter-2 drafts — and durable predicate persistence is the cross-ring half).
- Rune `Like`/`ILike` (smallest carried cut); DROP/ALTER LOGICAL TYPE; information_schema.logical_types;
  PK/FK-derived identity (blocked on FK).

## Council history

iter-0: Claude=078 (grounded refinement-contracts as a must-split mega-scope), Codex+Gemini=elision-
first. iter-1: **cross-over** — Codex re-grounded to 078 on ADR 078's deferral text; Claude conceded
to the elision headcount (wrong reason). iter-2: Claude collapsed back to 078 — *verifying one
load-bearing fact (078's own deferral) beats counting votes* — and addressed Gemini's "integrity
before optimization" hold (elision-first sans durable predicate is itself a thin optimization).
iter-3 (dual): operator broke the cycle → 094 = residual minimization; Codex accepted the lock
(EmptyRelation→095), Gemini's dual aligned; design converged on the shared shape. Migration:
`plans/impl/094_predicate_residual_minimization.md`.

## Promotion criteria

Factored `(P∧R1)∨(P∧R2)` emits `pushed=P, residual=R1∨R2`; `NOT(P∨R)` emits `pushed=¬P, residual=¬R`;
branch-strips-to-TRUE and extractor all-pushed yield `residual=None`; structural-identity only;
reconstruction failures after a safe pushed split fall back to ADR 078's original residual; the
extended 3VL property fixture is green across all new shapes; the 078 DML original-residual guards
are inverted in-change (suite green, not red); 074/075/078 + all prior green; no
IR/SQL/PBC/Wendelon/wire change. Migration:
`plans/impl/094_predicate_residual_minimization.md`.
