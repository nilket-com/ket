# Impl Plan 078: Predicate Pushdown Boolean Distribution — Safe Boolean Factoring

**Status:** Promoted, paired with
`plans/adr/078_predicate_pushdown_boolean_distribution.md`.
**Posture:** Binder-side predicate partitioning — evolve 075's split into a
recursive `split_boolean_tree` walk that extracts only **necessary** PBC
predicates from mixed booleans (common-`OR` factors, `NOT(OR)` branch negations),
pushes them, keeps the original predicate as residual. **3VL-sound,
behavior-additive, bounded (no CNF).** No new opcode/dep/syn; reuse 075/077 across
SELECT/UPDATE/DELETE. Six phases.

## Verified anchors (grounded)

- **075 spine:** `partition_bound_predicate` (`binder.rs:1023`); `compile_pbc`
  (eligibility-by-attempt); `FilterDisposition` + residual `Filter`; applies to
  SELECT/UPDATE source/DELETE candidate scans (077).
- **Eligible OR/NOT already push:** `is_pbc_pushdown_eligible` recurses through
  `Or`/`And`/`Not` (`ir.rs:122`); `OP_OR`/`OP_NOT`/`OP_AND` exist (`pbc.rs:8-9`).
  So the gap is **mixed** OR/NOT only.
- **No existing De Morgan / factoring / normalization** — new, binder-local.

## Invariants carried in

- **Pushed predicate necessary** (`P TRUE ⇒ pushed TRUE`); never push one branch
  of a mixed `OR`. **Residual = original** for new mixed-shape extractions
  (075-minimal for top-level `AND`). **Structural identity only** (no algebraic
  equivalence). **Bounded budget** (no CNF; over-budget → 075 fallback).
  Compile-attempt classification. Scan-profile from the pushed candidate.
  **Behavior-additive** (WHERE-TRUE set preserved). No new opcode/dep/syn.

## Phase 0 — 3VL property-test harness first (the gate)

Build it before the factoring logic: generate random bound boolean predicates over
a small typed schema (incl. NULL-bearing columns) + random rows; assert for every
emitted split that `pushed(N) ∧ residual(P)` evaluated in Toron has exact
SQL/Kleene equality with `P` over all rows (incl. NULLs). This is stronger than
the storage safety invariant (`WHERE TRUE` set preservation) and is the net every
later phase keeps green.

## Phase 1 — Recursive splitter spine (`binder.rs`)

Implement `split_boolean_tree(node) -> (Option<pushed>, Option<residual>)` (Gemini's
skeleton):

- `AND(L,R)`: split both; pushed = `AND(P_L,P_R)`, residual = `AND(R_L,R_R)` (075,
  minimal residual — pushed conjuncts removed from residual).
- `OR`: if fully eligible → push whole; else **try common-factor extraction
  (Phase 2)**; else whole `OR` residual.
- `NOT`: if fully eligible → push whole; else if child is an `OR` → **De Morgan
  extraction (Phase 3)**; else whole `NOT` residual.
- base case: `compile_pbc`-attempt ⇒ `(Some,None)` or `(None,Some)`.
- Combine pushed candidates, **final `compile_pbc`**; on failure/over-budget →
  fall back to 075 behavior.

## Phase 2 — Common-`OR`-factoring

Flatten `OR` branches; flatten each branch's top-level `AND` terms; intersect by
**exact bound-subgraph structural equality** (node kind/type/ops/columns/literals/
patterns/child-order/target-types). A term in **every** branch that `compile_pbc`s
is necessary → push it; **residual = the original `OR`** (no reconstruction). Bound
branch/term count.

## Phase 3 — De Morgan on `NOT(OR)` only

`NOT(A OR B …)` not compiling whole ⇒ each `NOT A`, `NOT B`, … is necessary → push
the `compile_pbc`-able branch negations; residual = original `NOT`. **`NOT(AND)` →
no extraction** (becomes OR, no necessary term). Do not add standalone
double-negation simplification in this ADR; eligible double-negation may still
push through the whole-subtree compile path. Bounded depth.

## Phase 4 — Scan profile + DML

`scan_profile_hint` from the pushed candidate (point-read if a factored term is
PK-equality). Confirm the single partitioner serves SELECT/UPDATE/DELETE (no copy).

## Phase 5 — Guards, fixtures, evidence

- 15 + meta guards (necessary-predicate, **3VL property**, recursive-splitter-hosts-
  extraction, mixed-OR-no-partial, common-factor-every-branch, structural-not-
  algebraic, NOT(OR)-extracts/NOT(AND)-doesn't, bounded-budget, compile-attempt,
  scan-profile, shared-DML, whole-OR/NOT-still-push, 075-minimal-split, no-opcode/
  dep/syn, prior-074/075-pass).
- 13 fixtures (ADR list) — anchored by the **3VL property test**,
  `not_or_mixed_extracts_negated_pushable_branch`, `common_pushable_conjunct_
  factors_out_of_or`, `mixed_or_branch_does_not_push_partial`, `over_budget_falls_
  back`, `structurally_different_equivalent_terms_do_not_factor`.
- `plans/impl/078_predicate_pushdown_boolean_distribution_evidence.md`:
  `architecture_test adr078` + full (074/075/PBC green); `cargo test -p toron`
  (predicate/pbc/binder/exec + delete/update + the property test); existing
  WHERE/filter corpus green; `cargo check -p toron` feature matrix; `clippy -D
  warnings`; `git diff --check`; a **bytecode-shape snapshot** (e.g. `NOT(region=
  'west' OR name LIKE 'A%')` → pushes `region != 'west'`, residual = original).

## Watch-items

1. **Soundness** — every pushed term necessary; mixed-OR never pushes a branch;
   the property test is the net (build first, Phase 0).
2. **Residual = original** for factored OR/NOT — no minimal-residual reconstruction.
3. **Structural identity only** — no algebraic-equivalence prover.
4. **Bounded budget** — no CNF/DNF; over-budget → 075 fallback; deep-nesting fixture.
5. **De Morgan is `NOT(OR)`-only** — `NOT(AND)` yields an OR (no necessary term).
6. **Reuse 075/077** — partitioner change is the only new logic; FilterDisposition,
   residual Filter, DML sites unchanged.

## Acceptance checklist

- [ ] Phase 0: 3VL property test green on the 075 baseline.
- [ ] Phase 1: recursive `split_boolean_tree` (whole → AND → OR-factor → NOT-DeMorgan
      → combine+compile → 075 fallback).
- [ ] Phase 2: common-`OR`-factoring, structural identity, residual = original.
- [ ] Phase 3: De Morgan `NOT(OR)`-only; bounded.
- [ ] Phase 4: scan-profile from pushed; shared SELECT/UPDATE/DELETE path.
- [ ] Phase 5: 15 guards + 13 fixtures (incl. property test) + evidence;
      behavior-additive; 074/075 corpus green; no opcode/dep/syn.
