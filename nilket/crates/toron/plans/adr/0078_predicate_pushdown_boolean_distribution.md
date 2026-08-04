# ADR 078: Predicate Pushdown Boolean Distribution — Safe Boolean Factoring (OR / NOT)

**Status:** Promoted (plan) — **2-of-3 (Codex, Claude) on the full design**;
Gemini iter-3 held a minimal recursive-split variant (no De Morgan, no factoring)
that is safe but a near-no-op against ADR 075 (recorded as rejected — see Council
History).
**Category:** Toron compute-ring predicate pushdown / PBC attachment (binder-side).
**No Wendelon / storage / SQL-wire / worker-wire / telemetry / audit / dependency
/ PBC-opcode change; no `syn`.**

## The gap (grounded — narrower than "OR/NOT pushdown")

- Fully-PBC `AND`/`OR`/`NOT` trees already compile (existing `OP_AND`/`OP_OR`/
  `OP_NOT`); `is_pbc_pushdown_eligible` recurses, so 075 already pushes a fully-PBC
  `OR`/`NOT` when it's a top-level `AND` conjunct.
- A **mixed `P OR R` must not push `P` alone** — rows where `P` is false may satisfy
  `R`, so storage would drop valid rows. Fundamentally non-pushable.

078 earns its scope by extracting only **necessary** PBC predicates from mixed
boolean shapes, keeping the original mixed predicate as residual:

```text
(P AND R1) OR (P AND R2)  =>  push P, residual = original predicate
NOT (P OR R)              =>  push NOT P when it compiles, residual = original
```

## Soundness rule (the invariant the ADR rests on)

A pushed predicate `N` extracted from original `P` must be **necessary**:
`P(row) == TRUE ⇒ N(row) == TRUE`. Storage may drop rows where `N` is not true;
Toron re-evaluates the residual. This is the non-negotiable execution-safety rule:
no row that can make `P` true may be removed by storage.

The 078 extraction rules must satisfy the stronger SQL/Kleene-3VL identity for
emitted splits: `original == pushed AND residual` over TRUE/FALSE/NULL results
(075-minimal residual for top-level `AND`, original residual for new mixed
OR/NOT shapes). De Morgan + distribution are used only in shapes where that
identity holds. The 3VL property test (D13) is the durable proof.

## Decisions (14, Promotion-Locked)

**D1 Recursive splitter spine, extraction rules at OR/NOT.** Implement the
partitioner as a recursive `split_boolean_tree(node) -> (Option<pushed>,
Option<residual>)` walk — **Gemini's skeleton is the right structure**; the
necessary-candidate extraction rules (D6/D8) are the load-bearing OR/NOT behavior.

**D2 Necessary-filter invariant** (above) — pinned by a guard; D13 pins the
stronger Kleene-3VL equality expected from every emitted split.

**D3 Whole-subtree PBC preserved.** Fully-pushable `OR`/`NOT` push as whole
subtrees by `compile_pbc` attempt (075). 078 does not relabel or regress this.

**D4 075 top-level-`AND` split stays minimal.** Pushed conjuncts are removed from
the Toron residual (075 projection-pruning + scan-profile behavior unchanged).

**D5 New mixed-shape extractions keep the original residual.** For common-factor
`OR` and mixed `NOT(OR …)`, push only the necessary PBC candidates and keep the
**original** predicate as residual — no residual reconstruction/minimization this
ADR (correctness over minimality).

**D6 Common-factor extraction across `OR`.** Flatten an `OR` into branches, flatten
each branch's top-level `AND` terms; a term **structurally identical in every
branch** is necessary and may push if it `compile_pbc`s. `(P AND X) OR R` does
**not** push `P` unless that identical `P` is in every branch.

**D7 Structural identity only.** Exact bound-subgraph equality (node kind, output
type, operators, columns, literal values, patterns, child order, target types) —
**no** commutation, constant folding, range subsumption, arithmetic normalization,
SQL-text, or `Debug`-based equivalence (a later optimizer's job).

**D8 De Morgan on `NOT(OR …)` only.** If `NOT(A OR B …)` doesn't compile whole,
each branch negation (`NOT A`, `NOT B`, …) is necessary → push the `compile_pbc`-
able ones. **`NOT(A AND B)` yields no individually-necessary term** (De Morgan
makes it an `OR`) → stays residual unless the whole subtree compiles.

**D9 Bounded extraction, no CNF/DNF.** Do **not** distribute `(A AND B) OR C` into
`(A OR C) AND (B OR C)`. Small branch/term/node/candidate budgets; over-budget ⇒
fall back to 075 behavior.

**D10 Compile-attempt classification is authoritative** (075) — every candidate
accepted only by `compile_pbc` (preserves type/opcode/stack-depth/bytecode limits).

**D11 Pushed candidate drives scan profile + pruning** (075) — point-read hints
and pushed-only column pruning from the final pushed predicate.

**D12 One shared scan-predicate path** — SELECT/CTAS/INSERT-SELECT reads, UPDATE
source scans, DELETE candidate scans use the same partitioner (075/077). No
DML-local copy.

**D13 Behavior-additive proof gate.** Promotion requires a deterministic
SQL/Kleene-3VL fixture: for generated predicates and random **NULL-bearing** rows,
every emitted split satisfies `eval(pushed ∧ residual) == eval(original)`. Exact
Kleene equality is the stronger gate; preserving the `WHERE TRUE` set is the
minimum safety condition. No new test dependency.

**D14 Toron-local, syntax-neutral.** Existing SQL boolean syntax just partitions
better after binding; no new SQL surface, PBC opcode, Wendelon protocol,
dependency, telemetry, audit, or `syn` work.

## Architecture Guards (15 + meta)

`adr078_recursive_splitter_hosts_or_not_extraction_rules`,
`adr078_pushed_predicate_is_necessary_for_original`,
`adr078_factoring_preserves_3vl_where_true_set` (the property test, load-bearing),
`adr078_fully_pushable_or_and_not_still_push_as_whole_subtrees`,
`adr078_top_level_and_split_remains_minimal`,
`adr078_mixed_or_does_not_push_one_branch_only`,
`adr078_common_or_factor_requires_presence_in_every_branch`,
`adr078_common_factor_matching_is_structural_not_algebraic`,
`adr078_not_or_extracts_only_compilable_branch_negations`,
`adr078_not_and_does_not_push_individual_negated_terms`,
`adr078_boolean_factoring_has_bounded_extraction_budget`,
`adr078_pushdown_classification_still_uses_compile_pbc_attempts`,
`adr078_factored_pushdown_drives_scan_profile`,
`adr078_select_update_delete_share_boolean_factoring_path`,
`adr078_no_new_sql_surface_pbc_opcode_wendelon_wire_storage_or_dependency_change`.
**Meta:** `adr078_sqlparser_ast_remains_quarantined_to_sql_planner` (no `syn`);
`adr078_prior_074_075_predicate_guards_continue_to_pass`.

## Integration Fixtures (13)

`where_fully_pushable_or_pushes_whole_or` (075 regression);
`where_fully_pushable_not_pushes_whole_not`;
`where_mixed_or_branch_does_not_push_partial_branch` (D5/the guardrail);
`where_common_pushable_conjunct_factors_out_of_or_and_preserves_rows`;
`where_common_pk_conjunct_across_or_uses_point_read_profile`;
`where_structurally_different_equivalent_terms_do_not_factor` (D7);
`where_not_or_mixed_extracts_negated_pushable_branch_and_preserves_rows` (D8);
`where_not_and_mixed_does_not_push_negated_single_side`;
`boolean_factoring_over_budget_falls_back_without_wrong_results` (D9);
**`predicate_pushdown_3vl_property_random_rows_including_nulls`** (the gate);
`delete_common_or_factor_uses_pushed_candidate_plus_original_residual`;
`update_not_or_extraction_uses_pushed_source_scan_plus_original_residual`. Existing
074/075 predicate fixtures stay green.

## Honest value framing

These are **completeness** wins on relatively rare shapes (`NOT(eligible OR
non-eligible)`, common-factor ORs); 075 already handles top-level `AND` with mixed
conjuncts and eligible ORs. The worth is **sound, full-boolean-structure pushdown
as a foundation** for future predicate work — not a major perf swing.

## Out of Scope

No `syn`/AST source rewrite; no general boolean optimizer; no full CNF/DNF; no
algebraic-equivalence prover; no residual minimization for the new shapes; no new
SQL syntax; `SIMILAR TO`/`RLIKE`/`REGEXP`, `LIKE ESCAPE`, dynamic `LIKE`, `CASE`,
and Wendelon PBC-language changes all remain carried.

## Council History

075 deferred OR/NOT distribution (3VL caution); 077 closed the test-infra detour.
Claude grounded the narrow gap + the Kleene-3VL soundness of the rewrites and made
the **3VL property test** the gate. Codex contributed the **necessary-filter
invariant** and the **residual-is-original** simplification; Claude + Codex
converged on the full design (common-`OR` factoring, `NOT(OR)` De Morgan, bounded
extraction, structural identity, compile-attempt). **Gemini's recursive-split
skeleton was adopted as the implementation spine** (D1), but its **all-or-nothing
OR/NOT rules (no De Morgan, no factoring) were rejected** as safe-but-near-no-op
against 075 — the De Morgan + factoring extractions are what justify the ADR.
2-of-3 on the design; Gemini's "3-of-3" header reflects the shared recursive-split
*skeleton*, not the scope.

## Promotion criteria met

D-set 14; 15 + meta guards; 13 fixtures (incl. the 3VL property gate);
behavior-additive (WHERE-TRUE-set preserved); necessary-filter soundness;
Toron-local, no opcode/dep/syn; reuses the 075/077 partitioner across SELECT/
UPDATE/DELETE. Migration:
`plans/impl/0078_predicate_pushdown_boolean_distribution.md`.

## Conclusion

078 = **safe boolean factoring**: a recursive `split_boolean_tree` partitioner
(Gemini's spine) that extracts only **necessary** PBC predicates from mixed
booleans — common-`OR` factors + `NOT(OR)` branch negations — pushes them, and
keeps the original predicate (or the 075-minimal residual) in Toron. **3VL-sound**
(property-test-gated), bounded (no CNF), compile-attempt-classified, syntax-neutral.
Mixed ORs stay correctly residual; the win is sound full-boolean pushdown, not a
perf swing.
