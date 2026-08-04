# Impl Plan — ADR 094: Predicate Pushdown Residual Minimization (factored OR / NOT)

**Migration for** `plans/adr/0094_predicate_residual_minimization.md`. **Toron-only, `binder.rs`-localized.**
The change makes the two extraction helpers return a reconstructed minimal residual; the OR/NOT arms
of `split_boolean_tree` use it instead of `isolate_bound_subgraph(root)`. No IR/SQL/PBC/Wendelon/wire/dep.

## Pre-flight anchors (grounded — do not re-derive)

| Concern | Anchor |
|---|---|
| Split entry / fallback | `partition_bound_predicate` `binder.rs:1185` (OverBudget → `legacy_top_level_and_partition`) |
| The two residual sites to change | `split_boolean_tree` OR arm `binder.rs:1252-1256`, NOT arm `:1263-1272` (both set `residual = isolate_bound_subgraph(source, root)`) |
| Common-OR extractor | `try_extract_common_or_factors` `:1310` — collects branches + per-branch AND terms, finds common PBC-compilable factors |
| NOT(OR) extractor | `try_extract_not_or_branch_negations` `:1370` — builds & PBC-tests each branch negation |
| Branch/term collectors | `collect_bound_or_branches` `:1404`; `collect_top_level_bound_and` `:1431`; `bound_subgraphs_structurally_equal` (struct eq) |
| Build blocks | `isolate_bound_subgraph` `:1449`; `combine_bound_conjuncts(source, roots)` `:1456` (clones roots, ANDs); `combine_bound_programs_as_and(progs)` `:1477`; `build_negated_subgraph` (NOT of a subgraph) |
| Budgets | `BOOLEAN_FACTOR_MAX_NODES` / `_OR_BRANCHES` / `_AND_TERMS_PER_BRANCH` / `_CANDIDATES` |
| 3VL gate (auto-validates) | `predicate_pushdown_emitted_splits_preserve_kleene_3vl` `binder.rs:3814` (calls `partition_bound_predicate`, checks 27 {T,F,N}³ rows) |
| DML guard-inversion targets | `delete_test.rs:149`, `update_test.rs:341`; source guards `tests/architecture/predicate_pushdown.rs:887,891` |

## Phase 1 — Add `combine_bound_programs_as_or`

Mirror `combine_bound_programs_as_and` (`:1477`) exactly, with `BoundBinaryOp::Or`. Returns
`Result<Option<BoundExprProgram>, BooleanSplitError>`; `None` for an empty input vec. This is the only
new primitive — everything else reuses existing helpers.

## Phase 2 — `try_extract_common_or_factors` returns pushed + minimal residual (D1/D2)

Change the return to carry the residual:

```rust
struct OrFactoring { pushed: BoundExprProgram, residual: Option<BoundExprProgram> }
// fn try_extract_common_or_factors(..) -> Result<Option<OrFactoring>, BooleanSplitError>
```

Keep the existing branch/term collection (`:1320-1357`) and `necessary_candidates` discovery
unchanged, but **retain `branch_terms`** (already built at `:1320-1328`) and keep `necessary_candidates`
as `BoundExprId`s through residual construction. Compute `pushed` while retaining those ids:

```rust
let pushed_programs = necessary_candidates
    .iter()
    .copied()
    .map(|candidate| isolate_bound_subgraph(source, candidate))
    .collect::<Result<Vec<_>, _>>()?;
let pushed = combine_bound_programs_as_and(pushed_programs)?
    .ok_or(BooleanSplitError::Malformed)?;

let mut branch_residuals = Vec::with_capacity(branch_terms.len());
for terms in &branch_terms {
    let mut used = vec![false; terms.len()];
    for common in &necessary_candidates {
        let Some(index) = terms.iter().enumerate().find_map(|(index, term)| {
            (!used[index] && bound_subgraphs_structurally_equal(source, *common, *term))
                .then_some(index)
        }) else {
            return original_or_residual_factoring(source, root, pushed);
        };
        used[index] = true;
    }

    let remaining: Vec<BoundExprId> = terms
        .iter()
        .enumerate()
        .filter_map(|(index, term)| (!used[index]).then_some(*term))
        .collect();
    if remaining.is_empty() {
        // D2: branch strips to TRUE -> OR is TRUE -> residual None
        return Ok(Some(OrFactoring { pushed, residual: None }));
    }
    let Some(branch) = combine_bound_conjuncts(source, &remaining) else {
        return original_or_residual_factoring(source, root, pushed);
    };
    branch_residuals.push(branch);
}
let Ok(residual) = combine_bound_programs_as_or(branch_residuals) else {
    return original_or_residual_factoring(source, root, pushed);
};
Ok(Some(OrFactoring { pushed, residual }))
```

The one-match-per-common-factor `used` vector is deliberate: do not strip every structurally-equal
duplicate term in a branch, because that would smuggle in idempotence simplification beyond ADR 094's
exact extracted-factor rule. `original_or_residual_factoring` is a small shape-local fallback that
returns `pushed + isolate_bound_subgraph(source, root)` when minimal reconstruction fails after a
safe pushed predicate has already been identified.

## Phase 3 — `try_extract_not_or_branch_negations` returns pushed + minimal residual (D3)

Split the branch negations into pushed vs residual instead of only collecting pushed:

```rust
struct NotOrFactoring { pushed: BoundExprProgram, residual: Option<BoundExprProgram> }
// fn try_extract_not_or_branch_negations(
//     source: &BoundExprProgram,
//     not_root: BoundExprId,
//     or_root: BoundExprId,
// ) -> Result<Option<NotOrFactoring>, BooleanSplitError>

let mut pushed_negs = Vec::new();
let mut residual_negs = Vec::new();
for branch in branches {
    let neg = build_negated_subgraph(source, branch)?;
    if compile_pbc(&neg).is_some() {
        pushed_negs.push(neg);
        if pushed_negs.len() > BOOLEAN_FACTOR_MAX_CANDIDATES {
            return Err(BooleanSplitError::OverBudget);
        }
    } else {
        residual_negs.push(neg);
    }
}
if pushed_negs.is_empty() { return Ok(None); }            // nothing pushable -> 078 fallback at caller
let pushed = combine_bound_programs_as_and(pushed_negs)?.ok_or(BooleanSplitError::Malformed)?;
let Ok(residual) = combine_bound_programs_as_and(residual_negs) else {
    return original_not_or_residual_factoring(source, not_root, pushed);
}; // None if every branch pushed (D3)
Ok(Some(NotOrFactoring { pushed, residual }))
```

`residual = AND of NOT(unpushed branch)` is correct by De Morgan: `NOT(P∨R1∨R2) ≡ ¬P ∧ ¬R1 ∧ ¬R2`;
pushing `¬P` leaves `¬R1 ∧ ¬R2`. (Equivalent to Gemini's `NOT(R1∨R2)`; the `AND of ¬branch` form is
the direct reconstruction since `build_negated_subgraph` already yields per-branch negations.) The
candidate budget applies to pushed candidates; residual negations are bounded by the existing OR
branch budget and node budget.

## Phase 4 — `split_boolean_tree` OR/NOT arms use the returned residual

```rust
BoundExprNode::Binary { op: Or, .. } => {
    if let Some(f) = try_extract_common_or_factors(source, root)? {
        return Ok(BooleanPredicateSplit { pushed: Some(f.pushed), residual: f.residual });
    }
    Ok(BooleanPredicateSplit { pushed: None,
        residual: Some(isolate_bound_subgraph(source, root)?) })   // unchanged: no factoring
}
BoundExprNode::Unary { op: Not, input } => {
    if let Some(f) = try_extract_not_or_branch_negations(source, root, *input)? {
        return Ok(BooleanPredicateSplit { pushed: Some(f.pushed), residual: f.residual });
    }
    Ok(BooleanPredicateSplit { pushed: None,
        residual: Some(isolate_bound_subgraph(source, root)?) })   // unchanged: no factoring
}
```

**Fallback (D5):** extraction `OverBudget` before a safe split exists still propagates to
`partition_bound_predicate` (`:1191`) → `legacy_top_level_and_partition` (whole-predicate residual,
conservative). Reconstruction failures after a safe pushed predicate exists must use the
shape-local original-residual fallback (`pushed + original residual`) so ADR 094 never turns a valid
ADR 078 split into no pushdown. `Malformed` outside that known-pushed reconstruction path may still
return `None` (no pushdown). The AND arm (`:1230-1246`) is **untouched** — 075 owns the AND-minimal
residual (no overlap).

## Phase 5 — Behavior fixtures + 3VL property extension

New/updated fixtures (sibling to the 078 fixtures):
1. `where_common_pushable_conjunct_factors_out_with_minimal_residual` — assert pushed = common factor
   and **residual plan-shape = OR of branch-specifics only** (not the whole tree).
2. `where_common_pk_conjunct_minimal_residual_keeps_point_read_profile` — PK-equality common factor;
   scan profile still point-read; residual no longer repeats the equality.
3. `where_multiple_common_factors_strip_from_each_or_branch` — `(P∧Q∧R1)∨(P∧Q∧R2)`.
4. `where_common_factor_branch_strips_to_true_removes_residual_when_safe` — `(P∧R1)∨P` → residual `None`.
5. `where_not_or_minimal_residual_keeps_only_unpushed_branch_negations` — `NOT(P∨R)` → residual `¬R`.
6. `where_not_or_all_branch_negations_push_removes_residual` → residual `None` via helper/unit
   coverage if the normal SQL whole-subtree fast path catches the all-PBC case first.

**Extend the 3VL property gate** (`binder.rs:3814`): add `property_or_branch_strips_to_true_program`
(`(p∧r1)∨p`) and `property_not_or_partial_push_program` (`NOT(p∨r1)` where only `¬p` compiles) to the
loop array. The harness (`eval_test_program`, `k3_and`, `partition_bound_predicate`) is reused
unchanged — it now drives the minimal residuals through the same 27-row check. (The two existing
programs already auto-validate the minimal shape post-Phase 4; these broaden coverage to the D2/D3 edges.)

## Phase 6 — Invert the 078 DML guards (discipline #4)

- Retarget `delete_common_or_factor_uses_pushed_candidate_plus_original_residual` (`delete_test.rs:149`)
  → `..._plus_minimal_residual`; assert residual = `R1∨R2`, not the whole OR.
- Retarget `update_not_or_extraction_uses_pushed_source_scan_plus_original_residual` (`update_test.rs:341`)
  → `..._plus_minimal_residual`; assert residual = `¬R`.
- Update the source guards at `tests/architecture/predicate_pushdown.rs:887,891` that reference those
  fixture names. Add the five `adr094_*` guards from the ADR.

## Phase 7 — Verification

```
cargo test -p toron --test architecture_test          # 078 guards inverted + adr094_* added + keep-green
cargo test -p toron --test delete_test --test update_test --test predicate_widening_test
cargo test -p toron                                    # full suite; 3VL property gate green over new shapes
```

**Acceptance:** the six behavior gates pass; the extended 3VL property fixture is green across the new
minimal shapes (the correctness linchpin); the inverted DML guards/fixtures are green in their new
"minimal residual" form; 074/075/078 + all prior green; full suite **green, not red** (don't ship the
078 original-residual guards red — the 090-review lesson).

## Risks / notes

- **Pure efficiency, zero result change.** `P ∧ minimal_residual ≡ P ∧ original_residual` on survivors,
  so output is byte-identical; only Toron's residual work shrinks. The 3VL gate is the proof.
- **Ordering subtlety in Phase 2:** keep `necessary_candidates` as ids while building both `pushed`
  and the per-branch `used` masks; the existing code consumes them into `pushed` first — reorder, do
  not duplicate.
- **Reconstruction adds no real budget risk** (branches/terms already collected within the existing
  budget); reconstruction-only failures after a safe pushed split use the original-residual fallback
  to keep ADR 078's conservative floor.
- **No `NOT(AND)`** (yields an OR with no individually-necessary term) and **no AND-path change** (075
  owns it) — both pinned by guards so a future refactor can't silently widen scope.
