# ADR 110: Conditional Logical-Identity Preservation

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** on the core; the one sub-decision
(physical widening) is the **grounded 2-of-3 reject** (Claude + Codex), with Gemini's erase-to-naked surfaced
+ resolved against below (discipline #1). **Toron-only — binder-local nominal correctness** over the existing
`Expr::Case`; **no** new IR/bound variant, executor kernel, PBC opcode, Rune API, explicit-`NULL` substrate,
or Wendelon change. **Predecessors:** 087–098 (nominal-identity spine), 108 (`Expr::Case`), 109 (conditional
sugar). **Migration:** `plans/impl/110_conditional_logical_identity_preservation.md`.

## Doctrine — conditionals must not be an identity-erasure escape hatch

108 gave Toron `Expr::Case`; 109 made it broadly authored (six SQL sugars). All of it erases logical identity
to `NAKED` (108 D7 simplification). After 109 that is a real integrity hole: `CASE WHEN flag THEN customer_id
ELSE customer_id END` should stay a `CustomerId` but silently goes naked — an identity-erasure path exactly
where the frontend just made conditionals common. 110 closes it with **one shared `bind_case` rule** so SQL
CASE, all six 109 sugars, and a future Rune `.case` inherit correct nominal behavior by construction.

## Scale boundary

Binder-local identity rule over `Expr::Case`. The value is **nominal-identity flow** — downstream nominal
checks + projection-schema metadata — **not** refinement elision: 097/100 need a *stored-scan receipt*, and a
CASE result is computed, so it stays non-transparent for that optimizer proof (108 D7 holds). **No** SQL
grammar, Rune API, `Expr::Null`, general explicit-`NULL`, `NVL2`, IR/executor/PBC/storage/Wendelon surface.

## Grounding (verified)

- `bind_case` binds conditions/results, computes only physical `case_common_type`, retypes via
  `type_case_result`, appends with the **default `NAKED`**; it never reads `node_logical_type`.
- **Propagation is free:** `set_node_logical_type` (`ir/execution.rs:516`) stamps the appended node;
  `schema_for_exprs_with_output_names` (`binder.rs:5368`) already reads `root_logical_type` (`:5385`) and
  writes `field_with_logical_type_metadata` (the 089 path) → flows to the projection schema + downstream.
- **Policy tool:** `nominal_compatible` (`binder.rs:5157`) — same identity OK; both-naked OK; non-naked +
  naked-*literal* OK; non-naked + naked-*non-literal* → `NominalTypeMismatch`; mixed non-naked → reject;
  asserts equal physical. `reconcile_identity` is too permissive (a naked column would inherit identity) —
  not used for branch folding.
- **The widening fork's grounding:** `bind_comparison` (`:5064`) runs `coerce_comparison_literals` (coerces a
  *literal* toward the other side's physical), then `if left_type != right_type { return Err(TypeMismatch) }`
  — so a non-literal physical mismatch **rejects**, and a literal is coerced *toward the column* (the
  identity-bearing column is never widened). CASE differs: `case_common_type` chooses an arithmetic common
  type and `type_case_result` casts every branch — so CASE *can* widen an identity column where comparison
  cannot. There is therefore **no comparison precedent for erase-on-widening**; 087's "implicit erasure is a
  loud reject; the escape hatch is an explicit cast" governs. (This is why Gemini's erase analogy — §Council —
  does not hold.)
- `case_result_is_naked` (`sql_case_test.rs:210`) pins the 108 unconditional-naked simplification.

## Decisions (promotion-locked, 3-of-3 core)

**D1 — Compute CASE output identity in `bind_case`, stamp the node.** After `output_type`, a helper
`case_output_logical_type(builder, &result_ids, &output_type)` returns the preserved `LogicalTypeId` (or
`NAKED`) over the **pre-cast** result ids; `set_node_logical_type(case_id, …)` after `append_case` iff
non-naked. Propagation is the existing 089 path — no new machinery.

**D2 — Reuse 087's `nominal_compatible` (literal-aware); not `reconcile_identity`.** No second nominal policy.

**D3 — Neutral branches.** 109 `Utf8(None)` placeholders, missing-ELSE typed nulls, and naked **literals** are
identity-neutral; a CASE with one identity branch and naked-literal/placeholder branches carries the identity.

**D4 — Reject ambiguous erasure (intentional behavior change).** Mixed non-naked identities (`CustomerId` vs
`ProductId`) → reject; non-naked + naked-**non-literal** (`THEN customer_id ELSE other_int64_col`, same
physical) → reject. Today's silent `NAKED` becomes a `NominalTypeMismatch` — the same judgment `=`/`IN` make.

**D5 — Reject implicit physical widening of an identity-bearing branch** (grounded 2-of-3). If any non-naked
result branch's physical ≠ the chosen `output_type` (i.e. it would take a widening cast), binding **rejects**
— CASE neither silently preserves identity across a recast nor silently returns naked after erasing it. The
escape hatch is an explicit cast (`CASE WHEN flag THEN CAST(customer_id AS BIGINT) ELSE 1.5 END`), per 087.
All-naked widening (`int_col`+`float_col`) is unaffected — only identity branches reject on widening. The
error is a physical `TypeMismatch` with a diagnostic that tells the user to cast explicitly before widening.

**D6 — All-naked stays naked** (incl. all-naked numeric widening). Unchanged for ordinary SQL CASE.

**D7 — Identity flow, not refinement elision** (108 D7 holds; CASE is computed, not a stored-scan receipt).

**D8 — Reuse, no new substrate.** Binder-local + 089 propagation; all six 109 sugars inherit; no
IR/executor/PBC/Rune/Wendelon surface.

## Surfaced divergence (discipline #1) — physical widening: reject vs erase

iter-3 left one sub-decision open. **Claude + Codex: reject** (D5). **Gemini iter-2: erase-to-naked**, on
"consistent with `col_id = 3.0` semantics." Resolved to **reject**, because that analogy is grounded-against:
`bind_comparison` coerces the *literal* toward the identity-bearing column (so `customer_id = 3.0` keeps
`CustomerId`) and **never widens the identity column**; CASE's `case_common_type` *does* widen the column —
so there is no comparison precedent for erasing-on-widening, and 087's implicit-erasure-reject doctrine
applies. (Claude conceded its own iter-2 erase to reject on this same grounding.) Promotion-reversible; a
follow-on may revisit if the strictness proves costly.

## Behavior gates

`case_uniform_logical_identity_propagates_to_projection_schema`,
`case_missing_else_preserves_uniform_logical_identity`, `coalesce_logical_identity_preserves_through_null_fallback`,
`if_logical_identity_preserves_through_null_branch`, `case_literal_fallback_can_infer_logical_identity`
(`THEN customer_id ELSE 7` → CustomerId), `case_mixed_logical_identities_reject`,
`case_logical_identity_plus_naked_nonliteral_rejects`, `case_identity_branch_rejects_physical_widening`,
`case_produced_identity_is_nominally_checked_downstream`, `case_computed_identity_does_not_refinement_elide`,
`case_all_naked_result_stays_naked`, `case_numeric_widening_all_naked_still_works`.

## Architecture guards

- Retarget `case_result_is_naked` (`sql_case_test.rs:210`) → all-naked CASE stays naked (drop the
  unconditional-naked assertion).
- Add `adr110_case_preserves_compatible_logical_identity` (`tests/architecture/nominal_type_identity.rs`):
  `bind_case` computes identity from pre-cast `result_ids` via a helper that reads `node_logical_type`, calls
  `nominal_compatible`, treats `is_case_null_placeholder` neutral, rejects a non-naked branch whose physical ≠
  output type, and **not** `reconcile_identity`; `bind_case` calls `set_node_logical_type` on the CASE node.
- Keep 108/109 guards (sugar still desugars to CASE → inherits).

## Council history

3-way iter-0 (Rune `.case` / explicit-`NULL` / identity-repair). **Claude conceded Rune `.case`** to Codex's
identity-repair (grounding: the conditionals arc created an identity-erasure hole; fix the shared binder
before adding surfaces); Gemini's explicit-`NULL` is the bigger deferred cut. Shape converged iters 1–2
(`nominal_compatible` fold, stamp the node, neutral placeholders, retarget the naked guard). **Iter-3 closed
the widening sub-fork:** Claude conceded its iter-2 erase → reject after verifying `bind_comparison` rejects
non-literal physical mismatch (no erase precedent); Codex held reject; Gemini's erase resolved against on the
same grounding. Genuine 3-of-3 core; 2-of-3 grounded on the widening micro-decision.

## What to do next

Rune `.case` (now inherits correct identity); general explicit-`NULL` literals (+ `NVL2`); aggregate-output
identity (same nominal-flow theme).

## Promotion criteria

A CASE / 109 sugar whose non-placeholder branches share one identity (nakeds neutral as literals) carries it
to the projection schema + downstream nominal checks; mixed identities, identity-plus-naked-non-literal, and
implicit physical widening of an identity branch all reject (explicit CAST erases); all-naked stays naked;
identity flow, not elision; binder-local, no new IR/executor/PBC/Rune/Wendelon surface; `case_result_is_naked`
retargeted + `adr110_*` added; 108/109 guards green; Toron suite green. Migration:
`plans/impl/110_conditional_logical_identity_preservation.md`.
