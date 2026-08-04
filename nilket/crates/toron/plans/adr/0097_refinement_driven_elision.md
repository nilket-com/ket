# ADR 097: Refinement-Driven Read-Time Elision

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — verified against the iter-2
drafts: identical design (third `static_filter_class` arm; durable-registry-not-seed; stored-scan
provenance gate; 1D carrier-domain intersection; 095 `EmptyRelation` reuse; structural 3VL/error
conservatism). **Predecessors:** ADR 096 (the durable refinement *receipt* 097 spends) + ADR 095 (the
`EmptyRelation` target + `static_filter_class`). **Toron-only, binder-local.** Closes the integrity
tier: 095 substrate → 096 receipt → **097 payoff**.

## Doctrine — a stored receipt is not a sidecar

096 makes a *stored* refined column a receipt: every non-null stored `id` in a `PersonId CHECK (VALUE
> 0)` column passed write-boundary validation, so `WHERE id = -5` over it is statically impossible →
reuse 095's `EmptyRelation` (zero scan, zero PBC). But this is **not** true of every expression
carrying the `PersonId` sidecar — 092/093 logical `CAST`/`TRY_CAST` are ephemeral ascriptions (096
D8), so a projected `CAST(raw AS PersonId)` column carries the metadata yet may hold `-5`. Eliding
that would be unsound. **097 proves contradiction only from direct stored scan columns** whose
identity resolves through the *durable* registry to a refinement — the receipt, not the sidecar.

## Grounding

- `plan_filter_with_predicate_pushdown` (`binder.rs:1097`) runs `static_filter_class` before PBC and
  returns `EmptyRelation { schema: input.schema() }` on `AlwaysReject`. The classifier is
  **catalog-less today** (`&BoundExprProgram`), and `logical_type_spec` (`:3351`) is **seed-only**.
- 096 added `LogicalTypeSpec.refinement` (`ir.rs:38`) + the durable `Catalog::logical_type_registry()`;
  refinements live in the synced registry, not the seed (the 091/092 trap).
- A bound `BoundExprNode::Column(_)` carries its `LogicalTypeId` via the 087 sidecar
  (`node_logical_type`) — *metadata, not provenance*.
- `ExecutionPlan::Scan` carries `projection: Option<…>`; `schema()` is stored-schema oriented, so the
  first cut requires `projection: None` to keep predicate ordinals unambiguous stored ordinals.

## Decisions (promotion-locked)

**D1 — Third static-filter proof.** `static_filter_class` grows durable + input-plan context; ADR
095's structural cases stay first-class and unchanged; one new `AlwaysReject` case (durable refinement
contradiction over a direct stored scan column); **no new `AlwaysPass`** (filter removal stays out).
**D2 — Durable registry, not seed.** The binder threads `Catalog::logical_type_registry()` into
`plan_filter_with_predicate_pushdown` (+ its callers and the update-source path); the proof resolves
`node_logical_type` through that registry and **never** calls seed-only `logical_type_spec`. **D3 —
Stored-scan provenance gate (the soundness condition).** Elide only when `input` is
`ExecutionPlan::Scan { projection: None, .. }`, the atom references a direct `BoundExprNode::Column`
addressing the scan schema, and that node's logical type resolves in the durable registry to
`refinement: Some(_)`. `Project`/`Aggregate`/`Join`/`Exchange`/`Sort`/`EmptyRelation`, casts, computed,
and pass-through projections → `Unknown`. **D4 — One whole-root comparison.** `column op literal` or
`literal op column` using `BoundBinaryOp::{Eq, Neq, Lt, Lte, Gt, Gte}`, literal non-null +
carrier-compatible; defer
top-level-`AND` decomposition, multi-column, `OR`/`NOT`/functions/casts/non-literal → `Unknown`.
**D5 — Bounded 1D carrier-domain intersection.** Convert the durable refinement (`VALUE` comparisons
joined by `AND`) and the filter atom into one carrier domain; `AlwaysReject` iff intersection provably
empty. Numeric: open/closed interval bounds **+ finite excluded points** (so `CHECK (VALUE != 0)` +
`WHERE id = 0` elides); Boolean/Utf8: equality/inequality singletons only (no Utf8 range/collation);
**floats decode from 096 bit storage via `from_bits`, finite-only — NaN/non-finite → `Unknown`**
(compare decoded values, never raw bits). Not an SMT solver. **D6 — 3VL/nullability.** A nullable
refined column may store `NULL`; `col = -5` still elides (NULL rows don't satisfy the comparison);
refinement **never** rejects `col IS NULL` — 095's non-null structural rule remains the only `IS NULL`
elision. **D7 — Structural, error-conservative.** The proof never evaluates casts/arithmetic/functions/
expression-programs/row-values/PBC; any uncertain shape → `Unknown` (inherit 095). **D8 — Invert 096's
no-elision guard (discipline #4).** ADR 096's `no_refinement_elision_yet` is retargeted; the new
invariant is *"elision only from durable stored-scan receipts"* — not "no elision." **D9 — No surface
change.** No SQL grammar / logical-IR / execution-IR / executor / storage / PBC / distributed payload /
Wendelon contract / wire / client change.

## Behavior gate

`refined_positive_column_eq_negative_elides_to_empty_relation` (`id = -5`, zero scan);
`refined_positive_column_range_disjoint_elides` (`id < 0`);
`refined_positive_column_overlapping_range_does_not_elide` (`id = 5` / `id > 100`);
**`refinement_exclusion_point_elides`** (`CHECK (VALUE != 0)` + `id = 0`);
**`cast_ascribed_projected_column_does_not_elide`** (manual `Project(CAST(raw AS PersonId))`-over-scan
+ `Filter(id = -5)` — the soundness catch, no SQL-subquery dependency);
`refinement_on_non_scan_input_does_not_elide`; `nullable_refined_column_is_null_does_not_elide`;
`naked_or_unrefined_column_does_not_elide`; `refinement_elision_uses_durable_registry_not_seed_lookup`;
`float_refinement_decodes_bits_for_ordering` (`Price CHECK (VALUE >= 0.0)` + `price = -1.0`);
`count_star_over_refinement_elided_filter_returns_single_zero` (095 cardinality fence);
`update_or_insert_select_source_filter_elides_through_shared_planner` (bonus).

## Architecture guards

`adr097_refinement_elision_uses_durable_registry_not_seed`;
`adr097_refinement_elision_requires_stored_scan_column_provenance` (`Scan { projection: None }` +
direct `Column`); `adr097_refinement_elision_only_from_durable_stored_scan_receipts` (**inverts** 096's
`no_refinement_elision_yet`); `adr097_refinement_elision_reuses_095_empty_relation`;
`adr097_static_filter_class_is_structural_3vl_and_error_conservative`;
`adr097_cast_try_cast_remain_ephemeral_not_receipts`;
`adr097_no_wendelon_096_contract_pbc_executor_storage_or_wire_change`.

## Scale boundary

Toron binder-local; reads the registry already synced by 090/096; reuses 095's `EmptyRelation`. The
reasoner is a single-variable carrier-domain check, not a constraint solver. No cross-ring/exec/PBC/
wire surface. Touches no load-bearing seam.

## What to do next

- **Pass-through-projection provenance** (lift the `projection: None` restriction once lineage is
  mechanically proven) + **top-level-`AND` decomposition** (reuse the same domain reasoner:
  `id = -5 AND active`) + **multi-column** — the natural 097 follow-ons.
- Carried: `information_schema.logical_types` (render the refinement); `DROP`/`ALTER LOGICAL TYPE`;
  Rune `Like`/`ILike`; PK/FK-derived identity; the Wendelon `*V1` de-versioning sweep; Wendelon
  authority-side validation.

## Council history

095 built the elision target; 096 the enforced receipt. 097 iter-0: 3-of-3 lock on refinement-driven
elision; **Claude grounded the two catches** (durable-not-seed; stored-scan *provenance* not sidecar);
**Codex independently grounded both** + framed it "stored-column receipt contradiction." iter-1 (dual):
Claude + Codex converge; Codex added `Scan { projection: None }`, the `!=` excluded-point case, the
concrete `CarrierDomain` reasoner, and the **096-guard inversion**. iter-2: consolidated 3-of-3.
Migration: `plans/impl/0097_refinement_driven_elision.md`.

## Promotion criteria

Direct stored refined-column contradictions elide to `EmptyRelation` before PBC with zero scan;
overlapping / nullable-`IS NULL` / unrefined / seed-only / CAST-ascribed / projected/computed /
non-scan shapes do **not** elide; the proof reads the durable registry (not seed), uses excluded-point
+ interval intersection with decoded finite floats, preserves 095's cardinality + error fences, keeps
logical casts as non-receipt ascriptions; 096's no-elision guard inverted in-change; no surface change.
Migration: `plans/impl/0097_refinement_driven_elision.md`.
