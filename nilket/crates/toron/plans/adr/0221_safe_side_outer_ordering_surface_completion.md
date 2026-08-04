# Toron ADR 221: Safe-Side Outer Ordering Surface Completion

**Status:** Promoted. **Migration:** `plans/impl/0221_safe_side_outer_ordering_surface_completion.md`.
**Category:** Toron optimizer actuation — extends the safe-side outer-ordering
surface ADR 219 opened, by carrying its proven row-survival proof + receipt-armed
actuator to the next provably-safe shapes. The memo fork is **deferred**
(operator). No durable evidence-format change, no new verdict/decision kind, no
counter/key/table/`SystemTableKind`, no wire / executor / fragmenter / Wendelon
change; the no-memo fences stay standing.

## Why this cut (and why memo is deferred)

219 made the first safe-side outer reorder — left-outer **star of base-leaf
dimensions** around one preserved anchor (`extract_left_outer_star_join_candidate`,
join_order.rs:1358; base-leaf only via `left_outer_star_base_leaf_label`, :1397;
bound dimensions currently require `unique_source_label()` and
`composite_build == false`). Two common, provably-safe shapes still decline: the
**right-outer** mirror and the **left-outer snowflake** (a dimension that is itself
a left-outer sub-tree). 221 admits exactly those, reusing 219's machinery where it
still fits, and declines everything else.

The arc's destination — memo — is **deferred**: the recognizer dispatch is strict
first-match (binder.rs:1255–1299), so a shape gets exactly one specialist candidate
and there are no competing candidates to select between; the only non-redundant memo
actuation is therefore the Cascades multi-candidate search — too big for one cut and
gated on a production `conflicted` row. 221 widens a proven, receipt-armed behavior at
the lowest architectural risk while the memo chapter waits for its signal and design.

## The extended proof (what is provably safe to add)

**Right-outer star (mirror).** `D₁ RIGHT JOIN A … RIGHT JOIN A` preserves the same
anchor `A`; 219's row-survival invariant transposes exactly (the preserved side is the
anchor regardless of which syntactic side it is on). Same fences.

**Left-outer snowflake (opaque sub-tree dimension).** A top-level dimension may be an
opaque left-outer sub-tree — `A LEFT JOIN (B LEFT JOIN C ON B.kc=C.kc) ON A.k=B.k` —
**reordered only at the top level around the anchor**, the sub-tree's internal order
fixed. The invariant holds: every `A` row survives every top-level left outer join
(null-padded on miss); the sub-tree's matched-or-null content depends only on its
exported root key (`B.k` in the example) and its fixed internals, not on any other
dimension; so reordering the top-level dimensions is result-invariant. The sub-tree
is treated as one opaque dimension, and the top-level predicate may reference only
the anchor keys and that exported root key.

**Still fenced (decline, as 219):** within-sub-tree reorder; full-outer (both sides
preserved — not order-invariant); right/left mixed chains; right-outer opaque
snowflakes unless a later cut proves the right-deep schema restoration separately; a
predicate reaching *past the exported sub-tree root key* from the anchor level or
spanning two dimensions (breaks opacity / independence); nullable-residual
predicates that null-reject.

## Decisions (promotion-locked)

**D1 — Extend 219's receipt-armed actuator; same gate, same opt-in.** Fires only under
`ApplyAdvisory` when `preserved_outer_ordering` is `actionable`; `Manual` / cold ledger
/ non-Loosen evidence byte-identical. No new mode, no new durable surface.

**D2 — Add right-outer star and left-outer snowflake to the accepted subset.**
Everything in the fence list declines and binds as today. Sub-trees are **opaque**:
the actuator reorders top-level dimensions only, never within a sub-tree, and never
lifts a sub-tree-internal predicate to the anchor level. The snowflake path must not
depend on `unique_source_label()` for the whole sub-tree; it carries an exported
root-source label/key from recognition into binding.

**D3 — Same machinery, same proof obligation.** Shared `PreservedOuterOrderingGate`
result (219 D5), single bind-entry snapshot (219 D6), the 214 counterfactual as cost
signal only (never the live transform, 219 D3), and declared schema/column-order
restoration after the physical reorder.

**D4 — Memo deferred; no-memo fences unchanged.** This cut does **not** touch
`!"memo"` / `!"Cascades"` / `!src/optimizer.rs` / `adr197_..._not_generic_memo`; a
guard asserts they hold. The conflict-memo chapter resumes when a production
`conflicted` row + a Cascades multi-candidate-search design justify it.

## Honesty gates (discriminating)

- **Cardinal — `safe_side_right_outer_star_preserves_rows_and_schema`** and
  **`safe_side_left_outer_snowflake_preserves_rows_and_schema`** — EXECUTE each new
  shape with null-supplied **misses** and a multi-match dimension; result multiset +
  schema equal the fenced declared-order plan. Plan-diff is not proof.
- `right_outer_and_snowflake_reorder_only_when_gate_actionable` (self-arming pin).
- `within_subtree_reorder_stays_fenced` — the snowflake sub-tree's internal order is
  never permuted.
- `full_outer_and_cross_into_subtree_predicate_shapes_stay_fenced`.
- `manual_ignores_preserved_outer_ordering_for_new_shapes`.
- `selected_order_uses_outer_preflight_not_inner_receipt_targets` (219 continuity).
- `existing_left_outer_base_leaf_stars_bind_byte_identical` (the generalized extractor
  subsumes 219's path with no change to existing stars).
- `rune_safe_side_outer_surface_matches_sql`.

## Architecture guards (promotion-locked)

- `adr221_safe_side_outer_subset_is_left_right_star_and_left_outer_snowflake_only`
- `adr221_snowflake_subtree_is_opaque_no_internal_reorder`
- `adr221_full_outer_and_cross_into_subtree_predicates_stay_fenced`
- `adr221_outer_surface_uses_shared_gate_and_single_snapshot` (219 continuity)
- `adr221_no_new_durable_surface`
- `adr221_no_memo_fences_unchanged` (the deferral pin)

**Reconciled prior guards (named):**
- `adr219_outer_actuation_is_left_outer_star_only` (optimizer_rotation.rs:214) →
  **retarget** to "left/right-outer star + left-outer snowflake (opaque sub-tree)";
  the within-sub-tree / full-outer / cross-dimension fences stay.
- ADR 219's other guards (inner-targets-not-used, shared-gate, single-snapshot,
  recommended-step, manual byte-identical) → unchanged, extended to the new shapes.
- ADR 217 two-consumer / 216 140-220 / 218 graph guards → **unchanged**.
- The no-memo fences → **asserted unchanged** (memo deferred).

## Consensus (transparent)

Operator **deferred the memo fork**; 221 is the safe-side outer surface completion.
The convergence trail: Claude iter-0 leaned windowing and conceded; iter-1 converged
on Codex's conflict-memo *conditional on* a bounded fold-actuation; iter-2 grounded
that the fold-actuation cannot exist (strict first-match dispatch → no competing
candidates) and recommended deferring memo + completing the outer surface; the
operator chose defer; iter-3 developed this cut and grounded the snowflake proof
positive (opaque-sub-tree top-level reorder is invariant).

**Divergence surfaced (not papered over):** the conflict-memo admission line was
superseded by the operator's defer (and grounded as either redundant with the margin
advisory or requiring the too-big Cascades search). Codex iter-2 proposed an
*Evidence-Window Health Readout* (windowing-as-instrumentation) and notably
**dissolved the durable-format objection** via a separate window namespace with the
lifetime 220-matrix unchanged — a strong alternative, but it adds a hot-path
dual-write (substantial) and the operator steered to outer-completion this cycle.
**Windowing-health is the named leading next cut** (it makes the last dead cockpit
gate live and builds memo's "stable under evidence health" standing).

## Forward arc

`221 safe-side outer surface completion (this) → evidence-window health readout
(makes `windowing_decay_pressure` live + memo's stability standing) →
memo/Cascades when a production `conflicted` row + multi-candidate-search design
justify it → deployment elasticity signal; mixed-chain pushdown (needs its own
grounding cut)`.

## Deferred (named)

Memo/Cascades (deferred by operator — gated + multi-candidate search); evidence-window
health / windowing-decay durable-age substrate (the leading next cut, durable-format
objection now dissolved); within-chain / full-outer / mixed-outer reorder (each needs
its own proof); deployment elasticity signal; mixed-chain pushdown.
