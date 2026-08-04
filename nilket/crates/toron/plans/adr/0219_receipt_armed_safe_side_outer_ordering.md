# Toron ADR 219: Receipt-Armed Safe-Side Outer Ordering

**Status:** Promoted. **Migration:** `plans/impl/0219_receipt_armed_safe_side_outer_ordering.md`.
**Category:** Toron optimizer actuation - the first **non-inner** receipt-gated
behavior rotation, and the actuation payoff of the 209-214 preserved-outer arc.
No durable evidence-format change, no new verdict/decision kind, no new
counter/key/table/`SystemTableKind`, no wire / executor / fragmenter / Wendelon
change. `PreservedOuterOrder` stays verdict-only (216 untouched).

## Why this cut exists

The join-order reorder machinery is **inner-only**: recognizers reject
`NonInnerEquiJoin` (join_order.rs:1241/1338/1741); `preserved_outer_order` exists
precisely because outer edges are never reordered. 209-214 built the reach
(demotion of null-rejected outers) and the counterfactual instrumentation; 217
turned the missed-win signal into the `preserved_outer_ordering` gate. 219 takes
the payoff: it reorders **one provably-safe outer shape** when that gate is
`actionable`, and declines everything else - the first time the optimizer relaxes
the non-inner fence, done under a semantic proof.

This is the riskiest rotation in the arc (it can change query *results* if wrong),
so the cut leads with the proof and an **executing** result-equivalence gate, not
plan plumbing.

## The semantic proof (what makes the subset safe)

Permitted shape - a **left-outer star** around one preserved anchor:
`A LEFT JOIN D1 ON A.k1=D1.k1 LEFT JOIN D2 ON A.k2=D2.k2 ...` where every reordered
edge is `LeftOuter`, every edge preserves the **same** anchor `A` on the left,
every null-supplied side is a base dimension leaf, every predicate is an equi key
between `A` and exactly one `D_i`, and no predicate spans two dimensions or
null-rejects the padded side.

Under those fences the output multiset is invariant to dimension probe order:
- every anchor row survives every `A LEFT JOIN D_i` (null-padded on miss), so the
  surviving `A`-row multiset is identical for any order of the `D_i`;
- each `D_i`'s matched-or-null values depend only on `A.k_i=D_i.k_i`, not on any other
  `D_j` (no cross-dimension predicate), so each `A`-row gets the same values in any
  order;
- therefore reordering changes only physical evaluation order, not row survival or
  values. The reorder is result-preserving **by construction**; the declared output
  schema/column order is restored after the physical reorder.

This proof is also why the scope must stay narrow: full/right/mixed outer joins
and outer chains need their own proofs and are fenced.

## Grounding (verified)

- The 214 receipt `preserved_outer_order_receipt` (binder.rs:409) is an
  **inner-equi counterfactual**: it sets `inner_equi = true` on outer edges to
  *measure* the missed win. Valid as a cost/ranking signal; **unsafe as a plan**
  (applying its `edge_targets` drops null-supplied rows).
- The 217 gate `preserved_outer_ordering_gate` (join_order.rs:646) derives its
  status from `margin_advisory_report_for_scope(PreservedOuterOrder)`. That
  report's `advisory` already carries the `Loosen` recommendation and the
  recommended soft-margin step; 219 makes that typed gate result the D7 carrier.
- 218 established a single bind-entry `receipt_gate_snapshot()` resolver for
  margin policy; 219 extends it to one policy bundle (D6), adding no second read.
- `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220`
  (optimizer_accrual.rs:295/303) asserts `!recording_kinds.contains("PreservedOuterOrder")`
  and `decision_keys.len() == 140` - recording an outer-order decision would invert
  it, so this cut adds no decision recording.

## Decisions (promotion-locked)

**D1 - Receipt-armed, opt-in only.** Fires only under `ApplyAdvisory` and only when
the shared `preserved_outer_ordering` gate is `actionable` (Loosen, above floor 20).
`Manual`, cold ledger, insufficient/non-Loosen evidence -> byte-identical. Manual
keeps the existing no-evidence-read behavior: it resolves the base policy without a
receipt-gate snapshot.

**D2 - Left-outer star only (the proof above); decline otherwise.** Full/right/mixed
outer, snowflake/bridge/galaxy, composite-build dimensions, cross-dimension or
nullable-residual predicates all decline and bind exactly as today.

**D3 - The 214 counterfactual is evidence, not a transform.** Reuse the same
estimate/ranking machinery that produced its cost signal; never actuate the
inner-equi `edge_targets`, never call the receipt helper as the live transform, and
never relax the inner-star `NonInnerEquiJoin` fences.

**D4 - Build a real left-outer sequence, near the star machinery.** A new
`try_bind_preserved_outer_star_actuation` beside `bind_star_order_sequence`
(join_order.rs:3954) - not a generic `binder.rs` join branch. It binds selected
dimensions with `JoinKind::LeftOuter`, preflights declared and selected orders as
outer joins, and **restores declared output schema/column order** after any physical
reorder.

**D5 - One shared typed gate result.** Promote `preserved_outer_ordering_gate` to a
typed `PreservedOuterOrderingGate { status, advisory }` consumed by both the 217
`optimizer_receipt_gates` row builder (a wrapper) and the actuator - one predicate,
no drift.

**D6 - One bind-entry policy snapshot (extends 218).** Under `ApplyAdvisory`, resolve
an `EffectiveJoinOrderPolicy { tree_step, graph_steps, preserved_outer_gate }` from
exactly one `receipt_gate_snapshot()` at binder construction, threaded through
normal, cloned, and no-op binders. Under `Manual`, build the base policy with zero
receipt-gate snapshots. The actuator never queries `information_schema` and never
takes its own evidence snapshot; the information-schema readout remains a separate
report path when that table is queried.

**D7 - Actionable preserved-outer uses the gate advisory's recommended step.** The
actuator's preserved-outer ranking margin is `advisory.recommended_step`, not the
base margin (else armed-yet-inert in exactly the missed-win cases) and not a
recomputed margin (else readout/policy drift). The shared gate result carries it.

**D8 - No durable/wire-surface expansion.** No new verdict/decision kind, durable
key, counter, table, `SystemTableKind`, decision-matrix count, executor, fragmenter,
wire, or Wendelon surface. `PreservedOuterOrder` remains verdict-only; no
`accrue_outer_order_decision`, and the outer actuator must not reuse the existing
inner `accrue_star_order_*` helpers under `FlatStar`/`Snowflake*` decision kinds.

## Honesty gates (discriminating)

- **Cardinal - `safe_side_left_outer_star_preserves_rows_and_schema`:** EXECUTE a
  left-outer star with null-supplied **misses** and at least one multi-match
  dimension; result row multiset + schema equal the fenced declared-order plan.
  Plan-diff alone is not proof; this catches a semantics break.
- `safe_side_left_outer_star_reorders_when_gate_actionable` - actionable MissedWin
  changes physical dimension order.
- `preserved_outer_ordering_inert_until_gate_actionable` - cold/below-floor/non-Loosen
  -> byte-identical (self-arming pin).
- `manual_ignores_preserved_outer_ordering_gate`.
- `selected_order_uses_left_outer_preflight_not_inner_receipt_targets` - fails if the
  inner-equi counterfactual is the live transform (D3).
- `preserved_outer_full_right_cross_and_residual_shapes_stay_fenced` - unsafe shapes
  decline, bind as today.
- `preserved_outer_actuator_and_receipt_table_share_gate_result` (D5).
- `preserved_outer_policy_uses_gate_recommended_step` (D7 - fails if policy keeps the
  base margin after a Loosen recommendation).
- `rune_preserved_outer_ordering_matches_sql`.

## Architecture guards (promotion-locked)

- `adr219_outer_actuation_is_left_outer_star_only`
- `adr219_outer_actuation_uses_left_outer_sequence_not_inner_targets`
- `adr219_preserved_outer_gate_result_shared_by_readout_and_policy`
- `adr219_join_order_policy_uses_one_receipt_snapshot`
- `adr219_preserved_outer_policy_uses_gate_recommended_step`
- `adr219_manual_and_cold_ledger_are_byte_identical`
- `adr219_outer_actuation_does_not_record_inner_decisions`
- `adr219_no_new_durable_key_counter_table_kind_or_decision_matrix`
- `adr219_no_executor_fragmenter_wire_or_wendelon_surface`
- `adr219_decision_conflict_gate_remains_non_consumer`

**Reconciled prior guards (named):**
- `adr217_readout_is_read_only_no_policy_consumers` -> now allows **exactly two**
  consumers: 218 `graph_margin_divergence` + 219 `preserved_outer_ordering`;
  decision-conflict/windowing/counter-handoff stay non-consumers.
- ADR 217 preserved-outer gate guard -> the typed gate result, Loosen-only
  actionability unchanged.
- `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` -> **unchanged and
  asserted unchanged** (the load-bearing reconciliation; `PreservedOuterOrder` stays
  out of the decision lane, 140/220 intact).
- ADR 214 no-actuation wording -> narrows to allow only this left-outer-star
  receipt-armed policy; its opportunity-class/replay-id/durable-matrix guards intact.
- ADR 218 graph-kind guards -> unchanged; graph policy must keep rejecting
  preserved-outer reads.

The guard retarget surface is not just `optimizer_rotation.rs`: existing
`optimizer_accrual.rs` ADR191/208/213/215/217 checks and ADR218 rotation checks pin
the policy resolver and inner-only actuation helpers by exact names. The implementation
may preserve compatibility shims, but any renamed resolver/field/seam must be retargeted
there and proven by the full `architecture_test` binary.

## Consensus (transparent)

3-of-3 + operator on topic A (safe-side outer ordering, not the decision-conflict
readout). Shape: Claude iter-0 raised the load-bearing correction (the 214 receipt
is an inner-equi cost-probe, unsafe as a reorder); Codex iter-1 adopted it and
contributed the left-outer-star subset, the star-local seam, and the
no-durable-decision constraint; Claude iter-1 formalized the row-survival proof and
resolved three Gemini iter-1 divergences (general scope, counterfactual-reuse,
durable-decision recording) against three verified facts: the proof, the inner-equi
receipt, and the 216 matrix guard. Codex iter-2 added the final two locks (one
bind-entry policy bundle, D6; gate-recommended-step as the actuator margin, D7);
Gemini iter-2 adopted the full shape (left-outer-star, signal-not-plan, verdict-only,
schema restoration). No residual divergence.

## Forward arc

`219 receipt-armed safe-side left-outer-star ordering (this) -> wider safe-side outer
shapes only with their own semantic proof -> decision-conflict detail readout -> memo
-> windowing/decay durable-age substrate / elasticity signal as their gates become
computable`.

## Deferred (named)

Full/right/mixed outer reordering and outer chains (each needs its own proof);
snowflake/bridge/galaxy outer shapes; decision-conflict detail readout (the smaller
alternative, the next instrumentation cut); windowing/decay durable-age substrate;
deployment elasticity signal; mixed-chain pushdown; memo/Cascades.
