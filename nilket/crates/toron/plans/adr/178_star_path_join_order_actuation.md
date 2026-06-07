# ADR 178: Star-Path Join Order Actuation

**Status:** Promoted. **Migration:** `plans/impl/178_star_path_join_order_actuation.md`.

ADR 177 made strict star join order visible. ADR 178 makes that evidence act, but only
inside the safe star-path envelope: a left-deep inner broadcast star, complete evidence,
and a confidence-scaled improvement gate. When the gate fires, Toron applies dimensions in
the preferred order and restores the declared output schema/ordinal layout. Otherwise it
keeps the user's declared order and says why.

This is the first behavior-changing optimizer cut in the beyond-star arc. It is still not
Cascades, not a memo, not a broad `PlanProperties` contract, and not snowflake/general
graph planning. One evidence stream becomes one local actuator.

### Why the gate is a soft margin, not a hard guarantee

A clean FK-to-PK star join is usually row-non-increasing:
`join_output_ndv_containment_estimate` yields `|L| * |R| / max(ndv)` close to probe rows
when the dimension side is unique. The reorder win is therefore an expected-case
optimization: apply the most selective dimensions early so the intermediate stream shrinks
sooner. That selectivity is NDV-driven and may be soft.

So a hard-bound-only gate was considered and rejected. It would often fail to fire on the
queries ADR 178 is meant to improve. The honest gate requires the expected win to clear a
confidence-scaled margin large enough that plausible estimate error is unlikely to flip
the decision. The margin can be tuned later from observed estimate-vs-actual telemetry.

## Grounding

- **ADR 177 already owns the vocabulary.** `binder/join_order.rs` has relation facts,
  edge estimates, `StarOrderOutcome`, declared/preferred order, costs, and decline
  reasons. ADR 178 extends that module instead of adding a second optimizer model.
- **ADR 177 evidence is post-bind/EXPLAIN-shaped.** Today `bind_with_explain_trace` calls
  `analyze(&render_trace.join_order_edges)` after the joins are already bound, and scans
  capture join-order sources only when `render_trace.is_some()`. Actuation cannot depend
  on cockpit rows. It needs a planning-time star candidate path.
- **Post-bind physical rewrite is the wrong first cut.** `bind_join` allocates
  `JoinReplayId`s, pushes strategy receipts, inserts exchanges, applies right-build swap
  repair, computes scopes, and merges facts while building each edge. Rebuilding an
  already-bound physical subtree after all of that would replay too much binder state.
  The smaller and safer shape is planning-time extraction, then ordinary join binding in
  the chosen order.
- **Broadcast-only is the strategy-invariant envelope.** Reordering hash/non-broadcast
  stars lets order and strategy interact. In a broadcast star, each dimension remains a
  broadcast build onto the scan-linear probe. That preserves the execution topology and
  keeps `fragment.rs:80` untouched.
- **Column order is the correctness hinge.** Inner joins are reorderable for row
  semantics, but physical segment order changes `SELECT *`. ADR 178 must explicitly
  project the reordered star back to declared segment order.
- **177's no-cathedral guard still holds.** No `optimizer.rs`, `struct Optimizer`,
  `PlanProperties`, Cascades, or memo.

## Decisions

**D1 - Act only on strict broadcast stars.** Candidate shape:

- one probe relation;
- two or more dimension relations;
- left-deep `LogicalPlan::Join` chain;
- `JoinKind::Inner`;
- equi keys only;
- `JoinStrategyIntent::Auto` on every edge;
- all edges remain broadcast-viable under the selected sequence;
- scan-linear probe fence holds;
- every dimension resolves to one unique source label.

Anything else keeps declared order.

**D2 - Planning-time extraction, not cockpit-driven planning.** Add a planning-time star
extractor and actuator in `binder/join_order.rs`. It identifies the logical star block,
binds the probe and dimension legs with local join-order source capture, scores the
dimension edges, classifies the action, and then builds ordinary joins in either declared
or preferred order. `OptimizerExplainRenderTrace` renders the decision; it is never the
input to the decision.

**D3 - Confidence-scaled soft-margin gate.** Reorder only when the preferred order is
robustly better than the declared order:

- `orders_agree` -> keep declared;
- missing estimate -> decline/keep declared;
- hard-confidence win -> reorder on any positive model win;
- soft-confidence win -> reorder only when it clears a named robustness margin.

Hard confidence is derived from hard row/NDV evidence. Soft confidence covers
NDV-containment/sketch-derived estimates. The exact margin constant is local to
`binder/join_order.rs` and rendered in the receipt.

**D4 - Reuse ordinary join construction.** The actuator chooses only the dimension order.
Each physical edge still uses the existing join strategy chooser, exchange insertion,
right-build swap handling, replay eligibility, `JoinReplayId` allocation, and strategy
receipt path. ADR 178 may refactor `bind_join` into "bind children" plus "build one
already-bound scoped join", but it must not add a second physical join builder.

**D5 - Restore declared star output explicitly.** If the applied order differs from the
declared order, wrap the resulting star in a named N-segment projection restore. The final
schema, logical type metadata, field names, and downstream binder ordinals must match the
declared query shape. This generalizes the existing two-side right-build restore concept
to a probe-plus-N-dim segment permutation.

**D6 - Result invariant replaces ADR177's plan-byte invariant.** ADR177's headline was
"plan byte-identical." ADR178 intentionally ends that for the positive gated case. The new
invariant is: schema and column order are exact, and row multiset is unchanged. Tests may
use deterministic fixtures or explicit `ORDER BY` to compare concrete batches, but the ADR
does not rely on accidental hash-join output order.

**D7 - Receipts report action, not just preference.** The `join_order` cockpit row shows
declared order, preferred order, applied order, costs, confidence, margin, and action:

- `reordered`;
- `kept_already_optimal`;
- `kept_insufficient_margin`;
- `declined_<reason>`.

SQL `EXPLAIN` and Rune `explain()` share the rendering path.

**D8 - Retarget ADR177 guards explicitly.** 178 inverts two old truths:

- star-order evidence is no longer purely observational when the positive gate fires;
- join-order source capture is no longer only an EXPLAIN-render detail for candidate star
  planning.

The surviving guards assert the new truths: cockpit rendering is still EXPLAIN-gated,
planning does not consume cockpit rows, action lives in `binder/join_order.rs`, declines
keep declared order, and no executor/storage/Wendelon surface changes.

**D9 - No optimizer shell.** No `src/optimizer.rs`, no `struct Optimizer`, no memo, no
Cascades, and no broad `PlanProperties`. The abstraction is only the star-order actuator
earned by this consumer.

## Headline Gates

- `star_order_reorders_non_optimal_broadcast_star_to_preferred` - the ADR177 three-dim
  fixture binds the star in preferred dimension order when the gate clears.
- `star_order_reorder_preserves_schema_and_rows` - the reordered query exposes the
  declared schema/field order and returns the same row multiset as the declared-order
  baseline.
- `star_order_receipt_reports_reordered_action` - `EXPLAIN` shows declared, preferred,
  applied order, costs, confidence, margin, and `action=reordered`.

## Supporting Gates

- `star_order_keeps_declared_when_already_optimal` - no reorder and receipt says
  `kept_already_optimal`.
- `star_order_keeps_declared_when_soft_margin_is_thin` - a soft preferred win within the
  robustness margin keeps declared order.
- `star_order_declines_missing_facts` - missing row/NDV estimate keeps declared order.
- `star_order_declines_non_broadcast_or_non_star` - hash/non-broadcast star, outer join,
  non-equi join, duplicate dimension, or scan-linear fence violation keeps declared order.
- `star_order_replay_ids_and_receipts_follow_applied_sequence` - replay IDs stay
  consecutive and strategy receipts describe the physical applied order.
- `projected_star_reorder_restores_downstream_ordinals` - projection/aggregate/order-by
  over the reordered star resolves columns as if the declared order had been bound.
- `rune_star_order_reorders_same_as_sql` - Rune reaches the same action and receipt path.

## Architecture Guards

- `adr178_join_order_actuation_lives_in_join_order_module` - extraction, scoring,
  confidence gate, and action classification live in `binder/join_order.rs`.
- `adr178_planning_does_not_consume_cockpit_rows` - no planning decision reads
  `OptimizerExplainRenderTrace` or rendered `join_order` text.
- `adr178_reuses_177_estimate_vocabulary` - the actuator consumes the ADR177 relation/edge
  fact vocabulary and does not add a second estimator.
- `adr178_gate_is_confidence_scaled` - the gate reads confidence/source information and
  has hard and soft paths.
- `adr178_declared_output_restore_is_explicit` - reordered star output is repaired by a
  named N-segment restore helper.
- `adr178_broadcast_star_only` - actuation requires broadcast viability and declines
  hash/non-broadcast stars.
- `adr178_retargeted_177_observational_guard` - prior no-action guard is replaced by
  gated-action plus keep/decline invariants.
- `adr178_no_executor_fragment_storage_or_wendelon_change` - no executor, PBC,
  `fragment.rs`, storage, catalog wire, or Wendelon changes.
- `adr178_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.
- `adr178_cockpit_is_frontend_shared` - SQL and Rune render through the same optimizer
  explain row.

## Rejected Alternatives

**Post-bind physical transform.** Broadcast-only and confidence gating are correct, but a
post-bind transform is not the right implementation shape for this cut. Replay IDs,
exchange insertion, strategy receipts, right-build restore, scopes, and facts are all
produced during `bind_join`. A post-bind reorder would need to reconstruct too much of
that state. Planning-time extraction lets the existing binder do the physical work once,
in the chosen order.

**Hard-bound-only gate.** Rejected because FK-to-PK stars are often row-non-increasing in
every dimension order; the win is expected intermediate shrinkage from NDV selectivity, so
a hard-only gate would rarely actuate.

**Always-on telemetry before action.** Useful later, but ADR177 was already the observe
rung. ADR178 should pay it with one conservative actuator.

## Non-Goals

- no hash/non-broadcast star reordering;
- no pinned-hint star reordering;
- no outer, semi, anti, snowflake, bridge, many-to-many, cyclic, or general graph order;
- no bounded enumeration beyond ADR177's greedy preferred order;
- no always-on telemetry or measured estimate accuracy loop;
- no new optimizer fact or Wendelon producer;
- no executor, fragment, storage, PBC, wire, or catalog change;
- no optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Consensus Trail

The council took the actuation step over more telemetry: act on 177's evidence, safely.
Final convergence is active strict-star actuation. Gemini and Codex converged on
planning-time extraction and explicit output restoration. Claude supplied the important
safety tightening: broadcast-only strategy invariance plus a confidence-scaled gate. The
final shape adopts both: plan before physical binding, act only in the broadcast-star
envelope, restore declared output shape, and render the action.

## Forward Arc

`177 evidence -> 178 broadcast-star actuation -> measured/order-quality telemetry or gate
tuning -> hash/non-broadcast star strategy-aware ordering -> snowflake/chain evidence ->
bounded acyclic enumeration -> outer-join reorder safety -> relax left-deep execution
topology -> bridge/many-to-many -> cyclic caps -> memo only when receipts show local
decisions fighting`.

After 178 the star path is not just visible but self-optimizing within its safe envelope:
the first earned smartening, conservative by construction.

## Deferred (named)

Always-on join-order telemetry; margin tuning from observed estimate-vs-actual; pinned
hint support; hash/non-broadcast star ordering; snowflake/chain evidence and actuation;
bounded enumeration; outer-join reorder safety; broad plan properties; execution topology
relaxation; memo search.
