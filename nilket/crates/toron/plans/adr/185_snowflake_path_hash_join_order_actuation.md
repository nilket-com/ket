# ADR 185: Hash-Snowflake Join Order Actuation

**Status:** Promoted. **Migration:** `plans/impl/185_snowflake_path_hash_join_order_actuation.md`.

182 made a snowflake's join order visible, 183 measured it, 184 judged it. 185 **acts**: it
reorders a uniform-hash snowflake's **root** level — the fact's direct join order — when the
confidence-gated model shows a robust win, and restores the declared output. It is the
snowflake sub-arc's first behavior-changing cut, scoped to the tractable, high-value case:
**shallow** (root-level) actuation, with branch sub-stars built in declared order and treated
as composite root dimensions. Recursive snowflake-branch reorder is the named follow-on (186).

The clean insight that bounds this cut: **an actuated snowflake root is a star.** After the
root is reordered it is a uniform-hash star whose dimensions happen to include composite
(sub-star) legs. So 185 is not a new actuator — it relaxes 181's actuator to accept composite
dimensions, and the actuated root reuses the 178–181 receipt and the 179/180 **star** verdict
(which already models actuation). Snowflakes that do not actuate stay on the 182–184
observational path. No new verdict semantics.

## Grounding

- **The branch-fold lives at the existing decline point.** In `try_bind_star_order_actuation`
  (`binder.rs:1771`), a dimension whose join key resolves to *another dimension* instead of the
  fact currently declines `MissingRelationLabel`. That is exactly the snowflake signal. 185
  turns it into a fold: the dimension becomes a **branch member** of the dimension its key
  references, and that parent dimension becomes a **composite branch**.
- **Composite dimensions reuse the whole reorder path.** `choose_join_strategy` already scores
  a `PlanOrigin::Composite` build (`build_byte_candidate` → `estimated_byte_size`), so 181's
  dual-preflight uniform-strategy check classifies a root with composite legs unchanged.
  `restore_declared_star_order_project` (`binder/join.rs:675`) permutes physical segments by
  `(label, width)` — a composite branch is simply one **wide** segment, restored intact.
  `bind_star_order_sequence` binds composite legs via `bind_scoped_join` like any dimension.
- **A composite branch's output size is order-independent**, so the root's strategy
  classification (and 181's strategy-invariance proof) holds over composite legs exactly as for
  base scans.
- **Branch composites must bypass join-order actuation internally.** The existing
  `bind_plan_with_trace` join path calls the flat-star actuator before falling back to ordinary
  join binding, even when source capture is scratch-only. A synthesized branch therefore needs a
  declared-branch helper that binds the branch base and members and chains them with ordinary
  scoped joins in recorded order. Recursive branch/star actuation is deferred to ADR 186.
- **Hash snowflakes execute today** via `execute_hash_join`; a reordered uniform-hash root
  still executes there. Binder-only — no `fragment.rs`/executor change.
- **The actuated root is a star, so the verdict is already solved.** 184's snowflake verdict is
  observe-only (3-variant) because it was built for un-actuated snowflakes. An actuated root has
  an *applied* order and needs the 5-variant star verdict (`OverEager`/`MisRanked` included).
  Reporting the actuated root through the star `join_order` receipt + 179/180 gives exactly
  that, with zero new verdict code.
- **The post-bind emit seam must not clobber an actuated root.** `bind_with_explain_trace`
  currently runs `analyze_snowflake` unconditionally and clears `join_order` on recognition
  (`binder.rs:178`). An actuated snowflake root still has branch-chaining edges, so
  `analyze_snowflake` would recognize it and clear the actuated star receipt. The fix: skip
  snowflake recognition when `join_order` is already a `Recognized` (actuated) star.

## Decisions

**D1 - Recognize the snowflake root at planning time (branch-fold), single-level only.**
Collect the left-deep spine dimensions; resolve each outer key source. A key sourced from the
**fact** is a root dimension; a key sourced from a **prior root dimension** is a branch member
of it; a root dimension with members is a **composite branch**. A key sourced from a **branch
member** (deeper nesting) → **decline** (deferred to 186). Produce root dimensions: `Leaf` or
`Branch { base, members }`.

**D2 - Build composite branch dimensions through a declared-branch helper.** For each `Branch`,
build the branch sub-plan (`base ⋈ members`, declared order) into a composite
`BoundStarDimension` (`label` = branch base label, `width` = branch column count, `on` =
fact↔base keys). The helper may capture source facts and edge evidence into scratch state, but it
must not pass the synthesized branch join back through the star/snowflake actuator. 185 does
**not** recursively actuate branch internals.

**D3 - Reorder the root only, via 181's machinery.** Apply 181 unchanged over the root
dimensions (leaf + composite): dual-preflight that declared and selected root orders classify
to the **same uniform strategy** (decline mixed/broadcast/strategy-drift), the same
confidence-scaled soft-margin gate, `bind_star_order_sequence`, and
`restore_declared_star_order_project` (composite branch = one wide segment). If a root edge's
estimate is missing (composite-right estimate unavailable), the root **declines** conservatively
(`MissingEstimate`) and keeps declared order.

**D4 - The actuated root reports as a star; the emit seam is guarded.** When the root reorders,
emit the existing star `join_order` receipt and let 179/180 measure and judge it. Guard the
182 emit seam: run `analyze_snowflake`/set `snowflake_order` **only when `join_order` is not a
`Recognized` star**, so an actuated root (flat or snowflake) keeps its star receipt and emits no
`join_order_snowflake` row.

**D5 - Un-actuated snowflakes are unchanged.** A snowflake whose root is mixed, broadcast,
deeper-than-single-level, or estimate-missing declines actuation (`Ok(None)` → normal
declared-order bind) and keeps the 182–184 observational receipts byte-identically. 185 only
adds behavior on the uniform-hash, single-level-branch, robust-win case.

**D6 - Result invariant (178-style).** Schema and column order are exactly restored to the
declared shape; the row multiset is unchanged. Tests use deterministic fixtures / explicit
`ORDER BY`. Binder-only; hash execution via `execute_hash_join`.

**D7 - No shell, no executor/fragment/cost-model/Wendelon change.** No `optimizer.rs`, memo,
Cascades, `PlanProperties`. No `rewrite_broadcast_star_probe` change (broadcast snowflake
remains the deferred cliff), no cost model, no new optimizer fact, no Wendelon/storage/PBC/wire
change. The change is the planning-time recognizer + reuse of the star actuator + the emit
guard.

## Headline Gates

- `hash_snowflake_root_reorders_on_robust_win` - a uniform-hash snowflake
  (`orders → {customers→regions branch, stores leaf}`) written root-suboptimal binds the root
  in preferred order and emits a star `join_order` receipt with `action=reordered`.
- `hash_snowflake_reorder_is_results_identical` - the reordered snowflake returns the declared
  schema/column order and the same row multiset as the declared-order baseline (executed via
  `execute_hash_join`).
- `hash_snowflake_branch_built_declared_order` - the composite branch sub-star is bound in
  declared internal order even when it would independently qualify as a flat-star reorder
  candidate (185 reorders the root only); the branch segment restores intact.

## Supporting Gates

- already-optimal / thin-margin roots keep declared order: 185 reuses
  `classify_star_order_action`, so 181's `star_order_evidence_agrees_when_declared_is_optimal`
  and `star_order_keeps_declared_when_soft_margin_is_thin` gates cover the kept-declared case
  unchanged.
- `mixed_or_broadcast_snowflake_root_declines_actuation` - a mixed/broadcast root declines and
  keeps the 182–184 observational path (no reorder).
- `deep_snowflake_declines_actuation` - a branch member with its own members declines (shallow
  scope; deferred to 186).
- `actuated_hash_snowflake_root_uses_star_verdict` - under `EXPLAIN ANALYZE` the actuated root
  yields a 5-variant star `join_order_verdict` (can be `OverEager`/`MisRanked`), not a 3-variant
  snowflake verdict, and emits no `join_order_snowflake` row.
- the 182–184 snowflake observational suite (`snowflake_order_evidence_is_observational`,
  `snowflake_runtime_evidence_is_observational`, and the `snowflake_verdict_*` tests) stays
  green: a declined snowflake still emits its `join_order_snowflake` + `..._runtime` +
  `..._verdict` rows byte-identically.
- `rune_hash_snowflake_root_reorders_same_as_sql` - Rune parity.

## Architecture Guards

- `adr185_snowflake_root_actuation_reuses_181_machinery` - the root reorder calls
  `preflight_star_order_strategy`, `bind_star_order_sequence`, and
  `restore_declared_star_order_project`; no second reorder/restore.
- `adr185_branch_fold_at_relation_label_boundary` - the recognizer folds a non-fact-sourced
  dimension into a branch instead of declining `MissingRelationLabel`.
- `adr185_branch_built_declared_without_actuator` - composite branch legs are bound through
  ordinary scoped joins in declared order and the synthesized branch join is not passed through
  `try_bind_star_order_actuation`.
- `adr185_actuated_root_reports_as_star` - the emit seam skips `analyze_snowflake` when
  `join_order` is `Recognized`; an actuated root sets the star receipt, not `join_order_snowflake`.
- `adr185_no_executor_fragment_costmodel_or_wendelon_change` - no executor, `fragment.rs`,
  `rewrite_broadcast_star_probe`, cost model, storage, PBC, wire, or Wendelon change.
- `adr185_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.
- `adr185_cockpit_is_frontend_shared` - SQL and Rune share the receipt path.

## Rejected Alternatives

**A new snowflake verdict for actuated roots.** Unnecessary and duplicative — an actuated root
is a star, so the 5-variant star verdict already models `OverEager`/`MisRanked`. 184's 3-variant
verdict stays for un-actuated snowflakes.

**Allow flat-star branch self-actuation.** Rejected for 185 because it would make this cut
recursive in practice and would invalidate the root-only safety claim. Branch-local actuation is
useful, but it needs its own receipts and result-preservation gates in ADR 186.

**Full recursive actuation now (all levels, nested restore).** Bigger; shallow-first proves the
composite-dimension + root-restore mechanics on the high-value case and de-risks the recursion.

**Post-bind physical rewrite of the recognized 182 tree.** Replays too much binder state, the
same reason 178 chose planning-time extraction.

**Broadcast-snowflake actuation.** The broadcast build of a composite branch hits
`rewrite_broadcast_star_probe`'s scan-build assumption — the deferred execution cliff. 185
declines broadcast roots.

## Non-Goals

- no recursive snowflake-branch reorder (186) or nested restore;
- no broadcast-snowflake actuation / `rewrite_broadcast_star_probe` change (the cliff);
- no mixed-strategy snowflake ordering (needs a strategy-aware cost model);
- no deeper-than-single-level branch recognition this cut;
- no galaxy / bridge / cyclic;
- no executor, fragment, storage, PBC, wire, catalog, or Wendelon change;
- no optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Consensus Trail

The council took the act, scoped to the bounded high-value case. The load-bearing insight is
that an actuated snowflake root is a star: 185 relaxes 181's actuator to accept composite branch
dimensions (built through ordinary scoped joins in declared branch order), reorders the root under
the same uniform-strategy preflight and declared-output restore, and reports the result through
the star receipt and the 5-variant star verdict — so there is no new verdict, no new
reorder/restore, and no executor change. The one new seam guard keeps an actuated root reporting
as a star; snowflakes that cannot actuate stay observational on 182–184.

## Forward Arc

`182 evidence -> 183 runtime -> 184 verdict -> 185 hash-snowflake root actuation -> 186
recursive branch actuation (nested restore) -> broadcast-snowflake execution cliff
(rewrite_broadcast_star_probe) -> galaxy/multi-fact -> bridge/many-to-many -> cyclic caps ->
memo only when receipts show decisions fighting`.

After 185 a uniform-hash snowflake's fact-join order self-optimizes within its safe envelope —
the first earned snowflake smartening, conservative by construction.

## Deferred (named)

Recursive snowflake-branch actuation + nested restore (186); broadcast-snowflake execution
cliff (`rewrite_broadcast_star_probe`); deeper-than-single-level branch recognition;
mixed-strategy snowflake ordering; galaxy / bridge / cyclic; always-on verdict accrual.
