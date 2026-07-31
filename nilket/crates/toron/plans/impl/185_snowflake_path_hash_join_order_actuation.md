# Impl Plan - ADR 185: Hash-Snowflake Join Order Actuation

Pairs with `plans/adr/185_snowflake_path_hash_join_order_actuation.md`.

Reorder a uniform-hash snowflake's **root** level, branches built declared as composite
dimensions, by relaxing 181's star actuator to accept composite legs and folding the
snowflake's non-fact-keyed dimensions into branches at the existing decline point. The
actuated root reports as a star (181 receipt + 179/180 verdict), guarded so the 182 emit seam
does not clobber it. Branch internals use declared scoped joins rather than recursive actuation.
Results-identical, binder-only; no executor / `fragment.rs` / cost-model / Wendelon change.
Recursive branch reorder deferred to 186.

## Anchors

| Concern | Anchor |
|---|---|
| Decline point → branch-fold | `try_bind_star_order_actuation` key-source check (`binder.rs:1771`, `MissingRelationLabel`) |
| Spine extraction | `extract_star_join_candidate` (`binder/join_order.rs`) |
| Dimension loop / branch construction | root-level dimension attachment reuses the 181 dim loop; composite branches are built by a declared helper so synthesized branch joins do not re-enter `try_bind_star_order_actuation` |
| Composite build scoring | `choose_join_strategy` / `build_byte_candidate` `PlanOrigin::Composite` (`binder/join.rs`) |
| Reorder machinery (reused) | `preflight_star_order_strategy`, `bind_star_order_sequence`, `restore_declared_star_order_project`, `star_output_segments_for_order` |
| Bound dimension | `BoundStarDimension { plan, on, strategy_intent, label, evidence }` |
| Star receipt / verdict (reused) | `recognized_star_order_outcome`; 179/180 analyze rows |
| Emit seam (guard) | `bind_with_explain_trace` (`binder.rs:178`) `analyze_snowflake` → clears `join_order` |
| Tests | `binder/tests.rs`; `tests/explain_analyze_test.rs`; `tests/architecture/join_order.rs` |

## Phase 1 - Snowflake-root recognition with branch-fold (`binder.rs` + `binder/join_order.rs`)

Partition the spine. For each spine dimension, resolve its outer key source via
`relation_label_for_join_keys(&probe_plan.scope, &left_resolved_keys)`:

- source is the **fact probe** → root dimension (as today);
- source is a **prior root dimension** `D` → branch member of `D`;
- source is a **branch member** or unresolved → **decline** (deep snowflake / non-star; 186 or
  keep declared).

Result for the root: `Vec<RootDimension>` where each is `Leaf(spine dim)` or `Branch { base,
members }`. Track root-vs-member membership so a member-of-a-member is detected and declines.

## Phase 2 - Build composite branch dimensions declared, without recursive actuation (`binder.rs`)

For each `Branch`:

- build the branch `base ⋈ members` in declared order with the recorded member `on` keys;
- do **not** pass a synthesized branch `LogicalPlan::Join` through `bind_plan_with_trace`; that
  path invokes `try_bind_star_order_actuation` before ordinary join binding and would make 185
  recursively actuate flat-star branches;
- bind the branch base and members through the existing leaf/subplan binder, capture source facts
  into scratch state where needed, and chain the declared branch edges with ordinary scoped join
  binding;
- wrap as composite `BoundStarDimension`: `plan` = branch plan, `on` = fact↔base keys, `label`
  = branch base label, `evidence` = fact↔branch edge estimate via `join_order_edge_estimate`
  (right input = branch output; composite-source fallback already exists). If the estimate is
  `None`, decline the whole root (`MissingEstimate`).

Leaf root dimensions bind exactly as today.

## Phase 3 - Reorder the root via 181 (`binder.rs`)

Reuse the existing actuation tail unchanged over the root `BoundStarDimension`s:

- `preflight_star_order_strategy(declared)` and `(selected)` → require equal uniform strategy
  (decline mixed/broadcast/strategy-drift → `Ok(None)`);
- confidence-gated soft-margin gate (`classify_star_order_action`);
- `bind_star_order_sequence` in the selected order;
- if reordered, `restore_declared_star_order_project` with `star_output_segments_for_order`
  (composite branch contributes one wide segment from its plan schema);
- set the star `join_order` receipt via `recognized_star_order_outcome(... applied_order ...)`.

## Phase 4 - Guard the emit seam (`binder.rs:178`)

```rust
if matches!(render_trace.join_order, Some(StarOrderOutcome::Recognized(_))) {
    // actuated star (flat or snowflake root): keep the star receipt; no observational snowflake row
} else {
    render_trace.snowflake_order =
        analyze_snowflake(&render_trace.join_order_edges, &trace.join_decisions);
    if render_trace.snowflake_order.is_some() {
        render_trace.join_order = None;
    } else if render_trace.join_order.is_none() {
        render_trace.join_order = analyze(&render_trace.join_order_edges);
    }
}
```

A declined snowflake leaves `join_order` as `Some(Declined)` or `None`, so it still flows into
`analyze_snowflake` and keeps the 182–184 path. Only a `Recognized` (actuated) root skips it.

## Phase 5 - Behavioral tests (`binder/tests.rs`, `tests/explain_analyze_test.rs`)

Fixture: `hash_snowflake_join_order_catalog` — `orders` fact, `customers→regions` branch,
`stores` leaf, large dims (uniform hash), facts making the root declared order non-optimal.

- `hash_snowflake_root_reorders_on_robust_win` - root binds preferred; star `join_order`
  receipt `action=reordered`.
- `hash_snowflake_reorder_is_results_identical` - executed (hash): declared schema + same
  sorted row multiset as declared-order baseline.
- `hash_snowflake_branch_built_declared_order` - branch bound in declared internal order and not
  self-actuated even when the branch would independently qualify as a flat-star reorder
  candidate; branch segment restored intact.
- kept-declared case covered by reuse of `classify_star_order_action`: 181's
  `star_order_evidence_agrees_when_declared_is_optimal` /
  `star_order_keeps_declared_when_soft_margin_is_thin` stay green.
- `mixed_or_broadcast_snowflake_root_declines_actuation` - declines; 182–184 path intact.
- `deep_snowflake_declines_actuation` - branch-of-branch declines (shallow scope).
- `actuated_hash_snowflake_root_uses_star_verdict` - EXPLAIN ANALYZE → 5-variant star
  `join_order_verdict`; no `join_order_snowflake` row for the actuated root.
- declined-snowflake observational path covered by the 182–184 suite staying green
  (`snowflake_order_evidence_is_observational`, `snowflake_runtime_evidence_is_observational`,
  `snowflake_verdict_*`): the three `join_order_snowflake*` rows remain byte-identical.
- `rune_hash_snowflake_root_reorders_same_as_sql` - Rune parity.

## Phase 6 - Architecture guards (`tests/architecture/join_order.rs`)

- `adr185_snowflake_root_actuation_reuses_181_machinery` - calls
  `preflight_star_order_strategy` / `bind_star_order_sequence` /
  `restore_declared_star_order_project`; no new reorder/restore fn.
- `adr185_branch_fold_at_relation_label_boundary` - the recognizer folds a non-fact-sourced
  dimension into a branch (no `MissingRelationLabel` decline for the single-level snowflake).
- `adr185_branch_built_declared_without_actuator` - branch legs use ordinary scoped join binding
  in declared order; synthesized branch joins do not route through the star/snowflake actuator.
- `adr185_actuated_root_reports_as_star` - emit seam skips `analyze_snowflake` when
  `join_order` is `Recognized`; actuated root sets the star receipt, not `join_order_snowflake`.
- `adr185_no_executor_fragment_costmodel_or_wendelon_change` - marker sweep; no executor /
  `fragment.rs` / `rewrite_broadcast_star_probe` / cost-model / Wendelon change.
- `adr185_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.
- `adr185_cockpit_is_frontend_shared` - SQL and Rune share the receipt path.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib hash_snowflake
scripts/cargo-cgroup test -p toron --lib star_order        # 177-181 unchanged
scripts/cargo-cgroup test -p toron --lib snowflake         # 182-184 observational path intact
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr185
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

## Risks

- **Results-identical is the headline.** The composite-branch restore must round-trip column
  order exactly. Pin with `hash_snowflake_reorder_is_results_identical` (executed hash) +
  `hash_snowflake_branch_built_declared_order`.
- **Emit-seam clobber.** Without the Phase 4 guard, `analyze_snowflake` clears the actuated
  star receipt. Pin with `adr185_actuated_root_reports_as_star` +
  `actuated_hash_snowflake_root_uses_star_verdict`.
- **Strategy invariance over composite legs.** The dual-preflight must classify uniform strategy
  with a composite build; decline mixed/broadcast. Pin with
  `mixed_or_broadcast_snowflake_root_declines_actuation`.
- **Branch-fold scope.** Deep snowflakes (branch-of-branch) decline, not mis-build. Pin with
  `deep_snowflake_declines_actuation`.
- **Accidental recursive actuation.** Binding a synthesized branch join through the top-level join
  binder would let flat-star branch actuation fire in 185. Pin with
  `adr185_branch_built_declared_without_actuator` and the declared-order behavior gate.
- **Branch estimate fidelity.** Missing fact↔branch estimate → conservative root decline, never
  a wrong order. No new estimator.
- **Star/observational paths unchanged.** 181 flat-star + 182–184 snowflake observational must
  stay byte-identical. Pin with green `star_order` + the `snowflake` observational suite.

## Deferred

Recursive snowflake-branch actuation + nested restore (186); broadcast-snowflake execution
cliff (`rewrite_broadcast_star_probe`); deeper-than-single-level branch recognition;
mixed-strategy snowflake ordering; galaxy / bridge / cyclic; always-on verdict accrual.
