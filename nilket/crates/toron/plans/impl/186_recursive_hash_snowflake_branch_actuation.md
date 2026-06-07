# Impl Plan - ADR 186: Recursive Hash-Snowflake Branch Actuation with Edge-Owned Star Receipts

Pairs with `plans/adr/186_recursive_hash_snowflake_branch_actuation.md`.

Implement recursive hash-snowflake branch actuation by introducing an exclusive join-order
render state, making star receipts plural and edge-owned, extracting a private local-star
actuator from the 181/185 tail, and replacing 185's declared branch builder with a recursive
tree builder. Keep snowflake actuation hash-only, root-gated, binder/explain-only, and
results-identical.

## Anchors

| Concern | Anchor |
|---|---|
| Current invalid-state shape | `OptimizerExplainRenderTrace { join_order: Option<StarOrderOutcome>, snowflake_order: Option<SnowflakeOrderEvidence> }` in `binder/explain.rs` |
| 185 branch seam | `StarRootDimension`, `member_indices`, `bind_declared_star_branch_dimension` in `binder.rs` |
| Current star tail | `preflight_star_order_strategy`, `bind_star_order_sequence`, `restore_declared_star_order_project`, `recognized_star_order_outcome` |
| Edge evidence capture | `bind_scoped_join` pushes `JoinOrderEdgeEvidence { target: JoinTraceTarget { join_index }, ... }` |
| Runtime spend path | `optimizer_analyze_rows` currently matches one `StarOrderEvidence` against all `join_order_edges` |
| Observe-only snowflake path | `analyze_snowflake`, `optimizer_analyze_snowflake_*`, `measured_snowflake_order_verdict` |
| Broadcast cliff | `distributed/fragment.rs::rewrite_broadcast_star_probe` |
| Tests | `binder/tests.rs`, `tests/explain_analyze_test.rs`, `tests/architecture/join_order.rs` |

## Phase 1 - Exclusive render state (`binder/explain.rs`)

Add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderRenderReceipt {
    pub(super) probe_label: String,
    pub(super) outcome: StarOrderOutcome,
    pub(super) edge_targets: Vec<JoinTraceTarget>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) enum JoinOrderRenderState {
    #[default]
    None,
    StarReceipts(Vec<StarOrderRenderReceipt>),
    ObservedSnowflake(SnowflakeOrderEvidence),
}
```

Change `OptimizerExplainRenderTrace` to carry:

```rust
pub(super) join_order: JoinOrderRenderState,
```

Retarget helpers:

- `record_star_order_decline` sets or pushes into `StarReceipts`;
- recognized flat-star/root/branch actuation pushes `StarOrderRenderReceipt`;
- post-bind fallback sets `ObservedSnowflake(evidence)` only when the state has no recognized
  star receipts;
- declined-only star candidates may be replaced by `ObservedSnowflake` if `analyze_snowflake`
  recognizes the actual declared tree;
- source guards assert there are no independent star/snowflake option fields.

If existing flat-star decline semantics require exactly one decline row, use a small helper that
replaces a prior declined receipt for the same level rather than accumulating duplicates.

## Phase 2 - Explain/analyze render over the state (`binder/explain.rs`)

`optimizer_explain_plan`:

- `None` -> no join-order summary row;
- `ObservedSnowflake(evidence)` -> existing `join_order_snowflake` row;
- `StarReceipts(receipts)` -> render each receipt through the existing
  `optimizer_explain_join_order_row(&receipt.outcome)`.

`optimizer_analyze_rows`:

- `ObservedSnowflake(evidence)` -> existing 182-184 snowflake runtime/verdict path unchanged;
- `StarReceipts(receipts)` -> for each recognized receipt:
  - build a `BTreeSet<JoinTraceTarget>` from `edge_targets`;
  - filter `render_trace.join_order_edges` to those targets only;
  - match dimensions against that receipt's `StarOrderEvidence`;
  - emit `join_order_runtime` rows for those owned edges;
  - emit one `join_order_verdict` row for that receipt;
- `None` and declined-only receipts in analyze -> no runtime/verdict rows, matching current
  declined-star behavior.

Retarget `star_order_runtime_replay_ids` to collect replay IDs from recognized `StarReceipts`
edge targets only.

## Phase 3 - Snowflake tree classification (`binder.rs`)

Replace the 185 single-level `StarRootDimension` representation with a private tree model:

```rust
struct SnowflakeStarNode<'a> {
    dimension_index: usize,
    children: Vec<usize>,
    on: &'a [(Expr, Expr)],
    strategy_intent: JoinStrategyIntent,
    label: String,
    declared_position: usize,
}

struct SnowflakeStarTree<'a> {
    root_children: Vec<usize>,
    nodes: Vec<SnowflakeStarNode<'a>>,
}
```

Algorithm:

1. bind the fact probe and all dimension inputs in scratch, as 185 does;
2. resolve each candidate edge with `try_resolve_star_dimension_edge`;
3. determine parent label from the left side and child label from the right side;
4. attach fact-sourced children to `root_children`;
5. attach dimension-sourced children under their parent node;
6. reject duplicate labels, missing labels, shared parents, cycles, unknown parents, non-inner
   joins, non-equi joins, and ambiguous/non-tree shapes.

Keep this bounded to `extract_star_join_candidate`; no generalized join graph.

## Phase 4 - Private local-star actuator (`binder.rs`)

Factor the duplicated evidence/rank/preflight/bind/restore tail into a helper over bound inputs:

```rust
struct LocalStarActuation {
    plan: ScopedExecutionPlan,
    receipt: Option<StarOrderRenderReceipt>,
    strategy: Option<StarOrderStrategy>,
    action: Option<StarOrderAction>,
}
```

Helper responsibilities:

- accept `probe_plan`, `probe_label`, local `BoundStarDimension`s, local edge count, trace, and
  optional render trace;
- compute `JoinOrderDimEvidence`, confidence, preferred order, selected order, and applied
  order exactly as 181/185 do;
- run `preflight_star_order_strategy` for declared and selected orders;
- for snowflake-level actuation require equal `StarOrderStrategy::HashPartitioned`;
- record the local `join_order_edges` start offset before binding;
- bind through `bind_star_order_sequence`;
- collect `edge_targets` from the new local edges only;
- restore with `restore_declared_star_order_project` if reordered;
- return a recognized receipt for recognized hash levels, a declined receipt for local
  branch/root decline when useful, or no receipt for levels with fewer than two children.

Do not accept `LogicalPlan`; do not call `bind_plan_with_trace` on synthesized branch joins.

## Phase 5 - Recursive branch builder (`binder.rs`)

Replace `bind_declared_star_branch_dimension` with a recursive builder:

```rust
fn bind_snowflake_branch_dimension(...)
    -> Result<(ScopedExecutionPlan, Vec<StarOrderRenderReceipt>), BindingError>
```

For each node:

1. start from the already-bound base dimension input;
2. recursively build child nodes into restored branch plans;
3. convert child nodes/leaves into local `BoundStarDimension`s with evidence for
   base-to-child edges;
4. if fewer than two local children, chain them declared with `bind_scoped_join` and return no
   local receipt;
5. if at least two local children, call the local-star actuator;
6. if recognized hash, return the restored plan plus its receipt and any child receipts;
7. if locally declined, bind declared and return the declined receipt only in the committed
   real pass; preflight/scratch must not leak receipts.

The returned branch plan must be in local declared schema/scope order.

## Phase 6 - Root-gated commit (`binder.rs`)

The root path should run in two passes as 185 already does:

1. scratch bind all leaf inputs and recursively plan branches;
2. run the local-star actuator for the root;
3. if the root is not a recognized hash receipt, discard scratch branch receipts/plans and
   return `Ok(None)`;
4. if root is recognized hash, rebind for real:
   - recursively build branches;
   - bind root selected order;
   - restore root declared output;
   - commit `JoinOrderRenderState::StarReceipts(root_and_branch_receipts)`.

In `bind_with_explain_trace`, run `analyze_snowflake` only when the state has no recognized
star receipts. If `analyze_snowflake` recognizes an observe-only snowflake, replace the state
with `ObservedSnowflake`.

## Phase 7 - Behavioral tests (`binder/tests.rs`, `tests/explain_analyze_test.rs`)

Fixtures:

- bushy hash snowflake: `orders -> {customers -> {regions, segments}, stores}`;
- branch order declared non-optimal and root order declared non-optimal;
- deterministic storage fixture with explicit `ORDER BY`;
- broadcast/mixed root fixture for observe-only fallback;
- branch-local decline fixture with hash-safe root.

Tests:

- `recursive_hash_snowflake_actuates_root_and_bushy_branch`.
- `recursive_hash_snowflake_nested_restores_preserve_results`.
- `recursive_branch_receipts_are_edge_owned`.
- `root_decline_keeps_observed_snowflake_state`.
- `join_order_render_state_excludes_star_plus_observed_snowflake`.
- `chain_snowflake_emits_single_root_receipt`.
- `branch_decline_keeps_branch_declared_but_root_can_actuate`.
- `flat_star_receipt_shape_is_byte_identical`.
- `hash_snowflake_root_receipt_shape_is_byte_identical`.
- `declined_snowflake_observational_suite_stays_green`.
- `rune_recursive_hash_snowflake_reorders_same_as_sql`.

Retarget 185's `deep_snowflake_declines_actuation` to the positive recursive bushy/deeper case
where hash-safe, and keep a negative non-tree/cycle/shared-parent decline test.

## Phase 8 - Architecture guards (`tests/architecture/join_order.rs`)

- `adr186_join_order_render_state_is_exclusive`.
- `adr186_star_receipts_are_plural_and_edge_owned`.
- `adr186_local_star_actuator_reuses_181_machinery`.
- `adr186_analyze_spends_only_receipt_owned_edges`.
- `adr186_restores_compose_without_nested_helper`.
- `adr186_branch_recursion_uses_private_helpers_not_synthesized_top_level_binder`.
- `adr186_hash_only_no_broadcast_fragment_change`.
- `adr186_no_optimizer_shell_or_planproperties_contract`.
- `adr186_cockpit_is_frontend_shared`.

Retarget affected 177-185 guards that source-match `join_order: Option<StarOrderOutcome>` or
assume only one star receipt.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib recursive_hash_snowflake
scripts/cargo-cgroup test -p toron --lib hash_snowflake
scripts/cargo-cgroup test -p toron --lib star_order
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr186
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

## Risks

- **Invalid cockpit state.** Separate star/snowflake fields can both be populated. Pin with the
  exclusive enum and `join_order_render_state_excludes_star_plus_observed_snowflake`.
- **Receipt ambiguity.** Recursive behavior without edge-owned receipts makes verdicts
  ambiguous. Pin with `recursive_branch_receipts_are_edge_owned`.
- **Synthetic-plan recursion.** Re-entering the top-level binder on synthesized branches risks
  double receipts and scratch leaks. Pin with the private-helper architecture guard.
- **Branch-only actuation on root decline.** Pin with `root_decline_keeps_observed_snowflake_state`.
- **Nested restore drift.** Pin with result-identical and parent-key preservation assertions.
- **Flat-star/185 regression.** Pin with byte-identical one-receipt tests plus 177-181 and 185
  suites.
- **Broadcast leakage.** Pin with hash-only branch/root tests and the no-fragment-change guard.

## Deferred

Session/workload verdict accrual; broadcast-snowflake execution cliff; mixed-strategy
snowflake cost model; galaxy / bridge / cyclic recognition; always-on telemetry.
