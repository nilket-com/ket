# Impl Plan - ADR 178: Star-Path Join Order Actuation

Pairs with `plans/adr/0178_star_path_join_order_actuation.md`.

Implement strict broadcast-star actuation as a binder-only planning-time rewrite. Extract a
left-deep inner star candidate, bind the probe and dimension legs once with local
join-order source capture, score dimension edges, apply the confidence-scaled gate, build
ordinary joins in the selected order, and restore the declared output shape when applied
order differs from declared order.

## Anchors

| Concern | Anchor |
|---|---|
| Join dispatch | `LogicalPlan::Join` arm in `Binder::bind_plan_with_trace` (`binder.rs`) |
| One-edge join construction | `Binder::bind_join` (`binder.rs`) |
| Strategy/replay path | `choose_join_strategy`, `JoinReplayId`, `broadcast_replay_eligibility` (`binder/join.rs`, `binder.rs`) |
| Scan-linear fence | `broadcast_probe_is_scan_linear` (`binder/join.rs`) |
| ADR177 evidence vocabulary | `binder/join_order.rs` |
| Output restore precedent | `restore_declared_join_order_project` (`binder/join.rs`) |
| Cockpit render | `OptimizerExplainRenderTrace`, `optimizer_explain_join_order_row` (`binder/explain.rs`) |
| Existing fixtures | `star_join_order_catalog`, `non_optimal_star_order_sql` (`binder/tests.rs`) |
| Architecture guards | `tests/architecture/join_order.rs` |

## Phase 1 - Separate source capture from render capture

ADR177 ties join-order source capture to `render_trace.is_some()`. Split that:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinOrderSourceCapture {
    Off,
    On,
}
```

Thread the mode through `bind_plan_with_trace` or an equivalent helper.

Required behavior:

- ordinary non-candidate binds use `Off`;
- EXPLAIN still renders receipts only through `OptimizerExplainRenderTrace`;
- the star actuator can bind candidate probe/dim legs with `On` without fabricating a
  render trace;
- source capture is candidate-local, not blanket always-on telemetry.

Retarget the ADR177 EXPLAIN-gated guard to assert rendered rows are EXPLAIN-gated, while
ADR178 candidate source capture is allowed.

## Phase 2 - Add a planning-time strict-star extractor

In `binder/join_order.rs`, add a shallow logical extractor:

```rust
struct StarJoinCandidate<'a> {
    probe: &'a LogicalPlan,
    dimensions: Vec<StarJoinDimension<'a>>,
}

struct StarJoinDimension<'a> {
    input: &'a LogicalPlan,
    on: &'a [(Expr, Expr)],
    strategy_intent: JoinStrategyIntent,
    declared_position: usize,
}
```

Rules:

- walk only left-deep `LogicalPlan::Join` chains;
- require `JoinKind::Inner`;
- require two or more right-side dimensions;
- require `JoinStrategyIntent::Auto` on every edge;
- decline non-equi during key binding, but non-inner/pinned/non-left-deep can decline here;
- do not classify snowflake/general graph shapes.

The extractor does not bind expressions or estimate rows.

## Phase 3 - Refactor one-edge join construction

Factor current `bind_join` into two pieces:

1. child binding;
2. building one already-bound scoped join.

Shape:

```rust
fn bind_scoped_join(
    &self,
    left_plan: ScopedExecutionPlan,
    right_plan: ScopedExecutionPlan,
    kind: JoinKind,
    on: &[(Expr, Expr)],
    strategy_intent: JoinStrategyIntent,
    trace: &mut OptimizerTrace,
    render_trace: &mut Option<OptimizerExplainRenderTrace>,
    declared_position: usize,
) -> Result<ScopedExecutionPlan, BindingError>
```

This helper must preserve existing behavior:

- `bind_join_key_pair`;
- `validate_join_key_types`;
- single-key and multi-key cardinality estimate;
- `JoinReplayId` from `trace.join_decisions.len()`;
- `choose_join_strategy`;
- exchange insertion;
- right-build physical swap;
- `restore_declared_join_order_project` for a single swapped edge;
- `DecisionReceipt` push;
- `join_order_sources` merge;
- output facts/scope construction.

Then the fallback `bind_join` becomes child binding plus `bind_scoped_join`.

## Phase 4 - Bind candidate legs once and score edges

Add a Binder method:

```rust
async fn try_bind_star_order_actuation(
    &self,
    plan: &LogicalPlan,
    trace: &mut OptimizerTrace,
    render_trace: &mut Option<OptimizerExplainRenderTrace>,
) -> Result<Option<ScopedExecutionPlan>, BindingError>
```

Call it from the `LogicalPlan::Join` arm before the ordinary fallback.

Flow:

1. Extract candidate. If absent, return `Ok(None)`.
2. Bind the probe and each dimension leg independently with `JoinOrderSourceCapture::On`.
3. Resolve each edge's join keys between the bound probe scope and the dimension scope.
4. Require probe keys to map to one probe source label and each dimension to one unique
   source label.
5. Compute edge row estimates using the existing single-key/multi-key estimate functions
   and ADR177 `join_order_edge_estimate` helpers.
6. Derive `JoinOrderEstimateConfidence` from row/NDV provenance:

```rust
enum JoinOrderEstimateConfidence {
    Hard,
    Soft,
}
```

Hard should require hard row evidence and exact/hard NDV inputs. Anything sketch,
fallback, or mixed is soft. Missing remains a decline.

7. Require the probe plan to pass `broadcast_probe_is_scan_linear`.

Do not push physical join receipts during this scoring phase.

## Phase 5 - Gate and broadcast-viability preflight

Extend the outcome model:

```rust
enum StarOrderAction {
    Reordered,
    KeptAlreadyOptimal,
    KeptInsufficientMargin,
    Declined(StarOrderDeclineReason),
}
```

Define local constants, for example:

```rust
const STAR_ORDER_SOFT_MARGIN_NUMERATOR: u128 = 4;
const STAR_ORDER_SOFT_MARGIN_DENOMINATOR: u128 = 5;
```

Equivalent meaning: soft reorder requires `preferred_cost <= declared_cost * 4 / 5`.

Gate:

- if missing estimate or shape issue: decline;
- if declared and preferred agree: keep already optimal;
- if confidence is hard: any positive preferred win can apply;
- if confidence is soft: apply only when the soft margin clears.

Before committing to `Reordered`, preflight that the selected sequence remains a broadcast
star. This can be a pure helper that calls `choose_join_strategy` over the candidate facts
and scan-linear state without mutating `trace`, or a scratch build whose trace is
discarded. If any edge would become hash/non-broadcast, keep declared with a
broadcast-decline reason.

## Phase 6 - Build selected order through ordinary joins

Choose applied order:

- declared order for keep/decline;
- preferred order for `Reordered`.

Build the sequence by repeatedly calling `bind_scoped_join` over the already-bound probe
and dimension plans. This is where real `JoinReplayId`s and strategy receipts are
allocated.

Implementation guardrails:

- do not recursively re-bind the same probe/dim legs;
- do not mutate real `trace` until the final applied order is known;
- if the implementation uses scratch traces for preflight, discard them completely;
- preserve existing `force_hash_join_ids` semantics by letting the final build allocate
  replay IDs in physical applied order.

## Phase 7 - Restore declared star output order

When applied order differs from declared order, wrap the final star plan with:

```rust
fn restore_declared_star_order_project(
    input: ExecutionPlan,
    physical_output_schema: &SchemaRef,
    declared_output_schema: SchemaRef,
    physical_segments: &[StarOutputSegment],
    declared_segments: &[StarOutputSegment],
) -> Result<ExecutionPlan, BindingError>
```

`StarOutputSegment` should carry source label, width, and physical segment start. Build a
`BoundExprProgram` selecting physical ordinals in declared segment order. Preserve logical
type metadata by decoding and setting node logical types as
`restore_declared_join_order_project` already does.

The returned `ScopedExecutionPlan` must use the declared scope/schema. If segment mapping
is ambiguous, keep declared order rather than applying a suspect projection.

## Phase 8 - Render action receipts

Extend `StarOrderEvidence`/`StarOrderOutcome` and `optimizer_explain_join_order_row`:

- `declared_order`;
- `cost_preferred_order`;
- `applied_order`;
- `declared_cost`;
- `preferred_cost`;
- `confidence`;
- margin constants;
- action/reason.

The row remains in the existing optimizer explain system table schema. SQL `EXPLAIN` and
Rune `explain()` continue to share `optimizer_explain_plan`.

If no actuation candidate was seen and only ADR177 render edges exist, keep the old
decline/evidence rendering path for compatibility with non-candidate explain cases.

## Phase 9 - Retarget architecture guards

Update `tests/architecture/join_order.rs`.

Retarget ADR177 guards:

- replace the pure-observational guard with a guard that allows ADR178 gated action while
  asserting keep/decline paths preserve declared order;
- retarget EXPLAIN gating to rendered rows rather than candidate source capture;
- keep no-shell and no-execution-surface guards.

Add ADR178 guards:

- `adr178_join_order_actuation_lives_in_join_order_module`;
- `adr178_planning_does_not_consume_cockpit_rows`;
- `adr178_reuses_177_estimate_vocabulary`;
- `adr178_gate_is_confidence_scaled`;
- `adr178_declared_output_restore_is_explicit`;
- `adr178_broadcast_star_only`;
- `adr178_retargeted_177_observational_guard`;
- `adr178_no_executor_fragment_storage_or_wendelon_change`;
- `adr178_no_optimizer_shell_or_planproperties_contract`;
- `adr178_cockpit_is_frontend_shared`.

## Phase 10 - Behavioral tests

Add tests near the ADR177 star-order tests in `binder/tests.rs`.

- `star_order_reorders_non_optimal_broadcast_star_to_preferred`
  - use `star_join_order_catalog()` and `non_optimal_star_order_sql()`;
  - assert the physical applied dimension order is preferred.
- `star_order_reorder_preserves_schema_and_rows`
  - compare schema/field order exactly;
  - compare row multiset, or add explicit `ORDER BY` for deterministic batch equality.
- `star_order_receipt_reports_reordered_action`
  - `EXPLAIN` the positive fixture;
  - assert declared/preferred/applied orders, confidence, margin, and `action=reordered`.
- `star_order_keeps_declared_when_already_optimal`
  - preferred-declared fixture remains declared and receipt says already optimal.
- `star_order_keeps_declared_when_soft_margin_is_thin`
  - seed soft facts so preferred barely wins;
  - assert no physical reorder.
- `star_order_declines_missing_facts`
  - remove a required NDV/row fact and assert declared order.
- `star_order_declines_non_broadcast_or_non_star`
  - hash/non-broadcast star, outer join, non-equi, duplicate dimension, and scan-linear
    fence cases keep declared.
- `star_order_replay_ids_and_receipts_follow_applied_sequence`
  - collect replay metadata and join receipts after reorder;
  - assert IDs are consecutive and receipts match physical order.
- `projected_star_reorder_restores_downstream_ordinals`
  - projection/aggregate/order-by over the reordered star resolves against declared output
    ordinals.
- `rune_star_order_reorders_same_as_sql`
  - Rune plan reaches the same action row and applied order.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib star_order
scripts/cargo-cgroup test -p toron --lib join_replay
scripts/cargo-cgroup test -p toron --test architecture_test adr177
scripts/cargo-cgroup test -p toron --test architecture_test adr178
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
rg -n "StarOrder|JoinOrder|join_order" \
  crates/toron/src/exec.rs crates/toron/src/pbc.rs crates/toron/src/storage.rs \
  crates/toron/src/distributed/fragment.rs crates/wendelon/src -S
git diff --check
```

Expected leak scan: no executor/PBC/storage/fragment/Wendelon matches.

## Risks

- **Output corruption.** A wrong N-segment restore silently changes `SELECT *`. The schema
  and downstream-ordinal gates are load-bearing.
- **Row-order overclaim.** Hash joins do not promise SQL output order without `ORDER BY`.
  Tests should compare row multisets or force deterministic ordering.
- **Planning from render state.** Do not read `OptimizerExplainRenderTrace` to decide.
- **Double-binding.** Bind candidate legs once; build only the final selected sequence with
  real trace mutation.
- **Strategy interaction.** Decline hash/non-broadcast cases. The first cut is broadcast
  star only.
- **Replay IDs.** Reordering changes physical join order. Replay IDs should stay
  consecutive and tied to the final applied order.
- **Guard reconciliation.** ADR177's pure observational and EXPLAIN-only-source-capture
  guards must be retargeted, not silently deleted.

## Deferred

Always-on telemetry; margin tuning; pinned hints; hash/non-broadcast star ordering;
snowflake/chain evidence and actuation; bounded enumeration; outer-join reorder safety;
broad plan properties; execution topology relaxation; memo search.
