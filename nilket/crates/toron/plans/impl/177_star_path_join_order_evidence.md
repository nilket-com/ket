# Impl Plan - ADR 177: Star-Path Join Order Evidence

Pairs with `plans/adr/177_star_path_join_order_evidence.md`.

Observational first rung of the optimizer arc. Under EXPLAIN render capture, recognize a
strict left-deep inner-join star, require the executor's existing
`broadcast_probe_is_scan_linear` probe fence where it applies, compute the
cost-preferred dimension order from the estimate `bind_join` already computes, and emit a
declared-vs-preferred join-order evidence receipt — **without reordering or changing any
plan**. New optimizer-layer seed module; reuses existing estimators + the 174/175
cockpit. No executor/`fragment.rs` change.

## Anchors (post-176 layout)

| Concern | Anchor |
|---|---|
| Join dispatch / `bind_join` | `LogicalPlan::Join` arm in `binder.rs`; `bind_join` (Binder method, `binder.rs`) computes `join_estimated_row_count` |
| Probe scan-linearity fence | `broadcast_probe_is_scan_linear` (`binder/join.rs`); use as a strict eligibility fence, not as the whole star recognizer |
| Cardinality estimators (reuse) | `join_output_ndv_containment_estimate`, `multi_key_join_cardinality` (`binder/selectivity.rs`) |
| Trace / capture / render | `OptimizerTrace`, `OptimizerExplainRenderTrace`, `optimizer_explain_*` (`binder/explain.rs`) |
| EXPLAIN entry | `bind_explain` (`binder.rs`) → `bind_with_explain_trace` + `optimizer_explain_plan` |
| New optimizer step | `binder/join_order.rs` (declared `mod join_order;` in root) — the optimizer layer's seed |
| Tests | optimizer-explain test module; `tests/architecture/cockpit.rs` / new `join_order.rs` |

## Phase 1 - Per-join evidence entry (render capture only), at the real estimate

`bind_join` already computes `join_estimated_row_count`. When `render_trace.as_mut()` is
present, push a cheap per-edge entry into the render trace:

```rust
struct JoinEdgeEvidence {
    probe: RelationKey,            // left-spine relation identity
    dimension: RelationKey,        // right-side relation identity
    declared_position: usize,      // declared/recursion order
    estimated_rows: RowCount,      // the SAME number bind_join computed
    estimate_source: JoinOrderEstimateSource,
    inner_equi: bool,              // star-eligibility marker
}
```

`RelationKey` = the relation identity reachable from the bound scopes / scan labels.
`broadcast_probe_is_scan_linear` is a probe-shape eligibility fence, not the identity
source. Fidelity: the entry carries the optimizer's actual estimate, not a re-derivation.
Normal bind has `render_trace = None` and pushes nothing. Read-side only — no plan
mutation.

## Phase 2 - Star recognition + order compare (`binder/join_order.rs`)

A dedicated, read-only step (optimizer-layer seed):

```rust
struct DimEvidence { dimension: RelationKey, estimated_rows: RowCount, estimate_source: JoinOrderEstimateSource }
struct StarOrderEvidence {
    probe: RelationKey,
    dimensions: Vec<DimEvidence>,
    declared_order: Vec<usize>,
    cost_preferred_order: Vec<usize>,
    declared_cost: f64,
    preferred_cost: f64,
    orders_agree: bool,
}
enum StarOrderOutcome { Recognized(StarOrderEvidence), Declined(StarDeclineReason) }
```

- **Group** `JoinEdgeEvidence` entries sharing a bound probe label into a star chain,
  accepting only a single-probe, all-`inner_equi`, left-deep spine. Also require the
  current `broadcast_probe_is_scan_linear` probe-shape fence where applicable; do not
  invent a looser executable-star notion. Anything else → `Declined(reason)`.
- **Cost-preferred order** = greedy, dimensions ascending by `estimated_rows`;
  `declared_cost`/`preferred_cost` = sum-of-intermediate proxy; `orders_agree =
  declared_order == cost_preferred_order`.
- Read-only: consumes entries, builds no plan, returns the outcome.

## Phase 3 - Emit + render

- In `bind_with_explain_trace`, after `push_down_scan_projection`, call
  `join_order::analyze(&edges)` and attach the `StarOrderOutcome` to the render trace.
- Extend `optimizer_explain_*` (`binder/explain.rs`) with a `join_order` evidence row:
  probe, dimensions, `declared_order`, `cost_preferred_order`, `orders_agree`, costs (or
  decline reason). Label the cost as the model's preference.
- SQL `EXPLAIN` and Rune `explain()` share the path.

## Phase 4 - Behavioral tests

Fixture: a probe + 3 dimensions with differing range/NDV facts so a deliberately
non-optimal declared order exists.

- `star_order_evidence_reports_declared_vs_preferred` - non-optimal 3-dim star →
  `orders_agree = false`, `cost_preferred_order ≠ declared_order`, per-dim rows shown.
- `star_order_evidence_is_observational` - bind with/without EXPLAIN → identical inner
  `ExecutionPlan`; execution uses declared order (no reorder).
- `non_star_join_declines_with_reason` - outer join / non-star → `Declined(reason)`.
- `star_order_evidence_agrees_when_declared_is_optimal` - optimal declared → agree.
- `star_order_evidence_estimates_match_join_estimates` - receipt rows == the
  `join_estimated_row_count` bind_join computed (fidelity; no second estimator).
- `star_recognition_respects_scan_linear_probe_fence` - a candidate star whose probe
  spine fails `broadcast_probe_is_scan_linear` declines.
- `star_order_evidence_is_explain_gated` - normal bind emits no join-edge entries.
- `rune_explain_shows_star_order_evidence` - Rune parity.

## Phase 5 - Architecture guards

In `tests/architecture/cockpit.rs` (or new `join_order.rs`):

- `adr177_star_order_evidence_is_observational` - `join_order::analyze` read-only;
  `bind_join`/strategy/dispatch do not consult `StarOrderOutcome`; no reorder.
- `adr177_join_order_evidence_lives_in_optimizer_step` - grouping + cost compare in
  `binder/join_order.rs`, not inline in `bind_join`.
- `adr177_evidence_reuses_existing_estimate` - per-edge `estimated_rows` is the binder's
  computed value; no new fact producer / second estimator.
- `adr177_star_recognition_respects_scan_linear_probe_fence` - recognition uses
  `broadcast_probe_is_scan_linear` as a strict probe eligibility fence, not a looser star
  definition.
- `adr177_evidence_is_explain_gated` - per-edge entries pushed only when
  `OptimizerExplainRenderTrace` is present.
- `adr177_no_optimizer_shell_or_planproperties_contract` - no top-level `optimizer.rs`,
  `struct Optimizer`, broad `PlanProperties`, Cascades, or memo search.
- `adr177_no_execution_or_fragment_change` - no executor/PBC/`fragment.rs`/storage diff.
- `adr177_cockpit_is_frontend_shared` - rendered via `optimizer_explain_plan`.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib star_order_evidence
scripts/cargo-cgroup test -p toron --lib star_order_evidence_is_observational
scripts/cargo-cgroup test -p toron --test architecture_test adr177
scripts/cargo-cgroup test -p toron --test architecture_test          # prior guards stay green
scripts/cargo-cgroup test -p toron --lib                             # plans byte-identical = observational
git diff --check
```

## Risks

- **Observational invariant.** Evidence must never reorder or feed planning; keep
  `join_order::analyze` read-only, consumed only by render. The green plan suite + the
  observational guard are the proof.
- **Fidelity.** Report `bind_join`'s actual estimate (Phase 1 emission), not a
  re-derivation; pin with the estimates-match gate.
- **Recognition divergence.** Treat `broadcast_probe_is_scan_linear` as a strict probe
  eligibility fence; a looser star notion would produce evidence for shapes the current
  execution path would reject. Pin with `star_recognition_respects_scan_linear_probe_fence`.
- **Grouping correctness.** Entries must group into the right single-probe left-deep chain;
  decline on any ambiguity rather than guess.
- **Hot path.** Per-edge entries only when `OptimizerExplainRenderTrace` is present;
  normal bind allocates nothing for join-order evidence.
- **Cost-proxy honesty.** The greedy proxy is a heuristic; label it the model's preference,
  and let 178's reorder be earned by observed declared≠preferred frequency, not blind trust.
- **Outer/semi joins.** Order-sensitive; decline (inner-only this cut).

## Deferred

The reorder (178); always-on order-evidence telemetry; outer-join/snowflake/general-graph
evidence; bounded enumeration; fuller `PlanProperties`; `fragment.rs:80` relaxation.
