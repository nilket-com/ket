# Impl Plan - ADR 182: Snowflake-Path Join Order Evidence

Pairs with `plans/adr/0182_snowflake_path_join_order_evidence.md`.

Observational topology rung: recognize a strict single-root **left-deep-graph** snowflake
from the existing flat `JoinOrderEdgeEvidence` stream by chaining `outer.dimension ==
inner.probe`, emit **nested** per-level declared-vs-cost-preferred ranking evidence plus
per-edge strategy evidence, and render it as a `join_order_snowflake` cockpit row —
superseding the misleading flat `MissingRelationLabel` decline, **without reordering,
re-binding, or changing any plan**. Pure additive sibling to the 177–181 star path; no
executor / `fragment.rs` / cost-model / Wendelon change.

## Anchors

| Concern | Anchor |
|---|---|
| Edge evidence (reused as-is) | `JoinOrderEdgeEvidence` (`binder/join_order.rs:29`); `probe`/`dimension` `Option<String>`; `target.join_index` |
| Flat star ranking (reused per level) | factor `rank_star_order` / `StarOrderRanking` out of `recognized_star_order_outcome` (`binder/join_order.rs`) |
| Why chaining works | `relation_label_for_join_keys` (`binder/join_order.rs:308`) — label = join key's source relation |
| Snowflake signal (today's decline) | `try_bind_star_order_actuation` probe-mismatch decline (`binder.rs:1771`, `MissingRelationLabel`) |
| Join dispatch (actuation → fallthrough) | `binder.rs:1004` Join arm; `record_star_order_decline` (`binder.rs`) sets `join_order` |
| Per-edge strategy | `trace.join_decisions` `DecisionReceipt`s, correlated by `edge.target.join_index`; rendered as evidence, never a gate |
| Emit seam | `bind_with_explain_trace` (`binder.rs:178`) — after bind, before return |
| Runtime gate (stays star-only) | `star_order_runtime_replay_ids` (`binder.rs`) reads `join_order` |
| Cockpit render | `optimizer_explain_*` / `join_order_*_label` (`binder/explain.rs`) |
| Actuation recognizer (UNCHANGED) | `extract_star_join_candidate` (`binder/join_order.rs`) |
| Tests | `binder/tests.rs`; `tests/architecture/join_order.rs`; `tests/explain_analyze_test.rs` |

## Phase 1 - Factor strategy-free ranking (`binder/join_order.rs`)

Do not use `StarOrderEvidence` as the snowflake level type. After ADR 181 it carries
actuation/runtime semantics (`strategy`, `applied_order`, verdict inputs), while 182 is
plan-observation only and must represent mixed-strategy levels.

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderRanking {
    pub(super) probe: String,
    pub(super) dimensions: Vec<JoinOrderDimEvidence>,
    pub(super) declared_order: Vec<String>,
    pub(super) cost_preferred_order: Vec<String>,
    pub(super) declared_cost: u128,
    pub(super) preferred_cost: u128,
    pub(super) orders_agree: bool,
    pub(super) action: StarOrderAction,
    pub(super) confidence: JoinOrderEstimateConfidence,
}

pub(super) fn rank_star_order(
    probe: String,
    dimensions: Vec<JoinOrderDimEvidence>,
    confidence: JoinOrderEstimateConfidence,
) -> StarOrderRanking { ... }
```

`recognized_star_order_outcome` calls `rank_star_order` and wraps the ranking with
`strategy` + `applied_order`. Existing star behavior and labels stay byte-identical.

## Phase 2 - Snowflake recognition (`binder/join_order.rs`, read-only)

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeOrderEvidence {
    pub(super) ranking: StarOrderRanking,                  // this level, no strategy/applied-order lie
    pub(super) edge_strategies: Vec<SnowflakeEdgeStrategy>, // direct edges at this level
    pub(super) branches: Vec<SnowflakeBranch>,             // dims that are themselves sub-stars
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeBranch {
    pub(super) dimension: String,                  // parent-level dim that is the sub-star probe
    pub(super) evidence: SnowflakeOrderEvidence,   // recursive
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeEdgeStrategy {
    pub(super) dimension: String,
    pub(super) strategy: JoinStrategy,
    pub(super) placement: JoinPhysicalPlacement,
    pub(super) probe_scan_linear: bool,
}

// Some = recognized snowflake; None = not a snowflake (flat path owns rendering/decline).
pub(super) fn analyze_snowflake(
    edges: &[JoinOrderEdgeEvidence],
    decisions: &[DecisionReceipt],
) -> Option<SnowflakeOrderEvidence> { ... }
```

Algorithm (no new edge fields, no re-binding):
1. Every edge must be `inner_equi` with both labels present; else `None` (let the flat path
   own its decline — 182 never weakens a decline, never emits its own decline row). **No
   scan-linear gate.**
2. Adjacency `probe -> [(dimension, edge_index)...]`. **Fact root** = unique probe label
   never appearing as a dimension. Zero/multiple roots, a dimension with two parents (shared
   dim / bridge), or a cycle → `None`.
3. A dimension that also appears as a probe is a **branch**; else a **leaf**. Require ≥1
   branch (a branchless tree is a flat star → `None`, flat path handles it).
4. Build recursively: at each node, its direct dimension edges → level-local declared order
   (relative edge order) → `rank_star_order(probe, dims, Soft)`. Separately collect
   `SnowflakeEdgeStrategy` by looking up each edge's `DecisionReceipt` through
   `edge.target.join_index`; missing/mismatched receipt → `None`. Do not call
   `uniform_star_order_strategy`; mixed-strategy levels are observable, not declined. Recurse
   branch sub-trees. Cycle/visited guard bounds the recursion.

Read-only: consumes slices, builds no plan.

## Phase 3 - Emit + supersede (`binder.rs`)

In `bind_with_explain_trace`, after the plan is bound, before return:

```rust
render_trace.snowflake_order =
    analyze_snowflake(&render_trace.join_order_edges, &trace.join_decisions);
if render_trace.snowflake_order.is_some() {
    render_trace.join_order = None;            // supersede the actuation's flat MissingRelationLabel decline
} else if render_trace.join_order.is_none() {
    render_trace.join_order = analyze(&render_trace.join_order_edges);
}
```

Add `snowflake_order: Option<SnowflakeOrderEvidence>` to `OptimizerExplainRenderTrace`
(default `None`). `star_order_runtime_replay_ids` unchanged (reads `join_order`), so a
recognized snowflake (cleared `join_order`) yields no runtime/verdict rows.

## Phase 4 - Render (`binder/explain.rs`)

- Add a `join_order_snowflake` row kind, detail depth-first:
  `root=<probe>; declared=[..] preferred=[..] cost=../..; strategies=[dim:strategy/placement/scan_linear,..]; branch <dim>: ...`.
- Factor the flat order detail helper so it can render `StarOrderRanking` while existing
  `join_order` rows still include `strategy` and `applied_order`.
- Render the ranking action as candidate evidence (`would_reorder` /
  `would_keep_declared`), not as an applied action. Snowflake rows must never imply the plan
  was reordered.
- SQL `EXPLAIN`/`EXPLAIN ANALYZE` and Rune `explain()`/`analyze()` share the path. For
  EXPLAIN ANALYZE, emit the snowflake **plan** row in `optimizer_analyze_rows` even though
  `star_order_runtime_replay_ids` returns no replay ids.

## Phase 5 - Behavioral tests (`binder/tests.rs`)

Fixture: `snowflake_join_order_catalog()` — `orders` fact, a leaf dim on `orders`, and
`regions` keyed to `customers` (so `customers→regions` is a branch). SQL (left-deep,
SQL-reachable): `SELECT * FROM orders o JOIN customers c ON o.customer_id=c.id JOIN regions r
ON c.region_id=r.id JOIN stores s ON o.store_id=s.id`, with range/NDV facts making a
deliberately non-optimal order at both levels. Hash variant (large dims) for the execution
check.

- `snowflake_order_evidence_reports_nested_declared_vs_preferred` - nested evidence; both
  levels' declared≠preferred shown.
- `snowflake_order_evidence_is_observational` - bind with/without EXPLAIN → identical inner
  `ExecutionPlan`; hash-snowflake executes, declared-order results unchanged.
- `snowflake_recognized_supersedes_flat_missing_relation_decline` - on `main` the query emits
  `declined_missing_relation_label`; with 182 it emits `join_order_snowflake` and no flat
  `join_order` row. (Author the `main`-behavior assertion as the discriminator.)
- `snowflake_branch_links_outer_dimension_to_inner_probe` - branch `dimension` equals the
  sub-star probe (chaining).
- `flat_star_emits_no_snowflake_row` - a flat star → flat row, no snowflake row.
- `galaxy_or_shared_dimension_declines_snowflake` - shared dim / second root → no snowflake
  row; the prior flat decline stands.
- `snowflake_evidence_records_per_edge_strategy_without_gating` - a mixed level with one hash
  edge and one broadcast edge still emits snowflake evidence; both strategies/placements are
  rendered per edge.
- `snowflake_order_evidence_is_explain_gated` - normal bind emits nothing.
- `snowflake_explain_analyze_shows_plan_evidence_without_runtime_or_verdict` - EXPLAIN
  ANALYZE on the snowflake → nested plan rows, no `join_order_runtime` or
  `join_order_verdict` row.
- `rune_explain_shows_snowflake_order_evidence` - Rune parity.

## Phase 6 - Architecture guards (`tests/architecture/join_order.rs`)

- `adr182_snowflake_recognition_is_observational` - `analyze_snowflake` read-only;
  `bind_join`/strategy/`extract_star_join_candidate`/runtime do not consult `snowflake_order`;
  no reorder.
- `adr182_snowflake_reconstructs_from_existing_edges` - `JoinOrderEdgeEvidence` field set
  unchanged from 181; `analyze_snowflake` uses `edge.probe`/`edge.dimension` chaining.
- `adr182_snowflake_uses_strategy_free_ranking_not_fake_strategy` - `SnowflakeOrderEvidence`
  stores `StarOrderRanking`, not `StarOrderEvidence`; `analyze_snowflake` calls
  `rank_star_order` and does not inject `StarOrderStrategy::Broadcast` or any placeholder
  strategy.
- `adr182_snowflake_records_strategy_as_evidence_not_gate` - `analyze_snowflake` reads
  per-edge `DecisionReceipt`s by `target.join_index`, stores strategy/placement evidence, and
  does not call `uniform_star_order_strategy`.
- `adr182_snowflake_supersedes_decline_without_gating_runtime` - emit seam clears `join_order`
  on recognition; `star_order_runtime_replay_ids` reads `join_order` only (not
  `snowflake_order`).
- `adr182_no_executor_fragment_costmodel_or_wendelon_change` - marker sweep
  (`SnowflakeOrder*`, `analyze_snowflake`, `join_order_snowflake`) absent from `exec.rs`,
  `fragment.rs`, `pbc.rs`, `storage.rs`, `coordinator.rs`, `sql_planner.rs`, `distributed/`,
  Wendelon; `rewrite_broadcast_star_probe` unchanged.
- `adr182_no_optimizer_shell` → `adr179_no_optimizer_shell()`.
- `adr182_cockpit_is_frontend_shared` - rendered via the shared optimizer-explain path.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --lib star_order        # 177-181 star path byte-identical
scripts/cargo-cgroup test -p toron --lib non_star_join_declines_with_reason   # flat decline still stands
scripts/cargo-cgroup test -p toron --test architecture_test adr182
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --lib                   # plans byte-identical = observational
git diff --check
```

## Risks

- **Observational invariant.** `analyze_snowflake` read-only, computed in the emit seam from
  edges/decisions; green plan/star suites + observational guard are the proof.
- **Don't weaken existing declines.** `analyze_snowflake` must return `None` for non-snowflake
  declined shapes (e.g. `nested_hash_probe_with_tiny_right` → `probe_not_scan_linear`) so the
  flat decline stands. Strictness: single root, ≥1 branch, acyclic, no shared dim; pin with
  `non_star_join_declines_with_reason` staying green + `galaxy_or_shared_dimension_declines_snowflake`.
- **Star-path regression.** Flat star → `analyze_snowflake` returns `None` (no branch) → flat
  path byte-identical. Pin with `flat_star_emits_no_snowflake_row` + green `star_order`.
- **Chaining correctness.** Reconstruct strictly from key-source labels; decline on ambiguity.
- **Per-level strategy is evidence only.** `level_strategy` must never gate or reorder; it is
  rendered, not consulted. Pin with `adr182_snowflake_recognition_is_observational`.
- **Execution-touching tests use a hash snowflake.** A broadcast snowflake may hit the
  fragmenter cliff (`rewrite_broadcast_star_probe`) — out of scope; keep results checks on
  hash snowflakes that execute via `execute_hash_join`.
- **Hot path.** Snowflake evidence only when the render trace is present.

## Deferred

Snowflake runtime evidence + measured verdict (179/180-analog); hash-snowflake order
actuation (binder-only, 181 preflight machinery); broadcast-snowflake execution cliff
(`rewrite_broadcast_star_probe`); galaxy / bridge / cyclic recognition; strategy-aware cost
model.
