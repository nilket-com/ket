# Impl Plan — ADR 197: Galaxy / Bridge Cost Model and Ordering

Pairs with `plans/adr/0197_graph_order_evidence.md`.

A shape-aware cost model over the recognized galaxy/bridge evidence: a preferred-order recommendation
(observe-only) + a measured order verdict, reusing the star-order verdict vocabulary and measured
verdict shape as a pure pass over the 194–196 evidence. No new substrate, no recognizer change, no
actuation. Phase 0 locks the discriminating fixture.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Galaxy evidence the cost model reads | `GalaxyOrderEvidence { conformed, edges, component_count }`; `ConformedDimension { label, incident_relations }`; `GalaxyEdgeEvidence { left_label, right_label, estimated_rows, target }` `join_order.rs:387+` — probed `sales→product=100000`, `product→returns=5000` |
| Bridge evidence | `BridgeOrderEvidence { links }`; `BridgeLink { fact, bridge, dim, bridge_rows, fact_rows, incoming_target, outgoing_target }` |
| Verdict vocabulary to reuse | `StarOrderAction { Reordered, KeptAlreadyOptimal, .. }`; `StarOrderVerdict { Vindicated, MissedWin, OverEager, Indeterminate, .. }`; `StarOrderVerdictReceipt` |
| Shape to mirror | `measured_star_order_verdict` `:1429` |
| EXPLAIN galaxy/bridge order rows (extend detail) | `optimizer_explain_galaxy_order_row`/`galaxy_order_detail_label`; `optimizer_explain_bridge_order_row`/`bridge_order_detail_label` `explain.rs` |
| ANALYZE rows (add unified verdict) | `galaxy_analyze_rows` / `bridge_analyze_rows` `explain.rs`; 195/196 runtime measurement + replay-id correlation |

## Phase 0 — Discriminating fixtures (do first)

- `galaxy_order_evidence_recommends_cheaper_root` on `galaxy_join_order_catalog`
  (`sales→product=100000`, `returns→product=5000`): cost model → `preferred_root=returns`,
  `action=would_reorder`, `declared_root=sales`.
- `galaxy_already_optimal_order_kept`: a second catalog where the FROM-leading fact *is* the smaller
  fact-to-dimension join → `kept_optimal`. Proves the model decides from evidence, not a constant.

## Phase 1 — Cost model (pure functions, `join_order.rs`)

```text
struct GraphOrderRecommendation {
    pivot: String,                 // conformed dimension (galaxy) or bridge label
    declared_root: String,         // FROM root: the incident fact never appearing as a right_label
    preferred_root: String,        // argmin fact-to-pivot estimate
    costs: Vec<(String, u128)>,    // per-fact cost = its fact-to-pivot edge estimate (row-only)
    action: StarOrderAction,       // Reordered (would_reorder) | KeptAlreadyOptimal
}

fn galaxy_order_recommendation(galaxy) -> Option<GraphOrderRecommendation>:
    dim = galaxy.conformed.first()?                       // 2-fact first cut
    facts = &dim.incident_relations; if facts.len() < 2 { None }
    costs = facts.map(|f| (f, edge_estimate_between(galaxy.edges, f, dim.label)?))  // None on missing
    preferred_root = costs.min_by(cost)
    declared_root  = facts.find(|f| galaxy.edges.iter().all(|e| e.right_label != *f))  // never built
    action = if declared_root == preferred_root { KeptAlreadyOptimal } else { Reordered }

fn bridge_order_recommendation(bridge) -> Option<GraphOrderRecommendation>:
    link = bridge.links.first()?
    // candidate cost: fan-out-first (fact->bridge->dim) vs filter-first; the first cut is the row
    // proxy from bridge_rows vs fact_rows. preferred = the cheaper; declared = the FROM (fan-out-first).
```

`edge_estimate_between(edges, a, b)` returns the `estimated_rows` of the `GalaxyEdgeEvidence` whose
`{left_label,right_label} == {a,b}`. Row-only; returns `None` (no recommendation) on missing estimates.

## Phase 2 — Order evidence row (EXPLAIN, observe-only)

Extend `galaxy_order_detail_label` / `bridge_order_detail_label` with
`declared_root={}; preferred_root={}; action={}; costs=[fact:est,…]` when a recommendation exists. Row
kind/structure unchanged (`join_order_galaxy` / `join_order_bridge`), so 194/196 assertions not reading
the new keys stay green. Executed plan untouched.

## Phase 3 — Unified measured verdict (ANALYZE, reuse 195/196)

In `galaxy_analyze_rows` / `bridge_analyze_rows`, after the runtime rows, emit a single
`join_order_graph_verdict` row: compute the recommendation, then compare the **measured** fact-to-pivot
output rows (from the summaries via edge `target`→replay-id) against the preferred root. Map to
`StarOrderVerdict` (`MissedWin` when the run kept a costlier root the measurement shows was beatable;
`Vindicated` when the measured order confirms the cheaper root; `Indeterminate` when measurements are
absent). Recommendation string `WOULD_REORDER_ROOT_FACT` / `KEEP_DECLARED`. Reuses
`measured_star_order_verdict`'s comparison shape.

## Phase 4 — Tests

- `galaxy_order_evidence_recommends_cheaper_root` — Phase 0 headline; `would_reorder`,
  `preferred_root=returns`; byte-identical; fails on `main`.
- `galaxy_already_optimal_order_kept` — `kept_optimal`.
- `galaxy_verdict_recommends_reordering_suboptimal_root_fact` — ANALYZE → `MissedWin` +
  `WOULD_REORDER_ROOT_FACT`.
- `graph_verdict_indeterminate_on_missing_measurement`; `graph_verdicts_are_observational_only`.
- `bridge_order_evidence_recommends_delaying_massive_fanout` + bridge verdict.
- `graph_order_cost_is_row_only` — the cost reads only estimates/rows, never `JoinStrategy`.
- `bridge_order_snowflake_galaxy_and_star_observe_byte_identical` (regression).
- `rune_graph_order_matches_sql` — Rune parity.

## Phase 5 — Architecture guards

- `adr197_graph_order_cost_is_row_only` — `galaxy_order_recommendation` / `bridge_order_recommendation`
  read estimates/endpoint rows only; no `JoinStrategy`/byte terms (preserves ADR 181).
- `adr197_graph_scorer_is_shape_aware_not_generic_memo` — enumerates the recognized shape's incident
  facts / bridge link; no general permutation / memo / Cascades.
- `adr197_graph_order_is_observe_only` — never reaches `bind_local_star_actuation`; executed plan
  unchanged; verdict only under ANALYZE.
- `adr197_verdicts_reuse_180_184_vocabulary` — uses `StarOrderVerdict` /
  `measured_star_order_verdict`, no parallel impl; one unified `join_order_graph_verdict`.
- `adr197_graph_order_is_binder_only` — no fragmenter/executor/storage/Wendelon/PBC/wire/grammar.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib galaxy_order
scripts/cargo-cgroup test -p toron --lib graph
scripts/cargo-cgroup test -p toron --lib galaxy
scripts/cargo-cgroup test -p toron --lib bridge
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr197
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one test filter per command.

## Risks

- **Cost honesty.** Row-only selectivity heuristic, not a global-optimality claim; the guard +
  measured verdict keep it honest. If receipts stop discriminating optimal vs suboptimal roots, too early.
- **Scope creep into search.** Pin shape-awareness — enumerate the recognized shape's candidates only.
- **Byte-identity.** Order/verdict rows are additive detail; 194–196 evidence rows and all executed
  plans stay identical. Measurement gaps → `Indeterminate`, never a fabricated verdict.

## Deferred

Graph topology actuation; n-fact galaxy / multi-hop bridge ordering; general join-order search /
memo/Cascades; cross-shape joint ordering; catalog PK/FK classification; outer joins; durable evidence.
