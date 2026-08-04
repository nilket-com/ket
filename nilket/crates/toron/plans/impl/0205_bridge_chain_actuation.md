# Impl Plan — ADR 205: Bridge Chain Actuation

Pairs with `plans/adr/0205_bridge_chain_actuation.md`. Reuses the 200/202 unified actuator + gating; the new
mechanism is the path reversal and the Bridge/Chain disambiguation. All anchors verified.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Unified actuator (gating, soft margin, scratch) | `try_bind_graph_actuation` (join_order.rs:2292) — replace hardcoded `reroot_galaxy_join_tree` with `shape.reroot(...)` |
| Shape dispatch + decision accrual | dispatch loop (binder.rs:1126) `[Galaxy, Bridge]` → `[Galaxy, Bridge, Chain]`; `accrue_graph_order_decision` |
| `GraphShape` methods | `decision_kind` / `recommend` / `set_applied_root` (join_order.rs:2195) — add `Chain` arms + a `reroot` method |
| Star-pivot reroot (cannot reuse) | `reroot_galaxy_join_tree` (join_order.rs:602) — sibling `reverse_bridge_chain_join_tree` |
| Leaf identity through Filter/Project | `galaxy_leaf_label` (the 202 helper) |
| Chain recognizer + recommendation (landed) | `analyze_bridge_chain` / `bridge_chain_order_recommendation` (204) |
| Bridge arm to gate | `bridge_order_recommendation` reads `links.first()` with no length check |
| Applied-root receipt field | `bridge_chain_order` render field — add `applied_bridge_chain_root` + `set_applied_bridge_chain_root` |
| Distributed digest fixture to mirror | `three_fact_galaxy_reroot_digest_matches_ast_order_distributed` |

## Phase 0 — D0: the fragmentation digest gate (run FIRST)

Reverse the 3-hop chain (via the new fn behind a test-only call, or a hand-authored plan), bind both AST and
reversed orders, and assert the **distributed** digests match (mirror the galaxy reroot digest fixture).
Green → proceed. Red → stop; 205 is a fragmenter cut, not this cut.

## Phase 1 — D1: `reverse_bridge_chain_join_tree`

`join_order.rs`: collect the left-deep chain's leaves + per-edge `on` + per-join `(kind, strategy_intent)`
(walk the left spine, as `reroot_galaxy_join_tree` does), then rebuild left-deep from the `D` leaf,
re-orienting each `on` (swap to `(dim, fact)` for the new position). Carry `(kind, intent)` **by original
edge index** (the 202 F1 trap). `None` unless it is a left-deep all-leaf inner chain whose ends match the
recommendation. Reuse `galaxy_leaf_label` for leaf identity through Filter/Project.

## Phase 2 — D2: `GraphShape::Chain` + `reroot` dispatch + Bridge gate

`join_order.rs`:
- `GraphShape::Chain` variant; `decision_kind → BridgeChainRoot`; `recommend → analyze_bridge_chain(...).map(
  |c| bridge_chain_order_recommendation(&c))`; `set_applied_root → set_applied_bridge_chain_root`.
- `fn reroot(self, plan, rec) -> Option<LogicalPlan>` dispatching galaxy-reroot vs chain-reverse; call it in
  `try_bind_graph_actuation` in place of the hardcoded `reroot_galaxy_join_tree`.
- Gate `Bridge::recommend` to `analyze_bridge(...).filter(|b| b.links.len() == 1)` (actuation). AND gate the
  bridge VERDICT arms (`measured_verdicts_from_summaries` + the EXPLAIN ANALYZE verdict row in
  `bridge_analyze_rows`) on `render_trace.bridge_chain_order.is_none()` so a chain emits only
  `BridgeChainRoot`, never a partial first-junction `BridgeRoot` — the 3-way partition on the verdict
  surface. The observe-time `set_observed_bridge` path + per-junction runtime rows are untouched.
`explain.rs`: `applied_bridge_chain_root` field + `set_applied_bridge_chain_root`; the chain explain/analyze
rows flip to `reordered; applied_root=…` when set (mirror the bridge `applied_bridge_root` row).
`binder.rs`: dispatch loop `[Galaxy, Bridge, Chain]`.

## Phase 3 — tests + guards

`binder/tests.rs` (reuse the 204 `three_hop_bridge_chain_catalog` + a selective-`D` variant):

| test | locks | fails under |
|---|---|---|
| `three_hop_chain_reverse_digest_matches_ast_order_distributed` (Phase 0, lib.rs) | reversal preserves the result, distributed | a broken reversal |
| `two_hop_bridge_chain_reroots_to_cheaper_driver_end` | actuation fires → EXPLAIN `reordered; applied_root` | `main` (observe-only) |
| `three_hop_chain_is_recognized` / `two_hop_bridge_chain_is_recognized` | F-end cheapest → no actuation, stays `observed` | an over-eager actuator |
| `chain_query_does_not_trigger_single_bridge_actuation` | the 3-way partition, actuation surface (Bridge gated) | the ungated Bridge arm |
| `chain_verdict_log_has_no_partial_bridge_verdict` | the 3-way partition, verdict surface | the ungated bridge verdict arm (double-counts) |
| `bridge_reroots_to_selective_dimension` (existing) | single-bridge actuation + verdict still fire | the gate over-firing |
| 194–204 EXPLAIN + execution suites byte-identical | no drift | any regression |

The actuated-order **verdict** test is deferred (D3 — the reversed chain does not re-recognize, so its
measured costs cannot key to reversed-plan targets; the receipt is delivered via the carried scratch
evidence + applied-root marker, the verdict is a named follow-on).

`tests/architecture/join_order.rs`:
- `adr205_chain_actuation_reuses_unified_gating`: `try_bind_graph_actuation` calls `shape.reroot(`; soft
  margin + scratch + `accrue_graph_order_decision` reused; no new actuator/fragmenter.
- `adr205_chain_reroot_is_path_reversal_not_star`: `reverse_bridge_chain_join_tree` exists, carries meta by
  index, and `Chain` does NOT route through `reroot_galaxy_join_tree`.
- `adr205_bridge_arm_is_single_junction`: `Bridge::recommend` gates on `links.len() == 1`.
- `adr205_chain_suppresses_partial_bridge_verdict`: both bridge verdict arms gate on
  `bridge_chain_order.is_none()`.
- `adr205_chain_actuation_is_binder_only`: the reversal/marker names don't leak to fragment / exec /
  storage / Wendelon / pbc / wire.

## Verification gate (one filter per command)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib chain
scripts/cargo-cgroup test  -p toron --lib bridge        # single-bridge still actuates
scripts/cargo-cgroup test  -p toron --lib galaxy        # + snowflake, star (no drift)
scripts/cargo-cgroup test  -p toron --test architecture_test adr205
scripts/cargo-cgroup test  -p toron --test architecture_test adr204
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check ; git diff --check
```

## Risks

- **Phase-0 falsification** — if the reversed chain doesn't fragment byte-identical-modulo-order, 205 is the
  fragmenter extension; the gate runs first so we learn before building the arm.
- **Reversal meta-carry (the 202 F1 trap)** — `(kind, strategy_intent)` must travel by original edge index,
  not rebuilt position; pinned by the guard + the digest gate.
- **Bridge/Chain disambiguation** — without gating Bridge to one junction, a chain double-actuates; pinned
  by `chain_query_does_not_trigger_single_bridge_actuation` AND the still-passing single-bridge actuation.
- **Actuating on the entry-hop heuristic** — sound for *direction* (204 Δ1: the middle is common). The
  absolute-cost feedback (the compounded verdict) is the deferred follow-on, not a silent gap.
- **Reversed-chain re-recognition (MATERIALIZED)** — the reversed chain does NOT re-recognize (interior
  junctions fail inverse-F1 in reverse). Resolution shipped: the RECEIPT uses the carried scratch evidence +
  applied-root marker so the row renders `reordered; applied_root=D`; the actuated-order VERDICT is
  suppressed (deferred) because it would key measured costs to wrong joins. The 204 observe verdict still
  judges non-actuated chains. Pinned by `chain_query_does_not_trigger_single_bridge_actuation` (decision
  accrues) + `two_hop_bridge_chain_reroots_to_cheaper_driver_end` (receipt).
