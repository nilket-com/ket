# ADR 206: Cross-Shape Joint Actuation

**Status:** Promoted. **Migration:** `plans/impl/0206_cross_shape_joint_actuation.md`.

The 200→205 arc grew a 3-way graph partition (galaxy / single-junction bridge / multi-junction chain)
layered over the tree recognizer (star / snowflake). The 201 `graph_actuated` XOR gate decides *which*
shape's verdicts a query emits — and 205's chain was never wired into it. ADR 206 makes that boundary
**partition-complete**: an actuated chain is judged by exactly **one** coherent verdict — its own
applied-order `BridgeChainRoot` — with its orphan tree verdicts suppressed, just as galaxy/bridge already are.

## Grounding (verified against the code) — the concrete gap, and a scope honesty note

The literal reading of "joint actuation" — actuating two *independent* shapes in one query — is essentially
**unreachable** in normal SQL: a single SELECT's join graph is connected, and a connected join over distinct
relations is recognized as exactly one shape; two independent shapes need a cross-join of two shaped clusters
(rare, weird). So this cut delivers the **concrete, reachable** cross-shape gap — the *verdict partition* —
and defers literal multi-region joint actuation until a fixture reaches it.

- **The `graph_actuated` gate is stale (a 205 gap).** `measured_verdicts_from_summaries` (explain.rs:549)
  gates tree-verdict emission on `applied_galaxy_root.is_some() || applied_bridge_root.is_some()` — it omits
  `applied_bridge_chain_root`. So an actuated **chain** has `graph_actuated == false`, and its FROM tree's
  snowflake/star verdicts are emitted. Verified: a chain query's verdict set is
  `[SnowflakeRoot, SnowflakeBranch, …, BridgeChainRoot]` — for an *actuated* chain those tree verdicts are
  **orphans** (the re-rooted tree was bound on the no-op recorder, so its decisions never accrued — exactly
  the orphan the 201 gate exists to suppress).
- **The boundary fix alone leaves a hole.** Suppressing the actuated chain's tree verdicts (correct) without
  emitting its own verdict (205 deferred it) leaves an actuated chain judged by **nothing** — we reordered a
  plan and the verdict log is silent on whether it helped. The partition is coherent only if the chain's
  applied-order verdict is emitted. So this cut completes the boundary **and** the deferred actuated-order
  verdict — they are one surface.
- **The actuated chain doesn't re-recognize (205's reason for deferring).** Interior junctions fail the
  inverse-F1 gate in reverse, so `analyze_bridge_chain` on the reversed plan declines — which is why the
  galaxy/bridge "verdict-judges-applied-via-re-recognition" path doesn't work for chains. But the reversed
  plan's edges ARE present (`render_trace.join_order_edges`, populated by the re-bind) with the reversed
  targets the measured costs need. The missing piece is *constructing the reversed evidence with those
  targets*, not re-recognition.

## Decision

**D1 — Partition-complete the `graph_actuated` boundary.** Replace the ad-hoc
`applied_galaxy_root.is_some() || applied_bridge_root.is_some()` with a single partition-complete accessor
`OptimizerExplainRenderTrace::applied_graph_root() -> Option<&str>` (Some iff any of galaxy / bridge /
**chain** applied). An actuated chain then suppresses its orphan tree verdicts exactly as galaxy/bridge do,
and a future 4th shape can't be silently forgotten — the gate is exhaustive by construction, not a
hand-maintained OR.

**D2 — Emit the actuated chain's applied-order verdict (the deferred 205 follow-on).** After actuation, build
the **reversed `BridgeChainEvidence`**: reverse the recognized hops (swap each hop's `fact↔dim`, `in↔out`
signatures and edge-row estimates) and resolve `incoming_target`/`outgoing_target` from the **reversed
plan's** `join_order_edges` (matched by relation label, since re-recognition declines). Set it on
`render_trace` alongside `applied_bridge_chain_root`, and un-suppress the chain verdict arm for actuated
chains. The existing `measured_verdicts_from_summaries` chain arm + `measured_graph_order_verdict` then judge
the **applied** (reversed) order — `Vindicated` if the reversal was measured-optimal, `OverEager`/`MissedWin`
if not — the 198 `…vindicates_applied_root` shape, reached by construction rather than re-recognition.

**D3 — One coherent verdict per actuated chain.** The net contract: an actuated chain emits its
`BridgeChainRoot` applied-order verdict and **nothing else** — no orphan `SnowflakeRoot`/`Branch` (D1), no
partial first-junction `BridgeRoot` (205's verdict-surface gate). The 3-way partition is complete on the
verdict surface. Kept (non-actuated) chains are unchanged (observe verdict + their real FROM-tree verdicts).

**D4 — Invariants / boundaries.** Plans + EXPLAIN tree receipts byte-identical (a verdict-log-only change).
Binder + explain + the existing verdict log. No new fragmenter / executor / storage / Wendelon / PBC / wire /
grammar. Galaxy / single-bridge / snowflake / star verdict behavior unchanged.

## Honesty gate

`actuated_chain_verdict_vindicates_applied_root`: a selective 3-hop chain actuates, and under EXPLAIN ANALYZE
its `BridgeChainRoot` verdict reads `Vindicated` (the reversal was the measured-cheaper order), keyed to the
**reversed** plan's measured costs. Must **fail on `main`** (205 suppresses the actuated chain verdict).

**Load-bearing discriminators:** `actuated_chain_emits_no_orphan_tree_verdict` (an actuated chain's verdict
set contains `BridgeChainRoot` and **no** `SnowflakeRoot`/`SnowflakeBranch` — revert-verified: fails under the
stale galaxy/bridge-only gate, leaking `[SnowflakeRoot, SnowflakeBranch, …]`); `actuated_chain_verdict_uses_
reversed_plan_targets` (every reversed target resolves to an executed join, and exploding the reversed
declared-end faults the reversal); and the kept-chain regression (a non-actuated chain still emits its
FROM-tree verdicts + observe chain verdict, unchanged). **Finding:** a pure AST-vs-reversed *outcome*
discriminator isn't behaviorally reachable — both keyings read the same plan-index summaries (they differ
only in the declared-root label), so the test locks target-resolution soundness, not outcome divergence. The
verdict-via-reversed-evidence path is instead revert-verified (the actuated verdict is absent under 205's
suppression).

## Consensus (transparent)

Council selected **cross-shape joint actuation** over closing the chain verdict loop; grounding shows they are
one cut — the cross-shape *verdict partition* is incoherent for an actuated chain unless its own verdict is
emitted. Two corrections grounded in: the `graph_actuated` gate is stale (chain omitted → orphan tree
verdicts today), and the applied-order verdict is reachable by constructing reversed evidence with
reversed-plan targets (not by re-recognition, which declines). The literal multi-region joint actuation is a
named deferral (unreachable in normal SQL today).

## Forward arc

`205 chain actuation → 206 cross-shape verdict partition / actuated-order verdict (this) → multi-region joint
actuation (independent shapes in one query — deferred until reachable) → memo only when receipts show local
decisions fighting`.

## Deferred (named)

Multi-region joint actuation (two independent shapes in one connected query — actuate both, not first-wins;
deferred until a reachable fixture exists); drive-from-a-middle-bridge (general join-order search); the
peak-vs-sum cost model (204); outer joins; durable optimizer evidence.
