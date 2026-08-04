# ADR 205: Bridge Chain Actuation

**Status:** Promoted. **Migration:** `plans/impl/0205_bridge_chain_actuation.md`.

204 made chain *evidence* bounded-N-hop and observe-only. ADR 205 actuates it: when the recognized chain's
cheaper driver-end is `D`, **reverse the path** `F ⋈ B1 ⋈ … ⋈ Bk ⋈ D → D ⋈ Bk ⋈ … ⋈ B1 ⋈ F` and bind the
reversed plan. Distributed result byte-identical-modulo-order; the chain decision becomes a committed
re-root. Gated by a Phase-0 fragmentation proof and the 190/191 soft margin.

## Grounding (verified against the code)

- **Actuating on the entry-hop recommendation is SOUND — and 204 is why.** The recommendation is a 2-ended
  `F`-vs-`D` decision over the two *entry* hops; 204's load-bearing invariant (Δ1) is that the middles are
  *common* terms that do not discriminate direction. So the entry-hop comparison **is** the correct
  direction discriminator — actuating on it picks the right end. The compounded verdict (which folds the
  middles) is the *absolute-cost* reality check after the fact, not a pre-gate. This resolves the apparent
  tension ("actuate on a heuristic 204 showed is incomplete?"): incomplete for *absolute cost*, exact for
  *direction*.
- **`reroot_galaxy_join_tree` cannot do this.** It is star-pivot shaped — it requires every fact to join the
  one conformed `pivot` (`galaxy_leaf_label(&dim_leaf) == pivot`, join_order.rs:653) and rotates facts
  around it. A chain is a **path** with no single pivot; driving from `D` is a **reversal**, not a rotation.
  A new path-reversal reassociation is genuinely required (confirmed: no `reverse`/`reroot_chain` exists).
- **The unified actuator is already shape-parameterized.** `try_bind_graph_actuation` (join_order.rs:2292)
  scratch-recognizes via `GraphShape::recommend`, soft-margin-gates, calls `reroot_galaxy_join_tree`, then
  `shape.set_applied_root`. The dispatch loop (binder.rs:1126) runs `[Galaxy, Bridge]` and accrues the
  outcome via `accrue_graph_order_decision(shape, …)`. Adding a `Chain` arm is mostly filling in
  `GraphShape`'s methods — except the reroot step, which hardcodes `reroot_galaxy_join_tree`.
- **The Bridge/Chain disambiguation is load-bearing (and fixes a latent interaction).**
  `bridge_order_recommendation` reads `links.first()` with **no length check**, and `analyze_bridge`
  returns `k` links for a k-hop chain. So a chain query **already** feeds the Bridge *actuation* arm, which
  can re-root around only its *first* junction. Today this is dormant (the 203/204 chain fixtures are clean
  FK chains that keep), but landing Chain without gating Bridge would let a chain double-actuate or fall
  through to a partial bridge re-root. The `Galaxy XOR Bridge` invariant must become a **3-way partition**:
  Galaxy / single-junction Bridge / multi-junction Chain. The partition spans **two** surfaces: (1) the
  *actuation recommend* (Bridge gated to one junction), and (2) the *verdict log* — a chain also carries
  `bridge_order`, so the per-junction `BridgeRoot` verdict (judged only at `links.first()`) is **suppressed**
  when a chain is recognized, leaving the authoritative `BridgeChainRoot`; otherwise
  `information_schema.optimizer_runtime_verdicts` double-counts. The observe-time per-junction bridge *rows*
  (ADR 196/204 EXPLAIN observability) remain additive and unaffected — only the *verdict* is suppressed.

## Decision

**D0 — Phase-0 fragmentation gate (falsification-first).** Before building the arm: prove a *reversed* 3-hop
chain (`D ⋈ B2 ⋈ B1 ⋈ F`) binds + fragments + executes with a **distributed digest byte-identical to the
AST order** (the 202 digest-gate rhythm). Inner joins are multiset-commutative, and the reversed chain is a
left-deep inner chain — the 200/202 fragmenter family — so this should hold; the gate *proves* it. **If it
fails, 205 becomes the fragmenter extension** and actuation is the follow-on (the honest fork).

**D1 — Path-reversal reassociation.** `reverse_bridge_chain_join_tree(plan) -> Option<LogicalPlan>`: walk
the left-deep chain collecting its leaves, per-edge join conditions, and per-join `(kind, strategy_intent)`,
then rebuild left-deep from the `D` end, re-orienting each `on` for its new left/right position. Sibling of
`reroot_galaxy_join_tree` (a path reversal, not a star rotation); reuses `galaxy_leaf_label` (recurses
Filter/Project, the 202 helper) for leaf identity, and carries join meta **by original edge index** (the
202 F1 trap — never by rebuilt position). `None` for any shape that is not a left-deep all-leaf inner chain.

**D2 — `GraphShape::Chain` arm + a `reroot` dispatch method, with Bridge disambiguated.**
- Add `GraphShape::Chain`; `decision_kind → BridgeChainRoot`, `recommend → analyze_bridge_chain(…).map(
  bridge_chain_order_recommendation)`, `set_applied_root → set_applied_bridge_chain_root`.
- Add `GraphShape::reroot(plan, rec) -> Option<LogicalPlan>`: `Galaxy|Bridge → reroot_galaxy_join_tree(plan,
  rec.pivot, rec.preferred_root)`; `Chain → reverse_bridge_chain_join_tree(plan)`. `try_bind_graph_actuation`
  calls `shape.reroot(...)` instead of the hardcoded galaxy reroot.
- **Gate Bridge to a single junction (two surfaces):** `GraphShape::Bridge::recommend` returns `None`
  (NotCandidate) when `analyze_bridge` yields `links.len() != 1`, so multi-junction queries are Chain's
  exclusively (actuation). AND the bridge *verdict* arms (`measured_verdicts_from_summaries` + the EXPLAIN
  ANALYZE verdict row) gate on `bridge_chain_order.is_none()`, so a chain emits only `BridgeChainRoot`, never
  a partial first-junction `BridgeRoot`. Together these restore a clean 3-way partition on both surfaces and
  close the latent partial-bridge-on-a-chain path.
- Dispatch loop becomes `[Galaxy, Bridge, Chain]`. Bounded to the 204 recognizer (`≤ MAX_BRIDGE_CHAIN_HOPS`).

**D3 — Gating + accrual reuse (no new machinery).** Reuse the soft margin (190/191), the no-op-recorder
scratch recognition (cannot recurse / pollute the log), and `accrue_graph_order_decision` — so the chain
re-root accrues a real `bridge_chain_root` decision (`reordered` / `kept_optimal` / `kept_thin_margin` /
`declined`), **upgrading** 204's verdict-only `BridgeChainRoot` to a committed decision.

**The actuated-order *verdict* is deferred (the re-recognition risk materialized).** Unlike the symmetric
galaxy/bridge re-root, the reversed chain does **not** re-recognize: interior junctions fail the inverse-F1
gate in reverse (a bridge becomes dimension-like, e.g. `order_lines`=5000 `< line_promotions`=20000). The
galaxy verdict-after-actuation works *only because* re-recognition supplies the rerooted plan's edge
targets, which key the measured costs; the asymmetric chain can't supply reversed-plan targets, so a
verdict computed from the recognized (AST) evidence would key to the wrong joins. So the chain receipt is
delivered via the named fallback — the original chain evidence is carried so the row renders `reordered;
applied_root=D` — but the actuated-order verdict is **suppressed** (a named follow-on). The 204 observe
verdict still judges non-actuated (kept) chains. Recursion terminates trivially: the reversed plan does not
re-recognize as a chain (`NotCandidate`), so no nested actuation.

**D4 — Invariants / boundaries.** Distributed result byte-identical-modulo-order; the EXPLAIN chain row
flips `observed → reordered; applied_root=D`. Binder + explain + the existing decision log. No new
fragmenter, executor, storage, Wendelon, PBC, wire, or grammar. Galaxy / single-bridge / snowflake / star
unchanged.

## Honesty gate

`three_hop_chain_reroots_to_cheaper_driver_end_and_executes_distributed`: a reachable 3-hop chain with a
selective predicate on `D` (the 199/204 reorder pattern) **actuates** to `D ⋈ B2 ⋈ B1 ⋈ F`, distributed
digest byte-identical-modulo-order to the AST order, decision accrues `bridge_chain_root, reordered`, and
the EXPLAIN chain row shows `applied_root`. Must **fail on `main`** (204 is observe-only — no chain in the
actuation loop).

Delivered as `two_hop_bridge_chain_reroots_to_cheaper_driver_end` (EXPLAIN `reordered; applied_root`) +
the Phase-0 distributed digest gate (`three_hop_chain_reverse_digest_matches_ast_order_distributed`).

**Load-bearing discriminators:** the **Phase-0 digest gate** (the reversal preserves the result, distributed);
`three_hop_chain_is_recognized` / `two_hop_bridge_chain_is_recognized` (F-end cheapest → no actuation,
EXPLAIN stays `observed`); `chain_query_does_not_trigger_single_bridge_actuation` (the Bridge-disambiguation
guard — a chain accrues a `bridge_chain_root` decision, never a `bridge_root` one); and
`chain_verdict_log_has_no_partial_bridge_verdict` (the verdict-surface partition — a chain emits only
`BridgeChainRoot`, never a partial `BridgeRoot`). The actuated-order verdict discriminator is deferred (see
D3). Paired regression: the 194–204 suites
byte-identical; single-bridge actuation (`bridge_reroots_to_selective_dimension`) still fires for a true
one-junction bridge.

## Consensus (transparent)

**3-of-3** — the unanimous named follow-on from 203/204. Falsification-first applies to the *fragmentation*
(D0), not the cost model (proven directional by 204's Δ1). Two corrections grounded in: the reroot cannot
reuse the star-pivot `reroot_galaxy_join_tree` (a path reversal is new mechanism), and the Bridge actuation
arm must be gated to a single junction (the 3-way partition) or a chain double-actuates.

## Forward arc

`204 bounded N-hop chain evidence → 205 bridge chain actuation (this) → cross-shape joint actuation (the 201
graph_actuated XOR boundary, now a 3-way partition) → memo only when receipts show local decisions fighting`.

## Deferred (named)

The **actuated-order verdict** (D3 — judging the reversed execution needs reversed-plan edge targets the
asymmetric chain does not re-recognize; build the reversed `BridgeChainEvidence` with reversed-plan targets,
or judge via the applied-root marker); drive-from-a-middle-bridge (general join-order search, not a 2-ended
reversal); chains beyond `MAX_BRIDGE_CHAIN_HOPS`; the peak-vs-sum cost model (204 deferred — actuation
inherits peak); cross-shape joint actuation; outer joins; durable optimizer evidence.
