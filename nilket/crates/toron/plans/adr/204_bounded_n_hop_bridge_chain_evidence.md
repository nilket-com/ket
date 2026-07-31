# ADR 204: Bounded N-Hop Bridge Chain Evidence

**Status:** Promoted. **Migration:** `plans/impl/204_bounded_n_hop_bridge_chain_evidence.md`.

203 opened the path topology at its floor — *exactly* a 2-hop chain `F ⋈ B1 ⋈ B2 ⋈ D`. The `len()==2` bound
was a deliberate floor, not the shape's natural breadth: real bridge chains run longer
(`order→line→shipment→carrier→region`). ADR 204 generalizes recognition and the compounded verdict to a
**bounded N-hop** chain `F ⋈ B1 ⋈ … ⋈ Bk ⋈ D`, still **observe-only** — no re-root, byte-identical
execution. The chain re-root is the named follow-on (205).

## Grounding (verified against the code) — why evidence (not actuation), and the exact shape

The council weighed **actuate the 2-hop we already recognize** vs **widen evidence to N-hop**. Evidence won
on four grounded facts:

- **The 203 compounded model is proven *synthetically*, not on real reachable fixtures.** The
  `Vindicated→MissedWin` flip in `chain_verdict_folds_middle_hop_explosion` comes from a *hand-injected*
  10M-row middle observation. The fold's *correctness* is locked (fold-vs-endcap discriminator); its
  *predictiveness on reachable chains* is not. Actuation re-roots plans on that model — premature.
- **The 2-hop bound is arbitrary.** Actuating only `len()==2` actuates a special case of an unproven
  general model. Widening evidence exercises the cost model at the shape's natural breadth first.
- **Risk asymmetry.** Actuation can *regress* a plan; evidence is byte-identical and harmless. Ground the
  model before it changes a plan — the same falsification-first rule that made 203 observe-only still
  applies, because the model is *still new at N*.
- **The generalization is real but bounded — not search.** Verified:
  - `analyze_bridge` already returns **k links** for a k-hop chain (one per qualifying junction); the 203
    recognizer simply rejects `links.len() != 2`. The unlock is the **stitch generalization**, not new
    recognition.
  - `bridge_chain_order_recommendation` reads only `hops.first()/last()` (the two path ends) — **already
    N-hop-ready, untouched.**
  - `bridge_chain_measured_costs` peaks over **one** middle (`head.outgoing_target`); the real cost-model
    gap is peaking over **all** middles.
  - `bridge_chain_order_target_label` hardcodes `chain[F->B1->B2->D]` — it **drops interior bridges** and
    must list the full path.

**The load-bearing invariant — the verdict stays 2-ended.** `bridge_chain_measured_costs` returns **exactly
two** labelled costs — `(head.fact, …)` and `(tail.dim, …)` — for *any* N. So `measured_graph_order_verdict`
is still a 2-way `F`-vs-`D` argmin: the N middles never become new candidate ends, they only **compound into
the two end costs**. This is why the recommendation needs no change, and why N-hop evidence is a *bounded
generalization* and not a step toward N-way join-order search.

## Decision

**D1 — Bounded N-hop recognizer (dark, at bind time).** Generalize `analyze_bridge_chain` from
`links.len()==2` to a **single connected chain over a capped walk**: pick the unique head link whose `fact`
is no junction's `bridge` (the free fact end `F`), then walk successors by `link_{i+1}.fact == link_i.bridge`
(contiguous `link_i.dim == link_{i+1}.bridge`), requiring the walk to consume **all** links
(`order.len() == links.len()`) over `k+2` **distinct** relations. Decline to `None` for: branching (a reused
`fact`), a cycle/repeated relation, a **disconnected** link set (a chain co-occurring with an unrelated
bridge — multi-component recognition is deferred), `k < 2`, or `k > MAX_BRIDGE_CHAIN_HOPS`. **Termination is
structural** — the walk consumes distinct relations and declines on repeat, so it is intrinsically finite
(`O(links)`, not search); `MAX_BRIDGE_CHAIN_HOPS` (= 4) is the **supported breadth** for this cut
("we vouch for the compounded model only this deep"), not what makes the walk bounded. Bind-time,
byte-identical.

**D2 — Full-path label.** Generalize `bridge_chain_order_target_label` to `chain[F->B1->…->Bk->D]` (head
fact, every hop's bridge, tail dim). The detail label already iterates all hops; the recommendation is
unchanged.

**D3 — Compounded verdict over *all* middles (the cost-model generalization).** The verdict remains a 2-way
`F`-vs-`D` decision; the generalization folds every middle into the two compounded end costs, never into a
new end. Generalize `bridge_chain_measured_costs`: the declared end RAN, so its cost is the **peak** (`max`)
over the *measured* `F`-entry (`head.incoming_target`) **and every measured inter-bridge middle** (each
non-tail hop's `outgoing_target`); the un-run end is the *estimated* peak over the `D`-entry
(`tail.outgoing_edge_rows`) **and every estimated middle**. An explosion in **any** interior middle (not
just the head's) lifts the declared peak above the counterfactual → `MissedWin`. Peak is the 203 combinator
(so `k=2` is byte-identical) and a **deliberate heuristic** — it catches any single-hop explosion and
proportional drift, and is *tested* by the measured verdict, not claimed optimal (sum-/product-compounding
is the named follow-on if a reachable fixture shows peak mis-ranking).

**D4 — Earn the breadth with a reachable fixture.** The cut binds an actual **≥3-hop** chain query the
binder produces (a 5-relation `F⋈B1⋈B2⋈B3⋈D` seeded with monotonic-increasing cardinality so the inverse-F1
gate passes at *each* junction) — not an observation injection — so N-hop evidence proves the shape is
**reachable**, clearing the bar the 2-hop synthetic test did not.

**D5 — Observational invariant / boundaries.** No re-root, no actuator, no new decision kind (the chain
still rides `BridgeChainRoot` verdict-only). Execution plans + EXPLAIN tree receipts byte-identical to
`main`. Binder + explain + the existing verdict log. No fragmenter, executor, storage, Wendelon, PBC, wire,
or grammar change. **Actuation explicitly deferred to ADR 205** (path-reversal re-root, Phase-0-gated).

## Honesty gate

`three_hop_chain_interior_middle_explosion_is_missed_win`: a **reachable** `F⋈B1⋈B2⋈B3⋈D` whose *interior*
middle `B2⋈B3` (not the head's `B1⋈B2`) explodes emits a `bridge_chain_root` `MissedWin`. Must **fail two
ways on the rejected alternatives**: under the 2-hop recognizer (`len()!=2` → no chain row at all for a
3-hop query) AND under the 2-hop cost model (peaks only `head.outgoing` → misses the interior explosion →
`Vindicated`). Plan + results byte-identical.

**Load-bearing discriminators:** `three_hop_chain_is_recognized` (the bounded walk accepts `k=3`; the 203
recognizer returned `None`); `branching_bridge_chain_is_declined` (a fork → `None`);
`chain_plus_unrelated_bridge_declines_chain` (a disconnected second junction → the 199 bridge row, **no**
chain row); `over_cap_chain_is_declined` (`k > cap` → `None`); and `two_hop_chain_row_and_verdict_unchanged`
(the `k=2` instance is byte-identical). Paired regression: the 194–203 EXPLAIN + execution suites
byte-identical.

## Consensus (transparent)

**2-of-3 (Codex, Claude)** on **bounded N-hop evidence, actuation deferred**. Codex grounded the rung:
recognize+measure the shape reliably at its real breadth before re-rooting plans; the cost model must be
boring on reachable fixtures first. Claude led with 2-hop *actuation* and **conceded on the grounding** —
the 203 model is synthetic, the 2-hop bound arbitrary, the risk asymmetric, and observe-only is consistent
with 203's own justification. Three corrections folded into this promotion: "bounded" is a *claim* boundary
over a structurally-finite walk (not what makes it terminate); the recognizer accepts a *single connected*
chain (disconnected multi-component declines); and the peak combinator is a named, tested heuristic
(sum-compounding deferred). The chain *re-root* is the unanimous named follow-on.

## Forward arc

`203 2-hop chain evidence → 204 bounded N-hop chain evidence (this) → 205 bridge chain actuation (the
path-reversal re-root, Phase-0-gated by a chain-fragmentation proof) → cross-shape joint actuation → memo
only when receipts show local decisions fighting`.

## Deferred (named)

Bridge chain actuation (the path-reversal re-root, 205); chains beyond `MAX_BRIDGE_CHAIN_HOPS`; branching /
bushy bridges; multi-component link sets (a chain co-occurring with an unrelated bridge/chain);
sum-/product-compounded chain cost (peak under-counts cumulative drift in a small interior middle);
cross-shape joint actuation (the 201 `graph_actuated` XOR boundary); general join-order search / memo /
Cascades; durable optimizer evidence.
