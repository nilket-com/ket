# ADR 199: Bridge Actuation

**Status:** Promoted. **Migration:** `plans/impl/0199_bridge_actuation.md`.

198 actuated the galaxy — the first plan-changing graph cut. ADR 199 is the **second and final actuate
rung of the inner-equi graph arc**: actuate the recognized many-to-many **bridge** (`F ⋈ B ⋈ D`) when
rooting the selective dimension beats the FROM order, by reassociating the inner-join tree and binding
the re-rooted plan through the **existing snowflake staging path** — byte-identical results,
distributed-digest-proven. Deliberately narrow: the single reachable 3-relation bridge, gated.

## Grounding (what is reachable, what is already done, the one new piece)

- **A re-rooted bridge is a snowflake — reuse the engine, as 198 did.** A bridge binds as a snowflake
  today (196): `Join(Join(F, B), D)` (F root, B composite-build branch, D leaf). `reroot_galaxy_join_tree`
  (`join_order.rs:579`) is **role-neutral** — it swaps the outer-right leaf with the inner-left leaf
  around the inner-right pivot, `Join(Join(A, M), C) → Join(Join(C, M), A)`. Feed it the bridge tree and
  it returns `Join(Join(D, B), F)` — D root, B pivot-branch, F leaf, a snowflake the existing
  actuator/fragmenter already stages. So the reassociation, the staging path, and the 178/186 schema
  restoration (`restore_declared_star_order_project`, `join.rs:692`) are **all reused from 198**,
  exactly as 198 reused 192/193's fragmenter.

- **Observe + measure are already built (196/197).** `analyze_bridge` recognizes the junction by the
  conservative inverse-F1 cardinality gate; 197 emits a preferred order + a measured
  `join_order_graph_verdict`. Only the actuate rung is missing — the 197→198 step, now for the bridge.

- **The one genuinely new piece: the bridge's *order* cost must be sharpened from an endpoint-row gauge
  into a relation-rooted decision — and the data for it is already present.** Grounded in the structs,
  not asserted:
  - `BridgeLink` (`join_order.rs:428`) carries only `bridge_rows` and `fact_rows`.
    `bridge_order_recommendation` therefore sets `action = Reordered` iff `bridge_rows > fact_rows` and
    returns `preferred_root = "filter_first"` — an abstract *strategy label*, not a relation. That gate
    proves a **fan-out exists**; it does **not** prove **rooting at D is cheaper**.
  - **Why the proxy is *doubly* blind — and the win condition it exposes.** The reachable fixture is
    `orders(1000) ⋈ order_items(10000, fans out) ⋈ products(100)`. The proxy sees `bridge_rows 10000 >
    fact_rows 1000` and would reroot — but under FK integrity with **no predicate on D**, `|D⋈B| ≈
    |F⋈B| ≈ 10000` (every bridge row matches one product), so rerooting **buys nothing**. The reroot
    wins **only when D's *effective* rows are reduced** — a `WHERE products.… = …` predicate or genuine
    join selectivity — shrinking `B⋈D` below `F⋈B`. The endpoint-row proxy sees neither the join
    selectivity nor a dimension predicate; it would reroot any M:N junction for free.
  - The sound signal is already on the edges `analyze_bridge` consumes. `JoinOrderEdgeEvidence` carries
    `estimated_rows` — the per-edge **join-result** estimate (selectivity-/predicate-aware, 149/150),
    the *same field the galaxy roots on* (`GalaxyEdgeEvidence.estimated_rows` via `edge_estimate_between`).
    `analyze_bridge` already holds both incident edges: `incoming` (`F⋈B`) and `outgoing` (`B⋈D`). So
    the relation-rooted cost is a one-line comparison — lead with the relation whose join *into the
    bridge* is the smaller build, `outgoing.estimated_rows < incoming.estimated_rows` ⟹ root D — the
    galaxy's `argmin`-over-edge-estimates discipline applied to the bridge's two edges, with the same
    conservative "any estimate missing → decline" short-circuit.
  - The 196 `bridge_rows >= fact_rows` inverse-F1 gate is the **recognizer** (is `B` a bridge?) and
    **stays unchanged**. D1 sharpens only the **order cost** — a different question (which root is
    cheaper?), answered by the edge estimates, not endpoint rows.

- **Fragmentation is proven, not assumed (the 193/198 discipline).** `(D ⋈ B) ⋈ F` is a snowflake, but
  whether it *fragments + digest-matches* is the Phase 0 gate — a distributed fixture proven **before**
  any actuation code ships. **Falsification risk to clear in Phase 0:** the win requires a reachable
  bridge with a *selective predicate on D* that both reroots under D1 **and** fragments + digest-matches.
  If the only reachable bridges are clean FK joins with no dimension predicate, or the predicate-on-D
  reroot hits a fragmenter cliff, the cut re-scopes to gate-exists-declines or escalates — the same
  193/194 falsification discipline, run before any actuation code.

## Decision

**D1 — Sharpen the bridge order recommendation to a relation-rooted edge-estimate cost.**
`bridge_order_recommendation` compares the two incident edge estimates — `incoming.estimated_rows`
(`F⋈B`) vs `outgoing.estimated_rows` (`B⋈D`) — and sets `preferred_root` to the **relation** rooting the
smaller build (D ⟹ filter-first, F ⟹ fan-out-first / FROM root); `action = Reordered` when it differs
from the FROM root. Conservative decline (`None`, kept) when either edge estimate is absent — mirroring
`galaxy_order_recommendation`'s `?` short-circuit. Carry the two edge estimates onto `BridgeLink`. Stays
row-only (197/181) — edge estimates are rows. **The fan-out *ratio* survives as a derived render
gauge** (the M:N magnitude is useful telemetry); the recommendation's **action signal** is now
relation-rooted, so `EXPLAIN`'s `would_reorder` prediction and `EXPLAIN ANALYZE`'s actuation stay
consistent — the cockpit never promises a reorder the actuator won't perform.

**D2 — `try_bind_bridge_actuation`, sibling of `try_bind_galaxy_actuation` (`join_order.rs:2050`).** On a
recognized bridge whose sharpened recommendation prefers a different root and clears the D3 margin,
reassociate via the **existing** `reroot_galaxy_join_tree` and bind the re-rooted snowflake through the
existing staging path; restore the AST schema with `restore_declared_star_order_project` (178/186).
Recognition runs on the actuation-free no-op-recorder binder (`without_decision_recording()` — recognition
never pollutes the decision log). Wired in the `LogicalPlan::Join` arm alongside the galaxy actuator
(a query is a galaxy XOR a bridge XOR a star — recognizers are mutually exclusive by their gates). A named
follow-on unifies galaxy + bridge into one `try_bind_graph_actuation` once both have landed; folding it
now would reopen landed 198 code for no behavior gain.

**D3 — Soft-margin gate (190/191).** Actuate iff the preferred root beats the declared root by the
configured soft margin on the edge-estimate costs — no reroot on a thin win.

**D4 — Selected-preflight decline (193/198).** Actuate iff the re-rooted plan's own
`preflight_star_order_strategy` proves a valid, fragmentable plan; strategy drift / nested-probe cliff →
decline to the AST order, byte-identical, observe-only `would_reorder`. A bridge that cannot safely
re-root behaves exactly as 197.

**D5 — Receipt + verdict reflect the *applied* reroot.** A new `set_applied_bridge_root` (mirror 198's
`set_applied_galaxy_root`) flips the `join_order_bridge` order detail `action=would_reorder` →
`action=reordered; applied_root={D}`; 197's `join_order_graph_verdict` now judges the **applied** bridge
order, newly enabling `OverEager`/`MisRanked` for bridges (the run reordered; measurement can vindicate
or fault it), not only `MissedWin`.

**D6 — Boundaries.** Toron binder + the existing snowflake staging path. No new fragmenter capability, no
executor kernel, scheduler, storage, Wendelon, PBC, wire, or grammar change.

## Guards reconciled (discipline #4)

- **`adr197_graph_order_is_observe_only` survives.** It pins that the recommendation functions contain no
  `bind_local_star_actuation` — the recommendation stays pure; actuation lives in the new sibling, exactly
  as 198 kept the galaxy recommendation pure. D1's edge-estimate sharpening adds no actuation.
- **`adr197_graph_order_cost_is_row_only` survives, strengthened** — add `bridge.contains("estimated_rows")`
  so the bridge is pinned to the same row-only edge cost as the galaxy.
- **`adr196_bridge_is_observe_only` is retargeted** → `adr199_bridge_recognizer_is_pure`: its
  system-level intent ("the bridge never actuates") inverts; the surviving, still-true property is
  recognizer/recommendation purity (`analyze_bridge` carries no actuation; the verdict stays render-time).
  Add five `adr199_bridge_actuation_*` guards mirroring 198's galaxy set (reuses-snowflake-machinery,
  respects-soft-margin, recognition-recursion-free, binder-only, reroot-reused-not-new) plus
  `adr199_bridge_recommendation_sharpens_beyond_fanout_proxy`.

## Honesty gate

`bridge_reroots_to_selective_dimension_and_executes_distributed`: a reachable `F ⋈ B ⋈ D` with a
**selective predicate on D** (so `B⋈D` build estimate is cheaper than `F⋈B`) **executes distributed**
with `D` as the driving root, **digest-matches** the AST-order baseline (the 193/198 proof shape), and
emits a `join_order_bridge` receipt with `action=reordered; applied_root={D}`. Must **fail on `main`**
(197 only recommends; the executed plan is the AST order).

**Load-bearing discriminator — `bridge_fans_out_but_dimension_not_selective_is_kept`:** the **same**
fan-out bridge with **no predicate on D** (FK-complete, so `B⋈D ≈ F⋈B` — the *old endpoint-row proxy
would still reroot*) is **kept at the FROM order**. This **fails under the blunt proxy and passes only
with D1's edge-estimate cost** — so it locks the cost sharpening as a real decision, not a constant (a
test locks a design only if it fails under the rejected alternative). The predicate-on-D vs no-predicate
pair *is* the proof that D1 costs effective rows, not raw cardinality.

Partners: `bridge_already_optimal_order_kept`, `bridge_declines_reorder_on_fragmenter_cliff`,
`bridge_nested_restoration_preserves_output_schema`, `bridge_reorder_respects_soft_margin`,
`rune_bridge_reorder_matches_sql` (Rune parity — Rune is the primary interface), and an `EXPLAIN ANALYZE`
`bridge_overeager_verdict_faults_applied_root`. Paired regression: the 194–198 galaxy/bridge/snowflake
suites byte-identical where no actuation fires (the bridge observe receipt sharpens to the relation-rooted
prediction — a deliberate, reconciled change, not a silent regression).

## Consensus (transparent)

**3-of-3 (Claude, Codex, Gemini)** on topic and mechanism: a bounded bridge actuation cut reusing 198's
role-neutral `reroot_galaxy_join_tree` + snowflake staging + 178/186 restoration + the 185/192/193
preflight fences, proven by a distributed digest fixture (Phase 0, before any actuation code); a sibling
`try_bind_bridge_actuation`; applied-reroot receipts/verdicts; binder-only. All three independently
converged on the **load-bearing correction — the blunt `bridge_rows > fact_rows` proxy must not drive
actuation; the cost must be relation-rooted from the existing `B⋈D` edge estimate.**

**Resolved divergence (surfaced, not papered over):** *where the sharpened cost lives.* Claude + Gemini
(2-of-3) sharpen the single `bridge_order_recommendation` to relation-rooted; Codex preferred keeping
197's observe gauge byte-identical and adding a **separate** actuation-only comparator. Resolved toward
the single relation-rooted recommendation, grounded: 197's observe receipt exists to *predict* what
actuation does, so a gauge that recommends `would_reorder` on the blunt proxy while the actuator declines
on the sharp cost is a cockpit that lies about its own behavior. The single sharpened recommendation keeps
observe ⟺ actuate consistent. Codex's concern is honored — the fan-out **ratio** survives as a derived
informational gauge (demoted from action-signal to telemetry, not lost). If implementation finds the
`B⋈D` edge estimate unreliable on the reachable fixtures, the conservative path is decline, never a
blunt-proxy fallback (the discriminator gate pins this).

## Forward arc

`198 galaxy root-swap → 199 bridge actuation (this; completes the inner-equi graph actuate rung) →
n-fact / multi-hop graph ordering + actuation → graph verdict accrual into the decision log → memo only
when receipts show local decisions fighting`. After 199 both non-tree inner-equi classes observe →
measure → judge → **actuate**; the next frontier is breadth (n-fact / multi-hop) and workload-scale
telemetry, with the memo deferred until receipts prove local decisions conflict.

## Deferred (named)

n-fact galaxy / multi-hop bridge chains (length > 1) ordering + actuation; unify galaxy+bridge into one
`try_bind_graph_actuation`; always-on graph verdict accrual into the 187/188 decision log; general
join-order search / memo / Cascades; cross-shape joint actuation; catalog PK/FK structural classification
(would make the recognizers exact vs heuristic); outer joins; durable optimizer evidence.
