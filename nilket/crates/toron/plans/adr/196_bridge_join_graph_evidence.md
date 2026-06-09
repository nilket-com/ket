# ADR 196: Bridge / Many-to-Many Join Graph Evidence

**Status:** Promoted. **Migration:** `plans/impl/196_bridge_join_graph_evidence.md`.

ADR 194 made the conformed galaxy observable; 195 measured its fan-out. ADR 196 opens the second
non-tree graph class — the **bridge** (junction): `Fact ⋈ Bridge ⋈ Dimension`, where `Bridge` links two
relations many-to-many through two distinct foreign keys. A bridge query binds **as a snowflake** today
(Fact root, Bridge = composite-build branch, Dim = leaf), so bridge evidence is an observe-only sibling
annotation that distinguishes a many-to-many junction from an ordinary 1:N branch, reusing 194's
role-neutral substrate and 195's runtime machinery. No actuation, no ordering.

## Grounding (the discriminator is the cut — and it is verified)

A bridge node and a snowflake branch node are **structurally identical** in the role-neutral trace:
both are intermediaries with two distinct *own-column* key signatures, each to one relation
(`bridge`: `b.fid → {fact}`, `b.did → {dim}`; `customers`: `c.id → {orders}`, `c.region_id →
{regions}`). The trace cannot see PK-vs-FK, so structure alone cannot separate them. The only
role-neutral separator is **cardinality** — the inverse of ADR 194's F1 row gate — and it was verified
against the real substrate:

- A branch bind (`orders ⋈ customers ⋈ regions`) captures `customers` `endpoint_rows = 5000`
  consistently on both its edges, vs its fact-side probe `orders` `endpoint_rows = 100000`. The
  intermediary is **smaller** than its fact → *dimension-like* → not a bridge.
- On the promoted bridge fixture, the junction is not dimension-like relative to its fact-side probe:
  the observed bridge rows are at least as large as the fact-side probe. The recognizer uses that
  conservative inverse-F1 gate (`endpoint_rows(B) >= endpoint_rows(F)`) rather than a structural label.

So the recognizer compares the intermediary's `endpoint_rows` (194's per-relation estimate, captured on
`left/right_endpoint_rows`) to its **fact-side probe's** `endpoint_rows`: bridge iff `>=`. This is the
same gate F1 added, inverted; the data is present and consistent (probed, not assumed).

## Decision

**D1 — `analyze_bridge` over the role-neutral edges (observe-only, sibling type).** Add
`BridgeOrderEvidence` (sibling of `GalaxyOrderEvidence`, not an extension) and
`analyze_bridge(&[JoinOrderEdgeEvidence]) -> Option<BridgeOrderEvidence>`. An intermediary node `B` is a
relation that is the **`dimension`** of one inner-equi edge (incoming from fact `F = that edge's
probe`) **and** the **`probe`** of another inner-equi edge (outgoing to `D = that edge's dimension`),
with the two `B`-side signatures **distinct**. `B` is a **bridge** iff it is **not dimension-like**:
`endpoint_rows(B) >= endpoint_rows(F)`. Decline when `B` is dimension-like (snowflake owns it), the
signatures coincide (conformed dimension — 194 owns it), non-inner/non-equi, a cycle, or any required
`endpoint_rows` is missing (conservative — no cardinality evidence, no bridge claim). Never actuates.

**D2 — Bridge plan receipt.** `join_order_bridge` row: `target = F->B->D`, detail = the distinct
signatures (`B on {F-side sig}={B.fid} and {B-side sig}={D.id}`) and the estimated fan-out
ratio (`endpoint_rows(B) / endpoint_rows(F)` as a float). A gauge, not a steer.

**D3 — Bridge runtime fan-out (the dual rung, reusing 195).** Under `EXPLAIN ANALYZE`,
`join_order_bridge_runtime` rows per bridge-incident edge carry the measured **fan-out ratio =
`output_rows / probe_rows`** at the bridge edge — the M:N explosion made real, distinct from 195's
estimate-accuracy ratio. Reuses 195's `join_order_runtime_measurement` / replay-id correlation; the
fan-out is computed from the observation summary (`output_rows`, `probe_rows`).

**D4 — Replay-id union (195 pattern).** `star_order_runtime_replay_ids` unions `bridge_order` edge
targets explicitly, so bridge measurement is gathered from the bridge substrate, not inherited from the
tree allowlist.

**D5 — No actuator; additive; bounded.** No global graph ordering, no bridge actuation (deferred — the
gauges phase for non-tree topologies). Bridge evidence rides sibling fields, additive to the tree +
galaxy receipts; a bridge query emits its snowflake receipt AND a bridge row, and galaxy + bridge rows
coexist when both shapes are present. Existing star/snowflake/galaxy output stays byte-identical.
Binder/explain/analyze only; no fragmenter, executor, storage, Wendelon, PBC, wire, or grammar change.

## Honesty gate

`bridge_evidence_measures_many_to_many_fanout`: a reachable `fact ⋈ bridge ⋈ dim` query whose bridge
fans out M:N emits a `join_order_bridge` plan row identifying the junction + distinct signatures, and
under `EXPLAIN ANALYZE` a `join_order_bridge_runtime` row whose measured fan-out ratio (`output_rows /
probe_rows`, when `probe_rows > 0`) is **> 1**. Must **fail on `main`** (no bridge recognizer). The load-bearing partner gate
`bridge_recognizer_distinguishes_bridge_from_snowflake_branch`: the **same** structural query shape with
a *dimension-like* intermediary (small branch, e.g. `orders ⋈ customers ⋈ regions`) emits **no** bridge
row — proving the cardinality gate, not structure, does the work. Supporting:
`bridge_recognizer_declines_conformed_dimension`, `bridge_recognizer_rejects_cyclic_joins`,
`galaxy_and_bridge_evidence_coexist`. Paired regression:
`snowflake_galaxy_and_star_evidence_byte_identical`.

## Consensus (transparent)

3-of-3 (Claude, Codex, Gemini). All three converged on: `analyze_bridge` as a sibling recognizer over
194's `JoinOrderEdgeEvidence`; the intermediary defined as dimension-of-one-edge / probe-of-another on
distinct signatures; the **inverse-F1 cardinality gate** (`endpoint_rows(B) >= endpoint_rows(F)`) as
the bridge-vs-branch discriminator — explicitly *not* structure, the same discipline 194 used to avoid
over-claiming galaxy on an alias star; runtime fan-out `output_rows / probe_rows`; the `bridge_order`
replay-id union; and observe-only, cockpit-only boundaries. The cardinality discriminator was probed
against the real substrate before promotion, with the conservative no-claim path for missing or zero
denominators. The galaxy/bridge cost-model + ordering leap stays deferred until both non-tree graph
classes are visible and measured.

## Forward arc

`195 galaxy runtime → 196 bridge evidence (this) → galaxy/bridge cost model + ordering → measured order
verdict → bounded graph topology actuation → memo`. 196 completes the observe+measure gauges for both
non-tree graph classes; the cost model then has both shapes measured before it builds graph-ordering
authority.

## Deferred (named)

Bridge/galaxy actuation; global graph ordering / cost model; galaxy & bridge measured order verdict;
catalog PK/FK structural classification; multi-hop bridge chains (length > 1); outer joins;
memo/Cascades; durable optimizer evidence.
