# ADR 194: Role-Neutral Join Graph Evidence Substrate

**Status:** Promoted. **Migration:** `plans/impl/194_role_neutral_join_graph_evidence_substrate.md`.

ADR 177–193 made the **tree-shaped** join-order path (flat stars, single-root snowflakes) visible,
measurable, and actuatable. 193's forward arc names the next rung as "galaxy evidence" — the
multi-fact warehouse query where two facts share a conformed dimension (`sales ⋈ product ⋈ returns`).
ADR 194 is the **substrate** that rung requires. It does not actuate, fragment, or reorder a galaxy;
it makes the multi-fact join graph **observable**, and proves — with the negative result baked into
the design — why the obvious "recognize a galaxy over the existing edge stream" cut could not work.

## Grounding (the precise blocker: trace roles, not SQL expressibility)

The promoted topic was "galaxy evidence." A reachability probe (run during the final iteration, kept
as Phase 0) corrected the framing, and the correction is load-bearing:

- **SQL *can* express the conformed two-fact query.** `sales ⋈ product ⋈ returns` (both facts joining
  `product`) binds cleanly. What it *cannot* do is survive in the evidence substrate as a galaxy.
- **The edge substrate records only the left-deep trace role.** At the single capture site
  (`binder.rs:1910-1932`), each `JoinOrderEdgeEvidence` keeps `probe = relation_label_for_join_keys(left)`
  and `dimension = relation_label_for_join_keys(right)` — i.e. **probe = the key-source label on the
  left input, dimension = the key-source label on the right input**. Those labels are trace roles,
  not role-neutral graph endpoints. The richer facts present at that line — both resolved key column
  sets (`left_resolved_keys`/`right_resolved_keys`) and the per-source row/NDV facts carried in
  `join_order_sources` — are not preserved on the edge. Only the directed role labels survive.
- **So every connected multi-fact query collapses to a single-root tree.** `analyze_snowflake` builds
  `dimension_parent: Map<dimension, probe>`; in a left-deep bind every relation after the leading
  `FROM` table is the right input of exactly one join → exactly one parent, one root. `sales ⋈ product
  ⋈ returns` records as `sales→product`, `product→returns` — a snowflake — even though the role-neutral
  truth is that **`product` is a conformed dimension joined by two facts on the same key**. That truth
  is erased at capture, not at recognition.
- **The existing galaxy guard is synthetic.** `galaxy_or_shared_dimension_declines_snowflake`
  (`tests.rs:9150`) hand-builds a multi-root `JoinOrderEdgeEvidence` stream the binder never emits. A
  recognizer over the current edge stream (the shape iter-1 converged on, "consumes existing edge
  evidence, no second traversal") would be **unreachable dead code**.

**The cut therefore is the substrate, not a recognizer over it.** Capture the role-neutral graph at
the bind site; derive the existing star/snowflake roles from it (byte-identically); and add a
**sibling** galaxy observation that reads the role-neutral endpoints to see the conformed dimension a
directed trace cannot.

## Decision

Augment join-order edge capture with side-neutral endpoints, derive the existing tree roles from the
richer substrate, and add an observe-only galaxy evidence row. No actuation, no fragmenter, no
runtime verdict.

**D1 — Role-neutral edge capture.** Extend `JoinOrderEdgeEvidence` with side-neutral endpoints
recorded at `binder.rs:1910`: left/right **key-source labels** (the relation label that owns the
resolved join key on each input side), left/right key signatures (the resolved join-key column
identities per side), and a small per-endpoint facts summary (rows/key NDV) copied from
`join_order_sources` by those labels. Do not use `left_plan.facts` / `right_plan.facts` as endpoint
facts: on composite inputs those describe the current plan side, not the relation that owns the join
key. The directed `probe` / `dimension` fields become **derived views** (`probe == left key-source
label`, `dimension == right key-source label`), not the only durable shape. All new fields are
populated from values already in scope at the capture site — no second binder traversal, no
execution-plan inspection.

**D2 — Tree roles derive from the substrate, byte-identically.** `analyze` and `analyze_snowflake`
keep producing the exact same flat-star and snowflake evidence and receipts, now sourced from the
derived `probe`/`dimension` views. Existing star/snowflake/mixed actuation, ordering, soft-margin,
replay, and decision accrual are **unchanged** — pinned by the full 177–193 suite running green and
byte-identical. The substrate is additive.

**D3 — `analyze_galaxy` over role-neutral endpoints (separate type, observe-only).** Add
`GalaxyOrderEvidence` as a sibling of `SnowflakeOrderEvidence` (not an extension — impossible states
unrepresentable). `analyze_galaxy(&[JoinOrderEdgeEvidence], &[DecisionReceipt]) ->
Option<GalaxyOrderEvidence>` builds an undirected relation graph keyed by the role-neutral endpoints:
a relation that is an endpoint of ≥2 edges on the **same key signature** is a **conformed dimension
candidate**; the other endpoint relations are its incident relation candidates. It recognizes the
reachable conformed multi-fact shape that the directed trace records as a snowflake; it declines
(`None`) on non-inner/non-equi edges, missing labels/estimates, decision/edge mismatch, a single
fact-like root (a plain star/snowflake), or any shape it cannot bound. It never calls
`bind_local_star_actuation` or any actuator.

**D4 — Sibling cockpit row, additive to the tree receipt.** Galaxy evidence is a **separate**
`render_trace.galaxy_order: Option<GalaxyOrderEvidence>` field, not a variant of the `join_order`
render state — so it coexists with `StarReceipts`/`ObservedSnowflake` and leaves their match arms and
behavior untouched. EXPLAIN emits a `join_order_galaxy` row naming the conformed dimension(s), the
incident relation candidates, the component count, and the role-neutral edge list (endpoints, key
signatures, per-edge strategy/placement/estimate). It renders no global preferred order, no
`WOULD_REORDER`, no verdict — a gauge, not a steer.

**D5 — No runtime, no strategy, no cost change.** No `join_order_galaxy_runtime` or `_verdict`
(deferred). Row-only ranking, strategy selection, soft margins, advisory margins, and replay are
untouched. The row records existing per-edge strategies/estimates so a later rung can see where local
decisions fight.

**D6 — Reconcile the synthetic guard into a truthful one (Phase 0).** Rewrite
`galaxy_or_shared_dimension_declines_snowflake`: it (a) keeps asserting `analyze_snowflake` declines
the multi-root edge set, and (b) adds the proof that the **binder cannot emit** that synthetic old
edge stream from real SQL today (cross join / comma join / trivial-ON all rejected at parse/bind;
conformed two-fact binds to a single-root directed trace), and (c) asserts the **new role-neutral
substrate + `analyze_galaxy`** recognize the *reachable* conformed two-fact query. The guard stops
masquerading as a live gap and becomes a true statement about the substrate.

**D7 — Boundaries.** Toron binder/explain only. No SQL grammar, Rune verb, executor, distributed
fragmenter, PBC, storage, Wendelon, catalog, or wire change. Pure evidence + the substrate it needs.

## Honesty gate

`conformed_two_fact_query_emits_galaxy_evidence`: a reachable SQL query with two fact tables sharing a
conformed dimension (`sales ⋈ product` and `returns ⋈ product`, both on `product.id`) binds normally,
its physical plan and results stay **byte-identical** to today, AND EXPLAIN now emits a
`join_order_galaxy` row naming `product` as the conformed dimension shared by `{sales, returns}`. It
must **fail on `main`** (today the query records only a single-root snowflake; the role-neutral
endpoints needed to see `product` as shared are not captured). Paired regression:
`snowflake_and_star_evidence_byte_identical` — the derived tree roles reproduce the exact pre-194
receipts on every existing star/snowflake fixture.

## Forward arc

`193 mixed root actuation → 194 role-neutral join-graph substrate (this: the conformed graph becomes
observable) → galaxy runtime evidence → galaxy measured verdicts → bridge evidence → bounded graph
topology actuation → memo only when receipts show local decisions fighting`. 194 deliberately stops
at observation: it pays for the substrate once so every later graph rung reads a role-neutral graph
instead of a left-deep trace.

## Consensus (transparent)

3-of-3 converged on the *shape* (observe-only, separate `GalaxyOrderEvidence`, retarget the decline
guard, defer runtime — Claude/Codex/Gemini iter-1/2). The final-iteration reachability probe falsified
the *substrate assumption* shared by all three drafts ("recognize a galaxy over the existing edge
stream"). The operator supplied the load-bearing correction: the blocker is the left-deep trace role
in `JoinOrderEdgeEvidence`, not SQL expressibility — so the cut is the role-neutral substrate, with
the negative result folded into Phase 0 and the galaxy recognizer reading the new endpoints. This ADR
promotes that resolution. Gemini's sibling-row (D4) and Codex's separate-type/no-second-traversal (D3)
are carried; the "thin recognizer over the existing stream" framing is the rejected alternative,
rejected by the probe.

## Deferred (named)

Galaxy runtime rows; galaxy measured verdicts; galaxy actuation / global graph ordering; bridge /
many-to-many evidence; catalog-owned fact/dimension classification (the substrate records trace-level
role-neutral endpoints, not warehouse fact semantics); heterogeneous-stage fragmenter; branch-local
mixed; strategy-aware cost model; outer joins; memo/Cascades; durable/persisted optimizer evidence.
