# ADR 195: Galaxy Runtime Evidence

**Status:** Promoted. **Migration:** `plans/impl/0195_galaxy_runtime_evidence.md`.

ADR 194 made the conformed multi-fact graph observable at plan time: a reachable two-fact query
(`sales ⋈ product ⋈ returns`) that the directed trace records as a single-root snowflake now emits a
role-neutral `join_order_galaxy` row naming `product` as the conformed dimension shared by both facts.
But that row is **estimate-only** — nothing has measured whether the conformed-dimension join fans out
as the optimizer guessed. ADR 195 applies the proven Observe → **Measure** rung to the graph domain:
under `EXPLAIN ANALYZE`, emit measured per-edge runtime evidence plus a per-edge estimate-vs-measured
accuracy gauge for the galaxy graph. Observe-only still: **no order verdict, no actuation, no global
graph ordering.**

## Decision

**D1 — Reuse `GalaxyOrderEvidence`; no new render-trace field or measured type.** Galaxy runtime rows
are built at render time in the ANALYZE row builder from the existing `render_trace.galaxy_order`
(194's sibling field) plus the `JoinRuntimeObservation`s already passed to `render_explain_analyze` —
exactly as `join_order_snowflake_runtime` rows are built from the snowflake evidence plus observations.
A measurement is a *view over* 194's graph identity, not a second representation.

**D2 — Per-edge runtime rows, correlated by replay id, reusing the measurement machinery.**
`EXPLAIN ANALYZE` (and Rune `.analyze()`) emits one `join_order_galaxy_runtime` row per galaxy edge,
carrying the measured build / probe / output cardinalities and the estimated rows. Galaxy edges are a
subset of `join_order_edges` keyed by `JoinTraceTarget` (194), so the row reuses
`join_order_runtime_measurement` / `join_replay_id_for_trace_target` directly — the same correlation
the snowflake runtime rows use. The row labels the role-neutral endpoints (`left->right on signature`)
and flags edges whose endpoint is a `ConformedDimension`. The `join_order_galaxy` plan row repeats
under ANALYZE for visibility; no per-conformed-dimension aggregate row (deferred — the per-edge rows
already expose the fan-out).

**D3 — Per-edge estimate-vs-measured accuracy, NOT an order verdict.** The runtime measurement already
computes `ratio = actual / estimated` (`explain.rs:756`) — that *is* the accuracy gauge, and it
requires no galaxy ordering. The galaxy runtime row carries estimated, actual, and ratio per edge, so
conformed-dimension cardinality drift becomes visible. It emits **no** `WOULD_REORDER` /
`vindicated` / `over_eager` order verdict: a galaxy has no chosen order to judge until a galaxy cost
model / ordering lands (deferred). 195 is strictly Observe→Measure, never Judge-the-order.

**D4 — Replay-id collection unions galaxy edge targets (robustness).** `star_order_runtime_replay_ids`
(`binder.rs:297`) already collects every edge target for an `ObservedSnowflake`. 194's reachable
galaxy still rides that snowflake-shaped tree at plan time, but 195 must not rely on that coincidence:
it additionally unions `galaxy_order` edge targets unconditionally, so galaxy measurement is
explicitly gathered from the galaxy substrate itself rather than by inheriting the tree allowlist.
Additive; BTreeSet-deduped for the snowflake case.

**D5 — Additive and bounded.** Galaxy runtime rows ride alongside the snowflake/star runtime + verdict
rows the query already produces; existing star/snowflake ANALYZE output stays byte-identical. The
ANALYZE row builder (`optimizer_analyze_rows`, `explain.rs:547`) returns early per shape today — 195
appends the galaxy rows before those returns. The executor's generic ADR-179 observations are
sufficient; no fragmenter, executor, actuation, storage, Wendelon, PBC, wire, or grammar change.
Binder/explain/analyze only.

## Honesty gate

`conformed_galaxy_runtime_surfaces_measured_fanout_drift`: a reachable conformed two-fact query whose
conformed-dimension join is **mis-estimated** (the fixture seeds an estimate the seeded runtime
observation contradicts) emits, under `EXPLAIN ANALYZE`, `join_order_galaxy_runtime` rows whose
`actual` differs from `estimated` with a `ratio` ≠ 1 flagging the drift, while results and the physical
plan stay byte-identical. It must **fail on `main`** (194 emits no galaxy runtime row — the ANALYZE
builder has no galaxy arm). The gate requires the estimate-vs-measurement comparison, not merely that a
row exists — drift visibility is the point. Paired regressions:
`snowflake_and_star_runtime_byte_identical` and `non_galaxy_queries_emit_no_galaxy_runtime_rows`.

## Forward arc

`194 role-neutral galaxy substrate → 195 galaxy runtime evidence (this: measured fan-out + accuracy, no
order verdict) → bridge / many-to-many evidence → galaxy cost model + ordering → galaxy measured order
verdict → bounded graph topology actuation → memo only when receipts show local decisions fighting`.
195 measures the shape 194 made visible; bridge widens the perimeter next; the order verdict waits for
a galaxy cost model, because there is no order to judge until one exists.

## Consensus (transparent)

3-of-3. Claude and Codex led with runtime-evidence depth; Gemini initially argued bridge/many-to-many
breadth-first, then conceded explicitly to the depth-per-shape cadence — the 177–193 arc pulled each
tree shape through the full Observe→Measure→Judge→Actuate ladder before widening, so the galaxy is
measured before the topology map widens. All three converged on: reuse `GalaxyOrderEvidence` (no second
representation), per-edge replay-id-correlated runtime rows, an accuracy gauge (`actual/estimated`)
rather than an order verdict, the galaxy-target replay-id union, and binder/explain/analyze-only
boundaries. Bridge is sequenced as the next rung, not rejected.

## Deferred (named)

Galaxy measured *order* verdict; galaxy cost model / global graph ordering; galaxy actuation; bridge /
many-to-many evidence; per-conformed-dimension aggregate runtime row; catalog fact/dimension
classification; heterogeneous-stage fragmenter; branch-local mixed; outer joins; memo/Cascades;
durable optimizer evidence.
