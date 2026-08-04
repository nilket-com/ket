# Impl Plan — ADR 203: Multi-Hop Bridge Chain Ordering Evidence

Pairs with `plans/adr/0203_bridge_chain_ordering_evidence.md`. **Observe-only**: execution plans + EXPLAIN
tree receipts stay byte-identical to `main`; the cut adds a `join_order_bridge_chain` cockpit row + a
measured chain verdict. All anchors verified against the tree.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Single-bridge recognizer (compose its links) | `analyze_bridge` (join_order.rs) → `Vec<BridgeLink>`; a chain may yield ≥2 links, but the vector is relation-set ordered and must be stitched |
| `BridgeLink` fields (stitch + per-hop cost) | `fact` / `bridge` / `dim`, `incoming_edge_rows` / `outgoing_edge_rows`, `incoming_target` / `outgoing_target` |
| Chain-stitch rule (verified) | `link_{i+1}.fact == link_i.bridge` (`link2.fact = B1 = link1.bridge`) |
| Recognition hook (during BINDING) | `binder.rs:257` — `analyze_bridge(...)` → `render_trace.set_observed_bridge(...)`; add the chain beside it |
| Render-trace evidence field | `OptimizerExplainRenderTrace.bridge_order` (explain.rs) + `set_observed_bridge` → add `bridge_chain_order` + setter |
| Cockpit row builder to mirror | `optimizer_explain_bridge_order_row` / `bridge_analyze_rows` (explain.rs) |
| Verdict classifier + sampled seam | `measured_graph_order_verdict` + `measured_verdicts_from_summaries` (explain.rs) — add a chain arm |
| Telemetry replay-id union | `star_order_runtime_replay_ids` (binder.rs:301) — union the chain edge targets |

## Phase 1 — D1: recognize the 2-hop chain (dark, during binding)

`join_order.rs`:
- `pub(super) struct BridgeChainEvidence { hops: Vec<BridgeLink> }` (sibling of `BridgeOrderEvidence`;
  the ordered path `F→B1→B2→D` as its constituent links).
- `pub(super) fn analyze_bridge_chain(edges, decisions) -> Option<BridgeChainEvidence>`: run
  `analyze_bridge`, then stitch its relation-set-ordered links into one ordered path on
  `link_{i+1}.fact == link_i.bridge`. Accept **exactly one 2-hop chain** (two links stitching end-to-
  end, all relations distinct); return `None` for a single junction, branching (a relation that is
  `bridge` in two links), a cycle, or > 2 hops. A separate fn — `analyze_bridge` stays byte-identical.

`explain.rs`: add `bridge_chain_order: Option<BridgeChainEvidence>` to `OptimizerExplainRenderTrace` +
`set_observed_bridge_chain`.

`binder.rs` (~:257, beside the single-bridge hook): `if let Some(chain) = analyze_bridge_chain(...) {
render_trace.set_observed_bridge_chain(chain); }`. The single-bridge `set_observed_bridge` still runs
(additive). Normal binding byte-identical.

## Phase 2 — D2: path ordering recommendation (observe-only)

`join_order.rs`:
- `pub(super) fn bridge_chain_order_recommendation(chain) -> Option<GraphOrderRecommendation>`: cost the
  path driven from each end; the driver-end whose running fan-out is smaller wins. First-cut cost (a
  defensible heuristic, tested by D3): `F`-end entry hop `hops[0].incoming_edge_rows` compounded forward;
  `D`-end entry hop `hops.last().outgoing_edge_rows` compounded backward. `declared_root` = the AST `F`
  end; `preferred_root` = the cheaper end; `action = Reordered` if they differ; **`None` on any missing
  edge estimate**. Row-only (no JoinStrategy/byte/placement).

`explain.rs`: `optimizer_explain_bridge_chain_row` (sibling of `optimizer_explain_bridge_order_row`) —
`target = F->B1->B2->D`, detail = per-hop signatures + costs + `declared`/`preferred`/`action`. Push it in
the explain row builder when `render_trace.bridge_chain_order` is `Some` (additive, after the bridge row).

## Phase 3 — D3 measured verdict + D4 telemetry union

`binder.rs`, `star_order_runtime_replay_ids`: when `render_trace.bridge_chain_order` is `Some`, union each
hop's `incoming_target` + `outgoing_target` (the 196 pattern) so the sampled path collects the chain's
per-edge observations.

`explain.rs`, `measured_verdicts_from_summaries` (after the 201 galaxy/bridge arms): if `bridge_chain_order`
+ `bridge_chain_order_recommendation` are `Some`, assemble the **compounded** measured per-hop costs via
`bridge_chain_measured_costs` and push `MeasuredLevelVerdict { kind, verdict: runtime_verdict_class_from_star(
measured_graph_order_verdict(&rec, &measured)) }`. Add the EXPLAIN ANALYZE chain row (the analyze sibling
of the explain row) so the cockpit shows measured per-hop fan-out + the verdict.

**The recommendation and the verdict use deliberately different cost models — and that asymmetry is the
point.** The recommendation (D2) is a cheap *entry-hop* direction heuristic; the middle hop `B1⋈B2` is a
common term that does not discriminate direction, so folding it in would only wash out the entry signal
(and the recommendation would never reorder when the middle dominates). The verdict (D3) is the
*falsifier* of that heuristic, so it MUST fold the middle in — the middle is exactly the heuristic's blind
spot, and a chain explodes there. `bridge_chain_measured_costs` therefore computes a **compounded** cost:
the declared end RAN (the AST left-deep order drives from `F`), so its cost is the PEAK over the *measured*
entry `F⋈B1` (`head.incoming_target`) and *measured* middle `F⋈B1⋈B2` (`head.outgoing_target`); the `D`
end did NOT run (`D` joins last — no standalone `B2⋈D`), so its cost is the *estimated* peak of the `B2⋈D`
entry (`tail.outgoing_edge_rows`) and `B1⋈B2` middle (`head.outgoing_edge_rows`) — the measured-ran-vs-
estimated-counterfactual shape of `bridge_measured_costs`. A middle that explodes past its estimate lifts
the declared peak above the counterfactual → `MissedWin`, which an endcap-only cost would miss.

**Fork — verdict/decision kind.** Lean: a new `OptimizerDecisionKind::BridgeChainRoot` (provenance — the
operator reads `information_schema` to see a *chain* was judged, mirroring 200's GalaxyRoot/BridgeRoot).
Alternative: reuse `BridgeRoot`. Observe-only ⇒ no actuation commit or new decision kind; the measured
chain verdict still rides the existing sampled-verdict path. Decide at implementation.

## Phase 4 — guards + tests

Guards (`tests/architecture/join_order.rs` / `optimizer_accrual.rs`):
- `adr203_bridge_chain_is_observe_only`: `analyze_bridge_chain` / `bridge_chain_order_recommendation`
  contain no `reroot_galaxy_join_tree` / `bind_local_star_actuation`; no actuation / plan change.
- `adr203_bridge_chain_recognizer_is_bounded_two_hop`: stitches on `link_{i+1}.fact == link_i.bridge`,
  rejects > 2 hops / branching / cycles → `None`; no `Cascades` / search.
- `adr203_chain_recognition_runs_at_bind_time`: `binder.rs` sets `bridge_chain_order` beside
  `set_observed_bridge`.
- `adr203_bridge_chain_verdict_reuses_graph_machinery`: the chain arm calls `measured_graph_order_verdict`;
  `star_order_runtime_replay_ids` unions the chain targets.

Tests (`binder/tests.rs`):

| test | locks | fails under |
|---|---|---|
| `two_hop_bridge_chain_recommends_cheaper_driver_end` (EXPLAIN) | chain recognized + path order | `main` (no chain row) |
| `bridge_chain_recognizer_distinguishes_chain_from_single_junction` | the path-stitch (not "2 links") | a links-count recognizer |
| `balanced_chain_kept` | cost decides, not a constant | a constant recommendation |
| `chain_verdict_vindicates_or_faults_recommended_order` (ANALYZE/sampled) | D3 measured verdict | observe-without-judge |
| `chain_verdict_folds_middle_hop_explosion` (ANALYZE/sampled) | the middle hop is folded into the compounded verdict | the endcap-only cost (would mislabel an exploding middle `Vindicated`) |
| 194–202 EXPLAIN + execution suites byte-identical | observational purity (D5) | any plan/receipt drift |

Reachable 2-hop chain fixture: `orders ⋈ order_lines ⋈ line_promotions ⋈ promotions`, seeded so each
junction passes the inverse-F1 gate (`bridge_rows >= fact_rows` at each hop) and one end is the cheaper
driver.

## Verification gate (one filter per command)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib bridge_chain
scripts/cargo-cgroup test  -p toron --lib bridge        # single-bridge byte-identical
scripts/cargo-cgroup test  -p toron --lib galaxy        # + snowflake, star (no drift)
scripts/cargo-cgroup test  -p toron --test architecture_test adr203
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check ; git diff --check
```

## Risks

- **Path cost model (D2) is a heuristic** — by design (observe-only); the D3 measured verdict is the
  reality check. The receipt says `would_reorder_path`; the verdict says whether measurement agreed. The
  first cut must not over-claim optimality.
- **Recognizer over-claim** — a single bridge or branching shape must not be mis-read as a chain; pinned
  by the distinguishes-from-single-junction discriminator and the bounded-two-hop guard.
- **Sampled-path evidence** — the chain evidence must be set during *binding* (not EXPLAIN) or D3 can't
  judge under normal execution; the hook sits at `binder.rs:257`.
