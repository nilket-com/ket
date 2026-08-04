# Impl Plan — ADR 196: Bridge / Many-to-Many Join Graph Evidence

Pairs with `plans/adr/0196_bridge_join_graph_evidence.md`.

An observe + measure sibling recognizer over 194's role-neutral substrate, reusing 195's runtime
machinery. The cardinality discriminator (inverse-F1) is verified; Phase 0 locks the threshold on the
bridge-vs-branch fixture. No new substrate, no actuation.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Role-neutral edge endpoints (the discriminator's data) | `JoinOrderEdgeEvidence { probe, dimension, left/right_key_signature, left/right_endpoint_rows }` `join_order.rs:241` — probed: branch intermediary `customers`=5000 < fact `orders`=100000 |
| Galaxy recognizer (sibling pattern + by_relation/signature map + F1 gate) | `analyze_galaxy` `join_order.rs`; conformed/dimension-like gate (F1) |
| Galaxy evidence types (sibling shape) | `GalaxyOrderEvidence` / `ConformedDimension` / `GalaxyEdgeEvidence` `join_order.rs` |
| Sibling render field + setter | `OptimizerExplainRenderTrace.galaxy_order` `explain.rs:90`; `set_observed_galaxy` `:144` |
| Observe-only hook (runs analyze_galaxy; add analyze_bridge) | `binder.rs:225-260` |
| EXPLAIN galaxy row (15-col template) | `optimizer_explain_galaxy_order_row` `explain.rs:979` |
| ANALYZE row builder + galaxy runtime (195) | `optimizer_analyze_rows` `explain.rs:547`; `galaxy_analyze_rows` / `optimizer_analyze_galaxy_runtime_row` |
| Runtime measurement + summary (fan-out = output/probe) | `join_order_runtime_measurement` `explain.rs:747`; `JoinRuntimeObservationSummary { build_rows, probe_rows, output_rows }` |
| Replay-id union (195 pattern) | `star_order_runtime_replay_ids` `binder.rs:297` (galaxy union already present) |
| F1 gate to invert | `galaxy_recognizer_declines_single_fact_same_key_alias_star` `tests.rs` |

## Phase 0 — Bridge vs branch fixture + discriminator lock (do first)

Two fixtures of the **same structural shape** (`fact ⋈ mid ⋈ dim`), differing only in `mid`'s
cardinality:
- **bridge fixture:** `orders`(fact, 1_000) ⋈ `order_items`(mid, 10_000 — M:N) ⋈ `products`(dim, 100);
  `order_items` has FKs `order_id`, `product_id`. `mid_rows (10_000) >= fact_rows (1_000)` → bridge.
- **branch fixture:** reuse `snowflake_join_order_catalog` `orders ⋈ customers ⋈ regions`
  (`customers 5_000 < orders 100_000` → dimension-like → not a bridge).

Lock `bridge_recognizer_distinguishes_bridge_from_snowflake_branch` first: `analyze_bridge` returns
`Some` for the bridge fixture and `None` for the branch — proving cardinality, not structure, decides.

## Phase 1 — `analyze_bridge` recognizer (binder)

In `join_order.rs`, sibling to `analyze_galaxy`:

```text
fn analyze_bridge(edges) -> Option<BridgeOrderEvidence>:
    validate: all inner_equi, labels present, decision/target match (mirror analyze_galaxy preamble)
    rows[label] from left/right_endpoint_rows (consistent per relation)
    for each relation B:
        incoming = edges where B == dimension      // built onto a fact F
        outgoing = edges where B == probe          // joins out to a dim D
        if incoming non-empty AND outgoing non-empty:
            F = incoming.probe;  D = outgoing.dimension
            b_in_sig = incoming.right_key_signature;  b_out_sig = outgoing.left_key_signature
            if b_in_sig == b_out_sig: continue                      // conformed dimension — 194 owns it
            let (Some(b_rows), Some(f_rows)) = (rows[B], rows[F]) else continue   // conservative
            if f_rows == 0 { continue }                               // no denominator, no bridge claim
            if b_rows >= f_rows:                                    // inverse-F1 → bridge
                links.push(BridgeLink { fact: F, bridge: B, dim: D, b_in_sig, b_out_sig,
                                        fan_out_estimate: b_rows as f64 / f_rows as f64, edge_targets: [incoming.target, outgoing.target] })
    if links.is_empty() { None } else { Some(BridgeOrderEvidence { links }) }
```

`BridgeOrderEvidence { links: Vec<BridgeLink> }`; `BridgeLink { fact, bridge, dim, b_in_signature,
b_out_signature, fan_out_estimate, edge_targets }`. Separate type from `GalaxyOrderEvidence`. Reject a
cycle (a node both incoming and outgoing to the *same* relation, or `F == D`). No change to
`bind_local_star_actuation`, `order_cost`, or any recognizer.

## Phase 2 — Sibling render field + hook + plan row

- `OptimizerExplainRenderTrace.bridge_order: Option<BridgeOrderEvidence>` + `set_observed_bridge`.
- In the post-bind hook (`binder.rs`), after `analyze_galaxy`, run `analyze_bridge` and populate
  `bridge_order` if `Some` (additive; coexists with galaxy + tree receipts).
- `optimizer_explain_bridge_order_row` (15-col EXPLAIN schema, mirror galaxy): kind `join_order_bridge`,
  `target = F->B->D`, `outcome = observed`, detail = per-link signatures + `fan_out_estimate`. Append in
  `optimizer_explain_rows` after the galaxy row.

## Phase 3 — Bridge runtime fan-out (explain/analyze, reuse 195)

- `bridge_analyze_rows(render_trace, summaries)` mirrors `galaxy_analyze_rows`: a 10-col
  `optimizer_analyze_bridge_order_row` + one `join_order_bridge_runtime` row per bridge-incident edge.
- The runtime detail carries the measured **fan-out ratio = `output_rows / probe_rows`** (from the
  summary, when `probe_rows > 0`), distinct from 195's `actual/estimated`. Reuse
  `join_order_runtime_measurement` for build/probe/output; compute fan-out from
  `summary.output_rows / summary.probe_rows` and conservatively skip/decline the ratio when the
  denominator is zero.
- Append `bridge_analyze_rows(...)` before each early return in `optimizer_analyze_rows` (alongside the
  195 `galaxy_analyze_rows` extend).

## Phase 4 — Replay-id union (binder)

In `star_order_runtime_replay_ids`, after the galaxy union:
`if let Some(bridge) = &render_trace.bridge_order { targets.extend(bridge.links.iter().flat_map(|l| l.edge_targets.iter().copied())); }`.

## Phase 5 — Tests

- `bridge_evidence_measures_many_to_many_fanout` — headline; bridge fixture; `join_order_bridge` plan
  row + `join_order_bridge_runtime` with fan-out ratio `> 1`; fails on `main`.
- `bridge_recognizer_distinguishes_bridge_from_snowflake_branch` — Phase 0 lock; branch emits no bridge.
- `bridge_recognizer_declines_conformed_dimension` — galaxy fixture (same signature) is not a bridge.
- `bridge_recognizer_rejects_cyclic_joins`.
- `galaxy_and_bridge_evidence_coexist` — a query carrying both shapes emits both rows.
- `non_bridge_queries_emit_no_bridge_rows` + `snowflake_galaxy_and_star_evidence_byte_identical`.
- `rune_bridge_matches_sql` — Rune parity (primary interface).

## Phase 6 — Architecture guards

- `adr196_bridge_is_separate_type_from_galaxy` — `BridgeOrderEvidence` not an alias/extension; sibling
  render field, not a `JoinOrderRenderState` variant.
- `adr196_bridge_recognizer_uses_inverse_f1_gate` — `analyze_bridge` compares intermediary
  `endpoint_rows` to its fact-side probe (`>=`); declines on equal signatures (galaxy) / missing rows.
- `adr196_bridge_is_observe_only` — never reaches `bind_local_star_actuation`; no order verdict row.
- `adr196_bridge_runtime_reuses_195_measurement` — uses `join_order_runtime_measurement` /
  `join_replay_id_for_trace_target`; fan-out from the observation summary.
- `adr196_bridge_telemetry_unions_edge_targets` — `star_order_runtime_replay_ids` unions `bridge_order`.
- `adr196_no_fragmenter_executor_or_actuation` + `adr196_bridge_is_binder_only`.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib bridge
scripts/cargo-cgroup test -p toron --lib galaxy
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --test architecture_test adr196
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one test filter per command.

## Risks

- **Cardinality threshold.** The inverse-F1 `>=` gate is the whole cut; Phase 0's bridge-vs-branch
  fixture locks it. Missing `endpoint_rows` → decline (no false bridge).
- **Byte-identity.** Bridge rows append, never reorder existing star/snowflake/galaxy output; pin with
  the regression + the existing explain_analyze suite.
- **Recognizer overlap.** A conformed dimension (equal signatures) must route to galaxy, not bridge;
  pin with `bridge_recognizer_declines_conformed_dimension`.

## Deferred

Bridge/galaxy actuation; global graph ordering / cost model; measured order verdict; catalog PK/FK
classification; multi-hop bridge chains; outer joins; memo/Cascades; durable optimizer evidence.
