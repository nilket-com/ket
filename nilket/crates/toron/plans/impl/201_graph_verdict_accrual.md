# Impl Plan — ADR 201: Graph Verdict Accrual

Pairs with `plans/adr/201_graph_verdict_accrual.md`. Side-effect-only: execution plans + EXPLAIN
receipts stay byte-identical to `main`.

## Anchors

The measured-cost rows are reconciled to the **as-built** tree: D1/D2 factored the per-shape assemblies
into the shared helpers below (the pre-201 galaxy `galaxy_measured_fact_cost` and the inline
`bridge_analyze_rows` vec were the *inputs* to that extraction).

| Concern | Anchor (as built) |
|---|---|
| Sampled verdict producer (arms added here) | `measured_verdicts_from_summaries` (explain.rs:517) — `ObservedSnowflake` arm + `StarReceipts` loop |
| Verdict carrier / class map | `MeasuredLevelVerdict` (:461), `runtime_verdict_class_from_star` (:694) |
| Shared verdict classifier (extracted) | `measured_graph_order_verdict` (explain.rs) — called by render `graph_order_verdict_row` + the accrual arms |
| Galaxy measured-cost assembly | `galaxy_measured_costs` (explain.rs:949) wrapping `galaxy_measured_fact_cost` |
| Bridge measured-cost assembly | `bridge_measured_costs` (explain.rs:968) — fact = measured incoming `output_rows`; dim = `outgoing_edge_rows` |
| Actuation marker (NOT on evidence) | `render_trace.applied_galaxy_root` / `applied_bridge_root` (explain.rs:84/87) — render-trace fields |
| Observations already collected | `star_order_runtime_replay_ids` (binder.rs:301) unions galaxy + bridge targets |
| Sampled → verdict log (no change needed) | `coordinator.rs:1335` / `:2447` loop `measured_explain_analyze_verdicts` → `record_verdict(kind, verdict)` |

## Phase 1 — D1: extract the shared verdict classifier

`explain.rs`:
- Add `fn measured_graph_order_verdict(rec: &GraphOrderRecommendation, measured: &[(String, Option<u128>)])
  -> StarOrderVerdict`, lifting the inline body of `graph_order_verdict_row` (the
  `Indeterminate`-if-any-missing, else `Vindicated` iff `rec.declared_root == measured_optimal` else
  `MissedWin` logic).
- Rewrite `graph_order_verdict_row` to call it. **Checkpoint:** the 197/200 EXPLAIN-ANALYZE verdict rows
  stay byte-identical (render uses the same computation).

## Phase 2 — D2 + D3: graph arms + the asymmetry gate

`explain.rs`, in `measured_verdicts_from_summaries` — restructure from "early-return snowflake" to
"accumulate, then append graph verdicts":

```rust
let mut verdicts = Vec::new();
let graph_actuated = render_trace.applied_galaxy_root.is_some()
    || render_trace.applied_bridge_root.is_some();

// D3: tree verdicts are suppressed when a graph actuated — its re-rooted tree decisions were never
// accrued (200's no-op re-bind), so a tree verdict would be an orphan. Galaxy and bridge remain XOR
// shapes in the current binder, so this single gate is the right asymmetry discriminator.
if !graph_actuated {
    // ... the existing ObservedSnowflake arm + StarReceipts loop, pushing into `verdicts` ...
}

// D2: graph verdict — emitted whenever a graph is recognized (kept or actuated).
if let Some(galaxy) = &render_trace.galaxy_order {
    if let Some(rec) = galaxy_order_recommendation(galaxy) {
        let measured = galaxy_measured_costs(galaxy, &rec, summaries);
        verdicts.push(MeasuredLevelVerdict {
            kind: OptimizerDecisionKind::GalaxyRoot,
            verdict: runtime_verdict_class_from_star(measured_graph_order_verdict(&rec, &measured)),
        });
    }
}
if let Some(bridge) = &render_trace.bridge_order {
    if let Some(rec) = bridge_order_recommendation(bridge) {
        if let Some(measured) = bridge_measured_costs(bridge, summaries) {
            verdicts.push(MeasuredLevelVerdict {
                kind: OptimizerDecisionKind::BridgeRoot,
                verdict: runtime_verdict_class_from_star(measured_graph_order_verdict(&rec, &measured)),
            });
        }
    }
}
Ok(verdicts)
```

Convert the `ObservedSnowflake` early `return` into a push into `verdicts` (inside the `!graph_actuated`
block). The per-shape measured assemblies are extracted into `galaxy_measured_costs` / `bridge_measured_costs`
(used by both render `*_analyze_rows` and the accrual arms) so render and accrual cannot drift (mirror D1).

## Phase 3 — guards + tests

Guards (`tests/architecture/optimizer_accrual.rs` / `join_order.rs`):
- `adr201_graph_verdicts_accrue_to_sampled_runtime_log`: `measured_verdicts_from_summaries` contains the
  `galaxy_order` / `bridge_order` arms emitting `GalaxyRoot` / `BridgeRoot`.
- `adr201_verdict_computation_is_shared_between_render_and_accrual`: `graph_order_verdict_row` calls
  `measured_graph_order_verdict`; the accrual arms call it too.
- `adr201_actuated_graph_suppresses_orphan_tree_verdict`: the tree arms are gated on a `graph_actuated`
  check reading `applied_galaxy_root`/`applied_bridge_root`.
- `adr201_verdict_accrual_is_side_effect_only`: no executor/fragmenter/storage/Wendelon markers (reuse
  the 188 binder/coordinator-only sweep).

Tests (`binder/tests.rs`):

| test | locks | fails under |
|---|---|---|
| `graph_actuation_accrues_runtime_verdict` (sample_rate=1) | D2 + D3: graph verdict, no orphan tree verdict | `main` (no arms) |
| `kept_graph_emits_both_graph_and_tree_verdict` | the actuated-vs-kept asymmetry | a blanket suppression |
| `graph_verdict_indeterminate_on_missing_measurement` | Indeterminate path | a non-`Option` cost |
| 194–200 EXPLAIN suites unchanged (regression) | render/plan purity | any receipt drift |

For the sampled tests, drive the coordinator path (mirror the 188 `adr188_*` sampled tests:
`sample_rate = 1`, normal execution, then read `optimizer_runtime_verdict_rows`).

## Verification gate (one filter per command)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib graph_actuation_accrues_runtime_verdict
scripts/cargo-cgroup test  -p toron --lib galaxy        # + bridge, snowflake, star (no drift)
scripts/cargo-cgroup test  -p toron --lib optimizer_runtime_verdict
scripts/cargo-cgroup test  -p toron --test architecture_test adr201
scripts/cargo-cgroup test  -p toron --test architecture_test adr200    # survives
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check ; git diff --check
```

## Risks

- **Orphan verdict (D3).** The whole faithfulness point — pinned by the headline (no orphan tree verdict
  for an actuated graph) and the kept-graph discriminator (both verdicts coexist).
- **Render drift (D1).** Mitigated by the Phase-1 checkpoint: 197/200 ANALYZE verdict rows byte-identical
  after the extraction.
- **Bridge measured assembly drift.** Extracted into `bridge_measured_costs` (and `galaxy_measured_costs`),
  the single helper used by both render `*_analyze_rows` and the accrual arms.
