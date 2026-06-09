# Impl Plan — ADR 200: Graph Decision & Verdict Accrual

Pairs with `plans/adr/200_graph_decision_and_verdict_accrual.md`. Side-effect-only: execution plans +
EXPLAIN receipts stay byte-identical to `main`. All anchors verified against the tree.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| The two duplicate actuators | `try_bind_galaxy_actuation` (join_order.rs:2062), `try_bind_bridge_actuation` (~:2150) |
| Shared result / reroot / recommendation | `GalaxyActuationResult` (:2000), `reroot_galaxy_join_tree` (:579), `GraphOrderRecommendation` |
| Actuator wiring | `binder.rs` Join arm (~:1106–1135, the two `if let …Actuated` blocks) |
| Decision-accrual mirror | `accrue_star_order_receipt` (:3076) / `accrue_star_order_decline` (:3087) → `decision_recorder.record` |
| Decision taxonomy | `OptimizerDecisionKind` + `optimizer_decision_kind_label` (decision_log.rs:8/181) |
| Sampled verdict seam | `measured_verdicts_from_summaries` (explain.rs ~:512), `MeasuredLevelVerdict` (:461), `runtime_verdict_class_from_star` (:694) |
| Render-time verdict to extract from | `graph_order_verdict_row` (explain.rs) — inline Vindicated/MissedWin/Indeterminate logic |
| Graph observations already collected | `star_order_runtime_replay_ids` (binder.rs:301) unions galaxy + bridge targets |

## Phase 1 — D1: unify the actuators (behavior-neutral refactor first)

`join_order.rs`:
- `enum GraphShape { Galaxy, Bridge }`. Replace the current two-way galaxy result with a graph-specific
  `GraphActuationResult` that can represent `Actuated`, `KeptAlreadyOptimal`, `KeptThinMargin`, and
  `Declined(reason)` so accrual can observe the committed outcome rather than infer it from
  `NotCandidate`.
- `async fn try_bind_graph_actuation(&self, plan, trace, render_trace, shape: GraphShape)`: lift the
  galaxy body; replace the 3 seam points by `match shape`:
  - recognize: `Galaxy => analyze_galaxy(...).and_then(galaxy_order_recommendation)`,
    `Bridge => analyze_bridge(...).and_then(bridge_order_recommendation)` → `Option<GraphOrderRecommendation>`.
  - applied-root setter: `set_applied_galaxy_root` / `set_applied_bridge_root`.
  - (accrual kind: `GalaxyRoot` / `BridgeRoot` — Phase 2.)
  Delete `try_bind_galaxy_actuation` and `try_bind_bridge_actuation`.

`binder.rs` (Join arm): replace the two `if let GalaxyActuationResult::Actuated …` blocks with a loop
over `[GraphShape::Galaxy, GraphShape::Bridge]` returning on the first `Actuated` (galaxy XOR bridge — the
recognizers are mutually exclusive; keep galaxy-first to match `main`).

**Checkpoint:** run the 194–199 lib + architecture suites and confirm byte-identical *before* Phase 2.
This isolates the behavior-neutral unify from the new accrual behavior.

## Phase 2 — D2: graph decision accrual

`decision_log.rs`: add `OptimizerDecisionKind::GalaxyRoot` / `BridgeRoot`; extend
`optimizer_decision_kind_label` (`"galaxy_root"` / `"bridge_root"`).

`join_order.rs` (in `try_bind_graph_actuation`): add `accrue_graph_order_receipt` /
`accrue_graph_order_decline` (mirror the star pair at :3076/:3087 — `self.decision_recorder.record(kind,
outcome)`), keyed by the shape's kind:
- Reordered (actuation fires) → `record(kind, Reordered)`.
- The already-optimal path → `record(kind, KeptOptimal)`.
- The thin-margin keep path → `record(kind, KeptThinMargin)`.
- A declined path (missing estimate / preflight cliff) → `record(kind, Declined(reason))`.
Recognition stays on `without_decision_recording()`, so scratch never accrues.

## Phase 3 — D3: graph verdict accrual (sampled path)

`explain.rs`:
- Extract `measured_graph_order_verdict(rec: &GraphOrderRecommendation, measured: &[(String, Option<u128>)])
  -> StarOrderVerdict` from the inline verdict logic in `graph_order_verdict_row`; rewrite that row
  builder to call the helper (render + accrual share one computation).
- In `measured_verdicts_from_summaries`, after the snowflake/star arms, add:
  - `if let Some(galaxy) = &render_trace.galaxy_order { … MeasuredLevelVerdict { kind: GalaxyRoot,
    verdict: runtime_verdict_class_from_star(measured_graph_order_verdict(rec, measured)) } }`
  - same for `bridge_order` → `BridgeRoot`, reusing the bridge measured-cost assembly that
    `bridge_analyze_rows` already computes (fact-root = measured incoming `output_rows`; dim-root =
    `outgoing_edge_rows` estimate). The replay-id union (binder.rs:301) already supplies the summaries.
- The 188 `record_verdicts` loop consumes the new `MeasuredLevelVerdict`s unchanged → graph verdicts land
  in `…optimizer_runtime_verdicts`; 189/191 advisory picks up graph `MissedWin`s for free.

**Escape hatch (ADR Fork A):** if the galaxy measured-cost assembly in the sampled path needs more than
the bridge one already provides, ship D1+D2 as ADR 200 and split D3 to ADR 201 — note it in the seal.

## Phase 4 — guards (tests/architecture)

- **Retarget** `adr198_galaxy_actuation_*` + `adr199_bridge_actuation_*` from the per-shape function names
  to `try_bind_graph_actuation` (keep every invariant assertion; the `GraphShape` match keeps the
  two-shape bound).
- **Add:**
  - `adr200_unified_actuator_replaces_duplicates`: no `try_bind_galaxy_actuation` /
    `try_bind_bridge_actuation` symbols remain; `try_bind_graph_actuation` present.
  - `adr200_graph_decisions_accrue_to_shared_log`: the actuator calls `accrue_graph_order_*` /
    `decision_recorder`; `GalaxyRoot`/`BridgeRoot` in the kind label.
  - `adr200_graph_verdicts_accrue_to_sampled_runtime_log`: `measured_verdicts_from_summaries` contains the
    `galaxy_order` / `bridge_order` arms.
  - `adr200_accrual_is_side_effect_only`: no executor/fragmenter/storage/Wendelon markers (reuse the
    187/188 binder-only offender sweep).

## Tests

| test | locks | fails under |
|---|---|---|
| `graph_actuation_accrues_committed_decisions_and_verdicts` (sample_rate=1, no EXPLAIN) | D2+D3 headline | `main` (0×) |
| `kept_graph_accrues_kept_not_reordered` | accrual = committed decision, not fired | accrual-on-fire-only |
| `unified_actuator_preserves_198_199_behavior` (physical plan byte-identical) | D1 neutrality | a leaky unify |
| 194–199 EXPLAIN suites unchanged (regression) | side-effect purity | any receipt/plan drift |

## Verification gate (one filter per command)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib galaxy        # + bridge, snowflake, star (Phase 1 checkpoint)
scripts/cargo-cgroup test  -p toron --lib optimizer_decisions
scripts/cargo-cgroup test  -p toron --lib graph_actuation_accrues
scripts/cargo-cgroup test  -p toron --test architecture_test adr200
scripts/cargo-cgroup test  -p toron --test architecture_test adr198   # retarget green
scripts/cargo-cgroup test  -p toron --test architecture_test adr199   # retarget green
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check ; git diff --check
```

## Risks

- **Unify leak (D1).** Mitigated by the Phase-1 checkpoint: 194–199 byte-identical *before* any accrual.
- **Verdict assembly (D3).** The replay-id union already collects graph observations; the residual is the
  measured-cost assembly per shape. Escape hatch splits D3 if it exceeds the bridge's existing assembly.
- **Guard-retarget completeness.** Grep that no `try_bind_galaxy_actuation`/`try_bind_bridge_actuation`
  string survives in src or tests after the rename.
