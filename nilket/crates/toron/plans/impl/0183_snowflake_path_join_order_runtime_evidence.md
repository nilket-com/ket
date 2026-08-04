# Impl Plan - ADR 183: Snowflake-Path Join Order Runtime Evidence

Pairs with `plans/adr/0183_snowflake_path_join_order_runtime_evidence.md`.

Measure what 182 made visible: under `EXPLAIN ANALYZE`, instrument a recognized snowflake's
joins and emit per-edge **measured** join outputs labeled by snowflake level alongside 182's
plan evidence, reusing 179's measurement substrate wholesale — **no reorder, no verdict, no
new capture or evidence field**. Two binder edits: extend the replay-id gate (D1) and the
analyze-row renderer (D2). No executor / `fragment.rs` / cost-model / Wendelon change.

## Anchors

| Concern | Anchor |
|---|---|
| Replay-id gate (the one required change) | `star_order_runtime_replay_ids` (`binder.rs`) — gates on `join_order` Recognized |
| Snowflake evidence (read-only here) | `SnowflakeOrderEvidence`/`ranking.dimensions`/`branches` (`binder/join_order.rs`) |
| Edges carry replay id | `JoinOrderEdgeEvidence.target.join_index` (`binder/join_order.rs`) |
| Analyze rows (182 snowflake early-return) | `optimizer_analyze_rows` (`binder/explain.rs`) |
| Star runtime row builder (mirror + share helper) | `optimizer_analyze_join_order_row` (`binder/explain.rs`) → `join_order_runtime` |
| Measurement substrate (reused) | `JoinRuntimeObservation`, `summarize_join_runtime_observations`, `JoinRuntimeObservationSummary`, `join_replay_id_for_trace_target` (`binder/explain.rs`) |
| EXPLAIN ANALYZE entry | `bind_explain_analyze` → `render_explain_analyze` → `optimizer_analyze_plan` (`binder.rs`) |
| Tests | `tests/explain_analyze_test.rs`; `binder/tests.rs`; `tests/architecture/join_order.rs` |

## Phase 1 - Instrument snowflake joins (`binder.rs`, D1)

Extend the gate in `star_order_runtime_replay_ids` (and clarify its doc-comment: serves star
**and** snowflake join-order runtime evidence — behavior-neutral):

```rust
if !matches!(render_trace.join_order, Some(StarOrderOutcome::Recognized(_)))
    && render_trace.snowflake_order.is_none()
{
    return Ok(BTreeSet::new());
}
// unchanged: map join_order_edges -> JoinReplayId via edge.target.join_index
```

`join_order_edges` are populated for a snowflake (normal `bind_join`), and 182 guarantees
every edge belongs to the tree, so the existing map yields exactly the snowflake's join
replay ids. Star path unchanged (`snowflake_order` is `None`).

## Phase 2 - Snowflake runtime rows (`binder/explain.rs`, D2)

In `optimizer_analyze_rows`, replace the 182 early-return with: plan-evidence row **plus**
per-edge runtime rows.

```rust
if let Some(evidence) = render_trace.snowflake_order.as_ref() {
    let mut rows = vec![optimizer_analyze_snowflake_order_row(evidence)];
    let dims = flatten_snowflake_dimensions(evidence);          // label -> (probe, JoinOrderDimEvidence)
    for edge in &render_trace.join_order_edges {
        let Some(dim) = edge.dimension.as_deref() else { continue };
        let Some((probe, dimension)) = dims.get(dim) else { continue };  // matches every edge (invariant)
        rows.push(optimizer_analyze_snowflake_runtime_row(probe, edge, dimension, summaries)?);
    }
    return Ok(rows);                                             // NO verdict row (D3)
}
```

- `flatten_snowflake_dimensions`: recurse the evidence tree → `BTreeMap<String, (String /*probe*/, JoinOrderDimEvidence)>`
  keyed by dimension label (unique by the recognizer's `dimension_parent` rule).
- Factor the shared math out of `optimizer_analyze_join_order_row` into
  `join_order_runtime_measurement(edge, estimated_rows, summaries) -> (actual, ratio, summary)`
  so star and snowflake rows share one computation.
- `optimizer_analyze_snowflake_runtime_row`: kind `join_order_snowflake_runtime`, target
  `join[<replay_id>] level=<probe> dimension=<dim>`, status `measured`, detail `estimated=..;
  actual=..; ratio=..; source=..; build_rows=..; probe_rows=..`, then the same 6 typed columns
  (estimated/actual/ratio/source/build/probe) as the star runtime row.
- The `actual` value is the physical output row count of the declared execution edge for that
  replay id. The level/probe label is context from 182's reconstructed tree, not a
  counterfactual isolated sub-star measurement.

## Phase 3 - Behavioral tests

`tests/explain_analyze_test.rs` (add a coordinator-level snowflake fixture mirroring the
`binder/tests.rs` snowflake catalog):

- `snowflake_runtime_evidence_reports_measured_edge_cardinalities_by_level` - hash snowflake →
  one `join_order_snowflake_runtime` row per edge with measured `actual`, across root +
  branch.
- `snowflake_runtime_evidence_covers_root_and_branch` - a root-level and a branch-level
  runtime row both present and measured.
- `snowflake_runtime_rows_correlate_by_replay_id` - the `customers→regions` branch row's
  measured rows equal that join's observed output.
- `snowflake_explain_analyze_shows_runtime_without_verdict` - runtime rows present, **no**
  `join_order_verdict` row. *(Update the 182 `snowflake_explain_analyze_shows_plan_evidence_without_runtime_or_verdict`
  to this — runtime now present.)*
- `star_runtime_evidence_unchanged` - a star `EXPLAIN ANALYZE` still emits the 179/180
  runtime+verdict rows unchanged.
- `rune_analyze_shows_snowflake_runtime_evidence` - Rune parity.

`binder/tests.rs`:

- `snowflake_runtime_replay_ids_collected_from_edges` - `star_order_runtime_replay_ids`
  returns the snowflake's replay ids (non-empty) when `snowflake_order` is set; empty for a
  non-star/non-snowflake.
- `snowflake_runtime_evidence_is_observational` - bound `ExecutionPlan` identical with/without
  ANALYZE; declared-order results unchanged (hash snowflake).

## Phase 4 - Architecture guards (`tests/architecture/join_order.rs`)

Retarget ADR 182's `adr182_snowflake_supersedes_decline_without_gating_runtime`: it should no
longer assert that snowflake analyze emits no runtime rows. The surviving invariant is that
the flat-star runtime path remains gated by `join_order`, while ADR 183 explicitly owns the
snowflake runtime gate and still emits no snowflake verdict.

- `adr183_snowflake_runtime_reuses_179_measurement` - snowflake runtime row uses
  `JoinRuntimeObservationSummary` + `join_replay_id_for_trace_target`; no new observation type
  / optimizer fact.
- `adr183_snowflake_runtime_replay_ids_from_edges` - `star_order_runtime_replay_ids` body
  references `snowflake_order` and maps `join_order_edges`; no new evidence field.
- `adr183_snowflake_runtime_emits_no_verdict` - the snowflake analyze branch contains no
  `optimizer_analyze_*_verdict_row` call.
- `adr183_snowflake_runtime_is_observational` - no reorder; runtime renderer read-only.
- `adr183_no_executor_fragment_costmodel_or_wendelon_change` - marker sweep
  (`join_order_snowflake_runtime`, snowflake runtime fns) absent from `exec.rs`, `fragment.rs`,
  `pbc.rs`, `storage.rs`, `coordinator.rs`, `sql_planner.rs`, `distributed/`, Wendelon;
  `rewrite_broadcast_star_probe` unchanged.
- `adr183_no_optimizer_shell` → `adr179_no_optimizer_shell()`.
- `adr183_cockpit_is_frontend_shared` - rendered via the shared optimizer-analyze path.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test explain_analyze_test snowflake
scripts/cargo-cgroup test -p toron --test explain_analyze_test          # 179/180 star path unchanged
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --lib star_order                     # 177-181 unchanged
scripts/cargo-cgroup test -p toron --test architecture_test adr183
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

## Risks

- **Replay-id gate is load-bearing.** Without the `snowflake_order` gate extension, no
  snowflake join is instrumented → zero measured rows. Pin with
  `snowflake_runtime_replay_ids_collected_from_edges` +
  `snowflake_runtime_evidence_reports_measured_per_level_cardinalities`.
- **Star path must stay byte-identical.** The gate is `Recognized(join_order) ||
  snowflake_order.is_some()`; a star has `snowflake_order = None`. The shared measurement
  helper must not change the star row. Pin with `star_runtime_evidence_unchanged` + green
  `star_order`/`explain_analyze`.
- **Correlation by label.** `flatten_snowflake_dimensions` relies on unique dimension labels
  (recognizer's `dimension_parent` no-shared-dimension rule); a duplicate would have declined
  recognition in 182. Decline-safety already pinned by 182's
  `galaxy_or_shared_dimension_declines_snowflake`.
- **Counterfactual confusion.** The runtime row is a declared-edge physical output count, not
  a hypothetical level-local execution of that sub-star. Keep the row wording explicit so 184
  does not accidentally treat these measurements as an already-replanned snowflake.
- **No verdict leakage.** The snowflake branch must not push a verdict row (that is 184). Pin
  with `snowflake_explain_analyze_shows_runtime_without_verdict` + the no-verdict guard.
- **Observational.** Runtime rendering read-only over the executed plan/observations; the plan
  is the declared-order snowflake. Pin with `snowflake_runtime_evidence_is_observational`.
- **Hot path.** Replay ids / runtime rows only under `EXPLAIN ANALYZE` (render trace present).

## Deferred

Snowflake measured verdict (184, 180-analog); hash-snowflake order actuation (185, recursive
build + 181 preflight); broadcast-snowflake execution cliff (`rewrite_broadcast_star_probe`);
galaxy / bridge / cyclic recognition; always-on snowflake runtime telemetry; strategy-aware
cost model.
