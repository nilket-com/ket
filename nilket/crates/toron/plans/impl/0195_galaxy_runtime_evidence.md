# Impl Plan — ADR 195: Galaxy Runtime Evidence

Pairs with `plans/adr/0195_galaxy_runtime_evidence.md`.

Additive `EXPLAIN ANALYZE` galaxy runtime rows + per-edge accuracy gauge, reusing 194's
`GalaxyOrderEvidence` and the snowflake runtime measurement machinery. No new type, no actuation, no
fragmenter. Phase 0 (the drift fixture) first — it fixes the gate's row shape.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| 194 galaxy evidence (sibling render field) | `OptimizerExplainRenderTrace.galaxy_order: Option<GalaxyOrderEvidence>` `explain.rs:90`; `GalaxyEdgeEvidence { target, left/right_label, left/right_key_signature, estimated_rows, strategy, placement }` `join_order.rs` |
| ANALYZE row builder (early-returns per shape; append galaxy rows before each return) | `optimizer_analyze_rows` `explain.rs:547`; snowflake `return` `:572` |
| Runtime measurement (computes `ratio = actual/estimated` — the accuracy gauge) | `join_order_runtime_measurement` `explain.rs:~745`; `ratio` `:756` |
| Snowflake runtime row template (schema/columns) | `optimizer_analyze_snowflake_runtime_row` `explain.rs:767` |
| Replay-id → trace target | `join_replay_id_for_trace_target` `explain.rs:900` |
| Replay-id collection allowlist (union galaxy targets) | `star_order_runtime_replay_ids` `binder.rs:297` |
| Observations + summaries | `JoinRuntimeObservation`; `JoinRuntimeObservationSummary { build_rows, probe_rows, output_rows }` |
| 194 galaxy EXPLAIN row + labels (reuse for the ANALYZE plan-row repeat) | `optimizer_explain_galaxy_order_row` / `galaxy_order_target_label` / `galaxy_order_detail_label` `explain.rs:979` |
| Galaxy fixture (extend with a mis-estimated conformed join) | `galaxy_join_order_catalog` / `galaxy_conformed_order_sql` `tests.rs` |

## Phase 0 — Drift fixture + gate (do first)

Add a conformed two-fact fixture whose conformed-dimension join is mis-estimated: seed `product`'s key
NDV / row facts so the estimated conformed-edge cardinality is (e.g.) 100, then drive
`render_explain_analyze` with a `JoinRuntimeObservation` whose `output_rows` is materially different
(e.g. 5). On `main`, assert `EXPLAIN ANALYZE` emits **no** `join_order_galaxy_runtime` row (the failing
signal). This fixes the exact row shape the gate needs and proves the drift is observable.

## Phase 1 — Replay-id collection union (binder)

`bind_explain_analyze` currently gathers runtime observations through `star_order_runtime_replay_ids`
(`binder.rs:297`), which only knows about the tree receipt. Extend that allowlist so a recognized galaxy
also contributes its edge targets:

```rust
if let Some(galaxy) = &render_trace.galaxy_order {
    targets.extend(galaxy.edges.iter().map(|edge| edge.target));
}
```

Additive; redundant-but-harmless for the snowflake case (BTreeSet dedups). Guard that galaxy replay ids
are collected iff a galaxy is recognized, and that the existing tree path stays byte-identical.

## Phase 2 — Galaxy runtime rows (explain/analyze)

In `optimizer_analyze_rows`, before each early `return Ok(rows)` (and the final return), append galaxy
rows from `render_trace.galaxy_order` via a helper:

```text
fn galaxy_analyze_rows(galaxy, render_trace, summaries) -> Result<Vec<Vec<ScalarValue>>>:
    rows = [optimizer_explain_galaxy_order_row(galaxy)]   // the plan row repeats under ANALYZE
    let by_target = galaxy.edges keyed by target
    let conformed_edges = set of (target) whose endpoint is a ConformedDimension
    for edge in render_trace.join_order_edges:
        let Some(gedge) = by_target.get(edge.target) else continue
        // galaxy edges ARE join_order_edges by target — reuse the snowflake measurement directly
        let estimated = edge.estimated_rows expect "analyze_galaxy declines on None"
        let measurement = join_order_runtime_measurement(edge, estimated, summaries)?
        rows.push(galaxy_runtime_row(gedge, measurement, conformed))
    rows
```

`galaxy_runtime_row` mirrors `optimizer_analyze_snowflake_runtime_row`:
- kind `join_order_galaxy_runtime`;
- target `join[{replay_id}] {left}->{right}` (+ `conformed={dim}` when the edge touches a conformed
  dimension);
- detail `left->right on {left_sig}={right_sig}; estimated={}; actual={}; ratio={}; build_rows={};
  probe_rows={}; strategy={}; placement={}; conformed={bool}` — `ratio` is the per-edge accuracy gauge;
- the Int64 estimated/actual/build/probe columns in the same schema positions as the snowflake runtime
  row.

No order-verdict row is emitted.

## Phase 3 — Tests

- `conformed_galaxy_runtime_surfaces_measured_fanout_drift` — Phase 0 headline; fails on `main`;
  `join_order_galaxy_runtime` rows; `actual != estimated`, `ratio != 1` on the conformed edge.
- `galaxy_runtime_keys_by_replay_id` — each runtime row's replay id matches a galaxy edge target; one
  row per galaxy edge.
- `galaxy_runtime_has_no_order_verdict` — no `join_order_galaxy_verdict` / `WOULD_REORDER` row.
- `galaxy_runtime_is_additive_to_snowflake` — the conformed query still emits its
  `join_order_snowflake_runtime` rows alongside the galaxy runtime rows.
- `snowflake_and_star_runtime_byte_identical` + `non_galaxy_queries_emit_no_galaxy_runtime_rows` —
  single-fact ANALYZE output unchanged, no galaxy rows.
- `rune_galaxy_runtime_matches_sql` — Rune parity (primary interface).

## Phase 4 — Architecture guards

- `adr195_galaxy_runtime_reuses_galaxy_order` — no new render-trace field / `GalaxyRuntimeEvidence`
  type; runtime rows read `galaxy_order` + summaries.
- `adr195_galaxy_verdict_is_deferred` — the galaxy ANALYZE path emits no verdict/reorder row
  (`!contains("join_order_galaxy_verdict")`, no `WOULD_REORDER`).
- `adr195_galaxy_measurement_reuses_179_replay_correlation` — uses `join_order_runtime_measurement` /
  `join_replay_id_for_trace_target`, no parallel measurement impl.
- `adr195_galaxy_telemetry_unions_edge_targets` — `star_order_runtime_replay_ids` unions `galaxy_order`
  edge targets.
- `adr195_no_fragmenter_executor_or_actuation` — `distributed/`, `exec.rs` untouched; galaxy never
  reaches `bind_local_star_actuation`.
- `adr195_galaxy_runtime_is_binder_only` — no storage/Wendelon/PBC/wire/grammar surface.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib conformed_galaxy_runtime_surfaces_measured_fanout_drift
scripts/cargo-cgroup test -p toron --lib galaxy
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --test architecture_test adr195
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one test filter per command.

## Risks

- **Byte-identity of existing ANALYZE output.** Galaxy rows must append, never reorder/replace the
  snowflake/star runtime+verdict rows. Pin with `snowflake_and_star_runtime_byte_identical` and the
  existing explain_analyze suite green.
- **Order-verdict creep.** The honest galaxy signal is per-edge cardinality accuracy, not an order
  judgment — no `WOULD_REORDER`. Guard `adr195_galaxy_verdict_is_deferred`.
- **Replay-id gaps.** If a future galaxy is recognized without a tree receipt, D4's union keeps its
  observations gathered; pin that galaxy targets are collected independent of the join_order state.

## Deferred

Galaxy measured order verdict; galaxy cost model / ordering; galaxy actuation; bridge / many-to-many
evidence; per-conformed-dimension aggregate row; heterogeneous-stage fragmenter; branch-local mixed;
outer joins; memo/Cascades; durable optimizer evidence.
