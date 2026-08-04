# ADR 183: Snowflake-Path Join Order Runtime Evidence

**Status:** Promoted. **Migration:** `plans/impl/0183_snowflake_path_join_order_runtime_evidence.md`.

ADR 182 made a snowflake's join order **visible** from estimates (plan evidence, per level,
with strategy). ADR 183 adds **measured truth**: under `EXPLAIN ANALYZE`, it reports the
**actual** per-edge join output rows for a recognized snowflake, labeled by snowflake level
alongside 182's estimated plan evidence. It is the **179-analog** one topology level up —
runtime evidence, still
**observational**: nothing is reordered, the bound `ExecutionPlan` is unchanged, results are
unchanged. The snowflake **verdict** (was the declared order beaten by the preferred order,
*measured*?) is the next rung (184), exactly as 180 followed 179 for the star.

This is the grounded "measure before act" step the council chose over building the recursive
snowflake actuator on estimate-only evidence: confirm at runtime whether snowflake order
actually matters, *then* (184/185) judge and act.

## Grounding

- **The measurement substrate already exists (179).** `bind_explain_analyze` collects the
  replay ids to instrument via `star_order_runtime_replay_ids`, the coordinator executes and
  captures `JoinRuntimeObservation`s, and `optimizer_analyze_plan` →
  `summarize_join_runtime_observations` keys per-join measured rows by `JoinReplayId`. 183
  reuses all of it; it captures no new fact and adds no new observation type.
- **Replay ids are gated on `join_order`; that is the one required change.**
  `star_order_runtime_replay_ids` returns empty unless `join_order` is `Recognized`, then
  maps `join_order_edges` to replay ids via `edge.target.join_index`. For a snowflake 182
  clears `join_order` but **`join_order_edges` remain populated**, so the gate must also fire
  when `snowflake_order.is_some()`. Without this, no snowflake join is instrumented and there
  is nothing to measure.
- **`snowflake_order = Some` ⟹ every edge belongs to the tree.** 182's `analyze_snowflake`
  only recognizes when `visited_edges.len() == edges.len()` — every `join_order_edge` is part
  of the single-root snowflake. So mapping `join_order_edges` to replay ids instruments
  exactly the snowflake's joins (no orphans), and the per-edge runtime loop yields one row for
  every level and branch.
- **Measured rows are declared-execution edge outputs.** The `actual` column is the physical
  output row count for that replay id in the declared left-deep execution. The snowflake level
  label explains where the edge belongs in 182's reconstructed tree; it is not a counterfactual
  isolated sub-star cardinality. ADR 184 must interpret these as declared-edge measurements.
- **The replay-id correlation needs no new evidence field.** The star runtime renderer walks
  `render_trace.join_order_edges` (each carrying `target.join_index`) and joins them to
  summaries by replay id and to estimates by dimension label. The snowflake evidence's
  per-level dimensions carry the estimates, and the recognizer already forbids duplicate /
  shared dimensions (`dimension_parent`), so a flat `label -> (probe, dimension)` map across
  levels is unambiguous. 183 reuses the edge list for the replay-id correlation exactly as the
  star path does — `SnowflakeOrderEvidence`/`StarOrderEvidence` get **no new field**.
- **Runtime evidence is the 179 rung; verdict is 180.** The current star analyze emits
  per-edge `join_order_runtime` rows *and* a verdict row (180 added the verdict). 183 emits
  snowflake **runtime rows only**; the snowflake verdict row is 184. Same mechanism boundary.
- **Observational.** `EXPLAIN ANALYZE` already executes the declared-order plan; 183 only adds
  measured rows to the rendered system table. The bound `ExecutionPlan`, execution, and
  results are unchanged.

## Decisions

**D1 - Instrument snowflake joins (the one binder change).** Extend
`star_order_runtime_replay_ids` to also fire when `render_trace.snowflake_order.is_some()`,
collecting replay ids from `join_order_edges` (unchanged mapping). A star is unaffected
(`snowflake_order` is `None`); a non-star/non-snowflake still yields the empty set. Clarify
its doc-comment that it now serves both star and snowflake join-order runtime evidence
(behavior-neutral).

**D2 - Emit per-edge snowflake runtime rows.** In `optimizer_analyze_rows`, the snowflake
branch emits 182's plan-evidence row **plus** one `join_order_snowflake_runtime` row per
snowflake edge: the level (probe), dimension, estimated vs **measured** output rows, ratio,
estimate source, and build/probe rows — measured via the same `JoinReplayId` summary lookup
the star path uses (`join_replay_id_for_trace_target(edge.target)`). Because every edge
belongs to the tree (grounding), the loop over `join_order_edges` covers root and every
branch. The level/probe is rendered so root and branch measurements are distinguishable; the
actual row count remains the physical output of that replayed join edge.
Factor the estimated/actual/ratio computation shared with `optimizer_analyze_join_order_row`
into one helper so star and snowflake runtime rows cannot diverge.

**D3 - No verdict this cut.** The snowflake analyze branch emits **no** `join_order_verdict`
row. The measured-vs-preferred snowflake verdict (180-analog) is ADR 184.

**D4 - Observational invariant.** No reorder, no plan change. The bound `ExecutionPlan` is
identical with and without `EXPLAIN ANALYZE`; the snowflake executes in declared order;
results are unchanged. The runtime renderer is read-only over the executed plan and the
observations.

**D5 - Reuse 179 measurement; no new capture or fact.** `JoinRuntimeObservation`,
`summarize_join_runtime_observations`, and the replay-id correlation are reused as-is. No new
optimizer fact, no second observation type, no new evidence field, no executor capture change.

**D6 - Frontend-shared.** SQL `EXPLAIN ANALYZE` and Rune `analyze()` render snowflake runtime
evidence through the same path.

**D7 - No shell, no actuation, no executor/fragment/cost-model/Wendelon change.** No
`optimizer.rs`, memo, Cascades, `PlanProperties`. No snowflake reorder, no
`extract_star_join_candidate`/`fragment.rs`/`rewrite_broadcast_star_probe` change, no cost
model, no Wendelon/storage/PBC/wire change. The only binder changes are the replay-id gate
(D1) and the runtime-row rendering (D2).

## Headline Gates

- `snowflake_runtime_evidence_reports_measured_edge_cardinalities_by_level` - `EXPLAIN ANALYZE` on
  a hash snowflake (`orders → customers → regions` + a leaf dim) emits a
  `join_order_snowflake_runtime` row per edge with measured `actual` rows, across the root and
  the `customers` branch.
- `snowflake_runtime_evidence_is_observational` - the bound `ExecutionPlan` is identical with
  and without `EXPLAIN ANALYZE`; the snowflake executes in declared order; results unchanged.
- `snowflake_explain_analyze_shows_runtime_without_verdict` - the snowflake analyze output
  contains runtime rows **and no** `join_order_verdict` row (the 184 boundary). *(Evolves the
  182 `..._without_runtime_or_verdict` gate.)*

## Supporting Gates

- `snowflake_runtime_rows_correlate_by_replay_id` - each runtime row's measured rows come from
  the observation of the matching join (`edge.target.join_index`).
- `snowflake_runtime_evidence_covers_root_and_branch` - both a root-level edge and a
  branch-level edge are measured.
- `snowflake_runtime_replay_ids_collected_from_edges` - `star_order_runtime_replay_ids` yields
  the snowflake's join replay ids (non-empty) when `snowflake_order` is set; empty for a
  non-star/non-snowflake.
- `star_runtime_evidence_unchanged` - the star `EXPLAIN ANALYZE` runtime+verdict rows
  (179/180) are byte-identical.
- `rune_analyze_shows_snowflake_runtime_evidence` - Rune `analyze()` parity.

## Architecture Guards

- `adr183_snowflake_runtime_reuses_179_measurement` - the snowflake runtime row uses
  `JoinRuntimeObservationSummary` + `join_replay_id_for_trace_target`; defines no new
  observation type or optimizer fact.
- `adr183_snowflake_runtime_replay_ids_from_edges` - `star_order_runtime_replay_ids` fires for
  `snowflake_order` and maps `join_order_edges` to replay ids (no new field on the evidence).
- `adr183_snowflake_runtime_emits_no_verdict` - the snowflake analyze branch pushes no verdict
  row.
- `adr183_snowflake_runtime_is_observational` - no reorder; the runtime renderer is read-only
  over the executed plan/observations.
- `adr183_no_executor_fragment_costmodel_or_wendelon_change` - no executor capture,
  `fragment.rs`, `rewrite_broadcast_star_probe`, cost model, storage, PBC, wire, or Wendelon
  change.
- `adr183_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.
- `adr183_cockpit_is_frontend_shared` - SQL and Rune render through the same path.

## Rejected Alternatives

**Add replay ids / join_index to the snowflake evidence structs.** Unnecessary: the
replay-id correlation comes from `join_order_edges` (which carry `target.join_index`), exactly
as the star runtime path correlates. Keep the evidence structs unchanged.

**Fold in the verdict (180-analog).** A genuine mechanism boundary — the same one that
separated 179 from 180 for the star. 183 delivers runtime evidence; the verdict is 184.

**Build the snowflake actuator now (act on estimates).** Snowflake actuation is a new
recursive build; acting on estimate-only evidence is acting ahead of its justification. 183
measures first so 184 (verdict) and 185 (actuation) are earned.

## Non-Goals

- no snowflake verdict (184) or actuation/reorder (185-ish);
- no broadcast-snowflake execution relaxation (`rewrite_broadcast_star_probe` — the cliff);
- no galaxy / bridge / cyclic recognition;
- no always-on runtime telemetry (EXPLAIN ANALYZE-gated this cut);
- no new optimizer fact, observation type, second estimator, or cost model;
- no executor capture, fragment, storage, PBC, wire, catalog, or Wendelon change;
- no optimizer shell, memo, Cascades, or broad `PlanProperties` contract.

## Consensus Trail

The council chose measure-before-act for the snowflake topology: the star arc went
observe→act because star actuation was a cheap reuse, but snowflake actuation is a new
recursive build, so the grounded sequencing is observe (182) → **measure (183)** → judge (184)
→ act (185). 183 reuses 179's measurement substrate almost wholesale — one gate extension so
snowflake joins are instrumented, then per-edge runtime rows correlated by replay id — and
defers the verdict at the proven 179/180 boundary. Observational, additive, the star runtime
path byte-identical.

## Forward Arc

`182 snowflake plan evidence -> 183 snowflake runtime evidence -> 184 snowflake measured
verdict (180-analog) -> 185 hash-snowflake order actuation (binder-only recursive build, 181
preflight machinery) -> broadcast-snowflake execution cliff (rewrite_broadcast_star_probe) ->
galaxy/multi-fact -> bridge/many-to-many -> cyclic caps -> memo only when receipts show
decisions fighting`.

After 183 the snowflake's order is visible and its declared execution edges are measured,
level-labeled — the runtime truth that will let 184 judge whether reordering is worth the
recursive build.

## Deferred (named)

Snowflake measured verdict (184, 180-analog); hash-snowflake order actuation (185); the
broadcast-snowflake execution cliff (`rewrite_broadcast_star_probe`); galaxy / bridge /
cyclic recognition; always-on snowflake runtime telemetry; a strategy-aware cost model for
mixed/snowflake ordering.
