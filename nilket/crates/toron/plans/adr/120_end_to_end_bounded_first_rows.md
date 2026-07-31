# ADR 120: End-to-End Bounded First Rows

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — Codex reframed iter-0 from
layer-led to behavior-led; iter-1 Codex grounded the worker-hop trap and Claude conceded; iter-2 adopted
Codex's bifurcated gates + worker-hop expansion; iter-3 locked the schema-stability tension. **Category:**
Toron/Wendelon cross-ring behavior-led capstone of the 115→119 substrate arc. **Predecessors:** 118
(projected-path cost bound; the whole-record follow-on lands here), 119 (live cross-stage pipelining; the
coordinator-cursor follow-on lands here). **Migration:**
`plans/impl/120_end_to_end_bounded_first_rows.md`.

## Scale boundary

ADR 120 is the **user-visible capstone** of the 115→119 substrate arc at the in-process coordinator
surface: a SQL/Rune-planned distributed read issuing `SELECT * FROM huge_table LIMIT 3` through the
collecting compatibility wrapper — or consuming three rows from an unbounded `execute_query_stream`
cursor and dropping it — receives the first three rows before the query completes and cancels work all the
way down through the coordinator, the worker, ADR 119's live exchange, and ADR 118's storage scan,
**regardless of whether the projected fast path or the whole-record fallback engages**. Non-scope:
SQL/Rune planner changes; external SQL socket result streaming through `daemon.rs`; new IR variants;
Wendelon wire widening beyond 118's `TABLE_QUERY_RESP`; Toron worker wire widening beyond existing
`TaskBatch`/`TaskComplete` timing; 117/119 multi-upstream barrier; streamable `Sort`/`Aggregate`
expansion; intra-row streaming; configurable cursor depth (V1 fixed); cursor surface extended to DDL/DML
returning paths.

## Doctrine — three hops must each stream end-to-end

> The cost-bound chain is only as fast as the slowest non-streaming hop. A cursor over a task-complete
> buffer is barrier relaxation in different clothing; the [[feedback_cut_must_deliver_the_name]]
> discipline composes across hops.

Three hops carry data from storage to caller; each one must stream or the bound stalls there:

1. **Storage → worker (Wendelon scan).** ADR 118's projected path streams per-location; ADR 118's
   whole-record path accumulates `rebatch_groups` post-scan and stalls the bound when `SELECT *` falls
   onto it. **ADR 120 refactors the whole-record path to per-location emit.**
2. **Worker → coordinator (Toron `TaskBatch` frames).** `drive_streamable_plan` is internally streaming
   (`server/mod.rs:402/:440`) but the `collect` callback pushes into `results: Vec` (`:437`); frames
   leave the worker only after `TaskExecutionReport` is built (`:222`/`:630`). **ADR 120 sinks
   `TaskBatch` frames directly for streamable FinalResult plans, before `TaskComplete`.**
3. **Coordinator → cursor → caller.** `SchedulerRun { final_results: Vec<RecordBatch> }`
   (`scheduler.rs:81`) gates the caller on full query completion. **ADR 120 adds a
   `Stream<Item = Result<RecordBatch>>` cursor; the existing collecting `execute_query` becomes a
   compatibility wrapper.**

The streaming headline is pinned by hop-specific behavior gates and architecture guards: if any hop drifts
back to its pre-120 buffer shape, either the streaming headline or the corresponding hop guard fails.

## Decisions (promotion-locked)

**D1 — Bifurcated headline gates.** Two load-bearing gates:
- **`collect_select_star_limit_3_stops_whole_record_scan_before_full_scan`** — authored SQL `LIMIT 3`
  through the existing collecting `execute_query`. Pins ADR 116 `Take` + Wendelon whole-record refactor
  (D2) + the compatibility wrapper.
- **`stream_select_star_consume_three_then_drop_stops_whole_record_scan`** — caller drops an unbounded
  `execute_query_stream` after consuming three rows. **No authored `LIMIT`.** Pins D2 + D3 + D4 + D5.
  Hop-specific gates and guards pin each layer of the [[feedback_cut_must_deliver_the_name]]
  across-hops composition.

**D2 — Wendelon whole-record per-location incremental emit.** Refactor `data_server.rs:14020-14092`
(`for group in rebatch_groups { ... }`) into per-location emit inside the `'ranges` scan loop, mirroring
the projected path at `:13922`. Three preserved details:
(a) row-identity uses `survival_mask(program, &batch)` *before* identity append (matches existing
post-concat order);
(b) `max_hlc`/`input_rows`/`row_limit_satisfied` move to per-location updates;
(c) descriptor-backed typed-table queries assert the emitted whole-record batch schema matches the table
descriptor. If a descriptorless or mixed-schema whole-record path still relies on `rebatch_groups` for
correctness, it keeps a compatibility path or earns a separate behavior proof. Bounded-buffer accumulation
(D10/V2) recovers frame efficiency later without reintroducing the post-scan barrier.

**D3 — Worker `drive_streamable_plan` sink for FinalResult sends `TaskBatch` frames directly.** For
`spec.output == TaskOutputBinding::FinalResult` AND a streamable terminal plan (the 119 streamable set:
`Scan`/`Filter`/`Project`/`Take`, including the 119 live-shuffle consumer suffix), an async streamable
batch sink encodes and sends `MessageKind::TaskBatch` frames on the worker→coordinator connection per
batch — *before* `TaskComplete`. `TaskExecutionReport.results` is **not populated** for these tasks.
Existing wire frames unchanged; only the timing of `TaskBatch` emission moves. Non-streamable FinalResult plans
(Sort/Aggregate/Join terminals) remain completion-buffered — declared V1 boundary (G10).

**D4 — `NetworkTaskDispatcher` forwards FinalResult `TaskBatch` frames immediately.** At
`server/mod.rs:3033-3037`, when a cursor sink is installed for a streamable-FinalResult task, decoded
`TaskBatchPayload`s route to the cursor channel as they arrive. No local `Vec` collection for those tasks.
`TaskComplete` still drives scheduler state, retries, telemetry, error handling — but does not gate
cursor visibility.

**D5 — Coordinator `execute_query_stream` returns a cursor before `dispatch_loop` completes via background
driver.** `pub async fn execute_query_stream(plan, ...) -> Result<DistributedResultStream, QueryError>`
does not `.await` `dispatch_loop`. It (a) performs admission + active-query registration (moving
lifetimes onto the driver/cursor); (b) constructs the cursor channel (`mpsc::channel(16)`, D8); (c)
spawns a `tokio::task` driver that runs the scheduler with the cursor's sender wired into the FinalResult
forwarding path; (d) returns `DistributedResultStream` immediately, wrapping the receiver + a query
cancellation handle + RAII completion of admission/active-query state on drop.

**D6 — Cursor scope is read-query only; collecting compatibility preserved.** `execute_query` keeps its
`Result<Vec<RecordBatch>, QueryError>` signature, implemented as
`execute_query_stream(...).collect().await` for distributed read queries. DDL/DML and mutation-source
returning paths keep their existing collecting implementation in V1.

**D7 — Cursor lifetime owns query lifetime; drop synchronously cancels.** Dropping
`DistributedResultStream` before EOF synchronously marks the active query cancelled via
`ActiveQueryRegistry`; the driver observes the cancellation and runs the existing remote-cancel fanout
(which 119 wired through the live exchange and 118 wired through Wendelon's storage scan). `Drop` cannot
`await`; cancellation registration is non-blocking and the upstream-task cancellation is processed
asynchronously by the driver.

**D8 — Bounded backpressure end-to-end, fixed channel depth.** Scheduler→cursor is `mpsc::channel(16)`.
Slow consumer → cursor channel full → driver-side completion blocks → worker-side `TaskBatch` send
blocks on the network → 119 live exchange backpressures upstream producers → 118 storage scan pauses. No
new Toron query wall-time budget is introduced; SQL statement timeout and Wendelon's
`QueryExecutionBudget::max_wall_time` remain in their existing layers.

**D9 — 118 scope-pin guard inverts; no new Wendelon wire frame.** `adr118_cost_bound_scope_is_projected_path`
retires; replaced by `adr120_cost_bound_is_access_path_independent_for_descriptor_backed_queries`. The
`// ADR 118 cost-bound scope:` in-source comment goes with the descriptor-backed rebatch loop.
`TABLE_QUERY_RESP` shape unchanged from 118.

**D10 — V1 strict per-location whole-record emit; bounded-buffer V2 named.** Strict per-location maximizes
the cost bound's bite. Bounded accumulator (frame-overhead reduction) is a deliberately-named V2 follow-on.

**D11 — Scope boundary.** No SQL/Rune planner change. No new `LogicalPlan`/`ExecutionPlan` variant. No
Wendelon wire widening. No Toron worker wire widening. No 117/119 multi-upstream barrier change. No
streamable-plan predicate widening.

## Behavior gates

- **D1 collecting headline** — `collect_select_star_limit_3_stops_whole_record_scan_before_full_scan`.
- **D1 streaming headline** — `stream_select_star_consume_three_then_drop_stops_whole_record_scan` —
  pinned by D2/D3/D4/D5 hop-specific gates and guards.
- `streaming_cursor_yields_first_batch_before_terminal_task_complete` (D3+D4 worker-hop honesty).
- `cursor_drop_cancels_query_and_bounds_remote_scan` (D7 chain).
- `collecting_execute_query_remains_behavior_compatible` (D6 regression).
- `whole_record_table_query_emits_per_location` (D2).
- `whole_record_cancelled_consumer_stops_before_all_row_groups_scanned` (D2 cost-bound proof — mirror of
  118 projected gate via `CancelOnFirstWriteTransport`).
- `whole_record_pbc_per_location_matches_concatenated_pbc` (D2 equivalence).
- `worker_streamable_final_result_sends_taskbatch_before_taskcomplete` (D3).
- `network_dispatcher_forwards_final_taskbatch_to_cursor_without_buffering` (D4).
- `non_streamable_final_result_remains_completion_buffered` (D3 V1 boundary).
- `coordinator_backpressure_pauses_upstream_producers` (D8).
- Regressions: 115/116/117/118/119 suites all green.

## Architecture guards

- **`adr120_coordinator_has_streaming_cursor_surface`** (G1) — `execute_query_stream` exists; returns
  a `Stream<Item = Result<RecordBatch>>` shape.
- **`adr120_collecting_execute_is_compatibility_wrapper`** (G2) — collecting `execute_query` drains the
  streaming cursor.
- **`adr120_execute_query_stream_uses_background_driver`** (G3) — `execute_query_stream` spawns a driver
  task; does not `.await` `dispatch_loop`.
- **`adr120_worker_streams_final_result_taskbatch_frames`** (G4) — async `drive_streamable_plan` sink for
  streamable FinalResult sends frames directly; `TaskExecutionReport.results` not populated for that case.
- **`adr120_network_dispatcher_forwards_final_taskbatch_immediately`** (G5) — `NetworkTaskDispatcher`'s
  `TaskBatch` handler routes to cursor sink for FinalResult.
- **`adr120_whole_record_path_emits_per_location`** (G6) — `data_server.rs` whole-record region contains
  `writer.emit_batch(&result).await?` inside the scan loop.
- **`adr120_rebatch_groups_post_scan_emit_is_gone_for_descriptor_backed_queries`** (G7) — descriptor-backed
  whole-record table queries do not use a post-scan `for group in rebatch_groups {` emit loop.
- **`adr120_cost_bound_is_access_path_independent_for_descriptor_backed_queries`** (G8, replaces 118's
  scope-pin) — descriptor-backed projected and whole-record per-location emits live inside the scan loop;
  cancel checks bite both.
- **`adr120_cursor_drop_cancels_active_query_synchronously`** (G9) — `DistributedResultStream::Drop`
  registers cancellation via `ActiveQueryRegistry`.
- **`adr120_non_streamable_final_result_uses_completion_buffer`** (G10) — Sort/Aggregate/Join terminal
  plans take the Vec-buffered path.
- **`adr120_119_multi_upstream_barrier_untouched`** (G11) — 117/119's `stage_barrier_open` unchanged.
- **`adr120_no_sql_rune_ir_change`** (G12) — SQL/Rune/IR/PBC source files contain no ADR 120 markers.
- **`adr120_no_wendelon_protocol_widening`** (G13) — `TABLE_QUERY_RESP` shape unchanged from 118.
- **`adr120_no_toron_worker_protocol_widening`** (G14) — no new `MessageKind` variant.
- **Recipe:** `cargo test -p wendelon` + `cargo test -p toron` +
  `cargo test -p toron --test architecture_test adr117 adr118 adr119 adr120` +
  `cargo check -p toron --all-targets`.

## Council history

iter-0: Claude led with client-streaming (Toron-layer); Gemini with whole-record cost-streaming
(Wendelon-layer). Codex reframed behavior-led as "End-to-End Bounded First Rows"; operator endorsed +
sharpened ("not just a projection-friendly variant"); audit confirmed `SELECT *` not guaranteed-projected
(`l2_column_fetch_enabled` gate + `read_projected_record → Ok(None)` fallbacks). iter-1: Claude drafted
Complete A; Codex grounded that the cursor over a task-complete-buffered worker is the same
syntactic-relocation trap as 119 iter-1 (`TaskExecutionReport.results: Vec<RecordBatch>` at
`server/mod.rs:67-68`; `TaskBatch` frames sent at `:630` only after report returns). Claude conceded.
iter-2 adopted Codex's bifurcated gates + D3/D4 worker-hop expansion + D5 spawn-driver lifetime
correction + D2's three preserved details. iter-3 locked the schema-stability tension (D2 detail c):
descriptor-backed typed-table queries assert a stable table schema; any descriptorless mixed-schema path
keeps a compatibility path or earns its own proof. Bounded-buffer accumulation (D10) recovers frame
efficiency later as V2. See [[feedback_behavior_led_topic_reconciles_layer_led_splits]] and
[[feedback_cut_must_deliver_the_name]].

## What to do next

Bounded-buffer whole-record emit (D10 frame-overhead reduction); configurable cursor depth (D8
follow-on); cursor surface extended to DDL/DML returning paths (D6 scope follow-on); multi-upstream live
pipelining (119's named follow-on, joins as customer); streamable-plan coverage extension
(`Sort`/`Aggregate` bounded-memory shapes); joins frontier (relation-alias prerequisites first); logical-
type lifecycle (`DROP`/`ALTER LOGICAL TYPE`); declared-additive types.

## Promotion criteria

Both headline gates green (`collect_select_star_limit_3_stops_whole_record_scan_before_full_scan` AND
`stream_select_star_consume_three_then_drop_stops_whole_record_scan`); D2/D3/D4/D5 hop-specific gates and
guards pin the across-hop streaming composition; cursor first batch arrives before terminal-task
`TaskComplete`; cursor drop propagates cancellation through coordinator → worker → 119 live exchange →
118 storage stream regardless of access path; `execute_query` collecting behavior unchanged; 117/119
multi-upstream barrier exact; no SQL/Rune/IR change; no Wendelon protocol widening; no Toron worker wire
widening; the 14 `adr120_*` guards + 118's retired guard properly replaced; all 115/116/117/118/119
regressions hold; full `cargo test -p wendelon` + `cargo test -p toron` +
`cargo test -p toron --test architecture_test adr117 adr118 adr119 adr120` +
`cargo check -p toron --all-targets` green.
Migration: `plans/impl/120_end_to_end_bounded_first_rows.md`.
