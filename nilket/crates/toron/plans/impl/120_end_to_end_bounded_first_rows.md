# Impl Plan — ADR 120: End-to-End Bounded First Rows

**Migration for** `plans/adr/120_end_to_end_bounded_first_rows.md`. **Cross-ring scope:** Wendelon
`data_server.rs` (whole-record per-location emit); Toron worker (`drive_streamable_plan` sink for
FinalResult); Toron `NetworkTaskDispatcher` (forward TaskBatch immediately for FinalResult); Toron
scheduler + coordinator (streaming cursor + spawn-driver lifetime).

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Wendelon projected per-location emit (model) | `data_server.rs:13922-13923` |
| Wendelon whole-record post-scan emit (to delete) | `data_server.rs:14020-14092` `for group in rebatch_groups { ... }` |
| Wendelon whole-record per-location body | `data_server.rs:13933-14017` |
| PBC on concatenated today | `data_server.rs:14049` `pbc::execute_program(program, &concatenated)` |
| 118 scope-pin guard (to retire) | `wendelon/tests/architecture_test.rs::adr118_cost_bound_scope_is_projected_path` |
| 118 in-source scope comment | `data_server.rs` `// ADR 118 cost-bound scope:` above rebatch loop |
| Worker `drive_streamable_plan` Vec-collecting sink | `toron/src/server/mod.rs:402/:437/:440` |
| `TaskExecutionReport.results: Vec<RecordBatch>` | `toron/src/server/mod.rs:67-68` |
| Worker TaskBatch send (post-report today) | `toron/src/server/mod.rs:630-633` |
| Dispatcher TaskBatch decode (to forward immediately) | `toron/src/server/mod.rs:3033-3037` |
| Coordinator entry (Vec) | `coordinator.rs:344` `execute_query`; `:615` `Ok(run.final_results)` |
| Scheduler final-results collection | `scheduler.rs:81/:565` |
| Active-query / admission lifetime (stack-local today) | `coordinator.rs::execute_query_with_context` |
| Cancellation chain (existing) | `ActiveQueryRegistry`; 059/060 token chain; 119 `cancel_live`; 118 storage stream cancel |

## Phase 0 — Bifurcated failing gates first

In `crates/toron/tests/wendelon_storage_execution_test.rs` (LiveWendelonFixture):

- `collect_select_star_limit_3_stops_whole_record_scan_before_full_scan` — collecting; authored `LIMIT 3`.
- `stream_select_star_consume_three_then_drop_stops_whole_record_scan` — streaming; no authored LIMIT;
  consume three rows + drop; first batch arrives before terminal `TaskComplete`; Wendelon stops mid-scan.

Both **must fail on `main`**. The streaming gate is backed by hop-specific guards for D2/D3/D4/D5.

Companion red gates:
- `streaming_cursor_yields_first_batch_before_terminal_task_complete`
- `worker_streamable_final_result_sends_taskbatch_before_taskcomplete`
- `network_dispatcher_forwards_final_taskbatch_to_cursor_without_buffering`
- `whole_record_table_query_emits_per_location`
- `whole_record_cancelled_consumer_stops_before_all_row_groups_scanned`
- `whole_record_pbc_per_location_matches_concatenated_pbc`
- `non_streamable_final_result_remains_completion_buffered`
- `coordinator_backpressure_pauses_upstream_producers`

## Phase 1 — Wendelon whole-record per-location refactor (D2)

In `data_server.rs`'s `'ranges` scan loop whole-record branch (`:13933-14017`):

1. Delete `rebatch_groups: Vec<RebatchGroup>` accumulation from the descriptor-backed whole-record path
   (`:13999-14017`).
2. Per-location: after materialize + runtime-filter, if `row_identity_projection == Append` call
   `survival_mask(program, &batch)` before identity append (preserves Codex detail a / matches existing
   post-concat order at `:14037-14060`); then `pbc::execute_program(program, &batch)`; then
   `append_query_row_identity_column` if applicable; then emit via `writer.emit_batch(&result).await?`.
3. Move `max_hlc`/`input_rows`/`row_limit_satisfied` accounting to per-location updates (Codex detail b).
4. Descriptor-backed typed-table queries assert emitted whole-record schemas match the table descriptor.
   If descriptorless or mixed-schema paths still rely on `rebatch_groups`, keep a compatibility path or
   add a separate behavior proof before widening scope.
5. Delete `for group in rebatch_groups { ... }` (`:14020-14092`) and the `// ADR 118 cost-bound scope:`
   comment.

## Phase 2 — Worker `drive_streamable_plan` sink for FinalResult (D3)

In `crates/toron/src/server/mod.rs`:

1. Identify streamable-FinalResult: `spec.output == TaskOutputBinding::FinalResult` AND terminal plan is
   streamable (reuse the 119 `is_streamable_*` predicates or add a `is_streamable_final_result(spec)`
   sibling).
2. For streamable-FinalResult, refactor `drive_streamable_plan` to use an async batch sink (or add an
   async sibling used by this path). The sink replacing `&mut collect` at `:440` encodes a
   `TaskBatchPayload` per batch and awaits the existing `MessageKind::TaskBatch` send on the
   worker→coordinator connection. No `results.push` and no hidden unbounded queue for this case.
3. `TaskExecutionReport.results` is empty for streamable-FinalResult (lexical pin G4).
4. `TaskComplete` still fires at end with terminal status + telemetry (existing path unchanged).
5. Non-streamable FinalResult plans keep the existing Vec-collecting path (G10 boundary).

## Phase 3 — `NetworkTaskDispatcher` forwards FinalResult `TaskBatch` immediately (D4)

In `server/mod.rs:3033-3037`:

1. Add a sink-installation API the scheduler/coordinator wires at terminal-stage dispatch.
2. When a cursor sink is installed for a FinalResult task, decoded `TaskBatchPayload`s route to the
   cursor channel as they arrive. No local `Vec` collection for these tasks.
3. `TaskComplete` continues to drive scheduler state without gating cursor visibility.

## Phase 4 — Scheduler streams FinalResult to bounded channel (D8)

`SchedulerRun.final_results: Vec<RecordBatch>` is removed (or kept only for non-streamable terminal
plans' compatibility). The scheduler emits FinalResult batches via the sink the coordinator's driver
(Phase 5) wires through `mpsc::channel(16)`.

Edge cases: scheduler error mid-query → `Err(...)` on channel + close; success → close sender → cursor
`.next()` returns `None`.

## Phase 5 — Coordinator `execute_query_stream` + background driver (D5, D7)

```rust
pub async fn execute_query_stream(
    &self,
    plan: &LogicalPlan,
) -> Result<DistributedResultStream, QueryError>;
```

Body:
1. Admission + active-query registration (move lifetimes onto driver/cursor).
2. Construct cursor channel: `mpsc::channel(16)`.
3. `tokio::spawn` the driver: runs `dispatch_loop` with the cursor sender wired into FinalResult
   forwarding; on completion or error, sender closes (or sends `Err`).
4. Return `DistributedResultStream` immediately, wrapping receiver + cancellation handle + RAII completion.

`DistributedResultStream::Drop` (D7) synchronously calls `ActiveQueryRegistry`'s cancel (non-blocking
registration). The driver observes the cancellation and runs remote-cancel fanout (existing 119 + 118
chain).

## Phase 6 — Compatibility wrapper (D6)

```rust
pub async fn execute_query(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>, QueryError> {
    let mut stream = self.execute_query_stream(plan).await?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}
```

DDL/DML and mutation-source returning paths keep their existing collecting implementation (V1 boundary
G12).

## Phase 7 — 118 scope-pin retirement + 120 guards (D9)

In `crates/wendelon/tests/architecture_test.rs`:
- **Delete** `adr118_cost_bound_scope_is_projected_path`.
- **Add** `adr120_cost_bound_is_access_path_independent_for_descriptor_backed_queries` (G8).
- **Add** `adr120_whole_record_path_emits_per_location` (G6).
- **Add** `adr120_rebatch_groups_post_scan_emit_is_gone_for_descriptor_backed_queries` (G7).
- **Add** `adr120_no_wendelon_protocol_widening` (G13).

In `crates/toron/tests/architecture/distributed_execution/`:
- `adr120_coordinator_has_streaming_cursor_surface` (G1)
- `adr120_collecting_execute_is_compatibility_wrapper` (G2)
- `adr120_execute_query_stream_uses_background_driver` (G3)
- `adr120_worker_streams_final_result_taskbatch_frames` (G4)
- `adr120_network_dispatcher_forwards_final_taskbatch_immediately` (G5)
- `adr120_cursor_drop_cancels_active_query_synchronously` (G9)
- `adr120_non_streamable_final_result_uses_completion_buffer` (G10)
- `adr120_119_multi_upstream_barrier_untouched` (G11)
- `adr120_no_sql_rune_ir_change` (G12)
- `adr120_no_toron_worker_protocol_widening` (G14)

## Phase 8 — Behavior tests + composition honesty

All Phase 0 gates plus the hop-specific architecture guards. Do not require a synthetic "swap each hop
back to Vec buffering" test unless implementation introduces explicit test-only toggles; the behavior
gates plus G3/G4/G5/G6/G7/G8/G9 are the reversion pins. Include
`non_streamable_final_result_remains_completion_buffered` and `coordinator_backpressure_pauses_upstream_producers`.

## Phase 9 — Verification

```sh
cargo fmt
cargo test -p wendelon
cargo test -p wendelon --test architecture_test adr118 adr120
cargo test -p toron distributed::scheduler::tests
cargo test -p toron --test architecture_test adr117 adr118 adr119 adr120
cargo test -p toron --test wendelon_storage_execution_test
cargo test -p toron --test distributed_process_test
cargo test -p toron
cargo check -p toron --all-targets
git diff --check
```

## Acceptance

Both headline gates green; D2/D3/D4/D5 hop-specific gates and guards pin the streaming composition;
cursor first batch arrives before terminal-task `TaskComplete`; cursor drop propagates cancellation end-to-end
regardless of access path; `execute_query` collecting behavior unchanged; 117/119 barrier exact; no
SQL/Rune/IR change; no Wendelon protocol widening; no Toron worker wire widening; the 14 `adr120_*`
guards + 118's retired guard properly replaced; 115-119 regressions hold; full Toron + Wendelon suites
+ `--all-targets` green.
Migration: `plans/adr/120_end_to_end_bounded_first_rows.md`.
