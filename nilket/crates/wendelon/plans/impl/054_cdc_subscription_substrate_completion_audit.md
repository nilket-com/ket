# ADR 054 Completion Audit

Objective audited: finish the ADR 054 CDC subscription substrate
implementation work.

Audit date: 2026-05-04.

## Success Criteria

Implementation success means:

- ADR 054 wire/schema/storage/network/client surfaces are present.
- The named implementation-plan files exist.
- The named tests, architectural guards, runbook, roadmap update, and
  quick bench smoke exist and pass.
- The implementation preserves the load-bearing council decisions:
  full-cursor catchup bounds, safe global snapshot cut, live-tail
  safe-emission watermark, open-time ring continuity, locality-scoped
  heartbeat, raw integer carriers for sparse enums, visible-state
  checkpoint digest, bounded metric labels, and substrate-only scope.

Promotion-to-shipped success additionally requires the full bench
acceptance matrix on representative hardware. That gate is still open.

## Prompt-To-Artifact Checklist

| Requirement | Evidence | Status |
|-------------|----------|--------|
| CDC schema types and raw sparse-enum carriers | `src/schema/cdc.rs`; `tests/cdc_cursor_ordering.rs`; `tests/cdc_frame_schema.rs`; architecture guard `adr054_cdc_subscription_substrate_contract` | Done |
| Four CDC `MsgType` constants, no new frame flags | `src/schema/core.rs`; architecture guard | Done |
| `CdcLiveTailRing`, staged entries, finalization release, eviction frontiers | `src/storage/cdc.rs`; `tests/cdc_live_tail.rs`; `tests/cdc_admission_stage.rs`; `tests/cdc_finalization_release.rs` | Done |
| Admission stages only become visible after finalization | `tests/cdc_admission_stage.rs`; data-server `stage_cdc_table_children` / `cdc_release_pending_finalization*` guards | Done |
| Pending-finalization failure / eviction accounting | `src/storage/cdc.rs`; `src/network/data_server.rs`; `tests/cdc_live_tail.rs`; `tests/cdc_finalization_release.rs` | Done |
| Subscribe handler, catchup, live tail, checkpoints, rejects | `src/network/data_server.rs`; `src/network/cdc.rs`; `tests/cdc_frame_round_trip.rs`; architecture guard | Done |
| Full-tuple catchup bounds and same-HLC suffix regression | `cdc_cursor_in_catchup_window`; `tests/cdc_catchup.rs`; `tests/cdc_cursor_ordering.rs`; architecture guard | Done |
| Safe global snapshot cut and live safe-emission watermark | `cdc_capture_snapshot_hlc`, `cdc_safe_emission_hlc`, `drain_cdc_ready_live_entries`; `tests/cdc_live_tail.rs`; data-server unit tests; architecture guard | Done |
| Open-time ring-continuity guard | `CdcLiveTailRing::open_time_ring_continuity_check`; `tests/cdc_live_tail.rs`; architecture guard | Done |
| Locality-scoped heartbeat, not per table | `CdcLocalityHeartbeatWorker`; `spawn_cdc_locality_heartbeat_supervisor`; data-server heartbeat tests; architecture guard | Done |
| `SnapshotAtOpen` final empty `CDC_FRAME` with `STREAM_END` | `handle_cdc_subscribe_stream`; `cdc_emit_empty_frame`; `tests/cdc_frame_round_trip.rs` | Done |
| Resume digest validation and checkpoint digest domain | `CdcSubscribeRequestMeta`; `CdcCheckpointMeta`; `tests/cdc_checkpoint.rs`; handler open path | Done |
| Slow-consumer nonblocking fanout and recovery frontier | `CdcSubscriptionRegistry::fanout_finalized_entry`; `CdcLiveTailRing::evicted_cursor_frontier`; `tests/cdc_backpressure.rs`; `tests/cdc_live_tail.rs` | Done |
| Cancel/close unregisters subscription | handler write-error path; `CdcSubscriptionRegistry::unregister`; `tests/cdc_subscribe_open.rs` | Done, locally covered |
| All eight reject codes are typed and terminal | `CdcRejectCode::ALL`; `cdc_emit_reject`; `tests/cdc_frame_round_trip.rs` | Done, wire-level |
| CDC client API | `src/client/core.rs`; `crates/wendelon-client/src/lib.rs`; architecture guard | Done |
| Bounded `cdc_*` metrics and `cdc_event` compression path | `src/metrics/registry.rs`; `src/network/cdc.rs`; `plans/ops/observability.md`; `tests/metrics_smoke.rs`; architecture guard | Done |
| Audit events / runbook / roadmap | `src/audit.rs`; `plans/ops/runbooks/cdc_subscription.md`; `plans/ops/runbooks/README.md`; `plans/wip/roadmap.md` | Done |
| Fixture manifest | `tests/fixtures/cdc_subscription/README.md` | Done |
| Bench harness, quick smoke, acceptance verifier, and evidence wrapper | `benches/cdc_subscription.rs`; `scripts/bench/run_cdc_subscription_acceptance.sh`; `plans/impl/054_cdc_subscription_substrate_bench_results.md`; verification gate commands | Done |
| Full bench acceptance thresholds | `scripts/bench/run_cdc_subscription_acceptance.sh <output-dir> <measured>` runs `benches/cdc_subscription.rs --full --acceptance-one-way-quic-rtt-micros <measured>`, captures environment metadata, and validates that every expected `cdc_acceptance_*` line reports `status=pass`. `plans/impl/054_cdc_subscription_substrate_bench_results.md` includes a representative acceptance template for hardware, topology, RTT method, command, and pass/fail output. The full matrix has been executed locally without a representative RTT verifier, and the acceptance verifier has been smoke-tested locally with a placeholder RTT. Catchup throughput and admission-stage isolation clear the starting thresholds in-process; real one-way QUIC RTT and representative-hardware small / medium / large matrix are still not measured. | Open |

## Verification Commands

Gate commands run during this audit cycle:

```sh
cargo test --lib schema::cdc
cargo test --lib network::cdc
cargo test --lib storage::cdc
cargo test cdc_cursor_ordering --quiet
cargo test cdc_frame_schema --quiet
cargo test cdc_live_tail --quiet
cargo test cdc_admission_stage --quiet
cargo test cdc_finalization_release --quiet
cargo test cdc_subscribe_open --quiet
cargo test cdc_frame_round_trip --quiet
cargo test cdc_catchup --quiet
cargo test cdc_checkpoint --quiet
cargo test cdc_backpressure --quiet
cargo test --test architecture_test --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench cdc_subscription --no-run
cargo bench --bench cdc_subscription -- --quick
cargo bench --bench cdc_subscription -- --quick --acceptance-one-way-quic-rtt-micros 1000
CDC_ACCEPTANCE_BENCH_ARGS=--quick CDC_ACCEPTANCE_MATRIX_ROWS=1024 scripts/bench/run_cdc_subscription_acceptance.sh /tmp/wendelon-cdc-acceptance-smoke 1000
cargo bench --bench cdc_subscription -- --rows 10000 --iters 3
cargo bench --bench cdc_subscription -- --full
cargo check --quiet
cargo check --benches --quiet
git diff --check
git diff --cached --check
cargo fmt --check
```

## Residual Risk

- The quick, 10k, and local `--full` bench samples are synthetic and
  in-process. They are useful for code review and regression shape.
  The acceptance verifier smoke uses a placeholder 1000 microsecond
  RTT only to prove the reporter and failure gate are wired. The
  local `--full` matrix clears the catchup-throughput and
  admission-stage isolation starting thresholds locally. It does not
  measure real one-way QUIC RTT or representative deployment
  hardware, so the promotion-to-shipped performance gate remains
  open.
- Some stream lifecycle behavior is covered through the registry and
  wire helpers rather than a full QUIC client/server CDC E2E. The
  handler paths are guarded architecturally and by focused unit tests,
  but a future full CDC integration scenario should exercise the
  complete transport path.
