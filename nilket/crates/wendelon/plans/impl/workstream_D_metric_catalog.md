# Workstream D Metric Catalog

Date: 2026-04-22
Author Role: Lead Architect
Workstream: `D1`
Roadmap Source: `plans/impl/workstream_D.md`

## Purpose

This catalog defines the metrics that are load-bearing for Workstream `D`.

The point is not to list every counter in the codebase. The point is to identify:

- the metrics that gates and operator claims depend on
- who owns their correctness in code
- who owns their dashboard/alert meaning
- which milestone requires them to be real
- which metrics still do not exist and therefore block honest claims

## Ownership Model

Wendelon is still founder-scale. “Owner” here means component ownership, not a large-team org chart.

- `Code Owner`: the workstream/component responsible for emitting the metric correctly
- `Dashboard/Alert Owner`: the workstream responsible for turning the metric into usable evidence

Current owner mapping:

- `B`: Stellarator correctness and write semantics
- `C`: transport, runtime, admission, and storage-execution shape
- `D`: dashboards, alerts, validation, and gate evidence
- `G`: audit/evidence semantics

## Catalog

Status values:

- `live`: metric exists now and is suitable to build on
- `gap`: metric does not exist yet and must be added before the named gate is honest

| Metric | Status | Semantic Definition | Code Owner | Dashboard / Alert Owner | Required By | Primary Source |
|---|---|---|---|---|---|---|
| `wendelon_data_ring_write_requests_total` | `live` | Count of handled write requests at the Data Ring front door | `B` | `D` | `M1` | `src/metrics/registry.rs`, `src/network/data_server.rs` |
| `wendelon_data_ring_read_requests_total` | `live` | Count of handled read requests | `B` | `D` | `M1` | same |
| `wendelon_data_ring_query_requests_total` | `live` | Count of handled query requests | `C` | `D` | `M1` | same |
| `wendelon_data_ring_migrate_requests_total` | `live` | Count of handled migrate requests at the Data Ring front door | `C` | `D` | `M2` | same |
| `wendelon_group_commit_window_seconds` | `live` | Time spent coalescing writes before durability flush | `B` | `D` | `M1` | `src/metrics/registry.rs` |
| `wendelon_quic_handshake_failures_total{class=*}` | `live` | Handshake failures broken down into transport, identity, and authorization classes | `C` | `D` | `M2` | `src/network/server.rs`, `src/metrics/registry.rs` |
| `wendelon_transport_active_connections` | `live` | Current admitted inbound transport connections | `C` | `D` | `M1` | `src/network/server.rs` |
| `wendelon_transport_active_stream_handlers` | `live` | Current admitted stream-handler tasks | `C` | `D` | `M1` | `src/network/server.rs` |
| `wendelon_transport_admission_rejections_total{reason=*}` | `live` | Listener-level overload rejections by reason | `C` | `D` | `M1` | `src/network/server.rs`, `src/metrics/registry.rs` |
| `wendelon_data_ring_backpressure_rejections_total{scope=*}` | `live` | Post-auth request rejections by tenant/actor scope | `C` | `D` | `M1` | `src/network/data_server.rs`, `src/metrics/registry.rs` |
| `wendelon_data_ring_backpressure_active_tenant_requests` | `live` | In-flight tenant-scoped requests admitted after auth | `C` | `D` | `M1` | same |
| `wendelon_data_ring_backpressure_active_actor_requests` | `live` | In-flight actor-scoped requests admitted after auth | `C` | `D` | `M1` | same |
| `wendelon_storage_wal_blocking_queue_depth` | `live` | Queue depth for isolated WAL/control metadata blocking work | `C` | `D` | `M2` | `src/storage/file_ops.rs`, `src/metrics/registry.rs` |
| `wendelon_storage_slab_blocking_queue_depth` | `live` | Queue depth for isolated slab metadata blocking work | `C` | `D` | `M2` | same |
| `wendelon_l1_read_latency_seconds` | `live` | Storage-side latency for L1 local reads | `C` | `D` | `M2` | `src/metrics/registry.rs` |
| `wendelon_l2_range_read_latency_seconds` | `live` | Storage-side latency for L2 range reads | `C` | `D` | `M2` | same |
| `wendelon_l2_upload_latency_seconds` | `live` | Storage-side latency for L2 upload operations | `C` | `D` | `M2` | same |
| `wendelon_storage_tiering_passes_total` | `live` | Completed tiering sweeper passes | `C` | `D` | `M2` | `src/metrics/registry.rs` |
| `wendelon_storage_tiering_pass_errors_total` | `live` | Tiering sweeper passes that errored | `C` | `D` | `M2` | same |
| `wendelon_storage_tiering_failures_total` | `live` | Per-slab tiering failures | `C` | `D` | `M2` | same |
| `wendelon_storage_rehydration_guard_skips_total` | `live` | Rehydration attempts skipped by thrash guard | `C` | `D` | `M2` | same |
| `wendelon_storage_rehydrated_reclaims_total` | `live` | Rehydrated slabs reclaimed from L1 | `C` | `D` | `M2` | same |
| `wendelon_storage_migration_reclaimed_slabs_total` | `live` | Tombstoned slabs reclaimed after migration | `C` | `D` | `M2` | same |
| `wendelon_storage_migration_reclaimed_bytes_total` | `live` | Bytes reclaimed after migration | `C` | `D` | `M2` | same |
| `wendelon_data_ring_lease_refresh_passes_total` | `live` | Lease refresh loop executions | `C` | `D` | `M2` | `src/network/data_server.rs` |
| `wendelon_data_ring_lease_grants_total` | `live` | Lease grants accepted into cache | `C` | `D` | `M2` | same |
| `wendelon_data_ring_lease_denials_total` | `live` | Lease refresh attempts denied | `C` | `D` | `M2` | same |
| `wendelon_data_ring_lease_errors_total` | `live` | Lease refresh failures | `C` | `D` | `M2` | same |
| `wendelon_data_ring_lease_cache_entries` | `live` | Current lease descriptors cached locally | `C` | `D` | `M2` | same |
| `wendelon_audit_materialization_lag_entries` | `live` | Gap between authoritative audit entries and local materialization | `G` | `D` | `M3` | `src/metrics/registry.rs` |
| `wendelon_audit_emit_node_local_total` | `live` | Node-local audit events emitted | `G` | `D` | `M3` | same |
| `wendelon_audit_emit_raft_log_total` | `live` | Audit events emitted from Raft authority | `G` | `D` | `M3` | same |
| `wendelon_audit_sink_failures_*` | `live` | Sink failures by local/syslog/kafka family | `G` | `D` | `M3` | same |
| `wendelon_otlp_spans_dropped_total` | `live` | Spans dropped before export because local telemetry handling could not keep up | `D` | `D` | `M2` | `src/metrics/registry.rs` |
| `wendelon_data_ring_write_outcome_total{outcome=*}` | `live` | Count of writes by terminal/retryable outcome state | `B` | `D` | `M1` | `src/metrics/registry.rs`, `src/network/data_server.rs` |
| `wendelon_data_ring_pending_finalization_backlog` | `live` | Current count of unresolved pending-finalization records | `B` | `D` | `M1` | `src/network/pending_finalizations.rs`, `src/network/data_server.rs`, `src/metrics/registry.rs` |
| `wendelon_data_ring_pending_finalization_replay_total{path=*}` | `live` | Replays/import/reconcile attempts of pending-finalization work by path | `B` | `D` | `M1` | `src/network/data_server.rs`, `src/metrics/registry.rs` |
| `wendelon_data_ring_pending_finalization_replay_latency_seconds` | `live` | Time spent replaying or reconciling pending-finalization work | `B` | `D` | `M1` | same |
| `wendelon_data_ring_durable_quorum_latency_seconds` | `live` | Time from request admission to durable quorum satisfied | `B` | `D` | `M1` | `src/network/data_server.rs`, `src/metrics/registry.rs` |
| `wendelon_storage_wal_append_latency_seconds` | `live` | WAL append latency before flush | `C` | `D` | `M2` | `src/consensus/raft_storage.rs`, `src/metrics/registry.rs` |
| `wendelon_storage_wal_fsync_latency_seconds` | `live` | WAL durability flush latency | `C` | `D` | `M2` | same |
| `wendelon_io_uring_submission_queue_depth{class=*}` | `live` | Pending Wendelon-side `io_uring` submissions queued for each durability class before kernel dispatch | `C` | `D` | `M2` | `src/storage/io_uring.rs`, `src/metrics/registry.rs` |
| `wendelon_io_uring_completion_queue_depth{class=*}` | `live` | In-flight `io_uring` operations awaiting completion for each durability class | `C` | `D` | `M2` | same |
| `wendelon_io_uring_errors_total{class,op,errno}` | `live` | `io_uring` submit or operation failures broken down by class/op/error | `C` | `D` | `M2` | same |
| `wendelon_data_ring_migration_proxy_reads_total{outcome=*}` | `live` | Lazy-migration point-read proxy attempts by outcome | `C` | `D` | `M2` | `src/network/data_server.rs`, `src/metrics/registry.rs` |
| `wendelon_data_ring_migration_proxy_read_latency_seconds` | `live` | Latency added by lazy-migration point-read proxy resolution | `C` | `D` | `M2` | same |

## Post-D2 Follow-On

The next instrumentation pass should close new blind spots only if later workstreams surface them.

Right now the D2 metric queue is closed: the first D2 pass covered B-path and WAL-path metrics, D2.2 covered lazy-migration proxy cost, and D2.3 closed `io_uring` backlog/error visibility.

## D1 Outcome

`D1` is complete when these three things are true:

1. the metric names that gate `B` and `C` are frozen enough to build docs and alerts around
2. every live series has a code owner and a dashboard/alert owner
3. every missing series is called out as a gate-blocking implementation gap instead of being hand-waved into “later”

This catalog now satisfies that bar. It does not satisfy `D2`. The gap rows above are the explicit D2 queue.

## Rules

- A metric is not “live” unless its semantics are narrow enough that a second person could build a dashboard from the name and definition alone.
- Request counters are not enough. Every critical path also needs at least one state, latency, or rejection series.
- Missing metrics in this catalog are treated as real implementation gaps, not documentation gaps.
