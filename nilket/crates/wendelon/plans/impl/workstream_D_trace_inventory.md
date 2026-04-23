# Workstream D Trace Inventory

Date: 2026-04-22
Author Role: Lead Architect
Workstream: `D2`
Roadmap Source: `plans/impl/workstream_D.md`

## Purpose

This is the minimum trace inventory needed to keep `B` and `C` debuggable.

It is intentionally narrow. A span belongs here only if it helps answer one of these questions:

- where did a write, read, or query spend time
- did pending-finalization recovery happen, and on which path
- which critical paths still have no dedicated trace coverage

## Live Spans

| Span | Source | Why It Exists | Key Fields |
|---|---|---|---|
| `data_ring.write.request` | `src/network/data_server.rs` `handle_write` and `handle_bulk_stream` | Front-door write trace for unary and bulk writes. This is the first useful latency/correlation seam for the bifurcated write path. | `stream_id`, `tenant_id`, `expected_epoch`, `await_finalization`, `durability`, `write_sequence` |
| `data_ring.read.request` | `src/network/data_server.rs` `handle_read` | Front-door read trace with consistency context. | `stream_id`, `tenant_id`, `expected_epoch`, `consistency` |
| `data_ring.query.request` | `src/network/data_server.rs` `handle_query` | Unary query trace for plan + execution latency. | `stream_id`, `tenant_id`, `expected_epoch`, `consistency` |
| `data_ring.query.stream` | `src/network/data_server.rs` `handle_query_stream` | Streaming query trace for plan construction and stream-oriented execution. | `stream_id`, `tenant_id`, `expected_epoch`, `consistency` |
| `data_ring.pending_finalization.reconcile` | `src/network/data_server.rs` `try_reconcile_pending_record_once` | Tells an operator whether pending-finalization work was replayed by duplicate retry, background reconciler, or orphan-witness import path. | `node_id`, `tenant_id`, `epoch`, `path`, `pending_id` |
| `data_ring.migration.proxy_read` | `src/network/data_server.rs` `try_migrate_point_read` | Makes lazy-migration point-read fallback visible, including whether the read resolved, authoritatively missed, or stayed incomplete. | `node_id`, `tenant_id`, `key_locality`, `as_of_hlc_physical_ms`, `as_of_hlc_logical`, `as_of_hlc_node_id`, `migration_id`, `source_count`, `outcome` |
| `storage.io_uring.batch` | `src/storage/io_uring.rs` `io_thread_loop` | Internal ring-visibility span for queue depth and completion readiness by durability class. | `class`, `submission_queue_depth`, `completion_queue_depth`, `cq_ready` |

## What These Spans Cover

- `B` duplicate/replay recovery is now trace-visible at the reconciliation seam.
- `B/C` request entry for write/read/query is now trace-visible with enough fields to join against metrics and logs.
- `C` lazy-migration point reads are now trace-visible at the actual fallback seam instead of being inferred from request spans.
- `C/D` now have an internal `io_uring` trace seam that can explain ring backlog vs completion behavior without guessing from outer request spans.
- `D` can now inventory real trace names instead of hand-waving around OTLP support.

## Remaining Follow-Ons

`D2` is first-pass complete. These are the remaining follow-ons that stayed intentionally out of the first landing:

- no dedicated `control_ring.wal.append` span yet
- no dedicated `control_ring.wal.fsync` span yet
- no listener/connection-level span inventory in `src/network/server.rs`

## Post-D2 Priority

If I were sequencing the next trace pass, I would do it in this order:

1. `control_ring.wal.append`
2. `control_ring.wal.fsync`
3. listener/connection admission spans only if real operator debugging still needs them

The reason for this order is simple: request-entry, lazy-migration, and `io_uring` internal spans now exist, but the remaining blind spots are still inside durability and listener admission behavior.
