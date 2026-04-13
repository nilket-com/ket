# Observability & SLOs

Wendelon must be highly transparent to operators to diagnose network partitions, Raft bottlenecks, and storage tiering issues.

## 1. Tracing
- **OpenTelemetry:** Every request flowing through the system carries a W3C trace context.
- **Spans:** Key spans include `quic_stream_accept`, `raft_propose`, `io_uring_write`, and `background_sweeper_upload`.

## 2. Metrics (Prometheus)
Key gauges and histograms:
- `raft_commit_latency_ms`: Time to achieve quorum.
- `data_ring_write_latency_ms`: Client-observed write latency.
- `e_safe_lag_ms`: Delta between wall clock and the global `E_safe` watermark.
- `slab_tiering_bytes`: Volume of data residing in L0, L1, and L2.
- `proxy_hop_count`: Rate of proxied writes (indicates ring churn or client stale routing).

## 3. Service Level Objectives (SLOs)
Wendelon targets the following strict SLOs for a healthy cluster:
- **Availability:** 99.99% uptime for Data Ring reads/writes.
- **Latency (L1 Read):** p99 < 10ms for local NVMe reads.
- **Latency (L2 Read):** p99 < 150ms for S3/Object storage reads.
- **Durability:** 99.999999999% (11 nines) via L2 object storage replication.