# ADR 027: Observability Posture — OTLP, Tracing Stack, Health/Metrics Split

## Status
Proposed

## Context
Operators running Wendelon in production must be able to diagnose a slow request across nodes, correlate a quorum stall with a specific Raft proposal, spot a tiering hot-spot before it becomes an outage, and scrape authenticated metrics into their existing monitoring stack. The earlier `plans/ops/observability.md` draft named OpenTelemetry and Prometheus at a high level without pinning protocols, stack composition, endpoint security, or listener topology. This ADR closes those gaps.

Audit is a separate concern (ADR 025). Observability is for operators; audit is for security/compliance reviewers. Three streams — traces, metrics, audit — with three different durability, security, and retention postures.

Forces in play:
- OTLP is the 2026 industry convergence point. Jaeger, Tempo, Honeycomb, Datadog, New Relic, and the OpenTelemetry Collector all ingest OTLP natively. Shipping native protocols for any of them is maintenance tax with no operator benefit.
- Rust's `tracing` ecosystem is the obvious substrate; the question is which layers compose into the default subscriber and whether runtime reconfiguration is required.
- Health checks and metrics scraping have incompatible security postures. Load balancer and k8s liveness probes are unauthenticated by construction; authenticated metrics scrapers have full cert material. Sharing a listener forces the weaker posture on both or breaks the weaker client.
- An operator who cannot trace a slow request across QUIC RPCs cannot operate the cluster. Context propagation across the transport is not optional depth — it is a minimum-viable property.
- Metrics exposure is a security surface. Unauthenticated metrics leak cluster-internal state (node count, leader identity, per-tenant byte counts, key IDs if not filtered) that helps an adversary plan an attack. Authentication on metrics is not over-engineering.

## Decision

1. **OTLP is the only trace export protocol.** Traces are exported over OTLP (gRPC preferred; HTTP/protobuf supported as a configuration option). No native Jaeger, no Zipkin, no vendor-specific protocols. Deployments that need to terminate traces in a Jaeger or vendor backend run the OpenTelemetry Collector in front of their backend — the Collector is the protocol-translation layer, not the Wendelon binary.

   The OTLP endpoint is configured with:
   - Destination URL.
   - mTLS client certificate (reusing the Node CA or a dedicated observability CA — see Open Questions).
   - Sampling policy (head-based, configurable sampling rate; default 1% in production, 100% in development).
   - Batch export configuration (max queue size, export timeout) with operator-tunable defaults.

2. **`tracing-subscriber` stack composition is fixed at binary startup.** The tracing subscriber is composed as a deterministic layer stack:
   - `EnvFilter` (per-module level filtering, configured via `WENDELON_LOG` env var and a reload handle).
   - `fmt` layer — JSON format in production builds, human-readable TTY format in development builds. Selected at compile time via a build flag.
   - OTLP exporter layer, always present in production; enabled in development when an OTLP endpoint is configured.
   - Metrics bridge layer (see decision §4) that derives metric values from span durations and events.

   The stack is composed once at startup and does not hot-swap layers. `EnvFilter` supports log-level changes via a reload handle accessible through an authenticated admin RPC; layer composition itself is immutable for the process lifetime.

3. **Trace context propagates across QUIC RPCs.** Every inbound RPC extracts W3C trace context (`traceparent`, `tracestate`) from a dedicated frame header on the first QUIC stream of the RPC. Every outbound RPC injects the current trace context into the same header. Context propagation is a property of the RPC framework, not an opt-in per call site; a call site that forgets to propagate is a bug the framework catches.

   Span taxonomy is fixed at a curated set of top-level spans: `quic.stream.accept`, `rpc.dispatch.{operation}`, `raft.propose`, `raft.apply`, `storage.l1.write`, `storage.l2.put_slab`, `l2.sweeper.upload`, `audit.emit`, and a small number of others. Call sites inside these spans create nested spans at their discretion. The curated top-level set is documented and stable; adding a new top-level span is a cross-cutting change that requires a doc update.

4. **Metrics are OpenMetrics (Prometheus-compatible), scraped from an authenticated endpoint.** The metrics endpoint:
   - Listens on a **separate port** from both `/health` and `/ready` (decision §5).
   - Requires mTLS with the cert's role in `{operator, read_only_observer}`. Scrapers present a cert; `data_node` certs do not have metrics-scrape authority.
   - Serves OpenMetrics text format (Prometheus parseable) on `/metrics`.
   - Includes standard process metrics (CPU, memory, file descriptors) plus Wendelon-specific metrics (see §6).
   - Does not include any authentication-sensitive material in label values (no full cert subjects, no raw key IDs, no `tenant_id` in labels for cardinality reasons — see §7).

5. **Three endpoints on three distinct listeners, because TLS client-auth posture is decided at handshake time.**

   A TLS server decides whether to *request* or *require* a client certificate during the handshake, before any HTTP path routing happens. That means `/health` (unauthenticated) and `/ready` (mTLS-required) cannot share one listener — there is no per-path control over client-auth in the handshake. The topology therefore uses three distinct listeners, each bound to its own port, with the auth posture fixed at the listener level:

   | Endpoint | Listener | TLS posture | Output | Consumers |
   |----------|----------|-------------|--------|-----------|
   | `/health` | Health port (plain TCP, unauthenticated) | No TLS — plain HTTP, or TLS-without-client-auth if the operator wants cert-encrypted probes. | `200 OK` body `ok` if process is alive, `503` otherwise. No other information. | k8s liveness probe, load balancer health check. |
   | `/ready` | Readiness port (mTLS required) | mTLS required; role in `{operator, read_only_observer, data_node}`. | Structured JSON: cluster member list, leader, epoch, drain status, `audit_materialization_lag_entries`, degraded-reason enum if any. | k8s readiness probe (via service mesh that terminates mTLS), operator `wendelon-ctl status`. |
   | `/metrics` | Metrics port (mTLS required) | mTLS required; role in `{operator, read_only_observer}`. | OpenMetrics text format. | Prometheus-compatible scrapers. |

   Three endpoints, three listener ports, three security postures. Readiness and metrics are on separate listeners because they have different role-authorization sets (`/ready` allows `data_node`; `/metrics` does not); keeping them separate means firewall policy can differ per port and each can be scraped independently.

   `/health` returns no state; an unauthenticated probe cannot learn whether the cluster is healthy, only whether this process is alive. State queries require authentication by construction, enforced at the listener.

6. **Observability depth at pilot is the full complement, not a subset.** The pilot ships with:
   - Distributed tracing with context propagation across QUIC RPCs (decision §3).
   - Histogram-based latency metrics with dimensional labels (not just counters).
   - OTLP export for traces.
   - Authenticated OpenMetrics scrape for metrics (decision §4).

   "Pilot depth" means an operator can, on day one: trace a slow request across nodes, open a histogram on p99 commit latency, scrape into an existing Prometheus instance, and drive alerting off labeled metrics. Deferring any of these pushes the day operators can self-diagnose into the future.

   The default metric set covers:
   - **Raft.** `raft_propose_latency_seconds` (histogram), `raft_commit_latency_seconds` (histogram), `raft_log_lag_entries` (gauge), `raft_current_leader` (gauge with leader label), `raft_membership_epoch` (gauge).
   - **Transport.** `quic_active_streams` (gauge), `quic_handshake_failures_total` (counter), `rpc_requests_total` (counter with operation/outcome labels), `rpc_latency_seconds` (histogram).
   - **Storage.** `wal_fsync_latency_seconds` (histogram), `storage_tier_bytes` (gauge with tier label), `slab_compaction_backlog_bytes` (gauge), `group_commit_window_seconds` (histogram).
   - **L2.** `l2_upload_latency_seconds` (histogram), `l2_range_read_latency_seconds` (histogram), `l2_orphan_reclaim_bytes_total` (counter).
   - **Crypto/Identity.** `kms_unwrap_latency_seconds` (histogram), `tenant_kek_cache_hit_total` (counter), `cert_expiry_seconds` (gauge with role label).
   - **Process.** Standard process metrics (CPU, memory, FDs, threads).

7. **Label cardinality is bounded by construction.** High-cardinality dimensions (tenant_id, slab_id, key_id) are **not** metric labels. They appear in trace attributes (bounded per-trace) and in audit events (ADR 025), but not in metrics.

   Per-tenant quota usage — which an operator legitimately wants to see — is exposed via a dedicated low-cardinality endpoint (`/tenant-usage` on the metrics port, mTLS-authenticated, operator-only, paginated). Per-tenant metrics that should exist will be added as explicit low-cardinality gauges (e.g., `tenant_count_active` rather than a per-tenant label). The discipline is: if the label's distinct-value count is unbounded, it is not a metric label.

## Rationale

- **OTLP-only eliminates protocol sprawl.** Supporting Jaeger-native and Zipkin-native and vendor-specific protocols is maintenance load for a problem the OpenTelemetry Collector already solves. Operators who need non-OTLP backends run the Collector; Wendelon speaks one protocol.
- **Fixed-at-startup subscriber stack is the right trade between flexibility and predictability.** Runtime-reconfigurable layer stacks produce correctness bugs (spans appear/disappear mid-trace). `EnvFilter` reload via an admin RPC handles the one legitimate runtime-reconfigure case (log-level changes) without opening the general problem.
- **Context propagation as a framework property eliminates a class of bugs.** If call sites have to remember to propagate context, a fraction will forget and the resulting traces will be broken. The framework extracts on ingress, injects on egress; call-site code does not touch `traceparent`.
- **Separate listeners match the security postures to the consumers.** k8s liveness probes cannot do mTLS; metrics scrapers must do mTLS. Three listeners is one more config item than one listener; the win is a security posture that maps correctly to the threat model.
- **Authenticated metrics from day one, not "secure by default later."** Closing an insecure metrics default after it has been deployed is harder than introducing a secure one. The same argument applies to any exposed endpoint: secure on day one, or secure never.
- **Pilot depth must be complete, not incremental.** An operator cannot debug a distributed system with half an observability stack. Shipping tracing-without-metrics or metrics-without-OTLP means diagnosis-by-adjacency: the operator hunts for signal in whichever stream is complete. Both must be complete.
- **Label cardinality discipline is load-bearing.** A single `tenant_id` label on one histogram turns a ~1 KB metric into a ~1 GB metric at 1 million tenants. Cardinality control is not polish; it is the difference between a metrics backend that works and one that falls over.

## Consequences (The "Negatives")

- **Three listeners means three ports to open, firewall, and document.** Real operational cost. Mitigated by documented defaults and by the fact that two of the three ports (health, metrics) are typically local-only behind the orchestrator's service mesh.
- **OTLP-only means operators with Jaeger or Zipkin backends must run the OpenTelemetry Collector.** Additional infrastructure component. Mitigated by the Collector being small, well-known, and already in most operators' toolbox.
- **Cardinality discipline means per-tenant metrics aren't "free."** Operators asking "what's tenant X's commit latency right now?" will not find it as a labeled metric; they will find it in traces (for a sampled request) or in a dedicated low-cardinality endpoint. This is a deliberate trade: the metrics backend survives, and the per-tenant detail lives in traces where it belongs.
- **mTLS on `/ready` means the k8s readiness probe path requires a service mesh or a sidecar that terminates mTLS.** Operators who want readiness without a mesh can use `/health` for both probes and accept coarser readiness; the richer `/ready` requires the mesh. Documented trade-off, not a breakage.
- **`tracing-subscriber` layer ordering is fragile.** The stack's composition order affects what downstream layers see. Mitigated by keeping composition centralized (one function) and by asserting the composition via integration tests.
- **Sampled production traces miss cold rare bugs.** 1% sampling in production means 99% of traces are dropped. Mitigated by tail-based sampling via the OpenTelemetry Collector (keep all traces that contain an error span) as an operator-configurable Collector pipeline, not a Wendelon-side feature.
- **Rejected alternatives explicitly:**
  - *Jaeger/Zipkin/vendor-native trace protocols* — rejected; the Collector solves it.
  - *Shared health+metrics endpoint* — rejected; incompatible security postures.
  - *Unauthenticated metrics* — rejected; leaks cluster-internal state to any port-reachable client.
  - *Per-tenant metric labels* — rejected; cardinality blowup. Per-tenant detail lives in traces and dedicated endpoints.
  - *Deferring distributed tracing to post-pilot* — rejected; operators cannot diagnose the cluster without it.
  - *Runtime-reconfigurable subscriber stack* — rejected for composition-order correctness; `EnvFilter` reload covers the one legitimate runtime case.

## Open Questions

- **Dedicated observability CA vs. Node CA reuse for OTLP export.** The metrics and trace export destinations are outside the cluster's trust boundary in some deployments (SaaS observability vendors). Whether to extend the Node CA to sign OTLP client certs, or to provision a separate observability CA, is an operator deployment choice with security implications. *Resolves via:* a follow-up security note; default is Node-CA reuse for self-hosted observability, documented-caveat for SaaS export.
- **Sampling policy interaction with audit.** Sampled-out traces still corresponded to real RPCs that may have produced audit records. The audit record contains no trace context by design (audit is a separate stream). Whether to carry the sampled-in trace ID into the audit record for correlation is a schema question for ADR 025. *Resolves via:* a one-line ADR 025 schema addendum (an optional `trace_id` field on audit records).
- **Metrics for FIPS build profile.** The FIPS build profile's entropy state, self-test results, and module version are observability-worthy. Whether these are exposed as metrics, as `/ready` fields, or only in logs needs a concrete decision. *Resolves via:* a follow-up note paired with ADR 026 acceptance.
- **Tail-based sampling pipeline documentation.** Operators wanting to keep all error traces need a documented Collector pipeline. Not an ADR-level decision, but a documentation deliverable that should land with pilot GA. *Resolves via:* `plans/ops/observability.md` section (not a separate ADR).

## References
- ADR 004: Zero-copy Transport (QUIC context propagation surface)
- ADR 011: Auto Adaptive Transport (same)
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization (metrics scrape auth, `/ready` auth)
- ADR 025: Audit Event Model (parallel stream; not merged into observability)
- ADR 026: FIPS 140-3 Certification Target (FIPS-visibility metrics open item)
- `plans/ops/observability.md`
