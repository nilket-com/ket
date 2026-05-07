# Observability

Operator-facing telemetry for Wendelon. **Not** the same thing as audit — audit is for security/compliance reviewers and is operationalized in [Audit](./audit.md) on top of ADR 025; observability is for operators diagnosing the running cluster. Three streams, three postures:

| Stream | ADR | Purpose | Security posture | Retention |
|--------|-----|---------|------------------|-----------|
| Traces | 027 | Per-request causal path across nodes | mTLS on export; sampled in prod | Exporter-backend-defined |
| Metrics | 027 | Aggregate timeseries for SLOs and alerting | mTLS on scrape; operator-only | Prometheus-backend-defined |
| Audit | 025 | Attributable record of admin/auth actions | Local chain always-on; off-box via `Syslog`/`Kafka` | ≥ 90d default, tunable |

This document operationalizes ADR 027. Anything architectural goes in the ADR; this document covers **what an operator configures, runs, and alerts on**.

The current workspace implements the three-listener posture, the Prometheus/OpenMetrics exporter, the recording rules, the alert bundle described below, and OTLP trace export over both `http/protobuf` and gRPC. Both transports are implemented in pure Rust and remain feature-gated so the default build stays free of optional exporter code.

---

## 1. Listener topology

Per ADR 027 §5, Wendelon exposes observability and lifecycle endpoints on **three distinct listeners** because TLS client-auth is decided at handshake time, not per path.

| Port (default) | Endpoint | TLS posture | Roles that may connect | Consumer |
|----------------|----------|-------------|------------------------|----------|
| `:7431` | `/health` | Plain TCP, no client auth | anyone with L4 reach | k8s liveness, LB health |
| `:7432` | `/ready` | mTLS required | `operator`, `read_only_observer`, `data_node` | k8s readiness (through mesh/sidecar), `wendelon-ctl status` |
| `:7433` | `/metrics` | mTLS required | `operator`, `read_only_observer` | Prometheus-compatible scrapers |

Rules:
- Ports must be distinct. A deployment may firewall `:7432` and `:7433` to cluster-internal network ranges; `:7431` is typically the only externally reachable listener of the three.
- `/health` returns only `200 OK` with body `ok` (process alive) or `503` (process not alive). It carries **no cluster state** — an unauthenticated prober cannot learn whether the cluster is healthy.
- `/ready` returns structured JSON. Current field set:
  ```json
  {
    "node_id": "n-04",
    "epoch": 1842,
    "leader_id": "n-01",
    "cluster_members": ["n-01","n-02","n-03","n-04","n-05"],
    "drain_state": "none | draining | drained",
    "audit_materialization_lag_entries": 0,
    "degraded_reason": null
  }
  ```
  `degraded_reason` is a typed enum (`AuditLagExceeded`, `WalFsyncElevated`, `L2UploadBacklog`, ...). A node marks itself not-fully-ready whenever `degraded_reason` is non-null or `audit_materialization_lag_entries > 0` (ADR 025 §7).
- `/metrics` serves OpenMetrics text on mTLS. `data_node` certs **cannot** scrape; only `operator` and `read_only_observer`.

### Opening the listeners in config

```toml
[observability.listeners]
health  = "0.0.0.0:7431"
ready   = "0.0.0.0:7432"   # or cluster-internal CIDR
metrics = "0.0.0.0:7433"   # or cluster-internal CIDR
```

All three listener sockets must bind successfully at startup; a bind failure is a **startup error**, not a degraded-mode operation.

---

## 2. Traces

### Protocol

- OTLP only. The current pilot implementation ships both `http/protobuf` and gRPC as optional pure-Rust transports; neither is compiled into the default build unless its cargo feature is enabled.
- No Jaeger-native, no Zipkin-native. Operators using those backends run the **OpenTelemetry Collector** as a translation layer.

### Configuration

```toml
[observability.traces]
endpoint         = "https://otel-collector.internal:4317"
protocol         = "grpc"                       # or "http/protobuf"
sampling_rate    = 0.01                          # 1% default in prod; 1.0 in dev
client_cert_path = "/etc/wendelon/obs-client.crt"
client_key_path  = "/etc/wendelon/obs-client.key"
ca_bundle_path   = "/etc/wendelon/obs-ca.crt"
max_queue_size   = 8192
export_timeout_s = 30
```

Client cert may be signed by the Node CA (self-hosted observability backends) or by a dedicated observability CA (SaaS vendors). See ADR 027 Open Questions for the decision framework; default is Node-CA reuse in-cluster.

### Span taxonomy (curated top-level spans)

Per ADR 027 §3:

- `quic.stream.accept`
- `rpc.dispatch.{operation}` (operation drawn from the closed RPC enum)
- `raft.propose`
- `raft.apply`
- `storage.l1.write`
- `storage.l2.put_slab`
- `l2.sweeper.upload`
- `audit.emit`

Call sites within these spans may create nested spans freely. Adding a new top-level span is a cross-cutting change that requires updating this list and ADR 027.

### Context propagation

W3C `traceparent` / `tracestate` is carried in a dedicated QUIC frame header on the first stream of each RPC. Extraction on ingress and injection on egress are framework-level; call sites do not touch the headers. A forgotten-propagation bug surfaces at framework tests, not at operators.

### Tail-based sampling recipe (Collector pipeline)

There are two operating modes; pick one based on what the deployment actually needs. **The distinction matters because head-sampled-out traces are gone — the Collector cannot recover what Wendelon never exported.**

**Mode A — Cheap default, forward the full 1% head-sampled stream.** Keep `sampling_rate = 0.01` on the Wendelon side. Every head-sampled trace is forwarded to the trace backend without further filtering. No tail-sampler is needed: at 1% of traffic the volume is already small, and decimating the 1% stream further would only lose signal that survived head sampling.

```yaml
processors:
  batch:                # batching only; no tail-sampling processor
    timeout: 2s
    send_batch_size: 1024
```

Trade-off: export bandwidth stays at ~1% of original traffic. 99% of error and slow traces in the original stream are lost at the head before the Collector ever sees them. This mode is appropriate when the deployment tolerates statistical visibility on errors and slowness — fine for developer environments, probably not for production incident forensics.

If an operator on Mode A wants to preserve errors/slow traces at production fidelity while still paying a cheap default-traffic bill, that is Mode B's problem to solve, not a tail-sampler applied to the 1% stream.

**Mode B — Full capture of errors and slow traces.** Set `sampling_rate = 1.0` on the Wendelon side. Every trace is exported to the Collector, which applies the tail-sampler to decide what to forward to the trace backend:

```yaml
processors:
  tail_sampling:
    decision_wait: 10s
    num_traces: 500000
    expected_new_traces_per_sec: 50000
    policies:
      - name: keep-errors
        type: status_code
        status_code: { status_codes: [ERROR] }
      - name: keep-slow
        type: latency
        latency: { threshold_ms: 500 }
      - name: baseline-sample
        type: probabilistic
        probabilistic: { sampling_percentage: 1 }
```

Trade-off: Wendelon → Collector bandwidth is 100x higher than Mode A, and the Collector must be provisioned to handle the full trace rate. In exchange, **every** error trace and **every** slow trace in the traffic reaches the trace backend; only the boring fast-and-successful traces are decimated. This is the correct mode when incident forensics on production traffic is a requirement.

**The two modes cannot be combined.** Head sampling is fire-and-forget: whatever Wendelon drops at the head is not visible to the Collector at all. There is no Mode-C that "tail-samples errors from the dropped 99%" — that stream does not leave the process. Mode A is "forward the 1% head stream as-is"; Mode B is "export 100%, tail-sample at the Collector." Pick one per deployment.

---

## 3. Metrics

### Subscriber composition

The `tracing-subscriber` stack is composed **once** at startup (ADR 027 §2):

1. `EnvFilter` (module-level log filter, reloadable via authenticated admin RPC).
2. `fmt` layer — JSON in prod, human-readable in dev (build flag).
3. OTLP exporter layer.
4. Metrics bridge layer (derives histograms from span durations).

Layer order is fixed in code and asserted by integration test. Runtime layer hot-swap is not supported; only `EnvFilter`'s filter string is reloadable.

### Cardinality discipline (load-bearing)

Per ADR 027 §7: **high-cardinality dimensions are not metric labels.** `tenant_id`, `slab_id`, `key_id`, full cert subjects, raw key IDs — none of these are Prometheus labels. They appear in trace attributes and (where appropriate) in audit events.

Per-tenant detail is intentionally absent from the current `/metrics` surface. Use traces for per-request debugging; a dedicated operator-facing per-tenant usage endpoint remains planned rather than emitted as Prometheus labels.

Operational effect: a tenant-growth event from 1k → 1M tenants does not blow up the Prometheus backend.

### Default metric catalog

The pilot ships with the following Prometheus/OpenMetrics surface. Metric names below are listed exactly as exported, including the `wendelon_` prefix.

#### Raft / Control Ring
| Metric | Type | Labels | Use |
|--------|------|--------|-----|
| `wendelon_raft_current_leader` | gauge | `leader_id` | Present only while a leader exists; the no-leader alert keys off absence |
| `wendelon_raft_membership_epoch` | gauge | none | Current applied membership epoch |
| `wendelon_liveness_tracked_peers` | gauge | none | Peers currently tracked by the liveness monitor |
| `wendelon_liveness_stale_peers` | gauge | none | Peers currently considered stale |
| `wendelon_clock_drift_checks_total` | counter | none | Clock-drift checks executed |
| `wendelon_clock_drift_insufficient_samples_total` | counter | none | Clock-drift checks skipped for insufficient samples |
| `wendelon_clock_drift_exceeded_total` | counter | none | Drift checks that breached the configured bound |
| `wendelon_clock_drift_offset_ms` | gauge | none | Last observed absolute drift offset |
| `wendelon_eviction_sweeps_total` | counter | none | Eviction watchdog passes |
| `wendelon_eviction_refused_quorum_total` | counter | none | Evictions refused to preserve quorum |
| `wendelon_evictions_total` | counter | none | Peers evicted from the ring |
| `wendelon_eviction_advancer_failures_total` | counter | none | Membership-advance attempts that failed |

#### Transport / Data Ring
| Metric | Type | Labels | Use |
|--------|------|--------|-----|
| `wendelon_quic_handshake_failures_total` | counter | `reason` (`transport`, `identity`, `authorization`) | QUIC handshakes that failed before an authenticated session came up |
| `wendelon_data_ring_write_requests_total` | counter | none | Data Ring writes handled |
| `wendelon_data_ring_read_requests_total` | counter | none | Data Ring reads handled |
| `wendelon_data_ring_query_requests_total` | counter | none | Query requests handled |
| `wendelon_data_ring_migrate_requests_total` | counter | none | Migration RPCs handled |
| `wendelon_data_ring_lease_refresh_passes_total` | counter | none | Lease-manager passes |
| `wendelon_data_ring_lease_grants_total` | counter | none | Lease grants accepted |
| `wendelon_data_ring_lease_denials_total` | counter | none | Lease refresh denials |
| `wendelon_data_ring_lease_errors_total` | counter | none | Lease refresh failures |
| `wendelon_data_ring_lease_cache_entries` | gauge | none | Cached lease descriptors |
| `wendelon_network_compression_batches_total` | counter | `codec`, `path`, `result` | Fresh wire-side Arrow IPC encode attempts by codec, bounded path, and result |
| `wendelon_network_compression_encoded_bytes_total` | counter | `codec`, `path` | Encoded Arrow IPC bytes emitted by fresh wire encoders |
| `wendelon_network_compression_encode_micros` | histogram | `codec`, `path` | Fresh wire-side Arrow IPC encode duration in microseconds |
| `wendelon_network_compression_ratio` | histogram | `codec`, `path` | Encoded/plaintext byte ratio when available without production double-encoding |
| `wendelon_network_compression_decode_error_total` | counter | `codec` | Wire-side Arrow IPC decode failures by codec; reason text stays in logs/spans |

#### Change Data Capture
| Metric | Type | Labels | Use |
|--------|------|--------|-----|
| `wendelon_cdc_subscriptions_total` | counter | `result` (`opened`, `closed`, `rejected`, `cancelled`) | CDC stream lifecycle outcomes |
| `wendelon_cdc_events_emitted_total` | counter | `source` (`ring`, `catchup`) | CDC storage-version events emitted by source path |
| `wendelon_cdc_event_bytes_emitted_total` | counter | `source` (`ring`, `catchup`) | Encoded CDC frame bytes emitted by source path |
| `wendelon_cdc_checkpoints_emitted_total` | counter | `source` (`ring`, `catchup`) | Visible-state digest checkpoints emitted by source path |
| `wendelon_cdc_rejects_total` | counter | `code` (`below_retention_floor`, `ring_overflow_during_catchup`, `schema_fingerprint_incompatible`, `checkpoint_digest_mismatch`, `unknown_table`, `tenant_auth_denied`, `table_not_replicated_here`, `internal_engine_error`) | Terminal typed CDC rejects |
| `wendelon_cdc_subscription_lag_micros` | histogram | `source` (`ring`, `catchup`) | Emit lag from finalized/catchup source to wire frame |
| `wendelon_cdc_ring_overflow_total` | counter | none | Live-tail ring evictions by capacity pressure |
| `wendelon_cdc_catchup_to_live_handoff_micros` | histogram | none | Subscribe-open catchup duration before live-tail handoff |
| `wendelon_cdc_staged_entries_in_ring` | gauge | none | Current staged/live CDC entries resident in the in-memory ring |
| `wendelon_cdc_pending_finalization_evicted_total` | counter | none | Staged CDC entries evicted or discarded before finalization release |

CDC metrics deliberately do not label tenant, table, subscription id, peer,
schema fingerprint, cursor, or free-form error strings. Use CDC audit events
and traces for attribution; keep Prometheus bounded to the enums above.

#### Storage / L2
| Metric | Type | Labels | Use |
|--------|------|--------|-----|
| `wendelon_group_commit_window_seconds` | histogram | none | Group-commit coalescing latency before `fdatasync` |
| `wendelon_l1_read_latency_seconds` | histogram | none | Storage-side latency for L1-served reads |
| `wendelon_l2_range_read_latency_seconds` | histogram | none | Storage-side latency for reads served from tiered L2 slabs |
| `wendelon_l2_upload_latency_seconds` | histogram | none | Time to upload a tiered slab object to L2 |
| `wendelon_storage_tiering_passes_total` | counter | none | Tiering sweeper passes |
| `wendelon_storage_tiering_pass_errors_total` | counter | none | Sweeper passes that returned an error |
| `wendelon_storage_tiered_slabs_total` | counter | none | Slabs tiered from L1 into L2 |
| `wendelon_storage_tiering_failures_total` | counter | none | Per-slab tiering failures |
| `wendelon_storage_tiering_skipped_small_total` | counter | none | Slabs skipped for being below the size threshold |
| `wendelon_storage_tiering_skipped_already_tiered_total` | counter | none | Slabs skipped because the state machine had already pivoted them |
| `wendelon_storage_rehydration_guard_skips_total` | counter | none | Rehydrated slabs retained in L1 by the thrash guard |
| `wendelon_storage_rehydrated_reclaims_total` | counter | none | Rehydrated slabs reclaimed after cooling |
| `wendelon_storage_migration_reclaimed_slabs_total` | counter | none | Tombstoned slabs reclaimed after migration |
| `wendelon_storage_migration_reclaimed_bytes_total` | counter | none | Bytes reclaimed after migration |
| `wendelon_storage_l2_row_group_fetch_requests_total` | counter | `strategy` (`row_group`, `columnar_buffer`, `whole_record`) | ADR 056 row-group projected-read strategy selection |
| `wendelon_storage_l2_row_group_fetch_fallback_total` | counter | `reason` (`disabled`, `single_row`, `full_projection`, `small_record`, `too_few_rows`, `too_many_row_groups`, `too_many_bytes`, `too_many_ranges`, `metadata_probe_cap`, `unsupported_arrow_layout`, `rehydration_available`, `unpaged_record`) | ADR 056 economic fallback distribution |
| `wendelon_storage_l2_row_group_fetch_row_groups_total` | counter | `decision` (`skip`, `keep`, `predicate_fetch`, `no_survivors`) | Row-group pruning decisions |
| `wendelon_storage_l2_row_group_fetch_bytes_total` | counter | `kind` (`directory`, `phase_b`, `phase_c`, `avoided`) | Row-group fetch bytes and savings accounting |
| `wendelon_storage_l2_row_group_fetch_plan_micros` | histogram | none | ADR 056 planning time |
| `wendelon_storage_l2_row_group_fetch_selected_ranges` | histogram | none | Encrypted chunk-run count selected by row-group fetch |
| `wendelon_storage_l2_row_group_fetch_directory_bytes` | histogram | none | Parsed row-group directory size |
| `wendelon_storage_l2_row_group_fetch_invariant_violations_total` | counter | `violation_kind` (`page_directory_parse_error`, `row_group_range_overflow`, `layout_unsupported`) | Fail-closed ADR 056 corruption/invariant signal |
| `wendelon_storage_l2_encoding_buffer_encodings_total` | counter | `cascade` (`plain`, `lz4_frame`, `bit_packed`, `rle`, `dictionary`, `delta`, `fsst`, `alp`) | ADR 057 cascade distribution for encoded row-group buffers |
| `wendelon_storage_l2_encoding_materialize_micros` | histogram | `cascade` (`plain`, `lz4_frame`, `bit_packed`, `rle`, `dictionary`, `delta`, `fsst`, `alp`) | Per-cascade Encoding materialization CPU |
| `wendelon_storage_l2_encoding_simd_pages_fetched_total` | counter | `cascade` (`plain`, `lz4_frame`, `bit_packed`, `rle`, `dictionary`, `delta`, `fsst`, `alp`) | SIMD-page fetch count by cascade |
| `wendelon_storage_l2_encoding_compression_ratio` | histogram | `cascade` (`plain`, `lz4_frame`, `bit_packed`, `rle`, `dictionary`, `delta`, `fsst`, `alp`) | Encoded/decoded Encoding byte ratio |
| `wendelon_storage_l2_encoding_cascade_select_micros` | histogram | none | Writer-side cascade selection time |
| `wendelon_storage_l2_encoding_directory_bytes` | histogram | none | Parsed/written Encoding directory metadata bytes |
| `wendelon_storage_l2_encoding_fallback_total` | counter | `reason` (`reader_disabled`, `directory_missing`, `unsupported_arrow_layout`, `too_many_pages`, `too_many_bytes`, `dense_selection`, `plain_buffer`) | Encoding economic fallback distribution |
| `wendelon_storage_l2_encoding_bytes_total` | counter | `kind` (`page`, `buffer`, `avoided`) | Encoding byte accounting |
| `wendelon_storage_l2_encoding_invariant_violations_total` | counter | `violation_kind` (`encoding_directory_parse`, `buffer_encoding_mismatch`, `page_range_overflow`, `cascade_unsupported`, `decoded_length_mismatch`) | Fail-closed ADR 057 corruption/invariant signal |

#### Descriptor Fingerprints / CSB
| Metric | Type | Labels | Use |
|--------|------|--------|-----|
| `wendelon_storage_csb_merge_logical_fingerprint_mismatch_total` | counter | `tenant`, `table` | CSB direct-answer or merge candidates rejected because logical schema fingerprints differed |
| `wendelon_storage_csb_payload_coverage_fall_through_total` | counter | `tenant`, `table`, `reason` | Logical-fingerprint matches that fell through because parsed stats payload coverage was insufficient |
| `wendelon_storage_csb_merge_pre_adr_051_cliff_total` | counter | `tenant`, `table` | CSB candidates whose table and generation matched but whose fingerprint slot did not match the ADR 051 logical fingerprint |
| `wendelon_storage_aggregate_cache_logical_fingerprint_hit_total` | counter | `tenant`, `table` | Aggregate response cross-bind checks whose logical schema fingerprint matched the request |
| `wendelon_storage_aggregate_cache_logical_fingerprint_miss_total` | counter | `tenant`, `table` | Aggregate response cross-bind checks whose logical schema fingerprint differed from the request |
| `wendelon_storage_descriptor_recompute_seconds` | histogram | `tenant`, `table` | Descriptor logical and physical fingerprint recompute duration on schema-update submission |
| `wendelon_storage_descriptor_validate_seconds` | histogram | `tenant`, `table` | Descriptor validation duration on schema-update submission |
| `wendelon_storage_descriptor_logical_only_mutations_total` | counter | `tenant`, `table` | Accepted descriptor mutations where only the logical fingerprint changed. Expected zero by ADR 051 stacking; nonzero indicates an invariant breach or future preimage change |
| `wendelon_storage_descriptor_physical_only_mutations_total` | counter | `tenant`, `table` | Accepted descriptor mutations where only the physical descriptor fingerprint changed |
| `wendelon_storage_descriptor_combined_mutations_total` | counter | `tenant`, `table` | Accepted descriptor mutations where both descriptor fingerprints changed |

These ADR 051 diagnostics are table-scoped because the first operational
question is which descriptor axis or stats policy changed. Keep long-term
dashboards aggregated by job / cluster and use the table labels for
short-window rollout triage.

#### Table Retention
| Metric | Type | Labels | Use |
|--------|------|--------|-----|
| `wendelon_storage_table_retention_policy_updates_total` | counter | `policy` (`inherit_cluster`, `disabled`, `fixed_window`) | Accepted typed lifecycle mutations by target policy |
| `wendelon_storage_table_retention_read_floor_rejects_total` | counter | none | Reads rejected because `as_of_hlc` predates the effective table floor |
| `wendelon_storage_table_retention_read_filtered_rows_total` | counter | none | Rows hidden by the logical read-time floor before physical compaction catches up |
| `wendelon_storage_table_retention_l1_rows_pruned_total` | counter | `kind` (`data`, `tombstone`) | Rows pruned from L1 by table-aware retention compaction |
| `wendelon_storage_table_retention_l1_bytes_reclaimed_total` | counter | none | L1 bytes reclaimed by table-aware retention compaction |
| `wendelon_storage_table_retention_l2_rows_pruned_total` | counter | `kind` (`data`, `tombstone`) | Rows pruned from L2 by table-aware retention rewrites |
| `wendelon_storage_table_retention_l2_bytes_reclaimed_total` | counter | none | L2 bytes reclaimed by table-aware retention rewrites |
| `wendelon_storage_table_retention_pair_drops_total` | counter | none | Data/tombstone pairs jointly reclaimed after the table floor crosses `delete_hlc` |
| `wendelon_storage_table_retention_ambiguous_rows_retained_total` | counter | none | Rows retained because table ownership or lifecycle resolution was ambiguous |
| `wendelon_storage_table_retention_lag_seconds` | gauge | none | Maximum observed age of rows still physically present below an effective table floor |
| `wendelon_storage_table_retention_policy_active_tables_total` | gauge | `mode` (`inherit`, `disabled`, `fixed`) | Current lifecycle catalog entries by retention mode |

These metrics intentionally do not label by tenant or table. Use audit
events and traces for tenant/table attribution; Prometheus remains a
bounded-cardinality operational surface.

#### Crypto / Audit
| Metric | Type | Labels | Use |
|--------|------|--------|-----|
| `wendelon_tenant_kek_cache_hit_total` | counter | `outcome` (`hit`, `miss`) | Tenant KEK cache effectiveness |
| `wendelon_audit_materialization_lag_entries` | gauge | none | Per-node local-chain lag behind authoritative Raft events |
| `wendelon_audit_emit_total` | counter | `authority`, `outcome` | Audit emission volume by authority |
| `wendelon_audit_sink_failure_total` | counter | `sink`, `reason` | Audit sink failures by sink |
| `wendelon_crypto_decrypt_audit_emit_failures_total` | counter | none | `crypto.decrypt_rejected` audit events that failed to persist |

Per-tenant Prometheus labels remain explicitly out of scope for the pilot to keep backend cardinality finite.

---

## 4. Service Level Objectives (SLOs)

SLOs are commitments measured against the metric catalog above. The pilot ships with **targets** — numbers we intend the chaos and simulator harnesses (ADR 028) to validate before moving from `proposed` to `accepted`. Numbers marked `TARGET (unvalidated)` are aspirational until a soak run confirms them.

| SLO | Target | Metric basis | Measurement window | Status |
|-----|--------|--------------|---------------------|--------|
| Group-commit p99 | ≤ 10 ms | `wendelon_group_commit_window_seconds` p99 | 5m rolling | TARGET (unvalidated) |
| L1 read p99 (storage-side) | ≤ 10 ms | `wendelon_l1_read_latency_seconds` p99 | 5m rolling | TARGET (unvalidated) |
| L2 read p99 (storage-side) | ≤ 150 ms | `wendelon_l2_range_read_latency_seconds` p99 | 5m rolling | TARGET (unvalidated) |
| L2 upload p99 | ≤ 2 s | `wendelon_l2_upload_latency_seconds` p99 | 5m rolling | TARGET (unvalidated) |
| Audit materialization lag | ≤ 100 entries steady-state | `wendelon_audit_materialization_lag_entries` | instantaneous | TARGET (unvalidated) |

**Durability** is not an availability-style SLO; it is a design property of the replication + L2 model. The "11 nines" figure is a marketing-shaped number that depends on assumptions about object-store replication, scrub cadence, and correlated-failure rate that this project has not yet modeled. The pilot does not claim a specific durability figure; see `plans/ops/testing.md` and the chaos-harness soak results for what is actually validated.

Downgrading a target from TARGET to COMMITTED requires: (a) a weekly soak under `plans/ops/testing.md` coverage ladder that hits the target on at least 4 consecutive weeks; (b) a dashboard and alert rule that would fire before breach.

---

## 5. Alerts (seed set)

Operators run these out of the box. Each alert is named after the metric it fires on; all thresholds are tunable via cluster config.

### 5.1 Required recording rules

Histogram-derived quantiles are **not** precomputed series — `foo_seconds{p=0.99}` is shorthand that does not exist in Prometheus. The alert expressions below depend on a small set of recording rules that must be loaded in the Prometheus server. Ship these with the default alerting bundle:

```yaml
groups:
  - name: wendelon_recording_rules
    interval: 30s
    rules:
      - record: job:wendelon_group_commit_window_seconds:p99_5m
        expr: histogram_quantile(0.99, sum by (job, le) (rate(wendelon_group_commit_window_seconds_bucket[5m])))
      - record: job:wendelon_l1_read_latency_seconds:p99_5m
        expr: histogram_quantile(0.99, sum by (job, le) (rate(wendelon_l1_read_latency_seconds_bucket[5m])))
      - record: job:wendelon_l2_range_read_latency_seconds:p99_5m
        expr: histogram_quantile(0.99, sum by (job, le) (rate(wendelon_l2_range_read_latency_seconds_bucket[5m])))
      - record: job:wendelon_l2_upload_latency_seconds:p99_5m
        expr: histogram_quantile(0.99, sum by (job, le) (rate(wendelon_l2_upload_latency_seconds_bucket[5m])))
```

The `job:` naming prefix follows the Prometheus convention for aggregated-across-instances recording rules. SLO dashboards in §4 and alerts in §5.2 both reference these precomputed series.

### 5.2 Alert rules

| Alert | Condition | Severity | Notes |
|-------|-----------|----------|-------|
| `RaftNoLeader` | `absent(wendelon_raft_current_leader)` for 60s | page | Cluster unwritable |
| `GroupCommitWindowHigh` | `job:wendelon_group_commit_window_seconds:p99_5m > 0.01` for 10m | page | The local durable path is accumulating queueing latency |
| `L1ReadLatencyHigh` | `job:wendelon_l1_read_latency_seconds:p99_5m > 0.01` for 10m | warn | Local slab reads are above the target band |
| `L2RangeReadLatencyHigh` | `job:wendelon_l2_range_read_latency_seconds:p99_5m > 0.15` for 10m | warn | Tiered reads are above the target band |
| `L2UploadLatencyHigh` | `job:wendelon_l2_upload_latency_seconds:p99_5m > 2` for 10m | warn | The sweeper is taking too long to push slabs into L2 |
| `AuditMaterializationLagRising` | `wendelon_audit_materialization_lag_entries > 1000` for 5m | page | Local chain can't keep up with Raft; degraded `/ready` |
| `AuditSinkFailing` | `rate(wendelon_audit_sink_failure_total{sink="local"}[5m]) > 0` | page | Local-chain writes failing — NodeLocal events fail-closed |
| `TenantKekCacheThrashing` | `sum(rate(wendelon_tenant_kek_cache_hit_total{outcome="miss"}[5m])) / sum(rate(wendelon_tenant_kek_cache_hit_total[5m])) > 0.5` for 10m | warn | Cache undersized; KMS cost spike |
| `QuicHandshakeFailureSpike` | `sum(rate(wendelon_quic_handshake_failures_total[5m])) > 1` for 10m | warn | Per-second handshake failure rate is above the steady-state baseline |

`AuditMaterializationLagRising` and `AuditSinkFailing` are **required paging alerts** for compliance deployments (the audit fail-closed posture for NodeLocal events means these conditions can reject auth operations).

Operators running these alerts without the recording rules in §5.1 must substitute the raw `histogram_quantile(0.99, sum by (job, le) (rate(<metric>_bucket[5m])))` expression inline — the result is equivalent but harder to read in alert routing.

---

## 6. Runbook pointers

For each alert above, operators need a runbook. The current runbook catalog
lives at `plans/ops/runbooks/`.

Current high-signal mappings:

- `AuditMaterializationLagRising` and `AuditSinkFailing`
  - `plans/ops/runbooks/audit_sink_archive_distress.md`
- persisted finalization distress outside the default alert table
  - `plans/ops/runbooks/pending_finalization_backlog_distress.md`
- recovery workflows
  - `plans/ops/runbooks/cluster_restore.md`
  - `plans/ops/runbooks/node_replacement.md`
- ADR 043 storage-body rollout and stabilization watch
  - `plans/ops/runbooks/adr043_storage_body_stabilization.md`
- ADR 053 wire Arrow IPC compression rollout and decode-error triage
  - `plans/ops/runbooks/wire_arrow_ipc_body_compression.md`
- ADR 046 StateDigest substrate promotion and benchmark interpretation
  - `plans/ops/runbooks/state_digest_substrate.md`

Minimum runbook shape:

1. What the alert means in plain language.
2. First-look metrics and traces (links to specific dashboards/queries).
3. Diagnostic decision tree: "if X, check Y; if Y, check Z."
4. Mitigation actions, ranked by reversibility.
5. Escalation criteria.

---

## 7. FIPS-build visibility

The FIPS build profile (ADR 026) exposes these additional signals:

- `fips_module_self_test_total{outcome="ok"}` — counter of successful bootstrap-time FIPS self-test checks. Wendelon increments it during node bootstrap when it verifies the aws-lc-rs module is running in FIPS mode. A failed bootstrap self-test aborts startup before `/metrics` is available, so operators see that case via the startup error path rather than a scrape.
- `fips_module_version_info{version}` — gauge always `1`, with a version label sourced from the pinned aws-lc-rs build. (Low-cardinality: one label value per build.)
- `/ready` adds a `fips_mode` boolean field and a `fips_self_test_last_ts` field when built with the FIPS profile.
  `fips_self_test_last_ts` is a Unix-seconds timestamp for the successful bootstrap-time self-test check.

Non-FIPS builds do not emit these signals.

**Mixed-profile clusters are not a supported steady state.** Per ADR 026 Consequences, a cluster where some nodes run the FIPS build and others run the default build is an unsupported topology — crypto backends differ, and audit chain hashes are incompatible (blake3 vs. SHA-256). The supported operation is "every node on the same build profile."

The `fips_mode` field in `/ready` and the `fips_module_version_info` metric exist specifically to make the **unsupported states visible**, not to legitimize them:

- **During a profile upgrade** (e.g., migrating a cluster from `default` to `fips` by rolling-restarting each node): the operator can see, live, which nodes have converted and which have not. Once the roll completes, every node reports the same `fips_mode`; until then, the cluster is in the documented transient mixed state.
- **As a misconfiguration detector:** if `fips_mode` is not uniform across the cluster after an upgrade has supposedly completed, something went wrong — the mixed state is a bug, and the signal exists to surface it.

These signals are diagnostic, not a license to run a mixed fleet as a target state. Operators needing FIPS coverage run a fully-FIPS cluster; operators not needing it run a fully-default cluster. The upgrade window is bounded by `plans/ops/upgrades.md`.

The implemented metric and field names above are the operator contract for W10.

---

## 8. What is explicitly not observability

- **Audit.** Audit events are ADR 025's concern. The audit stream has different durability semantics (fail-closed for NodeLocal, materialization-lag for RaftLog) and different retention (≥ 90 days default). Sink-shape differences and archive guidance live in [Audit](./audit.md). Do not route audit events through the trace or metric pipelines.
- **Debug logs.** Debug logging is operator-mode-only and is not part of the telemetry SLA. `EnvFilter` controls it; `tracing` events at `debug!` and `trace!` are dropped in production builds unless explicitly enabled.
- **Per-tenant business analytics.** `/metrics` does not carry per-tenant labels. Per-tenant business metrics are downstream of this system, not inside it.

---

## References

- ADR 027: Observability Posture — OTLP, Tracing Stack, Health/Metrics Split
- ADR 025: Audit Event Model and Tamper-Evident Hash Chain (separate stream)
- `plans/ops/audit.md` — operator-facing audit sink shapes and archive guidance
- ADR 026: FIPS 140-3 Certification Target (FIPS-build visibility additions)
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization (role-gating on `/ready` and `/metrics`)
- ADR 028: Deterministic Simulator and `AsyncFileOps` Abstraction (SLO validation harness)
- `plans/ops/testing.md` — coverage ladder and soak schedule that validate SLO targets
- `plans/ops/runbooks/` — per-alert and recovery runbook catalog
