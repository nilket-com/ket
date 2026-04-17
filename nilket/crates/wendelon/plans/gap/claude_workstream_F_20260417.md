# Workstream F — Observability and Operations

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_execution_20260417.md` (Phase 6)
**Status:** First pass
**Subsystems:** `src/metrics/`, `src/main.rs`, new `src/cli/` module
**Depends on:** Workstream A (integration gate, tracing subscriber)

---

## Problem Statement

The system's operational surface is insufficient for enterprise SRE teams:

1. **No distributed tracing.** `tracing` crate is used pervasively but
   there is no OTLP/Jaeger/Tempo exporter. An SRE cannot follow a write
   across nodes.
2. **Metrics are basic.** Only counters and gauges — no latency
   histograms, no dimensional labels. The exporter is a bare TCP listener
   with no auth (hardening is in Workstream E).
3. **No operator CLI.** Backup, restore, cluster-status, transfer-leader,
   and drain exist only as library calls. An SRE has to write Rust.
4. **No health/readiness endpoint.** No k8s probes, no systemd notify.
5. **openraft defaults are untuned.** Election, heartbeat, snapshot, and
   compaction intervals are all at openraft defaults with no documented
   rationale.

---

## Code Ownership / Subsystem Boundaries

| Area | Primary Files | Notes |
|------|--------------|-------|
| Distributed tracing | `src/main.rs`, new tracing layer config | OTLP exporter |
| Metrics | `src/metrics/registry.rs`, `src/metrics/exporter.rs` | Histograms, labels |
| Operator CLI | New `src/cli/` or `src/bin/wendelon-ctl.rs` | Subcommands |
| Health endpoint | `src/network/server.rs` or `src/main.rs` | HTTP health + readiness |
| Container integration | `src/main.rs` | systemd notify, preStop semantics |
| openraft tuning | `src/node.rs` | Config with rationale |

---

## Ordered Implementation Steps

### F.1 — Distributed Tracing

| # | Step | Target | Details |
|---|------|--------|---------|
| F.1.1 | Add `tracing-opentelemetry` + OTLP exporter | `Cargo.toml`, `src/main.rs` | Configure via env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`). Default off. |
| F.1.2 | Propagate trace context across QUIC RPCs | `src/network/core.rs` | Add trace-id/span-id to HRFT frame header (or as a metadata field). Extract on receive and attach to the server-side span. |
| F.1.3 | Instrument key code paths | Various | Add `#[instrument]` or manual spans to: write path (leader append + fan-out + quorum wait), read path (slab lookup + optional L2 fetch), control RPCs (join, lease, transfer). |
| F.1.4 | Test: trace spans appear in collector | Integration | Start a mock OTLP collector, do a write, verify spans for leader + replicas are received with correct parent-child relationships. |

### F.2 — Metrics Enrichment

| # | Step | Target | Details |
|---|------|--------|---------|
| F.2.1 | Add latency histograms | `src/metrics/registry.rs` | Write latency (client → quorum ack), read latency (client → response), L2 tier latency, L2 rehydration latency. Use meaningful bucket boundaries. |
| F.2.2 | Add dimensional labels | `src/metrics/registry.rs` | Label by operation type, slab tier (L1/L2), success/failure. Avoid high-cardinality labels (no per-key labels). |
| F.2.3 | OpenMetrics format support | `src/metrics/exporter.rs` | Accept `Accept: application/openmetrics-text` and respond accordingly. |
| F.2.4 | Optional remote-write / push-gateway | `src/metrics/` | For environments where scrape is not possible. Behind config flag. |

### F.3 — Operator CLI

| # | Step | Target | Details |
|---|------|--------|---------|
| F.3.1 | CLI framework scaffolding | `src/bin/wendelon-ctl.rs` or `src/cli/` | Use `clap` for subcommand parsing. Connect to a node's control port via QUIC + mTLS. |
| F.3.2 | `wendelon cluster-status` | CLI | Display: current epoch, leader node, membership list, node health, protocol version. |
| F.3.3 | `wendelon transfer-leader <target>` | CLI | Invoke `TRANSFER_REQ` on the current leader. |
| F.3.4 | `wendelon drain <node>` | CLI | Invoke `begin_drain` on the target node. Wait for quiescence. |
| F.3.5 | `wendelon backup <output-dir>` | CLI | Invoke backup on the target node, stream the result to local disk. |
| F.3.6 | `wendelon restore <backup-dir>` | CLI | Restore from a backup into a fresh node data directory. |
| F.3.7 | CLI auth via operator cert | CLI | Require mTLS with an operator-role cert (ties into Workstream E). |

### F.4 — Health and Container Integration

| # | Step | Target | Details |
|---|------|--------|---------|
| F.4.1 | Health endpoint (`/health`, `/ready`) | `src/main.rs` or `src/network/server.rs` | `/health`: node is alive. `/ready`: node has joined cluster and can serve reads. Separate HTTP listener on a configurable port. |
| F.4.2 | systemd notify integration | `src/main.rs` | Call `sd_notify(READY=1)` after successful bootstrap. `STOPPING=1` on drain start. |
| F.4.3 | k8s preStop hook semantics | `src/main.rs` | On SIGTERM, begin drain, wait for quiescence, then exit. Document the recommended k8s `terminationGracePeriodSeconds`. |
| F.4.4 | Liveness probe documentation | Docs | Recommend k8s config: liveness → `/health`, readiness → `/ready`, startup probe with generous timeout for recovery. |

### F.5 — openraft Tuning

| # | Step | Target | Details |
|---|------|--------|---------|
| F.5.1 | Review openraft config knobs | Research | Document what each knob does, default value, and recommended value for a storage-fabric workload. |
| F.5.2 | Set explicit defaults with rationale | `src/node.rs:477–490` | Election timeout, heartbeat interval, snapshot policy (log count threshold), max entries per append, purge policy. |
| F.5.3 | Expose tunable subset via TOML | `src/config.rs` | Allow operators to override the defaults for their deployment. |

---

## Test Plan

| Test | Type | What it proves |
|------|------|----------------|
| Trace spans across nodes | Integration | OTLP export + context propagation work |
| Histogram buckets populated | Unit | Latency histograms record observations |
| `cluster-status` returns correct info | Integration | CLI can connect and display live state |
| Health endpoint returns 200 | Integration | Probe endpoint works |
| Readiness flips after join | Integration | `/ready` is false before join, true after |

---

## Rollout and Migration Concerns

- **OTLP dependency.** Adding `tracing-opentelemetry` and `opentelemetry-otlp`
  adds non-trivial dependencies. Ensure they're behind a feature flag if
  binary size is a concern.
- **Metrics format change.** Adding histograms and labels changes the
  `/metrics` output format. Existing Prometheus scrape configs and
  dashboards may need updating. Document the new metric names.
- **CLI binary.** A separate `wendelon-ctl` binary means a second build
  artifact. Alternatively, make it a subcommand of the main binary
  (`wendelon ctl cluster-status`). Decide based on deployment preference.
- **Health port.** A separate HTTP listener for health probes means
  another port to configure and expose. Document the default port and
  ensure it doesn't conflict with the metrics port.

---

## Failure Modes and Rollback

- **OTLP collector unavailable.** The tracing exporter must degrade
  gracefully — drop spans, don't block the hot path. `opentelemetry`
  exporters are async and have batch processors with bounded queues.
  Verify that a dead collector doesn't cause backpressure into the
  write path.
- **Histogram cardinality explosion.** If labels are too fine-grained,
  Prometheus scrapes become expensive. Cap label cardinality and document
  which labels are safe to add.
- **CLI auth failure.** If the operator's cert is expired or not trusted,
  the CLI should give a clear error, not a cryptic TLS handshake failure.

---

## Exit Criteria

- A write can be traced across leader + 2 replicas in Jaeger/Tempo.
- `wendelon cluster-status` returns node health, epoch, and leader.
- k8s deployment with rolling upgrade succeeds (probes work, drain
  completes before pod termination).
- openraft config has explicit values with documented rationale.

---

## Open Questions

1. **OTLP vs. Jaeger-native?** OTLP is the standard, but some shops
   still use Jaeger-native protocol. Support OTLP only and let the
   collector translate?
2. **CLI as separate binary or subcommand?** Separate binary is cleaner
   for deployment (operator doesn't need the full daemon). Subcommand is
   simpler to build and distribute.
3. **Health endpoint security.** Should the health/readiness endpoint
   require auth? k8s probes typically hit an unauthenticated endpoint.
   But exposing an unauthenticated HTTP listener is a (minor) attack
   surface.
4. **Metrics port vs. health port.** Same listener or different? Same is
   simpler but means health probes hit the same endpoint as Prometheus
   scrapes, which may have different auth requirements.
5. **openraft snapshot policy.** Should snapshots trigger on log count,
   time interval, or both? What's the right log count for a storage
   fabric where each Raft entry is a control-plane operation (not a data
   write)?
