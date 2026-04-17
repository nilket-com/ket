# Workstream G: Operations & Observability
**Date:** 2026-04-17
**Author:** Gemini CLI

## Goal
Make the product operable, observable, and maintainable by SREs for a pilot deployment.

## Scope & Tasks

### 1. Observability
*   **Target Files:** `src/metrics/`, `src/main.rs`, `Cargo.toml`
*   **Tasks:**
    *   Add distributed tracing via OpenTelemetry (OTLP exporter).
    *   Implement span propagation across QUIC RPCs so a single request can be traced across the leader and replicas.
    *   Add latency histograms and safe dimensional labels to the metrics registry.
    *   Implement OpenMetrics / remote-write export options.

### 2. Operator Tooling & Lifecycle
*   **Target Files:** `src/main.rs`, `src/bin/wendelon_cli.rs` (new)
*   **Tasks:**
    *   Build an operator CLI with commands: `wendelon backup`, `restore`, `cluster-status`, `transfer-leader`, `drain`.
    *   Add k8s health/readiness endpoints (liveness probes).
    *   Integrate systemd notify / container lifecycle hooks for graceful shutdown.
    *   Tune and document default `openraft` configurations (election timeouts, heartbeat intervals).

## Exit Criteria
*   Distributed requests can be traced in tools like Jaeger/Tempo.
*   Operator CLI can report cluster health and execute administrative actions.
*   Kubernetes deployments with rolling upgrades succeed seamlessly.

---
## Open Questions
*   Should the CLI be bundled into the main binary (e.g., `wendelon admin cluster-status`) or shipped as a separate standalone binary?