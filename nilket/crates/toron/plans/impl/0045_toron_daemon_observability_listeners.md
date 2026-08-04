# Implementation Plan: ADR 045 Toron Daemon Observability Listeners

**Status:** Promoted (paired with `plans/adr/0045_toron_daemon_observability_listeners.md`)

## Implementation Thesis

Compose three HTTP/1.1 observability listeners over ADR 044's TLS
policy substrate. Add `crates/toron/src/network/observability_http.rs`
with hand-rolled HTTP/1.1 helpers (no hyper / axum / actix). Add
`crates/toron/src/network/observability.rs` with the listener
`spawn` functions, `ReadinessSnapshot` shape, and the metric
registry surface. Extend `crates/toron/src/config.rs` with
`[observability.listeners]` plus port-collision validation. Extend
`crates/toron/src/network/tls.rs::admit_san` to accept the new
`monitor-` SAN prefix. Extend `crates/toron/src/daemon.rs` shutdown
to flip `ReadinessSnapshot.degraded_reason` to `"draining"` on
SIGTERM. Wrap every `read_request` call site in a 5-second
`tokio::time::timeout` for slowloris hardening. Preserve every ADR
042 / 043 / 044 invariant byte-identical when `[observability]` is
absent. Keep metric and newly introduced type names brand-clean.

## Phase 0: Audit

Resolve the following before Phase 1; capture in
`plans/impl/0045_toron_daemon_observability_listeners_evidence.md`.

1. **HTTP helper placement (Toron-local vs shared `tls-pki`).** ADR
   044 chose Plan W-C (Toron-local TLS module). Consistency favors
   Toron-local HTTP helpers at
   `crates/toron/src/network/observability_http.rs`. If a near-term
   shared-crate extraction ADR is scheduled, this audit decides
   whether to land the HTTP helpers there first; default to local
   under Plan W-C inertia.

2. **`tokio_rustls` workspace pin.** Toron's existing TLS code uses
   rustls 0.23 + quinn 0.11 (per ADR 044 Phase 0 evidence). Confirm
   `tokio_rustls` is in the dep graph at a compatible version; mirror
   Wendelon's pin
   (`crates/wendelon/src/metrics/observability.rs:21`).

3. **Wendelon HTTP helper shape audit.** Read Wendelon's
   `read_request` / `write_response` / `parse_request_line` helpers.
   Function signatures and behavior are the parity target. Note any
   helper that depends on Wendelon-private types (e.g., audit
   parameters) and strip those for Toron's copy.

4. **Bind-port collision shape.** Read
   `crates/toron/src/config.rs` validation for ADR 042 + 043 + 044
   bind addresses. Identify the validation seam where the five
   `SocketAddr` values can be collected for pairwise-distinct check
   (D1 sharpening).

5. **`ToronDaemon::shutdown()` drain-state seam.** Read ADR 042 D11
   shutdown semantics. Identify the seam where `degraded_reason` can
   be set to `Some("draining")` atomically before the listener-task
   abort loop begins (D6 drain-state sharpening).

6. **Metric counter wiring seams.** Identify the call sites in
   `daemon.rs` + `coordinator.rs` + `network/tls.rs` where the V1
   metric families are incremented / observed. Likely seam shape: a
   shared `Arc<MetricsRegistry>` constructed at daemon startup and
   threaded into the listener accept loops + `admit_san` rejection
   paths.

7. **Drain-state interior mutability shape.** `ReadinessState`
   must expose a `set_degraded_reason` mutator. Choose between
   `RwLock<Option<String>>` and `arc-swap::ArcSwapOption<String>`.
   Phase 0 decides; both work for V1 (lock-free under arc-swap is
   nice-to-have but not load-bearing).

## Phase 1: Dependencies and Module Skeleton

Update `crates/toron/Cargo.toml` only if missing:

- `tokio_rustls` (workspace pin matching Wendelon's).
- Reuse existing `tokio`, `tracing`, `serde`, `serde_json`, `uuid`,
  `rustls`, `rustls-pki-types` deps.

**Do not** add `hyper`, `http`, `axum`, `actix-web`, or any HTTP
framework. Guard 5 enforces.

**Do not** touch `crates/nilket/Cargo.toml`. ADR 040 D4
root-runtime-free invariant preserved.

Add module exports in `crates/toron/src/network/mod.rs`:

```rust
pub mod tls;             // existing (ADR 044)
pub mod observability;   // new in ADR 045
pub mod observability_http;  // new in ADR 045 (helpers)
```

## Phase 2: `observability_http.rs` — Hand-Rolled HTTP/1.1 Helpers

Mirror Wendelon's helper shape at the function-signature level.

```rust
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum time to wait for an HTTP request-line + headers from a
/// connected client. Prevents slowloris-style DoS where a client
/// holds connections open indefinitely without sending complete
/// requests. 5 seconds accommodates kube-probe and Prometheus scrape
/// RTTs with margin. Tighter timeouts risk cutting legitimate slow
/// clients on degraded networks; looser timeouts widen the attack
/// window.
pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Read an HTTP/1.1 request up to and including the empty CRLF that
/// terminates the request-line + headers. Returns the raw bytes.
/// Caps headers at 8 KiB; returns `ErrorKind::InvalidData` on
/// overflow.
pub async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Vec<u8>>;

/// Parse the request line into `(method, path)`. Returns
/// `ErrorKind::InvalidData` on malformed input.
pub fn parse_request_line(raw: &[u8]) -> std::io::Result<(&str, &str)>;

/// Write a minimal HTTP/1.1 response: status line, content-type,
/// content-length, connection-close, blank line, body. Closes the
/// connection after.
pub async fn write_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    status: &str,         // e.g. "200 OK" or "503 Service Unavailable"
    content_type: &str,
    body: &str,
) -> std::io::Result<()>;
```

**Behavior:**

- `read_request` reads up to 8 KiB of header data; returns
  `ErrorKind::InvalidData` on overflow.
- `parse_request_line` returns `(method, path)` slices into the raw
  buffer. Method validation is up to the caller (D4: only GET).
- `write_response` writes status line, three response headers
  (Content-Type, Content-Length, `Connection: close`), CRLF, body,
  then flushes.

**No** HTTP/1.1 keep-alive, **no** chunked encoding, **no** request
body handling, **no** request validation beyond well-formedness.

## Phase 3: `observability.rs` — Listener Spawn + Readiness + Metrics

Define the listener functions, snapshot type, and metric registry.

```rust
pub const HEALTH_PATH: &str = "/health";
pub const READY_PATH: &str = "/ready";
pub const METRICS_PATH: &str = "/metrics";

pub struct ObservabilityListeners {
    pub health: SocketAddr,
    pub ready: SocketAddr,
    pub metrics: SocketAddr,
}

pub struct ObservabilityHandles {
    pub health: ListenerHandle,
    pub ready: ListenerHandle,
    pub metrics: ListenerHandle,
}

pub struct ListenerHandle {
    pub local_addr: SocketAddr,
    pub task: JoinHandle<()>,
}

#[derive(Clone)]
pub struct ReadinessSnapshot {
    pub ready: bool,
    pub worker_listener_bound: bool,
    pub sql_listener_bound: bool,
    pub observability_listeners_bound: bool,
    pub descriptors_loaded: u64,
    pub static_workers_configured: u32,
    pub uptime_seconds: u64,
    pub degraded_reason: Option<String>,
}

impl ReadinessSnapshot {
    pub fn is_ready(&self) -> bool {
        self.ready
            && self.worker_listener_bound
            && self.sql_listener_bound
            && self.observability_listeners_bound
            && self.degraded_reason.is_none()
    }
    pub fn http_status(&self) -> &'static str {
        if self.is_ready() { "200 OK" } else { "503 Service Unavailable" }
    }
    pub fn to_json(&self) -> String {
        // Emit a stable object in the ADR-pinned field order. Do not
        // rely on serde_json::Map iteration order unless the ordered
        // map feature is deliberately enabled.
        let degraded_reason = self
            .degraded_reason
            .as_ref()
            .map(|reason| serde_json::to_string(reason).expect("serialize degraded reason"))
            .unwrap_or_else(|| "null".to_string());
        format!(
            "{{\"ready\":{},\"worker_listener_bound\":{},\"sql_listener_bound\":{},\"observability_listeners_bound\":{},\"descriptors_loaded\":{},\"static_workers_configured\":{},\"uptime_seconds\":{},\"degraded_reason\":{}}}",
            self.ready,
            self.worker_listener_bound,
            self.sql_listener_bound,
            self.observability_listeners_bound,
            self.descriptors_loaded,
            self.static_workers_configured,
            self.uptime_seconds,
            degraded_reason
        )
    }
}
```

Do not include `worker_id`, `tenant_id`, endpoints, SQL text, query
IDs, task IDs, table names, certificate subjects, or UUIDs in the V1
readiness JSON.

```rust
pub struct ReadinessState { ... }

impl ReadinessState {
    pub async fn snapshot(&self) -> ReadinessSnapshot;
    pub async fn set_degraded_reason(&self, reason: Option<String>);
}
```

`/ready` and `/metrics` require a TCP TLS acceptor; `/health` is the
only plaintext path:

```rust
pub async fn spawn(
    addresses: ObservabilityListeners,
    tls_acceptor: TlsAcceptor,
    readiness: Arc<ReadinessState>,
    metrics: Arc<MetricsRegistry>,
    cluster_domain: String,
) -> std::io::Result<ObservabilityHandles> { /* ... */ }
```

Each handler wraps `observability_http::read_request` in
`tokio::time::timeout(READ_REQUEST_TIMEOUT, ...)`:

```rust
async fn handle_health_connection(mut stream: TcpStream) -> std::io::Result<()> {
    let req = match timeout(observability_http::READ_REQUEST_TIMEOUT,
                            observability_http::read_request(&mut stream)).await {
        Ok(Ok(req)) => req,
        Ok(Err(_)) => return Ok(()),
        Err(_) => return Ok(()),  // slowloris -> close without response
    };
    match observability_http::parse_request_line(&req)? {
        ("GET", HEALTH_PATH) => {
            observability_http::write_response(
                &mut stream, "200 OK", "text/plain; charset=utf-8", "ok\n"
            ).await
        }
        (method, HEALTH_PATH) if method != "GET" => {
            observability_http::write_response(
                &mut stream, "405 Method Not Allowed",
                "text/plain; charset=utf-8",
                "method not allowed\n",
            ).await
        }
        _ => {
            observability_http::write_response(
                &mut stream, "404 Not Found", "text/plain; charset=utf-8", "not found\n"
            ).await
        }
    }
}
```

`/ready` and `/metrics` handlers follow the same timeout and
method/path pattern with TLS acceptance and `monitor-` SAN admission
before the request-read step.

The metric registry is a flat struct of `AtomicU64` counters and
gauges, one field per honest V1 metric family. Render to Prometheus
0.0.4 text in `MetricsRegistry::render_text()` with functional,
brand-clean names.

```rust
pub struct MetricsRegistry {
    pub daemon_started_at: std::time::Instant,
    pub listener_accepts: ListenerCounters,
    pub listener_rejects: ListenerRejectCounters,
    pub metrics_scrapes_total: AtomicU64,
    pub descriptors_loaded: AtomicU64,
    pub static_workers_configured: AtomicU64,
}

pub struct ListenerCounters {
    pub worker: AtomicU64,
    pub sql: AtomicU64,
    pub health: AtomicU64,
    pub ready: AtomicU64,
    pub metrics: AtomicU64,
}

pub struct ListenerRejectCounters {
    pub mtls_handshake_failure: AtomicU64,
    pub peer_unknown_ca: AtomicU64,
    pub san_shape_rejected: AtomicU64,
    pub san_role_rejected: AtomicU64,
    pub bind_unavailable: AtomicU64,
    pub request_timeout: AtomicU64,
}
```

Metric names rendered by `MetricsRegistry`:

```text
daemon_ready
daemon_uptime_seconds
listener_up{listener="worker"}
listener_up{listener="sql"}
listener_up{listener="health"}
listener_up{listener="ready"}
listener_up{listener="metrics"}
listener_accepts_total{listener="sql"}
listener_rejects_total{listener="metrics",reason="san_role_rejected"}
metrics_scrapes_total
descriptors_loaded
static_workers_configured
```

Only add `queries_in_flight` or `tasks_in_flight` if they are wired
around real execution/task seams. Do not ship placeholder zero gauges.

## Phase 4: `[observability.listeners]` Config Section

Extend the existing observability config. Because ADR 045 touches this
surface, also apply the small brand-clean rename pass:
`ToronObservabilityConfig` -> `ObservabilityConfig`,
`ToronTracingFormat` -> `TracingFormat`, and
`RawToronObservabilityConfig` -> `RawObservabilityConfig`.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToronBootstrapConfig {
    // existing fields...
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub level: String,
    pub format: TracingFormat,
    pub listeners: Option<ObservabilityListeners>,
}

struct RawObservabilityConfig {
    level: String,
    format: String,
    listeners: Option<RawObservabilityListeners>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityListeners {
    pub health: SocketAddr,
    pub ready: SocketAddr,
    pub metrics: SocketAddr,
}

struct RawObservabilityListeners {
    health: Option<String>,
    ready: Option<String>,
    metrics: Option<String>,
}
```

Add validation in `validate_bootstrap_config`:

1. If `[observability.listeners]` is present, the three bind addresses
   must all be present.
2. `[observability.listeners].health` must resolve to a loopback
   address. Reject with stable diagnostic
   `observability health listener must bind a loopback address`.
3. If `[observability.listeners]` is present and `[tls]` is absent,
   reject with stable diagnostic
   `observability ready and metrics listeners require authenticated transport`.
   Ready and metrics require the shared ADR 044 TLS material for both
   loopback and non-loopback binds.
4. **Port-collision check:** collect all `SocketAddr`s from
   `[worker]`, `[sql]` (if present), `[observability.listeners]` and
   verify pairwise distinctness. Reject with stable diagnostic
   `listener bind addresses must be pairwise distinct` on collision.

**Example-config comment block** (rendered by
`render_example_config()`):

```toml
# Observability listeners (ADR 045). Three separate bind addresses
# for /health, /ready, /metrics. Missing [observability.listeners] preserves
# ADR 042/043/044 behavior byte-identical.
#
# /health is always plaintext + loopback-only (Kubernetes liveness
# probe shape).
# /ready and /metrics require ADR 044 TLS material and admit
# scrapers whose client cert SAN matches:
#     monitor-<principal>.<cluster_domain>
[observability.listeners]
health = "127.0.0.1:7780"
ready  = "127.0.0.1:7781"
metrics = "127.0.0.1:7782"
```

## Phase 5: `admit_san` Extension for `monitor-` Prefix

Extend `crates/toron/src/network/tls.rs::admit_san`:

```rust
match (prefix, role) {
    ("toron", ListenerRole::WorkerTask) => { /* existing */ }
    ("toron", ListenerRole::Sql) => Err(TlsError::SanRoleRejected),  // ADR 044 D5 tightening
    ("client", ListenerRole::Sql) => { /* existing */ }
    ("monitor", ListenerRole::Observability) => {
        Ok(PeerKind::Monitor { principal: id_token.to_owned() })
    }
    _ => Err(TlsError::SanShapeRejected),
}
```

`ListenerRole` enum extends:

```rust
pub enum ListenerRole {
    WorkerTask,
    Sql,
    Observability,  // new in ADR 045
}
```

`/health` does no SAN admission (plaintext path); no new role
needed.

`PeerKind::Monitor { principal: String }` is a new variant for
telemetry / logging.

## Phase 6: Daemon Wiring — Listener Spawn + Drain-State Hook

In `crates/toron/src/daemon.rs`:

```rust
// After ADR 044 worker task + SQL listeners bind, before READY/SQL_READY emission:

let observability_handles = match &config.observability.listeners {
    Some(listeners) => {
        let tls_acceptor = TlsAcceptor::from(
            tls_policy.rustls_server_config_for(ListenerRole::Observability),
        );
        Some(observability::spawn(
            listeners.clone(),
            tls_acceptor,
            reporter.clone(),
            metrics_registry.clone(),
            cluster_domain.clone(),
        ).await?)
    }
    None => None,
};

// Emit existing READY / SQL_READY lines (ADR 042 D12 / ADR 043 D16
// invariants preserved byte-identical).
println!("READY {}", worker_endpoint);
if let Some(sql_addr) = sql_endpoint {
    println!("SQL_READY {}", sql_addr);
}

// Append OBSERVABILITY_READY (D8 pinned format):
if let Some(handles) = &observability_handles {
    println!(
        "OBSERVABILITY_READY health={} ready={} metrics={}",
        handles.health.local_addr,
        handles.ready.local_addr,
        handles.metrics.local_addr
    );
}
```

**Drain-state hook (D6):** in `ToronDaemon::shutdown()`, flip
`reporter`'s `degraded_reason` to `Some("draining")` **before** the
listener-task abort loop begins:

```rust
pub async fn shutdown(self) -> Result<(), ToronDaemonError> {
    self.reporter.set_degraded_reason(Some("draining".to_owned()));
    // ...existing ADR 042 D11 drain logic...
}
```

The `set_degraded_reason` mutator uses `RwLock<Option<String>>` or
`arc-swap::ArcSwapOption<String>` per Phase 0 decision.

## Phase 7: Architecture Guards

Add 12 guards + 1 convention guard to
`crates/toron/tests/architecture_test.rs`. Examples:

```rust
#[test]
fn adr045_observability_listeners_use_hand_rolled_http_no_hyper_no_axum() {
    let cargo = include_str!("../Cargo.toml");
    for forbidden in ["hyper", "axum", "actix-web", "hyper-util"] {
        assert!(!cargo.contains(forbidden),
            "Toron Cargo.toml must not include {forbidden}");
    }
}

#[test]
fn adr045_no_revocation_no_audit_no_traces_in_observability() {
    let obs = include_str!("../src/network/observability.rs");
    let http = include_str!("../src/network/observability_http.rs");
    for forbidden in [
        "RevocationSnapshot", "AuditSink", "AuditEvent",
        "opentelemetry", "OtlpExporter",
    ] {
        assert!(!obs.contains(forbidden) && !http.contains(forbidden),
            "forbidden import: {forbidden}");
    }
}

#[test]
fn adr045_observability_http_has_per_request_read_timeout() {
    let http = include_str!("../src/network/observability_http.rs");
    let obs = include_str!("../src/network/observability.rs");
    assert!(http.contains("READ_REQUEST_TIMEOUT"),
        "observability_http.rs must declare READ_REQUEST_TIMEOUT");
    assert!(http.contains("Duration::from_secs(5)")
        || http.contains("Duration::from_millis(5000)"),
        "READ_REQUEST_TIMEOUT must be 5 seconds");
    assert!(obs.contains("timeout(observability_http::READ_REQUEST_TIMEOUT")
        || obs.contains("tokio::time::timeout("),
        "observability.rs handlers must wrap read_request in tokio::time::timeout");
}

#[test]
fn adr045_initial_metric_set_is_functional_brand_clean_and_stable() {
    let obs = include_str!("../src/network/observability.rs");
    assert!(
        !obs.contains("toron_"),
        "ADR 045 metric names must be functional, not brand-prefixed"
    );
    for required in [
        "daemon_ready",
        "daemon_uptime_seconds",
        "listener_up",
        "listener_accepts_total",
        "listener_rejects_total",
        "metrics_scrapes_total",
        "descriptors_loaded",
        "static_workers_configured",
    ] {
        assert!(obs.contains(required), "missing metric family: {required}");
    }
}

#[test]
fn adr045_ready_snapshot_excludes_high_cardinality_identity_fields() {
    let obs = include_str!("../src/network/observability.rs");
    let snapshot = obs
        .split("struct ReadinessSnapshot")
        .nth(1)
        .expect("ReadinessSnapshot must exist");
    for forbidden in [
        "worker_id",
        "tenant_id",
        "query_id",
        "task_id",
        "sql_text",
        "certificate",
        "endpoint",
        "table_name",
    ] {
        assert!(
            !snapshot.contains(forbidden),
            "ReadinessSnapshot must not expose {forbidden}"
        );
    }
}
```

Source-text guards (#3, #5, #6, #7, #8, #9, #10, #11, #12,
convention) assert string presence/absence. Runtime guards (#1, #2,
#4) exercise loader / admission paths.

## Phase 8: Integration Fixtures

### `crates/toron/tests/observability_listener_test.rs` (new)

1. `toron_config_missing_observability_section_loads_byte_identical_to_adr044`
2. `toron_health_endpoint_rejects_non_loopback_bind` — config-time.
3. `toron_ready_and_metrics_require_tls_when_configured` — config-time
   witness for D2's stricter TLS requirement even on loopback.
4. `toron_config_observability_port_collision_rejected`
5. `toron_health_endpoint_returns_200_ok_with_lowercase_ok_body`
6. `toron_ready_endpoint_emits_json_snapshot_with_expected_fields_in_stable_order`
7. `toron_metrics_endpoint_emits_prometheus_text_format_with_brand_clean_names`
8. `toron_ready_and_metrics_reject_peer_without_monitor_san_prefix`
9. `toron_observability_ready_stdout_line_appended_after_sql_ready_with_pinned_format`
10. `toron_observability_listener_closes_slow_client_after_read_timeout`
   — iter-3 sharpening witness; open a TCP connection to `/health`,
   write 5 bytes, pause 6 seconds; expect EOF on read (server closed
   connection without writing response).

### Optional V1-evidence fixtures (non-gating, post-promotion)

- `toron_metrics_endpoint_returns_expected_counter_after_synthetic_workload`
- `toron_ready_endpoint_returns_503_during_drain`

## Phase 9: Verification

Capture in `plans/impl/045_*_evidence.md`:

1. **Phase 0 evidence:** HTTP helper placement decision; tokio_rustls
   version; metric counter wiring seam audit; interior-mutability
   choice for `set_degraded_reason`.
2. **Phase 1-6 evidence:** `cargo test -p toron --tests` green;
   `cargo clippy -p toron -- -D warnings` clean.
3. **Phase 7 evidence:** all 12 + 1 architecture guards pass.
4. **Phase 8 evidence:** all 10 required fixtures pass; optional
   V1-evidence fixtures captured separately.
5. **ADR 042 / 043 / 044 invariant preservation:**
   - example configs without `[observability]` boot byte-identical;
   - `READY <worker>` / `SQL_READY <sql>` stdout lines byte-
     identical;
   - all ADR 042 / 043 / 044 architecture guards continue to pass;
   - `toron_fixture_worker.rs` source unchanged (10th preserved
     cycle).
6. **Hyper / axum / actix absence:** `cargo tree -p toron` confirms
   no HTTP framework in transitive deps beyond what was already
   present.
7. **Slowloris read-timeout effective:** slow-client fixture passes; manual
   spot-check with `nc` confirms hung connection is closed by server
   after ~5 seconds.

## Phase 10: Evidence Record

Materialize `plans/impl/0045_toron_daemon_observability_listeners_evidence.md`
with:

- Phase 0 audit findings.
- Phase 1-6 implementation evidence.
- Phase 7 guard pass evidence.
- Phase 8 fixture pass evidence.
- Phase 9 verification evidence (invariant-preservation checklist,
  fixture-worker source-unchanged check, 10th preserved cycle).
- Risk register disposition.

## Acceptance Checklist

- [ ] Phase 0 audit captures HTTP helper placement decision.
- [ ] Phase 1 dependencies added; `crates/nilket/Cargo.toml`
      untouched.
- [ ] Phase 2 `observability_http.rs` compiles clean
      (`cargo clippy -- -D warnings`).
- [ ] Phase 2 `READ_REQUEST_TIMEOUT` constant declared at 5 seconds.
- [ ] Phase 2 `read_request` call sites wrapped in
      `tokio::time::timeout` in all three handlers.
- [ ] Phase 3 `observability.rs` ships `ReadinessSnapshot` +
      `MetricsRegistry`.
- [ ] Phase 4 config validation rejects non-loopback `/health` bind
      AND any configured `/ready` or `/metrics` without `[tls]`.
- [ ] Phase 4 port-collision check rejects bind-address collisions.
- [ ] Phase 4 `render_example_config()` includes
      `[observability.listeners]` comment block.
- [ ] Phase 5 `admit_san` extends to `monitor-` prefix; ADR 044
      admission unchanged for `toron-` and `client-`.
- [ ] Phase 6 daemon emits `OBSERVABILITY_READY` line byte-identical
      to D8 pinned format.
- [ ] Phase 6 drain-state hook flips `/ready` to 503 within one poll
      interval on SIGTERM.
- [ ] Phase 7 all 12 ADR 045 guards + 1 convention guard pass.
- [ ] Phase 8 all 10 required fixtures pass.
- [ ] `toron_fixture_worker.rs` source unchanged (10th preserved
      cycle).
- [ ] ADR 042 / 043 / 044 guards continue to pass.
- [ ] No `hyper` / `axum` / `actix-web` in `cargo tree`.
- [ ] No `toron_` metric names in rendered metrics.
- [ ] No placeholder query/task in-flight gauges ship.
- [ ] Phase 10 evidence record materialized.

## Non-Goals

(Mirrors ADR 045 Out of Scope.)

- `nilket toron inspect` real wiring (ADR 046 lead candidate).
- OTLP traces export.
- Audit-sink emission per scrape.
- Revocation surface (CRL / OCSP / `RevocationSnapshot`).
- `ActorPermission` model parallel to Wendelon's.
- Prometheus histogram metrics.
- OpenMetrics protobuf format.
- Per-tenant metric labels.
- Distributed tracing context propagation.
- Aggregate cluster-wide readiness.
- Log aggregation / forwarding.
- Cross-host `/health`.
- HTTP/2 / HTTP/3 / pipelining / keep-alive.

## Risk Register

1. **Hand-rolled HTTP/1.1 parser correctness.** A bug in
   `read_request` could read forever or misparse a malformed request.
   Mitigation: 8 KiB read cap; pattern-match exact CRLF terminators;
   reject anything ambiguous; mirror Wendelon's helper shape (already
   battle-tested); 5-second `READ_REQUEST_TIMEOUT` upper-bounds the
   read phase even on parser bug.
2. **`tokio_rustls` API drift from Wendelon's version.** Mitigation:
   Phase 0 pins versions explicitly.
3. **Drain-state race conditions.** If `set_degraded_reason("draining")`
   doesn't happen-before the next `/ready` request, that request
   could return 200 instead of 503 during drain. Mitigation: use
   `Arc<RwLock<Option<String>>>` for the field, or `arc-swap` for
   lock-free updates. Phase 0 picks one.
4. **Metric counter accuracy.** Counters are `AtomicU64` with
   `Relaxed` ordering. Mitigation: industry-standard for metrics
   (eventual-consistency is fine); document.
5. **Port-collision check false positive.** If an operator
   deliberately binds two listeners to the same port for unsupported
   reasons, D1 rejects them. Mitigation: this is intentional; surface
   the stable diagnostic.
6. **`monitor-` SAN prefix divergence from Wendelon causes operator
   confusion.** Documented as non-carry-over in ADR §D3. Mitigation:
   example config comment block in `render_example_config()` shows
   the canonical `monitor-<principal>.<cluster_domain>` SAN shape
   with rationale.
7. **ADR 044 `admit_san` regression from `monitor-` extension.**
   Adding a new prefix could subtly break ADR 044 admission semantics
   for `toron-` or `client-`. Mitigation: all ADR 044 guards 7 + 15
   continue to pass; new guard 4 covers `monitor-` per-listener
   admission.
8. **Read timeout cuts legitimate slow clients on degraded
   networks.** 5 seconds is a balance; tighter would cut more
   legitimate scrapers. Mitigation: documented in `READ_REQUEST_TIMEOUT`
   constant comment; operators on degraded networks can patch the
   constant (acceptable since this is a hardening default, not a
   wire-protocol invariant).

## Cross-References

- ADR file (paired):
  `plans/adr/0045_toron_daemon_observability_listeners.md`.
- Wendelon observability helpers (parity reference):
  `crates/wendelon/src/metrics/observability.rs`.
- Wendelon `ReadinessSnapshot.to_json()` (D6 parity reference):
  `crates/wendelon/src/metrics/observability.rs:539-602`.
- Wendelon metric naming convention (D5 parity reference):
  `crates/wendelon/src/metrics/registry.rs:1907+`.
- ADR 044 `admit_san` (extended by D3):
  `plans/adr/0044_authenticated_remote_toron_quic_listeners.md`.
- ADR 042 D11 shutdown semantics (extended by D6 drain-state hook):
  `plans/adr/0042_toron_compute_daemon_substrate.md`.
- ADR 042 D12 / 043 D16 / 044 D8 stdout readiness lines (extended by
  D8 OBSERVABILITY_READY line): same files.
- Iter brainstorm trail:
  `plans/wip/claude_iter_{0..4}_adr_045.md` and
  `plans/wip/claude_iter_{2..4}_impl_045.md`.
