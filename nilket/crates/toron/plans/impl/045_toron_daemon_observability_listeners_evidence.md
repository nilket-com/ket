# ADR 045 Implementation Evidence

Status: implemented in the working tree alongside
`plans/adr/045_toron_daemon_observability_listeners.md`.

## Audit Decisions

- HTTP helpers stayed Toron-local in `src/network/observability_http.rs`,
  matching ADR 044's Plan W-C local TLS-module posture.
- `tokio-rustls = "0.26"` was added to `crates/toron/Cargo.toml`,
  matching the rustls 0.23-compatible version already present in the
  workspace lockfile.
- `/ready` JSON field order is pinned by explicit serialization rather
  than relying on `serde_json::Map` iteration order.
- `[observability.listeners]` validation reuses the existing config
  normalization seam and adds a pairwise listener bind collision check
  for non-ephemeral ports.
- Observability runtime uses one shared `Arc<MetricsRegistry>` and
  one shared `Arc<ReadinessState>` threaded through the daemon listener
  tasks.

## Verification

Commands run on 2026-05-19:

```text
cargo check -p toron
cargo test -p toron --test toron_observability_listener_test
cargo test -p toron --test architecture_test adr045
cargo test -p toron --test architecture_test
cargo test -p toron --test toron_daemon_test
cargo test -p toron --test tls_listener_test
cargo test -p toron --test sql_listener_test --no-run
cargo test -p toron --lib network::tls::tests
cargo test -p toron --test sql_listener_test
cargo test -p toron --test config_test
cargo clippy -p toron -- -D warnings
```

All listed commands passed.
