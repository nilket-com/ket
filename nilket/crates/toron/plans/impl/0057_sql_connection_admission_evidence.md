# ADR 057 Two-Tier SQL Connection Admission Evidence

Date: 2026-05-21 America/Chicago.

## Passing Gates

- `cargo fmt -p toron`
  - Result: passed.
- `cargo check -p toron --tests`
  - Result: passed.
- `cargo clippy -p toron --all-targets -- -D warnings`
  - Result: passed.
- `cargo test -p toron network::connection::tests:: -- --test-threads=1`
  - Result: 9 passed.
- `cargo test -p toron --test config_test sql_connection_cap_config_round_trips_and_zero_disables -- --test-threads=1`
  - Result: 1 passed.
- `cargo test -p toron --test observability_listener_test sql_connection_metrics_are_configured_plus_other -- --test-threads=1`
  - Result: 1 passed.
- `cargo test -p toron --test tls_listener_test remote_sql_listener_rejects_malformed_client_sans_and_unknown_ca -- --test-threads=1`
  - Result: 1 passed.
  - Covered: malformed/unknown-CA mTLS handshakes do not terminate the SQL
    accept loop; a valid client is accepted afterward.
- `cargo test -p toron --test sql_listener_test global_sql_connection_cap_refuses_excess_incoming_before_handshake -- --test-threads=1`
  - Result: 1 passed.
- `cargo test -p toron --test sql_listener_test loopback_sql_tenant_connection_cap_uses_config_tenant -- --test-threads=1`
  - Result: 1 passed.
- `cargo test -p toron --test sql_listener_test remote_sql_tenant_connection_cap_closes_excess_after_san_resolution -- --test-threads=1`
  - Result: 1 passed.
- `cargo test -p toron --test architecture_test adr057 -- --test-threads=1`
  - Result: 22 passed.
- `cargo test -p toron --test architecture_test -- --test-threads=1`
  - Result: 558 passed.
- `cargo test -p toron -- --test-threads=6`
  - Result: passed.
  - Covered: full Toron test suite, including SQL listener (13 passed),
    TLS listener (10 passed), observability listener (21 passed), and
    socket/TLS negative-path fixtures. Expected negative TLS-handshake,
    stream-close, and OpenRaft shutdown diagnostics were emitted while the
    corresponding tests passed.
- `git diff --check`
  - Result: passed.

## Requirement Evidence

- Config surface:
  - `[sql].max_concurrent_connections` normalizes `0` / omitted to `None`
    and `>0` to `NonZeroU32`.
  - `TenantBudget.max_concurrent_connections` follows the same
    default-disabled shape under `[tenant_budgets.default]` and per-tenant
    entries.
- Listener placement:
  - SQL alone uses the `quinn::Incoming` seam:
    `accept_sql_incoming()` before handshake and
    `accept_incoming_connection(incoming)` after Tier 1 admission.
  - Worker transport and observability keep their existing accept paths.
- Admission semantics:
  - Tier 1 is tenant-agnostic and refuses excess `Incoming` handles before
    the handshake without a SQL frame.
  - Tier 1 handshake failure logs and continues accepting; endpoint closure
    remains the outer accept-loop break condition.
  - Tier 2 runs after SQL tenant resolution. Loopback uses the configured
    daemon tenant; remote SQL uses the mTLS client SAN tenant.
  - Tier 2 excess closes the QUIC connection without accepting a SQL stream
    or emitting `ResourceExhausted`.
  - Permits are RAII-held across the connection task and release on normal
    close, stream-accept failure, and task cancellation.
- Metrics and scope:
  - Global metrics are aggregate-only:
    `sql_connections_active` and `sql_connection_rejections_total`.
  - Tenant metrics are bounded to configured tenants plus `"other"`:
    `tenant_sql_connections_active` and
    `tenant_sql_connection_rejections_total`.
  - No coordinator, admission-controller, worker-transport, SQL wire,
    Wendelon server, `crates/nilket`, or `toron_fixture_worker.rs` drift.
