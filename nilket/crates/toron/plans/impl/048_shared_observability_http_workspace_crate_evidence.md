# ADR 048 Implementation Evidence

Date: 2026-05-20

## Scope

Implemented the ADR 048 shared `observability-http` workspace crate
under `crates/observability-http/`.

The crate owns the bounded HTTP/1.1 helper surface only:

- request header read until `\r\n\r\n`;
- request-line parsing with public `RequestLine { method, path, version }`;
- Title-Case response writing;
- inspect-style request writing;
- `Content-Length` response reading with chunked rejection and body cap.

Toron consumes the crate through the existing
`crates/toron/src/network/observability_http.rs` shim, which keeps
`READ_REQUEST_TIMEOUT` Toron-local. `CONNECT_TIMEOUT` remains in
`crates/toron/src/cli.rs`.

Toron's import paths stay unchanged. The three route-matching call
sites in `crates/toron/src/network/observability.rs` perform the
small `RequestLine` tuple-to-field rewrite: bind
`let line = parse_request_line(...)`, then match
`(line.method, line.path)`.

Wendelon production observability code remains untouched.

## Verification

Commands run:

```text
cargo test -p observability-http
cargo test -p toron --test architecture_test
cargo test -p toron --test toron_observability_listener_test
cargo test -p toron --lib inspect
cargo test -p toron --test toron_daemon_test
cargo test -p toron --test toron_sql_listener_test
cargo test -p toron --test toron_tls_listener_test
cargo test -p toron
cargo clippy -p observability-http -- -D warnings
git diff --check
```

All listed commands passed.

Observed pass counts:

- `observability-http` substrate fixtures: 9 passed.
- Toron architecture tests: 382 passed.
- Toron observability listener fixtures: 11 passed.
- Toron inspect-focused lib tests: 11 passed.
- Toron daemon listener fixtures: 5 passed.
- Toron SQL listener fixtures: 5 passed.
- Toron TLS listener fixtures: 9 passed.
- Full `cargo test -p toron`: passed, including 1 ignored
  Wendelon storage fixture.

## ADR 048 Fixture Mapping

- Fixtures 1-9 are covered by
  `crates/observability-http/tests/substrate_test.rs`.
- Fixture 10 is covered by
  `toron_observability_ready_metrics_preserve_responses_post_extraction`.
- Fixture 11 is covered by
  `toron_inspect_preserves_ready_fetch_post_extraction`.
- Fixture 12 is covered by
  `prior_adr04x_architecture_guards_pass_post_http_extraction`.

## Preserved Boundaries

- `crates/nilket/Cargo.toml` unchanged.
- `crates/toron/src/bin/toron_fixture_worker.rs` unchanged.
- `crates/wendelon/src/metrics/observability.rs` unchanged.
- `crates/wendelon/src/metrics/exporter.rs` unchanged.
- `crates/wendelon/src/cli.rs` unchanged.
- No Wendelon `observability_http` import.
- No Wendelon `inspect` branch.
- No new Toron observability routes.
- No TLS, routing, status-code policy, timeout policy, metrics, audit,
  listener, or daemon lifecycle surface in `observability-http`.
- All 17 ADR 048 diagnostic strings are present in the shared crate
  source or substrate fixtures.

## Guard Adjustments

- Prior-cycle guard
  `adr045_observability_http_has_per_request_read_timeout` keeps its
  function name stable, but its request-header-cap assertion now
  checks `MAX_REQUEST_HEADER_BYTES` presence through the Toron shim
  instead of an inline `const` declaration. ADR 048 moved the literal
  into `observability-http`; ADR 048 guard 3 pins the shared storage
  site.
