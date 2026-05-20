# ADR 044 Evidence

Date: 2026-05-19

## Scope

Implemented the ADR 044 Plan W-C fallback as a Toron-local TLS module
under `crates/toron/src/network/tls.rs`.

The public ADR 044 TLS surface is brand-clean per
`plans/conventions/no-project-name-in-protocols.md`:

- `config::TlsConfig`
- `network::tls::ListenerPolicy`
- `network::tls::TlsError`
- `DEFAULT_CLUSTER_DOMAIN`
- `AUTH_QUIC_ALPN`
- `canonical_daemon_dns_name`
- `canonical_client_dns_name`

The SAN wire convention remains the ADR 044 convention:

- daemon/server identity: `toron-<worker_id_uuid>.<cluster_domain>`
- SQL client identity: `client-<principal>.<cluster_domain>`

## Phase 0 Audit

- Selected Plan W-C: Toron-local TLS policy copy under
  `crates/toron/src/network/tls.rs`. Plan W-A was rejected because it
  would import Wendelon server internals into Toron. Plan W-B remains
  the long-term refactor target, but extracting Wendelon's TLS module
  would broaden this transport substrate ADR.
- quinn / rustls pins used by Toron: `quinn = "0.11"`,
  `rustls = "0.23"`, `rustls-pki-types = "1.10"`.
- TLS test material is generated in tests with `rcgen`, not checked in:
  daemon/server certs, `client-*` certs, sibling `toron-*` certs,
  wrong-prefix certs, subdomain-injection certs, wildcard listener
  certs, unknown-CA certs, and encrypted/non-PKCS8 key material.
- Listener seams: `QuicEndpoint::bind_authenticated` threads policy
  server configs into worker-task and SQL acceptors; loopback configs
  keep `QuicEndpoint::bind_loopback`.
- Dial seam: `ToronTransport::connect_worker` carries `WorkerEntry`
  identity so `QuicTransport` can derive
  `canonical_daemon_dns_name(&worker.worker_id, cluster_domain)` for
  authenticated static-worker dials.
- SQL CLI seam: `parse_sql_args` handles flag/env precedence and
  `run_sql_async` selects `client_authenticated` plus explicit SNI
  when TLS material is present.

## Verification

Passed:

- `cargo test -p toron --test config_test`
  - 11 passed
- `cargo test -p toron --test tls_listener_test`
  - 9 passed
- `cargo test -p toron --test architecture_test adr044`
  - 17 passed
- `cargo test -p toron cli::tests`
  - 6 passed
- `cargo test -p toron network::tls::tests`
  - 4 passed
- `cargo test -p toron`
  - lib: 200 passed
  - architecture: 331 passed
  - SQL listener integration: 5 passed
  - TLS listener integration: 9 passed
  - config integration: 11 passed
  - Wendelon storage execution: 9 passed, 1 ignored
  - doctests: 0 passed

Fixture coverage:

- Required fixtures 1-5: covered by `config_test` plus listener
  startup tests for encrypted/non-PKCS8 key rejection.
- Required fixtures 6, 11, 12: covered by `cli::tests`, including
  mTLS flag success, env success, flag-over-env success, and unknown-CA
  connection-failure exit.
- Required fixtures 7, 9, 10: covered by `tls_listener_test`.
- Required fixture 8: covered by
  `toron_sql_select_executes_over_authenticated_remote_listener`.
- Optional SAN helper evidence: covered by
  `adr044_san_convention_mirrors_wendelon_shape`.

Quality gates:

- `cargo clippy -p toron --no-deps -- -D warnings`
  - passed.
- `cargo clippy -p toron -- -D warnings`
  - passed.
- `cargo fmt --check -p toron -p wendelon`
  - passed.
- `git diff --check`
  - passed.

Clippy baseline notes:

- Existing Wendelon path-dependency warnings are isolated behind
  crate-level lint allowances in `crates/wendelon/src/lib.rs`, with the
  build-script `collapsible_if` cleaned directly.
- Toron's package-local `manual_div_ceil` allowance preserves
  `crates/toron/src/bin/toron_fixture_worker.rs` byte-identical while
  allowing `cargo clippy -p toron -- -D warnings` to cover the ADR 044
  Toron changes.

## Preserved Boundaries

- `crates/nilket/Cargo.toml` unchanged.
- `crates/toron/src/bin/toron_fixture_worker.rs` unchanged.
- SQL wire remains unchanged; mTLS failures occur before `SqlFrame`.
- `READY` and `SQL_READY` output paths are unchanged.
- No token/JWT/OIDC/SPIFFE/TOFU/fingerprint auth surface added.
- No CRL/OCSP/reload/watch/runtime cert rotation surface added.
