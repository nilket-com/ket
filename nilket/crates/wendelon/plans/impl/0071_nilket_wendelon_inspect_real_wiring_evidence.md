# ADR 071 implementation evidence

ADR 071 is implemented as Wendelon-local CLI composition over the
existing `/ready` listener, ADR 047 `tls-pki`, and ADR 048
`observability-http`.

## Commands

- `cargo fmt --check` — pass.
- `git diff --check` — pass.
- `cargo test -p wendelon --lib wendelon_inspect -- --nocapture`
  — pass, 16 focused inspect tests.
- `cargo test -p wendelon --test wendelon_inspect_cli -- --nocapture`
  — pass, 2 real mTLS inspect fixtures, including an audit-backed
  `ObservabilityReady` authorization-denied assertion for a
  `query_session` actor.
- `cargo test -p wendelon --test architecture_test adr071 -- --nocapture`
  — pass, 17 ADR 071 guards.
- `cargo test -p wendelon --test architecture_test` — pass, 48
  architecture tests.
- `cargo test -p wendelon` — pass, full Wendelon suite.
- `cargo test -p nilket` — pass, 20 root CLI integration tests.
- `cargo test -p tls-pki` — pass, 5 substrate tests.
- `cargo clippy -p wendelon -- -D warnings` — pass.
- `cargo clippy -p nilket -- -D warnings` — pass.

## Notes

- The first full `cargo test -p wendelon` attempt failed before test
  execution completed because the shared `target/` directory exhausted
  disk space. `cargo clean -p wendelon` removed generated package build
  artifacts only; the full test command was then rerun and passed.
- `crates/nilket/Cargo.toml` and nilket runtime source remain unchanged.
  A stale root CLI integration assertion that still expected ADR 040's
  reserved `toron inspect` diagnostic was updated to the ADR 046 wired
  behavior, and a lightweight `nilket wendelon inspect` dispatch fixture
  was added.
- `crates/tls-pki/src/asn1.rs` received one internal clippy-only
  mechanical fix (`is_multiple_of`) required by the Wendelon clippy gate;
  no `tls-pki` public API changed.
- TLS material/config setup reports `failed to initialize mTLS client`,
  matching Toron ADR 046's setup-vs-handshake split; only the actual
  connector failure reports `failed to complete TLS handshake`.
