# Implementation Plan: ADR 046 Nilket Toron Inspect Real Wiring

**Status:** Promoted (paired with `plans/adr/046_nilket_toron_inspect_real_wiring.md`)

## Implementation Thesis

Lift `nilket toron inspect` to a real CLI client. Add a new
`inspect` arm to `crates/toron/src/cli.rs` mirroring the ADR 043
`sql` arm structure (one-shot async runner + sync dispatch
boundary). Extend `crates/toron/src/network/observability_http.rs`
with two new client-side helpers (`write_request`,
`read_response`) parallel to the existing server-side helpers
(`read_request`, `write_response`). Construct TLS client config
via a **parallel** path to ADR 043's SQL client TLS construction
(no refactor in this cycle per D10 non-refactor decision). Parse
the `/ready` JSON response into `ReadinessSnapshot` (ADR 045 D6
shape) with forward-compatible serde tolerance for unknown
fields. Render as line-per-field text to **stdout** by default,
JSON pass-through to **stdout** with `--json`. Diagnostics emit
to **stderr** on error paths only. Accept HTTP 200 and 503 as
expected status codes; route any other status to exit code 2
with the pinned diagnostic per D7. Both TCP connect phase and
response-read phase wrap in 5-second `tokio::time::timeout`
(symmetric phase bounding). Exit with 0 (ready) / 1 (degraded) /
2 (usage / config / connectivity / protocol failure). Zero
daemon-side changes. Preserve every ADR 042 / 043 / 044 / 045
invariant.

## Phase 0: Audit

Resolve the following before Phase 1; capture in
`plans/impl/046_*_evidence.md`.

1. **ADR 045 hand-rolled HTTP helper shape.** Read
   `crates/toron/src/network/observability_http.rs` for the
   existing `read_request` / `write_response` /
   `parse_request_line` signatures and behavior. The new client-
   side helpers mirror this shape at the function-signature
   level.
2. **ADR 043 client TLS flag plumbing seam.** Read
   `crates/toron/src/cli.rs::sql` (or sibling) for the existing
   flag/env precedence and `tokio_rustls::TlsConnector`
   construction. Identify the construction shape but do NOT
   refactor into a shared helper in this cycle (D10).
3. **ADR 045 `ReadinessSnapshot` JSON serialization.** Read
   `crates/toron/src/network/observability.rs::ReadinessSnapshot::to_json`
   for the byte-level format. Confirm forward-compatible JSON
   parsing on the client side: `serde_json::from_slice` does NOT
   require `#[serde(deny_unknown_fields)]`.
4. **CLI arg-parsing shape.** Read `crates/toron/src/cli.rs` for
   the existing hand-rolled command parser. Identify how to add
   `inspect` alongside `start` and `sql` without introducing a
   parser dependency or restructuring the CLI.
5. **`nilket` root dispatch.** Read `crates/nilket/src/main.rs`
   (or equivalent) to confirm the `inspect` subcommand routes
   through `toron::cli::run` byte-identical to `start` / `sql`.
   No `crates/nilket` changes expected.
6. **HTTP response status code branching.** Confirm the parse
   path will distinguish 200 / 503 (expected) from all other
   codes (route to exit 2). Stable diagnostic text must
   substring-match `unexpected HTTP status <code>`.
7. **Timeout constant placement.** `CONNECT_TIMEOUT` and
   `READ_REQUEST_TIMEOUT` constants can live next to each other
   in `crates/toron/src/network/observability_http.rs` or split
   (`READ_REQUEST_TIMEOUT` in observability_http.rs from ADR 045,
   `CONNECT_TIMEOUT` next to the CLI dispatch in `cli.rs`). Phase
   0 picks; both work for V1.
8. **Phase 0 evidence record.** Capture audit findings before
   Phase 1 starts. Include version pins for `tokio_rustls`,
   `rustls`, `serde_json` (already in dep graph; no new deps
   expected).

## Phase 1: Dependencies and Module Skeleton

**No new dependencies.** All required crates already in
`crates/toron/Cargo.toml` from ADR 044 + ADR 045:

- `tokio` (existing)
- `tokio_rustls` (added in ADR 045 Phase 1)
- `rustls` + `rustls-pki-types` (added in ADR 044 Phase 1)
- `serde` + `serde_json` (existing)
- `tracing` (existing)

**Do not** add `http`, `hyper`, `reqwest`, `ureq`, `tower`,
`axum`, or `actix-web`. Guard 9 enforces.

**Do not** touch `crates/nilket/Cargo.toml`. ADR 040 D4 / ADR
042 D2 / ADR 043 D2 / ADR 045 invariants preserved.

No new modules; ADR 046 extends
`crates/toron/src/network/observability_http.rs`,
`crates/toron/src/network/tls.rs`, and
`crates/toron/src/cli.rs` only.

## Phase 2: Extend `observability_http.rs` with Client-Side Helpers

Add two new functions and one new product type:

```rust
pub struct HttpResponse {
    pub status_code: u16,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

/// Write a minimal HTTP/1.1 request: request-line, Host header,
/// Connection: close header, blank line. No request body.
/// Host header value is the endpoint host:port per RFC 7230 §5.4
/// (NOT the TLS SNI server name).
pub async fn write_request<S: AsyncWrite + Unpin>(
    stream: &mut S,
    method: &str,    // "GET" only in V1
    path: &str,      // "/ready" only in V1
    host: &str,      // endpoint host:port (D9 pinning)
) -> std::io::Result<()>;

/// Read an HTTP/1.1 response: status line, headers, content-
/// length-bounded body. Caps body at 64 KiB; returns
/// `ErrorKind::InvalidData` on overflow or malformed input.
pub async fn read_response<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> std::io::Result<HttpResponse>;
```

**Behavior:**

- `write_request` writes
  `<METHOD> <path> HTTP/1.1\r\nHost: <host>\r\nConnection: close\r\n\r\n`
  and flushes.
- `read_response` reads up to 8 KiB of status line + headers,
  parses status code (3 digits), reads `Content-Length` header,
  then reads exactly that many body bytes (capped at 64 KiB
  total response size). Returns `ErrorKind::InvalidData` on
  malformed status line, missing/non-numeric `Content-Length`,
  body exceeding 64 KiB, or chunked transfer encoding.

**Per-response read timeout:** caller wraps `read_response` in a
5-second `tokio::time::timeout` using the existing
`READ_REQUEST_TIMEOUT` constant from ADR 045.

## Phase 3: CLI Subcommand Wiring in `crates/toron/src/cli.rs`

Add a new `inspect` arm alongside the existing `start` and `sql`
arms in the current hand-rolled parser.

```rust
[verb, rest @ ..] if verb == "inspect" => run_inspect(rest),
```

Add a private parsed-args type:

```rust
struct InspectArgs {
    endpoint: Option<SocketAddr>,
    server_name: Option<String>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    ca: Option<PathBuf>,
    json: bool,
}
```

Manual parser behavior:

- `--endpoint <socket-addr>`;
- `--server-name <dns-name>`;
- `--cert <path>`;
- `--key <path>`;
- `--ca <path>`;
- `--json`;
- any unknown `--flag` returns
  `unknown toron inspect option: <flag>`;
- any non-flag argument returns
  `toron inspect does not accept positional arguments`.

Resolution:

- endpoint: flag > `TORON_INSPECT_ENDPOINT` > `127.0.0.1:7781`;
- TLS fields: flag > matching `TORON_INSPECT_*` env var;
- all four TLS fields are required, including on loopback.

**Timeout constants (iter-3 sharpening):**

```rust
/// Maximum time to wait for the TCP connect phase to complete.
/// Prevents indefinite hang on non-routable addresses (silent
/// SYN-drop). 5 seconds is symmetric with `READ_REQUEST_TIMEOUT`
/// for the response-wait phase; total wall-clock upper bound
/// for failed dials is ~10s.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
```

**Error enum:**

```rust
enum InspectError {
    Usage(String),
    Connectivity { action: &'static str, source: io::Error },
    UnexpectedStatus(u16),
}
```

**Dispatch:**

```rust
fn run_inspect_sync(args: InspectArgs) -> ExitCode {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("inspect runtime");
    runtime.block_on(run_inspect_async(args))
}

async fn run_inspect_async(args: InspectArgs) -> ExitCode {
    let endpoint = resolve_endpoint(args.endpoint);
    let tls = match build_inspect_tls(&args) {
        Ok(t) => t,
        Err(InspectError::Usage(msg)) => {
            eprintln!("{msg}");
            return ExitCode::from(2);
        }
        Err(e) => unreachable!("build_inspect_tls only emits Usage errors: {e:?}"),
    };
    match dial_and_render(endpoint, tls, args.json).await {
        Ok(exit) => exit,
        Err(InspectError::Usage(msg)) => {
            eprintln!("{msg}");
            ExitCode::from(2)
        }
        Err(InspectError::Connectivity { action, source }) => {
            // For the "within timeout" action variants, don't include source details
            // (the io::Error is synthetic).
            if action.contains("within timeout") {
                eprintln!("toron inspect: failed to {action}");
            } else {
                eprintln!("toron inspect: failed to {action}: {source}");
            }
            ExitCode::from(2)
        }
        Err(InspectError::UnexpectedStatus(code)) => {
            eprintln!("toron inspect: failed to read /ready response: unexpected HTTP status {code}");
            ExitCode::from(2)
        }
    }
}
```

**Dial path (iter-3 connect-timeout + iter-2 status code mapping):**

```rust
async fn dial_and_render(
    endpoint: SocketAddr,
    tls: InspectTlsConfig,
    json_flag: bool,
) -> Result<ExitCode, InspectError> {
    // iter-3 sharpening: wrap TcpStream::connect in CONNECT_TIMEOUT.
    let tcp = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(endpoint)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => return Err(InspectError::Connectivity {
            action: "connect to endpoint",
            source: e,
        }),
        Err(_) => return Err(InspectError::Connectivity {
            action: "connect to endpoint within timeout",
            source: io::Error::new(io::ErrorKind::TimedOut, "5s"),
        }),
    };

    let connector = TlsConnector::from(Arc::new(tls.rustls_config));
    let server_name = rustls::pki_types::ServerName::try_from(tls.server_name.clone())
        .map_err(|e| InspectError::Connectivity {
            action: "build server name",
            source: io::Error::new(io::ErrorKind::InvalidInput, e),
        })?;
    let mut stream = connector.connect(server_name, tcp).await
        .map_err(|e| InspectError::Connectivity { action: "complete TLS handshake", source: e })?;

    // D9: Host header is endpoint host:port form, not SNI server name.
    observability_http::write_request(&mut stream, "GET", "/ready", &endpoint.to_string()).await
        .map_err(|e| InspectError::Connectivity { action: "write request", source: e })?;

    let response = tokio::time::timeout(
        observability_http::READ_REQUEST_TIMEOUT,
        observability_http::read_response(&mut stream),
    )
    .await
    .map_err(|_| InspectError::Connectivity {
        action: "receive response within timeout",
        source: io::Error::new(io::ErrorKind::TimedOut, "5s"),
    })?
    .map_err(|e| InspectError::Connectivity { action: "parse HTTP response", source: e })?;

    // D7 status code mapping: 200 and 503 expected; any other is exit 2.
    if response.status_code != 200 && response.status_code != 503 {
        return Err(InspectError::UnexpectedStatus(response.status_code));
    }

    let snapshot: ReadinessSnapshot = serde_json::from_slice(&response.body)
        .map_err(|e| InspectError::Connectivity {
            action: "parse readiness snapshot",
            source: io::Error::new(io::ErrorKind::InvalidData, e),
        })?;

    // D6 stdout/stderr separation: success-path output to stdout.
    if json_flag {
        let stdout_body = std::str::from_utf8(&response.body)
            .map_err(|e| InspectError::Connectivity {
                action: "decode JSON response as UTF-8",
                source: io::Error::new(io::ErrorKind::InvalidData, e),
            })?;
        print!("{stdout_body}");
        if !stdout_body.ends_with('\n') {
            println!();
        }
    } else {
        render_text(&snapshot);
    }

    Ok(if snapshot.ready { ExitCode::SUCCESS } else { ExitCode::from(1) })
}
```

`ReadinessSnapshot` is re-exported from
`crates/toron/src/network/observability.rs`. Forward-compatible
JSON parsing: `serde_json::from_slice` tolerates unknown fields
by default.

## Phase 4: TLS Client Config Construction (Parallel to ADR 043)

Per D10 non-refactor decision, ADR 046 keeps a parallel
construction path to ADR 043's SQL client TLS construction.
Implement the HTTP rustls client config inside
`crates/toron/src/network/tls.rs`, where the existing private
PEM loading and verifier helpers already live:

```rust
pub fn http_client_config_from_paths(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
) -> Result<rustls::ClientConfig, TlsError>
```

The helper reuses `reject_env_interpolation`, `TlsMaterial::load`,
the existing exact server verifier semantics, and sets
`OBSERVABILITY_HTTP_ALPN`. It returns rustls `ClientConfig`, not
quinn `ClientConfig`. This shares low-level TLS material loading
without refactoring the SQL CLI parser or changing SQL behavior.

```rust
struct InspectTlsConfig {
    rustls_config: rustls::ClientConfig,
    server_name: String,
}

fn build_inspect_tls(args: &InspectArgs) -> Result<InspectTlsConfig, InspectError> {
    let cert_path = args.cert.as_ref();
    let key_path = args.key.as_ref();
    let ca_path = args.ca.as_ref();
    let server_name = args.server_name.as_ref();

    if cert_path.is_none() || key_path.is_none() || ca_path.is_none() || server_name.is_none() {
        return Err(InspectError::Usage(
            "toron inspect requires --server-name, --cert, --key, and --ca".into()
        ));
    }

    let rustls_config = crate::network::tls::http_client_config_from_paths(
        cert_path.expect("checked above"),
        key_path.expect("checked above"),
        ca_path.expect("checked above"),
    )
    .map_err(|error| InspectError::Connectivity {
        action: "initialize mTLS client",
        source: io::Error::new(io::ErrorKind::InvalidInput, error),
    })?;
    Ok(InspectTlsConfig {
        rustls_config,
        server_name: server_name.expect("checked above").clone(),
    })
}
```

The CLI parsing paths remain parallel to preserve ADR 043
stability. A future refactor ADR (alongside Plan W-B `tls-pki`
extraction) can consolidate SQL and inspect option parsing.

## Phase 5: Text Rendering

```rust
fn render_text(snapshot: &ReadinessSnapshot) {
    println!("ready: {}", snapshot.ready);
    println!("worker_listener_bound: {}", snapshot.worker_listener_bound);
    println!("sql_listener_bound: {}", snapshot.sql_listener_bound);
    println!("observability_listeners_bound: {}", snapshot.observability_listeners_bound);
    println!("descriptors_loaded: {}", snapshot.descriptors_loaded);
    println!("static_workers_configured: {}", snapshot.static_workers_configured);
    println!("uptime_seconds: {}", snapshot.uptime_seconds);
    match &snapshot.degraded_reason {
        None => println!("degraded_reason: null"),
        Some(reason) => println!(
            "degraded_reason: {}",
            serde_json::to_string(reason).expect("serialize degraded reason")
        ),
    }
}
```

Field order pinned byte-identical to ADR 045 D6 declaration. All
`println!` writes go to stdout (D6 separation). Fixture 6
asserts byte-identical output against a known payload.

## Phase 6: Architecture Guards

Add 12 guards + 1 convention guard to
`crates/toron/tests/architecture_test.rs`. Examples:

```rust
#[test]
fn adr046_toron_inspect_is_not_reserved() {
    let cli_src = include_str!("../src/cli.rs");
    assert!(
        cli_src.contains("verb == \"inspect\"") && cli_src.contains("run_inspect("),
        "inspect must be a real hand-rolled CLI command"
    );
    assert!(
        !cli_src.contains("[verb] if verb == \"inspect\" => reserved(\"inspect\")"),
        "inspect reservation arm must be removed"
    );
}

#[test]
fn adr046_inspect_uses_hand_rolled_http_no_http_crate_no_hyper_no_reqwest() {
    let cargo = include_str!("../Cargo.toml");
    for forbidden in ["http", "hyper", "reqwest", "ureq", "tower", "axum", "actix-web", "hyper-util"] {
        assert!(!cargo.contains(forbidden),
            "Toron Cargo.toml must not include {forbidden}");
    }
}

#[test]
fn adr046_inspect_handles_only_200_and_503_status_codes() {
    let cli_src = include_str!("../src/cli.rs");
    assert!(cli_src.contains("status_code != 200"),
        "inspect dial path must distinguish status code 200");
    assert!(cli_src.contains("status_code != 503") || cli_src.contains("!= 503"),
        "inspect dial path must distinguish status code 503");
    assert!(cli_src.contains("UnexpectedStatus") || cli_src.contains("unexpected HTTP status"),
        "inspect must route unexpected status codes to a stable diagnostic");
}

#[test]
fn adr046_inspect_has_tcp_connect_timeout() {
    let cli_src = include_str!("../src/cli.rs");
    assert!(
        cli_src.contains("CONNECT_TIMEOUT") || cli_src.contains("Duration::from_secs(5)"),
        "cli.rs must declare CONNECT_TIMEOUT or use Duration::from_secs(5) for connect phase"
    );
    assert!(
        cli_src.contains("tokio::time::timeout(CONNECT_TIMEOUT")
            || cli_src.contains("timeout(CONNECT_TIMEOUT"),
        "TcpStream::connect call sites must be wrapped in tokio::time::timeout"
    );
}

#[test]
fn adr046_no_new_daemon_wire_surface_in_observability_module() {
    let obs = include_str!("../src/network/observability.rs");
    for required in ["HEALTH_PATH", "READY_PATH", "METRICS_PATH"] {
        assert!(obs.contains(required),
            "observability.rs must retain HTTP path constant {required}");
    }
    for forbidden in ["INSPECT_PATH", "INFO_PATH", "VERSION_PATH", "WORKERS_PATH"] {
        assert!(!obs.contains(forbidden),
            "ADR 046 must not add HTTP path constant {forbidden} to observability.rs");
    }
}

#[test]
fn adr046_nilket_root_stays_runtime_free() {
    let nilket_cargo = include_str!("../../nilket/Cargo.toml");
    for forbidden in ["tokio = ", "clap = ", "tracing = ", "tokio_rustls", "tokio-rustls"] {
        assert!(!nilket_cargo.contains(forbidden),
            "crates/nilket/Cargo.toml must not include {forbidden}");
    }
}
```

Source-text guards (#1, #3, #4, #5, #6, #7, #8, #9, #10, #11,
#12, convention) assert string presence/absence. Runtime guard
(#2 — client-only) exercises absence of daemon constructors in
inspect dispatch.

## Phase 7: Integration Fixtures

### `crates/toron/tests/toron_inspect_cli_test.rs` (new)

Ten required fixtures:

1. `nilket_toron_inspect_dials_endpoint_from_flag`
2. `nilket_toron_inspect_dials_endpoint_from_env`
3. `nilket_toron_inspect_requires_all_four_tls_flags_together`
4. `nilket_toron_inspect_reports_connection_failure`
5. `nilket_toron_inspect_rejects_peer_without_monitor_san_client_cert`
6. `nilket_toron_inspect_renders_text_output_with_stable_field_order`
7. `nilket_toron_inspect_emits_json_passthrough_with_json_flag`
8. `nilket_toron_inspect_exit_zero_on_ready_one_on_degraded`
9. `nilket_toron_inspect_rejects_unexpected_http_status_code`
10. `nilket_toron_inspect_reports_connect_timeout` — exercises
    the timeout branch through a deterministic pending-connect
    helper or test-only injectable connector; asserts exit 2 +
    stable diagnostic `toron inspect: failed to connect to
    endpoint within timeout`.

**Test infrastructure:** Most fixtures use a mock HTTP server
(thin TCP listener accepting connection, optionally performing
mTLS handshake, returning canned HTTP response). Mock reuses ADR
045's `observability_http::write_response`. Cert generation via
`rcgen` mirrors ADR 044 / 045 patterns.

Fixture 5 (SAN rejection) uses a full Toron daemon instance via
ADR 045 observability listener spawn.

Fixture 9 (unexpected status) uses mock server returning HTTP
418 with small JSON body.

Fixture 10 (connect timeout) does not depend on external
routing behavior. Use a pending-connect helper or test-only
injectable connector with a short test timeout to witness the
same diagnostic branch deterministically.

### Optional V1-evidence fixtures (non-gating)

- `nilket_toron_inspect_reports_drain_state`
- `nilket_toron_inspect_round_trips_against_local_daemon`

## Phase 8: Verification

Capture in `plans/impl/046_*_evidence.md`:

1. **Phase 0 evidence:** HTTP helper audit; ADR 043 TLS
   parallel-path confirmed; ReadinessSnapshot forward-compat
   confirmed; timeout constant placement decision.
2. **Phase 1-5 evidence:** `cargo test -p toron --tests` green;
   `cargo clippy -p toron -- -D warnings` clean.
3. **Phase 6 evidence:** all 12 + 1 architecture guards pass.
4. **Phase 7 evidence:** all 10 required fixtures pass; optional
   V1-evidence fixtures captured separately.
5. **ADR 042 / 043 / 044 / 045 invariant preservation:**
   - example configs without `[observability]` boot byte-
     identical;
   - `READY <worker>` / `SQL_READY <sql>` / `OBSERVABILITY_READY
     ...` stdout lines byte-identical;
   - all prior-cycle architecture guards continue to pass;
   - `toron_fixture_worker.rs` source unchanged (11th preserved
     cycle).
6. **No new daemon-side wire:** `git diff HEAD --
   crates/toron/src/network/observability.rs` shows zero new
   spawn / accept / handle functions, zero new HTTP path
   constants, zero new `[observability]` config additions.
7. **No HTTP crate / framework:** `cargo tree -p toron` confirms
   no `http`, `hyper`, `reqwest`, `tower`, `axum`, or `ureq`.
8. **`crates/nilket` Cargo.toml unchanged.**
9. **Stdout/stderr separation:** fixtures 4 + 5 + 9 + 10 assert
   error-path messages on stderr; fixtures 6 + 7 + 8 assert
   success-path output (including degraded path) on stdout.
10. **Symmetric phase timeout bounding:** fixture 10 exercises
    the connect-timeout branch deterministically; `CONNECT_TIMEOUT`
    declared at 5 seconds; ADR 045's `READ_REQUEST_TIMEOUT` (5s)
    reused on the read phase.

## Phase 9: Evidence Record

Materialize `plans/impl/046_*_evidence.md` with:

- Phase 0 audit findings.
- Phase 1-5 implementation evidence.
- Phase 6 guard pass evidence.
- Phase 7 fixture pass evidence.
- Phase 8 verification evidence (invariant-preservation
  checklist, fixture-worker source-unchanged check, 11th
  preserved cycle, stdout/stderr separation evidence, symmetric
  phase timeout evidence).
- Risk register disposition.

## Acceptance Checklist

- [ ] Phase 0 audit captures HTTP helper signature parity
      decision.
- [ ] Phase 0 audit confirms ADR 043 TLS plumbing kept parallel
      (no refactor).
- [ ] Phase 0 audit confirms `ReadinessSnapshot` is forward-
      compatible (no `deny_unknown_fields`).
- [ ] Phase 0 audit pins `CONNECT_TIMEOUT` placement.
- [ ] Phase 1 confirms no new dependencies needed.
- [ ] Phase 2 `observability_http.rs` extension compiles clean.
- [ ] Phase 2 `Host:` header pinned to endpoint host:port.
- [ ] Phase 3 `cli.rs` inspect subcommand parses flag/env/default
      with documented precedence.
- [ ] Phase 3 `CONNECT_TIMEOUT` constant declared at 5 seconds.
- [ ] Phase 3 `TcpStream::connect` call site wrapped in
      `tokio::time::timeout`.
- [ ] Phase 3 dial path explicitly handles HTTP 200 and 503;
      routes other codes to `InspectError::UnexpectedStatus`.
- [ ] Phase 4 TLS client config requires all four flags
      together; fails fast with stable diagnostic.
- [ ] Phase 4 TLS client config kept parallel to ADR 043 (no
      refactor).
- [ ] Phase 5 text renderer emits fields in pinned order to
      stdout.
- [ ] Phase 5 error diagnostics emit to stderr only.
- [ ] Phase 6 all 12 ADR 046 guards + 1 convention guard pass.
- [ ] Phase 7 all 10 required fixtures pass.
- [ ] `toron_fixture_worker.rs` source unchanged (11th preserved
      cycle).
- [ ] ADR 042 / 043 / 044 / 045 guards continue to pass.
- [ ] No `http` / `hyper` / `reqwest` / `tower` / `axum` /
      `ureq` in `cargo tree`.
- [ ] `crates/nilket/Cargo.toml` untouched.
- [ ] `crates/toron/src/network/observability.rs` has no new
      HTTP endpoint constants or spawn functions.
- [ ] Exit code semantics 0/1/2 verified by fixtures 3 + 4 +
      8 + 9 + 10.
- [ ] Phase 9 evidence record materialized.

## Non-Goals

(Mirrors ADR 046 Out of Scope.)

- `/inspect` daemon-side endpoint.
- Worker enumeration / cluster topology view.
- Active-query / active-task introspection.
- Descriptor listing / schema browsing.
- `--watch` / streaming mode.
- Cluster-wide aggregate inspect.
- Multi-tenant filter.
- Inspect-specific SAN prefix.
- Auto-discovery of cert paths.
- Sub-subcommands.
- Live cert rotation / CRL / OCSP.
- `/info` / `/version` daemon endpoints.
- Shared TLS construction helper extraction (future refactor
  ADR).

## Risk Register

1. **ADR 043 TLS construction refactor risk (mitigated).** D10
   non-refactor decision commits to parallel construction. ~20
   lines of duplication tolerable; future refactor ADR
   consolidates alongside Plan W-B `tls-pki` extraction.
2. **Hand-rolled HTTP response parser correctness.**
   `read_response` must correctly handle Content-Length, status
   lines, and reject chunked encoding. Mitigation: 64 KiB body
   cap; 5s `READ_REQUEST_TIMEOUT`; `ErrorKind::InvalidData` on
   any ambiguity; explicit fixture for each parse failure mode.
3. **JSON parse drift between server and client.** Mitigation:
   forward-compatible parsing (no `deny_unknown_fields`);
   fixture 6 asserts byte-identical round-trip; both sides use
   the same `ReadinessSnapshot` type.
4. **Exit-code-2 false positives during legitimate slow
   networks.** Both `CONNECT_TIMEOUT` (5s) and
   `READ_REQUEST_TIMEOUT` (5s) could fire on legitimate slow
   responses. Mitigation: 5s is generous for connect + sub-KB
   JSON GET; document constants inline; operators on degraded
   networks can patch independently.
5. **`monitor-` SAN prefix collision between scraper and
   inspect provisioning.** No security issue (same read-only
   role), but audit logs may not distinguish. Mitigation:
   documented in ADR §D5; future ADR can introduce `inspect-`
   prefix if separation needed.
6. **ADR 045 D2 mTLS-on-loopback requirement.** Operators
   expecting "loopback = no auth" surprised. Mitigation:
   documented in D4; consistent operator mental model.
7. **`crates/nilket` Cargo.toml accidental change.** Mitigation:
   guard 10 source-text checks on every test run.
8. **`Host:` header mismatch with TLS SNI.** Some
   intermediaries / proxies may compare them and reject
   mismatches. Mitigation: V1 is direct-dial only (no proxies);
   conventional RFC 7230 §5.4 choice prevents future ambiguity.
9. **Unexpected HTTP status code drift.** Mitigation: diagnostic
   includes actual status code; operators distinguish proxy
   issues from daemon issues.
10. **Connect-timeout test flakiness.** Real network addresses
    differ by host routing table and firewall policy, so a
    documentation-block address is not a stable timeout witness.
    Mitigation: fixture 4 covers the real ECONNREFUSED path;
    fixture 10 uses a deterministic pending-connect helper or
    test-only injectable connector for the timeout branch.

## Cross-References

- ADR file (paired):
  `plans/adr/046_nilket_toron_inspect_real_wiring.md`.
- ADR 045 hand-rolled HTTP helpers (Phase 2 reference):
  `crates/toron/src/network/observability_http.rs`.
- ADR 045 `ReadinessSnapshot` shape (Phase 5 reference):
  `crates/toron/src/network/observability.rs`.
- ADR 043 SQL client TLS plumbing (Phase 4 parallel reference):
  `plans/adr/043_public_toron_sql_listener.md`.
- ADR 045 D2 mTLS-on-loopback (D4 reference):
  `plans/adr/045_toron_daemon_observability_listeners.md`.
- Iter brainstorm trail:
  `plans/wip/claude_iter_{0..3}_adr_046.md` and
  `plans/wip/claude_iter_{1..3}_impl_046.md`.
