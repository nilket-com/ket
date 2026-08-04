# Implementation Plan: ADR 071 — Nilket Wendelon Inspect Real Wiring

**Status:** Implemented (paired with
`plans/adr/0071_nilket_wendelon_inspect_real_wiring.md`; evidence in
`plans/impl/0071_nilket_wendelon_inspect_real_wiring_evidence.md`).

## Overview

Eight-phase migration. CLI-only composition cycle; no new
crate, no daemon-side changes, no shared-helper extraction.
Wendelon consumes the (Toron-sequence) ADR 047 `tls-pki` +
ADR 048 `observability-http` substrates that already exist.

```
Phase 0: Audit (ADR 046 inspect shape, Wendelon substrate, env-var parity, cert helpers)
Phase 1: Cargo dependency + CLI dispatch arm
Phase 2: Argument parsing + InspectOptions + precedence
Phase 3: TLS + HTTP client wiring (fetch_ready)
Phase 4: Readiness view parse + render + exit-code mapping
Phase 5: Architecture guards
Phase 6: Integration fixtures
Phase 7: Evidence record + promotion
```

## Phase 0: Audit

**Tasks:**

**0.1** Read Toron's ADR 046 inspect implementation in
`crates/toron/src/cli.rs` to mirror its structure:
- `run_inspect` dispatch + arg parse.
- `InspectOptions` struct + precedence
  (`TORON_INSPECT_ENDPOINT` / `_SERVER_NAME` / `_CLIENT_CERT`
  / `_CLIENT_KEY` / `_CA`, lines 207-227).
- `fetch_ready` / `fetch_ready_with_tcp_connect` (lines
  ~277-350).
- exit-code mapping (lines 254-272): **body-authoritative**
  (`snapshot.ready` → exit 0/1; status gate 200/503 → else
  exit 2).
- text renderer (lines 388-396): each value as compact JSON
  (ADR 046 JSON-style value rendering: optional strings quoted
  when present, bools/numbers bare, `null` optionals), extended
  to Wendelon's string fields.
- timeout constants: `CONNECT_TIMEOUT` (cli.rs:17),
  `READ_REQUEST_TIMEOUT` (observability_http shim).

**0.2** Confirm Wendelon substrate:
- `/ready` listener at `metrics/observability.rs:239`,
  `ActorPermission::ObservabilityReady` (Wendelon ADR 027
  observability posture).
- `ReadinessSnapshot` at `:52`; `is_ready()` at `:535`
  (`degraded_reason.is_none() && audit_materialization_lag_entries == 0`);
  `to_json()` at `:547` (field order + `display_node_id` +
  conditional FIPS fields).
- TLS wrapper: `crate::network::tls::TlsMaterial::load`,
  `rustls_client_config_with_alpn(&material, Vec::new())`.
- Observability server ALPN: `Vec::new()` (cli.rs:162).
- Default `/ready` port `7432` (`examples/wendelon.toml.example`).

**0.3** Confirm `crates/wendelon/tests/support/tls_pki.rs`
generated-actor-cert helper API for the actor-authorization
round-trip fixture (fixture 14). Record the actual helper
names before Phase 6.4.

**0.4** Confirm Wendelon CLI dispatch shape
(`crates/wendelon/src/cli.rs:19-56`). Existing arms:
`[flag]` help/version/example, `[cmd, subcmd, audit_path]`
audit verify, `[flag, path]` write-example, `[config_path]`
run daemon. **Critical:** the new `inspect` arm
(`[cmd, rest @ ..] if cmd == "inspect"`) must be ordered
before the single-arg `[config_path]` arm so a bare
`inspect` routes to `run_inspect` (which reports missing-TLS-
flags), not to `run_daemon("inspect")`.

**Acceptance:** ADR 046 structure mapped; Wendelon substrate
+ cert-helper API + dispatch insertion point confirmed.

## Phase 1: Cargo dependency + CLI dispatch arm

**Tasks:**

**1.1** Add to `crates/wendelon/Cargo.toml`:

```toml
observability-http = { path = "../observability-http" }
```

(`tls-pki` is already a Wendelon dependency from ADR 047.)

**1.2** Add the dispatch arm in `wendelon::cli::run`, ordered
before the `[config_path]` single-arg arm:

```rust
[cmd, rest @ ..] if cmd == "inspect" => run_inspect(rest),
```

**1.3** Update `print_usage` to name `inspect`.

**Acceptance:** `cargo build -p wendelon` succeeds; bare
`nilket wendelon inspect` dispatches to a stub `run_inspect`
returning exit 2 + usage (NOT daemon boot).

## Phase 2: Argument parsing + InspectOptions + precedence

**Tasks:**

**2.1** Define `InspectOptions` (brand-clean, no V1 suffix):
`endpoint: SocketAddr`, `server_name: String`,
`cert_path: PathBuf`, `key_path: PathBuf`, `ca_path: PathBuf`,
`json: bool`.

**2.2** Implement `run_inspect(args: &[String]) -> ExitCode`:
parse `--endpoint`, `--server-name`, `--cert`, `--key`,
`--ca`, `--json`. Unknown flags → exit 2 + usage.

**2.3** Endpoint precedence (D3): `--endpoint` >
`WENDELON_INSPECT_ENDPOINT` > `127.0.0.1:7432`. Invalid →
exit 2 + `invalid WENDELON_INSPECT_ENDPOINT: <error>`.

**2.4** TLS field precedence: flag > env
(`WENDELON_INSPECT_SERVER_NAME`, `_CLIENT_CERT`,
`_CLIENT_KEY`, `_CA`). If any of the four absent → exit 2 +
`wendelon inspect requires --server-name, --cert, --key, and
--ca`.

**Acceptance:** unit tests for precedence + all-or-none TLS
flags pass (fixtures 1, 2).

## Phase 3: TLS + HTTP client wiring (fetch_ready)

**Tasks:**

**3.1** Define timeout constants in `crates/wendelon/src/cli.rs`
(D12): `CONNECT_TIMEOUT` + `READ_RESPONSE_TIMEOUT`, both
`Duration::from_secs(5)`.

**3.2** Build TLS material + client config (D6):

```rust
let material = crate::network::tls::TlsMaterial::load(
    &options.cert_path, &options.key_path, &options.ca_path,
)?;
let client_config = crate::network::tls::rustls_client_config_with_alpn(
    &material, Vec::new(),
)?;
let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
```

**3.3** Implement `fetch_ready(options) -> Result<_, InspectError>`:
- TCP connect wrapped in `timeout(CONNECT_TIMEOUT, ...)` →
  `ConnectTimeout` / `Connect(...)`.
- TLS handshake via `connector.connect(server_name, stream)`
  → `TlsHandshake(...)`.
- `observability_http::write_request(&mut tls, "GET",
  "/ready", &host)`, `host` = endpoint `host:port` (RFC 7230
  §5.4) → `WriteRequest(...)`.
- `timeout(READ_RESPONSE_TIMEOUT,
  observability_http::read_response(&mut tls))` →
  `ResponseTimeout` / `ParseHttp(...)`.
- Status gate (D9): `status_code != 200 && != 503` →
  `UnexpectedStatus(status_code)`.

**3.4** Define `InspectError` enum (brand-clean) with one
variant per pinned diagnostic (D11).

**Acceptance:** fixtures 3 (request shape), 12 (connect
timeout), 13 (response timeout) pass.

## Phase 4: Readiness view parse + render + exit-code mapping

**Tasks:**

**4.1** Define narrow `ReadinessView` (NOT Wendelon's
server-side `ReadinessSnapshot`). Parse the body via
`serde_json::from_slice::<serde_json::Value>` + checked
extraction (resilient to conditional FIPS fields). Missing/
malformed required base fields → `ParseSnapshot(...)`.

**4.2** Readiness derivation (D8):
`degraded_reason.is_none() && audit_materialization_lag_entries == 0`.

**4.3** Output (D10):
- `--json`: response body bytes verbatim to stdout + one
  newline.
- Default: line-per-field, struct-declaration order, each
  value as compact JSON (strings quoted, scalars bare, `null`
  optionals, compact arrays); FIPS fields appended only when
  present in body.

**4.4** Exit-code mapping (D11): Ok + ready → 0; Ok + degraded
→ 1; Err → 2 + pinned stderr diagnostic.

**Acceptance:** fixtures 4-11 pass.

## Phase 5: Architecture guards

Add 16 ADR 071 guards + 1 convention guard to
`crates/wendelon/tests/architecture_test.rs` (Wendelon's own
architecture-guard location; reads Wendelon source directly).
Any guard that needs to assert a cross-ring invariant about
Toron source can read it via a workspace-relative path, but
ADR 071's guards are Wendelon-local source-text checks.

**Tasks:** implement guards 1-16 + convention guard per the
ADR §Architecture Guards list. Notes:

- Guard 8 (`adr071_wendelon_inspect_does_not_use_generic_http_clients`):
  **scope the `reqwest`/`hyper`/`h2` assertion to the inspect
  function region** (marker-comment-delimited span or
  function-body slice), NOT whole-file — Wendelon's `cli.rs`
  may reference `reqwest` elsewhere for OTLP.
- Guard 16 (`adr071_prior_adr_guards_continue_to_pass`):
  mirror the ADR 047/048 meta-guard convention, but assert
  **Wendelon's own** prior-cycle architecture-guard function
  names persist + the Wendelon guard sweep passes (not Toron's
  `adr04x_` prefixes). Existence inventory, not a byte-snapshot.
- Guard 14 (`adr071_wendelon_observability_server_helpers_remain_local`):
  assert no `observability_http` import in
  `metrics/observability.rs` / `metrics/exporter.rs`
  (ADR 048 D5 deferral intact); the inspect path's import in
  `cli.rs` is the only Wendelon-side `observability_http`
  consumer.

**Acceptance:** all 16 guards + 1 convention guard pass; all
prior-cycle Wendelon architecture guards still pass.

## Phase 6: Integration fixtures

Implement 15 required + 2 optional V1-evidence fixtures.

**Tasks:**

**6.1** Inline unit tests in `crates/wendelon/src/cli.rs::tests`
for parse/render/exit-code logic (fixtures 1, 2, 5, 6, 7, 8,
9 — synthetic `HttpResponse`/body inputs).

**6.2** In-memory `/ready` server harness (parallel to Toron's
`MinimalReadyServer`) for request-shape + status-handling
fixtures (3, 4, 10, 11).

**6.3** Deterministic timeout fixtures (12, 13) via pending-
connect + slow-response helpers (avoid host-routing flakiness).

**6.4** Real mTLS round-trip fixture (14) in
`crates/wendelon/tests/` using
`crates/wendelon/tests/support/tls_pki.rs` generated-actor-
cert helpers: `ObservabilityReady` actor succeeds; non-
authorized actor rejected by the server admission layer.

**6.5** Substrate-usage fixture (15): source/runtime-backed
assertion that inspect consumes ADR 048 helpers (no Wendelon-
local client parser copy).

**6.6** Optional V1-evidence (non-gating): end-to-end against
a bootstrapped node; break-glass diagnostic-scope cert.

**Acceptance:** `cargo test -p wendelon` passes all 15
required fixtures.

## Phase 7: Evidence record + promotion

**Tasks:**

**7.1** Stage the implementation, guards, fixtures, ADR, impl
plan, and roadmap updates as one review unit.

**7.2** Verify the promoted ADR + impl docs exist in the
**Wendelon** sequence (this file +
`plans/adr/0071_nilket_wendelon_inspect_real_wiring.md`).

**7.3** Update `crates/wendelon/plans/wip/roadmap.md`:
- Status section: ADR 071 entry as a Wendelon
  ops/observability CLI composition ADR (not a storage-engine
  continuation).
- Forward sequence: thread ADR 071 in as the operator-plane
  inspect-parity consumer that completes the symmetric
  `nilket {toron,wendelon} inspect` surface; next Wendelon
  ADR slot is 072.

**7.4** Update `crates/toron/plans/wip/roadmap.md` to
cross-reference Wendelon ADR 071 as the inspect-parity
consumer (NOT as Toron ADR 049). The Toron ADR 049 slot is
reclaimed for the next Toron operator-plane / cross-ring
candidate (multi-tenant routing lead).

**7.5** Same-window housekeeping (already done): ADR 047 D8
reconciliation is complete in the Toron roadmap + ADR 047
impl plan (all describe the as-built trust-chain-only D8
shape).

**Acceptance:** atomic commit lands the inspect implementation
+ guards + fixtures; ADR + impl promoted in the Wendelon
sequence; both roadmaps updated.

## Risk register

| Risk | Mitigation |
|------|-----------|
| `inspect` arm misordered vs `[config_path]` catch-all → bare `inspect` boots daemon | Phase 0.4 + 1.2: arm before single-arg arm; fixture asserts bare `inspect` reports missing-TLS-flags |
| Guard 8 false-positive on Wendelon OTLP `reqwest` usage | Phase 5: scope assertion to inspect function region |
| `READ_RESPONSE_TIMEOUT` naming diverges from Toron's `READ_REQUEST_TIMEOUT` | D12 documents the intentional divergence; guard 12 pins the Wendelon name |
| Wendelon `to_json` field order depends on `serde_json` `preserve_order` | `--json` passes body through verbatim; text renderer pins its own order |
| Actor-cert helper API differs from assumption | Phase 0.3 confirms before Phase 6.4 |
| `display_node_id` rendering mismatch (text vs JSON) | Phase 4.3 renders the parsed JSON value verbatim (compact-JSON), so node-id display matches the body |
| Guard 16 meta-guard accidentally asserts Toron `adr04x_` prefixes instead of Wendelon's | Phase 5: guard 16 inventories Wendelon's own prior-cycle guard names (the file lives in `crates/wendelon/tests/`) |
| Compact-JSON text rendering surprises operators expecting bare identifiers | D10 documents JSON-style value rendering; ADR 046 already quotes optional strings when present, and Wendelon applies the same deterministic rule to string fields |

## Phase totals

- 8 phases (0-6 implementation + Phase 7 promotion).
- 0 new crates.
- ~250-350 LOC added to `crates/wendelon/src/cli.rs`.
- 1 line added to `crates/wendelon/Cargo.toml`.
- 16 new architecture guards + 1 convention guard in
  `crates/wendelon/tests/architecture_test.rs`.
- 15 required fixtures (inline unit + `crates/wendelon/tests/`
  integration) + 2 optional V1-evidence.
- Zero changes to `crates/nilket/Cargo.toml` and nilket runtime
  source. The root CLI integration test was updated to retire a
  stale ADR 040-era Toron inspect reservation assertion and to
  cover `nilket wendelon inspect` dispatch.
- Zero changes to `crates/wendelon/src/metrics/`.
- Zero changes to `crates/wendelon/src/network/`.
- Zero changes to Toron's inspect implementation (the
  operator-plane mirror is a cross-reference, not shared code).
- Zero changes to `observability-http` / `tls-pki` public
  APIs. One internal `tls-pki` clippy lint was fixed during
  verification.

## Acceptance checklist

- [x] Phase 0 audit maps ADR 046 shape + Wendelon substrate +
      cert-helper API + dispatch insertion point.
- [x] Phase 1 dispatch arm + Cargo dep; bare `inspect` routes
      correctly (not daemon boot).
- [x] Phase 2 arg parsing + precedence + all-or-none TLS.
- [x] Phase 3 fetch_ready with empty-ALPN client config +
      symmetric timeouts.
- [x] Phase 4 body-authoritative readiness + compact-JSON
      render + exit codes.
- [x] Phase 5 16 guards + 1 convention guard pass (in
      `crates/wendelon/tests/architecture_test.rs`).
- [x] Phase 6 15 required fixtures pass.
- [x] Phase 7 ADR + impl promoted in Wendelon sequence; both
      roadmaps updated. Evidence:
      `0071_nilket_wendelon_inspect_real_wiring_evidence.md`.

Ready for implementation.
