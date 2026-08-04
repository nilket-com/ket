# ADR 046: Nilket Toron Inspect Real Wiring

**Status:** Promoted
**Seed:** ADR 045 promotion §"ADR 046 lead candidate"
(`crates/toron/plans/wip/roadmap.md`); ADR 040 reservation
pattern; ADR 042 + ADR 043 reservation-lift discipline; ADR 045 D6
`ReadinessSnapshot` shape as composition substrate.
**Council:** Claude, Codex, Gemini (3 iterations: iter-0 topic-
shape survey with Shape A `/ready` client lift selected over
Shapes B-E; iter-1 three sharpenings — D7 connectivity error
class enumerated with stable `failed to <action>:` diagnostic
prefix, D9 hand-rolled HTTP/1.1 client committed (mirrors ADR 045
D4 server-side posture), D4 four-TLS-flags-together requirement
pinned with stable diagnostic; iter-2 three preemptive
sharpenings — D7 response status code mapping pinned (200/503
expected, any other → exit 2 with stable diagnostic), D9 `Host:`
header value pinned to endpoint host:port per RFC 7230 §5.4, D6
stdout/stderr separation pinned, plus D10 public TLS API
separation for SQL vs inspect with shared internal rustls config
construction allowed; iter-3 connect-timeout symmetry
sharpening — D7 wraps `TcpStream::connect` in 5-second
`tokio::time::timeout` symmetric with ADR 045 D4
`READ_REQUEST_TIMEOUT`; iter-4 not needed; promoted directly from
iter-3 ratification).
**Category:** Composition ADR over ADR 045 (CLI-side reservation
lift; zero daemon-side changes).

## Summary

Lift the last ADR 040 reservation (`nilket toron inspect`) into a
real CLI client. After this ADR, all three ADR 040 reservations
are lifted (`start` by ADR 042, `sql` by ADR 043, `inspect` by
ADR 046). The CLI dials a Toron daemon's `/ready` endpoint over
HTTPS+mTLS using a `monitor-<principal>.<cluster_domain>` client
cert (reuses ADR 045 D3 SAN admission), receives the JSON
`ReadinessSnapshot` body (ADR 045 D6 shape), parses it forward-
compatibly, and renders one of two output formats on stdout:
human-readable line-per-field text by default, or JSON byte-
identical pass-through with `--json` flag. Exit code reflects
readiness state (0 ready, 1 degraded, 2 usage / config /
connectivity / protocol error). Diagnostics emit on stderr.

Reuses ADR 043 D12's `--server-name` / `--cert` / `--key` / `--ca`
client TLS flag plumbing pattern as a **parallel public API and
CLI parsing path**. SQL keeps `client_config_from_paths` returning
QUIC client config; inspect adds `http_client_config_from_paths`
returning rustls HTTP client config. Both may share a private
rustls `ClientConfig` builder parameterized by ALPN so the common
certificate loading and exact server verifier semantics do not
fork.
Reuses ADR 045's hand-rolled HTTP/1.1 helpers extended with
client-side `write_request` + `read_response` mirroring the
existing `read_request` + `write_response` server-side shape.
Both connect phase and response-read phase wrap in 5-second
`tokio::time::timeout` for symmetric wall-clock bounding (~10s
upper bound on failed dials).

**No** new daemon-side wire surface. **No** new SAN prefix.
**No** new configuration sections. **No** in-process daemon
construction. **No** http / hyper / reqwest / axum / tower /
ureq client.
**No** new revocation surface (preserves ADR 044 D7 / ADR 045
D7). **No** watch / streaming / cluster-wide aggregation.
**No** sub-subcommands. **No** auto-discovery of cert paths.
**No** ADR 043 CLI parser or public TLS API refactor in this
cycle.

## Motivation

ADR 045 shipped `/ready` returning `ReadinessSnapshot` JSON over
mTLS HTTPS. ADR 046 surfaces this data through a CLI inspection
path, completing the V1 operator-facing CLI surface:
`nilket toron start` (daemon lifecycle, ADR 042),
`nilket toron sql` (query workload, ADR 043),
`nilket toron inspect` (health observation, ADR 046).

After ADR 046, the ADR 040 reservation pattern is fully
discharged. Future CLI commands can be added through composition
ADRs without needing a new reservation pattern. The cycle is
purely CLI-side: zero daemon-side wire / config / endpoint
changes. The smallest possible composition cycle over ADR 045's
substrate.

## Decisions (11, Promotion-Locked)

**D1: Lift `nilket toron inspect`.**

```text
nilket toron inspect [--endpoint <addr>]
                     [--server-name <name>]
                     [--cert <path>]
                     [--key <path>]
                     [--ca <path>]
                     [--json]
```

ADR 040's `inspect` reservation diagnostic text is removed. After
ADR 046, all three ADR 040 reservations are lifted; the
reservation pattern discharges cleanly. `nilket toron start` and
`nilket toron sql` continue unchanged.

**D2: The CLI is a client, not an in-process introspection tool.**

Mirrors ADR 043 D2. `nilket toron inspect` constructs a Tokio
runtime inside Toron, dials the daemon's `/ready` endpoint over
mTLS HTTPS, sends a GET request, receives the JSON
`ReadinessSnapshot` body, renders it, and exits. It must **not**
construct `ToronDaemon`, `ToronWorker`,
`DistributedQueryCoordinator`, descriptor registries, metrics
registries, or any other in-process daemon machinery.

**D3: Endpoint selection mirrors ADR 043 D3.**

Precedence: `--endpoint` flag > `TORON_INSPECT_ENDPOINT` env var
> compiled default `127.0.0.1:7781` (matches ADR 045 D1 example
config `/ready` listener port).

Endpoint is a `SocketAddr` (host:port), not a URL. HTTPS scheme
is implicit (the `/ready` endpoint is always HTTPS per ADR 045
D2). Parsing stays in `crates/toron/src/cli.rs`; `crates/nilket`
stays runtime-free.

**D4: TLS client flags mirror ADR 044 + ADR 045 admission model.**

`nilket toron inspect` accepts the ADR 043 D12 client TLS flag
set verbatim:

```text
--server-name toron-<worker_uuid>.<cluster_domain>
--cert certs/monitor-<principal>.pem
--key certs/monitor-<principal>.key
--ca certs/peer-ca.pem
```

Env equivalents:

```text
TORON_INSPECT_ENDPOINT
TORON_INSPECT_SERVER_NAME
TORON_INSPECT_CLIENT_CERT
TORON_INSPECT_CLIENT_KEY
TORON_INSPECT_CA
```

Precedence: flag > env. All four TLS fields (cert, key, CA,
server-name) must be present together; missing any one fails
before dialing with exit code 2 + stable diagnostic on stderr:

```text
toron inspect requires --server-name, --cert, --key, and --ca
```

**No** loopback-only-skip-TLS path. ADR 045 D2 requires mTLS for
`/ready` even on loopback; ADR 046 follows.

**D5: Client cert SAN must use `monitor-` prefix; no new SAN prefix.**

Reuses ADR 045 D3 per-listener admission. `nilket toron inspect`
presents a `monitor-<principal>.<cluster_domain>` client cert.
The daemon's `/ready` listener admits `monitor-` prefixed SANs
only. Inspect is read-only observation, same role as Prometheus
scrape; operators provision one `monitor-` cert per principal
and use it for both scrape and inspect.

**No** new `inspect-` SAN prefix. ADR 045's `admit_san` admission
set remains `<prefix>` ∈ {`toron`, `client`, `monitor`}.

**D6: Output rendering — text by default, JSON pass-through; stdout/stderr separation pinned.**

Default output: stable line-per-field rendering of
`ReadinessSnapshot` to **stdout**:

```text
ready: true
worker_listener_bound: true
sql_listener_bound: true
observability_listeners_bound: true
descriptors_loaded: 12
static_workers_configured: 0
uptime_seconds: 47
degraded_reason: null
```

Pinned formatting:

- field names byte-identical with JSON field names (snake_case);
- field order byte-identical with ADR 045 D6 `ReadinessSnapshot`
  field declaration order;
- booleans rendered `true` / `false` lowercase;
- numerics rendered without thousands separators;
- `degraded_reason: null` for `None`;
- `degraded_reason: "<value>"` for `Some(value)`, rendered with
  JSON string escaping so the line remains single-line and
  field-for-field with the JSON shape.

With `--json`: write the daemon's response body byte-identical to
**stdout**, followed by a single trailing `\n`. No pretty-
printing, no field reordering, no whitespace normalization.

**Stdout/stderr separation:**

- **stdout** receives rendered output (text or JSON) on success
  paths (exit 0 or exit 1).
- **stderr** receives diagnostic messages on error paths (exit 2).
  No diagnostic messages on success paths.
- On exit 1 (daemon reachable but degraded), the daemon's reply
  is rendered to stdout exactly as on exit 0; the operator reads
  `degraded_reason` from the output. No additional stderr
  message.

`--json` flag does not affect exit code semantics; exit code
reflects `ReadinessSnapshot.ready` regardless of output format.

**D7: Exit code reflects readiness and failure class; status code mapping pinned; both connect and read phases timeout-bounded.**

| Exit code | Meaning |
| --------- | ------- |
| 0 | HTTP 200, valid JSON, `ReadinessSnapshot.ready == true` |
| 1 | HTTP 200 or HTTP 503, valid JSON, `ReadinessSnapshot.ready == false` |
| 2 | usage / config / connectivity / protocol error (missing required flag, malformed args, TCP / TLS / HTTP / JSON / timeout failure, including unexpected status codes) |

**Response status code mapping:**

Per ADR 045 D6, the daemon's `/ready` endpoint returns:

- HTTP 200 OK with body `ReadinessSnapshot { ready: true, .. }` iff ready;
- HTTP 503 Service Unavailable with body
  `ReadinessSnapshot { ready: false, degraded_reason: Some(_), .. }`
  iff degraded.

ADR 046 accepts both 200 and 503 as expected statuses provided
the body parses successfully as `ReadinessSnapshot`. Any other
status code maps to exit code 2 with stable diagnostic on
stderr:

```text
toron inspect: failed to read /ready response: unexpected HTTP status <code>
```

**Symmetric phase timeouts (iter-3 sharpening):**

Both the TCP connect phase and the response-read phase wrap in
`tokio::time::timeout`:

- `CONNECT_TIMEOUT: Duration = Duration::from_secs(5)` —
  prevents indefinite hang on non-routable / silent-SYN-drop
  addresses.
- `READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5)` —
  reused from ADR 045; prevents slowloris-style servers from
  holding the client connection open.

Total wall-clock upper bound for a failed dial: ~10 seconds
(5s connect + 5s response wait). Naming the two timeouts
separately keeps the rationale visible; future tuning can
move them independently.

Connectivity error class (exit 2) covers:

- TCP connect refused / timeout / unreachable (including
  `CONNECT_TIMEOUT` fire);
- TLS handshake failure (cert mismatch, unknown CA, SAN role
  rejection);
- HTTP response parse failure (malformed status line, missing
  Content-Length, chunked transfer encoding);
- response-read timeout fire;
- JSON parse failure (response body doesn't deserialize as
  `ReadinessSnapshot`);
- unexpected HTTP status code (not 200 or 503).

Stable diagnostics on stderr (`<action>` token part of the
stable surface):

```text
toron inspect: failed to connect to endpoint: <details>
toron inspect: failed to connect to endpoint within timeout
toron inspect: failed to complete TLS handshake: <details>
toron inspect: failed to write request: <details>
toron inspect: failed to receive response within timeout
toron inspect: failed to parse HTTP response: <details>
toron inspect: failed to read /ready response: unexpected HTTP status <code>
toron inspect: failed to parse readiness snapshot: <details>
```

Shell-script idiom: `nilket toron inspect && deploy.sh` runs the
deploy step iff the daemon reports ready.

**D8: Zero new daemon-side wire surface.**

ADR 046 ships only CLI-side changes. **No** new HTTP endpoint
added to the daemon. **No** changes to `/ready`, `/health`,
`/metrics`. **No** changes to ADR 043 SQL wire frames. **No**
new configuration sections.

Architecture guard 8 asserts via source-text invariant:
`crates/toron/src/network/observability.rs` does not gain new
`*_PATH` constants or spawn-function declarations in ADR 046.

**D9: Hand-rolled HTTP/1.1 client mirrors ADR 045's server-side helpers; `Host:` header pinned to endpoint host:port.**

Extend `crates/toron/src/network/observability_http.rs` with two
new client-side helpers:

```rust
pub async fn write_request<S: AsyncWrite + Unpin>(
    stream: &mut S,
    method: &str,    // "GET" only in V1
    path: &str,      // "/ready" only in V1
    host: &str,      // endpoint host:port (D9 pinning)
) -> std::io::Result<()>;

pub async fn read_response<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> std::io::Result<HttpResponse>;
```

**`Host:` header value:** the request emits

```text
GET <path> HTTP/1.1\r\nHost: <endpoint-host>:<endpoint-port>\r\nConnection: close\r\n\r\n
```

The `Host` header carries `<endpoint-host>:<endpoint-port>` in
canonical HTTP/1.1 shape — derived from the operator-specified
endpoint, **not** from the TLS SNI server name. Matches RFC 7230
§5.4 conventions; independent of mTLS server-name binding.

**No** `http` crate, **no** hyper, **no** reqwest, **no** axum,
**no** tower, **no** ureq. Guard 9 enforces via Cargo.toml
source-text check.

**D10: `crates/nilket` stays runtime-free; SQL and inspect TLS APIs stay parallel with a shared internal rustls builder.**

ADR 040 D4 / ADR 042 D2 / ADR 043 D2 / ADR 045 invariants carry
forward. `crates/nilket/Cargo.toml` is not touched by ADR 046.
Tokio / tracing / TLS / JSON deps all stay in
`crates/toron`.

**ADR 043 TLS stability boundary:** ADR 046 keeps the SQL and
inspect **public** construction paths parallel:

1. `client_config_from_paths` remains the SQL/QUIC client API and
   `nilket toron sql` parsing is not restructured.
2. `http_client_config_from_paths` is added for inspect/HTTP and
   returns rustls `ClientConfig`.
3. Both paths share a private `build_rustls_client_config`
   helper parameterized by ALPN. This is an internal
   behavior-preserving extraction of common rustls certificate,
   root-store, client-auth, and exact server verifier setup; it
   does not alter ADR 043's public API or CLI behavior.

A future refactoring ADR (alongside Plan W-B `tls-pki`
extraction) can still consolidate CLI option parsing, public
TLS-PKI APIs, or cross-crate TLS material ownership. Those broader
refactors remain out of scope for ADR 046.

**D11: One-shot CLI; no watch mode, no streaming.**

Mirrors ADR 043 D17. `nilket toron inspect` is one-shot: one
invocation = one dial = one render = exit. **No** `--watch`
flag, **no** streaming output, **no** periodic re-poll.
Operators wanting periodic inspection use shell loops or
supervisor scheduling.

## Architecture Guards (12, Promotion-Locked)

1. `adr046_toron_inspect_is_not_reserved` — guards D1.
2. `adr046_toron_inspect_is_client_only` — guards D2; the CLI
   does not construct daemon machinery.
3. `adr046_toron_inspect_dials_ready_endpoint` — guards D3 + D8;
   source-text check that the CLI dials `/ready` path only.
4. `adr046_toron_inspect_endpoint_precedence_flag_env_default`
   — guards D3 precedence rule.
5. `adr046_toron_inspect_requires_all_four_tls_flags_together`
   — guards D4; missing any one of cert/key/CA/server-name fails
   with stable diagnostic.
6. `adr046_toron_inspect_renders_text_by_default_json_with_flag`
   — guards D6.
7. `adr046_toron_inspect_exit_codes_are_pinned_to_0_1_2`
   — guards D7; the three exit code branches all exist in
   `crates/toron/src/cli.rs`.
8. `adr046_no_new_daemon_wire_surface_in_observability_module`
   — guards D8.
9. `adr046_inspect_uses_hand_rolled_http_no_http_crate_no_hyper_no_reqwest`
   — guards D9; Cargo.toml forbids http / hyper / reqwest / axum /
   tower / ureq.
10. `adr046_nilket_root_stays_runtime_free` — guards D10; carries
    forward ADR 040 D4 / ADR 042 / ADR 043 / ADR 045 invariant.
11. `adr046_inspect_handles_only_200_and_503_status_codes` —
    guards D7 status code mapping; source-text check that the
    CLI accepts HTTP 200 and 503 as expected and routes any
    other status code to exit 2 with the pinned diagnostic.
12. **`adr046_inspect_has_tcp_connect_timeout`** — guards D7
    iter-3 sharpening; source-text assertion that
    `CONNECT_TIMEOUT` is declared (or `Duration::from_secs(5)`
    is used at the `TcpStream::connect` call site) and the
    connect call is wrapped in `tokio::time::timeout`.

Post-promotion convention guard:

- **`adr046_new_inspect_types_are_brand_clean_and_no_v1_suffix`**
  — eleventh consecutive cycle preserving the no-V1-suffix
  convention (ADRs 039 / 041 / 042 / 043 / 044 / 045 / 046).

## Integration Fixtures (10 required targets + 2 V1-evidence optional)

**Required targets (gates promotion):**

Implemented inline in `crates/toron/src/cli.rs::tests` to access
the private hand-rolled parser, renderer, and deterministic
connect-timeout helper without widening production API surface.

1. `inspect_fetches_ready_from_flag_endpoint_and_preserves_json_body`
   — dials the flag-selected endpoint and preserves the daemon
   JSON body.
2. `inspect_endpoint_env_and_flags_precedence` — covers
   `TORON_INSPECT_ENDPOINT`, TLS env vars, `--json`, and flag >
   env precedence.
3. `inspect_requires_complete_tls_options_even_on_loopback` —
   missing any TLS field fails before dialing with the stable
   diagnostic.
4. `inspect_reports_connection_failure_exit_two` —
   unreachable endpoint returning ECONNREFUSED → exit 2 +
   stable diagnostic.
5. `inspect_rejects_peer_without_monitor_san_client_cert`.
6. `inspect_text_renderer_is_stable` — stable field order and
   JSON-style `degraded_reason` escaping.
7. `adr046_toron_inspect_renders_text_by_default_json_with_flag`
   — source-text guard for JSON pass-through with exactly one
   trailing newline, no trim / pretty / envelope behavior.
8. `inspect_exit_zero_on_ready_one_on_degraded` —
   degraded path output on stdout (no stderr diagnostic).
9. `inspect_unexpected_status_returns_exit_two` —
   fixture daemon returns HTTP 418; inspect surfaces exit 2 +
   stable diagnostic.
10. **`inspect_connect_timeout_maps_to_stable_error`** — iter-3
    sharpening witness; exercises the timeout branch through a
    deterministic pending-connect helper or test-only injectable
    connector rather than relying on external routing behavior.
    Inspect surfaces exit 2 + stable diagnostic `toron inspect:
    failed to connect to endpoint within timeout`.

**Optional V1-evidence (non-gating, post-promotion):**

- `nilket_toron_inspect_reports_drain_state` — full composition
  with ADR 045 D6 drain semantics.
- `nilket_toron_inspect_round_trips_against_local_daemon` —
  end-to-end happy path against a local Toron daemon with an
  ephemeral `monitor-` test cert.

## Sub-Lift Posture

**Single-plan.** No Plan A / Plan B framing. The cycle ships:

- one CLI subcommand argument parse + dispatch path;
- one HTTPS client dial via ADR 045 D2 mTLS-required `/ready`
  endpoint;
- hand-rolled HTTP/1.1 client helpers extending ADR 045's
  existing `observability_http.rs` module;
- one text renderer + JSON pass-through path;
- three exit code branches with distinct operator semantics;
- closed enum of expected HTTP status codes (200/503);
- symmetric 5-second timeouts on both connect and response-read
  phases.

Matches ADR 042 / 043 / 044 / 045 single-plan posture. Pure
composition over ADR 045 — no new daemon-side substrate. ADR 043
SQL CLI parsing and public TLS API are preserved; only the
private rustls client-config builder is shared between QUIC and
HTTP client paths (D10).

## Out of Scope (Recorded for Future ADRs)

- **`/inspect` daemon-side endpoint with richer detail.** Future
  ADR if operator pressure surfaces.
- **Worker enumeration / cluster topology view.** Coordinator-
  state observability deferred.
- **Active-query / active-task introspection.** Future substrate
  cycle.
- **Descriptor listing / schema browsing.** Cross-ring (Wendelon)
  cycle.
- **`--watch` / streaming mode.** V1 is one-shot.
- **Cluster-wide aggregate inspect** across multiple daemons.
- **Multi-tenant filter on inspect output.** Bundles with
  multi-tenant routing (ADR 047+).
- **Inspect-specific SAN prefix.** Reuses `monitor-` per D5.
- **Inspect cert/key flag auto-discovery / `~/.toron/` defaults.**
- **`nilket toron inspect <subcommand>`** for richer surface.
- **Live cert rotation / CRL / OCSP.** Carries forward ADR 044
  D7 / ADR 045 D7 deferrals.
- **`/info` or `/version` endpoints exposing build metadata.**
- **Cross-ring `nilket wendelon inspect`.**
- **Broader TLS construction consolidation** beyond the private
  rustls client-config builder: CLI option parser consolidation,
  public TLS-PKI API extraction, or cross-crate TLS material
  ownership. **Future refactor ADR** (likely alongside Plan W-B
  `tls-pki` extraction).
- **OpenAPI / machine-readable schema for `/ready` response.**

## Council History

- **Iter 0:** Claude proposed Shape A (`/ready` client lift),
  surveying Shapes B (new `/inspect` endpoint), C (in-process
  config printer), D (defer ADR 046), E (bundled endpoint +
  CLI). Took positions on `monitor-` SAN reuse, line-per-field
  text rendering, readiness-aware exit semantics, zero daemon-side
  change. Predicted Codex would converge on Shape A; Gemini
  might push for Shape B endpoint bundling (predicted 2-of-3
  against).
- **Iter 1:** No retractions; three sharpenings: D7 connectivity
  error class enumerated with stable `failed to <action>:`
  diagnostic prefix; D9 hand-rolled HTTP/1.1 client committed
  (matches ADR 045 D4 server-side hand-rolled posture); D4
  four-TLS-flags-together requirement pinned with stable
  diagnostic. D-set count moved 10 → 11 (D9 promoted from iter-0
  D8 sub-detail to standalone decision). Guard count 10 → 10.
  Required fixtures unchanged at 8.
- **Iter 2:** D-set / guards / fixtures locked at iter-1 shape
  with three preemptive sharpenings: D7 response status code
  mapping pinned (200/503 expected, any other → exit 2 with
  stable diagnostic); D9 `Host:` header value pinned to
  endpoint-host:port per RFC 7230 §5.4 (not TLS SNI server
  name); D6 stdout/stderr separation pinned (output to stdout
  including degraded path; diagnostics to stderr on error
  paths only). D10 sharpened with public-path separation:
  ADR 043 SQL parsing and public TLS API stay parallel while a
  private rustls client-config builder may be shared by SQL/QUIC
  and inspect/HTTP paths. New guard 11
  (`adr046_inspect_handles_only_200_and_503_status_codes`)
  preempts unexpected-status drift. New gating fixture 9
  (`inspect_unexpected_status_returns_exit_two` in the shipped
  inline CLI test organization).
  Guards 10 + 1 → 11 + 1. Required fixtures 8 → 9. No
  retractions.
- **Iter 3:** Final triple ratification. One sharpening, no
  retractions: D7 extended with symmetric TCP connect timeout
  (5-second `CONNECT_TIMEOUT` wrapping `TcpStream::connect` via
  `tokio::time::timeout`), mirroring ADR 045 D4
  `READ_REQUEST_TIMEOUT` server-side slowloris hardening on the
  client connect side. New guard 12
  (`adr046_inspect_has_tcp_connect_timeout`). New gating
  fixture 10
  (`inspect_connect_timeout_maps_to_stable_error` in the shipped
  inline CLI test organization) using a deterministic
  pending-connect helper. D-set locked at 11. Guard
  set locked at 12 + 1 convention. Fixture set locked at 10
  required + 2 V1-evidence optional. Ready to promote.

## Conclusion

ADR 046 ships:

- one `nilket toron inspect` CLI subcommand lifting the last
  ADR 040 reservation;
- one HTTPS+mTLS client dial against ADR 045 D2's `/ready`
  endpoint;
- hand-rolled HTTP/1.1 client helpers extending ADR 045's
  existing `observability_http.rs` module (no http / hyper /
  reqwest / axum / tower / ureq);
- `monitor-` SAN prefix reuse (no new admission model);
- line-per-field human text rendering by default + `--json` JSON
  pass-through; output on stdout, diagnostics on stderr;
- three exit code branches distinguishing ready (0) / degraded
  (1) / usage-config-connectivity-protocol failure (2)
  semantics;
- closed enum of expected HTTP status codes (200/503); any
  other status routes to exit 2 with pinned diagnostic;
- `Host:` header pinned to endpoint host:port per RFC 7230
  §5.4;
- symmetric 5-second timeouts on both TCP connect and
  response-read phases (total wall-clock upper bound ~10s for
  failed dials);
- zero new daemon-side wire surface;
- zero new daemon-side config sections;
- ADR 043 SQL CLI parsing and public TLS API kept parallel for
  ADR 043 stability, with a shared private rustls builder;
- byte-identical preservation of ADR 042 / 043 / 044 / 045
  invariants;
- 12 promoted architecture guards + 1 convention guard + 10
  required integration fixtures + 2 V1-evidence optional
  fixtures;
- single-plan substrate posture;
- eleventh consecutive cycle preserving the no-V1-suffix
  convention.

**After ADR 046, the V1 operator-facing CLI surface is
complete:** `nilket toron start` (ADR 042), `nilket toron sql`
(ADR 043), `nilket toron inspect` (ADR 046) — each backed by
real substrate, each one-shot, each runtime-free at the
`crates/nilket` boundary. The ADR 040 reservation pattern
discharges cleanly after three consecutive lifts.

The nine-phase migration is materialized in
`plans/impl/0046_nilket_toron_inspect_real_wiring.md`.

## Promotion Notes

ADR 046 is the **third consecutive ADR 040 reservation lift**
(after ADR 042 `start` and ADR 043 `sql`) and the **first
reservation lift with zero daemon-side changes**. It establishes
the discipline for future CLI composition over existing daemon-
side substrate: extend the CLI dispatch path; reuse existing
admission models; preserve every prior-cycle invariant byte-
identical; add one new gating fixture per substantive sharpening.

After ADR 046, no ADR 040 reservations remain. Future CLI
commands compose directly through `crates/toron/src/cli.rs`
without needing a new reservation-and-lift discipline.
