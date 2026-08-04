# ADR 048: Shared `observability-http` Workspace Crate

**Status:** Promoted
**Seed:** ADR 047 promotion §"ADR 048 candidates"
(`crates/toron/plans/wip/roadmap.md`); accumulated parallel
HTTP/1.1 helpers across ADR 045 (server-side) + ADR 046
(client-side) + Wendelon `metrics/observability.rs` + Wendelon
`metrics/exporter.rs`; ADR 049 lead-candidate (`nilket wendelon
inspect` parity) imminent need for shared client-side parser.
**Council:** Claude, Codex, Gemini (3 iterations: iter-0
topic-shape survey with three-way split — Codex
`observability-http` extraction, Claude `nilket wendelon
inspect` parity, Gemini multi-tenant routing; iter-1 Claude
R1 retraction (Shape A demoted to ADR 049 lead candidate;
Codex's `observability-http` extraction adopted as ADR 048
lead); iter-1 Claude S1 sharpening (Wendelon production code
fully untouched for V1, not "opportunistic"); iter-2 dual
ADR+impl convergence Codex+Claude 2-of-3; iter-3 three small
sharpenings — S1 D8 enumerates 17 promotion-locked diagnostic
strings explicitly, S2 new guard 5
`adr048_observability_http_public_surface_is_small`, S3
fixture 3 split into happy-path + missing-parts-reject;
iter-4 not needed; promoted from iter-3 ratification with
Codex+Claude 2-of-3 convergence). **Second refactor cycle in
the ADR 040-onward sequence** (first was ADR 047 TLS/PKI).
**Category:** Refactor ADR — behavior-preserving extraction
with zero functional changes.

## Summary

Extract a new `crates/observability-http/` workspace crate
owning the bounded, hand-rolled HTTP/1.1 request/response
helpers used by Toron's observability listeners (ADR 045
`/health` / `/ready` / `/metrics`) and the inspect-style CLI
client (ADR 046 `nilket toron inspect`). Toron consumes the
crate via a thin compatibility shim at
`crates/toron/src/network/observability_http.rs` preserving
ADR 045 + ADR 046 source-text guard expectations.

ADR 048 is a **pure behavior-preserving refactor** — zero
functional changes, zero wire changes, zero config changes,
zero new operator-visible functionality. All ADR
042/043/044/045/046/047 integration fixtures continue to
pass byte-identical pre/post the extraction. ADR 040-047
architecture-guard function names, coverage, and pass count
are preserved by the promotion meta-guard; assertion content
may relax only where a literal storage site moves behind a
shared helper or re-export and the new ADR guards pin the
new storage site.

Wendelon production observability code (`metrics/observability.rs`
+ `metrics/exporter.rs`) is **fully untouched** in V1. Three
observed behavior divergences (request-parser shape,
request-line return shape, response header casing) gate
behavior-preserving migration. A future ADR can decide
whether to ratify a Wendelon behavior change or extend
`observability-http` with a Wendelon-shape compatibility
layer.

**No** TLS in `observability-http` (`tls-pki` owns that).
**No** timeout policy (caller-owned; Toron keeps
`READ_REQUEST_TIMEOUT` and `CONNECT_TIMEOUT` in
`crates/toron/src/network/observability_http.rs` and
`crates/toron/src/cli.rs` respectively). **No** routing /
path dispatch (caller-owned). **No** status-code semantics
(caller-owned). **No** web framework dependencies (forbidden
list: `hyper`, `axum`, `actix-web`, `tower`, `reqwest`,
`ureq`, `h2`, `http`, `httparse`, `bytes`, `tokio-util`,
`rustls`, `tokio-rustls`, `quinn`, `tls-pki`, `tracing`).
**No** Wendelon production code migration. **No** Wendelon
inspect CLI (ADR 049 lead candidate; non-binding). **No**
multi-tenant routing (Gemini iter-0/1/2 dissent; ADR 050+
candidate). **No** Plan A / Plan B sub-lift framing
(single-plan, matches ADR 042/043/044/045/046/047).

## Motivation

ADR 045 introduced Toron's hand-rolled HTTP/1.1 helpers in
`crates/toron/src/network/observability_http.rs` for the
`/health` / `/ready` / `/metrics` listeners (server-side
`read_request` + `parse_request_line` + `write_response`).
ADR 046 extended the same module with client-side helpers
(`write_request` + `read_response`) for the `nilket toron
inspect` CLI. Wendelon maintains its own inline HTTP/1.1
helpers in two locations:

- `crates/wendelon/src/metrics/observability.rs:488-530` —
  server-side `read_request` (one-read probe returning
  `String`), `parse_request_line` (returns `(method, path)`
  tuple), `write_response` (lowercase `content-type` /
  `content-length` / `connection` headers).
- `crates/wendelon/src/metrics/exporter.rs:35-67` —
  inline `handle_connection` with hand-rolled lowercase
  response strings.

**Three sites of HTTP/1.1 duplication across two rings.**
The next-cycle composition target — `nilket wendelon
inspect` parity, projected as ADR 049 lead — would create
a fourth duplication site: a Wendelon-local client-side
HTTP/1.1 parser. Either copying Toron's helpers (new
Toron+Wendelon duplication) or importing Toron internals
cross-ring (breaks discipline).

Extracting the shared substrate now prevents the imminent
duplication and gives ADR 049 a clean import path:
`use observability_http::{write_request, read_response,
HttpResponse}`.

**Wendelon production code is not migrated** in ADR 048
because three behavior divergences gate byte-identical
migration:

1. **Request parser shape.** Wendelon's `read_request`
   (`metrics/observability.rs:488-504`) is a tiny one-read
   probe returning `String`; Toron's reads until
   `\r\n\r\n` with 8 KiB cap returning `Vec<u8>`.
   Migrating Wendelon changes malformed-request acceptance
   behavior.
2. **Request-line return shape.** Wendelon's
   `parse_request_line` (`metrics/observability.rs:506-518`)
   returns `(method, path)` tuple; Toron's returns
   `RequestLine { method, path, version }`. Migration
   changes call-site signatures and exposes the protocol
   version field.
3. **Response header casing.** Wendelon emits
   `content-type` / `content-length` / `connection`
   lowercase (`metrics/observability.rs:527`,
   `metrics/exporter.rs:56,67`); Toron emits Title-Case
   `Content-Type` / `Content-Length` / `Connection`.
   Migration changes byte-level response output.

ADR 048 ships the shared substrate with **Toron-shape
semantics** (terminator-detecting parser, `RequestLine`
struct, Title-Case response headers). Wendelon's production
helpers stay Wendelon-local for V1.

## Decisions (10, Promotion-Locked)

**D1: Create `crates/observability-http/` workspace crate
with four-module scope.**

New crate at `crates/observability-http/`. Workspace member
auto-picked-up by the root `crates/*` glob. Brand-clean name
(no `nilket-` / `toron-` / `wendelon-` prefix; no V1 suffix;
**thirteenth consecutive cycle** preserving the convention —
ADRs 039 / 041 / 042 / 043 / 044 / 045 / 046 / 047 / 048).

Module structure:

```
crates/observability-http/src/
├── lib.rs            (module exports + bounded limits)
├── request.rs        (read_request + parse_request_line + RequestLine)
├── response.rs       (read_response + HttpResponse + Content-Length + chunked reject)
└── writer.rs         (write_request + write_response)
```

Public API (promotion-locked):

```rust
// lib.rs
pub const MAX_REQUEST_HEADER_BYTES: usize = 8 * 1024;
pub const MAX_RESPONSE_HEADER_BYTES: usize = 8 * 1024;
pub const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;

pub use request::{RequestLine, parse_request_line, read_request};
pub use response::{HttpResponse, read_response};
pub use writer::{write_request, write_response};
```

```rust
// request.rs
pub struct RequestLine<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub version: &'a str,
}

pub async fn read_request<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> std::io::Result<Vec<u8>>;

pub fn parse_request_line(raw: &[u8])
    -> std::io::Result<RequestLine<'_>>;
```

```rust
// response.rs
pub struct HttpResponse {
    pub status_code: u16,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

pub async fn read_response<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> std::io::Result<HttpResponse>;
```

```rust
// writer.rs
pub async fn write_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    status: &str,
    content_type: &str,
    body: &str,
) -> std::io::Result<()>;

pub async fn write_request<S: AsyncWrite + Unpin>(
    stream: &mut S,
    method: &str,
    path: &str,
    host: &str,
) -> std::io::Result<()>;
```

**`RequestLine.version` is public.** Toron's current parser
already requires a third request-line token. Exposing that
token as `version` preserves the parse boundary without
adding version policy to the shared crate. `parse_request_line`
rejects missing version but does not validate version content
— version policy stays caller-owned.

**Not in `observability-http`:**

- TLS (`tls-pki` owns it).
- Routing / path dispatch / route table.
- Listener types.
- Status code semantics (callers map status codes to
  application semantics; ADR 045 keeps `/health` /
  `/ready` / `/metrics` route constants Toron-local; ADR
  046 keeps the 200 / 503 closed-status surface Toron-
  local).
- Timeout policy (caller-owned; see D3).
- Tracing / metrics / audit / status enum.

**D2: Behavior-preserving refactor; zero functional changes.**

ADR 048 ships **zero functional changes**. All
ADR 042/043/044/045/046/047 integration fixtures continue to
pass byte-identical pre/post extraction, and ADR 040-047
architecture-guard coverage is preserved by the ADR 048
meta-guard.

Specifically preserved:

- ADR 045 `/health` / `/ready` / `/metrics` route constants
  (`HEALTH_PATH`, `READY_PATH`, `METRICS_PATH`) stay Toron-
  local in `crates/toron/src/network/observability.rs`.
- ADR 045 D4 server-side `READ_REQUEST_TIMEOUT = 5s` stays
  Toron-local (see D3).
- ADR 045 D5 8 KiB request-header cap byte-stable through
  the shared crate.
- ADR 045 D6 `ReadinessSnapshot` JSON shape unchanged.
- ADR 045 metric exposition format unchanged.
- ADR 046 D7 inspect 200 / 503 closed-status surface stays
  Toron-local in `crates/toron/src/cli.rs`.
- ADR 046 D7 `CONNECT_TIMEOUT = 5s` stays Toron-local.
- ADR 046 D9 `Host:` header + `Connection: close` shape on
  inspect request.
- All diagnostic strings on error paths preserved byte-
  identical (D8 enumeration).

Architecture guard 12
(`adr048_prior_adr04x_guards_continue_to_pass`) asserts ADR
040-047 architecture guard function names continue to exist
and the guard sweep pass count is preserved pre/post
extraction. ADR 048 follows the ADR 047 refactor precedent:
prior-cycle guard function names remain stable; assertion
content may be minimally relaxed only when a literal storage
site moves behind a shared helper or re-export and the
guard's semantic intent is pinned by the new cycle's guards.

**D3: Caller-owned timeout policy.**

`observability-http` does not import `std::time::Duration`
or `tokio::time`. Toron keeps both timeout constants in
ring-local locations:

```rust
// crates/toron/src/network/observability_http.rs (the shim)
pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

// crates/toron/src/cli.rs (per ADR 046's existing location)
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
```

ADR 045 guard 4
(`adr045_observability_http_module_uses_5_second_read_request_timeout`)
source-text-matches `READ_REQUEST_TIMEOUT` in
`crates/toron/src/network/observability_http.rs`. Keeping
the constant there preserves byte-identical guard pass-rate
post-extraction.

ADR 046 guard 12 (`adr046_inspect_has_tcp_connect_timeout`)
source-text-matches `CONNECT_TIMEOUT` in
`crates/toron/src/cli.rs` (its original location). ADR 048
does **not** move `CONNECT_TIMEOUT` — leaves it in the
cli.rs site that ADR 046 pinned.

**D4: Toron consumes via a compatibility shim.**

`crates/toron/src/network/observability_http.rs` becomes a
thin module re-exporting the shared helpers and keeping
the `READ_REQUEST_TIMEOUT` constant Toron-local:

```rust
use std::time::Duration;

pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub use observability_http::{
    HttpResponse, MAX_REQUEST_HEADER_BYTES, MAX_RESPONSE_BODY_BYTES,
    MAX_RESPONSE_HEADER_BYTES, RequestLine,
    parse_request_line, read_request, read_response,
    write_request, write_response,
};
```

The shim approach (Codex iter-0 D4 / Claude iter-1 S3) is
chosen over absorb-the-call-site-rewrite because:

1. Preserves ADR 045 guard 4's timeout source-text intent
   while moving the request-header-cap literal behind the
   shared crate re-export.
2. Keeps timeout policy operator-facing and ring-local
   (Toron may evolve its timeout values independently of
   any future Wendelon timeout policy).
3. Lower-churn diff; smaller blast radius.

Toron's existing import paths continue unchanged:
`use crate::network::observability_http::{...}` now resolves
through the shim re-exports. The three observability
route-matching sites in `crates/toron/src/network/observability.rs`
perform the small tuple-to-field rewrite required by
`RequestLine { method, path, version }`: bind
`let line = parse_request_line(...)`, then match
`(line.method, line.path)`. Inspect already consumes only
`write_request` and `read_response`.

**D5: Wendelon production code stays Wendelon-local.**

Wendelon's `crates/wendelon/src/metrics/observability.rs`
and `crates/wendelon/src/metrics/exporter.rs` are **not
modified** in this cycle. Architecture guard 11
(`adr048_wendelon_observability_http_helpers_remain_local`)
asserts source-text presence of Wendelon's `read_request` /
`parse_request_line` / `write_response` and the inline
`handle_connection` response strings in `exporter.rs`.

Production Wendelon modules **must not** import
`observability_http` in this cycle. Test-only imports are
not needed for ADR 048; the important invariant is no
production migration.

Reason: three observable divergences (parser shape,
request-line return shape, response header casing — see
Motivation) gate behavior-preserving migration. A future
ADR can decide between (a) ratifying a Wendelon behavior
change to adopt Toron-shape semantics, or (b) extending
`observability-http` with a Wendelon-shape compatibility
layer.

**ADR 048 does NOT ship:**

- Wendelon-side `observability-http` re-exports.
- Wendelon-side migration witness fixtures.
- Wendelon-side architecture guard requiring helper
  consumption.

**ADR 048 DOES ship:**

- `observability-http` exposed for Wendelon's future
  client-side consumption via ADR 049 (`nilket wendelon
  inspect`). Wendelon production server-side code stays
  as-is.

**D6: Crate dependency set is single — Tokio IO traits only.**

```toml
[package]
name = "observability-http"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
tokio = { version = "1.37", default-features = false, features = ["io-util"] }

[dev-dependencies]
tokio = { version = "1.37", default-features = false, features = ["io-util", "macros", "rt"] }
```

Local Tokio dependency with `io-util` only (not
`tokio.workspace = true` which would pull in the workspace
`full` feature set). Architecture guard 2
(`adr048_observability_http_depends_only_on_tokio_io`) pins
this exactly.

**Forbidden in `observability-http`** (architecture guard 2
asserts absence):

```
hyper, hyper-util, axum, actix-web, tower, reqwest, ureq,
h2, http, httparse, bytes, tokio-util, rustls, tokio-rustls,
quinn, tls-pki, tracing
```

**D7: Status interpretation is caller-owned.**

`observability-http::read_response` returns `HttpResponse`
with a numeric `status_code: u16`. It does not enumerate
the acceptable status set. Toron's CLI maps `200 → ready`,
`503 → degraded`, anything else → exit 2 + stable
diagnostic `"toron inspect: failed to read /ready response:
unexpected HTTP status N"`. ADR 046 guard 11
(`adr046_inspect_handles_only_200_and_503_status_codes`)
stays Toron-local and continues to pass byte-identical.

**D8: Diagnostic strings are pinned in the shared crate.**

The crate owns these **17 promotion-locked** stable
IO/parser diagnostic strings, byte-identical to Toron's
pre-extraction emitted strings:

```
connection closed before HTTP headers completed
HTTP request headers exceed 8 KiB
missing HTTP request line
missing HTTP method
missing HTTP path
missing HTTP protocol version
connection closed before HTTP response headers completed
HTTP response headers exceed 8 KiB
missing HTTP response status line
missing HTTP response version
unsupported HTTP response version
missing HTTP response status code
malformed HTTP response status code
malformed HTTP response header
chunked HTTP response bodies are not supported
missing Content-Length header
HTTP response body exceeds 64 KiB
```

Application diagnostics — e.g., inspect's `unexpected HTTP
status <code>` — stay caller-owned (Toron-local; not
shared-crate concern).

`parse_request_line` rejects missing request line / method /
path / version but does **not** reject a non-`HTTP/1.1`
request version. Callers own request-version policy.

`read_response` **does** reject non-`HTTP/1.1` response
versions (emitting `unsupported HTTP response version`),
matching current Toron inspect behavior.

If both `Transfer-Encoding: chunked` and `Content-Length`
are present in a response, the chunked rejection wins
(matches current Toron behavior).

**D9: Single-plan posture; cross-ring discipline preserved.**

No Plan A / Plan B framing. Toron-side extraction is the
single plan; Wendelon production code untouched (D5).
Single-plan matches ADR 042/043/044/045/046/047.

**Cross-ring discipline preserved:** `observability-http`
exports policy-neutral substrate only. No ring-internal
types (no `Toron*`, no `Wendelon*`, no `Nilket*`). No TLS
/ quinn / tracing dependencies. No operator-visible config
or wire surface.

**D10: Workspace lints / clippy / formatting consistency.**

`crates/observability-http/` follows the workspace's
`edition = "2024"`, formatter settings, and clippy lint
set. Adds `#![deny(unsafe_code)]` to `lib.rs`. No new
clippy allow-lists.

## Architecture Guards (12, Promotion-Locked)

1. `adr048_observability_http_crate_exists_under_workspace_glob`
   — `crates/observability-http/Cargo.toml` exists; root
   `Cargo.toml` contains the `crates/*` glob.
2. `adr048_observability_http_depends_only_on_tokio_io` —
   `crates/observability-http/Cargo.toml` lists only the
   single `tokio` dependency with `io-util`-only features;
   none of the forbidden token list (D6) appears.
3. `adr048_observability_http_has_bounded_header_and_body_limits`
   — `crates/observability-http/src/lib.rs` declares
   `MAX_REQUEST_HEADER_BYTES = 8 * 1024`,
   `MAX_RESPONSE_HEADER_BYTES = 8 * 1024`,
   `MAX_RESPONSE_BODY_BYTES = 64 * 1024`.
4. `adr048_observability_http_does_not_own_timeouts` —
   `crates/observability-http/` source does not reference
   `Duration`, `tokio::time`, `timeout`,
   `READ_REQUEST_TIMEOUT`, or `CONNECT_TIMEOUT`.
5. `adr048_observability_http_public_surface_is_small` —
   `crates/observability-http/src/` contains the 10
   required public tokens (3 `MAX_*` constants + `RequestLine`
   + `HttpResponse` + 5 helper functions); does not expose
   router / listener / TLS / tracing / TCP types
   (forbidden list: `pub struct Router`, `pub struct Listener`,
   `pub fn listen`, `pub fn dispatch`, `pub fn route`,
   `pub struct StatusCode`, `pub enum Status`, `rustls::`,
   `quinn::`, `tls_pki::`, `tracing::`, `TcpListener`,
   `TcpStream`).
6. `adr048_toron_observability_http_module_is_shim_only` —
   `crates/toron/src/network/observability_http.rs` contains
   `READ_REQUEST_TIMEOUT` constant + `pub use
   observability_http::{...}`, not parser loops or response-
   body parsing logic. Source-text asserts absence of
   parser-state identifiers (`header_end`, `content_length`,
   `body_len`, parser `loop {`).
7. `adr048_toron_observability_and_inspect_use_shared_http_helper`
   — `crates/toron/src/network/observability.rs` and
   `crates/toron/src/cli.rs` reference `observability_http::`
   (directly or via the shim re-exports).
8. `adr048_no_new_toron_observability_routes` — no new route
   constants in `crates/toron/src/network/observability.rs`
   beyond the ADR 045 `HEALTH_PATH` / `READY_PATH` /
   `METRICS_PATH` set.
9. `adr048_no_nilket_root_runtime_dependency_change` —
   `crates/nilket/Cargo.toml` is byte-identical pre/post
   (ADR 040 D4 / ADR 042 D2 / ADR 043 D2 / ADR 045 D9 /
   ADR 046 D10 / ADR 047 invariant preserved).
10. `adr048_no_wendelon_inspect_branch_this_cycle` —
    `crates/wendelon/src/cli.rs` does not contain an
    `inspect` arm or `WendelonInspect*` types.
11. `adr048_wendelon_observability_http_helpers_remain_local`
    — `crates/wendelon/src/metrics/observability.rs` still
    contains `async fn read_request`, `fn parse_request_line`,
    `async fn write_response`; `crates/wendelon/src/metrics/exporter.rs`
    still contains the inline `handle_connection` response-
    string formatting. No production Wendelon module imports
    `observability_http`.
12. `adr048_prior_adr04x_guards_continue_to_pass` — meta-
    guard asserting all ADR 040-047 architecture guard
    function names continue to exist and the ADR 04x guard
    sweep pass count is preserved pre/post. Prior-cycle
    assertion content may relax only when a literal moves
    behind a shared helper or re-export and a new ADR guard
    pins the new storage site.

**Post-promotion convention guard:**

- `adr048_new_observability_http_types_are_brand_clean_and_no_v1_suffix`
  — thirteenth consecutive cycle preserving the no-V1-
  suffix convention. New types `RequestLine`,
  `HttpResponse` carry no project-name prefix and no V1
  suffix.

## Integration Fixtures (12 required + 0 V1-evidence optional)

**Required (gates promotion):**

1. `observability_http_reads_request_until_header_terminator`
   — split-read request headers accepted only after the
   `\r\n\r\n` terminator; partial reads buffered.
2. `observability_http_rejects_request_headers_over_limit`
   — request headers above 8 KiB fail with stable diagnostic
   `HTTP request headers exceed 8 KiB`.
3. `observability_http_parses_request_line_with_version_field`
   — happy path: `RequestLine { method, path, version }`
   populated for `b"GET /ready HTTP/1.1\r\n..."`;
   `line.method == "GET"`, `line.path == "/ready"`,
   `line.version == "HTTP/1.1"`.
4. `observability_http_rejects_missing_request_line_parts`
   — missing request line / method / path / version each
   produce the pinned D8 diagnostic byte-identical (`missing
   HTTP request line` / `missing HTTP method` / `missing HTTP
   path` / `missing HTTP protocol version`).
5. `observability_http_writes_titlecase_response_headers`
   — `write_response(&mut buf, "200 OK", "application/json",
   "{}")` produces bytes containing `\r\nContent-Type:
   application/json\r\n`, `\r\nContent-Length: 2\r\n`,
   `\r\nConnection: close\r\n\r\n{}`. Byte-level assertion.
6. `observability_http_reads_content_length_response` —
   response parser reads exactly the declared `Content-
   Length` body length; preserves `Content-Type` value
   into `HttpResponse.content_type`.
7. `observability_http_rejects_chunked_response` —
   `Transfer-Encoding: chunked` in a response fails with
   stable diagnostic `chunked HTTP response bodies are not
   supported`.
8. `observability_http_rejects_response_body_over_limit` —
   response body above 64 KiB fails with stable diagnostic
   `HTTP response body exceeds 64 KiB`.
9. `observability_http_writes_get_request_with_host_and_close`
   — inspect-shape request remains exactly `GET <path>
   HTTP/1.1\r\nHost: <host>\r\nConnection: close\r\n\r\n`.
   Byte-level assertion.
10. `toron_observability_ready_metrics_preserve_responses_post_extraction`
    — Toron's existing ADR 045 `/ready` and `/metrics`
    integration fixtures still pass byte-identical (uses the
    shim path; exercises the shared crate transparently).
11. `toron_inspect_preserves_ready_fetch_post_extraction` —
    ADR 046 `nilket toron inspect` integration fixtures
    still pass byte-identical.
12. `prior_adr04x_architecture_guards_pass_post_http_extraction`
    — meta-fixture matching ADR 047's approach: prior-cycle
    architecture guard function-name inventory + pass count
    preserved (asserts ADR 040-047 architecture-test
    function names present and the guard sweep passes). This
    is a guard-inventory/pass-count fixture, not a source-
    assertion byte-snapshot.

**Optional V1-evidence (non-gating, post-promotion):**

- **None.** Codex iter-0 proposed two Wendelon-side optional
  fixtures
  (`wendelon_valid_ready_metrics_requests_parse_with_observability_http`,
  `wendelon_observability_response_bytes_preserved_if_migrated`);
  dropped at iter-1 because they presume a Wendelon-side
  migration policy that ADR 048 explicitly defers.

## Sub-Lift Posture

**Single-plan.** No Plan A / Plan B framing.

- Toron-side extraction is the single plan.
- Wendelon production code is not migrated (D5).
- Wendelon-side adoption is **deferred entirely**, not
  "optional V1-evidence."

Matches ADR 042/043/044/045/046/047 single-plan posture.

## Out of Scope

- Wendelon production HTTP helper migration
  (`metrics/observability.rs`, `metrics/exporter.rs`).
  Future ADR after a behavior-change policy is decided.
- `nilket wendelon inspect` CLI (ADR 049 lead candidate per
  Codex iter-3 §Roadmap Carry-Forward / Claude iter-1
  deferral commitment; non-binding).
- Multi-tenant routing (Gemini iter-0/1/2 dissent); too
  large for ADR 048 cycle scope; remains the ADR 050+
  leading large-substrate candidate.
- OTLP traces export, audit subsystem, live cert rotation,
  CRL / OCSP — no deployment evidence pressure.
- HTTP/2 / HTTP/3 / keep-alive / pipelining / chunked
  transfer support.
- Generic HTTP library posture (`observability-http` is
  deliberately narrow per D1; guard 5 enforces).
- `RequestLine` field expansion beyond `method` / `path` /
  `version`.
- Response writer header customization (always emits the
  same three Title-Case headers).
- Diagnostic-string drift: Phase 0 audit captures the exact
  pre-extraction text; shared crate emits byte-identical
  post-extraction (D8).
- Wendelon's lower-case response header normalization;
  ADR 048 ships Toron-shape Title-Case only.
- Toron CLI consolidation (`SqlClientTlsOptions` +
  `InspectTlsOptions` merge into shared `TlsCliOptions`).
  Rejected from ADR 047 (Gemini iter-3 proposal); future
  Toron-local cleanup ADR candidate.

## Council History

**Iter-0 split:**

- Codex iter-0: `observability-http` extraction (refactor;
  same family as ADR 047).
- Claude iter-0: `nilket wendelon inspect` parity
  (composition over ADR 045/046/047).
- Gemini iter-0: Multi-tenant routing substrate (4+ iter
  substrate).

**Iter-1 convergence (Codex+Claude 2-of-3):**

- Claude R1 retracted iter-0 Shape A as ADR 048 lead.
  Adopted Codex's `observability-http` extraction. Shape A
  demoted to ADR 049 lead candidate.
- Codex iter-1 adopted Claude iter-1 S1 (Wendelon
  production code untouched for V1).
- Iter-1 sharpenings S1-S8 (Claude) + iter-2 questions
  Q1-Q4 (Codex) resolved into iter-2 ADR.
- Gemini iter-1 held multi-tenant routing position;
  outvoted 2-of-3.

**Iter-2 dual draft (Codex+Claude convergence):**

- D-set locked at 10. Codex's 11-D structure collapsed by
  merging D11 (no multi-tenant) into Out-of-Scope and D12
  (convention posture) into D10 lints.
- Guard set: 11 architecture guards + 1 convention (iter-3
  added one).
- Fixture set: 11 required + 0 V1-evidence (iter-3 split
  one).

**Iter-3 ratification (Codex+Claude 2-of-3):**

- S1: D8 enumerates 17 promotion-locked diagnostic strings
  explicitly (Codex iter-2 D7 enumeration adopted; Claude
  iter-2 D8 extended in-place).
- S2: New guard 5
  (`adr048_observability_http_public_surface_is_small`)
  added (Codex iter-2 §Architecture Guards adopted).
- S3: Fixture 3 split into happy-path
  (`observability_http_parses_request_line_with_version_field`)
  + missing-parts-reject
  (`observability_http_rejects_missing_request_line_parts`).
- Codex iter-3 + Codex iter-4 ratified the same shape; no
  retractions on either side.
- Gemini iter-3 held multi-tenant routing; outvoted 2-of-3
  again; carried forward as ADR 050+ candidate.

**Accepted (Codex+Claude 2-of-3):**

- `observability-http` workspace crate creation.
- Refactor-only cycle, behavior-preserving.
- Toron substantial consumption via compatibility shim.
- Wendelon production code fully untouched for V1.
- Single Tokio `io-util` dependency, no web stack / TLS /
  tracing.
- Caller-owned timeout policy.
- Public `RequestLine.version` field.
- 17 pinned diagnostic strings byte-stable pre/post.
- Status-code interpretation caller-owned.
- No Wendelon inspect / no multi-tenant / no new operator-
  visible surface in ADR 048.

**Rejected (outvoted 2-of-3 Codex+Claude):**

- Gemini iter-0/1/2/3 multi-tenant routing as ADR 048 lead
  — too large for cycle scope; tenant-catalog isolation +
  mTLS tenant extraction + scoped-catalog decorator
  bundled into one ADR would consume 4+ iterations.
  Carried forward as ADR 050+ candidate.

## Promotion criteria met

1. **D-set locked at 10.** Council convergence reached
   2-of-3.
2. **Guard set locked at 12 + 1 convention.**
3. **Fixture set locked at 12 required + 0 V1-evidence
   optional.**
4. **Single-plan posture.**
5. **Cross-ring discipline preserved.** `observability-http`
   exports no ring-internal types.
6. **No-V1-suffix convention preserved** (thirteenth
   consecutive cycle: ADRs 039 / 041 / 042 / 043 / 044 /
   045 / 046 / 047 / 048).
7. **ADR 040-047 invariants preserved.** Refactor is
   behavior-preserving; fixture baseline comparison plus
   architecture guard name/pass-count preservation gates
   promotion.
8. **`toron_fixture_worker.rs` source unchanged**
   (thirteenth preserved cycle invariant).
9. **Second refactor cycle in the ADR 040-onward sequence.**
   First was ADR 047 (TLS/PKI). ADR 048 (HTTP/1.1
   observability) completes the operator-plane substrate
   pair.

## Conclusion

ADR 048 is a tight refactor cycle: one workspace crate
extracted, Toron consumes through a thin shim, Wendelon
production code untouched. Pays down observed HTTP/1.1
duplication across three sites (Toron `observability_http`
+ Wendelon `observability` + Wendelon `exporter`); prevents
the imminent fourth-site duplication that ADR 049 (Shape A,
`nilket wendelon inspect` parity) would otherwise create.
Behavior-preserving; promotion gated on byte-identical
pre/post comparison of ADR 042-047 fixtures, preservation
of ADR 040-047 architecture guard names/pass count, and
byte-identical preservation of the 17 pinned diagnostic
strings.

The eight-phase migration is materialized in
`plans/impl/0048_shared_observability_http_workspace_crate.md`.
