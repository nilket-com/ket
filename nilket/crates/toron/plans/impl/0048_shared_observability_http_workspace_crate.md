# Implementation Plan: ADR 048 — Shared `observability-http` Workspace Crate

**Status:** Promoted (paired with `plans/adr/0048_shared_observability_http_workspace_crate.md`).

## Overview

Eight-phase migration. Lighter than ADR 047's eleven phases —
no Wendelon-side migration to coordinate; no policy substrate
versus PKI substrate split; no DER/CBOR sub-extraction. The
crate has a deliberately narrow surface (10 public tokens
total per architecture guard 5).

```
Phase 0: Audit (capture pre-extraction baseline)
Phase 1: Crate skeleton
Phase 2: Move helpers from Toron to observability-http
Phase 3: Convert Toron module to compatibility shim
Phase 4: Integration tests in crates/observability-http/tests/
Phase 5: Architecture guards
Phase 6: Byte-identical post-extraction verification
Phase 7: Evidence record + promotion housekeeping
```

## Phase 0: Audit

Capture pre-extraction baseline so behavior-preservation
gates promotion.

**Tasks:**

**0.1** Enumerate all functions and constants in
`crates/toron/src/network/observability_http.rs`. Confirm
the public surface matches what the shared crate will
expose (D1):

- `read_request<S>(stream: &mut S) -> std::io::Result<Vec<u8>>`
- `parse_request_line(raw: &[u8]) -> std::io::Result<RequestLine<'_>>`
- `write_response<S>(stream: &mut S, status, content_type, body)`
- `write_request<S>(stream: &mut S, method, path, host)`
- `read_response<S>(stream: &mut S) -> std::io::Result<HttpResponse>`
- `MAX_REQUEST_HEADER_BYTES`, `MAX_RESPONSE_HEADER_BYTES`,
  `MAX_RESPONSE_BODY_BYTES` constants.
- `RequestLine<'a>`, `HttpResponse` types.
- Toron-local: `READ_REQUEST_TIMEOUT`.
- Confirm `CONNECT_TIMEOUT` lives in
  `crates/toron/src/cli.rs` per ADR 046 guard 12.

**0.2** Capture diagnostic strings emitted by the helpers
(D8). For each `Err(Error::new(_, "..."))` /
`Err(Error::new(_, format!("...")))` site, record the exact
text and verify it matches the iter-3 ADR §D8 17-string
enumeration:

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

If Toron's current emitted strings drift from the 17-string
list, Phase 0 reports the drift and Phase 2 picks one of:

- (a) Adopt the 17 strings as canonical (Toron-side migration
  of diagnostic text — falls within behavior-preserving scope
  if Toron's prior tests pinned the older bytes; not
  acceptable if they did).
- (b) Retain Toron's actual strings; update the promoted
  ADR D8 enumeration to reflect the actual bytes (recorded
  as a tiny post-promotion ADR amendment).

Expected outcome is option (a): the 17-string list was
drafted from Toron's source.

**0.3** Inventory all consumers of the Toron helpers:

- `crates/toron/src/network/observability.rs` — server-side
  `/health` / `/ready` / `/metrics` handlers.
- `crates/toron/src/cli.rs` — `inspect` subcommand client.
- Test fixtures in `crates/toron/tests/` and any inline
  `#[cfg(test)]` modules.

**0.4** Capture prior-cycle architecture guard inventory:
list all function names matching `fn adr04[2-7]_*` in
`crates/toron/tests/architecture_test.rs`. Capture pass-count
from `cargo test -p toron --tests architecture_test`.

**0.5** Capture ADR 045 / 046 integration fixture pass-count
from running their specific named fixtures:

- ADR 045 observability fixtures (~9-10 fixtures).
- ADR 046 inspect fixtures (~10-12 fixtures including
  iter-3 sharpening).

**0.6** Confirm Wendelon's HTTP helpers are exactly where
D5 / guard 11 assume:

- `crates/wendelon/src/metrics/observability.rs:488` —
  `async fn read_request<S: AsyncRead + Unpin>(stream: &mut S)
  -> std::io::Result<String>`.
- `crates/wendelon/src/metrics/observability.rs:506` —
  `fn parse_request_line(req: &str) -> std::io::Result<(&str, &str)>`.
- `crates/wendelon/src/metrics/observability.rs:520` —
  `async fn write_response<S: AsyncWrite + Unpin>(stream: &mut S,
  status: &str, content_type: &str, body: &str)`.
- `crates/wendelon/src/metrics/exporter.rs:35` —
  `async fn handle_connection(mut stream: TcpStream)` with
  inline `content-type: text/plain` lowercase response
  strings.

**Acceptance:**

- Baseline test output captured for ADR 045/046 fixture
  pass-count and ADR 040-047 architecture-test pass-count.
- Diagnostic-string inventory verified against the 17-string
  D8 enumeration; drift (if any) triaged before Phase 2.
- All consumer call-sites identified.
- Wendelon-side helpers confirmed at expected source paths.

## Phase 1: Crate skeleton

Create the new workspace crate.

**Tasks:**

**1.1** Create `crates/observability-http/Cargo.toml`:

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

**1.2** Create `crates/observability-http/src/lib.rs`:

```rust
//! Bounded HTTP/1.1 helpers for observability listeners and
//! inspect-style clients. Hand-rolled, intentionally narrow.
//!
//! This crate does not own routing, TLS, status semantics,
//! timeouts, metrics, audit, or daemon lifecycle. Callers
//! compose those locally.

#![deny(unsafe_code)]

pub mod request;
pub mod response;
pub mod writer;

pub const MAX_REQUEST_HEADER_BYTES: usize = 8 * 1024;
pub const MAX_RESPONSE_HEADER_BYTES: usize = 8 * 1024;
pub const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;

pub use request::{RequestLine, parse_request_line, read_request};
pub use response::{HttpResponse, read_response};
pub use writer::{write_request, write_response};
```

**1.3** Create empty `request.rs`, `response.rs`, `writer.rs`
placeholders (each module body filled in Phase 2).

**1.4** Confirm root `Cargo.toml` already has `crates/*`
workspace glob (verified during ADR 047 promotion). No edit
needed.

**Acceptance:** `cargo build -p observability-http` succeeds
with empty module bodies.

## Phase 2: Move helpers from Toron to observability-http

Move the parser/writer implementations from Toron to the
new crate. Preserve diagnostic strings byte-identical to
the iter-3 D8 17-string enumeration.

**Tasks:**

**2.1** Move `read_request` (with terminator-detection loop)
into `crates/observability-http/src/request.rs`. Preserve
diagnostic strings:

- `connection closed before HTTP headers completed`
- `HTTP request headers exceed 8 KiB`

**2.2** Move `parse_request_line` + the `RequestLine<'a>`
struct into `crates/observability-http/src/request.rs`. The
struct fields stay `pub method`, `pub path`, `pub version`
(D1). Preserve diagnostic strings:

- `missing HTTP request line`
- `missing HTTP method`
- `missing HTTP path`
- `missing HTTP protocol version`

`parse_request_line` rejects missing parts but does NOT
validate that `version == "HTTP/1.1"` (caller-owned per D8
implementation notes).

**2.3** Move `write_response` (with Title-Case headers
`Content-Type` / `Content-Length` / `Connection: close`)
into `crates/observability-http/src/writer.rs`. Emit format:

```
HTTP/1.1 <status>\r\n
Content-Type: <content_type>\r\n
Content-Length: <body.len()>\r\n
Connection: close\r\n
\r\n
<body>
```

**2.4** Move `write_request` (with `<method> <path>
HTTP/1.1\r\nHost: <host>\r\nConnection: close\r\n\r\n`
shape) into the same file.

**2.5** Move `read_response` + the `Content-Length` parsing
+ chunked-response rejection + body cap enforcement +
`HttpResponse` struct into
`crates/observability-http/src/response.rs`. Preserve
diagnostic strings:

- `connection closed before HTTP response headers completed`
- `HTTP response headers exceed 8 KiB`
- `missing HTTP response status line`
- `missing HTTP response version`
- `unsupported HTTP response version`
- `missing HTTP response status code`
- `malformed HTTP response status code`
- `malformed HTTP response header`
- `chunked HTTP response bodies are not supported`
- `missing Content-Length header`
- `HTTP response body exceeds 64 KiB`

`read_response` rejects non-`HTTP/1.1` response versions
(matches current Toron inspect behavior). If both `Transfer-
Encoding: chunked` and `Content-Length` are present, chunked
rejection wins.

**2.6** Diagnostic-string verification: for each diagnostic
moved in 2.1-2.5, confirm the emitted bytes match the
Phase 0 audit list byte-identical. A test fixture (Phase 4
fixtures 2, 4, 7, and 8) verifies representative diagnostic
paths at runtime; Phase 6.4 verifies the full 17-string set.

**Acceptance:**

- `cargo build -p observability-http` succeeds.
- The crate's source contains no `Duration`, `tokio::time`,
  `timeout`, `READ_REQUEST_TIMEOUT`, `CONNECT_TIMEOUT`,
  `tls_pki`, `quinn`, `rustls`, `tracing`, `bytes`,
  `tokio-util`, `httparse` references (architecture guard 4
  preview).
- Toron's `observability_http.rs` body is empty at this
  point (Phase 3 fills it with shim).

## Phase 3: Convert Toron module to compatibility shim

Wire Toron's existing import paths to the shared helpers via
re-exports, keeping `READ_REQUEST_TIMEOUT` Toron-local.

**Tasks:**

**3.1** Rewrite `crates/toron/src/network/observability_http.rs`:

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

**3.2** Add `observability-http = { path = "../observability-http" }`
to `crates/toron/Cargo.toml` dependencies (sorted
alphabetically among existing entries).

**3.3** Update Toron's observability route-matching call
sites for the new `RequestLine` return shape. The imports
stay on `crate::network::observability_http::{...}` through
the shim, but match arms that currently destructure
`(method, path)` must bind `let line = parse_request_line(...)`
and compare `line.method` / `line.path`. Confirm the inspect
call site in `crates/toron/src/cli.rs` still needs no
route-pattern rewrite because it uses only `write_request`
and `read_response`.

**3.4** Verify `CONNECT_TIMEOUT` stays in
`crates/toron/src/cli.rs` (its ADR 046 source location).
ADR 048 does NOT move it; ADR 046 guard 12
(`adr046_inspect_has_tcp_connect_timeout`) source-text-
matches `cli.rs` and continues to pass byte-identical.

**Acceptance:**

- `cargo build -p toron` succeeds.
- `cargo test -p toron --tests architecture_test` passes
  for ADR 040-047 guards (ADR 048 guards added in Phase 5).
- Toron call-site imports unchanged.
- ADR 045 guard 4 source-text-match for
  `READ_REQUEST_TIMEOUT` in
  `crates/toron/src/network/observability_http.rs` passes.
- ADR 046 guard 12 source-text-match for `CONNECT_TIMEOUT`
  in `crates/toron/src/cli.rs` passes.

## Phase 4: Integration tests in observability-http

Create `crates/observability-http/tests/substrate_test.rs`
covering the 9 substrate fixtures (fixtures 1-9 from the
ADR §Integration Fixtures section).

**Tasks:**

**4.1** Implement fixture 1
(`observability_http_reads_request_until_header_terminator`):
synthesize a stream that delivers headers in two halves;
assert `read_request` reads until `\r\n\r\n`.

**4.2** Implement fixture 2
(`observability_http_rejects_request_headers_over_limit`):
synthesize 9 KiB of header bytes; assert `Err(InvalidData)`
with byte-identical diagnostic `HTTP request headers exceed
8 KiB`.

**4.3** Implement fixture 3
(`observability_http_parses_request_line_with_version_field`):
parse `b"GET /ready HTTP/1.1\r\nHost: ..."`; assert
`line.method == "GET"`, `line.path == "/ready"`,
`line.version == "HTTP/1.1"`. Happy-path only.

**4.4** Implement fixture 4
(`observability_http_rejects_missing_request_line_parts`):
three sub-asserts:

- Parse `b"\r\nHost: ..."` (missing request line) → diagnostic
  `missing HTTP request line` byte-identical.
- Parse `b" /ready HTTP/1.1\r\n..."` (missing method) →
  diagnostic `missing HTTP method` byte-identical.
- Parse `b"GET  HTTP/1.1\r\n..."` (missing path) → diagnostic
  `missing HTTP path` byte-identical.
- Parse `b"GET /ready\r\n..."` (missing version) → diagnostic
  `missing HTTP protocol version` byte-identical.

**4.5** Implement fixture 5
(`observability_http_writes_titlecase_response_headers`):
call `write_response(&mut buf, "200 OK", "application/json",
"{}")`; assert the resulting bytes contain
`\r\nContent-Type: application/json\r\n`,
`\r\nContent-Length: 2\r\n`,
`\r\nConnection: close\r\n\r\n{}`. Byte-level equality.

**4.6** Implement fixture 6
(`observability_http_reads_content_length_response`):
synthesize a stream with `HTTP/1.1 200 OK\r\nContent-Type:
text/plain\r\nContent-Length: 5\r\n\r\nhello`; assert
`HttpResponse { status_code: 200, content_type:
Some("text/plain"), body: b"hello".to_vec() }`.

**4.7** Implement fixture 7
(`observability_http_rejects_chunked_response`):
synthesize a response with `Transfer-Encoding: chunked`;
assert byte-identical diagnostic `chunked HTTP response
bodies are not supported`.

**4.8** Implement fixture 8
(`observability_http_rejects_response_body_over_limit`):
synthesize a response with `Content-Length: 65537`; assert
byte-identical diagnostic `HTTP response body exceeds 64
KiB` (rejection on length, not on body read — preserves
current Toron behavior).

**4.9** Implement fixture 9
(`observability_http_writes_get_request_with_host_and_close`):
call `write_request(&mut buf, "GET", "/ready",
"toron-x.cluster.local:9443")`; assert bytes equal
`b"GET /ready HTTP/1.1\r\nHost: toron-x.cluster.local:9443\r\nConnection: close\r\n\r\n"`.

**Acceptance:**

- `cargo test -p observability-http` passes all 9 substrate
  fixtures.

## Phase 5: Architecture guards

Add 12 ADR 048 architecture guards + 1 convention guard to
`crates/toron/tests/architecture_test.rs`.

**Tasks:**

**5.1** Guard 1
(`adr048_observability_http_crate_exists_under_workspace_glob`):
assert `crates/observability-http/Cargo.toml` exists; assert
root `Cargo.toml` contains `"crates/*"`. Mirrors ADR 047
guard 1 pattern.

**5.2** Guard 2
(`adr048_observability_http_depends_only_on_tokio_io`): read
`crates/observability-http/Cargo.toml`; assert presence of
the `tokio` line with `features = ["io-util"]`; assert
absence of every forbidden token from D6.

**5.3** Guard 3
(`adr048_observability_http_has_bounded_header_and_body_limits`):
read `crates/observability-http/src/lib.rs`; assert presence
of the three `const MAX_*` lines with their exact byte
limits (`8 * 1024`, `8 * 1024`, `64 * 1024`).

**5.4** Guard 4
(`adr048_observability_http_does_not_own_timeouts`):
collect source-text across `crates/observability-http/src/`;
assert absence of `Duration`, `tokio::time`, `timeout`,
`READ_REQUEST_TIMEOUT`, `CONNECT_TIMEOUT`.

**5.5** Guard 5
(`adr048_observability_http_public_surface_is_small`): read
source-text across `crates/observability-http/src/`; assert
presence of 10 required tokens and absence of 12 forbidden
tokens:

```rust
let combined = format!("{lib_rs}{request_rs}{response_rs}{writer_rs}");

// Required surface tokens.
for required in [
    "pub const MAX_REQUEST_HEADER_BYTES",
    "pub const MAX_RESPONSE_HEADER_BYTES",
    "pub const MAX_RESPONSE_BODY_BYTES",
    "pub struct RequestLine",
    "pub struct HttpResponse",
    "pub async fn read_request",
    "pub fn parse_request_line",
    "pub async fn read_response",
    "pub async fn write_request",
    "pub async fn write_response",
] {
    assert!(combined.contains(required),
        "observability-http must expose {required}");
}

// Forbidden surface tokens.
for forbidden in [
    "pub struct Router",
    "pub struct Listener",
    "pub fn listen",
    "pub fn dispatch",
    "pub fn route",
    "pub struct StatusCode",
    "pub enum Status",
    "rustls::",
    "quinn::",
    "tls_pki::",
    "tracing::",
    "TcpListener",
    "TcpStream",
] {
    assert!(!combined.contains(forbidden),
        "observability-http must not expose {forbidden}");
}
```

**5.6** Guard 6
(`adr048_toron_observability_http_module_is_shim_only`):
read `crates/toron/src/network/observability_http.rs`;
assert presence of `pub use observability_http::` re-export
+ `READ_REQUEST_TIMEOUT` constant; assert absence of
parser-state identifiers (`header_end`, `content_length`,
`body_len`, parser `loop {`).

**5.7** Guard 7
(`adr048_toron_observability_and_inspect_use_shared_http_helper`):
grep `crates/toron/src/network/observability.rs` and
`crates/toron/src/cli.rs` for `observability_http::`
references; assert presence in both.

**5.8** Guard 8 (`adr048_no_new_toron_observability_routes`):
collect source-text of
`crates/toron/src/network/observability.rs`; assert path
constant inventory matches the ADR 045 set exactly
(`"/health"`, `"/ready"`, `"/metrics"` — no additions).

**5.9** Guard 9
(`adr048_no_nilket_root_runtime_dependency_change`): read
`crates/nilket/Cargo.toml`; assert presence of byte-stable
manifest fields (no new dependencies introduced as part of
ADR 048).

**5.10** Guard 10
(`adr048_no_wendelon_inspect_branch_this_cycle`): read
`crates/wendelon/src/cli.rs`; assert absence of `"inspect"`
arm in the CLI dispatch and absence of `WendelonInspect`
types.

**5.11** Guard 11
(`adr048_wendelon_observability_http_helpers_remain_local`):
read `crates/wendelon/src/metrics/observability.rs`; assert
presence of `async fn read_request<S`, `fn parse_request_line(`,
`async fn write_response<S`. Read
`crates/wendelon/src/metrics/exporter.rs`; assert presence
of `async fn handle_connection(` + lowercase
`content-type: text/plain` response strings. Assert
absence of `use observability_http::` in both Wendelon
production files.

**5.12** Guard 12
(`adr048_prior_adr04x_guards_continue_to_pass`): mirror
ADR 047's meta-guard. Read
`crates/toron/tests/architecture_test.rs`; assert presence
of `fn adr04[2-7]_` prefixes (existence inventory). Mirror
ADR 047 guard 10's existence-check shape, not a baseline-
snapshot comparison. Prior-cycle guard function names stay
stable; assertion content may relax only when a literal
moves behind a shared helper or re-export and a new ADR
guard pins the new storage site.

**5.13** Convention guard
(`adr048_new_observability_http_types_are_brand_clean_and_no_v1_suffix`):
collect source-text of `crates/observability-http/src/`;
assert absence of `ToronHttp`, `WendelonHttp`, `NilketHttp`,
`HttpResponseV1`, `RequestLineV1`, `ObservabilityHttpV1`.

**Acceptance:**

- `cargo test -p toron --tests architecture_test` passes
  all 12 new ADR 048 guards + 1 convention guard.
- All prior-cycle ADR 040-047 guards still pass.

## Phase 6: Byte-identical post-extraction verification

Run the complete pre/post comparison. Promotion gates on
this.

**Tasks:**

**6.1** Run ADR 045 observability integration fixtures
(`/ready`, `/metrics`, `/health` HTTP-shape, request-cap,
chunked reject, etc.); assert pass-count matches Phase 0
audit.

**6.2** Run ADR 046 inspect integration fixtures (HTTP/1.1
client, 200/503 closed status, exit-code mapping,
`Host:`/`Connection: close` shape, connect-timeout,
response-read timeout); assert pass-count matches Phase 0
audit.

**6.3** Run ADR 040-047 architecture-test sweep; assert
pass-count matches Phase 0 audit.

**6.4** Diagnostic-string post-extraction check: capture
the bytes emitted by each diagnostic-emitting code path
(e.g., synthesize a 9 KiB request header) and compare with
Phase 0 inventory. Byte-identical required for all 17
strings.

**6.5** If any pre/post divergence surfaces, **stop**.
Triage:

- If divergence is in `observability-http` diagnostic
  strings → adjust string literals to match Phase 0 audit.
- If divergence is in Toron call-site behavior → confirm
  shim re-exports are wired correctly.
- If divergence is in a prior-cycle guard's source-text
  expectation because a literal storage site moved behind
  the shared crate/shim → keep the prior guard function
  name stable, minimally relax the assertion to the new
  re-export/helper boundary, and add an ADR 048 guard that
  pins the shared storage site. For any other divergence,
  adjust shim layout to preserve the prior expectation.

**6.6** Re-run after triage; confirm full pre/post
equality.

**Acceptance:**

- ADR 045 + ADR 046 integration fixtures pass-count
  byte-identical pre/post.
- ADR 040-047 architecture-test function names and pass
  count preserved pre/post.
- 17 diagnostic strings byte-identical pre/post.
- New ADR 048 fixtures (Phase 4) + new architecture guards
  (Phase 5) all pass.

## Phase 7: Evidence record + promotion housekeeping

**Tasks:**

**7.1** Commit the staged extraction in one atomic commit
(or two: substrate-add + Toron-shim, but prefer atomic for
review clarity).

**7.2** Verify the promoted ADR + impl docs already exist:

- `crates/toron/plans/adr/0048_shared_observability_http_workspace_crate.md`
  (Status: Promoted; pairs with `plans/impl/048_*.md`).
- `crates/toron/plans/impl/0048_shared_observability_http_workspace_crate.md`
  (Status: Promoted; pairs with `plans/adr/048_*.md`).

**7.3** Verify `crates/toron/plans/wip/roadmap.md` has:

- Status section: ADR 048 bullet after ADR 047's.
- Forward sequence: `### ADR 048 promoted; ADR 049
  candidates` section at top.
- Demoted `### ADR 047 promoted; ADR 048 candidates`
  to `(HISTORICAL)`.

**7.4** Optional same-window housekeeping (per ADR 048
iter-0/iter-2 §"Out of scope" / iter-3 §"Optional
housekeeping"):

- Reconcile ADR 047 D8 wording in `roadmap.md` Status
  bullet + Forward section if any wrapper-pattern wording
  remains; it should match the as-built trust-chain-helper
  shape per the promoted ADR 047 doc.
- Verify `plans/impl/047_*.md` reflects the as-built D8
  shape (`build_client_cert_chain_verifier` +
  `build_server_cert_chain_verifier`; consumers compose
  policy wrappers locally). Update if stale.

These are roadmap-cleanup tasks, not ADR 048 promotion
gates — non-blocking but useful in the same commit window.

**Acceptance:**

- Atomic commit lands `crates/observability-http/`,
  modified Toron files, and updated `architecture_test.rs`.
- ADR 048 ADR + impl docs in `plans/adr/` and `plans/impl/`
  with Promoted status (already materialized at promotion
  time).
- Roadmap updated (already materialized at promotion time).
- ADR 047 D8 reconciliation done or scheduled as a
  follow-up.

## Risk register

| Risk | Mitigation |
|------|-----------|
| Diagnostic-string byte-drift between Toron's pre-extraction strings and the 17-string D8 enumeration | Phase 0.2 verifies match; triage Phase 2 if drift surfaces |
| ADR 045 guard source-text expectations break if shim layout differs from pre-extraction | Phase 3.1 explicitly preserves `READ_REQUEST_TIMEOUT` at the same file path. If a literal moves behind the shared crate/re-export, Phase 6.5 allows minimal assertion relaxation while ADR 048 guards pin the new shared storage site |
| ADR 046 guard 12 source-text-match breaks if `CONNECT_TIMEOUT` moves | Phase 3.4 explicitly leaves `CONNECT_TIMEOUT` in `cli.rs` (its ADR 046 location) |
| `RequestLine.version` exposure creates future API churn | D1 documents that Toron's parser already requires a third request-line token; exposing that token as a field keeps version policy caller-owned without adding a status or route surface |
| Tokio dependency conflict if `tokio.workspace = true` is configured | D6 specifies local dependency with `io-util` only, bypassing workspace feature unification |
| Wendelon-side guard 11 source-text drift if Wendelon helpers are renamed during a parallel commit | Phase 5.11 pins specific function-signature substrings; if Wendelon refactors concurrently, guard 11 must be updated alongside |
| Phase 6.1-6.3 pass-count divergence due to unrelated flaky test | Re-run before triage; isolate flake from extraction-caused divergence |
| Architecture guard 5 too strict if a future module adds a new symbol matching a forbidden token (e.g., `TcpStream` inside test-only code) | Phase 5.5 uses substring matching across `src/` only (not `tests/`); test-only types are exempt |
| Module split (lib.rs + request.rs + response.rs + writer.rs) creates inter-module visibility complications | Phase 1.2 module declarations use `pub mod`; Phase 2 re-uses pub items via the lib.rs `pub use` re-exports — flat consumer surface preserved |

## Phase totals

- 8 phases (0-7).
- 1 new workspace crate, ~600-800 LOC across 4 source
  files (lib.rs + request.rs + response.rs + writer.rs).
- 1 modified Toron module (becomes ~15-line shim).
- 1 modified Toron `Cargo.toml` (one new path dependency
  line).
- 12 new architecture guards + 1 convention guard.
- 9 substrate fixtures (in
  `crates/observability-http/tests/substrate_test.rs`).
- 3 preservation/sweep fixtures (in
  `crates/toron/tests/architecture_test.rs` and existing
  fixture infrastructure):
  - `toron_observability_ready_metrics_preserve_responses_post_extraction`
  - `toron_inspect_preserves_ready_fetch_post_extraction`
  - `prior_adr04x_architecture_guards_pass_post_http_extraction`
- Zero new operator-visible behavior.
- Zero changes to `crates/nilket/`.
- Zero changes to `crates/wendelon/src/metrics/`.
- Zero changes to `crates/wendelon/src/cli.rs`.
- Zero changes to ADR 045 / 046 / 047 source files outside
  of Toron's shim conversion.

## Acceptance checklist

- [ ] Phase 0 audit captures pre-extraction baseline; 17-
      string diagnostic verification passes.
- [ ] Phase 1 crate skeleton compiles.
- [ ] Phase 2 helpers moved with byte-identical diagnostic
      strings.
- [ ] Phase 3 Toron module is a thin shim; ADR 045 guard 4
      + ADR 046 guard 12 source-text-match preserved.
- [ ] Phase 4 substrate fixtures pass (9 fixtures including
      iter-3 fixture 4 missing-parts-reject).
- [ ] Phase 5 architecture guards added and passing (12 +
      1 convention; includes iter-3 guard 5 public-surface-
      is-small).
- [ ] Phase 6 pre/post comparison verified (ADR 045 +
      ADR 046 fixtures byte-identical, ADR 040-047
      architecture guard names/pass count preserved, 17
      diagnostic strings byte-identical).
- [ ] Phase 7 evidence record committed; ADR + impl +
      roadmap promoted.
- [ ] Optional: ADR 047 D8 reconciliation in roadmap.

Ready for implementation.
