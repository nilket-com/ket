# ADR 071: Nilket Wendelon Inspect Real Wiring

**Status:** Promoted
**Seed:** Toron operator-plane roadmap ADR 049-candidate slot
(`nilket wendelon inspect` parity, LEAD) — drafted through the
Toron council process as candidate ADR 049, then relocated to
the Wendelon sequence as ADR 071 because Wendelon owns the
consumer ADR (the implementation + fixtures land in
`crates/wendelon`, and the server-side surface it builds on
was specced in Wendelon ADR 027 observability posture). ADR 046
(`nilket toron inspect`) is the compute-ring shape mirrored;
ADR 047 `tls-pki` + ADR 048 `observability-http` are the shared
cross-ring substrates consumed; Wendelon's existing mTLS
`/ready` listener (`metrics/observability.rs:239`) is the
target.
**Council:** Claude, Codex, Gemini (3 iterations via the Toron
operator-plane council process: iter-0 3-of-3 convergence on
Shape A `nilket wendelon inspect` parity; iter-1 Codex four
sharpenings + Claude eight sharpenings (S1-S8) correcting two
Gemini factual errors (endpoint `7432` not `7781`; empty ALPN
not `http/1.1`); iter-2 dual ADR+impl convergence Codex+Claude
2-of-3 with Claude R1 retraction (verified ADR 046 is
body-authoritative, not status-code-driven, at
`crates/toron/src/cli.rs:254-257`); iter-3 Codex final
ratification adding the prior-cycle meta-guard (guard 16) +
compact-JSON text rendering; promoted from iter-3 with
Codex+Claude 2-of-3 convergence). Gemini iter-3 agreed on the
topic and broad shape but retained two factual errors (endpoint
`7781`, `READ_REQUEST_TIMEOUT` naming), both corrected against
source for promotion.
**Category:** Wendelon ops/observability CLI composition ADR —
client-only, behavior-mirroring Toron's ADR 046 to the storage
ring; zero daemon-side changes. Composes over the shared
cross-ring substrates (ADR 047 / ADR 048) and Wendelon's own
observability posture (ADR 027). Not a storage-engine
continuation.

## Summary

Add `nilket wendelon inspect` — a one-shot, client-only CLI
that dials Wendelon's existing mTLS `/ready` listener, sends
one `GET /ready` request via ADR 048's `observability-http`
client helpers, parses Wendelon's existing readiness JSON
into a narrow internal `ReadinessView`, renders text by
default or byte-preserved JSON with `--json`, and exits with
the same closed 0/1/2 readiness/error scheme as Toron's
ADR 046.

ADR 071 is a **pure composition cycle** — zero new Wendelon
daemon endpoint, zero `/ready` schema change, zero
actor-permission change, zero server-side observability
helper migration, zero generic HTTP client, zero new
substrate. It consumes ADR 047 `tls-pki` + ADR 048
`observability-http` + Wendelon's existing `/ready` endpoint
(Wendelon ADR 027 observability posture) and actor-certificate
admission.

**Wendelon ops/observability CLI composition.** First
client-side consumer of the ADR 047 + ADR 048 substrates from
the storage ring (Wendelon was a server-side `tls-pki`
consumer; Toron's inspect was the first `observability-http`
client consumer). Completes the operator-plane symmetric
inspect surface: after ADR 071, `nilket toron inspect`
(Toron ADR 046) + `nilket wendelon inspect` (this ADR) are
symmetric across rings.

## Motivation

Toron's ADR 046 delivered `nilket toron inspect` for the
compute ring. Wendelon (storage ring) has the substrate — an
mTLS `/ready` listener serving `ReadinessSnapshot` JSON
(`metrics/observability.rs:239`, admitting
`ActorPermission::ObservabilityReady`, established under
Wendelon ADR 027 observability posture) — but no operator
CLI; operators must use `curl` / `nc` for readiness
diagnostics.

ADR 048 was deliberately pulled before this cycle so the
Wendelon inspect client would consume the shared
`observability-http` helpers (`write_request`,
`read_response`) instead of creating a fourth hand-rolled
HTTP/1.1 site. ADR 071 validates both prior cross-ring
substrate cycles immediately: Wendelon becomes the second
direct client-side consumer of `observability-http` and a
cross-ring client-side consumer of `tls-pki`.

The inspect command mirrors ADR 046's operator contract while
adapting to three Wendelon-specific realities:

1. **Different readiness payload.** Wendelon's
   `ReadinessSnapshot` (`metrics/observability.rs:52`) carries
   storage-ring fields (`node_id`, `epoch`, `leader_id`,
   `cluster_members`, `drain_state`,
   `audit_materialization_lag_entries`, `degraded_reason`,
   optional `fips_mode` / `fips_self_test_last_ts`), not
   Toron's compute-ring fields. No single `ready` field; the
   readiness predicate is `is_ready()` =
   `degraded_reason.is_none() && audit_materialization_lag_entries == 0`
   (`metrics/observability.rs:535`).
2. **Different admission model.** Wendelon admits `/ready` by
   `ActorPermission::ObservabilityReady` (principal-based via
   `authorize_actor`), not by Toron's `monitor-` SAN prefix.
3. **Different ALPN posture.** Wendelon's observability
   listeners advertise no ALPN (`crates/wendelon/src/cli.rs:162`
   builds the observability server config with `Vec::new()`
   ALPN). `HRFT_ALPN = "wendelon-hrft"` is data-plane-only.

## Decisions (15, Promotion-Locked)

**D1: Lift `nilket wendelon inspect`.**

Add an `inspect` branch to `wendelon::cli::run`
(`crates/wendelon/src/cli.rs`):

```text
nilket wendelon inspect [--endpoint <addr>]
                        [--server-name <name>]
                        [--cert <path>]
                        [--key <path>]
                        [--ca <path>]
                        [--json]
```

Dispatch arm follows the existing match-arm pattern
(`[cmd, rest @ ..] if cmd == "inspect" => run_inspect(rest)`),
ordered before the single-arg `[config_path]` daemon arm so a
bare `inspect` reports the missing-TLS-flags diagnostic rather
than booting a daemon. The legacy `wendelon` binary reaches
the same grammar through `wendelon::cli::run`. Help text names
`inspect`.

**D2: Client-only, daemon-zero.**

The inspect path creates a Tokio runtime, opens one TCP
connection, performs one mTLS handshake, sends one HTTP/1.1
request, reads one response, renders once, and exits. It must
not construct `WendelonNode`, `NodeBootstrapConfig`, Raft
state, storage managers, observability listeners, audit sinks,
or readiness reporters. No new daemon endpoint, no readiness
schema change, no listener migration.

**D3: Endpoint and TLS precedence are pinned.**

Endpoint precedence: `--endpoint` >
`WENDELON_INSPECT_ENDPOINT` > compiled default
`127.0.0.1:7432`. The default matches the example Wendelon
`/ready` listener (`crates/wendelon/examples/wendelon.toml.example`:
`ready = "0.0.0.0:7432"`). Endpoint is a `SocketAddr`, not a
URL; HTTPS is implicit.

TLS field precedence: flag > env. Environment variables
(mirroring Toron's `TORON_INSPECT_*` shape, verified at
`crates/toron/src/cli.rs:207-227`):

```text
WENDELON_INSPECT_ENDPOINT
WENDELON_INSPECT_SERVER_NAME
WENDELON_INSPECT_CLIENT_CERT
WENDELON_INSPECT_CLIENT_KEY
WENDELON_INSPECT_CA
```

All four TLS fields (`--server-name`, `--cert`, `--key`,
`--ca`) must be present together. Missing any one fails before
dialing with exit code 2 and the pinned diagnostic:

```text
wendelon inspect requires --server-name, --cert, --key, and --ca
```

No loopback plaintext fallback (Wendelon's `/ready` is
mTLS-only). No certificate path auto-discovery. No URL parser.

**D4: Explicit server name only.**

Operators pass Wendelon's existing canonical node DNS SAN
explicitly: `node-<node_id>.<cluster_domain>`. ADR 071 does
not add `--node-id` / `--cluster-domain` sugar — this keeps
Wendelon inspect parallel to ADR 046's explicit SNI contract
and avoids a second server-name construction path.

**D5: Wendelon actor authorization, not Toron monitor
identity.**

The client certificate is a Wendelon actor certificate
authorized by the existing server-side policy for
`ActorPermission::ObservabilityReady`
(`network/tls.rs:416`). ADR 071 does **not** add `monitor-`
or `inspect-` DNS SAN prefixes, a new `ActorPermission`, a
new `ActorRole`, actor-claims CBOR changes, revocation
changes, or audit-policy changes. The existing server-side
admission layer decides.

**D6: Use Wendelon's existing TLS wrapper and empty ALPN.**

The client construction path:

```rust
let material = crate::network::tls::TlsMaterial::load(cert, key, ca)?;
let client_config = crate::network::tls::rustls_client_config_with_alpn(
    &material, Vec::new(),
)?;
let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
```

The empty ALPN vector is intentional and matches Wendelon's
observability server (`crates/wendelon/src/cli.rs:162` builds
the server config with `Vec::new()` ALPN). Advertising
`http/1.1` would be a daemon-side listener-policy change and
is out of scope. Wendelon's `TlsMaterial` newtype wrapper
preserves `std::io::Result<Self>` (ADR 047 D3); the CLI
consumes the wrapper, not raw `tls-pki`.

This is a deliberate cross-ring divergence from ADR 046:
Toron's inspect client advertises `http/1.1` (Toron's listener
gates on it); Wendelon's inspect client advertises no ALPN.

**D7: Consume ADR 048's shared HTTP client helpers.**

The inspect path uses
`observability_http::{write_request, read_response}`. ADR 071
adds `observability-http = { path = "../observability-http" }`
to `crates/wendelon/Cargo.toml`. Wendelon already has generic
HTTP crates (`reqwest` / `h2` / optional `http`) for OTLP and
other surfaces; ADR 071 forbids their use **in the inspect
path** (architecture guard 8, source-text scoped to the
inspect region) rather than forbidding their manifest
presence.

**D8: Readiness is derived from the existing Wendelon body.**

ADR 071 does not add a `ready` JSON field. The CLI derives a
narrow `ReadinessView` from existing `/ready` body fields.
Required base fields: `node_id`, `epoch`, `leader_id`,
`cluster_members`, `drain_state`,
`audit_materialization_lag_entries`, `degraded_reason`.
Optional fields: `fips_mode`, `fips_self_test_last_ts`.

Readiness rule (mirrors the server's `is_ready()`):

```text
ready    iff degraded_reason == null AND audit_materialization_lag_entries == 0
degraded otherwise
```

Missing or malformed required base fields → exit 2 with
`wendelon inspect: failed to parse readiness snapshot:
<details>`. The CLI parses into a narrow internal
`ReadinessView` (via `serde_json::Value` + checked field
extraction, resilient to conditional FIPS fields); it does
**not** duplicate Wendelon's server-side `ReadinessSnapshot`
type.

**D9: HTTP status policy mirrors ADR 046 (body-authoritative).**

HTTP 200 and HTTP 503 are the only expected statuses. For
those statuses, the **body-derived** readiness view (D8)
determines exit 0 vs exit 1. Any other status → exit 2 with
`wendelon inspect: failed to read /ready response: unexpected
HTTP status <code>`.

This mirrors ADR 046's verified behavior
(`crates/toron/src/cli.rs:254-257` reads `snapshot.ready`;
`:349-350` gates on 200/503). No status/body mismatch policy
is added — the server derives status from the same predicate,
so mismatches are synthetic.

**D10: Output contract is pinned (compact-JSON values).**

`--json`: write the response body byte-identical to stdout +
one trailing newline. No pretty-print, normalization, field
reordering, or envelope.

Default (text): line-per-field rendering in Wendelon's
`ReadinessSnapshot` declaration order, with each value
rendered as its **compact JSON value** after the `field: `
label:

```text
node_id: "n-01"
epoch: 7
leader_id: "n-01"
cluster_members: ["n-01"]
drain_state: "none"
audit_materialization_lag_entries: 0
degraded_reason: null
```

(Strings quoted, numbers/booleans bare, `null` for absent
optionals, compact JSON arrays. This preserves ADR 046's
JSON-style value rendering rule: optional strings are quoted
when present, and Wendelon visibly has more string-typed fields
than Toron's common bool/number fields.)

FIPS fields appended after `degraded_reason` **only when
present** in the body (omit-when-absent):

```text
fips_mode: true
fips_self_test_last_ts: 1710000000
```

Successful (exit 0) and degraded (exit 1) paths write only
stdout. Diagnostics go to stderr only on exit 2; never to
stdout.

**D11: Exit codes and diagnostics mirror ADR 046.**

| Exit | Meaning |
|------|---------|
| 0 | HTTP 200/503, valid body, body-derived ready |
| 1 | HTTP 200/503, valid body, body-derived degraded |
| 2 | usage / config / connectivity / TLS / HTTP / JSON / timeout / unexpected-status error |

Pinned diagnostics (mirror ADR 046 with the ring name
swapped):

```text
wendelon inspect requires --server-name, --cert, --key, and --ca
wendelon inspect: failed to connect to endpoint: <details>
wendelon inspect: failed to connect to endpoint within timeout
wendelon inspect: failed to initialize mTLS client: <details>
wendelon inspect: failed to complete TLS handshake: <details>
wendelon inspect: failed to write request: <details>
wendelon inspect: failed to receive response within timeout
wendelon inspect: failed to parse HTTP response: <details>
wendelon inspect: failed to read /ready response: unexpected HTTP status <code>
wendelon inspect: failed to parse readiness snapshot: <details>
```

**D12: Timeout policy is Wendelon-local.**

```rust
// crates/wendelon/src/cli.rs
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const READ_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
```

Both constants live in `crates/wendelon/src/cli.rs`
(caller-owned). `observability-http` remains timeout-free
(ADR 048 D3). The response-read constant is named
`READ_RESPONSE_TIMEOUT` (not Toron's `READ_REQUEST_TIMEOUT`)
because this is a genuinely client-side response read and the
constant is fresh Wendelon-local; accuracy wins over
cross-ring name continuity since the constant isn't shared.

**D13: One-shot only.**

No `--watch`, streaming, retry loop, periodic polling,
multi-node aggregation, or cluster-wide readiness summary.
Mirrors ADR 046 D11 / Toron ADR 043 D17.

**D14: Wendelon server-side observability helpers stay local.**

ADR 048 D5 deferred Wendelon server-side migration to
`observability-http` because parser shape, request-line return
shape, and response header casing diverge. ADR 071 preserves
that deferral. Wendelon's existing server-side `read_request`,
`parse_request_line`, and `write_response` remain in
`metrics/observability.rs`; `metrics/exporter.rs` inline
helpers remain. No production Wendelon module imports
`observability_http` except the new inspect path in `cli.rs`.

**D15: Root runtime-free invariant preserved.**

`crates/nilket/Cargo.toml` is byte-identical pre/post (the
nilket dispatcher invariant from Toron ADR 040 D4 / ADR 042
D2 / ADR 043 D2 / ADR 045 D9 / ADR 046 D10 / ADR 047 / ADR 048).
`nilket` keeps dispatching to `wendelon::cli::run`; all Tokio
/ TLS / JSON / HTTP client work stays in `crates/wendelon`.

**Single-plan posture.** No Plan A / Plan B framing. No shared
inspect-CLI helper extraction with Toron (Toron's inspect is a
closed ADR 046 surface; sharing would expand scope and
re-touch ADR 046 code). Wendelon ships a parallel local
implementation consuming the shared **substrates**, not a
shared **CLI helper**.

## Architecture Guards (16, Promotion-Locked)

Implemented in `crates/wendelon/tests/architecture_test.rs`
(Wendelon's own architecture-guard location), except any
explicitly cross-ring invariant that genuinely needs
workspace-level inspection.

1. `adr071_wendelon_inspect_branch_exists` — `wendelon::cli`
   dispatch + help text name `inspect`.
2. `adr071_wendelon_inspect_is_client_only` — inspect path
   does not construct `WendelonNode`, `NodeBootstrapConfig`,
   storage managers, Raft state, observability listeners, or
   audit sinks.
3. `adr071_wendelon_inspect_endpoint_precedence_is_pinned` —
   source pins `WENDELON_INSPECT_ENDPOINT` + default
   `127.0.0.1:7432`.
4. `adr071_wendelon_inspect_tls_flags_are_all_or_none` —
   source pins the all-four diagnostic.
5. `adr071_wendelon_inspect_uses_existing_actor_authorization`
   — no new `ActorPermission`, no `Inspect` permission, no
   `monitor-`/`inspect-` SAN prefix; `ObservabilityReady`
   remains the server admission point.
6. `adr071_wendelon_inspect_uses_empty_alpn` — inspect TLS
   construction uses an empty ALPN vector; no `HRFT_ALPN` or
   `http/1.1` in the inspect path.
7. `adr071_wendelon_inspect_uses_observability_http_client_helpers`
   — inspect source references
   `observability_http::write_request` +
   `observability_http::read_response`.
8. `adr071_wendelon_inspect_does_not_use_generic_http_clients`
   — the inspect region of `cli.rs` does not reference
   `reqwest`, `hyper`, `h2`, `http::Request`,
   `http::Response`, `tower`, `ureq` (source-text scoped to
   the inspect region, not whole-file, since Wendelon uses
   `reqwest` elsewhere for OTLP).
9. `adr071_wendelon_inspect_accepts_only_200_and_503` — any
   other HTTP status maps to the pinned unexpected-status
   diagnostic.
10. `adr071_wendelon_inspect_exit_codes_are_pinned_to_0_1_2`
    — closed three-code mapping; no `ExitCode::from(3)`.
11. `adr071_wendelon_inspect_text_and_json_output_are_pinned`
    — stable text field order (compact-JSON values), JSON
    pass-through, stdout/stderr split.
12. `adr071_wendelon_inspect_has_symmetric_timeouts` — both
    `CONNECT_TIMEOUT` and `READ_RESPONSE_TIMEOUT` =
    `Duration::from_secs(5)` in `cli.rs`.
13. `adr071_no_new_wendelon_observability_routes` — no new
    `*_PATH` constants and no `/inspect` route in
    `metrics/observability.rs`.
14. `adr071_wendelon_observability_server_helpers_remain_local`
    — ADR 048 D5 deferral intact: Wendelon's server-side
    `read_request` / `parse_request_line` / `write_response`
    remain local; no `observability_http` import in
    `metrics/observability.rs` or `metrics/exporter.rs`.
15. `adr071_nilket_root_stays_runtime_free` —
    `crates/nilket/Cargo.toml` unchanged.
16. `adr071_prior_adr_guards_continue_to_pass` — meta-guard
    following the ADR 047/048 refactor-guard convention:
    prior-cycle Wendelon architecture-guard function names
    continue to exist and the guard sweep passes; not a
    byte-snapshot of every assertion body.

**Post-promotion convention guard:**

- `adr071_new_inspect_types_are_brand_clean_and_no_v1_suffix`
  — new internal names are semantic (`InspectOptions`,
  `InspectError`, `ReadinessView`) and use no `WendelonInspect*`
  prefix or `*V1` suffix. Preserves the ongoing no-V1-suffix
  convention shared across the nilket operator-plane work.

## Integration Fixtures (15 required + 2 V1-evidence optional)

Implemented under `crates/wendelon/tests/` (inline unit tests
in `crates/wendelon/src/cli.rs::tests` where no server is
needed).

**Required (gates promotion):**

1. `wendelon_inspect_endpoint_env_and_flag_precedence` —
   endpoint + TLS env vars + flag override precedence.
2. `wendelon_inspect_requires_complete_tls_options` — missing
   any TLS field fails before dialing with the stable
   diagnostic.
3. `wendelon_inspect_writes_ready_request_with_host_header` —
   byte-level assertion for `GET /ready
   HTTP/1.1\r\nHost: <endpoint>\r\nConnection: close\r\n\r\n`.
4. `wendelon_inspect_preserves_json_body_with_json_flag` —
   `--json` emits the response body byte-identical + one
   newline.
5. `wendelon_inspect_text_renderer_omits_absent_fips_fields` —
   non-FIPS body renders without `fips_mode` /
   `fips_self_test_last_ts`.
6. `wendelon_inspect_text_renderer_appends_present_fips_fields`
   — FIPS body appends both fields after `degraded_reason`.
7. `wendelon_inspect_exit_zero_on_body_ready` — `degraded_reason
   == null && lag == 0` → exit 0.
8. `wendelon_inspect_exit_one_on_degraded_reason` — non-null
   `degraded_reason` → exit 1.
9. `wendelon_inspect_exit_one_on_audit_materialization_lag` —
   `audit_materialization_lag_entries > 0` with null
   `degraded_reason` → exit 1 (proves body-derived readiness,
   not string-only degraded detection).
10. `wendelon_inspect_accepts_200_and_503_as_expected_statuses`
    — both statuses parse and map per body.
11. `wendelon_inspect_unexpected_status_returns_exit_two` —
    HTTP 418 → pinned unexpected-status diagnostic, exit 2.
12. `wendelon_inspect_connect_timeout_maps_to_stable_error` —
    deterministic timeout fixture (pending-connect helper),
    not host-routing-dependent.
13. `wendelon_inspect_response_read_timeout_maps_to_stable_error`
    — deterministic slow-response fixture → pinned
    receive-timeout diagnostic, exit 2.
14. `wendelon_inspect_actor_authorization_round_trip` — real
    mTLS: actor cert with `ObservabilityReady` succeeds; actor
    cert without it is rejected by the existing server
    admission layer. Uses
    `crates/wendelon/tests/support/tls_pki.rs` generated-cert
    helpers.
15. `wendelon_inspect_uses_observability_http_substrate` —
    source/runtime-backed assertion that inspect consumes ADR
    048 helpers instead of adding a Wendelon-local client
    parser.

**Optional V1-evidence (non-gating, post-promotion):**

- `wendelon_inspect_end_to_end_against_bootstrapped_node` —
  full inspect against a bootstrapped Wendelon node with
  generated certs and real observability listeners.
- `wendelon_inspect_accepts_break_glass_diagnostic_scope` —
  break-glass diagnostic-scope cert accepted for `/ready`.

## Sub-Lift Posture

**Single-plan.** Wendelon-local CLI composition. No shared
inspect-CLI helper extraction with Toron (a future small
refactor ADR can extract one if a third inspect consumer or a
`--watch` mode surfaces pressure).

## Out of Scope

- New Wendelon daemon-side wire surface (`/inspect` endpoint,
  richer detail, readiness JSON field additions, `/info`,
  `/version`).
- Shared inspect-CLI helper extraction between Toron +
  Wendelon.
- Wendelon server-side `observability-http` migration (ADR 048
  D5 still defers this).
- New `ActorPermission` / `ActorRole` / SAN prefix / actor-
  claims CBOR / cert issuance / revocation / audit-policy
  change.
- `--node-id` / `--cluster-domain` server-name sugar.
- `--watch` / streaming / retry / cluster aggregation.
- `reqwest`-based inspect implementation.
- Generic cross-ring inspect abstraction.
- Toron CLI consolidation; multi-tenant routing; OTLP traces;
  audit subsystem; live cert rotation.

## Council History

Drafted through the Toron operator-plane council process as
candidate ADR 049; relocated to the Wendelon sequence as
ADR 071 at promotion (Wendelon owns the consumer ADR; the code
lands in `crates/wendelon`, and the `/ready` surface it builds
on was specced in Wendelon ADR 027).

**Iter-0 (3-of-3 convergence on Shape A):**
- Codex: detailed `nilket wendelon inspect` draft (11
  decisions, 13 guards, 10+2 fixtures).
- Claude: Shape A lead with 7 open questions.
- Gemini: Shape A with initial 4-decision set.

**Iter-1:**
- Codex: four sharpenings (body-authoritative readiness, FIPS
  omit-when-absent, explicit `--server-name`, required
  actor-authz fixture).
- Claude: eight sharpenings (S1-S8) correcting two Gemini
  factual errors — endpoint `7432` (not `7781`), empty ALPN
  (not `http/1.1`), both verified against source.
- Gemini: refined D-set; retained the two factual errors.

**Iter-2 (Codex+Claude convergence):**
- Claude R1: retracted iter-1 S3 (status-code-driven exit).
  Verified ADR 046 is body-authoritative
  (`crates/toron/src/cli.rs:254-257`). Adopted Codex's D6/D9.
- D-set converged; guard + fixture sets expanded.

**Iter-3 (Codex+Claude 2-of-3, promoted):**
- Codex iter-3 added guard 16 (prior-cycle meta-guard,
  ADR 047/048 convention) + compact-JSON text rendering (D10).
- Claude adopted both refinements for promotion; JSON-style
  value rendering is ADR 046-compatible and pins Wendelon's
  string fields deterministically.
- Gemini iter-3 agreed on topic + broad shape; retained
  endpoint `7781` + `READ_REQUEST_TIMEOUT` naming, both
  corrected against source.

**Accepted (Codex+Claude 2-of-3, topic 3-of-3):**
- `nilket wendelon inspect` parity, CLI-only composition.
- Reuse `tls-pki` + `observability-http`; Wendelon-local
  policy.
- Body-authoritative readiness mirroring ADR 046.
- Exit codes 0/1/2; symmetric 5s timeouts; default endpoint
  `127.0.0.1:7432`; empty ALPN; `ObservabilityReady`
  admission.
- Compact-JSON text rendering; `--json` byte-passthrough.
- Zero daemon-side surface.

**Corrected for promotion (Gemini factual errors):**
- Default endpoint `127.0.0.1:7432` (not `7781`) — verified
  against `examples/wendelon.toml.example`.
- Response-read timeout named `READ_RESPONSE_TIMEOUT` (Codex+
  Claude) — client-side accurate.
- Empty ALPN (not `http/1.1`) — verified against
  `crates/wendelon/src/cli.rs:162`.

**Sequence relocation (at promotion):**
- Filed in the Toron operator-plane sequence as candidate
  ADR 049 during the council process, then relocated to the
  Wendelon sequence as ADR 071 because the deliverable is a
  Wendelon-ring feature (all code in `crates/wendelon`) whose
  server-side counterpart is Wendelon ADR 027. The Toron
  roadmap cross-references this ADR as the consumer that
  completes the symmetric operator-plane inspect surface.

## Promotion criteria met

1. **D-set locked at 15.**
2. **Guard set locked at 16 + 1 convention** (in
   `crates/wendelon/tests/architecture_test.rs`).
3. **Fixture set locked at 15 required + 2 V1-evidence
   optional.**
4. **Single-plan posture.**
5. **Cross-ring discipline:** inspect consumes shared
   substrates; ring-specific admission/rendering stays
   Wendelon-local.
6. **No-V1-suffix convention preserved** (shared nilket
   operator-plane convention).
7. **Toron ADR 042-048 + Wendelon ADR 027 invariants
   preserved; daemon-side zero.**
8. **No Toron runtime or inspect implementation changes** (the
   Toron operator-plane mirror is a cross-reference, not shared
   code; Toron plan files may reference this Wendelon ADR).
9. **Wendelon ops/observability CLI composition; completes the
   operator-plane symmetric inspect surface.**

## Conclusion

ADR 071 is a tight Wendelon-local CLI composition cycle: one
`inspect` subcommand consuming ADR 047 `tls-pki` + ADR 048
`observability-http`, dialing Wendelon's existing `/ready`
endpoint (ADR 027 observability posture). Body-authoritative
readiness mirroring Toron's ADR 046; compact-JSON text
rendering; empty-ALPN client config matching Wendelon's
listener; `ObservabilityReady` admission. Zero daemon-side
surface. Completes the symmetric operator inspect surface
across both rings: `nilket toron inspect` (Toron ADR 046) +
`nilket wendelon inspect` (this ADR).

The eight-phase migration is materialized in
`plans/impl/071_nilket_wendelon_inspect_real_wiring.md`.
