# ADR 045: Toron Daemon Observability Listeners

**Status:** Promoted
**Seed:** ADR 044 D9 (deferred observability listeners as ADR 045
lead candidate); roadmap §"ADR 045 lead candidate"
(`crates/toron/plans/wip/roadmap.md`); Wendelon prior art
(`crates/wendelon/src/metrics/observability.rs`).
**Council:** Claude, Codex, Gemini (4 iterations: iter-0 topic-shape
survey with Shape A composition over ADR 044 recommended over Shapes
B-E; iter-1 Claude three retractions — R1 committed to hand-rolled
HTTP/1.1 over `tokio_rustls` after Wendelon helper-shape audit, R2
`/health` body `"OK\n"` → `"ok\n"` byte-identical with Wendelon, R3
metric scrape counting narrowed to `/metrics` only while keeping
metric names brand-clean, later implementation review records R4
dropping Wendelon-style project-prefixed metric names entirely;
iter-2
D-set / guards / fixtures locked with three preemptive sharpenings —
D1 port-collision validation, D4 HTTP method handling pinned to
404 for unknown paths and 405 for unsupported methods, D6
low-cardinality drain-state semantics surfaced;
iter-3 DoS-hardening sharpening — D4 5-second per-request read
timeout preventing slowloris, slow-client fixture + guard 12; iter-4 final
ratification, no new content).
**Category:** Composition ADR over ADR 044 (operator-facing
observability surface; no new substrate).

## Summary

Lift ADR 044 D9 reservation. Add three HTTP/1.1 listeners under a
new `[observability.listeners]` config section: `/health` (plaintext,
loopback-only, always); `/ready` and `/metrics` (mTLS-gated via
reused ADR 044 `ListenerPolicy`). Extend ADR 044's `admit_san`
exact-shape verifier with a new `monitor-<principal>.<cluster_domain>`
SAN prefix; admission set becomes `<prefix>` ∈
{`toron`, `client`, `monitor`}. Add hand-rolled HTTP/1.1 helpers
(`crates/toron/src/network/observability_http.rs`) mirroring
Wendelon's `read_request` / `write_response` / `parse_request_line`
shape; no hyper / axum / actix-web dependency. Per-request read
operations wrap a 5-second `tokio::time::timeout` to prevent
slowloris-style DoS. Add `ReadinessSnapshot` with `is_ready()` /
`http_status()` / `to_json()` derivation pattern parallel to
Wendelon's `ReadinessSnapshot`; Toron-appropriate low-cardinality
fields (no raft, no daemon identity fields).
Append fourth stdout line `OBSERVABILITY_READY health=<addr>
ready=<addr> metrics=<addr>` after ADR 042 `READY <worker>` and (if
configured) ADR 043 `SQL_READY <sql>`. Tight initial Prometheus 0.0.4
text exposition surface with functional, brand-clean metric names.

**No** histograms. **No** per-tenant labels. **No** `RevocationSnapshot`,
`AuditSink`, OTLP traces, or `ActorPermission` model carry-over from
Wendelon. **No** HTTP/2 / HTTP/3. **No** OpenMetrics protobuf format.
**No** hyper / axum / actix-web. **No** keep-alive / pipelining /
chunked encoding. **No** changes to ADR 042 / 043 / 044 invariants.

## Motivation

ADR 044 shipped authenticated remote QUIC listeners; operators can
now dial Toron from real consoles. But they cannot observe daemon
health from a monitoring agent or Kubernetes liveness probe. The
first deploy-shaped operator concern after authentication is "is the
daemon up and ready?"

ADR 045 is the smallest cycle that ships observability listeners
without introducing audit subsystems, revocation surfaces, OTLP push
exporters, or actor-permission registries. It composes purely over
ADR 044 — no new substrate.

Wendelon-parity is half-paid: Wendelon's `/health`, `/ready`,
`/metrics` shapes (path constants, body literal `"ok\n"`, hand-rolled
HTTP helpers, `ReadinessSnapshot.to_json()` derivation pattern,
scrape-counting selectivity, and `tokio_rustls` TLS posture) all
transfer cleanly. The non-carry-overs are explicit:
no Wendelon-internal pattern is dragged into Toron through
observability composition.

## Decisions (10, Promotion-Locked)

**D1: Add `[observability.listeners]` config section.**

The existing `[observability]` tracing section remains. ADR 045 adds
a nested optional listeners table and, because it touches this source
surface, folds in the brand-clean config rename:
`ToronObservabilityConfig` -> `ObservabilityConfig` and
`ToronTracingFormat` -> `TracingFormat`.

```toml
[observability.listeners]
health = "127.0.0.1:7780"
ready  = "127.0.0.1:7781"
metrics = "127.0.0.1:7782"
```

Three separate bind addresses (mirrors Wendelon's
`RawObservabilityListeners` shape at
`crates/wendelon/src/config.rs:596-601`). Missing
`[observability.listeners]` keeps ADR 042 / 043 / 044 behavior
byte-identical (no observability listener bound).

All three bind addresses must be present-or-absent together. Stable
diagnostic on partial config:

```text
observability listeners require health, ready, and metrics bind addresses together
```

**Port-collision validation:** config-load checks that all five
`SocketAddr`s — `[worker].task_listener_bind`,
`[sql].listener_bind` (if `[sql]` present),
`[observability.listeners].health`, `[observability.listeners].ready`,
`[observability.listeners].metrics` — are pairwise distinct (by
`(ip, port)` tuple). Stable diagnostic on collision:

```text
listener bind addresses must be pairwise distinct
```

**Example-config comment block** (rendered by `render_example_config()`)
documents the canonical `monitor-<principal>.<cluster_domain>` SAN
provisioning recipe inline.

**D2: `/health` plaintext + `"ok\n"` body byte-identical with Wendelon; `/ready` + `/metrics` require ADR 044 TLS.**

`/health` is unauthenticated, always plaintext HTTP/1.1, always
loopback-only (regardless of `[tls]` presence). Returns:

```text
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8
Content-Length: 3
Connection: close

ok
```

Body literal `"ok\n"` (lowercase, three bytes). Byte-identical with
Wendelon `crates/wendelon/src/metrics/observability.rs:225`.

`/ready` and `/metrics` admission:

| Bind          | `[tls]` present | Outcome |
| ------------- | --------------- | ------- |
| loopback      | no              | **reject** with stable diagnostic |
| loopback      | yes             | mTLS HTTPS |
| non-loopback  | no              | **reject** with stable diagnostic |
| non-loopback  | yes             | mTLS HTTPS |

Stable diagnostics (brand-clean, no project-name prefix — applies
ADR 044's brand-clean lesson):

```text
observability ready and metrics listeners require authenticated transport
observability health listener must bind a loopback address
```

The second diagnostic fires for any non-loopback `health` bind even
if `[tls]` is configured. `/health` admits no non-loopback shape in
V1. Ready and metrics remain mTLS-protected even on loopback so the
deployment-state and load surfaces do not grow a second plaintext
policy branch.

**D3: `monitor-` SAN prefix; first divergence from Wendelon's actor model.**

Extends ADR 044's `admit_san` exact-shape verifier:
`<prefix>` ∈ {`toron`, `client`, `monitor`}.

`monitor-<principal>.<cluster_domain>` is the scraper identity SAN.
`<principal>` matches `[A-Za-z0-9-]+` (same id-token rule as ADR 044
D5).

Per-listener SAN admission:

- `/health` — no client cert required (plaintext path).
- `/ready` — admits `monitor-` only.
- `/metrics` — admits `monitor-` only.

Same exact-shape attack-rejection guarantees as ADR 044 D5
(subdomain injection, wildcard abuse, underscore confused-deputy,
malformed id_token).

**Explicit Wendelon-divergence:** Wendelon uses `ActorPermission` +
`ActorTrustRoots` + `authorize_actor`
(`crates/wendelon/src/metrics/observability.rs:254,307,407`). Toron
does **not** carry this over because:

1. Toron's ADR 044 admission model is SAN-prefix → role; introducing
   `ActorPermission` is a substrate-class addition with no V1
   expressiveness gain (one extra indirection between SAN and
   admission decision).
2. Wendelon's `ActorPermission` is intertwined with revocation
   snapshots; ADR 044 D7 excluded revocation from Toron V1.
3. The roles ADR 045 needs are expressible with three lines of
   match on SAN prefix in `admit_san`.

Operator cost acknowledged: Toron+Wendelon clusters provision
scrapers with different identity-shape per ring (Toron:
`monitor-<principal>.<cluster_domain>` SAN; Wendelon: actor
permission via trust roots). Documented as a non-carry-over.

**D4: Hand-rolled HTTP/1.1 over `tokio_rustls`; no hyper / axum / actix; 5-second read timeout.**

Helpers (`read_request`, `write_response`, `parse_request_line`) live
at `crates/toron/src/network/observability_http.rs` (Toron-local under
ADR 044 Plan W-C; may move to a shared `tls-pki` workspace crate if
the W-B follow-up ADR extracts both TLS + HTTP helpers).

Mirror Wendelon's helper shape at the function-signature level.
**No** `hyper`, **no** `http`, **no** `axum`, **no** `actix-web`.

**HTTP method handling:** only GET is handled. Non-GET requests on
`/health`, `/ready`, or `/metrics` return:

```text
HTTP/1.1 405 Method Not Allowed
Content-Type: text/plain; charset=utf-8
Content-Length: 19
Connection: close

method not allowed
```

Body literal `"method not allowed\n"` (19 bytes). Unknown paths on
any of the three listeners return 404 with body literal
`"not found\n"` (10 bytes). This is a narrow Toron divergence from
Wendelon's fallthrough because ADR 045 has exactly three pinned GET
routes and can report method-vs-path errors without pulling in a web
framework.

**Per-request read timeout (iter-3 sharpening — DoS hardening):**
each handler wraps `read_request` in a 5-second
`tokio::time::timeout`:

```rust
pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

let req = match timeout(READ_REQUEST_TIMEOUT, read_request(&mut stream)).await {
    Ok(Ok(req)) => req,
    Ok(Err(_)) => return,    // malformed/oversized → close without response
    Err(_) => return,        // slowloris → close without response
};
```

On timeout fire: connection closed without writing a response body
(no agreed-upon framing since the client never completed a request).
5 seconds accommodates kube-probe + Prometheus scrape RTTs with
margin while preventing slowloris hold-open against externally
reachable `/ready` / `/metrics` listeners. The same timeout applies
to `/health` for symmetry.

`read_request` itself caps headers at 8 KiB; oversized headers return
`ErrorKind::InvalidData`. **No** HTTP/1.1 keep-alive, **no** chunked
encoding, **no** request body handling, **no** HTTP/2 / HTTP/3.

**D5: Prometheus 0.0.4 text format; functional brand-clean metric names.**

Content-Type: `text/plain; version=0.0.4; charset=utf-8`. **No**
OpenMetrics protobuf. **No** histograms in V1.

V1 metric surface uses functional names. It does not repeat the
project name; this applies ADR 044's no-project-name-in-protocols
lesson to metrics.

```text
daemon_ready
daemon_uptime_seconds
listener_up{listener="worker"}
listener_up{listener="sql"}
listener_up{listener="health"}
listener_up{listener="ready"}
listener_up{listener="metrics"}
listener_accepts_total{listener="sql"}
listener_rejects_total{listener="metrics",reason="san_role_rejected"}
metrics_scrapes_total
descriptors_loaded
static_workers_configured
```

Counted only on `/metrics` (not `/ready`); parallel to Wendelon's
selectivity at `crates/wendelon/src/metrics/observability.rs:319`.

**Pinned `{reason}` label values (closed enum):** `mtls_handshake_failure`,
`peer_unknown_ca`, `san_shape_rejected`, `san_role_rejected`,
`bind_unavailable`, `request_timeout`. Stable cardinality ≤6 per
label.

**No** per-tenant labels (preserves ADR 043 D9 / ADR 044 D4
server-stamped-tenant invariant). Multi-tenant labeling defers to
the future ADR that introduces multi-tenant routing.

`queries_in_flight` and `tasks_in_flight` may be added only if the
implementation wires them honestly around existing execution seams.
ADR 045 must not ship placeholder zero gauges.

**D6: `/ready` JSON snapshot mirrors Wendelon `to_json()` shape; drain-state surfaced.**

`ReadinessSnapshot` shape:

```rust
pub struct ReadinessSnapshot {
    pub ready: bool,
    pub worker_listener_bound: bool,
    pub sql_listener_bound: bool,
    pub observability_listeners_bound: bool,
    pub descriptors_loaded: u64,
    pub static_workers_configured: u32,
    pub uptime_seconds: u64,
    pub degraded_reason: Option<String>,
}
```

`to_json()` emits a stable JSON object in the above field order; the
implementation must not depend on map iteration order unless the
ordered-map feature is explicitly enabled. `is_ready()` returns true
iff `ready` is true, listeners are bound, and
`degraded_reason.is_none()`.
`http_status()` returns
`"200 OK"` or `"503 Service Unavailable"`. Content-Type:
`application/json; charset=utf-8`. Structurally parallel to Wendelon's
`crates/wendelon/src/metrics/observability.rs:539-602`.

No `tenant_id`, `worker_id`, query ID, task ID, table name, SQL text,
certificate subject, endpoint address, or worker UUID appears in the
readiness body. The TLS server name identifies the daemon; the
readiness document stays low-cardinality.

**Drain-state semantics:** when the daemon receives `SIGTERM` (or
`ToronDaemon::shutdown()` is called) and shutdown begins:

1. `degraded_reason` is immediately set to `Some("draining")`.
2. `/ready` returns 503 + JSON body with `degraded_reason: "draining"`.
3. `/health` continues to return 200 (the daemon process is still
   alive; liveness ≠ readiness).
4. New worker-task / SQL connections continue to drain per ADR 042
   D11 (existing connections drain for `drain_timeout_seconds`, then
   force-abort).

Operator value: Kubernetes rolling deployments route traffic away
from a draining pod within one `/ready` poll interval (typically
5-10s); in-flight queries finish; pod terminates cleanly.

`degraded_reason` is an enum-of-strings (closed set, no free-form):
`"draining"`, `"descriptor_registry_empty"`, `"worker_task_listener_unbound"`,
`"sql_listener_unbound"`, `"observability_listeners_unbound"`. Future
ADRs may extend.

**D7: Wendelon-pattern non-carry-overs.**

Four explicit non-carry-overs:

1. **No `RevocationSnapshot`** threaded into `/ready` or `/metrics`
   verifiers. Preserves ADR 044 D7 (no runtime-mutable revocation in
   Toron V1).
2. **No `AuditSink` emission per scrape.** Wendelon emits AuditEvent
   on every `/ready` and `/metrics` access; Toron has no audit
   subsystem and ADR 045 doesn't introduce one. `tracing` log lines
   at INFO level suffice for V1 operator forensics (preserves ADR
   042 D13 tracing-only-observability stance).
3. **No OTLP traces export.** Wendelon's `[observability.traces]`
   subsystem is a separate concern; Toron V1 declines.
4. **No Wendelon-style project-prefixed metric names.** Wendelon's
   `wendelon_<noun>_<unit>` metric family convention predates ADR
   044's no-project-name-in-protocols rule. Toron starts with
   functional brand-clean names from D5 (`daemon_ready`,
   `listener_up{...}`, etc.) and guard 10 forbids `toron_`.

Guard #7 asserts the runtime-scope non-carry-overs through a
forbidden-import source-text check on the observability module:

```rust
for forbidden in [
    "RevocationSnapshot", "AuditSink", "AuditEvent",
    "opentelemetry", "OtlpExporter",
] {
    assert!(!src.contains(forbidden), "forbidden import: {forbidden}");
}
```

**D8: `OBSERVABILITY_READY` stdout line format pinned.**

Single line appended after `READY <worker>` and (if configured)
`SQL_READY <sql>`:

```text
OBSERVABILITY_READY health=<addr> ready=<addr> metrics=<addr>
```

Format pinned byte-identical: keys `health`, `ready`, `metrics` in
that order; each value is the bound `<ip>:<port>` form (matches ADR
042 D12 / ADR 043 D16 `<endpoint>` convention). Source-text guard
asserts the format string appears exactly once in `daemon.rs`.

ADR 042 D12 `READY <worker>` and ADR 043 D16 `SQL_READY <sql>` lines
preserved byte-identical (ADR 044 D8 invariant extended).

**D9: Reuse ADR 044's one-cert-per-daemon invariant.**

The daemon's `toron-<worker_id>.<cluster_domain>` cert (from ADR 044
D11) serves as TLS server identity for `/ready` and `/metrics`
listeners too. Scraper cert (`monitor-` SAN) is operator-provisioned
separately as a client identity. **No** `[observability.tls]`
independent config section. Extends ADR 044 D11 / guard 13 to a
fourth listener class.

**D10: Tight initial metric surface; no histograms; no per-tenant labels.**

V1 metric surface enumerated in D5. **No** Prometheus histogram
metrics (storage cost + bucket-schema lock-in deferred). **No**
per-tenant labels (preserves server-stamped-tenant invariant).

Operators wanting histograms aggregate counter deltas client-side;
ADR 047+ can widen the surface once V1 shape is proven.

## Architecture Guards (12, Promotion-Locked)

Added to `crates/toron/tests/architecture_test.rs`:

1. `adr045_observability_section_is_optional_for_backward_compat` —
   guards D1 backward compat; ADR 042 / 043 / 044 minimal configs
   continue to load byte-identical.
2. `adr045_health_listener_is_loopback_only_always` — guards D2;
   `/health` rejects non-loopback bind regardless of TLS posture.
3. `adr045_ready_and_metrics_require_tls_when_configured` — guards
   D2; the two protected endpoints require shared ADR 044 TLS for
   loopback and non-loopback binds.
4. `adr045_monitor_san_prefix_admission_is_per_listener` — guards D3;
   the `monitor-` prefix is admitted only on `/ready` and `/metrics`;
   reuses ADR 044's `admit_san` exact-shape verifier extended with
   the new prefix.
5. `adr045_observability_listeners_use_hand_rolled_http_no_hyper_no_axum`
   — guards D4; source-text check ensures the observability module
   doesn't import hyper / axum / actix-web.
6. `adr045_metrics_uses_prometheus_text_format_no_histograms` —
   guards D5; metrics module emits text exposition; no `_bucket`
   suffixed series in the registry.
7. `adr045_no_revocation_no_audit_no_traces_in_observability` —
   guards D7; runtime-scope forbidden-import source-text guard.
8. `adr045_observability_ready_line_is_appended_not_replaced` —
   guards D8; source-text invariant that `OBSERVABILITY_READY` is
   emitted **after** `READY` and `SQL_READY`; previous lines remain
   byte-identical.
9. `adr045_one_tls_material_set_per_daemon_still_holds` — guards D9;
   extends ADR 044 guard 13.
10. `adr045_initial_metric_set_is_functional_brand_clean_and_stable` —
    guards D5 + D10; source-text check that rendered metric names are
    functional, contain no `toron_` prefix, and carry no per-tenant
    labels.
11. `adr045_ready_snapshot_excludes_high_cardinality_identity_fields`
    — guards D6; `/ready` JSON excludes worker identity, tenant
    identity, endpoint, query, task, SQL text, certificate, and table
    fields.
12. `adr045_observability_http_has_per_request_read_timeout` —
    guards D4 iter-3 sharpening; source-text assertion that
    `READ_REQUEST_TIMEOUT` is declared and `tokio::time::timeout` is
    invoked at the `read_request` call sites.

Post-promotion convention guard:

- **`adr045_new_observability_types_are_brand_clean_and_no_v1_suffix`**
  — tenth consecutive cycle preserving the no-V1-suffix convention
  (ADRs 039 / 041 / 042 / 043 / 044 / 045).

## Integration Fixtures (10 required + 2 V1-evidence optional)

**Required (gates promotion):**

1. `toron_config_missing_observability_section_loads_byte_identical_to_adr044` —
   ADR 042 / 043 / 044 example configs still load unchanged.
2. `toron_health_endpoint_returns_200_ok_with_lowercase_ok_body` —
   asserts byte-identical `"ok\n"` body.
3. `toron_health_endpoint_rejects_non_loopback_bind` — config-time
   rejection regardless of TLS posture.
4. `toron_ready_and_metrics_require_tls_when_configured` —
   config-time witness for D2's stricter TLS requirement even on
   loopback.
5. `toron_config_observability_port_collision_rejected` — supports
   D1 port-collision sharpening.
6. `toron_ready_endpoint_emits_json_snapshot_with_expected_fields_in_stable_order`
7. `toron_metrics_endpoint_emits_prometheus_text_format_with_brand_clean_names`
8. `toron_ready_and_metrics_reject_peer_without_monitor_san_prefix`
   — folds wrong-CA, wrong-prefix, malformed-SAN rejection variants
   into one handshake-time admission gate.
9. `toron_observability_ready_stdout_line_appended_after_sql_ready_with_pinned_format`
10. **`toron_observability_listener_closes_slow_client_after_read_timeout`**
   — iter-3 sharpening witness; open a TCP connection to `/health`,
   write 5 bytes, pause 6 seconds; expect connection closed by
   server without HTTP response.

**Optional V1-evidence (non-gating, post-promotion):**

- `toron_metrics_endpoint_returns_expected_counter_after_synthetic_workload`
  — synthesize 3 SQL accepts + 1 rejection; scrape `/metrics`;
  assert counter values match.
- `toron_ready_endpoint_returns_503_during_drain` — exercise D6
  drain-state sharpening; SIGTERM the daemon; assert `/ready` flips
  to 503 + `degraded_reason: "draining"` within one poll interval.

## Sub-Lift Posture

**Single-plan.** No Plan A / Plan B framing. The cycle ships:

- one shared `[observability.listeners]` config section composing
  over ADR 044's `[tls]` policy;
- three HTTP/1.1 listeners with role-differentiated TLS posture;
- one `monitor-` SAN prefix extension to ADR 044's `admit_san`;
- hand-rolled HTTP/1.1 helpers (no hyper / axum / actix);
- 5-second per-request read timeout (slowloris hardening);
- one `ReadinessSnapshot` with `is_ready()` / `http_status()` /
  `to_json()` derivation pattern mirroring Wendelon's;
- drain-state semantics (`/ready` returns 503 with
  `degraded_reason: "draining"` immediately on SIGTERM);
- tight initial brand-clean metric surface with closed-cardinality
  labels;
- explicit Wendelon-pattern non-carry-overs (no revocation, no
  audit, no OTLP, no `ActorPermission`, no project-prefixed metric
  names).

Matches ADR 044 single-plan posture. Pure composition over ADR 044
— no new substrate.

## Out of Scope (Recorded for Future ADRs)

- **`nilket toron inspect` real wiring composing over `ReadinessSnapshot`.**
  **ADR 046 lead candidate** — last ADR 040 reservation lift; small
  composition cycle.
- **OTLP traces export** (`[observability.traces]`). Future ADR.
- **Audit-sink emission per scrape.** Wendelon's pattern; not in
  Toron V1.
- **Revocation surface (CRL / OCSP / `RevocationSnapshot`).** Carries
  forward ADR 044 D7 deferral.
- **`ActorPermission` model** parallel to Wendelon's. Future ADR if
  per-listener role admission needs to evolve beyond SAN prefix.
- **Prometheus histogram metrics.** Future surface-widening once V1
  shape is proven.
- **OpenMetrics protobuf format.** Prometheus 0.0.4 text sufficient
  for V1.
- **Per-tenant metric labels.** Bundles with multi-tenant routing
  (ADR 047+).
- **Distributed tracing context propagation across worker task
  dispatch.** Substrate cycle.
- **Aggregate cluster-wide readiness across many daemons.** Operator
  / monitoring-layer concern.
- **Log aggregation / forwarding.** Operator concern.
- **Cross-host `/health`.** `/health` is loopback-only by D2;
  cross-host readiness is `/ready`'s job.
- **HTTP/2 / HTTP/3 / pipelining / keep-alive** for observability
  listeners. V1 is GET-per-connection over HTTP/1.1.
- **Extract shared `tls-pki` workspace crate** (ADR 044 Plan W-B
  follow-up). HTTP helpers may move there alongside TLS helpers when
  the extraction ADR ships.

## Council History

- **Iter 0:** Claude proposed Shape A (observability listeners as
  composition over ADR 044), surveyed Shapes B (refactor-only
  `tls-pki` extraction), C (multi-tenant routing), D (`inspect`
  wiring), E (live cert rotation / revocation). Took positions on
  hand-rolled-vs-hyper (deferred to Phase 0), `monitor-` SAN prefix,
  no audit / revocation / OTLP. Predicted Codex would converge on
  Shape A and hand-rolled; Gemini would push for OTLP / audit
  bundling (predicted 2-of-3 against).
- **Iter 1:** Claude retracted three iter-0 positions after code
  audit of Wendelon's observability handlers
  (`crates/wendelon/src/metrics/observability.rs:21,124,221-237,
  239-289,292-340,539-545,547-602`): R1 — committed to hand-rolled
  HTTP/1.1 (no hyper); R2 — `/health` body `"OK\n"` → `"ok\n"`
  byte-identical with Wendelon; R3 — scrape counter
  scrape counting narrowed to `/metrics` only while preserving
  brand-clean metric names.
  Four sharpenings: D6 `/ready` JSON shape mirrored from Wendelon's
  `to_json()` pattern with stable field ordering; D3
  acknowledged as first SAN-prefix divergence from Wendelon's
  `ActorPermission`; D7 runtime-scope forbidden-import guard; D8
  stdout line format pinned. D-set count unchanged at 10; guards 10+1
  → 11+1; required fixtures unchanged at 8.
- **Iter 2:** D-set / guards / fixtures locked at iter-1 shape.
  Three preemptive sharpenings: D1 port-collision config validation
  (preempts Codex-style sharpening over bind-address conflicts);
  D4 HTTP method handling pinned to 404 for unknown paths and 405 for
  unsupported methods; D6 drain-state semantics surfaced explicitly
  (`/ready` returns 503 with `degraded_reason: "draining"` during
  SIGTERM; operator value for Kubernetes rolling deployments). No
  retractions.
- **Iter 3:** DoS-hardening sharpening. D4 extended with 5-second
  per-request `tokio::time::timeout` wrapping `read_request`
  preventing slowloris-style attacks against externally reachable
  `/ready` and `/metrics` listeners. New guard 12; new gating
  slow-client fixture. D1 cosmetic addition: example-config comment block
  showing the canonical `monitor-` SAN provisioning recipe.
- **Iter 4:** Final triple ratification. No new sharpenings, no
  retractions, no new decisions. Three-of-three council voices
  ratify iter-3 unchanged. D-set locked at 10. Guard set locked at
  12 + 1 convention. Fixture set locked at 10 required + 2
  V1-evidence optional. Ready to promote.
- **Implementation review addendum:** R4 — retracted Wendelon-style
  `toron_<noun>_<unit>` metric naming in favor of ADR 044-consistent
  functional brand-clean names. This is recorded as an explicit
  Wendelon non-carry-over in D7 and enforced by guard 10.

## Conclusion

ADR 045 ships:

- one shared `[observability.listeners]` config section composing
  over ADR 044's `[tls]` policy;
- three HTTP/1.1 listeners (`/health` plaintext loopback-only;
  `/ready` and `/metrics` mTLS-gated via reused `ListenerPolicy`);
- one `monitor-` SAN prefix extension to ADR 044's `admit_san`
  exact-shape verifier;
- hand-rolled HTTP/1.1 helpers (no hyper / axum / actix); function-
  signature parallel to Wendelon's helpers;
- 5-second per-request read timeout preventing slowloris-style DoS;
- one `ReadinessSnapshot` with `is_ready()` / `http_status()` /
  `to_json()` derivation pattern mirroring Wendelon's; Toron-
  appropriate fields (no raft);
- drain-state semantics: `/ready` returns 503 +
  `degraded_reason: "draining"` immediately on SIGTERM
  (operator-essential for Kubernetes rolling deployments);
- Prometheus 0.0.4 text format; functional brand-clean metric names;
- closed `{reason}` label cardinality; no histograms; no per-tenant
  labels;
- explicit Wendelon-pattern non-carry-overs (no revocation, no
  audit, no OTLP, no `ActorPermission`, no project-prefixed metric
  names);
- byte-identical preservation of ADR 042 `READY <worker>` + ADR 043
  `SQL_READY <sql>` stdout lines; one appended `OBSERVABILITY_READY`
  line;
- 12 promoted architecture guards + 1 convention guard + 10 required
  integration fixtures + 2 V1-evidence optional fixtures;
- single-plan substrate posture;
- tenth consecutive cycle preserving the no-V1-suffix convention
  (ADRs 039 / 041 / 042 / 043 / 044 / 045).

Observability surface is now operator-reachable. `inspect` reservation
(last from ADR 040), live cert rotation / revocation, multi-tenant
routing + metric labels, OTLP push, audit subsystem, histogram metric
surface — all remain follow-up ADRs with clear scope boundaries.
ADR 046 lead candidate is `nilket toron inspect` real wiring composing
over `ReadinessSnapshot`.

## Promotion Notes

ADR 045 is the **first pure-composition ADR over ADR 044's TLS policy
substrate** (ADR 044 itself was substrate + operator-surface
continuation; ADR 045 introduces no new substrate). It establishes
the discipline for composition cycles over ADR 044: extend the
`admit_san` admission set; reuse the one-cert-per-daemon invariant;
preserve the byte-identical readiness-line continuity invariant; add
listener-class-specific stdout readiness lines as new fourth+
position; preserve the fixture-worker source-isolation invariant
(tenth consecutive cycle).

The ten-phase migration is materialized in
`plans/impl/0045_toron_daemon_observability_listeners.md`.
