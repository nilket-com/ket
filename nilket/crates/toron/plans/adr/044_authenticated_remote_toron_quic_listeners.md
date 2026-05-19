# ADR 044: Authenticated Remote Toron QUIC Listeners

**Status:** Promoted
**Seed:** ADR 042 D9 (worker task listener loopback floor); ADR 043 D5
(SQL listener loopback floor); ADR 043 council disposition naming
ADR 044 as the lead candidate; Wendelon's existing
`node-<id>.<cluster_domain>` SAN convention
(`crates/wendelon/src/network/tls.rs:58, 623-628`).
**Council:** Claude, Codex, Gemini (3 iterations: iter-0 topic-shape
survey with Shape A bundled-remote-auth recommendation; iter-1 Claude
three retractions — R1 SAN-convention harmonization with Wendelon, R2
Plan A live-cert-rotation seam collapsed to single-plan, R3 new
SqlError codes withdrawn since TLS rejection happens at handshake;
iter-2 D-set / guards / fixtures locked at 11 / 14+1 / 9+2 with D7
sharpened to exclude CRL / OCSP / runtime-mutable revocation; iter-3
final triple ratification with one Phase-2 SAN-admission exact-shape
sharpening preventing subdomain injection; Codex post-merge review
pins remote SQL client TLS options and client-only SQL admission).
**Category:** Substrate ADR — operator-surface continuation (admits
remote binds + remote static-worker dials behind a shared mTLS policy).

## Summary

Lift the ADR 042 D9 worker task listener loopback floor **and** the
ADR 043 D5 SQL listener loopback floor **together** behind a single
shared `[tls]` policy section. Add a TLS-policy module — placement
decided at Phase 0 audit (Plan W-B preferred: a sanctioned shared
`tls-pki` workspace crate that both Wendelon and Toron consume; Plan
W-C fallback: a Toron-local copy under `crates/toron/src/network/tls.rs`)
— carrying `ListenerPolicy`, the canonical SAN helper
`canonical_daemon_dns_name`, the listener-cert loader, the peer-CA
verifier, and the SAN-admission exact-shape verifier. Extend
`crates/toron/src/config.rs` with a `[tls]` section. Extend
`crates/toron/src/daemon.rs`'s worker task listener and SQL listener
acceptors to consume the shared policy. Extend the coordinator's
static-worker dial path (`crates/toron/src/coordinator.rs` or
equivalent) to admit non-loopback `[[coordinator.static_workers]]`
endpoints when `[tls]` is present AND the dialed peer's cert SAN binds
byte-identically to `toron-<worker_id>.<cluster_domain>`. One cert per
daemon; restart-only rotation; no CRL / OCSP / runtime-mutable
revocation; no observability listeners; no multi-tenant routing.

`nilket toron sql` gains explicit remote mTLS client flags and server
name plumbing inside Toron so ADR 044's remote SQL listener is usable
from the operator CLI without adding runtime dependencies to
`crates/nilket`.

**No** new wire frames. **No** new `SqlError` codes. **No** token /
JWT / OIDC / SPIFFE auth surface. **No** TOFU / fingerprint pinning.
**No** live cert rotation. **No** CRL parsing / OCSP stapling /
runtime-mutable revocation list. **No** observability listeners.
**No** multi-tenant routing. **No** Wendelon descriptor discovery /
refresh. **No** changes to `READY` / `SQL_READY` stdout lines. **No**
loosening of any ADR 042 / ADR 043 invariant. **No** auto-generated
dev cert in production. Those are follow-up ADRs (ADR 045+).

## Motivation

ADR 042 shipped a loopback worker task listener; ADR 043 shipped a
loopback SQL listener and made the worker task listener load-bearing
for SQL dispatch. Together they form the substrate for V1's
multi-process SIGKILL-byte-identical-retry-digest target — but the
substrate is unreachable from real operator consoles or sibling
daemons on different hosts. ADR 044 is the smallest cycle that admits
remote binds and remote dials behind a shared mTLS policy, without
introducing multi-tenant routing, token auth, observability listeners,
or live cert rotation.

The shape harmonizes with Wendelon's existing TLS posture (one CA,
one cert per node, one canonical SAN). Operators provisioning a
Toron+Wendelon cluster reuse the cluster_domain and CA-management
patterns they already know. The validator code is symmetric (accept
side + dial side share the verifier); admitting both directions in
one cycle costs ~one extra config-validator branch + one runtime SAN
binding check over admitting one direction.

ADR 044 keeps the substrate floor real, never inert (matches ADR 042
discipline). Configs without `[tls]` keep the ADR 042 + ADR 043
loopback-only behavior byte-identical. Configs with `[tls]` admit
non-loopback binds and remote static-worker dials behind mTLS.

## Decisions (12, Promotion-Locked)

**D1: Add a shared `[tls]` section to `ToronBootstrapConfig`.**

```toml
[tls]
listener_cert = "/etc/toron/tls/listener.pem"
listener_key  = "/etc/toron/tls/listener.key"   # unencrypted PEM PKCS#8
peer_ca       = "/etc/toron/tls/peer-ca.pem"
cluster_domain = "cluster.local"
```

- `cluster_domain` defaults to `"cluster.local"` (byte-identical with
  `wendelon::network::tls::DEFAULT_CLUSTER_DOMAIN`).
- Missing `[tls]` keeps ADR 042 + ADR 043 worker-only / SQL-loopback
  behavior byte-identical.
- Path resolution: absolute paths used as-is; relative paths resolved
  against the config file's parent directory (matches Wendelon's loader
  convention).
- No `${VAR}` environment interpolation.
- `listener_key` must be unencrypted PEM PKCS#8; encrypted keys
  rejected at load time.

Stable diagnostic strings:

```text
tls listener_key must be unencrypted PEM PKCS#8
tls config paths do not interpolate environment variables
tls cluster_domain must not be empty
```

**D2: Non-loopback bind admitted iff `[tls]` is configured.**

For each of `[worker].task_listener_bind` and `[sql].listener_bind`:

| Bind          | `[tls]` present | Outcome |
| ------------- | --------------- | ------- |
| loopback      | no              | accept  |
| loopback      | yes             | accept  |
| non-loopback  | no              | **reject** with ADR 044 stable, brand-clean diagnostic |
| non-loopback  | yes             | accept  |

ADR 044 lifts the ADR 042 D9 and ADR 043 D5 loopback floors while
superseding their older toron-prefixed diagnostics with brand-clean
strings, per `plans/conventions/no-project-name-in-protocols.md`.

Pinned ADR 044 diagnostic strings:

```text
worker listener requires authenticated transport for non-loopback bind address
sql listener requires authenticated transport for non-loopback bind address
```

**D3: mTLS only. No tokens, no SPIFFE-URI SAN parsing.**

V1 peer authentication is X.509 mTLS against `peer_ca`. Peer cert must
present a DNS SAN matching the per-listener admission rule (D5). **No**
bearer / JWT / OIDC token paths. **No** SPIFFE-URI SAN parsing. **No**
TOFU / fingerprint pinning. **No** opportunistic / STARTTLS-style
negotiation.

**D4: Server-stamped tenant remains.**

ADR 043 D9 invariant preserved: `tenant_id` is stamped from
`[node].tenant_id`. Peer cert identity is used for **admission**, not
for tenant routing. Multi-tenant routing (per-tenant catalog isolation,
per-tenant quota, per-tenant memory budget) defers to ADR 045+.

**D5: SAN convention mirrors Wendelon; admission is exact-shape per listener.**

The daemon's server identity SAN is:

```text
toron-<worker_id_uuid>.<cluster_domain>
```

External SQL client identity SAN is:

```text
client-<principal>.<cluster_domain>
```

`<principal>` is operator-assigned, alphanumeric + `-`. Helper:

```rust
pub fn canonical_daemon_dns_name(worker_id: &Uuid, cluster_domain: &str) -> String {
    format!("toron-{worker_id}.{cluster_domain}")
}
```

Structurally parallel to `wendelon::network::tls::canonical_node_dns_name`.

**Admission verifier (exact-shape match — iter-3 sharpening):** the
SAN must parse as exactly:

```text
<prefix>-<id_token>.<cluster_domain>
```

with the following constraints:

1. SAN has exactly one leading label before the `cluster_domain`
   labels (no extra subdomain).
2. `<prefix>` ∈ {`toron`, `client`}.
3. `<id_token>` matches `[A-Za-z0-9-]+` — no dots, underscores,
   wildcards, or other separators.
4. For `<prefix> = toron`, `<id_token>` must additionally parse as a
   UUID matching the daemon's `worker_id` (D11) or the dialed peer's
   declared `worker_id` (D10).

Anything else → handshake rejection. This prevents:

- subdomain injection (`toron-foo.evil.cluster.local`);
- wildcard abuse (`toron-*.cluster.local`);
- underscore confused-deputy (`toron-foo_bar.cluster.local`);
- malformed UUID under `toron-`.

**Per-listener client-certificate admission:**

- Worker task listener: accepts SANs with prefix `toron-` only.
- SQL listener: accepts SANs with prefix `client-` only. The SQL
  listener still presents the daemon's `toron-<worker_id>` certificate
  as its server identity; it does not accept sibling daemon certs as SQL
  clients in ADR 044. Daemon-to-daemon SQL/admin paths are a future ADR
  if a concrete use case appears.

**D6: No new wire frames. No new `SqlError` codes.**

`SqlMessage` byte-identical to ADR 043. `SqlError` code surface
byte-identical to ADR 043. mTLS handshake failures happen **before**
the SqlFrame layer exists; quinn / rustls reject the peer cert during
handshake and the connection drops. The CLI surfaces this as a
connection-establishment error with stable wrapper text:

```text
failed to establish authenticated connection to <endpoint>: <reason>
```

Wrapper prefix is stable; `<reason>` is quinn/rustls text (not stable
surface, used for diagnostics only). Non-zero exit.

**D7: Cert rotation is daemon-restart. No CRL / OCSP in V1.**

`ListenerPolicy` loads cert/key/CA at startup and never reloads.

- **No** `reload_tls` method, **no** SIGHUP handler, **no** inotify
  watch, **no** `quinn::ServerConfig` swap.
- **No** CRL parsing, **no** OCSP stapling, **no** runtime-mutable
  revocation list. Wendelon's `RevocationSnapshot` machinery
  (`crates/wendelon/src/node.rs:1898+`) is intentionally **not**
  threaded into Toron V1.
- Revocation is operator-effected by removing the revoked principal's
  signing chain from `peer_ca` and restarting the daemon.

Live cert rotation and runtime revocation defer to a named follow-up
cycle when a real deployment surfaces the need.

**D8: `READY <worker>` and `SQL_READY <sql>` stdout lines unchanged.**

Byte-identical to ADR 042 D12 / ADR 043 D16. ADR 044 changes only
which bind addresses are admissible, not how the daemon announces them.

**D9: No observability listener in this ADR.**

Observability listeners (health / ready / metrics endpoints) bundle in
ADR 045 as a composition cycle over ADR 044's TLS policy. Bundling in
this ADR would drag HTTP / Prometheus exposition library decisions
into a transport-substrate cycle and mix category lines.

**D10: Remote `[[coordinator.static_workers]]` admitted under SAN binding.**

ADR 043 D12 rejected non-loopback static worker endpoints at config
validation. ADR 044 lifts this gate:

- non-loopback endpoint → requires `[tls]` configured;
- entry carries explicit `worker_id` (UUID);
- daemon dials the endpoint, validates peer cert SAN exact-shape
  matches `toron-<worker_id>.<cluster_domain>` byte-identical
  (per D5);
- mismatch → stable validator error at startup OR connection-time
  failure surfaced by the coordinator's existing error path.

**No** static-IP pinning. **No** allow-list config beyond the
`worker_id` declared in the static-workers entry. SAN exact-shape
binding to `worker_id` is the only identity proof.

**D11: Toron is a TLS server and TLS client with one cert.**

One cert per daemon. Cert SAN includes
`toron-<worker_id>.<cluster_domain>`. The daemon presents this cert
when accepting inbound connections (worker task listener + SQL
listener) AND when dialing outbound to sibling workers under D10.
**No** separate `[worker.tls]` / `[sql.tls]` / `[client.tls]` sections.
**No** separate dial-side cert configuration.

**D12: Remote SQL clients provide TLS material and server name explicitly.**

`nilket toron sql` remains implemented in `crates/toron`, but gains
client-side TLS options for remote endpoints:

```text
nilket toron sql \
  --endpoint 10.0.0.10:7742 \
  --server-name toron-00000000-0000-0000-0000-000000000044.cluster.local \
  --cert certs/client-alice.pem \
  --key certs/client-alice.key \
  --ca certs/peer-ca.pem \
  "select * from orders"
```

Environment equivalents:

```text
TORON_SQL_ENDPOINT
TORON_SQL_SERVER_NAME
TORON_SQL_CLIENT_CERT
TORON_SQL_CLIENT_KEY
TORON_SQL_CA
```

Precedence remains flag > env > compiled default for endpoint; TLS
fields use flag > env. A non-loopback SQL endpoint requires cert, key,
CA, and server name together and fails before dialing if any are
missing. Loopback SQL with no TLS flags keeps ADR 043 client behavior.
`crates/nilket` remains runtime-free.

## Architecture Guards (16, Promotion-Locked)

Added to `crates/toron/tests/architecture_test.rs`:

1. `adr044_toron_tls_config_surface_exists_and_is_optional` — guards
   D1 backward compat; ADR 042 / ADR 043 minimal configs continue
   to load byte-identical, and non-loopback endpoints require `[tls]`.
2. `adr044_tls_policy_uses_exact_san_admission_per_listener` — guards
   D3 / D5 policy shape: mTLS, exact DNS-SAN admission, role-aware
   listener policy, and canonical daemon/client DNS helpers.
3. `adr044_daemon_selects_authenticated_quic_only_for_non_loopback_tls_listeners`
   — guards D2 acceptor wiring; non-loopback worker-task and SQL
   listeners use authenticated QUIC, while loopback fallback remains.
4. `adr044_sql_cli_remote_tls_flags_do_not_add_nilket_runtime_dependencies`
   — guards D12 and ADR 040 D4; remote SQL TLS flags/env live in
   `crates/toron`, and `crates/nilket` remains runtime-free.
5. `adr044_does_not_change_sql_wire_or_add_token_auth_models` — guards
   D3 / D6; no bearer/JWT/OIDC token auth and no SQL wire or
   `SqlError` expansion.
6. `adr044_no_cert_reload_or_revocation_scope_creep` — guards D7; no
   reload method, no CRL parsing, no OCSP, no runtime-mutable
   revocation surface.
7. `adr044_san_admission_is_exact_shape_per_listener` — guards D5;
   the exact-shape verifier (iter-3 sharpening) runs in the listener
   accept path; rejects subdomain injection, wildcards, underscore
   confused-deputy, malformed UUID.
8. `adr044_loopback_diagnostic_text_is_brand_clean_and_stable` —
   guards D2 and D10 stable diagnostics, including the coordinator
   static-worker non-loopback-without-TLS diagnostic.
9. `adr044_tenant_id_remains_server_stamped` — guards D4.
10. `adr044_ready_lines_are_unchanged` — guards D8 source-text
    invariant.
11. `adr044_no_observability_listener_in_this_adr` — guards D9.
12. `adr044_remote_static_worker_requires_tls_and_san_binding` —
    guards D10.
13. `adr044_one_tls_material_set_per_daemon` — guards D11; no per-
    listener TLS config sections.
14. `adr044_san_convention_mirrors_wendelon_shape` — the helper
    format string is `"toron-{worker_id}.{cluster_domain}"`,
    structurally parallel to Wendelon's
    `"node-{node_id}.{cluster_domain}"`.
15. `adr044_sql_listener_client_admission_is_client_prefix_only` —
    guards D5; a daemon `toron-` certificate is a SQL server identity,
    not a SQL client credential in ADR 044.
16. `adr044_no_spiffe_svid_or_uri_san_identity_model` — guards D3; no
    SPIFFE/SVID/URI-SAN identity model is introduced.

Post-promotion convention guard:

- **`adr044_new_tls_policy_names_are_brand_clean_and_no_v1_suffix`** —
  `ListenerPolicy`, `TlsConfig`, `canonical_daemon_dns_name`
  carry no `V1` suffix and introduce no `Toron*` brand prefix into
  the new TLS policy type/helper surface. Ninth consecutive cycle
  preserving the no-V1-suffix convention (ADRs 039 / 041 / 042 / 043 /
  044).

## Integration Fixtures (12 required + 2 V1-evidence optional)

**Required (gates promotion):**

1. `toron_config_missing_tls_section_loads_byte_identical_to_adr043` —
   ADR 042 / ADR 043 example configs still load unchanged.
2. `toron_config_non_loopback_bind_without_tls_rejected` — diagnostic
   matches ADR 044's stable, brand-clean wording.
3. `toron_config_non_loopback_bind_with_tls_loads` — happy path.
4. `toron_config_missing_cert_file_fails_with_stable_diagnostic`
5. `toron_config_encrypted_key_fails_with_stable_diagnostic` —
   supports D1 unencrypted-PKCS8 sharpening.
6. `toron_daemon_rejects_peer_with_unknown_ca` — client-side wrapper
   diagnostic + non-zero exit. Asserts wrapper text + exit code,
   **not** a server-side SqlError frame.
7. `toron_daemon_rejects_peer_with_wrong_san_prefix` — folds the
   wrong-CA, wrong-cluster_domain, wrong-role rejection variants into
   one handshake-time admission gate; includes a daemon `toron-` cert
   rejected as a SQL client credential.
8. `toron_sql_listener_accepts_authenticated_remote_client` — wire-
   correctness witness for SQL over remote mTLS QUIC.
9. `toron_worker_task_listener_accepts_authenticated_remote_coordinator`
   — wire-correctness witness for worker-task over remote mTLS QUIC.
10. **`toron_daemon_rejects_peer_with_subdomain_injection_san`** — peer
    cert signed by `peer_ca` with SAN
    `toron-foo.evil.cluster.local`; daemon `cluster_domain =
    "cluster.local"`; expects handshake-time rejection on both
    listeners (iter-3 sharpening witness).
11. `nilket_toron_sql_remote_requires_tls_material_and_server_name` —
    non-loopback endpoint without the complete cert/key/CA/server-name
    set fails before dialing; no SQL frame is sent.
12. `nilket_toron_sql_remote_dials_with_mtls_flags_and_env` — remote
    SQL succeeds with explicit CLI flags and with environment-variable
    equivalents; flag values override env values.

**Optional V1-evidence (non-gating, post-promotion):**

- `toron_remote_two_daemon_sigkill_retry_digest_is_canonical` — V1
  target signal over remote mTLS QUIC, two daemons, dial-side
  admission exercised. Reachable in this ADR because D10 admits
  remote static-worker dial.
- `toron_san_helper_matches_wendelon_shape` — parity fixture asserting
  `canonical_daemon_dns_name(uuid, "cluster.local")` returns
  `"toron-<uuid>.cluster.local"`, structurally parallel to
  `wendelon::canonical_node_dns_name(1, "cluster.local")` returning
  `"node-1.cluster.local"`.

## Sub-Lift Posture

**Single-plan.** No Plan A / Plan B framing. The iter-0 Plan A live
cert rotation seam was retracted in iter-1 (R2). The cycle ships:

- a complete shared TLS policy substrate;
- applied to both listeners (worker task + SQL) accept paths;
- applied to dial-side static workers (D10);
- with one cert per daemon (D11);
- with restart-only rotation and no runtime revocation surface (D7);
- with exact-shape SAN admission preventing injection attacks (D5,
  iter-3 sharpening);
- with remote SQL client TLS and server-name plumbing in
  `nilket toron sql` (D12).

Matches ADR 042 promotion-locked single-plan posture. The substrate
is real, never inert (rejects the same shape ADR 042 already rejected).

The only Phase-0-audit-dependent axis is the TLS policy module
placement (Plan W-A depend on Wendelon; Plan W-B preferred — shared
`tls-pki` workspace crate; Plan W-C fallback — Toron-local copy).
This is an implementation-shape decision, not a Plan A / Plan B
sub-lift in the ADR-decision sense; the ADR ships identical decision
surface either way.

## Out of Scope (Recorded for Future ADRs)

- **Toron daemon observability listeners (health / ready / metrics).**
  **ADR 045 lead candidate** — composition cycle over ADR 044's TLS
  policy.
- **Multi-tenant routing.** Server-stamped tenant remains. ADR 045+
  once per-tenant isolation policy exists. Now genuinely unblocked by
  ADR 044's authenticated remote QUIC carrying peer identity.
- **Token-based auth (bearer / JWT / OIDC).** Future ADR over the
  same listener policy substrate.
- **SPIFFE-SVID URI SAN parsing.** Same.
- **Live cert rotation without daemon restart.** Future ADR when
  deployment surfaces need.
- **CRL parsing / OCSP stapling / runtime-mutable revocation.** Same
  follow-up cycle as live rotation.
- **Wendelon descriptor discovery / refresh.** Orthogonal cross-ring
  cycle.
- **`nilket toron inspect` real wiring.** Last ADR 040 reservation;
  orthogonal cycle.
- **Cross-tenant memory budgeting / quotas.** ADR 045+.
- **Persistent SQL connections / pipelining / prepared statements.**
  Future ADR over ADR 043's wire surface.
- **gRPC / Arrow Flight / Postgres wire frontends.** Future ADR(s)
  wrap `SqlFrame` after admission policy is settled.
- **Worker membership / placement / gossip ("weak control ring").**
  Stays out as in ADR 042 / 043.
- **Extract shared `tls-pki` workspace crate** as a sanctioned
  Wendelon/Toron dependency. Phase 0 audit decides whether ADR 044
  Plan W-B does this in-cycle or defers to a named follow-up
  refactoring ADR.

## Council History

- **Iter 0:** Claude proposed Shape A (bundled remote-auth substrate
  lifting both ADR 042 D9 + ADR 043 D5 loopback floors), surveying
  Shapes B (split substrate + apply), C (worker-listener-first), D
  (SQL-listener-first; rejected as incoherent given ADR 043 D11 load-
  bearing). Took strong positions on mTLS-only, server-stamped tenant,
  one-cert-per-daemon. Predicted Codex would converge on Shape A,
  Gemini would push for OIDC + bundled observability (rejected 2-of-3).
- **Iter 1:** Claude retracted three iter-0 positions: R1 — invented
  role-prefixed SAN `<role>.<id>.toron.<cluster_domain>` replaced by
  Wendelon-parallel `toron-<worker_id>.<cluster_domain>` after code
  audit of `crates/wendelon/src/network/tls.rs:623-628`; R2 — Plan A
  live-cert-rotation seam collapsed to single-plan; R3 — new SqlError
  codes withdrawn since TLS rejection happens at handshake before
  `SqlFrame` layer exists. D-set 12 → 11; guards 15 → 14; required
  fixtures 10 → 9; sub-lift posture Plan-A/B → single-plan.
- **Iter 2:** D-set / guards / fixtures locked at iter-1 shape. D7
  sharpened to explicitly exclude CRL / OCSP / runtime-mutable
  revocation (preempting a Codex-style sharpening over Wendelon's
  `RevocationSnapshot` machinery at `crates/wendelon/src/node.rs:1898+`).
  Phase 0 audit decision documented (Plan W-A/W-B/W-C TLS policy
  placement).
- **Iter 3:** Final triple ratification. One narrow sharpening, no
  retractions: D5 SAN admission tightened to exact-shape match
  preventing four attack shapes (subdomain injection, wildcard
  abuse, underscore confused-deputy, malformed UUID under `toron-`).
  Required fixtures 9 → 10 (added
  `toron_daemon_rejects_peer_with_subdomain_injection_san`).
  Codex post-merge review added D12 and tightened SQL client admission
  to `client-` certificates only. D-set locked at 12. Guard set locked
  at 16 + 1 convention. Fixture set locked at 12 required + 2
  V1-evidence optional. Ready to promote.

## Conclusion

ADR 044 ships:

- one shared `[tls]` config section that lifts ADR 042 D9 and ADR 043 D5
  loopback floors simultaneously;
- one `ListenerPolicy` substrate threaded into both listener
  accept paths AND the static-worker dial path;
- SAN convention harmonized with Wendelon's `node-<id>.<cluster_domain>`
  shape via `toron-<worker_id>.<cluster_domain>`;
- exact-shape SAN admission per listener (`toron-` only for worker
  task; `client-` only for SQL client certificates); subdomain injection /
  wildcard abuse / underscore confused-deputy / malformed UUID under
  `toron-` all rejected at handshake;
- remote SQL client TLS flags / env vars and explicit server-name
  verification while keeping `crates/nilket` runtime-free;
- one cert per daemon, both server identity and dial identity;
- restart-only rotation, no CRL / OCSP / runtime-mutable revocation;
- no wire-frame additions, no new SqlError codes, no observability
  listeners, no multi-tenant routing;
- byte-identical preservation of `READY` / `SQL_READY` stdout lines
  from ADR 042 / ADR 043;
- 16 promoted architecture guards + 1 convention guard + 12 required
  integration fixtures + 2 V1-evidence optional fixtures;
- single-plan substrate posture.

Authenticated remote substrate now exists for Toron compute ring.
Observability listeners, multi-tenant routing, descriptor refresh,
live cert rotation, runtime revocation, prepared statements, gRPC /
Postgres wire frontends, and `inspect` wiring all remain follow-up
ADRs with clear scope boundaries — ADR 045 lead candidate is
observability listeners as a composition cycle over ADR 044's TLS
policy.

## Promotion Notes

ADR 044 is the **third ADR that lifts an ADR 042/043 substrate floor**
(after ADR 042 lifting the `nilket toron start` reservation and
ADR 043 lifting the `nilket toron sql` reservation + the
`providers: HashMap::new()` execution gap). It establishes a
discipline for transport-substrate cycles: lift the loopback floor
behind a single shared policy substrate; preserve stable, brand-clean
diagnostics; preserve the no-V1-suffix convention;
preserve the fixture-worker source-isolation invariant (ninth
consecutive cycle).

The ten-phase migration is materialized in
`plans/impl/044_authenticated_remote_toron_quic_listeners.md`.
