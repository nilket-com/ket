# Implementation Plan: ADR 044 Authenticated Remote Toron QUIC Listeners

**Status:** Promoted (paired with `plans/adr/044_authenticated_remote_toron_quic_listeners.md`)

## Implementation Thesis

Lift ADR 042 D9 and ADR 043 D5 loopback floors together behind a
single shared mTLS policy. Add a TLS-policy module (placement decided
at Phase 0: Plan W-B shared `tls-pki` workspace crate preferred; Plan
W-C Toron-local copy as fallback). The module owns cert loading,
peer-CA verification, the canonical SAN helper
(`canonical_toron_dns_name`), and the per-listener SAN-admission
exact-shape verifier (no subdomain injection, no wildcard abuse, no
underscore confused-deputy, no malformed UUID under `toron-`). Extend
`crates/toron/src/config.rs` with a `[tls]` section. Thread the policy
into both listener accept paths (`crates/toron/src/daemon.rs`) and the
static-worker dial path (`crates/toron/src/coordinator.rs` or
sibling). Add remote SQL client TLS flags and explicit server-name
plumbing to `nilket toron sql` inside `crates/toron`. One cert per
daemon; restart-only rotation; no CRL / OCSP / runtime revocation.
Preserve every ADR 042 / ADR 043 invariant byte-identically when
`[tls]` is absent.

## Phase 0: Audit

Resolve the following load-bearing assumptions and capture evidence
in the impl evidence record before Phase 1 starts.

1. **TLS policy module placement (Plan W-A / W-B / W-C).** Audit
   `crates/wendelon/src/network/tls.rs` for the surface area Toron
   would need: `canonical_node_dns_name`, `DEFAULT_CLUSTER_DOMAIN`,
   the rustls server / client config builders, the peer-CA verifier.
   - **Plan W-A:** depend directly on the `wendelon` crate from Toron
     and reuse the verifier in place. **Rejected** because it imports
     Wendelon server internals into Toron (ADR 042 D15 / cross-ring
     discipline).
   - **Plan W-B (preferred):** extract `wendelon::network::tls` into
     a sanctioned shared workspace crate (`crates/tls-pki` or
     similar) that both Wendelon and Toron depend on. Long-term
     correct shape.
   - **Plan W-C (fallback):** copy the helpers into a Toron-local
     module (`crates/toron/src/network/tls.rs`); keep structurally
     parallel via guard 14. Acceptable V1 fallback if Plan W-B's
     extraction has unresolved seams that would force ADR 044 beyond
     its 3-iteration budget.
   Decision rule: select W-B if the Wendelon-side extraction fits
   in this cycle; else W-C plus a named follow-up "extract shared
   `tls-pki` crate" refactoring ADR.

2. **quinn / rustls version compatibility.** Confirm the workspace's
   quinn / rustls pins accept a `Vec<CertificateDer>` chain +
   `PrivateKeyDer` + custom `ClientCertVerifier` / `ServerCertVerifier`
   for mTLS in both server and client roles. Read
   `crates/wendelon/src/network/quic_transport.rs` for the existing
   pattern; mirror it.

3. **rustls private-key parser.** Confirm `rustls_pemfile::pkcs8_private_keys`
   (or equivalent) is available. If only PKCS#1 is in scope, D1's
   "unencrypted PEM PKCS#8" diagnostic surfaces for PKCS#1 keys too
   (or D1 widens to PKCS#1 + PKCS#8 — Phase 0 decides; default is
   strict PKCS#8 only).

4. **Toron daemon's current listener accept loop seams.** Read
   `crates/toron/src/daemon.rs`'s ADR 042 worker task acceptor and
   ADR 043's SQL acceptor. Identify the single seam each uses for
   "build the QUIC server config." That seam becomes the policy
   thread-through point in Phase 4.

5. **Toron coordinator's current static-worker dial seam.** Read
   `crates/toron/src/coordinator.rs` (or equivalent). Identify the
   single seam for "build the QUIC client config when dialing a
   static worker." That seam becomes the dial-side policy thread in
   Phase 5.

6. **Test TLS material manifest.** Confirm or add a checked-in
   fixture CA + leaf certs under `crates/toron/tests/fixtures/tls/`.
   Required SANs in the fixture set:
   - `toron-00000000-0000-0000-0000-000000000001.cluster.local` —
     daemon A under test.
   - `toron-00000000-0000-0000-0000-000000000002.cluster.local` —
     daemon B for two-daemon fixtures.
   - `client-alice.cluster.local` — happy-path SQL client.
   - `client-bob.cluster.local` — second SQL client.
   - `unsigned-by-our-CA.cluster.local` — for the unknown-CA
     rejection fixture (fixture 6).
   - `foo-1.cluster.local` — wrong SAN prefix (neither `toron-` nor
     `client-`) for fixture 7.
   - `toron-foo.evil.cluster.local` — subdomain injection for
     fixture 10 (iter-3 sharpening witness).
   - `toron-foo_bar.cluster.local` — underscore confused-deputy
     (covered by fixture 10 or fixture 7 depending on Phase 7
     organization).

7. **`peer_ca` loading from a multi-cert PEM.** Confirm `rustls`
   accepts a PEM with multiple `-----BEGIN CERTIFICATE-----` blocks
   as the trust anchor set. Required for operators to rotate the CA
   incrementally.

8. **SQL client TLS option seam.** Read `crates/toron/src/cli.rs` and
   the ADR 043 SQL client runner. Identify where endpoint flag/env
   precedence is applied and where the QUIC client is built; this is
   where `--server-name`, `--cert`, `--key`, and `--ca` are added.

9. **Phase 0 evidence record.** Capture W-B vs W-C decision, quinn
   / rustls version, test material manifest, and any seam-audit
   findings in `plans/impl/044_evidence.md` before Phase 1 starts.

## Phase 1: Dependencies and Module Skeleton

Update `crates/toron/Cargo.toml` only if missing:

- `rustls` (workspace pin, matching Wendelon's pin from Phase 0).
- `rustls-pemfile`.
- `rustls-pki-types`.
- Phase 0 W-B/W-C selection: add a `tls-pki` workspace dependency
  (W-B) OR no new workspace dep (W-C; module lives crate-local).
- Reuse existing `quinn`, `tokio`, `serde`, `tracing`, `uuid` deps.

Add module exports in `crates/toron/src/lib.rs`:

```rust
pub mod network {
    pub mod tls;
}
```

Under Plan W-B the module is a thin Toron-side wrapper that
re-exports `tls_pki::ToronTlsListenerPolicy` and provides Toron-
specific helpers like `canonical_toron_dns_name`. Under Plan W-C the
module owns the verifier implementation directly.

**Do not** touch `crates/nilket/Cargo.toml`. ADR 040 D4
root-runtime-free invariant preserved.

## Phase 2: TLS Policy Module — `crates/toron/src/network/tls.rs`

Define the policy types. No `V1` suffix.

```rust
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

pub const DEFAULT_TORON_CLUSTER_DOMAIN: &str = "cluster.local";

/// Canonical DNS SAN for a Toron daemon. Structurally parallel to
/// `wendelon::network::tls::canonical_node_dns_name`.
pub fn canonical_toron_dns_name(worker_id: &Uuid, cluster_domain: &str) -> String {
    format!("toron-{worker_id}.{cluster_domain}")
}

/// Canonical DNS SAN for an external SQL client principal.
pub fn canonical_toron_client_dns_name(principal: &str, cluster_domain: &str) -> String {
    format!("client-{principal}.{cluster_domain}")
}

#[derive(Debug, Clone)]
pub struct ToronTlsConfig {
    pub listener_cert: PathBuf,
    pub listener_key: PathBuf,
    pub peer_ca: PathBuf,
    pub cluster_domain: String,
}

pub struct ToronTlsListenerPolicy {
    cluster_domain: String,
    server_config_worker_task: Arc<rustls::ServerConfig>,
    server_config_sql: Arc<rustls::ServerConfig>,
    client_config_dial: Arc<rustls::ClientConfig>,
}

#[derive(Debug, Clone, Copy)]
pub enum ListenerRole {
    WorkerTask,
    Sql,
}

#[derive(Debug, Clone)]
pub enum PeerKind {
    SiblingDaemon { worker_id: Uuid },
    Client { principal: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ToronTlsError {
    #[error("toron tls listener_key must be unencrypted PEM PKCS#8")]
    KeyNotPkcs8,
    #[error("toron tls config paths do not interpolate environment variables")]
    EnvInterpolation,
    #[error("toron tls cluster_domain must not be empty")]
    ClusterDomainEmpty,
    #[error("toron tls cert file not found or unreadable: {0}")]
    CertFile(PathBuf),
    #[error("toron tls peer_ca file not found or unreadable: {0}")]
    PeerCaFile(PathBuf),
    #[error("toron tls peer cert SAN admission rejected")]
    SanShapeRejected,
    #[error("toron tls peer cert SAN role rejected for listener")]
    SanRoleRejected,
}

impl ToronTlsListenerPolicy {
    pub fn load(config: &ToronTlsConfig) -> Result<Self, ToronTlsError> {
        // 1. Validate cluster_domain non-empty.
        // 2. Reject ${VAR} patterns in any path.
        // 3. Parse listener_cert as PEM cert chain (reject empty/malformed).
        // 4. Parse listener_key as PKCS#8 (reject if not PKCS#8).
        // 5. Parse peer_ca as PEM cert chain; build RootCertStore.
        // 6. Build ServerConfig for WorkerTask role (verifier admits `toron-` only).
        // 7. Build ServerConfig for Sql role (verifier admits `client-` only).
        // 8. Build ClientConfig for dial-side; callers pass explicit DNS server names.
        unimplemented!()
    }

    pub fn server_config_for(&self, role: ListenerRole) -> Arc<rustls::ServerConfig> {
        match role {
            ListenerRole::WorkerTask => self.server_config_worker_task.clone(),
            ListenerRole::Sql => self.server_config_sql.clone(),
        }
    }

    pub fn client_config_for_worker_dial(&self) -> Arc<rustls::ClientConfig> {
        // Callers pass `canonical_toron_dns_name(worker_id, cluster_domain)`
        // as the quinn/rustls server name when connecting.
        self.client_config_dial.clone()
    }
}
```

**Exact-shape SAN admission verifier (iter-3 sharpening, D5):**

```rust
fn admit_san(san: &str, role: ListenerRole, cluster_domain: &str) -> Result<PeerKind, ToronTlsError> {
    let suffix = format!(".{cluster_domain}");
    let leading = san
        .strip_suffix(&suffix)
        .ok_or(ToronTlsError::SanShapeRejected)?;

    // Exact-shape: leading must contain no dots (no extra subdomain).
    if leading.contains('.') {
        return Err(ToronTlsError::SanShapeRejected);
    }

    let (prefix, id_token) = leading
        .split_once('-')
        .ok_or(ToronTlsError::SanShapeRejected)?;

    // id_token must be [A-Za-z0-9-]+ — no dots, underscores, wildcards.
    if id_token.is_empty()
        || !id_token.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err(ToronTlsError::SanShapeRejected);
    }

    match (prefix, role) {
        ("toron", ListenerRole::WorkerTask) => {
            let worker_id = Uuid::parse_str(id_token)
                .map_err(|_| ToronTlsError::SanShapeRejected)?;
            Ok(PeerKind::SiblingDaemon { worker_id })
        }
        ("toron", ListenerRole::Sql) => Err(ToronTlsError::SanRoleRejected),
        ("client", ListenerRole::Sql) => {
            Ok(PeerKind::Client { principal: id_token.to_owned() })
        }
        ("client", ListenerRole::WorkerTask) => Err(ToronTlsError::SanRoleRejected),
        _ => Err(ToronTlsError::SanShapeRejected),
    }
}
```

The custom `ClientCertVerifier` delegates trust-chain validation to
rustls's default verifier (built from `peer_ca`), then calls
`admit_san` on each DNS SAN in the leaf cert. The first SAN that
passes `admit_san` for the listener's role admits the peer. If no
SAN passes, the handshake is rejected.

## Phase 3: `[tls]` Config Section in `crates/toron/src/config.rs`

Add the optional section to `ToronBootstrapConfig`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToronBootstrapConfig {
    // existing fields...
    #[serde(default)]
    pub tls: Option<ToronTlsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToronTlsConfig {
    pub listener_cert: PathBuf,
    pub listener_key: PathBuf,
    pub peer_ca: PathBuf,
    #[serde(default = "default_cluster_domain")]
    pub cluster_domain: String,
}

fn default_cluster_domain() -> String {
    "cluster.local".to_string()
}
```

Extend `validate_bootstrap_config`:

1. Resolve relative paths in `[tls]` against the config file's parent
   directory.
2. Reject `${VAR}` patterns in cert / key / ca paths with the stable
   diagnostic.
3. For each listener (`[worker].task_listener_bind`,
   `[sql].listener_bind`):
   - parse to `SocketAddr`;
   - if `is_loopback()` → accept;
   - else if `[tls]` is `None` → reject with the byte-identical-with-
     ADR-042-D9 / ADR-043-D5 diagnostic string;
   - else → accept (TLS-policy load deferred to daemon start).
4. For each `[[coordinator.static_workers]]` entry:
   - parse endpoint to `SocketAddr`;
   - if `is_loopback()` → accept;
   - else if `[tls]` is `None` → reject (preserves ADR 043 D12 gate
     when TLS absent);
   - else → accept (SAN binding check deferred to dial time).

Update `render_example_config` to include a commented-out `[tls]`
block showing the canonical shape and the default `cluster_domain`.

## Phase 4: Listener Acceptor Wiring in `crates/toron/src/daemon.rs`

Thread the policy into both listener acceptors.

**Worker task listener (ADR 042 path):**

```rust
let worker_server_config = config.tls.as_ref().map(|_| {
    tls_policy.server_config_for(ListenerRole::WorkerTask)
});

// In the accept loop:
match worker_server_config.as_ref() {
    Some(tls_cfg) => accept_quic_tls(stream, tls_cfg).await?,  // mTLS
    None => accept_quic_loopback(stream).await?,               // ADR 042 loopback-only QUIC path
}
```

The unauthenticated loopback QUIC path is preserved byte-identical when
`[tls]` is absent (D2 backward compat). When TLS is configured, accepts
go through rustls; the SAN-admission verifier rejects non-`toron-` peers
at handshake.

**SQL listener (ADR 043 path):**

```rust
let sql_server_config = config.tls.as_ref().map(|_| {
    tls_policy.server_config_for(ListenerRole::Sql)
});
```

Same pattern. Unauthenticated loopback QUIC fallback when `[tls]` is
absent. The role-aware verifier admits `client-` SAN prefixes only for
SQL client certificates. The SQL listener still presents the daemon's
`toron-<worker_id>` certificate as server identity.

`READY <worker>` and `SQL_READY <sql>` stdout lines emitted unchanged
(D8 invariant).

## Phase 5: Coordinator Dial-Side Wiring

Thread the policy into the static-worker dial path
(`crates/toron/src/coordinator.rs` or equivalent module).

```rust
for static_worker in config.coordinator.static_workers.iter() {
    let endpoint = static_worker.endpoint;
    let client_config = match (&config.tls, endpoint.ip().is_loopback()) {
        (Some(_), _) => Some(tls_policy.client_config_for_worker_dial()),
        (None, true) => None,  // ADR 043 D12 unauthenticated loopback QUIC path preserved
        (None, false) => unreachable!("config validation rejects this"),
    };
    let server_name = canonical_toron_dns_name(&static_worker.worker_id, &cluster_domain);
    // Build the dial endpoint using client_config; rustls's
    // ServerCertVerifier requires the peer's cert SAN exact-shape
    // match `toron-<worker_id>.<cluster_domain>` (verified through
    // the same `admit_san` helper as the accept-side verifier — Phase 6
    // guard ensures symmetry).
}
```

Mismatch → dial-time connection error surfaced by the coordinator's
existing error path (no new error variant per D6).

### Phase 5b: SQL CLI Remote Client Wiring

Update `crates/toron/src/cli.rs` and the ADR 043 SQL client runner.

Add flags to `nilket toron sql`:

```text
--server-name <dns-name>
--cert <path>
--key <path>
--ca <path>
```

Add environment variables:

```text
TORON_SQL_SERVER_NAME
TORON_SQL_CLIENT_CERT
TORON_SQL_CLIENT_KEY
TORON_SQL_CA
```

Rules:

- Existing endpoint precedence remains flag > env > compiled default.
- TLS fields use flag > env.
- Loopback endpoint with no TLS flags keeps ADR 043 client behavior.
- Non-loopback endpoint requires cert, key, CA, and server name
  together; partial material fails before dialing.
- The client sends the explicit server name as SNI and verifies the
  daemon server certificate against that name.
- `crates/nilket` remains runtime-free.

## Phase 6: Architecture Guards

Add to `crates/toron/tests/architecture_test.rs`. Sixteen ADR 044
guards + one convention guard, structurally mirroring ADR 043's
implementation pattern.

```rust
#[test]
fn adr044_tls_section_is_optional_for_loopback_only_config() {
    let config_text = include_str!("../examples/loopback_only.toml");
    let parsed: ToronBootstrapConfig = toml::from_str(config_text).unwrap();
    assert!(parsed.tls.is_none());
}

#[test]
fn adr044_san_admission_is_exact_shape_per_listener() {
    use toron::network::tls::{admit_san, ListenerRole};
    // Valid `toron-` for worker task.
    assert!(admit_san("toron-00000000-0000-0000-0000-000000000001.cluster.local",
                      ListenerRole::WorkerTask, "cluster.local").is_ok());
    // Subdomain injection rejected.
    assert!(admit_san("toron-foo.evil.cluster.local",
                      ListenerRole::WorkerTask, "cluster.local").is_err());
    // Wildcard rejected.
    assert!(admit_san("toron-*.cluster.local",
                      ListenerRole::WorkerTask, "cluster.local").is_err());
    // Underscore rejected.
    assert!(admit_san("toron-foo_bar.cluster.local",
                      ListenerRole::WorkerTask, "cluster.local").is_err());
    // Malformed UUID rejected.
    assert!(admit_san("toron-notauuid.cluster.local",
                      ListenerRole::WorkerTask, "cluster.local").is_err());
    // client- rejected on worker task listener.
    assert!(admit_san("client-alice.cluster.local",
                      ListenerRole::WorkerTask, "cluster.local").is_err());
    // client- accepted on SQL listener.
    assert!(admit_san("client-alice.cluster.local",
                      ListenerRole::Sql, "cluster.local").is_ok());
    // toron- rejected as a SQL client certificate.
    assert!(admit_san("toron-00000000-0000-0000-0000-000000000001.cluster.local",
                      ListenerRole::Sql, "cluster.local").is_err());
}

#[test]
fn adr044_san_convention_mirrors_wendelon_shape() {
    let src = include_str!("../src/network/tls.rs");
    assert!(src.contains("\"toron-{worker_id}.{cluster_domain}\""));
}

#[test]
fn adr044_loopback_diagnostic_text_byte_identical_with_adr042_and_adr043() {
    let src = include_str!("../src/config.rs");
    assert!(src.contains("toron worker listener requires authenticated transport for non-loopback bind address"));
    assert!(src.contains("toron sql listener requires authenticated transport for non-loopback bind address"));
}

// ... remaining 12 + convention guard ...
```

Source-text guards (#3, #8, #9, #10, #11, #13, #14, #15, convention)
assert absence / presence of specific strings in source files. Runtime
guards (#1, #2, #4, #5, #6, #7, #12, #16) exercise the loader /
verifier / listener admission / SQL CLI option paths in unit test scope.

## Phase 7: Integration Fixtures

### `crates/toron/tests/toron_tls_config_test.rs` (new)

1. `toron_config_missing_tls_section_loads_byte_identical_to_adr043`
2. `toron_config_non_loopback_bind_without_tls_rejected` (pinned
   diagnostic string match)
3. `toron_config_non_loopback_bind_with_tls_loads`
4. `toron_config_missing_cert_file_fails_with_stable_diagnostic`
5. `toron_config_encrypted_key_fails_with_stable_diagnostic`

### `crates/toron/tests/toron_tls_listener_test.rs` (new)

6. `toron_daemon_rejects_peer_with_unknown_ca` — dial with a cert
   signed by an unrelated CA; assert client-side wrapper diagnostic
   + non-zero exit.
7. `toron_daemon_rejects_peer_with_wrong_san_prefix` — dial with a
   cert signed by `peer_ca` but SAN prefix `foo-` (not `toron-` or
   `client-`); assert handshake failure on both listeners. Also assert
   that a valid `toron-<uuid>` daemon cert is rejected when used as a
   SQL client credential.
8. `toron_sql_listener_accepts_authenticated_remote_client` — bind
   daemon on `127.0.0.1:0` with TLS, dial from a `client-alice`-SAN
   cert, submit a SELECT, assert Arrow result batches.
9. `toron_worker_task_listener_accepts_authenticated_remote_coordinator`
   — bind daemon on `127.0.0.1:0` with TLS, dial worker task port
   from a `toron-<other-uuid>` cert, submit a `CancelTask`, assert
   response.
10. `toron_daemon_rejects_peer_with_subdomain_injection_san` —
    peer cert signed by `peer_ca` with SAN
    `toron-foo.evil.cluster.local`. Daemon configured with
    `cluster_domain = "cluster.local"`. Dial both listeners; assert
    handshake-time rejection on both with the client-side wrapper
    diagnostic shape (no SqlError frame per D6). **Iter-3
    sharpening witness.**

### `crates/toron/tests/toron_sql_cli_test.rs` (extended)

11. `nilket_toron_sql_remote_requires_tls_material_and_server_name` —
    non-loopback endpoint without the complete cert/key/CA/server-name
    set fails before dialing.
12. `nilket_toron_sql_remote_dials_with_mtls_flags_and_env` — remote
    SQL succeeds once with CLI flags and once with environment-variable
    equivalents; flag values override env values.

### Optional V1-evidence fixtures (non-gating, post-promotion)

- `toron_remote_two_daemon_sigkill_retry_digest_is_canonical` — two
  daemons with TLS, one dialing the other as a static worker (D10),
  SIGKILL the worker mid-task, observe canonical retry digest. The
  V1 target signal over remote mTLS QUIC.
- `toron_san_helper_matches_wendelon_shape` — parity helper test
  asserting `canonical_toron_dns_name(uuid, "cluster.local")` returns
  `"toron-<uuid>.cluster.local"`, structurally parallel to
  `wendelon::canonical_node_dns_name(1, "cluster.local")` returning
  `"node-1.cluster.local"`.

## Phase 8: Verification

For each phase, capture evidence in `plans/impl/044_evidence.md`
before merging:

1. **Phase 0 evidence:** W-B / W-C decision recorded with rationale;
   quinn / rustls version capture; test material manifest with the
   eight required fixture certs listed.
2. **Phase 1 evidence:** `cargo build -p toron` green; `crates/nilket/Cargo.toml`
   diff confirms no new top-level deps (ADR 040 D4 invariant).
3. **Phase 2-5 evidence:** `cargo test -p toron --tests` green;
   `cargo clippy -p toron -- -D warnings` clean.
4. **Phase 6 evidence:** all 16 + 1 architecture guards pass.
5. **Phase 7 evidence:** all 12 required fixtures pass; optional
   fixtures captured as separate evidence lines (non-gating).
6. **ADR 042 / ADR 043 invariant preservation:**
   - example config without `[tls]` boots byte-identical (`READY` /
     `SQL_READY` line text matches pre-ADR 044);
   - ADR 042 / 043 architecture guards continue to pass;
   - `toron_fixture_worker.rs` source unchanged (ninth preserved
     cycle for the byte-identical refactor-invariant chain;
     ADRs 035 / 036 / 037 / 038 / 039 / 041 / 042 / 043 / 044).
7. **Cross-platform sanity:** TLS material loading exercised on
   Linux. Phase 0 audits confirm darwin / Windows out-of-scope for
   V1 (matches ADR 042 / 043 posture).

## Phase 9: Evidence Record

Materialize `plans/impl/044_evidence.md` with:

- Phase 0 audit findings (W-B vs W-C decision; quinn / rustls
  version; test material manifest).
- Phase 1-5 implementation evidence (commit refs, file diff
  summaries, key seam touch-points).
- Phase 6 guard pass evidence.
- Phase 7 fixture pass evidence, including SQL CLI remote TLS option
  coverage.
- Phase 8 verification evidence (loopback-only backward compat
  byte-identical check; ADR 042 / 043 invariant preservation
  checklist; fixture-worker source-unchanged check).
- Risk-register disposition (see below).

## Acceptance Checklist

- [ ] Phase 0 audit captures W-B / W-C decision with rationale.
- [ ] Phase 1 dependencies added; `crates/nilket/Cargo.toml`
      untouched.
- [ ] Phase 2 TLS policy module compiles clean (`cargo clippy
      -- -D warnings`).
- [ ] Phase 3 config validation rejects non-loopback bind without
      `[tls]` with byte-identical-with-ADR-042-D9 / ADR-043-D5
      diagnostic strings.
- [ ] Phase 4 listener acceptors thread the policy through;
      loopback-only path byte-identical when `[tls]` absent.
- [ ] Phase 5 coordinator dial path validates SAN binding to
      `worker_id` byte-identical via `canonical_toron_dns_name`.
- [ ] Phase 5b SQL CLI remote client flags/env and explicit server-name
      verification implemented in `crates/toron`.
- [ ] Phase 6 all 16 ADR 044 guards + 1 convention guard pass.
- [ ] Phase 7 all 12 required fixtures pass.
- [ ] `toron_fixture_worker.rs` source unchanged.
- [ ] ADR 042 / ADR 043 guards continue to pass.
- [ ] SAN admission rejects subdomain injection, wildcard abuse,
      underscore confused-deputy, and malformed UUID under `toron-`
      (iter-3 sharpening fixture #10 passes).
- [ ] Phase 9 evidence record materialized.

## Non-Goals

(Mirrors ADR 044 Out of Scope; included here for impl-plan reviewers
who may not read the ADR.)

- Multi-tenant routing.
- Token / OIDC / SPIFFE auth surface.
- Live cert rotation, CRL, OCSP, runtime revocation.
- Observability listeners (health / ready / metrics). **ADR 045 lead
  candidate.**
- Wendelon descriptor discovery / refresh.
- `nilket toron inspect` real wiring.
- Cross-tenant memory budgets / quotas.
- Persistent SQL connections / pipelining / prepared statements.
- gRPC / Arrow Flight / Postgres wire frontends.
- Worker membership / placement / gossip.
- Auto-generated dev certs in production.

## Risk Register

1. **Wendelon TLS helper extraction (Phase 0 W-B vs W-C).** If
   extraction has unresolved seams (e.g., the verifier depends on a
   Wendelon-private type), fall back to Plan W-C and file a named
   follow-up "extract shared `tls-pki` crate" ADR. Mitigation: guard
   14 keeps the helper source-text parallel to Wendelon's even under
   Plan W-C.
2. **rustls API drift.** quinn / rustls workspace versions could
   drift from Wendelon's. Mitigation: Phase 0 explicitly captures
   versions; pin via workspace `Cargo.toml` if needed.
3. **Test TLS material maintenance burden.** Checked-in fixture
   certs expire eventually. Mitigation: generate certs with long
   expiries (10+ years); document regeneration command in
   `crates/toron/tests/fixtures/tls/README.md`.
4. **Operator-perceived split between server and SQL client identity.**
   The SQL listener presents the daemon's `toron-<worker_id>` cert as
   server identity but admits only `client-<principal>` certs as SQL
   client credentials. Mitigation: D5 and D12 separate server-name
   verification from client-certificate admission, and the SQL CLI
   fixtures cover both flag and env configuration.
5. **`peer_ca` rotation requires daemon restart (D7).** Operators
   rotating the CA must restart all daemons. Mitigation: documented
   operational pattern; live rotation deferred to a named follow-up
   ADR.
6. **D10 dial-side validator drift.** If the dial-side
   `ServerCertVerifier` is constructed differently from the
   accept-side `ClientCertVerifier`, the two paths could enforce
   different policies. Mitigation: both verifiers route through the
   same `admit_san` helper; guard 7 covers per-listener admission;
   add a unit test asserting symmetric behavior across accept/dial
   paths.
7. **Subdomain-injection check correctness (iter-3 sharpening).**
   The `admit_san` exact-shape verifier must reject all four attack
   shapes (subdomain injection, wildcard abuse, underscore confused-
   deputy, malformed UUID). Mitigation: guard 7 exercises all four
   rejection shapes; fixture 10 exercises the subdomain-injection
   case end-to-end through rustls's handshake.
8. **Remote SQL SNI ambiguity.** Inferring a DNS server name from an IP
   endpoint is unreliable. Mitigation: D12 requires explicit
   `--server-name` / `TORON_SQL_SERVER_NAME` for non-loopback SQL
   clients and fails before dialing if it is missing.

## Cross-References

- ADR file (paired): `plans/adr/044_authenticated_remote_toron_quic_listeners.md`.
- Wendelon SAN helper (parity reference):
  `crates/wendelon/src/network/tls.rs:623-628`.
- Wendelon TLS QUIC provider (pattern reference):
  `crates/wendelon/src/network/quic_transport.rs:45-64, 145-159`.
- ADR 042 D9 (loopback floor lifted by ADR 044 D2):
  `plans/adr/042_toron_compute_daemon_substrate.md`.
- ADR 043 D5 (loopback floor lifted by ADR 044 D2):
  `plans/adr/043_public_toron_sql_listener.md`.
- ADR 043 D12 (static-worker loopback floor lifted by ADR 044 D10):
  same file.
- Iter brainstorm trail: `plans/wip/claude_iter_{0..3}_adr_044.md`
  and `plans/wip/claude_iter_{2..3}_impl_044.md`.
