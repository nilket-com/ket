# Implementation Plan: ADR 047 Shared `tls-pki` Workspace Crate

**Status:** Promoted (paired with `plans/adr/047_shared_tls_pki_workspace_crate.md`)

## Implementation Thesis

Extract a shared `crates/tls-pki/` workspace crate consumed by
both Wendelon and Toron with a seven-submodule scope: `material`
(TlsMaterial + PEM loaders), `asn1` (DerReader +
extract_dns_sans + extract_subject + extract_extension), `cbor`
(CborReader + primitives), `san` (admit_san + canonical_dns_name
+ PeerIdentity), `config_builder` (build_rustls_server_config +
build_rustls_client_config parameterized by ALPN and caller-owned
verifier policy), `verifier` (trust-chain verifier helpers), and
`error` (PkiError). Behavior-preserving refactor; zero functional
changes. All ADR 042/043/044/045/046 architecture guards and
integration fixtures pass after extraction; stable output and
diagnostic invariants remain covered by their existing tests. Toron-side
migration consumes substantially through public re-exports +
Toron-local ALPN constants + quinn wrapping + UUID post-check +
existing canonical daemon/client/monitor wrappers. Wendelon-side
migration is **minimal-import-only**: shared-substrate imports +
one `canonical_node_dns_name` wrapper + local `PkiError` →
`std::io::Error` mapping helpers at call sites. Wendelon's ring-internal
machinery (`ActorIdentity`, `ActorTrustRoots`, `authorize_actor`,
`ACTOR_CLAIMS_OID_DOTTED`, `VerifiedTransportPeer`,
`verified_peer_node_id`, `authorize_break_glass_scope`,
`HRFT_ALPN`, `RevocationSnapshot`, inline DER/CBOR parsing for
actor claims) stays untouched. Wendelon-side inline DER/CBOR is
grandfathered for V1; future ADR can migrate.

## Phase 0: Audit

Resolve the following before Phase 1; capture in
`plans/impl/047_*_evidence.md`.

1. **`TlsMaterial` struct field-set parity.** Read both
   `crates/wendelon/src/network/tls.rs::TlsMaterial` and
   `crates/toron/src/network/tls.rs::TlsMaterial`. Confirm both
   have `cert_chain: Vec<CertificateDer<'static>>`, `key:
   PrivateKeyDer<'static>`, `ca_roots: Arc<RootCertStore>`. If field
   sets diverge, identify reconciliation path before Phase 2.
2. **rustls dependency pin.** Confirm Toron and Wendelon use the
   same `rustls` and `rustls-pki-types` versions. `tls-pki` adopts
   those same pins rather than introducing a second TLS stack.
3. **Wendelon-side `TlsMaterial::load` semantics.** Read
   Wendelon's loader for any Wendelon-specific behavior not
   present in Toron's loader. Plan to fold union of behaviors
   into `tls-pki`'s shared loader.
4. **Toron-side `TlsMaterial::load` semantics.** Read Toron's
   loader for ADR 044 `${VAR}` env-interpolation rejection
   logic. Confirm `tls-pki`'s shared loader preserves rejection.
5. **Wendelon-side verifier construction.** Read Wendelon's
   existing custom verifier shape. Identify the composition
   seam: where does Wendelon plug in `authorize_actor`?
   `tls-pki` trust-chain helpers must expose enough primitive
   surface that Wendelon's wrapper composes without duplicating
   peer-CA chain verification.
6. **Toron-side verifier construction.** Read Toron's
   post-ADR-046 `build_rustls_server_config` for existing
   per-role verifier construction. Confirm `allowed_san_prefixes`
   parameterization is byte-identical with
   `tls-pki` trust-chain helper and caller-owned verifier boundary.
7. **Toron-side inline DER parsing check.** Confirm Toron does
   not currently do any inline DER parsing (search for
   `DerReader`, `der_parser::`, `x509-parser::` usage in
   `crates/toron/src/`). If found, plan migration to
   `tls_pki::asn1` in Phase 6.
8. **ADR 042/043/044/045/046 architecture guard inventory.**
   Run `cargo test -p toron --test architecture_test 'adr04'`
   AND `cargo test -p wendelon --test architecture_test
   'adr04'` pre-extraction; capture baseline pass counts +
   test names. Re-run post-extraction; assert identical.
9. **ADR 042/043/044/045/046 integration fixture inventory.**
   Same as #8 for each integration test file in both rings.
10. **Workspace `Cargo.toml` placement.** Confirm `tls-pki` can
    be added under the existing root `crates/*` workspace glob
    without an explicit root member edit.
11. **clippy/rustfmt/edition workspace config.** Confirm
    `tls-pki` adopts existing config without new lint
    deviations.
12. **`rcgen` test dependency.** Confirm `rcgen` (or equivalent
    cert-generation crate) is available as a dev-dependency for
    test fixture cert generation in `crates/tls-pki/tests/`.

## Phase 1: Create `crates/tls-pki/` Workspace Crate

Add the new crate under the existing workspace glob:

```toml
# crates/tls-pki/Cargo.toml
[package]
name = "tls-pki"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
rustls = { version = "0.23", default-features = false, features = ["ring", "std"] }
rustls-pki-types = "1.10"
thiserror = "2"

[dev-dependencies]
rcgen = "0.12"
```

The root `Cargo.toml` already has `members = ["crates/*", ...]`,
so do not add an explicit `"crates/tls-pki"` member unless the
workspace layout changes.

Module skeleton in `crates/tls-pki/src/lib.rs`:

```rust
//! Shared TLS / PKI primitives consumed by both Wendelon and
//! Toron. Behavior-preserving extraction from ADR 047.
//!
//! `tls-pki` is a pure-sync rustls + ASN.1/DER + CBOR substrate
//! library. It does not depend on quinn, tokio, or tracing. It
//! does not host ring-internal admission models
//! (`ActorPermission` stays Wendelon-internal; `monitor-` SAN
//! prefix admission stays Toron-callable but not Toron-internal).

#![deny(unsafe_code)]
#![warn(clippy::all)]

mod material;
mod asn1;
mod cbor;
mod san;
mod config_builder;
mod verifier;
mod error;

pub use material::TlsMaterial;
pub use asn1::{DerReader, extract_dns_sans, extract_subject, extract_extension};
pub use cbor::CborReader;
pub use san::{admit_san, canonical_dns_name, PeerIdentity};
pub use config_builder::{build_rustls_server_config, build_rustls_client_config};
pub use verifier::{build_client_cert_chain_verifier, build_server_cert_chain_verifier};
pub use error::PkiError;

pub const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";
```

## Phase 2: Move `TlsMaterial` + Loader

Move `TlsMaterial::load` from Toron's
`crates/toron/src/network/tls.rs` into
`crates/tls-pki/src/material.rs`. Preserves ADR 044 D6 semantics:

- private key file must be unencrypted PEM PKCS#8;
- cert/key/ca paths reject `${VAR}` env-interpolation patterns;
- cert chain and CA bundle parse as PEM with one or more
  `-----BEGIN CERTIFICATE-----` blocks.

Wendelon's existing `TlsMaterial::load` deletes its local loader
body but keeps a thin Wendelon-local newtype wrapper around
`tls_pki::TlsMaterial` so the public `load` signature remains
`std::io::Result<Self>`. Field access dereferences to the shared
substrate. Phase 0 audit confirmed field-set parity.

`TlsMaterial` is **not** `Clone` (preserves Wendelon's existing
non-Clone discipline — `PrivateKeyDer` is not Clone).

## Phase 3: Add `asn1` + `cbor` Submodules

`crates/tls-pki/src/asn1.rs`:

```rust
use rustls::pki_types::CertificateDer;
use crate::error::PkiError;

pub struct DerReader<'a> { /* internal state */ }

impl<'a> DerReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self;
    pub fn read_sequence(&mut self) -> Result<DerReader<'_>, PkiError>;
    pub fn read_oid(&mut self) -> Result<Vec<u32>, PkiError>;
    pub fn read_bytes(&mut self, expected_tag: u8) -> Result<&'a [u8], PkiError>;
    // ... minimal set needed for SAN/Subject/Extension extraction ...
}

pub struct Subject { pub raw: Vec<u8> }

pub fn extract_dns_sans(cert: &CertificateDer) -> Result<Vec<String>, PkiError>;
pub fn extract_subject(cert: &CertificateDer) -> Result<Subject, PkiError>;
pub fn extract_extension(cert: &CertificateDer, oid: &[u32]) -> Result<Option<Vec<u8>>, PkiError>;
```

Implementation notes:

- Phase 0 audit identifies whether to use a pure-hand-rolled DER
  reader or a crate dep (`x509-parser` / `der`). For ADR 047
  V1, hand-rolled is preferred (consistent with ADR 045 D4
  hand-rolled HTTP/1.1 + ADR 046 hand-rolled HTTP client
  precedent). Wendelon's existing inline DER parsing for actor
  claims can be the reference implementation Phase 3 mirrors.
- `extract_dns_sans` returns the SAN values in order. Used by
  Toron's local `SanClientCertVerifier` and by any future shared
  verifier helpers that need SAN inspection.
- `extract_subject` + `extract_extension` exposed for future
  Wendelon adoption (V1 grandfathers Wendelon's inline parsing).

`crates/tls-pki/src/cbor.rs`:

```rust
use crate::error::PkiError;

pub struct CborReader<'a> { /* internal state */ }

impl<'a> CborReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self;
    pub fn read_map(&mut self) -> Result<CborReader<'_>, PkiError>;
    pub fn read_text(&mut self) -> Result<&'a str, PkiError>;
    pub fn read_bytes(&mut self) -> Result<&'a [u8], PkiError>;
    pub fn read_uint(&mut self) -> Result<u64, PkiError>;
    // ... minimal set needed for actor-claim decoding ...
}
```

Implementation notes:

- Minimal CBOR primitives needed for Wendelon actor-claim
  decoding. Wendelon's existing inline CBOR parsing is the
  reference.
- Exposed for future Wendelon adoption; V1 grandfathers inline.

**V1 scope note:** Wendelon does NOT migrate its existing inline
DER/CBOR parsing to `tls_pki::asn1`/`tls_pki::cbor` in this
cycle. The helpers are exposed for future Wendelon adoption per
Codex iter-3 deferral framing. The `tls-pki`-internal use is in
`verifier.rs` via `extract_dns_sans`.

## Phase 4: Move `admit_san` + `canonical_dns_name` + `PeerIdentity`

Move from Toron's `crates/toron/src/network/tls.rs` (the
ADR 044 D5 / ADR 045 / ADR 046 exact-shape verifier) into
`crates/tls-pki/src/san.rs`. Function signature changes from
the Toron-internal `ListenerRole`-based variant to the
`allowed_prefixes: &[&str]`-based variant per D7:

```rust
pub fn admit_san(
    san: &str,
    cluster_domain: &str,
    allowed_prefixes: &[&str],
) -> Result<PeerIdentity, PkiError> {
    let suffix = format!(".{cluster_domain}");
    let leading = san.strip_suffix(&suffix)
        .ok_or_else(|| PkiError::SanShapeRejected {
            reason: "missing cluster_domain suffix".into(),
        })?;
    if leading.contains('.') {
        return Err(PkiError::SanShapeRejected {
            reason: "subdomain injection".into(),
        });
    }
    let (prefix, id_token) = leading.split_once('-')
        .ok_or_else(|| PkiError::SanShapeRejected {
            reason: "missing prefix-id_token split".into(),
        })?;
    if id_token.is_empty() ||
       !id_token.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(PkiError::SanShapeRejected {
            reason: "id_token must be [A-Za-z0-9-]+".into(),
        });
    }
    if !allowed_prefixes.contains(&prefix) {
        return Err(PkiError::SanPrefixRejected {
            prefix: prefix.to_string(),
            allowed: allowed_prefixes.iter().map(|s| s.to_string()).collect(),
        });
    }
    Ok(PeerIdentity {
        prefix: prefix.to_string(),
        id_token: id_token.to_string(),
        cluster_domain: cluster_domain.to_string(),
    })
}

pub fn canonical_dns_name(prefix: &str, id_token: &str, cluster_domain: &str) -> String {
    format!("{prefix}-{id_token}.{cluster_domain}")
}
```

`PeerIdentity` open struct with public fields (D6):

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    pub prefix: String,
    pub id_token: String,
    pub cluster_domain: String,
}
```

**UUID validation:** unlike ADR 046's Toron-specific
`admit_san` that included UUID parsing for `toron-` prefix,
shared `tls-pki::admit_san` does NOT parse UUIDs. UUID
validation (for `toron-` prefix) becomes a Toron-side
post-check after `admit_san` returns (Phase 6 below).

## Phase 5: Move rustls Config Builders + Trust-Chain Helpers

Move from Toron's post-ADR-046 `network::tls.rs` into
`crates/tls-pki/src/config_builder.rs`:

```rust
pub fn build_rustls_server_config(
    material: &TlsMaterial,
    client_verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
    alpn: &[u8],
) -> Result<rustls::ServerConfig, PkiError> {
    install_rustls_provider();
    let mut config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(material.cert_chain.clone(), material.key.clone_key())
        .map_err(|e| PkiError::Config(format!("server config: {e}")))?;
    config.alpn_protocols = vec![alpn.to_vec()];
    Ok(config)
}

pub fn build_rustls_client_config(
    material: &TlsMaterial,
    server_verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
    alpn: &[u8],
) -> Result<rustls::ClientConfig, PkiError> {
    install_rustls_provider();
    let mut config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(server_verifier)
        .with_client_auth_cert(material.cert_chain.clone(), material.key.clone_key())
        .map_err(|e| PkiError::Config(format!("client config: {e}")))?;
    config.alpn_protocols = vec![alpn.to_vec()];
    Ok(config)
}
```

`crates/tls-pki/src/verifier.rs`:

```rust
use crate::error::PkiError;

pub fn build_client_cert_chain_verifier(
    ca_roots: Arc<RootCertStore>,
) -> Result<Arc<dyn ClientCertVerifier>, PkiError> {
    WebPkiClientVerifier::builder(ca_roots)
        .build()
        .map_err(|e| PkiError::Config(format!("client cert verifier: {e}")))
}

pub fn build_server_cert_chain_verifier(
    ca_roots: Arc<RootCertStore>,
) -> Result<Arc<dyn ServerCertVerifier>, PkiError> {
    WebPkiServerVerifier::builder(ca_roots)
        .build()
        .map_err(|e| PkiError::Config(format!("server cert verifier: {e}")))
}
```

`install_rustls_provider` is crate-private (installs default
`ring` crypto provider for rustls). Idempotent.

Toron's `ExactServerCertVerifier` and `SanClientCertVerifier`
remain Toron-local wrappers around these trust-chain helpers and
continue to call `tls_pki::asn1::extract_dns_sans` +
`tls_pki::admit_san` before mapping to `PeerKind`. Wendelon's actor
verifier remains Wendelon-local and may wrap the trust-chain helper
before applying `authorize_actor` and revocation logic.

`PrivateKeyDer::clone_key()` is a workspace-wide convenience
extension trait (or equivalent) to handle non-Clone
`PrivateKeyDer`.

## Phase 6: Toron-Side Migration

`crates/toron/src/network/tls.rs` consumes shared substrate while
keeping its existing ADR 044 policy surface:

```rust
pub use tls_pki::{
    TlsMaterial, PeerIdentity,
    build_rustls_server_config, build_rustls_client_config,
    build_client_cert_chain_verifier, build_server_cert_chain_verifier,
    canonical_dns_name,
    DEFAULT_CLUSTER_DOMAIN,
};

// Existing Toron policy/protocol surface remains.
pub const AUTH_QUIC_ALPN: &[u8] = b"sql-task-auth";
pub const OBSERVABILITY_HTTP_ALPN: &[u8] = b"http/1.1";
pub enum TlsError { /* existing variants preserved */ }
pub enum ListenerRole { WorkerTask, Sql, Observability }
pub enum PeerKind { /* existing variants preserved */ }

pub fn canonical_daemon_dns_name(worker_uuid: &str, cluster_domain: &str) -> String {
    tls_pki::canonical_dns_name("toron", worker_uuid, cluster_domain)
}

// Existing signature and role mapping remain; exact DNS parsing delegates
// to tls_pki::admit_san internally.
pub fn admit_san(
    san: &str,
    role: ListenerRole,
    cluster_domain: &str,
) -> Result<PeerKind, TlsError> {
    // Preserve ("toron", ListenerRole::WorkerTask), SQL/client/monitor
    // role rejection arms, and Uuid::parse_str(id_token).
}

struct ExactServerCertVerifier { /* Toron-local policy wrapper */ }
struct SanClientCertVerifier { /* Toron-local policy wrapper */ }
```

Existing Toron-side definitions of low-level `TlsMaterial`,
provider installation, DER SAN extraction, and rustls config
assembly delete. Existing Toron-side policy definitions remain:
`TlsError`, `admit_san`, `ListenerRole`, `PeerKind`,
`ExactServerCertVerifier`, `SanClientCertVerifier`,
`canonical_daemon_dns_name`, `canonical_client_dns_name`, and
`canonical_monitor_dns_name`. ADR 042/043/044/045/046 tests that
reference `TlsError` remain unchanged; `PkiError` is mapped into
`TlsError` at Toron's wrapper boundary.

## Phase 7: Wendelon-Side Migration (Minimal-Import-Only)

`crates/wendelon/src/network/tls.rs` adds at the top:

```rust
pub use tls_pki::{
    TlsMaterial,
    build_rustls_server_config,
    build_rustls_client_config,
    build_client_cert_chain_verifier,
    build_server_cert_chain_verifier,
    canonical_dns_name,
    DEFAULT_CLUSTER_DOMAIN,
};

pub fn canonical_node_dns_name(node_id: u32, cluster_domain: &str) -> String {
    tls_pki::canonical_dns_name("node", &node_id.to_string(), cluster_domain)
}

fn pki_error_to_io(err: tls_pki::PkiError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err)
}
```

Existing Wendelon-side definitions of `TlsMaterial::load`,
`canonical_node_dns_name`, and inner rustls config builder
helpers delete. Wendelon's existing wrapper functions
(`rustls_server_config`, `rustls_server_config_with_alpn`,
`rustls_server_config_with_peer_roots`, `server_config`,
`client_config`, etc.) delegate internally to `tls-pki`'s
shared helpers; external signatures preserved byte-identical.

Wendelon's `rustls_server_config_with_peer_roots` may wrap the
shared trust-chain verifier with `WendelonClientCertVerifier`
composition per ADR D8.

**Wendelon-internal symbols** (untouched):
`ActorIdentity`, `ActorTrustRoots`, `ActorPermission`,
`authorize_actor`, `authorize_break_glass_scope`,
`VerifiedTransportPeer`, `verified_peer_node_id`,
`verified_peer_node_id_from_identity`,
`ACTOR_CLAIMS_OID_DOTTED`, `HRFT_ALPN`, `RevocationSnapshot`,
all `extract_actor_*` helpers. Wendelon's inline DER parsing for
actor claims and inline CBOR decoding for actor claims **stays
unchanged in this cycle** per Codex iter-3 deferral framing.

## Phase 8: Architecture Guards

Add 11 guards + 1 convention guard to
`crates/toron/tests/architecture_test.rs` AND
`crates/wendelon/tests/architecture_test.rs` where appropriate.

Toron-side guards:

```rust
#[test]
fn adr047_tls_pki_crate_exists_under_workspace_glob() {
    let workspace_cargo = include_str!("../../../Cargo.toml");
    assert!(workspace_cargo.contains("\"crates/*\""),
        "workspace crates/* glob must cover crates/tls-pki");
    assert!(std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tls-pki/Cargo.toml")
        .exists(), "crates/tls-pki/Cargo.toml must exist");
}

#[test]
fn adr047_tls_pki_has_no_quinn_no_tokio_no_tracing_deps() {
    let cargo = include_str!("../../tls-pki/Cargo.toml");
    for forbidden in ["quinn", "tokio", "tracing", "async-trait", "hyper"] {
        assert!(!cargo.contains(forbidden),
            "tls-pki Cargo.toml must not include {forbidden}");
    }
}

#[test]
fn adr047_toron_tls_module_consumes_tls_pki_substrate() {
    let src = include_str!("../src/network/tls.rs");
    for required in [
        "tls_pki::",
        "TlsMaterial",
        "build_rustls_server_config",
        "build_rustls_client_config",
        "canonical_dns_name",
    ] {
        assert!(src.contains(required),
            "tls.rs must reference tls-pki API: {required}");
    }
    for preserved in [
        "pub enum TlsError",
        "pub enum ListenerRole",
        "pub enum PeerKind",
        "ExactServerCertVerifier",
        "SanClientCertVerifier",
        "canonical_daemon_dns_name",
        "canonical_client_dns_name",
    ] {
        assert!(src.contains(preserved),
            "Toron TLS policy token must remain local: {preserved}");
    }
}

#[test]
fn adr047_no_toron_internal_types_in_tls_pki_public_api() {
    let pki_src = include_str!("../../tls-pki/src/lib.rs");
    for forbidden in [
        "WorkerId", "tenant_id", "ToronWorker", "ToronDaemon",
        "quinn::", "tokio::",
    ] {
        assert!(!pki_src.contains(forbidden),
            "tls-pki/src/lib.rs must not contain {forbidden}");
    }
}

#[test]
fn adr047_no_alpn_constants_in_tls_pki() {
    for path in [
        "../../tls-pki/src/lib.rs",
        "../../tls-pki/src/material.rs",
        "../../tls-pki/src/asn1.rs",
        "../../tls-pki/src/cbor.rs",
        "../../tls-pki/src/san.rs",
        "../../tls-pki/src/config_builder.rs",
        "../../tls-pki/src/verifier.rs",
        "../../tls-pki/src/error.rs",
    ] {
        let src = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path)
        ).expect("read tls-pki source");
        for forbidden in ["ALPN", "HRFT_ALPN", "AUTH_QUIC_ALPN", "OBSERVABILITY_HTTP_ALPN"] {
            assert!(!src.contains(forbidden),
                "tls-pki/{} must not declare ALPN constants: {}", path, forbidden);
        }
    }
}

#[test]
fn adr047_toron_der_and_cbor_parsing_delegates_to_tls_pki() {
    let toron_tls = include_str!("../src/network/tls.rs");
    let toron_observability = include_str!("../src/network/observability.rs");
    let toron_observability_http = include_str!("../src/network/observability_http.rs");

    for src in [toron_tls, toron_observability, toron_observability_http] {
        for forbidden in ["DerReader::new", "CborReader::new"] {
            assert!(!src.contains(forbidden),
                "Toron DerReader/CborReader use must delegate to tls-pki: {forbidden}");
        }
    }
}
```

Wendelon-side guards:

```rust
#[test]
fn adr047_wendelon_tls_module_consumes_tls_pki_substrate() {
    let src = include_str!("../src/network/tls.rs");
    assert!(src.contains("use tls_pki::") || src.contains("pub use tls_pki::"),
        "wendelon/network/tls.rs must import from tls-pki");
    assert!(src.contains("tls_pki::canonical_dns_name"),
        "canonical_node_dns_name must delegate to tls_pki::canonical_dns_name");
}

#[test]
fn adr047_no_wendelon_internal_types_in_tls_pki_public_api() {
    let pki_src = include_str!("../../tls-pki/src/lib.rs");
    for forbidden in [
        "ActorIdentity", "ActorTrustRoots", "ActorPermission",
        "VerifiedTransportPeer", "ACTOR_CLAIMS_OID_DOTTED",
        "RevocationSnapshot", "WendelonNode", "NodeId",
        "authorize_actor", "authorize_break_glass",
    ] {
        assert!(!pki_src.contains(forbidden),
            "tls-pki must not contain Wendelon-internal: {forbidden}");
    }
}
```

Source-text guards cover #1-#9 + #11 + convention. Runtime
guard #10 is the prior-cycle guard-name existence + sweep gate
described in Phase 9.

## Phase 9: Integration Fixtures

### `crates/tls-pki/tests/tls_pki_tests.rs` (new)

Six required fixtures:

1. `tls_pki_canonical_dns_name_parametric_round_trips`
2. `tls_pki_admit_san_exact_shape_rejects_attack_classes`
3. `tls_pki_admit_san_rejects_disallowed_prefix`
4. `tls_pki_tls_material_load_rejects_env_interpolation`
5. `tls_pki_build_rustls_client_config_accepts_alpn_parameter`
6. `tls_pki_byte_identical_post_extraction_with_prior_cycle_guards`
   — promotion evidence: verify all `adr042_*` / `adr043_*` /
   `adr044_*` / `adr045_*` / `adr046_*` architecture guard names
   still exist and the prior ADR guard sweep passes after
   extraction. Phase 0 / Phase 10 evidence records pass-count and
   test-name inventories for the pre/post comparison rather than a
   committed stdout snapshot.

Test infrastructure uses `rcgen` for test cert generation (same
pattern as ADR 044/045/046 fixtures).

### Optional V1-evidence fixtures (non-gating)

- `wendelon_tls_pki_imports_compose_with_actor_permission` —
  end-to-end Wendelon-side verification that
  `WendelonClientCertVerifier` composes cleanly with `tls-pki`'s
  trust-chain verifier and Wendelon's `authorize_actor`.
- `wendelon_actor_claims_parse_through_tls_pki_asn1_cbor` —
  Wendelon-side migration witness for future use of
  `tls_pki::asn1` + `tls_pki::cbor` in actor-claim parsing.
  Validates primitives work for Wendelon's use case;
  Wendelon's V1 inline parsing stays unchanged.

## Phase 10: Verification

Capture in `plans/impl/047_*_evidence.md`:

1. **Phase 0 evidence:** `TlsMaterial` field-set parity
   confirmed; rustls version pin documented; Wendelon verifier
   composition seam identified; ADR 042–046 guard inventory
   captured (baseline counts); Toron-side inline DER check
   (expected: no inline DER usage).
2. **Phase 1-7 evidence:** `cargo test -p toron --tests`
   green; `cargo test -p wendelon --tests` green; `cargo test
   -p tls-pki --tests` green; `cargo clippy --workspace -- -D
   warnings` clean.
3. **Phase 8 evidence:** all 11 + 1 architecture guards pass.
4. **Phase 9 evidence:** all 6 required fixtures pass; optional
   V1-evidence fixtures captured separately.
5. **ADR 042/043/044/045/046 invariant preservation (critical
   evidence):**
   - All ADR 042/043/044/045/046 architecture guards pass after
     extraction. Captured as test count diff + test name list
     diff; no committed stdout snapshot is required.
   - All ADR 042/043/044/045/046 integration fixtures pass
     **byte-identical pre/post**.
   - `toron_fixture_worker.rs` source unchanged (12th
     preserved cycle).
   - `READY <worker>` / `SQL_READY <sql>` /
     `OBSERVABILITY_READY ...` stdout lines byte-identical.
   - SAN admission semantics byte-identical.
6. **No new operator-visible functionality:** `git diff HEAD
   --` shows no changes to public CLI surface, public daemon
   config surface, or public wire surface.
7. **No quinn / tokio / tracing / async deps in `tls-pki`:**
   `cargo tree -p tls-pki` confirms.
8. **Cross-ring discipline:** `tls-pki` public API contains no
   `quinn::*` / `tokio::*` / `WendelonNode` / `ToronWorker` /
   `NodeId` / `WorkerId` / `tenant_id` / `ActorIdentity` /
   `ActorPermission` / `RevocationSnapshot` types.
9. **Toron-side DER/CBOR delegation:** guard 11 passes; no
   `DerReader::new` / `CborReader::new` in Toron-side source
   outside `tls-pki`. Wendelon actor-claim parsing remains
   grandfathered for V1.

## Phase 11: Evidence Record

Materialize `plans/impl/047_*_evidence.md` with:

- Phase 0 audit findings.
- Phase 1-7 implementation evidence (workspace `Cargo.toml`
  diff, `crates/tls-pki/` skeleton, Toron-side substantial
  migration diff, Wendelon-side minimal-import diff).
- Phase 8 guard pass evidence.
- Phase 9 fixture pass evidence.
- Phase 10 verification evidence — **especially the prior-cycle
  guard + fixture pass-count and test-name inventories.** Critical
  refactor evidence.
- Risk register disposition.

## Acceptance Checklist

- [ ] Phase 0 audit captures `TlsMaterial` field-set parity,
      rustls version, Wendelon verifier composition seam,
      prior-cycle guard counts, Toron-side inline DER check.
- [ ] Phase 1 `crates/tls-pki/` workspace crate added; seven
      submodules declared; no quinn/tokio/tracing deps;
      clippy clean.
- [ ] Phase 2 `TlsMaterial::load` moved; env-interpolation
      rejection preserved; Wendelon keeps a thin local newtype
      wrapper preserving `std::io::Result<Self>`.
- [ ] Phase 3 `asn1` + `cbor` submodules implemented;
      `extract_dns_sans` used by Toron policy wrappers.
- [ ] Phase 4 `admit_san` + `canonical_dns_name` +
      `PeerIdentity` moved; UUID post-check stays Toron-side.
- [ ] Phase 5 rustls config builders + trust-chain verifier
      helpers moved; caller-owned policy verifier boundary
      preserved.
- [ ] Phase 6 Toron-side migration: substantial consumption;
      Toron-local ALPN constants + quinn wrapping + UUID
      post-check + canonical daemon/client/monitor wrappers;
      old substrate definitions deleted, policy definitions kept.
- [ ] Phase 7 Wendelon-side minimal-import migration:
      shared-substrate imports + one `canonical_node_dns_name`
      wrapper + local `PkiError` mapping helpers;
      Wendelon-internal machinery untouched.
- [ ] Phase 8 all 11 ADR 047 guards + 1 convention guard pass.
- [ ] Phase 9 all 6 required fixtures pass.
- [ ] **All ADR 042/043/044/045/046 architecture guards
      continue to pass after extraction (critical refactor
      invariant).**
- [ ] **All ADR 042/043/044/045/046 integration fixtures
      continue to pass after extraction.**
- [ ] `toron_fixture_worker.rs` source unchanged (12th
      preserved cycle).
- [ ] `crates/nilket/Cargo.toml` untouched.
- [ ] No `quinn` / `tokio` / `tracing` in `cargo tree -p tls-pki`.
- [ ] Phase 11 evidence record materialized; pre/post guard and
      fixture pass-count/test-name inventories documented.

## Non-Goals

(Mirrors ADR 047 Out of Scope.)

- HTTP/1.1 helper extraction (`observability-http`).
- Toron CLI consolidation (`SqlClientTlsOptions` +
  `InspectTlsOptions` merge).
- `GenericCertVerifier<F>` closure-based verifier.
- `ActorPermission` parity Toron-side.
- Revocation surface (CRL / OCSP / `RevocationSnapshot`) in
  `tls-pki`.
- Multi-tenant routing.
- OTLP traces export.
- Audit subsystem in Toron.
- Live cert rotation hooks pre-emptively in `tls-pki`.
- Token-based auth.
- Quinn-specific wrapping in `tls-pki`.
- Async / tokio integration in `tls-pki`.
- `tls-pki` versioning / publishing.
- Wendelon actor-permission machinery move.
- Wendelon-side inline DER/CBOR migration.
- Wendelon's `network::tls` module restructure beyond minimal
  imports.
- SAN admission with regex / glob patterns.
- Cross-ring shared error enum spanning ring-internal
  variants.
- Strict no-inline-DER enforcement Wendelon-side.

## Risk Register

1. **`TlsMaterial` field-set parity divergence between
   Wendelon and Toron.** Mitigation: Phase 0 audit explicitly
   confirms parity; reconciliation path identified if
   divergent.
2. **Wendelon's verifier composition seam doesn't accept
   `tls-pki`'s `Arc<dyn ClientCertVerifier>`.** Mitigation:
   Phase 0 audit identifies the seam; D8 sketch is a Claude
   prediction that Phase 0 confirms.
3. **rustls version drift between rings.** Mitigation:
   Phase 0 audit captures both pins; `tls-pki` adopts the
   existing shared pin rather than introducing a second TLS
   stack.
4. **Error conversion regression at the Wendelon boundary.**
   The `tls_pki::PkiError` → `std::io::Error` conversion may
   change error message text in subtle ways. Mitigation:
   Wendelon-side existing test suite asserts byte-identical
   error messages on prior-cycle fixtures; Phase 10
   invariant verification flags any diff.
5. **Toron-side `admit_san` UUID post-check skew from prior
   cycles.** Mitigation: fixture coverage asserts UUID
   rejection for malformed `toron-` SANs through Toron's
   wrapper.
6. **`extract_dns_sans` implementation correctness.** Hand-
   rolled DER parsing risks correctness bugs. Mitigation:
   Wendelon's existing inline DER parsing is the reference
   implementation Phase 3 mirrors. Fixture 2 exercises
   subdomain injection, wildcard, underscore, malformed DNS
   shape through `tls_pki::admit_san`; Toron fixtures preserve
   malformed `toron-` UUID rejection through the local wrapper.
7. **Refactor blast radius across 6 crates** (`tls-pki`,
   `toron`, `wendelon`, plus test crates that import migrated
   types). Mitigation: Phase 0 audit captures import sites;
   migration done in phases (Phase 2 first, validate; Phase 3
   next, validate; etc.).
8. **D8 caller-owned verifier sketch wrong.** The
   `WendelonClientCertVerifier` shape in D8 is a Claude
   prediction. Phase 0 audit may surface a different
   composition shape. Mitigation: D8 documented as the
   sketch; Phase 0 audit confirms or refines.
9. **`PkiError` leaks into Toron public policy surface.**
   ADR 044 guards expect Toron's `pub enum TlsError` and local
   role-rejection tokens. Mitigation: keep `TlsError` local and
   map `PkiError` into existing variants at the wrapper boundary.
10. **`tls_pki::asn1` + `tls_pki::cbor` implementation
    complexity.** Hand-rolled DER + CBOR primitives are
    non-trivial. Mitigation: scope is minimal (only the
    primitives needed for `extract_dns_sans` are V1-load-
    bearing; `extract_subject` + `extract_extension` +
    `CborReader` are exposed for future use but Wendelon V1
    keeps inline parsing; correctness verification limited to
    primitives actually used by `tls-pki`-internal callers).

## Cross-References

- ADR file (paired):
  `plans/adr/047_shared_tls_pki_workspace_crate.md`.
- Wendelon TLS module (Phase 0 audit reference):
  `crates/wendelon/src/network/tls.rs`.
- Toron TLS module (Phase 6 migration target):
  `crates/toron/src/network/tls.rs`.
- ADR 044 D5 SAN admission discipline (preserved):
  `plans/adr/044_authenticated_remote_toron_quic_listeners.md`.
- ADR 045 D3 `monitor-` SAN prefix (preserved):
  `plans/adr/045_toron_daemon_observability_listeners.md`.
- ADR 046 internal `build_rustls_client_config` partial
  extraction (completed by ADR 047):
  `plans/adr/046_nilket_toron_inspect_real_wiring.md`.
- Iter brainstorm trail:
  `crates/toron/plans/wip/claude_iter_{0..3}_adr_047.md` and
  `crates/toron/plans/wip/claude_iter_{2,3}_impl_047.md`.
