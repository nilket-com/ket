# ADR 047: Shared `tls-pki` Workspace Crate

**Status:** Promoted
**Seed:** ADR 046 promotion §"ADR 047 candidates"
(`crates/toron/plans/wip/roadmap.md`); ADR 044 Plan W-B explicit
follow-up; ADR 046 internal `build_rustls_client_config`
partial-extraction half-step; accumulated parallel TLS
construction across ADR 043 + ADR 044 + ADR 045 + ADR 046.
**Council:** Claude, Codex, Gemini (3 iterations: iter-0 topic-
shape survey with Shape A `tls-pki` extraction selected over
Shapes B-F; iter-1 Claude three retractions — R1 Shape A full
cross-ring migration retracted after Wendelon `network::tls.rs`
audit (~30% shareable, not ~70%); R2 shared `TlsError` enum
retracted because Wendelon returns `std::io::Result<T>`; R3 Plan
A/Plan B framing collapsed to single-plan; iter-2 three preemptive
sharpenings — D5 error conversion direction pinned, D6
`PeerIdentity` open struct confirmed, D8 caller-owned verifier
boundary pinned; iter-3 two convergence
sharpenings driven by Codex+Gemini iter-3 — D1 module scope
expanded to seven submodules adding `asn1` + `cbor` per
Codex+Gemini 2-of-3 convergence, shared-crate error type named
`PkiError` to reflect expanded scope while Toron's `TlsError`
remains local; iter-4 not needed; promoted
from iter-3 ratification). **First refactor cycle in the
ADR 040-onward sequence.**
**Category:** Refactor ADR — behavior-preserving cross-ring
extraction with zero functional changes.

## Summary

Extract a shared `crates/tls-pki/` workspace crate consumed by
both Wendelon and Toron, consolidating cert/key/CA PEM loading,
rustls config builders (parameterized by ALPN), exact-shape SAN
admission verification (parameterized by allowed prefix set),
canonical DNS name format helpers (parameterized by ring prefix),
ASN.1/DER parsing primitives (`DerReader`, `extract_dns_sans`,
`extract_subject`, `extract_extension`), CBOR reader primitives
for Wendelon actor-claim decoding, and rustls config assembly
helpers that accept caller-owned verifier policy. Each ring keeps
its own admission verifier while sharing cert/key/CA loading and
config construction.

ADR 047 is a **pure behavior-preserving refactor** — zero
functional changes, zero wire changes, zero config changes, zero
new operator-visible functionality. All
ADR 042/043/044/045/046 architecture guards and integration
fixtures continue to pass after extraction; prior stable-output and
diagnostic invariants remain enforced by their own guards and
fixtures.

Toron-side migration consumes substantially through public
re-exports + Toron-local ALPN constants + quinn wrapping +
existing canonical daemon/client/monitor wrappers + UUID post-check
for `toron-` prefix admission. Wendelon-side migration is
**minimal-import-only**: shared-substrate imports plus one
`canonical_node_dns_name` wrapper and local `PkiError` →
`std::io::Error` mapping helpers at Wendelon call sites. Wendelon's
ring-internal machinery (`ActorIdentity`, `ActorTrustRoots`,
`authorize_actor`, `ACTOR_CLAIMS_OID_DOTTED`,
`VerifiedTransportPeer`, `verified_peer_node_id`,
`authorize_break_glass_scope`, `HRFT_ALPN`, `RevocationSnapshot`,
inline DER/CBOR parsing for actor claims) stays untouched.

**No** ALPN constants in `tls-pki` (ring-specific protocol
tokens). **No** quinn / tokio / tracing dependencies in `tls-pki`
(pure-sync rustls/asn1/cbor substrate library). **No**
Wendelon-internal types in `tls-pki` public API. **No**
Toron-internal types in `tls-pki` public API. **No** new
operator-visible functionality. **No** wire / config / endpoint
changes. **No** Toron CLI consolidation of `sql` + `inspect` TLS
options (rejected 2-of-3 Codex+Claude as out-of-scope for
cross-ring substrate ADR; future Toron-side cleanup ADR
candidate). **No** `GenericCertVerifier<F>` closure-based
verifier helper (D8 ships the broader caller-owned trait-object
verifier boundary plus trust-chain helpers instead). **No** Plan A /
Plan B sub-lift framing (single-plan posture, matches
ADR 042/043/044/045/046).

## Motivation

ADR 044 Phase 0 selected Plan W-C (Toron-local TLS module)
because Plan W-B (shared crate extraction) "would broaden the
substrate cycle." ADR 044 explicitly named Plan W-B as a future
follow-up. Two cycles later (ADR 046), the implementation
introduced `build_rustls_client_config(material, alpn)` as a
shared internal helper inside Toron's `network::tls.rs` — a
half-step toward extraction (flagged in ADR 046 code review as
a partial-refactor of ADR 043's TLS construction path). ADR 047
completes the extraction across the workspace.

Four cycles of accumulated parallel TLS construction (ADR 043
SQL client TLS, ADR 044 listener policy, ADR 045 observability
mTLS acceptor, ADR 046 inspect client TLS) plus Wendelon's
original `network::tls.rs` have produced drift risk in
SAN-admission semantics, cert loading error handling, and rustls
config builder shape. Consolidating into one shared crate
eliminates the drift risk and clarifies the cross-ring TLS
contract.

Composition substrate value: future ADRs (multi-tenant routing,
token-based auth, live cert rotation, `ActorPermission` parity,
OTLP, audit subsystem, Wendelon-side full DER/CBOR migration)
compose over one shared TLS+PKI module instead of two.

## Decisions (10, Promotion-Locked)

**D1: Create `crates/tls-pki/` workspace crate with seven-submodule scope.**

New crate at `crates/tls-pki/`. The root workspace already has a
`crates/*` member glob, so no explicit root `Cargo.toml` member line
is required unless implementation changes the workspace layout.
Brand-clean name (no `nilket-` / `toron-` / `wendelon-` prefix; no
V1 suffix).

Module structure:

```
crates/tls-pki/src/
├── lib.rs            (module exports + DEFAULT_CLUSTER_DOMAIN)
├── material.rs       (TlsMaterial + PEM loaders + env-interpolation rejection)
├── asn1.rs           (DerReader + extract_dns_sans + extract_subject + extract_extension)
├── cbor.rs           (CborReader + primitives)
├── san.rs            (admit_san + canonical_dns_name + PeerIdentity)
├── config_builder.rs (build_rustls_server_config + build_rustls_client_config)
├── verifier.rs       (trust-chain verifier helpers; policy stays caller-local)
└── error.rs          (PkiError)
```

Public API (final):

```rust
pub struct TlsMaterial {
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    pub ca_roots: Arc<RootCertStore>,
}

impl TlsMaterial {
    pub fn load(cert_path: &Path, key_path: &Path, ca_path: &Path) -> Result<Self, PkiError>;
}

// asn1 module — neutral DER parsing primitives.
pub struct DerReader<'a> { /* ... */ }
pub fn extract_dns_sans(cert: &CertificateDer) -> Result<Vec<String>, PkiError>;
pub fn extract_subject(cert: &CertificateDer) -> Result<Subject, PkiError>;
pub fn extract_extension(cert: &CertificateDer, oid: &[u32]) -> Result<Option<Vec<u8>>, PkiError>;

// cbor module — neutral CBOR primitives.
pub struct CborReader<'a> { /* ... */ }

// san module — exact-shape admission + canonical name format.
pub struct PeerIdentity {
    pub prefix: String,
    pub id_token: String,
    pub cluster_domain: String,
}

pub fn admit_san(
    san: &str,
    cluster_domain: &str,
    allowed_prefixes: &[&str],
) -> Result<PeerIdentity, PkiError>;

pub fn canonical_dns_name(
    prefix: &str,
    id_token: &str,
    cluster_domain: &str,
) -> String;

// config_builder module — rustls config assembly with caller-owned policy.
pub fn build_rustls_server_config(
    material: &TlsMaterial,
    client_verifier: Arc<dyn ClientCertVerifier>,
    alpn: &[u8],
) -> Result<rustls::ServerConfig, PkiError>;

pub fn build_rustls_client_config(
    material: &TlsMaterial,
    server_verifier: Arc<dyn ServerCertVerifier>,
    alpn: &[u8],
) -> Result<rustls::ClientConfig, PkiError>;

// verifier module — trust-chain helpers only.
pub fn build_client_cert_chain_verifier(
    ca_roots: Arc<RootCertStore>,
) -> Result<Arc<dyn ClientCertVerifier>, PkiError>;

pub fn build_server_cert_chain_verifier(
    ca_roots: Arc<RootCertStore>,
) -> Result<Arc<dyn ServerCertVerifier>, PkiError>;

pub const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";

#[derive(Debug, thiserror::Error)]
pub enum PkiError {
    CertRead { path: PathBuf, source: rustls::pki_types::pem::Error },
    CertParse { path: PathBuf },
    KeyRead { path: PathBuf, source: std::io::Error },
    KeyParseNotPkcs8 { path: PathBuf },
    CaRead { path: PathBuf, source: rustls::pki_types::pem::Error },
    CaParse { path: PathBuf },
    EnvInterpolation { path: PathBuf },
    SanShapeRejected { reason: String },
    SanPrefixRejected { prefix: String, allowed: Vec<String> },
    DerParse { reason: String },
    CborParse { reason: String },
    Config(String),
}
```

**Not in `tls-pki`** (ring-internal; carries forward iter-1 R1
audit findings):

- ALPN constants (`HRFT_ALPN`, `AUTH_QUIC_ALPN`,
  `OBSERVABILITY_HTTP_ALPN` all stay ring-local).
- Wendelon's `ActorIdentity`, `ActorTrustRoots`,
  `ActorPermission`, `authorize_actor`,
  `authorize_break_glass_scope`, `VerifiedTransportPeer`,
  `verified_peer_node_id`, `ACTOR_CLAIMS_OID_DOTTED`,
  `RevocationSnapshot`.
- Quinn config wrapping (`quinn::ServerConfig` /
  `quinn::ClientConfig` wrapping stays each ring's
  `network::tls.rs`).
- Async / tokio integration (`tls-pki` is pure-sync substrate).
- Toron's `WorkerId`, `tenant_id`, `ToronWorker`,
  `ToronDaemon` types.
- Toron CLI options structs (`SqlClientTlsOptions`,
  `InspectTlsOptions`) — Toron-local cleanup; future ADR
  candidate.

**Crate dependencies:** `rustls`, `rustls-pki-types`, `thiserror`;
`rcgen` may be a dev-dependency for generated test certificates. Match
the current Toron/Wendelon rustls pins.
**No** quinn / tokio / tracing / async-runtime deps. Guard 2
enforces.

**D2: Behavior-preserving refactor; zero functional changes.**

ADR 047 ships **zero functional changes**. All
ADR 042/043/044/045/046 architecture guards continue to pass after
extraction. All prior-cycle integration fixtures continue to pass
after extraction, with stable output / diagnostic invariants covered
by their existing tests.

Specifically preserved:

- ADR 044 D5 `admit_san` exact-shape verifier semantics
  (subdomain injection, wildcard, underscore, and malformed
  `toron-` UUID all rejected through Toron's wrapper).
- ADR 044 `admit_san` admission set
  `<prefix>` ∈ {`toron`, `client`, `monitor`} unchanged.
- ADR 044 D5 UUID requirement for `toron-` prefix (preserved
  via Toron-side post-check after `tls_pki::admit_san`).
- ADR 045 `monitor-` SAN prefix admission on `/ready` /
  `/metrics`.
- ADR 045 D6 `ReadinessSnapshot` JSON shape.
- ADR 046 `nilket toron inspect` exit code semantics (0/1/2).
- Wendelon's `node-<id>.<cluster_domain>` SAN convention.
- Wendelon's `authorize_actor` admission composition (via
  `WendelonClientCertVerifier` wrapping `tls-pki`'s base
  verifier per D8).
- Stable diagnostic strings on all error paths.

Architecture guard `adr047_byte_identical_post_extraction_with_prior_cycle_guards`
asserts: all ADR 042/043/044/045/046 architecture guard function
names continue to exist and the ADR 04x guard sweep passes after
extraction.

**D3: Wendelon-side migration is minimal-import-only.**

Wendelon's `crates/wendelon/src/network/tls.rs` adds shared
imports plus a compatibility wrapper that preserves Wendelon's
existing `std::io::Result` load signature:

```rust
pub use tls_pki::DEFAULT_CLUSTER_DOMAIN;

pub struct TlsMaterial(tls_pki::TlsMaterial);

impl TlsMaterial {
    pub fn load(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
        ca_path: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        tls_pki::TlsMaterial::load(cert_path.as_ref(), key_path.as_ref(), ca_path.as_ref())
            .map(Self)
            .map_err(pki_error_to_io)
    }
}

impl std::ops::Deref for TlsMaterial {
    type Target = tls_pki::TlsMaterial;
    fn deref(&self) -> &Self::Target { &self.0 }
}

pub fn canonical_node_dns_name(node_id: u32, cluster_domain: &str) -> String {
    tls_pki::canonical_dns_name("node", &node_id.to_string(), cluster_domain)
}
```

**What stays Wendelon-internal** (no migration):

- `ActorIdentity`, `ActorTrustRoots`, `ActorPermission`,
  `authorize_actor`, `authorize_break_glass_scope`.
- `VerifiedTransportPeer`, `verified_peer_node_id`,
  `verified_peer_node_id_from_identity`.
- `ACTOR_CLAIMS_OID_DOTTED`.
- `HRFT_ALPN`.
- Wendelon's inline DER parsing for actor-claim extraction
  (grandfathered for V1; future ADR can migrate to
  `tls_pki::asn1`).
- Wendelon's inline CBOR decoding for actor-claim parsing
  (grandfathered for V1; future ADR can migrate to
  `tls_pki::cbor`).
- `rustls_server_config` (no-arg variant), `rustls_server_config_with_alpn`,
  `rustls_server_config_with_peer_roots`, `server_config`,
  `client_config` — Wendelon-internal wrappers around the
  shared builders.

Wendelon's existing public API surface remains byte-identical at
the function-signature level. `TlsMaterial` remains Wendelon-local
as a thin wrapper solely so `TlsMaterial::load` continues to return
`std::io::Result<Self>`; field access still dereferences to the
shared substrate. Some functions now delegate internally to
`tls-pki`, but external callers see the same shapes. Wendelon's
existing test suite continues to pass byte-identical.

**D4: Toron-side migration consumes substrate while preserving policy surface.**

Toron's `crates/toron/src/network/tls.rs` consumes `tls-pki`
for material loading, DER extraction, provider installation, and
rustls config assembly. It must preserve the ADR 044 public policy
tokens already guarded by architecture tests:

```rust
use tls_pki::{
    TlsMaterial,
    build_rustls_server_config, build_rustls_client_config,
    build_client_cert_chain_verifier, build_server_cert_chain_verifier,
    canonical_dns_name,
};

// Toron-local policy and protocol names remain present.
pub const DEFAULT_CLUSTER_DOMAIN: &str = tls_pki::DEFAULT_CLUSTER_DOMAIN;
pub const AUTH_QUIC_ALPN: &[u8] = b"sql-task-auth";
pub const OBSERVABILITY_HTTP_ALPN: &[u8] = b"http/1.1";
pub enum TlsError { /* existing ADR 044 variants preserved */ }
pub enum ListenerRole { WorkerTask, Sql, Observability }
pub enum PeerKind { /* existing ADR 044 variants preserved */ }

pub fn canonical_daemon_dns_name(worker_id: &WorkerId, cluster_domain: &str) -> String {
    tls_pki::canonical_dns_name("toron", &worker_id.get().to_string(), cluster_domain)
}

pub fn admit_san(
    san: &str,
    role: ListenerRole,
    cluster_domain: &str,
) -> Result<PeerKind, TlsError> {
    // Delegates exact DNS parsing to tls_pki::admit_san, then preserves
    // Toron's listener-role mapping and toron- UUID post-check locally.
}

struct ExactServerCertVerifier { /* delegates chain verification, keeps Toron policy */ }
struct SanClientCertVerifier { /* delegates chain verification, keeps Toron policy */ }

// Toron-side quinn wrapping remains local.
pub fn client_config_from_paths(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
) -> Result<quinn::ClientConfig, TlsError> {
    let material = tls_pki::TlsMaterial::load(cert_path, key_path, ca_path)?;
    // Build Toron's ExactServerCertVerifier locally, then pass it to tls-pki.
}

pub fn http_client_config_from_paths(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
) -> Result<rustls::ClientConfig, TlsError> {
    let material = tls_pki::TlsMaterial::load(cert_path, key_path, ca_path)?;
    // Same local verifier policy, HTTP ALPN.
}
```

Existing ADR 046 internal helper `build_rustls_client_config`
deletes (moved to `tls-pki`). Existing low-level `TlsMaterial::load`
deletes (moved to `tls-pki`). Existing Toron policy surface remains:
`TlsError`, `admit_san`, `ExactServerCertVerifier`,
`SanClientCertVerifier`, and canonical daemon/client/monitor DNS
helpers stay in Toron so ADR 044 guards remain valid.

**D5: `PkiError` is `tls-pki`-local; conversion at ring boundaries.**

`tls-pki` exports `pub enum PkiError` (full variant list in D1).

**Wendelon-side error conversion:** Wendelon's existing helpers
return `std::io::Result<T>`. Wendelon wrapper code converts
`tls_pki::PkiError` → `std::io::Error` at the boundary (per D3).
Wendelon's external API signatures remain byte-identical; in
particular, `TlsMaterial::load` remains
`std::io::Result<TlsMaterial>` via the local thin wrapper.

**Toron-side error conversion:** Toron keeps its existing
`TlsError` enum and maps `tls_pki::PkiError` into the existing
variants at the wrapper boundary. ADR 044 source guards that
expect `pub enum TlsError` and `TlsError::SanRoleRejected`
continue to pass unchanged. Operator-facing diagnostic content is
byte-identical pre/post (Phase 8 verification asserts).

**D6: `PeerIdentity` is an open struct with public fields.**

`pub struct PeerIdentity { pub prefix: String, pub id_token:
String, pub cluster_domain: String }`. Open struct; public
fields; no accessor methods.

Rationale: both rings need field access for ring-side
composition layers (Wendelon maps `id_token` → `NodeId`; Toron
treats `id_token` as opaque UUID string). Accessor methods add
ceremony without security benefit.

**D7: SAN admission abstraction parameterized by allowed prefix set.**

`admit_san(san, cluster_domain, allowed_prefixes: &[&str])
-> Result<PeerIdentity, PkiError>`. Exact-shape verifier
semantics from ADR 044 D5 preserved byte-identical:

- exactly one leading label before the `cluster_domain` labels
  (no extra subdomain);
- `<prefix>` must appear in `allowed_prefixes`;
- `<id_token>` matches `[A-Za-z0-9-]+`;
- no dots / underscores / wildcards in the id_token.

Toron consumes with `&["toron", "client", "monitor"]`-style
prefix arrays + UUID post-check for `toron-`. Wendelon doesn't
consume `admit_san` in V1 (Wendelon's `authorize_actor` is a
different admission layer composed via D8); future Wendelon
adoption is optional but not required.

**D8: Verifier composition uses caller-owned policy.**

`tls-pki` exposes rustls trust-chain verifier helpers and config
builders that accept caller-owned verifier objects. Each ring then
layers its own policy locally:

```rust
pub fn build_client_cert_chain_verifier(
    ca_roots: Arc<RootCertStore>,
) -> Result<Arc<dyn ClientCertVerifier>, PkiError>;

pub fn build_server_cert_chain_verifier(
    ca_roots: Arc<RootCertStore>,
) -> Result<Arc<dyn ServerCertVerifier>, PkiError>;
```

Toron's `SanClientCertVerifier` and `ExactServerCertVerifier`
remain Toron-local wrappers around these helpers. Wendelon's
actor verifier remains Wendelon-local and may wrap the shared
trust-chain helper before applying `authorize_actor`, revocation,
and break-glass checks.

Concrete Wendelon-side composition shape:

```rust
struct WendelonClientCertVerifier {
    inner: Arc<dyn rustls::server::danger::ClientCertVerifier>,
    actor_trust_roots: Arc<ActorTrustRoots>,
    permission: ActorPermission,
    revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
}

impl rustls::server::danger::ClientCertVerifier for WendelonClientCertVerifier {
    fn verify_client_cert(&self, ...) -> Result<..., rustls::Error> {
        // 1. tls-pki trust-chain verification via self.inner.
        self.inner.verify_client_cert(...)?;
        // 2. Extract actor claims from cert extensions
        //    (Wendelon-internal OID parsing; may migrate to
        //    tls_pki::asn1 + tls_pki::cbor in a future cycle).
        let actor = extract_actor_identity(end_entity, &self.actor_trust_roots)?;
        // 3. Wendelon-internal authorize_actor + revocation check.
        authorize_actor(&actor, self.permission)?;
        if let Some(revocations) = &self.revocations { ... }
        Ok(ClientCertVerified::assertion())
    }
}
```

`tls-pki` does not know about `ActorIdentity` / `ActorPermission`
/ revocation / Toron listener roles. The composition is ring-side.

Gemini's `GenericCertVerifier<F>` closure-based approach did not
ship. ADR 047 instead keeps the substrate boundary broader:
`tls-pki` owns trust-chain helpers, while Toron and Wendelon own
their ring-specific trait-object policy wrappers.

**D9: Single-plan posture; cross-ring discipline preserved.**

ADR 047 ships single-plan (matches ADR 042/043/044/045/046).
ADR 042 D15 invariant ("Toron must not import Wendelon server
internals") strengthens: `tls-pki` is the only inter-ring shared
library for TLS+PKI concerns. Toron-Wendelon coupling otherwise
stays at `wendelon_client` for non-TLS concerns.

**D10: Workspace lints / clippy / formatting consistency.**

`tls-pki` adopts the workspace's existing
clippy/rustfmt/edition configuration. **No** new lint deviations
introduced. **No** unsafe code (TLS verifier construction is all
safe rustls; ASN.1/CBOR parsing uses safe slice arithmetic).
Crate-level docs in `tls-pki/src/lib.rs` explain the cross-ring
intent.

## Architecture Guards (11, Promotion-Locked)

1. `adr047_tls_pki_crate_exists_under_workspace_glob` —
   `crates/tls-pki/Cargo.toml` exists and the root workspace
   `crates/*` member glob covers it.
2. `adr047_tls_pki_has_no_quinn_no_tokio_no_tracing_deps` —
   `crates/tls-pki/Cargo.toml` doesn't list quinn / tokio /
   tracing.
3. `adr047_toron_tls_module_consumes_tls_pki_substrate` —
   `crates/toron/src/network/tls.rs` contains
   `tls_pki::` imports for material/config/parsing substrate while
   retaining Toron-local policy tokens.
4. `adr047_wendelon_tls_module_consumes_tls_pki_substrate` —
   `crates/wendelon/src/network/tls.rs` contains
   `tls_pki::` imports for shared helpers.
5. `adr047_no_wendelon_internal_types_in_tls_pki_public_api`
   — `tls-pki/src/lib.rs` doesn't reference
   `RevocationSnapshot`, `ActorPermission`, `ActorIdentity`,
   `ActorTrustRoots`, `VerifiedTransportPeer`,
   `ACTOR_CLAIMS_OID_DOTTED`, `WendelonNode`, `NodeId`,
   `authorize_break_glass_scope`, `authorize_actor`.
6. `adr047_no_toron_internal_types_in_tls_pki_public_api` —
   `tls-pki/src/lib.rs` doesn't reference `WorkerId`,
   `tenant_id`, `ToronWorker`, `ToronDaemon`, or any
   `quinn::*` types.
7. `adr047_admit_san_accepts_parameterized_prefix_set` —
   `tls-pki::admit_san` signature takes
   `allowed_prefixes: &[&str]`.
8. `adr047_canonical_dns_name_parameterized_by_prefix` —
   `tls-pki::canonical_dns_name` signature takes `prefix:
   &str`.
9. `adr047_no_alpn_constants_in_tls_pki` — `tls-pki` does not
   declare ALPN constants; ALPN values stay ring-local.
10. `adr047_byte_identical_post_extraction_with_prior_cycle_guards`
    — meta-guard asserting all ADR 042/043/044/045/046
    architecture guard function names continue to exist and
    the ADR 04x guard sweep passes after extraction.
11. `adr047_toron_der_and_cbor_parsing_delegates_to_tls_pki` —
    source-text guard asserting `DerReader` and `CborReader`
    patterns do not appear in Toron source outside `tls-pki`.
    Wendelon's actor-claim DER/CBOR parsing is explicitly
    grandfathered for V1 and not covered by this guard.

Post-promotion convention guard:

- **`adr047_new_tls_pki_types_are_brand_clean_and_no_v1_suffix`**
  — twelfth consecutive cycle preserving the no-V1-suffix
  convention (ADRs 039 / 041 / 042 / 043 / 044 / 045 / 046 /
  047). `TlsMaterial`, `PkiError`, `PeerIdentity`, `DerReader`,
  `CborReader` carry no V1 suffix and no `Toron*` / `Wendelon*`
  brand prefix.

## Integration Fixtures (6 required + 2 V1-evidence optional)

**Required (gates promotion):**

1. `tls_pki_canonical_dns_name_parametric_round_trips` —
   `canonical_dns_name("node", "1", "cluster.local")` ==
   `"node-1.cluster.local"`;
   `canonical_dns_name("toron", "<uuid>", "cluster.local")`
   == `"toron-<uuid>.cluster.local"`. One function, two
   ring-distinct call patterns.
2. `tls_pki_admit_san_exact_shape_rejects_attack_classes` —
   ADR 044 D5 exact-shape attack-class rejection exercised
   through the `tls-pki` public API with `&["toron", "client",
   "monitor"]` prefix set. Asserts rejection of subdomain
   injection, wildcards, underscores, and malformed DNS shape.
   Malformed `toron-` UUID rejection remains a Toron-wrapper
   fixture because UUID semantics are Toron policy.
3. `tls_pki_admit_san_rejects_disallowed_prefix` — `monitor-`
   SAN rejected when `allowed_prefixes` is `&["node"]`
   (would-be Wendelon-scenario); accepted when
   `allowed_prefixes` is `&["monitor"]` (Toron scenario).
4. `tls_pki_tls_material_load_rejects_env_interpolation` —
   ADR 044 D6 env-interpolation rejection (`${VAR}` patterns
   in cert/key/CA paths) through `TlsMaterial::load`.
5. `tls_pki_build_rustls_client_config_accepts_alpn_parameter`
   — two different ALPN values (`b"sql-task-auth"`,
   `b"http/1.1"`) build distinct-yet-valid
   `rustls::ClientConfig` instances. Constants passed in
   from caller-local definitions.
6. `tls_pki_byte_identical_post_extraction_with_prior_cycle_guards`
   — promotion evidence asserting all ADR 042/043/044/045/046
   architecture guards continue to exist and pass after extraction,
   with Phase 0 / Phase 10 pass-count and test-name inventories
   recorded for the pre/post comparison. The committed guard is an
   executable existence + sweep gate, not a committed stdout snapshot.

**Optional V1-evidence (non-gating, post-promotion):**

- `wendelon_tls_pki_imports_compose_with_actor_permission`
  — Wendelon-side verification that
  `WendelonClientCertVerifier` composes cleanly with `tls-pki`'s
  trust-chain verifier and Wendelon's `authorize_actor`.
- `wendelon_actor_claims_parse_through_tls_pki_asn1_cbor` —
  Wendelon-side migration witness for future use of
  `tls_pki::asn1::extract_extension` + `tls_pki::cbor::CborReader`
  in actor-claim parsing. Validates the primitives work for
  Wendelon's use case; Wendelon's V1 inline parsing stays.

## Sub-Lift Posture

**Single-plan.** No Plan A / Plan B framing (retracted in iter-1
R3 after Wendelon-side code audit revealed only ~30% shareable
surface).

First refactor cycle in the ADR 040-onward sequence; the
cycle-shape discipline holds (refactor cycles are their own
category, not mixed with substrate or composition).

## Out of Scope (Recorded for Future ADRs)

- **HTTP/1.1 helper extraction** (`observability-http` shared
  crate). Future ADR if cross-ring HTTP usage warrants.
- **Toron CLI consolidation** (`SqlClientTlsOptions` +
  `InspectTlsOptions` merge). **Rejected 2-of-3 Codex+Claude**
  as out-of-scope for cross-ring substrate ADR. Future
  Toron-local cleanup ADR candidate.
- **`GenericCertVerifier<F>` closure-based verifier.**
  Not shipped in ADR 047. D8 ships a trait-object caller-owned
  verifier boundary plus trust-chain helpers; a closure helper can
  be revisited only if future ergonomics pressure appears.
- **`ActorPermission` model parallel Toron-side.** ADR 044 D5
  uses SAN-prefix admission; ADR 047 doesn't introduce
  `ActorPermission` to Toron.
- **Revocation surface (CRL / OCSP / `RevocationSnapshot`)** in
  `tls-pki`. Stays Wendelon-internal; ADR 044 D7 / ADR 045 D7
  deferrals preserved.
- **Multi-tenant routing.** Future substrate cycle now
  genuinely unblocked.
- **OTLP traces export.** Future ADR.
- **Audit subsystem in Toron.** Future ADR.
- **Live cert rotation hooks.** Future ADR; `tls-pki` doesn't
  ship pre-emptive rotation API.
- **Token-based auth (bearer / JWT / OIDC).** Future ADR
  composes over `tls-pki` admission.
- **Quinn-specific config wrapping in `tls-pki`.** Stays
  ring-local.
- **Async / tokio integration in `tls-pki`.** Pure-sync
  substrate library.
- **`tls-pki` versioning / publishing to crates.io.** Stays
  workspace-internal V1.
- **Wendelon's actor-permission machinery move to `tls-pki`.**
  Wendelon-internal; not eligible for shared-crate hosting.
- **Wendelon's inline DER/CBOR migration to `tls_pki::asn1` /
  `tls_pki::cbor`.** Grandfathered for V1; future Wendelon-side
  ADR can migrate.
- **Wendelon's `network::tls` module restructure beyond
  re-export imports.** Wendelon's existing module hierarchy
  unchanged.
- **SAN admission with regex / glob patterns.** Exact-shape
  verifier stays exact-shape per ADR 044 D5.
- **Cross-ring shared error enum spanning ring-internal
  variants.** `tls_pki::PkiError` covers only `tls-pki`-shaped
  errors; ring-specific errors stay ring-local.
- **Strict no-inline-DER enforcement Wendelon-side.** Guard 11
  scope is Toron-side only for V1.

## Council History

- **Iter 0:** Claude proposed Shape A (full cross-ring
  `tls-pki` extraction with both Wendelon and Toron consuming
  substantially), surveyed Shapes B-F. Took positions on
  brand-clean crate name, Plan A/Plan B fallback framing,
  shared `TlsError` enum, parameterized `admit_san` /
  `canonical_dns_name`.
- **Iter 1:** Code audit of Wendelon's `network::tls.rs`
  (~635 lines) revealed Wendelon's TLS module is deeply
  entangled with ring-internal mechanisms (actor permissions,
  custom X.509 OID, verified peer node id, break-glass scopes).
  Claude retracted three iter-0 positions: R1 — Shape A
  retracted because Wendelon-side shareable surface is ~30%
  not ~70%; R2 — shared `TlsError` enum retracted because
  Wendelon returns `std::io::Result<T>` from existing helpers;
  R3 — Plan A / Plan B framing collapsed to single-plan
  posture. Wendelon-side migration scope reduced to
  minimal-import-only.
- **Iter 2:** Three preemptive sharpenings: D5 error
  conversion direction pinned; D6 `PeerIdentity` open struct
  confirmed; D8 caller-owned verifier boundary added with
  concrete `WendelonClientCertVerifier` composition sketch.
  D-set 10; guards 10 + 1; fixtures 6 + 2.
- **Iter 3:** Council convergence with Codex + Gemini. Two
  sharpenings adopted: D1 module structure expanded to seven
  submodules (`asn1` + `cbor` added per Codex+Gemini 2-of-3
  convergence on DER/CBOR neutral primitives); shared-crate
  error type named `PkiError` (Gemini's term; Toron's existing
  `TlsError` wrapper remains local). New guard
  11 (`adr047_toron_der_and_cbor_parsing_delegates_to_tls_pki`).
  Gemini's `GenericCertVerifier<F>` closure did not ship; the
  final D8 shape is broader caller-owned trait-object verifier
  composition around `tls-pki` trust-chain helpers. Gemini's Toron
  CLI consolidation was rejected 2-of-3 Codex+Claude as
  out-of-scope refactor and remains a future Toron-local cleanup
  candidate. D-set locked at 10. Guard set locked at 11 + 1
  convention. Fixture set locked at 6
  required + 2 V1-evidence optional. Ready to promote.

## Conclusion

ADR 047 ships:

- one shared `crates/tls-pki/` workspace crate with
  seven-submodule scope (`material`, `asn1`, `cbor`, `san`,
  `config_builder`, `verifier`, `error`);
- `TlsMaterial` PEM loader (cert chain + private key + CA
  roots) with env-interpolation rejection from ADR 044 D6;
- `DerReader` + `extract_dns_sans` + `extract_subject` +
  `extract_extension` neutral ASN.1/DER primitives;
- `CborReader` + primitives for actor-claim decoding;
- `admit_san` exact-shape verifier (ADR 044 D5 semantics)
  parameterized by allowed prefix set; `canonical_dns_name`
  parameterized by ring prefix; `PeerIdentity` open struct;
- `build_rustls_server_config` + `build_rustls_client_config`
  parameterized by ALPN and caller-owned verifier policy;
- trust-chain verifier helpers for rustls client/server
  verification;
- `PkiError` shared error type;
- Toron-side substantial substrate migration while preserving
  Toron-local ALPN constants, quinn wrapping, UUID post-check,
  `TlsError`, verifier policy types, and canonical
  daemon/client/monitor wrappers;
- Wendelon-side minimal-import-only migration with a
  `canonical_node_dns_name` wrapper and local `PkiError` →
  `std::io::Error` mapping helpers;
- zero functional changes, zero wire changes, zero config
  changes, zero new operator-visible functionality;
- 11 promoted architecture guards + 1 convention guard + 6
  required integration fixtures + 2 V1-evidence optional
  fixtures;
- single-plan substrate posture;
- twelfth consecutive cycle preserving the no-V1-suffix
  convention.

**First refactor cycle in the ADR 040-onward sequence.**
Behavior-preserving extraction that pays down accumulated
architectural debt from four cycles of parallel TLS
construction (ADR 043 + ADR 044 + ADR 045 + ADR 046). Sets up
cleaner composition for future substrate cycles (multi-tenant
routing, token auth, `ActorPermission` parity, live cert
rotation, OTLP, audit, Wendelon-side DER/CBOR migration).

The eleven-phase migration is materialized in
`plans/impl/047_shared_tls_pki_workspace_crate.md`.

## Promotion Notes

ADR 047 establishes the discipline for **refactor cycles** in
the ADR 040-onward sequence:

- pure behavior-preserving (zero functional changes; zero
  operator-visible surface changes);
- meta-guard + prior-cycle guard/fixture pass-count inventories gate
  promotion for refactor cycles;
- single-plan posture (no Plan A/Plan B unless a cross-ring
  audit reveals genuine sub-lift uncertainty);
- cross-ring discipline preserved (shared substrate doesn't
  host ring-internal types);
- council-majority adoption (2-of-3 convergence drives D-set
  scope; minority positions deferred to future ADRs).

Future refactor cycles inherit this discipline.
