//! mTLS config construction for the primary HRFT transport path.
//!
//! On the supported day-one path, inter-node traffic — Data Ring,
//! Control Ring, and Raft internal — terminates on a QUIC endpoint.
//! The same certificate material may also be reused by explicit
//! compatibility listeners, but this module exists first to secure
//! the QUIC path. Per `plans/network/security.md`, every connection
//! is mTLS 1.3: both sides present X.509 certificates issued by the
//! cluster PKI, and each node's `NodeId` binds to a Subject
//! Alternative Name in the cert.
//!
//! This module deals with the cryptographic shape plus the first-wave
//! node-identity binding contract: cert/key loading, rustls
//! `ClientConfig` / `ServerConfig` construction, ALPN negotiation,
//! and extraction of a verified peer `NodeId` from the leaf
//! certificate's DNS SAN set.
//!
//! ## ALPN
//!
//! The single ALPN token [`HRFT_ALPN`] is advertised on both sides.
//! Peers that cannot negotiate it are rejected at the TLS layer,
//! which prevents accidental cross-protocol collisions if someone
//! points a generic HTTP/3 client at the port.
//!
//! ## mTLS posture
//!
//! * **Server side:** requires a valid client certificate chaining
//!   to [`TlsMaterial::ca_roots`]. Anonymous clients are rejected at
//!   handshake time.
//! * **Client side:** presents [`TlsMaterial::cert_chain`] +
//!   [`TlsMaterial::key`] and verifies the server cert against the
//!   same CA pool. The SNI supplied by the dialer must match a SAN
//!   on the server cert.
//!
//! ## Scope — what this file deliberately does not do
//!
//! * Hot cert reload. Files are read once at [`TlsMaterial::load`].
//!   Rotations will need an outer supervisor.
//! * Key log / TLS tracing for debugging.

use crate::consensus::raft_storage::RevocationSnapshot;
use crate::schema::{NodeId, TenantId};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime, pem::PemObject};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::{Arc, Once};

/// Single ALPN token advertised on every HRFT QUIC connection.
/// Short, versionless; the framing header already carries a protocol
/// version if we need to bump the wire format without reshuffling
/// ALPN.
pub const HRFT_ALPN: &[u8] = b"wendelon-hrft";
pub const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";
pub const ACTOR_CLAIMS_OID_DOTTED: &str = "1.3.6.1.4.1.55555.1.1";

const SUBJECT_ALT_NAME_OID: &[u8] = &[0x55, 0x1D, 0x11];
// Placeholder PEN-backed OID until the production assignment lands.
// TODO(pilot): replace this placeholder with the assigned production PEN.
const ACTOR_CLAIMS_OID: &[u8] = &[0x2B, 0x06, 0x01, 0x04, 0x01, 0x83, 0xB2, 0x03, 0x01, 0x01];
const ACTOR_CLAIMS_V1_FIELD_COUNT: usize = 4;
const ACTOR_CLAIMS_ROLE_FIELD: &str = "role";
const ACTOR_CLAIMS_SCOPES_FIELD: &str = "scopes";
const ACTOR_CLAIMS_TENANT_ID_FIELD: &str = "tenant_id";
const ACTOR_CLAIMS_KEY_ID_FIELD: &str = "key_id";
const DNS_NAME_TAG: u8 = 0x82;
static INSTALL_RUSTLS_PROVIDER: Once = Once::new();

// ============================================================
//  ACTOR IDENTITY
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ActorTrustDomain {
	Data,
	Ops,
}

impl std::fmt::Display for ActorTrustDomain {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Data => f.write_str("data"),
			Self::Ops => f.write_str("ops"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorRole {
	DataNode,
	Operator,
	TenantAdmin,
	ClusterAdmin,
	BackupAdmin,
	ComplianceAuditor,
	ReadOnlyObserver,
	BreakGlassAdmin,
	QuerySession,
}

impl ActorRole {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::DataNode => "data_node",
			Self::Operator => "operator",
			Self::TenantAdmin => "tenant_admin",
			Self::ClusterAdmin => "cluster_admin",
			Self::BackupAdmin => "backup_admin",
			Self::ComplianceAuditor => "compliance_auditor",
			Self::ReadOnlyObserver => "read_only_observer",
			Self::BreakGlassAdmin => "break_glass_admin",
			Self::QuerySession => "query_session",
		}
	}

	pub fn trust_domain(self) -> ActorTrustDomain {
		match self {
			Self::DataNode | Self::ReadOnlyObserver | Self::QuerySession => ActorTrustDomain::Data,
			Self::Operator
			| Self::TenantAdmin
			| Self::ClusterAdmin
			| Self::BackupAdmin
			| Self::ComplianceAuditor
			| Self::BreakGlassAdmin => ActorTrustDomain::Ops,
		}
	}

	pub fn break_glass(self) -> bool {
		matches!(self, Self::BreakGlassAdmin)
	}

	fn parse(raw: &str) -> std::io::Result<Self> {
		match raw {
			"data_node" => Ok(Self::DataNode),
			"operator" => Ok(Self::Operator),
			"tenant_admin" => Ok(Self::TenantAdmin),
			"cluster_admin" => Ok(Self::ClusterAdmin),
			"backup_admin" => Ok(Self::BackupAdmin),
			"compliance_auditor" => Ok(Self::ComplianceAuditor),
			"read_only_observer" => Ok(Self::ReadOnlyObserver),
			"break_glass_admin" => Ok(Self::BreakGlassAdmin),
			"query_session" => Ok(Self::QuerySession),
			other => Err(Error::new(
				ErrorKind::PermissionDenied,
				format!("unrecognized actor role {other:?}"),
			)),
		}
	}
}

impl std::fmt::Display for ActorRole {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(self.as_str())
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorIdentity {
	pub role: ActorRole,
	pub trust_domain: ActorTrustDomain,
	pub cert_subject: Option<String>,
	pub node_id: Option<NodeId>,
	pub tenant_id: Option<String>,
	pub scopes: Vec<String>,
	pub key_id: Vec<u8>,
	pub break_glass: bool,
}

#[derive(Clone)]
pub struct ActorTrustRoots {
	pub data_roots: Arc<RootCertStore>,
	pub ops_roots: Arc<RootCertStore>,
}

impl ActorTrustRoots {
	pub fn load(
		data_ca_path: impl AsRef<Path>,
		ops_ca_path: impl AsRef<Path>,
	) -> std::io::Result<Self> {
		let data_roots = Arc::new(load_ca_roots(data_ca_path.as_ref())?);
		let ops_roots = Arc::new(load_ca_roots(ops_ca_path.as_ref())?);
		Ok(Self {
			data_roots,
			ops_roots,
		})
	}

	pub fn combined_roots(&self) -> Arc<RootCertStore> {
		let mut roots = (*self.data_roots).clone();
		roots.extend(self.ops_roots.roots.iter().cloned());
		Arc::new(roots)
	}
}

impl ActorIdentity {
	pub fn new(
		role: ActorRole,
		node_id: Option<NodeId>,
		tenant_id: Option<String>,
		scopes: Vec<String>,
		key_id: Vec<u8>,
	) -> std::io::Result<Self> {
		if key_id.is_empty() {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"actor certificate key_id must not be empty",
			));
		}
		if matches!(role, ActorRole::DataNode) && node_id.is_none() {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"data_node certificate must carry a canonical node SAN",
			));
		}
		Ok(Self {
			role,
			trust_domain: role.trust_domain(),
			cert_subject: None,
			node_id,
			tenant_id,
			scopes,
			key_id,
			break_glass: role.break_glass(),
		})
	}

	pub fn data_node(node_id: NodeId) -> Self {
		Self {
			role: ActorRole::DataNode,
			trust_domain: ActorTrustDomain::Data,
			cert_subject: None,
			node_id: Some(node_id),
			tenant_id: None,
			scopes: vec!["cluster".to_string()],
			key_id: node_id.id.to_le_bytes().to_vec(),
			break_glass: false,
		}
	}

	pub fn with_cert_subject(mut self, cert_subject: Option<String>) -> Self {
		self.cert_subject = cert_subject;
		self
	}

	pub fn claimed_tenant_id(&self) -> std::io::Result<Option<TenantId>> {
		match self.tenant_id.as_deref() {
			None => Ok(None),
			Some(raw) => raw
				.parse::<u64>()
				.map(TenantId::new)
				.map(Some)
				.map_err(|_| {
					Error::new(
						ErrorKind::PermissionDenied,
						format!("actor tenant_id claim {raw:?} is not a valid u64"),
					)
				}),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorPermission {
	InterNodeTransport,
	OpsControl,
	ControlStatus,
	ClusterControl,
	TenantAdminRead,
	TenantAdminMutation,
	TenantKekAdmin,
	AuditRead,
	ObservabilityReady,
	ObservabilityMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BreakGlassScope {
	Diagnostic,
	TenantOps,
	KeyOps,
	ClusterOps,
	Full,
}

impl BreakGlassScope {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Diagnostic => "diagnostic",
			Self::TenantOps => "tenant_ops",
			Self::KeyOps => "key_ops",
			Self::ClusterOps => "cluster_ops",
			Self::Full => "full",
		}
	}

	pub fn parse(raw: &str) -> Option<Self> {
		match raw {
			"diagnostic" => Some(Self::Diagnostic),
			"tenant_ops" => Some(Self::TenantOps),
			"key_ops" => Some(Self::KeyOps),
			"cluster_ops" => Some(Self::ClusterOps),
			"full" => Some(Self::Full),
			_ => None,
		}
	}
}

impl std::fmt::Display for BreakGlassScope {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(self.as_str())
	}
}

pub fn authorize_break_glass_scope(
	actor: &ActorIdentity,
	allowed: &[BreakGlassScope],
	context: &str,
) -> std::io::Result<()> {
	if !actor.break_glass {
		return Ok(());
	}
	let has_allowed = actor
		.scopes
		.iter()
		.filter_map(|scope| BreakGlassScope::parse(scope))
		.any(|scope| scope == BreakGlassScope::Full || allowed.contains(&scope));
	if has_allowed {
		return Ok(());
	}
	let allowed_list = allowed
		.iter()
		.map(|scope| scope.as_str())
		.collect::<Vec<_>>()
		.join(", ");
	Err(Error::new(
		ErrorKind::PermissionDenied,
		format!(
			"break-glass actor scopes {:?} do not authorize {context}; allowed scopes: {allowed_list}",
			actor.scopes
		),
	))
}

pub fn authorize_actor(actor: &ActorIdentity, permission: ActorPermission) -> std::io::Result<()> {
	let allowed = match permission {
		ActorPermission::InterNodeTransport => matches!(actor.role, ActorRole::DataNode),
		ActorPermission::OpsControl => matches!(
			actor.role,
			ActorRole::Operator
				| ActorRole::TenantAdmin
				| ActorRole::ClusterAdmin
				| ActorRole::BackupAdmin
				| ActorRole::BreakGlassAdmin
		),
		ActorPermission::ControlStatus => matches!(
			actor.role,
			ActorRole::DataNode
				| ActorRole::Operator
				| ActorRole::ClusterAdmin
				| ActorRole::BackupAdmin
				| ActorRole::ComplianceAuditor
				| ActorRole::ReadOnlyObserver
				| ActorRole::BreakGlassAdmin
		),
		ActorPermission::ClusterControl => matches!(
			actor.role,
			ActorRole::DataNode
				| ActorRole::Operator
				| ActorRole::ClusterAdmin
				| ActorRole::BreakGlassAdmin
		),
		ActorPermission::TenantAdminRead => matches!(
			actor.role,
			ActorRole::Operator
				| ActorRole::TenantAdmin
				| ActorRole::BackupAdmin
				| ActorRole::ReadOnlyObserver
				| ActorRole::BreakGlassAdmin
		),
		ActorPermission::TenantAdminMutation => matches!(
			actor.role,
			ActorRole::Operator | ActorRole::TenantAdmin | ActorRole::BreakGlassAdmin
		),
		ActorPermission::TenantKekAdmin => matches!(
			actor.role,
			ActorRole::Operator | ActorRole::BackupAdmin | ActorRole::BreakGlassAdmin
		),
		ActorPermission::AuditRead => matches!(
			actor.role,
			ActorRole::Operator
				| ActorRole::ComplianceAuditor
				| ActorRole::ReadOnlyObserver
				| ActorRole::BreakGlassAdmin
		),
		ActorPermission::ObservabilityReady => matches!(
			actor.role,
			ActorRole::DataNode
				| ActorRole::Operator
				| ActorRole::TenantAdmin
				| ActorRole::ClusterAdmin
				| ActorRole::BackupAdmin
				| ActorRole::ComplianceAuditor
				| ActorRole::ReadOnlyObserver
				| ActorRole::BreakGlassAdmin
		),
		ActorPermission::ObservabilityMetrics => matches!(
			actor.role,
			ActorRole::Operator
				| ActorRole::TenantAdmin
				| ActorRole::ClusterAdmin
				| ActorRole::BackupAdmin
				| ActorRole::ComplianceAuditor
				| ActorRole::ReadOnlyObserver
				| ActorRole::BreakGlassAdmin
		),
	};
	if allowed {
		match permission {
			ActorPermission::ControlStatus
			| ActorPermission::AuditRead
			| ActorPermission::ObservabilityReady
			| ActorPermission::ObservabilityMetrics => authorize_break_glass_scope(
				actor,
				&[BreakGlassScope::Diagnostic],
				&format!("{permission:?}"),
			)?,
			ActorPermission::TenantAdminRead | ActorPermission::TenantAdminMutation => {
				authorize_break_glass_scope(
					actor,
					&[BreakGlassScope::TenantOps],
					&format!("{permission:?}"),
				)?
			}
			ActorPermission::TenantKekAdmin => authorize_break_glass_scope(
				actor,
				&[BreakGlassScope::KeyOps],
				&format!("{permission:?}"),
			)?,
			ActorPermission::ClusterControl | ActorPermission::OpsControl => {
				authorize_break_glass_scope(
					actor,
					&[BreakGlassScope::ClusterOps],
					&format!("{permission:?}"),
				)?
			}
			ActorPermission::InterNodeTransport => {}
		}
		return Ok(());
	}
	Err(Error::new(
		ErrorKind::PermissionDenied,
		format!(
			"actor role {} is not allowed for {permission:?}",
			actor.role
		),
	))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedTransportPeer {
	pub node_id: NodeId,
	pub actor_identity: ActorIdentity,
}

// ============================================================
//  MATERIAL
// ============================================================

/// Owned in-memory copy of the material needed to build mTLS
/// configs for this node: its own leaf + chain, its private key,
/// and the trusted root pool used to verify the peer.
///
/// Callers typically load this once at boot and pass references
/// into both [`server_config`] and [`client_config`]; those
/// builders copy the chain and key out via `PrivateKeyDer::clone_key`.
/// `TlsMaterial` itself is not `Clone` because `PrivateKeyDer` is
/// intentionally move-only — wrap it in an `Arc` if you need shared
/// ownership.
pub struct TlsMaterial {
	/// Leaf-first chain up to (but not including) the root.
	pub cert_chain: Vec<CertificateDer<'static>>,
	/// PKCS#8 / RSA / SEC1 private key matching the leaf cert.
	pub key: PrivateKeyDer<'static>,
	/// Trust anchors used to verify the remote peer (both when we
	/// are the client and when we are the server).
	pub ca_roots: Arc<RootCertStore>,
}

impl TlsMaterial {
	/// Load cert chain, private key, and CA bundle from PEM files
	/// on disk. Convenience for the happy path where an operator
	/// drops three PEMs into `/etc/wendelon/certs/`; programmatic
	/// setups (tests, KMS-fetched keys) should construct
	/// `TlsMaterial` by hand.
	pub fn load(
		cert_path: impl AsRef<Path>,
		key_path: impl AsRef<Path>,
		ca_path: impl AsRef<Path>,
	) -> std::io::Result<Self> {
		let cert_chain = load_cert_chain(cert_path.as_ref())?;
		if cert_chain.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("no certificates found in {}", cert_path.as_ref().display()),
			));
		}
		let key = load_private_key(key_path.as_ref())?;
		let ca_roots = load_ca_roots(ca_path.as_ref())?;
		Ok(Self {
			cert_chain,
			key,
			ca_roots: Arc::new(ca_roots),
		})
	}
}

// ============================================================
//  QUIC / RUSTLS CONFIG BUILDERS
// ============================================================

pub fn rustls_server_config(material: &TlsMaterial) -> std::io::Result<ServerConfig> {
	rustls_server_config_with_alpn(material, vec![HRFT_ALPN.to_vec()])
}

pub fn rustls_server_config_with_alpn(
	material: &TlsMaterial,
	alpn_protocols: Vec<Vec<u8>>,
) -> std::io::Result<ServerConfig> {
	rustls_server_config_with_peer_roots(material, material.ca_roots.clone(), alpn_protocols)
}

pub fn rustls_server_config_with_peer_roots(
	material: &TlsMaterial,
	peer_roots: Arc<RootCertStore>,
	alpn_protocols: Vec<Vec<u8>>,
) -> std::io::Result<ServerConfig> {
	install_rustls_provider();
	let verifier = WebPkiClientVerifier::builder(peer_roots)
		.build()
		.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("client verifier: {e}")))?;

	let mut rustls_cfg = ServerConfig::builder()
		.with_client_cert_verifier(verifier)
		.with_single_cert(material.cert_chain.clone(), material.key.clone_key())
		.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("server cert/key: {e}")))?;
	rustls_cfg.alpn_protocols = alpn_protocols;
	Ok(rustls_cfg)
}

pub fn rustls_client_config(material: &TlsMaterial) -> std::io::Result<ClientConfig> {
	rustls_client_config_with_alpn(material, vec![HRFT_ALPN.to_vec()])
}

pub fn rustls_client_config_with_alpn(
	material: &TlsMaterial,
	alpn_protocols: Vec<Vec<u8>>,
) -> std::io::Result<ClientConfig> {
	install_rustls_provider();
	let mut rustls_cfg = ClientConfig::builder()
		.with_root_certificates((*material.ca_roots).clone())
		.with_client_auth_cert(material.cert_chain.clone(), material.key.clone_key())
		.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("client cert/key: {e}")))?;
	rustls_cfg.alpn_protocols = alpn_protocols;
	Ok(rustls_cfg)
}

fn install_rustls_provider() {
	INSTALL_RUSTLS_PROVIDER.call_once(|| {
		let _ = rustls::crypto::ring::default_provider().install_default();
	});
}

/// Build a `quinn::ServerConfig` that:
///
/// * presents this node's cert+key to incoming peers,
/// * requires every client to present a cert chaining to
///   `material.ca_roots`,
/// * advertises only [`HRFT_ALPN`].
pub fn server_config(material: &TlsMaterial) -> std::io::Result<quinn::ServerConfig> {
	let rustls_cfg = rustls_server_config(material)?;
	let quic_cfg = QuicServerConfig::try_from(rustls_cfg)
		.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("QuicServerConfig: {e}")))?;
	Ok(quinn::ServerConfig::with_crypto(Arc::new(quic_cfg)))
}

/// Build a `quinn::ClientConfig` that:
///
/// * verifies the server cert against `material.ca_roots`,
/// * presents this node's cert+key as the client identity,
/// * advertises only [`HRFT_ALPN`].
pub fn client_config(material: &TlsMaterial) -> std::io::Result<quinn::ClientConfig> {
	let rustls_cfg = rustls_client_config(material)?;
	let quic_cfg = QuicClientConfig::try_from(rustls_cfg)
		.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("QuicClientConfig: {e}")))?;
	Ok(quinn::ClientConfig::new(Arc::new(quic_cfg)))
}

// ============================================================
//  NODE-ID BINDING
// ============================================================

/// Canonical DNS SAN for a Wendelon node certificate.
///
/// The first-wave identity contract is rigid:
/// `node-<id>.<cluster_domain>`.
pub fn canonical_node_dns_name(node_id: u32, cluster_domain: &str) -> String {
	format!("node-{node_id}.{cluster_domain}")
}

/// Convenience for the default first-wave cluster domain.
pub fn default_node_dns_name(node_id: u32) -> String {
	canonical_node_dns_name(node_id, DEFAULT_CLUSTER_DOMAIN)
}

/// Extract the verified peer [`NodeId`] from a `quinn` connection's
/// rustls peer identity.
pub fn verified_peer_node_id(
	connection: &quinn::Connection,
	cluster_domain: &str,
) -> std::io::Result<NodeId> {
	let identity = connection.peer_identity().ok_or_else(|| {
		Error::new(
			ErrorKind::PermissionDenied,
			"peer presented no TLS identity",
		)
	})?;
	verified_peer_node_id_from_identity(identity, cluster_domain)
}

/// Extract the verified peer [`NodeId`] from a rustls peer-identity
/// box returned by `quinn`.
pub fn verified_peer_node_id_from_identity(
	identity: Box<dyn Any>,
	cluster_domain: &str,
) -> std::io::Result<NodeId> {
	let certs = identity
		.downcast::<Vec<CertificateDer<'static>>>()
		.map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"peer identity is not a rustls certificate chain",
			)
		})?;
	verified_peer_node_id_from_chain(certs.as_slice(), cluster_domain)
}

/// Extract and validate the peer's actor identity from the leaf
/// certificate. Unlike [`verified_peer_node_id_from_chain`], this
/// accepts certs that do not carry a canonical node SAN (operators,
/// observers) and instead requires the W2 actor-claims extension.
pub fn verified_peer_actor_identity(
	connection: &quinn::Connection,
	cluster_domain: &str,
) -> std::io::Result<ActorIdentity> {
	verified_peer_actor_identity_with_revocations(connection, cluster_domain, None)
}

pub fn verified_peer_actor_identity_with_revocations(
	connection: &quinn::Connection,
	cluster_domain: &str,
	revocations: Option<&RevocationSnapshot>,
) -> std::io::Result<ActorIdentity> {
	let identity = connection.peer_identity().ok_or_else(|| {
		Error::new(
			ErrorKind::PermissionDenied,
			"peer presented no TLS identity",
		)
	})?;
	verified_peer_actor_identity_from_identity_with_revocations(
		identity,
		cluster_domain,
		revocations,
	)
}

pub fn verified_peer_actor_identity_from_identity(
	identity: Box<dyn Any>,
	cluster_domain: &str,
) -> std::io::Result<ActorIdentity> {
	verified_peer_actor_identity_from_identity_with_revocations(identity, cluster_domain, None)
}

pub fn verified_peer_actor_identity_from_identity_with_revocations(
	identity: Box<dyn Any>,
	cluster_domain: &str,
	revocations: Option<&RevocationSnapshot>,
) -> std::io::Result<ActorIdentity> {
	let certs = identity
		.downcast::<Vec<CertificateDer<'static>>>()
		.map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"peer identity is not a rustls certificate chain",
			)
		})?;
	verified_peer_actor_identity_from_chain_with_revocations(
		certs.as_slice(),
		cluster_domain,
		revocations,
	)
}

pub fn verified_peer_actor_identity_from_chain(
	cert_chain: &[CertificateDer<'_>],
	cluster_domain: &str,
) -> std::io::Result<ActorIdentity> {
	verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations(
		cert_chain,
		cluster_domain,
		None,
		None,
	)
}

pub fn verified_peer_actor_identity_from_chain_with_revocations(
	cert_chain: &[CertificateDer<'_>],
	cluster_domain: &str,
	revocations: Option<&RevocationSnapshot>,
) -> std::io::Result<ActorIdentity> {
	verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations(
		cert_chain,
		cluster_domain,
		None,
		revocations,
	)
}

pub fn verified_peer_actor_identity_from_chain_with_trust_roots(
	cert_chain: &[CertificateDer<'_>],
	cluster_domain: &str,
	trust_roots: Option<&ActorTrustRoots>,
) -> std::io::Result<ActorIdentity> {
	verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations(
		cert_chain,
		cluster_domain,
		trust_roots,
		None,
	)
}

pub fn verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations(
	cert_chain: &[CertificateDer<'_>],
	cluster_domain: &str,
	trust_roots: Option<&ActorTrustRoots>,
	revocations: Option<&RevocationSnapshot>,
) -> std::io::Result<ActorIdentity> {
	let leaf = cert_chain
		.first()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "peer certificate chain is empty"))?;
	let node_id = extract_optional_peer_node_id(leaf.as_ref(), cluster_domain)?;
	let claims = extract_actor_claims(leaf.as_ref())?;
	if let Some(trust_roots) = trust_roots {
		let actual_domain = detect_actor_trust_domain(cert_chain, trust_roots)?;
		let expected_domain = claims.role.trust_domain();
		if actual_domain != expected_domain {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"actor role {} must chain to the {expected_domain} CA, but the certificate chained to the {actual_domain} CA",
					claims.role
				),
			));
		}
	}
	let identity = ActorIdentity::new(
		claims.role,
		node_id,
		claims.tenant_id,
		claims.scopes,
		claims.key_id,
	)?
	.with_cert_subject(extract_cert_subject(leaf.as_ref()).ok());
	if let Some(revocations) = revocations {
		if revocations.contains(identity.trust_domain, &identity.key_id) {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"actor certificate key_id is revoked",
			));
		}
	}
	Ok(identity)
}

/// Transport-facing identity verifier. Inter-node QUIC requires a
/// `data_node` actor and a canonical SAN bound to the remote
/// `NodeId`.
pub fn verified_transport_peer(
	connection: &quinn::Connection,
	cluster_domain: &str,
) -> std::io::Result<VerifiedTransportPeer> {
	verified_transport_peer_with_revocations(connection, cluster_domain, None)
}

pub fn verified_transport_peer_with_revocations(
	connection: &quinn::Connection,
	cluster_domain: &str,
	revocations: Option<&RevocationSnapshot>,
) -> std::io::Result<VerifiedTransportPeer> {
	let identity = connection.peer_identity().ok_or_else(|| {
		Error::new(
			ErrorKind::PermissionDenied,
			"peer presented no TLS identity",
		)
	})?;
	verified_transport_peer_from_identity_with_revocations(identity, cluster_domain, revocations)
}

pub fn verified_transport_peer_from_identity(
	identity: Box<dyn Any>,
	cluster_domain: &str,
) -> std::io::Result<VerifiedTransportPeer> {
	verified_transport_peer_from_identity_with_revocations(identity, cluster_domain, None)
}

pub fn verified_transport_peer_from_identity_with_revocations(
	identity: Box<dyn Any>,
	cluster_domain: &str,
	revocations: Option<&RevocationSnapshot>,
) -> std::io::Result<VerifiedTransportPeer> {
	let certs = identity
		.downcast::<Vec<CertificateDer<'static>>>()
		.map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"peer identity is not a rustls certificate chain",
			)
		})?;
	verified_transport_peer_from_chain_with_revocations(
		certs.as_slice(),
		cluster_domain,
		revocations,
	)
}

pub fn verified_transport_peer_from_chain(
	cert_chain: &[CertificateDer<'_>],
	cluster_domain: &str,
) -> std::io::Result<VerifiedTransportPeer> {
	verified_transport_peer_from_chain_with_revocations(cert_chain, cluster_domain, None)
}

pub fn verified_transport_peer_from_chain_with_revocations(
	cert_chain: &[CertificateDer<'_>],
	cluster_domain: &str,
	revocations: Option<&RevocationSnapshot>,
) -> std::io::Result<VerifiedTransportPeer> {
	let actor_identity = verified_peer_actor_identity_from_chain_with_revocations(
		cert_chain,
		cluster_domain,
		revocations,
	)?;
	authorize_actor(&actor_identity, ActorPermission::InterNodeTransport)?;
	let node_id = actor_identity.node_id.ok_or_else(|| {
		Error::new(
			ErrorKind::PermissionDenied,
			"inter-node transport certificate is missing a canonical node SAN",
		)
	})?;
	Ok(VerifiedTransportPeer {
		node_id,
		actor_identity,
	})
}

/// Parse the leaf certificate in `cert_chain` and recover the peer's
/// canonical node identity from its DNS SAN set.
pub fn verified_peer_node_id_from_chain(
	cert_chain: &[CertificateDer<'_>],
	cluster_domain: &str,
) -> std::io::Result<NodeId> {
	let leaf = cert_chain
		.first()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "peer certificate chain is empty"))?;
	let dns_sans = extract_dns_sans(leaf.as_ref())?;
	if dns_sans.is_empty() {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			"peer certificate contains no DNS SANs",
		));
	}
	extract_optional_peer_node_id(leaf.as_ref(), cluster_domain)?.ok_or_else(|| {
		Error::new(
			ErrorKind::PermissionDenied,
			format!("peer certificate contains no canonical node SAN for domain {cluster_domain}"),
		)
	})
}

fn load_cert_chain(path: &Path) -> std::io::Result<Vec<CertificateDer<'static>>> {
	let certs = CertificateDer::pem_file_iter(path)
		.map_err(|e| {
			Error::new(
				ErrorKind::InvalidData,
				format!("reading {}: {e}", path.display()),
			)
		})?
		.collect::<Result<Vec<_>, _>>()
		.map_err(|e| {
			Error::new(
				ErrorKind::InvalidData,
				format!("reading {}: {e}", path.display()),
			)
		})?;
	Ok(certs)
}

fn load_private_key(path: &Path) -> std::io::Result<PrivateKeyDer<'static>> {
	PrivateKeyDer::from_pem_file(path).map_err(|e| {
		Error::new(
			ErrorKind::InvalidData,
			format!("reading {}: {e}", path.display()),
		)
	})
}

fn load_ca_roots(path: &Path) -> std::io::Result<RootCertStore> {
	let certs = CertificateDer::pem_file_iter(path)
		.map_err(|e| {
			Error::new(
				ErrorKind::InvalidData,
				format!("reading {}: {e}", path.display()),
			)
		})?
		.collect::<Result<Vec<_>, _>>()
		.map_err(|e| {
			Error::new(
				ErrorKind::InvalidData,
				format!("reading {}: {e}", path.display()),
			)
		})?;
	let mut roots = RootCertStore::empty();
	for cert in certs {
		roots.add(cert).map_err(|e| {
			Error::new(
				ErrorKind::InvalidData,
				format!("adding CA cert from {}: {e}", path.display()),
			)
		})?;
	}
	if roots.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("no CA certificates found in {}", path.display()),
		));
	}
	Ok(roots)
}

fn detect_actor_trust_domain(
	cert_chain: &[CertificateDer<'_>],
	trust_roots: &ActorTrustRoots,
) -> std::io::Result<ActorTrustDomain> {
	let data_ok = verify_chain_against_roots(cert_chain, trust_roots.data_roots.clone())?;
	let ops_ok = verify_chain_against_roots(cert_chain, trust_roots.ops_roots.clone())?;
	match (data_ok, ops_ok) {
		(true, false) => Ok(ActorTrustDomain::Data),
		(false, true) => Ok(ActorTrustDomain::Ops),
		(true, true) => Err(Error::new(
			ErrorKind::PermissionDenied,
			"peer certificate chains to both the data and ops trust roots",
		)),
		(false, false) => Err(Error::new(
			ErrorKind::PermissionDenied,
			"peer certificate does not chain to a configured trust root",
		)),
	}
}

fn verify_chain_against_roots(
	cert_chain: &[CertificateDer<'_>],
	roots: Arc<RootCertStore>,
) -> std::io::Result<bool> {
	install_rustls_provider();
	let verifier = WebPkiClientVerifier::builder(roots)
		.build()
		.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("client verifier: {e}")))?;
	let Some((end_entity, intermediates)) = cert_chain.split_first() else {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"peer certificate chain is empty",
		));
	};
	Ok(verifier
		.verify_client_cert(end_entity, intermediates, UnixTime::now())
		.is_ok())
}

/// Frozen pilot-era actor-claims payload.
///
/// The extension is a versionless CBOR map with exactly four keys:
/// `role`, `scopes`, `tenant_id`, and `key_id`. Cluster-wide actors
/// still carry `tenant_id`, but encode it as `null` rather than omitting
/// the field. This keeps the external claim surface stable without
/// forcing cert-fixture churn for a new version field.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ActorClaimsV1 {
	pub(crate) role: ActorRole,
	pub(crate) scopes: Vec<String>,
	pub(crate) tenant_id: Option<String>,
	pub(crate) key_id: Vec<u8>,
}

pub(crate) fn encode_actor_claims_cbor_v1(
	role: ActorRole,
	scopes: &[String],
	tenant_id: Option<&str>,
	key_id: &[u8],
) -> Vec<u8> {
	let mut out = Vec::new();
	cbor_write_len(&mut out, 5, ACTOR_CLAIMS_V1_FIELD_COUNT);

	cbor_write_text(&mut out, ACTOR_CLAIMS_ROLE_FIELD);
	cbor_write_text(&mut out, role.as_str());

	cbor_write_text(&mut out, ACTOR_CLAIMS_SCOPES_FIELD);
	cbor_write_len(&mut out, 4, scopes.len());
	for scope in scopes {
		cbor_write_text(&mut out, scope);
	}

	cbor_write_text(&mut out, ACTOR_CLAIMS_TENANT_ID_FIELD);
	match tenant_id {
		Some(tenant_id) => cbor_write_text(&mut out, tenant_id),
		None => out.push(0xF6),
	}

	cbor_write_text(&mut out, ACTOR_CLAIMS_KEY_ID_FIELD);
	cbor_write_bytes(&mut out, key_id);

	out
}

fn cbor_write_text(out: &mut Vec<u8>, text: &str) {
	cbor_write_len(out, 3, text.len());
	out.extend_from_slice(text.as_bytes());
}

fn cbor_write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
	cbor_write_len(out, 2, bytes.len());
	out.extend_from_slice(bytes);
}

fn cbor_write_len(out: &mut Vec<u8>, major: u8, len: usize) {
	match len {
		0..=23 => out.push((major << 5) | (len as u8)),
		24..=0xFF => {
			out.push((major << 5) | 24);
			out.push(len as u8);
		}
		0x100..=0xFFFF => {
			out.push((major << 5) | 25);
			out.extend_from_slice(&(len as u16).to_be_bytes());
		}
		_ => panic!("actor claims encoder does not support length {len}"),
	}
}

// ============================================================
//  ASN.1 HELPERS
// ============================================================

fn extract_optional_peer_node_id(
	cert_der: &[u8],
	cluster_domain: &str,
) -> std::io::Result<Option<NodeId>> {
	let dns_sans = extract_dns_sans(cert_der)?;
	if dns_sans.is_empty() {
		return Ok(None);
	}

	let mut parsed: Option<NodeId> = None;
	for san in dns_sans {
		let Some(id_text) = canonical_node_id_text(&san, cluster_domain) else {
			continue;
		};
		let id = id_text.parse::<u32>().map_err(|_| {
			Error::new(
				ErrorKind::PermissionDenied,
				format!("canonical node SAN {san:?} is not numeric"),
			)
		})?;
		let node_id = NodeId { id, _reserved: 0 };
		if parsed.replace(node_id).is_some() {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"peer certificate contains more than one canonical node SAN",
			));
		}
	}
	Ok(parsed)
}

fn canonical_node_id_text<'a>(dns_name: &'a str, cluster_domain: &str) -> Option<&'a str> {
	let prefix = "node-";
	let suffix = format!(".{cluster_domain}");
	if !dns_name.starts_with(prefix) || !dns_name.ends_with(&suffix) {
		return None;
	}
	let end = dns_name.len().checked_sub(suffix.len())?;
	if end <= prefix.len() {
		return Some("");
	}
	Some(&dns_name[prefix.len()..end])
}

fn extract_dns_sans(cert_der: &[u8]) -> std::io::Result<Vec<String>> {
	let mut cert = DerReader::new(cert_der);
	let certificate = cert.read_tlv(0x30, "certificate")?;
	let mut certificate = DerReader::new(certificate);
	let tbs = certificate.read_tlv(0x30, "tbsCertificate")?;
	let mut tbs = DerReader::new(tbs);

	if tbs.peek_tag() == Some(0xA0) {
		tbs.read_any_tlv("version")?;
	}
	for field in [
		"serialNumber",
		"signature",
		"issuer",
		"validity",
		"subject",
		"subjectPublicKeyInfo",
	] {
		tbs.read_any_tlv(field)?;
	}
	while matches!(tbs.peek_tag(), Some(0x81 | 0x82 | 0xA1 | 0xA2)) {
		tbs.read_any_tlv("uniqueId")?;
	}
	if tbs.peek_tag() != Some(0xA3) {
		return Ok(Vec::new());
	}

	let extensions = tbs.read_tlv(0xA3, "extensions")?;
	let mut extensions = DerReader::new(extensions);
	let extensions = extensions.read_tlv(0x30, "extensions sequence")?;
	let mut extensions = DerReader::new(extensions);
	let mut dns_sans = Vec::new();

	while !extensions.is_empty() {
		let extension = extensions.read_tlv(0x30, "extension")?;
		let mut extension = DerReader::new(extension);
		let oid = extension.read_tlv(0x06, "extension oid")?;
		if extension.peek_tag() == Some(0x01) {
			extension.read_any_tlv("extension critical")?;
		}
		let value = extension.read_tlv(0x04, "extension value")?;
		if oid == SUBJECT_ALT_NAME_OID {
			let mut san_value = DerReader::new(value);
			let general_names = san_value.read_tlv(0x30, "subjectAltName")?;
			let mut general_names = DerReader::new(general_names);
			while !general_names.is_empty() {
				let (tag, value) = general_names.read_any_tlv("generalName")?;
				if tag == DNS_NAME_TAG {
					let dns_name = std::str::from_utf8(value).map_err(|e| {
						Error::new(
							ErrorKind::InvalidData,
							format!("DNS SAN is not valid UTF-8/IA5: {e}"),
						)
					})?;
					dns_sans.push(dns_name.to_string());
				}
			}
		}
	}

	Ok(dns_sans)
}

fn extract_cert_subject(cert_der: &[u8]) -> std::io::Result<String> {
	let mut cert = DerReader::new(cert_der);
	let certificate = cert.read_tlv(0x30, "certificate")?;
	let mut certificate = DerReader::new(certificate);
	let tbs = certificate.read_tlv(0x30, "tbsCertificate")?;
	let mut tbs = DerReader::new(tbs);

	if tbs.peek_tag() == Some(0xA0) {
		tbs.read_any_tlv("version")?;
	}
	for field in ["serialNumber", "signature", "issuer", "validity"] {
		tbs.read_any_tlv(field)?;
	}
	let subject = tbs.read_tlv(0x30, "subject")?;
	format_subject(subject)
}

fn format_subject(subject_der: &[u8]) -> std::io::Result<String> {
	let mut subject = DerReader::new(subject_der);
	let mut rdns = Vec::new();
	while !subject.is_empty() {
		let rdn = subject.read_tlv(0x31, "relative distinguished name")?;
		let mut rdn = DerReader::new(rdn);
		let mut attributes = Vec::new();
		while !rdn.is_empty() {
			let attribute = rdn.read_tlv(0x30, "attribute type and value")?;
			let mut attribute = DerReader::new(attribute);
			let oid = attribute.read_tlv(0x06, "subject attribute oid")?;
			let (tag, value) = attribute.read_any_tlv("subject attribute value")?;
			let key = subject_attribute_name(oid);
			let value = decode_subject_attribute_value(tag, value)?;
			attributes.push(format!("{key}={value}"));
		}
		if !attributes.is_empty() {
			rdns.push(attributes.join("+"));
		}
	}
	if rdns.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"peer certificate subject is empty",
		));
	}
	Ok(rdns.join(","))
}

fn subject_attribute_name(oid: &[u8]) -> &'static str {
	match oid {
		[0x55, 0x04, 0x03] => "CN",
		[0x55, 0x04, 0x06] => "C",
		[0x55, 0x04, 0x07] => "L",
		[0x55, 0x04, 0x08] => "ST",
		[0x55, 0x04, 0x0A] => "O",
		[0x55, 0x04, 0x0B] => "OU",
		[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x09, 0x01] => "emailAddress",
		_ => "OID",
	}
}

fn decode_subject_attribute_value(tag: u8, value: &[u8]) -> std::io::Result<String> {
	match tag {
		0x0C | 0x13 | 0x14 | 0x16 => {
			std::str::from_utf8(value)
				.map(str::to_string)
				.map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("certificate subject is not valid UTF-8: {err}"),
					)
				})
		}
		0x1E => {
			if value.len() % 2 != 0 {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"certificate BMPString subject has odd byte length",
				));
			}
			let code_units = value
				.chunks_exact(2)
				.map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
				.collect::<Vec<_>>();
			String::from_utf16(&code_units).map_err(|err| {
				Error::new(
					ErrorKind::InvalidData,
					format!("certificate subject BMPString is invalid UTF-16: {err}"),
				)
			})
		}
		_ => Ok(format!("#{}", crate::audit::hex_string(value))),
	}
}

fn extract_actor_claims(cert_der: &[u8]) -> std::io::Result<ActorClaimsV1> {
	let mut cert = DerReader::new(cert_der);
	let certificate = cert.read_tlv(0x30, "certificate")?;
	let mut certificate = DerReader::new(certificate);
	let tbs = certificate.read_tlv(0x30, "tbsCertificate")?;
	let mut tbs = DerReader::new(tbs);

	if tbs.peek_tag() == Some(0xA0) {
		tbs.read_any_tlv("version")?;
	}
	for field in [
		"serialNumber",
		"signature",
		"issuer",
		"validity",
		"subject",
		"subjectPublicKeyInfo",
	] {
		tbs.read_any_tlv(field)?;
	}
	while matches!(tbs.peek_tag(), Some(0x81 | 0x82 | 0xA1 | 0xA2)) {
		tbs.read_any_tlv("uniqueId")?;
	}
	if tbs.peek_tag() != Some(0xA3) {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			"peer certificate is missing the actor-claims extension",
		));
	}

	let extensions = tbs.read_tlv(0xA3, "extensions")?;
	let mut extensions = DerReader::new(extensions);
	let extensions = extensions.read_tlv(0x30, "extensions sequence")?;
	let mut extensions = DerReader::new(extensions);
	let mut claims: Option<ActorClaimsV1> = None;

	while !extensions.is_empty() {
		let extension = extensions.read_tlv(0x30, "extension")?;
		let mut extension = DerReader::new(extension);
		let oid = extension.read_tlv(0x06, "extension oid")?;
		let _critical = if extension.peek_tag() == Some(0x01) {
			let critical = extension.read_tlv(0x01, "extension critical")?;
			match critical {
				[0x00] => false,
				[0xFF] => true,
				_ => {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"extension critical flag is not valid DER BOOLEAN",
					));
				}
			}
		} else {
			false
		};
		let value = extension.read_tlv(0x04, "extension value")?;
		if oid != ACTOR_CLAIMS_OID {
			continue;
		}
		if claims.is_some() {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"peer certificate contains more than one actor-claims extension",
			));
		}
		claims = Some(parse_actor_claims_cbor(value)?);
	}

	claims.ok_or_else(|| {
		Error::new(
			ErrorKind::PermissionDenied,
			"peer certificate is missing the actor-claims extension",
		)
	})
}

pub(crate) fn parse_actor_claims_cbor(bytes: &[u8]) -> std::io::Result<ActorClaimsV1> {
	let mut reader = CborReader::new(bytes);
	let field_count = reader.read_len_for_major(5, "actor claims map")?;
	if field_count != ACTOR_CLAIMS_V1_FIELD_COUNT {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			format!(
				"actor-claims extension must contain exactly {ACTOR_CLAIMS_V1_FIELD_COUNT} fields"
			),
		));
	}
	let mut role: Option<ActorRole> = None;
	let mut scopes: Option<Vec<String>> = None;
	let mut tenant_id: Option<Option<String>> = None;
	let mut key_id: Option<Vec<u8>> = None;

	for _ in 0..field_count {
		let key = reader.read_text("actor-claims key")?;
		match key.as_str() {
			ACTOR_CLAIMS_ROLE_FIELD => {
				if role.is_some() {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"actor-claims extension contains duplicate role",
					));
				}
				let role_text = reader.read_text("actor role")?;
				role = Some(ActorRole::parse(&role_text)?);
			}
			ACTOR_CLAIMS_SCOPES_FIELD => {
				if scopes.is_some() {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"actor-claims extension contains duplicate scopes",
					));
				}
				let scope_count = reader.read_len_for_major(4, "actor scopes")?;
				let mut parsed = Vec::with_capacity(scope_count);
				for _ in 0..scope_count {
					parsed.push(reader.read_text("actor scope")?);
				}
				scopes = Some(parsed);
			}
			ACTOR_CLAIMS_TENANT_ID_FIELD => {
				if tenant_id.is_some() {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"actor-claims extension contains duplicate tenant_id",
					));
				}
				tenant_id = Some(if reader.peek_simple_null() {
					reader.read_null("tenant_id")?;
					None
				} else {
					Some(reader.read_text("tenant_id")?)
				});
			}
			ACTOR_CLAIMS_KEY_ID_FIELD => {
				if key_id.is_some() {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"actor-claims extension contains duplicate key_id",
					));
				}
				key_id = Some(reader.read_bytes("key_id")?);
			}
			other => {
				return Err(Error::new(
					ErrorKind::PermissionDenied,
					format!("actor-claims extension contains unknown field {other:?}"),
				));
			}
		}
	}
	if !reader.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"actor-claims extension contains trailing CBOR bytes",
		));
	}
	let claims = ActorClaimsV1 {
		role: role.ok_or_else(|| {
			Error::new(
				ErrorKind::PermissionDenied,
				"actor-claims extension is missing role",
			)
		})?,
		scopes: scopes.ok_or_else(|| {
			Error::new(
				ErrorKind::PermissionDenied,
				"actor-claims extension is missing scopes",
			)
		})?,
		tenant_id: tenant_id.ok_or_else(|| {
			Error::new(
				ErrorKind::PermissionDenied,
				"actor-claims extension is missing tenant_id",
			)
		})?,
		key_id: key_id.ok_or_else(|| {
			Error::new(
				ErrorKind::PermissionDenied,
				"actor-claims extension is missing key_id",
			)
		})?,
	};
	if claims.key_id.is_empty() {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			"actor-claims extension key_id must not be empty",
		));
	}
	Ok(claims)
}

struct DerReader<'a> {
	input: &'a [u8],
	pos: usize,
}

impl<'a> DerReader<'a> {
	fn new(input: &'a [u8]) -> Self {
		Self { input, pos: 0 }
	}

	fn is_empty(&self) -> bool {
		self.pos >= self.input.len()
	}

	fn peek_tag(&self) -> Option<u8> {
		self.input.get(self.pos).copied()
	}

	fn read_tlv(&mut self, expected_tag: u8, what: &str) -> std::io::Result<&'a [u8]> {
		let (tag, value) = self.read_any_tlv(what)?;
		if tag != expected_tag {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"invalid DER for {what}: expected tag 0x{expected_tag:02x}, got 0x{tag:02x}"
				),
			));
		}
		Ok(value)
	}

	fn read_any_tlv(&mut self, what: &str) -> std::io::Result<(u8, &'a [u8])> {
		if self.is_empty() {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				format!("truncated DER while reading {what}"),
			));
		}
		let tag = self.input[self.pos];
		self.pos += 1;
		let len = self.read_len(what)?;
		let end = self.pos.checked_add(len).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!("DER length overflow while reading {what}"),
			)
		})?;
		if end > self.input.len() {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				format!("truncated DER payload while reading {what}"),
			));
		}
		let value = &self.input[self.pos..end];
		self.pos = end;
		Ok((tag, value))
	}

	fn read_len(&mut self, what: &str) -> std::io::Result<usize> {
		if self.is_empty() {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				format!("truncated DER length while reading {what}"),
			));
		}
		let first = self.input[self.pos];
		self.pos += 1;
		if first & 0x80 == 0 {
			return Ok(first as usize);
		}
		let width = (first & 0x7F) as usize;
		if width == 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("indefinite DER length is not allowed for {what}"),
			));
		}
		if width > std::mem::size_of::<usize>() || self.pos + width > self.input.len() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("invalid DER length width for {what}"),
			));
		}
		let mut len = 0usize;
		for _ in 0..width {
			len = (len << 8) | self.input[self.pos] as usize;
			self.pos += 1;
		}
		Ok(len)
	}
}

struct CborReader<'a> {
	input: &'a [u8],
	pos: usize,
}

impl<'a> CborReader<'a> {
	fn new(input: &'a [u8]) -> Self {
		Self { input, pos: 0 }
	}

	fn is_empty(&self) -> bool {
		self.pos >= self.input.len()
	}

	fn peek_simple_null(&self) -> bool {
		matches!(self.input.get(self.pos), Some(0xF6))
	}

	fn read_text(&mut self, what: &str) -> std::io::Result<String> {
		let len = self.read_len_for_major(3, what)?;
		let end = self.checked_end(len, what)?;
		let text = std::str::from_utf8(&self.input[self.pos..end]).map_err(|e| {
			Error::new(
				ErrorKind::InvalidData,
				format!("invalid UTF-8 while reading {what}: {e}"),
			)
		})?;
		self.pos = end;
		Ok(text.to_string())
	}

	fn read_bytes(&mut self, what: &str) -> std::io::Result<Vec<u8>> {
		let len = self.read_len_for_major(2, what)?;
		let end = self.checked_end(len, what)?;
		let bytes = self.input[self.pos..end].to_vec();
		self.pos = end;
		Ok(bytes)
	}

	fn read_null(&mut self, what: &str) -> std::io::Result<()> {
		let byte = self.read_u8(what)?;
		if byte != 0xF6 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("expected CBOR null for {what}, got 0x{byte:02x}"),
			));
		}
		Ok(())
	}

	fn read_len_for_major(&mut self, expected_major: u8, what: &str) -> std::io::Result<usize> {
		let byte = self.read_u8(what)?;
		let major = byte >> 5;
		let additional = byte & 0x1F;
		if major != expected_major {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"unexpected CBOR major type for {what}: expected {expected_major}, got {major}"
				),
			));
		}
		self.read_additional(additional, what)
	}

	fn read_additional(&mut self, additional: u8, what: &str) -> std::io::Result<usize> {
		match additional {
			n @ 0..=23 => Ok(n as usize),
			24 => Ok(self.read_u8(what)? as usize),
			25 => {
				let end = self.checked_end(2, what)?;
				let value = u16::from_be_bytes(self.input[self.pos..end].try_into().unwrap());
				self.pos = end;
				Ok(value as usize)
			}
			26 => {
				let end = self.checked_end(4, what)?;
				let value = u32::from_be_bytes(self.input[self.pos..end].try_into().unwrap());
				self.pos = end;
				Ok(value as usize)
			}
			27 => {
				let end = self.checked_end(8, what)?;
				let value = u64::from_be_bytes(self.input[self.pos..end].try_into().unwrap());
				self.pos = end;
				usize::try_from(value).map_err(|_| {
					Error::new(
						ErrorKind::InvalidData,
						format!("CBOR length overflow while reading {what}"),
					)
				})
			}
			31 => Err(Error::new(
				ErrorKind::InvalidData,
				format!("indefinite-length CBOR is not allowed for {what}"),
			)),
			_ => Err(Error::new(
				ErrorKind::InvalidData,
				format!("unsupported CBOR additional info {additional} for {what}"),
			)),
		}
	}

	fn read_u8(&mut self, what: &str) -> std::io::Result<u8> {
		if self.is_empty() {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				format!("truncated CBOR while reading {what}"),
			));
		}
		let byte = self.input[self.pos];
		self.pos += 1;
		Ok(byte)
	}

	fn checked_end(&self, len: usize, what: &str) -> std::io::Result<usize> {
		let end = self.pos.checked_add(len).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!("CBOR length overflow while reading {what}"),
			)
		})?;
		if end > self.input.len() {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				format!("truncated CBOR payload while reading {what}"),
			));
		}
		Ok(end)
	}
}

#[cfg(test)]
mod tests {
	use super::{
		ACTOR_CLAIMS_KEY_ID_FIELD, ACTOR_CLAIMS_ROLE_FIELD, ACTOR_CLAIMS_SCOPES_FIELD,
		ACTOR_CLAIMS_TENANT_ID_FIELD, ActorClaimsV1, ActorRole, parse_actor_claims_cbor,
	};

	#[test]
	fn actor_claims_v1_accepts_cluster_wide_shape() {
		let claims = parse_actor_claims_cbor(
			actor_claims_cbor([
				(ACTOR_CLAIMS_ROLE_FIELD, cbor_text("cluster_admin")),
				(
					ACTOR_CLAIMS_SCOPES_FIELD,
					cbor_array([cbor_text("cluster"), cbor_text("ops")]),
				),
				(ACTOR_CLAIMS_TENANT_ID_FIELD, cbor_null()),
				(ACTOR_CLAIMS_KEY_ID_FIELD, cbor_bytes([0xAA, 0xBB, 0xCC])),
			])
			.as_slice(),
		)
		.unwrap();
		assert_eq!(
			claims,
			ActorClaimsV1 {
				role: ActorRole::ClusterAdmin,
				scopes: vec!["cluster".to_string(), "ops".to_string()],
				tenant_id: None,
				key_id: vec![0xAA, 0xBB, 0xCC],
			}
		);
	}

	#[test]
	fn actor_claims_v1_accepts_tenant_bound_shape() {
		let claims = parse_actor_claims_cbor(
			actor_claims_cbor([
				(ACTOR_CLAIMS_ROLE_FIELD, cbor_text("backup_admin")),
				(ACTOR_CLAIMS_SCOPES_FIELD, cbor_array([cbor_text("tenant")])),
				(ACTOR_CLAIMS_TENANT_ID_FIELD, cbor_text("42")),
				(ACTOR_CLAIMS_KEY_ID_FIELD, cbor_bytes([1, 2, 3, 4])),
			])
			.as_slice(),
		)
		.unwrap();
		assert_eq!(
			claims,
			ActorClaimsV1 {
				role: ActorRole::BackupAdmin,
				scopes: vec!["tenant".to_string()],
				tenant_id: Some("42".to_string()),
				key_id: vec![1, 2, 3, 4],
			}
		);
	}

	#[test]
	fn actor_claims_v1_rejects_missing_scopes() {
		let err = parse_actor_claims_cbor(
			actor_claims_cbor([
				(ACTOR_CLAIMS_ROLE_FIELD, cbor_text("tenant_admin")),
				(ACTOR_CLAIMS_TENANT_ID_FIELD, cbor_text("7")),
				(ACTOR_CLAIMS_KEY_ID_FIELD, cbor_bytes([7, 7, 7])),
			])
			.as_slice(),
		)
		.unwrap_err();
		assert!(err.to_string().contains("exactly 4 fields"));
	}

	#[test]
	fn actor_claims_v1_rejects_missing_tenant_id() {
		let err = parse_actor_claims_cbor(
			actor_claims_cbor([
				(ACTOR_CLAIMS_ROLE_FIELD, cbor_text("tenant_admin")),
				(ACTOR_CLAIMS_SCOPES_FIELD, cbor_array([cbor_text("tenant")])),
				(ACTOR_CLAIMS_KEY_ID_FIELD, cbor_bytes([7, 7, 7])),
			])
			.as_slice(),
		)
		.unwrap_err();
		assert!(err.to_string().contains("exactly 4 fields"));
	}

	#[test]
	fn actor_claims_v1_rejects_unknown_field() {
		let err = parse_actor_claims_cbor(
			actor_claims_cbor([
				(ACTOR_CLAIMS_ROLE_FIELD, cbor_text("tenant_admin")),
				(ACTOR_CLAIMS_SCOPES_FIELD, cbor_array([cbor_text("tenant")])),
				(ACTOR_CLAIMS_TENANT_ID_FIELD, cbor_text("7")),
				("version", cbor_text("v1")),
			])
			.as_slice(),
		)
		.unwrap_err();
		assert!(err.to_string().contains("unknown field"));
	}

	#[test]
	fn actor_claims_v1_rejects_duplicate_field() {
		let err = parse_actor_claims_cbor(
			actor_claims_cbor([
				(ACTOR_CLAIMS_ROLE_FIELD, cbor_text("tenant_admin")),
				(ACTOR_CLAIMS_ROLE_FIELD, cbor_text("cluster_admin")),
				(ACTOR_CLAIMS_TENANT_ID_FIELD, cbor_text("7")),
				(ACTOR_CLAIMS_KEY_ID_FIELD, cbor_bytes([7, 7, 7])),
			])
			.as_slice(),
		)
		.unwrap_err();
		assert!(err.to_string().contains("duplicate role"));
	}

	#[test]
	fn actor_claims_v1_rejects_empty_key_id() {
		let err = parse_actor_claims_cbor(
			actor_claims_cbor([
				(ACTOR_CLAIMS_ROLE_FIELD, cbor_text("tenant_admin")),
				(ACTOR_CLAIMS_SCOPES_FIELD, cbor_array([cbor_text("tenant")])),
				(ACTOR_CLAIMS_TENANT_ID_FIELD, cbor_text("7")),
				(ACTOR_CLAIMS_KEY_ID_FIELD, cbor_bytes([])),
			])
			.as_slice(),
		)
		.unwrap_err();
		assert!(err.to_string().contains("key_id must not be empty"));
	}

	fn actor_claims_cbor<const N: usize>(fields: [(&str, Vec<u8>); N]) -> Vec<u8> {
		let mut out = Vec::new();
		cbor_write_len(&mut out, 5, fields.len());
		for (key, value) in fields {
			cbor_write_text(&mut out, key);
			out.extend_from_slice(&value);
		}
		out
	}

	fn cbor_array<const N: usize>(items: [Vec<u8>; N]) -> Vec<u8> {
		let mut out = Vec::new();
		cbor_write_len(&mut out, 4, items.len());
		for item in items {
			out.extend_from_slice(&item);
		}
		out
	}

	fn cbor_text(text: &str) -> Vec<u8> {
		let mut out = Vec::new();
		cbor_write_len(&mut out, 3, text.len());
		out.extend_from_slice(text.as_bytes());
		out
	}

	fn cbor_bytes<const N: usize>(bytes: [u8; N]) -> Vec<u8> {
		let mut out = Vec::new();
		cbor_write_len(&mut out, 2, bytes.len());
		out.extend_from_slice(&bytes);
		out
	}

	fn cbor_null() -> Vec<u8> {
		vec![0xF6]
	}

	fn cbor_write_text(out: &mut Vec<u8>, text: &str) {
		cbor_write_len(out, 3, text.len());
		out.extend_from_slice(text.as_bytes());
	}

	fn cbor_write_len(out: &mut Vec<u8>, major: u8, len: usize) {
		match len {
			0..=23 => out.push((major << 5) | (len as u8)),
			24..=0xFF => {
				out.push((major << 5) | 24);
				out.push(len as u8);
			}
			0x100..=0xFFFF => {
				out.push((major << 5) | 25);
				out.extend_from_slice(&(len as u16).to_be_bytes());
			}
			_ => panic!("test helper does not support CBOR length {len}"),
		}
	}
}
