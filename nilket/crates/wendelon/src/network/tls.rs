//! mTLS config construction for the HRFT QUIC transport.
//!
//! All inter-node traffic — Data Ring, Control Ring, and Raft
//! internal — terminates on a single QUIC endpoint bound to either
//! `control_bind` or `data_bind` (see `plans/ops/configuration.md`).
//! Per `plans/network/security.md`, every connection is mTLS 1.3:
//! both sides present X.509 certificates issued by the cluster PKI,
//! and each node's `NodeId` binds to a Subject Alternative Name in
//! the cert.
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

use crate::schema::NodeId;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::any::Any;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;

/// Single ALPN token advertised on every HRFT QUIC connection.
/// Short, versionless; the framing header already carries a protocol
/// version if we need to bump the wire format without reshuffling
/// ALPN.
pub const HRFT_ALPN: &[u8] = b"wendelon-hrft";
pub const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";

const SUBJECT_ALT_NAME_OID: &[u8] = &[0x55, 0x1D, 0x11];
const DNS_NAME_TAG: u8 = 0x82;

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

/// Build a `quinn::ServerConfig` that:
///
/// * presents this node's cert+key to incoming peers,
/// * requires every client to present a cert chaining to
///   `material.ca_roots`,
/// * advertises only [`HRFT_ALPN`].
pub fn server_config(material: &TlsMaterial) -> std::io::Result<quinn::ServerConfig> {
	let verifier = WebPkiClientVerifier::builder(material.ca_roots.clone())
		.build()
		.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("client verifier: {e}")))?;

	let mut rustls_cfg = ServerConfig::builder()
		.with_client_cert_verifier(verifier)
		.with_single_cert(material.cert_chain.clone(), material.key.clone_key())
		.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("server cert/key: {e}")))?;
	rustls_cfg.alpn_protocols = vec![HRFT_ALPN.to_vec()];

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
	let mut rustls_cfg = ClientConfig::builder()
		.with_root_certificates((*material.ca_roots).clone())
		.with_client_auth_cert(material.cert_chain.clone(), material.key.clone_key())
		.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("client cert/key: {e}")))?;
	rustls_cfg.alpn_protocols = vec![HRFT_ALPN.to_vec()];

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

	parsed.ok_or_else(|| {
		Error::new(
			ErrorKind::PermissionDenied,
			format!("peer certificate contains no canonical node SAN for domain {cluster_domain}"),
		)
	})
}

// ============================================================
//  PEM LOADING HELPERS
// ============================================================

fn open_pem(path: &Path) -> std::io::Result<BufReader<File>> {
	File::open(path)
		.map(BufReader::new)
		.map_err(|e| Error::new(e.kind(), format!("opening {}: {e}", path.display())))
}

fn load_cert_chain(path: &Path) -> std::io::Result<Vec<CertificateDer<'static>>> {
	let mut reader = open_pem(path)?;
	rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()
}

fn load_private_key(path: &Path) -> std::io::Result<PrivateKeyDer<'static>> {
	let mut reader = open_pem(path)?;
	rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			format!("no private key found in {}", path.display()),
		)
	})
}

fn load_ca_roots(path: &Path) -> std::io::Result<RootCertStore> {
	let mut reader = open_pem(path)?;
	let mut roots = RootCertStore::empty();
	for cert in rustls_pemfile::certs(&mut reader) {
		let cert = cert?;
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

// ============================================================
//  ASN.1 HELPERS
// ============================================================

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
