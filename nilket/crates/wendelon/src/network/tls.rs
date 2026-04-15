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
//! This module deals only with the cryptographic shape — cert/key
//! loading, rustls `ClientConfig` / `ServerConfig` construction,
//! ALPN negotiation — and leaves policy (cert reloading, SAN → node
//! id mapping, revocation) to the ops layer that calls it.
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
//! * SAN extraction / NodeId verification beyond webpki's standard
//!   name-matching — a follow-up can add a custom verifier that
//!   pulls the numeric id out of the subject and cross-checks it
//!   against the Raft membership.
//! * Key log / TLS tracing for debugging.

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;

/// Single ALPN token advertised on every HRFT QUIC connection.
/// Short, versionless; the framing header already carries a protocol
/// version if we need to bump the wire format without reshuffling
/// ALPN.
pub const HRFT_ALPN: &[u8] = b"wendelon-hrft";

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
