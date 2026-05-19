use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{
	CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime, pem::PemObject,
};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
	CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, Error as RustlsError,
	RootCertStore, ServerConfig, SignatureScheme,
};
use uuid::Uuid;

use crate::config::TlsConfig;
use crate::distributed::identity::WorkerId;

pub const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";
pub const AUTH_QUIC_ALPN: &[u8] = b"sql-task-auth";
pub const OBSERVABILITY_HTTP_ALPN: &[u8] = b"http/1.1";

const SUBJECT_ALT_NAME_OID: &[u8] = &[0x55, 0x1D, 0x11];
const DNS_NAME_TAG: u8 = 0x82;
static INSTALL_RUSTLS_PROVIDER: Once = Once::new();

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
	#[error("tls listener_key must be unencrypted PEM PKCS#8")]
	KeyNotPkcs8,
	#[error("tls config paths do not interpolate environment variables")]
	EnvInterpolation,
	#[error("tls cluster_domain must not be empty")]
	ClusterDomainEmpty,
	#[error("tls cert file not found or unreadable: {0}")]
	CertFile(PathBuf),
	#[error("tls peer_ca file not found or unreadable: {0}")]
	PeerCaFile(PathBuf),
	#[error("tls listener cert SAN does not match node.worker_id")]
	ListenerSanMismatch,
	#[error("tls peer cert SAN admission rejected")]
	SanShapeRejected,
	#[error("tls peer cert SAN role rejected for listener")]
	SanRoleRejected,
	#[error("tls config error: {0}")]
	Config(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerRole {
	WorkerTask,
	Sql,
	Observability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerKind {
	SiblingDaemon { worker_id: WorkerId },
	Client { principal: String },
	Monitor { principal: String },
}

pub fn canonical_daemon_dns_name(worker_id: &WorkerId, cluster_domain: &str) -> String {
	format!("toron-{}.{cluster_domain}", worker_id.get())
}

pub fn canonical_client_dns_name(principal: &str, cluster_domain: &str) -> String {
	format!("client-{principal}.{cluster_domain}")
}

pub fn canonical_monitor_dns_name(principal: &str, cluster_domain: &str) -> String {
	format!("monitor-{principal}.{cluster_domain}")
}

pub fn validate_cluster_domain(cluster_domain: &str) -> Result<(), TlsError> {
	if cluster_domain.trim().is_empty() {
		return Err(TlsError::ClusterDomainEmpty);
	}
	if cluster_domain.split('.').any(|label| {
		label.is_empty()
			|| label.starts_with('-')
			|| label.ends_with('-')
			|| !label
				.chars()
				.all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
	}) {
		return Err(TlsError::Config(
			"tls cluster_domain must be a DNS name".to_string(),
		));
	}
	Ok(())
}

pub fn reject_env_interpolation(path: &Path) -> Result<(), TlsError> {
	let raw = path.to_string_lossy();
	if raw.contains("${") {
		return Err(TlsError::EnvInterpolation);
	}
	Ok(())
}

pub fn admit_san(
	san: &str,
	role: ListenerRole,
	cluster_domain: &str,
) -> Result<PeerKind, TlsError> {
	let suffix = format!(".{cluster_domain}");
	let leading = san
		.strip_suffix(&suffix)
		.ok_or(TlsError::SanShapeRejected)?;
	if leading.contains('.') {
		return Err(TlsError::SanShapeRejected);
	}
	let (prefix, id_token) = leading.split_once('-').ok_or(TlsError::SanShapeRejected)?;
	if id_token.is_empty()
		|| !id_token
			.chars()
			.all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
	{
		return Err(TlsError::SanShapeRejected);
	}

	match (prefix, role) {
		("toron", ListenerRole::WorkerTask) => {
			let uuid = Uuid::parse_str(id_token).map_err(|_| TlsError::SanShapeRejected)?;
			Ok(PeerKind::SiblingDaemon {
				worker_id: WorkerId::from_uuid(uuid),
			})
		}
		("toron", ListenerRole::Sql) => Err(TlsError::SanRoleRejected),
		("toron", ListenerRole::Observability) => Err(TlsError::SanRoleRejected),
		("client", ListenerRole::Sql) => Ok(PeerKind::Client {
			principal: id_token.to_string(),
		}),
		("client", ListenerRole::WorkerTask) => Err(TlsError::SanRoleRejected),
		("client", ListenerRole::Observability) => Err(TlsError::SanRoleRejected),
		("monitor", ListenerRole::Observability) => Ok(PeerKind::Monitor {
			principal: id_token.to_string(),
		}),
		("monitor", ListenerRole::WorkerTask | ListenerRole::Sql) => Err(TlsError::SanRoleRejected),
		_ => Err(TlsError::SanShapeRejected),
	}
}

pub struct ListenerPolicy {
	cluster_domain: String,
	server_config_worker_task: quinn::ServerConfig,
	server_config_sql: quinn::ServerConfig,
	server_config_observability: Arc<ServerConfig>,
	client_config: quinn::ClientConfig,
}

impl ListenerPolicy {
	pub fn load(config: &TlsConfig, worker_id: &WorkerId) -> Result<Self, TlsError> {
		validate_cluster_domain(&config.cluster_domain)?;
		for path in [&config.listener_cert, &config.listener_key, &config.peer_ca] {
			reject_env_interpolation(path)?;
		}
		let material =
			TlsMaterial::load(&config.listener_cert, &config.listener_key, &config.peer_ca)?;
		validate_listener_cert_identity(&material, worker_id, &config.cluster_domain)?;
		Ok(Self {
			cluster_domain: config.cluster_domain.clone(),
			server_config_worker_task: server_config_for(
				&material,
				ListenerRole::WorkerTask,
				&config.cluster_domain,
			)?,
			server_config_sql: server_config_for(
				&material,
				ListenerRole::Sql,
				&config.cluster_domain,
			)?,
			server_config_observability: Arc::new(build_rustls_server_config(
				&material,
				ListenerRole::Observability,
				&config.cluster_domain,
				OBSERVABILITY_HTTP_ALPN,
			)?),
			client_config: client_config(&material)?,
		})
	}

	pub fn cluster_domain(&self) -> &str {
		self.cluster_domain.as_str()
	}

	pub fn server_config_for(&self, role: ListenerRole) -> quinn::ServerConfig {
		match role {
			ListenerRole::WorkerTask => self.server_config_worker_task.clone(),
			ListenerRole::Sql => self.server_config_sql.clone(),
			ListenerRole::Observability => {
				panic!("observability uses rustls_server_config_for")
			}
		}
	}

	pub fn rustls_server_config_for(&self, role: ListenerRole) -> Arc<ServerConfig> {
		match role {
			ListenerRole::Observability => Arc::clone(&self.server_config_observability),
			ListenerRole::WorkerTask | ListenerRole::Sql => {
				panic!("worker and SQL listeners use QUIC server_config_for")
			}
		}
	}

	pub fn client_config(&self) -> quinn::ClientConfig {
		self.client_config.clone()
	}
}

fn validate_listener_cert_identity(
	material: &TlsMaterial,
	worker_id: &WorkerId,
	cluster_domain: &str,
) -> Result<(), TlsError> {
	let expected = canonical_daemon_dns_name(worker_id, cluster_domain);
	let leaf = material
		.cert_chain
		.first()
		.ok_or(TlsError::ListenerSanMismatch)?;
	let dns_sans = extract_dns_sans(leaf.as_ref()).map_err(|_| TlsError::SanShapeRejected)?;
	if dns_sans.iter().any(|san| {
		san == &expected && admit_san(san, ListenerRole::WorkerTask, cluster_domain).is_ok()
	}) {
		return Ok(());
	}
	Err(TlsError::ListenerSanMismatch)
}

pub fn client_config_from_paths(
	cert_path: &Path,
	key_path: &Path,
	ca_path: &Path,
) -> Result<quinn::ClientConfig, TlsError> {
	for path in [cert_path, key_path, ca_path] {
		reject_env_interpolation(path)?;
	}
	let material = TlsMaterial::load(cert_path, key_path, ca_path)?;
	client_config(&material)
}

struct TlsMaterial {
	cert_chain: Vec<CertificateDer<'static>>,
	key: PrivateKeyDer<'static>,
	ca_roots: Arc<RootCertStore>,
}

impl TlsMaterial {
	fn load(cert_path: &Path, key_path: &Path, ca_path: &Path) -> Result<Self, TlsError> {
		let cert_chain = CertificateDer::pem_file_iter(cert_path)
			.map_err(|_| TlsError::CertFile(cert_path.to_path_buf()))?
			.collect::<Result<Vec<_>, _>>()
			.map_err(|_| TlsError::CertFile(cert_path.to_path_buf()))?;
		if cert_chain.is_empty() {
			return Err(TlsError::CertFile(cert_path.to_path_buf()));
		}
		let key = PrivatePkcs8KeyDer::from_pem_file(key_path)
			.map(PrivateKeyDer::from)
			.map_err(|_| TlsError::KeyNotPkcs8)?;
		let certs = CertificateDer::pem_file_iter(ca_path)
			.map_err(|_| TlsError::PeerCaFile(ca_path.to_path_buf()))?
			.collect::<Result<Vec<_>, _>>()
			.map_err(|_| TlsError::PeerCaFile(ca_path.to_path_buf()))?;
		let mut ca_roots = RootCertStore::empty();
		for cert in certs {
			ca_roots
				.add(cert)
				.map_err(|_| TlsError::PeerCaFile(ca_path.to_path_buf()))?;
		}
		if ca_roots.is_empty() {
			return Err(TlsError::PeerCaFile(ca_path.to_path_buf()));
		}
		Ok(Self {
			cert_chain,
			key,
			ca_roots: Arc::new(ca_roots),
		})
	}
}

fn server_config_for(
	material: &TlsMaterial,
	role: ListenerRole,
	cluster_domain: &str,
) -> Result<quinn::ServerConfig, TlsError> {
	let rustls_config = build_rustls_server_config(material, role, cluster_domain, AUTH_QUIC_ALPN)?;
	let quic_config = QuicServerConfig::try_from(rustls_config)
		.map_err(|error| TlsError::Config(format!("QUIC server config: {error}")))?;
	Ok(quinn::ServerConfig::with_crypto(Arc::new(quic_config)))
}

fn build_rustls_server_config(
	material: &TlsMaterial,
	role: ListenerRole,
	cluster_domain: &str,
	alpn: &[u8],
) -> Result<ServerConfig, TlsError> {
	install_rustls_provider();
	let verifier = WebPkiClientVerifier::builder(material.ca_roots.clone())
		.build()
		.map_err(|error| TlsError::Config(format!("client verifier: {error}")))?;
	let role_verifier = Arc::new(SanClientCertVerifier {
		inner: verifier,
		role,
		cluster_domain: cluster_domain.to_string(),
	});
	let mut rustls_config = ServerConfig::builder()
		.with_client_cert_verifier(role_verifier)
		.with_single_cert(material.cert_chain.clone(), material.key.clone_key())
		.map_err(|error| TlsError::Config(format!("server cert/key: {error}")))?;
	rustls_config.alpn_protocols = vec![alpn.to_vec()];
	Ok(rustls_config)
}

fn client_config(material: &TlsMaterial) -> Result<quinn::ClientConfig, TlsError> {
	install_rustls_provider();
	let verifier = WebPkiServerVerifier::builder(material.ca_roots.clone())
		.build()
		.map_err(|error| TlsError::Config(format!("server verifier: {error}")))?;
	let exact_server_verifier = Arc::new(ExactServerCertVerifier { inner: verifier });
	let mut rustls_config = ClientConfig::builder()
		.dangerous()
		.with_custom_certificate_verifier(exact_server_verifier)
		.with_client_auth_cert(material.cert_chain.clone(), material.key.clone_key())
		.map_err(|error| TlsError::Config(format!("client cert/key: {error}")))?;
	rustls_config.alpn_protocols = vec![AUTH_QUIC_ALPN.to_vec()];
	let quic_config = QuicClientConfig::try_from(rustls_config)
		.map_err(|error| TlsError::Config(format!("QUIC client config: {error}")))?;
	Ok(quinn::ClientConfig::new(Arc::new(quic_config)))
}

fn install_rustls_provider() {
	INSTALL_RUSTLS_PROVIDER.call_once(|| {
		let _ = rustls::crypto::ring::default_provider().install_default();
	});
}

#[derive(Debug)]
struct ExactServerCertVerifier {
	inner: Arc<dyn ServerCertVerifier>,
}

impl ServerCertVerifier for ExactServerCertVerifier {
	fn verify_server_cert(
		&self,
		end_entity: &CertificateDer<'_>,
		intermediates: &[CertificateDer<'_>],
		server_name: &ServerName<'_>,
		ocsp_response: &[u8],
		now: UnixTime,
	) -> Result<ServerCertVerified, RustlsError> {
		self.inner.verify_server_cert(
			end_entity,
			intermediates,
			server_name,
			ocsp_response,
			now,
		)?;
		let ServerName::DnsName(dns_name) = server_name else {
			return Err(RustlsError::InvalidCertificate(
				CertificateError::ApplicationVerificationFailure,
			));
		};
		let expected = dns_name.as_ref();
		let Some((_, cluster_domain)) = expected.split_once('.') else {
			return Err(RustlsError::InvalidCertificate(
				CertificateError::ApplicationVerificationFailure,
			));
		};
		let dns_sans = extract_dns_sans(end_entity.as_ref()).map_err(|_| {
			RustlsError::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
		})?;
		if dns_sans.iter().any(|san| {
			san == expected && admit_san(san, ListenerRole::WorkerTask, cluster_domain).is_ok()
		}) {
			return Ok(ServerCertVerified::assertion());
		}
		Err(RustlsError::InvalidCertificate(
			CertificateError::ApplicationVerificationFailure,
		))
	}

	fn verify_tls12_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, RustlsError> {
		self.inner.verify_tls12_signature(message, cert, dss)
	}

	fn verify_tls13_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, RustlsError> {
		self.inner.verify_tls13_signature(message, cert, dss)
	}

	fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
		self.inner.supported_verify_schemes()
	}
}

#[derive(Debug)]
struct SanClientCertVerifier {
	inner: Arc<dyn ClientCertVerifier>,
	role: ListenerRole,
	cluster_domain: String,
}

impl ClientCertVerifier for SanClientCertVerifier {
	fn root_hint_subjects(&self) -> &[DistinguishedName] {
		self.inner.root_hint_subjects()
	}

	fn verify_client_cert(
		&self,
		end_entity: &CertificateDer<'_>,
		intermediates: &[CertificateDer<'_>],
		now: UnixTime,
	) -> Result<ClientCertVerified, RustlsError> {
		self.inner
			.verify_client_cert(end_entity, intermediates, now)?;
		let dns_sans = extract_dns_sans(end_entity.as_ref()).map_err(|_| {
			RustlsError::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
		})?;
		if dns_sans
			.iter()
			.any(|san| admit_san(san, self.role, &self.cluster_domain).is_ok())
		{
			return Ok(ClientCertVerified::assertion());
		}
		Err(RustlsError::InvalidCertificate(
			CertificateError::ApplicationVerificationFailure,
		))
	}

	fn verify_tls12_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, RustlsError> {
		self.inner.verify_tls12_signature(message, cert, dss)
	}

	fn verify_tls13_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, RustlsError> {
		self.inner.verify_tls13_signature(message, cert, dss)
	}

	fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
		self.inner.supported_verify_schemes()
	}
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
					let dns_name = std::str::from_utf8(value).map_err(|error| {
						Error::new(
							ErrorKind::InvalidData,
							format!("DNS SAN is not valid UTF-8/IA5: {error}"),
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

#[cfg(test)]
mod tests {
	use super::*;

	const DOMAIN: &str = "cluster.local";
	const WORKER_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

	#[test]
	fn worker_task_listener_admits_only_toron_uuid_sans() {
		let san = format!("toron-{WORKER_UUID}.{DOMAIN}");
		let admitted = admit_san(&san, ListenerRole::WorkerTask, DOMAIN).expect("admit worker");

		assert_eq!(
			admitted,
			PeerKind::SiblingDaemon {
				worker_id: WorkerId::from_uuid(Uuid::parse_str(WORKER_UUID).expect("uuid")),
			}
		);
		assert!(matches!(
			admit_san(
				"client-analytics.cluster.local",
				ListenerRole::WorkerTask,
				DOMAIN
			),
			Err(TlsError::SanRoleRejected)
		));
	}

	#[test]
	fn sql_listener_admits_only_client_principal_sans() {
		let admitted = admit_san("client-analytics.cluster.local", ListenerRole::Sql, DOMAIN)
			.expect("admit client");

		assert_eq!(
			admitted,
			PeerKind::Client {
				principal: "analytics".to_string(),
			}
		);
		assert!(matches!(
			admit_san(
				&format!("toron-{WORKER_UUID}.{DOMAIN}"),
				ListenerRole::Sql,
				DOMAIN
			),
			Err(TlsError::SanRoleRejected)
		));
	}

	#[test]
	fn observability_listener_admits_only_monitor_principal_sans() {
		let admitted = admit_san(
			"monitor-prometheus.cluster.local",
			ListenerRole::Observability,
			DOMAIN,
		)
		.expect("admit monitor");

		assert_eq!(
			admitted,
			PeerKind::Monitor {
				principal: "prometheus".to_string(),
			}
		);
		let daemon_san = format!("toron-{WORKER_UUID}.{DOMAIN}");
		for san in ["client-analytics.cluster.local", daemon_san.as_str()] {
			assert!(
				matches!(
					admit_san(san, ListenerRole::Observability, DOMAIN),
					Err(TlsError::SanRoleRejected)
				),
				"{san} should be rejected for observability"
			);
		}
	}

	#[test]
	fn san_admission_rejects_non_exact_cluster_shapes() {
		for san in [
			"client-analytics.extra.cluster.local",
			"client-analytics.cluster.local.evil",
			"client-.cluster.local",
			"client-analytics_dev.cluster.local",
		] {
			assert!(
				matches!(
					admit_san(san, ListenerRole::Sql, DOMAIN),
					Err(TlsError::SanShapeRejected)
				),
				"{san} should be rejected"
			);
		}
		assert!(matches!(
			admit_san(
				"toron-not-a-uuid.cluster.local",
				ListenerRole::WorkerTask,
				DOMAIN
			),
			Err(TlsError::SanShapeRejected)
		));
	}

	#[test]
	fn cluster_domain_rejects_empty_or_malformed_names() {
		for domain in [
			"",
			" ",
			".cluster.local",
			"cluster..local",
			"cluster.local-",
		] {
			assert!(
				validate_cluster_domain(domain).is_err(),
				"{domain:?} should be rejected"
			);
		}
		assert!(validate_cluster_domain("cluster.local").is_ok());
	}
}
