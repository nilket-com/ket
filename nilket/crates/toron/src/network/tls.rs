use std::path::{Path, PathBuf};
use std::sync::Arc;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
	CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, Error as RustlsError,
	ServerConfig, SignatureScheme,
};
use tls_pki::{
	PkiError, TlsMaterial, build_client_cert_chain_verifier,
	build_rustls_client_config as build_pki_rustls_client_config,
	build_rustls_server_config as build_pki_rustls_server_config, build_server_cert_chain_verifier,
	canonical_dns_name, extract_dns_sans,
};
use uuid::Uuid;

use crate::config::TlsConfig;
use crate::distributed::identity::WorkerId;

pub const DEFAULT_CLUSTER_DOMAIN: &str = tls_pki::DEFAULT_CLUSTER_DOMAIN;
pub const AUTH_QUIC_ALPN: &[u8] = b"sql-task-auth";
pub const OBSERVABILITY_HTTP_ALPN: &[u8] = b"http/1.1";

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

impl From<PkiError> for TlsError {
	fn from(error: PkiError) -> Self {
		match error {
			PkiError::KeyRead { .. } | PkiError::KeyParseNotPkcs8 { .. } => TlsError::KeyNotPkcs8,
			PkiError::EnvInterpolation { .. } => TlsError::EnvInterpolation,
			PkiError::CertRead { path, .. } | PkiError::CertParse { path } => {
				TlsError::CertFile(path)
			}
			PkiError::CaRead { path, .. } | PkiError::CaParse { path } => {
				TlsError::PeerCaFile(path)
			}
			PkiError::SanShapeRejected { .. }
			| PkiError::SanPrefixRejected { .. }
			| PkiError::DerParse { .. }
			| PkiError::CborParse { .. } => TlsError::SanShapeRejected,
			PkiError::Config(message) => TlsError::Config(message),
		}
	}
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
	canonical_dns_name("toron", &worker_id.get().to_string(), cluster_domain)
}

pub fn canonical_client_dns_name(principal: &str, cluster_domain: &str) -> String {
	canonical_dns_name("client", principal, cluster_domain)
}

pub fn canonical_monitor_dns_name(principal: &str, cluster_domain: &str) -> String {
	canonical_dns_name("monitor", principal, cluster_domain)
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
	tls_pki::material::reject_env_interpolation(path).map_err(TlsError::from)
}

pub fn admit_san(
	san: &str,
	role: ListenerRole,
	cluster_domain: &str,
) -> Result<PeerKind, TlsError> {
	let identity = tls_pki::admit_san(san, cluster_domain, &["toron", "client", "monitor"])
		.map_err(|_| TlsError::SanShapeRejected)?;
	let prefix = identity.prefix.as_str();
	let id_token = identity.id_token.as_str();

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
	let dns_sans = extract_dns_sans(leaf).map_err(TlsError::from)?;
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

pub fn http_client_config_from_paths(
	cert_path: &Path,
	key_path: &Path,
	ca_path: &Path,
) -> Result<ClientConfig, TlsError> {
	for path in [cert_path, key_path, ca_path] {
		reject_env_interpolation(path)?;
	}
	let material = TlsMaterial::load(cert_path, key_path, ca_path)?;
	build_rustls_client_config(&material, OBSERVABILITY_HTTP_ALPN)
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
	let verifier = build_client_cert_chain_verifier(material.ca_roots.clone())?;
	let role_verifier = Arc::new(SanClientCertVerifier {
		inner: verifier,
		role,
		cluster_domain: cluster_domain.to_string(),
	});
	build_pki_rustls_server_config(material, role_verifier, alpn).map_err(TlsError::from)
}

fn client_config(material: &TlsMaterial) -> Result<quinn::ClientConfig, TlsError> {
	let rustls_config = build_rustls_client_config(material, AUTH_QUIC_ALPN)?;
	let quic_config = QuicClientConfig::try_from(rustls_config)
		.map_err(|error| TlsError::Config(format!("QUIC client config: {error}")))?;
	Ok(quinn::ClientConfig::new(Arc::new(quic_config)))
}

fn build_rustls_client_config(
	material: &TlsMaterial,
	alpn: &[u8],
) -> Result<ClientConfig, TlsError> {
	let verifier = build_server_cert_chain_verifier(material.ca_roots.clone())?;
	let exact_server_verifier = Arc::new(ExactServerCertVerifier { inner: verifier });
	build_pki_rustls_client_config(material, exact_server_verifier, alpn).map_err(TlsError::from)
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
		let dns_sans = extract_dns_sans(end_entity).map_err(|_| {
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
		let dns_sans = extract_dns_sans(end_entity).map_err(|_| {
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
