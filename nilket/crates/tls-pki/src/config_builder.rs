use std::sync::{Arc, Once};

use rustls::client::danger::ServerCertVerifier;
use rustls::server::danger::ClientCertVerifier;
use rustls::{ClientConfig, ServerConfig};

use crate::error::PkiError;
use crate::material::TlsMaterial;

static INSTALL_RUSTLS_PROVIDER: Once = Once::new();

pub fn install_rustls_provider() {
	INSTALL_RUSTLS_PROVIDER.call_once(|| {
		let _ = rustls::crypto::ring::default_provider().install_default();
	});
}

pub fn build_rustls_server_config(
	material: &TlsMaterial,
	client_verifier: Arc<dyn ClientCertVerifier>,
	alpn: &[u8],
) -> Result<ServerConfig, PkiError> {
	build_rustls_server_config_with_alpns(material, client_verifier, vec![alpn.to_vec()])
}

pub fn build_rustls_server_config_with_alpns(
	material: &TlsMaterial,
	client_verifier: Arc<dyn ClientCertVerifier>,
	alpn_protocols: Vec<Vec<u8>>,
) -> Result<ServerConfig, PkiError> {
	install_rustls_provider();
	let mut rustls_config = ServerConfig::builder()
		.with_client_cert_verifier(client_verifier)
		.with_single_cert(material.cert_chain.clone(), material.key.clone_key())
		.map_err(|error| PkiError::Config(format!("server cert/key: {error}")))?;
	rustls_config.alpn_protocols = alpn_protocols;
	Ok(rustls_config)
}

pub fn build_rustls_client_config(
	material: &TlsMaterial,
	server_verifier: Arc<dyn ServerCertVerifier>,
	alpn: &[u8],
) -> Result<ClientConfig, PkiError> {
	build_rustls_client_config_with_alpns(material, server_verifier, vec![alpn.to_vec()])
}

pub fn build_rustls_client_config_with_alpns(
	material: &TlsMaterial,
	server_verifier: Arc<dyn ServerCertVerifier>,
	alpn_protocols: Vec<Vec<u8>>,
) -> Result<ClientConfig, PkiError> {
	install_rustls_provider();
	let mut rustls_config = ClientConfig::builder()
		.dangerous()
		.with_custom_certificate_verifier(server_verifier)
		.with_client_auth_cert(material.cert_chain.clone(), material.key.clone_key())
		.map_err(|error| PkiError::Config(format!("client cert/key: {error}")))?;
	rustls_config.alpn_protocols = alpn_protocols;
	Ok(rustls_config)
}
