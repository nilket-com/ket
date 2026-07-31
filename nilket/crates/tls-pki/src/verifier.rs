use std::sync::Arc;

use rustls::RootCertStore;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::ServerCertVerifier;
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::ClientCertVerifier;

use crate::config_builder::install_rustls_provider;
use crate::error::PkiError;

pub fn build_client_cert_chain_verifier(
	ca_roots: Arc<RootCertStore>,
) -> Result<Arc<dyn ClientCertVerifier>, PkiError> {
	install_rustls_provider();
	WebPkiClientVerifier::builder(ca_roots)
		.build()
		.map(|verifier| verifier as Arc<dyn ClientCertVerifier>)
		.map_err(|error| PkiError::Config(format!("client verifier: {error}")))
}

pub fn build_server_cert_chain_verifier(
	ca_roots: Arc<RootCertStore>,
) -> Result<Arc<dyn ServerCertVerifier>, PkiError> {
	install_rustls_provider();
	WebPkiServerVerifier::builder(ca_roots)
		.build()
		.map(|verifier| verifier as Arc<dyn ServerCertVerifier>)
		.map_err(|error| PkiError::Config(format!("server verifier: {error}")))
}
