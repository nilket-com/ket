use std::path::PathBuf;

use rustls::pki_types::pem;

#[derive(Debug, thiserror::Error)]
pub enum PkiError {
	#[error("tls cert file not found or unreadable: {path}")]
	CertRead { path: PathBuf, source: pem::Error },
	#[error("tls cert file not found or unreadable: {path}")]
	CertParse { path: PathBuf },
	#[error("tls listener_key file not found or unreadable: {path}")]
	KeyRead {
		path: PathBuf,
		source: std::io::Error,
	},
	#[error("tls listener_key must be unencrypted PEM PKCS#8")]
	KeyParseNotPkcs8 { path: PathBuf },
	#[error(transparent)]
	KeyAccess(#[from] key_guard::KeyAccessError),
	#[error("certificate issuance failed: {reason}")]
	Issue { reason: String },
	#[error("tls peer_ca file not found or unreadable: {path}")]
	CaRead { path: PathBuf, source: pem::Error },
	#[error("tls peer_ca file not found or unreadable: {path}")]
	CaParse { path: PathBuf },
	#[error("tls config paths do not interpolate environment variables")]
	EnvInterpolation { path: PathBuf },
	#[error("tls peer cert SAN admission rejected: {reason}")]
	SanShapeRejected { reason: String },
	#[error("tls peer cert SAN prefix {prefix} rejected; allowed: {allowed:?}")]
	SanPrefixRejected {
		prefix: String,
		allowed: Vec<String>,
	},
	#[error("DER parse error: {reason}")]
	DerParse { reason: String },
	#[error("CBOR parse error: {reason}")]
	CborParse { reason: String },
	#[error("tls config error: {0}")]
	Config(String),
}
