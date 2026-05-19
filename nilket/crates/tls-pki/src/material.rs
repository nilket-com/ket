use std::path::Path;
use std::sync::Arc;

use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

use crate::error::PkiError;

pub struct TlsMaterial {
	pub cert_chain: Vec<CertificateDer<'static>>,
	pub key: PrivateKeyDer<'static>,
	pub ca_roots: Arc<RootCertStore>,
}

impl TlsMaterial {
	pub fn load(cert_path: &Path, key_path: &Path, ca_path: &Path) -> Result<Self, PkiError> {
		for path in [cert_path, key_path, ca_path] {
			reject_env_interpolation(path)?;
		}
		let cert_chain = load_cert_chain(cert_path)?;
		if cert_chain.is_empty() {
			return Err(PkiError::CertParse {
				path: cert_path.to_path_buf(),
			});
		}
		let key = load_private_key(key_path)?;
		let ca_roots = load_ca_roots(ca_path)?;
		Ok(Self {
			cert_chain,
			key,
			ca_roots: Arc::new(ca_roots),
		})
	}
}

pub fn reject_env_interpolation(path: &Path) -> Result<(), PkiError> {
	if path.to_string_lossy().contains("${") {
		return Err(PkiError::EnvInterpolation {
			path: path.to_path_buf(),
		});
	}
	Ok(())
}

pub fn load_cert_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>, PkiError> {
	reject_env_interpolation(path)?;
	CertificateDer::pem_file_iter(path)
		.map_err(|source| PkiError::CertRead {
			path: path.to_path_buf(),
			source,
		})?
		.collect::<Result<Vec<_>, _>>()
		.map_err(|_| PkiError::CertParse {
			path: path.to_path_buf(),
		})
}

pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, PkiError> {
	reject_env_interpolation(path)?;
	PrivateKeyDer::from_pem_file(path).map_err(|_| PkiError::KeyParseNotPkcs8 {
		path: path.to_path_buf(),
	})
}

pub fn load_ca_roots(path: &Path) -> Result<RootCertStore, PkiError> {
	reject_env_interpolation(path)?;
	let certs = CertificateDer::pem_file_iter(path)
		.map_err(|source| PkiError::CaRead {
			path: path.to_path_buf(),
			source,
		})?
		.collect::<Result<Vec<_>, _>>()
		.map_err(|_| PkiError::CaParse {
			path: path.to_path_buf(),
		})?;
	let mut roots = RootCertStore::empty();
	for cert in certs {
		roots.add(cert).map_err(|_| PkiError::CaParse {
			path: path.to_path_buf(),
		})?;
	}
	if roots.is_empty() {
		return Err(PkiError::CaParse {
			path: path.to_path_buf(),
		});
	}
	Ok(roots)
}
