//! Explicitly development-only PKI generation for the local Nilket profile.
//!
//! This is configuration material generation, not a production custody policy.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use rcgen::{
	BasicConstraints, Certificate, CertificateParams, CustomExtension, DnType,
	ExtendedKeyUsagePurpose, IsCa, KeyUsagePurpose,
};

const ACTOR_CLAIMS_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 55555, 1, 1];
const CLUSTER_DOMAIN: &str = "cluster.local";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevelopmentPki {
	pub node_ca_cert: PathBuf,
	pub node_ca_key: PathBuf,
	pub operator_ca_cert: PathBuf,
	pub operator_ca_key: PathBuf,
	pub wendelon_node_cert: PathBuf,
	pub wendelon_node_key: PathBuf,
	pub toron_storage_cert: PathBuf,
	pub toron_storage_key: PathBuf,
	pub operator_cert: PathBuf,
	pub operator_key: PathBuf,
}

impl DevelopmentPki {
	fn under(directory: &Path) -> Self {
		Self {
			node_ca_cert: directory.join("node-ca.crt"),
			node_ca_key: directory.join("node-ca.key"),
			operator_ca_cert: directory.join("operator-ca.crt"),
			operator_ca_key: directory.join("operator-ca.key"),
			wendelon_node_cert: directory.join("wendelon-node-1.crt"),
			wendelon_node_key: directory.join("wendelon-node-1.key"),
			toron_storage_cert: directory.join("toron-storage.crt"),
			toron_storage_key: directory.join("toron-storage.key"),
			operator_cert: directory.join("local-operator.crt"),
			operator_key: directory.join("local-operator.key"),
		}
	}

	fn all_paths(&self) -> [&Path; 10] {
		[
			&self.node_ca_cert,
			&self.node_ca_key,
			&self.operator_ca_cert,
			&self.operator_ca_key,
			&self.wendelon_node_cert,
			&self.wendelon_node_key,
			&self.toron_storage_cert,
			&self.toron_storage_key,
			&self.operator_cert,
			&self.operator_key,
		]
	}
}

/// Generate the complete local-development identity set once, or validate and
/// reuse the complete retained set. A partial set is refused rather than
/// silently repaired because retained profile artifacts are operator-visible.
pub fn write_local_development_pki(directory: &Path) -> io::Result<DevelopmentPki> {
	create_private_dir(directory)?;
	let paths = DevelopmentPki::under(directory);
	let present = paths.all_paths().map(Path::exists);
	if present.iter().all(|exists| *exists) {
		validate_permissions(&paths)?;
		return Ok(paths);
	}
	if present.iter().any(|exists| *exists) {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!(
				"partial local development PKI at {}; use the locked local-profile reset",
				directory.display()
			),
		));
	}

	let node_ca = ca("nilket-local-node-ca")?;
	let operator_ca = ca("nilket-local-operator-ca")?;
	let wendelon = leaf(
		"wendelon-node-1",
		Some(format!("node-1.{CLUSTER_DOMAIN}")),
		"data_node",
		None,
		1_u32.to_le_bytes().to_vec(),
	)?;
	let toron = leaf(
		"toron-storage",
		Some(format!("node-2.{CLUSTER_DOMAIN}")),
		"data_node",
		Some("0"),
		2_u32.to_le_bytes().to_vec(),
	)?;
	let operator = leaf(
		"local-operator",
		None,
		"operator",
		None,
		vec![0x4c, 0x4f, 0x43, 0x4c],
	)?;

	write_cert(
		&paths.node_ca_cert,
		node_ca.serialize_pem().map_err(rcgen_error)?,
	)?;
	write_key(&paths.node_ca_key, node_ca.serialize_private_key_pem())?;
	write_cert(
		&paths.operator_ca_cert,
		operator_ca.serialize_pem().map_err(rcgen_error)?,
	)?;
	write_key(
		&paths.operator_ca_key,
		operator_ca.serialize_private_key_pem(),
	)?;
	write_signed(
		&paths.wendelon_node_cert,
		&paths.wendelon_node_key,
		&wendelon,
		&node_ca,
	)?;
	write_signed(
		&paths.toron_storage_cert,
		&paths.toron_storage_key,
		&toron,
		&node_ca,
	)?;
	write_signed(
		&paths.operator_cert,
		&paths.operator_key,
		&operator,
		&operator_ca,
	)?;
	Ok(paths)
}

fn ca(common_name: &str) -> io::Result<Certificate> {
	let mut params = CertificateParams::new(Vec::new());
	params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
	params.key_usages = vec![
		KeyUsagePurpose::DigitalSignature,
		KeyUsagePurpose::KeyCertSign,
		KeyUsagePurpose::CrlSign,
	];
	params
		.distinguished_name
		.push(DnType::CommonName, common_name);
	Certificate::from_params(params).map_err(rcgen_error)
}

fn leaf(
	common_name: &str,
	dns_name: Option<String>,
	role: &str,
	tenant_id: Option<&str>,
	key_id: Vec<u8>,
) -> io::Result<Certificate> {
	let mut params = CertificateParams::new(dns_name.into_iter().collect::<Vec<_>>());
	params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
	params.extended_key_usages = vec![
		ExtendedKeyUsagePurpose::ServerAuth,
		ExtendedKeyUsagePurpose::ClientAuth,
	];
	params
		.distinguished_name
		.push(DnType::CommonName, common_name);
	params
		.custom_extensions
		.push(CustomExtension::from_oid_content(
			ACTOR_CLAIMS_OID,
			actor_claims(role, tenant_id, &key_id),
		));
	Certificate::from_params(params).map_err(rcgen_error)
}

fn actor_claims(role: &str, tenant_id: Option<&str>, key_id: &[u8]) -> Vec<u8> {
	let mut out = Vec::new();
	cbor_len(&mut out, 5, 4);
	cbor_text(&mut out, "role");
	cbor_text(&mut out, role);
	cbor_text(&mut out, "scopes");
	cbor_len(&mut out, 4, 1);
	cbor_text(&mut out, "cluster");
	cbor_text(&mut out, "tenant_id");
	match tenant_id {
		Some(tenant_id) => cbor_text(&mut out, tenant_id),
		None => out.push(0xf6),
	}
	cbor_text(&mut out, "key_id");
	cbor_len(&mut out, 2, key_id.len());
	out.extend_from_slice(key_id);
	out
}

fn cbor_text(out: &mut Vec<u8>, value: &str) {
	cbor_len(out, 3, value.len());
	out.extend_from_slice(value.as_bytes());
}

fn cbor_len(out: &mut Vec<u8>, major: u8, length: usize) {
	match length {
		0..=23 => out.push((major << 5) | length as u8),
		24..=0xff => out.extend_from_slice(&[(major << 5) | 24, length as u8]),
		0x100..=0xffff => {
			out.push((major << 5) | 25);
			out.extend_from_slice(&(length as u16).to_be_bytes());
		}
		_ => unreachable!("local actor claim fields are bounded"),
	}
}

fn write_signed(
	cert_path: &Path,
	key_path: &Path,
	leaf: &Certificate,
	ca: &Certificate,
) -> io::Result<()> {
	write_cert(
		cert_path,
		leaf.serialize_pem_with_signer(ca).map_err(rcgen_error)?,
	)?;
	write_key(key_path, leaf.serialize_private_key_pem())
}

fn write_cert(path: &Path, pem: String) -> io::Result<()> {
	write_create_new(path, pem.as_bytes(), 0o644)
}

fn write_key(path: &Path, pem: String) -> io::Result<()> {
	write_create_new(path, pem.as_bytes(), 0o600)
}

fn write_create_new(path: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(mode)
		.open(path)?;
	file.write_all(bytes)?;
	file.flush()
}

fn create_private_dir(path: &Path) -> io::Result<()> {
	fs::DirBuilder::new()
		.recursive(true)
		.mode(0o700)
		.create(path)?;
	fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

fn validate_permissions(paths: &DevelopmentPki) -> io::Result<()> {
	for key in [
		&paths.node_ca_key,
		&paths.operator_ca_key,
		&paths.wendelon_node_key,
		&paths.toron_storage_key,
		&paths.operator_key,
	] {
		let mode = fs::metadata(key)?.permissions().mode() & 0o777;
		if mode != 0o600 {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				format!(
					"local private key {} has mode {mode:o}; expected 600",
					key.display()
				),
			));
		}
	}
	Ok(())
}

fn rcgen_error(error: rcgen::Error) -> io::Error {
	io::Error::other(error)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::{SystemTime, UNIX_EPOCH};

	#[test]
	fn local_pki_is_create_once_permissioned_and_rust_generated() {
		let dir = std::env::temp_dir().join(format!(
			"nilket-local-pki-{}-{}",
			std::process::id(),
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("clock")
				.as_nanos()
		));
		let first = write_local_development_pki(&dir).expect("generate local pki");
		let retained = fs::read(&first.operator_cert).expect("read retained cert");
		let second = write_local_development_pki(&dir).expect("reuse complete local pki");
		assert_eq!(first, second);
		assert_eq!(
			fs::read(&second.operator_cert).expect("read cert"),
			retained
		);
		assert_eq!(
			fs::metadata(&dir).expect("metadata").permissions().mode() & 0o777,
			0o700
		);
		assert!(!include_str!("development.rs").contains("Command::new(\"openssl\")"));
		fs::remove_dir_all(dir).expect("remove fixture");
	}
}
