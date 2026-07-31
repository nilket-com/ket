use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rcgen::{
	BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
	KeyUsagePurpose,
};
use rustls::pki_types::CertificateDer;
use tls_pki::{
	CborReader, DEFAULT_CLUSTER_DOMAIN, PkiError, TlsMaterial, admit_san,
	build_client_cert_chain_verifier, build_rustls_client_config, build_rustls_server_config,
	build_server_cert_chain_verifier, canonical_dns_name, extract_dns_sans, extract_extension,
	extract_subject,
};

#[test]
fn loads_material_extracts_der_and_builds_configs() {
	let pki = TestPki::new("client-analytics.cluster.local");
	let material =
		TlsMaterial::load(&pki.cert_path, &pki.key_path, &pki.ca_path).expect("load material");
	assert_eq!(material.cert_chain.len(), 1);

	let leaf = CertificateDer::from(pki.leaf_der.clone());
	assert_eq!(
		extract_dns_sans(&leaf).expect("dns sans"),
		vec!["client-analytics.cluster.local".to_string()]
	);
	assert!(
		extract_subject(&leaf)
			.expect("subject")
			.display
			.contains("CN=client-analytics.cluster.local")
	);
	assert!(
		extract_extension(&leaf, &[2, 5, 29, 17])
			.expect("san extension")
			.is_some()
	);

	let client_verifier =
		build_client_cert_chain_verifier(material.ca_roots.clone()).expect("client verifier");
	let server =
		build_rustls_server_config(&material, client_verifier, b"test-alpn").expect("server cfg");
	assert_eq!(server.alpn_protocols, vec![b"test-alpn".to_vec()]);
}

#[test]
fn tls_pki_tls_material_load_rejects_env_interpolation() {
	for (cert_path, key_path, ca_path) in [
		("${CERT}", "leaf.key", "ca.pem"),
		("leaf.pem", "${KEY}", "ca.pem"),
		("leaf.pem", "leaf.key", "${CA}"),
	] {
		assert!(
			matches!(
				TlsMaterial::load(
					Path::new(cert_path),
					Path::new(key_path),
					Path::new(ca_path)
				),
				Err(PkiError::EnvInterpolation { .. })
			),
			"expected env-interpolation rejection for {cert_path}, {key_path}, {ca_path}"
		);
	}
}

#[test]
fn tls_pki_build_rustls_client_config_accepts_alpn_parameter() {
	let pki = TestPki::new("client-analytics.cluster.local");
	let material =
		TlsMaterial::load(&pki.cert_path, &pki.key_path, &pki.ca_path).expect("load material");

	let sql_verifier =
		build_server_cert_chain_verifier(material.ca_roots.clone()).expect("sql verifier");
	let sql =
		build_rustls_client_config(&material, sql_verifier, b"sql-task-auth").expect("sql cfg");
	assert_eq!(sql.alpn_protocols, vec![b"sql-task-auth".to_vec()]);

	let http_verifier =
		build_server_cert_chain_verifier(material.ca_roots.clone()).expect("http verifier");
	let http = build_rustls_client_config(&material, http_verifier, b"http/1.1").expect("http cfg");
	assert_eq!(http.alpn_protocols, vec![b"http/1.1".to_vec()]);
	assert_ne!(sql.alpn_protocols, http.alpn_protocols);
}

#[test]
fn san_helpers_are_prefix_parameterized_and_exact_shape() {
	assert_eq!(
		canonical_dns_name("client", "analytics", DEFAULT_CLUSTER_DOMAIN),
		"client-analytics.cluster.local"
	);

	let identity = admit_san(
		"client-analytics.cluster.local",
		DEFAULT_CLUSTER_DOMAIN,
		&["client", "monitor"],
	)
	.expect("client san");
	assert_eq!(identity.prefix, "client");
	assert_eq!(identity.id_token, "analytics");
	assert_eq!(identity.cluster_domain, DEFAULT_CLUSTER_DOMAIN);

	assert!(admit_san("node-7.cluster.local", DEFAULT_CLUSTER_DOMAIN, &["client"]).is_err());
	assert!(
		admit_san(
			"client-analytics.extra.cluster.local",
			DEFAULT_CLUSTER_DOMAIN,
			&["client"]
		)
		.is_err()
	);
	assert!(
		admit_san(
			"client-analytics_dev.cluster.local",
			DEFAULT_CLUSTER_DOMAIN,
			&["client"]
		)
		.is_err()
	);
}

#[test]
fn cbor_reader_parses_stable_primitives_and_rejects_trailing_shape_errors() {
	let mut reader = CborReader::new(&[
		0xA3, 0x64, b'r', b'o', b'l', b'e', 0x65, b'a', b'd', b'm', b'i', b'n', 0x63, b'k', b'e',
		b'y', 0x43, 1, 2, 3, 0x66, b't', b'e', b'n', b'a', b'n', b't', 0xF6,
	]);

	assert_eq!(reader.read_len_for_major(5, "map").expect("map"), 3);
	assert_eq!(reader.read_text("role key").expect("role key"), "role");
	assert_eq!(reader.read_text("role").expect("role"), "admin");
	assert_eq!(reader.read_text("key key").expect("key key"), "key");
	assert_eq!(reader.read_bytes("key").expect("key"), vec![1, 2, 3]);
	assert_eq!(
		reader.read_text("tenant key").expect("tenant key"),
		"tenant"
	);
	assert!(reader.peek_simple_null());
	reader.read_null("tenant").expect("tenant null");
	assert!(reader.is_empty());

	let mut bad = CborReader::new(&[0x9F]);
	assert!(bad.read_len_for_major(4, "indefinite array").is_err());
}

struct TestPki {
	_dir: TestDir,
	cert_path: PathBuf,
	key_path: PathBuf,
	ca_path: PathBuf,
	leaf_der: Vec<u8>,
}

impl TestPki {
	fn new(dns_name: &str) -> Self {
		let dir = TestDir::new();
		let cert_path = dir.path().join("leaf.pem");
		let key_path = dir.path().join("leaf.key");
		let ca_path = dir.path().join("ca.pem");
		let ca = ca_cert();
		let leaf = leaf_cert(dns_name);
		let leaf_der = leaf.serialize_der_with_signer(&ca).expect("leaf DER");
		write_signed_cert(&cert_path, &key_path, &leaf, &ca);
		fs::write(&ca_path, ca.serialize_pem().expect("ca PEM")).expect("write ca");
		Self {
			_dir: dir,
			cert_path,
			key_path,
			ca_path,
			leaf_der,
		}
	}
}

fn ca_cert() -> Certificate {
	let mut params = CertificateParams::new(Vec::new());
	params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
	params.key_usages = vec![
		KeyUsagePurpose::DigitalSignature,
		KeyUsagePurpose::KeyCertSign,
	];
	params
		.distinguished_name
		.push(DnType::CommonName, "tls-pki-test-ca");
	Certificate::from_params(params).expect("CA cert")
}

fn leaf_cert(dns_name: &str) -> Certificate {
	let mut params = CertificateParams::new(vec![dns_name.to_string()]);
	params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
	params.extended_key_usages = vec![
		ExtendedKeyUsagePurpose::ServerAuth,
		ExtendedKeyUsagePurpose::ClientAuth,
	];
	params.distinguished_name.push(DnType::CommonName, dns_name);
	Certificate::from_params(params).expect("leaf cert")
}

fn write_signed_cert(cert_path: &Path, key_path: &Path, cert: &Certificate, ca: &Certificate) {
	fs::write(
		cert_path,
		cert.serialize_pem_with_signer(ca).expect("signed cert PEM"),
	)
	.expect("write cert");
	fs::write(key_path, cert.serialize_private_key_pem()).expect("write key");
}

struct TestDir {
	path: PathBuf,
}

impl TestDir {
	fn new() -> Self {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("clock after epoch")
			.as_nanos();
		let path =
			std::env::temp_dir().join(format!("tls-pki-test-{}-{nanos}", std::process::id()));
		fs::create_dir_all(&path).expect("create temp dir");
		Self { path }
	}

	fn path(&self) -> &Path {
		&self.path
	}
}

impl Drop for TestDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.path);
	}
}
