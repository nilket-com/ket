#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use wendelon::network::tls::ACTOR_CLAIMS_OID_DOTTED;

pub const CLUSTER_DOMAIN: &str = "cluster.local";
const CERT_VALIDITY_DAYS: &str = "30";

pub struct TestPkiPaths {
	pub ca_cert: PathBuf,
	pub ops_ca_cert: PathBuf,
	pub server_cert: PathBuf,
	pub server_key: PathBuf,
	pub data_client_cert: PathBuf,
	pub data_client_key: PathBuf,
	pub operator_cert: PathBuf,
	pub operator_key: PathBuf,
	pub break_glass_cert: PathBuf,
	pub break_glass_key: PathBuf,
	pub operator_on_node_ca_cert: PathBuf,
	pub operator_on_node_ca_key: PathBuf,
	pub data_node_on_ops_ca_cert: PathBuf,
	pub data_node_on_ops_ca_key: PathBuf,
}

pub fn write_test_pki(dir: &Path) -> TestPkiPaths {
	write_test_pki_with_server_dns(dir, &format!("node-1.{CLUSTER_DOMAIN}"))
}

pub fn write_test_pki_with_server_dns(dir: &Path, server_dns_name: &str) -> TestPkiPaths {
	std::fs::create_dir_all(dir).unwrap();
	let paths = TestPkiPaths {
		ca_cert: dir.join("ca.crt"),
		ops_ca_cert: dir.join("ops_ca.crt"),
		server_cert: dir.join("server.crt"),
		server_key: dir.join("server.key"),
		data_client_cert: dir.join("data_client.crt"),
		data_client_key: dir.join("data_client.key"),
		operator_cert: dir.join("operator.crt"),
		operator_key: dir.join("operator.key"),
		break_glass_cert: dir.join("break_glass.crt"),
		break_glass_key: dir.join("break_glass.key"),
		operator_on_node_ca_cert: dir.join("operator_on_node_ca.crt"),
		operator_on_node_ca_key: dir.join("operator_on_node_ca.key"),
		data_node_on_ops_ca_cert: dir.join("data_node_on_ops_ca.crt"),
		data_node_on_ops_ca_key: dir.join("data_node_on_ops_ca.key"),
	};

	let data_ca_key = dir.join("ca.key");
	let ops_ca_key = dir.join("ops_ca.key");
	generate_ca("w2-test-ca", &paths.ca_cert, &data_ca_key);
	generate_ca("w2-test-ops-ca", &paths.ops_ca_cert, &ops_ca_key);

	generate_leaf(
		LeafSpec {
			common_name: "node-1",
			role: "data_node",
			node_dns_name: Some(server_dns_name.to_string()),
			key_id: 1u32.to_le_bytes().to_vec(),
			scopes: vec!["cluster"],
			cert_path: &paths.server_cert,
			key_path: &paths.server_key,
			ca_cert_path: &paths.ca_cert,
			ca_key_path: &data_ca_key,
			serial: 1001,
		},
		dir,
	);
	generate_leaf(
		LeafSpec {
			common_name: "node-2",
			role: "data_node",
			node_dns_name: Some(format!("node-2.{CLUSTER_DOMAIN}")),
			key_id: 2u32.to_le_bytes().to_vec(),
			scopes: vec!["cluster"],
			cert_path: &paths.data_client_cert,
			key_path: &paths.data_client_key,
			ca_cert_path: &paths.ca_cert,
			ca_key_path: &data_ca_key,
			serial: 1002,
		},
		dir,
	);
	generate_leaf(
		LeafSpec {
			common_name: "operator",
			role: "operator",
			node_dns_name: None,
			key_id: vec![5, 6, 7, 8],
			scopes: vec!["cluster"],
			cert_path: &paths.operator_cert,
			key_path: &paths.operator_key,
			ca_cert_path: &paths.ops_ca_cert,
			ca_key_path: &ops_ca_key,
			serial: 2001,
		},
		dir,
	);
	generate_leaf(
		LeafSpec {
			common_name: "break-glass",
			role: "break_glass_admin",
			node_dns_name: None,
			key_id: vec![9, 10, 11, 12],
			scopes: vec!["diagnostic"],
			cert_path: &paths.break_glass_cert,
			key_path: &paths.break_glass_key,
			ca_cert_path: &paths.ops_ca_cert,
			ca_key_path: &ops_ca_key,
			serial: 2002,
		},
		dir,
	);
	generate_leaf(
		LeafSpec {
			common_name: "operator-on-node-ca",
			role: "operator",
			node_dns_name: None,
			key_id: vec![13, 14, 15, 16],
			scopes: vec!["cluster"],
			cert_path: &paths.operator_on_node_ca_cert,
			key_path: &paths.operator_on_node_ca_key,
			ca_cert_path: &paths.ca_cert,
			ca_key_path: &data_ca_key,
			serial: 1003,
		},
		dir,
	);
	generate_leaf(
		LeafSpec {
			common_name: "data-node-on-ops-ca",
			role: "data_node",
			node_dns_name: Some(format!("node-2.{CLUSTER_DOMAIN}")),
			key_id: vec![17, 18, 19, 20],
			scopes: vec!["cluster"],
			cert_path: &paths.data_node_on_ops_ca_cert,
			key_path: &paths.data_node_on_ops_ca_key,
			ca_cert_path: &paths.ops_ca_cert,
			ca_key_path: &ops_ca_key,
			serial: 2003,
		},
		dir,
	);

	paths
}

struct LeafSpec<'a> {
	common_name: &'a str,
	role: &'a str,
	node_dns_name: Option<String>,
	key_id: Vec<u8>,
	scopes: Vec<&'a str>,
	cert_path: &'a Path,
	key_path: &'a Path,
	ca_cert_path: &'a Path,
	ca_key_path: &'a Path,
	serial: u64,
}

fn generate_ca(common_name: &str, cert_path: &Path, key_path: &Path) {
	run_openssl(
		Command::new("openssl")
			.arg("req")
			.arg("-x509")
			.arg("-newkey")
			.arg("rsa:2048")
			.arg("-nodes")
			.arg("-keyout")
			.arg(key_path)
			.arg("-out")
			.arg(cert_path)
			.arg("-days")
			.arg(CERT_VALIDITY_DAYS)
			.arg("-sha256")
			.arg("-subj")
			.arg(format!("/CN={common_name}"))
			.arg("-addext")
			.arg("basicConstraints=critical,CA:TRUE")
			.arg("-addext")
			.arg("keyUsage=critical,keyCertSign,cRLSign"),
	);
}

fn generate_leaf(spec: LeafSpec<'_>, dir: &Path) {
	let csr_path = dir.join(format!("{}.csr", spec.common_name));
	let ext_path = dir.join(format!("{}.ext", spec.common_name));
	let actor_claims = encode_actor_claims(spec.role, &spec.scopes, &spec.key_id);
	let mut ext = String::from(
		"[v3_leaf]\n\
basicConstraints=critical,CA:false\n\
keyUsage=critical,digitalSignature,keyEncipherment\n\
extendedKeyUsage=serverAuth,clientAuth\n\
subjectKeyIdentifier=hash\n\
authorityKeyIdentifier=keyid,issuer\n",
	);
	ext.push_str(&format!(
		"{ACTOR_CLAIMS_OID_DOTTED}=DER:{}\n",
		to_openssl_hex(&actor_claims)
	));
	if let Some(node_dns_name) = &spec.node_dns_name {
		ext.push_str("subjectAltName=@alt_names\n\n[alt_names]\n");
		ext.push_str(&format!("DNS.1={node_dns_name}\n"));
	}
	std::fs::write(&ext_path, ext).unwrap();

	run_openssl(
		Command::new("openssl")
			.arg("req")
			.arg("-new")
			.arg("-newkey")
			.arg("rsa:2048")
			.arg("-nodes")
			.arg("-keyout")
			.arg(spec.key_path)
			.arg("-out")
			.arg(&csr_path)
			.arg("-subj")
			.arg(format!("/CN={}", spec.common_name)),
	);
	run_openssl(
		Command::new("openssl")
			.arg("x509")
			.arg("-req")
			.arg("-in")
			.arg(&csr_path)
			.arg("-CA")
			.arg(spec.ca_cert_path)
			.arg("-CAkey")
			.arg(spec.ca_key_path)
			.arg("-out")
			.arg(spec.cert_path)
			.arg("-days")
			.arg(CERT_VALIDITY_DAYS)
			.arg("-sha256")
			.arg("-set_serial")
			.arg(spec.serial.to_string())
			.arg("-extfile")
			.arg(&ext_path)
			.arg("-extensions")
			.arg("v3_leaf"),
	);
}

fn run_openssl(command: &mut Command) {
	let output = command
		.output()
		.unwrap_or_else(|err| panic!("failed to run openssl: {err}"));
	if output.status.success() {
		return;
	}
	panic!(
		"openssl command failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
		output.status.code(),
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr),
	);
}

fn encode_actor_claims(role: &str, scopes: &[&str], key_id: &[u8]) -> Vec<u8> {
	let mut out = Vec::new();
	cbor_write_len(&mut out, 5, 4);

	cbor_write_text(&mut out, "role");
	cbor_write_text(&mut out, role);

	cbor_write_text(&mut out, "scopes");
	cbor_write_len(&mut out, 4, scopes.len());
	for scope in scopes {
		cbor_write_text(&mut out, scope);
	}

	cbor_write_text(&mut out, "tenant_id");
	out.push(0xF6);

	cbor_write_text(&mut out, "key_id");
	cbor_write_bytes(&mut out, key_id);

	out
}

fn cbor_write_text(out: &mut Vec<u8>, text: &str) {
	cbor_write_len(out, 3, text.len());
	out.extend_from_slice(text.as_bytes());
}

fn cbor_write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
	cbor_write_len(out, 2, bytes.len());
	out.extend_from_slice(bytes);
}

fn cbor_write_len(out: &mut Vec<u8>, major: u8, len: usize) {
	match len {
		0..=23 => out.push((major << 5) | (len as u8)),
		24..=0xFF => {
			out.push((major << 5) | 24);
			out.push(len as u8);
		}
		0x100..=0xFFFF => {
			out.push((major << 5) | 25);
			out.extend_from_slice(&(len as u16).to_be_bytes());
		}
		_ => panic!("CBOR test helper does not support length {len}"),
	}
}

fn to_openssl_hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 3);
	for (idx, byte) in bytes.iter().enumerate() {
		if idx > 0 {
			out.push(':');
		}
		use std::fmt::Write as _;
		write!(&mut out, "{byte:02X}").unwrap();
	}
	out
}
