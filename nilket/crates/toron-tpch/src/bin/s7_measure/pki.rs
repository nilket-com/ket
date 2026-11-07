//! Harness-generated test certificates, as in the wendelon test PKI
//! (`crates/wendelon/tests/support/tls_pki.rs`): a data CA, an ops CA, the
//! node-1 `data_node` server certificate, an `operator` certificate for
//! tenant/table administration, and a `data_node` writer certificate for the
//! data path: router ingress classifies `TABLE_WRITE_REQ` as node-required on
//! both commits, as the test PKI's `data_client` (node-2) certificate does.
//! Generated with `openssl` into the block directory, so every block has
//! fresh material.

use std::path::{Path, PathBuf};
use std::process::Command;
use wendelon::network::tls::ACTOR_CLAIMS_OID_DOTTED;

pub const CLUSTER_DOMAIN: &str = "cluster.local";
const CERT_VALIDITY_DAYS: &str = "7";

pub struct Pki {
	pub ca_cert: PathBuf,
	pub ops_ca_cert: PathBuf,
	pub server_cert: PathBuf,
	pub server_key: PathBuf,
	pub operator_cert: PathBuf,
	pub operator_key: PathBuf,
	pub writer_cert: PathBuf,
	pub writer_key: PathBuf,
}

pub fn write(dir: &Path, node_id: u32) -> std::io::Result<Pki> {
	std::fs::create_dir_all(dir)?;
	let pki = Pki {
		ca_cert: dir.join("ca.crt"),
		ops_ca_cert: dir.join("ops_ca.crt"),
		server_cert: dir.join("server.crt"),
		server_key: dir.join("server.key"),
		operator_cert: dir.join("operator.crt"),
		operator_key: dir.join("operator.key"),
		writer_cert: dir.join("writer.crt"),
		writer_key: dir.join("writer.key"),
	};
	let ca_key = dir.join("ca.key");
	let ops_ca_key = dir.join("ops_ca.key");
	ca("s7-measure-data-ca", &pki.ca_cert, &ca_key)?;
	ca("s7-measure-ops-ca", &pki.ops_ca_cert, &ops_ca_key)?;
	leaf(
		dir,
		&Leaf {
			common_name: &format!("node-{node_id}"),
			role: "data_node",
			dns: Some(wendelon::network::tls::canonical_node_dns_name(
				node_id,
				CLUSTER_DOMAIN,
			)),
			key_id: node_id.to_le_bytes().to_vec(),
			cert: &pki.server_cert,
			key: &pki.server_key,
			ca_cert: &pki.ca_cert,
			ca_key: &ca_key,
			serial: 1001,
		},
	)?;
	leaf(
		dir,
		&Leaf {
			common_name: "operator",
			role: "operator",
			dns: None,
			key_id: vec![5, 6, 7, 8],
			cert: &pki.operator_cert,
			key: &pki.operator_key,
			ca_cert: &pki.ops_ca_cert,
			ca_key: &ops_ca_key,
			serial: 2001,
		},
	)?;
	let writer_id = node_id + 1;
	leaf(
		dir,
		&Leaf {
			common_name: &format!("node-{writer_id}"),
			role: "data_node",
			dns: Some(wendelon::network::tls::canonical_node_dns_name(
				writer_id,
				CLUSTER_DOMAIN,
			)),
			key_id: writer_id.to_le_bytes().to_vec(),
			cert: &pki.writer_cert,
			key: &pki.writer_key,
			ca_cert: &pki.ca_cert,
			ca_key: &ca_key,
			serial: 1002,
		},
	)?;
	Ok(pki)
}

struct Leaf<'a> {
	common_name: &'a str,
	role: &'a str,
	dns: Option<String>,
	key_id: Vec<u8>,
	cert: &'a Path,
	key: &'a Path,
	ca_cert: &'a Path,
	ca_key: &'a Path,
	serial: u64,
}

fn ca(common_name: &str, cert: &Path, key: &Path) -> std::io::Result<()> {
	openssl(
		Command::new("openssl")
			.args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout"])
			.arg(key)
			.arg("-out")
			.arg(cert)
			.args(["-days", CERT_VALIDITY_DAYS, "-sha256", "-subj"])
			.arg(format!("/CN={common_name}"))
			.args(["-addext", "basicConstraints=critical,CA:TRUE"])
			.args(["-addext", "keyUsage=critical,keyCertSign,cRLSign"]),
	)
}

fn leaf(dir: &Path, spec: &Leaf<'_>) -> std::io::Result<()> {
	let csr = dir.join(format!("{}.csr", spec.common_name));
	let ext = dir.join(format!("{}.ext", spec.common_name));
	let claims = actor_claims(spec.role, &["cluster"], &spec.key_id);
	let mut text = String::from(
		"[v3_leaf]\nbasicConstraints=critical,CA:false\n\
		 keyUsage=critical,digitalSignature,keyEncipherment\n\
		 extendedKeyUsage=serverAuth,clientAuth\nsubjectKeyIdentifier=hash\n\
		 authorityKeyIdentifier=keyid,issuer\n",
	);
	text.push_str(&format!(
		"{ACTOR_CLAIMS_OID_DOTTED}=DER:{}\n",
		claims
			.iter()
			.map(|b| format!("{b:02X}"))
			.collect::<Vec<_>>()
			.join(":")
	));
	if let Some(dns) = &spec.dns {
		text.push_str(&format!(
			"subjectAltName=@alt_names\n\n[alt_names]\nDNS.1={dns}\n"
		));
	}
	std::fs::write(&ext, text)?;
	openssl(
		Command::new("openssl")
			.args(["req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout"])
			.arg(spec.key)
			.arg("-out")
			.arg(&csr)
			.arg("-subj")
			.arg(format!("/CN={}", spec.common_name)),
	)?;
	openssl(
		Command::new("openssl")
			.args(["x509", "-req", "-in"])
			.arg(&csr)
			.arg("-CA")
			.arg(spec.ca_cert)
			.arg("-CAkey")
			.arg(spec.ca_key)
			.arg("-out")
			.arg(spec.cert)
			.args(["-days", CERT_VALIDITY_DAYS, "-sha256", "-set_serial"])
			.arg(spec.serial.to_string())
			.arg("-extfile")
			.arg(&ext)
			.args(["-extensions", "v3_leaf"]),
	)?;
	use std::os::unix::fs::PermissionsExt;
	std::fs::set_permissions(spec.key, std::fs::Permissions::from_mode(0o600))
}

fn openssl(command: &mut Command) -> std::io::Result<()> {
	let output = command.output()?;
	if output.status.success() {
		return Ok(());
	}
	Err(std::io::Error::other(format!(
		"openssl failed ({:?}): {}",
		output.status.code(),
		String::from_utf8_lossy(&output.stderr)
	)))
}

/// The CBOR actor-claims map: role, scopes, null tenant, key id.
fn actor_claims(role: &str, scopes: &[&str], key_id: &[u8]) -> Vec<u8> {
	let mut out = Vec::new();
	cbor_len(&mut out, 5, 4);
	cbor_text(&mut out, "role");
	cbor_text(&mut out, role);
	cbor_text(&mut out, "scopes");
	cbor_len(&mut out, 4, scopes.len());
	for scope in scopes {
		cbor_text(&mut out, scope);
	}
	cbor_text(&mut out, "tenant_id");
	out.push(0xF6);
	cbor_text(&mut out, "key_id");
	cbor_len(&mut out, 2, key_id.len());
	out.extend_from_slice(key_id);
	out
}

fn cbor_text(out: &mut Vec<u8>, text: &str) {
	cbor_len(out, 3, text.len());
	out.extend_from_slice(text.as_bytes());
}

fn cbor_len(out: &mut Vec<u8>, major: u8, len: usize) {
	match len {
		0..=23 => out.push((major << 5) | len as u8),
		24..=0xFF => out.extend_from_slice(&[(major << 5) | 24, len as u8]),
		_ => {
			out.push((major << 5) | 25);
			out.extend_from_slice(&(len as u16).to_be_bytes());
		}
	}
}
