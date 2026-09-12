//! `nilket pki`: a deployment's own certificate authority (ADR 0165).
//!
//! Three operations, each writing only what it issued:
//!
//! - `init-ca --dir DIR [--name NAME]` creates `ca.crt` and `ca.key`.
//! - `issue-listener --dir DIR --worker-id UUID [--cluster-domain D]`
//!   creates `listener.crt` and `listener.key` with SAN
//!   `toron-<worker-id>.<domain>`, for the node's `[tls]` section.
//! - `sign-client --dir DIR --csr REQUEST.csr --tenant N --out CLIENT.crt
//!   [--cluster-domain D]` signs a request generated elsewhere with SAN
//!   `client-<tenant>.<domain>`. No private key is read or written: the
//!   requester keeps its key where it was generated.
//!
//! Files are created, never overwritten. Keys are written owner-only.

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use tls_pki::{
	DEFAULT_CLUSTER_DOMAIN, generate_ca, issue_listener, issue_storage, load_ca, sign_client_csr,
};

pub fn run<I>(program: &str, args: I) -> ExitCode
where
	I: IntoIterator<Item = String>,
{
	let args: Vec<String> = args.into_iter().collect();
	match args.first().map(String::as_str) {
		Some("init-ca") => report(init_ca(&args[1..])),
		Some("issue-listener") => report(issue_listener_command(&args[1..])),
		Some("sign-client") => report(sign_client(&args[1..])),
		Some("issue-storage") => report(issue_storage_command(&args[1..])),
		Some("--help") | Some("-h") | None => {
			print_usage(program);
			ExitCode::SUCCESS
		}
		Some(other) => {
			eprintln!("unknown {program} command: {other}");
			print_usage(program);
			ExitCode::from(2)
		}
	}
}

fn report(result: Result<String, String>) -> ExitCode {
	match result {
		Ok(message) => {
			println!("{message}");
			ExitCode::SUCCESS
		}
		Err(message) => {
			eprintln!("{message}");
			ExitCode::from(2)
		}
	}
}

fn print_usage(program: &str) {
	eprintln!(
		"Usage: {program} COMMAND [OPTIONS]\n\nCommands:\n  init-ca --dir DIR [--name NAME]\n      Create ca.crt and ca.key in DIR.\n  issue-listener --dir DIR --worker-id UUID [--cluster-domain DOMAIN]\n      Create listener.crt and listener.key in DIR for a Toron node.\n  sign-client --dir DIR --csr FILE --tenant N --out FILE [--cluster-domain DOMAIN]\n      Sign a client's certificate request for tenant N; writes only the certificate.
  issue-storage --ca-cert FILE --ca-key FILE --tenant N --out-dir DIR [--node-index I] [--cluster-domain DOMAIN]
      Create toron-storage.crt and toron-storage.key claiming tenant N, signed by the node CA."
	);
}

struct Options {
	values: Vec<(String, String)>,
}

impl Options {
	fn parse(args: &[String], allowed: &[&str]) -> Result<Self, String> {
		let mut values = Vec::new();
		let mut iter = args.iter();
		while let Some(flag) = iter.next() {
			if !allowed.contains(&flag.as_str()) {
				return Err(format!("unknown option: {flag}"));
			}
			let value = iter
				.next()
				.ok_or_else(|| format!("{flag} requires a value"))?;
			if values.iter().any(|(seen, _)| seen == flag) {
				return Err(format!("{flag} may be given only once"));
			}
			values.push((flag.clone(), value.clone()));
		}
		Ok(Self { values })
	}

	fn get(&self, flag: &str) -> Option<&str> {
		self.values
			.iter()
			.find(|(seen, _)| seen == flag)
			.map(|(_, value)| value.as_str())
	}

	fn required(&self, flag: &str) -> Result<&str, String> {
		self.get(flag).ok_or_else(|| format!("{flag} is required"))
	}

	fn cluster_domain(&self) -> &str {
		self.get("--cluster-domain")
			.unwrap_or(DEFAULT_CLUSTER_DOMAIN)
	}
}

fn init_ca(args: &[String]) -> Result<String, String> {
	let options = Options::parse(args, &["--dir", "--name"])?;
	let dir = PathBuf::from(options.required("--dir")?);
	let name = options.get("--name").unwrap_or("nilket-deployment-ca");
	std::fs::create_dir_all(&dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
	let issued = generate_ca(name).map_err(|error| error.to_string())?;
	write_cert(&dir.join("ca.crt"), &issued.cert)?;
	write_key(
		&dir.join("ca.key"),
		issued.key.as_deref().expect("a new CA has a key"),
	)?;
	Ok(format!("wrote {}/ca.crt and ca.key", dir.display()))
}

fn issue_listener_command(args: &[String]) -> Result<String, String> {
	let options = Options::parse(args, &["--dir", "--worker-id", "--cluster-domain"])?;
	let dir = PathBuf::from(options.required("--dir")?);
	let worker_id = options.required("--worker-id")?;
	uuid::Uuid::parse_str(worker_id).map_err(|error| format!("--worker-id: {error}"))?;
	let ca = read_ca(&dir)?;
	let issued = issue_listener(&ca, worker_id, options.cluster_domain())
		.map_err(|error| error.to_string())?;
	write_cert(&dir.join("listener.crt"), &issued.cert)?;
	write_key(
		&dir.join("listener.key"),
		issued.key.as_deref().expect("a listener has a key"),
	)?;
	Ok(format!(
		"wrote {}/listener.crt and listener.key for toron-{worker_id}.{}",
		dir.display(),
		options.cluster_domain()
	))
}

fn sign_client(args: &[String]) -> Result<String, String> {
	let options = Options::parse(
		args,
		&["--dir", "--csr", "--tenant", "--out", "--cluster-domain"],
	)?;
	let dir = PathBuf::from(options.required("--dir")?);
	let csr_path = PathBuf::from(options.required("--csr")?);
	let tenant = options.required("--tenant")?;
	let out = PathBuf::from(options.required("--out")?);
	let csr = std::fs::read_to_string(&csr_path)
		.map_err(|error| format!("read {}: {error}", csr_path.display()))?;
	let ca = read_ca(&dir)?;
	let issued = sign_client_csr(&ca, &csr, tenant, options.cluster_domain())
		.map_err(|error| error.to_string())?;
	write_cert(&out, &issued.cert)?;
	Ok(format!(
		"wrote {} for client-{tenant}.{}",
		out.display(),
		options.cluster_domain()
	))
}

fn issue_storage_command(args: &[String]) -> Result<String, String> {
	let options = Options::parse(
		args,
		&[
			"--ca-cert",
			"--ca-key",
			"--tenant",
			"--out-dir",
			"--node-index",
			"--cluster-domain",
		],
	)?;
	let ca_cert = std::fs::read_to_string(options.required("--ca-cert")?)
		.map_err(|error| format!("read --ca-cert: {error}"))?;
	let ca_key = std::fs::read_to_string(options.required("--ca-key")?)
		.map_err(|error| format!("read --ca-key: {error}"))?;
	let tenant = options.required("--tenant")?;
	let out = PathBuf::from(options.required("--out-dir")?);
	let node_index: u32 = options
		.get("--node-index")
		.unwrap_or("2")
		.parse()
		.map_err(|error| format!("--node-index: {error}"))?;
	let ca = load_ca(&ca_cert, &ca_key).map_err(|error| error.to_string())?;
	let issued = issue_storage(&ca, tenant, options.cluster_domain(), node_index)
		.map_err(|error| error.to_string())?;
	std::fs::create_dir_all(&out).map_err(|error| format!("create {}: {error}", out.display()))?;
	write_cert(&out.join("toron-storage.crt"), &issued.cert)?;
	write_key(
		&out.join("toron-storage.key"),
		issued.key.as_deref().expect("a storage identity has a key"),
	)?;
	Ok(format!(
		"wrote {}/toron-storage.crt and toron-storage.key claiming tenant {tenant} as node-{node_index}.{}",
		out.display(),
		options.cluster_domain()
	))
}

fn read_ca(dir: &Path) -> Result<rcgen::Certificate, String> {
	let cert = std::fs::read_to_string(dir.join("ca.crt"))
		.map_err(|error| format!("read {}/ca.crt: {error}", dir.display()))?;
	let key = std::fs::read_to_string(dir.join("ca.key"))
		.map_err(|error| format!("read {}/ca.key: {error}", dir.display()))?;
	load_ca(&cert, &key).map_err(|error| error.to_string())
}

fn write_cert(path: &Path, pem: &str) -> Result<(), String> {
	write_new(path, pem, 0o644)
}

fn write_key(path: &Path, pem: &str) -> Result<(), String> {
	write_new(path, pem, 0o600)
}

fn write_new(path: &Path, pem: &str, mode: u32) -> Result<(), String> {
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(mode)
		.open(path)
		.map_err(|error| format!("create {}: {error}", path.display()))?;
	file.write_all(pem.as_bytes())
		.and_then(|()| file.flush())
		.map_err(|error| format!("write {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn options_are_strict() {
		let parsed = Options::parse(
			&["--dir".to_owned(), "/tmp/x".to_owned()],
			&["--dir", "--name"],
		)
		.unwrap();
		assert_eq!(parsed.get("--dir"), Some("/tmp/x"));
		assert_eq!(parsed.cluster_domain(), DEFAULT_CLUSTER_DOMAIN);
		assert!(Options::parse(&["--bogus".to_owned(), "x".to_owned()], &["--dir"]).is_err());
		assert!(Options::parse(&["--dir".to_owned()], &["--dir"]).is_err());
		assert!(
			Options::parse(
				&[
					"--dir".to_owned(),
					"a".to_owned(),
					"--dir".to_owned(),
					"b".to_owned()
				],
				&["--dir"]
			)
			.is_err()
		);
	}

	#[test]
	fn the_three_operations_round_trip_without_moving_a_key() {
		let dir = std::env::temp_dir().join(format!("nilket-pki-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		init_ca(&["--dir".to_owned(), dir.display().to_string()]).unwrap();
		issue_listener_command(&[
			"--dir".to_owned(),
			dir.display().to_string(),
			"--worker-id".to_owned(),
			"00000000-0000-0000-0000-000000000042".to_owned(),
		])
		.unwrap();

		// The requester's key stays on the requester's side of the call.
		let requester =
			rcgen::Certificate::from_params(rcgen::CertificateParams::new(Vec::new())).unwrap();
		let csr_path = dir.join("client.csr");
		std::fs::write(&csr_path, requester.serialize_request_pem().unwrap()).unwrap();
		let out = dir.join("client.crt");
		sign_client(&[
			"--dir".to_owned(),
			dir.display().to_string(),
			"--csr".to_owned(),
			csr_path.display().to_string(),
			"--tenant".to_owned(),
			"1".to_owned(),
			"--out".to_owned(),
			out.display().to_string(),
		])
		.unwrap();
		assert!(out.exists());
		assert!(!dir.join("client.key").exists());
		for name in ["ca.key", "listener.key"] {
			use std::os::unix::fs::PermissionsExt;
			let mode = std::fs::metadata(dir.join(name))
				.unwrap()
				.permissions()
				.mode() & 0o777;
			assert_eq!(mode, 0o600, "{name}");
		}
		// Never overwrite what was issued before.
		assert!(init_ca(&["--dir".to_owned(), dir.display().to_string()]).is_err());
		let _ = std::fs::remove_dir_all(&dir);
	}
}
