use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

const RESERVED_MESSAGE_SUFFIX: &str = "is reserved; production execution is not wired in ADR 040";
const DEFAULT_SQL_ENDPOINT: &str = "127.0.0.1:7742";
const AUTHENTICATED_CONNECTION_ERROR_PREFIX: &str =
	"failed to establish authenticated connection to";

struct SqlClientOptions {
	endpoint: SocketAddr,
	tls: Option<SqlClientTlsOptions>,
}

struct SqlClientTlsOptions {
	server_name: String,
	cert_path: PathBuf,
	key_path: PathBuf,
	ca_path: PathBuf,
}

pub fn run<I>(program: &str, args: I) -> ExitCode
where
	I: IntoIterator<Item = String>,
{
	let args: Vec<String> = args.into_iter().collect();
	match args.as_slice() {
		[] => {
			print_usage(program);
			ExitCode::from(2)
		}
		[flag] if flag == "--help" || flag == "-h" => {
			print_usage(program);
			ExitCode::SUCCESS
		}
		[flag] if flag == "--version" || flag == "-V" => {
			println!("toron {}", env!("CARGO_PKG_VERSION"));
			ExitCode::SUCCESS
		}
		[flag] if flag == "--print-example-config" => {
			print!("{}", crate::config::render_example_config());
			ExitCode::SUCCESS
		}
		[flag, path] if flag == "--write-example-config" => {
			match crate::config::write_example_config(Path::new(path)) {
				Ok(()) => {
					eprintln!("toron: wrote starter config to {path}");
					ExitCode::SUCCESS
				}
				Err(error) => {
					eprintln!("failed to write starter config to {path}: {error}");
					ExitCode::from(2)
				}
			}
		}
		[verb, config_path] if verb == "start" => {
			crate::daemon::run_start(PathBuf::from(config_path))
		}
		[verb] if verb == "start" => {
			eprintln!("toron start requires <path/to/toron.toml>");
			ExitCode::from(2)
		}
		[verb, rest @ ..] if verb == "sql" && !rest.is_empty() => run_sql(rest),
		[verb] if verb == "sql" => {
			eprintln!("toron sql requires <statement...>");
			ExitCode::from(2)
		}
		[verb] if verb == "inspect" => reserved("inspect"),
		[verb, _rest @ ..] if verb == "inspect" => reserved("inspect"),
		[unknown, ..] => {
			eprintln!("unknown toron command: {unknown}");
			print_usage(program);
			ExitCode::from(2)
		}
	}
}

fn reserved(verb: &str) -> ExitCode {
	eprintln!("toron {verb} {RESERVED_MESSAGE_SUFFIX}");
	ExitCode::from(2)
}

fn run_sql(args: &[String]) -> ExitCode {
	let (options, statement_args) = match parse_sql_args(args) {
		Ok(parsed) => parsed,
		Err(message) => {
			eprintln!("{message}");
			return ExitCode::from(2);
		}
	};
	if statement_args.is_empty() {
		eprintln!("toron sql requires <statement...>");
		return ExitCode::from(2);
	}
	let statement = statement_args.join(" ");
	let runtime = match tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("failed to initialize Toron runtime: {error}");
			return ExitCode::from(2);
		}
	};
	runtime.block_on(run_sql_async(options, statement))
}

fn parse_sql_args(args: &[String]) -> Result<(SqlClientOptions, &[String]), String> {
	let mut endpoint_flag: Option<SocketAddr> = None;
	let mut server_name_flag: Option<String> = None;
	let mut cert_flag: Option<PathBuf> = None;
	let mut key_flag: Option<PathBuf> = None;
	let mut ca_flag: Option<PathBuf> = None;
	let mut index = 0usize;

	while let Some(arg) = args.get(index) {
		match arg.as_str() {
			"--endpoint" => {
				let value = args
					.get(index + 1)
					.ok_or_else(|| "toron sql --endpoint requires <socket-addr>".to_string())?;
				endpoint_flag = Some(
					value
						.parse::<SocketAddr>()
						.map_err(|error| format!("invalid toron sql endpoint: {error}"))?,
				);
				index += 2;
			}
			"--server-name" => {
				server_name_flag = Some(
					args.get(index + 1)
						.ok_or_else(|| "toron sql --server-name requires <dns-name>".to_string())?
						.clone(),
				);
				index += 2;
			}
			"--cert" => {
				cert_flag =
					Some(PathBuf::from(args.get(index + 1).ok_or_else(|| {
						"toron sql --cert requires <path>".to_string()
					})?));
				index += 2;
			}
			"--key" => {
				key_flag =
					Some(PathBuf::from(args.get(index + 1).ok_or_else(|| {
						"toron sql --key requires <path>".to_string()
					})?));
				index += 2;
			}
			"--ca" => {
				ca_flag =
					Some(PathBuf::from(args.get(index + 1).ok_or_else(|| {
						"toron sql --ca requires <path>".to_string()
					})?));
				index += 2;
			}
			flag if flag.starts_with("--") => {
				return Err(format!("unknown toron sql option: {flag}"));
			}
			_ => break,
		}
	}

	let endpoint = match endpoint_flag {
		Some(endpoint) => endpoint,
		None => match std::env::var("TORON_SQL_ENDPOINT") {
			Ok(value) => value
				.parse::<SocketAddr>()
				.map_err(|error| format!("invalid TORON_SQL_ENDPOINT: {error}"))?,
			Err(_) => DEFAULT_SQL_ENDPOINT
				.parse::<SocketAddr>()
				.expect("compiled default SQL endpoint is valid"),
		},
	};
	let server_name = server_name_flag.or_else(|| std::env::var("TORON_SQL_SERVER_NAME").ok());
	let cert_path = cert_flag.or_else(|| {
		std::env::var("TORON_SQL_CLIENT_CERT")
			.ok()
			.map(PathBuf::from)
	});
	let key_path = key_flag.or_else(|| {
		std::env::var("TORON_SQL_CLIENT_KEY")
			.ok()
			.map(PathBuf::from)
	});
	let ca_path = ca_flag.or_else(|| std::env::var("TORON_SQL_CA").ok().map(PathBuf::from));
	let tls_field_count = [
		server_name.is_some(),
		cert_path.is_some(),
		key_path.is_some(),
		ca_path.is_some(),
	]
	.into_iter()
	.filter(|present| *present)
	.count();
	let tls = if tls_field_count == 0 {
		if !endpoint.ip().is_loopback() {
			return Err(
				"toron sql remote endpoint requires --server-name, --cert, --key, and --ca"
					.to_string(),
			);
		}
		None
	} else if tls_field_count == 4 {
		Some(SqlClientTlsOptions {
			server_name: server_name.expect("checked above"),
			cert_path: cert_path.expect("checked above"),
			key_path: key_path.expect("checked above"),
			ca_path: ca_path.expect("checked above"),
		})
	} else {
		return Err(
			"toron sql TLS options require server name, cert, key, and CA together".to_string(),
		);
	};
	Ok((SqlClientOptions { endpoint, tls }, &args[index..]))
}

async fn run_sql_async(options: SqlClientOptions, statement: String) -> ExitCode {
	let endpoint = options.endpoint;
	let (client, authenticated_server_name) = match options.tls {
		Some(tls) => {
			let client_config = match crate::network::tls::client_config_from_paths(
				&tls.cert_path,
				&tls.key_path,
				&tls.ca_path,
			) {
				Ok(config) => config,
				Err(error) => {
					eprintln!("failed to initialize SQL mTLS client: {error}");
					return ExitCode::from(2);
				}
			};
			let client = match crate::quic::QuicEndpoint::client_authenticated(client_config) {
				Ok(client) => client,
				Err(error) => {
					eprintln!("failed to initialize SQL QUIC client: {error}");
					return ExitCode::from(2);
				}
			};
			(client, Some(tls.server_name))
		}
		None => {
			let client = match crate::quic::QuicEndpoint::client_loopback() {
				Ok(client) => client,
				Err(error) => {
					eprintln!("failed to initialize SQL QUIC client: {error}");
					return ExitCode::from(2);
				}
			};
			(client, None)
		}
	};
	let connect = async {
		match authenticated_server_name.as_deref() {
			Some(server_name) => {
				client
					.connect_stream_with_server_name(endpoint, server_name)
					.await
			}
			None => client.connect_stream(endpoint).await,
		}
	};
	let mut stream = match tokio::time::timeout(std::time::Duration::from_secs(2), connect).await {
		Ok(Ok(stream)) => stream,
		Ok(Err(error)) => {
			if authenticated_server_name.is_some() {
				eprintln!("{AUTHENTICATED_CONNECTION_ERROR_PREFIX} {endpoint}: {error}");
			} else {
				eprintln!("failed to connect to toron sql endpoint {endpoint}: {error}");
			}
			return ExitCode::from(2);
		}
		Err(_) => {
			if authenticated_server_name.is_some() {
				eprintln!(
					"{AUTHENTICATED_CONNECTION_ERROR_PREFIX} {endpoint}: connection timed out"
				);
			} else {
				eprintln!(
					"failed to connect to toron sql endpoint {endpoint}: connection timed out"
				);
			}
			return ExitCode::from(2);
		}
	};
	let query_id = crate::distributed::identity::QueryId::new();
	let request = crate::sql_wire::SqlFrame::execute(query_id, statement, None);
	if let Err(error) = crate::sql_wire::write_frame(&mut stream, &request).await {
		eprintln!("failed to send toron sql request: {error}");
		return ExitCode::from(2);
	}
	if let Err(error) = stream.finish().await {
		eprintln!("failed to finish toron sql request stream: {error}");
		return ExitCode::from(2);
	}

	let mut batches = Vec::new();
	loop {
		let frame = match crate::sql_wire::read_frame(&mut stream).await {
			Ok(frame) => frame,
			Err(error) => {
				eprintln!("failed to read toron sql response: {error}");
				return ExitCode::from(1);
			}
		};
		match frame.message {
			crate::sql_wire::SqlMessage::ResultBatch(batch) => {
				if let Err(error) = decode_sql_batch(&batch.arrow_ipc_bytes, &mut batches) {
					eprintln!("failed to decode toron sql result batch: {error}");
					return ExitCode::from(1);
				}
			}
			crate::sql_wire::SqlMessage::Complete(complete) => {
				if batches.is_empty() {
					if complete.row_count == 0 {
						println!("OK");
					} else {
						println!("OK rows={}", complete.row_count);
					}
				} else if let Err(error) = render_batches(&batches) {
					eprintln!("failed to render toron sql results: {error}");
					return ExitCode::from(1);
				}
				return ExitCode::SUCCESS;
			}
			crate::sql_wire::SqlMessage::Error(error) => {
				eprintln!("toron sql error [{:?}]: {}", error.code, error.message);
				return ExitCode::from(1);
			}
			crate::sql_wire::SqlMessage::Execute(_) | crate::sql_wire::SqlMessage::Cancel(_) => {}
		}
	}
}

fn decode_sql_batch(
	bytes: &[u8],
	batches: &mut Vec<RecordBatch>,
) -> Result<(), arrow::error::ArrowError> {
	let reader = StreamReader::try_new(bytes, None)?;
	for batch in reader {
		batches.push(batch?);
	}
	Ok(())
}

fn render_batches(batches: &[RecordBatch]) -> Result<(), arrow::error::ArrowError> {
	let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
	let rendered = arrow_cast::pretty::pretty_format_batches(batches)?;
	println!("{rendered}");
	println!("({total_rows} rows)");
	Ok(())
}

fn print_usage(program: &str) {
	eprintln!(
		"usage:\n  {program} start <path/to/toron.toml>\n  {program} --print-example-config\n  {program} --write-example-config <path>\n  {program} sql [--endpoint <socket-addr>] [--server-name <dns-name>] [--cert <path>] [--key <path>] [--ca <path>] <statement...>\n  {program} inspect [target]\n\nToron inspect is reserved in ADR 040."
	);
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::net::{IpAddr, Ipv4Addr};
	use std::time::{Duration, SystemTime, UNIX_EPOCH};

	use rcgen::{
		BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
		KeyUsagePurpose,
	};

	static SQL_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
	const SQL_ENV_KEYS: [&str; 5] = [
		"TORON_SQL_ENDPOINT",
		"TORON_SQL_SERVER_NAME",
		"TORON_SQL_CLIENT_CERT",
		"TORON_SQL_CLIENT_KEY",
		"TORON_SQL_CA",
	];

	fn args(values: &[&str]) -> Vec<String> {
		values.iter().map(|value| value.to_string()).collect()
	}

	fn clear_sql_env() {
		for key in SQL_ENV_KEYS {
			unsafe {
				// SAFETY: SQL option tests serialize TORON_SQL_* mutations with SQL_ENV_LOCK.
				std::env::remove_var(key);
			}
		}
	}

	struct CliSqlPki {
		ca_cert: PathBuf,
		wrong_ca_cert: PathBuf,
		daemon_cert: PathBuf,
		daemon_key: PathBuf,
		client_cert: PathBuf,
		client_key: PathBuf,
	}

	impl CliSqlPki {
		fn generate(worker_id: crate::distributed::identity::WorkerId, principal: &str) -> Self {
			let dir = fresh_sql_cli_dir();
			let ca = ca_cert();
			let wrong_ca = ca_cert();
			let daemon_dns = crate::network::tls::canonical_daemon_dns_name(
				&worker_id,
				crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
			);
			let daemon = leaf_cert(
				&daemon_dns,
				&[
					ExtendedKeyUsagePurpose::ServerAuth,
					ExtendedKeyUsagePurpose::ClientAuth,
				],
			);
			let client_dns = crate::network::tls::canonical_client_dns_name(
				principal,
				crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
			);
			let client = leaf_cert(&client_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
			let pki = Self {
				ca_cert: dir.join("ca.pem"),
				wrong_ca_cert: dir.join("wrong-ca.pem"),
				daemon_cert: dir.join("daemon.pem"),
				daemon_key: dir.join("daemon.key"),
				client_cert: dir.join("client.pem"),
				client_key: dir.join("client.key"),
			};
			std::fs::write(&pki.ca_cert, ca.serialize_pem().expect("CA PEM")).expect("write CA");
			std::fs::write(
				&pki.wrong_ca_cert,
				wrong_ca.serialize_pem().expect("wrong CA PEM"),
			)
			.expect("write wrong CA");
			write_signed_cert(&pki.daemon_cert, &pki.daemon_key, &daemon, &ca);
			write_signed_cert(&pki.client_cert, &pki.client_key, &client, &ca);
			pki
		}
	}

	struct MinimalSqlServer {
		endpoint: crate::quic::QuicEndpoint,
		connect_addr: SocketAddr,
	}

	impl MinimalSqlServer {
		fn start(worker_id: crate::distributed::identity::WorkerId, pki: &CliSqlPki) -> Self {
			let tls_config = crate::config::TlsConfig {
				listener_cert: pki.daemon_cert.clone(),
				listener_key: pki.daemon_key.clone(),
				peer_ca: pki.ca_cert.clone(),
				cluster_domain: crate::network::tls::DEFAULT_CLUSTER_DOMAIN.to_string(),
			};
			let policy = crate::network::tls::ListenerPolicy::load(&tls_config, &worker_id)
				.expect("load SQL listener policy");
			let endpoint = crate::quic::QuicEndpoint::bind_authenticated(
				"0.0.0.0:0".parse().expect("SQL bind addr"),
				policy.server_config_for(crate::network::tls::ListenerRole::Sql),
			)
			.expect("bind SQL mTLS server");
			let local_addr = endpoint.local_addr().expect("SQL local addr");
			let connect_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), local_addr.port());
			Self {
				endpoint,
				connect_addr,
			}
		}

		fn spawn_complete_once(&self) -> tokio::task::JoinHandle<()> {
			let endpoint = self.endpoint.clone();
			tokio::spawn(async move {
				let mut stream = endpoint.accept_stream().await.expect("accept SQL stream");
				let frame = crate::sql_wire::read_frame(&mut stream)
					.await
					.expect("read SQL request");
				match frame.message {
					crate::sql_wire::SqlMessage::Execute(_) => {}
					other => panic!("unexpected SQL request frame: {other:?}"),
				}
				let response = crate::sql_wire::SqlFrame::response(
					frame.query_id,
					crate::sql_wire::SqlMessage::Complete(crate::sql_wire::Complete {
						row_count: 0,
					}),
				);
				crate::sql_wire::write_frame(&mut stream, &response)
					.await
					.expect("write SQL complete");
				let _ = stream.finish().await;
				tokio::time::sleep(Duration::from_millis(25)).await;
			})
		}
	}

	fn fresh_sql_cli_dir() -> PathBuf {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system time after epoch")
			.as_nanos();
		let dir =
			std::env::temp_dir().join(format!("toron-sql-cli-{}-{nanos}", std::process::id()));
		std::fs::create_dir_all(&dir).expect("create temp dir");
		dir
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
			.push(DnType::CommonName, "toron-sql-cli-test-ca");
		Certificate::from_params(params).expect("CA cert")
	}

	fn leaf_cert(dns_name: &str, extended_key_usages: &[ExtendedKeyUsagePurpose]) -> Certificate {
		let mut params = CertificateParams::new(vec![dns_name.to_string()]);
		params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
		params.extended_key_usages = extended_key_usages.to_vec();
		params.distinguished_name.push(DnType::CommonName, dns_name);
		Certificate::from_params(params).expect("leaf cert")
	}

	fn write_signed_cert(cert_path: &Path, key_path: &Path, cert: &Certificate, ca: &Certificate) {
		std::fs::write(
			cert_path,
			cert.serialize_pem_with_signer(ca).expect("signed cert PEM"),
		)
		.expect("write cert");
		std::fs::write(key_path, cert.serialize_private_key_pem()).expect("write key");
	}

	async fn assert_successful_sql_client(options: SqlClientOptions, statement: String) {
		let exit = run_sql_async(options, statement).await;
		assert_eq!(exit, ExitCode::SUCCESS);
	}

	#[test]
	fn remote_sql_endpoint_requires_complete_tls_flags() {
		let _guard = SQL_ENV_LOCK.lock().expect("lock SQL env");
		clear_sql_env();

		let missing_args = args(&["--endpoint", "10.0.0.1:7742", "SELECT", "1"]);
		let missing = match parse_sql_args(&missing_args) {
			Ok(_) => panic!("remote endpoint without TLS must fail"),
			Err(error) => error,
		};
		assert!(missing.contains("--server-name, --cert, --key, and --ca"));

		let partial_args = args(&[
			"--endpoint",
			"10.0.0.1:7742",
			"--server-name",
			"toron-00000000-0000-0000-0000-000000000044.cluster.local",
			"SELECT",
			"1",
		]);
		let partial = match parse_sql_args(&partial_args) {
			Ok(_) => panic!("partial TLS flags must fail"),
			Err(error) => error,
		};
		assert!(partial.contains("server name, cert, key, and CA together"));
	}

	#[test]
	fn remote_sql_endpoint_accepts_complete_tls_flags_and_preserves_statement() {
		let _guard = SQL_ENV_LOCK.lock().expect("lock SQL env");
		clear_sql_env();

		let cli_args = args(&[
			"--endpoint",
			"10.0.0.1:7742",
			"--server-name",
			"toron-00000000-0000-0000-0000-000000000044.cluster.local",
			"--cert",
			"client.pem",
			"--key",
			"client.key",
			"--ca",
			"ca.pem",
			"SELECT",
			"1",
		]);
		let parsed = parse_sql_args(&cli_args).expect("complete TLS flags parse");

		assert_eq!(parsed.0.endpoint, "10.0.0.1:7742".parse().unwrap());
		let tls = parsed.0.tls.expect("TLS options");
		assert_eq!(
			tls.server_name,
			"toron-00000000-0000-0000-0000-000000000044.cluster.local"
		);
		assert_eq!(tls.cert_path, PathBuf::from("client.pem"));
		assert_eq!(tls.key_path, PathBuf::from("client.key"));
		assert_eq!(tls.ca_path, PathBuf::from("ca.pem"));
		assert_eq!(parsed.1, &cli_args[10..]);
	}

	#[test]
	fn remote_sql_endpoint_accepts_complete_tls_env() {
		let _guard = SQL_ENV_LOCK.lock().expect("lock SQL env");
		clear_sql_env();
		unsafe {
			// SAFETY: SQL option tests serialize TORON_SQL_* mutations with SQL_ENV_LOCK.
			std::env::set_var("TORON_SQL_ENDPOINT", "10.0.0.2:7742");
			std::env::set_var(
				"TORON_SQL_SERVER_NAME",
				"toron-00000000-0000-0000-0000-000000000045.cluster.local",
			);
			std::env::set_var("TORON_SQL_CLIENT_CERT", "env-client.pem");
			std::env::set_var("TORON_SQL_CLIENT_KEY", "env-client.key");
			std::env::set_var("TORON_SQL_CA", "env-ca.pem");
		}

		let cli_args = args(&["SELECT", "1"]);
		let parsed = parse_sql_args(&cli_args).expect("complete TLS env parse");

		assert_eq!(parsed.0.endpoint, "10.0.0.2:7742".parse().unwrap());
		let tls = parsed.0.tls.expect("TLS options");
		assert_eq!(
			tls.server_name,
			"toron-00000000-0000-0000-0000-000000000045.cluster.local"
		);
		assert_eq!(tls.cert_path, PathBuf::from("env-client.pem"));
		assert_eq!(tls.key_path, PathBuf::from("env-client.key"));
		assert_eq!(tls.ca_path, PathBuf::from("env-ca.pem"));
		assert_eq!(parsed.1, &cli_args[..]);

		clear_sql_env();
	}

	#[test]
	fn remote_sql_endpoint_flags_override_tls_env() {
		let _guard = SQL_ENV_LOCK.lock().expect("lock SQL env");
		clear_sql_env();
		unsafe {
			// SAFETY: SQL option tests serialize TORON_SQL_* mutations with SQL_ENV_LOCK.
			std::env::set_var("TORON_SQL_ENDPOINT", "10.0.0.2:7742");
			std::env::set_var(
				"TORON_SQL_SERVER_NAME",
				"toron-00000000-0000-0000-0000-000000000045.cluster.local",
			);
			std::env::set_var("TORON_SQL_CLIENT_CERT", "env-client.pem");
			std::env::set_var("TORON_SQL_CLIENT_KEY", "env-client.key");
			std::env::set_var("TORON_SQL_CA", "env-ca.pem");
		}
		let cli_args = args(&[
			"--endpoint",
			"10.0.0.3:7742",
			"--server-name",
			"toron-00000000-0000-0000-0000-000000000046.cluster.local",
			"--cert",
			"flag-client.pem",
			"--key",
			"flag-client.key",
			"--ca",
			"flag-ca.pem",
			"SELECT",
			"1",
		]);

		let parsed = parse_sql_args(&cli_args).expect("flag TLS options parse");

		assert_eq!(parsed.0.endpoint, "10.0.0.3:7742".parse().unwrap());
		let tls = parsed.0.tls.expect("TLS options");
		assert_eq!(
			tls.server_name,
			"toron-00000000-0000-0000-0000-000000000046.cluster.local"
		);
		assert_eq!(tls.cert_path, PathBuf::from("flag-client.pem"));
		assert_eq!(tls.key_path, PathBuf::from("flag-client.key"));
		assert_eq!(tls.ca_path, PathBuf::from("flag-ca.pem"));
		assert_eq!(parsed.1, &cli_args[10..]);

		clear_sql_env();
	}

	#[tokio::test]
	async fn remote_sql_endpoint_dials_with_mtls_flags_and_env() {
		let worker_id = crate::distributed::identity::WorkerId::from_u128(54);
		let pki = CliSqlPki::generate(worker_id, "alice");
		let server_name = crate::network::tls::canonical_daemon_dns_name(
			&worker_id,
			crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
		);

		let flags_server = MinimalSqlServer::start(worker_id, &pki);
		let flags_handle = flags_server.spawn_complete_once();
		let (flags_options, flags_statement) = {
			let _guard = SQL_ENV_LOCK.lock().expect("lock SQL env");
			clear_sql_env();
			let cli_args = args(&[
				"--endpoint",
				&flags_server.connect_addr.to_string(),
				"--server-name",
				&server_name,
				"--cert",
				&pki.client_cert.display().to_string(),
				"--key",
				&pki.client_key.display().to_string(),
				"--ca",
				&pki.ca_cert.display().to_string(),
				"SELECT",
				"1",
			]);
			let parsed = parse_sql_args(&cli_args).expect("flag TLS options parse");
			clear_sql_env();
			(parsed.0, parsed.1.join(" "))
		};
		assert_successful_sql_client(flags_options, flags_statement).await;
		tokio::time::timeout(Duration::from_secs(2), flags_handle)
			.await
			.expect("flag SQL server completed")
			.expect("flag SQL server task");

		let env_server = MinimalSqlServer::start(worker_id, &pki);
		let env_handle = env_server.spawn_complete_once();
		let (env_options, env_statement) = {
			let _guard = SQL_ENV_LOCK.lock().expect("lock SQL env");
			clear_sql_env();
			unsafe {
				// SAFETY: SQL option tests serialize TORON_SQL_* mutations with SQL_ENV_LOCK.
				std::env::set_var("TORON_SQL_ENDPOINT", env_server.connect_addr.to_string());
				std::env::set_var("TORON_SQL_SERVER_NAME", &server_name);
				std::env::set_var(
					"TORON_SQL_CLIENT_CERT",
					pki.client_cert.display().to_string(),
				);
				std::env::set_var("TORON_SQL_CLIENT_KEY", pki.client_key.display().to_string());
				std::env::set_var("TORON_SQL_CA", pki.ca_cert.display().to_string());
			}
			let cli_args = args(&["SELECT", "1"]);
			let parsed = parse_sql_args(&cli_args).expect("env TLS options parse");
			clear_sql_env();
			(parsed.0, parsed.1.join(" "))
		};
		assert_successful_sql_client(env_options, env_statement).await;
		tokio::time::timeout(Duration::from_secs(2), env_handle)
			.await
			.expect("env SQL server completed")
			.expect("env SQL server task");

		let override_server = MinimalSqlServer::start(worker_id, &pki);
		let override_handle = override_server.spawn_complete_once();
		let (override_options, override_statement) = {
			let _guard = SQL_ENV_LOCK.lock().expect("lock SQL env");
			clear_sql_env();
			unsafe {
				// SAFETY: SQL option tests serialize TORON_SQL_* mutations with SQL_ENV_LOCK.
				std::env::set_var("TORON_SQL_ENDPOINT", "127.0.0.1:1");
				std::env::set_var(
					"TORON_SQL_SERVER_NAME",
					"toron-00000000-0000-0000-0000-000000000001.cluster.local",
				);
				std::env::set_var("TORON_SQL_CLIENT_CERT", "env-client.pem");
				std::env::set_var("TORON_SQL_CLIENT_KEY", "env-client.key");
				std::env::set_var("TORON_SQL_CA", "env-ca.pem");
			}
			let cli_args = args(&[
				"--endpoint",
				&override_server.connect_addr.to_string(),
				"--server-name",
				&server_name,
				"--cert",
				&pki.client_cert.display().to_string(),
				"--key",
				&pki.client_key.display().to_string(),
				"--ca",
				&pki.ca_cert.display().to_string(),
				"SELECT",
				"1",
			]);
			let parsed = parse_sql_args(&cli_args).expect("override TLS options parse");
			clear_sql_env();
			(parsed.0, parsed.1.join(" "))
		};
		assert_successful_sql_client(override_options, override_statement).await;
		tokio::time::timeout(Duration::from_secs(2), override_handle)
			.await
			.expect("override SQL server completed")
			.expect("override SQL server task");
	}

	#[tokio::test]
	async fn remote_sql_endpoint_unknown_ca_returns_connection_failure_exit() {
		let worker_id = crate::distributed::identity::WorkerId::from_u128(55);
		let pki = CliSqlPki::generate(worker_id, "alice");
		let server = MinimalSqlServer::start(worker_id, &pki);
		let server_name = crate::network::tls::canonical_daemon_dns_name(
			&worker_id,
			crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
		);
		let options = SqlClientOptions {
			endpoint: server.connect_addr,
			tls: Some(SqlClientTlsOptions {
				server_name,
				cert_path: pki.client_cert.clone(),
				key_path: pki.client_key.clone(),
				ca_path: pki.wrong_ca_cert.clone(),
			}),
		};

		let exit = run_sql_async(options, "SELECT 1".to_string()).await;

		assert_eq!(exit, ExitCode::from(2));
		server.endpoint.close();
	}
}
