use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

const DEFAULT_SQL_ENDPOINT: &str = "127.0.0.1:7742";
const DEFAULT_INSPECT_ENDPOINT: &str = "127.0.0.1:7781";
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const AUTHENTICATED_CONNECTION_ERROR_PREFIX: &str =
	"failed to establish authenticated connection to";

struct SqlClientOptions {
	endpoint: SocketAddr,
	tls: Option<SqlClientTlsOptions>,
	session_timezone: String,
}

struct SqlClientTlsOptions {
	server_name: String,
	cert_path: PathBuf,
	key_path: PathBuf,
	ca_path: PathBuf,
}

struct InspectOptions {
	endpoint: SocketAddr,
	tls: InspectTlsOptions,
	json: bool,
}

struct InspectTlsOptions {
	server_name: String,
	cert_path: PathBuf,
	key_path: PathBuf,
	ca_path: PathBuf,
}

struct InspectTlsConfig {
	server_name: String,
	rustls_config: rustls::ClientConfig,
}

struct InspectReadyResponse {
	body: String,
	snapshot: crate::network::observability::ReadinessSnapshot,
}

#[derive(Debug)]
enum InspectError {
	Connectivity {
		action: &'static str,
		source: io::Error,
	},
	UnexpectedStatus(u16),
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
		[verb, rest @ ..] if verb == "inspect" => run_inspect(rest),
		[unknown, ..] => {
			eprintln!("unknown toron command: {unknown}");
			print_usage(program);
			ExitCode::from(2)
		}
	}
}

fn run_inspect(args: &[String]) -> ExitCode {
	let options = match parse_inspect_args(args) {
		Ok(options) => options,
		Err(message) => {
			eprintln!("{message}");
			return ExitCode::from(2);
		}
	};
	let runtime = match tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("failed to initialize Toron runtime: {error}");
			return ExitCode::from(2);
		}
	};
	runtime.block_on(run_inspect_async(options))
}

fn parse_inspect_args(args: &[String]) -> Result<InspectOptions, String> {
	let mut endpoint_flag: Option<SocketAddr> = None;
	let mut server_name_flag: Option<String> = None;
	let mut cert_flag: Option<PathBuf> = None;
	let mut key_flag: Option<PathBuf> = None;
	let mut ca_flag: Option<PathBuf> = None;
	let mut json = false;
	let mut index = 0usize;

	while let Some(arg) = args.get(index) {
		match arg.as_str() {
			"--endpoint" => {
				let value = args
					.get(index + 1)
					.ok_or_else(|| "toron inspect --endpoint requires <socket-addr>".to_string())?;
				endpoint_flag = Some(
					value
						.parse::<SocketAddr>()
						.map_err(|error| format!("invalid toron inspect endpoint: {error}"))?,
				);
				index += 2;
			}
			"--server-name" => {
				server_name_flag = Some(
					args.get(index + 1)
						.ok_or_else(|| {
							"toron inspect --server-name requires <dns-name>".to_string()
						})?
						.clone(),
				);
				index += 2;
			}
			"--cert" => {
				cert_flag =
					Some(PathBuf::from(args.get(index + 1).ok_or_else(|| {
						"toron inspect --cert requires <path>".to_string()
					})?));
				index += 2;
			}
			"--key" => {
				key_flag =
					Some(PathBuf::from(args.get(index + 1).ok_or_else(|| {
						"toron inspect --key requires <path>".to_string()
					})?));
				index += 2;
			}
			"--ca" => {
				ca_flag =
					Some(PathBuf::from(args.get(index + 1).ok_or_else(|| {
						"toron inspect --ca requires <path>".to_string()
					})?));
				index += 2;
			}
			"--json" => {
				json = true;
				index += 1;
			}
			flag if flag.starts_with("--") => {
				return Err(format!("unknown toron inspect option: {flag}"));
			}
			_ => return Err("toron inspect does not accept positional arguments".to_string()),
		}
	}

	let endpoint = match endpoint_flag {
		Some(endpoint) => endpoint,
		None => match std::env::var("TORON_INSPECT_ENDPOINT") {
			Ok(value) => value
				.parse::<SocketAddr>()
				.map_err(|error| format!("invalid TORON_INSPECT_ENDPOINT: {error}"))?,
			Err(_) => DEFAULT_INSPECT_ENDPOINT
				.parse::<SocketAddr>()
				.expect("compiled default inspect endpoint is valid"),
		},
	};
	let server_name = server_name_flag.or_else(|| std::env::var("TORON_INSPECT_SERVER_NAME").ok());
	let cert_path = cert_flag.or_else(|| {
		std::env::var("TORON_INSPECT_CLIENT_CERT")
			.ok()
			.map(PathBuf::from)
	});
	let key_path = key_flag.or_else(|| {
		std::env::var("TORON_INSPECT_CLIENT_KEY")
			.ok()
			.map(PathBuf::from)
	});
	let ca_path = ca_flag.or_else(|| std::env::var("TORON_INSPECT_CA").ok().map(PathBuf::from));
	if server_name.is_none() || cert_path.is_none() || key_path.is_none() || ca_path.is_none() {
		return Err("toron inspect requires --server-name, --cert, --key, and --ca".to_string());
	}
	Ok(InspectOptions {
		endpoint,
		tls: InspectTlsOptions {
			server_name: server_name.expect("checked above"),
			cert_path: cert_path.expect("checked above"),
			key_path: key_path.expect("checked above"),
			ca_path: ca_path.expect("checked above"),
		},
		json,
	})
}

async fn run_inspect_async(options: InspectOptions) -> ExitCode {
	match fetch_ready(&options).await {
		Ok(response) => {
			if options.json {
				print!("{}", response.body);
				if !response.body.ends_with('\n') {
					println!();
				}
			} else {
				print!("{}", render_readiness_text(&response.snapshot));
			}
			if response.snapshot.ready {
				ExitCode::SUCCESS
			} else {
				ExitCode::from(1)
			}
		}
		Err(InspectError::Connectivity { action, source }) => {
			if action.contains("within timeout") {
				eprintln!("toron inspect: failed to {action}");
			} else {
				eprintln!("toron inspect: failed to {action}: {source}");
			}
			ExitCode::from(2)
		}
		Err(InspectError::UnexpectedStatus(status_code)) => {
			eprintln!(
				"toron inspect: failed to read /ready response: unexpected HTTP status {status_code}"
			);
			ExitCode::from(2)
		}
	}
}

async fn fetch_ready(options: &InspectOptions) -> Result<InspectReadyResponse, InspectError> {
	fetch_ready_with_tcp_connect(
		options,
		TcpStream::connect(options.endpoint),
		CONNECT_TIMEOUT,
	)
	.await
}

async fn fetch_ready_with_tcp_connect<F>(
	options: &InspectOptions,
	connect: F,
	connect_timeout: Duration,
) -> Result<InspectReadyResponse, InspectError>
where
	F: Future<Output = io::Result<TcpStream>>,
{
	let tls = build_inspect_tls(&options.tls)?;
	let tcp = match tokio::time::timeout(connect_timeout, connect).await {
		Ok(Ok(stream)) => stream,
		Ok(Err(source)) => {
			return Err(InspectError::Connectivity {
				action: "connect to endpoint",
				source,
			});
		}
		Err(_) => {
			return Err(InspectError::Connectivity {
				action: "connect to endpoint within timeout",
				source: io::Error::new(io::ErrorKind::TimedOut, "5s"),
			});
		}
	};
	let connector = TlsConnector::from(Arc::new(tls.rustls_config));
	let server_name = ServerName::try_from(tls.server_name.clone()).map_err(|error| {
		InspectError::Connectivity {
			action: "complete TLS handshake",
			source: io::Error::new(io::ErrorKind::InvalidInput, error),
		}
	})?;
	let mut stream =
		connector
			.connect(server_name, tcp)
			.await
			.map_err(|error| InspectError::Connectivity {
				action: "complete TLS handshake",
				source: io::Error::other(error),
			})?;
	crate::network::observability_http::write_request(
		&mut stream,
		"GET",
		crate::network::observability::READY_PATH,
		&options.endpoint.to_string(),
	)
	.await
	.map_err(|source| InspectError::Connectivity {
		action: "write request",
		source,
	})?;
	let response = tokio::time::timeout(
		crate::network::observability_http::READ_REQUEST_TIMEOUT,
		crate::network::observability_http::read_response(&mut stream),
	)
	.await
	.map_err(|_| InspectError::Connectivity {
		action: "receive response within timeout",
		source: io::Error::new(io::ErrorKind::TimedOut, "5s"),
	})?
	.map_err(|source| InspectError::Connectivity {
		action: "parse HTTP response",
		source,
	})?;
	if response.status_code != 200 && response.status_code != 503 {
		return Err(InspectError::UnexpectedStatus(response.status_code));
	}
	let snapshot =
		serde_json::from_slice::<crate::network::observability::ReadinessSnapshot>(&response.body)
			.map_err(|error| InspectError::Connectivity {
				action: "parse readiness snapshot",
				source: io::Error::new(io::ErrorKind::InvalidData, error),
			})?;
	let body = String::from_utf8(response.body).map_err(|error| InspectError::Connectivity {
		action: "parse readiness snapshot",
		source: io::Error::new(io::ErrorKind::InvalidData, error),
	})?;
	Ok(InspectReadyResponse { body, snapshot })
}

fn build_inspect_tls(options: &InspectTlsOptions) -> Result<InspectTlsConfig, InspectError> {
	let rustls_config = crate::network::tls::http_client_config_from_paths(
		&options.cert_path,
		&options.key_path,
		&options.ca_path,
	)
	.map_err(|error| InspectError::Connectivity {
		action: "initialize mTLS client",
		source: io::Error::new(io::ErrorKind::InvalidInput, error),
	})?;
	Ok(InspectTlsConfig {
		server_name: options.server_name.clone(),
		rustls_config,
	})
}

fn render_readiness_text(snapshot: &crate::network::observability::ReadinessSnapshot) -> String {
	let degraded_reason = snapshot
		.degraded_reason
		.as_ref()
		.map(|reason| serde_json::to_string(reason).expect("serialize degraded reason"))
		.unwrap_or_else(|| "null".to_string());
	format!(
		"ready: {}\nworker_listener_bound: {}\nsql_listener_bound: {}\nobservability_listeners_bound: {}\ndescriptors_loaded: {}\nstatic_workers_configured: {}\nuptime_seconds: {}\ndegraded_reason: {}\n",
		snapshot.ready,
		snapshot.worker_listener_bound,
		snapshot.sql_listener_bound,
		snapshot.observability_listeners_bound,
		snapshot.descriptors_loaded,
		snapshot.static_workers_configured,
		snapshot.uptime_seconds,
		degraded_reason
	)
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
	let mut timezone_flag: Option<String> = None;
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
			"--timezone" => {
				timezone_flag = Some(
					args.get(index + 1)
						.ok_or_else(|| "toron sql --timezone requires <iana-timezone>".to_string())?
						.clone(),
				);
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
	let session_timezone = timezone_flag.unwrap_or_else(detect_local_timezone);
	Ok((
		SqlClientOptions {
			endpoint,
			tls,
			session_timezone,
		},
		&args[index..],
	))
}

fn detect_local_timezone() -> String {
	iana_time_zone::get_timezone().unwrap_or_else(|_| "UTC".to_string())
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
	let request = crate::sql_wire::SqlFrame::execute_with_timezone(
		query_id,
		statement,
		None,
		options.session_timezone,
	);
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
	println!("{}", format_batches_for_display(batches)?);
	Ok(())
}

fn format_batches_for_display(batches: &[RecordBatch]) -> Result<String, arrow::error::ArrowError> {
	let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
	let rendered = arrow_cast::pretty::pretty_format_batches(batches)?;
	Ok(format!("{rendered}\n({total_rows} rows)"))
}

fn print_usage(program: &str) {
	eprintln!(
		"usage:\n  {program} start <path/to/toron.toml>\n  {program} --print-example-config\n  {program} --write-example-config <path>\n  {program} sql [--endpoint <socket-addr>] [--timezone <iana-timezone>] [--server-name <dns-name>] [--cert <path>] [--key <path>] [--ca <path>] <statement...>\n  {program} inspect [--endpoint <socket-addr>] --server-name <dns-name> --cert <path> --key <path> --ca <path> [--json]"
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
	static INSPECT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
	const SQL_ENV_KEYS: [&str; 5] = [
		"TORON_SQL_ENDPOINT",
		"TORON_SQL_SERVER_NAME",
		"TORON_SQL_CLIENT_CERT",
		"TORON_SQL_CLIENT_KEY",
		"TORON_SQL_CA",
	];
	const INSPECT_ENV_KEYS: [&str; 5] = [
		"TORON_INSPECT_ENDPOINT",
		"TORON_INSPECT_SERVER_NAME",
		"TORON_INSPECT_CLIENT_CERT",
		"TORON_INSPECT_CLIENT_KEY",
		"TORON_INSPECT_CA",
	];

	fn args(values: &[&str]) -> Vec<String> {
		values.iter().map(|value| value.to_string()).collect()
	}

	#[test]
	fn date32_select_output_renders_iso_date() {
		let schema = Arc::new(arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new("event_date", arrow::datatypes::DataType::Date32, true),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(arrow::array::Date32Array::from(vec![
				Some(19_723),
				Some(19_782),
				None,
			]))],
		)
		.expect("Date32 batch");

		let rendered = format_batches_for_display(&[batch]).expect("render Date32 batch");

		assert!(rendered.contains("2024-01-01"));
		assert!(rendered.contains("2024-02-29"));
		assert!(rendered.contains("(3 rows)"));
		assert!(!rendered.contains("19723"));
		assert!(!rendered.contains("19782"));
	}

	#[test]
	fn timestamp_select_output_renders_session_timezone() {
		let schema = Arc::new(arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new(
				"observed_at",
				arrow::datatypes::DataType::Timestamp(
					arrow::datatypes::TimeUnit::Millisecond,
					Some("America/Chicago".into()),
				),
				true,
			),
		]));
		let utc_value = crate::timestamp::parse_explicit_timestamptz(
			"2024-01-01T06:00:00Z",
			arrow::datatypes::TimeUnit::Millisecond,
		)
		.expect("timestamp");
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(
				arrow::array::TimestampMillisecondArray::from(vec![Some(utc_value)])
					.with_timezone("America/Chicago"),
			)],
		)
		.expect("timestamp batch");

		let rendered = format_batches_for_display(&[batch]).expect("render timestamp batch");

		assert!(rendered.contains("2024-01-01T00:00:00"));
		assert!(!rendered.contains("2024-01-01T06:00:00"));
		assert!(rendered.contains("(1 rows)"));
	}

	#[test]
	fn cli_sql_timezone_flag_stamps_execute_request() {
		let _guard = SQL_ENV_LOCK.lock().expect("lock SQL env");
		clear_sql_env();
		let cli_args = args(&[
			"--timezone",
			"America/Chicago",
			"SELECT",
			"TIMESTAMPTZ",
			"'2024-01-01T00:00:00Z'",
		]);

		let (options, statement) = parse_sql_args(&cli_args).expect("timezone flag parses");

		assert_eq!(options.session_timezone, "America/Chicago");
		assert_eq!(statement, &cli_args[2..]);
		let frame = crate::sql_wire::SqlFrame::execute_with_timezone(
			crate::distributed::identity::QueryId::from_u128(150),
			statement.join(" "),
			None,
			options.session_timezone,
		);
		let crate::sql_wire::SqlMessage::Execute(request) = frame.message else {
			panic!("expected execute request");
		};
		assert_eq!(request.session_timezone, "America/Chicago");
		assert_eq!(
			request.statement,
			"SELECT TIMESTAMPTZ '2024-01-01T00:00:00Z'"
		);
	}

	#[test]
	fn cli_sql_auto_detects_timezone_with_utc_fallback() {
		let _guard = SQL_ENV_LOCK.lock().expect("lock SQL env");
		clear_sql_env();
		let detected = detect_local_timezone();
		assert!(!detected.is_empty());

		let cli_args = args(&["SELECT", "1"]);
		let (options, statement) = parse_sql_args(&cli_args).expect("default timezone parses");

		assert_eq!(options.session_timezone, detected);
		assert_eq!(statement, &cli_args[..]);
	}

	fn clear_sql_env() {
		for key in SQL_ENV_KEYS {
			unsafe {
				// SAFETY: SQL option tests serialize TORON_SQL_* mutations with SQL_ENV_LOCK.
				std::env::remove_var(key);
			}
		}
	}

	fn clear_inspect_env() {
		for key in INSPECT_ENV_KEYS {
			unsafe {
				// SAFETY: Inspect option tests serialize TORON_INSPECT_* mutations with INSPECT_ENV_LOCK.
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

	struct CliInspectPki {
		ca_cert: PathBuf,
		daemon_cert: PathBuf,
		daemon_key: PathBuf,
		monitor_cert: PathBuf,
		monitor_key: PathBuf,
		client_cert: PathBuf,
		client_key: PathBuf,
	}

	impl CliInspectPki {
		fn generate(worker_id: crate::distributed::identity::WorkerId, principal: &str) -> Self {
			let dir = fresh_inspect_cli_dir();
			let ca = ca_cert();
			let daemon_dns = crate::network::tls::canonical_daemon_dns_name(
				&worker_id,
				crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
			);
			let daemon = leaf_cert(&daemon_dns, &[ExtendedKeyUsagePurpose::ServerAuth]);
			let monitor_dns = crate::network::tls::canonical_monitor_dns_name(
				principal,
				crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
			);
			let monitor = leaf_cert(&monitor_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
			let client_dns = crate::network::tls::canonical_client_dns_name(
				principal,
				crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
			);
			let client = leaf_cert(&client_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
			let pki = Self {
				ca_cert: dir.join("ca.pem"),
				daemon_cert: dir.join("daemon.pem"),
				daemon_key: dir.join("daemon.key"),
				monitor_cert: dir.join("monitor.pem"),
				monitor_key: dir.join("monitor.key"),
				client_cert: dir.join("client.pem"),
				client_key: dir.join("client.key"),
			};
			std::fs::write(&pki.ca_cert, ca.serialize_pem().expect("CA PEM")).expect("write CA");
			write_signed_cert(&pki.daemon_cert, &pki.daemon_key, &daemon, &ca);
			write_signed_cert(&pki.monitor_cert, &pki.monitor_key, &monitor, &ca);
			write_signed_cert(&pki.client_cert, &pki.client_key, &client, &ca);
			pki
		}
	}

	struct MinimalReadyServer {
		listener: tokio::net::TcpListener,
		acceptor: tokio_rustls::TlsAcceptor,
		connect_addr: SocketAddr,
	}

	impl MinimalReadyServer {
		async fn start(
			worker_id: crate::distributed::identity::WorkerId,
			pki: &CliInspectPki,
		) -> Self {
			let tls_config = crate::config::TlsConfig {
				listener_cert: pki.daemon_cert.clone(),
				listener_key: pki.daemon_key.clone(),
				peer_ca: pki.ca_cert.clone(),
				cluster_domain: crate::network::tls::DEFAULT_CLUSTER_DOMAIN.to_string(),
			};
			let policy = crate::network::tls::ListenerPolicy::load(&tls_config, &worker_id)
				.expect("load observability listener policy");
			let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
				.await
				.expect("bind ready listener");
			let local_addr = listener.local_addr().expect("ready local addr");
			let connect_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), local_addr.port());
			Self {
				listener,
				acceptor: tokio_rustls::TlsAcceptor::from(
					policy
						.rustls_server_config_for(crate::network::tls::ListenerRole::Observability),
				),
				connect_addr,
			}
		}

		fn spawn_response_once(
			self,
			status: &'static str,
			body: String,
		) -> tokio::task::JoinHandle<io::Result<Vec<u8>>> {
			tokio::spawn(async move {
				let (stream, _) = self.listener.accept().await?;
				let mut stream = self
					.acceptor
					.accept(stream)
					.await
					.map_err(io::Error::other)?;
				let request = crate::network::observability_http::read_request(&mut stream).await?;
				crate::network::observability_http::write_response(
					&mut stream,
					status,
					"application/json",
					&body,
				)
				.await?;
				Ok(request)
			})
		}
	}

	fn fresh_inspect_cli_dir() -> PathBuf {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system time after epoch")
			.as_nanos();
		let dir =
			std::env::temp_dir().join(format!("toron-inspectcli-{}-{nanos}", std::process::id()));
		std::fs::create_dir_all(&dir).expect("create temp dir");
		dir
	}

	fn readiness_snapshot(
		ready: bool,
		degraded_reason: Option<&str>,
	) -> crate::network::observability::ReadinessSnapshot {
		crate::network::observability::ReadinessSnapshot {
			ready,
			worker_listener_bound: true,
			sql_listener_bound: true,
			observability_listeners_bound: true,
			descriptors_loaded: 12,
			static_workers_configured: 2,
			uptime_seconds: 47,
			degraded_reason: degraded_reason.map(str::to_string),
		}
	}

	fn inspect_options(
		endpoint: SocketAddr,
		server_name: String,
		cert_path: PathBuf,
		key_path: PathBuf,
		ca_path: PathBuf,
		json: bool,
	) -> InspectOptions {
		InspectOptions {
			endpoint,
			tls: InspectTlsOptions {
				server_name,
				cert_path,
				key_path,
				ca_path,
			},
			json,
		}
	}

	async fn assert_successful_sql_client(options: SqlClientOptions, statement: String) {
		let exit = run_sql_async(options, statement).await;
		assert_eq!(exit, ExitCode::SUCCESS);
	}

	#[test]
	fn inspect_requires_complete_tls_options_even_on_loopback() {
		let _guard = INSPECT_ENV_LOCK.lock().expect("lock inspect env");
		clear_inspect_env();

		let missing = match parse_inspect_args(&[]) {
			Ok(_) => panic!("inspect without TLS must fail"),
			Err(error) => error,
		};
		assert_eq!(
			missing,
			"toron inspect requires --server-name, --cert, --key, and --ca"
		);

		let partial_args = args(&[
			"--endpoint",
			"127.0.0.1:7781",
			"--server-name",
			"toron-00000000-0000-0000-0000-000000000046.cluster.local",
		]);
		let partial = match parse_inspect_args(&partial_args) {
			Ok(_) => panic!("partial inspect TLS must fail"),
			Err(error) => error,
		};
		assert_eq!(
			partial,
			"toron inspect requires --server-name, --cert, --key, and --ca"
		);

		let positional = match parse_inspect_args(&args(&["127.0.0.1:7781"])) {
			Ok(_) => panic!("positional inspect endpoint must fail"),
			Err(error) => error,
		};
		assert_eq!(
			positional,
			"toron inspect does not accept positional arguments"
		);
	}

	#[test]
	fn inspect_endpoint_env_and_flags_precedence() {
		let _guard = INSPECT_ENV_LOCK.lock().expect("lock inspect env");
		clear_inspect_env();
		unsafe {
			// SAFETY: Inspect option tests serialize TORON_INSPECT_* mutations with INSPECT_ENV_LOCK.
			std::env::set_var("TORON_INSPECT_ENDPOINT", "127.0.0.1:7782");
			std::env::set_var(
				"TORON_INSPECT_SERVER_NAME",
				"toron-00000000-0000-0000-0000-000000000046.cluster.local",
			);
			std::env::set_var("TORON_INSPECT_CLIENT_CERT", "env-monitor.pem");
			std::env::set_var("TORON_INSPECT_CLIENT_KEY", "env-monitor.key");
			std::env::set_var("TORON_INSPECT_CA", "env-ca.pem");
		}

		let env_options = parse_inspect_args(&args(&["--json"])).expect("env inspect options");
		assert_eq!(env_options.endpoint, "127.0.0.1:7782".parse().unwrap());
		assert!(env_options.json);
		assert_eq!(
			env_options.tls.server_name,
			"toron-00000000-0000-0000-0000-000000000046.cluster.local"
		);
		assert_eq!(env_options.tls.cert_path, PathBuf::from("env-monitor.pem"));
		assert_eq!(env_options.tls.key_path, PathBuf::from("env-monitor.key"));
		assert_eq!(env_options.tls.ca_path, PathBuf::from("env-ca.pem"));

		let flag_options = parse_inspect_args(&args(&[
			"--endpoint",
			"127.0.0.1:7783",
			"--server-name",
			"toron-00000000-0000-0000-0000-000000000047.cluster.local",
			"--cert",
			"flag-monitor.pem",
			"--key",
			"flag-monitor.key",
			"--ca",
			"flag-ca.pem",
		]))
		.expect("flag inspect options");
		assert_eq!(flag_options.endpoint, "127.0.0.1:7783".parse().unwrap());
		assert!(!flag_options.json);
		assert_eq!(
			flag_options.tls.server_name,
			"toron-00000000-0000-0000-0000-000000000047.cluster.local"
		);
		assert_eq!(
			flag_options.tls.cert_path,
			PathBuf::from("flag-monitor.pem")
		);
		assert_eq!(flag_options.tls.key_path, PathBuf::from("flag-monitor.key"));
		assert_eq!(flag_options.tls.ca_path, PathBuf::from("flag-ca.pem"));

		clear_inspect_env();
	}

	#[test]
	fn inspect_text_renderer_is_stable() {
		let ready = readiness_snapshot(true, None);
		assert_eq!(
			render_readiness_text(&ready),
			"ready: true\nworker_listener_bound: true\nsql_listener_bound: true\nobservability_listeners_bound: true\ndescriptors_loaded: 12\nstatic_workers_configured: 2\nuptime_seconds: 47\ndegraded_reason: null\n"
		);

		let degraded = readiness_snapshot(false, Some("warming\n\"quoted\""));
		assert_eq!(
			render_readiness_text(&degraded),
			"ready: false\nworker_listener_bound: true\nsql_listener_bound: true\nobservability_listeners_bound: true\ndescriptors_loaded: 12\nstatic_workers_configured: 2\nuptime_seconds: 47\ndegraded_reason: \"warming\\n\\\"quoted\\\"\"\n"
		);
	}

	#[test]
	fn inspect_json_parser_ignores_unknown_fields() {
		let parsed = serde_json::from_str::<crate::network::observability::ReadinessSnapshot>(
			"{\"ready\":true,\"worker_listener_bound\":true,\"sql_listener_bound\":true,\"observability_listeners_bound\":true,\"descriptors_loaded\":0,\"static_workers_configured\":0,\"uptime_seconds\":1,\"degraded_reason\":null,\"future_field\":\"ignored\"}",
		)
		.expect("forward-compatible readiness snapshot");

		assert!(parsed.ready);
		assert_eq!(parsed.degraded_reason, None);
	}

	#[tokio::test]
	async fn inspect_fetches_ready_from_flag_endpoint_and_preserves_json_body() {
		let worker_id = crate::distributed::identity::WorkerId::from_u128(46);
		let pki = CliInspectPki::generate(worker_id, "operator");
		let server = MinimalReadyServer::start(worker_id, &pki).await;
		let body = readiness_snapshot(true, None).to_json();
		let endpoint = server.connect_addr;
		let handle = server.spawn_response_once("200 OK", body.clone());
		let server_name = crate::network::tls::canonical_daemon_dns_name(
			&worker_id,
			crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
		);
		let options = inspect_options(
			endpoint,
			server_name,
			pki.monitor_cert.clone(),
			pki.monitor_key.clone(),
			pki.ca_cert.clone(),
			true,
		);

		let response = fetch_ready(&options).await.expect("ready response");
		let request = handle.await.expect("ready server task").expect("request");
		let request = String::from_utf8(request).expect("request utf8");

		assert_eq!(response.body, body);
		assert_eq!(response.snapshot, readiness_snapshot(true, None));
		assert!(request.starts_with("GET /ready HTTP/1.1\r\n"));
		assert!(request.contains(&format!("Host: {}\r\n", options.endpoint)));
	}

	#[tokio::test]
	async fn toron_inspect_preserves_ready_fetch_post_extraction() {
		let worker_id = crate::distributed::identity::WorkerId::from_u128(52);
		let pki = CliInspectPki::generate(worker_id, "operator");
		let server = MinimalReadyServer::start(worker_id, &pki).await;
		let body = readiness_snapshot(true, None).to_json();
		let endpoint = server.connect_addr;
		let handle = server.spawn_response_once("200 OK", body.clone());
		let server_name = crate::network::tls::canonical_daemon_dns_name(
			&worker_id,
			crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
		);
		let options = inspect_options(
			endpoint,
			server_name,
			pki.monitor_cert.clone(),
			pki.monitor_key.clone(),
			pki.ca_cert.clone(),
			true,
		);

		let response = fetch_ready(&options).await.expect("ready response");
		let request = handle.await.expect("ready server task").expect("request");
		let request = String::from_utf8(request).expect("request utf8");

		assert_eq!(response.body, body);
		assert_eq!(response.snapshot, readiness_snapshot(true, None));
		assert!(request.starts_with("GET /ready HTTP/1.1\r\n"));
		assert!(request.contains(&format!("Host: {}\r\n", options.endpoint)));
	}

	#[tokio::test]
	async fn inspect_exit_zero_on_ready_one_on_degraded() {
		let worker_id = crate::distributed::identity::WorkerId::from_u128(47);
		let pki = CliInspectPki::generate(worker_id, "operator");
		let server_name = crate::network::tls::canonical_daemon_dns_name(
			&worker_id,
			crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
		);

		let ready_server = MinimalReadyServer::start(worker_id, &pki).await;
		let ready_endpoint = ready_server.connect_addr;
		let ready_handle =
			ready_server.spawn_response_once("200 OK", readiness_snapshot(true, None).to_json());
		let ready_exit = run_inspect_async(inspect_options(
			ready_endpoint,
			server_name.clone(),
			pki.monitor_cert.clone(),
			pki.monitor_key.clone(),
			pki.ca_cert.clone(),
			false,
		))
		.await;
		assert_eq!(ready_exit, ExitCode::SUCCESS);
		ready_handle
			.await
			.expect("ready server task")
			.expect("ready request");

		let degraded_server = MinimalReadyServer::start(worker_id, &pki).await;
		let degraded_endpoint = degraded_server.connect_addr;
		let degraded_handle = degraded_server.spawn_response_once(
			"503 Service Unavailable",
			readiness_snapshot(false, Some("warming")).to_json(),
		);
		let degraded_exit = run_inspect_async(inspect_options(
			degraded_endpoint,
			server_name,
			pki.monitor_cert.clone(),
			pki.monitor_key.clone(),
			pki.ca_cert.clone(),
			false,
		))
		.await;
		assert_eq!(degraded_exit, ExitCode::from(1));
		degraded_handle
			.await
			.expect("degraded server task")
			.expect("degraded request");
	}

	#[tokio::test]
	async fn inspect_unexpected_status_returns_exit_two() {
		let worker_id = crate::distributed::identity::WorkerId::from_u128(48);
		let pki = CliInspectPki::generate(worker_id, "operator");
		let server = MinimalReadyServer::start(worker_id, &pki).await;
		let endpoint = server.connect_addr;
		let handle = server
			.spawn_response_once("418 I'm a Teapot", readiness_snapshot(true, None).to_json());
		let server_name = crate::network::tls::canonical_daemon_dns_name(
			&worker_id,
			crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
		);

		let exit = run_inspect_async(inspect_options(
			endpoint,
			server_name,
			pki.monitor_cert.clone(),
			pki.monitor_key.clone(),
			pki.ca_cert.clone(),
			false,
		))
		.await;

		assert_eq!(exit, ExitCode::from(2));
		handle
			.await
			.expect("teapot server task")
			.expect("teapot request");
	}

	#[tokio::test]
	async fn inspect_rejects_peer_without_monitor_san_client_cert() {
		let worker_id = crate::distributed::identity::WorkerId::from_u128(49);
		let pki = CliInspectPki::generate(worker_id, "operator");
		let server = MinimalReadyServer::start(worker_id, &pki).await;
		let endpoint = server.connect_addr;
		let handle = server.spawn_response_once("200 OK", readiness_snapshot(true, None).to_json());
		let server_name = crate::network::tls::canonical_daemon_dns_name(
			&worker_id,
			crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
		);
		let options = inspect_options(
			endpoint,
			server_name,
			pki.client_cert.clone(),
			pki.client_key.clone(),
			pki.ca_cert.clone(),
			false,
		);

		let error = match fetch_ready(&options).await {
			Ok(_) => panic!("client- SAN must not be admitted to observability listener"),
			Err(error) => error,
		};

		assert!(matches!(error, InspectError::Connectivity { .. }));
		assert!(
			tokio::time::timeout(Duration::from_secs(2), handle)
				.await
				.expect("server observed rejected client")
				.expect("server task")
				.is_err()
		);
	}

	#[tokio::test]
	async fn inspect_reports_connection_failure_exit_two() {
		let worker_id = crate::distributed::identity::WorkerId::from_u128(50);
		let pki = CliInspectPki::generate(worker_id, "operator");
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
			.await
			.expect("bind unused port");
		let endpoint = listener.local_addr().expect("unused local addr");
		drop(listener);
		let server_name = crate::network::tls::canonical_daemon_dns_name(
			&worker_id,
			crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
		);

		let exit = run_inspect_async(inspect_options(
			endpoint,
			server_name,
			pki.monitor_cert.clone(),
			pki.monitor_key.clone(),
			pki.ca_cert.clone(),
			false,
		))
		.await;

		assert_eq!(exit, ExitCode::from(2));
	}

	#[tokio::test]
	async fn inspect_connect_timeout_maps_to_stable_error() {
		let worker_id = crate::distributed::identity::WorkerId::from_u128(51);
		let pki = CliInspectPki::generate(worker_id, "operator");
		let server_name = crate::network::tls::canonical_daemon_dns_name(
			&worker_id,
			crate::network::tls::DEFAULT_CLUSTER_DOMAIN,
		);
		let options = inspect_options(
			"127.0.0.1:7781".parse().expect("endpoint"),
			server_name,
			pki.monitor_cert.clone(),
			pki.monitor_key.clone(),
			pki.ca_cert.clone(),
			false,
		);

		let error = match fetch_ready_with_tcp_connect(
			&options,
			std::future::pending::<io::Result<TcpStream>>(),
			Duration::from_millis(1),
		)
		.await
		{
			Ok(_) => panic!("pending connect must time out"),
			Err(error) => error,
		};

		match error {
			InspectError::Connectivity { action, source } => {
				assert_eq!(action, "connect to endpoint within timeout");
				assert_eq!(source.kind(), io::ErrorKind::TimedOut);
			}
			InspectError::UnexpectedStatus(status) => {
				panic!("unexpected status error from connect timeout: {status}")
			}
		}
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
		let pki = CliSqlPki::generate(worker_id, "1");
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
		let pki = CliSqlPki::generate(worker_id, "1");
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
			session_timezone: "UTC".to_string(),
		};

		let exit = run_sql_async(options, "SELECT 1".to_string()).await;

		assert_eq!(exit, ExitCode::from(2));
		server.endpoint.close();
	}
}
