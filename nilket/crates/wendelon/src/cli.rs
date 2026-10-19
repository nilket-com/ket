//! Wendelon role command-line runner.
//!
//! This module owns Wendelon's branch grammar for both the legacy
//! `wendelon` binary and the unified `nilket wendelon` branch.

mod audit_trust;

use std::future::Future;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use serde_json::{Map, Value};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::audit::verify_local_audit_evidence;
use crate::config::{load_node_bootstrap_config, render_example_config, write_example_config};
use crate::metrics::observability::{self, ObservabilityHandles};
use crate::metrics::telemetry;
use crate::network::{ActorTrustRoots, TlsMaterial, rustls_server_config_with_peer_roots};
use crate::node::{NodeBootstrapConfig, WendelonNode};
use ::observability::http as observability_http;

mod index;
// Offline Root2 compromise audit; the root2 tests drive its executor over real WAL fixtures.
pub(crate) mod root2_audit;
mod tenant;

const DEFAULT_INSPECT_ENDPOINT: &str = "127.0.0.1:7432";
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const READ_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const INSPECT_TLS_REQUIRED: &str =
	"wendelon inspect requires --server-name, --cert, --key, and --ca";

#[derive(Debug, Clone, PartialEq, Eq)]
struct InspectOptions {
	endpoint: SocketAddr,
	server_name: String,
	cert_path: PathBuf,
	key_path: PathBuf,
	ca_path: PathBuf,
	json: bool,
}

/// Authenticated Wendelon `/ready` client shared by `wendelon inspect`, the
/// Nilket local supervisor, and live integration fixtures (TORON-532).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessProbe {
	endpoint: SocketAddr,
	server_name: String,
	cert_path: PathBuf,
	key_path: PathBuf,
	ca_path: PathBuf,
}

impl ReadinessProbe {
	pub fn new(
		endpoint: SocketAddr,
		server_name: impl Into<String>,
		cert_path: impl Into<PathBuf>,
		key_path: impl Into<PathBuf>,
		ca_path: impl Into<PathBuf>,
	) -> Self {
		Self {
			endpoint,
			server_name: server_name.into(),
			cert_path: cert_path.into(),
			key_path: key_path.into(),
			ca_path: ca_path.into(),
		}
	}

	pub async fn check(&self) -> Result<bool, String> {
		fetch_ready(&self.inspect_options())
			.await
			.map(|response| response.view.ready)
			.map_err(|error| inspect_error_message(&error))
	}

	pub fn check_blocking(&self) -> Result<bool, String> {
		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.map_err(|error| format!("Wendelon readiness runtime: {error}"))?;
		runtime.block_on(self.check())
	}

	fn inspect_options(&self) -> InspectOptions {
		InspectOptions {
			endpoint: self.endpoint,
			server_name: self.server_name.clone(),
			cert_path: self.cert_path.clone(),
			key_path: self.key_path.clone(),
			ca_path: self.ca_path.clone(),
			json: false,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadinessView {
	node_id: Value,
	epoch: Value,
	leader_id: Value,
	cluster_members: Value,
	drain_state: Value,
	audit_materialization_lag_entries: Value,
	degraded_reason: Value,
	fips_mode: Option<Value>,
	fips_self_test_last_ts: Option<Value>,
	ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InspectReadyResponse {
	body: Vec<u8>,
	view: ReadinessView,
}

#[derive(Debug)]
enum InspectError {
	Connect(io::Error),
	ConnectTimeout,
	TlsConfig(io::Error),
	TlsHandshake(io::Error),
	WriteRequest(io::Error),
	ResponseTimeout,
	ParseHttp(io::Error),
	UnexpectedStatus(u16),
	ParseSnapshot(String),
}

pub fn run<I>(program: &str, args: I) -> ExitCode
where
	I: IntoIterator<Item = String>,
{
	let args: Vec<String> = args.into_iter().collect();
	match args.as_slice() {
		[flag] if flag == "--help" || flag == "-h" => {
			print_usage(program);
			ExitCode::SUCCESS
		}
		[flag] if flag == "--version" || flag == "-V" => {
			println!("wendelon {}", env!("CARGO_PKG_VERSION"));
			ExitCode::SUCCESS
		}
		[flag] if flag == "--print-example-config" => {
			print!("{}", render_example_config());
			ExitCode::SUCCESS
		}
		[cmd, subcmd, audit_path] if cmd == "audit" && subcmd == "verify" => {
			verify_audit(audit_path, None)
		}
		[cmd, subcmd, audit_path, flag, trust_path]
			if cmd == "audit" && subcmd == "verify" && flag == "--trusted-audit-roots" =>
		{
			verify_audit(audit_path, Some(trust_path))
		}
		[cmd, subcmd, rest @ ..] if cmd == "audit" && subcmd == "root2" => root2_audit::run(rest),
		[cmd, rest @ ..] if cmd == "inspect" => run_inspect(rest),
		[cmd, rest @ ..] if cmd == "index" => index::run(rest),
		[cmd, rest @ ..] if cmd == "tenant" => tenant::run(rest),
		[flag, path] if flag == "--write-example-config" => match write_example_config(path) {
			Ok(()) => {
				eprintln!("wendelon: wrote starter config to {path}");
				ExitCode::SUCCESS
			}
			Err(e) => {
				eprintln!("failed to write starter config to {path}: {e}");
				ExitCode::from(2)
			}
		},
		[config_path] => run_daemon(PathBuf::from(config_path)),
		_ => {
			print_usage(program);
			ExitCode::from(2)
		}
	}
}

fn print_usage(program: &str) {
	if program == "wendelon" {
		eprintln!("This binary is deprecated; use `nilket wendelon` instead.");
	}
	eprintln!(
		"usage:\n  {program} <path/to/wendelon.toml>\n  {program} inspect [--endpoint <addr>] [--server-name <name>] [--cert <path>] [--key <path>] [--ca <path>] [--json]\n  {program} index create --bootstrap <node-id>@<addr> --cert <path> --key <path> --ca <path> --tenant <id> --namespace <name> --table <name> --name <name> --column <name> [--id <uuid>]\n  {program} index drop --bootstrap <node-id>@<addr> --cert <path> --key <path> --ca <path> --tenant <id> --id <uuid>\n  {program} index status --bootstrap <node-id>@<addr> --cert <path> --key <path> --ca <path> --tenant <id> [--json]\n  {program} tenant create --bootstrap <node-id>@<addr> --cert <path> --key <path> --ca <path> --tenant <id> [--quota-bytes <n>] [--replication-factor <n>] [--json]\n  {program} tenant get --bootstrap <node-id>@<addr> --cert <path> --key <path> --ca <path> --tenant <id> [--json]\n  {program} tenant list --bootstrap <node-id>@<addr> --cert <path> --key <path> --ca <path> [--json]\n  {program} audit verify <path/to/audit.ndjson> [--trusted-audit-roots <file>]\n  {program} audit root2 <data-dir> --trusted-manifest <file> (--role <admin|governance|recovery> --epoch <n>)... | --key <file> [--suspect-only]\n  {program} --print-example-config\n  {program} --write-example-config <path/to/wendelon.toml>"
	);
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
		Err(err) => {
			eprintln!("failed to initialize Wendelon runtime: {err}");
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
				let value = args.get(index + 1).ok_or_else(|| {
					"wendelon inspect --endpoint requires <socket-addr>".to_string()
				})?;
				endpoint_flag = Some(
					value
						.parse::<SocketAddr>()
						.map_err(|error| format!("invalid wendelon inspect endpoint: {error}"))?,
				);
				index += 2;
			}
			"--server-name" => {
				server_name_flag = Some(
					args.get(index + 1)
						.ok_or_else(|| {
							"wendelon inspect --server-name requires <dns-name>".to_string()
						})?
						.clone(),
				);
				index += 2;
			}
			"--cert" => {
				cert_flag =
					Some(PathBuf::from(args.get(index + 1).ok_or_else(|| {
						"wendelon inspect --cert requires <path>".to_string()
					})?));
				index += 2;
			}
			"--key" => {
				key_flag =
					Some(PathBuf::from(args.get(index + 1).ok_or_else(|| {
						"wendelon inspect --key requires <path>".to_string()
					})?));
				index += 2;
			}
			"--ca" => {
				ca_flag =
					Some(PathBuf::from(args.get(index + 1).ok_or_else(|| {
						"wendelon inspect --ca requires <path>".to_string()
					})?));
				index += 2;
			}
			"--json" => {
				json = true;
				index += 1;
			}
			flag if flag.starts_with("--") => {
				return Err(format!("unknown wendelon inspect option: {flag}"));
			}
			_ => return Err("wendelon inspect does not accept positional arguments".to_string()),
		}
	}

	let endpoint = match endpoint_flag {
		Some(endpoint) => endpoint,
		None => match std::env::var("WENDELON_INSPECT_ENDPOINT") {
			Ok(value) => value
				.parse::<SocketAddr>()
				.map_err(|error| format!("invalid WENDELON_INSPECT_ENDPOINT: {error}"))?,
			Err(_) => DEFAULT_INSPECT_ENDPOINT
				.parse::<SocketAddr>()
				.expect("compiled default inspect endpoint is valid"),
		},
	};
	let server_name =
		server_name_flag.or_else(|| std::env::var("WENDELON_INSPECT_SERVER_NAME").ok());
	let cert_path = cert_flag.or_else(|| {
		std::env::var("WENDELON_INSPECT_CLIENT_CERT")
			.ok()
			.map(PathBuf::from)
	});
	let key_path = key_flag.or_else(|| {
		std::env::var("WENDELON_INSPECT_CLIENT_KEY")
			.ok()
			.map(PathBuf::from)
	});
	let ca_path = ca_flag.or_else(|| std::env::var("WENDELON_INSPECT_CA").ok().map(PathBuf::from));
	if server_name.is_none() || cert_path.is_none() || key_path.is_none() || ca_path.is_none() {
		return Err(INSPECT_TLS_REQUIRED.to_string());
	}
	Ok(InspectOptions {
		endpoint,
		server_name: server_name.expect("checked above"),
		cert_path: cert_path.expect("checked above"),
		key_path: key_path.expect("checked above"),
		ca_path: ca_path.expect("checked above"),
		json,
	})
}

async fn run_inspect_async(options: InspectOptions) -> ExitCode {
	let probe = ReadinessProbe::new(
		options.endpoint,
		options.server_name.clone(),
		options.cert_path.clone(),
		options.key_path.clone(),
		options.ca_path.clone(),
	);
	match fetch_ready(&probe.inspect_options()).await {
		Ok(response) => {
			if let Err(err) =
				io::stdout().write_all(&render_inspect_output(&response, options.json))
			{
				eprintln!("wendelon inspect: failed to write output: {err}");
				return ExitCode::from(2);
			}
			if response.view.ready {
				ExitCode::SUCCESS
			} else {
				ExitCode::from(1)
			}
		}
		Err(error) => {
			eprintln!("{}", inspect_error_message(&error));
			ExitCode::from(2)
		}
	}
}

fn inspect_error_message(error: &InspectError) -> String {
	match error {
		InspectError::Connect(source) => {
			format!("wendelon inspect: failed to connect to endpoint: {source}")
		}
		InspectError::ConnectTimeout => {
			"wendelon inspect: failed to connect to endpoint within timeout".to_string()
		}
		InspectError::TlsConfig(source) => {
			format!("wendelon inspect: failed to initialize mTLS client: {source}")
		}
		InspectError::TlsHandshake(source) => {
			format!("wendelon inspect: failed to complete TLS handshake: {source}")
		}
		InspectError::WriteRequest(source) => {
			format!("wendelon inspect: failed to write request: {source}")
		}
		InspectError::ResponseTimeout => {
			"wendelon inspect: failed to receive response within timeout".to_string()
		}
		InspectError::ParseHttp(source) => {
			format!("wendelon inspect: failed to parse HTTP response: {source}")
		}
		InspectError::UnexpectedStatus(status_code) => {
			format!(
				"wendelon inspect: failed to read /ready response: unexpected HTTP status {status_code}"
			)
		}
		InspectError::ParseSnapshot(details) => {
			format!("wendelon inspect: failed to parse readiness snapshot: {details}")
		}
	}
}

async fn fetch_ready(options: &InspectOptions) -> Result<InspectReadyResponse, InspectError> {
	fetch_ready_with_tcp_connect(
		options,
		TcpStream::connect(options.endpoint),
		CONNECT_TIMEOUT,
		READ_RESPONSE_TIMEOUT,
	)
	.await
}

async fn fetch_ready_with_tcp_connect<F>(
	options: &InspectOptions,
	connect: F,
	connect_timeout: Duration,
	read_timeout: Duration,
) -> Result<InspectReadyResponse, InspectError>
where
	F: Future<Output = io::Result<TcpStream>>,
{
	let material = TlsMaterial::load(&options.cert_path, &options.key_path, &options.ca_path)
		.map_err(InspectError::TlsConfig)?;
	let client_config = crate::network::tls::rustls_client_config_with_alpn(&material, Vec::new())
		.map_err(InspectError::TlsConfig)?;
	let tcp = match tokio::time::timeout(connect_timeout, connect).await {
		Ok(Ok(stream)) => stream,
		Ok(Err(source)) => return Err(InspectError::Connect(source)),
		Err(_) => return Err(InspectError::ConnectTimeout),
	};
	let server_name = ServerName::try_from(options.server_name.clone()).map_err(|error| {
		InspectError::TlsConfig(io::Error::new(io::ErrorKind::InvalidInput, error))
	})?;
	let connector = TlsConnector::from(Arc::new(client_config));
	let mut stream = connector
		.connect(server_name, tcp)
		.await
		.map_err(|error| InspectError::TlsHandshake(io::Error::other(error)))?;
	observability_http::write_request(
		&mut stream,
		"GET",
		crate::metrics::observability::READY_PATH,
		&options.endpoint.to_string(),
	)
	.await
	.map_err(InspectError::WriteRequest)?;
	let response =
		tokio::time::timeout(read_timeout, observability_http::read_response(&mut stream))
			.await
			.map_err(|_| InspectError::ResponseTimeout)?
			.map_err(InspectError::ParseHttp)?;
	if response.status_code != 200 && response.status_code != 503 {
		return Err(InspectError::UnexpectedStatus(response.status_code));
	}
	let view = parse_readiness_view(&response.body)?;
	Ok(InspectReadyResponse {
		body: response.body,
		view,
	})
}

fn parse_readiness_view(body: &[u8]) -> Result<ReadinessView, InspectError> {
	let value = serde_json::from_slice::<Value>(body)
		.map_err(|error| InspectError::ParseSnapshot(error.to_string()))?;
	let object = value
		.as_object()
		.ok_or_else(|| InspectError::ParseSnapshot("expected JSON object".to_string()))?;
	let node_id = required_string_value(object, "node_id")?;
	let epoch = required_u64_value(object, "epoch")?;
	let leader_id = required_nullable_string_value(object, "leader_id")?;
	let cluster_members = required_string_array_value(object, "cluster_members")?;
	let drain_state = required_string_value(object, "drain_state")?;
	let audit_materialization_lag_entries =
		required_u64_value(object, "audit_materialization_lag_entries")?;
	let degraded_reason = required_nullable_string_value(object, "degraded_reason")?;
	let fips_mode = optional_bool_value(object, "fips_mode")?;
	let fips_self_test_last_ts = optional_nullable_u64_value(object, "fips_self_test_last_ts")?;
	let audit_lag = audit_materialization_lag_entries
		.as_u64()
		.expect("required_u64_value returns a JSON u64");
	let retention_violated = retention_violation_value(object)?;
	let ready = degraded_reason.is_null() && audit_lag == 0 && !retention_violated;
	Ok(ReadinessView {
		node_id,
		epoch,
		leader_id,
		cluster_members,
		drain_state,
		audit_materialization_lag_entries,
		degraded_reason,
		fips_mode,
		fips_self_test_last_ts,
		ready,
	})
}

fn required_value<'a>(
	object: &'a Map<String, Value>,
	field: &str,
) -> Result<&'a Value, InspectError> {
	object
		.get(field)
		.ok_or_else(|| InspectError::ParseSnapshot(format!("missing required field {field}")))
}

fn required_string_value(object: &Map<String, Value>, field: &str) -> Result<Value, InspectError> {
	let value = required_value(object, field)?;
	if value.is_string() {
		Ok(value.clone())
	} else {
		Err(InspectError::ParseSnapshot(format!(
			"field {field} must be a string"
		)))
	}
}

fn required_nullable_string_value(
	object: &Map<String, Value>,
	field: &str,
) -> Result<Value, InspectError> {
	let value = required_value(object, field)?;
	if value.is_null() || value.is_string() {
		Ok(value.clone())
	} else {
		Err(InspectError::ParseSnapshot(format!(
			"field {field} must be a string or null"
		)))
	}
}

fn required_u64_value(object: &Map<String, Value>, field: &str) -> Result<Value, InspectError> {
	let value = required_value(object, field)?;
	if value.as_u64().is_some() {
		Ok(value.clone())
	} else {
		Err(InspectError::ParseSnapshot(format!(
			"field {field} must be an unsigned integer"
		)))
	}
}

fn required_string_array_value(
	object: &Map<String, Value>,
	field: &str,
) -> Result<Value, InspectError> {
	let value = required_value(object, field)?;
	let Some(values) = value.as_array() else {
		return Err(InspectError::ParseSnapshot(format!(
			"field {field} must be an array"
		)));
	};
	if values.iter().all(Value::is_string) {
		Ok(value.clone())
	} else {
		Err(InspectError::ParseSnapshot(format!(
			"field {field} must contain only strings"
		)))
	}
}

/// A server that predates Control retention reporting omits the key; that is
/// not a violation. A present key must carry a null or string `violation`.
fn retention_violation_value(object: &Map<String, Value>) -> Result<bool, InspectError> {
	let Some(retention) = object.get("control_retention") else {
		return Ok(false);
	};
	match retention.get("violation") {
		Some(Value::Null) => Ok(false),
		Some(Value::String(_)) => Ok(true),
		_ => Err(InspectError::ParseSnapshot(
			"field control_retention.violation must be null or a string".to_string(),
		)),
	}
}

fn optional_bool_value(
	object: &Map<String, Value>,
	field: &str,
) -> Result<Option<Value>, InspectError> {
	let Some(value) = object.get(field) else {
		return Ok(None);
	};
	if value.is_boolean() {
		Ok(Some(value.clone()))
	} else {
		Err(InspectError::ParseSnapshot(format!(
			"field {field} must be a boolean"
		)))
	}
}

fn optional_nullable_u64_value(
	object: &Map<String, Value>,
	field: &str,
) -> Result<Option<Value>, InspectError> {
	let Some(value) = object.get(field) else {
		return Ok(None);
	};
	if value.is_null() || value.as_u64().is_some() {
		Ok(Some(value.clone()))
	} else {
		Err(InspectError::ParseSnapshot(format!(
			"field {field} must be an unsigned integer or null"
		)))
	}
}

fn render_inspect_output(response: &InspectReadyResponse, json: bool) -> Vec<u8> {
	if json {
		let mut out = response.body.clone();
		out.push(b'\n');
		return out;
	}
	render_readiness_text(&response.view).into_bytes()
}

fn render_readiness_text(view: &ReadinessView) -> String {
	let mut out = String::new();
	push_json_line(&mut out, "node_id", &view.node_id);
	push_json_line(&mut out, "epoch", &view.epoch);
	push_json_line(&mut out, "leader_id", &view.leader_id);
	push_json_line(&mut out, "cluster_members", &view.cluster_members);
	push_json_line(&mut out, "drain_state", &view.drain_state);
	push_json_line(
		&mut out,
		"audit_materialization_lag_entries",
		&view.audit_materialization_lag_entries,
	);
	push_json_line(&mut out, "degraded_reason", &view.degraded_reason);
	if let Some(fips_mode) = &view.fips_mode {
		push_json_line(&mut out, "fips_mode", fips_mode);
	}
	if let Some(fips_self_test_last_ts) = &view.fips_self_test_last_ts {
		push_json_line(&mut out, "fips_self_test_last_ts", fips_self_test_last_ts);
	}
	out
}

fn push_json_line(out: &mut String, field: &str, value: &Value) {
	out.push_str(field);
	out.push_str(": ");
	out.push_str(&serde_json::to_string(value).expect("compact JSON value"));
	out.push('\n');
}

fn verify_audit(audit_path: &str, trust_path: Option<&str>) -> ExitCode {
	let trust = match trust_path
		.map(|path| audit_trust::load(std::path::Path::new(path)))
		.transpose()
	{
		Ok(trust) => trust,
		Err(err) => {
			eprintln!("{err}");
			return ExitCode::from(2);
		}
	};
	match verify_local_audit_evidence(audit_path, trust.as_ref()) {
		Ok(report) => match serde_json::to_string_pretty(&report) {
			Ok(json) => {
				print!("{json}");
				print!("\n");
				if report.ok() {
					ExitCode::SUCCESS
				} else {
					ExitCode::from(1)
				}
			}
			Err(err) => {
				eprintln!("failed to serialize audit verification report: {err}");
				ExitCode::from(2)
			}
		},
		Err(err) => {
			eprintln!("audit verify failed for {audit_path}: {err}");
			ExitCode::from(2)
		}
	}
}

fn run_daemon(config_path: PathBuf) -> ExitCode {
	let runtime = match tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime,
		Err(err) => {
			eprintln!("failed to initialize Wendelon runtime: {err}");
			return ExitCode::from(2);
		}
	};
	let exit = runtime.block_on(start_daemon(config_path));
	// Bound the runtime teardown too: dropping a runtime waits for every
	// blocking task, which could otherwise hang the exit after shutdown.
	runtime.shutdown_timeout(std::time::Duration::from_secs(10));
	if let Some(watchdog) = exit.watchdog {
		watchdog.disarm();
	}
	exit.code
}

async fn start_daemon(config_path: PathBuf) -> DaemonExit {
	let cfg: NodeBootstrapConfig = match load_node_bootstrap_config(&config_path) {
		Ok(cfg) => cfg,
		Err(e) => {
			eprintln!("failed to load {}: {e}", config_path.display());
			return DaemonExit::now(ExitCode::from(2));
		}
	};
	let transport_tls = cfg.transport_tls.clone();
	let observability_cfg = cfg.observability.clone();
	let telemetry = match telemetry::install_global(
		observability_cfg
			.as_ref()
			.and_then(|observability| observability.traces.as_ref()),
		cfg.node_id,
	) {
		Ok(handle) => handle,
		Err(e) => {
			eprintln!("failed to initialize tracing subscriber: {e}");
			return DaemonExit::now(ExitCode::from(2));
		}
	};
	let shutdown_deadline =
		crate::shutdown_watchdog::daemon_shutdown_deadline(cfg.storage_shutdown_timeout);
	let node = match WendelonNode::bootstrap(cfg).await {
		Ok(n) => n,
		Err(e) => {
			eprintln!("bootstrap failed: {e}");
			return DaemonExit::now(ExitCode::from(1));
		}
	};
	eprintln!("wendelon: bootstrapped node {}", node.local_node.id);

	let observability = match (
		transport_tls.as_ref(),
		observability_cfg
			.as_ref()
			.and_then(|obs| obs.listeners.clone()),
	) {
		(Some(tls), Some(listeners)) => {
			let material = match TlsMaterial::load(&tls.cert_path, &tls.key_path, &tls.node_ca_path)
			{
				Ok(material) => material,
				Err(e) => {
					eprintln!("failed to load observability TLS material: {e}");
					return stop_after_failed_start(node, shutdown_deadline, ExitCode::from(2))
						.await;
				}
			};
			let trust_roots = match ActorTrustRoots::load(&tls.node_ca_path, &tls.operator_ca_path)
			{
				Ok(roots) => Arc::new(roots),
				Err(e) => {
					eprintln!("failed to load observability trust roots: {e}");
					return stop_after_failed_start(node, shutdown_deadline, ExitCode::from(2))
						.await;
				}
			};
			let server_config = match rustls_server_config_with_peer_roots(
				&material,
				trust_roots.combined_roots(),
				Vec::new(),
			) {
				Ok(material) => material,
				Err(e) => {
					eprintln!("failed to build observability TLS server config: {e}");
					return stop_after_failed_start(node, shutdown_deadline, ExitCode::from(2))
						.await;
				}
			};
			let reporter = Arc::new(node.readiness_reporter());
			match observability::spawn(
				listeners,
				TlsAcceptor::from(Arc::new(server_config)),
				reporter,
				node.local_node.id,
				&tls.cluster_domain,
				trust_roots,
				node.audit_sink.clone(),
				Some(node.state_machine.subscribe_revocation_snapshot()),
			)
			.await
			{
				Ok(handles) => {
					eprintln!(
						"wendelon: health listening on {}",
						handles.health.local_addr
					);
					eprintln!("wendelon: ready listening on {}", handles.ready.local_addr);
					eprintln!(
						"wendelon: metrics listening on {}",
						handles.metrics.local_addr
					);
					Some(handles)
				}
				Err(e) => {
					eprintln!("failed to start observability listeners: {e}");
					return stop_after_failed_start(node, shutdown_deadline, ExitCode::from(1))
						.await;
				}
			}
		}
		_ => None,
	};

	wait_for_stop_signal().await;
	eprintln!("wendelon: draining");
	// From here the process must exit within the deadline. A shutdown that
	// cannot prove every writer stopped terminates instead of lingering with the
	// lock held; the next start finds no clean-shutdown record and recovers.
	let watchdog = crate::shutdown_watchdog::ShutdownWatchdog::arm(shutdown_deadline);
	if let Err(e) = node.shutdown().await {
		if is_shutdown_incomplete(&e) {
			crate::shutdown_watchdog::terminate_unclean(&format!(
				"storage shutdown incomplete: {e}"
			));
		}
		eprintln!("shutdown failed: {e}");
		return DaemonExit::armed(ExitCode::from(1), watchdog);
	}
	if let Some(handles) = observability {
		if let Err(e) = shutdown_observability(handles).await {
			eprintln!("observability shutdown failed: {e}");
			return DaemonExit::armed(ExitCode::from(1), watchdog);
		}
	}
	if let Err(e) = telemetry.shutdown().await {
		eprintln!("telemetry shutdown failed: {e}");
		return DaemonExit::armed(ExitCode::from(1), watchdog);
	}
	eprintln!("wendelon: shutdown complete");
	DaemonExit::armed(ExitCode::SUCCESS, watchdog)
}

/// How the daemon body ended. An armed watchdog stays armed until the runtime
/// itself has been torn down, so blocking work left behind cannot hang the exit.
struct DaemonExit {
	code: ExitCode,
	watchdog: Option<crate::shutdown_watchdog::ShutdownWatchdog>,
}

impl DaemonExit {
	fn now(code: ExitCode) -> Self {
		Self {
			code,
			watchdog: None,
		}
	}
	fn armed(code: ExitCode, watchdog: crate::shutdown_watchdog::ShutdownWatchdog) -> Self {
		Self {
			code,
			watchdog: Some(watchdog),
		}
	}
}

/// A failure after bootstrap still owes the node a bounded shutdown: dropping a
/// live node would leave Raft and its workers running with the lock held.
async fn stop_after_failed_start(
	node: WendelonNode,
	deadline: std::time::Duration,
	code: ExitCode,
) -> DaemonExit {
	let watchdog = crate::shutdown_watchdog::ShutdownWatchdog::arm(deadline);
	if let Err(e) = node.shutdown().await {
		if is_shutdown_incomplete(&e) {
			crate::shutdown_watchdog::terminate_unclean(&format!(
				"storage shutdown incomplete: {e}"
			));
		}
		eprintln!("shutdown after failed start failed: {e}");
	}
	DaemonExit::armed(code, watchdog)
}

/// Drain on SIGINT (interactive, and what `nilket local` sends) or SIGTERM
/// (the default stop signal of systemd and Kubernetes).
async fn wait_for_stop_signal() {
	#[cfg(unix)]
	{
		let mut term =
			match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
				Ok(term) => term,
				Err(error) => {
					eprintln!(
						"wendelon: SIGTERM handler unavailable ({error}); draining on SIGINT only"
					);
					tokio::signal::ctrl_c().await.ok();
					return;
				}
			};
		tokio::select! {
			_ = tokio::signal::ctrl_c() => {}
			_ = term.recv() => {}
		}
	}
	#[cfg(not(unix))]
	tokio::signal::ctrl_c().await.ok();
}

fn is_shutdown_incomplete(error: &std::io::Error) -> bool {
	error
		.get_ref()
		.is_some_and(|inner| inner.is::<crate::storage::ownership::ShutdownIncomplete>())
}

async fn shutdown_observability(handles: ObservabilityHandles) -> std::io::Result<()> {
	for handle in [&handles.health, &handles.ready, &handles.metrics] {
		handle.task.abort();
	}
	for (label, task) in [
		("health listener", handles.health.task),
		("ready listener", handles.ready.task),
		("metrics listener", handles.metrics.task),
	] {
		match task.await {
			Ok(()) => {}
			Err(err) if err.is_cancelled() => {}
			Err(err) => {
				return Err(std::io::Error::other(format!(
					"{label} failed during shutdown: {err}",
				)));
			}
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::net::SocketAddr;
	use std::path::{Path, PathBuf};
	use std::process::Command;
	use std::sync::Mutex;
	use std::time::{SystemTime, UNIX_EPOCH};

	use tokio_rustls::TlsAcceptor;

	static INSPECT_ENV_LOCK: Mutex<()> = Mutex::new(());
	const TEST_CLUSTER_DOMAIN: &str = "cluster.local";
	const CERT_VALIDITY_DAYS: &str = "30";

	#[derive(Clone)]
	struct StaticPki {
		ca_cert: PathBuf,
		ops_ca_cert: PathBuf,
		server_cert: PathBuf,
		server_key: PathBuf,
		operator_cert: PathBuf,
		operator_key: PathBuf,
	}

	struct MinimalReadyServer {
		listener: tokio::net::TcpListener,
		acceptor: TlsAcceptor,
		connect_addr: SocketAddr,
	}

	impl MinimalReadyServer {
		async fn start(pki: &StaticPki) -> Self {
			let material = TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert)
				.expect("load server material");
			let trust_roots =
				ActorTrustRoots::load(&pki.ca_cert, &pki.ops_ca_cert).expect("load trust roots");
			let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
				.await
				.expect("bind ready listener");
			let connect_addr = listener.local_addr().expect("ready listener addr");
			let server_config = crate::network::tls::rustls_server_config_with_peer_roots(
				&material,
				trust_roots.combined_roots(),
				Vec::new(),
			)
			.expect("build server config");
			Self {
				listener,
				acceptor: TlsAcceptor::from(Arc::new(server_config)),
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
				let request = observability_http::read_request(&mut stream).await?;
				observability_http::write_response(
					&mut stream,
					status,
					"application/json; charset=utf-8",
					&body,
				)
				.await?;
				Ok(request)
			})
		}

		fn spawn_slow_response_once(self) -> tokio::task::JoinHandle<io::Result<Vec<u8>>> {
			tokio::spawn(async move {
				let (stream, _) = self.listener.accept().await?;
				let mut stream = self
					.acceptor
					.accept(stream)
					.await
					.map_err(io::Error::other)?;
				let request = observability_http::read_request(&mut stream).await?;
				tokio::time::sleep(Duration::from_secs(60)).await;
				Ok(request)
			})
		}
	}

	fn args(values: &[&str]) -> Vec<String> {
		values.iter().map(|value| (*value).to_string()).collect()
	}

	fn fresh_inspect_test_dir() -> PathBuf {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system time after epoch")
			.as_nanos();
		let dir = std::env::temp_dir().join(format!(
			"wendelon-inspect-cli-{}-{nanos}",
			std::process::id()
		));
		std::fs::create_dir_all(&dir).expect("create inspect test dir");
		dir
	}

	fn generated_pki() -> StaticPki {
		let dir = fresh_inspect_test_dir().join("pki");
		std::fs::create_dir_all(&dir).expect("create pki dir");
		let pki = StaticPki {
			ca_cert: dir.join("ca.crt"),
			ops_ca_cert: dir.join("ops_ca.crt"),
			server_cert: dir.join("server.crt"),
			server_key: dir.join("server.key"),
			operator_cert: dir.join("operator.crt"),
			operator_key: dir.join("operator.key"),
		};
		let data_ca_key = dir.join("ca.key");
		let ops_ca_key = dir.join("ops_ca.key");
		generate_ca("w2-inspect-test-ca", &pki.ca_cert, &data_ca_key);
		generate_ca("w2-inspect-test-ops-ca", &pki.ops_ca_cert, &ops_ca_key);
		generate_leaf(LeafSpec {
			common_name: "node-1",
			role: "data_node",
			node_dns_name: Some(format!("node-1.{TEST_CLUSTER_DOMAIN}")),
			key_id: 1u32.to_le_bytes().to_vec(),
			scopes: vec!["cluster"],
			cert_path: &pki.server_cert,
			key_path: &pki.server_key,
			ca_cert_path: &pki.ca_cert,
			ca_key_path: &data_ca_key,
			serial: 1001,
		});
		generate_leaf(LeafSpec {
			common_name: "operator",
			role: "operator",
			node_dns_name: None,
			key_id: vec![5, 6, 7, 8],
			scopes: vec!["cluster"],
			cert_path: &pki.operator_cert,
			key_path: &pki.operator_key,
			ca_cert_path: &pki.ops_ca_cert,
			ca_key_path: &ops_ca_key,
			serial: 2001,
		});
		pki
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

	fn generate_leaf(spec: LeafSpec<'_>) {
		let dir = spec
			.cert_path
			.parent()
			.expect("leaf cert path has parent directory");
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
			"{}=DER:{}\n",
			crate::network::tls::ACTOR_CLAIMS_OID_DOTTED,
			to_openssl_hex(&actor_claims)
		));
		if let Some(node_dns_name) = &spec.node_dns_name {
			ext.push_str("subjectAltName=@alt_names\n\n[alt_names]\n");
			ext.push_str(&format!("DNS.1={node_dns_name}\n"));
		}
		std::fs::write(&ext_path, ext).expect("write openssl extfile");

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
			write!(&mut out, "{byte:02X}").expect("write hex byte");
		}
		out
	}

	fn static_pki_paths() -> StaticPki {
		let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/pki");
		StaticPki {
			ca_cert: root.join("ca.crt"),
			ops_ca_cert: root.join("ops_ca.crt"),
			server_cert: root.join("server.crt"),
			server_key: root.join("server.key"),
			operator_cert: root.join("operator.crt"),
			operator_key: root.join("operator.key"),
		}
	}

	fn inspect_options(endpoint: SocketAddr, pki: &StaticPki, json: bool) -> InspectOptions {
		InspectOptions {
			endpoint,
			server_name: "node-1.cluster.local".to_string(),
			cert_path: pki.operator_cert.clone(),
			key_path: pki.operator_key.clone(),
			ca_path: pki.ca_cert.clone(),
			json,
		}
	}

	fn clear_inspect_env() {
		unsafe {
			// SAFETY: tests serialize WENDELON_INSPECT_* mutations with INSPECT_ENV_LOCK.
			std::env::remove_var("WENDELON_INSPECT_ENDPOINT");
			std::env::remove_var("WENDELON_INSPECT_SERVER_NAME");
			std::env::remove_var("WENDELON_INSPECT_CLIENT_CERT");
			std::env::remove_var("WENDELON_INSPECT_CLIENT_KEY");
			std::env::remove_var("WENDELON_INSPECT_CA");
		}
	}

	fn ready_body() -> String {
		r#"{"node_id":"n-01","epoch":7,"leader_id":"n-01","cluster_members":["n-01"],"drain_state":"none","audit_materialization_lag_entries":0,"degraded_reason":null}"#.to_string()
	}

	fn degraded_reason_body() -> String {
		r#"{"node_id":"n-01","epoch":7,"leader_id":"n-01","cluster_members":["n-01"],"drain_state":"none","audit_materialization_lag_entries":0,"degraded_reason":"warming\n\"quoted\""}"#.to_string()
	}

	fn lag_body() -> String {
		r#"{"node_id":"n-01","epoch":7,"leader_id":"n-01","cluster_members":["n-01"],"drain_state":"none","audit_materialization_lag_entries":2,"degraded_reason":null}"#.to_string()
	}

	fn fips_body() -> String {
		r#"{"node_id":"n-01","epoch":7,"leader_id":"n-01","cluster_members":["n-01"],"drain_state":"none","audit_materialization_lag_entries":0,"degraded_reason":null,"fips_mode":true,"fips_self_test_last_ts":1710000000}"#.to_string()
	}

	#[test]
	fn wendelon_inspect_requires_complete_tls_options() {
		let _guard = INSPECT_ENV_LOCK.lock().expect("lock inspect env");
		clear_inspect_env();

		let missing = parse_inspect_args(&[]).expect_err("inspect without TLS must fail");
		assert_eq!(missing, INSPECT_TLS_REQUIRED);

		let partial = parse_inspect_args(&args(&[
			"--endpoint",
			"127.0.0.1:7432",
			"--server-name",
			"node-1.cluster.local",
		]))
		.expect_err("partial TLS options must fail");
		assert_eq!(partial, INSPECT_TLS_REQUIRED);

		let positional = parse_inspect_args(&args(&["127.0.0.1:7432"]))
			.expect_err("positional endpoint must fail");
		assert_eq!(
			positional,
			"wendelon inspect does not accept positional arguments"
		);
	}

	#[test]
	fn wendelon_inspect_endpoint_env_and_flag_precedence() {
		let _guard = INSPECT_ENV_LOCK.lock().expect("lock inspect env");
		clear_inspect_env();
		unsafe {
			// SAFETY: tests serialize WENDELON_INSPECT_* mutations with INSPECT_ENV_LOCK.
			std::env::set_var("WENDELON_INSPECT_ENDPOINT", "127.0.0.1:7433");
			std::env::set_var("WENDELON_INSPECT_SERVER_NAME", "node-1.cluster.local");
			std::env::set_var("WENDELON_INSPECT_CLIENT_CERT", "env-operator.pem");
			std::env::set_var("WENDELON_INSPECT_CLIENT_KEY", "env-operator.key");
			std::env::set_var("WENDELON_INSPECT_CA", "env-ca.pem");
		}

		let env_options = parse_inspect_args(&args(&["--json"])).expect("env inspect options");
		assert_eq!(env_options.endpoint, "127.0.0.1:7433".parse().unwrap());
		assert_eq!(env_options.server_name, "node-1.cluster.local");
		assert_eq!(env_options.cert_path, PathBuf::from("env-operator.pem"));
		assert_eq!(env_options.key_path, PathBuf::from("env-operator.key"));
		assert_eq!(env_options.ca_path, PathBuf::from("env-ca.pem"));
		assert!(env_options.json);

		let flag_options = parse_inspect_args(&args(&[
			"--endpoint",
			"127.0.0.1:7434",
			"--server-name",
			"node-2.cluster.local",
			"--cert",
			"flag-operator.pem",
			"--key",
			"flag-operator.key",
			"--ca",
			"flag-ca.pem",
		]))
		.expect("flag inspect options");
		assert_eq!(flag_options.endpoint, "127.0.0.1:7434".parse().unwrap());
		assert_eq!(flag_options.server_name, "node-2.cluster.local");
		assert_eq!(flag_options.cert_path, PathBuf::from("flag-operator.pem"));
		assert_eq!(flag_options.key_path, PathBuf::from("flag-operator.key"));
		assert_eq!(flag_options.ca_path, PathBuf::from("flag-ca.pem"));
		assert!(!flag_options.json);

		clear_inspect_env();
	}

	#[test]
	fn wendelon_inspect_text_renderer_omits_absent_fips_fields() {
		let view = parse_readiness_view(ready_body().as_bytes()).expect("ready view");
		assert_eq!(
			render_readiness_text(&view),
			"node_id: \"n-01\"\nepoch: 7\nleader_id: \"n-01\"\ncluster_members: [\"n-01\"]\ndrain_state: \"none\"\naudit_materialization_lag_entries: 0\ndegraded_reason: null\n"
		);
	}

	#[test]
	fn wendelon_inspect_text_renderer_appends_present_fips_fields() {
		let view = parse_readiness_view(fips_body().as_bytes()).expect("fips view");
		assert_eq!(
			render_readiness_text(&view),
			"node_id: \"n-01\"\nepoch: 7\nleader_id: \"n-01\"\ncluster_members: [\"n-01\"]\ndrain_state: \"none\"\naudit_materialization_lag_entries: 0\ndegraded_reason: null\nfips_mode: true\nfips_self_test_last_ts: 1710000000\n"
		);
	}

	#[test]
	fn wendelon_inspect_preserves_json_body_with_json_flag() {
		let body = ready_body().into_bytes();
		let view = parse_readiness_view(&body).expect("ready view");
		let response = InspectReadyResponse {
			body: body.clone(),
			view,
		};
		let mut expected = body;
		expected.push(b'\n');
		assert_eq!(render_inspect_output(&response, true), expected);
	}

	#[test]
	fn wendelon_inspect_exit_zero_on_body_ready() {
		let view = parse_readiness_view(ready_body().as_bytes()).expect("ready view");
		assert!(view.ready);
	}

	#[test]
	fn wendelon_inspect_exit_one_on_degraded_reason() {
		let view = parse_readiness_view(degraded_reason_body().as_bytes()).expect("degraded view");
		assert!(!view.ready);
		assert_eq!(
			render_readiness_text(&view),
			"node_id: \"n-01\"\nepoch: 7\nleader_id: \"n-01\"\ncluster_members: [\"n-01\"]\ndrain_state: \"none\"\naudit_materialization_lag_entries: 0\ndegraded_reason: \"warming\\n\\\"quoted\\\"\"\n"
		);
	}

	#[test]
	fn wendelon_inspect_reads_control_retention_violation() {
		let with_retention = |violation: &str| {
			ready_body().replace(
				"}",
				&format!(
					r#","control_retention":{{"configured":"full","history":"fresh","violation":{violation},"refusals":{{}}}}}}"#
				),
			)
		};
		// A server that predates retention reporting omits the key.
		let omitted = parse_readiness_view(ready_body().as_bytes()).expect("omitted");
		assert!(omitted.ready);
		let null = parse_readiness_view(with_retention("null").as_bytes()).expect("null");
		assert!(null.ready);
		let latched = parse_readiness_view(with_retention(r#""snapshot_request""#).as_bytes())
			.expect("latched");
		assert!(!latched.ready);
		// The rendered fields are unchanged; only readiness reflects the latch.
		assert_eq!(
			render_readiness_text(&latched),
			render_readiness_text(&omitted)
		);
		for malformed in ["1", "true", "[]", "{}"] {
			match parse_readiness_view(with_retention(malformed).as_bytes()) {
				Err(InspectError::ParseSnapshot(message)) => assert_eq!(
					message,
					"field control_retention.violation must be null or a string"
				),
				_ => panic!("malformed violation {malformed} must not parse"),
			}
		}
		let missing = ready_body().replace("}", r#","control_retention":{}}"#);
		assert!(matches!(
			parse_readiness_view(missing.as_bytes()),
			Err(InspectError::ParseSnapshot(_))
		));
	}

	#[test]
	fn wendelon_inspect_exit_one_on_audit_materialization_lag() {
		let view = parse_readiness_view(lag_body().as_bytes()).expect("lag view");
		assert!(!view.ready);
	}

	#[tokio::test]
	async fn wendelon_inspect_writes_ready_request_with_host_header() {
		let pki = generated_pki();
		let server = MinimalReadyServer::start(&pki).await;
		let endpoint = server.connect_addr;
		let handle = server.spawn_response_once("200 OK", ready_body());
		let response = fetch_ready(&inspect_options(endpoint, &pki, false))
			.await
			.expect("ready response");
		let request = handle.await.expect("server task").expect("request");
		let request = String::from_utf8(request).expect("request utf8");

		assert!(response.view.ready);
		assert_eq!(
			request,
			format!("GET /ready HTTP/1.1\r\nHost: {endpoint}\r\nConnection: close\r\n\r\n")
		);
	}

	#[tokio::test]
	async fn wendelon_inspect_accepts_200_and_503_as_expected_statuses() {
		let pki = generated_pki();
		let ready_server = MinimalReadyServer::start(&pki).await;
		let ready_endpoint = ready_server.connect_addr;
		let ready_handle = ready_server.spawn_response_once("200 OK", ready_body());
		let ready = fetch_ready(&inspect_options(ready_endpoint, &pki, false))
			.await
			.expect("ready response");
		assert!(ready.view.ready);
		ready_handle
			.await
			.expect("ready server task")
			.expect("request");

		let degraded_server = MinimalReadyServer::start(&pki).await;
		let degraded_endpoint = degraded_server.connect_addr;
		let degraded_handle =
			degraded_server.spawn_response_once("503 Service Unavailable", lag_body());
		let degraded = fetch_ready(&inspect_options(degraded_endpoint, &pki, false))
			.await
			.expect("degraded response");
		assert!(!degraded.view.ready);
		degraded_handle
			.await
			.expect("degraded server task")
			.expect("request");
	}

	#[tokio::test]
	async fn wendelon_inspect_unexpected_status_returns_exit_two() {
		let pki = generated_pki();
		let server = MinimalReadyServer::start(&pki).await;
		let endpoint = server.connect_addr;
		let handle = server.spawn_response_once("418 I'm a Teapot", ready_body());
		let error = fetch_ready(&inspect_options(endpoint, &pki, false))
			.await
			.expect_err("unexpected status must fail");
		assert!(matches!(error, InspectError::UnexpectedStatus(418)));
		handle.await.expect("server task").expect("request");
	}

	#[tokio::test]
	async fn wendelon_inspect_connect_timeout_maps_to_stable_error() {
		let pki = generated_pki();
		let options = inspect_options("127.0.0.1:7432".parse().expect("endpoint"), &pki, false);
		let error = fetch_ready_with_tcp_connect(
			&options,
			std::future::pending::<io::Result<TcpStream>>(),
			Duration::from_millis(1),
			READ_RESPONSE_TIMEOUT,
		)
		.await
		.expect_err("pending connect must time out");

		assert!(matches!(error, InspectError::ConnectTimeout));
		assert_eq!(
			inspect_error_message(&error),
			"wendelon inspect: failed to connect to endpoint within timeout"
		);
	}

	#[tokio::test]
	async fn wendelon_inspect_response_read_timeout_maps_to_stable_error() {
		let pki = generated_pki();
		let server = MinimalReadyServer::start(&pki).await;
		let endpoint = server.connect_addr;
		let handle = server.spawn_slow_response_once();
		let error = fetch_ready_with_tcp_connect(
			&inspect_options(endpoint, &pki, false),
			TcpStream::connect(endpoint),
			CONNECT_TIMEOUT,
			Duration::from_millis(1),
		)
		.await
		.expect_err("slow response must time out");

		assert!(matches!(error, InspectError::ResponseTimeout));
		assert_eq!(
			inspect_error_message(&error),
			"wendelon inspect: failed to receive response within timeout"
		);
		handle.abort();
	}

	#[test]
	fn wendelon_inspect_uses_observability_http_substrate() {
		let cli = include_str!("cli.rs");
		assert!(cli.contains("observability_http::write_request"));
		assert!(cli.contains("observability_http::read_response"));
		let inspect_region = cli
			.split("async fn fetch_ready_with_tcp_connect")
			.nth(1)
			.and_then(|rest| rest.split("fn parse_readiness_view").next())
			.expect("inspect fetch region");
		assert!(!inspect_region.contains("reqwest"));
	}

	#[test]
	fn wendelon_inspect_default_endpoint_is_pinned() {
		let _guard = INSPECT_ENV_LOCK.lock().expect("lock inspect env");
		clear_inspect_env();
		let pki = static_pki_paths();
		let options = parse_inspect_args(&args(&[
			"--server-name",
			"node-1.cluster.local",
			"--cert",
			pki.operator_cert.to_str().expect("cert path"),
			"--key",
			pki.operator_key.to_str().expect("key path"),
			"--ca",
			pki.ca_cert.to_str().expect("ca path"),
		]))
		.expect("default endpoint options");
		assert_eq!(
			options.endpoint,
			DEFAULT_INSPECT_ENDPOINT.parse::<SocketAddr>().unwrap()
		);
	}

	#[test]
	fn wendelon_inspect_error_diagnostics_are_pinned() {
		assert_eq!(
			inspect_error_message(&InspectError::TlsConfig(io::Error::new(
				io::ErrorKind::InvalidInput,
				"bad material"
			))),
			"wendelon inspect: failed to initialize mTLS client: bad material"
		);
		assert_eq!(
			inspect_error_message(&InspectError::TlsHandshake(io::Error::other("denied"))),
			"wendelon inspect: failed to complete TLS handshake: denied"
		);
		assert_eq!(
			inspect_error_message(&InspectError::UnexpectedStatus(418)),
			"wendelon inspect: failed to read /ready response: unexpected HTTP status 418"
		);
		assert_eq!(
			inspect_error_message(&InspectError::ParseSnapshot("missing".into())),
			"wendelon inspect: failed to parse readiness snapshot: missing"
		);
	}

	#[test]
	fn fresh_inspect_test_paths_are_unique() {
		assert!(fresh_inspect_test_dir().exists());
	}
}
