use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;

use serde::Serialize;
use wendelon_client::{
	BootstrapPeer, Client, ClientConfig, MaintenanceCompletionAuditWire, MaintenanceReportItemWire,
	MaintenanceReportOutcomeWire, MaintenanceReportResponseWire, MaintenanceRequeueOutcomeWire,
	MaintenanceRequeueResponseWire, TlsPaths,
};

const DEFAULT_PAGE_ITEMS: u16 = 64;
const MAX_REPORT_RESTARTS: usize = 3;

#[derive(Clone, Debug)]
struct ConnectionOptions {
	bootstrap: BootstrapPeer,
	target_node_id: u32,
	cert_path: PathBuf,
	key_path: PathBuf,
	ca_path: PathBuf,
	cluster_domain: String,
	json: bool,
}

#[derive(Debug)]
struct ReportOptions {
	connection: ConnectionOptions,
	page_items: u16,
}

#[derive(Debug)]
struct RequeueOptions {
	connection: ConnectionOptions,
	operation_code: u16,
	slabs: Vec<u64>,
	fingerprint: Vec<u8>,
}

#[derive(Serialize)]
struct ReportDocument {
	serving_node_id: u32,
	policy_enabled: bool,
	diagnostic: Option<String>,
	incarnation: [u8; 16],
	generation: u64,
	items: Vec<MaintenanceReportItemWire>,
}

#[async_trait::async_trait]
trait MaintenanceControlClient {
	async fn report_page(
		&self,
		target_node_id: u32,
		max_items: u16,
		cursor: Vec<u8>,
	) -> Result<MaintenanceReportResponseWire, String>;

	async fn requeue(
		&self,
		target_node_id: u32,
		operation_code: u16,
		slabs: Vec<u64>,
		expected_fingerprint: Vec<u8>,
	) -> Result<MaintenanceRequeueResponseWire, String>;
}

#[async_trait::async_trait]
impl MaintenanceControlClient for Client {
	async fn report_page(
		&self,
		target_node_id: u32,
		max_items: u16,
		cursor: Vec<u8>,
	) -> Result<MaintenanceReportResponseWire, String> {
		self.maintenance_report(target_node_id, max_items, cursor)
			.await
			.map_err(|error| error.to_string())
	}

	async fn requeue(
		&self,
		target_node_id: u32,
		operation_code: u16,
		slabs: Vec<u64>,
		expected_fingerprint: Vec<u8>,
	) -> Result<MaintenanceRequeueResponseWire, String> {
		self.maintenance_requeue(target_node_id, operation_code, slabs, expected_fingerprint)
			.await
			.map_err(|error| error.to_string())
	}
}

pub(super) fn run(args: &[String]) -> ExitCode {
	match args {
		[verb, rest @ ..] if verb == "report" => match parse_report(rest) {
			Ok(options) => run_async(move || run_report(options)),
			Err(message) => usage_error(&message),
		},
		[verb, rest @ ..] if verb == "requeue" => match parse_requeue(rest) {
			Ok(options) => run_async(move || run_requeue(options)),
			Err(message) => usage_error(&message),
		},
		[] => usage_error("toron maintenance requires report or requeue"),
		[verb, ..] => usage_error(&format!("unknown toron maintenance command: {verb}")),
	}
}

fn run_async<F, Fut>(work: F) -> ExitCode
where
	F: FnOnce() -> Fut,
	Fut: Future<Output = ExitCode>,
{
	match tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime.block_on(work()),
		Err(error) => {
			eprintln!("failed to initialize Toron maintenance runtime: {error}");
			ExitCode::from(2)
		}
	}
}

async fn run_report(options: ReportOptions) -> ExitCode {
	let client = match connect(&options.connection).await {
		Ok(client) => client,
		Err(error) => {
			eprintln!("maintenance report connection failed: {error}");
			return ExitCode::from(2);
		}
	};
	match collect_report(
		&client,
		options.connection.target_node_id,
		options.page_items,
	)
	.await
	{
		Ok(document) => {
			if options.connection.json {
				match serde_json::to_string_pretty(&document) {
					Ok(rendered) => println!("{rendered}"),
					Err(error) => {
						eprintln!("failed to render maintenance report: {error}");
						return ExitCode::from(1);
					}
				}
			} else {
				print_report(&document);
			}
			ExitCode::SUCCESS
		}
		Err(error) => {
			eprintln!("maintenance report failed: {error}");
			ExitCode::from(1)
		}
	}
}

async fn run_requeue(options: RequeueOptions) -> ExitCode {
	let client = match connect(&options.connection).await {
		Ok(client) => client,
		Err(error) => {
			eprintln!("maintenance requeue connection failed: {error}");
			return ExitCode::from(2);
		}
	};
	run_requeue_with_client(&options, &client).await
}

async fn run_requeue_with_client(
	options: &RequeueOptions,
	client: &(impl MaintenanceControlClient + Sync),
) -> ExitCode {
	let response = client
		.requeue(
			options.connection.target_node_id,
			options.operation_code,
			options.slabs.clone(),
			options.fingerprint.clone(),
		)
		.await;
	match response {
		Ok(response) => render_requeue(options, &response),
		Err(error) => {
			// The request may have committed before the response was lost. A fresh
			// node-local report establishes convergence without claiming who cleared it.
			match collect_report(
				client,
				options.connection.target_node_id,
				DEFAULT_PAGE_ITEMS,
			)
			.await
			{
				Ok(report) if !report.policy_enabled => {
					eprintln!(
						"maintenance requeue failed: {error}; read-after-ambiguity found the policy disabled, so convergence cannot be established"
					);
					ExitCode::from(1)
				}
				Ok(report) => match fingerprint_state(
					&report.items,
					options.operation_code,
					&options.slabs,
					&options.fingerprint,
				) {
					FingerprintState::Absent => {
						eprintln!(
							"maintenance requeue response was lost; a fresh report shows the reviewed quarantine absent (converged, clearer unknown)"
						);
						ExitCode::SUCCESS
					}
					FingerprintState::Same => {
						eprintln!(
							"maintenance requeue failed before convergence was observed: {error}; the reviewed quarantine is still present"
						);
						ExitCode::from(1)
					}
					FingerprintState::Changed => {
						eprintln!(
							"maintenance requeue response was ambiguous and the quarantine changed; review a new report before retrying"
						);
						ExitCode::from(1)
					}
				},
				Err(report_error) => {
					eprintln!(
						"maintenance requeue failed: {error}; the required read-after-ambiguity also failed: {report_error}"
					);
					ExitCode::from(1)
				}
			}
		}
	}
}

async fn connect(options: &ConnectionOptions) -> Result<Client, wendelon_client::ClientError> {
	let mut config = ClientConfig::new(
		SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
		vec![options.bootstrap.clone()],
		TlsPaths {
			cert_path: options.cert_path.clone(),
			key_path: options.key_path.clone(),
			ca_path: options.ca_path.clone(),
		},
	);
	config.cluster_domain = options.cluster_domain.clone();
	Client::connect_quic(config).await
}

async fn collect_report(
	client: &(impl MaintenanceControlClient + Sync),
	target_node_id: u32,
	page_items: u16,
) -> Result<ReportDocument, String> {
	for _ in 0..MAX_REPORT_RESTARTS {
		let mut cursor = Vec::new();
		let mut items = Vec::new();
		let mut identity = None;
		loop {
			let response = client
				.report_page(target_node_id, page_items, cursor)
				.await?;
			match response.outcome {
				MaintenanceReportOutcomeWire::Page => {}
				MaintenanceReportOutcomeWire::StaleCursor => break,
				MaintenanceReportOutcomeWire::PolicyDisabled { diagnostic } => {
					return Ok(ReportDocument {
						serving_node_id: response.serving_node_id,
						policy_enabled: false,
						diagnostic: Some(diagnostic),
						incarnation: response.incarnation,
						generation: response.generation,
						items: response.items,
					});
				}
				MaintenanceReportOutcomeWire::InvalidRequest { diagnostic }
				| MaintenanceReportOutcomeWire::Unavailable { diagnostic } => {
					return Err(diagnostic);
				}
			}
			let page_identity = (
				response.serving_node_id,
				response.incarnation,
				response.generation,
			);
			if let Some(expected) = identity {
				if expected != page_identity {
					break;
				}
			} else {
				identity = Some(page_identity);
			}
			items.extend(response.items);
			if response.next_cursor.is_empty() {
				let (serving_node_id, incarnation, generation) =
					identity.expect("a successful report has at least one page");
				return Ok(ReportDocument {
					serving_node_id,
					policy_enabled: true,
					diagnostic: None,
					incarnation,
					generation,
					items,
				});
			}
			cursor = response.next_cursor;
		}
	}
	Err("maintenance report changed during three complete paging attempts".to_owned())
}

fn render_requeue(options: &RequeueOptions, response: &MaintenanceRequeueResponseWire) -> ExitCode {
	if options.connection.json {
		match serde_json::to_string_pretty(response) {
			Ok(rendered) => println!("{rendered}"),
			Err(error) => {
				eprintln!("failed to render maintenance requeue response: {error}");
				return ExitCode::from(1);
			}
		}
	} else {
		println!(
			"node={} operation={} slabs={} outcome={} completion_audit={}",
			response.serving_node_id,
			operation_label(response.operation_code),
			response.slab_count,
			requeue_outcome_label(&response.outcome),
			completion_audit_label(&response.completion_audit),
		);
	}
	if matches!(
		response.outcome,
		MaintenanceRequeueOutcomeWire::Cleared { .. }
	) {
		ExitCode::SUCCESS
	} else {
		ExitCode::from(1)
	}
}

fn print_report(document: &ReportDocument) {
	println!(
		"node={} generation={} incarnation={}",
		document.serving_node_id,
		document.generation,
		hex(&document.incarnation),
	);
	if !document.policy_enabled {
		println!(
			"maintenance policy disabled: {}",
			document.diagnostic.as_deref().unwrap_or("no diagnostic")
		);
		return;
	}
	for item in &document.items {
		match item {
			MaintenanceReportItemWire::QuarantinedWork {
				operation_code,
				slabs,
				origins,
				quarantine_fingerprint,
			} => {
				println!(
					"quarantine operation={} slabs={} origins={} fingerprint={}",
					operation_label(*operation_code),
					slab_list(slabs),
					origins.len(),
					hex(quarantine_fingerprint),
				);
				for origin in origins {
					println!(
						"  origin={} reason={} attempts={} last_failure_ms={} deadline={} detail={}",
						origin.origin_code,
						origin.reason,
						origin.attempts,
						origin.last_failure_ms,
						origin
							.next_eligible_at_ms
							.map(|value| value.to_string())
							.unwrap_or_else(|| "quarantined".to_owned()),
						origin.detail,
					);
				}
			}
			MaintenanceReportItemWire::RetryableCooldown {
				operation_code,
				slabs,
				consecutive_failures,
				next_eligible_at_ms,
			} => println!(
				"retryable operation={} slabs={} failures={} deadline={}",
				operation_label(*operation_code),
				slab_list(slabs),
				consecutive_failures,
				next_eligible_at_ms,
			),
			MaintenanceReportItemWire::InFlight {
				operation_code,
				slabs,
			} => println!(
				"in_flight operation={} slabs={}",
				operation_label(*operation_code),
				slab_list(slabs),
			),
			MaintenanceReportItemWire::BackingOff {
				operation_code,
				slabs,
				origins,
			} => {
				println!(
					"backing_off operation={} slabs={} origins={}",
					operation_label(*operation_code),
					slab_list(slabs),
					origins.len(),
				);
				for origin in origins {
					println!(
						"  origin={} reason={} attempts={} last_failure_ms={} deadline={} detail={}",
						origin.origin_code,
						origin.reason,
						origin.attempts,
						origin.last_failure_ms,
						origin
							.next_eligible_at_ms
							.map(|value| value.to_string())
							.unwrap_or_else(|| "quarantined".to_owned()),
						origin.detail,
					);
				}
			}
		}
	}
	if document.items.is_empty() {
		println!("no maintenance policy work");
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FingerprintState {
	Absent,
	Same,
	Changed,
}

fn fingerprint_state(
	items: &[MaintenanceReportItemWire],
	operation_code: u16,
	slabs: &[u64],
	expected: &[u8],
) -> FingerprintState {
	for item in items {
		if let MaintenanceReportItemWire::QuarantinedWork {
			operation_code: found_operation,
			slabs: found_slabs,
			quarantine_fingerprint,
			..
		} = item && *found_operation == operation_code
			&& found_slabs == slabs
		{
			return if quarantine_fingerprint == expected {
				FingerprintState::Same
			} else {
				FingerprintState::Changed
			};
		}
	}
	FingerprintState::Absent
}

fn parse_report(args: &[String]) -> Result<ReportOptions, String> {
	let (connection, values) = parse_common(args, &[])?;
	let page_items = values.page_items.unwrap_or(DEFAULT_PAGE_ITEMS);
	if page_items == 0 {
		return Err("--page-items must be greater than zero".to_owned());
	}
	Ok(ReportOptions {
		connection,
		page_items,
	})
}

fn parse_requeue(args: &[String]) -> Result<RequeueOptions, String> {
	let (connection, values) = parse_common(args, &["operation", "slabs", "fingerprint"])?;
	let operation = values
		.operation
		.ok_or_else(|| "maintenance requeue requires --operation".to_owned())?;
	let operation_code = parse_operation(&operation)?;
	let slabs = parse_slabs(
		&values
			.slabs
			.ok_or_else(|| "maintenance requeue requires --slabs".to_owned())?,
	)?;
	let fingerprint = parse_hex(
		&values
			.fingerprint
			.ok_or_else(|| "maintenance requeue requires --fingerprint".to_owned())?,
	)?;
	if fingerprint.len() != 33 {
		return Err("--fingerprint must encode exactly 33 bytes".to_owned());
	}
	Ok(RequeueOptions {
		connection,
		operation_code,
		slabs,
		fingerprint,
	})
}

#[derive(Default)]
struct ParsedValues {
	bootstrap: Option<String>,
	node: Option<String>,
	cert: Option<PathBuf>,
	key: Option<PathBuf>,
	ca: Option<PathBuf>,
	cluster_domain: Option<String>,
	page_items: Option<u16>,
	operation: Option<String>,
	slabs: Option<String>,
	fingerprint: Option<String>,
	json: bool,
}

fn parse_common(
	args: &[String],
	allowed_specific: &[&str],
) -> Result<(ConnectionOptions, ParsedValues), String> {
	let mut values = ParsedValues::default();
	let mut index = 0;
	while index < args.len() {
		let flag = args[index].as_str();
		if flag == "--json" {
			values.json = true;
			index += 1;
			continue;
		}
		let value = args
			.get(index + 1)
			.ok_or_else(|| format!("{flag} requires a value"))?;
		match flag {
			"--bootstrap" => values.bootstrap = Some(value.clone()),
			"--node" => values.node = Some(value.clone()),
			"--cert" => values.cert = Some(PathBuf::from(value)),
			"--key" => values.key = Some(PathBuf::from(value)),
			"--ca" => values.ca = Some(PathBuf::from(value)),
			"--cluster-domain" => values.cluster_domain = Some(value.clone()),
			"--page-items" if allowed_specific.is_empty() => {
				values.page_items = Some(
					value
						.parse()
						.map_err(|_| "--page-items must be a u16".to_owned())?,
				)
			}
			"--operation" if allowed_specific.contains(&"operation") => {
				values.operation = Some(value.clone())
			}
			"--slabs" if allowed_specific.contains(&"slabs") => values.slabs = Some(value.clone()),
			"--fingerprint" if allowed_specific.contains(&"fingerprint") => {
				values.fingerprint = Some(value.clone())
			}
			_ => return Err(format!("unknown maintenance option: {flag}")),
		}
		index += 2;
	}
	let bootstrap =
		parse_bootstrap(&values.bootstrap.clone().ok_or_else(|| {
			"maintenance requires --bootstrap <node-id>@<socket-addr>".to_owned()
		})?)?;
	let target_node_id = values
		.node
		.as_deref()
		.ok_or_else(|| "maintenance requires --node".to_owned())?
		.parse::<u32>()
		.map_err(|_| "--node must be a u32".to_owned())?;
	let connection = ConnectionOptions {
		bootstrap,
		target_node_id,
		cert_path: values
			.cert
			.clone()
			.ok_or_else(|| "maintenance requires --cert".to_owned())?,
		key_path: values
			.key
			.clone()
			.ok_or_else(|| "maintenance requires --key".to_owned())?,
		ca_path: values
			.ca
			.clone()
			.ok_or_else(|| "maintenance requires --ca".to_owned())?,
		cluster_domain: values
			.cluster_domain
			.clone()
			.unwrap_or_else(|| tls_pki::DEFAULT_CLUSTER_DOMAIN.to_owned()),
		json: values.json,
	};
	Ok((connection, values))
}

fn parse_bootstrap(value: &str) -> Result<BootstrapPeer, String> {
	let (node, address) = value
		.split_once('@')
		.ok_or_else(|| "--bootstrap must be <node-id>@<socket-addr>".to_owned())?;
	let node_id = node
		.parse::<u32>()
		.map_err(|_| "bootstrap node id must be a u32".to_owned())?;
	address
		.parse::<SocketAddr>()
		.map_err(|_| "bootstrap address must be a socket address".to_owned())?;
	Ok(BootstrapPeer {
		node_id,
		address: address.to_owned(),
	})
}

fn parse_operation(value: &str) -> Result<u16, String> {
	match value {
		"lsm" | "lsm_compaction" => Ok(1),
		"retention" | "retention_prune" => Ok(2),
		"backfill" | "secondary_index_backfill" => Ok(3),
		"tombstone" | "tombstone_reclaim" => Ok(4),
		_ => Err("--operation must be lsm, retention, backfill, or tombstone".to_owned()),
	}
}

fn parse_slabs(value: &str) -> Result<Vec<u64>, String> {
	let mut slabs = value
		.split(',')
		.map(|part| {
			part.parse::<u64>()
				.map_err(|_| format!("invalid slab id: {part}"))
		})
		.collect::<Result<Vec<_>, _>>()?;
	if slabs.is_empty() {
		return Err("--slabs must name at least one slab".to_owned());
	}
	let original = slabs.clone();
	slabs.sort_unstable();
	slabs.dedup();
	if slabs != original {
		return Err("--slabs must be strictly increasing and contain no duplicates".to_owned());
	}
	Ok(slabs)
}

fn parse_hex(value: &str) -> Result<Vec<u8>, String> {
	if !value.len().is_multiple_of(2) {
		return Err("hex input must contain an even number of digits".to_owned());
	}
	value
		.as_bytes()
		.chunks_exact(2)
		.map(|pair| {
			let text = std::str::from_utf8(pair)
				.map_err(|_| "hex input must contain only ASCII digits".to_owned())?;
			u8::from_str_radix(text, 16).map_err(|_| format!("invalid hex byte: {text}"))
		})
		.collect()
}

fn hex(bytes: &[u8]) -> String {
	let mut rendered = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		use std::fmt::Write as _;
		let _ = write!(rendered, "{byte:02x}");
	}
	rendered
}

fn slab_list(slabs: &[u64]) -> String {
	slabs
		.iter()
		.map(u64::to_string)
		.collect::<Vec<_>>()
		.join(",")
}

fn operation_label(code: u16) -> &'static str {
	match code {
		1 => "lsm_compaction",
		2 => "retention_prune",
		3 => "secondary_index_backfill",
		4 => "tombstone_reclaim",
		_ => "unknown",
	}
}

fn requeue_outcome_label(outcome: &MaintenanceRequeueOutcomeWire) -> &'static str {
	match outcome {
		MaintenanceRequeueOutcomeWire::Cleared { .. } => "cleared",
		MaintenanceRequeueOutcomeWire::PolicyDisabled { .. } => "policy_disabled",
		MaintenanceRequeueOutcomeWire::InFlight => "in_flight",
		MaintenanceRequeueOutcomeWire::NotQuarantined => "not_quarantined",
		MaintenanceRequeueOutcomeWire::StaleReport { .. } => "stale_report",
		MaintenanceRequeueOutcomeWire::AuditUnavailable => "audit_unavailable",
		MaintenanceRequeueOutcomeWire::StoreFailure { .. } => "store_failure",
	}
}

fn completion_audit_label(audit: &MaintenanceCompletionAuditWire) -> &'static str {
	match audit {
		MaintenanceCompletionAuditWire::NotAttempted => "not_attempted",
		MaintenanceCompletionAuditWire::Recorded => "recorded",
		MaintenanceCompletionAuditWire::Failed { .. } => "failed",
	}
}

fn usage_error(message: &str) -> ExitCode {
	eprintln!("{message}");
	eprintln!(
		"usage: toron maintenance report --bootstrap <node-id>@<socket-addr> --node <node-id> --cert <path> --key <path> --ca <path> [--cluster-domain <domain>] [--page-items <count>] [--json]"
	);
	eprintln!(
		"       toron maintenance requeue --bootstrap <node-id>@<socket-addr> --node <node-id> --cert <path> --key <path> --ca <path> --operation <lsm|retention|backfill|tombstone> --slabs <id,...> --fingerprint <hex> [--cluster-domain <domain>] [--json]"
	);
	ExitCode::from(2)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Mutex;
	use wendelon_client::MAINTENANCE_CONTROL_WIRE_VERSION;

	fn args(values: &[&str]) -> Vec<String> {
		values.iter().map(|value| (*value).to_owned()).collect()
	}

	#[test]
	fn requeue_requires_exact_canonical_work_identity() {
		let base = [
			"--bootstrap",
			"7@127.0.0.1:7007",
			"--node",
			"7",
			"--cert",
			"cert.pem",
			"--key",
			"key.pem",
			"--ca",
			"ca.pem",
			"--operation",
			"lsm",
			"--slabs",
			"2,1",
			"--fingerprint",
			&"00".repeat(33),
		];
		assert!(parse_requeue(&args(&base)).is_err());
	}

	#[test]
	fn fingerprint_state_never_treats_a_changed_receipt_as_absent() {
		let items = vec![MaintenanceReportItemWire::QuarantinedWork {
			operation_code: 1,
			slabs: vec![4, 9],
			origins: Vec::new(),
			quarantine_fingerprint: vec![1; 33],
		}];
		assert_eq!(
			fingerprint_state(&items, 1, &[4, 9], &[2; 33]),
			FingerprintState::Changed
		);
		assert_eq!(
			fingerprint_state(&items, 2, &[4, 9], &[2; 33]),
			FingerprintState::Absent
		);
	}

	#[derive(Clone, Copy, Debug, PartialEq, Eq)]
	enum AmbiguousCall {
		Requeue,
		Report,
	}

	struct ResponseDroppingClient {
		state: Mutex<(bool, Vec<AmbiguousCall>)>,
	}

	#[async_trait::async_trait]
	impl MaintenanceControlClient for ResponseDroppingClient {
		async fn report_page(
			&self,
			target_node_id: u32,
			_max_items: u16,
			cursor: Vec<u8>,
		) -> Result<MaintenanceReportResponseWire, String> {
			assert!(cursor.is_empty());
			let mut state = self.state.lock().unwrap();
			state.1.push(AmbiguousCall::Report);
			assert!(
				!state.0,
				"the fresh report runs only after the server-side clear"
			);
			Ok(MaintenanceReportResponseWire {
				version: MAINTENANCE_CONTROL_WIRE_VERSION,
				serving_node_id: target_node_id,
				outcome: MaintenanceReportOutcomeWire::Page,
				incarnation: [7; 16],
				generation: 2,
				items: Vec::new(),
				next_cursor: Vec::new(),
			})
		}

		async fn requeue(
			&self,
			target_node_id: u32,
			operation_code: u16,
			slabs: Vec<u64>,
			expected_fingerprint: Vec<u8>,
		) -> Result<MaintenanceRequeueResponseWire, String> {
			assert_eq!(target_node_id, 7);
			assert_eq!(operation_code, 1);
			assert_eq!(slabs, [4, 9]);
			assert_eq!(expected_fingerprint, vec![3; 33]);
			let mut state = self.state.lock().unwrap();
			state.1.push(AmbiguousCall::Requeue);
			assert!(state.0, "the reviewed quarantine exists before the clear");
			state.0 = false;
			Err("response dropped after the server committed the clear".to_owned())
		}
	}

	/// ADR 0582 acceptance 6: the mutation commits but its response is lost. The
	/// command may report success only after a fresh node-local report observes the
	/// reviewed fingerprint absent; it must not infer success from transport loss.
	#[tokio::test]
	async fn a_dropped_response_after_a_successful_clear_forces_a_fresh_report() {
		let client = ResponseDroppingClient {
			state: Mutex::new((true, Vec::new())),
		};
		let options = RequeueOptions {
			connection: ConnectionOptions {
				bootstrap: BootstrapPeer {
					node_id: 7,
					address: "127.0.0.1:7007".to_owned(),
				},
				target_node_id: 7,
				cert_path: PathBuf::from("unused-cert.pem"),
				key_path: PathBuf::from("unused-key.pem"),
				ca_path: PathBuf::from("unused-ca.pem"),
				cluster_domain: "unused.test".to_owned(),
				json: false,
			},
			operation_code: 1,
			slabs: vec![4, 9],
			fingerprint: vec![3; 33],
		};

		assert_eq!(
			run_requeue_with_client(&options, &client).await,
			ExitCode::SUCCESS
		);
		let state = client.state.lock().unwrap();
		assert!(!state.0, "the quarantine was actually cleared");
		assert_eq!(
			state.1,
			[AmbiguousCall::Requeue, AmbiguousCall::Report],
			"success is claimed only after the required read-after-ambiguity"
		);
	}
}
