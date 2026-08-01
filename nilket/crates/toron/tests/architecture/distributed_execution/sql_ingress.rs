#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, network_tls::*, observability_audit::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};
use super::{
	cancellation::*, descriptor_sync::*, resource_governance::*, scheduler_dispatch::*,
	tenant_admission::*, worker_lifecycle::*,
};

#[test]
pub(crate) fn message_kind_retired_morsel_complete() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire source");
	let message_kind = source
		.split("pub enum MessageKind")
		.nth(1)
		.and_then(|rest| rest.split("impl MessageKind").next())
		.expect("MessageKind block");

	assert!(!message_kind.contains("MorselComplete"));
	assert_eq!(toron::wire::MessageKind::COUNT, 12);
}

#[test]
pub(crate) fn wire_contract_has_no_legacy_toron_message_wrapper() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire source");

	for forbidden in [
		"ToronMessage",
		"ExecuteMorsel",
		"ArrowBatch",
		"ShuffleData",
		"Error(String)",
	] {
		assert!(
			!source.contains(forbidden),
			"legacy wire marker {forbidden} is still present"
		);
	}
}

#[test]
pub(crate) fn wire_envelope_requires_routable_identity() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire source");
	let envelope = source
		.split("pub struct WireEnvelope")
		.nth(1)
		.and_then(|rest| rest.split("impl WireEnvelope").next())
		.expect("WireEnvelope block");
	let identity = source
		.split("pub enum EnvelopeIdentity")
		.nth(1)
		.and_then(|rest| rest.split("impl EnvelopeIdentity").next())
		.expect("EnvelopeIdentity block");

	assert!(envelope.contains("pub identity: EnvelopeIdentity,"));
	assert!(identity.contains("Task(TaskIdentity)"));
	assert!(identity.contains("Partition(PartitionAddress)"));
	assert!(identity.contains("worker_id: WorkerId"));
	assert!(identity.contains("lease_epoch: LeaseEpoch"));
}

#[test]
pub(crate) fn shuffle_surface_has_no_push_to_peer_path() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			let relative = path.strip_prefix(&manifest_dir).unwrap_or(path);
			if relative == Path::new("src/wire.rs") {
				return false;
			}
			line.contains("send_shuffle_data") || line.contains("ShuffleData")
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"push-style shuffle path found outside wire guard scope:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr057_sql_handshake_failures_do_not_stop_accept_loop() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_loop = brace_blocks_after_marker(&daemon, "async fn run_sql_accept_loop")
		.into_iter()
		.next()
		.expect("run_sql_accept_loop")
		.1;

	assert!(sql_loop.contains("endpoint.accept_sql_incoming()"));
	assert!(sql_loop.contains("QuicEndpoint::accept_incoming_connection(incoming)"));
	assert!(sql_loop.contains("\"sql listener handshake failed\""));
	let handshake_failure = sql_loop
		.find("\"sql listener handshake failed\"")
		.expect("handshake-failure log");
	let after = &sql_loop[handshake_failure..];
	assert!(after.contains("continue;"));
}

#[test]
pub(crate) fn adr057_sql_connection_limiter_lives_at_sql_accept_edge() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let connection = std::fs::read_to_string(manifest_dir().join("src/network/connection.rs"))
		.expect("read connection limiter");
	let server = source_files(&manifest_dir().join("src/server"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read server source"))
		.collect::<Vec<_>>()
		.join("\n");

	assert!(connection.contains("pub struct SqlConnectionLimiter"));
	assert!(daemon.contains("SqlConnectionLimiter::new("));
	assert!(daemon.contains("try_acquire_tier1()"));
	assert!(daemon.contains("try_acquire_tier2(tenant_id)"));
	assert!(!server.contains("SqlConnectionLimiter"));
}

#[test]
pub(crate) fn adr057_global_cap_uses_quic_incoming_before_handshake() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let quic = std::fs::read_to_string(manifest_dir().join("src/quic.rs")).expect("read quic");
	let sql_loop = brace_blocks_after_marker(&daemon, "async fn run_sql_accept_loop")
		.into_iter()
		.next()
		.expect("run_sql_accept_loop")
		.1;

	assert!(quic.contains("pub async fn accept_sql_incoming"));
	assert!(quic.contains("pub async fn accept_incoming_connection"));
	let tier1 = sql_loop.find("try_acquire_tier1()").expect("tier1 admit");
	let handshake = sql_loop
		.find("QuicEndpoint::accept_incoming_connection(incoming)")
		.expect("handshake");
	assert!(tier1 < handshake);
}

#[test]
pub(crate) fn adr057_per_tenant_cap_checks_after_sql_tenant_resolution() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_loop = brace_blocks_after_marker(&daemon, "async fn run_sql_accept_loop")
		.into_iter()
		.next()
		.expect("run_sql_accept_loop")
		.1;

	let tenant = sql_loop
		.find("options.tenant_source.resolve(&connection)")
		.expect("tenant resolution");
	let tier2 = sql_loop
		.find("try_acquire_tier2(tenant_id)")
		.expect("tier2 admit");
	assert!(tenant < tier2);
}

#[test]
pub(crate) fn adr057_loopback_sql_connection_cap_uses_config_tenant() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read sql listener tests");

	assert!(daemon.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
	assert!(sql_tests.contains("loopback_sql_tenant_connection_cap_uses_config_tenant"));
}

#[test]
pub(crate) fn adr057_connection_limiter_is_not_query_coordinator_or_admission_controller() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(!coordinator.contains("SqlConnectionLimiter"));
	assert!(!coordinator.contains("ConnectionLimitExceeded"));
	assert!(!budget.contains("SqlConnectionLimiter"));
	assert!(!budget.contains("ConnectionLimitExceeded"));
}

#[test]
pub(crate) fn adr057_connection_state_uses_one_lock_not_per_bucket_atomics() {
	let connection = std::fs::read_to_string(manifest_dir().join("src/network/connection.rs"))
		.expect("read connection limiter");

	assert!(connection.contains("state: Mutex<SqlConnectionState>"));
	assert_eq!(connection.matches("Mutex<SqlConnectionState>").count(), 1);
	assert!(!connection.contains("AtomicUsize"));
	assert!(!connection.contains("DashMap"));
}

#[test]
pub(crate) fn adr057_tenant_connection_keys_are_configured_plus_other() {
	let connection = std::fs::read_to_string(manifest_dir().join("src/network/connection.rs"))
		.expect("read connection limiter");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert!(connection.contains("enum TenantConnectionKey"));
	assert!(connection.contains("Configured(TenantId)"));
	assert!(connection.contains("Other"));
	assert!(connection.contains("configured_tenant_ids()"));
	assert!(observability.contains("tenant_sql_connections_active{{tenant_id=\\\"{label}\\\"}}"));
}

#[test]
pub(crate) fn adr057_connection_permits_release_by_raii_no_reaper() {
	let connection = std::fs::read_to_string(manifest_dir().join("src/network/connection.rs"))
		.expect("read connection limiter");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(connection.contains("impl Drop for Tier1Permit"));
	assert!(connection.contains("impl Drop for Tier2Permit"));
	assert!(daemon.contains("let _connection_permits = (tier1_permit, tier2_permit);"));
	assert!(!connection.contains("tokio::spawn"));
	assert!(!connection.contains("reaper"));
}

#[test]
pub(crate) fn adr057_tier1_refuses_incoming_without_sql_wire_error() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_loop = brace_blocks_after_marker(&daemon, "async fn run_sql_accept_loop")
		.into_iter()
		.next()
		.expect("run_sql_accept_loop")
		.1;
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read sql listener tests");

	assert!(sql_loop.contains("incoming.refuse();"));
	assert!(sql_tests.contains("assert_transport_rejects_without_sql_frame"));
	assert!(!sql_loop.contains("SqlErrorCode::ResourceExhausted"));
}

#[test]
pub(crate) fn adr057_tier2_closes_connection_without_sql_wire_error() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_loop = brace_blocks_after_marker(&daemon, "async fn run_sql_accept_loop")
		.into_iter()
		.next()
		.expect("run_sql_accept_loop")
		.1;
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read sql listener tests");

	assert!(sql_loop.contains("tenant sql connection limit exhausted"));
	assert!(sql_tests.contains("assert_authenticated_transport_rejects_without_sql_frame"));
	assert!(!sql_loop.contains("SqlErrorCode::ResourceExhausted"));
}

#[test]
pub(crate) fn adr057_metrics_split_global_aggregate_and_tenant_bounded() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	for metric in [
		"sql_connections_active",
		"sql_connection_rejections_total",
		"tenant_sql_connections_active",
		"tenant_sql_connection_rejections_total",
	] {
		assert!(observability.contains(metric), "missing metric {metric}");
	}
	assert!(observability.contains("tenant_metric_slots()"));
	assert!(observability_test.contains("sql_connection_metrics_are_configured_plus_other"));
}

#[test]
pub(crate) fn adr057_general_listener_metrics_remain_tenant_label_free() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	for line in observability.lines().filter(|line| {
		line.contains("listener_accepts_total") || line.contains("listener_rejects_total")
	}) {
		assert!(
			!line.contains("tenant_id"),
			"listener metric gained tenant label: {line}"
		);
	}
	assert!(observability.contains("ConnectionLimit"));
}

#[test]
pub(crate) fn adr057_default_disabled_caps_preserve_current_behavior() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let connection = std::fs::read_to_string(manifest_dir().join("src/network/connection.rs"))
		.expect("read connection limiter");

	assert!(config.contains("max_concurrent_connections: Option<NonZeroU32>"));
	assert!(config.contains("max_concurrent_connections.and_then(NonZeroU32::new)"));
	assert!(
		connection
			.contains("default_disabled_connection_caps_preserve_current_connection_behavior")
	);
}

#[test]
pub(crate) fn adr057_no_worker_transport_or_observability_connection_cap() {
	let worker_loop =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let server_source = source_files(&manifest_dir().join("src/server"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read server source"))
		.collect::<Vec<_>>()
		.join("\n");

	assert!(worker_loop.contains("run_quic_worker_accept_loop"));
	assert!(worker_loop.contains("accepted = endpoint.accept_connection()"));
	assert!(!server_source.contains("SqlConnectionLimiter"));
}

#[test]
pub(crate) fn adr057_no_parser_query_rate_queue_borrow_or_cluster_connection_coordination() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	for source in [coordinator.as_str(), budget.as_str(), planner.as_str()] {
		assert!(!source.contains("SqlConnectionLimiter"));
		assert!(!source.contains("ConnectionLimitExceeded"));
	}
}

#[test]
pub(crate) fn adr057_sql_quic_idle_timeout_not_disabled() {
	let quic = std::fs::read_to_string(manifest_dir().join("src/quic.rs")).expect("read quic");

	assert!(!quic.contains("max_idle_timeout(None)"));
	assert!(!quic.contains("keep_alive_interval(None)"));
}

#[test]
pub(crate) fn adr057_no_wendelon_sql_wire_nilket_or_fixture_worker_change() {
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	for source in [
		sql_wire.as_str(),
		fixture.as_str(),
		nilket_manifest.as_str(),
		wendelon_source.as_str(),
	] {
		assert!(!source.contains("SqlConnectionLimiter"));
		assert!(!source.contains("max_concurrent_connections"));
		assert!(!source.contains("ConnectionLimitExceeded"));
	}
}

#[test]
pub(crate) fn adr057_prior_adr049_050_051_052_053_054_055_056_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_", "adr056_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr057_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "clap", "tracing"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr057_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("SqlConnectionLimiter"));
	assert!(!worker.contains("max_concurrent_connections"));
	assert!(!worker.contains("ConnectionLimitExceeded"));
}

#[test]
pub(crate) fn adr057_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"SqlConnectionLimiterV1",
		"SqlConnectionAdmissionV1",
		"TenantConnectionKeyV1",
		"ConnectionLimitExceededV1",
	]);
}

#[test]
pub(crate) fn adr058_sql_request_reader_checks_length_before_allocation() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let wire_test = std::fs::read_to_string(manifest_dir().join("tests/toron_sql_wire_test.rs"))
		.expect("read SQL wire tests");
	let reader = brace_blocks_after_marker(&wire, "pub async fn read_request_frame_bounded")
		.into_iter()
		.next()
		.expect("bounded request reader")
		.1;

	let prefix = reader
		.find("u32::from_le_bytes(len_buf)")
		.expect("declared length decode");
	let check = reader
		.find("declared_len > max_request_frame_bytes.get()")
		.expect("cap check");
	let allocation = reader.find("vec![0u8; len]").expect("payload allocation");
	assert!(prefix < check);
	assert!(check < allocation);
	assert!(reader.contains("RequestFrameTooLarge"));
	assert!(
		wire_test
			.contains("oversized_declared_request_length_rejected_before_allocation_or_decode")
	);
}

#[test]
pub(crate) fn adr058_statement_limit_runs_before_sql_planner() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let handler = brace_blocks_after_marker(&daemon, "async fn handle_sql_connection")
		.into_iter()
		.next()
		.expect("handle_sql_connection")
		.1;

	let statement_check = handler
		.find("request.statement.len()")
		.expect("statement length check");
	let planner_new = handler.find("SqlPlanner::new()").expect("planner creation");
	let plan = handler
		.find("planner.plan(&request.statement)")
		.expect("planner call");
	assert!(statement_check < planner_new);
	assert!(statement_check < plan);
	assert!(handler.contains("RejectReason::StatementTooLarge"));
	assert!(handler.contains("SqlErrorCode::ResourceExhausted"));
}

#[test]
pub(crate) fn adr058_request_limits_are_default_on_nonzero_safety_floor() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(config.contains("max_request_frame_bytes: NonZeroU32"));
	assert!(config.contains("max_statement_bytes: NonZeroU32"));
	assert!(config.contains("max_request_frame_bytes: Option<u32>"));
	assert!(config.contains("max_statement_bytes: Option<u32>"));
	assert!(config.contains("raw.unwrap_or(default)"));
	assert!(config.contains("must be greater than 0"));
	assert!(config_test.contains("sql_request_caps_default_to_nonzero_equal_safety_floor"));
	assert!(config_test.contains("sql_request_caps_reject_zero"));
}

#[test]
pub(crate) fn adr058_request_limits_have_hard_ceilings() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(
		wire.contains("pub const SQL_REQUEST_FRAME_BYTES_HARD_CEILING: u32 = 64 * 1024 * 1024")
	);
	assert!(wire.contains("pub const SQL_STATEMENT_BYTES_HARD_CEILING: u32 = 64 * 1024 * 1024"));
	assert!(config.contains("SQL_REQUEST_FRAME_BYTES_HARD_CEILING"));
	assert!(config.contains("SQL_STATEMENT_BYTES_HARD_CEILING"));
	assert!(config.contains("value > hard_ceiling"));
	assert!(config_test.contains("sql_request_caps_reject_values_over_hard_ceiling"));
}

#[test]
pub(crate) fn adr058_statement_cap_cannot_exceed_request_frame_cap() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(config.contains("max_statement_bytes.get() > max_request_frame_bytes.get()"));
	assert!(config.contains(
		"sql.max_statement_bytes must be less than or equal to sql.max_request_frame_bytes"
	));
	assert!(config_test.contains("sql_statement_cap_cannot_exceed_request_frame_cap"));
}

#[test]
pub(crate) fn adr058_default_statement_cap_matches_request_cap() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(wire.contains("pub const DEFAULT_SQL_REQUEST_FRAME_BYTES: u32 = 4 * 1024 * 1024"));
	assert!(wire.contains("pub const DEFAULT_SQL_STATEMENT_BYTES: u32 = 4 * 1024 * 1024"));
	assert!(
		config_test.contains("assert_eq!(sql.max_statement_bytes, sql.max_request_frame_bytes)")
	);
}

#[test]
pub(crate) fn adr058_frame_reject_has_no_sql_error_before_query_id() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let handler = brace_blocks_after_marker(&daemon, "async fn handle_sql_connection")
		.into_iter()
		.next()
		.expect("handle_sql_connection")
		.1;
	let reject = handler
		.split("RejectReason::RequestFrameTooLarge")
		.nth(1)
		.expect("frame reject path")
		.split("return Ok(())")
		.next()
		.expect("frame reject block");
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");

	assert!(reject.contains("stream.finish().await"));
	assert!(!reject.contains("write_sql_error"));
	assert!(!reject.contains("SqlErrorCode"));
	assert!(!reject.contains("frame.query_id"));
	assert!(
		sql_tests
			.contains("oversized_declared_request_length_rejected_before_allocation_or_decode")
	);
	assert!(sql_tests.contains("must not emit a SQL error frame"));
}

#[test]
pub(crate) fn adr058_statement_reject_uses_existing_resource_exhausted() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let handler = brace_blocks_after_marker(&daemon, "async fn handle_sql_connection")
		.into_iter()
		.next()
		.expect("handle_sql_connection")
		.1;
	let statement_reject = handler
		.split("RejectReason::StatementTooLarge")
		.nth(1)
		.expect("statement reject path")
		.split("let timeout_duration")
		.next()
		.expect("statement reject block");
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");

	assert!(statement_reject.contains("write_sql_error"));
	assert!(statement_reject.contains("frame.query_id"));
	assert!(statement_reject.contains("SqlErrorCode::ResourceExhausted"));
	assert!(sql_tests.contains(
		"oversized_decoded_execute_statement_returns_resource_exhausted_without_planning"
	));
}

#[test]
pub(crate) fn adr058_cancel_frames_are_not_statement_capped() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let cancel = brace_blocks_after_marker(&daemon, "SqlMessage::Cancel")
		.into_iter()
		.next()
		.expect("cancel branch")
		.1;
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");

	assert!(cancel.contains("CancellationTrigger::SqlCancel"));
	assert!(cancel.contains("SqlErrorCode::Cancelled"));
	assert!(!cancel.contains("SqlErrorCode::CancellationUnsupported"));
	assert!(!cancel.contains("max_statement_bytes"));
	assert!(!cancel.contains("StatementTooLarge"));
	assert!(sql_tests.contains("cancel_below_request_frame_cap_is_not_statement_capped"));
}

#[test]
pub(crate) fn adr058_request_frame_limit_is_server_inbound_only() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let cli = std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read cli");
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let writer = brace_blocks_after_marker(&wire, "pub async fn write_frame")
		.into_iter()
		.next()
		.expect("write_frame")
		.1;

	assert!(daemon.contains("read_request_frame_bounded(&mut stream"));
	assert!(!cli.contains("read_request_frame_bounded"));
	assert!(!writer.contains("max_request_frame_bytes"));
	assert!(!writer.contains("RequestFrameTooLarge"));
}

#[test]
pub(crate) fn adr058_result_batch_frames_keep_existing_wire_shape() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let wire_test = std::fs::read_to_string(manifest_dir().join("tests/toron_sql_wire_test.rs"))
		.expect("read SQL wire tests");
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");

	assert!(wire.contains("ResultBatch(ResultBatch)"));
	assert!(wire.contains("pub arrow_ipc_bytes: ByteBuf"));
	assert!(wire.contains("postcard::to_allocvec(frame)"));
	assert!(wire_test.contains("toron_sql_wire_round_trips_arrow_ipc_batch"));
	assert!(sql_tests.contains("server_result_batches_not_constrained_by_inbound_request_cap"));
}

#[test]
pub(crate) fn adr058_no_new_sql_wire_variant_or_protocol_version() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let message = brace_blocks_after_marker(&wire, "pub enum SqlMessage")
		.into_iter()
		.next()
		.expect("SqlMessage")
		.1;
	let protocol = brace_blocks_after_marker(&wire, "pub enum SqlProtocolVersion")
		.into_iter()
		.next()
		.expect("SqlProtocolVersion")
		.1;

	for variant in [
		"Execute",
		"Cancel",
		"ResultBatch",
		"Complete",
		"Error",
		"Health",
		"Healthy",
	] {
		assert!(
			message.contains(variant),
			"missing SQL message variant {variant}"
		);
	}
	assert!(!message.contains("RequestTooLarge"));
	assert!(!message.contains("Ingress"));
	assert_eq!(
		message.matches("Health(").count(),
		1,
		"TORON-532 permits exactly one health request variant"
	);
	assert_eq!(
		message.matches("Healthy(").count(),
		1,
		"TORON-532 permits exactly one health response variant"
	);
	assert!(protocol.contains("Current"));
	assert!(!protocol.contains("V1"));
	assert!(!protocol.contains("V2"));
}

#[test]
pub(crate) fn adr058_no_parser_rate_queue_connection_or_cancel_drift() {
	let source = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"parser_rate",
		"ParserRate",
		"parser_queue",
		"ParserQueue",
		"prepared_statement",
		"PreparedStatement",
		"CancelTaskRegistry",
		"SqlRequestQueue",
	] {
		assert!(
			!source.contains(forbidden),
			"ADR 058 added out-of-scope parser/queue/cancel drift: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr058_request_ingress_metrics_are_bounded_and_text_free() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	assert!(observability.contains("RequestFrameTooLarge"));
	assert!(observability.contains("StatementTooLarge"));
	assert!(observability.contains("\"request_frame_too_large\""));
	assert!(observability.contains("\"statement_too_large\""));
	for line in observability.lines().filter(|line| {
		line.contains("listener_rejects_total") || line.contains("request_frame_too_large")
	}) {
		for forbidden in [
			"query_id",
			"statement_text",
			"sql_text",
			"tenant_id",
			"table_name",
		] {
			assert!(
				!line.contains(forbidden),
				"request ingress metric leaked forbidden token {forbidden}: {line}"
			);
		}
	}
	assert!(
		observability_test
			.contains("request_ingress_metrics_use_bounded_reasons_and_omit_sql_text")
	);
}

#[test]
pub(crate) fn adr058_no_worker_transport_observability_wendelon_or_nilket_change() {
	let server_source = source_files(&manifest_dir().join("src/server"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read server source"))
		.collect::<Vec<_>>()
		.join("\n");
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	for (name, source) in [
		("server", server_source.as_str()),
		("fixture worker", fixture.as_str()),
		("nilket", nilket_manifest.as_str()),
		("wendelon", wendelon_source.as_str()),
	] {
		assert!(
			!source.contains("read_request_frame_bounded"),
			"ADR 058 bounded reader leaked into {name}"
		);
		assert!(
			!source.contains("RequestFrameTooLarge"),
			"ADR 058 request frame reject leaked into {name}"
		);
		assert!(
			!source.contains("StatementTooLarge"),
			"ADR 058 statement reject leaked into {name}"
		);
	}
}

#[test]
pub(crate) fn adr058_prior_adr043_and_adr049_through_adr057_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr043_", "adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_",
		"adr056_", "adr057_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr058_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "clap", "tracing", "arrow"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr058_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("read_request_frame_bounded"));
	assert!(!worker.contains("max_request_frame_bytes"));
	assert!(!worker.contains("max_statement_bytes"));
	assert!(!worker.contains("request_frame_too_large"));
	assert!(!worker.contains("statement_too_large"));
}

#[test]
pub(crate) fn adr058_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"RequestFrameTooLargeV1",
		"SqlRequestFrameLimitV1",
		"SqlStatementLimitV1",
		"BoundedSqlIngressV1",
	]);
}
