#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, network_tls::*, observability_audit::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};
use super::{
	descriptor_sync::*, resource_governance::*, scheduler_dispatch::*, sql_ingress::*,
	tenant_admission::*, worker_lifecycle::*,
};

#[test]
pub(crate) fn adr059_reuses_existing_cancel_wire_and_rejects_control_protocol_drift() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");
	let transport = std::fs::read_to_string(manifest_dir().join("src/server/transport.rs"))
		.expect("read transport");
	let quic = std::fs::read_to_string(manifest_dir().join("src/quic.rs")).expect("read quic");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(wire.contains("CancelTask"));
	assert!(wire.contains("TaskCancelled"));
	assert!(!wire.contains("ControlMessage"));
	assert!(!transport.contains("open_uni"));
	assert!(!quic.contains("open_uni"));
	assert!(quic.contains("open_bi()"));
	assert!(server.contains("cancel_task_on_worker"));
	assert!(server.contains("MessageKind::CancelTask"));
	assert!(server.contains("MessageKind::TaskCancelled"));
}

#[test]
pub(crate) fn adr059_sql_cancel_uses_existing_cancel_frame_not_execute_grammar() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let cancel_branch = brace_blocks_after_marker(&daemon, "SqlMessage::Cancel")
		.into_iter()
		.next()
		.expect("cancel branch")
		.1;

	assert!(wire.contains("Cancel(CancelRequest)"));
	assert!(wire.contains("pub struct CancelRequest"));
	assert!(cancel_branch.contains("request.query_id"));
	assert!(cancel_branch.contains("CancellationTrigger::SqlCancel"));
	assert!(cancel_branch.contains("SqlErrorCode::Cancelled"));
	assert!(!daemon.contains("CANCEL <"));
	assert!(!cancel_branch.contains("CancellationUnsupported"));
}

#[test]
pub(crate) fn adr059_sql_cancel_is_tenant_scoped_and_not_cross_tenant_oracle() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let cancel_branch = brace_blocks_after_marker(&daemon, "SqlMessage::Cancel")
		.into_iter()
		.next()
		.expect("cancel branch")
		.1;

	assert!(cancellation.contains("ActiveQueryKey::new(tenant_id, query_id)"));
	assert!(cancel_branch.contains("tenant_id"));
	assert!(cancel_branch.contains("request.query_id"));
	assert!(cancel_branch.contains("SqlErrorCode::Cancelled"));
	assert!(!cancel_branch.contains("TableNotFound"));
	assert!(!cancel_branch.contains("NoLiveWorker"));
	assert!(
		cancellation.contains("active_query_registry_is_tenant_scoped_idempotent_and_raii_removed")
	);
}

#[test]
pub(crate) fn adr059_active_query_registry_is_tenant_scoped_and_shared_by_triggers() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(coordinator.contains("ActiveQueryRegistry"));
	assert!(cancellation.contains("pub struct ActiveQueryKey"));
	assert!(cancellation.contains("pub tenant_id: TenantId"));
	assert!(cancellation.contains("pub query_id: QueryId"));
	assert!(coordinator.contains("cancel_query_for_tenant"));
	assert!(daemon.contains("CancellationTrigger::StatementTimeout"));
	assert!(daemon.contains("CancellationTrigger::ClientDisconnect"));
	assert!(daemon.contains("CancellationTrigger::SqlCancel"));
	assert!(daemon.matches("cancel_query_for_tenant").count() >= 3);
}

#[test]
pub(crate) fn adr059_active_query_registry_is_tenant_scoped_and_raii_removed() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(cancellation.contains("pub struct ActiveQueryKey"));
	assert!(cancellation.contains("pub tenant_id: TenantId"));
	assert!(cancellation.contains("pub query_id: QueryId"));
	assert!(cancellation.contains("impl Drop for ActiveQueryGuard"));
	assert!(cancellation.contains("remove_if_same"));
	assert!(cancellation.contains("ptr_eq"));
	assert!(coordinator.contains(".active_queries"));
	assert!(coordinator.contains("register_with_downstream"));
	assert!(coordinator.contains("context.tenant_id"));
	assert!(coordinator.contains("context.query_id"));
}

#[test]
pub(crate) fn adr059_timeout_disconnect_and_sql_cancel_share_one_handle() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	for trigger in [
		"CancellationTrigger::StatementTimeout",
		"CancellationTrigger::ClientDisconnect",
		"CancellationTrigger::SqlCancel",
	] {
		assert!(daemon.contains(trigger), "missing trigger {trigger}");
	}
	assert!(daemon.matches("cancel_query_for_tenant").count() >= 3);
	assert!(!daemon.contains("cancel_query_for_timeout"));
	assert!(!daemon.contains("cancel_query_for_disconnect"));
}

#[test]
pub(crate) fn adr059_statement_timeout_fires_cancel_before_returning() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let timeout_pos = daemon
		.find("_ = &mut timeout_sleep")
		.expect("timeout branch");
	let branch = &daemon[timeout_pos..];
	let cancel_pos = branch
		.find("cancel_query_for_tenant")
		.expect("timeout cancel call");
	let error_pos = branch
		.find("SqlErrorCode::StatementTimeout")
		.expect("statement timeout error");

	assert!(cancel_pos < error_pos);
	assert!(branch[..error_pos].contains("CancellationTrigger::StatementTimeout"));
	assert!(branch[..error_pos].contains("cancellation_request_outcome(&cancellation)"));
}

#[test]
pub(crate) fn adr059_client_disconnect_before_execute_decode_sends_no_cancel() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let handler = brace_blocks_after_marker(&daemon, "async fn handle_sql_connection")
		.into_iter()
		.next()
		.expect("SQL connection handler")
		.1;
	let frame_read = handler
		.find("read_request_frame_bounded")
		.expect("bounded frame read");
	let execute_branch = handler.find("SqlMessage::Execute").expect("execute branch");
	let pre_execute = &handler[frame_read..execute_branch];
	let execute = brace_blocks_after_marker(&handler, "SqlMessage::Execute")
		.into_iter()
		.next()
		.expect("execute branch")
		.1;

	assert!(!pre_execute.contains("CancellationTrigger::ClientDisconnect"));
	assert!(!pre_execute.contains("connection.closed()"));
	assert!(execute.contains("connection.closed()"));
	assert!(execute.contains("CancellationTrigger::ClientDisconnect"));
}

#[test]
pub(crate) fn adr059_client_response_does_not_wait_for_remote_cancel_ack() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let timeout_branch = &daemon[daemon
		.find("CancellationTrigger::StatementTimeout")
		.expect("timeout trigger")..];
	let cancel_branch = brace_blocks_after_marker(&daemon, "SqlMessage::Cancel")
		.into_iter()
		.next()
		.expect("cancel branch")
		.1;

	assert!(coordinator.contains("tokio::spawn(async move"));
	assert!(coordinator.contains("REMOTE_CANCEL_ACK_TIMEOUT"));
	assert!(
		!timeout_branch[..timeout_branch
			.find("SqlErrorCode::StatementTimeout")
			.unwrap()]
			.contains("TaskCancelled")
	);
	assert!(!cancel_branch.contains("TaskCancelled"));
	assert!(!cancel_branch.contains("REMOTE_CANCEL_ACK_TIMEOUT"));
}

#[test]
pub(crate) fn adr059_worker_cancel_task_is_not_ack_only_and_tombstones_are_bounded() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(cancellation.contains("MAX_PENDING_TASK_CANCELLATIONS"));
	assert!(cancellation.contains("PENDING_TASK_CANCELLATION_TTL"));
	assert!(cancellation.contains("ActiveTaskRegistry"));
	assert!(cancellation.contains("pending"));
	assert!(server.contains("cancelled_before_start"));
	assert!(server.contains("self.active_tasks.cancel(identity)"));
	assert!(server.contains("cancelled_task_error()"));
	assert!(server.contains("partition_store.cancel_query(identity.query_id)"));
}

#[test]
pub(crate) fn adr059_remote_cancel_uses_existing_cancel_task_wire() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let wire = std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");

	assert!(wire.contains("CancelTask"));
	assert!(wire.contains("TaskCancelled"));
	assert!(coordinator.contains("cancel_task_on_worker(stream, task.identity)"));
	assert!(server.contains("MessageKind::CancelTask"));
	assert!(server.contains("MessageKind::TaskCancelled"));
}

#[test]
pub(crate) fn adr059_out_of_band_cancel_stream_is_bidirectional_for_ack() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let transport = std::fs::read_to_string(manifest_dir().join("src/server/transport.rs"))
		.expect("read transport");
	let quic = std::fs::read_to_string(manifest_dir().join("src/quic.rs")).expect("read quic");

	assert!(transport.contains("bidirectional stream"));
	assert!(quic.contains("open_bi()"));
	assert!(!quic.contains("open_uni"));
	assert!(coordinator.contains("transport.connect_worker(&task.worker)"));
	assert!(coordinator.contains("cancel_task_on_worker(stream, task.identity)"));
}

#[test]
pub(crate) fn adr059_no_control_message_uni_stream_or_control_endpoint() {
	assert_toron_sources_do_not_contain(&[
		"ControlMessage",
		"open_uni",
		"CancelControl",
		"worker_control",
		"control_endpoint",
	]);
}

#[test]
pub(crate) fn adr059_worker_handles_cancel_stream_concurrently_with_task_stream() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let worker_loop = brace_blocks_after_marker(&daemon, "async fn run_quic_worker_accept_loop")
		.into_iter()
		.next()
		.expect("worker accept loop")
		.1;

	assert!(worker_loop.contains("connection_tasks.lock().await.spawn(async move"));
	assert!(worker_loop.contains("QuicEndpoint::accept_stream_on(&connection)"));
	assert!(server.contains("MessageKind::StartTask"));
	assert!(server.contains("MessageKind::CancelTask"));
}

#[test]
pub(crate) fn adr059_worker_active_registry_keyed_by_task_identity_raii_deregistered() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(cancellation.contains("active: HashMap<TaskIdentity, ActiveTaskState>"));
	assert!(cancellation.contains("signal: TaskCancellationSignal"));
	assert!(cancellation.contains("impl Drop for ActiveTaskGuard"));
	assert!(cancellation.contains("inner.active.remove(&self.identity)"));
	assert!(server.contains("let active_task = self.active_tasks.register(identity);"));
}

#[test]
pub(crate) fn adr059_worker_cancel_task_not_ack_only_and_idempotent() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let cancel_branch = brace_blocks_after_marker(&server, "MessageKind::CancelTask")
		.into_iter()
		.next()
		.expect("cancel task branch")
		.1;

	assert!(cancel_branch.contains("self.active_tasks.cancel(identity)"));
	assert!(cancel_branch.contains("partition_store.cancel_query(identity.query_id)"));
	assert!(cancel_branch.contains("MessageKind::TaskCancelled"));
	assert!(cancel_branch.contains("TaskCancelledPayload { identity }"));
}

#[test]
pub(crate) fn adr059_cancel_before_start_task_tombstones_and_aborts_on_registration() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(cancellation.contains("insert_pending(identity)"));
	assert!(cancellation.contains("cancelled_before_start"));
	assert!(server.contains("if active_task.cancelled_before_start"));
	assert!(server.contains("record_cancelled().await"));
	assert!(server.contains("cancelled_task_error()"));
	assert!(server.contains("send_task_failed(&mut stream, &mut phase, identity, error).await?"));
	assert!(server.contains("cancel_task_before_registration_tombstones_and_prevents_execution"));
}

#[test]
pub(crate) fn adr059_pending_cancel_tombstone_is_bounded_and_evicted() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");

	assert!(cancellation.contains("MAX_PENDING_TASK_CANCELLATIONS"));
	assert!(cancellation.contains("PENDING_TASK_CANCELLATION_TTL"));
	assert!(cancellation.contains("pending_order: VecDeque<TaskIdentity>"));
	assert!(cancellation.contains("while self.pending.len() > MAX_PENDING_TASK_CANCELLATIONS"));
	assert!(cancellation.contains("prune_pending"));
	assert!(
		cancellation.contains("active_task_cancel_before_registration_is_bounded_and_consumed")
	);
}

#[test]
pub(crate) fn adr059_remote_cancel_fanout_is_bounded_and_records_aggregate_outcomes() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert!(coordinator.contains("MAX_REMOTE_CANCEL_FANOUTS"));
	assert!(coordinator.contains("Semaphore::new(MAX_REMOTE_CANCEL_FANOUTS)"));
	assert!(coordinator.contains("try_acquire_owned()"));
	assert!(coordinator.contains("tokio::time::timeout"));
	assert!(coordinator.contains("CancellationOutcome::RemoteAck"));
	assert!(coordinator.contains("CancellationOutcome::RemoteTimeout"));
	assert!(coordinator.contains("CancellationOutcome::RemoteError"));
	assert!(
		observability
			.contains("query_cancellations_total{{trigger=\\\"{}\\\",outcome=\\\"{}\\\"}}")
	);
}

#[test]
pub(crate) fn adr059_cancel_fanout_is_bounded_background_cleanup() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(coordinator.contains("MAX_REMOTE_CANCEL_FANOUTS"));
	assert!(coordinator.contains("cancel_fanout_permits"));
	assert!(coordinator.contains("try_acquire_owned()"));
	assert!(coordinator.contains("tokio::spawn(async move"));
	assert!(coordinator.contains("REMOTE_CANCEL_CONNECT_TIMEOUT"));
	assert!(coordinator.contains("REMOTE_CANCEL_ACK_TIMEOUT"));
}

#[test]
pub(crate) fn adr059_abort_is_drop_at_await_with_pre_publish_suppression() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let start_task = brace_blocks_after_marker(&server, "MessageKind::StartTask")
		.into_iter()
		.next()
		.expect("StartTask arm")
		.1;

	assert!(start_task.contains("tokio::select!"));
	assert!(start_task.contains("execute_task_local_with_telemetry"));
	assert!(start_task.contains("active_task.signal.cancelled()"));
	assert!(start_task.contains("publish_task_output"));
	assert!(start_task.contains("active_task.signal.is_cancelled()"));
	assert!(!server.contains("tokio_util::sync::CancellationToken"));
}

#[test]
pub(crate) fn adr059_local_cleanup_runs_when_remote_cancel_fails() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(coordinator.contains("scheduler.cancel_query(context.query_id)"));
	assert!(coordinator.contains("CancellationOutcome::RemoteError"));
	assert!(coordinator.contains("remote cancel connect failed"));
	assert!(server.contains("partition_store.cancel_query(identity.query_id)"));
}

#[test]
pub(crate) fn adr059_pre_admission_rejects_emit_no_cancel() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let execute_query =
		brace_blocks_after_marker(&coordinator, "pub async fn execute_query_with_context")
			.into_iter()
			.next()
			.expect("execute query")
			.1;
	let admit_query = brace_blocks_after_marker(&coordinator, "async fn admit_query")
		.into_iter()
		.next()
		.expect("admit query")
		.1;
	let admit_pos = execute_query.find("admit_query(&context)").expect("admit");
	let register_pos = execute_query
		.find(".active_queries")
		.expect("active query register");

	assert!(admit_pos < register_pos);
	assert!(!admit_query.contains("CancelTask"));
	assert!(!admit_query.contains("cancel_query_for_tenant"));
}

#[test]
pub(crate) fn adr059_wendelon_storage_cancellation_claim_is_retargeted_and_metrics_identity_free() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let adr060 = std::fs::read_to_string(
		manifest_dir().join("plans/adr/060_wendelon_table_query_cancellation_consumer.md"),
	)
	.expect("read adr060");

	assert!(cancellation.contains("WendelonTableQueryAttempt"));
	assert!(cancellation.contains("WendelonTableQueryAttempt"));
	assert!(cancellation.contains("cancel_table_query_attempt"));
	assert!(adr060.contains("no-Wendelon caveat"));
	assert!(adr060.contains("best-effort"));
	assert!(adr060.contains("checkpoint-bounded"));
	for forbidden in [
		"query_cancellations_total{query_id",
		"query_cancellations_total{task_id",
		"query_cancellations_total{worker_id",
		"query_cancellations_total{tenant_id",
		"query_cancellations_total{table",
		"query_cancellations_total{endpoint",
	] {
		assert!(!observability.contains(forbidden), "{forbidden} leaked");
	}
}

#[test]
pub(crate) fn adr059_prior_adr043_and_adr049_through_adr058_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr043_", "adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_",
		"adr056_", "adr057_", "adr058_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr059_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr059_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	for forbidden in [
		"ActiveTaskRegistry",
		"ActiveQueryRegistry",
		"CancellationTrigger",
		"query_cancellations_total",
		"cancel_query_for_tenant",
	] {
		assert!(
			!worker.contains(forbidden),
			"fixture worker gained {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr059_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ActiveTaskRegistryV1",
		"ActiveQueryRegistryV1",
		"TaskCancellationSignalV1",
		"CancellationTriggerV1",
		"CancellationOutcomeV1",
		"CancellationMetricsV1",
		"RemoteTaskCancellationV1",
	]);
}

#[test]
pub(crate) fn adr060_consumes_wendelon_adr073_without_new_wire() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let wendelon_client = std::fs::read_to_string(
		workspace_root_dir().join("crates/wendelon/crates/wendelon-client/src/lib.rs"),
	)
	.expect("read wendelon-client facade");

	assert!(cancellation.contains("WendelonTableQueryAttempt"));
	assert!(wendelon_client.contains("cancel_table_query_attempt"));
	assert!(!wire.contains("TableQueryCancel"));
	assert!(!sql_wire.contains("TableQueryCancel"));
	assert_toron_sources_do_not_contain(&["TABLE_QUERY_CANCEL_REQ", "TABLE_QUERY_CANCEL_RESP"]);
}

#[test]
pub(crate) fn adr060_client_hook_exposes_attempt_without_retry_duplication() {
	let facade = std::fs::read_to_string(
		workspace_root_dir().join("crates/wendelon/crates/wendelon-client/src/lib.rs"),
	)
	.expect("read wendelon-client facade");
	let core =
		std::fs::read_to_string(workspace_root_dir().join("crates/wendelon/src/client/core.rs"))
			.expect("read wendelon client core");

	assert!(facade.contains("table_query_stream_with_runtime_filter_and_attempt_hook"));
	assert!(core.contains("pub struct TableQueryAttempt"));
	assert!(core.contains("attempt_request.meta.request_id = TableQueryRequestId::random();"));
	assert!(core.contains("hook(TableQueryAttempt"));
	assert!(core.contains("table_query_once(target, &attempt_request, attempt_guard)"));
	assert!(
		!std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
			.unwrap()
			.contains("TableQueryRequestId::random()")
	);
}

#[test]
pub(crate) fn adr060_hook_returns_or_holds_raii_attempt_guard() {
	let core =
		std::fs::read_to_string(workspace_root_dir().join("crates/wendelon/src/client/core.rs"))
			.expect("read wendelon client core");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");

	assert!(core.contains("pub type TableQueryAttemptGuard"));
	assert!(core.contains("_table_query_attempt_guard: Option<TableQueryAttemptGuard>"));
	assert!(storage.contains("Ok(Box::new(guard) as wendelon_client::TableQueryAttemptGuard)"));
	assert!(cancellation.contains("pub struct DownstreamAttemptGuard"));
	assert!(cancellation.contains("impl Drop for DownstreamAttemptGuard"));
}

#[test]
pub(crate) fn adr060_attempt_identity_is_target_tenant_request_id_only() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let attempt = brace_blocks_after_marker(&cancellation, "pub struct WendelonTableQueryAttempt")
		.into_iter()
		.next()
		.expect("attempt struct")
		.1;

	for required in [
		"pub target: NodeId",
		"pub tenant_id: WendelonTenantId",
		"pub request_id: TableQueryRequestId",
	] {
		assert!(attempt.contains(required), "missing {required}");
	}
	for forbidden in [
		"QueryId",
		"TaskIdentity",
		"SessionId",
		"TableId",
		"fingerprint",
		"endpoint",
		"sql",
	] {
		assert!(
			!attempt.contains(forbidden),
			"{forbidden} leaked into attempt identity"
		);
	}
}

#[test]
pub(crate) fn adr060_no_toron_identity_crosses_wendelon_cancel_wire() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let fanout = brace_blocks_after_marker(&cancellation, "tokio::spawn(async move")
		.into_iter()
		.find(|(_, block)| block.contains("cancel_table_query_attempt"))
		.expect("downstream fanout")
		.1;

	assert!(fanout.contains("registered.attempt.target"));
	assert!(fanout.contains("registered.attempt.tenant_id"));
	assert!(fanout.contains("registered.attempt.request_id"));
	for forbidden in [
		"query_id",
		"TaskIdentity",
		"SessionId",
		"TableId",
		"fingerprint",
		"endpoint",
	] {
		assert!(
			!fanout.contains(forbidden),
			"{forbidden} crossed cancel fanout"
		);
	}
}

#[test]
pub(crate) fn adr060_attempt_registers_before_table_query_write() {
	let core =
		std::fs::read_to_string(workspace_root_dir().join("crates/wendelon/src/client/core.rs"))
			.expect("read wendelon client core");
	let hook_pos = core.find("hook(TableQueryAttempt").expect("hook call");
	let request_pos = core
		.find("table_query_once(target, &attempt_request, attempt_guard)")
		.expect("table query once call");
	let table_query_once = brace_blocks_after_marker(&core, "async fn table_query_once")
		.into_iter()
		.next()
		.expect("table_query_once")
		.1;

	assert!(hook_pos < request_pos);
	assert!(table_query_once.contains("write_frame(&mut *stream"));
}

#[test]
pub(crate) fn adr060_retry_redirect_replaces_attempt_registration() {
	let core =
		std::fs::read_to_string(workspace_root_dir().join("crates/wendelon/src/client/core.rs"))
			.expect("read wendelon client core");
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");

	assert!(core.contains("for attempt in 0..DATA_RETRY_BUDGET"));
	assert!(core.contains("let attempt_guard = match hook.as_deref_mut()"));
	assert!(core.contains("QueryOutcome::Redirect(retry)"));
	assert!(cancellation.contains("wendelon_redirect_retry_replaces_attempt_registration"));
}

#[test]
pub(crate) fn adr060_raii_removes_attempt_on_terminal_paths() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");

	assert!(cancellation.contains("registration_id"));
	assert!(cancellation.contains("inner.attempts.remove(&self.registration_id)"));
	assert!(
		cancellation.contains("downstream_attempt_raii_removes_on_success_error_redirect_and_drop")
	);
}

#[test]
pub(crate) fn adr060_cancel_is_parent_fired_not_scan_watcher_primary() {
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");

	assert!(cancellation.contains("ActiveQueryHandle"));
	assert!(cancellation.contains("ActiveTaskRegistry"));
	assert!(cancellation.contains("cancel_registered_attempts(trigger.into())"));
	assert!(cancellation.contains("DownstreamCancellationTrigger::WorkerCancelTask"));
	assert!(!storage.contains("tokio::spawn"));
	assert!(!storage.contains("AbortOnDrop"));
}

#[test]
pub(crate) fn adr060_coordinator_cancel_triggers_share_downstream_path() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(coordinator.contains("DownstreamAttemptRegistry::new"));
	assert!(coordinator.contains("downstream_cancellation_context"));
	for trigger in [
		"CancellationTrigger::StatementTimeout",
		"CancellationTrigger::ClientDisconnect",
		"CancellationTrigger::SqlCancel",
	] {
		assert!(daemon.contains(trigger), "missing trigger {trigger}");
	}
}

#[test]
pub(crate) fn adr060_worker_cancel_task_fires_downstream_before_drop_at_await() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let cancel_pos = cancellation
		.find("DownstreamCancellationTrigger::WorkerCancelTask")
		.expect("worker downstream cancel");
	let signal_pos = cancellation[cancel_pos..]
		.find("state.signal.cancel()")
		.expect("signal cancel");

	assert!(signal_pos > 0);
	assert!(server.contains("Some(active_task.downstream.context())"));
	assert!(server.contains("active_task.signal.cancelled()"));
}

#[test]
pub(crate) fn adr060_pre_admission_rejects_emit_no_wendelon_cancel() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let execute_query =
		brace_blocks_after_marker(&coordinator, "pub async fn execute_query_with_context")
			.into_iter()
			.next()
			.expect("execute query")
			.1;
	let admit_pos = execute_query.find("admit_query(&context)").expect("admit");
	let downstream_pos = execute_query
		.find("DownstreamAttemptRegistry::new")
		.expect("downstream registry");

	assert!(admit_pos < downstream_pos);
}

#[test]
pub(crate) fn adr060_already_cancelled_before_scan_sends_no_table_query_req() {
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let scan = brace_blocks_after_marker(&storage, "impl StorageScan for WendelonStorageScan")
		.into_iter()
		.next()
		.expect("wendelon scan impl")
		.1;
	let cancelled_pos = scan.find("is_cancelled").expect("cancelled check");
	let query_pos = scan
		.find("table_query_stream_with_runtime_filter_and_attempt_hook")
		.expect("table query call");

	assert!(cancelled_pos < query_pos);
	assert!(scan.contains("return Err(StorageError::Cancelled)"));
}

#[test]
pub(crate) fn adr060_wendelon_query_cancelled_maps_to_toron_cancelled() {
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(storage.contains("QUERY_CANCELLED"));
	assert!(storage.contains("StorageError::Cancelled"));
	assert!(server.contains("StorageError::Cancelled) => TaskFailureKind::Cancelled"));
	assert!(daemon.contains("StorageError::Cancelled"));
}

#[test]
pub(crate) fn adr060_downstream_cancel_fanout_is_bounded_and_nonblocking() {
	let cancellation =
		std::fs::read_to_string(manifest_dir().join("src/distributed/cancellation.rs"))
			.expect("read cancellation");

	assert!(cancellation.contains("MAX_DOWNSTREAM_CANCEL_FANOUTS"));
	assert!(cancellation.contains("DOWNSTREAM_CANCEL_ACK_TIMEOUT"));
	assert!(cancellation.contains("try_acquire_owned()"));
	assert!(cancellation.contains("tokio::spawn(async move"));
	assert!(
		cancellation
			.contains("downstream_cancel_fanout_timeout_does_not_delay_sql_cancel_response")
	);
}

#[test]
pub(crate) fn adr060_stream_drop_is_fallback_not_primary() {
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let wendelon_router =
		std::fs::read_to_string(workspace_root_dir().join("crates/wendelon/src/network/router.rs"))
			.expect("read wendelon router");

	assert!(storage.contains("table_query_stream_with_runtime_filter_and_attempt_hook"));
	assert!(storage.contains("register_wendelon_table_query_attempt"));
	assert!(wendelon_router.contains("stream_dropped"));
	assert!(!storage.contains("stream_dropped"));
}

#[test]
pub(crate) fn adr060_metrics_are_aggregate_only() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	assert!(observability.contains("toron_downstream_storage_cancellations_total"));
	assert!(observability.contains("DownstreamCancellationTrigger::ALL"));
	assert!(observability.contains("DownstreamCancellationOutcome::ALL"));
	assert!(
		observability_test
			.contains("downstream_cancellation_metrics_are_aggregate_trigger_outcome_only")
	);
	for forbidden in [
		"toron_downstream_storage_cancellations_total{query_id",
		"toron_downstream_storage_cancellations_total{task_id",
		"toron_downstream_storage_cancellations_total{worker_id",
		"toron_downstream_storage_cancellations_total{tenant_id",
		"toron_downstream_storage_cancellations_total{table",
		"toron_downstream_storage_cancellations_total{endpoint",
	] {
		assert!(!observability.contains(forbidden), "{forbidden} leaked");
	}
}

#[test]
pub(crate) fn adr060_no_writes_cdc_admin_agg_or_legacy_query_scope() {
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let wendelon_core =
		std::fs::read_to_string(workspace_root_dir().join("crates/wendelon/src/client/core.rs"))
			.expect("read wendelon core");

	assert!(storage.contains("impl StorageScan for WendelonStorageScan"));
	assert!(storage.contains("table_query_stream_with_runtime_filter_and_attempt_hook"));
	assert!(!storage.contains("aggregate_with_attempt_hook"));
	assert!(!storage.contains("write_with_attempt_hook"));
	assert!(!storage.contains("delete_with_attempt_hook"));
	assert!(!storage.contains("cdc_subscribe_with_attempt_hook"));
	assert!(wendelon_core.contains(
		"MsgType::QUERY_RESP,\n\t\t\tStreamingQueryMetaMode::StrictContinuations,\n\t\t\tNone,"
	));
}

#[test]
pub(crate) fn adr060_no_control_ring_uni_stream_sidecar_or_sql_grammar() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");

	assert!(coordinator.contains("DownstreamAttemptRegistry"));
	assert!(!sql_wire.contains("CancelWendelon"));
	assert_toron_sources_do_not_contain(&[
		"open_uni",
		"ControlMessage",
		"storage_cancel_listener",
		"wendelon_cancel_listener",
		"CancelWendelon",
	]);
}

#[test]
pub(crate) fn adr060_retargets_adr059_no_wendelon_claim_narrowly() {
	let adr059_guard = architecture_corpus_source();
	let adr060 = std::fs::read_to_string(
		manifest_dir().join("plans/adr/060_wendelon_table_query_cancellation_consumer.md"),
	)
	.expect("read adr060");

	assert!(adr059_guard.contains(
		"adr059_wendelon_storage_cancellation_claim_is_retargeted_and_metrics_identity_free"
	));
	assert!(adr060.contains("best-effort and checkpoint-bounded"));
	assert!(adr060.contains("no broader claim"));
}

#[test]
pub(crate) fn adr060_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr060_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	for forbidden in [
		"DownstreamAttemptRegistry",
		"WendelonTableQueryAttempt",
		"toron_downstream_storage_cancellations_total",
		"cancel_table_query_attempt",
	] {
		assert!(
			!worker.contains(forbidden),
			"fixture worker gained {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr060_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"DownstreamAttemptRegistryV1",
		"DownstreamCancellationContextV1",
		"DownstreamCancellationMetricsV1",
		"WendelonTableQueryAttemptV1",
		"TableQueryCancelSenderV1",
		"StorageCancellationV1",
	]);
}

#[test]
pub(crate) fn adr060_prior_adr049_to_059_guards_continue_to_pass_except_retarget() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_", "adr056_",
		"adr057_", "adr058_", "adr059_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains(
		"adr059_wendelon_storage_cancellation_claim_is_retargeted_and_metrics_identity_free"
	));
}
