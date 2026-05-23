#![allow(unused_imports)]

use super::support::*;
use super::{
	core_invariants::*, network_tls::*, observability_audit::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};

#[test]
pub(crate) fn distributed_identity_keeps_morsel_id_worker_local() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src/distributed"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("MorselId"))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"MorselId found in distributed identity/task surfaces:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

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
pub(crate) fn task_failure_kind_keeps_partition_store_errors_separate() {
	let source = std::fs::read_to_string(manifest_dir().join("src/error/distributed.rs"))
		.expect("read distributed error source");
	let task_failure = source
		.split("pub enum TaskFailureKind")
		.nth(1)
		.and_then(|rest| rest.split("impl TaskFailureKind").next())
		.expect("TaskFailureKind block");

	assert_eq!(enum_variant_count(task_failure), 7);
	assert!(!task_failure.contains("PartitionMissing"));
	assert!(!task_failure.contains("StaleAttempt"));
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
pub(crate) fn server_and_distributed_paths_have_no_sleep_mock() {
	let manifest_dir = manifest_dir();
	let offenders = ["src/server", "src/distributed"]
		.into_iter()
		.flat_map(|dir| source_lines(&manifest_dir.join(dir)))
		.filter(|(_, _, line)| line.contains("tokio::time::sleep"))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"sleep mock found in runtime path:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
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
pub(crate) fn worker_runtime_does_not_bind_logical_plans() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src/server"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("bind(") || line.contains("crate::binder::bind"))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"worker runtime still binds logical plans:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn runtime_transport_is_not_duplex_only() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			let test_only_duplex_import =
				path.ends_with("src/server/mod.rs") && line.contains("duplex as test_duplex");
			!test_only_duplex_import
				&& (line.contains("tokio::io::duplex") || line.contains("MockZeroCopyTransport"))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"duplex-only transport marker found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn scheduler_dispatch_loop_uses_acceptance_barrier() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let dispatch_loop = source
		.split("pub async fn dispatch_loop")
		.nth(1)
		.and_then(|rest| rest.split("fn stage_barrier_open").next())
		.expect("dispatch_loop block");
	let barrier = source
		.split("fn stage_barrier_open")
		.nth(1)
		.and_then(|rest| rest.split("fn failure_retryable").next())
		.expect("stage_barrier_open block");

	assert!(dispatch_loop.contains("self.stage_barrier_open"));
	assert!(barrier.contains("AcceptanceState::LiveAccepted"));
	assert!(!barrier.contains("AttemptState::Completed"));
}

#[test]
pub(crate) fn g16_no_non_pinned_join_algorithms_in_runtime_paths() {
	let manifest_dir = manifest_dir();
	let forbidden = [
		"SortMergeJoin",
		"NestedLoopJoin",
		"CrossJoin",
		"SemiJoin",
		"AntiJoin",
		"OuterJoin",
		"LeftJoin",
		"RightJoin",
		"FullJoin",
		"HashJoin",
		"BroadcastJoin",
	];
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, _)| {
			let relative = path.strip_prefix(&manifest_dir).unwrap_or(path);
			relative == Path::new("src/exec.rs") || relative.starts_with("src/distributed")
		})
		.filter(|(_, _, line)| forbidden.iter().any(|marker| line.contains(marker)))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"non-pinned join algorithm marker found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn g17_join_ir_surface_has_single_join_variant_and_two_strategies() {
	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR");
	let execution_plan = execution_source
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan block");
	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR");
	let join_kind = logical_source
		.split("pub enum JoinKind")
		.nth(1)
		.and_then(|rest| rest.split("pub enum JoinStrategy").next())
		.expect("JoinKind block");
	let join_strategy = logical_source
		.split("pub enum JoinStrategy")
		.nth(1)
		.and_then(|rest| rest.split("pub enum InsertLiteral").next())
		.expect("JoinStrategy block");
	let logical_plan = logical_source
		.split("pub enum LogicalPlan")
		.nth(1)
		.expect("LogicalPlan block");
	let join_kind_variants = join_kind
		.lines()
		.filter_map(|line| {
			let trimmed = line.trim();
			trimmed
				.ends_with(',')
				.then(|| trimmed.trim_end_matches(',').to_string())
		})
		.collect::<Vec<_>>();

	assert_eq!(execution_plan.matches("Join {").count(), 1);
	assert_eq!(logical_plan.matches("Join {").count(), 1);
	assert_eq!(
		join_kind_variants,
		vec![
			"Inner",
			"LeftOuter",
			"RightOuter",
			"FullOuter",
			"LeftSemi",
			"RightSemi",
			"LeftAnti",
			"RightAnti",
		]
	);
	assert_eq!(enum_variant_count(join_kind), 8);
	assert_eq!(enum_variant_count(join_strategy), 2);
	assert!(join_strategy.contains("HashPartitioned"));
	assert!(join_strategy.contains("Broadcast"));
}

#[test]
pub(crate) fn g18_scheduler_uses_multi_upstream_stage_barrier() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let barrier = source
		.split("fn stage_barrier_open")
		.nth(1)
		.and_then(|rest| rest.split("fn materialize_task_spec").next())
		.expect("stage_barrier_open block");

	assert!(source.contains("pub upstream_stages: Vec<StageId>"));
	assert!(!source.contains("upstream_stage: Option"));
	assert!(barrier.contains("upstream_stages.iter().all"));
}

#[test]
pub(crate) fn g19_join_hash_spill_uses_m10_async_file_ops() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read execution source");
	let spill = source
		.split("struct SpilledJoinBuild")
		.nth(1)
		.and_then(|rest| rest.split("fn encode_join_spill_page").next())
		.expect("SpilledJoinBuild block");

	assert!(source.contains("use wendelon::storage::{AsyncFileOps"));
	assert!(spill.contains("Arc<dyn AsyncFileOps>"));
	assert!(spill.contains(".open("));
	assert!(spill.contains(".write_plain_at("));
	assert!(spill.contains(".read_plain_at("));
	assert!(spill.contains(".fdatasync("));
	assert!(spill.contains(".unlink("));
	for forbidden in [
		"std::fs::remove_file",
		"OpenOptions::new",
		".seek(",
		".read_exact(",
		".write_all(",
	] {
		assert!(
			!spill.contains(forbidden),
			"join hash spill bypasses AsyncFileOps with {forbidden}"
		);
	}
}

#[test]
pub(crate) fn broadcast_join_fragmenter_input_shape_is_architecture_guarded() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let broadcast = source
		.split("fn fragment_broadcast_join_plan")
		.nth(1)
		.and_then(|rest| rest.split("fn producer_task").next())
		.expect("broadcast fragmenter block");

	assert_eq!(broadcast.matches("TaskInputBinding::Scan").count(), 1);
	assert!(broadcast.contains("share: Some(ScanShare"));
	assert_eq!(
		broadcast
			.matches("TaskInputBinding::ShuffleConsumer")
			.count(),
		1
	);
	assert!(broadcast.contains("partition_id: PartitionId::new(0)"));
	assert!(!broadcast.contains("share: None"));
}

#[test]
pub(crate) fn adr049_sql_tenant_source_is_authenticated_peer_identity() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs")).expect("read TLS");

	assert!(daemon.contains("SqlTenantSource::AuthenticatedPeer"));
	assert!(daemon.contains("tenant_source.resolve(&connection)"));
	assert!(tls.contains("pub fn accepted_sql_tenant("));
	assert!(tls.contains(".peer_identity()"));
	assert!(tls.contains("downcast::<Vec<CertificateDer<'static>>>()"));
	assert!(tls.contains("extract_dns_sans(leaf)"));
}

#[test]
pub(crate) fn adr049_loopback_sql_retains_config_tenant_fallback() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");

	assert!(daemon.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
	assert!(daemon.contains("QuicEndpoint::bind_loopback(sql_config.listener_bind)"));
}

#[test]
pub(crate) fn adr049_remote_sql_never_falls_back_to_config_tenant() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let resolve = brace_blocks_after_marker(&daemon, "fn resolve(&self, connection")
		.into_iter()
		.next()
		.expect("SqlTenantSource::resolve")
		.1;

	assert!(daemon.contains("SqlTenantSource::AuthenticatedPeer"));
	assert!(daemon.contains("policy.cluster_domain().to_string()"));
	assert!(resolve.contains("accepted_sql_tenant(connection, cluster_domain)"));
	assert!(!resolve.contains("config.node.tenant_id"));
}

#[test]
pub(crate) fn adr049_sql_client_san_requires_canonical_numeric_tenant() {
	let admitted = admit_san(
		"client-7.cluster.local",
		ListenerRole::Sql,
		DEFAULT_CLUSTER_DOMAIN,
	)
	.expect("admit tenant 7 client");
	assert_eq!(
		admitted,
		PeerKind::Client {
			principal: "7".to_string(),
			tenant_id: TenantId(7),
		}
	);

	for san in [
		"client-0.cluster.local",
		"client-007.cluster.local",
		"client-+7.cluster.local",
		"client-0x7.cluster.local",
	] {
		assert!(
			admit_san(san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err(),
			"{san} should not be admitted"
		);
	}
}

#[test]
pub(crate) fn adr049_sql_client_san_rejects_non_numeric_principal_for_tenant() {
	for san in [
		"client-analytics.cluster.local",
		"client-tenant7.cluster.local",
		"client-seven.cluster.local",
	] {
		assert!(
			matches!(
				admit_san(san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN),
				Err(toron::network::tls::TlsError::SanShapeRejected)
			),
			"{san} should reject as a tenant route"
		);
	}
}

#[test]
pub(crate) fn adr049_sql_listener_still_rejects_sibling_daemon_identity() {
	let worker_id = WorkerId::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000);
	let daemon_san = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	assert!(admit_san(&daemon_san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err());
	assert!(
		admit_san(
			&daemon_san,
			ListenerRole::WorkerTask,
			DEFAULT_CLUSTER_DOMAIN
		)
		.is_ok()
	);
}

#[test]
pub(crate) fn adr049_quic_peer_identity_extracted_before_sql_frame_read() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let loop_body = brace_blocks_after_marker(&daemon, "async fn run_sql_accept_loop")
		.into_iter()
		.next()
		.expect("run_sql_accept_loop")
		.1;

	let resolve_pos = loop_body
		.find("tenant_source.resolve(&connection)")
		.expect("tenant extraction");
	let stream_pos = loop_body
		.find("QuicEndpoint::accept_stream_on(&connection)")
		.expect("stream accept");
	assert!(resolve_pos < stream_pos);
	assert!(!loop_body[..stream_pos].contains("read_sql_frame"));
}

#[test]
pub(crate) fn adr049_peer_kind_client_carries_tenant_id() {
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs")).expect("read TLS");
	let peer_kind = brace_blocks_after_marker(&tls, "pub enum PeerKind")
		.into_iter()
		.next()
		.expect("PeerKind")
		.1;

	assert!(peer_kind.contains("Client {"));
	assert!(peer_kind.contains("principal: String"));
	assert!(peer_kind.contains("tenant_id: TenantId"));
}

#[test]
pub(crate) fn adr049_coordinator_scopes_catalog_before_binding() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let body = brace_blocks_after_marker(&coordinator, "async fn execute_query_with_admission")
		.into_iter()
		.next()
		.expect("execute_query_with_admission")
		.1;

	let scoped_pos = body
		.find("Arc::clone(&self.catalog).scoped(context.tenant_id)")
		.expect("scoped catalog");
	let bind_pos = body.find("bind(plan, catalog.as_ref()").expect("bind");
	assert!(scoped_pos < bind_pos);
	assert!(!body.contains("bind(plan, self.catalog.as_ref()"));
}

#[test]
pub(crate) fn adr049_descriptor_registry_keys_refs_and_descriptors_by_tenant() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	for required in [
		"by_ref: HashMap<(TenantId, TableRef), TableId>",
		"by_id: HashMap<(TenantId, TableId), TableBinding>",
		"descriptors: HashMap<(TenantId, TableId), wendelon_client::TableSchemaDescriptor>",
		"finalized_drops: HashMap<(TenantId, TableId), DropOperationId>",
		"register_descriptor_for_tenant",
	] {
		assert!(
			catalog.contains(required),
			"missing tenant-keyed token {required}"
		);
	}
}

#[test]
pub(crate) fn adr049_create_and_drop_table_use_scoped_tenant() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("fn scoped(self: Arc<Self>, tenant_id: TenantId)"));
	assert!(catalog.contains("TableSchemaUpdateRequest::create("));
	assert!(catalog.contains("wendelon_tenant_id(self.tenant_id)"));
	assert!(catalog.contains("tenant_id: wendelon_tenant_id(self.tenant_id)"));
	assert!(catalog.contains(".consume_finalized_drop("));
	assert!(catalog.contains("self.tenant_id,"));
	assert!(catalog.contains("remove_table_for_tenant(self.tenant_id"));
}

#[test]
pub(crate) fn adr049_storage_descriptor_lookup_uses_request_tenant() {
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");

	assert_eq!(
		storage
			.matches(".descriptor(request.tenant_id, request.table_id)")
			.count(),
		3
	);
	assert!(storage.contains("mark_finalized_drop(request.tenant_id, request.table_id"));
}

#[test]
pub(crate) fn adr049_task_context_and_storage_requests_preserve_tenant() {
	let context = std::fs::read_to_string(manifest_dir().join("src/distributed/context.rs"))
		.expect("read task context");
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");

	assert!(context.contains("pub tenant_id: TenantId"));
	assert!(context.contains("context.tenant_id"));
	for required in [
		"tenant_id: self.context.tenant_id",
		"tenant_id: context.tenant_id",
	] {
		assert!(
			exec.contains(required) || coordinator.contains(required),
			"missing tenant propagation token {required}"
		);
	}
}

#[test]
pub(crate) fn adr049_no_sql_wire_tenant_claim_or_message_variant() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let cli = std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI");

	assert!(!wire.contains("tenant_id"));
	assert!(!wire.contains("TenantRoute"));
	assert!(!wire.contains("ClientIdentity"));
	assert!(!cli.contains("--tenant"));
	for forbidden in [
		"TORON_SQL_TOKEN",
		"Authorization",
		"Bearer",
		"JWT",
		"OIDC",
		"SPIFFE",
	] {
		assert!(!wire.contains(forbidden), "SQL wire contains {forbidden}");
	}
}

#[test]
pub(crate) fn adr049_no_new_sql_error_codes() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let error_codes = brace_blocks_after_marker(&wire, "pub enum SqlErrorCode")
		.into_iter()
		.next()
		.expect("SqlErrorCode")
		.1;

	for forbidden in [
		"Tls",
		"Auth",
		"Certificate",
		"TenantRoute",
		"ClientIdentity",
	] {
		assert!(
			!error_codes.contains(forbidden),
			"SqlErrorCode gained ADR 049-forbidden token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr049_no_per_tenant_metric_labels() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert_bounded_resource_tenant_labels_only(&observability);
	assert!(!observability.contains("tenant=\""));
	assert!(observability.contains("reason=\\\"{}\\\""));
}

#[test]
pub(crate) fn adr049_no_wendelon_production_changes() {
	let wendelon_source = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"accepted_sql_tenant",
		"SqlTenantSource",
		"client-<tenant_id>",
		"TenantRoute",
	] {
		assert!(
			!wendelon_source.contains(forbidden),
			"Wendelon source contains Toron ADR 049 token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr049_prior_adr04x_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr049_nilket_root_remains_runtime_free() {
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
pub(crate) fn adr049_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	assert!(!fixture.contains("tenant_id"));
	assert!(!fixture.contains("TlsConfig"));
	assert!(!fixture.contains("daemon::"));
}

#[test]
pub(crate) fn adr049_new_types_are_brand_clean_and_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronTenantRoute",
		"ToronClientIdentity",
		"ToronAcceptedSqlConnection",
		"TenantRouteV1",
		"ClientIdentityV1",
		"AcceptedSqlConnectionV1",
	]);
}

#[test]
pub(crate) fn adr050_tenant_budget_policy_config_is_node_local() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"pub tenant_budgets: TenantBudgetPolicy",
		"tenant_budgets: Option<RawTenantBudgetPolicy>",
		"[tenant_budgets.default]",
		"[[tenant_budgets.tenants]]",
		"memory_per_task_bytes",
		"aggregate_memory_bytes",
	] {
		assert!(
			config.contains(required),
			"missing tenant budget config token {required}"
		);
	}
	assert!(config.contains("TenantBudgetPolicy::omitted_config"));
}

#[test]
pub(crate) fn adr050_tenant_budget_config_validates_default_duplicates_and_nonzero() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"tenant_budgets.default is required",
		"tenant_budgets tenant_id must be greater than 0",
		"tenant_budgets memory_per_task_bytes must be greater than 0",
		"tenant_budgets aggregate_memory_bytes must be greater than 0",
		"duplicate tenant_budgets entry for tenant_id",
		"memory_per_task_bytes must be less than or equal to aggregate_memory_bytes",
	] {
		assert!(
			config.contains(required),
			"missing tenant budget validation token {required}"
		);
	}
}

#[test]
pub(crate) fn adr050_query_memory_governed_by_query_budget_not_memory_manager() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let memory =
		std::fs::read_to_string(manifest_dir().join("src/server/memory.rs")).expect("read memory");

	assert!(coordinator.contains("query_budget_for_base(context.tenant_id, self.query_budget)"));
	assert!(coordinator.contains("TaskBudget::from_query_budget(query_budget)"));
	assert!(!coordinator.contains("MemoryManager::scoped"));
	assert!(!memory.contains("TenantId"));
	assert!(!memory.contains("TenantMemory"));
}

#[test]
pub(crate) fn adr050_per_query_budget_sourced_by_tenant_at_coordinator() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");

	for required in [
		"fn admit_query(&self, context: &ExecutionContext)",
		"query_budget_for_base(context.tenant_id, self.query_budget)",
		"execute_query_with_admission(plan, context, query_budget,",
		"fragment_config(context, query_budget)",
	] {
		assert!(
			coordinator.contains(required),
			"missing tenant query budget source token {required}"
		);
	}
}

#[test]
pub(crate) fn adr050_aggregate_admission_reserves_and_releases_by_tenant() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");

	for required in [
		"pub struct TenantMemoryAccountant",
		"pub fn try_reserve(",
		"pub struct TenantMemoryReservation",
		"impl Drop for TenantMemoryReservation",
		"pub struct TenantAdmissionController",
		"pub struct AdmissionReservation",
		"self.release_once();",
	] {
		assert!(
			budget.contains(required),
			"missing tenant memory accountant token {required}"
		);
	}
	assert!(coordinator.contains("tenant_admission"));
	assert!(coordinator.contains("try_admit(context.tenant_id, query_budget.memory_per_task)"));
	assert!(coordinator.contains("_reservation: AdmissionReservation"));
}

#[test]
pub(crate) fn adr050_aggregate_admission_rejects_before_bind_or_execution() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let execute_query =
		brace_blocks_after_marker(&coordinator, "pub async fn execute_query_with_context")
			.into_iter()
			.next()
			.expect("execute_query_with_context")
			.1;
	let helper = brace_blocks_after_marker(&coordinator, "async fn execute_query_with_admission")
		.into_iter()
		.next()
		.expect("execute_query_with_admission")
		.1;

	assert!(execute_query.contains("let admitted = self.admit_query(&context).await?;"));
	assert!(execute_query.contains(".execute_query_with_admission(plan, context, query_budget,"));
	assert!(helper.contains("bind(plan, catalog.as_ref(), &self.config).await?"));
}

#[test]
pub(crate) fn adr050_budget_exhaustion_maps_to_resource_exhausted() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	for required in [
		"SqlErrorCode::ResourceExhausted",
		"ExecutionError::BudgetExhausted",
		"TaskFailureKind::BudgetExhausted",
		"BudgetDimension::Memory",
		"tenant memory budget exhausted",
	] {
		assert!(
			daemon.contains(required),
			"missing ResourceExhausted mapping token {required}"
		);
	}
}

#[test]
pub(crate) fn adr050_resource_exhausted_is_only_new_sql_error_code() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let error_codes = brace_blocks_after_marker(&wire, "pub enum SqlErrorCode")
		.into_iter()
		.next()
		.expect("SqlErrorCode")
		.1;
	let variants = error_codes
		.lines()
		.map(str::trim)
		.filter(|line| line.ends_with(','))
		.collect::<Vec<_>>();

	assert!(variants.contains(&"ResourceExhausted,"));
	assert_eq!(
		variants.len(),
		12,
		"SqlErrorCode should be 11 baseline + ResourceExhausted"
	);
	assert!(!wire.contains("TenantBudgetMessage"));
	assert!(!wire.contains("ResourceQuota"));
}

#[test]
pub(crate) fn adr050_default_and_loopback_tenants_use_default_budget() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("pub fn budget_for(&self, tenant_id: TenantId) -> TenantBudget"));
	assert!(budget.contains(".unwrap_or(self.default)"));
	assert!(config.contains("TenantBudgetPolicy::omitted_config(worker_memory_budget)"));
	assert!(daemon.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
	assert!(daemon.contains(".with_tenant_budget_policy(config.tenant_budgets.clone())"));
}

#[test]
pub(crate) fn adr050_memory_only_no_cpu_connection_or_storage_quotas() {
	let source = [
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config"),
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord"),
		std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
			.expect("read budget"),
	]
	.join("\n");

	for forbidden in [
		"CpuBudget",
		"cpu_quota",
		"connection_quota",
		"storage_quota",
	] {
		assert!(
			!source.contains(forbidden),
			"ADR 050 memory budget gained unrelated quota surface {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr050_no_per_tenant_metric_labels() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert_bounded_resource_tenant_labels_only(&observability);
	assert!(!observability.contains("tenant=\""));
}

#[test]
pub(crate) fn adr050_no_wendelon_production_changes() {
	let wendelon_source = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"TenantMemoryAccountant",
		"ResourceExhausted",
		"tenant_budgets",
		"SqlErrorCode",
	] {
		assert!(
			!wendelon_source.contains(forbidden),
			"Wendelon source contains Toron ADR 050 token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr050_prior_adr04x_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_", "adr049_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr050_nilket_root_remains_runtime_free() {
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
pub(crate) fn adr050_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	assert!(!fixture.contains("TenantBudgetPolicy"));
	assert!(!fixture.contains("tenant_budgets"));
	assert!(!fixture.contains("ResourceExhausted"));
}

#[test]
pub(crate) fn adr050_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronTenantBudget",
		"ToronTenantMemory",
		"ToronResourceExhausted",
		"TenantBudgetPolicyV1",
		"TenantBudgetV1",
		"TenantMemoryAccountantV1",
		"TenantMemoryReservationV1",
		"ResourceExhaustedV1",
	]);
}

#[test]
pub(crate) fn adr051_descriptor_bootstrap_entries_are_tenant_explicit() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("pub struct DescriptorBootstrap"));
	assert!(config.contains("pub struct DescriptorBootstrapEntry"));
	assert!(config.contains("pub tenant_id: u64"));
	assert!(config.contains("pub table_id: u64"));
	assert!(config.contains("tenant_id: Option<u64>"));
	assert!(config.contains("table_id: Option<u64>"));
}

#[test]
pub(crate) fn adr051_descriptor_bootstrap_uses_brand_clean_names() {
	assert_toron_sources_do_not_contain(&[
		"ToronDescriptorBootstrap",
		"ToronDescriptorBootstrapTable",
		"DescriptorBootstrapV1",
		"DescriptorBootstrapEntryV1",
	]);
}

#[test]
pub(crate) fn adr051_descriptor_bootstrap_rejects_missing_tenant_and_table_id() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config test");

	for required in [
		"descriptor bootstrap tenant_id is required",
		"descriptor bootstrap table_id is required",
		"descriptor_bootstrap_requires_tenant_id",
		"descriptor_bootstrap_requires_table_id",
	] {
		assert!(
			config.contains(required) || config_test.contains(required),
			"missing required-id guard token {required}"
		);
	}
}

#[test]
pub(crate) fn adr051_descriptor_bootstrap_rejects_zero_tenant_and_table_id() {
	let source = [
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config"),
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog"),
		std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
			.expect("read config test"),
	]
	.join("\n");

	for required in [
		"descriptor bootstrap tenant_id 0 is reserved",
		"descriptor bootstrap table_id 0 is reserved",
		"descriptor_bootstrap_rejects_zero_tenant",
		"descriptor_bootstrap_rejects_zero_table_id",
	] {
		assert!(
			source.contains(required),
			"missing zero-id guard token {required}"
		);
	}
}

#[test]
pub(crate) fn adr051_descriptor_bootstrap_rejects_duplicate_table_id_within_tenant() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config test");

	assert!(catalog.contains("duplicate descriptor bootstrap table_id {} for tenant {}"));
	assert!(catalog.contains("state.by_id.contains_key(&(tenant_id, table_id))"));
	assert!(config_test.contains("descriptor_bootstrap_rejects_duplicate_table_id_within_tenant"));
}

#[test]
pub(crate) fn adr051_descriptor_bootstrap_rejects_duplicate_table_name_within_tenant() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config test");

	assert!(catalog.contains("duplicate descriptor bootstrap table {} for tenant {}"));
	assert!(catalog.contains("state.by_ref.contains_key(&(tenant_id, table_ref.clone()))"));
	assert!(
		config_test.contains("descriptor_bootstrap_rejects_duplicate_table_name_within_tenant")
	);
}

#[test]
pub(crate) fn adr051_descriptor_bootstrap_allows_duplicate_keys_across_tenants() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config test");

	assert!(catalog.contains("by_id: HashMap<(TenantId, TableId), TableBinding>"));
	assert!(catalog.contains("by_ref: HashMap<(TenantId, TableRef), TableId>"));
	assert!(config_test.contains("descriptor_bootstrap_allows_same_table_id_across_tenants"));
	assert!(config_test.contains("descriptor_bootstrap_allows_same_table_name_across_tenants"));
	assert!(!catalog.contains("HashSet<(TenantId, TableObjectId)>"));
}

#[test]
pub(crate) fn adr051_loader_returns_tenant_table_descriptor_triples() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("pub fn load_descriptor_bootstrap"));
	assert!(config.contains("Vec<(TenantId, TableId, wendelon_client::TableSchemaDescriptor)>"));
	assert!(
		config.contains(
			"descriptors.push((TenantId(tenant_id), TableId(table_id), table.descriptor))"
		)
	);
}

#[test]
pub(crate) fn adr051_daemon_uses_tenant_explicit_registry_constructor() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let startup = brace_blocks_after_marker(&daemon, "async fn start_with_client")
		.into_iter()
		.next()
		.expect("ToronDaemon::start_with_client")
		.1;

	assert!(startup.contains("WendelonDescriptorRegistry::from_tenant_descriptors"));
	assert!(startup.contains("descriptor_triples"));
	assert!(!startup.contains("WendelonDescriptorRegistry::from_descriptors("));
}

#[test]
pub(crate) fn adr051_daemon_bootstrap_does_not_default_to_node_tenant() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let startup = brace_blocks_after_marker(&daemon, "async fn start_with_client")
		.into_iter()
		.next()
		.expect("ToronDaemon::start_with_client")
		.1;

	assert!(!startup.contains("from_descriptors_for_tenant"));
	assert!(!startup.contains("from_tenant_descriptors(config.node.tenant_id"));
	assert!(startup.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
}

#[test]
pub(crate) fn adr051_registry_constructor_is_atomic_for_bootstrap() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let constructor = brace_blocks_after_marker(&catalog, "pub fn from_tenant_descriptors")
		.into_iter()
		.next()
		.expect("from_tenant_descriptors")
		.1;

	assert!(constructor.contains("let registry = Self::default();"));
	assert!(constructor.contains("Ok(registry)"));
	assert!(constructor.contains("insert_wendelon_descriptor_locked("));
	assert!(!constructor.contains("&self"));
}

#[test]
pub(crate) fn adr051_descriptor_identity_fields_are_preserved() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config test");

	assert!(config.contains("table.descriptor"));
	assert!(catalog.contains("TableBinding::from_descriptor(table_id, &descriptor)?"));
	assert!(config_test.contains("descriptor_bootstrap_preserves_physical_fingerprint"));
	assert!(!config.contains("table_object_id ="));
}

#[test]
pub(crate) fn adr051_no_sql_wire_or_tenant_source_changes() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let cli = std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(!wire.contains("tenant_id"));
	assert!(!wire.contains("TenantRoute"));
	assert!(!wire.contains("Admin"));
	assert!(!cli.contains("--tenant"));
	assert!(daemon.contains("SqlTenantSource::AuthenticatedPeer"));
	assert!(daemon.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
}

#[test]
pub(crate) fn adr051_no_live_descriptor_sync_or_reload() {
	let source = [
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon"),
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config"),
	]
	.join("\n");

	for forbidden in [
		"list_descriptors",
		"refresh_descriptor",
		"watch_descriptor",
		"reload_descriptor",
		"descriptor_reload",
		"SqlMessage::Admin",
	] {
		assert!(
			!source.contains(forbidden),
			"ADR 051 must stay startup-only; found {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr051_no_per_tenant_metric_labels() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert!(observability.contains("descriptors_loaded"));
	assert_bounded_resource_tenant_labels_only(&observability);
	assert!(!observability.contains("tenant=\""));
}

#[test]
pub(crate) fn adr051_no_wendelon_production_changes() {
	let wendelon_source = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"DescriptorBootstrap",
		"load_descriptor_bootstrap",
		"from_tenant_descriptors",
		"SqlTenantSource",
	] {
		assert!(
			!wendelon_source.contains(forbidden),
			"Wendelon source contains Toron ADR 051 token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr051_prior_adr04x_and_adr050_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_", "adr049_", "adr050_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr051_nilket_root_remains_runtime_free() {
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
pub(crate) fn adr051_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	assert!(!fixture.contains("DescriptorBootstrap"));
	assert!(!fixture.contains("load_descriptor_bootstrap"));
	assert!(!fixture.contains("tenant_id"));
}

#[test]
pub(crate) fn adr051_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronDescriptorBootstrap",
		"ToronDescriptorBootstrapTable",
		"DescriptorBootstrapV1",
		"DescriptorBootstrapEntryV1",
		"TenantDescriptorBootstrapV1",
	]);
}

#[test]
pub(crate) fn adr052_tenant_budget_policy_has_concurrency_limit() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget source");

	assert!(budget.contains("max_concurrent_queries: NonZeroU32"));
	assert!(budget.contains("derive_max_concurrent_queries"));
	assert!(budget.contains("pub fn budget_for(&self, tenant_id: TenantId) -> TenantBudget"));
	assert!(budget.contains("pub fn from_query_budget(query_budget: QueryBudget) -> Self"));
}

#[test]
pub(crate) fn adr052_concurrency_config_validates_nonzero() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config source");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(config.contains("max_concurrent_queries: Option<u32>"));
	assert!(config.contains("tenant_budgets max_concurrent_queries must be greater than 0"));
	assert!(config_test.contains("zero-concurrency"));
	assert!(config_test.contains("tenant_concurrency_policy_config_round_trips"));
	assert!(config_test.contains("tenant_concurrency_config_rejects_zero"));
}

#[test]
pub(crate) fn adr052_omitted_config_derives_bounded_concurrency_default() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget source");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(budget.contains("aggregate_memory.get() / per_task"));
	assert!(budget.contains("u32::try_from(slots).unwrap_or(u32::MAX)"));
	assert!(budget.contains("NonZeroU32::new(1).expect(\"one is nonzero\")"));
	assert!(config_test.contains("tenant_concurrency_omitted_config_derives_default"));
}

#[test]
pub(crate) fn adr052_concurrency_accountant_is_tenant_keyed_and_raii() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget source");

	assert!(budget.contains("pub struct TenantConcurrencyAccountant"));
	assert!(budget.contains("active: Arc<Mutex<HashMap<TenantId, u32>>>"));
	assert!(budget.contains("pub fn try_reserve("));
	assert!(budget.contains("pub struct TenantConcurrencyReservation"));
	assert!(budget.contains("impl Drop for TenantConcurrencyReservation"));
	assert!(budget.contains("pub struct TenantConcurrencyRejection"));
	assert!(!budget.contains("BudgetDimension::Concurrency"));
}

#[test]
pub(crate) fn adr052_concurrency_admission_rejects_before_bind_or_execution() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let admit = brace_blocks_after_marker(&coordinator, "async fn admit_query")
		.into_iter()
		.next()
		.expect("admit_query")
		.1;

	assert!(admit.contains("tenant_admission"));
	assert!(admit.contains("try_admit"));
	assert!(admit.contains("AdmissionAttempt::Rejected"));
	assert!(coordinator.contains("tenant_concurrency_limit_rejects_before_bind"));
	assert!(coordinator.contains("execute_query_with_context(&count_plan(), context)"));
}

#[test]
pub(crate) fn adr052_admitted_query_holds_memory_and_concurrency_reservations() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");

	assert!(coordinator.contains("_reservation: AdmissionReservation"));
	assert!(coordinator.contains("TenantAdmissionController"));
	assert!(coordinator.contains("memory_rejection_releases_concurrency_reservation"));
	assert!(coordinator.contains("tenant_concurrency_reservation_releases_on_error"));
	assert!(coordinator.contains("tenant_concurrency_reservation_releases_on_success"));
	assert!(coordinator.contains("tenant_concurrency_active(TenantId(8)), 0"));
}

#[test]
pub(crate) fn adr052_concurrency_exhaustion_maps_to_resource_exhausted() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");

	assert!(daemon.contains("ExecutionError::ConcurrencyExhausted"));
	assert!(daemon.contains("SqlErrorCode::ResourceExhausted"));
	assert!(daemon.contains("tenant concurrency limit exhausted"));
	assert!(daemon.contains("sql_concurrency_exhaustion_returns_resource_exhausted"));
}

#[test]
pub(crate) fn adr052_resource_exhausted_reuses_existing_sql_error_code() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let error_codes = brace_blocks_after_marker(&wire, "pub enum SqlErrorCode")
		.into_iter()
		.next()
		.expect("SqlErrorCode")
		.1;

	assert!(error_codes.contains("ResourceExhausted"));
	assert!(!error_codes.contains("Concurrency"));
	assert!(!wire.contains("tenant_id: TenantId"));
}

#[test]
pub(crate) fn adr052_one_tenant_saturated_does_not_block_another() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");

	assert!(coordinator.contains("one_tenant_saturated_does_not_block_another"));
	assert!(coordinator.contains("one_tenant_concurrency_saturation_does_not_block_another"));
}

#[test]
pub(crate) fn adr052_resource_metrics_have_bounded_tenant_labels() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	assert_bounded_resource_tenant_labels_only(&observability);
	assert!(observability.contains("configured_tenant_ids()"));
	assert!(observability.contains("\"other\""));
	assert!(observability_test.contains("resource_metrics_label_configured_tenant"));
	assert!(observability_test.contains("resource_metrics_bucket_unconfigured_tenant_as_other"));
	assert!(observability_test.contains("resource_metrics_expose_memory_and_concurrency_limits"));
	assert!(observability_test.contains("resource_admission_rejections_are_labeled_by_resource"));
}

#[test]
pub(crate) fn adr052_general_daemon_metrics_remain_tenant_label_free() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	assert_bounded_resource_tenant_labels_only(&observability);
	assert!(observability_test.contains("general_daemon_metrics_do_not_gain_tenant_labels"));
	for forbidden in [
		"listener_accepts_total{{tenant_id",
		"listener_rejects_total{{tenant_id",
		"daemon_ready{{tenant_id",
		"daemon_uptime_seconds{{tenant_id",
		"descriptors_loaded{{tenant_id",
		"static_workers_configured{{tenant_id",
	] {
		assert!(
			!observability.contains(forbidden),
			"general metric gained tenant label pattern {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr052_no_cpu_connection_or_storage_quota_surface() {
	let source = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"CpuBudget",
		"cpu_quota",
		"connection_quota",
		"storage_quota",
	] {
		assert!(
			!source.contains(forbidden),
			"ADR 052 concurrency admission gained unrelated quota surface {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr052_no_wendelon_production_changes() {
	let wendelon_source = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"TenantConcurrencyAccountant",
		"max_concurrent_queries",
		"tenant_admission_rejections_total",
		"ConcurrencyExhausted",
	] {
		assert!(
			!wendelon_source.contains(forbidden),
			"Wendelon source contains Toron ADR 052 token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr052_no_descriptor_sync_stub_or_runtime_ingestion() {
	let source = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"DescriptorSource",
		"watch_descriptor",
		"reload_descriptor",
		"runtime_descriptor",
		"descriptor_reload",
	] {
		assert!(
			!source.contains(forbidden),
			"ADR 052 must not add descriptor sync machinery; found {forbidden}"
		);
	}
	assert!(
		source.contains("sync_descriptor_for_tenant"),
		"ADR 053 promotion should provide the governed descriptor-sync consumer"
	);
}

#[test]
pub(crate) fn adr052_prior_adr04x_05x_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_", "adr049_", "adr050_", "adr051_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr052_nilket_root_remains_runtime_free() {
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
pub(crate) fn adr052_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	assert!(!fixture.contains("TenantConcurrencyAccountant"));
	assert!(!fixture.contains("max_concurrent_queries"));
	assert!(!fixture.contains("tenant_budgets"));
}

#[test]
pub(crate) fn adr052_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronTenantConcurrencyAccountant",
		"ToronTenantConcurrencyReservation",
		"TenantConcurrencyAccountantV1",
		"TenantConcurrencyReservationV1",
		"TenantConcurrencyRejectionV1",
		"TenantAdmissionRejectionCountersV1",
	]);
}

#[test]
pub(crate) fn adr053_descriptor_sync_lives_in_toron_consumer() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let wendelon_server = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	assert!(catalog.contains("async fn resolve_table_after_local_miss"));
	assert!(catalog.contains("DescriptorSyncFlights"));
	assert!(catalog.contains("sync_descriptor_for_tenant"));
	assert!(!wendelon_server.contains("TenantDescriptorSyncAccountant"));
	assert!(!wendelon_server.contains("CatalogError::ResourceExhausted"));
}

#[test]
pub(crate) fn adr053_wendelon_server_descriptor_get_surface_unchanged() {
	let wendelon_core = std::fs::read_to_string(wendelon_manifest_dir().join("src/client/core.rs"))
		.expect("read Wendelon client core");
	let wendelon_network = source_lines(&wendelon_manifest_dir().join("src/network"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	assert!(wendelon_core.contains("pub async fn get_table_schema_descriptor"));
	assert!(wendelon_core.contains("CONTROL_REASON_NOT_FOUND => return Ok(None)"));
	assert!(wendelon_network.contains("TableDescriptorGetRequestMeta"));
	assert!(wendelon_network.contains("TableDescriptorGetResponseMeta"));
	assert!(!wendelon_network.contains("max_concurrent_descriptor_syncs"));
}

#[test]
pub(crate) fn adr053_resolve_table_miss_is_only_sync_trigger() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let resolve_table = wendelon_catalog_method_body(&catalog, "async fn resolve_table");
	let resolve_table_id = wendelon_catalog_method_body(&catalog, "async fn resolve_table_id");
	let create_table = wendelon_catalog_method_body(&catalog, "async fn create_table");
	let drop_table = wendelon_catalog_method_body(&catalog, "async fn drop_table");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");

	assert!(resolve_table.contains("resolve_table_after_local_miss"));
	assert!(resolve_table.contains("Err(CatalogError::TableNotFound"));
	for body in [resolve_table_id, drop_table] {
		assert!(!body.contains("resolve_table_after_local_miss"));
		assert!(!body.contains("get_table_schema_descriptor"));
	}
	assert!(!create_table.contains("get_table_schema_descriptor"));
	assert!(create_table.contains("ClientError::SchemaUpdateRejected"));
	assert!(create_table.contains("if if_not_exists"));
	assert!(create_table.contains("&& schema_update_rejected"));
	assert!(create_table.contains("resolve_table_after_local_miss(name).await"));
	assert!(!storage.contains("resolve_table_after_local_miss"));
	assert!(!storage.contains("get_table_schema_descriptor"));
}

#[test]
pub(crate) fn adr053_descriptor_sync_key_is_tenant_and_exact_name() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let key = brace_blocks_after_marker(&catalog, "struct DescriptorSyncKey")
		.into_iter()
		.next()
		.expect("DescriptorSyncKey")
		.1;
	let key_impl = brace_blocks_after_marker(&catalog, "impl DescriptorSyncKey")
		.into_iter()
		.next()
		.expect("DescriptorSyncKey impl")
		.1;

	assert!(key.contains("tenant_id: TenantId"));
	assert!(key.contains("namespace: String"));
	assert!(key.contains("table_name: String"));
	assert!(key_impl.contains(".schema"));
	assert!(key_impl.contains(".clone()"));
	assert!(key_impl.contains("unwrap_or_else(|| \"default\".to_string())"));
	assert!(key_impl.contains("table.table.clone()"));
	assert!(!key_impl.contains("list"));
	assert!(!key_impl.contains("enumer"));
}

#[test]
pub(crate) fn adr053_unqualified_bind_miss_uses_default_namespace() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("unwrap_or_else(|| \"default\".to_string())"));
	assert!(catalog.contains("fn canonical_table_ref"));
	assert!(catalog.contains("unwrap_or(\"default\")"));
	assert!(catalog.contains("resolve_table_unqualified_miss_fetches_default_namespace"));
}

#[test]
pub(crate) fn adr053_singleflight_shared_across_scoped_catalogs() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let scoped = wendelon_catalog_method_body(&catalog, "fn scoped");

	assert!(catalog.contains("descriptor_sync_flights: Option<Arc<DescriptorSyncFlights>>"));
	assert!(scoped.contains("descriptor_sync_flights: self.descriptor_sync_flights.clone()"));
	assert!(scoped.contains("descriptor_sync_metrics: Arc::clone(&self.descriptor_sync_metrics)"));
}

#[test]
pub(crate) fn adr053_singleflight_waiters_do_not_consume_sync_budget() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let miss = brace_blocks_after_marker(&catalog, "async fn resolve_table_after_local_miss")
		.into_iter()
		.next()
		.expect("resolve_table_after_local_miss")
		.1;
	let owner = brace_blocks_after_marker(&catalog, "async fn resolve_table_sync_owner")
		.into_iter()
		.next()
		.expect("resolve_table_sync_owner")
		.1;

	assert!(miss.contains("DescriptorSyncParticipant::Waiter"));
	assert!(!miss.contains("try_reserve"));
	assert!(owner.contains("owner.flights.accountant.try_reserve"));
	assert!(catalog.contains("concurrent_same_table_bind_misses_issue_one_descriptor_get"));
}

#[test]
pub(crate) fn adr053_descriptor_sync_budget_is_nonzero_configured() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(budget.contains("max_concurrent_descriptor_syncs: NonZeroU32"));
	assert!(budget.contains("derive_max_concurrent_descriptor_syncs"));
	assert!(budget.contains("pub struct TenantDescriptorSyncAccountant"));
	assert!(config.contains("max_concurrent_descriptor_syncs: Option<u32>"));
	assert!(
		config.contains("tenant_budgets max_concurrent_descriptor_syncs must be greater than 0")
	);
	assert!(config_test.contains("zero-descriptor-sync"));
}

#[test]
pub(crate) fn adr053_sync_budget_rejection_maps_resource_exhausted() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let error = std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read error");

	assert!(error.contains("ResourceExhausted { message: String }"));
	assert!(catalog.contains("tenant descriptor sync limit exhausted"));
	assert!(daemon.contains("CatalogError::ResourceExhausted"));
	assert!(daemon.contains("SqlErrorCode::ResourceExhausted"));
	assert!(daemon.contains("catalog_resource_exhausted_maps_to_resource_exhausted_sql_error"));
}

#[test]
pub(crate) fn adr053_descriptor_sync_uses_client_facade() {
	let toron_catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let client =
		std::fs::read_to_string(wendelon_manifest_dir().join("crates/wendelon-client/src/lib.rs"))
			.expect("read Wendelon client facade");

	assert!(client.contains("pub async fn get_table_schema_descriptor"));
	assert!(client.contains("self.inner"));
	assert!(toron_catalog.contains("trait DescriptorSyncClient"));
	assert!(toron_catalog.contains("impl DescriptorSyncClient for Client"));
	assert!(toron_catalog.contains(".get_table_schema_descriptor("));
}

#[test]
pub(crate) fn adr053_sync_merges_registry_in_place_no_arc_swap() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let helper = brace_blocks_after_marker(&catalog, "pub fn sync_descriptor_for_tenant")
		.into_iter()
		.next()
		.expect("sync_descriptor_for_tenant")
		.1;

	assert!(helper.contains("let mut state = self"));
	assert!(helper.contains(".inner"));
	assert!(helper.contains(".write()"));
	assert!(helper.contains("insert_wendelon_descriptor_locked"));
	assert!(!helper.contains("Arc::new"));
	assert!(!helper.contains("self.registry"));
}

#[test]
pub(crate) fn adr053_runtime_sync_does_not_call_from_tenant_descriptors() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let helper = brace_blocks_after_marker(&catalog, "pub fn sync_descriptor_for_tenant")
		.into_iter()
		.next()
		.expect("sync_descriptor_for_tenant")
		.1;
	let miss = brace_blocks_after_marker(&catalog, "async fn resolve_table_after_local_miss")
		.into_iter()
		.next()
		.expect("resolve_table_after_local_miss")
		.1;

	assert!(!helper.contains("from_tenant_descriptors"));
	assert!(!miss.contains("from_tenant_descriptors"));
}

#[test]
pub(crate) fn adr053_no_background_watch_reload_inventory_or_negative_cache() {
	let source = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"watch_descriptor",
		"reload_descriptor",
		"descriptor_reload",
		"list_descriptors",
		"DescriptorInventory",
		"negative_cache",
		"NegativeCache",
	] {
		assert!(
			!source.contains(forbidden),
			"ADR 053 must not add background/inventory/negative-cache machinery; found {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr053_create_drop_do_not_trigger_descriptor_sync_except_adr061_race_reconciliation()
{
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let create_table = wendelon_catalog_method_body(&catalog, "async fn create_table");
	let drop_table = wendelon_catalog_method_body(&catalog, "async fn drop_table");

	assert!(!create_table.contains("get_table_schema_descriptor"));
	assert!(create_table.contains("ClientError::SchemaUpdateRejected"));
	assert!(create_table.contains("if if_not_exists"));
	assert!(create_table.contains("&& schema_update_rejected"));
	assert!(create_table.contains("resolve_table_after_local_miss(name).await"));
	assert!(!create_table.contains("DescriptorSyncParticipant"));

	assert!(!drop_table.contains("get_table_schema_descriptor"));
	assert!(!drop_table.contains("resolve_table_after_local_miss"));
	assert!(!drop_table.contains("DescriptorSyncParticipant"));
}

#[test]
pub(crate) fn adr053_resolve_table_id_and_storage_do_not_trigger_descriptor_sync() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let resolve_id = wendelon_catalog_method_body(&catalog, "async fn resolve_table_id");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");

	assert!(!resolve_id.contains("get_table_schema_descriptor"));
	assert!(!resolve_id.contains("resolve_table_after_local_miss"));
	assert!(!storage.contains("get_table_schema_descriptor"));
	assert!(!storage.contains("resolve_table_after_local_miss"));
}

#[test]
pub(crate) fn adr053_descriptor_evolved_does_not_add_direct_sync_path() {
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let select_loop = brace_blocks_after_marker(&coordinator, "pub async fn execute_query")
		.into_iter()
		.last()
		.expect("QueryCoordinator::execute_query")
		.1;
	let write_helper = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan(")
		.into_iter()
		.next()
		.expect("execute_write_plan")
		.1;

	assert!(storage.contains("StorageError::DescriptorEvolved"));
	assert!(select_loop.contains("invalidate_descriptor_evolution(catalog.as_ref(), &error)"));
	assert!(select_loop.contains("bind(plan, catalog.as_ref(), &self.config).await?"));
	assert!(write_helper.contains("invalidate_descriptor_evolution_on_error"));
	assert!(!write_helper.contains("loop"));
	assert!(!coordinator.contains("get_table_schema_descriptor"));
	assert!(!storage.contains("sync_descriptor_for_tenant"));
	assert!(!storage.contains("resolve_table_after_local_miss"));
}

#[test]
pub(crate) fn adr053_catalog_liveness_metrics_have_no_tenant_or_table_labels() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	assert!(observability.contains("catalog_descriptor_sync_requests_total"));
	assert!(observability.contains("outcome=\\\"{outcome}\\\""));
	for forbidden in ["tenant_id", "namespace", "table_name", "table_object_id"] {
		let offenders = observability
			.lines()
			.filter(|line| {
				line.contains("catalog_descriptor_sync_requests_total") && line.contains(forbidden)
			})
			.collect::<Vec<_>>();
		assert!(
			offenders.is_empty(),
			"descriptor sync metric gained forbidden label {forbidden}: {offenders:?}"
		);
	}
	assert!(
		observability_test.contains("catalog_descriptor_sync_metrics_are_aggregate_outcome_only")
	);
}

#[test]
pub(crate) fn adr053_retry_is_internal_to_resolve_table_not_coordinator() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let resolve_table = wendelon_catalog_method_body(&catalog, "async fn resolve_table");

	assert!(resolve_table.contains("resolve_table_after_local_miss"));
	assert!(!coordinator.contains("resolve_table_after_local_miss"));
	assert!(!coordinator.contains("get_table_schema_descriptor"));
}

#[test]
pub(crate) fn adr054_descriptor_evolved_carries_local_table_id() {
	let error = std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read error");
	let distributed = std::fs::read_to_string(manifest_dir().join("src/error/distributed.rs"))
		.expect("read distributed error");

	assert!(error.contains("DescriptorEvolved { table_id: TableId }"));
	assert!(distributed.contains("DescriptorEvolved {\n\t\ttable_id: TableId,"));
	assert!(distributed.contains("expected: Option<PhysicalDescriptorFingerprint>"));
	assert!(distributed.contains("current: Option<PhysicalDescriptorFingerprint>"));
}

#[test]
pub(crate) fn adr054_wendelon_evolved_error_mapped_with_request_context() {
	let error = std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read error");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");

	assert!(
		!error.contains("ClientError::TableDescriptorEvolved { .. } => Self::DescriptorEvolved")
	);
	assert!(storage.contains("fn map_wendelon_client_error_to_storage("));
	assert!(storage.contains("StorageError::DescriptorEvolved { table_id }"));
	assert!(storage.contains("map_wendelon_client_error_to_storage(request.table_id, error)"));
}

#[test]
pub(crate) fn adr054_trigger_is_descriptor_evolved_only() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let helper = brace_blocks_after_marker(&coordinator, "fn descriptor_evolved_table_id")
		.into_iter()
		.next()
		.expect("descriptor_evolved_table_id")
		.1;

	assert!(helper.contains("StorageError::DescriptorEvolved"));
	assert!(helper.contains("TaskFailureKind::DescriptorEvolved"));
	for forbidden in ["TableNotFound", "AdminFailed", "ResourceExhausted"] {
		assert!(
			!helper.contains(forbidden),
			"ADR 054 trigger helper widened to {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr054_invalidation_removes_by_id_payload_and_all_refs() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let invalidation =
		brace_blocks_after_marker(&catalog, "pub fn invalidate_descriptor_for_tenant")
			.into_iter()
			.next()
			.expect("invalidate_descriptor_for_tenant")
			.1;

	assert!(catalog.contains("pub struct InvalidatedDescriptor"));
	assert!(invalidation.contains("state.by_id.remove(&(tenant_id, table))"));
	assert!(invalidation.contains("state.descriptors.remove(&(tenant_id, table))"));
	assert!(invalidation.contains("state.by_ref.retain"));
	assert!(invalidation.contains("*entry_tenant == tenant_id && *table_id == table"));
	assert!(invalidation.contains("refs.push(table_ref.clone())"));
}

#[test]
pub(crate) fn adr054_invalidation_is_tenant_scoped_and_idempotent() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let invalidation =
		brace_blocks_after_marker(&catalog, "pub fn invalidate_descriptor_for_tenant")
			.into_iter()
			.next()
			.expect("invalidate_descriptor_for_tenant")
			.1;

	assert!(invalidation.contains("tenant_id: TenantId"));
	assert!(invalidation.contains("table: TableId"));
	assert!(invalidation.contains("let binding = state.by_id.remove(&(tenant_id, table))?;"));
	assert!(!invalidation.contains("allocate_wendelon_table_id_locked"));
	assert!(!invalidation.contains("sync_descriptor_for_tenant"));
	assert!(!invalidation.contains("from_tenant_descriptors"));
	assert!(catalog.contains("registry_invalidate_descriptor_for_tenant_is_tenant_scoped"));
}

#[test]
pub(crate) fn adr054_select_retry_reuses_adr053_bind_miss_sync() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(coordinator.contains("invalidate_descriptor_evolution(catalog.as_ref(), &error)"));
	assert!(coordinator.contains("let bound = bind(plan, catalog.as_ref(), &self.config).await?;"));
	assert!(catalog.contains("resolve_table_after_local_miss"));
	assert!(catalog.contains("DescriptorSyncParticipant::Owner"));
	assert!(
		coordinator
			.contains("coordinator_returns_resource_exhausted_when_refresh_sync_is_over_budget")
	);
	assert!(!coordinator.contains("get_table_schema_descriptor"));
}

#[test]
pub(crate) fn adr054_no_force_refresh_resolve_table_flag() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(!catalog.contains("force_refresh"));
	assert!(!catalog.contains("force_descriptor_refresh"));
	assert!(!catalog.contains("refresh_descriptor_for_tenant"));
}

#[test]
pub(crate) fn adr054_invalidation_is_path_agnostic_select_write_delete() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(coordinator.contains("fn invalidate_descriptor_evolution_on_error"));
	assert!(coordinator.contains("execute_write_plan_with_source("));
	assert!(coordinator.contains("invalidate_descriptor_evolution_on_error(catalog"));
	assert!(coordinator.contains("LogicalPlan::Delete"));
	assert!(coordinator.contains("LogicalPlan::CreateTableAsSelect"));
}

#[test]
pub(crate) fn adr054_writes_invalidate_but_do_not_retry() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let write_helper = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan(")
		.into_iter()
		.next()
		.expect("execute_write_plan")
		.1;

	assert!(write_helper.contains("invalidate_descriptor_evolution_on_error"));
	assert!(!write_helper.contains("loop"));
	assert!(coordinator.contains("write_descriptor_evolved_invalidates_without_retrying"));
}

#[test]
pub(crate) fn adr054_replacement_object_returns_schema_instability() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let guard = brace_blocks_after_marker(&coordinator, "async fn reject_replacement_object")
		.into_iter()
		.next()
		.expect("reject_replacement_object")
		.1;

	assert!(guard.contains("current.table == stale.table_object_id"));
	assert!(guard.contains("record_replacement"));
	assert!(guard.contains("QueryError::SchemaInstability"));
	assert!(
		coordinator.contains("coordinator_rejects_replacement_object_after_descriptor_evolved")
	);
	assert!(
		coordinator.contains("coordinator_returns_table_not_found_after_descriptor_evolved_drop")
	);
}

#[test]
pub(crate) fn adr054_second_evolution_returns_schema_instability() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(coordinator.contains("retried_descriptor_evolution"));
	assert!(coordinator.contains("Err(QueryError::SchemaInstability)"));
	assert!(
		coordinator
			.contains("coordinator_fails_schema_instability_after_second_descriptor_evolved")
	);
}

#[test]
pub(crate) fn adr054_no_ttl_reaper_watch_inventory_or_push() {
	let source = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"descriptor_ttl",
		"DescriptorTtl",
		"descriptor_reaper",
		"DescriptorReaper",
		"watch_descriptor",
		"push_invalidation",
		"DescriptorInventory",
	] {
		assert!(
			!source.contains(forbidden),
			"ADR 054 added forbidden refresh machinery: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr054_no_new_descriptor_refresh_budget_or_governance_knob() {
	let source = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in ["TenantRateLimiter", "fairness_weight", "weighted_fair"] {
		assert!(
			!source.contains(forbidden),
			"ADR 054 added governance knob {forbidden}"
		);
	}
	assert!(source.contains("max_concurrent_descriptor_syncs"));
}

#[test]
pub(crate) fn adr054_no_wendelon_server_or_sql_wire_change() {
	let toron_wire =
		std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read toron wire");
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	assert!(!toron_wire.contains("DescriptorRefresh"));
	assert!(!wendelon_source.contains("DescriptorRefresh"));
	assert!(!wendelon_source.contains("catalog_descriptor_refresh"));
}

#[test]
pub(crate) fn adr054_refresh_metrics_are_aggregate_only() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	assert!(observability.contains("catalog_descriptor_refresh_requests_total"));
	for outcome in ["invalidated", "missing", "replacement"] {
		assert!(observability.contains(outcome), "missing outcome {outcome}");
	}
	for forbidden in [
		"tenant_id",
		"namespace",
		"table_name",
		"table_object_id",
		"fingerprint",
	] {
		let offenders = observability
			.lines()
			.filter(|line| {
				line.contains("catalog_descriptor_refresh_requests_total")
					&& line.contains(forbidden)
			})
			.collect::<Vec<_>>();
		assert!(
			offenders.is_empty(),
			"descriptor refresh metric gained forbidden label {forbidden}: {offenders:?}"
		);
	}
	assert!(
		observability_test
			.contains("catalog_descriptor_refresh_metrics_are_aggregate_outcome_only")
	);
}

#[test]
pub(crate) fn adr054_prior_adr049_050_051_052_053_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in ["adr049_", "adr050_", "adr051_", "adr052_", "adr053_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr054_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr054_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("DescriptorEvolved"));
	assert!(!worker.contains("catalog_descriptor_refresh"));
	assert!(!worker.contains("force_refresh"));
}

#[test]
pub(crate) fn adr054_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	let source = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	assert!(source.contains("InvalidatedDescriptor"));
	assert!(source.contains("DescriptorRefreshMetrics"));
	assert!(!source.contains("InvalidatedDescriptorV1"));
	assert!(!source.contains("DescriptorRefreshMetricsV1"));
	assert!(!source.contains("DescriptorInvalidationV1"));
}

#[test]
pub(crate) fn adr055_queue_lives_at_admit_query_before_bind() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let execute =
		brace_blocks_after_marker(&coordinator, "pub async fn execute_query_with_context")
			.into_iter()
			.next()
			.expect("execute_query_with_context")
			.1;
	let admit_pos = execute
		.find("admit_query(&context).await?")
		.expect("admission before execution");
	let execute_pos = execute
		.find("execute_query_with_admission")
		.expect("execute with admission");
	assert!(admit_pos < execute_pos);
	assert!(coordinator.contains("let bound = bind(plan, catalog.as_ref(), &self.config).await?"));
}

#[test]
pub(crate) fn adr055_queue_entries_hold_no_plan_descriptor_or_reservation() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let waiter = budget
		.split("struct Waiter")
		.nth(1)
		.expect("waiter")
		.split("struct WaiterId")
		.next()
		.expect("waiter body");

	for required in ["tenant_id", "memory", "sequence", "grant"] {
		assert!(waiter.contains(required), "Waiter missing {required}");
	}
	for forbidden in [
		"LogicalPlan",
		"ExecutionPlan",
		"TableBinding",
		"Descriptor",
		"Storage",
	] {
		assert!(
			!waiter.contains(forbidden),
			"Waiter must not retain {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr055_omitted_config_preserves_hard_reject() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(budget.contains("let Some(admission) = self.admission else"));
	assert!(budget.contains("try_grant_owned_legacy_order"));
	assert!(
		budget.contains("omitted_admission_config_preserves_memory_and_concurrency_hard_reject")
	);
	assert!(config.contains("admission: Option<RawAdmissionPolicy>"));
	assert!(config.contains("pub admission: Option<AdmissionPolicy>"));
}

#[test]
pub(crate) fn adr055_no_queue_bypass_when_tenant_has_waiters() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("state.has_waiters(tenant_id)"));
	assert!(budget.contains("same_tenant_fifo_holds_new_request_cannot_bypass_existing_waiter"));
	assert!(budget.contains("push_back(waiter)"));
}

#[test]
pub(crate) fn adr055_borrowing_uses_explicit_shared_pool_only() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	for required in [
		"shared_memory",
		"shared_concurrent_queries",
		"shared_memory_used",
		"shared_concurrency_used",
		"borrowed_memory_and_concurrency_never_exceed_shared_pool_caps",
	] {
		assert!(
			budget.contains(required),
			"missing shared-pool token {required}"
		);
	}
}

#[test]
pub(crate) fn adr055_no_borrow_from_other_tenant_owned_budget() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("tenant_a_cannot_consume_tenant_b_owned_budget"));
	assert!(budget.contains("budget.max_borrowed_memory"));
	assert!(budget.contains("budget.max_borrowed_concurrent_queries"));
	assert!(!budget.contains("idle_owned"));
}

#[test]
pub(crate) fn adr055_grants_decide_memory_concurrency_and_borrow_atomically() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("struct GrantResources"));
	assert!(budget.contains("compute_grant("));
	assert!(budget.contains("apply_grant(state, tenant_id, resources)"));
	assert!(
		budget.contains("dropping_borrowed_memory_reservation_preserves_owned_memory_accounting")
	);
	assert!(!budget.contains("fetch_sub"));
}

#[test]
pub(crate) fn adr055_weighted_scheduler_uses_integer_deficit_rr() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("deficits: HashMap<TenantId, i64>"));
	assert!(budget.contains("admission_weight"));
	assert!(budget.contains("weighted_tenants_receive_deterministic_integer_deficit_grant_order"));
	assert!(budget.contains("remove_queue_if_empty"));
	assert!(budget.contains("drained_unconfigured_queue_prunes_scheduler_deficit"));
}

#[test]
pub(crate) fn adr055_no_preemption_or_mid_query_revocation() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(budget.contains("borrowed_in_flight_query_not_preempted_when_other_tenant_arrives"));
	assert!(!coordinator.contains("revocation"));
}

#[test]
pub(crate) fn adr055_no_background_scheduler_actor() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("pump_locked"));
	assert!(budget.contains("Drop for AdmissionReservation"));
	assert!(!budget.contains("tokio::spawn"));
	assert!(!budget.contains("JoinHandle"));
}

#[test]
pub(crate) fn adr055_admission_lock_not_held_across_await() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(coordinator.contains("tokio::time::timeout(wait, queued.recv()).await"));
	assert!(budget.contains("state: Mutex<AdmissionState>"));
	assert!(budget.contains("drop(state);"));
}

#[test]
pub(crate) fn adr055_no_connection_cap_surface() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let server = source_files(&manifest_dir().join("src/server"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read server source"))
		.collect::<Vec<_>>()
		.join("\n");

	for (name, source) in [
		("coordinator", coordinator.as_str()),
		("tenant admission controller", budget.as_str()),
		("worker/server transport", server.as_str()),
	] {
		for forbidden in [
			"SqlConnectionLimiter",
			"try_acquire_tier1",
			"try_acquire_tier2",
			"tenant sql connection limit exhausted",
		] {
			assert!(
				!source.contains(forbidden),
				"ADR 055/056 keep connection caps out of {name}: {forbidden}"
			);
		}
	}
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
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
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
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
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
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
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
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
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
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
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
	assert!(server.contains("send_task_failed(&mut stream, identity, error).await?"));
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
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
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

	assert!(facade.contains("table_query_with_runtime_filter_and_attempt_hook"));
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
		.find("table_query_with_runtime_filter_and_attempt_hook")
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

	assert!(storage.contains("table_query_with_runtime_filter_and_attempt_hook"));
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
	assert!(storage.contains("table_query_with_runtime_filter_and_attempt_hook"));
	assert!(!storage.contains("aggregate_with_attempt_hook"));
	assert!(!storage.contains("write_with_attempt_hook"));
	assert!(!storage.contains("delete_with_attempt_hook"));
	assert!(!storage.contains("cdc_subscribe_with_attempt_hook"));
	assert!(wendelon_core.contains("MsgType::QUERY_RESP, None"));
}

#[test]
pub(crate) fn adr060_no_control_ring_uni_stream_sidecar_or_sql_grammar() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");

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

#[test]
pub(crate) fn adr058_request_frame_limit_is_server_inbound_only() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let cli = std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read cli");
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
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
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let wire_test = std::fs::read_to_string(manifest_dir().join("tests/toron_sql_wire_test.rs"))
		.expect("read SQL wire tests");
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");

	assert!(wire.contains("ResultBatch(ResultBatch)"));
	assert!(wire.contains("pub arrow_ipc_bytes: ByteBuf"));
	assert!(wire.contains("let payload = encode_payload(frame);"));
	assert!(wire_test.contains("toron_sql_wire_round_trips_arrow_ipc_batch"));
	assert!(sql_tests.contains("server_result_batches_not_constrained_by_inbound_request_cap"));
}

#[test]
pub(crate) fn adr058_no_new_sql_wire_variant_or_protocol_version() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
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

	for variant in ["Execute", "Cancel", "ResultBatch", "Complete", "Error"] {
		assert!(
			message.contains(variant),
			"missing SQL message variant {variant}"
		);
	}
	assert!(!message.contains("RequestTooLarge"));
	assert!(!message.contains("Ingress"));
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

#[test]
pub(crate) fn adr056_rate_is_outermost_controller_gate() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let try_admit = brace_blocks_after_marker(&budget, "fn try_admit(self: &Arc<Self>")
		.into_iter()
		.next()
		.expect("try_admit")
		.1;

	let rate_pos = try_admit
		.find("try_consume_rate")
		.expect("try_admit consumes rate");
	let admission_pos = try_admit
		.find("let Some(admission) = self.admission else")
		.expect("try_admit admission branch");
	let grant_pos = try_admit
		.find("try_grant_owned_legacy_order")
		.expect("try_admit grant path");

	assert!(rate_pos < admission_pos);
	assert!(rate_pos < grant_pos);
}

#[test]
pub(crate) fn adr056_rate_gate_runs_before_admission_enabled_branch() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let try_admit = brace_blocks_after_marker(&budget, "fn try_admit(self: &Arc<Self>")
		.into_iter()
		.next()
		.expect("try_admit")
		.1;

	assert!(try_admit.contains("self.try_consume_rate(&mut state, tenant_id, budget)"));
	assert!(
		try_admit.find("try_consume_rate").expect("rate gate")
			< try_admit
				.find("let Some(admission) = self.admission else")
				.expect("admission branch")
	);
}

#[test]
pub(crate) fn adr056_rate_rejection_never_enters_queue_or_reservation() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let try_consume_rate = brace_blocks_after_marker(&budget, "fn try_consume_rate")
		.into_iter()
		.next()
		.expect("try_consume_rate")
		.1;
	let try_admit = brace_blocks_after_marker(&budget, "fn try_admit(self: &Arc<Self>")
		.into_iter()
		.next()
		.expect("try_admit")
		.1;

	assert!(try_consume_rate.contains("TenantAdmissionRejection::RateLimited"));
	assert!(!try_consume_rate.contains("push_back"));
	assert!(!try_consume_rate.contains("AdmissionReservation"));
	assert!(
		try_admit.find("try_consume_rate").expect("rate gate")
			< try_admit
				.find("push_back(waiter)")
				.expect("queue insertion")
	);
	assert!(budget.contains("rate_rejected_query_never_enters_admission_queue"));
}

#[test]
pub(crate) fn adr056_rate_token_consumed_on_pass_never_refunded() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let try_consume_rate = brace_blocks_after_marker(&budget, "fn try_consume_rate")
		.into_iter()
		.next()
		.expect("try_consume_rate")
		.1;
	let release_reservation = brace_blocks_after_marker(&budget, "fn release_reservation")
		.into_iter()
		.next()
		.expect("release_reservation")
		.1;

	assert!(try_consume_rate.contains("bucket.tokens = bucket.tokens.saturating_sub(1)"));
	assert!(budget.contains("token_consumed_on_pass_not_refunded_on_queue_timeout"));
	assert!(coordinator.contains("token_consumed_on_pass_not_refunded_on_bind_error"));
	assert!(coordinator.contains("token_consumed_on_pass_not_refunded_on_execution_error"));
	assert!(!release_reservation.contains("rate_buckets"));
}

#[test]
pub(crate) fn adr056_rate_has_no_reservation_or_drop_release_path() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let reservation = budget
		.split("pub struct AdmissionReservation")
		.nth(1)
		.expect("reservation")
		.split("struct GrantDelivery")
		.next()
		.expect("reservation body");
	let drop_impl = brace_blocks_after_marker(&budget, "impl Drop for AdmissionReservation")
		.into_iter()
		.next()
		.expect("drop impl")
		.1;

	assert!(!reservation.contains("rate"));
	assert!(!drop_impl.contains("rate"));
	assert!(!drop_impl.contains("TokenBucket"));
}

#[test]
pub(crate) fn adr056_lazy_refill_uses_injected_clock_no_background_task() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("trait AdmissionClock"));
	assert!(budget.contains("fn now_millis(&self) -> u64"));
	assert!(budget.contains("SystemAdmissionClock"));
	assert!(budget.contains("with_clock"));
	assert!(budget.contains("fn refill_rate_bucket"));
	assert!(budget.contains("tokens_refill_with_injected_clock"));
	assert!(!budget.contains("tokio::spawn"));
	assert!(!budget.contains("JoinHandle"));
}

#[test]
pub(crate) fn adr056_no_lock_free_or_separate_rate_limiter_component() {
	let source = source_files(&manifest_dir().join("src"))
		.into_iter()
		.map(|path| {
			let path_text = path.display().to_string();
			let source = std::fs::read_to_string(&path).expect("read source");
			(path_text, source)
		})
		.collect::<Vec<_>>();

	for (path, text) in &source {
		assert!(!path.ends_with("rate_limit.rs"));
		assert!(!text.contains("TenantRateLimiter"));
		assert!(!text.contains("packed"));
	}
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	assert!(budget.contains("rate_buckets: HashMap<RateBucketKey, TokenBucket>"));
	assert!(budget.contains("last_refill_millis: u64"));
	assert!(!budget.contains("fetch_update"));
}

#[test]
pub(crate) fn adr056_default_zero_rate_preserves_current_behavior() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(budget.contains("pub max_queries_per_second: u32"));
	assert!(budget.contains("pub query_burst_capacity: u32"));
	assert!(budget.contains("max_queries_per_second == 0"));
	assert!(budget.contains("default_zero_rate_preserves_current_admission_behavior"));
	assert!(config.contains("max_queries_per_second.unwrap_or(0)"));
	assert!(config.contains("query_burst_capacity.unwrap_or(0)"));
}

#[test]
pub(crate) fn adr056_configured_tenants_have_buckets_other_is_shared() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("enum RateBucketKey"));
	assert!(budget.contains("Configured(TenantId)"));
	assert!(budget.contains("Other"));
	assert!(budget.contains("fn rate_bucket_key"));
	assert!(budget.contains("self.policy.is_configured_tenant(tenant_id)"));
	assert!(budget.contains("unconfigured_tenants_share_other_rate_bucket"));
	assert!(budget.contains("configured_tenant_rate_bucket_isolated_from_other"));
	assert!(!budget.contains("HashMap<TenantId, TokenBucket>"));
}

#[test]
pub(crate) fn adr056_resource_exhausted_reused_no_new_sql_code() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(coordinator.contains("TenantAdmissionRejection::RateLimited"));
	assert!(coordinator.contains("tenant query rate limit exhausted for tenant"));
	assert!(coordinator.contains("CatalogError::ResourceExhausted"));
	assert!(daemon.contains("SqlErrorCode::ResourceExhausted"));
	assert!(!wire.contains("RateLimitExceeded"));
	assert!(!wire.contains("RateLimited"));
}

#[test]
pub(crate) fn adr056_rate_uses_existing_bounded_rejection_metric_no_new_family() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	assert!(budget.contains("AdmissionResource::Rate"));
	assert!(observability.contains("(AdmissionResource::Rate, \"rate\")"));
	assert!(observability.contains("tenant_admission_rejections_total"));
	assert!(!observability.contains("tenant_rate_limit_rejections_total"));
	assert!(
		observability_test
			.contains("rate_rejection_metric_uses_configured_plus_other_and_resource_rate")
	);
	for forbidden in [
		"query_id",
		"sql_text",
		"table_id",
		"descriptor",
		"fingerprint",
	] {
		let offenders = observability
			.lines()
			.filter(|line| {
				line.contains("tenant_admission_rejections_total") && line.contains(forbidden)
			})
			.collect::<Vec<_>>();
		assert!(
			offenders.is_empty(),
			"rate rejection metric gained forbidden label {forbidden}: {offenders:?}"
		);
	}
}

#[test]
pub(crate) fn adr056_no_preparse_listener_connection_or_worker_transport_rate() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let transport = source_files(&manifest_dir().join("src/server"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read transport source"))
		.collect::<Vec<_>>()
		.join("\n");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");

	assert!(!daemon.contains("max_queries_per_second"));
	assert!(!daemon.contains("RateLimited"));
	assert!(!transport.contains("RateLimited"));
	assert!(!transport.contains("TenantRateLimiter"));
	assert!(!coordinator.contains("pre_parse"));
	assert!(!coordinator.contains("parse_rate"));
}

#[test]
pub(crate) fn adr056_descriptor_retry_does_not_reconsume_rate_token() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");

	assert!(coordinator.contains("adr054_evolved_retry_stays_inside_one_admission_reservation"));
	assert!(coordinator.contains(".with_tenant_budget_policy(rate_policy(1, 1))"));
	assert!(coordinator.contains("retried_descriptor_evolution"));
}

#[test]
pub(crate) fn adr056_no_wendelon_sql_wire_nilket_or_fixture_worker_change() {
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!sql_wire.contains("RateLimited"));
	assert!(!sql_wire.contains("RateLimitExceeded"));
	assert!(!wendelon_source.contains("max_queries_per_second"));
	assert!(!wendelon_source.contains("TenantRateLimiter"));
	assert!(!nilket_manifest.contains("tokio"));
	assert!(!worker.contains("max_queries_per_second"));
	assert!(!worker.contains("RateLimited"));
}

#[test]
pub(crate) fn adr056_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr056_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("TenantAdmissionController"));
	assert!(!worker.contains("max_queries_per_second"));
	assert!(!worker.contains("RateLimited"));
}

#[test]
pub(crate) fn adr056_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"TenantRateLimiter",
		"RateLimitExceeded",
		"QueryRateAdmissionV1",
		"TenantRateBucketV1",
		"RateLimitedV1",
	]);
}

#[test]
pub(crate) fn adr056_prior_adr049_050_051_052_053_054_055_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr055_no_descriptor_registry_refresh_or_sync_change() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(!budget.contains("DescriptorRefresh"));
	assert!(coordinator.contains("let admitted = self.admit_query(&context).await?;"));
	assert!(coordinator.contains("execute_query_with_admission(plan, context, query_budget,"));
	assert!(coordinator.contains("descriptor_evolved_table_id"));
	assert!(
		coordinator
			.contains("bind_miss_after_grant_still_resource_exhausted_and_releases_admission")
	);
	assert!(coordinator.contains("adr054_evolved_retry_stays_inside_one_admission_reservation"));
}

#[test]
pub(crate) fn adr055_no_wendelon_server_or_sql_wire_change() {
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	assert!(!sql_wire.contains("Admission"));
	assert!(!sql_wire.contains("Queue"));
	assert!(!wendelon_source.contains("TenantAdmissionController"));
	assert!(!wendelon_source.contains("tenant_admission_queue"));
}

#[test]
pub(crate) fn adr055_resource_exhausted_reused_no_new_sql_code() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");

	assert!(daemon.contains("CatalogError::ResourceExhausted"));
	assert!(daemon.contains("SqlErrorCode::ResourceExhausted"));
	assert!(!wire.contains("AdmissionQueue"));
	assert!(!wire.contains("QueueTimeout"));
}

#[test]
pub(crate) fn adr055_metrics_bounded_configured_plus_other() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	for metric in [
		"tenant_admission_queue_depth",
		"tenant_admission_borrowed_memory_bytes",
		"tenant_admission_borrowed_concurrency",
		"tenant_admission_queue_timeouts_total",
		"tenant_admission_queue_full_rejections_total",
	] {
		assert!(observability.contains(metric), "missing metric {metric}");
	}
	assert!(observability.contains("metric_slots()"));
	assert!(observability.contains("tenant_id=\\\"{label}\\\""));
	assert!(observability.contains("\"other\""));
	assert!(
		observability_test
			.contains("queue_and_borrow_metrics_are_configured_plus_other_no_table_labels")
	);
	for forbidden in ["query_id", "sql_text", "table_id", "fingerprint"] {
		let offenders = observability
			.lines()
			.filter(|line| line.contains("tenant_admission_") && line.contains(forbidden))
			.collect::<Vec<_>>();
		assert!(
			offenders.is_empty(),
			"admission metric gained forbidden label {forbidden}: {offenders:?}"
		);
	}
}

#[test]
pub(crate) fn adr055_prior_adr049_050_051_052_053_054_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr055_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr055_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("TenantAdmissionController"));
	assert!(!worker.contains("AdmissionPolicy"));
	assert!(!worker.contains("tenant_admission_queue"));
}

#[test]
pub(crate) fn adr055_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronTenantAdmissionController",
		"BoundedFairAdmissionQueue",
		"AdmissionPolicyV1",
		"TenantAdmissionControllerV1",
		"QueuedAdmissionV1",
		"AdmissionReservationV1",
	]);
}

#[test]
pub(crate) fn adr053_prior_adr049_050_051_052_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in ["adr049_", "adr050_", "adr051_", "adr052_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr053_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr053_toron_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	for forbidden in [
		"DescriptorSync",
		"max_concurrent_descriptor_syncs",
		"sync_descriptor_for_tenant",
		"get_table_schema_descriptor",
	] {
		assert!(
			!fixture.contains(forbidden),
			"fixture worker contains {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr053_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronDescriptorSync",
		"ToronDescriptorSyncAccountant",
		"TenantDescriptorSyncAccountantV1",
		"TenantDescriptorSyncReservationV1",
		"TenantDescriptorSyncRejectionV1",
		"DescriptorSyncMetricsV1",
		"DescriptorSyncFlightsV1",
	]);
}
