#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, network_tls::*, observability_audit::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};
use super::{
	cancellation::*, descriptor_sync::*, scheduler_dispatch::*, sql_ingress::*,
	tenant_admission::*, worker_lifecycle::*,
};

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
	assert!(
		helper.contains("let planner_config = distributed_planner_config(self.config.clone());"),
		"distributed admission derives its binding config after admission"
	);
	assert!(helper.contains("bind(plan, catalog.as_ref(), &planner_config).await?"));
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
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
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
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
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
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
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
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
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
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
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
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");

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
