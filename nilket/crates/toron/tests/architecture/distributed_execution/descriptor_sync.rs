#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, network_tls::*, observability_audit::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};
use super::{
	cancellation::*, resource_governance::*, scheduler_dispatch::*, sql_ingress::*,
	tenant_admission::*, worker_lifecycle::*,
};

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
	assert!(catalog.contains(
		"TableBinding::from_descriptor_with_logical_types(table_id, &descriptor, resolver)?"
	));
	assert!(catalog.contains("LogicalTypeResolver::with_registry"));
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
