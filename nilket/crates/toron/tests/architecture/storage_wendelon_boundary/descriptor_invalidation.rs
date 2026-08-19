#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, ring_placement::*, sql_planner_surface::*,
};
use super::{
	ddl_admin::*, dml_delete_update::*, returning::*, row_identity::*, wendelon_catalog::*,
};

#[test]
pub(crate) fn adr038_wendelon_descriptor_registry_shared_by_all_adapters() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(catalog.contains("pub struct WendelonDescriptorRegistry"));
	assert!(catalog.contains(
		"descriptors: HashMap<(TenantId, TableId), wendelon_client::TableSchemaDescriptor>"
	));
	for adapter in [
		"pub struct WendelonStorageScan",
		"pub struct WendelonStorageWrite",
		"pub struct WendelonStorageDeleteRows",
		"pub struct WendelonStorageAdmin",
	] {
		let body = brace_blocks_after_marker(&storage, adapter)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{adapter} body"))
			.1;
		assert!(body.contains("WendelonDescriptorRegistry"));
		assert!(!body.contains("HashMap<TableId, wendelon_client::TableSchemaDescriptor>"));
	}
}

#[test]
pub(crate) fn adr063_descriptor_evolved_invalidates_without_retry() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let write_helper = brace_blocks_after_marker(
		&coordinator,
		"async fn execute_write_plan_with_operation_id(",
	)
	.into_iter()
	.next()
	.expect("execute_write_plan")
	.1;

	assert!(coordinator.contains("LogicalPlan::Update { .. }"));
	assert!(
		write_helper.contains("invalidate_descriptor_evolution_on_error(catalog, error).await?")
	);
	assert!(!write_helper.contains("loop"));
	assert!(!write_helper.contains("retry"));
}

#[test]
pub(crate) fn adr038_drop_does_not_invalidate_bound_plans() {
	for path in [
		manifest_dir().join("src/catalog.rs"),
		manifest_dir().join("src/storage.rs"),
		manifest_dir().join("src/coordinator.rs"),
	] {
		let source = std::fs::read_to_string(&path).expect("read source");
		for forbidden in ["TaskSpec", "plan_cache", "invalidate_plan"] {
			assert!(
				!source.contains(forbidden),
				"{} contains {forbidden}",
				path.display()
			);
		}
	}
}

#[test]
pub(crate) fn adr029_select_retry_loop_is_preserved() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let query_impl = coordinator
		.split("impl QueryCoordinator")
		.nth(1)
		.expect("QueryCoordinator impl");
	let loop_body = query_impl
		.split("loop {")
		.nth(1)
		.and_then(|rest| rest.split("match self.executor.execute_bound").next())
		.expect("descriptor-evolution retry loop body");

	assert!(loop_body.contains("let measured_binding = if measure_runtime_verdict"));
	assert!(loop_body.contains("Some("));
	assert!(loop_body.contains("bind_explain_analyze_with_statement_timestamp("));
	assert!(loop_body.contains("None => {"));
	assert!(loop_body.contains("bind_with_statement_timestamp("));
}
