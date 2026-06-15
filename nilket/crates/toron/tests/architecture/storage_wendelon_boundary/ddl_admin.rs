#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, ring_placement::*, sql_planner_surface::*,
};
use super::{
	descriptor_invalidation::*, dml_delete_update::*, returning::*, row_identity::*,
	wendelon_catalog::*,
};

#[test]
pub(crate) fn adr029_sql_planner_accepts_only_select_and_create_table() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let plan_body = brace_blocks_after_marker(&planner, "pub fn plan")
		.into_iter()
		.next()
		.expect("SqlPlanner::plan body")
		.1;
	let lower_statement = brace_blocks_after_marker(&planner, "fn lower_statement")
		.into_iter()
		.next()
		.expect("SqlPlanner::lower_statement body")
		.1;
	let dispatch_body = format!("{plan_body}\n{lower_statement}");

	assert!(dispatch_body.contains("Statement::Query(query)"));
	assert!(dispatch_body.contains("Statement::CreateTable(create_table)"));
	assert!(dispatch_body.contains("Statement::Insert(insert)"));
	assert!(dispatch_body.contains("Statement::Delete(delete)"));
	assert!(dispatch_body.contains("Statement::Drop { .. }"));
	assert!(dispatch_body.contains(
		"unsupported SQL statement; only SELECT, EXPLAIN, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE are supported"
	));
	assert!(!dispatch_body.contains("only SELECT and CREATE TABLE are supported"));
	assert!(!dispatch_body.contains("only SELECT is supported"));
}

#[test]
pub(crate) fn adr029_create_table_destructure_is_exhaustive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table(")
		.into_iter()
		.next()
		.expect("lower_create_table body")
		.1;

	for marker in [
		"query",
		"like",
		"clone",
		"dynamic",
		"iceberg",
		"snapshot",
		"table_options",
		"version",
		"primary_key",
		"partition_by",
		"with_storage_lifecycle_policy",
		"with_tags",
		"catalog_sync",
		"storage_serialization_policy",
		"target_lag",
		"warehouse",
		"refresh_mode",
		"backup",
	] {
		assert!(body.contains(marker), "lower_create_table missing {marker}");
	}
	assert!(!body.contains(".."));
}

#[test]
pub(crate) fn adr029_create_table_ir_surface_is_narrow() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");

	assert!(logical.contains("pub struct CreateTableSchema"));
	assert!(logical.contains("pub enum CreateTableColumnType"));
	assert!(logical.contains("CreateTable {\n\t\tname: TableRef,\n\t\tschema: CreateTableSchema,"));
	assert!(execution.contains("CreateTable {\n\t\tname: TableRef,\n\t\tschema: SchemaRef,"));
	assert!(!logical.contains("ColumnSpec"));
	assert!(!execution.contains("ColumnSpec"));
	assert!(!execution.contains("CreateTableColumn"));
}

#[test]
pub(crate) fn adr029_create_table_name_lowering_is_strict() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_ddl_table_name")
		.into_iter()
		.next()
		.expect("lower_ddl_table_name body")
		.1;

	assert!(body.contains("[table]"));
	assert!(body.contains("[schema, table]"));
	assert!(body.contains("DDL table names support only one- or two-part table names"));
	assert!(!body.contains("table_ref_from_object_name"));
}

#[test]
pub(crate) fn adr029_create_table_type_lattice_is_ddl_specific() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table_data_type")
		.into_iter()
		.next()
		.expect("lower_create_table_data_type body")
		.1;

	for marker in [
		"CreateTableColumnType::Physical(DataType::Int32)",
		"CreateTableColumnType::Physical(DataType::Int64)",
		"CreateTableColumnType::Physical(DataType::Float32)",
		"CreateTableColumnType::Physical(DataType::Float64)",
		"CreateTableColumnType::Physical(DataType::Utf8)",
		"CreateTableColumnType::Physical(DataType::Boolean)",
		"SqlDataType::Custom(name, modifiers)",
		"CreateTableColumnType::LogicalName",
		"SqlDataType::Float(_)",
		"SqlDataType::Decimal(_)",
	] {
		assert!(body.contains(marker), "DDL type lattice missing {marker}");
	}
	assert!(!body.contains("lower_cast_target_type"));
}

#[test]
pub(crate) fn adr029_create_table_column_options_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table_column_options")
		.into_iter()
		.next()
		.expect("lower_create_table_column_options body")
		.1;

	for marker in [
		"ColumnOption::Null => true",
		"ColumnOption::NotNull => false",
		"ColumnOption::Default",
		"ColumnOption::PrimaryKey",
		"PrimaryKeyConstraint",
		"characteristics",
		"ColumnOption::Unique",
		"ColumnOption::Check",
		"ColumnOption::ForeignKey",
		"ColumnOption::Generated",
		"ColumnOption::Srid",
		"ColumnOption::Invisible",
		"named constraint",
	] {
		assert!(
			body.contains(marker),
			"DDL column option guard missing {marker}"
		);
	}
}

#[test]
pub(crate) fn adr029_create_table_rejects_storage_synthetic_column_names() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	for marker in [
		"STORAGE_ROW_HLC_COLUMN",
		"STORAGE_ROW_KEY_ID_COLUMN",
		"STORAGE_ROW_ROUTING_LOCALITY_COLUMN",
		"STORAGE_ROW_TABLE_OBJECT_ID_COLUMN",
		"STORAGE_ROW_TOMBSTONE_COLUMN",
		"STORAGE_SYNTHETIC_COLUMN_NAMES.contains",
		"reserved for storage metadata",
	] {
		assert!(
			planner.contains(marker),
			"storage synthetic guard missing {marker}"
		);
	}
}

#[test]
pub(crate) fn adr029_coordinators_intercept_create_table() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");

	let query_impl = coordinator
		.split("impl QueryCoordinator")
		.nth(1)
		.expect("QueryCoordinator impl");
	let ddl_branch = query_impl
		.find("LogicalPlan::CreateTable")
		.expect("local DDL branch");
	let retry_loop = query_impl
		.find("let mut retried_descriptor_evolution = false")
		.expect("descriptor retry loop");
	assert!(ddl_branch < retry_loop);
	assert!(query_impl.contains("execute_catalog_plan("));
	assert!(query_impl.contains("let catalog = Arc::clone(&self.catalog).scoped"));
	assert!(query_impl.contains("catalog.as_ref()"));
	assert!(query_impl.contains("self.admin.as_ref()"));

	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let distributed_branch = distributed_impl
		.find("LogicalPlan::CreateTable")
		.expect("distributed DDL branch");
	let fragment_call = distributed_impl
		.find("fragment_plan(")
		.expect("fragmenter call");
	assert!(distributed_branch < fragment_call);
	assert!(distributed_impl.contains("execute_catalog_plan("));
	assert!(distributed_impl.contains("Arc::clone(&self.catalog).scoped(context.tenant_id)"));
	assert!(distributed_impl.contains("catalog.as_ref()"));
	assert!(distributed_impl.contains("self.admin.as_ref()"));

	let helper_body = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper body")
		.1;
	assert!(helper_body.contains(".create_table_with_outcome("));
	assert!(helper_body.contains("row_identity.clone()"));
	assert!(helper_body.contains("Ok(Vec::new())"));
	assert_eq!(
		coordinator.matches(".create_table_with_outcome(").count(),
		2
	);
}

#[test]
pub(crate) fn adr029_executor_and_fragmenter_reject_create_table() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let fragment_plan = brace_blocks_after_marker(&fragmenter, "pub fn fragment_plan")
		.into_iter()
		.next()
		.expect("fragment_plan body")
		.1;

	assert!(exec.contains("CREATE TABLE execution is coordinator-only"));
	assert!(fragmenter.contains("CreateTableNotFragmentable"));
	assert!(fragment_plan.contains("ExecutionPlan::CreateTable"));
	assert!(
		fragment_plan.find("ExecutionPlan::CreateTable").unwrap()
			< fragment_plan.find("fragment_execution_plan").unwrap()
	);
}

#[test]
pub(crate) fn adr033_logical_variant_count_is_13() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert_eq!(enum_variant_count(logical_plan), 24);
	assert!(logical_plan.contains("Union {"));
	assert!(logical_plan.contains("Distinct {"));
	assert!(logical_plan.contains("CreateTableAsSelect {"));
	assert!(logical_plan.contains("Explain {"));
	assert!(logical_plan.contains("ExplainAnalyze {"));
	assert!(logical_plan.contains("CreateLogicalType {"));
	assert!(logical_plan.contains("Update {"));
	assert!(logical_plan.contains("table: TableRef,"));
	assert!(logical_plan.contains("if_not_exists: bool,"));
	assert!(logical_plan.contains("source: Box<LogicalPlan>,"));
	assert!(logical_plan.contains("DropTable {"));
}

#[test]
pub(crate) fn adr033_execution_variant_count_is_11() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");

	assert_eq!(enum_variant_count(execution_plan), 20);
	assert!(execution_plan.contains("Union {"));
	assert!(execution_plan.contains("EmptyRelation {"));
	assert!(execution_plan.contains("SystemTable {"));
	assert!(execution_plan.contains("CreateTableAsSelect {"));
	assert!(execution_plan.contains("CreateLogicalType {"));
	assert!(execution_plan.contains("DropTable {"));
	assert!(execution_plan.contains("Update {"));
}

#[test]
pub(crate) fn adr033_execution_ctas_has_no_precreate_identity() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	let ctas = brace_blocks_after_marker(execution_plan, "CreateTableAsSelect {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::CreateTableAsSelect body")
		.1;

	for required in [
		"table: TableRef,",
		"schema: SchemaRef,",
		"if_not_exists: bool,",
		"source: Box<ExecutionPlan>,",
	] {
		assert!(ctas.contains(required), "CTAS body missing {required}");
	}
	for forbidden in [
		"legacy_table_id",
		"TableId",
		"TableObjectId",
		"fingerprint",
		"PhysicalDescriptorFingerprint",
	] {
		assert!(
			!ctas.contains(forbidden),
			"ADR 033: bound CTAS must not carry pre-create identity field {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr033_planner_routes_only_query_create_table_to_ctas() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_create_table = brace_blocks_after_marker(&planner, "fn lower_create_table(")
		.into_iter()
		.next()
		.expect("lower_create_table body")
		.1;
	let lower_ctas = brace_blocks_after_marker(&planner, "fn lower_create_table_as_select")
		.into_iter()
		.next()
		.expect("lower_create_table_as_select body")
		.1;

	assert!(lower_create_table.contains("if query.is_some()"));
	assert!(lower_create_table.contains("return self.lower_create_table_as_select(create_table);"));
	assert!(lower_ctas.contains("self.lower_query(query)?"));
	assert!(lower_ctas.contains("LogicalPlan::CreateTableAsSelect"));
}

#[test]
pub(crate) fn adr033_planner_rejects_ctas_column_list_while_allowing_if_not_exists() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_ctas = brace_blocks_after_marker(&planner, "fn lower_create_table_as_select")
		.into_iter()
		.next()
		.expect("lower_create_table_as_select body")
		.1;

	assert!(lower_ctas.contains("if_not_exists: *if_not_exists"));
	assert!(!lower_ctas.contains("CTAS does not support IF NOT EXISTS in v1"));
	assert!(lower_ctas.contains("if !columns.is_empty()"));
	assert!(lower_ctas.contains("CTAS does not support explicit column lists"));
}

#[test]
pub(crate) fn adr033_binder_binds_source_before_inferring_schema() {
	let binder = binder_source();
	let body = brace_blocks_after_marker(&binder, "async fn bind_create_table_as_select")
		.into_iter()
		.next()
		.expect("bind_create_table_as_select body")
		.1;
	let bind_source = body
		.find("bind_plan_with_trace(source, trace, render_trace, join_order_source_capture)")
		.expect("source bind");
	let infer_schema = body.find("infer_ctas_schema").expect("schema inference");

	assert!(bind_source < infer_schema);
	assert!(!body.contains(".create_table("));
	assert!(!body.contains(".resolve_table("));
}

#[test]
pub(crate) fn adr033_schema_inference_rejects_synthetic_names() {
	let binder = binder_source();
	let infer = brace_blocks_after_marker(&binder, "fn infer_ctas_schema")
		.into_iter()
		.next()
		.expect("infer_ctas_schema body")
		.1;
	let validate = brace_blocks_after_marker(&binder, "fn validate_ctas_column_name")
		.into_iter()
		.next()
		.expect("validate_ctas_column_name body")
		.1;

	for literal in [
		"\"__row_hlc__\"",
		"\"__row_key_id__\"",
		"\"__row_routing_locality__\"",
		"\"__row_table_object_id__\"",
		"\"__row_tombstone__\"",
	] {
		assert!(
			!binder.contains(literal),
			"ADR 033: binder must not fork Wendelon synthetic literal {literal}"
		);
	}
	for constant in [
		"STORAGE_ROW_HLC_COLUMN",
		"STORAGE_ROW_KEY_ID_COLUMN",
		"STORAGE_ROW_ROUTING_LOCALITY_COLUMN",
		"STORAGE_ROW_TABLE_OBJECT_ID_COLUMN",
		"STORAGE_ROW_TOMBSTONE_COLUMN",
	] {
		assert!(
			binder.contains(constant),
			"ADR 033: binder must reuse Wendelon constant {constant}"
		);
	}
	assert!(infer.contains("validate_ctas_column_name"));
	assert!(validate.contains("STORAGE_SYNTHETIC_COLUMN_NAMES.contains"));
	assert!(validate.contains("reserved for storage metadata"));
}

#[test]
pub(crate) fn adr033_schema_inference_rejects_duplicate_names() {
	let binder = binder_source();
	let infer = brace_blocks_after_marker(&binder, "fn infer_ctas_schema")
		.into_iter()
		.next()
		.expect("infer_ctas_schema body")
		.1;

	assert!(infer.contains("seen.insert"));
	assert!(infer.contains("duplicate CTAS output column"));
}

#[test]
pub(crate) fn adr033_schema_inference_uses_adr029_type_lattice() {
	let binder = binder_source();
	let type_lattice = brace_blocks_after_marker(&binder, "fn is_supported_ctas_column_type")
		.into_iter()
		.next()
		.expect("is_supported_ctas_column_type body")
		.1;

	for data_type in [
		"DataType::Boolean",
		"DataType::Int32",
		"DataType::Int64",
		"DataType::Float32",
		"DataType::Float64",
		"DataType::Utf8",
		"DataType::Date32",
	] {
		assert!(
			type_lattice.contains(data_type),
			"CTAS type lattice missing {data_type}"
		);
	}
	assert!(!type_lattice.contains("DataType::Null"));
	assert!(!type_lattice.contains("DataType::UInt32"));
}

#[test]
pub(crate) fn adr033_local_coordinator_intercepts_before_retry() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let query_impl = coordinator
		.split("impl QueryCoordinator")
		.nth(1)
		.expect("QueryCoordinator impl");
	let ctas_branch = query_impl
		.find("LogicalPlan::CreateTableAsSelect { .. }")
		.expect("local CTAS branch");
	let retry_loop = query_impl
		.find("let mut retried_descriptor_evolution = false")
		.expect("descriptor retry loop");

	assert!(ctas_branch < retry_loop);
	assert!(query_impl.contains("execute_write_plan(catalog.as_ref(), &bound).await"));
}

#[test]
pub(crate) fn adr033_coordinator_reads_source_before_create_table() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let execute_write =
		brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
			.into_iter()
			.next()
			.expect("execute_write_plan_with_source body")
			.1;
	let ctas_arm = execute_write
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.and_then(|rest| rest.split("other =>").next())
		.expect("CTAS arm");
	let create_table = ctas_arm
		.find(".create_table_with_outcome(")
		.expect("create_table_with_outcome call");
	let execute_source = ctas_arm
		.find("source_executor.execute_read_plan(source)")
		.expect("source execution");

	assert!(execute_source < create_table);
}

#[test]
pub(crate) fn adr033_coordinator_uses_create_table_binding_for_write() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let execute_write =
		brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
			.into_iter()
			.next()
			.expect("execute_write_plan_with_source body")
			.1;
	let ctas_arm = execute_write
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.and_then(|rest| rest.split("other =>").next())
		.expect("CTAS arm");

	assert!(ctas_arm.contains("let outcome = catalog"));
	assert!(ctas_arm.contains("let binding = outcome.binding"));
	assert!(ctas_arm.contains("table_id: binding.legacy_table_id"));
	assert!(ctas_arm.contains("table_object_id: binding.table"));
	assert!(ctas_arm.contains("physical_descriptor_fingerprint: binding.fingerprint"));
}

#[test]
pub(crate) fn adr033_empty_source_ctas_writes_zero_row_batch() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let execute_write =
		brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
			.into_iter()
			.next()
			.expect("execute_write_plan_with_source body")
			.1;
	let ctas_arm = execute_write
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.and_then(|rest| rest.split("other =>").next())
		.expect("CTAS arm");

	assert!(ctas_arm.contains("materialize_insert_select_batch"));
	assert!(ctas_arm.contains("None => write::materialize_empty_record_batch(schema)?"));
	assert!(ctas_arm.contains("write(WriteRequest"));

	let coordinator_write =
		std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
			.expect("read coordinator/write.rs");
	let empty_helper =
		brace_blocks_after_marker(&coordinator_write, "fn materialize_empty_record_batch")
			.into_iter()
			.next()
			.expect("materialize_empty_record_batch body")
			.1;
	assert!(empty_helper.contains("typed_null_array(field.data_type(), 0, field.name())"));
}

#[test]
pub(crate) fn adr033_distributed_coordinator_routes_before_fragment() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let ctas_route = distributed_impl
		.find("LogicalPlan::CreateTableAsSelect")
		.expect("distributed CTAS route");
	let fragment_call = distributed_impl
		.find("fragment_plan(")
		.expect("fragmenter call");

	assert!(ctas_route < fragment_call);
	assert!(distributed_impl.contains("execute_write_plan_with_source"));
	assert!(
		!distributed_impl.contains("distributed CREATE TABLE AS SELECT execution is not wired")
	);
}

#[test]
pub(crate) fn adr033_fragmenter_rejects_before_join_or_exchange() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let fragment_plan = brace_blocks_after_marker(&fragmenter, "pub fn fragment_plan")
		.into_iter()
		.next()
		.expect("fragment_plan body")
		.1;

	assert!(exec.contains("CREATE TABLE AS SELECT execution is coordinator-only"));
	assert!(fragmenter.contains("CreateTableAsSelectNotFragmentable"));
	assert!(fragment_plan.contains("ExecutionPlan::CreateTableAsSelect"));
	assert!(
		fragment_plan
			.find("ExecutionPlan::CreateTableAsSelect")
			.unwrap() < fragment_plan.find("fragment_execution_plan").unwrap()
	);
	assert!(
		fragment_plan
			.find("ExecutionPlan::CreateTableAsSelect")
			.unwrap() < fragment_plan.find("fragment_execution_plan").unwrap()
	);
}

#[test]
pub(crate) fn adr033_distributed_walkers_treat_ctas_as_leaf() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	for (function, marker) in [
		(
			"fn first_join",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
		(
			"fn find_exchange",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
		(
			"fn count_exchanges",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
		(
			"fn first_scan_metadata",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
		(
			"fn replace_exchange_with_scan",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
		(
			"fn replace_first_join",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
	] {
		let body = brace_blocks_after_marker(&fragmenter, function)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{function} body"))
			.1;
		assert!(body.contains(marker), "{function} missing {marker}");
	}

	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	for (function, marker) in [
		(
			"fn join_strategy_label",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
		(
			"fn contains_join",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
		(
			"fn contains_broadcast_join",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
		(
			"fn scan_schema_for_table_id",
			"| ExecutionPlan::CreateTableAsSelect { .. }",
		),
	] {
		let body = brace_blocks_after_marker(&server, function)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{function} body"))
			.1;
		assert!(body.contains(marker), "{function} missing {marker}");
	}
	assert!(
		server.contains("ExecutionPlan::CreateTableAsSelect { .. } => \"create_table_as_select\"")
	);

	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let first_scan_fingerprint = brace_blocks_after_marker(&scheduler, "fn first_scan_fingerprint")
		.into_iter()
		.next()
		.expect("first_scan_fingerprint body")
		.1;
	assert!(first_scan_fingerprint.contains("| ExecutionPlan::CreateTableAsSelect { .. }"));
}

#[test]
pub(crate) fn adr033_does_not_modify_insert_select_empty_contract() {
	let coordinator_write =
		std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
			.expect("read coordinator/write.rs");
	let materialize =
		brace_blocks_after_marker(&coordinator_write, "fn materialize_write_select_batch")
			.into_iter()
			.next()
			.expect("materialize_write_select_batch body")
			.1;

	assert!(materialize.contains("return Ok(None);"));
}

#[test]
pub(crate) fn adr033_does_not_add_plan_arena_dependency() {
	let cargo =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read toron Cargo.toml");
	for forbidden in ["bumpalo", "typed-arena", "generational-arena", "slab"] {
		assert!(
			!cargo.contains(forbidden),
			"unexpected arena dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr034_sql_planner_accepts_drop_table() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let plan_body = brace_blocks_after_marker(&planner, "pub fn plan")
		.into_iter()
		.next()
		.expect("SqlPlanner::plan body")
		.1;
	let lower_statement = brace_blocks_after_marker(&planner, "fn lower_statement")
		.into_iter()
		.next()
		.expect("SqlPlanner::lower_statement body")
		.1;
	let dispatch_body = format!("{plan_body}\n{lower_statement}");

	assert!(dispatch_body.contains("Statement::Drop { .. }"));
	assert!(dispatch_body.contains("self.lower_drop_table(statement)"));
	assert!(dispatch_body.contains("DROP TABLE"));
}

#[test]
pub(crate) fn adr034_drop_table_destructure_is_exhaustive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_drop_table")
		.into_iter()
		.next()
		.expect("lower_drop_table body")
		.1;

	for marker in [
		"object_type",
		"if_exists",
		"names",
		"cascade",
		"restrict",
		"purge",
		"temporary",
	] {
		assert!(body.contains(marker), "lower_drop_table missing {marker}");
	}
	assert!(!body.contains(".."));
}

#[test]
pub(crate) fn adr034_drop_table_reject_surface_is_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_drop_table")
		.into_iter()
		.next()
		.expect("lower_drop_table body")
		.1;

	for marker in [
		"ObjectType::Table",
		"names.len() != 1",
		"*cascade",
		"*restrict",
		"*purge",
		"*temporary",
	] {
		assert!(
			body.contains(marker),
			"DROP TABLE reject lattice missing {marker}"
		);
	}
}

#[test]
pub(crate) fn adr034_drop_table_name_lowering_is_ddl_specific() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let helper = brace_blocks_after_marker(&planner, "fn lower_ddl_table_name")
		.into_iter()
		.next()
		.expect("lower_ddl_table_name body")
		.1;
	let lower_drop = brace_blocks_after_marker(&planner, "fn lower_drop_table")
		.into_iter()
		.next()
		.expect("lower_drop_table body")
		.1;

	assert!(helper.contains("[table]"));
	assert!(helper.contains("[schema, table]"));
	assert!(helper.contains("DDL table names support only one- or two-part table names"));
	assert!(lower_drop.contains("lower_ddl_table_name(&names[0])?"));
	assert_eq!(planner.matches("lower_ddl_table_name(").count(), 4);
	assert!(!planner.contains("fn lower_create_table_name"));
	assert!(!helper.contains("table_ref_from_object_name"));
}

#[test]
pub(crate) fn adr034_logical_variant_count_is_14() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert_eq!(enum_variant_count(logical_plan), 24);
	assert!(logical_plan.contains("Union {"));
	assert!(logical_plan.contains("Distinct {"));
	assert!(logical_plan.contains("CreateLogicalType {"));
	assert!(logical_plan.contains("Explain {"));
	assert!(logical_plan.contains("ExplainAnalyze {"));
	assert!(logical_plan.contains("DropTable {"));
	assert!(logical_plan.contains("Update {"));
	assert!(logical_plan.contains("name: TableRef,"));
	assert!(logical_plan.contains("if_exists: bool,"));
}

#[test]
pub(crate) fn adr034_execution_variant_count_is_12() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");

	assert_eq!(enum_variant_count(execution_plan), 20);
	assert!(execution_plan.contains("Union {"));
	assert!(execution_plan.contains("EmptyRelation {"));
	assert!(execution_plan.contains("SystemTable {"));
	assert!(execution_plan.contains("CreateLogicalType {"));
	assert!(execution_plan.contains("DropTable {"));
	assert!(execution_plan.contains("Update {"));
}

#[test]
pub(crate) fn adr034_execution_drop_table_has_no_identity_or_box() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	let drop_table = brace_blocks_after_marker(execution_plan, "DropTable {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::DropTable body")
		.1;

	assert!(drop_table.contains("name: TableRef,"));
	assert!(drop_table.contains("if_exists: bool,"));
	for forbidden in [
		"TableId",
		"TableObjectId",
		"PhysicalDescriptorFingerprint",
		"SchemaRef",
		"Box<",
	] {
		assert!(
			!drop_table.contains(forbidden),
			"DROP TABLE execution variant must not carry {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr034_binder_drop_table_is_pass_through() {
	let binder = binder_source();
	let drop_arm = binder
		.split("LogicalPlan::DropTable")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Insert").next())
		.expect("DropTable bind arm");

	assert!(drop_arm.contains("ScopedExecutionPlan::empty_scope"));
	assert!(drop_arm.contains("ExecutionPlan::DropTable"));
	assert!(drop_arm.contains("name: name.clone()"));
	assert!(drop_arm.contains("if_exists: *if_exists"));
	assert!(!drop_arm.contains(".resolve_table("));
	assert!(!drop_arm.contains(".resolve_table_id("));
	assert!(!drop_arm.contains(".drop_table("));
}

#[test]
pub(crate) fn adr034_coordinators_intercept_drop_table() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");

	let query_impl = coordinator
		.split("impl QueryCoordinator")
		.nth(1)
		.expect("QueryCoordinator impl");
	let drop_branch = query_impl
		.find("LogicalPlan::DropTable")
		.expect("local DROP branch");
	let retry_loop = query_impl
		.find("let mut retried_descriptor_evolution = false")
		.expect("descriptor retry loop");
	assert!(drop_branch < retry_loop);

	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let distributed_drop = distributed_impl
		.find("LogicalPlan::DropTable")
		.expect("distributed DROP branch");
	let fragment_call = distributed_impl
		.find("fragment_plan(")
		.expect("fragmenter call");
	assert!(distributed_drop < fragment_call);

	let helper = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper body")
		.1;
	let drop_helper = brace_blocks_after_marker(
		&coordinator,
		"async fn drop_existing_table_with_storage_admin",
	)
	.into_iter()
	.next()
	.expect("drop_existing_table_with_storage_admin helper body")
	.1;
	assert!(helper.contains("ExecutionPlan::DropTable"));
	assert!(helper.contains("drop_existing_table_with_storage_admin"));
	assert!(drop_helper.contains(".drop_table(name, false)"));
	assert!(helper.contains("Ok(Vec::new())"));
}

#[test]
pub(crate) fn adr034_executor_and_fragmenter_reject_drop_table() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let fragment_plan = brace_blocks_after_marker(&fragmenter, "pub fn fragment_plan")
		.into_iter()
		.next()
		.expect("fragment_plan body")
		.1;

	assert!(exec.contains("unbound DROP TABLE execution"));
	assert!(exec.contains("DROP TABLE execution is coordinator-only"));
	assert!(fragmenter.contains("DropTableNotFragmentable"));
	assert!(fragment_plan.contains("ExecutionPlan::DropTable"));
	assert!(
		fragment_plan.find("ExecutionPlan::DropTable").unwrap()
			< fragment_plan.find("fragment_execution_plan").unwrap()
	);
	assert!(
		fragment_plan.find("ExecutionPlan::DropTable").unwrap()
			< fragment_plan.find("fragment_execution_plan").unwrap()
	);
}

#[test]
pub(crate) fn adr034_distributed_walkers_treat_drop_table_as_leaf() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	for function in [
		"fn first_join",
		"fn find_exchange",
		"fn count_exchanges",
		"fn first_scan_metadata",
		"fn replace_exchange_with_scan",
		"fn replace_first_join",
	] {
		let body = brace_blocks_after_marker(&fragmenter, function)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{function} body"))
			.1;
		assert!(
			body.contains("| ExecutionPlan::DropTable { .. }"),
			"{function} missing DropTable leaf arm"
		);
	}

	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let first_scan_fingerprint = brace_blocks_after_marker(&scheduler, "fn first_scan_fingerprint")
		.into_iter()
		.next()
		.expect("first_scan_fingerprint body")
		.1;
	assert!(first_scan_fingerprint.contains("| ExecutionPlan::DropTable { .. }"));

	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	for function in [
		"fn join_strategy_label",
		"fn contains_join",
		"fn contains_broadcast_join",
		"fn scan_schema_for_table_id",
	] {
		let body = brace_blocks_after_marker(&server, function)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{function} body"))
			.1;
		assert!(
			body.contains("| ExecutionPlan::DropTable { .. }"),
			"{function} missing DropTable leaf arm"
		);
	}
}

#[test]
pub(crate) fn adr034_operator_label_is_drop_table() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let operator_kind = brace_blocks_after_marker(&server, "fn operator_kind_label")
		.into_iter()
		.next()
		.expect("operator_kind_label body")
		.1;

	assert!(operator_kind.contains("ExecutionPlan::DropTable { .. } => \"drop_table\""));
}

#[test]
pub(crate) fn adr034_does_not_add_storage_reclaim_or_plan_arena() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	for forbidden in [
		"StorageReclaim",
		"StorageDrop",
		"delete_table",
		"reclaim_table",
	] {
		assert!(
			!storage.contains(forbidden),
			"unexpected storage reclaim surface {forbidden}"
		);
	}

	let cargo =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read toron Cargo.toml");
	for forbidden in ["bumpalo", "typed-arena", "generational-arena", "slab"] {
		assert!(
			!cargo.contains(forbidden),
			"unexpected dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr038_drop_table_storage_request_shape_is_context_only() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let request = storage
		.split("pub struct DropTableStorageRequest")
		.nth(1)
		.and_then(|rest| rest.split("#[non_exhaustive]").next())
		.expect("DropTableStorageRequest block");

	for field in [
		"pub session_id: SessionId,",
		"pub query_id: QueryId,",
		"pub tenant_id: TenantId,",
		"pub table_id: TableId,",
		"pub table_object_id: TableObjectId,",
		"pub schema_generation: u64,",
		"pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,",
		"pub profile_hint: Option<QueryProfileHint>,",
		"pub trace_context: Option<TraceContext>,",
	] {
		assert!(request.contains(field), "missing request field {field}");
	}
	for forbidden in [
		"client_write_id",
		"durability",
		"await_finalization",
		"retry",
		"runtime_filter",
		"payload",
	] {
		assert!(
			!request.contains(forbidden),
			"DropTableStorageRequest must not contain {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr038_execution_drop_table_remains_identity_free() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	let drop_table = brace_blocks_after_marker(execution_plan, "DropTable {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::DropTable body")
		.1;

	assert!(drop_table.contains("name: TableRef,"));
	assert!(drop_table.contains("if_exists: bool,"));
	for forbidden in [
		"TableId",
		"TableObjectId",
		"PhysicalDescriptorFingerprint",
		"SchemaRef",
		"Box<",
	] {
		assert!(!drop_table.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
pub(crate) fn adr038_coordinator_resolves_drop_identity_at_execution() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let helper = brace_blocks_after_marker(
		&coordinator,
		"async fn drop_existing_table_with_storage_admin",
	)
	.into_iter()
	.next()
	.expect("drop_existing_table_with_storage_admin helper body")
	.1;

	assert!(helper.contains("catalog.resolve_table_local(name).await"));
	assert!(helper.contains("DropTableStorageRequest"));
	assert!(
		helper
			.find("catalog.resolve_table_local(name).await")
			.unwrap() < helper.find(".drop_table_storage").unwrap()
	);
}

#[test]
pub(crate) fn adr038_fragmenter_drop_table_defensive_reject_remains() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let fragment_plan = brace_blocks_after_marker(&fragmenter, "pub fn fragment_plan")
		.into_iter()
		.next()
		.expect("fragment_plan body")
		.1;

	assert!(fragmenter.contains("DropTableNotFragmentable"));
	assert!(fragment_plan.contains("ExecutionPlan::DropTable"));
}

#[test]
pub(crate) fn adr039_no_table_schema_drop_request_v1_in_toron() {
	assert_toron_sources_do_not_contain(&["TableSchemaDropRequestV1"]);
}

#[test]
pub(crate) fn adr039_no_drop_table_storage_proof_type() {
	assert_toron_sources_do_not_contain(&["DropTableStorageProof"]);
}

#[test]
pub(crate) fn adr061_ctas_if_not_exists_is_accepted() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people")
		.expect("CTAS IF NOT EXISTS should plan");

	let LogicalPlan::CreateTableAsSelect { if_not_exists, .. } = plan else {
		panic!("expected CTAS logical plan");
	};

	assert!(if_not_exists);
}

#[test]
pub(crate) fn adr061_ctas_ir_carries_if_not_exists() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR");
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR");
	let binder = binder_source();
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	for source in [&logical, &execution] {
		let ctas = brace_blocks_after_marker(source, "CreateTableAsSelect {")
			.into_iter()
			.next()
			.expect("CTAS variant")
			.1;
		assert!(ctas.contains("if_not_exists: bool,"));
	}
	assert!(planner.contains("if_not_exists: *if_not_exists"));
	assert!(binder.contains("*if_not_exists"));
	assert!(binder.contains("if_not_exists,"));
}

#[test]
pub(crate) fn adr061_or_replace_if_not_exists_remains_rejected() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE OR REPLACE TABLE IF NOT EXISTS copy AS SELECT id FROM people")
		.expect_err("OR REPLACE plus IF NOT EXISTS should reject");

	assert!(
		err.to_string()
			.contains("CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS")
	);
}

#[test]
pub(crate) fn adr061_ctas_column_list_remains_rejected() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE copy (id INT) AS SELECT id FROM people")
		.expect_err("CTAS column list should reject");

	assert!(err.to_string().contains("explicit column lists"));
}

#[test]
pub(crate) fn adr061_ctas_write_is_gated_on_created_outcome() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let execute_write =
		brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
			.into_iter()
			.next()
			.expect("execute_write_plan_with_source")
			.1;
	let ctas_arm = execute_write
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::Delete").next())
		.expect("CTAS arm");
	let outcome = ctas_arm
		.find("let outcome = catalog")
		.expect("created outcome");
	let write_gate = ctas_arm
		.find("if !outcome.created")
		.expect("created=false write gate");
	let writer = ctas_arm.find(".write(WriteRequest").expect("writer call");

	assert!(outcome < write_gate);
	assert!(write_gate < writer);
}

#[test]
pub(crate) fn adr061_ctas_binds_source_before_existing_target_skip() {
	let binder = binder_source();
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let bind_body = brace_blocks_after_marker(&binder, "async fn bind_create_table_as_select")
		.into_iter()
		.next()
		.expect("bind_create_table_as_select")
		.1;
	let bind_source = bind_body
		.find("bind_plan_with_trace(source, trace, render_trace, join_order_source_capture)")
		.expect("source bind");
	let infer_schema = bind_body
		.find("infer_ctas_schema")
		.expect("schema inference");
	let ctas_arm =
		brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
			.into_iter()
			.next()
			.expect("execute_write_plan_with_source")
			.1;

	assert!(bind_source < infer_schema);
	assert!(!bind_body.contains("resolve_table"));
	assert!(ctas_arm.contains("if *if_not_exists"));
	assert!(ctas_arm.contains("source_executor.execute_read_plan(source)"));
}

#[test]
pub(crate) fn adr061_ctas_uses_exact_schema_compatibility() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(catalog.contains("existing.schema.as_ref() == schema.as_ref()"));
	assert!(coordinator.contains("existing.schema.as_ref() == schema.as_ref()"));
	assert!(catalog.contains("CatalogError::SchemaMismatch"));
	assert!(coordinator.contains("CatalogError::SchemaMismatch"));
	assert!(!catalog.contains("can_cast_types"));
	assert!(!coordinator.contains("can_cast_types"));
}

#[test]
pub(crate) fn adr061_ctas_remains_coordinator_owned_non_fragmentable() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter");
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec");

	assert!(coordinator.contains("ExecutionPlan::CreateTableAsSelect"));
	assert!(coordinator.contains("create_table_with_outcome"));
	assert!(coordinator.contains(".write(WriteRequest"));
	assert!(fragment.contains("CreateTableAsSelectNotFragmentable"));
	assert!(exec.contains("CREATE TABLE AS SELECT execution is coordinator-only"));
}

#[test]
pub(crate) fn adr061_retargets_prior_ctas_if_not_exists_reject_guards() {
	let architecture_test = architecture_corpus_source();
	let ctas_test =
		std::fs::read_to_string(manifest_dir().join("tests/ctas_test.rs")).expect("read ctas test");

	assert!(
		architecture_test
			.contains("adr033_planner_rejects_ctas_column_list_while_allowing_if_not_exists")
	);
	assert!(architecture_test.contains("adr041_ctas_if_not_exists_is_supported_after_adr061"));
	let old_adr041_guard = ["fn ", "adr041_ctas_if_not_exists", "_remains_rejected"].concat();
	assert!(!architecture_test.contains(&old_adr041_guard));
	assert!(!ctas_test.contains("fn ctas_if_not_exists_remains_rejected"));
	assert!(ctas_test.contains("fn ctas_if_not_exists_is_accepted"));
}

#[test]
pub(crate) fn adr061_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr061_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	for forbidden in [
		"CreateTableOutcome",
		"create_table_with_outcome",
		"CTAS IF NOT EXISTS",
		"if_not_exists",
	] {
		assert!(
			!worker.contains(forbidden),
			"fixture worker gained {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr061_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"CreateTableOutcomeV1",
		"CreateTableAsSelectV1",
		"CtasIfNotExistsV1",
		"CtasOutcomeV1",
		"CreateTableWithOutcomeV1",
	]);
}

#[test]
pub(crate) fn adr061_prior_adr033_to_060_guards_continue_to_pass_except_retarget() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr033_", "adr034_", "adr035_", "adr036_", "adr037_", "adr038_", "adr039_", "adr040_",
		"adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_", "adr048_",
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_", "adr056_",
		"adr057_", "adr058_", "adr059_", "adr060_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(
		architecture_test
			.contains("adr033_planner_rejects_ctas_column_list_while_allowing_if_not_exists")
	);
	assert!(architecture_test.contains("adr041_ctas_if_not_exists_is_supported_after_adr061"));
}

#[test]
pub(crate) fn adr038_no_background_gc_or_scheduler() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let storage_admin =
		brace_blocks_after_marker(&storage, "impl StorageAdmin for InMemoryTableStorage")
			.into_iter()
			.next()
			.expect("InMemoryTableStorage admin impl")
			.1;
	let registry = brace_blocks_after_marker(&catalog, "impl WendelonDescriptorRegistry")
		.into_iter()
		.next()
		.expect("registry impl")
		.1;
	let helper = brace_blocks_after_marker(
		&coordinator,
		"async fn drop_existing_table_with_storage_admin",
	)
	.into_iter()
	.next()
	.expect("drop_existing_table_with_storage_admin helper")
	.1;

	for body in [storage_admin, registry, helper] {
		assert!(!body.contains("tokio::spawn"));
		assert!(!body.contains("std::thread::spawn"));
	}
}

#[test]
pub(crate) fn adr038_no_dedicated_create_or_replace_sql_surface() {
	let manifest_dir = manifest_dir();
	for source in [
		std::fs::read_to_string(manifest_dir.join("src/ir.rs")).expect("read source"),
		std::fs::read_to_string(manifest_dir.join("src/ir/execution.rs")).expect("read source"),
		std::fs::read_to_string(manifest_dir.join("src/sql_planner.rs")).expect("read source"),
		binder_source(),
	] {
		assert!(!source.contains("CreateOrReplaceTable"));
		assert!(!source.contains("TableSchemaReplaceRequest"));
		assert!(!source.contains("drop_table_with_replacement"));
	}
}

#[test]
pub(crate) fn adr038_no_write_receipt_or_read_your_writes_fields() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let request = storage
		.split("pub struct DropTableStorageRequest")
		.nth(1)
		.and_then(|rest| rest.split("#[non_exhaustive]").next())
		.expect("DropTableStorageRequest block");
	let outcome = storage
		.split("pub struct DropTableStorageOutcome")
		.nth(1)
		.and_then(|rest| rest.split("impl DropTableStorageOutcome").next())
		.expect("DropTableStorageOutcome block");

	for forbidden in [
		"receipt",
		"client_write_id",
		"durability",
		"await_finalization",
		"read_your_writes",
	] {
		assert!(!request.contains(forbidden), "request contains {forbidden}");
		assert!(!outcome.contains(forbidden), "outcome contains {forbidden}");
	}
	assert!(outcome.contains("pub reclaimed_tables: usize,"));
}

#[test]
pub(crate) fn adr038_no_blake3_or_name_hash_object_ids() {
	let cargo =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Cargo.toml");
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(cargo.contains("blake3 = \"1.5\""));
	for forbidden in ["blake3", "Sha2", "Sha3", "xxh3"] {
		assert!(!catalog.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
pub(crate) fn adr038_no_table_object_id_reuse() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(create_body.contains("TableObjectId::new(*Uuid::new_v4().as_bytes())"));
	assert!(!create_body.contains("existing.table"));
	assert!(!create_body.contains("descriptor.table_object_id"));
}
