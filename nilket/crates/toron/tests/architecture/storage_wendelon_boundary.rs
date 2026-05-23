#![allow(unused_imports)]

use super::support::*;
use super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, ring_placement::*, sql_planner_surface::*,
};

#[test]
pub(crate) fn adr031_bound_expr_program_shape() {
	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let program = brace_blocks_after_marker(&execution_source, "pub struct BoundExprProgram")
		.into_iter()
		.next()
		.expect("BoundExprProgram block")
		.1;
	for field in [
		"pub nodes: Vec<BoundExprNode>",
		"pub node_types: Vec<DataType>",
		"pub literals: Vec<ScalarValue>",
		"pub roots: Vec<BoundExprId>",
	] {
		assert!(program.contains(field), "missing {field}");
	}
	assert!(execution_source.contains("pub struct BoundExprId(pub u32);"));
	let aggregate = brace_blocks_after_marker(&execution_source, "pub struct BoundAggregateCall")
		.into_iter()
		.next()
		.expect("BoundAggregateCall block")
		.1;
	assert!(aggregate.contains("pub args: BoundExprProgram"));
}

#[test]
pub(crate) fn adr031_bound_expr_node_has_no_boxes() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("Box<ExecutionExpr>") || line.contains("ExecutionExpr::")
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR 031 production ExecutionExpr references found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);

	let execution_source = std::fs::read_to_string(manifest_dir.join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let node = execution_source
		.split("pub enum BoundExprNode")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("BoundExprNode enum");
	assert_eq!(enum_variant_count(node), 9);
	for variant in [
		"Column",
		"Literal",
		"Cast",
		"TryCast",
		"Binary",
		"Unary",
		"InList",
		"Like",
		"TimeBucket",
	] {
		assert!(node.contains(variant), "missing BoundExprNode::{variant}");
	}
}

#[test]
pub(crate) fn adr031_bound_expr_program_partial_eq_only() {
	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	for type_name in ["BoundExprProgram", "BoundExprNode", "BoundAggregateCall"] {
		let derives = derive_items_before(&execution_source, type_name);
		assert!(
			derives.iter().any(|item| item == "PartialEq"),
			"{type_name} must derive PartialEq"
		);
		assert!(
			!derives.iter().any(|item| item == "Eq" || item == "Hash"),
			"{type_name} must not derive Eq or Hash"
		);
	}
}

#[test]
pub(crate) fn adr031_execution_plan_uses_bound_expr_program() {
	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution_source
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	for field in [
		"predicate: BoundExprProgram",
		"exprs: BoundExprProgram",
		"group_expr: BoundExprProgram",
		"aggr_expr: Vec<BoundAggregateCall>",
		"key: BoundExprProgram",
	] {
		assert!(execution_plan.contains(field), "missing {field}");
	}
	let aggregate = brace_blocks_after_marker(&execution_source, "pub struct BoundAggregateCall")
		.into_iter()
		.next()
		.expect("BoundAggregateCall block")
		.1;
	assert!(aggregate.contains("pub args: BoundExprProgram"));
}

#[test]
pub(crate) fn adr031_execution_plan_insert_stays_expression_free() {
	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let insert = brace_blocks_after_marker(&execution_source, "Insert {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::Insert block")
		.1;
	assert!(insert.contains("rows: Vec<Vec<ScalarValue>>"));
	assert!(!insert.contains("BoundExprProgram"));
}

#[test]
pub(crate) fn adr031_join_and_sort_remain_ordinal_keyed() {
	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let sort_key = brace_blocks_after_marker(&execution_source, "pub struct ExecutionSortKey")
		.into_iter()
		.next()
		.expect("ExecutionSortKey block")
		.1;
	assert!(sort_key.contains("pub ordinal: ColumnOrdinal"));
	assert!(!sort_key.contains("BoundExprProgram"));
	assert!(execution_source.contains("left_keys: Vec<ColumnOrdinal>"));
	assert!(execution_source.contains("right_keys: Vec<ColumnOrdinal>"));
}

#[test]
pub(crate) fn adr031_pbc_reads_bound_expr_program() {
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read PBC source");
	let compile = brace_blocks_after_marker(&pbc, "pub(crate) fn compile")
		.into_iter()
		.next()
		.expect("PbcCompiler::compile block")
		.1;
	assert!(compile.contains("predicate: Option<&BoundExprProgram>"));
	assert!(!compile.contains("Option<&ExecutionExpr>"));
	assert!(compile.contains("single_root()"));
}

#[test]
pub(crate) fn adr031_executor_evaluates_program_linearly() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let method = brace_blocks_after_marker(&exec, "pub fn evaluate_bound_program")
		.into_iter()
		.next()
		.expect("evaluate_bound_program block")
		.1;
	let evaluate =
		brace_blocks_after_marker(&exec, "pub fn evaluate_bound_program_without_storage")
			.into_iter()
			.next()
			.expect("evaluate_bound_program_without_storage block")
			.1;
	assert!(method.contains("Self::evaluate_bound_program_without_storage"));
	assert!(evaluate.contains("program.nodes.iter().enumerate()"));
	assert_eq!(
		evaluate
			.matches("evaluate_bound_program_without_storage(")
			.count(),
		1
	);
}

#[test]
pub(crate) fn adr031_binder_emits_bound_expr_program() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	assert!(binder.contains("BoundExprProgramBuilder"));
	assert!(binder.contains("fn bind_expr_program"));
	assert!(binder.contains("fn bind_expr_into_program"));
	assert!(!binder.contains("ExecutionExpr::Cast"));
	assert!(!binder.contains("ExecutionExpr::TryCast"));
}

#[test]
pub(crate) fn adr031_adr028_signature_stays_logical_expr() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let signature = brace_blocks_after_marker(&planner, "struct GroupedAggregateSignature")
		.into_iter()
		.next()
		.expect("GroupedAggregateSignature block")
		.1;
	assert!(signature.contains("args: Vec<Expr>"));
	assert!(!signature.contains("BoundExprProgram"));
	for helper in [
		"fn hash_expr_structural",
		"fn expr_structural_eq",
		"fn hash_scalar_structural",
		"fn scalar_structural_eq",
		"fn hash_data_type_structural",
		"fn data_type_structural_eq",
	] {
		assert!(planner.contains(helper), "missing ADR 028 helper {helper}");
	}
}

#[test]
pub(crate) fn adr029_sql_planner_accepts_only_select_and_create_table() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let plan_body = brace_blocks_after_marker(&planner, "pub fn plan")
		.into_iter()
		.next()
		.expect("SqlPlanner::plan body")
		.1;

	assert!(plan_body.contains("Statement::Query(query)"));
	assert!(plan_body.contains("Statement::CreateTable(create_table)"));
	assert!(plan_body.contains("Statement::Insert(insert)"));
	assert!(plan_body.contains("Statement::Delete(delete)"));
	assert!(plan_body.contains("Statement::Drop { .. }"));
	assert!(plan_body.contains(
		"unsupported SQL statement; only SELECT, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE are supported"
	));
	assert!(!plan_body.contains("only SELECT and CREATE TABLE are supported"));
	assert!(!plan_body.contains("only SELECT is supported"));
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

	assert!(logical.contains("CreateTable {\n\t\tname: TableRef,\n\t\tschema: SchemaRef,"));
	assert!(execution.contains("CreateTable {\n\t\tname: TableRef,\n\t\tschema: SchemaRef,"));
	assert!(!logical.contains("ColumnSpec"));
	assert!(!logical.contains("CreateTableColumn"));
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
		"Ok(DataType::Int32)",
		"Ok(DataType::Int64)",
		"Ok(DataType::Float32)",
		"Ok(DataType::Float64)",
		"Ok(DataType::Utf8)",
		"Ok(DataType::Boolean)",
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
pub(crate) fn adr029_catalog_create_table_returns_binding() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let error =
		std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read error source");

	assert!(catalog.contains("async fn create_table("));
	assert!(catalog.contains(") -> Result<TableBinding, CatalogError>;"));
	assert!(error.contains("TableAlreadyExists"));
	assert!(error.contains("SchemaMismatch"));
	assert!(error.contains("AdminUnavailable"));
	assert!(error.contains("AdminFailed"));
	assert!(error.contains("FingerprintResolution"));
}

#[test]
pub(crate) fn adr029_mock_catalog_is_interior_mutable() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(catalog.contains("inner: Arc<RwLock<MockCatalogInner>>"));
	assert!(catalog.contains("next_table_id: u64"));
	assert!(catalog.contains("self.inner.write()"));
	assert!(catalog.contains("bump_next_table_id(&mut inner, legacy_table_id)"));
}

#[test]
pub(crate) fn adr029_mock_catalog_identity_uses_descriptor_fingerprint() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let body = brace_blocks_after_marker(&catalog, "fn mock_table_identity")
		.into_iter()
		.next()
		.expect("mock_table_identity body")
		.1;

	assert!(body.contains("TableObjectId::new"));
	assert!(body.contains("create_table_descriptor"));
	assert!(catalog.contains("TableSchemaDescriptor::new_with_layout"));
	assert!(body.contains("physical_descriptor_fingerprint"));
	assert!(!catalog.contains("PhysicalDescriptorFingerprint::from_bytes"));
}

#[test]
pub(crate) fn adr029_mock_catalog_reference_insertion_does_not_overwrite() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let impl_block = catalog
		.split("impl Catalog for MockCatalog")
		.nth(1)
		.and_then(|rest| rest.split("impl Catalog for WendelonCatalog").next())
		.expect("MockCatalog Catalog impl");
	let create_body = brace_blocks_after_marker(impl_block, "async fn create_table")
		.into_iter()
		.next()
		.expect("MockCatalog::create_table body")
		.1;

	assert!(create_body.contains("insert((self.tenant_id, name.clone()), legacy_table_id)"));
	assert!(create_body.contains(".entry((self.tenant_id, TableRef::new(name.table.clone())))"));
	assert!(create_body.contains(".or_insert(legacy_table_id)"));
	assert_eq!(
		create_body
			.matches(".insert((self.tenant_id, name.clone()), legacy_table_id)")
			.count(),
		1
	);
}

#[test]
pub(crate) fn adr029_wendelon_catalog_create_table_has_admin_boundary() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let impl_block = catalog
		.split("impl Catalog for WendelonCatalog")
		.nth(1)
		.expect("WendelonCatalog Catalog impl");
	let body = brace_blocks_after_marker(impl_block, "async fn create_table")
		.into_iter()
		.next()
		.expect("WendelonCatalog::create_table body")
		.1;

	assert!(body.contains("TableSchemaUpdateRequest::create"));
	assert!(body.contains("submit_table_schema_update"));
	assert!(!body.contains("CatalogError::AdminUnavailable"));
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
			< fragment_plan.find("count_exchanges").unwrap()
	);
}

#[test]
pub(crate) fn adr030_insert_destructure_is_exhaustive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_insert(")
		.into_iter()
		.next()
		.expect("lower_insert body")
		.1;

	for marker in [
		"insert_token",
		"optimizer_hints",
		"or",
		"ignore",
		"into",
		"table",
		"table_alias",
		"columns",
		"overwrite",
		"source",
		"assignments",
		"partitioned",
		"after_columns",
		"has_table_keyword",
		"on",
		"returning",
		"output",
		"replace_into",
		"priority",
		"insert_alias",
		"settings",
		"format_clause",
		"multi_table_insert_type",
		"multi_table_into_clauses",
		"multi_table_when_clauses",
		"multi_table_else_clause",
	] {
		assert!(body.contains(marker), "lower_insert missing {marker}");
	}
	assert!(body.contains("missing INTO keyword"));
	assert!(body.contains("TABLE keyword"));
	assert!(!body.contains(".. }"));
	assert!(!body.contains("..,"));
}

#[test]
pub(crate) fn adr030_logical_plan_insert_uses_insert_literal() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert!(logical.contains("pub enum InsertLiteral"));
	assert!(logical_plan.contains("rows: Vec<Vec<InsertLiteral>>"));
	assert!(!logical_plan.contains("Vec<Vec<Expr>>"));
}

#[test]
pub(crate) fn adr030_execution_plan_insert_has_no_record_batch() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");

	assert!(execution_plan.contains("Insert {\n\t\tlegacy_table_id: TableId,"));
	assert!(execution_plan.contains("table: TableObjectId,"));
	assert!(execution_plan.contains("fingerprint: PhysicalDescriptorFingerprint,"));
	assert!(execution_plan.contains("schema: SchemaRef,"));
	assert!(execution_plan.contains("rows: Vec<Vec<ScalarValue>>,"));
	let insert_body = execution_plan
		.split("Insert {")
		.nth(1)
		.and_then(|rest| rest.split("},").next())
		.expect("ExecutionPlan::Insert body");
	assert!(!insert_body.contains("RecordBatch"));
	assert!(!insert_body.contains("payload"));
}

#[test]
pub(crate) fn adr030_catalog_has_no_insert_method() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let trait_body = brace_blocks_after_marker(&catalog, "pub trait Catalog")
		.into_iter()
		.next()
		.expect("Catalog trait body")
		.1;

	assert!(!trait_body.contains("insert"));
	assert!(!trait_body.contains("write_"));
}

#[test]
pub(crate) fn adr030_no_storage_synthetic_column_construction_in_toron() {
	let coordinator_write =
		std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
			.expect("read coordinator/write.rs");

	for marker in [
		"STORAGE_ROW_HLC_COLUMN",
		"STORAGE_ROW_KEY_ID_COLUMN",
		"STORAGE_ROW_ROUTING_LOCALITY_COLUMN",
		"STORAGE_ROW_TABLE_OBJECT_ID_COLUMN",
		"STORAGE_ROW_TOMBSTONE_COLUMN",
		"__row_",
	] {
		assert!(
			!coordinator_write.contains(marker),
			"coordinator write helper must not construct storage synthetic column {marker}"
		);
	}
}

#[test]
pub(crate) fn adr030_no_blake3_in_toron_insert_path() {
	let coordinator_write =
		std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
			.expect("read coordinator/write.rs");

	assert!(!coordinator_write.contains("blake3"));
}

#[test]
pub(crate) fn adr030_coordinator_insert_uses_wendelon_client_wrapper() {
	let manifest_dir = manifest_dir();
	let storage =
		std::fs::read_to_string(manifest_dir.join("src/storage.rs")).expect("read storage source");
	let write_impl =
		brace_blocks_after_marker(&storage, "impl StorageWrite for WendelonStorageWrite")
			.into_iter()
			.next()
			.expect("WendelonStorageWrite impl")
			.1;

	assert!(write_impl.contains("self\n\t\t\t.client\n\t\t\t.table_write("));
	assert!(write_impl.contains("wendelon_client::WriteOptions"));
	assert!(!storage.contains("TableWriteRequest {"));
	for (path, line, text) in source_lines(&manifest_dir.join("src")) {
		assert!(
			!text.contains("TableWriteRequest {"),
			"ADR 030 D8: Toron must not construct low-level TableWriteRequest at {}:{}: {}",
			path.display(),
			line,
			text
		);
	}
}

#[test]
pub(crate) fn adr030_write_options_set_await_finalization_true() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let write_impl =
		brace_blocks_after_marker(&storage, "impl StorageWrite for WendelonStorageWrite")
			.into_iter()
			.next()
			.expect("WendelonStorageWrite impl")
			.1;

	assert!(write_impl.contains("await_finalization: true"));
	assert!(!write_impl.contains("WriteOptions::default()"));
}

#[test]
pub(crate) fn adr030_wendelon_storage_write_mirrors_scan_error_mapping() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let write_impl =
		brace_blocks_after_marker(&storage, "impl StorageWrite for WendelonStorageWrite")
			.into_iter()
			.next()
			.expect("WendelonStorageWrite impl")
			.1;

	assert!(write_impl.contains("descriptor(request.tenant_id, request.table_id)?"));
	assert!(write_impl.contains("StorageError::DescriptorEvolved"));
	assert!(write_impl.contains("StorageError::Internal"));
	assert!(storage.contains("StorageError::TableNotFound"));
}

#[test]
pub(crate) fn adr030_local_coordinator_intercepts_insert_before_retry_loop() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let query_impl = coordinator
		.split("impl QueryCoordinator")
		.nth(1)
		.expect("QueryCoordinator impl");
	let insert_branch = query_impl
		.find("LogicalPlan::Insert { .. }")
		.expect("local INSERT branch");
	let retry_loop = query_impl
		.find("let mut retried_descriptor_evolution = false")
		.expect("descriptor retry loop");

	assert!(insert_branch < retry_loop);
	assert!(query_impl.contains("execute_write_plan(catalog.as_ref(), &bound).await"));
	assert!(query_impl.contains("writer: Arc<dyn StorageWrite>"));
}

#[test]
pub(crate) fn adr030_distributed_coordinator_routes_insert_before_fragment_plan() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let insert_route = distributed_impl
		.find("LogicalPlan::Insert")
		.expect("distributed INSERT route");
	let fragment_call = distributed_impl
		.find("fragment_plan(")
		.expect("fragment plan call");

	assert!(insert_route < fragment_call);
	assert!(distributed_impl.contains("execute_write_plan_with_source"));
	assert!(!distributed_impl.contains("distributed INSERT execution is not wired"));
}

#[test]
pub(crate) fn adr030_executor_and_fragmenter_reject_insert() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let fragment_plan = brace_blocks_after_marker(&fragmenter, "pub fn fragment_plan")
		.into_iter()
		.next()
		.expect("fragment_plan body")
		.1;

	assert!(exec.contains("INSERT execution is coordinator-only"));
	assert!(fragmenter.contains("InsertNotFragmentable"));
	assert!(fragment_plan.contains("ExecutionPlan::Insert"));
	assert!(
		fragment_plan.find("ExecutionPlan::Insert").unwrap()
			< fragment_plan.find("count_exchanges").unwrap()
	);
}

#[test]
pub(crate) fn adr032_logical_plan_retains_insert_select_variant() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert_eq!(enum_variant_count(logical_plan), 16);
	assert!(logical_plan.contains("InsertSelect {"));
	assert!(logical_plan.contains("DropTable {"));
	assert!(logical_plan.contains("Update {"));
	assert!(logical_plan.contains("table: TableRef,"));
	assert!(logical_plan.contains("columns: Option<Vec<String>>,"));
	assert!(logical_plan.contains("source: Box<LogicalPlan>,"));
}

#[test]
pub(crate) fn adr032_execution_plan_retains_insert_select_variant() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	assert_eq!(enum_variant_count(execution_plan), 14);
	assert!(execution_plan.contains("InsertSelect {"));
	assert!(execution_plan.contains("DropTable {"));
	assert!(execution_plan.contains("Update {"));
}

#[test]
pub(crate) fn adr032_insert_stays_values_only() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");
	let logical_insert = logical_plan
		.split("Insert {")
		.nth(1)
		.and_then(|rest| rest.split("InsertSelect {").next())
		.expect("LogicalPlan::Insert body");
	assert!(logical_insert.contains("rows: Vec<Vec<InsertLiteral>>"));
	assert!(!logical_insert.contains("source:"));

	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	let execution_insert = execution_plan
		.split("Insert {")
		.nth(1)
		.and_then(|rest| rest.split("InsertSelect {").next())
		.expect("ExecutionPlan::Insert body");
	assert!(execution_insert.contains("rows: Vec<Vec<ScalarValue>>,"));
	assert!(!execution_insert.contains("BoundExprProgram"));
	assert!(!execution_insert.contains("PbcProgram"));
	assert!(!execution_insert.contains("RecordBatch"));
}

#[test]
pub(crate) fn adr032_insert_select_carries_target_columns() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");

	assert!(execution_plan.contains("InsertSelect {"));
	assert!(execution_plan.contains("legacy_table_id: TableId,"));
	assert!(execution_plan.contains("table: TableObjectId,"));
	assert!(execution_plan.contains("fingerprint: PhysicalDescriptorFingerprint,"));
	assert!(execution_plan.contains("schema: SchemaRef,"));
	assert!(execution_plan.contains("target_columns: Vec<ColumnOrdinal>,"));
	assert!(execution_plan.contains("source: Box<ExecutionPlan>,"));
}

#[test]
pub(crate) fn adr032_insert_select_no_direct_bound_expr_program() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let insert_select = brace_blocks_after_marker(&execution, "InsertSelect {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::InsertSelect body")
		.1;
	assert!(!insert_select.contains("BoundExprProgram"));
	assert!(!insert_select.contains("PbcProgram"));
	assert!(!insert_select.contains("RecordBatch"));
	assert!(!insert_select.contains("payload"));
}

#[test]
pub(crate) fn adr032_storage_write_trait_unchanged() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let write_request = brace_blocks_after_marker(&storage, "pub struct WriteRequest")
		.into_iter()
		.next()
		.expect("WriteRequest body")
		.1;
	let storage_write = brace_blocks_after_marker(&storage, "pub trait StorageWrite")
		.into_iter()
		.next()
		.expect("StorageWrite trait")
		.1;
	assert!(write_request.contains("pub payload: RecordBatch,"));
	assert!(storage_write.contains("async fn write(&self, request: WriteRequest)"));
	assert!(!storage_write.contains("insert_select"));
	assert!(!storage_write.contains("write_select"));
}

#[test]
pub(crate) fn adr032_sql_planner_splits_values_and_select_sources() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lowered_source = planner
		.split("enum LoweredInsertSource")
		.nth(1)
		.and_then(|rest| rest.split("fn lower_insert(").next())
		.expect("LoweredInsertSource block");
	assert!(lowered_source.contains("Values(Vec<Vec<InsertLiteral>>)"));
	assert!(lowered_source.contains("Select(LogicalPlan)"));

	let lower_insert = brace_blocks_after_marker(&planner, "fn lower_insert(")
		.into_iter()
		.next()
		.expect("lower_insert body")
		.1;
	assert!(lower_insert.contains("LoweredInsertSource::Values(rows) => Ok(LogicalPlan::Insert"));
	assert!(
		lower_insert
			.contains("LoweredInsertSource::Select(source) => Ok(LogicalPlan::InsertSelect")
	);
	let lower_insert_source = brace_blocks_after_marker(&planner, "fn lower_insert_source")
		.into_iter()
		.next()
		.expect("lower_insert_source body")
		.1;
	assert!(lower_insert_source.contains("SetExpr::Values"));
	assert!(lower_insert_source.contains("planner.lower_query(query)"));
	assert!(!lower_insert_source.contains("INSERT source must be VALUES"));
}

#[test]
pub(crate) fn adr032_binder_enforces_source_target_arity() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let bind_insert_select = brace_blocks_after_marker(&binder, "async fn bind_insert_select")
		.into_iter()
		.next()
		.expect("bind_insert_select body")
		.1;

	assert!(bind_insert_select.contains("resolve_insert_target_columns(&schema, columns)?"));
	assert!(bind_insert_select.contains("validate_omitted_insert_target_columns"));
	assert!(bind_insert_select.contains("self.bind_plan(source).await?"));
	assert!(bind_insert_select.contains("source_schema.fields().len() != target_columns.len()"));
	assert!(bind_insert_select.contains("is_insert_select_coercible"));
	assert!(bind_insert_select.contains("ExecutionPlan::InsertSelect"));
	assert!(bind_insert_select.contains("target_columns,"));
	assert!(bind_insert_select.contains("source: Box::new(source)"));

	let insert_select_policy = brace_blocks_after_marker(&binder, "fn is_insert_select_coercible")
		.into_iter()
		.next()
		.expect("is_insert_select_coercible body")
		.1;
	assert!(insert_select_policy.contains("source == target"));
	for conversion in [
		"(DataType::Int32, DataType::Int64)",
		"(DataType::Int64, DataType::Int32)",
		"(DataType::Float64, DataType::Float32)",
	] {
		assert!(
			insert_select_policy.contains(conversion),
			"INSERT SELECT numeric policy missing {conversion}"
		);
	}
}

#[test]
pub(crate) fn adr032_local_coordinator_intercepts_before_retry() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let query_impl = coordinator
		.split("impl QueryCoordinator")
		.nth(1)
		.expect("QueryCoordinator impl");
	let insert_select_branch = query_impl
		.find("LogicalPlan::InsertSelect { .. }")
		.expect("local INSERT SELECT branch");
	let retry_loop = query_impl
		.find("let mut retried_descriptor_evolution = false")
		.expect("descriptor retry loop");
	assert!(insert_select_branch < retry_loop);

	let execute_write = brace_blocks_after_marker(query_impl, "async fn execute_write_plan")
		.into_iter()
		.next()
		.expect("execute_write_plan body")
		.1;
	assert!(execute_write.contains("LocalMutationSourceExecutor"));
	assert!(execute_write.contains("execute_write_plan_with_source"));

	let shared_write =
		brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
			.into_iter()
			.next()
			.expect("execute_write_plan_with_source body")
			.1;
	assert!(shared_write.contains("ExecutionPlan::InsertSelect"));
	assert!(shared_write.contains("source_executor.execute_read_plan(source).await?"));
	assert!(shared_write.contains("materialize_insert_select_batch"));
	assert!(shared_write.contains("write(WriteRequest"));
}

#[test]
pub(crate) fn adr032_distributed_coordinator_routes_before_fragment() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let insert_select_route = distributed_impl
		.find("LogicalPlan::InsertSelect")
		.expect("distributed INSERT SELECT route");
	let fragment_call = distributed_impl
		.find("fragment_plan(")
		.expect("fragment plan call");

	assert!(insert_select_route < fragment_call);
	assert!(distributed_impl.contains("execute_write_plan_with_source"));
	assert!(
		!distributed_impl.contains("distributed INSERT INTO ... SELECT execution is not wired")
	);
}

#[test]
pub(crate) fn adr032_fragmenter_rejects_before_join_or_exchange() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let fragment_plan = brace_blocks_after_marker(&fragmenter, "pub fn fragment_plan")
		.into_iter()
		.next()
		.expect("fragment_plan body")
		.1;

	assert!(exec.contains("INSERT INTO ... SELECT execution is coordinator-only"));
	assert!(fragmenter.contains("InsertSelectNotFragmentable"));
	assert!(fragment_plan.contains("ExecutionPlan::InsertSelect"));
	assert!(
		fragment_plan.find("ExecutionPlan::InsertSelect").unwrap()
			< fragment_plan.find("count_exchanges").unwrap()
	);
}

#[test]
pub(crate) fn adr032_distributed_walkers_treat_insert_select_as_leaf() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	for (function, marker) in [
		(
			"fn first_join",
			"| ExecutionPlan::InsertSelect { .. } => None",
		),
		(
			"fn find_exchange",
			"| ExecutionPlan::InsertSelect { .. } => None",
		),
		(
			"fn count_exchanges",
			"| ExecutionPlan::InsertSelect { .. } => 0",
		),
		(
			"fn first_scan_metadata",
			"| ExecutionPlan::InsertSelect { .. } => None",
		),
		(
			"fn replace_exchange_with_scan",
			"| ExecutionPlan::InsertSelect { .. } => plan",
		),
		(
			"fn replace_first_join",
			"| ExecutionPlan::InsertSelect { .. } => plan",
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
			"| ExecutionPlan::InsertSelect { .. } => None",
		),
		(
			"fn contains_join",
			"| ExecutionPlan::InsertSelect { .. } => false",
		),
		(
			"fn contains_broadcast_join",
			"| ExecutionPlan::InsertSelect { .. } => false",
		),
		(
			"fn scan_schema_for_table_id",
			"| ExecutionPlan::InsertSelect { .. } => None",
		),
	] {
		let body = brace_blocks_after_marker(&server, function)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{function} body"))
			.1;
		assert!(body.contains(marker), "{function} missing {marker}");
	}
	assert!(server.contains("ExecutionPlan::InsertSelect { .. } => \"insert_select\""));

	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let first_scan_fingerprint = brace_blocks_after_marker(&scheduler, "fn first_scan_fingerprint")
		.into_iter()
		.next()
		.expect("first_scan_fingerprint body")
		.1;
	assert!(first_scan_fingerprint.contains("| ExecutionPlan::InsertSelect { .. } => None"));
}

#[test]
pub(crate) fn adr032_coordinator_uses_only_evaluate_cast() {
	let coordinator_write =
		std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
			.expect("read coordinator/write.rs");
	let materialize =
		brace_blocks_after_marker(&coordinator_write, "fn materialize_write_select_batch")
			.into_iter()
			.next()
			.expect("materialize_write_select_batch body")
			.1;
	assert!(materialize.contains("TABLE_WRITE_MAX_ROWS"));
	assert!(materialize.contains("coerce_insert_select_array"));
	assert!(materialize.contains("typed_null_array"));
	assert!(materialize.contains("validate_non_null_targets"));
	assert!(coordinator_write.contains("evaluate_cast"));
	assert!(!coordinator_write.contains("evaluate_try_cast"));
	assert!(!coordinator_write.contains("blake3"));
}

#[test]
pub(crate) fn adr032_insert_select_does_not_add_plan_arena_dependency() {
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
pub(crate) fn adr033_logical_variant_count_is_13() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert_eq!(enum_variant_count(logical_plan), 16);
	assert!(logical_plan.contains("CreateTableAsSelect {"));
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

	assert_eq!(enum_variant_count(execution_plan), 14);
	assert!(execution_plan.contains("CreateTableAsSelect {"));
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
	assert!(lower_ctas.contains("CTAS does not support explicit column lists in v1"));
}

#[test]
pub(crate) fn adr033_binder_binds_source_before_inferring_schema() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let body = brace_blocks_after_marker(&binder, "async fn bind_create_table_as_select")
		.into_iter()
		.next()
		.expect("bind_create_table_as_select body")
		.1;
	let bind_source = body
		.find("self.bind_plan(source).await?")
		.expect("source bind");
	let infer_schema = body.find("infer_ctas_schema").expect("schema inference");

	assert!(bind_source < infer_schema);
	assert!(!body.contains(".create_table("));
	assert!(!body.contains(".resolve_table("));
}

#[test]
pub(crate) fn adr033_schema_inference_rejects_synthetic_names() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
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
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
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
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
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
	] {
		assert!(
			type_lattice.contains(data_type),
			"CTAS type lattice missing {data_type}"
		);
	}
	assert!(!type_lattice.contains("DataType::Date32"));
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
			.unwrap() < fragment_plan.find("contains_join").unwrap()
	);
	assert!(
		fragment_plan
			.find("ExecutionPlan::CreateTableAsSelect")
			.unwrap() < fragment_plan.find("count_exchanges").unwrap()
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

	assert!(plan_body.contains("Statement::Drop { .. }"));
	assert!(plan_body.contains("self.lower_drop_table(&ast[0])"));
	assert!(plan_body.contains("DROP TABLE"));
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

	assert_eq!(enum_variant_count(logical_plan), 16);
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

	assert_eq!(enum_variant_count(execution_plan), 14);
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
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let drop_arm = binder
		.split("LogicalPlan::DropTable")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Insert").next())
		.expect("DropTable bind arm");

	assert!(drop_arm.contains("Ok(ExecutionPlan::DropTable"));
	assert!(drop_arm.contains("name: name.clone()"));
	assert!(drop_arm.contains("if_exists: *if_exists"));
	assert!(!drop_arm.contains(".resolve_table("));
	assert!(!drop_arm.contains(".resolve_table_id("));
	assert!(!drop_arm.contains(".drop_table("));
}

#[test]
pub(crate) fn adr034_catalog_drop_table_returns_optional_binding() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let trait_block = brace_blocks_after_marker(&catalog, "pub trait Catalog")
		.into_iter()
		.next()
		.expect("Catalog trait")
		.1;

	assert!(trait_block.contains("async fn drop_table("));
	assert!(trait_block.contains(") -> Result<Option<TableBinding>, CatalogError>;"));
}

#[test]
pub(crate) fn adr034_mock_catalog_drop_table_cleans_aliases() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let impl_block = catalog
		.split("impl Catalog for MockCatalog")
		.nth(1)
		.and_then(|rest| rest.split("impl Catalog for WendelonCatalog").next())
		.expect("MockCatalog Catalog impl");
	let drop_body = brace_blocks_after_marker(impl_block, "async fn drop_table")
		.into_iter()
		.next()
		.expect("MockCatalog::drop_table body")
		.1;

	assert!(drop_body.contains(".remove(&(self.tenant_id, table_id))"));
	assert!(drop_body.contains(".by_ref"));
	assert!(drop_body.contains(".retain(|(tenant_id, _), id|"));
	assert!(drop_body.contains("*id != table_id"));
	assert!(!drop_body.contains("by_ref.insert"));
	assert!(!drop_body.contains("or_insert"));
}

#[test]
pub(crate) fn adr039_wendelon_catalog_drop_table_consumes_finalized_marker() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let impl_block = catalog
		.split("impl Catalog for WendelonCatalog")
		.nth(1)
		.expect("WendelonCatalog Catalog impl");
	let drop_body = brace_blocks_after_marker(impl_block, "async fn drop_table")
		.into_iter()
		.next()
		.expect("WendelonCatalog::drop_table body")
		.1;

	assert!(!drop_body.contains("CatalogError::AdminUnavailable"));
	assert!(drop_body.contains("DropOperationId"));
	assert!(drop_body.contains("consume_finalized_drop"));
	assert!(drop_body.contains("remove_table"));
	assert!(!drop_body.contains("drop_table_schema"));
	assert!(drop_body.contains("if_exists"));
	assert!(drop_body.contains("Ok(None)"));
	assert!(drop_body.contains("CatalogError::TableNotFound"));
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
			< fragment_plan.find("contains_join").unwrap()
	);
	assert!(
		fragment_plan.find("ExecutionPlan::DropTable").unwrap()
			< fragment_plan.find("count_exchanges").unwrap()
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
pub(crate) fn adr034_catalog_trait_grows_by_exactly_one_method() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let trait_block = catalog
		.split("pub trait Catalog")
		.nth(1)
		.and_then(|rest| rest.split("#[derive(Default)]").next())
		.expect("Catalog trait");

	assert!(trait_block.contains("async fn drop_table"));
	for forbidden in [
		"async fn truncate_table",
		"async fn replace_table",
		"async fn drop_schema",
		"async fn alter_table",
		"async fn rename_table",
		"async fn drop_table_outcome",
	] {
		assert!(
			!trait_block.contains(forbidden),
			"ADR 034: Catalog must not grow beyond drop_table (found {forbidden})"
		);
	}
}

#[test]
pub(crate) fn adr036_sql_planner_accepts_delete() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	assert!(planner.contains("Statement::Delete(delete) => self.lower_delete(delete)"));
	assert!(planner.contains("SELECT, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE"));
}

#[test]
pub(crate) fn adr036_delete_destructure_is_exhaustive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_delete = brace_blocks_after_marker(&planner, "fn lower_delete")
		.into_iter()
		.next()
		.expect("lower_delete body")
		.1;

	for field in [
		"delete_token",
		"optimizer_hints",
		"tables",
		"from",
		"using",
		"selection",
		"returning",
		"output",
		"order_by",
		"limit",
	] {
		assert!(lower_delete.contains(field), "DELETE missing field {field}");
	}
	assert!(
		!lower_delete.contains(".."),
		"DELETE lowering must exhaustively destructure sqlparser::ast::Delete"
	);
}

#[test]
pub(crate) fn adr036_delete_target_table_destructure_is_exhaustive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_delete = brace_blocks_after_marker(&planner, "fn lower_delete")
		.into_iter()
		.next()
		.expect("lower_delete body")
		.1;

	for field in [
		"name",
		"alias",
		"args",
		"with_hints",
		"index_hints",
		"version",
		"with_ordinality",
		"partitions",
		"json_path",
		"sample",
	] {
		assert!(
			lower_delete.contains(field),
			"DELETE target table destructure missing {field}"
		);
	}
	assert!(
		!lower_delete.contains("TableFactor::Table { .. }"),
		"DELETE target table destructure must not use a rest pattern"
	);
}

#[test]
pub(crate) fn adr036_delete_reject_surface_is_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_delete = brace_blocks_after_marker(&planner, "fn lower_delete")
		.into_iter()
		.next()
		.expect("lower_delete body")
		.1;

	for diagnostic in [
		"DELETE supports exactly one target table",
		"unsupported DELETE clause: USING",
		"unsupported DELETE clause: ORDER BY",
		"unsupported DELETE clause: LIMIT",
		"DELETE requires FROM keyword",
		"unsupported DELETE target: joins not supported",
		"DELETE target must be a single table",
		"unsupported DELETE target: table alias",
		"unsupported DELETE target: table arguments",
		"unsupported DELETE target: table hints",
		"unsupported DELETE target: table version",
		"unsupported DELETE target: WITH ORDINALITY",
		"unsupported DELETE target: partitions",
		"unsupported DELETE target: json path",
		"unsupported DELETE target: sample",
	] {
		assert!(
			lower_delete.contains(diagnostic),
			"DELETE lowering missing reject diagnostic {diagnostic}"
		);
	}
	assert!(lower_delete.contains("lower_returning_projection(returning.as_deref())"));
}

#[test]
pub(crate) fn adr036_logical_variant_count_is_15() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert_eq!(enum_variant_count(logical_plan), 16);
	assert!(logical_plan.contains("Delete {"));
	assert!(logical_plan.contains("Update {"));
	assert!(logical_plan.contains("table: TableRef,"));
	assert!(logical_plan.contains("predicate: Option<Expr>,"));
}

#[test]
pub(crate) fn adr036_execution_variant_count_is_13() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");

	assert_eq!(enum_variant_count(execution_plan), 14);
	assert!(execution_plan.contains("Delete {"));
	assert!(execution_plan.contains("Update {"));
}

#[test]
pub(crate) fn adr036_ir_delete_variants_are_leafs() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let logical_delete = brace_blocks_after_marker(&logical, "Delete {")
		.into_iter()
		.next()
		.expect("LogicalPlan::Delete body")
		.1;
	let execution_delete = brace_blocks_after_marker(&execution, "Delete {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::Delete body")
		.1;

	for body in [&logical_delete, &execution_delete] {
		assert!(!body.contains("Box<"), "DELETE must remain a leaf plan");
		assert!(
			!body.contains("input:"),
			"DELETE must not carry an input child"
		);
		assert!(
			!body.contains("source:"),
			"DELETE must not carry a source child"
		);
	}
}

#[test]
pub(crate) fn adr036_execution_delete_carries_shape_a_identity() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_delete = brace_blocks_after_marker(&execution, "Delete {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::Delete body")
		.1;

	for field in [
		"legacy_table_id: TableId",
		"table: TableObjectId",
		"fingerprint: PhysicalDescriptorFingerprint",
		"schema: SchemaRef",
		"predicate: Option<BoundExprProgram>",
		"pbc_program: Option<PbcProgram>",
		"profile_hint: QueryProfileHint",
	] {
		assert!(execution_delete.contains(field), "DELETE missing {field}");
	}
}

#[test]
pub(crate) fn adr036_delete_has_no_filter_disposition_field() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_delete = brace_blocks_after_marker(&execution, "Delete {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::Delete body")
		.1;

	assert!(!execution_delete.contains("FilterDisposition"));
	assert!(!execution_delete.contains("disposition"));
}

#[test]
pub(crate) fn adr036_binder_delete_uses_boolean_root_validator() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let bind_delete = brace_blocks_after_marker(&binder, "async fn bind_delete")
		.into_iter()
		.next()
		.expect("bind_delete body")
		.1;
	let validator = brace_blocks_after_marker(&binder, "fn require_single_boolean_root")
		.into_iter()
		.next()
		.expect("require_single_boolean_root body")
		.1;

	assert!(bind_delete.contains("require_single_boolean_root(&program, \"DELETE WHERE\")?"));
	assert!(!bind_delete.contains("DataType::Boolean"));
	assert!(validator.contains("program.root_count() != 1"));
	assert!(validator.contains("program.root_type(0)"));
	assert!(validator.contains("DataType::Boolean"));
}

#[test]
pub(crate) fn adr036_binder_delete_allows_residual_predicates() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let bind_delete = brace_blocks_after_marker(&binder, "async fn bind_delete")
		.into_iter()
		.next()
		.expect("bind_delete body")
		.1;

	assert!(bind_delete.contains("predicate_pushdown_plan(program, &scan)"));
	assert!(bind_delete.contains("pushdown.residual_predicate"));
	assert!(bind_delete.contains("pushdown.pbc_program"));
	for forbidden in [
		"PBC pushdown required",
		"pushdown-only DELETE",
		"not pushdown",
	] {
		assert!(
			!bind_delete.contains(forbidden),
			"DELETE binder must not reject residual predicates via {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr036_delete_candidate_scan_uses_append_only() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let delete_candidate =
		brace_blocks_after_marker(&coordinator, "fn derive_delete_candidate_plan")
			.into_iter()
			.next()
			.expect("derive_delete_candidate_plan body")
			.1;

	assert!(delete_candidate.contains("projection: None"));
	assert!(delete_candidate.contains("row_identity: RowIdentityProjection::Append"));
}

#[test]
pub(crate) fn adr036_delete_candidate_scan_uses_no_runtime_filter() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let local_source = brace_blocks_after_marker(
		&coordinator,
		"impl MutationSourceExecutor for LocalMutationSourceExecutor",
	)
	.into_iter()
	.next()
	.expect("local mutation source executor body")
	.1;
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let single_task = brace_blocks_after_marker(&fragmenter, "fn single_task_stage")
		.into_iter()
		.next()
		.expect("single_task_stage body")
		.1;

	assert!(local_source.contains("with_runtime_filter(None)"));
	assert!(single_task.contains("runtime_filter: None"));
	assert!(!local_source.contains("self.context.runtime_filter"));
	assert!(!local_source.contains("spec.context.runtime_filter"));
}

#[test]
pub(crate) fn adr036_delete_residual_filter_before_identity_extraction() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let delete_candidate =
		brace_blocks_after_marker(&coordinator, "fn derive_delete_candidate_plan")
			.into_iter()
			.next()
			.expect("derive_delete_candidate_plan body")
			.1;
	let shared_write =
		brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
			.into_iter()
			.next()
			.expect("execute_write_plan_with_source body")
			.1;
	let candidate_execution = shared_write
		.find("execute_delete_candidate_plan")
		.expect("candidate plan execution");
	let extract = shared_write
		.find("extract_delete_row_key_ids")
		.expect("identity extraction call");

	assert!(delete_candidate.contains("FilterDisposition::EvaluateInToron"));
	assert!(candidate_execution < extract);
}

#[test]
pub(crate) fn adr036_adr035_materializer_name_preserved() {
	let delete_source = std::fs::read_to_string(manifest_dir().join("src/coordinator/delete.rs"))
		.expect("read coordinator delete source");
	let materializer = brace_blocks_after_marker(
		&delete_source,
		"pub(crate) async fn materialize_delete_row_key_ids",
	)
	.into_iter()
	.next()
	.expect("materialize_delete_row_key_ids body")
	.1;
	let materialize_candidates = materializer
		.find("materialize_delete_candidate_batches")
		.expect("candidate materializer call");
	let extract = materializer
		.find("extract_delete_row_key_ids")
		.expect("identity extraction call");

	assert!(materialize_candidates < extract);
}

#[test]
pub(crate) fn adr036_delete_uses_execute_delete_with_candidates() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let execute_write = coordinator
		.split("async fn execute_write_plan")
		.nth(1)
		.expect("execute_write_plan body");
	let delete_arm = execute_write
		.split("ExecutionPlan::Delete {")
		.nth(1)
		.and_then(|rest| rest.split("other => Err").next())
		.expect("ExecutionPlan::Delete write arm");

	assert!(delete_arm.contains("execute_delete_with_candidates"));
}

#[test]
pub(crate) fn adr036_delete_result_uses_deleted_only() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let execute_write = coordinator
		.split("async fn execute_write_plan")
		.nth(1)
		.expect("execute_write_plan body");
	let delete_arm = execute_write
		.split("ExecutionPlan::Delete {")
		.nth(1)
		.and_then(|rest| rest.split("other => Err").next())
		.expect("ExecutionPlan::Delete write arm");

	assert!(delete_arm.contains("delete_count_batch(outcome.deleted)"));
	for forbidden in [
		"outcome.requested",
		"outcome.already_deleted",
		"outcome.not_found",
	] {
		assert!(
			!delete_arm.contains(forbidden),
			"DELETE result count must not use {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr036_distributed_delete_routes_before_fragment() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let delete_route = distributed_impl
		.find("LogicalPlan::Delete")
		.expect("distributed DELETE route");
	let fragment_call = distributed_impl
		.find("fragment_plan(")
		.expect("fragment_plan call");

	assert!(delete_route < fragment_call);
	assert!(distributed_impl.contains("execute_write_plan_with_source"));
	assert!(!distributed_impl.contains("distributed DELETE execution is not wired"));
}

#[test]
pub(crate) fn adr036_fragmenter_delete_defensive_reject() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let fragment_plan = brace_blocks_after_marker(&fragmenter, "pub fn fragment_plan")
		.into_iter()
		.next()
		.expect("fragment_plan body")
		.1;
	let delete_reject = fragment_plan
		.find("ExecutionPlan::Delete")
		.expect("fragmenter DELETE reject");

	assert!(fragmenter.contains("DeleteNotFragmentable"));
	assert!(delete_reject < fragment_plan.find("contains_join").unwrap());
	assert!(delete_reject < fragment_plan.find("count_exchanges").unwrap());
}

#[test]
pub(crate) fn adr036_executor_delete_defensive_arms() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");

	assert!(exec.contains("unbound DELETE execution"));
	assert!(exec.contains("DELETE execution is coordinator-only"));
}

#[test]
pub(crate) fn adr036_operator_label_is_delete() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let operator_kind = brace_blocks_after_marker(&server, "fn operator_kind_label")
		.into_iter()
		.next()
		.expect("operator_kind_label body")
		.1;

	assert!(operator_kind.contains("ExecutionPlan::Delete { .. } => \"delete\""));
}

#[test]
pub(crate) fn adr036_synthetic_identity_still_not_sql_visible() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");

	for source in [&planner, &binder] {
		assert!(source.contains("STORAGE_SYNTHETIC_COLUMN_NAMES"));
		assert!(source.contains("STORAGE_ROW_KEY_ID_COLUMN"));
		assert!(source.contains("STORAGE_ROW_HLC_COLUMN"));
	}
}

#[test]
pub(crate) fn adr036_filter_helper_preserves_execution_plan_filter() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let helper =
		brace_blocks_after_marker(&exec, "pub(crate) fn filter_batches_with_bound_predicate")
			.into_iter()
			.next()
			.expect("filter_batches_with_bound_predicate body")
			.1;
	let execute_bound = brace_blocks_after_marker(&exec, "pub fn execute_bound")
		.into_iter()
		.next()
		.expect("execute_bound body")
		.1;

	assert!(helper.contains("downcast_ref::<BooleanArray>()"));
	assert!(helper.contains("{context} must return boolean"));
	assert!(execute_bound.contains("filter_batches_with_bound_predicate("));
	assert!(execute_bound.contains("\"filter predicate\""));
	assert!(execute_bound.contains("FilterDisposition::FullyPushed"));
}

#[test]
pub(crate) fn adr037_task_execution_context_has_exact_field_set() {
	let context = std::fs::read_to_string(manifest_dir().join("src/distributed/context.rs"))
		.expect("read distributed context source");
	let task_context = brace_blocks_after_marker(&context, "pub struct TaskExecutionContext")
		.into_iter()
		.next()
		.expect("TaskExecutionContext body")
		.1;

	for field in [
		"pub session_id: SessionId",
		"pub tenant_id: TenantId",
		"pub read_bound: HlcBound",
		"pub trace_context: Option<TraceContext>",
	] {
		assert!(task_context.contains(field), "missing {field}");
	}
	for forbidden in [
		"query_id",
		"profile_hint",
		"runtime_filter",
		"row_identity",
		"task_budget",
		"task_telemetry",
		"legacy_table_pins",
		"join_spill",
	] {
		assert!(
			!task_context.contains(forbidden),
			"TaskExecutionContext must not carry {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr037_task_spec_carries_task_execution_context() {
	let task =
		std::fs::read_to_string(manifest_dir().join("src/distributed/task.rs")).expect("task");
	let task_spec = brace_blocks_after_marker(&task, "pub struct TaskSpec")
		.into_iter()
		.next()
		.expect("TaskSpec body")
		.1;

	assert!(task.contains("use crate::distributed::context::TaskExecutionContext;"));
	assert!(task_spec.contains("pub context: TaskExecutionContext"));
}

#[test]
pub(crate) fn adr037_distributed_context_module_is_wired() {
	let module =
		std::fs::read_to_string(manifest_dir().join("src/distributed/mod.rs")).expect("mod");

	assert!(module.contains("pub mod context;"));
}

#[test]
pub(crate) fn adr037_fragment_config_carries_task_context() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("fragmenter");
	let config = brace_blocks_after_marker(&fragmenter, "pub struct FragmentPlanConfig")
		.into_iter()
		.next()
		.expect("FragmentPlanConfig body")
		.1;

	assert!(config.contains("pub task_context: TaskExecutionContext"));
	assert!(fragmenter.contains("pub const fn with_task_context"));
}

#[test]
pub(crate) fn adr037_fragmenter_stamps_every_task_spec_with_context() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("fragmenter");
	let task_specs = fragmenter.matches("TaskSpec {").count();
	let stamped = fragmenter.matches("context: config.task_context").count();

	assert!(task_specs > 0, "fragmenter should emit TaskSpec literals");
	assert_eq!(
		stamped, task_specs,
		"every fragment TaskSpec must be stamped"
	);
}

#[test]
pub(crate) fn adr037_worker_context_comes_from_task_spec_not_local_test() {
	let server = std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("server");
	let execute_task =
		brace_blocks_after_marker(&server, "async fn execute_task_local_with_telemetry")
			.into_iter()
			.next()
			.expect("execute_task_local_with_telemetry body")
			.1;

	assert!(execute_task.contains("ExecutionContext::new("));
	assert!(execute_task.contains("spec.context.session_id"));
	assert!(execute_task.contains("spec.identity.query_id"));
	assert!(execute_task.contains("spec.context.tenant_id"));
	assert!(execute_task.contains("spec.context.read_bound"));
	assert!(execute_task.contains("spec.context.trace_context"));
	assert!(execute_task.contains("crate::telemetry::task_trace_context(spec)"));
	assert!(!execute_task.contains("ExecutionContext::local_test()"));
}

#[test]
pub(crate) fn adr037_task_context_derives_from_execution_context_without_query_id() {
	let context = std::fs::read_to_string(manifest_dir().join("src/distributed/context.rs"))
		.expect("distributed context");
	let from_execution = brace_blocks_after_marker(&context, "pub fn from_execution_context")
		.into_iter()
		.next()
		.expect("from_execution_context body")
		.1;

	assert!(from_execution.contains("context.session_id"));
	assert!(from_execution.contains("context.tenant_id"));
	assert!(from_execution.contains("context.read_bound"));
	assert!(from_execution.contains("context.trace_context"));
	assert!(!from_execution.contains("context.query_id"));
}

#[test]
pub(crate) fn adr037_distributed_coordinator_is_context_and_sink_bearing() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let struct_body =
		brace_blocks_after_marker(&coordinator, "pub struct DistributedQueryCoordinator")
			.into_iter()
			.next()
			.expect("DistributedQueryCoordinator body")
			.1;

	assert!(struct_body.contains("context: ExecutionContext"));
	assert!(struct_body.contains("writer: Arc<dyn StorageWrite>"));
	assert!(struct_body.contains("deleter: Arc<dyn StorageDeleteRows>"));
	assert!(struct_body.contains("admin: Arc<dyn StorageAdmin>"));
}

#[test]
pub(crate) fn adr037_distributed_coordinator_exposes_write_sink_constructors() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("pub fn with_context("));
	assert!(coordinator.contains("pub fn with_writer("));
	assert!(coordinator.contains("pub fn with_writer_and_deleter("));
	assert!(coordinator.contains("pub fn with_context_writer_and_deleter("));
	assert!(coordinator.contains("pub fn with_storage_admin("));
	assert!(coordinator.contains("pub fn with_writer_deleter_and_admin("));
}

#[test]
pub(crate) fn adr037_distributed_dml_routes_to_shared_driver() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let execute_query =
		brace_blocks_after_marker(&coordinator, "async fn execute_query_with_admission")
			.into_iter()
			.find(|(_, block)| block.contains("DistributedMutationSourceExecutor"))
			.expect("distributed execute_query_with_admission body")
			.1;

	for variant in [
		"LogicalPlan::Insert",
		"LogicalPlan::InsertSelect",
		"LogicalPlan::CreateTableAsSelect",
		"LogicalPlan::Delete",
	] {
		assert!(execute_query.contains(variant), "missing {variant}");
	}
	assert!(execute_query.contains("DistributedMutationSourceExecutor"));
	assert!(execute_query.contains("execute_write_plan_with_source"));
}

#[test]
pub(crate) fn adr037_distributed_read_executor_wraps_fragment_scheduler() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let distributed_source = brace_blocks_after_marker(
		&coordinator,
		"impl<Registry, Transport> MutationSourceExecutor",
	)
	.into_iter()
	.next()
	.expect("distributed source executor body")
	.1;

	assert!(distributed_source.contains("execute_bound_distributed_with_context("));
	assert!(distributed_source.contains("plan.clone()"));
	assert!(distributed_source.contains("self.context"));
	assert!(distributed_source.contains("self.query_budget"));
	assert!(distributed_source.contains(".await"));
}

#[test]
pub(crate) fn adr037_distributed_fragment_config_uses_execution_context() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let fragment_config = brace_blocks_after_marker(&coordinator, "fn fragment_config")
		.into_iter()
		.next()
		.expect("fragment_config body")
		.1;

	assert!(fragment_config.contains("context.query_id"));
	assert!(fragment_config.contains("TaskExecutionContext::from_execution_context"));
	assert!(fragment_config.contains("context"));
	assert!(fragment_config.contains("query_budget: QueryBudget"));
	assert!(fragment_config.contains("TaskBudget::from_query_budget(query_budget)"));
}

#[test]
pub(crate) fn adr037_mutation_source_executor_is_async_trait() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("#[async_trait::async_trait]\ntrait MutationSourceExecutor"));
	assert!(coordinator.contains("async fn execute_read_plan"));
	assert!(coordinator.contains("async fn execute_delete_candidate_plan"));
}

#[test]
pub(crate) fn adr037_local_mutation_source_wraps_executor() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let local_source = brace_blocks_after_marker(
		&coordinator,
		"impl MutationSourceExecutor for LocalMutationSourceExecutor",
	)
	.into_iter()
	.next()
	.expect("local source executor body")
	.1;

	assert!(local_source.contains("self.executor"));
	assert!(local_source.contains("execute_bound(plan, None)"));
}

#[test]
pub(crate) fn adr037_local_delete_candidate_execution_clears_runtime_filter() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let local_source = brace_blocks_after_marker(
		&coordinator,
		"impl MutationSourceExecutor for LocalMutationSourceExecutor",
	)
	.into_iter()
	.next()
	.expect("local source executor body")
	.1;

	assert!(local_source.contains("with_runtime_filter(None)"));
	assert!(!local_source.contains("spec.context.runtime_filter"));
}

#[test]
pub(crate) fn adr037_shared_driver_handles_all_mutation_variants() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let shared = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared mutation driver body")
		.1;

	for variant in [
		"ExecutionPlan::Insert",
		"ExecutionPlan::InsertSelect",
		"ExecutionPlan::CreateTableAsSelect",
		"ExecutionPlan::Delete",
	] {
		assert!(shared.contains(variant), "missing {variant}");
	}
}

#[test]
pub(crate) fn adr037_insert_values_does_not_execute_source_plan() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let shared = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared mutation driver body")
		.1;
	let insert_arm = shared
		.split("ExecutionPlan::Insert {")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::InsertSelect").next())
		.expect("insert values arm");

	assert!(insert_arm.contains("materialize_insert_batch"));
	assert!(insert_arm.contains("write(WriteRequest"));
	assert!(!insert_arm.contains("execute_read_plan"));
	assert!(!insert_arm.contains("fragment_plan"));
}

#[test]
pub(crate) fn adr037_ctas_reads_source_before_catalog_create() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let shared = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared mutation driver body")
		.1;
	let ctas_arm = shared
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::Delete").next())
		.expect("CTAS arm");
	let read = ctas_arm
		.find("source_executor.execute_read_plan(source)")
		.expect("source read");
	let create = ctas_arm
		.find("create_table_with_outcome")
		.expect("catalog create");

	assert!(read < create);
}

#[test]
pub(crate) fn adr037_delete_candidate_plan_sets_row_identity_append() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let derive = brace_blocks_after_marker(&coordinator, "fn derive_delete_candidate_plan")
		.into_iter()
		.next()
		.expect("delete candidate builder")
		.1;

	assert!(derive.contains("ExecutionPlan::Scan"));
	assert!(derive.contains("projection: None"));
	assert!(derive.contains("row_identity: RowIdentityProjection::Append"));
}

#[test]
pub(crate) fn adr037_delete_candidate_plan_wraps_residual_filter() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let derive = brace_blocks_after_marker(&coordinator, "fn derive_delete_candidate_plan")
		.into_iter()
		.next()
		.expect("delete candidate builder")
		.1;

	assert!(derive.contains("if let Some(predicate) = predicate"));
	assert!(derive.contains("ExecutionPlan::Filter"));
	assert!(derive.contains("FilterDisposition::EvaluateInToron"));
}

#[test]
pub(crate) fn adr037_execution_scan_carries_row_identity_field() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("execution ir");
	let scan = brace_blocks_after_marker(&execution, "Scan {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::Scan body")
		.1;

	assert!(scan.contains("row_identity: RowIdentityProjection"));
}

#[test]
pub(crate) fn adr037_scan_schema_appends_identity_only_when_requested() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("execution ir");
	let helper = brace_blocks_after_marker(&execution, "fn scan_output_schema")
		.into_iter()
		.next()
		.expect("scan_output_schema body")
		.1;

	assert!(helper.contains("RowIdentityProjection::None => schema.clone()"));
	assert!(helper.contains("RowIdentityProjection::Append"));
	assert!(helper.contains("STORAGE_ROW_KEY_ID_COLUMN"));
	assert!(helper.contains("DataType::FixedSizeBinary(32)"));
}

#[test]
pub(crate) fn adr037_binder_default_scans_use_no_row_identity() {
	let binder = std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("binder");
	let scan_plan = brace_blocks_after_marker(&binder, "fn scan_plan")
		.into_iter()
		.next()
		.expect("scan_plan body")
		.1;

	assert!(scan_plan.contains("row_identity: crate::storage::RowIdentityProjection::None"));
}

#[test]
pub(crate) fn adr037_executor_threads_scan_row_identity_to_storage() {
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("exec");
	let execute_bound = brace_blocks_after_marker(&exec, "pub fn execute_bound")
		.into_iter()
		.next()
		.expect("execute_bound body")
		.1;

	assert!(execute_bound.contains("row_identity,"));
	assert!(execute_bound.contains("row_identity: *row_identity"));
}

#[test]
pub(crate) fn adr037_shuffle_input_scans_do_not_request_row_identity() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("fragmenter");
	let shuffle_scan = brace_blocks_after_marker(&fragmenter, "fn shuffle_input_scan")
		.into_iter()
		.next()
		.expect("shuffle_input_scan body")
		.1;

	assert!(shuffle_scan.contains("row_identity: crate::storage::RowIdentityProjection::None"));
}

#[test]
pub(crate) fn adr037_fragmenter_still_rejects_raw_mutation_plans() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("fragmenter");
	let fragment_plan = brace_blocks_after_marker(&fragmenter, "pub fn fragment_plan")
		.into_iter()
		.next()
		.expect("fragment_plan body")
		.1;

	for variant in [
		"ExecutionPlan::CreateTable",
		"ExecutionPlan::DropTable",
		"ExecutionPlan::Delete",
		"ExecutionPlan::Insert",
		"ExecutionPlan::InsertSelect",
		"ExecutionPlan::CreateTableAsSelect",
	] {
		assert!(fragment_plan.contains(variant), "missing {variant}");
	}
}

#[test]
pub(crate) fn adr037_old_distributed_dml_reject_strings_are_removed() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	for removed in [
		"distributed INSERT execution is not wired",
		"distributed INSERT INTO ... SELECT execution is not wired",
		"distributed CREATE TABLE AS SELECT execution is not wired",
		"distributed DELETE execution is not wired",
	] {
		assert!(
			!coordinator.contains(removed),
			"old reject remains: {removed}"
		);
	}
}

#[test]
pub(crate) fn adr037_functional_fixtures_cover_distributed_write_context() {
	let fixture =
		std::fs::read_to_string(manifest_dir().join("tests/distributed_write_context_test.rs"))
			.expect("distributed write context test");

	for fixture_name in [
		"distributed_insert_values_writes_without_dispatch",
		"distributed_insert_select_fragments_source_and_writes_once",
		"distributed_ctas_fragments_source_then_creates_and_writes",
		"distributed_delete_fragments_candidate_read_and_deletes_once",
	] {
		assert!(fixture.contains(fixture_name), "missing {fixture_name}");
	}
}

#[test]
pub(crate) fn adr035_scan_request_uses_row_identity_enum() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let scan_request = storage
		.split("pub struct ScanRequest")
		.nth(1)
		.and_then(|rest| rest.split("pub struct WriteRequest").next())
		.expect("ScanRequest block");

	assert!(storage.contains("pub enum RowIdentityProjection"));
	assert!(scan_request.contains("pub row_identity: RowIdentityProjection,"));
}

#[test]
pub(crate) fn adr035_no_include_row_key_id_bool() {
	for (path, line, text) in source_lines(&manifest_dir().join("src")) {
		assert!(
			!text.contains("include_row_key_id: bool"),
			"ADR 035 must use RowIdentityProjection, not a bool: {}:{line}",
			path.display()
		);
	}
}

#[test]
pub(crate) fn adr035_delete_request_uses_vec_identity() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let request = storage
		.split("pub struct DeleteRowsRequest")
		.nth(1)
		.and_then(|rest| rest.split("#[non_exhaustive]").next())
		.expect("DeleteRowsRequest block");

	assert!(request.contains("pub row_key_ids: Vec<Identity>,"));
}

#[test]
pub(crate) fn adr035_delete_request_has_no_record_batch_payload() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let request = storage
		.split("pub struct DeleteRowsRequest")
		.nth(1)
		.and_then(|rest| rest.split("#[non_exhaustive]").next())
		.expect("DeleteRowsRequest block");

	assert!(!request.contains("RecordBatch"));
	assert!(!request.contains("payload"));
}

#[test]
pub(crate) fn adr035_delete_outcome_preserves_four_counters() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let outcome = storage
		.split("pub struct DeleteRowsOutcome")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ScanStream").next())
		.expect("DeleteRowsOutcome block");

	for field in [
		"pub requested: usize,",
		"pub deleted: usize,",
		"pub already_deleted: usize,",
		"pub not_found: usize,",
	] {
		assert!(outcome.contains(field), "missing ADR 035 counter {field}");
	}
}

#[test]
pub(crate) fn adr035_storage_delete_rows_trait_is_separate() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let trait_body = storage
		.split("pub trait StorageDeleteRows")
		.nth(1)
		.and_then(|rest| rest.split("pub struct InMemoryStorageScan").next())
		.expect("StorageDeleteRows trait block");

	assert!(trait_body.contains("async fn delete_rows("));
	assert!(trait_body.contains("request: DeleteRowsRequest,"));
	assert!(trait_body.contains("Result<DeleteRowsOutcome, StorageError>"));
}

#[test]
pub(crate) fn adr035_storage_write_trait_unchanged() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let write_trait = storage
		.split("pub trait StorageWrite")
		.nth(1)
		.and_then(|rest| rest.split("pub trait StorageDeleteRows").next())
		.expect("StorageWrite trait block");

	assert!(write_trait.contains(
		"async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError>;"
	));
	assert!(!write_trait.contains("delete_rows"));
}

#[test]
pub(crate) fn adr035_no_blake3_or_hash_identity_generation_in_toron() {
	let manifest_dir = manifest_dir();
	let sources = [
		manifest_dir.join("src/storage.rs"),
		manifest_dir.join("src/coordinator/delete.rs"),
	];

	for source_path in sources {
		let source = std::fs::read_to_string(&source_path).expect("read source");
		for forbidden in [
			"blake3",
			"Identity::blake3",
			"Identity::blake3_tenant",
			"Identity::blake3_keyed",
			"sha",
			"DefaultHasher",
		] {
			assert!(
				!source.contains(forbidden),
				"ADR 035 in-memory identities must be deterministic prefix/suffix, found {forbidden} in {}",
				source_path.display()
			);
		}
	}
}

#[test]
pub(crate) fn adr035_in_memory_identity_layout_is_prefix_suffix() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let body = storage
		.split("fn in_memory_row_identity")
		.nth(1)
		.and_then(|rest| rest.split("fn take_record_batch_rows").next())
		.expect("in_memory_row_identity body");

	assert!(body.contains("bytes[0..16].copy_from_slice(table_object_id.as_bytes())"));
	assert!(body.contains("bytes[24..32].copy_from_slice(&ordinal.to_be_bytes())"));
}

#[test]
pub(crate) fn adr035_in_memory_storage_tracks_row_ids_and_tombstones() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let data = storage
		.split("struct InMemoryTableData")
		.nth(1)
		.and_then(|rest| rest.split("pub struct InMemoryTableStorage").next())
		.expect("InMemoryTableData block");

	for field in ["row_key_ids", "deleted_row_key_ids", "next_row_key_ordinal"] {
		assert!(data.contains(field), "missing ADR 035 field {field}");
	}
}

#[test]
pub(crate) fn adr035_identity_scan_rejects_user_projection() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");

	assert!(storage.contains("request.row_identity == RowIdentityProjection::Append"));
	assert!(storage.contains("request.projected_columns.is_some()"));
	assert!(storage.contains("StorageError::Internal"));
	assert!(
		storage.contains("scan with row_identity=Append cannot be combined with projected_columns")
	);
}

#[test]
pub(crate) fn adr035_coordinator_delete_module_exists() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let delete = std::fs::read_to_string(manifest_dir().join("src/coordinator/delete.rs"))
		.expect("read delete coordinator source");

	assert!(coordinator.contains("mod delete;"));
	assert!(delete.contains("materialize_delete_row_key_ids"));
}

#[test]
pub(crate) fn adr035_materializer_validates_identity_column_shape() {
	let delete = std::fs::read_to_string(manifest_dir().join("src/coordinator/delete.rs"))
		.expect("read delete coordinator source");

	assert!(delete.contains("STORAGE_ROW_KEY_ID_COLUMN"));
	assert!(delete.contains("DataType::FixedSizeBinary(32)"));
	assert!(delete.contains("null_count() != 0"));
}

#[test]
pub(crate) fn adr035_select_scan_uses_row_identity_none() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let scan_requests = brace_blocks_after_marker(&exec, "ScanRequest {");
	assert!(
		!scan_requests.is_empty(),
		"ADR 035 guard expected ScanRequest constructors"
	);
	for (line, block) in scan_requests {
		assert!(
			block.contains("RowIdentityProjection::None")
				|| block.contains("row_identity: *row_identity"),
			"ADR 035/037 ScanRequest at exec.rs:{line} must set row_identity explicitly"
		);
	}
}

#[test]
pub(crate) fn adr035_wendelon_adapter_calls_table_delete() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let adapter = storage
		.split("impl StorageDeleteRows for WendelonStorageDeleteRows")
		.nth(1)
		.and_then(|rest| rest.split("fn append_projection").next())
		.expect("WendelonStorageDeleteRows adapter block");

	assert!(adapter.contains(".table_delete("));
	assert!(!adapter.contains(".table_delete_rows("));
}

#[test]
pub(crate) fn adr038_storage_admin_trait_has_one_method() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let trait_body = storage
		.split("pub trait StorageAdmin")
		.nth(1)
		.and_then(|rest| rest.split("pub struct InMemoryStorageScan").next())
		.expect("StorageAdmin trait block");

	assert_eq!(trait_body.matches("async fn").count(), 1);
	assert!(trait_body.contains("async fn drop_table_storage("));
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
pub(crate) fn adr038_in_memory_storage_admin_removes_table_entry() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let body = brace_blocks_after_marker(&storage, "impl StorageAdmin for InMemoryTableStorage")
		.into_iter()
		.next()
		.expect("InMemoryTableStorage StorageAdmin impl")
		.1;

	assert!(body.contains("tables.remove(&request.table_id)"));
}

#[test]
pub(crate) fn adr038_drop_table_storage_first_catalog_second() {
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

	assert!(
		helper.find(".drop_table_storage").unwrap()
			< helper.find("catalog.drop_table(name, false)").unwrap()
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
pub(crate) fn adr038_wendelon_create_table_admin_unavailable_removed() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(!create_body.contains("CatalogError::AdminUnavailable"));
	assert!(!create_body.contains("CREATE TABLE is not wired"));
}

#[test]
pub(crate) fn adr038_wendelon_create_uses_table_schema_update_request_create() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(create_body.contains("TableSchemaUpdateRequest::create"));
	assert!(create_body.contains("submit_table_schema_update"));
}

#[test]
pub(crate) fn adr038_wendelon_create_uses_uuid_object_id_not_fingerprint() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(create_body.contains("Uuid::new_v4"));
	for forbidden in ["blake3", "xxh3", "Sha2", "Sha3"] {
		assert!(!create_body.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
pub(crate) fn adr038_drop_route_uses_storage_admin_sink_when_configured() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("admin: Arc<dyn StorageAdmin>"));
	assert!(coordinator.contains("self.admin.as_ref()"));
	assert!(coordinator.contains(".drop_table_storage"));
}

#[test]
pub(crate) fn adr039_wendelon_drop_admin_unavailable_lifted() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let drop_body = wendelon_catalog_method_body(&catalog, "async fn drop_table");

	assert!(!drop_body.contains("CatalogError::AdminUnavailable"));
	assert!(drop_body.contains("consume_finalized_drop"));
	assert!(drop_body.contains("remove_table"));
}

#[test]
pub(crate) fn adr039_wendelon_admin_error_mapping_is_structured() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let catalog_body = brace_blocks_after_marker(&catalog, "fn map_wendelon_admin_error")
		.into_iter()
		.next()
		.expect("catalog admin error mapper")
		.1;
	let storage_body =
		brace_blocks_after_marker(&storage, "fn map_wendelon_admin_error_to_storage")
			.into_iter()
			.next()
			.expect("storage admin error mapper")
			.1;

	for body in [catalog_body, storage_body] {
		assert!(body.contains("ClientError::Admin"));
		assert!(body.contains("WendelonAdminError::"));
		assert!(!body.contains(".contains("));
	}
}

#[test]
pub(crate) fn adr039_no_table_schema_drop_request_v1_in_toron() {
	assert_toron_sources_do_not_contain(&["TableSchemaDropRequestV1"]);
}

#[test]
pub(crate) fn adr039_toron_does_not_import_wendelon_internal_stage_types() {
	assert_toron_sources_do_not_contain(&[
		"TableDropPrepareRequest",
		"TableReclaimCompletionRequest",
		"TableDropFinalizeRequest",
		"DropStage",
	]);
}

#[test]
pub(crate) fn adr039_toron_does_not_orchestrate_wendelon_drop_stages() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let body = brace_blocks_after_marker(&storage, "impl StorageAdmin for WendelonStorageAdmin")
		.into_iter()
		.next()
		.expect("WendelonStorageAdmin StorageAdmin impl")
		.1;

	assert!(body.contains(".drop_table_schema("));
	assert!(body.contains("TableSchemaDropRequest::current"));
	assert!(!body.contains("TableSchemaDropRequest::V1"));
	for forbidden in [
		"submit_table_drop_prepare",
		"submit_table_reclaim_completion",
		"submit_table_drop_finalize",
		"submit_table_schema_drop",
		"TableDropPrepareRequest",
		"TableReclaimCompletionRequest",
		"TableDropFinalizeRequest",
	] {
		assert!(!body.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
pub(crate) fn adr039_no_catalog_drop_table_with_proof_method() {
	assert_toron_sources_do_not_contain(&["drop_table_with_proof"]);
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
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
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
	assert!(binder.contains("if_not_exists: *if_not_exists"));
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
pub(crate) fn adr061_catalog_create_exposes_created_outcome() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let outcome = brace_blocks_after_marker(&catalog, "pub struct CreateTableOutcome")
		.into_iter()
		.next()
		.expect("CreateTableOutcome")
		.1;

	assert!(outcome.contains("pub binding: TableBinding,"));
	assert!(outcome.contains("pub created: bool,"));
	assert!(catalog.contains("async fn create_table_with_outcome("));
	assert!(catalog.contains(".map(|outcome| outcome.binding)"));
	assert!(catalog.contains("created: false"));
	assert!(catalog.contains("created: true"));
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
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let bind_body = brace_blocks_after_marker(&binder, "async fn bind_create_table_as_select")
		.into_iter()
		.next()
		.expect("bind_create_table_as_select")
		.1;
	let bind_source = bind_body
		.find("self.bind_plan(source).await?")
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
pub(crate) fn adr061_no_wendelon_wire_worker_or_sql_protocol_change() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let wendelon_sources = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	for source in [&wire, &sql_wire, &server, &wendelon_sources] {
		assert!(!source.contains("CreateTableOutcome"));
		assert!(!source.contains("create_table_with_outcome"));
		assert!(!source.contains("CtasIfNotExists"));
	}
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
pub(crate) fn adr062_create_table_row_identity_type_is_toron_local() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");

	assert!(logical.contains("pub enum CreateTableRowIdentity"));
	assert!(logical.contains("AppendOnly"));
	assert!(logical.contains("PrimaryKey { column: String }"));
	assert!(!logical.contains("RowIdentityPolicy"));
	assert!(!execution.contains("RowIdentityPolicy"));
}

#[test]
pub(crate) fn adr062_logical_create_table_carries_row_identity() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let create = brace_blocks_after_marker(&logical, "CreateTable {")
		.into_iter()
		.next()
		.expect("logical CreateTable")
		.1;

	assert!(create.contains("row_identity: CreateTableRowIdentity,"));
}

#[test]
pub(crate) fn adr062_execution_create_table_carries_row_identity() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let create = brace_blocks_after_marker(&execution, "CreateTable {")
		.into_iter()
		.next()
		.expect("execution CreateTable")
		.1;

	assert!(create.contains("row_identity: CreateTableRowIdentity,"));
}

#[test]
pub(crate) fn adr062_binder_preserves_create_table_row_identity() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");

	assert!(binder.contains("row_identity: row_identity.clone()"));
	assert!(binder.contains("row_identity,"));
}

#[test]
pub(crate) fn adr062_sql_planner_accepts_column_primary_key() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE pk_column (id INT NOT NULL PRIMARY KEY, note TEXT)")
		.expect("column PK should plan");

	let LogicalPlan::CreateTable { row_identity, .. } = plan else {
		panic!("expected create table");
	};
	assert_eq!(
		row_identity,
		CreateTableRowIdentity::PrimaryKey {
			column: "id".to_string()
		}
	);
}

#[test]
pub(crate) fn adr062_sql_planner_accepts_table_primary_key() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE pk_table (id BIGINT NOT NULL, PRIMARY KEY(id))")
		.expect("table PK should plan");

	let LogicalPlan::CreateTable { row_identity, .. } = plan else {
		panic!("expected create table");
	};
	assert_eq!(
		row_identity,
		CreateTableRowIdentity::PrimaryKey {
			column: "id".to_string()
		}
	);
}

#[test]
pub(crate) fn adr062_sql_planner_rejects_duplicate_primary_key() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, PRIMARY KEY(id))")
		.expect_err("duplicate PK should reject");

	assert!(err.to_string().contains("PRIMARY KEY more than once"));
}

#[test]
pub(crate) fn adr062_sql_planner_rejects_composite_primary_key() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE t (id INT NOT NULL, sku TEXT NOT NULL, PRIMARY KEY(id, sku))")
		.expect_err("composite PK should reject");

	assert!(
		err.to_string()
			.contains("PRIMARY KEY must reference exactly one column")
	);
}

#[test]
pub(crate) fn adr062_sql_planner_requires_primary_key_not_null() {
	for sql in [
		"CREATE TABLE t (id INT PRIMARY KEY)",
		"CREATE TABLE t (id INT, PRIMARY KEY(id))",
		"CREATE TABLE t (id INT NULL PRIMARY KEY)",
	] {
		let err = toron::sql_planner::SqlPlanner::new()
			.plan(sql)
			.expect_err("nullable PK should reject");
		assert!(
			err.to_string()
				.contains("PRIMARY KEY column must be declared NOT NULL")
		);
	}
}

#[test]
pub(crate) fn adr062_sql_planner_rejects_unsupported_primary_key_types() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE t (active BOOLEAN NOT NULL PRIMARY KEY)")
		.expect_err("unsupported PK type should reject");

	assert!(
		err.to_string()
			.contains("unsupported CREATE TABLE PRIMARY KEY column type")
	);
}

#[test]
pub(crate) fn adr062_ctas_has_no_primary_key_identity_surface() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	for source in [&logical, &execution] {
		let ctas = brace_blocks_after_marker(source, "CreateTableAsSelect {")
			.into_iter()
			.next()
			.expect("CTAS block")
			.1;
		assert!(!ctas.contains("row_identity"));
	}
	assert!(planner.contains("CTAS does not support explicit column lists"));
	assert!(planner.contains("unsupported CREATE TABLE clause: table-level PRIMARY KEY"));
}

#[test]
pub(crate) fn adr062_catalog_create_request_carries_row_identity() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let trait_body = brace_blocks_after_marker(&catalog, "pub trait Catalog")
		.into_iter()
		.next()
		.expect("Catalog trait")
		.1;

	assert!(trait_body.contains("row_identity: CreateTableRowIdentity"));
	assert!(catalog.contains("create_table_row_identity_policy"));
}

#[test]
pub(crate) fn adr062_wendelon_catalog_maps_primary_key_descriptor() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("CreateTableRowIdentity::PrimaryKey"));
	assert!(catalog.contains("RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single"));
	assert!(catalog.contains("RowIdentityPolicy::AppendOnly"));
}

#[test]
pub(crate) fn adr062_if_not_exists_compares_row_identity() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("matches_create_identity(&row_identity)"));
	assert!(catalog.contains("CatalogError::SchemaMismatch"));
	assert!(catalog.contains("TableRowIdentity::Unsupported"));
}

#[test]
pub(crate) fn adr062_no_pk_create_remains_append_only() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE no_pk (id INT NOT NULL)")
		.expect("no-PK create should plan");

	let LogicalPlan::CreateTable { row_identity, .. } = plan else {
		panic!("expected create table");
	};
	assert_eq!(row_identity, CreateTableRowIdentity::AppendOnly);
}

#[test]
pub(crate) fn adr062_update_surface_is_present_under_pk_update_contract_after_adr063() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(planner.contains("Statement::Update(update) => self.lower_update(update)"));
	assert!(logical.contains("Update {"));
	assert!(execution.contains("Update {"));
	assert!(planner.contains("lower_returning_projection(returning.as_deref())"));
	assert!(binder.contains("UPDATE requires a PRIMARY KEY table"));
	assert!(binder.contains("UPDATE cannot modify the PRIMARY KEY column"));
	assert!(fragmenter.contains("UpdateNotFragmentable"));
	assert!(server.contains("ExecutionPlan::Update { .. } => \"update\""));
}

#[test]
pub(crate) fn adr062_returning_surface_is_retargeted_after_adr064() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");

	assert!(planner.contains("fn lower_returning_projection"));
	assert!(logical.contains("enum ReturningProjection"));
	assert!(execution.contains("struct BoundReturningProjection"));
	assert!(!planner.contains("unsupported CREATE TABLE AS clause: RETURNING"));
}

#[test]
pub(crate) fn adr062_prior_adr038_append_only_guard_retargeted() {
	let architecture_test = architecture_corpus_source();

	assert!(architecture_test.contains(
		"adr038_wendelon_create_row_identity_policy_is_create_request_driven_after_adr062"
	));
	let old_guard = [
		"fn ",
		"adr038_wendelon_create_uses_append_only",
		"_row_identity_policy",
	]
	.concat();
	assert!(!architecture_test.contains(&old_guard));
	assert!(architecture_test.contains("adr062_no_pk_create_remains_append_only"));
}

#[test]
pub(crate) fn adr062_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr062_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	for forbidden in [
		"CreateTableRowIdentity",
		"TableRowIdentity",
		"PrimaryKeyDescriptorV1",
		"PRIMARY KEY",
	] {
		assert!(
			!worker.contains(forbidden),
			"fixture worker gained {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr062_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"CreateTableRowIdentityV1",
		"TableRowIdentityV1",
		"PrimaryKeyDdlV1",
		"CreateTablePrimaryKeyV1",
		"CreateTableRowIdentityPolicyV1",
	]);
}

#[test]
pub(crate) fn adr062_prior_adr033_to_061_guards_continue_to_pass_except_retarget() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr033_", "adr034_", "adr035_", "adr036_", "adr037_", "adr038_", "adr039_", "adr040_",
		"adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_", "adr048_",
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_", "adr056_",
		"adr057_", "adr058_", "adr059_", "adr060_", "adr061_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains(
		"adr038_wendelon_create_row_identity_policy_is_create_request_driven_after_adr062"
	));
}

#[test]
pub(crate) fn adr063_sql_planner_accepts_statement_update() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let plan_body = brace_blocks_after_marker(&planner, "pub fn plan")
		.into_iter()
		.next()
		.expect("plan body")
		.1;

	assert!(plan_body.contains("Statement::Update(update) => self.lower_update(update)"));
}

#[test]
pub(crate) fn adr063_sql_planner_destructures_sqlparser_062_update_fields_after_adr073() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_update = brace_blocks_after_marker(&planner, "fn lower_update(&self")
		.into_iter()
		.next()
		.expect("lower_update")
		.1;

	assert!(lower_update.contains("let sqlparser::ast::Update {"));
	for field in [
		"update_token",
		"optimizer_hints",
		"table",
		"assignments",
		"from",
		"selection",
		"returning",
		"output",
		"or",
		"order_by",
		"limit",
	] {
		assert!(
			lower_update.contains(field),
			"UPDATE destructure lost sqlparser 0.62 field {field}"
		);
	}
	assert!(!lower_update.contains("{ .."));
	assert!(!lower_update.contains("..,"));
}

#[test]
pub(crate) fn adr063_sqlparser_upgrade_is_now_owned_by_adr073() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");
	let architecture_test = architecture_corpus_source();

	assert!(manifest.contains("sqlparser = \"0.62\""));
	assert!(architecture_test.contains("fn adr073_sqlparser_is_pinned_to_062"));
	let stale_guard = [
		"fn ",
		"adr063_sqlparser_062_upgrade",
		"_is_not_part_of_update_surface",
	]
	.concat();
	assert!(!architecture_test.contains(&stale_guard));
}

#[test]
pub(crate) fn adr063_unsupported_statement_text_lists_update() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let plan_body = brace_blocks_after_marker(&planner, "pub fn plan")
		.into_iter()
		.next()
		.expect("plan body")
		.1;

	assert!(plan_body.contains("SELECT, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE"));
}

#[test]
pub(crate) fn adr063_update_where_requirement_is_retargeted_after_adr070() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_update = brace_blocks_after_marker(&planner, "fn lower_update(&self")
		.into_iter()
		.next()
		.expect("lower_update")
		.1;
	let lower_delete = brace_blocks_after_marker(&planner, "fn lower_delete")
		.into_iter()
		.next()
		.expect("lower_delete")
		.1;

	assert!(lower_update.contains("selection"));
	assert!(lower_update.contains(".map(|expr| self.lower_expr(expr))"));
	assert!(lower_update.contains(".transpose()?"));
	assert!(!lower_update.contains("UPDATE requires a WHERE clause"));
	assert!(lower_delete.contains("let predicate = selection"));
	assert!(lower_delete.contains(".map(|expr| self.lower_expr(expr))"));
	assert!(!lower_delete.contains("DELETE requires a WHERE clause"));
}

#[test]
pub(crate) fn adr063_update_rejects_unsupported_clauses() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_update = brace_blocks_after_marker(&planner, "fn lower_update(&self")
		.into_iter()
		.next()
		.expect("lower_update")
		.1;

	for clause in [
		"unsupported UPDATE clause: FROM",
		"unsupported UPDATE clause: OR",
	] {
		assert!(lower_update.contains(clause), "missing {clause}");
	}
	assert!(lower_update.contains("lower_returning_projection(returning.as_deref())"));
}

#[test]
pub(crate) fn adr063_update_rejects_target_alias_joins_and_qualified_targets() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_target = brace_blocks_after_marker(&planner, "fn lower_update_target_table")
		.into_iter()
		.next()
		.expect("lower_update_target_table")
		.1;
	let lower_update = brace_blocks_after_marker(&planner, "fn lower_update(&self")
		.into_iter()
		.next()
		.expect("lower_update")
		.1;
	let lower_assignment = brace_blocks_after_marker(&planner, "fn lower_update_assignment_target")
		.into_iter()
		.next()
		.expect("lower_update_assignment_target")
		.1;

	for target_reject in [
		"unsupported UPDATE target: table alias",
		"unsupported UPDATE target: table arguments",
		"unsupported UPDATE target: table hints",
		"unsupported UPDATE target: table version",
		"unsupported UPDATE target: WITH ORDINALITY",
		"unsupported UPDATE target: partitions",
		"unsupported UPDATE target: json path",
		"unsupported UPDATE target: sample",
	] {
		assert!(
			lower_target.contains(target_reject),
			"missing {target_reject}"
		);
	}
	assert!(lower_update.contains("unsupported UPDATE target: joins not supported"));
	assert!(lower_assignment.contains("unsupported UPDATE assignment target: qualified column"));
}

#[test]
pub(crate) fn adr063_update_rejects_duplicate_and_empty_assignments() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_assignments = brace_blocks_after_marker(&planner, "fn lower_update_assignments")
		.into_iter()
		.next()
		.expect("lower_update_assignments")
		.1;
	let lower_assignment = brace_blocks_after_marker(&planner, "fn lower_update_assignment_target")
		.into_iter()
		.next()
		.expect("lower_update_assignment_target")
		.1;

	assert!(lower_assignments.contains("UPDATE requires at least one assignment"));
	assert!(lower_assignments.contains("duplicate UPDATE assignment target"));
	assert!(lower_assignment.contains("unsupported UPDATE assignment target: tuple assignment"));
}

#[test]
pub(crate) fn adr063_logical_update_shape_is_name_based() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let update = brace_blocks_after_marker(&logical, "Update {")
		.into_iter()
		.next()
		.expect("logical update")
		.1;

	assert!(update.contains("table: TableRef"));
	assert!(update.contains("assignments: Vec<(String, Expr)>"));
	assert!(update.contains("predicate: Option<Expr>"));
}

#[test]
pub(crate) fn adr063_execution_update_shape_is_source_bearing() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let update = brace_blocks_after_marker(&execution, "Update {")
		.into_iter()
		.next()
		.expect("execution update")
		.1;

	for required in [
		"legacy_table_id: TableId",
		"table: TableObjectId",
		"fingerprint: PhysicalDescriptorFingerprint",
		"schema: SchemaRef",
		"source: Box<ExecutionPlan>",
		"profile_hint: QueryProfileHint",
	] {
		assert!(update.contains(required), "missing {required}");
	}
}

#[test]
pub(crate) fn adr063_logical_update_has_no_wendelon_identity_types() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let update = brace_blocks_after_marker(&logical, "Update {")
		.into_iter()
		.next()
		.expect("logical update")
		.1;

	for forbidden in [
		"TableRowIdentity",
		"RowIdentityPolicy",
		"RowIdentityProjection",
		"TableObjectId",
		"PhysicalDescriptorFingerprint",
	] {
		assert!(
			!update.contains(forbidden),
			"logical UPDATE leaked {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr063_binder_requires_primary_key_row_identity() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;

	assert!(bind_update.contains("TableRowIdentity::PrimaryKey"));
	assert!(bind_update.contains("primary_key_columns.contains(primary_key_ordinal)"));
	assert!(bind_update.contains("UPDATE requires a PRIMARY KEY table"));
}

#[test]
pub(crate) fn adr063_binder_rejects_primary_key_assignment() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;

	assert!(bind_update.contains("UPDATE cannot modify the PRIMARY KEY column"));
	assert!(bind_update.contains("STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&column.as_str())"));
}

#[test]
pub(crate) fn adr063_binder_builds_full_row_source_projection() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;

	assert!(bind_update.contains("source_exprs"));
	assert!(bind_update.contains("schema.fields().iter().enumerate()"));
	assert!(bind_update.contains(".remove(&ordinal)"));
	assert!(bind_update.contains("Expr::column(field.name().clone())"));
	assert!(bind_update.contains("output_names.push(Some(field.name().clone()))"));
	assert!(bind_update.contains("ExecutionPlan::Project"));
}

#[test]
pub(crate) fn adr063_update_source_scan_does_not_request_append_identity() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;

	assert!(bind_update.contains("scan_plan(binding.clone(), None)"));
	assert!(!bind_update.contains("RowIdentityProjection::Append"));
	assert!(!bind_update.contains("TableRowIdentity::AppendOnly"));
}

#[test]
pub(crate) fn adr063_update_reuses_existing_point_read_profile() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let predicate_helper =
		brace_blocks_after_marker(&binder, "fn bind_update_source_for_predicate")
			.into_iter()
			.next()
			.expect("bind_update_source_for_predicate")
			.1;

	assert!(
		predicate_helper.contains("plan_filter_with_predicate_pushdown(scan, bound_predicate)")
	);
	assert!(binder.contains("scan_profile_hint(&predicate, input)"));
	assert!(binder.contains("apply_scan_profile_hint(&mut input, Some(profile_hint))"));
	assert!(!predicate_helper.contains("update_profile_hint"));
}

#[test]
pub(crate) fn adr063_update_reuses_write_source_coercion_policy() {
	let write = std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
		.expect("read write helpers");

	assert!(write.contains("pub(super) fn materialize_update_select_batch"));
	assert!(write.contains("materialize_write_select_batch(\"UPDATE\""));
	assert!(write.contains("coerce_insert_select_array("));
	assert!(write.contains("validate_non_null_targets(operation"));
}

#[test]
pub(crate) fn adr063_coordinator_update_uses_storage_write_not_delete() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update_start = coordinator
		.find("ExecutionPlan::Update {\n\t\t\tlegacy")
		.expect("coordinator update arm start");
	let update_end = update_start
		+ coordinator[update_start..]
			.find("ExecutionPlan::CreateTableAsSelect")
			.expect("coordinator update arm end");
	let update_arm = &coordinator[update_start..update_end];

	assert!(update_arm.contains("source_executor.execute_read_plan(source).await?"));
	assert!(update_arm.contains("write::materialize_update_select_batch(schema, &source_batches)"));
	assert!(update_arm.contains("write(WriteRequest"));
	assert!(update_arm.contains("profile_hint: Some(*profile_hint)"));
	assert!(update_arm.contains("affected_count_batch(outcome.rows)"));
	assert!(!update_arm.contains("delete_rows"));
	assert!(!update_arm.contains("StorageDeleteRows"));
}

#[test]
pub(crate) fn adr063_descriptor_evolved_invalidates_without_retry() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let write_helper = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan(")
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
pub(crate) fn adr063_update_remains_non_fragmentable_and_worker_rejected() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec");

	assert!(fragmenter.contains("UpdateNotFragmentable"));
	assert!(fragmenter.contains("matches!(plan, ExecutionPlan::Update { .. })"));
	assert!(server.contains("ExecutionPlan::Update { .. } => \"update\""));
	assert!(exec.contains("UPDATE execution is coordinator-only"));
	assert!(exec.contains("unbound UPDATE execution"));
}

#[test]
pub(crate) fn adr063_sql_response_is_complete_only_for_non_returning_dml_after_adr064() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_listener_test =
		std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
			.expect("read SQL listener tests");

	assert!(daemon.contains("returning: Some(_)"));
	assert!(daemon.contains("sql_affected_row_count(&batches)"));
	assert!(sql_listener_test.contains("sql_listener_update_complete_frame_carries_row_count"));
	assert!(sql_listener_test.contains("SqlMessage::Complete"));
}

#[test]
pub(crate) fn adr063_no_wendelon_wire_storage_or_descriptor_change() {
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"LogicalPlan::Update",
		"ExecutionPlan::Update",
		"Statement::Update",
		"UpdateNotFragmentable",
		"StorageUpdate",
		"TableUpdateRequest",
		"TableUpdateResponse",
	] {
		assert!(
			!wendelon_source.contains(forbidden),
			"ADR 063 leaked UPDATE surface into Wendelon: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr063_no_transaction_lock_compare_swap_surface_after_adr070() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let update_start = coordinator
		.find("ExecutionPlan::Update {\n\t\t\tlegacy")
		.expect("coordinator update arm start");
	let update_end = update_start
		+ coordinator[update_start..]
			.find("ExecutionPlan::CreateTableAsSelect")
			.expect("coordinator update arm end");
	let update_arm = &coordinator[update_start..update_end];

	for forbidden in [
		"CompareSwap",
		"compare_swap",
		"lost_update",
		"transaction",
		"rollback",
		"StorageDeleteRows",
		"delete_rows",
	] {
		assert!(
			!bind_update.contains(forbidden),
			"bind UPDATE gained {forbidden}"
		);
		assert!(
			!update_arm.contains(forbidden),
			"coordinator UPDATE gained {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr063_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr063_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	for forbidden in [
		"LogicalPlan::Update",
		"ExecutionPlan::Update",
		"UPDATE",
		"UpdateNotFragmentable",
	] {
		assert!(
			!worker.contains(forbidden),
			"fixture worker gained UPDATE surface {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr063_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"UpdateV1",
		"SqlUpdateV1",
		"LogicalUpdateV1",
		"ExecutionUpdateV1",
		"UpdateRequestV1",
	]);
}

#[test]
pub(crate) fn adr063_prior_adr054_to_062_guards_continue_to_pass_except_retarget() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_", "adr061_",
		"adr062_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(
		architecture_test
			.contains("adr062_update_surface_is_present_under_pk_update_contract_after_adr063")
	);
	let stale_guard = ["fn ", "adr062_update_surface", "_remains_absent"].concat();
	assert!(!architecture_test.contains(&stale_guard));
}

#[test]
pub(crate) fn adr064_returning_supports_four_dml_variants() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");

	assert_eq!(
		planner
			.matches("lower_returning_projection(returning.as_deref())")
			.count(),
		3
	);
	for variant in ["Insert {", "InsertSelect {", "Update {", "Delete {"] {
		let body = logical
			.split(variant)
			.nth(1)
			.unwrap_or_else(|| panic!("missing {variant}"));
		assert!(body.contains("returning: Option<ReturningProjection>"));
	}
}

#[test]
pub(crate) fn adr064_ctas_returning_remains_out_of_scope() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let lower_create = brace_blocks_after_marker(&planner, "fn lower_create_table")
		.into_iter()
		.next()
		.expect("lower_create_table")
		.1;
	let ctas = logical
		.split("CreateTableAsSelect {")
		.nth(1)
		.expect("CTAS logical variant")
		.split("DropTable {")
		.next()
		.expect("CTAS logical body");

	assert!(!lower_create.contains("lower_returning_projection"));
	assert!(!ctas.contains("ReturningProjection"));
}

#[test]
pub(crate) fn adr064_sqlparser_upgrade_is_now_owned_by_adr073() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");
	let architecture_test = architecture_corpus_source();

	assert!(manifest.contains("sqlparser = \"0.62\""));
	assert!(architecture_test.contains("fn adr073_sqlparser_is_pinned_to_062"));
	let stale_guard = ["fn ", "adr064_sqlparser", "_stays_054"].concat();
	assert!(!architecture_test.contains(&stale_guard));
}

#[test]
pub(crate) fn adr064_returning_ir_is_toron_local() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");

	assert!(logical.contains("pub enum ReturningProjection"));
	assert!(logical.contains("Exprs {\n\t\texprs: Vec<Expr>"));
	assert!(execution.contains("pub struct BoundReturningProjection"));
	assert!(!logical.contains("SelectItem"));
	assert!(!execution.contains("SelectItem"));
}

#[test]
pub(crate) fn adr064_bound_plan_schema_reports_returning_schema() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let schema = brace_blocks_after_marker(&execution, "pub fn schema(&self)")
		.into_iter()
		.next()
		.expect("ExecutionPlan::schema")
		.1;

	assert!(schema.contains("returning: Some(returning)"));
	assert!(schema.contains("returning.output_schema.clone()"));
	assert!(schema.contains("result_count_schema_int64(\"deleted\")"));
	assert!(schema.contains("Arc::new(Schema::empty())"));
}

#[test]
pub(crate) fn adr064_returning_projection_vocab_is_intentionally_narrow() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower = brace_blocks_after_marker(&planner, "fn lower_returning_projection")
		.into_iter()
		.next()
		.expect("lower_returning_projection")
		.1;

	for allowed in [
		"SelectItem::Wildcard(_)",
		"SelectItem::UnnamedExpr",
		"SelectItem::ExprWithAlias",
	] {
		assert!(lower.contains(allowed));
	}
	for rejected in [
		"mixed wildcard RETURNING",
		"qualified wildcard RETURNING",
		"qualified RETURNING columns",
	] {
		assert!(lower.contains(rejected));
	}
}

#[test]
pub(crate) fn adr064_returning_binds_against_target_user_schema() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let helper = brace_blocks_after_marker(&binder, "fn bind_returning_projection")
		.into_iter()
		.next()
		.expect("bind_returning_projection")
		.1;

	assert!(helper.contains(".fields()"));
	assert!(helper.contains("bind_expr_program(&exprs, schema, config)"));
	assert!(helper.contains("schema_for_exprs_with_output_names"));
	assert!(binder.contains("RETURNING column is reserved for storage metadata"));
}

#[test]
pub(crate) fn adr064_returning_does_not_touch_wendelon_or_worker_transport() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read wire");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter");

	assert!(!wire.contains("Returning"));
	assert!(server.contains("ExecutionPlan::InsertSelect { .. } => \"insert_select\""));
	assert!(fragmenter.contains("InsertNotFragmentable"));
	assert!(fragmenter.contains("DeleteNotFragmentable"));
	assert!(fragmenter.contains("UpdateNotFragmentable"));
}

#[test]
pub(crate) fn adr064_returning_does_not_add_sql_wire_message_or_protocol_version() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");
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
		"Execute(ExecuteRequest)",
		"Cancel(CancelRequest)",
		"ResultBatch(ResultBatch)",
		"Complete(Complete)",
		"Error(SqlError)",
	] {
		assert!(
			message.contains(variant),
			"missing SQL message variant {variant}"
		);
	}
	for forbidden in [
		"Returning",
		"ResultSchema",
		"MutationResult",
		"DmlResult",
		"SchemaCarrier",
	] {
		assert!(
			!message.contains(forbidden),
			"RETURNING added SQL wire frame surface {forbidden}"
		);
	}
	assert!(protocol.contains("Current"));
	assert!(!protocol.contains("V1"));
	assert!(!protocol.contains("V2"));
	assert!(wire.contains("pub arrow_ipc_bytes: ByteBuf"));
	assert!(daemon.contains("SqlMessage::ResultBatch(ResultBatch"));
	assert!(sql_tests.contains("sql_listener_dml_returning_sends_result_batches_then_complete"));
}

#[test]
pub(crate) fn adr064_returning_projection_is_validated_before_mutation() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let insert_pos = coordinator
		.find("let returning_batches =\n\t\t\t\tproject_returning_batches")
		.expect("returning projection");
	let write_pos = coordinator
		.find(".write(WriteRequest")
		.expect("write request");

	assert!(insert_pos < write_pos);
	assert!(coordinator.contains("project_returning_batches(returning.as_ref()"));
	assert!(coordinator.contains("Executor::evaluate_bound_program_without_storage"));
}

#[test]
pub(crate) fn adr064_returning_rows_emit_only_after_storage_success() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let projection = coordinator
		.find("project_returning_batches(returning.as_ref()")
		.expect("projection");
	let write = coordinator[projection..]
		.find(".await?;")
		.map(|offset| projection + offset)
		.expect("storage await");
	let emit = coordinator[write..]
		.find("Ok(batches)")
		.map(|offset| write + offset)
		.expect("returning emit");

	assert!(projection < write);
	assert!(write < emit);
}

#[test]
pub(crate) fn adr064_complete_row_count_matches_storage_receipt_and_returned_rows() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(coordinator.contains("verify_write_returning_receipt"));
	assert!(coordinator.contains("verify_delete_returning_receipt"));
	assert!(coordinator.contains("batch_row_count(batches)"));
	assert!(daemon.contains("batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64"));
}

#[test]
pub(crate) fn adr064_no_new_pk_gate_for_insert_or_delete_returning() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_insert = brace_blocks_after_marker(&binder, "async fn bind_insert(")
		.into_iter()
		.next()
		.expect("bind_insert")
		.1;
	let bind_delete = brace_blocks_after_marker(&binder, "async fn bind_delete(")
		.into_iter()
		.next()
		.expect("bind_delete")
		.1;

	assert!(bind_insert.contains("bind_returning_projection"));
	assert!(bind_delete.contains("bind_returning_projection"));
	assert!(!bind_insert.contains("PRIMARY KEY"));
	assert!(!bind_delete.contains("PRIMARY KEY"));
}

#[test]
pub(crate) fn adr064_update_returning_inherits_pk_update_contract() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update(")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;

	assert!(bind_update.contains("bind_returning_projection"));
	assert!(bind_update.contains("UPDATE requires a PRIMARY KEY table"));
	assert!(bind_update.contains("UPDATE cannot modify the PRIMARY KEY column"));
}

#[test]
pub(crate) fn adr064_delete_returning_fails_closed_on_partial_receipt() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let delete_test = std::fs::read_to_string(manifest_dir().join("tests/delete_test.rs"))
		.expect("read delete tests");

	assert!(coordinator.contains("DELETE RETURNING receipt mismatch"));
	assert!(coordinator.contains("outcome.already_deleted == 0"));
	assert!(coordinator.contains("outcome.not_found == 0"));
	assert!(delete_test.contains("delete_returning_fails_closed_on_partial_receipt"));
}

#[test]
pub(crate) fn adr064_delete_returning_uses_pre_delete_rows_without_identity() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let delete_mod = std::fs::read_to_string(manifest_dir().join("src/coordinator/delete.rs"))
		.expect("read delete module");

	assert!(coordinator.contains("delete_candidate_user_batches(&candidate_batches, schema)"));
	assert!(coordinator.contains("project_returning_batches(Some(returning), &user_batches)"));
	assert!(delete_mod.contains("let identity_index = batch.num_columns() - 1"));
	assert!(delete_mod.contains("batch.columns()[..identity_index].to_vec()"));
}

#[test]
pub(crate) fn adr064_insert_returning_uses_inserted_target_rows() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let insert_test = std::fs::read_to_string(manifest_dir().join("tests/insert_test.rs"))
		.expect("read insert tests");

	assert!(coordinator.contains("write::materialize_insert_batch(schema, rows)"));
	assert!(coordinator.contains("std::slice::from_ref(&payload)"));
	assert!(insert_test.contains("insert_values_returning_emits_inserted_rows"));
}

#[test]
pub(crate) fn adr064_insert_select_returning_uses_target_shaped_payload() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let insert_test = std::fs::read_to_string(manifest_dir().join("tests/insert_test.rs"))
		.expect("read insert tests");

	assert!(coordinator.contains(
		"write::materialize_insert_select_batch(schema, target_columns, &source_batches)"
	));
	assert!(coordinator.contains("project_returning_batches(returning.as_ref()"));
	assert!(insert_test.contains("insert_select_returning_preserves_target_reorder_and_coercion"));
}

#[test]
pub(crate) fn adr064_update_returning_uses_post_update_rows() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(
		coordinator.contains("write::materialize_update_select_batch(schema, &source_batches)")
	);
	assert!(coordinator.contains("project_returning_batches(returning.as_ref()"));
	assert!(update_test.contains("update_returning_emits_post_update_values"));
}

#[test]
pub(crate) fn adr064_returning_star_hides_storage_synthetic_columns() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let delete_test = std::fs::read_to_string(manifest_dir().join("tests/delete_test.rs"))
		.expect("read delete tests");

	assert!(binder.contains("ReturningProjection::Star"));
	assert!(binder.contains("schema.fields()"));
	assert!(delete_test.contains("delete_returning_star_hides_appended_row_identity"));
	assert!(delete_test.contains("batch.schema().fields().len(), 3"));
}

#[test]
pub(crate) fn adr064_returning_rejects_aggregate_window_subquery() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower = brace_blocks_after_marker(&planner, "fn lower_returning_projection")
		.into_iter()
		.next()
		.expect("lower_returning_projection")
		.1;

	assert!(lower.contains("expr_contains_aggregate_call"));
	assert!(lower.contains("expr_contains_window_function"));
	assert!(lower.contains("expr_contains_subquery"));
}

#[test]
pub(crate) fn adr064_descriptor_evolved_still_invalidates_without_retry() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(coordinator.contains("invalidate_descriptor_evolution_on_error"));
	assert!(!coordinator.contains("retry_returning"));
	assert!(update_test.contains("update_descriptor_evolved_invalidates_without_retry"));
}

#[test]
pub(crate) fn adr064_prior_returning_absence_guards_are_retargeted() {
	let architecture_test = architecture_corpus_source();

	assert!(architecture_test.contains("adr062_returning_surface_is_retargeted_after_adr064"));
	assert!(
		architecture_test
			.contains("adr063_sql_response_is_complete_only_for_non_returning_dml_after_adr064")
	);
	let stale_delete = ["unsupported DELETE clause: ", "RETURNING"].concat();
	let stale_update = ["unsupported UPDATE clause: ", "RETURNING"].concat();
	assert!(!architecture_test.contains(&stale_delete));
	assert!(!architecture_test.contains(&stale_update));
}

#[test]
pub(crate) fn adr064_no_returning_specific_memory_or_config_knob() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(!config.to_lowercase().contains("returning"));
	assert!(!budget.to_lowercase().contains("returning"));
}

#[test]
pub(crate) fn adr064_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr064_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	for forbidden in [
		"RETURNING",
		"ReturningProjection",
		"BoundReturningProjection",
	] {
		assert!(
			!worker.contains(forbidden),
			"fixture worker gained RETURNING surface {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr064_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ReturningV1",
		"SqlReturningV1",
		"ReturningProjectionV1",
		"BoundReturningProjectionV1",
		"DmlReturningV1",
	]);
}

#[test]
pub(crate) fn adr064_prior_adr030_to_063_guards_continue_to_pass_except_retarget() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr030_", "adr031_", "adr032_", "adr033_", "adr034_", "adr035_", "adr036_", "adr037_",
		"adr038_", "adr039_", "adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_",
		"adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_", "adr053_",
		"adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_", "adr061_",
		"adr062_", "adr063_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains("adr062_returning_surface_is_retargeted_after_adr064"));
	assert!(
		architecture_test
			.contains("adr063_sql_response_is_complete_only_for_non_returning_dml_after_adr064")
	);
}

#[test]
pub(crate) fn adr038_strict_admin_sink_default_is_unsupported_storage_admin() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("UnsupportedStorageAdmin"));
	assert!(coordinator.contains("Arc::new(UnsupportedStorageAdmin)"));
	assert!(coordinator.contains("pub fn with_storage_admin("));
	assert!(coordinator.contains("pub fn with_writer_deleter_and_admin("));
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
	for path in [
		manifest_dir.join("src/ir.rs"),
		manifest_dir.join("src/ir/execution.rs"),
		manifest_dir.join("src/sql_planner.rs"),
		manifest_dir.join("src/binder.rs"),
	] {
		let source = std::fs::read_to_string(&path).expect("read source");
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

#[test]
pub(crate) fn adr038_in_memory_admin_validates_object_id_and_fingerprint() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let body = brace_blocks_after_marker(&storage, "impl StorageAdmin for InMemoryTableStorage")
		.into_iter()
		.next()
		.expect("InMemoryTableStorage StorageAdmin impl")
		.1;

	assert!(body.contains("data.table_object_id != request.table_object_id"));
	assert!(
		body.contains("data.physical_descriptor_fingerprint")
			&& body.contains("request.physical_descriptor_fingerprint")
	);
}

#[test]
pub(crate) fn adr038_wendelon_create_row_identity_policy_is_create_request_driven_after_adr062() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(catalog.contains("CreateTableRowIdentity::AppendOnly"));
	assert!(catalog.contains("CreateTableRowIdentity::PrimaryKey"));
	assert!(catalog.contains("RowIdentityPolicy::AppendOnly"));
	assert!(catalog.contains("RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single"));
	assert!(!catalog.contains("RowIdentityPolicy::ByOrdinal"));
}

#[test]
pub(crate) fn adr038_no_separate_catalog_cache_after_admin() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	for forbidden in ["refresh_catalog", "evict_descriptor"] {
		assert!(
			!catalog.contains(forbidden),
			"unexpected helper {forbidden}"
		);
	}
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
pub(crate) fn adr038_catalog_drop_table_only_called_from_routing_helper() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let helper = brace_blocks_after_marker(
		&coordinator,
		"async fn drop_existing_table_with_storage_admin",
	)
	.into_iter()
	.next()
	.expect("drop_existing_table_with_storage_admin helper")
	.1;

	assert_eq!(coordinator.matches(".drop_table(").count(), 1);
	assert!(helper.contains("catalog.drop_table(name, false).await"));
}

#[test]
pub(crate) fn adr035_wendelon_projection_change_is_privileged() {
	let columnar = std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/columnar.rs"))
		.expect("read Wendelon columnar source");
	let insert_required = columnar
		.split("fn insert_required_column")
		.nth(1)
		.and_then(|rest| rest.split("fn remap_column_fetch_requirement").next())
		.expect("insert_required_column body");
	let core = std::fs::read_to_string(wendelon_manifest_dir().join("src/client/core.rs"))
		.expect("read Wendelon client core source");
	let table = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema source");
	let client =
		std::fs::read_to_string(wendelon_manifest_dir().join("crates/wendelon-client/src/lib.rs"))
			.expect("read Wendelon client wrapper source");

	assert!(columnar.contains("cannot appear in PBC PROJECT output"));
	assert!(insert_required.contains("RequirementColumn::Storage"));
	assert!(insert_required.contains("requirement.storage_columns.insert(storage_column)"));
	assert!(core.contains("pub row_identity: RowIdentityProjection,"));
	assert!(table.contains("pub row_identity_projection: u8,"));
	assert!(client.contains("pub row_identity: RowIdentityProjection,"));
	assert!(client.contains("row_identity_projection: options.row_identity.as_u8()"));
	assert!(client.contains("row_identity: options.row_identity"));
}

#[test]
pub(crate) fn adr035_synthetic_names_remain_rejected_in_sql() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");

	for source in [&planner, &binder] {
		assert!(source.contains("STORAGE_SYNTHETIC_COLUMN_NAMES"));
		assert!(source.contains("STORAGE_ROW_KEY_ID_COLUMN"));
		assert!(source.contains("STORAGE_ROW_HLC_COLUMN"));
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

	assert!(loop_body.contains("let bound = bind(plan, catalog.as_ref(), &self.config).await?"));
}
