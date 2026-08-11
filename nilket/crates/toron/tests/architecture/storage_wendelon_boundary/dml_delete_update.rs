#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, ring_placement::*, sql_planner_surface::*,
};
use super::{
	ddl_admin::*, descriptor_invalidation::*, returning::*, row_identity::*, wendelon_catalog::*,
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
	assert_eq!(enum_variant_count(node), 16);
	for variant in [
		"Column",
		"Literal",
		"NullLiteral",
		"Cast",
		"TryCast",
		"Binary",
		"Unary",
		"InList",
		"Like",
		"Substring",
		"Case",
		"Floor",
		"TimeBucket",
		"AtTimeZone",
		"Extract",
		"DateTrunc",
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
	let exec = exec_source();
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
	let binder = binder_source();
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

	assert!(execution_plan.contains("Insert {\n\t\tlocal_table_id: TableId,"));
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
	let exec = exec_source();
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
			< fragment_plan.find("fragment_execution_plan").unwrap()
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

	assert_eq!(enum_variant_count(logical_plan), 29);
	assert!(logical_plan.contains("Union {"));
	assert!(logical_plan.contains("Distinct {"));
	assert!(logical_plan.contains("InsertSelect {"));
	assert!(logical_plan.contains("Explain {"));
	assert!(logical_plan.contains("ExplainAnalyze {"));
	assert!(logical_plan.contains("CreateLogicalType {"));
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
	assert_eq!(enum_variant_count(execution_plan), 23);
	assert!(execution_plan.contains("Union {"));
	assert!(execution_plan.contains("EmptyRelation {"));
	assert!(execution_plan.contains("SystemTable {"));
	assert!(execution_plan.contains("InsertSelect {"));
	assert!(execution_plan.contains("CreateLogicalType {"));
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
	assert!(execution_plan.contains("local_table_id: TableId,"));
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
	let binder = binder_source();
	let bind_insert_select = brace_blocks_after_marker(&binder, "async fn bind_insert_select")
		.into_iter()
		.next()
		.expect("bind_insert_select body")
		.1;

	assert!(bind_insert_select.contains("resolve_insert_target_columns(&schema, columns)?"));
	assert!(bind_insert_select.contains("validate_omitted_insert_target_columns"));
	assert!(bind_insert_select.contains(
		"bind_plan_with_trace(source, trace, render_trace, join_order_source_capture, None)"
	));
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
	let exec = exec_source();
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
			< fragment_plan.find("fragment_execution_plan").unwrap()
	);
}

#[test]
pub(crate) fn adr032_distributed_walkers_treat_insert_select_as_leaf() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	for (function, marker) in [
		(
			"fn first_join",
			"| ExecutionPlan::RecursiveRef { .. } => None",
		),
		(
			"fn find_exchange",
			"| ExecutionPlan::RecursiveRef { .. } => None",
		),
		(
			"fn count_exchanges",
			"| ExecutionPlan::RecursiveRef { .. } => 0",
		),
		(
			"fn first_scan_metadata",
			"| ExecutionPlan::RecursiveRef { .. } => None",
		),
		(
			"fn replace_exchange_with_scan",
			"| ExecutionPlan::RecursiveRef { .. } => plan",
		),
		(
			"fn replace_first_join",
			"| ExecutionPlan::RecursiveRef { .. } => plan",
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
			"| ExecutionPlan::RecursiveRef { .. } => None",
		),
		(
			"fn contains_join",
			"| ExecutionPlan::RecursiveRef { .. } => false",
		),
		(
			"fn contains_broadcast_join",
			"| ExecutionPlan::RecursiveRef { .. } => false",
		),
		(
			"fn scan_schema_for_table_id",
			"| ExecutionPlan::RecursiveRef { .. } => None",
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
pub(crate) fn adr036_sql_planner_accepts_delete() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	assert!(planner.contains("Statement::Delete(delete) => self.lower_delete(delete)"));
	assert!(
		planner.contains("SELECT, EXPLAIN, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE")
	);
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

	assert_eq!(enum_variant_count(logical_plan), 29);
	assert!(logical_plan.contains("Union {"));
	assert!(logical_plan.contains("Distinct {"));
	assert!(logical_plan.contains("Delete {"));
	assert!(logical_plan.contains("Explain {"));
	assert!(logical_plan.contains("ExplainAnalyze {"));
	assert!(logical_plan.contains("CreateLogicalType {"));
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

	assert_eq!(enum_variant_count(execution_plan), 23);
	assert!(execution_plan.contains("Union {"));
	assert!(execution_plan.contains("EmptyRelation {"));
	assert!(execution_plan.contains("SystemTable {"));
	assert!(execution_plan.contains("Delete {"));
	assert!(execution_plan.contains("CreateLogicalType {"));
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
		"local_table_id: TableId",
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
	let binder = binder_source();
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
	let binder = binder_source();
	let bind_delete = brace_blocks_after_marker(&binder, "async fn bind_delete")
		.into_iter()
		.next()
		.expect("bind_delete body")
		.1;

	assert!(bind_delete.contains("predicate_pushdown_plan(program, &scan, &logical_types)"));
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
	assert!(delete_reject < fragment_plan.find("fragment_execution_plan").unwrap());
}

#[test]
pub(crate) fn adr036_executor_delete_defensive_arms() {
	let exec = exec_source();

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
pub(crate) fn adr036_filter_helper_preserves_execution_plan_filter() {
	let exec = exec_source();
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
pub(crate) fn adr063_sql_planner_accepts_statement_update() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let plan_body = brace_blocks_after_marker(&planner, "pub fn plan")
		.into_iter()
		.next()
		.expect("plan body")
		.1;
	let lower_statement = brace_blocks_after_marker(&planner, "fn lower_statement")
		.into_iter()
		.next()
		.expect("lower_statement body")
		.1;
	let dispatch_body = format!("{plan_body}\n{lower_statement}");

	assert!(dispatch_body.contains("Statement::Update(update) => self.lower_update(update)"));
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
	let lower_statement = brace_blocks_after_marker(&planner, "fn lower_statement")
		.into_iter()
		.next()
		.expect("lower_statement body")
		.1;
	let dispatch_body = format!("{plan_body}\n{lower_statement}");

	assert!(
		dispatch_body
			.contains("SELECT, EXPLAIN, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE")
	);
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
		"local_table_id: TableId",
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
pub(crate) fn adr063_binder_builds_full_row_source_projection() {
	let binder = binder_source();
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
	let binder = binder_source();
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
	let binder = binder_source();
	let predicate_helper =
		brace_blocks_after_marker(&binder, "fn bind_update_source_for_predicate")
			.into_iter()
			.next()
			.expect("bind_update_source_for_predicate")
			.1;

	assert!(predicate_helper.contains("plan_filter_with_predicate_pushdown("));
	assert!(predicate_helper.contains("scan,"));
	assert!(predicate_helper.contains("bound_predicate,"));
	assert!(predicate_helper.contains("&logical_types"));
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
		.find("ExecutionPlan::Update {\n\t\t\tlocal")
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
pub(crate) fn adr063_update_remains_non_fragmentable_and_worker_rejected() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let exec = exec_source();

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
pub(crate) fn adr063_no_transaction_lock_compare_swap_surface_after_adr070() {
	let binder = binder_source();
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let update_start = coordinator
		.find("ExecutionPlan::Update {\n\t\t\tlocal")
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
