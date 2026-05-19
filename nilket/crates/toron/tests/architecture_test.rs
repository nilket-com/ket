use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::{BindingError, StorageError};
use toron::exec::Executor;
use toron::ir::{Expr, LogicalPlan, ScalarValue, TableId};
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

#[test]
fn no_eval_time_string_lookup() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("column_by_name"))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"RecordBatch::column_by_name found outside binding code:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[tokio::test]
async fn pbc_engaged_on_eligible_filter() {
	let batch = batch();
	let observations = Arc::new(Mutex::new(Vec::new()));
	let storage = Arc::new(RecordingStorageScan {
		batch,
		observations: observations.clone(),
	});
	let executor = Executor::new(storage);
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Gt(
			Box::new(Expr::column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
		),
	};
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	executor
		.execute_bound(&bound, None)
		.await
		.expect("execute bound");

	let observations = observations.lock().unwrap();
	assert_eq!(observations.len(), 1);
	assert!(
		observations[0],
		"eligible filter did not send pbc_predicate"
	);
}

#[tokio::test]
async fn pbc_declines_arithmetic_filter() {
	let batch = batch();
	let observations = Arc::new(Mutex::new(Vec::new()));
	let storage = Arc::new(RecordingStorageScan {
		batch,
		observations: observations.clone(),
	});
	let executor = Executor::new(storage);
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Gt(
			Box::new(Expr::Add(
				Box::new(Expr::column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
			)),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
		),
	};
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	executor
		.execute_bound(&bound, None)
		.await
		.expect("execute bound");

	let observations = observations.lock().unwrap();
	assert_eq!(observations.len(), 1);
	assert!(
		!observations[0],
		"arithmetic filter must not send pbc_predicate"
	);
}

#[tokio::test]
async fn pbc_declines_cast_filter() {
	let batch = batch();
	let observations = Arc::new(Mutex::new(Vec::new()));
	let storage = Arc::new(RecordingStorageScan {
		batch,
		observations: observations.clone(),
	});
	let executor = Executor::new(storage);
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Gt(
			Box::new(Expr::Cast(
				Box::new(Expr::column("amount")),
				DataType::Int64,
			)),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(100)))),
		),
	};
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	executor
		.execute_bound(&bound, None)
		.await
		.expect("execute bound");

	let observations = observations.lock().unwrap();
	assert_eq!(observations.len(), 1);
	assert!(!observations[0], "cast filter must not send pbc_predicate");
}

#[tokio::test]
async fn pbc_declines_identity_cast_filter() {
	let batch = batch();
	let observations = Arc::new(Mutex::new(Vec::new()));
	let storage = Arc::new(RecordingStorageScan {
		batch,
		observations: observations.clone(),
	});
	let executor = Executor::new(storage);
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Gt(
			Box::new(Expr::Cast(
				Box::new(Expr::column("amount")),
				DataType::Int32,
			)),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
		),
	};
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	executor
		.execute_bound(&bound, None)
		.await
		.expect("execute bound");

	let observations = observations.lock().unwrap();
	assert_eq!(observations.len(), 1);
	assert!(
		!observations[0],
		"identity cast filter must not send pbc_predicate"
	);
}

#[tokio::test]
async fn pbc_declines_try_cast_filter() {
	let batch = batch();
	let observations = Arc::new(Mutex::new(Vec::new()));
	let storage = Arc::new(RecordingStorageScan {
		batch,
		observations: observations.clone(),
	});
	let executor = Executor::new(storage);
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::IsNotNull(Box::new(Expr::TryCast(
			Box::new(Expr::column("amount")),
			DataType::Int64,
		))),
	};
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	executor
		.execute_bound(&bound, None)
		.await
		.expect("execute bound");

	let observations = observations.lock().unwrap();
	assert_eq!(observations.len(), 1);
	assert!(
		!observations[0],
		"try cast filter must not send pbc_predicate"
	);
}

#[tokio::test]
async fn pbc_declines_identity_try_cast_filter() {
	let batch = batch();
	let observations = Arc::new(Mutex::new(Vec::new()));
	let storage = Arc::new(RecordingStorageScan {
		batch,
		observations: observations.clone(),
	});
	let executor = Executor::new(storage);
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Gt(
			Box::new(Expr::TryCast(
				Box::new(Expr::column("amount")),
				DataType::Int32,
			)),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
		),
	};
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	executor
		.execute_bound(&bound, None)
		.await
		.expect("execute bound");

	let observations = observations.lock().unwrap();
	assert_eq!(observations.len(), 1);
	assert!(
		!observations[0],
		"identity try cast filter must not send pbc_predicate"
	);
}

#[test]
fn no_public_result_string_boundaries() {
	let manifest_dir = manifest_dir();
	let result_marker = ["Result", "<"].concat();
	let string_marker = ["String", ">"].concat();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("pub ") && line.contains(&result_marker) && line.contains(&string_marker)
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"public Result<_, String> boundaries found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
fn bound_expr_node_has_no_string_column_variant() {
	let source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");

	assert!(!source.contains("Column(String)"));
	assert!(source.contains("Column(ResolvedColumn)"));
}

#[tokio::test]
async fn experimental_kernels_are_gated() {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		output_names: vec![None],
		exprs: vec![Expr::TimeBucket(
			std::time::Duration::from_secs(60),
			Box::new(Expr::column("ts")),
		)],
	};

	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("default planner must reject experimental kernels");
	assert!(matches!(err, BindingError::ExperimentalKernel { .. }));
}

#[test]
fn scan_request_requires_descriptor_pin() {
	let source = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let scan_request = source
		.split("pub struct ScanRequest")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ScanStream").next())
		.expect("ScanRequest block");

	assert!(
		scan_request
			.contains("pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,")
	);
	assert!(!scan_request.contains("physical_descriptor_fingerprint: Option<"));
}

#[test]
fn wendelon_storage_scan_forwards_runtime_filter_to_table_query() {
	let source = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let impl_block = source
		.split("impl StorageScan for WendelonStorageScan")
		.nth(1)
		.and_then(|rest| rest.split("fn append_projection").next())
		.expect("WendelonStorageScan StorageScan impl block");

	assert!(impl_block.contains(".table_query_with_runtime_filter("));
	assert!(impl_block.contains("request.runtime_filter.clone()"));
	assert!(!impl_block.contains(".table_query("));
}

#[test]
fn adr014_runtime_filter_descriptor_is_kind_tagged_without_bloom_outer_fields() {
	let source =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/runtime_filter.rs"))
			.expect("read Wendelon runtime-filter source");
	let descriptor = source
		.split("pub struct RuntimeFilterDescriptor")
		.nth(1)
		.and_then(|rest| rest.split("pub enum RuntimeFilterKind").next())
		.expect("RuntimeFilterDescriptor block");
	let kind = source
		.split("pub enum RuntimeFilterKind")
		.nth(1)
		.and_then(|rest| rest.split("pub struct RuntimeFilterBloom").next())
		.expect("RuntimeFilterKind block");

	assert!(descriptor.contains("pub kind: RuntimeFilterKind,"));
	for forbidden in [
		"hash_seed",
		"bit_count",
		"hash_count",
		"byte_budget",
		"target_fp_rate_bp",
		"payload",
	] {
		assert!(
			!descriptor.contains(forbidden),
			"Bloom-specific field {forbidden} leaked onto RuntimeFilterDescriptor"
		);
	}
	assert_eq!(enum_variant_count(kind), 2);
	assert!(kind.contains("Bloom(RuntimeFilterBloom)"));
	assert!(kind.contains("ExactSet(RuntimeFilterExactSet)"));
}

#[test]
fn adr014_runtime_filter_descriptor_literals_name_kind() {
	let manifest_dir = manifest_dir();
	let source_paths = [
		manifest_dir.join("src"),
		manifest_dir.join("tests"),
		wendelon_manifest_dir().join("src/schema/runtime_filter.rs"),
		wendelon_manifest_dir().join("src/network/data_server.rs"),
	];
	let mut offenders = Vec::new();
	for path in source_paths {
		let files = if path.is_dir() {
			source_files(&path)
		} else {
			vec![path]
		};
		for file in files {
			let source = std::fs::read_to_string(&file).expect("read source");
			for (line, block) in brace_blocks_after_marker(&source, "RuntimeFilterDescriptor {") {
				let source_line = source.lines().nth(line - 1).unwrap_or("");
				if source_line.contains("-> RuntimeFilterDescriptor {") {
					continue;
				}
				if !block.contains("kind:") {
					offenders.push((
						file.clone(),
						line,
						block.lines().next().unwrap_or("").to_string(),
					));
				}
			}
		}
	}

	assert!(
		offenders.is_empty(),
		"RuntimeFilterDescriptor literal/destructure without kind:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
fn adr014_runtime_filter_contract_has_no_version_suffixed_types() {
	let manifest_dir = manifest_dir();
	let markers = [
		["RuntimeFilterDescriptor", "V"].concat(),
		["RuntimeFilterKind", "V"].concat(),
		["RuntimeFilterBloom", "V"].concat(),
		["RuntimeFilterExactSet", "V"].concat(),
		["RuntimeFilterFragment", "V"].concat(),
	];
	let mut offenders = source_lines(&manifest_dir.join("src"));
	offenders.extend(source_lines(&manifest_dir.join("tests")));
	offenders.extend(source_lines(&wendelon_manifest_dir().join("src/schema")));
	offenders.retain(|(_, _, line)| markers.iter().any(|marker| line.contains(marker)));

	assert!(
		offenders.is_empty(),
		"version-suffixed runtime-filter type found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
fn adr014_does_not_add_sql_in_lowering_to_toron_planner() {
	let source = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	for forbidden in ["InList", "InSubquery", "Expr::InList", "Expr::InSubquery"] {
		assert!(
			!source.contains(forbidden),
			"ADR 014 must not add SQL IN lowering marker {forbidden}"
		);
	}
}

#[test]
fn scan_stream_collection_localized() {
	let manifest_dir = manifest_dir();
	let try_collect_marker = ["try", "_", "collect"].concat();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			let relative = path.strip_prefix(&manifest_dir).unwrap_or(path);
			if relative == Path::new("src/coordinator.rs") || relative == Path::new("src/error.rs")
			{
				return false;
			}
			line.contains("ScanStream::collect")
				|| line.contains(".batches.collect()")
				|| line.contains(&try_collect_marker)
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ScanStream collection outside coordinator boundary:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
fn architecture_guard_rune_context_is_allow_listed() {
	let source = std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read rune engine source");
	let default_modules_marker = ["with", "_", "default", "_", "modules"].concat();

	assert!(!source.contains(&default_modules_marker));
	assert!(source.contains("Context::new()"));
	assert!(source.contains("install_minimal_primitives"));
}

#[test]
fn distributed_identity_keeps_morsel_id_worker_local() {
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
fn message_kind_retired_morsel_complete() {
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
fn task_failure_kind_keeps_partition_store_errors_separate() {
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
fn architecture_guards_unchanged_plus_sort_variant() {
	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical_source
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");
	assert_eq!(enum_variant_count(logical_plan), 15);
	assert!(logical_plan.contains("Sort {"));
	assert!(logical_plan.contains("CreateTable {"));
	assert!(logical_plan.contains("CreateTableAsSelect {"));
	assert!(logical_plan.contains("DropTable {"));
	assert!(logical_plan.contains("Insert {"));
	assert!(logical_plan.contains("InsertSelect {"));
	assert!(logical_plan.contains("output_names: Vec<Option<String>>"));
	assert!(logical_source.contains("pub struct SortExpr"));
	assert!(logical_source.contains("pub enum InsertLiteral"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution_source
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	assert_eq!(enum_variant_count(execution_plan), 13);
	assert!(execution_plan.contains("Sort {"));
	assert!(execution_plan.contains("CreateTable {"));
	assert!(execution_plan.contains("CreateTableAsSelect {"));
	assert!(execution_plan.contains("DropTable {"));
	assert!(execution_plan.contains("Insert {"));
	assert!(execution_plan.contains("InsertSelect {"));
	assert!(execution_plan.contains("predicate: BoundExprProgram"));
	assert!(execution_plan.contains("exprs: BoundExprProgram"));
	assert!(execution_plan.contains("group_expr: BoundExprProgram"));
	assert!(execution_plan.contains("aggr_expr: Vec<BoundAggregateCall>"));
	assert!(execution_plan.contains("key: BoundExprProgram"));
	assert!(execution_source.contains("pub struct ExecutionSortKey"));
}

#[test]
fn adr031_bound_expr_program_shape() {
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
fn adr031_bound_expr_node_has_no_boxes() {
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
	assert_eq!(enum_variant_count(node), 7);
	for variant in [
		"Column",
		"Literal",
		"Cast",
		"TryCast",
		"Binary",
		"Unary",
		"TimeBucket",
	] {
		assert!(node.contains(variant), "missing BoundExprNode::{variant}");
	}
}

#[test]
fn adr031_bound_expr_program_partial_eq_only() {
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
fn adr031_execution_plan_uses_bound_expr_program() {
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
fn adr031_execution_plan_insert_stays_expression_free() {
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
fn adr031_join_and_sort_remain_ordinal_keyed() {
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
fn adr031_pbc_reads_bound_expr_program() {
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
fn adr031_executor_evaluates_program_linearly() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let evaluate = brace_blocks_after_marker(&exec, "pub fn evaluate_bound_program")
		.into_iter()
		.next()
		.expect("evaluate_bound_program block")
		.1;
	assert!(evaluate.contains("program.nodes.iter().enumerate()"));
	assert_eq!(evaluate.matches("evaluate_bound_program(").count(), 1);
}

#[test]
fn adr031_binder_emits_bound_expr_program() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	assert!(binder.contains("BoundExprProgramBuilder"));
	assert!(binder.contains("fn bind_expr_program"));
	assert!(binder.contains("fn bind_expr_into_program"));
	assert!(!binder.contains("ExecutionExpr::Cast"));
	assert!(!binder.contains("ExecutionExpr::TryCast"));
}

#[test]
fn adr031_adr028_signature_stays_logical_expr() {
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
fn lower_query_and_lower_select_have_no_rest_pattern() {
	let source = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	for function in ["fn lower_query", "fn lower_select"] {
		let body = source
			.split(function)
			.nth(1)
			.and_then(|rest| rest.split("\n\tfn ").next())
			.expect(function);
		assert!(
			!body.contains(".."),
			"{function} contains a rest pattern and may silently drop parser fields"
		);
	}
}

#[test]
fn lower_select_does_not_return_input_from_wildcard_projection_loop() {
	let source = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_select = source
		.split("fn lower_select")
		.nth(1)
		.and_then(|rest| rest.split("\n\tfn lower_order_by").next())
		.expect("lower_select body");
	let projection_loop = lower_select
		.split("for item in projection")
		.nth(1)
		.and_then(|rest| rest.split("Ok(LogicalPlan::Project").next())
		.expect("non-aggregate projection loop");

	assert!(lower_select.contains("matches!(projection.as_slice(), [SelectItem::Wildcard(_)])"));
	assert!(
		!projection_loop.contains("return Ok(plan)"),
		"wildcard projection loop still returns the input plan"
	);
	assert!(projection_loop.contains("mixed wildcard projections are not supported"));
}

#[test]
fn adr015_grouped_aggregate_ordering_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("GROUP BY with ORDER BY is not supported"));

	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let detector_index = fragmenter
		.find("is_terminal_sort_over_grouped_aggregate(&plan)")
		.expect("ADR 015 detector call");
	let reject_index = fragmenter
		.find("FragmentError::MultipleExchangeBoundaries")
		.expect("multi-exchange reject");
	assert!(
		detector_index < reject_index,
		"ADR 015 detector must stay before the generic multi-exchange reject"
	);

	let detector = fragmenter
		.split("fn is_terminal_sort_over_grouped_aggregate")
		.nth(1)
		.and_then(|rest| {
			rest.split("fn fragment_terminal_sort_over_grouped_aggregate")
				.next()
		})
		.expect("ADR 015 detector block");
	assert!(detector.contains("ExecutionPlan::Sort"));
	assert!(detector.contains("ExecutionPlan::Exchange"));
	assert!(detector.contains("ExecutionPlan::Aggregate"));
	assert!(detector.contains("*partition_count != 1 || !key.is_empty_roots()"));
	assert!(detector.contains("partition_count: group_partition_count"));
	assert!(fragmenter.contains("if exchange_count > 1"));
}

#[test]
fn aggregate_kernel_catalog_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("HAVING is not supported"));
	assert!(planner.contains("HAVING aggregate-call must appear in SELECT list"));
	assert!(planner.contains("ambiguous HAVING aggregate-call"));
	assert!(planner.contains("qualified HAVING column references are not supported"));
	assert!(planner.contains("unsupported aggregate function"));

	let lib = std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source");
	assert!(lib.contains("mod aggregate_kernel;"));
	assert!(!lib.contains("pub mod aggregate_kernel"));

	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	assert!(catalog.contains("debug_assert_eq!(spec.kind, kernel);"));
	let stable_kernel_enum = catalog
		.split("pub(crate) enum StableAggregateKernel")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("stable aggregate kernel enum");
	for variant in ["CountAll", "Count", "Sum", "Min", "Max", "Avg"] {
		assert!(
			stable_kernel_enum.contains(variant),
			"stable aggregate kernel missing {variant}"
		);
	}
	assert_eq!(enum_variant_count(stable_kernel_enum), 6);

	let stable_rows = catalog
		.split("STABLE_AGGREGATE_KERNELS")
		.nth(1)
		.and_then(|rest| rest.split("];").next())
		.expect("stable aggregate kernel rows");
	assert_eq!(stable_rows.matches("AggregateKernelSpec {").count(), 6);
	for marker in [
		"kind: StableAggregateKernel::CountAll",
		"kind: StableAggregateKernel::Count",
		"kind: StableAggregateKernel::Sum",
		"kind: StableAggregateKernel::Min",
		"kind: StableAggregateKernel::Max",
		"kind: StableAggregateKernel::Avg",
		"arity: AggregateArity::Zero",
		"arity: AggregateArity::One",
		"input_domain: AggregateInputDomain::Any",
		"input_domain: AggregateInputDomain::SumNumeric",
		"input_domain: AggregateInputDomain::AvgNumeric",
		"input_domain: AggregateInputDomain::OrderedMinMax",
		"output_rule: AggregateOutputRule::Int64",
		"output_rule: AggregateOutputRule::Float64",
		"output_rule: AggregateOutputRule::SameAsInput",
		"output_rule: AggregateOutputRule::SumNumeric",
		"nullable: false",
		"nullable: true",
		"grouped_supported: true",
	] {
		assert!(stable_rows.contains(marker), "catalog missing {marker}");
	}
	assert_eq!(stable_rows.matches("sql_name: \"COUNT\"").count(), 2);
	for sql_name in ["COUNT", "SUM", "MIN", "MAX", "AVG"] {
		assert!(
			stable_rows.contains(&format!("sql_name: \"{sql_name}\"")),
			"catalog missing SQL name {sql_name}"
		);
	}
	for output_name in ["count", "sum", "min", "max", "avg"] {
		assert!(
			stable_rows.contains(&format!("output_name: \"{output_name}\"")),
			"catalog missing output name {output_name}"
		);
	}
	assert!(!stable_rows.contains("LinearRegress"));
	assert!(!stable_rows.contains("ApproxQuantile"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let filter_disposition = execution_source
		.split("pub enum FilterDisposition")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ExecutionSortKey").next())
		.expect("FilterDisposition enum");
	assert_eq!(enum_variant_count(filter_disposition), 2);
	assert!(filter_disposition.contains("EvaluateInToron"));

	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let detector = fragmenter
		.split("fn is_terminal_sort_over_grouped_aggregate")
		.nth(1)
		.and_then(|rest| {
			rest.split("fn fragment_terminal_sort_over_grouped_aggregate")
				.next()
		})
		.expect("ADR 016 detector block");
	assert!(detector.contains("ExecutionPlan::Filter"));
	assert!(detector.contains("ExecutionPlan::Aggregate"));
	assert!(detector.contains("having = Some"));

	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let agg_state_enum = exec
		.split("enum AggState")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("AggState enum");
	assert_eq!(enum_variant_count(agg_state_enum), 13);
	let before_agg_state = exec.split("enum AggState").next().unwrap_or("");
	let derives_above = before_agg_state.rsplit("#[derive").next().unwrap_or("");
	assert!(derives_above.contains("Copy"));
}

#[test]
fn bind_expr_remains_aggregate_free() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let bind_expr = binder
		.split("fn bind_expr")
		.nth(1)
		.and_then(|rest| rest.split("fn bind_comparison").next())
		.expect("bind_expr block");
	assert!(!bind_expr.contains("AggregateCall"));
	assert!(!bind_expr.contains("AggregateFunction"));
}

#[test]
fn adr017_order_by_aggregate_call_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(
		planner.contains(
			"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list"
		)
	);
	assert!(planner.contains("ambiguous ORDER BY aggregate-call"));
	assert!(planner.contains("qualified HAVING column references are not supported"));
	assert!(planner.contains("struct LoweredSelect"));
	assert!(planner.contains("aggregate_output_catalog: Option<AggregateOutputCatalog>"));
	assert!(planner.contains("fn rewrite_grouped_aggregate_calls"));
	assert!(planner.contains("struct AggregateOutputCatalog"));
	assert!(!planner.contains("HavingAggregateScope"));
	assert!(!planner.contains("rewrite_having_aggregate_calls"));

	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let bind_expr = binder
		.split("fn bind_expr")
		.nth(1)
		.and_then(|rest| rest.split("fn bind_comparison").next())
		.expect("bind_expr block");
	assert!(!bind_expr.contains("AggregateCall"));
	assert!(!bind_expr.contains("AggregateFunction"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let filter_disposition = execution_source
		.split("pub enum FilterDisposition")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ExecutionSortKey").next())
		.expect("FilterDisposition enum");
	assert_eq!(enum_variant_count(filter_disposition), 2);
}

#[test]
fn adr018_scalar_aggregate_having_order_by_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(planner.contains("HAVING requires GROUP BY or an aggregate projection"));
	assert!(!planner.contains("HAVING without GROUP BY is not supported"));
	assert!(planner.contains("qualified HAVING column references are not supported"));
	assert!(planner.contains("HAVING aggregate-call must appear in SELECT list"));
	assert!(
		planner.contains("HAVING does not resolve SELECT aliases; spell the aggregate explicitly")
	);
	assert!(planner.contains("ambiguous HAVING aggregate-call"));
	assert!(planner.contains("ambiguous ORDER BY aggregate-call"));
	assert!(
		planner.contains(
			"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list"
		)
	);

	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let bind_expr = binder
		.split("fn bind_expr")
		.nth(1)
		.and_then(|rest| rest.split("fn bind_comparison").next())
		.expect("bind_expr block");
	assert!(!bind_expr.contains("AggregateCall"));
	assert!(!bind_expr.contains("AggregateFunction"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let filter_disposition = execution_source
		.split("pub enum FilterDisposition")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ExecutionSortKey").next())
		.expect("FilterDisposition enum");
	assert_eq!(enum_variant_count(filter_disposition), 2);
}

#[test]
fn adr023_post_aggregate_projection_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("GroupedAggregateScope"));
	assert!(planner.contains("struct AggregateOutputCatalog"));
	assert!(!planner.contains("GROUP BY projection must list all keys in order before aggregates"));
	assert!(
		planner.contains("HAVING does not resolve SELECT aliases; spell the aggregate explicitly")
	);
	assert!(
		planner.contains(
			"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list"
		)
	);

	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let project_variant = logical_source
		.split("Project {")
		.nth(1)
		.and_then(|rest| rest.split("Sort {").next())
		.expect("LogicalPlan::Project variant");
	assert!(project_variant.contains("output_names: Vec<Option<String>>"));

	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let detector = fragmenter
		.split("fn is_terminal_sort_over_grouped_aggregate")
		.nth(1)
		.and_then(|rest| {
			rest.split("fn fragment_terminal_sort_over_grouped_aggregate")
				.next()
		})
		.expect("ADR 023 detector block");
	let project_index = detector
		.find("ExecutionPlan::Project")
		.expect("detector peels Project");
	let filter_index = detector
		.find("ExecutionPlan::Filter")
		.expect("detector peels Filter");
	assert!(
		project_index < filter_index,
		"terminal sort detector must peel Project before Filter"
	);
}

#[test]
fn adr024_scalar_arithmetic_guards_are_narrow() {
	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	for variant in [
		"Cast(Box<Expr>, DataType)",
		"TryCast(Box<Expr>, DataType)",
		"Add(Box<Expr>, Box<Expr>)",
		"Sub(Box<Expr>, Box<Expr>)",
		"Mul(Box<Expr>, Box<Expr>)",
		"Div(Box<Expr>, Box<Expr>)",
	] {
		assert!(logical_source.contains(variant), "missing Expr::{variant}");
	}

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let binary_op = execution_source
		.split("pub enum BoundBinaryOp")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("BoundBinaryOp enum");
	for variant in ["Add", "Sub", "Mul", "Div"] {
		assert!(
			binary_op.contains(variant),
			"missing BoundBinaryOp::{variant}"
		);
	}
	let node = execution_source
		.split("pub enum BoundExprNode")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("BoundExprNode enum");
	assert!(node.contains("Cast {"));
	assert!(node.contains("TryCast {"));
	assert!(node.contains("Binary {"));
	assert!(node.contains("op: BoundBinaryOp"));
	assert!(node.contains("left: BoundExprId"));
	assert!(node.contains("right: BoundExprId"));
	assert!(execution_source.contains("pub fn arithmetic_output_type"));
	assert!(execution_source.contains("(Int32, Int32) => Some(Int32)"));
	assert!(execution_source.contains("(Int32, Int64) | (Int64, Int32) | (Int64, Int64)"));
	assert!(execution_source.contains("(Float32, Float32) => Some(Float32)"));

	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	for operator in [
		"BinaryOperator::Plus => Ok(Expr::Add",
		"BinaryOperator::Minus => Ok(Expr::Sub",
		"BinaryOperator::Multiply => Ok(Expr::Mul",
		"BinaryOperator::Divide => Ok(Expr::Div",
	] {
		assert!(
			planner.contains(operator),
			"missing SQL lowerer for {operator}"
		);
	}
	assert!(!planner.contains("aggregate arguments must be simple column references"));
	assert!(planner.contains("aggregate arguments must not contain aggregate calls"));

	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	assert!(binder.contains("fn bind_arithmetic"));
	assert!(binder.contains("fn is_widening_cast"));
	for conversion in [
		"(DataType::Int32, DataType::Int64)",
		"(DataType::Int32, DataType::Float64)",
		"(DataType::Int64, DataType::Float64)",
		"(DataType::Float32, DataType::Float64)",
	] {
		assert!(binder.contains(conversion), "missing cast {conversion}");
	}

	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read PBC source");
	assert!(pbc.contains("arithmetic, Cast, and TryCast are not PBC-eligible"));
	assert!(!pbc.contains("OP_ADD"));
	assert!(!pbc.contains("OP_SUB"));
	assert!(!pbc.contains("OP_MUL"));
	assert!(!pbc.contains("OP_DIV"));

	let pbc_helper = brace_blocks_after_marker(&logical_source, "fn is_pbc_pushdown_eligible")
		.into_iter()
		.next()
		.expect("is_pbc_pushdown_eligible body")
		.1;
	assert!(pbc_helper.contains("Self::TryCast(_, _)"));
	assert!(pbc_helper.contains("false"));
}

#[test]
fn adr025_sql_cast_lowers_via_planner() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_expr = brace_blocks_after_marker(&planner, "fn lower_expr")
		.into_iter()
		.next()
		.expect("lower_expr body")
		.1;

	assert!(lower_expr.contains("SqlExpr::Cast {"));
	assert!(lower_expr.contains("CastKind::Cast => Ok(Expr::Cast"));
	assert!(lower_expr.contains("lower_cast_target_type(data_type)?"));
	assert!(lower_expr.contains("if format.is_some()"));
	assert!(lower_expr.contains("CAST FORMAT clause is not supported"));

	let rewrite_grouped_aggregates =
		brace_blocks_after_marker(&planner, "fn rewrite_grouped_aggregate_calls")
			.into_iter()
			.next()
			.expect("rewrite_grouped_aggregate_calls body")
			.1;
	assert!(rewrite_grouped_aggregates.contains("SqlExpr::Cast {"));
	assert!(rewrite_grouped_aggregates.contains("self.rewrite_grouped_aggregate_calls(expr"));

	for (marker, guard) in [
		(
			"fn reject_qualified_having_column_references",
			"reject_qualified_having_column_references",
		),
		(
			"fn reject_having_alias_references(\n\texpr",
			"reject_having_alias_references",
		),
		(
			"fn reject_qualified_aggregate_projection_references",
			"reject_qualified_aggregate_projection_references",
		),
		(
			"fn expr_contains_aggregate_call",
			"expr_contains_aggregate_call",
		),
		(
			"fn expr_contains_nonaggregate_column_reference",
			"expr_contains_nonaggregate_column_reference",
		),
		("fn collect_scope_refs", "collect_scope_refs"),
	] {
		let body = brace_blocks_after_marker(&planner, marker)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{guard} body"))
			.1;
		assert!(
			body.contains("SqlExpr::Cast"),
			"{guard} must recurse into CAST"
		);
	}
}

#[test]
fn adr025_order_by_cast_uses_hidden_sort_projection() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_query = brace_blocks_after_marker(&planner, "fn lower_query")
		.into_iter()
		.next()
		.expect("lower_query body")
		.1;
	assert!(lower_query.contains("sort_needs_hidden_projection(&order_by)"));
	assert!(lower_query.contains("add_hidden_sort_projection"));
	assert!(lower_query.contains("final_projection"));

	let hidden_gate = brace_blocks_after_marker(&planner, "fn sort_needs_hidden_projection")
		.into_iter()
		.next()
		.expect("sort_needs_hidden_projection body")
		.1;
	assert!(hidden_gate.contains("!sort_key_is_direct_output_reference(&key.expr)"));

	let validator = brace_blocks_after_marker(&planner, "fn validate_hidden_sort_key_refs")
		.into_iter()
		.next()
		.expect("validate_hidden_sort_key_refs body")
		.1;
	assert!(validator.contains("Expr::Cast(inner, _) | Expr::TryCast(inner, _)"));
	assert!(!planner.contains("fn expr_contains_cast_family"));

	let hidden_projection = brace_blocks_after_marker(&planner, "fn add_hidden_sort_projection")
		.into_iter()
		.next()
		.expect("add_hidden_sort_projection body")
		.1;
	assert!(hidden_projection.contains("LogicalPlan::Project"));
	assert!(
		hidden_projection
			.contains("ORDER BY computed aggregate expression requires unique output names")
	);
	assert!(hidden_projection.contains("unique_hidden_sort_name"));

	let visible_names = brace_blocks_after_marker(&planner, "fn visible_output_names")
		.into_iter()
		.next()
		.expect("visible_output_names body")
		.1;
	assert!(visible_names.contains("AggregateOutputCatalog::natural_output_names"));

	let hidden_name = brace_blocks_after_marker(&planner, "fn unique_hidden_sort_name")
		.into_iter()
		.next()
		.expect("unique_hidden_sort_name body")
		.1;
	assert!(hidden_name.contains("__toron_order_key_{idx}"));
}

#[test]
fn adr025_sql_cast_target_type_normalization() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let target_normalizer = brace_blocks_after_marker(&planner, "fn lower_cast_target_type")
		.into_iter()
		.next()
		.expect("lower_cast_target_type body")
		.1;

	for mapping in [
		"SqlDataType::Int32 => Ok(DataType::Int32)",
		"SqlDataType::BigInt(_) | SqlDataType::Int64 => Ok(DataType::Int64)",
		"SqlDataType::Real | SqlDataType::Float4 | SqlDataType::Float32",
		"SqlDataType::Float64 => Ok(DataType::Float64)",
	] {
		assert!(
			target_normalizer.contains(mapping),
			"missing CAST target mapping {mapping}"
		);
	}
	assert!(target_normalizer.contains("SqlDataType::Float(_)"));
	assert!(target_normalizer.contains("unsupported CAST target type: FLOAT"));
	assert!(target_normalizer.contains("unsupported CAST target type: {other}"));
}

#[test]
fn adr025_explicit_cast_policy_is_separate_from_implicit_widening() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let explicit_cast = brace_blocks_after_marker(&binder, "fn bind_explicit_cast")
		.into_iter()
		.next()
		.expect("bind_explicit_cast body")
		.1;
	let implicit_cast = brace_blocks_after_marker(&binder, "fn cast_if_needed")
		.into_iter()
		.next()
		.expect("cast_if_needed body")
		.1;
	let widening = brace_blocks_after_marker(&binder, "fn is_widening_cast")
		.into_iter()
		.next()
		.expect("is_widening_cast body")
		.1;
	let explicit_policy =
		brace_blocks_after_marker(&binder, "fn is_supported_explicit_numeric_cast")
			.into_iter()
			.next()
			.expect("is_supported_explicit_numeric_cast body")
			.1;

	assert!(explicit_cast.contains("is_supported_explicit_numeric_cast"));
	assert!(explicit_cast.contains("builder.append_cast(inner, target.clone(), target.clone())"));
	assert!(!explicit_cast.contains("source == *target"));
	assert!(!explicit_cast.contains("source == target"));
	assert!(implicit_cast.contains("if source == *target"));
	assert!(implicit_cast.contains("is_widening_cast"));

	for conversion in [
		"(DataType::Int32, DataType::Int64)",
		"(DataType::Int32, DataType::Float64)",
		"(DataType::Int64, DataType::Float64)",
		"(DataType::Float32, DataType::Float64)",
	] {
		assert!(
			widening.contains(conversion),
			"implicit widening missing {conversion}"
		);
	}
	assert!(!widening.contains("(DataType::Int64, DataType::Int32)"));
	assert!(!widening.contains("(DataType::Float64, DataType::Float32)"));

	for conversion in [
		"(DataType::Int32, DataType::Int32)",
		"(DataType::Int32, DataType::Int64)",
		"(DataType::Int32, DataType::Float32)",
		"(DataType::Int32, DataType::Float64)",
		"(DataType::Int64, DataType::Int32)",
		"(DataType::Int64, DataType::Int64)",
		"(DataType::Int64, DataType::Float32)",
		"(DataType::Int64, DataType::Float64)",
		"(DataType::Float32, DataType::Int32)",
		"(DataType::Float32, DataType::Int64)",
		"(DataType::Float32, DataType::Float32)",
		"(DataType::Float32, DataType::Float64)",
		"(DataType::Float64, DataType::Int32)",
		"(DataType::Float64, DataType::Int64)",
		"(DataType::Float64, DataType::Float32)",
		"(DataType::Float64, DataType::Float64)",
	] {
		assert!(
			explicit_policy.contains(conversion),
			"explicit CAST policy missing {conversion}"
		);
	}
}

#[test]
fn adr025_cast_supports_numeric_primitive_lattice() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let evaluate_cast = brace_blocks_after_marker(&exec, "fn evaluate_cast")
		.into_iter()
		.next()
		.expect("evaluate_cast body")
		.1;

	assert!(evaluate_cast.contains("if array.data_type() == target"));
	for arm in [
		"(DataType::Int32, DataType::Int64) => cast_i32_to_i64(array)",
		"(DataType::Int32, DataType::Float32) => cast_i32_to_f32(array)",
		"(DataType::Int32, DataType::Float64) => cast_i32_to_f64(array)",
		"(DataType::Int64, DataType::Int32) => cast_i64_to_i32(array)",
		"(DataType::Int64, DataType::Float32) => cast_i64_to_f32(array)",
		"(DataType::Int64, DataType::Float64) => cast_i64_to_f64(array)",
		"(DataType::Float32, DataType::Int32) => cast_f32_to_i32(array)",
		"(DataType::Float32, DataType::Int64) => cast_f32_to_i64(array)",
		"(DataType::Float32, DataType::Float64) => cast_f32_to_f64(array)",
		"(DataType::Float64, DataType::Int32) => cast_f64_to_i32(array)",
		"(DataType::Float64, DataType::Int64) => cast_f64_to_i64(array)",
		"(DataType::Float64, DataType::Float32) => cast_f64_to_f32(array)",
	] {
		assert!(
			evaluate_cast.contains(arm),
			"missing evaluate_cast arm {arm}"
		);
	}

	let int64_to_float32 = brace_blocks_after_marker(&exec, "fn cast_i64_to_f32")
		.into_iter()
		.next()
		.expect("cast_i64_to_f32 body")
		.1;
	assert!(int64_to_float32.contains("v as f32"));
}

#[test]
fn adr025_cast_failure_returns_kernel_error_with_prefix() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let cast_failed = brace_blocks_after_marker(&exec, "fn cast_failed")
		.into_iter()
		.next()
		.expect("cast_failed body")
		.1;

	assert!(cast_failed.contains("ExecutionError::Kernel"));
	assert!(cast_failed.contains("cast failed: {source:?} to {target:?}: {cause}"));
}

#[test]
fn adr026_cast_syntax_doublecolon_and_format_remain_rejected() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_expr = brace_blocks_after_marker(&planner, "fn lower_expr")
		.into_iter()
		.next()
		.expect("lower_expr body")
		.1;

	for rejected in [
		"CastKind::DoubleColon",
		"unsupported cast syntax: ::; only CAST(... AS ...) is supported",
		"CAST FORMAT clause is not supported",
	] {
		assert!(
			lower_expr.contains(rejected),
			"missing syntax guard {rejected}"
		);
	}
	assert!(lower_expr.contains("CastKind::TryCast | CastKind::SafeCast"));
	assert!(lower_expr.contains("Ok(Expr::TryCast"));
	assert!(!lower_expr.contains("unsupported cast syntax: TRY_CAST"));
	assert!(!lower_expr.contains("unsupported cast syntax: SAFE_CAST"));
}

#[test]
fn adr025_float_to_int_uses_exclusive_upper_bound() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(exec.contains("const F64_I32_LOWER: f64 = i32::MIN as f64"));
	assert!(exec.contains("const F64_I32_UPPER: f64 = 2147483648.0"));
	assert!(exec.contains("const F64_I64_LOWER: f64 = i64::MIN as f64"));
	assert!(exec.contains("const F64_I64_UPPER: f64 = 9223372036854775808.0"));

	for (marker, bound) in [
		("fn cast_f32_to_i32", "trunc < F64_I32_UPPER"),
		("fn cast_f32_to_i64", "trunc < F64_I64_UPPER"),
		("fn cast_f64_to_i32", "trunc < F64_I32_UPPER"),
		("fn cast_f64_to_i64", "trunc < F64_I64_UPPER"),
	] {
		let body = brace_blocks_after_marker(&exec, marker)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{marker} body"))
			.1;
		assert!(
			body.contains(bound),
			"{marker} missing exclusive upper bound"
		);
		assert!(
			!body.contains("trunc <= F64_"),
			"{marker} must not use inclusive upper bound"
		);
		assert!(body.contains("NaN has no integer image"));
		assert!(body.contains("Inf has no integer image"));
	}

	assert!(
		brace_blocks_after_marker(&exec, "fn cast_f32_to_i32")
			.into_iter()
			.next()
			.expect("cast_f32_to_i32 body")
			.1
			.contains("f64::from(value).trunc()")
	);
	assert!(
		brace_blocks_after_marker(&exec, "fn cast_f32_to_i64")
			.into_iter()
			.next()
			.expect("cast_f32_to_i64 body")
			.1
			.contains("f64::from(value).trunc()")
	);
}

#[test]
fn adr026_try_cast_ir_surface_is_narrow() {
	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	assert!(logical_source.contains("TryCast(Box<Expr>, DataType)"));
	let logical_pbc = brace_blocks_after_marker(&logical_source, "fn is_pbc_pushdown_eligible")
		.into_iter()
		.next()
		.expect("logical pbc eligibility body")
		.1;
	assert!(logical_pbc.contains("Self::TryCast(_, _)"));
	assert!(logical_pbc.contains("false"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	assert!(execution_source.contains("TryCast {"));
	assert!(execution_source.contains("target: DataType"));
	let nullable = brace_blocks_after_marker(&execution_source, "fn nullable")
		.into_iter()
		.next()
		.or_else(|| {
			brace_blocks_after_marker(&execution_source, "fn node_nullable")
				.into_iter()
				.next()
		})
		.expect("nullable body")
		.1;
	assert!(nullable.contains("BoundExprNode::TryCast { .. } => Some(true)"));
}

#[test]
fn adr026_try_cast_lowers_via_planner() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_expr = brace_blocks_after_marker(&planner, "fn lower_expr")
		.into_iter()
		.next()
		.expect("lower_expr body")
		.1;
	assert!(lower_expr.contains("CastKind::TryCast | CastKind::SafeCast"));
	assert!(lower_expr.contains("Ok(Expr::TryCast"));
	assert!(lower_expr.contains("lower_cast_target_type(data_type)?"));
	assert!(lower_expr.contains("CastKind::DoubleColon"));
	assert!(lower_expr.contains("CAST FORMAT clause is not supported"));
}

#[test]
fn adr026_try_cast_policy_is_separate() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let bind_try_cast = brace_blocks_after_marker(&binder, "fn bind_try_cast")
		.into_iter()
		.next()
		.expect("bind_try_cast body")
		.1;
	assert!(bind_try_cast.contains("is_supported_explicit_numeric_cast"));
	assert!(bind_try_cast.contains("unsupported TRY_CAST/SAFE_CAST"));
	assert!(
		bind_try_cast.contains("builder.append_try_cast(inner, target.clone(), target.clone())")
	);
	assert!(
		!bind_try_cast.contains("if source == *target"),
		"bind_try_cast must not elide identity TryCast"
	);
	assert!(
		!bind_try_cast.contains("if source == target"),
		"bind_try_cast must not elide identity TryCast"
	);
	assert!(binder.contains("BoundExprNode::TryCast { input, .. }"));
	assert!(binder.contains("BoundExprNode::TryCast { input, target }"));
}

#[test]
fn adr026_try_cast_supports_numeric_primitive_lattice() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let evaluate_try_cast = brace_blocks_after_marker(&exec, "fn evaluate_try_cast")
		.into_iter()
		.next()
		.expect("evaluate_try_cast body")
		.1;
	assert!(evaluate_try_cast.contains("if array.data_type() == target"));
	assert!(!evaluate_try_cast.contains("evaluate_cast("));

	for arm in [
		"(DataType::Int32, DataType::Int64) => cast_i32_to_i64(array)",
		"(DataType::Int32, DataType::Float32) => cast_i32_to_f32(array)",
		"(DataType::Int32, DataType::Float64) => cast_i32_to_f64(array)",
		"(DataType::Int64, DataType::Float32) => cast_i64_to_f32(array)",
		"(DataType::Int64, DataType::Float64) => cast_i64_to_f64(array)",
		"(DataType::Float32, DataType::Float64) => cast_f32_to_f64(array)",
		"(DataType::Int64, DataType::Int32) => try_cast_i64_to_i32(array)",
		"(DataType::Float64, DataType::Float32) => try_cast_f64_to_f32(array)",
		"(DataType::Float32, DataType::Int32) => try_cast_f32_to_i32(array)",
		"(DataType::Float32, DataType::Int64) => try_cast_f32_to_i64(array)",
		"(DataType::Float64, DataType::Int32) => try_cast_f64_to_i32(array)",
		"(DataType::Float64, DataType::Int64) => try_cast_f64_to_i64(array)",
	] {
		assert!(
			evaluate_try_cast.contains(arm),
			"missing evaluate_try_cast arm {arm}"
		);
	}
}

#[test]
fn adr026_try_cast_nulls_only_data_domain_failures() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(!exec.contains("catch_unwind"));
	for kernel in [
		"fn try_cast_i64_to_i32",
		"fn try_cast_f64_to_f32",
		"fn try_cast_f32_to_i32",
		"fn try_cast_f32_to_i64",
		"fn try_cast_f64_to_i32",
		"fn try_cast_f64_to_i64",
	] {
		let body = brace_blocks_after_marker(&exec, kernel)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{kernel} body"))
			.1;
		assert!(
			!body.contains("cast_failed("),
			"{kernel} must not emit cast_failed errors"
		);
		assert!(
			!body.contains("ExecutionError::Kernel"),
			"{kernel} must not construct Kernel errors directly"
		);
		assert!(body.contains("return None"), "{kernel} must emit None");
	}

	for (kernel, bound) in [
		("fn try_cast_f32_to_i32", "trunc < F64_I32_UPPER"),
		("fn try_cast_f32_to_i64", "trunc < F64_I64_UPPER"),
		("fn try_cast_f64_to_i32", "trunc < F64_I32_UPPER"),
		("fn try_cast_f64_to_i64", "trunc < F64_I64_UPPER"),
	] {
		let body = brace_blocks_after_marker(&exec, kernel)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{kernel} body"))
			.1;
		assert!(body.contains(bound), "{kernel} missing bound {bound}");
	}
	assert!(
		brace_blocks_after_marker(&exec, "fn try_cast_f64_to_f32")
			.into_iter()
			.next()
			.expect("try_cast_f64_to_f32 body")
			.1
			.contains("value.is_finite() && value.abs() > f32::MAX as f64")
	);
}

#[test]
fn adr026_try_cast_pbc_declines() {
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read PBC source");
	assert!(pbc.contains("BoundExprNode::TryCast { .. }"));
	assert!(pbc.contains("arithmetic, Cast, and TryCast are not PBC-eligible"));
	assert!(!pbc.contains("OP_CAST"));
	assert!(!pbc.contains("OP_TRY_CAST"));
}

#[test]
fn adr026_try_cast_order_by_uses_hidden_sort_projection() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("fn expr_contains_cast_family"));

	let sort_gate = brace_blocks_after_marker(&planner, "fn sort_needs_hidden_projection")
		.into_iter()
		.next()
		.expect("sort_needs_hidden_projection body")
		.1;
	assert!(sort_gate.contains("!sort_key_is_direct_output_reference(&key.expr)"));

	let validator = brace_blocks_after_marker(&planner, "fn validate_hidden_sort_key_refs")
		.into_iter()
		.next()
		.expect("validate_hidden_sort_key_refs body")
		.1;
	assert!(validator.contains("Expr::Cast(inner, _) | Expr::TryCast(inner, _)"));
}

#[test]
fn adr027_hidden_sort_projection_gate_is_generalized() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	let gate = brace_blocks_after_marker(&planner, "fn sort_needs_hidden_projection")
		.into_iter()
		.next()
		.expect("sort_needs_hidden_projection body")
		.1;
	assert!(gate.contains("!sort_key_is_direct_output_reference(&key.expr)"));
	assert!(!gate.contains("expr_contains_cast_family"));
	assert!(!planner.contains("fn expr_contains_cast_family"));
}

#[test]
fn adr027_hidden_sort_key_refs_are_visible_outputs() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	let validator = brace_blocks_after_marker(&planner, "fn validate_hidden_sort_key_refs")
		.into_iter()
		.next()
		.expect("validate_hidden_sort_key_refs body")
		.1;

	assert!(validator.contains("column.qualifier.is_some()"));
	assert!(validator.contains("visible_names.contains(&column.name.0)"));
	assert!(
		validator.contains(
			"ORDER BY computed aggregate expression must reference visible output columns"
		)
	);
	assert!(validator.contains("Expr::Literal(_) => Ok(())"));
	assert!(validator.contains("Expr::Cast(inner, _) | Expr::TryCast(inner, _)"));
	for op in ["Add", "Sub", "Mul", "Div"] {
		assert!(validator.contains(&format!("Expr::{op}(left, right)")));
	}
	for op in ["Eq", "Neq", "Lt", "Lte", "Gt", "Gte", "And", "Or"] {
		assert!(validator.contains(&format!("Expr::{op}(left, right)")));
	}
	for op in ["Not", "IsNull", "IsNotNull"] {
		assert!(validator.contains(&format!("Expr::{op}(inner)")));
	}
	assert!(validator.contains("Expr::TimeBucket(_, inner)"));
}

#[test]
fn adr027_hidden_sort_projection_contract_is_pinned() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	assert!(planner.contains("fn visible_output_names"));
	assert!(planner.contains("fn has_duplicate_names"));
	assert!(planner.contains("fn unique_hidden_sort_name"));

	let mechanism = brace_blocks_after_marker(&planner, "fn add_hidden_sort_projection")
		.into_iter()
		.next()
		.expect("add_hidden_sort_projection body")
		.1;
	assert!(mechanism.contains("validate_hidden_sort_key_refs"));
	assert!(planner.contains("__toron_order_key_"));
	assert!(
		planner.contains("ORDER BY computed aggregate expression requires unique output names")
	);
	assert!(mechanism.contains("FinalProjection"));
}

#[test]
fn adr028_aggregate_arguments_accept_computed_expressions() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("aggregate arguments must be simple column references"));
	assert!(planner.contains("aggregate arguments must not contain aggregate calls"));
	let validator = brace_blocks_after_marker(&planner, "fn validate_aggregate_argument")
		.into_iter()
		.next()
		.expect("validate aggregate argument")
		.1;
	assert!(validator.contains("reject_nested_aggregate"));
}

#[test]
fn adr028_aggregate_signature_uses_planner_private_visitors() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let signature = brace_blocks_after_marker(&planner, "struct GroupedAggregateSignature")
		.into_iter()
		.next()
		.expect("GroupedAggregateSignature body")
		.1;
	assert!(signature.contains("kernel: aggregate_kernel::StableAggregateKernel"));
	assert!(signature.contains("args: Vec<Expr>"));
	assert!(planner.contains("impl PartialEq for GroupedAggregateSignature"));
	assert!(planner.contains("impl Hash for GroupedAggregateSignature"));
	let hash_impl = brace_blocks_after_marker(&planner, "impl Hash for GroupedAggregateSignature")
		.into_iter()
		.next()
		.expect("GroupedAggregateSignature Hash impl")
		.1;
	assert!(hash_impl.contains("self.args.len().hash(state)"));
	for helper in [
		"fn hash_expr_structural",
		"fn expr_structural_eq",
		"fn hash_scalar_structural",
		"fn scalar_structural_eq",
		"fn hash_data_type_structural",
		"fn data_type_structural_eq",
	] {
		assert!(planner.contains(helper), "missing {helper}");
	}
	for forbidden in ["AggregateArgKey", "ScalarKey", "DataTypeKey", "DurationKey"] {
		assert!(!planner.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
fn adr028_aggregate_signature_visitors_are_exhaustive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let hash_body = brace_blocks_after_marker(&planner, "fn hash_expr_structural")
		.into_iter()
		.next()
		.expect("hash expr structural")
		.1;
	let eq_body = brace_blocks_after_marker(&planner, "fn expr_structural_eq")
		.into_iter()
		.next()
		.expect("expr structural eq")
		.1;
	for variant in [
		"Expr::Column",
		"Expr::Literal",
		"Expr::Cast",
		"Expr::TryCast",
		"Expr::Add",
		"Expr::Sub",
		"Expr::Mul",
		"Expr::Div",
		"Expr::Eq",
		"Expr::Neq",
		"Expr::Lt",
		"Expr::Lte",
		"Expr::Gt",
		"Expr::Gte",
		"Expr::And",
		"Expr::Or",
		"Expr::Not",
		"Expr::IsNull",
		"Expr::IsNotNull",
		"Expr::TimeBucket",
	] {
		assert!(
			hash_body.contains(variant),
			"hash visitor missing {variant}"
		);
		assert!(eq_body.contains(variant), "eq visitor missing {variant}");
	}
}

#[test]
fn adr028_aggregate_signature_visitors_preserve_float_bits() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let hash_body = brace_blocks_after_marker(&planner, "fn hash_scalar_structural")
		.into_iter()
		.next()
		.expect("hash scalar structural")
		.1;
	let eq_body = brace_blocks_after_marker(&planner, "fn scalar_structural_eq")
		.into_iter()
		.next()
		.expect("scalar structural eq")
		.1;
	assert!(hash_body.contains("f32::to_bits"));
	assert!(hash_body.contains("f64::to_bits"));
	assert!(eq_body.contains("f32::to_bits"));
	assert!(eq_body.contains("f64::to_bits"));
	for variant in [
		"ScalarValue::Boolean",
		"ScalarValue::Int32",
		"ScalarValue::Int64",
		"ScalarValue::Float32",
		"ScalarValue::Float64",
		"ScalarValue::Utf8",
	] {
		assert!(hash_body.contains(variant), "hash scalar missing {variant}");
		assert!(eq_body.contains(variant), "eq scalar missing {variant}");
	}
}

#[test]
fn adr028_data_type_structural_eq_is_reflexive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let eq_body = brace_blocks_after_marker(&planner, "fn data_type_structural_eq")
		.into_iter()
		.next()
		.expect("data type structural eq")
		.1;
	assert!(eq_body.contains("left == right"));
	assert!(!eq_body.contains("matches!"));
	let hash_body = brace_blocks_after_marker(&planner, "fn hash_data_type_structural")
		.into_iter()
		.next()
		.expect("hash data type structural")
		.1;
	for data_type in [
		"DataType::Int32",
		"DataType::Int64",
		"DataType::Float32",
		"DataType::Float64",
	] {
		assert!(
			hash_body.contains(data_type),
			"hash data type missing {data_type}"
		);
	}
	assert!(hash_body.contains("format!"));
}

#[test]
fn adr028_aggregate_signature_count_all_is_zero_arg() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let signature = brace_blocks_after_marker(&planner, "fn grouped_aggregate_signature")
		.into_iter()
		.next()
		.expect("grouped aggregate signature")
		.1;
	assert!(signature.contains("aggregate_kernel::AggregateArity::Zero"));
	assert!(signature.contains("args: Vec::new()"));
	let lower_count = brace_blocks_after_marker(&planner, "fn lower_count_aggregate")
		.into_iter()
		.next()
		.expect("lower count aggregate")
		.1;
	assert!(lower_count.contains("Expr::column(\"count\")"));
}

#[test]
fn adr028_nested_aggregate_rejects_at_sql_walk() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let walker = brace_blocks_after_marker(&planner, "fn reject_nested_aggregate")
		.into_iter()
		.next()
		.expect("reject nested aggregate")
		.1;
	for arm in [
		"SqlExpr::Function",
		"SqlExpr::BinaryOp",
		"SqlExpr::UnaryOp",
		"SqlExpr::Cast",
		"SqlExpr::IsNull",
		"SqlExpr::IsNotNull",
		"SqlExpr::Nested",
	] {
		assert!(walker.contains(arm), "walker missing {arm}");
	}
	assert!(walker.contains("aggregate_function_name(&function.name).is_some()"));
	assert!(planner.contains("aggregate arguments must not contain aggregate calls"));
}

#[test]
fn adr028_experimental_kernels_excluded_from_grouped_signatures() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	let mapping = brace_blocks_after_marker(&catalog, "fn stable_kernel_for_function")
		.into_iter()
		.next()
		.expect("stable kernel for function")
		.1;
	assert!(mapping.contains(
		"AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => None"
	));
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let signature = brace_blocks_after_marker(&planner, "fn grouped_aggregate_signature")
		.into_iter()
		.next()
		.expect("grouped aggregate signature")
		.1;
	assert!(signature.contains("aggregate_kernel::stable_kernel_for_function(function)?"));
}

#[test]
fn adr028_aggregate_output_names_stay_kernel_names() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let output_names = brace_blocks_after_marker(&planner, "fn aggregate_output_name")
		.into_iter()
		.next()
		.expect("aggregate output name")
		.1;
	assert!(!output_names.contains("format!"));
	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	for name in [
		"output_name: \"sum\"",
		"output_name: \"count\"",
		"output_name: \"min\"",
		"output_name: \"max\"",
		"output_name: \"avg\"",
	] {
		assert!(catalog.contains(name), "missing output name {name}");
	}
}

#[test]
fn adr028_binder_executor_aggregate_args_preserved() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let stable_bind = brace_blocks_after_marker(&binder, "fn bind_stable_aggregate_call")
		.into_iter()
		.next()
		.expect("bind stable aggregate call")
		.1;
	assert!(stable_bind.contains("bind_expr_program(args"));
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(exec.contains("self.evaluate_expr(&args[0]"));
	assert!(exec.contains("self.evaluate_bound_single_root(&call.args"));
}

#[test]
fn adr028_public_ir_remains_non_eq_non_hash() {
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source");
	for type_name in ["AggregateFunction", "Expr", "ScalarValue"] {
		let before_type = ir
			.split(&format!("pub enum {type_name}"))
			.next()
			.expect("type marker");
		let derives = before_type.rsplit("#[derive").next().unwrap_or("");
		let derive_items = derives
			.split(')')
			.next()
			.and_then(|line| line.split('(').nth(1))
			.unwrap_or("");
		let has_forbidden_derive = derive_items
			.split(',')
			.map(str::trim)
			.any(|item| item == "Eq" || item == "Hash");
		assert!(
			!has_forbidden_derive,
			"ADR 028 D2: {type_name} must not derive Eq or Hash"
		);
		assert!(
			!ir.contains(&format!("impl Hash for {type_name}")),
			"ADR 028 D2: {type_name} must not implement Hash"
		);
		assert!(
			!ir.contains(&format!("impl Eq for {type_name}")),
			"ADR 028 D2: {type_name} must not implement Eq"
		);
	}
}

#[test]
fn adr019_min_max_aggregate_kernels_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(planner.contains("\"MIN\" => self.lower_min_aggregate"));
	assert!(planner.contains("\"MAX\" => self.lower_max_aggregate"));
	assert!(planner.contains("MIN requires one argument"));
	assert!(planner.contains("MAX requires one argument"));
	assert!(planner.contains("unsupported aggregate function"));

	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	assert!(binder.contains("expects Int32, Int64, Float32, or Float64 input"));

	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	assert!(catalog.contains("kind: StableAggregateKernel::Min"));
	assert!(catalog.contains("kind: StableAggregateKernel::Max"));
	assert!(catalog.contains("input_domain: AggregateInputDomain::OrderedMinMax"));
	assert!(catalog.contains("output_rule: AggregateOutputRule::SameAsInput"));

	let aggregate_function_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source");
	let aggregate_function = aggregate_function_source
		.split("pub enum AggregateFunction")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("AggregateFunction enum");
	assert!(aggregate_function.contains("Min,"));
	assert!(aggregate_function.contains("Max,"));

	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(exec.contains("AggState::MinInt32"));
	assert!(exec.contains("AggState::MinInt64"));
	assert!(exec.contains("AggState::MinFloat32"));
	assert!(exec.contains("AggState::MinFloat64"));
	assert!(exec.contains("AggState::MaxInt32"));
	assert!(exec.contains("AggState::MaxInt64"));
	assert!(exec.contains("AggState::MaxFloat32"));
	assert!(exec.contains("AggState::MaxFloat64"));
	let before_agg_state = exec.split("enum AggState").next().unwrap_or("");
	let derives_above = before_agg_state.rsplit("#[derive").next().unwrap_or("");
	assert!(derives_above.contains("Copy"));

	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	assert!(!storage.contains("AggregateExpression"));
	assert!(!storage.contains("AggregateFunctionTag"));
}

#[test]
fn adr020_avg_aggregate_kernel_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(planner.contains("\"AVG\" => self.lower_avg_aggregate"));
	assert!(planner.contains("AVG requires one argument"));
	assert!(planner.contains("unsupported aggregate function"));

	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	assert!(binder.contains("AVG expects numeric input"));

	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	assert!(catalog.contains("kind: StableAggregateKernel::Avg"));
	assert!(catalog.contains("kind: StableAggregateKernel::Avg"));
	assert!(catalog.contains("output_name: \"avg\""));
	assert!(catalog.contains("input_domain: AggregateInputDomain::AvgNumeric"));
	assert!(catalog.contains("output_rule: AggregateOutputRule::Float64"));

	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(exec.contains("AggState::AvgInt"));
	assert!(exec.contains("AggState::AvgFloat"));
	assert!(exec.contains("fn avg_state_for_data_type"));
	assert!(exec.contains("fn add_avg_value"));
	assert!(exec.contains("fn avg_finalize"));
	let before_agg_state = exec.split("enum AggState").next().unwrap_or("");
	let derives_above = before_agg_state.rsplit("#[derive").next().unwrap_or("");
	assert!(derives_above.contains("Copy"));

	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	assert!(!storage.contains("AggregateExpression"));
	assert!(!storage.contains("AggregateFunctionTag"));
}

fn enum_variant_count(enum_body: &str) -> usize {
	enum_body
		.lines()
		.filter(|line| {
			let trimmed = line.trim_start();
			trimmed
				.chars()
				.next()
				.is_some_and(|first| first.is_ascii_uppercase())
				&& (trimmed.ends_with(',') || trimmed.ends_with('{'))
		})
		.count()
}

fn derive_items_before(source: &str, marker: &str) -> Vec<String> {
	let before = source
		.split(marker)
		.next()
		.unwrap_or_else(|| panic!("missing marker {marker}"));
	let derives = before.rsplit("#[derive").next().unwrap_or("");
	derives
		.split(')')
		.next()
		.and_then(|line| line.split('(').nth(1))
		.unwrap_or("")
		.split(',')
		.map(str::trim)
		.filter(|item| !item.is_empty())
		.map(ToOwned::to_owned)
		.collect()
}

#[test]
fn wire_contract_has_no_legacy_toron_message_wrapper() {
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
fn wire_envelope_requires_routable_identity() {
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
fn server_and_distributed_paths_have_no_sleep_mock() {
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
fn shuffle_surface_has_no_push_to_peer_path() {
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
fn worker_runtime_does_not_bind_logical_plans() {
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
fn runtime_transport_is_not_duplex_only() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("tokio::io::duplex") || line.contains("MockZeroCopyTransport")
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"duplex-only transport marker found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
fn scheduler_dispatch_loop_uses_acceptance_barrier() {
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
fn g16_no_non_pinned_join_algorithms_in_runtime_paths() {
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
fn g17_join_ir_surface_has_single_join_variant_and_two_strategies() {
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
fn g18_scheduler_uses_multi_upstream_stage_barrier() {
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
fn g19_join_hash_spill_uses_m10_async_file_ops() {
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
fn broadcast_join_fragmenter_input_shape_is_architecture_guarded() {
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
fn adr029_sql_planner_accepts_only_select_and_create_table() {
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
		"unsupported SQL statement; only SELECT, CREATE TABLE, INSERT, DELETE, and DROP TABLE are supported"
	));
	assert!(!plan_body.contains("only SELECT and CREATE TABLE are supported"));
	assert!(!plan_body.contains("only SELECT is supported"));
}

#[test]
fn adr029_create_table_destructure_is_exhaustive() {
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
		"primary_key",
		"partition_by",
		"with_tags",
	] {
		assert!(body.contains(marker), "lower_create_table missing {marker}");
	}
	assert!(!body.contains(".."));
}

#[test]
fn adr029_create_table_ir_surface_is_narrow() {
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
fn adr029_create_table_name_lowering_is_strict() {
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
fn adr029_create_table_type_lattice_is_ddl_specific() {
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
fn adr029_create_table_column_options_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table_column_nullability")
		.into_iter()
		.next()
		.expect("lower_create_table_column_nullability body")
		.1;

	for marker in [
		"ColumnOption::Null => true",
		"ColumnOption::NotNull => false",
		"ColumnOption::Default",
		"ColumnOption::Unique",
		"ColumnOption::Check",
		"ColumnOption::ForeignKey",
		"ColumnOption::Generated",
		"named constraint",
	] {
		assert!(
			body.contains(marker),
			"DDL column option guard missing {marker}"
		);
	}
}

#[test]
fn adr029_create_table_rejects_storage_synthetic_column_names() {
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
fn adr029_catalog_create_table_returns_binding() {
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
fn adr029_mock_catalog_is_interior_mutable() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(catalog.contains("inner: RwLock<MockCatalogInner>"));
	assert!(catalog.contains("next_table_id: u64"));
	assert!(catalog.contains("self.inner.get_mut()"));
	assert!(catalog.contains("bump_next_table_id(inner, legacy_table_id)"));
}

#[test]
fn adr029_mock_catalog_identity_uses_descriptor_fingerprint() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let body = brace_blocks_after_marker(&catalog, "fn mock_table_identity")
		.into_iter()
		.next()
		.expect("mock_table_identity body")
		.1;

	assert!(body.contains("TableObjectId::new"));
	assert!(body.contains("TableSchemaDescriptor::new_with_layout"));
	assert!(body.contains("physical_descriptor_fingerprint"));
	assert!(!catalog.contains("PhysicalDescriptorFingerprint::from_bytes"));
}

#[test]
fn adr029_mock_catalog_reference_insertion_does_not_overwrite() {
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

	assert!(create_body.contains("inner.by_ref.insert(name.clone(), legacy_table_id)"));
	assert!(create_body.contains(".entry(TableRef::new(name.table.clone()))"));
	assert!(create_body.contains(".or_insert(legacy_table_id)"));
	assert_eq!(create_body.matches("by_ref.insert").count(), 1);
}

#[test]
fn adr029_wendelon_catalog_create_table_has_admin_boundary() {
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
fn adr029_coordinators_intercept_create_table() {
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
	assert!(query_impl.contains("self.catalog.as_ref()"));
	assert!(query_impl.contains("self.admin.as_ref()"));

	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let distributed_branch = distributed_impl
		.find("ExecutionPlan::CreateTable")
		.expect("distributed DDL branch");
	let fragment_call = distributed_impl
		.find("fragment_plan(")
		.expect("fragmenter call");
	assert!(distributed_branch < fragment_call);
	assert!(distributed_impl.contains("execute_catalog_plan("));
	assert!(distributed_impl.contains("self.catalog.as_ref()"));
	assert!(distributed_impl.contains("self.admin.as_ref()"));

	let helper_body = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper body")
		.1;
	assert!(helper_body.contains(".create_table(name, schema.clone(), *if_not_exists)"));
	assert!(helper_body.contains("Ok(Vec::new())"));
	assert_eq!(
		coordinator
			.matches("create_table(name, schema.clone(), *if_not_exists)")
			.count(),
		1
	);
}

#[test]
fn adr029_executor_and_fragmenter_reject_create_table() {
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
fn adr030_insert_destructure_is_exhaustive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_insert(")
		.into_iter()
		.next()
		.expect("lower_insert body")
		.1;

	for marker in [
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
		"replace_into",
		"priority",
		"insert_alias",
		"settings",
		"format_clause",
	] {
		assert!(body.contains(marker), "lower_insert missing {marker}");
	}
	assert!(body.contains("missing INTO keyword"));
	assert!(body.contains("TABLE keyword"));
	assert!(!body.contains(".. }"));
	assert!(!body.contains("..,"));
}

#[test]
fn adr030_logical_plan_insert_uses_insert_literal() {
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
fn adr030_execution_plan_insert_has_no_record_batch() {
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
fn adr030_catalog_has_no_insert_method() {
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
fn adr030_no_storage_synthetic_column_construction_in_toron() {
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
fn adr030_no_blake3_in_toron_insert_path() {
	let cargo =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read toron Cargo.toml");
	let coordinator_write =
		std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
			.expect("read coordinator/write.rs");

	assert!(!cargo.contains("blake3"));
	assert!(!coordinator_write.contains("blake3"));
}

#[test]
fn adr030_coordinator_insert_uses_wendelon_client_wrapper() {
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
fn adr030_write_options_set_await_finalization_true() {
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
fn adr030_wendelon_storage_write_mirrors_scan_error_mapping() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let write_impl =
		brace_blocks_after_marker(&storage, "impl StorageWrite for WendelonStorageWrite")
			.into_iter()
			.next()
			.expect("WendelonStorageWrite impl")
			.1;

	assert!(write_impl.contains("descriptor_registry.descriptor(request.table_id)?"));
	assert!(write_impl.contains("StorageError::DescriptorEvolved"));
	assert!(write_impl.contains("StorageError::Internal"));
	assert!(storage.contains("StorageError::TableNotFound"));
}

#[test]
fn adr030_local_coordinator_intercepts_insert_before_retry_loop() {
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
	assert!(query_impl.contains("execute_write_plan(&bound).await"));
	assert!(query_impl.contains("writer: Arc<dyn StorageWrite>"));
}

#[test]
fn adr030_distributed_coordinator_routes_insert_before_fragment_plan() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let insert_route = distributed_impl
		.find("ExecutionPlan::Insert")
		.expect("distributed INSERT route");
	let fragment_call = distributed_impl
		.find("fragment_plan(")
		.expect("fragment plan call");

	assert!(insert_route < fragment_call);
	assert!(distributed_impl.contains("execute_write_plan_with_source"));
	assert!(!distributed_impl.contains("distributed INSERT execution is not wired"));
}

#[test]
fn adr030_executor_and_fragmenter_reject_insert() {
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
fn adr032_logical_plan_retains_insert_select_variant() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert_eq!(enum_variant_count(logical_plan), 15);
	assert!(logical_plan.contains("InsertSelect {"));
	assert!(logical_plan.contains("DropTable {"));
	assert!(logical_plan.contains("table: TableRef,"));
	assert!(logical_plan.contains("columns: Option<Vec<String>>,"));
	assert!(logical_plan.contains("source: Box<LogicalPlan>,"));
}

#[test]
fn adr032_execution_plan_retains_insert_select_variant() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	assert_eq!(enum_variant_count(execution_plan), 13);
	assert!(execution_plan.contains("InsertSelect {"));
	assert!(execution_plan.contains("DropTable {"));
}

#[test]
fn adr032_insert_stays_values_only() {
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
fn adr032_insert_select_carries_target_columns() {
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
fn adr032_insert_select_no_direct_bound_expr_program() {
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
fn adr032_storage_write_trait_unchanged() {
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
fn adr032_sql_planner_splits_values_and_select_sources() {
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
fn adr032_binder_enforces_source_target_arity() {
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
fn adr032_local_coordinator_intercepts_before_retry() {
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
fn adr032_distributed_coordinator_routes_before_fragment() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let insert_select_route = distributed_impl
		.find("ExecutionPlan::InsertSelect")
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
fn adr032_fragmenter_rejects_before_join_or_exchange() {
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
fn adr032_distributed_walkers_treat_insert_select_as_leaf() {
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
fn adr032_coordinator_uses_only_evaluate_cast() {
	let coordinator_write =
		std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
			.expect("read coordinator/write.rs");
	let materialize = brace_blocks_after_marker(
		&coordinator_write,
		"pub(super) fn materialize_insert_select_batch",
	)
	.into_iter()
	.next()
	.expect("materialize_insert_select_batch body")
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
fn adr032_insert_select_does_not_add_plan_arena_dependency() {
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
fn adr033_logical_variant_count_is_13() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert_eq!(enum_variant_count(logical_plan), 15);
	assert!(logical_plan.contains("CreateTableAsSelect {"));
	assert!(logical_plan.contains("table: TableRef,"));
	assert!(logical_plan.contains("source: Box<LogicalPlan>,"));
	assert!(logical_plan.contains("DropTable {"));
}

#[test]
fn adr033_execution_variant_count_is_11() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");

	assert_eq!(enum_variant_count(execution_plan), 13);
	assert!(execution_plan.contains("CreateTableAsSelect {"));
	assert!(execution_plan.contains("DropTable {"));
}

#[test]
fn adr033_execution_ctas_has_no_precreate_identity() {
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
fn adr033_planner_routes_only_query_create_table_to_ctas() {
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
fn adr033_planner_rejects_ctas_column_list_and_if_not_exists() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_ctas = brace_blocks_after_marker(&planner, "fn lower_create_table_as_select")
		.into_iter()
		.next()
		.expect("lower_create_table_as_select body")
		.1;

	assert!(lower_ctas.contains("if *if_not_exists"));
	assert!(lower_ctas.contains("CTAS does not support IF NOT EXISTS in v1"));
	assert!(lower_ctas.contains("if !columns.is_empty()"));
	assert!(lower_ctas.contains("CTAS does not support explicit column lists in v1"));
}

#[test]
fn adr033_binder_binds_source_before_inferring_schema() {
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
fn adr033_schema_inference_rejects_synthetic_names() {
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
fn adr033_schema_inference_rejects_duplicate_names() {
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
fn adr033_schema_inference_uses_adr029_type_lattice() {
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
fn adr033_local_coordinator_intercepts_before_retry() {
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
	assert!(query_impl.contains("execute_write_plan(&bound).await"));
}

#[test]
fn adr033_coordinator_reads_source_before_create_table() {
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
	let create_table = ctas_arm.find(".create_table(").expect("create_table call");
	let execute_source = ctas_arm
		.find("source_executor.execute_read_plan(source)")
		.expect("source execution");

	assert!(execute_source < create_table);
}

#[test]
fn adr033_coordinator_uses_create_table_binding_for_write() {
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

	assert!(ctas_arm.contains("let binding = catalog.create_table"));
	assert!(ctas_arm.contains("table_id: binding.legacy_table_id"));
	assert!(ctas_arm.contains("table_object_id: binding.table"));
	assert!(ctas_arm.contains("physical_descriptor_fingerprint: binding.fingerprint"));
}

#[test]
fn adr033_empty_source_ctas_writes_zero_row_batch() {
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
fn adr033_distributed_coordinator_routes_before_fragment() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let ctas_route = distributed_impl
		.find("ExecutionPlan::CreateTableAsSelect")
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
fn adr033_fragmenter_rejects_before_join_or_exchange() {
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
fn adr033_distributed_walkers_treat_ctas_as_leaf() {
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
fn adr033_does_not_modify_insert_select_empty_contract() {
	let coordinator_write =
		std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
			.expect("read coordinator/write.rs");
	let materialize = brace_blocks_after_marker(
		&coordinator_write,
		"pub(super) fn materialize_insert_select_batch",
	)
	.into_iter()
	.next()
	.expect("materialize_insert_select_batch body")
	.1;

	assert!(materialize.contains("return Ok(None);"));
}

#[test]
fn adr033_does_not_add_plan_arena_dependency() {
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
fn adr034_sql_planner_accepts_drop_table() {
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
fn adr034_drop_table_destructure_is_exhaustive() {
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
fn adr034_drop_table_reject_surface_is_narrow() {
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
fn adr034_drop_table_name_lowering_is_ddl_specific() {
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
fn adr034_logical_variant_count_is_14() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert_eq!(enum_variant_count(logical_plan), 15);
	assert!(logical_plan.contains("DropTable {"));
	assert!(logical_plan.contains("name: TableRef,"));
	assert!(logical_plan.contains("if_exists: bool,"));
}

#[test]
fn adr034_execution_variant_count_is_12() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");

	assert_eq!(enum_variant_count(execution_plan), 13);
	assert!(execution_plan.contains("DropTable {"));
}

#[test]
fn adr034_execution_drop_table_has_no_identity_or_box() {
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
fn adr034_binder_drop_table_is_pass_through() {
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
fn adr034_catalog_drop_table_returns_optional_binding() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let trait_block = catalog
		.split("pub trait Catalog")
		.nth(1)
		.and_then(|rest| rest.split("}").next())
		.expect("Catalog trait");

	assert!(trait_block.contains("async fn drop_table("));
	assert!(trait_block.contains(") -> Result<Option<TableBinding>, CatalogError>;"));
}

#[test]
fn adr034_mock_catalog_drop_table_cleans_aliases() {
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

	assert!(drop_body.contains(".remove(&table_id)"));
	assert!(drop_body.contains("inner.by_ref.retain"));
	assert!(drop_body.contains("*id != table_id"));
	assert!(!drop_body.contains("by_ref.insert"));
	assert!(!drop_body.contains("or_insert"));
}

#[test]
fn adr034_wendelon_catalog_drop_table_is_admin_unavailable() {
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

	assert!(drop_body.contains("CatalogError::AdminUnavailable"));
	assert!(drop_body.contains("DROP TABLE is not wired to Wendelon catalog administration"));
	assert!(drop_body.contains("if_exists"));
	assert!(drop_body.contains("Ok(None)"));
	assert!(drop_body.contains("CatalogError::TableNotFound"));
}

#[test]
fn adr034_coordinators_intercept_drop_table() {
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
		.find("ExecutionPlan::DropTable")
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
	assert!(helper.contains("ExecutionPlan::DropTable"));
	assert!(helper.contains(".drop_table(name, false)"));
	assert!(helper.contains("Ok(Vec::new())"));
}

#[test]
fn adr034_executor_and_fragmenter_reject_drop_table() {
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
fn adr034_distributed_walkers_treat_drop_table_as_leaf() {
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
fn adr034_operator_label_is_drop_table() {
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
fn adr034_does_not_add_storage_reclaim_or_plan_arena() {
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
	for forbidden in [
		"blake3",
		"bumpalo",
		"typed-arena",
		"generational-arena",
		"slab",
	] {
		assert!(
			!cargo.contains(forbidden),
			"unexpected dependency {forbidden}"
		);
	}
}

#[test]
fn adr034_catalog_trait_grows_by_exactly_one_method() {
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
fn adr036_sql_planner_accepts_delete() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	assert!(planner.contains("Statement::Delete(delete) => self.lower_delete(delete)"));
	assert!(planner.contains("SELECT, CREATE TABLE, INSERT, DELETE, and DROP TABLE"));
}

#[test]
fn adr036_delete_destructure_is_exhaustive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_delete = brace_blocks_after_marker(&planner, "fn lower_delete")
		.into_iter()
		.next()
		.expect("lower_delete body")
		.1;

	for field in [
		"tables",
		"from",
		"using",
		"selection",
		"returning",
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
fn adr036_delete_target_table_destructure_is_exhaustive() {
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
fn adr036_delete_reject_surface_is_narrow() {
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
		"unsupported DELETE clause: RETURNING",
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
}

#[test]
fn adr036_logical_variant_count_is_15() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");

	assert_eq!(enum_variant_count(logical_plan), 15);
	assert!(logical_plan.contains("Delete {"));
	assert!(logical_plan.contains("table: TableRef,"));
	assert!(logical_plan.contains("predicate: Option<Expr>,"));
}

#[test]
fn adr036_execution_variant_count_is_13() {
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");

	assert_eq!(enum_variant_count(execution_plan), 13);
	assert!(execution_plan.contains("Delete {"));
}

#[test]
fn adr036_ir_delete_variants_are_leafs() {
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
fn adr036_execution_delete_carries_shape_a_identity() {
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
fn adr036_delete_has_no_filter_disposition_field() {
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
fn adr036_binder_delete_uses_boolean_root_validator() {
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
fn adr036_binder_delete_allows_residual_predicates() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let bind_delete = brace_blocks_after_marker(&binder, "async fn bind_delete")
		.into_iter()
		.next()
		.expect("bind_delete body")
		.1;

	assert!(bind_delete.contains("bound_predicate.as_ref().and_then(compile_pbc)"));
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
fn adr036_delete_candidate_scan_uses_append_only() {
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
fn adr036_delete_candidate_scan_uses_no_runtime_filter() {
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
fn adr036_delete_residual_filter_before_identity_extraction() {
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
fn adr036_adr035_materializer_name_preserved() {
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
fn adr036_delete_uses_execute_delete_with_candidates() {
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
fn adr036_delete_result_uses_deleted_only() {
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
fn adr036_distributed_delete_routes_before_fragment() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let distributed_impl = coordinator
		.split("impl<Registry, Transport> DistributedQueryCoordinator")
		.nth(1)
		.expect("DistributedQueryCoordinator impl");
	let delete_route = distributed_impl
		.find("ExecutionPlan::Delete")
		.expect("distributed DELETE route");
	let fragment_call = distributed_impl
		.find("fragment_plan(")
		.expect("fragment_plan call");

	assert!(delete_route < fragment_call);
	assert!(distributed_impl.contains("execute_write_plan_with_source"));
	assert!(!distributed_impl.contains("distributed DELETE execution is not wired"));
}

#[test]
fn adr036_fragmenter_delete_defensive_reject() {
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
fn adr036_executor_delete_defensive_arms() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");

	assert!(exec.contains("unbound DELETE execution"));
	assert!(exec.contains("DELETE execution is coordinator-only"));
}

#[test]
fn adr036_operator_label_is_delete() {
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
fn adr036_synthetic_identity_still_not_sql_visible() {
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
fn adr036_filter_helper_preserves_execution_plan_filter() {
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
fn adr037_task_execution_context_has_exact_field_set() {
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
fn adr037_task_spec_carries_task_execution_context() {
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
fn adr037_distributed_context_module_is_wired() {
	let module =
		std::fs::read_to_string(manifest_dir().join("src/distributed/mod.rs")).expect("mod");

	assert!(module.contains("pub mod context;"));
}

#[test]
fn adr037_fragment_config_carries_task_context() {
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
fn adr037_fragmenter_stamps_every_task_spec_with_context() {
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
fn adr037_worker_context_comes_from_task_spec_not_local_test() {
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
	assert!(execute_task.contains("task_context.trace_context = spec.context.trace_context"));
	assert!(!execute_task.contains("ExecutionContext::local_test()"));
}

#[test]
fn adr037_task_context_derives_from_execution_context_without_query_id() {
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
fn adr037_distributed_coordinator_is_context_and_sink_bearing() {
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
fn adr037_distributed_coordinator_exposes_write_sink_constructors() {
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
fn adr037_distributed_dml_routes_to_shared_driver() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let execute_query = brace_blocks_after_marker(&coordinator, "pub async fn execute_query")
		.into_iter()
		.find(|(_, block)| block.contains("DistributedMutationSourceExecutor"))
		.expect("distributed execute_query body")
		.1;

	for variant in [
		"ExecutionPlan::Insert",
		"ExecutionPlan::InsertSelect",
		"ExecutionPlan::CreateTableAsSelect",
		"ExecutionPlan::Delete",
	] {
		assert!(execute_query.contains(variant), "missing {variant}");
	}
	assert!(execute_query.contains("DistributedMutationSourceExecutor"));
	assert!(execute_query.contains("execute_write_plan_with_source"));
}

#[test]
fn adr037_distributed_read_executor_wraps_fragment_scheduler() {
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

	assert!(distributed_source.contains("execute_bound_distributed(plan.clone())"));
	assert!(distributed_source.contains(".await"));
}

#[test]
fn adr037_distributed_fragment_config_uses_execution_context() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let fragment_config = brace_blocks_after_marker(&coordinator, "fn fragment_config")
		.into_iter()
		.next()
		.expect("fragment_config body")
		.1;

	assert!(fragment_config.contains("self.context.query_id"));
	assert!(fragment_config.contains("TaskExecutionContext::from_execution_context"));
	assert!(fragment_config.contains("TaskBudget::from_query_budget(self.query_budget)"));
}

#[test]
fn adr037_mutation_source_executor_is_async_trait() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("#[async_trait::async_trait]\ntrait MutationSourceExecutor"));
	assert!(coordinator.contains("async fn execute_read_plan"));
	assert!(coordinator.contains("async fn execute_delete_candidate_plan"));
}

#[test]
fn adr037_local_mutation_source_wraps_executor() {
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
fn adr037_local_delete_candidate_execution_clears_runtime_filter() {
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
fn adr037_shared_driver_handles_all_mutation_variants() {
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
fn adr037_insert_values_does_not_execute_source_plan() {
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
fn adr037_ctas_reads_source_before_catalog_create() {
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
		.find("catalog.create_table")
		.expect("catalog create");

	assert!(read < create);
}

#[test]
fn adr037_delete_candidate_plan_sets_row_identity_append() {
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
fn adr037_delete_candidate_plan_wraps_residual_filter() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let derive = brace_blocks_after_marker(&coordinator, "fn derive_delete_candidate_plan")
		.into_iter()
		.next()
		.expect("delete candidate builder")
		.1;

	assert!(derive.contains("if let (Some(predicate), None)"));
	assert!(derive.contains("ExecutionPlan::Filter"));
	assert!(derive.contains("FilterDisposition::EvaluateInToron"));
}

#[test]
fn adr037_execution_scan_carries_row_identity_field() {
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
fn adr037_scan_schema_appends_identity_only_when_requested() {
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
fn adr037_binder_default_scans_use_no_row_identity() {
	let binder = std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("binder");
	let scan_plan = brace_blocks_after_marker(&binder, "fn scan_plan")
		.into_iter()
		.next()
		.expect("scan_plan body")
		.1;

	assert!(scan_plan.contains("row_identity: crate::storage::RowIdentityProjection::None"));
}

#[test]
fn adr037_executor_threads_scan_row_identity_to_storage() {
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
fn adr037_shuffle_input_scans_do_not_request_row_identity() {
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
fn adr037_fragmenter_still_rejects_raw_mutation_plans() {
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
fn adr037_old_distributed_dml_reject_strings_are_removed() {
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
fn adr037_functional_fixtures_cover_distributed_write_context() {
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
fn adr035_scan_request_uses_row_identity_enum() {
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
fn adr035_no_include_row_key_id_bool() {
	for (path, line, text) in source_lines(&manifest_dir().join("src")) {
		assert!(
			!text.contains("include_row_key_id: bool"),
			"ADR 035 must use RowIdentityProjection, not a bool: {}:{line}",
			path.display()
		);
	}
}

#[test]
fn adr035_delete_request_uses_vec_identity() {
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
fn adr035_delete_request_has_no_record_batch_payload() {
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
fn adr035_delete_outcome_preserves_four_counters() {
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
fn adr035_storage_delete_rows_trait_is_separate() {
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
fn adr035_storage_write_trait_unchanged() {
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
fn adr035_no_blake3_or_hash_identity_generation_in_toron() {
	let manifest_dir = manifest_dir();
	let sources = [
		manifest_dir.join("src/storage.rs"),
		manifest_dir.join("src/coordinator/delete.rs"),
		manifest_dir.join("Cargo.toml"),
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
fn adr035_in_memory_identity_layout_is_prefix_suffix() {
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
fn adr035_in_memory_storage_tracks_row_ids_and_tombstones() {
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
fn adr035_identity_scan_rejects_user_projection() {
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
fn adr035_coordinator_delete_module_exists() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let delete = std::fs::read_to_string(manifest_dir().join("src/coordinator/delete.rs"))
		.expect("read delete coordinator source");

	assert!(coordinator.contains("mod delete;"));
	assert!(delete.contains("materialize_delete_row_key_ids"));
}

#[test]
fn adr035_materializer_validates_identity_column_shape() {
	let delete = std::fs::read_to_string(manifest_dir().join("src/coordinator/delete.rs"))
		.expect("read delete coordinator source");

	assert!(delete.contains("STORAGE_ROW_KEY_ID_COLUMN"));
	assert!(delete.contains("DataType::FixedSizeBinary(32)"));
	assert!(delete.contains("null_count() != 0"));
}

#[test]
fn adr035_select_scan_uses_row_identity_none() {
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
fn adr035_wendelon_adapter_calls_table_delete() {
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
fn adr038_storage_admin_trait_has_one_method() {
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
fn adr038_drop_table_storage_request_shape_is_context_only() {
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
fn adr038_execution_drop_table_remains_identity_free() {
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
fn adr038_coordinator_resolves_drop_identity_at_execution() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let helper = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper body")
		.1;

	assert!(helper.contains("catalog.resolve_table(name).await"));
	assert!(helper.contains("DropTableStorageRequest"));
	assert!(
		helper.find("catalog.resolve_table(name).await").unwrap()
			< helper.find(".drop_table_storage").unwrap()
	);
}

#[test]
fn adr038_in_memory_storage_admin_removes_table_entry() {
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
fn adr038_drop_table_storage_first_catalog_second() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let helper = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper body")
		.1;

	assert!(
		helper.find(".drop_table_storage").unwrap()
			< helper.find("catalog.drop_table(name, false)").unwrap()
	);
}

#[test]
fn adr038_fragmenter_drop_table_defensive_reject_remains() {
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
fn adr038_wendelon_descriptor_registry_shared_by_all_adapters() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(catalog.contains("pub struct WendelonDescriptorRegistry"));
	assert!(
		catalog.contains("descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptor>")
	);
	for adapter in [
		"pub struct WendelonStorageScan",
		"pub struct WendelonStorageWrite",
		"pub struct WendelonStorageDeleteRows",
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
fn adr038_wendelon_create_table_admin_unavailable_removed() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(!create_body.contains("CatalogError::AdminUnavailable"));
	assert!(!create_body.contains("CREATE TABLE is not wired"));
}

#[test]
fn adr038_wendelon_create_uses_table_schema_update_request_create() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(create_body.contains("TableSchemaUpdateRequest::create"));
	assert!(create_body.contains("submit_table_schema_update"));
}

#[test]
fn adr038_wendelon_create_uses_uuid_object_id_not_fingerprint() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(create_body.contains("Uuid::new_v4"));
	for forbidden in ["blake3", "xxh3", "Sha2", "Sha3"] {
		assert!(!create_body.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
fn adr038_drop_route_uses_storage_admin_sink_when_configured() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("admin: Arc<dyn StorageAdmin>"));
	assert!(coordinator.contains("self.admin.as_ref()"));
	assert!(coordinator.contains(".drop_table_storage"));
}

#[test]
fn adr038_wendelon_drop_remains_admin_unavailable_until_adr_039() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let drop_body = wendelon_catalog_method_body(&catalog, "async fn drop_table");

	assert!(drop_body.contains("CatalogError::AdminUnavailable"));
	assert!(drop_body.contains("DROP TABLE is not wired to Wendelon catalog administration"));
}

#[test]
fn adr038_strict_admin_sink_default_is_unsupported_storage_admin() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("UnsupportedStorageAdmin"));
	assert!(coordinator.contains("Arc::new(UnsupportedStorageAdmin)"));
	assert!(coordinator.contains("pub fn with_storage_admin("));
	assert!(coordinator.contains("pub fn with_writer_deleter_and_admin("));
}

#[test]
fn adr038_no_background_gc_or_scheduler() {
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
	let helper = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper")
		.1;

	for body in [storage_admin, registry, helper] {
		assert!(!body.contains("tokio::spawn"));
		assert!(!body.contains("std::thread::spawn"));
	}
}

#[test]
fn adr038_no_create_or_replace_sql_surface() {
	let manifest_dir = manifest_dir();
	for path in [
		manifest_dir.join("src/ir.rs"),
		manifest_dir.join("src/ir/execution.rs"),
		manifest_dir.join("src/sql_planner.rs"),
		manifest_dir.join("src/binder.rs"),
	] {
		let source = std::fs::read_to_string(&path).expect("read source");
		assert!(!source.contains("CreateOrReplaceTable"));
		assert!(!source.contains("CREATE OR REPLACE"));
	}
}

#[test]
fn adr038_no_write_receipt_or_read_your_writes_fields() {
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
fn adr038_no_blake3_or_name_hash_object_ids() {
	let cargo =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Cargo.toml");
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(!cargo.contains("blake3"));
	for forbidden in ["blake3", "Sha2", "Sha3", "xxh3"] {
		assert!(!catalog.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
fn adr038_no_table_object_id_reuse() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(create_body.contains("TableObjectId::new(*Uuid::new_v4().as_bytes())"));
	assert!(!create_body.contains("existing.table"));
	assert!(!create_body.contains("descriptor.table_object_id"));
}

#[test]
fn adr038_in_memory_admin_validates_object_id_and_fingerprint() {
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
fn adr038_wendelon_create_uses_append_only_row_identity_policy() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(create_body.contains("RowIdentityPolicy::AppendOnly"));
	assert!(!create_body.contains("RowIdentityPolicy::PrimaryKey"));
	assert!(!create_body.contains("RowIdentityPolicy::ByOrdinal"));
}

#[test]
fn adr038_no_separate_catalog_cache_after_admin() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	for forbidden in ["invalidate_table", "refresh_catalog", "evict_descriptor"] {
		assert!(
			!catalog.contains(forbidden),
			"unexpected helper {forbidden}"
		);
	}
}

#[test]
fn adr038_drop_does_not_invalidate_bound_plans() {
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
fn adr038_catalog_drop_table_only_called_from_routing_helper() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let helper = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper")
		.1;

	assert_eq!(coordinator.matches(".drop_table(").count(), 1);
	assert!(helper.contains("catalog.drop_table(name, false).await"));
}

#[test]
fn adr035_wendelon_projection_change_is_privileged() {
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
fn adr035_synthetic_names_remain_rejected_in_sql() {
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
fn adr029_select_retry_loop_is_preserved() {
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

	assert!(
		loop_body.contains("let bound = bind(plan, self.catalog.as_ref(), &self.config).await?")
	);
}

struct RecordingStorageScan {
	batch: RecordBatch,
	observations: Arc<Mutex<Vec<bool>>>,
}

#[async_trait::async_trait]
impl StorageScan for RecordingStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		self.observations
			.lock()
			.unwrap()
			.push(request.pbc_predicate.is_some());
		Ok(ScanStream::from_batches(
			self.batch.schema(),
			request.session_id,
			request.query_id,
			vec![self.batch.clone()],
		))
	}
}

fn wendelon_catalog_method_body(source: &str, method: &str) -> String {
	let impl_block = source
		.split("impl Catalog for WendelonCatalog")
		.nth(1)
		.expect("WendelonCatalog Catalog impl");
	brace_blocks_after_marker(impl_block, method)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("{method} body"))
		.1
}

fn source_lines(root: &Path) -> Vec<(PathBuf, usize, String)> {
	let files = source_files(root);
	files
		.into_iter()
		.flat_map(|file| {
			std::fs::read_to_string(&file)
				.expect("read source")
				.lines()
				.enumerate()
				.map(|(idx, line)| (file.clone(), idx + 1, line.to_string()))
				.collect::<Vec<_>>()
		})
		.collect()
}

fn source_files(root: &Path) -> Vec<PathBuf> {
	let mut files = Vec::new();
	collect_rs_files(root, &mut files);
	files
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
	for entry in std::fs::read_dir(dir).expect("read source dir") {
		let entry = entry.expect("read dir entry");
		let path = entry.path();
		if path.is_dir() {
			collect_rs_files(&path, out);
		} else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
			out.push(path);
		}
	}
}

fn brace_blocks_after_marker(source: &str, marker: &str) -> Vec<(usize, String)> {
	let mut blocks = Vec::new();
	let mut search_start = 0usize;
	while let Some(offset) = source[search_start..].find(marker) {
		let start = search_start + offset;
		let line = source[..start].matches('\n').count() + 1;
		let mut depth = 0usize;
		let mut saw_open = false;
		let mut end = None;
		for (relative_idx, ch) in source[start..].char_indices() {
			match ch {
				'{' => {
					saw_open = true;
					depth += 1;
				}
				'}' if saw_open => {
					depth = depth.saturating_sub(1);
					if depth == 0 {
						end = Some(start + relative_idx + ch.len_utf8());
						break;
					}
				}
				_ => {}
			}
		}
		if let Some(end) = end {
			blocks.push((line, source[start..end].to_string()));
			search_start = end;
		} else {
			blocks.push((line, source[start..].to_string()));
			break;
		}
	}
	blocks
}

fn format_offenders(manifest_dir: &Path, offenders: &[(PathBuf, usize, String)]) -> String {
	offenders
		.iter()
		.map(|(file, line, source)| {
			format!(
				"{}:{line}: {}",
				file.strip_prefix(manifest_dir).unwrap_or(file).display(),
				source.trim()
			)
		})
		.collect::<Vec<_>>()
		.join("\n")
}

fn manifest_dir() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn wendelon_manifest_dir() -> PathBuf {
	manifest_dir()
		.parent()
		.expect("crates directory")
		.join("wendelon")
}

fn descriptor() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("ts", DataType::Int64, true),
	]);
	TableSchemaDescriptor::new(
		TableObjectId::new([0xA2; 16]),
		1,
		"architecture",
		"sales",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}

fn batch() -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("ts", DataType::Int64, true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3])),
			Arc::new(Int32Array::from(vec![Some(10), Some(150), Some(200)])),
			Arc::new(Int64Array::from(vec![1_000, 2_000, 3_000])),
		],
	)
	.expect("batch")
}
