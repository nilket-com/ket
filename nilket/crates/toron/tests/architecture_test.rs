use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::distributed::identity::WorkerId;
use toron::error::{BindingError, StorageError};
use toron::exec::Executor;
use toron::ir::{CreateTableRowIdentity, Expr, LogicalPlan, ScalarValue, TableId};
use toron::network::tls::{
	DEFAULT_CLUSTER_DOMAIN, ListenerRole, PeerKind, admit_san, canonical_client_dns_name,
	canonical_daemon_dns_name, canonical_monitor_dns_name,
};
use toron::storage::{ScanRequest, ScanStream, StorageScan, TenantId};
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
	assert_eq!(enum_variant_count(logical_plan), 16);
	assert!(logical_plan.contains("Sort {"));
	assert!(logical_plan.contains("CreateTable {"));
	assert!(logical_plan.contains("CreateTableAsSelect {"));
	assert!(logical_plan.contains("DropTable {"));
	assert!(logical_plan.contains("Update {"));
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
	assert_eq!(enum_variant_count(execution_plan), 14);
	assert!(execution_plan.contains("Sort {"));
	assert!(execution_plan.contains("CreateTable {"));
	assert!(execution_plan.contains("CreateTableAsSelect {"));
	assert!(execution_plan.contains("DropTable {"));
	assert!(execution_plan.contains("Update {"));
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

	for (marker, bound, range_bound) in [
		(
			"fn cast_f32_to_i32",
			"trunc < F64_I32_UPPER",
			"(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc)",
		),
		(
			"fn cast_f32_to_i64",
			"trunc < F64_I64_UPPER",
			"(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc)",
		),
		(
			"fn cast_f64_to_i32",
			"trunc < F64_I32_UPPER",
			"(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc)",
		),
		(
			"fn cast_f64_to_i64",
			"trunc < F64_I64_UPPER",
			"(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc)",
		),
	] {
		let body = brace_blocks_after_marker(&exec, marker)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{marker} body"))
			.1;
		assert!(
			body.contains(bound) || body.contains(range_bound),
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

	for (kernel, bound, range_bound) in [
		(
			"fn try_cast_f32_to_i32",
			"trunc < F64_I32_UPPER",
			"(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc)",
		),
		(
			"fn try_cast_f32_to_i64",
			"trunc < F64_I64_UPPER",
			"(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc)",
		),
		(
			"fn try_cast_f64_to_i32",
			"trunc < F64_I32_UPPER",
			"(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc)",
		),
		(
			"fn try_cast_f64_to_i64",
			"trunc < F64_I64_UPPER",
			"(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc)",
		),
	] {
		let body = brace_blocks_after_marker(&exec, kernel)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{kernel} body"))
			.1;
		assert!(
			body.contains(bound) || body.contains(range_bound),
			"{kernel} missing bound {bound}"
		);
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
		"unsupported SQL statement; only SELECT, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE are supported"
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

	assert!(catalog.contains("inner: Arc<RwLock<MockCatalogInner>>"));
	assert!(catalog.contains("next_table_id: u64"));
	assert!(catalog.contains("self.inner.write()"));
	assert!(catalog.contains("bump_next_table_id(&mut inner, legacy_table_id)"));
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
	assert!(body.contains("create_table_descriptor"));
	assert!(catalog.contains("TableSchemaDescriptor::new_with_layout"));
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
	let coordinator_write =
		std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
			.expect("read coordinator/write.rs");

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

	assert!(write_impl.contains("descriptor(request.tenant_id, request.table_id)?"));
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
	assert!(query_impl.contains("execute_write_plan(catalog.as_ref(), &bound).await"));
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

	assert_eq!(enum_variant_count(logical_plan), 16);
	assert!(logical_plan.contains("InsertSelect {"));
	assert!(logical_plan.contains("DropTable {"));
	assert!(logical_plan.contains("Update {"));
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
	assert_eq!(enum_variant_count(execution_plan), 14);
	assert!(execution_plan.contains("InsertSelect {"));
	assert!(execution_plan.contains("DropTable {"));
	assert!(execution_plan.contains("Update {"));
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

	assert_eq!(enum_variant_count(logical_plan), 16);
	assert!(logical_plan.contains("CreateTableAsSelect {"));
	assert!(logical_plan.contains("Update {"));
	assert!(logical_plan.contains("table: TableRef,"));
	assert!(logical_plan.contains("if_not_exists: bool,"));
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

	assert_eq!(enum_variant_count(execution_plan), 14);
	assert!(execution_plan.contains("CreateTableAsSelect {"));
	assert!(execution_plan.contains("DropTable {"));
	assert!(execution_plan.contains("Update {"));
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
fn adr033_planner_rejects_ctas_column_list_while_allowing_if_not_exists() {
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
	assert!(query_impl.contains("execute_write_plan(catalog.as_ref(), &bound).await"));
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
	let create_table = ctas_arm
		.find(".create_table_with_outcome(")
		.expect("create_table_with_outcome call");
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

	assert!(ctas_arm.contains("let outcome = catalog"));
	assert!(ctas_arm.contains("let binding = outcome.binding"));
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
	let materialize =
		brace_blocks_after_marker(&coordinator_write, "fn materialize_write_select_batch")
			.into_iter()
			.next()
			.expect("materialize_write_select_batch body")
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

	assert_eq!(enum_variant_count(logical_plan), 16);
	assert!(logical_plan.contains("DropTable {"));
	assert!(logical_plan.contains("Update {"));
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

	assert_eq!(enum_variant_count(execution_plan), 14);
	assert!(execution_plan.contains("DropTable {"));
	assert!(execution_plan.contains("Update {"));
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
	let trait_block = brace_blocks_after_marker(&catalog, "pub trait Catalog")
		.into_iter()
		.next()
		.expect("Catalog trait")
		.1;

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

	assert!(drop_body.contains(".remove(&(self.tenant_id, table_id))"));
	assert!(drop_body.contains(".by_ref"));
	assert!(drop_body.contains(".retain(|(tenant_id, _), id|"));
	assert!(drop_body.contains("*id != table_id"));
	assert!(!drop_body.contains("by_ref.insert"));
	assert!(!drop_body.contains("or_insert"));
}

#[test]
fn adr039_wendelon_catalog_drop_table_consumes_finalized_marker() {
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
	for forbidden in ["bumpalo", "typed-arena", "generational-arena", "slab"] {
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
	assert!(planner.contains("SELECT, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE"));
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
fn adr036_logical_variant_count_is_15() {
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
fn adr036_execution_variant_count_is_13() {
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
	assert!(execute_task.contains("spec.context.trace_context"));
	assert!(execute_task.contains("crate::telemetry::task_trace_context(spec)"));
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

	assert!(distributed_source.contains("execute_bound_distributed_with_context("));
	assert!(distributed_source.contains("plan.clone()"));
	assert!(distributed_source.contains("self.context"));
	assert!(distributed_source.contains("self.query_budget"));
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

	assert!(fragment_config.contains("context.query_id"));
	assert!(fragment_config.contains("TaskExecutionContext::from_execution_context"));
	assert!(fragment_config.contains("context"));
	assert!(fragment_config.contains("query_budget: QueryBudget"));
	assert!(fragment_config.contains("TaskBudget::from_query_budget(query_budget)"));
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
		.find("create_table_with_outcome")
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
fn adr039_wendelon_drop_admin_unavailable_lifted() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let drop_body = wendelon_catalog_method_body(&catalog, "async fn drop_table");

	assert!(!drop_body.contains("CatalogError::AdminUnavailable"));
	assert!(drop_body.contains("consume_finalized_drop"));
	assert!(drop_body.contains("remove_table"));
}

#[test]
fn adr039_wendelon_admin_error_mapping_is_structured() {
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
fn adr039_no_table_schema_drop_request_v1_in_toron() {
	assert_toron_sources_do_not_contain(&["TableSchemaDropRequestV1"]);
}

#[test]
fn adr039_toron_does_not_import_wendelon_internal_stage_types() {
	assert_toron_sources_do_not_contain(&[
		"TableDropPrepareRequest",
		"TableReclaimCompletionRequest",
		"TableDropFinalizeRequest",
		"DropStage",
	]);
}

#[test]
fn adr039_toron_does_not_orchestrate_wendelon_drop_stages() {
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
fn adr039_no_catalog_drop_table_with_proof_method() {
	assert_toron_sources_do_not_contain(&["drop_table_with_proof"]);
}

#[test]
fn adr039_no_drop_table_storage_proof_type() {
	assert_toron_sources_do_not_contain(&["DropTableStorageProof"]);
}

#[test]
fn adr040_nilket_is_leaf_workspace_package() {
	let nilket_cargo_path = workspace_root_dir().join("crates/nilket/Cargo.toml");
	let nilket_cargo = std::fs::read_to_string(&nilket_cargo_path).expect("read nilket Cargo.toml");

	assert!(nilket_cargo.contains("name = \"nilket\""));
	assert!(nilket_cargo.contains("toron = { path = \"../toron\" }"));
	assert!(nilket_cargo.contains("wendelon = { path = \"../wendelon\" }"));

	let mut offenders = Vec::new();
	for cargo_path in workspace_cargo_tomls() {
		if cargo_path == nilket_cargo_path {
			continue;
		}
		let source = std::fs::read_to_string(&cargo_path).expect("read Cargo.toml");
		if source.lines().any(|line| {
			let trimmed = line.trim();
			trimmed.starts_with("nilket ") || trimmed.starts_with("nilket=")
		}) {
			offenders.push((cargo_path, 1, "depends on nilket".to_string()));
		}
	}

	assert!(
		offenders.is_empty(),
		"workspace crate depends on nilket:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
fn adr040_nilket_root_has_no_role_runtime_dependencies() {
	let cargo = std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
		.expect("read nilket Cargo.toml");

	for forbidden in ["clap", "tokio", "tracing"] {
		assert!(
			!cargo
				.lines()
				.any(|line| line.trim_start().starts_with(forbidden)),
			"nilket root has forbidden direct dependency {forbidden}"
		);
	}
}

#[test]
fn adr040_nilket_delegates_to_two_roles() {
	let main = std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
		.expect("read nilket main");

	assert!(main.contains("Some(\"wendelon\") => wendelon::cli::run("));
	assert!(main.contains("Some(\"toron\") => toron::cli::run("));
}

#[test]
fn adr040_no_extra_root_commands() {
	let main = std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
		.expect("read nilket main");

	for forbidden in [
		"Some(\"query\")",
		"Some(\"sql\")",
		"Some(\"inspect\")",
		"Some(\"diag\")",
		"Some(\"standalone\")",
		"Some(\"local\")",
	] {
		assert!(
			!main.contains(forbidden),
			"nilket root dispatches {forbidden}"
		);
	}
}

#[test]
fn adr040_wendelon_cli_module_exists() {
	let cli = std::fs::read_to_string(wendelon_manifest_dir().join("src/cli.rs"))
		.expect("read Wendelon CLI source");

	assert!(cli.contains("pub fn run<I>(program: &str, args: I) -> ExitCode"));
}

#[test]
fn adr040_toron_cli_module_exists() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("pub fn run<I>(program: &str, args: I) -> ExitCode"));
}

#[test]
fn adr040_wendelon_cli_does_not_import_toron_runtime() {
	let cli = std::fs::read_to_string(wendelon_manifest_dir().join("src/cli.rs"))
		.expect("read Wendelon CLI source");

	assert!(!cli.contains("use toron::"));
}

#[test]
fn adr040_toron_cli_does_not_import_wendelon_runtime() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	for forbidden in [
		"use wendelon::",
		"WendelonNode",
		"SlabManager",
		"TableDropPrepareRequest",
		"TableReclaimCompletionRequest",
		"TableDropFinalizeRequest",
	] {
		assert!(!cli.contains(forbidden), "Toron CLI contains {forbidden}");
	}
}

#[test]
fn adr040_wendelon_binary_delegates_to_cli_runner() {
	let main = wendelon_manifest_dir().join("src/main.rs");
	if !main.exists() {
		return;
	}
	let source = std::fs::read_to_string(main).expect("read Wendelon main");

	assert!(source.contains("wendelon::cli::run"));
	assert!(!source.contains("tokio::main"));
	assert!(!source.contains("WendelonNode::bootstrap"));
}

#[test]
fn adr041_schema_create_or_replace_no_longer_rejected() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table(")
		.into_iter()
		.next()
		.expect("lower_create_table body")
		.1;

	assert!(body.contains("or_replace: *or_replace"));
	assert!(!body.contains("unsupported CREATE TABLE clause: OR REPLACE"));
}

#[test]
fn adr041_ctas_or_replace_no_longer_rejected() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table_as_select")
		.into_iter()
		.next()
		.expect("lower_create_table_as_select body")
		.1;

	assert!(body.contains("or_replace: *or_replace"));
	assert!(!body.contains("unsupported CREATE TABLE clause: OR REPLACE"));
}

#[test]
fn adr041_ctas_if_not_exists_is_supported_after_adr061() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table_as_select")
		.into_iter()
		.next()
		.expect("lower_create_table_as_select body")
		.1;

	assert!(body.contains("if_not_exists: *if_not_exists"));
	assert!(!body.contains("CTAS does not support IF NOT EXISTS in v1"));
}

#[test]
fn adr041_or_replace_if_not_exists_is_explicitly_rejected() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	assert!(planner.contains("CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS"));
	assert_eq!(
		planner
			.matches("CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS")
			.count(),
		2
	);
}

#[test]
fn adr041_no_statement_sequence_ir() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");

	assert!(!logical.contains("LogicalPlan::Sequence"));
	assert!(!logical.contains("Sequence {"));
	assert!(!execution.contains("ExecutionPlan::Sequence"));
	assert!(!execution.contains("Sequence {"));
}

#[test]
fn adr041_no_wendelon_replace_request() {
	let mut offenders = source_lines(&manifest_dir().join("src"));
	offenders.extend(source_lines(&wendelon_manifest_dir().join("src")));
	offenders.retain(|(_, _, line)| {
		[
			"TableSchemaReplaceRequest",
			"submit_table_replace",
			"drop_table_with_replacement",
		]
		.iter()
		.any(|forbidden| line.contains(forbidden))
	});

	assert!(
		offenders.is_empty(),
		"replacement protocol symbol found:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
fn adr041_replace_reuses_storage_admin_drop() {
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
	let catalog = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper")
		.1;
	let create_arm = catalog
		.split("ExecutionPlan::CreateTable")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::DropTable").next())
		.expect("CreateTable arm");
	let drop_arm = catalog
		.split("ExecutionPlan::DropTable")
		.nth(1)
		.and_then(|rest| rest.split("other =>").next())
		.expect("DropTable arm");

	assert_eq!(coordinator.matches("DropTableStorageRequest {").count(), 1);
	assert!(helper.contains("DropTableStorageRequest {"));
	assert!(create_arm.contains("drop_existing_table_with_storage_admin"));
	assert!(drop_arm.contains("drop_existing_table_with_storage_admin"));
	assert!(!create_arm.contains("DropTableStorageRequest {"));
}

#[test]
fn adr041_ctas_replace_materializes_source_before_drop() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let shared = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared write driver")
		.1;
	let ctas_arm = shared
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::Delete").next())
		.expect("CTAS arm");
	let source = ctas_arm
		.find("source_executor.execute_read_plan(source)")
		.expect("source materialization");
	let drop = ctas_arm
		.find("drop_existing_table_with_storage_admin")
		.expect("replace drop helper");
	let create = ctas_arm
		.find("create_table_with_outcome")
		.expect("catalog create");

	assert!(source < drop);
	assert!(drop < create);
}

#[test]
fn adr041_toron_cli_reservations_preserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("[verb, rest @ ..] if verb == \"sql\" && !rest.is_empty()"));
	assert!(cli.contains("run_sql(rest)"));
	assert!(cli.contains("[verb, rest @ ..] if verb == \"inspect\" => run_inspect(rest)"));
	assert!(!cli.contains("is reserved; production execution is not wired in ADR 040"));
	assert!(!cli.contains("reserved(\"inspect\")"));
}

#[test]
fn adr042_toron_start_is_not_reserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("crate::daemon::run_start(PathBuf::from(config_path))"));
	assert!(!cli.contains("[verb] if verb == \"start\" => reserved(\"start\")"));
}

#[test]
fn adr042_toron_inspect_remains_reserved_after_sql_lift() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("[verb, rest @ ..] if verb == \"sql\" && !rest.is_empty()"));
	assert!(cli.contains("run_sql(rest)"));
	assert!(cli.contains("[verb, rest @ ..] if verb == \"inspect\" => run_inspect(rest)"));
	assert!(!cli.contains("reserved(\"inspect\")"));
}

#[test]
fn adr042_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	let nilket_src =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
			.expect("read nilket main");

	for forbidden in ["tokio", "clap", "tracing"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct {forbidden} dependency"
		);
		assert!(
			!nilket_src.contains(forbidden),
			"nilket root source gained direct {forbidden} runtime surface"
		);
	}
}

#[test]
fn adr042_toron_cli_owns_runtime_not_nilket() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let nilket_src =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
			.expect("read nilket main");

	assert!(daemon.contains("tokio::runtime::Builder::new_multi_thread()"));
	assert!(cli.contains("crate::daemon::run_start"));
	assert!(!nilket_src.contains("tokio::runtime"));
}

#[test]
fn adr042_toron_config_surface_exists() {
	let lib = std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(lib.contains("pub mod config;"));
	assert!(config.contains("pub struct ToronBootstrapConfig"));
	assert!(config.contains("pub fn load_toron_bootstrap_config"));
	assert!(config.contains("pub fn render_example_config"));
	assert!(config.contains("pub fn write_example_config"));
}

#[test]
fn adr042_descriptor_bootstrap_is_product_type_with_table_ids() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("pub struct DescriptorBootstrap"));
	assert!(config.contains("pub tables: Vec<DescriptorBootstrapEntry>"));
	assert!(config.contains("pub struct DescriptorBootstrapEntry"));
	assert!(config.contains("pub tenant_id: u64"));
	assert!(config.contains("pub table_id: u64"));
	assert!(config.contains("pub descriptor: wendelon_client::TableSchemaDescriptor"));
	assert!(config.contains("Vec<(TenantId, TableId, wendelon_client::TableSchemaDescriptor)>"));
}

#[test]
fn adr042_descriptor_bootstrap_has_no_v1_type_suffix() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(!config.contains("ToronDescriptorBootstrapV1"));
	assert!(!config.contains("ToronDescriptorBootstrapTableV1"));
	assert!(!config.contains("DescriptorBootstrapV1"));
	assert!(!config.contains("DescriptorBootstrapEntryV1"));
}

#[test]
fn adr042_daemon_uses_single_descriptor_registry() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let startup = brace_blocks_after_marker(&daemon, "async fn start_with_client")
		.into_iter()
		.next()
		.expect("ToronDaemon::start_with_client")
		.1;

	assert_eq!(
		startup
			.matches("WendelonDescriptorRegistry::from_tenant_descriptors")
			.count(),
		1
	);
	assert!(startup.contains("let registry = Arc::new("));
	assert!(startup.contains("WendelonCatalog::with_client_and_registry"));
	assert!(startup.contains("WendelonStorageScan::with_descriptor_registry"));
	assert!(startup.contains("WendelonStorageWrite::with_descriptor_registry"));
	assert!(startup.contains("WendelonStorageDeleteRows::with_descriptor_registry"));
	assert!(startup.contains("WendelonStorageAdmin::new"));
	assert!(startup.matches("Arc::clone(&registry)").count() >= 5);
}

#[test]
fn adr042_descriptor_bootstrap_is_startup_only() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let startup = brace_blocks_after_marker(&daemon, "async fn start_with_client")
		.into_iter()
		.next()
		.expect("ToronDaemon::start_with_client")
		.1;

	assert!(startup.contains("load_descriptor_bootstrap(path)?"));
	for forbidden in [
		"list_descriptors",
		"refresh_descriptor",
		"watch_descriptor",
		"refresh_topology",
	] {
		assert!(
			!daemon.contains(forbidden),
			"daemon contains descriptor discovery/refresh token {forbidden}"
		);
	}
}

#[test]
fn adr042_unauthenticated_task_listener_is_loopback_only() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("NON_LOOPBACK_BIND_DIAGNOSTIC"));
	assert!(config.contains(".parse::<SocketAddr>()"));
	assert!(config.contains("task_listener_bind.ip().is_loopback()"));
	assert!(daemon.contains("TcpListener::bind(config.worker.task_listener_bind)"));
}

#[test]
fn adr043_toron_sql_is_not_reserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("run_sql(rest)"));
	assert!(cli.contains("toron sql requires <statement...>"));
	assert!(!cli.contains("reserved(\"sql\")"));
}

#[test]
fn adr043_toron_inspect_remains_reserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("[verb, rest @ ..] if verb == \"inspect\" => run_inspect(rest)"));
	assert!(!cli.contains("reserved(\"inspect\")"));
}

#[test]
fn adr043_nilket_root_sql_stays_rejected() {
	let nilket_src =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
			.expect("read nilket main");

	assert!(!nilket_src.contains("Some(\"sql\")"));
	assert!(nilket_src.contains("Some(\"toron\") => toron::cli::run"));
}

#[test]
fn adr043_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "arrow-cast"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr043_toron_cli_sql_is_client_only() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	for forbidden in [
		"DistributedQueryCoordinator",
		"QueryCoordinator",
		"WendelonCatalog",
		"WendelonStorageScan",
		"WendelonStorageWrite",
		"WendelonStorageDeleteRows",
		"WendelonStorageAdmin",
		"WendelonDescriptorRegistry",
	] {
		assert!(
			!cli.contains(forbidden),
			"Toron CLI SQL client must not construct {forbidden}"
		);
	}
	assert!(cli.contains("QuicEndpoint::client_loopback()"));
}

#[test]
fn adr043_sql_listener_config_surface_exists() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"pub struct SqlConfig",
		"pub enum SqlTransport",
		"pub struct CoordinatorConfig",
		"pub struct StaticWorkerConfig",
		"pub sql: Option<SqlConfig>",
		"pub coordinator: CoordinatorConfig",
		"transport = \"quic\"",
		"listener_bind = \"127.0.0.1:7742\"",
	] {
		assert!(
			config.contains(required),
			"missing ADR 043 config token {required}"
		);
	}
}

#[test]
fn adr043_sql_listener_is_loopback_only() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("NON_LOOPBACK_SQL_BIND_DIAGNOSTIC"));
	assert!(config.contains("listener_bind.ip().is_loopback()"));
	assert!(config.contains("!listener_bind.ip().is_loopback() && !tls_present"));
	assert!(config.contains("sql.statement_timeout_seconds must be greater than 0"));
}

#[test]
fn adr043_task_transport_is_quic_by_default_and_loopback_only() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let transport = std::fs::read_to_string(manifest_dir().join("src/server/transport.rs"))
		.expect("read Toron transport source");

	assert!(daemon.contains("QuicEndpoint::bind_loopback(worker_bind_endpoint)"));
	assert!(daemon.contains("QuicTransport::new()?"));
	assert!(transport.contains("pub struct QuicTransport"));
	assert!(transport.contains("QuicEndpoint::client_loopback()?"));
	assert!(transport.contains("connect_worker"));
}

#[test]
fn adr044_toron_tls_config_surface_exists_and_is_optional() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"pub tls: Option<TlsConfig>",
		"pub struct TlsConfig",
		"# [tls]",
		"DEFAULT_CLUSTER_DOMAIN",
		"normalize_tls_config",
		"!task_listener_bind.ip().is_loopback() && tls.is_none()",
		"normalize_sql_config(sql, tls.is_some())",
		"!endpoint.ip().is_loopback() && tls.is_none()",
	] {
		assert!(
			config.contains(required),
			"missing ADR 044 TLS config token {required}"
		);
	}
}

#[test]
fn adr044_tls_policy_uses_exact_san_admission_per_listener() {
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs"))
		.expect("read TLS policy");

	for required in [
		"pub enum ListenerRole",
		"WorkerTask",
		"Sql",
		"pub enum PeerKind",
		"pub struct ListenerPolicy",
		"pub enum TlsError",
		"AUTH_QUIC_ALPN",
		"canonical_daemon_dns_name",
		"canonical_client_dns_name",
		"ExactServerCertVerifier",
		"SanClientCertVerifier",
		"admit_san",
		"(\"toron\", ListenerRole::WorkerTask)",
		"(\"toron\", ListenerRole::Sql) => Err(TlsError::SanRoleRejected)",
		"(\"client\", ListenerRole::Sql)",
		"(\"client\", ListenerRole::WorkerTask) => Err(TlsError::SanRoleRejected)",
		"Uuid::parse_str(id_token)",
	] {
		assert!(
			tls.contains(required),
			"missing ADR 044 TLS policy token {required}"
		);
	}
}

#[test]
fn adr044_daemon_selects_authenticated_quic_only_for_non_loopback_tls_listeners() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let transport = std::fs::read_to_string(manifest_dir().join("src/server/transport.rs"))
		.expect("read Toron transport source");

	assert!(daemon.contains("ListenerPolicy::load"));
	assert!(daemon.contains("QuicEndpoint::bind_authenticated"));
	assert!(daemon.contains("server_config_for(ListenerRole::WorkerTask)"));
	assert!(daemon.contains("server_config_for(ListenerRole::Sql)"));
	assert!(daemon.contains("QuicEndpoint::bind_loopback"));
	assert!(daemon.contains("QuicTransport::authenticated"));
	assert!(transport.contains("connect_stream_with_server_name(remote, &server_name)"));
	assert!(transport.contains("canonical_daemon_dns_name(&worker.worker_id, cluster_domain)"));
}

#[test]
fn adr044_sql_cli_remote_tls_flags_do_not_add_nilket_runtime_dependencies() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for required in [
		"--server-name",
		"--cert",
		"--key",
		"--ca",
		"TORON_SQL_SERVER_NAME",
		"TORON_SQL_CLIENT_CERT",
		"TORON_SQL_CLIENT_KEY",
		"TORON_SQL_CA",
		"client_config_from_paths",
		"client_authenticated",
		"failed to establish authenticated connection to",
	] {
		assert!(
			cli.contains(required),
			"missing ADR 044 SQL CLI token {required}"
		);
	}
	for forbidden in ["tokio", "quinn", "rustls", "arrow-cast"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr044_does_not_change_sql_wire_or_add_token_auth_models() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");

	for forbidden in ["Tls", "Auth", "Principal", "Certificate", "client_cert"] {
		assert!(
			!wire.contains(forbidden),
			"SQL wire gained ADR 044-forbidden token {forbidden}"
		);
	}
	for path in [
		"src/cli.rs",
		"src/daemon.rs",
		"src/quic.rs",
		"src/network/tls.rs",
		"src/server/transport.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(path)).expect("read source");
		for forbidden in [
			"TORON_SQL_TOKEN",
			"Authorization",
			"Bearer",
			"TOFU",
			"fingerprint",
			"SPIFFE",
			"OIDC",
		] {
			assert!(
				!source.contains(forbidden),
				"{path} gained ADR 044-forbidden token auth model {forbidden}"
			);
		}
	}
}

#[test]
fn adr044_no_cert_reload_or_revocation_scope_creep() {
	assert_toron_sources_do_not_contain(&[
		"reload_cert",
		"watch_cert",
		"refresh_cert",
		"CRL",
		"OCSP",
	]);
}

#[test]
fn adr044_san_admission_is_exact_shape_per_listener() {
	let worker_id = WorkerId::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000);
	let daemon_san = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
	let admitted = admit_san(
		&daemon_san,
		ListenerRole::WorkerTask,
		DEFAULT_CLUSTER_DOMAIN,
	)
	.expect("admit daemon SAN");
	assert_eq!(admitted, PeerKind::SiblingDaemon { worker_id });

	let client_san = canonical_client_dns_name("7", DEFAULT_CLUSTER_DOMAIN);
	let admitted = admit_san(&client_san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN)
		.expect("admit client SAN");
	assert_eq!(
		admitted,
		PeerKind::Client {
			principal: "7".to_string(),
			tenant_id: TenantId(7),
		}
	);

	for (san, role) in [
		(
			"toron-550e8400-e29b-41d4-a716-446655440000.evil.cluster.local",
			ListenerRole::WorkerTask,
		),
		("*.cluster.local", ListenerRole::WorkerTask),
		("client-analytics.cluster.local", ListenerRole::Sql),
		("client-007.cluster.local", ListenerRole::Sql),
		("client-0.cluster.local", ListenerRole::Sql),
		("client-analytics_dev.cluster.local", ListenerRole::Sql),
		("toron-not-a-uuid.cluster.local", ListenerRole::WorkerTask),
		("foo-analytics.cluster.local", ListenerRole::Sql),
	] {
		assert!(
			admit_san(san, role, DEFAULT_CLUSTER_DOMAIN).is_err(),
			"{san} should not be admitted for {role:?}"
		);
	}
	assert!(
		admit_san(
			&client_san,
			ListenerRole::WorkerTask,
			DEFAULT_CLUSTER_DOMAIN
		)
		.is_err()
	);
	assert!(admit_san(&daemon_san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err());
}

#[test]
fn adr044_loopback_diagnostic_text_is_brand_clean_and_stable() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"worker listener requires authenticated transport for non-loopback bind address",
		"sql listener requires authenticated transport for non-loopback bind address",
		"coordinator static workers require authenticated transport for non-loopback endpoints",
	] {
		assert!(config.contains(required), "missing diagnostic {required}");
	}
	for forbidden in [
		"Toron TLS",
		"toron TLS",
		"ToronTls",
		"toron-tls",
		"remote SQL auth",
	] {
		assert!(
			!config.contains(forbidden),
			"config diagnostic surface contains project-branded TLS token {forbidden}"
		);
	}
}

#[test]
fn adr044_tenant_id_remains_server_stamped() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");

	assert!(daemon.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
	assert!(daemon.contains("accepted_sql_tenant(connection, cluster_domain)"));
	for source in [daemon.as_str(), wire.as_str()] {
		for forbidden in ["client_tenant", "tenant_claim", "tenant_id: Option"] {
			assert!(
				!source.contains(forbidden),
				"ADR 044 must not add client-selected tenant token {forbidden}"
			);
		}
	}
}

#[test]
fn adr044_ready_lines_are_unchanged() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert_eq!(
		daemon
			.matches("println!(\"READY {}\", daemon.handle().worker_endpoint);")
			.count(),
		1
	);
	assert_eq!(
		daemon
			.matches("println!(\"SQL_READY {sql_endpoint}\");")
			.count(),
		1
	);
}

#[test]
fn adr045_observability_section_is_optional_for_backward_compat() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"pub struct ObservabilityConfig",
		"pub struct ObservabilityListeners",
		"pub listeners: Option<ObservabilityListeners>",
		"listeners: Option<RawObservabilityListeners>",
		"listeners: None",
		"[observability]",
		"# [observability.listeners]",
		"normalize_observability_config(raw.observability, tls.is_some(), base_dir)",
	] {
		assert!(
			config.contains(required),
			"missing ADR 045 optional config token {required}"
		);
	}
}

#[test]
fn adr045_health_listener_is_loopback_only_always() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("OBSERVABILITY_HEALTH_LOOPBACK_DIAGNOSTIC"));
	assert!(config.contains("!health.ip().is_loopback()"));
	assert!(config.contains("observability health listener must bind a loopback address"));
}

#[test]
fn adr045_ready_and_metrics_require_tls_when_configured() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC"));
	assert!(config.contains("if !tls_present"));
	assert!(daemon.contains("config.observability.listeners.is_some() && tls_policy.is_none()"));
	assert!(daemon.contains("rustls_server_config_for(ListenerRole::Observability)"));
	assert!(daemon.contains("TlsAcceptor::from"));
}

#[test]
fn adr045_monitor_san_prefix_admission_is_per_listener() {
	let monitor_san = canonical_monitor_dns_name("prometheus", DEFAULT_CLUSTER_DOMAIN);
	let admitted = admit_san(
		&monitor_san,
		ListenerRole::Observability,
		DEFAULT_CLUSTER_DOMAIN,
	)
	.expect("admit monitor SAN");
	assert_eq!(
		admitted,
		PeerKind::Monitor {
			principal: "prometheus".to_string(),
		}
	);

	assert!(
		admit_san(
			&monitor_san,
			ListenerRole::WorkerTask,
			DEFAULT_CLUSTER_DOMAIN
		)
		.is_err()
	);
	assert!(admit_san(&monitor_san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err());

	let worker_id = WorkerId::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000);
	let daemon_san = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
	let client_san = canonical_client_dns_name("analytics", DEFAULT_CLUSTER_DOMAIN);
	for san in [
		daemon_san.as_str(),
		client_san.as_str(),
		"monitor_ops.cluster.local",
	] {
		assert!(
			admit_san(san, ListenerRole::Observability, DEFAULT_CLUSTER_DOMAIN).is_err(),
			"observability listener should reject {san}"
		);
	}
}

#[test]
fn adr045_observability_listeners_use_hand_rolled_http_no_hyper_no_axum() {
	let manifest_dir = manifest_dir();
	for path in [
		"src/network/observability.rs",
		"src/network/observability_http.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir.join(path)).expect("read source");
		assert!(source.contains("read_request"));
		assert!(source.contains("write_response"));
		for forbidden in [
			"hyper",
			"axum",
			"actix",
			"tower",
			"Request<",
			"Response<",
			"keep-alive",
			"http2",
			"quinn",
		] {
			assert!(
				!source.contains(forbidden),
				"{path} contains ADR 045-forbidden HTTP framework/protocol token {forbidden}"
			);
		}
	}
}

#[test]
fn adr045_metrics_uses_prometheus_text_format_no_histograms() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	let adr045_observability = observability
		.split("fn render_audit_metrics")
		.next()
		.expect("pre-audit observability source");

	assert!(adr045_observability.contains("text/plain; version=0.0.4; charset=utf-8"));
	assert!(adr045_observability.contains("# TYPE daemon_ready gauge"));
	assert!(adr045_observability.contains("listener_accepts_total"));
	assert!(adr045_observability.contains("metrics_scrapes_total"));
	for forbidden in ["_bucket", "_sum", "_count", "histogram", "Histogram"] {
		assert!(
			!adr045_observability.contains(forbidden),
			"ADR 045 metrics must not introduce histogram token {forbidden}"
		);
	}
}

#[test]
fn adr045_no_revocation_no_audit_no_traces_in_observability() {
	for path in [
		"src/network/observability.rs",
		"src/network/observability_http.rs",
		"src/network/tls.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(path)).expect("read source");
		for forbidden in [
			"RevocationSnapshot",
			"CRL",
			"OCSP",
			"AuditSink",
			"audit_event",
			"opentelemetry",
			"OTLP",
			"ActorPermission",
		] {
			if path == "src/network/observability.rs" && forbidden == "audit_event" {
				continue;
			}
			assert!(
				!source.contains(forbidden),
				"{path} contains ADR 045-forbidden observability expansion token {forbidden}"
			);
		}
	}
}

#[test]
fn adr045_observability_ready_line_is_appended_not_replaced() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	let ready = daemon
		.find("println!(\"READY {}\", daemon.handle().worker_endpoint);")
		.expect("READY line");
	let sql_ready = daemon
		.find("println!(\"SQL_READY {sql_endpoint}\");")
		.expect("SQL_READY line");
	let observability_ready = daemon
		.find("\"OBSERVABILITY_READY health={} ready={} metrics={}\"")
		.expect("OBSERVABILITY_READY line");
	assert!(ready < sql_ready);
	assert!(sql_ready < observability_ready);
}

#[test]
fn adr045_one_tls_material_set_per_daemon_still_holds() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs"))
		.expect("read TLS source");

	assert_eq!(config.matches("tls: Option<RawTlsConfig>").count(), 1);
	assert_eq!(config.matches("pub tls: Option<TlsConfig>").count(), 1);
	assert_eq!(daemon.matches("ListenerPolicy::load").count(), 1);
	assert!(tls.contains("server_config_worker_task"));
	assert!(tls.contains("server_config_sql"));
	assert!(tls.contains("server_config_observability"));
	for forbidden in [
		"[observability.tls]",
		"observability_tls",
		"ready_tls",
		"metrics_tls",
	] {
		assert!(
			!config.contains(forbidden) && !daemon.contains(forbidden),
			"ADR 045 must reuse daemon TLS material set, found {forbidden}"
		);
	}
}

#[test]
fn adr045_initial_metric_set_is_functional_brand_clean_and_stable() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	let adr045_observability = observability
		.split("fn render_audit_metrics")
		.next()
		.expect("pre-audit observability source");
	let adr045_stable_observability = adr045_observability
		.replace(
			"toron_downstream_storage_cancellations_total",
			"downstream_storage_cancellations_total",
		)
		.replace("toron_otlp_spans_dropped_total", "otlp_spans_dropped_total");

	for required in [
		"daemon_ready",
		"daemon_uptime_seconds",
		"listener_up",
		"listener_accepts_total",
		"listener_rejects_total",
		"metrics_scrapes_total",
		"descriptors_loaded",
		"static_workers_configured",
		"listener=\\\"{}\\\"",
		"reason=\\\"{}\\\"",
	] {
		assert!(
			observability.contains(required),
			"missing ADR 045 metric token {required}"
		);
	}
	for forbidden in [
		"toron_",
		"worker_id",
		"query_id",
		"task_id",
		"table_id",
		"sql_text",
		"certificate",
	] {
		assert!(
			!adr045_stable_observability.contains(forbidden),
			"ADR 045 metrics include forbidden high-cardinality/branded token {forbidden}"
		);
	}
	assert_bounded_resource_tenant_labels_only(&observability);
}

#[test]
fn adr045_ready_snapshot_excludes_high_cardinality_identity_fields() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");

	for required in [
		"pub struct ReadinessSnapshot",
		"pub ready: bool",
		"pub worker_listener_bound: bool",
		"pub sql_listener_bound: bool",
		"pub observability_listeners_bound: bool",
		"pub descriptors_loaded: u64",
		"pub static_workers_configured: u32",
		"pub uptime_seconds: u64",
		"pub degraded_reason: Option<String>",
	] {
		assert!(
			observability.contains(required),
			"missing ADR 045 readiness field {required}"
		);
	}
	let readiness_struct =
		brace_blocks_after_marker(&observability, "pub struct ReadinessSnapshot")
			.into_iter()
			.next()
			.expect("ReadinessSnapshot")
			.1;
	let readiness_json = brace_blocks_after_marker(&observability, "pub fn to_json")
		.into_iter()
		.next()
		.expect("ReadinessSnapshot::to_json")
		.1;
	let readiness_surface = format!("{readiness_struct}\n{readiness_json}");
	for forbidden in [
		"worker_id",
		"tenant_id",
		"query_id",
		"task_id",
		"table_id",
		"sql_text",
		"certificate",
		"endpoint",
		"uuid",
	] {
		assert!(
			!readiness_surface.contains(forbidden),
			"ADR 045 readiness surface includes forbidden token {forbidden}"
		);
	}
}

#[test]
fn adr045_observability_http_has_per_request_read_timeout() {
	let http = std::fs::read_to_string(manifest_dir().join("src/network/observability_http.rs"))
		.expect("read observability HTTP source");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");

	assert!(http.contains("pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);"));
	assert!(http.contains("MAX_REQUEST_HEADER_BYTES"));
	assert!(observability.contains("timeout(READ_REQUEST_TIMEOUT, read_request(stream))"));
}

#[test]
fn adr045_new_observability_types_are_brand_clean_and_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronObservabilityConfig",
		"ToronTracingFormat",
		"RawToronObservabilityConfig",
		"ToronReadinessSnapshot",
		"ToronMetricsRegistry",
		"ToronObservabilityListeners",
		"ObservabilityConfigV1",
		"ReadinessSnapshotV1",
		"MetricsRegistryV1",
	]);
}

#[test]
fn adr046_toron_inspect_is_not_reserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("[verb, rest @ ..] if verb == \"inspect\" => run_inspect(rest)"));
	assert!(cli.contains("fn run_inspect(args: &[String]) -> ExitCode"));
	assert!(!cli.contains("reserved(\"inspect\")"));
	assert!(!cli.contains("Toron inspect is reserved in ADR 040"));
}

#[test]
fn adr046_toron_inspect_is_client_only() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let inspect_body = brace_blocks_after_marker(&cli, "async fn fetch_ready_with_tcp_connect")
		.into_iter()
		.next()
		.expect("fetch_ready_with_tcp_connect body")
		.1;

	assert!(cli.contains("TcpStream::connect(options.endpoint)"));
	for required in ["TlsConnector::from", "read_response"] {
		assert!(
			inspect_body.contains(required),
			"inspect client missing {required}"
		);
	}
	for forbidden in [
		"ToronDaemon",
		"ToronWorker",
		"DistributedQueryCoordinator",
		"WendelonCatalog",
		"DescriptorRegistry",
		"MetricsRegistry",
		"StorageScan",
	] {
		assert!(
			!inspect_body.contains(forbidden),
			"inspect client constructs daemon machinery token {forbidden}"
		);
	}
}

#[test]
fn adr046_toron_inspect_dials_ready_endpoint() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let inspect_body = brace_blocks_after_marker(&cli, "async fn fetch_ready_with_tcp_connect")
		.into_iter()
		.next()
		.expect("fetch_ready_with_tcp_connect body")
		.1;

	assert!(inspect_body.contains("crate::network::observability::READY_PATH"));
	assert!(inspect_body.contains("write_request("));
	assert!(inspect_body.contains("&options.endpoint.to_string()"));
	assert!(!inspect_body.contains("&tls.server_name"));
	for forbidden in [
		"HEALTH_PATH",
		"METRICS_PATH",
		"\"/inspect\"",
		"\"/workers\"",
		"\"/config\"",
	] {
		assert!(
			!inspect_body.contains(forbidden),
			"inspect client must not dial {forbidden}"
		);
	}
}

#[test]
fn adr046_toron_inspect_endpoint_precedence_flag_env_default() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let parse_body = brace_blocks_after_marker(&cli, "fn parse_inspect_args")
		.into_iter()
		.next()
		.expect("parse_inspect_args body")
		.1;

	assert!(cli.contains("const DEFAULT_INSPECT_ENDPOINT: &str = \"127.0.0.1:7781\";"));
	assert!(parse_body.contains("\"--endpoint\""));
	assert!(parse_body.contains("std::env::var(\"TORON_INSPECT_ENDPOINT\")"));
	assert!(parse_body.contains("DEFAULT_INSPECT_ENDPOINT"));
	assert!(parse_body.contains("invalid TORON_INSPECT_ENDPOINT"));
}

#[test]
fn adr046_toron_inspect_requires_all_four_tls_flags_together() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let parse_body = brace_blocks_after_marker(&cli, "fn parse_inspect_args")
		.into_iter()
		.next()
		.expect("parse_inspect_args body")
		.1;

	for required in [
		"TORON_INSPECT_SERVER_NAME",
		"TORON_INSPECT_CLIENT_CERT",
		"TORON_INSPECT_CLIENT_KEY",
		"TORON_INSPECT_CA",
		"toron inspect requires --server-name, --cert, --key, and --ca",
	] {
		assert!(parse_body.contains(required), "missing {required}");
	}
	assert!(!parse_body.contains("endpoint.ip().is_loopback()"));
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs"))
		.expect("read TLS source");

	assert!(tls.contains("canonical_monitor_dns_name"));
	assert!(tls.contains("\"monitor\""));
	assert!(!tls.contains("inspect-"));
	assert!(!cli.contains("inspect-"));
}

#[test]
fn adr046_toron_inspect_renders_text_by_default_json_with_flag() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let render_body = brace_blocks_after_marker(&cli, "fn render_readiness_text")
		.into_iter()
		.next()
		.expect("render_readiness_text body")
		.1;

	for required in [
		"ready:",
		"worker_listener_bound:",
		"sql_listener_bound:",
		"observability_listeners_bound:",
		"descriptors_loaded:",
		"static_workers_configured:",
		"uptime_seconds:",
		"degraded_reason:",
		"serde_json::to_string",
		"null",
	] {
		assert!(render_body.contains(required), "missing {required}");
	}
	for forbidden in [
		"endpoint",
		"worker_id",
		"tenant_id",
		"query_id",
		"task_id",
		"table",
		"sql_text",
		"certificate",
		"uuid",
	] {
		assert!(
			!render_body.contains(forbidden),
			"inspect text renderer leaks {forbidden}"
		);
	}
	let run_body = brace_blocks_after_marker(&cli, "async fn run_inspect_async")
		.into_iter()
		.next()
		.expect("run_inspect_async body")
		.1;

	assert!(run_body.contains("print!(\"{}\", response.body)"));
	assert!(run_body.contains("!response.body.ends_with('\\n')"));
	assert!(!run_body.contains("trim_end"));
	assert!(!run_body.contains("pretty"));
	assert!(!run_body.contains("envelope"));

	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	assert!(observability.contains("serde::Deserialize"));
	assert!(cli.contains("serde_json::from_slice"));
	assert!(!observability.contains("deny_unknown_fields"));
	assert!(!cli.contains("deny_unknown_fields"));
}

#[test]
fn adr046_toron_inspect_exit_codes_are_pinned_to_0_1_2() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let run_body = brace_blocks_after_marker(&cli, "async fn run_inspect_async")
		.into_iter()
		.next()
		.expect("run_inspect_async body")
		.1;

	assert!(run_body.contains("ExitCode::SUCCESS"));
	assert!(run_body.contains("ExitCode::from(1)"));
	assert!(run_body.contains("ExitCode::from(2)"));
	assert!(!run_body.contains("ExitCode::from(3)"));
}

#[test]
fn adr046_inspect_handles_only_200_and_503_status_codes() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let inspect_body = brace_blocks_after_marker(&cli, "async fn fetch_ready_with_tcp_connect")
		.into_iter()
		.next()
		.expect("fetch_ready_with_tcp_connect body")
		.1;

	assert!(inspect_body.contains("response.status_code != 200"));
	assert!(inspect_body.contains("response.status_code != 503"));
	assert!(inspect_body.contains("InspectError::UnexpectedStatus"));
	assert!(cli.contains("unexpected HTTP status {status_code}"));
}

#[test]
fn adr046_inspect_has_tcp_connect_timeout() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);"));
	assert!(cli.contains("tokio::time::timeout(connect_timeout, connect)"));
	assert!(cli.contains("TcpStream::connect(options.endpoint)"));
	assert!(cli.contains("CONNECT_TIMEOUT"));
}

#[test]
fn adr046_no_new_daemon_wire_surface_in_observability_module() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");

	for forbidden in ["INSPECT_PATH", "INFO_PATH", "VERSION_PATH", "WORKERS_PATH"] {
		assert!(
			!observability.contains(forbidden),
			"ADR 046 must not add observability endpoint {forbidden}"
		);
	}
	assert!(daemon.contains("println!(\"READY {}\", daemon.handle().worker_endpoint);"));
	assert!(daemon.contains("println!(\"SQL_READY {sql_endpoint}\");"));
	assert!(daemon.contains("\"OBSERVABILITY_READY health={} ready={} metrics={}\""));
	assert!(!sql_wire.contains("Inspect"));
	assert!(!sql_wire.contains("AuthToken"));
}

#[test]
fn adr047_tls_pki_crate_exists_under_workspace_glob() {
	let root_manifest =
		std::fs::read_to_string(workspace_root_dir().join("Cargo.toml")).expect("read workspace");
	assert!(root_manifest.contains("\"crates/*\""));
	assert!(
		workspace_root_dir()
			.join("crates/tls-pki/Cargo.toml")
			.exists()
	);
}

#[test]
fn adr047_tls_pki_has_no_quinn_no_tokio_no_tracing_deps() {
	let manifest = std::fs::read_to_string(workspace_root_dir().join("crates/tls-pki/Cargo.toml"))
		.expect("read tls-pki manifest");

	for forbidden in [
		"quinn",
		"tokio",
		"tokio-rustls",
		"tracing",
		"async-trait",
		"reqwest",
		"hyper",
		"axum",
		"actix-web",
	] {
		assert!(
			!manifest.contains(forbidden),
			"tls-pki manifest must not contain dependency token {forbidden}"
		);
	}
}

#[test]
fn adr047_toron_tls_module_consumes_tls_pki_substrate() {
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs"))
		.expect("read Toron TLS source");

	for required in [
		"use tls_pki::",
		"TlsMaterial",
		"build_client_cert_chain_verifier",
		"build_server_cert_chain_verifier",
		"build_pki_rustls_server_config",
		"build_pki_rustls_client_config",
		"canonical_dns_name",
		"extract_dns_sans",
		"pub enum TlsError",
		"pub enum ListenerRole",
		"pub enum PeerKind",
		"ExactServerCertVerifier",
		"SanClientCertVerifier",
		"canonical_daemon_dns_name",
		"canonical_client_dns_name",
		"canonical_monitor_dns_name",
		"AUTH_QUIC_ALPN",
		"OBSERVABILITY_HTTP_ALPN",
	] {
		assert!(
			tls.contains(required),
			"Toron TLS source missing {required}"
		);
	}
}

#[test]
fn adr047_wendelon_tls_module_consumes_tls_pki_substrate() {
	let tls = std::fs::read_to_string(wendelon_manifest_dir().join("src/network/tls.rs"))
		.expect("read Wendelon TLS source");

	for required in [
		"pub use tls_pki::DEFAULT_CLUSTER_DOMAIN",
		"pub struct TlsMaterial(tls_pki::TlsMaterial)",
		"tls_pki::TlsMaterial::load",
		"tls_pki::load_ca_roots",
		"tls_pki::build_client_cert_chain_verifier",
		"tls_pki::build_server_cert_chain_verifier",
		"tls_pki::build_rustls_server_config_with_alpns",
		"tls_pki::build_rustls_client_config_with_alpns",
		"tls_pki::canonical_dns_name(\"node\"",
		"HRFT_ALPN",
		"ActorIdentity",
		"ActorTrustRoots",
		"VerifiedTransportPeer",
	] {
		assert!(
			tls.contains(required),
			"Wendelon TLS source missing {required}"
		);
	}
}

#[test]
fn adr047_no_wendelon_internal_types_in_tls_pki_public_api() {
	let source = tls_pki_source_text();

	for forbidden in [
		"RevocationSnapshot",
		"ActorPermission",
		"ActorIdentity",
		"ActorTrustRoots",
		"VerifiedTransportPeer",
		"ACTOR_CLAIMS_OID_DOTTED",
		"WendelonNode",
		"NodeId",
		"authorize_break_glass_scope",
		"authorize_actor",
	] {
		assert!(
			!source.contains(forbidden),
			"tls-pki public/source surface contains Wendelon-internal token {forbidden}"
		);
	}
}

#[test]
fn adr047_no_toron_internal_types_in_tls_pki_public_api() {
	let source = tls_pki_source_text();

	for forbidden in [
		"WorkerId",
		"tenant_id",
		"ToronWorker",
		"ToronDaemon",
		"SqlClientTlsOptions",
		"InspectTlsOptions",
		"quinn::",
	] {
		assert!(
			!source.contains(forbidden),
			"tls-pki public/source surface contains Toron-internal token {forbidden}"
		);
	}
}

#[test]
fn adr047_admit_san_accepts_parameterized_prefix_set() {
	let san = std::fs::read_to_string(workspace_root_dir().join("crates/tls-pki/src/san.rs"))
		.expect("read tls-pki san source");

	assert!(san.contains("pub fn admit_san("));
	assert!(san.contains("allowed_prefixes: &[&str]"));
	assert!(san.contains("allowed_prefixes.contains(&prefix)"));
	assert_eq!(
		tls_pki::admit_san("node-7.cluster.local", DEFAULT_CLUSTER_DOMAIN, &["node"])
			.expect("admit node")
			.id_token,
		"7"
	);
	assert!(
		tls_pki::admit_san("node-7.cluster.local", DEFAULT_CLUSTER_DOMAIN, &["client"]).is_err()
	);
}

#[test]
fn adr047_canonical_dns_name_parameterized_by_prefix() {
	let san = std::fs::read_to_string(workspace_root_dir().join("crates/tls-pki/src/san.rs"))
		.expect("read tls-pki san source");

	assert!(san.contains("pub fn canonical_dns_name("));
	assert!(san.contains("prefix: &str"));
	assert_eq!(
		tls_pki::canonical_dns_name("node", "7", DEFAULT_CLUSTER_DOMAIN),
		"node-7.cluster.local"
	);
	assert_eq!(
		tls_pki::canonical_dns_name(
			"toron",
			"550e8400-e29b-41d4-a716-446655440000",
			DEFAULT_CLUSTER_DOMAIN
		),
		"toron-550e8400-e29b-41d4-a716-446655440000.cluster.local"
	);
}

#[test]
fn adr047_no_alpn_constants_in_tls_pki() {
	let source = tls_pki_source_text();

	for forbidden in ["HRFT_ALPN", "AUTH_QUIC_ALPN", "OBSERVABILITY_HTTP_ALPN"] {
		assert!(
			!source.contains(forbidden),
			"tls-pki must not declare ring-local ALPN constant {forbidden}"
		);
	}
}

#[test]
fn adr047_byte_identical_post_extraction_with_prior_cycle_guards() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in ["adr042_", "adr043_", "adr044_", "adr045_", "adr046_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	for required in [
		"adr044_tls_policy_uses_exact_san_admission_per_listener",
		"adr045_ready_snapshot_excludes_high_cardinality_identity_fields",
		"adr046_toron_inspect_exit_codes_are_pinned_to_0_1_2",
	] {
		assert!(
			architecture_test.contains(required),
			"prior-cycle guard {required} must remain present"
		);
	}
}

#[test]
fn adr047_toron_der_and_cbor_parsing_delegates_to_tls_pki() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("DerReader")
				|| line.contains("CborReader")
				|| line.contains("SUBJECT_ALT_NAME_OID")
				|| line.contains("DNS_NAME_TAG")
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"Toron source must delegate DER/CBOR parsing to tls-pki:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
fn adr047_new_tls_pki_types_are_brand_clean_and_no_v1_suffix() {
	let source = tls_pki_source_text();

	for forbidden in [
		"ToronTls",
		"WendelonTls",
		"NilketTls",
		"ToronPki",
		"WendelonPki",
		"TlsMaterialV1",
		"PkiErrorV1",
		"PeerIdentityV1",
		"DerReaderV1",
		"CborReaderV1",
	] {
		assert!(
			!source.contains(forbidden),
			"tls-pki contains forbidden branded/V1 token {forbidden}"
		);
	}
}

#[test]
fn adr048_observability_http_substrate_is_retargeted_to_observability_namespace() {
	let root_manifest =
		std::fs::read_to_string(workspace_root_dir().join("Cargo.toml")).expect("read workspace");
	assert!(root_manifest.contains("\"crates/*\""));
	assert!(
		workspace_root_dir()
			.join("crates/observability/Cargo.toml")
			.exists()
	);
	assert!(
		!workspace_root_dir()
			.join("crates/observability-http/Cargo.toml")
			.exists(),
		"ADR 065 retarget removes the flat observability-http crate"
	);
}

#[test]
fn adr048_observability_http_depends_only_on_tokio_io() {
	let manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/observability/Cargo.toml"))
			.expect("read observability manifest");
	let http_source = observability_http_source_text();
	assert!(manifest.contains(r#"default = []"#));
	assert!(manifest.contains(r#"http = ["dep:tokio"]"#));
	assert!(manifest.contains(
		r#"tokio = { version = "1.37", default-features = false, features = ["io-util", "macros", "rt"] }"#
	));
	for forbidden in [
		"hyper",
		"hyper-util",
		"axum",
		"actix-web",
		"tower",
		"reqwest",
		"ureq",
		"h2",
		"httparse",
		"bytes::",
		"tokio-util",
		"rustls",
		"tokio-rustls",
		"quinn",
		"tls-pki",
		"tracing",
	] {
		assert!(
			!http_source.contains(forbidden),
			"observability-http source must not contain dependency token {forbidden}"
		);
	}
}

#[test]
fn adr048_observability_http_has_bounded_header_and_body_limits() {
	let lib =
		std::fs::read_to_string(workspace_root_dir().join("crates/observability/src/http/mod.rs"))
			.expect("read observability::http module");

	for required in [
		"pub const MAX_REQUEST_HEADER_BYTES: usize = 8 * 1024;",
		"pub const MAX_RESPONSE_HEADER_BYTES: usize = 8 * 1024;",
		"pub const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;",
	] {
		assert!(lib.contains(required), "missing bounded limit {required}");
	}
}

#[test]
fn adr048_observability_http_does_not_own_timeouts() {
	let source = observability_http_source_text();

	for forbidden in [
		"Duration",
		"tokio::time",
		"timeout",
		"READ_REQUEST_TIMEOUT",
		"CONNECT_TIMEOUT",
	] {
		assert!(
			!source.contains(forbidden),
			"observability-http must not own timeout token {forbidden}"
		);
	}
}

#[test]
fn adr048_observability_http_public_surface_is_small() {
	let source = observability_http_source_text();

	for required in [
		"pub const MAX_REQUEST_HEADER_BYTES",
		"pub const MAX_RESPONSE_HEADER_BYTES",
		"pub const MAX_RESPONSE_BODY_BYTES",
		"pub struct RequestLine",
		"pub struct HttpResponse",
		"pub async fn read_request",
		"pub fn parse_request_line",
		"pub async fn read_response",
		"pub async fn write_request",
		"pub async fn write_response",
	] {
		assert!(
			source.contains(required),
			"observability-http must expose {required}"
		);
	}
	for forbidden in [
		"pub struct Router",
		"pub struct Listener",
		"pub fn listen",
		"pub fn dispatch",
		"pub fn route",
		"pub struct StatusCode",
		"pub enum Status",
		"rustls::",
		"quinn::",
		"tls_pki::",
		"tracing::",
		"TcpListener",
		"TcpStream",
	] {
		assert!(
			!source.contains(forbidden),
			"observability-http must not expose {forbidden}"
		);
	}
}

#[test]
fn adr048_toron_observability_http_module_is_shim_only() {
	let http = std::fs::read_to_string(manifest_dir().join("src/network/observability_http.rs"))
		.expect("read observability HTTP shim");

	assert!(http.contains("pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);"));
	assert!(http.contains("pub use observability::http::"));
	for required in [
		"HttpResponse",
		"MAX_REQUEST_HEADER_BYTES",
		"MAX_RESPONSE_HEADER_BYTES",
		"MAX_RESPONSE_BODY_BYTES",
		"RequestLine",
		"parse_request_line",
		"read_request",
		"read_response",
		"write_request",
		"write_response",
	] {
		assert!(http.contains(required), "shim missing re-export {required}");
	}
	for forbidden in ["header_end", "content_length", "body_len", "loop {"] {
		assert!(
			!http.contains(forbidden),
			"observability HTTP shim contains parser token {forbidden}"
		);
	}
}

#[test]
fn adr048_toron_observability_and_inspect_use_shared_http_helper() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(observability.contains("observability_http::parse_request_line"));
	assert!(cli.contains("observability_http::write_request"));
	assert!(cli.contains("observability_http::read_response"));
}

#[test]
fn adr048_no_new_toron_observability_routes() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");

	for required in [
		"pub const HEALTH_PATH: &str = \"/health\";",
		"pub const READY_PATH: &str = \"/ready\";",
		"pub const METRICS_PATH: &str = \"/metrics\";",
	] {
		assert!(
			observability.contains(required),
			"missing observability route {required}"
		);
	}
	assert_eq!(observability.matches("_PATH: &str").count(), 3);
	assert!(!observability.contains("INSPECT_PATH"));
	assert!(!observability.contains("INFO_PATH"));
	assert!(!observability.contains("VERSION_PATH"));
	assert!(!observability.contains("WORKERS_PATH"));
}

#[test]
fn adr048_no_nilket_root_runtime_dependency_change() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in [
		"tokio",
		"clap",
		"tracing",
		"rustls",
		"tokio-rustls",
		"observability-http",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr048_no_wendelon_inspect_branch_this_cycle() {
	let cli = std::fs::read_to_string(wendelon_manifest_dir().join("src/cli.rs"))
		.expect("read Wendelon CLI source");

	assert!(cli.contains("[cmd, rest @ ..] if cmd == \"inspect\""));
	assert!(cli.contains("struct InspectOptions"));
	assert!(cli.contains("observability_http::write_request"));
	assert!(!cli.contains("ToronInspect"));
}

#[test]
fn adr048_wendelon_observability_http_helpers_remain_local() {
	let observability =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/observability.rs"))
			.expect("read Wendelon observability source");
	let exporter = std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/exporter.rs"))
		.expect("read Wendelon exporter source");

	for required in [
		"async fn read_request<S",
		"fn parse_request_line(",
		"async fn write_response<S",
	] {
		assert!(
			observability.contains(required),
			"Wendelon observability source missing local helper {required}"
		);
	}
	assert!(exporter.contains("async fn handle_connection("));
	assert!(exporter.contains("content-type: text/plain"));
	assert!(!observability.contains("observability_http"));
	assert!(!exporter.contains("observability_http"));
}

#[test]
fn adr048_prior_adr04x_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	for required in [
		"adr040_nilket_root_has_no_role_runtime_dependencies",
		"adr045_ready_snapshot_excludes_high_cardinality_identity_fields",
		"adr046_toron_inspect_exit_codes_are_pinned_to_0_1_2",
		"adr047_byte_identical_post_extraction_with_prior_cycle_guards",
	] {
		assert!(
			architecture_test.contains(required),
			"prior-cycle guard {required} must remain present"
		);
	}
}

#[test]
fn prior_adr04x_architecture_guards_pass_post_http_extraction() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing ADR guard prefix {prefix}");
	}
	for required in [
		"toron_observability_ready_metrics_preserve_responses_post_extraction",
		"toron_inspect_preserves_ready_fetch_post_extraction",
		"prior_adr04x_architecture_guards_pass_post_http_extraction",
	] {
		assert!(
			architecture_test.contains(required)
				|| std::fs::read_to_string(
					manifest_dir().join("tests/observability_listener_test.rs")
				)
				.expect("read observability listener test")
				.contains(required)
				|| std::fs::read_to_string(manifest_dir().join("src/cli.rs"))
					.expect("read cli source")
					.contains(required),
			"ADR 048 preservation fixture {required} must remain present"
		);
	}
}

#[test]
fn adr048_new_observability_http_types_are_brand_clean_and_no_v1_suffix() {
	let source = observability_http_source_text();

	for forbidden in [
		"ToronHttp",
		"WendelonHttp",
		"NilketHttp",
		"HttpResponseV1",
		"RequestLineV1",
		"ObservabilityHttpV1",
	] {
		assert!(
			!source.contains(forbidden),
			"observability-http contains forbidden branded/V1 token {forbidden}"
		);
	}
}

#[test]
fn adr065_observability_crate_exists_under_workspace_glob() {
	assert!(
		workspace_root_dir()
			.join("crates/observability/Cargo.toml")
			.exists()
	);
	assert!(
		std::fs::read_to_string(workspace_root_dir().join("Cargo.toml"))
			.expect("read workspace manifest")
			.contains("\"crates/*\"")
	);
}

#[test]
fn adr065_observability_default_features_are_empty() {
	assert!(observability_manifest().contains("default = []"));
}

#[test]
fn adr065_observability_exposes_http_and_audit_modules() {
	let lib = std::fs::read_to_string(workspace_root_dir().join("crates/observability/src/lib.rs"))
		.expect("read observability lib");
	assert!(lib.contains("#[cfg(feature = \"http\")]"));
	assert!(lib.contains("pub mod http;"));
	assert!(lib.contains("#[cfg(feature = \"audit\")]"));
	assert!(lib.contains("pub mod audit;"));
}

#[test]
fn adr065_observability_builds_with_only_http_feature() {
	let manifest = observability_manifest();
	assert!(manifest.contains(r#"http = ["dep:tokio"]"#));
	assert!(manifest.contains("tokio = {"));
	assert!(manifest.contains("optional = true"));
}

#[test]
fn adr065_observability_builds_with_only_audit_feature() {
	let manifest = observability_manifest();
	assert!(manifest.contains("audit = ["));
	for required in [
		"dep:serde",
		"dep:serde_json",
		"dep:ed25519-dalek",
		"dep:rand",
	] {
		assert!(
			manifest.contains(required),
			"audit feature missing {required}"
		);
	}
}

#[test]
fn adr065_observability_http_does_not_reference_audit() {
	let source = observability_http_source_text();
	for forbidden in [
		"observability::audit",
		"crate::audit",
		"super::audit",
		"AuditSink",
	] {
		assert!(
			!source.contains(forbidden),
			"observability::http must not reference audit token {forbidden}"
		);
	}
}

#[test]
fn adr065_observability_audit_does_not_reference_http() {
	let source = observability_audit_source_text();
	assert!(!source.contains("pub mod http"));
	assert!(!source.contains("observability::http"));
	assert!(!source.contains("HttpResponse"));
	assert!(!source.contains("RequestLine"));
}

#[test]
fn adr065_observability_http_retargets_adr048_substrate() {
	let toron_manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let wendelon_manifest = std::fs::read_to_string(wendelon_manifest_dir().join("Cargo.toml"))
		.expect("read Wendelon manifest");
	assert!(toron_manifest.contains("observability = { path = \"../observability\""));
	assert!(toron_manifest.contains("\"http\""));
	assert!(toron_manifest.contains("\"audit\""));
	assert!(wendelon_manifest.contains("observability = { path = \"../observability\""));
	assert!(wendelon_manifest.contains("\"http\""));
	assert!(
		workspace_root_dir()
			.join("crates/observability/src/http/mod.rs")
			.exists()
	);
}

#[test]
fn adr065_observability_http_preserves_limits_and_timeout_boundary() {
	adr048_observability_http_has_bounded_header_and_body_limits();
	adr048_observability_http_does_not_own_timeouts();
	adr045_observability_http_has_per_request_read_timeout();
}

#[test]
fn adr065_observability_http_preserves_small_public_surface() {
	adr048_observability_http_public_surface_is_small();
}

#[test]
fn adr065_flat_observability_http_runtime_crate_removed() {
	assert!(
		!workspace_root_dir()
			.join("crates/observability-http")
			.exists()
	);
	assert!(!observability_manifest().contains("name = \"observability-http\""));
}

#[test]
fn adr065_observability_has_no_toron_or_wendelon_dependency() {
	let manifest = observability_manifest();
	for forbidden in ["toron", "wendelon", "../toron", "../wendelon"] {
		assert!(
			!manifest.contains(forbidden),
			"observability manifest must not depend on {forbidden}"
		);
	}
}

#[test]
fn adr065_observability_has_no_quinn_raft_sql_wire_tls_or_router_dependency() {
	let manifest = observability_manifest();
	let source = format!(
		"{}\n{}",
		observability_http_source_text(),
		observability_audit_source_text()
	);
	for forbidden in [
		"quinn",
		"openraft",
		"sql_wire",
		"SqlMessage",
		"router",
		"Router",
	] {
		assert!(
			!manifest.contains(forbidden),
			"manifest contains ring dependency {forbidden}"
		);
	}
	for forbidden in [
		"quinn",
		"openraft",
		"sql_wire",
		"SqlMessage",
		"tls-pki",
		"tls_pki",
		"rustls",
		"router",
		"Router",
	] {
		assert!(!source.contains(forbidden), "source contains {forbidden}");
	}
}

#[test]
fn adr065_wendelon_consumes_observability_audit() {
	let manifest = std::fs::read_to_string(wendelon_manifest_dir().join("Cargo.toml"))
		.expect("read Wendelon manifest");
	let audit = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(manifest.contains("\"audit\""));
	assert!(audit.contains("use observability::audit as shared_audit;"));
	assert!(audit.contains("shared_audit::build_audit_sink"));
	assert!(audit.contains("shared_audit::LocalAuditSink::open_with_archive"));
}

#[test]
fn adr065_wendelon_payload_remains_ring_owned_and_typed() {
	let audit = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	for required in [
		"pub enum AuditOperation",
		"pub enum AuditSchema",
		"pub struct AdministrativeControlSchema",
		"pub struct LocalSecuritySchema",
		"pub struct DataAccessSchema",
		"pub fn from_actor_identity",
	] {
		assert!(
			audit.contains(required),
			"Wendelon audit missing {required}"
		);
	}
	for forbidden in ["pub enum ToronPayload", "pub struct ToronPayload"] {
		assert!(!audit.contains(forbidden));
	}
}

#[test]
fn adr065_wendelon_audit_serialization_is_preserved() {
	let audit = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(audit.contains("serialize_struct(\"AuditEvent\", 13)"));
	assert!(audit.contains("event.serialize_field(\"authority_model\", &self.authority_model())"));
	assert!(audit.contains("event.serialize_field(\"operation\", &self.operation)"));
	assert!(audit.contains("serde_json::to_value(schema)"));
	assert!(audit.contains("identity.as_bytes()"));
}

#[test]
fn adr065_wendelon_audit_verify_cli_uses_shared_verifier() {
	let cli = std::fs::read_to_string(wendelon_manifest_dir().join("src/cli.rs"))
		.expect("read Wendelon CLI");
	let audit = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(cli.contains("use crate::audit::verify_local_audit_evidence"));
	assert!(audit.contains("shared_audit::verify_local_audit_evidence"));
}

#[test]
fn adr065_local_ndjson_chain_sink_is_in_observability_audit() {
	let source = observability_audit_source_text();
	assert!(source.contains("pub struct LocalAuditSink"));
	assert!(source.contains("OpenOptions::new().create(true).append(true).open"));
	assert!(source.contains("serde_json::to_string(&persisted)"));
	assert!(source.contains("sync_data()"));
}

#[test]
fn adr065_audit_sink_trait_is_in_observability_audit() {
	let source = observability_audit_source_text();
	assert!(source.contains("pub trait AuditSink: Send + Sync"));
	assert!(source.contains("fn emit(&self, event: AuditEvent)"));
	assert!(source.contains("fn chain_head(&self)"));
}

#[test]
fn adr065_audit_format_literals_are_consumer_provided() {
	let shared = observability_audit_source_text();
	let wendelon = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(shared.contains("pub struct AuditFormat"));
	for literal in [
		"wendelon-audit-genesis-v1",
		"wendelon-audit-chain-root-v1",
		"wendelon@47450",
	] {
		assert!(
			!shared.contains(literal),
			"shared audit must not hard-code Wendelon literal {literal}"
		);
		assert!(
			wendelon.contains(literal),
			"Wendelon adapter must provide literal {literal}"
		);
	}
}

#[test]
fn adr065_kafka_sink_remains_feature_gated() {
	let manifest = observability_manifest();
	let source = observability_audit_source_text();
	assert!(manifest.contains(r#"kafka = ["audit", "dep:rdkafka"]"#));
	assert!(source.contains("#[cfg(feature = \"kafka\")]"));
	assert!(source.contains("pub trait KafkaProducer"));
}

#[test]
fn adr065_toron_audit_consumption_keeps_shared_substrate_policy_free() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read Toron config");
	let shared = observability_audit_source_text();
	assert!(manifest.contains("features = [\"http\", \"audit\"]"));
	assert!(config.contains("[audit]"));
	assert!(manifest.contains("default-features = false"));
	for forbidden in [
		"tokio::sync::mpsc",
		"try_send",
		"ToronAuditEvent",
		"ToronAuditOperation",
		"SqlErrorCode",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared audit substrate gained Toron policy token {forbidden}"
		);
	}
}

#[test]
fn adr065_no_async_try_send_policy_in_observability_audit() {
	let source = observability_audit_source_text();
	for forbidden in ["tokio::sync::mpsc", "try_send", "spawn(", "async fn"] {
		assert!(
			!source.contains(forbidden),
			"shared audit contains async policy {forbidden}"
		);
	}
}

#[test]
fn adr065_audit_schema_version_is_preserved() {
	let wendelon = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(wendelon.contains("const AUDIT_SCHEMA_VERSION: u16 = 4;"));
	assert!(wendelon.contains("schema_version: AUDIT_SCHEMA_VERSION"));
}

#[test]
fn adr065_observability_namespace_reserves_future_telemetry_facet() {
	let adr = std::fs::read_to_string(
		manifest_dir().join("plans/adr/065_observability_namespace_audit_substrate.md"),
	)
	.expect("read ADR 065");
	let lib = std::fs::read_to_string(workspace_root_dir().join("crates/observability/src/lib.rs"))
		.expect("read observability lib");
	assert!(adr.contains("observability::telemetry"));
	assert!(lib.contains("#[cfg(feature = \"telemetry\")]"));
	assert!(lib.contains("pub mod telemetry"));
}

#[test]
fn adr065_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	for forbidden in ["tokio", "rustls", "observability", "tls-pki", "serde_json"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr065_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");
	assert!(!worker.contains("observability"));
	assert!(!worker.contains("audit"));
}

#[test]
fn adr065_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	let source = format!(
		"{}\n{}",
		observability_http_source_text(),
		observability_audit_source_text()
	);
	for forbidden in [
		"ToronAudit",
		"WendelonAudit",
		"NilketAudit",
		"AuditEventV1",
		"AuditSinkV1",
		"ObservabilityV1",
		"TelemetryV1",
	] {
		assert!(
			!source.contains(forbidden),
			"observability surface contains forbidden branded/V1 token {forbidden}"
		);
	}
}

#[test]
fn adr065_prior_adr045_to_064_guards_continue_to_pass_except_adr048_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");
	for prefix in [
		"adr045_", "adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_",
		"adr053_", "adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_",
		"adr061_", "adr062_", "adr063_", "adr064_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains("adr065_flat_observability_http_runtime_crate_removed"));
}

#[test]
fn adr066_toron_consumes_observability_audit_without_kafka_default() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(manifest.contains("observability = { path = \"../observability\""));
	assert!(manifest.contains("default-features = false"));
	assert!(manifest.contains("features = [\"http\", \"audit\"]"));
	assert!(!manifest.contains("\"kafka\""));
	assert!(audit.contains("use observability::audit as shared_audit;"));
	assert!(audit.contains("shared_audit::build_audit_sink"));
	assert!(audit.contains("toron_audit_format()"));
}

#[test]
fn adr066_audit_config_is_default_disabled_and_enabled_requires_identity_path_queue() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("pub enum AuditConfig"));
	assert!(config.contains("Disabled"));
	assert!(config.contains("Enabled(Box<AuditEnabledConfig>)"));
	assert!(config.contains("enabled: bool"));
	assert!(config.contains("return Ok(AuditConfig::Disabled);"));
	for required in [
		"audit.node_id is required when audit is enabled",
		"audit.node_numeric_id is required when audit is enabled",
		"audit.local_path is required when audit is enabled",
		"audit.queue_capacity is required when audit is enabled",
	] {
		assert!(
			config.contains(required),
			"missing required validator {required}"
		);
	}
}

#[test]
fn adr066_toron_audit_node_id_is_typed_and_not_uuid_or_bare_numeric() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(audit.contains("const TORON_NODE_ID_PREFIX: &str = \"toron-n-\""));
	assert!(audit.contains("parse_toron_audit_node_numeric"));
	assert!(audit.contains("strip_prefix(TORON_NODE_ID_PREFIX)"));
	assert!(config_test.contains("audit_config_rejects_bare_numeric_and_uuid_node_ids"));
	assert!(config_test.contains("audit_config_rejects_numeric_mismatch_and_zero_queue"));
}

#[test]
fn adr066_audit_payload_is_toron_owned_and_typed() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for required in [
		"pub enum ToronAuditOperation",
		"pub enum ToronAuditEventClass",
		"pub enum StatementClass",
		"pub enum DurationBucket",
		"pub enum RowCountBucket",
		"pub enum SqlErrorCodeClass",
		"pub struct ToronAuditEvent",
		"fn to_shared(&self, node_id: &ToronAuditNodeId)",
	] {
		assert!(
			audit.contains(required),
			"missing typed audit surface {required}"
		);
	}
	assert!(!audit.contains("pub type ToronAuditEvent = shared_audit::AuditEvent"));
}

#[test]
fn adr066_event_taxonomy_covers_lifecycle_edge_denial_and_query_outcomes() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for operation in [
		"toron.lifecycle.startup",
		"toron.lifecycle.shutdown",
		"toron.audit.sink_failure",
		"toron.sql.edge.connection_limit_denied",
		"toron.sql.edge.tenant_identity_denied",
		"toron.sql.edge.request_frame_too_large",
		"toron.sql.edge.statement_too_large",
		"toron.sql.edge.protocol_mismatch",
		"toron.sql.edge.unexpected_client_frame",
		"toron.sql.query.completed",
		"toron.sql.query.planned_error",
		"toron.sql.query.execution_error",
		"toron.sql.query.cancelled",
		"toron.sql.query.timed_out",
		"toron.sql.query.disconnect_aborted",
	] {
		assert!(audit.contains(operation), "missing operation {operation}");
	}
}

#[test]
fn adr066_emitter_is_bounded_try_send_fail_open() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("tokio::sync::{mpsc, oneshot}"));
	assert!(audit.contains("mpsc::channel(config.queue_capacity.get())"));
	assert!(audit.contains("tx.try_send(event)"));
	assert!(audit.contains("TrySendError::Full"));
	assert!(audit.contains("TrySendError::Closed"));
	assert!(!audit.contains(".send(event).await"));
	assert!(audit.contains("ToronAuditCommand::fire_and_forget(event)"));
	assert!(audit.contains("record_drop(metrics, event.event(), ToronAuditDropReason::QueueFull)"));
}

#[test]
fn adr066_disabled_audit_has_no_metrics_or_readiness_effect() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability test");

	assert!(daemon.contains("let audit_metrics = config"));
	assert!(daemon.contains(".is_enabled()\n\t\t\t.then"));
	assert!(observability.contains("let Some(metrics) = &self.audit_metrics else"));
	assert!(
		observability_test.contains("audit_metrics_are_disabled_until_audit_config_is_enabled")
	);
	assert!(
		!observability_test.contains("audit_metrics: Some(Arc::new(ToronAuditMetrics::default()))")
	);
}

#[test]
fn adr066_readiness_degrades_only_on_local_chain_open_or_write_failure() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("audit_local_chain_unavailable"));
	assert!(audit.contains("audit_local_chain_write_failed"));
	assert!(audit.contains("set_degraded_reason"));
	let queue_full_block = audit
		.split("TrySendError::Full")
		.nth(1)
		.expect("queue full branch");
	assert!(
		!queue_full_block
			.lines()
			.take(8)
			.any(|line| line.contains("set_degraded_reason"))
	);
}

#[test]
fn adr066_sql_edge_denials_emit_audit_without_sql_frame_drift() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");

	for required in [
		"SqlConnectionLimitDenied",
		"SqlTenantIdentityDenied",
		"SqlRequestFrameTooLarge",
		"SqlStatementTooLarge",
		"SqlProtocolMismatch",
		"SqlUnexpectedClientFrame",
	] {
		assert!(
			daemon.contains(required),
			"missing daemon audit edge {required}"
		);
	}
	assert!(!wire.contains("Audit"));
	assert!(!wire.contains("AuditEvent"));
	assert!(!wire.contains("protocol_version: SqlProtocolVersion::V"));
}

#[test]
fn adr066_query_outcomes_emit_after_outcome_is_known() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	for required in [
		"QueryCompleted",
		"QueryPlannedError",
		"QueryExecutionError",
		"QueryCancelled",
		"QueryTimedOut",
		"QueryDisconnectAborted",
		".with_row_count(row_count)",
		".with_duration(query_started.elapsed())",
	] {
		assert!(
			daemon.contains(required),
			"missing query audit path {required}"
		);
	}
	let completion_index = daemon.find("QueryCompleted").expect("completed audit");
	let row_count_index = daemon
		.find("let row_count = if returns_rows")
		.expect("row count");
	assert!(row_count_index < completion_index);
}

#[test]
fn adr066_audit_payload_privacy_has_only_bounded_fields() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	for allowed in [
		"event_class",
		"session_id",
		"query_id",
		"statement_class",
		"sql_error_code_class",
		"duration_bucket",
		"row_count_bucket",
	] {
		assert!(audit.contains(&format!("with_parameter(\"{allowed}\"")));
	}
	for forbidden in [
		"with_parameter(\"sql",
		"with_parameter(\"statement",
		"with_parameter(\"row",
		"with_parameter(\"arrow",
		"with_parameter(\"table",
		"with_parameter(\"descriptor",
		"with_parameter(\"peer",
		"with_parameter(\"cert",
		"with_parameter(\"san",
		"with_parameter(\"error",
		"with_parameter(\"plan",
	] {
		if forbidden == "with_parameter(\"statement" {
			assert!(audit.contains("with_parameter(\"statement_class\""));
		} else if forbidden == "with_parameter(\"row" {
			assert!(audit.contains("with_parameter(\"row_count_bucket\""));
		} else if forbidden == "with_parameter(\"sql" {
			assert!(audit.contains("with_parameter(\"sql_error_code_class\""));
		} else if forbidden == "with_parameter(\"error" {
			assert!(audit.contains("with_parameter(\"error_class\""));
		} else {
			assert!(
				!audit.contains(forbidden),
				"audit payload leaked {forbidden}"
			);
		}
	}
	assert!(daemon.contains("planner.plan(&request.statement)"));
	assert!(!audit.contains("request.statement"));
}

#[test]
fn adr066_audit_metrics_are_aggregate_and_identity_free() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let tests =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	for family in [
		"toron_audit_events_total",
		"toron_audit_dropped_total",
		"toron_audit_sink_errors_total",
		"toron_audit_security_event_unrecorded_total",
	] {
		assert!(observability.contains(family), "missing metric {family}");
	}
	for forbidden in [
		"tenant_id",
		"query_id",
		"session_id",
		"table",
		"descriptor",
		"sql_text",
	] {
		let offenders = observability
			.lines()
			.filter(|line| line.contains("toron_audit_") && line.contains(forbidden))
			.collect::<Vec<_>>();
		assert!(
			offenders.is_empty(),
			"audit metric gained forbidden label {forbidden}: {offenders:?}"
		);
	}
	assert!(tests.contains("audit_metrics_are_aggregate_only"));
}

#[test]
fn adr066_security_unrecorded_counts_queue_closed_and_sink_errors() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for required in [
		"ToronAuditSecurityEventUnrecordedReason",
		"QueueFull",
		"EmitterClosed",
		"SinkError",
		"record_security_unrecorded",
		"security_relevant()",
	] {
		assert!(
			audit.contains(required),
			"missing security unrecorded path {required}"
		);
	}
	assert!(audit.contains("unavailable_emitter_records_security_drop_without_panic"));
}

#[test]
fn adr066_archive_and_syslog_are_optional_but_kafka_is_not_configured_by_toron() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("archive: Option<RawAuditArchiveConfig>"));
	assert!(config.contains("syslog: Option<RawAuditSyslogConfig>"));
	assert!(config.contains("ArchiveAuditConfig"));
	assert!(config.contains("SyslogAuditConfig"));
	assert!(config.contains("kafka: None"));
	assert!(!config.contains("RawAuditKafkaConfig"));
}

#[test]
fn adr066_shared_observability_audit_remains_policy_free() {
	let shared = observability_audit_source_text();

	for forbidden in [
		"ToronAudit",
		"SqlErrorCode",
		"SqlMessage",
		"QueryId",
		"crate::storage::TenantId",
		"tokio::sync::mpsc",
		"try_send",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared audit gained Toron policy or async policy {forbidden}"
		);
	}
}

#[test]
fn adr066_no_wendelon_nilket_fixture_worker_or_sql_wire_runtime_change() {
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

	assert!(!sql_wire.contains("Audit"));
	assert!(!wendelon_source.contains("ToronAudit"));
	assert!(!nilket_manifest.contains("observability"));
	assert!(!worker.contains("Audit"));
	assert!(!worker.contains("audit"));
}

#[test]
fn adr066_fixtures_cover_config_metrics_emitter_and_sql_paths() {
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");
	let sql_test = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for fixture in [
		"audit_config_enabled_accepts_toron_node_id_and_resolves_paths",
		"audit_config_rejects_bare_numeric_and_uuid_node_ids",
		"audit_config_rejects_numeric_mismatch_and_zero_queue",
		"audit_metrics_are_disabled_until_audit_config_is_enabled",
		"audit_metrics_are_aggregate_only",
		"unavailable_emitter_records_security_drop_without_panic",
		"toron_sql_select_from_bootstrapped_descriptor_executes_over_wire",
		"oversized_declared_request_length_rejected_before_allocation_or_decode",
		"oversized_decoded_execute_statement_returns_resource_exhausted_without_planning",
		"audit_records_planned_error_without_raw_sql",
		"cancel_below_request_frame_cap_is_not_statement_capped",
	] {
		let haystack = format!("{config_test}\n{observability_test}\n{sql_test}\n{audit}");
		assert!(haystack.contains(fixture), "missing fixture {fixture}");
	}
}

#[test]
fn adr066_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in [
		"tokio",
		"quinn",
		"rustls",
		"observability",
		"tls-pki",
		"serde_json",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr066_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("ToronAudit"));
	assert!(!worker.contains("observability"));
	assert!(!worker.contains("[audit]"));
}

#[test]
fn adr066_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	let shared = observability_audit_source_text();
	for forbidden in [
		"ToronAudit",
		"ToronAuditEvent",
		"ToronAuditV1",
		"AuditEmitterV1",
		"AuditConsumerV1",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared observability surface contains forbidden Toron/V1 token {forbidden}"
		);
	}
	let toron_audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	assert!(!toron_audit.contains("V1"));
}

#[test]
fn adr066_prior_adr045_to_065_guards_continue_to_pass_except_audit_feature_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr045_", "adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_",
		"adr053_", "adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_",
		"adr061_", "adr062_", "adr063_", "adr064_", "adr065_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr067_observability_telemetry_module_exists_and_is_feature_gated() {
	let lib = std::fs::read_to_string(workspace_root_dir().join("crates/observability/src/lib.rs"))
		.expect("read observability lib");
	assert!(lib.contains("#[cfg(feature = \"telemetry\")]"));
	assert!(lib.contains("pub mod telemetry;"));
	assert!(
		workspace_root_dir()
			.join("crates/observability/src/telemetry/mod.rs")
			.exists()
	);
}

#[test]
fn adr067_telemetry_feature_is_independent_of_http_and_audit() {
	let manifest = observability_manifest();
	let source = observability_telemetry_source_text();
	assert!(manifest.contains("telemetry = []"));
	assert!(manifest.contains(r#"http = ["dep:tokio"]"#));
	assert!(!manifest.contains(r#"telemetry = ["http""#));
	assert!(!manifest.contains(r#"telemetry = ["audit""#));
	for forbidden in [
		"observability::http",
		"observability::audit",
		"crate::http",
		"crate::audit",
		"super::http",
		"super::audit",
	] {
		assert!(
			!source.contains(forbidden),
			"telemetry references {forbidden}"
		);
	}
}

#[test]
fn adr067_otlp_http_and_grpc_features_are_non_default() {
	let manifest = observability_manifest();
	assert!(manifest.contains("default = []"));
	assert!(manifest.contains("otlp-http = ["));
	assert!(manifest.contains("otlp-grpc = ["));
	for required in [
		"dep:reqwest",
		"dep:h2",
		"dep:http",
		"dep:tokio-rustls",
		"dep:tls-pki",
	] {
		assert!(
			manifest.contains(required),
			"missing OTLP feature dep {required}"
		);
	}
}

#[test]
fn adr067_wendelon_otlp_features_delegate_to_observability() {
	let manifest = std::fs::read_to_string(wendelon_manifest_dir().join("Cargo.toml"))
		.expect("read Wendelon manifest");
	assert!(manifest.contains(r#"otlp-http = ["observability/otlp-http"]"#));
	assert!(manifest.contains(r#"otlp-grpc = ["observability/otlp-grpc"]"#));
	assert!(manifest.contains(r#"features = ["http", "audit", "telemetry"]"#));
	assert!(!manifest.contains(r#"otlp-http = []"#));
	assert!(!manifest.contains(r#""dep:h2""#));
	assert!(!manifest.contains(r#""dep:http""#));
}

#[test]
fn adr067_wendelon_build_dispatch_uses_observability_telemetry() {
	let telemetry =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/telemetry.rs"))
			.expect("read Wendelon telemetry");
	assert!(telemetry.contains("use observability::telemetry as shared_telemetry;"));
	assert!(telemetry.contains("shared_telemetry::OtlpTraceLayer::new"));
	assert!(telemetry.contains("fn build_dispatch("));
	assert!(!telemetry.contains("mod otlp"));
	assert!(!telemetry.contains("fn encode_export_trace_service_request"));
}

#[test]
fn adr067_wendelon_otlp_behavior_fixtures_remain_present() {
	for fixture in [
		"tests/otlp_http_export.rs",
		"tests/otlp_grpc_export.rs",
		"tests/config_loader.rs",
	] {
		assert!(
			wendelon_manifest_dir().join(fixture).exists(),
			"missing Wendelon fixture {fixture}"
		);
	}
	let http = std::fs::read_to_string(wendelon_manifest_dir().join("tests/otlp_http_export.rs"))
		.expect("read OTLP HTTP test");
	let grpc = std::fs::read_to_string(wendelon_manifest_dir().join("tests/otlp_grpc_export.rs"))
		.expect("read OTLP gRPC test");
	assert!(http.contains("telemetry::build_dispatch(Some(&trace_cfg), 7)"));
	assert!(grpc.contains("telemetry::build_dispatch(Some(&trace_cfg), 8)"));
	assert!(grpc.contains("telemetry::build_dispatch(Some(&trace_cfg), 9)"));
}

#[test]
fn adr067_shared_telemetry_has_no_wendelon_or_toron_imports() {
	let source = observability_telemetry_source_text();
	for forbidden in [
		"wendelon",
		"Wendelon",
		"toron",
		"Toron",
		"crate::metrics",
		"crate::network",
		"crate::storage",
	] {
		assert!(
			!source.contains(forbidden),
			"shared telemetry contains ring token {forbidden}"
		);
	}
}

#[test]
fn adr067_wendelon_policy_literals_remain_ring_owned() {
	let shared = observability_telemetry_source_text();
	let telemetry =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/telemetry.rs"))
			.expect("read Wendelon telemetry");
	for literal in [
		"WENDELON_LOG",
		"wendelon.traces",
		"service.name",
		"service.version",
		"wendelon.node_id",
	] {
		assert!(
			telemetry.contains(literal),
			"Wendelon missing policy literal {literal}"
		);
		assert!(
			!shared.contains(literal),
			"shared telemetry owns Wendelon policy literal {literal}"
		);
	}
	assert!(telemetry.contains("TelemetryProfile::new"));
}

#[test]
fn adr067_wendelon_dropped_span_metric_name_remains_ring_owned() {
	let shared = observability_telemetry_source_text();
	let telemetry =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/telemetry.rs"))
			.expect("read Wendelon telemetry");
	assert!(telemetry.contains("otlp_spans_dropped_total"));
	assert!(!shared.contains("otlp_spans_dropped_total"));
	assert!(telemetry.contains("WendelonDroppedSpanObserver"));
}

#[test]
fn adr067_w3c_trace_context_validation_is_shared() {
	let shared = observability_telemetry_source_text();
	let compute =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/integration/compute.rs"))
			.expect("read Wendelon compute integration");
	assert!(shared.contains("pub struct TraceContext"));
	assert!(shared.contains("pub fn validate_traceparent"));
	assert!(shared.contains("pub fn validate_tracestate"));
	assert!(compute.contains("use observability::telemetry as shared_telemetry;"));
	assert!(compute.contains("shared_telemetry::TraceContext"));
	assert!(compute.contains("shared_telemetry::validate_traceparent(traceparent)"));
}

#[test]
fn adr067_compute_trace_context_v1_is_preserved_as_compatibility_surface() {
	let compute =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/integration/compute.rs"))
			.expect("read Wendelon compute integration");
	assert!(compute.contains("pub struct ComputeTraceContextV1"));
	assert!(compute.contains("impl ComputeTraceContextV1"));
	assert!(compute.contains("shared_telemetry::TraceContext"));
	assert!(
		compute.contains("pub const TRACEPARENT_LEN: usize = shared_telemetry::TRACEPARENT_LEN;")
	);
}

#[test]
fn adr067_queryid_to_traceid_mapping_is_absent() {
	let shared = observability_telemetry_source_text();
	for forbidden in [
		"QueryId",
		"query_id",
		"tenant_id",
		"session_id",
		"trace_id_from_query",
		"TraceId::from",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared telemetry gained identity mapping token {forbidden}"
		);
	}
}

#[test]
fn adr067_otlp_queue_remains_bounded_try_send_fail_open() {
	let shared = observability_telemetry_source_text();
	for required in [
		"mpsc::channel(cfg.max_queue_size)",
		"tx.try_send(ExportCommand::Span(completed))",
		"TrySendError::Full",
		"TrySendError::Closed",
		"record_dropped_span",
	] {
		assert!(
			shared.contains(required),
			"missing bounded queue token {required}"
		);
	}
	assert!(!shared.contains(".send(ExportCommand::Span"));
}

#[test]
fn adr067_otlp_shutdown_handle_remains_explicit() {
	let shared = observability_telemetry_source_text();
	let telemetry =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/telemetry.rs"))
			.expect("read Wendelon telemetry");
	assert!(shared.contains("pub struct OtlpShutdownHandle"));
	assert!(shared.contains("pub async fn shutdown(self)"));
	assert!(telemetry.contains("pub struct TelemetryHandle"));
	assert!(telemetry.contains("shutdown: Option<shared_telemetry::OtlpShutdownHandle>"));
}

#[test]
fn adr067_http_and_grpc_export_paths_remain_feature_gated() {
	let shared = observability_telemetry_source_text();
	for required in [
		"#[cfg(feature = \"otlp-http\")]",
		"#[cfg(feature = \"otlp-grpc\")]",
		"const OTLP_HTTP_TRACES_PATH: &str = \"/v1/traces\";",
		"/opentelemetry.proto.collector.trace.v1.TraceService/Export",
		"application/x-protobuf",
		"application/grpc",
		"te\", \"trailers",
	] {
		assert!(
			shared.contains(required),
			"missing OTLP protocol token {required}"
		);
	}
}

#[test]
fn adr067_no_new_opentelemetry_sdk_or_tonic_dependency() {
	let manifests = workspace_cargo_tomls()
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read manifest"))
		.collect::<Vec<_>>()
		.join("\n");
	for forbidden in [
		"opentelemetry-otlp",
		"opentelemetry_sdk",
		"opentelemetry =",
		"tonic",
		"prost =",
	] {
		assert!(
			!manifests.contains(forbidden),
			"manifest gained {forbidden}"
		);
	}
}

#[test]
fn adr067_metrics_http_exporter_is_not_moved_as_otlp() {
	let exporter = std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/exporter.rs"))
		.expect("read Wendelon metrics exporter");
	let shared = observability_telemetry_source_text();
	assert!(exporter.contains("METRICS_PATH"));
	assert!(exporter.contains("\"/metrics\""));
	assert!(exporter.contains("MetricsExporterHandle"));
	assert!(!shared.contains("METRICS_PATH"));
	assert!(!shared.contains("MetricsExporterHandle"));
}

#[test]
fn adr067_no_toron_telemetry_config_or_exporter_yet() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read Toron config");
	let source = source_files(&manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Toron source"))
		.collect::<Vec<_>>()
		.join("\n");
	assert!(source.contains("pub struct TaskTelemetry"));
	assert!(manifest.contains(r#"telemetry = ["observability/telemetry"]"#));
	assert!(config.contains("[observability.telemetry]"));
	assert!(config.contains("RawTelemetryConfig"));
	assert!(source.contains("OtlpTraceLayer"));
}

#[test]
fn adr067_no_audit_http_sql_wire_worker_or_storage_behavior_change() {
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read Toron storage");
	assert!(!sql_wire.contains("Otlp"));
	assert!(!sql_wire.contains("TraceExport"));
	assert!(!worker.contains("Otlp"));
	assert!(!worker.contains("TraceExport"));
	assert!(!storage.contains("Otlp"));
	assert!(!storage.contains("TraceExport"));
	adr065_observability_http_does_not_reference_audit();
	adr065_observability_audit_does_not_reference_http();
}

#[test]
fn adr067_prior_adr065_066_observability_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");
	for prefix in ["adr065_", "adr066_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(
			count > 0,
			"missing prior observability guard prefix {prefix}"
		);
	}
}

#[test]
fn adr067_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	for forbidden in [
		"tokio",
		"quinn",
		"rustls",
		"observability",
		"tls-pki",
		"reqwest",
		"h2",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr067_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");
	assert!(!worker.contains("observability"));
	assert!(!worker.contains("Otlp"));
	assert!(!worker.contains("TraceExport"));
	assert!(!worker.contains("traceparent"));
}

#[test]
fn adr067_new_shared_surface_is_brand_clean_and_has_no_v1_suffix() {
	let shared = observability_telemetry_source_text();
	for forbidden in [
		"Wendelon",
		"Toron",
		"Nilket",
		"TraceContextV1",
		"TelemetryProfileV1",
		"OtlpTraceLayerV1",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared telemetry surface contains forbidden brand/V1 token {forbidden}"
		);
	}
}

#[test]
fn adr067_wendelon_v1_trace_context_suffix_is_compatibility_scoped() {
	let shared = observability_telemetry_source_text();
	let compute =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/integration/compute.rs"))
			.expect("read Wendelon compute integration");
	assert!(!shared.contains("V1"));
	assert!(compute.contains("pub struct ComputeTraceContextV1"));
	assert!(compute.contains("shared_telemetry::TraceContext"));
}

#[test]
fn adr067_prior_adr045_to_066_guards_continue_to_pass_except_otlp_feature_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");
	for prefix in [
		"adr045_", "adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_",
		"adr053_", "adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_",
		"adr061_", "adr062_", "adr063_", "adr064_", "adr065_", "adr066_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains("adr067_wendelon_otlp_features_delegate_to_observability"));
}

#[test]
fn adr068_toron_telemetry_features_are_default_disabled() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	assert!(manifest.contains("[features]"));
	assert!(manifest.contains("default = []"));
	assert!(manifest.contains(r#"telemetry = ["observability/telemetry"]"#));
	assert!(manifest.contains(r#"otlp-http = ["telemetry", "observability/otlp-http"]"#));
	assert!(manifest.contains(r#"otlp-grpc = ["telemetry", "observability/otlp-grpc"]"#));
}

#[test]
fn adr068_toron_otlp_features_delegate_to_observability() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	assert!(manifest.contains(r#"observability = { path = "../observability", default-features = false, features = ["http", "audit"] }"#));
	assert!(manifest.contains(r#"otlp-http = ["telemetry", "observability/otlp-http"]"#));
	assert!(manifest.contains(r#"otlp-grpc = ["telemetry", "observability/otlp-grpc"]"#));
	assert!(!manifest.contains(r#""dep:h2""#));
	assert!(!manifest.contains(r#""dep:http""#));
}

#[test]
fn adr068_observability_telemetry_config_is_default_absent_noop() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read Toron config");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	assert!(config.contains("pub telemetry: Option<TelemetryConfig>"));
	assert!(config.contains("telemetry: None"));
	assert!(config.contains("# [observability.telemetry]"));
	assert!(daemon.contains("if config.observability.telemetry.is_none()"));
}

#[test]
fn adr068_telemetry_config_uses_toron_raw_config_surface() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read Toron config");
	assert!(config.contains("struct RawTelemetryConfig"));
	assert!(config.contains("fn normalize_telemetry_config("));
	assert!(config.contains("fn normalize_telemetry_protocol("));
	assert!(!config.contains("TraceExportConfig"));
}

#[test]
fn adr068_enabled_telemetry_requires_successful_subscriber_install() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(daemon.contains("config.observability.telemetry.is_some()"));
	assert!(daemon.contains("Some(crate::telemetry::install_global("));
	assert!(daemon.contains("Arc::clone(&metrics)"));
	assert!(telemetry.contains("tracing::dispatcher::set_global_default(dispatch).map_err"));
	assert!(telemetry.contains("global tracing subscriber already installed"));
}

#[test]
fn adr068_toron_telemetry_wrapper_uses_observability_telemetry() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("use observability::telemetry as shared_telemetry;"));
	assert!(telemetry.contains("shared_telemetry::OtlpTraceLayer::new"));
	assert!(telemetry.contains("shared_trace_config"));
	assert!(!telemetry.contains("mod otlp"));
	assert!(!telemetry.contains("fn encode_export_trace_service_request"));
}

#[test]
fn adr068_toron_policy_literals_are_ring_owned() {
	let shared = observability_telemetry_source_text();
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	for literal in [
		"TORON_LOG",
		"toron.traces",
		"toron.node_id",
		"toron_otlp_spans_dropped_total",
	] {
		assert!(telemetry.contains(literal), "Toron missing {literal}");
		assert!(!shared.contains(literal), "shared telemetry owns {literal}");
	}
	assert!(telemetry.contains("\"service.name\", \"toron\""));
	assert!(telemetry.contains("\"service.version\""));
}

#[test]
fn adr068_toron_node_id_matches_toron_n_u32_identity() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("parse_toron_audit_node_numeric(&audit.node_id)"));
	assert!(telemetry.contains("parsed != audit.node_numeric_id"));
	assert!(telemetry.contains("\"toron.node_id\""));
}

#[test]
fn adr068_trace_id_is_derived_blake3_not_literal_query_id() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("const TRACE_ID_DOMAIN: &[u8] = b\"toron.trace.v1\";"));
	assert!(telemetry.contains("let mut hasher = blake3::Hasher::new();"));
	assert!(telemetry.contains("hasher.update(query_id.get().as_bytes())"));
	assert!(telemetry.contains("nonzero_trace_id(trace_id)"));
	assert!(!telemetry.contains("Uuid::from_u128"));
	assert!(!telemetry.contains("trace_id = *query_id.get().as_bytes()"));
}

#[test]
fn adr068_query_id_is_not_exported_as_span_attribute() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let production = telemetry.split("#[cfg(test)]").next().unwrap_or(&telemetry);
	for forbidden in ["query_id =", "toron.query_id", "query.id", "query_id = %"] {
		assert!(
			!production.contains(forbidden),
			"query id span attr marker found: {forbidden}"
		);
	}
}

#[test]
fn adr068_sampling_is_head_based_deterministic_and_query_root_only() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let production = telemetry.split("#[cfg(test)]").next().unwrap_or(&telemetry);
	assert!(telemetry.contains("pub fn head_sample("));
	assert!(telemetry.contains("u64::from_be_bytes"));
	assert!(telemetry.contains("head % TRACE_SAMPLING_DENOMINATOR < u64::from(rate)"));
	assert_eq!(production.matches("head_sample(").count(), 2);
	assert!(telemetry.contains("pub fn query_trace_context(query_id: QueryId)"));
}

#[test]
fn adr068_explicit_sampled_field_is_authoritative() {
	let shared = observability_telemetry_source_text();
	assert!(shared.contains("pub const OTEL_SAMPLED_FIELD"));
	assert!(shared.contains("ExplicitSamplingContext::Unsampled"));
	assert!(shared.contains("ExplicitSamplingContext::Sampled(context)"));
	assert!(shared.contains("unwrap_or_else(|| self.should_sample_root_span())"));
	assert!(shared.contains("return ExplicitSamplingContext::Unsampled;"));
}

#[test]
fn adr068_worker_never_resamples_absent_trace_context() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	assert!(telemetry.contains("fn unsampled_task_span"));
	assert!(telemetry.contains("otel.sampled = false"));
	assert!(server.contains("crate::telemetry::task_trace_context(spec)"));
	assert!(!server.contains("head_sample("));
}

#[test]
fn adr068_external_context_fields_are_shared_ring_neutral_and_stripped() {
	let shared = observability_telemetry_source_text();
	for constant in [
		"OTEL_TRACE_ID_FIELD",
		"OTEL_SPAN_ID_FIELD",
		"OTEL_PARENT_SPAN_ID_FIELD",
		"OTEL_SAMPLED_FIELD",
	] {
		assert!(shared.contains(constant), "missing {constant}");
	}
	assert!(shared.contains("self.explicit_context.record(field.name(), &value)"));
	assert!(shared.contains("return;"));
	assert!(!shared.contains("Toron"));
}

#[test]
fn adr068_propagation_uses_task_execution_context_trace_context() {
	let context = std::fs::read_to_string(manifest_dir().join("src/distributed/context.rs"))
		.expect("read context source");
	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	assert!(context.contains("pub trace_context: Option<TraceContext>"));
	assert!(context.contains("context.trace_context"));
	assert!(scheduler.contains("spec.context.trace_context = Some(trace_context);"));
	assert!(server.contains("spec.context.trace_context"));
}

#[test]
fn adr068_taskspec_trace_is_not_a_live_carrier() {
	let task = std::fs::read_to_string(manifest_dir().join("src/distributed/task.rs"))
		.expect("read task source");
	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	assert!(task.contains("pub trace: Option<TraceContext>"));
	assert!(!scheduler.contains(".trace ="));
	assert!(!server.contains("spec.trace"));
}

#[test]
fn adr068_boundary_span_ids_are_toron_derived_nonzero() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	for domain in [
		"QUERY_SPAN_DOMAIN",
		"DISPATCH_SPAN_DOMAIN",
		"TASK_SPAN_DOMAIN",
		"STORAGE_SCAN_SPAN_DOMAIN",
	] {
		assert!(telemetry.contains(domain), "missing {domain}");
	}
	assert!(telemetry.contains("fn task_identity_span_id("));
	assert!(telemetry.contains("fn nonzero_span_id("));
	assert!(telemetry.contains("span_id[7] = 1"));
}

#[test]
fn adr068_worker_task_spans_attach_raw_task_telemetry_with_bounded_labels() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("pub fn record_task_telemetry"));
	for field in [
		"toron.task.pulled_shuffle_bytes",
		"toron.task.build_rows",
		"toron.task.emitted_rows",
		"toron.task.task_wall_micros",
		"toron.task.framework_overhead_micros",
	] {
		assert!(telemetry.contains(field), "missing task field {field}");
	}
	assert!(telemetry.contains("fn worker_count_bucket("));
	assert!(telemetry.contains("toron.task.stage_role = field::Empty"));
	assert!(telemetry.contains("toron.task.operator_kind = field::Empty"));
}

#[test]
fn adr068_error_status_and_bounded_error_class_on_failed_spans() {
	let shared = observability_telemetry_source_text();
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(shared.contains("metadata.level() == &tracing::Level::ERROR"));
	assert!(shared.contains("state.status_code = SpanStatusCode::Error"));
	assert!(telemetry.contains("toron.error_class"));
	assert!(telemetry.contains("fn query_error_class("));
	assert!(telemetry.contains("fn task_failure_class("));
	assert!(!telemetry.contains("error = %"));
}

#[test]
fn adr068_storage_scan_spans_do_not_change_storage_results() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(exec.contains("let span = crate::telemetry::storage_scan_span(&req);"));
	assert!(exec.contains("self.storage.scan(req).instrument(span).await?"));
	assert!(telemetry.contains("\"toron.storage.scan\""));
	assert!(!telemetry.contains("projected_columns"));
	assert!(!telemetry.contains("pbc_predicate"));
}

#[test]
fn adr068_wendelon_table_query_trace_carrier_is_owned_by_wendelon_adr074() {
	let client =
		std::fs::read_to_string(wendelon_manifest_dir().join("crates/wendelon-client/src/lib.rs"))
			.expect("read Wendelon client");
	let table_schema = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema");
	let options = client
		.split("pub struct TableQueryOptions")
		.nth(1)
		.and_then(|rest| rest.split("impl Default for TableQueryOptions").next())
		.expect("TableQueryOptions block");
	let meta = table_schema
		.split("pub struct TableQueryRequestMeta")
		.nth(1)
		.and_then(|rest| rest.split("pub struct TableQueryResponseMeta").next())
		.expect("TableQueryRequestMeta block");
	assert!(options.contains("trace_context: TableQueryTraceContext"));
	assert!(meta.contains("trace_id: [u8; 16]"));
	assert!(meta.contains("trace_parent_span_id: [u8; 8]"));
}

#[test]
fn adr069_wendelon_trace_consumer_is_toron_only() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(storage.contains("fn wendelon_table_query_trace_context("));
	assert!(storage.contains("wendelon_client::TableQueryTraceContext"));
	assert!(telemetry.contains("pub(crate) fn storage_scan_trace_context("));

	let mut offenders = Vec::new();
	for path in source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.chain(source_files(
			&wendelon_manifest_dir().join("crates/wendelon-client/src"),
		)) {
		let content = std::fs::read_to_string(&path).expect("read Wendelon source");
		for forbidden in [
			"wendelon_table_query_trace_context",
			"storage_scan_trace_context",
			"toron.storage.scan",
		] {
			if content.contains(forbidden) {
				offenders.push(format!("{} contains {forbidden}", path.display()));
			}
		}
	}
	assert!(
		offenders.is_empty(),
		"ADR 069 consumer hooks must stay out of Wendelon production:\n{}",
		offenders.join("\n")
	);
}

#[test]
fn adr069_only_storage_scan_populates_table_query_trace_context() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let production = storage.split("#[cfg(test)]").next().unwrap_or(&storage);
	assert_eq!(
		production
			.matches("fn wendelon_table_query_trace_context(")
			.count(),
		1
	);
	assert_eq!(
		production
			.matches("TableQueryTraceContext::Sampled")
			.count(),
		1
	);
	assert_eq!(
		production
			.matches("trace_context: wendelon_table_query_trace_context(&request),")
			.count(),
		1
	);
}

#[test]
fn adr069_trace_context_set_once_on_shared_options() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, scan_block) =
		brace_blocks_after_marker(&storage, "impl StorageScan for WendelonStorageScan")
			.into_iter()
			.next()
			.expect("WendelonStorageScan impl block");
	let options_pos = scan_block
		.find("let mut options = wendelon_client::TableQueryOptions {")
		.expect("options initialization");
	let carrier = "trace_context: wendelon_table_query_trace_context(&request),";
	let carrier_pos = scan_block.find(carrier).expect("trace context carrier");
	let default_pos = scan_block
		.find("..wendelon_client::TableQueryOptions::default()")
		.expect("default options update");
	let branch_pos = scan_block
		.find("let query_result = if let Some(downstream_cancellation)")
		.expect("cancellation branch");
	assert_eq!(scan_block.matches(carrier).count(), 1);
	assert!(options_pos < carrier_pos);
	assert!(carrier_pos < default_pos);
	assert!(default_pos < branch_pos);
}

#[test]
fn adr069_disabled_local_telemetry_sends_absent_even_with_context() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(helper.contains("if !telemetry_enabled"));
	assert!(helper.contains("return wendelon_client::TableQueryTraceContext::Absent;"));
}

#[test]
fn adr069_enabled_unsampled_sends_unsampled_carrier() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(helper.contains("None => wendelon_client::TableQueryTraceContext::Unsampled"));
}

#[test]
fn adr069_enabled_sampled_sends_sampled_carrier() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(helper.contains("Some(context) => wendelon_client::TableQueryTraceContext::Sampled"));
	assert!(helper.contains("trace_id: context.trace_id"));
	assert!(helper.contains("parent_span_id: context.span_id"));
}

#[test]
fn adr069_parent_span_id_equals_storage_scan_trace_context_span_id() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(helper.contains("match crate::telemetry::storage_scan_trace_context(request)"));
	assert!(helper.contains("parent_span_id: context.span_id"));
}

#[test]
fn adr069_parent_span_id_is_not_request_trace_context_span_id() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(!helper.contains("request.trace_context"));
	assert!(!helper.contains("parent_span_id: parent.span_id"));
	assert!(!helper.contains("parent_span_id: request"));
}

#[test]
fn adr069_storage_scan_span_id_remains_private_to_telemetry() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("\nfn storage_scan_span_id(request: &ScanRequest) -> [u8; 8]"));
	assert!(!telemetry.contains("pub(crate) fn storage_scan_span_id"));
	assert!(!telemetry.contains("pub fn storage_scan_span_id"));
	assert!(telemetry.contains("pub(crate) fn storage_scan_trace_context(request: &ScanRequest)"));
}

#[test]
fn adr069_storage_rs_does_not_duplicate_storage_scan_hash_recipe() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let production = storage.split("#[cfg(test)]").next().unwrap_or(&storage);
	for forbidden in [
		"STORAGE_SCAN_SPAN_DOMAIN",
		"blake3::Hasher",
		"hasher.update(request.query_id",
	] {
		assert!(
			!production.contains(forbidden),
			"storage.rs must delegate storage-scan span id derivation, found {forbidden}"
		);
	}
}

#[test]
fn adr069_cancellation_attempt_hook_shape_is_preserved() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, scan_block) =
		brace_blocks_after_marker(&storage, "impl StorageScan for WendelonStorageScan")
			.into_iter()
			.next()
			.expect("WendelonStorageScan impl block");
	assert!(scan_block.contains("table_query_with_runtime_filter_and_attempt_hook"));
	assert!(scan_block.contains("register_wendelon_table_query_attempt"));
	assert!(scan_block.contains("WendelonTableQueryAttempt"));
	assert!(scan_block.contains("target: attempt.target"));
	assert!(scan_block.contains("tenant_id: attempt.tenant_id"));
	assert!(scan_block.contains("request_id: attempt.request_id"));
}

#[test]
fn adr069_no_toron_derived_wendelon_table_query_span_id() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	for forbidden in [
		"wendelon.table.query",
		"TABLE_QUERY_TRACE",
		"request_id",
		"blake3",
	] {
		assert!(
			!helper.contains(forbidden),
			"Toron must pass the parent for Wendelon's table-query span, not derive it; found {forbidden}"
		);
	}
}

#[test]
fn adr069_no_traceparent_compute_trace_context_v1_or_taskspec_trace() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let production = storage.split("#[cfg(test)]").next().unwrap_or(&storage);
	for forbidden in ["traceparent", "ComputeTraceContextV1", "TaskSpec.trace"] {
		assert!(
			!production.contains(forbidden),
			"ADR 069 must use the Wendelon table-query carrier directly; found {forbidden}"
		);
	}
}

#[test]
fn adr069_no_sql_tenant_table_descriptor_peer_queryid_attrs() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	for forbidden in [
		"sql",
		"tenant_id",
		"table_id",
		"descriptor",
		"peer",
		"query_id",
	] {
		assert!(
			!helper.contains(forbidden),
			"trace carrier mapping must not add unbounded attributes; found {forbidden}"
		);
	}
}

#[test]
fn adr069_no_wendelon_production_change() {
	let mut offenders = Vec::new();
	for path in source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.chain(source_files(
			&wendelon_manifest_dir().join("crates/wendelon-client/src"),
		)) {
		let content = std::fs::read_to_string(&path).expect("read Wendelon source");
		for forbidden in [
			"wendelon_table_query_trace_context",
			"storage_scan_trace_context",
			"toron.storage.scan",
		] {
			if content.contains(forbidden) {
				offenders.push(format!("{} contains {forbidden}", path.display()));
			}
		}
	}
	assert!(
		offenders.is_empty(),
		"ADR 069 must not add Toron consumer hooks to Wendelon production:\n{}",
		offenders.join("\n")
	);
}

#[test]
fn adr069_only_table_query_read_path_carrier_populated() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let production = storage.split("#[cfg(test)]").next().unwrap_or(&storage);
	assert_eq!(
		production
			.matches("trace_context: wendelon_table_query_trace_context")
			.count(),
		1
	);
	assert!(!production.contains("table_insert_with_trace_context"));
	assert!(!production.contains("delete_rows_with_trace_context"));
	assert!(!production.contains("drop_table_with_trace_context"));
}

#[test]
fn adr069_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	for forbidden in [
		"tokio",
		"quinn",
		"rustls",
		"observability",
		"tls-pki",
		"reqwest",
		"h2",
		"wendelon-client",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr069_reconciles_074_producer_only_toron_population_guards() {
	let toron_arch = std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
		.expect("read Toron architecture tests");
	let (_, adr068_block) = brace_blocks_after_marker(
		&toron_arch,
		"fn adr068_wendelon_table_query_trace_carrier_is_owned_by_wendelon_adr074",
	)
	.into_iter()
	.next()
	.expect("ADR 068 reconciliation guard block");
	assert!(!adr068_block.contains("!storage.contains(\"TableQueryTraceContext\")"));
	assert!(!adr068_block.contains("consumer ADR, not ADR 074"));

	let wendelon_arch =
		std::fs::read_to_string(wendelon_manifest_dir().join("tests/architecture_test.rs"))
			.expect("read Wendelon architecture tests");
	let (_, adr074_block) =
		brace_blocks_after_marker(&wendelon_arch, "fn adr074_no_toron_production_change")
			.into_iter()
			.next()
			.expect("ADR 074 reconciliation guard block");
	assert!(adr074_block.contains("ADR 069 owns Toron's table-query trace carrier population"));
	assert!(!adr074_block.contains("\"TableQueryTraceContext\","));
	assert!(!adr074_block.contains("\"trace_parent_span_id\""));
}

#[test]
fn adr069_adr068_and_adr074_guards_remain_green() {
	let toron_arch = std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
		.expect("read Toron architecture tests");
	let wendelon_arch =
		std::fs::read_to_string(wendelon_manifest_dir().join("tests/architecture_test.rs"))
			.expect("read Wendelon architecture tests");
	for required in [
		"fn adr068_wendelon_table_query_trace_carrier_is_owned_by_wendelon_adr074",
		"fn adr069_wendelon_trace_consumer_is_toron_only",
		"fn adr069_reconciles_074_producer_only_toron_population_guards",
	] {
		assert!(toron_arch.contains(required), "missing {required}");
	}
	assert!(wendelon_arch.contains("fn adr074_no_toron_production_change"));
	assert!(wendelon_arch.contains("fn adr074_toron_consumer_population_is_owned_by_adr069"));
}

#[test]
fn adr068_no_sql_table_tenant_session_descriptor_peer_queryid_span_attrs() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let production = telemetry.split("#[cfg(test)]").next().unwrap_or(&telemetry);
	for forbidden in [
		"sql =",
		"statement =",
		"table_name",
		"column_name",
		"tenant_id =",
		"session_id =",
		"physical_descriptor_fingerprint =",
		"peer",
		"query_id =",
	] {
		assert!(
			!production.contains(forbidden),
			"forbidden span attr marker found: {forbidden}"
		);
	}
}

#[test]
fn adr068_toron_otlp_queue_is_bounded_fail_open() {
	let shared = observability_telemetry_source_text();
	assert!(shared.contains("mpsc::channel(cfg.max_queue_size)"));
	assert!(shared.contains("tx.try_send(ExportCommand::Span(completed))"));
	assert!(shared.contains("TrySendError::Full"));
	assert!(shared.contains("TrySendError::Closed"));
}

#[test]
fn adr068_toron_dropped_span_metric_is_aggregate_only() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let metrics = std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
		.expect("read metrics source");
	assert!(telemetry.contains("TORON_DROPPED_SPAN_METRIC"));
	assert!(metrics.contains("# TYPE toron_otlp_spans_dropped_total counter"));
	assert!(metrics.contains("toron_otlp_spans_dropped_total {}"));
	assert!(!metrics.contains("toron_otlp_spans_dropped_total{"));
}

#[test]
fn adr068_toron_shutdown_handle_is_explicit() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(daemon.contains("telemetry: Option<crate::telemetry::TelemetryHandle>"));
	assert!(daemon.contains("telemetry.shutdown()"));
	assert!(telemetry.contains("pub struct TelemetryHandle"));
	assert!(telemetry.contains("OtlpShutdownHandle"));
}

#[test]
fn adr068_wendelon_otlp_behavior_fixtures_remain_unchanged() {
	adr067_wendelon_otlp_behavior_fixtures_remain_present();
	let http = std::fs::read_to_string(wendelon_manifest_dir().join("tests/otlp_http_export.rs"))
		.expect("read Wendelon OTLP HTTP fixture");
	let grpc = std::fs::read_to_string(wendelon_manifest_dir().join("tests/otlp_grpc_export.rs"))
		.expect("read Wendelon OTLP gRPC fixture");
	assert!(!http.contains("toron"));
	assert!(!grpc.contains("toron"));
}

#[test]
fn adr068_nilket_root_stays_runtime_free() {
	adr067_nilket_root_stays_runtime_free();
}

#[test]
fn adr068_fixture_worker_source_unchanged() {
	adr067_fixture_worker_source_unchanged();
}

#[test]
fn adr068_toron_telemetry_surface_is_brand_clean_no_v1() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let shared = observability_telemetry_source_text();
	for forbidden in ["TraceContextV1", "TelemetryProfileV1", "OtlpTraceLayerV1"] {
		assert!(!telemetry.contains(forbidden), "Toron contains {forbidden}");
		assert!(!shared.contains(forbidden), "shared contains {forbidden}");
	}
	assert!(!shared.contains("Toron"));
}

#[test]
fn adr068_prior_adr045_to_067_guards_continue_to_pass_except_external_context_seam() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");
	for prefix in [
		"adr045_", "adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_",
		"adr053_", "adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_",
		"adr061_", "adr062_", "adr063_", "adr064_", "adr065_", "adr066_", "adr067_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains("adr068_explicit_sampled_field_is_authoritative"));
}

#[test]
fn adr049_sql_tenant_source_is_authenticated_peer_identity() {
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
fn adr049_loopback_sql_retains_config_tenant_fallback() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");

	assert!(daemon.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
	assert!(daemon.contains("QuicEndpoint::bind_loopback(sql_config.listener_bind)"));
}

#[test]
fn adr049_remote_sql_never_falls_back_to_config_tenant() {
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
fn adr049_sql_client_san_requires_canonical_numeric_tenant() {
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
fn adr049_sql_client_san_rejects_non_numeric_principal_for_tenant() {
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
fn adr049_sql_listener_still_rejects_sibling_daemon_identity() {
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
fn adr049_quic_peer_identity_extracted_before_sql_frame_read() {
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
fn adr049_peer_kind_client_carries_tenant_id() {
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
fn adr049_coordinator_scopes_catalog_before_binding() {
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
fn adr049_descriptor_registry_keys_refs_and_descriptors_by_tenant() {
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
fn adr049_create_and_drop_table_use_scoped_tenant() {
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
fn adr049_storage_descriptor_lookup_uses_request_tenant() {
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
fn adr049_task_context_and_storage_requests_preserve_tenant() {
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
fn adr049_no_sql_wire_tenant_claim_or_message_variant() {
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
fn adr049_no_new_sql_error_codes() {
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
fn adr049_no_per_tenant_metric_labels() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert_bounded_resource_tenant_labels_only(&observability);
	assert!(!observability.contains("tenant=\""));
	assert!(observability.contains("reason=\\\"{}\\\""));
}

#[test]
fn adr049_no_wendelon_production_changes() {
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
fn adr049_prior_adr04x_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr049_nilket_root_remains_runtime_free() {
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
fn adr049_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	assert!(!fixture.contains("tenant_id"));
	assert!(!fixture.contains("TlsConfig"));
	assert!(!fixture.contains("daemon::"));
}

#[test]
fn adr049_new_types_are_brand_clean_and_no_v1_suffix() {
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
fn adr050_tenant_budget_policy_config_is_node_local() {
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
fn adr050_tenant_budget_config_validates_default_duplicates_and_nonzero() {
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
fn adr050_query_memory_governed_by_query_budget_not_memory_manager() {
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
fn adr050_per_query_budget_sourced_by_tenant_at_coordinator() {
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
fn adr050_aggregate_admission_reserves_and_releases_by_tenant() {
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
fn adr050_aggregate_admission_rejects_before_bind_or_execution() {
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
fn adr050_budget_exhaustion_maps_to_resource_exhausted() {
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
fn adr050_resource_exhausted_is_only_new_sql_error_code() {
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
fn adr050_default_and_loopback_tenants_use_default_budget() {
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
fn adr050_memory_only_no_cpu_connection_or_storage_quotas() {
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
fn adr050_no_per_tenant_metric_labels() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert_bounded_resource_tenant_labels_only(&observability);
	assert!(!observability.contains("tenant=\""));
}

#[test]
fn adr050_no_wendelon_production_changes() {
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
fn adr050_prior_adr04x_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_", "adr049_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr050_nilket_root_remains_runtime_free() {
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
fn adr050_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	assert!(!fixture.contains("TenantBudgetPolicy"));
	assert!(!fixture.contains("tenant_budgets"));
	assert!(!fixture.contains("ResourceExhausted"));
}

#[test]
fn adr050_new_surface_is_brand_clean_and_has_no_v1_suffix() {
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
fn adr051_descriptor_bootstrap_entries_are_tenant_explicit() {
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
fn adr051_descriptor_bootstrap_uses_brand_clean_names() {
	assert_toron_sources_do_not_contain(&[
		"ToronDescriptorBootstrap",
		"ToronDescriptorBootstrapTable",
		"DescriptorBootstrapV1",
		"DescriptorBootstrapEntryV1",
	]);
}

#[test]
fn adr051_descriptor_bootstrap_rejects_missing_tenant_and_table_id() {
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
fn adr051_descriptor_bootstrap_rejects_zero_tenant_and_table_id() {
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
fn adr051_descriptor_bootstrap_rejects_duplicate_table_id_within_tenant() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config test");

	assert!(catalog.contains("duplicate descriptor bootstrap table_id {} for tenant {}"));
	assert!(catalog.contains("state.by_id.contains_key(&(tenant_id, table_id))"));
	assert!(config_test.contains("descriptor_bootstrap_rejects_duplicate_table_id_within_tenant"));
}

#[test]
fn adr051_descriptor_bootstrap_rejects_duplicate_table_name_within_tenant() {
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
fn adr051_descriptor_bootstrap_allows_duplicate_keys_across_tenants() {
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
fn adr051_loader_returns_tenant_table_descriptor_triples() {
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
fn adr051_daemon_uses_tenant_explicit_registry_constructor() {
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
fn adr051_daemon_bootstrap_does_not_default_to_node_tenant() {
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
fn adr051_registry_constructor_is_atomic_for_bootstrap() {
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
fn adr051_descriptor_identity_fields_are_preserved() {
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
fn adr051_no_sql_wire_or_tenant_source_changes() {
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
fn adr051_no_live_descriptor_sync_or_reload() {
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
fn adr051_no_per_tenant_metric_labels() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert!(observability.contains("descriptors_loaded"));
	assert_bounded_resource_tenant_labels_only(&observability);
	assert!(!observability.contains("tenant=\""));
}

#[test]
fn adr051_no_wendelon_production_changes() {
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
fn adr051_prior_adr04x_and_adr050_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_", "adr049_", "adr050_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr051_nilket_root_remains_runtime_free() {
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
fn adr051_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	assert!(!fixture.contains("DescriptorBootstrap"));
	assert!(!fixture.contains("load_descriptor_bootstrap"));
	assert!(!fixture.contains("tenant_id"));
}

#[test]
fn adr051_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronDescriptorBootstrap",
		"ToronDescriptorBootstrapTable",
		"DescriptorBootstrapV1",
		"DescriptorBootstrapEntryV1",
		"TenantDescriptorBootstrapV1",
	]);
}

#[test]
fn adr052_tenant_budget_policy_has_concurrency_limit() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget source");

	assert!(budget.contains("max_concurrent_queries: NonZeroU32"));
	assert!(budget.contains("derive_max_concurrent_queries"));
	assert!(budget.contains("pub fn budget_for(&self, tenant_id: TenantId) -> TenantBudget"));
	assert!(budget.contains("pub fn from_query_budget(query_budget: QueryBudget) -> Self"));
}

#[test]
fn adr052_concurrency_config_validates_nonzero() {
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
fn adr052_omitted_config_derives_bounded_concurrency_default() {
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
fn adr052_concurrency_accountant_is_tenant_keyed_and_raii() {
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
fn adr052_concurrency_admission_rejects_before_bind_or_execution() {
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
fn adr052_admitted_query_holds_memory_and_concurrency_reservations() {
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
fn adr052_concurrency_exhaustion_maps_to_resource_exhausted() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");

	assert!(daemon.contains("ExecutionError::ConcurrencyExhausted"));
	assert!(daemon.contains("SqlErrorCode::ResourceExhausted"));
	assert!(daemon.contains("tenant concurrency limit exhausted"));
	assert!(daemon.contains("sql_concurrency_exhaustion_returns_resource_exhausted"));
}

#[test]
fn adr052_resource_exhausted_reuses_existing_sql_error_code() {
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
fn adr052_one_tenant_saturated_does_not_block_another() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");

	assert!(coordinator.contains("one_tenant_saturated_does_not_block_another"));
	assert!(coordinator.contains("one_tenant_concurrency_saturation_does_not_block_another"));
}

#[test]
fn adr052_resource_metrics_have_bounded_tenant_labels() {
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
fn adr052_general_daemon_metrics_remain_tenant_label_free() {
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
fn adr052_no_cpu_connection_or_storage_quota_surface() {
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
fn adr052_no_wendelon_production_changes() {
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
fn adr052_no_descriptor_sync_stub_or_runtime_ingestion() {
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
fn adr052_prior_adr04x_05x_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_", "adr049_", "adr050_", "adr051_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr052_nilket_root_remains_runtime_free() {
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
fn adr052_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	assert!(!fixture.contains("TenantConcurrencyAccountant"));
	assert!(!fixture.contains("max_concurrent_queries"));
	assert!(!fixture.contains("tenant_budgets"));
}

#[test]
fn adr052_new_surface_is_brand_clean_and_has_no_v1_suffix() {
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
fn adr053_descriptor_sync_lives_in_toron_consumer() {
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
fn adr053_wendelon_server_descriptor_get_surface_unchanged() {
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
fn adr053_resolve_table_miss_is_only_sync_trigger() {
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
fn adr053_descriptor_sync_key_is_tenant_and_exact_name() {
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
fn adr053_unqualified_bind_miss_uses_default_namespace() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("unwrap_or_else(|| \"default\".to_string())"));
	assert!(catalog.contains("fn canonical_table_ref"));
	assert!(catalog.contains("unwrap_or(\"default\")"));
	assert!(catalog.contains("resolve_table_unqualified_miss_fetches_default_namespace"));
}

#[test]
fn adr053_singleflight_shared_across_scoped_catalogs() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let scoped = wendelon_catalog_method_body(&catalog, "fn scoped");

	assert!(catalog.contains("descriptor_sync_flights: Option<Arc<DescriptorSyncFlights>>"));
	assert!(scoped.contains("descriptor_sync_flights: self.descriptor_sync_flights.clone()"));
	assert!(scoped.contains("descriptor_sync_metrics: Arc::clone(&self.descriptor_sync_metrics)"));
}

#[test]
fn adr053_singleflight_waiters_do_not_consume_sync_budget() {
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
fn adr053_descriptor_sync_budget_is_nonzero_configured() {
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
fn adr053_sync_budget_rejection_maps_resource_exhausted() {
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
fn adr053_descriptor_sync_uses_client_facade() {
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
fn adr053_sync_merges_registry_in_place_no_arc_swap() {
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
fn adr053_runtime_sync_does_not_call_from_tenant_descriptors() {
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
fn adr053_no_background_watch_reload_inventory_or_negative_cache() {
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
fn adr053_create_drop_do_not_trigger_descriptor_sync_except_adr061_race_reconciliation() {
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
fn adr053_resolve_table_id_and_storage_do_not_trigger_descriptor_sync() {
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
fn adr053_descriptor_evolved_does_not_add_direct_sync_path() {
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
fn adr053_catalog_liveness_metrics_have_no_tenant_or_table_labels() {
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
fn adr053_retry_is_internal_to_resolve_table_not_coordinator() {
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
fn adr054_descriptor_evolved_carries_local_table_id() {
	let error = std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read error");
	let distributed = std::fs::read_to_string(manifest_dir().join("src/error/distributed.rs"))
		.expect("read distributed error");

	assert!(error.contains("DescriptorEvolved { table_id: TableId }"));
	assert!(distributed.contains("DescriptorEvolved {\n\t\ttable_id: TableId,"));
	assert!(distributed.contains("expected: Option<PhysicalDescriptorFingerprint>"));
	assert!(distributed.contains("current: Option<PhysicalDescriptorFingerprint>"));
}

#[test]
fn adr054_wendelon_evolved_error_mapped_with_request_context() {
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
fn adr054_trigger_is_descriptor_evolved_only() {
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
fn adr054_invalidation_removes_by_id_payload_and_all_refs() {
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
fn adr054_invalidation_is_tenant_scoped_and_idempotent() {
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
fn adr054_select_retry_reuses_adr053_bind_miss_sync() {
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
fn adr054_no_force_refresh_resolve_table_flag() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(!catalog.contains("force_refresh"));
	assert!(!catalog.contains("force_descriptor_refresh"));
	assert!(!catalog.contains("refresh_descriptor_for_tenant"));
}

#[test]
fn adr054_invalidation_is_path_agnostic_select_write_delete() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(coordinator.contains("fn invalidate_descriptor_evolution_on_error"));
	assert!(coordinator.contains("execute_write_plan_with_source("));
	assert!(coordinator.contains("invalidate_descriptor_evolution_on_error(catalog"));
	assert!(coordinator.contains("LogicalPlan::Delete"));
	assert!(coordinator.contains("LogicalPlan::CreateTableAsSelect"));
}

#[test]
fn adr054_writes_invalidate_but_do_not_retry() {
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
fn adr054_replacement_object_returns_schema_instability() {
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
fn adr054_second_evolution_returns_schema_instability() {
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
fn adr054_no_ttl_reaper_watch_inventory_or_push() {
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
fn adr054_no_new_descriptor_refresh_budget_or_governance_knob() {
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
fn adr054_no_wendelon_server_or_sql_wire_change() {
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
fn adr054_refresh_metrics_are_aggregate_only() {
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
fn adr054_prior_adr049_050_051_052_053_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in ["adr049_", "adr050_", "adr051_", "adr052_", "adr053_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr054_nilket_root_stays_runtime_free() {
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
fn adr054_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("DescriptorEvolved"));
	assert!(!worker.contains("catalog_descriptor_refresh"));
	assert!(!worker.contains("force_refresh"));
}

#[test]
fn adr054_new_surface_is_brand_clean_and_has_no_v1_suffix() {
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
fn adr055_queue_lives_at_admit_query_before_bind() {
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
fn adr055_queue_entries_hold_no_plan_descriptor_or_reservation() {
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
fn adr055_omitted_config_preserves_hard_reject() {
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
fn adr055_no_queue_bypass_when_tenant_has_waiters() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("state.has_waiters(tenant_id)"));
	assert!(budget.contains("same_tenant_fifo_holds_new_request_cannot_bypass_existing_waiter"));
	assert!(budget.contains("push_back(waiter)"));
}

#[test]
fn adr055_borrowing_uses_explicit_shared_pool_only() {
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
fn adr055_no_borrow_from_other_tenant_owned_budget() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("tenant_a_cannot_consume_tenant_b_owned_budget"));
	assert!(budget.contains("budget.max_borrowed_memory"));
	assert!(budget.contains("budget.max_borrowed_concurrent_queries"));
	assert!(!budget.contains("idle_owned"));
}

#[test]
fn adr055_grants_decide_memory_concurrency_and_borrow_atomically() {
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
fn adr055_weighted_scheduler_uses_integer_deficit_rr() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("deficits: HashMap<TenantId, i64>"));
	assert!(budget.contains("admission_weight"));
	assert!(budget.contains("weighted_tenants_receive_deterministic_integer_deficit_grant_order"));
	assert!(budget.contains("remove_queue_if_empty"));
	assert!(budget.contains("drained_unconfigured_queue_prunes_scheduler_deficit"));
}

#[test]
fn adr055_no_preemption_or_mid_query_revocation() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(budget.contains("borrowed_in_flight_query_not_preempted_when_other_tenant_arrives"));
	assert!(!coordinator.contains("revocation"));
}

#[test]
fn adr055_no_background_scheduler_actor() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(budget.contains("pump_locked"));
	assert!(budget.contains("Drop for AdmissionReservation"));
	assert!(!budget.contains("tokio::spawn"));
	assert!(!budget.contains("JoinHandle"));
}

#[test]
fn adr055_admission_lock_not_held_across_await() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(coordinator.contains("tokio::time::timeout(wait, queued.recv()).await"));
	assert!(budget.contains("state: Mutex<AdmissionState>"));
	assert!(budget.contains("drop(state);"));
}

#[test]
fn adr055_no_connection_cap_surface() {
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
fn adr057_sql_handshake_failures_do_not_stop_accept_loop() {
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
fn adr057_sql_connection_limiter_lives_at_sql_accept_edge() {
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
fn adr057_global_cap_uses_quic_incoming_before_handshake() {
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
fn adr057_per_tenant_cap_checks_after_sql_tenant_resolution() {
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
fn adr057_loopback_sql_connection_cap_uses_config_tenant() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read sql listener tests");

	assert!(daemon.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
	assert!(sql_tests.contains("loopback_sql_tenant_connection_cap_uses_config_tenant"));
}

#[test]
fn adr057_connection_limiter_is_not_query_coordinator_or_admission_controller() {
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
fn adr057_connection_state_uses_one_lock_not_per_bucket_atomics() {
	let connection = std::fs::read_to_string(manifest_dir().join("src/network/connection.rs"))
		.expect("read connection limiter");

	assert!(connection.contains("state: Mutex<SqlConnectionState>"));
	assert_eq!(connection.matches("Mutex<SqlConnectionState>").count(), 1);
	assert!(!connection.contains("AtomicUsize"));
	assert!(!connection.contains("DashMap"));
}

#[test]
fn adr057_tenant_connection_keys_are_configured_plus_other() {
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
fn adr057_connection_permits_release_by_raii_no_reaper() {
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
fn adr057_tier1_refuses_incoming_without_sql_wire_error() {
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
fn adr057_tier2_closes_connection_without_sql_wire_error() {
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
fn adr057_metrics_split_global_aggregate_and_tenant_bounded() {
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
fn adr057_general_listener_metrics_remain_tenant_label_free() {
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
fn adr057_default_disabled_caps_preserve_current_behavior() {
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
fn adr057_no_worker_transport_or_observability_connection_cap() {
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
fn adr057_no_parser_query_rate_queue_borrow_or_cluster_connection_coordination() {
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
fn adr057_sql_quic_idle_timeout_not_disabled() {
	let quic = std::fs::read_to_string(manifest_dir().join("src/quic.rs")).expect("read quic");

	assert!(!quic.contains("max_idle_timeout(None)"));
	assert!(!quic.contains("keep_alive_interval(None)"));
}

#[test]
fn adr057_no_wendelon_sql_wire_nilket_or_fixture_worker_change() {
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
fn adr057_prior_adr049_050_051_052_053_054_055_056_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_", "adr056_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr057_nilket_root_stays_runtime_free() {
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
fn adr057_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("SqlConnectionLimiter"));
	assert!(!worker.contains("max_concurrent_connections"));
	assert!(!worker.contains("ConnectionLimitExceeded"));
}

#[test]
fn adr057_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"SqlConnectionLimiterV1",
		"SqlConnectionAdmissionV1",
		"TenantConnectionKeyV1",
		"ConnectionLimitExceededV1",
	]);
}

#[test]
fn adr058_sql_request_reader_checks_length_before_allocation() {
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
fn adr058_statement_limit_runs_before_sql_planner() {
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
fn adr058_request_limits_are_default_on_nonzero_safety_floor() {
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
fn adr058_request_limits_have_hard_ceilings() {
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
fn adr058_statement_cap_cannot_exceed_request_frame_cap() {
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
fn adr058_default_statement_cap_matches_request_cap() {
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
fn adr058_frame_reject_has_no_sql_error_before_query_id() {
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
fn adr058_statement_reject_uses_existing_resource_exhausted() {
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
fn adr058_cancel_frames_are_not_statement_capped() {
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
fn adr059_reuses_existing_cancel_wire_and_rejects_control_protocol_drift() {
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
fn adr059_sql_cancel_uses_existing_cancel_frame_not_execute_grammar() {
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
fn adr059_sql_cancel_is_tenant_scoped_and_not_cross_tenant_oracle() {
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
fn adr059_active_query_registry_is_tenant_scoped_and_shared_by_triggers() {
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
fn adr059_active_query_registry_is_tenant_scoped_and_raii_removed() {
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
fn adr059_timeout_disconnect_and_sql_cancel_share_one_handle() {
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
fn adr059_statement_timeout_fires_cancel_before_returning() {
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
fn adr059_client_disconnect_before_execute_decode_sends_no_cancel() {
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
fn adr059_client_response_does_not_wait_for_remote_cancel_ack() {
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
fn adr059_worker_cancel_task_is_not_ack_only_and_tombstones_are_bounded() {
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
fn adr059_remote_cancel_uses_existing_cancel_task_wire() {
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
fn adr059_out_of_band_cancel_stream_is_bidirectional_for_ack() {
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
fn adr059_no_control_message_uni_stream_or_control_endpoint() {
	assert_toron_sources_do_not_contain(&[
		"ControlMessage",
		"open_uni",
		"CancelControl",
		"worker_control",
		"control_endpoint",
	]);
}

#[test]
fn adr059_worker_handles_cancel_stream_concurrently_with_task_stream() {
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
fn adr059_worker_active_registry_keyed_by_task_identity_raii_deregistered() {
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
fn adr059_worker_cancel_task_not_ack_only_and_idempotent() {
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
fn adr059_cancel_before_start_task_tombstones_and_aborts_on_registration() {
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
fn adr059_pending_cancel_tombstone_is_bounded_and_evicted() {
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
fn adr059_remote_cancel_fanout_is_bounded_and_records_aggregate_outcomes() {
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
fn adr059_cancel_fanout_is_bounded_background_cleanup() {
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
fn adr059_abort_is_drop_at_await_with_pre_publish_suppression() {
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
fn adr059_local_cleanup_runs_when_remote_cancel_fails() {
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
fn adr059_pre_admission_rejects_emit_no_cancel() {
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
fn adr059_wendelon_storage_cancellation_claim_is_retargeted_and_metrics_identity_free() {
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
fn adr059_prior_adr043_and_adr049_through_adr058_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr043_", "adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_",
		"adr056_", "adr057_", "adr058_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr059_nilket_root_stays_runtime_free() {
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
fn adr059_fixture_worker_source_unchanged() {
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
fn adr059_new_surface_is_brand_clean_and_has_no_v1_suffix() {
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
fn adr060_consumes_wendelon_adr073_without_new_wire() {
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
fn adr060_client_hook_exposes_attempt_without_retry_duplication() {
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
fn adr060_hook_returns_or_holds_raii_attempt_guard() {
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
fn adr060_attempt_identity_is_target_tenant_request_id_only() {
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
fn adr060_no_toron_identity_crosses_wendelon_cancel_wire() {
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
fn adr060_attempt_registers_before_table_query_write() {
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
fn adr060_retry_redirect_replaces_attempt_registration() {
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
fn adr060_raii_removes_attempt_on_terminal_paths() {
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
fn adr060_cancel_is_parent_fired_not_scan_watcher_primary() {
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
fn adr060_coordinator_cancel_triggers_share_downstream_path() {
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
fn adr060_worker_cancel_task_fires_downstream_before_drop_at_await() {
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
fn adr060_pre_admission_rejects_emit_no_wendelon_cancel() {
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
fn adr060_already_cancelled_before_scan_sends_no_table_query_req() {
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
fn adr060_wendelon_query_cancelled_maps_to_toron_cancelled() {
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
fn adr060_downstream_cancel_fanout_is_bounded_and_nonblocking() {
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
fn adr060_stream_drop_is_fallback_not_primary() {
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
fn adr060_metrics_are_aggregate_only() {
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
fn adr060_no_writes_cdc_admin_agg_or_legacy_query_scope() {
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
fn adr060_no_control_ring_uni_stream_sidecar_or_sql_grammar() {
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
fn adr060_retargets_adr059_no_wendelon_claim_narrowly() {
	let adr059_guard = std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
		.expect("read architecture test");
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
fn adr060_nilket_root_stays_runtime_free() {
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
fn adr060_fixture_worker_source_unchanged() {
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
fn adr060_new_surface_is_brand_clean_and_has_no_v1_suffix() {
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
fn adr060_prior_adr049_to_059_guards_continue_to_pass_except_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

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
fn adr061_ctas_if_not_exists_is_accepted() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people")
		.expect("CTAS IF NOT EXISTS should plan");

	let LogicalPlan::CreateTableAsSelect { if_not_exists, .. } = plan else {
		panic!("expected CTAS logical plan");
	};

	assert!(if_not_exists);
}

#[test]
fn adr061_ctas_ir_carries_if_not_exists() {
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
fn adr061_or_replace_if_not_exists_remains_rejected() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE OR REPLACE TABLE IF NOT EXISTS copy AS SELECT id FROM people")
		.expect_err("OR REPLACE plus IF NOT EXISTS should reject");

	assert!(
		err.to_string()
			.contains("CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS")
	);
}

#[test]
fn adr061_ctas_column_list_remains_rejected() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE copy (id INT) AS SELECT id FROM people")
		.expect_err("CTAS column list should reject");

	assert!(err.to_string().contains("explicit column lists"));
}

#[test]
fn adr061_catalog_create_exposes_created_outcome() {
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
fn adr061_ctas_write_is_gated_on_created_outcome() {
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
fn adr061_ctas_binds_source_before_existing_target_skip() {
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
fn adr061_ctas_uses_exact_schema_compatibility() {
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
fn adr061_ctas_remains_coordinator_owned_non_fragmentable() {
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
fn adr061_no_wendelon_wire_worker_or_sql_protocol_change() {
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
fn adr061_retargets_prior_ctas_if_not_exists_reject_guards() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");
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
fn adr061_nilket_root_stays_runtime_free() {
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
fn adr061_fixture_worker_source_unchanged() {
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
fn adr061_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"CreateTableOutcomeV1",
		"CreateTableAsSelectV1",
		"CtasIfNotExistsV1",
		"CtasOutcomeV1",
		"CreateTableWithOutcomeV1",
	]);
}

#[test]
fn adr061_prior_adr033_to_060_guards_continue_to_pass_except_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

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
fn adr062_create_table_row_identity_type_is_toron_local() {
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
fn adr062_logical_create_table_carries_row_identity() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let create = brace_blocks_after_marker(&logical, "CreateTable {")
		.into_iter()
		.next()
		.expect("logical CreateTable")
		.1;

	assert!(create.contains("row_identity: CreateTableRowIdentity,"));
}

#[test]
fn adr062_execution_create_table_carries_row_identity() {
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
fn adr062_binder_preserves_create_table_row_identity() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");

	assert!(binder.contains("row_identity: row_identity.clone()"));
	assert!(binder.contains("row_identity,"));
}

#[test]
fn adr062_sql_planner_accepts_column_primary_key() {
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
fn adr062_sql_planner_accepts_table_primary_key() {
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
fn adr062_sql_planner_rejects_duplicate_primary_key() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, PRIMARY KEY(id))")
		.expect_err("duplicate PK should reject");

	assert!(err.to_string().contains("PRIMARY KEY more than once"));
}

#[test]
fn adr062_sql_planner_rejects_composite_primary_key() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE t (id INT NOT NULL, sku TEXT NOT NULL, PRIMARY KEY(id, sku))")
		.expect_err("composite PK should reject");

	assert!(
		err.to_string()
			.contains("PRIMARY KEY must reference exactly one column")
	);
}

#[test]
fn adr062_sql_planner_requires_primary_key_not_null() {
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
fn adr062_sql_planner_rejects_unsupported_primary_key_types() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE t (active BOOLEAN NOT NULL PRIMARY KEY)")
		.expect_err("unsupported PK type should reject");

	assert!(
		err.to_string()
			.contains("unsupported CREATE TABLE PRIMARY KEY column type")
	);
}

#[test]
fn adr062_ctas_has_no_primary_key_identity_surface() {
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
fn adr062_catalog_create_request_carries_row_identity() {
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
fn adr062_wendelon_catalog_maps_primary_key_descriptor() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("CreateTableRowIdentity::PrimaryKey"));
	assert!(catalog.contains("RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single"));
	assert!(catalog.contains("RowIdentityPolicy::AppendOnly"));
}

#[test]
fn adr062_if_not_exists_compares_row_identity() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("matches_create_identity(&row_identity)"));
	assert!(catalog.contains("CatalogError::SchemaMismatch"));
	assert!(catalog.contains("TableRowIdentity::Unsupported"));
}

#[test]
fn adr062_no_pk_create_remains_append_only() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE no_pk (id INT NOT NULL)")
		.expect("no-PK create should plan");

	let LogicalPlan::CreateTable { row_identity, .. } = plan else {
		panic!("expected create table");
	};
	assert_eq!(row_identity, CreateTableRowIdentity::AppendOnly);
}

#[test]
fn adr062_update_surface_is_present_under_pk_update_contract_after_adr063() {
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
fn adr062_returning_surface_is_retargeted_after_adr064() {
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
fn adr062_prior_adr038_append_only_guard_retargeted() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

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
fn adr062_nilket_root_stays_runtime_free() {
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
fn adr062_fixture_worker_source_unchanged() {
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
fn adr062_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"CreateTableRowIdentityV1",
		"TableRowIdentityV1",
		"PrimaryKeyDdlV1",
		"CreateTablePrimaryKeyV1",
		"CreateTableRowIdentityPolicyV1",
	]);
}

#[test]
fn adr062_prior_adr033_to_061_guards_continue_to_pass_except_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

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
fn adr063_sql_planner_accepts_statement_update() {
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
fn adr063_sql_planner_destructures_sqlparser_062_update_fields_after_adr073() {
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
fn adr063_sqlparser_upgrade_is_now_owned_by_adr073() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

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
fn adr063_unsupported_statement_text_lists_update() {
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
fn adr063_update_where_requirement_is_retargeted_after_adr070() {
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
fn adr063_update_rejects_unsupported_clauses() {
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
fn adr063_update_rejects_target_alias_joins_and_qualified_targets() {
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
fn adr063_update_rejects_duplicate_and_empty_assignments() {
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
fn adr063_logical_update_shape_is_name_based() {
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
fn adr063_execution_update_shape_is_source_bearing() {
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
fn adr063_logical_update_has_no_wendelon_identity_types() {
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
fn adr063_binder_requires_primary_key_row_identity() {
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
fn adr063_binder_rejects_primary_key_assignment() {
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
fn adr063_binder_builds_full_row_source_projection() {
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
fn adr063_update_source_scan_does_not_request_append_identity() {
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
fn adr063_update_reuses_existing_point_read_profile() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let predicate_helper =
		brace_blocks_after_marker(&binder, "fn bind_update_source_for_predicate")
			.into_iter()
			.next()
			.expect("bind_update_source_for_predicate")
			.1;

	assert!(predicate_helper.contains("scan_profile_hint(&bound_predicate, &scan)"));
	assert!(predicate_helper.contains("apply_scan_profile_hint(&mut scan, Some(profile_hint))"));
	assert!(!predicate_helper.contains("update_profile_hint"));
}

#[test]
fn adr063_update_reuses_write_source_coercion_policy() {
	let write = std::fs::read_to_string(manifest_dir().join("src/coordinator/write.rs"))
		.expect("read write helpers");

	assert!(write.contains("pub(super) fn materialize_update_select_batch"));
	assert!(write.contains("materialize_write_select_batch(\"UPDATE\""));
	assert!(write.contains("coerce_insert_select_array("));
	assert!(write.contains("validate_non_null_targets(operation"));
}

#[test]
fn adr063_coordinator_update_uses_storage_write_not_delete() {
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
fn adr063_descriptor_evolved_invalidates_without_retry() {
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
fn adr063_update_remains_non_fragmentable_and_worker_rejected() {
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
fn adr063_sql_response_is_complete_only_for_non_returning_dml_after_adr064() {
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
fn adr063_no_wendelon_wire_storage_or_descriptor_change() {
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
fn adr063_no_transaction_lock_compare_swap_surface_after_adr070() {
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
fn adr063_nilket_root_stays_runtime_free() {
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
fn adr063_fixture_worker_source_unchanged() {
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
fn adr063_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"UpdateV1",
		"SqlUpdateV1",
		"LogicalUpdateV1",
		"ExecutionUpdateV1",
		"UpdateRequestV1",
	]);
}

#[test]
fn adr063_prior_adr054_to_062_guards_continue_to_pass_except_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

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
fn adr064_returning_supports_four_dml_variants() {
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
fn adr064_ctas_returning_remains_out_of_scope() {
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
fn adr064_sqlparser_upgrade_is_now_owned_by_adr073() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	assert!(manifest.contains("sqlparser = \"0.62\""));
	assert!(architecture_test.contains("fn adr073_sqlparser_is_pinned_to_062"));
	let stale_guard = ["fn ", "adr064_sqlparser", "_stays_054"].concat();
	assert!(!architecture_test.contains(&stale_guard));
}

#[test]
fn adr064_returning_ir_is_toron_local() {
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
fn adr064_bound_plan_schema_reports_returning_schema() {
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
fn adr064_returning_projection_vocab_is_intentionally_narrow() {
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
fn adr064_returning_binds_against_target_user_schema() {
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
fn adr064_returning_does_not_touch_wendelon_or_worker_transport() {
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
fn adr064_returning_does_not_add_sql_wire_message_or_protocol_version() {
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
fn adr064_returning_projection_is_validated_before_mutation() {
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
fn adr064_returning_rows_emit_only_after_storage_success() {
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
fn adr064_complete_row_count_matches_storage_receipt_and_returned_rows() {
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
fn adr064_no_new_pk_gate_for_insert_or_delete_returning() {
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
fn adr064_update_returning_inherits_pk_update_contract() {
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
fn adr064_delete_returning_fails_closed_on_partial_receipt() {
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
fn adr064_delete_returning_uses_pre_delete_rows_without_identity() {
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
fn adr064_insert_returning_uses_inserted_target_rows() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let insert_test = std::fs::read_to_string(manifest_dir().join("tests/insert_test.rs"))
		.expect("read insert tests");

	assert!(coordinator.contains("write::materialize_insert_batch(schema, rows)"));
	assert!(coordinator.contains("std::slice::from_ref(&payload)"));
	assert!(insert_test.contains("insert_values_returning_emits_inserted_rows"));
}

#[test]
fn adr064_insert_select_returning_uses_target_shaped_payload() {
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
fn adr064_update_returning_uses_post_update_rows() {
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
fn adr064_returning_star_hides_storage_synthetic_columns() {
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
fn adr064_returning_rejects_aggregate_window_subquery() {
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
fn adr064_descriptor_evolved_still_invalidates_without_retry() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(coordinator.contains("invalidate_descriptor_evolution_on_error"));
	assert!(!coordinator.contains("retry_returning"));
	assert!(update_test.contains("update_descriptor_evolved_invalidates_without_retry"));
}

#[test]
fn adr064_prior_returning_absence_guards_are_retargeted() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture tests");

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
fn adr064_no_returning_specific_memory_or_config_knob() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(!config.to_lowercase().contains("returning"));
	assert!(!budget.to_lowercase().contains("returning"));
}

#[test]
fn adr064_nilket_root_stays_runtime_free() {
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
fn adr064_fixture_worker_source_unchanged() {
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
fn adr064_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ReturningV1",
		"SqlReturningV1",
		"ReturningProjectionV1",
		"BoundReturningProjectionV1",
		"DmlReturningV1",
	]);
}

#[test]
fn adr064_prior_adr030_to_063_guards_continue_to_pass_except_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

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
fn adr070_update_logical_predicate_is_optional() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let update = brace_blocks_after_marker(&logical, "Update {")
		.into_iter()
		.next()
		.expect("logical update")
		.1;

	assert!(update.contains("predicate: Option<Expr>"));
	assert!(!update.contains("predicate: Expr,"));
}

#[test]
fn adr070_planner_accepts_update_without_where() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("UPDATE people SET note = 'new'")
		.expect("no-WHERE UPDATE should plan");

	let LogicalPlan::Update { predicate, .. } = plan else {
		panic!("expected update plan");
	};
	assert!(predicate.is_none());
}

#[test]
fn adr070_planner_lowers_missing_update_selection_to_none() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_update = brace_blocks_after_marker(&planner, "fn lower_update(&self")
		.into_iter()
		.next()
		.expect("lower_update")
		.1;

	assert!(lower_update.contains("let predicate = selection"));
	assert!(lower_update.contains(".map(|expr| self.lower_expr(expr))"));
	assert!(lower_update.contains(".transpose()?"));
	assert!(!lower_update.contains("UPDATE requires a WHERE clause"));
}

#[test]
fn adr070_update_without_where_is_not_lowered_as_true_literal() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let all_rows = brace_blocks_after_marker(&binder, "fn bind_update_source_for_all_rows")
		.into_iter()
		.next()
		.expect("bind_update_source_for_all_rows")
		.1;

	for forbidden in [
		"Expr::Literal",
		"ScalarValue::Boolean",
		"compile_pbc",
		"attach_pbc_program",
		"ExecutionPlan::Filter",
		"FilterDisposition",
	] {
		assert!(
			!all_rows.contains(forbidden),
			"no-WHERE UPDATE helper gained {forbidden}"
		);
	}
}

#[test]
fn adr070_update_without_where_builds_project_over_scan_without_filter() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let all_rows = brace_blocks_after_marker(&binder, "fn bind_update_source_for_all_rows")
		.into_iter()
		.next()
		.expect("bind_update_source_for_all_rows")
		.1;
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(bind_update.contains("bind_update_source_for_all_rows(scan)"));
	assert!(bind_update.contains("ExecutionPlan::Project"));
	assert!(bind_update.contains("input: Box::new(project_input)"));
	assert!(!all_rows.contains("ExecutionPlan::Filter"));
	assert!(update_test.contains("binder_update_without_where_uses_full_scan_profile_no_filter"));
	assert!(update_test.contains("update_all_rows_source_scan"));
}

#[test]
fn adr070_update_without_where_scan_uses_no_append_identity() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let scan_plan = brace_blocks_after_marker(&binder, "fn scan_plan")
		.into_iter()
		.next()
		.expect("scan_plan")
		.1;

	assert!(bind_update.contains("scan_plan(binding.clone(), None)"));
	assert!(scan_plan.contains("RowIdentityProjection::None"));
	assert!(!bind_update.contains("RowIdentityProjection::Append"));
}

#[test]
fn adr070_update_without_where_profile_hint_is_scan() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let all_rows = brace_blocks_after_marker(&binder, "fn bind_update_source_for_all_rows")
		.into_iter()
		.next()
		.expect("bind_update_source_for_all_rows")
		.1;
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");

	assert!(all_rows.contains("QueryProfileHint::Scan"));
	assert!(!ir.contains("FullTable"));
}

#[test]
fn adr070_update_where_pk_equality_point_read_profile_still_works() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let predicate_helper =
		brace_blocks_after_marker(&binder, "fn bind_update_source_for_predicate")
			.into_iter()
			.next()
			.expect("bind_update_source_for_predicate")
			.1;
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(predicate_helper.contains("scan_profile_hint(&bound_predicate, &scan)"));
	assert!(predicate_helper.contains("apply_scan_profile_hint(&mut scan, Some(profile_hint))"));
	assert!(update_test.contains("binder_update_pk_equality_uses_point_read_profile"));
}

#[test]
fn adr070_update_without_where_still_requires_pk_table() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(bind_update.contains("TableRowIdentity::PrimaryKey"));
	assert!(bind_update.contains("primary_key_columns.contains(primary_key_ordinal)"));
	assert!(bind_update.contains("UPDATE requires a PRIMARY KEY table"));
	assert!(update_test.contains("binder_rejects_update_without_where_on_append_only_table"));
}

#[test]
fn adr070_update_without_where_preserves_pk_immutability() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(bind_update.contains("UPDATE cannot modify the PRIMARY KEY column"));
	assert!(update_test.contains("binder_rejects_update_without_where_primary_key_assignment"));
}

#[test]
fn adr070_update_returning_composes_with_no_where() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(
		coordinator.contains("write::materialize_update_select_batch(schema, &source_batches)")
	);
	assert!(coordinator.contains("project_returning_batches(returning.as_ref()"));
	assert!(coordinator.contains("verify_write_returning_receipt(\"UPDATE\""));
	assert!(update_test.contains("planner_accepts_update_without_where_returning"));
	assert!(update_test.contains("update_all_rows_returning_emits_post_update_rows"));
}

#[test]
fn adr070_update_remains_coordinator_owned_source_bearing_write() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update = brace_blocks_after_marker(&execution, "Update {")
		.into_iter()
		.next()
		.expect("execution update")
		.1;
	let update_start = coordinator
		.find("ExecutionPlan::Update {\n\t\t\tlegacy")
		.expect("coordinator update arm start");
	let update_end = update_start
		+ coordinator[update_start..]
			.find("ExecutionPlan::CreateTableAsSelect")
			.expect("coordinator update arm end");
	let update_arm = &coordinator[update_start..update_end];

	assert!(update.contains("source: Box<ExecutionPlan>"));
	assert!(update_arm.contains("source_executor.execute_read_plan(source).await?"));
	assert!(update_arm.contains("write(WriteRequest"));
	assert!(!update_arm.contains("delete_rows"));
	assert!(!update_arm.contains("StorageDeleteRows"));
}

#[test]
fn adr070_update_remains_non_fragmentable_and_worker_rejected() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec");
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(fragmenter.contains("UpdateNotFragmentable"));
	assert!(fragmenter.contains("matches!(plan, ExecutionPlan::Update { .. })"));
	assert!(server.contains("ExecutionPlan::Update { .. } => \"update\""));
	assert!(exec.contains("UPDATE execution is coordinator-only"));
	assert!(exec.contains("unbound UPDATE execution"));
	assert!(update_test.contains("fragmenter_rejects_direct_update_without_where_fragmentation"));
}

#[test]
fn adr070_delete_no_where_semantics_unchanged() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_delete = brace_blocks_after_marker(&planner, "fn lower_delete")
		.into_iter()
		.next()
		.expect("lower_delete")
		.1;
	let delete_test = std::fs::read_to_string(manifest_dir().join("tests/delete_test.rs"))
		.expect("read delete tests");

	assert!(lower_delete.contains("let predicate = selection"));
	assert!(lower_delete.contains(".map(|expr| self.lower_expr(expr))"));
	assert!(lower_delete.contains(".transpose()?"));
	assert!(!lower_delete.contains("DELETE requires a WHERE clause"));
	assert!(delete_test.contains("planner_lowers_delete_without_where"));
}

#[test]
fn adr070_no_transaction_lock_cas_retry_or_safety_knob() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
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
	for forbidden in [
		"update_without_where",
		"full_table_update",
		"FullTable",
		"confirm_update",
	] {
		assert!(
			!config.contains(forbidden),
			"config gained no-WHERE UPDATE knob {forbidden}"
		);
		assert!(
			!budget.contains(forbidden),
			"budget gained no-WHERE UPDATE knob {forbidden}"
		);
	}
}

#[test]
fn adr070_no_wendelon_wire_storage_or_descriptor_change() {
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
			"ADR 070 leaked UPDATE surface into Wendelon: {forbidden}"
		);
	}
}

#[test]
fn adr070_prior_adr062_to_064_guards_continue_to_pass_except_no_where_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	assert!(
		architecture_test
			.contains("adr062_update_surface_is_present_under_pk_update_contract_after_adr063")
	);
	assert!(
		architecture_test.contains("adr063_update_where_requirement_is_retargeted_after_adr070")
	);
	assert!(architecture_test.contains("adr063_logical_update_shape_is_name_based"));
	assert!(
		architecture_test.contains("adr063_no_transaction_lock_compare_swap_surface_after_adr070")
	);
	assert!(architecture_test.contains("adr064_update_returning_inherits_pk_update_contract"));
	let stale_update_reject = ["planner_rejects_update", "_without_where"].concat();
	assert!(!architecture_test.contains(&stale_update_reject));
}

#[test]
fn adr070_nilket_root_stays_runtime_free() {
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
fn adr070_fixture_worker_source_unchanged() {
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
fn adr070_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"UpdateWithoutWhereV1",
		"UpdateAllRowsV1",
		"FullTableUpdateV1",
		"BulkUpdateV1",
		"FullTable",
	]);
}

#[test]
fn adr071_toron_audit_event_classes_include_task_lifecycle_and_observability_access() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for required in [
		"TaskLifecycle",
		"ObservabilityAccess",
		"task_lifecycle",
		"observability_access",
		"pub const COUNT: usize = 6",
	] {
		assert!(audit.contains(required), "missing {required}");
	}
}

#[test]
fn adr071_toron_audit_operations_include_worker_task_and_observability_access() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for operation in [
		"toron.worker.task.started",
		"toron.worker.task.completed",
		"toron.worker.task.failed",
		"toron.worker.task.cancelled",
		"toron.observability.ready.access",
		"toron.observability.metrics.access",
	] {
		assert!(audit.contains(operation), "missing operation {operation}");
	}
}

#[test]
fn adr071_worker_task_audit_uses_toron_audit_emitter() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(server.contains("audit: ToronAuditEmitter"));
	assert!(server.contains("WorkerTaskAuditRecorder"));
	assert!(server.contains("ToronAuditEvent::worker_task_started"));
	assert!(server.contains("ToronAuditEvent::worker_task_completed"));
	assert!(server.contains("ToronAuditEvent::worker_task_failed"));
	assert!(server.contains("ToronAuditEvent::worker_task_cancelled"));
}

#[test]
fn adr071_worker_task_started_after_decode_route_register_and_accept() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let start_task = server
		.split("MessageKind::StartTask =>")
		.nth(1)
		.and_then(|rest| rest.split("MessageKind::CancelTask =>").next())
		.expect("StartTask arm");

	let decode = start_task
		.find("decode_payload(&envelope.payload)")
		.expect("decode");
	let route = start_task
		.find("if envelope.identity")
		.expect("route validate");
	let register = start_task
		.find("self.active_tasks.register(identity)")
		.expect("register");
	let cancelled = start_task
		.find("if active_task.cancelled_before_start")
		.expect("pre-start cancel check");
	let started = start_task
		.find("audit_recorder.record_started()")
		.expect("started audit");
	assert!(decode < route && route < register && register < cancelled && cancelled < started);
}

#[test]
fn adr071_worker_task_audit_emits_exactly_one_terminal_event() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let recorder = brace_blocks_after_marker(&server, "impl<'a> WorkerTaskAuditRecorder")
		.into_iter()
		.next()
		.expect("worker audit recorder")
		.1;

	assert!(recorder.contains("terminal_recorded"));
	assert_eq!(recorder.matches("self.terminal_recorded = true").count(), 3);
	for method in ["record_completed", "record_failed", "record_cancelled"] {
		let body = brace_blocks_after_marker(&recorder, &format!("fn {method}"))
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{method} body"))
			.1;
		assert!(body.contains("if self.terminal_recorded"));
		assert!(body.contains("self.terminal_recorded = true"));
	}
}

#[test]
fn adr071_worker_task_audit_fields_are_bounded() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for allowed in [
		"with_parameter(\"query_id\"",
		"with_parameter(\"stage_id\"",
		"with_parameter(\"task_id\"",
		"with_parameter(\"attempt_id\"",
		"with_parameter(\"worker_id\"",
		"with_parameter(\"stage_role\"",
		"with_parameter(\"operator_kind\"",
		"with_parameter(\"outcome_class\"",
		"with_parameter(\"error_class\"",
		"with_parameter(\"duration_bucket\"",
		"with_parameter(\"row_count_bucket\"",
	] {
		assert!(audit.contains(allowed), "missing bounded field {allowed}");
	}
}

#[test]
fn adr071_worker_task_audit_has_no_sql_plan_table_descriptor_value_payload_or_raw_error() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for forbidden in [
		"with_parameter(\"sql",
		"with_parameter(\"plan",
		"with_parameter(\"table",
		"with_parameter(\"column",
		"with_parameter(\"descriptor",
		"with_parameter(\"runtime_filter",
		"with_parameter(\"literal",
		"with_parameter(\"arrow",
		"with_parameter(\"payload",
		"with_parameter(\"peer",
		"with_parameter(\"raw_error",
	] {
		if forbidden == "with_parameter(\"sql" {
			assert!(audit.contains("with_parameter(\"sql_error_code_class\""));
		} else {
			assert!(
				!audit.contains(forbidden),
				"worker audit leaked {forbidden}"
			);
		}
	}
}

#[test]
fn adr071_worker_task_audit_does_not_change_task_wire_protocol() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");

	for forbidden in [
		"WorkerTaskStarted",
		"WorkerTaskCompleted",
		"WorkerTaskFailedAudit",
		"AuditEvent",
		"audit",
	] {
		assert!(!wire.contains(forbidden), "wire gained {forbidden}");
	}
}

#[test]
fn adr071_worker_audit_is_disabled_by_default_handle() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(server.contains("audit: ToronAuditEmitter::disabled()"));
	assert!(server.contains("pub fn with_audit_emitter"));
	assert!(daemon.contains("worker.with_audit_emitter(audit.clone())"));
}

#[test]
fn adr071_observability_ready_metrics_audit_is_in_toron_network_layer() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert!(observability.contains("ToronAuditEmitter"));
	assert!(observability.contains("ObservabilityReadyAccess"));
	assert!(observability.contains("ObservabilityMetricsAccess"));
	assert!(observability.contains("handle_ready_connection"));
	assert!(observability.contains("handle_metrics_connection"));
	adr071_shared_observability_crates_remain_policy_free();
}

#[test]
fn adr071_observability_health_listener_remains_unaudited() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let health = brace_blocks_after_marker(&observability, "async fn handle_health_connection")
		.into_iter()
		.next()
		.expect("health handler")
		.1;

	assert!(!health.contains("ToronAuditEmitter"));
	assert!(!health.contains("ToronAuditEvent"));
	assert!(!health.contains("record_observability_access"));
}

#[test]
fn adr071_observability_access_records_admitted_monitor_principal_only() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs")).expect("read tls");

	assert!(tls.contains("accepted_observability_monitor_principal"));
	assert!(tls.contains("ListenerRole::Observability"));
	assert!(tls.contains("PeerKind::Monitor"));
	assert!(observability.contains("accepted_observability_monitor_principal"));
	assert!(observability.contains("format!(\"monitor:{principal}\")"));
	assert!(audit.contains("with_parameter(\"actor_key_id\""));
	assert!(audit.contains("with_parameter(\"actor_role\""));
}

#[test]
fn adr071_observability_access_has_no_raw_san_dn_ip_or_certificate_material() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let source = format!("{audit}\n{observability}");

	for forbidden in [
		"with_parameter(\"san",
		"with_parameter(\"dn",
		"with_parameter(\"distinguished",
		"with_parameter(\"peer_ip",
		"with_parameter(\"peer_addr",
		"with_parameter(\"cert",
		"with_parameter(\"certificate",
		"with_parameter(\"serial",
		"with_parameter(\"header",
	] {
		assert!(
			!source.contains(forbidden),
			"access audit leaked {forbidden}"
		);
	}
}

#[test]
fn adr071_shared_observability_crates_remain_policy_free() {
	let shared = format!(
		"{}\n{}",
		observability_http_source_text(),
		observability_audit_source_text()
	);

	for forbidden in [
		"ToronAudit",
		"toron.worker.task",
		"toron.observability",
		"TaskLifecycle",
		"ObservabilityAccess",
		"try_send",
		"tokio::sync::mpsc",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared observability gained Toron policy {forbidden}"
		);
	}
}

#[test]
fn adr071_audit_emission_preserves_try_send_fail_open_no_request_failure() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert!(audit.contains("tx.try_send(event)"));
	assert!(audit.contains("TrySendError::Full"));
	assert!(audit.contains("TrySendError::Closed"));
	assert!(!audit.contains(".send(event).await"));
	assert!(!server.contains("record(") || !server.contains("?; // audit"));
	assert!(
		!observability.contains("record_observability_access(")
			|| !observability.contains("?; // audit")
	);
}

#[test]
fn adr071_fail_open_default_preserves_no_audit_request_failure_path_after_adr072() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let record = brace_blocks_after_marker(&audit, "pub fn record(&self")
		.into_iter()
		.next()
		.expect("record body")
		.1;
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(config.contains("# fail_closed = false"));
	assert!(record.contains("tx.try_send(event)"));
	assert!(!record.contains("AuditUnavailable"));
	assert!(required.contains("if !*fail_closed"));
	assert!(required.contains("self.record(event);"));
	assert!(required.contains("return Ok(());"));
}

#[test]
fn adr071_runtime_worker_audit_allowed_fixture_worker_stays_audit_free() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(server.contains("audit: ToronAuditEmitter"));
	assert!(server.contains("worker_task_started"));
	assert!(!fixture.contains("audit"));
	assert!(!fixture.contains("ToronAudit"));
}

#[test]
fn adr071_worker_node_id_matches_toron_n_u32() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("const TORON_NODE_ID_PREFIX: &str = \"toron-n-\""));
	assert!(audit.contains("parse_toron_audit_node_numeric"));
	assert!(audit.contains("audit.node_id must use toron-n-<u32>"));
}

#[test]
fn adr071_no_wendelon_nilket_sql_wire_storage_or_worker_wire_change() {
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let worker_wire =
		std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for source in [sql_wire.as_str(), worker_wire.as_str(), storage.as_str()] {
		assert!(!source.contains("ToronAudit"));
		assert!(!source.contains("AuditEvent"));
	}
	assert!(!wendelon_source.contains("toron.worker.task"));
	assert!(!nilket_manifest.contains("observability"));
}

#[test]
fn adr071_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in [
		"tokio",
		"quinn",
		"rustls",
		"observability",
		"tls-pki",
		"serde_json",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr071_prior_adr065_066_audit_guards_continue_to_pass_except_worker_retarget() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in ["adr065_", "adr066_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior audit guard prefix {prefix}");
	}
	assert!(
		architecture_test
			.contains("adr071_runtime_worker_audit_allowed_fixture_worker_stays_audit_free")
	);
}

#[test]
fn adr072_audit_fail_closed_is_opt_in_default_false() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("# fail_closed = false"));
	assert!(config.contains("pub fail_closed: bool"));
	assert!(config.contains("fail_closed: bool"));
	assert!(config.contains("fail_closed: raw.fail_closed"));
	assert!(config.contains("DEFAULT_AUDIT_FAIL_CLOSED_ACK_TIMEOUT_MS"));
}

#[test]
fn adr072_fail_closed_false_is_behavior_equivalent_to_071() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let sql_test = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(required.contains("if !*fail_closed"));
	assert!(required.contains("self.record(event);"));
	assert!(required.contains("return Ok(());"));
	assert!(sql_test.contains("fail_closed: false"));
	assert!(observability_test.contains("fail_closed: false"));
	assert!(audit.contains("record_required_delegates_fail_open_when_fail_closed_false"));
}

#[test]
fn adr072_fail_closed_true_requires_audit_enabled() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(config.contains("if !raw.enabled"));
	assert!(config.contains("if raw.fail_closed"));
	assert!(config.contains("audit.fail_closed requires audit.enabled = true"));
	assert!(config_test.contains("audit_fail_closed_config_accepts_enabled_and_rejects_disabled"));
}

#[test]
fn adr072_fail_closed_ack_timeout_is_bounded_and_nonzero() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(config.contains("DEFAULT_AUDIT_FAIL_CLOSED_ACK_TIMEOUT_MS: u64 = 500"));
	assert!(config.contains("fail_closed_ack_timeout_ms: Option<u64>"));
	assert!(config.contains("fail_closed_ack_timeout: Duration"));
	assert!(config.contains("audit.fail_closed_ack_timeout_ms must be greater than 0"));
	assert!(config_test.contains("fail_closed_ack_timeout"));
}

#[test]
fn adr072_record_remains_try_send_fail_open() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let record = brace_blocks_after_marker(&audit, "pub fn record(&self")
		.into_iter()
		.next()
		.expect("record body")
		.1;

	assert!(record.contains("ToronAuditCommand::fire_and_forget(event)"));
	assert!(record.contains("tx.try_send(event)"));
	assert!(record.contains("TrySendError::Full"));
	assert!(record.contains("TrySendError::Closed"));
	assert!(record.contains("record_drop(metrics"));
	assert!(!record.contains("AuditUnavailable"));
	assert!(!record.contains(".send("));
	assert!(!record.contains(".await"));
}

#[test]
fn adr072_record_required_is_mode_aware() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(required.contains("Result<(), AuditUnavailable>"));
	assert!(required.contains("Self::Disabled => Ok(())"));
	assert!(required.contains("if !*fail_closed"));
	assert!(required.contains("self.record(event);"));
	assert!(required.contains("ToronAuditCommand::required(event, ack_tx)"));
	assert!(required.contains("tokio::time::timeout(*ack_timeout, ack_rx).await"));
}

#[test]
fn adr072_required_audit_uses_worker_ack() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("required_ack: Option<oneshot::Sender<Result<(), AuditUnavailable>>>"));
	assert!(audit.contains("let (ack_tx, ack_rx) = oneshot::channel();"));
	assert!(audit.contains("ToronAuditCommand::required(event, ack_tx)"));
	assert!(audit.contains("let required_ack = command.required_ack;"));
}

#[test]
fn adr072_required_ack_means_local_chain_written_and_fsynced() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let shared_audit = observability_audit_source_text();
	let worker = brace_blocks_after_marker(&audit, "async fn run_audit_worker")
		.into_iter()
		.next()
		.expect("audit worker")
		.1;

	assert!(shared_audit.contains("file.sync_data()"));
	assert!(worker.contains("sink.emit(shared_event)"));
	assert!(worker.contains("ack.send(Ok(()))"));
}

#[test]
fn adr072_required_ack_uses_oneshot_after_sink_emit_returns() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let worker = brace_blocks_after_marker(&audit, "async fn run_audit_worker")
		.into_iter()
		.next()
		.expect("audit worker")
		.1;
	let emit = worker.find("sink.emit(shared_event)").expect("sink emit");
	let ack = worker.find("ack.send(Ok(()))").expect("success ack");

	assert!(emit < ack);
}

#[test]
fn adr072_security_relevant_is_eligibility_not_global_control_flow() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(audit.contains("pub fn security_relevant(&self) -> bool"));
	assert!(required.contains("!event.security_relevant()"));
	assert!(required.contains("event.operation() == ToronAuditOperation::AuditSinkFailure"));
	assert!(required.contains("self.record(event);"));
	assert!(required.contains("return Ok(());"));
}

#[test]
fn adr072_required_call_sites_match_surface_table() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	for operation in [
		"SqlRequestFrameTooLarge",
		"SqlStatementTooLarge",
		"SqlProtocolMismatch",
		"SqlUnexpectedClientFrame",
		"QueryCompleted",
		"QueryPlannedError",
		"QueryExecutionError",
		"QueryCancelled",
		"QueryTimedOut",
		"QueryDisconnectAborted",
	] {
		assert!(
			daemon.contains(operation),
			"missing SQL surface {operation}"
		);
	}
	assert!(daemon.contains("record_sql_audit_required_or_error"));
	assert!(daemon.contains("record_sql_audit_required_without_response"));
	assert!(server.contains("record_started().await"));
	assert!(server.contains("record_completed(emitted_rows).await"));
	assert!(server.contains("record_failed(&error).await"));
	assert!(server.contains("record_cancelled().await"));
	assert!(observability.contains("record_observability_access_required"));
}

#[test]
fn adr072_audit_sink_failure_is_never_required() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(required.contains("ToronAuditOperation::AuditSinkFailure"));
	assert!(required.contains("self.record(event);"));
	assert!(audit.contains("sink_failure_diagnostic_does_not_recursively_require_audit"));
}

#[test]
fn adr072_query_results_are_not_streamed_before_required_terminal_audit() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_test = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");
	let fail_closed = daemon
		.split("let encoded_result_batches")
		.nth(1)
		.expect("SQL success pre-encode block");
	let preencode = fail_closed
		.find("encode_sql_batch")
		.expect("pre-encode batch");
	let required = fail_closed
		.find("record_sql_audit_required_or_error")
		.expect("required terminal audit");
	let send_batch = fail_closed
		.find("SqlMessage::ResultBatch")
		.expect("send batch");

	assert!(preencode < required && required < send_batch);
	assert!(
		sql_test.contains(
			"fail_closed_sql_success_audit_failure_returns_internal_before_result_batches"
		)
	);
}

#[test]
fn adr072_worker_started_required_before_task_execution() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let start_task = server
		.split("MessageKind::StartTask =>")
		.nth(1)
		.and_then(|rest| rest.split("MessageKind::CancelTask =>").next())
		.expect("StartTask arm");
	let started = start_task
		.find("audit_recorder.record_started().await")
		.expect("started audit");
	let task_started = start_task
		.find("MessageKind::TaskStarted")
		.expect("TaskStarted response");
	let execute = start_task
		.find("self.execute_task_local_with_telemetry")
		.expect("task execution");

	assert!(started < task_started && started < execute);
	assert!(server.contains("worker_started_audit_failure_prevents_task_execution"));
}

#[test]
fn adr072_required_queue_full_closed_sink_error_or_timeout_rejects() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for required in [
		"AuditUnavailableReason::QueueFull",
		"AuditUnavailableReason::EmitterClosed",
		"AuditUnavailableReason::SinkFailed",
		"AuditUnavailableReason::Timeout",
		"required_audit_queue_full_returns_bounded_audit_unavailable",
		"required_audit_closed_emitter_returns_bounded_audit_unavailable",
		"required_audit_sink_error_rejects_and_degrades_readiness",
		"required_audit_ack_timeout_returns_bounded_audit_unavailable",
	] {
		assert!(
			audit.contains(required),
			"missing required failure {required}"
		);
	}
}

#[test]
fn adr072_audit_unavailable_errors_are_bounded_oracle_safe() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	for label in [
		"queue_full",
		"emitter_closed",
		"serialization_failed",
		"sink_failed",
		"timeout",
	] {
		assert!(audit.contains(label), "missing bounded reason {label}");
	}
	assert!(daemon.contains("const AUDIT_UNAVAILABLE_SQL_MESSAGE: &str = \"audit unavailable\""));
	assert!(server.contains("reason: \"audit_unavailable\".to_string()"));
	assert!(observability.contains("\"audit unavailable\\n\""));
	assert!(!daemon.contains("write_sql_error(stream, query_id, SqlErrorCode::Internal, error"));
}

#[test]
fn adr072_no_new_sql_error_or_task_failure_enum_variant() {
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let task_errors = std::fs::read_to_string(manifest_dir().join("src/error/distributed.rs"))
		.expect("read task errors");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(!sql_wire.contains("AuditUnavailable"));
	assert!(!task_errors.contains("AuditUnavailable"));
	assert!(server.contains("TaskFailureKind::TaskFailed"));
	assert!(server.contains("audit_unavailable"));
}

#[test]
fn adr072_response_gating_not_side_effect_gating_documented() {
	let adr = std::fs::read_to_string(manifest_dir().join("plans/adr/072_audit_fail_closed.md"))
		.expect("read ADR 072");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(adr.contains("response-gating"));
	assert!(adr.contains("not side-effect rollback"));
	assert!(daemon.contains("let encoded_result_batches"));
	assert!(server.contains("record_completed(emitted_rows).await"));
}

#[test]
fn adr072_fail_closed_degrades_readiness_for_security_relevant_surfaces() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("audit_local_chain_unavailable"));
	assert!(audit.contains("audit_local_chain_write_failed"));
	assert!(audit.contains("record_security_unrecorded"));
	assert!(audit.contains("required_audit_sink_error_rejects_and_degrades_readiness"));
}

#[test]
fn adr072_no_wendelon_storage_sql_wire_worker_wire_or_shared_substrate_change() {
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let worker_wire =
		std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read worker wire");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let shared = observability_audit_source_text();
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	for source in [
		sql_wire.as_str(),
		worker_wire.as_str(),
		storage.as_str(),
		shared.as_str(),
	] {
		assert!(!source.contains("audit.fail_closed"));
		assert!(!source.contains("ToronAuditEmitter"));
	}
	assert!(!wendelon_source.contains("audit.fail_closed"));
	assert!(!wendelon_source.contains("record_required"));
}

#[test]
fn adr072_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in [
		"tokio",
		"quinn",
		"rustls",
		"observability",
		"tls-pki",
		"serde_json",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr072_prior_adr065_066_071_audit_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in ["adr065_", "adr066_", "adr071_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior audit guard prefix {prefix}");
	}
	assert!(architecture_test.contains("fn adr072_record_required_is_mode_aware"));
}

#[test]
fn adr058_request_frame_limit_is_server_inbound_only() {
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
fn adr058_result_batch_frames_keep_existing_wire_shape() {
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
fn adr058_no_new_sql_wire_variant_or_protocol_version() {
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
fn adr058_no_parser_rate_queue_connection_or_cancel_drift() {
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
fn adr058_request_ingress_metrics_are_bounded_and_text_free() {
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
fn adr058_no_worker_transport_observability_wendelon_or_nilket_change() {
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
fn adr058_prior_adr043_and_adr049_through_adr057_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr043_", "adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_",
		"adr056_", "adr057_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr058_nilket_root_stays_runtime_free() {
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
fn adr058_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("read_request_frame_bounded"));
	assert!(!worker.contains("max_request_frame_bytes"));
	assert!(!worker.contains("max_statement_bytes"));
	assert!(!worker.contains("request_frame_too_large"));
	assert!(!worker.contains("statement_too_large"));
}

#[test]
fn adr058_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"RequestFrameTooLargeV1",
		"SqlRequestFrameLimitV1",
		"SqlStatementLimitV1",
		"BoundedSqlIngressV1",
	]);
}

#[test]
fn adr056_rate_is_outermost_controller_gate() {
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
fn adr056_rate_gate_runs_before_admission_enabled_branch() {
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
fn adr056_rate_rejection_never_enters_queue_or_reservation() {
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
fn adr056_rate_token_consumed_on_pass_never_refunded() {
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
fn adr056_rate_has_no_reservation_or_drop_release_path() {
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
fn adr056_lazy_refill_uses_injected_clock_no_background_task() {
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
fn adr056_no_lock_free_or_separate_rate_limiter_component() {
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
fn adr056_default_zero_rate_preserves_current_behavior() {
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
fn adr056_configured_tenants_have_buckets_other_is_shared() {
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
fn adr056_resource_exhausted_reused_no_new_sql_code() {
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
fn adr056_rate_uses_existing_bounded_rejection_metric_no_new_family() {
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
fn adr056_no_preparse_listener_connection_or_worker_transport_rate() {
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
fn adr056_descriptor_retry_does_not_reconsume_rate_token() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");

	assert!(coordinator.contains("adr054_evolved_retry_stays_inside_one_admission_reservation"));
	assert!(coordinator.contains(".with_tenant_budget_policy(rate_policy(1, 1))"));
	assert!(coordinator.contains("retried_descriptor_evolution"));
}

#[test]
fn adr056_no_wendelon_sql_wire_nilket_or_fixture_worker_change() {
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
fn adr056_nilket_root_stays_runtime_free() {
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
fn adr056_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("TenantAdmissionController"));
	assert!(!worker.contains("max_queries_per_second"));
	assert!(!worker.contains("RateLimited"));
}

#[test]
fn adr056_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"TenantRateLimiter",
		"RateLimitExceeded",
		"QueryRateAdmissionV1",
		"TenantRateBucketV1",
		"RateLimitedV1",
	]);
}

#[test]
fn adr056_prior_adr049_050_051_052_053_054_055_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr055_no_descriptor_registry_refresh_or_sync_change() {
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
fn adr055_no_wendelon_server_or_sql_wire_change() {
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
fn adr055_resource_exhausted_reused_no_new_sql_code() {
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
fn adr055_metrics_bounded_configured_plus_other() {
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
fn adr055_prior_adr049_050_051_052_053_054_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in [
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr055_nilket_root_stays_runtime_free() {
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
fn adr055_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("TenantAdmissionController"));
	assert!(!worker.contains("AdmissionPolicy"));
	assert!(!worker.contains("tenant_admission_queue"));
}

#[test]
fn adr055_new_surface_is_brand_clean_and_has_no_v1_suffix() {
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
fn adr053_prior_adr049_050_051_052_guards_continue_to_pass() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	for prefix in ["adr049_", "adr050_", "adr051_", "adr052_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
fn adr053_nilket_root_stays_runtime_free() {
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
fn adr053_toron_fixture_worker_source_unchanged() {
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
fn adr053_new_surface_is_brand_clean_and_has_no_v1_suffix() {
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

#[test]
fn adr046_inspect_uses_hand_rolled_http_no_http_crate_no_hyper_no_reqwest() {
	let cargo =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Cargo.toml");
	let http = std::fs::read_to_string(manifest_dir().join("src/network/observability_http.rs"))
		.expect("read observability HTTP source");

	for forbidden in [
		"http =",
		"hyper",
		"reqwest",
		"ureq",
		"tower",
		"axum",
		"actix-web",
		"hyper-util",
	] {
		if forbidden == "http =" {
			assert!(
				!cargo
					.lines()
					.map(str::trim)
					.any(|line| line.starts_with("http =")),
				"Toron Cargo.toml must not include a direct http crate dependency"
			);
			continue;
		}
		assert!(
			!cargo.contains(forbidden),
			"Toron Cargo.toml must not include {forbidden}"
		);
	}
	for required in ["write_request", "read_response", "HttpResponse"] {
		assert!(
			http.contains(required),
			"missing HTTP client helper {required}"
		);
	}
}

#[test]
fn adr046_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "clap", "tracing", "rustls", "tokio-rustls"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
fn adr046_new_inspect_types_are_brand_clean_and_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronInspect",
		"ToronReadyClient",
		"ToronHttpResponse",
		"InspectOptionsV1",
		"InspectTlsOptionsV1",
		"InspectErrorV1",
		"ReadyClientV1",
		"HttpResponseV1",
	]);
}

#[test]
fn adr044_remote_static_worker_requires_tls_and_san_binding() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let transport = std::fs::read_to_string(manifest_dir().join("src/server/transport.rs"))
		.expect("read Toron transport source");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(config.contains("NON_LOOPBACK_STATIC_WORKER_DIAGNOSTIC"));
	assert!(config.contains("!endpoint.ip().is_loopback() && tls.is_none()"));
	assert!(transport.contains("async fn connect_worker("));
	assert!(transport.contains("canonical_daemon_dns_name(&worker.worker_id, cluster_domain)"));
	assert!(transport.contains("connect_stream_with_server_name(remote, &server_name)"));
	assert!(server.contains("transport.connect_worker(worker).await"));
}

#[test]
fn adr044_one_tls_material_set_per_daemon() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("tls: Option<RawTlsConfig>"));
	assert!(config.contains("pub tls: Option<TlsConfig>"));
	for forbidden in ["[worker.tls]", "[sql.tls]", "worker_tls", "sql_tls"] {
		assert!(
			!config.contains(forbidden),
			"ADR 044 must use one shared TLS material set, found {forbidden}"
		);
	}
	assert_eq!(daemon.matches("ListenerPolicy::load").count(), 1);
	assert!(daemon.contains("server_config_for(ListenerRole::WorkerTask)"));
	assert!(daemon.contains("server_config_for(ListenerRole::Sql)"));
	assert!(daemon.contains("policy.client_config()"));
}

#[test]
fn adr044_san_convention_mirrors_wendelon_shape() {
	let worker_id = WorkerId::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000);
	assert_eq!(
		canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN),
		"toron-550e8400-e29b-41d4-a716-446655440000.cluster.local"
	);
	assert_eq!(
		canonical_client_dns_name("analytics", DEFAULT_CLUSTER_DOMAIN),
		"client-analytics.cluster.local"
	);

	let wendelon_tls = std::fs::read_to_string(wendelon_manifest_dir().join("src/network/tls.rs"))
		.expect("read Wendelon TLS source");
	assert!(wendelon_tls.contains("pub fn canonical_node_dns_name"));
	assert!(wendelon_tls.contains("canonical_dns_name(\"node\""));
}

#[test]
fn adr044_sql_listener_client_admission_is_client_prefix_only() {
	assert!(
		admit_san(
			"client-7.cluster.local",
			ListenerRole::Sql,
			DEFAULT_CLUSTER_DOMAIN
		)
		.is_ok()
	);
	for san in [
		"toron-550e8400-e29b-41d4-a716-446655440000.cluster.local",
		"node-7.cluster.local",
		"service-analytics.cluster.local",
		"client-analytics.cluster.local",
		"client-.cluster.local",
	] {
		assert!(
			admit_san(san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err(),
			"SQL listener should reject {san}"
		);
	}
}

#[test]
fn adr044_no_spiffe_svid_or_uri_san_identity_model() {
	for path in [
		"src/network/tls.rs",
		"src/cli.rs",
		"src/daemon.rs",
		"src/server/transport.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(path)).expect("read source");
		for forbidden in [
			"SPIFFE",
			"spiffe",
			"SVID",
			"uniformResourceIdentifier",
			"UriSan",
			"URI SAN",
		] {
			assert!(
				!source.contains(forbidden),
				"{path} contains ADR 044-forbidden identity model token {forbidden}"
			);
		}
	}
}

#[test]
fn adr044_new_tls_policy_names_are_brand_clean_and_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronTlsConfig",
		"ToronTlsListenerPolicy",
		"ToronTlsError",
		"RawToronTlsConfig",
		"DEFAULT_TORON_CLUSTER_DOMAIN",
		"TORON_AUTH_QUIC_ALPN",
		"canonical_toron_dns_name",
		"canonical_toron_client_dns_name",
		"ToronQuicTls",
		"ToronMtls",
		"TlsConfigV1",
		"ListenerPolicyV1",
	]);
}

#[test]
fn adr043_sql_wire_uses_length_prefixed_postcard() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");

	assert!(wire.contains("encode_payload(frame)"));
	assert!(wire.contains("decode_payload(&payload)"));
	assert!(wire.contains("to_le_bytes()"));
	assert!(wire.contains("u32::from_le_bytes"));
}

#[test]
fn adr043_sql_wire_uses_arrow_ipc_not_row_strings() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");

	assert!(wire.contains("pub arrow_ipc_bytes: ByteBuf"));
	assert!(!wire.contains("Vec<String>"));
	assert!(!wire.contains("rows: Vec"));
}

#[test]
fn adr043_sql_wire_types_have_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"SqlFrameV1",
		"SqlMessageV1",
		"SqlErrorCodeV1",
		"ExecuteRequestV1",
	]);
}

#[test]
fn adr043_new_sql_protocol_names_are_brand_clean() {
	assert_toron_sources_do_not_contain(&[
		"ToronSql",
		"ToronQuic",
		"TORON_QUIC",
		"toron-loopback",
		"ToronCoordinatorConfig",
		"ToronStaticWorker",
		"ToronWendelonTlsConfig",
		"RawToronSql",
		"RawToronCoordinator",
		"RawToronStatic",
	]);
}

#[test]
fn adr043_no_grpc_protobuf_or_arrow_flight() {
	assert_toron_sources_do_not_contain(&["tonic", "prost", "arrow_flight", "arrow-flight"]);
}

#[test]
fn adr043_no_remote_sql_auth_policy() {
	for path in [
		"src/cli.rs",
		"src/daemon.rs",
		"src/sql_wire.rs",
		"src/quic.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(path)).expect("read source");
		for forbidden in [
			"TORON_SQL_TOKEN",
			"Authorization",
			"Bearer",
			"client_tenant",
			"tenant_claim",
			"remote_sql_auth",
		] {
			assert!(
				!source.contains(forbidden),
				"{path} contains remote SQL auth policy token {forbidden}"
			);
		}
	}
}

#[test]
fn adr043_daemon_uses_single_descriptor_registry_for_sql() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let startup = brace_blocks_after_marker(&daemon, "async fn start_with_client")
		.into_iter()
		.next()
		.expect("ToronDaemon::start_with_client")
		.1;

	assert_eq!(
		startup
			.matches("WendelonDescriptorRegistry::from_tenant_descriptors")
			.count(),
		1
	);
	assert!(startup.matches("Arc::clone(&registry)").count() >= 5);
	assert!(startup.contains("DistributedQueryCoordinator"));
}

#[test]
fn adr043_sql_uses_distributed_coordinator_not_local_bypass() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(daemon.contains("DistributedQueryCoordinator"));
	assert!(daemon.contains("sql_execution_context(tenant_id, frame.query_id, session_id)"));
	assert!(daemon.contains("execute_query_with_context(&plan, context)"));
	assert!(!daemon.contains("QueryCoordinator::new"));
	assert!(!daemon.contains("Executor::new("));
}

#[test]
fn adr043_static_worker_registry_config_is_coordinator_scoped() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("pub struct CoordinatorConfig"));
	assert!(config.contains("static_workers: Vec<RawStaticWorkerConfig>"));
	assert!(daemon.contains("StaticWorkerRegistry::new(registry_workers)"));
}

#[test]
fn adr043_no_wendelon_descriptor_refresh() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	for forbidden in [
		"list_descriptors",
		"refresh_descriptor",
		"watch_descriptor",
		"refresh_topology",
	] {
		assert!(
			!daemon.contains(forbidden),
			"daemon contains descriptor refresh token {forbidden}"
		);
	}
}

#[test]
fn adr043_wendelon_client_has_real_provider_mode_for_sql() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(daemon.contains("Client::connect_quic(client_config).await"));
	assert!(!daemon.contains("providers: HashMap::new()"));
}

#[test]
fn adr043_no_fixture_storage_in_production_sql() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(!daemon.contains("InMemoryTableStorage"));
	assert!(!daemon.contains("FixtureStorageScan"));
	assert!(!daemon.contains("toron_fixture_worker"));
}

#[test]
fn adr043_sql_ready_does_not_mutate_adr042_ready_line() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(daemon.contains("println!(\"READY {}\", daemon.handle().worker_endpoint);"));
	assert!(daemon.contains("println!(\"SQL_READY {sql_endpoint}\");"));
}

#[test]
fn adr043_sql_wire_uses_existing_wire_crate() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");

	assert!(wire.contains("use crate::wire::{decode_payload, encode_payload};"));
	assert!(!wire.contains("wendelon::"));
}

#[test]
fn adr042_no_public_query_service() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			[
				"ToronQueryService",
				"ExecuteSqlRequest",
				"SqlListener",
				"QueryListener",
				"ArrowResultService",
			]
			.iter()
			.any(|symbol| line.contains(symbol))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"Toron source contains ADR 042 public query service symbol(s):\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
fn adr042_observability_tracing_section_is_extended_not_replaced() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("[observability]"));
	assert!(config.contains("level = \"info\""));
	assert!(config.contains("format = \"pretty\""));
	assert!(config.contains("pub level: String"));
	assert!(config.contains("pub format: TracingFormat"));
	assert!(daemon.contains("install_tracing"));
	assert!(daemon.contains("tracing_subscriber::fmt()"));
	assert!(daemon.contains("observability::spawn"));
}

#[test]
fn adr042_no_fixture_storage_in_production_daemon() {
	let manifest_dir = manifest_dir();
	let daemon = std::fs::read_to_string(manifest_dir.join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let config = std::fs::read_to_string(manifest_dir.join("src/config.rs"))
		.expect("read Toron config source");
	let production = format!("{daemon}\n{config}");

	for forbidden in [
		"toron_fixture_worker",
		"FixtureStorageScan",
		"vals",
		"orders",
		"customers",
	] {
		assert!(
			!production.contains(forbidden),
			"production daemon/config contains fixture token {forbidden}"
		);
	}
}

#[test]
fn adr042_no_wendelon_node_in_toron_start() {
	let manifest_dir = manifest_dir();
	let daemon = std::fs::read_to_string(manifest_dir.join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let config = std::fs::read_to_string(manifest_dir.join("src/config.rs"))
		.expect("read Toron config source");
	let production = format!("{daemon}\n{config}");

	for forbidden in ["WendelonNode", "raft_storage", "SlabManager", "data_server"] {
		assert!(
			!production.contains(forbidden),
			"Toron start production source contains Wendelon server token {forbidden}"
		);
	}
}

#[test]
fn adr042_fixture_worker_source_remains_isolated() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	for forbidden in [
		"crate::daemon",
		"toron::daemon",
		"crate::config",
		"toron::config",
	] {
		assert!(
			!fixture.contains(forbidden),
			"fixture worker imports production daemon/config token {forbidden}"
		);
	}
}

#[test]
fn adr041_replace_uses_fresh_table_object_id() {
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
	let create = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper")
		.1;
	let write = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared write driver")
		.1;

	assert!(helper.contains(") -> Result<bool, QueryError>"));
	assert!(!helper.contains("Result<Option<TableBinding>"));
	for arm in [
		create
			.split("ExecutionPlan::CreateTable")
			.nth(1)
			.and_then(|rest| rest.split("ExecutionPlan::DropTable").next())
			.expect("CreateTable arm"),
		write
			.split("ExecutionPlan::CreateTableAsSelect")
			.nth(1)
			.and_then(|rest| rest.split("ExecutionPlan::Delete").next())
			.expect("CTAS arm"),
	] {
		let drop = arm
			.find("drop_existing_table_with_storage_admin")
			.expect("replace helper");
		let create = arm.find(".create_table").expect("fresh create");
		assert!(drop < create);
	}
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
fn adr038_no_dedicated_create_or_replace_sql_surface() {
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

	assert!(cargo.contains("blake3 = \"1.5\""));
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
fn adr038_wendelon_create_row_identity_policy_is_create_request_driven_after_adr062() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(catalog.contains("CreateTableRowIdentity::AppendOnly"));
	assert!(catalog.contains("CreateTableRowIdentity::PrimaryKey"));
	assert!(catalog.contains("RowIdentityPolicy::AppendOnly"));
	assert!(catalog.contains("RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single"));
	assert!(!catalog.contains("RowIdentityPolicy::ByOrdinal"));
}

#[test]
fn adr038_no_separate_catalog_cache_after_admin() {
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

	assert!(loop_body.contains("let bound = bind(plan, catalog.as_ref(), &self.config).await?"));
}

#[test]
fn adr073_sqlparser_is_pinned_to_062() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");
	let cargo_lock =
		std::fs::read_to_string(workspace_root_dir().join("Cargo.lock")).expect("read lockfile");
	let lock_entry = cargo_lock
		.split("[[package]]")
		.find(|entry| entry.contains("name = \"sqlparser\""))
		.expect("sqlparser lockfile entry");

	assert!(manifest.contains("sqlparser = \"0.62\""));
	assert!(lock_entry.contains("version = \"0.62.0\""));
	assert!(!manifest.contains("sqlparser = \"0.54\""));
	assert!(!lock_entry.contains("version = \"0.54.0\""));
}

#[test]
fn adr073_sqlparser_ast_is_quarantined_to_sql_planner() {
	let toron_manifest = manifest_dir();
	let mut offenders = source_lines(&toron_manifest.join("src"))
		.into_iter()
		.filter(|(path, _, _)| {
			path.file_name().and_then(|name| name.to_str()) != Some("sql_planner.rs")
		})
		.filter(|(_, _, line)| line.contains("sqlparser"))
		.collect::<Vec<_>>();

	for root in [
		wendelon_manifest_dir().join("src"),
		wendelon_manifest_dir().join("crates/wendelon-client/src"),
	] {
		offenders.extend(
			source_lines(&root)
				.into_iter()
				.filter(|(_, _, line)| line.contains("sqlparser")),
		);
	}

	assert!(
		offenders.is_empty(),
		"sqlparser leaked outside Toron SQL planner:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
fn adr073_no_wendelon_wire_storage_or_ir_leakage() {
	for path in [
		manifest_dir().join("src/ir.rs"),
		manifest_dir().join("src/ir/execution.rs"),
		manifest_dir().join("src/sql_wire.rs"),
		manifest_dir().join("src/storage.rs"),
	] {
		let source = std::fs::read_to_string(&path).expect("read source");
		assert!(
			!source.contains("sqlparser"),
			"{} leaked sqlparser",
			path.display()
		);
	}
}

#[test]
fn adr073_lowered_external_ast_structs_do_not_use_rest_patterns() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	for marker in [
		"fn lower_update(&self",
		"fn lower_delete",
		"fn lower_insert(",
		"fn lower_create_table(",
		"fn lower_create_table_as_select",
		"fn lower_drop_table",
		"fn lower_query",
		"fn lower_select",
		"fn lower_table_factor",
		"fn lower_update_target_table",
	] {
		let body = brace_blocks_after_marker(&planner, marker)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{marker} body"))
			.1;
		assert!(!body.contains("{ .."), "{marker} uses a rest pattern");
		assert!(!body.contains("..,"), "{marker} uses a rest pattern");
	}
}

#[test]
fn adr073_semantic_wildcard_arms_only_reject() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_limit = brace_blocks_after_marker(&planner, "fn lower_limit_clause")
		.into_iter()
		.next()
		.expect("lower_limit_clause")
		.1;
	let lower_order_by = brace_blocks_after_marker(&planner, "fn lower_order_by")
		.into_iter()
		.next()
		.expect("lower_order_by")
		.1;
	let lower_select = brace_blocks_after_marker(&planner, "fn lower_select")
		.into_iter()
		.next()
		.expect("lower_select")
		.1;

	assert!(lower_limit.contains("LimitClause::OffsetCommaLimit { .. }"));
	assert!(lower_limit.contains("LIMIT offset, count syntax is not supported"));
	assert!(lower_order_by.contains("OrderByKind::All(_)"));
	assert!(lower_order_by.contains("ORDER BY ALL is not supported"));
	assert!(lower_select.contains("SelectItem::ExprWithAliases"));
	assert!(lower_select.contains("multi-alias projections are not supported"));
	assert!(!lower_select.contains("_ => Ok("));
}

#[test]
fn adr073_query_limit_clause_preserves_ordered_limit_offset_else_rejects() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_limit = brace_blocks_after_marker(&planner, "fn lower_limit_clause")
		.into_iter()
		.next()
		.expect("lower_limit_clause")
		.1;
	let lower_query = brace_blocks_after_marker(&planner, "fn lower_query")
		.into_iter()
		.next()
		.expect("lower_query")
		.1;
	let lower_order_by = brace_blocks_after_marker(&planner, "fn lower_order_by")
		.into_iter()
		.next()
		.expect("lower_order_by")
		.1;

	for marker in [
		"LimitClause::LimitOffset",
		"limit_by",
		"LIMIT BY is not supported",
		"LimitClause::OffsetCommaLimit",
		"LIMIT offset, count syntax is not supported",
	] {
		assert!(
			lower_limit.contains(marker),
			"limit lowering missing {marker}"
		);
	}
	assert!(lower_query.contains("LIMIT/OFFSET without ORDER BY is not supported"));
	assert!(lower_query.contains("let limit = limit_offset"));
	assert!(lower_query.contains("let offset = limit_offset"));
	assert!(lower_order_by.contains("OrderByKind::Expressions"));
	assert!(lower_order_by.contains("OrderByKind::All(_)"));
	assert!(lower_order_by.contains("ORDER BY ALL is not supported"));
}

#[test]
fn adr073_insert_values_source_rejects_query_decorations() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_insert = brace_blocks_after_marker(&planner, "fn lower_insert(")
		.into_iter()
		.next()
		.expect("lower_insert")
		.1;
	let lower_source = brace_blocks_after_marker(&planner, "fn lower_insert_source")
		.into_iter()
		.next()
		.expect("lower_insert_source")
		.1;

	assert!(lower_insert.contains("lower_insert_column_list(columns)?"));
	assert!(planner.contains("TableObject::TableQuery"));
	for marker in [
		"SetExpr::Values",
		"query.order_by.is_some()",
		"query.limit_clause.is_some()",
		"query.fetch.is_some()",
		"!query.locks.is_empty()",
		"query.settings.is_some()",
		"query.format_clause.is_some()",
		"!query.pipe_operators.is_empty()",
		"unsupported INSERT source: must be VALUES only",
	] {
		assert!(
			lower_source.contains(marker),
			"INSERT source guard missing {marker}"
		);
	}
}

#[test]
fn adr073_new_062_select_item_and_expr_variants_are_explicitly_rejected() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	for marker in [
		"ObjectNamePart",
		"object_name_part_ident",
		"ExprWithAliases",
		"multi-alias projections are not supported",
		"qualified wildcard projections are not supported",
		"ColumnOption::Srid",
		"ColumnOption::Invisible",
		"CAST AS ARRAY is not supported",
		"SELECT EXCLUDE is not supported",
		"FROM-first SELECT is not supported",
		"unsupported INSERT clause: multi-table INSERT",
		"TableObject::TableQuery",
	] {
		assert!(
			planner.contains(marker),
			"missing explicit 0.62 guard {marker}"
		);
	}
}

#[test]
fn adr073_no_new_sql_capability_surface() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let plan_body = brace_blocks_after_marker(&planner, "pub fn plan")
		.into_iter()
		.next()
		.expect("plan body")
		.1;

	for accepted in [
		"Statement::Query(query)",
		"Statement::CreateTable(create_table)",
		"Statement::Insert(insert)",
		"Statement::Delete(delete)",
		"Statement::Update(update)",
		"Statement::Drop { .. }",
	] {
		assert!(
			plan_body.contains(accepted),
			"missing existing surface {accepted}"
		);
	}
	for forbidden in [
		"Statement::AlterTable",
		"Statement::CreateIndex",
		"Statement::CreateView",
		"Statement::Copy",
		"Statement::Merge",
	] {
		assert!(
			!plan_body.contains(forbidden),
			"planner gained new SQL surface {forbidden}"
		);
	}
	assert!(plan_body.contains(
		"unsupported SQL statement; only SELECT, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE are supported"
	));
}

#[test]
fn adr073_retargets_adr063_064_sqlparser_054_pins() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");

	assert!(
		architecture_test
			.contains("adr063_sql_planner_destructures_sqlparser_062_update_fields_after_adr073")
	);
	assert!(architecture_test.contains("adr063_sqlparser_upgrade_is_now_owned_by_adr073"));
	assert!(architecture_test.contains("adr064_sqlparser_upgrade_is_now_owned_by_adr073"));

	for stale_guard in [
		[
			"fn ",
			"adr063_sql_planner_destructures_sqlparser_054",
			"_update_fields",
		]
		.concat(),
		[
			"fn ",
			"adr063_sqlparser_062_upgrade",
			"_is_not_part_of_update_surface",
		]
		.concat(),
		["fn ", "adr064_sqlparser", "_stays_054"].concat(),
	] {
		assert!(!architecture_test.contains(&stale_guard));
	}
}

#[test]
fn adr073_prior_028_029_030_034_036_exhaustive_destructure_guards_pass_on_062() {
	let architecture_test =
		std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
			.expect("read architecture test");
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");

	for guard in [
		"adr028_aggregate_signature_visitors_are_exhaustive",
		"adr029_create_table_destructure_is_exhaustive",
		"adr030_insert_destructure_is_exhaustive",
		"adr034_drop_table_destructure_is_exhaustive",
		"adr036_delete_destructure_is_exhaustive",
	] {
		assert!(
			architecture_test.contains(guard),
			"missing prior guard {guard}"
		);
	}
	assert!(manifest.contains("sqlparser = \"0.62\""));
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

fn toron_source_lines_for_adr039_negative_guards() -> Vec<(PathBuf, usize, String)> {
	let manifest_dir = manifest_dir();
	let architecture_test = manifest_dir.join("tests/architecture_test.rs");
	let mut lines = source_lines(&manifest_dir.join("src"));
	lines.extend(
		source_lines(&manifest_dir.join("tests"))
			.into_iter()
			.filter(|(path, _, _)| path != &architecture_test),
	);
	lines
}

fn assert_toron_sources_do_not_contain(forbidden: &[&str]) {
	let manifest_dir = manifest_dir();
	let offenders = toron_source_lines_for_adr039_negative_guards()
		.into_iter()
		.filter(|(_, _, line)| forbidden.iter().any(|symbol| line.contains(symbol)))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"Toron source contains forbidden symbol(s):\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

fn assert_bounded_resource_tenant_labels_only(observability: &str) {
	for metric in [
		"tenant_memory_bytes_used",
		"tenant_memory_bytes_limit",
		"tenant_concurrency_active",
		"tenant_concurrency_limit",
		"tenant_admission_rejections_total",
	] {
		assert!(
			observability.contains(metric),
			"resource metric {metric} missing from observability source"
		);
	}
	for general_metric in [
		"listener_up",
		"listener_accepts_total",
		"listener_rejects_total",
		"metrics_scrapes_total",
		"daemon_ready",
		"daemon_uptime_seconds",
		"descriptors_loaded",
		"static_workers_configured",
	] {
		let offenders = observability
			.lines()
			.filter(|line| line.contains(general_metric) && line.contains("tenant_id"))
			.collect::<Vec<_>>();
		assert!(
			offenders.is_empty(),
			"general metric {general_metric} gained a tenant_id label: {offenders:?}"
		);
	}
	assert!(observability.contains("tenant_id=\\\"{label}\\\""));
	assert!(observability.contains("\"other\""));
	assert!(observability.contains("configured_tenant_ids"));
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

fn workspace_root_dir() -> PathBuf {
	manifest_dir()
		.parent()
		.expect("crates directory")
		.parent()
		.expect("workspace root")
		.to_path_buf()
}

fn tls_pki_source_text() -> String {
	let src_dir = workspace_root_dir().join("crates/tls-pki/src");
	source_files(&src_dir)
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read tls-pki source"))
		.collect::<Vec<_>>()
		.join("\n")
}

fn observability_http_source_text() -> String {
	let src_dir = workspace_root_dir().join("crates/observability/src/http");
	source_files(&src_dir)
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read observability::http source"))
		.collect::<Vec<_>>()
		.join("\n")
}

fn observability_audit_source_text() -> String {
	let src_dir = workspace_root_dir().join("crates/observability/src/audit");
	source_files(&src_dir)
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read observability::audit source"))
		.collect::<Vec<_>>()
		.join("\n")
}

fn observability_telemetry_source_text() -> String {
	let src_dir = workspace_root_dir().join("crates/observability/src/telemetry");
	source_files(&src_dir)
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read observability::telemetry source"))
		.collect::<Vec<_>>()
		.join("\n")
}

fn observability_manifest() -> String {
	std::fs::read_to_string(workspace_root_dir().join("crates/observability/Cargo.toml"))
		.expect("read observability manifest")
}

fn workspace_cargo_tomls() -> Vec<PathBuf> {
	let workspace_root = workspace_root_dir();
	let mut manifests = Vec::new();
	for relative_root in ["crates", "crates/wendelon/crates"] {
		let root = workspace_root.join(relative_root);
		for entry in std::fs::read_dir(root).expect("read workspace crate dir") {
			let path = entry.expect("read dir entry").path();
			let cargo_toml = path.join("Cargo.toml");
			if cargo_toml.exists() {
				manifests.push(cargo_toml);
			}
		}
	}
	manifests
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
