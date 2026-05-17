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
fn execution_expr_has_no_string_column_variant() {
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
	assert_eq!(enum_variant_count(logical_plan), 9);
	assert!(logical_plan.contains("Sort {"));
	assert!(logical_plan.contains("output_names: Vec<Option<String>>"));
	assert!(logical_source.contains("pub struct SortExpr"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution_source
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	assert_eq!(enum_variant_count(execution_plan), 7);
	assert!(execution_plan.contains("Sort {"));
	assert!(execution_source.contains("pub struct ExecutionSortKey"));
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
	assert!(detector.contains("*partition_count != 1 || !key.is_empty()"));
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
	for variant in [
		"Cast(Box<ExecutionExpr>, DataType)",
		"TryCast(Box<ExecutionExpr>, DataType)",
		"Add(Box<ExecutionExpr>, Box<ExecutionExpr>)",
		"Sub(Box<ExecutionExpr>, Box<ExecutionExpr>)",
		"Mul(Box<ExecutionExpr>, Box<ExecutionExpr>)",
		"Div(Box<ExecutionExpr>, Box<ExecutionExpr>)",
	] {
		assert!(
			execution_source.contains(variant),
			"missing ExecutionExpr::{variant}"
		);
	}
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
	assert!(explicit_cast.contains("Ok(ExecutionExpr::Cast(Box::new(inner), target.clone()))"));
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
	assert!(execution_source.contains("TryCast(Box<ExecutionExpr>, DataType)"));
	let data_type = brace_blocks_after_marker(&execution_source, "fn data_type")
		.into_iter()
		.next()
		.expect("data_type body")
		.1;
	assert!(data_type.contains("Self::Cast(_, target) | Self::TryCast(_, target)"));
	let nullable = brace_blocks_after_marker(&execution_source, "fn nullable")
		.into_iter()
		.next()
		.expect("nullable body")
		.1;
	assert!(nullable.contains("Self::TryCast(_, _) => true"));
	let pbc_eligible = brace_blocks_after_marker(&execution_source, "fn pbc_eligible")
		.into_iter()
		.next()
		.expect("execution pbc eligibility body")
		.1;
	assert!(pbc_eligible.contains("Self::TryCast(_, _)"));
	assert!(pbc_eligible.contains("false"));
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
	assert!(bind_try_cast.contains("Ok(ExecutionExpr::TryCast(Box::new(inner), target.clone()))"));
	assert!(
		!bind_try_cast.contains("if source == *target"),
		"bind_try_cast must not elide identity TryCast"
	);
	assert!(
		!bind_try_cast.contains("if source == target"),
		"bind_try_cast must not elide identity TryCast"
	);
	assert!(binder.contains("ExecutionExpr::TryCast(inner, _)"));
	assert!(binder.contains("ExecutionExpr::TryCast(inner, target)"));
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
	assert!(pbc.contains("ExecutionExpr::TryCast(_, _)"));
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
	assert!(stable_bind.contains("bind_expr"));
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(exec.contains("self.evaluate_expr(&args[0]"));
	assert!(exec.contains("self.evaluate_bound_expr(&call.args[0]"));
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
		.and_then(|rest| rest.split("pub enum LogicalPlan").next())
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
