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
		.expect("LogicalPlan enum");
	assert_eq!(enum_variant_count(logical_plan), 9);
	assert!(logical_plan.contains("Sort {"));
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
fn adr016_having_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("HAVING is not supported"));
	assert!(planner.contains("HAVING aggregate-call must appear in SELECT list"));
	assert!(planner.contains("ambiguous HAVING aggregate-call"));
	assert!(planner.contains("qualified HAVING column references are not supported"));

	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");
	let bind_expr = binder
		.split("fn bind_expr")
		.nth(1)
		.and_then(|rest| rest.split("fn bind_comparison").next())
		.expect("bind_expr block");
	assert!(!bind_expr.contains("AggregateCall"));
	assert!(!bind_expr.contains("AggregateFunction"));

	let aggregate_function_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source");
	let aggregate_function = aggregate_function_source
		.split("pub enum AggregateFunction")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("AggregateFunction enum");
	assert_eq!(enum_variant_count(aggregate_function), 5);

	let aggregate_output_name = planner
		.split("fn aggregate_output_name")
		.nth(1)
		.and_then(|rest| rest.split("fn grouped_aggregate_signature").next())
		.expect("planner aggregate output name helper");
	let aggregate_field = binder
		.split("fn aggregate_field")
		.nth(1)
		.and_then(|rest| rest.split("fn sum_output_data_type").next())
		.expect("binder aggregate field helper");
	for (variant, planner_name, binder_field) in [
		(
			"AggregateFunction::CountAll",
			"\"count\"",
			"Field::new(\"count\",",
		),
		(
			"AggregateFunction::Count",
			"\"count\"",
			"Field::new(\"count\",",
		),
		("AggregateFunction::Sum", "\"sum\"", "Field::new(\"sum\","),
		(
			"AggregateFunction::LinearRegress",
			"\"linreg\"",
			"Field::new(\"linreg\",",
		),
		(
			"AggregateFunction::ApproxQuantile(_)",
			"\"quantile\"",
			"Field::new(\"quantile\",",
		),
	] {
		assert!(
			match_arm_maps_to(aggregate_output_name, variant, planner_name),
			"planner aggregate_output_name mapping drifted for {variant}"
		);
		assert!(
			match_arm_maps_to(aggregate_field, variant, binder_field),
			"binder aggregate_field mapping drifted for {variant}"
		);
	}

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
}

#[test]
fn adr017_order_by_aggregate_call_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(planner.contains("ORDER BY aggregate-call must appear in SELECT list"));
	assert!(planner.contains("ambiguous ORDER BY aggregate-call"));
	assert!(planner.contains("qualified HAVING column references are not supported"));
	assert!(planner.contains("struct LoweredSelect"));
	assert!(planner.contains("grouped_aggregate_scope: Option<GroupedAggregateScope>"));
	assert!(planner.contains("fn rewrite_grouped_aggregate_calls"));
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
	assert!(planner.contains("ambiguous HAVING aggregate-call"));
	assert!(planner.contains("ambiguous ORDER BY aggregate-call"));
	assert!(planner.contains("ORDER BY aggregate-call must appear in SELECT list"));

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

fn compact_whitespace(source: &str) -> String {
	source.split_whitespace().collect()
}

fn match_arm_maps_to(source: &str, variant: &str, mapped_expr_prefix: &str) -> bool {
	let source = compact_whitespace(source);
	let mut offset = 0;
	while let Some(relative_start) = source[offset..].find(variant) {
		let variant_start = offset + relative_start;
		let variant_end = variant_start + variant.len();
		let after_variant = source[variant_end..].chars().next();
		if after_variant.is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
			offset = variant_end;
			continue;
		}

		let tail = &source[variant_end..];
		let Some(arrow_offset) = tail.find("=>") else {
			return false;
		};
		let pattern_tail = &tail[..arrow_offset];
		if pattern_tail.contains(',') || pattern_tail.contains('}') {
			offset = variant_end;
			continue;
		}

		if tail[arrow_offset + 2..].starts_with(mapped_expr_prefix) {
			return true;
		}
		offset = variant_end;
	}
	false
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
