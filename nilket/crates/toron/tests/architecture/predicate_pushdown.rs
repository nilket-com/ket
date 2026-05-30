#![allow(unused_imports)]

use super::support::*;
use super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, ring_placement::*, sql_planner_surface::*,
	storage_wendelon_boundary::*,
};

#[tokio::test]
pub(crate) async fn pbc_engaged_on_eligible_filter() {
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
pub(crate) async fn pbc_declines_arithmetic_filter() {
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
pub(crate) async fn pbc_declines_cast_filter() {
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
pub(crate) async fn pbc_declines_identity_cast_filter() {
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
pub(crate) async fn pbc_declines_try_cast_filter() {
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
pub(crate) async fn pbc_declines_identity_try_cast_filter() {
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
pub(crate) fn scan_request_requires_descriptor_pin() {
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
pub(crate) fn wendelon_storage_scan_forwards_runtime_filter_to_table_query() {
	let source = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let impl_block = source
		.split("impl StorageScan for WendelonStorageScan")
		.nth(1)
		.and_then(|rest| rest.split("fn append_projection").next())
		.expect("WendelonStorageScan StorageScan impl block");

	assert!(impl_block.contains(".table_query_stream_with_runtime_filter("));
	assert!(impl_block.contains("request.runtime_filter.clone()"));
	assert!(!impl_block.contains(".table_query("));
}

#[test]
pub(crate) fn adr118_toron_storage_uses_table_query_stream_api() {
	let source = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let impl_block = source
		.split("impl StorageScan for WendelonStorageScan")
		.nth(1)
		.and_then(|rest| rest.split("fn append_projection").next())
		.expect("WendelonStorageScan StorageScan impl block");

	assert!(impl_block.contains(".table_query_stream_with_runtime_filter("));
	assert!(impl_block.contains(".table_query_stream_with_runtime_filter_and_attempt_hook("));
	assert!(impl_block.contains(".next_batch()"));
	assert!(impl_block.contains("stream::unfold(Some(query_stream)"));
	assert!(impl_block.contains("ScanStream::new("));
	assert!(!impl_block.contains("query_result.batches"));
}

#[test]
pub(crate) fn adr118_attempt_hook_survives_stream_path() {
	let source = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let impl_block = source
		.split("impl StorageScan for WendelonStorageScan")
		.nth(1)
		.and_then(|rest| rest.split("fn append_projection").next())
		.expect("WendelonStorageScan StorageScan impl block");

	// ADR 060 downstream cancellation must stay wired into the streaming
	// scan: when a downstream cancellation context is present the scan
	// uses the attempt-hook variant of the stream API, so dropping a
	// bounded consumer still cancels/stops the Wendelon producer.
	assert!(impl_block.contains("request.downstream_cancellation"));
	assert!(impl_block.contains(".table_query_stream_with_runtime_filter_and_attempt_hook("));
}

#[test]
pub(crate) fn adr118_no_ir_sql_rune_pbc_change() {
	// ADR 118 is a storage-boundary change only. The streaming
	// table-query API must not leak into the IR / SQL planner / Rune /
	// PBC surfaces.
	for relative in [
		"src/ir.rs",
		"src/binder.rs",
		"src/sql_planner.rs",
		"src/rune_engine.rs",
		"src/pbc.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(relative))
			.unwrap_or_else(|err| panic!("read {relative}: {err}"));
		assert!(
			!source.contains("table_query_stream"),
			"{relative} must not reference the streaming storage API (ADR 118 is storage-only)"
		);
	}
}

#[test]
pub(crate) fn adr014_runtime_filter_descriptor_is_kind_tagged_without_bloom_outer_fields() {
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
pub(crate) fn adr014_runtime_filter_descriptor_literals_name_kind() {
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
pub(crate) fn adr014_runtime_filter_contract_has_no_version_suffixed_types() {
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
pub(crate) fn adr014_sql_in_absence_guard_is_retargeted_after_adr074() {
	let source = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	assert!(source.contains("SqlExpr::InList"));
	assert!(source.contains("Expr::InList"));
	assert!(source.contains("SqlExpr::InSubquery { .. }"));
	assert!(source.contains("IN subquery is not supported"));
}

#[test]
pub(crate) fn adr074_in_list_is_first_class_expr_and_bound_node() {
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read ir");
	let execution_ir =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec ir");
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");

	assert!(ir.contains("InList {"));
	assert!(ir.contains("Self::InList { expr, list, .. }"));
	assert!(execution_ir.contains("BoundExprNode::InList"));
	assert!(execution_ir.contains("pub fn append_in_list"));
	assert!(binder.contains("Expr::InList"));
	assert!(binder.contains("fn bind_in_list"));
}

#[test]
pub(crate) fn adr074_in_list_compiles_to_existing_op_in_list_opcode() {
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read pbc");

	assert!(pbc.contains("OP_IN_LIST"));
	assert!(pbc.contains("bytecode.push(OP_IN_LIST)"));
	assert!(pbc.contains("bytecode.extend_from_slice(&count.to_le_bytes())"));
	assert!(!pbc.contains("Op::InList"));
	assert!(!pbc.contains("OP_IN_LIST_V"));
}

#[test]
pub(crate) fn adr074_in_list_adds_no_new_wendelon_pbc_opcode() {
	let wendelon_pbc = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/pbc.rs"))
		.expect("read Wendelon pbc");

	assert!(wendelon_pbc.contains("pub const OP_IN_LIST: u8 = 0x30;"));
	assert_eq!(wendelon_pbc.matches("pub const OP_IN_LIST").count(), 1);
	assert!(!wendelon_pbc.contains("OP_IN_LIST_V"));
}

#[test]
pub(crate) fn adr074_in_list_negation_uses_existing_not_opcode() {
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read pbc");

	assert!(pbc.contains("OP_NOT"));
	assert!(pbc.contains("if *negated"));
	assert!(pbc.contains("bytecode.push(OP_NOT)"));
	assert!(!pbc.contains("OP_NOT_IN"));
}

#[test]
pub(crate) fn adr074_in_list_residual_eval_is_implemented_without_new_wendelon_surface() {
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec");
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");

	assert!(exec.contains("fn evaluate_in_list"));
	assert!(exec.contains("let mask = eq(expr_arr, item_arr)?"));
	assert!(exec.contains("or_kleene(&current, &mask)?"));
	assert!(!exec.contains("in_list_utf8"));
	assert!(!manifest.contains("arrow-ord"));
}

#[test]
pub(crate) fn adr074_in_list_pushdown_eligible_iff_members_eligible() {
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read ir");

	assert!(ir.contains("Self::InList { expr, list, .. }"));
	assert!(ir.contains("expr.is_pbc_pushdown_eligible()"));
	assert!(ir.contains("list.iter().all(Self::is_pbc_pushdown_eligible)"));
}

#[test]
pub(crate) fn adr074_in_list_has_static_item_limit() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	assert!(planner.contains("const MAX_IN_LIST_ITEMS: usize = 32;"));
	assert!(planner.contains("list.len() > MAX_IN_LIST_ITEMS"));
	assert!(planner.contains("IN list exceeds {MAX_IN_LIST_ITEMS} items"));
}

#[test]
pub(crate) fn adr074_between_desugars_with_no_new_ir_or_opcode() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read ir");
	let execution_ir =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec ir");
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read pbc");

	assert!(planner.contains("SqlExpr::Between"));
	assert!(planner.contains("fn lower_between"));
	assert!(planner.contains("Expr::And"));
	assert!(planner.contains("Expr::Gte"));
	assert!(!ir.contains("Expr::Between"));
	assert!(!ir.contains("Between {"));
	assert!(!execution_ir.contains("BoundExprNode::Between"));
	assert!(!pbc.contains("OP_BETWEEN"));
}

#[test]
pub(crate) fn adr074_similar_rlike_case_subquery_in_remain_unsupported() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	for marker in [
		"SqlExpr::SimilarTo { .. }",
		"SqlExpr::RLike { .. }",
		"SqlExpr::InSubquery { .. }",
		"SqlExpr::InUnnest { .. }",
		"BinaryOperator::PGLikeMatch",
		"BinaryOperator::PGILikeMatch",
	] {
		assert!(planner.contains(marker), "missing reject marker {marker}");
	}
}

#[test]
pub(crate) fn adr075_like_ilike_are_first_class_residual_only_predicates() {
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read ir");
	let execution_ir =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec ir");
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read pbc");
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec");

	assert!(ir.contains("Like {"));
	assert!(ir.contains("| Self::Like { .. }"));
	assert!(ir.contains("=> false"));
	assert!(execution_ir.contains("BoundExprNode::Like"));
	assert!(execution_ir.contains("pub fn append_like"));
	assert!(planner.contains("SqlExpr::Like {"));
	assert!(planner.contains("SqlExpr::ILike {"));
	assert!(planner.contains("fn lower_like"));
	assert!(planner.contains("LIKE pattern must be a string literal"));
	assert!(binder.contains("Expr::Like"));
	assert!(binder.contains("fn bind_like"));
	assert!(binder.contains("LIKE expects Utf8 input"));
	assert!(pbc.contains("BoundExprNode::Like { .. }"));
	assert!(pbc.contains("LIKE is not PBC-eligible"));
	assert!(exec.contains("fn evaluate_like"));
	assert!(exec.contains("arrow_like"));
	assert!(exec.contains("arrow_ilike"));
	assert!(!pbc.contains("OP_LIKE"));
}

#[test]
pub(crate) fn adr108_case_is_residual_bound_expression() {
	let manifest_dir = manifest_dir();
	let ir = std::fs::read_to_string(manifest_dir.join("src/ir.rs")).expect("read ir");
	let execution_ir =
		std::fs::read_to_string(manifest_dir.join("src/ir/execution.rs")).expect("read exec ir");
	let planner =
		std::fs::read_to_string(manifest_dir.join("src/sql_planner.rs")).expect("read planner");
	let binder = std::fs::read_to_string(manifest_dir.join("src/binder.rs")).expect("read binder");
	let pbc = std::fs::read_to_string(manifest_dir.join("src/pbc.rs")).expect("read pbc");
	let exec = std::fs::read_to_string(manifest_dir.join("src/exec.rs")).expect("read exec");
	let storage =
		std::fs::read_to_string(manifest_dir.join("src/storage.rs")).expect("read storage");
	let wendelon_offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("OP_CASE")
				|| line.contains("BoundExprNode::Case")
				|| line.contains("Expr::Case")
		})
		.collect::<Vec<_>>();

	assert!(ir.contains("Case {"));
	assert!(ir.contains("| Self::Case { .. }"));
	assert!(execution_ir.contains("Case {"));
	assert!(execution_ir.contains("else_input: BoundExprId"));
	assert!(execution_ir.contains("pub fn append_case"));
	assert!(execution_ir.contains("BoundExprNode::Case"));
	assert!(execution_ir.contains("node_nullable"));
	assert!(planner.contains("SqlExpr::Case"));
	assert!(planner.contains("fn lower_case"));
	assert!(!planner.contains("CASE is not supported"));
	assert!(binder.contains("Expr::Case"));
	assert!(binder.contains("fn bind_case"));
	assert!(binder.contains(
		"bind_boolean_context_expr(builder, input_schema, ctx, \"CASE WHEN\", condition)?"
	));
	assert!(binder.contains("fn case_common_type"));
	assert!(binder.contains("arithmetic_output_type(&common, data_type)"));
	assert!(binder.contains("fn typed_null_for_data_type"));
	assert!(pbc.contains("BoundExprNode::Case { .. }"));
	assert!(pbc.contains("CASE is not PBC-eligible"));
	assert!(!pbc.contains("OP_CASE"));
	assert!(exec.contains("fn evaluate_case"));
	assert!(exec.contains("kernels::zip::zip"));
	assert!(exec.contains("treat CASE as a lazy error fence"));
	assert!(!ir.contains("LogicalPlan::Case"));
	assert!(!execution_ir.contains("ExecutionPlan::Case"));
	assert!(!storage.contains("BoundExprNode::Case"));
	assert!(!storage.contains("Expr::Case"));
	assert!(
		wendelon_offenders.is_empty(),
		"Wendelon must not grow CASE surface:\n{}",
		format_offenders(&wendelon_manifest_dir(), &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr075_partial_pushdown_splits_bound_top_level_and_only() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let splitter = brace_blocks_after_marker(&binder, "fn partition_bound_predicate")
		.into_iter()
		.next()
		.expect("partition_bound_predicate body")
		.1;
	let boolean_splitter = brace_blocks_after_marker(&binder, "fn split_boolean_tree")
		.into_iter()
		.next()
		.expect("split_boolean_tree body")
		.1;
	let legacy_splitter = brace_blocks_after_marker(&binder, "fn legacy_top_level_and_partition")
		.into_iter()
		.next()
		.expect("legacy_top_level_and_partition body")
		.1;
	let collector = brace_blocks_after_marker(&binder, "fn collect_top_level_bound_and")
		.into_iter()
		.next()
		.expect("collect_top_level_bound_and body")
		.1;

	assert!(splitter.contains("split_boolean_tree(predicate, root)"));
	assert!(splitter.contains("legacy_top_level_and_partition(predicate)"));
	assert!(boolean_splitter.contains("op: BoundBinaryOp::And"));
	assert!(boolean_splitter.contains("combine_bound_programs_as_and"));
	assert!(legacy_splitter.contains("compile_pbc(&isolated).is_some()"));
	assert!(legacy_splitter.contains("combine_bound_conjuncts(predicate, &pushed)"));
	assert!(legacy_splitter.contains("combine_bound_conjuncts(predicate, &residual)"));
	assert!(collector.contains("op: BoundBinaryOp::And"));
	assert!(!collector.contains("BoundBinaryOp::Or"));
	assert!(!collector.contains("BoundUnaryOp::Not"));
	assert!(binder.contains("let Some(pbc_program) = compile_pbc(&pushed_predicate)"));
	assert!(binder.contains("pushdown.residual_predicate"));
	assert!(binder.contains("FilterDisposition::EvaluateInToron"));
}

#[test]
pub(crate) fn adr075_partial_pushdown_covers_select_update_delete_scan_sites() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");
	let update_test =
		std::fs::read_to_string(manifest_dir().join("tests/update_test.rs")).expect("read update");
	let delete_test =
		std::fs::read_to_string(manifest_dir().join("tests/delete_test.rs")).expect("read delete");

	let filter_arm = binder
		.split("LogicalPlan::Filter { input, predicate } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Project").next())
		.expect("Filter bind arm");
	assert!(filter_arm.contains("plan_filter_with_predicate_pushdown("));
	assert!(filter_arm.contains("input.plan"));
	assert!(filter_arm.contains("bound_predicate"));
	assert!(filter_arm.contains("&logical_types"));
	let update_helper = first_block(&binder, "async fn bind_update_source_for_predicate");
	assert!(update_helper.contains("self.catalog.logical_type_registry()"));
	assert!(update_helper.contains("plan_filter_with_predicate_pushdown("));
	assert!(update_helper.contains("&logical_types"));
	assert!(binder.contains("predicate_pushdown_plan(program, &scan)"));
	assert!(coordinator.contains("if let Some(predicate) = predicate"));
	assert!(predicate_test.contains("where_like_residual_splits_from_pbc_conjunct"));
	assert!(update_test.contains("binder_update_splits_pbc_conjunct_from_like_residual"));
	assert!(delete_test.contains("binder_delete_splits_pbc_conjunct_from_like_residual"));
}

#[test]
pub(crate) fn adr078_recursive_splitter_hosts_or_not_extraction_rules() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let partitioner = brace_blocks_after_marker(&binder, "fn partition_bound_predicate")
		.into_iter()
		.next()
		.expect("partition_bound_predicate body")
		.1;
	let splitter = brace_blocks_after_marker(&binder, "fn split_boolean_tree")
		.into_iter()
		.next()
		.expect("split_boolean_tree body")
		.1;

	assert!(partitioner.contains("split_boolean_tree(predicate, root)"));
	assert!(partitioner.contains("legacy_top_level_and_partition(predicate)"));
	assert!(splitter.contains("try_extract_common_or_factors(source, root)"));
	assert!(splitter.contains("try_extract_not_or_branch_negations(source, root, *input)"));
}

#[test]
pub(crate) fn adr078_pushed_predicate_is_necessary_for_original() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let common = brace_blocks_after_marker(&binder, "fn try_extract_common_or_factors")
		.into_iter()
		.next()
		.expect("try_extract_common_or_factors body")
		.1;
	let not_or = brace_blocks_after_marker(&binder, "fn try_extract_not_or_branch_negations")
		.into_iter()
		.next()
		.expect("try_extract_not_or_branch_negations body")
		.1;

	assert!(common.contains("necessary_candidates"));
	assert!(common.contains("present_in_every_branch"));
	assert!(common.contains("compile_pbc(&isolated).is_some()"));
	assert!(not_or.contains("pushed_negations"));
	assert!(not_or.contains("build_negated_subgraph"));
	assert!(not_or.contains("compile_pbc(&candidate).is_some()"));
}

#[test]
pub(crate) fn adr078_factoring_preserves_3vl_where_true_set() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	assert!(binder.contains("predicate_pushdown_emitted_splits_preserve_kleene_3vl"));
	assert!(binder.contains("partition_bound_predicate(&program)"));
	assert!(binder.contains("split must preserve exact K3 value"));
	assert!(predicate_test.contains("predicate_pushdown_3vl_property_random_rows_including_nulls"));
	assert!(predicate_test.contains("enum K3"));
	assert!(predicate_test.contains("fn k3_and"));
	assert!(predicate_test.contains("fn k3_or"));
	assert!(predicate_test.contains("fn k3_not"));
	assert!(predicate_test.contains("k3_and(pushed, residual)"));
}

#[test]
pub(crate) fn adr078_fully_pushable_or_and_not_still_push_as_whole_subtrees() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");
	let splitter = brace_blocks_after_marker(&binder, "fn split_boolean_tree")
		.into_iter()
		.next()
		.expect("split_boolean_tree body")
		.1;

	assert!(splitter.contains("let whole = isolate_bound_subgraph(source, root)?"));
	assert!(splitter.contains("compile_pbc(&whole).is_some()"));
	assert!(predicate_test.contains("where_fully_pushable_or_pushes_whole_or"));
	assert!(predicate_test.contains("where_fully_pushable_not_pushes_whole_not"));
}

#[test]
pub(crate) fn adr078_top_level_and_split_remains_minimal() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let splitter = brace_blocks_after_marker(&binder, "fn split_boolean_tree")
		.into_iter()
		.next()
		.expect("split_boolean_tree body")
		.1;
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	assert!(splitter.contains("op: BoundBinaryOp::And"));
	assert!(splitter.contains("[left.pushed, right.pushed]"));
	assert!(splitter.contains("[left.residual, right.residual]"));
	assert!(predicate_test.contains("where_like_residual_splits_from_pbc_conjunct"));
}

#[test]
pub(crate) fn adr078_mixed_or_does_not_push_one_branch_only() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let common = brace_blocks_after_marker(&binder, "fn try_extract_common_or_factors")
		.into_iter()
		.next()
		.expect("try_extract_common_or_factors body")
		.1;
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	assert!(common.contains("present_in_every_branch"));
	assert!(predicate_test.contains("where_mixed_or_branch_does_not_push_partial_branch"));
	assert!(predicate_test.contains("assert!(scan_pbc(scan).is_none())"));
}

#[test]
pub(crate) fn adr078_common_or_factor_requires_presence_in_every_branch() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let common = brace_blocks_after_marker(&binder, "fn try_extract_common_or_factors")
		.into_iter()
		.next()
		.expect("try_extract_common_or_factors body")
		.1;

	assert!(common.contains("collect_bound_or_branches"));
	assert!(common.contains("collect_top_level_bound_and"));
	assert!(common.contains("branch_terms.iter().skip(1).all"));
	assert!(common.contains("present_in_every_branch"));
}

#[test]
pub(crate) fn adr078_common_factor_matching_is_structural_not_algebraic() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let structural =
		brace_blocks_after_marker(&binder, "fn bound_subgraphs_structurally_equal_inner")
			.into_iter()
			.next()
			.expect("bound_subgraphs_structurally_equal_inner body")
			.1;
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	for marker in [
		"source.node_type(left) != source.node_type(right)",
		"BoundExprNode::Column",
		"BoundExprNode::Literal",
		"BoundExprNode::Binary",
		"BoundExprNode::Like",
		"left_op == right_op",
	] {
		assert!(
			structural.contains(marker),
			"missing structural marker {marker}"
		);
	}
	assert!(!structural.contains("pretty"));
	assert!(!structural.contains("Debug"));
	assert!(!structural.contains("format!"));
	assert!(predicate_test.contains("where_structurally_different_equivalent_terms_do_not_factor"));
}

#[test]
pub(crate) fn adr078_not_or_extracts_only_compilable_branch_negations() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let not_or = brace_blocks_after_marker(&binder, "fn try_extract_not_or_branch_negations")
		.into_iter()
		.next()
		.expect("try_extract_not_or_branch_negations body")
		.1;
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	assert!(not_or.contains("build_negated_subgraph"));
	assert!(not_or.contains("compile_pbc(&candidate).is_some()"));
	assert!(not_or.contains("combine_bound_programs_as_and(pushed_negations)"));
	assert!(not_or.contains("combine_bound_programs_as_and(residual_negations)"));
	assert!(
		predicate_test
			.contains("where_not_or_minimal_residual_keeps_only_unpushed_branch_negations")
	);
}

#[test]
pub(crate) fn adr078_not_and_does_not_push_individual_negated_terms() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let not_or = brace_blocks_after_marker(&binder, "fn try_extract_not_or_branch_negations")
		.into_iter()
		.next()
		.expect("try_extract_not_or_branch_negations body")
		.1;
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	assert!(not_or.contains("op: BoundBinaryOp::Or"));
	assert!(!not_or.contains("BoundBinaryOp::And"));
	assert!(predicate_test.contains("where_not_and_mixed_does_not_push_negated_single_side"));
}

#[test]
pub(crate) fn adr078_boolean_factoring_has_bounded_extraction_budget() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	for marker in [
		"BOOLEAN_FACTOR_MAX_NODES",
		"BOOLEAN_FACTOR_MAX_OR_BRANCHES",
		"BOOLEAN_FACTOR_MAX_AND_TERMS_PER_BRANCH",
		"BOOLEAN_FACTOR_MAX_CANDIDATES",
		"BooleanSplitError::OverBudget",
		"legacy_top_level_and_partition(predicate)",
	] {
		assert!(binder.contains(marker), "missing budget marker {marker}");
	}
	assert!(
		predicate_test.contains("boolean_factoring_over_budget_falls_back_without_wrong_results")
	);
}

#[test]
pub(crate) fn adr078_pushdown_classification_still_uses_compile_pbc_attempts() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");

	for marker in [
		"compile_pbc(&whole).is_some()",
		"compile_pbc(&isolated).is_some()",
		"compile_pbc(&candidate).is_some()",
		"let Some(pbc_program) = compile_pbc(&pushed_predicate)",
	] {
		assert!(
			binder.contains(marker),
			"missing compile-attempt marker {marker}"
		);
	}
}

#[test]
pub(crate) fn adr078_factored_pushdown_drives_scan_profile() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	assert!(binder.contains("let profile_hint = scan_profile_hint(&pushed_predicate, input)"));
	assert!(
		predicate_test
			.contains("where_common_pk_conjunct_minimal_residual_keeps_point_read_profile")
	);
}

#[test]
pub(crate) fn adr078_select_update_delete_share_boolean_factoring_path() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");
	let update_test =
		std::fs::read_to_string(manifest_dir().join("tests/update_test.rs")).expect("read update");
	let delete_test =
		std::fs::read_to_string(manifest_dir().join("tests/delete_test.rs")).expect("read delete");

	let filter_arm = binder
		.split("LogicalPlan::Filter { input, predicate } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Project").next())
		.expect("Filter bind arm");
	assert!(filter_arm.contains("plan_filter_with_predicate_pushdown("));
	assert!(filter_arm.contains("input.plan"));
	assert!(filter_arm.contains("bound_predicate"));
	assert!(filter_arm.contains("&logical_types"));
	let update_helper = first_block(&binder, "async fn bind_update_source_for_predicate");
	assert!(update_helper.contains("self.catalog.logical_type_registry()"));
	assert!(update_helper.contains("plan_filter_with_predicate_pushdown("));
	assert!(update_helper.contains("&logical_types"));
	assert!(binder.contains("predicate_pushdown_plan(program, &scan)"));
	assert!(
		predicate_test.contains("where_common_pushable_conjunct_factors_out_with_minimal_residual")
	);
	assert!(
		update_test
			.contains("update_not_or_extraction_uses_pushed_source_scan_plus_minimal_residual")
	);
	assert!(
		delete_test.contains("delete_common_or_factor_uses_pushed_candidate_plus_minimal_residual")
	);
}

#[test]
pub(crate) fn adr094_common_or_factor_reconstructs_minimal_residual() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let common = brace_blocks_after_marker(&binder, "fn try_extract_common_or_factors")
		.into_iter()
		.next()
		.expect("try_extract_common_or_factors body")
		.1;
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	assert!(common.contains("branch_residuals"));
	assert!(common.contains("let mut used = vec![false; terms.len()]"));
	assert!(common.contains("combine_bound_conjuncts(source, &remaining)"));
	assert!(common.contains("combine_bound_programs_as_or(branch_residuals)"));
	assert!(common.contains("original_or_residual_factoring(source, root, pushed)"));
	assert!(
		predicate_test.contains("where_common_pushable_conjunct_factors_out_with_minimal_residual")
	);
	assert!(predicate_test.contains("where_multiple_common_factors_strip_from_each_or_branch"));
}

#[test]
pub(crate) fn adr094_not_or_reconstructs_remaining_branch_negations() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let not_or = brace_blocks_after_marker(&binder, "fn try_extract_not_or_branch_negations")
		.into_iter()
		.next()
		.expect("try_extract_not_or_branch_negations body")
		.1;
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	assert!(not_or.contains("pushed_negations"));
	assert!(not_or.contains("residual_negations"));
	assert!(not_or.contains("compile_pbc(&candidate).is_some()"));
	assert!(not_or.contains("combine_bound_programs_as_and(pushed_negations)"));
	assert!(not_or.contains("combine_bound_programs_as_and(residual_negations)"));
	assert!(not_or.contains("original_not_or_residual_factoring(source, not_root, pushed)"));
	assert!(!not_or.contains("BoundBinaryOp::And"));
	assert!(
		predicate_test
			.contains("where_not_or_minimal_residual_keeps_only_unpushed_branch_negations")
	);
}

#[test]
pub(crate) fn adr094_branch_strips_to_true_removes_common_or_residual() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let common = brace_blocks_after_marker(&binder, "fn try_extract_common_or_factors")
		.into_iter()
		.next()
		.expect("try_extract_common_or_factors body")
		.1;
	let predicate_test =
		std::fs::read_to_string(manifest_dir().join("tests/predicate_widening_test.rs"))
			.expect("read predicate tests");

	assert!(common.contains("if remaining.is_empty()"));
	assert!(common.contains("residual: None"));
	assert!(
		predicate_test
			.contains("where_common_factor_branch_strips_to_true_removes_residual_when_safe")
	);
}

#[test]
pub(crate) fn adr094_minimal_residual_preserves_078_kleene_gate() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");

	assert!(binder.contains("predicate_pushdown_emitted_splits_preserve_kleene_3vl"));
	assert!(binder.contains("property_common_or_three_branch_program"));
	assert!(binder.contains("property_or_branch_strips_to_true_program"));
	assert!(binder.contains("property_not_or_partial_push_program"));
	assert!(binder.contains("where_not_or_all_branch_negations_push_removes_residual"));
}

#[test]
pub(crate) fn adr094_no_new_ir_sql_pbc_wendelon_surface() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read ir");
	let execution_ir = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution ir");
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read pbc");
	let wendelon_pbc = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/pbc.rs"))
		.expect("read Wendelon pbc");

	for source in [&ir, &execution_ir] {
		assert!(!source.contains("MinimalResidual"));
		assert!(!source.contains("FactoredResidual"));
		assert!(!source.contains("ResidualMinimization"));
	}
	assert!(!manifest.lines().any(|line| {
		let line = line.trim_start();
		line.starts_with("syn =") || line.starts_with("syn ")
	}));
	for marker in ["OP_BOOLEAN_FACTOR", "OP_DE_MORGAN", "OP_FACTOR_OR"] {
		assert!(!pbc.contains(marker), "Toron PBC leaked opcode {marker}");
		assert!(
			!wendelon_pbc.contains(marker),
			"Wendelon PBC leaked opcode {marker}"
		);
	}
	for relative in [
		"src/sql_planner.rs",
		"src/sql_wire.rs",
		"src/storage.rs",
		"src/coordinator.rs",
		"src/coordinator/delete.rs",
		"src/coordinator/write.rs",
		"src/exec.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(relative)).expect("read source");
		for marker in [
			"ResidualMinimization",
			"MinimalResidual",
			"FactoredResidual",
			"try_extract_common_or_factors",
			"try_extract_not_or_branch_negations",
		] {
			assert!(!source.contains(marker), "{relative} leaked {marker}");
		}
	}
}

#[test]
pub(crate) fn adr078_no_new_sql_surface_pbc_opcode_wendelon_wire_storage_or_dependency_change() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read pbc");
	let wendelon_pbc = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/pbc.rs"))
		.expect("read Wendelon pbc");

	assert!(!manifest.lines().any(|line| {
		let line = line.trim_start();
		line.starts_with("syn =") || line.starts_with("syn ")
	}));
	for marker in ["OP_BOOLEAN_FACTOR", "OP_DE_MORGAN", "OP_FACTOR_OR"] {
		assert!(!pbc.contains(marker), "Toron PBC leaked opcode {marker}");
		assert!(
			!wendelon_pbc.contains(marker),
			"Wendelon PBC leaked opcode {marker}"
		);
	}
	for relative in [
		"src/storage.rs",
		"src/sql_wire.rs",
		"src/audit.rs",
		"src/telemetry.rs",
		"src/distributed/scheduler.rs",
		"src/distributed/task.rs",
	] {
		let path = manifest_dir().join(relative);
		if !path.exists() {
			continue;
		}
		let source = std::fs::read_to_string(&path).expect("read source");
		assert!(
			!source.contains("split_boolean_tree"),
			"{relative} leaked ADR078 binder helper"
		);
		assert!(
			!source.contains("BOOLEAN_FACTOR"),
			"{relative} leaked ADR078 budget marker"
		);
	}
}

#[test]
pub(crate) fn adr078_sqlparser_ast_remains_quarantined_to_sql_planner() {
	adr074_sqlparser_ast_remains_quarantined_to_sql_planner();
}

#[test]
pub(crate) fn adr078_prior_074_075_predicate_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for guard in [
		"adr074_in_list_is_first_class_expr_and_bound_node",
		"adr074_between_desugars_with_no_new_ir_or_opcode",
		"adr075_like_ilike_are_first_class_residual_only_predicates",
		"adr075_partial_pushdown_splits_bound_top_level_and_only",
		"adr075_partial_pushdown_covers_select_update_delete_scan_sites",
	] {
		assert!(
			architecture_test.contains(guard),
			"missing prior guard {guard}"
		);
	}
}

#[test]
pub(crate) fn adr074_alter_table_remains_unsupported() {
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

	assert!(!dispatch_body.contains("Statement::AlterTable"));
	assert!(dispatch_body.contains("unsupported SQL statement; only SELECT"));
}

#[test]
pub(crate) fn adr074_retargets_adr014_sql_in_absence_guard() {
	let architecture_test = architecture_corpus_source();

	assert!(architecture_test.contains("adr014_sql_in_absence_guard_is_retargeted_after_adr074"));
	let stale_guard = [
		"fn ",
		"adr014_does_not_add_sql_in",
		"_lowering_to_toron_planner",
	]
	.concat();
	assert!(!architecture_test.contains(&stale_guard));
}

#[test]
pub(crate) fn adr074_no_wendelon_storage_wire_worker_audit_or_telemetry_change() {
	let forbidden = [
		"Expr::InList",
		"BoundExprNode::InList",
		"OP_IN_LIST",
		"MAX_IN_LIST_ITEMS",
	];
	for relative in [
		"src/storage.rs",
		"src/sql_wire.rs",
		"src/audit.rs",
		"src/telemetry.rs",
		"src/distributed/scheduler.rs",
		"src/distributed/task.rs",
	] {
		let path = manifest_dir().join(relative);
		if !path.exists() {
			continue;
		}
		let source = std::fs::read_to_string(&path).expect("read source");
		for marker in forbidden {
			assert!(
				!source.contains(marker),
				"{relative} leaked ADR074 marker {marker}"
			);
		}
	}
}

#[test]
pub(crate) fn adr074_sqlparser_ast_remains_quarantined_to_sql_planner() {
	adr073_sqlparser_ast_is_quarantined_to_sql_planner();
}

#[test]
pub(crate) fn adr074_nilket_root_stays_runtime_free() {
	adr070_nilket_root_stays_runtime_free();
}

#[test]
pub(crate) fn adr074_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	let sources = [
		manifest_dir().join("src/ir.rs"),
		manifest_dir().join("src/ir/execution.rs"),
		manifest_dir().join("src/sql_planner.rs"),
		manifest_dir().join("src/pbc.rs"),
	];
	for path in sources {
		let source = std::fs::read_to_string(&path).expect("read source");
		assert!(
			!source.contains("InListV1"),
			"{} has V1 suffix",
			path.display()
		);
		assert!(
			!source.contains("NilDB"),
			"{} has stale brand",
			path.display()
		);
	}
}

#[test]
pub(crate) fn adr074_prior_010_023_024_025_026_029_030_034_036_064_073_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for guard in [
		"adr010", "adr023", "adr024", "adr025", "adr026", "adr029", "adr030", "adr034", "adr036",
		"adr064", "adr073",
	] {
		assert!(
			architecture_test.contains(guard),
			"missing prior guard family {guard}"
		);
	}
}

#[test]
pub(crate) fn adr095_empty_relation_is_execution_plan_only() {
	let execution = source("src/ir/execution.rs");
	let execution_plan = first_block(&execution, "pub enum ExecutionPlan");
	assert!(execution_plan.contains("EmptyRelation"));
	assert!(execution_plan.contains("schema: SchemaRef"));

	let logical = source("src/ir.rs");
	let logical_plan = first_block(&logical, "pub enum LogicalPlan");
	assert!(!logical_plan.contains("EmptyRelation"));
}

#[test]
pub(crate) fn adr095_filter_elision_runs_before_pbc_pushdown() {
	let binder = source("src/binder.rs");
	let filter_helper = first_block(&binder, "fn plan_filter_with_predicate_pushdown");
	let classifier = filter_helper
		.find("static_filter_class(&input, &original_predicate, logical_types)")
		.expect("static classifier call");
	let pushdown = filter_helper
		.find("predicate_pushdown_plan(predicate, &input)")
		.expect("predicate pushdown call");

	assert!(classifier < pushdown);
	assert!(filter_helper.contains("ExecutionPlan::EmptyRelation"));
	assert!(filter_helper.contains("fallback_profile.unwrap_or(QueryProfileHint::Scan)"));
}

#[test]
pub(crate) fn adr095_static_filter_classifier_is_error_conservative() {
	let binder = source("src/binder.rs");
	let atom_classifier = first_block(&binder, "fn static_filter_atom_class");
	let and_classifier = first_block(&binder, "fn static_filter_and_class");
	let closed_root = first_block(&binder, "fn closed_root_filter_class");
	let structural_null = first_block(&binder, "fn structural_non_nullable_is_null");

	assert!(atom_classifier.contains("program_has_column_references"));
	assert!(!atom_classifier.contains("BoundBinaryOp::And"));
	assert!(!atom_classifier.contains("BoundBinaryOp::Or"));
	assert!(!atom_classifier.contains("BoundUnaryOp::Not"));
	assert!(and_classifier.contains("collect_top_level_bound_and"));
	assert!(and_classifier.contains("op: BoundBinaryOp::And"));
	assert!(!and_classifier.contains("BoundBinaryOp::Or"));
	assert!(!and_classifier.contains("BoundUnaryOp::Not"));
	assert!(closed_root.contains("evaluate_bound_program_without_storage"));
	assert!(closed_root.matches("StaticFilterClass::Unknown").count() >= 4);
	assert!(!closed_root.contains("BoundBinaryOp::And"));
	assert!(!closed_root.contains("BoundBinaryOp::Or"));
	assert!(!closed_root.contains("BoundUnaryOp::Not"));
	assert!(structural_null.contains("BoundUnaryOp::IsNull"));
	assert!(structural_null.contains("BoundExprNode::Column(column)"));
	assert!(structural_null.contains("!column.nullable"));
}

#[test]
pub(crate) fn adr095_plan_walkers_handle_empty_relation_as_leaf() {
	for (relative, marker) in [
		("src/binder.rs", "fn terminal_exchange"),
		("src/binder.rs", "fn optimize_for_required"),
		("src/distributed/fragment.rs", "fn first_join"),
		("src/distributed/fragment.rs", "fn find_exchange"),
		("src/distributed/fragment.rs", "fn count_exchanges"),
		("src/distributed/fragment.rs", "fn first_scan_metadata"),
		(
			"src/distributed/fragment.rs",
			"fn replace_exchange_with_scan",
		),
		("src/distributed/fragment.rs", "fn replace_first_join"),
		("src/distributed/scheduler.rs", "fn first_scan_fingerprint"),
		("src/server/mod.rs", "fn join_strategy_label"),
		("src/server/mod.rs", "fn contains_join"),
		("src/server/mod.rs", "fn operator_kind_label"),
		("src/server/mod.rs", "fn contains_broadcast_join"),
		("src/server/mod.rs", "fn scan_schema_for_table_id"),
	] {
		let source = source(relative);
		let block = first_block(&source, marker);
		assert!(
			block.contains("ExecutionPlan::EmptyRelation"),
			"{relative} {marker} does not name EmptyRelation"
		);
	}

	let server = source("src/server/mod.rs");
	let operator_kind = first_block(&server, "fn operator_kind_label");
	assert!(operator_kind.contains("\"empty_relation\""));
}

#[test]
pub(crate) fn adr095_no_sql_pbc_wendelon_storage_surface() {
	for relative in [
		"src/ir.rs",
		"src/sql_planner.rs",
		"src/pbc.rs",
		"src/storage.rs",
	] {
		let source = source(relative);
		if relative == "src/ir.rs" {
			let logical_plan = first_block(&source, "pub enum LogicalPlan");
			assert!(!logical_plan.contains("EmptyRelation"));
		} else {
			assert!(
				!source.contains("EmptyRelation"),
				"{relative} gained public EmptyRelation surface"
			);
		}
	}

	let wendelon_offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("EmptyRelation"))
		.collect::<Vec<_>>();
	assert!(
		wendelon_offenders.is_empty(),
		"EmptyRelation leaked to Wendelon:\n{}",
		format_offenders(&wendelon_manifest_dir(), &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr097_refinement_elision_uses_durable_registry_not_seed() {
	let binder = source("src/binder.rs");
	let filter_helper = first_block(&binder, "fn plan_filter_with_predicate_pushdown");
	let scan_atom = first_block(&binder, "fn stored_scan_refinement_atom");
	let refinement_helper = first_block(&binder, "fn refinement_static_filter_class");

	assert!(filter_helper.contains("logical_types: &LogicalTypeRegistry"));
	assert!(
		filter_helper.contains("static_filter_class(&input, &original_predicate, logical_types)")
	);
	assert!(binder.contains("self.catalog.logical_type_registry()"));
	assert!(scan_atom.contains("logical_types.spec(logical_type)"));
	assert!(!scan_atom.contains("logical_type_spec"));
	assert!(!refinement_helper.contains("logical_type_spec"));
}

#[test]
pub(crate) fn adr097_refinement_elision_requires_stored_scan_column_provenance() {
	let binder = source("src/binder.rs");
	let scan_atom = first_block(&binder, "fn stored_scan_refinement_atom");
	let receipt = first_block(&binder, "fn stored_refinement_receipt");
	let logical_type_ddl = source("tests/logical_type_ddl_test.rs");

	assert!(receipt.contains("ExecutionPlan::Scan"));
	assert!(receipt.contains("projection: None"));
	assert!(scan_atom.contains("BoundExprNode::Column"));
	assert!(scan_atom.contains("node_logical_type(column_node)"));
	assert!(scan_atom.contains("refinement"));
	assert!(logical_type_ddl.contains("cast_ascribed_projected_column_does_not_elide"));
	assert!(logical_type_ddl.contains("refinement_through_transparent_project_elides"));
	assert!(!logical_type_ddl.contains("refinement_on_non_scan_input_does_not_elide"));
}

#[test]
pub(crate) fn adr097_refinement_elision_only_from_durable_stored_scan_receipts() {
	let binder = source("src/binder.rs");
	let logical_type_ddl = source("tests/logical_type_ddl_test.rs");
	let scan_atom = first_block(&binder, "fn stored_scan_refinement_atom");
	let receipt = first_block(&binder, "fn stored_refinement_receipt");

	assert!(receipt.contains("projection: None"));
	assert!(scan_atom.contains("logical_types.spec(logical_type)"));
	assert!(logical_type_ddl.contains("refinement_elision_uses_durable_registry_not_seed_lookup"));
	assert!(logical_type_ddl.contains("naked_or_unrefined_column_does_not_elide"));
	assert!(logical_type_ddl.contains("nullable_refined_column_is_null_does_not_elide"));
}

#[test]
pub(crate) fn adr097_refinement_elision_reuses_095_empty_relation() {
	let binder = source("src/binder.rs");
	let execution = source("src/ir/execution.rs");
	let execution_plan = first_block(&execution, "pub enum ExecutionPlan");
	let filter_helper = first_block(&binder, "fn plan_filter_with_predicate_pushdown");

	assert!(filter_helper.contains("ExecutionPlan::EmptyRelation"));
	assert!(execution_plan.contains("CreateLogicalType"));
	assert!(!execution_plan.contains("RefinementElision"));
	assert!(!execution_plan.contains("StaticReject"));
}

#[test]
pub(crate) fn adr097_static_filter_class_is_structural_3vl_and_error_conservative() {
	let binder = source("src/binder.rs");
	let atom_classifier = first_block(&binder, "fn static_filter_atom_class");
	let refinement_helper = first_block(&binder, "fn refinement_static_filter_class");
	let scan_atom = first_block(&binder, "fn stored_scan_refinement_atom");

	assert!(atom_classifier.contains("structural_non_nullable_is_null(predicate)"));
	assert!(
		atom_classifier.contains("refinement_static_filter_class(input, predicate, logical_types)")
	);
	assert!(!refinement_helper.contains("evaluate_bound_program_without_storage"));
	assert!(!refinement_helper.contains("predicate_pushdown_plan"));
	assert!(!refinement_helper.contains("compile_pbc"));
	assert!(!scan_atom.contains("Executor::"));
}

#[test]
pub(crate) fn adr098_and_decomposition_runs_unified_atom_classifier() {
	let binder = source("src/binder.rs");
	let classifier = first_block(&binder, "fn static_filter_class");
	let and_classifier = first_block(&binder, "fn static_filter_and_class");
	let atom_classifier = first_block(&binder, "fn static_filter_atom_class");

	assert!(classifier.contains("static_filter_and_class(input, predicate, logical_types)"));
	assert!(classifier.contains("static_filter_atom_class(input, predicate, logical_types)"));
	assert!(
		and_classifier.contains("collect_top_level_bound_and(predicate, root, &mut conjuncts)")
	);
	assert!(and_classifier.contains("isolate_bound_subgraph(predicate, conjunct)"));
	assert!(and_classifier.contains("static_filter_atom_class(input, &isolated, logical_types)"));
	assert!(atom_classifier.contains("structural_non_nullable_is_null(predicate)"));
	assert!(
		atom_classifier.contains("refinement_static_filter_class(input, predicate, logical_types)")
	);
	assert!(atom_classifier.contains("closed_root_filter_class(predicate)"));
}

#[test]
pub(crate) fn adr098_and_elision_requires_error_free_siblings() {
	let binder = source("src/binder.rs");
	let and_classifier = first_block(&binder, "fn static_filter_and_class");
	let conjunct_checker = first_block(&binder, "fn static_elision_error_free_conjunct");
	let node_checker = first_block(&binder, "fn static_elision_error_free_node");
	let operand_checker = first_block(&binder, "fn static_elision_error_free_operand");

	assert!(and_classifier.contains("count_bound_subgraph_nodes(predicate, root)"));
	assert!(and_classifier.contains("BOOLEAN_FACTOR_MAX_NODES"));
	assert!(and_classifier.contains("STATIC_ELISION_MAX_AND_CONJUNCTS"));
	assert!(and_classifier.contains("static_elision_error_free_conjunct(&isolated)"));
	assert!(conjunct_checker.contains("closed_root_filter_class(predicate)"));
	assert!(conjunct_checker.contains("StaticFilterClass::Unknown"));
	for marker in [
		"BoundBinaryOp::Eq",
		"BoundBinaryOp::Neq",
		"BoundBinaryOp::Lt",
		"BoundBinaryOp::Lte",
		"BoundBinaryOp::Gt",
		"BoundBinaryOp::Gte",
		"op: BoundBinaryOp::And",
		"BoundUnaryOp::IsNull | BoundUnaryOp::IsNotNull",
		"_ => false",
	] {
		assert!(
			node_checker.contains(marker),
			"missing allowlist marker {marker}"
		);
	}
	assert!(operand_checker.contains("BoundExprNode::Column(_)"));
	assert!(operand_checker.contains("BoundExprNode::Literal(_)"));
	for forbidden in [
		"BoundBinaryOp::Or",
		"BoundUnaryOp::Not",
		"BoundBinaryOp::Add",
		"BoundBinaryOp::Sub",
		"BoundBinaryOp::Mul",
		"BoundBinaryOp::Div",
		"BoundExprNode::Cast",
		"BoundExprNode::TryCast",
		"BoundExprNode::InList",
		"BoundExprNode::Like",
	] {
		assert!(
			!node_checker.contains(forbidden),
			"unsafe marker {forbidden} was allowlisted"
		);
	}
}

#[test]
pub(crate) fn adr098_and_decomposition_inherits_097_durable_registry_and_scan_provenance() {
	let binder = source("src/binder.rs");
	let and_classifier = first_block(&binder, "fn static_filter_and_class");
	let scan_atom = first_block(&binder, "fn stored_scan_refinement_atom");
	let receipt = first_block(&binder, "fn stored_refinement_receipt");

	assert!(and_classifier.contains("static_filter_atom_class(input, &isolated, logical_types)"));
	assert!(receipt.contains("ExecutionPlan::Scan"));
	assert!(receipt.contains("projection: None"));
	assert!(scan_atom.contains("BoundExprNode::Column"));
	assert!(scan_atom.contains("node_logical_type(column_node)"));
	assert!(scan_atom.contains("logical_types.spec(logical_type)"));
	assert!(!scan_atom.contains("logical_type_spec"));
}

#[test]
pub(crate) fn adr098_and_elision_reuses_095_empty_relation_no_alwayspass() {
	let binder = source("src/binder.rs");
	let filter_helper = first_block(&binder, "fn plan_filter_with_predicate_pushdown");
	let and_classifier = first_block(&binder, "fn static_filter_and_class");

	assert!(filter_helper.contains("ExecutionPlan::EmptyRelation"));
	assert!(filter_helper.contains("StaticFilterClass::AlwaysReject"));
	assert!(!filter_helper.contains("StaticFilterClass::AlwaysPass"));
	assert!(and_classifier.contains("StaticFilterClass::AlwaysPass | StaticFilterClass::Unknown"));
	assert!(and_classifier.contains("Some(StaticFilterClass::Unknown)"));
}

#[test]
pub(crate) fn adr098_no_boolean_algebra_beyond_and() {
	let binder = source("src/binder.rs");
	let and_classifier = first_block(&binder, "fn static_filter_and_class");
	let node_checker = first_block(&binder, "fn static_elision_error_free_node");

	assert!(and_classifier.contains("op: BoundBinaryOp::And"));
	assert!(and_classifier.contains("collect_top_level_bound_and"));
	assert!(node_checker.contains("op: BoundBinaryOp::And"));
	for forbidden in [
		"BoundBinaryOp::Or",
		"BoundUnaryOp::Not",
		"combine_bound_programs_as_or",
		"try_extract_common_or_factors",
		"try_extract_not_or_branch_negations",
	] {
		assert!(!and_classifier.contains(forbidden));
		assert!(!node_checker.contains(forbidden));
	}
}

#[test]
pub(crate) fn adr098_no_wendelon_pbc_executor_storage_or_wire_change() {
	let binder = source("src/binder.rs");
	let pbc = source("src/pbc.rs");
	let executor = source("src/exec.rs");
	let storage = source("src/storage.rs");
	let coordinator = source("src/coordinator.rs");
	let wendelon_offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("static_filter_and_class")
				|| line.contains("static_filter_atom_class")
				|| line.contains("static_elision_error_free")
				|| line.contains("STATIC_ELISION_MAX_AND_CONJUNCTS")
		})
		.collect::<Vec<_>>();

	for marker in [
		"fn static_filter_and_class",
		"fn static_filter_atom_class",
		"fn static_elision_error_free_conjunct",
		"STATIC_ELISION_MAX_AND_CONJUNCTS",
	] {
		assert!(binder.contains(marker));
		assert!(!pbc.contains(marker));
		assert!(!executor.contains(marker));
		assert!(!storage.contains(marker));
		assert!(!coordinator.contains(marker));
	}
	assert!(
		wendelon_offenders.is_empty(),
		"ADR098 helper leaked to Wendelon:\n{}",
		format_offenders(&wendelon_manifest_dir(), &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr100_derived_table_source_is_narrow_not_general_subquery_support() {
	let planner = source("src/sql_planner.rs");
	let lower_select = first_block(&planner, "fn lower_select");
	let table_factor = first_block(&planner, "fn lower_table_factor");
	let query_shape = first_block(&planner, "fn validate_derived_query_shape");
	let select_shape = first_block(&planner, "fn validate_derived_select_shape");
	let logical_type_ddl = source("tests/logical_type_ddl_test.rs");

	assert!(table_factor.contains("TableFactor::Derived"));
	for marker in [
		"lateral",
		"sample",
		"derived tables require an alias",
		"derived table alias AT is not supported",
		"lower_derived_table",
	] {
		assert!(
			table_factor.contains(marker),
			"missing derived arm marker {marker}"
		);
	}
	for marker in [
		"query.with.is_some()",
		"query.fetch.is_some()",
		"SetExpr::Select",
		"unsupported derived table query body",
	] {
		assert!(
			query_shape.contains(marker),
			"missing query-shape marker {marker}"
		);
	}
	assert!(!query_shape.contains("query.order_by.is_some()"));
	assert!(!query_shape.contains("query_has_limit_or_offset"));
	for marker in [
		"projection_contains_window_function",
		"select.from.len() != 1",
		"!source.joins.is_empty()",
		"TableFactor::Derived",
		"unsupported derived table source",
	] {
		assert!(
			select_shape.contains(marker),
			"missing select-shape marker {marker}"
		);
	}
	assert!(lower_select.contains("table_factor_is_derived"));
	assert!(lower_select.contains("joins around derived tables are not supported"));
	assert!(
		logical_type_ddl.contains("derived_table_source_scope_rejects_general_subquery_features")
	);
}

#[test]
pub(crate) fn adr100_refinement_provenance_crosses_only_transparent_project() {
	let binder = source("src/binder.rs");
	let receipt = first_block(&binder, "fn stored_refinement_receipt");
	let child_ordinal = first_block(&binder, "fn transparent_project_child_ordinal");

	assert!(receipt.contains("ExecutionPlan::Project"));
	assert!(receipt.contains("transparent_project_child_ordinal(exprs, ordinal)"));
	assert!(receipt.contains("stored_refinement_receipt(input, child, depth + 1)"));
	assert!(receipt.contains("ExecutionPlan::Scan"));
	assert!(receipt.contains("projection: None"));
	assert!(receipt.contains("decode_column_logical_type(child_field)"));
	assert!(receipt.contains("exprs.root_logical_type(ordinal.0 as usize)"));
	assert!(child_ordinal.contains("exprs.roots.get(ordinal.0 as usize)"));
	assert!(child_ordinal.contains("BoundExprNode::Column(column)"));
	assert!(child_ordinal.contains("_ => None"));
}

#[test]
pub(crate) fn adr100_refinement_receipt_root_is_scan_or_transparent_project_chain() {
	let binder = source("src/binder.rs");
	let scan_atom = first_block(&binder, "fn stored_scan_refinement_atom");
	let receipt = first_block(&binder, "fn stored_refinement_receipt");
	let logical_type_ddl = source("tests/logical_type_ddl_test.rs");

	assert!(scan_atom.contains("stored_refinement_receipt(input, column.ordinal, 0)"));
	assert!(!scan_atom.contains("let ExecutionPlan::Scan"));
	assert!(receipt.contains("ExecutionPlan::Scan"));
	assert!(receipt.contains("ExecutionPlan::Project"));
	assert!(receipt.contains("_ => None"));
	assert!(logical_type_ddl.contains("refinement_through_transparent_project_elides"));
	assert!(logical_type_ddl.contains("cast_ascribed_projected_column_does_not_elide"));
	assert!(!logical_type_ddl.contains("refinement_on_non_scan_input_does_not_elide"));
}

#[test]
pub(crate) fn adr100_refinement_receipt_reuses_089_identity_no_redrive() {
	let binder = source("src/binder.rs");
	let scan_atom = first_block(&binder, "fn stored_scan_refinement_atom");
	let receipt = first_block(&binder, "fn stored_refinement_receipt");

	assert!(scan_atom.contains("predicate.node_logical_type(column_node)"));
	assert!(scan_atom.contains("logical_type.is_naked()"));
	assert!(scan_atom.contains("logical_types.spec(logical_type)"));
	assert!(scan_atom.contains("receipt.field"));
	assert!(!scan_atom.contains("logical_type_spec"));
	assert!(!scan_atom.contains("logical_type_name"));
	assert!(!scan_atom.contains("CUSTOMER_ID_LOGICAL_TYPE"));
	assert!(!scan_atom.contains("field.name"));
	assert!(!receipt.contains("logical_types.spec"));
	assert!(!receipt.contains("logical_type_spec"));
}

#[test]
pub(crate) fn adr100_eager_error_fence_policy_unchanged() {
	let binder = source("src/binder.rs");
	let classifier = first_block(&binder, "fn static_filter_class");
	let and_classifier = first_block(&binder, "fn static_filter_and_class");
	let conjunct_checker = first_block(&binder, "fn static_elision_error_free_conjunct");
	let logical_type_ddl = source("tests/logical_type_ddl_test.rs");

	assert!(classifier.contains("static_filter_and_class(input, predicate, logical_types)"));
	assert!(classifier.contains("static_filter_atom_class(input, predicate, logical_types)"));
	assert!(and_classifier.contains("static_elision_error_free_conjunct(&isolated)"));
	assert!(and_classifier.contains("StaticFilterClass::AlwaysPass | StaticFilterClass::Unknown"));
	assert!(conjunct_checker.contains("StaticFilterClass::Unknown"));
	assert!(logical_type_ddl.contains("derived_table_outer_filter_keeps_098_eager_error_fence"));
}

#[test]
pub(crate) fn adr100_no_projected_scan_receipt_until_elision_has_a_consumer() {
	let binder = source("src/binder.rs");
	let bind_entry = first_block(&binder, "pub async fn bind");
	let bind_with_trace = first_block(&binder, "pub(crate) async fn bind_with_trace");
	let receipt = first_block(&binder, "fn stored_refinement_receipt");

	assert!(bind_entry.contains("bind_with_trace(plan, catalog, config).await?"));
	assert!(bind_with_trace.contains("bind_plan_with_trace(plan, &mut trace)"));
	assert!(bind_with_trace.contains("push_down_scan_projection(plan.plan)?"));
	assert!(receipt.contains("projection: None"));
	assert!(!receipt.contains("projection: Some"));
}

#[test]
pub(crate) fn adr100_no_ir_exec_pbc_storage_or_wendelon_change() {
	let logical = source("src/ir.rs");
	let logical_plan = first_block(&logical, "pub enum LogicalPlan");
	let execution = source("src/ir/execution.rs");
	let execution_plan = first_block(&execution, "pub enum ExecutionPlan");
	let planner = source("src/sql_planner.rs");
	let binder = source("src/binder.rs");
	let pbc = source("src/pbc.rs");
	let executor = source("src/exec.rs");
	let storage = source("src/storage.rs");
	let coordinator = source("src/coordinator.rs");
	let wendelon_offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("stored_refinement_receipt")
				|| line.contains("transparent_project_child_ordinal")
				|| line.contains("validate_derived_query_shape")
				|| line.contains("TableFactor::Derived")
		})
		.collect::<Vec<_>>();

	assert!(!logical_plan.contains("Derived"));
	assert!(!execution_plan.contains("Derived"));
	assert!(planner.contains("TableFactor::Derived"));
	assert!(binder.contains("stored_refinement_receipt"));
	for marker in [
		"stored_refinement_receipt",
		"transparent_project_child_ordinal",
		"validate_derived_query_shape",
		"TableFactor::Derived",
	] {
		assert!(!pbc.contains(marker), "{marker} leaked to PBC");
		assert!(!executor.contains(marker), "{marker} leaked to executor");
		assert!(!storage.contains(marker), "{marker} leaked to storage");
		assert!(
			!coordinator.contains(marker),
			"{marker} leaked to coordinator"
		);
	}
	assert!(
		wendelon_offenders.is_empty(),
		"ADR100 helper leaked to Wendelon:\n{}",
		format_offenders(&wendelon_manifest_dir(), &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr104_alias_column_lists_are_single_bind_time_mechanism() {
	let planner = source("src/sql_planner.rs");
	let alias_plan = first_block(&planner, "fn alias_plan");
	let lower_derived = first_block(&planner, "fn lower_derived_table");
	let table_factor = first_block(&planner, "fn lower_table_factor");
	let logical = source("src/ir.rs");
	let logical_plan = first_block(&logical, "pub enum LogicalPlan");

	assert!(!planner.contains("fn apply_derived_alias_column_list"));
	assert!(!planner.contains("derived table alias column list arity mismatch"));
	assert!(planner.contains("fn alias_column_names"));
	assert!(alias_plan.contains("LogicalPlan::Alias"));
	assert!(alias_plan.contains("TableRef::new(alias.name.value.clone())"));
	assert!(alias_plan.contains("columns: alias_column_names(alias)?"));
	assert!(logical_plan.contains("columns: Vec<String>"));
	assert!(lower_derived.contains("alias_plan(plan, Some(alias))"));
	assert!(table_factor.contains("alias_plan(plan, alias.as_ref())"));
}

#[test]
pub(crate) fn adr104_binder_alias_columns_build_ordinal_identity_project() {
	let binder = source("src/binder.rs");
	let bind_plan = first_block(&binder, "fn bind_plan_with_trace");
	let helper = first_block(&binder, "fn bind_alias_column_rename");

	assert!(bind_plan.contains("LogicalPlan::Alias {"));
	assert!(bind_plan.contains("bind_alias_column_rename(input.plan, columns)"));
	assert!(helper.contains("columns.len() != schema.fields().len()"));
	assert!(helper.contains("BoundExprProgramBuilder::new()"));
	assert!(helper.contains("decode_column_logical_type(field)"));
	assert!(helper.contains("ColumnOrdinal(u32::try_from(idx)"));
	assert!(helper.contains("builder.append_column(ResolvedColumn"));
	assert!(helper.contains("builder.set_node_logical_type(node, logical_type)"));
	assert!(helper.contains("schema_for_exprs_with_output_names"));
	assert!(helper.contains("ExecutionPlan::Project"));
	assert!(!helper.contains("Expr::column"));
}

#[test]
pub(crate) fn adr104_alias_column_renames_reuse_transparent_project_receipt() {
	let binder = source("src/binder.rs");
	let child_ordinal = first_block(&binder, "fn transparent_project_child_ordinal");
	let receipt = first_block(&binder, "fn stored_refinement_receipt");
	let helper = first_block(&binder, "fn bind_alias_column_rename");
	let logical_type_ddl = source("tests/logical_type_ddl_test.rs");

	assert!(
		logical_type_ddl
			.contains("alias_columns_preserve_refinement_elision_base_explicit_and_wildcard")
	);
	assert!(receipt.contains("transparent_project_child_ordinal(exprs, ordinal)"));
	assert!(receipt.contains("decode_column_logical_type(child_field)"));
	assert!(child_ordinal.contains("BoundExprNode::Column(column)"));
	assert!(helper.contains("builder.append_column(ResolvedColumn"));
	assert!(helper.contains("decode_column_logical_type(field)"));
	assert!(helper.contains("set_node_logical_type(node, logical_type)"));
}

#[test]
pub(crate) fn adr104_no_execution_pbc_storage_wendelon_information_schema_surface() {
	let logical = source("src/ir.rs");
	let logical_plan = first_block(&logical, "pub enum LogicalPlan");
	let execution = source("src/ir/execution.rs");
	let execution_plan = first_block(&execution, "pub enum ExecutionPlan");
	let binder = source("src/binder.rs");
	let information_schema = first_block(&binder, "async fn bind_information_schema_relation");
	let pbc = source("src/pbc.rs");
	let executor = source("src/exec.rs");
	let storage = source("src/storage.rs");
	let coordinator = source("src/coordinator.rs");
	let runtime_markers = [
		"alias_column_names",
		"bind_alias_column_rename",
		"AliasColumn",
		"DerivedAlias",
	];
	let wendelon_offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| runtime_markers.iter().any(|marker| line.contains(marker)))
		.collect::<Vec<_>>();

	assert!(logical_plan.contains("Alias"));
	assert!(logical_plan.contains("columns: Vec<String>"));
	assert!(!execution_plan.contains("Alias"));
	for marker in runtime_markers {
		assert!(
			!execution_plan.contains(marker),
			"{marker} leaked to ExecutionPlan"
		);
		assert!(
			!information_schema.contains(marker),
			"{marker} leaked to information_schema"
		);
		assert!(!pbc.contains(marker), "{marker} leaked to PBC");
		assert!(!executor.contains(marker), "{marker} leaked to executor");
		assert!(!storage.contains(marker), "{marker} leaked to storage");
		assert!(
			!coordinator.contains(marker),
			"{marker} leaked to coordinator"
		);
	}
	assert!(
		wendelon_offenders.is_empty(),
		"ADR104 alias-column helper leaked to Wendelon:\n{}",
		format_offenders(&wendelon_manifest_dir(), &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr104_alias_match_sweep_is_complete() {
	for relative in [
		"src/binder.rs",
		"src/sql_planner.rs",
		"src/exec.rs",
		"src/audit.rs",
		"src/daemon.rs",
		"tests/rune_relation_handles_test.rs",
	] {
		let source = source(relative);
		for stale in [
			"LogicalPlan::Alias { input, alias }",
			"LogicalPlan::Alias { input, alias: _ }",
		] {
			assert!(
				!source.contains(stale),
				"{relative} contains stale Alias match {stale}"
			);
		}
	}
}

#[test]
pub(crate) fn adr103_alias_column_guards_retarget_to_104_unified_path() {
	let planner = source("src/sql_planner.rs");
	let logical_type_ddl = source("tests/logical_type_ddl_test.rs");

	assert!(!planner.contains("fn apply_derived_alias_column_list"));
	assert!(logical_type_ddl.contains("alias_columns_explicit_derived_rename_output"));
	assert!(logical_type_ddl.contains("alias_columns_wildcard_derived_rename_output"));
	assert!(logical_type_ddl.contains("alias_columns_arity_mismatch_rejects_at_bind"));
}

#[test]
pub(crate) fn adr105_project_coalescing_lives_in_existing_post_bind_optimizer() {
	let binder = source("src/binder.rs");
	let bind_entry = first_block(&binder, "pub async fn bind");
	let bind_with_trace = first_block(&binder, "pub(crate) async fn bind_with_trace");
	let optimizer = first_block(&binder, "fn optimize_for_required");

	assert!(bind_entry.contains("bind_with_trace(plan, catalog, config).await?"));
	assert!(bind_with_trace.contains("push_down_scan_projection(plan.plan)?"));
	assert!(optimizer.contains("collapse_adjacent_identity_project(input, exprs, output_schema)"));
}

#[test]
pub(crate) fn adr105_coalescing_is_adjacent_project_only() {
	let binder = source("src/binder.rs");
	let helper = first_block(&binder, "fn collapse_adjacent_identity_project");

	assert!(helper.contains("ExecutionPlan::Project { output_schema, .. }"));
	for marker in [
		"ExecutionPlan::Scan",
		"ExecutionPlan::SystemTable",
		"ExecutionPlan::EmptyRelation",
		"ExecutionPlan::Filter",
		"ExecutionPlan::Aggregate",
		"ExecutionPlan::Exchange",
		"ExecutionPlan::Sort",
		"ExecutionPlan::Join",
		"ExecutionPlan::Update",
		"ExecutionPlan::InsertSelect",
	] {
		assert!(
			!helper.contains(marker),
			"ADR105 coalescer should not directly remove {marker}"
		);
	}
}

#[test]
pub(crate) fn adr105_identity_requires_ordinal_and_metadata_match() {
	let binder = source("src/binder.rs");
	let helper = first_block(&binder, "fn is_metadata_transparent_identity_project");

	assert!(helper.contains("BoundExprNode::Column(column)"));
	assert!(helper.contains("ColumnOrdinal(u32::try_from(idx)"));
	assert!(helper.contains("column.ordinal"));
	assert!(helper.contains("field.data_type()"));
	assert!(helper.contains("field.is_nullable()"));
	assert!(helper.contains("parent_exprs.root_logical_type(idx)"));
	assert!(helper.contains("decode_column_logical_type(field)"));
}

#[test]
pub(crate) fn adr105_identity_check_ignores_names_parent_schema_wins() {
	let binder = source("src/binder.rs");
	let helper = first_block(&binder, "fn is_metadata_transparent_identity_project");
	let coalescer = first_block(&binder, "fn collapse_adjacent_identity_project");

	assert!(!helper.contains("field.name"));
	assert!(coalescer.contains("exprs: child_exprs"));
	assert!(coalescer.contains("output_schema: parent_output_schema"));
}

#[test]
pub(crate) fn adr105_no_sql_ir_exec_pbc_storage_wendelon_information_schema_surface() {
	let planner = source("src/sql_planner.rs");
	let logical = source("src/ir.rs");
	let logical_plan = first_block(&logical, "pub enum LogicalPlan");
	let execution = source("src/ir/execution.rs");
	let execution_plan = first_block(&execution, "pub enum ExecutionPlan");
	let binder = source("src/binder.rs");
	let information_schema = first_block(&binder, "async fn bind_information_schema_relation");
	let pbc = source("src/pbc.rs");
	let executor = source("src/exec.rs");
	let storage = source("src/storage.rs");
	let coordinator = source("src/coordinator.rs");
	let markers = [
		"collapse_adjacent_identity_project",
		"is_metadata_transparent_identity_project",
	];
	let wendelon_offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker)))
		.collect::<Vec<_>>();

	for marker in markers {
		assert!(!planner.contains(marker), "{marker} leaked to SQL planner");
		assert!(
			!logical_plan.contains(marker),
			"{marker} leaked to LogicalPlan"
		);
		assert!(
			!execution_plan.contains(marker),
			"{marker} leaked to ExecutionPlan"
		);
		assert!(
			!information_schema.contains(marker),
			"{marker} leaked to information_schema"
		);
		assert!(!pbc.contains(marker), "{marker} leaked to PBC");
		assert!(!executor.contains(marker), "{marker} leaked to executor");
		assert!(!storage.contains(marker), "{marker} leaked to storage");
		assert!(
			!coordinator.contains(marker),
			"{marker} leaked to coordinator"
		);
	}
	assert!(
		wendelon_offenders.is_empty(),
		"ADR105 project coalescing helper leaked to Wendelon:\n{}",
		format_offenders(&wendelon_manifest_dir(), &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr106_derived_aggregates_reuse_existing_aggregate_ir() {
	let logical = source("src/ir.rs");
	let logical_plan = first_block(&logical, "pub enum LogicalPlan");
	let execution = source("src/ir/execution.rs");
	let execution_plan = first_block(&execution, "pub enum ExecutionPlan");
	let planner = source("src/sql_planner.rs");
	let lower_derived = first_block(&planner, "fn lower_derived_table");

	assert!(logical_plan.contains("Aggregate"));
	assert!(execution_plan.contains("Aggregate"));
	assert!(planner.contains("LogicalPlan::Aggregate"));
	assert!(!logical_plan.contains("DerivedAggregate"));
	assert!(!execution_plan.contains("DerivedAggregate"));
	assert!(!lower_derived.contains("DerivedAggregate"));
}

#[test]
pub(crate) fn adr106_lower_derived_table_lifts_only_aggregate_fence() {
	let planner = source("src/sql_planner.rs");
	let lower_derived = first_block(&planner, "fn lower_derived_table");

	for marker in [
		"validate_derived_query_shape(subquery)",
		"validate_derived_select_shape(select)",
		"self.lower_select(select)?",
		"alias_plan(plan, Some(alias))",
	] {
		assert!(
			lower_derived.contains(marker),
			"lower_derived_table missing {marker}"
		);
	}
	assert!(!lower_derived.contains("lower_query("));
	assert!(!lower_derived.contains("aggregate_output_catalog.is_some()"));
	assert!(!lower_derived.contains("aggregate derived tables are not supported"));
}

#[test]
pub(crate) fn adr106_general_derived_source_fences_remain() {
	let planner = source("src/sql_planner.rs");
	let query_shape = first_block(&planner, "fn validate_derived_query_shape");
	let select_shape = first_block(&planner, "fn validate_derived_select_shape");

	for marker in [
		"query.with.is_some()",
		"SetExpr::Select",
		"unsupported derived table query body",
	] {
		assert!(
			query_shape.contains(marker),
			"missing query-shape fence {marker}"
		);
	}
	assert!(!query_shape.contains("query.order_by.is_some()"));
	assert!(!query_shape.contains("query_has_limit_or_offset"));
	for marker in [
		"projection_contains_window_function",
		"select.from.len() != 1",
		"!source.joins.is_empty()",
		"TableFactor::Derived",
		"unsupported derived table source",
	] {
		assert!(
			select_shape.contains(marker),
			"missing derived select fence {marker}"
		);
	}
	for removed in [
		"projection_contains_aggregate_call",
		"!group_by_is_empty(&select.group_by)",
		"select.having.is_some()",
	] {
		assert!(
			!select_shape.contains(removed),
			"aggregate fence should be lifted from derived select shape: {removed}"
		);
	}
}

#[test]
pub(crate) fn adr106_alias_column_rename_remains_generic_bound_project() {
	let planner = source("src/sql_planner.rs");
	let lower_derived = first_block(&planner, "fn lower_derived_table");
	let binder = source("src/binder.rs");
	let bind_plan = first_block(&binder, "fn bind_plan_with_trace");
	let helper = first_block(&binder, "fn bind_alias_column_rename");
	let logical_type_ddl = source("tests/logical_type_ddl_test.rs");

	assert!(lower_derived.contains("alias_plan(plan, Some(alias))"));
	assert!(bind_plan.contains("LogicalPlan::Alias {"));
	assert!(bind_plan.contains("bind_alias_column_rename(input.plan, columns)"));
	assert!(helper.contains("ExecutionPlan::Project"));
	assert!(logical_type_ddl.contains("aggregate_derived_alias_columns_rename_outputs"));
	for marker in [
		"aggregate_alias",
		"bind_aggregate_alias",
		"rename_aggregate",
		"apply_aggregate_alias",
	] {
		assert!(
			!planner.contains(marker) && !binder.contains(marker),
			"aggregate-local alias helper leaked in via {marker}"
		);
	}
}

#[test]
pub(crate) fn adr106_no_ir_exec_pbc_storage_wendelon_information_schema_surface() {
	let logical = source("src/ir.rs");
	let logical_plan = first_block(&logical, "pub enum LogicalPlan");
	let execution = source("src/ir/execution.rs");
	let execution_plan = first_block(&execution, "pub enum ExecutionPlan");
	let binder = source("src/binder.rs");
	let information_schema = first_block(&binder, "async fn bind_information_schema_relation");
	let pbc = source("src/pbc.rs");
	let executor = source("src/exec.rs");
	let storage = source("src/storage.rs");
	let coordinator = source("src/coordinator.rs");
	let rune = source("src/rune_engine.rs");
	let markers = [
		"DerivedAggregate",
		"aggregate_derived",
		"bind_aggregate_derived",
		"lower_aggregate_derived",
		"aggregate derived tables",
	];
	let wendelon_offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker)))
		.collect::<Vec<_>>();

	for marker in markers {
		assert!(
			!logical_plan.contains(marker),
			"{marker} leaked to LogicalPlan"
		);
		assert!(
			!execution_plan.contains(marker),
			"{marker} leaked to ExecutionPlan"
		);
		assert!(
			!information_schema.contains(marker),
			"{marker} leaked to information_schema"
		);
		assert!(!pbc.contains(marker), "{marker} leaked to PBC");
		assert!(!executor.contains(marker), "{marker} leaked to executor");
		assert!(!storage.contains(marker), "{marker} leaked to storage");
		assert!(
			!coordinator.contains(marker),
			"{marker} leaked to coordinator"
		);
		assert!(!rune.contains(marker), "{marker} leaked to Rune");
	}
	assert!(
		wendelon_offenders.is_empty(),
		"ADR106 derived aggregate helper leaked to Wendelon:\n{}",
		format_offenders(&wendelon_manifest_dir(), &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr097_cast_try_cast_remain_ephemeral_not_receipts() {
	let binder = source("src/binder.rs");
	let logical_type_ddl = source("tests/logical_type_ddl_test.rs");
	let sql_logical_cast = first_block(&binder, "fn bind_sql_logical_cast");
	let receipt = first_block(&binder, "fn stored_refinement_receipt");

	assert!(sql_logical_cast.contains("set_node_logical_type"));
	assert!(!sql_logical_cast.contains("refinement"));
	assert!(receipt.contains("ExecutionPlan::Scan"));
	assert!(logical_type_ddl.contains("cast_ascribed_projected_column_does_not_elide"));
}

#[test]
pub(crate) fn adr097_no_wendelon_096_contract_pbc_executor_storage_or_wire_change() {
	let wendelon_compute =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/integration/compute.rs"))
			.expect("read Wendelon compute schema");
	let definition = first_block(&wendelon_compute, "pub struct LogicalTypeDefinition");
	assert!(definition.contains("pub physical: LogicalTypePhysical"));
	assert!(definition.contains("pub refinement: Option<LogicalTypeRefinementPredicate>"));
	assert!(!wendelon_compute.contains("BoundExprProgram"));
	assert!(!wendelon_compute.contains("ScalarValue"));

	let sql_planner = source("src/sql_planner.rs");
	assert!(sql_planner.contains("CREATE LOGICAL TYPE CHECK"));
	assert!(sql_planner.contains("parse_create_logical_type_refinement_clause"));

	let binder = source("src/binder.rs");
	let pbc = source("src/pbc.rs");
	let executor = source("src/exec.rs");
	let storage = source("src/storage.rs");

	assert!(binder.contains("fn refinement_static_filter_class"));
	assert!(!pbc.contains("refinement_static_filter_class"));
	assert!(!pbc.contains("CarrierDomain"));
	assert!(!executor.contains("refinement_static_filter_class"));
	assert!(!storage.contains("refinement_static_filter_class"));
}

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative)).expect("read Toron source")
}

fn first_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing block for {marker}"))
		.1
}
