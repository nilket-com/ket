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

	assert!(impl_block.contains(".table_query_with_runtime_filter("));
	assert!(impl_block.contains("request.runtime_filter.clone()"));
	assert!(!impl_block.contains(".table_query("));
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
	assert!(exec.contains("or(&current, &mask)?"));
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
		"SqlExpr::Case { .. }",
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
pub(crate) fn adr075_partial_pushdown_splits_bound_top_level_and_only() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let splitter = brace_blocks_after_marker(&binder, "fn partition_bound_predicate")
		.into_iter()
		.next()
		.expect("partition_bound_predicate body")
		.1;
	let collector = brace_blocks_after_marker(&binder, "fn collect_top_level_bound_and")
		.into_iter()
		.next()
		.expect("collect_top_level_bound_and body")
		.1;

	assert!(splitter.contains("compile_pbc(&isolated).is_some()"));
	assert!(splitter.contains("combine_bound_conjuncts(predicate, &pushed)"));
	assert!(splitter.contains("combine_bound_conjuncts(predicate, &residual)"));
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

	assert!(binder.contains("plan_filter_with_predicate_pushdown(input, bound_predicate)"));
	assert!(binder.contains("plan_filter_with_predicate_pushdown(scan, bound_predicate)"));
	assert!(binder.contains("predicate_pushdown_plan(program, &scan)"));
	assert!(coordinator.contains("if let Some(predicate) = predicate"));
	assert!(predicate_test.contains("where_like_residual_splits_from_pbc_conjunct"));
	assert!(update_test.contains("binder_update_splits_pbc_conjunct_from_like_residual"));
	assert!(delete_test.contains("binder_delete_splits_pbc_conjunct_from_like_residual"));
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

	assert!(!plan_body.contains("Statement::AlterTable"));
	assert!(plan_body.contains("unsupported SQL statement; only SELECT"));
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
