use super::support::*;

const EXEC_ROOT_LINE_LIMIT: usize = 11_700;
const EXEC_ROOT_AFTER_AGGREGATE_LINE_LIMIT: usize = 10_000;
const EXEC_ROOT_AFTER_EXPR_LINE_LIMIT: usize = 7_500;
const EXEC_JOIN_ROOT_LINE_LIMIT: usize = 5_900;
const EXEC_JOIN_ROOT_AFTER_STREAMING_LINE_LIMIT: usize = 3_800;

#[test]
pub(crate) fn adr350_exec_root_declares_join_module() {
	let root = exec_root_source();

	assert!(
		root.contains("mod join;"),
		"exec root must declare the join semantic module"
	);
}

#[test]
pub(crate) fn adr350_exec_join_module_exists() {
	let manifest = manifest_dir();
	let path = manifest.join("src/exec/join.rs");

	assert!(path.exists(), "missing exec/join.rs semantic module");
}

#[test]
pub(crate) fn adr353_exec_root_declares_aggregate_module() {
	let root = exec_root_source();

	assert!(
		root.contains("mod aggregate;"),
		"exec root must declare the aggregate executor module"
	);
}

#[test]
pub(crate) fn adr353_exec_aggregate_module_exists() {
	let manifest = manifest_dir();
	let path = manifest.join("src/exec/aggregate.rs");

	assert!(
		path.exists(),
		"missing exec/aggregate.rs aggregate executor module"
	);
}

#[test]
pub(crate) fn adr435_generic_bound_grouped_fold_has_one_table_owner() {
	let manifest = manifest_dir();
	let aggregate = std::fs::read_to_string(manifest.join("src/exec/aggregate.rs"))
		.expect("read exec aggregate source");
	let streaming = std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs"))
		.expect("read exec streaming source");
	let group_table_path = manifest.join("src/exec/aggregate/group_table.rs");

	assert!(
		aggregate.contains("mod group_table;"),
		"aggregate executor must own the ADR 435 group-table module"
	);
	assert!(
		group_table_path.exists(),
		"missing ADR 435 group-table owner"
	);
	for (path, source) in [
		("src/exec/aggregate.rs", aggregate.as_str()),
		("src/exec/join/streaming.rs", streaming.as_str()),
	] {
		assert!(
			!source.contains("BTreeMap<Vec<GroupValue>, Vec<AggState>>"),
			"{path} must not restore a raw generic grouped-fold BTreeMap"
		);
		assert!(
			!source.contains("std::collections::btree_map::Entry"),
			"{path} must route grouped-fold entry lookup through the ADR 435 owner"
		);
	}

	let fold_sites = aggregate
		.matches("fold_bound_grouped_aggregate_batch(")
		.count()
		+ streaming
			.matches("fold_bound_grouped_aggregate_batch(")
			.count();
	let finalize_sites = aggregate
		.matches("finalize_bound_grouped_aggregate(")
		.count()
		+ streaming
			.matches("finalize_bound_grouped_aggregate(")
			.count();
	assert_eq!(
		fold_sites, 19,
		"expected one definition plus 18 fold callers"
	);
	assert_eq!(
		finalize_sites, 11,
		"expected one definition plus 10 finalize callers"
	);
	assert!(
		aggregate.contains("groups: &mut BoundGroupedAggregateGroups")
			&& streaming.contains("groups: &mut BoundGroupedAggregateGroups"),
		"all generic fold consumers must use the shared group-table owner"
	);
}

#[test]
pub(crate) fn adr438_global_fold_join_filter_profile_is_shape_owned_and_off_by_default() {
	let manifest = manifest_dir();
	let join =
		std::fs::read_to_string(manifest.join("src/exec/join.rs")).expect("read exec join source");
	let aggregate = std::fs::read_to_string(manifest.join("src/exec/aggregate.rs"))
		.expect("read exec aggregate source");
	let streaming = std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs"))
		.expect("read exec streaming source");
	let profile_path = manifest.join("src/exec/join/global_fold_profile.rs");
	let profile = std::fs::read_to_string(&profile_path).expect("read ADR 438 profile source");

	assert!(join.contains("mod global_fold_profile;"));
	assert!(profile_path.exists(), "missing ADR 438 profile owner");
	assert!(profile.contains("TORON_GLOBAL_FOLD_JOIN_FILTER_PROFILE_DIR"));
	assert!(profile.contains("return Ok(None);"));
	assert!(profile.contains("active_leaf: Option<GlobalFoldProfileLeaf>"));
	assert!(profile.contains("checked_sub(leaf_total)"));
	assert!(profile.contains("M -> S reconciliation failed"));
	assert!(streaming.contains("partitioned_streamed_scan_build_scan_probe"));
	assert!(streaming.contains("GlobalFoldProfileLeaf::PredicateEvalFilter"));
	assert!(aggregate.contains("GlobalFoldProfileLeaf::DecimalExpr"));
	assert!(aggregate.contains("GlobalFoldProfileLeaf::NumericFold"));
	assert!(streaming.contains("profile.persist()?"));
	assert!(
		!profile.contains("q19") && !streaming.contains("TORON_Q19"),
		"ADR 438 profiler must stay query-neutral"
	);
}

#[test]
pub(crate) fn adr441_membership_chain_profile_is_post_fence_exclusive_and_query_neutral() {
	let manifest = manifest_dir();
	let join =
		std::fs::read_to_string(manifest.join("src/exec/join.rs")).expect("read exec join source");
	let streaming = std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs"))
		.expect("read exec streaming source");
	let profile_path = manifest.join("src/exec/join/membership_chain_profile.rs");
	let profile = std::fs::read_to_string(&profile_path).expect("read ADR 441 profile source");

	assert!(join.contains("mod membership_chain_profile;"));
	assert!(profile_path.exists(), "missing ADR 441 profile owner");
	assert!(profile.contains("TORON_MEMBERSHIP_CHAIN_PROFILE_DIR"));
	assert!(profile.contains("return Ok(None);"));
	assert!(profile.contains("active_leaf: Option<ActiveLeaf>"));
	assert!(profile.contains("checked_sub(leaf_total)"));
	assert!(profile.contains("spill-role reconciliation failed"));
	assert!(profile.contains("presence/root reconciliation failed"));
	assert!(streaming.contains("let membership_profile = MembershipChainProfile::from_env()?;"));
	let owner = brace_blocks_after_marker(
		&streaming,
		"pub(in crate::exec) async fn try_streaming_projected_left_semi_grouped_aggregate",
	);
	let [(_, owner)] = owner.as_slice() else {
		panic!("expected one projected LeftSemi grouped owner");
	};
	let fence = owner
		.find("let stage = stages.pop().expect")
		.expect("committed membership-chain stage");
	let construct = owner
		.find("MembershipChainProfile::from_env()?")
		.expect("profile construction in live owner");
	assert!(
		fence < construct,
		"profile construction must follow shape admission"
	);
	assert!(owner.contains("profile.persist()?"));
	assert!(streaming.contains("MembershipChainOperation::StorageScanDecode"));
	assert!(streaming.contains("MembershipChainOperation::PartitionSpillWrite"));
	assert!(streaming.contains("MembershipChainOperation::SpillReadDecode"));
	assert!(streaming.contains("MembershipChainOperation::LookupSelection"));
	assert!(streaming.contains("MembershipChainOperation::ArrowMaterialize"));
	assert!(
		!profile.contains("q18") && !profile.contains("TPC-H") && !streaming.contains("TORON_Q18"),
		"ADR 441 profiler must stay shape-owned and query-neutral"
	);
}

#[test]
pub(crate) fn adr439_partitioned_scan_build_opts_into_primitive_index_before_rebuild() {
	let manifest = manifest_dir();
	let streaming = std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs"))
		.expect("read exec streaming source");
	let owners = brace_blocks_after_marker(
		&streaming,
		"async fn try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks",
	);
	let [(_, owner)] = owners.as_slice() else {
		panic!("expected exactly one partitioned streamed scan-build owner");
	};

	let eligibility = owner
		.find("let use_primitive_resident_index =")
		.expect("primitive index eligibility in live owner");
	let non_null_safe = owner[eligibility..]
		.find("!null_safe_keys")
		.map(|offset| eligibility + offset)
		.expect("non-null-safe primitive index fence in live owner");
	let primitive_variants = owner[non_null_safe..]
		.find("matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64)")
		.map(|offset| non_null_safe + offset)
		.expect("primitive key-plan fence in live owner");
	let guarded_opt_in = owner[primitive_variants..]
		.find("if use_primitive_resident_index")
		.map(|offset| primitive_variants + offset)
		.expect("guarded primitive index opt-in in live owner");
	let opt_in = owner[guarded_opt_in..]
		.find("spilled.use_compact_resident_index();")
		.map(|offset| guarded_opt_in + offset)
		.expect("compact resident index opt-in in live owner");
	let rebuild = owner[opt_in..]
		.find(".rebuild_index_from_pages(")
		.map(|offset| opt_in + offset)
		.expect("adjacent resident index rebuild in live owner");

	assert!(eligibility < primitive_variants);
	assert!(eligibility < non_null_safe);
	assert!(non_null_safe < primitive_variants);
	assert!(primitive_variants < guarded_opt_in);
	assert!(guarded_opt_in < opt_in);
	assert!(opt_in < rebuild, "primitive opt-in must precede rebuild");
	assert!(
		!owner[opt_in..rebuild].contains("rebuild_index_from_pages"),
		"no intervening rebuild may bypass the guarded primitive opt-in"
	);
	assert!(
		!owner.contains("q19") && !owner.contains("TORON_Q19"),
		"primitive index selection must remain shape-owned"
	);
}

#[test]
pub(crate) fn adr354_exec_root_declares_expr_module() {
	let root = exec_root_source();

	assert!(
		root.contains("mod expr;"),
		"exec root must declare the expression evaluator module"
	);
}

#[test]
pub(crate) fn adr354_exec_expr_module_exists() {
	let manifest = manifest_dir();
	let path = manifest.join("src/exec/expr.rs");

	assert!(
		path.exists(),
		"missing exec/expr.rs expression evaluator module"
	);
}

#[test]
pub(crate) fn adr351_exec_join_root_declares_spill_module() {
	let join_root = exec_join_root_source();

	assert!(
		join_root.contains("mod spill;"),
		"exec/join root must declare the spill storage submodule"
	);
}

#[test]
pub(crate) fn adr351_exec_join_spill_module_exists() {
	let manifest = manifest_dir();
	let path = manifest.join("src/exec/join/spill.rs");

	assert!(path.exists(), "missing exec/join/spill.rs storage module");
}

#[test]
pub(crate) fn adr352_exec_join_root_declares_streaming_module() {
	let join_root = exec_join_root_source();

	assert!(
		join_root.contains("mod streaming;"),
		"exec/join root must declare the streaming orchestration submodule"
	);
}

#[test]
pub(crate) fn adr352_exec_join_streaming_module_exists() {
	let manifest = manifest_dir();
	let path = manifest.join("src/exec/join/streaming.rs");

	assert!(
		path.exists(),
		"missing exec/join/streaming.rs orchestration module"
	);
}

#[test]
pub(crate) fn adr350_exec_root_excludes_carved_join_clusters() {
	let root = exec_root_source();

	for carved_definition in [
		"async fn execute_hash_join(",
		"fn materialize_join_selection(",
		"struct SpilledJoinBuild",
		"struct SpilledInnerJoinBuild",
		"enum JoinOutputRow",
		"enum JoinKeyPlan",
		"fn join_spill_index_bytes(",
		"fn record_join_runtime_observation(",
	] {
		assert!(
			!root.contains(carved_definition),
			"exec root still defines carved join cluster marker {carved_definition}"
		);
	}
}

#[test]
pub(crate) fn adr351_exec_join_root_excludes_carved_spill_storage() {
	let join_root = exec_join_root_source();

	for carved_definition in [
		"struct SpilledJoinBuild",
		"struct SpilledInnerJoinBuild",
		"struct SpilledSemiAntiExistenceBuild",
		"struct JoinSpillPage",
		"fn write_join_spill_plaintext_page(",
		"fn encode_record_batch_ipc(",
		"fn join_spill_index_bytes(",
		"static NEXT_JOIN_SPILL_ID",
	] {
		assert!(
			!join_root.contains(carved_definition),
			"exec/join root still defines carved spill storage marker {carved_definition}"
		);
	}
}

#[test]
pub(crate) fn adr352_exec_join_root_excludes_carved_streaming_orchestration() {
	let join_root = exec_join_root_source();

	for carved_definition in [
		"struct ReorientedInnerJoinStage",
		"struct StreamingSemiAntiStage",
		"enum StreamingSemiAntiExistence",
		"struct StreamingLeftOuterRecombineRuntime",
		"fn extract_reoriented_inner_chain(",
		"fn decompose_streaming_existence_plan(",
		"fn extract_streaming_inner_prefix_semi_anti_chain(",
		"async fn try_streaming_join0_filter(",
		"async fn try_streaming_collapsing_global_aggregate(",
		"async fn prepare_streaming_semi_anti_runtime(",
		"fn probe_streaming_left_outer_recombine(",
	] {
		assert!(
			!join_root.contains(carved_definition),
			"exec/join root still defines carved streaming marker {carved_definition}"
		);
	}
}

#[test]
pub(crate) fn adr353_exec_root_excludes_carved_aggregate_executor_cluster() {
	let root = exec_root_source();

	for carved_definition in [
		"fn bound_global_aggregate(",
		"fn fold_global_aggregate_state(",
		"fn init_global_aggregate_state(",
		"fn finalize_global_aggregate_state(",
		"fn bound_grouped_aggregate(",
		"fn fold_bound_grouped_aggregate_batch(",
		"fn finalize_bound_grouped_aggregate(",
		"enum AggState",
		"enum GroupValue",
		"fn init_bound_states(",
		"fn group_value(",
		"fn group_column(",
	] {
		assert!(
			!root.contains(carved_definition),
			"exec root still defines carved aggregate executor marker {carved_definition}"
		);
	}
}

#[test]
pub(crate) fn adr354_exec_root_excludes_carved_expr_evaluator_cluster() {
	let root = exec_root_source();

	for carved_definition in [
		"pub fn evaluate_bound_program(",
		"pub fn evaluate_bound_program_without_storage(",
		"fn evaluate_bound_single_root(",
		"fn filter_batches_with_bound_predicate(",
		"fn filter_batch_with_bound_predicate(",
		"fn evaluate_bound_binary(",
		"fn evaluate_bound_unary(",
		"fn evaluate_unbound_arithmetic(",
		"pub(crate) fn evaluate_cast(",
		"fn evaluate_decimal_division(",
		"fn evaluate_arithmetic(",
		"enum ArithmeticOp",
		"const F64_I32_LOWER",
	] {
		assert!(
			!root.contains(carved_definition),
			"exec root still defines carved expression evaluator marker {carved_definition}"
		);
	}
}

#[test]
pub(crate) fn adr350_exec_root_is_bounded() {
	let lines = exec_root_source().lines().count();
	// ADR 350: first semantic executor decomposition ratchet. The post-join-carve root measured
	// 11,611 lines after fmt; 11,700 leaves import/module churn slack while failing if join logic
	// drifts back into exec.rs. Follow-up executor carves should lower this threshold.
	assert!(
		lines <= EXEC_ROOT_LINE_LIMIT,
		"exec root has {lines} lines; expected <= {EXEC_ROOT_LINE_LIMIT} after ADR 350 join carve"
	);
}

#[test]
pub(crate) fn adr353_exec_root_is_bounded_after_aggregate_carve() {
	let lines = exec_root_source().lines().count();
	// ADR 353: aggregate executor/state cluster carved from exec.rs into exec/aggregate.rs.
	// The post-carve root measured 9,772 lines after fmt; 10,000 leaves import/module churn slack
	// while failing if aggregate executor logic drifts back into the root.
	assert!(
		lines <= EXEC_ROOT_AFTER_AGGREGATE_LINE_LIMIT,
		"exec root has {lines} lines; expected <= {EXEC_ROOT_AFTER_AGGREGATE_LINE_LIMIT} after ADR 353 aggregate carve"
	);
}

#[test]
pub(crate) fn adr354_exec_root_is_bounded_after_expr_carve() {
	let lines = exec_root_source().lines().count();
	// ADR 354: expression evaluator cluster carved from exec.rs into exec/expr.rs.
	// The post-carve root measured below this threshold after fmt; 7,500 leaves module/import
	// churn slack while failing if expression evaluator logic drifts back into the root.
	assert!(
		lines <= EXEC_ROOT_AFTER_EXPR_LINE_LIMIT,
		"exec root has {lines} lines; expected <= {EXEC_ROOT_AFTER_EXPR_LINE_LIMIT} after ADR 354 expr carve"
	);
}

#[test]
pub(crate) fn adr351_exec_join_root_is_bounded() {
	let lines = exec_join_root_source().lines().count();
	// ADR 351: spill storage carved from exec/join.rs into exec/join/spill.rs. The post-carve
	// join root measured 5,801 lines after fmt; 5,900 leaves import/module churn slack while
	// failing if spill storage drifts back into the join root.
	assert!(
		lines <= EXEC_JOIN_ROOT_LINE_LIMIT,
		"exec/join root has {lines} lines; expected <= {EXEC_JOIN_ROOT_LINE_LIMIT} after ADR 351 spill carve"
	);
}

#[test]
pub(crate) fn adr352_exec_join_root_is_bounded_after_streaming_carve() {
	let lines = exec_join_root_source().lines().count();
	// ADR 352: streaming orchestration carved from exec/join.rs into exec/join/streaming.rs.
	// The post-carve join root measured 3,684 lines before fmt; 3,800 leaves import/module
	// churn slack while failing if streaming logic drifts back into the join root.
	assert!(
		lines <= EXEC_JOIN_ROOT_AFTER_STREAMING_LINE_LIMIT,
		"exec/join root has {lines} lines; expected <= {EXEC_JOIN_ROOT_AFTER_STREAMING_LINE_LIMIT} after ADR 352 streaming carve"
	);
}

#[test]
pub(crate) fn adr350_exec_modules_have_no_query_number_references() {
	let manifest = manifest_dir();
	let offenders = exec_source_files()
		.into_iter()
		.flat_map(|path| {
			std::fs::read_to_string(&path)
				.expect("read exec source file")
				.lines()
				.enumerate()
				.filter_map(|(idx, line)| {
					if contains_query_number_reference(line) {
						Some((path.clone(), idx + 1, line.to_string()))
					} else {
						None
					}
				})
				.collect::<Vec<_>>()
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"executor modules must stay shape-named, not TPC-H query-number branded:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr350_exec_public_surface_is_still_rooted() {
	let lib = std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source");
	let root = exec_root_source();
	let exec = exec_source();

	assert!(lib.contains("pub mod exec;"));
	assert!(root.contains("pub struct Executor"));
	assert!(root.contains("pub struct ExecutionContext"));
	assert!(exec.contains("pub(crate) fn materialize_scalar_rows("));
	assert!(root.contains("pub(crate) use self::expr::evaluate_cast;"));
	assert!(exec.contains("pub(crate) fn evaluate_cast("));
	let _ = std::mem::size_of::<toron::exec::Executor>();
	let _ = std::mem::size_of::<toron::exec::ExecutionContext>();
}

fn contains_query_number_reference(line: &str) -> bool {
	line.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
		.any(|token| {
			let bytes = token.as_bytes();
			matches!(bytes.len(), 2 | 3)
				&& matches!(bytes[0], b'q' | b'Q')
				&& bytes[1..].iter().all(u8::is_ascii_digit)
		})
}
