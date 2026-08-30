use super::support::*;

const EXEC_ROOT_LINE_LIMIT: usize = 11_700;
const EXEC_ROOT_AFTER_AGGREGATE_LINE_LIMIT: usize = 10_000;
// ADR 541 adds the caller-deadline context field. ADR 556 adds the statement-clock
// seam and its execution-context plumbing; both are real context surfaces, not
// executor logic drifting back — bumped by their measured cost, not rounded up.
// ADR 560 briefly raised this to 7,580 to hold hand-written aggregate variant
// lists; ADR 561's registry removed them and relocated the window state init
// beside its constructors, so the ratchet returns to its pre-560 value.
// ADR 569 adds the Range synthesis arms (bound + legacy executor); ADR 571
// the scalar-subquery kernel's unbound reject arms.
// ADR 554 Branch A follow-on: +3 for the `time_input_processing` brackets at the
// `Filter` and `Project` arms. The helper and its test were carved to
// `exec/input_processing.rs` first; these three lines are the call sites
// themselves, which have to sit at the arms they measure. The bound still fails
// on expression-evaluator drift, which is what it is for.
// ADR 554 follow-on: +5 for the executed-node-kind route receipt at the top of
// `execute_bound_node`. It cannot be carved out — the receipt has to sit at the
// dispatch it names, above the attribution gate that would otherwise hide it.
// ADR 554 follow-on: +6 for re-exporting the join selection locality probe's gate
// and env name. The re-export has to sit in the executor root because the harness
// stamps the probe's state into its artifact header, and `join` is a private
// module. Still fails on expression-evaluator drift, which is what it is for.
// ADR 554 follow-on: +3 for the `SideTakeTiming::AccountedElsewhere` markers at
// this root's three `materialize_join_selection` test call sites. The marker is
// required at every call site by design — the exclusion of the spilled and
// streaming gathers must be stated, not defaulted.
// ADR 554 follow-on: +5 for the slice/`take` equivalence check's env name, gate and
// verified-column tally joining the same re-export, and for the wider comment that
// now has to explain two diagnostics rather than one. Same reason as above: the
// harness stamps both into its artifact header and `join` is private.
// ADR 580: +2 for the batch-local equivalence gate joining the same re-export, so
// the harness can stamp it into the seal and withhold timing authority from a
// diagnostic run.
// ADR 580 measurement: +1 for the route gate beside it. The route is off by
// default and exists to denominate one projection; the harness needs its name to
// stamp the seal and to withhold authority while it is on.
// ADR 554 follow-on: +2 for the re-export comment, which now has to say where each
// diagnostic's state is stamped — the locality probe in the artifact header, the
// equivalence check in the seal — and that both gates also withhold timing authority.
const EXEC_ROOT_AFTER_EXPR_LINE_LIMIT: usize = 7_597;
const EXEC_JOIN_ROOT_LINE_LIMIT: usize = 5_900;
// ADR 554 follow-on: 3_825 -> 3_890 for the index-build split — two nested child
// timers selected by join kind, and the tests that pin the selection, including the
// discriminating case where an all-paired outer join still charges the control.
// The carve boundary this bound guards is unchanged: nothing added here is streaming
// orchestration, which is what a drift back into the join root would look like.
// ADR 554 follow-on: 3_890 -> 4_290 for the lazy index builder, its backfill rail and
// the deferral counters. Still not streaming orchestration, so the guard's own subject
// is untouched — but the join root has taken ~800 lines across two cuts, and the output
// gather machinery (run tracking, lazy build, slice, equivalence) is now a coherent body
// with its own tests. The next cut in this area should carve it into a submodule beside
// `spill` and `streaming` rather than raise this bound a third time.
// ADR 554 follow-on: that carve happened, so the bound RATCHETS DOWN, 4_290 -> 3_420. The
// machinery and its tests now live in `join/output_materialization.rs`; the join root
// measured 3,383 lines after the move. A bound raised for work that has since left the
// file would silently license the next 900 lines of drift.
const EXEC_JOIN_ROOT_AFTER_STREAMING_LINE_LIMIT: usize = 3_420;

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
	// ADR 499 centralizes the streaming scan's three branches through one timed
	// helper; ownership remains singular while the syntactic caller count drops.
	assert_eq!(
		fold_sites, 17,
		"expected one definition plus 16 fold callers"
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
pub(crate) fn adr443_grouped_chain_profile_covers_the_top_owner_and_preserves_residency() {
	let manifest = manifest_dir();
	let join =
		std::fs::read_to_string(manifest.join("src/exec/join.rs")).expect("read exec join source");
	let streaming = std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs"))
		.expect("read exec streaming source");
	let profile_path = manifest.join("src/exec/join/grouped_chain_profile.rs");
	let profile = std::fs::read_to_string(&profile_path).expect("read ADR 443 profile source");
	let residency = std::fs::read_to_string(manifest.join("src/exec/join/residency_profile.rs"))
		.expect("read ADR 429 residency source");

	assert!(join.contains("mod grouped_chain_profile;"));
	assert!(profile_path.exists(), "missing ADR 443 profile owner");
	assert!(profile.contains("TORON_GROUPED_CHAIN_PROFILE_DIR"));
	assert!(profile.contains("return Ok(None);"));
	assert!(profile.contains("active_leaf: Option<GroupedChainProfileLeaf>"));
	assert!(profile.contains("checked_sub(leaf_total)"));
	assert!(profile.contains("spill-role reconciliation failed"));
	assert!(profile.contains("task_spill_at_root_start"));
	assert!(profile.contains("task_spill_at_base_end"));
	assert!(profile.contains("task_spill_at_root_end"));
	assert!(residency.contains("TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH"));
	assert!(residency.contains("const GROUPED_RESERVOIR_PROFILE_SCHEMA_VERSION: u32 = 1;"));

	let owners = brace_blocks_after_marker(
		&streaming,
		"pub(in crate::exec) async fn try_streaming_reoriented_inner_chain_grouped_aggregate",
	);
	let [(_, owner)] = owners.as_slice() else {
		panic!("expected one reoriented grouped-chain owner");
	};
	let fence = owner
		.find("let Some((base, mut stages)) = extract_reoriented_inner_chain")
		.expect("grouped-chain structural admission");
	let construct = owner
		.find("GroupedChainProfile::from_env(stages.len())?")
		.expect("profile construction in top-level owner");
	assert!(
		fence < construct,
		"profile construction must follow grouped-chain structural admission"
	);
	assert!(owner.contains("profile.persist()?"));
	for leaf in [
		"RuntimePrepBuild",
		"Finalization",
		"Cleanup",
		"BaseAccumulation",
		"ReservoirPartitionSpillWrite",
		"BaseConcat",
		"ReservoirReadProbe",
		"SuffixJoin",
		"GroupedFold",
	] {
		assert!(
			streaming.contains(&format!("GroupedChainProfileLeaf::{leaf}")),
			"missing ADR 443 leaf hook {leaf}"
		);
	}
	assert!(
		!profile.contains("q05") && !profile.contains("TPC-H") && !streaming.contains("TORON_Q05"),
		"ADR 443 profiler must stay shape-owned and query-neutral"
	);
}

#[test]
pub(crate) fn adr444_grouped_chain_refinement_is_nested_scoped_and_harness_evaluated() {
	let manifest = manifest_dir();
	let join =
		std::fs::read_to_string(manifest.join("src/exec/join.rs")).expect("read exec join source");
	let streaming = std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs"))
		.expect("read exec streaming source");
	let spill = std::fs::read_to_string(manifest.join("src/exec/join/spill.rs"))
		.expect("read exec spill source");
	let profile = std::fs::read_to_string(manifest.join("src/exec/join/grouped_chain_profile.rs"))
		.expect("read grouped-chain profile source");
	let residency = std::fs::read_to_string(manifest.join("src/exec/join/residency_profile.rs"))
		.expect("read ADR 429 residency source");
	let evaluator = std::fs::read_to_string(
		manifest
			.parent()
			.expect("workspace crates directory")
			.join("toron-tpch/src/adr444_profile_eval.rs"),
	)
	.expect("read harness evaluator");

	assert!(join.contains("GroupedChainSpillObserver"));
	assert!(join.contains("GroupedChainSubLeaf"));
	assert!(profile.contains("const PROFILE_SCHEMA_VERSION: u32 = 2;"));
	assert!(profile.contains("active_sub_leaf: Option<GroupedChainSubLeaf>"));
	assert!(profile.contains("sub-leaf micros"));
	assert!(profile.contains("ParentRefinementSnapshot"));
	assert!(profile.contains("gap_within_ceiling: gap_ratio <= 0.10"));
	assert!(profile.contains("page_inventory: BTreeMap<(u64, u64), u64>"));
	assert!(profile.contains("ciphertext page inventory mismatch"));
	assert!(profile.contains("physical write byte mismatch"));
	assert!(profile.contains("read_amplification"));
	assert!(streaming.contains("let grouped_chain_observer = grouped_chain_profile.map"));
	assert!(streaming.contains("grouped_chain_observer.as_ref()"));
	let spilled_builds =
		brace_blocks_after_marker(&spill, "pub(in crate::exec) struct SpilledInnerJoinBuild");
	let [(_, spilled_build)] = spilled_builds.as_slice() else {
		panic!("expected exactly one spilled inner-join build owner");
	};
	assert!(
		!spilled_build.contains("GroupedChainSpillObserver")
			&& !spilled_build.contains("GroupedChainProfile"),
		"ADR 444 observer must not become ordinary spilled-build state"
	);
	assert!(spill.contains("write_batch_chunks_unindexed_profiled"));
	assert!(spill.contains("read_batch_page_profiled"));
	assert!(spill.contains("rebuild_index_from_pages_profiled"));
	assert!(spill.contains("try_pin_decoded_pages_profiled"));
	for leaf in [
		"WriterSetup",
		"WriteScanDecode",
		"WritePartitionRouting",
		"WriteIpcEncode",
		"WriteEncryption",
		"WritePhysicalWrite",
		"WriteFinishFsync",
		"ReadProbePartitioning",
		"ReadPhysicalRead",
		"ReadDecrypt",
		"ReadIpcDecode",
		"ReadCacheLru",
		"ReadIndexRebuildCpu",
		"ReadLookup",
		"ReadMaterialization",
	] {
		assert!(
			profile.contains(&format!("GroupedChainSubLeaf::{leaf}"))
				|| spill.contains(&format!("GroupedChainSubLeaf::{leaf}"))
				|| streaming.contains(&format!("GroupedChainSubLeaf::{leaf}")),
			"missing ADR 444 sub-leaf {leaf}"
		);
	}
	assert!(evaluator.contains("pub fn evaluate_adr444_cohort"));
	assert!(evaluator.contains("pub fn evaluate_adr444_variance"));
	assert!(evaluator.contains("if median == 0.0"));
	assert!(evaluator.contains("unique_largest_index"));
	assert!(evaluator.contains("Adr444Gate::G4"));
	assert!(evaluator.contains("positive_total"));
	assert!(residency.contains("TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH"));
	assert!(residency.contains("const GROUPED_RESERVOIR_PROFILE_SCHEMA_VERSION: u32 = 1;"));
	assert!(!profile.contains("q05") && !profile.contains("TPC-H"));
	assert!(!spill.contains("TORON_Q05") && !streaming.contains("TORON_Q05"));
}

#[test]
pub(crate) fn adr439_partitioned_scan_build_opts_into_primitive_index_before_rebuild() {
	let manifest = manifest_dir();
	let streaming = std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs"))
		.expect("read exec streaming source");
	let spill = std::fs::read_to_string(manifest.join("src/exec/join/spill.rs"))
		.expect("read spill source");
	let owners = brace_blocks_after_marker(
		&streaming,
		"async fn try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks",
	);
	let [(_, owner)] = owners.as_slice() else {
		panic!("expected exactly one partitioned streamed scan-build owner");
	};
	assert!(
		owner.contains(".complete_coalesced_index_after_finish("),
		"partitioned scan-build must delegate index completion to the typed lifecycle owner"
	);
	let completions = brace_blocks_after_marker(
		&spill,
		"pub(in crate::exec) async fn complete_coalesced_index_after_finish",
	);
	let [(_, completion)] = completions.as_slice() else {
		panic!("expected exactly one coalesced index completion owner");
	};
	let control = completion
		.find("CoalescedPageIndexMode::RebuildAfterWrite")
		.expect("control lifecycle branch");
	let non_null_safe = completion[control..]
		.find("!null_safe_keys")
		.map(|offset| control + offset)
		.expect("non-null-safe primitive index fence in completion owner");
	let primitive_variants = completion[non_null_safe..]
		.find("matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64)")
		.map(|offset| non_null_safe + offset)
		.expect("primitive key-plan fence in completion owner");
	let opt_in = completion[primitive_variants..]
		.find("self.use_compact_resident_index();")
		.map(|offset| primitive_variants + offset)
		.expect("compact resident index opt-in in completion owner");
	let rebuild = completion[opt_in..]
		.find(".rebuild_index_from_pages_profiled(")
		.map(|offset| opt_in + offset)
		.expect("adjacent resident index rebuild in completion owner");

	assert!(control < non_null_safe);
	assert!(non_null_safe < primitive_variants);
	assert!(primitive_variants < opt_in);
	assert!(opt_in < rebuild, "primitive opt-in must precede rebuild");
	assert!(
		!completion[opt_in..rebuild].contains("rebuild_index_from_pages"),
		"no intervening rebuild may bypass the guarded primitive opt-in"
	);
	assert!(
		!owner.contains("q19") && !owner.contains("TORON_Q19"),
		"primitive index selection must remain shape-owned"
	);
}

#[test]
pub(crate) fn adr445_page_cache_bypass_is_fenced_to_resident_arm() {
	let manifest = manifest_dir();
	let streaming = std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs"))
		.expect("read exec streaming source");
	let owners = brace_blocks_after_marker(
		&streaming,
		"async fn probe_partitioned_streamed_inner_scan_build_reservoir",
	);
	let [(_, owner)] = owners.as_slice() else {
		panic!("expected exactly one grouped-reservoir probe owner");
	};
	let resident = owner
		.find("if resident_baseline_bytes.saturating_add(loaded_resident) <= loaded_cap")
		.expect("resident arm fence");
	let streamed = owner[resident..]
		.find("} else {")
		.map(|offset| resident + offset)
		.expect("streamed arm fence");
	let bypass = owner[resident..streamed]
		.find("spilled.disable_page_cache();")
		.map(|offset| resident + offset)
		.expect("resident cache bypass");
	let resident_drain = owner[bypass..]
		.find("read_all_batch_pages_profiled")
		.map(|offset| bypass + offset)
		.expect("resident drain read");

	assert!(resident < bypass);
	assert!(bypass < resident_drain);
	assert!(resident_drain < streamed);
	assert!(
		!owner[streamed..].contains("disable_page_cache"),
		"streamed arm must retain its page cache"
	);
	let loaded_workers = brace_blocks_after_marker(&streaming, "fn run_loaded_partition_work");
	let [(_, loaded_worker)] = loaded_workers.as_slice() else {
		panic!("expected exactly one resident partition worker");
	};
	let loaded_signature = loaded_worker
		.split_once('{')
		.map_or(loaded_worker.as_str(), |(signature, _)| signature);
	assert!(
		!loaded_signature.contains("SpilledInnerJoinBuild")
			&& !loaded_signature.contains("spilled"),
		"resident worker must not regain access to the spill build"
	);
	let spill = std::fs::read_to_string(manifest.join("src/exec/join/spill.rs"))
		.expect("read exec spill source");
	assert!(spill.contains("pub(in crate::exec) fn disable_page_cache"));
	assert!(spill.contains("if self.cache_cap == 0 || bytes_len > self.cache_cap"));
	assert!(spill.contains("self.touch_cache_entry(page_index);"));
}

#[test]
pub(crate) fn adr446_local_runtime_filter_is_fenced_to_terminal_reservoir() {
	let manifest = manifest_dir();
	let streaming = std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs"))
		.expect("read exec streaming source");
	let owners = brace_blocks_after_marker(
		&streaming,
		"async fn try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir",
	);
	let [(_, owner)] = owners.as_slice() else {
		panic!("expected exactly one local runtime-filter owner");
	};
	let fence = owner
		.find("local_runtime_filter_shape_is_admitted(")
		.expect("local filter must use the shape admission fence");
	let build = owner[fence..]
		.find("build_local_terminal_runtime_filter(")
		.map(|offset| fence + offset)
		.expect("local filter must be built in the terminal owner");
	let reservoir = owner[build..]
		.find("build_partitioned_streaming_scan_inner_build_reservoir(")
		.map(|offset| build + offset)
		.expect("terminal reservoir build must follow local filter construction");
	let assignment = owner[reservoir..]
		.find("local_runtime_filter,")
		.map(|offset| reservoir + offset)
		.expect("local filter must be threaded into the terminal reservoir");
	assert!(fence < build && build < reservoir && reservoir < assignment);
	assert!(owner.contains("runtime.probe_keys.first().copied()"));
	assert!(owner.contains("partition_build_keys.first().copied()"));
	assert!(streaming.contains("!null_safe_keys"));

	let runtime_filter = std::fs::read_to_string(manifest.join("src/exec/runtime_filter.rs"))
		.expect("read runtime filter source");
	assert!(runtime_filter.contains("build_runtime_filter_fragment_for_ordinal"));
	assert!(
		runtime_filter
			.contains("build_runtime_filter_fragment_for_ordinal(batches, ordinal, key_type)")
	);
	assert!(streaming.contains("req.runtime_filter.is_none()"));
	assert!(!streaming.contains("self.context.runtime_filter ="));
}

#[test]
pub(crate) fn adr447_materiality_floor_is_not_the_regression_threshold() {
	let main = std::fs::read_to_string(
		manifest_dir()
			.parent()
			.expect("crates directory")
			.join("toron-tpch/src/main.rs"),
	)
	.expect("read toron-tpch perf harness");
	assert_eq!(
		main.matches("const REGRESSION_THRESHOLD: f64").count(),
		1,
		"regression alarm constant must remain a single declaration"
	);
	assert_eq!(
		main.matches("pub const MATERIALITY_FLOOR: f64").count(),
		1,
		"materiality floor must remain a distinct declaration"
	);
	let estimator = brace_blocks_after_marker(&main, "pub fn paired_median_shift")
		.into_iter()
		.next()
		.expect("materiality estimator must exist")
		.1;
	assert!(
		!estimator.contains("REGRESSION_THRESHOLD"),
		"materiality estimator must not read the regression alarm threshold"
	);
	assert!(
		main.contains("regression_alarm_band")
			|| main.contains("REGRESSION_THRESHOLD.max(2.0 * spread_ratio.max(baseline_spread))"),
		"regression alarm must remain explicitly named and scoped"
	);
}

#[test]
pub(crate) fn adr448_runtime_filter_kill_switch_is_default_on_ablation_only() {
	let streaming = std::fs::read_to_string(manifest_dir().join("src/exec/join/streaming.rs"))
		.expect("read streaming source");
	assert!(streaming.contains("LOCAL_RUNTIME_FILTER_ABLATION_ENV"));
	assert!(streaming.contains("fn local_runtime_filter_enabled_from_env"));
	assert!(streaming.contains("local_runtime_filter_enabled()"));
	assert!(
		streaming.contains(
			"Some(value) if matches!(value.as_str(), \"1\" | \"true\" | \"yes\" | \"on\")"
		)
	);
	assert!(streaming.contains("&& local_runtime_filter_shape_is_admitted"));
	assert_eq!(
		streaming
			.matches("TORON_ABLATE_LOCAL_RUNTIME_FILTER")
			.count(),
		1,
		"the live ablation variable must be read only through its named constant"
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

/// ADR 554 follow-on: the receipt must not drift back under the handle guard.
///
/// `declare_stage_vocabulary` returns `None` when the node-attribution file
/// diagnostic is off, so a `commit_stage_vocabulary` call placed inside
/// `if let Some(handle) = stage_handle` never runs in an ordinary atlas run.
/// That is not hypothetical: it shipped, and an authoritative SF=8 run came back
/// with an empty receipt on all 1,323 distributed nodes. The handle is an
/// `Option` argument now precisely so the call sites cannot be guarded on it.
#[test]
pub(crate) fn adr554_vocabulary_commits_are_not_guarded_on_the_handle() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = std::fs::read_to_string(manifest_dir.join("src/exec/join/streaming.rs")).unwrap();
	let calls: Vec<usize> = source
		.match_indices("commit_stage_vocabulary(")
		.map(|(index, _)| index)
		.collect();
	assert!(
		!calls.is_empty(),
		"the vocabulary commit sites vanished; this guard is now vacuous"
	);
	let guarded: Vec<usize> = calls
		.iter()
		.filter(|index| {
			// Look back over the enclosing statement, not the whole function: a
			// handle guard that applies to this call sits immediately above it.
			let start = index.saturating_sub(240);
			let preamble = &source[start..**index];
			let Some(tail) = preamble.rsplit_once('\n').map(|(head, _)| head) else {
				return false;
			};
			// An `if let Some(..) = stage_handle` with no intervening close brace
			// still covers this call.
			match tail.rfind("= stage_handle") {
				None => false,
				Some(at) => !tail[at..].contains('}'),
			}
		})
		.copied()
		.collect();
	assert!(
		guarded.is_empty(),
		"commit_stage_vocabulary must be called unconditionally and passed the \
		 optional handle, never placed under an `if let Some(..) = stage_handle` \
		 guard — that guard is off whenever the diagnostic is off; {} site(s) are \
		 guarded",
		guarded.len()
	);
}
/// ADR 554 follow-on: every committed route reaches the task-local receipt.
///
/// A unit test can prove the helper works but not that all call sites use it.
/// This proves reach structurally: each `commit_stage_vocabulary` call must pass
/// the task telemetry slot, so a new streaming route cannot commit a vocabulary
/// that never lands in an atlas node.
#[test]
pub(crate) fn adr554_every_vocabulary_commit_reaches_the_task_receipt() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = std::fs::read_to_string(manifest_dir.join("src/exec/join/streaming.rs")).unwrap();
	let calls: Vec<usize> = source
		.match_indices("commit_stage_vocabulary(")
		.map(|(index, _)| index)
		.collect();
	assert!(
		!calls.is_empty(),
		"the vocabulary commit sites vanished; this guard is now vacuous"
	);
	let missing: Vec<usize> = calls
		.iter()
		.filter(|index| {
			// The slot may sit on the same line or the next, depending on rustfmt.
			let tail = &source[**index..source.len().min(**index + 200)];
			!tail.contains("&self.context.task_telemetry")
		})
		.copied()
		.collect();
	assert!(
		missing.is_empty(),
		"every commit_stage_vocabulary call must pass &self.context.task_telemetry \
		 so the committed route reaches the task-local receipt; {} call(s) do not",
		missing.len()
	);
}

/// Every task-level dispatch arm carries an entry-point receipt.
///
/// The dispatch chain in `execute_task_local_with_telemetry_and_stream_sink` is
/// the only place that decides which driver runs a fragment, and an arm added
/// without a receipt is invisible rather than wrong — the set would simply be
/// empty for that shape, which reads as "no data" and not "missing call". This
/// counts drivers against receipts so a new arm fails the build instead.
#[test]
pub(crate) fn adr554_every_task_dispatch_arm_records_an_entry_point() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = std::fs::read_to_string(manifest_dir.join("src/server/mod.rs")).unwrap();
	let body = dispatch_chain_body(&source);
	let drivers = body.matches("drive_streamable_plan(&spec.plan").count()
		+ body.matches("self.execute_live_shuffle_producer(").count()
		+ body.matches("self.execute_live_shuffle_consumer(").count()
		+ body.matches("executor.execute_bound(&spec.plan").count();
	let receipts = body
		.matches("record_executor_entry_point(&telemetry, \"")
		.count();
	assert!(
		drivers > 0,
		"the task dispatch arms vanished; this guard is now vacuous"
	);
	assert_eq!(
		receipts, drivers,
		"each of the {drivers} dispatch arm(s) must record an entry point; found \
		 {receipts} receipt call(s). An arm without one reports an empty set, \
		 which reads as an absent route rather than a missing call."
	);

	// Distinct labels, or two arms merge into one indistinguishable receipt.
	let mut labels: Vec<&str> = body
		.match_indices("record_executor_entry_point(&telemetry, \"")
		.map(|(index, matched)| {
			let rest = &body[index + matched.len()..];
			&rest[..rest
				.find('"')
				.expect("entry point label is a string literal")]
		})
		.collect();
	labels.sort_unstable();
	let distinct = labels.len();
	labels.dedup();
	assert_eq!(
		labels.len(),
		distinct,
		"entry point labels must be distinct; a repeated label makes two arms \
		 indistinguishable in the receipt"
	);
}

/// The executed-node-kind receipt stays outside the node-attribution gate.
///
/// `node_attribution::enter` returns an inert span when the file diagnostic is
/// off. Recording the node kind through it — or after it, inside a block it
/// guards — would make the receipt empty in exactly the ordinary atlas run it
/// exists for. That failure already happened once with the vocabulary receipt;
/// this pins the ordering so it cannot happen again silently.
#[test]
pub(crate) fn adr554_node_kind_receipt_precedes_the_attribution_gate() {
	let source = exec_root_source();
	let receipt = source
		.find("node_attribution::record_executed_node_kind(")
		.expect("the executed-node-kind receipt vanished; this guard is now vacuous");
	let gate = source
		.find("node_attribution::enter(")
		.expect("the node attribution span vanished; this guard is now vacuous");
	assert!(
		receipt < gate,
		"record_executed_node_kind must be called before node_attribution::enter, \
		 never under it: `enter` is inert when the file diagnostic is off, and the \
		 receipt has to be present in an ordinary atlas run"
	);
	assert_eq!(
		source
			.matches("node_attribution::record_executed_node_kind(")
			.count(),
		1,
		"exactly one receipt site: the recursive executor dispatch. A second site \
		 would double-count nothing but would blur where the fact comes from."
	);
}

/// The dispatch chain's body, from the task telemetry it writes into down to the
/// end of the arm chain. Scoping the guard to this span keeps unrelated
/// `execute_bound` calls elsewhere in the file from inflating either count.
fn dispatch_chain_body(source: &str) -> &str {
	let start = source
		.find("let telemetry = Arc::new(Mutex::new(task_telemetry_for_spec(spec)));")
		.expect("task telemetry construction moved; rescope this guard");
	let rest = &source[start..];
	// The chain is bracketed by two `record_worker_telemetry` calls: the shuffle
	// pull bookkeeping above it and the row/batch bookkeeping below. Anchoring on
	// those rather than on body text keeps the guard from silently widening to the
	// whole file — which would count unrelated `execute_bound` calls as arms and
	// make the assertion fail for the wrong reason.
	let marker = "record_worker_telemetry(&telemetry, |task| {";
	let first = rest.find(marker).expect("pre-chain bookkeeping moved") + marker.len();
	let end = first
		+ rest[first..]
			.find(marker)
			.expect("post-chain bookkeeping moved; rescope this guard");
	&rest[..end]
}

/// The two plan-label lists agree, arm for arm.
///
/// `server::operator_kind_label` and `ExecutionPlan::node_label` spell out the
/// same variant-to-name mapping in two places. That duplication is deliberate —
/// nine ADR guards assert the arm text lives in the worker layer, and delegating
/// would move it out from under all of them — but duplication that nothing checks
/// drifts. This is the check: same variants, same names, in both.
///
/// The join short-circuit in `operator_kind_label` is above its match and so is
/// not an arm; it is a property of the fragment, not of a node, and is out of
/// scope here on purpose.
#[test]
pub(crate) fn adr554_operator_kind_label_and_node_label_agree() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let server = std::fs::read_to_string(manifest_dir.join("src/server/mod.rs")).unwrap();
	let ir = std::fs::read_to_string(manifest_dir.join("src/ir/execution.rs")).unwrap();

	let worker = label_arms(&server, "fn operator_kind_label(", "ExecutionPlan::");
	let plan = label_arms(&ir, "pub fn node_label(", "Self::");

	assert!(
		worker.len() >= 20,
		"operator_kind_label arms did not parse ({} found); this guard is now \
		 vacuous",
		worker.len()
	);
	assert_eq!(
		worker, plan,
		"operator_kind_label and ExecutionPlan::node_label must map every variant \
		 to the same name. They are spelled out separately so the worker-layer ADR \
		 guards keep their target; drift between them means one label list has been \
		 updated for a new variant and the other has not."
	);
}

/// Extract `(variant, label)` pairs from the first `match` in the named function.
fn label_arms(source: &str, signature: &str, prefix: &str) -> Vec<(String, String)> {
	let start = source
		.find(signature)
		.unwrap_or_else(|| panic!("{signature} not found; rescope this guard"));
	let body = &source[start..];
	let end = body.find("\n}\n").map_or(body.len(), |at| at);
	let mut arms: Vec<(String, String)> = body[..end]
		.lines()
		.filter_map(|line| {
			let line = line.trim();
			let rest = line.strip_prefix(prefix)?;
			let (variant, tail) = rest.split_once(" { .. } => \"")?;
			let label = tail.strip_suffix("\",")?;
			Some((variant.to_owned(), label.to_owned()))
		})
		.collect();
	arms.sort();
	arms
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
	// ADR 516 raises the bound to 3,825 for the primitive probe-window observer and its tests;
	// suffix orchestration remains in streaming.rs, so the carve boundary itself is unchanged.
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

#[test]
pub(crate) fn adr514_compact_sizing_scope_is_pinned() {
	// ADR 514: the representation-aware sizing exists as a second opinion at
	// exactly one decision — the streamed scan-build partitioning choice — and
	// the shared binder/executor admission constant is untouched. The counts
	// below pin that scope (definitions plus the single production call plus
	// the named tests); a new consumer must retarget this guard explicitly.
	let manifest = manifest_dir();
	let streaming =
		std::fs::read_to_string(manifest.join("src/exec/join/streaming.rs")).expect("streaming");
	let spill = std::fs::read_to_string(manifest.join("src/exec/join/spill.rs")).expect("spill");
	let ir = std::fs::read_to_string(manifest.join("src/ir.rs")).expect("ir");

	assert_eq!(
		ir.matches("HASH_JOIN_BUILD_ROW_OVERHEAD_BYTES").count(),
		2,
		"the shared admission constant keeps its definition and single use in ir.rs"
	);
	assert_eq!(
		streaming
			.matches("HASH_JOIN_BUILD_ROW_OVERHEAD_BYTES")
			.count(),
		0,
		"the sizing fork must not consume the shared admission constant directly"
	);
	assert_eq!(
		spill
			.matches("compact_streamed_build_owner_bound_bytes")
			.count(),
		1,
		"the owner bound is defined once beside the accounting it mirrors"
	);
	assert_eq!(
		streaming
			.matches("compact_streamed_build_owner_bound_bytes")
			.count(),
		4,
		"one production consumer (effective_partition_sizing_bytes) plus the three named tests"
	);
	assert_eq!(
		streaming
			.matches("effective_partition_sizing_bytes")
			.count(),
		7,
		"one definition, one call in the partitioning choice, five test references"
	);
}

/// ADR 580: `matched_rows` has exactly one writer, and it attributes by route.
///
/// A direct assignment would add rows to the total without saying which route
/// produced them, and the route-scoped counters would silently stop summing to
/// it — the projection they exist to denominate would then be computed over rows
/// no route claimed. `add_matched_rows` is that single writer.
#[test]
pub(crate) fn adr580_matched_rows_has_one_attributing_writer() {
	let manifest = manifest_dir();
	let offenders = source_files(&manifest.join("src"))
		.into_iter()
		.filter(|path| !path.ends_with("distributed/task.rs"))
		.flat_map(|path| {
			std::fs::read_to_string(&path)
				.expect("read source file")
				.lines()
				.enumerate()
				.filter_map(|(index, line)| {
					line.contains("counters.matched_rows =")
						.then(|| (path.clone(), index + 1, line.to_string()))
				})
				.collect::<Vec<_>>()
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"matched_rows must be written only through `add_matched_rows`, which \
		 attributes the rows to a route:\n{}",
		format_offenders(&manifest, &offenders)
	);
}
