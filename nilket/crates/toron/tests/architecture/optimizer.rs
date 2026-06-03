use super::support::*;

fn binder_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source")
}

fn ir_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source")
}

fn execution_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source")
}

fn planner_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source")
}

fn rune_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read Rune engine source")
}

fn exec_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read executor source")
}

fn lib_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source")
}

fn catalog_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog source")
}

fn config_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config source")
}

fn coordinator_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source")
}

fn daemon_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source")
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

#[test]
pub(crate) fn adr132_dominance_comparator_in_single_helper() {
	let binder = binder_source();
	let bind_join = source_block(&binder, "async fn bind_join");
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");

	assert_eq!(
		binder.matches("fn right_build_materially_smaller(").count(),
		1
	);
	assert_eq!(
		binder
			.matches("const AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR")
			.count(),
		1
	);
	assert!(choose_auto.contains("right_build_materially_smaller(left.bytes, right.bytes)"));
	assert!(choose_auto.contains("let left_viable"));
	assert!(choose_auto.contains("let right_viable"));
	assert!(choose_auto.contains("match (left_viable, right_viable)"));
	assert!(!bind_join.contains("right_build_materially_smaller"));
	assert!(!bind_join.contains("AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR"));
	assert!(!bind_join.contains("rejected_side_bytes"));
}

#[test]
pub(crate) fn adr132_dominance_is_integer_ratio_over_hard_bytes() {
	let binder = binder_source();
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");
	let candidate = source_block(&binder, "fn build_byte_candidate");
	let helper = source_block(&binder, "fn right_build_materially_smaller");

	assert!(binder.contains("const AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR: u64 = 2"));
	assert!(helper.contains("u128::from(right_bytes.get())"));
	assert!(helper.contains("u128::from(AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR)"));
	assert!(helper.contains("<= u128::from(left_bytes.get())"));
	assert!(choose_auto.contains("left_build_candidate"));
	assert!(choose_auto.contains("right_build_candidate"));
	assert!(candidate.contains("byte_upper_bound"));
	assert!(candidate.contains("build_bytes < threshold"));
	for forbidden in [
		"f32",
		"f64",
		"0.8",
		"row_upper_bound",
		"confidence",
		"selectivity",
		"saturating_mul",
	] {
		assert!(
			!helper.contains(forbidden),
			"dominance comparator contains forbidden marker {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr132_left_biased_hysteresis() {
	let binder = binder_source();
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");

	assert!(choose_auto.contains("right_build_materially_smaller(left.bytes, right.bytes)"));
	assert!(choose_auto.contains("BroadcastBuildSide::Right"));
	assert!(choose_auto.contains("JoinPhysicalPlacement::SwappedRightBuild"));
	assert!(choose_auto.contains("(Some(left), Some(right)) => Some(("));
	assert!(choose_auto.contains("BroadcastBuildSide::Left"));
	assert!(choose_auto.contains("JoinPhysicalPlacement::Natural"));
	assert!(binder.contains("both_sides_tiny_equal_keeps_left_build"));
	assert!(binder.contains("both_sides_eligible_near_tie_keeps_left_build"));
}

#[test]
pub(crate) fn adr132_broadcast_inferred_receipt_carries_rejected_side_bytes() {
	let binder = binder_source();
	let reason = source_block(&binder, "enum JoinStrategyDecisionReason");
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");

	assert!(reason.contains("rejected_side_bytes: Option<ByteCount>"));
	assert!(choose_auto.contains("rejected_side_bytes"));
	assert!(choose_auto.contains("Some(left.bytes)"));
	assert!(choose_auto.contains("Some(right.bytes)"));
	assert!(choose_auto.contains("None"));
	assert!(binder.contains("sole_eligible_side_reports_no_rejected_bytes"));
}

#[test]
pub(crate) fn adr132_no_config_or_public_explain_surface() {
	let manifest_dir = manifest_dir();
	let binder = binder_source();
	let planner_config = source_block(&binder, "pub struct PlannerConfig");

	assert!(!planner_config.contains("dominance"));
	assert!(!planner_config.contains("rejected_side_bytes"));
	assert!(!planner_config.contains("right_build_materially_smaller"));

	let mut offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			path.file_name().and_then(|name| name.to_str()) != Some("binder.rs")
				&& (line.contains("rejected_side_bytes")
					|| line.contains("AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR")
					|| line.contains("right_build_materially_smaller"))
		})
		.collect::<Vec<_>>();
	offenders.extend(
		source_lines(&manifest_dir.join("src"))
			.into_iter()
			.filter(|(_, _, line)| {
				line.contains("EXPLAIN") && line.contains("rejected_side_bytes")
			}),
	);
	assert!(
		offenders.is_empty(),
		"ADR132 receipt/config leaked into public source surface:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr132_no_executor_fragmenter_scheduler_wire_or_wendelon_change() {
	let manifest_dir = manifest_dir();
	let markers = [
		"AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR",
		"right_build_materially_smaller",
		"rejected_side_bytes",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/distributed"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("src/sql_wire.rs"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);
	assert!(
		offenders.is_empty(),
		"ADR132 dominance comparator leaked into execution/distributed/wire/Wendelon substrate:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr133_optimizer_trace_is_binder_local() {
	let manifest_dir = manifest_dir();
	let binder = binder_source();
	let bind_join = source_block(&binder, "async fn bind_join");

	assert!(binder.contains("pub(crate) struct JoinTraceTarget"));
	assert!(binder.contains("pub(crate) struct DecisionReceipt"));
	assert!(binder.contains("pub(crate) struct OptimizerTrace"));
	assert!(bind_join.contains("trace.join_decisions.push(DecisionReceipt"));
	assert!(bind_join.contains("target: JoinTraceTarget { join_index }"));
	assert!(bind_join.contains("reason: decision.reason"));

	let markers = ["OptimizerTrace", "DecisionReceipt", "JoinTraceTarget"];
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			path.file_name().and_then(|name| name.to_str()) != Some("binder.rs")
				&& markers.iter().any(|marker| line.contains(marker))
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR133 trace markers leaked outside binder.rs:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr133_trace_types_are_crate_private_not_public() {
	let binder = binder_source();

	for marker in [
		"pub(crate) struct JoinTraceTarget",
		"pub(crate) struct DecisionReceipt",
		"pub(crate) struct OptimizerTrace",
		"pub(crate) enum JoinPhysicalPlacement",
		"pub(crate) enum BroadcastBuildSide",
		"pub(crate) enum JoinStrategyDecisionReason",
		"pub(crate) enum HashByDefaultReason",
	] {
		assert!(
			binder.contains(marker),
			"missing crate-private marker {marker}"
		);
	}

	for forbidden in [
		"pub struct JoinTraceTarget",
		"pub struct DecisionReceipt",
		"pub struct OptimizerTrace",
		"pub enum JoinPhysicalPlacement",
		"pub enum BroadcastBuildSide",
		"pub enum JoinStrategyDecisionReason",
		"pub enum HashByDefaultReason",
	] {
		assert!(
			!binder.contains(forbidden),
			"ADR133 trace type is public instead of pub(crate): {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr133_bind_is_wrapper_over_bind_with_trace() {
	let binder = binder_source();
	let bind_entry = source_block(&binder, "pub async fn bind");
	let bind_with_trace = source_block(&binder, "pub(crate) async fn bind_with_trace");

	assert!(bind_entry.contains("bind_with_trace(plan, catalog, config).await?"));
	assert!(!bind_entry.contains("Binder { catalog, config }"));
	assert!(!bind_entry.contains("bind_plan_with_trace"));
	assert!(bind_with_trace.contains("let mut trace = OptimizerTrace::default()"));
	assert!(bind_with_trace.contains("bind_plan_with_trace(plan, &mut trace)"));
	assert!(bind_with_trace.contains("push_down_scan_projection(plan.plan)?"));
	assert!(bind_with_trace.contains("Ok((plan, trace))"));
}

#[test]
pub(crate) fn adr133_no_trace_on_execution_plan_or_wire() {
	let manifest_dir = manifest_dir();
	let markers = [
		"OptimizerTrace",
		"DecisionReceipt",
		"JoinTraceTarget",
		"JoinStrategyDecisionReason",
		"HashByDefaultReason",
		"JoinPhysicalPlacement",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/ir/execution.rs"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("src/sql_wire.rs"),
		manifest_dir.join("src/distributed"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}
	assert!(
		offenders.is_empty(),
		"ADR133 trace/reason types leaked into execution plan or wire:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr133_no_public_explain_surface() {
	let planner = planner_source();
	let binder = binder_source();
	let execution = execution_source();

	assert!(!planner.contains("bind_with_trace"));
	assert!(!planner.contains("OptimizerTrace"));
	assert!(!planner.contains("DecisionReceipt"));
	assert!(binder.contains("fn optimizer_explain_plan"));
	assert!(binder.contains("SystemTableKind::OptimizerExplain"));
	assert!(!execution.contains("OptimizerTrace"));
	assert!(!execution.contains("DecisionReceipt"));
}

#[test]
pub(crate) fn adr135_probe_eligibility_and_behavior_gates_cover_broadcast_star() {
	let binder = binder_source();
	let lib = lib_source();
	let helper = source_block(&binder, "fn broadcast_probe_is_scan_linear");

	for marker in [
		"ExecutionPlan::Join {",
		"*kind == JoinKind::Inner",
		"*strategy == JoinStrategy::Broadcast",
		"matches!(terminal_exchange(left), Some((1, _)))",
		"broadcast_probe_is_scan_linear(right)",
		"| ExecutionPlan::Join { .. }",
	] {
		assert!(
			helper.contains(marker),
			"missing ADR135 helper marker {marker}"
		);
	}
	for gate in [
		"right_tiny_left_deep_join_infers_broadcast_star",
		"nested_hash_probe_still_stays_hash",
		"explain_broadcast_star_emits_one_receipt_per_link",
		"explain_probe_not_scan_linear_reports_hash_reason_detail",
	] {
		assert!(binder.contains(gate), "missing ADR135 binder gate {gate}");
	}
	assert!(lib.contains("three_way_broadcast_star_digest_matches_hash_over_tcp"));
}

#[test]
pub(crate) fn adr134_single_shared_explain_node() {
	let ir = ir_source();
	let execution = execution_source();
	let logical_plan = source_block(&ir, "pub enum LogicalPlan");
	let execution_plan = source_block(&execution, "pub enum ExecutionPlan");

	assert_eq!(logical_plan.matches("Explain {").count(), 1);
	assert!(logical_plan.contains("input: Box<LogicalPlan>"));
	assert!(!execution_plan.contains("Explain {"));
	assert!(execution.contains("OptimizerExplain"));
}

#[test]
pub(crate) fn adr134_sql_lowers_explain_to_logical_node() {
	let planner = planner_source();
	let lower_statement = source_block(&planner, "fn lower_statement");
	let lower_explain = source_block(&planner, "fn lower_explain_statement");

	assert!(lower_statement.contains("Statement::Explain"));
	assert!(lower_statement.contains("self.lower_explain_statement"));
	assert!(lower_explain.contains("Statement::Query(query)"));
	assert!(lower_explain.contains("LogicalPlan::Explain"));
	assert!(lower_explain.contains("input: Box::new(self.lower_query(query)?)"));
	assert!(!lower_explain.contains("bind_with_trace"));
	assert!(!lower_explain.contains("OptimizerTrace"));
}

#[test]
pub(crate) fn adr134_rune_dataframe_explain_wraps_logical_node() {
	let rune = rune_source();
	let explain_method = source_block(&rune, "pub fn explain");
	let module = source_block(&rune, "pub fn create_toron_module");

	assert!(explain_method.contains("plan: LogicalPlan::Explain"));
	assert!(explain_method.contains("input: Box::new(self.plan)"));
	assert!(module.contains("m.associated_function(\"explain\", RuneDataFrame::explain)?;"));
	assert!(!explain_method.contains("bind"));
	assert!(!explain_method.contains("execute"));
}

#[test]
pub(crate) fn adr134_explain_binds_via_bind_with_trace_not_execute() {
	let binder = binder_source();
	let bind_plan = source_block(&binder, "fn bind_plan_with_trace");
	let bind_explain = source_block(&binder, "async fn bind_explain");

	assert!(bind_plan.contains("LogicalPlan::Explain { input }"));
	assert!(bind_plan.contains("self.bind_explain(input).await?"));
	assert!(bind_explain.contains("bind_with_trace(input, self.catalog, self.config).await?"));
	assert!(bind_explain.contains("optimizer_explain_plan(trace)"));
	for forbidden in ["Executor", "execute_bound", ".execute(", "StorageScan"] {
		assert!(
			!bind_explain.contains(forbidden),
			"EXPLAIN bind path must not execute inner query: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr134_explain_renders_via_systemtable_existing_result_path() {
	let binder = binder_source();
	let execution = execution_source();
	let exec = exec_source();
	let optimizer_explain = source_block(&binder, "fn optimizer_explain_plan");

	assert!(execution.contains("OptimizerExplain"));
	assert!(optimizer_explain.contains("SystemTableKind::OptimizerExplain"));
	assert!(optimizer_explain.contains("system_table_plan("));
	assert!(exec.contains("ExecutionPlan::SystemTable { schema, rows, .. }"));
	assert!(!execution.contains("OptimizerTrace"));
	assert!(!execution.contains("DecisionReceipt"));
}

#[test]
pub(crate) fn adr134_sql_explain_accepts_only_default_query_form() {
	let planner = planner_source();
	let lower_explain = source_block(&planner, "fn lower_explain_statement");

	for required in [
		"describe_alias != DescribeAlias::Explain",
		"EXPLAIN ANALYZE is not supported",
		"EXPLAIN VERBOSE is not supported",
		"EXPLAIN QUERY PLAN is not supported",
		"EXPLAIN ESTIMATE is not supported",
		"EXPLAIN FORMAT is not supported",
		"EXPLAIN options are not supported",
		"EXPLAIN TABLE is not supported",
		"nested EXPLAIN is not supported",
		"EXPLAIN supports SELECT queries only",
	] {
		assert!(
			planner.contains(required) || lower_explain.contains(required),
			"EXPLAIN default-only guard missing {required}"
		);
	}
}

#[test]
pub(crate) fn adr134_schema_labels_are_stable_not_debug() {
	let binder = binder_source();
	let schema = source_block(&binder, "fn optimizer_explain_schema");
	let row = source_block(&binder, "fn optimizer_explain_row");

	for label in [
		"join_index",
		"strategy",
		"placement",
		"reason",
		"reason_detail",
		"build_side",
		"build_bytes",
		"threshold_bytes",
		"rejected_side_bytes",
		"hash_partitioned",
		"broadcast",
		"natural",
		"swapped_right_build",
		"pinned_by_author",
		"broadcast_inferred",
		"hash_by_default",
		"probe_not_scan_linear",
	] {
		assert!(
			binder.contains(label),
			"missing stable EXPLAIN label {label}"
		);
	}
	assert!(row.contains("join_strategy_label"));
	assert!(row.contains("join_physical_placement_label"));
	assert!(row.contains("join_strategy_decision_reason_label"));
	assert!(!row.contains("Debug"));
	assert!(!row.contains("{:?}"));
	assert!(schema.contains("Field::new(\"join_index\", DataType::Int64, false)"));
}

#[test]
pub(crate) fn adr134_join_trace_target_remains_index_only() {
	let binder = binder_source();
	let target = source_block(&binder, "struct JoinTraceTarget");

	assert!(target.contains("join_index: usize"));
	for forbidden in ["qualifier", "table", "alias", "label", "name"] {
		assert!(
			!target.contains(forbidden),
			"JoinTraceTarget must remain index-only: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr134_explain_is_join_decision_only_not_plan_tree_or_analyze() {
	let binder = binder_source();
	let planner = planner_source();
	let schema = source_block(&binder, "fn optimizer_explain_schema");
	let lower_explain = source_block(&planner, "fn lower_explain_statement");

	assert!(lower_explain.contains("EXPLAIN ANALYZE is not supported"));
	assert!(lower_explain.contains("EXPLAIN QUERY PLAN is not supported"));
	for forbidden in [
		"operator",
		"node",
		"plan_text",
		"subplan",
		"runtime",
		"actual",
	] {
		assert!(
			!schema.contains(forbidden),
			"optimizer EXPLAIN schema grew plan-tree/analyze field {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr134_reason_types_stay_crate_private_and_off_wire() {
	let manifest_dir = manifest_dir();
	let binder = binder_source();
	let markers = [
		"OptimizerTrace",
		"DecisionReceipt",
		"JoinTraceTarget",
		"JoinStrategyDecisionReason",
		"HashByDefaultReason",
		"JoinPhysicalPlacement",
	];

	for marker in [
		"pub(crate) struct JoinTraceTarget",
		"pub(crate) struct DecisionReceipt",
		"pub(crate) struct OptimizerTrace",
		"pub(crate) enum JoinStrategyDecisionReason",
	] {
		assert!(binder.contains(marker));
	}

	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("src/sql_wire.rs"),
	] {
		let source = std::fs::read_to_string(&path).expect("read wire source");
		offenders.extend(
			source
				.lines()
				.enumerate()
				.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
				.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
		);
	}
	assert!(
		offenders.is_empty(),
		"ADR134 trace/reason types leaked onto wire:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr134_ctx_sql_allows_readonly_explain_but_still_rejects_mutations() {
	let planner = planner_source();
	let rune = rune_source();
	let plan_relation = source_block(&planner, "pub fn plan_relation");
	let terminal = source_block(&planner, "fn relation_plan_is_terminal");
	let lower_explain = source_block(&planner, "fn lower_explain_statement");

	assert!(rune.contains("SqlPlanner::new().plan_relation(&query)"));
	assert!(plan_relation.contains("relation_plan_is_terminal(&plan)"));
	assert!(!terminal.contains("LogicalPlan::Explain"));
	assert!(lower_explain.contains("EXPLAIN supports SELECT queries only"));
	assert!(binder_source().contains("ctx_sql_explain_rejects_mutation_explain"));
}

#[test]
pub(crate) fn adr133_no_executor_fragmenter_scheduler_or_wendelon_change() {
	let manifest_dir = manifest_dir();
	let markers = [
		"OptimizerTrace",
		"DecisionReceipt",
		"JoinTraceTarget",
		"bind_with_trace",
		"JoinStrategyDecisionReason",
		"HashByDefaultReason",
		"JoinPhysicalPlacement",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/distributed"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("src/sql_wire.rs"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);
	assert!(
		offenders.is_empty(),
		"ADR133 trace leaked into executor/distributed/wire/Wendelon substrate:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr136_table_schema_descriptor_not_modified() {
	let wendelon_table =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
			.expect("read Wendelon table schema source");
	let catalog = catalog_source();

	assert!(!wendelon_table.contains("optimizer_facts"));
	assert!(!wendelon_table.contains("materialized_byte_upper_bound"));
	assert!(catalog.contains("pub struct DescriptorOptimizerFacts"));
}

#[test]
pub(crate) fn adr136_facts_are_descriptor_adjacent_sidecar() {
	let catalog = catalog_source();
	let config = config_source();
	let daemon = daemon_source();
	let sidecar = source_block(&catalog, "struct DescriptorOptimizerFacts");

	for required in [
		"schema_generation: u64",
		"physical_descriptor_fingerprint: PhysicalDescriptorFingerprint",
		"manifest_epoch: u64",
		"materialized_byte_upper_bound: Option<ByteCount>",
		"row_upper_bound: Option<RowCount>",
	] {
		assert!(
			sidecar.contains(required),
			"missing sidecar field {required}"
		);
	}
	assert!(config.contains("optimizer_facts: Option<DescriptorOptimizerFacts>"));
	assert!(config.contains("load_descriptor_bootstrap_with_optimizer_facts"));
	assert!(daemon.contains("load_descriptor_bootstrap_with_optimizer_facts"));
	assert!(daemon.contains("from_tenant_descriptors_with_facts"));
}

#[test]
pub(crate) fn adr136_table_binding_constructible_with_optimizer_facts() {
	let catalog = catalog_source();
	let binding_impl = source_block(&catalog, "impl TableBinding");

	assert!(binding_impl.contains("from_descriptor_with_optimizer_facts"));
	assert!(binding_impl.contains("from_descriptor_with_logical_types_and_optimizer_facts"));
	assert!(binding_impl.contains(".for_descriptor(descriptor)"));
	assert!(catalog.contains("descriptor_sidecar_facts_populate_table_optimizer_facts"));
	assert!(catalog.contains("legacy_descriptor_paths_still_default_unknown"));
	assert!(catalog.contains("mismatched_facts_default_to_unknown"));
	assert!(catalog.contains("known_empty_sidecar_normalizes_to_zero_bytes"));
}

#[test]
pub(crate) fn adr136_descriptor_refresh_updates_optimizer_facts() {
	let catalog = catalog_source();

	assert!(catalog.contains("from_tenant_descriptors_with_facts"));
	assert!(catalog.contains("register_descriptor_for_tenant_with_facts"));
	assert!(catalog.contains("sync_descriptor_for_tenant_with_facts"));
	assert!(catalog.contains("sync_latest_descriptor_for_tenant_with_facts"));
	assert!(catalog.contains("existing.optimizer_facts != candidate.optimizer_facts"));
	assert!(catalog.contains("facts_refresh_updates_binding_without_descriptor_identity_change"));
}

#[test]
pub(crate) fn adr136_planner_default_enables_auto_broadcast() {
	let binder = binder_source();
	let planner_config = source_block(&binder, "pub struct PlannerConfig");
	let default_impl = source_block(&binder, "impl Default for PlannerConfig");

	assert!(planner_config.contains("auto_broadcast_max_build_bytes: Option<ByteCount>"));
	assert!(default_impl.contains("auto_broadcast_max_build_bytes"));
	assert!(default_impl.contains("Some(ByteCount::new(1024 * 1024))"));
	assert!(binder.contains("star_query_infers_broadcast_without_seed_optimizer_facts"));
	assert!(
		binder.contains("rune_ctx_sql_star_query_infers_broadcast_without_seed_optimizer_facts")
	);
}

#[test]
pub(crate) fn adr136_missing_facts_still_hash() {
	let binder = binder_source();
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");

	assert!(choose_auto.contains("HashByDefaultReason::MissingBuildByteUpperBound"));
	assert!(binder.contains("planner_default_on_missing_facts_still_hashes"));
}

#[test]
pub(crate) fn adr136_byte_upper_bound_is_materialized() {
	let catalog = catalog_source();
	let config = config_source();
	let sidecar = source_block(&catalog, "struct DescriptorOptimizerFacts");

	assert!(sidecar.contains("materialized_byte_upper_bound"));
	assert!(config.contains("optimizer_facts: Option<DescriptorOptimizerFacts>"));
	assert!(!catalog.contains("estimated_size_bytes"));
	assert!(!catalog.contains("LsmRunSummary"));
	assert!(!config.contains("estimated_size_bytes"));
	assert!(!config.contains("LsmRunSummary"));
}

#[test]
pub(crate) fn adr136_no_wendelon_producer_in_toron() {
	let manifest_dir = manifest_dir();
	let markers = ["DescriptorOptimizerFacts", "materialized_byte_upper_bound"];
	let offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker)))
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR136 Toron-side facts leaked into Wendelon producer/schema source:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr137_filter_preserves_single_base_facts() {
	let binder = binder_source();
	let facts_impl = source_block(&binder, "impl PlanOptimizerFacts");
	let filter_arm = binder
		.split("LogicalPlan::Filter { input, predicate } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Project").next())
		.expect("Filter bind arm");

	assert!(facts_impl.contains("fn preserve_volume_non_increasing(self) -> Self"));
	assert!(filter_arm.contains("input_facts.preserve_volume_non_increasing()"));
	assert!(filter_arm.contains("filter_includes_primary_key_row_cap"));
	assert!(filter_arm.contains(".with_pk_row_cap()"));
	assert!(!filter_arm.contains("PlanOptimizerFacts::composite_with_row"));
	assert!(binder.contains("filtered_tiny_dim_infers_broadcast"));
	assert!(binder.contains("filtered_dim_without_facts_still_hashes"));
}

#[test]
pub(crate) fn adr137_project_preserves_byte_bound_only_for_column_subset() {
	let binder = binder_source();
	let facts_impl = source_block(&binder, "impl PlanOptimizerFacts");
	let project_helper = source_block(&binder, "fn project_roots_are_distinct_columns");
	let project_arm = binder
		.split("LogicalPlan::Project {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Aggregate").next())
		.expect("Project bind arm");

	assert!(facts_impl.contains("fn preserve_through_project"));
	assert!(facts_impl.contains("project_roots_are_distinct_columns(exprs)"));
	assert!(facts_impl.contains("byte_upper_bound: None"));
	assert!(project_helper.contains("BoundExprNode::Column(column)"));
	assert!(project_helper.contains("seen.insert(column.ordinal)"));
	assert!(project_arm.contains(".preserve_through_project(&exprs, &output_schema)"));
	assert!(!project_arm.contains("PlanOptimizerFacts::composite_with_row"));
	assert!(binder.contains("narrowing_project_tiny_dim_infers_broadcast"));
	assert!(binder.contains("expression_project_drops_byte_bound_and_hashes"));
	assert!(binder.contains("duplicate_project_drops_byte_bound"));
}

#[test]
pub(crate) fn adr137_pk_row_cap_does_not_shrink_byte_bound() {
	let binder = binder_source();
	let with_pk_cap = source_block(&binder, "fn with_pk_row_cap");
	let pk_recognizer = source_block(&binder, "fn predicate_includes_primary_key_equality");
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");

	assert!(with_pk_cap.contains("RowCount::new(1)"));
	assert!(with_pk_cap.contains("row_upper_bound"));
	assert!(!with_pk_cap.contains("byte_upper_bound"));
	assert!(pk_recognizer.contains("primary_key_columns.len() != 1"));
	assert!(!choose_auto.contains("row_upper_bound"));
	assert!(binder.contains("pk_filtered_large_table_caps_rows_but_still_hashes"));
	assert!(binder.contains("composite_pk_single_column_filter_does_not_cap_rows"));
}

#[test]
pub(crate) fn adr137_row_local_facts_go_through_shared_helper() {
	let binder = binder_source();
	let facts_impl = source_block(&binder, "impl PlanOptimizerFacts");
	let filter_arm = binder
		.split("LogicalPlan::Filter { input, predicate } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Project").next())
		.expect("Filter bind arm");
	let project_arm = binder
		.split("LogicalPlan::Project {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Aggregate").next())
		.expect("Project bind arm");
	let sort_arm = binder
		.split("LogicalPlan::Sort {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Take").next())
		.expect("Sort bind arm");

	assert_eq!(
		facts_impl
			.matches("fn preserve_volume_non_increasing")
			.count(),
		1
	);
	assert_eq!(facts_impl.matches("fn preserve_through_project").count(), 1);
	assert_eq!(facts_impl.matches("fn with_pk_row_cap").count(), 1);
	assert!(filter_arm.contains("preserve_volume_non_increasing"));
	assert!(project_arm.contains("preserve_through_project"));
	for arm in [filter_arm, project_arm] {
		assert!(!arm.contains("PlanOptimizerFacts {"));
		assert!(!arm.contains("PlanOptimizerFacts::composite_with_row"));
		assert!(!arm.contains("PlanOptimizerFacts::composite_with_bounds"));
	}
	assert!(sort_arm.contains("PlanOptimizerFacts::composite_with_row"));
}

#[test]
pub(crate) fn adr137_no_substrate_or_wire_change() {
	let manifest_dir = manifest_dir();
	let markers = [
		"preserve_volume_non_increasing",
		"preserve_through_project",
		"with_pk_row_cap",
		"project_roots_are_distinct_columns",
		"filter_includes_primary_key_row_cap",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/distributed"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("src/sql_wire.rs"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);
	assert!(
		offenders.is_empty(),
		"ADR137 row-local fact helpers leaked into execution/distributed/wire/Wendelon substrate:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr138_byte_upper_bound_not_written_by_toron_derivation() {
	let binder = binder_source();
	let facts = source_block(&binder, "struct PlanOptimizerFacts");
	let derive = source_block(&binder, "fn derive_estimated_byte_size");
	let estimator = source_block(&binder, "fn estimate_materialized_byte_size");

	assert!(facts.contains("byte_upper_bound: Option<ByteCount>"));
	assert!(facts.contains("estimated_byte_size: Option<ByteCount>"));
	assert!(derive.contains("estimated_byte_size"));
	assert!(derive.contains("estimate_materialized_byte_size"));
	assert!(!derive.contains("byte_upper_bound"));
	assert!(!estimator.contains("byte_upper_bound"));
	assert!(binder.contains("hard_byte_bound_still_wins_over_estimate"));
}

#[test]
pub(crate) fn adr138_estimate_and_derivation_live_in_one_helper() {
	let binder = binder_source();
	let scan_arm = binder
		.split("LogicalPlan::Scan { table, projection } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::ScanNamed").next())
		.expect("Scan bind arm");
	let scan_named_arm = binder
		.split("LogicalPlan::ScanNamed { table, projection } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Alias").next())
		.expect("ScanNamed bind arm");
	let filter_arm = binder
		.split("LogicalPlan::Filter { input, predicate } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Project").next())
		.expect("Filter bind arm");
	let project_helper = source_block(&binder, "fn preserve_through_project");

	assert_eq!(binder.matches("fn derive_estimated_byte_size").count(), 1);
	assert_eq!(
		binder.matches("fn estimate_materialized_byte_size").count(),
		1
	);
	assert_eq!(binder.matches("fn per_row_estimate_bytes").count(), 1);
	assert!(binder.contains("DEFAULT_UTF8_BINARY_PAYLOAD_BYTES"));
	assert!(binder.contains("DEFAULT_LARGE_UTF8_BINARY_PAYLOAD_BYTES"));
	assert!(binder.contains("RECORD_BATCH_IPC_FRAMING_OVERHEAD_BYTES"));
	assert!(scan_arm.contains(".derive_estimated_byte_size(plan.schema().as_ref())"));
	assert!(scan_named_arm.contains(".derive_estimated_byte_size(plan.schema().as_ref())"));
	assert!(filter_arm.contains(".derive_estimated_byte_size(plan.schema().as_ref())"));
	assert!(project_helper.contains("derive_estimated_byte_size(output_schema)"));
	assert!(project_helper.contains("estimated_byte_size: None"));
	assert!(binder.contains("string_bearing_tiny_dim_infers_broadcast_by_estimate"));
	assert!(binder.contains("pk_filtered_string_dim_broadcasts_by_estimate"));
}

#[test]
pub(crate) fn adr138_estimate_over_counts_encoded_payload_unit() {
	let binder = binder_source();
	let estimator = source_block(&binder, "fn estimate_materialized_byte_size");
	let per_row = source_block(&binder, "fn per_row_estimate_bytes");

	assert!(binder.contains("estimated_size_over_counts_encoded_payload"));
	assert!(binder.contains("encode_record_batch"));
	assert!(binder.contains("representative_estimate_batches"));
	assert!(estimator.contains("pad_to_arrow_buffer_alignment"));
	assert!(estimator.contains("div_ceil_u64(rows, 8)"));
	assert!(estimator.contains("saturating_add"));
	assert!(per_row.contains("DataType::Utf8 | DataType::Binary"));
	assert!(per_row.contains("DataType::LargeUtf8 | DataType::LargeBinary"));
	assert!(per_row.contains("DataType::Dictionary(_, _)"));
	assert!(per_row.contains("DataType::RunEndEncoded(_, _)"));
	assert!(per_row.contains("=> None"));
}

#[test]
pub(crate) fn adr138_chooser_prefers_hard_then_estimate_and_labels_estimated() {
	let binder = binder_source();
	let reason = source_block(&binder, "enum JoinStrategyDecisionReason");
	let candidate = source_block(&binder, "fn build_byte_candidate");
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");
	let explain_columns = source_block(&binder, "fn optimizer_explain_build_columns");
	let label = source_block(&binder, "fn join_strategy_decision_reason_label");

	let hard_index = candidate
		.find("if let Some(build_bytes) = facts.byte_upper_bound")
		.expect("hard byte path");
	let estimate_index = candidate
		.find("estimated_byte_size")
		.expect("estimated byte path");
	assert!(
		hard_index < estimate_index,
		"hard byte candidate must be considered before estimate"
	);
	assert!(candidate.contains("return (build_bytes < threshold).then_some"));
	assert!(choose_auto.contains("BuildByteSource::Hard"));
	assert!(choose_auto.contains("BuildByteSource::Estimated"));
	assert!(reason.contains("BroadcastEstimated"));
	assert!(reason.contains("estimated_byte_size: ByteCount"));
	assert!(explain_columns.contains("JoinStrategyDecisionReason::BroadcastEstimated"));
	assert!(label.contains("\"broadcast_estimated\""));
	assert!(binder.contains("estimated_broadcast_receipt_is_labeled_estimated"));
	assert!(binder.contains("estimate_above_effective_threshold_hashes"));
}

#[test]
pub(crate) fn adr138_take_lineage_unchanged() {
	let binder = binder_source();
	let take_helper = source_block(&binder, "fn take");
	let take_arm = binder
		.split("LogicalPlan::Take { input, limit } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Join").next())
		.expect("Take bind arm");

	assert!(take_helper.contains("Self::composite_with_row(row_upper_bound)"));
	assert!(!take_helper.contains("derive_estimated_byte_size"));
	assert!(take_arm.contains("input.facts.take(*limit)"));
	assert!(!take_arm.contains("derive_estimated_byte_size"));
	assert!(binder.contains("take_limited_dim_still_hashes"));
}

#[test]
pub(crate) fn adr138_default_threshold_unchanged() {
	let binder = binder_source();
	let default_impl = source_block(&binder, "impl Default for PlannerConfig");
	let cap_ceiling = source_block(&binder, "fn default_auto_broadcast_cap_ceiling");

	assert!(default_impl.contains("Some(ByteCount::new(1024 * 1024))"));
	assert!(cap_ceiling.contains("broadcast_build_bytes_per_task.get() / 4"));
	assert!(!default_impl.contains("estimated_byte_size"));
	assert!(!default_impl.contains("RECORD_BATCH_IPC_FRAMING_OVERHEAD_BYTES"));
}

#[test]
pub(crate) fn adr138_no_substrate_or_wire_change() {
	let manifest_dir = manifest_dir();
	let markers = [
		"estimated_byte_size",
		"estimate_materialized_byte_size",
		"per_row_estimate_bytes",
		"BroadcastEstimated",
		"broadcast_estimated",
		"DEFAULT_UTF8_BINARY_PAYLOAD_BYTES",
		"RECORD_BATCH_IPC_FRAMING_OVERHEAD_BYTES",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/distributed"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("src/sql_wire.rs"),
		manifest_dir.join("src/catalog.rs"),
		manifest_dir.join("src/config.rs"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);
	assert!(
		offenders.is_empty(),
		"ADR138 estimate markers leaked into execution/distributed/wire/catalog/config/Wendelon substrate:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);

	let lib = lib_source();
	assert!(lib.contains("distributed_broadcast_join_over_cap_fails_terminally_over_tcp"));
	assert!(lib.contains("rune_authored_broadcast_join_respects_cap_terminally"));
	assert!(lib.contains("TaskFailureKind::BudgetExhausted"));
}

#[test]
pub(crate) fn adr139_join_strategy_intent_carries_pin_source() {
	let ir = ir_source();
	let planner = planner_source();
	let rune = rune_source();
	let lib = lib_source();
	let intent = source_block(&ir, "pub enum JoinStrategyIntent");
	let source = source_block(&ir, "pub enum JoinStrategyPinSource");
	let build_join_plan = source_block(&planner, "pub(crate) fn build_join_plan");

	assert!(intent.contains("Pinned {"));
	assert!(intent.contains("strategy: JoinStrategy"));
	assert!(intent.contains("source: JoinStrategyPinSource"));
	assert!(source.contains("Direct"));
	assert!(source.contains("SqlHint"));
	assert!(source.contains("Rune"));
	assert!(build_join_plan.contains("source: JoinStrategyPinSource"));
	assert!(planner.contains("source: JoinStrategyPinSource::SqlHint"));
	assert!(rune.contains("JoinStrategyPinSource::Rune"));
	assert!(lib.contains("sql_and_rune_broadcast_pins_have_distinct_sources"));
}

#[test]
pub(crate) fn adr139_sql_hint_pin_is_replayable() {
	let planner = planner_source();
	let binder = binder_source();
	let lib = lib_source();

	assert!(planner.contains("source: JoinStrategyPinSource::SqlHint"));
	assert!(binder.contains("source: JoinStrategyPinSource::SqlHint"));
	assert!(binder.contains("BroadcastReplayEligibility::Replayable"));
	assert!(lib.contains("sql_broadcast_hint_over_cap_replays_as_hash"));
}

#[test]
pub(crate) fn adr139_rune_broadcast_pin_is_terminal() {
	let rune = rune_source();
	let binder = binder_source();
	let lib = lib_source();

	assert!(rune.contains("JoinStrategyPinSource::Rune"));
	assert!(binder.contains("BroadcastReplayEligibility::Terminal"));
	assert!(lib.contains("rune_authored_broadcast_join_respects_cap_terminally"));
}

#[test]
pub(crate) fn adr139_direct_broadcast_pin_is_terminal() {
	let binder = binder_source();
	let lib = lib_source();

	assert!(binder.contains("BroadcastReplayEligibility::Terminal"));
	assert!(lib.contains("direct_pinned_broadcast_over_cap_stays_terminal"));
	assert!(lib.contains("source: JoinStrategyPinSource::Direct"));
}

#[test]
pub(crate) fn adr139_broadcast_replication_not_marked_retryable() {
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget source");
	let retryable = source_block(&budget, "pub const fn retryable");

	assert!(budget.contains("BroadcastReplication"));
	assert!(retryable.contains("Self::Memory | Self::ShuffleBytes"));
	assert!(!retryable.contains("BroadcastReplication"));
}

#[test]
pub(crate) fn adr139_replay_trigger_classifies_scheduler_error_before_query_error_mapping() {
	let coordinator = coordinator_source();
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");
	let non_streaming = coordinator
		.split("execute_bound_distributed_attempt(")
		.nth(1)
		.and_then(|rest| rest.split("async fn admit_query").next())
		.expect("non-streaming distributed read loop");
	let streaming = coordinator
		.split("async fn run_streaming_read_driver")
		.nth(1)
		.and_then(|rest| rest.split("fn spawn_remote_cancel_fanout_task").next())
		.expect("streaming read driver");
	let streaming_trigger = streaming
		.find("broadcast_cap_replay_trigger(&error, &stages")
		.expect("streaming trigger before mapping");
	let streaming_map = streaming
		.find("let error = scheduler_error(error)")
		.expect("streaming scheduler_error mapping");

	assert!(trigger.contains("SchedulerError::TaskFailed"));
	assert!(trigger.contains("TaskFailureKind::BudgetExhausted"));
	assert!(trigger.contains("BudgetDimension::BroadcastReplication"));
	assert!(trigger.contains("task_key_replayable_broadcast_join_ids(stages, *key)"));
	assert!(non_streaming.contains("DistributedAttemptError::Scheduler { error, stages }"));
	assert!(non_streaming.contains("broadcast_cap_replay_trigger(&error, &stages"));
	assert!(streaming_trigger < streaming_map);
}

#[test]
pub(crate) fn adr139_replay_owned_by_coordinator_not_scheduler() {
	let coordinator = coordinator_source();
	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");

	assert!(coordinator.contains("retried_broadcast_replay"));
	assert!(coordinator.contains("force_hash_join_ids = force_hash_join_ids"));
	assert!(coordinator.contains("broadcast_cap_replay_trigger"));
	assert!(!scheduler.contains("force_hash_join_ids"));
	assert!(!scheduler.contains("broadcast_cap_replay"));
}

#[test]
pub(crate) fn adr139_mutation_paths_return_before_broadcast_replay() {
	let coordinator = coordinator_source();
	let execute = source_block(&coordinator, "async fn execute_query_with_admission");
	let replay_state = execute
		.find("let mut planner_config = self.config.clone()")
		.expect("broadcast replay state in read path");

	assert!(execute[..replay_state].contains("LogicalPlan::Insert { .. }"));
	assert!(execute[..replay_state].contains("LogicalPlan::InsertSelect { .. }"));
	assert!(execute[..replay_state].contains("LogicalPlan::CreateTableAsSelect { .. }"));
	assert!(execute[..replay_state].contains("LogicalPlan::Delete { .. }"));
	assert!(execute[..replay_state].contains("LogicalPlan::Update { .. }"));
	assert!(execute[..replay_state].matches("return result;").count() >= 2);
	assert!(!execute[..replay_state].contains("retried_broadcast_replay"));
	assert!(!execute[..replay_state].contains("force_hash_join_ids"));
}

#[test]
pub(crate) fn adr139_streaming_replay_requires_no_batch_sent() {
	let coordinator = coordinator_source();
	let streaming = source_block(&coordinator, "async fn run_streaming_read_driver");
	let trigger = streaming
		.find("broadcast_cap_replay_trigger(&error, &stages")
		.expect("streaming broadcast replay trigger");
	let force_hash = streaming
		.find("config.force_hash_join_ids = force_hash_join_ids")
		.expect("streaming replay force-hash switch");

	assert!(trigger < force_hash);
	assert!(streaming[..force_hash].contains("!batch_sent.load(Ordering::SeqCst)"));
	assert!(
		streaming
			.contains("with_final_result_sender(Some(sender.clone()), Some(batch_sent.clone()))")
	);
}

#[test]
pub(crate) fn adr139_replay_mode_demotes_auto_and_sql_broadcasts_only() {
	let binder = binder_source();
	let choose = source_block(&binder, "fn choose_join_strategy");
	let bind_join = source_block(&binder, "async fn bind_join");

	assert!(choose.contains("replay_id: JoinReplayId"));
	assert!(choose.contains("force_hash_join_ids.contains(&replay_id)"));
	assert!(choose.contains("JoinStrategyIntent::Auto if force_hash"));
	assert!(choose.contains("source: JoinStrategyPinSource::SqlHint"));
	assert!(choose.contains("HashAfterBroadcastCap"));
	assert!(choose.contains("JoinStrategyIntent::Pinned { strategy, .. }"));
	assert!(bind_join.contains("force_demoted_sql_broadcast"));
	assert!(bind_join.contains("strip_root_exchange(left_plan.plan)"));
	assert!(binder.contains("force_hash_join_ids_demotes_auto_and_sql_broadcast_only"));
}

#[test]
pub(crate) fn adr139_replay_mode_installs_hash_exchanges_for_demoted_pins() {
	let binder = binder_source();
	let bind_join = source_block(&binder, "async fn bind_join");

	assert!(bind_join.contains("force_demoted_sql_broadcast"));
	assert!(bind_join.contains("strip_root_exchange(left_plan.plan)"));
	assert!(bind_join.contains("exchange_for_join_keys("));
	assert!(binder.contains("terminal_exchange(&left)"));
	assert!(binder.contains("terminal_exchange(&right)"));
	assert!(binder.contains("DEFAULT_AUTO_JOIN_PARTITION_COUNT"));
}

#[test]
pub(crate) fn adr139_replay_attempt_has_no_broadcast_join_and_is_one_shot() {
	let binder = binder_source();
	let coordinator = coordinator_source();

	assert!(binder.contains("HashAfterBroadcastCap"));
	assert!(binder.contains("assert_join_strategy(&auto_plan, JoinStrategy::HashPartitioned)"));
	assert!(binder.contains("assert_eq!(strategy, JoinStrategy::HashPartitioned)"));
	assert!(coordinator.contains("let mut retried_broadcast_replay = false"));
	assert!(coordinator.contains("if !retried_broadcast_replay"));
	assert!(coordinator.contains("retried_broadcast_replay = true"));
	assert!(!coordinator.contains("BudgetDimension::ShuffleBytes"));
}

#[test]
pub(crate) fn adr139_shuffle_bytes_not_a_replay_trigger() {
	let coordinator = coordinator_source();
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");

	assert!(trigger.contains("BudgetDimension::BroadcastReplication"));
	assert!(!trigger.contains("ShuffleBytes"));
}

#[test]
pub(crate) fn adr139_fragment_namespace_offsets_stage_and_exchange_ids() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragment source");
	let config = source_block(&fragment, "pub struct FragmentPlanConfig");
	let allocator_new = source_block(&fragment, "fn new(id_namespace: u32)");
	let lib = lib_source();

	assert!(config.contains("id_namespace: u32"));
	assert!(fragment.contains("with_id_namespace"));
	assert!(fragment.contains("FRAGMENT_ID_NAMESPACE_STRIDE"));
	assert!(allocator_new.contains("checked_mul(FRAGMENT_ID_NAMESPACE_STRIDE)"));
	assert!(allocator_new.contains("next_stage: base"));
	assert!(allocator_new.contains("next_exchange: base"));
	assert!(!allocator_new.contains("AttemptId"));
	assert!(lib.contains("replay_uses_fresh_fragment_namespace"));
}

#[test]
pub(crate) fn adr139_attempt_id_not_used_as_replay_namespace() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragment source");
	let coordinator = coordinator_source();
	let allocator_new = source_block(&fragment, "fn new(id_namespace: u32)");

	assert!(allocator_new.contains("id_namespace"));
	assert!(!allocator_new.contains("AttemptId"));
	assert!(coordinator.contains("fragment_id_namespace"));
	assert!(!coordinator.contains("AttemptId::new(fragment_id_namespace"));
}

#[test]
pub(crate) fn adr139_partition_payload_size_unit_unchanged() {
	let partition_store =
		std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
			.expect("read partition store source");
	let payload_size = source_block(&partition_store, "pub fn size_bytes");

	assert!(payload_size.contains("ByteCount::from_usize(self.bytes.len())"));
}

#[test]
pub(crate) fn adr139_138_estimate_channel_untouched() {
	let binder = binder_source();
	let lib = lib_source();

	assert!(binder.contains("byte_upper_bound: Option<ByteCount>"));
	assert!(binder.contains("estimated_byte_size: Option<ByteCount>"));
	assert!(binder.contains("fn derive_estimated_byte_size"));
	assert!(lib.contains("estimated_broadcast_over_cap_replays_as_hash_and_matches_digest"));
	assert!(lib.contains("hard_bound_broadcast_over_cap_replays_as_hash_and_matches_digest"));
}

#[test]
pub(crate) fn adr140_execution_join_carries_stable_replay_id_and_replay_eligibility() {
	let execution = execution_source();
	let execution_plan = source_block(&execution, "pub enum ExecutionPlan");
	let join_replay_id = source_block(&execution, "pub struct JoinReplayId");
	let replay_eligibility = source_block(&execution, "pub enum BroadcastReplayEligibility");

	assert!(join_replay_id.contains("pub struct JoinReplayId(pub u32)"));
	assert!(
		execution
			.contains("PartialOrd, Ord, Hash, Serialize, Deserialize)]\npub struct JoinReplayId")
	);
	assert!(replay_eligibility.contains("Replayable"));
	assert!(replay_eligibility.contains("Terminal"));
	assert!(execution_plan.contains("replay_id: JoinReplayId"));
	assert!(execution_plan.contains("replay_eligibility: BroadcastReplayEligibility"));
	assert!(!execution_plan.contains("JoinStrategyIntent"));
}

#[test]
pub(crate) fn adr140_join_replay_id_is_trace_join_index_deterministic() {
	let binder = binder_source();
	let bind_join = source_block(&binder, "async fn bind_join");

	assert!(bind_join.contains("let join_index = trace.join_decisions.len()"));
	assert!(bind_join.contains("let replay_id = JoinReplayId(join_index as u32)"));
	assert!(bind_join.contains("choose_join_strategy("));
	assert!(bind_join.contains("replay_id,"));
	assert!(bind_join.contains("target: JoinTraceTarget { join_index }"));
	assert!(binder.contains("join_replay_id_deterministic_across_rebind"));
}

#[test]
pub(crate) fn adr140_optimizer_receipt_target_uses_join_replay_id() {
	let binder = binder_source();
	let target = source_block(&binder, "struct JoinTraceTarget");
	let bind_join = source_block(&binder, "async fn bind_join");

	assert!(target.contains("join_index: usize"));
	assert!(!target.contains("TableId"));
	assert!(bind_join.contains("let replay_id = JoinReplayId(join_index as u32)"));
	assert!(bind_join.contains("target: JoinTraceTarget { join_index }"));
}

#[test]
pub(crate) fn adr140_fragment_rewrites_preserve_join_replay_metadata() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragment source");
	let join_boundary = source_block(&fragment, "struct JoinBoundary");
	let hash_fragment = source_block(&fragment, "fn fragment_hash_join_plan");
	let star_rewrite = source_block(&fragment, "fn rewrite_broadcast_star_probe");
	let replace_exchange = source_block(&fragment, "fn replace_exchange_with_scan");

	assert!(join_boundary.contains("replay_id: JoinReplayId"));
	assert!(join_boundary.contains("replay_eligibility: BroadcastReplayEligibility"));
	assert!(hash_fragment.contains("replay_id: join.replay_id"));
	assert!(hash_fragment.contains("replay_eligibility: join.replay_eligibility"));
	assert!(star_rewrite.contains("replay_id,"));
	assert!(star_rewrite.contains("replay_eligibility,"));
	assert!(replace_exchange.contains("replay_id,"));
	assert!(replace_exchange.contains("replay_eligibility,"));
}

#[test]
pub(crate) fn adr140_replay_force_hash_targets_join_id_set_not_global_bool() {
	let binder = binder_source();
	let coordinator = coordinator_source();
	let config = source_block(&binder, "pub struct PlannerConfig");
	let choose = source_block(&binder, "fn choose_join_strategy");

	assert!(config.contains("force_hash_join_ids: BTreeSet<JoinReplayId>"));
	assert!(!config.contains("force_hash_joins: bool"));
	assert!(choose.contains("replay_id: JoinReplayId"));
	assert!(choose.contains("force_hash_join_ids.contains(&replay_id)"));
	assert!(coordinator.contains("force_hash_join_ids = force_hash_join_ids"));
	assert!(!coordinator.contains("force_hash_joins = true"));
}

#[test]
pub(crate) fn adr140_terminal_pins_do_not_globally_veto_replay() {
	let binder = binder_source();
	let coordinator = coordinator_source();

	assert!(binder.contains("fn broadcast_replay_eligibility"));
	assert!(binder.contains("BroadcastReplayEligibility::Replayable"));
	assert!(binder.contains("BroadcastReplayEligibility::Terminal"));
	assert!(!coordinator.contains("has_terminal_pinned_broadcast"));
	assert!(!coordinator.contains("plan_broadcast_replay_class"));
	assert!(coordinator.contains("collect_replayable_broadcast_join_ids"));
	assert!(coordinator.contains("BroadcastReplayEligibility::Replayable"));
	assert!(binder.contains("mixed_sql_and_rune_broadcast_plan_replays_sql_preserves_rune"));
}

#[test]
pub(crate) fn adr140_failed_task_collects_replayable_id_set_not_single() {
	let coordinator = coordinator_source();
	let lib = lib_source();
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");
	let task_collect = source_block(&coordinator, "fn task_key_replayable_broadcast_join_ids");
	let plan_collect = source_block(&coordinator, "fn collect_replayable_broadcast_join_ids");

	assert!(trigger.contains("Option<BTreeSet<JoinReplayId>>"));
	assert!(trigger.contains("task_key_replayable_broadcast_join_ids(stages, *key)"));
	assert!(trigger.contains("(!ids.is_empty()).then_some(ids)"));
	assert!(task_collect.contains("let mut ids = BTreeSet::new()"));
	assert!(plan_collect.contains("ids.insert(*replay_id)"));
	assert!(plan_collect.contains("collect_replayable_broadcast_join_ids(left, ids)"));
	assert!(plan_collect.contains("collect_replayable_broadcast_join_ids(right, ids)"));
	assert!(!coordinator.contains("single_broadcast"));
	assert!(lib.contains("broadcast_star_over_cap_replays_all_task_local_dims_and_succeeds"));
}

#[test]
pub(crate) fn adr140_preserves_139_fences() {
	let coordinator = coordinator_source();
	let streaming = source_block(&coordinator, "async fn run_streaming_read_driver");
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");
	let execute = source_block(&coordinator, "async fn execute_query_with_admission");

	assert!(coordinator.contains("let mut retried_broadcast_replay = false"));
	assert!(coordinator.contains("retried_broadcast_replay = true"));
	assert!(streaming.contains("!batch_sent.load(Ordering::SeqCst)"));
	assert!(coordinator.contains("fragment_id_namespace"));
	assert!(coordinator.contains("next_fragment_id_namespace"));
	assert!(trigger.contains("BudgetDimension::BroadcastReplication"));
	assert!(!trigger.contains("ShuffleBytes"));
	assert!(execute.contains("LogicalPlan::Insert { .. }"));
	assert!(execute.contains("LogicalPlan::Delete { .. }"));
	assert!(execute.contains("LogicalPlan::Update { .. }"));
}

#[test]
pub(crate) fn adr140_no_failure_wire_or_shufflebytes_scope_change() {
	let coordinator = coordinator_source();
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let task = std::fs::read_to_string(manifest_dir().join("src/distributed/task.rs"))
		.expect("read task source");

	assert!(trigger.contains("BudgetDimension::BroadcastReplication"));
	assert!(!trigger.contains("ShuffleBytes"));
	assert!(!server.contains("replay_hint"));
	assert!(!task.contains("broadcast_join_id"));
	assert!(!task.contains("replay_hint"));
}

#[test]
pub(crate) fn adr140_138_fact_split_untouched() {
	let binder = binder_source();

	assert!(binder.contains("byte_upper_bound: Option<ByteCount>"));
	assert!(binder.contains("estimated_byte_size: Option<ByteCount>"));
	assert!(binder.contains("fn derive_estimated_byte_size"));
	assert!(!binder.contains("estimated_byte_size: None,\n\t\t\tbyte_upper_bound"));
}

#[test]
pub(crate) fn adr141_column_ndv_fact_is_descriptor_adjacent_not_schema_descriptor() {
	let catalog = catalog_source();
	let table_policy = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema");

	assert!(catalog.contains("pub struct DescriptorColumnNdvFact"));
	assert!(catalog.contains("pub struct DescriptorOptimizerFacts"));
	assert!(catalog.contains("#[serde(default)]"));
	assert!(catalog.contains("pub column_ndv: Vec<DescriptorColumnNdvFact>"));
	assert!(catalog.contains("pub struct TableOptimizerFacts"));
	assert!(catalog.contains("pub column_ndv: BTreeMap<ColumnOrdinal, DistinctCountFact>"));
	assert!(!table_policy.contains("column_ndv"));
	assert!(!table_policy.contains("DistinctCountFact"));
}

#[test]
pub(crate) fn adr141_ndv_fact_is_labeled_exact_or_approximate() {
	let catalog = catalog_source();
	let fact = source_block(&catalog, "pub enum DistinctCountFact");

	assert!(fact.contains("Exact(u64)"));
	assert!(fact.contains("Approx { estimate: u64, rel_err_ppm: u32 }"));
	assert!(!fact.contains("f64"));
	assert!(catalog.contains("pub const fn point_estimate"));
}

#[test]
pub(crate) fn adr141_selectivity_writes_estimated_row_count_only() {
	let binder = binder_source();
	let facts = source_block(&binder, "impl PlanOptimizerFacts");
	let filter_arm = binder
		.split("LogicalPlan::Filter { input, predicate } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Project").next())
		.expect("Filter bind arm");

	assert!(binder.contains("estimated_row_count: Option<RowCount>"));
	assert!(facts.contains("fn with_estimated_row_count"));
	assert!(facts.contains("estimated_row_count: Some(estimate)"));
	assert!(!facts.contains("byte_upper_bound: Some(estimate"));
	assert!(filter_arm.contains("filter_equality_ndv_estimated_row_count"));
	assert!(filter_arm.contains(".with_estimated_row_count(estimated_row_count)"));
	assert!(binder.contains("ndv_selectivity_writes_estimated_row_count_not_hard_bound"));
}

#[test]
pub(crate) fn adr141_exact_ndv_does_not_write_hard_bound_this_cut() {
	let binder = binder_source();
	let catalog = catalog_source();
	let selectivity = source_block(&binder, "fn equality_ndv_estimated_row_count");
	let fact = source_block(&catalog, "pub enum DistinctCountFact");

	assert!(fact.contains("Exact(u64)"));
	assert!(selectivity.contains("distinct_count.point_estimate()"));
	assert!(!selectivity.contains("with_pk_row_cap"));
	assert!(binder.contains("exact_ndv_still_uses_estimate_channel"));
}

#[test]
pub(crate) fn adr141_estimated_rows_feed_estimated_bytes_not_hard_bytes() {
	let binder = binder_source();
	let derive = source_block(&binder, "fn derive_estimated_byte_size");
	let row_source = source_block(&binder, "fn estimate_row_source");
	let choose = source_block(&binder, "fn build_byte_candidate");

	assert!(derive.contains("estimate_row_source(self.row_upper_bound, self.estimated_row_count)"));
	assert!(derive.contains("estimate_materialized_byte_size(row_count, schema)"));
	assert!(row_source.contains("Some(std::cmp::min(hard, estimated))"));
	assert!(
		choose.find("facts.byte_upper_bound").unwrap()
			< choose.find("estimated_byte_size").unwrap()
	);
	assert!(!derive.contains("byte_upper_bound: Some"));
}

#[test]
pub(crate) fn adr141_ndv_selectivity_excludes_or_range_and_multicolumn() {
	let binder = binder_source();
	let helper = source_block(
		&binder,
		"fn predicate_node_equality_ndv_estimated_row_count",
	);

	assert!(helper.contains("BoundBinaryOp::Eq"));
	assert!(helper.contains("BoundBinaryOp::And"));
	assert!(!helper.contains("BoundBinaryOp::Or"));
	assert!(!helper.contains("BoundBinaryOp::Lt"));
	assert!(binder.contains("fn filter_range_selectivity_estimated_row_count"));
	assert!(binder.contains("range_filter_uses_min_max_range_selectivity_not_ndv"));
}

#[test]
pub(crate) fn adr141_and_filters_do_not_multiply_ndv_selectivities() {
	let binder = binder_source();
	let helper = source_block(
		&binder,
		"fn predicate_node_equality_ndv_estimated_row_count",
	);
	let facts = source_block(&binder, "fn with_estimated_row_count");

	assert!(helper.contains("Some(std::cmp::min(left, right))"));
	assert!(facts.contains("std::cmp::min(existing, estimate)"));
	assert!(binder.contains("and_filter_uses_single_equality_not_independence_product"));
}

#[test]
pub(crate) fn adr141_missing_ndv_is_inert() {
	let binder = binder_source();
	let lookup = source_block(&binder, "fn column_ndv_for_input");

	assert!(lookup.contains("Option<DistinctCountFact>"));
	assert!(lookup.contains("find_map"));
	assert!(binder.contains("missing_ndv_keeps_137_138_behavior"));
}

#[test]
pub(crate) fn adr141_scan_ndv_is_bind_time_only_not_executed() {
	let execution = execution_source();
	let binder = binder_source();
	let exec = exec_source();
	let execute_bound = source_block(&exec, "pub fn execute_bound");

	assert!(execution.contains("column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>"));
	assert!(binder.contains("column_ndv_for_input"));
	assert!(binder.contains("scan_plan"));
	assert!(!execute_bound.contains("column_ndv"));
}

#[test]
pub(crate) fn adr141_wendelon_producer_is_142_owned_and_toron_types_do_not_leak() {
	let facts = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("src")
			.join("schema")
			.join("optimizer_facts.rs"),
	)
	.expect("read Wendelon optimizer facts");

	assert!(facts.contains("pub struct TableOptimizerFacts"));
	assert!(facts.contains("pub struct ColumnNdvFact"));
	assert!(facts.contains("pub enum NdvFact"));
	assert!(
		!facts.contains("V1"),
		"new Wendelon optimizer fact types must follow no-version-suffixes.md"
	);
	assert!(!facts.contains("DistinctCountFact"));
	assert!(!facts.contains("DescriptorColumnNdvFact"));
	assert!(!facts.contains("DescriptorOptimizerFacts"));
}

#[test]
pub(crate) fn adr142_wendelon_optimizer_facts_have_no_toron_dependency() {
	let manifest = std::fs::read_to_string(wendelon_manifest_dir().join("Cargo.toml"))
		.expect("read Wendelon manifest");
	let facts = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("src")
			.join("schema")
			.join("optimizer_facts.rs"),
	)
	.expect("read Wendelon optimizer facts");
	let ndv_fact = source_block(&facts, "pub enum NdvFact");

	assert!(!manifest.contains("toron"));
	assert!(!facts.contains("toron"));
	assert!(facts.contains("rel_err_ppm: u32"));
	assert!(!ndv_fact.contains("f64"));
}

#[test]
pub(crate) fn adr142_table_schema_descriptor_not_mutated_for_optimizer_facts() {
	let table = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema");
	let descriptor = source_block(&table, "pub struct TableSchemaDescriptor");

	assert!(!descriptor.contains("TableOptimizerFacts"));
	assert!(!descriptor.contains("column_ndv"));
	assert!(!descriptor.contains("NdvFact"));
}

#[test]
pub(crate) fn adr142_descriptor_get_list_wire_contracts_unchanged_and_facts_are_separate() {
	let core = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/core.rs"))
		.expect("read Wendelon schema core");
	let client = std::fs::read_to_string(wendelon_manifest_dir().join("src/client/core.rs"))
		.expect("read Wendelon client");

	assert!(core.contains("TABLE_DESCRIPTOR_GET_REQ: Self = Self(0x3C)"));
	assert!(core.contains("TABLE_DESCRIPTOR_GET_RESP: Self = Self(0x3D)"));
	assert!(core.contains("TABLE_DESCRIPTOR_LIST_REQ: Self = Self(0x44)"));
	assert!(core.contains("TABLE_DESCRIPTOR_LIST_RESP: Self = Self(0x45)"));
	assert!(core.contains("TABLE_OPTIMIZER_FACTS_GET_REQ: Self = Self(0x46)"));
	assert!(core.contains("TABLE_OPTIMIZER_FACTS_GET_RESP: Self = Self(0x47)"));
	assert!(client.contains("TABLE_DESCRIPTOR_GET_RESP descriptor_len mismatch"));
	assert!(client.contains("TABLE_DESCRIPTOR_LIST_RESP descriptors_len mismatch"));
	assert!(client.contains("TABLE_OPTIMIZER_FACTS_GET_RESP facts_len mismatch"));

	let facts_get = source_block(&client, "pub async fn get_table_optimizer_facts");
	assert!(
		!facts_get.contains("Wendelon"),
		"new optimizer-facts protocol errors must follow no-project-name-in-protocols.md"
	);
}

#[test]
pub(crate) fn adr142_default_stats_policy_stays_empty() {
	let table = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema");

	assert!(
		table.contains("#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]")
	);
	assert!(table.contains("stats_policy: ColumnStatsPolicyV1::default()"));
}

#[test]
pub(crate) fn adr144_range_facts_are_descriptor_sidecars() {
	let catalog = catalog_source();
	let execution = execution_source();
	let table = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema");
	let wendelon_facts = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("src")
			.join("schema")
			.join("optimizer_facts.rs"),
	)
	.expect("read Wendelon optimizer facts");
	let descriptor = source_block(&table, "pub struct TableSchemaDescriptor");

	assert!(wendelon_facts.contains("pub struct ColumnRangeFact"));
	assert!(catalog.contains("pub struct RangeFact"));
	assert!(catalog.contains("pub struct DescriptorColumnRangeFact"));
	assert!(catalog.contains("column_range: BTreeMap<ColumnOrdinal, RangeFact>"));
	assert!(catalog.contains("column_range: Vec<DescriptorColumnRangeFact>"));
	assert!(catalog.contains("fn descriptor_column_range"));
	assert!(catalog.contains("descriptor_optimizer_facts_from_storage"));
	assert!(execution.contains("column_range: Vec<(ColumnOrdinal, RangeFact)>"));
	assert!(!descriptor.contains("ColumnRangeFact"));
	assert!(!descriptor.contains("column_range"));
}

#[test]
pub(crate) fn adr144_default_stats_policy_remains_empty() {
	let table = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema");
	let manager = std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read Wendelon slab manager");

	assert!(table.contains("stats_policy: ColumnStatsPolicyV1::default()"));
	assert!(manager.contains("range_fact_with_empty_stats_policy_is_produced_from_min_max"));
}

#[test]
pub(crate) fn adr144_range_scalar_tags_are_schema_owned() {
	let binder = binder_source();
	let schema_facts = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("src")
			.join("schema")
			.join("optimizer_facts.rs"),
	)
	.expect("read Wendelon optimizer facts");
	let storage_stats =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/stats.rs"))
			.expect("read Wendelon storage stats");
	let client = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("crates")
			.join("wendelon-client")
			.join("src")
			.join("lib.rs"),
	)
	.expect("read wendelon-client lib");

	assert!(schema_facts.contains("pub const ARROW_STATS_TAG_INT64"));
	assert!(schema_facts.contains("pub fn decode_signed_optimizer_fact_scalar"));
	assert!(schema_facts.contains("pub fn encode_signed_optimizer_fact_scalar"));
	assert!(storage_stats.contains("pub use crate::schema::{"));
	assert!(client.contains("ARROW_STATS_TAG_INT64"));
	assert!(client.contains("ARROW_STATS_TAG_UTF8"));
	assert!(client.contains("decode_signed_optimizer_fact_scalar"));
	assert!(binder.contains("wendelon_client::{"));
	assert!(binder.contains("decode_signed_optimizer_fact_scalar"));
	assert!(!binder.contains("wendelon::storage::stats"));
}

#[test]
pub(crate) fn adr144_range_fact_wire_compatibility_is_explicit() {
	let schema_facts = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("src")
			.join("schema")
			.join("optimizer_facts.rs"),
	)
	.expect("read Wendelon optimizer facts");

	assert!(schema_facts.contains("struct LegacyTableOptimizerFacts"));
	assert!(schema_facts.contains("impl From<LegacyTableOptimizerFacts> for TableOptimizerFacts"));
	assert!(schema_facts.contains("postcard::from_bytes::<LegacyTableOptimizerFacts>(bytes)"));
	assert!(schema_facts.contains("column_range: Vec::new()"));
	assert!(schema_facts.contains("table_optimizer_facts_legacy_payload_decodes_empty_range"));
}

#[test]
pub(crate) fn adr144_range_selectivity_estimate_channel_only() {
	let binder = binder_source();
	let filter_arm = binder
		.split("LogicalPlan::Filter")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Project").next())
		.expect("Filter bind arm");
	let range = source_block(&binder, "fn filter_range_selectivity_estimated_row_count");
	let estimate = source_block(&binder, "fn estimated_rows_for_range");

	assert!(filter_arm.contains("filter_range_selectivity_estimated_row_count"));
	assert!(filter_arm.contains("min_optional_row_count"));
	assert!(filter_arm.contains(".with_estimated_row_count(estimated_row_count)"));
	assert!(range.contains("facts.origin != PlanOrigin::SingleBaseRelation"));
	assert!(estimate.contains("range.non_null_count"));
	assert!(!range.contains("row_upper_bound"));
	assert!(!range.contains("byte_upper_bound"));
	assert!(!estimate.contains("row_upper_bound"));
	assert!(!estimate.contains("byte_upper_bound"));
	assert!(!estimate.contains("EmptyRelation"));
	assert!(binder.contains("range_filter_estimates_without_row_upper_bound"));
	assert!(binder.contains("range_outside_envelope_estimates_zero_without_hard_bound"));
}

#[test]
pub(crate) fn adr144_ndv_helper_remains_equality_only() {
	let binder = binder_source();
	let ndv = source_block(
		&binder,
		"fn predicate_node_equality_ndv_estimated_row_count",
	);
	let range = source_block(&binder, "fn filter_range_selectivity_estimated_row_count");

	assert!(ndv.contains("BoundBinaryOp::Eq"));
	assert!(!ndv.contains("BoundBinaryOp::Lt"));
	assert!(!ndv.contains("BoundBinaryOp::Gte"));
	assert!(!range.contains("column_ndv"));
	assert!(binder.contains("range_and_equality_take_single_tightest_estimate"));
}

#[test]
pub(crate) fn adr144_no_kll_quantile_optimizer_fact() {
	let binder = binder_source();
	let schema_facts = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("src")
			.join("schema")
			.join("optimizer_facts.rs"),
	)
	.expect("read Wendelon optimizer facts");
	let range = source_block(&binder, "fn filter_range_selectivity_estimated_row_count");

	for forbidden in [
		"Kll",
		"KLL",
		"Quantile",
		"quantile",
		"Histogram",
		"histogram",
	] {
		assert!(
			!schema_facts.contains(forbidden),
			"range optimizer facts must not introduce {forbidden}"
		);
		assert!(
			!range.contains(forbidden),
			"range selectivity helper must not introduce {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr144_range_producer_reuses_full_coverage_and_truncation_fence() {
	let manager = std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read Wendelon slab manager");
	let table_facts = source_block(&manager, "pub async fn table_optimizer_facts");
	let range_fact = source_block(&manager, "fn range_fact_for_column");

	assert!(table_facts.contains("has_uncovered_local_rows_for_descriptor"));
	assert!(table_facts.contains("return Ok(None)"));
	assert!(range_fact.contains("COLUMN_STAT_MIN_TRUNCATED"));
	assert!(range_fact.contains("COLUMN_STAT_MAX_TRUNCATED"));
	assert!(range_fact.contains("non_null_count"));
	assert!(manager.contains("range_fact_requires_full_contributor_coverage"));
	assert!(manager.contains("range_truncated_min_max_omits_fact"));
}

#[test]
pub(crate) fn adr147_float_range_facts_are_finite_and_estimate_channel_only() {
	let binder = binder_source();
	let schema_facts = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("src")
			.join("schema")
			.join("optimizer_facts.rs"),
	)
	.expect("read Wendelon optimizer facts");
	let storage_stats =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/stats.rs"))
			.expect("read Wendelon storage stats");
	let manager = std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read Wendelon slab manager");
	let client = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("crates")
			.join("wendelon-client")
			.join("src")
			.join("lib.rs"),
	)
	.expect("read wendelon-client lib");
	let arrow_tag = source_block(&storage_stats, "pub fn arrow_stats_type_tag");
	let collect = source_block(&storage_stats, "fn collect_column_stats_in_one_pass");
	let collect_f32 = source_block(&storage_stats, "fn collect_float32_column_stats");
	let collect_f64 = source_block(&storage_stats, "fn collect_float64_column_stats");
	let scalar = source_block(&binder, "fn range_scalar_for_fact");
	let real_estimate = source_block(&binder, "fn estimated_rows_for_real_range");

	assert!(schema_facts.contains("pub const ARROW_STATS_TAG_FLOAT32"));
	assert!(schema_facts.contains("pub const ARROW_STATS_TAG_FLOAT64"));
	assert!(schema_facts.contains("pub fn encode_float32_optimizer_fact_scalar"));
	assert!(schema_facts.contains("pub fn encode_float64_optimizer_fact_scalar"));
	assert!(schema_facts.contains("pub fn decode_float32_optimizer_fact_scalar"));
	assert!(schema_facts.contains("pub fn decode_float64_optimizer_fact_scalar"));
	assert!(schema_facts.contains("float_canonical_encode_round_trips_and_orders"));
	assert!(schema_facts.contains("range_fact_tag_supported"));
	assert!(client.contains("ARROW_STATS_TAG_FLOAT64"));
	assert!(client.contains("encode_float64_optimizer_fact_scalar"));
	assert!(client.contains("decode_float64_optimizer_fact_scalar"));
	assert!(arrow_tag.contains("DataType::Float32"));
	assert!(arrow_tag.contains("DataType::Float64"));
	assert!(collect.contains("collect_float32_column_stats"));
	assert!(collect.contains("collect_float64_column_stats"));
	assert!(collect_f32.contains("partial_cmp"));
	assert!(collect_f64.contains("partial_cmp"));
	assert!(storage_stats.contains("counts_only_column_stats"));
	assert!(storage_stats.contains("float_stats_normalizes_negative_zero"));
	assert!(storage_stats.contains("table_optimizer_facts_emits_float_range_for_finite_values"));
	assert!(
		storage_stats.contains("table_optimizer_facts_suppresses_float_range_with_nan_or_infinity")
	);
	assert!(manager.contains("table_optimizer_facts_emits_float_range_for_finite_values"));
	assert!(manager.contains("table_optimizer_facts_suppresses_float_range_with_nan_or_infinity"));
	assert!(binder.contains("enum RangeScalar"));
	assert!(binder.contains("RangeScalar::Integral"));
	assert!(binder.contains("RangeScalar::Real"));
	assert!(binder.contains("struct FloatBound"));
	assert!(binder.contains("RangeInterval::Real"));
	assert!(scalar.contains("ScalarValue::Int32"));
	assert!(scalar.contains("ScalarValue::Int64"));
	assert!(scalar.contains("ScalarValue::Float32"));
	assert!(scalar.contains("ScalarValue::Float64"));
	assert!(scalar.contains("exact_i64_to_f64"));
	assert!(real_estimate.contains("real_interval_contains_value"));
	assert!(real_estimate.contains("clip_real_lower"));
	assert!(real_estimate.contains("clip_real_upper"));
	assert!(!real_estimate.contains("row_upper_bound"));
	assert!(!real_estimate.contains("byte_upper_bound"));
	assert!(!real_estimate.contains("EmptyRelation"));
	assert!(!real_estimate.contains("Kll"));
	assert!(!real_estimate.contains("Quantile"));
	assert!(
		binder.contains(
			"float_range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate"
		)
	);
	assert!(
		binder.contains("float_range_endpoint_inclusive_ge_max_estimates_one_row_but_gt_max_zero")
	);
	assert!(binder.contains("float_range_does_not_write_hard_bounds_or_empty_relation"));
	assert!(binder.contains("int_literal_against_float_fact_consumed_as_f64"));
	assert!(binder.contains("integer_range_facts_keep_discrete_interpolation"));
	assert!(binder.contains("utf8_range_facts_still_fail_open"));
	assert!(binder.contains("opaque_float_range_facts_still_fail_open"));
}

#[test]
pub(crate) fn adr148_in_list_ndv_selectivity_is_consumer_only_and_estimate_channel() {
	let binder = binder_source();
	let filter = source_block(&binder, "fn filter_equality_ndv_estimated_row_count");
	let predicate = source_block(
		&binder,
		"fn predicate_node_equality_ndv_estimated_row_count",
	);
	let helper = source_block(&binder, "fn in_list_ndv_estimated_row_count");
	let literal = source_block(&binder, "fn in_list_literal_cardinality");
	let scalar = source_block(&binder, "fn in_list_scalar_literal_cardinality");
	let key = source_block(&binder, "enum InListLiteralKey");

	assert!(predicate.contains("BoundExprNode::InList"));
	assert!(predicate.contains("negated"));
	assert!(predicate.contains("in_list_ndv_estimated_row_count"));
	assert!(filter.contains("facts.origin != PlanOrigin::SingleBaseRelation"));
	assert!(filter.contains("facts.row_upper_bound?"));
	assert!(helper.contains("BoundExprNode::Column"));
	assert!(helper.contains("column_ndv_for_input"));
	assert!(helper.contains("BTreeSet"));
	assert!(helper.contains("in_list_literal_cardinality"));
	assert!(helper.contains("estimated_rows_for_ndv"));
	assert!(literal.contains("BoundExprNode::NullLiteral"));
	assert!(scalar.contains("is_finite"));
	assert!(key.contains("Float32(u32)"));
	assert!(key.contains("Float64(u64)"));
	for forbidden in [
		"byte_upper_bound",
		"row_upper_bound: Some",
		"EmptyRelation",
		"TableOptimizerFacts",
		"DescriptorOptimizerFacts",
		"column_range",
		"Kll",
		"KLL",
		"CountMin",
		"Count-Min",
		"Theta",
		"tensor",
		"Wendelon",
		"wendelon",
	] {
		assert!(
			!helper.contains(forbidden),
			"IN-list NDV helper leaked forbidden marker {forbidden}"
		);
	}
	assert!(binder.contains("in_list_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate"));
	assert!(binder.contains("in_list_ndv_estimates_distinct_non_null_literals"));
	assert!(binder.contains("in_list_ndv_deduplicates_duplicate_literals"));
	assert!(binder.contains("in_list_ndv_ignores_null_literals"));
	assert!(binder.contains("in_list_ndv_all_null_list_fails_open"));
	assert!(binder.contains("in_list_non_finite_float_literal_fails_open"));
	assert!(binder.contains("in_list_k_exceeds_ndv_caps_at_base_rows"));
	assert!(binder.contains("not_in_does_not_use_ndv_selectivity"));
	assert!(binder.contains("in_list_without_ndv_or_row_bound_fails_open"));
	assert!(binder.contains("in_list_and_equality_keep_tightest_estimate"));
	assert!(binder.contains("in_list_does_not_write_hard_bounds_or_empty_relation"));
	assert!(binder.contains("equality_ndv_existing_behavior_unchanged"));
}

#[test]
pub(crate) fn adr142_ndv_requires_full_contributor_coverage() {
	let manager = std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read Wendelon slab manager");
	let facts = source_block(&manager, "pub async fn table_optimizer_facts");
	let local = source_block(&manager, "async fn has_uncovered_local_rows_for_descriptor");
	let tiered = source_block(&manager, "async fn load_tiered_optimizer_stats_contributor");

	assert!(facts.contains("has_uncovered_local_rows_for_descriptor"));
	assert!(facts.contains("return Ok(None)"));
	assert!(facts.contains("load_tiered_optimizer_stats_contributor"));
	assert!(local.contains("secondary_index_table_ids_from_plaintext"));
	assert!(tiered.contains("TieredStatsContributor::OtherTable"));
}

#[test]
pub(crate) fn adr142_hll_merge_uses_registers_not_estimate_scalars() {
	let manager = std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read Wendelon slab manager");
	let helper = source_block(&manager, "fn ndv_fact_for_column");

	assert!(helper.contains("HllRegisters::from_registers"));
	assert!(helper.contains("merged.precision() == registers.precision()"));
	assert!(helper.contains("merged.merge(&registers)"));
	assert!(helper.contains("hll_complete = false"));
	assert!(helper.contains("merged_hll.estimate()"));
	assert!(!helper.contains("distinct_count_for_column_ordinal"));
}

#[test]
pub(crate) fn adr142_fact_fetch_is_fail_open_and_no_toron_stats_cache() {
	let catalog = catalog_source();
	let sync = source_block(&catalog, "async fn resolve_table_sync_owner");

	assert!(sync.contains("get_table_optimizer_facts"));
	assert!(sync.contains(".ok()\n\t\t\t\t\t.flatten()"));
	assert!(sync.contains("sync_descriptor_for_tenant_with_facts"));
	assert!(catalog.contains("facts_fetch_failure_syncs_descriptor_without_facts"));
	assert!(!catalog.contains("wendelon_stats_cache"));
	assert!(!catalog.contains("background stats scanner"));
}

#[test]
pub(crate) fn adr142_141_consumer_maps_only_explicit_hard_bounds() {
	let catalog = catalog_source();
	let mapper = source_block(&catalog, "fn descriptor_optimizer_facts_from_storage");
	let binder = binder_source();

	assert!(mapper.contains("facts.schema_generation != descriptor.schema_generation"));
	assert!(mapper.contains("facts.physical_descriptor_fingerprint"));
	assert!(mapper.contains("materialized_byte_upper_bound: None"));
	assert!(mapper.contains("row_upper_bound: facts.row_upper_bound.map(RowCount::new)"));
	assert!(mapper.contains("column_ndv"));
	assert!(catalog.contains("produced_ndv_never_writes_hard_bound"));
	assert!(catalog.contains("storage_optimizer_facts_map_hard_row_bound"));
	assert!(binder.contains("fn filter_equality_ndv_estimated_row_count"));
	assert!(binder.contains("fn estimated_rows_for_ndv"));
	assert!(binder.contains("rune_equality_filtered_dim_broadcasts_over_wendelon_ndv_no_seed"));
}

#[test]
pub(crate) fn adr143_sql_join_filter_pushdown_is_planner_lowering_only() {
	let manifest_dir = manifest_dir();
	let planner = planner_source();
	let lower_select = source_block(&planner, "fn lower_select");
	let lower_where = source_block(&planner, "fn lower_where");
	let push = source_block(&planner, "fn push_side_local_join_filters");
	let collect = source_block(&planner, "fn collect_scope_refs");
	let matcher = source_block(&planner, "fn join_filter_conjunct_matches");
	let operator_kind = source_block(&planner, "fn join_operator_kind_for_filter_pushdown");

	assert!(lower_select.contains("for join in &table_with_joins.joins"));
	assert!(lower_select.contains("all_inner_join_chain"));
	assert!(lower_select.contains("join_operator_kind_for_filter_pushdown"));
	assert!(lower_select.contains("split_top_level_and(selection)"));
	assert!(lower_select.contains("JoinFilterTarget::Left"));
	assert!(lower_select.contains("JoinFilterTarget::Right"));
	assert!(lower_select.contains("combine_and_sql_exprs"));
	assert!(lower_select.contains("self.lower_where(plan, &selection)?"));
	assert!(lower_select.contains("self.lower_where(plan, selection)?"));
	assert!(!lower_select.contains("joins around derived tables are not supported"));
	assert!(lower_where.contains("plan = LogicalPlan::Filter"));
	assert!(push.contains("pending.drain(..)"));
	assert!(push.contains("join_filter_conjunct_matches"));
	assert!(push.contains("self.lower_expr(conjunct)?"));
	assert!(push.contains("pushed.push(predicate)"));
	assert!(push.contains("LogicalPlan::Filter"));
	assert!(collect.contains("Ok(ColumnScope::Inner) => refs.inner = true"));
	assert!(matcher.contains("refs.outer && !refs.inner && !refs.unsupported"));
	assert!(matcher.contains("refs.inner && !refs.outer && !refs.unsupported"));
	assert!(operator_kind.contains("JoinOperator::Join(_) | JoinOperator::Inner(_)"));
	assert!(operator_kind.contains("Some(JoinKind::Inner)"));
	assert!(!planner.contains("side_local_join_filter_predicate"));
	assert!(!planner.contains("pbc_ineligible_false_expr"));

	let markers = [
		"JoinFilterTarget",
		"push_side_local_join_filters",
		"join_filter_conjunct_matches",
		"join_operator_kind_for_filter_pushdown",
		"all_inner_join_chain",
	];
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			path.file_name().and_then(|name| name.to_str()) != Some("sql_planner.rs")
				&& markers.iter().any(|marker| line.contains(marker))
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR143 SQL join-filter pushdown leaked outside SQL planner source:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr143_behavior_gates_cover_planner_binder_and_runtime() {
	let planner = planner_source();
	let binder = binder_source();
	let lib = lib_source();
	let multi_table_scan = source_block(&lib, "impl StorageScan for MultiTableStorageScan");

	for gate in [
		"sql_inner_join_pushes_right_local_conjunct_to_right_input",
		"sql_inner_join_pushes_leftmost_local_conjunct_to_left_input",
		"sql_multi_dim_star_pushes_each_filter_to_tightest_input",
		"sql_cross_side_conjunct_remains_residual",
		"sql_unqualified_conjunct_remains_residual",
		"sql_constant_conjunct_remains_residual",
		"sql_outer_join_chain_disables_filter_pushdown",
		"sql_filter_pushdown_preserves_broadcast_hint_resolution",
	] {
		assert!(planner.contains(gate), "missing ADR143 planner gate {gate}");
	}
	for gate in [
		"sql_equality_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate",
		"sql_inner_join_pushes_leftmost_local_conjunct_to_left_input",
		"sql_multi_dim_star_each_filtered_dim_broadcasts",
	] {
		assert!(binder.contains(gate), "missing ADR143 binder gate {gate}");
	}
	for gate in [
		"sql_filter_pushdown_digest_matches_above_join_filter",
		"sql_outer_join_chain_filter_stays_above_and_returns_empty_counterexample",
	] {
		assert!(lib.contains(gate), "missing ADR143 runtime gate {gate}");
	}
	assert!(multi_table_scan.contains("request.pbc_predicate"));
	assert!(multi_table_scan.contains("wendelon::schema::pbc::execute"));
}

#[test]
pub(crate) fn adr145_derived_tables_allowed_in_inner_join_chains() {
	let planner = planner_source();
	let lower_select = source_block(&planner, "fn lower_select");

	assert!(lower_select.contains("let all_inner_join_chain"));
	assert!(lower_select.contains("let contains_derived_join_factor"));
	assert!(lower_select.contains("table_factor_is_derived(&table_with_joins.relation)"));
	assert!(lower_select.contains(".any(|join| table_factor_is_derived(&join.relation))"));
	assert!(lower_select.contains("&& !table_with_joins.joins.is_empty()"));
	assert!(lower_select.contains("&& !all_inner_join_chain"));
	assert!(
		lower_select.contains("derived tables in joins are only supported in all-inner chains")
	);
	assert!(
		lower_select
			.contains("let mut plan = self.lower_table_factor(&table_with_joins.relation)?")
	);
	assert!(lower_select.contains("let mut right = self.lower_table_factor(&join.relation)?"));
	assert!(!lower_select.contains("joins around derived tables are not supported"));
	assert!(planner.contains("sql_derived_join_digest_matches_hand_authored_logical_plan"));
}

#[test]
pub(crate) fn adr145_derived_join_rejects_non_inner_chains() {
	let planner = planner_source();
	let lower_select = source_block(&planner, "fn lower_select");

	assert!(
		lower_select.contains("derived tables in joins are only supported in all-inner chains")
	);
	for gate in [
		"sql_outer_join_around_derived_table_stays_rejected",
		"sql_join_inside_derived_table_stays_rejected",
		"sql_nested_lateral_sampled_derived_join_fences_hold",
	] {
		assert!(planner.contains(gate), "missing ADR145 planner gate {gate}");
	}
}

#[test]
pub(crate) fn adr145_derived_join_keeps_v1_derived_fences() {
	let planner = planner_source();
	let table_factor = source_block(&planner, "fn lower_table_factor");
	let select_shape = source_block(&planner, "fn validate_derived_select_shape");
	let query_shape = source_block(&planner, "fn validate_derived_query_shape");

	for marker in [
		"lateral derived tables are not supported",
		"sampled derived tables are not supported",
		"derived tables require an alias",
		"derived table alias AT is not supported",
		"lower_derived_table",
	] {
		assert!(
			table_factor.contains(marker),
			"missing derived table-factor fence {marker}"
		);
	}
	for marker in [
		"joins inside derived tables are not supported",
		"nested derived tables are not supported",
	] {
		assert!(
			select_shape.contains(marker),
			"missing derived select-shape fence {marker}"
		);
	}
	assert!(query_shape.contains("unsupported derived table query body"));
}

#[test]
pub(crate) fn adr145_pure_subset_project_rederives_projected_byte_estimate() {
	let binder = binder_source();
	let facts_impl = source_block(&binder, "impl PlanOptimizerFacts");
	let project_arm = binder
		.split("LogicalPlan::Project {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Aggregate").next())
		.expect("Project bind arm");

	assert!(facts_impl.contains(
		"fn preserve_through_project(self, exprs: &BoundExprProgram, output_schema: &Schema) -> Self"
	));
	assert!(facts_impl.contains("project_roots_are_distinct_columns(exprs)"));
	assert!(facts_impl.contains("derive_estimated_byte_size(output_schema)"));
	assert!(project_arm.contains("schema_for_exprs_with_output_names(&exprs, output_names)"));
	assert!(project_arm.contains(".preserve_through_project(&exprs, &output_schema)"));
	assert!(binder.contains("pure_subset_project_shrinks_estimated_byte_size"));
	assert!(binder.contains("sql_join_projected_derived_dim_broadcasts_where_full_column_hashes"));
}

#[test]
pub(crate) fn adr145_expression_projects_do_not_shrink_estimated_bytes() {
	let binder = binder_source();
	let project_helper = source_block(&binder, "fn preserve_through_project");

	assert!(project_helper.contains("estimated_byte_size: None"));
	assert!(binder.contains("expression_project_keeps_byte_estimate_none"));
}

#[test]
pub(crate) fn adr145_broadcast_hints_resolve_derived_aliases() {
	let binder = binder_source();
	let planner = planner_source();

	assert!(binder.contains("sql_broadcast_hint_targets_derived_alias"));
	assert!(
		planner.contains("join_strategy_intent_for_hint(&mut broadcast_hint, &plan, &right, kind)")
	);
}

#[test]
pub(crate) fn adr145_no_wendelon_executor_storage_or_chooser_change() {
	let manifest_dir = manifest_dir();
	let binder = binder_source();
	let choose = source_block(&binder, "fn choose_auto_join_strategy");
	let candidate = source_block(&binder, "fn build_byte_candidate");
	let markers = [
		"sql_join_projected_derived_dim_broadcasts_where_full_column_hashes",
		"contains_derived_join_factor",
		"pure_subset_project_shrinks_estimated_byte_size",
		"wide_customer_sql_schema",
		"preserve_through_project(&exprs, &output_schema)",
		"derived tables in joins are only supported in all-inner chains",
	];
	let mut offenders = Vec::new();

	for path in [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/coordinator.rs"),
		manifest_dir.join("src/storage.rs"),
	] {
		let source = std::fs::read_to_string(&path).expect("read Toron source");
		offenders.extend(
			source
				.lines()
				.enumerate()
				.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
				.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
		);
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);
	assert!(
		offenders.is_empty(),
		"ADR145 planner/binder-only markers leaked into executor/storage/coordinator/Wendelon source:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
	assert!(!choose.contains("Derived"));
	assert!(!choose.contains("Project"));
	assert!(!candidate.contains("Derived"));
	assert!(!candidate.contains("Project"));
}

#[test]
pub(crate) fn adr146_hard_row_bounds_are_optimizer_facts_not_descriptor_metadata() {
	let catalog = catalog_source();
	let table = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema");
	let wendelon_facts = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("src")
			.join("schema")
			.join("optimizer_facts.rs"),
	)
	.expect("read Wendelon optimizer facts");
	let descriptor = source_block(&table, "pub struct TableSchemaDescriptor");

	assert!(wendelon_facts.contains("pub row_upper_bound: Option<u64>"));
	assert!(wendelon_facts.contains("TableOptimizerFactsWithoutRowBound"));
	assert!(catalog.contains("row_upper_bound: Option<RowCount>"));
	assert!(!wendelon_facts.contains("materialized_byte_upper_bound"));
	assert!(!descriptor.contains("row_upper_bound"));
	assert!(!descriptor.contains("materialized_byte_upper_bound"));
}

#[test]
pub(crate) fn adr146_row_bound_producer_is_coverage_proven_and_candidate_independent() {
	let manager = std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read Wendelon slab manager");
	let table_facts = source_block(&manager, "pub async fn table_optimizer_facts");

	assert!(table_facts.contains("has_uncovered_local_rows_for_descriptor"));
	assert!(table_facts.contains("TieredStatsContributor::Missing => return Ok(None)"));
	assert!(table_facts.contains("row_upper_bound_for_blocks(&blocks)"));
	assert!(table_facts.contains("row_upper_bound: Some(row_upper_bound)"));
	assert!(!table_facts.contains("ndv_candidates.is_empty() && range_candidates.is_empty()"));
	assert!(manager.contains("table_optimizer_facts_emits_row_bound_for_candidate_less_table"));
	assert!(manager.contains("table_optimizer_facts_known_empty_emits_zero_row_bound"));
	assert!(manager.contains("table_optimizer_facts_missing_tiered_stats_suppresses_row_bound"));
	assert!(manager.contains("row_upper_bound_saturates_on_overflow"));
}

#[test]
pub(crate) fn adr146_toron_imports_row_bound_without_materialized_byte_producer() {
	let catalog = catalog_source();
	let mapper = source_block(&catalog, "fn descriptor_optimizer_facts_from_storage");

	assert!(mapper.contains("row_upper_bound: facts.row_upper_bound.map(RowCount::new)"));
	assert!(mapper.contains("materialized_byte_upper_bound: None"));
	assert!(!mapper.contains("materialized_byte_upper_bound: facts"));
	assert!(catalog.contains("storage_optimizer_facts_map_hard_row_bound"));
	assert!(catalog.contains("storage_optimizer_facts_known_empty_normalizes_to_zero_bytes"));
	assert!(catalog.contains("produced_ndv_never_writes_hard_bound"));
}

#[test]
pub(crate) fn adr146_hard_row_bound_drives_estimates_without_scan_elision() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn derive_estimated_byte_size");
	let scan_named = binder
		.split("LogicalPlan::ScanNamed")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Alias").next())
		.expect("ScanNamed bind arm");

	assert!(
		estimate.contains("estimate_row_source(self.row_upper_bound, self.estimated_row_count)")
	);
	assert!(scan_named.contains("PlanOptimizerFacts::from_table(optimizer_facts)"));
	assert!(scan_named.contains("derive_estimated_byte_size(plan.schema().as_ref())"));
	assert!(!scan_named.contains("EmptyRelation"));
	assert!(binder.contains("hard_row_bound_anchors_scan_byte_estimate"));
	assert!(binder.contains("row_zero_fact_does_not_elide_scan"));
	assert!(binder.contains("known_empty_table_infers_broadcast"));
	assert!(binder.contains("direct_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate"));
}

#[test]
pub(crate) fn adr146_no_compressed_bytes_or_descriptor_counts_as_hard_fact_source() {
	let manager = std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read Wendelon slab manager");
	let wendelon_facts = std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("src")
			.join("schema")
			.join("optimizer_facts.rs"),
	)
	.expect("read Wendelon optimizer facts");
	let table_facts = source_block(&manager, "pub async fn table_optimizer_facts");

	assert!(!wendelon_facts.contains("compressed"));
	assert!(!wendelon_facts.contains("materialized_byte"));
	assert!(!table_facts.contains("size_bytes"));
	assert!(!table_facts.contains("materialized_byte"));
	assert!(!table_facts.contains("descriptor.row_count"));
}

#[test]
pub(crate) fn adr141_138_hard_estimate_split_untouched() {
	let binder = binder_source();
	let choose = source_block(&binder, "fn build_byte_candidate");

	assert!(binder.contains("byte_upper_bound: Option<ByteCount>"));
	assert!(binder.contains("estimated_byte_size: Option<ByteCount>"));
	assert!(binder.contains("estimated_row_count: Option<RowCount>"));
	assert!(
		choose.find("facts.byte_upper_bound").unwrap()
			< choose.find("estimated_byte_size").unwrap()
	);
	assert!(!choose.contains("estimated_row_count"));
}

#[test]
pub(crate) fn adr141_139_140_replay_fences_untouched() {
	let coordinator = coordinator_source();
	let binder = binder_source();
	let lib = lib_source();
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");
	let config = source_block(&binder, "pub struct PlannerConfig");

	assert!(trigger.contains("BudgetDimension::BroadcastReplication"));
	assert!(!trigger.contains("ShuffleBytes"));
	assert!(coordinator.contains("let mut retried_broadcast_replay = false"));
	assert!(config.contains("force_hash_join_ids: BTreeSet<JoinReplayId>"));
	assert!(lib.contains("ndv_underestimate_over_cap_replays_as_hash"));
}

#[test]
pub(crate) fn adr154_uniform_ndv_math_stays_current_behavior() {
	let binder = binder_source();
	let equality = source_block(&binder, "fn equality_ndv_estimated_row_count");
	let in_list = source_block(&binder, "fn in_list_ndv_estimated_row_count");
	let estimator = source_block(&binder, "fn estimated_rows_for_ndv");
	let lib = lib_source();

	assert!(equality.contains("estimated_rows_for_ndv"));
	assert!(equality.contains("distinct_count.point_estimate()"));
	assert!(in_list.contains("estimated_rows_for_ndv"));
	assert!(in_list.contains("distinct_literal_count"));
	assert!(estimator.contains("let divisor = u128::from(ndv.max(1))"));
	assert!(estimator.contains("/ divisor"));
	for forbidden in ["HeavyHitter", "CountMin", "TopK", "frequency"] {
		assert!(
			!equality.contains(forbidden) && !in_list.contains(forbidden),
			"uniform NDV helper unexpectedly contains {forbidden}"
		);
	}
	assert!(binder.contains("skewed_hot_key_filter_records_uniform_ndv_miss"));
	assert!(binder.contains("uniform_ndv_hot_key_in_list_miss_is_measured"));
	assert!(lib.contains("uniform_ndv_hot_key_miss_triggers_broadcast_replay"));
	assert!(lib.contains("uniform_ndv_miss_within_headroom_is_absorbed_no_replay"));
}

#[test]
pub(crate) fn adr154_no_live_heavy_hitter_or_count_min_producer() {
	let offenders = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.chain(source_lines(&wendelon_manifest_dir().join("src")))
		.filter(|(_, _, line)| {
			[
				"HeavyHitter",
				"CountMin",
				"Count-Min",
				"TopK",
				"top_k",
				"frequency",
			]
			.iter()
			.any(|marker| line.contains(marker))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ADR154 must not add a live heavy-hitter/count-min producer: {offenders:?}"
	);
}

#[test]
pub(crate) fn adr154_no_public_filter_explain_surface() {
	let binder = binder_source();
	let explain_schema = source_block(&binder, "fn optimizer_explain_schema");
	let explain_plan = source_block(&binder, "fn optimizer_explain_plan");

	assert!(explain_schema.contains("\"join_index\""));
	assert!(explain_schema.contains("\"strategy\""));
	assert!(explain_schema.contains("\"reason\""));
	for forbidden in [
		"filter_index",
		"estimated_rows",
		"actual_rows",
		"ndv",
		"hot_key",
		"skew",
	] {
		assert!(
			!explain_schema.contains(forbidden),
			"optimizer EXPLAIN schema gained filter/skew column {forbidden}"
		);
	}
	assert!(explain_plan.contains("join_decisions"));
	assert!(!explain_plan.contains("filter_estimates"));
}

#[test]
pub(crate) fn adr154_estimate_channel_and_replay_fences_untouched() {
	let binder = binder_source();
	let coordinator = coordinator_source();
	let with_estimate = source_block(&binder, "fn with_estimated_row_count");
	let build_candidate = source_block(&binder, "fn build_byte_candidate");
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");
	let lib = lib_source();

	assert!(with_estimate.contains("estimated_row_count: Some(estimate)"));
	assert!(!with_estimate.contains("byte_upper_bound: Some"));
	assert!(
		build_candidate.find("facts.byte_upper_bound").unwrap()
			< build_candidate.find("estimated_byte_size").unwrap()
	);
	assert!(trigger.contains("BudgetDimension::BroadcastReplication"));
	assert!(!trigger.contains("ShuffleBytes"));
	assert!(binder.contains("skew_miss_measurement_does_not_change_estimated_row_count"));
	assert!(lib.contains("run_distributed_attempts_with_replay_signal"));
}
