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
	let markers = [
		"DescriptorOptimizerFacts",
		"optimizer_facts",
		"materialized_byte_upper_bound",
	];
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
	assert!(project_arm.contains("input.facts.preserve_through_project(&exprs)"));
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
