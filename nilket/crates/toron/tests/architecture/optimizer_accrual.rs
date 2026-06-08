use super::support::*;

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

fn source_lines_for_path(path: &Path) -> Vec<(PathBuf, usize, String)> {
	if path.is_dir() {
		return source_lines(path);
	}
	let source = std::fs::read_to_string(path).expect("read source file");
	source
		.lines()
		.enumerate()
		.map(|(idx, line)| (path.to_path_buf(), idx + 1, line.to_string()))
		.collect()
}

#[test]
pub(crate) fn adr187_decision_log_is_coordinator_scoped_shared() {
	let binder_root = binder_root_source();
	let coordinator = source("src/coordinator.rs");
	let config = first_block(&binder_root, "pub struct PlannerConfig");
	let default = first_block(&binder_root, "impl Default for PlannerConfig");
	let query_coordinator = first_block(&coordinator, "pub struct QueryCoordinator");

	assert!(binder_root.contains("mod decision_log;"));
	assert!(config.contains("decision_log: Arc<OptimizerDecisionLog>"));
	assert!(default.contains("decision_log: Arc::new(OptimizerDecisionLog::default())"));
	assert!(query_coordinator.contains("config: PlannerConfig"));
	assert!(!binder_root.contains("struct Binder<'a> {\n\tdecision_log"));
}

#[test]
pub(crate) fn adr187_accrual_recording_is_always_on_not_explain_gated() {
	let join_order = source("src/binder/join_order.rs");
	let decline = first_block(&join_order, "fn record_optimizer_star_order_decline");
	let receipt = first_block(&join_order, "fn record_optimizer_star_order_receipt");

	for block in [&decline, &receipt] {
		assert!(block.contains("decision_log"));
		assert!(block.contains(".record("));
		assert!(!block.contains("render_trace"));
		assert!(!block.contains("OptimizerExplainRenderTrace"));
		assert!(!block.contains("JoinOrderRenderState"));
	}
}

#[test]
pub(crate) fn adr187_accrual_records_committed_not_scratch() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let bind_plan = first_block(&binder, "fn bind_plan_with_trace");
	let try_actuate = first_block(&join_order, "async fn try_bind_star_order_actuation");
	let preflight = first_block(&join_order, "fn preflight_star_order_strategy");
	let sequence = first_block(&join_order, "fn bind_star_order_sequence");
	let local = first_block(&join_order, "fn bind_local_star_actuation");
	let root_dimensions = first_block(&join_order, "fn bind_snowflake_root_dimensions");

	assert!(join_order.contains("pub(super) enum StarOrderActuationResult"));
	assert!(try_actuate.contains("StarOrderActuationResult::Declined"));
	assert!(try_actuate.contains("record_optimizer_star_order_receipt("));
	assert!(try_actuate.contains("record_optimizer_star_order_decline("));
	assert!(bind_plan.contains("StarOrderActuationResult::Declined"));
	assert!(bind_plan.contains("trace.record_optimizer_decisions = false;"));
	assert!(bind_plan.contains("trace.record_optimizer_decisions = previous;"));
	for block in [preflight, sequence, local, root_dimensions] {
		assert!(!block.contains("decision_log"));
		assert!(!block.contains("record_optimizer_star_order"));
	}
}

#[test]
pub(crate) fn adr187_accrual_is_side_effect_only() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let selectivity = source("src/binder/selectivity.rs");
	let pushdown = source("src/binder/pushdown.rs");
	let fragment = source("src/distributed/fragment.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");

	assert!(bind_relation.contains("decision_log.snapshot()"));
	for (name, source) in [
		("join_order", join_order.as_str()),
		("selectivity", selectivity.as_str()),
		("pushdown", pushdown.as_str()),
		("fragment", fragment.as_str()),
	] {
		assert!(
			!source.contains("decision_log.snapshot()"),
			"{name} reads optimizer decision accrual during planning"
		);
	}
}

#[test]
pub(crate) fn adr187_report_reuses_information_schema_path() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_DECISIONS_TABLE"));
	assert!(execution.contains("InformationSchemaOptimizerDecisions"));
	assert!(bind_relation.contains("INFORMATION_SCHEMA_OPTIMIZER_DECISIONS_TABLE"));
	assert!(bind_relation.contains("SystemTableKind::InformationSchemaOptimizerDecisions"));
	assert!(bind_relation.contains("system_table_plan("));
	assert!(bind_relation.contains("information_schema_optimizer_decision_rows"));
}

#[test]
pub(crate) fn adr187_decisions_only_no_runtime_capture() {
	let decision_log = source("src/binder/decision_log.rs");

	for forbidden in [
		"JoinRuntimeObservation",
		"StarOrderVerdict",
		"SnowflakeOrderVerdict",
		"render_explain_analyze",
		"actual_rows",
		"output_rows",
	] {
		assert!(
			!decision_log.contains(forbidden),
			"decision log gained runtime/verdict surface {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr187_decision_log_is_bounded() {
	let decision_log = source("src/binder/decision_log.rs");
	let log_struct = first_block(&decision_log, "pub struct OptimizerDecisionLog");
	let record = first_block(&decision_log, "pub(crate) fn record");
	let snapshot = first_block(&decision_log, "pub(crate) fn snapshot");

	assert!(decision_log.contains("DEFAULT_OPTIMIZER_DECISION_LOG_MAX_KEYS"));
	assert!(log_struct.contains("overflow_count: AtomicU64"));
	assert!(log_struct.contains("max_keys: usize"));
	assert!(record.contains("counts.len() >= self.max_keys"));
	assert!(record.contains("overflow_count.fetch_add"));
	assert!(snapshot.contains("OptimizerDecisionKind::Overflow"));
	assert!(snapshot.contains("OptimizerDecisionOutcome::Overflow"));
}

#[test]
pub(crate) fn adr187_no_executor_fragment_storage_or_wendelon_change() {
	let manifest = manifest_dir();
	let markers = [
		"OptimizerDecisionLog",
		"InformationSchemaOptimizerDecisions",
		"optimizer_decisions",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/distributed"),
		manifest.join("../wendelon/src"),
	] {
		offenders.extend(
			source_lines_for_path(&path)
				.into_iter()
				.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
		);
	}
	assert!(
		offenders.is_empty(),
		"ADR187 decision accrual leaked outside binder/coordinator/report surface:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr187_no_optimizer_shell_or_planproperties_contract() {
	let manifest = manifest_dir();
	assert!(!manifest.join("src/optimizer.rs").exists());
	assert!(!manifest.join("src/optimizer").exists());

	let offenders = source_lines(&manifest.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			let trimmed = line.trim_start();
			trimmed.starts_with("struct Optimizer ")
				|| trimmed.starts_with("pub struct Optimizer ")
				|| trimmed.starts_with("enum Optimizer ")
				|| trimmed.starts_with("pub enum Optimizer ")
				|| line.contains("PlanProperties")
				|| line.contains("Cascades")
				|| trimmed.starts_with("struct Memo ")
				|| trimmed.starts_with("pub struct Memo ")
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR187 introduced optimizer shell/property contract:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr188_measured_accrual_reuses_capture_and_verdict() {
	let binder = binder_root_source();
	let coordinator = source("src/coordinator.rs");
	let explain = source("src/binder/explain.rs");
	let task = source("src/distributed/task.rs");
	let sampled = first_block(&coordinator, "async fn run_streaming_read_driver");
	let measured = first_block(&explain, "pub(crate) fn measured_verdicts");

	assert!(task.contains("MeasureJoins"));
	assert!(binder.contains("pub(crate) async fn bind_explain_analyze"));
	assert!(binder.contains("pub(crate) fn measured_explain_analyze_verdicts"));
	assert!(sampled.contains("bind_explain_analyze"));
	assert!(sampled.contains("analyze_observation_mode"));
	assert!(sampled.contains("measured_explain_analyze_verdicts"));
	assert!(measured.contains("measured_verdicts_from_summaries"));
	assert!(explain.contains("measured_star_order_verdict"));
	assert!(explain.contains("measured_snowflake_order_verdict"));
	assert!(!task.contains("AlwaysMeasure"));
	assert!(!explain.contains("fn measured_runtime_verdict_classifier"));
}

#[test]
pub(crate) fn adr188_no_executor_capture_change() {
	let exec = source("src/exec.rs");
	let fragment = source("src/distributed/fragment.rs");
	let task = source("src/distributed/task.rs");
	let mode = first_block(&task, "pub enum JoinRuntimeObservationMode");

	assert!(mode.contains("Disabled"));
	assert!(mode.contains("MeasureJoins"));
	assert!(!mode.contains("RuntimeVerdict"));
	assert!(!exec.contains("OptimizerRuntimeVerdict"));
	assert!(!exec.contains("record_verdict"));
	assert!(!fragment.contains("OptimizerRuntimeVerdict"));
	assert!(!fragment.contains("record_verdict"));
}

#[test]
pub(crate) fn adr188_sampled_path_returns_user_data() {
	let coordinator = source("src/coordinator.rs");
	let streaming = first_block(&coordinator, "async fn run_streaming_read_driver");
	let local = brace_blocks_after_marker(&coordinator, "pub async fn execute_query")
		.into_iter()
		.map(|(_, block)| block)
		.find(|block| block.contains("self.executor.execute_bound"))
		.expect("local QueryCoordinator execute_query block");

	assert!(streaming.contains("with_final_result_sender"));
	assert!(streaming.contains("record_verdict"));
	assert!(streaming.contains("return Ok(())"));
	assert!(!streaming.contains("render_explain_analyze"));
	assert!(!streaming.contains("materialize_system_table_plan"));
	assert!(local.contains("record_verdict"));
	assert!(local.contains("return Ok(results)"));
	assert!(!local.contains("render_explain_analyze"));
	assert!(!local.contains("materialize_system_table_plan"));
}

#[test]
pub(crate) fn adr188_measured_accrual_is_side_effect_only() {
	let binder = binder_root_source();
	let coordinator = source("src/coordinator.rs");
	let decision_log = source("src/binder/decision_log.rs");
	let join_order = source("src/binder/join_order.rs");
	let selectivity = source("src/binder/selectivity.rs");
	let pushdown = source("src/binder/pushdown.rs");
	let fragment = source("src/distributed/fragment.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");

	assert!(decision_log.contains("pub(crate) fn record_verdict"));
	assert!(decision_log.contains("pub(crate) fn verdict_snapshot"));
	assert!(coordinator.contains(".record_verdict("));
	assert!(bind_relation.contains("decision_log.verdict_snapshot()"));
	for (name, source) in [
		("join_order", join_order.as_str()),
		("selectivity", selectivity.as_str()),
		("pushdown", pushdown.as_str()),
		("fragment", fragment.as_str()),
	] {
		assert!(
			!source.contains("verdict_snapshot()"),
			"{name} reads runtime verdict accrual during planning"
		);
		assert!(
			!source.contains("record_verdict"),
			"{name} writes runtime verdict accrual outside execution"
		);
	}
}

#[test]
pub(crate) fn adr188_default_off_zero_overhead() {
	let binder = binder_root_source();
	let coordinator = source("src/coordinator.rs");
	let config = first_block(&binder, "pub struct PlannerConfig");
	let default = first_block(&binder, "impl Default for PlannerConfig");
	let gate = first_block(&coordinator, "fn should_measure_runtime_verdict");
	let streaming = first_block(&coordinator, "async fn run_streaming_read_driver");

	assert!(config.contains("measured_accrual_sample_rate: u32"));
	assert!(config.contains("measured_accrual_counter: Arc<AtomicU64>"));
	assert!(default.contains("measured_accrual_sample_rate: 0"));
	assert!(default.contains("measured_accrual_counter: Arc::new(AtomicU64::new(0))"));
	assert!(gate.contains("if rate == 0"));
	assert!(gate.contains("return false"));
	assert!(gate.contains("fetch_add(1, Ordering::Relaxed).saturating_add(1)"));
	assert!(streaming.contains("if measure_runtime_verdict"));
	assert!(streaming.contains("None => bind(&plan"));
}

#[test]
pub(crate) fn adr188_report_reuses_information_schema_path() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_RUNTIME_VERDICTS_TABLE"));
	assert!(execution.contains("InformationSchemaOptimizerRuntimeVerdicts"));
	assert!(bind_relation.contains("INFORMATION_SCHEMA_OPTIMIZER_RUNTIME_VERDICTS_TABLE"));
	assert!(bind_relation.contains("SystemTableKind::InformationSchemaOptimizerRuntimeVerdicts"));
	assert!(bind_relation.contains("system_table_plan("));
	assert!(bind_relation.contains("information_schema_optimizer_runtime_verdict_rows"));
}

#[test]
pub(crate) fn adr188_no_storage_or_wendelon_change() {
	let manifest = manifest_dir();
	let markers = [
		"OptimizerRuntimeVerdict",
		"InformationSchemaOptimizerRuntimeVerdicts",
		"optimizer_runtime_verdicts",
		"measured_accrual_sample_rate",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/distributed"),
		manifest.join("../wendelon/src"),
	] {
		offenders.extend(
			source_lines_for_path(&path)
				.into_iter()
				.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
		);
	}
	assert!(
		offenders.is_empty(),
		"ADR188 runtime verdict accrual leaked outside binder/coordinator/report surface:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr188_no_optimizer_shell_or_planproperties_contract() {
	let manifest = manifest_dir();
	assert!(!manifest.join("src/optimizer.rs").exists());
	assert!(!manifest.join("src/optimizer").exists());

	let offenders = source_lines(&manifest.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			let trimmed = line.trim_start();
			trimmed.starts_with("struct Optimizer ")
				|| trimmed.starts_with("pub struct Optimizer ")
				|| trimmed.starts_with("enum Optimizer ")
				|| trimmed.starts_with("pub enum Optimizer ")
				|| line.contains("PlanProperties")
				|| line.contains("Cascades")
				|| trimmed.starts_with("struct Memo ")
				|| trimmed.starts_with("pub struct Memo ")
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR188 introduced optimizer shell/property contract:\n{}",
		format_offenders(&manifest, &offenders)
	);
}
