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
	let decline = first_block(&join_order, "fn accrue_star_order_decline");
	let receipt = first_block(&join_order, "fn accrue_star_order_receipt");

	for block in [&decline, &receipt] {
		assert!(block.contains("decision_recorder"));
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
	assert!(try_actuate.contains("accrue_star_order_receipt("));
	assert!(try_actuate.contains("accrue_star_order_decline("));
	assert!(bind_plan.contains("StarOrderActuationResult::Declined"));
	// The declined fall-back re-binds on a binder carrying the zero-size no-op recorder, so the
	// receipt is not recorded twice — a fact about the recorder's type, not a runtime toggle.
	assert!(bind_plan.contains(".without_decision_recording()"));
	for block in [preflight, sequence, local, root_dimensions] {
		assert!(!block.contains("decision_log"));
		assert!(!block.contains("accrue_star_order"));
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

	// ADR 191: the join-order planning helpers still must not read verdict accrual. The only
	// sanctioned readers are the opt-in margin resolver and the read-only advisory report, which
	// run once at bind entry — never inside the actuation/observation decision path. Carve those
	// two helpers out before sweeping the file so the planning-path invariant stays exact.
	let resolver = first_block(
		&join_order,
		"pub(crate) fn effective_join_order_soft_margin_step",
	);
	let report = first_block(&join_order, "pub(crate) fn margin_advisory_report");
	assert!(report.contains("verdict_snapshot()"));
	let join_order_planning = join_order.replace(&resolver, "").replace(&report, "");

	for (name, source) in [
		("join_order", join_order_planning.as_str()),
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

#[test]
pub(crate) fn adr189_advisory_is_read_only() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let decision_log = source("src/binder/decision_log.rs");
	let recommend = first_block(&join_order, "pub(crate) fn recommend_soft_margin");
	let classify = first_block(&join_order, "pub(super) fn classify_star_order_action");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	// ADR 191: the advisory is invoked read-only through the report helper, not inline.
	let advisory_report = first_block(&join_order, "pub(crate) fn margin_advisory_report");

	assert!(recommend.contains("snapshot: &[OptimizerRuntimeVerdictRow]"));
	assert!(recommend.contains("current: SoftMarginLadderStep"));
	assert!(!recommend.contains("decision_log"));
	assert!(!recommend.contains(".record("));
	assert!(!recommend.contains("record_verdict"));
	assert!(bind_relation.contains("decision_log.verdict_snapshot()"));
	assert!(bind_relation.contains("margin_advisory_report("));
	assert!(advisory_report.contains("recommend_soft_margin("));
	assert!(!classify.contains("recommend_soft_margin"));
	assert!(!classify.contains("verdict_snapshot"));
	assert!(!decision_log.contains("recommend_soft_margin"));
	assert!(!decision_log.contains("MarginAdvisory"));
}

#[test]
pub(crate) fn adr189_advisory_does_not_change_margin_or_decision() {
	let join_order = source("src/binder/join_order.rs");
	let classify = first_block(&join_order, "pub(super) fn classify_star_order_action");

	// Retargeted for ADR 190: the margin is now a `SoftMargin` parameter, not a const read in the
	// classifier. The surviving 189 invariant is that the decision path never consults the advisory
	// or the accrued verdicts — the advisory remains read-only and one-directional.
	assert!(classify.contains("JoinOrderEstimateConfidence::Hard => StarOrderAction::Reordered"));
	assert!(!classify.contains("MarginAdvisory"));
	assert!(!classify.contains("recommend_soft_margin"));
	assert!(!classify.contains("OptimizerRuntimeVerdict"));
	assert!(!classify.contains("verdict_snapshot"));
}

#[test]
pub(crate) fn adr189_advisory_reuses_188_verdict_log() {
	let binder = binder_root_source();
	let decision_log = source("src/binder/decision_log.rs");
	let join_order = source("src/binder/join_order.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	let log_struct = first_block(&decision_log, "pub struct OptimizerDecisionLog");
	let with_max_keys = first_block(&decision_log, "pub(crate) fn with_max_keys");
	// ADR 191: the advisory report path is the indirection that reuses the 188 verdict log.
	let advisory_report = first_block(&join_order, "pub(crate) fn margin_advisory_report");

	assert!(decision_log.contains("pub(crate) fn verdict_snapshot"));
	assert!(join_order.contains("snapshot: &[OptimizerRuntimeVerdictRow]"));
	assert!(bind_relation.contains("decision_log.verdict_snapshot()"));
	assert!(bind_relation.contains("margin_advisory_report("));
	assert!(advisory_report.contains("recommend_soft_margin("));
	assert!(advisory_report.contains("decision_log.verdict_snapshot()"));
	assert!(
		log_struct.contains("verdict_counts: Mutex<BTreeMap<OptimizerRuntimeVerdictKey, u64>>")
	);
	assert!(with_max_keys.contains("verdict_counts: Mutex::new(BTreeMap::new())"));
	assert_eq!(
		decision_log.matches("pub(crate) fn record_verdict").count(),
		1
	);
}

#[test]
pub(crate) fn adr189_advisory_excludes_misranked_indeterminate() {
	let join_order = source("src/binder/join_order.rs");
	let recommend = first_block(&join_order, "pub(crate) fn recommend_soft_margin");
	let rows = source("src/binder/explain.rs");
	let advisory_rows = first_block(
		&rows,
		"pub(super) fn information_schema_optimizer_margin_advisory_rows",
	);

	assert!(recommend.contains("OptimizerRuntimeVerdictClass::MisRanked"));
	assert!(recommend.contains("mis_ranked = mis_ranked.saturating_add(row.count);"));
	assert!(recommend.contains("OptimizerRuntimeVerdictClass::Indeterminate"));
	assert!(recommend.contains("indeterminate = indeterminate.saturating_add(row.count);"));
	assert!(recommend.contains("let sample_size = missed_win"));
	assert!(recommend.contains(".saturating_add(over_eager)"));
	assert!(recommend.contains(".saturating_add(vindicated)"));
	assert!(!recommend.contains(".saturating_add(mis_ranked)"));
	assert!(!recommend.contains(".saturating_add(indeterminate)"));
	assert!(advisory_rows.contains("advisory.mis_ranked"));
	assert!(advisory_rows.contains("advisory.indeterminate"));
}

#[test]
pub(crate) fn adr189_report_reuses_information_schema_path() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let explain = source("src/binder/explain.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_MARGIN_ADVISORY_TABLE"));
	assert!(execution.contains("InformationSchemaOptimizerMarginAdvisory"));
	assert!(bind_relation.contains("INFORMATION_SCHEMA_OPTIMIZER_MARGIN_ADVISORY_TABLE"));
	assert!(bind_relation.contains("SystemTableKind::InformationSchemaOptimizerMarginAdvisory"));
	assert!(bind_relation.contains("system_table_plan("));
	assert!(bind_relation.contains("information_schema_optimizer_margin_advisory_schema"));
	assert!(bind_relation.contains("information_schema_optimizer_margin_advisory_rows"));
	assert!(explain.contains("information_schema_optimizer_margin_advisory_schema"));
}

#[test]
pub(crate) fn adr189_no_executor_fragmenter_storage_or_wendelon_change() {
	let manifest = manifest_dir();
	let markers = [
		"InformationSchemaOptimizerMarginAdvisory",
		"optimizer_margin_advisory",
		"MarginAdvisory",
		"recommend_soft_margin",
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
		"ADR189 margin advisory leaked outside binder/report surface:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr189_no_optimizer_shell_or_planproperties_contract() {
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
		"ADR189 introduced optimizer shell/property contract:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr190_margin_is_config_not_const() {
	let join_order = source("src/binder/join_order.rs");
	let classify = first_block(&join_order, "pub(super) fn classify_star_order_action");
	let rank = first_block(&join_order, "pub(super) fn rank_star_order");

	assert!(classify.contains("margin: SoftMargin"));
	assert!(classify.contains("margin.denominator"));
	assert!(classify.contains("margin.numerator"));
	assert!(!classify.contains("STAR_ORDER_SOFT_MARGIN"));
	assert!(rank.contains("margin: SoftMargin"));
}

#[test]
pub(crate) fn adr190_applied_margin_recorded_in_ranking_and_evidence() {
	let join_order = source("src/binder/join_order.rs");
	let explain = source("src/binder/explain.rs");
	let ranking = first_block(&join_order, "pub(super) struct StarOrderRanking");
	let evidence = first_block(&join_order, "pub(super) struct StarOrderEvidence");
	let rerank = first_block(&join_order, "pub(super) fn measured_order_rerank");

	assert!(ranking.contains("applied_margin: SoftMargin"));
	assert!(evidence.contains("applied_margin: SoftMargin"));
	assert!(rerank.contains("ranking.applied_margin"));
	assert!(explain.contains("evidence.applied_margin.label()"));
	assert!(explain.contains("ranking.applied_margin.label()"));
}

#[test]
pub(crate) fn adr190_observed_paths_receive_configured_margin() {
	let binder = binder_root_source();
	let bind_explain = first_block(&binder, "async fn bind_with_explain_trace");
	let default = first_block(&binder, "impl Default for PlannerConfig");

	// ADR 191: observed paths receive the resolved effective step captured once at bind entry, not
	// a fresh `config` read. Default Manual makes that resolved step equal to the configured step,
	// preserving the ADR 190 behavior.
	assert!(bind_explain.contains("let binder = Binder::new(catalog, config)"));
	assert!(
		bind_explain
			.contains("let join_order_soft_margin_step = binder.join_order_soft_margin_step")
	);
	// Structural, format-insensitive check: rustfmt may render either observed call inline or
	// multi-line (dropping/keeping a trailing comma), so match the argument structure with
	// whitespace removed rather than counting commas. Both observed calls must pass the resolved
	// step as their last argument.
	let compact: String = bind_explain
		.chars()
		.filter(|c| !c.is_whitespace())
		.collect();
	assert!(compact.contains(
		"analyze_snowflake(&render_trace.join_order_edges,&trace.join_decisions,join_order_soft_margin_step"
	));
	assert!(compact.contains("analyze(&render_trace.join_order_edges,join_order_soft_margin_step"));
	assert!(!bind_explain.contains("config.join_order_soft_margin_step"));
	assert!(
		default
			.contains("join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual")
	);
}

#[test]
pub(crate) fn adr190_advisory_current_is_configured_margin() {
	let join_order = source("src/binder/join_order.rs");
	let advisory_report = first_block(&join_order, "pub(crate) fn margin_advisory_report");

	// ADR 191: the advisory input is still the operator-configured base step, fed to the 189
	// policy by the report helper — never the already-applied step (that would double-step).
	assert!(advisory_report.contains("recommend_soft_margin("));
	assert!(advisory_report.contains("config.join_order_soft_margin_step,"));
}

#[test]
pub(crate) fn adr190_margin_bounded_to_ladder() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let soft_margin = first_block(&join_order, "pub(crate) struct SoftMargin");

	assert!(binder.contains("pub join_order_soft_margin_step: SoftMarginLadderStep"));
	// Raw ratio fields stay private; the operator selects a bounded ladder step only.
	assert!(!soft_margin.contains("pub numerator"));
	assert!(!soft_margin.contains("pub denominator"));
	assert!(!soft_margin.contains("pub(crate) numerator"));
}

#[test]
pub(crate) fn adr190_operator_applied_not_auto() {
	let manifest = manifest_dir();
	// The margin step is set only at config construction, never mutated and never written from
	// the accrual/verdict path.
	let assignments = source_lines(&manifest.join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains(".join_order_soft_margin_step ="))
		.collect::<Vec<_>>();
	assert!(
		assignments.is_empty(),
		"join_order_soft_margin_step must not be reassigned (operator-applied, not auto):\n{}",
		format_offenders(&manifest, &assignments)
	);
	assert!(!source("src/binder/decision_log.rs").contains("join_order_soft_margin_step"));
}

#[test]
pub(crate) fn adr190_no_executor_storage_or_wendelon_change() {
	let manifest = manifest_dir();
	let markers = ["join_order_soft_margin_step", "SoftMarginLadderStep"];
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
		"ADR190 margin config leaked outside binder surface:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr190_no_optimizer_shell_or_planproperties_contract() {
	adr189_no_optimizer_shell_or_planproperties_contract();
}

#[test]
pub(crate) fn adr191_margin_application_mode_is_typed() {
	let binder = binder_root_source();
	let config = first_block(&binder, "pub struct PlannerConfig");
	let default = first_block(&binder, "impl Default for PlannerConfig");

	assert!(binder.contains("pub enum JoinOrderSoftMarginApplication {"));
	assert!(
		config.contains("pub join_order_soft_margin_application: JoinOrderSoftMarginApplication")
	);
	assert!(
		default
			.contains("join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual")
	);
	// No bool auto-tune shape anywhere in the binder.
	assert!(!binder.contains("join_order_margin_auto_tune"));
}

#[test]
pub(crate) fn adr191_effective_margin_resolved_once_at_bind_entry() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let binder_struct = first_block(&binder, "struct Binder<'a>");
	let binder_new = first_block(
		&binder,
		"fn new(catalog: &'a dyn Catalog, config: &'a PlannerConfig) -> Self",
	);
	let analyze = first_block(&join_order, "pub(super) fn analyze");
	let analyze_snowflake = first_block(&join_order, "pub(super) fn analyze_snowflake");

	// The resolved step lives on the binder and is set once via the resolver at construction.
	assert!(binder_struct.contains("join_order_soft_margin_step: SoftMarginLadderStep"));
	assert!(binder_new.contains("effective_join_order_soft_margin_step(config)"));
	// Star actuation reads the resolved field, never a fresh config/log read.
	assert!(join_order.contains("self.join_order_soft_margin_step.margin()"));
	assert!(!join_order.contains("self.config.join_order_soft_margin_step"));
	// Low-level star/snowflake helpers never re-snapshot the verdict log mid-bind.
	assert!(!analyze.contains("verdict_snapshot"));
	assert!(!analyze_snowflake.contains("verdict_snapshot"));
}

#[test]
pub(crate) fn adr191_auto_apply_reuses_189_policy_only() {
	let join_order = source("src/binder/join_order.rs");
	let resolver = first_block(
		&join_order,
		"pub(crate) fn effective_join_order_soft_margin_step",
	);

	// The opt-in path returns exactly the 189 recommendation; it owns no policy of its own.
	assert!(resolver.contains("recommend_soft_margin("));
	assert!(resolver.contains(".recommended_step"));
	assert!(!resolver.contains("looser"));
	assert!(!resolver.contains("tighter"));
	assert!(!resolver.contains("for "));
	assert!(!resolver.contains("while "));
	assert!(!resolver.contains("MIN_SAMPLE"));
	assert!(!resolver.contains("HYSTERESIS"));
}

#[test]
pub(crate) fn adr191_report_preserves_configured_recommended_and_applied_margins() {
	let explain = source("src/binder/explain.rs");
	let join_order = source("src/binder/join_order.rs");
	let schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_margin_advisory_schema",
	);
	let report = first_block(&join_order, "pub(crate) fn margin_advisory_report");

	assert!(schema.contains("\"current_margin\""));
	assert!(schema.contains("\"recommended_margin\""));
	assert!(schema.contains("\"application_mode\""));
	assert!(schema.contains("\"applied_margin\""));
	// New columns are appended at the tail so existing indexes stay stable.
	let sample_idx = schema.find("\"sample_size\"").expect("sample_size column");
	let app_idx = schema
		.find("\"application_mode\"")
		.expect("application_mode column");
	let applied_idx = schema
		.find("\"applied_margin\"")
		.expect("applied_margin column");
	assert!(sample_idx < app_idx && app_idx < applied_idx);
	// The report still feeds the configured base step into the advisory.
	assert!(report.contains("config.join_order_soft_margin_step,"));
}

#[test]
pub(crate) fn adr191_retargets_190_operator_applied_not_auto() {
	let manifest = manifest_dir();
	let assignments = source_lines(&manifest.join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains(".join_order_soft_margin_step ="))
		.collect::<Vec<_>>();
	assert!(
		assignments.is_empty(),
		"join_order_soft_margin_step must never be reassigned; the only derived path is the opt-in resolver:\n{}",
		format_offenders(&manifest, &assignments)
	);
	let decision_log = source("src/binder/decision_log.rs");
	assert!(!decision_log.contains("join_order_soft_margin_step"));
	assert!(!decision_log.contains("join_order_soft_margin_application"));
}

#[test]
pub(crate) fn adr191_default_manual_preserves_190() {
	let binder = binder_root_source();
	let default = first_block(&binder, "impl Default for PlannerConfig");

	assert!(
		default
			.contains("join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual")
	);
	assert!(default.contains("join_order_soft_margin_step: CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP"));
}

#[test]
pub(crate) fn adr191_no_executor_fragmenter_storage_or_wendelon_change() {
	let manifest = manifest_dir();
	let markers = [
		"JoinOrderSoftMarginApplication",
		"effective_join_order_soft_margin_step",
		"applied_margin",
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
		"ADR191 opt-in auto-apply leaked outside the binder surface:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr191_no_optimizer_shell_or_planproperties_contract() {
	adr189_no_optimizer_shell_or_planproperties_contract();
}
