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

	assert!(bind_relation.contains("runtime_evidence.decision_snapshot()"));
	for (name, source) in [
		("join_order", join_order.as_str()),
		("selectivity", selectivity.as_str()),
		("pushdown", pushdown.as_str()),
		("fragment", fragment.as_str()),
	] {
		assert!(
			!source.contains("decision_snapshot()"),
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
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let recording_kinds = first_block(&decision_log, "pub(crate) fn decision_recording_kinds");
	let outcomes = first_block(&decision_log, "pub(crate) fn optimizer_decision_outcomes");
	let all_keys = first_block(&runtime_evidence, "pub(crate) fn all_decision_counter_keys");

	assert!(decision_log.contains("pub struct OptimizerDecisionLog;"));
	assert!(!decision_log.contains("DEFAULT_OPTIMIZER_DECISION_LOG_MAX_KEYS"));
	assert!(!decision_log.contains("overflow_count"));
	assert!(!decision_log.contains("max_keys"));
	assert_eq!(
		recording_kinds.matches("OptimizerDecisionKind::").count(),
		7
	);
	assert_eq!(outcomes.matches("OptimizerDecisionOutcome::").count(), 10);
	assert!(all_keys.contains("decision_recording_kinds().len()"));
	assert!(all_keys.contains("* optimizer_decision_outcomes().len()"));
	assert!(all_keys.contains("* runtime_evidence_lanes().len()"));
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
pub(crate) fn adr216_decision_telemetry_uses_existing_optimizer_decisions_table() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let explain = source("src/binder/explain.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	let schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_decisions_schema",
	);
	let rows = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_decision_rows",
	);

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_DECISIONS_TABLE"));
	assert!(execution.contains("InformationSchemaOptimizerDecisions"));
	assert!(!execution.contains("InformationSchemaOptimizerRuntimeDecisions"));
	assert!(bind_relation.contains("SystemTableKind::InformationSchemaOptimizerDecisions"));
	assert!(bind_relation.contains("runtime_evidence.decision_snapshot()"));
	assert!(schema.contains("\"decision_kind\""));
	assert!(schema.contains("\"outcome\""));
	assert!(schema.contains("\"decline_reason\""));
	assert!(schema.contains("\"count\""));
	assert!(schema.contains("\"provenance\""));
	assert!(rows.contains("optimizer_runtime_verdict_provenance_label(decision.provenance)"));
}

#[test]
pub(crate) fn adr216_decision_keys_are_structured_not_strings() {
	let decision_log = source("src/binder/decision_log.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let key = first_block(
		&decision_log,
		"pub(crate) struct OptimizerRuntimeDecisionKey",
	);
	let state = first_block(&runtime_evidence, "struct RuntimeEvidenceState");
	let counter_key = first_block(&runtime_evidence, "pub(crate) fn decision_counter_key");

	assert!(key.contains("kind: OptimizerDecisionKind"));
	assert!(key.contains("outcome: OptimizerDecisionOutcome"));
	assert!(key.contains("lane: OptimizerRuntimeEvidenceLane"));
	assert!(state.contains("decision_pending: BTreeMap<OptimizerRuntimeDecisionKey, u64>"));
	assert!(state.contains("decision_in_flight: BTreeMap<OptimizerRuntimeDecisionKey, u64>"));
	assert!(state.contains("decision_durable_cache: BTreeMap<OptimizerRuntimeDecisionKey, u64>"));
	assert!(counter_key.contains("optimizer_decision_kind_label(key.kind)"));
	assert!(counter_key.contains("decision_outcome_key_label(key.outcome)"));
	assert!(!state.contains("BTreeMap<String, u64>"));
}

#[test]
pub(crate) fn adr216_decision_matrix_is_140_keys_and_evidence_matrix_220() {
	let decision_log = source("src/binder/decision_log.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let recording_kinds = first_block(&decision_log, "pub(crate) fn decision_recording_kinds");
	let outcomes = first_block(&decision_log, "pub(crate) fn optimizer_decision_outcomes");
	let lanes = first_block(&runtime_evidence, "fn runtime_evidence_lanes");
	let all_decision = first_block(&runtime_evidence, "pub(crate) fn all_decision_counter_keys");
	let all_verdict = first_block(&runtime_evidence, "pub(crate) fn all_verdict_counter_keys");
	let tests = source("src/binder/runtime_evidence.rs");

	assert_eq!(
		recording_kinds.matches("OptimizerDecisionKind::").count(),
		7
	);
	for kind in [
		"StarCandidate",
		"FlatStar",
		"SnowflakeRoot",
		"SnowflakeBranch",
		"GalaxyRoot",
		"BridgeRoot",
		"BridgeChainRoot",
	] {
		assert!(
			recording_kinds.contains(kind),
			"decision recording matrix is missing {kind}"
		);
	}
	assert!(!recording_kinds.contains("PreservedOuterOrder"));
	assert!(!decision_log.contains("Overflow"));
	assert_eq!(outcomes.matches("OptimizerDecisionOutcome::").count(), 10);
	assert_eq!(lanes.matches("OptimizerRuntimeEvidenceLane::").count(), 2);
	assert!(all_decision.contains("decision_recording_kinds().len()"));
	assert!(all_decision.contains("* optimizer_decision_outcomes().len()"));
	assert!(all_decision.contains("* runtime_evidence_lanes().len()"));
	assert!(all_verdict.contains("runtime_evidence_decision_kinds().len()"));
	assert!(tests.contains("assert_eq!(decision_keys.len(), 140)"));
	assert!(tests.contains("assert_eq!(decision_keys.len() + verdict_keys.len(), 220)"));
}

#[test]
pub(crate) fn adr216_decision_flush_is_periodic_not_per_query() {
	let binder = binder_root_source();
	let coordinator = source("src/coordinator.rs");
	let daemon = source("src/daemon.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let flush = first_block(&runtime_evidence, "pub async fn flush_decision_deltas");
	let streaming = first_block(&coordinator, "async fn run_streaming_read_driver");
	let local = brace_blocks_after_marker(&coordinator, "pub async fn execute_query")
		.into_iter()
		.map(|(_, block)| block)
		.find(|block| block.contains("self.executor.execute_bound"))
		.expect("local QueryCoordinator execute_query block");

	assert!(daemon.contains("flush_decision_deltas().await"));
	assert!(daemon.contains("refresh_durable_cache().await"));
	assert!(flush.contains("counter_fetch_add"));
	assert_eq!(flush.matches("counter_fetch_add").count(), 1);
	for block in [&streaming, &local] {
		assert!(!block.contains("flush_decision_deltas"));
		assert!(!block.contains("record_decision"));
	}
	assert!(!binder.contains("flush_decision_deltas"));
}

#[test]
pub(crate) fn decision_record_is_latency_identical_no_per_query_flush() {
	adr216_decision_flush_is_periodic_not_per_query();
}

#[test]
pub(crate) fn adr216_decision_provenance_is_bind_entry_not_posthoc() {
	let binder = binder_root_source();
	let bind_with_trace = first_block(&binder, "pub(crate) async fn bind_with_trace");
	let bind_with_explain_trace = first_block(&binder, "async fn bind_with_explain_trace");
	let provenance = first_block(&binder, "fn decision_provenance_for_outer_join_demotions");
	let runtime_recorder = source("src/binder/runtime_evidence.rs");
	let recorder = first_block(&runtime_recorder, "impl OptimizerRuntimeDecisionRecorder");

	for block in [&bind_with_trace, &bind_with_explain_trace] {
		let demote = block
			.find("demote_null_rejected_outer_joins")
			.expect("bind entry demotes outer joins");
		let select = block
			.find("decision_provenance_for_outer_join_demotions(&outer_join_demotions)")
			.expect("bind entry selects decision provenance from demotion receipts");
		let recorder = block
			.find("with_decision_provenance(decision_provenance)")
			.expect("bind entry installs provenance-bearing recorder");
		let bind = block
			.find("bind_plan_with_trace")
			.expect("binds normalized plan");
		assert!(demote < select);
		assert!(select < recorder);
		assert!(recorder < bind);
	}
	assert!(provenance.contains("outer_join_demotions.is_empty()"));
	assert!(provenance.contains("OptimizerRuntimeVerdictProvenance::Native"));
	assert!(provenance.contains("OptimizerRuntimeVerdictProvenance::Demoted"));
	assert!(recorder.contains("provenance: OptimizerRuntimeVerdictProvenance"));
	assert!(!recorder.contains("render_trace"));
}

#[test]
pub(crate) fn adr216_noop_recorder_still_gates_scratch_decisions() {
	let binder = binder_root_source();
	let decision_log = source("src/binder/decision_log.rs");
	let without = first_block(&binder, "fn without_decision_recording");

	assert!(decision_log.contains("trait OptimizerDecisionRecorder"));
	assert!(decision_log.contains("struct NoOpDecisionRecorder"));
	assert!(without.contains("Arc::new(NoOpDecisionRecorder)"));
	assert!(binder.contains(".without_decision_recording()"));
	assert!(!binder.contains("record_optimizer_decisions"));
}

#[test]
pub(crate) fn adr216_decision_telemetry_is_read_only_no_policy_consumers() {
	let manifest = manifest_dir();
	let allowed = [
		manifest.join("src").join("binder.rs"),
		manifest.join("src/binder/runtime_evidence.rs"),
		manifest.join("src/binder/tests.rs"),
	];
	let offenders = source_lines(&manifest.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			line.contains("decision_snapshot()")
				&& !allowed
					.iter()
					.any(|allowed_path| path.as_path() == allowed_path.as_path())
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"decision telemetry gained a policy/report consumer outside information_schema:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr216_no_executor_fragmenter_storage_wire_or_wendelon_surface() {
	let manifest = manifest_dir();
	let markers = [
		"OptimizerRuntimeDecisionKey",
		"flush_decision_deltas",
		"optimizer_evidence/decision",
		"decision_snapshot",
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
		"ADR216 decision telemetry leaked outside binder/daemon/test surfaces:\n{}",
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
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let join_order = source("src/binder/join_order.rs");
	let selectivity = source("src/binder/selectivity.rs");
	let pushdown = source("src/binder/pushdown.rs");
	let fragment = source("src/distributed/fragment.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");

	assert!(!decision_log.contains("pub(crate) fn record_verdict"));
	assert!(!decision_log.contains("pub(crate) fn verdict_snapshot"));
	assert!(runtime_evidence.contains("pub(crate) fn record_verdict"));
	assert!(runtime_evidence.contains("pub(crate) fn verdict_snapshot"));
	assert!(coordinator.contains(".record_verdict("));
	assert!(bind_relation.contains("runtime_evidence.verdict_snapshot()"));

	// ADRs 191/215: the join-order planning helpers still must not read verdict accrual. The only
	// sanctioned readers are the opt-in shared margin resolver and the read-only advisory report,
	// which run once at bind entry — never inside the actuation/observation decision path. Carve
	// those helpers out before sweeping the file so the planning-path invariant stays exact.
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let report = first_block(&join_order, "pub(crate) fn margin_advisory_report");
	let report_rows = first_block(&join_order, "pub(crate) fn margin_advisory_report_rows");
	assert_eq!(resolver.matches("receipt_gate_snapshot()").count(), 1);
	assert!(!resolver.contains("verdict_snapshot()"));
	assert!(report.contains("verdict_snapshot()"));
	assert!(report_rows.contains("verdict_snapshot()"));
	let join_order_planning = join_order
		.replace(&resolver, "")
		.replace(&report, "")
		.replace(&report_rows, "");

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
	let advisory_report = first_block(&join_order, "pub(crate) fn margin_advisory_report_rows");
	let advisory_row = first_block(&join_order, "fn margin_advisory_report_for_scope");

	assert!(recommend.contains("snapshot: &[OptimizerRuntimeVerdictRow]"));
	assert!(recommend.contains("current: SoftMarginLadderStep"));
	assert!(!recommend.contains("decision_log"));
	assert!(!recommend.contains(".record("));
	assert!(!recommend.contains("record_verdict"));
	assert!(bind_relation.contains("runtime_evidence.verdict_snapshot()"));
	assert!(bind_relation.contains("margin_advisory_report_rows("));
	assert!(advisory_report.contains("margin_advisory_report_for_scope(&snapshot"));
	assert!(advisory_row.contains("recommend_soft_margin("));
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
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let join_order = source("src/binder/join_order.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	let evidence_struct = first_block(&runtime_evidence, "pub struct OptimizerRuntimeEvidence");
	let evidence_state = first_block(&runtime_evidence, "struct RuntimeEvidenceState");
	// ADR 191: the advisory report path is the indirection that reuses the 188 verdict log.
	let advisory_report = first_block(&join_order, "pub(crate) fn margin_advisory_report_rows");
	let advisory_row = first_block(&join_order, "fn margin_advisory_report_for_scope");

	assert!(!decision_log.contains("pub(crate) fn verdict_snapshot"));
	assert!(runtime_evidence.contains("pub(crate) fn verdict_snapshot"));
	assert!(join_order.contains("snapshot: &[OptimizerRuntimeVerdictRow]"));
	assert!(bind_relation.contains("runtime_evidence.verdict_snapshot()"));
	assert!(bind_relation.contains("margin_advisory_report_rows("));
	assert!(advisory_report.contains("margin_advisory_report_for_scope(&snapshot"));
	assert!(advisory_row.contains("recommend_soft_margin("));
	assert!(advisory_report.contains("runtime_evidence.verdict_snapshot()"));
	assert!(evidence_struct.contains("state: Mutex<RuntimeEvidenceState>"));
	assert!(evidence_state.contains("pending: BTreeMap<OptimizerRuntimeVerdictKey, u64>"));
	assert!(evidence_state.contains("durable_cache: BTreeMap<OptimizerRuntimeVerdictKey, u64>"));
	assert_eq!(
		runtime_evidence
			.matches("pub(crate) fn record_verdict")
			.count(),
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

	// ADR 208: observed tree paths receive the resolved tree step captured once at bind entry, not
	// a fresh `config` read. Default Manual makes that resolved step equal to the configured step,
	// preserving the ADR 190 behavior.
	assert!(bind_explain.contains("let binder = Binder::new(catalog, config)"));
	assert!(
		bind_explain
			.contains("let tree_join_order_soft_margin_step = binder.join_order_policy.tree_step")
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
		"analyze_snowflake(&render_trace.join_order_edges,&trace.join_decisions,tree_join_order_soft_margin_step"
	));
	assert!(
		compact.contains("analyze(&render_trace.join_order_edges,tree_join_order_soft_margin_step")
	);
	assert!(!bind_explain.contains("config.join_order_soft_margin_step"));
	assert!(
		default
			.contains("join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual")
	);
}

#[test]
pub(crate) fn adr190_advisory_current_is_configured_margin() {
	let join_order = source("src/binder/join_order.rs");
	let advisory_report = first_block(&join_order, "fn margin_advisory_report_for_scope");

	// ADR 191: the advisory input is still the operator-configured base step, fed to the 189
	// policy by the report helper — never the already-applied step (that would double-step).
	assert!(advisory_report.contains("recommend_soft_margin("));
	assert!(advisory_report.contains("config.join_order_soft_margin_step"));
	assert!(advisory_report.contains("current_step"));
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
	let binder_new = first_block(&binder, "fn new_with_decision_provenance");
	let analyze = first_block(&join_order, "pub(super) fn analyze");
	let analyze_snowflake = first_block(&join_order, "pub(super) fn analyze_snowflake");

	// The resolved policy lives on the binder and is set once via the resolver at construction.
	assert!(binder_struct.contains("join_order_policy: EffectiveJoinOrderPolicy"));
	assert!(binder_new.contains("effective_join_order_policy(config)"));
	assert_eq!(
		binder_new
			.matches("effective_join_order_policy(config)")
			.count(),
		1
	);
	// Star actuation reads the resolved tree field, never a fresh config/log read.
	assert!(join_order.contains("self.join_order_policy.tree_step.margin()"));
	assert!(!join_order.contains("self.config.join_order_soft_margin_step"));
	// Low-level star/snowflake helpers never re-snapshot the verdict log mid-bind.
	assert!(!analyze.contains("verdict_snapshot"));
	assert!(!analyze_snowflake.contains("verdict_snapshot"));
}

#[test]
pub(crate) fn adr191_auto_apply_reuses_189_policy_only() {
	let join_order = source("src/binder/join_order.rs");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");

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
	assert!(schema.contains("\"evidence_scope\""));
	assert!(schema.contains("\"excluded_sample_size\""));
	assert!(schema.contains("\"row_kind\""));
	// New columns are appended at the tail so existing indexes stay stable.
	let sample_idx = schema.find("\"sample_size\"").expect("sample_size column");
	let app_idx = schema
		.find("\"application_mode\"")
		.expect("application_mode column");
	let applied_idx = schema
		.find("\"applied_margin\"")
		.expect("applied_margin column");
	let scope_idx = schema
		.find("\"evidence_scope\"")
		.expect("evidence_scope column");
	let excluded_idx = schema
		.find("\"excluded_sample_size\"")
		.expect("excluded_sample_size column");
	let row_kind_idx = schema.find("\"row_kind\"").expect("row_kind column");
	assert!(sample_idx < app_idx && app_idx < applied_idx && applied_idx < scope_idx);
	assert!(scope_idx < excluded_idx && excluded_idx < row_kind_idx);
	// The report still feeds the configured base step into the advisory.
	assert!(report.contains("MarginAdvisoryScope::TreeFamily"));
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
		"effective_tree_join_order_soft_margin_step",
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

#[test]
pub(crate) fn adr208_soft_margin_advisory_filters_to_tree_verdict_kinds() {
	let join_order = source("src/binder/join_order.rs");
	let predicate = first_block(&join_order, "pub(crate) fn is_tree_margin_advisory_kind");
	let input = first_block(&join_order, "fn margin_advisory_input");
	let sampled_verdict = first_block(&join_order, "fn is_margin_advisory_sampled_verdict");
	let tree_input = first_block(&join_order, "fn tree_margin_advisory_input");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let report = first_block(&join_order, "pub(crate) fn margin_advisory_report");

	for kind in [
		"StarCandidate",
		"FlatStar",
		"SnowflakeRoot",
		"SnowflakeBranch",
		"GalaxyRoot",
		"BridgeRoot",
		"BridgeChainRoot",
	] {
		assert!(
			predicate.contains(kind),
			"scope predicate must classify {kind}"
		);
	}
	assert!(input.contains("scope.includes(row.kind)"));
	assert!(input.contains("excluded_sample_size"));
	assert!(input.contains("is_margin_advisory_sampled_verdict(row.verdict)"));
	assert!(sampled_verdict.contains("OptimizerRuntimeVerdictClass::MissedWin"));
	assert!(sampled_verdict.contains("OptimizerRuntimeVerdictClass::OverEager"));
	assert!(sampled_verdict.contains("OptimizerRuntimeVerdictClass::Vindicated"));
	assert!(tree_input.contains("MarginAdvisoryScope::TreeFamily"));
	assert!(resolver.contains("tree_margin_advisory_input"));
	assert!(resolver.contains("recommend_soft_margin("));
	assert!(resolver.contains("&tree_input.rows"));
	assert!(report.contains("MarginAdvisoryScope::TreeFamily"));
}

#[test]
pub(crate) fn adr208_tree_and_graph_margin_steps_are_separate() {
	let binder = binder_root_source();
	let binder_struct = first_block(&binder, "struct Binder<'a>");
	let binder_new = first_block(&binder, "fn new_with_decision_provenance");
	let no_recording = first_block(&binder, "fn without_decision_recording");

	assert!(binder_struct.contains("join_order_policy: EffectiveJoinOrderPolicy"));
	assert!(binder_new.contains("effective_join_order_policy(config)"));
	assert!(binder_new.contains("join_order_policy"));
	assert!(no_recording.contains("join_order_policy: self.join_order_policy"));
}

#[test]
pub(crate) fn adr208_graph_margin_gate_reads_separate_graph_step_under_apply_advisory() {
	let join_order = source("src/binder/join_order.rs");
	let actuation = first_block(&join_order, "pub(super) async fn try_bind_graph_actuation");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");

	assert!(resolver.contains("graph_steps:"));
	assert!(resolver.contains("graph_margin_advisory_input"));
	assert!(resolver.contains("graph_kind_steps_from_gate"));
	assert!(actuation.contains("join_order_policy"));
	assert!(actuation.contains("graph_steps"));
	assert!(actuation.contains("step_for(shape.decision_kind())"));
	assert!(!actuation.contains("self.join_order_policy.tree_step.margin()"));
	assert!(actuation.contains("GraphActuationResult::KeptThinMargin"));
}

#[test]
pub(crate) fn adr208_runtime_verdict_report_remains_complete() {
	let binder = binder_root_source();
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	let kinds = first_block(&runtime_evidence, "fn runtime_evidence_decision_kinds");

	for kind in [
		"StarCandidate",
		"FlatStar",
		"SnowflakeRoot",
		"SnowflakeBranch",
		"GalaxyRoot",
		"BridgeRoot",
		"BridgeChainRoot",
		"PreservedOuterOrder",
	] {
		assert!(
			kinds.contains(kind),
			"runtime verdict report must retain {kind}"
		);
	}
	assert!(bind_relation.contains("INFORMATION_SCHEMA_OPTIMIZER_RUNTIME_VERDICTS_TABLE"));
	assert!(bind_relation.contains("runtime_evidence.verdict_snapshot()"));
	assert!(bind_relation.contains("information_schema_optimizer_runtime_verdict_rows"));
	let explain = source("src/binder/explain.rs");
	let schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_runtime_verdicts_schema",
	);
	assert!(schema.contains("\"provenance\""));
}

#[test]
pub(crate) fn adr208_margin_advisory_report_appends_scope_columns() {
	let explain = source("src/binder/explain.rs");
	let join_order = source("src/binder/join_order.rs");
	let schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_margin_advisory_schema",
	);
	let rows = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_margin_advisory_rows",
	);
	let report_rows = first_block(&join_order, "pub(crate) fn margin_advisory_report_rows");

	let applied_idx = schema
		.find("\"applied_margin\"")
		.expect("applied_margin column");
	let scope_idx = schema
		.find("\"evidence_scope\"")
		.expect("evidence_scope column");
	let excluded_idx = schema
		.find("\"excluded_sample_size\"")
		.expect("excluded_sample_size column");
	let row_kind_idx = schema.find("\"row_kind\"").expect("row_kind column");
	assert!(applied_idx < scope_idx && scope_idx < excluded_idx && excluded_idx < row_kind_idx);
	assert!(rows.contains("report.evidence_scope"));
	assert!(rows.contains("report.excluded_sample_size"));
	assert!(rows.contains("report.row_kind"));
	assert!(report_rows.contains("MarginAdvisoryScope::TreeFamily"));
	assert!(report_rows.contains("MarginAdvisoryScope::GraphFamily"));
}

#[test]
pub(crate) fn adr208_apply_advisory_remains_resolve_once_no_storage_await() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let binder_new = first_block(&binder, "fn new_with_decision_provenance");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let report = first_block(&join_order, "pub(crate) fn margin_advisory_report");

	assert_eq!(
		binder_new
			.matches("effective_join_order_policy(config)")
			.count(),
		1
	);
	assert!(resolver.contains("runtime_evidence.receipt_gate_snapshot()"));
	assert!(report.contains("runtime_evidence.verdict_snapshot()"));
	for block in [&resolver, &report] {
		assert!(!block.contains(".await"));
		assert!(!block.contains("counter_read"));
		assert!(!block.contains("counter_fetch_add"));
		assert!(!block.contains("refresh_durable_cache"));
	}
}

#[test]
pub(crate) fn adr213_graph_advisory_scope_accepts_only_graph_kinds() {
	let join_order = source("src/binder/join_order.rs");
	let tree_predicate = first_block(&join_order, "pub(crate) fn is_tree_margin_advisory_kind");
	let graph_predicate = first_block(&join_order, "pub(crate) fn is_graph_margin_advisory_kind");
	let scope_impl = first_block(&join_order, "impl MarginAdvisoryScope");

	for kind in [
		"GalaxyRoot",
		"BridgeRoot",
		"BridgeChainRoot",
		"StarCandidate",
		"FlatStar",
		"SnowflakeRoot",
		"SnowflakeBranch",
		"PreservedOuterOrder",
	] {
		assert!(
			graph_predicate.contains(kind),
			"graph predicate must classify {kind}"
		);
		assert!(
			tree_predicate.contains(kind),
			"tree predicate must classify {kind}"
		);
	}
	assert!(scope_impl.contains("Self::TreeFamily => is_tree_margin_advisory_kind(kind)"));
	assert!(scope_impl.contains("Self::GraphFamily => is_graph_margin_advisory_kind(kind)"));
	assert!(scope_impl.contains("Self::GraphKind(scope_kind) => kind == scope_kind"));
	assert!(scope_impl.contains("Self::GraphFamily => \"graph_join_order\""));
	assert!(scope_impl.contains("optimizer_decision_kind_label(kind)"));
}

#[test]
pub(crate) fn adr213_margin_advisory_reads_evidence_snapshot_once() {
	let join_order = source("src/binder/join_order.rs");
	let report_rows = first_block(&join_order, "pub(crate) fn margin_advisory_report_rows");
	let row_helper = first_block(&join_order, "fn margin_advisory_report_for_scope");
	let input_helper = first_block(&join_order, "fn margin_advisory_input");

	assert_eq!(
		report_rows.matches("verdict_snapshot()").count(),
		1,
		"scoped margin advisory rows must share one evidence snapshot"
	);
	assert!(report_rows.contains("let snapshot = config.runtime_evidence.verdict_snapshot();"));
	assert!(report_rows.contains("margin_advisory_report_for_scope(&snapshot"));
	assert!(!row_helper.contains("verdict_snapshot"));
	assert!(!input_helper.contains("verdict_snapshot"));
}

#[test]
pub(crate) fn adr213_margin_advisory_report_is_scope_keyed_knob_and_detail_rows() {
	let explain = source("src/binder/explain.rs");
	let join_order = source("src/binder/join_order.rs");
	let schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_margin_advisory_schema",
	);
	let rows = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_margin_advisory_rows",
	);
	let report_struct = first_block(&join_order, "pub(crate) struct MarginAdvisoryReport");
	let report_rows = first_block(&join_order, "pub(crate) fn margin_advisory_report_rows");
	let scope_impl = first_block(&join_order, "impl MarginAdvisoryScope");

	let excluded_idx = schema
		.find("\"excluded_sample_size\"")
		.expect("excluded_sample_size column");
	let row_kind_idx = schema.find("\"row_kind\"").expect("row_kind column");
	assert!(
		excluded_idx < row_kind_idx,
		"row_kind must be appended at the tail"
	);

	for label in [
		"tree_join_order",
		"graph_join_order",
		"galaxy_root",
		"bridge_root",
		"bridge_chain_root",
		"knob_advisory",
		"kind_detail",
	] {
		assert!(
			join_order.contains(label) || rows.contains(label),
			"missing advisory report label {label}"
		);
	}
	assert!(report_struct.contains("row_kind: MarginAdvisoryRowKind"));
	assert!(rows.contains("margin_advisory_row_kind_label(report.row_kind)"));
	assert!(scope_impl.contains("Self::TreeFamily | Self::GraphFamily"));
	assert!(scope_impl.contains("Self::GraphKind(_) => MarginAdvisoryRowKind::KindDetail"));

	let tree_idx = report_rows
		.find("MarginAdvisoryScope::TreeFamily")
		.expect("tree row scope");
	let graph_idx = report_rows
		.find("MarginAdvisoryScope::GraphFamily")
		.expect("graph row scope");
	let galaxy_idx = report_rows
		.find("OptimizerDecisionKind::GalaxyRoot")
		.expect("galaxy detail row");
	let bridge_idx = report_rows
		.find("OptimizerDecisionKind::BridgeRoot")
		.expect("bridge detail row");
	let chain_idx = report_rows
		.find("OptimizerDecisionKind::BridgeChainRoot")
		.expect("bridge-chain detail row");
	assert!(tree_idx < graph_idx && graph_idx < galaxy_idx);
	assert!(galaxy_idx < bridge_idx && bridge_idx < chain_idx);
}

#[test]
pub(crate) fn adr213_graph_advisory_reuses_family_agnostic_margin_math() {
	let join_order = source("src/binder/join_order.rs");
	let recommend = first_block(&join_order, "pub(crate) fn recommend_soft_margin");
	let input = first_block(&join_order, "fn margin_advisory_input");
	let row_helper = first_block(&join_order, "fn margin_advisory_report_for_scope");
	let graph_predicate = first_block(&join_order, "pub(crate) fn is_graph_margin_advisory_kind");

	assert!(recommend.contains("snapshot: &[OptimizerRuntimeVerdictRow]"));
	assert!(!recommend.contains("MarginAdvisoryScope"));
	assert!(!recommend.contains("is_tree_margin_advisory_kind"));
	assert!(!recommend.contains("is_graph_margin_advisory_kind"));
	assert!(input.contains("scope.includes(row.kind)"));
	assert!(row_helper.contains("recommend_soft_margin(&input.rows, current_step)"));
	assert!(!graph_predicate.contains("recommend_soft_margin"));
}

#[test]
pub(crate) fn adr213_graph_readout_drives_graph_gate_only_through_apply_advisory() {
	let join_order = source("src/binder/join_order.rs");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let actuation = first_block(&join_order, "pub(super) async fn try_bind_graph_actuation");
	let row_helper = first_block(&join_order, "fn margin_advisory_report_for_scope");

	assert!(resolver.contains("JoinOrderSoftMarginApplication::Manual"));
	assert!(resolver.contains("JoinOrderSoftMarginApplication::ApplyAdvisory"));
	assert!(resolver.contains("let snapshot = config.runtime_evidence.receipt_gate_snapshot();"));
	assert!(resolver.contains("graph_margin_advisory_input(&snapshot.verdicts)"));
	assert!(resolver.contains("recommend_soft_margin("));
	assert!(resolver.contains("&graph_input.rows"));
	assert!(actuation.contains("join_order_policy"));
	assert!(actuation.contains("graph_steps"));
	assert!(actuation.contains("step_for(shape.decision_kind())"));
	assert!(!actuation.contains("recommend_soft_margin"));
	assert!(!actuation.contains("margin_advisory"));
	assert!(row_helper.contains("scope.is_actionable_knob()"));
}

#[test]
pub(crate) fn adr213_runtime_verdict_report_remains_complete() {
	adr208_runtime_verdict_report_remains_complete();

	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let keys = first_block(&runtime_evidence, "fn runtime_evidence_decision_kinds");
	let all_keys = first_block(&runtime_evidence, "pub(crate) fn all_verdict_counter_keys");
	assert_eq!(keys.matches("OptimizerDecisionKind::").count(), 8);
	assert!(all_keys.contains("runtime_evidence_decision_kinds().len()"));
	assert!(all_keys.contains("* verdict_classes().len()"));
	assert!(all_keys.contains("* runtime_evidence_lanes().len()"));
}

#[test]
pub(crate) fn adr213_no_executor_fragmenter_storage_wire_wendelon_or_outer_join_surface() {
	let manifest = manifest_dir();
	let markers = [
		"MarginAdvisoryScope",
		"MarginAdvisoryRowKind",
		"margin_advisory_report_rows",
		"optimizer_margin_advisory",
		"graph_join_order",
		"row_kind",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/distributed"),
		manifest.join("src/binder/outer_join_demotion.rs"),
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
		"ADR213 graph margin advisory readout leaked outside binder/reporting surfaces:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr214_outer_kind_vocabulary_is_exactly_preserved_outer_order() {
	let decision_log = source("src/binder/decision_log.rs");
	let kinds = first_block(&decision_log, "pub(crate) enum OptimizerDecisionKind");
	let labels = first_block(&decision_log, "pub(crate) fn optimizer_decision_kind_label");

	assert!(kinds.contains("PreservedOuterOrder"));
	assert!(labels.contains("PreservedOuterOrder => \"preserved_outer_order\""));
	for forbidden in ["DemotedOuter", "OuterStar", "OuterJoinOrder"] {
		assert!(
			!decision_log.contains(forbidden),
			"ADR214 admits one outer-ordering kind; found {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr214_provenance_is_dual_write_totals_unchanged() {
	let decision_log = source("src/binder/decision_log.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let explain = source("src/binder/explain.rs");
	let key = first_block(
		&decision_log,
		"pub(crate) struct OptimizerRuntimeVerdictKey",
	);
	let row = first_block(
		&decision_log,
		"pub(crate) struct OptimizerRuntimeVerdictRow",
	);
	let record = first_block(&runtime_evidence, "pub(crate) fn record_verdict");
	let codec = first_block(&runtime_evidence, "pub(crate) fn verdict_counter_key");
	let snapshot = first_block(&runtime_evidence, "pub(crate) fn verdict_snapshot");
	let snapshot_rows = first_block(&runtime_evidence, "fn verdict_rows_from_state");
	let schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_runtime_verdicts_schema",
	);

	assert!(decision_log.contains("enum OptimizerRuntimeVerdictProvenance"));
	assert!(decision_log.contains("enum OptimizerRuntimeEvidenceLane"));
	assert!(key.contains("lane: OptimizerRuntimeEvidenceLane"));
	assert!(row.contains("provenance: OptimizerRuntimeVerdictProvenance"));
	assert!(record.contains("lane: OptimizerRuntimeEvidenceLane::Total"));
	assert!(record.contains("if provenance == OptimizerRuntimeVerdictProvenance::Demoted"));
	assert!(record.contains("lane: OptimizerRuntimeEvidenceLane::Demoted"));
	assert!(codec.contains("OptimizerRuntimeEvidenceLane::Total"));
	assert!(codec.contains("\"{}/{}\""));
	assert!(codec.contains("OptimizerRuntimeEvidenceLane::Demoted"));
	assert!(codec.contains("\"{}/demoted/{}\""));
	assert!(snapshot.contains("verdict_rows_from_state(&state)"));
	assert!(snapshot_rows.contains("let native = total.saturating_sub(demoted)"));
	assert!(snapshot_rows.contains("OptimizerRuntimeVerdictProvenance::Native"));
	assert!(snapshot_rows.contains("OptimizerRuntimeVerdictProvenance::Demoted"));
	assert!(schema.contains("\"provenance\""));
}

#[test]
pub(crate) fn adr214_counterfactual_uses_noop_recorder_scratch_bind() {
	let binder = binder_root_source();
	let explain = source("src/binder/explain.rs");
	let trace = first_block(&explain, "pub(super) struct OptimizerExplainRenderTrace");
	let helper = first_block(&binder, "fn preserved_outer_order_receipt");
	let collect = first_block(&binder, "fn star_order_runtime_replay_ids");

	// ADR214 uses the allowed contained edge-vector helper: cloned trace edges answer the
	// inner-equivalent question and map back to real executed join targets; no shared recorder is touched.
	assert!(trace.contains("preserved_outer_order: Vec<PreservedOuterOrderReceipt>"));
	assert!(helper.contains("let mut counterfactual_edges = edges.to_vec()"));
	assert!(helper.contains("is_preserved_outer_equi_edge"));
	assert!(helper.contains("edge.inner_equi = true"));
	assert!(helper.contains("edge.probe_scan_linear = true"));
	assert!(helper.contains("analyze(&counterfactual_edges"));
	assert!(!helper.contains(".record("));
	assert!(!helper.contains("decision_recorder"));
	assert!(!helper.contains("bind_plan"));
	assert!(collect.contains("render_trace.preserved_outer_order"));
	assert!(collect.contains("receipt.receipt.edge_targets"));
}

#[test]
pub(crate) fn adr214_outer_verdicts_are_opportunity_classes() {
	let explain = source("src/binder/explain.rs");
	let verdicts = first_block(&explain, "fn measured_verdicts_from_summaries");
	let outer = first_block(&explain, "fn runtime_verdict_class_from_preserved_outer");

	assert!(verdicts.contains("OptimizerDecisionKind::PreservedOuterOrder"));
	assert!(verdicts.contains("runtime_verdict_class_from_preserved_outer"));
	assert!(outer.contains("StarOrderVerdict::MissedWin | StarOrderVerdict::MisRanked"));
	assert!(outer.contains("OptimizerRuntimeVerdictClass::MissedWin"));
	assert!(outer.contains("StarOrderVerdict::Vindicated | StarOrderVerdict::OverEager"));
	assert!(outer.contains("OptimizerRuntimeVerdictClass::Vindicated"));
	assert!(outer.contains("StarOrderVerdict::Indeterminate"));
	assert!(outer.contains("OptimizerRuntimeVerdictClass::Indeterminate"));
}

#[test]
pub(crate) fn adr214_runtime_evidence_matrix_is_80_keys_enumerable() {
	adr207_runtime_evidence_is_extracted_fixed_matrix();

	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let kinds = first_block(&runtime_evidence, "fn runtime_evidence_decision_kinds");
	let lanes = first_block(&runtime_evidence, "fn runtime_evidence_lanes");
	let all_keys = first_block(&runtime_evidence, "pub(crate) fn all_verdict_counter_keys");

	assert_eq!(kinds.matches("OptimizerDecisionKind::").count(), 8);
	assert_eq!(lanes.matches("OptimizerRuntimeEvidenceLane::").count(), 2);
	assert!(all_keys.contains("* runtime_evidence_lanes().len()"));
}

#[test]
pub(crate) fn adr214_outer_family_has_no_advisory_knob() {
	let join_order = source("src/binder/join_order.rs");
	let tree_predicate = first_block(&join_order, "pub(crate) fn is_tree_margin_advisory_kind");
	let graph_predicate = first_block(&join_order, "pub(crate) fn is_graph_margin_advisory_kind");
	let input = first_block(&join_order, "fn margin_advisory_input");
	let report_rows = first_block(&join_order, "pub(crate) fn margin_advisory_report_rows");

	assert!(tree_predicate.contains("PreservedOuterOrder"));
	assert!(graph_predicate.contains("PreservedOuterOrder"));
	assert!(input.contains("if scope.includes(row.kind)"));
	assert!(input.contains("excluded_sample_size"));
	assert!(report_rows.contains("MarginAdvisoryScope::TreeFamily"));
	assert!(report_rows.contains("MarginAdvisoryScope::GraphFamily"));
	assert!(!report_rows.contains("PreservedOuterOrder"));
}

#[test]
pub(crate) fn adr214_no_executor_fragmenter_storage_wire_or_wendelon_change() {
	let manifest = manifest_dir();
	let markers = [
		"PreservedOuterOrder",
		"OptimizerRuntimeVerdictProvenance",
		"OptimizerRuntimeEvidenceLane",
		"preserved_outer_order",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/wire.rs"),
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
		"ADR214 outer-ordering evidence leaked outside binder/reporting/coordinator surfaces:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr215_graph_application_shares_apply_advisory_mode_no_new_mode() {
	let binder = binder_root_source();
	let config = first_block(&binder, "pub struct PlannerConfig");
	let application = first_block(&binder, "pub enum JoinOrderSoftMarginApplication");
	let default = first_block(&binder, "impl Default for PlannerConfig");

	assert!(application.contains("Manual"));
	assert!(application.contains("ApplyAdvisory"));
	assert_eq!(application.matches("ApplyAdvisory").count(), 1);
	assert!(config.contains("join_order_soft_margin_application: JoinOrderSoftMarginApplication"));
	assert!(!config.contains("graph_join_order_soft_margin_application"));
	assert!(!config.contains("graph_join_order_soft_margin_step"));
	assert!(default.contains("JoinOrderSoftMarginApplication::Manual"));
	assert!(!binder.contains("GraphJoinOrderSoftMarginApplication"));
	assert!(!binder.contains("graph_margin_auto"));
}

#[test]
pub(crate) fn adr215_bind_margin_steps_share_one_snapshot() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let binder_new = first_block(&binder, "fn new_with_decision_provenance");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");

	assert_eq!(
		binder_new
			.matches("effective_join_order_policy(config)")
			.count(),
		1
	);
	assert!(!binder_new.contains("effective_tree_join_order_soft_margin_step(config)"));
	assert!(!binder_new.contains("effective_graph_join_order_soft_margin_step(config)"));
	assert_eq!(resolver.matches("receipt_gate_snapshot()").count(), 1);
	assert!(!resolver.contains("verdict_snapshot()"));
	assert!(resolver.contains("let snapshot = config.runtime_evidence.receipt_gate_snapshot();"));
	assert!(resolver.contains("let tree_input = tree_margin_advisory_input(&snapshot.verdicts);"));
	assert!(
		resolver.contains("let graph_input = graph_margin_advisory_input(&snapshot.verdicts);")
	);
}

#[test]
pub(crate) fn adr215_graph_resolver_is_scope_pure() {
	let join_order = source("src/binder/join_order.rs");
	let graph_predicate = first_block(&join_order, "pub(crate) fn is_graph_margin_advisory_kind");
	let graph_input = first_block(&join_order, "fn graph_margin_advisory_input");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");

	assert!(graph_input.contains("MarginAdvisoryScope::GraphFamily"));
	assert!(resolver.contains("graph_margin_advisory_input(&snapshot.verdicts)"));
	assert!(resolver.contains("recommend_soft_margin("));
	assert!(resolver.contains("&graph_input.rows"));
	for kind in ["GalaxyRoot", "BridgeRoot", "BridgeChainRoot"] {
		assert!(
			graph_predicate.contains(kind),
			"graph resolver must accept {kind}"
		);
	}
	for kind in [
		"StarCandidate",
		"FlatStar",
		"SnowflakeRoot",
		"SnowflakeBranch",
		"PreservedOuterOrder",
	] {
		assert!(
			graph_predicate.contains(kind),
			"graph resolver must classify {kind}"
		);
	}
}

#[test]
pub(crate) fn adr215_insufficient_graph_evidence_is_byte_identical() {
	let tests = source("src/binder/tests.rs");
	let test = first_block(
		&tests,
		"async fn apply_advisory_with_insufficient_graph_evidence_binds_byte_identical",
	);
	let recommend = first_block(
		&source("src/binder/join_order.rs"),
		"pub(crate) fn recommend_soft_margin",
	);

	assert!(test.contains("JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE - 1"));
	assert!(test.contains("OptimizerRuntimeVerdictClass::MissedWin"));
	assert!(test.contains("OptimizerRuntimeVerdictClass::OverEager"));
	assert!(test.contains("format!(\"{manual_plan:?}\")"));
	assert!(test.contains("format!(\"{auto_plan:?}\")"));
	assert!(test.contains("effective_graph_join_order_soft_margin_step(&auto)"));
	assert!(recommend.contains("sample_size < JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE"));
	assert!(recommend.contains("MarginRecommendation::InsufficientEvidence"));
	assert!(recommend.contains("MarginRecommendation::Keep"));
}

#[test]
pub(crate) fn adr215_kind_detail_rows_remain_non_actionable() {
	let join_order = source("src/binder/join_order.rs");
	let scope_impl = first_block(&join_order, "impl MarginAdvisoryScope");
	let row_helper = first_block(&join_order, "fn margin_advisory_report_for_scope");

	assert!(scope_impl.contains("Self::TreeFamily | Self::GraphFamily"));
	assert!(scope_impl.contains("Self::GraphKind(_) => MarginAdvisoryRowKind::KindDetail"));
	assert!(scope_impl.contains("fn is_actionable_knob"));
	assert!(scope_impl.contains("matches!(self, Self::TreeFamily | Self::GraphFamily)"));
	assert!(row_helper.contains("if scope.is_actionable_knob()"));
	assert!(row_helper.contains("JoinOrderSoftMarginApplication::Manual"));
}

#[test]
pub(crate) fn adr215_no_outer_family_application() {
	let join_order = source("src/binder/join_order.rs");
	let tests = source("src/binder/tests.rs");
	let tree_predicate = first_block(&join_order, "pub(crate) fn is_tree_margin_advisory_kind");
	let graph_predicate = first_block(&join_order, "pub(crate) fn is_graph_margin_advisory_kind");
	let outer_gate = first_block(
		&tests,
		"async fn preserved_outer_evidence_steers_neither_family",
	);

	assert!(tree_predicate.contains("PreservedOuterOrder"));
	assert!(graph_predicate.contains("PreservedOuterOrder"));
	assert!(outer_gate.contains("OptimizerDecisionKind::PreservedOuterOrder"));
	assert!(outer_gate.contains("effective_tree_join_order_soft_margin_step(&config)"));
	assert!(outer_gate.contains("effective_graph_join_order_soft_margin_step(&config)"));
	assert!(outer_gate.contains("SoftMarginLadderStep::Baseline"));
}

#[test]
pub(crate) fn adr215_no_new_keys_tables_or_config() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let manifest = manifest_dir();
	let config = first_block(&binder, "pub struct PlannerConfig");
	let system_table_kind = first_block(&execution, "pub enum SystemTableKind");

	assert!(!config.contains("graph_join_order_soft_margin_application"));
	assert!(!config.contains("graph_join_order_soft_margin_step"));
	assert_eq!(
		system_table_kind.matches("OptimizerMarginAdvisory").count(),
		1
	);
	assert!(runtime_evidence.contains("all_verdict_counter_keys"));
	assert!(!runtime_evidence.contains("GraphFamilyMargin"));

	let markers = [
		"effective_join_order_policy",
		"effective_graph_join_order_soft_margin_step",
		"graph_margin_advisory_input",
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
		"ADR215 graph margin application leaked outside binder policy/reporting surfaces:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr208_no_wendelon_counter_replication_claim() {
	let manifest = manifest_dir();
	let markers = [
		"join_order_policy",
		"graph_steps",
		"tree_margin_advisory_input",
		"evidence_scope",
		"excluded_sample_size",
	];
	let offenders = source_lines_for_path(&manifest.join("../wendelon/src"))
		.into_iter()
		.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker)))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ADR208 Toron advisory policy leaked into Wendelon:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr207_runtime_evidence_is_extracted_fixed_matrix() {
	let binder = binder_root_source();
	let decision_log = source("src/binder/decision_log.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let kinds = first_block(&runtime_evidence, "fn runtime_evidence_decision_kinds");
	let classes = first_block(&runtime_evidence, "fn verdict_classes");
	let all_keys = first_block(&runtime_evidence, "pub(crate) fn all_verdict_counter_keys");

	assert!(binder.contains("mod runtime_evidence;"));
	assert!(binder.contains("pub runtime_evidence: Arc<OptimizerRuntimeEvidence>"));
	assert!(binder.contains("runtime_evidence: Arc::new(OptimizerRuntimeEvidence::local_only())"));
	assert!(!decision_log.contains("verdict_counts"));
	assert!(!decision_log.contains("pub(crate) fn record_verdict"));
	assert!(!decision_log.contains("pub(crate) fn verdict_snapshot"));
	assert!(runtime_evidence.contains("OPTIMIZER_EVIDENCE_KEY_PREFIX"));
	for kind in [
		"StarCandidate",
		"FlatStar",
		"SnowflakeRoot",
		"SnowflakeBranch",
		"GalaxyRoot",
		"BridgeRoot",
		"BridgeChainRoot",
		"PreservedOuterOrder",
	] {
		assert!(
			kinds.contains(kind),
			"fixed evidence matrix is missing {kind}"
		);
	}
	for class in [
		"Vindicated",
		"MissedWin",
		"OverEager",
		"MisRanked",
		"Indeterminate",
	] {
		assert!(
			classes.contains(class),
			"fixed evidence matrix is missing {class}"
		);
	}
	assert!(all_keys.contains("runtime_evidence_decision_kinds().len()"));
	assert!(all_keys.contains("* verdict_classes().len()"));
	assert!(all_keys.contains("* runtime_evidence_lanes().len()"));
	assert!(runtime_evidence.contains("OptimizerRuntimeEvidenceLane::Total"));
	assert!(runtime_evidence.contains("OptimizerRuntimeEvidenceLane::Demoted"));
	assert!(runtime_evidence.contains("\"{}/{}\""));
	assert!(runtime_evidence.contains("\"{}/demoted/{}\""));
	assert!(!runtime_evidence.contains("prefix_scan"));
	assert!(!runtime_evidence.contains("SystemTableRegistry"));
}

#[test]
pub(crate) fn adr207_snapshot_merges_durable_pending_and_in_flight() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let snapshot = first_block(&runtime_evidence, "pub(crate) fn verdict_snapshot");
	let snapshot_rows = first_block(&runtime_evidence, "fn verdict_rows_from_state");
	let flush = first_block(&runtime_evidence, "fn take_pending_for_flush");
	let ack = first_block(&runtime_evidence, "fn ack_flush");
	let refresh = first_block(&runtime_evidence, "pub async fn refresh_durable_cache");

	assert!(snapshot.contains("verdict_rows_from_state(&state)"));
	assert!(snapshot_rows.contains("state.durable_cache.clone()"));
	assert!(snapshot_rows.contains("state.pending.iter().chain(state.in_flight.iter())"));
	assert!(snapshot_rows.contains("native > 0"));
	assert!(snapshot_rows.contains("demoted > 0"));
	assert!(flush.contains("state.in_flight.get(&key)"));
	assert!(flush.contains("state.in_flight.insert(key, delta)"));
	assert!(ack.contains(".max(current_value)"));
	assert!(refresh.contains("value.unwrap_or(0)"));
}

#[test]
pub(crate) fn adr207_flush_is_awaited_post_success_not_background_queue() {
	let coordinator = source("src/coordinator.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let streaming = first_block(&coordinator, "async fn run_streaming_read_driver");
	let local = brace_blocks_after_marker(&coordinator, "pub async fn execute_query")
		.into_iter()
		.map(|(_, block)| block)
		.find(|block| block.contains("self.executor.execute_bound"))
		.expect("local QueryCoordinator execute_query block");

	for block in [&streaming, &local] {
		assert!(block.contains(".record_verdict("));
		assert!(block.contains(".flush_keys(touched).await"));
	}
	assert!(!runtime_evidence.contains("mpsc"));
	assert!(!runtime_evidence.contains("tokio::spawn"));
}

#[test]
pub(crate) fn adr207_uses_counter_facade_and_never_retries_ambiguous_delta() {
	let manifest = manifest_dir();
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let flush = first_block(&runtime_evidence, "pub(crate) async fn flush_keys");
	let drop_ambiguous = first_block(&runtime_evidence, "fn drop_ambiguous_in_flight");
	let unsupported = first_block(&runtime_evidence, "fn latch_replication_unsupported");

	assert!(runtime_evidence.contains(".counter_fetch_add("));
	assert!(runtime_evidence.contains(".counter_read("));
	assert_eq!(flush.matches("counter_fetch_add").count(), 1);
	assert!(flush.contains("COUNTER_REPLICATION_UNSUPPORTED"));
	assert!(flush.contains("drop_ambiguous_in_flight"));
	assert!(!drop_ambiguous.contains("counter_fetch_add"));
	assert!(unsupported.contains("ReplicationUnsupported"));

	let raw_counter_offenders = source_lines(&manifest.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("CounterAddRequest")
				|| line.contains("CounterAddRequestMeta")
				|| line.contains("COUNTER_ADD_REQ")
		})
		.collect::<Vec<_>>();
	assert!(
		raw_counter_offenders.is_empty(),
		"ADR207 must use the Wendelon counter facade, not raw counter frames:\n{}",
		format_offenders(&manifest, &raw_counter_offenders)
	);
}

#[test]
pub(crate) fn adr207_bind_path_never_awaits_storage_for_evidence() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let report = first_block(&join_order, "pub(crate) fn margin_advisory_report");
	let report_rows = first_block(&join_order, "pub(crate) fn margin_advisory_report_rows");

	for block in [&bind_relation, &resolver, &report, &report_rows] {
		assert!(block.contains("runtime_evidence"));
		assert!(!block.contains("counter_read"));
		assert!(!block.contains("counter_fetch_add"));
		assert!(!block.contains("refresh_durable_cache"));
	}
	assert!(!resolver.contains(".await"));
	assert!(!report.contains(".await"));
}

#[test]
pub(crate) fn adr207_status_surface_and_metrics_report_sink_health() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let explain = source("src/binder/explain.rs");
	let observability = source("src/network/observability.rs");
	let daemon = source("src/daemon.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	let status_schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_evidence_status_schema",
	);
	let status_rows = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_evidence_status_rows",
	);
	let metrics = first_block(&observability, "fn render_optimizer_evidence_metrics");

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_EVIDENCE_STATUS_TABLE"));
	assert!(execution.contains("InformationSchemaOptimizerEvidenceStatus"));
	assert!(bind_relation.contains("INFORMATION_SCHEMA_OPTIMIZER_EVIDENCE_STATUS_TABLE"));
	assert!(bind_relation.contains("runtime_evidence.status_snapshot()"));
	for column in [
		"mode",
		"state",
		"pending_deltas",
		"dropped_deltas",
		"ambiguous_deltas",
		"refresh_failures",
		"last_error",
	] {
		assert!(
			status_schema.contains(column),
			"status schema is missing {column}"
		);
		assert!(
			status_rows.contains(column),
			"status rows are missing {column}"
		);
	}
	for metric in [
		"optimizer_evidence_pending_deltas",
		"optimizer_evidence_dropped_deltas_total",
		"optimizer_evidence_ambiguous_deltas_total",
		"optimizer_evidence_refresh_failures_total",
		"optimizer_evidence_durable_enabled",
	] {
		assert!(
			metrics.contains(metric),
			"metrics output is missing {metric}"
		);
	}
	assert!(daemon.contains("refresh_durable_cache().await"));
	assert!(daemon.contains("OPTIMIZER_EVIDENCE_REFRESH_INTERVAL"));
	assert!(daemon.contains("runtime_evidence: Some(Arc::clone(&runtime_evidence))"));
}

#[test]
pub(crate) fn adr207_no_wendelon_production_or_optimizer_shell_change() {
	let manifest = manifest_dir();
	let offenders = source_lines(&manifest.join("../wendelon/src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("optimizer_evidence"))
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR207 optimizer evidence leaked into Wendelon production crate:\n{}",
		format_offenders(&manifest, &offenders)
	);

	adr191_no_optimizer_shell_or_planproperties_contract();
}

#[test]
pub(crate) fn adr217_receipt_gates_are_explicit_five_row_system_table() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let explain = source("src/binder/explain.rs");
	let join_order = source("src/binder/join_order.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	let schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_receipt_gates_schema",
	);
	let rows = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_receipt_gate_rows",
	);
	let report = first_block(&join_order, "pub(crate) fn optimizer_receipt_gate_rows");

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_RECEIPT_GATES_TABLE"));
	assert!(binder.contains("information_schema.optimizer_receipt_gates"));
	assert!(execution.contains("InformationSchemaOptimizerReceiptGates"));
	assert!(bind_relation.contains("SystemTableKind::InformationSchemaOptimizerReceiptGates"));
	assert!(bind_relation.contains("optimizer_receipt_gate_rows(self.config)"));
	assert!(bind_relation.contains("information_schema_optimizer_receipt_gates_schema"));
	assert!(bind_relation.contains("information_schema_optimizer_receipt_gate_rows"));
	for column in [
		"gate_name",
		"gate_family",
		"status",
		"sample_count",
		"primary_signal",
		"secondary_signal",
		"reason",
		"next_cut",
	] {
		assert!(schema.contains(column), "schema is missing {column}");
		assert!(rows.contains(column), "row projection is missing {column}");
	}
	assert!(report.contains("graph_margin_divergence_gate_row(&snapshot, config)"));
	assert!(report.contains("preserved_outer_ordering_gate_row(&snapshot, config)"));
	assert!(report.contains("decision_conflict_pressure_gate(&snapshot)"));
	assert!(report.contains("windowing_decay_pressure_gate(&snapshot)"));
	assert!(report.contains("counter_handoff_readiness_gate(&snapshot)"));
	for gate in [
		"graph_margin_divergence",
		"preserved_outer_ordering",
		"decision_conflict_pressure",
		"windowing_decay_pressure",
		"counter_handoff_readiness",
	] {
		assert!(join_order.contains(gate), "missing receipt gate {gate}");
	}
}

#[test]
pub(crate) fn adr217_receipt_gate_status_vocabulary_is_closed() {
	let join_order = source("src/binder/join_order.rs");
	let status = first_block(&join_order, "pub(crate) enum OptimizerReceiptGateStatus");
	let label = first_block(
		&join_order,
		"pub(crate) fn optimizer_receipt_gate_status_label",
	);

	assert_eq!(enum_variant_count(&status), 4);
	for token in [
		"InsufficientEvidence",
		"Inactive",
		"Actionable",
		"BlockedExternalSignal",
		"insufficient_evidence",
		"inactive",
		"actionable",
		"blocked_external_signal",
	] {
		assert!(
			status.contains(token) || label.contains(token),
			"missing receipt gate status token {token}"
		);
	}
}

#[test]
pub(crate) fn adr217_receipt_gate_snapshot_is_single_locked_snapshot() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let join_order = source("src/binder/join_order.rs");
	let snapshot = first_block(&runtime_evidence, "pub(crate) fn receipt_gate_snapshot");
	let report = first_block(&join_order, "pub(crate) fn optimizer_receipt_gate_rows");

	assert!(runtime_evidence.contains("pub(crate) struct OptimizerReceiptGateSnapshot"));
	assert!(snapshot.matches("self.state.lock()").count() == 1);
	assert!(snapshot.contains("verdict_rows_from_state(&state)"));
	assert!(snapshot.contains("decision_rows_from_state(&state)"));
	assert!(snapshot.contains("status_snapshot_from_state(&state)"));
	assert!(report.contains("receipt_gate_snapshot()"));
	assert!(!report.contains("verdict_snapshot()"));
	assert!(!report.contains("decision_snapshot()"));
	assert!(!report.contains("status_snapshot()"));
}

#[test]
pub(crate) fn adr217_graph_gate_uses_ready_kind_divergence_not_family_rollup() {
	let join_order = source("src/binder/join_order.rs");
	let graph = first_block(&join_order, "fn graph_margin_divergence_gate");
	let row = first_block(&join_order, "fn graph_margin_divergence_gate_row");
	let signals = first_block(&join_order, "fn graph_kind_gate_signals");

	assert!(join_order.contains("pub(crate) struct GraphMarginDivergenceGate"));
	assert!(graph.contains("ready.len() < 2"));
	assert!(graph.contains("left.recommendation != right.recommendation"));
	assert!(graph.contains("left.recommended_step != right.recommended_step"));
	assert!(graph.contains("OptimizerReceiptGateStatus::Actionable"));
	assert!(row.contains("graph_kind_recommendations_diverge"));
	assert!(signals.contains("MarginAdvisoryScope::GraphKind(kind)"));
	assert!(signals.contains("JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE"));
	for kind in ["GalaxyRoot", "BridgeRoot", "BridgeChainRoot"] {
		assert!(signals.contains(kind), "graph gate missing {kind}");
	}
}

#[test]
pub(crate) fn adr217_preserved_outer_gate_is_separate_loosen_only_signal() {
	let join_order = source("src/binder/join_order.rs");
	let preserved = first_block(&join_order, "fn preserved_outer_ordering_gate");

	assert!(preserved.contains("OptimizerDecisionKind::PreservedOuterOrder"));
	assert!(preserved.contains("MarginAdvisoryScope::GraphKind"));
	assert!(preserved.contains("JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE"));
	assert!(preserved.contains("report.advisory.recommendation == MarginRecommendation::Loosen"));
	let row = first_block(&join_order, "fn preserved_outer_ordering_gate_row");
	assert!(row.contains("preserved_outer_missed_win_ready"));
	assert!(row.contains("preserved_outer_join_order"));
}

#[test]
pub(crate) fn adr217_decision_conflict_gate_uses_durable_decision_rows() {
	let join_order = source("src/binder/join_order.rs");
	let gate = first_block(&join_order, "fn decision_conflict_pressure_gate");
	let reports = first_block(&join_order, "pub(crate) fn decision_conflict_reports");
	let strongest = first_block(&join_order, "fn strongest_decision_conflict_report");
	let compare = first_block(&join_order, "fn compare_decision_conflict_report");

	assert!(gate.contains("decision_conflict_reports(snapshot)"));
	assert!(gate.contains("strongest_decision_conflict_report(&reports)"));
	assert!(gate.contains("DecisionConflictStatus::Conflicted"));
	assert!(gate.contains("reordered_and_blocked_ready"));
	assert!(join_order.contains("fn decision_conflict_status("));
	assert!(join_order.contains("JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE"));
	assert!(reports.contains("snapshot.decisions"));
	assert!(reports.contains("OptimizerDecisionOutcome::Reordered"));
	assert!(reports.contains("OptimizerDecisionOutcome::KeptThinMargin"));
	assert!(reports.contains("OptimizerDecisionOutcome::Declined(reason)"));
	assert!(reports.contains("OptimizerDecisionOutcome::KeptOptimal"));
	assert!(reports.contains("row.provenance"));
	assert!(strongest.contains("report.total_count > 0"));
	assert!(compare.contains("conflict_strength"));
	assert!(compare.contains("total_count"));
}

#[test]
pub(crate) fn adr217_blocked_placeholders_cannot_render_actionable() {
	let join_order = source("src/binder/join_order.rs");
	let windowing = first_block(&join_order, "fn windowing_decay_pressure_gate");
	let counter = first_block(&join_order, "fn counter_handoff_readiness_gate");

	assert!(windowing.contains("OptimizerReceiptGateStatus::Inactive"));
	assert!(!windowing.contains("OptimizerReceiptGateStatus::Actionable"));
	assert!(windowing.contains("windowing_decay_substrate_unbuilt"));
	assert!(counter.contains("OptimizerReceiptGateStatus::BlockedExternalSignal"));
	assert!(!counter.contains("OptimizerReceiptGateStatus::Actionable"));
	assert!(counter.contains("elasticity_signal_unbuilt"));
}

#[test]
pub(crate) fn adr217_readout_has_no_policy_consumers_or_storage_surface() {
	let manifest = manifest_dir();
	let markers = ["optimizer_receipt_gate", "optimizer_receipt_gates"];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/distributed"),
		manifest.join("src/coordinator.rs"),
		manifest.join("src/daemon.rs"),
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
		"ADR217 receipt gate readout leaked outside binder/report/tests:\n{}",
		format_offenders(&manifest, &offenders)
	);

	let join_order = source("src/binder/join_order.rs");
	let report = first_block(&join_order, "pub(crate) fn optimizer_receipt_gate_rows");
	for forbidden in [
		"effective_join_order_policy",
		"try_bind_star_order_actuation",
		"bind_galaxy_order",
		"counter_fetch_add",
		"refresh_durable_cache",
	] {
		assert!(
			!report.contains(forbidden),
			"receipt gate readout gained policy/storage edge {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr217_no_new_durable_counter_keys_or_flush_path() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let keys = first_block(&runtime_evidence, "pub(crate) fn all_verdict_counter_keys");
	let decision_keys = first_block(&runtime_evidence, "pub(crate) fn all_decision_counter_keys");
	let flush = first_block(&runtime_evidence, "pub(crate) async fn flush_keys");
	let decision_flush = first_block(&runtime_evidence, "pub async fn flush_decision_deltas");

	assert!(!runtime_evidence.contains("optimizer_evidence/receipt"));
	assert!(!runtime_evidence.contains("optimizer_receipt_gates"));
	assert!(!keys.contains("receipt"));
	assert!(!decision_keys.contains("receipt"));
	assert_eq!(flush.matches("counter_fetch_add").count(), 1);
	assert_eq!(decision_flush.matches("counter_fetch_add").count(), 1);
}

#[test]
pub(crate) fn adr207_verdict_evidence_keys_are_fixed_matrix_no_registry() {
	adr207_runtime_evidence_is_extracted_fixed_matrix();
}

#[test]
pub(crate) fn adr207_counter_absent_maps_to_zero_in_consumer() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let refresh = first_block(&runtime_evidence, "pub async fn refresh_durable_cache");
	let snapshot_rows = first_block(&runtime_evidence, "fn verdict_rows_from_state");

	assert!(refresh.contains("value.unwrap_or(0)"));
	assert!(snapshot_rows.contains("native > 0"));
	assert!(snapshot_rows.contains("demoted > 0"));
}

#[test]
pub(crate) fn adr207_snapshot_is_durable_cache_plus_pending_not_local_plus_durable() {
	adr207_snapshot_merges_durable_pending_and_in_flight();
}

#[test]
pub(crate) fn adr207_in_flight_deltas_are_not_reissued_or_hidden() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let snapshot_rows = first_block(&runtime_evidence, "fn verdict_rows_from_state");
	let flush = first_block(&runtime_evidence, "fn take_pending_for_flush");
	let drop_ambiguous = first_block(&runtime_evidence, "fn drop_ambiguous_in_flight");

	assert!(snapshot_rows.contains("state.in_flight.iter()"));
	assert!(flush.contains("state.in_flight.get(&key)"));
	assert!(drop_ambiguous.contains("state.in_flight.remove(&key)"));
	assert!(!drop_ambiguous.contains("state.pending"));
}

#[test]
pub(crate) fn adr207_ack_flush_uses_larger_wins_cache_update() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let ack = first_block(&runtime_evidence, "fn ack_flush");

	assert!(ack.contains(".max(current_value)"));
}

#[test]
pub(crate) fn adr207_no_fresh_write_id_after_ambiguous_flush() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let flush = first_block(&runtime_evidence, "pub(crate) async fn flush_keys");
	let drop_ambiguous = first_block(&runtime_evidence, "fn drop_ambiguous_in_flight");

	assert_eq!(flush.matches("counter_fetch_add").count(), 1);
	assert!(!drop_ambiguous.contains("counter_fetch_add"));
	assert!(!drop_ambiguous.contains("pending"));
}

#[test]
pub(crate) fn adr207_uses_counter_fetch_add_facade_not_raw_counter_frames() {
	adr207_uses_counter_facade_and_never_retries_ambiguous_delta();
}

#[test]
pub(crate) fn adr207_decision_lane_stays_local() {
	let decision_log = source("src/binder/decision_log.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");

	assert!(decision_log.contains("pub(crate) fn record("));
	assert!(!decision_log.contains("pub(crate) fn snapshot("));
	assert!(!decision_log.contains("verdict_counts"));
	assert!(!decision_log.contains("pub(crate) fn record_verdict"));
	assert!(!decision_log.contains("pub(crate) fn verdict_snapshot"));
	assert!(runtime_evidence.contains("pub(crate) fn record_verdict"));
	assert!(runtime_evidence.contains("pub(crate) fn verdict_snapshot"));
	assert!(runtime_evidence.contains("pub(crate) fn record_decision"));
	assert!(runtime_evidence.contains("pub(crate) fn decision_snapshot"));
}

#[test]
pub(crate) fn adr207_replication_unsupported_latches_degraded() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let flush = first_block(&runtime_evidence, "pub(crate) async fn flush_keys");
	let unsupported = first_block(&runtime_evidence, "fn latch_replication_unsupported");

	assert!(flush.contains("COUNTER_REPLICATION_UNSUPPORTED"));
	assert!(unsupported.contains("replication_unsupported = true"));
	assert!(unsupported.contains("circuit_open = true"));
	assert!(unsupported.contains("OptimizerEvidenceMode::Degraded"));
	assert!(unsupported.contains("OptimizerEvidenceState::ReplicationUnsupported"));
}

#[test]
pub(crate) fn adr207_no_otlp_optimizer_metric_expansion() {
	let telemetry = source("src/telemetry.rs");

	assert!(!telemetry.contains("optimizer_evidence"));
	assert!(!telemetry.contains("OptimizerRuntimeEvidence"));
}

#[test]
pub(crate) fn adr207_no_wendelon_production_change() {
	adr207_no_wendelon_production_or_optimizer_shell_change();
}

#[test]
pub(crate) fn adr207_evidence_failure_never_fails_queries() {
	let coordinator = source("src/coordinator.rs");
	let streaming = first_block(&coordinator, "async fn run_streaming_read_driver");
	let local = brace_blocks_after_marker(&coordinator, "pub async fn execute_query")
		.into_iter()
		.map(|(_, block)| block)
		.find(|block| block.contains("self.executor.execute_bound"))
		.expect("local QueryCoordinator execute_query block");

	for block in [&streaming, &local] {
		assert!(block.contains(".flush_keys(touched).await;"));
		assert!(!block.contains(".flush_keys(touched).await?"));
		assert!(!block.contains(".flush_keys(touched).await.map_err"));
	}
}

#[test]
pub(crate) fn adr201_graph_verdicts_accrue_to_sampled_runtime_log() {
	let explain = source("src/binder/explain.rs");
	let verdicts = first_block(&explain, "fn measured_verdicts_from_summaries");

	// The sampled verdict producer emits graph verdicts from the recognized galaxy/bridge evidence.
	assert!(verdicts.contains("render_trace.galaxy_order"));
	assert!(verdicts.contains("render_trace.bridge_order"));
	assert!(verdicts.contains("OptimizerDecisionKind::GalaxyRoot"));
	assert!(verdicts.contains("OptimizerDecisionKind::BridgeRoot"));
}

#[test]
pub(crate) fn adr201_verdict_computation_is_shared_between_render_and_accrual() {
	let explain = source("src/binder/explain.rs");
	let render_row = first_block(&explain, "fn graph_order_verdict_row");
	let verdicts = first_block(&explain, "fn measured_verdicts_from_summaries");

	// One authoritative classifier — render row and sampled accrual both call it.
	assert!(explain.contains("fn measured_graph_order_verdict"));
	assert!(render_row.contains("measured_graph_order_verdict"));
	assert!(verdicts.contains("measured_graph_order_verdict"));
}

#[test]
pub(crate) fn adr201_actuated_graph_suppresses_orphan_tree_verdict() {
	let explain = source("src/binder/explain.rs");
	let verdicts = first_block(&explain, "fn measured_verdicts_from_summaries");

	// Tree verdicts are gated on the actuated-graph check (the no-op-rebind suppressed their decisions),
	// so an actuated graph emits no orphan tree verdict. ADR 206: the check is partition-complete via the
	// `applied_graph_root()` accessor (galaxy / bridge / chain), so an actuated chain is suppressed too.
	let accessor = first_block(&explain, "pub(super) fn applied_graph_root");
	assert!(verdicts.contains("let graph_actuated"));
	assert!(verdicts.contains("render_trace.applied_graph_root().is_some()"));
	assert!(verdicts.contains("if !graph_actuated"));
	assert!(accessor.contains("applied_galaxy_root"));
	assert!(accessor.contains("applied_bridge_root"));
	assert!(accessor.contains("applied_bridge_chain_root"));
}

#[test]
pub(crate) fn adr201_verdict_accrual_is_side_effect_only() {
	let manifest = manifest_dir();
	let markers = [
		"measured_graph_order_verdict",
		"galaxy_measured_costs",
		"bridge_measured_costs",
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
		"ADR201 graph verdict accrual leaked outside the binder surface:\n{}",
		format_offenders(&manifest, &offenders)
	);
}
