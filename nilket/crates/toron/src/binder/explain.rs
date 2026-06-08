use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct JoinTraceTarget {
	pub(crate) join_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecisionReceipt {
	pub(crate) target: JoinTraceTarget,
	pub(crate) strategy: JoinStrategy,
	pub(crate) placement: JoinPhysicalPlacement,
	pub(crate) reason: JoinStrategyDecisionReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanTraceTarget {
	pub(crate) scan_index: usize,
	pub(crate) table_id: Option<TableId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterEstimateSource {
	Range,
	ConvertedRange,
	NdvEquality,
	NdvInList,
	HeavyHitterTopK,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanFilterOutcome {
	FullyPushed,
	PartiallyPushed,
	ResidualOnly,
	StaticElided,
	PointRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanFilterReceipt {
	pub(crate) target: ScanTraceTarget,
	pub(crate) outcome: ScanFilterOutcome,
	pub(crate) converted_pushdown: bool,
	pub(crate) profile_hint: QueryProfileHint,
	pub(crate) estimated_rows: Option<RowCount>,
	pub(crate) spent_fact: Option<FilterEstimateSource>,
	pub(crate) spent_column: Option<ColumnOrdinal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FilterEstimateEvidence {
	pub(super) rows: RowCount,
	pub(super) source: FilterEstimateSource,
	pub(super) column: Option<ColumnOrdinal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScanFilterPushdownEvidence {
	pub(super) outcome: ScanFilterOutcome,
	pub(super) converted_pushdown: bool,
	pub(super) profile_hint: QueryProfileHint,
	pub(super) table_id: Option<TableId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerTrace {
	pub(crate) join_decisions: Vec<DecisionReceipt>,
	pub(crate) scan_filter_decisions: Vec<ScanFilterReceipt>,
	pub(super) record_optimizer_decisions: bool,
}

impl Default for OptimizerTrace {
	fn default() -> Self {
		Self {
			join_decisions: Vec::new(),
			scan_filter_decisions: Vec::new(),
			record_optimizer_decisions: true,
		}
	}
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct OptimizerExplainRenderTrace {
	pub(super) scan_filters: Vec<ScanFilterRenderReceipt>,
	pub(super) join_order_edges: Vec<JoinOrderEdgeEvidence>,
	pub(super) join_order: JoinOrderRenderState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderRenderReceipt {
	pub(super) probe_label: String,
	pub(super) outcome: StarOrderOutcome,
	pub(super) edge_targets: Vec<JoinTraceTarget>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) enum JoinOrderRenderState {
	#[default]
	None,
	StarReceipts(Vec<StarOrderRenderReceipt>),
	ObservedSnowflake(SnowflakeOrderEvidence),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ScanFilterRenderReceipt {
	pub(super) target: ScanTraceTarget,
	pub(super) predicate_text: Option<String>,
	pub(super) pushed_predicate_text: Option<String>,
	pub(super) residual_predicate_text: Option<String>,
	pub(super) spent_column_name: Option<String>,
}

impl OptimizerExplainRenderTrace {
	pub(super) fn scan_filter(&self, target: ScanTraceTarget) -> Option<&ScanFilterRenderReceipt> {
		self.scan_filters
			.iter()
			.find(|receipt| receipt.target == target)
	}

	pub(super) fn has_recognized_star_order(&self) -> bool {
		match &self.join_order {
			JoinOrderRenderState::StarReceipts(receipts) => receipts
				.iter()
				.any(|receipt| matches!(receipt.outcome, StarOrderOutcome::Recognized(_))),
			JoinOrderRenderState::None | JoinOrderRenderState::ObservedSnowflake(_) => false,
		}
	}

	pub(super) fn star_order_receipts(&self) -> &[StarOrderRenderReceipt] {
		match &self.join_order {
			JoinOrderRenderState::StarReceipts(receipts) => receipts,
			JoinOrderRenderState::None | JoinOrderRenderState::ObservedSnowflake(_) => &[],
		}
	}

	pub(super) fn set_observed_snowflake(&mut self, evidence: SnowflakeOrderEvidence) {
		self.join_order = JoinOrderRenderState::ObservedSnowflake(evidence);
	}

	pub(super) fn set_star_receipt(&mut self, receipt: StarOrderRenderReceipt) {
		self.join_order = JoinOrderRenderState::StarReceipts(vec![receipt]);
	}

	pub(super) fn set_star_receipts(&mut self, receipts: Vec<StarOrderRenderReceipt>) {
		self.join_order = JoinOrderRenderState::StarReceipts(receipts);
	}
}

pub(super) fn table_name(table: &TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
	}
}

pub(super) fn is_information_schema_namespace(table: &TableRef) -> bool {
	table
		.schema
		.as_deref()
		.is_some_and(|schema| schema.eq_ignore_ascii_case(INFORMATION_SCHEMA_NAMESPACE))
}

pub(super) fn reject_information_schema_target(table: &TableRef) -> Result<(), BindingError> {
	if is_information_schema_namespace(table) {
		return Err(BindingError::UnsupportedExpression {
			message: format!("information_schema is read-only: {}", table_name(table)),
		});
	}
	Ok(())
}

pub(super) fn information_schema_logical_types_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("logical_type_id", DataType::Int64, false),
		Field::new("name", DataType::Utf8, false),
		Field::new("physical", DataType::Utf8, false),
		Field::new("refinement", DataType::Utf8, true),
	]))
}

pub(super) fn information_schema_tables_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("table_schema", DataType::Utf8, false),
		Field::new("table_name", DataType::Utf8, false),
		Field::new("table_type", DataType::Utf8, false),
	]))
}

pub(super) fn information_schema_columns_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("table_schema", DataType::Utf8, false),
		Field::new("table_name", DataType::Utf8, false),
		Field::new("column_name", DataType::Utf8, false),
		Field::new("ordinal_position", DataType::Int64, false),
		Field::new("data_type", DataType::Utf8, false),
		Field::new("is_nullable", DataType::Utf8, false),
		Field::new("logical_type_name", DataType::Utf8, true),
	]))
}

pub(super) fn information_schema_schemata_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![Field::new(
		"schema_name",
		DataType::Utf8,
		false,
	)]))
}

pub(super) fn information_schema_optimizer_decisions_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("decision_kind", DataType::Utf8, false),
		Field::new("outcome", DataType::Utf8, false),
		Field::new("decline_reason", DataType::Utf8, true),
		Field::new("count", DataType::Int64, false),
	]))
}

pub(super) fn information_schema_optimizer_decision_rows(
	decisions: &[OptimizerDecisionRow],
) -> Result<Vec<Vec<ScalarValue>>, BindingError> {
	decisions
		.iter()
		.map(|decision| {
			Ok(vec![
				ScalarValue::Utf8(Some(
					optimizer_decision_kind_label(decision.kind).to_string(),
				)),
				ScalarValue::Utf8(Some(
					optimizer_decision_outcome_label(decision.outcome).to_string(),
				)),
				ScalarValue::Utf8(match decision.outcome {
					OptimizerDecisionOutcome::Declined(reason) => {
						Some(optimizer_decision_decline_reason_label(reason).to_string())
					}
					OptimizerDecisionOutcome::Reordered
					| OptimizerDecisionOutcome::KeptOptimal
					| OptimizerDecisionOutcome::KeptThinMargin
					| OptimizerDecisionOutcome::Overflow => None,
				}),
				ScalarValue::Int64(Some(i64::try_from(decision.count).map_err(|_| {
					BindingError::UnsupportedExpression {
						message: "optimizer explain optimizer_decision_count exceeds i64::MAX"
							.to_string(),
					}
				})?)),
			])
		})
		.collect()
}

pub(super) fn optimizer_explain_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("decision_kind", DataType::Utf8, false),
		Field::new("target", DataType::Utf8, false),
		Field::new("outcome", DataType::Utf8, false),
		Field::new("detail", DataType::Utf8, true),
		Field::new("predicate_text", DataType::Utf8, true),
		Field::new("pushed_predicate_text", DataType::Utf8, true),
		Field::new("residual_predicate_text", DataType::Utf8, true),
		Field::new("estimated_rows", DataType::Int64, true),
		Field::new("spent_fact", DataType::Utf8, true),
		Field::new("spent_column", DataType::Utf8, true),
		Field::new("spent_column_name", DataType::Utf8, true),
		Field::new("build_side", DataType::Utf8, true),
		Field::new("build_bytes", DataType::Int64, true),
		Field::new("threshold_bytes", DataType::Int64, true),
		Field::new("rejected_side_bytes", DataType::Int64, true),
	]))
}

pub(super) fn optimizer_explain_plan(
	trace: OptimizerTrace,
	render_trace: Option<OptimizerExplainRenderTrace>,
) -> Result<ExecutionPlan, BindingError> {
	let mut rows = trace
		.join_decisions
		.iter()
		.map(optimizer_explain_row)
		.collect::<Result<Vec<_>, _>>()?;
	let render_trace = render_trace.as_ref();
	rows.extend(
		trace
			.scan_filter_decisions
			.iter()
			.map(|receipt| {
				optimizer_explain_scan_filter_row(
					receipt,
					render_trace.and_then(|trace| trace.scan_filter(receipt.target)),
				)
			})
			.collect::<Result<Vec<_>, _>>()?,
	);
	if let Some(render_trace) = render_trace {
		match &render_trace.join_order {
			JoinOrderRenderState::None => {}
			JoinOrderRenderState::ObservedSnowflake(evidence) => {
				rows.push(optimizer_explain_snowflake_order_row(evidence)?);
			}
			JoinOrderRenderState::StarReceipts(receipts) => {
				for receipt in receipts {
					rows.push(optimizer_explain_join_order_row(&receipt.outcome)?);
				}
			}
		}
	}
	system_table_plan(
		SystemTableKind::OptimizerExplain,
		optimizer_explain_schema(),
		rows,
		None,
	)
}

pub(super) fn optimizer_analyze_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("decision_kind", DataType::Utf8, false),
		Field::new("target", DataType::Utf8, false),
		Field::new("outcome", DataType::Utf8, false),
		Field::new("detail", DataType::Utf8, true),
		Field::new("estimated_rows", DataType::Int64, true),
		Field::new("actual_rows", DataType::Int64, true),
		Field::new("ratio", DataType::Float64, true),
		Field::new("estimate_source", DataType::Utf8, true),
		Field::new("build_rows", DataType::Int64, true),
		Field::new("probe_rows", DataType::Int64, true),
	]))
}

#[derive(Debug, Clone, Copy, Default)]
struct JoinRuntimeObservationSummary {
	build_rows: RowCount,
	probe_rows: RowCount,
	output_rows: RowCount,
}

pub(super) fn optimizer_analyze_plan(
	_trace: OptimizerTrace,
	render_trace: OptimizerExplainRenderTrace,
	observations: &[JoinRuntimeObservation],
) -> Result<ExecutionPlan, BindingError> {
	let summaries = summarize_join_runtime_observations(observations);
	let rows = optimizer_analyze_rows(&render_trace, &summaries)?;
	system_table_plan(
		SystemTableKind::OptimizerExplain,
		optimizer_analyze_schema(),
		rows,
		None,
	)
}

fn summarize_join_runtime_observations(
	observations: &[JoinRuntimeObservation],
) -> BTreeMap<JoinReplayId, JoinRuntimeObservationSummary> {
	let mut summaries = BTreeMap::<JoinReplayId, JoinRuntimeObservationSummary>::new();
	for observation in observations {
		let summary = summaries.entry(observation.replay_id).or_default();
		summary.build_rows = RowCount::new(
			summary
				.build_rows
				.get()
				.saturating_add(observation.build_rows.get()),
		);
		summary.probe_rows = RowCount::new(
			summary
				.probe_rows
				.get()
				.saturating_add(observation.probe_rows.get()),
		);
		summary.output_rows = RowCount::new(
			summary
				.output_rows
				.get()
				.saturating_add(observation.output_rows.get()),
		);
	}
	summaries
}

fn optimizer_analyze_rows(
	render_trace: &OptimizerExplainRenderTrace,
	summaries: &BTreeMap<JoinReplayId, JoinRuntimeObservationSummary>,
) -> Result<Vec<Vec<ScalarValue>>, BindingError> {
	if let JoinOrderRenderState::ObservedSnowflake(evidence) = &render_trace.join_order {
		let mut rows = vec![optimizer_analyze_snowflake_order_row(evidence)];
		let dimensions = flatten_snowflake_dimensions(evidence);
		let mut measured_rows_by_dimension = BTreeMap::new();
		for edge in &render_trace.join_order_edges {
			let Some(dimension) = edge.dimension.as_deref() else {
				continue;
			};
			let Some((probe, dimension)) = dimensions.get(dimension) else {
				continue;
			};
			let replay_id = join_replay_id_for_trace_target(edge.target)?;
			if let Some(summary) = summaries.get(&replay_id) {
				measured_rows_by_dimension.insert(dimension.dimension.clone(), summary.output_rows);
			}
			rows.push(optimizer_analyze_snowflake_runtime_row(
				probe, edge, dimension, summaries,
			)?);
		}
		let verdict = measured_snowflake_order_verdict(evidence, &measured_rows_by_dimension);
		for level in &verdict.levels {
			rows.push(optimizer_analyze_snowflake_verdict_row(level));
		}
		rows.push(optimizer_analyze_snowflake_aggregate_verdict_row(&verdict));
		return Ok(rows);
	}

	let mut rows = Vec::new();
	for receipt in render_trace.star_order_receipts() {
		let StarOrderOutcome::Recognized(evidence) = &receipt.outcome else {
			continue;
		};
		let edge_targets = receipt
			.edge_targets
			.iter()
			.copied()
			.collect::<BTreeSet<_>>();
		let dimensions = evidence
			.dimensions
			.iter()
			.map(|dimension| (dimension.dimension.as_str(), dimension))
			.collect::<BTreeMap<_, _>>();
		let mut measured_rows_by_dimension = BTreeMap::new();
		for edge in &render_trace.join_order_edges {
			if !edge_targets.contains(&edge.target) {
				continue;
			}
			let Some(dimension) = edge.dimension.as_deref() else {
				continue;
			};
			let Some(dimension) = dimensions.get(dimension) else {
				continue;
			};
			let replay_id = join_replay_id_for_trace_target(edge.target)?;
			if let Some(summary) = summaries.get(&replay_id) {
				measured_rows_by_dimension.insert(dimension.dimension.clone(), summary.output_rows);
			}
			rows.push(optimizer_analyze_join_order_row(
				edge, dimension, summaries,
			)?);
		}
		rows.push(optimizer_analyze_join_order_verdict_row(
			evidence,
			&measured_rows_by_dimension,
		)?);
	}
	Ok(rows)
}

fn optimizer_analyze_join_order_row(
	edge: &JoinOrderEdgeEvidence,
	dimension: &JoinOrderDimEvidence,
	summaries: &BTreeMap<JoinReplayId, JoinRuntimeObservationSummary>,
) -> Result<Vec<ScalarValue>, BindingError> {
	let measurement = join_order_runtime_measurement(edge, dimension.estimated_rows, summaries)?;
	let estimated_rows = dimension.estimated_rows;
	Ok(vec![
		ScalarValue::Utf8(Some("join_order_runtime".to_string())),
		ScalarValue::Utf8(Some(format!(
			"join[{}] dimension={}",
			measurement.replay_id.0, dimension.dimension
		))),
		ScalarValue::Utf8(Some("measured".to_string())),
		ScalarValue::Utf8(Some(format!(
			"estimated={}; actual={}; ratio={}; source={}; build_rows={}; probe_rows={}",
			estimated_rows.get(),
			measurement.actual_rows.map(RowCount::get).unwrap_or(0),
			measurement
				.ratio
				.map(|value| format!("{value:.6}"))
				.unwrap_or_else(|| "missing".to_string()),
			join_order_estimate_source_label(dimension.estimate_source),
			measurement.summary.build_rows.get(),
			measurement.summary.probe_rows.get()
		))),
		ScalarValue::Int64(Some(row_count_to_i64(estimated_rows, "estimated_rows")?)),
		ScalarValue::Int64(
			measurement
				.actual_rows
				.map(|rows| row_count_to_i64(rows, "actual_rows"))
				.transpose()?,
		),
		ScalarValue::Float64(measurement.ratio),
		ScalarValue::Utf8(Some(
			join_order_estimate_source_label(dimension.estimate_source).to_string(),
		)),
		ScalarValue::Int64(Some(row_count_to_i64(
			measurement.summary.build_rows,
			"build_rows",
		)?)),
		ScalarValue::Int64(Some(row_count_to_i64(
			measurement.summary.probe_rows,
			"probe_rows",
		)?)),
	])
}

#[derive(Debug, Clone, Copy)]
struct JoinOrderRuntimeMeasurement {
	replay_id: JoinReplayId,
	summary: JoinRuntimeObservationSummary,
	actual_rows: Option<RowCount>,
	ratio: Option<f64>,
}

fn join_order_runtime_measurement(
	edge: &JoinOrderEdgeEvidence,
	estimated_rows: RowCount,
	summaries: &BTreeMap<JoinReplayId, JoinRuntimeObservationSummary>,
) -> Result<JoinOrderRuntimeMeasurement, BindingError> {
	let replay_id = join_replay_id_for_trace_target(edge.target)?;
	let observed_summary = summaries.get(&replay_id).copied();
	let summary = observed_summary.unwrap_or_default();
	let actual_rows = observed_summary.map(|summary| summary.output_rows);
	let ratio = actual_rows.and_then(|actual| {
		(estimated_rows.get() > 0).then_some(actual.get() as f64 / estimated_rows.get() as f64)
	});
	Ok(JoinOrderRuntimeMeasurement {
		replay_id,
		summary,
		actual_rows,
		ratio,
	})
}

fn optimizer_analyze_snowflake_runtime_row(
	probe: &str,
	edge: &JoinOrderEdgeEvidence,
	dimension: &JoinOrderDimEvidence,
	summaries: &BTreeMap<JoinReplayId, JoinRuntimeObservationSummary>,
) -> Result<Vec<ScalarValue>, BindingError> {
	let measurement = join_order_runtime_measurement(edge, dimension.estimated_rows, summaries)?;
	Ok(vec![
		ScalarValue::Utf8(Some("join_order_snowflake_runtime".to_string())),
		ScalarValue::Utf8(Some(format!(
			"join[{}] level={} dimension={}",
			measurement.replay_id.0, probe, dimension.dimension
		))),
		ScalarValue::Utf8(Some("measured".to_string())),
		ScalarValue::Utf8(Some(format!(
			"level={}; dimension={}; estimated={}; actual={}; ratio={}; source={}; build_rows={}; probe_rows={}",
			probe,
			dimension.dimension,
			dimension.estimated_rows.get(),
			measurement.actual_rows.map(RowCount::get).unwrap_or(0),
			measurement
				.ratio
				.map(|value| format!("{value:.6}"))
				.unwrap_or_else(|| "missing".to_string()),
			join_order_estimate_source_label(dimension.estimate_source),
			measurement.summary.build_rows.get(),
			measurement.summary.probe_rows.get()
		))),
		ScalarValue::Int64(Some(row_count_to_i64(
			dimension.estimated_rows,
			"estimated_rows",
		)?)),
		ScalarValue::Int64(
			measurement
				.actual_rows
				.map(|rows| row_count_to_i64(rows, "actual_rows"))
				.transpose()?,
		),
		ScalarValue::Float64(measurement.ratio),
		ScalarValue::Utf8(Some(
			join_order_estimate_source_label(dimension.estimate_source).to_string(),
		)),
		ScalarValue::Int64(Some(row_count_to_i64(
			measurement.summary.build_rows,
			"build_rows",
		)?)),
		ScalarValue::Int64(Some(row_count_to_i64(
			measurement.summary.probe_rows,
			"probe_rows",
		)?)),
	])
}

fn flatten_snowflake_dimensions(
	evidence: &SnowflakeOrderEvidence,
) -> BTreeMap<String, (String, JoinOrderDimEvidence)> {
	let mut dimensions = BTreeMap::new();
	collect_snowflake_dimensions(evidence, &mut dimensions);
	dimensions
}

fn collect_snowflake_dimensions(
	evidence: &SnowflakeOrderEvidence,
	dimensions: &mut BTreeMap<String, (String, JoinOrderDimEvidence)>,
) {
	for dimension in &evidence.ranking.dimensions {
		dimensions.insert(
			dimension.dimension.clone(),
			(evidence.ranking.probe.clone(), dimension.clone()),
		);
	}
	for branch in &evidence.branches {
		collect_snowflake_dimensions(&branch.evidence, dimensions);
	}
}

fn optimizer_analyze_join_order_verdict_row(
	evidence: &StarOrderEvidence,
	measured_rows_by_dimension: &BTreeMap<String, RowCount>,
) -> Result<Vec<ScalarValue>, BindingError> {
	let verdict = measured_star_order_verdict(evidence, measured_rows_by_dimension);
	Ok(vec![
		ScalarValue::Utf8(Some("join_order_verdict".to_string())),
		ScalarValue::Utf8(Some("star_path".to_string())),
		ScalarValue::Utf8(Some(star_order_verdict_label(verdict.verdict).to_string())),
		ScalarValue::Utf8(Some(star_order_verdict_detail_label(evidence, &verdict))),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
		ScalarValue::Float64(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
	])
}

fn optimizer_analyze_snowflake_verdict_row(level: &SnowflakeLevelVerdict) -> Vec<ScalarValue> {
	vec![
		ScalarValue::Utf8(Some("join_order_snowflake_verdict".to_string())),
		ScalarValue::Utf8(Some(format!("level={}", level.probe))),
		ScalarValue::Utf8(Some(
			snowflake_order_verdict_label(level.verdict).to_string(),
		)),
		ScalarValue::Utf8(Some(snowflake_order_verdict_detail_label(level))),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
		ScalarValue::Float64(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
	]
}

fn optimizer_analyze_snowflake_aggregate_verdict_row(
	verdict: &SnowflakeOrderVerdict,
) -> Vec<ScalarValue> {
	vec![
		ScalarValue::Utf8(Some("join_order_snowflake_verdict".to_string())),
		ScalarValue::Utf8(Some("snowflake".to_string())),
		ScalarValue::Utf8(Some(
			snowflake_order_verdict_label(verdict.aggregate).to_string(),
		)),
		ScalarValue::Utf8(Some(snowflake_order_aggregate_verdict_detail_label(
			verdict,
		))),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
		ScalarValue::Float64(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
	]
}

fn join_replay_id_for_trace_target(target: JoinTraceTarget) -> Result<JoinReplayId, BindingError> {
	u32::try_from(target.join_index)
		.map(JoinReplayId)
		.map_err(|_| BindingError::UnsupportedExpression {
			message: "optimizer analyze join replay id exceeds u32".to_string(),
		})
}

pub(super) fn optimizer_explain_row(
	receipt: &DecisionReceipt,
) -> Result<Vec<ScalarValue>, BindingError> {
	let (build_side, build_bytes, threshold_bytes, rejected_side_bytes) =
		optimizer_explain_build_columns(receipt)?;
	Ok(vec![
		ScalarValue::Utf8(Some("join".to_string())),
		ScalarValue::Utf8(Some(join_target_label(receipt.target)?)),
		ScalarValue::Utf8(Some(join_strategy_label(receipt.strategy).to_string())),
		ScalarValue::Utf8(Some(join_strategy_decision_detail_label(receipt))),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(build_side.map(str::to_string)),
		ScalarValue::Int64(build_bytes),
		ScalarValue::Int64(threshold_bytes),
		ScalarValue::Int64(rejected_side_bytes),
	])
}

pub(super) fn optimizer_explain_join_order_row(
	outcome: &StarOrderOutcome,
) -> Result<Vec<ScalarValue>, BindingError> {
	Ok(vec![
		ScalarValue::Utf8(Some("join_order".to_string())),
		ScalarValue::Utf8(Some(join_order_target_label(outcome)?)),
		ScalarValue::Utf8(Some(join_order_outcome_label(outcome).to_string())),
		ScalarValue::Utf8(Some(join_order_detail_label(outcome))),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
	])
}

pub(super) fn optimizer_explain_snowflake_order_row(
	evidence: &SnowflakeOrderEvidence,
) -> Result<Vec<ScalarValue>, BindingError> {
	Ok(vec![
		ScalarValue::Utf8(Some("join_order_snowflake".to_string())),
		ScalarValue::Utf8(Some(snowflake_order_target_label(evidence))),
		ScalarValue::Utf8(Some(snowflake_order_outcome_label(evidence).to_string())),
		ScalarValue::Utf8(Some(snowflake_order_detail_label(evidence))),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
	])
}

fn optimizer_analyze_snowflake_order_row(evidence: &SnowflakeOrderEvidence) -> Vec<ScalarValue> {
	vec![
		ScalarValue::Utf8(Some("join_order_snowflake".to_string())),
		ScalarValue::Utf8(Some(snowflake_order_target_label(evidence))),
		ScalarValue::Utf8(Some(snowflake_order_outcome_label(evidence).to_string())),
		ScalarValue::Utf8(Some(snowflake_order_detail_label(evidence))),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
		ScalarValue::Float64(None),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
	]
}

pub(super) fn optimizer_explain_scan_filter_row(
	receipt: &ScanFilterReceipt,
	render_receipt: Option<&ScanFilterRenderReceipt>,
) -> Result<Vec<ScalarValue>, BindingError> {
	Ok(vec![
		ScalarValue::Utf8(Some("scan_filter".to_string())),
		ScalarValue::Utf8(Some(scan_filter_target_label(receipt.target)?)),
		ScalarValue::Utf8(Some(scan_filter_outcome_label(
			receipt.outcome,
			receipt.converted_pushdown,
		))),
		ScalarValue::Utf8(Some(profile_hint_label(receipt.profile_hint).to_string())),
		ScalarValue::Utf8(render_receipt.and_then(|receipt| receipt.predicate_text.clone())),
		ScalarValue::Utf8(render_receipt.and_then(|receipt| receipt.pushed_predicate_text.clone())),
		ScalarValue::Utf8(
			render_receipt.and_then(|receipt| receipt.residual_predicate_text.clone()),
		),
		ScalarValue::Int64(
			receipt
				.estimated_rows
				.map(|rows| row_count_to_i64(rows, "estimated_rows"))
				.transpose()?,
		),
		ScalarValue::Utf8(receipt.spent_fact.map(spent_fact_label).map(str::to_string)),
		ScalarValue::Utf8(receipt.spent_column.map(column_ordinal_label).transpose()?),
		ScalarValue::Utf8(render_receipt.and_then(|receipt| receipt.spent_column_name.clone())),
		ScalarValue::Utf8(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
		ScalarValue::Int64(None),
	])
}

pub(super) fn optimizer_explain_build_columns(
	receipt: &DecisionReceipt,
) -> Result<(Option<&'static str>, Option<i64>, Option<i64>, Option<i64>), BindingError> {
	match receipt.reason {
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side,
			build_bytes,
			threshold,
			rejected_side_bytes,
		} => Ok((
			Some(broadcast_build_side_label(build_side)),
			Some(byte_count_to_i64(build_bytes, "build_bytes")?),
			Some(byte_count_to_i64(threshold, "threshold_bytes")?),
			rejected_side_bytes
				.map(|bytes| byte_count_to_i64(bytes, "rejected_side_bytes"))
				.transpose()?,
		)),
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side,
			estimated_byte_size,
			threshold,
			rejected_side_bytes,
		} => Ok((
			Some(broadcast_build_side_label(build_side)),
			Some(byte_count_to_i64(estimated_byte_size, "build_bytes")?),
			Some(byte_count_to_i64(threshold, "threshold_bytes")?),
			rejected_side_bytes
				.map(|bytes| byte_count_to_i64(bytes, "rejected_side_bytes"))
				.transpose()?,
		)),
		JoinStrategyDecisionReason::PinnedByAuthor
			if receipt.strategy == JoinStrategy::Broadcast =>
		{
			Ok((
				Some(match receipt.placement {
					JoinPhysicalPlacement::Natural => "left",
					JoinPhysicalPlacement::SwappedRightBuild => "right",
				}),
				None,
				None,
				None,
			))
		}
		JoinStrategyDecisionReason::PinnedByAuthor
		| JoinStrategyDecisionReason::HashByDefault { .. } => Ok((None, None, None, None)),
	}
}

pub(super) fn join_strategy_label(strategy: JoinStrategy) -> &'static str {
	match strategy {
		JoinStrategy::HashPartitioned => "hash_partitioned",
		JoinStrategy::Broadcast => "broadcast",
	}
}

pub(super) fn join_target_label(target: JoinTraceTarget) -> Result<String, BindingError> {
	Ok(format!(
		"join[{}]",
		usize_to_i64(target.join_index, "join_index")?
	))
}

pub(super) fn join_physical_placement_label(placement: JoinPhysicalPlacement) -> &'static str {
	match placement {
		JoinPhysicalPlacement::Natural => "natural",
		JoinPhysicalPlacement::SwappedRightBuild => "swapped_right_build",
	}
}

pub(super) fn join_strategy_decision_reason_label(
	reason: &JoinStrategyDecisionReason,
) -> &'static str {
	match reason {
		JoinStrategyDecisionReason::PinnedByAuthor => "pinned_by_author",
		JoinStrategyDecisionReason::BroadcastInferred { .. } => "broadcast_inferred",
		JoinStrategyDecisionReason::BroadcastEstimated { .. } => "broadcast_estimated",
		JoinStrategyDecisionReason::HashByDefault { .. } => "hash_by_default",
	}
}

pub(super) fn join_strategy_decision_reason_detail(
	reason: &JoinStrategyDecisionReason,
) -> Option<&'static str> {
	match reason {
		JoinStrategyDecisionReason::HashByDefault { reason } => {
			Some(hash_by_default_reason_label(*reason))
		}
		JoinStrategyDecisionReason::PinnedByAuthor
		| JoinStrategyDecisionReason::BroadcastInferred { .. }
		| JoinStrategyDecisionReason::BroadcastEstimated { .. } => None,
	}
}

pub(super) fn join_strategy_decision_detail_label(receipt: &DecisionReceipt) -> String {
	let placement = join_physical_placement_label(receipt.placement);
	let reason = join_strategy_decision_reason_label(&receipt.reason);
	match join_strategy_decision_reason_detail(&receipt.reason) {
		Some(detail) => format!("{placement}:{reason}:{detail}"),
		None => format!("{placement}:{reason}"),
	}
}

pub(super) fn hash_by_default_reason_label(reason: HashByDefaultReason) -> &'static str {
	match reason {
		HashByDefaultReason::AutoBroadcastDisabled => "auto_broadcast_disabled",
		HashByDefaultReason::NonInnerJoin => "non_inner_join",
		HashByDefaultReason::LeftInputNotSingleBaseRelation => {
			"left_input_not_single_base_relation"
		}
		HashByDefaultReason::RightInputNotSingleBaseRelation => {
			"right_input_not_single_base_relation"
		}
		HashByDefaultReason::ProbeNotScanLinear => "probe_not_scan_linear",
		HashByDefaultReason::MissingBuildByteUpperBound => "missing_build_byte_upper_bound",
		HashByDefaultReason::BuildByteUpperBoundAboveThreshold => {
			"build_byte_upper_bound_above_threshold"
		}
		HashByDefaultReason::HashAfterBroadcastCap => "hash_after_broadcast_cap",
	}
}

pub(super) fn join_order_target_label(outcome: &StarOrderOutcome) -> Result<String, BindingError> {
	Ok(match outcome {
		StarOrderOutcome::Recognized(evidence) => format!("star[probe={}]", evidence.probe),
		StarOrderOutcome::Declined(decline) => format!(
			"star_candidate[edges={}]",
			usize_to_i64(decline.edge_count, "join_order_edge_count")?
		),
	})
}

pub(super) fn join_order_outcome_label(outcome: &StarOrderOutcome) -> &'static str {
	match outcome {
		StarOrderOutcome::Recognized(evidence) => star_order_action_label(evidence.action),
		StarOrderOutcome::Declined(decline) => star_order_decline_outcome_label(decline.reason),
	}
}

pub(super) fn join_order_detail_label(outcome: &StarOrderOutcome) -> String {
	match outcome {
		StarOrderOutcome::Recognized(evidence) => format!(
			"strategy={}; dimensions=[{}]; declared_order=[{}]; cost_preferred_order=[{}]; applied_order=[{}]; declared_cost={}; preferred_cost={}; confidence={}; soft_margin={}/{}; estimates=[{}]",
			star_order_strategy_label(evidence.strategy),
			evidence
				.dimensions
				.iter()
				.map(|dim| dim.dimension.as_str())
				.collect::<Vec<_>>()
				.join(","),
			evidence.declared_order.join(","),
			evidence.cost_preferred_order.join(","),
			evidence.applied_order.join(","),
			evidence.declared_cost,
			evidence.preferred_cost,
			join_order_estimate_confidence_label(evidence.confidence),
			STAR_ORDER_SOFT_MARGIN_NUMERATOR,
			STAR_ORDER_SOFT_MARGIN_DENOMINATOR,
			evidence
				.dimensions
				.iter()
				.map(|dim| format!(
					"{}:{}:{}:{}",
					dim.dimension,
					dim.estimated_rows.get(),
					join_order_estimate_source_label(dim.estimate_source),
					join_order_estimate_confidence_label(dim.confidence)
				))
				.collect::<Vec<_>>()
				.join(",")
		),
		StarOrderOutcome::Declined(decline) => {
			format!(
				"reason={}; edges={}",
				star_order_decline_reason_label(decline.reason),
				decline.edge_count
			)
		}
	}
}

pub(super) fn snowflake_order_target_label(evidence: &SnowflakeOrderEvidence) -> String {
	format!("snowflake[probe={}]", evidence.ranking.probe)
}

pub(super) fn snowflake_order_outcome_label(evidence: &SnowflakeOrderEvidence) -> &'static str {
	if snowflake_order_would_reorder(evidence) {
		"would_reorder"
	} else {
		"would_keep_declared"
	}
}

fn snowflake_order_would_reorder(evidence: &SnowflakeOrderEvidence) -> bool {
	evidence.ranking.action == StarOrderAction::Reordered
		|| evidence
			.branches
			.iter()
			.any(|branch| snowflake_order_would_reorder(&branch.evidence))
}

pub(super) fn snowflake_order_detail_label(evidence: &SnowflakeOrderEvidence) -> String {
	snowflake_order_level_detail_label(evidence)
}

fn snowflake_order_level_detail_label(evidence: &SnowflakeOrderEvidence) -> String {
	let ranking = &evidence.ranking;
	format!(
		"root={}; action={}; dimensions=[{}]; declared_order=[{}]; cost_preferred_order=[{}]; declared_cost={}; preferred_cost={}; confidence={}; soft_margin={}/{}; estimates=[{}]; strategies=[{}]; branches=[{}]",
		ranking.probe,
		snowflake_order_action_label(ranking.action),
		ranking
			.dimensions
			.iter()
			.map(|dim| dim.dimension.as_str())
			.collect::<Vec<_>>()
			.join(","),
		ranking.declared_order.join(","),
		ranking.cost_preferred_order.join(","),
		ranking.declared_cost,
		ranking.preferred_cost,
		join_order_estimate_confidence_label(ranking.confidence),
		STAR_ORDER_SOFT_MARGIN_NUMERATOR,
		STAR_ORDER_SOFT_MARGIN_DENOMINATOR,
		ranking
			.dimensions
			.iter()
			.map(|dim| format!(
				"{}:{}:{}:{}",
				dim.dimension,
				dim.estimated_rows.get(),
				join_order_estimate_source_label(dim.estimate_source),
				join_order_estimate_confidence_label(dim.confidence)
			))
			.collect::<Vec<_>>()
			.join(","),
		evidence
			.edge_strategies
			.iter()
			.map(snowflake_edge_strategy_label)
			.collect::<Vec<_>>()
			.join(","),
		evidence
			.branches
			.iter()
			.map(|branch| format!(
				"{}:{{{}}}",
				branch.dimension,
				snowflake_order_level_detail_label(&branch.evidence)
			))
			.collect::<Vec<_>>()
			.join("|")
	)
}

fn snowflake_edge_strategy_label(strategy: &SnowflakeEdgeStrategy) -> String {
	format!(
		"{}:{}/{}:scan_linear={}",
		strategy.dimension,
		join_strategy_label(strategy.strategy),
		join_physical_placement_label(strategy.placement),
		strategy.probe_scan_linear
	)
}

fn snowflake_order_action_label(action: StarOrderAction) -> &'static str {
	match action {
		StarOrderAction::Reordered => "would_reorder",
		StarOrderAction::KeptAlreadyOptimal | StarOrderAction::KeptInsufficientMargin => {
			"would_keep_declared"
		}
	}
}

fn snowflake_order_verdict_label(verdict: SnowflakeOrderVerdictKind) -> &'static str {
	match verdict {
		SnowflakeOrderVerdictKind::Vindicated => "vindicated",
		SnowflakeOrderVerdictKind::MissedWin => "missed_win",
		SnowflakeOrderVerdictKind::Indeterminate => "indeterminate",
	}
}

fn snowflake_order_recommendation_label(
	recommendation: SnowflakeOrderRecommendation,
) -> &'static str {
	match recommendation {
		SnowflakeOrderRecommendation::KeepDeclared => "KEEP_DECLARED",
		SnowflakeOrderRecommendation::WouldReorder => "WOULD_REORDER",
		SnowflakeOrderRecommendation::NoRecommendation => "NO_RECOMMENDATION",
	}
}

fn snowflake_order_verdict_detail_label(level: &SnowflakeLevelVerdict) -> String {
	let measured = level.rerank.measured_ranking.as_ref();
	let measured_action = measured
		.map(|ranking| star_order_action_label(ranking.action))
		.unwrap_or("missing");
	let measured_declared_cost = measured
		.map(|ranking| ranking.declared_cost.to_string())
		.unwrap_or_else(|| "missing".to_string());
	let measured_preferred_cost = measured
		.map(|ranking| ranking.preferred_cost.to_string())
		.unwrap_or_else(|| "missing".to_string());
	format!(
		"estimated_action={}; measured_action={}; recommendation={}; declared_order=[{}]; estimated_preferred_order=[{}]; measured_applied_order=[{}]; estimated_declared_cost={}; estimated_preferred_cost={}; measured_declared_cost={}; measured_preferred_cost={}; accuracy=[{}]; strategies=[{}]",
		star_order_action_label(level.estimated_ranking.action),
		measured_action,
		snowflake_order_recommendation_label(level.recommendation),
		level.estimated_ranking.declared_order.join(","),
		level.estimated_ranking.cost_preferred_order.join(","),
		level.rerank.measured_applied_order.join(","),
		level.estimated_ranking.declared_cost,
		level.estimated_ranking.preferred_cost,
		measured_declared_cost,
		measured_preferred_cost,
		level
			.rerank
			.accuracy
			.iter()
			.map(star_order_dim_accuracy_label)
			.collect::<Vec<_>>()
			.join(","),
		level
			.strategies
			.iter()
			.map(snowflake_edge_strategy_label)
			.collect::<Vec<_>>()
			.join(",")
	)
}

fn snowflake_order_aggregate_verdict_detail_label(verdict: &SnowflakeOrderVerdict) -> String {
	format!(
		"recommendation={}; levels=[{}]",
		snowflake_order_recommendation_label(verdict.recommendation),
		verdict
			.levels
			.iter()
			.map(|level| format!(
				"{}:{}",
				level.probe,
				snowflake_order_verdict_label(level.verdict)
			))
			.collect::<Vec<_>>()
			.join(",")
	)
}

pub(super) fn star_order_strategy_label(strategy: StarOrderStrategy) -> &'static str {
	match strategy {
		StarOrderStrategy::Broadcast => "broadcast",
		StarOrderStrategy::HashPartitioned => "hash_partitioned",
	}
}

pub(super) fn star_order_action_label(action: StarOrderAction) -> &'static str {
	match action {
		StarOrderAction::Reordered => "reordered",
		StarOrderAction::KeptAlreadyOptimal => "kept_already_optimal",
		StarOrderAction::KeptInsufficientMargin => "kept_insufficient_margin",
	}
}

pub(super) fn star_order_verdict_label(verdict: StarOrderVerdict) -> &'static str {
	match verdict {
		StarOrderVerdict::Vindicated => "vindicated",
		StarOrderVerdict::MisRanked => "mis_ranked",
		StarOrderVerdict::MissedWin => "missed_win",
		StarOrderVerdict::OverEager => "over_eager",
		StarOrderVerdict::Indeterminate => "indeterminate",
	}
}

pub(super) fn star_order_verdict_recommendation_label(
	recommendation: StarOrderVerdictRecommendation,
) -> &'static str {
	match recommendation {
		StarOrderVerdictRecommendation::KeepApplied => "KEEP_APPLIED",
		StarOrderVerdictRecommendation::WouldReorder => "WOULD_REORDER",
		StarOrderVerdictRecommendation::WouldKeepDeclared => "WOULD_KEEP_DECLARED",
		StarOrderVerdictRecommendation::NoRecommendation => "NO_RECOMMENDATION",
	}
}

fn star_order_verdict_detail_label(
	evidence: &StarOrderEvidence,
	verdict: &StarOrderVerdictReceipt,
) -> String {
	let measured = verdict.measured_evidence.as_ref();
	let measured_action = measured
		.map(|evidence| star_order_action_label(evidence.action))
		.unwrap_or("missing");
	let measured_declared_cost = measured
		.map(|evidence| evidence.declared_cost.to_string())
		.unwrap_or_else(|| "missing".to_string());
	let measured_preferred_cost = measured
		.map(|evidence| evidence.preferred_cost.to_string())
		.unwrap_or_else(|| "missing".to_string());
	format!(
		"strategy={}; applied_order=[{}]; measured_applied_order=[{}]; original_action={}; measured_action={}; recommendation={}; original_declared_cost={}; original_preferred_cost={}; measured_declared_cost={}; measured_preferred_cost={}; accuracy=[{}]",
		star_order_strategy_label(evidence.strategy),
		evidence.applied_order.join(","),
		verdict.measured_applied_order.join(","),
		star_order_action_label(evidence.action),
		measured_action,
		star_order_verdict_recommendation_label(verdict.recommendation),
		evidence.declared_cost,
		evidence.preferred_cost,
		measured_declared_cost,
		measured_preferred_cost,
		verdict
			.accuracy
			.iter()
			.map(star_order_dim_accuracy_label)
			.collect::<Vec<_>>()
			.join(",")
	)
}

fn star_order_dim_accuracy_label(accuracy: &StarOrderDimAccuracy) -> String {
	let actual = accuracy
		.actual_rows
		.map(|rows| rows.get().to_string())
		.unwrap_or_else(|| "missing".to_string());
	let ratio = accuracy
		.actual_rows
		.and_then(|actual| {
			(accuracy.estimated_rows.get() > 0)
				.then_some(actual.get() as f64 / accuracy.estimated_rows.get() as f64)
		})
		.map(|value| format!("{value:.6}"))
		.unwrap_or_else(|| "missing".to_string());
	format!(
		"{}:estimated={}:actual={}:ratio={}:accuracy={}",
		accuracy.dimension,
		accuracy.estimated_rows.get(),
		actual,
		ratio,
		star_order_dim_accuracy_kind_label(accuracy)
	)
}

fn star_order_dim_accuracy_kind_label(accuracy: &StarOrderDimAccuracy) -> &'static str {
	let Some(actual_rows) = accuracy.actual_rows else {
		return "missing";
	};
	if actual_rows == accuracy.estimated_rows {
		"accurate"
	} else if actual_rows.get() > accuracy.estimated_rows.get() {
		"under_estimate"
	} else {
		"over_estimate"
	}
}

pub(super) fn star_order_decline_outcome_label(reason: StarOrderDeclineReason) -> &'static str {
	match reason {
		StarOrderDeclineReason::MixedStrategyStar => "declined_mixed_strategy_star",
		StarOrderDeclineReason::NonInnerEquiJoin => "declined_non_inner_equi_join",
		StarOrderDeclineReason::MissingRelationLabel => "declined_missing_relation_label",
		StarOrderDeclineReason::MultipleProbeRelations => "declined_multiple_probe_relations",
		StarOrderDeclineReason::DuplicateDimension => "declined_duplicate_dimension",
		StarOrderDeclineReason::MissingEstimate => "declined_missing_estimate",
		StarOrderDeclineReason::ProbeNotScanLinear => "declined_probe_not_scan_linear",
	}
}

pub(super) fn join_order_estimate_confidence_label(
	confidence: JoinOrderEstimateConfidence,
) -> &'static str {
	match confidence {
		JoinOrderEstimateConfidence::Hard => "hard",
		JoinOrderEstimateConfidence::Soft => "soft",
	}
}

pub(super) fn join_order_estimate_source_label(source: JoinOrderEstimateSource) -> &'static str {
	match source {
		JoinOrderEstimateSource::SingleKeyNdvContainment => "single_key_ndv_containment",
		JoinOrderEstimateSource::MultiKeyNdvContainment => "multi_key_ndv_containment",
		JoinOrderEstimateSource::Missing => "missing",
	}
}

pub(super) fn star_order_decline_reason_label(reason: StarOrderDeclineReason) -> &'static str {
	match reason {
		StarOrderDeclineReason::MixedStrategyStar => "mixed_strategy_star",
		StarOrderDeclineReason::NonInnerEquiJoin => "non_inner_equi_join",
		StarOrderDeclineReason::MissingRelationLabel => "missing_relation_label",
		StarOrderDeclineReason::MultipleProbeRelations => "multiple_probe_relations",
		StarOrderDeclineReason::DuplicateDimension => "duplicate_dimension",
		StarOrderDeclineReason::MissingEstimate => "missing_estimate",
		StarOrderDeclineReason::ProbeNotScanLinear => "probe_not_scan_linear",
	}
}

pub(super) fn broadcast_build_side_label(side: BroadcastBuildSide) -> &'static str {
	match side {
		BroadcastBuildSide::Left => "left",
		BroadcastBuildSide::Right => "right",
	}
}

pub(super) fn scan_filter_target_label(target: ScanTraceTarget) -> Result<String, BindingError> {
	let scan = usize_to_i64(target.scan_index, "scan_index")?;
	Ok(match target.table_id {
		Some(table_id) => format!("scan[{scan}] table_id={}", table_id.0),
		None => format!("scan[{scan}]"),
	})
}

pub(super) fn scan_filter_outcome_label(
	outcome: ScanFilterOutcome,
	converted_pushdown: bool,
) -> String {
	let label = match outcome {
		ScanFilterOutcome::FullyPushed => "fully_pushed",
		ScanFilterOutcome::PartiallyPushed => "partially_pushed",
		ScanFilterOutcome::ResidualOnly => "residual_only",
		ScanFilterOutcome::StaticElided => "static_elided",
		ScanFilterOutcome::PointRead => "point_read",
	};
	if converted_pushdown {
		format!("{label}:converted")
	} else {
		label.to_string()
	}
}

pub(super) fn profile_hint_label(profile_hint: QueryProfileHint) -> &'static str {
	match profile_hint {
		QueryProfileHint::PointRead => "point_read",
		QueryProfileHint::Scan => "scan",
	}
}

pub(super) fn spent_fact_label(source: FilterEstimateSource) -> &'static str {
	match source {
		FilterEstimateSource::Range => "range",
		FilterEstimateSource::ConvertedRange => "converted_range",
		FilterEstimateSource::NdvEquality => "ndv_equality",
		FilterEstimateSource::NdvInList => "ndv_in_list",
		FilterEstimateSource::HeavyHitterTopK => "heavy_hitter_top_k",
	}
}

pub(super) fn column_ordinal_label(column: ColumnOrdinal) -> Result<String, BindingError> {
	Ok(format!("col[{}]", i64::from(column.0)))
}

pub(super) fn byte_count_to_i64(
	value: ByteCount,
	column: &'static str,
) -> Result<i64, BindingError> {
	i64::try_from(value.get()).map_err(|_| BindingError::UnsupportedExpression {
		message: format!("optimizer explain {column} exceeds i64::MAX"),
	})
}

pub(super) fn usize_to_i64(value: usize, column: &'static str) -> Result<i64, BindingError> {
	i64::try_from(value).map_err(|_| BindingError::UnsupportedExpression {
		message: format!("optimizer explain {column} exceeds i64::MAX"),
	})
}

pub(super) fn row_count_to_i64(value: RowCount, column: &'static str) -> Result<i64, BindingError> {
	i64::try_from(value.get()).map_err(|_| BindingError::UnsupportedExpression {
		message: format!("optimizer explain {column} exceeds i64::MAX"),
	})
}

pub(super) fn system_table_plan(
	kind: SystemTableKind,
	full_schema: SchemaRef,
	rows: Vec<Vec<ScalarValue>>,
	projection: Option<&Vec<String>>,
) -> Result<ExecutionPlan, BindingError> {
	let Some(projection) = projection else {
		return Ok(ExecutionPlan::SystemTable {
			kind,
			schema: full_schema,
			rows,
		});
	};

	let requested = projection
		.iter()
		.map(|column| resolve_column(column, &full_schema).map(|column| column.ordinal))
		.collect::<Result<Vec<_>, _>>()?;
	let schema = project_schema(&full_schema, &requested)?;
	let rows = project_scalar_rows(&rows, &requested)?;

	Ok(ExecutionPlan::SystemTable { kind, schema, rows })
}

pub(super) fn logical_type_spec_row(
	spec: LogicalTypeSpec,
) -> Result<Vec<ScalarValue>, BindingError> {
	Ok(vec![
		ScalarValue::Int64(Some(i64::from(spec.id.0))),
		ScalarValue::Utf8(Some(spec.name)),
		ScalarValue::Utf8(Some(sql_carrier_keyword(&spec.physical)?.to_string())),
		ScalarValue::Utf8(
			spec.refinement
				.as_ref()
				.map(render_refinement_check)
				.transpose()?,
		),
	])
}

pub(super) fn information_schema_table_rows(
	descriptors: &[TableSchemaDescriptor],
) -> Vec<Vec<ScalarValue>> {
	descriptors
		.iter()
		.map(|descriptor| {
			vec![
				ScalarValue::Utf8(Some(descriptor.namespace.clone())),
				ScalarValue::Utf8(Some(descriptor.table_name.clone())),
				ScalarValue::Utf8(Some("BASE TABLE".to_string())),
			]
		})
		.collect()
}

pub(super) fn information_schema_schemata_rows(
	descriptors: &[TableSchemaDescriptor],
) -> Vec<Vec<ScalarValue>> {
	descriptors
		.iter()
		.map(|descriptor| descriptor.namespace.clone())
		.collect::<BTreeSet<_>>()
		.into_iter()
		.map(|schema_name| vec![ScalarValue::Utf8(Some(schema_name))])
		.collect()
}

pub(super) fn information_schema_column_rows(
	descriptors: &[TableSchemaDescriptor],
	logical_types: &LogicalTypeRegistry,
) -> Result<Vec<Vec<ScalarValue>>, BindingError> {
	let mut rows = Vec::new();
	for descriptor in descriptors {
		let schema =
			descriptor
				.arrow_schema()
				.map_err(|error| BindingError::UnsupportedExpression {
					message: format!(
						"failed to decode table descriptor schema for {}.{}: {error}",
						descriptor.namespace, descriptor.table_name
					),
				})?;
		for (index, field) in schema.fields().iter().enumerate() {
			let ordinal =
				i64::try_from(index + 1).map_err(|_| BindingError::UnsupportedExpression {
					message: "information_schema.columns ordinal overflow".to_string(),
				})?;
			rows.push(vec![
				ScalarValue::Utf8(Some(descriptor.namespace.clone())),
				ScalarValue::Utf8(Some(descriptor.table_name.clone())),
				ScalarValue::Utf8(Some(field.name().clone())),
				ScalarValue::Int64(Some(ordinal)),
				ScalarValue::Utf8(Some(sql_carrier_keyword(field.data_type())?.to_string())),
				ScalarValue::Utf8(Some(
					if field.is_nullable() { "YES" } else { "NO" }.to_string(),
				)),
				ScalarValue::Utf8(logical_type_display_name(field, logical_types)?),
			]);
		}
	}
	Ok(rows)
}

pub(super) fn logical_type_display_name(
	field: &Field,
	logical_types: &LogicalTypeRegistry,
) -> Result<Option<String>, BindingError> {
	let logical_type = decode_column_logical_type_with_resolver(
		field,
		LogicalTypeResolver::with_registry(logical_types),
	)
	.map_err(|error| BindingError::UnsupportedExpression {
		message: error.to_string(),
	})?;
	if logical_type.is_naked() {
		return Ok(None);
	}
	LogicalTypeResolver::with_registry(logical_types)
		.spec(logical_type)
		.map(|spec| Some(spec.name))
		.ok_or_else(|| BindingError::UnsupportedExpression {
			message: format!(
				"logical type metadata for column {} references unknown id {}",
				field.name(),
				logical_type.0
			),
		})
}

pub(super) fn sql_carrier_keyword(data_type: &DataType) -> Result<&'static str, BindingError> {
	match data_type {
		DataType::Int32 => Ok("INT"),
		DataType::Int64 => Ok("BIGINT"),
		DataType::Date32 => Ok("DATE"),
		DataType::Timestamp(_, Some(timezone)) if timezone.as_ref() == "UTC" => Ok("TIMESTAMPTZ"),
		DataType::Float32 => Ok("REAL"),
		DataType::Float64 => Ok("DOUBLE"),
		DataType::Utf8 => Ok("TEXT"),
		DataType::Boolean => Ok("BOOLEAN"),
		other => Err(BindingError::UnsupportedExpression {
			message: format!("unsupported SQL carrier for information_schema: {other:?}"),
		}),
	}
}

pub(super) fn render_refinement_check(
	predicate: &LogicalTypeRefinementPredicate,
) -> Result<String, BindingError> {
	Ok(format!(
		"CHECK ({})",
		render_refinement_predicate(predicate)?
	))
}

pub(super) fn render_refinement_predicate(
	predicate: &LogicalTypeRefinementPredicate,
) -> Result<String, BindingError> {
	match predicate {
		LogicalTypeRefinementPredicate::Comparison { left, op, right } => Ok(format!(
			"{} {} {}",
			render_refinement_value(left)?,
			refinement_op_sql(*op),
			render_refinement_value(right)?
		)),
		LogicalTypeRefinementPredicate::And(predicates) => predicates
			.iter()
			.map(render_refinement_predicate)
			.collect::<Result<Vec<_>, _>>()
			.map(|parts| parts.join(" AND ")),
	}
}

pub(super) fn render_refinement_value(
	value: &LogicalTypeRefinementValue,
) -> Result<String, BindingError> {
	match value {
		LogicalTypeRefinementValue::Value => Ok("VALUE".to_string()),
		LogicalTypeRefinementValue::Int64(value) => Ok(value.to_string()),
		LogicalTypeRefinementValue::Int32(value) => Ok(value.to_string()),
		LogicalTypeRefinementValue::Float64(bits) => {
			let value = f64::from_bits(*bits);
			if value.is_finite() {
				Ok(value.to_string())
			} else {
				Err(BindingError::UnsupportedExpression {
					message: "logical type FLOAT64 refinement literal is not finite".to_string(),
				})
			}
		}
		LogicalTypeRefinementValue::Float32(bits) => {
			let value = f32::from_bits(*bits);
			if value.is_finite() {
				Ok(value.to_string())
			} else {
				Err(BindingError::UnsupportedExpression {
					message: "logical type FLOAT32 refinement literal is not finite".to_string(),
				})
			}
		}
		LogicalTypeRefinementValue::Utf8(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
		LogicalTypeRefinementValue::Boolean(value) => {
			Ok(if *value { "TRUE" } else { "FALSE" }.to_string())
		}
	}
}

pub(super) fn refinement_op_sql(op: LogicalTypeRefinementComparisonOp) -> &'static str {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => "=",
		LogicalTypeRefinementComparisonOp::Ne => "!=",
		LogicalTypeRefinementComparisonOp::Lt => "<",
		LogicalTypeRefinementComparisonOp::Le => "<=",
		LogicalTypeRefinementComparisonOp::Gt => ">",
		LogicalTypeRefinementComparisonOp::Ge => ">=",
	}
}

pub(super) fn project_scalar_rows(
	rows: &[Vec<ScalarValue>],
	requested: &[ColumnOrdinal],
) -> Result<Vec<Vec<ScalarValue>>, BindingError> {
	rows.iter()
		.map(|row| {
			requested
				.iter()
				.map(|column| {
					row.get(column.0 as usize).cloned().ok_or_else(|| {
						BindingError::UnsupportedExpression {
							message: format!(
								"internal: system table projection column {} out of range",
								column.0
							),
						}
					})
				})
				.collect::<Result<Vec<_>, _>>()
		})
		.collect()
}

pub(super) type OrdinalRemap = HashMap<ColumnOrdinal, ColumnOrdinal>;
