use super::*;

pub(super) type JoinOrderSourceMap = BTreeMap<String, JoinOrderRelationFacts>;

pub(super) const STAR_ORDER_SOFT_MARGIN_NUMERATOR: u128 = 4;
pub(super) const STAR_ORDER_SOFT_MARGIN_DENOMINATOR: u128 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JoinOrderRelationFacts {
	pub(super) rows: Option<RowCount>,
	pub(super) row_confidence: JoinOrderEstimateConfidence,
	pub(super) column_ndv: HashMap<String, DistinctCountFact>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinOrderEstimateConfidence {
	Hard,
	Soft,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinOrderEstimateSource {
	SingleKeyNdvContainment,
	MultiKeyNdvContainment,
	Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JoinOrderEdgeEvidence {
	pub(super) target: JoinTraceTarget,
	pub(super) probe: Option<String>,
	pub(super) dimension: Option<String>,
	pub(super) declared_position: usize,
	pub(super) estimated_rows: Option<RowCount>,
	pub(super) estimate_source: JoinOrderEstimateSource,
	pub(super) inner_equi: bool,
	pub(super) probe_scan_linear: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JoinOrderDimEvidence {
	pub(super) dimension: String,
	pub(super) declared_position: usize,
	pub(super) estimated_rows: RowCount,
	pub(super) estimate_source: JoinOrderEstimateSource,
	pub(super) confidence: JoinOrderEstimateConfidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StarOrderAction {
	Reordered,
	KeptAlreadyOptimal,
	KeptInsufficientMargin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderEvidence {
	pub(super) probe: String,
	pub(super) dimensions: Vec<JoinOrderDimEvidence>,
	pub(super) declared_order: Vec<String>,
	pub(super) cost_preferred_order: Vec<String>,
	pub(super) applied_order: Vec<String>,
	pub(super) declared_cost: u128,
	pub(super) preferred_cost: u128,
	pub(super) orders_agree: bool,
	pub(super) action: StarOrderAction,
	pub(super) confidence: JoinOrderEstimateConfidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StarOrderDeclineReason {
	NotBroadcastStar,
	NonInnerEquiJoin,
	MissingRelationLabel,
	MultipleProbeRelations,
	DuplicateDimension,
	MissingEstimate,
	ProbeNotScanLinear,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderDecline {
	pub(super) reason: StarOrderDeclineReason,
	pub(super) edge_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StarOrderOutcome {
	Recognized(StarOrderEvidence),
	Declined(StarOrderDecline),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StarOrderVerdict {
	Vindicated,
	MisRanked,
	MissedWin,
	OverEager,
	Indeterminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StarOrderVerdictRecommendation {
	KeepApplied,
	WouldReorder,
	WouldKeepDeclared,
	NoRecommendation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderDimAccuracy {
	pub(super) dimension: String,
	pub(super) estimated_rows: RowCount,
	pub(super) actual_rows: Option<RowCount>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderVerdictReceipt {
	pub(super) measured_applied_order: Vec<String>,
	pub(super) verdict: StarOrderVerdict,
	pub(super) recommendation: StarOrderVerdictRecommendation,
	pub(super) accuracy: Vec<StarOrderDimAccuracy>,
	pub(super) measured_evidence: Option<StarOrderEvidence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinOrderSourceCapture {
	Off,
	On,
}

impl JoinOrderSourceCapture {
	pub(super) fn enabled(self) -> bool {
		matches!(self, Self::On)
	}
}

pub(super) struct StarJoinCandidate<'a> {
	pub(super) probe: &'a LogicalPlan,
	pub(super) dimensions: Vec<StarJoinDimension<'a>>,
}

pub(super) struct StarJoinDimension<'a> {
	pub(super) input: &'a LogicalPlan,
	pub(super) on: &'a [(Expr, Expr)],
	pub(super) strategy_intent: JoinStrategyIntent,
	pub(super) declared_position: usize,
}

pub(super) fn extract_star_join_candidate(plan: &LogicalPlan) -> Option<StarJoinCandidate<'_>> {
	let mut current = plan;
	let mut dimensions = Vec::new();
	loop {
		match current {
			LogicalPlan::Join {
				left,
				right,
				kind,
				on,
				strategy_intent,
			} if *kind == JoinKind::Inner
				&& *strategy_intent == JoinStrategyIntent::Auto
				&& !logical_plan_contains_join(right) =>
			{
				dimensions.push(StarJoinDimension {
					input: right,
					on,
					strategy_intent: *strategy_intent,
					declared_position: 0,
				});
				current = left;
			}
			_ => break,
		}
	}
	if dimensions.len() < 2 || logical_plan_contains_join(current) {
		return None;
	}
	dimensions.reverse();
	for (declared_position, dimension) in dimensions.iter_mut().enumerate() {
		dimension.declared_position = declared_position;
	}
	Some(StarJoinCandidate {
		probe: current,
		dimensions,
	})
}

fn logical_plan_contains_join(plan: &LogicalPlan) -> bool {
	match plan {
		LogicalPlan::Join { .. } => true,
		LogicalPlan::Alias { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Take { input, .. }
		| LogicalPlan::Explain { input }
		| LogicalPlan::ExplainAnalyze { input }
		| LogicalPlan::CreateTableAsSelect { source: input, .. }
		| LogicalPlan::InsertSelect { source: input, .. } => logical_plan_contains_join(input),
		LogicalPlan::Scan { .. }
		| LogicalPlan::ScanNamed { .. }
		| LogicalPlan::CreateTable { .. }
		| LogicalPlan::CreateLogicalType { .. }
		| LogicalPlan::DropTable { .. }
		| LogicalPlan::Delete { .. }
		| LogicalPlan::Update { .. }
		| LogicalPlan::Insert { .. } => false,
	}
}

pub(super) fn relation_key_label(table: &TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
	}
}

pub(super) fn join_order_sources_for_scan(
	qualifier: &TableRef,
	schema: &SchemaRef,
	projection: Option<&[ColumnOrdinal]>,
	facts: &TableOptimizerFacts,
) -> JoinOrderSourceMap {
	let mut column_ndv = HashMap::new();
	for (output_index, field) in schema.fields().iter().enumerate() {
		let output_ordinal =
			ColumnOrdinal(u32::try_from(output_index).expect("join order source ordinal overflow"));
		let descriptor_ordinal = projection
			.and_then(|columns| columns.get(output_index).copied())
			.unwrap_or(output_ordinal);
		if let Some(distinct_count) = facts.column_ndv.get(&descriptor_ordinal) {
			column_ndv.insert(field.name().clone(), *distinct_count);
		}
	}
	let rows = estimate_row_source(facts.row_upper_bound, None);
	let row_confidence = if facts.row_upper_bound.is_some() {
		JoinOrderEstimateConfidence::Hard
	} else {
		JoinOrderEstimateConfidence::Soft
	};
	BTreeMap::from([(
		relation_key_label(qualifier),
		JoinOrderRelationFacts {
			rows,
			row_confidence,
			column_ndv,
		},
	)])
}

pub(super) fn alias_join_order_sources(
	sources: Option<JoinOrderSourceMap>,
	alias: &TableRef,
) -> Option<JoinOrderSourceMap> {
	let mut sources = sources?;
	if sources.len() != 1 {
		return None;
	}
	let (_, facts) = sources.pop_first().expect("single source");
	Some(BTreeMap::from([(relation_key_label(alias), facts)]))
}

pub(super) fn update_single_source_join_order_rows(
	sources: Option<JoinOrderSourceMap>,
	scope: &RelationScope,
	rows: Option<RowCount>,
) -> Option<JoinOrderSourceMap> {
	let mut sources = sources?;
	let label = scope.unique_source_label()?;
	if let Some(facts) = sources.get_mut(&label) {
		if facts.rows.is_none() && rows.is_some() {
			facts.row_confidence = JoinOrderEstimateConfidence::Soft;
		}
		facts.rows = rows.or(facts.rows);
	}
	Some(sources)
}

pub(super) fn merge_join_order_sources(
	left: Option<JoinOrderSourceMap>,
	right: Option<JoinOrderSourceMap>,
) -> Option<JoinOrderSourceMap> {
	match (left, right) {
		(Some(mut left), Some(right)) => {
			for (label, facts) in right {
				if left.insert(label, facts).is_some() {
					return None;
				}
			}
			Some(left)
		}
		(Some(left), None) => Some(left),
		(None, Some(right)) => Some(right),
		(None, None) => None,
	}
}

pub(super) fn relation_label_for_join_keys(
	scope: &RelationScope,
	keys: &[ResolvedJoinKey],
) -> Option<String> {
	let mut labels = BTreeSet::new();
	for key in keys {
		labels.insert(scope.source_label_for_ordinal(key.column.ordinal)?);
	}
	if labels.len() == 1 {
		labels.into_iter().next()
	} else {
		None
	}
}

pub(super) fn join_order_estimate_source(
	left_keys: &[ResolvedJoinKey],
	right_keys: &[ResolvedJoinKey],
	estimated_rows: Option<RowCount>,
) -> JoinOrderEstimateSource {
	if estimated_rows.is_none() {
		return JoinOrderEstimateSource::Missing;
	}
	if left_keys.len() == 1 && right_keys.len() == 1 {
		JoinOrderEstimateSource::SingleKeyNdvContainment
	} else if left_keys.len() == right_keys.len() && left_keys.len() >= 2 {
		JoinOrderEstimateSource::MultiKeyNdvContainment
	} else {
		JoinOrderEstimateSource::Missing
	}
}

pub(super) fn join_order_edge_estimate(
	left_sources: Option<&JoinOrderSourceMap>,
	right_sources: Option<&JoinOrderSourceMap>,
	probe: Option<&str>,
	dimension: Option<&str>,
	left_keys: &[ResolvedJoinKey],
	right_keys: &[ResolvedJoinKey],
	actual_join_estimate: Option<RowCount>,
) -> (Option<RowCount>, JoinOrderEstimateSource) {
	if actual_join_estimate.is_some() {
		return (
			actual_join_estimate,
			join_order_estimate_source(left_keys, right_keys, actual_join_estimate),
		);
	}
	// Later left-deep star edges see a composite left input, so the normal output
	// estimate can fail open. This evidence-only fallback reuses carried base
	// row/NDV facts and never feeds PlanOptimizerFacts or strategy selection.
	let Some(estimated_rows) = estimate_join_order_edge_from_sources(
		left_sources,
		right_sources,
		probe,
		dimension,
		left_keys,
		right_keys,
	) else {
		return (None, JoinOrderEstimateSource::Missing);
	};
	(
		Some(estimated_rows),
		join_order_estimate_source(left_keys, right_keys, Some(estimated_rows)),
	)
}

pub(super) fn join_order_estimate_confidence(
	left_sources: Option<&JoinOrderSourceMap>,
	right_sources: Option<&JoinOrderSourceMap>,
	probe: Option<&str>,
	dimension: Option<&str>,
	left_keys: &[ResolvedJoinKey],
	right_keys: &[ResolvedJoinKey],
) -> JoinOrderEstimateConfidence {
	let Some(probe) = probe else {
		return JoinOrderEstimateConfidence::Soft;
	};
	let Some(dimension) = dimension else {
		return JoinOrderEstimateConfidence::Soft;
	};
	let Some(left_facts) = left_sources.and_then(|sources| sources.get(probe)) else {
		return JoinOrderEstimateConfidence::Soft;
	};
	let Some(right_facts) = right_sources.and_then(|sources| sources.get(dimension)) else {
		return JoinOrderEstimateConfidence::Soft;
	};
	if left_facts.row_confidence != JoinOrderEstimateConfidence::Hard
		|| right_facts.row_confidence != JoinOrderEstimateConfidence::Hard
	{
		return JoinOrderEstimateConfidence::Soft;
	}
	for (left_key, right_key) in left_keys.iter().zip(right_keys) {
		let Some(left_ndv) = left_facts.column_ndv.get(&left_key.column.name.0) else {
			return JoinOrderEstimateConfidence::Soft;
		};
		let Some(right_ndv) = right_facts.column_ndv.get(&right_key.column.name.0) else {
			return JoinOrderEstimateConfidence::Soft;
		};
		if !matches!(left_ndv, DistinctCountFact::Exact(_))
			|| !matches!(right_ndv, DistinctCountFact::Exact(_))
		{
			return JoinOrderEstimateConfidence::Soft;
		}
	}
	JoinOrderEstimateConfidence::Hard
}

pub(super) fn join_order_overall_confidence(
	dimensions: &[JoinOrderDimEvidence],
) -> JoinOrderEstimateConfidence {
	if dimensions
		.iter()
		.all(|dim| dim.confidence == JoinOrderEstimateConfidence::Hard)
	{
		JoinOrderEstimateConfidence::Hard
	} else {
		JoinOrderEstimateConfidence::Soft
	}
}

fn estimate_join_order_edge_from_sources(
	left_sources: Option<&JoinOrderSourceMap>,
	right_sources: Option<&JoinOrderSourceMap>,
	probe: Option<&str>,
	dimension: Option<&str>,
	left_keys: &[ResolvedJoinKey],
	right_keys: &[ResolvedJoinKey],
) -> Option<RowCount> {
	if left_keys.len() != right_keys.len() || left_keys.is_empty() {
		return None;
	}
	let probe = probe?;
	let dimension = dimension?;
	let left_facts = left_sources?.get(probe)?;
	let right_facts = right_sources?.get(dimension)?;
	let left_rows = left_facts.rows?;
	let right_rows = right_facts.rows?;
	if left_keys.len() == 1 {
		let left_ndv = *left_facts.column_ndv.get(&left_keys[0].column.name.0)?;
		let right_ndv = *right_facts.column_ndv.get(&right_keys[0].column.name.0)?;
		return ndv_containment_join_rows(left_rows, right_rows, left_ndv, right_ndv);
	}

	let numerator = u128::from(left_rows.get()) * u128::from(right_rows.get());
	let mut denominator = 1_u128;
	for (left_key, right_key) in left_keys.iter().zip(right_keys) {
		let left_ndv = left_facts
			.column_ndv
			.get(&left_key.column.name.0)?
			.point_estimate();
		let right_ndv = right_facts
			.column_ndv
			.get(&right_key.column.name.0)?
			.point_estimate();
		denominator = multiply_denominator_capped_at_numerator(
			denominator,
			u128::from(left_ndv.max(right_ndv)),
			numerator,
		)?;
	}
	ndv_containment_join_rows_from_denominator(left_rows, right_rows, denominator)
}

pub(super) fn analyze(edges: &[JoinOrderEdgeEvidence]) -> Option<StarOrderOutcome> {
	if edges.len() < 2 {
		return None;
	}
	if edges.iter().any(|edge| !edge.inner_equi) {
		return Some(StarOrderOutcome::Declined(StarOrderDecline {
			reason: StarOrderDeclineReason::NonInnerEquiJoin,
			edge_count: edges.len(),
		}));
	}
	if edges.iter().any(|edge| !edge.probe_scan_linear) {
		return Some(StarOrderOutcome::Declined(StarOrderDecline {
			reason: StarOrderDeclineReason::ProbeNotScanLinear,
			edge_count: edges.len(),
		}));
	}

	let Some(first_probe) = edges.first().and_then(|edge| edge.probe.as_ref()) else {
		return Some(StarOrderOutcome::Declined(StarOrderDecline {
			reason: StarOrderDeclineReason::MissingRelationLabel,
			edge_count: edges.len(),
		}));
	};
	if edges
		.iter()
		.any(|edge| edge.probe.as_deref() != Some(first_probe.as_str()))
	{
		return Some(StarOrderOutcome::Declined(StarOrderDecline {
			reason: StarOrderDeclineReason::MultipleProbeRelations,
			edge_count: edges.len(),
		}));
	}

	let mut dimensions = Vec::with_capacity(edges.len());
	let mut seen_dimensions = BTreeSet::new();
	for edge in edges {
		let Some(dimension) = &edge.dimension else {
			return Some(StarOrderOutcome::Declined(StarOrderDecline {
				reason: StarOrderDeclineReason::MissingRelationLabel,
				edge_count: edges.len(),
			}));
		};
		if !seen_dimensions.insert(dimension.clone()) {
			return Some(StarOrderOutcome::Declined(StarOrderDecline {
				reason: StarOrderDeclineReason::DuplicateDimension,
				edge_count: edges.len(),
			}));
		}
		let Some(estimated_rows) = edge.estimated_rows else {
			return Some(StarOrderOutcome::Declined(StarOrderDecline {
				reason: StarOrderDeclineReason::MissingEstimate,
				edge_count: edges.len(),
			}));
		};
		dimensions.push(JoinOrderDimEvidence {
			dimension: dimension.clone(),
			declared_position: edge.declared_position,
			estimated_rows,
			estimate_source: edge.estimate_source,
			confidence: JoinOrderEstimateConfidence::Soft,
		});
	}

	Some(recognized_star_order_outcome(
		first_probe.clone(),
		dimensions,
		None,
		JoinOrderEstimateConfidence::Soft,
	))
}

pub(super) fn recognized_star_order_outcome(
	probe: String,
	dimensions: Vec<JoinOrderDimEvidence>,
	applied_order_override: Option<Vec<String>>,
	confidence: JoinOrderEstimateConfidence,
) -> StarOrderOutcome {
	let declared_order = dimensions
		.iter()
		.map(|dim| dim.dimension.clone())
		.collect::<Vec<_>>();
	let mut preferred_dims = dimensions.clone();
	preferred_dims.sort_by(|left, right| {
		left.estimated_rows
			.cmp(&right.estimated_rows)
			.then(left.declared_position.cmp(&right.declared_position))
	});
	let cost_preferred_order = preferred_dims
		.iter()
		.map(|dim| dim.dimension.clone())
		.collect::<Vec<_>>();
	let declared_cost = order_cost(&dimensions);
	let preferred_cost = order_cost(&preferred_dims);
	let orders_agree = declared_order == cost_preferred_order;
	let action =
		classify_star_order_action(declared_cost, preferred_cost, orders_agree, confidence);
	let applied_order = applied_order_override.unwrap_or_else(|| declared_order.clone());

	StarOrderOutcome::Recognized(StarOrderEvidence {
		probe,
		dimensions,
		declared_order,
		cost_preferred_order,
		applied_order,
		declared_cost,
		preferred_cost,
		orders_agree,
		action,
		confidence,
	})
}

pub(super) fn measured_star_order_verdict(
	evidence: &StarOrderEvidence,
	measured_rows_by_dimension: &BTreeMap<String, RowCount>,
) -> StarOrderVerdictReceipt {
	let mut measured_dimensions = Vec::with_capacity(evidence.dimensions.len());
	let mut accuracy = Vec::with_capacity(evidence.dimensions.len());
	let mut missing_measurement = false;
	for dimension in &evidence.dimensions {
		let actual_rows = measured_rows_by_dimension
			.get(&dimension.dimension)
			.copied();
		accuracy.push(StarOrderDimAccuracy {
			dimension: dimension.dimension.clone(),
			estimated_rows: dimension.estimated_rows,
			actual_rows,
		});
		let Some(actual_rows) = actual_rows else {
			missing_measurement = true;
			continue;
		};
		measured_dimensions.push(JoinOrderDimEvidence {
			dimension: dimension.dimension.clone(),
			declared_position: dimension.declared_position,
			estimated_rows: actual_rows,
			estimate_source: dimension.estimate_source,
			confidence: dimension.confidence,
		});
	}
	if missing_measurement || measured_dimensions.len() != evidence.dimensions.len() {
		return StarOrderVerdictReceipt {
			measured_applied_order: Vec::new(),
			verdict: StarOrderVerdict::Indeterminate,
			recommendation: StarOrderVerdictRecommendation::NoRecommendation,
			accuracy,
			measured_evidence: None,
		};
	}

	let measured = recognized_star_order_outcome(
		evidence.probe.clone(),
		measured_dimensions.clone(),
		None,
		evidence.confidence,
	);
	let StarOrderOutcome::Recognized(measured) = measured else {
		unreachable!("recognized_star_order_outcome always recognizes");
	};
	let measured_applied_order = applied_order_for_action(&measured);
	let measured = recognized_star_order_outcome(
		evidence.probe.clone(),
		measured_dimensions,
		Some(measured_applied_order.clone()),
		evidence.confidence,
	);
	let StarOrderOutcome::Recognized(measured) = measured else {
		unreachable!("recognized_star_order_outcome always recognizes");
	};

	let verdict = if evidence.applied_order == measured.applied_order {
		StarOrderVerdict::Vindicated
	} else if evidence.action == StarOrderAction::Reordered
		&& measured.action == StarOrderAction::Reordered
	{
		StarOrderVerdict::MisRanked
	} else if evidence.action == StarOrderAction::Reordered {
		StarOrderVerdict::OverEager
	} else if measured.action == StarOrderAction::Reordered {
		StarOrderVerdict::MissedWin
	} else {
		StarOrderVerdict::Indeterminate
	};
	let recommendation = match verdict {
		StarOrderVerdict::Vindicated => StarOrderVerdictRecommendation::KeepApplied,
		StarOrderVerdict::MisRanked | StarOrderVerdict::MissedWin => {
			StarOrderVerdictRecommendation::WouldReorder
		}
		StarOrderVerdict::OverEager => StarOrderVerdictRecommendation::WouldKeepDeclared,
		StarOrderVerdict::Indeterminate => StarOrderVerdictRecommendation::NoRecommendation,
	};

	StarOrderVerdictReceipt {
		measured_applied_order,
		verdict,
		recommendation,
		accuracy,
		measured_evidence: Some(measured),
	}
}

fn applied_order_for_action(evidence: &StarOrderEvidence) -> Vec<String> {
	if evidence.action == StarOrderAction::Reordered {
		evidence.cost_preferred_order.clone()
	} else {
		evidence.declared_order.clone()
	}
}

pub(super) fn classify_star_order_action(
	declared_cost: u128,
	preferred_cost: u128,
	orders_agree: bool,
	confidence: JoinOrderEstimateConfidence,
) -> StarOrderAction {
	if orders_agree {
		return StarOrderAction::KeptAlreadyOptimal;
	}
	match confidence {
		JoinOrderEstimateConfidence::Hard => StarOrderAction::Reordered,
		JoinOrderEstimateConfidence::Soft => {
			if preferred_cost.saturating_mul(STAR_ORDER_SOFT_MARGIN_DENOMINATOR)
				<= declared_cost.saturating_mul(STAR_ORDER_SOFT_MARGIN_NUMERATOR)
			{
				StarOrderAction::Reordered
			} else {
				StarOrderAction::KeptInsufficientMargin
			}
		}
	}
}

pub(super) fn order_cost(dimensions: &[JoinOrderDimEvidence]) -> u128 {
	let len = dimensions.len();
	dimensions
		.iter()
		.enumerate()
		.map(|(idx, dim)| {
			let downstream_weight =
				u128::try_from(len - idx).expect("join order cost weight overflow");
			downstream_weight * u128::from(dim.estimated_rows.get())
		})
		.sum()
}
