use super::*;

pub(super) type JoinOrderSourceMap = BTreeMap<String, JoinOrderRelationFacts>;

pub(super) const STAR_ORDER_SOFT_MARGIN_NUMERATOR: u128 = 4;
pub(super) const STAR_ORDER_SOFT_MARGIN_DENOMINATOR: u128 = 5;
pub(crate) const JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE: u64 = 20;
pub(crate) const JOIN_ORDER_MARGIN_ADVISORY_HYSTERESIS_NUMERATOR: u64 = 1;
pub(crate) const JOIN_ORDER_MARGIN_ADVISORY_HYSTERESIS_DENOMINATOR: u64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SoftMarginLadderStep {
	Tightest,
	Tight,
	Baseline,
	Loose,
	Loosest,
}

impl SoftMarginLadderStep {
	fn tighter(self) -> Self {
		match self {
			Self::Tightest => Self::Tightest,
			Self::Tight => Self::Tightest,
			Self::Baseline => Self::Tight,
			Self::Loose => Self::Baseline,
			Self::Loosest => Self::Loose,
		}
	}

	fn looser(self) -> Self {
		match self {
			Self::Tightest => Self::Tight,
			Self::Tight => Self::Baseline,
			Self::Baseline => Self::Loose,
			Self::Loose => Self::Loosest,
			Self::Loosest => Self::Loosest,
		}
	}

	fn margin(self) -> SoftMargin {
		match self {
			Self::Tightest => SoftMargin::new(3, 5),
			Self::Tight => SoftMargin::new(7, 10),
			Self::Baseline => SoftMargin::new(
				STAR_ORDER_SOFT_MARGIN_NUMERATOR,
				STAR_ORDER_SOFT_MARGIN_DENOMINATOR,
			),
			Self::Loose => SoftMargin::new(9, 10),
			Self::Loosest => SoftMargin::new(19, 20),
		}
	}
}

pub(crate) const CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP: SoftMarginLadderStep =
	SoftMarginLadderStep::Baseline;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SoftMargin {
	numerator: u128,
	denominator: u128,
}

impl SoftMargin {
	const fn new(numerator: u128, denominator: u128) -> Self {
		assert!(numerator > 0);
		assert!(denominator > 0);
		assert!(numerator <= denominator);
		Self {
			numerator,
			denominator,
		}
	}

	pub(crate) fn label(self) -> String {
		format!("{}/{}", self.numerator, self.denominator)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarginRecommendation {
	Loosen,
	Tighten,
	Keep,
	InsufficientEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MarginAdvisory {
	pub(crate) current: SoftMargin,
	pub(crate) recommended: SoftMargin,
	pub(crate) recommendation: MarginRecommendation,
	pub(crate) missed_win: u64,
	pub(crate) over_eager: u64,
	pub(crate) vindicated: u64,
	pub(crate) mis_ranked: u64,
	pub(crate) indeterminate: u64,
	pub(crate) sample_size: u64,
}

pub(crate) fn recommend_soft_margin(
	snapshot: &[OptimizerRuntimeVerdictRow],
	current: SoftMarginLadderStep,
) -> MarginAdvisory {
	let mut missed_win = 0_u64;
	let mut over_eager = 0_u64;
	let mut vindicated = 0_u64;
	let mut mis_ranked = 0_u64;
	let mut indeterminate = 0_u64;
	for row in snapshot {
		match row.verdict {
			OptimizerRuntimeVerdictClass::MissedWin => {
				missed_win = missed_win.saturating_add(row.count);
			}
			OptimizerRuntimeVerdictClass::OverEager => {
				over_eager = over_eager.saturating_add(row.count);
			}
			OptimizerRuntimeVerdictClass::Vindicated => {
				vindicated = vindicated.saturating_add(row.count);
			}
			OptimizerRuntimeVerdictClass::MisRanked => {
				mis_ranked = mis_ranked.saturating_add(row.count);
			}
			OptimizerRuntimeVerdictClass::Indeterminate => {
				indeterminate = indeterminate.saturating_add(row.count);
			}
		}
	}
	let sample_size = missed_win
		.saturating_add(over_eager)
		.saturating_add(vindicated);
	let (recommendation, recommended_step) = if sample_size < JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE
	{
		(MarginRecommendation::InsufficientEvidence, current)
	} else {
		let net = i128::from(missed_win) - i128::from(over_eager);
		let threshold = i128::from(sample_size)
			.saturating_mul(i128::from(JOIN_ORDER_MARGIN_ADVISORY_HYSTERESIS_NUMERATOR));
		let scaled_net = net.saturating_mul(i128::from(
			JOIN_ORDER_MARGIN_ADVISORY_HYSTERESIS_DENOMINATOR,
		));
		if scaled_net > threshold {
			(MarginRecommendation::Loosen, current.looser())
		} else if scaled_net < threshold.saturating_neg() {
			(MarginRecommendation::Tighten, current.tighter())
		} else {
			(MarginRecommendation::Keep, current)
		}
	};
	MarginAdvisory {
		current: current.margin(),
		recommended: recommended_step.margin(),
		recommendation,
		missed_win,
		over_eager,
		vindicated,
		mis_ranked,
		indeterminate,
		sample_size,
	}
}

pub(crate) fn margin_recommendation_label(recommendation: MarginRecommendation) -> &'static str {
	match recommendation {
		MarginRecommendation::Loosen => "loosen",
		MarginRecommendation::Tighten => "tighten",
		MarginRecommendation::Keep => "keep",
		MarginRecommendation::InsufficientEvidence => "insufficient_evidence",
	}
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StarOrderStrategy {
	Broadcast,
	HashPartitioned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderRanking {
	pub(super) probe: String,
	pub(super) dimensions: Vec<JoinOrderDimEvidence>,
	pub(super) declared_order: Vec<String>,
	pub(super) cost_preferred_order: Vec<String>,
	pub(super) declared_cost: u128,
	pub(super) preferred_cost: u128,
	pub(super) orders_agree: bool,
	pub(super) action: StarOrderAction,
	pub(super) confidence: JoinOrderEstimateConfidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderEvidence {
	pub(super) probe: String,
	pub(super) dimensions: Vec<JoinOrderDimEvidence>,
	pub(super) declared_order: Vec<String>,
	pub(super) cost_preferred_order: Vec<String>,
	pub(super) applied_order: Vec<String>,
	pub(super) strategy: StarOrderStrategy,
	pub(super) declared_cost: u128,
	pub(super) preferred_cost: u128,
	pub(super) orders_agree: bool,
	pub(super) action: StarOrderAction,
	pub(super) confidence: JoinOrderEstimateConfidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StarOrderDeclineReason {
	MixedStrategyStar,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeOrderEvidence {
	pub(super) ranking: StarOrderRanking,
	pub(super) edge_strategies: Vec<SnowflakeEdgeStrategy>,
	pub(super) branches: Vec<SnowflakeBranch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeBranch {
	pub(super) dimension: String,
	pub(super) evidence: SnowflakeOrderEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeEdgeStrategy {
	pub(super) dimension: String,
	pub(super) strategy: JoinStrategy,
	pub(super) placement: JoinPhysicalPlacement,
	pub(super) probe_scan_linear: bool,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MeasuredOrderRerank {
	pub(super) measured_applied_order: Vec<String>,
	pub(super) accuracy: Vec<StarOrderDimAccuracy>,
	pub(super) measured_ranking: Option<StarOrderRanking>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SnowflakeOrderVerdictKind {
	Vindicated,
	MissedWin,
	Indeterminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SnowflakeOrderRecommendation {
	KeepDeclared,
	WouldReorder,
	NoRecommendation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeOrderVerdict {
	pub(super) levels: Vec<SnowflakeLevelVerdict>,
	pub(super) aggregate: SnowflakeOrderVerdictKind,
	pub(super) recommendation: SnowflakeOrderRecommendation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnowflakeLevelVerdict {
	pub(super) probe: String,
	pub(super) estimated_ranking: StarOrderRanking,
	pub(super) strategies: Vec<SnowflakeEdgeStrategy>,
	pub(super) rerank: MeasuredOrderRerank,
	pub(super) verdict: SnowflakeOrderVerdictKind,
	pub(super) recommendation: SnowflakeOrderRecommendation,
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
		StarOrderStrategy::Broadcast,
		JoinOrderEstimateConfidence::Soft,
	))
}

pub(super) fn analyze_snowflake(
	edges: &[JoinOrderEdgeEvidence],
	decisions: &[DecisionReceipt],
) -> Option<SnowflakeOrderEvidence> {
	if edges.len() < 2 {
		return None;
	}

	let mut adjacency = BTreeMap::<String, Vec<usize>>::new();
	let mut probes = BTreeSet::new();
	let mut dimensions = BTreeSet::new();
	let mut dimension_parent = BTreeMap::<String, String>::new();
	for (edge_index, edge) in edges.iter().enumerate() {
		if !edge.inner_equi {
			return None;
		}
		let probe = edge.probe.as_ref()?;
		let dimension = edge.dimension.as_ref()?;
		if probe == dimension || edge.estimated_rows.is_none() {
			return None;
		}
		let decision = decisions.get(edge.target.join_index)?;
		if decision.target != edge.target {
			return None;
		}
		if dimension_parent
			.insert(dimension.clone(), probe.clone())
			.is_some()
		{
			return None;
		}
		adjacency.entry(probe.clone()).or_default().push(edge_index);
		probes.insert(probe.clone());
		dimensions.insert(dimension.clone());
	}

	if !dimensions
		.iter()
		.any(|dimension| probes.contains(dimension))
	{
		return None;
	}
	let roots = probes
		.iter()
		.filter(|probe| !dimensions.contains(*probe))
		.collect::<Vec<_>>();
	if roots.len() != 1 {
		return None;
	}
	let root = roots[0].as_str();

	let mut visited_nodes = BTreeSet::new();
	let mut visited_edges = BTreeSet::new();
	let evidence = snowflake_order_level(
		root,
		edges,
		decisions,
		&adjacency,
		&mut visited_nodes,
		&mut visited_edges,
	)?;
	if visited_edges.len() != edges.len() {
		return None;
	}
	Some(evidence)
}

fn snowflake_order_level(
	probe: &str,
	edges: &[JoinOrderEdgeEvidence],
	decisions: &[DecisionReceipt],
	adjacency: &BTreeMap<String, Vec<usize>>,
	visited_nodes: &mut BTreeSet<String>,
	visited_edges: &mut BTreeSet<usize>,
) -> Option<SnowflakeOrderEvidence> {
	if !visited_nodes.insert(probe.to_string()) {
		return None;
	}
	let direct_edges = adjacency.get(probe)?;
	let mut dimensions = Vec::with_capacity(direct_edges.len());
	let mut edge_strategies = Vec::with_capacity(direct_edges.len());
	for (declared_position, edge_index) in direct_edges.iter().copied().enumerate() {
		if !visited_edges.insert(edge_index) {
			return None;
		}
		let edge = edges.get(edge_index)?;
		let dimension = edge.dimension.as_ref()?;
		let decision = decisions.get(edge.target.join_index)?;
		if decision.target != edge.target {
			return None;
		}
		dimensions.push(JoinOrderDimEvidence {
			dimension: dimension.clone(),
			declared_position,
			estimated_rows: edge.estimated_rows?,
			estimate_source: edge.estimate_source,
			confidence: JoinOrderEstimateConfidence::Soft,
		});
		edge_strategies.push(SnowflakeEdgeStrategy {
			dimension: dimension.clone(),
			strategy: decision.strategy,
			placement: decision.placement,
			probe_scan_linear: edge.probe_scan_linear,
		});
	}

	let ranking = rank_star_order(
		probe.to_string(),
		dimensions,
		JoinOrderEstimateConfidence::Soft,
	);
	let mut branches = Vec::new();
	for edge_index in direct_edges.iter().copied() {
		let dimension = edges.get(edge_index)?.dimension.as_ref()?;
		if adjacency.contains_key(dimension) {
			branches.push(SnowflakeBranch {
				dimension: dimension.clone(),
				evidence: snowflake_order_level(
					dimension,
					edges,
					decisions,
					adjacency,
					visited_nodes,
					visited_edges,
				)?,
			});
		}
	}

	Some(SnowflakeOrderEvidence {
		ranking,
		edge_strategies,
		branches,
	})
}

pub(super) fn recognized_star_order_outcome(
	probe: String,
	dimensions: Vec<JoinOrderDimEvidence>,
	applied_order_override: Option<Vec<String>>,
	strategy: StarOrderStrategy,
	confidence: JoinOrderEstimateConfidence,
) -> StarOrderOutcome {
	let ranking = rank_star_order(probe, dimensions, confidence);
	let applied_order = applied_order_override.unwrap_or_else(|| ranking.declared_order.clone());

	StarOrderOutcome::Recognized(StarOrderEvidence {
		probe: ranking.probe,
		dimensions: ranking.dimensions,
		declared_order: ranking.declared_order,
		cost_preferred_order: ranking.cost_preferred_order,
		applied_order,
		strategy,
		declared_cost: ranking.declared_cost,
		preferred_cost: ranking.preferred_cost,
		orders_agree: ranking.orders_agree,
		action: ranking.action,
		confidence: ranking.confidence,
	})
}

pub(super) fn rank_star_order(
	probe: String,
	dimensions: Vec<JoinOrderDimEvidence>,
	confidence: JoinOrderEstimateConfidence,
) -> StarOrderRanking {
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
	StarOrderRanking {
		probe,
		dimensions,
		declared_order,
		cost_preferred_order,
		declared_cost,
		preferred_cost,
		orders_agree,
		action,
		confidence,
	}
}

pub(super) fn measured_star_order_verdict(
	evidence: &StarOrderEvidence,
	measured_rows_by_dimension: &BTreeMap<String, RowCount>,
) -> StarOrderVerdictReceipt {
	let rerank = measured_order_rerank(
		&StarOrderRanking {
			probe: evidence.probe.clone(),
			dimensions: evidence.dimensions.clone(),
			declared_order: evidence.declared_order.clone(),
			cost_preferred_order: evidence.cost_preferred_order.clone(),
			declared_cost: evidence.declared_cost,
			preferred_cost: evidence.preferred_cost,
			orders_agree: evidence.orders_agree,
			action: evidence.action,
			confidence: evidence.confidence,
		},
		measured_rows_by_dimension,
	);
	let Some(measured_ranking) = rerank.measured_ranking.as_ref() else {
		return StarOrderVerdictReceipt {
			measured_applied_order: Vec::new(),
			verdict: StarOrderVerdict::Indeterminate,
			recommendation: StarOrderVerdictRecommendation::NoRecommendation,
			accuracy: rerank.accuracy,
			measured_evidence: None,
		};
	};

	let measured = star_order_evidence_from_ranking(
		measured_ranking.clone(),
		rerank.measured_applied_order.clone(),
		evidence.strategy,
	);

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
		measured_applied_order: rerank.measured_applied_order,
		verdict,
		recommendation,
		accuracy: rerank.accuracy,
		measured_evidence: Some(measured),
	}
}

pub(super) fn measured_order_rerank(
	ranking: &StarOrderRanking,
	measured_rows_by_dimension: &BTreeMap<String, RowCount>,
) -> MeasuredOrderRerank {
	let mut measured_dimensions = Vec::with_capacity(ranking.dimensions.len());
	let mut accuracy = Vec::with_capacity(ranking.dimensions.len());
	let mut missing_measurement = false;
	for dimension in &ranking.dimensions {
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
	if missing_measurement || measured_dimensions.len() != ranking.dimensions.len() {
		return MeasuredOrderRerank {
			measured_applied_order: Vec::new(),
			accuracy,
			measured_ranking: None,
		};
	}

	let measured_ranking = rank_star_order(
		ranking.probe.clone(),
		measured_dimensions,
		ranking.confidence,
	);
	let measured_applied_order = applied_order_for_ranking(&measured_ranking);
	MeasuredOrderRerank {
		measured_applied_order,
		accuracy,
		measured_ranking: Some(measured_ranking),
	}
}

pub(super) fn measured_snowflake_order_verdict(
	evidence: &SnowflakeOrderEvidence,
	measured_rows_by_dimension: &BTreeMap<String, RowCount>,
) -> SnowflakeOrderVerdict {
	let mut levels = Vec::new();
	collect_snowflake_order_verdicts(evidence, measured_rows_by_dimension, &mut levels);
	let aggregate = if levels
		.iter()
		.any(|level| level.verdict == SnowflakeOrderVerdictKind::Indeterminate)
	{
		SnowflakeOrderVerdictKind::Indeterminate
	} else if levels
		.iter()
		.any(|level| level.verdict == SnowflakeOrderVerdictKind::MissedWin)
	{
		SnowflakeOrderVerdictKind::MissedWin
	} else {
		SnowflakeOrderVerdictKind::Vindicated
	};
	let recommendation = snowflake_recommendation_for_verdict(aggregate);
	SnowflakeOrderVerdict {
		levels,
		aggregate,
		recommendation,
	}
}

fn collect_snowflake_order_verdicts(
	evidence: &SnowflakeOrderEvidence,
	measured_rows_by_dimension: &BTreeMap<String, RowCount>,
	levels: &mut Vec<SnowflakeLevelVerdict>,
) {
	let rerank = measured_order_rerank(&evidence.ranking, measured_rows_by_dimension);
	let verdict = match rerank
		.measured_ranking
		.as_ref()
		.map(|ranking| ranking.action)
	{
		None => SnowflakeOrderVerdictKind::Indeterminate,
		Some(StarOrderAction::Reordered) => SnowflakeOrderVerdictKind::MissedWin,
		Some(StarOrderAction::KeptAlreadyOptimal | StarOrderAction::KeptInsufficientMargin) => {
			SnowflakeOrderVerdictKind::Vindicated
		}
	};
	let recommendation = snowflake_recommendation_for_verdict(verdict);
	levels.push(SnowflakeLevelVerdict {
		probe: evidence.ranking.probe.clone(),
		estimated_ranking: evidence.ranking.clone(),
		strategies: evidence.edge_strategies.clone(),
		rerank,
		verdict,
		recommendation,
	});
	for branch in &evidence.branches {
		collect_snowflake_order_verdicts(&branch.evidence, measured_rows_by_dimension, levels);
	}
}

fn snowflake_recommendation_for_verdict(
	verdict: SnowflakeOrderVerdictKind,
) -> SnowflakeOrderRecommendation {
	match verdict {
		SnowflakeOrderVerdictKind::Vindicated => SnowflakeOrderRecommendation::KeepDeclared,
		SnowflakeOrderVerdictKind::MissedWin => SnowflakeOrderRecommendation::WouldReorder,
		SnowflakeOrderVerdictKind::Indeterminate => SnowflakeOrderRecommendation::NoRecommendation,
	}
}

fn star_order_evidence_from_ranking(
	ranking: StarOrderRanking,
	applied_order: Vec<String>,
	strategy: StarOrderStrategy,
) -> StarOrderEvidence {
	StarOrderEvidence {
		probe: ranking.probe,
		dimensions: ranking.dimensions,
		declared_order: ranking.declared_order,
		cost_preferred_order: ranking.cost_preferred_order,
		applied_order,
		strategy,
		declared_cost: ranking.declared_cost,
		preferred_cost: ranking.preferred_cost,
		orders_agree: ranking.orders_agree,
		action: ranking.action,
		confidence: ranking.confidence,
	}
}

fn applied_order_for_ranking(ranking: &StarOrderRanking) -> Vec<String> {
	if ranking.action == StarOrderAction::Reordered {
		ranking.cost_preferred_order.clone()
	} else {
		ranking.declared_order.clone()
	}
}

pub(super) fn uniform_star_order_strategy(
	decisions: &[DecisionReceipt],
) -> Option<StarOrderStrategy> {
	if decisions.is_empty() {
		return None;
	}
	if decisions.iter().all(|decision| {
		decision.strategy == JoinStrategy::Broadcast
			&& decision.placement == JoinPhysicalPlacement::SwappedRightBuild
	}) {
		return Some(StarOrderStrategy::Broadcast);
	}
	if decisions.iter().all(|decision| {
		decision.strategy == JoinStrategy::HashPartitioned
			&& decision.placement == JoinPhysicalPlacement::Natural
	}) {
		return Some(StarOrderStrategy::HashPartitioned);
	}
	None
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

#[derive(Debug, Clone)]
struct StarChildDimension<'a> {
	plan: ScopedExecutionPlan,
	on: &'a [(Expr, Expr)],
	strategy_intent: JoinStrategyIntent,
	label: String,
	declared_position: usize,
}

#[derive(Debug, Clone)]
struct BoundStarDimension<'a> {
	plan: ScopedExecutionPlan,
	on: &'a [(Expr, Expr)],
	strategy_intent: JoinStrategyIntent,
	label: String,
	evidence: JoinOrderDimEvidence,
}

#[derive(Debug, Clone)]
struct SnowflakeStarNode<'a> {
	dimension_index: usize,
	children: Vec<usize>,
	on: &'a [(Expr, Expr)],
	strategy_intent: JoinStrategyIntent,
	label: String,
	declared_position: usize,
}

#[derive(Debug, Clone)]
struct SnowflakeStarTree<'a> {
	root_children: Vec<usize>,
	nodes: Vec<SnowflakeStarNode<'a>>,
}

impl SnowflakeStarTree<'_> {
	fn has_branch_dimensions(&self) -> bool {
		self.nodes.iter().any(|node| !node.children.is_empty())
	}
}

#[derive(Debug, Clone)]
struct BuiltSnowflakeDimension<'a> {
	dimension: StarChildDimension<'a>,
	receipts: Vec<StarOrderRenderReceipt>,
}

#[derive(Debug, Clone)]
struct LocalStarActuation {
	plan: ScopedExecutionPlan,
	receipt: StarOrderRenderReceipt,
}

#[derive(Debug, Clone)]
pub(super) enum StarOrderActuationResult {
	NotCandidate,
	Actuated(ScopedExecutionPlan),
	Declined,
}

#[derive(Debug, Clone)]
enum LocalStarActuationAttempt {
	Actuated(LocalStarActuation),
	Declined(StarOrderDeclineReason),
}

#[derive(Debug, Clone)]
struct StarDimensionJoinEdge {
	left_resolved_keys: Vec<ResolvedJoinKey>,
	right_resolved_keys: Vec<ResolvedJoinKey>,
	left_label: Option<String>,
	right_label: Option<String>,
}

fn star_output_segments(
	probe_label: &str,
	probe_plan: &ScopedExecutionPlan,
	dimensions: &[BoundStarDimension<'_>],
) -> Vec<StarOutputSegment> {
	let mut segments = Vec::with_capacity(dimensions.len() + 1);
	segments.push(StarOutputSegment {
		label: probe_label.to_string(),
		width: probe_plan.schema().fields().len(),
	});
	segments.extend(dimensions.iter().map(|dimension| StarOutputSegment {
		label: dimension.label.clone(),
		width: dimension.plan.schema().fields().len(),
	}));
	segments
}

fn star_output_segments_for_order(
	probe_label: &str,
	probe_plan: &ScopedExecutionPlan,
	dimensions: &[BoundStarDimension<'_>],
	order: &[usize],
) -> Vec<StarOutputSegment> {
	let mut segments = Vec::with_capacity(order.len() + 1);
	segments.push(StarOutputSegment {
		label: probe_label.to_string(),
		width: probe_plan.schema().fields().len(),
	});
	segments.extend(order.iter().map(|idx| {
		let dimension = &dimensions[*idx];
		StarOutputSegment {
			label: dimension.label.clone(),
			width: dimension.plan.schema().fields().len(),
		}
	}));
	segments
}

impl<'a> Binder<'a> {
	pub(super) async fn try_bind_star_order_actuation(
		&self,
		plan: &LogicalPlan,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
	) -> Result<StarOrderActuationResult, BindingError> {
		let Some(candidate) = extract_star_join_candidate(plan) else {
			return Ok(StarOrderActuationResult::NotCandidate);
		};

		let mut scratch_trace = trace.clone();
		let mut scratch_render_trace = render_trace.clone();
		let probe_plan = self
			.bind_plan_with_trace(
				candidate.probe,
				&mut scratch_trace,
				&mut scratch_render_trace,
				JoinOrderSourceCapture::On,
			)
			.await?;
		let Some(probe_label) = probe_plan.scope.unique_source_label() else {
			self.record_optimizer_star_order_decline(
				trace,
				OptimizerDecisionKind::StarCandidate,
				StarOrderDeclineReason::MissingRelationLabel,
			);
			self.record_star_order_decline(
				render_trace,
				StarOrderDeclineReason::MissingRelationLabel,
				candidate.dimensions.len(),
			);
			return Ok(StarOrderActuationResult::Declined);
		};
		if !broadcast_probe_is_scan_linear(&probe_plan.plan) {
			self.record_optimizer_star_order_decline(
				trace,
				OptimizerDecisionKind::StarCandidate,
				StarOrderDeclineReason::ProbeNotScanLinear,
			);
			self.record_star_order_decline(
				render_trace,
				StarOrderDeclineReason::ProbeNotScanLinear,
				candidate.dimensions.len(),
			);
			return Ok(StarOrderActuationResult::Declined);
		}

		let mut dimension_inputs = Vec::with_capacity(candidate.dimensions.len());
		for dimension in &candidate.dimensions {
			let dimension_plan = self
				.bind_plan_with_trace(
					dimension.input,
					&mut scratch_trace,
					&mut scratch_render_trace,
					JoinOrderSourceCapture::On,
				)
				.await?;
			dimension_inputs.push(dimension_plan);
		}

		let Some(tree) = self.classify_snowflake_star_tree(
			&probe_plan,
			&probe_label,
			&candidate.dimensions,
			&dimension_inputs,
			trace,
			render_trace,
		)?
		else {
			return Ok(StarOrderActuationResult::Declined);
		};
		if tree.root_children.len() < 2 {
			self.record_optimizer_star_order_decline(
				trace,
				root_decision_kind(&tree),
				StarOrderDeclineReason::MissingRelationLabel,
			);
			self.record_star_order_decline(
				render_trace,
				StarOrderDeclineReason::MissingRelationLabel,
				candidate.dimensions.len(),
			);
			return Ok(StarOrderActuationResult::Declined);
		}

		let force_hash_root = tree.has_branch_dimensions();
		let Some((scratch_root_dimensions, _scratch_branch_receipts)) = self
			.bind_snowflake_root_dimensions(
				&probe_plan,
				&probe_label,
				&tree,
				&mut scratch_trace,
				&mut scratch_render_trace,
				&dimension_inputs,
			)?
		else {
			self.record_optimizer_star_order_decline(
				trace,
				root_decision_kind(&tree),
				StarOrderDeclineReason::MissingEstimate,
			);
			if !force_hash_root {
				self.record_star_order_decline(
					render_trace,
					StarOrderDeclineReason::MissingEstimate,
					candidate.dimensions.len(),
				);
			}
			return Ok(StarOrderActuationResult::Declined);
		};
		let scratch_root_attempt = self.bind_local_star_actuation(
			probe_plan.clone(),
			probe_label.clone(),
			&scratch_root_dimensions,
			force_hash_root,
			candidate.dimensions.len(),
			&mut scratch_trace,
			&mut scratch_render_trace,
		)?;
		if let LocalStarActuationAttempt::Declined(reason) = scratch_root_attempt {
			self.record_optimizer_star_order_decline(trace, root_decision_kind(&tree), reason);
			if !force_hash_root {
				self.record_star_order_decline(render_trace, reason, candidate.dimensions.len());
			}
			return Ok(StarOrderActuationResult::Declined);
		}

		let real_probe_plan = self
			.bind_plan_with_trace(
				candidate.probe,
				trace,
				render_trace,
				JoinOrderSourceCapture::On,
			)
			.await?;
		let mut real_dimension_inputs = Vec::with_capacity(candidate.dimensions.len());
		for dimension in &candidate.dimensions {
			let plan = self
				.bind_plan_with_trace(
					dimension.input,
					trace,
					render_trace,
					JoinOrderSourceCapture::On,
				)
				.await?;
			real_dimension_inputs.push(plan);
		}

		let Some((real_root_dimensions, branch_receipts)) = self.bind_snowflake_root_dimensions(
			&real_probe_plan,
			&probe_label,
			&tree,
			trace,
			render_trace,
			&real_dimension_inputs,
		)?
		else {
			return Ok(StarOrderActuationResult::Declined);
		};
		let real_root_attempt = self.bind_local_star_actuation(
			real_probe_plan,
			probe_label,
			&real_root_dimensions,
			force_hash_root,
			candidate.dimensions.len(),
			trace,
			render_trace,
		)?;
		let LocalStarActuationAttempt::Actuated(real_root) = real_root_attempt else {
			if let LocalStarActuationAttempt::Declined(reason) = real_root_attempt {
				self.record_optimizer_star_order_decline(trace, root_decision_kind(&tree), reason);
			}
			return Ok(StarOrderActuationResult::Declined);
		};

		self.record_optimizer_star_order_receipt(
			trace,
			root_decision_kind(&tree),
			&real_root.receipt,
		);
		for branch_receipt in &branch_receipts {
			self.record_optimizer_star_order_receipt(
				trace,
				OptimizerDecisionKind::SnowflakeBranch,
				branch_receipt,
			);
		}

		if let Some(render_trace) = render_trace.as_mut() {
			let mut receipts = Vec::with_capacity(branch_receipts.len() + 1);
			receipts.push(real_root.receipt.clone());
			receipts.extend(branch_receipts);
			render_trace.set_star_receipts(receipts);
		}
		Ok(StarOrderActuationResult::Actuated(real_root.plan))
	}

	fn classify_snowflake_star_tree<'b>(
		&self,
		probe_plan: &ScopedExecutionPlan,
		probe_label: &str,
		candidate_dimensions: &[StarJoinDimension<'b>],
		dimension_inputs: &[ScopedExecutionPlan],
		trace: &OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
	) -> Result<Option<SnowflakeStarTree<'b>>, BindingError> {
		let mut root_children = Vec::new();
		let mut nodes = Vec::<SnowflakeStarNode<'b>>::with_capacity(candidate_dimensions.len());
		let mut seen_labels = BTreeSet::new();
		for (dimension_index, dimension) in candidate_dimensions.iter().enumerate() {
			let dimension_plan = &dimension_inputs[dimension_index];
			let mut matched_parent = None;
			if let Some(edge) =
				self.try_resolve_star_dimension_edge(probe_plan, dimension_plan, dimension.on)?
			{
				if edge.left_label.as_deref() == Some(probe_label) {
					let Some(dimension_label) = edge.right_label else {
						self.record_optimizer_star_order_decline(
							trace,
							OptimizerDecisionKind::StarCandidate,
							StarOrderDeclineReason::MissingRelationLabel,
						);
						self.record_star_order_decline(
							render_trace,
							StarOrderDeclineReason::MissingRelationLabel,
							candidate_dimensions.len(),
						);
						return Ok(None);
					};
					matched_parent = Some((None, dimension_label));
				}
			}

			if matched_parent.is_none() {
				for (parent_index, parent_node) in nodes.iter().enumerate() {
					let parent_plan = &dimension_inputs[parent_node.dimension_index];
					let Some(edge) = self.try_resolve_star_dimension_edge(
						parent_plan,
						dimension_plan,
						dimension.on,
					)?
					else {
						continue;
					};
					if edge.left_label.as_deref() != Some(parent_node.label.as_str()) {
						continue;
					}
					let Some(dimension_label) = edge.right_label else {
						self.record_optimizer_star_order_decline(
							trace,
							OptimizerDecisionKind::StarCandidate,
							StarOrderDeclineReason::MissingRelationLabel,
						);
						self.record_star_order_decline(
							render_trace,
							StarOrderDeclineReason::MissingRelationLabel,
							candidate_dimensions.len(),
						);
						return Ok(None);
					};
					matched_parent = Some((Some(parent_index), dimension_label));
					break;
				}
			}

			let Some((parent_index, dimension_label)) = matched_parent else {
				self.record_optimizer_star_order_decline(
					trace,
					OptimizerDecisionKind::StarCandidate,
					StarOrderDeclineReason::MissingRelationLabel,
				);
				self.record_star_order_decline(
					render_trace,
					StarOrderDeclineReason::MissingRelationLabel,
					candidate_dimensions.len(),
				);
				return Ok(None);
			};
			if !seen_labels.insert(dimension_label.clone()) {
				self.record_optimizer_star_order_decline(
					trace,
					OptimizerDecisionKind::StarCandidate,
					StarOrderDeclineReason::DuplicateDimension,
				);
				self.record_star_order_decline(
					render_trace,
					StarOrderDeclineReason::DuplicateDimension,
					candidate_dimensions.len(),
				);
				return Ok(None);
			}
			let node_index = nodes.len();
			nodes.push(SnowflakeStarNode {
				dimension_index,
				children: Vec::new(),
				on: dimension.on,
				strategy_intent: dimension.strategy_intent,
				label: dimension_label,
				declared_position: dimension.declared_position,
			});
			if let Some(parent_index) = parent_index {
				nodes[parent_index].children.push(node_index);
			} else {
				root_children.push(node_index);
			}
		}

		Ok(Some(SnowflakeStarTree {
			root_children,
			nodes,
		}))
	}

	fn bind_snowflake_root_dimensions<'b>(
		&self,
		probe_plan: &ScopedExecutionPlan,
		probe_label: &str,
		tree: &SnowflakeStarTree<'b>,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		bound_inputs: &[ScopedExecutionPlan],
	) -> Result<Option<(Vec<BoundStarDimension<'b>>, Vec<StarOrderRenderReceipt>)>, BindingError> {
		let mut dimensions = Vec::with_capacity(tree.root_children.len());
		let mut receipts = Vec::new();
		for node_index in &tree.root_children {
			let built = self.bind_snowflake_dimension(
				*node_index,
				tree,
				trace,
				render_trace,
				bound_inputs,
			)?;
			receipts.extend(built.receipts);
			let Some(dimension) =
				self.bound_star_dimension(probe_plan, probe_label, &built.dimension)?
			else {
				return Ok(None);
			};
			dimensions.push(dimension);
		}
		Ok(Some((dimensions, receipts)))
	}

	fn bind_snowflake_dimension<'b>(
		&self,
		node_index: usize,
		tree: &SnowflakeStarTree<'b>,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		bound_inputs: &[ScopedExecutionPlan],
	) -> Result<BuiltSnowflakeDimension<'b>, BindingError> {
		let node = &tree.nodes[node_index];
		let base_plan = bound_inputs[node.dimension_index].clone();
		let mut child_dimensions = Vec::with_capacity(node.children.len());
		let mut receipts = Vec::new();
		for child_index in &node.children {
			let child = self.bind_snowflake_dimension(
				*child_index,
				tree,
				trace,
				render_trace,
				bound_inputs,
			)?;
			receipts.extend(child.receipts);
			child_dimensions.push(child.dimension);
		}

		let plan = if child_dimensions.len() >= 2 {
			let mut bound_children = Vec::with_capacity(child_dimensions.len());
			let mut complete = true;
			for child in &child_dimensions {
				if let Some(bound_child) =
					self.bound_star_dimension(&base_plan, &node.label, child)?
				{
					bound_children.push(bound_child);
				} else {
					complete = false;
					break;
				}
			}
			if complete {
				match self.bind_local_star_actuation(
					base_plan.clone(),
					node.label.clone(),
					&bound_children,
					true,
					child_dimensions.len(),
					trace,
					render_trace,
				)? {
					LocalStarActuationAttempt::Actuated(local) => {
						receipts.push(local.receipt);
						local.plan
					}
					LocalStarActuationAttempt::Declined(_) => self.bind_declared_star_children(
						base_plan,
						&child_dimensions,
						trace,
						render_trace,
					)?,
				}
			} else {
				self.bind_declared_star_children(base_plan, &child_dimensions, trace, render_trace)?
			}
		} else if child_dimensions.is_empty() {
			base_plan
		} else {
			self.bind_declared_star_children(base_plan, &child_dimensions, trace, render_trace)?
		};

		Ok(BuiltSnowflakeDimension {
			dimension: StarChildDimension {
				plan,
				on: node.on,
				strategy_intent: node.strategy_intent,
				label: node.label.clone(),
				declared_position: node.declared_position,
			},
			receipts,
		})
	}

	fn bind_declared_star_children<'b>(
		&self,
		base_plan: ScopedExecutionPlan,
		dimensions: &[StarChildDimension<'b>],
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let mut current = base_plan;
		for dimension in dimensions {
			current = self.bind_scoped_join(
				current,
				dimension.plan.clone(),
				JoinKind::Inner,
				dimension.on,
				dimension.strategy_intent,
				trace,
				render_trace,
				dimension.declared_position,
			)?;
		}
		Ok(current)
	}

	fn bound_star_dimension<'b>(
		&self,
		probe_plan: &ScopedExecutionPlan,
		probe_label: &str,
		dimension: &StarChildDimension<'b>,
	) -> Result<Option<BoundStarDimension<'b>>, BindingError> {
		let Some(edge) =
			self.try_resolve_star_dimension_edge(probe_plan, &dimension.plan, dimension.on)?
		else {
			return Ok(None);
		};
		if edge.left_label.as_deref() != Some(probe_label)
			|| edge.right_label.as_deref() != Some(dimension.label.as_str())
		{
			return Ok(None);
		}
		let join_estimated_row_count = Self::join_estimated_row_count_for_keys(
			JoinKind::Inner,
			probe_plan,
			&dimension.plan,
			&edge.left_resolved_keys,
			&edge.right_resolved_keys,
		);
		let (estimated_rows, estimate_source) = join_order_edge_estimate(
			probe_plan.join_order_sources.as_ref(),
			dimension.plan.join_order_sources.as_ref(),
			Some(probe_label),
			Some(dimension.label.as_str()),
			&edge.left_resolved_keys,
			&edge.right_resolved_keys,
			join_estimated_row_count,
		);
		let Some(estimated_rows) = estimated_rows else {
			return Ok(None);
		};
		let confidence = join_order_estimate_confidence(
			probe_plan.join_order_sources.as_ref(),
			dimension.plan.join_order_sources.as_ref(),
			Some(probe_label),
			Some(dimension.label.as_str()),
			&edge.left_resolved_keys,
			&edge.right_resolved_keys,
		);
		Ok(Some(BoundStarDimension {
			plan: dimension.plan.clone(),
			on: dimension.on,
			strategy_intent: dimension.strategy_intent,
			label: dimension.label.clone(),
			evidence: JoinOrderDimEvidence {
				dimension: dimension.label.clone(),
				declared_position: dimension.declared_position,
				estimated_rows,
				estimate_source,
				confidence,
			},
		}))
	}

	fn bind_local_star_actuation<'b>(
		&self,
		probe_plan: ScopedExecutionPlan,
		probe_label: String,
		dimensions: &[BoundStarDimension<'b>],
		force_hash: bool,
		edge_count: usize,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
	) -> Result<LocalStarActuationAttempt, BindingError> {
		let evidence_dimensions = dimensions
			.iter()
			.map(|dimension| dimension.evidence.clone())
			.collect::<Vec<_>>();
		let confidence = join_order_overall_confidence(&evidence_dimensions);
		let recognized = recognized_star_order_outcome(
			probe_label.clone(),
			evidence_dimensions.clone(),
			None,
			StarOrderStrategy::Broadcast,
			confidence,
		);
		let StarOrderOutcome::Recognized(evidence) = &recognized else {
			unreachable!("recognized_star_order_outcome always recognizes");
		};
		let mut preferred_indices = (0..dimensions.len()).collect::<Vec<_>>();
		preferred_indices.sort_by(|left, right| {
			dimensions[*left]
				.evidence
				.estimated_rows
				.cmp(&dimensions[*right].evidence.estimated_rows)
				.then(
					dimensions[*left]
						.evidence
						.declared_position
						.cmp(&dimensions[*right].evidence.declared_position),
				)
		});
		let declared_indices = (0..dimensions.len()).collect::<Vec<_>>();
		let selected_indices = if evidence.action == StarOrderAction::Reordered {
			preferred_indices
		} else {
			declared_indices.clone()
		};

		let Some(declared_strategy) = self.preflight_star_order_strategy(
			probe_plan.clone(),
			dimensions,
			&declared_indices,
			trace,
		)?
		else {
			return Ok(LocalStarActuationAttempt::Declined(
				StarOrderDeclineReason::MixedStrategyStar,
			));
		};
		let Some(selected_strategy) = self.preflight_star_order_strategy(
			probe_plan.clone(),
			dimensions,
			&selected_indices,
			trace,
		)?
		else {
			return Ok(LocalStarActuationAttempt::Declined(
				StarOrderDeclineReason::MixedStrategyStar,
			));
		};
		if declared_strategy != selected_strategy {
			return Ok(LocalStarActuationAttempt::Declined(
				StarOrderDeclineReason::MixedStrategyStar,
			));
		}
		if force_hash && selected_strategy != StarOrderStrategy::HashPartitioned {
			return Ok(LocalStarActuationAttempt::Declined(
				StarOrderDeclineReason::MixedStrategyStar,
			));
		}

		let declared_segments = star_output_segments(&probe_label, &probe_plan, dimensions);
		let mut declared_output_schema = probe_plan.schema();
		let mut declared_scope = probe_plan.scope.clone();
		for dimension in dimensions {
			declared_output_schema = join_schema(
				JoinKind::Inner,
				&declared_output_schema,
				&dimension.plan.schema(),
			);
			declared_scope = RelationScope::join(
				JoinKind::Inner,
				&declared_scope,
				&dimension.plan.scope,
				&declared_output_schema,
			);
		}
		let applied_order = selected_indices
			.iter()
			.map(|idx| dimensions[*idx].label.clone())
			.collect::<Vec<_>>();
		let physical_segments = star_output_segments_for_order(
			&probe_label,
			&probe_plan,
			dimensions,
			&selected_indices,
		);
		let local_edge_start = render_trace
			.as_ref()
			.map(|trace| trace.join_order_edges.len())
			.unwrap_or(0);
		let mut output = self.bind_star_order_sequence(
			probe_plan,
			dimensions,
			&selected_indices,
			trace,
			render_trace,
		)?;
		if evidence.action == StarOrderAction::Reordered {
			let physical_output_schema = output.schema();
			output.plan = restore_declared_star_order_project(
				output.plan,
				&physical_output_schema,
				declared_output_schema,
				&physical_segments,
				&declared_segments,
			)?;
			output.scope = declared_scope;
		}

		let outcome = recognized_star_order_outcome(
			probe_label.clone(),
			evidence_dimensions,
			Some(applied_order),
			selected_strategy,
			confidence,
		);
		let edge_targets = render_trace
			.as_ref()
			.map(|trace| {
				trace.join_order_edges[local_edge_start..]
					.iter()
					.map(|edge| edge.target)
					.collect::<Vec<_>>()
			})
			.unwrap_or_else(|| {
				(0..edge_count)
					.map(|join_index| JoinTraceTarget { join_index })
					.collect::<Vec<_>>()
			});
		Ok(LocalStarActuationAttempt::Actuated(LocalStarActuation {
			plan: output,
			receipt: StarOrderRenderReceipt {
				probe_label,
				outcome,
				edge_targets,
			},
		}))
	}

	fn try_resolve_star_dimension_edge(
		&self,
		left_plan: &ScopedExecutionPlan,
		right_plan: &ScopedExecutionPlan,
		on: &[(Expr, Expr)],
	) -> Result<Option<StarDimensionJoinEdge>, BindingError> {
		let mut left_resolved_keys = Vec::new();
		let mut right_resolved_keys = Vec::new();
		for (left_expr, right_expr) in on {
			let Ok((left_key, right_key)) =
				bind_join_key_pair(left_expr, right_expr, &left_plan.scope, &right_plan.scope)
			else {
				return Ok(None);
			};
			validate_join_key_types(&left_key, &right_key)?;
			left_resolved_keys.push(left_key);
			right_resolved_keys.push(right_key);
		}
		let left_label = relation_label_for_join_keys(&left_plan.scope, &left_resolved_keys);
		let right_label = relation_label_for_join_keys(&right_plan.scope, &right_resolved_keys);
		Ok(Some(StarDimensionJoinEdge {
			left_resolved_keys,
			right_resolved_keys,
			left_label,
			right_label,
		}))
	}
	fn preflight_star_order_strategy<'b>(
		&self,
		probe_plan: ScopedExecutionPlan,
		dimensions: &[BoundStarDimension<'b>],
		order: &[usize],
		base_trace: &OptimizerTrace,
	) -> Result<Option<StarOrderStrategy>, BindingError> {
		let mut preflight_trace = base_trace.clone();
		let preflight_join_start = preflight_trace.join_decisions.len();
		let mut no_render_trace = None;
		let _ = self.bind_star_order_sequence(
			probe_plan,
			dimensions,
			order,
			&mut preflight_trace,
			&mut no_render_trace,
		)?;
		let preflight_decisions = &preflight_trace.join_decisions[preflight_join_start..];
		if preflight_decisions.len() != order.len() {
			return Ok(None);
		}
		Ok(uniform_star_order_strategy(preflight_decisions))
	}

	fn bind_star_order_sequence<'b>(
		&self,
		probe_plan: ScopedExecutionPlan,
		dimensions: &[BoundStarDimension<'b>],
		order: &[usize],
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let mut current = probe_plan;
		for idx in order {
			let dimension = &dimensions[*idx];
			current = self.bind_scoped_join(
				current,
				dimension.plan.clone(),
				JoinKind::Inner,
				dimension.on,
				dimension.strategy_intent,
				trace,
				render_trace,
				dimension.evidence.declared_position,
			)?;
		}
		Ok(current)
	}

	fn record_star_order_decline(
		&self,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		reason: StarOrderDeclineReason,
		edge_count: usize,
	) {
		if let Some(render_trace) = render_trace.as_mut() {
			let outcome = StarOrderOutcome::Declined(StarOrderDecline { reason, edge_count });
			render_trace.set_star_receipt(StarOrderRenderReceipt {
				probe_label: star_order_probe_label(&outcome),
				outcome,
				edge_targets: Vec::new(),
			});
		}
	}

	fn record_optimizer_star_order_receipt(
		&self,
		trace: &OptimizerTrace,
		kind: OptimizerDecisionKind,
		receipt: &StarOrderRenderReceipt,
	) {
		if !trace.record_optimizer_decisions {
			return;
		}
		if let StarOrderOutcome::Recognized(evidence) = &receipt.outcome {
			self.config
				.decision_log
				.record(kind, optimizer_decision_outcome_for_action(evidence.action));
		}
	}

	fn record_optimizer_star_order_decline(
		&self,
		trace: &OptimizerTrace,
		kind: OptimizerDecisionKind,
		reason: StarOrderDeclineReason,
	) {
		if !trace.record_optimizer_decisions {
			return;
		}
		self.config.decision_log.record(
			kind,
			OptimizerDecisionOutcome::Declined(optimizer_decision_decline_reason(reason)),
		);
	}
}

fn root_decision_kind(tree: &SnowflakeStarTree<'_>) -> OptimizerDecisionKind {
	if tree.has_branch_dimensions() {
		OptimizerDecisionKind::SnowflakeRoot
	} else {
		OptimizerDecisionKind::FlatStar
	}
}

fn optimizer_decision_outcome_for_action(action: StarOrderAction) -> OptimizerDecisionOutcome {
	match action {
		StarOrderAction::Reordered => OptimizerDecisionOutcome::Reordered,
		StarOrderAction::KeptAlreadyOptimal => OptimizerDecisionOutcome::KeptOptimal,
		StarOrderAction::KeptInsufficientMargin => OptimizerDecisionOutcome::KeptThinMargin,
	}
}

fn optimizer_decision_decline_reason(
	reason: StarOrderDeclineReason,
) -> OptimizerDecisionDeclineReason {
	match reason {
		StarOrderDeclineReason::MixedStrategyStar => {
			OptimizerDecisionDeclineReason::MixedStrategyStar
		}
		StarOrderDeclineReason::NonInnerEquiJoin => {
			OptimizerDecisionDeclineReason::NonInnerEquiJoin
		}
		StarOrderDeclineReason::MissingRelationLabel => {
			OptimizerDecisionDeclineReason::MissingRelationLabel
		}
		StarOrderDeclineReason::MultipleProbeRelations => {
			OptimizerDecisionDeclineReason::MultipleProbeRelations
		}
		StarOrderDeclineReason::DuplicateDimension => {
			OptimizerDecisionDeclineReason::DuplicateDimension
		}
		StarOrderDeclineReason::MissingEstimate => OptimizerDecisionDeclineReason::MissingEstimate,
		StarOrderDeclineReason::ProbeNotScanLinear => {
			OptimizerDecisionDeclineReason::ProbeNotScanLinear
		}
	}
}
