use super::*;

pub(super) type JoinOrderSourceMap = BTreeMap<String, JoinOrderRelationFacts>;

pub(super) const STAR_ORDER_SOFT_MARGIN_NUMERATOR: u128 = 4;
pub(super) const STAR_ORDER_SOFT_MARGIN_DENOMINATOR: u128 = 5;
pub(crate) const JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE: u64 = 20;
pub(crate) const JOIN_ORDER_MARGIN_ADVISORY_HYSTERESIS_NUMERATOR: u64 = 1;
pub(crate) const JOIN_ORDER_MARGIN_ADVISORY_HYSTERESIS_DENOMINATOR: u64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoftMarginLadderStep {
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

	pub(crate) fn margin(self) -> SoftMargin {
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
	pub(crate) current_step: SoftMarginLadderStep,
	pub(crate) recommended_step: SoftMarginLadderStep,
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
		current_step: current,
		recommended_step,
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

// ADR 191: the single opt-in derivation of the effective soft-margin step. `Manual` returns the
// ADR 190 configured base unchanged; `ApplyAdvisory` returns exactly the ADR 189 advisory's
// `recommended_step` (bounded to one ladder rung from the base). Resolved once at bind entry, never
// inside low-level star/snowflake helpers, and never mutating config or the decision log.
pub(crate) fn effective_join_order_soft_margin_step(
	config: &PlannerConfig,
) -> SoftMarginLadderStep {
	match config.join_order_soft_margin_application {
		JoinOrderSoftMarginApplication::Manual => config.join_order_soft_margin_step,
		JoinOrderSoftMarginApplication::ApplyAdvisory => {
			recommend_soft_margin(
				&config.decision_log.verdict_snapshot(),
				config.join_order_soft_margin_step,
			)
			.recommended_step
		}
	}
}

// ADR 191: report view that keeps the configured base, the ADR 189 recommendation from that base,
// the application mode, and the applied step together so the advisory table never recomputes from
// an already-applied margin (the Baseline -> Loose -> Loosest double-step trap).
pub(crate) struct MarginAdvisoryReport {
	pub(crate) advisory: MarginAdvisory,
	pub(crate) application: JoinOrderSoftMarginApplication,
	pub(crate) applied_step: SoftMarginLadderStep,
}

pub(crate) fn margin_advisory_report(config: &PlannerConfig) -> MarginAdvisoryReport {
	let advisory = recommend_soft_margin(
		&config.decision_log.verdict_snapshot(),
		config.join_order_soft_margin_step,
	);
	let applied_step = match config.join_order_soft_margin_application {
		JoinOrderSoftMarginApplication::Manual => config.join_order_soft_margin_step,
		JoinOrderSoftMarginApplication::ApplyAdvisory => advisory.recommended_step,
	};
	MarginAdvisoryReport {
		advisory,
		application: config.join_order_soft_margin_application,
		applied_step,
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
	// ADR 194: role-neutral graph endpoints. `probe`/`dimension` above are the left/right key-source
	// labels (the relation that owns the resolved join key on each side) — they are the directed trace
	// role. The key signatures below identify *which column* of each key-source relation the edge
	// touches (`["product.id"]`), so a galaxy recognizer can see that two edges conform on the same
	// dimension column. Endpoint rows are the per-key-source relation row estimate from
	// `join_order_sources` (NOT `plan.facts`, which on a composite input describes the whole built-up
	// side, not the relation that owns the key).
	pub(super) left_key_signature: Vec<String>,
	pub(super) right_key_signature: Vec<String>,
	pub(super) left_endpoint_rows: Option<RowCount>,
	pub(super) right_endpoint_rows: Option<RowCount>,
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
	// ADR 193: a valid hash/broadcast mixture at one local-star level (some edges hash, some
	// broadcast). Authorized only at root levels when the selected physical order's own preflight
	// proves a valid mixed placement set.
	Mixed,
}

// ADR 192: typed authorization for a local star actuation, replacing the old `force_hash: bool`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StarOrderStrategyAuthorization {
	// Flat-star root: strict ADR-181 uniformity (`uniform_star_order_strategy`); declared and
	// selected orders must resolve to the same strategy.
	AnyUniform,
	// Snowflake root with composite branch builds: fenced uniformity
	// (`uniform_snowflake_order_strategy`, which accepts `Natural` placement for a composite build)
	// plus the broadcast-upgrade relaxation — a clean selected-order Broadcast actuates even when the
	// declared order preflights to hash, because composite-build strategy is order-variant.
	CompositeBuildSnowflakeRoot,
	// Branch star folded into a composite build: hash-only, so the fragmenter never sees a nested
	// broadcast probe (BroadcastProbeNotStarLinear).
	HashOnlyCompositeProbe,
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
	pub(super) applied_margin: SoftMargin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StarOrderEvidence {
	pub(super) probe: String,
	pub(super) dimensions: Vec<JoinOrderDimEvidence>,
	pub(super) declared_order: Vec<String>,
	pub(super) cost_preferred_order: Vec<String>,
	pub(super) applied_order: Vec<String>,
	pub(super) strategy: StarOrderStrategy,
	// ADR 193: per-dimension applied strategy/placement, in applied order. Renders the honest
	// `strategies=[d:hash_partitioned/natural,...]` detail behind a `strategy=mixed` summary (and is
	// equally available for uniform receipts). Generic, not snowflake-named.
	pub(super) edge_strategies: Vec<(String, JoinStrategy, JoinPhysicalPlacement)>,
	pub(super) declared_cost: u128,
	pub(super) preferred_cost: u128,
	pub(super) orders_agree: bool,
	pub(super) action: StarOrderAction,
	pub(super) confidence: JoinOrderEstimateConfidence,
	pub(super) applied_margin: SoftMargin,
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

// ADR 194: galaxy graph evidence is a sibling of `SnowflakeOrderEvidence`, not an extension — a
// snowflake is a single-root tree; a galaxy is a multi-fact graph linked by a conformed dimension.
// This is observe-only: it never carries a chosen order, verdict, or actuation receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GalaxyOrderEvidence {
	pub(super) conformed: Vec<ConformedDimension>,
	pub(super) edges: Vec<GalaxyEdgeEvidence>,
	pub(super) component_count: usize,
}

// A relation joined by two or more distinct relations on the *same* key-source column signature. This
// is a trace-level role-neutral observation ("`product.id` is referenced by `sales` and `returns`"),
// not a catalog-owned fact/dimension classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ConformedDimension {
	pub(super) label: String,
	pub(super) key_signature: Vec<String>,
	pub(super) incident_relations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GalaxyEdgeEvidence {
	pub(super) target: JoinTraceTarget,
	pub(super) left_label: String,
	pub(super) right_label: String,
	pub(super) left_key_signature: Vec<String>,
	pub(super) right_key_signature: Vec<String>,
	pub(super) estimated_rows: Option<RowCount>,
	pub(super) strategy: JoinStrategy,
	pub(super) placement: JoinPhysicalPlacement,
}

// ADR 196: bridge graph evidence is a sibling of `GalaxyOrderEvidence` — a bridge is a many-to-many
// junction (`Fact ⋈ Bridge ⋈ Dim`), structurally identical in the trace to a 1:N snowflake branch and
// separated from it only by a conservative, fixture-grounded inverse-F1 cardinality gate (the junction
// is not dimension-like relative to its fact-side probe). Observe-only: no order, verdict, or actuation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BridgeOrderEvidence {
	pub(super) links: Vec<BridgeLink>,
}

// Carries the row components rather than a precomputed `f64` so the evidence stays `Eq`; the float
// fan-out ratio is computed at render time (`bridge_rows / fact_rows`, with `fact_rows > 0` guaranteed
// by the recognizer's zero-denominator decline).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BridgeLink {
	pub(super) fact: String,
	pub(super) bridge: String,
	pub(super) dim: String,
	pub(super) b_in_signature: Vec<String>,
	pub(super) b_out_signature: Vec<String>,
	pub(super) bridge_rows: RowCount,
	pub(super) fact_rows: RowCount,
	pub(super) incoming_target: JoinTraceTarget,
	pub(super) outgoing_target: JoinTraceTarget,
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

// ADR 198: re-root a 2-fact galaxy `Join(Join(F1, D), F2)` to `Join(Join(F2, D), F1)` by inner-join
// reassociation — promoting the trailing fact `F2` to the root. Semantically safe (inner joins are
// commutative/associative); the `on` column refs are logical (re-resolved at bind time), so swapping
// the pair sides keeps each condition valid in its new position. Returns `None` for any shape that is
// not the exact 2-fact galaxy (two nested inner joins over leaf relations) → decline, AST order.
pub(super) fn reroot_galaxy_join_tree(plan: &LogicalPlan) -> Option<LogicalPlan> {
	let LogicalPlan::Join {
		left: outer_left,
		right: f2,
		kind: outer_kind,
		on: outer_on,
		strategy_intent: outer_intent,
	} = plan
	else {
		return None;
	};
	if *outer_kind != JoinKind::Inner || logical_plan_contains_join(f2) {
		return None;
	}
	let LogicalPlan::Join {
		left: f1,
		right: dim,
		kind: inner_kind,
		on: inner_on,
		strategy_intent: inner_intent,
	} = outer_left.as_ref()
	else {
		return None;
	};
	if *inner_kind != JoinKind::Inner
		|| logical_plan_contains_join(f1)
		|| logical_plan_contains_join(dim)
	{
		return None;
	}
	let new_inner = LogicalPlan::Join {
		left: f2.clone(),
		right: dim.clone(),
		kind: *inner_kind,
		on: outer_on
			.iter()
			.map(|(left, right)| (right.clone(), left.clone()))
			.collect(),
		strategy_intent: *inner_intent,
	};
	Some(LogicalPlan::Join {
		left: Box::new(new_inner),
		right: f1.clone(),
		kind: *outer_kind,
		on: inner_on
			.iter()
			.map(|(left, right)| (right.clone(), left.clone()))
			.collect(),
		strategy_intent: *outer_intent,
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

// ADR 194: the role-neutral key signature for one side of a join — the sorted, de-duplicated
// `"relation.column"` identities the resolved keys touch. Empty if any key column cannot be resolved
// to a single source, so the galaxy recognizer never conforms on an ambiguous endpoint.
pub(super) fn key_signature_for_join_keys(
	scope: &RelationScope,
	keys: &[ResolvedJoinKey],
) -> Vec<String> {
	let mut signature = BTreeSet::new();
	for key in keys {
		match scope.key_signature_for_ordinal(key.column.ordinal) {
			Some(label) => {
				signature.insert(label);
			}
			None => return Vec::new(),
		}
	}
	signature.into_iter().collect()
}

// ADR 194: the row estimate of the relation that owns a join key, looked up by its key-source label.
// Used to attach honest per-endpoint facts to a role-neutral edge.
pub(super) fn join_order_endpoint_rows(
	sources: Option<&JoinOrderSourceMap>,
	label: Option<&str>,
) -> Option<RowCount> {
	sources?.get(label?).and_then(|facts| facts.rows)
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

pub(super) fn analyze(
	edges: &[JoinOrderEdgeEvidence],
	margin_step: SoftMarginLadderStep,
) -> Option<StarOrderOutcome> {
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
		Vec::new(),
		JoinOrderEstimateConfidence::Soft,
		margin_step.margin(),
	))
}

pub(super) fn analyze_snowflake(
	edges: &[JoinOrderEdgeEvidence],
	decisions: &[DecisionReceipt],
	margin_step: SoftMarginLadderStep,
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
		margin_step,
	)?;
	if visited_edges.len() != edges.len() {
		return None;
	}
	Some(evidence)
}

// ADR 194: recognize a galaxy — a multi-fact graph linked by a conformed dimension — over the
// role-neutral edge endpoints. The directed trace records `sales ⋈ product ⋈ returns` as a snowflake
// (`product→returns`), but the role-neutral key signatures reveal that `product` is joined by both
// `sales` and `returns` on its *same* column (`product.id`). A snowflake branch, by contrast, fans out
// on its *own* differing foreign keys (`customers.region_id`, `customers.segment_id`), so it never
// conforms. Observe-only: no order, no verdict, no actuation.
pub(super) fn analyze_galaxy(
	edges: &[JoinOrderEdgeEvidence],
	decisions: &[DecisionReceipt],
) -> Option<GalaxyOrderEvidence> {
	if edges.len() < 2 {
		return None;
	}

	// Validate every edge and build the role-neutral galaxy edge list.
	let mut galaxy_edges = Vec::with_capacity(edges.len());
	for edge in edges {
		if !edge.inner_equi {
			return None;
		}
		let left_label = edge.probe.clone()?;
		let right_label = edge.dimension.clone()?;
		if left_label == right_label || edge.estimated_rows.is_none() {
			return None;
		}
		if edge.left_key_signature.is_empty() || edge.right_key_signature.is_empty() {
			return None;
		}
		let decision = decisions.get(edge.target.join_index)?;
		if decision.target != edge.target {
			return None;
		}
		galaxy_edges.push(GalaxyEdgeEvidence {
			target: edge.target,
			left_label,
			right_label,
			left_key_signature: edge.left_key_signature.clone(),
			right_key_signature: edge.right_key_signature.clone(),
			estimated_rows: edge.estimated_rows,
			strategy: decision.strategy,
			placement: decision.placement,
		});
	}

	// For each relation, group the *other* endpoints by the key signature on this relation's side. A
	// signature shared by >= 2 distinct other relations is a conformed-dimension *candidate*.
	let mut by_relation: BTreeMap<String, BTreeMap<Vec<String>, BTreeSet<String>>> =
		BTreeMap::new();
	for edge in &galaxy_edges {
		by_relation
			.entry(edge.left_label.clone())
			.or_default()
			.entry(edge.left_key_signature.clone())
			.or_default()
			.insert(edge.right_label.clone());
		by_relation
			.entry(edge.right_label.clone())
			.or_default()
			.entry(edge.right_key_signature.clone())
			.or_default()
			.insert(edge.left_label.clone());
	}

	// The per-relation row estimate, role-neutral (from `join_order_sources`, by key-source label).
	let mut rows_by_relation: BTreeMap<String, RowCount> = BTreeMap::new();
	for edge in edges {
		if let (Some(label), Some(rows)) = (edge.probe.as_ref(), edge.left_endpoint_rows) {
			rows_by_relation.entry(label.clone()).or_insert(rows);
		}
		if let (Some(label), Some(rows)) = (edge.dimension.as_ref(), edge.right_endpoint_rows) {
			rows_by_relation.entry(label.clone()).or_insert(rows);
		}
	}

	let mut conformed = Vec::new();
	for (relation, by_signature) in &by_relation {
		for (signature, incident) in by_signature {
			if incident.len() < 2 {
				continue;
			}
			// ADR 194 (F1): the candidate must be *dimension-like* — its row estimate no larger than
			// every incident relation it is shared by. A single fact joined to several aliases of one
			// dimension on its OWN foreign key (a duplicate-dimension star, e.g. `orders` joined to
			// `customers c1`/`customers c2` on `orders.customer_id`) makes the fact look conformed; the
			// row gate rejects it because the fact is larger than its dimensions. Conservative: a
			// candidate with missing row estimates is not claimed as conformed.
			let Some(candidate_rows) = rows_by_relation.get(relation) else {
				continue;
			};
			let dimension_like = incident.iter().all(|incident_relation| {
				rows_by_relation
					.get(incident_relation)
					.is_some_and(|incident_rows| candidate_rows <= incident_rows)
			});
			if !dimension_like {
				continue;
			}
			conformed.push(ConformedDimension {
				label: relation.clone(),
				key_signature: signature.clone(),
				incident_relations: incident.iter().cloned().collect(),
			});
		}
	}
	if conformed.is_empty() {
		// No conformed dimension links two facts — a plain star/snowflake, not a galaxy.
		return None;
	}

	let component_count = galaxy_component_count(&galaxy_edges);
	Some(GalaxyOrderEvidence {
		conformed,
		edges: galaxy_edges,
		component_count,
	})
}

// Connected-component count over the undirected relation graph (union-find by edge endpoints).
fn galaxy_component_count(edges: &[GalaxyEdgeEvidence]) -> usize {
	let mut parent: BTreeMap<String, String> = BTreeMap::new();
	fn find(parent: &mut BTreeMap<String, String>, node: &str) -> String {
		let mut current = node.to_string();
		loop {
			let next = parent
				.get(&current)
				.cloned()
				.unwrap_or_else(|| current.clone());
			if next == current {
				return current;
			}
			current = next;
		}
	}
	for edge in edges {
		parent
			.entry(edge.left_label.clone())
			.or_insert_with(|| edge.left_label.clone());
		parent
			.entry(edge.right_label.clone())
			.or_insert_with(|| edge.right_label.clone());
		let left_root = find(&mut parent, &edge.left_label);
		let right_root = find(&mut parent, &edge.right_label);
		if left_root != right_root {
			parent.insert(left_root, right_root);
		}
	}
	let nodes = parent.keys().cloned().collect::<Vec<_>>();
	nodes
		.iter()
		.map(|node| find(&mut parent, node))
		.collect::<BTreeSet<_>>()
		.len()
}

// ADR 196: recognize a bridge — a many-to-many junction `Fact ⋈ Bridge ⋈ Dim`. The bridge node `B` is
// the `dimension` of one edge (built onto a fact `F`) and the `probe` of another (joining out to a dim
// `D`), on two distinct signatures. Structurally this is identical to a 1:N snowflake branch; the only
// role-neutral separator is cardinality, so `B` is a bridge only when it is NOT dimension-like relative
// to its fact-side probe (a conservative, fixture-grounded inverse of ADR 194's F1 gate). Observe-only.
pub(super) fn analyze_bridge(
	edges: &[JoinOrderEdgeEvidence],
	decisions: &[DecisionReceipt],
) -> Option<BridgeOrderEvidence> {
	if edges.len() < 2 {
		return None;
	}
	for edge in edges {
		if !edge.inner_equi {
			return None;
		}
		edge.probe.as_ref()?;
		edge.dimension.as_ref()?;
		let decision = decisions.get(edge.target.join_index)?;
		if decision.target != edge.target {
			return None;
		}
	}

	// Per-relation row estimate (consistent across edges; role-neutral, from `join_order_sources`).
	let mut rows_by_relation: BTreeMap<String, RowCount> = BTreeMap::new();
	for edge in edges {
		if let (Some(label), Some(rows)) = (edge.probe.as_ref(), edge.left_endpoint_rows) {
			rows_by_relation.entry(label.clone()).or_insert(rows);
		}
		if let (Some(label), Some(rows)) = (edge.dimension.as_ref(), edge.right_endpoint_rows) {
			rows_by_relation.entry(label.clone()).or_insert(rows);
		}
	}

	let mut relations = BTreeSet::new();
	for edge in edges {
		if let Some(probe) = edge.probe.as_deref() {
			relations.insert(probe);
		}
		if let Some(dimension) = edge.dimension.as_deref() {
			relations.insert(dimension);
		}
	}

	let mut links = Vec::new();
	for bridge in &relations {
		// Incoming: bridge is built onto a fact. Outgoing: bridge joins out to a dimension.
		let Some(incoming) = edges
			.iter()
			.find(|edge| edge.dimension.as_deref() == Some(*bridge))
		else {
			continue;
		};
		let Some(outgoing) = edges
			.iter()
			.find(|edge| edge.probe.as_deref() == Some(*bridge))
		else {
			continue;
		};
		let (Some(fact), Some(dim)) = (incoming.probe.as_deref(), outgoing.dimension.as_deref())
		else {
			continue;
		};
		// Reject cycles / degenerate junctions.
		if fact == *bridge || dim == *bridge || fact == dim {
			continue;
		}
		let b_in_signature = &incoming.right_key_signature;
		let b_out_signature = &outgoing.left_key_signature;
		if b_in_signature.is_empty() || b_out_signature.is_empty() {
			continue;
		}
		// Equal signatures = a conformed dimension; ADR 194's galaxy recognizer owns that.
		if b_in_signature == b_out_signature {
			continue;
		}
		let (Some(bridge_rows), Some(fact_rows)) = (
			rows_by_relation.get(*bridge).copied(),
			rows_by_relation.get(fact).copied(),
		) else {
			continue;
		};
		// Zero denominator → no fan-out claim. Conservative inverse-F1: a bridge is NOT dimension-like.
		if fact_rows.get() == 0 || bridge_rows < fact_rows {
			continue;
		}
		links.push(BridgeLink {
			fact: fact.to_string(),
			bridge: (*bridge).to_string(),
			dim: dim.to_string(),
			b_in_signature: b_in_signature.clone(),
			b_out_signature: b_out_signature.clone(),
			bridge_rows,
			fact_rows,
			incoming_target: incoming.target,
			outgoing_target: outgoing.target,
		});
	}

	if links.is_empty() {
		None
	} else {
		Some(BridgeOrderEvidence { links })
	}
}

// ADR 197: the graph order scorer is NEW — it does not call the star cost functions
// (`order_cost`/`rank_star_order`); it costs the small candidate set the recognized graph shape admits
// directly from the captured edge estimates. Only the verdict vocabulary (`StarOrderAction`) and, at
// render time, the measured-verdict shape are reused. Row-only (cf. ADR 181); observe-only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GraphOrderRecommendation {
	pub(super) pivot: String,
	pub(super) declared_root: String,
	pub(super) preferred_root: String,
	pub(super) costs: Vec<(String, u128)>,
	pub(super) action: StarOrderAction,
}

// The row estimate of the edge connecting relations `a` and `b` (undirected), as a row-only cost.
fn edge_estimate_between(edges: &[GalaxyEdgeEvidence], a: &str, b: &str) -> Option<u128> {
	edges
		.iter()
		.find(|edge| {
			(edge.left_label == a && edge.right_label == b)
				|| (edge.left_label == b && edge.right_label == a)
		})
		.and_then(|edge| edge.estimated_rows)
		.map(|rows| u128::from(rows.get()))
}

// Galaxy order cost: root the fact whose join to the conformed dimension is smallest. `declared_root`
// is the fact never built as a dimension (the FROM root); `preferred_root` is the argmin.
pub(super) fn galaxy_order_recommendation(
	galaxy: &GalaxyOrderEvidence,
) -> Option<GraphOrderRecommendation> {
	let dim = galaxy.conformed.first()?;
	let facts = &dim.incident_relations;
	if facts.len() < 2 {
		return None;
	}
	let mut costs = Vec::with_capacity(facts.len());
	for fact in facts {
		costs.push((
			fact.clone(),
			edge_estimate_between(&galaxy.edges, fact, &dim.label)?,
		));
	}
	let preferred_root = costs
		.iter()
		.min_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)))
		.map(|(fact, _)| fact.clone())?;
	let declared_root = facts
		.iter()
		.find(|fact| galaxy.edges.iter().all(|edge| &edge.right_label != *fact))?
		.clone();
	let action = if declared_root == preferred_root {
		StarOrderAction::KeptAlreadyOptimal
	} else {
		StarOrderAction::Reordered
	};
	Some(GraphOrderRecommendation {
		pivot: dim.label.clone(),
		declared_root,
		preferred_root,
		costs,
		action,
	})
}

// Bridge order cost: a many-to-many bridge fans out (`bridge_rows >= fact_rows` by the ADR 196 gate).
// When the fan-out is real (`bridge_rows > fact_rows`), prefer delaying the bridge expansion until the
// selective dimension is applied (`filter_first`) over materializing it early (`fan_out_first`).
pub(super) fn bridge_order_recommendation(
	bridge: &BridgeOrderEvidence,
) -> Option<GraphOrderRecommendation> {
	let link = bridge.links.first()?;
	let fan_out_cost = u128::from(link.bridge_rows.get());
	let filter_cost = u128::from(link.fact_rows.get());
	let action = if link.bridge_rows > link.fact_rows {
		StarOrderAction::Reordered
	} else {
		StarOrderAction::KeptAlreadyOptimal
	};
	let preferred_root = match action {
		StarOrderAction::Reordered => "filter_first".to_string(),
		_ => "fan_out_first".to_string(),
	};
	Some(GraphOrderRecommendation {
		pivot: link.bridge.clone(),
		declared_root: "fan_out_first".to_string(),
		preferred_root,
		costs: vec![
			("fan_out_first".to_string(), fan_out_cost),
			("filter_first".to_string(), filter_cost),
		],
		action,
	})
}

fn snowflake_order_level(
	probe: &str,
	edges: &[JoinOrderEdgeEvidence],
	decisions: &[DecisionReceipt],
	adjacency: &BTreeMap<String, Vec<usize>>,
	visited_nodes: &mut BTreeSet<String>,
	visited_edges: &mut BTreeSet<usize>,
	margin_step: SoftMarginLadderStep,
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
		margin_step.margin(),
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
					margin_step,
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
	edge_strategies: Vec<(String, JoinStrategy, JoinPhysicalPlacement)>,
	confidence: JoinOrderEstimateConfidence,
	margin: SoftMargin,
) -> StarOrderOutcome {
	let ranking = rank_star_order(probe, dimensions, confidence, margin);
	let applied_order = applied_order_override.unwrap_or_else(|| ranking.declared_order.clone());

	StarOrderOutcome::Recognized(StarOrderEvidence {
		probe: ranking.probe,
		dimensions: ranking.dimensions,
		declared_order: ranking.declared_order,
		cost_preferred_order: ranking.cost_preferred_order,
		applied_order,
		strategy,
		edge_strategies,
		declared_cost: ranking.declared_cost,
		preferred_cost: ranking.preferred_cost,
		orders_agree: ranking.orders_agree,
		action: ranking.action,
		confidence: ranking.confidence,
		applied_margin: ranking.applied_margin,
	})
}

pub(super) fn rank_star_order(
	probe: String,
	dimensions: Vec<JoinOrderDimEvidence>,
	confidence: JoinOrderEstimateConfidence,
	margin: SoftMargin,
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
	let action = classify_star_order_action(
		declared_cost,
		preferred_cost,
		orders_agree,
		confidence,
		margin,
	);
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
		applied_margin: margin,
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
			applied_margin: evidence.applied_margin,
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
		ranking.applied_margin,
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
		edge_strategies: Vec::new(),
		declared_cost: ranking.declared_cost,
		preferred_cost: ranking.preferred_cost,
		orders_agree: ranking.orders_agree,
		action: ranking.action,
		confidence: ranking.confidence,
		applied_margin: ranking.applied_margin,
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

// ADR 192: the snowflake-fenced uniformity rule. A snowflake root's composite branch build resolves
// to `(Broadcast, Natural)` while leaf-scan dimensions resolve to `(Broadcast, SwappedRightBuild)`;
// both are valid single-partition broadcast builds, so an all-broadcast snowflake root is uniform
// even with mixed placement. The flat-star rule (`uniform_star_order_strategy`) stays strict — this
// is a separate function so flat stars never see the relaxed placement.
pub(super) fn uniform_snowflake_order_strategy(
	decisions: &[DecisionReceipt],
	composite_build_positions: &BTreeSet<usize>,
) -> Option<StarOrderStrategy> {
	if decisions.is_empty() {
		return None;
	}
	if decisions.iter().enumerate().all(|(position, decision)| {
		if decision.strategy != JoinStrategy::Broadcast {
			return false;
		}
		match decision.placement {
			JoinPhysicalPlacement::SwappedRightBuild => true,
			JoinPhysicalPlacement::Natural => composite_build_positions.contains(&position),
		}
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

// ADR 193: a per-dimension strategy fingerprint — the `(JoinStrategy, JoinPhysicalPlacement)` each
// dimension label resolves to in a given order. Mixed authorization uses the selected preflight;
// the fingerprint remains the uniform-stability check and the source of honest per-edge receipts.
type StrategyFingerprint = BTreeMap<String, (JoinStrategy, JoinPhysicalPlacement)>;

// ADR 193: the preflight result — a `Mixed`-capable strategy summary plus the per-dimension
// fingerprint. The uniform recognizers above stay the strict ADR-181/192 fast path; this only adds
// the mixed tier and the fingerprint.
struct PreflightStrategy {
	summary: StarOrderStrategy,
	fingerprint: StrategyFingerprint,
}

// ADR 193: validate each edge's placement (same rules the uniform recognizers enforce — broadcast
// needs SwappedRightBuild, or Natural only at an authenticated composite-build position; hash needs
// Natural) and build the per-dimension fingerprint. `None` if any edge is an invalid combination, so
// an unauthenticated `Broadcast + Natural` still declines rather than entering the mixed tier.
fn order_strategy_fingerprint(
	decisions: &[DecisionReceipt],
	order: &[usize],
	dimensions: &[BoundStarDimension<'_>],
	composite_build_positions: &BTreeSet<usize>,
) -> Option<StrategyFingerprint> {
	let mut fingerprint = StrategyFingerprint::new();
	for (position, decision) in decisions.iter().enumerate() {
		let valid = match (decision.strategy, decision.placement) {
			(JoinStrategy::Broadcast, JoinPhysicalPlacement::SwappedRightBuild) => true,
			(JoinStrategy::Broadcast, JoinPhysicalPlacement::Natural) => {
				composite_build_positions.contains(&position)
			}
			(JoinStrategy::HashPartitioned, JoinPhysicalPlacement::Natural) => true,
			_ => false,
		};
		if !valid {
			return None;
		}
		let label = dimensions[order[position]].label.clone();
		fingerprint.insert(label, (decision.strategy, decision.placement));
	}
	Some(fingerprint)
}

pub(super) fn classify_star_order_action(
	declared_cost: u128,
	preferred_cost: u128,
	orders_agree: bool,
	confidence: JoinOrderEstimateConfidence,
	margin: SoftMargin,
) -> StarOrderAction {
	if orders_agree {
		return StarOrderAction::KeptAlreadyOptimal;
	}
	match confidence {
		JoinOrderEstimateConfidence::Hard => StarOrderAction::Reordered,
		JoinOrderEstimateConfidence::Soft => {
			if preferred_cost.saturating_mul(margin.denominator)
				<= declared_cost.saturating_mul(margin.numerator)
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
	composite_build: bool,
}

#[derive(Debug, Clone)]
struct BoundStarDimension<'a> {
	plan: ScopedExecutionPlan,
	on: &'a [(Expr, Expr)],
	strategy_intent: JoinStrategyIntent,
	label: String,
	composite_build: bool,
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
pub(super) enum GalaxyActuationResult {
	NotCandidate,
	Actuated(ScopedExecutionPlan),
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
	// ADR 198: bounded 2-fact galaxy root-swap actuation. Scratch-recognize the galaxy, and if its
	// cost-optimal root differs from the FROM root by the soft margin, re-root the join tree and bind
	// the re-rooted plan — which is a snowflake the existing actuator + fragmenter already stage
	// (proven by the distributed digest fixture). Otherwise observe-only (exactly ADR 197).
	pub(super) async fn try_bind_galaxy_actuation(
		&self,
		plan: &LogicalPlan,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
	) -> Result<GalaxyActuationResult, BindingError> {
		let LogicalPlan::Join {
			left,
			right,
			kind,
			on,
			strategy_intent,
		} = plan
		else {
			return Ok(GalaxyActuationResult::NotCandidate);
		};

		// Scratch-recognize the galaxy via the actuation-free `bind_join` (not the actuation-carrying
		// `bind_plan_with_trace`): `bind_join` never re-enters galaxy actuation on this node, and the
		// galaxy's left subtree is a single-fact star (fewer than two edges, so not a galaxy itself), so
		// recognition cannot recurse — no runtime re-entry flag needed. The bind runs on a binder that
		// discards decisions, so the scratch can never pollute the shared decision log.
		let mut scratch_trace = trace.clone();
		let mut scratch_render = Some(OptimizerExplainRenderTrace::default());
		let _ = self
			.without_decision_recording()
			.bind_join(
				left,
				right,
				*kind,
				on,
				*strategy_intent,
				&mut scratch_trace,
				&mut scratch_render,
				JoinOrderSourceCapture::On,
			)
			.await?;
		let scratch_render = scratch_render.expect("galaxy scratch render trace present");
		let Some(galaxy) = analyze_galaxy(
			&scratch_render.join_order_edges,
			&scratch_trace.join_decisions,
		) else {
			return Ok(GalaxyActuationResult::NotCandidate);
		};
		let Some(recommendation) = galaxy_order_recommendation(&galaxy) else {
			return Ok(GalaxyActuationResult::NotCandidate);
		};
		// FROM root already cost-optimal → observe-only (no re-root).
		if recommendation.action != StarOrderAction::Reordered {
			return Ok(GalaxyActuationResult::NotCandidate);
		}
		// Soft-margin gate (190/191): only re-root on a clear win, never a thin one.
		let cost_of = |root: &str| {
			recommendation
				.costs
				.iter()
				.find(|(label, _)| label == root)
				.map(|(_, cost)| *cost)
		};
		let (Some(declared_cost), Some(preferred_cost)) = (
			cost_of(&recommendation.declared_root),
			cost_of(&recommendation.preferred_root),
		) else {
			return Ok(GalaxyActuationResult::NotCandidate);
		};
		let margin = self.join_order_soft_margin_step.margin();
		if preferred_cost.saturating_mul(margin.denominator)
			> declared_cost.saturating_mul(margin.numerator)
		{
			return Ok(GalaxyActuationResult::NotCandidate);
		}

		// Re-root the join tree and bind the re-rooted plan (a snowflake) on the real trace. Its own
		// galaxy check sees `KeptAlreadyOptimal` (FROM root now == preferred), so no further re-root.
		let Some(rerooted) = reroot_galaxy_join_tree(plan) else {
			return Ok(GalaxyActuationResult::NotCandidate);
		};
		let preferred_root = recommendation.preferred_root.clone();
		let bound = self
			.bind_plan_with_trace(&rerooted, trace, render_trace, JoinOrderSourceCapture::On)
			.await?;
		// The receipt reflects the applied re-root, not the re-rooted galaxy's intrinsic recommendation.
		if let Some(render_trace) = render_trace.as_mut() {
			render_trace.set_applied_galaxy_root(preferred_root);
		}
		Ok(GalaxyActuationResult::Actuated(bound))
	}

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
		// Scratch recognition must stay invisible to the shared decision log: bind probe/dimensions on a
		// no-op-recorder binder so a nested actuation inside them records only on the committed real pass
		// below — never twice. The galaxy scratch path suppresses its bind the same way.
		let scratch_binder = self.without_decision_recording();
		let probe_plan = scratch_binder
			.bind_plan_with_trace(
				candidate.probe,
				&mut scratch_trace,
				&mut scratch_render_trace,
				JoinOrderSourceCapture::On,
			)
			.await?;
		let Some(probe_label) = probe_plan.scope.unique_source_label() else {
			self.accrue_star_order_decline(
				OptimizerDecisionKind::StarCandidate,
				StarOrderDeclineReason::MissingRelationLabel,
			);
			self.render_star_order_decline(
				render_trace,
				StarOrderDeclineReason::MissingRelationLabel,
				candidate.dimensions.len(),
			);
			return Ok(StarOrderActuationResult::Declined);
		};
		if !broadcast_probe_is_scan_linear(&probe_plan.plan) {
			self.accrue_star_order_decline(
				OptimizerDecisionKind::StarCandidate,
				StarOrderDeclineReason::ProbeNotScanLinear,
			);
			self.render_star_order_decline(
				render_trace,
				StarOrderDeclineReason::ProbeNotScanLinear,
				candidate.dimensions.len(),
			);
			return Ok(StarOrderActuationResult::Declined);
		}

		let mut dimension_inputs = Vec::with_capacity(candidate.dimensions.len());
		for dimension in &candidate.dimensions {
			let dimension_plan = scratch_binder
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
			render_trace,
		)?
		else {
			return Ok(StarOrderActuationResult::Declined);
		};
		if tree.root_children.len() < 2 {
			self.accrue_star_order_decline(
				root_decision_kind(&tree),
				StarOrderDeclineReason::MissingRelationLabel,
			);
			self.render_star_order_decline(
				render_trace,
				StarOrderDeclineReason::MissingRelationLabel,
				candidate.dimensions.len(),
			);
			return Ok(StarOrderActuationResult::Declined);
		}

		// ADR 192: the old `force_hash_root` boolean did double duty — strategy authorization AND
		// render-decline suppression. Split them. `root_is_snowflake` keeps the render behavior
		// (suppress the flat-star decline row for a snowflake root, whose snowflake render is owned
		// elsewhere); a snowflake root actuates under the composite-build-fenced uniformity rule,
		// while a flat-star root keeps the strict ADR-181 rule.
		let root_is_snowflake = tree.has_branch_dimensions();
		let root_authorization = if root_is_snowflake {
			StarOrderStrategyAuthorization::CompositeBuildSnowflakeRoot
		} else {
			StarOrderStrategyAuthorization::AnyUniform
		};
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
			self.accrue_star_order_decline(
				root_decision_kind(&tree),
				StarOrderDeclineReason::MissingEstimate,
			);
			if !root_is_snowflake {
				self.render_star_order_decline(
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
			root_authorization,
			candidate.dimensions.len(),
			&mut scratch_trace,
			&mut scratch_render_trace,
		)?;
		if let LocalStarActuationAttempt::Declined(reason) = scratch_root_attempt {
			self.accrue_star_order_decline(root_decision_kind(&tree), reason);
			if !root_is_snowflake {
				self.render_star_order_decline(render_trace, reason, candidate.dimensions.len());
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
			root_authorization,
			candidate.dimensions.len(),
			trace,
			render_trace,
		)?;
		let LocalStarActuationAttempt::Actuated(real_root) = real_root_attempt else {
			if let LocalStarActuationAttempt::Declined(reason) = real_root_attempt {
				self.accrue_star_order_decline(root_decision_kind(&tree), reason);
			}
			return Ok(StarOrderActuationResult::Declined);
		};

		self.accrue_star_order_receipt(root_decision_kind(&tree), &real_root.receipt);
		for branch_receipt in &branch_receipts {
			self.accrue_star_order_receipt(OptimizerDecisionKind::SnowflakeBranch, branch_receipt);
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
						self.accrue_star_order_decline(
							OptimizerDecisionKind::StarCandidate,
							StarOrderDeclineReason::MissingRelationLabel,
						);
						self.render_star_order_decline(
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
						self.accrue_star_order_decline(
							OptimizerDecisionKind::StarCandidate,
							StarOrderDeclineReason::MissingRelationLabel,
						);
						self.render_star_order_decline(
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
				self.accrue_star_order_decline(
					OptimizerDecisionKind::StarCandidate,
					StarOrderDeclineReason::MissingRelationLabel,
				);
				self.render_star_order_decline(
					render_trace,
					StarOrderDeclineReason::MissingRelationLabel,
					candidate_dimensions.len(),
				);
				return Ok(None);
			};
			if !seen_labels.insert(dimension_label.clone()) {
				self.accrue_star_order_decline(
					OptimizerDecisionKind::StarCandidate,
					StarOrderDeclineReason::DuplicateDimension,
				);
				self.render_star_order_decline(
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
				let branch_authorization = if broadcast_probe_is_scan_linear(&base_plan.plan) {
					StarOrderStrategyAuthorization::AnyUniform
				} else {
					StarOrderStrategyAuthorization::HashOnlyCompositeProbe
				};
				match self.bind_local_star_actuation(
					base_plan.clone(),
					node.label.clone(),
					&bound_children,
					branch_authorization,
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
				composite_build: !child_dimensions.is_empty(),
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
			composite_build: dimension.composite_build,
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
		authorization: StarOrderStrategyAuthorization,
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
			Vec::new(),
			confidence,
			self.join_order_soft_margin_step.margin(),
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

		// ADR 192: a snowflake root uses the composite-build-fenced uniformity rule (accepts `Natural`
		// placement for the composite build); flat stars and branches keep the strict ADR-181 rule.
		let composite_builds = matches!(
			authorization,
			StarOrderStrategyAuthorization::CompositeBuildSnowflakeRoot
		);
		let declared = self.preflight_star_order_strategy(
			probe_plan.clone(),
			dimensions,
			&declared_indices,
			composite_builds,
			trace,
		)?;
		let Some(selected) = self.preflight_star_order_strategy(
			probe_plan.clone(),
			dimensions,
			&selected_indices,
			composite_builds,
			trace,
		)?
		else {
			return Ok(LocalStarActuationAttempt::Declined(
				StarOrderDeclineReason::MixedStrategyStar,
			));
		};
		// ADR 193: the SELECTED physical order is authoritative. Mixed strategy is order-variant —
		// `choose_auto_join_strategy` only broadcasts a dimension when its probe is scan-linear, so a
		// hash join earlier in the order forces later dimensions to hash. The cost-optimal order puts
		// broadcasts first and hashes last (a valid, fragmentable left-deep shape), even when the
		// declared order preflighted to all-hash. So a clean selected-order `Mixed` actuates, just like
		// ADR 192's selected-order `Broadcast` upgrade. The fingerprint is no longer an authorization
		// gate (it would block exactly this reorder); it is retained for receipt honesty.
		let fingerprint_match =
			declared.as_ref().map(|d| &d.fingerprint) == Some(&selected.fingerprint);
		// Typed authorization decides what is actuatable:
		// - `AnyUniform` (flat-star root): uniform (order-invariant, fingerprint matches) OR a clean
		//   selected `Mixed`.
		// - `CompositeBuildSnowflakeRoot` (snowflake root): ADR 192's selected `Broadcast` upgrade, OR
		//   a clean selected `Mixed`, OR a fingerprint-stable hash.
		// - `HashOnlyCompositeProbe` (branch): hash-only — mixed stays declined at branch level.
		let strategy_authorized = match authorization {
			StarOrderStrategyAuthorization::AnyUniform => {
				selected.summary == StarOrderStrategy::Mixed || fingerprint_match
			}
			StarOrderStrategyAuthorization::CompositeBuildSnowflakeRoot => {
				selected.summary == StarOrderStrategy::Broadcast
					|| selected.summary == StarOrderStrategy::Mixed
					|| fingerprint_match
			}
			StarOrderStrategyAuthorization::HashOnlyCompositeProbe => {
				selected.summary == StarOrderStrategy::HashPartitioned && fingerprint_match
			}
		};
		if !strategy_authorized {
			return Ok(LocalStarActuationAttempt::Declined(
				StarOrderDeclineReason::MixedStrategyStar,
			));
		}
		let selected_strategy = selected.summary;

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

		// ADR 193: per-dimension applied strategy/placement in applied order, for an honest mixed
		// (and uniform) receipt.
		let edge_strategies = applied_order
			.iter()
			.map(|label| {
				let (strategy, placement) = selected
					.fingerprint
					.get(label)
					.expect("selected preflight fingerprint covers applied order");
				(label.clone(), *strategy, *placement)
			})
			.collect::<Vec<_>>();
		let outcome = recognized_star_order_outcome(
			probe_label.clone(),
			evidence_dimensions,
			Some(applied_order),
			selected_strategy,
			edge_strategies,
			confidence,
			self.join_order_soft_margin_step.margin(),
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
		composite_builds: bool,
		base_trace: &OptimizerTrace,
	) -> Result<Option<PreflightStrategy>, BindingError> {
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
		// ADR 192: snowflake roots (composite builds) authenticate `Natural`-placement broadcast at
		// composite-build positions; flat stars keep the strict ADR-181 placement.
		let composite_build_positions = if composite_builds {
			order
				.iter()
				.enumerate()
				.filter_map(|(position, dimension_index)| {
					dimensions[*dimension_index]
						.composite_build
						.then_some(position)
				})
				.collect::<BTreeSet<_>>()
		} else {
			BTreeSet::new()
		};
		// ADR 193: validate placements + build the per-dimension fingerprint. `None` = an invalid
		// edge (e.g. unauthenticated `Broadcast + Natural`) → decline, never enter the mixed tier.
		let Some(fingerprint) = order_strategy_fingerprint(
			preflight_decisions,
			order,
			dimensions,
			&composite_build_positions,
		) else {
			return Ok(None);
		};
		// The strict ADR-181/192 uniform recognizers stay the fast path; a valid non-uniform set is
		// classified `Mixed` (ADR 193).
		let summary = if composite_builds {
			uniform_snowflake_order_strategy(preflight_decisions, &composite_build_positions)
		} else {
			uniform_star_order_strategy(preflight_decisions)
		}
		.unwrap_or(StarOrderStrategy::Mixed);
		Ok(Some(PreflightStrategy {
			summary,
			fingerprint,
		}))
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

	fn render_star_order_decline(
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

	fn accrue_star_order_receipt(
		&self,
		kind: OptimizerDecisionKind,
		receipt: &StarOrderRenderReceipt,
	) {
		if let StarOrderOutcome::Recognized(evidence) = &receipt.outcome {
			self.decision_recorder
				.record(kind, optimizer_decision_outcome_for_action(evidence.action));
		}
	}

	fn accrue_star_order_decline(
		&self,
		kind: OptimizerDecisionKind,
		reason: StarOrderDeclineReason,
	) {
		self.decision_recorder.record(
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
