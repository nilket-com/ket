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

pub(crate) fn is_tree_margin_advisory_kind(kind: OptimizerDecisionKind) -> bool {
	match kind {
		OptimizerDecisionKind::StarCandidate
		| OptimizerDecisionKind::FlatStar
		| OptimizerDecisionKind::SnowflakeRoot
		| OptimizerDecisionKind::SnowflakeBranch => true,
		OptimizerDecisionKind::GalaxyRoot
		| OptimizerDecisionKind::BridgeRoot
		| OptimizerDecisionKind::BridgeChainRoot
		| OptimizerDecisionKind::PreservedOuterOrder => false,
	}
}

pub(crate) fn is_graph_margin_advisory_kind(kind: OptimizerDecisionKind) -> bool {
	match kind {
		OptimizerDecisionKind::GalaxyRoot
		| OptimizerDecisionKind::BridgeRoot
		| OptimizerDecisionKind::BridgeChainRoot => true,
		OptimizerDecisionKind::StarCandidate
		| OptimizerDecisionKind::FlatStar
		| OptimizerDecisionKind::SnowflakeRoot
		| OptimizerDecisionKind::SnowflakeBranch
		| OptimizerDecisionKind::PreservedOuterOrder => false,
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarginAdvisoryRowKind {
	KnobAdvisory,
	KindDetail,
}

pub(crate) fn margin_advisory_row_kind_label(kind: MarginAdvisoryRowKind) -> &'static str {
	match kind {
		MarginAdvisoryRowKind::KnobAdvisory => "knob_advisory",
		MarginAdvisoryRowKind::KindDetail => "kind_detail",
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarginAdvisoryScope {
	TreeFamily,
	GraphFamily,
	GraphKind(OptimizerDecisionKind),
}

impl MarginAdvisoryScope {
	fn label(self) -> &'static str {
		match self {
			Self::TreeFamily => "tree_join_order",
			Self::GraphFamily => "graph_join_order",
			Self::GraphKind(kind) => optimizer_decision_kind_label(kind),
		}
	}

	fn row_kind(self) -> MarginAdvisoryRowKind {
		match self {
			Self::TreeFamily | Self::GraphFamily => MarginAdvisoryRowKind::KnobAdvisory,
			Self::GraphKind(_) => MarginAdvisoryRowKind::KindDetail,
		}
	}

	fn includes(self, kind: OptimizerDecisionKind) -> bool {
		match self {
			Self::TreeFamily => is_tree_margin_advisory_kind(kind),
			Self::GraphFamily => is_graph_margin_advisory_kind(kind),
			Self::GraphKind(scope_kind) => kind == scope_kind,
		}
	}

	fn is_actionable_knob(self) -> bool {
		matches!(self, Self::TreeFamily | Self::GraphFamily)
	}
}

struct MarginAdvisoryInput {
	rows: Vec<OptimizerRuntimeVerdictRow>,
	excluded_sample_size: u64,
}

fn is_margin_advisory_sampled_verdict(verdict: OptimizerRuntimeVerdictClass) -> bool {
	matches!(
		verdict,
		OptimizerRuntimeVerdictClass::MissedWin
			| OptimizerRuntimeVerdictClass::OverEager
			| OptimizerRuntimeVerdictClass::Vindicated
	)
}

fn margin_advisory_input(
	snapshot: &[OptimizerRuntimeVerdictRow],
	scope: MarginAdvisoryScope,
) -> MarginAdvisoryInput {
	let mut rows = Vec::new();
	let mut excluded_sample_size = 0_u64;
	for row in snapshot {
		if scope.includes(row.kind) {
			rows.push(row.clone());
		} else if is_margin_advisory_sampled_verdict(row.verdict) {
			excluded_sample_size = excluded_sample_size.saturating_add(row.count);
		}
	}
	MarginAdvisoryInput {
		rows,
		excluded_sample_size,
	}
}

fn tree_margin_advisory_input(snapshot: &[OptimizerRuntimeVerdictRow]) -> MarginAdvisoryInput {
	margin_advisory_input(snapshot, MarginAdvisoryScope::TreeFamily)
}

fn graph_margin_advisory_input(snapshot: &[OptimizerRuntimeVerdictRow]) -> MarginAdvisoryInput {
	margin_advisory_input(snapshot, MarginAdvisoryScope::GraphFamily)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EffectiveJoinOrderSoftMarginSteps {
	pub(crate) tree: SoftMarginLadderStep,
	pub(crate) graph: EffectiveGraphJoinOrderSoftMarginSteps,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EffectiveGraphJoinOrderSoftMarginSteps {
	pub(crate) family: SoftMarginLadderStep,
	pub(crate) galaxy: SoftMarginLadderStep,
	pub(crate) bridge: SoftMarginLadderStep,
	pub(crate) bridge_chain: SoftMarginLadderStep,
}

impl EffectiveGraphJoinOrderSoftMarginSteps {
	fn uniform(step: SoftMarginLadderStep) -> Self {
		Self {
			family: step,
			galaxy: step,
			bridge: step,
			bridge_chain: step,
		}
	}

	pub(crate) fn step_for(self, kind: OptimizerDecisionKind) -> SoftMarginLadderStep {
		match kind {
			OptimizerDecisionKind::GalaxyRoot => self.galaxy,
			OptimizerDecisionKind::BridgeRoot => self.bridge,
			OptimizerDecisionKind::BridgeChainRoot => self.bridge_chain,
			OptimizerDecisionKind::StarCandidate
			| OptimizerDecisionKind::FlatStar
			| OptimizerDecisionKind::SnowflakeRoot
			| OptimizerDecisionKind::SnowflakeBranch
			| OptimizerDecisionKind::PreservedOuterOrder => {
				unreachable!("graph margin requested for non-graph kind {kind:?}")
			}
		}
	}
}

// ADRs 215/218: the opt-in derivation of tree and graph soft-margin steps. `Manual` returns
// the ADR 190 configured base unchanged. `ApplyAdvisory` reads one snapshot, derives the tree
// family and graph family recommendations, then arms graph-kind steps only when the ADR 217
// graph divergence gate is actionable.
pub(crate) fn effective_join_order_soft_margin_steps(
	config: &PlannerConfig,
) -> EffectiveJoinOrderSoftMarginSteps {
	match config.join_order_soft_margin_application {
		JoinOrderSoftMarginApplication::Manual => EffectiveJoinOrderSoftMarginSteps {
			tree: config.join_order_soft_margin_step,
			graph: EffectiveGraphJoinOrderSoftMarginSteps::uniform(
				config.join_order_soft_margin_step,
			),
		},
		JoinOrderSoftMarginApplication::ApplyAdvisory => {
			let snapshot = config.runtime_evidence.receipt_gate_snapshot();
			let tree_input = tree_margin_advisory_input(&snapshot.verdicts);
			let graph_input = graph_margin_advisory_input(&snapshot.verdicts);
			let graph_family =
				recommend_soft_margin(&graph_input.rows, config.join_order_soft_margin_step)
					.recommended_step;
			let graph_gate = graph_margin_divergence_gate(&snapshot, config);
			EffectiveJoinOrderSoftMarginSteps {
				tree: recommend_soft_margin(&tree_input.rows, config.join_order_soft_margin_step)
					.recommended_step,
				graph: graph_kind_steps_from_gate(graph_family, &graph_gate),
			}
		}
	}
}

#[cfg(test)]
pub(crate) fn effective_tree_join_order_soft_margin_step(
	config: &PlannerConfig,
) -> SoftMarginLadderStep {
	effective_join_order_soft_margin_steps(config).tree
}

#[cfg(test)]
pub(crate) fn effective_graph_join_order_soft_margin_step(
	config: &PlannerConfig,
) -> SoftMarginLadderStep {
	effective_join_order_soft_margin_steps(config).graph.family
}

#[cfg(test)]
pub(crate) fn effective_graph_kind_join_order_soft_margin_step(
	config: &PlannerConfig,
	kind: OptimizerDecisionKind,
) -> SoftMarginLadderStep {
	effective_join_order_soft_margin_steps(config)
		.graph
		.step_for(kind)
}

// ADRs 208/213/215: report rows keep the configured base, scoped recommendation, application
// mode, and row-effective step together so the advisory table never recomputes from an
// already-applied margin (the Baseline -> Loose -> Loosest double-step trap).
pub(crate) struct MarginAdvisoryReport {
	pub(crate) advisory: MarginAdvisory,
	pub(crate) application: JoinOrderSoftMarginApplication,
	pub(crate) applied_step: SoftMarginLadderStep,
	pub(crate) evidence_scope: &'static str,
	pub(crate) excluded_sample_size: u64,
	pub(crate) row_kind: MarginAdvisoryRowKind,
}

fn margin_advisory_report_for_scope(
	snapshot: &[OptimizerRuntimeVerdictRow],
	config: &PlannerConfig,
	scope: MarginAdvisoryScope,
) -> MarginAdvisoryReport {
	let input = margin_advisory_input(snapshot, scope);
	let current_step = config.join_order_soft_margin_step;
	let advisory = recommend_soft_margin(&input.rows, current_step);
	let application = if scope.is_actionable_knob() {
		config.join_order_soft_margin_application
	} else {
		JoinOrderSoftMarginApplication::Manual
	};
	let applied_step = match application {
		JoinOrderSoftMarginApplication::Manual => current_step,
		JoinOrderSoftMarginApplication::ApplyAdvisory => advisory.recommended_step,
	};
	MarginAdvisoryReport {
		advisory,
		application,
		applied_step,
		evidence_scope: scope.label(),
		excluded_sample_size: input.excluded_sample_size,
		row_kind: scope.row_kind(),
	}
}

#[cfg(test)]
pub(crate) fn margin_advisory_report(config: &PlannerConfig) -> MarginAdvisoryReport {
	let snapshot = config.runtime_evidence.verdict_snapshot();
	margin_advisory_report_for_scope(&snapshot, config, MarginAdvisoryScope::TreeFamily)
}

pub(crate) fn margin_advisory_report_rows(config: &PlannerConfig) -> Vec<MarginAdvisoryReport> {
	let snapshot = config.runtime_evidence.verdict_snapshot();
	[
		MarginAdvisoryScope::TreeFamily,
		MarginAdvisoryScope::GraphFamily,
		MarginAdvisoryScope::GraphKind(OptimizerDecisionKind::GalaxyRoot),
		MarginAdvisoryScope::GraphKind(OptimizerDecisionKind::BridgeRoot),
		MarginAdvisoryScope::GraphKind(OptimizerDecisionKind::BridgeChainRoot),
	]
	.into_iter()
	.map(|scope| margin_advisory_report_for_scope(&snapshot, config, scope))
	.collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OptimizerReceiptGateStatus {
	InsufficientEvidence,
	Inactive,
	Actionable,
	BlockedExternalSignal,
}

pub(crate) fn optimizer_receipt_gate_status_label(
	status: OptimizerReceiptGateStatus,
) -> &'static str {
	match status {
		OptimizerReceiptGateStatus::InsufficientEvidence => "insufficient_evidence",
		OptimizerReceiptGateStatus::Inactive => "inactive",
		OptimizerReceiptGateStatus::Actionable => "actionable",
		OptimizerReceiptGateStatus::BlockedExternalSignal => "blocked_external_signal",
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerReceiptGateRow {
	pub(crate) gate_name: &'static str,
	pub(crate) gate_family: &'static str,
	pub(crate) status: OptimizerReceiptGateStatus,
	pub(crate) sample_count: u64,
	pub(crate) primary_signal: String,
	pub(crate) secondary_signal: String,
	pub(crate) reason: String,
	pub(crate) next_cut: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GraphKindGateSignal {
	kind: OptimizerDecisionKind,
	recommendation: MarginRecommendation,
	recommended_step: SoftMarginLadderStep,
	sample_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GraphMarginDivergenceGate {
	pub(crate) status: OptimizerReceiptGateStatus,
	pub(crate) ready: Vec<GraphKindGateSignal>,
}

impl GraphMarginDivergenceGate {
	fn sample_count(&self) -> u64 {
		self.ready
			.iter()
			.fold(0_u64, |acc, signal| acc.saturating_add(signal.sample_size))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecisionConflictGateSignal {
	kind: OptimizerDecisionKind,
	provenance: OptimizerRuntimeVerdictProvenance,
	reordered: u64,
	blocked_or_near_miss: u64,
}

impl DecisionConflictGateSignal {
	fn sample_count(self) -> u64 {
		self.reordered.saturating_add(self.blocked_or_near_miss)
	}

	fn conflict_strength(self) -> u64 {
		self.reordered.min(self.blocked_or_near_miss)
	}
}

pub(crate) fn optimizer_receipt_gate_rows(config: &PlannerConfig) -> Vec<OptimizerReceiptGateRow> {
	let snapshot = config.runtime_evidence.receipt_gate_snapshot();
	vec![
		graph_margin_divergence_gate_row(&snapshot, config),
		preserved_outer_ordering_gate(&snapshot, config),
		decision_conflict_pressure_gate(&snapshot),
		windowing_decay_pressure_gate(&snapshot),
		counter_handoff_readiness_gate(&snapshot),
	]
}

pub(crate) fn graph_margin_divergence_gate(
	snapshot: &OptimizerReceiptGateSnapshot,
	config: &PlannerConfig,
) -> GraphMarginDivergenceGate {
	let ready = graph_kind_gate_signals(&snapshot.verdicts, config);
	let has_divergence = ready.iter().any(|left| {
		ready.iter().any(|right| {
			left.kind != right.kind
				&& (left.recommendation != right.recommendation
					|| left.recommended_step != right.recommended_step)
		})
	});
	let status = if ready.len() < 2 {
		OptimizerReceiptGateStatus::InsufficientEvidence
	} else if has_divergence {
		OptimizerReceiptGateStatus::Actionable
	} else {
		OptimizerReceiptGateStatus::Inactive
	};
	GraphMarginDivergenceGate { status, ready }
}

fn graph_margin_divergence_gate_row(
	snapshot: &OptimizerReceiptGateSnapshot,
	config: &PlannerConfig,
) -> OptimizerReceiptGateRow {
	let gate = graph_margin_divergence_gate(snapshot, config);
	let sample_count = gate.sample_count();
	let reason = match gate.status {
		OptimizerReceiptGateStatus::InsufficientEvidence => "requires_two_ready_graph_kind_rows",
		OptimizerReceiptGateStatus::Actionable => "graph_kind_recommendations_diverge",
		OptimizerReceiptGateStatus::Inactive => "graph_kind_recommendations_converge",
		OptimizerReceiptGateStatus::BlockedExternalSignal => unreachable!(),
	};
	OptimizerReceiptGateRow {
		gate_name: "graph_margin_divergence",
		gate_family: "graph_join_order",
		status: gate.status,
		sample_count,
		primary_signal: format!("ready_kind_count={}", gate.ready.len()),
		secondary_signal: graph_kind_signal_label(&gate.ready, &snapshot.health),
		reason: reason.to_string(),
		next_cut: "per_kind_graph_margin",
	}
}

fn graph_kind_steps_from_gate(
	family: SoftMarginLadderStep,
	gate: &GraphMarginDivergenceGate,
) -> EffectiveGraphJoinOrderSoftMarginSteps {
	if gate.status != OptimizerReceiptGateStatus::Actionable {
		return EffectiveGraphJoinOrderSoftMarginSteps::uniform(family);
	}
	let mut steps = EffectiveGraphJoinOrderSoftMarginSteps::uniform(family);
	for signal in &gate.ready {
		match signal.kind {
			OptimizerDecisionKind::GalaxyRoot => steps.galaxy = signal.recommended_step,
			OptimizerDecisionKind::BridgeRoot => steps.bridge = signal.recommended_step,
			OptimizerDecisionKind::BridgeChainRoot => steps.bridge_chain = signal.recommended_step,
			OptimizerDecisionKind::StarCandidate
			| OptimizerDecisionKind::FlatStar
			| OptimizerDecisionKind::SnowflakeRoot
			| OptimizerDecisionKind::SnowflakeBranch
			| OptimizerDecisionKind::PreservedOuterOrder => {
				unreachable!("graph gate signal carried non-graph kind {:?}", signal.kind)
			}
		}
	}
	steps
}

fn graph_kind_gate_signals(
	verdicts: &[OptimizerRuntimeVerdictRow],
	config: &PlannerConfig,
) -> Vec<GraphKindGateSignal> {
	[
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionKind::BridgeChainRoot,
	]
	.into_iter()
	.filter_map(|kind| {
		let report = margin_advisory_report_for_scope(
			verdicts,
			config,
			MarginAdvisoryScope::GraphKind(kind),
		);
		(report.advisory.sample_size >= JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE).then_some(
			GraphKindGateSignal {
				kind,
				recommendation: report.advisory.recommendation,
				recommended_step: report.advisory.recommended_step,
				sample_size: report.advisory.sample_size,
			},
		)
	})
	.collect()
}

fn graph_kind_signal_label(
	signals: &[GraphKindGateSignal],
	health: &OptimizerEvidenceStatusSnapshot,
) -> String {
	let mut parts = signals
		.iter()
		.map(|signal| {
			format!(
				"{}={}:{}",
				optimizer_decision_kind_label(signal.kind),
				margin_recommendation_label(signal.recommendation),
				signal.recommended_step.margin().label()
			)
		})
		.collect::<Vec<_>>();
	parts.push(format!("evidence_health={}", evidence_health_label(health)));
	parts.join(",")
}

fn preserved_outer_ordering_gate(
	snapshot: &OptimizerReceiptGateSnapshot,
	config: &PlannerConfig,
) -> OptimizerReceiptGateRow {
	let report = margin_advisory_report_for_scope(
		&snapshot.verdicts,
		config,
		MarginAdvisoryScope::GraphKind(OptimizerDecisionKind::PreservedOuterOrder),
	);
	let status = if report.advisory.sample_size < JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE {
		OptimizerReceiptGateStatus::InsufficientEvidence
	} else if report.advisory.recommendation == MarginRecommendation::Loosen {
		OptimizerReceiptGateStatus::Actionable
	} else {
		OptimizerReceiptGateStatus::Inactive
	};
	let reason = match status {
		OptimizerReceiptGateStatus::InsufficientEvidence => {
			"requires_ready_preserved_outer_ordering_samples"
		}
		OptimizerReceiptGateStatus::Actionable => "preserved_outer_missed_win_ready",
		OptimizerReceiptGateStatus::Inactive => "preserved_outer_recommendation_not_loosen",
		OptimizerReceiptGateStatus::BlockedExternalSignal => unreachable!(),
	};
	OptimizerReceiptGateRow {
		gate_name: "preserved_outer_ordering",
		gate_family: "preserved_outer_join_order",
		status,
		sample_count: report.advisory.sample_size,
		primary_signal: format!(
			"recommendation={}",
			margin_recommendation_label(report.advisory.recommendation)
		),
		secondary_signal: format!(
			"missed_win={},over_eager={},vindicated={},evidence_health={}",
			report.advisory.missed_win,
			report.advisory.over_eager,
			report.advisory.vindicated,
			evidence_health_label(&snapshot.health)
		),
		reason: reason.to_string(),
		next_cut: "safe_side_outer_ordering",
	}
}

fn decision_conflict_pressure_gate(
	snapshot: &OptimizerReceiptGateSnapshot,
) -> OptimizerReceiptGateRow {
	let strongest = strongest_decision_conflict_signal(&snapshot.decisions);
	let actionable = strongest.filter(|signal| {
		signal.reordered >= JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE
			&& signal.blocked_or_near_miss >= JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE
	});
	let selected = actionable.or(strongest);
	let status = if actionable.is_some() {
		OptimizerReceiptGateStatus::Actionable
	} else {
		OptimizerReceiptGateStatus::InsufficientEvidence
	};
	let (sample_count, primary_signal, secondary_signal) = match selected {
		Some(signal) => (
			signal.sample_count(),
			format!(
				"kind={},provenance={}",
				optimizer_decision_kind_label(signal.kind),
				optimizer_runtime_verdict_provenance_label(signal.provenance)
			),
			format!(
				"reordered={},blocked_or_near_miss={},evidence_health={}",
				signal.reordered,
				signal.blocked_or_near_miss,
				evidence_health_label(&snapshot.health)
			),
		),
		None => (
			0,
			"no_decision_rows".to_string(),
			format!(
				"evidence_health={}",
				evidence_health_label(&snapshot.health)
			),
		),
	};
	OptimizerReceiptGateRow {
		gate_name: "decision_conflict_pressure",
		gate_family: "join_order_decisions",
		status,
		sample_count,
		primary_signal,
		secondary_signal,
		reason: if actionable.is_some() {
			"reordered_and_blocked_ready".to_string()
		} else {
			"requires_ready_reordered_and_blocked_decision_rows".to_string()
		},
		next_cut: "memo_conflict_readout",
	}
}

fn strongest_decision_conflict_signal(
	decisions: &[OptimizerDecisionRow],
) -> Option<DecisionConflictGateSignal> {
	let mut grouped: BTreeMap<
		(OptimizerDecisionKind, OptimizerRuntimeVerdictProvenance),
		DecisionConflictGateSignal,
	> = BTreeMap::new();
	for row in decisions {
		let entry =
			grouped
				.entry((row.kind, row.provenance))
				.or_insert(DecisionConflictGateSignal {
					kind: row.kind,
					provenance: row.provenance,
					reordered: 0,
					blocked_or_near_miss: 0,
				});
		match row.outcome {
			OptimizerDecisionOutcome::Reordered => {
				entry.reordered = entry.reordered.saturating_add(row.count);
			}
			OptimizerDecisionOutcome::KeptThinMargin | OptimizerDecisionOutcome::Declined(_) => {
				entry.blocked_or_near_miss = entry.blocked_or_near_miss.saturating_add(row.count);
			}
			OptimizerDecisionOutcome::KeptOptimal => {}
		}
	}
	grouped
		.into_values()
		.filter(|signal| signal.sample_count() > 0)
		.max_by(|left, right| compare_decision_conflict_signal(*left, *right))
}

fn compare_decision_conflict_signal(
	left: DecisionConflictGateSignal,
	right: DecisionConflictGateSignal,
) -> Ordering {
	left.conflict_strength()
		.cmp(&right.conflict_strength())
		.then_with(|| left.sample_count().cmp(&right.sample_count()))
		.then_with(|| right.kind.cmp(&left.kind))
		.then_with(|| right.provenance.cmp(&left.provenance))
}

fn windowing_decay_pressure_gate(
	snapshot: &OptimizerReceiptGateSnapshot,
) -> OptimizerReceiptGateRow {
	OptimizerReceiptGateRow {
		gate_name: "windowing_decay_pressure",
		gate_family: "evidence_windowing",
		status: OptimizerReceiptGateStatus::Inactive,
		sample_count: snapshot
			.verdicts
			.iter()
			.map(|row| row.count)
			.chain(snapshot.decisions.iter().map(|row| row.count))
			.fold(0_u64, |acc, count| acc.saturating_add(count)),
		primary_signal: "windowing_substrate=unbuilt".to_string(),
		secondary_signal: format!(
			"evidence_health={}",
			evidence_health_label(&snapshot.health)
		),
		reason: "windowing_decay_substrate_unbuilt".to_string(),
		next_cut: "windowing_decay_substrate",
	}
}

fn counter_handoff_readiness_gate(
	snapshot: &OptimizerReceiptGateSnapshot,
) -> OptimizerReceiptGateRow {
	OptimizerReceiptGateRow {
		gate_name: "counter_handoff_readiness",
		gate_family: "counter_handoff",
		status: OptimizerReceiptGateStatus::BlockedExternalSignal,
		sample_count: 0,
		primary_signal: "elasticity_signal=missing".to_string(),
		secondary_signal: format!(
			"evidence_health={}",
			evidence_health_label(&snapshot.health)
		),
		reason: "elasticity_signal_unbuilt".to_string(),
		next_cut: "counter_handoff_elasticity_signal",
	}
}

fn evidence_health_label(health: &OptimizerEvidenceStatusSnapshot) -> String {
	format!("{}/{}", health.mode.as_str(), health.state.as_str())
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
	pub(super) join_kind: JoinKind,
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
	// ADR 199: the per-edge join-result estimates (`F⋈B` incoming, `B⋈D` outgoing) — the actuation cost
	// basis. `bridge_rows`/`fact_rows` above are the 196 endpoint-row recognition gate (is `B` a
	// bridge?); these edge estimates answer the distinct ordering question (which root is cheaper?).
	pub(super) incoming_edge_rows: Option<RowCount>,
	pub(super) outgoing_edge_rows: Option<RowCount>,
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

// The relation label of a galaxy leaf (a `FROM`-clause scan, possibly aliased, and possibly wrapped in a
// pushed `Filter`/`Project` — e.g. a selective-dimension bridge re-root). Recurse through the
// volume-preserving wrappers to the relation, but stop at an `Alias` (it renames the relation). `None`
// for a non-leaf or unnameable node → the re-root declines.
fn galaxy_leaf_label(plan: &LogicalPlan) -> Option<String> {
	match plan {
		LogicalPlan::ScanNamed { table, .. } => Some(relation_key_label(table)),
		LogicalPlan::Alias { alias, .. } => Some(relation_key_label(alias)),
		LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
			galaxy_leaf_label(input)
		}
		_ => None,
	}
}

// ADR 198/202: re-root a left-deep conformed galaxy chain `Join(Join(…Join(F1,D),F2)…,Fn)` to lead with
// `preferred_root` (the cost-optimal fact), keeping the conformed dimension `D = pivot` as the pivot.
// Every fact joins only `D`, so the chain reassociates: collect each fact leaf with its canonical
// `(fact_key, dim_key)` join condition (and each join's kind/strategy by position), then rebuild
// left-deep with the preferred fact as the base driver — each `on` re-oriented for its new left/right
// position (the column refs are logical and re-resolved at bind, as in 198). The 2-fact galaxy and the
// 3-relation bridge are the `n=2` instance and produce byte-identical output. Returns `None` for any
// shape that is not a left-deep all-leaf inner-join chain on `pivot` → decline, AST order.
pub(super) fn reroot_galaxy_join_tree(
	plan: &LogicalPlan,
	pivot: &str,
	preferred_root: &str,
) -> Option<LogicalPlan> {
	// Walk the chain outermost → innermost. Each outer `Join(composite, Fi)` contributes the fact `Fi`
	// (D sits in the left composite, so canonical `(fact, dim) = (on.right, on.left)`); the innermost
	// `Join(F1, D)` contributes the base fact `F1` (`(on.left, on.right)`) and the dimension `D`.
	let mut fact_leaves: Vec<LogicalPlan> = Vec::new();
	let mut fact_on: Vec<Vec<(Expr, Expr)>> = Vec::new();
	let mut join_meta: Vec<(JoinKind, JoinStrategyIntent)> = Vec::new();
	let mut node = plan;
	let dim_leaf;
	loop {
		let LogicalPlan::Join {
			left,
			right,
			kind,
			on,
			strategy_intent,
		} = node
		else {
			return None;
		};
		if *kind != JoinKind::Inner || logical_plan_contains_join(right) {
			return None;
		}
		join_meta.push((*kind, *strategy_intent));
		if matches!(left.as_ref(), LogicalPlan::Join { .. }) {
			fact_leaves.push((**right).clone());
			fact_on.push(
				on.iter()
					.map(|(left, right)| (right.clone(), left.clone()))
					.collect(),
			);
			node = left.as_ref();
		} else {
			if logical_plan_contains_join(left) {
				return None;
			}
			fact_leaves.push((**left).clone());
			fact_on.push(
				on.iter()
					.map(|(left, right)| (left.clone(), right.clone()))
					.collect(),
			);
			dim_leaf = (**right).clone();
			break;
		}
	}
	// The conformed dimension must be the recommended pivot.
	if galaxy_leaf_label(&dim_leaf).as_deref() != Some(pivot) {
		return None;
	}
	// Reverse to innermost-first (FROM order); join_meta is positional (innermost join first).
	fact_leaves.reverse();
	fact_on.reverse();
	join_meta.reverse();
	// Order: preferred root first, then the remaining facts in their FROM order.
	let preferred_index = fact_leaves
		.iter()
		.position(|leaf| galaxy_leaf_label(leaf).as_deref() == Some(preferred_root))?;
	let mut order = Vec::with_capacity(fact_leaves.len());
	order.push(preferred_index);
	order.extend((0..fact_leaves.len()).filter(|&i| i != preferred_index));
	// Rebuild left-deep. Each join's kind/strategy travels with the fact whose original join step it
	// belonged to (`join_meta[idx]`, aligned with `fact_leaves[idx]`), NOT the rebuilt position — so a
	// pinned/mixed `strategy_intent` stays on its own fact edge after the re-root.
	let base_idx = order[0];
	let (base_kind, base_intent) = join_meta[base_idx];
	let mut composite = LogicalPlan::Join {
		left: Box::new(fact_leaves[base_idx].clone()),
		right: Box::new(dim_leaf.clone()),
		kind: base_kind,
		on: fact_on[base_idx].clone(),
		strategy_intent: base_intent,
	};
	for &idx in order.iter().skip(1) {
		let (kind, intent) = join_meta[idx];
		composite = LogicalPlan::Join {
			left: Box::new(composite),
			right: Box::new(fact_leaves[idx].clone()),
			kind,
			// D now sits in the left composite, so the fact joins from the right: `(dim, fact)`.
			on: fact_on[idx]
				.iter()
				.map(|(fact, dim)| (dim.clone(), fact.clone()))
				.collect(),
			strategy_intent: intent,
		};
	}
	Some(composite)
}

// ADR 205: reverse a left-deep bridge chain `Join(…Join(Join(F,B1),B2)…,D)` to drive from the `D` end:
// `Join(…Join(Join(D,Bk),B_{k-1})…,F)`. Unlike `reroot_galaxy_join_tree` (a star rotation around one
// `pivot`), a chain is a PATH with no pivot, so this is a full reversal — every edge's `on` is swapped for
// its flipped left/right position. Collect the chain's leaves (outermost-first `[D, Bk, …, B1]`), per-edge
// `on`, and per-join `(kind, strategy_intent)` by walking the left spine, then rebuild left-deep from the
// `D` leaf, carrying each `(kind, intent)` WITH its original edge (never by rebuilt position — the 202 F1
// trap). `None` for any shape that is not a left-deep all-leaf inner chain whose `D` end is `preferred_root`.
pub(super) fn reverse_bridge_chain_join_tree(
	plan: &LogicalPlan,
	preferred_root: &str,
) -> Option<LogicalPlan> {
	let mut leaves: Vec<LogicalPlan> = Vec::new();
	let mut conds: Vec<Vec<(Expr, Expr)>> = Vec::new();
	let mut join_meta: Vec<(JoinKind, JoinStrategyIntent)> = Vec::new();
	let mut node = plan;
	let base_leaf;
	loop {
		let LogicalPlan::Join {
			left,
			right,
			kind,
			on,
			strategy_intent,
		} = node
		else {
			return None;
		};
		if *kind != JoinKind::Inner || logical_plan_contains_join(right) {
			return None;
		}
		join_meta.push((*kind, *strategy_intent));
		leaves.push((**right).clone());
		conds.push(on.clone());
		if matches!(left.as_ref(), LogicalPlan::Join { .. }) {
			node = left.as_ref();
		} else {
			if logical_plan_contains_join(left) {
				return None;
			}
			base_leaf = (**left).clone(); // the fact end `F`
			break;
		}
	}
	// `leaves[0]` is the outermost right leaf — the `D` end the recommendation drives from.
	if galaxy_leaf_label(&leaves[0]).as_deref() != Some(preferred_root) {
		return None;
	}
	// Each edge's `on` swaps (its two relations flip left/right in the reversal).
	let swap = |on: &[(Expr, Expr)]| -> Vec<(Expr, Expr)> {
		on.iter().map(|(l, r)| (r.clone(), l.clone())).collect()
	};
	// Rebuild left-deep from `D`: attach the bridges `Bk…B1` (leaves[1..]) via their own edges, then the
	// base fact `F` via the innermost edge. `(kind, intent)` travels with each edge index.
	let mut composite = leaves[0].clone();
	for i in 1..leaves.len() {
		let (kind, intent) = join_meta[i - 1];
		composite = LogicalPlan::Join {
			left: Box::new(composite),
			right: Box::new(leaves[i].clone()),
			kind,
			on: swap(&conds[i - 1]),
			strategy_intent: intent,
		};
	}
	let (kind, intent) = *join_meta.last()?;
	Some(LogicalPlan::Join {
		left: Box::new(composite),
		right: Box::new(base_leaf),
		kind,
		on: swap(conds.last()?),
		strategy_intent: intent,
	})
}

// ADR 206: build the REVERSED chain evidence for an actuated chain's verdict. The reversed plan does not
// re-recognize (`analyze_bridge_chain` declines — interior junctions fail inverse-F1 in reverse), so we
// reverse the recognized AST `chain` structurally (reverse hop order, swap each hop's `fact↔dim` +
// `in↔out` signature) and re-resolve `incoming_target`/`outgoing_target` + edge-row estimates against the
// REVERSED plan's `reversed_edges` (matched by relation label), so the verdict keys its measured costs to
// the joins that actually ran. `None` if any reversed edge can't be matched. Used only for the verdict —
// the receipt keeps the AST evidence so the `chain[F->…->D]` path label is unchanged.
pub(super) fn reverse_chain_evidence(
	chain: &BridgeChainEvidence,
	reversed_edges: &[JoinOrderEdgeEvidence],
) -> Option<BridgeChainEvidence> {
	let edge_between = |a: &str, b: &str| -> Option<&JoinOrderEdgeEvidence> {
		reversed_edges.iter().find(|edge| {
			match (edge.probe.as_deref(), edge.dimension.as_deref()) {
				(Some(p), Some(d)) => (p == a && d == b) || (p == b && d == a),
				_ => false,
			}
		})
	};
	let mut hops = Vec::with_capacity(chain.hops.len());
	for ast in chain.hops.iter().rev() {
		// Reversed direction: the AST `dim` is now the fact end, the AST `fact` is now the dim end.
		let fact = ast.dim.clone();
		let bridge = ast.bridge.clone();
		let dim = ast.fact.clone();
		let incoming = edge_between(&fact, &bridge)?;
		let outgoing = edge_between(&bridge, &dim)?;
		hops.push(BridgeLink {
			fact,
			bridge,
			dim,
			b_in_signature: ast.b_out_signature.clone(),
			b_out_signature: ast.b_in_signature.clone(),
			bridge_rows: ast.bridge_rows,
			fact_rows: ast.fact_rows,
			incoming_edge_rows: incoming.estimated_rows,
			outgoing_edge_rows: outgoing.estimated_rows,
			incoming_target: incoming.target,
			outgoing_target: outgoing.target,
		});
	}
	Some(BridgeChainEvidence { hops })
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
			incoming_edge_rows: incoming.estimated_rows,
			outgoing_edge_rows: outgoing.estimated_rows,
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

// ADR 204: the supported breadth for the bounded chain recognizer — the cut vouches for the compounded
// cost model only this deep, declining deeper chains rather than emitting an unvalidated verdict. This does
// NOT make the walk finite: the walk consumes distinct relations and declines on repeats, so it is
// intrinsically `O(links)`. The cap is the claim boundary, not the termination bound.
const MAX_BRIDGE_CHAIN_HOPS: usize = 4;

// ADR 203/204: a recognized bounded N-hop bridge chain `F ⋈ B1 ⋈ … ⋈ Bk ⋈ D` — the ordered path as its
// constituent bridge links (`hops[0] = F→B1→B2`, …, `hops[k-1] = B_{k-1}→Bk→D`). Sibling of
// `BridgeOrderEvidence`; observe-only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BridgeChainEvidence {
	pub(super) hops: Vec<BridgeLink>,
}

// ADR 203: recognize a bounded 2-hop bridge chain by composing `analyze_bridge`'s links into an ordered
// path. `analyze_bridge` returns links in **relation-set order, not path order** (its loop walks a
// `BTreeSet` of relation names), so they must be stitched: the chain head is the link whose `fact` is no
// link's `bridge` (the free fact end `F`), and the successor stitches on `next.fact == head.bridge`,
// contiguous via `head.dim == next.bridge`. Accept exactly one linear 2-hop chain over four distinct
// relations; `None` for a single junction, branching, a cycle, or longer/bushier shapes (the chain
// re-root and longer chains are deferred). Recognition only — no ordering, no actuation.
pub(super) fn analyze_bridge_chain(
	edges: &[JoinOrderEdgeEvidence],
	decisions: &[DecisionReceipt],
) -> Option<BridgeChainEvidence> {
	let links = analyze_bridge(edges, decisions)?.links;
	// Bounded: at least two junctions (a single junction is a 199 bridge), at most the supported breadth.
	if links.len() < 2 || links.len() > MAX_BRIDGE_CHAIN_HOPS {
		return None;
	}
	// Index links by their probed `fact` for O(1) successor lookup. A reused `fact` is a branch → decline.
	let mut by_fact: BTreeMap<&str, usize> = BTreeMap::new();
	for (i, link) in links.iter().enumerate() {
		if by_fact.insert(link.fact.as_str(), i).is_some() {
			return None;
		}
	}
	// The head is the unique link whose `fact` is no junction's `bridge` (the free fact end `F`).
	let bridges: BTreeSet<&str> = links.iter().map(|link| link.bridge.as_str()).collect();
	let mut head_indices = (0..links.len()).filter(|&i| !bridges.contains(links[i].fact.as_str()));
	let head_idx = head_indices.next()?;
	if head_indices.next().is_some() {
		return None;
	}
	// Walk the single connected path from the head, stitching `next.fact == current.bridge` (guaranteed by
	// the `by_fact` lookup) and contiguous via `current.dim == next.bridge`, until no successor. The walk
	// must consume EVERY link — a leftover is a disconnected second component (a chain co-occurring with an
	// unrelated bridge) and declines. Termination is structural: relations are distinct, so it cannot loop.
	let mut order = vec![head_idx];
	let mut current = &links[head_idx];
	while let Some(&next_idx) = by_fact.get(current.bridge.as_str()) {
		let next = &links[next_idx];
		if current.dim != next.bridge {
			return None;
		}
		order.push(next_idx);
		current = next;
	}
	if order.len() != links.len() {
		return None;
	}
	// All `k + 2` relations distinct (no cycle): the head fact, every bridge, the tail dim.
	let tail = &links[*order.last()?];
	let mut relations: BTreeSet<&str> = BTreeSet::new();
	relations.insert(links[head_idx].fact.as_str());
	relations.insert(tail.dim.as_str());
	for &i in &order {
		relations.insert(links[i].bridge.as_str());
	}
	if relations.len() != order.len() + 2 {
		return None;
	}
	Some(BridgeChainEvidence {
		hops: order.iter().map(|&i| links[i].clone()).collect(),
	})
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

// Bridge order cost (ADR 199): root the relation whose join *into the bridge* is the smaller build.
// The FROM order roots the fact `F`, materializing `F⋈B` (the incoming edge) before the dimension
// filters; rooting the dimension `D` instead materializes `D⋈B` (the outgoing edge) first. Prefer `D`
// iff `D⋈B` is cheaper than `F⋈B` — a relation-rooted edge-estimate decision, the galaxy's
// `edge_estimate_between` discipline applied to the bridge's two incident edges (not the endpoint-row
// `bridge_rows`/`fact_rows`, which is the 196 *recognition* gate and is blind to the dimension's
// selectivity). Conservative: a missing edge estimate yields no recommendation — never actuate on an
// unknown cost.
pub(super) fn bridge_order_recommendation(
	bridge: &BridgeOrderEvidence,
) -> Option<GraphOrderRecommendation> {
	let link = bridge.links.first()?;
	let fact_root_cost = u128::from(link.incoming_edge_rows?.get());
	let dim_root_cost = u128::from(link.outgoing_edge_rows?.get());
	let action = if dim_root_cost < fact_root_cost {
		StarOrderAction::Reordered
	} else {
		StarOrderAction::KeptAlreadyOptimal
	};
	let preferred_root = match action {
		StarOrderAction::Reordered => link.dim.clone(),
		_ => link.fact.clone(),
	};
	Some(GraphOrderRecommendation {
		pivot: link.bridge.clone(),
		declared_root: link.fact.clone(),
		preferred_root,
		costs: vec![
			(link.fact.clone(), fact_root_cost),
			(link.dim.clone(), dim_root_cost),
		],
		action,
	})
}

// ADR 203: path ordering for a 2-hop bridge chain `F ⋈ B1 ⋈ B2 ⋈ D` — drive from the cheaper end.
// `declared_root` is the AST `F` end; the candidates are the two chain ends, costed by their *entry*
// hop: `F`-end = `F⋈B1` (head incoming edge), `D`-end = `B2⋈D` (tail outgoing edge). This generalizes the
// single-bridge filter-first rule (drive the selective end) to the path; it is a deliberate heuristic
// over the compounded fan-out, *tested* by the measured chain verdict (D3), not claimed optimal.
// Conservative: a missing edge estimate yields no recommendation.
pub(super) fn bridge_chain_order_recommendation(
	chain: &BridgeChainEvidence,
) -> Option<GraphOrderRecommendation> {
	let head = chain.hops.first()?;
	let tail = chain.hops.last()?;
	let fact_end_cost = u128::from(head.incoming_edge_rows?.get());
	let dim_end_cost = u128::from(tail.outgoing_edge_rows?.get());
	let action = if dim_end_cost < fact_end_cost {
		StarOrderAction::Reordered
	} else {
		StarOrderAction::KeptAlreadyOptimal
	};
	let preferred_root = match action {
		StarOrderAction::Reordered => tail.dim.clone(),
		_ => head.fact.clone(),
	};
	Some(GraphOrderRecommendation {
		pivot: chain
			.hops
			.iter()
			.map(|hop| hop.bridge.clone())
			.collect::<Vec<_>>()
			.join("->"),
		declared_root: head.fact.clone(),
		preferred_root,
		costs: vec![
			(head.fact.clone(), fact_end_cost),
			(tail.dim.clone(), dim_end_cost),
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

// ADR 200: the committed outcome of a graph actuation attempt. Unlike a two-way candidate/actuated
// enum, the keep and decline cases are explicit so accrual records the *committed* decision faithfully
// (a soft-margin keep is `KeptThinMargin`, a can't-evaluate is `Declined`) rather than inferring it from
// a collapsed `NotCandidate`.
#[derive(Debug, Clone)]
pub(super) enum GraphActuationResult {
	// The node is not a graph of this shape — no decision, no accrual.
	NotCandidate,
	Actuated(ScopedExecutionPlan),
	// Recognized; the FROM root is already cost-optimal.
	KeptAlreadyOptimal,
	// Recognized; a cheaper root exists but does not clear the soft margin.
	KeptThinMargin,
	// Recognized; could not be costed (missing estimate) or could not re-root.
	Declined(OptimizerDecisionDeclineReason),
}

// ADR 200/205: the graph shape the unified actuator dispatches over. A query is a galaxy, a single-junction
// bridge, or a multi-junction chain — a 3-way partition (ADR 205 gates Bridge to one junction so a chain is
// Chain's exclusively), so the arms are mutually exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GraphShape {
	Galaxy,
	Bridge,
	Chain,
}

impl GraphShape {
	fn decision_kind(self) -> OptimizerDecisionKind {
		match self {
			GraphShape::Galaxy => OptimizerDecisionKind::GalaxyRoot,
			GraphShape::Bridge => OptimizerDecisionKind::BridgeRoot,
			GraphShape::Chain => OptimizerDecisionKind::BridgeChainRoot,
		}
	}

	// Recognize the shape over the scratch trace. `None` → not this shape (NotCandidate); `Some(None)` →
	// recognized but not costable (Declined); `Some(Some(rec))` → recognized with a recommendation.
	fn recommend(
		self,
		render: &OptimizerExplainRenderTrace,
		trace: &OptimizerTrace,
	) -> Option<Option<GraphOrderRecommendation>> {
		match self {
			GraphShape::Galaxy => analyze_galaxy(&render.join_order_edges, &trace.join_decisions)
				.map(|galaxy| galaxy_order_recommendation(&galaxy)),
			// ADR 205: Bridge actuation is a SINGLE junction only — a multi-junction chain is Chain's, not a
			// partial re-root around the first junction (`bridge_order_recommendation` reads `links.first()`).
			GraphShape::Bridge => analyze_bridge(&render.join_order_edges, &trace.join_decisions)
				.filter(|bridge| bridge.links.len() == 1)
				.map(|bridge| bridge_order_recommendation(&bridge)),
			GraphShape::Chain => {
				analyze_bridge_chain(&render.join_order_edges, &trace.join_decisions)
					.map(|chain| bridge_chain_order_recommendation(&chain))
			}
		}
	}

	// ADR 205: re-root the recognized graph. Galaxy/Bridge rotate around the pivot; a chain has no pivot, so
	// it is a path reversal (drive from `D`).
	fn reroot(self, plan: &LogicalPlan, rec: &GraphOrderRecommendation) -> Option<LogicalPlan> {
		match self {
			GraphShape::Galaxy | GraphShape::Bridge => {
				reroot_galaxy_join_tree(plan, &rec.pivot, &rec.preferred_root)
			}
			GraphShape::Chain => reverse_bridge_chain_join_tree(plan, &rec.preferred_root),
		}
	}

	fn set_applied_root(self, render: &mut OptimizerExplainRenderTrace, root: String) {
		match self {
			GraphShape::Galaxy => render.set_applied_galaxy_root(root),
			GraphShape::Bridge => render.set_applied_bridge_root(root),
			GraphShape::Chain => render.set_applied_bridge_chain_root(root),
		}
	}
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
	// ADR 198/199, unified in ADR 200: bounded graph root-swap actuation for galaxies and bridges.
	// Scratch-recognize the graph of `shape`, and if its cost-optimal root differs from the FROM root by
	// the soft margin, re-root the join tree and bind the re-rooted plan — a snowflake the existing
	// actuator + fragmenter already stage (proven by the distributed digest fixtures). Otherwise
	// observe-only (exactly ADR 197). The scratch recognition binds via the actuation-free `bind_join` on
	// a no-op-recorder binder, so it can neither recurse into actuation nor pollute the shared decision
	// log. Recursion terminates naturally: after re-rooting, the new root owns the smaller fact-edge, so
	// the re-rooted graph's own recommendation is `KeptAlreadyOptimal`.
	pub(super) async fn try_bind_graph_actuation(
		&self,
		plan: &LogicalPlan,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		shape: GraphShape,
	) -> Result<GraphActuationResult, BindingError> {
		let LogicalPlan::Join {
			left,
			right,
			kind,
			on,
			strategy_intent,
		} = plan
		else {
			return Ok(GraphActuationResult::NotCandidate);
		};

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
		let scratch_render = scratch_render.expect("graph scratch render trace present");
		let Some(maybe_recommendation) = shape.recommend(&scratch_render, &scratch_trace) else {
			// Not a graph of this shape — no decision to record.
			return Ok(GraphActuationResult::NotCandidate);
		};
		let Some(recommendation) = maybe_recommendation else {
			// Recognized, but the order could not be costed (a missing edge estimate).
			return Ok(GraphActuationResult::Declined(
				OptimizerDecisionDeclineReason::MissingEstimate,
			));
		};
		// FROM root already cost-optimal → observe-only (no re-root).
		if recommendation.action != StarOrderAction::Reordered {
			return Ok(GraphActuationResult::KeptAlreadyOptimal);
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
			return Ok(GraphActuationResult::Declined(
				OptimizerDecisionDeclineReason::MissingEstimate,
			));
		};
		let margin = self
			.graph_join_order_soft_margin_steps
			.step_for(shape.decision_kind())
			.margin();
		if preferred_cost.saturating_mul(margin.denominator)
			> declared_cost.saturating_mul(margin.numerator)
		{
			return Ok(GraphActuationResult::KeptThinMargin);
		}

		// Re-root the join tree and bind the re-rooted plan (a snowflake). The re-bind runs on a
		// no-op-recorder binder: the committed graph decision is accrued once by the caller, so the
		// re-rooted plan's own re-recognition (a phantom `KeptAlreadyOptimal`) is discarded. Plan and
		// render are unaffected — only the decision sink differs.
		let Some(rerooted) = shape.reroot(plan, &recommendation) else {
			return Ok(GraphActuationResult::Declined(
				OptimizerDecisionDeclineReason::MissingEstimate,
			));
		};
		let preferred_root = recommendation.preferred_root.clone();
		// ADR 205: the reversed chain is ASYMMETRIC — interior junctions fail the inverse-F1 gate in reverse
		// (a bridge becomes dimension-like), so the reversed plan does NOT re-recognize as a chain (unlike
		// the symmetric galaxy/bridge re-root). Re-derive the recognized chain from the scratch edges (the
		// scratch bind populates edges but not the observe-hook field) so the receipt can render the actuated
		// path even though re-recognition yields nothing.
		let scratch_chain = analyze_bridge_chain(
			&scratch_render.join_order_edges,
			&scratch_trace.join_decisions,
		);
		let bound = self
			.without_decision_recording()
			.bind_plan_with_trace(&rerooted, trace, render_trace, JoinOrderSourceCapture::On)
			.await?;
		// The receipt reflects the applied re-root, not the re-rooted graph's intrinsic recommendation.
		if let Some(render_trace) = render_trace.as_mut() {
			shape.set_applied_root(render_trace, preferred_root);
			// ADR 205: restore the recognized chain for the receipt (the reversed plan did not re-recognize).
			// The actuated-order *verdict* is deferred — judging the reversed execution needs reversed-plan
			// edge targets that only re-recognition supplies; the 204 observe verdict still judges non-
			// actuated chains.
			if shape == GraphShape::Chain && render_trace.bridge_chain_order.is_none() {
				if let Some(chain) = scratch_chain {
					render_trace.set_observed_bridge_chain(chain);
				}
			}
		}
		Ok(GraphActuationResult::Actuated(bound))
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
			self.tree_join_order_soft_margin_step.margin(),
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
			self.tree_join_order_soft_margin_step.margin(),
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

	// ADR 200: record the committed graph-actuation decision (one per recognized galaxy/bridge) into the
	// shared decision log, under the shape's `GalaxyRoot`/`BridgeRoot` kind. The caller maps the
	// `GraphActuationResult` to the outcome so the decline reason is preserved. This runs on `self`; a
	// no-op-recorder binder discards it (the re-rooted re-bind, whose decision the outer call committed).
	pub(super) fn accrue_graph_order_decision(
		&self,
		shape: GraphShape,
		outcome: OptimizerDecisionOutcome,
	) {
		self.decision_recorder
			.record(shape.decision_kind(), outcome);
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
