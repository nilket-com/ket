use super::join::ProbeLinearityFence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerDecisionKind {
	StarCandidate,
	FlatStar,
	SnowflakeRoot,
	SnowflakeBranch,
	// ADR 200: graph (non-tree) actuation provenance — a galaxy / bridge root-swap decision.
	GalaxyRoot,
	BridgeRoot,
	// ADR 203: observe-only multi-hop bridge chain path-order verdict provenance (no actuation).
	BridgeChainRoot,
	// ADR 214: observe-only preserved-outer ordering opportunity evidence. No actuation.
	PreservedOuterOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerDecisionDeclineReason {
	MixedStrategyStar,
	NonInnerEquiJoin,
	MissingRelationLabel,
	MultipleProbeRelations,
	DuplicateDimension,
	MissingEstimate,
	ProbeNotScanLinear,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerDecisionOutcome {
	Reordered,
	KeptOptimal,
	KeptThinMargin,
	Declined(OptimizerDecisionDeclineReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerRuntimeVerdictClass {
	Vindicated,
	MissedWin,
	OverEager,
	MisRanked,
	Indeterminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerRuntimeVerdictProvenance {
	Native,
	Demoted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerRuntimeEvidenceLane {
	Total,
	Demoted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct OptimizerRuntimeVerdictKey {
	pub(crate) kind: OptimizerDecisionKind,
	pub(crate) verdict: OptimizerRuntimeVerdictClass,
	pub(crate) lane: OptimizerRuntimeEvidenceLane,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct OptimizerRuntimeDecisionKey {
	pub(crate) kind: OptimizerDecisionKind,
	pub(crate) outcome: OptimizerDecisionOutcome,
	pub(crate) lane: OptimizerRuntimeEvidenceLane,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerProbeLinearityConsumer {
	HashFallback,
	StarDecline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct OptimizerProbeLinearityFenceKey {
	pub(crate) consumer: OptimizerProbeLinearityConsumer,
	pub(crate) fence: ProbeLinearityFence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerDecisionRow {
	pub(crate) kind: OptimizerDecisionKind,
	pub(crate) outcome: OptimizerDecisionOutcome,
	pub(crate) provenance: OptimizerRuntimeVerdictProvenance,
	pub(crate) count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerRuntimeVerdictRow {
	pub(crate) kind: OptimizerDecisionKind,
	pub(crate) verdict: OptimizerRuntimeVerdictClass,
	pub(crate) provenance: OptimizerRuntimeVerdictProvenance,
	pub(crate) count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OptimizerProbeLinearityFenceStatus {
	Absent,
	InsufficientEvidence,
	Observed,
	Dominant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerProbeLinearityFenceRow {
	pub(crate) consumer: OptimizerProbeLinearityConsumer,
	pub(crate) fence: ProbeLinearityFence,
	pub(crate) recent_count: u64,
	pub(crate) lifetime_count: u64,
	pub(crate) consumer_recent_total: u64,
	pub(crate) consumer_lifetime_total: u64,
	pub(crate) recent_share_per_mille: u64,
	pub(crate) status: OptimizerProbeLinearityFenceStatus,
	pub(crate) reason: String,
	pub(crate) next_cut: &'static str,
}

#[derive(Debug, Default)]
pub struct OptimizerDecisionLog;

impl OptimizerDecisionLog {
	pub(crate) fn record(&self, _kind: OptimizerDecisionKind, _outcome: OptimizerDecisionOutcome) {}
}

// A sink for optimizer decisions. Committed binds carry a runtime-evidence recorder; the
// `OptimizerDecisionLog` ZST is retained as a no-op compatibility shell for old config construction.
// A bind that must not pollute the shared log — a scratch recognition bind, or a declined-actuation
// re-bind — is handed the `NoOpDecisionRecorder`, so "do not record" is a fact about the type the
// binder carries, not a runtime flag a caller can forget to set.
pub(crate) trait OptimizerDecisionRecorder {
	fn record(&self, kind: OptimizerDecisionKind, outcome: OptimizerDecisionOutcome);

	fn record_probe_linearity_fence(
		&self,
		_consumer: OptimizerProbeLinearityConsumer,
		_fence: ProbeLinearityFence,
	) {
	}
}

impl OptimizerDecisionRecorder for OptimizerDecisionLog {
	fn record(&self, kind: OptimizerDecisionKind, outcome: OptimizerDecisionOutcome) {
		OptimizerDecisionLog::record(self, kind, outcome);
	}
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct NoOpDecisionRecorder;

impl OptimizerDecisionRecorder for NoOpDecisionRecorder {
	fn record(&self, _kind: OptimizerDecisionKind, _outcome: OptimizerDecisionOutcome) {}
}

pub(crate) fn optimizer_decision_kind_label(kind: OptimizerDecisionKind) -> &'static str {
	match kind {
		OptimizerDecisionKind::StarCandidate => "star_candidate",
		OptimizerDecisionKind::FlatStar => "flat_star",
		OptimizerDecisionKind::SnowflakeRoot => "snowflake_root",
		OptimizerDecisionKind::SnowflakeBranch => "snowflake_branch",
		OptimizerDecisionKind::GalaxyRoot => "galaxy_root",
		OptimizerDecisionKind::BridgeRoot => "bridge_root",
		OptimizerDecisionKind::BridgeChainRoot => "bridge_chain_root",
		OptimizerDecisionKind::PreservedOuterOrder => "preserved_outer_order",
	}
}

pub(crate) fn optimizer_probe_linearity_consumer_label(
	consumer: OptimizerProbeLinearityConsumer,
) -> &'static str {
	match consumer {
		OptimizerProbeLinearityConsumer::HashFallback => "hash_fallback",
		OptimizerProbeLinearityConsumer::StarDecline => "star_decline",
	}
}

pub(crate) fn optimizer_probe_linearity_consumers() -> [OptimizerProbeLinearityConsumer; 2] {
	[
		OptimizerProbeLinearityConsumer::HashFallback,
		OptimizerProbeLinearityConsumer::StarDecline,
	]
}

pub(crate) fn optimizer_probe_linearity_fence_status_label(
	status: OptimizerProbeLinearityFenceStatus,
) -> &'static str {
	match status {
		OptimizerProbeLinearityFenceStatus::Absent => "absent",
		OptimizerProbeLinearityFenceStatus::InsufficientEvidence => "insufficient_evidence",
		OptimizerProbeLinearityFenceStatus::Observed => "observed",
		OptimizerProbeLinearityFenceStatus::Dominant => "dominant",
	}
}

pub(crate) fn optimizer_decision_outcome_label(outcome: OptimizerDecisionOutcome) -> &'static str {
	match outcome {
		OptimizerDecisionOutcome::Reordered => "reordered",
		OptimizerDecisionOutcome::KeptOptimal => "kept_optimal",
		OptimizerDecisionOutcome::KeptThinMargin => "kept_thin_margin",
		OptimizerDecisionOutcome::Declined(_) => "declined",
	}
}

pub(crate) fn optimizer_decision_decline_reason_label(
	reason: OptimizerDecisionDeclineReason,
) -> &'static str {
	match reason {
		OptimizerDecisionDeclineReason::MixedStrategyStar => "mixed_strategy_star",
		OptimizerDecisionDeclineReason::NonInnerEquiJoin => "non_inner_equi_join",
		OptimizerDecisionDeclineReason::MissingRelationLabel => "missing_relation_label",
		OptimizerDecisionDeclineReason::MultipleProbeRelations => "multiple_probe_relations",
		OptimizerDecisionDeclineReason::DuplicateDimension => "duplicate_dimension",
		OptimizerDecisionDeclineReason::MissingEstimate => "missing_estimate",
		OptimizerDecisionDeclineReason::ProbeNotScanLinear => "probe_not_scan_linear",
	}
}

pub(crate) fn decision_recording_kinds() -> [OptimizerDecisionKind; 7] {
	[
		OptimizerDecisionKind::StarCandidate,
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerDecisionKind::SnowflakeBranch,
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionKind::BridgeChainRoot,
	]
}

pub(crate) fn optimizer_decision_outcomes() -> [OptimizerDecisionOutcome; 10] {
	[
		OptimizerDecisionOutcome::Reordered,
		OptimizerDecisionOutcome::KeptOptimal,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::MixedStrategyStar),
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::NonInnerEquiJoin),
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::MissingRelationLabel),
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::MultipleProbeRelations),
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::DuplicateDimension),
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::MissingEstimate),
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::ProbeNotScanLinear),
	]
}

pub(crate) fn optimizer_runtime_verdict_label(
	verdict: OptimizerRuntimeVerdictClass,
) -> &'static str {
	match verdict {
		OptimizerRuntimeVerdictClass::Vindicated => "vindicated",
		OptimizerRuntimeVerdictClass::MissedWin => "missed_win",
		OptimizerRuntimeVerdictClass::OverEager => "over_eager",
		OptimizerRuntimeVerdictClass::MisRanked => "mis_ranked",
		OptimizerRuntimeVerdictClass::Indeterminate => "indeterminate",
	}
}

pub(crate) fn optimizer_runtime_verdict_provenance_label(
	provenance: OptimizerRuntimeVerdictProvenance,
) -> &'static str {
	match provenance {
		OptimizerRuntimeVerdictProvenance::Native => "native",
		OptimizerRuntimeVerdictProvenance::Demoted => "demoted",
	}
}
