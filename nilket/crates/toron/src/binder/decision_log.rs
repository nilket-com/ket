use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const DEFAULT_OPTIMIZER_DECISION_LOG_MAX_KEYS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerDecisionKind {
	StarCandidate,
	FlatStar,
	SnowflakeRoot,
	SnowflakeBranch,
	Overflow,
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
	Overflow,
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
pub(crate) struct OptimizerDecisionKey {
	pub(crate) kind: OptimizerDecisionKind,
	pub(crate) outcome: OptimizerDecisionOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct OptimizerRuntimeVerdictKey {
	pub(crate) kind: OptimizerDecisionKind,
	pub(crate) verdict: OptimizerRuntimeVerdictClass,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerDecisionRow {
	pub(crate) kind: OptimizerDecisionKind,
	pub(crate) outcome: OptimizerDecisionOutcome,
	pub(crate) count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerRuntimeVerdictRow {
	pub(crate) kind: OptimizerDecisionKind,
	pub(crate) verdict: OptimizerRuntimeVerdictClass,
	pub(crate) count: u64,
}

#[derive(Debug)]
pub struct OptimizerDecisionLog {
	counts: Mutex<BTreeMap<OptimizerDecisionKey, u64>>,
	verdict_counts: Mutex<BTreeMap<OptimizerRuntimeVerdictKey, u64>>,
	overflow_count: AtomicU64,
	max_keys: usize,
}

impl Default for OptimizerDecisionLog {
	fn default() -> Self {
		Self::with_max_keys(DEFAULT_OPTIMIZER_DECISION_LOG_MAX_KEYS)
	}
}

impl OptimizerDecisionLog {
	pub(crate) fn with_max_keys(max_keys: usize) -> Self {
		Self {
			counts: Mutex::new(BTreeMap::new()),
			verdict_counts: Mutex::new(BTreeMap::new()),
			overflow_count: AtomicU64::new(0),
			max_keys,
		}
	}

	pub(crate) fn record(&self, kind: OptimizerDecisionKind, outcome: OptimizerDecisionOutcome) {
		let key = OptimizerDecisionKey { kind, outcome };
		let mut counts = self.counts.lock().expect("optimizer decision log");
		if let Some(count) = counts.get_mut(&key) {
			*count = count.saturating_add(1);
			return;
		}
		if counts.len() >= self.max_keys {
			self.overflow_count.fetch_add(1, Ordering::Relaxed);
			return;
		}
		counts.insert(key, 1);
	}

	pub(crate) fn record_verdict(
		&self,
		kind: OptimizerDecisionKind,
		verdict: OptimizerRuntimeVerdictClass,
	) {
		let key = OptimizerRuntimeVerdictKey { kind, verdict };
		let mut counts = self.verdict_counts.lock().expect("optimizer verdict log");
		if let Some(count) = counts.get_mut(&key) {
			*count = count.saturating_add(1);
			return;
		}
		counts.insert(key, 1);
	}

	pub(crate) fn snapshot(&self) -> Vec<OptimizerDecisionRow> {
		let mut rows = {
			let counts = self.counts.lock().expect("optimizer decision log");
			counts
				.iter()
				.map(|(key, count)| OptimizerDecisionRow {
					kind: key.kind,
					outcome: key.outcome,
					count: *count,
				})
				.collect::<Vec<_>>()
		};
		let overflow_count = self.overflow_count.load(Ordering::Relaxed);
		if overflow_count > 0 {
			rows.push(OptimizerDecisionRow {
				kind: OptimizerDecisionKind::Overflow,
				outcome: OptimizerDecisionOutcome::Overflow,
				count: overflow_count,
			});
		}
		rows
	}

	pub(crate) fn verdict_snapshot(&self) -> Vec<OptimizerRuntimeVerdictRow> {
		let counts = self.verdict_counts.lock().expect("optimizer verdict log");
		counts
			.iter()
			.map(|(key, count)| OptimizerRuntimeVerdictRow {
				kind: key.kind,
				verdict: key.verdict,
				count: *count,
			})
			.collect()
	}
}

pub(crate) fn optimizer_decision_kind_label(kind: OptimizerDecisionKind) -> &'static str {
	match kind {
		OptimizerDecisionKind::StarCandidate => "star_candidate",
		OptimizerDecisionKind::FlatStar => "flat_star",
		OptimizerDecisionKind::SnowflakeRoot => "snowflake_root",
		OptimizerDecisionKind::SnowflakeBranch => "snowflake_branch",
		OptimizerDecisionKind::Overflow => "overflow",
	}
}

pub(crate) fn optimizer_decision_outcome_label(outcome: OptimizerDecisionOutcome) -> &'static str {
	match outcome {
		OptimizerDecisionOutcome::Reordered => "reordered",
		OptimizerDecisionOutcome::KeptOptimal => "kept_optimal",
		OptimizerDecisionOutcome::KeptThinMargin => "kept_thin_margin",
		OptimizerDecisionOutcome::Declined(_) => "declined",
		OptimizerDecisionOutcome::Overflow => "overflow",
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
