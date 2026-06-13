use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use wendelon::schema::{TenantId, WriteStatus};
use wendelon_client::{Client, ClientError, WriteOptions};

use super::decision_log::{
	OptimizerDecisionKind, OptimizerDecisionOutcome, OptimizerDecisionRecorder,
	OptimizerDecisionRow, OptimizerRuntimeDecisionKey, OptimizerRuntimeEvidenceLane,
	OptimizerRuntimeVerdictClass, OptimizerRuntimeVerdictKey, OptimizerRuntimeVerdictProvenance,
	OptimizerRuntimeVerdictRow, decision_recording_kinds, optimizer_decision_decline_reason_label,
	optimizer_decision_kind_label, optimizer_decision_outcome_label, optimizer_decision_outcomes,
	optimizer_runtime_verdict_label,
};

pub const OPTIMIZER_EVIDENCE_KEY_PREFIX: &str = "optimizer_evidence/verdict/";
pub const OPTIMIZER_DECISION_EVIDENCE_KEY_PREFIX: &str = "optimizer_evidence/decision/";
const UNAVAILABLE_CIRCUIT_BREAKER_FAILURES: u64 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerEvidenceMode {
	LocalOnly,
	Durable,
	Degraded,
}

impl OptimizerEvidenceMode {
	pub(crate) fn as_str(self) -> &'static str {
		match self {
			Self::LocalOnly => "local_only",
			Self::Durable => "durable",
			Self::Degraded => "degraded",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerEvidenceState {
	Ok,
	Stale,
	ReplicationUnsupported,
	Unavailable,
}

impl OptimizerEvidenceState {
	pub(crate) fn as_str(self) -> &'static str {
		match self {
			Self::Ok => "ok",
			Self::Stale => "stale",
			Self::ReplicationUnsupported => "replication_unsupported",
			Self::Unavailable => "unavailable",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerEvidenceStatusSnapshot {
	pub mode: OptimizerEvidenceMode,
	pub state: OptimizerEvidenceState,
	pub pending_deltas: u64,
	pub dropped_deltas: u64,
	pub ambiguous_deltas: u64,
	pub refresh_failures: u64,
	pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct DurableEvidenceBinding {
	client: Arc<Client>,
	tenant_id: TenantId,
	flush_timeout: Duration,
}

impl DurableEvidenceBinding {
	pub fn new(client: Arc<Client>, tenant_id: TenantId, flush_timeout: Duration) -> Self {
		Self {
			client,
			tenant_id,
			flush_timeout,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerEvidenceRefreshReport {
	pub refreshed_keys: usize,
	pub failed: bool,
}

pub struct OptimizerRuntimeEvidence {
	state: Mutex<RuntimeEvidenceState>,
	binding: Option<DurableEvidenceBinding>,
}

impl fmt::Debug for OptimizerRuntimeEvidence {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("OptimizerRuntimeEvidence")
			.field("has_binding", &self.binding.is_some())
			.field("status", &self.status_snapshot())
			.finish()
	}
}

#[derive(Debug)]
struct RuntimeEvidenceState {
	pending: BTreeMap<OptimizerRuntimeVerdictKey, u64>,
	in_flight: BTreeMap<OptimizerRuntimeVerdictKey, u64>,
	durable_cache: BTreeMap<OptimizerRuntimeVerdictKey, u64>,
	decision_pending: BTreeMap<OptimizerRuntimeDecisionKey, u64>,
	decision_in_flight: BTreeMap<OptimizerRuntimeDecisionKey, u64>,
	decision_durable_cache: BTreeMap<OptimizerRuntimeDecisionKey, u64>,
	mode: OptimizerEvidenceMode,
	state: OptimizerEvidenceState,
	dropped_deltas: u64,
	ambiguous_deltas: u64,
	refresh_failures: u64,
	consecutive_unavailable_failures: u64,
	circuit_open: bool,
	replication_unsupported: bool,
	last_error: Option<String>,
}

impl Default for RuntimeEvidenceState {
	fn default() -> Self {
		Self {
			pending: BTreeMap::new(),
			in_flight: BTreeMap::new(),
			durable_cache: BTreeMap::new(),
			decision_pending: BTreeMap::new(),
			decision_in_flight: BTreeMap::new(),
			decision_durable_cache: BTreeMap::new(),
			mode: OptimizerEvidenceMode::LocalOnly,
			state: OptimizerEvidenceState::Ok,
			dropped_deltas: 0,
			ambiguous_deltas: 0,
			refresh_failures: 0,
			consecutive_unavailable_failures: 0,
			circuit_open: false,
			replication_unsupported: false,
			last_error: None,
		}
	}
}

impl OptimizerRuntimeEvidence {
	pub fn local_only() -> Self {
		Self {
			state: Mutex::new(RuntimeEvidenceState::default()),
			binding: None,
		}
	}

	pub fn with_wendelon(binding: DurableEvidenceBinding) -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(RuntimeEvidenceState {
				mode: OptimizerEvidenceMode::Durable,
				..RuntimeEvidenceState::default()
			}),
			binding: Some(binding),
		})
	}

	pub(crate) fn record_verdict(
		&self,
		kind: OptimizerDecisionKind,
		verdict: OptimizerRuntimeVerdictClass,
		provenance: OptimizerRuntimeVerdictProvenance,
	) -> Vec<OptimizerRuntimeVerdictKey> {
		let mut keys = Vec::with_capacity(2);
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		let total = OptimizerRuntimeVerdictKey {
			kind,
			verdict,
			lane: OptimizerRuntimeEvidenceLane::Total,
		};
		increment_count(&mut state.pending, total);
		keys.push(total);
		if provenance == OptimizerRuntimeVerdictProvenance::Demoted {
			let demoted = OptimizerRuntimeVerdictKey {
				kind,
				verdict,
				lane: OptimizerRuntimeEvidenceLane::Demoted,
			};
			increment_count(&mut state.pending, demoted);
			keys.push(demoted);
		}
		keys
	}

	pub(crate) fn record_decision(
		&self,
		kind: OptimizerDecisionKind,
		outcome: OptimizerDecisionOutcome,
		provenance: OptimizerRuntimeVerdictProvenance,
	) -> Vec<OptimizerRuntimeDecisionKey> {
		if !decision_recording_kinds().contains(&kind) {
			return Vec::new();
		}
		let mut keys = Vec::with_capacity(2);
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		let total = OptimizerRuntimeDecisionKey {
			kind,
			outcome,
			lane: OptimizerRuntimeEvidenceLane::Total,
		};
		increment_count(&mut state.decision_pending, total);
		keys.push(total);
		if provenance == OptimizerRuntimeVerdictProvenance::Demoted {
			let demoted = OptimizerRuntimeDecisionKey {
				kind,
				outcome,
				lane: OptimizerRuntimeEvidenceLane::Demoted,
			};
			increment_count(&mut state.decision_pending, demoted);
			keys.push(demoted);
		}
		keys
	}

	pub(crate) fn verdict_snapshot(&self) -> Vec<OptimizerRuntimeVerdictRow> {
		let state = self.state.lock().expect("optimizer runtime evidence");
		let mut counts = state.durable_cache.clone();
		for (key, count) in state.pending.iter().chain(state.in_flight.iter()) {
			let entry = counts.entry(*key).or_insert(0);
			*entry = entry.saturating_add(*count);
		}
		let mut rows = Vec::new();
		for kind in runtime_evidence_decision_kinds() {
			for verdict in verdict_classes() {
				let total = counts
					.get(&OptimizerRuntimeVerdictKey {
						kind,
						verdict,
						lane: OptimizerRuntimeEvidenceLane::Total,
					})
					.copied()
					.unwrap_or(0);
				let demoted = counts
					.get(&OptimizerRuntimeVerdictKey {
						kind,
						verdict,
						lane: OptimizerRuntimeEvidenceLane::Demoted,
					})
					.copied()
					.unwrap_or(0);
				let native = total.saturating_sub(demoted);
				if native > 0 {
					rows.push(OptimizerRuntimeVerdictRow {
						kind,
						verdict,
						provenance: OptimizerRuntimeVerdictProvenance::Native,
						count: native,
					});
				}
				if demoted > 0 {
					rows.push(OptimizerRuntimeVerdictRow {
						kind,
						verdict,
						provenance: OptimizerRuntimeVerdictProvenance::Demoted,
						count: demoted,
					});
				}
			}
		}
		rows
	}

	pub(crate) fn decision_snapshot(&self) -> Vec<OptimizerDecisionRow> {
		let state = self.state.lock().expect("optimizer runtime evidence");
		let mut counts = state.decision_durable_cache.clone();
		for (key, count) in state
			.decision_pending
			.iter()
			.chain(state.decision_in_flight.iter())
		{
			let entry = counts.entry(*key).or_insert(0);
			*entry = entry.saturating_add(*count);
		}
		let mut rows = Vec::new();
		for kind in decision_recording_kinds() {
			for outcome in optimizer_decision_outcomes() {
				let total = counts
					.get(&OptimizerRuntimeDecisionKey {
						kind,
						outcome,
						lane: OptimizerRuntimeEvidenceLane::Total,
					})
					.copied()
					.unwrap_or(0);
				let demoted = counts
					.get(&OptimizerRuntimeDecisionKey {
						kind,
						outcome,
						lane: OptimizerRuntimeEvidenceLane::Demoted,
					})
					.copied()
					.unwrap_or(0);
				let native = total.saturating_sub(demoted);
				if native > 0 {
					rows.push(OptimizerDecisionRow {
						kind,
						outcome,
						provenance: OptimizerRuntimeVerdictProvenance::Native,
						count: native,
					});
				}
				if demoted > 0 {
					rows.push(OptimizerDecisionRow {
						kind,
						outcome,
						provenance: OptimizerRuntimeVerdictProvenance::Demoted,
						count: demoted,
					});
				}
			}
		}
		rows
	}

	pub(crate) fn status_snapshot(&self) -> OptimizerEvidenceStatusSnapshot {
		let state = self.state.lock().expect("optimizer runtime evidence");
		OptimizerEvidenceStatusSnapshot {
			mode: state.mode,
			state: state.state,
			pending_deltas: sum_counts(&state.pending)
				.saturating_add(sum_counts(&state.in_flight))
				.saturating_add(sum_counts(&state.decision_pending))
				.saturating_add(sum_counts(&state.decision_in_flight)),
			dropped_deltas: state.dropped_deltas,
			ambiguous_deltas: state.ambiguous_deltas,
			refresh_failures: state.refresh_failures,
			last_error: state.last_error.clone(),
		}
	}

	pub async fn flush_decision_deltas(&self) {
		let Some(binding) = self.binding.clone() else {
			return;
		};
		let keys = self.pending_decision_keys();
		for key in keys {
			let Some(delta) = self.take_pending_decision_for_flush(key) else {
				continue;
			};
			let key_bytes = decision_counter_key(key);
			let result = tokio::time::timeout(
				binding.flush_timeout,
				binding.client.counter_fetch_add(
					binding.tenant_id,
					&key_bytes,
					delta,
					WriteOptions::default(),
				),
			)
			.await;
			match result {
				Ok(Ok(receipt)) => self.ack_decision_flush(key, receipt.current_value),
				Ok(Err(ClientError::WriteFailed { status }))
					if status == WriteStatus::COUNTER_REPLICATION_UNSUPPORTED =>
				{
					self.latch_decision_replication_unsupported(
						key,
						delta,
						format!("counter fetch-add failed with status {}", status.as_str()),
					);
				}
				Ok(Err(error)) => {
					self.drop_ambiguous_decision_in_flight(key, delta, error.to_string())
				}
				Err(_) => self.drop_ambiguous_decision_in_flight(
					key,
					delta,
					"counter fetch-add timed out".to_string(),
				),
			}
		}
	}

	pub(crate) async fn flush_keys(
		&self,
		keys: impl IntoIterator<Item = OptimizerRuntimeVerdictKey>,
	) {
		let Some(binding) = self.binding.clone() else {
			return;
		};
		let keys = keys.into_iter().collect::<BTreeSet<_>>();
		for key in keys {
			let Some(delta) = self.take_pending_for_flush(key) else {
				continue;
			};
			let key_bytes = verdict_counter_key(key);
			let result = tokio::time::timeout(
				binding.flush_timeout,
				binding.client.counter_fetch_add(
					binding.tenant_id,
					&key_bytes,
					delta,
					WriteOptions::default(),
				),
			)
			.await;
			match result {
				Ok(Ok(receipt)) => self.ack_flush(key, receipt.current_value),
				Ok(Err(ClientError::WriteFailed { status }))
					if status == WriteStatus::COUNTER_REPLICATION_UNSUPPORTED =>
				{
					self.latch_replication_unsupported(
						key,
						delta,
						format!("counter fetch-add failed with status {}", status.as_str()),
					);
				}
				Ok(Err(error)) => self.drop_ambiguous_in_flight(key, delta, error.to_string()),
				Err(_) => self.drop_ambiguous_in_flight(
					key,
					delta,
					"counter fetch-add timed out".to_string(),
				),
			}
		}
	}

	pub async fn refresh_durable_cache(&self) -> OptimizerEvidenceRefreshReport {
		let Some(binding) = self.binding.clone() else {
			return OptimizerEvidenceRefreshReport {
				refreshed_keys: 0,
				failed: false,
			};
		};
		if self.is_replication_unsupported() {
			return OptimizerEvidenceRefreshReport {
				refreshed_keys: 0,
				failed: true,
			};
		}
		let mut refreshed = BTreeMap::new();
		for (key, key_bytes) in all_verdict_counter_keys() {
			match binding
				.client
				.counter_read(binding.tenant_id, &key_bytes)
				.await
			{
				Ok(value) => {
					refreshed.insert(key, value.unwrap_or(0));
				}
				Err(error) => {
					self.mark_refresh_failed(error.to_string());
					return OptimizerEvidenceRefreshReport {
						refreshed_keys: refreshed.len(),
						failed: true,
					};
				}
			}
		}
		let mut refreshed_decisions = BTreeMap::new();
		for (key, key_bytes) in all_decision_counter_keys() {
			match binding
				.client
				.counter_read(binding.tenant_id, &key_bytes)
				.await
			{
				Ok(value) => {
					refreshed_decisions.insert(key, value.unwrap_or(0));
				}
				Err(error) => {
					self.mark_refresh_failed(error.to_string());
					return OptimizerEvidenceRefreshReport {
						refreshed_keys: refreshed.len().saturating_add(refreshed_decisions.len()),
						failed: true,
					};
				}
			}
		}
		let refreshed_keys = refreshed.len().saturating_add(refreshed_decisions.len());
		self.merge_refresh(refreshed, refreshed_decisions);
		OptimizerEvidenceRefreshReport {
			refreshed_keys,
			failed: false,
		}
	}

	fn pending_decision_keys(&self) -> Vec<OptimizerRuntimeDecisionKey> {
		let state = self.state.lock().expect("optimizer runtime evidence");
		state.decision_pending.keys().copied().collect()
	}

	fn take_pending_for_flush(&self, key: OptimizerRuntimeVerdictKey) -> Option<u64> {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		if self.binding.is_none()
			|| state.replication_unsupported
			|| state.circuit_open
			|| state.in_flight.get(&key).copied().unwrap_or(0) > 0
		{
			return None;
		}
		let delta = state.pending.remove(&key).unwrap_or(0);
		if delta == 0 {
			return None;
		}
		state.in_flight.insert(key, delta);
		Some(delta)
	}

	fn take_pending_decision_for_flush(&self, key: OptimizerRuntimeDecisionKey) -> Option<u64> {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		if self.binding.is_none()
			|| state.replication_unsupported
			|| state.circuit_open
			|| state.decision_in_flight.get(&key).copied().unwrap_or(0) > 0
		{
			return None;
		}
		let delta = state.decision_pending.remove(&key).unwrap_or(0);
		if delta == 0 {
			return None;
		}
		state.decision_in_flight.insert(key, delta);
		Some(delta)
	}

	fn ack_flush(&self, key: OptimizerRuntimeVerdictKey, current_value: u64) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.in_flight.remove(&key);
		let cached = state.durable_cache.entry(key).or_insert(0);
		*cached = (*cached).max(current_value);
		if !state.replication_unsupported {
			state.consecutive_unavailable_failures = 0;
			state.circuit_open = false;
			state.mode = OptimizerEvidenceMode::Durable;
			state.state = OptimizerEvidenceState::Ok;
			state.last_error = None;
		}
	}

	fn ack_decision_flush(&self, key: OptimizerRuntimeDecisionKey, current_value: u64) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.decision_in_flight.remove(&key);
		let cached = state.decision_durable_cache.entry(key).or_insert(0);
		*cached = (*cached).max(current_value);
		if !state.replication_unsupported {
			state.consecutive_unavailable_failures = 0;
			state.circuit_open = false;
			state.mode = OptimizerEvidenceMode::Durable;
			state.state = OptimizerEvidenceState::Ok;
			state.last_error = None;
		}
	}

	fn drop_ambiguous_in_flight(&self, key: OptimizerRuntimeVerdictKey, delta: u64, error: String) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.in_flight.remove(&key);
		state.dropped_deltas = state.dropped_deltas.saturating_add(delta);
		state.ambiguous_deltas = state.ambiguous_deltas.saturating_add(delta);
		state.consecutive_unavailable_failures =
			state.consecutive_unavailable_failures.saturating_add(1);
		if state.consecutive_unavailable_failures >= UNAVAILABLE_CIRCUIT_BREAKER_FAILURES {
			state.circuit_open = true;
			state.mode = OptimizerEvidenceMode::Degraded;
		}
		state.state = OptimizerEvidenceState::Unavailable;
		state.last_error = Some(error);
	}

	fn drop_ambiguous_decision_in_flight(
		&self,
		key: OptimizerRuntimeDecisionKey,
		delta: u64,
		error: String,
	) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.decision_in_flight.remove(&key);
		state.dropped_deltas = state.dropped_deltas.saturating_add(delta);
		state.ambiguous_deltas = state.ambiguous_deltas.saturating_add(delta);
		state.consecutive_unavailable_failures =
			state.consecutive_unavailable_failures.saturating_add(1);
		if state.consecutive_unavailable_failures >= UNAVAILABLE_CIRCUIT_BREAKER_FAILURES {
			state.circuit_open = true;
			state.mode = OptimizerEvidenceMode::Degraded;
		}
		state.state = OptimizerEvidenceState::Unavailable;
		state.last_error = Some(error);
	}

	fn latch_replication_unsupported(
		&self,
		key: OptimizerRuntimeVerdictKey,
		delta: u64,
		error: String,
	) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.in_flight.remove(&key);
		state.dropped_deltas = state.dropped_deltas.saturating_add(delta);
		state.ambiguous_deltas = state.ambiguous_deltas.saturating_add(delta);
		state.replication_unsupported = true;
		state.circuit_open = true;
		state.mode = OptimizerEvidenceMode::Degraded;
		state.state = OptimizerEvidenceState::ReplicationUnsupported;
		state.last_error = Some(error);
	}

	fn latch_decision_replication_unsupported(
		&self,
		key: OptimizerRuntimeDecisionKey,
		delta: u64,
		error: String,
	) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.decision_in_flight.remove(&key);
		state.dropped_deltas = state.dropped_deltas.saturating_add(delta);
		state.ambiguous_deltas = state.ambiguous_deltas.saturating_add(delta);
		state.replication_unsupported = true;
		state.circuit_open = true;
		state.mode = OptimizerEvidenceMode::Degraded;
		state.state = OptimizerEvidenceState::ReplicationUnsupported;
		state.last_error = Some(error);
	}

	fn is_replication_unsupported(&self) -> bool {
		let state = self.state.lock().expect("optimizer runtime evidence");
		state.replication_unsupported
	}

	fn mark_refresh_failed(&self, error: String) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.refresh_failures = state.refresh_failures.saturating_add(1);
		if !state.replication_unsupported {
			state.state = OptimizerEvidenceState::Stale;
		}
		state.last_error = Some(error);
	}

	fn merge_refresh(
		&self,
		refreshed: BTreeMap<OptimizerRuntimeVerdictKey, u64>,
		refreshed_decisions: BTreeMap<OptimizerRuntimeDecisionKey, u64>,
	) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		for (key, value) in refreshed {
			let cached = state.durable_cache.entry(key).or_insert(0);
			*cached = (*cached).max(value);
		}
		for (key, value) in refreshed_decisions {
			let cached = state.decision_durable_cache.entry(key).or_insert(0);
			*cached = (*cached).max(value);
		}
		if !state.replication_unsupported {
			state.consecutive_unavailable_failures = 0;
			state.circuit_open = false;
			state.mode = OptimizerEvidenceMode::Durable;
			state.state = OptimizerEvidenceState::Ok;
			state.last_error = None;
		}
	}
}

#[derive(Debug, Clone)]
pub(crate) struct OptimizerRuntimeDecisionRecorder {
	evidence: Arc<OptimizerRuntimeEvidence>,
	provenance: OptimizerRuntimeVerdictProvenance,
}

impl OptimizerRuntimeDecisionRecorder {
	pub(crate) fn new(
		evidence: Arc<OptimizerRuntimeEvidence>,
		provenance: OptimizerRuntimeVerdictProvenance,
	) -> Self {
		Self {
			evidence,
			provenance,
		}
	}
}

impl OptimizerDecisionRecorder for OptimizerRuntimeDecisionRecorder {
	fn record(&self, kind: OptimizerDecisionKind, outcome: OptimizerDecisionOutcome) {
		self.evidence
			.record_decision(kind, outcome, self.provenance);
	}
}

pub(crate) fn verdict_counter_key(key: OptimizerRuntimeVerdictKey) -> Vec<u8> {
	let suffix = match key.lane {
		OptimizerRuntimeEvidenceLane::Total => format!(
			"{}/{}",
			optimizer_decision_kind_label(key.kind),
			optimizer_runtime_verdict_label(key.verdict)
		),
		OptimizerRuntimeEvidenceLane::Demoted => format!(
			"{}/demoted/{}",
			optimizer_decision_kind_label(key.kind),
			optimizer_runtime_verdict_label(key.verdict)
		),
	};
	format!("{OPTIMIZER_EVIDENCE_KEY_PREFIX}{suffix}").into_bytes()
}

pub(crate) fn decision_counter_key(key: OptimizerRuntimeDecisionKey) -> Vec<u8> {
	let suffix = match key.lane {
		OptimizerRuntimeEvidenceLane::Total => format!(
			"{}/{}",
			optimizer_decision_kind_label(key.kind),
			decision_outcome_key_label(key.outcome)
		),
		OptimizerRuntimeEvidenceLane::Demoted => format!(
			"{}/demoted/{}",
			optimizer_decision_kind_label(key.kind),
			decision_outcome_key_label(key.outcome)
		),
	};
	format!("{OPTIMIZER_DECISION_EVIDENCE_KEY_PREFIX}{suffix}").into_bytes()
}

pub(crate) fn all_verdict_counter_keys() -> Vec<(OptimizerRuntimeVerdictKey, Vec<u8>)> {
	let mut keys = Vec::with_capacity(
		runtime_evidence_decision_kinds().len()
			* verdict_classes().len()
			* runtime_evidence_lanes().len(),
	);
	for kind in runtime_evidence_decision_kinds() {
		for verdict in verdict_classes() {
			for lane in runtime_evidence_lanes() {
				let key = OptimizerRuntimeVerdictKey {
					kind,
					verdict,
					lane,
				};
				keys.push((key, verdict_counter_key(key)));
			}
		}
	}
	keys
}

pub(crate) fn all_decision_counter_keys() -> Vec<(OptimizerRuntimeDecisionKey, Vec<u8>)> {
	let mut keys = Vec::with_capacity(
		decision_recording_kinds().len()
			* optimizer_decision_outcomes().len()
			* runtime_evidence_lanes().len(),
	);
	for kind in decision_recording_kinds() {
		for outcome in optimizer_decision_outcomes() {
			for lane in runtime_evidence_lanes() {
				let key = OptimizerRuntimeDecisionKey {
					kind,
					outcome,
					lane,
				};
				keys.push((key, decision_counter_key(key)));
			}
		}
	}
	keys
}

fn decision_outcome_key_label(outcome: OptimizerDecisionOutcome) -> String {
	match outcome {
		OptimizerDecisionOutcome::Declined(reason) => {
			format!(
				"declined/{}",
				optimizer_decision_decline_reason_label(reason)
			)
		}
		OptimizerDecisionOutcome::Reordered
		| OptimizerDecisionOutcome::KeptOptimal
		| OptimizerDecisionOutcome::KeptThinMargin => {
			optimizer_decision_outcome_label(outcome).to_string()
		}
	}
}

fn runtime_evidence_decision_kinds() -> [OptimizerDecisionKind; 8] {
	[
		OptimizerDecisionKind::StarCandidate,
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerDecisionKind::SnowflakeBranch,
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionKind::BridgeChainRoot,
		OptimizerDecisionKind::PreservedOuterOrder,
	]
}

fn runtime_evidence_lanes() -> [OptimizerRuntimeEvidenceLane; 2] {
	[
		OptimizerRuntimeEvidenceLane::Total,
		OptimizerRuntimeEvidenceLane::Demoted,
	]
}

fn verdict_classes() -> [OptimizerRuntimeVerdictClass; 5] {
	[
		OptimizerRuntimeVerdictClass::Vindicated,
		OptimizerRuntimeVerdictClass::MissedWin,
		OptimizerRuntimeVerdictClass::OverEager,
		OptimizerRuntimeVerdictClass::MisRanked,
		OptimizerRuntimeVerdictClass::Indeterminate,
	]
}

fn sum_counts<K>(counts: &BTreeMap<K, u64>) -> u64 {
	counts
		.values()
		.fold(0u64, |acc, count| acc.saturating_add(*count))
}

fn increment_count<K: Ord>(counts: &mut BTreeMap<K, u64>, key: K) {
	let count = counts.entry(key).or_insert(0);
	*count = count.saturating_add(1);
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeSet;

	use super::*;

	fn star_missed() -> OptimizerRuntimeVerdictKey {
		OptimizerRuntimeVerdictKey {
			kind: OptimizerDecisionKind::StarCandidate,
			verdict: OptimizerRuntimeVerdictClass::MissedWin,
			lane: OptimizerRuntimeEvidenceLane::Total,
		}
	}

	#[test]
	fn runtime_evidence_keys_are_fixed_80_key_matrix() {
		let keys = all_verdict_counter_keys();
		assert_eq!(keys.len(), 80);
		let unique = keys
			.iter()
			.map(|(_, key)| key.clone())
			.collect::<BTreeSet<_>>();
		assert_eq!(unique.len(), 80);
		assert!(!keys.iter().any(|(_, key)| {
			key.windows(b"overflow".len())
				.any(|window| window == b"overflow")
		}));
		assert!(
			keys.iter()
				.any(|(_, key)| key == b"optimizer_evidence/verdict/star_candidate/missed_win")
		);
		assert!(
			keys.iter()
				.any(|(_, key)| key == b"optimizer_evidence/verdict/bridge_chain_root/vindicated")
		);
		assert!(
			keys.iter()
				.any(|(_, key)| key
					== b"optimizer_evidence/verdict/star_candidate/demoted/missed_win")
		);
		assert!(keys.iter().any(
			|(_, key)| key == b"optimizer_evidence/verdict/preserved_outer_order/indeterminate"
		));
	}

	#[test]
	fn runtime_evidence_decision_keys_are_fixed_140_key_matrix() {
		let decision_keys = all_decision_counter_keys();
		let verdict_keys = all_verdict_counter_keys();
		assert_eq!(decision_keys.len(), 140);
		assert_eq!(decision_keys.len() + verdict_keys.len(), 220);
		let unique = decision_keys
			.iter()
			.map(|(_, key)| key.clone())
			.collect::<BTreeSet<_>>();
		assert_eq!(unique.len(), 140);
		assert!(
			decision_keys
				.iter()
				.all(|(key, _)| key.kind != OptimizerDecisionKind::PreservedOuterOrder)
		);
		assert!(!decision_keys.iter().any(|(_, key)| {
			key.windows(b"overflow".len())
				.any(|window| window == b"overflow")
		}));
		assert!(
			decision_keys
				.iter()
				.any(|(_, key)| key == b"optimizer_evidence/decision/flat_star/reordered")
		);
		assert!(
			decision_keys.iter().any(|(_, key)| key
				== b"optimizer_evidence/decision/flat_star/declined/mixed_strategy_star")
		);
		assert!(decision_keys.iter().any(|(_, key)| key
			== b"optimizer_evidence/decision/flat_star/demoted/declined/non_inner_equi_join"));
	}

	#[test]
	fn runtime_evidence_local_only_snapshot_unchanged() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		evidence.record_verdict(
			OptimizerDecisionKind::StarCandidate,
			OptimizerRuntimeVerdictClass::MissedWin,
			OptimizerRuntimeVerdictProvenance::Native,
		);
		evidence.record_verdict(
			OptimizerDecisionKind::StarCandidate,
			OptimizerRuntimeVerdictClass::MissedWin,
			OptimizerRuntimeVerdictProvenance::Native,
		);
		assert_eq!(
			evidence.verdict_snapshot(),
			vec![OptimizerRuntimeVerdictRow {
				kind: OptimizerDecisionKind::StarCandidate,
				verdict: OptimizerRuntimeVerdictClass::MissedWin,
				provenance: OptimizerRuntimeVerdictProvenance::Native,
				count: 2,
			}]
		);
	}

	#[test]
	fn runtime_evidence_demoted_decision_dual_writes_total_and_sibling() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		let touched = evidence.record_decision(
			OptimizerDecisionKind::FlatStar,
			OptimizerDecisionOutcome::Reordered,
			OptimizerRuntimeVerdictProvenance::Demoted,
		);
		assert_eq!(
			touched,
			vec![
				OptimizerRuntimeDecisionKey {
					kind: OptimizerDecisionKind::FlatStar,
					outcome: OptimizerDecisionOutcome::Reordered,
					lane: OptimizerRuntimeEvidenceLane::Total,
				},
				OptimizerRuntimeDecisionKey {
					kind: OptimizerDecisionKind::FlatStar,
					outcome: OptimizerDecisionOutcome::Reordered,
					lane: OptimizerRuntimeEvidenceLane::Demoted,
				},
			]
		);
		assert_eq!(
			evidence.decision_snapshot(),
			vec![OptimizerDecisionRow {
				kind: OptimizerDecisionKind::FlatStar,
				outcome: OptimizerDecisionOutcome::Reordered,
				provenance: OptimizerRuntimeVerdictProvenance::Demoted,
				count: 1,
			}]
		);
	}

	#[test]
	fn native_decision_rows_are_total_minus_demoted() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		evidence.record_decision(
			OptimizerDecisionKind::FlatStar,
			OptimizerDecisionOutcome::Reordered,
			OptimizerRuntimeVerdictProvenance::Native,
		);
		evidence.record_decision(
			OptimizerDecisionKind::FlatStar,
			OptimizerDecisionOutcome::Reordered,
			OptimizerRuntimeVerdictProvenance::Demoted,
		);
		assert_eq!(
			evidence.decision_snapshot(),
			vec![
				OptimizerDecisionRow {
					kind: OptimizerDecisionKind::FlatStar,
					outcome: OptimizerDecisionOutcome::Reordered,
					provenance: OptimizerRuntimeVerdictProvenance::Native,
					count: 1,
				},
				OptimizerDecisionRow {
					kind: OptimizerDecisionKind::FlatStar,
					outcome: OptimizerDecisionOutcome::Reordered,
					provenance: OptimizerRuntimeVerdictProvenance::Demoted,
					count: 1,
				},
			]
		);
	}

	#[test]
	fn decline_reason_is_preserved_in_durable_key() {
		let keys = all_decision_counter_keys();
		assert!(keys.iter().any(|(_, key)| key
			== b"optimizer_evidence/decision/snowflake_root/declined/missing_estimate"));
		assert!(keys.iter().any(|(_, key)| {
			key
			== b"optimizer_evidence/decision/snowflake_root/demoted/declined/probe_not_scan_linear"
		}));
		assert!(
			!keys
				.iter()
				.any(|(_, key)| key == b"optimizer_evidence/decision/snowflake_root/declined")
		);
	}

	#[test]
	fn preserved_outer_order_records_no_durable_decision() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		assert!(
			evidence
				.record_decision(
					OptimizerDecisionKind::PreservedOuterOrder,
					OptimizerDecisionOutcome::Reordered,
					OptimizerRuntimeVerdictProvenance::Native,
				)
				.is_empty()
		);
		assert!(evidence.decision_snapshot().is_empty());
	}

	#[test]
	fn runtime_evidence_demoted_verdict_dual_writes_total_and_sibling() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		let touched = evidence.record_verdict(
			OptimizerDecisionKind::FlatStar,
			OptimizerRuntimeVerdictClass::Vindicated,
			OptimizerRuntimeVerdictProvenance::Demoted,
		);
		assert_eq!(
			touched,
			vec![
				OptimizerRuntimeVerdictKey {
					kind: OptimizerDecisionKind::FlatStar,
					verdict: OptimizerRuntimeVerdictClass::Vindicated,
					lane: OptimizerRuntimeEvidenceLane::Total,
				},
				OptimizerRuntimeVerdictKey {
					kind: OptimizerDecisionKind::FlatStar,
					verdict: OptimizerRuntimeVerdictClass::Vindicated,
					lane: OptimizerRuntimeEvidenceLane::Demoted,
				},
			]
		);
		assert_eq!(
			evidence.verdict_snapshot(),
			vec![OptimizerRuntimeVerdictRow {
				kind: OptimizerDecisionKind::FlatStar,
				verdict: OptimizerRuntimeVerdictClass::Vindicated,
				provenance: OptimizerRuntimeVerdictProvenance::Demoted,
				count: 1,
			}]
		);
	}

	#[test]
	fn runtime_evidence_cache_plus_pending_counts_once() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		let key = star_missed();
		{
			let mut state = evidence.state.lock().expect("optimizer runtime evidence");
			state.durable_cache.insert(key, 3);
		}
		evidence.record_verdict(
			key.kind,
			key.verdict,
			OptimizerRuntimeVerdictProvenance::Native,
		);
		assert_eq!(evidence.verdict_snapshot()[0].count, 4);
	}

	#[test]
	fn runtime_evidence_in_flight_delta_visible_once() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		let key = star_missed();
		{
			let mut state = evidence.state.lock().expect("optimizer runtime evidence");
			state.durable_cache.insert(key, 10);
			state.in_flight.insert(key, 2);
		}
		assert_eq!(evidence.verdict_snapshot()[0].count, 12);
	}

	#[test]
	fn runtime_evidence_flush_ack_does_not_double_count() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		let key = star_missed();
		{
			let mut state = evidence.state.lock().expect("optimizer runtime evidence");
			state.in_flight.insert(key, 1);
		}
		evidence.ack_flush(key, 1);
		assert_eq!(evidence.verdict_snapshot()[0].count, 1);
	}

	#[test]
	fn runtime_evidence_ack_larger_wins() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		let key = star_missed();
		{
			let mut state = evidence.state.lock().expect("optimizer runtime evidence");
			state.durable_cache.insert(key, 7);
			state.in_flight.insert(key, 1);
		}
		evidence.ack_flush(key, 5);
		assert_eq!(evidence.verdict_snapshot()[0].count, 7);
	}

	#[test]
	fn runtime_evidence_ack_after_replication_latch_keeps_degraded_status() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		let key = star_missed();
		{
			let mut state = evidence.state.lock().expect("optimizer runtime evidence");
			state.in_flight.insert(key, 1);
		}
		evidence.latch_replication_unsupported(key, 1, "unsupported".to_string());
		{
			let mut state = evidence.state.lock().expect("optimizer runtime evidence");
			state.in_flight.insert(key, 1);
		}

		evidence.ack_flush(key, 2);

		let status = evidence.status_snapshot();
		assert_eq!(status.mode, OptimizerEvidenceMode::Degraded);
		assert_eq!(status.state, OptimizerEvidenceState::ReplicationUnsupported);
		assert_eq!(status.last_error.as_deref(), Some("unsupported"));
		assert_eq!(evidence.verdict_snapshot()[0].count, 2);
	}

	#[test]
	fn runtime_evidence_ambiguous_flush_drops_without_retry() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		let key = star_missed();
		{
			let mut state = evidence.state.lock().expect("optimizer runtime evidence");
			state.in_flight.insert(key, 2);
		}
		evidence.drop_ambiguous_in_flight(key, 2, "timeout".to_string());
		let status = evidence.status_snapshot();
		assert_eq!(status.pending_deltas, 0);
		assert_eq!(status.dropped_deltas, 2);
		assert_eq!(status.ambiguous_deltas, 2);
		assert_eq!(status.state, OptimizerEvidenceState::Unavailable);
	}
}
