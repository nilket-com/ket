use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use wendelon::schema::{TenantId, WriteStatus};
use wendelon_client::{Client, ClientError, WriteOptions};

use super::decision_log::{
	OptimizerDecisionKind, OptimizerDecisionOutcome, OptimizerDecisionRecorder,
	OptimizerDecisionRow, OptimizerProbeLinearityConsumer, OptimizerProbeLinearityFenceKey,
	OptimizerProbeLinearityFenceRow, OptimizerProbeLinearityFenceStatus,
	OptimizerRuntimeDecisionKey, OptimizerRuntimeEvidenceLane, OptimizerRuntimeVerdictClass,
	OptimizerRuntimeVerdictKey, OptimizerRuntimeVerdictProvenance, OptimizerRuntimeVerdictRow,
	decision_recording_kinds, optimizer_decision_decline_reason_label,
	optimizer_decision_kind_label, optimizer_decision_outcome_label, optimizer_decision_outcomes,
	optimizer_probe_linearity_consumer_label, optimizer_probe_linearity_consumers,
	optimizer_runtime_verdict_label,
};
use super::join::{ProbeLinearityFence, probe_linearity_fences};

pub const OPTIMIZER_EVIDENCE_KEY_PREFIX: &str = "optimizer_evidence/verdict/";
pub const OPTIMIZER_DECISION_EVIDENCE_KEY_PREFIX: &str = "optimizer_evidence/decision/";
pub const OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX: &str = "optimizer_evidence/window/";
pub const OPTIMIZER_PROBE_LINEARITY_EVIDENCE_KEY_PREFIX: &str =
	"optimizer_evidence/probe_linearity/";
const UNAVAILABLE_CIRCUIT_BREAKER_FAILURES: u64 = 3;
const OPTIMIZER_EVIDENCE_WINDOW_BUCKET: Duration = Duration::from_secs(60);
const PROBE_LINEARITY_FENCE_MIN_SAMPLE: u64 = 20;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerReceiptGateSnapshot {
	pub(crate) verdicts: Vec<OptimizerRuntimeVerdictRow>,
	pub(crate) decisions: Vec<OptimizerDecisionRow>,
	pub(crate) recent_decisions: Vec<OptimizerDecisionRow>,
	pub(crate) health: OptimizerEvidenceStatusSnapshot,
	pub(crate) window: OptimizerEvidenceWindowSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct OptimizerEvidenceWindowId(pub(crate) u64);

impl OptimizerEvidenceWindowId {
	fn previous(self) -> Self {
		Self(self.0.saturating_sub(1))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerEvidenceWindowCounterKey {
	Verdict {
		window: OptimizerEvidenceWindowId,
		key: OptimizerRuntimeVerdictKey,
	},
	Decision {
		window: OptimizerEvidenceWindowId,
		key: OptimizerRuntimeDecisionKey,
	},
	ProbeLinearityFence {
		window: OptimizerEvidenceWindowId,
		key: OptimizerProbeLinearityFenceKey,
	},
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerEvidenceWindowSnapshot {
	pub(crate) current_window: OptimizerEvidenceWindowId,
	pub(crate) previous_window: OptimizerEvidenceWindowId,
	pub(crate) recent_count: u64,
	pub(crate) stale_lifetime_count: u64,
	pub(crate) lifetime_count: u64,
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
	window_id_source: OptimizerEvidenceWindowIdSource,
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
	probe_linearity_pending: BTreeMap<OptimizerProbeLinearityFenceKey, u64>,
	probe_linearity_in_flight: BTreeMap<OptimizerProbeLinearityFenceKey, u64>,
	probe_linearity_durable_cache: BTreeMap<OptimizerProbeLinearityFenceKey, u64>,
	window_pending: BTreeMap<OptimizerEvidenceWindowCounterKey, u64>,
	window_in_flight: BTreeMap<OptimizerEvidenceWindowCounterKey, u64>,
	window_durable_cache: BTreeMap<OptimizerEvidenceWindowCounterKey, u64>,
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
			probe_linearity_pending: BTreeMap::new(),
			probe_linearity_in_flight: BTreeMap::new(),
			probe_linearity_durable_cache: BTreeMap::new(),
			window_pending: BTreeMap::new(),
			window_in_flight: BTreeMap::new(),
			window_durable_cache: BTreeMap::new(),
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

#[derive(Debug, Clone)]
enum OptimizerEvidenceWindowIdSource {
	System {
		bucket: Duration,
	},
	#[cfg(test)]
	Manual(Arc<AtomicU64>),
}

impl OptimizerEvidenceWindowIdSource {
	fn system() -> Self {
		Self::System {
			bucket: OPTIMIZER_EVIDENCE_WINDOW_BUCKET,
		}
	}

	fn current(&self) -> OptimizerEvidenceWindowId {
		match self {
			Self::System { bucket } => {
				let bucket_secs = bucket.as_secs().max(1);
				let now_secs = SystemTime::now()
					.duration_since(UNIX_EPOCH)
					.unwrap_or(Duration::ZERO)
					.as_secs();
				OptimizerEvidenceWindowId(now_secs / bucket_secs)
			}
			#[cfg(test)]
			Self::Manual(value) => OptimizerEvidenceWindowId(value.load(AtomicOrdering::SeqCst)),
		}
	}
}

impl OptimizerRuntimeEvidence {
	pub fn local_only() -> Self {
		Self {
			state: Mutex::new(RuntimeEvidenceState::default()),
			binding: None,
			window_id_source: OptimizerEvidenceWindowIdSource::system(),
		}
	}

	#[cfg(test)]
	pub(crate) fn local_only_with_window_id(window_id: u64) -> Self {
		Self {
			state: Mutex::new(RuntimeEvidenceState::default()),
			binding: None,
			window_id_source: OptimizerEvidenceWindowIdSource::Manual(Arc::new(AtomicU64::new(
				window_id,
			))),
		}
	}

	pub fn with_wendelon(binding: DurableEvidenceBinding) -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(RuntimeEvidenceState {
				mode: OptimizerEvidenceMode::Durable,
				..RuntimeEvidenceState::default()
			}),
			binding: Some(binding),
			window_id_source: OptimizerEvidenceWindowIdSource::system(),
		})
	}

	#[cfg(test)]
	pub(crate) fn set_window_id_for_test(&self, window_id: u64) {
		let OptimizerEvidenceWindowIdSource::Manual(value) = &self.window_id_source else {
			panic!("test window id setter requires a manual window id source");
		};
		value.store(window_id, AtomicOrdering::SeqCst);
	}

	pub(crate) fn record_verdict(
		&self,
		kind: OptimizerDecisionKind,
		verdict: OptimizerRuntimeVerdictClass,
		provenance: OptimizerRuntimeVerdictProvenance,
	) -> Vec<OptimizerRuntimeVerdictKey> {
		let mut keys = Vec::with_capacity(2);
		let window = self.window_id_source.current();
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		let total = OptimizerRuntimeVerdictKey {
			kind,
			verdict,
			lane: OptimizerRuntimeEvidenceLane::Total,
		};
		increment_count(&mut state.pending, total);
		increment_count(
			&mut state.window_pending,
			OptimizerEvidenceWindowCounterKey::Verdict { window, key: total },
		);
		keys.push(total);
		if provenance == OptimizerRuntimeVerdictProvenance::Demoted {
			let demoted = OptimizerRuntimeVerdictKey {
				kind,
				verdict,
				lane: OptimizerRuntimeEvidenceLane::Demoted,
			};
			increment_count(&mut state.pending, demoted);
			increment_count(
				&mut state.window_pending,
				OptimizerEvidenceWindowCounterKey::Verdict {
					window,
					key: demoted,
				},
			);
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
		let window = self.window_id_source.current();
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		let total = OptimizerRuntimeDecisionKey {
			kind,
			outcome,
			lane: OptimizerRuntimeEvidenceLane::Total,
		};
		increment_count(&mut state.decision_pending, total);
		increment_count(
			&mut state.window_pending,
			OptimizerEvidenceWindowCounterKey::Decision { window, key: total },
		);
		keys.push(total);
		if provenance == OptimizerRuntimeVerdictProvenance::Demoted {
			let demoted = OptimizerRuntimeDecisionKey {
				kind,
				outcome,
				lane: OptimizerRuntimeEvidenceLane::Demoted,
			};
			increment_count(&mut state.decision_pending, demoted);
			increment_count(
				&mut state.window_pending,
				OptimizerEvidenceWindowCounterKey::Decision {
					window,
					key: demoted,
				},
			);
			keys.push(demoted);
		}
		keys
	}

	pub(crate) fn record_probe_linearity_fence(
		&self,
		consumer: OptimizerProbeLinearityConsumer,
		fence: ProbeLinearityFence,
	) -> OptimizerProbeLinearityFenceKey {
		let window = self.window_id_source.current();
		let key = OptimizerProbeLinearityFenceKey { consumer, fence };
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		increment_count(&mut state.probe_linearity_pending, key);
		increment_count(
			&mut state.window_pending,
			OptimizerEvidenceWindowCounterKey::ProbeLinearityFence { window, key },
		);
		key
	}

	pub(crate) fn verdict_snapshot(&self) -> Vec<OptimizerRuntimeVerdictRow> {
		let state = self.state.lock().expect("optimizer runtime evidence");
		verdict_rows_from_state(&state)
	}

	pub(crate) fn decision_snapshot(&self) -> Vec<OptimizerDecisionRow> {
		let state = self.state.lock().expect("optimizer runtime evidence");
		decision_rows_from_state(&state)
	}

	pub(crate) fn probe_linearity_fence_snapshot(&self) -> Vec<OptimizerProbeLinearityFenceRow> {
		let current_window = self.window_id_source.current();
		let state = self.state.lock().expect("optimizer runtime evidence");
		probe_linearity_fence_rows_from_state(&state, current_window)
	}

	pub(crate) fn status_snapshot(&self) -> OptimizerEvidenceStatusSnapshot {
		let state = self.state.lock().expect("optimizer runtime evidence");
		status_snapshot_from_state(&state)
	}

	pub(crate) fn receipt_gate_snapshot(&self) -> OptimizerReceiptGateSnapshot {
		let current_window = self.window_id_source.current();
		let state = self.state.lock().expect("optimizer runtime evidence");
		let verdicts = verdict_rows_from_state(&state);
		let decisions = decision_rows_from_state(&state);
		let recent_decisions = recent_decision_rows_from_state(&state, current_window);
		let health = status_snapshot_from_state(&state);
		let window = window_snapshot_from_state(&state, current_window);
		OptimizerReceiptGateSnapshot {
			verdicts,
			decisions,
			recent_decisions,
			health,
			window,
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

	pub async fn flush_probe_linearity_deltas(&self) {
		let Some(binding) = self.binding.clone() else {
			return;
		};
		let keys = self.pending_probe_linearity_keys();
		for key in keys {
			let Some(delta) = self.take_pending_probe_linearity_for_flush(key) else {
				continue;
			};
			let key_bytes = probe_linearity_counter_key(key);
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
				Ok(Ok(receipt)) => self.ack_probe_linearity_flush(key, receipt.current_value),
				Ok(Err(ClientError::WriteFailed { status }))
					if status == WriteStatus::COUNTER_REPLICATION_UNSUPPORTED =>
				{
					self.latch_probe_linearity_replication_unsupported(
						key,
						delta,
						format!("counter fetch-add failed with status {}", status.as_str()),
					);
				}
				Ok(Err(error)) => {
					self.drop_ambiguous_probe_linearity_in_flight(key, delta, error.to_string())
				}
				Err(_) => self.drop_ambiguous_probe_linearity_in_flight(
					key,
					delta,
					"counter fetch-add timed out".to_string(),
				),
			}
		}
	}

	pub async fn flush_window_deltas(&self) {
		let Some(binding) = self.binding.clone() else {
			return;
		};
		let keys = self.pending_window_keys();
		for key in keys {
			let Some(delta) = self.take_pending_window_for_flush(key) else {
				continue;
			};
			let key_bytes = window_counter_key(key);
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
				Ok(Ok(receipt)) => self.ack_window_flush(key, receipt.current_value),
				Ok(Err(ClientError::WriteFailed { status }))
					if status == WriteStatus::COUNTER_REPLICATION_UNSUPPORTED =>
				{
					self.latch_window_replication_unsupported(
						key,
						delta,
						format!("counter fetch-add failed with status {}", status.as_str()),
					);
				}
				Ok(Err(error)) => {
					self.drop_ambiguous_window_in_flight(key, delta, error.to_string())
				}
				Err(_) => self.drop_ambiguous_window_in_flight(
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
		let mut refreshed_probe_linearity = BTreeMap::new();
		for (key, key_bytes) in all_probe_linearity_counter_keys() {
			match binding
				.client
				.counter_read(binding.tenant_id, &key_bytes)
				.await
			{
				Ok(value) => {
					refreshed_probe_linearity.insert(key, value.unwrap_or(0));
				}
				Err(error) => {
					self.mark_refresh_failed(error.to_string());
					return OptimizerEvidenceRefreshReport {
						refreshed_keys: refreshed
							.len()
							.saturating_add(refreshed_decisions.len())
							.saturating_add(refreshed_probe_linearity.len()),
						failed: true,
					};
				}
			}
		}
		let current_window = self.window_id_source.current();
		let previous_window = current_window.previous();
		let mut refreshed_windows = BTreeMap::new();
		for (key, key_bytes) in all_window_counter_keys(current_window, previous_window) {
			match binding
				.client
				.counter_read(binding.tenant_id, &key_bytes)
				.await
			{
				Ok(value) => {
					refreshed_windows.insert(key, value.unwrap_or(0));
				}
				Err(error) => {
					self.mark_refresh_failed(error.to_string());
					return OptimizerEvidenceRefreshReport {
						refreshed_keys: refreshed
							.len()
							.saturating_add(refreshed_decisions.len())
							.saturating_add(refreshed_probe_linearity.len())
							.saturating_add(refreshed_windows.len()),
						failed: true,
					};
				}
			}
		}
		let refreshed_keys = refreshed
			.len()
			.saturating_add(refreshed_decisions.len())
			.saturating_add(refreshed_probe_linearity.len())
			.saturating_add(refreshed_windows.len());
		self.merge_refresh(
			refreshed,
			refreshed_decisions,
			refreshed_probe_linearity,
			refreshed_windows,
		);
		OptimizerEvidenceRefreshReport {
			refreshed_keys,
			failed: false,
		}
	}

	fn pending_decision_keys(&self) -> Vec<OptimizerRuntimeDecisionKey> {
		let state = self.state.lock().expect("optimizer runtime evidence");
		state.decision_pending.keys().copied().collect()
	}

	fn pending_probe_linearity_keys(&self) -> Vec<OptimizerProbeLinearityFenceKey> {
		let state = self.state.lock().expect("optimizer runtime evidence");
		state.probe_linearity_pending.keys().copied().collect()
	}

	fn pending_window_keys(&self) -> Vec<OptimizerEvidenceWindowCounterKey> {
		let state = self.state.lock().expect("optimizer runtime evidence");
		state.window_pending.keys().copied().collect()
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

	fn take_pending_probe_linearity_for_flush(
		&self,
		key: OptimizerProbeLinearityFenceKey,
	) -> Option<u64> {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		if self.binding.is_none()
			|| state.replication_unsupported
			|| state.circuit_open
			|| state
				.probe_linearity_in_flight
				.get(&key)
				.copied()
				.unwrap_or(0)
				> 0
		{
			return None;
		}
		let delta = state.probe_linearity_pending.remove(&key).unwrap_or(0);
		if delta == 0 {
			return None;
		}
		state.probe_linearity_in_flight.insert(key, delta);
		Some(delta)
	}

	fn take_pending_window_for_flush(&self, key: OptimizerEvidenceWindowCounterKey) -> Option<u64> {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		if self.binding.is_none()
			|| state.replication_unsupported
			|| state.circuit_open
			|| state.window_in_flight.get(&key).copied().unwrap_or(0) > 0
		{
			return None;
		}
		let delta = state.window_pending.remove(&key).unwrap_or(0);
		if delta == 0 {
			return None;
		}
		state.window_in_flight.insert(key, delta);
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

	fn ack_probe_linearity_flush(&self, key: OptimizerProbeLinearityFenceKey, current_value: u64) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.probe_linearity_in_flight.remove(&key);
		let cached = state.probe_linearity_durable_cache.entry(key).or_insert(0);
		*cached = (*cached).max(current_value);
		if !state.replication_unsupported {
			state.consecutive_unavailable_failures = 0;
			state.circuit_open = false;
			state.mode = OptimizerEvidenceMode::Durable;
			state.state = OptimizerEvidenceState::Ok;
			state.last_error = None;
		}
	}

	fn ack_window_flush(&self, key: OptimizerEvidenceWindowCounterKey, current_value: u64) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.window_in_flight.remove(&key);
		let cached = state.window_durable_cache.entry(key).or_insert(0);
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

	fn drop_ambiguous_probe_linearity_in_flight(
		&self,
		key: OptimizerProbeLinearityFenceKey,
		delta: u64,
		error: String,
	) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.probe_linearity_in_flight.remove(&key);
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

	fn drop_ambiguous_window_in_flight(
		&self,
		key: OptimizerEvidenceWindowCounterKey,
		delta: u64,
		error: String,
	) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.window_in_flight.remove(&key);
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

	fn latch_probe_linearity_replication_unsupported(
		&self,
		key: OptimizerProbeLinearityFenceKey,
		delta: u64,
		error: String,
	) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.probe_linearity_in_flight.remove(&key);
		state.dropped_deltas = state.dropped_deltas.saturating_add(delta);
		state.ambiguous_deltas = state.ambiguous_deltas.saturating_add(delta);
		state.replication_unsupported = true;
		state.circuit_open = true;
		state.mode = OptimizerEvidenceMode::Degraded;
		state.state = OptimizerEvidenceState::ReplicationUnsupported;
		state.last_error = Some(error);
	}

	fn latch_window_replication_unsupported(
		&self,
		key: OptimizerEvidenceWindowCounterKey,
		delta: u64,
		error: String,
	) {
		let mut state = self.state.lock().expect("optimizer runtime evidence");
		state.window_in_flight.remove(&key);
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
		refreshed_probe_linearity: BTreeMap<OptimizerProbeLinearityFenceKey, u64>,
		refreshed_windows: BTreeMap<OptimizerEvidenceWindowCounterKey, u64>,
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
		for (key, value) in refreshed_probe_linearity {
			let cached = state.probe_linearity_durable_cache.entry(key).or_insert(0);
			*cached = (*cached).max(value);
		}
		for (key, value) in refreshed_windows {
			let cached = state.window_durable_cache.entry(key).or_insert(0);
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

	fn record_probe_linearity_fence(
		&self,
		consumer: OptimizerProbeLinearityConsumer,
		fence: ProbeLinearityFence,
	) {
		self.evidence.record_probe_linearity_fence(consumer, fence);
	}
}

pub(crate) fn verdict_counter_key(key: OptimizerRuntimeVerdictKey) -> Vec<u8> {
	format!(
		"{OPTIMIZER_EVIDENCE_KEY_PREFIX}{}",
		verdict_counter_suffix(key)
	)
	.into_bytes()
}

pub(crate) fn decision_counter_key(key: OptimizerRuntimeDecisionKey) -> Vec<u8> {
	format!(
		"{OPTIMIZER_DECISION_EVIDENCE_KEY_PREFIX}{}",
		decision_counter_suffix(key)
	)
	.into_bytes()
}

pub(crate) fn probe_linearity_counter_key(key: OptimizerProbeLinearityFenceKey) -> Vec<u8> {
	format!(
		"{OPTIMIZER_PROBE_LINEARITY_EVIDENCE_KEY_PREFIX}{}",
		probe_linearity_counter_suffix(key)
	)
	.into_bytes()
}

pub(crate) fn window_counter_key(key: OptimizerEvidenceWindowCounterKey) -> Vec<u8> {
	match key {
		OptimizerEvidenceWindowCounterKey::Verdict { window, key } => format!(
			"{OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX}{}/verdict/{}",
			window.0,
			verdict_counter_suffix(key)
		)
		.into_bytes(),
		OptimizerEvidenceWindowCounterKey::Decision { window, key } => format!(
			"{OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX}{}/decision/{}",
			window.0,
			decision_counter_suffix(key)
		)
		.into_bytes(),
		OptimizerEvidenceWindowCounterKey::ProbeLinearityFence { window, key } => format!(
			"{OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX}{}/probe_linearity/{}",
			window.0,
			probe_linearity_counter_suffix(key)
		)
		.into_bytes(),
	}
}

fn verdict_counter_suffix(key: OptimizerRuntimeVerdictKey) -> String {
	match key.lane {
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
	}
}

fn decision_counter_suffix(key: OptimizerRuntimeDecisionKey) -> String {
	match key.lane {
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
	}
}

fn probe_linearity_counter_suffix(key: OptimizerProbeLinearityFenceKey) -> String {
	format!(
		"{}/{}",
		optimizer_probe_linearity_consumer_label(key.consumer),
		key.fence.label()
	)
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

pub(crate) fn all_probe_linearity_counter_keys() -> Vec<(OptimizerProbeLinearityFenceKey, Vec<u8>)>
{
	let mut keys = Vec::with_capacity(
		optimizer_probe_linearity_consumers().len() * probe_linearity_fences().len(),
	);
	for consumer in optimizer_probe_linearity_consumers() {
		for fence in probe_linearity_fences() {
			let key = OptimizerProbeLinearityFenceKey { consumer, fence };
			keys.push((key, probe_linearity_counter_key(key)));
		}
	}
	keys
}

pub(crate) fn all_window_counter_keys(
	current_window: OptimizerEvidenceWindowId,
	previous_window: OptimizerEvidenceWindowId,
) -> Vec<(OptimizerEvidenceWindowCounterKey, Vec<u8>)> {
	let mut keys = Vec::with_capacity(
		2 * (all_verdict_counter_keys().len()
			+ all_decision_counter_keys().len()
			+ all_probe_linearity_counter_keys().len()),
	);
	for window in [current_window, previous_window] {
		for (key, _) in all_verdict_counter_keys() {
			let window_key = OptimizerEvidenceWindowCounterKey::Verdict { window, key };
			keys.push((window_key, window_counter_key(window_key)));
		}
		for (key, _) in all_decision_counter_keys() {
			let window_key = OptimizerEvidenceWindowCounterKey::Decision { window, key };
			keys.push((window_key, window_counter_key(window_key)));
		}
		for (key, _) in all_probe_linearity_counter_keys() {
			let window_key = OptimizerEvidenceWindowCounterKey::ProbeLinearityFence { window, key };
			keys.push((window_key, window_counter_key(window_key)));
		}
	}
	keys
}

fn verdict_rows_from_state(state: &RuntimeEvidenceState) -> Vec<OptimizerRuntimeVerdictRow> {
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

fn decision_rows_from_state(state: &RuntimeEvidenceState) -> Vec<OptimizerDecisionRow> {
	let mut counts = state.decision_durable_cache.clone();
	for (key, count) in state
		.decision_pending
		.iter()
		.chain(state.decision_in_flight.iter())
	{
		let entry = counts.entry(*key).or_insert(0);
		*entry = entry.saturating_add(*count);
	}
	decision_rows_from_counts(&counts)
}

fn recent_decision_rows_from_state(
	state: &RuntimeEvidenceState,
	current_window: OptimizerEvidenceWindowId,
) -> Vec<OptimizerDecisionRow> {
	let previous_window = current_window.previous();
	let mut counts: BTreeMap<OptimizerRuntimeDecisionKey, u64> = BTreeMap::new();
	for window_counts in [
		&state.window_durable_cache,
		&state.window_pending,
		&state.window_in_flight,
	] {
		for (counter_key, count) in window_counts {
			let OptimizerEvidenceWindowCounterKey::Decision { window, key } = counter_key else {
				continue;
			};
			if *window != current_window && *window != previous_window {
				continue;
			}
			let entry = counts.entry(*key).or_insert(0);
			*entry = (*entry).saturating_add(*count);
		}
	}
	decision_rows_from_counts(&counts)
}

fn probe_linearity_fence_rows_from_state(
	state: &RuntimeEvidenceState,
	current_window: OptimizerEvidenceWindowId,
) -> Vec<OptimizerProbeLinearityFenceRow> {
	let lifetime_counts = probe_linearity_lifetime_counts_from_state(state);
	let recent_counts = recent_probe_linearity_counts_from_state(state, current_window);
	let mut consumer_lifetime_totals: BTreeMap<OptimizerProbeLinearityConsumer, u64> =
		BTreeMap::new();
	let mut consumer_recent_totals: BTreeMap<OptimizerProbeLinearityConsumer, u64> =
		BTreeMap::new();
	for (key, count) in &lifetime_counts {
		let total = consumer_lifetime_totals.entry(key.consumer).or_insert(0);
		*total = total.saturating_add(*count);
	}
	for (key, count) in &recent_counts {
		let total = consumer_recent_totals.entry(key.consumer).or_insert(0);
		*total = total.saturating_add(*count);
	}

	let mut rows = Vec::new();
	for consumer in optimizer_probe_linearity_consumers() {
		let consumer_lifetime_total = consumer_lifetime_totals
			.get(&consumer)
			.copied()
			.unwrap_or(0);
		let consumer_recent_total = consumer_recent_totals.get(&consumer).copied().unwrap_or(0);
		for fence in probe_linearity_fences() {
			let key = OptimizerProbeLinearityFenceKey { consumer, fence };
			let lifetime_count = lifetime_counts.get(&key).copied().unwrap_or(0);
			let recent_count = recent_counts.get(&key).copied().unwrap_or(0);
			let recent_share_per_mille = if consumer_recent_total == 0 {
				0
			} else {
				recent_count.saturating_mul(1000) / consumer_recent_total
			};
			let (status, reason) =
				probe_linearity_status_and_reason(recent_count, consumer_recent_total);
			rows.push(OptimizerProbeLinearityFenceRow {
				consumer,
				fence,
				recent_count,
				lifetime_count,
				consumer_recent_total,
				consumer_lifetime_total,
				recent_share_per_mille,
				status,
				reason: reason.to_string(),
				next_cut: probe_linearity_next_cut(status),
			});
		}
	}
	rows
}

fn probe_linearity_lifetime_counts_from_state(
	state: &RuntimeEvidenceState,
) -> BTreeMap<OptimizerProbeLinearityFenceKey, u64> {
	let mut counts = state.probe_linearity_durable_cache.clone();
	for (key, count) in state
		.probe_linearity_pending
		.iter()
		.chain(state.probe_linearity_in_flight.iter())
	{
		let entry = counts.entry(*key).or_insert(0);
		*entry = entry.saturating_add(*count);
	}
	counts
}

fn recent_probe_linearity_counts_from_state(
	state: &RuntimeEvidenceState,
	current_window: OptimizerEvidenceWindowId,
) -> BTreeMap<OptimizerProbeLinearityFenceKey, u64> {
	let previous_window = current_window.previous();
	let mut counts: BTreeMap<OptimizerProbeLinearityFenceKey, u64> = BTreeMap::new();
	for window_counts in [
		&state.window_durable_cache,
		&state.window_pending,
		&state.window_in_flight,
	] {
		for (counter_key, count) in window_counts {
			let OptimizerEvidenceWindowCounterKey::ProbeLinearityFence { window, key } =
				counter_key
			else {
				continue;
			};
			if *window != current_window && *window != previous_window {
				continue;
			}
			let entry = counts.entry(*key).or_insert(0);
			*entry = (*entry).saturating_add(*count);
		}
	}
	counts
}

fn probe_linearity_status_and_reason(
	recent_count: u64,
	consumer_recent_total: u64,
) -> (OptimizerProbeLinearityFenceStatus, &'static str) {
	if recent_count == 0 {
		(
			OptimizerProbeLinearityFenceStatus::Absent,
			"no_recent_evidence",
		)
	} else if consumer_recent_total < PROBE_LINEARITY_FENCE_MIN_SAMPLE {
		(
			OptimizerProbeLinearityFenceStatus::InsufficientEvidence,
			"consumer_recent_total_below_floor",
		)
	} else if recent_count >= PROBE_LINEARITY_FENCE_MIN_SAMPLE
		&& recent_count.saturating_mul(2) > consumer_recent_total
	{
		(
			OptimizerProbeLinearityFenceStatus::Dominant,
			"recent_strict_majority",
		)
	} else {
		(
			OptimizerProbeLinearityFenceStatus::Observed,
			"recent_not_strict_majority",
		)
	}
}

fn probe_linearity_next_cut(status: OptimizerProbeLinearityFenceStatus) -> &'static str {
	match status {
		OptimizerProbeLinearityFenceStatus::Dominant => "targeted_probe_linearity_relaxation",
		OptimizerProbeLinearityFenceStatus::Absent
		| OptimizerProbeLinearityFenceStatus::InsufficientEvidence
		| OptimizerProbeLinearityFenceStatus::Observed => "await_dominant_probe_linearity_fence",
	}
}

fn decision_rows_from_counts(
	counts: &BTreeMap<OptimizerRuntimeDecisionKey, u64>,
) -> Vec<OptimizerDecisionRow> {
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

fn status_snapshot_from_state(state: &RuntimeEvidenceState) -> OptimizerEvidenceStatusSnapshot {
	OptimizerEvidenceStatusSnapshot {
		mode: state.mode,
		state: state.state,
		pending_deltas: sum_counts(&state.pending)
			.saturating_add(sum_counts(&state.in_flight))
			.saturating_add(sum_counts(&state.decision_pending))
			.saturating_add(sum_counts(&state.decision_in_flight))
			.saturating_add(sum_counts(&state.probe_linearity_pending))
			.saturating_add(sum_counts(&state.probe_linearity_in_flight))
			.saturating_add(sum_counts(&state.window_pending))
			.saturating_add(sum_counts(&state.window_in_flight)),
		dropped_deltas: state.dropped_deltas,
		ambiguous_deltas: state.ambiguous_deltas,
		refresh_failures: state.refresh_failures,
		last_error: state.last_error.clone(),
	}
}

fn window_snapshot_from_state(
	state: &RuntimeEvidenceState,
	current_window: OptimizerEvidenceWindowId,
) -> OptimizerEvidenceWindowSnapshot {
	let previous_window = current_window.previous();
	let lifetime_count = lifetime_total_count_from_state(state);
	let recent_count = window_total_count_from_state(state, current_window)
		.saturating_add(window_total_count_from_state(state, previous_window));
	OptimizerEvidenceWindowSnapshot {
		current_window,
		previous_window,
		recent_count,
		stale_lifetime_count: lifetime_count.saturating_sub(recent_count),
		lifetime_count,
	}
}

fn lifetime_total_count_from_state(state: &RuntimeEvidenceState) -> u64 {
	let mut total = 0u64;
	for counts in [&state.durable_cache, &state.pending, &state.in_flight] {
		for (key, count) in counts {
			if key.lane == OptimizerRuntimeEvidenceLane::Total {
				total = total.saturating_add(*count);
			}
		}
	}
	for counts in [
		&state.decision_durable_cache,
		&state.decision_pending,
		&state.decision_in_flight,
	] {
		for (key, count) in counts {
			if key.lane == OptimizerRuntimeEvidenceLane::Total {
				total = total.saturating_add(*count);
			}
		}
	}
	for counts in [
		&state.probe_linearity_durable_cache,
		&state.probe_linearity_pending,
		&state.probe_linearity_in_flight,
	] {
		for count in counts.values() {
			total = total.saturating_add(*count);
		}
	}
	total
}

fn window_total_count_from_state(
	state: &RuntimeEvidenceState,
	window: OptimizerEvidenceWindowId,
) -> u64 {
	let mut total = 0u64;
	for counts in [
		&state.window_durable_cache,
		&state.window_pending,
		&state.window_in_flight,
	] {
		for (key, count) in counts {
			let total_lane = match key {
				OptimizerEvidenceWindowCounterKey::Verdict {
					window: key_window,
					key,
				} => *key_window == window && key.lane == OptimizerRuntimeEvidenceLane::Total,
				OptimizerEvidenceWindowCounterKey::Decision {
					window: key_window,
					key,
				} => *key_window == window && key.lane == OptimizerRuntimeEvidenceLane::Total,
				OptimizerEvidenceWindowCounterKey::ProbeLinearityFence {
					window: key_window,
					..
				} => *key_window == window,
			};
			if total_lane {
				total = total.saturating_add(*count);
			}
		}
	}
	total
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
	fn adr222_window_keys_are_separate_namespace_not_in_220_matrix() {
		let window_keys =
			all_window_counter_keys(OptimizerEvidenceWindowId(9), OptimizerEvidenceWindowId(8));
		assert_eq!(window_keys.len(), 468);
		let unique = window_keys
			.iter()
			.map(|(_, key)| key.clone())
			.collect::<BTreeSet<_>>();
		assert_eq!(unique.len(), 468);
		assert!(
			window_keys
				.iter()
				.all(|(_, key)| key.starts_with(OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX.as_bytes()))
		);
		assert!(window_keys.iter().any(
			|(_, key)| key == b"optimizer_evidence/window/9/verdict/star_candidate/missed_win"
		));
		assert!(window_keys.iter().any(|(_, key)| {
			key
			== b"optimizer_evidence/window/8/decision/flat_star/demoted/declined/non_inner_equi_join"
		}));
		assert!(window_keys.iter().any(|(_, key)| {
			key == b"optimizer_evidence/window/9/probe_linearity/hash_fallback/non_broadcast_join"
		}));
		assert!(window_keys.iter().any(|(_, key)| {
			key == b"optimizer_evidence/window/8/probe_linearity/star_decline/sort"
		}));
		assert_eq!(all_verdict_counter_keys().len(), 80);
		assert_eq!(all_decision_counter_keys().len(), 140);
		assert_eq!(all_probe_linearity_counter_keys().len(), 14);
	}

	#[test]
	fn adr229_probe_linearity_keys_are_fixed_14_key_detail_matrix() {
		let keys = all_probe_linearity_counter_keys();
		assert_eq!(keys.len(), 14);
		let unique = keys
			.iter()
			.map(|(_, key)| key.clone())
			.collect::<BTreeSet<_>>();
		assert_eq!(unique.len(), 14);
		assert!(keys.iter().all(|(_, key)| {
			key.starts_with(OPTIMIZER_PROBE_LINEARITY_EVIDENCE_KEY_PREFIX.as_bytes())
		}));
		assert!(keys.iter().any(|(_, key)| {
			key == b"optimizer_evidence/probe_linearity/hash_fallback/non_broadcast_join"
		}));
		assert!(
			keys.iter()
				.any(|(_, key)| key == b"optimizer_evidence/probe_linearity/star_decline/sort")
		);
	}

	#[test]
	fn lifetime_matrix_counts_unchanged_by_windowing() {
		let verdict_keys = all_verdict_counter_keys();
		let decision_keys = all_decision_counter_keys();
		let window_keys =
			all_window_counter_keys(OptimizerEvidenceWindowId(3), OptimizerEvidenceWindowId(2));

		assert_eq!(verdict_keys.len(), 80);
		assert_eq!(decision_keys.len(), 140);
		assert_eq!(verdict_keys.len() + decision_keys.len(), 220);
		assert_eq!(all_probe_linearity_counter_keys().len(), 14);
		assert_eq!(window_keys.len(), 468);
		assert!(
			verdict_keys
				.iter()
				.all(|(_, key)| !key.starts_with(OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX.as_bytes()))
		);
		assert!(
			decision_keys
				.iter()
				.all(|(_, key)| !key.starts_with(OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX.as_bytes()))
		);
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
	fn receipt_gate_snapshot_reads_verdicts_decisions_and_health() {
		let evidence = OptimizerRuntimeEvidence::local_only();
		evidence.record_verdict(
			OptimizerDecisionKind::GalaxyRoot,
			OptimizerRuntimeVerdictClass::MissedWin,
			OptimizerRuntimeVerdictProvenance::Native,
		);
		evidence.record_decision(
			OptimizerDecisionKind::FlatStar,
			OptimizerDecisionOutcome::KeptThinMargin,
			OptimizerRuntimeVerdictProvenance::Native,
		);

		let snapshot = evidence.receipt_gate_snapshot();

		assert_eq!(
			snapshot.verdicts,
			vec![OptimizerRuntimeVerdictRow {
				kind: OptimizerDecisionKind::GalaxyRoot,
				verdict: OptimizerRuntimeVerdictClass::MissedWin,
				provenance: OptimizerRuntimeVerdictProvenance::Native,
				count: 1,
			}]
		);
		assert_eq!(
			snapshot.decisions,
			vec![OptimizerDecisionRow {
				kind: OptimizerDecisionKind::FlatStar,
				outcome: OptimizerDecisionOutcome::KeptThinMargin,
				provenance: OptimizerRuntimeVerdictProvenance::Native,
				count: 1,
			}]
		);
		assert_eq!(snapshot.recent_decisions, snapshot.decisions);
		assert_eq!(snapshot.health.pending_deltas, 4);
		assert_eq!(snapshot.window.lifetime_count, 2);
		assert_eq!(snapshot.window.recent_count, 2);
		assert_eq!(snapshot.window.stale_lifetime_count, 0);
	}

	#[test]
	fn receipt_gate_snapshot_reads_windows_under_same_lock() {
		let evidence = OptimizerRuntimeEvidence::local_only_with_window_id(10);
		evidence.record_verdict(
			OptimizerDecisionKind::GalaxyRoot,
			OptimizerRuntimeVerdictClass::MissedWin,
			OptimizerRuntimeVerdictProvenance::Native,
		);
		evidence.record_decision(
			OptimizerDecisionKind::FlatStar,
			OptimizerDecisionOutcome::Reordered,
			OptimizerRuntimeVerdictProvenance::Native,
		);

		let snapshot = evidence.receipt_gate_snapshot();

		assert_eq!(
			snapshot.window.current_window,
			OptimizerEvidenceWindowId(10)
		);
		assert_eq!(
			snapshot.window.previous_window,
			OptimizerEvidenceWindowId(9)
		);
		assert_eq!(snapshot.window.lifetime_count, 2);
		assert_eq!(snapshot.window.recent_count, 2);
		assert_eq!(snapshot.window.stale_lifetime_count, 0);
		assert_eq!(
			snapshot.recent_decisions,
			vec![OptimizerDecisionRow {
				kind: OptimizerDecisionKind::FlatStar,
				outcome: OptimizerDecisionOutcome::Reordered,
				provenance: OptimizerRuntimeVerdictProvenance::Native,
				count: 1,
			}]
		);
	}

	#[test]
	fn window_read_is_bounded_current_previous_manifest() {
		let evidence = OptimizerRuntimeEvidence::local_only_with_window_id(10);
		evidence.record_verdict(
			OptimizerDecisionKind::GalaxyRoot,
			OptimizerRuntimeVerdictClass::MissedWin,
			OptimizerRuntimeVerdictProvenance::Native,
		);
		evidence.record_decision(
			OptimizerDecisionKind::FlatStar,
			OptimizerDecisionOutcome::Reordered,
			OptimizerRuntimeVerdictProvenance::Native,
		);
		evidence.set_window_id_for_test(12);

		let snapshot = evidence.receipt_gate_snapshot();

		assert_eq!(
			snapshot.window.current_window,
			OptimizerEvidenceWindowId(12)
		);
		assert_eq!(
			snapshot.window.previous_window,
			OptimizerEvidenceWindowId(11)
		);
		assert_eq!(snapshot.window.lifetime_count, 2);
		assert_eq!(snapshot.window.recent_count, 0);
		assert_eq!(snapshot.window.stale_lifetime_count, 2);
		assert!(snapshot.recent_decisions.is_empty());
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
