//! ADR 0699 S3a: proposing parent decisions to the Raft leader.
//!
//! A data node never proposes a decision directly unless it is the leader. It
//! sends a [`ForwardedDecision`], which has no field for a time stamp, a floor
//! or another node's identity. The leader admits it for the node authenticated
//! on the connection, stamps time-bearing commands with its own clock after a
//! health check, and proposes. The local leader takes the same path.
//!
//! The only producers of `WendelonRequest::Decision` outside tests are
//! [`admit`] and [`floor_proposal`] (pinned by an architecture test).

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::consensus::decision_mode::DecisionMode;
use crate::consensus::decisions::{
	AbortBody, ChildAckBody, Claim, DECISION_EXPIRY_MS, DecisionCommand, DecisionResult, ParentKey,
	Signed, Signer,
};
use crate::consensus::monitor::{MAX_OFFSET_MS, PhysicalClock};
use crate::consensus::raft_storage::{SmState, WendelonStateMachine};
use crate::consensus::{
	RegisterIncarnation, StoreIncarnation, WendelonRaft, WendelonRequest, WendelonResponse,
};
use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::schema::{FrameHeader, MsgType, NodeId};

/// The largest forwarded proposal or response. A Finalize carries at most
/// `MAX_CHILDREN × MAX_PLAN_MEMBERS` acknowledgements of a few hundred bytes.
pub const MAX_DECISION_FRAME_BYTES: usize = 1 << 20;

/// What a data node may ask the leader to propose. No variant carries a stamp,
/// a floor or a key: the leader supplies those.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardedDecision {
	Reserve {
		claim: Signed<Claim>,
	},
	Finalize {
		key: ParentKey,
		request_digest: [u8; 32],
		acks: Vec<Signed<ChildAckBody>>,
	},
	Resolve {
		key: ParentKey,
		request_digest: [u8; 32],
	},
	Abort {
		abort: Signed<AbortBody>,
	},
	/// Bind the sender's store incarnation to the sender's configured key.
	BindSigner {
		incarnation: StoreIncarnation,
	},
	/// First registration of the sender's store incarnation, or its exact
	/// retry. Replacing an established incarnation is not expressible: this
	/// checkpoint has no replacement, copy or re-entry.
	RegisterIncarnation {
		incarnation: StoreIncarnation,
	},
	/// ADR 0699 S4b: a delete that published nothing, signed by its coordinator.
	RecordNoop {
		noop: Signed<crate::consensus::decisions::NoopBody>,
	},
	/// ADR 0699 S5a: a member's report of its staged base inventory.
	Report {
		report: Signed<crate::consensus::decisions::ReportBody>,
	},
}

/// ADR 0700: a time-bearing command before the leader stamps it. It becomes a
/// Raft command only through [`Unstamped::stamp`], whose input is the leader's
/// gated decision clock, so no time-bearing command can be proposed unstamped.
#[derive(Debug)]
pub(crate) enum Unstamped {
	Reserve {
		claim: Signed<Claim>,
	},
	RecordNoop {
		noop: Signed<crate::consensus::decisions::NoopBody>,
	},
}

impl Unstamped {
	pub(crate) fn stamp(self, stamp_ms: u64) -> WendelonRequest {
		WendelonRequest::Decision(match self {
			Self::Reserve { claim } => DecisionCommand::Reserve { claim, stamp_ms },
			Self::RecordNoop { noop } => DecisionCommand::RecordNoop { noop, stamp_ms },
		})
	}
}

/// What the leader admits from a forwarded request: a command, or one that
/// still needs the gated clock.
#[derive(Debug)]
pub(crate) enum Admitted {
	Ready(WendelonRequest),
	Unstamped(Unstamped),
}

/// A forwarded proposal on the wire, with the sender's clock reading. The
/// reading is a drift sample for the leader's health check, never a decision
/// stamp: the leader's monitor otherwise hears follower clocks only by chance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposeRequest {
	pub sent_ms: u64,
	pub request: ForwardedDecision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalRefusal {
	/// The leader runs today's authority.
	ModeToday,
	/// The leader's clock is not known to be within the drift bound.
	ClockUnhealthy,
	/// The request names a node other than the authenticated sender.
	NotTheSender,
	/// The sender has no configured decision key.
	UnknownSignerKey,
	/// The frame is oversized or does not decode.
	Malformed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalOutcome {
	Decision(DecisionResult),
	/// Whether the incarnation registration was applied.
	Incarnation {
		accepted: bool,
	},
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalResponse {
	/// Committed and applied at `log_index`.
	Applied {
		outcome: ProposalOutcome,
		log_index: u64,
	},
	NotLeader {
		leader_hint: Option<u32>,
	},
	Refused(ProposalRefusal),
	/// The proposal may or may not have committed; ask again.
	Indeterminate(String),
}

/// A node's lookup of a parent's recorded decision, by key and request digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupRequest {
	pub key: ParentKey,
	pub request_digest: [u8; 32],
	/// The sender's clock reading, a drift sample only.
	pub sent_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LookupResponse {
	/// Read from the leader's applied state after a linearizable read barrier.
	Found(crate::consensus::decisions::Lookup),
	NotLeader {
		leader_hint: Option<u32>,
	},
	Refused(ProposalRefusal),
	/// The barrier did not complete; ask again.
	Indeterminate(String),
}

/// ADR 0699 S4: a reader's request for a linearizable committed cut.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CutRequest {
	/// The sender's clock reading, a drift sample only.
	pub sent_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CutResponse {
	/// The leader's read index after a linearizable read barrier: every
	/// decision committed before the request was sent is at or under it.
	Cut(u64),
	NotLeader {
		leader_hint: Option<u32>,
	},
	Refused(ProposalRefusal),
	/// The barrier did not complete; ask again.
	Indeterminate(String),
}

/// The leader's clock reading, refused unless the drift monitor's own test
/// holds right now: a cluster median exists and the offset is within bound.
/// ADR 0700: how recent a peer's clock sample must be, by the monotonic
/// clock, to count for the leader's decision clock. A follower that has sent
/// the current leader nothing for [`CLOCK_PROBE_INTERVAL`] probes it, so this
/// spans several missed probes and ordinary scheduling delay.
pub(crate) const CLOCK_SAMPLE_FRESHNESS: std::time::Duration = std::time::Duration::from_secs(5);
/// How long a follower may stay silent toward the current leader before its
/// maintenance sends a clock probe.
pub(crate) const CLOCK_PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
/// The bound on one clock probe (a CUT round trip, which runs a linearizable
/// barrier at the leader).
pub(crate) const CLOCK_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// ADR 0700: the authenticated decision requests' clock readings, one per
/// peer, each with its monotonic receipt. Only [`DecisionProposals::observe_sender_clock`]
/// writes it, so no other path can refresh a sample.
#[derive(Default)]
pub(crate) struct DecisionClockSamples {
	samples: std::sync::Mutex<BTreeMap<u32, (u64, std::time::Instant)>>,
}

impl DecisionClockSamples {
	fn record(&self, peer: u32, remote_ms: u64) {
		self.samples
			.lock()
			.unwrap()
			.insert(peer, (remote_ms, std::time::Instant::now()));
	}

	/// Every sample with its monotonic age.
	fn aged(&self) -> Vec<(u32, u64, std::time::Duration)> {
		let now = std::time::Instant::now();
		self.samples
			.lock()
			.unwrap()
			.iter()
			.map(|(peer, (remote, received))| {
				(*peer, *remote, now.saturating_duration_since(*received))
			})
			.collect()
	}
}

/// The voters of the control configuration, or `None` for a joint
/// configuration, which this checkpoint does not support.
pub(crate) fn current_voters(state: &SmState) -> Option<std::collections::BTreeSet<u32>> {
	let membership = state.last_membership.membership();
	if membership.get_joint_config().len() != 1 {
		return None;
	}
	Some(membership.voter_ids().collect())
}

/// ADR 0700: the leader's decision clock gate. With `n` current voters (the
/// leader one of them) it needs fresh samples from `floor(n/2)` other voters,
/// so the leader and they form a majority; the median of ALL fresh voter
/// samples, each projected by its monotonic age, must be within
/// [`MAX_OFFSET_MS`] of the leader's clock. Self, non-voters and stale samples
/// never count, and a wall-clock jump cannot rejuvenate a sample. A single
/// voter has no peer median: its own clock is used unchecked. This is evidence
/// of recent peer observations, not proof a peer is alive or that any clock is
/// objectively right; Raft quorum and leadership checks stay mandatory.
pub(crate) fn clock_gate(
	now_ms: u64,
	local: u32,
	voters: Option<&std::collections::BTreeSet<u32>>,
	samples: &[(u32, u64, std::time::Duration)],
) -> Result<u64, ProposalRefusal> {
	let voters = voters.ok_or(ProposalRefusal::ClockUnhealthy)?;
	if !voters.contains(&local) {
		return Err(ProposalRefusal::ClockUnhealthy);
	}
	let needed = voters.len() / 2;
	let mut seen = std::collections::BTreeSet::new();
	let mut projected: Vec<u64> = samples
		.iter()
		.filter(|(peer, _, age)| {
			*peer != local
				&& voters.contains(peer)
				&& *age <= CLOCK_SAMPLE_FRESHNESS
				&& seen.insert(*peer)
		})
		.map(|(_, remote, age)| remote.saturating_add(age.as_millis() as u64))
		.collect();
	if needed == 0 {
		return Ok(now_ms);
	}
	if projected.len() < needed {
		return Err(ProposalRefusal::ClockUnhealthy);
	}
	projected.sort_unstable();
	let n = projected.len();
	let median = if n % 2 == 1 {
		projected[n / 2]
	} else {
		let (a, b) = (projected[n / 2 - 1], projected[n / 2]);
		a / 2 + b / 2 + ((a & 1) + (b & 1)) / 2
	};
	if now_ms.abs_diff(median) > MAX_OFFSET_MS {
		return Err(ProposalRefusal::ClockUnhealthy);
	}
	Ok(now_ms)
}

/// Turns a forwarded request from `sender` into the command the leader
/// proposes. Pure: every check that does not need the replicated state, which
/// apply then enforces itself.
/// [`admission`], stamped from `clock_now` when it needs time.
#[cfg(test)]
pub(crate) fn admit(
	mode: DecisionMode,
	sender: u32,
	request: ForwardedDecision,
	trusted_keys: &BTreeMap<u32, [u8; 32]>,
	clock_now: impl FnOnce() -> Result<u64, ProposalRefusal>,
) -> Result<WendelonRequest, ProposalRefusal> {
	match admission(mode, sender, request, trusted_keys)? {
		Admitted::Ready(command) => Ok(command),
		Admitted::Unstamped(unstamped) => Ok(unstamped.stamp(clock_now()?)),
	}
}

pub(crate) fn admission(
	mode: DecisionMode,
	sender: u32,
	request: ForwardedDecision,
	trusted_keys: &BTreeMap<u32, [u8; 32]>,
) -> Result<Admitted, ProposalRefusal> {
	if mode != DecisionMode::ParentDecisions {
		return Err(ProposalRefusal::ModeToday);
	}
	let command = match request {
		ForwardedDecision::Reserve { claim } => {
			let coordinator = claim
				.body
				.children
				.get(usize::from(claim.body.coordinator))
				.map(|child| child.holder.node);
			if coordinator != Some(sender) {
				return Err(ProposalRefusal::NotTheSender);
			}
			return Ok(Admitted::Unstamped(Unstamped::Reserve { claim }));
		}
		ForwardedDecision::Finalize {
			key,
			request_digest,
			acks,
		} => DecisionCommand::Finalize {
			key,
			request_digest,
			acks,
		},
		ForwardedDecision::Resolve {
			key,
			request_digest,
		} => DecisionCommand::Resolve {
			key,
			request_digest,
		},
		ForwardedDecision::Abort { abort } => DecisionCommand::Abort { abort },
		// Not time-bearing: a report is judged by its generation and membership.
		ForwardedDecision::Report { report } => {
			if report.body.reporter.node != sender {
				return Err(ProposalRefusal::NotTheSender);
			}
			DecisionCommand::Report { report }
		}
		ForwardedDecision::RecordNoop { noop } => {
			if noop.body.coordinator.node != sender {
				return Err(ProposalRefusal::NotTheSender);
			}
			return Ok(Admitted::Unstamped(Unstamped::RecordNoop { noop }));
		}
		ForwardedDecision::BindSigner { incarnation } => {
			let key = *trusted_keys
				.get(&sender)
				.ok_or(ProposalRefusal::UnknownSignerKey)?;
			DecisionCommand::BindSigner {
				signer: Signer {
					node: sender,
					incarnation,
				},
				key,
			}
		}
		ForwardedDecision::RegisterIncarnation { incarnation } => {
			return Ok(Admitted::Ready(WendelonRequest::RegisterIncarnation(
				RegisterIncarnation {
					node: sender,
					incarnation,
					supersedes: None,
				},
			)));
		}
	};
	Ok(Admitted::Ready(WendelonRequest::Decision(command)))
}

/// The leader's floor advance, if its healthy clock allows a higher floor.
pub(crate) fn floor_proposal(state: &SmState, now_ms: u64) -> Option<WendelonRequest> {
	let decisions = state.decisions.as_ref()?;
	let floor_ms = now_ms.checked_sub(DECISION_EXPIRY_MS)?;
	(floor_ms > decisions.floor_ms()).then_some(WendelonRequest::Decision(
		DecisionCommand::AdvanceFloor {
			floor_ms,
			stamp_ms: now_ms,
		},
	))
}

fn outcome_of(response: WendelonResponse) -> Result<ProposalOutcome, String> {
	match response {
		WendelonResponse::Decision(result) => Ok(ProposalOutcome::Decision(result)),
		WendelonResponse::Success => Ok(ProposalOutcome::Incarnation { accepted: true }),
		WendelonResponse::Refused(_) => Ok(ProposalOutcome::Incarnation { accepted: false }),
		other => Err(format!(
			"unexpected response to a decision proposal: {other:?}"
		)),
	}
}

/// One node's view of the proposal path: the leader side for forwarded
/// requests, the sender side for its own, and the leader's floor task.
pub struct DecisionProposals {
	mode: DecisionMode,
	local: NodeId,
	raft: WendelonRaft,
	state_machine: WendelonStateMachine,
	trusted_keys: Arc<BTreeMap<u32, [u8; 32]>>,
	clock: Arc<dyn PhysicalClock>,
	factory: Arc<TransportProviderFactory>,
	pool: Arc<BufPool>,
	/// ADR 0700: peer clock samples for the decision clock gate.
	clock_samples: DecisionClockSamples,
	/// ADR 0700: the leader this node last sent a request to, and when.
	last_sent: std::sync::Mutex<Option<(u32, std::time::Instant)>>,
}

/// ADR 0700: the one clock probe a maintenance loop owns; its presence is the
/// in-flight guard. [`ClockProbe::stop`] cancels and awaits it, and dropping
/// the owner (its loop aborted) cancels it, so a stopped node holds no probe
/// and a cancelled probe never leaves the guard set.
#[derive(Default)]
pub(crate) struct ClockProbe(Option<tokio::task::JoinHandle<()>>);

impl ClockProbe {
	/// Whether a probe is still running; a finished one is released.
	fn busy(&mut self) -> bool {
		if self.0.as_ref().is_some_and(|probe| probe.is_finished()) {
			self.0 = None;
		}
		self.0.is_some()
	}

	fn start(&mut self, probe: impl std::future::Future<Output = ()> + Send + 'static) {
		debug_assert!(!self.busy());
		self.0 = Some(tokio::spawn(probe));
	}

	/// Cancels the probe, if any, and waits for it to end.
	pub(crate) async fn stop(&mut self) {
		if let Some(probe) = self.0.take() {
			probe.abort();
			let _ = probe.await;
		}
	}
}

impl Drop for ClockProbe {
	fn drop(&mut self) {
		if let Some(probe) = &self.0 {
			probe.abort();
		}
	}
}

impl DecisionProposals {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		mode: DecisionMode,
		local: NodeId,
		raft: WendelonRaft,
		state_machine: WendelonStateMachine,
		trusted_keys: BTreeMap<u32, [u8; 32]>,
		clock: Arc<dyn PhysicalClock>,
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
	) -> Self {
		Self {
			mode,
			local,
			raft,
			state_machine,
			trusted_keys: Arc::new(trusted_keys),
			clock,
			factory,
			pool,
			clock_samples: DecisionClockSamples::default(),
			last_sent: std::sync::Mutex::new(None),
		}
	}

	/// ADR 0700: the decision clock now, through the gate.
	async fn decision_clock_now(&self) -> Result<u64, ProposalRefusal> {
		let voters = self.state_machine.read_state(current_voters).await;
		clock_gate(
			self.clock.now_ms(),
			self.local.id,
			voters.as_ref(),
			&self.clock_samples.aged(),
		)
	}

	/// ADR 0700: a follower that has sent the current leader nothing for
	/// [`CLOCK_PROBE_INTERVAL`] sends it one bounded CUT, whose `sent_ms` the
	/// leader records. At most one is in flight, owned by `probe`; it never
	/// blocks the caller, and it runs whatever this node's readiness or the
	/// clock gate says.
	pub(crate) async fn clock_probe_step(self: &Arc<Self>, probe: &mut ClockProbe) {
		if probe.busy() {
			return;
		}
		if self.mode != DecisionMode::ParentDecisions {
			return;
		}
		let Some(leader) = self.raft.current_leader().await else {
			return;
		};
		if leader == self.local.id {
			return;
		}
		if let Some((to, at)) = *self.last_sent.lock().unwrap()
			&& to == leader
			&& at.elapsed() < CLOCK_PROBE_INTERVAL
		{
			return;
		}
		let this = self.clone();
		probe.start(async move {
			let _ = tokio::time::timeout(CLOCK_PROBE_TIMEOUT, this.cut()).await;
		});
	}

	/// Leader side: admit and propose a request authenticated as `sender`.
	pub(crate) async fn propose_as(
		&self,
		sender: u32,
		request: ForwardedDecision,
	) -> ProposalResponse {
		let leader = self.raft.current_leader().await;
		if leader != Some(self.local.id) {
			return ProposalResponse::NotLeader {
				leader_hint: leader,
			};
		}
		let command = match admission(self.mode, sender, request, &self.trusted_keys) {
			Ok(Admitted::Ready(command)) => command,
			// Only the gated decision clock stamps a time-bearing command.
			Ok(Admitted::Unstamped(unstamped)) => match self.decision_clock_now().await {
				Ok(stamp_ms) => unstamped.stamp(stamp_ms),
				Err(refusal) => return ProposalResponse::Refused(refusal),
			},
			Err(refusal) => return ProposalResponse::Refused(refusal),
		};
		self.commit(command).await
	}

	async fn commit(&self, command: WendelonRequest) -> ProposalResponse {
		match self.raft.client_write(command).await {
			Ok(response) => match outcome_of(response.data) {
				Ok(outcome) => ProposalResponse::Applied {
					outcome,
					log_index: response.log_id.index,
				},
				Err(message) => ProposalResponse::Indeterminate(message),
			},
			Err(error) => match self.raft.current_leader().await {
				leader if leader != Some(self.local.id) => ProposalResponse::NotLeader {
					leader_hint: leader,
				},
				_ => ProposalResponse::Indeterminate(error.to_string()),
			},
		}
	}

	/// Sender side: propose locally when leader, otherwise forward.
	pub(crate) async fn propose(&self, request: ForwardedDecision) -> ProposalResponse {
		match self.raft.current_leader().await {
			Some(leader) if leader == self.local.id => {
				self.propose_as(self.local.id, request).await
			}
			Some(leader) => self
				.forward(leader, &request)
				.await
				.unwrap_or_else(|error| ProposalResponse::Indeterminate(error.to_string())),
			None => ProposalResponse::NotLeader { leader_hint: None },
		}
	}

	async fn forward(
		&self,
		leader: u32,
		request: &ForwardedDecision,
	) -> std::io::Result<ProposalResponse> {
		self.round_trip(
			leader,
			MsgType::DECISION_PROPOSE_REQ,
			MsgType::DECISION_PROPOSE_RESP,
			&ProposeRequest {
				sent_ms: self.clock.now_ms(),
				request: request.clone(),
			},
		)
		.await
	}

	/// Records a forwarding node's clock reading for the leader's drift test.
	/// ADR 0700: the samples feed only the decision clock gate, which refuses
	/// a decision rather than aborting the process. They no longer feed the
	/// drift monitor's tracker: a request's transit and queueing delay under
	/// load made them look like drift and aborted a healthy node.
	pub(crate) fn observe_sender_clock(&self, sender: u32, sent_ms: u64) {
		if sender != self.local.id {
			self.clock_samples.record(sender, sent_ms);
		}
	}

	/// The local clock reading a sender attaches.
	pub(crate) fn now_ms(&self) -> u64 {
		self.clock.now_ms()
	}

	/// Leader side of a lookup: a linearizable read barrier, then the applied
	/// state. No clock and no current-authority check come before it.
	pub(crate) async fn lookup_as_leader(&self, request: LookupRequest) -> LookupResponse {
		if self.mode != DecisionMode::ParentDecisions {
			return LookupResponse::Refused(ProposalRefusal::ModeToday);
		}
		let leader = self.raft.current_leader().await;
		if leader != Some(self.local.id) {
			return LookupResponse::NotLeader {
				leader_hint: leader,
			};
		}
		if let Err(error) = self.raft.ensure_linearizable().await {
			return LookupResponse::Indeterminate(error.to_string());
		}
		let found = self
			.state_machine
			.read_state(move |state| {
				state.decisions.as_ref().map(|decisions| {
					crate::consensus::decisions::lookup_parent(
						decisions,
						&request.key,
						&request.request_digest,
					)
				})
			})
			.await;
		match found {
			Some(found) => LookupResponse::Found(found),
			None => LookupResponse::Refused(ProposalRefusal::ModeToday),
		}
	}

	/// Sender side: look up locally when leader, otherwise ask the leader.
	pub(crate) async fn lookup(&self, request: LookupRequest) -> LookupResponse {
		match self.raft.current_leader().await {
			Some(leader) if leader == self.local.id => self.lookup_as_leader(request).await,
			Some(leader) => self
				.round_trip(
					leader,
					MsgType::DECISION_LOOKUP_REQ,
					MsgType::DECISION_LOOKUP_RESP,
					&request,
				)
				.await
				.unwrap_or_else(|error| LookupResponse::Indeterminate(error.to_string())),
			None => LookupResponse::NotLeader { leader_hint: None },
		}
	}

	/// Leader side of a cut: a linearizable read barrier, then its read index.
	pub(crate) async fn cut_as_leader(&self) -> CutResponse {
		if self.mode != DecisionMode::ParentDecisions {
			return CutResponse::Refused(ProposalRefusal::ModeToday);
		}
		let leader = self.raft.current_leader().await;
		if leader != Some(self.local.id) {
			return CutResponse::NotLeader {
				leader_hint: leader,
			};
		}
		match self.raft.ensure_linearizable().await {
			Ok(read) => CutResponse::Cut(read.map_or(0, |log_id| log_id.index)),
			Err(error) => CutResponse::Indeterminate(error.to_string()),
		}
	}

	/// Reader side: the cut from the leader, locally when leader.
	pub(crate) async fn cut(&self) -> CutResponse {
		match self.raft.current_leader().await {
			Some(leader) if leader == self.local.id => self.cut_as_leader().await,
			Some(leader) => self
				.round_trip(
					leader,
					MsgType::DECISION_CUT_REQ,
					MsgType::DECISION_CUT_RESP,
					&CutRequest {
						sent_ms: self.now_ms(),
					},
				)
				.await
				.unwrap_or_else(|error| CutResponse::Indeterminate(error.to_string())),
			None => CutResponse::NotLeader { leader_hint: None },
		}
	}

	/// Waits, bounded, until this node has applied `index`.
	pub(crate) async fn wait_applied(
		&self,
		index: u64,
		bound: std::time::Duration,
	) -> std::io::Result<()> {
		if index == 0 {
			return Ok(());
		}
		self.raft
			.wait(Some(bound))
			.applied_index_at_least(Some(index), "a read cut")
			.await
			.map(|_| ())
			.map_err(|error| Error::new(ErrorKind::TimedOut, error.to_string()))
	}

	/// One bounded request and response with the leader.
	async fn round_trip<Q: Serialize, R: serde::de::DeserializeOwned>(
		&self,
		leader: u32,
		request_type: MsgType,
		response_type: MsgType,
		request: &Q,
	) -> std::io::Result<R> {
		let meta = postcard::to_stdvec(request).map_err(Error::other)?;
		*self.last_sent.lock().unwrap() = Some((leader, std::time::Instant::now()));
		if meta.len() > MAX_DECISION_FRAME_BYTES {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"decision frame exceeds its bound",
			));
		}
		let conn = self
			.factory
			.get_connection(NodeId {
				id: leader,
				_reserved: 0,
			})
			.await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: request_type.0,
			flags: 0,
			stream_id: stream.stream_id() as u32,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, &meta, None, None).await?;
		stream.finish().await?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		if frame.header.msg_type != response_type.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"unexpected decision response 0x{:02x}",
					frame.header.msg_type
				),
			));
		}
		decode_bounded(frame.meta.as_ref())
	}

	/// ADR 0699 S5a, leader only: commits the pending generation once every
	/// member reported, or proposes the next target. The reducer decides.
	pub(crate) async fn generation_step(&self) -> Option<ProposalResponse> {
		if self.mode != DecisionMode::ParentDecisions
			|| self.raft.current_leader().await != Some(self.local.id)
		{
			return None;
		}
		let command = self
			.state_machine
			.read_state(|state| {
				if let Some(gid) = crate::consensus::decisions::generations::ready_commit(state) {
					return Some(DecisionCommand::CommitGeneration { gid });
				}
				let applied = state.last_applied_log_id.map_or(0, |id| id.index);
				crate::consensus::decisions::generations::next_target(state, applied)
					.map(|target| DecisionCommand::ProposeTarget { target })
			})
			.await?;
		Some(self.commit(WendelonRequest::Decision(command)).await)
	}

	/// Leader-only floor task step. Proposes nothing unless leader with a
	/// healthy clock and a higher floor to set.
	pub(crate) async fn advance_floor_once(&self) -> Option<ProposalResponse> {
		if self.mode != DecisionMode::ParentDecisions
			|| self.raft.current_leader().await != Some(self.local.id)
		{
			return None;
		}
		let now = self.decision_clock_now().await.ok()?;
		let command = self
			.state_machine
			.read_state(|state| floor_proposal(state, now))
			.await?;
		Some(self.commit(command).await)
	}

	/// Proposes Resolve for the reserved parents this node should drive: a bound
	/// lineage closed, and this node now holds the coordinator's range. Bounded
	/// per call; the reducer decides.
	pub(crate) async fn resolve_closed_once(&self, limit: usize) -> usize {
		if self.mode != DecisionMode::ParentDecisions {
			return 0;
		}
		let local = self.local.id;
		let candidates = self
			.state_machine
			.read_state(move |state| {
				crate::consensus::decisions::resolvable_by(state, local, limit)
			})
			.await;
		let mut proposed = 0;
		for (key, request_digest) in candidates {
			if matches!(
				self.propose(ForwardedDecision::Resolve {
					key,
					request_digest
				})
				.await,
				ProposalResponse::Applied { .. }
			) {
				proposed += 1;
			}
		}
		proposed
	}

	/// Registers this store's incarnation, then binds its decision key, each
	/// only when the applied state lacks it. One step per call. A different
	/// established incarnation for this node is left alone: this store is not
	/// that node's registered store, and nothing here may retire it.
	pub(crate) async fn ensure_signer_once(
		&self,
		incarnation: StoreIncarnation,
	) -> Option<ProposalResponse> {
		if self.mode != DecisionMode::ParentDecisions {
			return None;
		}
		let local = self.local.id;
		let (current, bound) = self
			.state_machine
			.read_state(|state| {
				let current = state
					.incarnations
					.get(&local)
					.and_then(|registry| registry.current);
				let bound = state.decisions.as_ref().is_some_and(|decisions| {
					decisions
						.signer_key(Signer {
							node: local,
							incarnation,
						})
						.is_some()
				});
				(current, bound)
			})
			.await;
		let request = maintenance_step(current, bound, incarnation)?;
		Some(self.propose(request).await)
	}
}

/// The maintenance decision for this store, from the applied registry: register
/// a first incarnation, then bind it. A different established incarnation for
/// this node is left alone.
pub(crate) fn maintenance_step(
	current: Option<StoreIncarnation>,
	bound: bool,
	incarnation: StoreIncarnation,
) -> Option<ForwardedDecision> {
	match current {
		None => Some(ForwardedDecision::RegisterIncarnation { incarnation }),
		Some(current) if current != incarnation => None,
		Some(_) if !bound => Some(ForwardedDecision::BindSigner { incarnation }),
		Some(_) => None,
	}
}

pub(crate) fn decode_bounded<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> std::io::Result<T> {
	if bytes.len() > MAX_DECISION_FRAME_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"decision proposal frame exceeds its bound",
		));
	}
	postcard::from_bytes(bytes).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

#[cfg(test)]
#[path = "decision_proposals_tests.rs"]
mod tests;
