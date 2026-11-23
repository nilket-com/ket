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
use crate::consensus::owner_ops::{OwnerOpAnswer, OwnerOpChannel, OwnerOpOutcome};
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
	/// ADR 0721: `key` is the verifying key of the secret the sender signs
	/// with. The leader binds its own trust-map entry for the sender, and only
	/// if the two are equal: `key` never replaces that lookup.
	BindSigner {
		incarnation: StoreIncarnation,
		key: [u8; 32],
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
	/// ADR 0725: one slab operation signed by the sender as its owner. The
	/// leader checks only that the owner's node is the sender; the state machine
	/// verifies the signature and decides.
	OwnerOp(crate::consensus::owner_ops::SignedOwnerOp),
	/// ADR 0729: a replica asks for a lapsed holder's range, for itself.
	TakeOverLineage {
		takeover: Signed<crate::consensus::decisions::TakeOverBody>,
	},
	/// ADR 0106 (wendelon): a promoted replacement's signed recovery statement,
	/// for the sender's own slot.
	RecoveryProved {
		proof: Signed<crate::consensus::decisions::RecoveryBody>,
	},
	/// ADR 0108 (wendelon): a recovered replacement's signed activation, for
	/// the sender's own slot.
	ActivateReplacement {
		proof: Signed<crate::consensus::decisions::ActivationBody>,
	},
	/// ADR 0108 (wendelon): a store's secondary-index receipt, for the sender's
	/// own slot.
	IndexReceipt {
		proof: Signed<crate::consensus::decisions::IndexReceiptBody>,
	},
	/// ADR 0105 (wendelon): a Done replacement admits its new store as the
	/// sender's current incarnation. The node is always the sender; the state
	/// machine judges the token, instance, birth and registry.
	PromoteIncarnation {
		token: u128,
		r_new: u32,
		birth: crate::consensus::decisions::instances::Birth,
		expected: StoreIncarnation,
		incarnation: StoreIncarnation,
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
	/// ADR 0729.
	TakeOverLineage {
		takeover: Signed<crate::consensus::decisions::TakeOverBody>,
	},
	/// Wendelon ADR 0120: a Begin, a Renew and a Recover take the gated stamp.
	CtasBegin {
		begin: CtasBeginRequest,
	},
	CtasRenew {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		epoch: u64,
		owner: [u8; 32],
	},
	CtasRecover {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		new_epoch: u64,
	},
	/// Wendelon ADR 0120 D9: the terminal commands take the gated stamp too: the
	/// terminal time is the committed clock with it.
	CtasPublish {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		epoch: u64,
		owner: [u8; 32],
	},
	CtasAbandon {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		epoch: u64,
		owner: [u8; 32],
		cause: crate::consensus::decisions::ctas::CtasAbandonCause,
	},
	CtasConclude {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		epoch: u64,
	},
}

impl Unstamped {
	pub(crate) fn stamp(self, stamp_ms: u64) -> WendelonRequest {
		WendelonRequest::Decision(match self {
			Self::Reserve { claim } => DecisionCommand::Reserve { claim, stamp_ms },
			Self::RecordNoop { noop } => DecisionCommand::RecordNoop { noop, stamp_ms },
			Self::TakeOverLineage { takeover } => {
				DecisionCommand::TakeOverLineage { takeover, stamp_ms }
			}
			Self::CtasBegin { begin } => DecisionCommand::CtasBegin {
				begin: crate::consensus::decisions::ctas::CtasBegin {
					tenant: begin.tenant,
					op: begin.op,
					fingerprint: begin.fingerprint,
					descriptor: begin.descriptor,
					declaration: begin.declaration,
					dispatch_hash: begin.dispatch_hash,
					stamp_ms,
				},
			},
			Self::CtasRenew {
				tenant,
				op,
				epoch,
				owner,
			} => DecisionCommand::CtasRenew {
				tenant,
				op,
				epoch,
				owner,
				stamp_ms,
			},
			Self::CtasRecover {
				tenant,
				op,
				new_epoch,
			} => DecisionCommand::CtasRecover {
				tenant,
				op,
				new_epoch,
				stamp_ms,
			},
			Self::CtasPublish {
				tenant,
				op,
				epoch,
				owner,
			} => DecisionCommand::CtasPublish {
				tenant,
				op,
				epoch,
				owner,
				stamp_ms,
			},
			Self::CtasAbandon {
				tenant,
				op,
				epoch,
				owner,
				cause,
			} => DecisionCommand::CtasAbandon {
				tenant,
				op,
				epoch,
				owner,
				cause,
				stamp_ms,
			},
			Self::CtasConclude { tenant, op, epoch } => DecisionCommand::CtasConclude {
				tenant,
				op,
				epoch,
				stamp_ms,
			},
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
	/// ADR 0721: the key the sender signs with is not the one this leader is
	/// configured to trust for it. Nothing is bound, so correcting a
	/// configuration can still bind the right key.
	SignerKeyDisagrees,
	/// ADR 0104 (wendelon): the slot to replace is the leader's own; a lost
	/// voter is replaced from another member.
	ReplacesLeader,
	/// Wendelon ADR 0120 D6: the caller's secret is not the CTAS operation's
	/// owner's (or no chain exists). Appended.
	CtasNotOwner,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalOutcome {
	Decision(DecisionResult),
	/// Whether the incarnation registration was applied.
	Incarnation {
		accepted: bool,
	},
	/// ADR 0105 (wendelon): a promotion's answer, with its refusal kept so the
	/// replacement tells a wait from a definitive refusal.
	Promotion {
		refused: Option<crate::consensus::SlabRefusal>,
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
	/// Wendelon ADR 0120: a CTAS operation's chain at the leader's read index
	/// `log_index`, or none, with its object's publication index if published.
	CtasResolved {
		chain: Option<Box<crate::consensus::decisions::ctas::CtasChain>>,
		visible_from: Option<u64>,
		log_index: u64,
	},
	/// ADR 0725: an owner operation committed and applied at `log_index`, with
	/// the state machine's own answer.
	OwnerApplied {
		answer: OwnerOpAnswer,
		log_index: u64,
	},
	/// ADR 0725: an owner operation this node did not hand to the log: it
	/// does not lead, and found that out before proposing. Any error after the
	/// proposal was handed over is `Indeterminate` instead.
	OwnerNotSubmitted {
		leader_hint: Option<u32>,
	},
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
	/// ADR 0702: signalled on every recorded sample, for a waiting admission.
	arrived: tokio::sync::Notify,
}

impl DecisionClockSamples {
	fn record(&self, peer: u32, remote_ms: u64) {
		self.samples
			.lock()
			.unwrap()
			.insert(peer, (remote_ms, std::time::Instant::now()));
		self.arrived.notify_waiters();
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

/// ADR 0702: how long a time-bearing admission waits for the decision clock.
pub(crate) const CLOCK_ADMISSION_WAIT: std::time::Duration = CLOCK_PROBE_TIMEOUT;

/// The bound on one remote decision RPC attempt (a forwarded proposal, a
/// lookup or a cut), from before connecting until the answer is read. A design
/// value, not a measured latency guarantee:
/// - above [`CLOCK_ADMISSION_WAIT`] and [`CLOCK_PROBE_TIMEOUT`] (2 s), so an
///   answer delayed only by the leader's clock admission still arrives;
/// - below the 15 s lifecycle drain bound, so a follower's base-validation cut
///   cannot alone exhaust a drain.
///
/// It bounds one attempt only, never a whole write or a shutdown: an operation
/// makes several calls and has other waits. Expiry is an unknown outcome.
pub(crate) const DECISION_RPC_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// ADR 0702: what a waited admission decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClockWait {
	/// The gate passed and this node is still leader: stamp with this.
	Stamp(u64),
	Refused(ProposalRefusal),
	/// The gate passed but leadership moved meanwhile: nothing is proposed.
	NotLeader,
}

/// ADR 0702: the unchanged decision clock gate, re-evaluated on each recorded
/// sample until it passes. The whole evaluation, every gate evaluation and the
/// final leadership check included, is bounded by `deadline` (absolute, never
/// renewed): on expiry the gate's own `ClockUnhealthy` refusal stands and no
/// stamp is returned, and a stamp ready only at the deadline does not win. No
/// lock is held while waiting, and the arrival notification is armed before
/// each evaluation so none is missed. A refusal other than `ClockUnhealthy`
/// returns at once. This bounds admission, not the Raft commit that follows.
pub(crate) async fn wait_for_decision_clock<G, GateFuture, L, LeaderFuture>(
	arrived: &tokio::sync::Notify,
	deadline: tokio::time::Instant,
	mut gate: G,
	still_leader: L,
) -> ClockWait
where
	G: FnMut() -> GateFuture,
	GateFuture: std::future::Future<Output = Result<u64, ProposalRefusal>>,
	L: FnOnce() -> LeaderFuture,
	LeaderFuture: std::future::Future<Output = bool>,
{
	let mut still_leader = Some(still_leader);
	let admission = async {
		loop {
			let notified = arrived.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			match gate().await {
				Ok(stamp_ms) => {
					let check = still_leader.take().expect("the leadership check runs once");
					return if check().await {
						ClockWait::Stamp(stamp_ms)
					} else {
						ClockWait::NotLeader
					};
				}
				Err(ProposalRefusal::ClockUnhealthy) => notified.await,
				Err(refusal) => return ClockWait::Refused(refusal),
			}
		}
	};
	let unhealthy = ClockWait::Refused(ProposalRefusal::ClockUnhealthy);
	match tokio::time::timeout_at(deadline, admission).await {
		Ok(ClockWait::Stamp(_)) if tokio::time::Instant::now() >= deadline => unhealthy,
		Ok(decided) => decided,
		Err(_) => unhealthy,
	}
}

/// The voters of the control configuration, or `None` for a joint
/// configuration, which this checkpoint does not support.
pub(crate) fn current_voters(state: &SmState) -> Option<std::collections::BTreeSet<u32>> {
	let membership = state.last_membership.membership();
	if membership.get_joint_config().len() != 1 {
		return None;
	}
	// ADR 0104 (wendelon): clock samples are keyed by the sender's slot, so the
	// gate counts voters by slot; an unmapped instance makes no answer.
	membership
		.voter_ids()
		.map(|voter| crate::consensus::decisions::instances::slot_of(state, voter).ok())
		.collect()
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
		// ADR 0729: only the successor asks for itself; the stamp is the leader's.
		ForwardedDecision::TakeOverLineage { takeover } => {
			if takeover.body.successor.node != sender {
				return Err(ProposalRefusal::NotTheSender);
			}
			return Ok(Admitted::Unstamped(Unstamped::TakeOverLineage { takeover }));
		}
		ForwardedDecision::BindSigner {
			incarnation,
			key: senders,
		} => {
			let key = *trusted_keys
				.get(&sender)
				.ok_or(ProposalRefusal::UnknownSignerKey)?;
			if key != senders {
				return Err(ProposalRefusal::SignerKeyDisagrees);
			}
			DecisionCommand::BindSigner {
				signer: Signer {
					node: sender,
					incarnation,
				},
				key,
			}
		}
		// ADR 0725: the owner's node must be the sender. Nothing else is judged
		// here: the signature, the incarnation and the command are apply's.
		ForwardedDecision::OwnerOp(op) => {
			if op.claimed_owner().node() != sender {
				return Err(ProposalRefusal::NotTheSender);
			}
			return Ok(Admitted::Ready(WendelonRequest::OwnerOp(op)));
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
		ForwardedDecision::RecoveryProved { proof } => {
			if proof.body.slot != sender {
				return Err(ProposalRefusal::NotTheSender);
			}
			DecisionCommand::RecoveryProved { proof }
		}
		ForwardedDecision::ActivateReplacement { proof } => {
			if proof.body.slot != sender {
				return Err(ProposalRefusal::NotTheSender);
			}
			DecisionCommand::ActivateReplacement { proof }
		}
		ForwardedDecision::IndexReceipt { proof } => {
			if proof.body.signer.node != sender {
				return Err(ProposalRefusal::NotTheSender);
			}
			DecisionCommand::IndexReceipt { proof }
		}
		ForwardedDecision::PromoteIncarnation {
			token,
			r_new,
			birth,
			expected,
			incarnation,
		} => {
			return Ok(Admitted::Ready(WendelonRequest::PromoteIncarnation(
				crate::consensus::PromoteIncarnation {
					node: sender,
					token,
					r_new,
					birth,
					expected,
					incarnation,
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

/// ADR 0736: the only decision commands the administrative route may have the
/// leader propose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BackupHoldProposal {
	Begin { id: u128 },
	Release { id: u128, entered_at: u64 },
}

/// Wendelon ADR 0120 D6: a Begin before its stamp: the statement's chosen
/// declaration, object and owner commitment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasBeginRequest {
	pub tenant: crate::schema::TenantId,
	pub op: [u8; 16],
	pub fingerprint: [u8; 32],
	pub descriptor: crate::schema::TableSchemaDescriptor,
	pub declaration: crate::consensus::decisions::ctas::CtasDeclarationBytes,
	pub dispatch_hash: [u8; 32],
}

/// Wendelon ADR 0120 D6: a CTAS command as its statement's caller asks for it,
/// through the authenticated administrative route (which checked the caller's
/// tenant). An owner command carries the owner's secret; the leader checks it
/// against the applied chain and proposes a command naming only the commitment.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasProposal {
	Begin(CtasBeginRequest),
	Renew {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		epoch: u64,
		secret: [u8; 32],
	},
	Publish {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		epoch: u64,
		secret: [u8; 32],
	},
	Abandon {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		epoch: u64,
		secret: [u8; 32],
		cause: crate::consensus::decisions::ctas::CtasAbandonCause,
	},
	Recover {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		new_epoch: u64,
	},
	Conclude {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
		epoch: u64,
	},
	/// The operation's chain, read at the leader after a linearizable read
	/// barrier: how a statement learns, before running its SELECT, whether its
	/// operation id is already declared and by which statement and owner.
	/// Proposes nothing.
	Resolve {
		tenant: crate::schema::TenantId,
		op: [u8; 16],
	},
}

impl CtasProposal {
	/// The tenant the request names; the route checks the caller's against it.
	pub fn tenant(&self) -> crate::schema::TenantId {
		match self {
			Self::Begin(begin) => begin.tenant,
			Self::Renew { tenant, .. }
			| Self::Publish { tenant, .. }
			| Self::Abandon { tenant, .. }
			| Self::Recover { tenant, .. }
			| Self::Conclude { tenant, .. }
			| Self::Resolve { tenant, .. } => *tenant,
		}
	}
}

/// The secret never appears in a log line or a panic message.
impl std::fmt::Debug for CtasProposal {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let kind = match self {
			Self::Begin(_) => "Begin",
			Self::Renew { .. } => "Renew",
			Self::Publish { .. } => "Publish",
			Self::Abandon { .. } => "Abandon",
			Self::Recover { .. } => "Recover",
			Self::Conclude { .. } => "Conclude",
			Self::Resolve { .. } => "Resolve",
		};
		f.debug_struct("CtasProposal")
			.field("kind", &kind)
			.field("tenant", &self.tenant())
			.field("secret", &"<redacted>")
			.finish()
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
	/// Wendelon ADR 0121 D1: the window this node's configuration binds when it
	/// leads a store that has none bound (ms; 0 is the Disabled binding).
	retention_window_ms: u64,
	/// ADR 0700: peer clock samples for the decision clock gate.
	clock_samples: DecisionClockSamples,
	/// ADR 0700: the leader this node last sent a request to, and when.
	last_sent: std::sync::Mutex<Option<(u32, std::time::Instant)>>,
	/// Test-only (ADR 0707): the leader's handling of one forwarded proposal,
	/// held before it is admitted.
	#[cfg(test)]
	forwarded_hold: std::sync::Mutex<Option<ForwardedHold>>,
	/// Test-only (wendelon ADR 0120): pauses the leader's CTAS maintenance, so a
	/// control can drive Recover, Conclude and the owner's own Publish itself.
	#[cfg(test)]
	pub(crate) ctas_maintenance_paused: std::sync::atomic::AtomicBool,
	/// Test-only (wendelon ADR 0122): pauses the leader's generation steps, so a
	/// control can move the clock to a chosen point before the next target samples it.
	#[cfg(test)]
	pub(crate) generations_paused: std::sync::atomic::AtomicBool,
	/// Wendelon ADR 0123 D7: the targets this node proposed as leader, counted by
	/// reason, and the most recent (gid, reason) pairs, bounded. Proposer policy only.
	proposed_reasons: std::sync::Mutex<ProposedReasons>,
}

/// Wendelon ADR 0123 D7: proposals by reason, and the recent ones by gid.
#[derive(Default)]
pub(crate) struct ProposedReasons {
	pub(crate) counts: BTreeMap<&'static str, u64>,
	pub(crate) recent: std::collections::VecDeque<(
		u64,
		[u8; 32],
		crate::consensus::decisions::generations::TargetReason,
	)>,
}

const RECENT_PROPOSED_REASONS: usize = 256;

/// Test-only (wendelon ADR 0123 L3): each forwarded Reserve the reducer refused, by key.
#[cfg(test)]
pub(crate) static RESERVE_REFUSALS: std::sync::Mutex<
	Vec<(
		crate::consensus::decisions::ParentKey,
		crate::consensus::decisions::DecisionRefusal,
	)>,
> = std::sync::Mutex::new(Vec::new());

/// Test-only (ADR 0707): holds the leader's handling of the next forwarded
/// proposal from `sender` that `matches`, once: `reached` is notified with the
/// request recorded in `held`, and the handler waits for `release`.
#[cfg(test)]
pub(crate) struct ForwardedHold {
	pub(crate) sender: u32,
	pub(crate) matches: fn(&ForwardedDecision) -> bool,
	pub(crate) reached: Arc<tokio::sync::Notify>,
	pub(crate) release: Arc<tokio::sync::Notify>,
	pub(crate) held: Arc<std::sync::Mutex<Option<ForwardedDecision>>>,
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
			retention_window_ms: 0,
			clock_samples: DecisionClockSamples::default(),
			last_sent: std::sync::Mutex::new(None),
			#[cfg(test)]
			forwarded_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			ctas_maintenance_paused: std::sync::atomic::AtomicBool::new(false),
			#[cfg(test)]
			generations_paused: std::sync::atomic::AtomicBool::new(false),
			proposed_reasons: Default::default(),
		}
	}

	/// Wendelon ADR 0121 D1: the window this node binds when it leads an unbound
	/// store.
	pub(crate) fn with_retention_window_ms(mut self, window_ms: u64) -> Self {
		self.retention_window_ms = window_ms;
		self
	}

	/// ADR 0104 (wendelon): the current leader as a dialable slot; this node's
	/// own slot only when the leader is this very Raft instance.
	pub(crate) async fn leader_slot(&self) -> Option<u32> {
		crate::consensus::decisions::instances::leader_slot(
			&self.raft,
			&self.state_machine,
			self.local.id,
		)
		.await
	}

	/// Test-only (ADR 0707): queues a hold of the leader's handling of a
	/// forwarded proposal (see [`ForwardedHold`]).
	#[cfg(test)]
	pub(crate) fn hold_forwarded_for_test(&self, hold: ForwardedHold) {
		*self.forwarded_hold.lock().unwrap() = Some(hold);
	}

	/// Test-only (ADR 0707): the leader's handler of a forwarded proposal
	/// pauses here if a queued hold matches it.
	#[cfg(test)]
	pub(crate) async fn pause_forwarded(&self, sender: u32, request: &ForwardedDecision) {
		let hold = {
			let mut slot = self.forwarded_hold.lock().unwrap();
			match slot.as_ref() {
				Some(hold) if hold.sender == sender && (hold.matches)(request) => slot.take(),
				_ => None,
			}
		};
		if let Some(hold) = hold {
			*hold.held.lock().unwrap() = Some(request.clone());
			hold.reached.notify_one();
			hold.release.notified().await;
		}
	}

	/// ADR 0700: the decision clock now, through the gate. ADR 0729: also the
	/// stamp a leader puts on a lease batch it builds.
	pub(crate) async fn decision_clock_now(&self) -> Result<u64, ProposalRefusal> {
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
		let Some(leader) = self.leader_slot().await else {
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
		if let ForwardedDecision::OwnerOp(_) = &request {
			return self.propose_owner_op_as(sender, request).await;
		}
		let leader = self.leader_slot().await;
		if leader != Some(self.local.id) {
			return ProposalResponse::NotLeader {
				leader_hint: leader,
			};
		}
		// Test-only (wendelon ADR 0123 L3): a Reserve's key, so its applied refusal can be
		// recorded at the reducer boundary.
		#[cfg(test)]
		let reserve_key = match &request {
			ForwardedDecision::Reserve { claim } => Some(claim.body.key),
			_ => None,
		};
		let command = match admission(self.mode, sender, request, &self.trusted_keys) {
			Ok(Admitted::Ready(command)) => command,
			// Only the gated decision clock stamps a time-bearing command. ADR
			// 0702: after a leader change the gate may lack a fresh voter sample
			// until a follower's probe arrives, so admission waits for it,
			// bounded, then rechecks leadership before stamping.
			Ok(Admitted::Unstamped(unstamped)) => {
				let deadline = tokio::time::Instant::now() + CLOCK_ADMISSION_WAIT;
				match wait_for_decision_clock(
					&self.clock_samples.arrived,
					deadline,
					|| self.decision_clock_now(),
					|| async { self.leader_slot().await == Some(self.local.id) },
				)
				.await
				{
					ClockWait::Stamp(stamp_ms) => unstamped.stamp(stamp_ms),
					ClockWait::Refused(refusal) => return ProposalResponse::Refused(refusal),
					ClockWait::NotLeader => {
						return ProposalResponse::NotLeader {
							leader_hint: self.leader_slot().await,
						};
					}
				}
			}
			Err(refusal) => return ProposalResponse::Refused(refusal),
		};
		let response = self.commit(command).await;
		#[cfg(test)]
		if let (
			Some(key),
			ProposalResponse::Applied {
				outcome:
					ProposalOutcome::Decision(crate::consensus::decisions::DecisionResult::Refused(
						refusal,
					)),
				..
			},
		) = (reserve_key, &response)
		{
			RESERVE_REFUSALS.lock().unwrap().push((key, *refusal));
		}
		response
	}

	async fn commit(&self, command: WendelonRequest) -> ProposalResponse {
		let promotion = matches!(command, WendelonRequest::PromoteIncarnation(_));
		match self.raft.client_write(command).await {
			Ok(response) if promotion => match response.data {
				WendelonResponse::Success => ProposalResponse::Applied {
					outcome: ProposalOutcome::Promotion { refused: None },
					log_index: response.log_id.index,
				},
				WendelonResponse::Refused(refusal) => ProposalResponse::Applied {
					outcome: ProposalOutcome::Promotion {
						refused: Some(refusal),
					},
					log_index: response.log_id.index,
				},
				other => ProposalResponse::Indeterminate(format!(
					"unexpected response to a promotion: {other:?}"
				)),
			},
			Ok(response) => match outcome_of(response.data) {
				Ok(outcome) => ProposalResponse::Applied {
					outcome,
					log_index: response.log_id.index,
				},
				Err(message) => ProposalResponse::Indeterminate(message),
			},
			Err(error) => match self.leader_slot().await {
				leader if leader != Some(self.local.id) => ProposalResponse::NotLeader {
					leader_hint: leader,
				},
				_ => ProposalResponse::Indeterminate(error.to_string()),
			},
		}
	}

	/// ADR 0725, leader side: admit and propose an owner operation. Only the
	/// answers given before `client_write` is called say it was not submitted.
	async fn propose_owner_op_as(
		&self,
		sender: u32,
		request: ForwardedDecision,
	) -> ProposalResponse {
		let leader = self.leader_slot().await;
		if leader != Some(self.local.id) {
			return ProposalResponse::OwnerNotSubmitted {
				leader_hint: leader,
			};
		}
		let command = match admission(self.mode, sender, request, &self.trusted_keys) {
			Ok(Admitted::Ready(command)) => command,
			Ok(Admitted::Unstamped(_)) => {
				return ProposalResponse::Refused(ProposalRefusal::Malformed);
			}
			Err(refusal) => return ProposalResponse::Refused(refusal),
		};
		use crate::consensus::slab_ops::OpResponse;
		match self.raft.client_write(command).await {
			Ok(response) => {
				let answer = match response.data {
					WendelonResponse::Op(OpResponse::Receipt(receipt)) => {
						OwnerOpAnswer::Receipt(receipt)
					}
					WendelonResponse::Op(OpResponse::Ack(receipt)) => OwnerOpAnswer::Ack(receipt),
					WendelonResponse::Refused(refusal) => OwnerOpAnswer::Refused(refusal),
					other => {
						return ProposalResponse::Indeterminate(format!(
							"unexpected response to an owner operation: {other:?}"
						));
					}
				};
				ProposalResponse::OwnerApplied {
					answer,
					log_index: response.log_id.index,
				}
			}
			// The proposal was handed over: whatever the error says about the
			// role now, it does not prove nothing was appended.
			Err(error) => ProposalResponse::Indeterminate(error.to_string()),
		}
	}

	/// ADR 0725, owner side: propose this node's own signed operation, locally
	/// when it leads and through the leader otherwise. A known leader is only
	/// where to send it. The caller bounds the whole attempt.
	async fn propose_own_op(
		&self,
		op: crate::consensus::owner_ops::SignedOwnerOp,
	) -> OwnerOpOutcome {
		let request = ForwardedDecision::OwnerOp(op);
		let response = match self.leader_slot().await {
			Some(leader) if leader == self.local.id => {
				self.propose_as(self.local.id, request).await
			}
			Some(leader) => match self.forward(leader, &request).await {
				Ok(response) => response,
				Err(error) => return OwnerOpOutcome::Unknown(error.to_string()),
			},
			None => return OwnerOpOutcome::NotSubmitted("no leader is known".into()),
		};
		match response {
			ProposalResponse::OwnerApplied { answer, log_index } => {
				OwnerOpOutcome::Applied { answer, log_index }
			}
			ProposalResponse::OwnerNotSubmitted { leader_hint } => OwnerOpOutcome::NotSubmitted(
				format!("the node asked does not lead (hint {leader_hint:?})"),
			),
			// An admission refusal: decided before anything was proposed.
			ProposalResponse::Refused(refusal) => {
				OwnerOpOutcome::NotSubmitted(format!("refused at admission: {refusal:?}"))
			}
			ProposalResponse::Indeterminate(message) => OwnerOpOutcome::Unknown(message),
			other => OwnerOpOutcome::Unknown(format!(
				"unexpected response to an owner operation: {other:?}"
			)),
		}
	}

	/// Sender side: propose locally when leader, otherwise forward.
	pub(crate) async fn propose(&self, request: ForwardedDecision) -> ProposalResponse {
		match self.leader_slot().await {
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
			self.mode.raft_frame_version(),
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
		let leader = self.leader_slot().await;
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

	/// Wendelon ADR 0123 D7: the recent proposals (gid, target digest, reason), oldest
	/// first, so a control can attribute a committed certificate by its target digest.
	/// Read only by tests (DC-286).
	#[cfg(test)]
	pub(crate) fn proposed_trace(
		&self,
	) -> Vec<(
		u64,
		[u8; 32],
		crate::consensus::decisions::generations::TargetReason,
	)> {
		self.proposed_reasons
			.lock()
			.unwrap()
			.recent
			.iter()
			.copied()
			.collect()
	}

	/// Wendelon ADR 0123 D7: this node's proposals as leader, by reason.
	/// Read only by tests (DC-286).
	#[cfg(test)]
	pub(crate) fn proposed_counts(&self) -> BTreeMap<&'static str, u64> {
		self.proposed_reasons.lock().unwrap().counts.clone()
	}

	/// Wendelon ADR 0123 D7: the reason this node proposed target `gid` with, if it
	/// was among the recent proposals here.
	/// Read only by tests (DC-286).
	#[cfg(test)]
	pub(crate) fn proposed_reason(
		&self,
		gid: u64,
	) -> Option<crate::consensus::decisions::generations::TargetReason> {
		self.proposed_reasons
			.lock()
			.unwrap()
			.recent
			.iter()
			.rev()
			.find(|(recent, _, _)| *recent == gid)
			.map(|(_, _, reason)| *reason)
	}

	/// Sender side: look up locally when leader, otherwise ask the leader.
	pub(crate) async fn lookup(&self, request: LookupRequest) -> LookupResponse {
		match self.leader_slot().await {
			Some(leader) if leader == self.local.id => self.lookup_as_leader(request).await,
			Some(leader) => self
				.round_trip(
					leader,
					MsgType::DECISION_LOOKUP_REQ,
					MsgType::DECISION_LOOKUP_RESP,
					// Wendelon ADR 0123 D6: the decision envelope, both ways, checked before decode.
					crate::consensus::decision_mode::RAFT_FRAME_VERSION_PARENT_DECISIONS,
					&request,
				)
				.await
				.unwrap_or_else(|error| LookupResponse::Indeterminate(error.to_string())),
			None => LookupResponse::NotLeader { leader_hint: None },
		}
	}

	/// ADR 0736, leader side: proposes the backup hold's begin or release. Not
	/// time-bearing and not forwarded: the operator's request reaches the leader
	/// through the authenticated administrative route. The reducer decides.
	pub(crate) async fn propose_backup_hold(&self, hold: BackupHoldProposal) -> ProposalResponse {
		if self.mode != DecisionMode::ParentDecisions {
			return ProposalResponse::Refused(ProposalRefusal::ModeToday);
		}
		let command = match hold {
			BackupHoldProposal::Begin { id } => DecisionCommand::BeginBackupHold { id },
			BackupHoldProposal::Release { id, entered_at } => {
				DecisionCommand::ReleaseBackupHold { id, entered_at }
			}
		};
		self.commit(WendelonRequest::Decision(command)).await
	}

	/// Wendelon ADR 0120 D6, leader side: a CTAS command. Not forwarded: the
	/// caller's request reaches the leader through the authenticated
	/// administrative route, which checked its tenant; a follower answers with the
	/// leader hint. An owner command's secret is checked here, against the applied
	/// chain's commitment, and never enters the command. Begin, Renew and Recover
	/// take the gated leader clock. The reducer decides everything else.
	pub(crate) async fn propose_ctas(&self, proposal: CtasProposal) -> ProposalResponse {
		if self.mode != DecisionMode::ParentDecisions {
			return ProposalResponse::Refused(ProposalRefusal::ModeToday);
		}
		let leader = self.leader_slot().await;
		if leader != Some(self.local.id) {
			return ProposalResponse::NotLeader {
				leader_hint: leader,
			};
		}
		let owner_of = |tenant, op| async move {
			self.state_machine
				.read_state(move |state| {
					state
						.decisions
						.as_ref()
						.and_then(|decisions| decisions.ctas().chain(tenant, op))
						.map(|chain| chain.dispatch_hash)
				})
				.await
		};
		let proves = |secret: &[u8; 32], owner: Option<[u8; 32]>| {
			owner.filter(|owner| *blake3::hash(secret).as_bytes() == *owner)
		};
		if let CtasProposal::Resolve { tenant, op } = proposal {
			return self.resolve_ctas(tenant, op).await;
		}
		let admitted = match proposal {
			CtasProposal::Begin(begin) => Admitted::Unstamped(Unstamped::CtasBegin { begin }),
			CtasProposal::Renew {
				tenant,
				op,
				epoch,
				secret,
			} => match proves(&secret, owner_of(tenant, op).await) {
				Some(owner) => Admitted::Unstamped(Unstamped::CtasRenew {
					tenant,
					op,
					epoch,
					owner,
				}),
				None => return ProposalResponse::Refused(ProposalRefusal::CtasNotOwner),
			},
			CtasProposal::Publish {
				tenant,
				op,
				epoch,
				secret,
			} => match proves(&secret, owner_of(tenant, op).await) {
				Some(owner) => Admitted::Unstamped(Unstamped::CtasPublish {
					tenant,
					op,
					epoch,
					owner,
				}),
				None => return ProposalResponse::Refused(ProposalRefusal::CtasNotOwner),
			},
			CtasProposal::Abandon {
				tenant,
				op,
				epoch,
				secret,
				cause,
			} => match proves(&secret, owner_of(tenant, op).await) {
				Some(owner) => Admitted::Unstamped(Unstamped::CtasAbandon {
					tenant,
					op,
					epoch,
					owner,
					cause,
				}),
				None => return ProposalResponse::Refused(ProposalRefusal::CtasNotOwner),
			},
			CtasProposal::Recover {
				tenant,
				op,
				new_epoch,
			} => Admitted::Unstamped(Unstamped::CtasRecover {
				tenant,
				op,
				new_epoch,
			}),
			CtasProposal::Conclude { tenant, op, epoch } => {
				Admitted::Unstamped(Unstamped::CtasConclude { tenant, op, epoch })
			}
			CtasProposal::Resolve { .. } => unreachable!("answered above"),
		};
		let command = match admitted {
			Admitted::Ready(command) => command,
			Admitted::Unstamped(unstamped) => {
				let deadline = tokio::time::Instant::now() + CLOCK_ADMISSION_WAIT;
				match wait_for_decision_clock(
					&self.clock_samples.arrived,
					deadline,
					|| self.decision_clock_now(),
					|| async { self.leader_slot().await == Some(self.local.id) },
				)
				.await
				{
					ClockWait::Stamp(stamp_ms) => unstamped.stamp(stamp_ms),
					ClockWait::Refused(refusal) => return ProposalResponse::Refused(refusal),
					ClockWait::NotLeader => {
						return ProposalResponse::NotLeader {
							leader_hint: self.leader_slot().await,
						};
					}
				}
			}
		};
		self.commit(command).await
	}

	/// Wendelon ADR 0120: [`CtasProposal::Resolve`] at the leader: a
	/// linearizable read barrier, its application here, then the chain.
	async fn resolve_ctas(
		&self,
		tenant: crate::schema::TenantId,
		op: [u8; 16],
	) -> ProposalResponse {
		let index = match self.cut_as_leader().await {
			CutResponse::Cut(index) => index,
			CutResponse::NotLeader { leader_hint } => {
				return ProposalResponse::NotLeader { leader_hint };
			}
			CutResponse::Refused(refusal) => return ProposalResponse::Refused(refusal),
			CutResponse::Indeterminate(why) => return ProposalResponse::Indeterminate(why),
		};
		if let Err(error) = self
			.wait_applied(index, std::time::Duration::from_secs(5))
			.await
		{
			return ProposalResponse::Indeterminate(error.to_string());
		}
		let (chain, visible_from) = self
			.state_machine
			.read_state(move |state| {
				let Some(decisions) = state.decisions.as_ref() else {
					return (None, None);
				};
				let chain = decisions.ctas().chain(tenant, op).cloned();
				let visible_from = chain
					.as_ref()
					.and_then(|chain| decisions.ctas().visible_from(tenant, chain.object));
				(chain.map(Box::new), visible_from)
			})
			.await;
		ProposalResponse::CtasResolved {
			chain,
			visible_from,
			log_index: index,
		}
	}

	/// Leader side of a cut: a linearizable read barrier, then its read index.
	pub(crate) async fn cut_as_leader(&self) -> CutResponse {
		if self.mode != DecisionMode::ParentDecisions {
			return CutResponse::Refused(ProposalRefusal::ModeToday);
		}
		let leader = self.leader_slot().await;
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
		match self.leader_slot().await {
			Some(leader) if leader == self.local.id => self.cut_as_leader().await,
			Some(leader) => self
				.round_trip(
					leader,
					MsgType::DECISION_CUT_REQ,
					MsgType::DECISION_CUT_RESP,
					crate::schema::HRFT_ENVELOPE_VERSION,
					&CutRequest {
						sent_ms: self.now_ms(),
					},
				)
				.await
				.unwrap_or_else(|error| CutResponse::Indeterminate(error.to_string())),
			None => CutResponse::NotLeader { leader_hint: None },
		}
	}

	/// DST stage 1 (Codex 01a1e61e): the recorded clock samples with their monotonic ages, as
	/// the gate reads them. Read only: nothing here records, refreshes or removes a sample.
	#[cfg(test)]
	pub(crate) fn clock_samples_for_test(&self) -> Vec<(u32, u64, std::time::Duration)> {
		self.clock_samples.aged()
	}

	/// DST stage 1 (Codex 01a1e607, 01a1e61e): the test-directed sampling entry. It is
	/// [`Self::cut`] itself, the ordinary authenticated CUT whose `sent_ms` the leader records,
	/// sent at a time the test chooses instead of after [`CLOCK_PROBE_INTERVAL`] of silence: a
	/// recorded scheduling difference from [`Self::clock_probe_step`], nothing else. It sets
	/// `last_sent` exactly as every round trip does; it never resets the cooldown or edits a
	/// sample, and it is awaited, unlike the probe's detached task.
	#[cfg(test)]
	pub(crate) async fn test_directed_sample(&self) -> CutResponse {
		self.cut().await
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

	/// One request and response with the leader, the whole attempt bounded
	/// by [`DECISION_RPC_DEADLINE`]: resolution and connection, opening the
	/// stream, writing, finishing and reading the answer. The deadline is set
	/// once, before the first of them, and never renewed. On expiry the
	/// attempt ends with [`ErrorKind::TimedOut`] and drops only its own
	/// stream; the shared connection stays usable. A timed-out attempt is an
	/// unknown outcome: the leader may still have admitted the request and
	/// may commit it later.
	async fn round_trip<Q: Serialize, R: serde::de::DeserializeOwned>(
		&self,
		leader: u32,
		request_type: MsgType,
		response_type: MsgType,
		// The frame version of this request and its answer. Each message
		// family has its own: forwarded proposals carry the ParentDecisions
		// envelope (ADR 0721), every other request here stays at the one
		// HRFT version.
		version: u8,
		request: &Q,
	) -> std::io::Result<R> {
		let deadline = tokio::time::Instant::now() + DECISION_RPC_DEADLINE;
		let meta = postcard::to_stdvec(request).map_err(Error::other)?;
		*self.last_sent.lock().unwrap() = Some((leader, std::time::Instant::now()));
		if meta.len() > MAX_DECISION_FRAME_BYTES {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"decision frame exceeds its bound",
			));
		}
		let attempt = async {
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
				version,
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
			decode_response(&frame, response_type, version)
		};
		tokio::time::timeout_at(deadline, attempt)
			.await
			.unwrap_or_else(|_| {
				Err(Error::new(
					ErrorKind::TimedOut,
					format!(
						"decision request 0x{:02x} to the leader {leader} had no answer within {:?}",
						request_type.0, DECISION_RPC_DEADLINE
					),
				))
			})
	}
}

/// ADR 0707: what one of decision maintenance's own proposals came to, as the
/// caller saw it. Local and never serialized: [`ProposalResponse`], the wire
/// answer, is unchanged.
#[derive(Debug)]
pub(crate) enum MaintenanceProposalResult {
	/// The node is stopping and the caller stopped waiting. A fact about the
	/// caller only, never evidence about the proposal: it may not have been
	/// submitted, or may be committed already or commit later. Raft alone
	/// decides it, and nothing is cleared or compensated on this answer.
	Stopped,
	Response(ProposalResponse),
}

/// ADR 0707: waits for `proposal`'s answer only until `stop` is set. A stop
/// already set returns [`MaintenanceProposalResult::Stopped`] before
/// `proposal` is polled, so nothing is submitted; a stop set during the wait
/// drops `proposal` wherever it waits. A closed stop channel never stops it.
pub(crate) async fn until_stopped(
	mut stop: tokio::sync::watch::Receiver<bool>,
	proposal: impl std::future::Future<Output = ProposalResponse>,
) -> MaintenanceProposalResult {
	if *stop.borrow_and_update() {
		return MaintenanceProposalResult::Stopped;
	}
	tokio::select! {
		biased;
		Ok(_) = stop.wait_for(|stopping| *stopping) => MaintenanceProposalResult::Stopped,
		response = proposal => MaintenanceProposalResult::Response(response),
	}
}

/// DC-296 (Codex 01a20716): the step decision maintenance's pass had selected, observed by a
/// drain that times out. Selecting a step happens just before its call; the word says which
/// call the pass selected and since when, not that the callee is running, waiting for a lock
/// or holding one, nor for how long. `Head` is the loop's head (no pass admitted: waiting for
/// a tick, a wake or the stop); `ProbeStop` is the stop having won there (cancelling and
/// joining the clock probe); `PassEnd` is a finished pass's end hooks (a no-op in production).
/// The certification step's phases are selected by this pass's own call only (its
/// `MaintenanceProposals`), never by a recovering replacement's build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceStep {
	Head,
	ProbeStop,
	ClockProbe,
	Signer,
	Retention,
	Floor,
	Takeover,
	Resolve,
	Ctas,
	RefusedIntents,
	Generation,
	Replacement,
	ValidateBase,
	/// The certification step entered: its lifecycle permit, then activating a committed
	/// generation.
	CertifyActivate,
	/// The closed set's `read_state` call, its lock acquisition included.
	CertifyClosedSet,
	/// The candidate's build and proofs (the base meta, `build_candidate`, the least
	/// visible row).
	CertifyBuild,
	/// The durable stage (or restage) and its signed readback.
	CertifyStage,
	/// The report: its proposal and wait, under the stop.
	CertifyReport,
	Cleanup,
	DebtSweep,
	PassEnd,
}

impl MaintenanceStep {
	/// Every step, in its code order: the code is the 1-based position (0: none selected).
	pub(crate) const ALL: [Self; 21] = [
		Self::Head,
		Self::ProbeStop,
		Self::ClockProbe,
		Self::Signer,
		Self::Retention,
		Self::Floor,
		Self::Takeover,
		Self::Resolve,
		Self::Ctas,
		Self::RefusedIntents,
		Self::Generation,
		Self::Replacement,
		Self::ValidateBase,
		Self::CertifyActivate,
		Self::CertifyClosedSet,
		Self::CertifyBuild,
		Self::CertifyStage,
		Self::CertifyReport,
		Self::Cleanup,
		Self::DebtSweep,
		Self::PassEnd,
	];
	const TIME_BITS: u32 = 56;

	/// The published code: the 1-based position in [`Self::ALL`].
	pub(crate) fn code(self) -> u64 {
		Self::ALL
			.iter()
			.position(|step| *step == self)
			.map_or(0, |at| at as u64 + 1)
	}

	fn from_code(code: u64) -> Option<Self> {
		code.checked_sub(1)
			.and_then(|at| Self::ALL.get(at as usize))
			.copied()
	}
}

/// DC-296 (Codex 01a20716): one decision maintenance task's selected step, one word (the step's
/// code in the top byte, the unix ms it was selected below), read without waiting. The task
/// owns it: it selects each step, and its [`MaintenanceStepClear`] clears the word when the task
/// returns or is cancelled. A clone is the node's read side, for the drain's timeout branch.
#[derive(Clone, Debug, Default)]
pub(crate) struct MaintenanceStepCell(Arc<StepWord>);

/// The shared word (one pointer in every build).
#[derive(Debug, Default)]
struct StepWord {
	word: std::sync::atomic::AtomicU64,
	/// Test-only: every selection, in order, once a control asked for it.
	#[cfg(test)]
	trace: std::sync::Mutex<Option<Vec<MaintenanceStep>>>,
}

impl MaintenanceStepCell {
	/// The pass selected `step`, now.
	pub(crate) fn select(&self, step: MaintenanceStep) {
		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map_or(0, |elapsed| elapsed.as_millis() as u64);
		let since = now & ((1 << MaintenanceStep::TIME_BITS) - 1);
		self.0.word.store(
			(step.code() << MaintenanceStep::TIME_BITS) | since,
			std::sync::atomic::Ordering::SeqCst,
		);
		#[cfg(test)]
		if let Some(trace) = self.0.trace.lock().unwrap().as_mut() {
			trace.push(step);
		}
	}

	/// The selected step and since when (unix ms); `None` while none is selected (before the
	/// task's first selection, or after it ended).
	pub(crate) fn selected(&self) -> Option<(MaintenanceStep, u64)> {
		let word = self.0.word.load(std::sync::atomic::Ordering::SeqCst);
		MaintenanceStep::from_code(word >> MaintenanceStep::TIME_BITS)
			.map(|step| (step, word & ((1 << MaintenanceStep::TIME_BITS) - 1)))
	}

	/// The owning task's guard: dropped (return or cancellation), it clears the word.
	pub(crate) fn clear_on_exit(&self) -> MaintenanceStepClear {
		MaintenanceStepClear(self.0.clone())
	}

	/// Test-only: records every selection from now on.
	#[cfg(test)]
	pub(crate) fn record_for_test(&self) {
		self.0.trace.lock().unwrap().get_or_insert_with(Vec::new);
	}

	/// Test-only: the selections recorded so far, in order.
	#[cfg(test)]
	pub(crate) fn trace_for_test(&self) -> Vec<MaintenanceStep> {
		self.0.trace.lock().unwrap().clone().unwrap_or_default()
	}
}

/// Clears its task's selected step when dropped.
pub(crate) struct MaintenanceStepClear(Arc<StepWord>);

impl Drop for MaintenanceStepClear {
	fn drop(&mut self) {
		self.0.word.store(0, std::sync::atomic::Ordering::SeqCst);
	}
}

/// ADR 0707: decision maintenance's own proposal path, the only one the
/// maintenance stop cancels. It wraps the whole proposal future: the leader's
/// admission and `client_write`, or the follower's forward (connect, open,
/// write, read). Proposals from client RPCs, and the leader's handling of
/// forwarded ones, go through [`DecisionProposals`] and never see this stop.
///
/// Each method derives its command from applied state on every tick; the
/// replay basis at each call site names the reducer check that makes a late,
/// duplicate or re-derived copy harmless: each of those checks refuses before
/// changing anything, under the maintained retained-bytes invariant. That is
/// not a claim that every refusal leaves state unchanged: DC-205
/// (`commit_generation`'s `CounterOverflow` after its removals) is a known,
/// separate exception. Raft has no log-level dedup, and an old-term entry that
/// survives a leader change is applied before anything the new leader appends.
pub(crate) struct MaintenanceProposals<'a> {
	proposals: &'a DecisionProposals,
	stop: tokio::sync::watch::Receiver<bool>,
	/// DC-296: this task's selected step; the certification phases select through it.
	step: MaintenanceStepCell,
}

impl<'a> MaintenanceProposals<'a> {
	pub(crate) fn new(
		proposals: &'a DecisionProposals,
		stop: tokio::sync::watch::Receiver<bool>,
		step: MaintenanceStepCell,
	) -> Self {
		Self {
			proposals,
			stop,
			step,
		}
	}

	/// DC-296: this pass selected `step`.
	pub(crate) fn select(&self, step: MaintenanceStep) {
		self.step.select(step);
	}

	/// DC-296: this task's step word, for the certification phases.
	pub(crate) fn step(&self) -> &MaintenanceStepCell {
		&self.step
	}

	/// Sender side, local or forwarded, until the node stops.
	async fn propose(&self, request: ForwardedDecision) -> MaintenanceProposalResult {
		until_stopped(self.stop.clone(), self.proposals.propose(request)).await
	}

	/// Leader side, until the node stops.
	async fn commit(&self, command: WendelonRequest) -> MaintenanceProposalResult {
		until_stopped(self.stop.clone(), self.proposals.commit(command)).await
	}

	/// ADR 0699 S5a, leader only: commits the pending generation once every
	/// member reported, or proposes the next target. The reducer decides.
	/// Wendelon ADR 0121 D1, leader only: binds this node's configured window
	/// while the store has none. Derived only while unbound; a second copy
	/// returns RetentionBound and changes nothing, and another leader's
	/// different window is refused RetentionRebind (`consensus/decisions.rs`
	/// `bind_retention`).
	pub(crate) async fn ensure_retention_once(&self) -> Option<MaintenanceProposalResult> {
		let proposals = self.proposals;
		if proposals.mode != DecisionMode::ParentDecisions
			|| proposals.leader_slot().await != Some(proposals.local.id)
		{
			return None;
		}
		let unbound = proposals
			.state_machine
			.read_state(|state| {
				state
					.decisions
					.as_ref()
					.is_some_and(|decisions| decisions.retention().is_none())
			})
			.await;
		if !unbound {
			return None;
		}
		Some(
			self.commit(WendelonRequest::Decision(DecisionCommand::BindRetention {
				window_ms: proposals.retention_window_ms,
			}))
			.await,
		)
	}

	pub(crate) async fn generation_step(&self) -> Option<MaintenanceProposalResult> {
		let proposals = self.proposals;
		if proposals.mode != DecisionMode::ParentDecisions
			|| proposals.leader_slot().await != Some(proposals.local.id)
		{
			return None;
		}
		#[cfg(test)]
		if proposals
			.generations_paused
			.load(std::sync::atomic::Ordering::Acquire)
		{
			return None;
		}
		// Wendelon ADR 0121 D3: the gated decision clock, the age trigger's only
		// input; without a fresh sample, ordinary targets still progress.
		let now_ms = proposals.decision_clock_now().await.ok();
		let (command, reason) = proposals
			.state_machine
			.read_state(move |state| {
				if let Some(gid) = crate::consensus::decisions::generations::ready_commit(state) {
					return Some((DecisionCommand::CommitGeneration { gid }, None));
				}
				let applied = state.last_applied_log_id.map_or(0, |id| id.index);
				crate::consensus::decisions::generations::next_target_with_reason(
					state, applied, now_ms,
				)
				.map(|(target, reason)| (DecisionCommand::ProposeTarget { target }, Some(reason)))
			})
			.await?;
		// Wendelon ADR 0123 D7: each proposal is named once, by its first reason.
		if let (DecisionCommand::ProposeTarget { target }, Some(reason)) = (&command, reason) {
			tracing::info!(
				gid = target.gid,
				t = target.t,
				stamp_ms = target.retention.stamp_ms,
				reason = reason.label(),
				"generation target proposed"
			);
			crate::metrics::global().observe_decision_generation_target_proposed(reason.label());
			let mut proposed = proposals.proposed_reasons.lock().unwrap();
			*proposed.counts.entry(reason.label()).or_default() += 1;
			if proposed.recent.len() == RECENT_PROPOSED_REASONS {
				proposed.recent.pop_front();
			}
			proposed.recent.push_back((
				target.gid,
				crate::consensus::decisions::generations::target_digest(target),
				reason,
			));
		}
		// Replay basis. ProposeTarget is derived with `gid = next_gid` while no
		// current target is pending; a second copy is refused StaleGeneration,
		// since the first moved `next_gid` (`consensus/decisions/generations.rs:167-176`,
		// `:189`), and never replaces the recorded target or its reports.
		// CommitGeneration is derived once every member reported; a second copy
		// finds nothing pending, and one whose target was replaced fails the
		// gid check, both StaleGeneration (`generations.rs:240-248`).
		Some(self.commit(WendelonRequest::Decision(command)).await)
	}

	/// Leader-only floor task step. Proposes nothing unless leader with a
	/// healthy clock and a higher floor to set.
	pub(crate) async fn advance_floor_once(&self) -> Option<MaintenanceProposalResult> {
		let proposals = self.proposals;
		if proposals.mode != DecisionMode::ParentDecisions
			|| proposals.leader_slot().await != Some(proposals.local.id)
		{
			return None;
		}
		let now = proposals.decision_clock_now().await.ok()?;
		let command = proposals
			.state_machine
			.read_state(|state| floor_proposal(state, now))
			.await?;
		// Replay basis: derived only while the floor would rise. A floor at or
		// below the current one is refused FloorNotAdvanced with the committed
		// clock untouched (`consensus/decisions.rs:1756-1758`); a late copy with
		// a higher floor stays valid, its floor being its own stamp minus the
		// expiry.
		Some(self.commit(command).await)
	}

	/// Wendelon ADR 0120 D9: leader maintenance of CTAS chains (Recover after a
	/// lapsed owner lease, Conclude while recovering, Prune after retention), at
	/// the gated clock, with no client traffic. Bounded per call; the reducer
	/// decides. The number applied without a refusal.
	pub(crate) async fn ctas_step(&self, limit: usize) -> usize {
		let proposals = self.proposals;
		#[cfg(test)]
		if proposals
			.ctas_maintenance_paused
			.load(std::sync::atomic::Ordering::Acquire)
		{
			return 0;
		}
		if proposals.mode != DecisionMode::ParentDecisions
			|| proposals.leader_slot().await != Some(proposals.local.id)
		{
			return 0;
		}
		let Ok(now) = proposals.decision_clock_now().await else {
			return 0;
		};
		let due = proposals
			.state_machine
			.read_state(move |state| {
				state.decisions.as_ref().map_or_else(Vec::new, |decisions| {
					crate::consensus::decisions::ctas::maintenance_due(decisions, now, limit)
				})
			})
			.await;
		let mut applied = 0;
		for command in due {
			// Replay basis: Recover is answered Recovering or the terminal
			// answer once applied, Conclude the terminal answer, Prune Unknown;
			// none changes state twice.
			match self.commit(WendelonRequest::Decision(command)).await {
				MaintenanceProposalResult::Stopped => break,
				MaintenanceProposalResult::Response(ProposalResponse::Applied {
					outcome: ProposalOutcome::Decision(result),
					..
				}) if !matches!(result, DecisionResult::Refused(_)) => applied += 1,
				MaintenanceProposalResult::Response(_) => {}
			}
		}
		applied
	}

	/// Proposes Resolve for the reserved parents this node should drive: a bound
	/// lineage closed, and this node now holds the coordinator's range. Bounded
	/// per call; the reducer decides. Counts only Applied answers, and ends its
	/// remaining loop once the node stops.
	pub(crate) async fn resolve_closed_once(&self, limit: usize) -> usize {
		let proposals = self.proposals;
		if proposals.mode != DecisionMode::ParentDecisions {
			return 0;
		}
		let local = proposals.local.id;
		let candidates = proposals
			.state_machine
			.read_state(move |state| {
				crate::consensus::decisions::resolvable_by(state, local, limit)
			})
			.await;
		let mut proposed = 0;
		for (key, request_digest) in candidates {
			// Replay basis: derived for non-terminal parents with a closed
			// lineage. A terminal parent returns its existing result unchanged;
			// one no longer open is concluded only while a lineage is still
			// closed, otherwise StillOpen; a digest mismatch or an absent key is
			// refused (`consensus/decisions.rs:1661-1674`).
			match self
				.propose(ForwardedDecision::Resolve {
					key,
					request_digest,
				})
				.await
			{
				MaintenanceProposalResult::Stopped => break,
				MaintenanceProposalResult::Response(ProposalResponse::Applied { .. }) => {
					proposed += 1;
				}
				MaintenanceProposalResult::Response(_) => {}
			}
		}
		proposed
	}

	/// ADR 0729: proposes a takeover of each range this store should now take:
	/// held by another node, lapsed as this store's applied state and clock see
	/// it, and past this store's stagger. The reducer decides; a refusal (not
	/// lapsed on the ring clock, a consumed generation) is the normal answer to
	/// a proposal that lost. Counts takeovers applied.
	pub(crate) async fn take_over_lapsed_once(
		&self,
		key: &ed25519_dalek::SigningKey,
		signer: crate::consensus::decisions::Signer,
		stagger_ms: u64,
		limit: usize,
	) -> usize {
		let proposals = self.proposals;
		if proposals.mode != DecisionMode::ParentDecisions {
			return 0;
		}
		let now_ms = proposals.clock.now_ms();
		let candidates = proposals
			.state_machine
			.read_state(move |state| {
				crate::consensus::decisions::takeover_candidates(
					state,
					signer.node,
					now_ms,
					stagger_ms,
					limit,
				)
			})
			.await;
		let mut taken = 0;
		for (range_start, generation) in candidates {
			let takeover = crate::consensus::decisions::evidence::sign(
				key,
				crate::consensus::decisions::TakeOverBody {
					range_start,
					generation,
					successor: signer,
				},
				signer,
			);
			match self
				.propose(ForwardedDecision::TakeOverLineage { takeover })
				.await
			{
				MaintenanceProposalResult::Stopped => break,
				MaintenanceProposalResult::Response(ProposalResponse::Applied {
					outcome:
						ProposalOutcome::Decision(
							crate::consensus::decisions::DecisionResult::TookOver { .. },
						),
					..
				}) => taken += 1,
				MaintenanceProposalResult::Response(_) => {}
			}
		}
		taken
	}

	/// Registers this store's incarnation, then binds its decision key, each
	/// only when the applied state lacks it. One step per call. A different
	/// established incarnation for this node is left alone: this store is not
	/// that node's registered store, and nothing here may retire it.
	pub(crate) async fn ensure_signer_once(
		&self,
		incarnation: StoreIncarnation,
	) -> Option<MaintenanceProposalResult> {
		let proposals = self.proposals;
		if proposals.mode != DecisionMode::ParentDecisions {
			return None;
		}
		let local = proposals.local.id;
		let (current, bound) = proposals
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
		// The key this node signs with: bootstrap checked that its secret's
		// verifying key is its own entry in its trust map.
		let key = *proposals.trusted_keys.get(&local)?;
		let request = maintenance_step(current, bound, incarnation, key)?;
		// Replay basis. RegisterIncarnation is derived only while the applied
		// registry has no current incarnation for this node: an exact replay
		// returns Success with no change (`consensus/raft_storage.rs:5434-5440`),
		// and one after another incarnation got there first is refused
		// StaleSupersede (`raft_storage.rs:6727-6737`). BindSigner is derived
		// only while this incarnation's key is unbound: a second copy returns
		// SignerBound and inserts nothing, a different key gets SignerRebind and
		// a non-current incarnation SignerNotCurrent
		// (`consensus/decisions.rs:1724-1731`).
		Some(self.propose(request).await)
	}

	/// ADR 0699 S5a: a member's report of its durably staged candidate. The
	/// caller ignores the answer: a later tick skips the report once applied
	/// state holds it, and otherwise signs it again from the same staged
	/// metadata.
	pub(crate) async fn report(
		&self,
		report: Signed<crate::consensus::decisions::ReportBody>,
	) -> MaintenanceProposalResult {
		self.propose(ForwardedDecision::Report { report }).await
	}
}

/// The maintenance decision for this store, from the applied registry: register
/// a first incarnation, then bind it. A different established incarnation for
/// this node is left alone.
pub(crate) fn maintenance_step(
	current: Option<StoreIncarnation>,
	bound: bool,
	incarnation: StoreIncarnation,
	key: [u8; 32],
) -> Option<ForwardedDecision> {
	match current {
		None => Some(ForwardedDecision::RegisterIncarnation { incarnation }),
		Some(current) if current != incarnation => None,
		Some(_) if !bound => Some(ForwardedDecision::BindSigner { incarnation, key }),
		Some(_) => None,
	}
}

/// ADR 0721: an answer's type and frame version are judged before its body is
/// decoded, so an error frame or an answer in another encoding is never parsed
/// as this response.
pub(crate) fn check_response_frame(
	header: &FrameHeader,
	response_type: MsgType,
	version: u8,
) -> std::io::Result<()> {
	if header.msg_type != response_type.0 || header.version != version {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"unexpected decision response 0x{:02x} at frame version {}",
				header.msg_type, header.version
			),
		));
	}
	Ok(())
}

/// The client's judgment of a decision answer: its type and frame version first,
/// then (only then) its body. Wendelon ADR 0123 D6: an answer at another
/// envelope is refused before its body is interpreted.
pub(crate) fn decode_response<T: serde::de::DeserializeOwned>(
	frame: &crate::network::core::HrftFrame,
	response_type: MsgType,
	version: u8,
) -> std::io::Result<T> {
	check_response_frame(&frame.header, response_type, version)?;
	decode_bounded(frame.meta.as_ref())
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

#[async_trait::async_trait]
impl OwnerOpChannel for DecisionProposals {
	async fn propose_owner_op(
		&self,
		op: crate::consensus::owner_ops::SignedOwnerOp,
	) -> OwnerOpOutcome {
		self.propose_own_op(op).await
	}

	async fn read_cut(&self) -> std::io::Result<u64> {
		match self.cut().await {
			CutResponse::Cut(index) => Ok(index),
			other => Err(Error::new(
				ErrorKind::WouldBlock,
				format!("no linearizable cut: {other:?}"),
			)),
		}
	}

	async fn wait_applied(&self, index: u64, bound: std::time::Duration) -> std::io::Result<()> {
		DecisionProposals::wait_applied(self, index, bound).await
	}

	fn leader_known(&self) -> bool {
		self.raft.metrics().borrow().current_leader.is_some()
	}
}

// ============================================================
//  ADR 0104 (wendelon): A FRESH STORE'S FIRST IDENTITY
// ============================================================

/// A fresh Raft store, born `birth`, asks for its first identity for `slot`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceGrantRequest {
	pub birth: crate::consensus::decisions::instances::Birth,
	pub slot: u32,
	pub kind: crate::consensus::decisions::instances::GrantKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceGrantResponse {
	/// The identity granted to this birth, now or earlier.
	Granted {
		instance: u32,
	},
	/// Ask the leader: its slot and address, when known.
	NotLeader {
		leader: Option<(u32, String)>,
	},
	/// The reducer refused; nothing was granted.
	Refused(crate::consensus::decisions::DecisionRefusal),
	/// The request names another slot than its connection authenticates.
	Unauthorized,
	ModeToday,
	/// The proposal may or may not have committed; ask again with the same birth.
	Indeterminate(String),
}

impl DecisionProposals {
	/// Leader side of a grant. The slot must be the one the connection
	/// authenticates; the reducer decides everything else.
	pub(crate) async fn grant_instance(
		&self,
		authenticated_slot: u32,
		request: InstanceGrantRequest,
	) -> InstanceGrantResponse {
		if self.mode != DecisionMode::ParentDecisions {
			return InstanceGrantResponse::ModeToday;
		}
		if request.slot != authenticated_slot {
			return InstanceGrantResponse::Unauthorized;
		}
		if self.leader_slot().await != Some(self.local.id) {
			return InstanceGrantResponse::NotLeader {
				leader: self.leader_address().await,
			};
		}
		let command = WendelonRequest::Decision(DecisionCommand::GrantInstance {
			birth: request.birth,
			slot: request.slot,
			kind: request.kind,
		});
		match self.commit(command).await {
			ProposalResponse::Applied {
				outcome: ProposalOutcome::Decision(DecisionResult::InstanceGranted { instance }),
				..
			} => InstanceGrantResponse::Granted { instance },
			ProposalResponse::Applied {
				outcome: ProposalOutcome::Decision(DecisionResult::Refused(refusal)),
				..
			} => InstanceGrantResponse::Refused(refusal),
			ProposalResponse::NotLeader { .. } => InstanceGrantResponse::NotLeader {
				leader: self.leader_address().await,
			},
			ProposalResponse::Indeterminate(message) => {
				InstanceGrantResponse::Indeterminate(message)
			}
			other => InstanceGrantResponse::Indeterminate(format!(
				"unexpected answer to a grant: {other:?}"
			)),
		}
	}

	/// ADR 0104 (wendelon): the leader opens a replacement. The slot to
	/// replace is never its own.
	pub(crate) async fn propose_begin_replacement(
		&self,
		token: u128,
		slot: u32,
	) -> ProposalResponse {
		if self.mode != DecisionMode::ParentDecisions {
			return ProposalResponse::Refused(ProposalRefusal::ModeToday);
		}
		if slot == self.local.id {
			return ProposalResponse::Refused(ProposalRefusal::ReplacesLeader);
		}
		self.commit(WendelonRequest::Decision(
			DecisionCommand::BeginReplacement { token, slot },
		))
		.await
	}

	/// The current leader's slot and its address in the applied membership.
	async fn leader_address(&self) -> Option<(u32, String)> {
		let leader = self.raft.current_leader().await?;
		self.state_machine
			.read_state(|state| {
				let slot = crate::consensus::decisions::instances::slot_of(state, leader).ok()?;
				let node = state.last_membership.membership().get_node(&leader)?;
				Some((slot, node.addr.clone()))
			})
			.await
	}
}

/// ADR 0104 (wendelon): what one step of the replacement controller did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReplacementStep {
	/// Not the leader, no open replacement, or it waits for its grant.
	Idle,
	AddedLearner {
		r_new: u32,
	},
	Waiting(&'static str),
	Swapped {
		r_new: u32,
	},
	Completed {
		r_new: u32,
	},
	/// The membership is not one this replacement's states allow; nothing is
	/// proposed over it.
	Foreign(String),
	Failed(String),
}

/// One membership call's bound: the controller retries on its next step.
const MEMBERSHIP_CALL_BOUND: std::time::Duration = std::time::Duration::from_secs(10);

impl MaintenanceProposals<'_> {
	/// ADR 0104 (wendelon), leader only: reconciles the open replacement from
	/// the applied state and does at most one thing: add the reserved instance
	/// as a learner, promote it by one swap once it has caught up to the entry
	/// that added it, resume a joint configuration to the recorded target, or
	/// record Done. A membership change appended and not yet applied settles
	/// nothing, so the controller waits for it.
	pub(crate) async fn replacement_step(&self) -> ReplacementStep {
		use crate::consensus::decisions::instances::ReplacementPhase;
		let proposals = self.proposals;
		if proposals.mode != DecisionMode::ParentDecisions
			|| proposals.leader_slot().await != Some(proposals.local.id)
		{
			return ReplacementStep::Idle;
		}
		let view = proposals
			.state_machine
			.read_state(|state| {
				let decisions = state.decisions.as_ref()?;
				let (token, replacement) = decisions.instances().open_replacement()?;
				let ReplacementPhase::Reserved { r_new } = replacement.phase else {
					return None;
				};
				let membership = state.last_membership.membership();
				let configs: Vec<std::collections::BTreeSet<u32>> =
					membership.get_joint_config().clone();
				let nodes: std::collections::BTreeSet<u32> =
					membership.nodes().map(|(id, _)| *id).collect();
				let address = membership
					.get_node(&replacement.r_old)
					.map(|node| node.addr.clone());
				Some((
					token,
					replacement.clone(),
					r_new,
					configs,
					nodes,
					address,
					state.last_membership.log_id().map(|id| id.index),
				))
			})
			.await;
		let Some((token, replacement, r_new, configs, nodes, address, applied)) = view else {
			return ReplacementStep::Idle;
		};
		let effective = proposals
			.raft
			.metrics()
			.borrow()
			.membership_config
			.log_id()
			.map(|id| id.index);
		if effective != applied {
			return ReplacementStep::Waiting("a membership change is not applied yet");
		}
		let target = replacement.target(r_new);
		match configs.as_slice() {
			[uniform] if *uniform == target && !nodes.contains(&replacement.r_old) => {
				match self
					.commit(WendelonRequest::Decision(
						DecisionCommand::CompleteReplacement { token },
					))
					.await
				{
					MaintenanceProposalResult::Response(ProposalResponse::Applied {
						outcome:
							ProposalOutcome::Decision(DecisionResult::ReplacementCompleted { r_new }),
						..
					}) => ReplacementStep::Completed { r_new },
					other => ReplacementStep::Failed(format!("recording Done: {other:?}")),
				}
			}
			[uniform] if *uniform == replacement.pre_voters => {
				if !nodes.contains(&r_new) {
					let Some(addr) = address else {
						return ReplacementStep::Failed(
							"the replaced instance has no address in the membership".into(),
						);
					};
					return match tokio::time::timeout(
						MEMBERSHIP_CALL_BOUND,
						proposals
							.raft
							.add_learner(r_new, openraft::BasicNode { addr }, false),
					)
					.await
					{
						Ok(Ok(_)) => ReplacementStep::AddedLearner { r_new },
						Ok(Err(error)) => ReplacementStep::Failed(error.to_string()),
						Err(_) => ReplacementStep::Waiting("adding the learner"),
					};
				}
				// The learner's entry is the applied membership: it must hold
				// that entry before it is promoted.
				let added = applied.unwrap_or(0);
				let matched = proposals
					.raft
					.metrics()
					.borrow()
					.replication
					.as_ref()
					.and_then(|replication| replication.get(&r_new).cloned().flatten())
					.map(|id| id.index);
				if matched.is_none_or(|index| index < added) {
					return ReplacementStep::Waiting("the reserved instance is catching up");
				}
				self.swap(target, r_new).await
			}
			[old, new] if *old == replacement.pre_voters && *new == target => {
				self.swap(target, r_new).await
			}
			_ => ReplacementStep::Foreign(format!(
				"applied membership {configs:?} (nodes {nodes:?}) is not a state of replacement {token:#x}"
			)),
		}
	}

	/// The one swap, from the recorded pre-change voters or from their joint
	/// configuration, to the recorded target; the old instance is removed,
	/// never kept as a learner.
	async fn swap(&self, target: std::collections::BTreeSet<u32>, r_new: u32) -> ReplacementStep {
		match tokio::time::timeout(
			MEMBERSHIP_CALL_BOUND,
			self.proposals
				.raft
				.change_membership(openraft::ChangeMembers::ReplaceAllVoters(target), false),
		)
		.await
		{
			Ok(Ok(_)) => ReplacementStep::Swapped { r_new },
			Ok(Err(error)) => ReplacementStep::Failed(error.to_string()),
			Err(_) => ReplacementStep::Waiting("the swap"),
		}
	}
}
