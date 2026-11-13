//! Owner-signed slab operations of a `ParentDecisions` store.
//!
//! Under today's authority a slab operation is its owner's because the owner
//! appended it as leader. A `ParentDecisions` store instead signs each
//! operation with its incarnation's decision key, and the state machine
//! verifies the signature before anything else, so an owner that follows can
//! have its operation appended by the leader. The three kinds are signed under
//! three domains: a Submit signature is not a Cancel of the same operation, and
//! an Ack signature covers its exact batch.
//!
//! This rests on the existing signer binding and trust map. It is not
//! Byzantine agreement and it protects no history inside a snapshot.

use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};

use crate::consensus::core::{OwnerRef, SlabRefusal};
use crate::consensus::decisions::evidence::{self, Evidence};
use crate::consensus::decisions::{DecisionRefusal, DecisionState, Signed, Signer};
use crate::consensus::slab_ops::{AckOutcomes, CancelOp, SlabCommand, SlabOp, TierOp};
use crate::storage::index::OpTicket;

fn signer_of(owner: OwnerRef) -> Signer {
	Signer {
		node: owner.node(),
		incarnation: owner.incarnation(),
	}
}

impl Evidence for SlabOp {
	const DOMAIN: &'static str = "wendelon owner slab op submit v1";
	fn bound_signer(&self) -> Option<Signer> {
		Some(signer_of(self.command.owner()))
	}
}

impl Evidence for CancelOp {
	const DOMAIN: &'static str = "wendelon owner slab op cancel v1";
	fn bound_signer(&self) -> Option<Signer> {
		Some(signer_of(self.key.owner))
	}
}

impl Evidence for AckOutcomes {
	const DOMAIN: &'static str = "wendelon owner slab op ack v1";
	fn bound_signer(&self) -> Option<Signer> {
		Some(signer_of(self.owner))
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Kind {
	Submit(Signed<SlabOp>),
	Cancel(Signed<CancelOp>),
	Ack(Signed<AckOutcomes>),
}

/// One signed operation of a slab's owner. Outside tests it is made only from
/// an allocated ticket or an acknowledgement batch, by the owner's key, and it
/// has an effect only as the [`VerifiedOwnerOp`] that [`verify`] returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedOwnerOp(Kind);

/// What an owner operation asks for, once verified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerOp {
	Submit(SlabOp),
	Cancel(CancelOp),
	Ack(AckOutcomes),
}

/// An owner operation whose signature was verified against the key bound to
/// its owner's exact incarnation. Only [`verify`] makes one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VerifiedOwnerOp(OwnerOp);

impl SignedOwnerOp {
	/// The ticket's own operation, signed as the ticket's owner.
	pub(crate) fn submit(key: &SigningKey, ticket: &OpTicket) -> Self {
		let op = SlabOp {
			op_seq: ticket.key().op_seq,
			command: ticket.command().clone(),
		};
		Self(Kind::Submit(evidence::sign(
			key,
			op,
			signer_of(ticket.key().owner),
		)))
	}

	/// A Cancel of the ticket's own operation, by its original digest.
	pub(crate) fn cancel(key: &SigningKey, ticket: &OpTicket) -> Self {
		let cancel = CancelOp {
			key: ticket.key(),
			target_digest: ticket.digest(),
		};
		Self(Kind::Cancel(evidence::sign(
			key,
			cancel,
			signer_of(ticket.key().owner),
		)))
	}

	/// An acknowledgement of exactly `seqs`.
	pub(crate) fn ack(key: &SigningKey, owner: OwnerRef, seqs: Vec<u64>) -> Self {
		Self(Kind::Ack(evidence::sign(
			key,
			AckOutcomes { owner, seqs },
			signer_of(owner),
		)))
	}

	/// The owner this operation names. Unverified: admission compares it with
	/// the authenticated sender, and apply verifies the signature.
	pub(crate) fn claimed_owner(&self) -> OwnerRef {
		match &self.0 {
			Kind::Submit(signed) => signed.body.command.owner(),
			Kind::Cancel(signed) => signed.body.key.owner,
			Kind::Ack(signed) => signed.body.owner,
		}
	}

	/// The command a Submit carries, unverified, for audit records only.
	pub(crate) fn claimed_command(&self) -> Option<&SlabCommand> {
		match &self.0 {
			Kind::Submit(signed) => Some(&signed.body.command),
			Kind::Cancel(_) | Kind::Ack(_) => None,
		}
	}

	/// Test-only: an arbitrary operation signed as its own owner, for apply
	/// tests that have no ticket.
	#[cfg(test)]
	pub(crate) fn submit_for_test(key: &SigningKey, op: SlabOp) -> Self {
		let signer = signer_of(op.command.owner());
		Self(Kind::Submit(evidence::sign(key, op, signer)))
	}

	#[cfg(test)]
	pub(crate) fn cancel_for_test(key: &SigningKey, cancel: CancelOp) -> Self {
		let signer = signer_of(cancel.key.owner);
		Self(Kind::Cancel(evidence::sign(key, cancel, signer)))
	}

	/// Test-only: a Submit's signature presented as a Cancel of the same key
	/// and digest. `None` for another kind.
	#[cfg(test)]
	pub(crate) fn submit_signature_as_cancel_for_test(&self) -> Option<Self> {
		let Kind::Submit(signed) = &self.0 else {
			return None;
		};
		Some(Self(Kind::Cancel(Signed {
			body: CancelOp {
				key: crate::consensus::slab_ops::OpKey {
					owner: signed.body.command.owner(),
					op_seq: signed.body.op_seq,
				},
				target_digest: signed.body.command.digest(),
			},
			signer: signed.signer,
			signature: signed.signature.clone(),
		})))
	}

	/// Test-only: the same signature over an Ack batch with `seq` added.
	#[cfg(test)]
	pub(crate) fn ack_with_added_seq_for_test(&self, seq: u64) -> Option<Self> {
		let Kind::Ack(signed) = &self.0 else {
			return None;
		};
		let mut altered = signed.clone();
		altered.body.seqs.push(seq);
		Some(Self(Kind::Ack(altered)))
	}

	/// Test-only: the same body and signature attributed to `signer`.
	#[cfg(test)]
	pub(crate) fn attributed_to_for_test(&self, signer: Signer) -> Self {
		let mut altered = self.clone();
		match &mut altered.0 {
			Kind::Submit(signed) => signed.signer = signer,
			Kind::Cancel(signed) => signed.signer = signer,
			Kind::Ack(signed) => signed.signer = signer,
		}
		altered
	}
}

impl VerifiedOwnerOp {
	pub(crate) fn owner(&self) -> OwnerRef {
		match &self.0 {
			OwnerOp::Submit(op) => op.command.owner(),
			OwnerOp::Cancel(cancel) => cancel.key.owner,
			OwnerOp::Ack(ack) => ack.owner,
		}
	}

	/// The node whose leadership admits a new effect of this operation. An
	/// owner that follows may publish a slab's first, uncertified tier pointer,
	/// cancel its own operation and acknowledge its outcomes, so for those the
	/// owner's own node stands in. Every other command keeps the rule that its
	/// owner appended it as leader: the answer is `appending_leader` itself.
	pub(crate) fn admitting_leader(&self, appending_leader: u32) -> u32 {
		let follower_surface = match &self.0 {
			OwnerOp::Submit(op) => op.command.is_first_publication(),
			OwnerOp::Cancel(_) | OwnerOp::Ack(_) => true,
		};
		if follower_surface {
			self.owner().node()
		} else {
			appending_leader
		}
	}

	pub(crate) fn into_op(self) -> OwnerOp {
		self.0
	}
}

impl SlabCommand {
	/// A slab's first tier pointer, with no certificate: the one command an
	/// owner may submit while it follows (ADR 0725).
	pub fn is_first_publication(&self) -> bool {
		matches!(
			self,
			Self::Tier(TierOp { tier, replaces: None }) if tier.certified.is_none()
		)
	}
}

fn refusal(refusal: DecisionRefusal, owner: OwnerRef) -> SlabRefusal {
	match refusal {
		DecisionRefusal::SignerMismatch => SlabRefusal::OwnerOpSignerMismatch { owner },
		_ => SlabRefusal::OwnerOpUnverifiable { owner },
	}
}

/// Ed25519 (strict) against the key bound to the owner's exact incarnation,
/// under the operation kind's own domain. Checked before any lookup or effect.
pub(crate) fn verify(
	decisions: &DecisionState,
	signed: &SignedOwnerOp,
) -> Result<VerifiedOwnerOp, SlabRefusal> {
	let owner = signed.claimed_owner();
	let op = match &signed.0 {
		Kind::Submit(signed) => evidence::verify(decisions, signed)
			.map(|verified| OwnerOp::Submit(verified.body().clone())),
		Kind::Cancel(signed) => {
			evidence::verify(decisions, signed).map(|verified| OwnerOp::Cancel(*verified.body()))
		}
		Kind::Ack(signed) => evidence::verify(decisions, signed)
			.map(|verified| OwnerOp::Ack(verified.body().clone())),
	};
	op.map(VerifiedOwnerOp)
		.map_err(|error| refusal(error, owner))
}

/// What the state machine answered an owner operation: its receipt, its
/// acknowledgement, or a typed refusal that recorded nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnerOpAnswer {
	Receipt(crate::consensus::slab_ops::OpReceipt),
	Ack(crate::consensus::slab_ops::AckReceipt),
	Refused(SlabRefusal),
}

/// An owner operation's outcome as its owner must treat it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerOpOutcome {
	/// Committed, with the state machine's answer. This node has not
	/// necessarily applied `log_index` yet.
	Applied {
		answer: OwnerOpAnswer,
		log_index: u64,
	},
	/// Proven not handed to the log.
	NotSubmitted(String),
	/// It may or may not have committed.
	Unknown(String),
}

/// How an owner reaches the log with its own operations while it may be a
/// follower: the proposal path, a linearizable cut, and the local apply wait.
#[async_trait::async_trait]
pub(crate) trait OwnerOpChannel: Send + Sync {
	/// Propose `op`, locally when this node leads and through the leader
	/// otherwise. The caller bounds the attempt.
	async fn propose_owner_op(&self, op: SignedOwnerOp) -> OwnerOpOutcome;

	/// The leader's read index after a linearizable barrier.
	async fn read_cut(&self) -> std::io::Result<u64>;

	/// Waits, bounded, until this node has applied `index`.
	async fn wait_applied(&self, index: u64, bound: std::time::Duration) -> std::io::Result<()>;

	/// Whether a leader is known. A scheduling hint, never authority.
	fn leader_known(&self) -> bool;
}
