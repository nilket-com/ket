//! Operation identity and evidence for slab commands (ADR 0697 §5).
//!
//! Every slab-keyed command travels as one operation of its owner: a key, the
//! command, and the canonical digest of that command. The state machine records
//! each decided operation in its owner's bounded window, so a retry learns what
//! happened instead of guessing, and a decision survives until its owner has
//! acknowledged it. A receipt answers only "what happened to this operation"; it
//! never confers current authority.

use crate::consensus::core::{OwnerRef, SlabRef, TieredSlab};
use crate::schema::{
	ByteCount, CompactionJobId, CompactionLevelIndex, HybridTimestamp, RowCount, SlabId, TenantId,
	TenantKekId,
};
use crate::storage::CompactionCommit;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The fixed per-owner decision window. A live owner holds at most this many
/// outcomes; it says nothing of total memory across arbitrarily many owners.
pub const OPS_WINDOW_W: u64 = 1024;

/// An operation's identity: one sequence per owner across every kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OpKey {
	pub owner: OwnerRef,
	pub op_seq: u64,
}

/// The immutable identity of one tiered object. Advisory tombstone and
/// supersession metadata are deliberately absent: a metadata refresh names the
/// same generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierGeneration {
	pub l2_uri: String,
	pub payload_hash: [u8; 32],
	pub is_encrypted: bool,
	pub tenant_id: Option<TenantId>,
	pub tenant_kek_id: Option<TenantKekId>,
	pub stats_format_version: Option<u16>,
	pub stats_block_len: Option<u32>,
	pub stats_block_hash128: Option<[u8; 16]>,
}

impl From<&TieredSlab> for TierGeneration {
	fn from(tier: &TieredSlab) -> Self {
		// Exhaustive: a new field must be classified as identity or advisory.
		let TieredSlab {
			slab: _,
			l2_uri,
			payload_hash,
			is_encrypted,
			tenant_id,
			tenant_kek_id,
			stats_format_version,
			stats_block_len,
			stats_block_hash128,
			tombstone_metadata: _,
			supersession_metadata: _,
		} = tier;
		Self {
			l2_uri: l2_uri.clone(),
			payload_hash: *payload_hash,
			is_encrypted: *is_encrypted,
			tenant_id: *tenant_id,
			tenant_kek_id: *tenant_kek_id,
			stats_format_version: *stats_format_version,
			stats_block_len: *stats_block_len,
			stats_block_hash128: *stats_block_hash128,
		}
	}
}

/// Install or replace a slab's tier pointer. `replaces` names the generation
/// this proposer saw, `None` for a slab with no pointer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierOp {
	pub tier: TieredSlab,
	pub replaces: Option<TierGeneration>,
}

/// Retire exactly the generation this proposer saw.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireOp {
	pub slab: SlabRef,
	pub expected: TierGeneration,
}

/// The run a GC removal was reserved against, for the owner's own exact check.
/// These are run statistics, not content identity: local input authority stays
/// with the linked local intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunIdentity {
	pub compaction_level: CompactionLevelIndex,
	pub size_bytes: ByteCount,
	pub row_count: RowCount,
	pub min_row_hlc: HybridTimestamp,
	pub max_row_hlc: HybridTimestamp,
}

impl RunIdentity {
	pub fn of(run: &crate::storage::LsmRunSummary) -> Self {
		Self {
			compaction_level: run.compaction_level,
			size_bytes: run.size_bytes,
			row_count: run.row_count,
			min_row_hlc: run.min_row_hlc,
			max_row_hlc: run.max_row_hlc,
		}
	}
}

/// Remove a delete-pending run. Built whole when its mark is allocated and
/// immutable after, so it carries no manifest epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcRemove {
	/// The job its mark claimed the slab for: this removal is T1's Complete.
	pub job_id: CompactionJobId,
	pub slab: SlabRef,
	pub expected_run: RunIdentity,
}

/// T1's claim, for a slab with no manifest run for a mark to carry it. Once
/// accepted, a TierSlab of the slab is rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimOp {
	pub slab: SlabRef,
	pub job_id: CompactionJobId,
}

/// T1's terminal acknowledgement of a `ClaimOp`: claimed moves to removed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteOp {
	pub slab: SlabRef,
	pub job_id: CompactionJobId,
}

/// A reservation durably known never to have been exposed goes straight to
/// removed (T2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnOp {
	pub slab: SlabRef,
}

/// Exact, coalesced sets of one owner's slab ids. Inclusive, so the top id needs
/// no one-past-the-end. Never expanded id by id, and no bound on its size is
/// claimed. It exists only beneath an owner, in [`OwnerTerminal`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlabIntervals(Vec<(u64, u64)>);

impl SlabIntervals {
	pub fn contains(&self, slab: SlabId) -> bool {
		let id = slab.get();
		let after = self.0.partition_point(|(first, _)| *first <= id);
		after > 0 && self.0[after - 1].1 >= id
	}

	pub fn insert(&mut self, slab: SlabId) {
		let id = slab.get();
		let after = self.0.partition_point(|(first, _)| *first <= id);
		if after > 0 && self.0[after - 1].1 >= id {
			return;
		}
		let joins_before = after > 0 && self.0[after - 1].1.checked_add(1) == Some(id);
		let joins_after = after < self.0.len() && id.checked_add(1) == Some(self.0[after].0);
		match (joins_before, joins_after) {
			(true, true) => {
				self.0[after - 1].1 = self.0[after].1;
				self.0.remove(after);
			}
			(true, false) => self.0[after - 1].1 = id,
			(false, true) => self.0[after].0 = id,
			(false, false) => self.0.insert(after, (id, id)),
		}
	}

	/// Ordered, each interval well formed, and neither overlapping nor adjacent
	/// to the next: the one encoding of this set.
	pub fn is_canonical(&self) -> bool {
		self.0.iter().all(|(first, last)| first <= last)
			&& self.0.windows(2).all(|pair| {
				pair[0]
					.1
					.checked_add(1)
					.is_some_and(|next| next < pair[1].0)
			})
	}

	pub fn intervals(&self) -> &[(u64, u64)] {
		&self.0
	}

	#[cfg(test)]
	pub(crate) fn from_raw_for_test(intervals: Vec<(u64, u64)>) -> Self {
		Self(intervals)
	}
}

/// The terminal half of one owner's slab namespace (ADR 0697 §4). Its ids are
/// qualified by the key they sit under, and this is pairwise disjoint with that
/// owner's tiered slabs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerTerminal {
	pub claimed: BTreeMap<SlabId, CompactionJobId>,
	pub removed: SlabIntervals,
}

/// Every command that names a slab. Its owner has one source, the command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlabCommand {
	Tier(TierOp),
	Retire(RetireOp),
	Compaction(CompactionCommit),
	GcRemove(GcRemove),
	Claim(ClaimOp),
	Complete(CompleteOp),
	BurnReserved(BurnOp),
}

const DIGEST_DOMAIN: &[u8] = b"wendelon.slab-op.v1";
const DIGEST_KIND_TIER: u8 = 1;
const DIGEST_KIND_RETIRE: u8 = 2;
const DIGEST_KIND_COMPACTION: u8 = 3;
const DIGEST_KIND_GC_REMOVE: u8 = 4;
const DIGEST_KIND_CLAIM: u8 = 5;
const DIGEST_KIND_COMPLETE: u8 = 6;
const DIGEST_KIND_BURN: u8 = 7;

fn payload_digest<T: Serialize>(kind: u8, payload: &T) -> [u8; 32] {
	// These are plain structs with deterministic field order; postcard cannot
	// fail to encode them into a Vec.
	let bytes = postcard::to_stdvec(payload).expect("slab command payload encodes");
	let mut hasher = blake3::Hasher::new();
	hasher.update(DIGEST_DOMAIN);
	hasher.update(&[kind]);
	hasher.update(&bytes);
	*hasher.finalize().as_bytes()
}

impl SlabCommand {
	pub fn owner(&self) -> OwnerRef {
		match self {
			Self::Tier(op) => op.tier.slab.owner(),
			Self::Retire(op) => op.slab.owner(),
			Self::Compaction(commit) => commit.owner,
			Self::GcRemove(remove) => remove.slab.owner(),
			Self::Claim(op) => op.slab.owner(),
			Self::Complete(op) => op.slab.owner(),
			Self::BurnReserved(op) => op.slab.owner(),
		}
	}

	/// The canonical digest: the domain, a fixed kind byte, and the postcard
	/// encoding of the inner payload, so adding a variant changes no existing
	/// digest. The proposer and the state machine share this one function.
	pub fn digest(&self) -> [u8; 32] {
		match self {
			Self::Tier(op) => payload_digest(DIGEST_KIND_TIER, op),
			Self::Retire(op) => payload_digest(DIGEST_KIND_RETIRE, op),
			Self::Compaction(commit) => payload_digest(DIGEST_KIND_COMPACTION, commit),
			Self::GcRemove(remove) => payload_digest(DIGEST_KIND_GC_REMOVE, remove),
			Self::Claim(op) => payload_digest(DIGEST_KIND_CLAIM, op),
			Self::Complete(op) => payload_digest(DIGEST_KIND_COMPLETE, op),
			Self::BurnReserved(op) => payload_digest(DIGEST_KIND_BURN, op),
		}
	}
}

/// The payload of a request variant that no longer exists. It has no value, so
/// the variant cannot be constructed; decoding one fails as unsupported rather
/// than manufacturing a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetiredSlabCommand {}

impl Serialize for RetiredSlabCommand {
	fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
		match *self {}
	}
}

impl<'de> Deserialize<'de> for RetiredSlabCommand {
	fn deserialize<D: serde::Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
		Err(serde::de::Error::custom(
			"unsupported: a retired unkeyed slab command (ADR 0697)",
		))
	}
}

/// Payload of a `SlabOp` Raft entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlabOp {
	pub op_seq: u64,
	pub command: SlabCommand,
}

/// Close an operation: `target_digest` is the original command's digest, from
/// the local intent. Decided the same way as the original (§5.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOp {
	pub key: OpKey,
	pub target_digest: [u8; 32],
}

/// The owner has durably finished these operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckOutcomes {
	pub owner: OwnerRef,
	pub seqs: Vec<u64>,
}

/// Why an operation applied nothing. A definite decision about this one
/// operation, and only this one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlabRejection {
	/// A retirement of a slab with no pointer.
	NotTiered,
	/// The pointer is not the generation the proposer saw.
	GenerationMismatch {
		expected: Option<TierGeneration>,
		current: Option<TierGeneration>,
	},
	/// A compaction that changed the visible domain.
	VisibleDomainMismatch,
	/// Closed by `CancelOp` before the original was decided.
	Cancelled,
	/// A terminal transition of a slab that holds a tier pointer.
	Tiered,
	/// Claimed by another job, or a tier of a claimed slab.
	ClaimedBy { job_id: CompactionJobId },
	/// The slab's identity has ended; nothing names it again.
	Removed,
	/// A Complete whose job does not hold the claim.
	NotClaimedBy { current: Option<CompactionJobId> },
}

/// A decision as the state machine recorded it, at its original index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordedOutcome {
	Applied { index: u64 },
	Rejected { reason: SlabRejection, index: u64 },
}

/// What a request learned about its operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpOutcome {
	/// The recorded decision, unchanged, at its original index.
	Recorded(RecordedOutcome),
	/// The key was decided under another digest. Unrecorded, with no index of
	/// its own: an anomaly that grants no cleanup.
	Conflict,
	/// At or below the acknowledged prefix: pruned, or a stale duplicate.
	/// Unrecorded and ambiguous.
	Unavailable,
}

/// An operation's answer, echoing the request's key and digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpReceipt {
	pub key: OpKey,
	pub digest: [u8; 32],
	pub outcome: OpOutcome,
}

/// An acknowledgement's answer, sequence by sequence, correlated to its owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckReceipt {
	pub owner: OwnerRef,
	pub accepted: Vec<u64>,
	pub not_decided: Vec<u64>,
	pub outside_window: Vec<u64>,
	pub acked_through: u64,
}

/// A response carrying operation evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpResponse {
	Receipt(OpReceipt),
	Ack(AckReceipt),
}

/// One owner's decision window.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpTable {
	/// The acknowledged contiguous prefix.
	pub acked_through: u64,
	/// Decisions in `(acked_through, acked_through + W]`.
	pub outcomes: BTreeMap<u64, OpEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpEntry {
	pub digest: [u8; 32],
	pub outcome: RecordedOutcome,
	pub acked: bool,
}

impl OpTable {
	/// Whether `op_seq` is past the window. Saturating: a prefix near the top
	/// of the sequence space still refuses nothing below it.
	pub fn outside_window(&self, op_seq: u64) -> bool {
		op_seq > self.acked_through.saturating_add(OPS_WINDOW_W)
	}

	/// Mark `seqs` acknowledged where they are decided, and advance the prefix
	/// over the maximal contiguous acked run. Only here is anything pruned, and
	/// the prefix never crosses an absent, unacked or undecided sequence.
	pub fn acknowledge(&mut self, seqs: &[u64]) -> (Vec<u64>, Vec<u64>, Vec<u64>) {
		let (mut accepted, mut not_decided, mut outside_window) =
			(Vec::new(), Vec::new(), Vec::new());
		for &seq in seqs {
			if seq <= self.acked_through {
				accepted.push(seq);
			} else if self.outside_window(seq) {
				outside_window.push(seq);
			} else if let Some(entry) = self.outcomes.get_mut(&seq) {
				entry.acked = true;
				accepted.push(seq);
			} else {
				not_decided.push(seq);
			}
		}
		while let Some(next) = self.acked_through.checked_add(1)
			&& self.outcomes.get(&next).is_some_and(|entry| entry.acked)
		{
			self.outcomes.remove(&next);
			self.acked_through = next;
		}
		(accepted, not_decided, outside_window)
	}
}
