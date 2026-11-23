//! DC-287 (Codex 01a208b1): an opt-in, bounded record of each ParentDecisions table write attempt
//! whose answer is WRITE_STALLED, taken where the coordinator decides that stall and published
//! only when that stall is the attempt's response (a deadline answered first publishes
//! nothing). Covered: the ordinary table write; KV writes and CTAS chunks are not traced.
//! Observation only: it changes no answer, clock or policy. A node without it (the default)
//! takes no clock reading and records nothing.
//!
//! Each record carries the write's identity, the wire attempts remaining, the exact return site,
//! the typed answer the site acted on, and the monotonic time since the coordinator's entry. A
//! Finalize stall also carries what the local holder's own child write answered, when one was
//! observed. The ring keeps the last [`CAPACITY`] records; the total ever recorded is kept beside
//! it, so a record that was overwritten is counted, never silently absent.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::consensus::decisions::{DecisionRefusal, DecisionResult, Lookup};
use crate::network::decision_proposals::{
	LookupResponse, ProposalOutcome, ProposalRefusal, ProposalResponse,
};
use crate::schema::{TableWriteRequestMeta, WriteStatus};

use super::coordinator::Settled;

use super::child_writes::ChildRefusal;

/// The records a trace keeps; older ones are overwritten and counted.
pub(crate) const CAPACITY: usize = 16;

/// Where `build_claim` found no basis for a claim (each is a WRITE_STALLED return there).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClaimCheck {
	/// The applied state has no decision state.
	NoDecisions,
	/// No lineage covers the first row's position.
	FirstLineage,
	/// No epoch is recorded.
	Epoch,
	/// The tenant is not in the applied state.
	Tenant,
	/// No replica segment covers a row's position.
	Segment,
	/// No lineage covers a child's lowest position.
	ChildLineage,
	/// A planned replica has no current incarnation.
	Plan,
	/// No child holds the request's first row.
	Coordinator,
}

/// The exact return site of a stalled coordinated write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum StallSite {
	/// A recorded noop status under a write's key.
	ResumeNoop = 1,
	/// A Reserved parent whose recorded coordinator child is missing.
	ResumeNoCoordinatorChild = 2,
	/// A Reserved parent of another coordinator, with no open, leased route to it.
	ResumeUnroutable = 3,
	/// A Reserved parent of this node whose incarnation or lineage authority has ended.
	ResumeAuthorityEnded = 4,
	/// A claim redirect for a request with no positions.
	ClaimNoPosition = 5,
	/// A claim redirect whose range authority is unavailable (no leased holder).
	ClaimRangeUnavailable = 6,
	ClaimNoDecisions = 7,
	ClaimFirstLineage = 8,
	ClaimEpoch = 9,
	ClaimTenant = 10,
	ClaimSegment = 11,
	ClaimChildLineage = 12,
	ClaimPlan = 13,
	ClaimCoordinator = 14,
	/// The Reserve proposal was not applied as Reserved or Terminal.
	Reserve = 15,
	/// The lookup was not answered from the record (or was absent after this attempt reserved).
	Lookup = 16,
	/// The Finalize proposal was not applied as Terminal.
	Finalize = 17,
}

impl StallSite {
	pub(crate) fn of_claim(check: ClaimCheck) -> Self {
		match check {
			ClaimCheck::NoDecisions => Self::ClaimNoDecisions,
			ClaimCheck::FirstLineage => Self::ClaimFirstLineage,
			ClaimCheck::Epoch => Self::ClaimEpoch,
			ClaimCheck::Tenant => Self::ClaimTenant,
			ClaimCheck::Segment => Self::ClaimSegment,
			ClaimCheck::ChildLineage => Self::ClaimChildLineage,
			ClaimCheck::Plan => Self::ClaimPlan,
			ClaimCheck::Coordinator => Self::ClaimCoordinator,
		}
	}
}

/// The kind of answer a stall site acted on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum AnswerKind {
	/// The site decided from applied state (or a route read); no proposal or lookup answered.
	State = 0,
	NotLeader = 1,
	Refused = 2,
	Indeterminate = 3,
	/// Applied, with a decision result.
	AppliedDecision = 4,
	/// Applied, with another outcome.
	AppliedOther = 5,
	/// Any other proposal answer (CTAS resolution, owner answers).
	OtherProposal = 6,
	/// A lookup found something other than a record this site acts on.
	Found = 7,
}

/// The typed answer a stall site acted on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StallAnswer {
	pub(crate) kind: AnswerKind,
	pub(crate) decision: Option<DecisionResult>,
	pub(crate) proposal_refusal: Option<ProposalRefusal>,
	/// For [`AnswerKind::Found`]: what the lookup found, as [`lookup_code`].
	pub(crate) lookup: Option<u8>,
}

impl StallAnswer {
	pub(crate) const STATE: Self = Self {
		kind: AnswerKind::State,
		decision: None,
		proposal_refusal: None,
		lookup: None,
	};

	/// A lookup that found the key's record (the resume sites act on it).
	pub(crate) const RECORDED: Self = Self {
		kind: AnswerKind::Found,
		decision: None,
		proposal_refusal: None,
		lookup: Some(3),
	};

	pub(crate) fn of_proposal(response: &ProposalResponse) -> Self {
		let (kind, decision, proposal_refusal) = match response {
			ProposalResponse::Applied {
				outcome: ProposalOutcome::Decision(result),
				..
			} => (AnswerKind::AppliedDecision, Some(*result), None),
			ProposalResponse::Applied { .. } => (AnswerKind::AppliedOther, None, None),
			ProposalResponse::NotLeader { .. } => (AnswerKind::NotLeader, None, None),
			ProposalResponse::Refused(refusal) => (AnswerKind::Refused, None, Some(*refusal)),
			ProposalResponse::Indeterminate(_) => (AnswerKind::Indeterminate, None, None),
			_ => (AnswerKind::OtherProposal, None, None),
		};
		Self {
			kind,
			decision,
			proposal_refusal,
			lookup: None,
		}
	}

	pub(crate) fn of_lookup(response: &LookupResponse) -> Self {
		let (kind, proposal_refusal, lookup) = match response {
			LookupResponse::Found(found) => (AnswerKind::Found, None, Some(lookup_code(found))),
			LookupResponse::NotLeader { .. } => (AnswerKind::NotLeader, None, None),
			LookupResponse::Refused(refusal) => (AnswerKind::Refused, Some(*refusal), None),
			LookupResponse::Indeterminate(_) => (AnswerKind::Indeterminate, None, None),
		};
		Self {
			kind,
			decision: None,
			proposal_refusal,
			lookup,
		}
	}

	/// The decision's refusal, when the answer is an applied refusal.
	pub(crate) fn decision_refusal(&self) -> Option<DecisionRefusal> {
		match self.decision {
			Some(DecisionResult::Refused(refusal)) => Some(refusal),
			_ => None,
		}
	}
}

/// What a lookup found: 1 absent and eligible, 2 absent and not eligible, 3 recorded, 4 conflict,
/// 5 invalid identity, 6 noop, 7 settled.
pub(crate) fn lookup_code(found: &Lookup) -> u8 {
	match found {
		Lookup::Absent { eligible: true } => 1,
		Lookup::Absent { eligible: false } => 2,
		Lookup::Recorded { .. } => 3,
		Lookup::Conflict => 4,
		Lookup::InvalidIdentity => 5,
		Lookup::Noop { .. } => 6,
		Lookup::Settled { .. } => 7,
	}
}

/// What the local holders of a Finalize's children answered for their own child writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HolderSeen {
	/// The first local holder refusal observed, as [`ChildRefusal::code`].
	pub(crate) refusal: Option<u8>,
	/// Whether a local holder wrote its own child.
	pub(crate) acked: bool,
}

impl HolderSeen {
	pub(crate) fn refused(&mut self, refusal: &ChildRefusal) {
		self.refusal.get_or_insert(refusal.code());
	}

	/// Exported: 1 unknown (no local holder answer observed), 2 a local holder wrote its child
	/// and none refused, 3 + [`ChildRefusal::code`] the first local holder refusal.
	fn code(&self) -> u64 {
		match (self.refusal, self.acked) {
			(Some(code), _) => 3 + u64::from(code),
			(None, true) => 2,
			(None, false) => 1,
		}
	}
}

/// One stalled attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteStallRecord {
	/// 1 for the trace's first record.
	pub(crate) seq: u64,
	pub(crate) client_uuid: [u8; 16],
	pub(crate) sequence_no: u64,
	pub(crate) attempts_remaining: u8,
	pub(crate) site: StallSite,
	pub(crate) answer: StallAnswer,
	/// Finalize only: the local holder answers; `None` elsewhere.
	pub(crate) holder: Option<HolderSeen>,
	/// Finalize only: the acknowledgements it carried.
	pub(crate) acks: u32,
	/// Monotonic, from the coordinator's entry to this answer.
	pub(crate) elapsed: Duration,
	/// Wall clock at this answer.
	pub(crate) at_unix_ms: u64,
}

impl WriteStallRecord {
	/// The exported fields. Enum indices are their serialized variant indices plus one (0 is
	/// none): `decision` of `DecisionResult`, `decision_refusal` of `DecisionRefusal`,
	/// `proposal_refusal` of `ProposalRefusal`; `holder` is 0 off Finalize.
	///
	/// The identity travels in 32-bit words (`client_uuid_w0`..`w3`, big-endian; `sequence_no_hi`
	/// and `_lo`), so a reader that parses sample values as f64 keeps it exactly (Codex 01a208cb:
	/// a 64-bit half rounds above 2^53).
	pub(crate) fn fields(&self) -> [(&'static str, u64); 18] {
		let word = |at: usize| {
			u64::from(u32::from_be_bytes(
				self.client_uuid[at..at + 4].try_into().expect("four bytes"),
			))
		};
		[
			("seq", self.seq),
			("client_uuid_w0", word(0)),
			("client_uuid_w1", word(4)),
			("client_uuid_w2", word(8)),
			("client_uuid_w3", word(12)),
			("sequence_no_hi", self.sequence_no >> 32),
			("sequence_no_lo", self.sequence_no & u64::from(u32::MAX)),
			("attempts_remaining", u64::from(self.attempts_remaining)),
			("site", self.site as u64),
			("answer", self.answer.kind as u64),
			(
				"decision",
				self.answer.decision.map_or(0, |d| 1 + variant_index(&d)),
			),
			(
				"decision_refusal",
				self.answer
					.decision_refusal()
					.map_or(0, |r| 1 + variant_index(&r)),
			),
			(
				"proposal_refusal",
				self.answer
					.proposal_refusal
					.map_or(0, |r| 1 + variant_index(&r)),
			),
			("lookup", self.answer.lookup.map_or(0, u64::from)),
			("holder", self.holder.map_or(0, |seen| seen.code())),
			("acks", u64::from(self.acks)),
			(
				"elapsed_us",
				u64::try_from(self.elapsed.as_micros()).unwrap_or(u64::MAX),
			),
			("at_unix_ms", self.at_unix_ms),
		]
	}
}

/// An enum value's serialized variant index (postcard writes it first, as a varint). The
/// replicated enums this reads are persisted, so their order is already fixed.
fn variant_index<T: serde::Serialize>(value: &T) -> u64 {
	let Ok(bytes) = postcard::to_stdvec(value) else {
		return u64::MAX;
	};
	let mut index = 0u64;
	for (shift, byte) in bytes.iter().take(5).enumerate() {
		index |= u64::from(byte & 0x7f) << (7 * shift);
		if byte & 0x80 == 0 {
			return index;
		}
	}
	u64::MAX
}

/// A snapshot of a trace: the total ever recorded and the records kept, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WriteStallSnapshot {
	pub(crate) recorded: u64,
	pub(crate) records: Vec<WriteStallRecord>,
}

impl WriteStallSnapshot {
	/// Records no longer kept.
	pub(crate) fn overwritten(&self) -> u64 {
		self.recorded - self.records.len() as u64
	}
}

#[derive(Default)]
struct Ring {
	recorded: u64,
	slots: [Option<WriteStallRecord>; CAPACITY],
}

/// One node's trace.
pub(crate) struct WriteStallTrace {
	node: u32,
	ring: std::sync::Mutex<Ring>,
}

impl WriteStallTrace {
	pub(crate) fn new(node: u32) -> Arc<Self> {
		Arc::new(Self {
			node,
			ring: Default::default(),
		})
	}

	pub(crate) fn node(&self) -> u32 {
		self.node
	}

	fn push(&self, mut record: WriteStallRecord) {
		let mut ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
		ring.recorded += 1;
		record.seq = ring.recorded;
		let slot = ((record.seq - 1) % CAPACITY as u64) as usize;
		ring.slots[slot] = Some(record);
	}

	pub(crate) fn snapshot(&self) -> WriteStallSnapshot {
		let ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
		let mut records: Vec<WriteStallRecord> = ring.slots.iter().flatten().copied().collect();
		records.sort_by_key(|record| record.seq);
		WriteStallSnapshot {
			recorded: ring.recorded,
			records,
		}
	}
}

impl super::WendelonDataServer {
	/// DC-287 (observation only): when `enabled`, this node keeps a bounded record of each
	/// coordinated write attempt it answers WRITE_STALLED, published in its metrics.
	pub(crate) fn with_write_stall_trace(mut self, enabled: bool) -> Self {
		self.decisions.write_stall_trace = enabled.then(|| {
			let trace = WriteStallTrace::new(self.local_node.id);
			crate::metrics::global().publish_write_stall_trace(&trace);
			trace
		});
		self
	}

	/// DC-287: this node's stalled-write record, if it keeps one.
	#[cfg(test)]
	pub(crate) fn write_stall_trace(&self) -> Option<&Arc<WriteStallTrace>> {
		self.decisions.write_stall_trace.as_ref()
	}
}

/// One coordinated table write attempt's handle on its node's trace: a monotonic reading at
/// entry, taken only when the node traces. A stall the attempt decides is held here, with its
/// time at that decision, and reaches the trace only through [`Self::publish_if_answered`]:
/// only when that stall is the attempt's actual answer (Codex 01a208cb: after its caller's
/// deadline answered, an owned attempt is still polled to its end and its result discarded).
pub(crate) struct AttemptTrace<'a> {
	trace: Option<(&'a WriteStallTrace, Instant)>,
	meta: &'a TableWriteRequestMeta,
	held: std::sync::OnceLock<WriteStallRecord>,
}

impl<'a> AttemptTrace<'a> {
	pub(crate) fn enter(
		trace: Option<&'a WriteStallTrace>,
		meta: &'a TableWriteRequestMeta,
	) -> Self {
		Self {
			trace: trace.map(|trace| (trace, Instant::now())),
			meta,
			held: std::sync::OnceLock::new(),
		}
	}

	/// An attempt this node does not trace (a KV write's synthesized table metadata, a CTAS chunk).
	pub(crate) fn untraced(meta: &'a TableWriteRequestMeta) -> Self {
		Self::enter(None, meta)
	}

	/// Publishes the held stall when `answered`, the attempt's own result that became its
	/// response, is that stall. A deadline answer (`None`) publishes nothing.
	pub(crate) fn publish_if_answered(self, answered: Option<Settled>) {
		let (Some((trace, _)), Some(record)) = (self.trace, self.held.into_inner()) else {
			return;
		};
		if answered == Some(Settled::Indeterminate(WriteStatus::WRITE_STALLED)) {
			trace.push(record);
		}
	}

	/// The stalled answer from `site`, held when this node traces; `answer` is read only then.
	pub(crate) fn stall(&self, site: StallSite, answer: impl FnOnce() -> StallAnswer) -> Settled {
		self.record(site, answer, None, 0);
		Settled::Indeterminate(WriteStatus::WRITE_STALLED)
	}

	/// The stalled answer of a Finalize, with what the local holders answered and the
	/// acknowledgements it carried.
	pub(crate) fn stall_finalize(
		&self,
		answer: impl FnOnce() -> StallAnswer,
		holder: HolderSeen,
		acks: usize,
	) -> Settled {
		self.record(StallSite::Finalize, answer, Some(holder), acks);
		Settled::Indeterminate(WriteStatus::WRITE_STALLED)
	}

	fn record(
		&self,
		site: StallSite,
		answer: impl FnOnce() -> StallAnswer,
		holder: Option<HolderSeen>,
		acks: usize,
	) {
		let Some((_, entered)) = self.trace else {
			return;
		};
		let answer = answer();
		// One stall per attempt: each is returned as soon as it is decided.
		let _ = self.held.set(WriteStallRecord {
			seq: 0,
			client_uuid: self.meta.parent_write_id.client_uuid,
			sequence_no: self.meta.parent_write_id.sequence_no,
			attempts_remaining: self.meta.attempts_remaining(),
			site,
			answer,
			holder,
			acks: u32::try_from(acks).unwrap_or(u32::MAX),
			elapsed: entered.elapsed(),
			at_unix_ms: std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.map_or(0, |since| {
					u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
				}),
		});
	}
}

#[cfg(test)]
#[path = "write_stall_trace/tests.rs"]
mod tests;
