//! Wendelon ADR 0127: a traced holder lease refusal records what its own reads saw.
//!
//! Opt-in with the node's `write_stall_trace`. Two halves:
//! - the **issue witness** (renewal side): each `AcquireLeases` this leader is about to hand to
//!   `client_write`, one slot per target, with its request HLC, the digest of the complete
//!   signed descriptor and an outcome written by the same issue's ticket;
//! - the **capture** (holder side): what the refusing holder's own lease view read, taken inside
//!   that read, and the **evidence** built from it at the site that refused.
//!
//! Nothing here changes a lease decision: the holder's reads, their order, the sampled time,
//! the HLC predicate and verification are today's; the evidence only records them. Its claims
//! are narrow (ADR 0127 D3): row facts from the refusing read, and, when the witness allows,
//! agreement with a retained issue or a later witnessed issue, never a unique provenance, a
//! commit, an append or an apply instant.

use super::*;
use crate::schema::{HybridTimestamp, LeaseDescriptor};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// The witness's retained issues.
pub(crate) const WITNESS_CAPACITY: usize = 8;
/// The most rows of this holder a capture copies (ADR 0127 D2: the data-path bound).
pub(crate) const CAPTURED_ROWS: usize = 2;

/// A hybrid timestamp's three parts, compared as the HLC is (physical, logical, node).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Hlc {
	pub(crate) physical_ms: u64,
	pub(crate) logical: u32,
	pub(crate) node_id: u32,
}

impl From<HybridTimestamp> for Hlc {
	fn from(hlc: HybridTimestamp) -> Self {
		Self {
			physical_ms: hlc.physical_ms,
			logical: hlc.logical,
			node_id: hlc.node_id,
		}
	}
}

/// The blake3 digest of a complete signed descriptor (every field, its signature included).
pub(crate) fn descriptor_digest(lease: &LeaseDescriptor) -> [u8; 32] {
	*blake3::hash(bytemuck::bytes_of(lease)).as_bytes()
}

/// ADR 0127 D1: how an issue ended, from the actual hook.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum IssueOutcome {
	/// Reached the `client_write` call site. Not a claim of submission to Raft.
	CallSiteReached = 1,
	/// A validated in-budget results vector granted this target.
	Granted = 2,
	/// A validated in-budget results vector did not grant this target.
	NotGranted = 3,
	/// `client_write` failed, or its answer was not a valid results vector.
	Error = 4,
	/// The pass's budget ended before an answer was observed.
	Unanswered = 5,
}

/// One witnessed issue, one per target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IssueSlot {
	pub(crate) ticket: u64,
	pub(crate) range_start: u64,
	pub(crate) range_end: u64,
	pub(crate) epoch: u64,
	pub(crate) issuer: u32,
	pub(crate) request: Hlc,
	pub(crate) digest: [u8; 32],
	pub(crate) outcome: IssueOutcome,
	pub(crate) outcome_ticket: u64,
}

#[derive(Default)]
struct WitnessRing {
	slots: [Option<IssueSlot>; WITNESS_CAPACITY],
	next: usize,
	/// The highest ticket ever overwritten (0: none).
	evicted_through: u64,
}

/// What the witness held at an observation's cutoff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WitnessSnapshot {
	pub(crate) cutoff: u64,
	/// The lowest ticket this snapshot can still speak for: everything below may be evicted.
	pub(crate) complete_from: u64,
	/// The retained issues ticketed before the cutoff, each with its outcome as of the cutoff.
	pub(crate) slots: Vec<IssueSlot>,
}

/// ADR 0127 D1: the issue witness. One ordering counter: issues and outcomes take their
/// tickets while holding the ring's mutex and publish in the same critical section; an
/// observation takes its ticket without it. The mutex is never held across a Raft call or a
/// state read.
pub(crate) struct LeaseWitness {
	tickets: AtomicU64,
	ring: Mutex<WitnessRing>,
	/// Test-only: a one-shot hold inside the next issue's critical section.
	#[cfg(test)]
	pub(crate) hold_next_issue: Mutex<Option<std::sync::Arc<std::sync::Barrier>>>,
}

impl Default for LeaseWitness {
	fn default() -> Self {
		Self {
			tickets: AtomicU64::new(1),
			ring: Mutex::new(WitnessRing::default()),
			#[cfg(test)]
			hold_next_issue: Mutex::new(None),
		}
	}
}

impl LeaseWitness {
	/// One slot per lease, immediately before the batch is handed to `client_write`: the
	/// tickets and targets of what was issued.
	pub(crate) fn issue(&self, leases: &[LeaseDescriptor]) -> Vec<(u64, u64)> {
		let mut ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
		let issued = leases
			.iter()
			.map(|lease| {
				let ticket = self.tickets.fetch_add(1, Ordering::SeqCst);
				let at = ring.next;
				ring.next = (at + 1) % WITNESS_CAPACITY;
				if let Some(old) = ring.slots[at] {
					ring.evicted_through = ring.evicted_through.max(old.ticket);
				}
				ring.slots[at] = Some(IssueSlot {
					ticket,
					range_start: lease.range_start,
					range_end: lease.range_end,
					epoch: lease.epoch,
					issuer: lease.issuer.id,
					request: lease.lease_start.into(),
					digest: descriptor_digest(lease),
					outcome: IssueOutcome::CallSiteReached,
					outcome_ticket: ticket,
				});
				(ticket, lease.range_start)
			})
			.collect();
		#[cfg(test)]
		{
			let hold = self
				.hold_next_issue
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.take();
			if let Some(barrier) = hold {
				barrier.wait();
				barrier.wait();
			}
		}
		drop(ring);
		issued
	}

	/// The terminal outcome of each issued target. A slot is updated only while it still holds
	/// the same ticket AND target: a reused slot's new occupant is never touched.
	pub(crate) fn finish(&self, issued: &[(u64, u64)], outcome: impl Fn(u64) -> IssueOutcome) {
		if issued.is_empty() {
			return;
		}
		let mut ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
		for &(ticket, range_start) in issued {
			let outcome = outcome(range_start);
			if let Some(slot) = ring
				.slots
				.iter_mut()
				.flatten()
				.find(|slot| slot.ticket == ticket && slot.range_start == range_start)
			{
				slot.outcome = outcome;
				slot.outcome_ticket = self.tickets.fetch_add(1, Ordering::SeqCst);
			}
		}
	}

	/// An observation's cutoff ticket.
	pub(crate) fn observe(&self) -> u64 {
		self.tickets.fetch_add(1, Ordering::SeqCst)
	}

	/// What the witness held at `cutoff`, or `None` when its lock is contended (unknown).
	pub(crate) fn snapshot(&self, cutoff: u64) -> Option<WitnessSnapshot> {
		let ring = self.ring.try_lock().ok()?;
		let slots = ring
			.slots
			.iter()
			.flatten()
			.filter(|slot| slot.ticket < cutoff)
			.map(|slot| {
				let mut slot = *slot;
				if slot.outcome_ticket >= cutoff {
					slot.outcome = IssueOutcome::CallSiteReached;
				}
				slot
			})
			.collect();
		Some(WitnessSnapshot {
			cutoff,
			complete_from: ring.evicted_through + 1,
			slots,
		})
	}

	/// Test-only: every retained slot as it is now.
	#[cfg(test)]
	pub(crate) fn slots_for_test(&self) -> Vec<IssueSlot> {
		let ring = self.ring.lock().unwrap();
		ring.slots.iter().flatten().copied().collect()
	}
}

/// ADR 0127 D2: whether verification ran on a captured row, and its actual result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum VerifyState {
	/// The read's filter dropped the row, so verification did not run on it.
	#[default]
	NotEvaluated = 1,
	Verified = 2,
	Failed = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CapturedRow {
	pub(crate) lease: LeaseDescriptor,
	/// The read's own filter: current epoch and `HLC(now, 0, 0) < lease_expiry`.
	pub(crate) filter_live: bool,
	pub(crate) verify: VerifyState,
}

/// ADR 0127 D2: what one holder lease view read, captured inside that read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LeaseCapture {
	pub(crate) cutoff: u64,
	pub(crate) now_ms: u64,
	pub(crate) e_view: Option<u64>,
	pub(crate) applied_index: Option<u64>,
	pub(crate) target: (u64, u64),
	pub(crate) rows: [Option<CapturedRow>; CAPTURED_ROWS],
	pub(crate) truncated: bool,
	pub(crate) overlapping: u8,
}

impl LeaseCapture {
	/// The verification loop's actual result for a row the read returned.
	pub(crate) fn note_verify(&mut self, lease: &LeaseDescriptor, ok: bool) {
		for row in self.rows.iter_mut().flatten() {
			if row.lease == *lease {
				row.verify = if ok {
					VerifyState::Verified
				} else {
					VerifyState::Failed
				};
			}
		}
	}
}

/// Inside the lease view's read: today's rows (exactly [`super::child_writes::applied_leases`])
/// and the capture of this holder's rows overlapping `target`, at most [`CAPTURED_ROWS`].
pub(crate) fn applied_leases_captured(
	state: &SmState,
	local: u32,
	now_ms: u64,
	target: (u64, u64),
	cutoff: u64,
) -> (Vec<LeaseDescriptor>, LeaseCapture) {
	let e_view = state
		.epoch_history
		.last_key_value()
		.map(|(_, config)| config.epoch);
	let mut capture = LeaseCapture {
		cutoff,
		now_ms,
		e_view,
		applied_index: state.last_applied_log_id.map(|id| id.index),
		target,
		rows: [None; CAPTURED_ROWS],
		truncated: false,
		overlapping: 0,
	};
	// The kept rows are the same whatever the map's order, and the first is the one the class and
	// the provenance speak for (ADR 0127 D3): a row covering the whole target first, then one of
	// the view's epoch, then one the view admitted, then the newest grant, then by range; nothing
	// is allocated. So whenever a covering row of the view's epoch exists, the first kept row is
	// one, however many rows overlap; only two rows are kept, and more are flagged as truncated.
	let rank = |row: &CapturedRow| {
		let lease = &row.lease;
		(
			!(lease.range_start <= target.0 && target.1 <= lease.range_end),
			Some(lease.epoch) != e_view,
			!row.filter_live,
			std::cmp::Reverse(Hlc::from(lease.lease_start)),
			lease.range_start,
			lease.range_end,
		)
	};
	// One pass: the view's own scan, its own filter result per row reused as `filter_live`.
	let leases =
		super::child_writes::scan_applied_leases(state, local, now_ms, |lease, admitted| {
			if !(lease.range_start <= target.1 && target.0 <= lease.range_end) {
				return;
			}
			capture.overlapping = capture.overlapping.saturating_add(1);
			let mut row = Some(CapturedRow {
				lease: *lease,
				filter_live: admitted,
				verify: VerifyState::NotEvaluated,
			});
			for slot in capture.rows.iter_mut() {
				let better = match (&row, &slot) {
					(Some(new), Some(kept)) => rank(new) < rank(kept),
					(Some(_), None) => true,
					(None, _) => false,
				};
				if better {
					std::mem::swap(slot, &mut row);
				}
			}
			if row.is_some() {
				capture.truncated = true;
			}
		});
	(leases, capture)
}

/// A holder's lease view with its capture: `(start, end, epoch)` per verified live lease.
pub(crate) type CapturedLeaseSource = std::sync::Arc<
	dyn Fn(
			(u64, u64),
		) -> std::pin::Pin<
			Box<dyn std::future::Future<Output = (Vec<(u64, u64, u64)>, LeaseCapture)> + Send>,
		> + Send
		+ Sync,
>;

/// Which holder check refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum LeaseSite {
	/// Plan admission (`child_write_keyed`): the lease view, then the admission read's epoch.
	PlanAdmission = 1,
	/// Under the stripe (`holder_authority`): the epoch and lineage read, then the lease view.
	Stripe = 2,
}

/// ADR 0127 D3: what the refusing read's rows show.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum RowClass {
	/// A verified, live, covering row: the refusal came from the decision epoch.
	Live = 1,
	/// A covering row of the view's epoch the read's own HLC filter excluded.
	Expired = 2,
	/// A covering row the read returned and verification refused.
	Unverified = 3,
	/// No row of this holder covers the range (the capture was complete).
	Absent = 4,
	/// No covering row among the captured ones, and the capture was truncated.
	Incomplete = 5,
	/// The site-1 hint did not match the actual range: no derived claim.
	Withheld = 6,
}

/// One captured row's facts and the refusing site's predicates on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RowFacts {
	pub(crate) range_start: u64,
	pub(crate) range_end: u64,
	pub(crate) epoch: u64,
	pub(crate) holder: u32,
	pub(crate) issuer: u32,
	pub(crate) start: Hlc,
	pub(crate) expiry: Hlc,
	pub(crate) digest: [u8; 32],
	pub(crate) filter_live: bool,
	pub(crate) verify: VerifyState,
	pub(crate) covered: bool,
	pub(crate) epoch_dec: bool,
}

/// A retained issue this row agrees with, in the retained window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Agreement {
	/// The latest retained agreeing issue.
	pub(crate) ticket: u64,
	/// How many retained issues agree (the retained window only; never "unique").
	pub(crate) retained_matches: u8,
	pub(crate) outcome: IssueOutcome,
}

/// The latest retained issue for the row's target, epoch and issuer requested after the row's
/// start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LaterIssue {
	pub(crate) ticket: u64,
	pub(crate) request: Hlc,
	pub(crate) outcome: IssueOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Provenance {
	/// `None`: the witness was contended at the snapshot (unknown).
	pub(crate) complete_from: Option<u64>,
	pub(crate) agree: Option<Agreement>,
	pub(crate) later: Option<LaterIssue>,
}

/// ADR 0127 D3: the first `LeaseMissing` of one local holder child, as its own reads saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LeaseEvidence {
	pub(crate) site: LeaseSite,
	pub(crate) cutoff: u64,
	pub(crate) now_ms: u64,
	pub(crate) e_view: Option<u64>,
	pub(crate) e_dec: Option<u64>,
	pub(crate) applied_index: Option<u64>,
	/// What the capture was taken for (site 1: the coordinator's hint; site 2: the plan).
	pub(crate) target: (u64, u64),
	/// What the refusing predicate actually checked.
	pub(crate) actual: (u64, u64),
	pub(crate) hint_matches: bool,
	pub(crate) truncated: bool,
	pub(crate) overlapping: u8,
	pub(crate) rows: [Option<RowFacts>; CAPTURED_ROWS],
	pub(crate) class: RowClass,
	/// For the first covering row; `None` when withheld or when no row covers.
	pub(crate) provenance: Option<Provenance>,
}

impl LeaseEvidence {
	/// Built where `site` refused, from that site's capture and its own decision epoch and range.
	pub(crate) fn build(
		site: LeaseSite,
		capture: &LeaseCapture,
		e_dec: Option<u64>,
		actual: (u64, u64),
		witness: &LeaseWitness,
	) -> Self {
		let hint_matches = capture.target == actual;
		let rows = capture.rows.map(|row| {
			row.map(|row| {
				let lease = row.lease;
				RowFacts {
					range_start: lease.range_start,
					range_end: lease.range_end,
					epoch: lease.epoch,
					holder: lease.holder.id,
					issuer: lease.issuer.id,
					start: lease.lease_start.into(),
					expiry: lease.lease_expiry.into(),
					digest: descriptor_digest(&lease),
					filter_live: row.filter_live,
					verify: row.verify,
					covered: lease.range_start <= actual.0 && actual.1 <= lease.range_end,
					epoch_dec: Some(lease.epoch) == e_dec,
				}
			})
		});
		let class = classify(hint_matches, capture.e_view, capture.truncated, &rows);
		// The class and the provenance speak for the same row: the first, the one exported in full.
		// Provenance is given only when that row decided the class; otherwise it is withheld.
		let provenance = matches!(
			class,
			RowClass::Live | RowClass::Expired | RowClass::Unverified
		)
		.then_some(rows[0])
		.flatten()
		.map(|row| provenance(&row, witness.snapshot(capture.cutoff)));
		Self {
			site,
			cutoff: capture.cutoff,
			now_ms: capture.now_ms,
			e_view: capture.e_view,
			e_dec,
			applied_index: capture.applied_index,
			target: capture.target,
			actual,
			hint_matches,
			truncated: capture.truncated,
			overlapping: capture.overlapping,
			rows,
			class,
			provenance,
		}
	}
}

/// The row facts of the refusing read (ADR 0127 D3), from its FIRST captured row: the capture
/// ranks a row covering the whole target first, so that row decides when one exists, and it is
/// the row exported in full. A second row is exported as facts only.
pub(crate) fn classify(
	hint_matches: bool,
	e_view: Option<u64>,
	truncated: bool,
	rows: &[Option<RowFacts>; CAPTURED_ROWS],
) -> RowClass {
	if !hint_matches {
		return RowClass::Withheld;
	}
	let nothing_decides = if truncated {
		RowClass::Incomplete
	} else {
		RowClass::Absent
	};
	match rows[0] {
		Some(row) if row.covered => match (row.filter_live, row.verify) {
			(true, VerifyState::Verified) => RowClass::Live,
			(true, VerifyState::Failed) => RowClass::Unverified,
			// An admitted row the verification loop never saw: nothing can be said of it.
			(true, VerifyState::NotEvaluated) => RowClass::Incomplete,
			(false, _) if Some(row.epoch) == e_view => RowClass::Expired,
			// A covering row of another epoch only: no covering row of the view's epoch.
			(false, _) => nothing_decides,
		},
		_ => nothing_decides,
	}
}

fn provenance(row: &RowFacts, snapshot: Option<WitnessSnapshot>) -> Provenance {
	let Some(snapshot) = snapshot else {
		return Provenance {
			complete_from: None,
			agree: None,
			later: None,
		};
	};
	let agreeing = || {
		snapshot
			.slots
			.iter()
			.filter(|slot| slot.digest == row.digest)
	};
	let agree = agreeing()
		.max_by_key(|slot| slot.ticket)
		.map(|slot| Agreement {
			ticket: slot.ticket,
			retained_matches: u8::try_from(agreeing().count()).unwrap_or(u8::MAX),
			outcome: slot.outcome,
		});
	let later = snapshot
		.slots
		.iter()
		.filter(|slot| {
			slot.range_start == row.range_start
				&& slot.range_end == row.range_end
				&& slot.epoch == row.epoch
				&& slot.issuer == row.issuer
				&& slot.request > row.start
		})
		.max_by_key(|slot| slot.ticket)
		.map(|slot| LaterIssue {
			ticket: slot.ticket,
			request: slot.request,
			outcome: slot.outcome,
		});
	Provenance {
		complete_from: Some(snapshot.complete_from),
		agree,
		later,
	}
}

/// A holder child's evidence sink: the coordinator's diagnostic hint (the child's bounds) and
/// the first evidence any of its checks produced.
pub(crate) struct LeaseEvidenceSink {
	pub(crate) hint: (u64, u64),
	pub(crate) source: CapturedLeaseSource,
	pub(crate) witness: std::sync::Arc<super::write_stall_trace::WriteStallTrace>,
	first: Mutex<Option<Box<LeaseEvidence>>>,
}

impl LeaseEvidenceSink {
	pub(crate) fn new(
		hint: (u64, u64),
		source: CapturedLeaseSource,
		witness: std::sync::Arc<super::write_stall_trace::WriteStallTrace>,
	) -> std::sync::Arc<Self> {
		std::sync::Arc::new(Self {
			hint,
			source,
			witness,
			first: Mutex::new(None),
		})
	}

	/// Keeps `evidence` only if none was kept.
	pub(crate) fn offer(&self, evidence: LeaseEvidence) {
		let mut first = self.first.lock().unwrap_or_else(|e| e.into_inner());
		if first.is_none() {
			*first = Some(Box::new(evidence));
		}
	}

	pub(crate) fn take(&self) -> Option<Box<LeaseEvidence>> {
		self.first.lock().unwrap_or_else(|e| e.into_inner()).take()
	}

	pub(crate) fn lease_witness(&self) -> &LeaseWitness {
		&self.witness.leases
	}
}

/// The u64 words of one value: `(hi, lo)`, each exact in an f64 reader.
fn words(value: u64) -> (u64, u64) {
	(value >> 32, value & u64::from(u32::MAX))
}

fn digest_words(digest: &[u8; 32]) -> [u64; 8] {
	std::array::from_fn(|at| {
		u64::from(u32::from_be_bytes(
			digest[at * 4..at * 4 + 4].try_into().expect("four bytes"),
		))
	})
}

macro_rules! push_u64 {
	($out:expr, $hi:literal, $lo:literal, $value:expr) => {{
		let (hi, lo) = words($value);
		$out.push(($hi, hi));
		$out.push(($lo, lo));
	}};
}

macro_rules! push_digest {
	($out:expr, [$($name:literal),*], $digest:expr) => {{
		let words = digest_words($digest);
		let names = [$($name),*];
		for (name, word) in names.into_iter().zip(words) {
			$out.push((name, word));
		}
	}};
}

impl LeaseEvidence {
	/// ADR 0127 D4: the exported fields, in a fixed order and shape. Every u64 is two 32-bit
	/// words (`_hi`, `_lo`); a digest is eight; an absent optional value is its `_present` 0
	/// and zero words. Enum codes are their `repr(u8)` values.
	pub(crate) fn fields(&self) -> Vec<(&'static str, u64)> {
		let mut out = Vec::with_capacity(96);
		let opt = |value: Option<u64>| (u64::from(value.is_some()), value.unwrap_or(0));
		out.push(("site", self.site as u64));
		push_u64!(out, "cutoff_hi", "cutoff_lo", self.cutoff);
		push_u64!(out, "now_ms_hi", "now_ms_lo", self.now_ms);
		let (present, value) = opt(self.e_view);
		out.push(("e_view_present", present));
		push_u64!(out, "e_view_hi", "e_view_lo", value);
		let (present, value) = opt(self.e_dec);
		out.push(("e_dec_present", present));
		push_u64!(out, "e_dec_hi", "e_dec_lo", value);
		let (present, value) = opt(self.applied_index);
		out.push(("applied_present", present));
		push_u64!(out, "applied_hi", "applied_lo", value);
		push_u64!(out, "target_start_hi", "target_start_lo", self.target.0);
		push_u64!(out, "target_end_hi", "target_end_lo", self.target.1);
		push_u64!(out, "actual_start_hi", "actual_start_lo", self.actual.0);
		push_u64!(out, "actual_end_hi", "actual_end_lo", self.actual.1);
		out.push(("hint_matches", u64::from(self.hint_matches)));
		out.push(("truncated", u64::from(self.truncated)));
		out.push(("overlapping", u64::from(self.overlapping)));
		out.push(("class", self.class as u64));
		// The first captured row, in full (its digest names it).
		let row = self.rows[0];
		let r = row.unwrap_or(RowFacts {
			range_start: 0,
			range_end: 0,
			epoch: 0,
			holder: 0,
			issuer: 0,
			start: Hlc::default(),
			expiry: Hlc::default(),
			digest: [0; 32],
			filter_live: false,
			verify: VerifyState::NotEvaluated,
			covered: false,
			epoch_dec: false,
		});
		out.push(("r0_present", u64::from(row.is_some())));
		push_u64!(out, "r0_range_start_hi", "r0_range_start_lo", r.range_start);
		push_u64!(out, "r0_range_end_hi", "r0_range_end_lo", r.range_end);
		push_u64!(out, "r0_epoch_hi", "r0_epoch_lo", r.epoch);
		out.push(("r0_holder", u64::from(r.holder)));
		out.push(("r0_issuer", u64::from(r.issuer)));
		push_u64!(out, "r0_start_ms_hi", "r0_start_ms_lo", r.start.physical_ms);
		out.push(("r0_start_logical", u64::from(r.start.logical)));
		out.push(("r0_start_node", u64::from(r.start.node_id)));
		push_u64!(
			out,
			"r0_expiry_ms_hi",
			"r0_expiry_ms_lo",
			r.expiry.physical_ms
		);
		out.push(("r0_expiry_logical", u64::from(r.expiry.logical)));
		out.push(("r0_expiry_node", u64::from(r.expiry.node_id)));
		push_digest!(
			out,
			[
				"r0_digest_w0",
				"r0_digest_w1",
				"r0_digest_w2",
				"r0_digest_w3",
				"r0_digest_w4",
				"r0_digest_w5",
				"r0_digest_w6",
				"r0_digest_w7"
			],
			&r.digest
		);
		out.push(("r0_filter_live", u64::from(r.filter_live)));
		out.push(("r0_verify", r.verify as u64));
		out.push(("r0_covered", u64::from(r.covered)));
		out.push(("r0_epoch_dec", u64::from(r.epoch_dec)));
		// The second captured row: its facts and predicates (no provenance is claimed for it).
		let row = self.rows[1];
		let r = row.unwrap_or(RowFacts {
			range_start: 0,
			range_end: 0,
			epoch: 0,
			holder: 0,
			issuer: 0,
			start: Hlc::default(),
			expiry: Hlc::default(),
			digest: [0; 32],
			filter_live: false,
			verify: VerifyState::NotEvaluated,
			covered: false,
			epoch_dec: false,
		});
		out.push(("r1_present", u64::from(row.is_some())));
		push_u64!(out, "r1_range_start_hi", "r1_range_start_lo", r.range_start);
		push_u64!(out, "r1_range_end_hi", "r1_range_end_lo", r.range_end);
		push_u64!(out, "r1_epoch_hi", "r1_epoch_lo", r.epoch);
		push_u64!(
			out,
			"r1_expiry_ms_hi",
			"r1_expiry_ms_lo",
			r.expiry.physical_ms
		);
		out.push(("r1_expiry_logical", u64::from(r.expiry.logical)));
		out.push(("r1_expiry_node", u64::from(r.expiry.node_id)));
		out.push(("r1_filter_live", u64::from(r.filter_live)));
		out.push(("r1_verify", r.verify as u64));
		out.push(("r1_covered", u64::from(r.covered)));
		out.push(("r1_epoch_dec", u64::from(r.epoch_dec)));
		// Provenance of the first covering row.
		let provenance = self.provenance;
		out.push(("prov_present", u64::from(provenance.is_some())));
		let (present, value) = opt(provenance.and_then(|p| p.complete_from));
		out.push(("prov_complete_present", present));
		push_u64!(out, "prov_complete_from_hi", "prov_complete_from_lo", value);
		let agree = provenance.and_then(|p| p.agree);
		out.push(("agree_present", u64::from(agree.is_some())));
		push_u64!(
			out,
			"agree_ticket_hi",
			"agree_ticket_lo",
			agree.map_or(0, |a| a.ticket)
		);
		out.push((
			"agree_matches",
			agree.map_or(0, |a| u64::from(a.retained_matches)),
		));
		out.push(("agree_outcome", agree.map_or(0, |a| a.outcome as u64)));
		let later = provenance.and_then(|p| p.later);
		out.push(("later_present", u64::from(later.is_some())));
		push_u64!(
			out,
			"later_ticket_hi",
			"later_ticket_lo",
			later.map_or(0, |l| l.ticket)
		);
		push_u64!(
			out,
			"later_request_ms_hi",
			"later_request_ms_lo",
			later.map_or(0, |l| l.request.physical_ms)
		);
		out.push((
			"later_request_logical",
			later.map_or(0, |l| u64::from(l.request.logical)),
		));
		out.push((
			"later_request_node",
			later.map_or(0, |l| u64::from(l.request.node_id)),
		));
		out.push(("later_outcome", later.map_or(0, |l| l.outcome as u64)));
		out
	}
}

#[cfg(test)]
#[path = "lease_evidence/tests.rs"]
mod tests;
