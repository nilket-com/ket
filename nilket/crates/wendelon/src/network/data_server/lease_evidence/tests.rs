//! Wendelon ADR 0127: the issue witness, the capture, the classifier and the export, in isolation.

use super::*;
use crate::schema::NodeId;

const HOLDER: u32 = 51;

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

/// A signed-looking descriptor: every field set, the signature bytes derived from `tag`.
fn lease(range: (u64, u64), epoch: u64, start_ms: u64, ttl_ms: u64, tag: u8) -> LeaseDescriptor {
	let mut lease: LeaseDescriptor = bytemuck::Zeroable::zeroed();
	lease.epoch = epoch;
	lease.holder = NodeId {
		id: HOLDER,
		_reserved: 0,
	};
	lease.range_start = range.0;
	lease.range_end = range.1;
	lease.lease_start = hlc(start_ms, 0, HOLDER);
	lease.lease_expiry = hlc(start_ms + ttl_ms, 0, HOLDER);
	lease.issuer = NodeId {
		id: HOLDER,
		_reserved: 0,
	};
	lease.signature = [tag; 64];
	lease
}

fn state_with(epoch: u64, leases: &[LeaseDescriptor]) -> SmState {
	let mut state = SmState::default();
	state.epoch_history.insert(
		epoch,
		crate::consensus::EpochConfig {
			epoch,
			peers: Default::default(),
		},
	);
	for lease in leases {
		state.leases.insert(lease.range_start, *lease);
	}
	state
}

fn facts(lease: &LeaseDescriptor, filter_live: bool, verify: VerifyState, e_dec: u64) -> RowFacts {
	RowFacts {
		range_start: lease.range_start,
		range_end: lease.range_end,
		epoch: lease.epoch,
		holder: lease.holder.id,
		issuer: lease.issuer.id,
		start: lease.lease_start.into(),
		expiry: lease.lease_expiry.into(),
		digest: descriptor_digest(lease),
		filter_live,
		verify,
		covered: true,
		epoch_dec: lease.epoch == e_dec,
	}
}

// ---- the issue witness ----

/// A slot reused by a later issue is never modified by the evicted issue's outcome, whether
/// the newcomer is another target or the SAME target; a surviving sibling of a partly
/// overwritten batch is updated by its own ticket only.
#[test]
fn a_reused_slot_is_never_updated_by_an_evicted_issues_late_outcome() {
	let witness = LeaseWitness::default();
	let a = lease((0, 9), 1, 1_000, 3_000, 1);
	let b = lease((10, 19), 1, 1_000, 3_000, 2);
	// One batch, two targets, two tickets, two slots (0 and 1).
	let batch = witness.issue(&[a, b]);
	assert_eq!(batch.len(), 2);
	assert!(batch[0].0 < batch[1].0);
	// Seven more single-target issues: slots 2..7, then slot 0 is reused by the SAME target as
	// the batch's first lease (a newer request for range 0..9).
	for at in 0..6u64 {
		witness.issue(&[lease((100 + at * 10, 109 + at * 10), 1, 2_000, 3_000, 3)]);
	}
	let newcomer = lease((0, 9), 1, 9_000, 3_000, 4);
	let newcomer_ticket = witness.issue(&[newcomer])[0].0;
	let before = witness.slots_for_test();
	assert!(
		before.iter().all(|slot| slot.ticket != batch[0].0),
		"evicted"
	);
	assert!(
		before.iter().any(|slot| slot.ticket == batch[1].0),
		"the sibling survives"
	);
	// The late outcome of the whole batch: only the surviving sibling changes.
	witness.finish(&batch, |_| IssueOutcome::Granted);
	let after = witness.slots_for_test();
	let of = |slots: &[IssueSlot], ticket: u64| *slots.iter().find(|s| s.ticket == ticket).unwrap();
	assert_eq!(
		of(&after, newcomer_ticket),
		of(&before, newcomer_ticket),
		"the newcomer is untouched"
	);
	assert_eq!(
		of(&after, newcomer_ticket).outcome,
		IssueOutcome::CallSiteReached
	);
	assert_eq!(of(&after, batch[1].0).outcome, IssueOutcome::Granted);
	// The snapshot speaks only from above the evicted ticket.
	let snapshot = witness.snapshot(witness.observe()).unwrap();
	assert_eq!(snapshot.complete_from, batch[0].0 + 1);
	assert!(snapshot.slots.iter().all(|slot| slot.ticket != batch[0].0));
}

/// An issue after the cutoff is excluded; an outcome written after the cutoff is reported as
/// it stood at the cutoff (the slot had only reached the call site).
#[test]
fn the_snapshot_is_as_of_the_cutoff() {
	let witness = LeaseWitness::default();
	let before = witness.issue(&[lease((0, 9), 1, 1_000, 3_000, 1)]);
	let cutoff = witness.observe();
	let after = witness.issue(&[lease((0, 9), 1, 2_000, 3_000, 2)]);
	witness.finish(&before, |_| IssueOutcome::Unanswered);
	let snapshot = witness.snapshot(cutoff).unwrap();
	assert_eq!(
		snapshot.slots.len(),
		1,
		"the issue after the cutoff is excluded"
	);
	assert_eq!(snapshot.slots[0].ticket, before[0].0);
	assert_eq!(snapshot.slots[0].outcome, IssueOutcome::CallSiteReached);
	assert!(after[0].0 > cutoff);
	// A later observation sees the outcome.
	let later = witness.snapshot(witness.observe()).unwrap();
	let slot = later
		.slots
		.iter()
		.find(|s| s.ticket == before[0].0)
		.unwrap();
	assert_eq!(slot.outcome, IssueOutcome::Unanswered);
}

/// An observation taken while an issue holds the ring (its ticket assigned, its slot not yet
/// visible to anyone else): the snapshot is unknown (contended), never one that silently
/// misses the pre-cutoff issue; once the issue's critical section ends, it is there.
#[test]
fn an_observation_during_an_issue_is_unknown_and_never_misses_it() {
	let witness = std::sync::Arc::new(LeaseWitness::default());
	let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
	*witness.hold_next_issue.lock().unwrap() = Some(barrier.clone());
	let issuing = {
		let witness = witness.clone();
		std::thread::spawn(move || witness.issue(&[lease((0, 9), 1, 1_000, 3_000, 1)]))
	};
	barrier.wait(); // the issue holds the ring, its ticket taken
	let cutoff = witness.observe();
	assert_eq!(witness.snapshot(cutoff), None, "contended: unknown");
	barrier.wait();
	let issued = issuing.join().unwrap();
	assert!(issued[0].0 < cutoff);
	let snapshot = witness.snapshot(cutoff).unwrap();
	assert_eq!(snapshot.slots.len(), 1);
	assert_eq!(snapshot.slots[0].ticket, issued[0].0);
}

/// Byte-identical descriptors issued twice: agreement counts the retained window only and is
/// never "unique"; once the first copy is evicted the count drops and `complete_from` says why.
#[test]
fn a_duplicate_descriptor_never_reads_as_unique() {
	let witness = LeaseWitness::default();
	let twin = lease((0, 9), 1, 1_000, 3_000, 7);
	let first = witness.issue(&[twin])[0].0;
	let second = witness.issue(&[twin])[0].0;
	let row = facts(&twin, false, VerifyState::NotEvaluated, 1);
	let p = provenance(&row, witness.snapshot(witness.observe()));
	assert_eq!(
		p.agree.map(|a| (a.ticket, a.retained_matches)),
		Some((second, 2))
	);
	for at in 0..7u64 {
		witness.issue(&[lease((100 + at, 100 + at), 1, 2_000, 3_000, 1)]);
	}
	let p = provenance(&row, witness.snapshot(witness.observe()));
	assert_eq!(
		p.agree.map(|a| (a.ticket, a.retained_matches)),
		Some((second, 1))
	);
	assert_eq!(
		p.complete_from,
		Some(first + 1),
		"the first copy may exist below"
	);
}

// ---- the capture ----

/// At `now == expiry_ms` the row is live: `(t, 0, 0) < (t, 0, 51)`. The capture's filter is
/// exactly the lease view's (`applied_leases`).
#[test]
fn the_hlc_equality_boundary_is_live_in_the_view_and_the_capture() {
	let row = lease((0, 9), 1, 1_000, 3_000, 1);
	let state = state_with(1, &[row]);
	let now_ms = row.lease_expiry.physical_ms;
	assert_eq!(
		super::super::child_writes::applied_leases(&state, HOLDER, now_ms),
		vec![row]
	);
	let (rows, capture) = applied_leases_captured(&state, HOLDER, now_ms, (0, 9), 5);
	assert_eq!(rows, vec![row]);
	let captured = capture.rows[0].unwrap();
	assert!(captured.filter_live);
	let (_, later) = applied_leases_captured(&state, HOLDER, now_ms + 1, (0, 9), 6);
	assert!(!later.rows[0].unwrap().filter_live);
	assert!(super::super::child_writes::applied_leases(&state, HOLDER, now_ms + 1).is_empty());
	let facts = facts(&captured.lease, true, VerifyState::Verified, 1);
	assert_eq!(
		classify(true, Some(1), false, &[Some(facts), None]),
		RowClass::Live,
		"never expired at the boundary"
	);
}

/// More overlapping rows than the bound: the same two are kept whatever the map's order (the
/// one containing the target's start first), the count saturates, and with no covering row
/// among them the class is incomplete, never absent.
#[test]
fn a_truncated_capture_is_deterministic_and_never_reads_as_absent() {
	let rows = [
		lease((50, 60), 1, 1_000, 3_000, 1),
		lease((5, 20), 1, 1_000, 3_000, 2),
		lease((0, 4), 1, 1_000, 3_000, 3),
	];
	for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0]] {
		let mut state = state_with(1, &[]);
		for at in order {
			state.leases.insert(rows[at].range_start, rows[at]);
		}
		let (_, capture) = applied_leases_captured(&state, HOLDER, 2_000, (3, 55), 9);
		let kept: Vec<u64> = capture
			.rows
			.iter()
			.flatten()
			.map(|r| r.lease.range_start)
			.collect();
		assert_eq!(kept, vec![0, 5], "order {order:?}");
		assert!(capture.truncated);
		assert_eq!(capture.overlapping, 3);
	}
	let not_covering = |lease: &LeaseDescriptor| RowFacts {
		covered: false,
		..facts(lease, true, VerifyState::Verified, 1)
	};
	assert_eq!(
		classify(
			true,
			Some(1),
			true,
			&[Some(not_covering(&rows[2])), Some(not_covering(&rows[1]))]
		),
		RowClass::Incomplete
	);
	assert_eq!(
		classify(true, Some(1), false, &[Some(not_covering(&rows[2])), None]),
		RowClass::Absent
	);
}

// ---- the classifier ----

/// Site 1: the view read epoch 5 and a live verified row; admission read epoch 6. The refusal
/// is the epoch crossing, not a missing lease.
#[test]
fn an_epoch_crossing_at_plan_admission_reads_live() {
	let row = lease((0, 9), 5, 1_000, 3_000, 1);
	let facts = facts(&row, true, VerifyState::Verified, 6);
	assert!(!facts.epoch_dec);
	assert_eq!(
		classify(true, Some(5), false, &[Some(facts), None]),
		RowClass::Live
	);
}

/// Site 2: the decision read epoch 5, the view then read epoch 6 and filtered the epoch-5
/// row out. The rows show it at epoch 5; both epochs are kept, so the crossing is visible.
#[test]
fn an_epoch_crossing_under_the_stripe_keeps_both_epochs() {
	let row = lease((0, 9), 5, 1_000, 3_000, 1);
	let mut state = state_with(6, &[row]);
	state.leases.insert(0, row);
	let (rows, capture) = applied_leases_captured(&state, HOLDER, 1_500, (0, 9), 3);
	assert!(rows.is_empty(), "the view at epoch 6 drops it");
	assert_eq!(capture.e_view, Some(6));
	let witness = LeaseWitness::default();
	let evidence = LeaseEvidence::build(LeaseSite::Stripe, &capture, Some(5), (0, 9), &witness);
	assert_eq!((evidence.e_view, evidence.e_dec), (Some(6), Some(5)));
	let r0 = evidence.rows[0].unwrap();
	assert_eq!((r0.epoch, r0.filter_live, r0.epoch_dec), (5, false, true));
	assert_eq!(evidence.class, RowClass::Absent);
}

/// A site-1 hint that is not the range the predicate checked: no derived claim at all.
#[test]
fn a_mismatched_hint_withholds_every_derived_claim() {
	let row = lease((0, 9), 1, 1_000, 3_000, 1);
	let state = state_with(1, &[row]);
	let (_, capture) = applied_leases_captured(&state, HOLDER, 9_000, (0, 9), 3);
	let witness = LeaseWitness::default();
	let evidence = LeaseEvidence::build(
		LeaseSite::PlanAdmission,
		&capture,
		Some(1),
		(0, 4),
		&witness,
	);
	assert!(!evidence.hint_matches);
	assert_eq!(evidence.class, RowClass::Withheld);
	assert_eq!(evidence.provenance, None);
}

/// The consulted row agrees with an older retained issue A while a later issue B for the same
/// target exists: both facts are kept.
#[test]
fn agreement_with_an_older_issue_and_a_later_issue_are_both_kept() {
	let witness = LeaseWitness::default();
	let a = lease((0, 9), 1, 1_000, 3_000, 1);
	let b = lease((0, 9), 1, 2_000, 3_000, 2);
	let a_ticket = witness.issue(&[a]);
	witness.finish(&a_ticket, |_| IssueOutcome::Granted);
	let b_ticket = witness.issue(&[b]);
	witness.finish(&b_ticket, |_| IssueOutcome::Unanswered);
	let state = state_with(1, &[a]);
	let cutoff = witness.observe();
	let (_, capture) = applied_leases_captured(&state, HOLDER, 4_500, (0, 9), cutoff);
	let evidence = LeaseEvidence::build(
		LeaseSite::PlanAdmission,
		&capture,
		Some(1),
		(0, 9),
		&witness,
	);
	assert_eq!(evidence.class, RowClass::Expired);
	let provenance = evidence.provenance.unwrap();
	assert_eq!(
		provenance
			.agree
			.map(|a| (a.ticket, a.retained_matches, a.outcome)),
		Some((a_ticket[0].0, 1, IssueOutcome::Granted))
	);
	assert_eq!(
		provenance
			.later
			.map(|l| (l.ticket, l.request.physical_ms, l.outcome)),
		Some((b_ticket[0].0, 2_000, IssueOutcome::Unanswered))
	);
}

// ---- the export ----

fn value(fields: &[(&'static str, u64)], name: &str) -> u64 {
	fields
		.iter()
		.find(|(field, _)| *field == name)
		.unwrap_or_else(|| panic!("no field {name}"))
		.1
}

fn joined(fields: &[(&'static str, u64)], hi: &str, lo: &str) -> u64 {
	let (hi, lo) = (value(fields, hi), value(fields, lo));
	assert!(hi <= u64::from(u32::MAX) && lo <= u64::from(u32::MAX));
	// Each word survives an f64 reader exactly.
	assert_eq!(hi as f64 as u64, hi);
	assert_eq!(lo as f64 as u64, lo);
	(hi << 32) | lo
}

/// Every u64 travels as two 32-bit words and is rebuilt exactly, at 0, 2^53 + 1 and 2^64 - 1;
/// the shape is fixed and every name is unique.
#[test]
fn every_u64_round_trips_exactly_through_its_words() {
	for end in [0u64, (1 << 53) + 1, u64::MAX] {
		let row = lease((0, end), 1, 1_000, 3_000, 1);
		let state = state_with(1, &[row]);
		let witness = LeaseWitness::default();
		let (_, capture) = applied_leases_captured(&state, HOLDER, 9_000, (0, end), 7);
		let evidence =
			LeaseEvidence::build(LeaseSite::Stripe, &capture, Some(1), (0, end), &witness);
		let fields = evidence.fields();
		assert_eq!(joined(&fields, "r0_range_end_hi", "r0_range_end_lo"), end);
		assert_eq!(joined(&fields, "actual_end_hi", "actual_end_lo"), end);
		assert_eq!(joined(&fields, "now_ms_hi", "now_ms_lo"), 9_000);
		let mut names: Vec<&str> = fields.iter().map(|(name, _)| *name).collect();
		let count = names.len();
		names.sort_unstable();
		names.dedup();
		assert_eq!(names.len(), count, "unique names");
		assert!(
			fields
				.iter()
				.all(|(_, value)| *value <= u64::from(u32::MAX))
		);
		eprintln!("ADR0127 export: {count} fields per evidence");
	}
	let digest = descriptor_digest(&lease((0, 9), 1, 1_000, 3_000, 1));
	let words = digest_words(&digest);
	let mut rebuilt = [0u8; 32];
	for (at, word) in words.iter().enumerate() {
		rebuilt[at * 4..at * 4 + 4].copy_from_slice(&(*word as u32).to_be_bytes());
	}
	assert_eq!(rebuilt, digest);
}

/// The type sizes this opt-in carries (reported in the review packet).
#[test]
fn the_sizes_are_reported() {
	eprintln!(
		"ADR0127 sizes: LeaseEvidence {} B, Box<LeaseEvidence> {} B, LeaseCapture {} B, IssueSlot {} B, LeaseWitness {} B, Option<Box<LeaseEvidence>> {} B, LeaseDescriptor {} B",
		std::mem::size_of::<LeaseEvidence>(),
		std::mem::size_of::<Box<LeaseEvidence>>(),
		std::mem::size_of::<LeaseCapture>(),
		std::mem::size_of::<IssueSlot>(),
		std::mem::size_of::<LeaseWitness>(),
		std::mem::size_of::<Option<Box<LeaseEvidence>>>(),
		std::mem::size_of::<LeaseDescriptor>(),
	);
	assert_eq!(std::mem::size_of::<Option<Box<LeaseEvidence>>>(), 8);
}

/// Codex 01a20af0's counterexample: target [5, 10], holder rows [0, 6] and [5, 10], both expired.
/// The covering row is captured first, so the class, the agreement and the exported digest and
/// start all name the same row; the other is exported as facts only.
#[test]
fn the_provenance_names_the_row_exported_in_full() {
	let witness = LeaseWitness::default();
	let partial = lease((0, 6), 1, 1_000, 3_000, 1);
	let covering = lease((5, 10), 1, 1_000, 3_000, 2);
	let issued = witness.issue(&[covering]);
	witness.finish(&issued, |_| IssueOutcome::Granted);
	let state = state_with(1, &[partial, covering]);
	let cutoff = witness.observe();
	let (_, capture) = applied_leases_captured(&state, HOLDER, 9_000, (5, 10), cutoff);
	assert_eq!(
		capture.rows.map(|row| row.map(|row| row.lease.range_start)),
		[Some(5), Some(0)],
		"the covering row first"
	);
	let evidence = LeaseEvidence::build(
		LeaseSite::PlanAdmission,
		&capture,
		Some(1),
		(5, 10),
		&witness,
	);
	assert_eq!(evidence.class, RowClass::Expired);
	assert_eq!(
		evidence.provenance.and_then(|p| p.agree).map(|a| a.ticket),
		Some(issued[0].0)
	);
	let fields = evidence.fields();
	assert_eq!(joined(&fields, "r0_range_start_hi", "r0_range_start_lo"), 5);
	assert_eq!(joined(&fields, "r1_range_start_hi", "r1_range_start_lo"), 0);
	let exported: Vec<u64> = (0..8)
		.map(|at| value(&fields, &format!("r0_digest_w{at}")))
		.collect();
	assert_eq!(
		exported,
		digest_words(&descriptor_digest(&covering)).to_vec()
	);
	assert_ne!(descriptor_digest(&covering), descriptor_digest(&partial));
}

/// When the first row does not decide the class (here a covering row of another epoch only),
/// no provenance is given: nothing is claimed about a row the class does not rest on.
#[test]
fn no_provenance_without_a_deciding_first_row() {
	let witness = LeaseWitness::default();
	let old = lease((0, 9), 4, 1_000, 3_000, 1);
	witness.issue(&[old]);
	let state = state_with(5, &[old]);
	let (_, capture) = applied_leases_captured(&state, HOLDER, 1_500, (0, 9), witness.observe());
	let evidence = LeaseEvidence::build(LeaseSite::Stripe, &capture, Some(5), (0, 9), &witness);
	assert_eq!(evidence.class, RowClass::Absent);
	assert!(evidence.rows[0].is_some_and(|row| row.covered && row.epoch == 4));
	assert_eq!(evidence.provenance, None);
	assert_eq!(value(&evidence.fields(), "prov_present"), 0);
}

/// Codex 01a20b27: target [5, 10], view epoch 6, now 4000: [0, 10] at epoch 5 (start 2000,
/// expiry 3000) and [5, 11] at epoch 6 (start 1000, expiry 2000), both covering, both expired.
/// The covering row of the view's epoch is first whatever the map's order, even behind a newer
/// grant of another epoch and with a third covering row of another epoch, so the class is
/// expired, never absent; only two rows are kept and the third is flagged.
#[test]
fn a_covering_row_of_the_views_epoch_decides_before_another_epochs_newer_grant() {
	let other = lease((0, 10), 5, 2_000, 1_000, 1);
	let current = lease((5, 11), 6, 1_000, 1_000, 2);
	let third = lease((1, 12), 4, 3_000, 500, 3);
	for (rows, truncated) in [
		(vec![other, current], false),
		(vec![current, other], false),
		(vec![third, other, current], true),
		(vec![current, third, other], true),
	] {
		let mut state = state_with(6, &[]);
		for row in &rows {
			state.leases.insert(row.range_start, *row);
		}
		let (_, capture) = applied_leases_captured(&state, HOLDER, 4_000, (5, 10), 9);
		assert_eq!(
			capture.rows[0].map(|row| (row.lease.range_start, row.lease.epoch)),
			Some((5, 6)),
			"the current epoch's covering row first: {rows:?}"
		);
		assert_eq!(capture.truncated, truncated);
		let witness = LeaseWitness::default();
		let evidence =
			LeaseEvidence::build(LeaseSite::Stripe, &capture, Some(6), (5, 10), &witness);
		assert_eq!(evidence.class, RowClass::Expired, "{rows:?}");
		assert!(evidence.provenance.is_some());
	}
}
