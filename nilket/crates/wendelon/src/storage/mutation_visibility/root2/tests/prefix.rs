//! Stage T-S1: committed-prefix shape validation. Every typed error, the accepted history
//! variants, the precedence, and the documented non-detection limit.
use crate::consensus::retention::{
	ControlRetention, HistoryCheck, RetentionRefusals, RetentionViolation,
};
use crate::consensus::{RetentionStatus, WendelonRequest, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::prefix::*;
use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership};
use serde::Serialize;
use std::collections::BTreeMap;

type E2 = Entry<WendelonTypeConfig>;

fn status(configured: ControlRetention, history: HistoryCheck) -> RetentionStatus {
	RetentionStatus {
		configured,
		history,
		violation: None,
		refusals: RetentionRefusals::default(),
	}
}
fn full() -> RetentionStatus {
	status(ControlRetention::Full, HistoryCheck::Fresh)
}
fn lid(term: u64, node: u32, index: u64) -> LogId<u32> {
	LogId::new(CommittedLeaderId::new(term, node), index)
}
fn membership(log_id: LogId<u32>) -> E2 {
	Entry {
		log_id,
		payload: EntryPayload::Membership(Membership::from(BTreeMap::from([(
			1u32,
			BasicNode { addr: "n1".into() },
		)]))),
	}
}
fn blank(log_id: LogId<u32>) -> E2 {
	Entry {
		log_id,
		payload: EntryPayload::Blank,
	}
}
fn normal(log_id: LogId<u32>, byte: u8) -> E2 {
	Entry {
		log_id,
		payload: EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![byte; 3])),
	}
}
/// Genesis at 0, then (1,1,1) normal, (1,1,2) blank, (1,1,3) normal.
fn prefix() -> Vec<E2> {
	vec![
		membership(LogId::default()),
		normal(lid(1, 1, 1), 1),
		blank(lid(1, 1, 2)),
		normal(lid(1, 1, 3), 3),
	]
}
/// The whole prefix in one batch, captured at its last entry, re-read unchanged.
fn check(
	start: &RetentionStatus,
	batch: &[E2],
	from: u64,
	end: &RetentionStatus,
) -> Result<(), PrefixError2> {
	check_committed_prefix2(
		start,
		lid(1, 1, 3),
		batch,
		from,
		end,
		&prefix()[3],
		lid(1, 1, 3),
	)
}

#[test]
fn full_prefix_is_accepted_under_every_checked_history() {
	let histories = [
		HistoryCheck::Fresh,
		HistoryCheck::EmptyLog,
		HistoryCheck::Complete { stored_last: 0 },
		HistoryCheck::Complete { stored_last: 999 },
	];
	for a in histories {
		for b in histories {
			let (s, e) = (
				status(ControlRetention::Full, a),
				status(ControlRetention::Full, b),
			);
			assert_eq!(check(&s, &prefix(), 0, &e), Ok(()));
		}
	}
	// Later batches, and a boundary outside the batch.
	assert_eq!(check(&full(), &prefix()[2..], 2, &full()), Ok(()));
	assert_eq!(check(&full(), &prefix()[..2], 0, &full()), Ok(()));
	// A strictly greater commit point after the reads is allowed.
	let p = prefix();
	assert_eq!(
		check_committed_prefix2(&full(), lid(1, 1, 3), &p, 0, &full(), &p[3], lid(2, 1, 7)),
		Ok(())
	);
}

#[test]
fn status_failures_follow_the_selected_precedence() {
	let compacting = status(ControlRetention::Compacting, HistoryCheck::Fresh);
	let not_checked = status(ControlRetention::Full, HistoryCheck::NotChecked);
	let mut latched = full();
	latched.violation = Some(RetentionViolation::SnapshotRequest);
	let p = prefix();
	// Mode, start-only and end-only.
	assert_eq!(
		check(&compacting, &p, 0, &full()),
		Err(PrefixError2::NotFull)
	);
	assert_eq!(
		check(&full(), &p, 0, &compacting),
		Err(PrefixError2::NotFull)
	);
	// Violation, start-only and end-only.
	assert_eq!(check(&latched, &p, 0, &full()), Err(PrefixError2::Latched));
	assert_eq!(check(&full(), &p, 0, &latched), Err(PrefixError2::Latched));
	// NotChecked, start-only and end-only.
	assert_eq!(
		check(&not_checked, &p, 0, &full()),
		Err(PrefixError2::NotChecked)
	);
	assert_eq!(
		check(&full(), &p, 0, &not_checked),
		Err(PrefixError2::NotChecked)
	);
	// Combined: mode before violation, violation before history, history before the batch.
	assert_eq!(
		check(&latched, &p, 0, &compacting),
		Err(PrefixError2::NotFull)
	);
	let mut latched_not_checked = not_checked;
	latched_not_checked.violation = Some(RetentionViolation::SnapshotInstall);
	assert_eq!(
		check(&not_checked, &p, 0, &latched_not_checked),
		Err(PrefixError2::Latched)
	);
	assert_eq!(
		check(&full(), &[], 0, &not_checked),
		Err(PrefixError2::NotChecked)
	);
}

#[test]
fn empty_batch_is_a_gap_before_genesis() {
	assert_eq!(
		check(&full(), &[], 0, &full()),
		Err(PrefixError2::Gap { at: 0 })
	);
	assert_eq!(
		check(&full(), &[], 2, &full()),
		Err(PrefixError2::Gap { at: 2 })
	);
}

#[test]
fn genesis_must_be_membership_at_the_default_log_id() {
	let mut p = prefix();
	// Missing index 0: the batch starts at 1 although `from` is 0.
	assert_eq!(
		check(&full(), &p[1..], 0, &full()),
		Err(PrefixError2::MissingGenesis)
	);
	// Not Membership.
	p[0] = blank(LogId::default());
	assert_eq!(
		check(&full(), &p, 0, &full()),
		Err(PrefixError2::MissingGenesis)
	);
	// Membership, but a non-default full log id at index 0 (term, then node).
	for id in [lid(1, 0, 0), lid(0, 1, 0)] {
		p[0] = membership(id);
		assert_eq!(
			check(&full(), &p, 0, &full()),
			Err(PrefixError2::MissingGenesis)
		);
	}
}

#[test]
fn batch_indices_must_be_contiguous_from_the_supplied_start() {
	let p = prefix();
	let at = |idx: &[usize]| idx.iter().map(|&i| p[i].clone()).collect::<Vec<_>>();
	// Duplicate, out of order, missing.
	for batch in [at(&[1, 1, 2]), at(&[1, 3, 2]), at(&[1, 3])] {
		assert_eq!(
			check(&full(), &batch, 1, &full()),
			Err(PrefixError2::Gap { at: 2 })
		);
	}
	// A mismatch against the supplied `from` is detected; `from` itself is not authenticated.
	assert_eq!(
		check(&full(), &p[1..], 2, &full()),
		Err(PrefixError2::Gap { at: 2 })
	);
	// Beyond the captured commit point.
	assert_eq!(
		check_committed_prefix2(&full(), lid(1, 1, 2), &p, 0, &full(), &p[2], lid(1, 1, 2)),
		Err(PrefixError2::BeyondCaptured)
	);
	// The expected index is checked before the bound.
	let far = vec![p[1].clone(), normal(lid(1, 1, 9), 9)];
	assert_eq!(
		check(&full(), &far, 1, &full()),
		Err(PrefixError2::Gap { at: 2 })
	);
}

#[test]
fn index_max_is_checked_without_wrapping() {
	let max = lid(1, 1, u64::MAX);
	let last = normal(max, 7);
	let one = vec![last.clone()];
	assert_eq!(
		check_committed_prefix2(&full(), max, &one, u64::MAX, &full(), &last, max),
		Ok(())
	);
	let further = vec![last.clone(), normal(max, 8)];
	assert_eq!(
		check_committed_prefix2(&full(), max, &further, u64::MAX, &full(), &last, max),
		Err(PrefixError2::Overflow)
	);
}

#[test]
fn boundary_identity_is_a_whole_entry_obligation() {
	let p = prefix();
	let captured = lid(1, 1, 3);
	let run = |batch: &[E2], boundary: &E2| {
		check_committed_prefix2(&full(), captured, batch, 0, &full(), boundary, captured)
	};
	// Re-read with a different full log id (term, node).
	for id in [lid(2, 1, 3), lid(1, 2, 3)] {
		assert_eq!(run(&p, &normal(id, 3)), Err(PrefixError2::BoundaryMismatch));
	}
	// Same log id, changed payload.
	assert_eq!(
		run(&p, &normal(captured, 4)),
		Err(PrefixError2::BoundaryMismatch)
	);
	assert_eq!(
		run(&p, &blank(captured)),
		Err(PrefixError2::BoundaryMismatch)
	);
	// The batch copy carries a different term or node with an identical payload, while the
	// re-read matches `captured` exactly (note B counterexample).
	for id in [lid(2, 1, 3), lid(1, 2, 3)] {
		let mut q = p.clone();
		q[3] = normal(id, 3);
		assert_eq!(run(&q, &p[3]), Err(PrefixError2::BoundaryMismatch));
	}
	// Boundary outside the batch: only the re-read identity is checked.
	assert_eq!(run(&p[..2], &normal(captured, 99)), Ok(()));
	assert_eq!(
		run(&p[..2], &normal(lid(2, 1, 3), 3)),
		Err(PrefixError2::BoundaryMismatch)
	);
}

/// Documented limit: entries below the boundary are not compared against re-reads.
#[test]
fn altered_earlier_entry_is_not_detected() {
	let mut p = prefix();
	p[1] = normal(lid(1, 1, 1), 0xee);
	assert_eq!(check(&full(), &p, 0, &full()), Ok(()));
}

#[test]
fn committed_point_must_not_regress() {
	let p = prefix();
	let captured = lid(1, 1, 3);
	let run = |after| check_committed_prefix2(&full(), captured, &p, 0, &full(), &p[3], after);
	assert_eq!(run(lid(1, 1, 2)), Err(PrefixError2::Regressed));
	assert_eq!(run(lid(2, 1, 3)), Err(PrefixError2::Regressed));
	assert_eq!(run(lid(1, 2, 3)), Err(PrefixError2::Regressed));
	assert_eq!(run(captured), Ok(()));
	assert_eq!(run(lid(1, 1, 4)), Ok(()));
	// The boundary is checked before the commit point.
	assert_eq!(
		check_committed_prefix2(
			&full(),
			captured,
			&p,
			0,
			&full(),
			&blank(captured),
			lid(1, 1, 2)
		),
		Err(PrefixError2::BoundaryMismatch)
	);
}

/// No reachable encode failure is known for this postcard schema; the mapping is pinned through
/// the same generic encoder the checker uses.
#[test]
fn encode_failure_is_its_own_error() {
	struct Unencodable;
	impl Serialize for Unencodable {
		fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
			Err(serde::ser::Error::custom("unencodable"))
		}
	}
	assert_eq!(encode_for_test(&Unencodable), Err(PrefixError2::Encode));
	assert!(encode_for_test(&prefix()[3].payload).is_ok());
}
