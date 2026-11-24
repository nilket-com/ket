//! DC-290: the answered-attempt ring, its spans and its export, on its own.

use std::time::{Duration, Instant};

use super::*;
use crate::network::data_server::write_stall_trace::{AttemptTrace, WriteStallTrace};
use crate::schema::{TableWriteRequestMeta, WriteStatus};

fn meta(uuid: u8, attempts: u8) -> TableWriteRequestMeta {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.parent_write_id.client_uuid = [uuid; 16];
	meta.parent_write_id.sequence_no = u64::from(uuid) + 1000;
	meta.set_attempts_remaining(attempts);
	meta
}

const COMMITTED: Settled = Settled::Committed {
	dec: 1,
	commit_hlc: crate::schema::HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	},
};

fn nap(ms: u64) {
	std::thread::sleep(Duration::from_millis(ms));
}

#[test]
fn a_node_without_a_trace_marks_nothing_and_takes_no_frame_reading() {
	let meta = meta(1, 3);
	let attempt = AttemptTrace::enter(None, &meta).received(Some(Instant::now()));
	attempt.looked_up();
	attempt.children_start(2);
	attempt.children_end();
	assert!(
		!attempt.any_mark_for_test(),
		"no mark, no count and no frame reading are kept without a trace"
	);
	attempt.publish_if_answered(Some(COMMITTED));
}

#[test]
fn each_held_phase_is_at_least_its_hold_in_its_own_span() {
	let trace = WriteStallTrace::new(7);
	let meta = meta(2, 2);
	// A delay before the entry, one before the children, one in them, one after.
	let received = Instant::now();
	nap(40);
	let entered = Instant::now();
	let attempt = AttemptTrace::enter(Some(&trace), &meta).received(Some(received));
	attempt.looked_up();
	nap(20);
	attempt.looked_up();
	attempt.children_start(3);
	nap(80);
	attempt.children_end();
	// A second mark of either kind is ignored: nothing is overwritten.
	attempt.children_start(9);
	attempt.children_end();
	nap(10);
	attempt.publish_if_answered(Some(COMMITTED));
	let whole = entered.elapsed();
	let snapshot = trace.snapshot().attempts;
	assert_eq!((snapshot.recorded, snapshot.records.len()), (1, 1));
	let record = snapshot.records[0];
	assert_eq!(
		(record.seq, record.client_uuid, record.sequence_no),
		(1, [2; 16], 1002)
	);
	assert_eq!(
		(record.attempts_remaining, record.outcome, record.status),
		(2, 1, 0)
	);
	assert_eq!((record.lookups, record.children), (2, Some(3)));
	let ms = Duration::from_millis;
	assert!(record.pre.expect("a frame reading") >= ms(40));
	assert!(record.coord_before >= ms(20));
	assert!(record.children_span >= ms(80));
	assert!(record.coord_after >= ms(10));
	let sum = record.coord_before + record.children_span + record.coord_after;
	assert!(
		sum <= whole,
		"the three spans lie inside entry to answer: {sum:?} {whole:?}"
	);
	// The stalled-write ring is untouched by a success.
	assert_eq!(trace.snapshot().recorded, 0);
}

#[test]
fn an_attempt_answered_before_its_children_keeps_all_its_time_in_the_first_span() {
	let trace = WriteStallTrace::new(7);
	let meta = meta(3, 3);
	let attempt = AttemptTrace::enter(Some(&trace), &meta);
	attempt.looked_up();
	nap(15);
	let settled = attempt.stall(
		crate::network::data_server::write_stall_trace::StallSite::Reserve,
		|| crate::network::data_server::write_stall_trace::StallAnswer::STATE,
	);
	attempt.publish_if_answered(Some(settled));
	let snapshot = trace.snapshot();
	let record = snapshot.attempts.records[0];
	assert_eq!(
		(record.outcome, record.status),
		(4, WriteStatus::WRITE_STALLED.0)
	);
	assert_eq!(
		(record.children, record.pre, record.lookups),
		(None, None, 1)
	);
	assert!(record.coord_before >= Duration::from_millis(15));
	assert_eq!(
		(record.children_span, record.coord_after),
		(Duration::ZERO, Duration::ZERO)
	);
	// The same attempt is also the stalled ring's record.
	assert_eq!(snapshot.recorded, 1);
}

#[test]
fn an_attempt_whose_children_never_returned_keeps_that_time_in_the_children() {
	let trace = WriteStallTrace::new(7);
	let meta = meta(4, 3);
	let attempt = AttemptTrace::enter(Some(&trace), &meta);
	attempt.children_start(1);
	nap(15);
	attempt.publish_if_answered(Some(Settled::KnownNotCommitted));
	let record = trace.snapshot().attempts.records[0];
	assert_eq!(record.outcome, 2);
	assert!(record.children_span >= Duration::from_millis(15));
	assert_eq!(record.coord_after, Duration::ZERO);
}

#[test]
fn an_attempt_answered_by_its_callers_deadline_is_not_recorded() {
	let trace = WriteStallTrace::new(7);
	let meta = meta(5, 3);
	let attempt = AttemptTrace::enter(Some(&trace), &meta);
	attempt.children_start(1);
	attempt.children_end();
	attempt.publish_if_answered(None);
	assert_eq!(trace.snapshot().attempts, WriteAttemptSnapshot::default());
}

#[test]
fn the_ring_keeps_the_last_capacity_attempts_and_counts_the_rest() {
	let trace = WriteStallTrace::new(7);
	let total = CAPACITY as u64 + 5;
	for n in 0..total {
		let meta = meta(n as u8, 3);
		AttemptTrace::enter(Some(&trace), &meta).publish_if_answered(Some(COMMITTED));
	}
	let snapshot = trace.snapshot().attempts;
	assert_eq!(snapshot.recorded, total);
	assert_eq!(snapshot.records.len(), CAPACITY);
	assert_eq!(snapshot.overwritten(), 5);
	let seqs: Vec<u64> = snapshot.records.iter().map(|record| record.seq).collect();
	assert_eq!(
		seqs,
		(6..=total).collect::<Vec<_>>(),
		"a coherent range, oldest first"
	);
}

#[test]
fn every_outcome_has_its_own_code() {
	let codes: Vec<(u8, u8)> = [
		COMMITTED,
		Settled::KnownNotCommitted,
		Settled::Redirect(crate::schema::NodeId {
			id: 2,
			_reserved: 0,
		}),
		Settled::Indeterminate(WriteStatus::WRITE_STALLED),
		Settled::Refused(WriteStatus::WRITE_KEY_CONFLICT),
		Settled::Deadline,
	]
	.iter()
	.map(outcome_of)
	.collect();
	assert_eq!(
		codes,
		vec![
			(1, 0),
			(2, 0),
			(3, 0),
			(4, WriteStatus::WRITE_STALLED.0),
			(5, WriteStatus::WRITE_KEY_CONFLICT.0),
			(6, 0)
		]
	);
}

#[test]
fn the_record_is_one_cache_friendly_copy_and_the_ring_is_bounded() {
	// The retained storage, stated: reported in the review packet.
	eprintln!(
		"DC290 sizes: record {} B, ring {} B",
		std::mem::size_of::<WriteAttemptRecord>(),
		std::mem::size_of::<WriteAttemptRing>()
	);
	assert!(std::mem::size_of::<WriteAttemptRecord>() <= 128);
	assert!(std::mem::size_of::<WriteAttemptRing>() <= 32 * 136 + 64);
}

#[tokio::test]
async fn the_frame_reading_reaches_an_attempt_only_inside_its_scope_and_only_when_traced() {
	use crate::network::data_server::write_stall_trace::in_frame_scope;
	let trace = WriteStallTrace::new(7);
	let meta = meta(6, 3);
	let received = Instant::now();
	nap(20);
	in_frame_scope(Some(received), async {
		AttemptTrace::enter(Some(&trace), &meta)
			.with_frame_reading()
			.publish_if_answered(Some(COMMITTED));
		// An untraced attempt in the same scope takes nothing.
		assert!(
			!AttemptTrace::enter(None, &meta)
				.with_frame_reading()
				.any_mark_for_test()
		);
	})
	.await;
	// Outside a scope (a caller with no frame) the span is unknown, not zero.
	in_frame_scope(None, async {
		AttemptTrace::enter(Some(&trace), &meta)
			.with_frame_reading()
			.publish_if_answered(Some(COMMITTED));
	})
	.await;
	let records = trace.snapshot().attempts.records;
	assert!(records[0].pre.expect("inside the scope") >= Duration::from_millis(20));
	assert_eq!(records[1].pre, None);
}

/// Codex 01a20c30 R2: the span arithmetic on exact synthetic marks, with no clock and no
/// scheduler in the oracle. The three spans always sum to entry → answer exactly.
#[test]
fn the_spans_of_exact_marks_partition_entry_to_answer() {
	let ms = Duration::from_millis;
	let entered = Instant::now();
	let at = |after: u64| entered + ms(after);
	// Every mark taken: 300 ms before the children, 10,800 ms in them, 290 ms after.
	assert_eq!(
		spans(entered, Some(at(300)), Some(at(11_100)), at(11_390)),
		(ms(300), ms(10_800), ms(290))
	);
	// The children never returned: their span runs to the answer.
	assert_eq!(
		spans(entered, Some(at(300)), None, at(5_000)),
		(ms(300), ms(4_700), Duration::ZERO)
	);
	// Answered before the children: everything is before them.
	assert_eq!(
		spans(entered, None, None, at(8_100)),
		(ms(8_100), Duration::ZERO, Duration::ZERO)
	);
	// A return mark without a start mark cannot place anything after the children.
	assert_eq!(
		spans(entered, None, Some(at(40)), at(50)),
		(ms(50), Duration::ZERO, Duration::ZERO)
	);
	for (started, done, answered) in [
		(Some(at(1)), Some(at(2)), at(3)),
		(Some(at(7)), None, at(9)),
		(None, None, at(4)),
		(Some(at(0)), Some(at(0)), at(0)),
	] {
		let (before, children, after) = spans(entered, started, done, answered);
		assert_eq!(before + children + after, answered - entered);
	}
}
