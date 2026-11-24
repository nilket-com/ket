//! DC-287: the stalled-write trace's ring, codes and export, on its own.

use super::*;
use crate::consensus::decisions::{DecisionRefusal, DecisionResult};

fn meta(uuid: u8, attempts: u8) -> TableWriteRequestMeta {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.parent_write_id.client_uuid = [uuid; 16];
	meta.parent_write_id.sequence_no = u64::from(uuid) + 1000;
	meta.set_attempts_remaining(attempts);
	meta
}

/// One traced attempt whose stall, decided by `stall`, is its answer.
fn answered(
	trace: &WriteStallTrace,
	meta: &TableWriteRequestMeta,
	stall: impl FnOnce(&AttemptTrace<'_>) -> Settled,
) {
	let attempt = AttemptTrace::enter(Some(trace), meta);
	let settled = stall(&attempt);
	attempt.publish_if_answered(Some(settled));
}

#[test]
fn a_node_without_a_trace_answers_the_same_stall_and_records_nothing() {
	let meta = meta(1, 3);
	let attempt = AttemptTrace::enter(None, &meta);
	let mut read = false;
	let settled = attempt.stall(StallSite::Reserve, || {
		read = true;
		StallAnswer::STATE
	});
	assert_eq!(settled, Settled::Indeterminate(WriteStatus::WRITE_STALLED));
	assert!(!read, "the answer is not even read without a trace");
}

#[test]
fn a_traced_stall_keeps_its_identity_site_answer_and_elapsed_time() {
	let trace = WriteStallTrace::new(7);
	let meta = meta(2, 2);
	let before = Instant::now();
	let attempt = AttemptTrace::enter(Some(&trace), &meta);
	std::thread::sleep(Duration::from_millis(5));
	let answer = StallAnswer::of_proposal(&ProposalResponse::Applied {
		outcome: ProposalOutcome::Decision(DecisionResult::Refused(DecisionRefusal::BackupHold)),
		log_index: 9,
	});
	let settled = attempt.stall(StallSite::Reserve, || answer);
	assert_eq!(settled, Settled::Indeterminate(WriteStatus::WRITE_STALLED));
	let decided = before.elapsed();
	assert_eq!(trace.snapshot().recorded, 0, "held, not yet published");
	// The time kept is the inner decision's, not the publication's.
	std::thread::sleep(Duration::from_millis(30));
	attempt.publish_if_answered(Some(settled));
	let snapshot = trace.snapshot();
	assert_eq!((snapshot.recorded, snapshot.records.len()), (1, 1));
	let record = snapshot.records[0];
	assert_eq!(record.seq, 1);
	assert_eq!(record.client_uuid, [2; 16]);
	assert_eq!(record.sequence_no, 1002);
	assert_eq!(record.attempts_remaining, 2);
	assert_eq!(record.site, StallSite::Reserve);
	assert_eq!(record.answer.kind, AnswerKind::AppliedDecision);
	assert_eq!(
		record.answer.decision_refusal(),
		Some(DecisionRefusal::BackupHold)
	);
	assert_eq!(record.holder, None);
	assert!(record.elapsed >= Duration::from_millis(5));
	assert!(
		record.elapsed <= decided,
		"{:?} > {decided:?}",
		record.elapsed
	);
	assert!(record.at_unix_ms > 0);
}

/// Codex 01a20901: a stall that is not the attempt's response (its caller's deadline answered
/// first, or any other result) is never published; the trace counts nothing for it.
#[test]
fn a_stall_that_is_not_the_answer_is_not_published() {
	let trace = WriteStallTrace::new(7);
	let meta = meta(4, 3);
	let stall =
		|attempt: &AttemptTrace<'_>| attempt.stall(StallSite::Finalize, || StallAnswer::STATE);
	for answer in [
		None,
		Some(Settled::Deadline),
		Some(Settled::KnownNotCommitted),
	] {
		let attempt = AttemptTrace::enter(Some(&trace), &meta);
		stall(&attempt);
		attempt.publish_if_answered(answer);
	}
	// An attempt that decided no stall publishes nothing even if the answer is a stall.
	AttemptTrace::enter(Some(&trace), &meta)
		.publish_if_answered(Some(Settled::Indeterminate(WriteStatus::WRITE_STALLED)));
	let snapshot = trace.snapshot();
	assert_eq!(
		(
			snapshot.recorded,
			snapshot.records.len(),
			snapshot.leases.len()
		),
		(0, 0, 0)
	);
	// DC-290: the three attempts with an answer of their own are kept as answered attempts,
	// each with its outcome; the one a deadline answered is not.
	let outcomes: Vec<u8> = snapshot
		.attempts
		.records
		.iter()
		.map(|record| record.outcome)
		.collect();
	assert_eq!(outcomes, [6, 2, 4]);
	answered(&trace, &meta, stall);
	assert_eq!(trace.snapshot().recorded, 1, "the positive counterpart");
}

#[test]
fn a_finalize_stall_keeps_the_local_holder_refusal_or_says_it_is_unknown() {
	let trace = WriteStallTrace::new(7);
	let meta = meta(3, 1);
	let pending = || {
		StallAnswer::of_proposal(&ProposalResponse::Applied {
			outcome: ProposalOutcome::Decision(DecisionResult::PendingEvidence),
			log_index: 9,
		})
	};
	let mut refused = HolderSeen::default();
	refused.refused(&ChildRefusal::LeaseMissing);
	refused.refused(&ChildRefusal::NotCaughtUp);
	for (holder, acks) in [
		(refused, 0),
		(HolderSeen::default(), 0),
		(
			HolderSeen {
				refusal: None,
				acked: true,
			},
			1,
		),
	] {
		answered(&trace, &meta, |attempt| {
			attempt.stall_finalize(pending, holder, acks, None)
		});
	}
	let records = trace.snapshot().records;
	let field = |record: &WriteStallRecord, name: &str| {
		record
			.fields()
			.into_iter()
			.find(|(field, _)| *field == name)
			.unwrap()
			.1
	};
	// The first refusal observed is kept.
	assert_eq!(
		records[0].holder.unwrap().refusal,
		Some(ChildRefusal::LeaseMissing.code())
	);
	assert_eq!(
		field(&records[0], "holder"),
		3 + u64::from(ChildRefusal::LeaseMissing.code())
	);
	assert_eq!(
		field(&records[1], "holder"),
		1,
		"no local holder answer: unknown"
	);
	assert_eq!(
		field(&records[2], "holder"),
		2,
		"the local holder wrote its child"
	);
	assert_eq!(field(&records[2], "acks"), 1);
	for record in &records {
		assert_eq!(record.site, StallSite::Finalize);
		assert_eq!(
			record.answer.decision,
			Some(DecisionResult::PendingEvidence)
		);
		assert_eq!(field(record, "site"), StallSite::Finalize as u64);
		// PendingEvidence is DecisionResult's third variant.
		assert_eq!(field(record, "decision"), 3);
	}
}

#[test]
fn the_ring_keeps_the_newest_records_and_counts_every_overwritten_one() {
	let trace = WriteStallTrace::new(7);
	let total = CAPACITY as u64 + 5;
	for n in 0..total {
		let meta = meta(n as u8, 3);
		answered(&trace, &meta, |attempt| {
			attempt.stall(StallSite::Lookup, || StallAnswer::STATE)
		});
	}
	let snapshot = trace.snapshot();
	assert_eq!(snapshot.recorded, total);
	assert_eq!(snapshot.records.len(), CAPACITY);
	assert_eq!(snapshot.overwritten(), 5);
	let seqs: Vec<u64> = snapshot.records.iter().map(|record| record.seq).collect();
	assert_eq!(
		seqs,
		(6..=total).collect::<Vec<_>>(),
		"the newest, oldest first"
	);
	for record in &snapshot.records {
		assert_eq!(
			record.client_uuid,
			[(record.seq - 1) as u8; 16],
			"each kept record is its own attempt's"
		);
	}
}

#[test]
fn the_exported_identity_and_codes_are_exact() {
	let trace = WriteStallTrace::new(7);
	let mut meta = meta(0, 3);
	meta.parent_write_id.client_uuid = [
		0x01, 0xa2, 0x08, 0xa5, 0xc7, 0x00, 0x7f, 0xff, 0x80, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff,
		0xfe,
	];
	answered(&trace, &meta, |attempt| {
		attempt.stall(StallSite::Lookup, || {
			StallAnswer::of_lookup(&LookupResponse::Refused(ProposalRefusal::ClockUnhealthy))
		})
	});
	let record = trace.snapshot().records[0];
	let fields: std::collections::BTreeMap<_, _> = record.fields().into_iter().collect();
	assert_eq!(fields["client_uuid_w0"], 0x01a2_08a5);
	assert_eq!(fields["client_uuid_w1"], 0xc700_7fff);
	assert_eq!(fields["client_uuid_w2"], 0x8000_0000);
	assert_eq!(fields["client_uuid_w3"], 0xffff_fffe);
	assert_eq!(
		(fields["sequence_no_hi"], fields["sequence_no_lo"]),
		(0, 1000)
	);
	assert_eq!(fields["site"], StallSite::Lookup as u64);
	assert_eq!(fields["site"], 16);
	assert_eq!(fields["answer"], AnswerKind::Refused as u64);
	// ClockUnhealthy is ProposalRefusal's second variant.
	assert_eq!(fields["proposal_refusal"], 2);
	assert_eq!(fields["decision"], 0);
	assert_eq!(fields["lookup"], 0);
	assert_eq!(fields["holder"], 0, "not a Finalize");
	assert_eq!(fields.len(), record.fields().len(), "each field once");
}

/// What a scrape reader that parses sample values as f64 (the S8 reader's `Scrape::parse`)
/// reads back: each value through its rendered decimal text.
fn through_f64(value: u64) -> u64 {
	let read: f64 = value.to_string().parse().unwrap();
	read as u64
}

/// The identity a reader rebuilds from the exported words.
fn rebuilt(fields: &std::collections::BTreeMap<&str, u64>) -> ([u8; 16], u64) {
	let mut uuid = [0u8; 16];
	for (at, word) in [
		"client_uuid_w0",
		"client_uuid_w1",
		"client_uuid_w2",
		"client_uuid_w3",
	]
	.into_iter()
	.enumerate()
	{
		let word = u32::try_from(fields[word]).expect("a 32-bit word");
		uuid[at * 4..at * 4 + 4].copy_from_slice(&word.to_be_bytes());
	}
	(
		uuid,
		(fields["sequence_no_hi"] << 32) | fields["sequence_no_lo"],
	)
}

/// Codex 01a208cb: the failed write ids of sealed batches 56–58 (block
/// `…-7ac6f1cf47c424d9`), and ids differing from one of them only in a bit that a 64-bit half
/// loses in f64, all round-trip exactly through an f64 reader of the exported words; so does a
/// sequence number above 2^53.
#[test]
fn a_failed_write_id_round_trips_through_an_f64_scrape_reader() {
	let sealed: [[u8; 16]; 3] = [
		[
			1, 162, 8, 165, 199, 251, 116, 112, 169, 249, 220, 69, 92, 12, 209, 177,
		],
		[
			1, 162, 8, 165, 206, 99, 114, 131, 163, 192, 242, 184, 43, 50, 129, 134,
		],
		[
			1, 162, 8, 165, 212, 201, 124, 83, 187, 187, 51, 127, 255, 53, 254, 154,
		],
	];
	// The negative control: batch 56's id with its last bit flipped. Its 64-bit low half and the
	// original's are one f64 value, so the earlier hi/lo export could not tell them apart.
	let mut flipped = sealed[0];
	flipped[15] ^= 1;
	let low = |uuid: &[u8; 16]| u64::from_be_bytes(uuid[8..].try_into().unwrap());
	assert_eq!(low(&sealed[0]), 12_248_062_852_065_972_657);
	assert_eq!(
		through_f64(low(&sealed[0])),
		through_f64(low(&flipped)),
		"the control is sensitive: a 64-bit half loses this bit"
	);
	let trace = WriteStallTrace::new(7);
	let ids: Vec<([u8; 16], u64)> = sealed
		.iter()
		.map(|uuid| (*uuid, 0))
		.chain([(flipped, 0), (sealed[1], (1 << 53) + 1)])
		.collect();
	for (uuid, sequence_no) in &ids {
		let mut meta = meta(0, 3);
		meta.parent_write_id.client_uuid = *uuid;
		meta.parent_write_id.sequence_no = *sequence_no;
		answered(&trace, &meta, |attempt| {
			attempt.stall(StallSite::Finalize, || StallAnswer::STATE)
		});
	}
	let read: Vec<([u8; 16], u64)> = trace
		.snapshot()
		.records
		.iter()
		.map(|record| {
			let fields = record
				.fields()
				.into_iter()
				.map(|(field, value)| (field, through_f64(value)))
				.collect();
			rebuilt(&fields)
		})
		.collect();
	assert_eq!(read, ids, "every id, exactly, in order");
	assert_ne!(
		read[0], read[3],
		"ids one rounded-away bit apart stay distinct"
	);
}

#[test]
fn every_site_and_holder_refusal_has_its_own_code() {
	use StallSite::*;
	let sites = [
		ResumeNoop,
		ResumeNoCoordinatorChild,
		ResumeUnroutable,
		ResumeAuthorityEnded,
		ClaimNoPosition,
		ClaimRangeUnavailable,
		ClaimNoDecisions,
		ClaimFirstLineage,
		ClaimEpoch,
		ClaimTenant,
		ClaimSegment,
		ClaimChildLineage,
		ClaimPlan,
		ClaimCoordinator,
		Reserve,
		Lookup,
		Finalize,
	];
	let codes: std::collections::BTreeSet<u8> = sites.iter().map(|site| *site as u8).collect();
	assert_eq!(codes, (1..=17).collect());
	let refusals = [
		ChildRefusal::ModeToday,
		ChildRefusal::NotCaughtUp,
		ChildRefusal::UnknownReservation,
		ChildRefusal::ClaimMismatch,
		ChildRefusal::WrongSender,
		ChildRefusal::NotInPlan,
		ChildRefusal::LineageClosed,
		ChildRefusal::LeaseMissing,
		ChildRefusal::SchemaUnknown,
		ChildRefusal::OutOfRange,
		ChildRefusal::ContentMismatch,
		ChildRefusal::ParentRefused,
		ChildRefusal::ParentAccepted,
		ChildRefusal::NotAccepted,
		ChildRefusal::Unverified(String::new()),
		ChildRefusal::Storage(String::new()),
	];
	let codes: std::collections::BTreeSet<u8> = refusals.iter().map(ChildRefusal::code).collect();
	assert_eq!(codes.len(), refusals.len());
	assert!(codes.iter().all(|code| (1..=18).contains(code)));
	// The serialized variant index the export uses, at its two ends.
	assert_eq!(variant_index(&DecisionRefusal::ModeToday), 0);
	assert_eq!(variant_index(&DecisionResult::PendingEvidence), 2);
	assert_eq!(variant_index(&ProposalRefusal::ModeToday), 0);
}
