//! DC-287 (Codex 01a208b1): the stalled-write trace on a real one-voter ParentDecisions store,
//! enabled through its node configuration. Two different stall sites give the same external
//! answer (WRITE_STALLED, INDETERMINATE); the trace tells them apart and joins each record to
//! its write identity:
//! - a backup hold refuses the Reserve (a real refusal, on the Reserve side);
//! - the local holder refuses its own child (forced), so Finalize stays PendingEvidence.
//!
//! A committed write records nothing. A stall decided inside an attempt whose caller's deadline
//! answered first is not published (Codex 01a20901); the same identity's next attempt, whose
//! answer is that stall, is. The published scrape carries the same records.

use super::one_voter::{one_voter, open, until_settled};
use super::*;
use crate::consensus::decisions::{DecisionCommand, DecisionRefusal, DecisionResult};
use crate::network::data_server::child_writes::{ChildRefusal, PauseStage};
use crate::network::data_server::write_stall_trace::{
	AnswerKind, StallSite, WriteStallRecord, WriteStallTrace,
};

async fn decide(node: &WendelonNode, command: DecisionCommand) -> DecisionResult {
	match node
		.raft
		.client_write(crate::consensus::WendelonRequest::Decision(command))
		.await
		.expect("the one voter applies the command")
		.data
	{
		crate::consensus::WendelonResponse::Decision(result) => result,
		other => panic!("not a decision answer: {other:?}"),
	}
}

fn attempt(descriptor: &TableSchemaDescriptor, attempts: u8) -> TableWriteRequestMeta {
	let mut meta = write_meta(descriptor, 1, uuid_v7());
	meta.set_attempts_remaining(attempts);
	meta
}

async fn write(
	node: &WendelonNode,
	meta: TableWriteRequestMeta,
	batch: RecordBatch,
) -> crate::schema::TableWriteResponseMeta {
	node.data_server
		.table_write_for_test(meta, batch)
		.await
		.unwrap()
		.0
}

fn assert_stalled(response: &crate::schema::TableWriteResponseMeta, what: &str) {
	assert_eq!(
		(
			response.write.status,
			response.write.mutation_commit_knowledge()
		),
		(
			WriteStatus::WRITE_STALLED,
			MutationCommitKnowledge::INDETERMINATE
		),
		"{what}: {response:?}"
	);
}

/// The kept records of one write identity, in order.
fn records_of(trace: &WriteStallTrace, meta: &TableWriteRequestMeta) -> Vec<WriteStallRecord> {
	trace
		.snapshot()
		.records
		.into_iter()
		.filter(|record| {
			record.client_uuid == meta.parent_write_id.client_uuid
				&& record.sequence_no == meta.parent_write_id.sequence_no
		})
		.collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_trace_tells_two_stalls_with_the_same_answer_apart_and_joins_each_to_its_write() {
	let (_dir, mut cfg) = one_voter();
	cfg.write_stall_trace = true;
	let node = open(&cfg).await;
	until_settled(&node, None).await;
	let descriptor = descriptor();
	node.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_settled(&node, Some(descriptor.table_object_id)).await;
	let trace = node
		.data_server
		.write_stall_trace()
		.expect("the configuration enables the trace")
		.clone();

	// The success control: once a first write has committed, a committed write records nothing.
	let warm = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&write_retrying(&node.data_server, warm, rows(&descriptor, &[1])).await,
		"the first write",
	);
	let before = trace.snapshot().recorded;
	let ok = attempt(&descriptor, 3);
	assert_committed(
		&write(&node, ok, rows(&descriptor, &[2])).await,
		"the success control",
	);
	assert_eq!(
		trace.snapshot().recorded,
		before,
		"a committed write records nothing"
	);
	assert!(records_of(&trace, &ok).is_empty());

	// The Reserve side: a backup hold refuses the Reserve.
	let DecisionResult::HoldEntered { id, entered_at } =
		decide(&node, DecisionCommand::BeginBackupHold { id: 0x287 }).await
	else {
		panic!("hold not entered");
	};
	let held = attempt(&descriptor, 3);
	assert_stalled(
		&write(&node, held, rows(&descriptor, &[3])).await,
		"a write under the hold",
	);
	let [reserve] = records_of(&trace, &held)[..] else {
		panic!("one record for the held write: {:?}", trace.snapshot());
	};
	assert_eq!(reserve.seq, before + 1);
	assert_eq!(reserve.site, StallSite::Reserve);
	assert_eq!(reserve.answer.kind, AnswerKind::AppliedDecision);
	assert_eq!(
		reserve.answer.decision_refusal(),
		Some(DecisionRefusal::BackupHold)
	);
	assert_eq!(reserve.attempts_remaining, 3);
	assert_eq!(reserve.holder, None);
	assert_eq!(
		decide(&node, DecisionCommand::ReleaseBackupHold { id, entered_at }).await,
		DecisionResult::HoldReleased
	);
	assert_committed(
		&write(&node, held, rows(&descriptor, &[3])).await,
		"the held identity after the release",
	);
	assert_eq!(
		records_of(&trace, &held),
		vec![reserve],
		"its commit adds nothing"
	);

	// The Finalize side: the local holder refuses its own child, on two attempts of one write.
	let refuse = &node.data_server.decision_peer_for_test().refuse_holders;
	refuse.store(true, std::sync::atomic::Ordering::Release);
	let mut refused = attempt(&descriptor, 2);
	assert_stalled(
		&write(&node, refused, rows(&descriptor, &[4])).await,
		"a write whose holder refuses",
	);
	refused.set_attempts_remaining(1);
	assert_stalled(
		&write(&node, refused, rows(&descriptor, &[4])).await,
		"its resumed attempt",
	);
	let finalize = records_of(&trace, &refused);
	assert_eq!(finalize.len(), 2, "{:?}", trace.snapshot());
	let storage = ChildRefusal::Storage(String::new()).code();
	for (record, attempts) in finalize.iter().zip([2, 1]) {
		assert_eq!(record.site, StallSite::Finalize);
		assert_eq!(record.answer.kind, AnswerKind::AppliedDecision);
		assert_eq!(
			record.answer.decision,
			Some(DecisionResult::PendingEvidence)
		);
		let holder = record.holder.expect("a Finalize keeps its holder answer");
		assert_eq!((holder.refusal, holder.acked), (Some(storage), false));
		assert_eq!(record.acks, 0);
		assert_eq!(record.attempts_remaining, attempts);
	}

	// The negative control (Codex 01a20901): an attempt held before its Finalize, past its claim,
	// while its caller's deadline answers. Released, its Finalize still meets the refusing
	// holder and stalls inside the coordinator; that stall is not its answer (the deadline is),
	// so nothing is published for it.
	let recorded = trace.snapshot().recorded;
	let (reached, release) = node
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let mut hurried = attempt(&descriptor, 3);
	hurried.deadline_remaining_millis = 300;
	let server = node.data_server.clone();
	let batch = rows(&descriptor, &[5]);
	let held =
		tokio::spawn(async move { server.table_write_for_test(hurried, batch).await.unwrap().0 });
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the attempt is held before its Finalize");
	// Past the caller's deadline (300 ms from the attempt's entry), with margin.
	tokio::time::sleep(std::time::Duration::from_millis(800)).await;
	release.notify_one();
	let answer = tokio::time::timeout(BOUND, held)
		.await
		.expect("the held attempt ends")
		.unwrap();
	assert_eq!(
		(
			answer.write.status,
			answer.write.mutation_commit_knowledge()
		),
		(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationCommitKnowledge::INDETERMINATE
		),
		"the deadline is the answer: {answer:?}"
	);
	assert!(
		records_of(&trace, &hurried).is_empty(),
		"{:?}",
		trace.snapshot()
	);
	assert_eq!(trace.snapshot().recorded, recorded, "nothing was published");
	// Its positive counterpart, the same identity with no deadline: the resumed parent's
	// Finalize stall IS its answer, and is published.
	hurried.deadline_remaining_millis = 0;
	hurried.set_attempts_remaining(1);
	assert_stalled(
		&write(&node, hurried, rows(&descriptor, &[5])).await,
		"the same identity with no deadline",
	);
	let [counterpart] = records_of(&trace, &hurried)[..] else {
		panic!("one record: {:?}", trace.snapshot());
	};
	assert_eq!(
		(counterpart.site, counterpart.attempts_remaining),
		(StallSite::Finalize, 1)
	);
	assert_eq!(counterpart.seq, recorded + 1);

	refuse.store(false, std::sync::atomic::Ordering::Release);
	refused.set_attempts_remaining(3);
	assert_committed(
		&write(&node, refused, rows(&descriptor, &[4])).await,
		"the refused identity once its holder writes",
	);
	hurried.set_attempts_remaining(3);
	assert_committed(
		&write(&node, hurried, rows(&descriptor, &[5])).await,
		"the hurried identity once its holder writes",
	);
	assert_eq!(
		records_of(&trace, &refused),
		finalize,
		"its commit adds nothing"
	);

	// Same external answer, different sites, each joined to its own identity.
	assert_ne!(reserve.site, finalize[0].site);
	let snapshot = trace.snapshot();
	assert_eq!(snapshot.recorded, before + 4);
	assert_eq!(snapshot.overwritten(), 0);

	// The scrape publishes the same records.
	let text = crate::metrics::global().render_prometheus();
	assert!(
		text.contains(&format!(
			"wendelon_data_ring_write_stall_trace{{node=\"{}\",field=\"recorded\"}} {}\n",
			trace.node(),
			snapshot.recorded
		)),
		"{text}"
	);
	for record in [reserve, finalize[0], finalize[1], counterpart] {
		let slot =
			(record.seq - 1) % crate::network::data_server::write_stall_trace::CAPACITY as u64;
		for (field, value) in record.fields() {
			let series = format!(
				"wendelon_data_ring_write_stalled_attempt{{node=\"{}\",slot=\"{slot}\",field=\"{field}\"}} {value}\n",
				trace.node()
			);
			assert!(text.contains(&series), "missing {series}");
		}
	}

	tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a complete close")
		.unwrap();
}
