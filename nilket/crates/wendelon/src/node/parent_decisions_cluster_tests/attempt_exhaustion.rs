//! DC-212: what a last attempt (`attempts_remaining == 1`) may say about a
//! table write under ParentDecisions. The server's last-attempt rule does
//! nothing in this mode: the coordinator's or the admission's answer goes out
//! as it is, and no node stores a mutation outcome for the identity. The
//! parent record is the only durable outcome; exhaustion is the client's own
//! summary of the answers it received.
//!
//! The cluster controls send every request through the stream entry an
//! inbound request takes (the node's published service), so the server's
//! last-attempt rule runs. The two scripted controls at the end run the public
//! client's retry loop alone, against scripted answers and no cluster.

use super::write_deadline::{ReleaseOnDrop, StreamedWrite, handler_end, shut_down, until_reached};
use super::*;
use crate::client::{
	ClusterPeerWire, ClusterStatusWire, StaticTopologySource, TableWriteRequest, WendelonClient,
	WendelonClientConfig,
};
use crate::network::core::{HRFT_MAGIC, HrftFrame};
use crate::network::data_server::child_writes::PauseStage;
use crate::network::pool::PooledBuf;
use crate::network::server::{HrftService, RequestContext};
use crate::network::transport::{
	NodeRole, NodeRoleSet, TransportProvider, TransportStream, TransportTierSet,
};
use crate::schema::{
	FrameHeader, MsgType, MutationStopReason, TableWriteResponseMeta, WriteResponseMeta,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The mutation outcome each node has durably stored for `meta`'s identity, if
/// any: every node's own store, read as it stands.
fn stored_outcomes(
	cluster: &Cluster,
	meta: &TableWriteRequestMeta,
) -> Vec<(u32, WriteResponseMeta)> {
	NODES
		.into_iter()
		.filter_map(|node| {
			cluster
				.node(node)
				.data_server
				.stored_mutation_outcome_for_test(meta.parent_write_id, meta.tenant_id)
				.map(|outcome| (node, outcome))
		})
		.collect()
}

fn status_name(status: WriteStatus) -> String {
	match status {
		s if s == WriteStatus::OK => "OK".to_owned(),
		s if s == WriteStatus::NOT_LEADER => "NOT_LEADER".to_owned(),
		s if s == WriteStatus::BACKPRESSURED => "BACKPRESSURED".to_owned(),
		s if s == WriteStatus::WRITE_STALLED => "WRITE_STALLED".to_owned(),
		s if s == WriteStatus::WRITE_ATTEMPTS_EXHAUSTED => "WRITE_ATTEMPTS_EXHAUSTED".to_owned(),
		other => format!("{other:?}"),
	}
}

fn knowledge_name(knowledge: MutationCommitKnowledge) -> String {
	match knowledge {
		k if k == MutationCommitKnowledge::UNSPECIFIED => "UNSPECIFIED".to_owned(),
		k if k == MutationCommitKnowledge::COMMITTED => "COMMITTED".to_owned(),
		k if k == MutationCommitKnowledge::KNOWN_NOT_COMMITTED => "KNOWN_NOT_COMMITTED".to_owned(),
		k if k == MutationCommitKnowledge::INDETERMINATE => "INDETERMINATE".to_owned(),
		other => format!("{other:?}"),
	}
}

fn stop_name(stop: MutationStopReason) -> String {
	match stop {
		s if s == MutationStopReason::NONE => "NONE".to_owned(),
		s if s == MutationStopReason::DEADLINE => "DEADLINE".to_owned(),
		s if s == MutationStopReason::ATTEMPT_EXHAUSTION => "ATTEMPT_EXHAUSTION".to_owned(),
		other => format!("{other:?}"),
	}
}

fn answer_text(answer: &WriteResponseMeta) -> String {
	format!(
		"status {} knowledge {} stop {} hint {}",
		status_name(answer.status),
		knowledge_name(answer.mutation_commit_knowledge()),
		stop_name(answer.mutation_stop_reason()),
		answer.leader_hint.id
	)
}

fn outcome_text(outcomes: &[(u32, WriteResponseMeta)]) -> String {
	if outcomes.is_empty() {
		return "none".to_owned();
	}
	outcomes
		.iter()
		.map(|(node, outcome)| format!("at node {node}: {}", answer_text(outcome)))
		.collect::<Vec<_>>()
		.join("; ")
}

/// An answer's four parts, exactly.
fn assert_answer(
	answer: &WriteResponseMeta,
	status: WriteStatus,
	knowledge: MutationCommitKnowledge,
	stop: MutationStopReason,
	hint: u32,
	what: &str,
) {
	assert_eq!(
		(
			status_name(answer.status),
			knowledge_name(answer.mutation_commit_knowledge()),
			stop_name(answer.mutation_stop_reason()),
			answer.leader_hint.id
		),
		(
			status_name(status),
			knowledge_name(knowledge),
			stop_name(stop),
			hint
		),
		"{what}: answered {}",
		answer_text(answer)
	);
}

/// A redirect as the coordinator gives it on any attempt: not rewritten.
fn assert_unchanged_redirect(answer: &WriteResponseMeta, holder: u32, what: &str) {
	assert_answer(
		answer,
		WriteStatus::NOT_LEADER,
		MutationCommitKnowledge::INDETERMINATE,
		MutationStopReason::NONE,
		holder,
		what,
	);
}

/// No node stores any mutation outcome for the identity: not a refusal, and
/// not an indeterminate one either.
fn assert_no_outcome_row(cluster: &Cluster, meta: &TableWriteRequestMeta, what: &str) {
	let stored = stored_outcomes(cluster, meta);
	assert!(
		stored.is_empty(),
		"{what}: a mutation outcome is stored for a ParentDecisions table write: {}",
		outcome_text(&stored)
	);
}

/// One request on a stream into `node`'s published service, to its answer and
/// the end of its handler.
async fn answer_of(
	cluster: &Cluster,
	node: u32,
	meta: TableWriteRequestMeta,
	batch: RecordBatch,
	what: &str,
) -> TableWriteResponseMeta {
	let mut write = StreamedWrite::send(cluster, node, meta, batch, false).await;
	let answer = tokio::time::timeout(BOUND, write.response(&cluster.node(node).pool))
		.await
		.unwrap_or_else(|_| panic!("{what}: no answer"))
		.unwrap();
	handler_end(what, &mut write).await.unwrap();
	answer
}

/// Wire level (`attempts_remaining` is set by this test, not by a client).
/// The original attempt is held at its coordinator after its Reserve. The same
/// identity, as a last attempt at another node, is redirected from the
/// Reserved record: the redirect goes out unchanged and nothing is stored.
/// Released, the original finalizes the same parent Accepted and the row
/// reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_last_attempt_redirected_from_a_reserved_parent_stays_indeterminate_and_records_no_refusal()
 {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	// Any node other than the coordinator is redirected from the record.
	let other = leader;
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.set_attempts_remaining(3);
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::AfterReserve);
	let held = ReleaseOnDrop(release);
	let mut original = StreamedWrite::send(&cluster, primary, meta, batch.clone(), false).await;
	until_reached("the original after Reserve", &reached).await;
	let reserved = record_of(&cluster, key).await.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);
	assert_eq!(
		reserved.claim.children[usize::from(reserved.claim.coordinator)]
			.holder
			.node,
		primary,
		"the recorded coordinator is the original's node"
	);

	// The last attempt, at a node that is not the recorded coordinator.
	let mut last = meta;
	last.set_attempts_remaining(1);
	let answer = answer_of(&cluster, other, last, batch, "the last attempt")
		.await
		.write;
	let stored = stored_outcomes(&cluster, &meta);
	let while_held = record_of(&cluster, key).await.unwrap();
	eprintln!(
		"DC212 wire: last attempt at node {other} (coordinator {primary}) answered {}",
		answer_text(&answer)
	);
	eprintln!(
		"DC212 wire: mutation outcomes stored after the answer: {}",
		outcome_text(&stored)
	);
	eprintln!(
		"DC212 wire: parent status when answered: {:?}",
		while_held.status
	);
	assert_eq!(while_held, reserved, "the original is still held");
	assert_unchanged_redirect(
		&answer,
		primary,
		"wire level, a last attempt redirected from a Reserved parent",
	);
	assert_no_outcome_row(
		&cluster,
		&meta,
		"wire level, after the last attempt's answer",
	);

	// Released, the original completes the same reservation.
	drop(held);
	let original_answer =
		tokio::time::timeout(BOUND, original.response(&cluster.node(primary).pool))
			.await
			.unwrap()
			.unwrap();
	handler_end("the original", &mut original).await.unwrap();
	let records = records_of(&cluster, key).await;
	eprintln!(
		"DC212 wire: after release the original answered {}; parent records: {:?}",
		answer_text(&original_answer.write),
		records
			.iter()
			.map(|record| (record.rsv, record.status))
			.collect::<Vec<_>>()
	);
	assert_committed(&original_answer, "the original, released");
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	assert!(
		matches!(records[0].status, ParentStatus::Accepted { .. }),
		"{:?}",
		records[0]
	);
	assert_eq!(records[0].rsv, reserved.rsv);
	expect_everywhere(
		&cluster,
		&descriptor,
		&[(id, format!("row-{id}"))].into(),
		"the parent the last attempt was redirected from",
	)
	.await;
	assert_no_outcome_row(&cluster, &meta, "wire level, after the parent was Accepted");
	shut_down(cluster).await;
}

/// Control: a last attempt redirected with NO record under its identity. The
/// lookup answered Absent and this attempt proposed nothing, but the redirect
/// is answered as on any other attempt: conservatively INDETERMINATE, because
/// the node does not tell the two redirects apart and cannot speak for another
/// submission of the identity. Nothing is reserved and nothing is stored. A
/// later call of the identity at its coordinator commits as a first one.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_last_attempt_redirected_with_no_record_stays_indeterminate_and_reserves_nothing() {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.set_attempts_remaining(1);
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);

	let answer = answer_of(&cluster, leader, meta, batch.clone(), "the last attempt")
		.await
		.write;
	eprintln!(
		"DC212 control: last attempt at node {leader} (holder {primary}), no record: answered {}; \
		 mutation outcomes stored: {}",
		answer_text(&answer),
		outcome_text(&stored_outcomes(&cluster, &meta))
	);
	assert_unchanged_redirect(&answer, primary, "a last attempt redirected with no record");
	assert!(
		record_of(&cluster, key).await.is_none(),
		"nothing was reserved"
	);
	assert_no_outcome_row(&cluster, &meta, "after the redirect with no record");

	let mut again = meta;
	again.set_attempts_remaining(3);
	let committed = answer_of(&cluster, primary, again, batch, "the later call").await;
	eprintln!(
		"DC212 control: the same identity later at its coordinator answered {}",
		answer_text(&committed.write)
	);
	assert_committed(&committed, "the same identity later");
	assert_eq!(records_of(&cluster, key).await.len(), 1);
	assert_no_outcome_row(&cluster, &meta, "after the later call committed");
	shut_down(cluster).await;
}

/// Control: the authoritative definite negative. A recorded Refused parent
/// answers OK + KNOWN_NOT_COMMITTED from the record, at its coordinator and at
/// another node, whatever `attempts_remaining` says, and stores no mutation
/// outcome: the record is the outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_recorded_refused_parent_answers_known_not_committed_whatever_attempts_remain() {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let other = leader;
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.set_attempts_remaining(3);
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);

	// Reserved, then refused by its coordinator's own signed abort.
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let held = ReleaseOnDrop(release);
	let mut original = StreamedWrite::send(&cluster, primary, meta, batch.clone(), false).await;
	until_reached("the original before Finalize", &reached).await;
	let reserved = record_of(&cluster, key).await.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);
	abort(&cluster, &reserved).await;
	let refused = record_of(&cluster, key).await.unwrap();
	assert!(
		matches!(refused.status, ParentStatus::Refused { .. }),
		"{refused:?}"
	);
	drop(held);
	let original_answer =
		tokio::time::timeout(BOUND, original.response(&cluster.node(primary).pool))
			.await
			.unwrap()
			.unwrap();
	handler_end("the original", &mut original).await.unwrap();
	assert_eq!(
		original_answer.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
		"the original, released after the abort: {}",
		answer_text(&original_answer.write)
	);

	for (node, remaining) in [(other, 1), (primary, 1), (other, 3)] {
		let what = format!("a Refused parent asked at node {node} with {remaining} remaining");
		let mut asked = meta;
		asked.set_attempts_remaining(remaining);
		let answer = answer_of(&cluster, node, asked, batch.clone(), &what)
			.await
			.write;
		eprintln!("DC212 refused: {what}: answered {}", answer_text(&answer));
		assert_answer(
			&answer,
			WriteStatus::OK,
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
			MutationStopReason::NONE,
			node,
			&what,
		);
	}
	let records = records_of(&cluster, key).await;
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	assert!(
		matches!(records[0].status, ParentStatus::Refused { .. }),
		"{:?}",
		records[0]
	);
	assert_no_outcome_row(&cluster, &meta, "a recorded Refused parent");
	shut_down(cluster).await;
}

/// The server's side of the single-call sequence (a stalled attempt, then a
/// last attempt refused at admission); the client's side is the scripted
/// control below. A last attempt that this node's tenant admission refuses is
/// answered BACKPRESSURED as on any other attempt, not
/// WRITE_ATTEMPTS_EXHAUSTED, with no knowledge claimed, and nothing is
/// stored: the node has not looked the identity up.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_last_attempt_refused_at_admission_is_answered_backpressured_and_unrecorded() {
	let (cluster, leader, descriptor) = ready().await;
	let (id, _) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let node = leader;
	let server = &cluster.node(node).data_server;

	// The tenant admission is full: a limit of one, taken by an unrelated
	// write held before it would reserve.
	let (filler_id, _) = row_off_the_leader(&cluster, leader, &descriptor, id + 1).await;
	server.set_request_backpressure_limits_for_test(1, 1024);
	let (filler_reached, filler_release) =
		server.pause_coordinator_for_test(PauseStage::AfterAbsent);
	let filler_held = ReleaseOnDrop(filler_release);
	let mut filler = StreamedWrite::send(
		&cluster,
		node,
		write_meta(&descriptor, 1, uuid_v7()),
		rows(&descriptor, &[filler_id]),
		false,
	)
	.await;
	until_reached("the unrelated write, admitted and held", &filler_reached).await;

	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.set_attempts_remaining(1);
	let key = key_of(&descriptor, &meta);
	let answer = answer_of(
		&cluster,
		node,
		meta,
		rows(&descriptor, &[id]),
		"the last attempt",
	)
	.await
	.write;
	eprintln!(
		"DC212 admission: last attempt at node {node}, admission full: answered {}; mutation \
		 outcomes stored: {}",
		answer_text(&answer),
		outcome_text(&stored_outcomes(&cluster, &meta))
	);
	assert_answer(
		&answer,
		WriteStatus::BACKPRESSURED,
		MutationCommitKnowledge::UNSPECIFIED,
		MutationStopReason::NONE,
		node,
		"a last attempt refused at admission",
	);
	assert!(
		record_of(&cluster, key).await.is_none(),
		"nothing was reserved"
	);
	assert_no_outcome_row(&cluster, &meta, "after the admission refusal");

	drop(filler_held);
	handler_end("the unrelated write", &mut filler)
		.await
		.unwrap();
	server.set_request_backpressure_limits_for_test(1024, 1024);
	shut_down(cluster).await;
}

/// A node's published service as a client's transport reaches it, with one
/// stream held before it is handed over: stream number `gate_at` (from 0)
/// signals `arrived` and waits for `proceed`.
struct GatedService {
	inner: Arc<dyn HrftService>,
	streams: AtomicUsize,
	gate_at: usize,
	arrived: Arc<tokio::sync::Notify>,
	proceed: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl HrftService for GatedService {
	async fn handle_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		self.inner.handle_frame(context, frame).await
	}

	async fn handle_stream(
		&self,
		stream: Box<dyn TransportStream>,
		pool: Arc<BufPool>,
		context: RequestContext,
	) -> std::io::Result<()> {
		if self.streams.fetch_add(1, Ordering::SeqCst) == self.gate_at {
			self.arrived.notify_one();
			self.proceed.notified().await;
		}
		self.inner.handle_stream(stream, pool, context).await
	}
}

/// A public client, as node `client_id` of `network`, that knows every node
/// at `epoch` and believes `enter` leads.
fn client_entering_at(
	epoch: u64,
	network: &Arc<SimulatedTransportNetwork>,
	client_id: u32,
	enter: u32,
) -> Arc<WendelonClient> {
	let tier = TransportTier::QuicIoUring;
	let status = ClusterStatusWire {
		epoch,
		leader: Some(enter),
		replication_factor: NODES.len() as u32,
		peers: NODES
			.into_iter()
			.map(|node_id| ClusterPeerWire {
				node_id,
				address: address(node_id),
				tier_bits: TransportTierSet::empty().with(tier).as_u8(),
				role_bits: NodeRoleSet::empty().with(NodeRole::Data).as_u8(),
				capability_bits: 0,
			})
			.collect(),
	};
	let provider: Arc<dyn TransportProvider> = Arc::new(SimulatedTransportProvider::new(
		crate::schema::NodeId {
			id: client_id,
			_reserved: 0,
		},
		network.clone(),
		crate::network::pool::BufPool::new(64, 1024 * 1024),
		tier,
	));
	WendelonClient::new(WendelonClientConfig {
		local_tiers: TransportTierSet::empty().with(tier),
		providers: [(tier, provider)].into(),
		bootstrap: status.clone(),
		topology: Arc::new(StaticTopologySource::new(status)),
		pool_max_retained: 64,
		pool_max_cap_bytes: 1024 * 1024,
	})
}

/// Public client (no request field is set by hand). One application identity
/// is submitted twice through `WendelonClient::table_write`:
///
/// 1. The first call enters at the row's coordinator and is held there after
///    its Reserve (an in-flight original).
/// 2. The second call (the same identity resubmitted while the first is
///    outstanding) enters at another node, whose tenant admission is full for
///    its first two attempts. Before its third attempt is handed to the node
///    the admission is freed, so the third (the client's last,
///    `attempts_remaining == 1`) is admitted, finds the Reserved record and is
///    redirected from it. The redirect reaches the client unchanged, and the
///    client's own exhaustion reports WRITE_ATTEMPTS_EXHAUSTED + INDETERMINATE.
///
/// The fixture's part: the pause, the admission limit, and the transport gate
/// that frees the admission before the third stream. The attempts, their
/// targets and `attempts_remaining` are the client's own.
///
/// Observed here: three streams of the second call reached the other node,
/// and the call's result. NOT captured: the first two answers. That they were
/// BACKPRESSURED is inferred from the fixture (the admission was full while
/// those two streams were served, and only a BACKPRESSURED answer keeps the
/// client on that node for a further attempt) and from the client loop. The
/// result's hint is the coordinator, which is the third answer's hint: a
/// BACKPRESSURED third answer would have hinted the other node.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_public_client_is_not_told_known_not_committed_for_a_parent_another_call_holds_reserved()
 {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let other = leader;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);
	let request = TableWriteRequest {
		meta,
		descriptor: descriptor.clone(),
		payload: batch,
		deadline: None,
	};

	// The clients' own network: every node's published service, the other
	// node's behind the gate at its third stream.
	let network = Arc::new(SimulatedTransportNetwork::default());
	let (arrived, proceed) = (
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(tokio::sync::Notify::new()),
	);
	let gated = Arc::new(GatedService {
		inner: cluster.node(other).service.clone(),
		streams: AtomicUsize::new(0),
		gate_at: 2,
		arrived: arrived.clone(),
		proceed: proceed.clone(),
	});
	for node in NODES {
		let service: Arc<dyn HrftService> = if node == other {
			gated.clone()
		} else {
			cluster.node(node).service.clone()
		};
		network.publish(
			crate::schema::NodeId {
				id: node,
				_reserved: 0,
			},
			&address(node),
			service,
		);
	}
	let epoch = cluster.node(other).ring.current_epoch();

	// 1. The first call, held at the coordinator after its Reserve.
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::AfterReserve);
	let held = ReleaseOnDrop(release);
	let first_client = client_entering_at(epoch, &network, 98, primary);
	let first_request = request.clone();
	let first = tokio::spawn(async move { first_client.table_write(first_request).await });
	until_reached("the first call after Reserve", &reached).await;
	let reserved = record_of(&cluster, key).await.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);

	// The other node's tenant admission is full: a limit of one, taken by an
	// unrelated write held before it would reserve.
	let (filler_id, _) = row_off_the_leader(&cluster, leader, &descriptor, id + 1).await;
	let other_server = &cluster.node(other).data_server;
	other_server.set_request_backpressure_limits_for_test(1, 1024);
	let (filler_reached, filler_release) =
		other_server.pause_coordinator_for_test(PauseStage::AfterAbsent);
	let filler_held = ReleaseOnDrop(filler_release);
	let mut filler = StreamedWrite::send(
		&cluster,
		other,
		write_meta(&descriptor, 1, uuid_v7()),
		rows(&descriptor, &[filler_id]),
		false,
	)
	.await;
	until_reached("the unrelated write, admitted and held", &filler_reached).await;

	// 2. The second call of the same identity, entering at the other node.
	let second_client = client_entering_at(epoch, &network, 99, other);
	let second = tokio::spawn(async move { second_client.table_write(request).await });
	until_reached("the second call's third stream", &arrived).await;
	// Two streams have been served. Free the admission, then let the third in.
	drop(filler_held);
	handler_end("the unrelated write", &mut filler)
		.await
		.unwrap();
	other_server.set_request_backpressure_limits_for_test(1024, 1024);
	proceed.notify_one();
	let answer = tokio::time::timeout(BOUND, second)
		.await
		.expect("the second call returns while the first is held")
		.unwrap()
		.unwrap()
		.response
		.write;
	let stored = stored_outcomes(&cluster, &meta);
	let while_held = record_of(&cluster, key).await.unwrap();
	eprintln!(
		"DC212 client: the second call (entered at node {other}, coordinator {primary}) returned {} \
		 after {} streams to node {other}",
		answer_text(&answer),
		gated.streams.load(Ordering::SeqCst)
	);
	eprintln!(
		"DC212 client: mutation outcomes stored after the answer: {}",
		outcome_text(&stored)
	);
	eprintln!(
		"DC212 client: parent status when answered: {:?}",
		while_held.status
	);
	assert_eq!(gated.streams.load(Ordering::SeqCst), 3, "three attempts");
	assert_eq!(while_held, reserved, "the first call is still held");
	assert_answer(
		&answer,
		WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
		MutationCommitKnowledge::INDETERMINATE,
		MutationStopReason::ATTEMPT_EXHAUSTION,
		primary,
		"public client, the second call of a parent the first call holds Reserved",
	);
	assert_no_outcome_row(
		&cluster,
		&meta,
		"public client, after the second call returned",
	);

	// Released, the first call completes the same reservation.
	drop(held);
	let first_answer = tokio::time::timeout(BOUND, first)
		.await
		.unwrap()
		.unwrap()
		.unwrap()
		.response;
	let records = records_of(&cluster, key).await;
	eprintln!(
		"DC212 client: after release the first call returned {}; parent records: {:?}",
		answer_text(&first_answer.write),
		records
			.iter()
			.map(|record| (record.rsv, record.status))
			.collect::<Vec<_>>()
	);
	assert_committed(&first_answer, "the first call, released");
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	assert!(
		matches!(records[0].status, ParentStatus::Accepted { .. }),
		"{:?}",
		records[0]
	);
	assert_eq!(records[0].rsv, reserved.rsv);
	expect_everywhere(
		&cluster,
		&descriptor,
		&[(id, format!("row-{id}"))].into(),
		"the parent the second call was told about",
	)
	.await;
	assert_no_outcome_row(
		&cluster,
		&meta,
		"public client, after the parent was Accepted",
	);
	shut_down(cluster).await;
}

/// A scripted data node for the public client's retry loop: each table write
/// request is answered with the next scripted answer, hinting this node, and
/// its `attempts_remaining` is noted. No server code runs.
struct ScriptedTableWrites {
	node: u32,
	/// The node each answer hints.
	hint: u32,
	/// The parent write id of each request received.
	write_ids: std::sync::Mutex<Vec<crate::schema::ClientWriteId>>,
	answers: std::sync::Mutex<VecDeque<(WriteStatus, MutationCommitKnowledge)>>,
	attempts_remaining: std::sync::Mutex<Vec<u8>>,
}

#[async_trait::async_trait]
impl HrftService for ScriptedTableWrites {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		assert_eq!(frame.header.msg_type, MsgType::TABLE_WRITE_REQ.0);
		let request: TableWriteRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableWriteRequestMeta>()]);
		self.attempts_remaining
			.lock()
			.unwrap()
			.push(request.attempts_remaining());
		self.write_ids.lock().unwrap().push(request.parent_write_id);
		let (status, knowledge) = self
			.answers
			.lock()
			.unwrap()
			.pop_front()
			.expect("an attempt beyond the script");
		let mut write: WriteResponseMeta = bytemuck::Zeroable::zeroed();
		write.status = status;
		write.epoch = request.expected_epoch;
		write.commit_hlc = request.client_hlc;
		write.leader_hint = crate::schema::NodeId {
			id: self.hint,
			_reserved: 0,
		};
		write.echoed_write_id = request.parent_write_id;
		let response = TableWriteResponseMeta {
			write: write.with_mutation_outcome(knowledge, MutationStopReason::NONE),
			reject_code: 0,
			_pad: [0; 6],
		};
		Ok(Some(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_WRITE_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: std::mem::size_of::<TableWriteResponseMeta>() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: PooledBuf::orphan(bytemuck::bytes_of(&response).to_vec()),
			arrow_payload: None,
			trailing: None,
		}))
	}
}

/// One `WendelonClient::table_write` call against scripted answers: its
/// result, and the `attempts_remaining` of each request the client sent.
async fn client_call_against(
	script: &[(WriteStatus, MutationCommitKnowledge)],
) -> (WriteResponseMeta, Vec<u8>) {
	let enter = NODES[0];
	let scripted = Arc::new(ScriptedTableWrites {
		node: enter,
		hint: enter,
		write_ids: std::sync::Mutex::new(Vec::new()),
		answers: std::sync::Mutex::new(script.iter().copied().collect()),
		attempts_remaining: std::sync::Mutex::new(Vec::new()),
	});
	let network = Arc::new(SimulatedTransportNetwork::default());
	for node in NODES {
		network.publish(
			crate::schema::NodeId {
				id: node,
				_reserved: 0,
			},
			&address(node),
			scripted.clone(),
		);
	}
	let descriptor = descriptor();
	let client = client_entering_at(1, &network, 99, enter);
	let result = tokio::time::timeout(
		BOUND,
		client.table_write(TableWriteRequest {
			meta: write_meta(&descriptor, 1, uuid_v7()),
			payload: rows(&descriptor, &[1]),
			descriptor,
			deadline: None,
		}),
	)
	.await
	.expect("the call returns")
	.unwrap();
	let sent = scripted.attempts_remaining.lock().unwrap().clone();
	(result.response.write, sent)
}

/// The client's side of the single-call sequence, scripted (the server's side
/// is the admission control above; the two are NOT one end-to-end
/// reproduction). An earlier attempt of the call was answered WRITE_STALLED,
/// so it may have reserved; the later attempts, the last included, are
/// answered BACKPRESSURED as a ParentDecisions node now answers them. The
/// client's own exhaustion keeps the stalled attempt's uncertainty:
/// WRITE_ATTEMPTS_EXHAUSTED + INDETERMINATE.
#[tokio::test]
async fn the_client_reports_indeterminate_when_a_stalled_attempt_is_followed_by_backpressure_to_exhaustion()
 {
	let (answer, sent) = client_call_against(&[
		(
			WriteStatus::WRITE_STALLED,
			MutationCommitKnowledge::INDETERMINATE,
		),
		(
			WriteStatus::BACKPRESSURED,
			MutationCommitKnowledge::UNSPECIFIED,
		),
		(
			WriteStatus::BACKPRESSURED,
			MutationCommitKnowledge::UNSPECIFIED,
		),
	])
	.await;
	assert_eq!(sent, [3, 2, 1], "the client's own attempts");
	assert_answer(
		&answer,
		WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
		MutationCommitKnowledge::INDETERMINATE,
		MutationStopReason::ATTEMPT_EXHAUSTION,
		NODES[0],
		"a stalled attempt, then backpressure to exhaustion",
	);
}

/// Control, scoped to a FRESH identity: one with no prior and no concurrent
/// submission. Every attempt of the one call is answered BACKPRESSURED, so no
/// attempt of this call left anything unresolved and the client reports
/// KNOWN_NOT_COMMITTED. That is a statement about this call's attempts only.
/// It is not proof that the identity cannot commit: a call whose identity
/// another call has already submitted reports the same from the same answers
/// (its accumulator never saw the other call's uncertainty).
#[tokio::test]
async fn a_fresh_identity_answered_backpressured_on_every_attempt_is_reported_known_not_committed()
{
	let backpressured = (
		WriteStatus::BACKPRESSURED,
		MutationCommitKnowledge::UNSPECIFIED,
	);
	let (answer, sent) = client_call_against(&[backpressured; 3]).await;
	assert_eq!(sent, [3, 2, 1], "the client's own attempts");
	assert_answer(
		&answer,
		WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
		MutationStopReason::ATTEMPT_EXHAUSTION,
		NODES[0],
		"a fresh identity, every attempt backpressured",
	);
}

/// One possible route by which the public client is sent to a stopped node, built on purpose
/// (ADR 0718's checkpoint trace). It is not that checkpoint's history, which its log does not hold.
/// Node 1 is running and answers NOT_LEADER hinting node 3; node 3 is not on the simulated
/// network, as a stopped node is not. The client goes to node 1 first and follows the hint.
///
/// Until ADR 0729 the client's own connect to node 3 failed the call at once: no other attempt,
/// no other replica. Now a hinted node that cannot be reached is not the call's failure: nothing
/// was sent to it, it is not asked again by this call, and the next attempt asks node 1 again
/// under the same identity. Node 1 still redirects, so the call ends when its attempts do:
/// exhausted, and known not committed by this call, since every answer it got was a redirect
/// carrying no commit knowledge. (The simulated transport refuses the dial at once; a dial that
/// never completes is `client_route`'s subject.)
#[tokio::test]
async fn a_hint_to_a_stopped_node_is_skipped_and_the_call_ends_with_its_attempts() {
	let (enter, stopped) = (NODES[0], NODES[2]);
	let not_leader = (
		WriteStatus::NOT_LEADER,
		MutationCommitKnowledge::UNSPECIFIED,
	);
	let scripted = Arc::new(ScriptedTableWrites {
		node: enter,
		hint: stopped,
		write_ids: std::sync::Mutex::new(Vec::new()),
		answers: std::sync::Mutex::new([not_leader; 4].into_iter().collect()),
		attempts_remaining: std::sync::Mutex::new(Vec::new()),
	});
	let network = Arc::new(SimulatedTransportNetwork::default());
	for node in NODES {
		if node != stopped {
			network.publish(
				crate::schema::NodeId {
					id: node,
					_reserved: 0,
				},
				&address(node),
				scripted.clone(),
			);
		}
	}
	let descriptor = descriptor();
	let uuid = uuid_v7();
	let wanted = write_meta(&descriptor, 1, uuid).parent_write_id;
	for call in 1..=2_usize {
		// A new client each call, as the three-node workflow makes one per attempt.
		let client = client_entering_at(1, &network, 99, enter);
		let answer = tokio::time::timeout(
			BOUND,
			client.table_write(TableWriteRequest {
				meta: write_meta(&descriptor, 1, uuid),
				payload: rows(&descriptor, &[1]),
				descriptor: descriptor.clone(),
				deadline: None,
			}),
		)
		.await
		.expect("the call returns")
		.expect("an unreachable hinted node is not the call's error")
		.response
		.write;
		assert_eq!(
			(
				status_name(answer.status),
				knowledge_name(answer.mutation_commit_knowledge()),
				stop_name(answer.mutation_stop_reason()),
			),
			(
				status_name(WriteStatus::WRITE_ATTEMPTS_EXHAUSTED),
				knowledge_name(MutationCommitKnowledge::KNOWN_NOT_COMMITTED),
				stop_name(MutationStopReason::ATTEMPT_EXHAUSTION),
			),
			"call {call}: {}",
			answer_text(&answer)
		);
		// Per call: the first attempt reached node 1, the second was the hinted node's refused
		// connect, the third reached node 1 again. The same identity each time.
		assert_eq!(
			*scripted.attempts_remaining.lock().unwrap(),
			[3, 1].repeat(call),
			"call {call}"
		);
		assert_eq!(*scripted.write_ids.lock().unwrap(), vec![wanted; 2 * call]);
	}
}
