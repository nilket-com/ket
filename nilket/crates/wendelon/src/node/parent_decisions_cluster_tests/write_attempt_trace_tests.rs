//! DC-290 (Codex 01a20ba5): the answered-attempt record on a real one-voter ParentDecisions
//! store, enabled through its node configuration.
//! - A committed write is recorded once, with its identity, its outcome and its children.
//! - A write held after its Reserve, inside its holder's child write, or before its Finalize
//!   keeps at least that hold in the span before the children, in the children, or after them
//!   (a phase witness and a lower bound each; no upper bound on any phase).
//! - A write sent as a real frame by the public client carries a known frame span.
//! - A stalled attempt is recorded with its own outcome, and never reaches its children.
//! - An attempt whose caller's deadline answered first is not recorded.
//! - A node without the trace keeps nothing and publishes no family.

use super::one_voter::{one_voter, open, until_settled};
use super::*;
use crate::consensus::decisions::{DecisionCommand, DecisionResult};
use crate::network::data_server::child_writes::PauseStage;
use crate::network::data_server::write_stall_trace::WriteStallTrace;
use crate::network::data_server::write_stall_trace::attempts::{CAPACITY, WriteAttemptRecord};

fn attempts_of(trace: &WriteStallTrace, meta: &TableWriteRequestMeta) -> Vec<WriteAttemptRecord> {
	trace
		.snapshot()
		.attempts
		.records
		.into_iter()
		.filter(|record| {
			record.client_uuid == meta.parent_write_id.client_uuid
				&& record.sequence_no == meta.parent_write_id.sequence_no
		})
		.collect()
}

async fn ready(
	trace: bool,
) -> (
	crate::test_support::TestDir,
	WendelonNode,
	TableSchemaDescriptor,
) {
	let (dir, mut cfg) = one_voter();
	cfg.write_stall_trace = trace;
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
	let warm = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&write_retrying(&node.data_server, warm, rows(&descriptor, &[1])).await,
		"the first write",
	);
	(dir, node, descriptor)
}

/// One write held at `stage` for `hold`, then released; its response.
async fn held_write(
	node: &WendelonNode,
	stage: PauseStage,
	hold: std::time::Duration,
	meta: TableWriteRequestMeta,
	batch: RecordBatch,
) -> crate::schema::TableWriteResponseMeta {
	let (reached, release) = node.data_server.pause_coordinator_for_test(stage);
	let server = node.data_server.clone();
	let write =
		tokio::spawn(async move { server.table_write_for_test(meta, batch).await.unwrap().0 });
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the attempt reaches its pause");
	tokio::time::sleep(hold).await;
	release.notify_one();
	tokio::time::timeout(BOUND, write)
		.await
		.expect("the held attempt ends")
		.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn answered_attempts_are_recorded_with_their_spans_and_synthetic_delays_land_in_their_own() {
	let (_dir, node, descriptor) = ready(true).await;
	let trace = node
		.data_server
		.write_stall_trace()
		.expect("the configuration enables the trace")
		.clone();
	let hold = std::time::Duration::from_millis(400);

	// The ordinary comparator.
	let mut ordinary = write_meta(&descriptor, 1, uuid_v7());
	ordinary.set_attempts_remaining(3);
	let before = trace.snapshot().attempts.recorded;
	assert_committed(
		&node
			.data_server
			.table_write_for_test(ordinary, rows(&descriptor, &[2]))
			.await
			.unwrap()
			.0,
		"the ordinary write",
	);
	let [plain] = attempts_of(&trace, &ordinary)[..] else {
		panic!("one record: {:?}", trace.snapshot().attempts);
	};
	assert_eq!(plain.seq, before + 1);
	assert_eq!(
		(plain.outcome, plain.status, plain.attempts_remaining),
		(1, 0, 3)
	);
	assert_eq!(plain.lookups, 2, "one before the Reserve and one after it");
	assert!(plain.children.is_some_and(|children| children >= 1));
	assert_eq!(plain.pre, None, "this caller read no frame");

	// A delay after the Reserve: the span before the children.
	let mut early = write_meta(&descriptor, 1, uuid_v7());
	early.set_attempts_remaining(3);
	assert_committed(
		&held_write(
			&node,
			PauseStage::AfterReserve,
			hold,
			early,
			rows(&descriptor, &[3]),
		)
		.await,
		"the write held after its Reserve",
	);
	let [slow_before] = attempts_of(&trace, &early)[..] else {
		panic!("one record: {:?}", trace.snapshot().attempts);
	};
	assert_eq!(slow_before.outcome, 1);
	assert!(slow_before.coord_before >= hold, "{slow_before:?}");

	// A delay before the Finalize: the span after the children.
	let mut late = write_meta(&descriptor, 1, uuid_v7());
	late.set_attempts_remaining(2);
	assert_committed(
		&held_write(
			&node,
			PauseStage::BeforeFinalize,
			hold,
			late,
			rows(&descriptor, &[4]),
		)
		.await,
		"the write held before its Finalize",
	);
	let [slow_after] = attempts_of(&trace, &late)[..] else {
		panic!("one record: {:?}", trace.snapshot().attempts);
	};
	assert_eq!((slow_after.outcome, slow_after.attempts_remaining), (1, 2));
	assert!(slow_after.coord_after >= hold, "{slow_after:?}");

	// A hold inside the holder's own child write, after its intent began: the children's span.
	let after = crate::network::data_server::child_writes::persist_pause::arm_after_begin(
		&node.slab,
		descriptor.table_object_id,
	);
	let mut inner = write_meta(&descriptor, 1, uuid_v7());
	inner.set_attempts_remaining(3);
	let write = {
		let server = node.data_server.clone();
		let batch = rows(&descriptor, &[7]);
		tokio::spawn(async move { server.table_write_for_test(inner, batch).await.unwrap().0 })
	};
	tokio::time::timeout(BOUND, after.reached.notified())
		.await
		.expect("the holder pauses inside its child write");
	tokio::time::sleep(hold).await;
	after.release.notify_one();
	assert_committed(
		&tokio::time::timeout(BOUND, write)
			.await
			.expect("the held write ends")
			.unwrap(),
		"the write held inside its child",
	);
	let [slow_child] = attempts_of(&trace, &inner)[..] else {
		panic!("one record: {:?}", trace.snapshot().attempts);
	};
	assert_eq!(slow_child.outcome, 1);
	assert!(slow_child.children_span >= hold, "{slow_child:?}");

	// A stalled attempt keeps its own outcome and never reaches its children; its retry commits
	// and is a second record of the same identity.
	let DecisionResult::HoldEntered { id, entered_at } = (match node
		.raft
		.client_write(crate::consensus::WendelonRequest::Decision(
			DecisionCommand::BeginBackupHold { id: 0x290 },
		))
		.await
		.unwrap()
		.data
	{
		crate::consensus::WendelonResponse::Decision(result) => result,
		other => panic!("not a decision answer: {other:?}"),
	}) else {
		panic!("hold not entered");
	};
	let mut stalled = write_meta(&descriptor, 1, uuid_v7());
	stalled.set_attempts_remaining(3);
	let response = node
		.data_server
		.table_write_for_test(stalled, rows(&descriptor, &[5]))
		.await
		.unwrap()
		.0;
	assert_eq!(response.write.status, WriteStatus::WRITE_STALLED);
	node.raft
		.client_write(crate::consensus::WendelonRequest::Decision(
			DecisionCommand::ReleaseBackupHold { id, entered_at },
		))
		.await
		.unwrap();
	stalled.set_attempts_remaining(2);
	assert_committed(
		&node
			.data_server
			.table_write_for_test(stalled, rows(&descriptor, &[5]))
			.await
			.unwrap()
			.0,
		"the stalled identity after the release",
	);
	let [first, second] = attempts_of(&trace, &stalled)[..] else {
		panic!("two records: {:?}", trace.snapshot().attempts);
	};
	assert_eq!(
		(
			first.outcome,
			first.status,
			first.attempts_remaining,
			first.children
		),
		(4, WriteStatus::WRITE_STALLED.0, 3, None)
	);
	assert_eq!((second.outcome, second.attempts_remaining), (1, 2));
	assert!(first.seq < second.seq);

	// The caller's deadline answers while the attempt is held: nothing is recorded for it,
	// though its coordinator ran on.
	let recorded = trace.snapshot().attempts.recorded;
	let mut hurried = write_meta(&descriptor, 1, uuid_v7());
	hurried.set_attempts_remaining(3);
	hurried.deadline_remaining_millis = 300;
	let answer = held_write(
		&node,
		PauseStage::BeforeFinalize,
		std::time::Duration::from_millis(800),
		hurried,
		rows(&descriptor, &[6]),
	)
	.await;
	assert_eq!(answer.write.status, WriteStatus::WRITE_DEADLINE_EXCEEDED);
	assert!(attempts_of(&trace, &hurried).is_empty());
	assert_eq!(trace.snapshot().attempts.recorded, recorded);

	// The scrape publishes the same records.
	let snapshot = trace.snapshot().attempts;
	assert_eq!(snapshot.overwritten(), 0);
	let text = crate::metrics::global().render_prometheus();
	assert!(
		text.contains(&format!(
			"wendelon_data_ring_write_attempt_trace{{node=\"{}\",field=\"recorded\"}} {}\n",
			trace.node(),
			snapshot.recorded
		)),
		"{text}"
	);
	for record in [plain, slow_before, slow_after, slow_child, first, second] {
		let slot = (record.seq - 1) % CAPACITY as u64;
		for (field, value) in record.fields() {
			let series = format!(
				"wendelon_data_ring_write_attempt{{node=\"{}\",slot=\"{slot}\",field=\"{field}\"}} {value}\n",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_without_the_trace_keeps_no_attempts() {
	let (_dir, node, descriptor) = ready(false).await;
	assert!(node.data_server.write_stall_trace().is_none());
	assert!(
		node.data_server.frame_reading().is_none(),
		"no clock reading"
	);
	let meta = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&node
			.data_server
			.table_write_for_test(meta, rows(&descriptor, &[2]))
			.await
			.unwrap()
			.0,
		"an untraced write",
	);
	tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a complete close")
		.unwrap();
}

/// A public client of the one voter, on its own network, that believes it leads.
fn public_client(
	node: &WendelonNode,
	voter: u32,
	epoch: u64,
) -> Arc<crate::client::WendelonClient> {
	use crate::client::{
		ClusterPeerWire, ClusterStatusWire, StaticTopologySource, WendelonClient,
		WendelonClientConfig,
	};
	use crate::network::transport::{NodeRole, NodeRoleSet, TransportProvider, TransportTierSet};
	let network = Arc::new(SimulatedTransportNetwork::default());
	network.publish(
		crate::schema::NodeId {
			id: voter,
			_reserved: 0,
		},
		&address(voter),
		node.service.clone(),
	);
	let tier = TransportTier::QuicIoUring;
	let status = ClusterStatusWire {
		epoch,
		leader: Some(voter),
		replication_factor: 1,
		peers: vec![ClusterPeerWire {
			node_id: voter,
			address: address(voter),
			tier_bits: TransportTierSet::empty().with(tier).as_u8(),
			role_bits: NodeRoleSet::empty().with(NodeRole::Data).as_u8(),
			capability_bits: 0,
		}],
	};
	let provider: Arc<dyn TransportProvider> = Arc::new(SimulatedTransportProvider::new(
		crate::schema::NodeId {
			id: 98,
			_reserved: 0,
		},
		network,
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

/// Codex 01a20c30 R2: an ordinary write sent as a real frame by the public client, through the
/// node's own service and handler, records a known frame span; the same write through the
/// frameless test entry does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_real_frame_from_the_public_client_records_a_known_frame_span() {
	let (_dir, node, descriptor) = ready(true).await;
	let trace = node
		.data_server
		.write_stall_trace()
		.expect("the configuration enables the trace")
		.clone();
	let client = public_client(&node, trace.node(), node.ring.current_epoch());
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let result = tokio::time::timeout(
		BOUND,
		client.table_write(crate::client::TableWriteRequest {
			meta,
			descriptor: descriptor.clone(),
			payload: rows(&descriptor, &[2]),
			deadline: None,
		}),
	)
	.await
	.expect("the public call ends")
	.expect("the public call answers");
	assert_committed(&result.response, "the public client's write");
	let records = attempts_of(&trace, &meta);
	let committed: Vec<_> = records
		.iter()
		.filter(|record| record.outcome == 1)
		.collect();
	let [framed] = committed[..] else {
		panic!("one committed record: {:?}", trace.snapshot().attempts);
	};
	assert!(
		framed.pre.is_some(),
		"a real frame has a frame reading: {framed:?}"
	);
	assert!(framed.children.is_some_and(|children| children >= 1));

	let frameless = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&node
			.data_server
			.table_write_for_test(frameless, rows(&descriptor, &[3]))
			.await
			.unwrap()
			.0,
		"the frameless write",
	);
	let [unframed] = attempts_of(&trace, &frameless)[..] else {
		panic!("one record: {:?}", trace.snapshot().attempts);
	};
	assert_eq!(unframed.pre, None);

	tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a complete close")
		.unwrap();
}
