//! The public cancellation of an in-flight ParentDecisions table query, through
//! the data-frame handler (the gate a public client's TABLE_QUERY_CANCEL_REQ
//! meets), not the test-only registry call.

use super::*;
use crate::schema::{
	TableQueryCancelRequestMeta, TableQueryCancelResponseMeta, TableQueryCancelStatus,
};

/// A TABLE_QUERY_CANCEL_REQ through `node`'s data-frame handler, from a data-node
/// identity: what a public client's cancel gets from that node.
async fn cancel_at(
	cluster: &Cluster,
	node: u32,
	tenant: TenantId,
	request_id: crate::schema::TableQueryRequestId,
) -> TableQueryCancelResponseMeta {
	use crate::network::router::DataRingServer;
	let meta = TableQueryCancelRequestMeta {
		tenant_id: tenant,
		request_id,
	};
	let meta_bytes = bytemuck::bytes_of(&meta).to_vec();
	let frame = crate::network::core::HrftFrame {
		header: crate::schema::FrameHeader {
			magic: crate::network::core::HRFT_MAGIC,
			version: 1,
			msg_type: crate::schema::MsgType::TABLE_QUERY_CANCEL_REQ.0,
			flags: 0,
			stream_id: 87,
			meta_len: meta_bytes.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: crate::network::pool::PooledBuf::orphan(meta_bytes),
		arrow_payload: None,
		trailing: None,
	};
	let local = crate::schema::NodeId {
		id: node,
		_reserved: 0,
	};
	let context = crate::network::server::RequestContext {
		remote: Some(local),
		actor: crate::network::tls::ActorIdentity::data_node(local),
	};
	let response = tokio::time::timeout(
		BOUND,
		DataRingServer::handle_data_frame(cluster.node(node).data_server.as_ref(), context, frame),
	)
	.await
	.expect("the cancel answers")
	.expect("a response, not a dropped stream")
	.expect("a response frame");
	bytemuck::pod_read_unaligned(
		&response.meta[..std::mem::size_of::<TableQueryCancelResponseMeta>()],
	)
}

/// A query held at `node` after its cut, and its request id.
async fn held_query(
	cluster: &Cluster,
	node: u32,
	descriptor: &TableSchemaDescriptor,
) -> (
	crate::schema::TableQueryRequestId,
	tokio::task::JoinHandle<std::io::Result<crate::network::data_server::reader::TestQueryAnswer>>,
	Arc<tokio::sync::Notify>,
) {
	use crate::network::data_server::child_writes::PauseStage;
	let (reached, release) = cluster
		.node(node)
		.data_server
		.pause_coordinator_for_test(PauseStage::ReaderAfterCut);
	let server = cluster.node(node).data_server.clone();
	let meta = query_meta(cluster, node, descriptor);
	let request = meta.request_id;
	let reading = tokio::spawn(async move { server.table_query_for_test(meta).await });
	reached.notified().await;
	(request, reading, release)
}

/// The public cancel frame stops exactly the query it names.
///
/// A held query A is not stopped by a cancel naming another tenant (TERMINAL, not
/// visible), an unknown request, or A's request at a node not running it (both
/// ACCEPTED: the existing early-cancel contract records them against a query yet
/// to register). Released, A completes with its exact rows. A held query B,
/// cancelled at its node, is ACCEPTED and ends QUERY_CANCELLED instead of
/// completing. Another query afterwards is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_public_cancel_stops_an_in_flight_query_and_only_that_query() {
	let (cluster, leader, descriptor) = ready().await;
	let mut expected = Expected::new();
	for id in 1..=3 {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "q")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "q".into());
	}

	// A: scoping.
	let (a, reading, release) = held_query(&cluster, leader, &descriptor).await;
	let unknown = query_meta(&cluster, leader, &descriptor).request_id;
	let elsewhere = NODES.into_iter().find(|node| *node != leader).unwrap();
	for (what, node, tenant, id, status) in [
		(
			"another tenant",
			leader,
			TenantId::new(1),
			a,
			TableQueryCancelStatus::TERMINAL,
		),
		(
			"an unknown request",
			leader,
			TenantId::ZERO,
			unknown,
			TableQueryCancelStatus::ACCEPTED,
		),
		(
			"a node not running it",
			elsewhere,
			TenantId::ZERO,
			a,
			TableQueryCancelStatus::ACCEPTED,
		),
	] {
		let answer = cancel_at(&cluster, node, tenant, id).await;
		assert_eq!(answer.status, status, "{what}: {answer:?}");
	}
	release.notify_one();
	let completed = reading.await.unwrap().unwrap();
	assert_eq!(
		completed.status,
		Some(crate::schema::ReadStatus::OK),
		"A was not stopped: {completed:?}"
	);
	assert_eq!(answered(&completed), expected, "A's exact rows");

	// B: the cancel it names.
	let (b, reading, release) = held_query(&cluster, leader, &descriptor).await;
	let answer = cancel_at(&cluster, leader, TenantId::ZERO, b).await;
	assert_eq!(
		answer.status,
		TableQueryCancelStatus::ACCEPTED,
		"{answer:?}"
	);
	release.notify_one();
	let ended = reading.await.unwrap().unwrap();
	assert_eq!(
		ended.status,
		Some(crate::schema::ReadStatus::QUERY_CANCELLED),
		"B cancelled, not completed: {ended:?}"
	);

	// Another query is served.
	expect_at(
		&cluster,
		leader,
		&descriptor,
		&expected,
		"a query afterwards",
	)
	.await;
}
