//! ADR 0733: a ParentDecisions table query asks the object store for each remote object's
//! size and tail once, not once for the proof, again for the statistics and again for the
//! execution.
//!
//! The controlled case these counts are about: three nodes tiering to one LocalFs store whose
//! read requests are logged; encrypted objects far smaller than the tail window; ONE row per
//! object; the query is answered by one node from its own objects; promotion of remotely
//! read slabs is disabled in this fixture. Every `head`, range and whole-object read of
//! that node's objects during the query is counted, statistics loads included.

use super::offload::{until_offloaded, until_ready, write_rows};
use super::*;
use crate::storage::l2::request_log::{Request, RequestLog, log_requests_under};

/// A ready cluster over one request-logged store, with `rows` written one per slab and
/// every node's slabs offloaded. Returns the answering node's object URIs in row order.
async fn one_object_per_row(
	tag: &str,
	rows: &[(i64, &str)],
) -> (
	Cluster,
	u32,
	TableSchemaDescriptor,
	Expected,
	Vec<String>,
	std::sync::Arc<RequestLog>,
	crate::test_support::TestDir,
) {
	let root = crate::test_support::TestDir::new(&format!("query-tails-l2-{tag}"));
	let path = root.to_path_buf();
	let log = log_requests_under(&path);
	let (cluster, leader, descriptor) = ready_with(Cluster::start_with(move |_, cfg| {
		cfg.l2 = Some(crate::storage::ObjectStoreConfig::LocalFs { root: path.clone() });
		cfg.tiering.scan_interval = std::time::Duration::from_millis(200);
		cfg.tiering.op_driver_interval = std::time::Duration::from_millis(200);
		// The fixture's choice, so that a count is not changed by a slab coming home.
		cfg.rehydration.promote_after_l2_reads = 0;
		None
	}))
	.await;
	let mut expected = Expected::new();
	let mut uris = Vec::new();
	for row in rows {
		expected.extend(write_rows(&cluster, leader, &descriptor, &[*row]).await);
		let mut sealed = std::collections::BTreeMap::new();
		for node in NODES {
			sealed.insert(
				node,
				cluster.node(node).slab.force_seal_active_for_test().await,
			);
		}
		for node in NODES {
			until_offloaded(&cluster, node, sealed[&node], "one row, one object").await;
		}
		uris.push(
			cluster
				.node(leader)
				.slab
				.tier_pointer(sealed[&leader])
				.await
				.expect("the leader's pointer")
				.l2_uri,
		);
	}
	for node in NODES {
		until_ready(&cluster, node, "before the counted queries").await;
	}
	(cluster, leader, descriptor, expected, uris, log, root)
}

/// The logged requests that name one of `uris`, in order.
fn of_objects(log: &RequestLog, uris: &[String]) -> Vec<Request> {
	log.take()
		.into_iter()
		.filter(|request| {
			let uri = match request {
				Request::Head(uri) | Request::Range(uri, _, _) | Request::Whole(uri) => uri,
			};
			uris.contains(uri)
		})
		.collect()
}

/// One query at the node that owns `uris` asks, for each object, its size once and its
/// tail once, and nothing else: on a first query, when the slabs' statistics have never
/// been loaded, and on a second, when they have. The answers are exact both times.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_query_asks_the_store_for_each_objects_size_and_tail_once() {
	let rows = [(1, "a"), (2, "b"), (3, "c")];
	let (cluster, leader, descriptor, expected, uris, log, _root) =
		one_object_per_row("once", &rows).await;
	let mut observed = Vec::new();
	for what in ["the first query", "the second query"] {
		log.take();
		expect_at(&cluster, leader, &descriptor, &expected, what).await;
		let requests = of_objects(&log, &uris);
		eprintln!(
			"ADR0733 {what}: {} requests for {} objects: {requests:?}",
			requests.len(),
			uris.len()
		);
		observed.push(requests);
	}
	for (what, requests) in ["the first query", "the second query"]
		.iter()
		.zip(&observed)
	{
		for uri in &uris {
			let of_this: Vec<&Request> = requests
				.iter()
				.filter(|request| match request {
					Request::Head(named) | Request::Range(named, _, _) | Request::Whole(named) => {
						named == uri
					}
				})
				.collect();
			assert!(
				matches!(
					of_this.as_slice(),
					[Request::Head(_), Request::Range(_, 0, _)]
				),
				"{what}: the requests for {uri} are not one size and one tail: {of_this:?}"
			);
		}
		assert_eq!(
			requests.len(),
			2 * uris.len(),
			"{what}: {} requests for {} objects",
			requests.len(),
			uris.len()
		);
	}
}

/// The holder of a query's tails lives exactly as long as the query: it is gone when a
/// query has answered, when one is cancelled while it waits on the store, and when one's
/// future is dropped there.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_querys_held_tails_are_gone_when_the_query_ends() {
	let rows = [(1, "a"), (2, "b")];
	let (cluster, leader, descriptor, expected, uris, log, _root) =
		one_object_per_row("lifetime", &rows).await;
	let slab = cluster.node(leader).slab.clone();
	let never = std::sync::Weak::new();

	expect_at(
		&cluster,
		leader,
		&descriptor,
		&expected,
		"a completed query",
	)
	.await;
	let completed = slab.last_query_tails_for_test();
	assert!(!completed.ptr_eq(&never), "the query made a holder");
	assert_eq!(completed.strong_count(), 0, "after a completed query");

	let mut before = completed;
	for ending in ["cancelled", "dropped"] {
		// The query stops inside its first request for one of its objects.
		log.take();
		log.hold_reads(true);
		let server = cluster.node(leader).data_server.clone();
		let meta = query_meta(&cluster, leader, &descriptor);
		let request = meta.request_id;
		let reading = tokio::spawn(async move { server.table_query_for_test(meta).await });
		let deadline = tokio::time::Instant::now() + BOUND;
		while of_objects(&log, &uris).is_empty() {
			assert!(
				tokio::time::Instant::now() < deadline,
				"{ending}: the query never asked the store"
			);
			tokio::time::sleep(std::time::Duration::from_millis(10)).await;
		}
		let during = slab.last_query_tails_for_test();
		assert!(!during.ptr_eq(&before), "{ending}: a holder of its own");
		assert_eq!(
			during.strong_count(),
			1,
			"{ending}: held by the query alone"
		);
		if ending == "cancelled" {
			cluster
				.node(leader)
				.data_server
				.cancel_table_query_for_test(TenantId::ZERO, request);
			let answer = reading.await.unwrap().unwrap();
			assert_eq!(
				answer.status,
				Some(crate::schema::ReadStatus::QUERY_CANCELLED),
				"{answer:?}"
			);
		} else {
			reading.abort();
			assert!(reading.await.unwrap_err().is_cancelled());
		}
		// The store is still held: nothing the query started is running on.
		assert_eq!(during.strong_count(), 0, "{ending}: after the query ended");
		log.hold_reads(false);
		before = during;
	}
	expect_at(
		&cluster,
		leader,
		&descriptor,
		&expected,
		"a query afterwards",
	)
	.await;
}
