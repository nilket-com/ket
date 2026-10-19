//! The public counter-add wire for a write id that names another add, and the
//! local-persistence distrust fence an unsettled counter dedup store trips.
use super::*;
use crate::storage::counter::{CounterKey, counter_identity};

async fn counter_node(id: u32, tag: &str) -> (WendelonNode, crate::test_support::TestDir) {
	let dir = bootstrap_tempdir(tag);
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		id,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	(node, dir)
}

// A reused write id for another add answers WRITE_KEY_CONFLICT with zero values
// (never the recorded add's outcome), applies nothing, and leaves the original
// add's replay and the next add's base intact.
#[tokio::test]
async fn reused_counter_write_id_for_another_add_answers_write_key_conflict() {
	let (node, _dir) = counter_node(91, "counter-write-key-conflict").await;
	let key_identity = counter_identity(&CounterKey(b"k".to_vec()));

	let first = dispatch_counter_add(&node, 1, 5, key_identity).await;
	assert_eq!(first.status, WriteStatus::OK);
	assert_eq!((first.previous_value, first.current_value), (0, 5));

	let reused = dispatch_counter_add(&node, 1, 3, key_identity).await;
	assert_eq!(reused.status, WriteStatus::WRITE_KEY_CONFLICT);
	assert_eq!(reused.status.0, 24);
	assert_eq!(reused.status.as_str(), "write_key_conflict");
	assert_eq!((reused.previous_value, reused.current_value), (0, 0));
	assert_eq!(reused._pad1, [0; 7]);

	let retry = dispatch_counter_add(&node, 1, 5, key_identity).await;
	assert_eq!(retry.status, WriteStatus::DUPLICATE);
	assert_eq!((retry.previous_value, retry.current_value), (0, 5));

	let next = dispatch_counter_add(&node, 2, 1, key_identity).await;
	assert_eq!(next.status, WriteStatus::OK);
	assert_eq!(
		(next.previous_value, next.current_value),
		(5, 6),
		"the refused add applied nothing"
	);

	shutdown_lease_node(node).await;
}

// The distrust fence end to end: once the tenant's counter dedup store latches
// unsettled, the next add fails closed and its own poll trips the node-wide
// local-persistence fence, which persists its marker and fences later adds.
#[tokio::test]
async fn unsettled_counter_store_trips_the_distrust_fence_through_the_add_path() {
	let (node, _dir) = counter_node(92, "counter-distrust-fence").await;
	let key_identity = counter_identity(&CounterKey(b"k".to_vec()));

	let first = dispatch_counter_add(&node, 1, 5, key_identity).await;
	assert_eq!(first.status, WriteStatus::OK);
	assert_eq!(node.data_server.degraded_reason(), None);

	let leader = node
		.data_server
		.counter_leaders
		.get(&tenant())
		.map(|leader| leader.clone())
		.expect("the first add created the tenant's counter leader");
	leader.latch_unsettled_for_test();
	assert_eq!(
		node.data_server.degraded_reason(),
		None,
		"nothing has polled the latch yet"
	);

	let failed = dispatch_counter_add(&node, 2, 1, key_identity).await;
	assert_eq!(failed.status, WriteStatus::INTERNAL_ERROR);
	assert_eq!((failed.previous_value, failed.current_value), (0, 0));
	assert_eq!(
		node.data_server.degraded_reason().as_deref(),
		Some(LOCAL_PERSISTENCE_DISTRUST_REASON),
		"the add's own poll tripped the fence"
	);
	assert!(
		node.data_server.local_persistence_distrust_path.exists(),
		"the fence persisted its marker"
	);

	let fenced = dispatch_counter_add(&node, 3, 1, key_identity).await;
	assert_eq!(fenced.status, WriteStatus::INTERNAL_ERROR);
	assert_eq!(
		leader.read(&CounterKey(b"k".to_vec())).await.unwrap(),
		Some(5),
		"nothing after the latch was applied"
	);

	node.handles.abort();
	tokio::time::timeout(Duration::from_secs(10), node.shutdown())
		.await
		.expect("fenced node shutdown timed out")
		.unwrap();
}
