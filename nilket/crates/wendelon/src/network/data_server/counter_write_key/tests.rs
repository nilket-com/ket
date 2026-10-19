//! The public counter-add wire for a write id that names another add, for one
//! past the dedup horizon (E1) or not presentable at all (E0), the dedup
//! horizon's startup validation, the compaction the add path schedules, and
//! the local-persistence distrust fence an unsettled counter dedup store trips.
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

/// A UUIDv7 client uuid issued at `issued_ms`.
fn v7_uuid(issued_ms: u64) -> [u8; 16] {
	let write_key = crate::storage::counter::test_write_key_at(issued_ms, 0);
	write_key[..16].try_into().expect("16-byte client uuid")
}

fn now_ms() -> u64 {
	crate::consensus::monitor::SystemClock.now_ms()
}

// T-WIRE (server): an expired write id answers WRITE_OPERATION_EXPIRED (22)
// with ADR 542's INDETERMINATE knowledge and no stop reason, since an earlier
// presentation may have committed; a write id that is not a UUIDv7, or one
// issued beyond the clock-skew allowance, answers WRITE_IDENTITY_INVALID (25)
// with zeroed outcome axes. Neither applies anything. The control: a write-key
// conflict (24) still answers with zeroed axes.
#[tokio::test]
async fn expired_and_invalid_counter_write_ids_answer_their_wire_statuses() {
	let (node, _dir) = counter_node(93, "counter-write-id-horizon").await;
	let key_identity = counter_identity(&CounterKey(b"k".to_vec()));
	let horizon = crate::storage::counter::CounterRetentionConfig::default();

	let expired_uuid = v7_uuid(now_ms() - horizon.expiry_ms - 10_000);
	let expired = dispatch_counter_add_as(&node, expired_uuid, 1, 5, key_identity).await;
	assert_eq!(expired.status, WriteStatus::WRITE_OPERATION_EXPIRED);
	assert_eq!(expired.status.0, 22);
	assert_eq!(
		expired.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	assert_eq!(expired.mutation_stop_reason(), MutationStopReason::NONE);
	assert_eq!(expired._pad1, [3, 0, 0, 0, 0, 0, 0]);
	assert_eq!((expired.previous_value, expired.current_value), (0, 0));
	assert_eq!(expired.echoed_write_id.client_uuid, expired_uuid);

	let mut v4 = v7_uuid(now_ms());
	v4[6] = 0x40;
	let future = v7_uuid(now_ms() + horizon.clock_skew_ms + 60_000);
	for (uuid, what) in [(v4, "UUIDv4"), (future, "issued beyond the skew allowance")] {
		let invalid = dispatch_counter_add_as(&node, uuid, 1, 5, key_identity).await;
		assert_eq!(
			invalid.status,
			WriteStatus::WRITE_IDENTITY_INVALID,
			"{what}"
		);
		assert_eq!(invalid.status.0, 25);
		assert_eq!(invalid.status.as_str(), "write_identity_invalid");
		assert_eq!(invalid._pad1, [0; 7], "{what}: zeroed outcome axes");
		assert_eq!((invalid.previous_value, invalid.current_value), (0, 0));
	}

	// Nothing was applied: a valid add bases at 0, and a conflict (the
	// control) still answers 24 with zeroed axes.
	let first = dispatch_counter_add(&node, 1, 7, key_identity).await;
	assert_eq!(first.status, WriteStatus::OK);
	assert_eq!((first.previous_value, first.current_value), (0, 7));
	let conflict = dispatch_counter_add(&node, 1, 2, key_identity).await;
	assert_eq!(conflict.status, WriteStatus::WRITE_KEY_CONFLICT);
	assert_eq!(conflict._pad1, [0; 7]);

	shutdown_lease_node(node).await;
}

// T-CFGVAL (server): the data server refuses an out-of-range dedup horizon.
#[tokio::test]
async fn data_server_refuses_an_out_of_range_counter_retention() {
	let (node, _dir) = counter_node(94, "counter-retention-builder").await;
	let invalid = crate::storage::counter::CounterRetentionConfig {
		expiry_ms: crate::schema::COUNTER_INCARNATION_LIFETIME_MS,
		..Default::default()
	};
	let server = (*node.data_server).clone();
	assert_eq!(
		server
			.with_counter_retention(invalid)
			.err()
			.map(|error| error.kind()),
		Some(ErrorKind::InvalidInput)
	);
	shutdown_lease_node(node).await;
}

// The add path schedules the applied log's compaction when it is due, in the
// background: the tenant's store then serves a compacted generation, and adds
// and their replays go on.
#[tokio::test]
async fn counter_add_path_schedules_a_due_compaction() {
	let (node, _dir) = counter_node(95, "counter-compaction-trigger").await;
	let key_identity = counter_identity(&CounterKey(b"k".to_vec()));
	assert_eq!(
		dispatch_counter_add(&node, 1, 5, key_identity).await.status,
		WriteStatus::OK
	);
	let leader = node
		.data_server
		.counter_leaders
		.get(&tenant())
		.map(|leader| leader.clone())
		.unwrap();
	assert_eq!(leader.served_generation_for_test(), Some(0));
	leader.force_compaction_due_for_test();
	assert_eq!(
		dispatch_counter_add(&node, 2, 1, key_identity).await.status,
		WriteStatus::OK
	);
	let mut generation = None;
	for _ in 0..400 {
		generation = leader.served_generation_for_test();
		if generation == Some(1) {
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert_eq!(generation, Some(1), "a compacted generation is served");
	assert!(!leader.durability_unsettled());
	let replay = dispatch_counter_add(&node, 1, 5, key_identity).await;
	assert_eq!(replay.status, WriteStatus::DUPLICATE);
	let next = dispatch_counter_add(&node, 3, 1, key_identity).await;
	assert_eq!((next.previous_value, next.current_value), (6, 7));
	shutdown_lease_node(node).await;
}
