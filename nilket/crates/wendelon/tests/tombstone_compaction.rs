#[path = "support/control_ring.rs"]
mod support;

use arrow::array::{LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Duration;
use wendelon::consensus::{
	MigrationComplete, MigrationLedgerEntry, MigrationState, SeedMigration, WendelonRequest,
};
use wendelon::network::data_server::WendelonDataServer;
use wendelon::network::server::HrftService;
use wendelon::network::traits::DataRing;
use wendelon::node::{NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{
	Consistency, HybridTimestamp, Identity, Locality, NodeId, ReadRequestMeta, ReadStatus,
	TenantId, read_key_schema, write_payload_schema,
};
use wendelon::storage::core::StorageEngine;

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn key_locality(bytes: &[u8]) -> u64 {
	Locality::from(Identity::blake3_tenant(tenant(), bytes)).get()
}

fn key_identity(bytes: &[u8]) -> Identity {
	Identity::blake3_tenant(tenant(), bytes)
}

fn kv_batch(key: &[u8], value: &[u8]) -> RecordBatch {
	let schema = Arc::new(write_payload_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	let mut values = LargeBinaryBuilder::new();
	values.append_value(value);
	RecordBatch::try_new(
		schema,
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.unwrap()
}

fn key_batch(key: &[u8]) -> RecordBatch {
	let schema = Arc::new(read_key_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	RecordBatch::try_new(schema, vec![Arc::new(keys.finish())]).unwrap()
}

fn decode_single_value(frame: &[u8]) -> Vec<u8> {
	let reader =
		arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(frame), None).unwrap();
	let batches: Vec<_> = reader.map(|batch| batch.unwrap()).collect();
	let batch = batches.into_iter().next().expect("one batch");
	let values = batch
		.column_by_name("value")
		.expect("value column")
		.as_any()
		.downcast_ref::<LargeBinaryArray>()
		.expect("large binary value column");
	values.value(0).to_vec()
}

async fn bootstrap_custom_node(tag: &str, node_id: u32, slab_max_bytes: u64) -> WendelonNode {
	let mut cfg = NodeBootstrapConfig::dev(node_id, support::tempdir(tag), "127.0.0.1:0".into());
	cfg.slab_max_bytes = slab_max_bytes;
	WendelonNode::bootstrap_single_node(cfg).await.unwrap()
}

async fn seed_source(
	slab: &wendelon::storage::manager::SlabManager,
	key: &[u8],
	ts: HybridTimestamp,
	value: &[u8],
) {
	let batch = kv_batch(key, value);
	let bytes = support::encode_batch(&batch).unwrap();
	slab.append(tenant(), key_identity(key), ts, &bytes)
		.await
		.unwrap();
}

fn ledger_entry(
	migration_id: u64,
	dest_id: NodeId,
	source_ids: Vec<NodeId>,
	range_start: u64,
	range_end: u64,
	floor: HybridTimestamp,
) -> MigrationLedgerEntry {
	MigrationLedgerEntry {
		migration_id,
		range_start,
		range_end,
		from_epoch: 0,
		to_epoch: 1,
		logical_owner: dest_id,
		upstream_migration_id: None,
		source_replicas: source_ids,
		migration_hlc_floor: floor,
		state: MigrationState::Pending,
		completed_by: None,
		completed_at_hlc: None,
	}
}

async fn seed_migration(node: &WendelonNode, entry: MigrationLedgerEntry) {
	node.raft
		.client_write(WendelonRequest::SeedMigration(SeedMigration { entry }))
		.await
		.unwrap();
}

async fn wait_for_migration_state(
	node: &WendelonNode,
	migration_id: u64,
	state: MigrationState,
) -> MigrationLedgerEntry {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		if let Some(entry) = snapshot.migrations.get(&migration_id) {
			if entry.state == state {
				return entry.clone();
			}
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for migration {migration_id} to reach {state:?}"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

async fn mirror_completion(source: &WendelonNode, completed: &MigrationLedgerEntry) {
	source
		.raft
		.client_write(WendelonRequest::MigrationComplete(MigrationComplete {
			migration_id: completed.migration_id,
			range_start: completed.range_start,
			range_end: completed.range_end,
			to_epoch: completed.to_epoch,
			completed_by: completed.completed_by.expect("completed_by"),
			completed_at_hlc: completed.completed_at_hlc.expect("completed_at_hlc"),
			migration_hlc_floor: completed.migration_hlc_floor,
		}))
		.await
		.unwrap();
}

fn stale_read_meta(as_of_hlc: HybridTimestamp, key: &[u8]) -> ReadRequestMeta {
	ReadRequestMeta {
		min_hlc: hlc(0, 0, 0),
		expected_epoch: 0,
		tenant_id: tenant(),
		consistency: Consistency::STALE,
		_pad: [0; 3],
		key_len: key.len() as u32,
		_pad2: [0; 8],
		as_of_hlc,
	}
}

#[tokio::test]
async fn background_reclaim_rewrites_mixed_slab_and_preserves_live_versions() {
	let source = bootstrap_custom_node("tombstone-source-mixed", 81, 8 * 1024).await;
	let dest = bootstrap_custom_node("tombstone-dest-mixed", 82, 64 * 1024 * 1024).await;
	support::wait_for_local_leader(&source).await;
	support::wait_for_local_leader(&dest).await;
	source.handles.migration_sweeper.abort();
	dest.handles.migration_sweeper.abort();

	let moved_key = b"migrated-key";
	let kept_key = b"kept-key";
	let rotate_key = b"rotate-key";
	let moved_ts = hlc(10, 0, 1);
	let keep_ts = hlc(10, 0, 1);
	let rotate_ts = hlc(20, 0, 1);
	seed_source(&source.slab, moved_key, moved_ts, b"moved-value").await;
	seed_source(&source.slab, kept_key, keep_ts, b"kept-value").await;
	seed_source(&source.slab, rotate_key, rotate_ts, b"rotate-value").await;
	assert_eq!(
		source.slab.sealed_count().await,
		1,
		"precondition: mixed slab sealed"
	);
	let before_bytes = source.slab.local_l1_bytes().await;

	let moved_hash = key_locality(moved_key);
	let entry = ledger_entry(
		9_001,
		dest.local_node,
		vec![source.local_node],
		moved_hash,
		moved_hash,
		hlc(0, 0, 0),
	);
	seed_migration(&source, entry.clone()).await;
	seed_migration(&dest, entry).await;

	let source_sweeper = source.data_server.clone().spawn_migration_sweeper(
		Duration::from_millis(20),
		Duration::from_millis(40),
		1,
	);
	let source_service: Arc<dyn HrftService> = source.router.clone();
	let (factory, _pool) =
		support::loopback_factory_many(vec![(source.local_node, source_service)]);
	let dest_server = Arc::new(WendelonDataServer::new(
		dest.slab.clone(),
		dest.raft.clone(),
		dest.log_store.clone(),
		dest.state_machine.clone(),
		dest.local_node,
		dest.clock.clone(),
		dest.ring.clone(),
		factory,
		dest.pool.clone(),
	));
	let dest_sweeper = dest_server.clone().spawn_migration_sweeper(
		Duration::from_millis(20),
		Duration::from_millis(40),
		1,
	);

	let completed = wait_for_migration_state(&dest, 9_001, MigrationState::Completed).await;
	mirror_completion(&source, &completed).await;
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		let after_bytes = source.slab.local_l1_bytes().await;
		let moved_missing = source
			.slab
			.lookup_as_of(tenant(), key_identity(moved_key), moved_ts)
			.is_none();
		let kept_present = source
			.slab
			.lookup_as_of(tenant(), key_identity(kept_key), keep_ts)
			.is_some();
		if after_bytes < before_bytes && moved_missing && kept_present {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for mixed-slab reclaim"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let kept_loc = source
		.slab
		.lookup_as_of(tenant(), key_identity(kept_key), keep_ts)
		.expect("kept key retained");
	let kept_bytes = source.slab.read(kept_loc).await.unwrap();
	assert_eq!(decode_single_value(&kept_bytes), b"kept-value");
	assert!(
		source.slab.local_l1_bytes().await < before_bytes,
		"compaction should reclaim bytes from the mixed slab"
	);

	dest_sweeper.abort();
	source_sweeper.abort();
	support::abort_node(&source);
	support::abort_node(&dest);
}

#[tokio::test]
async fn historical_reads_succeed_from_new_owner_after_source_reclaim() {
	let source = bootstrap_custom_node("tombstone-source-history", 91, 12 * 1024).await;
	let dest = bootstrap_custom_node("tombstone-dest-history", 92, 64 * 1024 * 1024).await;
	support::wait_for_local_leader(&source).await;
	support::wait_for_local_leader(&dest).await;
	source.handles.migration_sweeper.abort();
	dest.handles.migration_sweeper.abort();

	let migrated_key = b"time-travel-key";
	let rotate_key = b"rotate-key";
	let older = hlc(10, 0, 1);
	let newer = hlc(20, 0, 1);
	seed_source(&source.slab, migrated_key, older, b"old-value").await;
	seed_source(&source.slab, migrated_key, newer, b"new-value").await;
	seed_source(&source.slab, b"kept-key", newer, b"keep-value").await;
	seed_source(&source.slab, rotate_key, hlc(30, 0, 1), b"rotate-value").await;
	assert_eq!(
		source.slab.sealed_count().await,
		1,
		"precondition: history slab sealed"
	);

	let key_locality = key_locality(migrated_key);
	let entry = ledger_entry(
		9_002,
		dest.local_node,
		vec![source.local_node],
		key_locality,
		key_locality,
		hlc(0, 0, 0),
	);
	seed_migration(&source, entry.clone()).await;
	seed_migration(&dest, entry).await;

	let source_sweeper = source.data_server.clone().spawn_migration_sweeper(
		Duration::from_millis(20),
		Duration::from_millis(40),
		1,
	);
	let source_service: Arc<dyn HrftService> = source.router.clone();
	let (factory, _pool) =
		support::loopback_factory_many(vec![(source.local_node, source_service)]);
	let dest_server = Arc::new(WendelonDataServer::new(
		dest.slab.clone(),
		dest.raft.clone(),
		dest.log_store.clone(),
		dest.state_machine.clone(),
		dest.local_node,
		dest.clock.clone(),
		dest.ring.clone(),
		factory,
		dest.pool.clone(),
	));
	let dest_sweeper = dest_server.clone().spawn_migration_sweeper(
		Duration::from_millis(20),
		Duration::from_millis(40),
		1,
	);

	let completed = wait_for_migration_state(&dest, 9_002, MigrationState::Completed).await;
	mirror_completion(&source, &completed).await;

	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if source
			.slab
			.lookup_as_of(tenant(), key_identity(migrated_key), newer)
			.is_none()
		{
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for source-side history reclaim"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let (old_resp, old_batch) = DataRing::read(
		&*dest.data_server,
		stale_read_meta(hlc(15, 0, 1), migrated_key),
		key_batch(migrated_key),
	)
	.await
	.unwrap();
	assert_eq!(old_resp.status, ReadStatus::OK);
	let old_value = old_batch.expect("historical value batch");
	let old_value = old_value
		.column(0)
		.as_any()
		.downcast_ref::<LargeBinaryArray>()
		.expect("value batch")
		.value(0)
		.to_vec();
	assert_eq!(old_value, b"old-value");

	let (new_resp, new_batch) = DataRing::read(
		&*dest.data_server,
		stale_read_meta(hlc(25, 0, 1), migrated_key),
		key_batch(migrated_key),
	)
	.await
	.unwrap();
	assert_eq!(new_resp.status, ReadStatus::OK);
	let new_value = new_batch.expect("latest value batch");
	let new_value = new_value
		.column(0)
		.as_any()
		.downcast_ref::<LargeBinaryArray>()
		.expect("value batch")
		.value(0)
		.to_vec();
	assert_eq!(new_value, b"new-value");

	dest_sweeper.abort();
	source_sweeper.abort();
	support::abort_node(&source);
	support::abort_node(&dest);
}
