#[path = "support/control_ring.rs"]
mod support;

use arrow::array::{LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

async fn bootstrap_custom_node(
	tag: &str,
	node_id: u32,
	slab_max_bytes: u64,
) -> (support::TestDir, WendelonNode) {
	let dir = support::tempdir(tag);
	let mut cfg = NodeBootstrapConfig::dev(node_id, dir.to_path_buf(), "127.0.0.1:0".into());
	cfg.slab_max_bytes = slab_max_bytes;
	(dir, WendelonNode::bootstrap_single_node(cfg).await.unwrap())
}

async fn bootstrap_retention_node(
	tag: &str,
	node_id: u32,
	slab_max_bytes: u64,
	retention_window: Duration,
) -> (support::TestDir, WendelonNode) {
	let dir = support::tempdir(tag);
	let mut cfg = NodeBootstrapConfig::dev(node_id, dir.to_path_buf(), "127.0.0.1:0".into());
	cfg.slab_max_bytes = slab_max_bytes;
	cfg.mvcc_retention_policy =
		wendelon::storage::MvccRetentionPolicy::FixedWindow(retention_window);
	(dir, WendelonNode::bootstrap_single_node(cfg).await.unwrap())
}

async fn bootstrap_retention_tiered_node(
	tag: &str,
	node_id: u32,
	slab_max_bytes: u64,
	retention_window: Duration,
) -> (support::TestDir, support::TestDir, WendelonNode) {
	let dir = support::tempdir(tag);
	let mut cfg = NodeBootstrapConfig::dev(node_id, dir.to_path_buf(), "127.0.0.1:0".into());
	cfg.slab_max_bytes = slab_max_bytes;
	cfg.mvcc_retention_policy =
		wendelon::storage::MvccRetentionPolicy::FixedWindow(retention_window);
	let l2_dir = support::tempdir(&format!("{tag}-l2"));
	cfg.l2 = Some(wendelon::storage::ObjectStoreConfig::LocalFs {
		root: l2_dir.to_path_buf(),
	});
	cfg.tiering.scan_interval = Duration::from_millis(20);
	let node = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	(dir, l2_dir, node)
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
		if let Some(entry) = snapshot.migrations.get(&migration_id)
			&& entry.state == state
		{
			return entry.clone();
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
		key_identity: Identity::blake3_tenant(tenant(), key),
		key_len: key.len() as u32,
		_pad2: [0; 8],
		as_of_hlc,
	}
}

#[tokio::test]
async fn background_reclaim_rewrites_mixed_slab_and_preserves_live_versions() {
	let (_source_dir, source) = bootstrap_custom_node("tombstone-source-mixed", 81, 8 * 1024).await;
	let (_dest_dir, dest) =
		bootstrap_custom_node("tombstone-dest-mixed", 82, 64 * 1024 * 1024).await;
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
		false,
	);
	let source_service: Arc<dyn HrftService> = source.router.clone();
	let (factory, _pool) =
		support::loopback_factory_many(vec![(source.local_node, source_service)]);
	let dest_server = Arc::new(
		WendelonDataServer::new(
			dest.slab.clone(),
			dest.raft.clone(),
			dest.log_store.clone(),
			dest.state_machine.clone(),
			dest.local_node,
			dest.clock.clone(),
			dest.ring.clone(),
			factory,
			dest.pool.clone(),
		)
		.unwrap(),
	);
	let dest_sweeper = dest_server.clone().spawn_migration_sweeper(
		Duration::from_millis(20),
		Duration::from_millis(40),
		1,
		false,
	);

	let completed = wait_for_migration_state(&dest, 9_001, MigrationState::Completed).await;
	mirror_completion(&source, &completed).await;
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		let after_bytes = source.slab.local_l1_bytes().await;
		let moved_missing = source
			.slab
			.lookup_as_of(tenant(), key_identity(moved_key), moved_ts)
			.unwrap()
			.is_none();
		let kept_present = source
			.slab
			.lookup_as_of(tenant(), key_identity(kept_key), keep_ts)
			.unwrap()
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
		.unwrap()
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
	let (_source_dir, source) =
		bootstrap_custom_node("tombstone-source-history", 91, 12 * 1024).await;
	let (_dest_dir, dest) =
		bootstrap_custom_node("tombstone-dest-history", 92, 64 * 1024 * 1024).await;
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
		false,
	);
	let source_service: Arc<dyn HrftService> = source.router.clone();
	let (factory, _pool) =
		support::loopback_factory_many(vec![(source.local_node, source_service)]);
	let dest_server = Arc::new(
		WendelonDataServer::new(
			dest.slab.clone(),
			dest.raft.clone(),
			dest.log_store.clone(),
			dest.state_machine.clone(),
			dest.local_node,
			dest.clock.clone(),
			dest.ring.clone(),
			factory,
			dest.pool.clone(),
		)
		.unwrap(),
	);
	let dest_sweeper = dest_server.clone().spawn_migration_sweeper(
		Duration::from_millis(20),
		Duration::from_millis(40),
		1,
		false,
	);

	let completed = wait_for_migration_state(&dest, 9_002, MigrationState::Completed).await;
	mirror_completion(&source, &completed).await;

	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if source
			.slab
			.lookup_as_of(tenant(), key_identity(migrated_key), newer)
			.unwrap()
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

#[tokio::test]
async fn mvcc_retention_pruning_is_audit_logged_and_preserves_retained_window_reads() {
	let (_node_dir, node) = bootstrap_retention_node(
		"mvcc-retention-prune",
		93,
		8 * 1024,
		Duration::from_secs(15),
	)
	.await;
	support::wait_for_local_leader(&node).await;
	node.handles.migration_sweeper.abort();

	let audit_path = node.slab.data_dir().join("audit.ndjson");
	let now_ms = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let key = b"retention-key";
	let oldest = hlc(now_ms.saturating_sub(50_000), 0, 1);
	let anchor = hlc(now_ms.saturating_sub(20_000), 0, 1);
	let latest = hlc(now_ms.saturating_sub(5_000), 0, 1);
	seed_source(&node.slab, key, oldest, b"oldest-value").await;
	seed_source(&node.slab, key, anchor, b"anchor-value").await;
	seed_source(&node.slab, key, latest, b"latest-value").await;
	let rotate_value = vec![b'x'; 16 * 1024];
	seed_source(&node.slab, b"rotate-key", latest, &rotate_value).await;
	// ADR 0098 decision 4: a payload larger than the cap rotates BEFORE it lands,
	// so the oversized value seals the history slab itself and then sits alone
	// in a second sealed slab once anything follows it.
	assert_eq!(
		node.slab.sealed_count().await,
		2,
		"precondition: history slab sealed"
	);

	let sweeper = node.data_server.clone().spawn_migration_sweeper(
		Duration::from_millis(20),
		Duration::from_millis(40),
		1,
		false,
	);
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		let audit = std::fs::read_to_string(&audit_path).unwrap_or_default();
		if node
			.slab
			.lookup_as_of(tenant(), key_identity(key), oldest)
			.unwrap()
			.is_none()
			&& audit.contains("\"event_type\":\"mvcc.snapshot_pruned\"")
		{
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for MVCC retention pruning"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let retained_as_of = hlc(now_ms.saturating_sub(12_000), 0, 1);
	let (retained_resp, retained_batch) = DataRing::read(
		&*node.data_server,
		stale_read_meta(retained_as_of, key),
		key_batch(key),
	)
	.await
	.unwrap();
	assert_eq!(retained_resp.status, ReadStatus::OK);
	assert_eq!(
		decode_single_value(&support::encode_batch(&retained_batch.unwrap()).unwrap()),
		b"anchor-value"
	);

	let pruned_as_of = hlc(now_ms.saturating_sub(30_000), 0, 1);
	let (pruned_resp, pruned_batch) = DataRing::read(
		&*node.data_server,
		stale_read_meta(pruned_as_of, key),
		key_batch(key),
	)
	.await
	.unwrap();
	assert_eq!(pruned_resp.status, ReadStatus::RETENTION_EXPIRED);
	assert!(pruned_batch.is_none());

	let audit = std::fs::read_to_string(&audit_path).unwrap();
	assert!(audit.contains("\"event_type\":\"mvcc.snapshot_pruned\""));
	assert!(audit.contains("pruned_versions=1"));

	sweeper.abort();
	support::abort_node(&node);
}

/// The node-local pause suppresses physical MVCC reclaim and nothing else. The
/// unpaused twin above prunes within two seconds on the same fixture, so a flag
/// that were merely read - or gated in the wrong place - fails here.
#[tokio::test]
async fn paused_mvcc_retention_prunes_nothing_and_leaves_reads_unchanged() {
	let (_node_dir, node) = bootstrap_retention_node(
		"mvcc-retention-paused",
		94,
		8 * 1024,
		Duration::from_secs(15),
	)
	.await;
	support::wait_for_local_leader(&node).await;
	node.handles.migration_sweeper.abort();

	let audit_path = node.slab.data_dir().join("audit.ndjson");
	let now_ms = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let key = b"retention-key";
	let oldest = hlc(now_ms.saturating_sub(50_000), 0, 1);
	let anchor = hlc(now_ms.saturating_sub(20_000), 0, 1);
	let latest = hlc(now_ms.saturating_sub(5_000), 0, 1);
	seed_source(&node.slab, key, oldest, b"oldest-value").await;
	seed_source(&node.slab, key, anchor, b"anchor-value").await;
	seed_source(&node.slab, key, latest, b"latest-value").await;
	let rotate_value = vec![b'x'; 16 * 1024];
	seed_source(&node.slab, b"rotate-key", latest, &rotate_value).await;
	// ADR 0098 decision 4: a payload larger than the cap rotates BEFORE it lands,
	// so the oversized value seals the history slab itself and then sits alone
	// in a second sealed slab once anything follows it.
	assert_eq!(
		node.slab.sealed_count().await,
		2,
		"precondition: history slab sealed"
	);

	let sweeper = node.data_server.clone().spawn_migration_sweeper(
		Duration::from_millis(20),
		Duration::from_millis(40),
		1,
		true,
	);
	// Long enough for many ticks of the 20 ms sweeper, and comfortably past the
	// two-second window in which the unpaused arm completes.
	tokio::time::sleep(Duration::from_secs(3)).await;

	assert!(
		node.slab
			.lookup_as_of(tenant(), key_identity(key), oldest)
			.unwrap()
			.is_some(),
		"a paused node reclaims nothing: the oldest version is still resident"
	);
	let audit = std::fs::read_to_string(&audit_path).unwrap_or_default();
	assert!(
		!audit.contains("\"event_type\":\"mvcc.snapshot_pruned\""),
		"a paused node emits no prune event: {audit}"
	);

	// The read floor is a policy decision, not a consequence of reclaim having
	// run. A read behind the window is refused on a paused node exactly as it is
	// on an unpaused one, even though the version it names is still on disk.
	let pruned_as_of = hlc(now_ms.saturating_sub(30_000), 0, 1);
	let (pruned_resp, pruned_batch) = DataRing::read(
		&*node.data_server,
		stale_read_meta(pruned_as_of, key),
		key_batch(key),
	)
	.await
	.unwrap();
	assert_eq!(pruned_resp.status, ReadStatus::RETENTION_EXPIRED);
	assert!(pruned_batch.is_none());

	// Chosen AFTER the sleep above: the read floor tracks wall clock, so a probe
	// picked relative to seed time would drift out of the 15 s window while the
	// test waits and would fail for a reason that has nothing to do with pausing.
	let retained_as_of = hlc(now_ms.saturating_sub(6_000), 0, 1);
	let (retained_resp, retained_batch) = DataRing::read(
		&*node.data_server,
		stale_read_meta(retained_as_of, key),
		key_batch(key),
	)
	.await
	.unwrap();
	assert_eq!(retained_resp.status, ReadStatus::OK);
	assert_eq!(
		decode_single_value(&support::encode_batch(&retained_batch.unwrap()).unwrap()),
		b"anchor-value"
	);

	sweeper.abort();
	support::abort_node(&node);
}

#[tokio::test]
async fn mvcc_retention_fully_pruned_tiered_slab_retires_end_to_end() {
	let (_node_dir, l2_dir, node) = bootstrap_retention_tiered_node(
		"mvcc-retention-retire-e2e",
		94,
		8 * 1024,
		Duration::from_secs(15),
	)
	.await;
	support::wait_for_local_leader(&node).await;
	node.handles.migration_sweeper.abort();
	if let Some(handle) = &node.handles.tiering_sweeper {
		handle.abort();
	}

	let audit_path = node.slab.data_dir().join("audit.ndjson");
	let now_ms = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let key = b"retire-key";
	let oldest = hlc(now_ms.saturating_sub(50_000), 0, 1);
	let anchor = hlc(now_ms.saturating_sub(20_000), 0, 1);
	let latest = hlc(now_ms.saturating_sub(5_000), 0, 1);

	let oldest_value = vec![b'x'; 16 * 1024];
	let oldest_bytes = support::encode_batch(&kv_batch(key, &oldest_value)).unwrap();
	let oldest_loc = node
		.slab
		.append(tenant(), key_identity(key), oldest, &oldest_bytes)
		.await
		.unwrap();
	let retired_slab_id = oldest_loc.slab_id;
	let anchor_bytes = support::encode_batch(&kv_batch(key, b"anchor-value")).unwrap();
	let anchor_loc = node
		.slab
		.append(tenant(), key_identity(key), anchor, &anchor_bytes)
		.await
		.unwrap();
	assert_ne!(
		anchor_loc.slab_id, retired_slab_id,
		"anchor version must land in a fresh slab so the old slab is fully prunable"
	);
	assert_eq!(
		node.slab.sealed_count().await,
		1,
		"old history slab should be sealed before tier retirement runs"
	);
	seed_source(&node.slab, key, latest, b"latest-value").await;

	let object_store: Arc<dyn wendelon::storage::ObjectStore> =
		Arc::new(wendelon::storage::LocalFsObjectStore::new(l2_dir.to_path_buf()).unwrap());
	let sweeper = wendelon::storage::SlabSweeper::new(
		node.slab.clone(),
		object_store,
		Arc::new(wendelon::storage::RaftCommitter::new(node.raft.clone())),
		node.state_machine.clone(),
		wendelon::storage::SweeperConfig::default(),
	)
	.with_clock(node.clock.clone())
	.with_audit_sink(node.local_node, node.audit_sink.clone())
	.with_mvcc_retention_policy(wendelon::storage::MvccRetentionPolicy::FixedWindow(
		Duration::from_secs(15),
	));

	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		let report = sweeper.sweep_once().await.unwrap();
		let snapshot = node.state_machine.state_snapshot().await;
		let audit = std::fs::read_to_string(&audit_path).unwrap_or_default();
		let retired_in_sm = !snapshot.tiered_slabs.contains_key(&retired_slab_id);
		let retired_locally = node.slab.tier_pointer(retired_slab_id).await.is_none();
		if retired_in_sm
			&& retired_locally
			&& node
				.slab
				.lookup_as_of(tenant(), key_identity(key), oldest)
				.unwrap()
				.is_none()
			&& node.slab.read(oldest_loc).await.is_err()
			&& audit.contains("\"event_type\":\"mvcc.snapshot_pruned\"")
			&& audit.contains("rewrite_action=retire")
		{
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for fully-pruned tier retirement: report={report:?} \
			 retired_in_sm={retired_in_sm} retired_locally={retired_locally} \
			 tiered_in_sm={} retained_lookup={} stale_read_err={} audit_retire={}",
			snapshot.tiered_slabs.contains_key(&retired_slab_id),
			node.slab
				.lookup_as_of(tenant(), key_identity(key), oldest)
				.unwrap()
				.is_none(),
			node.slab.read(oldest_loc).await.is_err(),
			audit.contains("rewrite_action=retire"),
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let retained_as_of = hlc(now_ms.saturating_sub(12_000), 0, 1);
	let (retained_resp, retained_batch) = DataRing::read(
		&*node.data_server,
		stale_read_meta(retained_as_of, key),
		key_batch(key),
	)
	.await
	.unwrap();
	assert_eq!(retained_resp.status, ReadStatus::OK);
	assert_eq!(
		decode_single_value(&support::encode_batch(&retained_batch.unwrap()).unwrap()),
		b"anchor-value"
	);

	let pruned_as_of = hlc(now_ms.saturating_sub(30_000), 0, 1);
	let (pruned_resp, pruned_batch) = DataRing::read(
		&*node.data_server,
		stale_read_meta(pruned_as_of, key),
		key_batch(key),
	)
	.await
	.unwrap();
	assert_eq!(pruned_resp.status, ReadStatus::RETENTION_EXPIRED);
	assert!(pruned_batch.is_none());

	let audit = std::fs::read_to_string(&audit_path).unwrap();
	assert!(audit.contains("\"event_type\":\"mvcc.snapshot_pruned\""));
	assert!(audit.contains("rewrite_action=retire"));
	support::abort_node(&node);
}
