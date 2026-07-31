#[path = "support/control_ring.rs"]
mod support;

use arrow::array::LargeBinaryBuilder;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use wendelon::consensus::{MigrationLedgerEntry, MigrationState, SeedMigration, WendelonRequest};
use wendelon::network::data_server::WendelonDataServer;
use wendelon::network::server::{HrftService, RequestContext};
use wendelon::schema::{
	HybridTimestamp, Identity, MsgType, NodeId, TenantId, write_payload_schema,
};
use wendelon::storage::core::StorageEngine;
use wendelon::storage::manager::SlabManager;

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
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

fn id(locality: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&locality.to_le_bytes());
	Identity::new(bytes)
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

async fn seed_source(slab: &SlabManager, locality: u64, ts: HybridTimestamp, value: &[u8]) {
	let batch = kv_batch(&locality.to_le_bytes(), value);
	let bytes = support::encode_batch(&batch).unwrap();
	slab.append(tenant(), id(locality), ts, &bytes)
		.await
		.unwrap();
}

fn ledger_entry(
	migration_id: u64,
	dest_id: NodeId,
	source_ids: Vec<NodeId>,
	range_start: u64,
	range_end: u64,
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
		migration_hlc_floor: hlc(0, 0, 0),
		state: MigrationState::Pending,
		completed_by: None,
		completed_at_hlc: None,
	}
}

async fn seed_migration(node: &wendelon::node::WendelonNode, entry: MigrationLedgerEntry) {
	node.raft
		.client_write(WendelonRequest::SeedMigration(SeedMigration { entry }))
		.await
		.unwrap();
}

async fn wait_for_migration_state(
	node: &wendelon::node::WendelonNode,
	migration_id: u64,
	state: MigrationState,
) -> MigrationLedgerEntry {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		if let Some(entry) = snapshot.migrations.get(&migration_id)
			&& entry.state == state
		{
			return entry.clone();
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for migration {migration_id} to reach state {state:?}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

async fn wait_for_key_count(
	slab: &SlabManager,
	range_start: u64,
	range_end: u64,
	as_of: HybridTimestamp,
	expected: usize,
) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
	loop {
		let mut keys: Vec<u64> = slab
			.scan_as_of(tenant(), range_start, range_end, as_of)
			.into_iter()
			.map(|(key, _)| key)
			.collect();
		keys.sort();
		keys.dedup();
		if keys.len() == expected {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for {expected} drained keys, observed {}",
			keys.len()
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

#[derive(Clone)]
struct FailingMigrateService {
	inner: Arc<dyn HrftService>,
	fail_migrate: Arc<AtomicBool>,
}

#[async_trait]
impl HrftService for FailingMigrateService {
	async fn handle_frame(
		&self,
		context: RequestContext,
		frame: wendelon::network::core::HrftFrame,
	) -> std::io::Result<Option<wendelon::network::core::HrftFrame>> {
		if frame.header.msg_type == MsgType::MIGRATE_REQ.0
			&& self.fail_migrate.load(Ordering::SeqCst)
		{
			return Err(Error::new(
				ErrorKind::ConnectionRefused,
				"injected migrate source failure",
			));
		}
		self.inner.handle_frame(context, frame).await
	}
}

#[tokio::test]
async fn seeded_migration_drains_without_client_reads() {
	let dest = support::bootstrap_node("migration-sweeper-dest", 61).await;
	let source_a = support::bootstrap_node("migration-sweeper-a", 62).await;
	let source_b = support::bootstrap_node("migration-sweeper-b", 63).await;
	support::wait_for_local_leader(&dest).await;
	support::wait_for_local_leader(&source_a).await;
	support::wait_for_local_leader(&source_b).await;
	dest.handles.migration_sweeper.abort();

	let source_a_service: Arc<dyn HrftService> = source_a.router.clone();
	let source_b_service: Arc<dyn HrftService> = source_b.router.clone();
	let (factory, _pool) = support::loopback_factory_many(vec![
		(source_a.local_node, source_a_service),
		(source_b.local_node, source_b_service),
	]);
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

	let ts = hlc(10, 0, 1);
	for key in 0..50u64 {
		seed_source(&source_a.slab, key, ts, b"v-a").await;
	}
	for key in 50..100u64 {
		seed_source(&source_b.slab, key, ts, b"v-b").await;
	}

	let entry = ledger_entry(
		7_001,
		dest.local_node,
		vec![source_a.local_node, source_b.local_node],
		0,
		99,
	);
	seed_migration(&dest, entry.clone()).await;
	seed_migration(&source_a, entry.clone()).await;
	seed_migration(&source_b, entry).await;

	let sweeper = dest_server.clone().spawn_migration_sweeper(
		std::time::Duration::from_millis(20),
		std::time::Duration::from_millis(40),
		1,
	);

	let completed = wait_for_migration_state(&dest, 7_001, MigrationState::Completed).await;
	assert_eq!(completed.completed_by, Some(dest.local_node));
	wait_for_key_count(&dest.slab, 0, 99, ts, 100).await;

	sweeper.abort();
	support::abort_node(&dest);
	support::abort_node(&source_a);
	support::abort_node(&source_b);
}

#[tokio::test]
async fn migration_sweeper_fails_over_to_next_source_and_retries_after_backoff() {
	let dest = support::bootstrap_node("migration-sweeper-failover-dest", 71).await;
	let source_a = support::bootstrap_node("migration-sweeper-failover-a", 72).await;
	let source_b = support::bootstrap_node("migration-sweeper-failover-b", 73).await;
	support::wait_for_local_leader(&dest).await;
	support::wait_for_local_leader(&source_a).await;
	support::wait_for_local_leader(&source_b).await;
	dest.handles.migration_sweeper.abort();

	let fail_source_a = Arc::new(AtomicBool::new(true));
	let source_a_service: Arc<dyn HrftService> = Arc::new(FailingMigrateService {
		inner: source_a.router.clone(),
		fail_migrate: fail_source_a.clone(),
	});
	let source_b_service: Arc<dyn HrftService> = source_b.router.clone();

	let (factory, _pool) = support::loopback_factory_many(vec![
		(source_a.local_node, source_a_service),
		(source_b.local_node, source_b_service),
	]);
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

	let ts = hlc(20, 0, 1);
	for key in 0..20u64 {
		seed_source(&source_a.slab, key, ts, b"shared").await;
		seed_source(&source_b.slab, key, ts, b"shared").await;
	}

	let entry = ledger_entry(
		7_002,
		dest.local_node,
		vec![source_a.local_node, source_b.local_node],
		0,
		19,
	);
	seed_migration(&dest, entry.clone()).await;
	seed_migration(&source_a, entry.clone()).await;
	seed_migration(&source_b, entry).await;

	let sweeper = dest_server.clone().spawn_migration_sweeper(
		std::time::Duration::from_millis(20),
		std::time::Duration::from_millis(60),
		1,
	);

	// First pass: source_a fails, but the sweeper still advances to
	// source_b and ingests its pages before reporting the migration
	// incomplete and backing off.
	wait_for_key_count(&dest.slab, 0, 19, ts, 20).await;
	let snapshot = dest.state_machine.state_snapshot().await;
	assert_eq!(
		snapshot
			.migrations
			.get(&7_002)
			.expect("migration seeded")
			.state,
		MigrationState::Pending
	);
	drop(snapshot);

	// Recovery: once the preferred source becomes reachable again,
	// the next pass should finish and commit MigrationComplete.
	fail_source_a.store(false, Ordering::SeqCst);
	let completed = wait_for_migration_state(&dest, 7_002, MigrationState::Completed).await;
	assert_eq!(completed.completed_by, Some(dest.local_node));

	sweeper.abort();
	support::abort_node(&dest);
	support::abort_node(&source_a);
	support::abort_node(&source_b);
}
