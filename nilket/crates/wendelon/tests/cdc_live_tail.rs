use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use wendelon::metrics::global as metrics;
use wendelon::network::cdc::{CDC_SUBSCRIPTION_BUFFER, CdcSubscriptionRegistry};
use wendelon::schema::{
	CdcCursor, CdcSubscribeBound, HybridTimestamp, Identity, Locality, RecordRowIndex,
	TableObjectId, TenantId, default_kv_table_descriptor_v1,
};
use wendelon::storage::{
	CatchupSnapshot, CdcFinalizationObserver, CdcLiveEntry, CdcLiveTailRing, CdcLocalityFrontiers,
	CdcLocalityHeartbeatWorker,
};

fn hlc(physical_ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical: 0,
		node_id: 1,
	}
}

fn cursor(ms: u64, key: u8) -> CdcCursor {
	CdcCursor::new(hlc(ms), Identity::new([key; 32]), RecordRowIndex::ZERO)
}

fn batch(value: i32) -> Arc<RecordBatch> {
	let schema = Arc::new(Schema::new(vec![Field::new(
		"value",
		DataType::Int32,
		false,
	)]));
	Arc::new(RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![value]))]).unwrap())
}

fn entry(ms: u64, key: u8) -> CdcLiveEntry {
	entry_for_table(ms, key, TableObjectId::new([3; 16]))
}

fn entry_for_table(ms: u64, key: u8, table_object_id: TableObjectId) -> CdcLiveEntry {
	CdcLiveEntry::new(
		cursor(ms, key),
		TenantId::new(7),
		table_object_id,
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
		batch(key as i32),
	)
}

#[tokio::test]
async fn cdc_live_tail_finalization_observer_releases_and_discards_entries() {
	let ring = Arc::new(CdcLiveTailRing::new(Locality::new(10), u64::MAX));
	let observer = CdcFinalizationObserver::new(ring.clone());
	let first = cursor(10, 1);
	let failed = cursor(11, 2);

	ring.push(entry(10, 1)).await;
	ring.push(entry(11, 2)).await;
	observer.stage(100, first).await;
	observer.stage(101, failed).await;

	assert_eq!(observer.on_finalized(100).await, 1);
	let eligible = ring
		.eligible_entries_after(
			TenantId::new(7),
			TableObjectId::new([3; 16]),
			CdcCursor::MIN,
			hlc(10),
			16,
		)
		.await;
	assert_eq!(eligible.len(), 1);
	assert_eq!(eligible[0].cursor, first);

	assert_eq!(observer.on_terminal_failure(101).await, 1);
	assert_eq!(ring.len().await, 1);
}

#[tokio::test]
async fn cdc_live_tail_finalization_cleanup_counts_entries_already_evicted_from_ring() {
	let ring = Arc::new(CdcLiveTailRing::new(Locality::new(10), 1));
	let observer = CdcFinalizationObserver::new(ring.clone());
	let finalized_after_eviction = cursor(10, 1);
	let terminal_after_eviction = cursor(11, 2);

	observer.stage(100, finalized_after_eviction).await;
	ring.push(entry(10, 1)).await;
	assert_eq!(ring.len().await, 0);
	let before = metrics().cdc_pending_finalization_evicted_total.get();
	assert_eq!(observer.on_finalized(100).await, 0);
	assert!(
		metrics().cdc_pending_finalization_evicted_total.get() >= before + 1,
		"finalization cleanup must count staged entries already evicted by ring overflow"
	);

	observer.stage(101, terminal_after_eviction).await;
	ring.push(entry(11, 2)).await;
	assert_eq!(ring.len().await, 0);
	let before = metrics().cdc_pending_finalization_evicted_total.get();
	assert_eq!(observer.on_terminal_failure(101).await, 1);
	assert!(
		metrics().cdc_pending_finalization_evicted_total.get() >= before + 1,
		"terminal cleanup must count staged entries already evicted by ring overflow"
	);
}

#[tokio::test]
async fn cdc_live_tail_open_time_ring_continuity_rejects_when_head_exceeds_ceiling() {
	let ring = CdcLiveTailRing::new(Locality::new(10), u64::MAX);
	ring.push(entry(90, 1).with_finalized(true)).await;

	let snapshot = CatchupSnapshot::new(hlc(50));
	assert!(
		ring.open_time_ring_continuity_check(&snapshot)
			.await
			.is_err()
	);

	let snapshot = CatchupSnapshot::new(hlc(100));
	assert!(
		ring.open_time_ring_continuity_check(&snapshot)
			.await
			.is_ok()
	);
}

#[tokio::test]
async fn cdc_live_tail_ring_rescan_recovers_entries_missed_by_full_subscription_queue() {
	let tenant_id = TenantId::new(7);
	let table_object_id = TableObjectId::new([3; 16]);
	let ring = CdcLiveTailRing::new(Locality::new(10), u64::MAX);
	let registry = CdcSubscriptionRegistry::new();
	let (_handle, _recv) = registry.register(
		tenant_id,
		table_object_id,
		CdcCursor::MIN,
		CdcSubscribeBound::Live,
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
	);

	for i in 0..CDC_SUBSCRIPTION_BUFFER {
		let entry = Arc::new(entry(10 + i as u64, i as u8).with_finalized(true));
		ring.push_arc(entry.clone()).await;
		assert_eq!(registry.fanout_finalized_entry(entry).sent, 1);
	}

	let missed = Arc::new(entry(10_000, 250).with_finalized(true));
	ring.push_arc(missed.clone()).await;
	let outcome = registry.fanout_finalized_entry(missed.clone());
	assert_eq!(outcome.sent, 0);
	assert_eq!(outcome.full, 1);

	let recovered = ring
		.eligible_entries_after(
			tenant_id,
			table_object_id,
			CdcCursor::MIN,
			hlc(10_000),
			usize::MAX,
		)
		.await;
	assert_eq!(
		recovered.last().map(|entry| entry.cursor),
		Some(missed.cursor)
	);
	assert_eq!(recovered.len(), CDC_SUBSCRIPTION_BUFFER + 1);
}

#[tokio::test]
async fn cdc_live_tail_ring_tracks_eviction_frontier_per_table() {
	let target_table = TableObjectId::new([3; 16]);
	let other_table = TableObjectId::new([4; 16]);
	let ring = CdcLiveTailRing::new(Locality::new(10), 1);

	ring.push(entry_for_table(10, 1, other_table)).await;
	assert_eq!(
		ring.evicted_cursor_frontier(TenantId::new(7), target_table)
			.await,
		None,
		"eviction from a different table must not make this table look behind"
	);
	assert_eq!(
		ring.evicted_cursor_frontier(TenantId::new(7), other_table)
			.await,
		Some(cursor(10, 1))
	);

	ring.push(entry_for_table(20, 2, target_table)).await;
	ring.push(entry_for_table(15, 3, target_table)).await;
	assert_eq!(
		ring.evicted_cursor_frontier(TenantId::new(7), target_table)
			.await,
		Some(cursor(20, 2)),
		"target-table frontier must be monotonic in full cursor order"
	);
}

#[tokio::test]
async fn cdc_live_tail_locality_heartbeat_worker_is_locality_scoped() {
	let worker = CdcLocalityHeartbeatWorker::new(Locality::new(42), Duration::from_millis(1));
	assert_eq!(worker.locality_id, Locality::new(42));
	assert_eq!(worker.interval, Duration::from_millis(1));
}

#[test]
fn cdc_live_tail_locality_frontiers_are_monotonic_and_min_reduced() {
	let frontiers = CdcLocalityFrontiers::new();
	let locality_a = Locality::new(10);
	let locality_b = Locality::new(20);
	let locality_c = Locality::new(30);

	assert_eq!(frontiers.min_for_localities([locality_a]), None);
	assert_eq!(frontiers.advance(locality_a, hlc(100)), hlc(100));
	assert_eq!(frontiers.advance(locality_a, hlc(90)), hlc(100));
	assert_eq!(frontiers.advance(locality_b, hlc(50)), hlc(50));

	assert_eq!(
		frontiers.min_for_localities([locality_a, locality_b]),
		Some(hlc(50))
	);
	assert_eq!(frontiers.advance(locality_b, hlc(110)), hlc(110));
	assert_eq!(
		frontiers.min_for_localities([locality_a, locality_b]),
		Some(hlc(100))
	);
	assert_eq!(frontiers.min_for_localities([locality_a, locality_c]), None);
	assert_eq!(
		frontiers.snapshot(),
		vec![(locality_a, hlc(100)), (locality_b, hlc(110))]
	);
}

#[tokio::test]
async fn cdc_live_tail_locality_frontier_waiter_wakes_only_on_advance() {
	let frontiers = CdcLocalityFrontiers::new();
	let locality = Locality::new(42);
	frontiers.advance(locality, hlc(100));

	let waiter = frontiers.wait_for_advance();
	tokio::pin!(waiter);
	frontiers.advance(locality, hlc(90));
	assert!(
		tokio::time::timeout(Duration::from_millis(10), &mut waiter)
			.await
			.is_err()
	);

	frontiers.advance(locality, hlc(110));
	tokio::time::timeout(Duration::from_millis(100), &mut waiter)
		.await
		.unwrap();
}

#[tokio::test]
async fn cdc_live_tail_locality_heartbeat_worker_can_advance_frontier() {
	let frontiers = Arc::new(CdcLocalityFrontiers::new());
	let emitted = Arc::new(AtomicU64::new(0));
	let worker = CdcLocalityHeartbeatWorker::new(Locality::new(42), Duration::from_millis(1));

	let task = {
		let frontiers = frontiers.clone();
		let emitted = emitted.clone();
		tokio::spawn(async move {
			worker
				.run(
					|_| async { true },
					move |locality_id| {
						let frontiers = frontiers.clone();
						let emitted = emitted.clone();
						async move {
							let physical_ms = 100 + emitted.fetch_add(1, Ordering::SeqCst);
							frontiers.advance(locality_id, hlc(physical_ms));
							Ok(())
						}
					},
				)
				.await
		})
	};

	tokio::time::timeout(Duration::from_millis(100), async {
		loop {
			if frontiers.get(Locality::new(42)).is_some() {
				break;
			}
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await
	.unwrap();
	task.abort();
	let _ = task.await;

	assert!(frontiers.get(Locality::new(42)).unwrap() >= hlc(100));
}
