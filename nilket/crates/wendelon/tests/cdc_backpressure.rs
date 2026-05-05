use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::network::cdc::{CDC_SUBSCRIPTION_BUFFER, CdcSubscriptionRegistry};
use wendelon::schema::{
	CdcCursor, CdcSubscribeBound, HybridTimestamp, Identity, Locality, RecordRowIndex,
	TableObjectId, TenantId, default_kv_table_descriptor_v1,
};
use wendelon::storage::{CdcLiveEntry, CdcLiveTailRing};

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
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

fn entry(ms: u64, key: u8, table_object_id: TableObjectId) -> CdcLiveEntry {
	CdcLiveEntry::new(
		cursor(ms, key),
		TenantId::new(7),
		table_object_id,
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
		batch(key as i32),
	)
	.with_finalized(true)
}

#[tokio::test]
async fn cdc_backpressure_keeps_ingest_nonblocking_and_records_table_gap() {
	let tenant_id = TenantId::new(7);
	let table_object_id = TableObjectId::new([3; 16]);
	let ring = CdcLiveTailRing::new(Locality::new(10), 1);
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
		let entry = Arc::new(entry(10 + i as u64, i as u8, table_object_id));
		ring.push_arc(entry.clone()).await;
		let _ = registry.fanout_finalized_entry(entry);
	}
	let overflowed = Arc::new(entry(10_000, 250, table_object_id));
	let _ = ring.push_arc(overflowed.clone()).await;
	let outcome = registry.fanout_finalized_entry(overflowed.clone());

	assert_eq!(outcome.sent, 0);
	assert_eq!(outcome.full, 1);
	assert_eq!(
		ring.evicted_cursor_frontier(tenant_id, table_object_id)
			.await,
		Some(overflowed.cursor)
	);
}
