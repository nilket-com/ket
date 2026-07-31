use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::network::cdc::CdcSubscriptionRegistry;
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

fn entry(ms: u64, key: u8, finalized: bool) -> CdcLiveEntry {
	CdcLiveEntry::new(
		cursor(ms, key),
		TenantId::new(7),
		TableObjectId::new([3; 16]),
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
		batch(key as i32),
	)
	.with_finalized(finalized)
}

#[tokio::test]
async fn cdc_admission_stage_fanout_only_after_finalization() {
	let tenant_id = TenantId::new(7);
	let table_object_id = TableObjectId::new([3; 16]);
	let ring = CdcLiveTailRing::new(Locality::new(10), u64::MAX);
	let registry = CdcSubscriptionRegistry::new();
	let (_handle, mut recv) = registry.register(
		tenant_id,
		table_object_id,
		CdcCursor::MIN,
		CdcSubscribeBound::Live,
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
	);

	let staged = Arc::new(entry(10, 1, false));
	ring.push_arc(staged.clone()).await;
	assert_eq!(registry.fanout_finalized_entry(staged.clone()).sent, 0);
	assert!(recv.try_recv().is_err());

	let released = ring
		.mark_finalized_entry(staged.cursor)
		.await
		.expect("staged entry should still be in ring");
	assert_eq!(registry.fanout_finalized_entry(released).sent, 1);
	assert_eq!(recv.recv().await.unwrap().cursor, staged.cursor);
}
