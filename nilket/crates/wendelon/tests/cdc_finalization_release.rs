use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::schema::{
	CdcCursor, HybridTimestamp, Identity, Locality, RecordRowIndex, TableObjectId, TenantId,
	default_kv_table_descriptor_v1,
};
use wendelon::storage::{CdcFinalizationObserver, CdcLiveEntry, CdcLiveTailRing};

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

fn entry(ms: u64, key: u8) -> CdcLiveEntry {
	CdcLiveEntry::new(
		cursor(ms, key),
		TenantId::new(7),
		TableObjectId::new([3; 16]),
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
		batch(key as i32),
	)
}

#[tokio::test]
async fn cdc_finalization_release_marks_success_and_evicts_failure() {
	let ring = Arc::new(CdcLiveTailRing::new(Locality::new(10), u64::MAX));
	let observer = CdcFinalizationObserver::new(ring.clone());
	let finalized = cursor(10, 1);
	let failed = cursor(11, 2);

	ring.push(entry(10, 1)).await;
	ring.push(entry(11, 2)).await;
	observer.stage(100, finalized).await;
	observer.stage(101, failed).await;

	assert_eq!(observer.on_finalized(100).await, 1);
	assert_eq!(observer.on_terminal_failure(101).await, 1);
	assert_eq!(ring.len().await, 1);
	assert!(ring.mark_finalized(finalized).await);
}
