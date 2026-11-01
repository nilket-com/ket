use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use wendelon::schema::RecordBodyCompressionPolicy;
use wendelon::storage::{ipc, multirow};

thread_local! {
	static TRACK: Cell<bool> = const { Cell::new(false) };
	static REQUESTED: Cell<usize> = const { Cell::new(0) };
}
struct TrackedSystem;
fn request(bytes: usize) {
	if TRACK.try_with(Cell::get).unwrap_or(false) {
		let _ = REQUESTED.try_with(|n| n.set(n.get().saturating_add(bytes)));
	}
}
unsafe impl GlobalAlloc for TrackedSystem {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		request(layout.size());
		unsafe { System.alloc(layout) }
	}
	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		request(layout.size());
		unsafe { System.alloc_zeroed(layout) }
	}
	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
		request(size);
		unsafe { System.realloc(ptr, layout, size) }
	}
	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		unsafe { System.dealloc(ptr, layout) }
	}
}
#[global_allocator]
static ALLOCATOR: TrackedSystem = TrackedSystem;
struct StopTracking;
impl Drop for StopTracking {
	fn drop(&mut self) {
		TRACK.with(|on| on.set(false));
	}
}

fn batch() -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new(
			"value",
			DataType::Int64,
			false,
		)])),
		vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
	)
	.unwrap()
}
fn bounded_rejection(bytes: &[u8]) {
	// Initialize normal decode telemetry outside the allocation observation.
	let valid = ipc::encode_record_batch_bytes(&batch(), RecordBodyCompressionPolicy::Uncompressed)
		.unwrap();
	assert_eq!(multirow::decode_batch(&valid).unwrap().num_rows(), 3);
	REQUESTED.with(|n| n.set(0));
	TRACK.with(|on| on.set(true));
	let stop = StopTracking;
	let result = multirow::decode_batch(bytes);
	drop(stop);
	let requested = REQUESTED.with(Cell::get);
	assert!(result.is_err());
	let budget = bytes.len().saturating_mul(8).saturating_add(8192);
	assert!(
		requested <= budget,
		"rejected {}-byte stream requested {requested} allocation bytes (budget {budget}); advertised metadata length must not drive allocation",
		bytes.len()
	);
}

#[test]
fn legacy_metadata_length_cannot_allocate_past_input() {
	bounded_rejection(&(1_i32 << 20).to_le_bytes());
}
#[test]
fn continuation_metadata_length_cannot_allocate_past_input() {
	let mut bytes = vec![0xff; 4];
	bytes.extend_from_slice(&(1_i32 << 20).to_le_bytes());
	bounded_rejection(&bytes);
}
#[test]
fn later_metadata_length_cannot_allocate_past_input() {
	let mut bytes =
		ipc::encode_record_batch_bytes(&batch(), RecordBodyCompressionPolicy::Uncompressed)
			.unwrap();
	assert_eq!(&bytes[bytes.len() - 8..], &[255, 255, 255, 255, 0, 0, 0, 0]);
	bytes.truncate(bytes.len() - 8);
	bytes.extend_from_slice(&[255; 4]);
	bytes.extend_from_slice(&(1_i32 << 20).to_le_bytes());
	bounded_rejection(&bytes);
}
#[test]
fn valid_streams_preserve_rows_and_compression() {
	for policy in [
		RecordBodyCompressionPolicy::Uncompressed,
		RecordBodyCompressionPolicy::Lz4Frame,
	] {
		for count in [1_usize, 2] {
			let batches = vec![batch(); count];
			let bytes = ipc::encode_record_batches_bytes(&batches, policy).unwrap();
			let decoded = multirow::decode_batch(&bytes).unwrap();
			assert_eq!(decoded.num_rows(), 3 * count);
			let values = decoded
				.column(0)
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap();
			assert_eq!(values.values().as_ref(), &[1_i64, 2, 3].repeat(count));
		}
	}
}
