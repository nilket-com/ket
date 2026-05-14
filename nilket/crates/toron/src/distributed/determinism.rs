use arrow::array::{
	ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
	Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::distributed::task::ArrowIpcDigest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortPolicy {
	None,
	FullRowLex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CanonicalBatchRowCount(pub usize);

impl CanonicalBatchRowCount {
	pub const DEFAULT: Self = Self(65_536);

	pub const fn new(value: usize) -> Self {
		Self(value)
	}

	pub const fn get(self) -> usize {
		self.0
	}
}

pub fn canonical_normalize(
	batches: &[RecordBatch],
	sort_policy: SortPolicy,
) -> Result<Vec<RecordBatch>, ArrowError> {
	canonical_normalize_with_batch_rows(batches, sort_policy, CanonicalBatchRowCount::DEFAULT)
}

pub fn canonical_arrow_ipc_bytes(
	batches: &[RecordBatch],
	sort_policy: SortPolicy,
) -> Result<Vec<u8>, ArrowError> {
	let normalized = canonical_normalize(batches, sort_policy)?;
	encode_ipc_stream(&normalized)
}

pub fn canonical_arrow_ipc_digest(
	batches: &[RecordBatch],
	sort_policy: SortPolicy,
) -> Result<ArrowIpcDigest, ArrowError> {
	let bytes = canonical_arrow_ipc_bytes(batches, sort_policy)?;
	Ok(ArrowIpcDigest::new(wendelon::hash::blake3::digest(&bytes)))
}

pub(crate) fn canonical_normalize_with_batch_rows(
	batches: &[RecordBatch],
	sort_policy: SortPolicy,
	batch_rows: CanonicalBatchRowCount,
) -> Result<Vec<RecordBatch>, ArrowError> {
	if batches.is_empty() {
		return Ok(Vec::new());
	}
	if batch_rows.get() == 0 {
		return Err(ArrowError::InvalidArgumentError(
			"canonical batch row count must be greater than zero".to_string(),
		));
	}

	let schema = batches[0].schema();
	ensure_same_schema(batches, &schema)?;

	let concatenated = concat_batches(&schema, batches)?;
	let sorted = match sort_policy {
		SortPolicy::None => concatenated,
		SortPolicy::FullRowLex => full_row_lex_sort(concatenated)?,
	};
	let sorted = canonicalize_null_slots(sorted)?;
	Ok(reslice_batch(sorted, batch_rows))
}

fn ensure_same_schema(batches: &[RecordBatch], schema: &SchemaRef) -> Result<(), ArrowError> {
	for batch in batches {
		if batch.schema() != *schema {
			return Err(ArrowError::SchemaError(
				"canonical normalization requires matching RecordBatch schemas".to_string(),
			));
		}
	}
	Ok(())
}

fn full_row_lex_sort(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
	if batch.num_columns() == 0 || batch.num_rows() <= 1 {
		return Ok(batch);
	}

	let sort_options = Some(SortOptions {
		descending: false,
		nulls_first: false,
	});
	let mut columns: Vec<SortColumn> = batch
		.columns()
		.iter()
		.cloned()
		.map(|values| SortColumn {
			values,
			options: sort_options,
		})
		.collect();
	columns.push(SortColumn {
		values: original_row_position_array(batch.num_rows()),
		options: sort_options,
	});

	let indices = lexsort_to_indices(&columns, None)?;
	let sorted_columns = batch
		.columns()
		.iter()
		.map(|column| take(column.as_ref(), &indices, None))
		.collect::<Result<Vec<_>, _>>()?;

	RecordBatch::try_new(batch.schema(), sorted_columns)
}

fn canonicalize_null_slots(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
	if batch
		.columns()
		.iter()
		.all(|column| column.null_count() == 0)
	{
		return Ok(batch);
	}
	let columns = batch
		.columns()
		.iter()
		.map(canonicalize_array_null_slots)
		.collect::<Vec<_>>();
	RecordBatch::try_new(batch.schema(), columns)
}

fn canonicalize_array_null_slots(array: &ArrayRef) -> ArrayRef {
	if array.null_count() == 0 {
		return array.clone();
	}

	macro_rules! rebuild_primitive {
		($ty:ty) => {
			if let Some(values) = array.as_any().downcast_ref::<$ty>() {
				return Arc::new(<$ty>::from(values.iter().collect::<Vec<_>>())) as ArrayRef;
			}
		};
	}

	rebuild_primitive!(BooleanArray);
	rebuild_primitive!(Int8Array);
	rebuild_primitive!(Int16Array);
	rebuild_primitive!(Int32Array);
	rebuild_primitive!(Int64Array);
	rebuild_primitive!(UInt8Array);
	rebuild_primitive!(UInt16Array);
	rebuild_primitive!(UInt32Array);
	rebuild_primitive!(UInt64Array);
	rebuild_primitive!(Float32Array);
	rebuild_primitive!(Float64Array);

	if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
		return Arc::new(StringArray::from(values.iter().collect::<Vec<_>>())) as ArrayRef;
	}

	array.clone()
}

fn original_row_position_array(row_count: usize) -> ArrayRef {
	Arc::new(UInt64Array::from_iter_values(
		0..u64::try_from(row_count).expect("row count fits u64"),
	))
}

fn reslice_batch(batch: RecordBatch, batch_rows: CanonicalBatchRowCount) -> Vec<RecordBatch> {
	let row_count = batch.num_rows();
	if row_count == 0 {
		return vec![batch];
	}

	let mut offset = 0;
	let mut batches = Vec::new();
	while offset < row_count {
		let len = batch_rows.get().min(row_count - offset);
		batches.push(batch.slice(offset, len));
		offset += len;
	}
	batches
}

fn encode_ipc_stream(batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError> {
	let Some(first_batch) = batches.first() else {
		return Ok(Vec::new());
	};
	let schema = first_batch.schema();
	ensure_same_schema(batches, &schema)?;

	let mut buffer = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
		for batch in batches {
			writer.write(batch)?;
		}
		writer.finish()?;
	}
	Ok(buffer)
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::array::{Int32Array, StringArray};
	use arrow::datatypes::{DataType, Field, Schema};

	fn batch(ids: Vec<Option<i32>>, labels: Vec<Option<&str>>) -> Result<RecordBatch, ArrowError> {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int32, true),
			Field::new("label", DataType::Utf8, true),
		]));

		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int32Array::from(ids)),
				Arc::new(StringArray::from(labels)),
			],
		)
	}

	#[test]
	fn full_row_lex_digest_is_stable_across_order_and_batch_boundaries() {
		let left = vec![
			batch(vec![Some(2), Some(1)], vec![Some("b"), None]).unwrap(),
			batch(vec![Some(3)], vec![Some("c")]).unwrap(),
		];
		let right = vec![
			batch(vec![Some(3), Some(1)], vec![Some("c"), None]).unwrap(),
			batch(vec![Some(2)], vec![Some("b")]).unwrap(),
		];

		let left_digest = canonical_arrow_ipc_digest(&left, SortPolicy::FullRowLex).unwrap();
		let right_digest = canonical_arrow_ipc_digest(&right, SortPolicy::FullRowLex).unwrap();

		assert_eq!(left_digest, right_digest);
	}

	#[test]
	fn unsorted_digest_preserves_row_order() {
		let left = vec![batch(vec![Some(1), Some(2)], vec![Some("a"), Some("b")]).unwrap()];
		let right = vec![batch(vec![Some(2), Some(1)], vec![Some("b"), Some("a")]).unwrap()];

		let left_digest = canonical_arrow_ipc_digest(&left, SortPolicy::None).unwrap();
		let right_digest = canonical_arrow_ipc_digest(&right, SortPolicy::None).unwrap();

		assert_ne!(left_digest, right_digest);
	}

	#[test]
	fn sort_runs_before_reslice() {
		let batches = vec![
			batch(
				vec![Some(4), Some(3), Some(2), Some(1)],
				vec![Some("d"), Some("c"), Some("b"), Some("a")],
			)
			.unwrap(),
		];

		let normalized = canonical_normalize_with_batch_rows(
			&batches,
			SortPolicy::FullRowLex,
			CanonicalBatchRowCount::new(2),
		)
		.unwrap();

		assert_eq!(normalized.len(), 2);
		let first_ids = normalized[0]
			.column(0)
			.as_any()
			.downcast_ref::<Int32Array>()
			.unwrap();
		let second_ids = normalized[1]
			.column(0)
			.as_any()
			.downcast_ref::<Int32Array>()
			.unwrap();
		assert_eq!(first_ids.value(0), 1);
		assert_eq!(first_ids.value(1), 2);
		assert_eq!(second_ids.value(0), 3);
		assert_eq!(second_ids.value(1), 4);
	}

	#[test]
	fn zero_row_batches_keep_schema_for_ipc_output() {
		let batches = vec![batch(Vec::new(), Vec::new()).unwrap()];

		let normalized = canonical_normalize(&batches, SortPolicy::FullRowLex).unwrap();
		let bytes = canonical_arrow_ipc_bytes(&batches, SortPolicy::FullRowLex).unwrap();

		assert_eq!(normalized.len(), 1);
		assert_eq!(normalized[0].num_rows(), 0);
		assert!(!bytes.is_empty());
	}
}
