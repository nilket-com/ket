use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::schema::{
	HybridTimestamp, Identity, Locality, LocalityPolicyV1, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptorV1, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, RowCount, RowGroupRowCount, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	SlabId, TableObjectId,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	PlaintextByteRange, RowGroupWriterConfig, TieredRecordId,
	encode_multirow_from_survivors_with_row_group_config, parse_record_buffer_directory,
};

fn descriptor(schema: &Schema) -> wendelon::schema::TableSchemaDescriptorV1 {
	wendelon::schema::TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x55; 16]),
		1,
		"test",
		"row_group_lsm",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicyV1::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn batch(row_count: usize) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("value", DataType::Int64, false),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as ArrayRef,
			Arc::new(Int64Array::from_iter_values(
				(0..row_count).map(|row| 100_i64 + row as i64),
			)) as ArrayRef,
		],
	)
	.unwrap()
}

fn source_rows(row_count: usize) -> Vec<MultiRowSourceRow> {
	(0..row_count)
		.map(|row| MultiRowSourceRow {
			source_row_index: row,
			key_identity: Identity::new([row as u8; 32]),
			routing_locality: Locality::new(7),
			row_hlc: HybridTimestamp {
				physical_ms: 1,
				logical: row as u32,
				node_id: 1,
			},
			is_tombstone: false,
			table_object_id: Some(TableObjectId::new([0x55; 16])),
		})
		.collect()
}

fn row_group_config(enabled: bool) -> RowGroupWriterConfig {
	RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(4),
		RowGroupRowCount::new(4),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(enabled)
}

#[test]
fn row_group_rewrite_output_is_deterministic_for_identical_inputs() {
	let batch = batch(10);
	let descriptor = descriptor(batch.schema().as_ref());
	let rows = source_rows(10);
	let config = row_group_config(true);
	let left = encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&rows,
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&config,
	)
	.unwrap();
	let right = encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&rows,
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&config,
	)
	.unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(55),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: left.frame.len() as u64,
		},
		&left.frame,
		&descriptor,
	)
	.unwrap();

	assert_eq!(left.frame, right.frame);
	assert_eq!(parsed.page_directory.unwrap().row_groups.len(), 3);
}

#[test]
fn row_group_rewrite_respects_disabled_writer_config() {
	let batch = batch(10);
	let descriptor = descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&source_rows(10),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(false),
	)
	.unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(55),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&descriptor,
	)
	.unwrap();

	assert!(parsed.page_directory.is_none());
}
