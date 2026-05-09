use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;
use wendelon::schema::{
	HybridTimestamp, Identity, Locality, LocalityPolicyV1, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptorV1, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, RowCount, RowGroupRowCount, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	SlabId, TableObjectId,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	PlaintextByteRange, ROW_GROUP_SCAN_TARGET_ROWS, RowGroupWriterConfig, TieredRecordId,
	decode_batch, encode_multirow_from_survivors_with_row_group_config,
	parse_record_buffer_directory,
};

fn descriptor(schema: &Schema) -> wendelon::schema::TableSchemaDescriptorV1 {
	wendelon::schema::TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x56; 16]),
		1,
		"test",
		"row_group_writer",
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
		Field::new("name", DataType::Utf8, true),
	]));
	let ids = (0..row_count).map(|row| row as i64).collect::<Vec<_>>();
	let names = (0..row_count)
		.map(|row| Some(format!("region-{row}")))
		.collect::<Vec<_>>();
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(ids)) as ArrayRef,
			Arc::new(StringArray::from(names)) as ArrayRef,
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
			table_object_id: Some(TableObjectId::new([0x56; 16])),
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
fn writer_disabled_emits_single_batch_without_directory() {
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
			slab_id: SlabId::new(56),
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
	assert_eq!(decode_batch(&encoded.frame).unwrap().num_rows(), 10);
}

#[test]
fn large_multirow_record_emits_multiple_batches_and_directory() {
	let batch = batch(10);
	let descriptor = descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&source_rows(10),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(true),
	)
	.unwrap();
	let reader = StreamReader::try_new(Cursor::new(&encoded.frame), None).unwrap();
	let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(56),
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
	let page_directory = parsed.page_directory.expect("row-group directory");

	assert_eq!(batches.len(), 3);
	assert_eq!(batches[0].num_rows(), 4);
	assert_eq!(batches[1].num_rows(), 4);
	assert_eq!(batches[2].num_rows(), 2);
	assert_eq!(page_directory.row_groups.len(), 3);
	assert_eq!(page_directory.row_count.get(), 10);
	assert_eq!(page_directory.row_groups[2].row_count.get(), 2);
	assert!(!page_directory.buffer_pages.is_empty());
	assert!(!page_directory.row_group_stats.is_empty());
	assert_eq!(decode_batch(&encoded.frame).unwrap().num_rows(), 10);
}

#[test]
fn scan_profile_emits_one_entry_directory_at_exact_target() {
	let row_count = ROW_GROUP_SCAN_TARGET_ROWS as usize;
	let batch = batch(row_count);
	let descriptor = descriptor(batch.schema().as_ref());
	let row_group_config = RowGroupWriterConfig::scan().with_enabled(true);
	let encoded = encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&source_rows(row_count),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config,
	)
	.unwrap();
	let reader = StreamReader::try_new(Cursor::new(&encoded.frame), None).unwrap();
	let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(56),
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
	let page_directory = parsed.page_directory.expect("scan row-group directory");

	assert_eq!(batches.len(), 1);
	assert_eq!(page_directory.row_groups.len(), 1);
	assert_eq!(
		page_directory.row_groups[0].row_count.get(),
		ROW_GROUP_SCAN_TARGET_ROWS
	);
	assert_eq!(
		page_directory.row_count.get(),
		ROW_GROUP_SCAN_TARGET_ROWS as u64
	);
}

#[test]
fn scan_profile_target_plus_one_keeps_tail_as_second_directory_entry() {
	let row_count = ROW_GROUP_SCAN_TARGET_ROWS as usize + 1;
	let batch = batch(row_count);
	let descriptor = descriptor(batch.schema().as_ref());
	let row_group_config = RowGroupWriterConfig::scan().with_enabled(true);
	let encoded = encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&source_rows(row_count),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config,
	)
	.unwrap();
	let reader = StreamReader::try_new(Cursor::new(&encoded.frame), None).unwrap();
	let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(56),
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
	let page_directory = parsed.page_directory.expect("scan row-group directory");

	assert_eq!(batches.len(), 2);
	assert_eq!(batches[0].num_rows(), ROW_GROUP_SCAN_TARGET_ROWS as usize);
	assert_eq!(batches[1].num_rows(), 1);
	assert_eq!(page_directory.row_groups.len(), 2);
	assert_eq!(
		page_directory.row_groups[0].row_count.get(),
		ROW_GROUP_SCAN_TARGET_ROWS
	);
	assert_eq!(page_directory.row_groups[1].row_count.get(), 1);
	assert_eq!(page_directory.row_count.get(), row_count as u64);
}
