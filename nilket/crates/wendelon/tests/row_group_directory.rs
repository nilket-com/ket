use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::schema::{
	HybridTimestamp, Identity, Locality, LocalityPolicy, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptor, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, RowCount, RowGroupRowCount, RowIdentityPolicy, SchemaEvolutionPolicy,
	SlabId, TableObjectId,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	PlaintextByteRange, RowGroupRejectCode, RowGroupWriterConfig, TieredRecordId,
	deserialize_page_directory_value, encode_multirow_from_survivors_with_row_group_config,
	parse_record_buffer_directory, serialize_page_directory_value, validate_page_directory,
};

struct EncodedFixture {
	descriptor: wendelon::schema::TableSchemaDescriptor,
	ipc_schema: Schema,
	frame_len: u64,
	directory: wendelon::storage::RecordPageDirectory,
}

fn descriptor(schema: &Schema) -> wendelon::schema::TableSchemaDescriptor {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x56; 16]),
		1,
		"test",
		"row_group_directory",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
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

fn row_group_config() -> RowGroupWriterConfig {
	RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(4),
		RowGroupRowCount::new(4),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true)
}

fn encoded_fixture() -> EncodedFixture {
	let batch = batch(10);
	let descriptor = descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&source_rows(10),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(),
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
	EncodedFixture {
		descriptor,
		ipc_schema: parsed.ipc_schema.clone(),
		frame_len: encoded.frame.len() as u64,
		directory: parsed.page_directory.expect("row-group directory"),
	}
}

#[test]
fn page_directory_serialization_roundtrips_and_validates() {
	let fixture = encoded_fixture();
	let value = serialize_page_directory_value(&fixture.directory).unwrap();
	let decoded = deserialize_page_directory_value(&value).unwrap();

	assert_eq!(decoded, fixture.directory);
	validate_page_directory(
		&decoded,
		fixture.frame_len,
		&fixture.ipc_schema,
		fixture.descriptor.logical_schema_fingerprint,
	)
	.unwrap();
}

#[test]
fn invalid_directory_encoding_is_parse_error() {
	let err = deserialize_page_directory_value("not base64").unwrap_err();

	assert_eq!(err, RowGroupRejectCode::PageDirectoryParseError);
}

#[test]
fn duplicate_row_group_index_is_rejected() {
	let fixture = encoded_fixture();
	let mut directory = fixture.directory;
	directory.row_groups[1].row_group_index = directory.row_groups[0].row_group_index;

	let err = validate_page_directory(
		&directory,
		fixture.frame_len,
		&fixture.ipc_schema,
		fixture.descriptor.logical_schema_fingerprint,
	)
	.unwrap_err();

	assert_eq!(err, RowGroupRejectCode::PageDirectoryParseError);
}

#[test]
fn buffer_range_outside_row_group_is_rejected_as_range_overflow() {
	let fixture = encoded_fixture();
	let mut directory = fixture.directory;
	let body_end = directory.row_groups[0].body_range.end().unwrap();
	let row_group_index = directory.row_groups[0].row_group_index;
	let first_page = directory
		.buffer_pages
		.iter_mut()
		.find(|page| page.row_group_index == row_group_index)
		.expect("buffer page");
	first_page.range.start = RecordBodyByteOffset::new(body_end.saturating_add(1));

	let err = validate_page_directory(
		&directory,
		fixture.frame_len,
		&fixture.ipc_schema,
		fixture.descriptor.logical_schema_fingerprint,
	)
	.unwrap_err();

	assert_eq!(err, RowGroupRejectCode::RowGroupRangeOverflow);
}
