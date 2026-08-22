use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::crypto::TieredPlaintextRange;
use wendelon::schema::{
	HybridTimestamp, Identity, IpcColumnOrdinal, Locality, LocalityPolicy,
	MultiRowLayoutDescriptor, PrimaryKeyDescriptor, RecordBodyByteLen, RecordBodyByteOffset,
	RecordBodyCompressionPolicy, RecordLayoutPolicy, RowCount, RowGroupIndex, RowGroupRowCount,
	RowIdentityPolicy, SchemaEvolutionPolicy, SlabId, TableObjectId,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	ColumnFetchRequirement, ColumnOrdinalSet, EncodingWriterConfig, PlaintextByteRange,
	RecordBodyByteRange, RowGroupWriterConfig, TieredRecordId,
	decode_projected_row_groups_from_plaintext_ranges,
	encode_multirow_from_survivors_with_row_group_and_encoding_config,
	parse_record_buffer_directory, selected_row_group_record_body_ranges,
};

fn descriptor(schema: &Schema) -> wendelon::schema::TableSchemaDescriptor {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x57; 16]),
		1,
		"test",
		"encoding_planner",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(1024, 1024, 8 * 1024 * 1024).unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn batch(row_count: usize) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
	let ids = (0..row_count)
		.map(|row| (row % 8) as i64)
		.collect::<Vec<_>>();
	RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids)) as ArrayRef]).unwrap()
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
			partition: None,
			table_object_id: Some(TableObjectId::new([0x57; 16])),
		})
		.collect()
}

fn row_group_config() -> RowGroupWriterConfig {
	RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(1024),
		RowGroupRowCount::new(1024),
		RowCount::new(1024),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true)
}

fn encoding_config() -> EncodingWriterConfig {
	EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		..EncodingWriterConfig::default()
	}
}

fn output_requirement(column: u32) -> ColumnFetchRequirement {
	ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([column]),
		..ColumnFetchRequirement::default()
	}
}

fn contains_range(outer: RecordBodyByteRange, inner: RecordBodyByteRange) -> bool {
	outer.start.get() <= inner.start.get() && inner.end().unwrap() <= outer.end().unwrap()
}

#[test]
fn row_group_planner_fetches_encoded_span_and_materializes_projected_group() {
	let batch = batch(2048);
	let descriptor = descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source_rows(2048),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(),
		&encoding_config(),
	)
	.unwrap();
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(57),
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
	let page_directory = directory.page_directory.as_ref().unwrap();
	let encoding_directory = directory.encoding_directory.as_ref().unwrap();
	let selected = [RowGroupIndex::ZERO];
	let requirement = output_requirement(0);
	let ranges =
		selected_row_group_record_body_ranges(&directory, page_directory, &requirement, &selected)
			.unwrap();

	let encoded_buffer = encoding_directory
		.buffers
		.iter()
		.find(|encoding| {
			encoding.row_group_index == RowGroupIndex::ZERO
				&& encoding.ipc_column_ordinal == IpcColumnOrdinal::ZERO
				&& encoding.encoded_range.len < encoding.decoded_byte_len
		})
		.expect("selected column should have a compact encoded buffer");
	let page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == encoded_buffer.row_group_index
				&& page.ipc_column_ordinal == encoded_buffer.ipc_column_ordinal
				&& page.ipc_buffer_ordinal == encoded_buffer.ipc_buffer_ordinal
		})
		.unwrap();
	let encoded_range = RecordBodyByteRange {
		start: RecordBodyByteOffset::new(
			page.range.start.get() + u64::from(encoded_buffer.encoded_range.start.get()),
		),
		len: encoded_buffer.encoded_range.len,
	};
	assert!(
		ranges
			.iter()
			.copied()
			.any(|range| contains_range(range, encoded_range)),
		"selected row-group ranges should cover encoded span {encoded_range:?}, got {ranges:?}",
	);
	assert_eq!(page.range.len, encoded_buffer.encoded_range.len);
	assert!(page.range.len.get() < encoded_buffer.decoded_byte_len.get());
	let decoded_length_span = RecordBodyByteRange {
		start: page.range.start,
		len: encoded_buffer.decoded_byte_len,
	};
	assert!(
		!ranges
			.iter()
			.copied()
			.any(|range| contains_range(range, decoded_length_span)),
		"selected row-group ranges should not fetch the decoded-length buffer span"
	);

	let plaintext_ranges = ranges
		.iter()
		.map(|range| TieredPlaintextRange::new(range.start, range.len))
		.collect::<Vec<_>>();
	let extracts = ranges
		.iter()
		.map(|range| {
			let start = range.start.get() as usize;
			let end = range.end().unwrap() as usize;
			encoded.frame[start..end].to_vec()
		})
		.collect::<Vec<_>>();
	let projected = decode_projected_row_groups_from_plaintext_ranges(
		&directory,
		page_directory,
		&requirement,
		&plaintext_ranges,
		&extracts,
		&selected,
	)
	.unwrap();

	assert_eq!(projected.batch.num_rows(), 1024);
	assert_eq!(projected.batch.num_columns(), 1);
}
