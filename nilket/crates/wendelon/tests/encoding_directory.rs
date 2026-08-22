use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::sync::Arc;
use wendelon::schema::{
	HybridTimestamp, Identity, Locality, LocalityPolicy, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptor, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, RowCount, RowGroupRowCount, RowIdentityPolicy, SchemaEvolutionPolicy,
	SlabId, TableObjectId, TableSchemaDescriptor,
};
use wendelon::storage::encoding::directory::{
	deserialize_encoding_directory_value, serialize_encoding_directory_value,
	validate_encoding_directory,
};
use wendelon::storage::encoding::{
	BufferEncoding, ENCODING_DIRECTORY_FORMAT_VERSION, ENCODING_DIRECTORY_MAGIC,
	EncodedBufferByteRange, EncodingCascade, EncodingDirectory, EncodingLeafCascade,
	EncodingPageStats,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	EncodingWriterConfig, PlaintextByteRange, RowGroupWriterConfig, TieredRecordId, decode_batch,
	encode_multirow_from_survivors_with_row_group_and_encoding_config,
	parse_record_buffer_directory,
};

fn descriptor(schema: &Schema) -> wendelon::schema::TableSchemaDescriptor {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x57; 16]),
		1,
		"test",
		"encoding_directory",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(12, 12, 8 * 1024 * 1024).unwrap(),
		),
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
		.map(|row| Some(format!("region-{}", row % 4)))
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

fn variable_width_string_batch(row_count: usize) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("name", DataType::Utf8, false),
	]));
	let ids = (0..row_count).map(|row| row as i64).collect::<Vec<_>>();
	let names = (0..row_count)
		.map(|row| match row % 4 {
			0 => "us",
			1 => "eu-west",
			2 => "apac",
			_ => "us",
		})
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

fn low_cardinality_i64_batch(row_count: usize) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("bucket", DataType::Int64, false),
	]));
	let ids = (0..row_count).map(|row| row as i64).collect::<Vec<_>>();
	let buckets = (0..row_count)
		.map(|row| (row % 4) as i64)
		.collect::<Vec<_>>();
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(ids)) as ArrayRef,
			Arc::new(Int64Array::from(buckets)) as ArrayRef,
		],
	)
	.unwrap()
}

fn source_rows(row_count: usize) -> Vec<MultiRowSourceRow> {
	(0..row_count)
		.map(|row| MultiRowSourceRow {
			source_row_index: row,
			key_identity: {
				let mut bytes = [0_u8; 32];
				bytes[..8].copy_from_slice(&(row as u64).to_le_bytes());
				Identity::new(bytes)
			},
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
		RowGroupRowCount::new(12),
		RowGroupRowCount::new(12),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true)
}

fn large_row_group_config() -> RowGroupWriterConfig {
	RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(1024),
		RowGroupRowCount::new(1024),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true)
}

fn multi_page_row_group_config() -> RowGroupWriterConfig {
	RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(2048),
		RowGroupRowCount::new(2048),
		RowCount::new(8),
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

fn parse_encoded_record(
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	source_rows: &[MultiRowSourceRow],
	slab_id: u64,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> wendelon::storage::RecordBufferDirectory {
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		batch,
		source_rows,
		descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		row_group_config,
		encoding_config,
	)
	.unwrap();
	parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(slab_id),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		descriptor,
	)
	.unwrap()
}

fn page_stats_fixture(
	slab_id: u64,
) -> (
	TableSchemaDescriptor,
	wendelon::storage::RecordBufferDirectory,
) {
	let batch = low_cardinality_i64_batch(4096);
	let descriptor = large_descriptor(batch.schema().as_ref());
	let rows = source_rows(4096);
	let parsed = parse_encoded_record(
		&batch,
		&descriptor,
		&rows,
		slab_id,
		&multi_page_row_group_config(),
		&encoding_config(),
	);
	(descriptor, parsed)
}

fn page_stats_value_buffer_mut(directory: &mut EncodingDirectory) -> &mut BufferEncoding {
	directory
		.buffers
		.iter_mut()
		.find(|encoding| !encoding.page_stats.is_empty())
		.expect("value buffer with page stats")
}

fn large_descriptor(schema: &Schema) -> wendelon::schema::TableSchemaDescriptor {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x57; 16]),
		1,
		"test",
		"encoding_directory_large",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(1024, 4096, 8 * 1024 * 1024).unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

#[test]
fn encoding_writer_emits_page_stats_for_fixed_width_predicate_value_buffer() {
	let batch = low_cardinality_i64_batch(4096);
	let descriptor = large_descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source_rows(4096),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&multi_page_row_group_config(),
		&encoding_config(),
	)
	.unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(62),
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
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let encoding_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory");
	validate_encoding_directory(
		encoding_directory,
		page_directory,
		&parsed.ipc_schema,
		descriptor.logical_schema_fingerprint,
	)
	.unwrap();
	let value_encoding = encoding_directory
		.buffers
		.iter()
		.find(|encoding| encoding.ipc_column_ordinal.get() == 1 && !encoding.page_stats.is_empty())
		.expect("fixed-width encoded value buffer with page stats");
	assert_eq!(
		value_encoding.page_stats.len(),
		value_encoding.page_end_offsets.len()
	);
	assert!(value_encoding.page_stats.len() > 1);
	assert!(
		value_encoding
			.page_stats
			.iter()
			.all(|stat| stat.null_count.get() == 0 && stat.min.is_some() && stat.max.is_some())
	);
}

#[test]
fn encoding_directory_roundtrips_validates_and_decode_batch_materializes() {
	let batch = batch(24);
	let descriptor = descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source_rows(24),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(),
		&encoding_config(),
	)
	.unwrap();
	let decoded = decode_batch(&encoded.frame).unwrap();
	assert_eq!(decoded.num_rows(), 24);

	let parsed = parse_record_buffer_directory(
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
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let encoding_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory");
	assert!(!encoding_directory.cascades.is_empty());
	assert!(!encoding_directory.buffers.is_empty());

	let value = serialize_encoding_directory_value(encoding_directory).unwrap();
	let roundtripped = deserialize_encoding_directory_value(&value).unwrap();
	assert_eq!(&roundtripped, encoding_directory);
	validate_encoding_directory(
		&roundtripped,
		page_directory,
		&parsed.ipc_schema,
		descriptor.logical_schema_fingerprint,
	)
	.unwrap();
}

#[test]
fn encoding_directory_serializes_fsst_row_safe_cascade_with_bumped_version() {
	let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
	let descriptor = descriptor(&schema);
	let directory = EncodingDirectory {
		record_id: TieredRecordId {
			slab_id: SlabId::new(67),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		row_count: RowCount::new(0),
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		target_simd_page_values: wendelon::schema::SimdPageValueCount::new(1024),
		cascades: vec![EncodingCascade::FsstRowSafe {
			symbol_table_byte_len: RecordBodyByteLen::ZERO,
		}],
		buffers: Vec::new(),
	};

	let value = serialize_encoding_directory_value(&directory).unwrap();
	let bytes = BASE64.decode(&value).unwrap();
	assert_eq!(
		u32::from_be_bytes(
			bytes[ENCODING_DIRECTORY_MAGIC.len()..ENCODING_DIRECTORY_MAGIC.len() + 4]
				.try_into()
				.unwrap()
		),
		ENCODING_DIRECTORY_FORMAT_VERSION
	);
	assert_eq!(
		deserialize_encoding_directory_value(&value).unwrap(),
		directory
	);
}

#[test]
fn encoding_directory_rejects_legacy_format_version() {
	let batch = low_cardinality_i64_batch(4096);
	let descriptor = large_descriptor(batch.schema().as_ref());
	let rows = source_rows(4096);
	let parsed = parse_encoded_record(
		&batch,
		&descriptor,
		&rows,
		64,
		&multi_page_row_group_config(),
		&encoding_config(),
	);
	let encoding_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory");
	let value = serialize_encoding_directory_value(encoding_directory).unwrap();
	let mut bytes = BASE64.decode(value).unwrap();
	assert_eq!(
		&bytes[..ENCODING_DIRECTORY_MAGIC.len()],
		ENCODING_DIRECTORY_MAGIC
	);
	bytes[ENCODING_DIRECTORY_MAGIC.len()..ENCODING_DIRECTORY_MAGIC.len() + 4]
		.copy_from_slice(&1_u32.to_be_bytes());
	let legacy = BASE64.encode(bytes);
	assert_eq!(
		deserialize_encoding_directory_value(&legacy).unwrap_err(),
		wendelon::storage::encoding::EncodingRejectCode::EncodingDirectoryParseError
	);
}

#[test]
fn encoding_writer_emits_page_stats_deterministically_for_same_input() {
	let batch = low_cardinality_i64_batch(4096);
	let descriptor = large_descriptor(batch.schema().as_ref());
	let rows = source_rows(4096);
	let first = parse_encoded_record(
		&batch,
		&descriptor,
		&rows,
		65,
		&multi_page_row_group_config(),
		&encoding_config(),
	);
	let second = parse_encoded_record(
		&batch,
		&descriptor,
		&rows,
		65,
		&multi_page_row_group_config(),
		&encoding_config(),
	);
	let first = serialize_encoding_directory_value(
		first
			.encoding_directory
			.as_ref()
			.expect("first Encoding directory"),
	)
	.unwrap();
	let second = serialize_encoding_directory_value(
		second
			.encoding_directory
			.as_ref()
			.expect("second Encoding directory"),
	)
	.unwrap();
	assert_eq!(first, second);
}

#[test]
fn encoding_writer_compacts_record_body_and_dense_materialization_survives() {
	let batch = batch(4096);
	let descriptor = large_descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source_rows(4096),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&large_row_group_config(),
		&encoding_config(),
	)
	.unwrap();
	let row_group_only = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source_rows(4096),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&large_row_group_config(),
		&EncodingWriterConfig::default(),
	)
	.unwrap();

	assert!(
		encoded.frame.len() < row_group_only.frame.len(),
		"Encoding should physically shrink the stored IPC body: encoded={} row_group_only={}",
		encoded.frame.len(),
		row_group_only.frame.len()
	);
	assert_eq!(decode_batch(&encoded.frame).unwrap().num_rows(), 4096);

	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(60),
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
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let encoding_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory");
	let encoding = encoding_directory
		.buffers
		.iter()
		.find(|encoding| encoding.encoded_range.len.get() < encoding.decoded_byte_len.get())
		.expect("at least one compacted buffer");
	let page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == encoding.row_group_index
				&& page.ipc_column_ordinal == encoding.ipc_column_ordinal
				&& page.ipc_buffer_ordinal == encoding.ipc_buffer_ordinal
		})
		.expect("compacted buffer page");
	assert_eq!(page.range.len.get(), encoding.encoded_range.len.get());
	assert!(page.range.len.get() < encoding.decoded_byte_len.get());
}

#[test]
fn encoding_writer_uses_value_dictionary_for_one_page_low_cardinality_utf8_data_buffer() {
	let batch = variable_width_string_batch(2048);
	let descriptor = large_descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source_rows(2048),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&large_row_group_config(),
		&encoding_config(),
	)
	.unwrap();
	let decoded = decode_batch(&encoded.frame).unwrap();
	assert_eq!(decoded.num_rows(), 2048);

	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(61),
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
	let encoding_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory");
	assert!(
		encoding_directory
			.cascades
			.iter()
			.any(|cascade| matches!(cascade, EncodingCascade::Dictionary { .. })),
		"one-page low-cardinality UTF-8 value buffer should use Dictionary"
	);
	assert!(
		encoding_directory
			.buffers
			.iter()
			.filter(|encoding| encoding.ipc_column_ordinal.get() == 1)
			.all(|encoding| encoding.page_stats.is_empty()),
		"UTF-8 value buffers must not carry ADR 063 V1 page stats"
	);
}

#[test]
fn encoding_writer_disabled_keeps_record_on_row_group_only_path() {
	let batch = batch(24);
	let descriptor = descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source_rows(24),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(),
		&EncodingWriterConfig::default(),
	)
	.unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(58),
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
	assert!(parsed.page_directory.is_some());
	assert!(parsed.encoding_directory.is_none());
}

#[test]
fn encoding_directory_rejects_page_offsets_on_non_page_cascade() {
	let batch = batch(24);
	let descriptor = descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source_rows(24),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(),
		&encoding_config(),
	)
	.unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(59),
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
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let first_page = page_directory
		.buffer_pages
		.iter()
		.find(|page| page.range.len.get() > 0)
		.expect("non-empty buffer page");
	let bad_directory = EncodingDirectory {
		record_id: TieredRecordId {
			slab_id: SlabId::new(59),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		row_count: page_directory.row_count,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		target_simd_page_values: encoding_config().target_simd_page_values,
		cascades: vec![EncodingCascade::Plain],
		buffers: vec![BufferEncoding {
			row_group_index: first_page.row_group_index,
			ipc_column_ordinal: first_page.ipc_column_ordinal,
			ipc_buffer_ordinal: first_page.ipc_buffer_ordinal,
			cascade_index: wendelon::schema::EncodingCascadeIndex::ZERO,
			decoded_byte_len: first_page.range.len,
			decoded_value_count: wendelon::schema::BufferValueCount::new(4),
			encoded_range: EncodedBufferByteRange {
				start: wendelon::schema::EncodingPageByteOffset::ZERO,
				len: first_page.range.len,
			},
			page_value_count: encoding_config().target_simd_page_values,
			page_end_offsets: vec![wendelon::schema::EncodingPageByteOffset::new(
				u32::try_from(first_page.range.len.get()).unwrap(),
			)],
			page_stats: Vec::new(),
		}],
	};
	let err = validate_encoding_directory(
		&bad_directory,
		page_directory,
		&parsed.ipc_schema,
		descriptor.logical_schema_fingerprint,
	)
	.unwrap_err();
	assert_eq!(
		err,
		wendelon::storage::encoding::EncodingRejectCode::EncodingDirectoryParseError
	);
}

#[test]
fn encoding_directory_rejects_page_stats_count_mismatch() {
	let (descriptor, parsed) = page_stats_fixture(66);
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let mut bad_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory")
		.clone();
	page_stats_value_buffer_mut(&mut bad_directory)
		.page_stats
		.pop();
	let err = validate_encoding_directory(
		&bad_directory,
		page_directory,
		&parsed.ipc_schema,
		descriptor.logical_schema_fingerprint,
	)
	.unwrap_err();
	assert_eq!(
		err,
		wendelon::storage::encoding::EncodingRejectCode::EncodingDirectoryParseError
	);
}

#[test]
fn encoding_directory_rejects_page_stat_type_mismatch() {
	let (descriptor, parsed) = page_stats_fixture(67);
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let mut bad_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory")
		.clone();
	let stat = page_stats_value_buffer_mut(&mut bad_directory)
		.page_stats
		.first_mut()
		.expect("page stat");
	stat.min.as_mut().expect("min").arrow_type_tag =
		wendelon::storage::stats::ARROW_STATS_TAG_BOOLEAN;
	let err = validate_encoding_directory(
		&bad_directory,
		page_directory,
		&parsed.ipc_schema,
		descriptor.logical_schema_fingerprint,
	)
	.unwrap_err();
	assert_eq!(
		err,
		wendelon::storage::encoding::EncodingRejectCode::EncodingDirectoryParseError
	);
}

#[test]
fn encoding_directory_rejects_page_null_count_overflow() {
	let (descriptor, parsed) = page_stats_fixture(68);
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let mut bad_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory")
		.clone();
	let buffer = page_stats_value_buffer_mut(&mut bad_directory);
	buffer.page_stats.first_mut().expect("page stat").null_count =
		wendelon::schema::PageNullCount::new(buffer.page_value_count.get() + 1);
	let err = validate_encoding_directory(
		&bad_directory,
		page_directory,
		&parsed.ipc_schema,
		descriptor.logical_schema_fingerprint,
	)
	.unwrap_err();
	assert_eq!(
		err,
		wendelon::storage::encoding::EncodingRejectCode::EncodingDirectoryParseError
	);
}

#[test]
fn encoding_directory_rejects_page_stats_outside_v1_cascade_intersection() {
	let batch = batch(24);
	let descriptor = descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source_rows(24),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(),
		&encoding_config(),
	)
	.unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(63),
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
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let first_page = page_directory
		.buffer_pages
		.iter()
		.find(|page| page.ipc_column_ordinal.get() == 0 && page.range.len.get() > 0)
		.expect("non-empty fixed-width page");
	let bad_directory = EncodingDirectory {
		record_id: TieredRecordId {
			slab_id: SlabId::new(63),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		row_count: page_directory.row_count,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		target_simd_page_values: encoding_config().target_simd_page_values,
		cascades: vec![EncodingCascade::Rle {
			run_count: wendelon::schema::RunCount::new(1),
			inner: Box::new(EncodingLeafCascade::Plain),
		}],
		buffers: vec![BufferEncoding {
			row_group_index: first_page.row_group_index,
			ipc_column_ordinal: first_page.ipc_column_ordinal,
			ipc_buffer_ordinal: first_page.ipc_buffer_ordinal,
			cascade_index: wendelon::schema::EncodingCascadeIndex::ZERO,
			decoded_byte_len: first_page.range.len,
			decoded_value_count: wendelon::schema::BufferValueCount::new(4),
			encoded_range: EncodedBufferByteRange {
				start: wendelon::schema::EncodingPageByteOffset::ZERO,
				len: first_page.range.len,
			},
			page_value_count: encoding_config().target_simd_page_values,
			page_end_offsets: vec![wendelon::schema::EncodingPageByteOffset::new(
				u32::try_from(first_page.range.len.get()).unwrap(),
			)],
			page_stats: vec![EncodingPageStats {
				null_count: wendelon::schema::PageNullCount::ZERO,
				min: None,
				max: None,
			}],
		}],
	};
	let err = validate_encoding_directory(
		&bad_directory,
		page_directory,
		&parsed.ipc_schema,
		descriptor.logical_schema_fingerprint,
	)
	.unwrap_err();
	assert_eq!(
		err,
		wendelon::storage::encoding::EncodingRejectCode::EncodingDirectoryParseError
	);
}

#[test]
fn encoding_directory_rejects_page_stats_on_delta_cascade() {
	let (descriptor, parsed) = page_stats_fixture(69);
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let mut bad_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory")
		.clone();
	bad_directory.cascades = vec![EncodingCascade::Delta {
		base: wendelon::schema::BaseValue::ZERO,
		inner: Box::new(EncodingLeafCascade::Plain),
	}];
	page_stats_value_buffer_mut(&mut bad_directory).cascade_index =
		wendelon::schema::EncodingCascadeIndex::ZERO;
	let err = validate_encoding_directory(
		&bad_directory,
		page_directory,
		&parsed.ipc_schema,
		descriptor.logical_schema_fingerprint,
	)
	.unwrap_err();
	assert_eq!(
		err,
		wendelon::storage::encoding::EncodingRejectCode::EncodingDirectoryParseError
	);
}

#[test]
fn encoding_directory_rejects_page_stats_on_variable_width_buffer() {
	let batch = variable_width_string_batch(4096);
	let descriptor = large_descriptor(batch.schema().as_ref());
	let rows = source_rows(4096);
	let parsed = parse_encoded_record(
		&batch,
		&descriptor,
		&rows,
		70,
		&multi_page_row_group_config(),
		&encoding_config(),
	);
	let page_directory = parsed.page_directory.as_ref().expect("row-group directory");
	let mut bad_directory = parsed
		.encoding_directory
		.as_ref()
		.expect("Encoding directory")
		.clone();
	let mut injected = bad_directory
		.buffers
		.iter()
		.find(|encoding| !encoding.page_end_offsets.is_empty())
		.cloned()
		.expect("encoded fixed-width value buffer");
	injected.ipc_column_ordinal = wendelon::schema::IpcColumnOrdinal::new(1);
	if injected.page_end_offsets.is_empty() {
		injected
			.page_end_offsets
			.push(wendelon::schema::EncodingPageByteOffset::new(
				injected.encoded_range.len.get() as u32,
			));
	}
	injected.page_stats = vec![
		EncodingPageStats {
			null_count: wendelon::schema::PageNullCount::ZERO,
			min: None,
			max: None,
		};
		injected.page_end_offsets.len()
	];
	bad_directory.buffers.push(injected);
	let err = validate_encoding_directory(
		&bad_directory,
		page_directory,
		&parsed.ipc_schema,
		descriptor.logical_schema_fingerprint,
	)
	.unwrap_err();
	assert!(matches!(
		err,
		wendelon::storage::encoding::EncodingRejectCode::EncodingDirectoryParseError
			| wendelon::storage::encoding::EncodingRejectCode::BufferEncodingMismatch
	));
}
