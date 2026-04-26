use crate::schema::table::{
	COLUMN_STATS_CSB_AEAD_TAG_BYTES, COLUMN_STATS_HLL_MAX_PRECISION,
	COLUMN_STATS_HLL_MIN_PRECISION, ColumnBloomSpecV1, ColumnHllSpecV1, ColumnSmallSetSpecV1,
	TableObjectId, TableSchemaDescriptorV1,
	max_column_stats_block_ciphertext_bytes_for_slab_payload,
};
use arrow::array::{
	Array, BinaryArray, BooleanArray, LargeBinaryArray, LargeStringArray, PrimitiveArray,
	StringArray,
};
use arrow::datatypes::{
	ArrowPrimitiveType, DataType, Date32Type, Date64Type, Field, Int8Type, Int16Type, Int32Type,
	Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
	TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeSet;
use std::io::{Error, ErrorKind};
use std::time::Instant;

pub const COLUMN_STATS_BLOCK_MAGIC: [u8; 8] = *b"WNDSTAT1";
pub const COLUMN_STATS_BLOCK_FORMAT_VERSION: u32 = 1;
pub const COLUMN_STATS_BLOCK_HEADER_BYTES: usize = 84;
pub const COLUMN_STATS_COLUMN_HEADER_BYTES: usize = 9;
pub const COLUMN_STATS_DEFAULT_PREFIX_BYTES: usize = 64;
const COLUMN_STATS_BLOOM_HASHES: u8 = 7;
const COLUMN_STATS_BLOOM_SIPHASH_K0: u64 = 0x0706_0504_0302_0100;
const COLUMN_STATS_BLOOM_SIPHASH_K1: u64 = 0x0F0E_0D0C_0B0A_0908;
const COLUMN_STATS_BLOOM_SIPHASH_ALT_K0: u64 = 0x1716_1514_1312_1110;
const COLUMN_STATS_BLOOM_SIPHASH_ALT_K1: u64 = 0x1F1E_1D1C_1B1A_1918;
const COLUMN_STATS_HLL_SIPHASH_K0: u64 = 0x2726_2524_2322_2120;
const COLUMN_STATS_HLL_SIPHASH_K1: u64 = 0x2F2E_2D2C_2B2A_2928;
const _: [(); COLUMN_STATS_BLOCK_HEADER_BYTES] = [(); 8 + 4 + 16 + 8 + 32 + 8 + 4 + 4];
const _: [(); COLUMN_STATS_COLUMN_HEADER_BYTES] = [(); 2 + 1 + 1 + 1 + 4];

pub const COLUMN_STAT_HAS_MIN: u16 = 1 << 0;
pub const COLUMN_STAT_HAS_MAX: u16 = 1 << 1;
pub const COLUMN_STAT_HAS_NULL_COUNT: u16 = 1 << 2;
pub const COLUMN_STAT_HAS_NON_NULL_COUNT: u16 = 1 << 3;
pub const COLUMN_STAT_HAS_BLOOM: u16 = 1 << 4;
pub const COLUMN_STAT_HAS_HLL: u16 = 1 << 5;
pub const COLUMN_STAT_HAS_SMALL_SET: u16 = 1 << 6;
pub const COLUMN_STAT_MIN_TRUNCATED: u16 = 1 << 7;
pub const COLUMN_STAT_MAX_TRUNCATED: u16 = 1 << 8;
pub const COLUMN_STAT_RESERVED_MASK: u16 = 0b1111_1110_0000_0000;

pub const ARROW_STATS_TAG_INT8: u8 = 0x01;
pub const ARROW_STATS_TAG_INT16: u8 = 0x02;
pub const ARROW_STATS_TAG_INT32: u8 = 0x03;
pub const ARROW_STATS_TAG_INT64: u8 = 0x04;
pub const ARROW_STATS_TAG_UINT8: u8 = 0x05;
pub const ARROW_STATS_TAG_UINT16: u8 = 0x06;
pub const ARROW_STATS_TAG_UINT32: u8 = 0x07;
pub const ARROW_STATS_TAG_UINT64: u8 = 0x08;
pub const ARROW_STATS_TAG_OPAQUE_UNSUPPORTED: u8 = 0x0E;
pub const ARROW_STATS_TAG_UTF8: u8 = 0x10;
pub const ARROW_STATS_TAG_LARGE_UTF8: u8 = 0x11;
pub const ARROW_STATS_TAG_BINARY: u8 = 0x12;
pub const ARROW_STATS_TAG_LARGE_BINARY: u8 = 0x13;
pub const ARROW_STATS_TAG_TIMESTAMP_MILLISECOND: u8 = 0x20;
pub const ARROW_STATS_TAG_TIMESTAMP_MICROSECOND: u8 = 0x21;
pub const ARROW_STATS_TAG_TIMESTAMP_NANOSECOND: u8 = 0x22;
pub const ARROW_STATS_TAG_DATE32: u8 = 0x30;
pub const ARROW_STATS_TAG_DATE64: u8 = 0x31;
pub const ARROW_STATS_TAG_BOOLEAN: u8 = 0x40;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStatsBlockHeader {
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub schema_fingerprint: [u8; 32],
	pub row_count: u64,
	pub column_block_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnStatsDescriptorIdentity {
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub schema_fingerprint: [u8; 32],
}

impl ColumnStatsDescriptorIdentity {
	pub fn from_descriptor(descriptor: &TableSchemaDescriptorV1) -> Self {
		Self {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			schema_fingerprint: descriptor.schema_fingerprint,
		}
	}

	pub fn from_header(header: &ColumnStatsBlockHeader) -> Self {
		Self {
			table_object_id: header.table_object_id,
			schema_generation: header.schema_generation,
			schema_fingerprint: header.schema_fingerprint,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStatsBlockEntry {
	pub column_ordinal: u16,
	pub arrow_type_tag: u8,
	pub stat_flags: u16,
	pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedColumnStatsBlock {
	pub header: ColumnStatsBlockHeader,
	pub columns: Vec<ColumnStatsBlockEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStatsBloomPayload {
	pub bit_count: u32,
	pub bit_array: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStatsHllPayload {
	pub precision: u8,
	pub registers: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedColumnStatsPayload {
	pub min: Option<Vec<u8>>,
	pub max: Option<Vec<u8>>,
	pub null_count: Option<u64>,
	pub non_null_count: Option<u64>,
	pub bloom_filter: Option<ColumnStatsBloomPayload>,
	pub hll_distinct: Option<ColumnStatsHllPayload>,
	pub small_set: Option<Vec<Vec<u8>>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DistinctCount {
	Exact(u64),
	Approx { estimate: u64, std_err: f64 },
	Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnStatsPrune {
	MayMatch,
	CannotMatch,
}

pub fn encode_column_stats_block(
	header: &ColumnStatsBlockHeader,
	columns: &[ColumnStatsBlockEntry],
) -> std::io::Result<Vec<u8>> {
	if header.column_block_count != columns.len() as u32 {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"CSB header column_block_count does not match entries",
		));
	}
	let mut out = Vec::with_capacity(
		COLUMN_STATS_BLOCK_HEADER_BYTES + columns.len() * COLUMN_STATS_COLUMN_HEADER_BYTES,
	);
	out.extend_from_slice(&COLUMN_STATS_BLOCK_MAGIC);
	out.extend_from_slice(&COLUMN_STATS_BLOCK_FORMAT_VERSION.to_be_bytes());
	out.extend_from_slice(header.table_object_id.as_bytes());
	out.extend_from_slice(&header.schema_generation.to_be_bytes());
	out.extend_from_slice(&header.schema_fingerprint);
	out.extend_from_slice(&header.row_count.to_be_bytes());
	out.extend_from_slice(&header.column_block_count.to_be_bytes());
	out.extend_from_slice(&0_u32.to_be_bytes());
	for column in columns {
		validate_column_stats_entry(column)?;
		let payload_len = u32::try_from(column.payload.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"CSB column payload exceeds u32::MAX bytes",
			)
		})?;
		out.extend_from_slice(&column.column_ordinal.to_be_bytes());
		out.push(column.arrow_type_tag);
		out.push((column.stat_flags >> 8) as u8);
		out.push(column.stat_flags as u8);
		out.extend_from_slice(&payload_len.to_be_bytes());
		out.extend_from_slice(&column.payload);
	}
	Ok(out)
}

pub fn parse_column_stats_block(bytes: &[u8]) -> std::io::Result<ParsedColumnStatsBlock> {
	if bytes.len() < COLUMN_STATS_BLOCK_HEADER_BYTES {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"CSB shorter than fixed header",
		));
	}
	if bytes[..8] != COLUMN_STATS_BLOCK_MAGIC {
		return Err(Error::new(ErrorKind::InvalidData, "CSB magic mismatch"));
	}
	let csb_format_version = u32::from_be_bytes(bytes[8..12].try_into().expect("slice len"));
	if csb_format_version != COLUMN_STATS_BLOCK_FORMAT_VERSION {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("unsupported CSB format version {csb_format_version}"),
		));
	}
	let mut table_object_id = [0_u8; 16];
	table_object_id.copy_from_slice(&bytes[12..28]);
	let schema_generation = u64::from_be_bytes(bytes[28..36].try_into().expect("slice len"));
	let mut schema_fingerprint = [0_u8; 32];
	schema_fingerprint.copy_from_slice(&bytes[36..68]);
	let row_count = u64::from_be_bytes(bytes[68..76].try_into().expect("slice len"));
	let column_block_count = u32::from_be_bytes(bytes[76..80].try_into().expect("slice len"));
	if bytes[80..84] != [0, 0, 0, 0] {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"CSB reserved header bytes must be zero",
		));
	}
	let column_block_count = usize::try_from(column_block_count).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"CSB column_block_count does not fit in usize",
		)
	})?;
	let remaining = bytes.len() - COLUMN_STATS_BLOCK_HEADER_BYTES;
	let minimum_column_bytes = column_block_count
		.checked_mul(COLUMN_STATS_COLUMN_HEADER_BYTES)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "CSB column_block_count overflow"))?;
	if minimum_column_bytes > remaining {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"CSB column_block_count exceeds remaining bytes",
		));
	}
	let mut cursor = COLUMN_STATS_BLOCK_HEADER_BYTES;
	let mut columns = Vec::with_capacity(column_block_count);
	for _ in 0..column_block_count {
		let entry_header_end = cursor
			.checked_add(COLUMN_STATS_COLUMN_HEADER_BYTES)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "CSB column cursor overflow"))?;
		let entry_header = bytes.get(cursor..entry_header_end).ok_or_else(|| {
			Error::new(
				ErrorKind::UnexpectedEof,
				"CSB truncated while reading column header",
			)
		})?;
		cursor = entry_header_end;
		let column_ordinal = u16::from_be_bytes(entry_header[0..2].try_into().expect("slice len"));
		let arrow_type_tag = entry_header[2];
		let stat_flags = u16::from_be_bytes([entry_header[3], entry_header[4]]);
		let block_length = u32::from_be_bytes(entry_header[5..9].try_into().expect("slice len"));
		let payload_len = usize::try_from(block_length).map_err(|_| {
			Error::new(ErrorKind::InvalidData, "CSB column payload length overflow")
		})?;
		let payload_end = cursor
			.checked_add(payload_len)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "CSB column payload overflow"))?;
		let payload = bytes
			.get(cursor..payload_end)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::UnexpectedEof,
					"CSB truncated while reading column payload",
				)
			})?
			.to_vec();
		cursor = payload_end;
		let entry = ColumnStatsBlockEntry {
			column_ordinal,
			arrow_type_tag,
			stat_flags,
			payload,
		};
		validate_column_stats_entry(&entry)?;
		columns.push(entry);
	}
	if cursor != bytes.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"CSB has trailing bytes after column blocks",
		));
	}
	Ok(ParsedColumnStatsBlock {
		header: ColumnStatsBlockHeader {
			table_object_id: TableObjectId::new(table_object_id),
			schema_generation,
			schema_fingerprint,
			row_count,
			column_block_count: u32::try_from(column_block_count).expect("parsed from u32"),
		},
		columns,
	})
}

pub fn validate_column_stats_compaction_inputs(
	headers: &[ColumnStatsBlockHeader],
) -> std::io::Result<Option<ColumnStatsDescriptorIdentity>> {
	let Some((first, rest)) = headers.split_first() else {
		return Ok(None);
	};
	let expected = ColumnStatsDescriptorIdentity::from_header(first);
	for header in rest {
		let actual = ColumnStatsDescriptorIdentity::from_header(header);
		if actual != expected {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"CSB compaction descriptor identity mismatch: expected {expected:?}, got {actual:?}"
				),
			));
		}
	}
	Ok(Some(expected))
}

pub fn validate_column_stats_entry(entry: &ColumnStatsBlockEntry) -> std::io::Result<()> {
	if entry.stat_flags & COLUMN_STAT_RESERVED_MASK != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"CSB column stat_flags reserved bits must be zero",
		));
	}
	if !is_valid_v1_arrow_stats_tag(entry.arrow_type_tag) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"unsupported CSB arrow_type_tag 0x{:02X}",
				entry.arrow_type_tag
			),
		));
	}
	if entry.stat_flags & COLUMN_STAT_MIN_TRUNCATED != 0
		&& entry.stat_flags & COLUMN_STAT_HAS_MIN == 0
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"CSB min_truncated requires has_min",
		));
	}
	if entry.stat_flags & COLUMN_STAT_MAX_TRUNCATED != 0
		&& entry.stat_flags & COLUMN_STAT_HAS_MAX == 0
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"CSB max_truncated requires has_max",
		));
	}
	if entry.stat_flags & (COLUMN_STAT_MIN_TRUNCATED | COLUMN_STAT_MAX_TRUNCATED) != 0
		&& !stats_tag_uses_prefix_truncation(entry.arrow_type_tag)
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"CSB truncation flags are only valid for string/binary stats",
		));
	}
	let count_flags = COLUMN_STAT_HAS_NULL_COUNT | COLUMN_STAT_HAS_NON_NULL_COUNT;
	if entry.stat_flags & count_flags != count_flags {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"CSB column entry must carry always-on null and non-null counts",
		));
	}
	if entry.arrow_type_tag == ARROW_STATS_TAG_OPAQUE_UNSUPPORTED && entry.stat_flags != count_flags
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"CSB opaque-tagged column entry must carry exactly null and non-null counts",
		));
	}
	Ok(())
}

pub fn arrow_stats_type_tag(data_type: &DataType) -> Option<u8> {
	match data_type {
		DataType::Int8 => Some(ARROW_STATS_TAG_INT8),
		DataType::Int16 => Some(ARROW_STATS_TAG_INT16),
		DataType::Int32 => Some(ARROW_STATS_TAG_INT32),
		DataType::Int64 => Some(ARROW_STATS_TAG_INT64),
		DataType::UInt8 => Some(ARROW_STATS_TAG_UINT8),
		DataType::UInt16 => Some(ARROW_STATS_TAG_UINT16),
		DataType::UInt32 => Some(ARROW_STATS_TAG_UINT32),
		DataType::UInt64 => Some(ARROW_STATS_TAG_UINT64),
		DataType::Utf8 => Some(ARROW_STATS_TAG_UTF8),
		DataType::LargeUtf8 => Some(ARROW_STATS_TAG_LARGE_UTF8),
		DataType::Binary => Some(ARROW_STATS_TAG_BINARY),
		DataType::LargeBinary => Some(ARROW_STATS_TAG_LARGE_BINARY),
		DataType::Timestamp(TimeUnit::Millisecond, _) => {
			Some(ARROW_STATS_TAG_TIMESTAMP_MILLISECOND)
		}
		DataType::Timestamp(TimeUnit::Microsecond, _) => {
			Some(ARROW_STATS_TAG_TIMESTAMP_MICROSECOND)
		}
		DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(ARROW_STATS_TAG_TIMESTAMP_NANOSECOND),
		DataType::Date32 => Some(ARROW_STATS_TAG_DATE32),
		DataType::Date64 => Some(ARROW_STATS_TAG_DATE64),
		DataType::Boolean => Some(ARROW_STATS_TAG_BOOLEAN),
		_ => None,
	}
}

pub fn is_valid_v1_arrow_stats_tag(tag: u8) -> bool {
	matches!(
		tag,
		ARROW_STATS_TAG_INT8
			| ARROW_STATS_TAG_INT16
			| ARROW_STATS_TAG_INT32
			| ARROW_STATS_TAG_INT64
			| ARROW_STATS_TAG_UINT8
			| ARROW_STATS_TAG_UINT16
			| ARROW_STATS_TAG_UINT32
			| ARROW_STATS_TAG_UINT64
			| ARROW_STATS_TAG_OPAQUE_UNSUPPORTED
			| ARROW_STATS_TAG_UTF8
			| ARROW_STATS_TAG_LARGE_UTF8
			| ARROW_STATS_TAG_BINARY
			| ARROW_STATS_TAG_LARGE_BINARY
			| ARROW_STATS_TAG_TIMESTAMP_MILLISECOND
			| ARROW_STATS_TAG_TIMESTAMP_MICROSECOND
			| ARROW_STATS_TAG_TIMESTAMP_NANOSECOND
			| ARROW_STATS_TAG_DATE32
			| ARROW_STATS_TAG_DATE64
			| ARROW_STATS_TAG_BOOLEAN
	)
}

pub fn column_stats_entry_matches_data_type(
	entry: &ColumnStatsBlockEntry,
	data_type: &DataType,
) -> bool {
	entry.arrow_type_tag != ARROW_STATS_TAG_OPAQUE_UNSUPPORTED
		&& arrow_stats_type_tag(data_type) == Some(entry.arrow_type_tag)
}

fn stats_tag_uses_prefix_truncation(tag: u8) -> bool {
	matches!(
		tag,
		ARROW_STATS_TAG_UTF8
			| ARROW_STATS_TAG_LARGE_UTF8
			| ARROW_STATS_TAG_BINARY
			| ARROW_STATS_TAG_LARGE_BINARY
	)
}

pub fn encode_signed_stat_value(value: i64, byte_width: usize) -> std::io::Result<Vec<u8>> {
	if !matches!(byte_width, 1 | 2 | 4 | 8) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"signed stat byte width must be one of 1, 2, 4, 8",
		));
	}
	let bits = byte_width * 8;
	let min = -(1_i128 << (bits - 1));
	let max = (1_i128 << (bits - 1)) - 1;
	let value_i128 = i128::from(value);
	if value_i128 < min || value_i128 > max {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"signed stat value exceeds requested byte width",
		));
	}
	let sign_bit = 1_u64 << (bits - 1);
	let mask = if bits == 64 {
		u64::MAX
	} else {
		(1_u64 << bits) - 1
	};
	let biased = ((value as u64) & mask) ^ sign_bit;
	Ok(biased.to_be_bytes()[8 - byte_width..].to_vec())
}

pub fn encode_unsigned_stat_value(value: u64, byte_width: usize) -> std::io::Result<Vec<u8>> {
	if !matches!(byte_width, 1 | 2 | 4 | 8) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"unsigned stat byte width must be one of 1, 2, 4, 8",
		));
	}
	let max = if byte_width == 8 {
		u64::MAX
	} else {
		(1_u64 << (byte_width * 8)) - 1
	};
	if value > max {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"unsigned stat value exceeds requested byte width",
		));
	}
	Ok(value.to_be_bytes()[8 - byte_width..].to_vec())
}

pub fn encode_bool_stat_value(value: bool) -> [u8; 1] {
	[u8::from(value)]
}

pub fn encode_length_prefixed_bytes(out: &mut Vec<u8>, value: &[u8]) -> std::io::Result<()> {
	let len = u32::try_from(value.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"stat byte value exceeds u32::MAX bytes",
		)
	})?;
	out.extend_from_slice(&len.to_be_bytes());
	out.extend_from_slice(value);
	Ok(())
}

pub fn build_always_on_column_stats_block(
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
) -> std::io::Result<Vec<u8>> {
	build_column_stats_block_internal(descriptor, batch, COLUMN_STATS_DEFAULT_PREFIX_BYTES, None)
}

pub fn build_always_on_column_stats_block_with_prefix_cap(
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	prefix_cap: usize,
) -> std::io::Result<Vec<u8>> {
	build_column_stats_block_internal(descriptor, batch, prefix_cap, None)
}

pub fn build_column_stats_block_with_ciphertext_budget(
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	prefix_cap: usize,
	max_ciphertext_bytes: usize,
) -> std::io::Result<Vec<u8>> {
	build_column_stats_block_internal(descriptor, batch, prefix_cap, Some(max_ciphertext_bytes))
}

pub fn max_column_stats_ciphertext_bytes_for_slab_payload(slab_payload_bytes: usize) -> usize {
	usize::try_from(max_column_stats_block_ciphertext_bytes_for_slab_payload(
		slab_payload_bytes as u64,
	))
	.unwrap_or(usize::MAX)
}

fn build_column_stats_block_internal(
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	prefix_cap: usize,
	max_ciphertext_bytes: Option<usize>,
) -> std::io::Result<Vec<u8>> {
	let start = Instant::now();
	let schema = descriptor.arrow_schema()?;
	if schema.fields().len() != batch.num_columns() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"CSB build column count mismatch: descriptor={} batch={}",
				schema.fields().len(),
				batch.num_columns()
			),
		));
	}

	let batch_schema = batch.schema();
	let mut pending = Vec::with_capacity(batch.num_columns());
	for (ordinal, field) in schema.fields().iter().enumerate() {
		let batch_field = batch_schema.field(ordinal);
		if batch_field.name() != field.name() || batch_field.data_type() != field.data_type() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"CSB build field {} mismatch: descriptor={:?} batch={:?}",
					ordinal,
					field.data_type(),
					batch_field.data_type()
				),
			));
		}
		let column_ordinal = u16::try_from(ordinal).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"CSB build column ordinal exceeds u16::MAX",
			)
		})?;
		let bloom_spec = descriptor
			.stats_policy
			.bloom_filters
			.iter()
			.find(|spec| spec.column_name.as_str() == field.name().as_str());
		let hll_spec = descriptor
			.stats_policy
			.hll_distincts
			.iter()
			.find(|spec| spec.column_name.as_str() == field.name().as_str());
		let small_set_spec = descriptor
			.stats_policy
			.small_sets
			.iter()
			.find(|spec| spec.column_name.as_str() == field.name().as_str());
		pending.push(build_pending_column_stats_entry(
			column_ordinal,
			field.as_ref(),
			batch.column(ordinal).as_ref(),
			prefix_cap,
			bloom_spec,
			hll_spec,
			small_set_spec,
			max_ciphertext_bytes.is_none(),
		)?);
	}
	apply_opt_in_stats_budget(&mut pending, max_ciphertext_bytes)?;
	let columns = pending
		.into_iter()
		.map(PendingColumnStatsEntry::finish)
		.collect::<Vec<_>>();

	let header = ColumnStatsBlockHeader {
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		schema_fingerprint: descriptor.schema_fingerprint,
		row_count: u64::try_from(batch.num_rows())
			.map_err(|_| Error::new(ErrorKind::InvalidInput, "CSB row_count exceeds u64::MAX"))?,
		column_block_count: u32::try_from(columns.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"CSB column_block_count exceeds u32::MAX",
			)
		})?,
	};
	let encoded = encode_column_stats_block(&header, &columns)?;
	crate::metrics::global().observe_pbc_slab_stats_build(start.elapsed());
	crate::metrics::global().observe_pbc_slab_stats_block_bytes(encoded.len() as u64);
	Ok(encoded)
}

pub fn decode_column_stats_payload(
	entry: &ColumnStatsBlockEntry,
) -> std::io::Result<DecodedColumnStatsPayload> {
	validate_column_stats_entry(entry)?;
	let mut cursor = 0_usize;
	let min = if entry.stat_flags & COLUMN_STAT_HAS_MIN != 0 {
		Some(read_length_prefixed_bytes(&entry.payload, &mut cursor)?)
	} else {
		None
	};
	let max = if entry.stat_flags & COLUMN_STAT_HAS_MAX != 0 {
		Some(read_length_prefixed_bytes(&entry.payload, &mut cursor)?)
	} else {
		None
	};
	let null_count = if entry.stat_flags & COLUMN_STAT_HAS_NULL_COUNT != 0 {
		Some(read_u64_be(&entry.payload, &mut cursor, "null_count")?)
	} else {
		None
	};
	let non_null_count = if entry.stat_flags & COLUMN_STAT_HAS_NON_NULL_COUNT != 0 {
		Some(read_u64_be(&entry.payload, &mut cursor, "non_null_count")?)
	} else {
		None
	};
	let bloom_filter = if entry.stat_flags & COLUMN_STAT_HAS_BLOOM != 0 {
		Some(read_bloom_filter(&entry.payload, &mut cursor)?)
	} else {
		None
	};
	let hll_distinct = if entry.stat_flags & COLUMN_STAT_HAS_HLL != 0 {
		Some(read_hll_distinct(&entry.payload, &mut cursor)?)
	} else {
		None
	};
	let small_set = if entry.stat_flags & COLUMN_STAT_HAS_SMALL_SET != 0 {
		Some(read_small_set(&entry.payload, &mut cursor)?)
	} else {
		None
	};
	if cursor != entry.payload.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"CSB column payload has trailing bytes",
		));
	}
	Ok(DecodedColumnStatsPayload {
		min,
		max,
		null_count,
		non_null_count,
		bloom_filter,
		hll_distinct,
		small_set,
	})
}

pub fn prune_by_encoded_min_max(
	entry: &ColumnStatsBlockEntry,
	lower_inclusive: Option<&[u8]>,
	upper_inclusive: Option<&[u8]>,
) -> std::io::Result<ColumnStatsPrune> {
	let payload = decode_column_stats_payload(entry)?;
	Ok(prune_decoded_by_encoded_min_max(
		entry.arrow_type_tag,
		entry.stat_flags,
		&payload,
		lower_inclusive,
		upper_inclusive,
	))
}

pub fn prune_by_encoded_any_of(
	entry: &ColumnStatsBlockEntry,
	values: &[Vec<u8>],
) -> std::io::Result<ColumnStatsPrune> {
	if values.is_empty() {
		return Ok(ColumnStatsPrune::CannotMatch);
	}
	let payload = decode_column_stats_payload(entry)?;
	if payload.non_null_count == Some(0) {
		return Ok(ColumnStatsPrune::CannotMatch);
	}
	if let Some(small_set) = payload.small_set.as_ref() {
		return if values
			.iter()
			.any(|value| small_set.binary_search(value).is_ok())
		{
			Ok(ColumnStatsPrune::MayMatch)
		} else {
			Ok(ColumnStatsPrune::CannotMatch)
		};
	}
	if let Some(bloom) = payload.bloom_filter.as_ref() {
		return if values
			.iter()
			.any(|value| bloom_may_contain_encoded_value(bloom, value))
		{
			Ok(ColumnStatsPrune::MayMatch)
		} else {
			Ok(ColumnStatsPrune::CannotMatch)
		};
	}
	for value in values {
		if prune_decoded_by_encoded_min_max(
			entry.arrow_type_tag,
			entry.stat_flags,
			&payload,
			Some(value),
			Some(value),
		) == ColumnStatsPrune::MayMatch
		{
			return Ok(ColumnStatsPrune::MayMatch);
		}
	}
	Ok(ColumnStatsPrune::CannotMatch)
}

pub fn bloom_may_contain_encoded_value(bloom: &ColumnStatsBloomPayload, value: &[u8]) -> bool {
	if bloom.bit_count == 0 {
		return false;
	}
	let Ok(expected_len) = bloom_bit_array_len(bloom.bit_count) else {
		return false;
	};
	if bloom.bit_array.len() != expected_len {
		return false;
	}
	bloom_bit_positions(value, bloom.bit_count)
		.into_iter()
		.all(|bit_index| bloom_bit_is_set(&bloom.bit_array, bit_index))
}

pub fn estimate_hll_distinct_count(hll: &ColumnStatsHllPayload) -> std::io::Result<f64> {
	validate_hll_payload_shape(hll)?;
	let m = hll.registers.len() as f64;
	let alpha = 0.7213 / (1.0 + 1.079 / m);
	let inverse_sum = hll
		.registers
		.iter()
		.map(|register| 2_f64.powi(-i32::from(*register)))
		.sum::<f64>();
	let raw = alpha * m * m / inverse_sum;
	let zero_registers = hll
		.registers
		.iter()
		.filter(|register| **register == 0)
		.count();
	if raw <= 2.5 * m && zero_registers != 0 {
		Ok(m * (m / zero_registers as f64).ln())
	} else {
		Ok(raw)
	}
}

pub fn distinct_count_for_column_stats_payload(
	payload: &DecodedColumnStatsPayload,
) -> std::io::Result<DistinctCount> {
	if let Some(small_set) = payload.small_set.as_ref() {
		let value_count = u64::try_from(small_set.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"CSB small-set value count exceeds u64::MAX",
			)
		})?;
		return Ok(DistinctCount::Exact(value_count));
	}
	if let Some(hll) = payload.hll_distinct.as_ref() {
		let estimate = estimate_hll_distinct_count(hll)?.round();
		let estimate = if estimate.is_sign_negative() {
			0
		} else if estimate > u64::MAX as f64 {
			u64::MAX
		} else {
			estimate as u64
		};
		let register_count = hll_register_count(hll.precision)? as f64;
		return Ok(DistinctCount::Approx {
			estimate,
			std_err: 1.04 / register_count.sqrt(),
		});
	}
	Ok(DistinctCount::Unknown)
}

pub fn distinct_count_for_column_stats_entry(
	entry: &ColumnStatsBlockEntry,
) -> std::io::Result<DistinctCount> {
	let payload = decode_column_stats_payload(entry)?;
	distinct_count_for_column_stats_payload(&payload)
}

pub fn distinct_count_for_column_ordinal(
	stats: &ParsedColumnStatsBlock,
	column_ordinal: u16,
) -> std::io::Result<DistinctCount> {
	let Some(entry) = stats
		.columns
		.iter()
		.find(|entry| entry.column_ordinal == column_ordinal)
	else {
		return Ok(DistinctCount::Unknown);
	};
	distinct_count_for_column_stats_entry(entry)
}

fn prune_decoded_by_encoded_min_max(
	arrow_type_tag: u8,
	stat_flags: u16,
	payload: &DecodedColumnStatsPayload,
	lower_inclusive: Option<&[u8]>,
	upper_inclusive: Option<&[u8]>,
) -> ColumnStatsPrune {
	if payload.non_null_count == Some(0) {
		return ColumnStatsPrune::CannotMatch;
	}
	let prefix_truncated = stats_tag_uses_prefix_truncation(arrow_type_tag);
	if let (Some(lower), Some(max)) = (lower_inclusive, payload.max.as_deref()) {
		if lower > max {
			if stat_flags & COLUMN_STAT_MAX_TRUNCATED == 0 {
				return ColumnStatsPrune::CannotMatch;
			}
			if prefix_truncated
				&& byte_prefix_successor(max).is_some_and(|successor| lower >= successor.as_slice())
			{
				return ColumnStatsPrune::CannotMatch;
			}
		}
	}
	if let (Some(upper), Some(min)) = (upper_inclusive, payload.min.as_deref()) {
		if upper < min {
			return ColumnStatsPrune::CannotMatch;
		}
		if upper == min && prefix_truncated && stat_flags & COLUMN_STAT_MIN_TRUNCATED != 0 {
			return ColumnStatsPrune::CannotMatch;
		}
	}
	ColumnStatsPrune::MayMatch
}

fn byte_prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
	let mut successor = prefix.to_vec();
	while let Some(last) = successor.pop() {
		if last != u8::MAX {
			successor.push(last + 1);
			return Some(successor);
		}
	}
	None
}

struct PendingColumnStatsEntry {
	entry: ColumnStatsBlockEntry,
	bloom: Option<Vec<u8>>,
	hll: Option<Vec<u8>>,
	small_set: Option<Vec<u8>>,
	select_bloom: bool,
	select_hll: bool,
	select_small_set: bool,
}

impl PendingColumnStatsEntry {
	fn finish(mut self) -> ColumnStatsBlockEntry {
		if self.select_bloom {
			if let Some(payload) = self.bloom.take() {
				self.entry.stat_flags |= COLUMN_STAT_HAS_BLOOM;
				self.entry.payload.extend_from_slice(&payload);
			}
		}
		if self.select_hll {
			if let Some(payload) = self.hll.take() {
				self.entry.stat_flags |= COLUMN_STAT_HAS_HLL;
				self.entry.payload.extend_from_slice(&payload);
			}
		}
		if self.select_small_set {
			if let Some(payload) = self.small_set.take() {
				self.entry.stat_flags |= COLUMN_STAT_HAS_SMALL_SET;
				self.entry.payload.extend_from_slice(&payload);
			}
		}
		self.entry
	}
}

#[derive(Clone, Copy)]
enum OptInStatKind {
	SmallSet,
	Bloom,
	Hll,
}

fn apply_opt_in_stats_budget(
	pending: &mut [PendingColumnStatsEntry],
	max_ciphertext_bytes: Option<usize>,
) -> std::io::Result<()> {
	let Some(max_ciphertext_bytes) = max_ciphertext_bytes else {
		return Ok(());
	};
	let mut plaintext_len = pending_plaintext_len(pending)?;
	let max_plaintext_len =
		max_ciphertext_bytes.saturating_sub(COLUMN_STATS_CSB_AEAD_TAG_BYTES as usize);
	for kind in [
		OptInStatKind::SmallSet,
		OptInStatKind::Bloom,
		OptInStatKind::Hll,
	] {
		for entry in pending.iter_mut() {
			maybe_select_opt_in_stat(entry, kind, &mut plaintext_len, max_plaintext_len)?;
		}
	}
	Ok(())
}

fn pending_plaintext_len(pending: &[PendingColumnStatsEntry]) -> std::io::Result<usize> {
	let payload_bytes = pending.iter().try_fold(0_usize, |sum, entry| {
		sum.checked_add(entry.entry.payload.len())
			.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CSB payload length overflow"))
	})?;
	COLUMN_STATS_BLOCK_HEADER_BYTES
		.checked_add(
			pending
				.len()
				.checked_mul(COLUMN_STATS_COLUMN_HEADER_BYTES)
				.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CSB header length overflow"))?,
		)
		.and_then(|bytes| bytes.checked_add(payload_bytes))
		.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CSB plaintext length overflow"))
}

fn maybe_select_opt_in_stat(
	entry: &mut PendingColumnStatsEntry,
	kind: OptInStatKind,
	plaintext_len: &mut usize,
	max_plaintext_len: usize,
) -> std::io::Result<()> {
	let payload_len = match kind {
		OptInStatKind::SmallSet => entry.small_set.as_ref().map(Vec::len),
		OptInStatKind::Bloom => entry.bloom.as_ref().map(Vec::len),
		OptInStatKind::Hll => entry.hll.as_ref().map(Vec::len),
	};
	let Some(payload_len) = payload_len else {
		return Ok(());
	};
	let next_len = plaintext_len
		.checked_add(payload_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CSB budget length overflow"))?;
	if next_len > max_plaintext_len {
		return Ok(());
	}
	*plaintext_len = next_len;
	match kind {
		OptInStatKind::SmallSet => entry.select_small_set = true,
		OptInStatKind::Bloom => entry.select_bloom = true,
		OptInStatKind::Hll => entry.select_hll = true,
	}
	Ok(())
}

struct CollectedColumnStats {
	min: Option<Vec<u8>>,
	max: Option<Vec<u8>>,
	min_truncated: bool,
	max_truncated: bool,
	null_count: u64,
	non_null_count: u64,
	bloom: Option<Vec<u8>>,
	hll: Option<Vec<u8>>,
	small_set: Option<Vec<u8>>,
}

fn build_pending_column_stats_entry(
	column_ordinal: u16,
	field: &Field,
	array: &dyn Array,
	prefix_cap: usize,
	bloom_spec: Option<&ColumnBloomSpecV1>,
	hll_spec: Option<&ColumnHllSpecV1>,
	small_set_spec: Option<&ColumnSmallSetSpecV1>,
	select_all_opt_ins: bool,
) -> std::io::Result<PendingColumnStatsEntry> {
	let Some(arrow_type_tag) = arrow_stats_type_tag(field.data_type()) else {
		let null_count = column_stats_count(array.null_count(), "null_count")?;
		let non_null_count = column_stats_count(
			array.len().saturating_sub(array.null_count()),
			"non_null_count",
		)?;
		let mut payload = Vec::with_capacity(16);
		payload.extend_from_slice(&null_count.to_be_bytes());
		payload.extend_from_slice(&non_null_count.to_be_bytes());
		return Ok(PendingColumnStatsEntry {
			entry: ColumnStatsBlockEntry {
				column_ordinal,
				arrow_type_tag: ARROW_STATS_TAG_OPAQUE_UNSUPPORTED,
				stat_flags: COLUMN_STAT_HAS_NULL_COUNT | COLUMN_STAT_HAS_NON_NULL_COUNT,
				payload,
			},
			bloom: None,
			hll: None,
			small_set: None,
			select_bloom: select_all_opt_ins,
			select_hll: select_all_opt_ins,
			select_small_set: select_all_opt_ins,
		});
	};
	let stats = collect_column_stats_in_one_pass(
		field.data_type(),
		array,
		arrow_type_tag,
		prefix_cap,
		bloom_spec,
		hll_spec,
		small_set_spec,
	)?;
	let mut stat_flags = COLUMN_STAT_HAS_NULL_COUNT | COLUMN_STAT_HAS_NON_NULL_COUNT;
	let mut payload = Vec::new();
	if let Some(value) = stats.min.as_deref() {
		stat_flags |= COLUMN_STAT_HAS_MIN;
		if stats.min_truncated {
			stat_flags |= COLUMN_STAT_MIN_TRUNCATED;
		}
		encode_length_prefixed_bytes(&mut payload, value)?;
	}
	if let Some(value) = stats.max.as_deref() {
		stat_flags |= COLUMN_STAT_HAS_MAX;
		if stats.max_truncated {
			stat_flags |= COLUMN_STAT_MAX_TRUNCATED;
		}
		encode_length_prefixed_bytes(&mut payload, value)?;
	}
	payload.extend_from_slice(&stats.null_count.to_be_bytes());
	payload.extend_from_slice(&stats.non_null_count.to_be_bytes());
	Ok(PendingColumnStatsEntry {
		entry: ColumnStatsBlockEntry {
			column_ordinal,
			arrow_type_tag,
			stat_flags,
			payload,
		},
		bloom: stats.bloom,
		hll: stats.hll,
		small_set: stats.small_set,
		select_bloom: select_all_opt_ins,
		select_hll: select_all_opt_ins,
		select_small_set: select_all_opt_ins,
	})
}

fn collect_column_stats_in_one_pass(
	data_type: &DataType,
	array: &dyn Array,
	arrow_type_tag: u8,
	prefix_cap: usize,
	bloom_spec: Option<&ColumnBloomSpecV1>,
	hll_spec: Option<&ColumnHllSpecV1>,
	small_set_spec: Option<&ColumnSmallSetSpecV1>,
) -> std::io::Result<CollectedColumnStats> {
	let null_count = column_stats_count(array.null_count(), "null_count")?;
	let non_null_count = column_stats_count(
		array.len().saturating_sub(array.null_count()),
		"non_null_count",
	)?;
	let mut min = None::<Vec<u8>>;
	let mut max = None::<Vec<u8>>;
	let mut bloom = bloom_spec
		.map(|spec| new_bloom_filter_for_row_count(array.len(), spec.bits_per_row.get()))
		.transpose()?;
	let mut hll = hll_spec
		.map(|spec| new_hll_distinct(spec.precision.get()))
		.transpose()?;
	let mut small_set = small_set_spec.map(SmallSetCollector::new);

	for_each_encoded_non_null_value(data_type, array, |value| {
		if min
			.as_deref()
			.map(|current| value.as_slice() < current)
			.unwrap_or(true)
		{
			min = Some(value.clone());
		}
		if max
			.as_deref()
			.map(|current| value.as_slice() > current)
			.unwrap_or(true)
		{
			max = Some(value.clone());
		}
		if let Some(bloom) = bloom.as_mut() {
			if bloom.bit_count != 0 {
				bloom_insert_encoded_value(bloom, &value);
			}
		}
		if let Some(hll) = hll.as_mut() {
			hll_insert_encoded_value(hll, &value);
		}
		if let Some(small_set) = small_set.as_mut() {
			small_set.insert(value)?;
		}
		Ok(())
	})?;

	let (min, min_truncated) = maybe_truncate_encoded_stat_value(min, arrow_type_tag, prefix_cap);
	let (max, max_truncated) = maybe_truncate_encoded_stat_value(max, arrow_type_tag, prefix_cap);
	let bloom = bloom.map(|bloom| {
		let mut payload = Vec::new();
		encode_bloom_filter(&mut payload, &bloom);
		payload
	});
	let hll = hll.map(|hll| {
		let mut payload = Vec::new();
		encode_hll_distinct(&mut payload, &hll);
		payload
	});
	let small_set = small_set
		.and_then(SmallSetCollector::finish)
		.map(|values| {
			let mut payload = Vec::new();
			encode_small_set(&mut payload, &values)?;
			Ok::<_, std::io::Error>(payload)
		})
		.transpose()?;

	Ok(CollectedColumnStats {
		min,
		max,
		min_truncated,
		max_truncated,
		null_count,
		non_null_count,
		bloom,
		hll,
		small_set,
	})
}

fn column_stats_count(value: usize, field: &str) -> std::io::Result<u64> {
	u64::try_from(value).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("CSB {field} exceeds u64::MAX"),
		)
	})
}

fn maybe_truncate_encoded_stat_value(
	value: Option<Vec<u8>>,
	arrow_type_tag: u8,
	prefix_cap: usize,
) -> (Option<Vec<u8>>, bool) {
	if stats_tag_uses_prefix_truncation(arrow_type_tag) {
		truncate_prefix(value, prefix_cap)
	} else {
		(value, false)
	}
}

pub fn locator_flags_for_parsed_column_stats_block(parsed: &ParsedColumnStatsBlock) -> u16 {
	let mut flags = 0_u16;
	if parsed
		.columns
		.iter()
		.any(|entry| entry.stat_flags & COLUMN_STAT_HAS_SMALL_SET != 0)
	{
		flags |= 1 << 1;
	}
	if parsed
		.columns
		.iter()
		.any(|entry| entry.stat_flags & COLUMN_STAT_HAS_BLOOM != 0)
	{
		flags |= 1 << 2;
	}
	if parsed
		.columns
		.iter()
		.any(|entry| entry.stat_flags & COLUMN_STAT_HAS_HLL != 0)
	{
		flags |= 1 << 3;
	}
	flags
}

fn encode_small_set(out: &mut Vec<u8>, values: &[Vec<u8>]) -> std::io::Result<()> {
	let value_count = u32::try_from(values.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"CSB small-set value_count exceeds u32::MAX",
		)
	})?;
	out.extend_from_slice(&value_count.to_be_bytes());
	for value in values {
		encode_length_prefixed_bytes(out, value)?;
	}
	Ok(())
}

fn encode_bloom_filter(out: &mut Vec<u8>, bloom: &ColumnStatsBloomPayload) {
	out.extend_from_slice(&bloom.bit_count.to_be_bytes());
	out.extend_from_slice(&bloom.bit_array);
}

fn encode_hll_distinct(out: &mut Vec<u8>, hll: &ColumnStatsHllPayload) {
	out.push(hll.precision);
	out.extend_from_slice(&hll.registers);
}

fn new_bloom_filter_for_row_count(
	row_count: usize,
	bits_per_row: u8,
) -> std::io::Result<ColumnStatsBloomPayload> {
	let row_count = u64::try_from(row_count)
		.map_err(|_| Error::new(ErrorKind::InvalidInput, "CSB row_count exceeds u64::MAX"))?;
	let bit_count_u64 = row_count
		.checked_mul(u64::from(bits_per_row))
		.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CSB bloom bit_count overflow"))?;
	let bit_count = u32::try_from(bit_count_u64).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"CSB bloom bit_count exceeds u32::MAX",
		)
	})?;
	let bit_array_len = bloom_bit_array_len(bit_count)?;
	let bloom = ColumnStatsBloomPayload {
		bit_count,
		bit_array: vec![0; bit_array_len],
	};
	Ok(bloom)
}

fn new_hll_distinct(precision: u8) -> std::io::Result<ColumnStatsHllPayload> {
	let register_count = hll_register_count(precision)?;
	Ok(ColumnStatsHllPayload {
		precision,
		registers: vec![0; register_count],
	})
}

fn bloom_insert_encoded_value(bloom: &mut ColumnStatsBloomPayload, value: &[u8]) {
	for bit_index in bloom_bit_positions(value, bloom.bit_count) {
		bloom_bit_set(&mut bloom.bit_array, bit_index);
	}
}

fn hll_insert_encoded_value(hll: &mut ColumnStatsHllPayload, value: &[u8]) {
	let hash = siphash13(
		COLUMN_STATS_HLL_SIPHASH_K0,
		COLUMN_STATS_HLL_SIPHASH_K1,
		value,
	);
	let (index, rank) = hll_register_index_and_rank(hash, hll.precision);
	if let Some(register) = hll.registers.get_mut(index) {
		*register = (*register).max(rank);
	}
}

fn hll_register_index_and_rank(hash: u64, precision: u8) -> (usize, u8) {
	let index = (hash >> (64 - precision)) as usize;
	let remaining = hash << precision;
	let max_rank = 64 - precision + 1;
	let rank = (remaining.leading_zeros() + 1).min(u32::from(max_rank)) as u8;
	(index, rank)
}

fn bloom_bit_positions(value: &[u8], bit_count: u32) -> [u32; COLUMN_STATS_BLOOM_HASHES as usize] {
	let h1 = siphash13(
		COLUMN_STATS_BLOOM_SIPHASH_K0,
		COLUMN_STATS_BLOOM_SIPHASH_K1,
		value,
	);
	let h2 = siphash13(
		COLUMN_STATS_BLOOM_SIPHASH_ALT_K0,
		COLUMN_STATS_BLOOM_SIPHASH_ALT_K1,
		value,
	) | 1;
	let mut positions = [0_u32; COLUMN_STATS_BLOOM_HASHES as usize];
	for (index, position) in positions.iter_mut().enumerate() {
		*position = h1
			.wrapping_add((index as u64).wrapping_mul(h2))
			.rem_euclid(u64::from(bit_count)) as u32;
	}
	positions
}

fn bloom_bit_array_len(bit_count: u32) -> std::io::Result<usize> {
	usize::try_from(u64::from(bit_count).div_ceil(8)).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"CSB bloom bit_count does not fit in usize",
		)
	})
}

fn hll_register_count(precision: u8) -> std::io::Result<usize> {
	if !(COLUMN_STATS_HLL_MIN_PRECISION..=COLUMN_STATS_HLL_MAX_PRECISION).contains(&precision) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"CSB HLL precision {precision} outside {}..={}",
				COLUMN_STATS_HLL_MIN_PRECISION, COLUMN_STATS_HLL_MAX_PRECISION
			),
		));
	}
	1_usize
		.checked_shl(u32::from(precision))
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "CSB HLL register count overflow"))
}

fn validate_hll_payload_shape(hll: &ColumnStatsHllPayload) -> std::io::Result<()> {
	let expected_len = hll_register_count(hll.precision)?;
	if hll.registers.len() != expected_len {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"CSB HLL register count {} does not match precision {}",
				hll.registers.len(),
				hll.precision
			),
		));
	}
	Ok(())
}

fn bloom_bit_set(bit_array: &mut [u8], bit_index: u32) {
	let byte_index = (bit_index / 8) as usize;
	let bit_offset = bit_index % 8;
	bit_array[byte_index] |= 1 << (7 - bit_offset);
}

fn bloom_bit_is_set(bit_array: &[u8], bit_index: u32) -> bool {
	let byte_index = (bit_index / 8) as usize;
	let bit_offset = bit_index % 8;
	bit_array
		.get(byte_index)
		.is_some_and(|byte| byte & (1 << (7 - bit_offset)) != 0)
}

fn siphash13(k0: u64, k1: u64, bytes: &[u8]) -> u64 {
	let mut v0 = 0x736f_6d65_7073_6575 ^ k0;
	let mut v1 = 0x646f_7261_6e64_6f6d ^ k1;
	let mut v2 = 0x6c79_6765_6e65_7261 ^ k0;
	let mut v3 = 0x7465_6462_7974_6573 ^ k1;
	let mut chunks = bytes.chunks_exact(8);
	for chunk in &mut chunks {
		let m = u64::from_le_bytes(chunk.try_into().expect("chunk len"));
		v3 ^= m;
		sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
		v0 ^= m;
	}
	let mut b = (bytes.len() as u64) << 56;
	for (index, byte) in chunks.remainder().iter().enumerate() {
		b |= u64::from(*byte) << (index * 8);
	}
	v3 ^= b;
	sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
	v0 ^= b;
	v2 ^= 0xff;
	for _ in 0..3 {
		sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
	}
	v0 ^ v1 ^ v2 ^ v3
}

fn sip_round(v0: &mut u64, v1: &mut u64, v2: &mut u64, v3: &mut u64) {
	*v0 = (*v0).wrapping_add(*v1);
	*v1 = (*v1).rotate_left(13);
	*v1 ^= *v0;
	*v0 = (*v0).rotate_left(32);
	*v2 = (*v2).wrapping_add(*v3);
	*v3 = (*v3).rotate_left(16);
	*v3 ^= *v2;
	*v0 = (*v0).wrapping_add(*v3);
	*v3 = (*v3).rotate_left(21);
	*v3 ^= *v0;
	*v2 = (*v2).wrapping_add(*v1);
	*v1 = (*v1).rotate_left(17);
	*v1 ^= *v2;
	*v2 = (*v2).rotate_left(32);
}

fn for_each_encoded_non_null_value<F>(
	data_type: &DataType,
	array: &dyn Array,
	mut emit: F,
) -> std::io::Result<()>
where
	F: FnMut(Vec<u8>) -> std::io::Result<()>,
{
	match data_type {
		DataType::Int8 => emit_signed_values::<Int8Type, _>(array, 1, &mut emit),
		DataType::Int16 => emit_signed_values::<Int16Type, _>(array, 2, &mut emit),
		DataType::Int32 => emit_signed_values::<Int32Type, _>(array, 4, &mut emit),
		DataType::Int64 => emit_signed_values::<Int64Type, _>(array, 8, &mut emit),
		DataType::UInt8 => emit_unsigned_values::<UInt8Type, _>(array, 1, &mut emit),
		DataType::UInt16 => emit_unsigned_values::<UInt16Type, _>(array, 2, &mut emit),
		DataType::UInt32 => emit_unsigned_values::<UInt32Type, _>(array, 4, &mut emit),
		DataType::UInt64 => emit_unsigned_values::<UInt64Type, _>(array, 8, &mut emit),
		DataType::Timestamp(TimeUnit::Millisecond, _) => {
			emit_signed_values::<TimestampMillisecondType, _>(array, 8, &mut emit)
		}
		DataType::Timestamp(TimeUnit::Microsecond, _) => {
			emit_signed_values::<TimestampMicrosecondType, _>(array, 8, &mut emit)
		}
		DataType::Timestamp(TimeUnit::Nanosecond, _) => {
			emit_signed_values::<TimestampNanosecondType, _>(array, 8, &mut emit)
		}
		DataType::Date32 => emit_signed_values::<Date32Type, _>(array, 4, &mut emit),
		DataType::Date64 => emit_signed_values::<Date64Type, _>(array, 8, &mut emit),
		DataType::Boolean => emit_bool_values(array, &mut emit),
		DataType::Utf8 => emit_utf8_values(array, &mut emit),
		DataType::LargeUtf8 => emit_large_utf8_values(array, &mut emit),
		DataType::Binary => emit_binary_values(array, &mut emit),
		DataType::LargeBinary => emit_large_binary_values(array, &mut emit),
		_ => Ok(()),
	}
}

fn emit_signed_values<T, F>(
	array: &dyn Array,
	byte_width: usize,
	emit: &mut F,
) -> std::io::Result<()>
where
	T: ArrowPrimitiveType,
	T::Native: Copy + Into<i64>,
	F: FnMut(Vec<u8>) -> std::io::Result<()>,
{
	let typed = downcast_array::<PrimitiveArray<T>>(array)?;
	for row in 0..typed.len() {
		if typed.is_valid(row) {
			emit(encode_signed_stat_value(
				typed.value(row).into(),
				byte_width,
			)?)?;
		}
	}
	Ok(())
}

fn emit_unsigned_values<T, F>(
	array: &dyn Array,
	byte_width: usize,
	emit: &mut F,
) -> std::io::Result<()>
where
	T: ArrowPrimitiveType,
	T::Native: Copy + Into<u64>,
	F: FnMut(Vec<u8>) -> std::io::Result<()>,
{
	let typed = downcast_array::<PrimitiveArray<T>>(array)?;
	for row in 0..typed.len() {
		if typed.is_valid(row) {
			emit(encode_unsigned_stat_value(
				typed.value(row).into(),
				byte_width,
			)?)?;
		}
	}
	Ok(())
}

fn emit_bool_values<F>(array: &dyn Array, emit: &mut F) -> std::io::Result<()>
where
	F: FnMut(Vec<u8>) -> std::io::Result<()>,
{
	let typed = downcast_array::<BooleanArray>(array)?;
	for row in 0..typed.len() {
		if typed.is_valid(row) {
			emit(encode_bool_stat_value(typed.value(row)).to_vec())?;
		}
	}
	Ok(())
}

fn emit_utf8_values<F>(array: &dyn Array, emit: &mut F) -> std::io::Result<()>
where
	F: FnMut(Vec<u8>) -> std::io::Result<()>,
{
	let typed = downcast_array::<StringArray>(array)?;
	emit_bytes_values(typed, |array, row| array.value(row).as_bytes(), emit)
}

fn emit_large_utf8_values<F>(array: &dyn Array, emit: &mut F) -> std::io::Result<()>
where
	F: FnMut(Vec<u8>) -> std::io::Result<()>,
{
	let typed = downcast_array::<LargeStringArray>(array)?;
	emit_bytes_values(typed, |array, row| array.value(row).as_bytes(), emit)
}

fn emit_binary_values<F>(array: &dyn Array, emit: &mut F) -> std::io::Result<()>
where
	F: FnMut(Vec<u8>) -> std::io::Result<()>,
{
	let typed = downcast_array::<BinaryArray>(array)?;
	emit_bytes_values(typed, |array, row| array.value(row), emit)
}

fn emit_large_binary_values<F>(array: &dyn Array, emit: &mut F) -> std::io::Result<()>
where
	F: FnMut(Vec<u8>) -> std::io::Result<()>,
{
	let typed = downcast_array::<LargeBinaryArray>(array)?;
	emit_bytes_values(typed, |array, row| array.value(row), emit)
}

fn emit_bytes_values<A, F, G>(array: &A, value_at: G, emit: &mut F) -> std::io::Result<()>
where
	A: Array,
	F: FnMut(Vec<u8>) -> std::io::Result<()>,
	G: Fn(&A, usize) -> &[u8],
{
	for row in 0..array.len() {
		if array.is_valid(row) {
			emit(value_at(array, row).to_vec())?;
		}
	}
	Ok(())
}

struct SmallSetCollector {
	values: BTreeSet<Vec<u8>>,
	encoded_bytes: usize,
	max_distinct_values: usize,
	max_bytes: usize,
	overflowed: bool,
}

impl SmallSetCollector {
	fn new(spec: &ColumnSmallSetSpecV1) -> Self {
		Self {
			values: BTreeSet::new(),
			encoded_bytes: 4,
			max_distinct_values: spec.max_distinct_values.get() as usize,
			max_bytes: spec.max_bytes.get() as usize,
			overflowed: false,
		}
	}

	fn insert(&mut self, value: Vec<u8>) -> std::io::Result<()> {
		if self.overflowed {
			return Ok(());
		}
		let inserted_len = value.len();
		if !self.values.insert(value) {
			return Ok(());
		}
		self.encoded_bytes = self
			.encoded_bytes
			.checked_add(4)
			.and_then(|bytes| bytes.checked_add(inserted_len))
			.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CSB small-set byte overflow"))?;
		if self.values.len() > self.max_distinct_values || self.encoded_bytes > self.max_bytes {
			self.overflowed = true;
		}
		Ok(())
	}

	fn finish(self) -> Option<Vec<Vec<u8>>> {
		if self.overflowed {
			None
		} else {
			Some(self.values.into_iter().collect())
		}
	}
}

#[cfg(test)]
fn min_max_for_array(
	data_type: &DataType,
	array: &dyn Array,
	prefix_cap: usize,
) -> std::io::Result<(Option<Vec<u8>>, Option<Vec<u8>>, bool, bool)> {
	let Some(arrow_type_tag) = arrow_stats_type_tag(data_type) else {
		return Ok((None, None, false, false));
	};
	let stats = collect_column_stats_in_one_pass(
		data_type,
		array,
		arrow_type_tag,
		prefix_cap,
		None,
		None,
		None,
	)?;
	Ok((
		stats.min,
		stats.max,
		stats.min_truncated,
		stats.max_truncated,
	))
}

fn truncate_prefix(value: Option<Vec<u8>>, prefix_cap: usize) -> (Option<Vec<u8>>, bool) {
	let Some(mut value) = value else {
		return (None, false);
	};
	if value.len() > prefix_cap {
		value.truncate(prefix_cap);
		(Some(value), true)
	} else {
		(Some(value), false)
	}
}

fn downcast_array<A: Array + 'static>(array: &dyn Array) -> std::io::Result<&A> {
	array.as_any().downcast_ref::<A>().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			format!(
				"CSB Arrow array downcast failed: expected {}",
				std::any::type_name::<A>()
			),
		)
	})
}

fn read_length_prefixed_bytes(bytes: &[u8], cursor: &mut usize) -> std::io::Result<Vec<u8>> {
	let len_bytes = bytes.get(*cursor..*cursor + 4).ok_or_else(|| {
		Error::new(
			ErrorKind::UnexpectedEof,
			"CSB payload truncated while reading byte length",
		)
	})?;
	*cursor += 4;
	let len = u32::from_be_bytes(len_bytes.try_into().expect("slice len")) as usize;
	let value = bytes.get(*cursor..*cursor + len).ok_or_else(|| {
		Error::new(
			ErrorKind::UnexpectedEof,
			"CSB payload truncated while reading byte value",
		)
	})?;
	*cursor += len;
	Ok(value.to_vec())
}

fn read_bloom_filter(bytes: &[u8], cursor: &mut usize) -> std::io::Result<ColumnStatsBloomPayload> {
	let bit_count_bytes = bytes.get(*cursor..*cursor + 4).ok_or_else(|| {
		Error::new(
			ErrorKind::UnexpectedEof,
			"CSB payload truncated while reading bloom bit_count",
		)
	})?;
	*cursor += 4;
	let bit_count = u32::from_be_bytes(bit_count_bytes.try_into().expect("slice len"));
	let bit_array_len = bloom_bit_array_len(bit_count)?;
	let bit_array = bytes.get(*cursor..*cursor + bit_array_len).ok_or_else(|| {
		Error::new(
			ErrorKind::UnexpectedEof,
			"CSB payload truncated while reading bloom bit_array",
		)
	})?;
	*cursor += bit_array_len;
	Ok(ColumnStatsBloomPayload {
		bit_count,
		bit_array: bit_array.to_vec(),
	})
}

fn read_hll_distinct(bytes: &[u8], cursor: &mut usize) -> std::io::Result<ColumnStatsHllPayload> {
	let precision = *bytes.get(*cursor).ok_or_else(|| {
		Error::new(
			ErrorKind::UnexpectedEof,
			"CSB payload truncated while reading HLL precision",
		)
	})?;
	*cursor += 1;
	let register_count = hll_register_count(precision)?;
	let registers = bytes
		.get(*cursor..*cursor + register_count)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::UnexpectedEof,
				"CSB payload truncated while reading HLL registers",
			)
		})?;
	*cursor += register_count;
	Ok(ColumnStatsHllPayload {
		precision,
		registers: registers.to_vec(),
	})
}

fn read_small_set(bytes: &[u8], cursor: &mut usize) -> std::io::Result<Vec<Vec<u8>>> {
	let count_bytes = bytes.get(*cursor..*cursor + 4).ok_or_else(|| {
		Error::new(
			ErrorKind::UnexpectedEof,
			"CSB payload truncated while reading small-set value_count",
		)
	})?;
	*cursor += 4;
	let value_count = u32::from_be_bytes(count_bytes.try_into().expect("slice len")) as usize;
	let mut values = Vec::with_capacity(value_count);
	for _ in 0..value_count {
		let value = read_length_prefixed_bytes(bytes, cursor)?;
		if values.last().is_some_and(|previous| previous >= &value) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"CSB small-set values must be sorted and unique",
			));
		}
		values.push(value);
	}
	Ok(values)
}

fn read_u64_be(bytes: &[u8], cursor: &mut usize, field: &str) -> std::io::Result<u64> {
	let value = bytes.get(*cursor..*cursor + 8).ok_or_else(|| {
		Error::new(
			ErrorKind::UnexpectedEof,
			format!("CSB payload truncated while reading {field}"),
		)
	})?;
	*cursor += 8;
	Ok(u64::from_be_bytes(value.try_into().expect("slice len")))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::table::{
		BloomBitsPerRow, ColumnHllSpecV1, ColumnSmallSetSpecV1, ColumnStatsPolicyV1, HllPrecision,
		LocalityPolicyV1, PrimaryKeyDescriptorV1, SchemaEvolutionPolicyV1, SmallSetByteCap,
		SmallSetValueCap,
	};
	use arrow::array::{Float64Array, Int64Array, LargeStringArray};
	use arrow::datatypes::{Field, Schema};
	use std::sync::Arc;

	fn stats_descriptor(schema: &Schema) -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new(
			TableObjectId::new([9; 16]),
			7,
			"analytics",
			"events",
			schema,
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	#[test]
	fn csb_plaintext_roundtrip_preserves_header_and_column_entry() {
		let header = ColumnStatsBlockHeader {
			table_object_id: TableObjectId::new([7; 16]),
			schema_generation: 42,
			schema_fingerprint: [0xAB; 32],
			row_count: 3,
			column_block_count: 1,
		};
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, &[0x01]).unwrap();
		encode_length_prefixed_bytes(&mut payload, &[0x05]).unwrap();
		payload.extend_from_slice(&1_u64.to_be_bytes());
		payload.extend_from_slice(&2_u64.to_be_bytes());
		let column = ColumnStatsBlockEntry {
			column_ordinal: 2,
			arrow_type_tag: ARROW_STATS_TAG_UINT8,
			stat_flags: COLUMN_STAT_HAS_MIN
				| COLUMN_STAT_HAS_MAX
				| COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT,
			payload,
		};

		let encoded = encode_column_stats_block(&header, &[column.clone()]).unwrap();
		assert_eq!(
			encoded.len(),
			COLUMN_STATS_BLOCK_HEADER_BYTES + 9 + column.payload.len()
		);
		let parsed = parse_column_stats_block(&encoded).unwrap();
		assert_eq!(parsed.header, header);
		assert_eq!(parsed.columns, vec![column]);
	}

	#[test]
	fn csb_plaintext_layout_matches_adr039_golden_offsets() {
		assert_eq!(COLUMN_STATS_BLOCK_HEADER_BYTES, 84);
		assert_eq!(COLUMN_STATS_COLUMN_HEADER_BYTES, 9);
		let mut table_object_id = [0_u8; 16];
		for (index, byte) in table_object_id.iter_mut().enumerate() {
			*byte = 0x10 + index as u8;
		}
		let mut schema_fingerprint = [0_u8; 32];
		for (index, byte) in schema_fingerprint.iter_mut().enumerate() {
			*byte = 0xA0 + index as u8;
		}
		let header = ColumnStatsBlockHeader {
			table_object_id: TableObjectId::new(table_object_id),
			schema_generation: 0x0102_0304_0506_0708,
			schema_fingerprint,
			row_count: 3,
			column_block_count: 1,
		};
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, b"ab").unwrap();
		encode_length_prefixed_bytes(&mut payload, b"az").unwrap();
		payload.extend_from_slice(&1_u64.to_be_bytes());
		payload.extend_from_slice(&2_u64.to_be_bytes());
		let column = ColumnStatsBlockEntry {
			column_ordinal: 0x0102,
			arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
			stat_flags: COLUMN_STAT_HAS_MIN
				| COLUMN_STAT_HAS_MAX
				| COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT
				| COLUMN_STAT_MIN_TRUNCATED
				| COLUMN_STAT_MAX_TRUNCATED,
			payload,
		};

		let encoded = encode_column_stats_block(&header, &[column]).unwrap();
		assert_eq!(&encoded[0..8], b"WNDSTAT1");
		assert_eq!(&encoded[8..12], &[0, 0, 0, 1]);
		assert_eq!(&encoded[12..28], table_object_id.as_slice());
		assert_eq!(&encoded[28..36], &[1, 2, 3, 4, 5, 6, 7, 8]);
		assert_eq!(&encoded[36..68], schema_fingerprint.as_slice());
		assert_eq!(&encoded[68..76], &[0, 0, 0, 0, 0, 0, 0, 3]);
		assert_eq!(&encoded[76..80], &[0, 0, 0, 1]);
		assert_eq!(&encoded[80..84], &[0, 0, 0, 0]);
		assert_eq!(
			&encoded[84..93],
			&[
				0x01,
				0x02,
				ARROW_STATS_TAG_LARGE_UTF8,
				0x01,
				0x8F,
				0,
				0,
				0,
				28
			]
		);
		let column = COLUMN_STATS_BLOCK_HEADER_BYTES;
		assert_eq!(&encoded[column..column + 2], &[0x01, 0x02]);
		assert_eq!(encoded[column + 2], ARROW_STATS_TAG_LARGE_UTF8);
		assert_eq!(encoded[column + 3], 0x01);
		assert_eq!(encoded[column + 4], 0x8F);
		assert_eq!(&encoded[column + 5..column + 9], &[0, 0, 0, 28]);
		assert_eq!(
			&encoded[93..],
			&[
				0, 0, 0, 2, b'a', b'b', 0, 0, 0, 2, b'a', b'z', 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
				0, 0, 0, 2
			]
		);
		assert_eq!(parse_column_stats_block(&encoded).unwrap().columns.len(), 1);
	}

	#[test]
	fn parser_rejects_csb_header_reserved_bytes_trailing_bytes_and_impossible_count() {
		let header = ColumnStatsBlockHeader {
			table_object_id: TableObjectId::new([1; 16]),
			schema_generation: 1,
			schema_fingerprint: [2; 32],
			row_count: 0,
			column_block_count: 0,
		};
		let encoded = encode_column_stats_block(&header, &[]).unwrap();

		let mut reserved = encoded.clone();
		reserved[80] = 1;
		let err = parse_column_stats_block(&reserved).unwrap_err();
		assert!(
			err.to_string().contains("reserved header bytes"),
			"non-zero CSB header reserved bytes must reject: {err}"
		);

		let mut trailing = encoded.clone();
		trailing.push(0);
		let err = parse_column_stats_block(&trailing).unwrap_err();
		assert!(
			err.to_string().contains("trailing bytes"),
			"extra bytes after declared column blocks must reject: {err}"
		);

		let mut payload_missing_counts = Vec::new();
		encode_length_prefixed_bytes(
			&mut payload_missing_counts,
			&encode_signed_stat_value(1, 8).unwrap(),
		)
		.unwrap();
		let mut missing_counts = encoded.clone();
		missing_counts[76..80].copy_from_slice(&1_u32.to_be_bytes());
		missing_counts.extend_from_slice(&0_u16.to_be_bytes());
		missing_counts.push(ARROW_STATS_TAG_INT64);
		missing_counts.extend_from_slice(&COLUMN_STAT_HAS_MIN.to_be_bytes());
		missing_counts.extend_from_slice(&(payload_missing_counts.len() as u32).to_be_bytes());
		missing_counts.extend_from_slice(&payload_missing_counts);
		let err = parse_column_stats_block(&missing_counts).unwrap_err();
		assert!(
			err.to_string().contains("always-on null"),
			"parser must reject non-opaque entries without mandatory counts: {err}"
		);

		let mut impossible_count = encoded;
		impossible_count[76..80].copy_from_slice(&u32::MAX.to_be_bytes());
		let err = parse_column_stats_block(&impossible_count).unwrap_err();
		assert!(
			err.to_string()
				.contains("column_block_count exceeds remaining bytes"),
			"parser must reject impossible counts before allocation: {err}"
		);
	}

	#[test]
	fn compaction_identity_guard_rejects_descriptor_mismatch() {
		let header = ColumnStatsBlockHeader {
			table_object_id: TableObjectId::new([2; 16]),
			schema_generation: 9,
			schema_fingerprint: [3; 32],
			row_count: 4,
			column_block_count: 2,
		};
		let mut same_descriptor = header.clone();
		same_descriptor.row_count = 11;
		same_descriptor.column_block_count = 5;
		assert_eq!(validate_column_stats_compaction_inputs(&[]).unwrap(), None);
		assert_eq!(
			validate_column_stats_compaction_inputs(&[header.clone(), same_descriptor])
				.unwrap()
				.expect("descriptor identity"),
			ColumnStatsDescriptorIdentity::from_header(&header)
		);

		let mut mismatched = header.clone();
		mismatched.schema_fingerprint[0] ^= 0xFF;
		let err = validate_column_stats_compaction_inputs(&[header, mismatched]).unwrap_err();
		assert!(
			err.to_string().contains("descriptor identity mismatch"),
			"schema_fingerprint mismatch must reject as compaction eligibility: {err}"
		);
	}

	#[test]
	fn signed_stat_encoding_preserves_order_under_memcmp() {
		let values = [-128_i64, -1, 0, 1, 127];
		let encoded = values
			.into_iter()
			.map(|value| encode_signed_stat_value(value, 1).unwrap())
			.collect::<Vec<_>>();
		assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
		assert_eq!(encoded[0], vec![0x00]);
		assert_eq!(encoded[2], vec![0x80]);
		assert_eq!(encoded[4], vec![0xFF]);
	}

	#[test]
	fn canonical_numeric_bool_time_and_date_encodings_match_adr039_golden_bytes() {
		assert_eq!(encode_signed_stat_value(i8::MIN as i64, 1).unwrap(), [0x00]);
		assert_eq!(encode_signed_stat_value(-1, 1).unwrap(), [0x7F]);
		assert_eq!(encode_signed_stat_value(0, 1).unwrap(), [0x80]);
		assert_eq!(encode_signed_stat_value(i8::MAX as i64, 1).unwrap(), [0xFF]);
		assert_eq!(
			encode_signed_stat_value(i16::MIN as i64, 2).unwrap(),
			[0x00, 0x00]
		);
		assert_eq!(encode_signed_stat_value(-1, 2).unwrap(), [0x7F, 0xFF]);
		assert_eq!(encode_signed_stat_value(0, 2).unwrap(), [0x80, 0x00]);
		assert_eq!(
			encode_signed_stat_value(i16::MAX as i64, 2).unwrap(),
			[0xFF, 0xFF]
		);
		assert_eq!(
			encode_signed_stat_value(i32::MIN as i64, 4).unwrap(),
			[0x00, 0x00, 0x00, 0x00]
		);
		assert_eq!(
			encode_signed_stat_value(0, 4).unwrap(),
			[0x80, 0x00, 0x00, 0x00]
		);
		assert_eq!(
			encode_signed_stat_value(i32::MAX as i64, 4).unwrap(),
			[0xFF, 0xFF, 0xFF, 0xFF]
		);
		assert_eq!(
			encode_signed_stat_value(i64::MIN, 8).unwrap(),
			[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
		);
		assert_eq!(
			encode_signed_stat_value(0, 8).unwrap(),
			[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
		);
		assert_eq!(
			encode_signed_stat_value(i64::MAX, 8).unwrap(),
			[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
		);
		assert!(encode_signed_stat_value(128, 1).is_err());
		assert!(encode_signed_stat_value(-129, 1).is_err());

		assert_eq!(encode_unsigned_stat_value(0, 1).unwrap(), [0x00]);
		assert_eq!(encode_unsigned_stat_value(255, 1).unwrap(), [0xFF]);
		assert_eq!(encode_unsigned_stat_value(0x1234, 2).unwrap(), [0x12, 0x34]);
		assert_eq!(
			encode_unsigned_stat_value(0x89AB_CDEF, 4).unwrap(),
			[0x89, 0xAB, 0xCD, 0xEF]
		);
		assert_eq!(
			encode_unsigned_stat_value(0x0123_4567_89AB_CDEF, 8).unwrap(),
			[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]
		);
		assert!(encode_unsigned_stat_value(256, 1).is_err());

		assert_eq!(encode_bool_stat_value(false), [0x00]);
		assert_eq!(encode_bool_stat_value(true), [0x01]);

		let date32 = PrimitiveArray::<Date32Type>::from(vec![Some(-1), Some(2), None]);
		let (min, max, min_truncated, max_truncated) =
			min_max_for_array(&DataType::Date32, &date32, 64).unwrap();
		assert_eq!(min, Some(encode_signed_stat_value(-1, 4).unwrap()));
		assert_eq!(max, Some(encode_signed_stat_value(2, 4).unwrap()));
		assert!(!min_truncated);
		assert!(!max_truncated);

		let date64 = PrimitiveArray::<Date64Type>::from(vec![Some(-1000), Some(2000), None]);
		let (min, max, _, _) = min_max_for_array(&DataType::Date64, &date64, 64).unwrap();
		assert_eq!(min, Some(encode_signed_stat_value(-1000, 8).unwrap()));
		assert_eq!(max, Some(encode_signed_stat_value(2000, 8).unwrap()));

		let ts_ms =
			PrimitiveArray::<TimestampMillisecondType>::from(vec![Some(-10), Some(20), None]);
		let (min, max, _, _) = min_max_for_array(
			&DataType::Timestamp(TimeUnit::Millisecond, None),
			&ts_ms,
			64,
		)
		.unwrap();
		assert_eq!(min, Some(encode_signed_stat_value(-10, 8).unwrap()));
		assert_eq!(max, Some(encode_signed_stat_value(20, 8).unwrap()));

		let ts_us =
			PrimitiveArray::<TimestampMicrosecondType>::from(vec![Some(-11), Some(21), None]);
		let (min, max, _, _) = min_max_for_array(
			&DataType::Timestamp(TimeUnit::Microsecond, None),
			&ts_us,
			64,
		)
		.unwrap();
		assert_eq!(min, Some(encode_signed_stat_value(-11, 8).unwrap()));
		assert_eq!(max, Some(encode_signed_stat_value(21, 8).unwrap()));

		let ts_ns =
			PrimitiveArray::<TimestampNanosecondType>::from(vec![Some(-12), Some(22), None]);
		let (min, max, _, _) =
			min_max_for_array(&DataType::Timestamp(TimeUnit::Nanosecond, None), &ts_ns, 64)
				.unwrap();
		assert_eq!(min, Some(encode_signed_stat_value(-12, 8).unwrap()));
		assert_eq!(max, Some(encode_signed_stat_value(22, 8).unwrap()));
	}

	#[test]
	fn canonical_string_and_binary_min_max_use_raw_prefix_bytes() {
		let utf8 = StringArray::from(vec![Some("us-west"), Some("eu-central"), None]);
		let (min, max, min_truncated, max_truncated) =
			min_max_for_array(&DataType::Utf8, &utf8, 2).unwrap();
		assert_eq!(min, Some(b"eu".to_vec()));
		assert_eq!(max, Some(b"us".to_vec()));
		assert!(min_truncated);
		assert!(max_truncated);

		let large_utf8 = LargeStringArray::from(vec![Some("zulu"), Some("alpha"), None]);
		let (min, max, min_truncated, max_truncated) =
			min_max_for_array(&DataType::LargeUtf8, &large_utf8, 3).unwrap();
		assert_eq!(min, Some(b"alp".to_vec()));
		assert_eq!(max, Some(b"zul".to_vec()));
		assert!(min_truncated);
		assert!(max_truncated);

		let binary = BinaryArray::from(vec![
			Some(&b"\x90\x00\x00"[..]),
			Some(&b"\x10\xFF\xFF"[..]),
			None,
		]);
		let (min, max, min_truncated, max_truncated) =
			min_max_for_array(&DataType::Binary, &binary, 2).unwrap();
		assert_eq!(min, Some(vec![0x10, 0xFF]));
		assert_eq!(max, Some(vec![0x90, 0x00]));
		assert!(min_truncated);
		assert!(max_truncated);

		let large_binary = LargeBinaryArray::from(vec![
			Some(&b"\xFF\x10\x10"[..]),
			Some(&b"\x00\xEE\xEE"[..]),
			None,
		]);
		let (min, max, min_truncated, max_truncated) =
			min_max_for_array(&DataType::LargeBinary, &large_binary, 2).unwrap();
		assert_eq!(min, Some(vec![0x00, 0xEE]));
		assert_eq!(max, Some(vec![0xFF, 0x10]));
		assert!(min_truncated);
		assert!(max_truncated);
	}

	#[test]
	fn small_set_string_values_remain_full_canonical_bytes() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = stats_descriptor(&schema);
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			small_sets: vec![ColumnSmallSetSpecV1 {
				column_name: "region".to_owned(),
				max_distinct_values: SmallSetValueCap::new(4).unwrap(),
				max_bytes: SmallSetByteCap::new(128).unwrap(),
			}],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![
					Some("alpha-long"),
					Some("alpine-long"),
					Some("alpha-long"),
				])) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let encoded =
			build_always_on_column_stats_block_with_prefix_cap(&descriptor, &batch, 2).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		let region = &parsed.columns[1];
		assert_ne!(region.stat_flags & COLUMN_STAT_MIN_TRUNCATED, 0);
		assert_ne!(region.stat_flags & COLUMN_STAT_MAX_TRUNCATED, 0);
		let decoded = decode_column_stats_payload(region).unwrap();
		assert_eq!(decoded.min, Some(b"al".to_vec()));
		assert_eq!(decoded.max, Some(b"al".to_vec()));
		assert_eq!(
			decoded.small_set,
			Some(vec![b"alpha-long".to_vec(), b"alpine-long".to_vec()])
		);
	}

	#[test]
	fn bloom_writer_sets_inserted_values_and_payload_layout() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = stats_descriptor(&schema);
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			bloom_filters: vec![ColumnBloomSpecV1 {
				column_name: "region".to_owned(),
				bits_per_row: BloomBitsPerRow::new(16).unwrap(),
			}],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![Some("us"), None, Some("eu")]))
					as Arc<dyn Array>,
			],
		)
		.unwrap();

		let encoded = build_always_on_column_stats_block(&descriptor, &batch).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		let region = &parsed.columns[1];
		assert_ne!(region.stat_flags & COLUMN_STAT_HAS_BLOOM, 0);
		assert_eq!(
			locator_flags_for_parsed_column_stats_block(&parsed) & (1 << 2),
			1 << 2
		);
		let decoded = decode_column_stats_payload(region).unwrap();
		let bloom = decoded.bloom_filter.as_ref().expect("bloom filter");
		assert_eq!(bloom.bit_count, 48);
		assert_eq!(bloom.bit_array.len(), 6);
		assert!(bloom_may_contain_encoded_value(bloom, b"eu"));
		assert!(bloom_may_contain_encoded_value(bloom, b"us"));
		assert_eq!(
			prune_by_encoded_any_of(region, &[b"eu".to_vec()]).unwrap(),
			ColumnStatsPrune::MayMatch
		);
		let absent = (0..1024)
			.map(|index| format!("absent-{index}").into_bytes())
			.find(|value| !bloom_may_contain_encoded_value(bloom, value))
			.expect("test bloom should leave at least one absent value unset");
		assert_eq!(
			prune_by_encoded_any_of(region, &[absent]).unwrap(),
			ColumnStatsPrune::CannotMatch
		);

		let bloom_start = 4 + 2 + 4 + 2 + 8 + 8;
		assert_eq!(
			&region.payload[bloom_start..bloom_start + 4],
			&48_u32.to_be_bytes()
		);
		assert_eq!(region.payload.len(), bloom_start + 4 + 6);
		assert_eq!(
			&region.payload[bloom_start + 4..],
			bloom.bit_array.as_slice()
		);
	}

	#[test]
	fn bloom_writer_uses_row_budget_and_skips_nulls() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = stats_descriptor(&schema);
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			bloom_filters: vec![ColumnBloomSpecV1 {
				column_name: "region".to_owned(),
				bits_per_row: BloomBitsPerRow::new(5).unwrap(),
			}],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![
					None::<&str>,
					None::<&str>,
					None::<&str>,
				])) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let encoded = build_always_on_column_stats_block(&descriptor, &batch).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		let region = &parsed.columns[1];
		let decoded = decode_column_stats_payload(region).unwrap();
		assert_eq!(decoded.min, None);
		assert_eq!(decoded.max, None);
		assert_eq!(decoded.null_count, Some(3));
		assert_eq!(decoded.non_null_count, Some(0));
		let bloom = decoded.bloom_filter.as_ref().expect("bloom filter");
		assert_eq!(bloom.bit_count, 15);
		assert_eq!(bloom.bit_array.len(), 2);
		assert!(bloom.bit_array.iter().all(|byte| *byte == 0));
		assert!(!bloom_may_contain_encoded_value(bloom, b"eu"));
		assert_eq!(
			&region.payload[16..20],
			&15_u32.to_be_bytes(),
			"bloom payload follows null_count and non_null_count"
		);
		assert_eq!(region.payload.len(), 16 + 4 + 2);
	}

	#[test]
	fn hll_writer_sets_registers_deterministically_and_payload_layout() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = stats_descriptor(&schema);
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			hll_distincts: vec![ColumnHllSpecV1 {
				column_name: "id".to_owned(),
				precision: HllPrecision::new(COLUMN_STATS_HLL_MIN_PRECISION).unwrap(),
			}],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![
					Some(-7),
					Some(42),
					Some(42),
					Some(5),
				])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![
					Some("eu"),
					Some("us"),
					Some("us"),
					None,
				])) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let encoded = build_always_on_column_stats_block(&descriptor, &batch).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		let id = &parsed.columns[0];
		assert_ne!(id.stat_flags & COLUMN_STAT_HAS_HLL, 0);
		assert_eq!(
			locator_flags_for_parsed_column_stats_block(&parsed) & (1 << 3),
			1 << 3
		);
		let decoded = decode_column_stats_payload(id).unwrap();
		let hll = decoded.hll_distinct.as_ref().expect("HLL payload");
		assert_eq!(hll.precision, COLUMN_STATS_HLL_MIN_PRECISION);
		assert_eq!(hll.registers.len(), 1 << COLUMN_STATS_HLL_MIN_PRECISION);
		let nonzero = hll
			.registers
			.iter()
			.enumerate()
			.filter_map(|(index, register)| (*register != 0).then_some((index, *register)))
			.collect::<Vec<_>>();
		assert_eq!(nonzero, vec![(47, 7), (66, 1), (1011, 2)]);

		let hll_start = 4 + 8 + 4 + 8 + 8 + 8;
		assert_eq!(id.payload[hll_start], COLUMN_STATS_HLL_MIN_PRECISION);
		assert_eq!(
			id.payload.len(),
			hll_start + 1 + (1 << COLUMN_STATS_HLL_MIN_PRECISION)
		);
		assert_eq!(&id.payload[hll_start + 1..], hll.registers.as_slice());
	}

	#[test]
	fn hll_writer_estimate_tracks_distinct_non_null_values_coarsely() {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let mut descriptor = stats_descriptor(&schema);
		let hll_spec = ColumnHllSpecV1::new("id");
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			hll_distincts: vec![hll_spec.clone()],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		let mut values = (0..100).map(Some).collect::<Vec<Option<i64>>>();
		values.extend([Some(3), Some(3), Some(99)]);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![Arc::new(Int64Array::from(values)) as Arc<dyn Array>],
		)
		.unwrap();

		let encoded = build_always_on_column_stats_block(&descriptor, &batch).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		let decoded = decode_column_stats_payload(&parsed.columns[0]).unwrap();
		let hll = decoded.hll_distinct.as_ref().expect("HLL payload");
		assert_eq!(hll.precision, hll_spec.precision.get());
		let estimate = estimate_hll_distinct_count(hll).unwrap();
		assert!(
			(70.0..=140.0).contains(&estimate),
			"HLL estimate should be close enough for planner estimation, got {estimate}"
		);
		match distinct_count_for_column_ordinal(&parsed, 0).unwrap() {
			DistinctCount::Approx { estimate, std_err } => {
				assert!(
					(70..=140).contains(&estimate),
					"rounded HLL estimate should stay in planner sanity bounds, got {estimate}"
				);
				let expected_std_err = 1.04 / ((1_u32 << hll_spec.precision.get()) as f64).sqrt();
				assert!((std_err - expected_std_err).abs() < f64::EPSILON);
			}
			other => panic!("HLL-only column should expose approximate distinct count: {other:?}"),
		}
		assert_eq!(
			distinct_count_for_column_ordinal(&parsed, 99).unwrap(),
			DistinctCount::Unknown
		);
	}

	#[test]
	fn distinct_count_accessor_prefers_small_set_and_ignores_bloom() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = stats_descriptor(&schema);
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			bloom_filters: vec![ColumnBloomSpecV1 {
				column_name: "region".to_owned(),
				bits_per_row: BloomBitsPerRow::new(16).unwrap(),
			}],
			hll_distincts: vec![ColumnHllSpecV1::new("region")],
			small_sets: vec![ColumnSmallSetSpecV1::new("region")],
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![
					Some("us"),
					Some("eu"),
					Some("us"),
				])) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let encoded = build_always_on_column_stats_block(&descriptor, &batch).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		let region = &parsed.columns[1];
		assert_ne!(region.stat_flags & COLUMN_STAT_HAS_BLOOM, 0);
		assert_ne!(region.stat_flags & COLUMN_STAT_HAS_HLL, 0);
		assert_ne!(region.stat_flags & COLUMN_STAT_HAS_SMALL_SET, 0);
		let decoded = decode_column_stats_payload(region).unwrap();
		assert_eq!(
			distinct_count_for_column_stats_payload(&decoded).unwrap(),
			DistinctCount::Exact(2)
		);
		assert_eq!(
			distinct_count_for_column_ordinal(&parsed, 1).unwrap(),
			DistinctCount::Exact(2)
		);

		let bloom_only = DecodedColumnStatsPayload {
			min: None,
			max: None,
			null_count: None,
			non_null_count: None,
			bloom_filter: decoded.bloom_filter,
			hll_distinct: None,
			small_set: None,
		};
		assert_eq!(
			distinct_count_for_column_stats_payload(&bloom_only).unwrap(),
			DistinctCount::Unknown
		);
	}

	#[test]
	fn hll_distinct_is_not_used_for_correctness_pruning() {
		let mut payload = Vec::new();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&3_u64.to_be_bytes());
		payload.push(COLUMN_STATS_HLL_MIN_PRECISION);
		payload.resize(
			payload.len() + (1_usize << COLUMN_STATS_HLL_MIN_PRECISION),
			0,
		);
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_INT64,
			stat_flags: COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT
				| COLUMN_STAT_HAS_HLL,
			payload,
		};

		assert!(matches!(
			distinct_count_for_column_stats_entry(&entry).unwrap(),
			DistinctCount::Approx { .. }
		));
		let absent = encode_signed_stat_value(999, 8).unwrap();
		assert_eq!(
			prune_by_encoded_any_of(&entry, &[absent]).unwrap(),
			ColumnStatsPrune::MayMatch
		);
	}

	#[test]
	fn hll_spec_rejects_invalid_precision() {
		let err = HllPrecision::new(COLUMN_STATS_HLL_MIN_PRECISION - 1).unwrap_err();
		assert!(
			err.to_string().contains("HLL precision"),
			"invalid HLL precision must reject at construction: {err}"
		);
	}

	#[test]
	fn ciphertext_budget_trims_opt_in_stats_but_keeps_always_on_entries() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = stats_descriptor(&schema);
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			bloom_filters: vec![ColumnBloomSpecV1 {
				column_name: "region".to_owned(),
				bits_per_row: BloomBitsPerRow::new(16).unwrap(),
			}],
			hll_distincts: vec![ColumnHllSpecV1::new("region")],
			small_sets: vec![ColumnSmallSetSpecV1::new("region")],
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![
					Some("us"),
					Some("eu"),
					Some("us"),
				])) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let budgeted = build_column_stats_block_with_ciphertext_budget(
			&descriptor,
			&batch,
			COLUMN_STATS_DEFAULT_PREFIX_BYTES,
			0,
		)
		.unwrap();
		let parsed = parse_column_stats_block(&budgeted).unwrap();
		let region = &parsed.columns[1];
		assert_eq!(region.stat_flags & COLUMN_STAT_HAS_BLOOM, 0);
		assert_eq!(region.stat_flags & COLUMN_STAT_HAS_HLL, 0);
		assert_eq!(region.stat_flags & COLUMN_STAT_HAS_SMALL_SET, 0);
		let decoded = decode_column_stats_payload(region).unwrap();
		assert_eq!(decoded.min, Some(b"eu".to_vec()));
		assert_eq!(decoded.max, Some(b"us".to_vec()));
		assert_eq!(decoded.null_count, Some(0));
		assert_eq!(decoded.non_null_count, Some(3));

		let unbounded = build_column_stats_block_with_ciphertext_budget(
			&descriptor,
			&batch,
			COLUMN_STATS_DEFAULT_PREFIX_BYTES,
			usize::MAX,
		)
		.unwrap();
		let parsed = parse_column_stats_block(&unbounded).unwrap();
		let region = &parsed.columns[1];
		assert_ne!(region.stat_flags & COLUMN_STAT_HAS_BLOOM, 0);
		assert_ne!(region.stat_flags & COLUMN_STAT_HAS_HLL, 0);
		assert_ne!(region.stat_flags & COLUMN_STAT_HAS_SMALL_SET, 0);
	}

	#[test]
	fn parser_rejects_reserved_flags_and_reserved_type_tags() {
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: 0x09,
			stat_flags: 0,
			payload: Vec::new(),
		};
		assert!(validate_column_stats_entry(&entry).is_err());
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_INT64,
			stat_flags: 1 << 15,
			payload: Vec::new(),
		};
		assert!(validate_column_stats_entry(&entry).is_err());
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_INT64,
			stat_flags: COLUMN_STAT_MIN_TRUNCATED,
			payload: Vec::new(),
		};
		assert!(validate_column_stats_entry(&entry).is_err());
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(1, 8).unwrap())
			.unwrap();
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_INT64,
			stat_flags: COLUMN_STAT_HAS_MIN | COLUMN_STAT_MIN_TRUNCATED,
			payload,
		};
		assert!(validate_column_stats_entry(&entry).is_err());
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_OPAQUE_UNSUPPORTED,
			stat_flags: COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT
				| COLUMN_STAT_HAS_SMALL_SET,
			payload: Vec::new(),
		};
		assert!(validate_column_stats_entry(&entry).is_err());
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_OPAQUE_UNSUPPORTED,
			stat_flags: COLUMN_STAT_HAS_NULL_COUNT,
			payload: Vec::new(),
		};
		assert!(validate_column_stats_entry(&entry).is_err());
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(1, 8).unwrap())
			.unwrap();
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_INT64,
			stat_flags: COLUMN_STAT_HAS_MIN,
			payload,
		};
		let err = validate_column_stats_entry(&entry).unwrap_err();
		assert!(
			err.to_string().contains("always-on null"),
			"non-opaque entries must carry mandatory counts: {err}"
		);
	}

	#[test]
	fn decoder_rejects_unsorted_small_set_values() {
		let mut payload = Vec::new();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&2_u64.to_be_bytes());
		payload.extend_from_slice(&2_u32.to_be_bytes());
		encode_length_prefixed_bytes(&mut payload, b"us").unwrap();
		encode_length_prefixed_bytes(&mut payload, b"eu").unwrap();
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
			stat_flags: COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT
				| COLUMN_STAT_HAS_SMALL_SET,
			payload,
		};

		assert!(decode_column_stats_payload(&entry).is_err());
	}

	#[test]
	fn decoder_reads_per_stat_payloads_in_adr039_order() {
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, &[0x10]).unwrap();
		encode_length_prefixed_bytes(&mut payload, &[0x20, 0x21]).unwrap();
		payload.extend_from_slice(&1_u64.to_be_bytes());
		payload.extend_from_slice(&2_u64.to_be_bytes());
		payload.extend_from_slice(&10_u32.to_be_bytes());
		payload.extend_from_slice(&[0b1010_0000, 0b0100_0000]);
		payload.push(COLUMN_STATS_HLL_MIN_PRECISION);
		let hll_registers = vec![0x3C; 1 << COLUMN_STATS_HLL_MIN_PRECISION];
		payload.extend_from_slice(&hll_registers);
		payload.extend_from_slice(&2_u32.to_be_bytes());
		encode_length_prefixed_bytes(&mut payload, b"aa").unwrap();
		encode_length_prefixed_bytes(&mut payload, b"bb").unwrap();
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
			stat_flags: COLUMN_STAT_HAS_MIN
				| COLUMN_STAT_HAS_MAX
				| COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT
				| COLUMN_STAT_HAS_BLOOM
				| COLUMN_STAT_HAS_HLL
				| COLUMN_STAT_HAS_SMALL_SET,
			payload,
		};

		let decoded = decode_column_stats_payload(&entry).unwrap();
		assert_eq!(decoded.min, Some(vec![0x10]));
		assert_eq!(decoded.max, Some(vec![0x20, 0x21]));
		assert_eq!(decoded.null_count, Some(1));
		assert_eq!(decoded.non_null_count, Some(2));
		assert_eq!(
			decoded.bloom_filter,
			Some(ColumnStatsBloomPayload {
				bit_count: 10,
				bit_array: vec![0b1010_0000, 0b0100_0000],
			})
		);
		assert_eq!(
			decoded.hll_distinct,
			Some(ColumnStatsHllPayload {
				precision: COLUMN_STATS_HLL_MIN_PRECISION,
				registers: hll_registers,
			})
		);
		assert_eq!(
			decoded.small_set,
			Some(vec![b"aa".to_vec(), b"bb".to_vec()])
		);
	}

	#[test]
	fn decoder_rejects_truncated_bloom_and_invalid_hll_precision() {
		let mut payload = Vec::new();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&2_u64.to_be_bytes());
		payload.extend_from_slice(&9_u32.to_be_bytes());
		payload.push(0x80);
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_INT64,
			stat_flags: COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT
				| COLUMN_STAT_HAS_BLOOM,
			payload,
		};
		let err = decode_column_stats_payload(&entry).unwrap_err();
		assert!(
			err.to_string().contains("bloom bit_array"),
			"bit_count=9 requires two bloom bytes: {err}"
		);

		let mut payload = Vec::new();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&2_u64.to_be_bytes());
		payload.push(COLUMN_STATS_HLL_MIN_PRECISION - 1);
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_INT64,
			stat_flags: COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT
				| COLUMN_STAT_HAS_HLL,
			payload,
		};
		let err = decode_column_stats_payload(&entry).unwrap_err();
		assert!(
			err.to_string().contains("HLL precision"),
			"HLL precision outside the descriptor range must reject: {err}"
		);
	}

	#[test]
	fn type_tag_match_helper_rejects_descriptor_mismatch_and_opaque_entries() {
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_INT64,
			stat_flags: COLUMN_STAT_HAS_NULL_COUNT | COLUMN_STAT_HAS_NON_NULL_COUNT,
			payload: Vec::new(),
		};
		assert!(column_stats_entry_matches_data_type(
			&entry,
			&DataType::Int64
		));
		assert!(!column_stats_entry_matches_data_type(
			&entry,
			&DataType::LargeUtf8
		));
		let opaque = ColumnStatsBlockEntry {
			arrow_type_tag: ARROW_STATS_TAG_OPAQUE_UNSUPPORTED,
			..entry
		};
		assert!(!column_stats_entry_matches_data_type(
			&opaque,
			&DataType::Int64
		));
	}

	#[test]
	fn builder_derives_arrow_min_max_null_counts_and_string_prefixes() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let descriptor = stats_descriptor(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(-4), Some(7), Some(42)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![
					Some("us-west-long"),
					None,
					Some("eu-central"),
				])) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let encoded =
			build_always_on_column_stats_block_with_prefix_cap(&descriptor, &batch, 2).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		assert_eq!(parsed.header.table_object_id, descriptor.table_object_id);
		assert_eq!(
			parsed.header.schema_generation,
			descriptor.schema_generation
		);
		assert_eq!(
			parsed.header.schema_fingerprint,
			descriptor.schema_fingerprint
		);
		assert_eq!(parsed.header.row_count, 3);
		assert_eq!(parsed.columns.len(), 2);

		let id = decode_column_stats_payload(&parsed.columns[0]).unwrap();
		assert_eq!(id.min, Some(encode_signed_stat_value(-4, 8).unwrap()));
		assert_eq!(id.max, Some(encode_signed_stat_value(42, 8).unwrap()));
		assert_eq!(id.null_count, Some(0));
		assert_eq!(id.non_null_count, Some(3));

		let region = decode_column_stats_payload(&parsed.columns[1]).unwrap();
		assert_eq!(region.min, Some(b"eu".to_vec()));
		assert_eq!(region.max, Some(b"us".to_vec()));
		assert_eq!(region.null_count, Some(1));
		assert_eq!(region.non_null_count, Some(2));
		assert_ne!(parsed.columns[1].stat_flags & COLUMN_STAT_MIN_TRUNCATED, 0);
		assert_ne!(parsed.columns[1].stat_flags & COLUMN_STAT_MAX_TRUNCATED, 0);
	}

	#[test]
	fn prune_helper_rejects_ranges_outside_encoded_min_max() {
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(10, 8).unwrap())
			.unwrap();
		encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(20, 8).unwrap())
			.unwrap();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&3_u64.to_be_bytes());
		let entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_INT64,
			stat_flags: COLUMN_STAT_HAS_MIN
				| COLUMN_STAT_HAS_MAX
				| COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT,
			payload,
		};

		let lower = encode_signed_stat_value(21, 8).unwrap();
		assert_eq!(
			prune_by_encoded_min_max(&entry, Some(&lower), None).unwrap(),
			ColumnStatsPrune::CannotMatch
		);
		let upper = encode_signed_stat_value(19, 8).unwrap();
		assert_eq!(
			prune_by_encoded_min_max(&entry, None, Some(&upper)).unwrap(),
			ColumnStatsPrune::MayMatch
		);
	}

	#[test]
	fn prune_helper_uses_truncated_prefix_bounds_when_safe() {
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, b"ab").unwrap();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&3_u64.to_be_bytes());
		let max_entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
			stat_flags: COLUMN_STAT_HAS_MAX
				| COLUMN_STAT_MAX_TRUNCATED
				| COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT,
			payload,
		};
		assert_eq!(
			prune_by_encoded_min_max(&max_entry, Some(b"ac"), None).unwrap(),
			ColumnStatsPrune::CannotMatch
		);
		assert_eq!(
			prune_by_encoded_min_max(&max_entry, Some(b"abz"), None).unwrap(),
			ColumnStatsPrune::MayMatch
		);

		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, b"eu").unwrap();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&3_u64.to_be_bytes());
		let min_entry = ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
			stat_flags: COLUMN_STAT_HAS_MIN
				| COLUMN_STAT_MIN_TRUNCATED
				| COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT,
			payload,
		};
		assert_eq!(
			prune_by_encoded_min_max(&min_entry, None, Some(b"eu")).unwrap(),
			ColumnStatsPrune::CannotMatch
		);
		assert_eq!(
			prune_by_encoded_min_max(&min_entry, None, Some(b"ev")).unwrap(),
			ColumnStatsPrune::MayMatch
		);
	}

	#[test]
	fn builder_emits_opaque_entry_for_unstattable_column_type() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("score", DataType::Float64, true),
		]);
		let descriptor = stats_descriptor(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as Arc<dyn Array>,
				Arc::new(Float64Array::from(vec![Some(1.5), None, Some(2.5)])) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let encoded = build_always_on_column_stats_block(&descriptor, &batch).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		assert_eq!(parsed.columns.len(), 2);

		let opaque = &parsed.columns[1];
		assert_eq!(opaque.column_ordinal, 1);
		assert_eq!(opaque.arrow_type_tag, ARROW_STATS_TAG_OPAQUE_UNSUPPORTED);
		assert_eq!(
			opaque.stat_flags,
			COLUMN_STAT_HAS_NULL_COUNT | COLUMN_STAT_HAS_NON_NULL_COUNT
		);
		validate_column_stats_entry(opaque).unwrap();
		let decoded = decode_column_stats_payload(opaque).unwrap();
		assert_eq!(decoded.min, None);
		assert_eq!(decoded.max, None);
		assert_eq!(decoded.null_count, Some(1));
		assert_eq!(decoded.non_null_count, Some(2));
		assert_eq!(decoded.small_set, None);
		assert!(!column_stats_entry_matches_data_type(
			opaque,
			&DataType::Float64
		));
	}

	#[test]
	fn builder_emits_small_set_for_opt_in_and_prunes_exact_absence() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = stats_descriptor(&schema);
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			small_sets: vec![ColumnSmallSetSpecV1 {
				column_name: "region".to_owned(),
				max_distinct_values: SmallSetValueCap::new(4).unwrap(),
				max_bytes: SmallSetByteCap::new(64).unwrap(),
			}],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![
					Some("us"),
					Some("eu"),
					Some("us"),
				])) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let encoded = build_always_on_column_stats_block(&descriptor, &batch).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		assert_ne!(
			locator_flags_for_parsed_column_stats_block(&parsed) & (1 << 1),
			0
		);
		let region = &parsed.columns[1];
		assert_ne!(region.stat_flags & COLUMN_STAT_HAS_SMALL_SET, 0);
		let decoded = decode_column_stats_payload(region).unwrap();
		assert_eq!(
			decoded.small_set,
			Some(vec![b"eu".to_vec(), b"us".to_vec()])
		);
		assert_eq!(
			prune_by_encoded_any_of(region, &[b"mx".to_vec()]).unwrap(),
			ColumnStatsPrune::CannotMatch
		);
		assert_eq!(
			prune_by_encoded_any_of(region, &[b"us".to_vec()]).unwrap(),
			ColumnStatsPrune::MayMatch
		);
	}

	#[test]
	fn builder_omits_small_set_when_caps_are_exceeded() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = stats_descriptor(&schema);
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			small_sets: vec![ColumnSmallSetSpecV1 {
				column_name: "region".to_owned(),
				max_distinct_values: SmallSetValueCap::new(1).unwrap(),
				max_bytes: SmallSetByteCap::new(64).unwrap(),
			}],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![Some("eu"), Some("us")])) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let encoded = build_always_on_column_stats_block(&descriptor, &batch).unwrap();
		let parsed = parse_column_stats_block(&encoded).unwrap();
		let region = &parsed.columns[1];
		assert_eq!(region.stat_flags & COLUMN_STAT_HAS_SMALL_SET, 0);
		assert_eq!(decode_column_stats_payload(region).unwrap().small_set, None);
		assert_eq!(
			prune_by_encoded_any_of(region, &[b"mx".to_vec()]).unwrap(),
			ColumnStatsPrune::MayMatch
		);
	}
}
