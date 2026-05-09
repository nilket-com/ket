use crate::schema::pbc::{
	ColumnPredicateComparisonV1, ColumnPredicateLiteralV1, PredicateTreeV1, Program as PbcProgram,
};
use crate::schema::{
	EncryptedChunkCount, IpcBufferIndex, IpcBufferLength, IpcBufferOffset, IpcColumnOrdinal,
	LogicalSchemaFingerprint, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordRowIndex, RowCount, RowGroupIndex, RowGroupRowCount, SlabId,
};
use crate::storage::columnar::{
	ColumnFetchError, ColumnFetchRequirement, ColumnOrdinalMapping, ColumnarChunkRun,
	IpcBufferRange, ProjectedRecordBatch, RecordBufferDirectory, StorageColumn, TieredRecordId,
	decode_projected_record_batch_from_plaintext_ranges,
};
use crate::storage::stats::{
	CanonicalScalar, arrow_stats_type_tag, canonical_stats_for_array_slice, encode_bool_stat_value,
	encode_signed_stat_value, encode_unsigned_stat_value, read_canonical_scalar,
	write_canonical_scalar,
};
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::ipc::{BodyCompressionMethod, CompressionType, MessageHeader, root_as_message};
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Error, ErrorKind};
use std::sync::Arc;

pub const ROW_GROUP_DIRECTORY_METADATA_KEY: &str = "storage.row_group_directory";
pub const ROW_GROUP_DIRECTORY_MAGIC: [u8; 8] = *b"WNDRGPD1";
pub const ROW_GROUP_DIRECTORY_FORMAT_VERSION: u32 = 1;

pub const ROW_GROUP_POINT_READ_TARGET_ROWS: u32 = 4_096;
pub const ROW_GROUP_POINT_READ_MAX_ROWS: u32 = 8_192;
pub const ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS: u64 = 8_192;
pub const ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_BYTES: u64 = 512 * 1024;
pub const ROW_GROUP_SCAN_TARGET_ROWS: u32 = 65_536;
pub const ROW_GROUP_SCAN_MAX_ROWS: u32 = 131_072;
pub const ROW_GROUP_SCAN_MIN_PAGED_RECORD_ROWS: u64 = 65_536;
pub const ROW_GROUP_SCAN_MIN_PAGED_RECORD_BYTES: u64 = 1024 * 1024;

pub const ROW_GROUP_TARGET_ROWS_DEFAULT: u32 = ROW_GROUP_POINT_READ_TARGET_ROWS;
pub const ROW_GROUP_MAX_ROWS_DEFAULT: u32 = ROW_GROUP_POINT_READ_MAX_ROWS;
pub const ROW_GROUP_MIN_PAGED_RECORD_ROWS_DEFAULT: u64 = ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS;
pub const ROW_GROUP_MIN_PAGED_RECORD_BYTES_DEFAULT: u64 =
	ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_BYTES;
pub const L2_ROW_GROUP_FETCH_SELECTED_ROW_GROUP_FRACTION_MICROS_DEFAULT: u32 = 250_000;
pub const L2_ROW_GROUP_FETCH_SELECTED_BYTE_FRACTION_MICROS_DEFAULT: u32 = 500_000;
pub const L2_ROW_GROUP_FETCH_MAX_RANGES_PER_RECORD_DEFAULT: u32 = 16;
pub const L2_ROW_GROUP_FETCH_MAX_PARALLEL_RANGE_GETS_DEFAULT: u32 = 4;
pub const L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR: u32 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordBodyByteRange {
	pub start: RecordBodyByteOffset,
	pub len: RecordBodyByteLen,
}

impl RecordBodyByteRange {
	pub fn end(self) -> std::io::Result<u64> {
		self.start
			.get()
			.checked_add(self.len.get())
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "record body byte range overflow"))
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPageDirectory {
	pub record_id: TieredRecordId,
	pub row_count: RowCount,
	pub row_groups: Vec<RowGroupDirectoryEntry>,
	pub buffer_pages: Vec<BufferPageDirectoryEntry>,
	pub row_group_stats: Vec<RowGroupColumnStats>,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowGroupDirectoryEntry {
	pub row_group_index: RowGroupIndex,
	pub first_record_row_index: RecordRowIndex,
	pub row_count: RowGroupRowCount,
	pub metadata_range: RecordBodyByteRange,
	pub body_range: RecordBodyByteRange,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferPageDirectoryEntry {
	pub row_group_index: RowGroupIndex,
	pub ipc_column_ordinal: IpcColumnOrdinal,
	pub ipc_buffer_ordinal: IpcBufferIndex,
	pub range: RecordBodyByteRange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowGroupColumnStats {
	pub row_group_index: RowGroupIndex,
	pub ipc_column_ordinal: IpcColumnOrdinal,
	pub null_count: RowCount,
	pub min: Option<CanonicalScalar>,
	pub max: Option<CanonicalScalar>,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowGroupRejectCode {
	PageDirectoryParseError = 1,
	RowGroupRangeOverflow = 2,
	LayoutUnsupported = 3,
}

impl RowGroupRejectCode {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::PageDirectoryParseError => "page_directory_parse_error",
			Self::RowGroupRangeOverflow => "row_group_range_overflow",
			Self::LayoutUnsupported => "layout_unsupported",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RowGroupFallbackReason {
	Disabled,
	SingleRow,
	FullProjection,
	SmallRecord,
	TooFewRows,
	TooManyRowGroups,
	TooManyBytes,
	TooManyRanges,
	MetadataProbeCap,
	UnsupportedArrowLayout,
	RehydrationAvailable,
	UnpagedRecord,
}

impl RowGroupFallbackReason {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Disabled => "disabled",
			Self::SingleRow => "single_row",
			Self::FullProjection => "full_projection",
			Self::SmallRecord => "small_record",
			Self::TooFewRows => "too_few_rows",
			Self::TooManyRowGroups => "too_many_row_groups",
			Self::TooManyBytes => "too_many_bytes",
			Self::TooManyRanges => "too_many_ranges",
			Self::MetadataProbeCap => "metadata_probe_cap",
			Self::UnsupportedArrowLayout => "unsupported_arrow_layout",
			Self::RehydrationAvailable => "rehydration_available",
			Self::UnpagedRecord => "unpaged_record",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RowGroupSizingProfile {
	PointRead,
	Scan,
}

impl RowGroupSizingProfile {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::PointRead => "point_read",
			Self::Scan => "scan",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RowGroupSizingValues {
	target_row_group_rows: RowGroupRowCount,
	max_row_group_rows: RowGroupRowCount,
	min_paged_record_rows: RowCount,
	min_paged_record_bytes: RecordBodyByteLen,
}

const fn point_read_row_group_sizing_values() -> RowGroupSizingValues {
	RowGroupSizingValues {
		target_row_group_rows: RowGroupRowCount::new(ROW_GROUP_POINT_READ_TARGET_ROWS),
		max_row_group_rows: RowGroupRowCount::new(ROW_GROUP_POINT_READ_MAX_ROWS),
		min_paged_record_rows: RowCount::new(ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS),
		min_paged_record_bytes: RecordBodyByteLen::new(ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_BYTES),
	}
}

const fn scan_row_group_sizing_values() -> RowGroupSizingValues {
	RowGroupSizingValues {
		target_row_group_rows: RowGroupRowCount::new(ROW_GROUP_SCAN_TARGET_ROWS),
		max_row_group_rows: RowGroupRowCount::new(ROW_GROUP_SCAN_MAX_ROWS),
		min_paged_record_rows: RowCount::new(ROW_GROUP_SCAN_MIN_PAGED_RECORD_ROWS),
		min_paged_record_bytes: RecordBodyByteLen::new(ROW_GROUP_SCAN_MIN_PAGED_RECORD_BYTES),
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowGroupWriterConfig {
	pub enabled: bool,
	pub profile: RowGroupSizingProfile,
	sizing: RowGroupSizingValues,
	pub stats_supported_types: BTreeSet<DataType>,
}

impl Default for RowGroupWriterConfig {
	fn default() -> Self {
		Self::point_read()
	}
}

impl RowGroupWriterConfig {
	pub fn with_profile(profile: RowGroupSizingProfile) -> Self {
		let sizing = match profile {
			RowGroupSizingProfile::PointRead => point_read_row_group_sizing_values(),
			RowGroupSizingProfile::Scan => scan_row_group_sizing_values(),
		};
		Self {
			enabled: false,
			profile,
			sizing,
			stats_supported_types: default_stats_supported_types(),
		}
	}

	pub fn point_read() -> Self {
		Self::with_profile(RowGroupSizingProfile::PointRead)
	}

	pub fn scan() -> Self {
		Self::with_profile(RowGroupSizingProfile::Scan)
	}

	pub fn point_read_with_overrides(
		target_row_group_rows: RowGroupRowCount,
		max_row_group_rows: RowGroupRowCount,
		min_paged_record_rows: RowCount,
		min_paged_record_bytes: RecordBodyByteLen,
	) -> Self {
		Self {
			enabled: false,
			profile: RowGroupSizingProfile::PointRead,
			sizing: RowGroupSizingValues {
				target_row_group_rows,
				max_row_group_rows,
				min_paged_record_rows,
				min_paged_record_bytes,
			},
			stats_supported_types: default_stats_supported_types(),
		}
	}

	pub fn with_enabled(mut self, enabled: bool) -> Self {
		self.enabled = enabled;
		self
	}

	pub(crate) fn apply_point_read_overrides(
		&mut self,
		target_row_group_rows: Option<u32>,
		max_row_group_rows: Option<u32>,
		min_paged_record_rows: Option<u64>,
		min_paged_record_bytes: Option<u64>,
	) -> std::io::Result<()> {
		if self.profile != RowGroupSizingProfile::PointRead {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"row group numeric overrides are only valid for the point_read profile",
			));
		}
		if let Some(rows) = target_row_group_rows {
			self.sizing.target_row_group_rows = RowGroupRowCount::new(rows);
		}
		if let Some(rows) = max_row_group_rows {
			self.sizing.max_row_group_rows = RowGroupRowCount::new(rows);
		}
		if let Some(rows) = min_paged_record_rows {
			self.sizing.min_paged_record_rows = RowCount::new(rows);
		}
		if let Some(bytes) = min_paged_record_bytes {
			self.sizing.min_paged_record_bytes = RecordBodyByteLen::try_new(bytes)?;
		}
		Ok(())
	}

	pub fn target_row_group_rows(&self) -> RowGroupRowCount {
		self.sizing.target_row_group_rows
	}

	pub fn max_row_group_rows(&self) -> RowGroupRowCount {
		self.sizing.max_row_group_rows
	}

	pub fn min_paged_record_rows(&self) -> RowCount {
		self.sizing.min_paged_record_rows
	}

	pub fn min_paged_record_bytes(&self) -> RecordBodyByteLen {
		self.sizing.min_paged_record_bytes
	}

	pub fn validate(&self) -> std::io::Result<()> {
		if self.target_row_group_rows().get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"row group target rows must be greater than zero",
			));
		}
		if self.max_row_group_rows().get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"row group max rows must be greater than zero",
			));
		}
		if self.max_row_group_rows() < self.target_row_group_rows() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"row group max rows must not be below target rows",
			));
		}
		if self.min_paged_record_rows().get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"row group min paged record rows must be greater than zero",
			));
		}
		if self.min_paged_record_bytes().get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"row group min paged record bytes must be greater than zero",
			));
		}
		Ok(())
	}

	pub fn should_page(&self, row_count: usize, estimated_body_bytes: u64) -> bool {
		self.enabled
			&& row_count as u64 >= self.min_paged_record_rows().get()
			&& estimated_body_bytes >= self.min_paged_record_bytes().get()
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct L2RowGroupFetchConfig {
	pub enabled: bool,
	pub max_selected_row_group_fraction_micros: u32,
	pub max_selected_byte_fraction_micros: u32,
	pub max_ranges_per_record: EncryptedChunkCount,
	pub max_parallel_range_gets: EncryptedChunkCount,
}

impl Default for L2RowGroupFetchConfig {
	fn default() -> Self {
		Self {
			enabled: false,
			max_selected_row_group_fraction_micros:
				L2_ROW_GROUP_FETCH_SELECTED_ROW_GROUP_FRACTION_MICROS_DEFAULT,
			max_selected_byte_fraction_micros:
				L2_ROW_GROUP_FETCH_SELECTED_BYTE_FRACTION_MICROS_DEFAULT,
			max_ranges_per_record: EncryptedChunkCount::new(
				L2_ROW_GROUP_FETCH_MAX_RANGES_PER_RECORD_DEFAULT,
			),
			max_parallel_range_gets: EncryptedChunkCount::new(
				L2_ROW_GROUP_FETCH_MAX_PARALLEL_RANGE_GETS_DEFAULT,
			),
		}
	}
}

impl L2RowGroupFetchConfig {
	pub fn validate(&self) -> std::io::Result<()> {
		for (name, value) in [
			(
				"row-group selected row-group fraction",
				self.max_selected_row_group_fraction_micros,
			),
			(
				"row-group selected byte fraction",
				self.max_selected_byte_fraction_micros,
			),
		] {
			if value >= L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!("{name} must be < 1_000_000 micros"),
				));
			}
		}
		if self.max_ranges_per_record.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"row-group max ranges per record must be greater than zero",
			));
		}
		if self.max_parallel_range_gets.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"row-group max parallel range gets must be greater than zero",
			));
		}
		Ok(())
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowGroupFetchPlan {
	pub record_id: TieredRecordId,
	pub directory: RecordPageDirectory,
	pub surviving_groups: Vec<RowGroupIndex>,
	pub predicate_fetched_groups: Vec<RowGroupIndex>,
	pub phase_b_fetch: Option<RowGroupRangeFetchPlan>,
	pub phase_c_fetch: RowGroupRangeFetchPlan,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowGroupRangeFetchPlan {
	pub chunk_runs: Vec<ColumnarChunkRun>,
	pub selected_plaintext_ranges: Vec<RecordBodyByteRange>,
	pub fetched_ciphertext_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct PhaseBOutcome {
	pub decoded_predicate_batches: HashMap<RowGroupIndex, RecordBatch>,
	pub survival_masks: HashMap<RowGroupIndex, BooleanArray>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowGroupPredicateKind {
	Equality {
		col: IpcColumnOrdinal,
		value: CanonicalScalar,
	},
	RangeLt {
		col: IpcColumnOrdinal,
		value: CanonicalScalar,
	},
	RangeLe {
		col: IpcColumnOrdinal,
		value: CanonicalScalar,
	},
	RangeGt {
		col: IpcColumnOrdinal,
		value: CanonicalScalar,
	},
	RangeGe {
		col: IpcColumnOrdinal,
		value: CanonicalScalar,
	},
	IsNull {
		col: IpcColumnOrdinal,
	},
	IsNotNull {
		col: IpcColumnOrdinal,
	},
	And(Vec<RowGroupPredicateKind>),
	Or(Vec<RowGroupPredicateKind>),
	AlwaysTrue,
	Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowGroupClassification {
	Skip,
	Keep,
	NeedsPredicateFetch,
}

pub fn default_stats_supported_types() -> BTreeSet<DataType> {
	[
		DataType::Int8,
		DataType::Int16,
		DataType::Int32,
		DataType::Int64,
		DataType::UInt8,
		DataType::UInt16,
		DataType::UInt32,
		DataType::UInt64,
		DataType::Utf8,
		DataType::LargeUtf8,
		DataType::Binary,
		DataType::LargeBinary,
		DataType::Timestamp(TimeUnit::Millisecond, None),
		DataType::Timestamp(TimeUnit::Microsecond, None),
		DataType::Timestamp(TimeUnit::Nanosecond, None),
		DataType::Date32,
		DataType::Date64,
		DataType::Boolean,
	]
	.into_iter()
	.collect()
}

pub fn serialize_page_directory_value(directory: &RecordPageDirectory) -> std::io::Result<String> {
	let mut out = Vec::new();
	out.extend_from_slice(&ROW_GROUP_DIRECTORY_MAGIC);
	out.extend_from_slice(&ROW_GROUP_DIRECTORY_FORMAT_VERSION.to_be_bytes());
	write_u64(&mut out, directory.record_id.slab_id.get());
	write_u64(&mut out, directory.record_id.record_offset.get());
	write_u64(&mut out, directory.row_count.get());
	out.extend_from_slice(directory.logical_schema_fingerprint.as_bytes());
	write_u32(
		&mut out,
		checked_len(directory.row_groups.len(), "row group count")?,
	);
	write_u32(
		&mut out,
		checked_len(directory.buffer_pages.len(), "buffer page count")?,
	);
	write_u32(
		&mut out,
		checked_len(directory.row_group_stats.len(), "row group stats count")?,
	);
	for entry in &directory.row_groups {
		write_u32(&mut out, entry.row_group_index.get());
		write_u32(&mut out, entry.first_record_row_index.get());
		write_u32(&mut out, entry.row_count.get());
		write_range(&mut out, entry.metadata_range);
		write_range(&mut out, entry.body_range);
	}
	for entry in &directory.buffer_pages {
		write_u32(&mut out, entry.row_group_index.get());
		write_u32(&mut out, entry.ipc_column_ordinal.get());
		write_u32(&mut out, entry.ipc_buffer_ordinal.get());
		write_range(&mut out, entry.range);
	}
	for stat in &directory.row_group_stats {
		write_u32(&mut out, stat.row_group_index.get());
		write_u32(&mut out, stat.ipc_column_ordinal.get());
		write_u64(&mut out, stat.null_count.get());
		write_scalar(&mut out, stat.min.as_ref())?;
		write_scalar(&mut out, stat.max.as_ref())?;
	}
	Ok(BASE64.encode(out))
}

pub fn deserialize_page_directory_value(
	value: &str,
) -> Result<RecordPageDirectory, RowGroupRejectCode> {
	let bytes = BASE64
		.decode(value)
		.map_err(|_| RowGroupRejectCode::PageDirectoryParseError)?;
	let mut cursor = 0_usize;
	let magic = read_exact(&bytes, &mut cursor, ROW_GROUP_DIRECTORY_MAGIC.len())?;
	if magic != ROW_GROUP_DIRECTORY_MAGIC {
		return Err(RowGroupRejectCode::PageDirectoryParseError);
	}
	let version = read_u32(&bytes, &mut cursor)?;
	if version != ROW_GROUP_DIRECTORY_FORMAT_VERSION {
		return Err(RowGroupRejectCode::PageDirectoryParseError);
	}
	let slab_id = SlabId::new(read_u64(&bytes, &mut cursor)?);
	let record_offset = RecordBodyByteOffset::new(read_u64(&bytes, &mut cursor)?);
	let row_count = RowCount::new(read_u64(&bytes, &mut cursor)?);
	let fingerprint = read_exact(&bytes, &mut cursor, 32)?;
	let mut fingerprint_bytes = [0_u8; 32];
	fingerprint_bytes.copy_from_slice(fingerprint);
	let row_group_count = read_count(&bytes, &mut cursor)?;
	let buffer_page_count = read_count(&bytes, &mut cursor)?;
	let stats_count = read_count(&bytes, &mut cursor)?;
	let mut row_groups = Vec::with_capacity(row_group_count);
	for _ in 0..row_group_count {
		row_groups.push(RowGroupDirectoryEntry {
			row_group_index: RowGroupIndex::new(read_u32(&bytes, &mut cursor)?),
			first_record_row_index: RecordRowIndex::new(read_u32(&bytes, &mut cursor)?),
			row_count: RowGroupRowCount::new(read_u32(&bytes, &mut cursor)?),
			metadata_range: read_range(&bytes, &mut cursor)?,
			body_range: read_range(&bytes, &mut cursor)?,
		});
	}
	let mut buffer_pages = Vec::with_capacity(buffer_page_count);
	for _ in 0..buffer_page_count {
		buffer_pages.push(BufferPageDirectoryEntry {
			row_group_index: RowGroupIndex::new(read_u32(&bytes, &mut cursor)?),
			ipc_column_ordinal: IpcColumnOrdinal::new(read_u32(&bytes, &mut cursor)?),
			ipc_buffer_ordinal: IpcBufferIndex::new(read_u32(&bytes, &mut cursor)?),
			range: read_range(&bytes, &mut cursor)?,
		});
	}
	let mut row_group_stats = Vec::with_capacity(stats_count);
	for _ in 0..stats_count {
		row_group_stats.push(RowGroupColumnStats {
			row_group_index: RowGroupIndex::new(read_u32(&bytes, &mut cursor)?),
			ipc_column_ordinal: IpcColumnOrdinal::new(read_u32(&bytes, &mut cursor)?),
			null_count: RowCount::new(read_u64(&bytes, &mut cursor)?),
			min: read_scalar(&bytes, &mut cursor)?,
			max: read_scalar(&bytes, &mut cursor)?,
		});
	}
	if cursor != bytes.len() {
		return Err(RowGroupRejectCode::PageDirectoryParseError);
	}
	Ok(RecordPageDirectory {
		record_id: TieredRecordId {
			slab_id,
			record_offset,
		},
		row_count,
		row_groups,
		buffer_pages,
		row_group_stats,
		logical_schema_fingerprint: LogicalSchemaFingerprint::from_bytes(fingerprint_bytes),
	})
}

pub fn validate_page_directory(
	directory: &RecordPageDirectory,
	record_body_len: u64,
	schema: &Schema,
	expected_fingerprint: LogicalSchemaFingerprint,
) -> Result<(), RowGroupRejectCode> {
	if directory.logical_schema_fingerprint != expected_fingerprint {
		return Err(RowGroupRejectCode::PageDirectoryParseError);
	}
	let mut expected_first = 0_u64;
	let mut seen_groups = BTreeSet::new();
	for (expected_index, group) in directory.row_groups.iter().enumerate() {
		if group.row_group_index.get() as usize != expected_index {
			return Err(RowGroupRejectCode::PageDirectoryParseError);
		}
		if !seen_groups.insert(group.row_group_index) {
			return Err(RowGroupRejectCode::PageDirectoryParseError);
		}
		if u64::from(group.first_record_row_index.get()) != expected_first {
			return Err(RowGroupRejectCode::PageDirectoryParseError);
		}
		expected_first = expected_first
			.checked_add(u64::from(group.row_count.get()))
			.ok_or(RowGroupRejectCode::RowGroupRangeOverflow)?;
		validate_range_inside_record(group.metadata_range, record_body_len)?;
		validate_range_inside_record(group.body_range, record_body_len)?;
	}
	if expected_first != directory.row_count.get() {
		return Err(RowGroupRejectCode::PageDirectoryParseError);
	}
	if directory.row_count.get() == 0 || directory.row_groups.is_empty() {
		return Err(RowGroupRejectCode::PageDirectoryParseError);
	}
	let mut buffer_seen = BTreeSet::new();
	for page in &directory.buffer_pages {
		if !seen_groups.contains(&page.row_group_index) {
			return Err(RowGroupRejectCode::PageDirectoryParseError);
		}
		if page.ipc_column_ordinal.get() as usize >= schema.fields().len() {
			return Err(RowGroupRejectCode::LayoutUnsupported);
		}
		let group = &directory.row_groups[page.row_group_index.get() as usize];
		validate_range_inside_range(page.range, group.body_range)?;
		if !buffer_seen.insert((
			page.row_group_index,
			page.ipc_column_ordinal,
			page.ipc_buffer_ordinal,
		)) {
			return Err(RowGroupRejectCode::PageDirectoryParseError);
		}
	}
	for stat in &directory.row_group_stats {
		if !seen_groups.contains(&stat.row_group_index) {
			return Err(RowGroupRejectCode::PageDirectoryParseError);
		}
		let Some(field) = schema.fields().get(stat.ipc_column_ordinal.get() as usize) else {
			return Err(RowGroupRejectCode::LayoutUnsupported);
		};
		let Some(tag) = arrow_stats_type_tag(field.data_type()) else {
			return Err(RowGroupRejectCode::PageDirectoryParseError);
		};
		if stat
			.min
			.as_ref()
			.is_some_and(|scalar| scalar.arrow_type_tag != tag)
			|| stat
				.max
				.as_ref()
				.is_some_and(|scalar| scalar.arrow_type_tag != tag)
		{
			return Err(RowGroupRejectCode::PageDirectoryParseError);
		}
	}
	Ok(())
}

pub fn row_group_stats_for_batch(
	row_group_index: RowGroupIndex,
	batch: &RecordBatch,
	stats_supported_types: &BTreeSet<DataType>,
) -> std::io::Result<Vec<RowGroupColumnStats>> {
	let mut stats = Vec::new();
	for (ordinal, field) in batch.schema().fields().iter().enumerate() {
		if !stats_supported_types.contains(field.data_type()) {
			continue;
		}
		let column = batch.column(ordinal);
		let Some(values) = canonical_stats_for_array_slice(
			field.data_type(),
			column.as_ref(),
			0,
			batch.num_rows(),
		)?
		else {
			continue;
		};
		let ordinal = u32::try_from(ordinal).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"row-group stats column ordinal exceeds u32",
			)
		})?;
		stats.push(RowGroupColumnStats {
			row_group_index,
			ipc_column_ordinal: IpcColumnOrdinal::new(ordinal),
			null_count: RowCount::new(values.null_count),
			min: values.min,
			max: values.max,
		});
	}
	Ok(stats)
}

pub fn build_page_directory_for_ipc_stream(
	record_id: TieredRecordId,
	ipc_bytes: &[u8],
	batches: &[RecordBatch],
	logical_schema_fingerprint: LogicalSchemaFingerprint,
	stats_supported_types: &BTreeSet<DataType>,
) -> std::io::Result<RecordPageDirectory> {
	let Some(first_batch) = batches.first() else {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"row-group IPC stream requires at least one batch",
		));
	};
	let schema = first_batch.schema();
	let column_buffer_counts = schema
		.fields()
		.iter()
		.map(|field| buffer_count_for_data_type(field.data_type()))
		.collect::<std::io::Result<Vec<_>>>()?;
	let mut offset = 0_usize;
	let schema_message = read_ipc_message_metadata(ipc_bytes, &mut offset)?.ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"row-group IPC stream ended before schema message",
		)
	})?;
	if schema_message.header_type != MessageHeader::Schema {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"row-group IPC stream did not start with a schema message",
		));
	}
	let total_rows = batches.iter().try_fold(0_u64, |sum, batch| {
		sum.checked_add(batch.num_rows() as u64)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "row-group row count overflow"))
	})?;
	let mut row_groups = Vec::with_capacity(batches.len());
	let mut buffer_pages = Vec::new();
	let mut row_group_stats = Vec::new();
	let mut first_record_row_index = 0_u64;
	for (batch_index, batch) in batches.iter().enumerate() {
		if batch.schema().as_ref() != schema.as_ref() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"row-group IPC batches must share one schema",
			));
		}
		let message = read_ipc_message_metadata(ipc_bytes, &mut offset)?.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"row-group IPC stream ended before expected record batch",
			)
		})?;
		if message.header_type != MessageHeader::RecordBatch {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"row-group IPC stream contained non-record-batch after schema",
			));
		}
		let record_batch = message.message.header_as_record_batch().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"row-group IPC record batch missing header",
			)
		})?;
		let flat_buffers = record_batch.buffers().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"row-group IPC record batch missing buffers",
			)
		})?;
		let row_group_index = RowGroupIndex::new(batch_index as u32);
		let metadata_range = RecordBodyByteRange {
			start: RecordBodyByteOffset::try_new(message.prefix_start as u64)?,
			len: RecordBodyByteLen::try_new((message.body_start - message.prefix_start) as u64)?,
		};
		let body_range = RecordBodyByteRange {
			start: RecordBodyByteOffset::try_new(message.body_start as u64)?,
			len: RecordBodyByteLen::try_new(message.body_len as u64)?,
		};
		row_groups.push(RowGroupDirectoryEntry {
			row_group_index,
			first_record_row_index: RecordRowIndex::new(
				u32::try_from(first_record_row_index).map_err(|_| {
					Error::new(
						ErrorKind::InvalidData,
						"row-group first row index exceeds u32",
					)
				})?,
			),
			row_count: RowGroupRowCount::new(u32::try_from(batch.num_rows()).map_err(|_| {
				Error::new(ErrorKind::InvalidData, "row-group row count exceeds u32")
			})?),
			metadata_range,
			body_range,
		});
		let mut first_buffer_ordinal = 0_u32;
		for (column_ordinal, buffer_count) in column_buffer_counts.iter().copied().enumerate() {
			let column_ordinal = u32::try_from(column_ordinal).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"row-group column ordinal exceeds u32",
				)
			})?;
			for buffer_offset in 0..buffer_count {
				let buffer_ordinal =
					first_buffer_ordinal
						.checked_add(buffer_offset)
						.ok_or_else(|| {
							Error::new(ErrorKind::InvalidData, "row-group buffer ordinal overflow")
						})?;
				if buffer_ordinal as usize >= flat_buffers.len() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"row-group buffer ordinal out of range",
					));
				}
				let buffer = flat_buffers.get(buffer_ordinal as usize);
				let body_offset = u64::try_from(buffer.offset()).map_err(|_| {
					Error::new(ErrorKind::InvalidData, "negative row-group buffer offset")
				})?;
				let len = u64::try_from(buffer.length()).map_err(|_| {
					Error::new(ErrorKind::InvalidData, "negative row-group buffer length")
				})?;
				let start = (message.body_start as u64)
					.checked_add(body_offset)
					.ok_or_else(|| {
						Error::new(ErrorKind::InvalidData, "row-group buffer start overflow")
					})?;
				buffer_pages.push(BufferPageDirectoryEntry {
					row_group_index,
					ipc_column_ordinal: IpcColumnOrdinal::new(column_ordinal),
					ipc_buffer_ordinal: IpcBufferIndex::new(buffer_ordinal),
					range: RecordBodyByteRange {
						start: RecordBodyByteOffset::try_new(start)?,
						len: RecordBodyByteLen::try_new(len)?,
					},
				});
			}
			first_buffer_ordinal =
				first_buffer_ordinal
					.checked_add(buffer_count)
					.ok_or_else(|| {
						Error::new(ErrorKind::InvalidData, "row-group buffer count overflow")
					})?;
		}
		row_group_stats.extend(row_group_stats_for_batch(
			row_group_index,
			batch,
			stats_supported_types,
		)?);
		first_record_row_index = first_record_row_index
			.checked_add(batch.num_rows() as u64)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "row-group row cursor overflow"))?;
	}
	if read_ipc_message_metadata(ipc_bytes, &mut offset)?.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"row-group IPC stream has extra message after expected batches",
		));
	}
	Ok(RecordPageDirectory {
		record_id,
		row_count: RowCount::new(total_rows),
		row_groups,
		buffer_pages,
		row_group_stats,
		logical_schema_fingerprint,
	})
}

pub fn classify_row_groups(
	directory: &RecordPageDirectory,
	predicate: &RowGroupPredicateKind,
	config: &L2RowGroupFetchConfig,
) -> Vec<(RowGroupIndex, RowGroupClassification)> {
	directory
		.row_groups
		.iter()
		.map(|group| {
			let classification = classify_group(directory, group, predicate, config);
			(group.row_group_index, classification)
		})
		.collect()
}

pub fn classify_pbc_for_row_group_pruning(
	program: &PbcProgram,
	schema: &Schema,
) -> RowGroupPredicateKind {
	predicate_tree_to_row_group_kind(program.predicate_tree(), schema)
}

pub fn materialize_row_groups_from_batches(
	batches: &[RecordBatch],
	directory: &RecordPageDirectory,
	requirement: &ColumnFetchRequirement,
	row_groups: &[RowGroupIndex],
) -> std::io::Result<ProjectedRecordBatch> {
	let selected = row_groups.iter().copied().collect::<BTreeSet<_>>();
	let mut materialized = Vec::new();
	for (index, batch) in batches.iter().enumerate() {
		if !selected.contains(&RowGroupIndex::new(index as u32)) {
			continue;
		}
		materialized.push(project_record_batch_for_requirement(batch, requirement)?);
	}
	concat_projected_batches(materialized, directory)
}

pub fn row_group_for_record_row(
	directory: &RecordPageDirectory,
	record_row_index: RecordRowIndex,
) -> Option<RowGroupDirectoryEntry> {
	let row = u64::from(record_row_index.get());
	directory.row_groups.iter().copied().find(|group| {
		let start = u64::from(group.first_record_row_index.get());
		let end = start.saturating_add(u64::from(group.row_count.get()));
		row >= start && row < end
	})
}

pub fn row_groups_for_record_rows(
	directory: &RecordPageDirectory,
	record_row_indices: &[RecordRowIndex],
) -> std::io::Result<Vec<RowGroupIndex>> {
	let mut groups = BTreeSet::new();
	for row in record_row_indices {
		let group = row_group_for_record_row(directory, *row).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"record row index {} is not covered by row-group directory",
					row.get()
				),
			)
		})?;
		groups.insert(group.row_group_index);
	}
	Ok(groups.into_iter().collect())
}

pub fn selected_row_group_record_body_ranges(
	record_directory: &RecordBufferDirectory,
	page_directory: &RecordPageDirectory,
	requirement: &ColumnFetchRequirement,
	row_groups: &[RowGroupIndex],
) -> Result<Vec<RecordBodyByteRange>, ColumnFetchError> {
	selected_row_group_record_body_ranges_excluding_requirement(
		record_directory,
		page_directory,
		requirement,
		None,
		row_groups,
	)
}

pub fn selected_row_group_record_body_ranges_excluding_requirement(
	record_directory: &RecordBufferDirectory,
	page_directory: &RecordPageDirectory,
	requirement: &ColumnFetchRequirement,
	excluded_requirement: Option<&ColumnFetchRequirement>,
	row_groups: &[RowGroupIndex],
) -> Result<Vec<RecordBodyByteRange>, ColumnFetchError> {
	let selected_groups = row_groups.iter().copied().collect::<BTreeSet<_>>();
	let mut selected_buffers =
		selected_buffer_ordinals_for_requirement(record_directory, requirement)?;
	if let Some(excluded_requirement) = excluded_requirement {
		for buffer in
			selected_buffer_ordinals_for_requirement(record_directory, excluded_requirement)?
		{
			selected_buffers.remove(&buffer);
		}
	}
	let mut ranges = Vec::new();
	for group in &page_directory.row_groups {
		if !selected_groups.contains(&group.row_group_index) {
			continue;
		}
		ranges.push(group.metadata_range);
		for page in &page_directory.buffer_pages {
			if page.row_group_index == group.row_group_index
				&& selected_buffers.contains(&page.ipc_buffer_ordinal.get())
				&& page.range.len.get() > 0
			{
				ranges.push(encoding_fetch_range_for_buffer_page(
					record_directory,
					page,
				)?);
			}
		}
	}
	coalesce_record_body_ranges(&mut ranges)?;
	Ok(ranges)
}

fn encoding_fetch_range_for_buffer_page(
	record_directory: &RecordBufferDirectory,
	page: &BufferPageDirectoryEntry,
) -> Result<RecordBodyByteRange, ColumnFetchError> {
	let Some(encoding_directory) = record_directory.encoding_directory.as_ref() else {
		return Ok(page.range);
	};
	let Some(encoding) = encoding_directory.buffers.iter().find(|encoding| {
		encoding.row_group_index == page.row_group_index
			&& encoding.ipc_column_ordinal == page.ipc_column_ordinal
			&& encoding.ipc_buffer_ordinal == page.ipc_buffer_ordinal
	}) else {
		return Ok(page.range);
	};
	let encoded_start = u64::from(encoding.encoded_range.start.get());
	let encoded_end = encoding
		.encoded_range
		.end()
		.map_err(|err| ColumnFetchError::InvalidRange(format!("Encoding range: {err}")))?;
	if encoded_end > page.range.len.get() {
		return Err(ColumnFetchError::InvalidRange(format!(
			"Encoding range {}..{} exceeds decoded buffer length {}",
			encoded_start,
			encoded_end,
			page.range.len.get()
		)));
	}
	if let Some(cascade) = encoding_directory
		.cascades
		.get(encoding.cascade_index.get() as usize)
	{
		crate::metrics::global().observe_storage_l2_encoding_simd_pages_fetched(
			cascade.kind().as_str(),
			encoding.page_end_offsets.len() as u64,
		);
	}
	let start = page
		.range
		.start
		.get()
		.checked_add(encoded_start)
		.ok_or_else(|| ColumnFetchError::InvalidRange("Encoding range start overflows".into()))?;
	Ok(RecordBodyByteRange {
		start: RecordBodyByteOffset::new(start),
		len: encoding.encoded_range.len,
	})
}

pub fn row_group_phase_record_body_ranges(
	record_directory: &RecordBufferDirectory,
	page_directory: &RecordPageDirectory,
	phase_b_requirement: &ColumnFetchRequirement,
	phase_c_requirement: &ColumnFetchRequirement,
	row_groups: &[RowGroupIndex],
) -> Result<(Vec<RecordBodyByteRange>, Vec<RecordBodyByteRange>), ColumnFetchError> {
	let phase_b = selected_row_group_record_body_ranges(
		record_directory,
		page_directory,
		phase_b_requirement,
		row_groups,
	)?;
	let phase_c = selected_row_group_record_body_ranges_excluding_requirement(
		record_directory,
		page_directory,
		phase_c_requirement,
		Some(phase_b_requirement),
		row_groups,
	)?;
	Ok((phase_b, phase_c))
}

pub fn row_group_range_fetch_plan_from_tiered(
	tiered_plan: &crate::crypto::TieredMultiRangeReadPlan,
	selected_ranges: &[RecordBodyByteRange],
) -> RowGroupRangeFetchPlan {
	let fetched_ciphertext_bytes = tiered_plan
		.ciphertext_ranges_in_object()
		.into_iter()
		.map(|range| range.end.saturating_sub(range.start))
		.sum();
	RowGroupRangeFetchPlan {
		chunk_runs: tiered_plan
			.chunk_runs()
			.iter()
			.map(|run| ColumnarChunkRun {
				first_chunk: run.first_chunk(),
				chunk_count: run.chunk_count(),
			})
			.collect(),
		selected_plaintext_ranges: selected_ranges.to_vec(),
		fetched_ciphertext_bytes,
	}
}

pub fn decode_projected_row_groups_from_plaintext_ranges<B: AsRef<[u8]>>(
	record_directory: &RecordBufferDirectory,
	page_directory: &RecordPageDirectory,
	requirement: &ColumnFetchRequirement,
	selected_ranges: &[crate::crypto::TieredPlaintextRange],
	plaintext_extracts: &[B],
	row_groups: &[RowGroupIndex],
) -> Result<ProjectedRecordBatch, ArrowError> {
	let mut projected = Vec::new();
	for group_index in row_groups {
		let group = page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == *group_index)
			.ok_or_else(|| {
				ArrowError::SchemaError(format!(
					"row group {} missing from page directory",
					group_index.get()
				))
			})?;
		let metadata =
			extract_record_body_range(group.metadata_range, selected_ranges, plaintext_extracts)?;
		let batch_meta = parse_row_group_batch_metadata(&metadata, &record_directory.ipc_schema)?;
		let scoped = scoped_record_directory_for_row_group(
			record_directory,
			page_directory,
			group,
			&batch_meta,
		)?;
		projected.push(decode_projected_record_batch_from_plaintext_ranges(
			&scoped,
			requirement,
			selected_ranges,
			plaintext_extracts,
		)?);
	}
	concat_projected_batches(projected, page_directory)
		.map_err(|err| ArrowError::ExternalError(Box::new(err)))
}

pub fn apply_phase_b_masks_to_projected_row_groups(
	phase_c_batches: Vec<(RowGroupIndex, ProjectedRecordBatch)>,
	phase_b: &PhaseBOutcome,
	predicate_fetched_groups: &[RowGroupIndex],
	page_directory: &RecordPageDirectory,
) -> std::io::Result<ProjectedRecordBatch> {
	let predicate_fetched = predicate_fetched_groups
		.iter()
		.copied()
		.collect::<BTreeSet<_>>();
	let mut template = None;
	let mut filtered = Vec::new();
	for (group, projected) in phase_c_batches {
		template.get_or_insert_with(|| projected.clone());
		if !predicate_fetched.contains(&group) {
			filtered.push(projected);
			continue;
		}
		let mask = phase_b.survival_masks.get(&group).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"Phase B survival mask missing for row group {}",
					group.get()
				),
			)
		})?;
		if mask.len() != projected.batch.num_rows() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"Phase B survival mask length {} does not match row group {} batch length {}",
					mask.len(),
					group.get(),
					projected.batch.num_rows()
				),
			));
		}
		if mask.true_count() == 0 {
			continue;
		}
		if mask.true_count() == mask.len() && mask.null_count() == 0 {
			filtered.push(projected);
			continue;
		}
		let batch = compute::filter_record_batch(&projected.batch, mask).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("Phase B row-group mask filter failed: {err}"),
			)
		})?;
		filtered.push(ProjectedRecordBatch {
			batch,
			ordinal_mapping: projected.ordinal_mapping,
			storage_mapping: projected.storage_mapping,
		});
	}
	if filtered.is_empty() {
		if let Some(template) = template {
			return Ok(template.slice(0, 0));
		}
	}
	concat_projected_batches(filtered, page_directory)
}

pub fn empty_projected_row_group(row_count: usize) -> std::io::Result<ProjectedRecordBatch> {
	let batch = RecordBatch::try_new_with_options(
		Arc::new(Schema::empty()),
		Vec::new(),
		&arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count)),
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("empty projected row group failed: {err}"),
		)
	})?;
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: ColumnOrdinalMapping::default(),
		storage_mapping: BTreeMap::new(),
	})
}

pub fn zip_phase_b_and_phase_c_projected_row_group(
	phase_b: &ProjectedRecordBatch,
	phase_c: &ProjectedRecordBatch,
	requirement: &ColumnFetchRequirement,
) -> std::io::Result<ProjectedRecordBatch> {
	if phase_b.batch.num_rows() != phase_c.batch.num_rows() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"Phase B row count {} does not match Phase C row count {}",
				phase_b.batch.num_rows(),
				phase_c.batch.num_rows()
			),
		));
	}
	let descriptor_len = requirement
		.user_visible()
		.iter()
		.max()
		.map(|ordinal| ordinal as usize + 1)
		.unwrap_or(0);
	let mut descriptor_to_physical = vec![None; descriptor_len];
	let mut storage_mapping = BTreeMap::new();
	let mut fields = Vec::new();
	let mut arrays = Vec::new();
	for descriptor_ordinal in requirement.user_visible().iter() {
		let (field, array) = projected_descriptor_column(phase_b, phase_c, descriptor_ordinal)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!("Phase B/C zip missing descriptor column {descriptor_ordinal}"),
				)
			})?;
		descriptor_to_physical[descriptor_ordinal as usize] = Some(arrays.len());
		fields.push(field);
		arrays.push(array);
	}
	for storage_column in requirement.storage_columns.iter() {
		let (field, array) = projected_storage_column(phase_b, phase_c, storage_column)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!("Phase B/C zip missing storage column {storage_column:?}"),
				)
			})?;
		storage_mapping.insert(storage_column, arrays.len());
		fields.push(field);
		arrays.push(array);
	}
	let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("Phase B/C projected batch zip failed: {err}"),
		)
	})?;
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: ColumnOrdinalMapping::new(descriptor_to_physical),
		storage_mapping,
	})
}

fn projected_descriptor_column(
	phase_b: &ProjectedRecordBatch,
	phase_c: &ProjectedRecordBatch,
	descriptor_ordinal: u32,
) -> Option<(arrow::datatypes::Field, ArrayRef)> {
	projected_descriptor_column_from(phase_b, descriptor_ordinal)
		.or_else(|| projected_descriptor_column_from(phase_c, descriptor_ordinal))
}

fn projected_descriptor_column_from(
	projected: &ProjectedRecordBatch,
	descriptor_ordinal: u32,
) -> Option<(arrow::datatypes::Field, ArrayRef)> {
	let physical = projected.physical_for(descriptor_ordinal)?;
	Some((
		projected.batch.schema().field(physical).clone(),
		projected.batch.column(physical).clone(),
	))
}

fn projected_storage_column(
	phase_b: &ProjectedRecordBatch,
	phase_c: &ProjectedRecordBatch,
	storage_column: StorageColumn,
) -> Option<(arrow::datatypes::Field, ArrayRef)> {
	projected_storage_column_from(phase_b, storage_column)
		.or_else(|| projected_storage_column_from(phase_c, storage_column))
}

fn projected_storage_column_from(
	projected: &ProjectedRecordBatch,
	storage_column: StorageColumn,
) -> Option<(arrow::datatypes::Field, ArrayRef)> {
	let physical = projected.storage_physical_for(storage_column)?;
	Some((
		projected.batch.schema().field(physical).clone(),
		projected.batch.column(physical).clone(),
	))
}

pub fn project_record_batch_for_requirement(
	batch: &RecordBatch,
	requirement: &ColumnFetchRequirement,
) -> std::io::Result<ProjectedRecordBatch> {
	let schema = batch.schema();
	let user_physical_ordinals = schema
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(physical, field)| {
			StorageColumn::from_name(field.name())
				.is_none()
				.then_some(physical)
		})
		.collect::<Vec<_>>();
	let mut descriptor_to_physical = vec![None; user_physical_ordinals.len()];
	let mut storage_mapping = BTreeMap::new();
	let mut projection = Vec::new();
	for descriptor_ordinal in requirement.user_visible().iter() {
		let descriptor_index = descriptor_ordinal as usize;
		let physical = *user_physical_ordinals
			.get(descriptor_index)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!("row-group descriptor ordinal {descriptor_ordinal} out of range"),
				)
			})?;
		descriptor_to_physical[descriptor_index] = Some(projection.len());
		projection.push(physical);
	}
	for storage_column in requirement.storage_columns.iter() {
		let physical = schema
			.fields()
			.iter()
			.position(|field| StorageColumn::from_name(field.name()) == Some(storage_column))
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!("row-group storage column {storage_column:?} missing"),
				)
			})?;
		storage_mapping.insert(storage_column, projection.len());
		projection.push(physical);
	}
	let projected = batch.project(&projection).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("row-group batch project failed: {err}"),
		)
	})?;
	Ok(ProjectedRecordBatch {
		batch: projected,
		ordinal_mapping: ColumnOrdinalMapping::new(descriptor_to_physical),
		storage_mapping,
	})
}

fn predicate_tree_to_row_group_kind(
	predicate: PredicateTreeV1,
	schema: &Schema,
) -> RowGroupPredicateKind {
	match predicate {
		PredicateTreeV1::Comparison(predicate) => {
			let Some(field) = schema.fields().get(predicate.column_index) else {
				return RowGroupPredicateKind::Unsupported;
			};
			let Some(value) = literal_to_canonical(field.data_type(), &predicate.literal) else {
				return RowGroupPredicateKind::Unsupported;
			};
			let col = IpcColumnOrdinal::new(predicate.column_index as u32);
			match predicate.comparison {
				ColumnPredicateComparisonV1::Eq => RowGroupPredicateKind::Equality { col, value },
				ColumnPredicateComparisonV1::Gt => RowGroupPredicateKind::RangeGt { col, value },
				ColumnPredicateComparisonV1::Gte => RowGroupPredicateKind::RangeGe { col, value },
				ColumnPredicateComparisonV1::Lt => RowGroupPredicateKind::RangeLt { col, value },
				ColumnPredicateComparisonV1::Lte => RowGroupPredicateKind::RangeLe { col, value },
				ColumnPredicateComparisonV1::InList => RowGroupPredicateKind::Unsupported,
			}
		}
		PredicateTreeV1::IsNull { column_index } => RowGroupPredicateKind::IsNull {
			col: IpcColumnOrdinal::new(column_index as u32),
		},
		PredicateTreeV1::IsNotNull { column_index } => RowGroupPredicateKind::IsNotNull {
			col: IpcColumnOrdinal::new(column_index as u32),
		},
		PredicateTreeV1::And(branches) => RowGroupPredicateKind::And(
			branches
				.into_iter()
				.map(|branch| predicate_tree_to_row_group_kind(branch, schema))
				.collect(),
		),
		PredicateTreeV1::Or(branches) => RowGroupPredicateKind::Or(
			branches
				.into_iter()
				.map(|branch| predicate_tree_to_row_group_kind(branch, schema))
				.collect(),
		),
		PredicateTreeV1::AlwaysTrue => RowGroupPredicateKind::AlwaysTrue,
		PredicateTreeV1::Unsupported => RowGroupPredicateKind::Unsupported,
	}
}

fn literal_to_canonical(
	data_type: &DataType,
	literal: &ColumnPredicateLiteralV1,
) -> Option<CanonicalScalar> {
	let tag = arrow_stats_type_tag(data_type)?;
	let bytes = match (data_type, literal) {
		(DataType::Int8, ColumnPredicateLiteralV1::Int(value)) => {
			encode_signed_stat_value(*value, 1).ok()?
		}
		(DataType::Int16, ColumnPredicateLiteralV1::Int(value)) => {
			encode_signed_stat_value(*value, 2).ok()?
		}
		(DataType::Int32, ColumnPredicateLiteralV1::Int(value)) => {
			encode_signed_stat_value(*value, 4).ok()?
		}
		(DataType::Int64, ColumnPredicateLiteralV1::Int(value)) => {
			encode_signed_stat_value(*value, 8).ok()?
		}
		(DataType::UInt8, ColumnPredicateLiteralV1::Int(value)) => {
			encode_unsigned_stat_value(u64::try_from(*value).ok()?, 1).ok()?
		}
		(DataType::UInt16, ColumnPredicateLiteralV1::Int(value)) => {
			encode_unsigned_stat_value(u64::try_from(*value).ok()?, 2).ok()?
		}
		(DataType::UInt32, ColumnPredicateLiteralV1::Int(value)) => {
			encode_unsigned_stat_value(u64::try_from(*value).ok()?, 4).ok()?
		}
		(DataType::UInt64, ColumnPredicateLiteralV1::Int(value)) => {
			encode_unsigned_stat_value(u64::try_from(*value).ok()?, 8).ok()?
		}
		(DataType::Date32, ColumnPredicateLiteralV1::Int(value)) => {
			encode_signed_stat_value(*value, 4).ok()?
		}
		(DataType::Date64, ColumnPredicateLiteralV1::Int(value)) => {
			encode_signed_stat_value(*value, 8).ok()?
		}
		(DataType::Timestamp(TimeUnit::Millisecond, _), ColumnPredicateLiteralV1::Ts(value))
		| (DataType::Timestamp(TimeUnit::Microsecond, _), ColumnPredicateLiteralV1::Ts(value))
		| (DataType::Timestamp(TimeUnit::Nanosecond, _), ColumnPredicateLiteralV1::Ts(value)) => {
			encode_signed_stat_value(*value, 8).ok()?
		}
		(DataType::Utf8 | DataType::LargeUtf8, ColumnPredicateLiteralV1::Str(bytes))
		| (DataType::Binary | DataType::LargeBinary, ColumnPredicateLiteralV1::Str(bytes)) => {
			bytes.clone()
		}
		(DataType::Boolean, ColumnPredicateLiteralV1::Int(value)) => {
			encode_bool_stat_value(*value != 0).to_vec()
		}
		_ => return None,
	};
	Some(CanonicalScalar {
		arrow_type_tag: tag,
		bytes,
		truncated: false,
	})
}

fn concat_projected_batches(
	batches: Vec<ProjectedRecordBatch>,
	directory: &RecordPageDirectory,
) -> std::io::Result<ProjectedRecordBatch> {
	let Some(first) = batches.first() else {
		return empty_projected_row_group(0);
	};
	let schema = first.batch.schema();
	let raw = batches
		.iter()
		.map(|batch| batch.batch.clone())
		.collect::<Vec<_>>();
	let concatenated = arrow::compute::concat_batches(&schema, &raw).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("row-group concat failed: {err}"),
		)
	})?;
	let _ = directory;
	Ok(ProjectedRecordBatch {
		batch: concatenated,
		ordinal_mapping: first.ordinal_mapping.clone(),
		storage_mapping: first.storage_mapping.clone(),
	})
}

struct RowGroupBatchMetadata {
	row_count: usize,
	record_body_compression: RecordBodyCompressionPolicy,
	column_null_counts: Vec<usize>,
}

fn selected_buffer_ordinals_for_requirement(
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
) -> Result<BTreeSet<u32>, ColumnFetchError> {
	let mut out = BTreeSet::new();
	for ordinal in requirement.user_visible().iter() {
		insert_column_buffer_ordinals(directory, ordinal, &mut out)?;
	}
	for storage_column in requirement.storage_columns.iter() {
		let ordinal = directory
			.storage_column_ordinals
			.get(&storage_column)
			.copied()
			.ok_or(ColumnFetchError::UnsupportedArrowLayout)?;
		insert_column_buffer_ordinals(directory, ordinal, &mut out)?;
	}
	Ok(out)
}

fn insert_column_buffer_ordinals(
	directory: &RecordBufferDirectory,
	column_ordinal: u32,
	out: &mut BTreeSet<u32>,
) -> Result<(), ColumnFetchError> {
	let mapping = directory
		.column_to_buffers
		.get(column_ordinal as usize)
		.ok_or_else(|| ColumnFetchError::InvalidRange("column ordinal out of range".into()))?;
	let first = mapping.first_buffer_ordinal.get();
	let end = first
		.checked_add(mapping.buffer_count)
		.ok_or_else(|| ColumnFetchError::InvalidRange("IPC buffer ordinal overflow".into()))?;
	out.extend(first..end);
	Ok(())
}

fn coalesce_record_body_ranges(
	ranges: &mut Vec<RecordBodyByteRange>,
) -> Result<(), ColumnFetchError> {
	ranges.sort_by_key(|range| range.start.get());
	let mut coalesced: Vec<RecordBodyByteRange> = Vec::with_capacity(ranges.len());
	for range in ranges.drain(..) {
		if range.len.get() == 0 {
			continue;
		}
		let Some(last) = coalesced.last_mut() else {
			coalesced.push(range);
			continue;
		};
		let last_end = last
			.end()
			.map_err(|err| ColumnFetchError::InvalidRange(err.to_string()))?;
		let range_end = range
			.end()
			.map_err(|err| ColumnFetchError::InvalidRange(err.to_string()))?;
		if range.start.get() <= last_end {
			let merged_end = last_end.max(range_end);
			last.len = RecordBodyByteLen::try_new(merged_end - last.start.get())
				.map_err(|err| ColumnFetchError::InvalidRange(err.to_string()))?;
		} else {
			coalesced.push(range);
		}
	}
	*ranges = coalesced;
	Ok(())
}

fn extract_record_body_range<B: AsRef<[u8]>>(
	range: RecordBodyByteRange,
	selected_ranges: &[crate::crypto::TieredPlaintextRange],
	plaintext_extracts: &[B],
) -> Result<Vec<u8>, ArrowError> {
	if selected_ranges.len() != plaintext_extracts.len() {
		return Err(ArrowError::InvalidArgumentError(format!(
			"row-group materializer expected {} plaintext extracts, got {}",
			selected_ranges.len(),
			plaintext_extracts.len()
		)));
	}
	let start = range.start.get();
	let end = range
		.end()
		.map_err(|err| ArrowError::SchemaError(err.to_string()))?;
	for (candidate, extract) in selected_ranges.iter().zip(plaintext_extracts.iter()) {
		let candidate_start = candidate.start.get();
		let candidate_end = candidate_start
			.checked_add(candidate.len.get())
			.ok_or_else(|| ArrowError::SchemaError("plaintext range overflow".into()))?;
		if start >= candidate_start && end <= candidate_end {
			let relative_start = usize::try_from(start - candidate_start).map_err(|_| {
				ArrowError::SchemaError("metadata relative start exceeds usize".into())
			})?;
			let relative_end = usize::try_from(end - candidate_start).map_err(|_| {
				ArrowError::SchemaError("metadata relative end exceeds usize".into())
			})?;
			let extract = extract.as_ref();
			if relative_end > extract.len() {
				return Err(ArrowError::SchemaError(
					"metadata range exceeds plaintext extract".into(),
				));
			}
			return Ok(extract[relative_start..relative_end].to_vec());
		}
	}
	Err(ArrowError::SchemaError(
		"row-group metadata range was not fetched".into(),
	))
}

fn parse_row_group_batch_metadata(
	bytes: &[u8],
	schema: &Schema,
) -> Result<RowGroupBatchMetadata, ArrowError> {
	let mut offset = 0_usize;
	if bytes.len().saturating_sub(offset) < 4 {
		return Err(ArrowError::IpcError(
			"row-group metadata prefix truncated".into(),
		));
	}
	let first = u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed width"));
	offset += 4;
	let metadata_len = if first == u32::MAX {
		if bytes.len().saturating_sub(offset) < 4 {
			return Err(ArrowError::IpcError(
				"row-group continuation prefix truncated".into(),
			));
		}
		let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed width"));
		offset += 4;
		len as usize
	} else {
		first as usize
	};
	let metadata_end = offset
		.checked_add(metadata_len)
		.ok_or_else(|| ArrowError::IpcError("row-group metadata length overflow".into()))?;
	if metadata_end != bytes.len() {
		return Err(ArrowError::IpcError(format!(
			"row-group metadata range length mismatch: metadata_end={metadata_end} bytes={}",
			bytes.len()
		)));
	}
	let message = root_as_message(&bytes[offset..metadata_end])
		.map_err(|err| ArrowError::IpcError(format!("row-group metadata parse: {err}")))?;
	if message.header_type() != MessageHeader::RecordBatch {
		return Err(ArrowError::IpcError(
			"row-group metadata is not a RecordBatch message".into(),
		));
	}
	let record_batch = message
		.header_as_record_batch()
		.ok_or_else(|| ArrowError::IpcError("row-group metadata missing batch header".into()))?;
	let row_count = usize::try_from(record_batch.length())
		.map_err(|_| ArrowError::IpcError("row-group metadata has negative row count".into()))?;
	let record_body_compression = match record_batch.compression() {
		None => RecordBodyCompressionPolicy::Uncompressed,
		Some(compression) => {
			if compression.method() != BodyCompressionMethod::BUFFER {
				return Err(ArrowError::IpcError(
					"row-group metadata has unsupported compression method".into(),
				));
			}
			match compression.codec() {
				CompressionType::LZ4_FRAME => RecordBodyCompressionPolicy::Lz4Frame,
				_ => {
					return Err(ArrowError::IpcError(
						"row-group metadata has unsupported compression codec".into(),
					));
				}
			}
		}
	};
	let nodes = record_batch
		.nodes()
		.ok_or_else(|| ArrowError::IpcError("row-group metadata missing field nodes".into()))?;
	let column_null_counts = nodes
		.iter()
		.map(|node| {
			usize::try_from(node.null_count()).map_err(|_| {
				ArrowError::IpcError("row-group metadata has negative null count".into())
			})
		})
		.collect::<Result<Vec<_>, _>>()?;
	if column_null_counts.len() != schema.fields().len() {
		return Err(ArrowError::SchemaError(format!(
			"row-group metadata node count {} does not match schema field count {}",
			column_null_counts.len(),
			schema.fields().len()
		)));
	}
	Ok(RowGroupBatchMetadata {
		row_count,
		record_body_compression,
		column_null_counts,
	})
}

fn scoped_record_directory_for_row_group(
	record_directory: &RecordBufferDirectory,
	page_directory: &RecordPageDirectory,
	group: &RowGroupDirectoryEntry,
	batch_meta: &RowGroupBatchMetadata,
) -> Result<RecordBufferDirectory, ArrowError> {
	let expected_rows = group.row_count.get() as usize;
	if batch_meta.row_count != expected_rows {
		return Err(ArrowError::SchemaError(format!(
			"row-group metadata row_count {} does not match directory row_count {expected_rows}",
			batch_meta.row_count
		)));
	}
	let total_buffers = record_directory
		.column_to_buffers
		.iter()
		.map(|mapping| {
			mapping
				.first_buffer_ordinal
				.get()
				.saturating_add(mapping.buffer_count)
		})
		.max()
		.unwrap_or(0) as usize;
	let mut buffers = vec![
		IpcBufferRange {
			offset_in_record: IpcBufferOffset::new(0),
			length: IpcBufferLength::new(0),
		};
		total_buffers
	];
	for page in page_directory
		.buffer_pages
		.iter()
		.filter(|page| page.row_group_index == group.row_group_index)
	{
		let index = page.ipc_buffer_ordinal.get() as usize;
		let Some(slot) = buffers.get_mut(index) else {
			return Err(ArrowError::SchemaError(format!(
				"row-group buffer ordinal {index} out of scoped directory range {total_buffers}"
			)));
		};
		*slot = IpcBufferRange {
			offset_in_record: IpcBufferOffset::new(page.range.start.get()),
			length: IpcBufferLength::new(page.range.len.get()),
		};
	}
	Ok(RecordBufferDirectory {
		record_id: page_directory.record_id,
		record_plaintext_range: record_directory.record_plaintext_range,
		row_count: batch_meta.row_count,
		record_body_compression: batch_meta.record_body_compression,
		ipc_schema: record_directory.ipc_schema.clone(),
		buffers,
		column_to_buffers: record_directory.column_to_buffers.clone(),
		column_null_counts: batch_meta.column_null_counts.clone(),
		storage_column_ordinals: record_directory.storage_column_ordinals.clone(),
		logical_schema_fingerprint: record_directory.logical_schema_fingerprint,
		page_directory: Some(page_directory.clone()),
		encoding_directory: record_directory.encoding_directory.clone(),
	})
}

fn classify_group(
	directory: &RecordPageDirectory,
	group: &RowGroupDirectoryEntry,
	predicate: &RowGroupPredicateKind,
	config: &L2RowGroupFetchConfig,
) -> RowGroupClassification {
	match predicate {
		RowGroupPredicateKind::AlwaysTrue | RowGroupPredicateKind::Unsupported => {
			RowGroupClassification::Keep
		}
		RowGroupPredicateKind::Equality { col, value } => classify_min_max(
			directory,
			group,
			*col,
			Some((value, true)),
			Some((value, true)),
			config,
		),
		RowGroupPredicateKind::RangeLt { col, value } => {
			classify_min_max(directory, group, *col, None, Some((value, false)), config)
		}
		RowGroupPredicateKind::RangeLe { col, value } => {
			classify_min_max(directory, group, *col, None, Some((value, true)), config)
		}
		RowGroupPredicateKind::RangeGt { col, value } => {
			classify_min_max(directory, group, *col, Some((value, false)), None, config)
		}
		RowGroupPredicateKind::RangeGe { col, value } => {
			classify_min_max(directory, group, *col, Some((value, true)), None, config)
		}
		RowGroupPredicateKind::IsNull { col } => match stats_for(directory, group, *col) {
			Some(stat) if stat.null_count.get() == 0 => RowGroupClassification::Skip,
			Some(_) => RowGroupClassification::Keep,
			None => missing_stats_classification(config),
		},
		RowGroupPredicateKind::IsNotNull { col } => match stats_for(directory, group, *col) {
			Some(stat) if stat.null_count.get() == u64::from(group.row_count.get()) => {
				RowGroupClassification::Skip
			}
			Some(_) => RowGroupClassification::Keep,
			None => missing_stats_classification(config),
		},
		RowGroupPredicateKind::And(branches) => {
			let mut out = RowGroupClassification::Keep;
			for branch in branches {
				match classify_group(directory, group, branch, config) {
					RowGroupClassification::Skip => return RowGroupClassification::Skip,
					RowGroupClassification::NeedsPredicateFetch => {
						out = RowGroupClassification::NeedsPredicateFetch;
					}
					RowGroupClassification::Keep => {}
				}
			}
			out
		}
		RowGroupPredicateKind::Or(branches) => {
			if branches.is_empty() {
				return RowGroupClassification::Keep;
			}
			let mut all_skip = true;
			for branch in branches {
				if classify_group(directory, group, branch, config) != RowGroupClassification::Skip
				{
					all_skip = false;
					break;
				}
			}
			if all_skip {
				RowGroupClassification::Skip
			} else {
				RowGroupClassification::Keep
			}
		}
	}
}

fn classify_min_max(
	directory: &RecordPageDirectory,
	group: &RowGroupDirectoryEntry,
	col: IpcColumnOrdinal,
	lower: Option<(&CanonicalScalar, bool)>,
	upper: Option<(&CanonicalScalar, bool)>,
	config: &L2RowGroupFetchConfig,
) -> RowGroupClassification {
	let Some(stat) = stats_for(directory, group, col) else {
		return missing_stats_classification(config);
	};
	if stat
		.min
		.as_ref()
		.zip(stat.max.as_ref())
		.is_none_or(|(min, max)| {
			lower
				.map(|(value, _)| value)
				.or_else(|| upper.map(|(value, _)| value))
				.is_some_and(|value| {
					value.arrow_type_tag != min.arrow_type_tag
						|| value.arrow_type_tag != max.arrow_type_tag
				})
		}) {
		return RowGroupClassification::Keep;
	}
	if let (Some((lower, inclusive)), Some(max)) = (lower, stat.max.as_ref()) {
		let disjoint = if inclusive {
			lower.bytes.as_slice() > max.bytes.as_slice()
		} else {
			lower.bytes.as_slice() >= max.bytes.as_slice()
		};
		if disjoint && !max.truncated {
			return RowGroupClassification::Skip;
		}
	}
	if let (Some((upper, inclusive)), Some(min)) = (upper, stat.min.as_ref()) {
		let disjoint = if inclusive {
			upper.bytes.as_slice() < min.bytes.as_slice()
		} else {
			upper.bytes.as_slice() <= min.bytes.as_slice()
		};
		if disjoint {
			return RowGroupClassification::Skip;
		}
		if upper.bytes == min.bytes && min.truncated {
			return RowGroupClassification::Skip;
		}
	}
	RowGroupClassification::Keep
}

fn missing_stats_classification(config: &L2RowGroupFetchConfig) -> RowGroupClassification {
	if config.enabled {
		RowGroupClassification::NeedsPredicateFetch
	} else {
		RowGroupClassification::Keep
	}
}

fn stats_for<'a>(
	directory: &'a RecordPageDirectory,
	group: &RowGroupDirectoryEntry,
	col: IpcColumnOrdinal,
) -> Option<&'a RowGroupColumnStats> {
	directory.row_group_stats.iter().find(|stat| {
		stat.row_group_index == group.row_group_index && stat.ipc_column_ordinal == col
	})
}

fn validate_range_inside_record(
	range: RecordBodyByteRange,
	record_body_len: u64,
) -> Result<(), RowGroupRejectCode> {
	let end = range
		.end()
		.map_err(|_| RowGroupRejectCode::RowGroupRangeOverflow)?;
	if end > record_body_len {
		return Err(RowGroupRejectCode::RowGroupRangeOverflow);
	}
	Ok(())
}

fn validate_range_inside_range(
	range: RecordBodyByteRange,
	outer: RecordBodyByteRange,
) -> Result<(), RowGroupRejectCode> {
	let end = range
		.end()
		.map_err(|_| RowGroupRejectCode::RowGroupRangeOverflow)?;
	let outer_end = outer
		.end()
		.map_err(|_| RowGroupRejectCode::RowGroupRangeOverflow)?;
	if range.start.get() < outer.start.get() || end > outer_end {
		return Err(RowGroupRejectCode::RowGroupRangeOverflow);
	}
	Ok(())
}

fn checked_len(len: usize, label: &str) -> std::io::Result<u32> {
	u32::try_from(len).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("row-group directory {label} exceeds u32::MAX"),
		)
	})
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
	out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
	out.extend_from_slice(&value.to_be_bytes());
}

fn write_range(out: &mut Vec<u8>, range: RecordBodyByteRange) {
	write_u64(out, range.start.get());
	write_u64(out, range.len.get());
}

fn write_scalar(out: &mut Vec<u8>, scalar: Option<&CanonicalScalar>) -> std::io::Result<()> {
	write_canonical_scalar(out, scalar)
}

fn read_exact<'a>(
	bytes: &'a [u8],
	cursor: &mut usize,
	len: usize,
) -> Result<&'a [u8], RowGroupRejectCode> {
	let end = cursor
		.checked_add(len)
		.ok_or(RowGroupRejectCode::PageDirectoryParseError)?;
	let value = bytes
		.get(*cursor..end)
		.ok_or(RowGroupRejectCode::PageDirectoryParseError)?;
	*cursor = end;
	Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, RowGroupRejectCode> {
	Ok(u32::from_be_bytes(
		read_exact(bytes, cursor, 4)?
			.try_into()
			.expect("fixed width"),
	))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, RowGroupRejectCode> {
	Ok(u64::from_be_bytes(
		read_exact(bytes, cursor, 8)?
			.try_into()
			.expect("fixed width"),
	))
}

fn read_count(bytes: &[u8], cursor: &mut usize) -> Result<usize, RowGroupRejectCode> {
	usize::try_from(read_u32(bytes, cursor)?)
		.map_err(|_| RowGroupRejectCode::PageDirectoryParseError)
}

fn read_range(bytes: &[u8], cursor: &mut usize) -> Result<RecordBodyByteRange, RowGroupRejectCode> {
	Ok(RecordBodyByteRange {
		start: RecordBodyByteOffset::new(read_u64(bytes, cursor)?),
		len: RecordBodyByteLen::new(read_u64(bytes, cursor)?),
	})
}

fn read_scalar(
	bytes: &[u8],
	cursor: &mut usize,
) -> Result<Option<CanonicalScalar>, RowGroupRejectCode> {
	read_canonical_scalar(bytes, cursor).map_err(|_| RowGroupRejectCode::PageDirectoryParseError)
}

struct IpcMessageMetadata<'a> {
	message: arrow::ipc::Message<'a>,
	header_type: MessageHeader,
	prefix_start: usize,
	body_start: usize,
	body_len: usize,
}

fn read_ipc_message_metadata<'a>(
	bytes: &'a [u8],
	offset: &mut usize,
) -> std::io::Result<Option<IpcMessageMetadata<'a>>> {
	if bytes.len().saturating_sub(*offset) < 4 {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"arrow IPC message prefix truncated",
		));
	}
	let prefix_start = *offset;
	let first = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().expect("fixed width"));
	*offset += 4;
	let metadata_len = if first == u32::MAX {
		if bytes.len().saturating_sub(*offset) < 4 {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"arrow IPC continuation prefix truncated",
			));
		}
		let len = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().expect("fixed width"));
		*offset += 4;
		len as usize
	} else {
		first as usize
	};
	if metadata_len == 0 {
		return Ok(None);
	}
	let metadata_start = *offset;
	let metadata_end = metadata_start
		.checked_add(metadata_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "arrow metadata length overflow"))?;
	if metadata_end > bytes.len() {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"arrow IPC metadata truncated",
		));
	}
	let message = root_as_message(&bytes[metadata_start..metadata_end]).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("arrow IPC message flatbuffer parse: {err}"),
		)
	})?;
	let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"negative arrow IPC message body length",
		)
	})?;
	let body_start = metadata_end;
	let body_end = body_start
		.checked_add(body_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "arrow IPC body length overflow"))?;
	if body_end > bytes.len() {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"arrow IPC body truncated",
		));
	}
	*offset = body_end;
	Ok(Some(IpcMessageMetadata {
		header_type: message.header_type(),
		message,
		prefix_start,
		body_start,
		body_len,
	}))
}

fn buffer_count_for_data_type(data_type: &DataType) -> std::io::Result<u32> {
	let count = match data_type {
		DataType::Null => 0,
		DataType::Utf8 | DataType::Binary | DataType::LargeBinary | DataType::LargeUtf8 => 3,
		DataType::Boolean | DataType::FixedSizeBinary(_) => 2,
		data_type if data_type.is_primitive() => 2,
		_ => {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("unsupported row-group Arrow layout {data_type:?}"),
			));
		}
	};
	Ok(count)
}
