use crate::schema::{
	BaseValue, BitsPerValue, BufferValueCount, BufferValueIndex, DictValueCount,
	EncodingCascadeIndex, EncodingPageByteOffset, EncodingPageCount, EncodingPageIndex,
	ExceptionCount, Exponent, Factor, IpcBufferIndex, IpcColumnOrdinal, LogicalSchemaFingerprint,
	PageNullCount, RecordBodyByteLen, RecordBodyByteOffset, RowCount, RowGroupIndex, RunCount,
	SampleRowCount, SimdPageValueCount, SlabId, TaskCount,
};
use crate::storage::columnar::TieredRecordId;
use crate::storage::stats::CanonicalScalar;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::{Error, ErrorKind};

pub const ENCODING_DIRECTORY_METADATA_KEY: &str = "storage.encoding_directory";
pub const ENCODING_DIRECTORY_MAGIC: [u8; 8] = *b"WNDENC01";
pub const ENCODING_DIRECTORY_FORMAT_VERSION: u32 = 2;

pub const ENCODING_TARGET_SIMD_PAGE_VALUES_DEFAULT: u32 = 1_024;
pub const ENCODING_CASCADE_SAMPLE_ROWS_DEFAULT: u32 = 256;
pub const ENCODING_MIN_BUFFER_BYTES_DEFAULT: u64 = 4_096;
pub const L2_ENCODING_MAX_SIMD_PAGES_PER_RECORD_DEFAULT: u32 = 64;
pub const L2_ENCODING_MAX_PARALLEL_MATERIALIZE_TASKS_DEFAULT: u32 = 4;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EncodingCascadeKind {
	Plain = 1,
	Lz4Frame = 2,
	BitPacked = 3,
	Rle = 4,
	Dictionary = 5,
	Delta = 6,
	Fsst = 7,
	Alp = 8,
}

impl EncodingCascadeKind {
	pub const ALL: [Self; 8] = [
		Self::Plain,
		Self::Lz4Frame,
		Self::BitPacked,
		Self::Rle,
		Self::Dictionary,
		Self::Delta,
		Self::Fsst,
		Self::Alp,
	];

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Plain => "plain",
			Self::Lz4Frame => "lz4_frame",
			Self::BitPacked => "bit_packed",
			Self::Rle => "rle",
			Self::Dictionary => "dictionary",
			Self::Delta => "delta",
			Self::Fsst => "fsst",
			Self::Alp => "alp",
		}
	}

	pub fn from_label(label: &str) -> Option<Self> {
		match label {
			"plain" => Some(Self::Plain),
			"lz4_frame" => Some(Self::Lz4Frame),
			"bit_packed" => Some(Self::BitPacked),
			"rle" => Some(Self::Rle),
			"dictionary" => Some(Self::Dictionary),
			"delta" => Some(Self::Delta),
			"fsst" => Some(Self::Fsst),
			"alp" => Some(Self::Alp),
			_ => None,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncodingLeafCascade {
	Plain,
	Lz4Frame,
	BitPacked { bits: BitsPerValue, base: BaseValue },
}

impl EncodingLeafCascade {
	pub fn kind(&self) -> EncodingCascadeKind {
		match self {
			Self::Plain => EncodingCascadeKind::Plain,
			Self::Lz4Frame => EncodingCascadeKind::Lz4Frame,
			Self::BitPacked { .. } => EncodingCascadeKind::BitPacked,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncodingCascade {
	Plain,
	Lz4Frame,
	BitPacked {
		bits: BitsPerValue,
		base: BaseValue,
	},
	Rle {
		run_count: RunCount,
		inner: Box<EncodingLeafCascade>,
	},
	Dictionary {
		dict_byte_len: RecordBodyByteLen,
		dict_value_count: DictValueCount,
		inner: Box<EncodingLeafCascade>,
	},
	Delta {
		base: BaseValue,
		inner: Box<EncodingLeafCascade>,
	},
	Fsst {
		symbol_table_byte_len: RecordBodyByteLen,
	},
	Alp {
		exponent: Exponent,
		factor: Factor,
		exception_count: ExceptionCount,
		inner: Box<EncodingLeafCascade>,
	},
}

impl EncodingCascade {
	pub fn kind(&self) -> EncodingCascadeKind {
		match self {
			Self::Plain => EncodingCascadeKind::Plain,
			Self::Lz4Frame => EncodingCascadeKind::Lz4Frame,
			Self::BitPacked { .. } => EncodingCascadeKind::BitPacked,
			Self::Rle { .. } => EncodingCascadeKind::Rle,
			Self::Dictionary { .. } => EncodingCascadeKind::Dictionary,
			Self::Delta { .. } => EncodingCascadeKind::Delta,
			Self::Fsst { .. } => EncodingCascadeKind::Fsst,
			Self::Alp { .. } => EncodingCascadeKind::Alp,
		}
	}

	pub fn emits_pages_in_v1(&self) -> bool {
		matches!(
			self,
			Self::BitPacked { .. } | Self::Rle { .. } | Self::Dictionary { .. }
		)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedBufferByteRange {
	pub start: EncodingPageByteOffset,
	pub len: RecordBodyByteLen,
}

impl EncodedBufferByteRange {
	pub fn end(self) -> std::io::Result<u64> {
		u64::from(self.start.get())
			.checked_add(self.len.get())
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"Encoding buffer byte range overflow",
				)
			})
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodingDirectory {
	pub record_id: TieredRecordId,
	pub row_count: RowCount,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub target_simd_page_values: SimdPageValueCount,
	pub cascades: Vec<EncodingCascade>,
	pub buffers: Vec<BufferEncoding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BufferEncoding {
	pub row_group_index: RowGroupIndex,
	pub ipc_column_ordinal: IpcColumnOrdinal,
	pub ipc_buffer_ordinal: IpcBufferIndex,
	pub cascade_index: EncodingCascadeIndex,
	pub decoded_byte_len: RecordBodyByteLen,
	pub decoded_value_count: BufferValueCount,
	pub encoded_range: EncodedBufferByteRange,
	pub page_value_count: SimdPageValueCount,
	pub page_end_offsets: Vec<EncodingPageByteOffset>,
	pub page_stats: Vec<EncodingPageStats>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodingPageStats {
	pub null_count: PageNullCount,
	pub min: Option<CanonicalScalar>,
	pub max: Option<CanonicalScalar>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodingPageDescriptor {
	pub page_index: EncodingPageIndex,
	pub first_value_index: BufferValueIndex,
	pub value_count: BufferValueCount,
	pub byte_range: EncodedBufferByteRange,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodingRejectCode {
	EncodingDirectoryParseError = 1,
	BufferEncodingMismatch = 2,
	PageRangeOverflow = 3,
	CascadeUnsupported = 4,
	DecodedLengthMismatch = 5,
}

impl EncodingRejectCode {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::EncodingDirectoryParseError => "encoding_directory_parse",
			Self::BufferEncodingMismatch => "buffer_encoding_mismatch",
			Self::PageRangeOverflow => "page_range_overflow",
			Self::CascadeUnsupported => "cascade_unsupported",
			Self::DecodedLengthMismatch => "decoded_length_mismatch",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EncodingFallbackReason {
	ReaderDisabled,
	DirectoryMissing,
	UnsupportedArrowLayout,
	TooManyPages,
	TooManyBytes,
	DenseSelection,
	PlainBuffer,
}

impl EncodingFallbackReason {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::ReaderDisabled => "reader_disabled",
			Self::DirectoryMissing => "directory_missing",
			Self::UnsupportedArrowLayout => "unsupported_arrow_layout",
			Self::TooManyPages => "too_many_pages",
			Self::TooManyBytes => "too_many_bytes",
			Self::DenseSelection => "dense_selection",
			Self::PlainBuffer => "plain_buffer",
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodingWriterConfig {
	pub enabled: bool,
	pub target_simd_page_values: SimdPageValueCount,
	pub cascade_sample_rows: SampleRowCount,
	pub min_buffer_bytes_for_encoding: RecordBodyByteLen,
	pub enabled_cascades: BTreeSet<EncodingCascadeKind>,
}

impl Default for EncodingWriterConfig {
	fn default() -> Self {
		Self {
			enabled: false,
			target_simd_page_values: SimdPageValueCount::new(
				ENCODING_TARGET_SIMD_PAGE_VALUES_DEFAULT,
			),
			cascade_sample_rows: SampleRowCount::new(ENCODING_CASCADE_SAMPLE_ROWS_DEFAULT),
			min_buffer_bytes_for_encoding: RecordBodyByteLen::new(
				ENCODING_MIN_BUFFER_BYTES_DEFAULT,
			),
			enabled_cascades: EncodingCascadeKind::ALL.into_iter().collect(),
		}
	}
}

impl EncodingWriterConfig {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.target_simd_page_values.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding writer target_simd_page_values must be greater than zero",
			));
		}
		if self.cascade_sample_rows.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding writer cascade_sample_rows must be greater than zero",
			));
		}
		if self.min_buffer_bytes_for_encoding.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding writer min_buffer_bytes_for_encoding must be greater than zero",
			));
		}
		if self.enabled && self.enabled_cascades.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding writer enabled_cascades must not be empty when enabled",
			));
		}
		Ok(())
	}

	pub fn cascade_enabled(&self, kind: EncodingCascadeKind) -> bool {
		self.enabled_cascades.contains(&kind)
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct L2EncodingFetchConfig {
	pub enabled: bool,
	pub max_simd_pages_per_record: EncodingPageCount,
	pub max_parallel_materialize_tasks: TaskCount,
}

impl Default for L2EncodingFetchConfig {
	fn default() -> Self {
		Self {
			enabled: false,
			max_simd_pages_per_record: EncodingPageCount::new(
				L2_ENCODING_MAX_SIMD_PAGES_PER_RECORD_DEFAULT,
			),
			max_parallel_materialize_tasks: TaskCount::new(
				L2_ENCODING_MAX_PARALLEL_MATERIALIZE_TASKS_DEFAULT,
			),
		}
	}
}

impl L2EncodingFetchConfig {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.max_simd_pages_per_record.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 encoding max_simd_pages_per_record must be greater than zero",
			));
		}
		if self.max_parallel_materialize_tasks.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 encoding max_parallel_materialize_tasks must be greater than zero",
			));
		}
		Ok(())
	}
}

pub fn zero_record_id() -> TieredRecordId {
	TieredRecordId {
		slab_id: SlabId::ZERO,
		record_offset: RecordBodyByteOffset::ZERO,
	}
}
