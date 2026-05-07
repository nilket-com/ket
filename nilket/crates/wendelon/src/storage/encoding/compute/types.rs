use crate::schema::{
	BufferValueCount, DictMatchSetSize, EncodingPageCount, RecordBodyByteLen,
	SelectivityFractionMicros, TaskCount,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::{Error, ErrorKind};

pub const ENCODING_COMPUTE_MAX_DICT_MATCH_SET_SIZE_DEFAULT: u32 = 4_096;
pub const ENCODING_COMPUTE_MIN_BUFFER_ROWS_FOR_ENCODED_EVAL_DEFAULT: u32 = 256;
pub const ENCODING_COMPUTE_MAX_PARALLEL_KERNEL_TASKS_DEFAULT: u32 = 4;
pub const ENCODING_COMPUTE_MAX_MASK_BYTES_PER_RECORD_DEFAULT: u64 = 256 * 1024;
pub const ENCODING_COMPUTE_MAX_ENCODED_PAGES_PER_RECORD_DEFAULT: u32 = 256;
pub const ENCODING_COMPUTE_MIN_SELECTIVITY_FOR_ENCODED_EVAL_MICROS_DEFAULT: u32 = 250_000;
pub const ENCODING_COMPUTE_MIN_PREFILTER_REJECTION_MICROS_DEFAULT: u32 = 100_000;
pub const SELECTIVITY_FRACTION_MICROS_MAX: u32 = 1_000_000;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EncodingComputeEvaluatorKind {
	BitPacked = 1,
	RlePlain = 2,
	RleBitPacked = 3,
	DictionaryPlain = 4,
	DictionaryBitPacked = 5,
	Delta = 6,
}

impl EncodingComputeEvaluatorKind {
	pub const ALL: [Self; 6] = [
		Self::BitPacked,
		Self::RlePlain,
		Self::RleBitPacked,
		Self::DictionaryPlain,
		Self::DictionaryBitPacked,
		Self::Delta,
	];

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::BitPacked => "bit_packed",
			Self::RlePlain => "rle_plain",
			Self::RleBitPacked => "rle_bit_packed",
			Self::DictionaryPlain => "dictionary_plain",
			Self::DictionaryBitPacked => "dictionary_bit_packed",
			Self::Delta => "delta",
		}
	}

	pub fn from_label(label: &str) -> Option<Self> {
		match label {
			"bit_packed" => Some(Self::BitPacked),
			"rle_plain" => Some(Self::RlePlain),
			"rle_bit_packed" => Some(Self::RleBitPacked),
			"dictionary_plain" => Some(Self::DictionaryPlain),
			"dictionary_bit_packed" => Some(Self::DictionaryBitPacked),
			"delta" => Some(Self::Delta),
			_ => None,
		}
	}
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EncodedAggregateKind {
	CountStar = 1,
	CountNonNull = 2,
	Min = 3,
	Max = 4,
	SignedSum = 5,
	UnsignedSum = 6,
	SignedAvg = 7,
	UnsignedAvg = 8,
}

impl EncodedAggregateKind {
	pub const ALL: [Self; 8] = [
		Self::CountStar,
		Self::CountNonNull,
		Self::Min,
		Self::Max,
		Self::SignedSum,
		Self::UnsignedSum,
		Self::SignedAvg,
		Self::UnsignedAvg,
	];

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::CountStar => "count_star",
			Self::CountNonNull => "count_non_null",
			Self::Min => "min",
			Self::Max => "max",
			Self::SignedSum => "signed_sum",
			Self::UnsignedSum => "unsigned_sum",
			Self::SignedAvg => "signed_avg",
			Self::UnsignedAvg => "unsigned_avg",
		}
	}

	pub fn from_label(label: &str) -> Option<Self> {
		match label {
			"count_star" => Some(Self::CountStar),
			"count_non_null" => Some(Self::CountNonNull),
			"min" => Some(Self::Min),
			"max" => Some(Self::Max),
			"signed_sum" => Some(Self::SignedSum),
			"unsigned_sum" => Some(Self::UnsignedSum),
			"signed_avg" => Some(Self::SignedAvg),
			"unsigned_avg" => Some(Self::UnsignedAvg),
			_ => None,
		}
	}
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EncodingComputeRejectCode {
	MaskLengthMismatch = 1,
	EncodedValueOverflow = 2,
	KernelDecodedLengthMismatch = 3,
	AggregateOverflowMismatch = 4,
	SelectionRangeOverflow = 5,
}

impl EncodingComputeRejectCode {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::MaskLengthMismatch => "mask_length_mismatch",
			Self::EncodedValueOverflow => "encoded_value_overflow",
			Self::KernelDecodedLengthMismatch => "kernel_decoded_length_mismatch",
			Self::AggregateOverflowMismatch => "aggregate_overflow_mismatch",
			Self::SelectionRangeOverflow => "selection_range_overflow",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EncodingComputeFallbackReason {
	ReaderDisabled,
	DirectoryMissing,
	UnsupportedProgram,
	UnsupportedCascade,
	UnsupportedArrowLayout,
	DenseSelection,
	TooManyMaskBytes,
	TooManyPages,
	AggregateUnsupported,
}

impl EncodingComputeFallbackReason {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::ReaderDisabled => "reader_disabled",
			Self::DirectoryMissing => "directory_missing",
			Self::UnsupportedProgram => "unsupported_program",
			Self::UnsupportedCascade => "unsupported_cascade",
			Self::UnsupportedArrowLayout => "unsupported_arrow_layout",
			Self::DenseSelection => "dense_selection",
			Self::TooManyMaskBytes => "too_many_mask_bytes",
			Self::TooManyPages => "too_many_pages",
			Self::AggregateUnsupported => "aggregate_unsupported",
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodingComputeConfig {
	pub enabled: bool,
	pub max_dict_match_set_size: DictMatchSetSize,
	pub min_buffer_rows_for_encoded_eval: BufferValueCount,
	pub max_parallel_kernel_tasks: TaskCount,
	pub max_mask_bytes_per_record: RecordBodyByteLen,
	pub max_encoded_pages_per_record: EncodingPageCount,
	pub min_selectivity_for_encoded_eval_micros: SelectivityFractionMicros,
	pub min_prefilter_rejection_micros: SelectivityFractionMicros,
	pub enabled_predicate_evaluators: BTreeSet<EncodingComputeEvaluatorKind>,
	pub enabled_aggregate_kernels: BTreeSet<EncodedAggregateKind>,
}

impl Default for EncodingComputeConfig {
	fn default() -> Self {
		Self {
			enabled: true,
			max_dict_match_set_size: DictMatchSetSize::new(
				ENCODING_COMPUTE_MAX_DICT_MATCH_SET_SIZE_DEFAULT,
			),
			min_buffer_rows_for_encoded_eval: BufferValueCount::new(
				ENCODING_COMPUTE_MIN_BUFFER_ROWS_FOR_ENCODED_EVAL_DEFAULT,
			),
			max_parallel_kernel_tasks: TaskCount::new(
				ENCODING_COMPUTE_MAX_PARALLEL_KERNEL_TASKS_DEFAULT,
			),
			max_mask_bytes_per_record: RecordBodyByteLen::new(
				ENCODING_COMPUTE_MAX_MASK_BYTES_PER_RECORD_DEFAULT,
			),
			max_encoded_pages_per_record: EncodingPageCount::new(
				ENCODING_COMPUTE_MAX_ENCODED_PAGES_PER_RECORD_DEFAULT,
			),
			min_selectivity_for_encoded_eval_micros: SelectivityFractionMicros::new(
				ENCODING_COMPUTE_MIN_SELECTIVITY_FOR_ENCODED_EVAL_MICROS_DEFAULT,
			),
			min_prefilter_rejection_micros: SelectivityFractionMicros::new(
				ENCODING_COMPUTE_MIN_PREFILTER_REJECTION_MICROS_DEFAULT,
			),
			enabled_predicate_evaluators: [
				EncodingComputeEvaluatorKind::BitPacked,
				EncodingComputeEvaluatorKind::DictionaryPlain,
				EncodingComputeEvaluatorKind::DictionaryBitPacked,
				EncodingComputeEvaluatorKind::Delta,
			]
			.into_iter()
			.collect(),
			enabled_aggregate_kernels: EncodedAggregateKind::ALL.into_iter().collect(),
		}
	}
}

impl EncodingComputeConfig {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.max_dict_match_set_size.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding compute max_dict_match_set_size must be greater than zero",
			));
		}
		if self.min_buffer_rows_for_encoded_eval.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding compute min_buffer_rows_for_encoded_eval must be greater than zero",
			));
		}
		if self.max_parallel_kernel_tasks.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding compute max_parallel_kernel_tasks must be greater than zero",
			));
		}
		if self.max_mask_bytes_per_record.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding compute max_mask_bytes_per_record must be greater than zero",
			));
		}
		if self.max_encoded_pages_per_record.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding compute max_encoded_pages_per_record must be greater than zero",
			));
		}
		if self.min_selectivity_for_encoded_eval_micros.get() > SELECTIVITY_FRACTION_MICROS_MAX {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding compute min_selectivity_for_encoded_eval_micros exceeds 1_000_000",
			));
		}
		if self.min_prefilter_rejection_micros.get() > SELECTIVITY_FRACTION_MICROS_MAX {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding compute min_prefilter_rejection_micros exceeds 1_000_000",
			));
		}
		if self.enabled && self.enabled_predicate_evaluators.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding compute enabled_predicate_evaluators must not be empty when enabled",
			));
		}
		if self.enabled && self.enabled_aggregate_kernels.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"encoding compute enabled_aggregate_kernels must not be empty when enabled",
			));
		}
		Ok(())
	}

	pub fn predicate_evaluator_enabled(&self, kind: EncodingComputeEvaluatorKind) -> bool {
		self.enabled_predicate_evaluators.contains(&kind)
	}

	pub fn aggregate_kernel_enabled(&self, kind: EncodedAggregateKind) -> bool {
		self.enabled_aggregate_kernels.contains(&kind)
	}
}
