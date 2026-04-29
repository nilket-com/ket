//! Aggregate pushdown protocol (AGG) schema and wire-state helpers.
//!
//! AGG is a sibling protocol to PBC: requests carry an optional PBC
//! predicate as row-admission bytecode, then a bounded list of
//! ungrouped aggregate expressions. This module keeps the protocol
//! catalog, request/response envelopes, reject codes, and per-state
//! byte encodings in one place so storage execution and client tests
//! share the same contract.

use crate::schema::pbc::Program as PbcProgram;
use crate::schema::{
	AggregateExpressionIndex, AggregateRequestId, ByteCount, Consistency, DurationMillis,
	HybridTimestamp, KllK, LocalityRange, LocationCount, PartialStateLength, RowCount,
	TableObjectId, TenantId,
};
use crate::schema::{COLUMN_STATS_HLL_MAX_PRECISION, COLUMN_STATS_HLL_MIN_PRECISION, HllPrecision};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};

pub const AGG_FORMAT_VERSION: u16 = 1;
pub const AGG_MAX_AGGREGATES: usize = 64;
pub const AGG_COUNT_STAR_COLUMN_SENTINEL: u16 = 0xFFFF;
pub const AGG_KLL_DEFAULT_K: u32 = 200;
pub const AGG_KLL_MIN_K: u32 = 64;
pub const AGG_KLL_MAX_K: u32 = 2048;
pub const AGG_MIN_MAX_PREFIX_BYTES: usize = 64;

pub type SchemaFingerprint = [u8; 32];
pub type PredicateBytecode = Vec<u8>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct AggregateFunctionTag(pub u8);

impl AggregateFunctionTag {
	pub const COUNT_STAR: Self = Self(0x01);
	pub const COUNT_NON_NULL: Self = Self(0x02);
	pub const SIGNED_SUM: Self = Self(0x03);
	pub const UNSIGNED_SUM: Self = Self(0x04);
	pub const MIN: Self = Self(0x05);
	pub const MAX: Self = Self(0x06);
	pub const SIGNED_AVG: Self = Self(0x07);
	pub const UNSIGNED_AVG: Self = Self(0x08);
	pub const APPROX_DISTINCT: Self = Self(0x09);
	pub const QUANTILE_KLL: Self = Self(0x0A);

	pub const ALL: [Self; 10] = [
		Self::COUNT_STAR,
		Self::COUNT_NON_NULL,
		Self::SIGNED_SUM,
		Self::UNSIGNED_SUM,
		Self::MIN,
		Self::MAX,
		Self::SIGNED_AVG,
		Self::UNSIGNED_AVG,
		Self::APPROX_DISTINCT,
		Self::QUANTILE_KLL,
	];

	pub const fn get(self) -> u8 {
		self.0
	}

	pub fn validate(self) -> Result<(), AggRejectCode> {
		if Self::ALL.contains(&self) {
			Ok(())
		} else {
			Err(AggRejectCode::UnsupportedFunction)
		}
	}

	pub const fn is_approximate(self) -> bool {
		matches!(self.0, 0x09 | 0x0A)
	}

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::COUNT_STAR => "count_star",
			Self::COUNT_NON_NULL => "count_non_null",
			Self::SIGNED_SUM => "signed_sum",
			Self::UNSIGNED_SUM => "unsigned_sum",
			Self::MIN => "min",
			Self::MAX => "max",
			Self::SIGNED_AVG => "signed_avg",
			Self::UNSIGNED_AVG => "unsigned_avg",
			Self::APPROX_DISTINCT => "approx_distinct",
			Self::QUANTILE_KLL => "quantile_kll",
			_ => "unknown",
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
	CountStar,
	CountNonNull {
		column: u16,
	},
	SignedSum {
		column: u16,
	},
	UnsignedSum {
		column: u16,
	},
	Min {
		column: u16,
	},
	Max {
		column: u16,
	},
	SignedAverageParts {
		column: u16,
	},
	UnsignedAverageParts {
		column: u16,
	},
	ApproxDistinct {
		column: u16,
		precision: HllPrecision,
	},
	Quantile {
		column: u16,
		sketch: QuantileSketch,
	},
}

impl AggregateFunction {
	pub const fn tag(&self) -> AggregateFunctionTag {
		match self {
			Self::CountStar => AggregateFunctionTag::COUNT_STAR,
			Self::CountNonNull { .. } => AggregateFunctionTag::COUNT_NON_NULL,
			Self::SignedSum { .. } => AggregateFunctionTag::SIGNED_SUM,
			Self::UnsignedSum { .. } => AggregateFunctionTag::UNSIGNED_SUM,
			Self::Min { .. } => AggregateFunctionTag::MIN,
			Self::Max { .. } => AggregateFunctionTag::MAX,
			Self::SignedAverageParts { .. } => AggregateFunctionTag::SIGNED_AVG,
			Self::UnsignedAverageParts { .. } => AggregateFunctionTag::UNSIGNED_AVG,
			Self::ApproxDistinct { .. } => AggregateFunctionTag::APPROX_DISTINCT,
			Self::Quantile { .. } => AggregateFunctionTag::QUANTILE_KLL,
		}
	}

	pub const fn column(&self) -> Option<u16> {
		match self {
			Self::CountStar => None,
			Self::CountNonNull { column }
			| Self::SignedSum { column }
			| Self::UnsignedSum { column }
			| Self::Min { column }
			| Self::Max { column }
			| Self::SignedAverageParts { column }
			| Self::UnsignedAverageParts { column }
			| Self::ApproxDistinct { column, .. }
			| Self::Quantile { column, .. } => Some(*column),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuantileSketch {
	Kll { k: KllK },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateExpression {
	pub tag: AggregateFunctionTag,
	pub column_ordinal: u16,
	#[serde(with = "serde_bytes")]
	pub parameter_block: Vec<u8>,
}

impl AggregateExpression {
	pub fn count_star() -> Self {
		Self {
			tag: AggregateFunctionTag::COUNT_STAR,
			column_ordinal: AGG_COUNT_STAR_COLUMN_SENTINEL,
			parameter_block: Vec::new(),
		}
	}

	pub fn function(&self) -> Result<AggregateFunction, AggRejectCode> {
		self.tag.validate()?;
		match self.tag {
			AggregateFunctionTag::COUNT_STAR => {
				if self.column_ordinal != AGG_COUNT_STAR_COLUMN_SENTINEL {
					return Err(AggRejectCode::ColumnUnknown);
				}
				require_empty_parameter_block(&self.parameter_block)?;
				Ok(AggregateFunction::CountStar)
			}
			AggregateFunctionTag::COUNT_NON_NULL => {
				require_column(self.column_ordinal)?;
				require_empty_parameter_block(&self.parameter_block)?;
				Ok(AggregateFunction::CountNonNull {
					column: self.column_ordinal,
				})
			}
			AggregateFunctionTag::SIGNED_SUM => {
				require_column(self.column_ordinal)?;
				require_empty_parameter_block(&self.parameter_block)?;
				Ok(AggregateFunction::SignedSum {
					column: self.column_ordinal,
				})
			}
			AggregateFunctionTag::UNSIGNED_SUM => {
				require_column(self.column_ordinal)?;
				require_empty_parameter_block(&self.parameter_block)?;
				Ok(AggregateFunction::UnsignedSum {
					column: self.column_ordinal,
				})
			}
			AggregateFunctionTag::MIN => {
				require_column(self.column_ordinal)?;
				require_empty_parameter_block(&self.parameter_block)?;
				Ok(AggregateFunction::Min {
					column: self.column_ordinal,
				})
			}
			AggregateFunctionTag::MAX => {
				require_column(self.column_ordinal)?;
				require_empty_parameter_block(&self.parameter_block)?;
				Ok(AggregateFunction::Max {
					column: self.column_ordinal,
				})
			}
			AggregateFunctionTag::SIGNED_AVG => {
				require_column(self.column_ordinal)?;
				require_empty_parameter_block(&self.parameter_block)?;
				Ok(AggregateFunction::SignedAverageParts {
					column: self.column_ordinal,
				})
			}
			AggregateFunctionTag::UNSIGNED_AVG => {
				require_column(self.column_ordinal)?;
				require_empty_parameter_block(&self.parameter_block)?;
				Ok(AggregateFunction::UnsignedAverageParts {
					column: self.column_ordinal,
				})
			}
			AggregateFunctionTag::APPROX_DISTINCT => {
				require_column(self.column_ordinal)?;
				Ok(AggregateFunction::ApproxDistinct {
					column: self.column_ordinal,
					precision: parse_hll_precision_parameter(&self.parameter_block)?,
				})
			}
			AggregateFunctionTag::QUANTILE_KLL => {
				require_column(self.column_ordinal)?;
				Ok(AggregateFunction::Quantile {
					column: self.column_ordinal,
					sketch: QuantileSketch::Kll {
						k: parse_kll_k_parameter(&self.parameter_block)?,
					},
				})
			}
			_ => Err(AggRejectCode::UnsupportedFunction),
		}
	}
}

fn require_column(column_ordinal: u16) -> Result<(), AggRejectCode> {
	if column_ordinal == AGG_COUNT_STAR_COLUMN_SENTINEL {
		Err(AggRejectCode::ColumnUnknown)
	} else {
		Ok(())
	}
}

fn require_empty_parameter_block(parameter_block: &[u8]) -> Result<(), AggRejectCode> {
	if parameter_block.is_empty() {
		Ok(())
	} else {
		Err(AggRejectCode::SketchParameterInvalid)
	}
}

fn parse_hll_precision_parameter(parameter_block: &[u8]) -> Result<HllPrecision, AggRejectCode> {
	let precision = match parameter_block {
		[] => HllPrecision::DEFAULT,
		[precision] => {
			HllPrecision::new(*precision).map_err(|_| AggRejectCode::SketchParameterInvalid)?
		}
		_ => return Err(AggRejectCode::SketchParameterInvalid),
	};
	if !(COLUMN_STATS_HLL_MIN_PRECISION..=COLUMN_STATS_HLL_MAX_PRECISION).contains(&precision.get())
	{
		return Err(AggRejectCode::SketchParameterInvalid);
	}
	Ok(precision)
}

fn parse_kll_k_parameter(parameter_block: &[u8]) -> Result<KllK, AggRejectCode> {
	let k = match parameter_block {
		[] => AGG_KLL_DEFAULT_K,
		[bytes @ ..] if bytes.len() == 4 => {
			u32::from_be_bytes(bytes.try_into().expect("validated KLL parameter length"))
		}
		_ => return Err(AggRejectCode::SketchParameterInvalid),
	};
	validate_kll_k(k)?;
	Ok(KllK::new(k))
}

pub fn validate_kll_k(k: u32) -> Result<(), AggRejectCode> {
	if (AGG_KLL_MIN_K..=AGG_KLL_MAX_K).contains(&k) {
		Ok(())
	} else {
		Err(AggRejectCode::SketchParameterInvalid)
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateBudgets {
	pub max_locations: LocationCount,
	pub max_rows: RowCount,
	pub max_wall_time_millis: DurationMillis,
	pub max_partial_state_bytes: ByteCount,
}

impl Default for AggregateBudgets {
	fn default() -> Self {
		Self {
			max_locations: LocationCount::new(u64::MAX),
			max_rows: RowCount::new(u64::MAX),
			max_wall_time_millis: DurationMillis::new(u64::MAX),
			max_partial_state_bytes: ByteCount::new(16 * 1024 * 1024),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateRequest {
	pub format_version: u16,
	pub request_id: AggregateRequestId,
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub schema_fingerprint: SchemaFingerprint,
	pub consistency: Consistency,
	pub min_hlc: HybridTimestamp,
	pub as_of_hlc: HybridTimestamp,
	pub locality_ranges: Vec<LocalityRange>,
	pub predicate_program: Option<PredicateBytecode>,
	pub aggregates: Vec<AggregateExpression>,
	pub budgets: AggregateBudgets,
}

impl AggregateRequest {
	pub fn validate_envelope(&self, exact_only: bool) -> Result<(), AggRejectCode> {
		if self.format_version != AGG_FORMAT_VERSION {
			return Err(AggRejectCode::FormatVersion);
		}
		if self.aggregates.len() > AGG_MAX_AGGREGATES {
			return Err(AggRejectCode::TooManyAggregates);
		}
		if self.as_of_hlc < self.min_hlc {
			return Err(AggRejectCode::BudgetExceeded);
		}
		if let Some(bytecode) = self.predicate_program.as_ref() {
			let program =
				PbcProgram::compile(bytecode).map_err(|_| AggRejectCode::ProgramInvalid)?;
			if program.has_projection() {
				return Err(AggRejectCode::ProgramInvalid);
			}
		}
		for expression in &self.aggregates {
			let function = expression.function()?;
			if exact_only && function.tag().is_approximate() {
				return Err(AggRejectCode::ApproxNotAllowed);
			}
		}
		Ok(())
	}

	pub fn expression_digest(&self) -> [u8; 32] {
		let mut hasher = blake3::Hasher::new();
		for expression in &self.aggregates {
			hasher.update(&[expression.tag.get()]);
			hasher.update(&expression.column_ordinal.to_be_bytes());
			hasher.update(&(expression.parameter_block.len() as u64).to_be_bytes());
			hasher.update(&expression.parameter_block);
		}
		*hasher.finalize().as_bytes()
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AggregateStatus {
	Success = 0,
	Partial = 1,
	Rejected = 2,
	NumericOverflow = 3,
}

impl AggregateStatus {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Success => "success",
			Self::Partial => "partial",
			Self::Rejected => "rejected",
			Self::NumericOverflow => "numeric_overflow",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum AggRejectCode {
	FormatVersion = 1,
	UnsupportedFunction = 2,
	TooManyAggregates = 3,
	ColumnUnknown = 4,
	TypeUnsupported = 5,
	SketchParameterInvalid = 6,
	ApproxNotAllowed = 7,
	ProgramInvalid = 8,
	SchemaStale = 9,
	SchemaUnknown = 10,
	BudgetExceeded = 11,
	DeadlineExceeded = 12,
	CapabilityUnavailable = 13,
	PartialCrossbindFailed = 14,
	PartialStateTooLarge = 15,
	NumericOverflow = 16,
	RetentionExpired = 17,
}

impl AggRejectCode {
	pub const fn code(self) -> u16 {
		self as u16
	}

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::FormatVersion => "AGG_REJECT_FORMAT_VERSION",
			Self::UnsupportedFunction => "AGG_REJECT_UNSUPPORTED_FUNCTION",
			Self::TooManyAggregates => "AGG_REJECT_TOO_MANY_AGGREGATES",
			Self::ColumnUnknown => "AGG_REJECT_COLUMN_UNKNOWN",
			Self::TypeUnsupported => "AGG_REJECT_TYPE_UNSUPPORTED",
			Self::SketchParameterInvalid => "AGG_REJECT_SKETCH_PARAMETER_INVALID",
			Self::ApproxNotAllowed => "AGG_REJECT_APPROX_NOT_ALLOWED",
			Self::ProgramInvalid => "AGG_REJECT_PROGRAM_INVALID",
			Self::SchemaStale => "AGG_REJECT_SCHEMA_STALE",
			Self::SchemaUnknown => "AGG_REJECT_SCHEMA_UNKNOWN",
			Self::BudgetExceeded => "AGG_REJECT_BUDGET_EXCEEDED",
			Self::DeadlineExceeded => "AGG_REJECT_DEADLINE_EXCEEDED",
			Self::CapabilityUnavailable => "AGG_REJECT_CAPABILITY_UNAVAILABLE",
			Self::PartialCrossbindFailed => "AGG_REJECT_PARTIAL_CROSSBIND_FAILED",
			Self::PartialStateTooLarge => "AGG_REJECT_PARTIAL_STATE_TOO_LARGE",
			Self::NumericOverflow => "AGG_NUMERIC_OVERFLOW",
			Self::RetentionExpired => "AGG_REJECT_RETENTION_EXPIRED",
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateDiagnostics {
	pub slabs_consulted: u64,
	pub slabs_pruned_by_csb: u64,
	pub slabs_answered_from_csb: u64,
	pub slabs_stats_unavailable: u64,
	pub rows_scanned: u64,
	pub rows_admitted: u64,
	pub rows_filtered_out: u64,
	pub peer_partials_merged: u32,
	pub peer_partial_bytes: u64,
	pub merge_micros: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregatePartial {
	pub expression_index: AggregateExpressionIndex,
	pub tag: AggregateFunctionTag,
	pub state_length: PartialStateLength,
	#[serde(with = "serde_bytes")]
	pub state: Vec<u8>,
}

impl AggregatePartial {
	pub fn new(
		expression_index: AggregateExpressionIndex,
		tag: AggregateFunctionTag,
		state: Vec<u8>,
	) -> std::io::Result<Self> {
		let state_length = u32::try_from(state.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"aggregate partial state exceeds u32::MAX",
			)
		})?;
		Ok(Self {
			expression_index,
			tag,
			state_length: PartialStateLength::new(state_length),
			state,
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateResponse {
	pub format_version: u16,
	pub request_id: AggregateRequestId,
	pub status: AggregateStatus,
	pub reject_code: Option<AggRejectCode>,
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub schema_fingerprint: SchemaFingerprint,
	pub aggregate_expression_digest: [u8; 32],
	pub as_of_hlc: HybridTimestamp,
	pub max_hlc: HybridTimestamp,
	pub partials: Vec<AggregatePartial>,
	pub diagnostics: AggregateDiagnostics,
}

impl AggregateResponse {
	pub fn rejected(request: &AggregateRequest, code: AggRejectCode) -> Self {
		Self {
			format_version: AGG_FORMAT_VERSION,
			request_id: request.request_id,
			status: AggregateStatus::Rejected,
			reject_code: Some(code),
			table_object_id: request.table_object_id,
			schema_generation: request.schema_generation,
			schema_fingerprint: request.schema_fingerprint,
			aggregate_expression_digest: request.expression_digest(),
			as_of_hlc: request.as_of_hlc,
			max_hlc: HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			},
			partials: Vec::new(),
			diagnostics: AggregateDiagnostics::default(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggregateState {
	Count {
		value: u64,
	},
	SignedSum {
		sum: i128,
		non_null_count: u64,
		overflow: bool,
	},
	UnsignedSum {
		sum: u128,
		non_null_count: u64,
		overflow: bool,
	},
	MinMax {
		value: Option<Vec<u8>>,
		arrow_type_tag: u8,
		truncated: bool,
	},
	SignedAverageParts {
		sum: i128,
		count: u64,
		overflow: bool,
	},
	UnsignedAverageParts {
		sum: u128,
		count: u64,
		overflow: bool,
	},
	Hll {
		precision: HllPrecision,
		registers: Vec<u8>,
	},
	Kll {
		arrow_type_tag: u8,
		k: KllK,
		total_item_count: u64,
		levels: Vec<Vec<Vec<u8>>>,
		compaction_seed: u32,
	},
	Empty,
}

impl AggregateState {
	pub fn overflowed(&self) -> bool {
		matches!(
			self,
			Self::SignedSum { overflow: true, .. }
				| Self::UnsignedSum { overflow: true, .. }
				| Self::SignedAverageParts { overflow: true, .. }
				| Self::UnsignedAverageParts { overflow: true, .. }
		)
	}

	pub fn has_truncated_minmax(&self) -> bool {
		matches!(
			self,
			Self::MinMax {
				truncated: true,
				..
			}
		)
	}
}

pub fn encode_aggregate_state(
	tag: AggregateFunctionTag,
	state: &AggregateState,
) -> std::io::Result<Vec<u8>> {
	let mut out = Vec::new();
	match (tag, state) {
		(
			AggregateFunctionTag::COUNT_STAR | AggregateFunctionTag::COUNT_NON_NULL,
			AggregateState::Count { value },
		) => {
			out.extend_from_slice(&value.to_be_bytes());
		}
		(
			AggregateFunctionTag::SIGNED_SUM,
			AggregateState::SignedSum {
				sum,
				non_null_count,
				overflow,
			},
		) => {
			if *non_null_count == 0 {
				return Ok(out);
			}
			out.extend_from_slice(&encode_signed_i128_comparable(*sum));
			out.push(u8::from(*overflow));
		}
		(
			AggregateFunctionTag::UNSIGNED_SUM,
			AggregateState::UnsignedSum {
				sum,
				non_null_count,
				overflow,
			},
		) => {
			if *non_null_count == 0 {
				return Ok(out);
			}
			out.extend_from_slice(&sum.to_be_bytes());
			out.push(u8::from(*overflow));
		}
		(
			AggregateFunctionTag::MIN | AggregateFunctionTag::MAX,
			AggregateState::MinMax {
				value, truncated, ..
			},
		) => {
			let Some(value) = value else {
				return Ok(out);
			};
			let len = u32::try_from(value.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"aggregate min/max state exceeds u32::MAX",
				)
			})?;
			out.extend_from_slice(&len.to_be_bytes());
			out.extend_from_slice(value);
			out.push(u8::from(*truncated));
		}
		(
			AggregateFunctionTag::SIGNED_AVG,
			AggregateState::SignedAverageParts {
				sum,
				count,
				overflow,
			},
		) => {
			if *count == 0 {
				return Ok(out);
			}
			out.extend_from_slice(&sum.to_be_bytes());
			out.extend_from_slice(&count.to_be_bytes());
			out.push(u8::from(*overflow));
		}
		(
			AggregateFunctionTag::UNSIGNED_AVG,
			AggregateState::UnsignedAverageParts {
				sum,
				count,
				overflow,
			},
		) => {
			if *count == 0 {
				return Ok(out);
			}
			out.extend_from_slice(&sum.to_be_bytes());
			out.extend_from_slice(&count.to_be_bytes());
			out.push(u8::from(*overflow));
		}
		(
			AggregateFunctionTag::APPROX_DISTINCT,
			AggregateState::Hll {
				precision,
				registers,
			},
		) => {
			out.push(precision.get());
			out.extend_from_slice(registers);
		}
		(
			AggregateFunctionTag::QUANTILE_KLL,
			AggregateState::Kll {
				arrow_type_tag,
				k,
				total_item_count,
				levels,
				compaction_seed,
			},
		) => {
			out.push(*arrow_type_tag);
			out.extend_from_slice(&k.get().to_be_bytes());
			out.extend_from_slice(&total_item_count.to_be_bytes());
			let level_count = u32::try_from(levels.len()).map_err(|_| {
				Error::new(ErrorKind::InvalidInput, "KLL level count exceeds u32::MAX")
			})?;
			out.extend_from_slice(&level_count.to_be_bytes());
			for level in levels {
				let level_size = u32::try_from(level.len()).map_err(|_| {
					Error::new(ErrorKind::InvalidInput, "KLL level size exceeds u32::MAX")
				})?;
				out.extend_from_slice(&level_size.to_be_bytes());
				for value in level {
					out.extend_from_slice(value);
				}
			}
			out.extend_from_slice(&compaction_seed.to_be_bytes());
		}
		(_, AggregateState::Empty) => {}
		_ => {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"aggregate state {:?} does not match tag {}",
					state,
					tag.as_str()
				),
			));
		}
	}
	Ok(out)
}

fn encode_signed_i128_comparable(value: i128) -> [u8; 16] {
	((value as u128) ^ (1_u128 << 127)).to_be_bytes()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{Consistency, Locality};

	fn request_with_aggregates(aggregates: Vec<AggregateExpression>) -> AggregateRequest {
		AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(9),
			tenant_id: TenantId::new(1),
			table_object_id: TableObjectId::new([7; 16]),
			schema_generation: 3,
			schema_fingerprint: [4; 32],
			consistency: Consistency::LINEARIZABLE,
			min_hlc: HybridTimestamp {
				physical_ms: 10,
				logical: 0,
				node_id: 1,
			},
			as_of_hlc: HybridTimestamp {
				physical_ms: 10,
				logical: 1,
				node_id: 1,
			},
			locality_ranges: vec![LocalityRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			predicate_program: None,
			aggregates,
			budgets: AggregateBudgets::default(),
		}
	}

	#[test]
	fn aggregate_catalog_tags_are_stable() {
		assert_eq!(AggregateFunctionTag::COUNT_STAR.get(), 0x01);
		assert_eq!(AggregateFunctionTag::COUNT_NON_NULL.get(), 0x02);
		assert_eq!(AggregateFunctionTag::SIGNED_SUM.get(), 0x03);
		assert_eq!(AggregateFunctionTag::UNSIGNED_SUM.get(), 0x04);
		assert_eq!(AggregateFunctionTag::MIN.get(), 0x05);
		assert_eq!(AggregateFunctionTag::MAX.get(), 0x06);
		assert_eq!(AggregateFunctionTag::SIGNED_AVG.get(), 0x07);
		assert_eq!(AggregateFunctionTag::UNSIGNED_AVG.get(), 0x08);
		assert_eq!(AggregateFunctionTag::APPROX_DISTINCT.get(), 0x09);
		assert_eq!(AggregateFunctionTag::QUANTILE_KLL.get(), 0x0A);
	}

	#[test]
	fn reject_code_discriminants_are_stable() {
		assert_eq!(AggRejectCode::FormatVersion.code(), 1);
		assert_eq!(AggRejectCode::PartialStateTooLarge.code(), 15);
		assert_eq!(AggRejectCode::NumericOverflow.code(), 16);
		assert_eq!(AggRejectCode::RetentionExpired.code(), 17);
	}

	#[test]
	fn request_rejects_more_than_64_aggregates() {
		let request = request_with_aggregates(vec![AggregateExpression::count_star(); 65]);
		assert_eq!(
			request.validate_envelope(false).unwrap_err(),
			AggRejectCode::TooManyAggregates
		);
	}

	#[test]
	fn exact_only_rejects_approximate_aggregates() {
		let request = request_with_aggregates(vec![AggregateExpression {
			tag: AggregateFunctionTag::APPROX_DISTINCT,
			column_ordinal: 1,
			parameter_block: Vec::new(),
		}]);
		assert_eq!(
			request.validate_envelope(true).unwrap_err(),
			AggRejectCode::ApproxNotAllowed
		);
	}

	#[test]
	fn state_encoding_matches_adr040_fixed_widths() {
		assert_eq!(
			encode_aggregate_state(
				AggregateFunctionTag::COUNT_STAR,
				&AggregateState::Count { value: 9 }
			)
			.unwrap(),
			9_u64.to_be_bytes()
		);

		let signed = encode_aggregate_state(
			AggregateFunctionTag::SIGNED_SUM,
			&AggregateState::SignedSum {
				sum: -2,
				non_null_count: 1,
				overflow: true,
			},
		)
		.unwrap();
		assert_eq!(signed.len(), 17);
		assert_eq!(signed[16], 1);

		let min = encode_aggregate_state(
			AggregateFunctionTag::MIN,
			&AggregateState::MinMax {
				value: Some(vec![0xAA, 0xBB]),
				arrow_type_tag: 0x05,
				truncated: false,
			},
		)
		.unwrap();
		assert_eq!(min, vec![0, 0, 0, 2, 0xAA, 0xBB, 0]);
	}
}
