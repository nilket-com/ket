use crate::schema::{
	AGG_MIN_MAX_PREFIX_BYTES, AggregateExpressionIndex, AggregateFunction, AggregateFunctionTag,
	AggregatePartial, AggregateRequest, AggregateState, BufferValueCount, RowCount,
	encode_aggregate_state,
};
use crate::storage::encoding::compute::{
	EncodedRowMask, EncodingComputeRejectCode, decode_encoded_buffer,
};
use crate::storage::encoding::{BufferEncoding, EncodingCascade};
use crate::storage::stats::{
	ARROW_STATS_TAG_BOOLEAN, ARROW_STATS_TAG_DATE32, ARROW_STATS_TAG_DATE64, ARROW_STATS_TAG_INT8,
	ARROW_STATS_TAG_INT16, ARROW_STATS_TAG_INT32, ARROW_STATS_TAG_INT64,
	ARROW_STATS_TAG_TIMESTAMP_MICROSECOND, ARROW_STATS_TAG_TIMESTAMP_MILLISECOND,
	ARROW_STATS_TAG_TIMESTAMP_NANOSECOND, ARROW_STATS_TAG_UINT8, ARROW_STATS_TAG_UINT16,
	ARROW_STATS_TAG_UINT32, ARROW_STATS_TAG_UINT64,
};
use arrow::datatypes::{DataType, TimeUnit};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EncodedAggregateContext {
	pub row_count: RowCount,
	pub column_non_null_counts: BTreeMap<u16, RowCount>,
	pub column_validity_masks: BTreeMap<u16, EncodedRowMask>,
	pub columns: BTreeMap<u16, EncodedAggregateColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedAggregateColumn {
	pub data_type: DataType,
	pub decoded_bytes: Vec<u8>,
	pub decoded_value_count: BufferValueCount,
	pub validity_mask: Option<EncodedRowMask>,
}

impl EncodedAggregateContext {
	pub fn new(row_count: RowCount) -> Self {
		Self {
			row_count,
			column_non_null_counts: BTreeMap::new(),
			column_validity_masks: BTreeMap::new(),
			columns: BTreeMap::new(),
		}
	}

	pub fn with_decoded_column(
		mut self,
		column: u16,
		data_type: DataType,
		decoded_bytes: Vec<u8>,
	) -> Self {
		let validity_mask = self.column_validity_masks.get(&column).cloned();
		self.columns.insert(
			column,
			EncodedAggregateColumn {
				data_type,
				decoded_bytes,
				decoded_value_count: BufferValueCount::new(self.row_count.get() as u32),
				validity_mask,
			},
		);
		self
	}

	pub fn with_validity_mask(
		mut self,
		column: u16,
		mask: EncodedRowMask,
	) -> Result<Self, EncodingComputeRejectCode> {
		if mask.rows() != self.row_count {
			return Err(EncodingComputeRejectCode::MaskLengthMismatch);
		}
		if let Some(existing) = self.columns.get_mut(&column) {
			existing.validity_mask = Some(mask.clone());
		}
		self.column_validity_masks.insert(column, mask);
		Ok(self)
	}

	pub fn with_encoded_column(
		mut self,
		column: u16,
		data_type: DataType,
		encoded_bytes: &[u8],
		encoding: &BufferEncoding,
		cascade: &EncodingCascade,
	) -> Result<Self, EncodingComputeRejectCode> {
		let decoded = decode_encoded_buffer(encoded_bytes, &data_type, cascade, encoding)
			.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let validity_mask = self.column_validity_masks.get(&column).cloned();
		self.columns.insert(
			column,
			EncodedAggregateColumn {
				data_type,
				decoded_bytes: decoded,
				decoded_value_count: encoding.decoded_value_count,
				validity_mask,
			},
		);
		Ok(self)
	}
}

pub fn try_fold_encoded_aggregate_request(
	request: &AggregateRequest,
	context: &EncodedAggregateContext,
	mask: Option<&EncodedRowMask>,
) -> Result<Option<Vec<AggregatePartial>>, EncodingComputeRejectCode> {
	let mut functions = Vec::with_capacity(request.aggregates.len());
	for expression in &request.aggregates {
		let Ok(function) = expression.function() else {
			return Ok(None);
		};
		if !aggregate_function_supported_by_context(&function, context, mask) {
			return Ok(None);
		}
		functions.push(function);
	}

	let selected = selected_rows(context, mask)?;
	let mut partials = Vec::with_capacity(request.aggregates.len());
	for (index, (expression, function)) in request.aggregates.iter().zip(functions).enumerate() {
		let state = fold_aggregate_function(function, context, &selected)?;
		let state = encode_aggregate_state(expression.tag, &state)
			.map_err(|_| EncodingComputeRejectCode::AggregateOverflowMismatch)?;
		let expression_index = AggregateExpressionIndex::new(
			u16::try_from(index)
				.map_err(|_| EncodingComputeRejectCode::AggregateOverflowMismatch)?,
		);
		let partial = AggregatePartial::new(expression_index, expression.tag, state)
			.map_err(|_| EncodingComputeRejectCode::AggregateOverflowMismatch)?;
		partials.push(partial);
	}
	Ok(Some(partials))
}

fn aggregate_function_supported_by_context(
	function: &AggregateFunction,
	context: &EncodedAggregateContext,
	mask: Option<&EncodedRowMask>,
) -> bool {
	match function {
		AggregateFunction::CountStar => true,
		AggregateFunction::CountNonNull { column } => {
			context.columns.contains_key(column)
				|| context.column_validity_masks.contains_key(column)
				|| context
					.column_non_null_counts
					.get(column)
					.is_some_and(|count| {
						mask.is_none() || count.get() == 0 || count.get() == context.row_count.get()
					})
		}
		AggregateFunction::Min { column } | AggregateFunction::Max { column } => context
			.columns
			.get(column)
			.is_some_and(|column| aggregate_min_max_type_tag(&column.data_type).is_some()),
		AggregateFunction::SignedSum { column }
		| AggregateFunction::SignedAverageParts { column } => context
			.columns
			.get(column)
			.is_some_and(|column| signed_integer_width(&column.data_type).is_some()),
		AggregateFunction::UnsignedSum { column }
		| AggregateFunction::UnsignedAverageParts { column } => context
			.columns
			.get(column)
			.is_some_and(|column| unsigned_integer_width(&column.data_type).is_some()),
		AggregateFunction::ApproxDistinct { .. } | AggregateFunction::Quantile { .. } => false,
	}
}

fn selected_rows(
	context: &EncodedAggregateContext,
	mask: Option<&EncodedRowMask>,
) -> Result<Vec<bool>, EncodingComputeRejectCode> {
	match mask {
		Some(mask) => {
			if mask.rows() != context.row_count {
				return Err(EncodingComputeRejectCode::MaskLengthMismatch);
			}
			mask.to_bool_vec()
		}
		None => Ok(vec![true; context.row_count.get() as usize]),
	}
}

fn fold_aggregate_function(
	function: AggregateFunction,
	context: &EncodedAggregateContext,
	selected: &[bool],
) -> Result<AggregateState, EncodingComputeRejectCode> {
	match function {
		AggregateFunction::CountStar => Ok(AggregateState::Count {
			value: selected.iter().filter(|selected| **selected).count() as u64,
		}),
		AggregateFunction::CountNonNull { column } => {
			if let Some(count) = context.column_non_null_counts.get(&column)
				&& count.get() == 0
			{
				return Ok(AggregateState::Count { value: 0 });
			}
			if let Some(count) = context.column_non_null_counts.get(&column)
				&& count.get() == context.row_count.get()
			{
				return Ok(AggregateState::Count {
					value: selected.iter().filter(|selected| **selected).count() as u64,
				});
			}
			if selected.iter().all(|selected| *selected)
				&& let Some(count) = context.column_non_null_counts.get(&column)
			{
				return Ok(AggregateState::Count { value: count.get() });
			}
			if let Some(mask) = context.column_validity_masks.get(&column) {
				let valid = mask.to_bool_vec()?;
				return Ok(AggregateState::Count {
					value: selected
						.iter()
						.zip(valid)
						.filter(|(selected, valid)| **selected && *valid)
						.count() as u64,
				});
			}
			let column = context
				.columns
				.get(&column)
				.ok_or(EncodingComputeRejectCode::AggregateOverflowMismatch)?;
			Ok(AggregateState::Count {
				value: selected_valid_rows(column, selected)? as u64,
			})
		}
		AggregateFunction::SignedSum { column } => {
			let values = signed_integer_values(column_ref(context, column)?)?;
			let (sum, count, overflow) =
				fold_signed_sum(&values, column_ref(context, column)?, selected)?;
			Ok(AggregateState::SignedSum {
				sum,
				non_null_count: count,
				overflow,
			})
		}
		AggregateFunction::UnsignedSum { column } => {
			let values = unsigned_integer_values(column_ref(context, column)?)?;
			let (sum, count, overflow) =
				fold_unsigned_sum(&values, column_ref(context, column)?, selected)?;
			Ok(AggregateState::UnsignedSum {
				sum,
				non_null_count: count,
				overflow,
			})
		}
		AggregateFunction::SignedAverageParts { column } => {
			let values = signed_integer_values(column_ref(context, column)?)?;
			let (sum, count, overflow) =
				fold_signed_sum(&values, column_ref(context, column)?, selected)?;
			Ok(AggregateState::SignedAverageParts {
				sum,
				count,
				overflow,
			})
		}
		AggregateFunction::UnsignedAverageParts { column } => {
			let values = unsigned_integer_values(column_ref(context, column)?)?;
			let (sum, count, overflow) =
				fold_unsigned_sum(&values, column_ref(context, column)?, selected)?;
			Ok(AggregateState::UnsignedAverageParts {
				sum,
				count,
				overflow,
			})
		}
		AggregateFunction::Min { column } => {
			fold_min_max(column_ref(context, column)?, selected, true)
		}
		AggregateFunction::Max { column } => {
			fold_min_max(column_ref(context, column)?, selected, false)
		}
		AggregateFunction::ApproxDistinct { .. } | AggregateFunction::Quantile { .. } => {
			Err(EncodingComputeRejectCode::AggregateOverflowMismatch)
		}
	}
}

fn column_ref(
	context: &EncodedAggregateContext,
	column: u16,
) -> Result<&EncodedAggregateColumn, EncodingComputeRejectCode> {
	context
		.columns
		.get(&column)
		.ok_or(EncodingComputeRejectCode::AggregateOverflowMismatch)
}

fn selected_valid_rows(
	column: &EncodedAggregateColumn,
	selected: &[bool],
) -> Result<usize, EncodingComputeRejectCode> {
	let validity = validity_values(column)?;
	Ok(selected
		.iter()
		.zip(validity)
		.filter(|(selected, valid)| **selected && *valid)
		.count())
}

fn fold_signed_sum(
	values: &[i128],
	column: &EncodedAggregateColumn,
	selected: &[bool],
) -> Result<(i128, u64, bool), EncodingComputeRejectCode> {
	let validity = validity_values(column)?;
	let mut sum = 0_i128;
	let mut count = 0_u64;
	let mut overflow = false;
	for ((value, selected), valid) in values.iter().zip(selected).zip(validity) {
		if !*selected || !valid {
			continue;
		}
		count = count.saturating_add(1);
		add_i128(&mut sum, *value, &mut overflow);
	}
	Ok((sum, count, overflow))
}

fn fold_unsigned_sum(
	values: &[u128],
	column: &EncodedAggregateColumn,
	selected: &[bool],
) -> Result<(u128, u64, bool), EncodingComputeRejectCode> {
	let validity = validity_values(column)?;
	let mut sum = 0_u128;
	let mut count = 0_u64;
	let mut overflow = false;
	for ((value, selected), valid) in values.iter().zip(selected).zip(validity) {
		if !*selected || !valid {
			continue;
		}
		count = count.saturating_add(1);
		add_u128(&mut sum, *value, &mut overflow);
	}
	Ok((sum, count, overflow))
}

fn fold_min_max(
	column: &EncodedAggregateColumn,
	selected: &[bool],
	min: bool,
) -> Result<AggregateState, EncodingComputeRejectCode> {
	let arrow_type_tag = aggregate_min_max_type_tag(&column.data_type)
		.ok_or(EncodingComputeRejectCode::AggregateOverflowMismatch)?;
	let values = canonical_values(column)?;
	let validity = validity_values(column)?;
	let mut best = None::<Vec<u8>>;
	let mut truncated = false;
	for ((candidate, selected), valid) in values.into_iter().zip(selected).zip(validity) {
		if !*selected || !valid {
			continue;
		}
		let replace = best
			.as_deref()
			.map(|current| {
				if min {
					candidate.as_slice() < current
				} else {
					candidate.as_slice() > current
				}
			})
			.unwrap_or(true);
		if replace {
			truncated = candidate.len() > AGG_MIN_MAX_PREFIX_BYTES;
			let mut candidate = candidate;
			if truncated {
				candidate.truncate(AGG_MIN_MAX_PREFIX_BYTES);
			}
			best = Some(candidate);
		}
	}
	Ok(AggregateState::MinMax {
		value: best,
		arrow_type_tag,
		truncated,
	})
}

fn validity_values(
	column: &EncodedAggregateColumn,
) -> Result<Vec<bool>, EncodingComputeRejectCode> {
	match &column.validity_mask {
		Some(mask) => {
			if mask.rows().get() != u64::from(column.decoded_value_count.get()) {
				return Err(EncodingComputeRejectCode::MaskLengthMismatch);
			}
			mask.to_bool_vec()
		}
		None => Ok(vec![true; column.decoded_value_count.get() as usize]),
	}
}

fn signed_integer_values(
	column: &EncodedAggregateColumn,
) -> Result<Vec<i128>, EncodingComputeRejectCode> {
	let width = signed_integer_width(&column.data_type)
		.ok_or(EncodingComputeRejectCode::AggregateOverflowMismatch)?;
	decode_fixed_width(column, width, |bytes| match width {
		1 => i128::from(i8::from_le_bytes([bytes[0]])),
		2 => i128::from(i16::from_le_bytes([bytes[0], bytes[1]])),
		4 => i128::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
		8 => i128::from(i64::from_le_bytes([
			bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
		])),
		_ => 0,
	})
}

fn unsigned_integer_values(
	column: &EncodedAggregateColumn,
) -> Result<Vec<u128>, EncodingComputeRejectCode> {
	let width = unsigned_integer_width(&column.data_type)
		.ok_or(EncodingComputeRejectCode::AggregateOverflowMismatch)?;
	decode_fixed_width(column, width, |bytes| match width {
		1 => u128::from(bytes[0]),
		2 => u128::from(u16::from_le_bytes([bytes[0], bytes[1]])),
		4 => u128::from(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
		8 => u128::from(u64::from_le_bytes([
			bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
		])),
		_ => 0,
	})
}

fn canonical_values(
	column: &EncodedAggregateColumn,
) -> Result<Vec<Vec<u8>>, EncodingComputeRejectCode> {
	match &column.data_type {
		DataType::Boolean => {
			let rows = column.decoded_value_count.get() as usize;
			let needed = rows.div_ceil(8);
			if column.decoded_bytes.len() < needed {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			Ok((0..rows)
				.map(|row| {
					vec![u8::from(
						column.decoded_bytes[row / 8] & (1_u8 << (row % 8)) != 0,
					)]
				})
				.collect())
		}
		DataType::Int8
		| DataType::Int16
		| DataType::Int32
		| DataType::Int64
		| DataType::Date32
		| DataType::Date64
		| DataType::Timestamp(_, _) => {
			let width = signed_canonical_width(&column.data_type)
				.ok_or(EncodingComputeRejectCode::AggregateOverflowMismatch)?;
			let values = decode_signed_canonical_values(column, width)?;
			Ok(values
				.into_iter()
				.map(|value| encode_signed_value(value, width))
				.collect())
		}
		DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
			let width = unsigned_integer_width(&column.data_type)
				.ok_or(EncodingComputeRejectCode::AggregateOverflowMismatch)?;
			let values = unsigned_integer_values(column)?;
			Ok(values
				.into_iter()
				.map(|value| encode_unsigned_value(value, width))
				.collect())
		}
		DataType::FixedSizeBinary(width) => {
			let width = usize::try_from(*width)
				.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			decode_fixed_width(column, width, |bytes| bytes.to_vec())
		}
		_ => Err(EncodingComputeRejectCode::AggregateOverflowMismatch),
	}
}

fn decode_signed_canonical_values(
	column: &EncodedAggregateColumn,
	width: usize,
) -> Result<Vec<i128>, EncodingComputeRejectCode> {
	decode_fixed_width(column, width, |bytes| match width {
		1 => i128::from(i8::from_le_bytes([bytes[0]])),
		2 => i128::from(i16::from_le_bytes([bytes[0], bytes[1]])),
		4 => i128::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
		8 => i128::from(i64::from_le_bytes([
			bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
		])),
		_ => 0,
	})
}

fn decode_fixed_width<T>(
	column: &EncodedAggregateColumn,
	width: usize,
	mut decode: impl FnMut(&[u8]) -> T,
) -> Result<Vec<T>, EncodingComputeRejectCode> {
	let rows = column.decoded_value_count.get() as usize;
	let needed = rows
		.checked_mul(width)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if column.decoded_bytes.len() < needed {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut out = Vec::with_capacity(rows);
	for row in 0..rows {
		let start = row * width;
		out.push(decode(&column.decoded_bytes[start..start + width]));
	}
	Ok(out)
}

fn signed_integer_width(data_type: &DataType) -> Option<usize> {
	match data_type {
		DataType::Int8 => Some(1),
		DataType::Int16 => Some(2),
		DataType::Int32 => Some(4),
		DataType::Int64 => Some(8),
		_ => None,
	}
}

fn unsigned_integer_width(data_type: &DataType) -> Option<usize> {
	match data_type {
		DataType::UInt8 => Some(1),
		DataType::UInt16 => Some(2),
		DataType::UInt32 => Some(4),
		DataType::UInt64 => Some(8),
		_ => None,
	}
}

fn signed_canonical_width(data_type: &DataType) -> Option<usize> {
	match data_type {
		DataType::Int8 => Some(1),
		DataType::Int16 => Some(2),
		DataType::Int32 | DataType::Date32 => Some(4),
		DataType::Int64
		| DataType::Date64
		| DataType::Timestamp(TimeUnit::Millisecond, _)
		| DataType::Timestamp(TimeUnit::Microsecond, _)
		| DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(8),
		_ => None,
	}
}

fn aggregate_min_max_type_tag(data_type: &DataType) -> Option<u8> {
	match data_type {
		DataType::Int8 => Some(ARROW_STATS_TAG_INT8),
		DataType::Int16 => Some(ARROW_STATS_TAG_INT16),
		DataType::Int32 => Some(ARROW_STATS_TAG_INT32),
		DataType::Int64 => Some(ARROW_STATS_TAG_INT64),
		DataType::UInt8 => Some(ARROW_STATS_TAG_UINT8),
		DataType::UInt16 => Some(ARROW_STATS_TAG_UINT16),
		DataType::UInt32 => Some(ARROW_STATS_TAG_UINT32),
		DataType::UInt64 => Some(ARROW_STATS_TAG_UINT64),
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

fn encode_signed_value(value: i128, byte_width: usize) -> Vec<u8> {
	let bits = byte_width * 8;
	let sign_bit = 1_u128 << (bits - 1);
	let mask = if bits == 128 {
		u128::MAX
	} else {
		(1_u128 << bits) - 1
	};
	let raw = (value as u128) & mask;
	let comparable = raw ^ sign_bit;
	let be = comparable.to_be_bytes();
	be[16 - byte_width..].to_vec()
}

fn encode_unsigned_value(value: u128, byte_width: usize) -> Vec<u8> {
	let be = value.to_be_bytes();
	be[16 - byte_width..].to_vec()
}

fn add_i128(sum: &mut i128, value: i128, overflow: &mut bool) {
	if let Some(next) = sum.checked_add(value) {
		*sum = next;
	} else {
		*sum = sum.saturating_add(value);
		*overflow = true;
		crate::metrics::global().observe_agg_int_overflow();
	}
}

fn add_u128(sum: &mut u128, value: u128, overflow: &mut bool) {
	if let Some(next) = sum.checked_add(value) {
		*sum = next;
	} else {
		*sum = sum.saturating_add(value);
		*overflow = true;
		crate::metrics::global().observe_agg_int_overflow();
	}
}

pub fn aggregate_kind_for_tag(
	tag: AggregateFunctionTag,
) -> Option<crate::storage::encoding::compute::EncodedAggregateKind> {
	use crate::storage::encoding::compute::EncodedAggregateKind;
	match tag {
		AggregateFunctionTag::COUNT_STAR => Some(EncodedAggregateKind::CountStar),
		AggregateFunctionTag::COUNT_NON_NULL => Some(EncodedAggregateKind::CountNonNull),
		AggregateFunctionTag::MIN => Some(EncodedAggregateKind::Min),
		AggregateFunctionTag::MAX => Some(EncodedAggregateKind::Max),
		AggregateFunctionTag::SIGNED_SUM => Some(EncodedAggregateKind::SignedSum),
		AggregateFunctionTag::UNSIGNED_SUM => Some(EncodedAggregateKind::UnsignedSum),
		AggregateFunctionTag::SIGNED_AVG => Some(EncodedAggregateKind::SignedAvg),
		AggregateFunctionTag::UNSIGNED_AVG => Some(EncodedAggregateKind::UnsignedAvg),
		_ => None,
	}
}
