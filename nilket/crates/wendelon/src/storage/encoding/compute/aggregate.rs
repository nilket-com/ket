use crate::schema::{
	AGG_MIN_MAX_PREFIX_BYTES, AggregateExpressionIndex, AggregateFunction, AggregateFunctionTag,
	AggregatePartial, AggregateRequest, AggregateState, BufferValueCount, RecordBodyByteLen,
	RowCount, encode_aggregate_state,
};
use crate::storage::encoding::cascade::{
	DictionaryIndexEncoding, RleByteRunIter, parse_bitpacked_frame, parse_delta_frame,
	parse_fixed_width_dictionary_frame,
};
use crate::storage::encoding::compute::{
	EncodedRowMask, EncodingComputeRejectCode, decode_encoded_buffer,
	kernels::scalar::{
		decoded_fixed_width_bytes_per_value, fixed_width_for_data_type, map_encoding_reject,
		read_unsigned_le_as_u128, signed_value_from_le_bytes, signed_value_from_u128,
	},
};
use crate::storage::encoding::page::expand_page_descriptors;
use crate::storage::encoding::{BufferEncoding, EncodingCascade, EncodingLeafCascade};
use crate::storage::stats::{
	ARROW_STATS_TAG_BOOLEAN, ARROW_STATS_TAG_DATE32, ARROW_STATS_TAG_DATE64, ARROW_STATS_TAG_INT8,
	ARROW_STATS_TAG_INT16, ARROW_STATS_TAG_INT32, ARROW_STATS_TAG_INT64,
	ARROW_STATS_TAG_TIMESTAMP_MICROSECOND, ARROW_STATS_TAG_TIMESTAMP_MILLISECOND,
	ARROW_STATS_TAG_TIMESTAMP_NANOSECOND, ARROW_STATS_TAG_UINT8, ARROW_STATS_TAG_UINT16,
	ARROW_STATS_TAG_UINT32, ARROW_STATS_TAG_UINT64,
};
use arrow::datatypes::{DataType, TimeUnit};
use std::collections::BTreeMap;

const DICTIONARY_AGG_STACK_COUNTS: usize = 4_096;

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
	pub encoded_source: Option<EncodedAggregateSource>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedAggregateSource {
	pub encoded_bytes: Vec<u8>,
	pub encoding: BufferEncoding,
	pub cascade: EncodingCascade,
}

struct SelectedRows {
	values: Vec<bool>,
	selected_count: u64,
}

impl SelectedRows {
	fn all(row_count: RowCount) -> Self {
		let rows = row_count.get() as usize;
		Self {
			values: vec![true; rows],
			selected_count: row_count.get(),
		}
	}

	fn as_slice(&self) -> &[bool] {
		&self.values
	}
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
				encoded_source: None,
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
				encoded_source: Some(EncodedAggregateSource {
					encoded_bytes: encoded_bytes.to_vec(),
					encoding: encoding.clone(),
					cascade: cascade.clone(),
				}),
			},
		);
		Ok(self)
	}

	pub fn with_native_encoded_column(
		mut self,
		column: u16,
		data_type: DataType,
		encoded_bytes: &[u8],
		encoding: &BufferEncoding,
		cascade: &EncodingCascade,
	) -> Result<Self, EncodingComputeRejectCode> {
		let validity_mask = self.column_validity_masks.get(&column).cloned();
		self.columns.insert(
			column,
			EncodedAggregateColumn {
				data_type,
				decoded_bytes: Vec::new(),
				decoded_value_count: encoding.decoded_value_count,
				validity_mask,
				encoded_source: Some(EncodedAggregateSource {
					encoded_bytes: encoded_bytes.to_vec(),
					encoding: encoding.clone(),
					cascade: cascade.clone(),
				}),
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

	if let Some(mask) = mask
		&& let Some(partials) = try_fold_native_masked_aggregate_request(request, context, mask)?
	{
		return Ok(Some(partials));
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

fn try_fold_native_masked_aggregate_request(
	request: &AggregateRequest,
	context: &EncodedAggregateContext,
	mask: &EncodedRowMask,
) -> Result<Option<Vec<AggregatePartial>>, EncodingComputeRejectCode> {
	if mask.rows() != context.row_count {
		return Err(EncodingComputeRejectCode::MaskLengthMismatch);
	}
	let Some(selection) = AggregateMaskSelection::new(mask)? else {
		return Ok(None);
	};
	let selected_count = mask.popcount()?.get();
	let mut partials = Vec::with_capacity(request.aggregates.len());
	for (index, expression) in request.aggregates.iter().enumerate() {
		let Ok(function) = expression.function() else {
			return Ok(None);
		};
		let state = match function {
			AggregateFunction::CountStar => AggregateState::Count {
				value: selected_count,
			},
			AggregateFunction::SignedSum { column } => {
				let column = column_ref(context, column)?;
				let Some((sum, count, overflow)) = fold_native_signed_sum_mask(column, &selection)?
				else {
					return Ok(None);
				};
				AggregateState::SignedSum {
					sum,
					non_null_count: count,
					overflow,
				}
			}
			AggregateFunction::UnsignedSum { column } => {
				let column = column_ref(context, column)?;
				let Some((sum, count, overflow)) =
					fold_native_unsigned_sum_mask(column, &selection)?
				else {
					return Ok(None);
				};
				AggregateState::UnsignedSum {
					sum,
					non_null_count: count,
					overflow,
				}
			}
			AggregateFunction::SignedAverageParts { column } => {
				let column = column_ref(context, column)?;
				let Some((sum, count, overflow)) = fold_native_signed_sum_mask(column, &selection)?
				else {
					return Ok(None);
				};
				AggregateState::SignedAverageParts {
					sum,
					count,
					overflow,
				}
			}
			AggregateFunction::UnsignedAverageParts { column } => {
				let column = column_ref(context, column)?;
				let Some((sum, count, overflow)) =
					fold_native_unsigned_sum_mask(column, &selection)?
				else {
					return Ok(None);
				};
				AggregateState::UnsignedAverageParts {
					sum,
					count,
					overflow,
				}
			}
			AggregateFunction::CountNonNull { .. }
			| AggregateFunction::Min { .. }
			| AggregateFunction::Max { .. }
			| AggregateFunction::ApproxDistinct { .. }
			| AggregateFunction::Quantile { .. } => return Ok(None),
		};
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

enum AggregateMaskSelection<'a> {
	All { rows: usize },
	None { rows: usize },
	Bitmap { rows: usize, bytes: &'a [u8] },
}

impl<'a> AggregateMaskSelection<'a> {
	fn new(mask: &'a EncodedRowMask) -> Result<Option<Self>, EncodingComputeRejectCode> {
		mask.validate()?;
		let rows = usize::try_from(mask.rows().get())
			.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
		Ok(match mask {
			EncodedRowMask::All { .. } => Some(Self::All { rows }),
			EncodedRowMask::None { .. } => Some(Self::None { rows }),
			EncodedRowMask::Bitmap { bytes, .. } => Some(Self::Bitmap { rows, bytes }),
			EncodedRowMask::Runs { .. } => None,
		})
	}

	fn rows(&self) -> usize {
		match self {
			Self::All { rows } | Self::None { rows } | Self::Bitmap { rows, .. } => *rows,
		}
	}
}

pub(crate) fn native_aggregate_cascade_eligible(
	function: &AggregateFunction,
	data_type: &DataType,
	cascade: Option<&EncodingCascade>,
) -> bool {
	match function {
		AggregateFunction::CountStar | AggregateFunction::CountNonNull { .. } => true,
		AggregateFunction::UnsignedSum { .. } | AggregateFunction::UnsignedAverageParts { .. } => {
			native_unsigned_aggregate_cascade_eligible(data_type, cascade)
		}
		AggregateFunction::SignedSum { .. }
		| AggregateFunction::SignedAverageParts { .. }
		| AggregateFunction::Min { .. }
		| AggregateFunction::Max { .. } => false,
		AggregateFunction::ApproxDistinct { .. } | AggregateFunction::Quantile { .. } => false,
	}
}

fn native_unsigned_aggregate_cascade_eligible(
	data_type: &DataType,
	cascade: Option<&EncodingCascade>,
) -> bool {
	if unsigned_integer_width(data_type).is_none() {
		return false;
	}
	match cascade {
		Some(EncodingCascade::BitPacked { bits, .. }) => {
			matches!(data_type, DataType::UInt8) && bits.get() == 8
		}
		Some(EncodingCascade::Dictionary {
			dict_value_count,
			inner,
			..
		}) => dict_value_count.get() <= 4 && matches!(inner.as_ref(), EncodingLeafCascade::Plain),
		_ => false,
	}
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
) -> Result<SelectedRows, EncodingComputeRejectCode> {
	match mask {
		Some(mask) => {
			if mask.rows() != context.row_count {
				return Err(EncodingComputeRejectCode::MaskLengthMismatch);
			}
			let selected_count = mask.popcount()?.get();
			Ok(SelectedRows {
				values: mask.to_bool_vec()?,
				selected_count,
			})
		}
		None => Ok(SelectedRows::all(context.row_count)),
	}
}

fn fold_aggregate_function(
	function: AggregateFunction,
	context: &EncodedAggregateContext,
	selected: &SelectedRows,
) -> Result<AggregateState, EncodingComputeRejectCode> {
	match function {
		AggregateFunction::CountStar => Ok(AggregateState::Count {
			value: selected.selected_count,
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
					value: selected.selected_count,
				});
			}
			if selected.as_slice().iter().all(|selected| *selected)
				&& let Some(count) = context.column_non_null_counts.get(&column)
			{
				return Ok(AggregateState::Count { value: count.get() });
			}
			if let Some(mask) = context.column_validity_masks.get(&column) {
				let valid = mask.to_bool_vec()?;
				return Ok(AggregateState::Count {
					value: selected
						.as_slice()
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
				value: selected_valid_rows(column, selected.as_slice())? as u64,
			})
		}
		AggregateFunction::SignedSum { column } => {
			let column = column_ref(context, column)?;
			if let Some((sum, count, overflow)) = fold_native_signed_sum(column, selected)? {
				return Ok(AggregateState::SignedSum {
					sum,
					non_null_count: count,
					overflow,
				});
			}
			let values = signed_integer_values(column)?;
			let (sum, count, overflow) = fold_signed_sum(&values, column, selected.as_slice())?;
			Ok(AggregateState::SignedSum {
				sum,
				non_null_count: count,
				overflow,
			})
		}
		AggregateFunction::UnsignedSum { column } => {
			let column = column_ref(context, column)?;
			if let Some((sum, count, overflow)) = fold_native_unsigned_sum(column, selected)? {
				return Ok(AggregateState::UnsignedSum {
					sum,
					non_null_count: count,
					overflow,
				});
			}
			let values = unsigned_integer_values(column)?;
			let (sum, count, overflow) = fold_unsigned_sum(&values, column, selected.as_slice())?;
			Ok(AggregateState::UnsignedSum {
				sum,
				non_null_count: count,
				overflow,
			})
		}
		AggregateFunction::SignedAverageParts { column } => {
			let column = column_ref(context, column)?;
			if let Some((sum, count, overflow)) = fold_native_signed_sum(column, selected)? {
				return Ok(AggregateState::SignedAverageParts {
					sum,
					count,
					overflow,
				});
			}
			let values = signed_integer_values(column)?;
			let (sum, count, overflow) = fold_signed_sum(&values, column, selected.as_slice())?;
			Ok(AggregateState::SignedAverageParts {
				sum,
				count,
				overflow,
			})
		}
		AggregateFunction::UnsignedAverageParts { column } => {
			let column = column_ref(context, column)?;
			if let Some((sum, count, overflow)) = fold_native_unsigned_sum(column, selected)? {
				return Ok(AggregateState::UnsignedAverageParts {
					sum,
					count,
					overflow,
				});
			}
			let values = unsigned_integer_values(column)?;
			let (sum, count, overflow) = fold_unsigned_sum(&values, column, selected.as_slice())?;
			Ok(AggregateState::UnsignedAverageParts {
				sum,
				count,
				overflow,
			})
		}
		AggregateFunction::Min { column } => {
			let column = column_ref(context, column)?;
			if let Some(state) = fold_native_min_max(column, selected.as_slice(), true)? {
				return Ok(state);
			}
			fold_min_max(column, selected.as_slice(), true)
		}
		AggregateFunction::Max { column } => {
			let column = column_ref(context, column)?;
			if let Some(state) = fold_native_min_max(column, selected.as_slice(), false)? {
				return Ok(state);
			}
			fold_min_max(column, selected.as_slice(), false)
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

fn fold_native_signed_sum(
	column: &EncodedAggregateColumn,
	selected: &SelectedRows,
) -> Result<Option<(i128, u64, bool)>, EncodingComputeRejectCode> {
	if signed_integer_width(&column.data_type).is_none() {
		return Ok(None);
	}
	let Some(source) = &column.encoded_source else {
		return Ok(None);
	};
	if !matches!(
		source.cascade,
		EncodingCascade::BitPacked { .. }
			| EncodingCascade::Dictionary { .. }
			| EncodingCascade::Delta { .. }
			| EncodingCascade::Rle { .. }
	) {
		return Ok(None);
	}
	if column.validity_mask.is_none()
		&& let Some(result) = fold_signed_sum_no_validity(column, source, selected)?
	{
		return Ok(Some(result));
	}
	let validity = validity_values(column)?;
	if selected.values.len() != source.encoding.decoded_value_count.get() as usize
		|| validity.len() != selected.values.len()
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut sum = 0_i128;
	let mut count = 0_u64;
	let mut overflow = false;
	fold_signed_encoded_values(
		column,
		source,
		selected.as_slice(),
		&validity,
		&mut sum,
		&mut count,
		&mut overflow,
	)?;
	Ok(Some((sum, count, overflow)))
}

fn fold_native_unsigned_sum(
	column: &EncodedAggregateColumn,
	selected: &SelectedRows,
) -> Result<Option<(u128, u64, bool)>, EncodingComputeRejectCode> {
	if unsigned_integer_width(&column.data_type).is_none() {
		return Ok(None);
	}
	let Some(source) = &column.encoded_source else {
		return Ok(None);
	};
	if !matches!(
		source.cascade,
		EncodingCascade::BitPacked { .. }
			| EncodingCascade::Dictionary { .. }
			| EncodingCascade::Delta { .. }
			| EncodingCascade::Rle { .. }
	) {
		return Ok(None);
	}
	if column.validity_mask.is_none()
		&& let Some(result) = fold_unsigned_sum_no_validity(column, source, selected)?
	{
		return Ok(Some(result));
	}
	let validity = validity_values(column)?;
	if selected.values.len() != source.encoding.decoded_value_count.get() as usize
		|| validity.len() != selected.values.len()
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut sum = 0_u128;
	let mut count = 0_u64;
	let mut overflow = false;
	fold_unsigned_encoded_values(
		column,
		source,
		selected.as_slice(),
		&validity,
		&mut sum,
		&mut count,
		&mut overflow,
	)?;
	Ok(Some((sum, count, overflow)))
}

fn fold_native_signed_sum_mask(
	column: &EncodedAggregateColumn,
	selected: &AggregateMaskSelection<'_>,
) -> Result<Option<(i128, u64, bool)>, EncodingComputeRejectCode> {
	if column.validity_mask.is_some() || signed_integer_width(&column.data_type).is_none() {
		return Ok(None);
	}
	let Some(source) = &column.encoded_source else {
		return Ok(None);
	};
	if selected.rows() != source.encoding.decoded_value_count.get() as usize {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let value_width = decoded_fixed_width_bytes_per_value(&source.encoding)?;
	if fixed_width_for_data_type(&column.data_type) != Some(value_width) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut sum = 0_i128;
	let mut count = 0_u64;
	let mut overflow = false;
	let mut row = 0_usize;
	let mut stack_counts = [0_u64; DICTIONARY_AGG_STACK_COUNTS];
	for page in encoded_page_slices(source, value_width)? {
		match &source.cascade {
			EncodingCascade::BitPacked { bits, base } => {
				let frame = parse_bitpacked_frame(
					page.bytes,
					*bits,
					*base,
					page.decoded_byte_len,
					page.value_count,
				)
				.map_err(map_encoding_reject)?;
				let page_rows = frame.value_count;
				for_selected_rows_in_page(selected, row, page_rows, |local_row| {
					let raw = bitpacked_value_at(&frame, local_row)?;
					let value = signed_value_from_u128(raw, frame.value_width, &column.data_type)?;
					count = count.saturating_add(1);
					add_i128(&mut sum, value, &mut overflow);
					Ok(())
				})?;
				row += page_rows;
			}
			EncodingCascade::Dictionary { inner, .. } => {
				let frame = parse_fixed_width_dictionary_frame(
					page.bytes,
					page.decoded_byte_len,
					page.value_count,
					inner,
				)
				.map_err(map_encoding_reject)?;
				if frame.dict_count <= DICTIONARY_AGG_STACK_COUNTS {
					let counts = &mut stack_counts[..frame.dict_count];
					counts.fill(0);
					count_masked_dictionary_indices(&frame, selected, row, counts)?;
					count = count.saturating_add(counts.iter().sum::<u64>());
					add_signed_dictionary_counts(
						&frame,
						&column.data_type,
						counts,
						&mut sum,
						&mut overflow,
					)?;
				} else {
					let mut counts = vec![0_u64; frame.dict_count];
					count_masked_dictionary_indices(&frame, selected, row, &mut counts)?;
					count = count.saturating_add(counts.iter().sum::<u64>());
					add_signed_dictionary_counts(
						&frame,
						&column.data_type,
						&counts,
						&mut sum,
						&mut overflow,
					)?;
				}
				row += frame.value_count;
			}
			_ => return Ok(None),
		}
	}
	if row != selected.rows() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(Some((sum, count, overflow)))
}

fn fold_native_unsigned_sum_mask(
	column: &EncodedAggregateColumn,
	selected: &AggregateMaskSelection<'_>,
) -> Result<Option<(u128, u64, bool)>, EncodingComputeRejectCode> {
	if column.validity_mask.is_some() || unsigned_integer_width(&column.data_type).is_none() {
		return Ok(None);
	}
	let Some(source) = &column.encoded_source else {
		return Ok(None);
	};
	if selected.rows() != source.encoding.decoded_value_count.get() as usize {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let value_width = decoded_fixed_width_bytes_per_value(&source.encoding)?;
	if fixed_width_for_data_type(&column.data_type) != Some(value_width) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut sum = 0_u128;
	let mut count = 0_u64;
	let mut overflow = false;
	let mut row = 0_usize;
	let mut stack_counts = [0_u64; DICTIONARY_AGG_STACK_COUNTS];
	for page in encoded_page_slices(source, value_width)? {
		match &source.cascade {
			EncodingCascade::BitPacked { bits, base } => {
				let frame = parse_bitpacked_frame(
					page.bytes,
					*bits,
					*base,
					page.decoded_byte_len,
					page.value_count,
				)
				.map_err(map_encoding_reject)?;
				let page_rows = frame.value_count;
				for_selected_rows_in_page(selected, row, page_rows, |local_row| {
					let value = bitpacked_value_at(&frame, local_row)?;
					count = count.saturating_add(1);
					add_u128(&mut sum, value, &mut overflow);
					Ok(())
				})?;
				row += page_rows;
			}
			EncodingCascade::Dictionary { inner, .. } => {
				let frame = parse_fixed_width_dictionary_frame(
					page.bytes,
					page.decoded_byte_len,
					page.value_count,
					inner,
				)
				.map_err(map_encoding_reject)?;
				if frame.dict_count <= DICTIONARY_AGG_STACK_COUNTS {
					let counts = &mut stack_counts[..frame.dict_count];
					counts.fill(0);
					count_masked_dictionary_indices(&frame, selected, row, counts)?;
					count = count.saturating_add(counts.iter().sum::<u64>());
					add_unsigned_dictionary_counts(&frame, counts, &mut sum, &mut overflow)?;
				} else {
					let mut counts = vec![0_u64; frame.dict_count];
					count_masked_dictionary_indices(&frame, selected, row, &mut counts)?;
					count = count.saturating_add(counts.iter().sum::<u64>());
					add_unsigned_dictionary_counts(&frame, &counts, &mut sum, &mut overflow)?;
				}
				row += frame.value_count;
			}
			_ => return Ok(None),
		}
	}
	if row != selected.rows() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(Some((sum, count, overflow)))
}

fn fold_signed_sum_no_validity(
	column: &EncodedAggregateColumn,
	source: &EncodedAggregateSource,
	selected: &SelectedRows,
) -> Result<Option<(i128, u64, bool)>, EncodingComputeRejectCode> {
	let value_width = decoded_fixed_width_bytes_per_value(&source.encoding)?;
	if fixed_width_for_data_type(&column.data_type) != Some(value_width) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	if selected.values.len() != source.encoding.decoded_value_count.get() as usize {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut sum = 0_i128;
	let mut overflow = false;
	let mut row = 0_usize;
	let mut stack_counts = [0_u64; DICTIONARY_AGG_STACK_COUNTS];
	for page in encoded_page_slices(source, value_width)? {
		match &source.cascade {
			EncodingCascade::BitPacked { bits, base } => {
				let frame = parse_bitpacked_frame(
					page.bytes,
					*bits,
					*base,
					page.decoded_byte_len,
					page.value_count,
				)
				.map_err(map_encoding_reject)?;
				if !bitpacked_byte_aligned(&frame) {
					return Ok(None);
				}
				let page_rows = frame.value_count;
				let selected_rows = selected
					.values
					.get(row..row + page_rows)
					.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
				for (bytes, keep) in frame
					.encoded
					.chunks_exact(frame.value_width)
					.zip(selected_rows)
				{
					if *keep {
						let raw = frame.base.saturating_add(read_unsigned_le_as_u128(bytes));
						let value =
							signed_value_from_u128(raw, frame.value_width, &column.data_type)?;
						add_i128(&mut sum, value, &mut overflow);
					}
				}
				row += page_rows;
			}
			EncodingCascade::Dictionary { inner, .. } => {
				let frame = parse_fixed_width_dictionary_frame(
					page.bytes,
					page.decoded_byte_len,
					page.value_count,
					inner,
				)
				.map_err(map_encoding_reject)?;
				let page_rows = frame.value_count;
				let selected_rows = selected
					.values
					.get(row..row + page_rows)
					.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
				if frame.dict_count <= DICTIONARY_AGG_STACK_COUNTS {
					let counts = &mut stack_counts[..frame.dict_count];
					counts.fill(0);
					count_selected_dictionary_indices(&frame, selected_rows, counts)?;
					add_signed_dictionary_counts(
						&frame,
						&column.data_type,
						counts,
						&mut sum,
						&mut overflow,
					)?;
				} else {
					let mut counts = vec![0_u64; frame.dict_count];
					count_selected_dictionary_indices(&frame, selected_rows, &mut counts)?;
					add_signed_dictionary_counts(
						&frame,
						&column.data_type,
						&counts,
						&mut sum,
						&mut overflow,
					)?;
				}
				row += page_rows;
			}
			_ => return Ok(None),
		}
	}
	if row != selected.values.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(Some((sum, selected.selected_count, overflow)))
}

fn fold_unsigned_sum_no_validity(
	column: &EncodedAggregateColumn,
	source: &EncodedAggregateSource,
	selected: &SelectedRows,
) -> Result<Option<(u128, u64, bool)>, EncodingComputeRejectCode> {
	let value_width = decoded_fixed_width_bytes_per_value(&source.encoding)?;
	if fixed_width_for_data_type(&column.data_type) != Some(value_width) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	if selected.values.len() != source.encoding.decoded_value_count.get() as usize {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut sum = 0_u128;
	let mut overflow = false;
	let mut row = 0_usize;
	let mut stack_counts = [0_u64; DICTIONARY_AGG_STACK_COUNTS];
	for page in encoded_page_slices(source, value_width)? {
		match &source.cascade {
			EncodingCascade::BitPacked { bits, base } => {
				let frame = parse_bitpacked_frame(
					page.bytes,
					*bits,
					*base,
					page.decoded_byte_len,
					page.value_count,
				)
				.map_err(map_encoding_reject)?;
				if !bitpacked_byte_aligned(&frame) {
					return Ok(None);
				}
				let page_rows = frame.value_count;
				let selected_rows = selected
					.values
					.get(row..row + page_rows)
					.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
				for (bytes, keep) in frame
					.encoded
					.chunks_exact(frame.value_width)
					.zip(selected_rows)
				{
					if *keep {
						let value = frame.base.saturating_add(read_unsigned_le_as_u128(bytes));
						add_u128(&mut sum, value, &mut overflow);
					}
				}
				row += page_rows;
			}
			EncodingCascade::Dictionary { inner, .. } => {
				let frame = parse_fixed_width_dictionary_frame(
					page.bytes,
					page.decoded_byte_len,
					page.value_count,
					inner,
				)
				.map_err(map_encoding_reject)?;
				let page_rows = frame.value_count;
				let selected_rows = selected
					.values
					.get(row..row + page_rows)
					.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
				if frame.dict_count <= DICTIONARY_AGG_STACK_COUNTS {
					let counts = &mut stack_counts[..frame.dict_count];
					counts.fill(0);
					count_selected_dictionary_indices(&frame, selected_rows, counts)?;
					add_unsigned_dictionary_counts(&frame, counts, &mut sum, &mut overflow)?;
				} else {
					let mut counts = vec![0_u64; frame.dict_count];
					count_selected_dictionary_indices(&frame, selected_rows, &mut counts)?;
					add_unsigned_dictionary_counts(&frame, &counts, &mut sum, &mut overflow)?;
				}
				row += page_rows;
			}
			_ => return Ok(None),
		}
	}
	if row != selected.values.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(Some((sum, selected.selected_count, overflow)))
}

fn bitpacked_byte_aligned(frame: &crate::storage::encoding::cascade::BitPackedFrame<'_>) -> bool {
	matches!(frame.bits, 8 | 16 | 32 | 64) && frame.value_width == usize::from(frame.bits / 8)
}

fn bitpacked_value_at(
	frame: &crate::storage::encoding::cascade::BitPackedFrame<'_>,
	row: usize,
) -> Result<u128, EncodingComputeRejectCode> {
	if row >= frame.value_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	if bitpacked_byte_aligned(frame) {
		let start = row
			.checked_mul(frame.value_width)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let end = start
			.checked_add(frame.value_width)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let Some(bytes) = frame.encoded.get(start..end) else {
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		};
		return Ok(frame.base.saturating_add(read_unsigned_le_as_u128(bytes)));
	}
	let bit_offset = row
		.checked_mul(usize::from(frame.bits))
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	let byte_offset = bit_offset / 8;
	let shift = bit_offset % 8;
	let needed = (shift + usize::from(frame.bits)).div_ceil(8);
	let end = byte_offset
		.checked_add(needed)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	let Some(bytes) = frame.encoded.get(byte_offset..end) else {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	};
	let mut acc = 0_u128;
	for (offset, byte) in bytes.iter().enumerate() {
		acc |= u128::from(*byte) << (offset * 8);
	}
	let mask = if frame.bits == 64 {
		u128::from(u64::MAX)
	} else {
		(1_u128 << frame.bits) - 1
	};
	Ok(frame.base.saturating_add((acc >> shift) & mask))
}

fn fold_signed_encoded_values(
	column: &EncodedAggregateColumn,
	source: &EncodedAggregateSource,
	selected: &[bool],
	validity: &[bool],
	sum: &mut i128,
	count: &mut u64,
	overflow: &mut bool,
) -> Result<(), EncodingComputeRejectCode> {
	let value_width = decoded_fixed_width_bytes_per_value(&source.encoding)?;
	if fixed_width_for_data_type(&column.data_type) != Some(value_width) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut row = 0_usize;
	for page in encoded_page_slices(source, value_width)? {
		match &source.cascade {
			EncodingCascade::BitPacked { bits, base } => {
				let frame = parse_bitpacked_frame(
					page.bytes,
					*bits,
					*base,
					page.decoded_byte_len,
					page.value_count,
				)
				.map_err(map_encoding_reject)?;
				visit_bitpacked_values(&frame, |raw| {
					let value = signed_value_from_u128(raw, frame.value_width, &column.data_type)?;
					if selected_row(row, selected, validity)? {
						*count = count.saturating_add(1);
						add_i128(sum, value, overflow);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Dictionary { inner, .. } => {
				let frame = parse_fixed_width_dictionary_frame(
					page.bytes,
					page.decoded_byte_len,
					page.value_count,
					inner,
				)
				.map_err(map_encoding_reject)?;
				let dictionary = signed_dictionary_values(&frame, &column.data_type)?;
				visit_dictionary_indices(&frame, |index| {
					let Some(value) = dictionary.get(index) else {
						return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
					};
					if selected_row(row, selected, validity)? {
						*count = count.saturating_add(1);
						add_i128(sum, *value, overflow);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Delta { .. } => {
				let frame = parse_delta_frame(page.bytes, page.decoded_byte_len)
					.map_err(map_encoding_reject)?;
				if frame.value_count != page.value_count.get() as usize {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				}
				visit_delta_values(&frame, |raw| {
					let value =
						signed_value_from_u128(raw as u128, frame.value_width, &column.data_type)?;
					if selected_row(row, selected, validity)? {
						*count = count.saturating_add(1);
						add_i128(sum, value, overflow);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Rle { .. } => {
				visit_rle_values(
					page.bytes,
					value_width,
					page.decoded_byte_len,
					page.value_count,
					|bytes| {
						let value = signed_value_from_le_bytes(bytes, &column.data_type)?;
						if selected_row(row, selected, validity)? {
							*count = count.saturating_add(1);
							add_i128(sum, value, overflow);
						}
						row += 1;
						Ok(())
					},
				)?;
			}
			EncodingCascade::Plain
			| EncodingCascade::Lz4Frame
			| EncodingCascade::Fsst { .. }
			| EncodingCascade::Alp { .. } => {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
		}
	}
	if row != selected.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn fold_unsigned_encoded_values(
	column: &EncodedAggregateColumn,
	source: &EncodedAggregateSource,
	selected: &[bool],
	validity: &[bool],
	sum: &mut u128,
	count: &mut u64,
	overflow: &mut bool,
) -> Result<(), EncodingComputeRejectCode> {
	let value_width = decoded_fixed_width_bytes_per_value(&source.encoding)?;
	if fixed_width_for_data_type(&column.data_type) != Some(value_width) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut row = 0_usize;
	for page in encoded_page_slices(source, value_width)? {
		match &source.cascade {
			EncodingCascade::BitPacked { bits, base } => {
				let frame = parse_bitpacked_frame(
					page.bytes,
					*bits,
					*base,
					page.decoded_byte_len,
					page.value_count,
				)
				.map_err(map_encoding_reject)?;
				visit_bitpacked_values(&frame, |value| {
					if selected_row(row, selected, validity)? {
						*count = count.saturating_add(1);
						add_u128(sum, value, overflow);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Dictionary { inner, .. } => {
				let frame = parse_fixed_width_dictionary_frame(
					page.bytes,
					page.decoded_byte_len,
					page.value_count,
					inner,
				)
				.map_err(map_encoding_reject)?;
				let dictionary = unsigned_dictionary_values(&frame)?;
				visit_dictionary_indices(&frame, |index| {
					let Some(value) = dictionary.get(index) else {
						return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
					};
					if selected_row(row, selected, validity)? {
						*count = count.saturating_add(1);
						add_u128(sum, *value, overflow);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Delta { .. } => {
				let frame = parse_delta_frame(page.bytes, page.decoded_byte_len)
					.map_err(map_encoding_reject)?;
				if frame.value_count != page.value_count.get() as usize {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				}
				visit_delta_values(&frame, |value| {
					if selected_row(row, selected, validity)? {
						*count = count.saturating_add(1);
						add_u128(sum, value as u128, overflow);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Rle { .. } => {
				visit_rle_values(
					page.bytes,
					value_width,
					page.decoded_byte_len,
					page.value_count,
					|bytes| {
						let value = read_unsigned_le_as_u128(bytes);
						if selected_row(row, selected, validity)? {
							*count = count.saturating_add(1);
							add_u128(sum, value, overflow);
						}
						row += 1;
						Ok(())
					},
				)?;
			}
			EncodingCascade::Plain
			| EncodingCascade::Lz4Frame
			| EncodingCascade::Fsst { .. }
			| EncodingCascade::Alp { .. } => {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
		}
	}
	if row != selected.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn selected_row(
	row: usize,
	selected: &[bool],
	validity: &[bool],
) -> Result<bool, EncodingComputeRejectCode> {
	if row >= selected.len() || row >= validity.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(selected[row] && validity[row])
}

fn visit_bitpacked_values(
	frame: &crate::storage::encoding::cascade::BitPackedFrame<'_>,
	mut visit: impl FnMut(u128) -> Result<(), EncodingComputeRejectCode>,
) -> Result<(), EncodingComputeRejectCode> {
	if matches!(frame.bits, 8 | 16 | 32 | 64) && frame.value_width == usize::from(frame.bits / 8) {
		for bytes in frame.encoded.chunks_exact(frame.value_width) {
			visit(frame.base.saturating_add(read_unsigned_le_as_u128(bytes)))?;
		}
		return Ok(());
	}
	let mask = if frame.bits == 64 {
		u128::from(u64::MAX)
	} else {
		(1_u128 << frame.bits) - 1
	};
	let mut acc = 0_u128;
	let mut acc_bits = 0_u8;
	let mut cursor = 0_usize;
	for _ in 0..frame.value_count {
		while acc_bits < frame.bits {
			let Some(byte) = frame.encoded.get(cursor) else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			acc |= u128::from(*byte) << acc_bits;
			acc_bits += 8;
			cursor += 1;
		}
		visit(frame.base.saturating_add(acc & mask))?;
		acc >>= frame.bits;
		acc_bits -= frame.bits;
	}
	Ok(())
}

fn signed_dictionary_values(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
	data_type: &DataType,
) -> Result<Vec<i128>, EncodingComputeRejectCode> {
	let mut values = Vec::with_capacity(frame.dict_count);
	for bytes in frame.dictionary.chunks_exact(frame.value_width) {
		values.push(signed_value_from_le_bytes(bytes, data_type)?);
	}
	if values.len() != frame.dict_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(values)
}

fn unsigned_dictionary_values(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
) -> Result<Vec<u128>, EncodingComputeRejectCode> {
	let mut values = Vec::with_capacity(frame.dict_count);
	for bytes in frame.dictionary.chunks_exact(frame.value_width) {
		values.push(read_unsigned_le_as_u128(bytes));
	}
	if values.len() != frame.dict_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(values)
}

fn visit_dictionary_indices(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
	mut visit: impl FnMut(usize) -> Result<(), EncodingComputeRejectCode>,
) -> Result<(), EncodingComputeRejectCode> {
	match frame.index_encoding {
		DictionaryIndexEncoding::Plain { width } => {
			let expected = frame
				.value_count
				.checked_mul(width)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			if frame.index_payload.len() != expected {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			for bytes in frame.index_payload.chunks_exact(width) {
				visit(read_dictionary_index_le(bytes, width)?)?;
			}
		}
		DictionaryIndexEncoding::BitPacked { bits } => {
			let mask = if bits == 64 {
				u128::from(u64::MAX)
			} else {
				(1_u128 << bits) - 1
			};
			let mut acc = 0_u128;
			let mut acc_bits = 0_u8;
			let mut cursor = 0_usize;
			for _ in 0..frame.value_count {
				while acc_bits < bits {
					let Some(byte) = frame.index_payload.get(cursor) else {
						return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
					};
					acc |= u128::from(*byte) << acc_bits;
					acc_bits += 8;
					cursor += 1;
				}
				visit((acc & mask) as usize)?;
				acc >>= bits;
				acc_bits -= bits;
			}
		}
	}
	Ok(())
}

fn count_selected_dictionary_indices(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
	selected_rows: &[bool],
	counts: &mut [u64],
) -> Result<(), EncodingComputeRejectCode> {
	if counts.len() != frame.dict_count || selected_rows.len() != frame.value_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	match frame.index_encoding {
		DictionaryIndexEncoding::Plain { width } => {
			let expected = frame
				.value_count
				.checked_mul(width)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			if frame.index_payload.len() != expected {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			for (bytes, keep) in frame.index_payload.chunks_exact(width).zip(selected_rows) {
				if *keep {
					increment_dictionary_count(counts, read_dictionary_index_le(bytes, width)?)?;
				}
			}
			return Ok(());
		}
		DictionaryIndexEncoding::BitPacked { bits: 8 } => {
			if frame.index_payload.len() != frame.value_count {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			for (index, keep) in frame.index_payload.iter().zip(selected_rows) {
				if *keep {
					increment_dictionary_count(counts, usize::from(*index))?;
				}
			}
			return Ok(());
		}
		DictionaryIndexEncoding::BitPacked { bits } if matches!(bits, 1 | 2 | 4) => {
			let expected = frame
				.value_count
				.checked_mul(bits as usize)
				.map(|bits| bits.div_ceil(8))
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			if frame.index_payload.len() != expected {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			let values_per_byte = usize::from(8 / bits);
			let mask = (1_u8 << bits) - 1;
			let mut local_row = 0_usize;
			for byte in frame.index_payload {
				for lane in 0..values_per_byte {
					if local_row == frame.value_count {
						break;
					}
					if selected_rows[local_row] {
						let shift = lane * usize::from(bits);
						increment_dictionary_count(counts, usize::from((byte >> shift) & mask))?;
					}
					local_row += 1;
				}
			}
			if local_row != frame.value_count {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			return Ok(());
		}
		DictionaryIndexEncoding::BitPacked { .. } => {}
	}
	let mut local_row = 0_usize;
	visit_dictionary_indices(frame, |index| {
		if selected_rows[local_row] {
			increment_dictionary_count(counts, index)?;
		}
		local_row += 1;
		Ok(())
	})?;
	if local_row != frame.value_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn count_masked_dictionary_indices(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
	selected: &AggregateMaskSelection<'_>,
	row_offset: usize,
	counts: &mut [u64],
) -> Result<(), EncodingComputeRejectCode> {
	if counts.len() != frame.dict_count
		|| row_offset
			.checked_add(frame.value_count)
			.is_none_or(|end| end > selected.rows())
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	match selected {
		AggregateMaskSelection::All { .. } => {
			count_all_dictionary_indices(frame, counts)?;
			return Ok(());
		}
		AggregateMaskSelection::None { .. } => return Ok(()),
		AggregateMaskSelection::Bitmap { .. } => {}
	}
	for_selected_rows_in_page(selected, row_offset, frame.value_count, |local_row| {
		let index = dictionary_index_at(frame, local_row)?;
		increment_dictionary_count(counts, index)
	})?;
	Ok(())
}

fn count_all_dictionary_indices(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
	counts: &mut [u64],
) -> Result<(), EncodingComputeRejectCode> {
	if counts.len() != frame.dict_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	match frame.index_encoding {
		DictionaryIndexEncoding::Plain { width } => {
			let expected = frame
				.value_count
				.checked_mul(width)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			if frame.index_payload.len() != expected {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			for bytes in frame.index_payload.chunks_exact(width) {
				increment_dictionary_count(counts, read_dictionary_index_le(bytes, width)?)?;
			}
			return Ok(());
		}
		DictionaryIndexEncoding::BitPacked { bits: 8 } => {
			if frame.index_payload.len() != frame.value_count {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			for index in frame.index_payload {
				increment_dictionary_count(counts, usize::from(*index))?;
			}
			return Ok(());
		}
		DictionaryIndexEncoding::BitPacked { bits } if matches!(bits, 1 | 2 | 4) => {
			let expected = frame
				.value_count
				.checked_mul(bits as usize)
				.map(|bits| bits.div_ceil(8))
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			if frame.index_payload.len() != expected {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			let values_per_byte = usize::from(8 / bits);
			let mask = (1_u8 << bits) - 1;
			let mut local_row = 0_usize;
			for byte in frame.index_payload {
				for lane in 0..values_per_byte {
					if local_row == frame.value_count {
						break;
					}
					let shift = lane * usize::from(bits);
					increment_dictionary_count(counts, usize::from((byte >> shift) & mask))?;
					local_row += 1;
				}
			}
			if local_row != frame.value_count {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			return Ok(());
		}
		DictionaryIndexEncoding::BitPacked { .. } => {}
	}
	let mut local_row = 0_usize;
	visit_dictionary_indices(frame, |index| {
		increment_dictionary_count(counts, index)?;
		local_row += 1;
		Ok(())
	})?;
	if local_row != frame.value_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn dictionary_index_at(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
	row: usize,
) -> Result<usize, EncodingComputeRejectCode> {
	if row >= frame.value_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	match frame.index_encoding {
		DictionaryIndexEncoding::Plain { width } => {
			let start = row
				.checked_mul(width)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			let end = start
				.checked_add(width)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			let Some(bytes) = frame.index_payload.get(start..end) else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			read_dictionary_index_le(bytes, width)
		}
		DictionaryIndexEncoding::BitPacked { bits } => {
			let bit_offset = row
				.checked_mul(usize::from(bits))
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			let byte_offset = bit_offset / 8;
			let shift = bit_offset % 8;
			let needed = (shift + usize::from(bits)).div_ceil(8);
			let Some(bytes) = frame.index_payload.get(byte_offset..byte_offset + needed) else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			let mut acc = 0_u128;
			for (offset, byte) in bytes.iter().enumerate() {
				acc |= u128::from(*byte) << (offset * 8);
			}
			let mask = if bits == 64 {
				u128::from(u64::MAX)
			} else {
				(1_u128 << bits) - 1
			};
			Ok(((acc >> shift) & mask) as usize)
		}
	}
}

fn for_selected_rows_in_page(
	selected: &AggregateMaskSelection<'_>,
	row_offset: usize,
	page_rows: usize,
	mut visit: impl FnMut(usize) -> Result<(), EncodingComputeRejectCode>,
) -> Result<(), EncodingComputeRejectCode> {
	let end = row_offset
		.checked_add(page_rows)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if end > selected.rows() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	match selected {
		AggregateMaskSelection::All { .. } => {
			for local_row in 0..page_rows {
				visit(local_row)?;
			}
		}
		AggregateMaskSelection::None { .. } => {}
		AggregateMaskSelection::Bitmap { bytes, .. } => {
			let mut row = row_offset;
			while row < end {
				let byte_index = row / 8;
				let byte_start = byte_index * 8;
				let byte_end = (byte_start + 8).min(end);
				let Some(mut byte) = bytes.get(byte_index).copied() else {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				};
				let first_bit = row - byte_start;
				byte &= u8::MAX << first_bit;
				let last_bits = byte_end - byte_start;
				if last_bits < 8 {
					byte &= (1_u8 << last_bits) - 1;
				}
				while byte != 0 {
					let bit = byte.trailing_zeros() as usize;
					visit(byte_start + bit - row_offset)?;
					byte &= byte - 1;
				}
				row = byte_end;
			}
		}
	}
	Ok(())
}

fn increment_dictionary_count(
	counts: &mut [u64],
	index: usize,
) -> Result<(), EncodingComputeRejectCode> {
	let Some(count) = counts.get_mut(index) else {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	};
	*count = count.saturating_add(1);
	Ok(())
}

fn add_signed_dictionary_counts(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
	data_type: &DataType,
	counts: &[u64],
	sum: &mut i128,
	overflow: &mut bool,
) -> Result<(), EncodingComputeRejectCode> {
	if counts.len() != frame.dict_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut values = 0_usize;
	for (bytes, repetitions) in frame
		.dictionary
		.chunks_exact(frame.value_width)
		.zip(counts.iter().copied())
	{
		let value = signed_value_from_le_bytes(bytes, data_type)?;
		add_i128_repeated(sum, value, repetitions, overflow);
		values += 1;
	}
	if values != frame.dict_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn add_unsigned_dictionary_counts(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
	counts: &[u64],
	sum: &mut u128,
	overflow: &mut bool,
) -> Result<(), EncodingComputeRejectCode> {
	if counts.len() != frame.dict_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut values = 0_usize;
	for (bytes, repetitions) in frame
		.dictionary
		.chunks_exact(frame.value_width)
		.zip(counts.iter().copied())
	{
		add_u128_repeated(sum, read_unsigned_le_as_u128(bytes), repetitions, overflow);
		values += 1;
	}
	if values != frame.dict_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn read_dictionary_index_le(
	bytes: &[u8],
	width: usize,
) -> Result<usize, EncodingComputeRejectCode> {
	Ok(match width {
		1 => usize::from(bytes[0]),
		2 => usize::from(u16::from_le_bytes([bytes[0], bytes[1]])),
		4 => u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize,
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}

fn visit_delta_values(
	frame: &crate::storage::encoding::cascade::DeltaFrame<'_>,
	mut visit: impl FnMut(i128) -> Result<(), EncodingComputeRejectCode>,
) -> Result<(), EncodingComputeRejectCode> {
	let mut emitted = 0_usize;
	let mut value = frame.first_value;
	visit(value)?;
	emitted += 1;
	for delta_bytes in frame.deltas.chunks_exact(frame.delta_width) {
		let delta = read_delta_i128(delta_bytes, frame.delta_width)?;
		value = value
			.checked_add(delta)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		visit(value)?;
		emitted += 1;
	}
	if emitted != frame.value_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn read_delta_i128(bytes: &[u8], width: usize) -> Result<i128, EncodingComputeRejectCode> {
	Ok(match width {
		1 => i128::from(i8::from_le_bytes([bytes[0]])),
		2 => i128::from(i16::from_le_bytes([bytes[0], bytes[1]])),
		4 => i128::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
		8 => i128::from(i64::from_le_bytes([
			bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
		])),
		16 => i128::from_le_bytes(
			bytes
				.try_into()
				.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?,
		),
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}

fn visit_rle_values(
	encoded_bytes: &[u8],
	value_width: usize,
	decoded_byte_len: RecordBodyByteLen,
	value_count: BufferValueCount,
	mut visit: impl FnMut(&[u8]) -> Result<(), EncodingComputeRejectCode>,
) -> Result<(), EncodingComputeRejectCode> {
	let expected = usize::try_from(decoded_byte_len.get())
		.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	let mut decoded_bytes = 0_usize;
	if value_width == 1 {
		for run in RleByteRunIter::new(encoded_bytes) {
			let run = run.map_err(map_encoding_reject)?;
			decoded_bytes = decoded_bytes
				.checked_add(run.run_len)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			if decoded_bytes > expected {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			for _ in 0..run.run_len {
				visit(std::slice::from_ref(&run.byte))?;
			}
		}
	} else {
		let mut lane = [0_u8; 16];
		let mut lane_len = 0_usize;
		for run in RleByteRunIter::new(encoded_bytes) {
			let run = run.map_err(map_encoding_reject)?;
			for _ in 0..run.run_len {
				decoded_bytes = decoded_bytes
					.checked_add(1)
					.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
				if decoded_bytes > expected || lane_len >= lane.len() {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				}
				lane[lane_len] = run.byte;
				lane_len += 1;
				if lane_len == value_width {
					visit(&lane[..value_width])?;
					lane_len = 0;
				}
			}
		}
		if lane_len != 0 {
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		}
	}
	if decoded_bytes != expected || decoded_bytes / value_width != value_count.get() as usize {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn fold_native_min_max(
	column: &EncodedAggregateColumn,
	selected: &[bool],
	min: bool,
) -> Result<Option<AggregateState>, EncodingComputeRejectCode> {
	let Some(arrow_type_tag) = aggregate_min_max_type_tag(&column.data_type) else {
		return Ok(None);
	};
	let Some(source) = &column.encoded_source else {
		return Ok(None);
	};
	if !matches!(
		source.cascade,
		EncodingCascade::BitPacked { .. }
			| EncodingCascade::Dictionary { .. }
			| EncodingCascade::Delta { .. }
			| EncodingCascade::Rle { .. }
	) {
		return Ok(None);
	}
	let validity = validity_values(column)?;
	if selected.len() != source.encoding.decoded_value_count.get() as usize
		|| validity.len() != selected.len()
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}

	let value = if let Some(width) = signed_canonical_width(&column.data_type) {
		fold_signed_min_max_encoded_values(column, source, selected, &validity, min)?
			.map(|best| encode_signed_value(best, width))
	} else if let Some(width) = unsigned_integer_width(&column.data_type) {
		fold_unsigned_min_max_encoded_values(column, source, selected, &validity, min)?
			.map(|best| encode_unsigned_value(best, width))
	} else {
		return Ok(None);
	};
	let truncated = value
		.as_ref()
		.is_some_and(|value| value.len() > AGG_MIN_MAX_PREFIX_BYTES);
	let value = value.map(|mut value| {
		if truncated {
			value.truncate(AGG_MIN_MAX_PREFIX_BYTES);
		}
		value
	});
	Ok(Some(AggregateState::MinMax {
		value,
		arrow_type_tag,
		truncated,
	}))
}

fn fold_signed_min_max_encoded_values(
	column: &EncodedAggregateColumn,
	source: &EncodedAggregateSource,
	selected: &[bool],
	validity: &[bool],
	min: bool,
) -> Result<Option<i128>, EncodingComputeRejectCode> {
	let value_width = decoded_fixed_width_bytes_per_value(&source.encoding)?;
	if fixed_width_for_data_type(&column.data_type) != Some(value_width) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut row = 0_usize;
	let mut best = None::<i128>;
	for page in encoded_page_slices(source, value_width)? {
		match &source.cascade {
			EncodingCascade::BitPacked { bits, base } => {
				let frame = parse_bitpacked_frame(
					page.bytes,
					*bits,
					*base,
					page.decoded_byte_len,
					page.value_count,
				)
				.map_err(map_encoding_reject)?;
				visit_bitpacked_values(&frame, |raw| {
					let value = signed_value_from_u128(raw, frame.value_width, &column.data_type)?;
					if selected_row(row, selected, validity)? {
						update_best(&mut best, value, min);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Dictionary { inner, .. } => {
				let frame = parse_fixed_width_dictionary_frame(
					page.bytes,
					page.decoded_byte_len,
					page.value_count,
					inner,
				)
				.map_err(map_encoding_reject)?;
				let dictionary = signed_dictionary_values(&frame, &column.data_type)?;
				visit_dictionary_indices(&frame, |index| {
					let Some(value) = dictionary.get(index) else {
						return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
					};
					if selected_row(row, selected, validity)? {
						update_best(&mut best, *value, min);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Delta { .. } => {
				let frame = parse_delta_frame(page.bytes, page.decoded_byte_len)
					.map_err(map_encoding_reject)?;
				if frame.value_count != page.value_count.get() as usize {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				}
				visit_delta_values(&frame, |raw| {
					let value =
						signed_value_from_u128(raw as u128, frame.value_width, &column.data_type)?;
					if selected_row(row, selected, validity)? {
						update_best(&mut best, value, min);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Rle { .. } => {
				visit_rle_values(
					page.bytes,
					value_width,
					page.decoded_byte_len,
					page.value_count,
					|bytes| {
						let value = signed_value_from_le_bytes(bytes, &column.data_type)?;
						if selected_row(row, selected, validity)? {
							update_best(&mut best, value, min);
						}
						row += 1;
						Ok(())
					},
				)?;
			}
			EncodingCascade::Plain
			| EncodingCascade::Lz4Frame
			| EncodingCascade::Fsst { .. }
			| EncodingCascade::Alp { .. } => {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
		}
	}
	if row != selected.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(best)
}

fn fold_unsigned_min_max_encoded_values(
	column: &EncodedAggregateColumn,
	source: &EncodedAggregateSource,
	selected: &[bool],
	validity: &[bool],
	min: bool,
) -> Result<Option<u128>, EncodingComputeRejectCode> {
	let value_width = decoded_fixed_width_bytes_per_value(&source.encoding)?;
	if fixed_width_for_data_type(&column.data_type) != Some(value_width) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut row = 0_usize;
	let mut best = None::<u128>;
	for page in encoded_page_slices(source, value_width)? {
		match &source.cascade {
			EncodingCascade::BitPacked { bits, base } => {
				let frame = parse_bitpacked_frame(
					page.bytes,
					*bits,
					*base,
					page.decoded_byte_len,
					page.value_count,
				)
				.map_err(map_encoding_reject)?;
				visit_bitpacked_values(&frame, |value| {
					if selected_row(row, selected, validity)? {
						update_best(&mut best, value, min);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Dictionary { inner, .. } => {
				let frame = parse_fixed_width_dictionary_frame(
					page.bytes,
					page.decoded_byte_len,
					page.value_count,
					inner,
				)
				.map_err(map_encoding_reject)?;
				let dictionary = unsigned_dictionary_values(&frame)?;
				visit_dictionary_indices(&frame, |index| {
					let Some(value) = dictionary.get(index) else {
						return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
					};
					if selected_row(row, selected, validity)? {
						update_best(&mut best, *value, min);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Delta { .. } => {
				let frame = parse_delta_frame(page.bytes, page.decoded_byte_len)
					.map_err(map_encoding_reject)?;
				if frame.value_count != page.value_count.get() as usize {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				}
				visit_delta_values(&frame, |value| {
					if selected_row(row, selected, validity)? {
						update_best(&mut best, value as u128, min);
					}
					row += 1;
					Ok(())
				})?;
			}
			EncodingCascade::Rle { .. } => {
				visit_rle_values(
					page.bytes,
					value_width,
					page.decoded_byte_len,
					page.value_count,
					|bytes| {
						let value = read_unsigned_le_as_u128(bytes);
						if selected_row(row, selected, validity)? {
							update_best(&mut best, value, min);
						}
						row += 1;
						Ok(())
					},
				)?;
			}
			EncodingCascade::Plain
			| EncodingCascade::Lz4Frame
			| EncodingCascade::Fsst { .. }
			| EncodingCascade::Alp { .. } => {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
		}
	}
	if row != selected.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(best)
}

fn update_best<T: Ord + Copy>(best: &mut Option<T>, candidate: T, min: bool) {
	let replace = best
		.map(|current| {
			if min {
				candidate < current
			} else {
				candidate > current
			}
		})
		.unwrap_or(true);
	if replace {
		*best = Some(candidate);
	}
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

struct EncodedPageSlice<'a> {
	bytes: &'a [u8],
	decoded_byte_len: RecordBodyByteLen,
	value_count: BufferValueCount,
}

fn encoded_page_slices<'a>(
	source: &'a EncodedAggregateSource,
	value_width: usize,
) -> Result<Vec<EncodedPageSlice<'a>>, EncodingComputeRejectCode> {
	if source.encoding.page_end_offsets.is_empty() {
		return Ok(vec![EncodedPageSlice {
			bytes: &source.encoded_bytes,
			decoded_byte_len: source.encoding.decoded_byte_len,
			value_count: source.encoding.decoded_value_count,
		}]);
	}
	let mut pages = Vec::new();
	for page in expand_page_descriptors(&source.encoding).map_err(map_encoding_reject)? {
		let start = usize::try_from(page.byte_range.start.get())
			.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let len = usize::try_from(page.byte_range.len.get())
			.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let end = start
			.checked_add(len)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let Some(bytes) = source.encoded_bytes.get(start..end) else {
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		};
		let decoded_byte_len = usize::try_from(page.value_count.get())
			.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?
			.checked_mul(value_width)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		pages.push(EncodedPageSlice {
			bytes,
			decoded_byte_len: RecordBodyByteLen::new(decoded_byte_len as u64),
			value_count: page.value_count,
		});
	}
	Ok(pages)
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

fn add_i128_repeated(sum: &mut i128, value: i128, repetitions: u64, overflow: &mut bool) {
	if repetitions == 0 {
		return;
	}
	let repetitions = i128::from(repetitions);
	if let Some(product) = value.checked_mul(repetitions)
		&& let Some(next) = sum.checked_add(product)
	{
		*sum = next;
		return;
	}
	*sum = sum.saturating_add(value.saturating_mul(repetitions));
	*overflow = true;
	crate::metrics::global().observe_agg_int_overflow();
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

fn add_u128_repeated(sum: &mut u128, value: u128, repetitions: u64, overflow: &mut bool) {
	if repetitions == 0 {
		return;
	}
	let repetitions = u128::from(repetitions);
	if let Some(product) = value.checked_mul(repetitions)
		&& let Some(next) = sum.checked_add(product)
	{
		*sum = next;
		return;
	}
	*sum = sum.saturating_add(value.saturating_mul(repetitions));
	*overflow = true;
	crate::metrics::global().observe_agg_int_overflow();
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
