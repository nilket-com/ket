use crate::schema::{
	BaseValue, BitsPerValue, BufferValueCount, RecordBodyByteLen, RunCount, SimdPageValueCount,
};
use crate::storage::encoding::cascade::{
	EncodedBuffer, encode_buffer, encode_buffer_with_value_offsets, fixed_width_bytes_per_value,
};
use crate::storage::encoding::sample::{ColumnSample, sample_buffer};
use crate::storage::encoding::{
	EncodingCascade, EncodingCascadeKind, EncodingLeafCascade, EncodingWriterConfig,
};
use arrow::datatypes::DataType;
use std::collections::HashSet;
use std::io::{Error, ErrorKind};

pub fn select_and_encode_buffer(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
) -> std::io::Result<EncodedBuffer> {
	select_and_encode_buffer_with_value_offsets(input, data_type, decoded_value_count, config, None)
}

pub fn select_and_encode_buffer_with_value_offsets(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
) -> std::io::Result<EncodedBuffer> {
	config.validate()?;
	let page_rows = config.target_simd_page_values;
	if !config.enabled || input.len() as u64 <= config.min_buffer_bytes_for_encoding.get() {
		return encode_buffer(
			input,
			data_type,
			EncodingCascade::Plain,
			decoded_value_count,
			page_rows,
		);
	}

	let sample = sample_buffer(
		input,
		data_type,
		decoded_value_count,
		config.cascade_sample_rows,
	);
	let cascade = select_candidate_cascade(
		input,
		data_type,
		decoded_value_count,
		&sample,
		config,
		value_offsets,
	)?;
	encode_buffer_with_value_offsets(
		input,
		data_type,
		cascade,
		decoded_value_count,
		page_rows,
		value_offsets,
	)
}

fn select_candidate_cascade(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	sample: &ColumnSample,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
) -> std::io::Result<EncodingCascade> {
	if input.is_empty() {
		return Ok(EncodingCascade::Plain);
	}
	if config.cascade_enabled(EncodingCascadeKind::Rle) {
		let runs = sample.run_count_estimate as usize;
		if sample.sampled_value_count > 0 && runs.saturating_mul(10) < sample.sampled_value_count {
			return Ok(EncodingCascade::Rle {
				run_count: RunCount::new(u32::try_from(runs).map_err(|_| {
					Error::new(ErrorKind::InvalidData, "Encoding RLE run count exceeds u32")
				})?),
				inner: Box::new(EncodingLeafCascade::Plain),
			});
		}
	}

	if config.cascade_enabled(EncodingCascadeKind::Delta)
		&& is_delta_candidate_type(data_type)
		&& let Some(width) = fixed_width_bytes_per_value(input, decoded_value_count)
			.filter(|width| matches!(width, 1 | 2 | 4 | 8))
	{
		let value_count = input.len() / width;
		if value_count > 1 && sample.is_monotonic {
			let (base, delta_width) = monotonic_delta_width(input, width).unwrap_or_else(|| {
				(
					sample.min.map(|value| value.get()).unwrap_or_default(),
					sample.delta_width_bytes.unwrap_or(16),
				)
			});
			let encoded_len = 26_usize.saturating_add(value_count.saturating_sub(1) * delta_width);
			if encoded_len.saturating_mul(2) <= input.len() {
				return Ok(EncodingCascade::Delta {
					base: BaseValue::new(base),
					inner: Box::new(EncodingLeafCascade::Plain),
				});
			}
		}
	}

	if config.cascade_enabled(EncodingCascadeKind::BitPacked)
		&& is_bitpacked_candidate_type(data_type)
		&& matches!(sample.bytes_per_value, Some(1 | 2 | 4 | 8))
		&& let Some(width) = sample.bytes_per_value
		&& let Some((min, max)) = fixed_width_min_max(input, width)
	{
		let span = max.saturating_sub(min);
		let bits = bits_required_u128(span);
		let value_count = decoded_value_count.get() as usize;
		let packed_len = value_count.saturating_mul(bits as usize).div_ceil(8);
		if bits < (width as u8).saturating_mul(8) && packed_len.saturating_mul(2) <= input.len() {
			return Ok(EncodingCascade::BitPacked {
				bits: BitsPerValue::new(bits),
				base: BaseValue::new(i128::try_from(min).unwrap_or(i128::MAX)),
			});
		}
	}

	if (config.cascade_enabled(EncodingCascadeKind::Fsst)
		|| config.cascade_enabled(EncodingCascadeKind::FsstRowSafe))
		&& is_variable_width_candidate_type(data_type)
		&& sample.fsst_compressible
		&& let Some(candidate) =
			select_fsst_candidate(input, data_type, decoded_value_count, config, value_offsets)?
	{
		return Ok(candidate);
	}

	if config.cascade_enabled(EncodingCascadeKind::Dictionary)
		&& is_variable_width_candidate_type(data_type)
		&& decoded_value_count.get() <= config.target_simd_page_values.get()
		&& let Some(offsets) = value_offsets
		&& let Some(candidate) = variable_width_dictionary_candidate(input, offsets)?
	{
		return Ok(candidate);
	}

	if config.cascade_enabled(EncodingCascadeKind::Dictionary)
		&& matches!(sample.bytes_per_value, Some(1 | 2 | 4 | 8))
		&& sample.distinct_count_estimate > 0
		&& sample.distinct_count_estimate <= 4
	{
		let value_count = decoded_value_count.get() as usize;
		let width = sample.bytes_per_value.unwrap_or(1);
		let distinct_count = fixed_width_distinct_count(input, width).min(u32::MAX as usize) as u32;
		if distinct_count == 0 || distinct_count > 65_536 {
			return Ok(EncodingCascade::Plain);
		}
		let index_width = if distinct_count <= 256 {
			1
		} else if distinct_count <= 65_536 {
			2
		} else {
			4
		};
		let index_bits = bits_required_u32(distinct_count.saturating_sub(1));
		let bitpacked_index_len = value_count.saturating_mul(index_bits as usize).div_ceil(8);
		let estimated_len = 14_usize
			.saturating_add(distinct_count as usize * width)
			.saturating_add(bitpacked_index_len.min(value_count.saturating_mul(index_width)));
		if estimated_len.saturating_mul(2) <= input.len() {
			return Ok(EncodingCascade::Dictionary {
				dict_byte_len: RecordBodyByteLen::new(
					u64::from(distinct_count).saturating_mul(width as u64),
				),
				dict_value_count: crate::schema::DictValueCount::new(distinct_count),
				inner: Box::new(EncodingLeafCascade::BitPacked {
					bits: BitsPerValue::new(index_bits),
					base: BaseValue::ZERO,
				}),
			});
		}
	}

	if config.cascade_enabled(EncodingCascadeKind::Alp) && sample.alp_candidate {
		return Ok(EncodingCascade::Alp {
			exponent: sample.alp_exponent.unwrap_or(crate::schema::Exponent::ZERO),
			factor: sample.alp_factor.unwrap_or(crate::schema::Factor::new(1)),
			exception_count: crate::schema::ExceptionCount::ZERO,
			inner: Box::new(EncodingLeafCascade::Plain),
		});
	}

	Ok(plain_or_lz4(config))
}

fn select_fsst_candidate(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
) -> std::io::Result<Option<EncodingCascade>> {
	let fsst_enabled = config.cascade_enabled(EncodingCascadeKind::Fsst);
	let row_safe_enabled = config.cascade_enabled(EncodingCascadeKind::FsstRowSafe);
	let fsst = EncodingCascade::Fsst {
		symbol_table_byte_len: RecordBodyByteLen::new(0),
	};
	if row_safe_enabled && let Some(offsets) = value_offsets {
		let row_safe = EncodingCascade::FsstRowSafe {
			symbol_table_byte_len: RecordBodyByteLen::new(0),
		};
		let row_safe_encoded = encode_buffer_with_value_offsets(
			input,
			data_type,
			row_safe.clone(),
			decoded_value_count,
			SimdPageValueCount::new(decoded_value_count.get().max(1)),
			Some(offsets),
		)?;
		let row_safe_len = row_safe_encoded.bytes.len();
		let plaintext_guard = row_safe_len <= input.len();
		let wfs1_guard = if fsst_enabled {
			let fsst_encoded = encode_buffer_with_value_offsets(
				input,
				data_type,
				fsst.clone(),
				decoded_value_count,
				SimdPageValueCount::new(decoded_value_count.get().max(1)),
				value_offsets,
			)?;
			row_safe_len <= fsst_encoded.bytes.len().saturating_mul(11).div_ceil(10)
		} else {
			true
		};
		if plaintext_guard && wfs1_guard {
			return Ok(Some(row_safe));
		}
	}
	if fsst_enabled {
		return Ok(Some(fsst));
	}
	Ok(None)
}

fn plain_or_lz4(config: &EncodingWriterConfig) -> EncodingCascade {
	if config.cascade_enabled(EncodingCascadeKind::Plain)
		|| !config.cascade_enabled(EncodingCascadeKind::Lz4Frame)
	{
		EncodingCascade::Plain
	} else {
		EncodingCascade::Lz4Frame
	}
}

fn bits_required_u32(span: u32) -> u8 {
	if span == 0 {
		1
	} else {
		(32 - span.leading_zeros()) as u8
	}
}

fn bits_required_u128(span: u128) -> u8 {
	if span == 0 {
		1
	} else {
		(128 - span.leading_zeros()) as u8
	}
}

fn fixed_width_min_max(input: &[u8], width: usize) -> Option<(u128, u128)> {
	let mut chunks = input.chunks_exact(width);
	if !chunks.remainder().is_empty() {
		return None;
	}
	let first = read_unsigned_le_as_u128(chunks.next()?);
	let mut min = first;
	let mut max = first;
	for chunk in chunks {
		let value = read_unsigned_le_as_u128(chunk);
		min = min.min(value);
		max = max.max(value);
	}
	Some((min, max))
}

fn fixed_width_distinct_count(input: &[u8], width: usize) -> usize {
	let mut distinct = Vec::<i128>::new();
	let mut large_distinct = None::<HashSet<i128>>;
	for value in input.chunks_exact(width) {
		let value = read_unsigned_le_as_i128(value);
		if let Some(distinct) = large_distinct.as_mut() {
			distinct.insert(value);
			if distinct.len() > 65_536 {
				break;
			}
		} else if !distinct.contains(&value) {
			distinct.push(value);
			if distinct.len() > 64 {
				let mut promoted = HashSet::with_capacity(128);
				promoted.extend(distinct.drain(..));
				large_distinct = Some(promoted);
			}
		}
	}
	large_distinct
		.as_ref()
		.map(HashSet::len)
		.unwrap_or(distinct.len())
}

fn is_delta_candidate_type(data_type: &DataType) -> bool {
	is_bitpacked_candidate_type(data_type)
}

fn is_bitpacked_candidate_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64
			| DataType::Date32
			| DataType::Date64
			| DataType::Time32(_)
			| DataType::Time64(_)
			| DataType::Timestamp(_, _)
			| DataType::Duration(_)
	)
}

fn is_variable_width_candidate_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
	)
}

fn variable_width_dictionary_candidate(
	input: &[u8],
	offsets: &[u64],
) -> std::io::Result<Option<EncodingCascade>> {
	if offsets.len() < 2 || offsets.last().copied() != Some(input.len() as u64) {
		return Ok(None);
	}
	let value_count = offsets.len() - 1;
	let mut distinct = HashSet::<&[u8]>::new();
	let mut previous = 0_u64;
	for pair in offsets.windows(2) {
		let start = pair[0];
		let end = pair[1];
		if start < previous || end < start || end > input.len() as u64 {
			return Ok(None);
		}
		previous = end;
		distinct.insert(&input[start as usize..end as usize]);
		if distinct.len() > 65_536 {
			return Ok(None);
		}
	}
	if distinct.is_empty() || distinct.len().saturating_mul(4) > value_count {
		return Ok(None);
	}
	let index_width = if distinct.len() <= 256 {
		1
	} else if distinct.len() <= 65_536 {
		2
	} else {
		4
	};
	let index_bits = bits_required_u32(u32::try_from(distinct.len().saturating_sub(1)).map_err(
		|_| {
			Error::new(
				ErrorKind::InvalidData,
				"Encoding Dictionary distinct count overflow",
			)
		},
	)?);
	let dict_payload_len = distinct.iter().try_fold(0_usize, |sum, value| {
		sum.checked_add(4)
			.and_then(|next| next.checked_add(value.len()))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "Encoding Dictionary size overflow"))
	})?;
	let estimated_len = 17_usize.saturating_add(dict_payload_len).saturating_add(
		value_count
			.saturating_mul(index_bits as usize)
			.div_ceil(8)
			.min(value_count.saturating_mul(index_width)),
	);
	if estimated_len.saturating_mul(2) > input.len() {
		return Ok(None);
	}
	Ok(Some(EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(dict_payload_len as u64),
		dict_value_count: crate::schema::DictValueCount::new(
			u32::try_from(distinct.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"Encoding Dictionary distinct count overflow",
				)
			})?,
		),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(index_bits),
			base: BaseValue::ZERO,
		}),
	}))
}

fn monotonic_delta_width(input: &[u8], width: usize) -> Option<(i128, usize)> {
	let mut chunks = input.chunks_exact(width);
	let first = read_unsigned_le_as_i128(chunks.next()?);
	let mut previous = first;
	let mut direction = 0_i8;
	let mut delta_width = 1_usize;
	for chunk in chunks {
		let value = read_unsigned_le_as_i128(chunk);
		let delta = value.checked_sub(previous)?;
		let step_direction = if delta == 0 {
			0
		} else {
			if delta > 0 { 1 } else { -1 }
		};
		if step_direction != 0 {
			if direction == 0 {
				direction = step_direction;
			} else if direction != step_direction {
				return None;
			}
		}
		delta_width = delta_width.max(delta_width_bytes(delta));
		previous = value;
	}
	Some((first, delta_width))
}

fn read_unsigned_le_as_i128(bytes: &[u8]) -> i128 {
	let mut value = 0_i128;
	for (shift, byte) in bytes.iter().enumerate() {
		value |= i128::from(*byte) << (shift * 8);
	}
	value
}

fn read_unsigned_le_as_u128(bytes: &[u8]) -> u128 {
	match bytes.len() {
		1 => u128::from(bytes[0]),
		2 => u128::from(u16::from_le_bytes(bytes.try_into().expect("2-byte value"))),
		4 => u128::from(u32::from_le_bytes(bytes.try_into().expect("4-byte value"))),
		8 => u128::from(u64::from_le_bytes(bytes.try_into().expect("8-byte value"))),
		_ => {
			let mut value = 0_u128;
			for (shift, byte) in bytes.iter().enumerate() {
				value |= u128::from(*byte) << (shift * 8);
			}
			value
		}
	}
}

fn delta_width_bytes(delta: i128) -> usize {
	if i8::try_from(delta).is_ok() {
		1
	} else if i16::try_from(delta).is_ok() {
		2
	} else if i32::try_from(delta).is_ok() {
		4
	} else if i64::try_from(delta).is_ok() {
		8
	} else {
		16
	}
}
