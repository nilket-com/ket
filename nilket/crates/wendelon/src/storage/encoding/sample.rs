use crate::schema::{BaseValue, BufferValueCount, Exponent, Factor, SampleRowCount};
use arrow::datatypes::DataType;
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnSample {
	pub data_type: DataType,
	pub decoded_value_count: usize,
	pub sampled_value_count: usize,
	pub bytes_per_value: Option<usize>,
	pub distinct_count_estimate: u32,
	pub min: Option<BaseValue>,
	pub max: Option<BaseValue>,
	pub run_count_estimate: u32,
	pub avg_string_length: Option<f32>,
	pub is_monotonic: bool,
	pub fsst_compressible: bool,
	pub alp_candidate: bool,
	pub alp_exponent: Option<Exponent>,
	pub alp_factor: Option<Factor>,
	pub delta_width_bytes: Option<usize>,
}

pub fn sample_buffer(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	sample_rows: SampleRowCount,
) -> ColumnSample {
	let value_count = decoded_value_count.get() as usize;
	let sample_limit = (sample_rows.get() as usize).max(1);
	let bytes_per_value = fixed_width_bytes_per_value(input, value_count);
	let sampled_indices = deterministic_sample_indices(value_count, sample_limit);
	let mut distinct_fixed = BTreeSet::<i128>::new();
	let mut distinct_byte = [false; 256];
	let mut distinct_byte_count = 0_u32;
	let mut min = None::<i128>;
	let mut max = None::<i128>;
	let mut previous_value = None::<i128>;
	let mut previous_byte = None::<u8>;
	let mut run_count = 0_u32;
	let mut direction = 0_i8;
	let mut monotonic = true;
	let mut delta_width = 1_usize;

	if let Some(width) = bytes_per_value {
		for index in sampled_indices {
			let start = index.saturating_mul(width);
			let end = start.saturating_add(width);
			let Some(bytes) = input.get(start..end) else {
				continue;
			};
			let value = read_unsigned_le_as_i128(bytes);
			distinct_fixed.insert(value);
			if previous_value != Some(value) {
				run_count = run_count.saturating_add(1);
			}
			min = Some(min.map_or(value, |current| current.min(value)));
			max = Some(max.map_or(value, |current| current.max(value)));
			if let Some(previous) = previous_value {
				let Some(delta) = value.checked_sub(previous) else {
					monotonic = false;
					delta_width = 16;
					previous_value = Some(value);
					continue;
				};
				let step_direction = if delta == 0 {
					0
				} else if delta > 0 {
					1
				} else {
					-1
				};
				if step_direction != 0 {
					if direction == 0 {
						direction = step_direction;
					} else if direction != step_direction {
						monotonic = false;
					}
				}
				delta_width = delta_width.max(delta_width_bytes(delta));
			}
			previous_value = Some(value);
		}
	} else {
		for byte in input.iter().take(sample_limit.saturating_mul(64)) {
			let slot = &mut distinct_byte[*byte as usize];
			if !*slot {
				*slot = true;
				distinct_byte_count = distinct_byte_count.saturating_add(1);
			}
			if previous_byte != Some(*byte) {
				run_count = run_count.saturating_add(1);
				previous_byte = Some(*byte);
			}
		}
		monotonic = false;
	}

	let sampled_value_count = if bytes_per_value.is_some() {
		sample_limit.min(value_count)
	} else {
		input.len().min(sample_limit.saturating_mul(64))
	};
	let (alp_candidate, alp_exponent, alp_factor) = sample_alp(data_type, input, bytes_per_value);

	ColumnSample {
		data_type: data_type.clone(),
		decoded_value_count: value_count,
		sampled_value_count,
		bytes_per_value,
		distinct_count_estimate: bytes_per_value
			.map(|_| u32::try_from(distinct_fixed.len()).unwrap_or(u32::MAX))
			.unwrap_or(distinct_byte_count),
		min: min.map(BaseValue::new),
		max: max.map(BaseValue::new),
		run_count_estimate: run_count.max(1),
		avg_string_length: sample_avg_variable_width(data_type, input, value_count),
		is_monotonic: monotonic && previous_value.is_some(),
		fsst_compressible: sample_fsst_compressible(data_type, input),
		alp_candidate,
		alp_exponent,
		alp_factor,
		delta_width_bytes: Some(delta_width).filter(|_| bytes_per_value.is_some()),
	}
}

pub(crate) fn deterministic_sample_indices(value_count: usize, sample_limit: usize) -> Vec<usize> {
	if value_count == 0 {
		return Vec::new();
	}
	let sample_count = value_count.min(sample_limit);
	let stride = value_count.div_ceil(sample_count).max(1);
	let mut indices = Vec::with_capacity(sample_count);
	let mut index = 0_usize;
	while indices.len() < sample_count && index < value_count {
		indices.push(index);
		index = index.saturating_add(stride);
	}
	if indices.last().copied() != Some(value_count - 1) && indices.len() < sample_count {
		indices.push(value_count - 1);
	}
	indices
}

fn fixed_width_bytes_per_value(input: &[u8], value_count: usize) -> Option<usize> {
	if value_count == 0 || input.is_empty() || input.len() % value_count != 0 {
		return None;
	}
	Some(input.len() / value_count)
}

fn read_unsigned_le_as_i128(bytes: &[u8]) -> i128 {
	let mut value = 0_i128;
	for (shift, byte) in bytes.iter().enumerate().take(16) {
		value |= i128::from(*byte) << (shift * 8);
	}
	value
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

fn sample_avg_variable_width(
	data_type: &DataType,
	input: &[u8],
	value_count: usize,
) -> Option<f32> {
	if !matches!(
		data_type,
		DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
	) || value_count == 0
	{
		return None;
	}
	Some(input.len() as f32 / value_count as f32)
}

pub(crate) fn sample_fsst_compressible(data_type: &DataType, input: &[u8]) -> bool {
	if !matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) || input.len() < 128 {
		return false;
	}
	let mut ascii = 0_usize;
	let mut repeated_pairs = 0_usize;
	let mut previous = None::<u8>;
	for byte in input {
		if byte.is_ascii_graphic() || byte.is_ascii_whitespace() {
			ascii = ascii.saturating_add(1);
		}
		if previous == Some(*byte) {
			repeated_pairs = repeated_pairs.saturating_add(1);
		}
		previous = Some(*byte);
	}
	let ascii_ratio = ascii as f32 / input.len() as f32;
	ascii_ratio >= 0.90 && repeated_pairs.saturating_mul(20) >= input.len()
}

fn sample_alp(
	data_type: &DataType,
	input: &[u8],
	bytes_per_value: Option<usize>,
) -> (bool, Option<Exponent>, Option<Factor>) {
	let Some(width) = bytes_per_value else {
		return (false, None, None);
	};
	match (data_type, width) {
		(DataType::Float32, 4) => sample_float32_exponent(input),
		(DataType::Float64, 8) => sample_float64_exponent(input),
		_ => (false, None, None),
	}
}

fn sample_float32_exponent(input: &[u8]) -> (bool, Option<Exponent>, Option<Factor>) {
	let mut counts = [0_u32; 256];
	let mut finite = 0_u32;
	for chunk in input.chunks_exact(4).take(256) {
		let bits = u32::from_le_bytes(chunk.try_into().expect("fixed width"));
		let exponent = ((bits >> 23) & 0xff) as usize;
		if exponent == 0xff {
			continue;
		}
		counts[exponent] = counts[exponent].saturating_add(1);
		finite = finite.saturating_add(1);
	}
	dominant_exponent(counts, finite)
}

fn sample_float64_exponent(input: &[u8]) -> (bool, Option<Exponent>, Option<Factor>) {
	let mut counts = [0_u32; 2048];
	let mut finite = 0_u32;
	for chunk in input.chunks_exact(8).take(256) {
		let bits = u64::from_le_bytes(chunk.try_into().expect("fixed width"));
		let exponent = ((bits >> 52) & 0x7ff) as usize;
		if exponent == 0x7ff {
			continue;
		}
		counts[exponent] = counts[exponent].saturating_add(1);
		finite = finite.saturating_add(1);
	}
	dominant_exponent(counts, finite)
}

fn dominant_exponent<const N: usize>(
	counts: [u32; N],
	finite: u32,
) -> (bool, Option<Exponent>, Option<Factor>) {
	if finite == 0 {
		return (false, None, None);
	}
	let Some((exponent, count)) = counts.iter().enumerate().max_by_key(|(_, count)| **count) else {
		return (false, None, None);
	};
	if count.saturating_mul(10) >= finite.saturating_mul(9) {
		(
			true,
			Some(Exponent::new(i16::try_from(exponent).unwrap_or(i16::MAX))),
			Some(Factor::new(1)),
		)
	} else {
		(false, None, None)
	}
}
