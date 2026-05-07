use crate::schema::{
	BaseValue, BitsPerValue, BufferValueCount, EncodingPageByteOffset, RecordBodyByteLen,
	SimdPageValueCount,
};
use crate::storage::encoding::{
	EncodedBufferByteRange, EncodingCascade, EncodingLeafCascade, EncodingRejectCode,
};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::io::{Read, Write};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedBuffer {
	pub cascade: EncodingCascade,
	pub bytes: Vec<u8>,
	pub decoded_byte_len: RecordBodyByteLen,
	pub decoded_value_count: BufferValueCount,
	pub page_end_offsets: Vec<EncodingPageByteOffset>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BitPackedFrame<'a> {
	pub encoded: &'a [u8],
	pub bits: u8,
	pub base: u128,
	pub value_width: usize,
	pub value_count: usize,
	pub decoded_byte_len: usize,
}

impl BitPackedFrame<'_> {
	pub(crate) fn unpack_values(&self) -> Result<Vec<u128>, EncodingRejectCode> {
		let mask = if self.bits == 64 {
			u128::from(u64::MAX)
		} else {
			(1_u128 << self.bits) - 1
		};
		let mut out = Vec::with_capacity(self.value_count);
		let mut acc = 0_u128;
		let mut acc_bits = 0_u8;
		let mut cursor = 0_usize;
		while out.len() < self.value_count {
			while acc_bits < self.bits {
				let Some(byte) = self.encoded.get(cursor) else {
					return Err(EncodingRejectCode::DecodedLengthMismatch);
				};
				acc |= u128::from(*byte) << acc_bits;
				acc_bits += 8;
				cursor += 1;
			}
			out.push(self.base.saturating_add(acc & mask));
			acc >>= self.bits;
			acc_bits -= self.bits;
		}
		Ok(out)
	}

	pub(crate) fn decode_bytes(&self) -> Result<Vec<u8>, EncodingRejectCode> {
		let values = self.unpack_values()?;
		let mut out = Vec::with_capacity(self.decoded_byte_len);
		for value in values {
			write_low_u128_bytes(&mut out, value, self.value_width);
		}
		Ok(out)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DictionaryIndexEncoding {
	Plain { width: usize },
	BitPacked { bits: u8 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FixedWidthDictionaryFrame<'a> {
	pub value_width: usize,
	pub value_count: usize,
	pub dict_count: usize,
	pub dictionary: &'a [u8],
	pub index_encoding: DictionaryIndexEncoding,
	pub index_payload: &'a [u8],
}

impl FixedWidthDictionaryFrame<'_> {
	pub(crate) fn indices(&self) -> Result<Vec<usize>, EncodingRejectCode> {
		match self.index_encoding {
			DictionaryIndexEncoding::BitPacked { bits } => {
				unpack_dictionary_indices(self.index_payload, self.value_count, bits)
			}
			DictionaryIndexEncoding::Plain { width } => {
				let expected = self
					.value_count
					.checked_mul(width)
					.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
				if self.index_payload.len() != expected {
					return Err(EncodingRejectCode::DecodedLengthMismatch);
				}
				let mut indices = Vec::with_capacity(self.value_count);
				for bytes in self.index_payload.chunks_exact(width) {
					indices.push(read_dictionary_index(bytes, width)?);
				}
				Ok(indices)
			}
		}
	}

	pub(crate) fn decode_bytes(&self) -> Result<Vec<u8>, EncodingRejectCode> {
		let indices = self.indices()?;
		let mut out = Vec::with_capacity(
			self.value_count
				.checked_mul(self.value_width)
				.ok_or(EncodingRejectCode::DecodedLengthMismatch)?,
		);
		for index in indices {
			let start = index
				.checked_mul(self.value_width)
				.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
			let end = start
				.checked_add(self.value_width)
				.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
			let Some(value) = self.dictionary.get(start..end) else {
				return Err(EncodingRejectCode::DecodedLengthMismatch);
			};
			out.extend_from_slice(value);
		}
		Ok(out)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DeltaFrame<'a> {
	pub value_width: usize,
	pub delta_width: usize,
	pub value_count: usize,
	pub first_value: i128,
	pub deltas: &'a [u8],
}

impl DeltaFrame<'_> {
	pub(crate) fn values(&self) -> Result<Vec<i128>, EncodingRejectCode> {
		let mut values = Vec::with_capacity(self.value_count);
		values.push(self.first_value);
		let mut value = self.first_value;
		for delta_bytes in self.deltas.chunks_exact(self.delta_width) {
			let delta = read_signed_delta(delta_bytes, self.delta_width)?;
			value = value
				.checked_add(delta)
				.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
			values.push(value);
		}
		if values.len() != self.value_count {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		Ok(values)
	}

	pub(crate) fn decode_bytes(&self) -> Result<Vec<u8>, EncodingRejectCode> {
		let values = self.values()?;
		let mut out = Vec::with_capacity(
			self.value_count
				.checked_mul(self.value_width)
				.ok_or(EncodingRejectCode::DecodedLengthMismatch)?,
		);
		for value in values {
			write_low_bytes(&mut out, value, self.value_width);
		}
		Ok(out)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RleByteRun {
	pub run_len: usize,
	pub byte: u8,
}

pub(crate) struct RleByteRunIter<'a> {
	encoded: &'a [u8],
	cursor: usize,
}

impl<'a> RleByteRunIter<'a> {
	pub(crate) fn new(encoded: &'a [u8]) -> Self {
		Self { encoded, cursor: 0 }
	}
}

impl Iterator for RleByteRunIter<'_> {
	type Item = Result<RleByteRun, EncodingRejectCode>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.cursor == self.encoded.len() {
			return None;
		}
		if self.encoded.len().saturating_sub(self.cursor) < 5 {
			self.cursor = self.encoded.len();
			return Some(Err(EncodingRejectCode::DecodedLengthMismatch));
		}
		let run_len = u32::from_be_bytes(
			self.encoded[self.cursor..self.cursor + 4]
				.try_into()
				.expect("fixed width"),
		) as usize;
		let byte = self.encoded[self.cursor + 4];
		self.cursor += 5;
		Some(Ok(RleByteRun { run_len, byte }))
	}
}

impl EncodedBuffer {
	pub fn encoded_range(&self) -> EncodedBufferByteRange {
		EncodedBufferByteRange {
			start: EncodingPageByteOffset::ZERO,
			len: RecordBodyByteLen::new(self.bytes.len() as u64),
		}
	}
}

pub fn encode_buffer(
	input: &[u8],
	data_type: &DataType,
	cascade: EncodingCascade,
	decoded_value_count: BufferValueCount,
	page_rows: SimdPageValueCount,
) -> std::io::Result<EncodedBuffer> {
	encode_buffer_with_value_offsets(
		input,
		data_type,
		cascade,
		decoded_value_count,
		page_rows,
		None,
	)
}

pub fn encode_buffer_with_value_offsets(
	input: &[u8],
	data_type: &DataType,
	cascade: EncodingCascade,
	decoded_value_count: BufferValueCount,
	page_rows: SimdPageValueCount,
	value_offsets: Option<&[u64]>,
) -> std::io::Result<EncodedBuffer> {
	let (bytes, page_end_offsets) =
		if let Some(bytes_per_value) = fixed_width_bytes_per_value(input, decoded_value_count) {
			if cascade.emits_pages_in_v1()
				&& value_offsets.is_none()
				&& page_rows.get() > 0
				&& decoded_value_count.get() > page_rows.get()
			{
				encode_fixed_width_pages(
					input,
					data_type,
					&cascade,
					decoded_value_count,
					page_rows,
					bytes_per_value,
				)?
			} else {
				let bytes = encode_payload(
					input,
					data_type,
					&cascade,
					decoded_value_count,
					value_offsets,
				)?;
				let page_end_offsets = if cascade.emits_pages_in_v1() && !bytes.is_empty() {
					vec![EncodingPageByteOffset::new(
						u32::try_from(bytes.len()).map_err(|_| {
							std::io::Error::new(
								std::io::ErrorKind::InvalidData,
								"Encoding page offset exceeds u32",
							)
						})?,
					)]
				} else {
					Vec::new()
				};
				(bytes, page_end_offsets)
			}
		} else {
			let bytes = encode_payload(
				input,
				data_type,
				&cascade,
				decoded_value_count,
				value_offsets,
			)?;
			let page_end_offsets = if cascade.emits_pages_in_v1() && !bytes.is_empty() {
				vec![EncodingPageByteOffset::new(
					u32::try_from(bytes.len()).map_err(|_| {
						std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"Encoding page offset exceeds u32",
						)
					})?,
				)]
			} else {
				Vec::new()
			};
			(bytes, page_end_offsets)
		};
	let _ = data_type;
	Ok(EncodedBuffer {
		cascade,
		bytes,
		decoded_byte_len: RecordBodyByteLen::new(input.len() as u64),
		decoded_value_count,
		page_end_offsets,
	})
}

fn encode_payload(
	input: &[u8],
	data_type: &DataType,
	cascade: &EncodingCascade,
	decoded_value_count: BufferValueCount,
	value_offsets: Option<&[u64]>,
) -> std::io::Result<Vec<u8>> {
	Ok(match cascade {
		EncodingCascade::Plain => input.to_vec(),
		EncodingCascade::Lz4Frame => encode_lz4_frame(input)?,
		EncodingCascade::Fsst { .. } => encode_fsst_bytes(input)?,
		EncodingCascade::Alp { .. } => encode_alp_bytes(input, data_type, decoded_value_count)?,
		EncodingCascade::BitPacked { bits, base } => {
			encode_bitpacked_bytes(input, *bits, *base, decoded_value_count)?
		}
		EncodingCascade::Rle { .. } => encode_rle_bytes(input)?,
		EncodingCascade::Dictionary { .. } => {
			let EncodingCascade::Dictionary { inner, .. } = cascade else {
				unreachable!("matched dictionary cascade")
			};
			encode_dictionary_bytes(input, decoded_value_count, value_offsets, inner)?
		}
		EncodingCascade::Delta { .. } => encode_delta_bytes(input, decoded_value_count)?,
	})
}

fn encode_fixed_width_pages(
	input: &[u8],
	data_type: &DataType,
	cascade: &EncodingCascade,
	decoded_value_count: BufferValueCount,
	page_rows: SimdPageValueCount,
	bytes_per_value: usize,
) -> std::io::Result<(Vec<u8>, Vec<EncodingPageByteOffset>)> {
	let mut out = Vec::new();
	let mut page_end_offsets = Vec::new();
	let mut first_value = 0_usize;
	let value_count = decoded_value_count.get() as usize;
	let page_values = page_rows.get() as usize;
	while first_value < value_count {
		let value_end = first_value.saturating_add(page_values).min(value_count);
		let byte_start = first_value.checked_mul(bytes_per_value).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding page byte start overflows",
			)
		})?;
		let byte_end = value_end.checked_mul(bytes_per_value).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding page byte end overflows",
			)
		})?;
		let encoded = encode_payload(
			&input[byte_start..byte_end],
			data_type,
			cascade,
			BufferValueCount::new(u32::try_from(value_end - first_value).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding page value count exceeds u32",
				)
			})?),
			None,
		)?;
		out.extend_from_slice(&encoded);
		page_end_offsets.push(EncodingPageByteOffset::new(
			u32::try_from(out.len()).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding page offset exceeds u32",
				)
			})?,
		));
		first_value = value_end;
	}
	Ok((out, page_end_offsets))
}

pub fn fixed_width_bytes_per_value(
	input: &[u8],
	decoded_value_count: BufferValueCount,
) -> Option<usize> {
	let values = decoded_value_count.get() as usize;
	if values == 0 || input.is_empty() || input.len() % values != 0 {
		return None;
	}
	Some(input.len() / values)
}

pub fn decode_buffer(
	encoded: &[u8],
	data_type: &DataType,
	cascade: &EncodingCascade,
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
) -> Result<Vec<u8>, EncodingRejectCode> {
	let decoded = match cascade {
		EncodingCascade::Plain => encoded.to_vec(),
		EncodingCascade::Lz4Frame => decode_lz4_frame(encoded, expected_byte_len)?,
		EncodingCascade::Fsst { .. } => decode_fsst_bytes(encoded, expected_byte_len)?,
		EncodingCascade::Alp { .. } => {
			decode_alp_bytes(encoded, data_type, expected_byte_len, expected_value_count)?
		}
		EncodingCascade::BitPacked { bits, base } => decode_bitpacked_bytes(
			encoded,
			*bits,
			*base,
			expected_byte_len,
			expected_value_count,
		)?,
		EncodingCascade::Rle { .. } => decode_rle_bytes(encoded, expected_byte_len)?,
		EncodingCascade::Dictionary { inner, .. } => {
			decode_dictionary_bytes(encoded, expected_byte_len, expected_value_count, inner)?
		}
		EncodingCascade::Delta { .. } => decode_delta_bytes(encoded, expected_byte_len)?,
	};
	if decoded.len() as u64 != expected_byte_len.get() {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let _ = (data_type, expected_value_count);
	Ok(decoded)
}

pub fn decode_page(
	encoded: &[u8],
	data_type: &DataType,
	cascade: &EncodingCascade,
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
) -> Result<Vec<u8>, EncodingRejectCode> {
	decode_buffer(
		encoded,
		data_type,
		cascade,
		expected_byte_len,
		expected_value_count,
	)
}

pub fn leaf_to_cascade(leaf: &EncodingLeafCascade) -> EncodingCascade {
	match leaf {
		EncodingLeafCascade::Plain => EncodingCascade::Plain,
		EncodingLeafCascade::Lz4Frame => EncodingCascade::Lz4Frame,
		EncodingLeafCascade::BitPacked { bits, base } => EncodingCascade::BitPacked {
			bits: *bits,
			base: *base,
		},
	}
}

fn encode_lz4_frame(input: &[u8]) -> std::io::Result<Vec<u8>> {
	if input.is_empty() {
		return Ok(Vec::new());
	}
	let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
	encoder.write_all(input)?;
	encoder.finish().map_err(|err| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			format!("Encoding lz4 frame encode: {err}"),
		)
	})
}

fn decode_lz4_frame(
	encoded: &[u8],
	expected_byte_len: RecordBodyByteLen,
) -> Result<Vec<u8>, EncodingRejectCode> {
	if encoded.is_empty() {
		return Ok(Vec::new());
	}
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	let mut out = Vec::with_capacity(expected);
	lz4_flex::frame::FrameDecoder::new(encoded)
		.read_to_end(&mut out)
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	Ok(out)
}

fn encode_fsst_bytes(input: &[u8]) -> std::io::Result<Vec<u8>> {
	if input.is_empty() {
		return Ok(Vec::new());
	}
	let mut counts = vec![0_u32; 65_536];
	for pair in input.windows(2) {
		let key = ((pair[0] as usize) << 8) | pair[1] as usize;
		counts[key] = counts[key].saturating_add(1);
	}
	let mut pairs = counts
		.iter()
		.enumerate()
		.filter(|(_, count)| **count >= 3)
		.collect::<Vec<_>>();
	pairs.sort_by(|(left_key, left_count), (right_key, right_count)| {
		right_count
			.cmp(left_count)
			.then_with(|| left_key.cmp(right_key))
	});
	let symbol_count = pairs.len().min(127);
	let mut pair_to_symbol = vec![u16::MAX; 65_536];
	let mut symbols = Vec::with_capacity(symbol_count);
	for (symbol, (key, _)) in pairs.into_iter().take(symbol_count).enumerate() {
		pair_to_symbol[key] = symbol as u16;
		symbols.push(((key >> 8) as u8, (key & 0xff) as u8));
	}
	let mut out = Vec::with_capacity(input.len());
	out.extend_from_slice(b"WFS1");
	out.push(symbol_count as u8);
	for (left, right) in &symbols {
		out.push(*left);
		out.push(*right);
	}
	let mut cursor = 0_usize;
	while cursor < input.len() {
		if cursor + 1 < input.len() {
			let key = ((input[cursor] as usize) << 8) | input[cursor + 1] as usize;
			let symbol = pair_to_symbol[key];
			if symbol != u16::MAX {
				out.push(0x80 | symbol as u8);
				cursor += 2;
				continue;
			}
		}
		let byte = input[cursor];
		if byte >= 0x80 {
			out.push(0xff);
			out.push(byte);
		} else {
			out.push(byte);
		}
		cursor += 1;
	}
	Ok(out)
}

fn decode_fsst_bytes(
	encoded: &[u8],
	expected_byte_len: RecordBodyByteLen,
) -> Result<Vec<u8>, EncodingRejectCode> {
	if encoded.is_empty() {
		return Ok(Vec::new());
	}
	if encoded.len() < 5 || &encoded[..4] != b"WFS1" {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let symbol_count = encoded[4] as usize;
	let symbol_bytes = symbol_count
		.checked_mul(2)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	let mut cursor = 5_usize
		.checked_add(symbol_bytes)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if cursor > encoded.len() {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let symbols = &encoded[5..cursor];
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	let mut out = Vec::with_capacity(expected);
	while cursor < encoded.len() {
		let byte = encoded[cursor];
		cursor += 1;
		if byte < 0x80 {
			out.push(byte);
			continue;
		}
		if byte != 0xff {
			let index = usize::from(byte & 0x7f)
				.checked_mul(2)
				.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
			let Some(pair) = symbols.get(index..index + 2) else {
				return Err(EncodingRejectCode::DecodedLengthMismatch);
			};
			out.extend_from_slice(pair);
			continue;
		}
		let Some(code) = encoded.get(cursor).copied() else {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		};
		cursor += 1;
		out.push(code);
		if out.len() > expected {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
	}
	Ok(out)
}

fn encode_alp_bytes(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
) -> std::io::Result<Vec<u8>> {
	let width = match data_type {
		DataType::Float32 => 4,
		DataType::Float64 => 8,
		_ => {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding ALP requires Float32 or Float64",
			));
		}
	};
	if fixed_width_bytes_per_value(input, decoded_value_count) != Some(width) {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding ALP value width mismatch",
		));
	}
	let value_count = decoded_value_count.get() as usize;
	if value_count == 0 {
		return Ok(Vec::new());
	}
	let first = &input[..width];
	let mut tail_len = 0_usize;
	while tail_len + 1 < width {
		let byte_index = width - 1 - tail_len;
		if input
			.chunks_exact(width)
			.all(|chunk| chunk[byte_index] == first[byte_index])
		{
			tail_len += 1;
		} else {
			break;
		}
	}
	let prefix_len = width - tail_len;
	let mut out = Vec::with_capacity(10 + tail_len + value_count.saturating_mul(prefix_len));
	out.extend_from_slice(b"WAL1");
	out.push(width as u8);
	out.push(tail_len as u8);
	out.extend_from_slice(&(value_count as u32).to_be_bytes());
	out.extend_from_slice(&first[prefix_len..]);
	for chunk in input.chunks_exact(width) {
		out.extend_from_slice(&chunk[..prefix_len]);
	}
	Ok(out)
}

fn decode_alp_bytes(
	encoded: &[u8],
	data_type: &DataType,
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
) -> Result<Vec<u8>, EncodingRejectCode> {
	if encoded.is_empty() {
		return Ok(Vec::new());
	}
	if encoded.len() < 10 || &encoded[..4] != b"WAL1" {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let width = encoded[4] as usize;
	if !matches!(
		(data_type, width),
		(DataType::Float32, 4) | (DataType::Float64, 8)
	) {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let tail_len = encoded[5] as usize;
	if tail_len >= width {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let value_count = u32::from_be_bytes(encoded[6..10].try_into().expect("fixed width")) as usize;
	if value_count != expected_value_count.get() as usize {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	if value_count.checked_mul(width) != Some(expected) {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let prefix_len = width - tail_len;
	let tail_start = 10_usize;
	let tail_end = tail_start
		.checked_add(tail_len)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	let payload_len = value_count
		.checked_mul(prefix_len)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if tail_end
		.checked_add(payload_len)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?
		!= encoded.len()
	{
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let tail = &encoded[tail_start..tail_end];
	let payload = &encoded[tail_end..];
	let mut out = Vec::with_capacity(expected);
	for prefix in payload.chunks_exact(prefix_len) {
		out.extend_from_slice(prefix);
		out.extend_from_slice(tail);
	}
	Ok(out)
}

fn encode_rle_bytes(input: &[u8]) -> std::io::Result<Vec<u8>> {
	let mut out = Vec::new();
	let mut cursor = 0_usize;
	while cursor < input.len() {
		let byte = input[cursor];
		let mut run = 1_usize;
		while cursor + run < input.len() && input[cursor + run] == byte && run < u32::MAX as usize {
			run += 1;
		}
		out.extend_from_slice(&(run as u32).to_be_bytes());
		out.push(byte);
		cursor += run;
	}
	Ok(out)
}

fn decode_rle_bytes(
	encoded: &[u8],
	expected_byte_len: RecordBodyByteLen,
) -> Result<Vec<u8>, EncodingRejectCode> {
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	let mut out = Vec::with_capacity(expected);
	for run in RleByteRunIter::new(encoded) {
		let run = run?;
		let end = out
			.len()
			.checked_add(run.run_len)
			.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
		if end > expected {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		out.resize(end, run.byte);
	}
	if out.len() != expected {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	Ok(out)
}

fn encode_dictionary_bytes(
	input: &[u8],
	decoded_value_count: BufferValueCount,
	value_offsets: Option<&[u64]>,
	inner: &EncodingLeafCascade,
) -> std::io::Result<Vec<u8>> {
	if let Some(offsets) = value_offsets {
		return encode_variable_width_dictionary_bytes(input, decoded_value_count, offsets, inner);
	}
	if let Some(width) = fixed_width_bytes_per_value(input, decoded_value_count) {
		if matches!(width, 1 | 2 | 4 | 8) {
			return encode_fixed_width_dictionary_bytes(input, width, decoded_value_count, inner);
		}
	}
	encode_byte_dictionary_bytes(input)
}

fn encode_fixed_width_dictionary_bytes(
	input: &[u8],
	width: usize,
	decoded_value_count: BufferValueCount,
	inner: &EncodingLeafCascade,
) -> std::io::Result<Vec<u8>> {
	let value_count = decoded_value_count.get() as usize;
	if value_count == 0 {
		return Ok(Vec::new());
	}
	let mut lookup = HashMap::<u64, u32>::with_capacity(64);
	let mut dict = Vec::<u64>::new();
	let mut indices = Vec::<u32>::with_capacity(value_count);
	for value in input.chunks_exact(width) {
		let value = read_unsigned_le_as_u64(value);
		let index = if let Some(index) = lookup.get(&value) {
			*index
		} else {
			let index = u32::try_from(dict.len()).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding Dictionary value count exceeds u32",
				)
			})?;
			lookup.insert(value, index);
			dict.push(value);
			index
		};
		indices.push(index);
	}
	match inner {
		EncodingLeafCascade::BitPacked { bits, .. } => {
			let bits = bits.get();
			let packed = pack_dictionary_indices(&indices, bits)?;
			let mut out = Vec::with_capacity(
				14_usize
					.saturating_add(dict.len().saturating_mul(width))
					.saturating_add(packed.len()),
			);
			out.extend_from_slice(b"WDB2");
			out.push(width as u8);
			out.push(bits);
			out.extend_from_slice(&(value_count as u32).to_be_bytes());
			out.extend_from_slice(&(dict.len() as u32).to_be_bytes());
			for value in &dict {
				write_low_u64_bytes(&mut out, *value, width);
			}
			out.extend_from_slice(&packed);
			Ok(out)
		}
		EncodingLeafCascade::Plain | EncodingLeafCascade::Lz4Frame => {
			let index_width = dictionary_index_width(dict.len())?;
			let mut index_payload = Vec::with_capacity(indices.len().saturating_mul(index_width));
			for index in indices {
				write_dictionary_index(&mut index_payload, index, index_width)?;
			}
			if matches!(inner, EncodingLeafCascade::Lz4Frame) {
				index_payload = encode_lz4_frame(&index_payload)?;
			}
			let mut out = Vec::with_capacity(
				14_usize
					.saturating_add(dict.len().saturating_mul(width))
					.saturating_add(index_payload.len()),
			);
			out.extend_from_slice(if matches!(inner, EncodingLeafCascade::Lz4Frame) {
				b"WDL2"
			} else {
				b"WDD1"
			});
			out.push(width as u8);
			out.push(index_width as u8);
			out.extend_from_slice(&(value_count as u32).to_be_bytes());
			out.extend_from_slice(&(dict.len() as u32).to_be_bytes());
			for value in &dict {
				write_low_u64_bytes(&mut out, *value, width);
			}
			out.extend_from_slice(&index_payload);
			Ok(out)
		}
	}
}

fn encode_variable_width_dictionary_bytes(
	input: &[u8],
	decoded_value_count: BufferValueCount,
	value_offsets: &[u64],
	inner: &EncodingLeafCascade,
) -> std::io::Result<Vec<u8>> {
	let value_count = decoded_value_count.get() as usize;
	if value_count == 0 {
		return Ok(Vec::new());
	}
	if value_offsets.len() != value_count.saturating_add(1) {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding Dictionary variable-width offset count mismatch",
		));
	}
	let mut lookup = HashMap::<&[u8], u32>::with_capacity(64);
	let mut dict = Vec::<&[u8]>::new();
	let mut indices = Vec::<u32>::with_capacity(value_count);
	let mut previous = 0_u64;
	for pair in value_offsets.windows(2) {
		let start = pair[0];
		let end = pair[1];
		if start < previous || end < start || end > input.len() as u64 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding Dictionary variable-width offset range invalid",
			));
		}
		previous = end;
		let value = &input[start as usize..end as usize];
		let index = if let Some(index) = lookup.get(value) {
			*index
		} else {
			let index = u32::try_from(dict.len()).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding Dictionary value count exceeds u32",
				)
			})?;
			lookup.insert(value, index);
			dict.push(value);
			index
		};
		indices.push(index);
	}
	if value_offsets.last().copied() != Some(input.len() as u64) {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding Dictionary variable-width final offset mismatch",
		));
	}
	let dict_payload_len = dict.iter().try_fold(0_usize, |sum, value| {
		sum.checked_add(4)
			.and_then(|next| next.checked_add(value.len()))
			.ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding Dictionary payload length overflow",
				)
			})
	})?;
	match inner {
		EncodingLeafCascade::BitPacked { bits, .. } => {
			let bits = bits.get();
			let index_payload = pack_dictionary_indices(&indices, bits)?;
			let mut out = Vec::with_capacity(
				17_usize
					.saturating_add(dict_payload_len)
					.saturating_add(index_payload.len()),
			);
			out.extend_from_slice(b"WVB2");
			out.push(bits);
			write_variable_dictionary_body(&mut out, value_count, &dict, dict_payload_len)?;
			out.extend_from_slice(&index_payload);
			Ok(out)
		}
		EncodingLeafCascade::Plain | EncodingLeafCascade::Lz4Frame => {
			let index_width = dictionary_index_width(dict.len())?;
			let mut index_payload = Vec::with_capacity(indices.len().saturating_mul(index_width));
			for index in indices {
				write_dictionary_index(&mut index_payload, index, index_width)?;
			}
			if matches!(inner, EncodingLeafCascade::Lz4Frame) {
				index_payload = encode_lz4_frame(&index_payload)?;
			}
			let mut out = Vec::with_capacity(
				17_usize
					.saturating_add(dict_payload_len)
					.saturating_add(index_payload.len()),
			);
			out.extend_from_slice(if matches!(inner, EncodingLeafCascade::Lz4Frame) {
				b"WVL2"
			} else {
				b"WDV1"
			});
			out.push(index_width as u8);
			write_variable_dictionary_body(&mut out, value_count, &dict, dict_payload_len)?;
			out.extend_from_slice(&index_payload);
			Ok(out)
		}
	}
}

fn write_variable_dictionary_body(
	out: &mut Vec<u8>,
	value_count: usize,
	dict: &[&[u8]],
	dict_payload_len: usize,
) -> std::io::Result<()> {
	out.extend_from_slice(&(value_count as u32).to_be_bytes());
	out.extend_from_slice(&(dict.len() as u32).to_be_bytes());
	out.extend_from_slice(&(dict_payload_len as u32).to_be_bytes());
	for value in dict {
		out.extend_from_slice(
			&u32::try_from(value.len())
				.map_err(|_| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"Encoding Dictionary value length exceeds u32",
					)
				})?
				.to_be_bytes(),
		);
		out.extend_from_slice(value);
	}
	Ok(())
}

fn pack_dictionary_indices(indices: &[u32], bits: u8) -> std::io::Result<Vec<u8>> {
	if bits == 0 || bits > 32 {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding Dictionary invalid bit-packed index width",
		));
	}
	let max_value = if bits == 32 {
		u32::MAX
	} else {
		(1_u32 << bits) - 1
	};
	let mut out = Vec::with_capacity((indices.len() * bits as usize).div_ceil(8));
	let mut acc = 0_u64;
	let mut acc_bits = 0_u8;
	for index in indices {
		if *index > max_value {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding Dictionary index exceeds bit-packed width",
			));
		}
		acc |= u64::from(*index) << acc_bits;
		acc_bits += bits;
		while acc_bits >= 8 {
			out.push(acc as u8);
			acc >>= 8;
			acc_bits -= 8;
		}
	}
	if acc_bits > 0 {
		out.push(acc as u8);
	}
	Ok(out)
}

fn unpack_dictionary_indices(
	encoded: &[u8],
	value_count: usize,
	bits: u8,
) -> Result<Vec<usize>, EncodingRejectCode> {
	if bits == 0 || bits > 32 {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let expected_len = value_count
		.checked_mul(bits as usize)
		.map(|bits| bits.div_ceil(8))
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if encoded.len() != expected_len {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let mask = if bits == 32 {
		u64::from(u32::MAX)
	} else {
		(1_u64 << bits) - 1
	};
	let mut out = Vec::with_capacity(value_count);
	let mut acc = 0_u64;
	let mut acc_bits = 0_u8;
	let mut cursor = 0_usize;
	while out.len() < value_count {
		while acc_bits < bits {
			let Some(byte) = encoded.get(cursor) else {
				return Err(EncodingRejectCode::DecodedLengthMismatch);
			};
			acc |= u64::from(*byte) << acc_bits;
			acc_bits += 8;
			cursor += 1;
		}
		out.push((acc & mask) as usize);
		acc >>= bits;
		acc_bits -= bits;
	}
	Ok(out)
}

fn read_unsigned_le_as_u64(bytes: &[u8]) -> u64 {
	let mut value = 0_u64;
	for (shift, byte) in bytes.iter().enumerate().take(8) {
		value |= u64::from(*byte) << (shift * 8);
	}
	value
}

fn write_low_u64_bytes(out: &mut Vec<u8>, value: u64, width: usize) {
	let bytes = value.to_le_bytes();
	out.extend_from_slice(&bytes[..width]);
}

fn dictionary_index_width(dict_len: usize) -> std::io::Result<usize> {
	if dict_len <= u8::MAX as usize + 1 {
		Ok(1)
	} else if dict_len <= u16::MAX as usize + 1 {
		Ok(2)
	} else if dict_len <= u32::MAX as usize {
		Ok(4)
	} else {
		Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding Dictionary value count exceeds u32",
		))
	}
}

fn write_dictionary_index(out: &mut Vec<u8>, index: u32, width: usize) -> std::io::Result<()> {
	match width {
		1 => out.push(u8::try_from(index).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding Dictionary index exceeds u8",
			)
		})?),
		2 => out.extend_from_slice(
			&u16::try_from(index)
				.map_err(|_| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"Encoding Dictionary index exceeds u16",
					)
				})?
				.to_be_bytes(),
		),
		4 => out.extend_from_slice(&index.to_be_bytes()),
		_ => {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding Dictionary invalid index width",
			));
		}
	}
	Ok(())
}

fn encode_byte_dictionary_bytes(input: &[u8]) -> std::io::Result<Vec<u8>> {
	let mut present = [false; 256];
	for byte in input {
		present[*byte as usize] = true;
	}
	let mut dict = Vec::new();
	let mut index = [0_u8; 256];
	for (byte, is_present) in present.iter().copied().enumerate() {
		if is_present {
			if dict.len() == u8::MAX as usize {
				return Ok(input.to_vec());
			}
			index[byte] = dict.len() as u8;
			dict.push(byte as u8);
		}
	}
	let mut out = Vec::with_capacity(2 + dict.len() + input.len());
	out.extend_from_slice(&(dict.len() as u16).to_be_bytes());
	out.extend_from_slice(&dict);
	for byte in input {
		out.push(index[*byte as usize]);
	}
	Ok(out)
}

fn decode_dictionary_bytes(
	encoded: &[u8],
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
	inner: &EncodingLeafCascade,
) -> Result<Vec<u8>, EncodingRejectCode> {
	if encoded.starts_with(b"WDD1") || encoded.starts_with(b"WDB2") || encoded.starts_with(b"WDL2")
	{
		return decode_fixed_width_dictionary_bytes(
			encoded,
			expected_byte_len,
			expected_value_count,
			inner,
		);
	}
	if encoded.starts_with(b"WDV1") || encoded.starts_with(b"WVB2") || encoded.starts_with(b"WVL2")
	{
		return decode_variable_width_dictionary_bytes(
			encoded,
			expected_byte_len,
			expected_value_count,
			inner,
		);
	}
	decode_byte_dictionary_bytes(encoded, expected_byte_len)
}

pub(crate) fn parse_fixed_width_dictionary_frame<'a>(
	encoded: &'a [u8],
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
	inner: &EncodingLeafCascade,
) -> Result<FixedWidthDictionaryFrame<'a>, EncodingRejectCode> {
	if encoded.len() < 14 {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let bitpacked = encoded.starts_with(b"WDB2");
	if !bitpacked && !encoded.starts_with(b"WDD1") {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let value_width = encoded[4] as usize;
	let index_width_or_bits = encoded[5] as usize;
	if !matches!(value_width, 1 | 2 | 4 | 8) {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let index_encoding = if bitpacked {
		if !matches!(inner, EncodingLeafCascade::BitPacked { .. }) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		if !(1..=32).contains(&index_width_or_bits) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		DictionaryIndexEncoding::BitPacked {
			bits: index_width_or_bits as u8,
		}
	} else {
		if !matches!(inner, EncodingLeafCascade::Plain) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		if !matches!(index_width_or_bits, 1 | 2 | 4) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		DictionaryIndexEncoding::Plain {
			width: index_width_or_bits,
		}
	};
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	let value_count = u32::from_be_bytes(encoded[6..10].try_into().expect("fixed width")) as usize;
	let dict_count = u32::from_be_bytes(encoded[10..14].try_into().expect("fixed width")) as usize;
	if value_count != expected_value_count.get() as usize
		|| value_count.checked_mul(value_width) != Some(expected)
	{
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let dict_byte_len = dict_count
		.checked_mul(value_width)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	let dict_start = 14_usize;
	let dict_end = dict_start
		.checked_add(dict_byte_len)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if dict_end > encoded.len() {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let index_payload = &encoded[dict_end..];
	let expected_index_len = match index_encoding {
		DictionaryIndexEncoding::BitPacked { bits } => value_count
			.checked_mul(bits as usize)
			.map(|bits| bits.div_ceil(8))
			.ok_or(EncodingRejectCode::DecodedLengthMismatch)?,
		DictionaryIndexEncoding::Plain { width } => value_count
			.checked_mul(width)
			.ok_or(EncodingRejectCode::DecodedLengthMismatch)?,
	};
	if index_payload.len() != expected_index_len {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	Ok(FixedWidthDictionaryFrame {
		value_width,
		value_count,
		dict_count,
		dictionary: &encoded[dict_start..dict_end],
		index_encoding,
		index_payload,
	})
}

fn decode_fixed_width_dictionary_bytes(
	encoded: &[u8],
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
	inner: &EncodingLeafCascade,
) -> Result<Vec<u8>, EncodingRejectCode> {
	if encoded.starts_with(b"WDD1") || encoded.starts_with(b"WDB2") {
		return parse_fixed_width_dictionary_frame(
			encoded,
			expected_byte_len,
			expected_value_count,
			inner,
		)?
		.decode_bytes();
	}
	if encoded.len() < 14 {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let bitpacked = encoded.starts_with(b"WDB2");
	let lz4_indices = encoded.starts_with(b"WDL2");
	let width = encoded[4] as usize;
	let index_width_or_bits = encoded[5] as usize;
	if !matches!(width, 1 | 2 | 4 | 8) {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	if bitpacked {
		if !matches!(inner, EncodingLeafCascade::BitPacked { .. }) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
	} else if lz4_indices {
		if !matches!(inner, EncodingLeafCascade::Lz4Frame) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
	} else if !matches!(index_width_or_bits, 1 | 2 | 4) {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	let value_count = u32::from_be_bytes(encoded[6..10].try_into().expect("fixed width")) as usize;
	let dict_count = u32::from_be_bytes(encoded[10..14].try_into().expect("fixed width")) as usize;
	if value_count != expected_value_count.get() as usize
		|| value_count.checked_mul(width) != Some(expected)
	{
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let dict_byte_len = dict_count
		.checked_mul(width)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	let dict_start = 14_usize;
	let dict_end = dict_start
		.checked_add(dict_byte_len)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if dict_end > encoded.len() {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let mut index_payload = encoded[dict_end..].to_vec();
	let indices = if bitpacked {
		unpack_dictionary_indices(&index_payload, value_count, index_width_or_bits as u8)?
	} else {
		if lz4_indices {
			index_payload = decode_lz4_frame(
				&index_payload,
				RecordBodyByteLen::new(
					value_count
						.checked_mul(index_width_or_bits)
						.ok_or(EncodingRejectCode::DecodedLengthMismatch)? as u64,
				),
			)?;
		}
		if index_payload.len() != value_count.saturating_mul(index_width_or_bits) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		let mut indices = Vec::with_capacity(value_count);
		let mut cursor = 0_usize;
		for _ in 0..value_count {
			let index = read_dictionary_index(
				&index_payload[cursor..cursor + index_width_or_bits],
				index_width_or_bits,
			)?;
			cursor += index_width_or_bits;
			indices.push(index);
		}
		indices
	};
	let dict = &encoded[dict_start..dict_end];
	let mut out = Vec::with_capacity(expected);
	for index in indices {
		let start = index
			.checked_mul(width)
			.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
		let end = start
			.checked_add(width)
			.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
		let Some(value) = dict.get(start..end) else {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		};
		out.extend_from_slice(value);
	}
	Ok(out)
}

fn decode_variable_width_dictionary_bytes(
	encoded: &[u8],
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
	inner: &EncodingLeafCascade,
) -> Result<Vec<u8>, EncodingRejectCode> {
	if encoded.len() < 17 {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let bitpacked = encoded.starts_with(b"WVB2");
	let lz4_indices = encoded.starts_with(b"WVL2");
	let index_width_or_bits = encoded[4] as usize;
	if bitpacked {
		if !matches!(inner, EncodingLeafCascade::BitPacked { .. }) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
	} else if lz4_indices {
		if !matches!(inner, EncodingLeafCascade::Lz4Frame) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
	} else if !matches!(index_width_or_bits, 1 | 2 | 4) {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let value_count = u32::from_be_bytes(encoded[5..9].try_into().expect("fixed width")) as usize;
	let dict_count = u32::from_be_bytes(encoded[9..13].try_into().expect("fixed width")) as usize;
	let dict_payload_len =
		u32::from_be_bytes(encoded[13..17].try_into().expect("fixed width")) as usize;
	if value_count != expected_value_count.get() as usize {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let dict_start = 17_usize;
	let dict_end = dict_start
		.checked_add(dict_payload_len)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if dict_end > encoded.len() {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let mut dict = Vec::<&[u8]>::with_capacity(dict_count);
	let mut cursor = dict_start;
	for _ in 0..dict_count {
		if dict_end.saturating_sub(cursor) < 4 {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		let len = u32::from_be_bytes(encoded[cursor..cursor + 4].try_into().expect("fixed width"))
			as usize;
		cursor += 4;
		let end = cursor
			.checked_add(len)
			.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
		if end > dict_end {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		dict.push(&encoded[cursor..end]);
		cursor = end;
	}
	if cursor != dict_end {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let mut index_payload = encoded[dict_end..].to_vec();
	let indices = if bitpacked {
		unpack_dictionary_indices(&index_payload, value_count, index_width_or_bits as u8)?
	} else {
		if lz4_indices {
			index_payload = decode_lz4_frame(
				&index_payload,
				RecordBodyByteLen::new(
					value_count
						.checked_mul(index_width_or_bits)
						.ok_or(EncodingRejectCode::DecodedLengthMismatch)? as u64,
				),
			)?;
		}
		if index_payload.len() != value_count.saturating_mul(index_width_or_bits) {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
		let mut indices = Vec::with_capacity(value_count);
		let mut cursor = 0_usize;
		for _ in 0..value_count {
			let index = read_dictionary_index(
				&index_payload[cursor..cursor + index_width_or_bits],
				index_width_or_bits,
			)?;
			cursor += index_width_or_bits;
			indices.push(index);
		}
		indices
	};
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	let mut out = Vec::with_capacity(expected);
	for index in indices {
		let Some(value) = dict.get(index) else {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		};
		out.extend_from_slice(value);
		if out.len() > expected {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		}
	}
	if out.len() != expected {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	Ok(out)
}

fn read_dictionary_index(bytes: &[u8], width: usize) -> Result<usize, EncodingRejectCode> {
	match width {
		1 => Ok(bytes[0] as usize),
		2 => Ok(u16::from_be_bytes(bytes.try_into().expect("fixed width")) as usize),
		4 => Ok(u32::from_be_bytes(bytes.try_into().expect("fixed width")) as usize),
		_ => Err(EncodingRejectCode::DecodedLengthMismatch),
	}
}

fn decode_byte_dictionary_bytes(
	encoded: &[u8],
	expected_byte_len: RecordBodyByteLen,
) -> Result<Vec<u8>, EncodingRejectCode> {
	if encoded.len() < 2 {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let dict_len = u16::from_be_bytes(encoded[..2].try_into().expect("fixed width")) as usize;
	let dict_end = 2_usize
		.checked_add(dict_len)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if dict_end > encoded.len() {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	let dict = &encoded[2..dict_end];
	let indices = &encoded[dict_end..];
	if indices.len() != expected {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let mut out = Vec::with_capacity(expected);
	for index in indices {
		let Some(byte) = dict.get(*index as usize) else {
			return Err(EncodingRejectCode::DecodedLengthMismatch);
		};
		out.push(*byte);
	}
	Ok(out)
}

fn encode_delta_bytes(
	input: &[u8],
	decoded_value_count: BufferValueCount,
) -> std::io::Result<Vec<u8>> {
	let Some(width) = fixed_width_bytes_per_value(input, decoded_value_count) else {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding Delta requires fixed-width values",
		));
	};
	if !matches!(width, 1 | 2 | 4 | 8) {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			format!("Encoding Delta unsupported value width {width}"),
		));
	}
	let count = decoded_value_count.get() as usize;
	if count == 0 {
		return Ok(Vec::new());
	}
	let mut chunks = input.chunks_exact(width);
	let first = read_unsigned_le_as_i128(chunks.next().ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding Delta missing first value",
		)
	})?);
	let mut max_delta_bytes = 1_usize;
	let mut previous = first;
	for chunk in chunks {
		let value = read_unsigned_le_as_i128(chunk);
		let delta = value.checked_sub(previous).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding Delta value delta overflows i128",
			)
		})?;
		max_delta_bytes = max_delta_bytes.max(delta_width_bytes(delta));
		previous = value;
	}
	let mut out = Vec::with_capacity(10 + 16 + count.saturating_mul(max_delta_bytes));
	out.extend_from_slice(b"WDL1");
	out.push(width as u8);
	out.push(max_delta_bytes as u8);
	out.extend_from_slice(&(count as u32).to_be_bytes());
	out.extend_from_slice(&first.to_le_bytes());
	let mut chunks = input.chunks_exact(width);
	let mut previous = read_unsigned_le_as_i128(
		chunks
			.next()
			.expect("Delta first value exists after non-empty count"),
	);
	for chunk in chunks {
		let value = read_unsigned_le_as_i128(chunk);
		let delta = value.checked_sub(previous).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding Delta value delta overflows i128",
			)
		})?;
		write_signed_delta(&mut out, delta, max_delta_bytes)?;
		previous = value;
	}
	Ok(out)
}

pub(crate) fn parse_delta_frame(
	encoded: &[u8],
	expected_byte_len: RecordBodyByteLen,
) -> Result<DeltaFrame<'_>, EncodingRejectCode> {
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	if expected == 0 {
		if encoded.is_empty() {
			return Ok(DeltaFrame {
				value_width: 1,
				delta_width: 1,
				value_count: 0,
				first_value: 0,
				deltas: &[],
			});
		}
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	if encoded.len() < 26 || &encoded[..4] != b"WDL1" {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let value_width = encoded[4] as usize;
	let delta_width = encoded[5] as usize;
	if !matches!(value_width, 1 | 2 | 4 | 8) || !matches!(delta_width, 1 | 2 | 4 | 8 | 16) {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	if expected % value_width != 0 {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let value_count = u32::from_be_bytes(encoded[6..10].try_into().expect("fixed width")) as usize;
	if value_count != expected / value_width {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let deltas_len = value_count
		.saturating_sub(1)
		.checked_mul(delta_width)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	let deltas_start = 26_usize;
	let deltas_end = deltas_start
		.checked_add(deltas_len)
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if deltas_end != encoded.len() {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let first_value = i128::from_le_bytes(encoded[10..26].try_into().expect("fixed width"));
	Ok(DeltaFrame {
		value_width,
		delta_width,
		value_count,
		first_value,
		deltas: &encoded[deltas_start..deltas_end],
	})
}

fn decode_delta_bytes(
	encoded: &[u8],
	expected_byte_len: RecordBodyByteLen,
) -> Result<Vec<u8>, EncodingRejectCode> {
	if expected_byte_len.get() == 0 {
		return Ok(Vec::new());
	}
	parse_delta_frame(encoded, expected_byte_len)?.decode_bytes()
}

fn read_unsigned_le_as_i128(bytes: &[u8]) -> i128 {
	let mut value = 0_i128;
	for (shift, byte) in bytes.iter().enumerate() {
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

fn write_signed_delta(out: &mut Vec<u8>, delta: i128, width: usize) -> std::io::Result<()> {
	match width {
		1 => out.extend_from_slice(&(delta as i8).to_le_bytes()),
		2 => out.extend_from_slice(&(delta as i16).to_le_bytes()),
		4 => out.extend_from_slice(&(delta as i32).to_le_bytes()),
		8 => out.extend_from_slice(&(delta as i64).to_le_bytes()),
		16 => out.extend_from_slice(&delta.to_le_bytes()),
		_ => {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding Delta invalid delta width",
			));
		}
	}
	Ok(())
}

fn read_signed_delta(bytes: &[u8], width: usize) -> Result<i128, EncodingRejectCode> {
	let delta = match width {
		1 => i128::from(i8::from_le_bytes([bytes[0]])),
		2 => i128::from(i16::from_le_bytes(bytes.try_into().expect("fixed width"))),
		4 => i128::from(i32::from_le_bytes(bytes.try_into().expect("fixed width"))),
		8 => i128::from(i64::from_le_bytes(bytes.try_into().expect("fixed width"))),
		16 => i128::from_le_bytes(bytes.try_into().expect("fixed width")),
		_ => return Err(EncodingRejectCode::DecodedLengthMismatch),
	};
	Ok(delta)
}

fn write_low_bytes(out: &mut Vec<u8>, value: i128, width: usize) {
	let bytes = value.to_le_bytes();
	out.extend_from_slice(&bytes[..width]);
}

fn encode_bitpacked_bytes(
	input: &[u8],
	bits: BitsPerValue,
	base: BaseValue,
	decoded_value_count: BufferValueCount,
) -> std::io::Result<Vec<u8>> {
	let bits = bits.get();
	if bits == 0 || bits > 64 {
		return Ok(input.to_vec());
	}
	let Some(width) = fixed_width_bytes_per_value(input, decoded_value_count) else {
		return Ok(input.to_vec());
	};
	if !matches!(width, 1 | 2 | 4 | 8) {
		return Ok(input.to_vec());
	}
	let value_count = decoded_value_count.get() as usize;
	let base = u128::try_from(base.get()).unwrap_or(0);
	let max_value = if bits == 64 {
		u128::from(u64::MAX)
	} else {
		(1_u128 << bits) - 1
	};
	let mut out = Vec::with_capacity((value_count * bits as usize).div_ceil(8));
	let mut acc = 0_u128;
	let mut acc_bits = 0_u8;
	for chunk in input.chunks_exact(width) {
		let value = read_unsigned_le_as_u128(chunk).saturating_sub(base);
		if value > max_value {
			return Ok(input.to_vec());
		}
		acc |= value << acc_bits;
		acc_bits += bits;
		while acc_bits >= 8 {
			out.push(acc as u8);
			acc >>= 8;
			acc_bits -= 8;
		}
	}
	if acc_bits > 0 {
		out.push(acc as u8);
	}
	Ok(out)
}

pub(crate) fn parse_bitpacked_frame<'a>(
	encoded: &'a [u8],
	bits: BitsPerValue,
	base: BaseValue,
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
) -> Result<BitPackedFrame<'a>, EncodingRejectCode> {
	let decoded_byte_len = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	let bits = bits.get();
	if bits == 0 || bits > 64 {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let value_count = expected_value_count.get() as usize;
	if value_count == 0 {
		if encoded.is_empty() && decoded_byte_len == 0 {
			return Ok(BitPackedFrame {
				encoded,
				bits,
				base: u128::try_from(base.get()).unwrap_or(0),
				value_width: 1,
				value_count,
				decoded_byte_len,
			});
		}
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	if decoded_byte_len % value_count != 0 {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let value_width = decoded_byte_len / value_count;
	if !matches!(value_width, 1 | 2 | 4 | 8) {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let expected_encoded_len = value_count
		.checked_mul(bits as usize)
		.map(|bits| bits.div_ceil(8))
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if encoded.len() != expected_encoded_len {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	Ok(BitPackedFrame {
		encoded,
		bits,
		base: u128::try_from(base.get()).unwrap_or(0),
		value_width,
		value_count,
		decoded_byte_len,
	})
}

fn decode_bitpacked_bytes(
	encoded: &[u8],
	bits: BitsPerValue,
	base: BaseValue,
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
) -> Result<Vec<u8>, EncodingRejectCode> {
	let expected = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?;
	let bits = bits.get();
	if bits == 0 || bits > 64 {
		if encoded.len() == expected {
			return Ok(encoded.to_vec());
		}
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let value_count = expected_value_count.get() as usize;
	if value_count == 0 {
		if encoded.is_empty() && expected == 0 {
			return Ok(Vec::new());
		}
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	if expected % value_count != 0 {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let width = expected / value_count;
	if !matches!(width, 1 | 2 | 4 | 8) {
		if encoded.len() == expected {
			return Ok(encoded.to_vec());
		}
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	let expected_encoded_len = value_count
		.checked_mul(bits as usize)
		.map(|bits| bits.div_ceil(8))
		.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
	if encoded.len() != expected_encoded_len {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	parse_bitpacked_frame(
		encoded,
		BitsPerValue::new(bits),
		base,
		expected_byte_len,
		expected_value_count,
	)?
	.decode_bytes()
}

fn read_unsigned_le_as_u128(bytes: &[u8]) -> u128 {
	match bytes.len() {
		1 => u128::from(bytes[0]),
		2 => u128::from(u16::from_le_bytes(bytes.try_into().expect("2-byte value"))),
		4 => u128::from(u32::from_le_bytes(bytes.try_into().expect("4-byte value"))),
		8 => u128::from(u64::from_le_bytes(bytes.try_into().expect("8-byte value"))),
		_ => {
			let mut value = 0_u128;
			for (shift, byte) in bytes.iter().enumerate().take(16) {
				value |= u128::from(*byte) << (shift * 8);
			}
			value
		}
	}
}

fn write_low_u128_bytes(out: &mut Vec<u8>, value: u128, width: usize) {
	match width {
		1 => out.push(value as u8),
		2 => out.extend_from_slice(&(value as u16).to_le_bytes()),
		4 => out.extend_from_slice(&(value as u32).to_le_bytes()),
		8 => out.extend_from_slice(&(value as u64).to_le_bytes()),
		_ => {
			let bytes = value.to_le_bytes();
			out.extend_from_slice(&bytes[..width]);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn bitpacked_frame_parser_matches_decoder() {
		for bits in [1_u8, 2, 4, 7, 8, 12, 16, 31, 32, 63, 64] {
			let mask = if bits == 64 {
				u64::MAX
			} else {
				(1_u64 << bits) - 1
			};
			let values = (0..64_u64).map(|value| value & mask).collect::<Vec<_>>();
			let decoded = values
				.iter()
				.flat_map(|value| value.to_le_bytes())
				.collect::<Vec<_>>();
			let encoded = encode_bitpacked_bytes(
				&decoded,
				BitsPerValue::new(bits),
				BaseValue::ZERO,
				BufferValueCount::new(values.len() as u32),
			)
			.expect("bitpacked encode");
			let frame = parse_bitpacked_frame(
				&encoded,
				BitsPerValue::new(bits),
				BaseValue::ZERO,
				RecordBodyByteLen::new(decoded.len() as u64),
				BufferValueCount::new(values.len() as u32),
			)
			.expect("bitpacked frame");
			assert_eq!(frame.decode_bytes().expect("frame decode"), decoded);
			assert_eq!(
				decode_bitpacked_bytes(
					&encoded,
					BitsPerValue::new(bits),
					BaseValue::ZERO,
					RecordBodyByteLen::new(decoded.len() as u64),
					BufferValueCount::new(values.len() as u32),
				)
				.expect("decode"),
				decoded
			);
		}
	}

	#[test]
	fn fixed_width_dictionary_frame_parser_matches_decoder() {
		let values = [7_u32, 9, 7, 11, 9, 13, 7, 13];
		let decoded = values
			.iter()
			.flat_map(|value| value.to_le_bytes())
			.collect::<Vec<_>>();
		let plain = encode_fixed_width_dictionary_bytes(
			&decoded,
			4,
			BufferValueCount::new(values.len() as u32),
			&EncodingLeafCascade::Plain,
		)
		.expect("plain dictionary encode");
		let frame = parse_fixed_width_dictionary_frame(
			&plain,
			RecordBodyByteLen::new(decoded.len() as u64),
			BufferValueCount::new(values.len() as u32),
			&EncodingLeafCascade::Plain,
		)
		.expect("plain dictionary frame");
		assert_eq!(frame.value_width, 4);
		assert_eq!(frame.value_count, values.len());
		assert_eq!(frame.dict_count, 4);
		assert_eq!(frame.indices().expect("plain indices").len(), values.len());
		assert_eq!(frame.decode_bytes().expect("plain decode"), decoded);

		let bitpacked_inner = EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(2),
			base: BaseValue::ZERO,
		};
		let bitpacked = encode_fixed_width_dictionary_bytes(
			&decoded,
			4,
			BufferValueCount::new(values.len() as u32),
			&bitpacked_inner,
		)
		.expect("bitpacked dictionary encode");
		let frame = parse_fixed_width_dictionary_frame(
			&bitpacked,
			RecordBodyByteLen::new(decoded.len() as u64),
			BufferValueCount::new(values.len() as u32),
			&bitpacked_inner,
		)
		.expect("bitpacked dictionary frame");
		assert!(matches!(
			frame.index_encoding,
			DictionaryIndexEncoding::BitPacked { bits: 2 }
		));
		assert_eq!(frame.decode_bytes().expect("bitpacked decode"), decoded);

		let truncated = &plain[..plain.len() - 1];
		assert_eq!(
			parse_fixed_width_dictionary_frame(
				truncated,
				RecordBodyByteLen::new(decoded.len() as u64),
				BufferValueCount::new(values.len() as u32),
				&EncodingLeafCascade::Plain,
			)
			.unwrap_err(),
			EncodingRejectCode::DecodedLengthMismatch
		);
	}

	#[test]
	fn delta_frame_parser_matches_decoder() {
		let values = [10_i64, 13, 12, 20, 20];
		let decoded = values
			.iter()
			.flat_map(|value| value.to_le_bytes())
			.collect::<Vec<_>>();
		let encoded = encode_delta_bytes(&decoded, BufferValueCount::new(values.len() as u32))
			.expect("delta encode");
		let frame = parse_delta_frame(&encoded, RecordBodyByteLen::new(decoded.len() as u64))
			.expect("delta frame");
		assert_eq!(frame.value_width, 8);
		assert_eq!(frame.value_count, values.len());
		assert_eq!(
			frame.values().expect("delta values"),
			values
				.iter()
				.map(|value| i128::from(*value))
				.collect::<Vec<_>>()
		);
		assert_eq!(frame.decode_bytes().expect("delta decode"), decoded);
		assert_eq!(
			parse_delta_frame(
				&encoded[..encoded.len() - 1],
				RecordBodyByteLen::new(decoded.len() as u64)
			)
			.unwrap_err(),
			EncodingRejectCode::DecodedLengthMismatch
		);
	}

	#[test]
	fn rle_byte_run_iterator_matches_decoder() {
		let decoded = [1_u8, 1, 1, 2, 2, 3];
		let encoded = encode_rle_bytes(&decoded).expect("rle encode");
		let runs = RleByteRunIter::new(&encoded)
			.collect::<Result<Vec<_>, _>>()
			.expect("rle runs");
		assert_eq!(
			runs,
			vec![
				RleByteRun {
					run_len: 3,
					byte: 1
				},
				RleByteRun {
					run_len: 2,
					byte: 2
				},
				RleByteRun {
					run_len: 1,
					byte: 3
				},
			]
		);
		assert_eq!(
			decode_rle_bytes(&encoded, RecordBodyByteLen::new(decoded.len() as u64))
				.expect("rle decode"),
			decoded
		);
		assert_eq!(
			RleByteRunIter::new(&[0_u8, 1, 2])
				.collect::<Result<Vec<_>, _>>()
				.unwrap_err(),
			EncodingRejectCode::DecodedLengthMismatch
		);
	}
}
