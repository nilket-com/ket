use arrow::datatypes::DataType;
use wendelon::schema::{
	BaseValue, BitsPerValue, BufferValueCount, RecordBodyByteLen, SimdPageValueCount,
};
use wendelon::storage::encoding::EncodingCascade;
use wendelon::storage::encoding::cascade::{
	decode_buffer, encode_buffer, encode_buffer_with_value_offsets,
};

fn roundtrip(cascade: EncodingCascade, input: &[u8]) {
	let encoded = encode_buffer(
		input,
		&DataType::UInt8,
		cascade.clone(),
		BufferValueCount::new(input.len() as u32),
		SimdPageValueCount::new(input.len() as u32),
	)
	.unwrap();
	let decoded = decode_buffer(
		&encoded.bytes,
		&DataType::UInt8,
		&cascade,
		RecordBodyByteLen::new(input.len() as u64),
		BufferValueCount::new(input.len() as u32),
	)
	.unwrap();
	assert_eq!(decoded, input);
}

fn u64_bytes(values: impl IntoIterator<Item = u64>) -> Vec<u8> {
	let mut out = Vec::new();
	for value in values {
		out.extend_from_slice(&value.to_le_bytes());
	}
	out
}

#[test]
fn encoding_cascades_roundtrip_byte_buffers() {
	let repeated = vec![7_u8; 4096];
	let bounded = (0..4096).map(|i| (i % 8) as u8).collect::<Vec<_>>();
	let mixed = (0..4096).map(|i| (i % 251) as u8).collect::<Vec<_>>();

	roundtrip(EncodingCascade::Plain, &mixed);
	roundtrip(EncodingCascade::Lz4Frame, &mixed);
	roundtrip(
		EncodingCascade::BitPacked {
			bits: BitsPerValue::new(3),
			base: BaseValue::ZERO,
		},
		&bounded,
	);
	roundtrip(
		EncodingCascade::Rle {
			run_count: wendelon::schema::RunCount::new(1),
			inner: Box::new(wendelon::storage::encoding::EncodingLeafCascade::Plain),
		},
		&repeated,
	);
	roundtrip(
		EncodingCascade::Dictionary {
			dict_byte_len: RecordBodyByteLen::new(8),
			dict_value_count: wendelon::schema::DictValueCount::new(8),
			inner: Box::new(wendelon::storage::encoding::EncodingLeafCascade::Plain),
		},
		&bounded,
	);
	roundtrip(
		EncodingCascade::Delta {
			base: BaseValue::ZERO,
			inner: Box::new(wendelon::storage::encoding::EncodingLeafCascade::Plain),
		},
		&mixed,
	);
	roundtrip(
		EncodingCascade::Fsst {
			symbol_table_byte_len: RecordBodyByteLen::ZERO,
		},
		&mixed,
	);
}

#[test]
fn delta_cascade_roundtrips_fixed_width_monotonic_values() {
	let values = u64_bytes((0..4096).map(|value| 10_000_u64 + value));
	let cascade = EncodingCascade::Delta {
		base: BaseValue::new(10_000),
		inner: Box::new(wendelon::storage::encoding::EncodingLeafCascade::Plain),
	};
	let encoded = encode_buffer(
		&values,
		&DataType::UInt64,
		cascade.clone(),
		BufferValueCount::new(4096),
		SimdPageValueCount::new(4096),
	)
	.unwrap();

	assert!(encoded.bytes.len() < values.len());
	let decoded = decode_buffer(
		&encoded.bytes,
		&DataType::UInt64,
		&cascade,
		RecordBodyByteLen::new(values.len() as u64),
		BufferValueCount::new(4096),
	)
	.unwrap();
	assert_eq!(decoded, values);
}

#[test]
fn dictionary_cascade_roundtrips_fixed_width_low_cardinality_values() {
	let values = u64_bytes((0..4096).map(|value| value % 16));
	let cascade = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(16 * 8),
		dict_value_count: wendelon::schema::DictValueCount::new(16),
		inner: Box::new(wendelon::storage::encoding::EncodingLeafCascade::Plain),
	};
	let encoded = encode_buffer(
		&values,
		&DataType::UInt64,
		cascade.clone(),
		BufferValueCount::new(4096),
		SimdPageValueCount::new(4096),
	)
	.unwrap();
	let decoded = decode_buffer(
		&encoded.bytes,
		&DataType::UInt64,
		&cascade,
		RecordBodyByteLen::new(values.len() as u64),
		BufferValueCount::new(4096),
	)
	.unwrap();

	assert_eq!(decoded, values);
	assert!(encoded.bytes.len() < values.len());
	assert_eq!(
		encoded.page_end_offsets.last().unwrap().get() as usize,
		encoded.bytes.len()
	);
}

#[test]
fn bitpacked_cascade_roundtrips_fixed_width_bounded_values() {
	let values = u64_bytes((0..4096).map(|value| (value * 17) % 1024));
	let cascade = EncodingCascade::BitPacked {
		bits: BitsPerValue::new(10),
		base: BaseValue::ZERO,
	};
	let encoded = encode_buffer(
		&values,
		&DataType::UInt64,
		cascade.clone(),
		BufferValueCount::new(4096),
		SimdPageValueCount::new(4096),
	)
	.unwrap();
	let decoded = decode_buffer(
		&encoded.bytes,
		&DataType::UInt64,
		&cascade,
		RecordBodyByteLen::new(values.len() as u64),
		BufferValueCount::new(4096),
	)
	.unwrap();

	assert_eq!(decoded, values);
	assert_eq!(encoded.bytes.len(), 5120);
	assert!(encoded.bytes.len() < values.len());
}

#[test]
fn dictionary_cascade_bitpacks_fixed_width_indices() {
	let values = u64_bytes((0..4096).map(|value| value % 4));
	let cascade = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(4 * 8),
		dict_value_count: wendelon::schema::DictValueCount::new(4),
		inner: Box::new(
			wendelon::storage::encoding::EncodingLeafCascade::BitPacked {
				bits: BitsPerValue::new(2),
				base: BaseValue::ZERO,
			},
		),
	};
	let encoded = encode_buffer(
		&values,
		&DataType::UInt64,
		cascade.clone(),
		BufferValueCount::new(4096),
		SimdPageValueCount::new(4096),
	)
	.unwrap();
	let decoded = decode_buffer(
		&encoded.bytes,
		&DataType::UInt64,
		&cascade,
		RecordBodyByteLen::new(values.len() as u64),
		BufferValueCount::new(4096),
	)
	.unwrap();

	assert_eq!(decoded, values);
	assert!(encoded.bytes.starts_with(b"WDB2"));
	assert!(encoded.bytes.len() < 1200);
}

#[test]
fn dictionary_cascade_roundtrips_variable_width_low_cardinality_values() {
	let values = ["us", "eu-west", "apac", "us", "us", "eu-west", "apac", "us"];
	let mut input = Vec::new();
	let mut offsets = Vec::with_capacity(values.len() + 1);
	offsets.push(0_u64);
	for value in values {
		input.extend_from_slice(value.as_bytes());
		offsets.push(input.len() as u64);
	}
	let cascade = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(input.len() as u64),
		dict_value_count: wendelon::schema::DictValueCount::new(3),
		inner: Box::new(wendelon::storage::encoding::EncodingLeafCascade::Plain),
	};
	let encoded = encode_buffer_with_value_offsets(
		&input,
		&DataType::Utf8,
		cascade.clone(),
		BufferValueCount::new((offsets.len() - 1) as u32),
		SimdPageValueCount::new(1024),
		Some(&offsets),
	)
	.unwrap();
	let decoded = decode_buffer(
		&encoded.bytes,
		&DataType::Utf8,
		&cascade,
		RecordBodyByteLen::new(input.len() as u64),
		BufferValueCount::new((offsets.len() - 1) as u32),
	)
	.unwrap();

	assert_eq!(decoded, input);
	assert!(encoded.bytes.starts_with(b"WDV1"));
	assert_eq!(
		encoded.page_end_offsets.last().unwrap().get() as usize,
		encoded.bytes.len()
	);
}

#[test]
fn dictionary_cascade_bitpacks_variable_width_indices() {
	let values = ["us", "eu-west", "apac", "us", "us", "eu-west", "apac", "us"];
	let mut input = Vec::new();
	let mut offsets = Vec::with_capacity(values.len() + 1);
	offsets.push(0_u64);
	for value in values {
		input.extend_from_slice(value.as_bytes());
		offsets.push(input.len() as u64);
	}
	let cascade = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(input.len() as u64),
		dict_value_count: wendelon::schema::DictValueCount::new(3),
		inner: Box::new(
			wendelon::storage::encoding::EncodingLeafCascade::BitPacked {
				bits: BitsPerValue::new(2),
				base: BaseValue::ZERO,
			},
		),
	};
	let encoded = encode_buffer_with_value_offsets(
		&input,
		&DataType::Utf8,
		cascade.clone(),
		BufferValueCount::new((offsets.len() - 1) as u32),
		SimdPageValueCount::new(1024),
		Some(&offsets),
	)
	.unwrap();
	let decoded = decode_buffer(
		&encoded.bytes,
		&DataType::Utf8,
		&cascade,
		RecordBodyByteLen::new(input.len() as u64),
		BufferValueCount::new((offsets.len() - 1) as u32),
	)
	.unwrap();

	assert_eq!(decoded, input);
	assert!(encoded.bytes.starts_with(b"WVB2"));
}

#[test]
fn fsst_cascade_uses_symbol_table_codec_for_repeated_string_bytes() {
	let input = "segment-segment-segment-".repeat(512).into_bytes();
	let cascade = EncodingCascade::Fsst {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_buffer(
		&input,
		&DataType::Utf8,
		cascade.clone(),
		BufferValueCount::new(512),
		SimdPageValueCount::new(512),
	)
	.unwrap();
	let decoded = decode_buffer(
		&encoded.bytes,
		&DataType::Utf8,
		&cascade,
		RecordBodyByteLen::new(input.len() as u64),
		BufferValueCount::new(512),
	)
	.unwrap();

	assert_eq!(decoded, input);
	assert!(encoded.bytes.starts_with(b"WFS1"));
	assert!(encoded.bytes.len() < input.len());
}

#[test]
fn alp_cascade_roundtrips_float_common_exponent_bytes() {
	let mut input = Vec::new();
	for row in 0..4096 {
		input.extend_from_slice(&(1.0_f64 + (row % 1024) as f64 / 8192.0).to_le_bytes());
	}
	let cascade = EncodingCascade::Alp {
		exponent: wendelon::schema::Exponent::new(1023),
		factor: wendelon::schema::Factor::new(1),
		exception_count: wendelon::schema::ExceptionCount::ZERO,
		inner: Box::new(wendelon::storage::encoding::EncodingLeafCascade::Plain),
	};
	let encoded = encode_buffer(
		&input,
		&DataType::Float64,
		cascade.clone(),
		BufferValueCount::new(4096),
		SimdPageValueCount::new(4096),
	)
	.unwrap();
	let decoded = decode_buffer(
		&encoded.bytes,
		&DataType::Float64,
		&cascade,
		RecordBodyByteLen::new(input.len() as u64),
		BufferValueCount::new(4096),
	)
	.unwrap();

	assert_eq!(decoded, input);
	assert!(encoded.bytes.starts_with(b"WAL1"));
	assert!(encoded.bytes.len() < input.len());
}
