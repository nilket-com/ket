use std::collections::BTreeSet;

use arrow::datatypes::DataType;
use wendelon::schema::{BufferValueCount, RecordBodyByteLen, SampleRowCount};
use wendelon::storage::encoding::sample::sample_buffer;
use wendelon::storage::encoding::select::select_and_encode_buffer;
use wendelon::storage::{EncodingCascade, EncodingCascadeKind, EncodingWriterConfig};

#[test]
fn selection_chooses_compacting_cascade_when_enabled() {
	let input = (0..4096).map(|index| (index % 8) as u8).collect::<Vec<_>>();
	let config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: BTreeSet::from([
			EncodingCascadeKind::Plain,
			EncodingCascadeKind::BitPacked,
		]),
		..EncodingWriterConfig::default()
	};

	let encoded = select_and_encode_buffer(
		&input,
		&DataType::UInt8,
		BufferValueCount::new(input.len() as u32),
		&config,
	)
	.unwrap();

	assert!(matches!(encoded.cascade, EncodingCascade::BitPacked { .. }));
	assert!(encoded.bytes.len() < input.len());
	assert_eq!(
		encoded.page_end_offsets.last().unwrap().get() as usize,
		encoded.bytes.len()
	);
}

#[test]
fn selection_respects_disabled_writer_and_cascade_allow_list() {
	let input = vec![3_u8; 4096];
	let disabled = EncodingWriterConfig {
		enabled: false,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		..EncodingWriterConfig::default()
	};
	let encoded = select_and_encode_buffer(
		&input,
		&DataType::UInt8,
		BufferValueCount::new(input.len() as u32),
		&disabled,
	)
	.unwrap();
	assert!(matches!(encoded.cascade, EncodingCascade::Plain));

	let plain_only = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: BTreeSet::from([EncodingCascadeKind::Plain]),
		..EncodingWriterConfig::default()
	};
	let encoded = select_and_encode_buffer(
		&input,
		&DataType::UInt8,
		BufferValueCount::new(input.len() as u32),
		&plain_only,
	)
	.unwrap();
	assert!(matches!(encoded.cascade, EncodingCascade::Plain));
}

#[test]
fn selection_chooses_delta_for_monotonic_fixed_width_values() {
	let mut input = Vec::new();
	for value in 0..4096_u64 {
		input.extend_from_slice(&(1_000_000_u64 + value).to_le_bytes());
	}
	let config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: BTreeSet::from([EncodingCascadeKind::Plain, EncodingCascadeKind::Delta]),
		..EncodingWriterConfig::default()
	};

	let encoded = select_and_encode_buffer(
		&input,
		&DataType::UInt64,
		BufferValueCount::new(4096),
		&config,
	)
	.unwrap();

	assert!(matches!(encoded.cascade, EncodingCascade::Delta { .. }));
	assert!(encoded.bytes.len() < input.len());
}

#[test]
fn selection_chooses_bitpacked_for_bounded_fixed_width_values() {
	let mut input = Vec::new();
	for value in 0..4096_u64 {
		input.extend_from_slice(&((value * 17) % 1024).to_le_bytes());
	}
	let config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: BTreeSet::from([
			EncodingCascadeKind::Plain,
			EncodingCascadeKind::BitPacked,
		]),
		..EncodingWriterConfig::default()
	};

	let encoded = select_and_encode_buffer(
		&input,
		&DataType::UInt64,
		BufferValueCount::new(4096),
		&config,
	)
	.unwrap();

	assert!(matches!(encoded.cascade, EncodingCascade::BitPacked { .. }));
	assert!(encoded.bytes.len() < input.len());
}

#[test]
fn selection_chooses_dictionary_for_low_cardinality_fixed_width_values() {
	let mut input = Vec::new();
	for value in 0..4096_u64 {
		input.extend_from_slice(&(value % 4).to_le_bytes());
	}
	let config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: BTreeSet::from([
			EncodingCascadeKind::Plain,
			EncodingCascadeKind::Dictionary,
		]),
		..EncodingWriterConfig::default()
	};

	let encoded = select_and_encode_buffer(
		&input,
		&DataType::UInt64,
		BufferValueCount::new(4096),
		&config,
	)
	.unwrap();

	assert!(matches!(
		encoded.cascade,
		EncodingCascade::Dictionary { .. }
	));
	assert!(encoded.bytes.len() < input.len());
}

#[test]
fn selection_chooses_plain_for_random_fixed_width_values_when_plain_allowed() {
	let mut input = Vec::new();
	let mut state = 0x9e37_79b9_7f4a_7c15_u64;
	for _ in 0..4096 {
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		input.extend_from_slice(&state.to_le_bytes());
	}
	let config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: BTreeSet::from([
			EncodingCascadeKind::Plain,
			EncodingCascadeKind::Lz4Frame,
			EncodingCascadeKind::BitPacked,
			EncodingCascadeKind::Dictionary,
			EncodingCascadeKind::Delta,
		]),
		..EncodingWriterConfig::default()
	};

	let encoded = select_and_encode_buffer(
		&input,
		&DataType::UInt64,
		BufferValueCount::new(4096),
		&config,
	)
	.unwrap();

	assert!(matches!(encoded.cascade, EncodingCascade::Plain));
}

#[test]
fn selection_chooses_lz4_for_random_fixed_width_values_when_plain_disabled() {
	let mut input = Vec::new();
	let mut state = 0x9e37_79b9_7f4a_7c15_u64;
	for _ in 0..4096 {
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		input.extend_from_slice(&state.to_le_bytes());
	}
	let config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: BTreeSet::from([
			EncodingCascadeKind::Lz4Frame,
			EncodingCascadeKind::BitPacked,
			EncodingCascadeKind::Dictionary,
			EncodingCascadeKind::Delta,
		]),
		..EncodingWriterConfig::default()
	};

	let encoded = select_and_encode_buffer(
		&input,
		&DataType::UInt64,
		BufferValueCount::new(4096),
		&config,
	)
	.unwrap();

	assert!(matches!(encoded.cascade, EncodingCascade::Lz4Frame));
}

#[test]
fn selection_uses_deterministic_sample_row_budget() {
	let mut input = Vec::new();
	for value in 0..4096_u64 {
		let sampled_value = if value == 4095 { 1 } else { 1_000_000 + value };
		input.extend_from_slice(&sampled_value.to_le_bytes());
	}
	let config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		cascade_sample_rows: SampleRowCount::new(2),
		enabled_cascades: BTreeSet::from([
			EncodingCascadeKind::Plain,
			EncodingCascadeKind::Delta,
			EncodingCascadeKind::Lz4Frame,
		]),
		..EncodingWriterConfig::default()
	};

	let encoded = select_and_encode_buffer(
		&input,
		&DataType::UInt64,
		BufferValueCount::new(4096),
		&config,
	)
	.unwrap();

	assert!(matches!(encoded.cascade, EncodingCascade::Delta { .. }));

	let full_sample = sample_buffer(
		&input,
		&DataType::UInt64,
		BufferValueCount::new(4096),
		SampleRowCount::new(4096),
	);
	assert!(!full_sample.is_monotonic);
}
