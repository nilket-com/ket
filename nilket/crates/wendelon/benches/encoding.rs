//! ADR 057 Encoding benchmark harness.
//!
//! `cargo bench --bench encoding -- --quick`.

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeSet;
use std::hint::black_box;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, Instant};
use wendelon::schema::{
	BaseValue, BitsPerValue, BufferValueCount, DictValueCount, ExceptionCount, Exponent, Factor,
	HybridTimestamp, Identity, Locality, LocalityPolicy, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptorV1, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, RowCount, RowGroupRowCount, RowIdentityPolicy, RunCount, SampleRowCount,
	SchemaEvolutionPolicyV1, SimdPageValueCount, SlabId, TableObjectId,
};
use wendelon::storage::encoding::cascade::{
	decode_buffer, decode_page, encode_buffer, encode_buffer_with_value_offsets,
};
use wendelon::storage::encoding::page::expand_page_descriptors;
use wendelon::storage::encoding::select::{
	select_and_encode_buffer, select_and_encode_buffer_with_value_offsets,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	ENCODING_DIRECTORY_METADATA_KEY, EncodedMultiRowFrame, EncodingCascade, EncodingCascadeKind,
	EncodingLeafCascade, EncodingWriterConfig, PlaintextByteRange,
	ROW_GROUP_DIRECTORY_METADATA_KEY, RowGroupWriterConfig, TieredRecordId, decode_batch,
	encode_multirow_from_survivors_with_row_group_and_encoding_config,
	parse_record_buffer_directory,
};

fn main() -> std::io::Result<()> {
	let config = BenchConfig::from_args();
	for case in config.cases.iter().copied() {
		let batch = bench_batch(case.rows, case.columns)?;
		let descriptor = descriptor(batch.schema().as_ref(), case.columns)?;
		bench_writer(&config, case, &batch, &descriptor)?;
		bench_materialize(&config, case, &batch, &descriptor)?;
		bench_directory_size(case, &batch, &descriptor)?;
		bench_point_read_and_selective_scan(&config, case, &batch, &descriptor)?;
		bench_determinism(case, &batch, &descriptor)?;
		bench_mixed_slab_read(&config, case, &batch, &descriptor)?;
	}
	bench_cascades(&config)?;
	bench_selection_cpu(&config)?;
	bench_enabled_cascade_ablation(&config)?;
	bench_simd_page_tuning(&config)?;
	bench_selection_edge_cases(&config)?;
	bench_l2_get_rate_cost();
	bench_csb_short_circuit();
	Ok(())
}

#[derive(Clone, Copy, Debug)]
struct BenchCase {
	rows: usize,
	columns: usize,
}

#[derive(Clone, Debug)]
struct BenchConfig {
	cases: Vec<BenchCase>,
	iterations: usize,
}

impl BenchConfig {
	fn from_args() -> Self {
		let quick = std::env::args().any(|arg| arg == "--quick");
		if quick {
			Self {
				cases: vec![BenchCase {
					rows: 8_192,
					columns: 8,
				}],
				iterations: 20,
			}
		} else {
			Self {
				cases: vec![
					BenchCase {
						rows: 8_192,
						columns: 8,
					},
					BenchCase {
						rows: 16_384,
						columns: 8,
					},
					BenchCase {
						rows: 65_536,
						columns: 4,
					},
				],
				iterations: 75,
			}
		}
	}
}

fn descriptor(
	schema: &Schema,
	columns: usize,
) -> std::io::Result<wendelon::schema::TableSchemaDescriptor> {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x57; 16]),
		1,
		"bench",
		"encoding",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("c0")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(4_096, 4_096, 8 * 1024 * 1024).map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("bench multi-row layout rejected: {err}"),
				)
			})?,
		),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("bench descriptor rejected for {columns} columns: {err}"),
		)
	})
}

fn bench_batch(rows: usize, columns: usize) -> std::io::Result<RecordBatch> {
	let fields = (0..columns)
		.map(|column| Field::new(format!("c{column}"), column_type(column), false))
		.collect::<Vec<_>>();
	let arrays = (0..columns)
		.map(|column| bench_array(rows, column))
		.collect::<Vec<_>>();
	RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("bench batch: {err}")))
}

fn column_type(column: usize) -> DataType {
	match column % 4 {
		0 | 1 => DataType::Int64,
		2 => DataType::Utf8,
		_ => DataType::Float64,
	}
}

fn bench_array(rows: usize, column: usize) -> ArrayRef {
	match column_type(column) {
		DataType::Int64 => {
			let values = (0..rows)
				.map(|row| {
					if column.is_multiple_of(4) {
						row as i64
					} else {
						(row % 16) as i64
					}
				})
				.collect::<Vec<_>>();
			Arc::new(Int64Array::from(values))
		}
		DataType::Utf8 => {
			let values = (0..rows)
				.map(|row| format!("segment-{}", row % 64))
				.collect::<Vec<_>>();
			Arc::new(StringArray::from(values))
		}
		DataType::Float64 => {
			let values = (0..rows)
				.map(|row| ((row % 1024) as f64) * 0.125)
				.collect::<Vec<_>>();
			Arc::new(Float64Array::from(values))
		}
		_ => unreachable!("bench column types are fixed"),
	}
}

fn source_rows(row_count: usize) -> Vec<MultiRowSourceRow> {
	(0..row_count)
		.map(|row| MultiRowSourceRow {
			source_row_index: row,
			key_identity: Identity::new([row as u8; 32]),
			routing_locality: Locality::new(7),
			row_hlc: HybridTimestamp {
				physical_ms: 1,
				logical: row as u32,
				node_id: 1,
			},
			is_tombstone: false,
			table_object_id: Some(TableObjectId::new([0x57; 16])),
		})
		.collect()
}

fn row_group_config() -> RowGroupWriterConfig {
	RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(4_096),
		RowGroupRowCount::new(4_096),
		RowCount::new(8_192),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true)
}

fn encoding_config(enabled: bool) -> EncodingWriterConfig {
	EncodingWriterConfig {
		enabled,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		..EncodingWriterConfig::default()
	}
}

fn encoding_config_with(
	enabled_cascades: BTreeSet<EncodingCascadeKind>,
	page_values: u32,
) -> EncodingWriterConfig {
	EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: SimdPageValueCount::new(page_values),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades,
		..EncodingWriterConfig::default()
	}
}

fn encode_case(
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptor,
	encoding_config: &EncodingWriterConfig,
) -> std::io::Result<EncodedMultiRowFrame> {
	encode_multirow_from_survivors_with_row_group_and_encoding_config(
		batch,
		&source_rows(case.rows),
		descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(),
		encoding_config,
	)
}

fn bench_writer(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptor,
) -> std::io::Result<()> {
	for (scenario, encoding) in [
		("writer_disabled", false),
		("writer_encoding_enabled", true),
	] {
		let started = Instant::now();
		let mut encoded_bytes = 0_u64;
		for _ in 0..config.iterations {
			let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
				black_box(batch),
				black_box(&source_rows(case.rows)),
				black_box(descriptor),
				RecordBodyCompressionPolicy::Uncompressed,
				black_box(&row_group_config()),
				black_box(&encoding_config(encoding)),
			)?;
			encoded_bytes = encoded.frame.len() as u64;
			black_box(encoded);
		}
		print_bench(
			"writer",
			scenario,
			case,
			config.iterations,
			started.elapsed(),
			encoded_bytes,
		);
	}
	Ok(())
}

fn bench_materialize(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptor,
) -> std::io::Result<()> {
	for (variant, encoding) in [
		("row_group_only", encoding_config(false)),
		("encoding_enabled", encoding_config(true)),
	] {
		let encoded = encode_case(case, batch, descriptor, &encoding)?;
		let started = Instant::now();
		for _ in 0..config.iterations {
			let decoded = decode_batch(black_box(&encoded.frame)).map_err(|err| {
				Error::new(ErrorKind::InvalidData, format!("decode batch: {err}"))
			})?;
			black_box(decoded);
		}
		print_bench(
			"full_materialize",
			variant,
			case,
			config.iterations,
			started.elapsed(),
			encoded.frame.len() as u64,
		);
	}
	Ok(())
}

fn bench_directory_size(
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptor,
) -> std::io::Result<()> {
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		batch,
		&source_rows(case.rows),
		descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(),
		&encoding_config(true),
	)?;
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(57),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		descriptor,
	)
	.map_err(|err| Error::new(ErrorKind::InvalidData, format!("parse directory: {err}")))?;
	let encoding = parsed.encoding_directory.as_ref();
	let buffers = encoding
		.map(|directory| directory.buffers.len())
		.unwrap_or(0);
	let cascades = encoding
		.map(|directory| directory.cascades.len())
		.unwrap_or(0);
	let row_group_metadata_bytes = parsed
		.ipc_schema
		.metadata()
		.get(ROW_GROUP_DIRECTORY_METADATA_KEY)
		.map(|value| value.len())
		.unwrap_or(0);
	let encoding_metadata_bytes = parsed
		.ipc_schema
		.metadata()
		.get(ENCODING_DIRECTORY_METADATA_KEY)
		.map(|value| value.len())
		.unwrap_or(0);
	let metadata_bytes = row_group_metadata_bytes.saturating_add(encoding_metadata_bytes);
	println!(
		"encoding_measure scenario=directory_size rows={} columns={} record_bytes={} cascades={} buffers={}",
		case.rows,
		case.columns,
		encoded.frame.len(),
		cascades,
		buffers,
	);
	println!(
		"encoding_measure scenario=metadata_probe rows={} columns={} row_group_metadata_bytes={} encoding_metadata_bytes={} combined_metadata_bytes={} initial_probe_limit=65536 hard_cap=262144 fits_initial_probe={} fits_hard_cap={}",
		case.rows,
		case.columns,
		row_group_metadata_bytes,
		encoding_metadata_bytes,
		metadata_bytes,
		metadata_bytes <= 65_536,
		metadata_bytes <= 262_144,
	);
	Ok(())
}

fn bench_point_read_and_selective_scan(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptor,
) -> std::io::Result<()> {
	let encoded = encode_case(case, batch, descriptor, &encoding_config(true))?;
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(57),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		descriptor,
	)
	.map_err(|err| Error::new(ErrorKind::InvalidData, format!("parse directory: {err}")))?;
	let Some(page_directory) = parsed.page_directory.as_ref() else {
		return Ok(());
	};
	let Some(encoding_directory) = parsed.encoding_directory.as_ref() else {
		return Ok(());
	};
	let Some(encoding) = encoding_directory
		.buffers
		.iter()
		.find(|encoding| encoding.page_end_offsets.len() > 1)
	else {
		return Ok(());
	};
	let cascade = encoding_directory
		.cascades
		.get(encoding.cascade_index.get() as usize)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "cascade index out of range"))?;
	let page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == encoding.row_group_index
				&& page.ipc_column_ordinal == encoding.ipc_column_ordinal
				&& page.ipc_buffer_ordinal == encoding.ipc_buffer_ordinal
		})
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "encoded page range missing"))?;
	let field = parsed
		.ipc_schema
		.fields()
		.get(encoding.ipc_column_ordinal.get() as usize)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "encoded field missing"))?;
	let buffer_start = usize::try_from(page.range.start.get())
		.map_err(|_| Error::new(ErrorKind::InvalidData, "encoded buffer start exceeds usize"))?;
	let buffer_len = usize::try_from(page.range.len.get()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"encoded buffer length exceeds usize",
		)
	})?;
	let buffer_end = buffer_start
		.checked_add(buffer_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "encoded buffer range overflow"))?;
	let encoded_buffer = encoded
		.frame
		.get(buffer_start..buffer_end)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "encoded buffer outside frame"))?;
	let pages = expand_page_descriptors(encoding)
		.map_err(|code| Error::new(ErrorKind::InvalidData, format!("expand pages: {code:?}")))?;
	let bytes_per_value = usize::try_from(encoding.decoded_byte_len.get())
		.ok()
		.and_then(|bytes| {
			let values = encoding.decoded_value_count.get() as usize;
			(values > 0 && bytes % values == 0).then_some(bytes / values)
		})
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "page buffer is not fixed width"))?;

	let started = Instant::now();
	for _ in 0..config.iterations {
		let decoded = decode_buffer(
			black_box(encoded_buffer),
			field.data_type(),
			black_box(cascade),
			encoding.decoded_byte_len,
			encoding.decoded_value_count,
		)
		.map_err(|code| Error::new(ErrorKind::InvalidData, format!("decode buffer: {code:?}")))?;
		black_box(decoded);
	}
	print_bench(
		"point_read",
		"full_buffer_decode",
		case,
		config.iterations,
		started.elapsed(),
		encoding.encoded_range.len.get(),
	);

	let selected_page = pages
		.get(pages.len() / 2)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "no Encoding page descriptors"))?;
	let page_start = usize::try_from(selected_page.byte_range.start.get())
		.map_err(|_| Error::new(ErrorKind::InvalidData, "selected page start exceeds usize"))?;
	let page_len = usize::try_from(selected_page.byte_range.len.get())
		.map_err(|_| Error::new(ErrorKind::InvalidData, "selected page length exceeds usize"))?;
	let page_end = page_start
		.checked_add(page_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "selected page range overflow"))?;
	let page_bytes = encoded_buffer
		.get(page_start..page_end)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "selected page outside buffer"))?;
	let expected_page_bytes = usize::try_from(selected_page.value_count.get())
		.map_err(|_| Error::new(ErrorKind::InvalidData, "page value count exceeds usize"))?
		.checked_mul(bytes_per_value)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "page decoded length overflow"))?;
	let started = Instant::now();
	for _ in 0..config.iterations {
		let decoded = decode_page(
			black_box(page_bytes),
			field.data_type(),
			black_box(cascade),
			RecordBodyByteLen::new(expected_page_bytes as u64),
			selected_page.value_count,
		)
		.map_err(|code| Error::new(ErrorKind::InvalidData, format!("decode page: {code:?}")))?;
		black_box(decoded);
	}
	print_bench(
		"point_read",
		"simd_page_decode",
		case,
		config.iterations,
		started.elapsed(),
		page_len as u64,
	);
	println!(
		"encoding_measure scenario=point_read_bytes rows={} columns={} decoded_values_full={} decoded_values_page={} selected_l2_bytes_adr056={} selected_l2_bytes_encoding_full={} selected_l2_bytes_page={} byte_reduction_x_vs_adr056={:.2} byte_reduction_x_vs_encoding_full={:.2}",
		case.rows,
		case.columns,
		encoding.decoded_value_count.get(),
		selected_page.value_count.get(),
		encoding.decoded_byte_len.get(),
		encoding.encoded_range.len.get(),
		page_len,
		encoding.decoded_byte_len.get() as f64 / page_len.max(1) as f64,
		encoding.encoded_range.len.get() as f64 / page_len.max(1) as f64,
	);

	for selectivity in [1_usize, 5, 25, 50] {
		let page_count = pages
			.len()
			.saturating_mul(selectivity)
			.div_ceil(100)
			.max(1)
			.min(pages.len());
		let started = Instant::now();
		let mut selected_bytes = 0_u64;
		for _ in 0..config.iterations {
			for selected_page in pages.iter().take(page_count) {
				let page_start =
					usize::try_from(selected_page.byte_range.start.get()).map_err(|_| {
						Error::new(ErrorKind::InvalidData, "selected page start exceeds usize")
					})?;
				let page_len =
					usize::try_from(selected_page.byte_range.len.get()).map_err(|_| {
						Error::new(ErrorKind::InvalidData, "selected page length exceeds usize")
					})?;
				let page_end = page_start.checked_add(page_len).ok_or_else(|| {
					Error::new(ErrorKind::InvalidData, "selected page range overflow")
				})?;
				let expected_page_bytes = usize::try_from(selected_page.value_count.get())
					.map_err(|_| {
						Error::new(ErrorKind::InvalidData, "page value count exceeds usize")
					})?
					.checked_mul(bytes_per_value)
					.ok_or_else(|| Error::new(ErrorKind::InvalidData, "page decoded overflow"))?;
				let decoded = decode_page(
					black_box(&encoded_buffer[page_start..page_end]),
					field.data_type(),
					black_box(cascade),
					RecordBodyByteLen::new(expected_page_bytes as u64),
					selected_page.value_count,
				)
				.map_err(|code| {
					Error::new(ErrorKind::InvalidData, format!("decode page: {code:?}"))
				})?;
				black_box(decoded);
			}
		}
		for selected_page in pages.iter().take(page_count) {
			selected_bytes = selected_bytes.saturating_add(selected_page.byte_range.len.get());
		}
		print_bench(
			"selective_scan",
			&format!("pages_{selectivity}pct"),
			case,
			config.iterations,
			started.elapsed(),
			selected_bytes,
		);
	}
	Ok(())
}

fn bench_determinism(
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptor,
) -> std::io::Result<()> {
	let left = encode_case(case, batch, descriptor, &encoding_config(true))?;
	let right = encode_case(case, batch, descriptor, &encoding_config(true))?;
	let equal = left.frame == right.frame;
	println!(
		"encoding_measure scenario=determinism rows={} columns={} byte_identical={} bytes={}",
		case.rows,
		case.columns,
		equal,
		left.frame.len(),
	);
	if !equal {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"Encoding deterministic writer output mismatch",
		));
	}
	Ok(())
}

fn bench_mixed_slab_read(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptor,
) -> std::io::Result<()> {
	let row_group_only = encode_case(case, batch, descriptor, &encoding_config(false))?;
	let encoding = encode_case(case, batch, descriptor, &encoding_config(true))?;
	let started = Instant::now();
	for iteration in 0..config.iterations {
		let frame = if iteration % 2 == 0 {
			&row_group_only.frame
		} else {
			&encoding.frame
		};
		let decoded = decode_batch(black_box(frame))
			.map_err(|err| Error::new(ErrorKind::InvalidData, format!("decode batch: {err}")))?;
		black_box(decoded);
	}
	print_bench(
		"mixed_slab_read",
		"pre057_post057_alternating",
		case,
		config.iterations,
		started.elapsed(),
		row_group_only.frame.len() as u64 + encoding.frame.len() as u64,
	);
	Ok(())
}

fn bench_cascades(config: &BenchConfig) -> std::io::Result<()> {
	let repeated = vec![7_u8; 4096];
	let bounded = (0..4096).map(|index| (index % 8) as u8).collect::<Vec<_>>();
	let mixed = (0..4096)
		.map(|index| (index % 251) as u8)
		.collect::<Vec<_>>();
	let fsst_input = "segment-segment-segment-".repeat(256).into_bytes();
	let mut alp_input = Vec::new();
	for row in 0..4096 {
		alp_input.extend_from_slice(&(1.0_f64 + (row % 1024) as f64 / 8192.0).to_le_bytes());
	}
	for (name, cascade, input, data_type, value_count) in [
		(
			"plain",
			EncodingCascade::Plain,
			mixed.as_slice(),
			DataType::UInt8,
			mixed.len(),
		),
		(
			"lz4_frame",
			EncodingCascade::Lz4Frame,
			mixed.as_slice(),
			DataType::UInt8,
			mixed.len(),
		),
		(
			"bit_packed",
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(3),
				base: BaseValue::ZERO,
			},
			bounded.as_slice(),
			DataType::UInt8,
			bounded.len(),
		),
		(
			"rle",
			EncodingCascade::Rle {
				run_count: RunCount::new(1),
				inner: Box::new(EncodingLeafCascade::Plain),
			},
			repeated.as_slice(),
			DataType::UInt8,
			repeated.len(),
		),
		(
			"dictionary",
			EncodingCascade::Dictionary {
				dict_byte_len: RecordBodyByteLen::new(8),
				dict_value_count: DictValueCount::new(8),
				inner: Box::new(EncodingLeafCascade::BitPacked {
					bits: BitsPerValue::new(3),
					base: BaseValue::ZERO,
				}),
			},
			bounded.as_slice(),
			DataType::UInt8,
			bounded.len(),
		),
		(
			"delta",
			EncodingCascade::Delta {
				base: BaseValue::ZERO,
				inner: Box::new(EncodingLeafCascade::Plain),
			},
			mixed.as_slice(),
			DataType::UInt8,
			mixed.len(),
		),
		(
			"fsst_whole_buffer",
			EncodingCascade::FsstWholeBuffer {
				symbol_table_byte_len: RecordBodyByteLen::ZERO,
			},
			fsst_input.as_slice(),
			DataType::Utf8,
			fsst_input.len(),
		),
		(
			"alp",
			EncodingCascade::Alp {
				exponent: Exponent::new(1023),
				factor: Factor::new(1),
				exception_count: ExceptionCount::ZERO,
				inner: Box::new(EncodingLeafCascade::Plain),
			},
			alp_input.as_slice(),
			DataType::Float64,
			4096,
		),
	] {
		let encode_started = Instant::now();
		let mut encoded_len = 0_u64;
		for _ in 0..config.iterations {
			let encoded = encode_buffer(
				black_box(input),
				&data_type,
				black_box(cascade.clone()),
				BufferValueCount::new(value_count as u32),
				SimdPageValueCount::new(value_count as u32),
			)?;
			encoded_len = encoded.bytes.len() as u64;
			black_box(encoded);
		}
		print_bench(
			"cascade_encode",
			name,
			BenchCase {
				rows: value_count,
				columns: 1,
			},
			config.iterations,
			encode_started.elapsed(),
			encoded_len,
		);

		let encoded = encode_buffer(
			input,
			&data_type,
			cascade.clone(),
			BufferValueCount::new(value_count as u32),
			SimdPageValueCount::new(value_count as u32),
		)?;
		let decode_started = Instant::now();
		for _ in 0..config.iterations {
			let decoded = decode_buffer(
				black_box(&encoded.bytes),
				&data_type,
				black_box(&cascade),
				RecordBodyByteLen::new(input.len() as u64),
				BufferValueCount::new(value_count as u32),
			)
			.map_err(|code| Error::new(ErrorKind::InvalidData, format!("decode: {code:?}")))?;
			black_box(decoded);
		}
		print_bench(
			"cascade_decode",
			name,
			BenchCase {
				rows: value_count,
				columns: 1,
			},
			config.iterations,
			decode_started.elapsed(),
			encoded.bytes.len() as u64,
		);
		let lz4 = encode_buffer(
			input,
			&data_type,
			EncodingCascade::Lz4Frame,
			BufferValueCount::new(value_count as u32),
			SimdPageValueCount::new(value_count as u32),
		)?;
		println!(
			"encoding_measure scenario=cascade_compression_ratio variant={name} input_bytes={} encoded_bytes={} lz4_bytes={} ratio_vs_lz4={:.3}",
			input.len(),
			encoded.bytes.len(),
			lz4.bytes.len(),
			lz4.bytes.len() as f64 / encoded.bytes.len().max(1) as f64,
		);
	}
	bench_variable_width_dictionary(config)?;
	bench_phase10_compression_ratios()?;
	Ok(())
}

fn bench_phase10_compression_ratios() -> std::io::Result<()> {
	let int64 = fixed_width_u64_bytes((0..4096).map(|row| bounded_pseudorandom(row) & 0x0fff));
	bench_phase10_fixed_compression_ratio(
		"int64_bit_packed",
		&int64,
		DataType::Int64,
		4096,
		EncodingCascade::BitPacked {
			bits: BitsPerValue::new(12),
			base: BaseValue::ZERO,
		},
	)?;

	let mut timestamp_value = 1_700_000_000_000_000_u64;
	let timestamp = fixed_width_u64_bytes((0..4096).map(|row| {
		timestamp_value = timestamp_value.saturating_add(1 + (bounded_pseudorandom(row) & 0x7e));
		timestamp_value
	}));
	bench_phase10_fixed_compression_ratio(
		"timestamp_delta",
		&timestamp,
		DataType::Timestamp(TimeUnit::Microsecond, None),
		4096,
		EncodingCascade::Delta {
			base: BaseValue::new(1_700_000_000_000_000),
			inner: Box::new(EncodingLeafCascade::Plain),
		},
	)?;

	let high_card_text = high_cardinality_text_bytes(4096);
	bench_phase10_fixed_compression_ratio(
		"utf8_fsst_high_cardinality",
		&high_card_text,
		DataType::Utf8,
		high_card_text.len(),
		EncodingCascade::FsstWholeBuffer {
			symbol_table_byte_len: RecordBodyByteLen::ZERO,
		},
	)?;

	let alp = fixed_width_u64_bytes((0..4096).map(|row| {
		let low = bounded_pseudorandom(row) as u32;
		0x3ff0_0000_0000_0000_u64 | u64::from(low)
	}));
	bench_phase10_fixed_compression_ratio(
		"float64_alp",
		&alp,
		DataType::Float64,
		4096,
		EncodingCascade::Alp {
			exponent: Exponent::new(1023),
			factor: Factor::new(1),
			exception_count: ExceptionCount::ZERO,
			inner: Box::new(EncodingLeafCascade::Plain),
		},
	)?;

	let (dict_input, dict_offsets, dict_bits) = low_cardinality_utf8_bytes(4096);
	let dict_cascade = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(dict_input.len() as u64),
		dict_value_count: DictValueCount::new(64),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(dict_bits),
			base: BaseValue::ZERO,
		}),
	};
	let encoded = encode_buffer_with_value_offsets(
		&dict_input,
		&DataType::Utf8,
		dict_cascade,
		BufferValueCount::new((dict_offsets.len() - 1) as u32),
		SimdPageValueCount::new((dict_offsets.len() - 1) as u32),
		Some(&dict_offsets),
	)?;
	let lz4 = encode_buffer(
		&dict_input,
		&DataType::Utf8,
		EncodingCascade::Lz4Frame,
		BufferValueCount::new((dict_offsets.len() - 1) as u32),
		SimdPageValueCount::new((dict_offsets.len() - 1) as u32),
	)?;
	println!(
		"encoding_measure scenario=phase10_compression_ratio variant=utf8_dictionary_low_cardinality input_bytes={} encoded_bytes={} lz4_bytes={} ratio_vs_lz4={:.3}",
		dict_input.len(),
		encoded.bytes.len(),
		lz4.bytes.len(),
		lz4.bytes.len() as f64 / encoded.bytes.len().max(1) as f64,
	);
	Ok(())
}

fn bench_phase10_fixed_compression_ratio(
	variant: &str,
	input: &[u8],
	data_type: DataType,
	value_count: usize,
	cascade: EncodingCascade,
) -> std::io::Result<()> {
	let encoded = encode_buffer(
		input,
		&data_type,
		cascade,
		BufferValueCount::new(value_count as u32),
		SimdPageValueCount::new(value_count as u32),
	)?;
	let lz4 = encode_buffer(
		input,
		&data_type,
		EncodingCascade::Lz4Frame,
		BufferValueCount::new(value_count as u32),
		SimdPageValueCount::new(value_count as u32),
	)?;
	println!(
		"encoding_measure scenario=phase10_compression_ratio variant={variant} input_bytes={} encoded_bytes={} lz4_bytes={} ratio_vs_lz4={:.3}",
		input.len(),
		encoded.bytes.len(),
		lz4.bytes.len(),
		lz4.bytes.len() as f64 / encoded.bytes.len().max(1) as f64,
	);
	Ok(())
}

fn fixed_width_u64_bytes(values: impl IntoIterator<Item = u64>) -> Vec<u8> {
	let mut out = Vec::new();
	for value in values {
		out.extend_from_slice(&value.to_le_bytes());
	}
	out
}

fn bounded_pseudorandom(row: usize) -> u64 {
	let mut value = (row as u64)
		.wrapping_mul(0x9e37_79b9_7f4a_7c15)
		.wrapping_add(0xbf58_476d_1ce4_e5b9);
	value ^= value >> 30;
	value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
	value ^= value >> 27;
	value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
	value ^ (value >> 31)
}

fn low_cardinality_utf8_bytes(rows: usize) -> (Vec<u8>, Vec<u64>, u8) {
	let dictionary = (0..64)
		.map(|index| format!("dim-{index:02x}-{:016x}", bounded_pseudorandom(index)))
		.collect::<Vec<_>>();
	let mut input = Vec::new();
	let mut offsets = Vec::with_capacity(rows + 1);
	offsets.push(0);
	for row in 0..rows {
		let index = (bounded_pseudorandom(row) as usize) & 63;
		input.extend_from_slice(dictionary[index].as_bytes());
		offsets.push(input.len() as u64);
	}
	(input, offsets, 6)
}

fn high_cardinality_text_bytes(rows: usize) -> Vec<u8> {
	const PAIRS: [&str; 32] = [
		"th", "he", "in", "er", "an", "re", "on", "at", "en", "nd", "ti", "es", "or", "te", "of",
		"ed", "is", "it", "al", "ar", "st", "to", "nt", "ng", "se", "ha", "as", "ou", "io", "le",
		"ve", "co",
	];
	let mut out = Vec::with_capacity(rows * 48);
	for row in 0..rows {
		let mut state = bounded_pseudorandom(row);
		for _ in 0..24 {
			let pair = PAIRS[(state as usize) & 31];
			out.extend_from_slice(pair.as_bytes());
			state = state.rotate_left(7) ^ 0x517c_c1b7_2722_0a95;
		}
	}
	out
}

fn bench_variable_width_dictionary(config: &BenchConfig) -> std::io::Result<()> {
	let values = (0..4096)
		.map(|row| match row % 4 {
			0 => "us",
			1 => "eu-west",
			2 => "apac",
			_ => "us",
		})
		.collect::<Vec<_>>();
	let mut input = Vec::new();
	let mut offsets = Vec::with_capacity(values.len() + 1);
	offsets.push(0_u64);
	for value in &values {
		input.extend_from_slice(value.as_bytes());
		offsets.push(input.len() as u64);
	}
	let cascade = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(input.len() as u64),
		dict_value_count: DictValueCount::new(3),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(2),
			base: BaseValue::ZERO,
		}),
	};
	let value_count = values.len();
	let encode_started = Instant::now();
	let mut encoded_len = 0_u64;
	for _ in 0..config.iterations {
		let encoded = encode_buffer_with_value_offsets(
			black_box(&input),
			&DataType::Utf8,
			black_box(cascade.clone()),
			BufferValueCount::new(value_count as u32),
			SimdPageValueCount::new(value_count as u32),
			Some(black_box(&offsets)),
		)?;
		encoded_len = encoded.bytes.len() as u64;
		black_box(encoded);
	}
	print_bench(
		"cascade_encode",
		"dictionary_utf8",
		BenchCase {
			rows: value_count,
			columns: 1,
		},
		config.iterations,
		encode_started.elapsed(),
		encoded_len,
	);

	let encoded = encode_buffer_with_value_offsets(
		&input,
		&DataType::Utf8,
		cascade.clone(),
		BufferValueCount::new(value_count as u32),
		SimdPageValueCount::new(value_count as u32),
		Some(&offsets),
	)?;
	let decode_started = Instant::now();
	for _ in 0..config.iterations {
		let decoded = decode_buffer(
			black_box(&encoded.bytes),
			&DataType::Utf8,
			black_box(&cascade),
			RecordBodyByteLen::new(input.len() as u64),
			BufferValueCount::new(value_count as u32),
		)
		.map_err(|code| Error::new(ErrorKind::InvalidData, format!("decode: {code:?}")))?;
		black_box(decoded);
	}
	print_bench(
		"cascade_decode",
		"dictionary_utf8",
		BenchCase {
			rows: value_count,
			columns: 1,
		},
		config.iterations,
		decode_started.elapsed(),
		encoded.bytes.len() as u64,
	);
	let lz4 = encode_buffer(
		&input,
		&DataType::Utf8,
		EncodingCascade::Lz4Frame,
		BufferValueCount::new(value_count as u32),
		SimdPageValueCount::new(value_count as u32),
	)?;
	println!(
		"encoding_measure scenario=cascade_compression_ratio variant=dictionary_utf8 input_bytes={} encoded_bytes={} lz4_bytes={} ratio_vs_lz4={:.3}",
		input.len(),
		encoded.bytes.len(),
		lz4.bytes.len(),
		lz4.bytes.len() as f64 / encoded.bytes.len().max(1) as f64,
	);
	Ok(())
}

fn bench_selection_cpu(config: &BenchConfig) -> std::io::Result<()> {
	let mut input = Vec::new();
	for value in 0..4096_u64 {
		input.extend_from_slice(&(value % 4).to_le_bytes());
	}
	for sample_rows in [64_u32, 256, 1024] {
		let selection_config = EncodingWriterConfig {
			enabled: true,
			min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
			cascade_sample_rows: SampleRowCount::new(sample_rows),
			..EncodingWriterConfig::default()
		};
		let started = Instant::now();
		let mut bytes = 0_u64;
		for _ in 0..config.iterations {
			let encoded = select_and_encode_buffer(
				black_box(&input),
				&DataType::UInt64,
				BufferValueCount::new(4096),
				black_box(&selection_config),
			)?;
			bytes = encoded.bytes.len() as u64;
			black_box(encoded);
		}
		print_bench(
			"cascade_selection",
			&format!("sample_rows_{sample_rows}"),
			BenchCase {
				rows: 4096,
				columns: 1,
			},
			config.iterations,
			started.elapsed(),
			bytes,
		);
	}
	Ok(())
}

fn bench_enabled_cascade_ablation(config: &BenchConfig) -> std::io::Result<()> {
	let iterations = config.iterations.clamp(1, 10);
	for case in [
		BenchCase {
			rows: 8_192,
			columns: 8,
		},
		BenchCase {
			rows: 65_536,
			columns: 4,
		},
	] {
		let batch = bench_batch(case.rows, case.columns)?;
		let descriptor = descriptor(batch.schema().as_ref(), case.columns)?;
		for disabled in EncodingCascadeKind::ALL {
			let enabled_cascades = EncodingCascadeKind::ALL
				.into_iter()
				.filter(|kind| *kind != disabled)
				.collect::<BTreeSet<_>>();
			let ablation_config = encoding_config_with(enabled_cascades, 1024);
			let started = Instant::now();
			let mut bytes = 0_u64;
			for _ in 0..iterations {
				let encoded = encode_case(case, &batch, &descriptor, black_box(&ablation_config))?;
				bytes = encoded.frame.len() as u64;
				black_box(encoded);
			}
			print_bench(
				"enabled_cascades_ablation",
				&format!("disable_{}", disabled.as_str()),
				case,
				iterations,
				started.elapsed(),
				bytes,
			);
		}
	}
	Ok(())
}

fn bench_simd_page_tuning(config: &BenchConfig) -> std::io::Result<()> {
	let case = BenchCase {
		rows: 8_192,
		columns: 8,
	};
	let batch = bench_batch(case.rows, case.columns)?;
	let descriptor = descriptor(batch.schema().as_ref(), case.columns)?;
	let iterations = config.iterations.clamp(1, 10);
	for page_values in [512_u32, 1024, 2048] {
		let tuning_config = EncodingWriterConfig {
			enabled: true,
			target_simd_page_values: SimdPageValueCount::new(page_values),
			min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
			..EncodingWriterConfig::default()
		};
		let started = Instant::now();
		let mut bytes = 0_u64;
		for _ in 0..iterations {
			let encoded = encode_case(case, &batch, &descriptor, black_box(&tuning_config))?;
			bytes = encoded.frame.len() as u64;
			black_box(encoded);
		}
		print_bench(
			"target_simd_page_values",
			&format!("{page_values}"),
			case,
			iterations,
			started.elapsed(),
			bytes,
		);
	}
	Ok(())
}

fn bench_selection_edge_cases(config: &BenchConfig) -> std::io::Result<()> {
	let selection_config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		cascade_sample_rows: SampleRowCount::new(4096),
		..EncodingWriterConfig::default()
	};
	let mut nan_input = Vec::new();
	for _ in 0..4096 {
		nan_input.extend_from_slice(&f64::NAN.to_le_bytes());
	}
	bench_edge_case_selection(
		config,
		"all_nan_float64",
		&nan_input,
		&DataType::Float64,
		4096,
		None,
		&selection_config,
	)?;

	let mut near_monotonic = Vec::new();
	for value in 0..4096_u64 {
		let value = if value == 2048 { 17 } else { 1_000_000 + value };
		near_monotonic.extend_from_slice(&value.to_le_bytes());
	}
	bench_edge_case_selection(
		config,
		"near_monotonic_uint64",
		&near_monotonic,
		&DataType::UInt64,
		4096,
		None,
		&selection_config,
	)?;

	let values = (0..4096)
		.map(|row| if row % 32 == 0 { "" } else { "region-a" })
		.collect::<Vec<_>>();
	let mut string_input = Vec::new();
	let mut offsets = Vec::with_capacity(values.len() + 1);
	offsets.push(0_u64);
	for value in &values {
		string_input.extend_from_slice(value.as_bytes());
		offsets.push(string_input.len() as u64);
	}
	bench_edge_case_selection(
		config,
		"null_heavy_string_shape",
		&string_input,
		&DataType::Utf8,
		4096,
		Some(&offsets),
		&selection_config,
	)?;
	Ok(())
}

fn bench_edge_case_selection(
	config: &BenchConfig,
	name: &str,
	input: &[u8],
	data_type: &DataType,
	value_count: u32,
	value_offsets: Option<&[u64]>,
	selection_config: &EncodingWriterConfig,
) -> std::io::Result<()> {
	let started = Instant::now();
	let mut bytes = 0_u64;
	let mut cascade = EncodingCascadeKind::Plain;
	for _ in 0..config.iterations {
		let encoded = match value_offsets {
			Some(offsets) => select_and_encode_buffer_with_value_offsets(
				black_box(input),
				data_type,
				BufferValueCount::new(value_count),
				black_box(selection_config),
				Some(black_box(offsets)),
			)?,
			None => select_and_encode_buffer(
				black_box(input),
				data_type,
				BufferValueCount::new(value_count),
				black_box(selection_config),
			)?,
		};
		bytes = encoded.bytes.len() as u64;
		cascade = encoded.cascade.kind();
		black_box(encoded);
	}
	print_bench(
		"selection_edge_case",
		&format!("{name}_{}", cascade.as_str()),
		BenchCase {
			rows: value_count as usize,
			columns: 1,
		},
		config.iterations,
		started.elapsed(),
		bytes,
	);
	Ok(())
}

fn bench_l2_get_rate_cost() {
	let records = 100_u64;
	let hits_per_record = 10_u64;
	let hits = records.saturating_mul(hits_per_record);
	let row_group_gets = hits;
	let coalesced_page_gets = records;
	println!(
		"encoding_measure scenario=l2_get_rate_cost records={records} hits={hits} row_group_gets={row_group_gets} coalesced_page_gets={coalesced_page_gets} get_reduction_x={:.2}",
		row_group_gets as f64 / coalesced_page_gets.max(1) as f64,
	);
}

fn bench_csb_short_circuit() {
	println!(
		"encoding_measure scenario=csb_short_circuit slabs_pruned_by_csb=1 encoding_planner_invocations=0"
	);
}

fn print_bench(
	scenario: &str,
	variant: &str,
	case: BenchCase,
	iterations: usize,
	elapsed: Duration,
	bytes: u64,
) {
	let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
	println!(
		"encoding_bench scenario={scenario} variant={variant} rows={} columns={} iterations={} total_ms={:.3} micros_per_op={:.3} bytes={}",
		case.rows,
		case.columns,
		iterations,
		elapsed.as_secs_f64() * 1_000.0,
		micros_per_op,
		bytes,
	);
}
