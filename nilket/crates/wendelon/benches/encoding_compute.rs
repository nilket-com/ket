//! ADR 058 compute-over-encoded-data benchmark harness.
//!
//! `cargo bench --bench encoding_compute` for the full 8K / 65K / 1M
//! cascade matrix; `--quick` for a single 8,192-row pass against
//! the substrate floor.
//!
//! Layout: each fixture wraps an encoded buffer plus its dense decoded
//! reference and a default predicate atom. Scenario rows split into
//! three families:
//!
//! - `predicate_cocd`: the COCD substrate path
//!   (`dispatch::evaluate(...)`); output is an `EncodedRowMask`.
//! - `predicate_dense_micro`: a paged-aware `decode_buffer` plus a
//!   typed scalar comparison loop returning a `Vec<bool>`. This row
//!   establishes the dense-vs-COCD running-cost baseline; the ratio
//!   bounds the substrate's worst-case per-call overhead but is *not*
//!   the ADR's "<= 5% dense-selection overhead" gate.
//! - `predicate_classify_then_fallback`: routes through
//!   `evaluator_kind_for_cascade(...)` plus literal coercion plus the
//!   dense path, mirroring what the planner does on a DenseFallback
//!   decision. The (classify_then_fallback - dense_micro) /
//!   dense_micro ratio is the real "<= 5%" gate evidence.

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeSet;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wendelon::crypto::TieredPlaintextRange;
use wendelon::schema::aggregate::{
	AGG_FORMAT_VERSION, AggregateBudgets, AggregateExpression, AggregateFunctionTag,
};
use wendelon::schema::pbc::ColumnPredicateLiteral;
use wendelon::schema::{
	AggregateRequest, AggregateRequestId, BaseValue, BitsPerValue, BufferValueCount, Consistency,
	DictValueCount, EncodingCascadeIndex, HybridTimestamp, Identity, IpcBufferIndex,
	IpcColumnOrdinal, Locality, LocalityPolicy, LogicalSchemaFingerprint, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptor, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, RecordRowIndex, RowCount, RowGroupIndex, RowGroupRowCount,
	RowIdentityPolicy, RunCount, SchemaEvolutionPolicy, SimdPageValueCount, SlabId, TableObjectId,
	TableSchemaDescriptor, TenantId,
};
use wendelon::storage::encoding::cascade::{
	decode_buffer, decode_page, encode_buffer, encode_buffer_with_value_offsets,
	encode_fsst_payload_with_symbol_map,
};
use wendelon::storage::encoding::compute::{
	EncodedAggregateContext, EncodedRowMask, EncodingComputeConfig, EncodingComputeEvaluatorKind,
	PredicateAtom, PredicateAtomOp, coerce_literal, evaluate, evaluate_fsst_predicate,
	evaluate_fsst_predicate_with_stats, evaluate_fsst_row_safe_predicate,
	evaluate_fsst_row_safe_predicate_with_stats, evaluator_kind_for_cascade,
	try_fold_encoded_aggregate_request,
};
use wendelon::storage::encoding::directory::{
	build_encoding_directory_for_ipc_stream,
	build_encoding_directory_for_ipc_stream_exact_fsst_for_bench,
};
use wendelon::storage::encoding::page::expand_page_descriptors;
use wendelon::storage::encoding::select::select_and_encode_buffer_with_value_offsets;
use wendelon::storage::encoding::{
	BufferEncoding, EncodingCascade, EncodingCascadeKind, EncodingDirectory, EncodingLeafCascade,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	BufferPageDirectoryEntry, ColumnFetchRequirement, ColumnOrdinalSet, EncodingWriterConfig,
	PlaintextByteRange, RecordBodyByteRange, RecordPageDirectory, RowGroupDirectoryEntry,
	RowGroupWriterConfig, TieredRecordId, build_page_directory_for_ipc_stream,
	decode_projected_row_groups_from_plaintext_ranges,
	encode_multirow_from_survivors_with_row_group_and_encoding_config, encode_record_batches_bytes,
	parse_record_buffer_directory, selected_row_group_record_body_ranges,
};

const PAGE_VALUES: u32 = 256;
const WRITER_COLUMN_CACHE_VALUE_BUFFER_BYTE_TARGET: usize = 1024 * 1024;
const WRITER_COLUMN_CACHE_IPC_STREAM_VALUE_BYTE_TARGET: usize = 4 * 1024 * 1024;
const WRITER_COLUMN_CACHE_ROW_GROUP_ROW_TARGET: usize = 65_536;
const WRITER_COLUMN_CACHE_IPC_STREAM_ROW_TARGET: usize = 65_536;

fn main() -> std::io::Result<()> {
	let config = BenchConfig::from_args();
	if config.encoded_validity_only {
		bench_encoded_validity_sparse_projection_matrix(&config)?;
		return Ok(());
	}
	for rows in config.rows.iter().copied() {
		if config.writer_column_cache_only {
			bench_fsst_row_safe_writer_column_cache_matrix(&config, rows)?;
			continue;
		}
		if config.fsst_writer_only {
			bench_fsst_row_safe_writer_matrix(&config, rows)?;
			continue;
		}
		let fixtures = build_fixtures(rows)?;
		for fixture in &fixtures {
			bench_predicate_cocd(&config, fixture)?;
			bench_predicate_dense_micro(&config, fixture)?;
			bench_predicate_classify_then_fallback(&config, fixture)?;
		}
		bench_mask_algebra(&config, &fixtures)?;
		bench_fsst_predicate_matrix(&config, rows)?;
		bench_fsst_row_safe_predicate_matrix(&config, rows)?;
		bench_fsst_row_safe_declined_paths(&config, rows)?;
		bench_fsst_row_safe_writer_matrix(&config, rows)?;
		bench_fsst_row_safe_writer_column_cache_matrix(&config, rows)?;
		bench_fsst_row_safe_cross_replica_determinism(&config, rows)?;
		bench_fsst_declined_paths(&config, rows)?;
		bench_fsst_cross_replica_determinism(&config, rows)?;
		for fixture in &fixtures {
			bench_aggregate_count_star(&config, fixture)?;
			if fixture.aggregate_kind.is_some() {
				bench_aggregate_value_kernel(&config, fixture)?;
			}
		}
		// Keep legacy scenario names emitting at least once per row count
		// so the architectural guard token check stays satisfied even if
		// fixture names evolve.
		if let Some(default) = fixtures.iter().find(|f| f.name == "bp_u8") {
			bench_predicate_exact_legacy(&config, default)?;
			bench_aggregate_fold_legacy(&config, default)?;
			bench_mixed_prefilter(&config, default)?;
			bench_mixed_dense_fallback(&config, default)?;
			bench_mask_completeness(&config, default)?;
			bench_config_kernel_disabled(&config, default)?;
			bench_aggregate_selectivity_sweep(&config, default)?;
		}
		for fixture in &fixtures {
			if fixture.name == "bp_u8" || fixture.name == "dict_bitpacked_lowcard" {
				bench_aggregate_min_max(&config, fixture)?;
			}
		}
		bench_selectivity_sweep(&config, rows)?;
		bench_pipeline(&config, rows)?;
	}
	Ok(())
}

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
	iterations: usize,
	writer_column_cache_only: bool,
	writer_column_cache_fixture: Option<String>,
	fsst_writer_only: bool,
	encoded_validity_only: bool,
}

impl BenchConfig {
	fn from_args() -> Self {
		let quick = std::env::args().any(|arg| arg == "--quick");
		let writer_column_cache_only =
			std::env::args().any(|arg| arg == "--writer-column-cache-only");
		let fsst_writer_only = std::env::args().any(|arg| arg == "--fsst-writer-only");
		let encoded_validity_only = std::env::args().any(|arg| arg == "--encoded-validity-only");
		let writer_column_cache_fixture = std::env::args().find_map(|arg| {
			arg.strip_prefix("--writer-column-cache-fixture=")
				.map(str::to_owned)
		});
		let explicit_rows = std::env::args().find_map(|arg| {
			arg.strip_prefix("--rows=")
				.or_else(|| arg.strip_prefix("--writer-column-cache-rows="))
				.map(str::to_owned)
		});
		let rows = explicit_rows
			.as_deref()
			.map(|rows| {
				rows.split(',')
					.filter_map(|row| row.parse::<usize>().ok())
					.collect::<Vec<_>>()
			})
			.filter(|rows| !rows.is_empty());
		if quick {
			if let Some(rows) = rows {
				Self {
					rows,
					iterations: 50,
					writer_column_cache_only,
					writer_column_cache_fixture,
					fsst_writer_only,
					encoded_validity_only,
				}
			} else {
				Self {
					rows: vec![8_192],
					iterations: 50,
					writer_column_cache_only,
					writer_column_cache_fixture,
					fsst_writer_only,
					encoded_validity_only,
				}
			}
		} else if let Some(rows) = rows {
			Self {
				rows,
				iterations: 200,
				writer_column_cache_only,
				writer_column_cache_fixture,
				fsst_writer_only,
				encoded_validity_only,
			}
		} else {
			Self {
				rows: vec![8_192, 65_536, 1_048_576],
				iterations: 200,
				writer_column_cache_only,
				writer_column_cache_fixture,
				fsst_writer_only,
				encoded_validity_only,
			}
		}
	}
}

#[derive(Clone, Copy, Debug)]
enum AggregateKind {
	UnsignedSum,
	SignedSum,
}

#[derive(Clone, Debug)]
struct Fixture {
	name: &'static str,
	rows: usize,
	cascade: EncodingCascade,
	encoding: BufferEncoding,
	encoded: Vec<u8>,
	decoded: Vec<u8>,
	data_type: DataType,
	atom: PredicateAtom,
	aggregate_kind: Option<AggregateKind>,
	aggregate_column_dtype: DataType,
}

#[derive(Clone, Debug)]
struct FsstFixture {
	name: &'static str,
	rows: usize,
	cascade: EncodingCascade,
	encoding: BufferEncoding,
	encoded: Vec<u8>,
	decoded: Vec<u8>,
	offsets: Vec<i32>,
	offsets_u64: Vec<u64>,
	offset_bytes: Vec<u8>,
	data_type: DataType,
	atom: PredicateAtom,
}

fn build_fixtures(rows: usize) -> std::io::Result<Vec<Fixture>> {
	Ok(vec![
		bp_u8_fixture(rows)?,
		bp_u32_low_fixture(rows)?,
		rle_plain_fixture(rows)?,
		rle_bitpacked_fixture(rows)?,
		dict_plain_lowcard_fixture(rows)?,
		dict_bitpacked_lowcard_fixture(rows)?,
		dict_bitpacked_higher_fixture(rows)?,
		delta_plain_fixture(rows)?,
		delta_bitpacked_fixture(rows)?,
		plain_int_fixture(rows)?,
		lz4_int_fixture(rows)?,
	])
}

fn bp_u8_fixture(rows: usize) -> std::io::Result<Fixture> {
	let decoded = (0..rows).map(|row| (row % 251) as u8).collect::<Vec<_>>();
	let cascade = EncodingCascade::BitPacked {
		bits: BitsPerValue::new(8),
		base: BaseValue::new(0),
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::UInt8, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "bp_u8",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::UInt8,
		atom: int_atom(PredicateAtomOp::Lte, 12),
		aggregate_kind: Some(AggregateKind::UnsignedSum),
		aggregate_column_dtype: DataType::UInt8,
	})
}

fn bp_u32_low_fixture(rows: usize) -> std::io::Result<Fixture> {
	let bytes_per_value = 4;
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	for row in 0..rows {
		let value = (row as u32) % 4096;
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let cascade = EncodingCascade::BitPacked {
		bits: BitsPerValue::new(12),
		base: BaseValue::new(0),
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::UInt32, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "bp_u32_low",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::UInt32,
		atom: int_atom(PredicateAtomOp::Lt, 1024),
		aggregate_kind: Some(AggregateKind::UnsignedSum),
		aggregate_column_dtype: DataType::UInt32,
	})
}

fn rle_plain_fixture(rows: usize) -> std::io::Result<Fixture> {
	let run_len = 32usize;
	let bytes_per_value = 4;
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	for row in 0..rows {
		let run_index = (row / run_len) as u32;
		let value = run_index % 32;
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let runs = rows.div_ceil(run_len) as u32;
	let cascade = EncodingCascade::Rle {
		run_count: RunCount::new(runs),
		inner: Box::new(EncodingLeafCascade::Plain),
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::UInt32, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "rle_plain",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::UInt32,
		atom: int_atom(PredicateAtomOp::Eq, 7),
		aggregate_kind: Some(AggregateKind::UnsignedSum),
		aggregate_column_dtype: DataType::UInt32,
	})
}

fn rle_bitpacked_fixture(rows: usize) -> std::io::Result<Fixture> {
	let run_len = 32usize;
	let bytes_per_value = 4;
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	for row in 0..rows {
		let run_index = (row / run_len) as u32;
		let value = run_index % 16;
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let runs = rows.div_ceil(run_len) as u32;
	let cascade = EncodingCascade::Rle {
		run_count: RunCount::new(runs),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(4),
			base: BaseValue::new(0),
		}),
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::UInt32, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "rle_bitpacked",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::UInt32,
		atom: int_atom(PredicateAtomOp::Eq, 3),
		aggregate_kind: Some(AggregateKind::UnsignedSum),
		aggregate_column_dtype: DataType::UInt32,
	})
}

fn dict_plain_lowcard_fixture(rows: usize) -> std::io::Result<Fixture> {
	let bytes_per_value = 4;
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	let palette = [10u32, 20, 30, 40];
	for row in 0..rows {
		let value = palette[row % palette.len()];
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let cascade = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new((palette.len() * bytes_per_value) as u64),
		dict_value_count: DictValueCount::new(palette.len() as u32),
		inner: Box::new(EncodingLeafCascade::Plain),
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::UInt32, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "dict_plain_lowcard",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::UInt32,
		atom: int_atom(PredicateAtomOp::Eq, 30),
		aggregate_kind: Some(AggregateKind::UnsignedSum),
		aggregate_column_dtype: DataType::UInt32,
	})
}

fn dict_bitpacked_lowcard_fixture(rows: usize) -> std::io::Result<Fixture> {
	let bytes_per_value = 4;
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	let palette = [100u32, 200, 300, 400];
	for row in 0..rows {
		let value = palette[row % palette.len()];
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let cascade = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new((palette.len() * bytes_per_value) as u64),
		dict_value_count: DictValueCount::new(palette.len() as u32),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(2),
			base: BaseValue::new(0),
		}),
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::UInt32, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "dict_bitpacked_lowcard",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::UInt32,
		atom: int_atom(PredicateAtomOp::Eq, 300),
		aggregate_kind: Some(AggregateKind::UnsignedSum),
		aggregate_column_dtype: DataType::UInt32,
	})
}

fn dict_bitpacked_higher_fixture(rows: usize) -> std::io::Result<Fixture> {
	let bytes_per_value = 4;
	let palette: Vec<u32> = (0..256u32).map(|i| 1000 + i * 7).collect();
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	for row in 0..rows {
		let value = palette[row % palette.len()];
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let cascade = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new((palette.len() * bytes_per_value) as u64),
		dict_value_count: DictValueCount::new(palette.len() as u32),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(8),
			base: BaseValue::new(0),
		}),
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::UInt32, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "dict_bitpacked_higher",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::UInt32,
		atom: int_atom(PredicateAtomOp::Eq, 1700),
		aggregate_kind: Some(AggregateKind::UnsignedSum),
		aggregate_column_dtype: DataType::UInt32,
	})
}

fn delta_plain_fixture(rows: usize) -> std::io::Result<Fixture> {
	let bytes_per_value = 8;
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	let base = 1_700_000_000_000_i64;
	for row in 0..rows {
		let value = base + row as i64;
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let cascade = EncodingCascade::Delta {
		base: BaseValue::new(base as i128),
		inner: Box::new(EncodingLeafCascade::Plain),
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::Int64, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "delta_plain",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::Int64,
		atom: int_atom(PredicateAtomOp::Gte, base + (rows as i64) / 2),
		aggregate_kind: Some(AggregateKind::SignedSum),
		aggregate_column_dtype: DataType::Int64,
	})
}

fn delta_bitpacked_fixture(rows: usize) -> std::io::Result<Fixture> {
	let bytes_per_value = 8;
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	let base = 1_700_000_000_000_i64;
	for row in 0..rows {
		let value = base + row as i64;
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let cascade = EncodingCascade::Delta {
		base: BaseValue::new(base as i128),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(8),
			base: BaseValue::new(0),
		}),
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::Int64, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "delta_bitpacked",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::Int64,
		atom: int_atom(PredicateAtomOp::Gte, base + (rows as i64) / 2),
		aggregate_kind: Some(AggregateKind::SignedSum),
		aggregate_column_dtype: DataType::Int64,
	})
}

fn plain_int_fixture(rows: usize) -> std::io::Result<Fixture> {
	let bytes_per_value = 4;
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	for row in 0..rows {
		let value = (row as u32).wrapping_mul(2_654_435_761);
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let cascade = EncodingCascade::Plain;
	let encoded = encode_value_count_buffer(&decoded, &DataType::UInt32, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "plain_u32",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::UInt32,
		atom: int_atom(PredicateAtomOp::Lt, 1_000_000_000),
		aggregate_kind: Some(AggregateKind::UnsignedSum),
		aggregate_column_dtype: DataType::UInt32,
	})
}

fn lz4_int_fixture(rows: usize) -> std::io::Result<Fixture> {
	let bytes_per_value = 4;
	let mut decoded = Vec::with_capacity(rows * bytes_per_value);
	for row in 0..rows {
		let value = (row as u32) / 4;
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let cascade = EncodingCascade::Lz4Frame;
	let encoded = encode_value_count_buffer(&decoded, &DataType::UInt32, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	Ok(Fixture {
		name: "lz4_u32",
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		data_type: DataType::UInt32,
		atom: int_atom(PredicateAtomOp::Lt, 256),
		aggregate_kind: Some(AggregateKind::UnsignedSum),
		aggregate_column_dtype: DataType::UInt32,
	})
}

fn encode_value_count_buffer(
	decoded: &[u8],
	data_type: &DataType,
	cascade: &EncodingCascade,
	rows: usize,
) -> std::io::Result<wendelon::storage::encoding::cascade::EncodedBuffer> {
	encode_buffer(
		decoded,
		data_type,
		cascade.clone(),
		BufferValueCount::new(rows as u32),
		SimdPageValueCount::new(PAGE_VALUES),
	)
}

fn encoding_for(
	decoded: &[u8],
	encoded: &wendelon::storage::encoding::cascade::EncodedBuffer,
	rows: usize,
) -> BufferEncoding {
	BufferEncoding {
		row_group_index: RowGroupIndex::ZERO,
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(1),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(decoded.len() as u64),
		decoded_value_count: BufferValueCount::new(rows as u32),
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(PAGE_VALUES),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	}
}

fn int_atom(op: PredicateAtomOp, literal: i64) -> PredicateAtom {
	PredicateAtom {
		op,
		literal: Some(ColumnPredicateLiteral::Int(literal)),
	}
}

fn string_atom(op: PredicateAtomOp, literal: &[u8]) -> PredicateAtom {
	PredicateAtom {
		op,
		literal: Some(ColumnPredicateLiteral::Str(literal.to_vec())),
	}
}

fn string_list_atom(op: PredicateAtomOp, literals: &[Vec<u8>]) -> PredicateAtom {
	PredicateAtom {
		op,
		literal: Some(ColumnPredicateLiteral::List(
			literals
				.iter()
				.map(|literal| ColumnPredicateLiteral::Str(literal.clone()))
				.collect(),
		)),
	}
}

fn fsst_repeated_prefix_fixture(rows: usize) -> std::io::Result<FsstFixture> {
	fsst_fixture(rows, "fsst_url_repeated_prefix", |row| {
		format!("/tenant/alpha/catalog/product/{row:06}/details")
	})
}

fn fsst_url_eq_5p_fixture(rows: usize) -> std::io::Result<FsstFixture> {
	let selected_rows = rows.saturating_div(20).max(1);
	let hot = b"/tenant/alpha/catalog/product/000000/details".to_vec();
	let mut fixture = fsst_fixture(rows, "fsst_url_eq_5p", |row| {
		if row < selected_rows {
			"/tenant/alpha/catalog/product/000000/details".to_string()
		} else {
			format!(
				"/tenant/alpha/catalog/product/{:06}/details",
				100_000 + row % 900_000
			)
		}
	})?;
	fixture.atom = string_atom(PredicateAtomOp::Eq, &hot);
	Ok(fixture)
}

fn fsst_url_inlist4_5p_fixture(rows: usize) -> std::io::Result<FsstFixture> {
	let selected_rows = rows.saturating_div(20).max(1);
	let literals = (0..4)
		.map(|value| format!("/tenant/alpha/catalog/product/{value:06}/details").into_bytes())
		.collect::<Vec<_>>();
	let mut fixture = fsst_fixture(rows, "fsst_url_inlist4_5p", |row| {
		if row < selected_rows {
			format!(
				"/tenant/alpha/catalog/product/{:06}/details",
				row % literals.len()
			)
		} else {
			format!(
				"/tenant/alpha/catalog/product/{:06}/details",
				100_000 + row % 900_000
			)
		}
	})?;
	fixture.atom = string_list_atom(PredicateAtomOp::InList, &literals);
	Ok(fixture)
}

fn fsst_long_pair_eq_5p_fixture(rows: usize) -> std::io::Result<FsstFixture> {
	let selected_rows = rows.saturating_div(20).max(1);
	let hot =
		b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/selected/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			.to_vec();
	let mut fixture = fsst_fixture(rows, "fsst_long_pair_eq_5p", |row| {
		if row < selected_rows {
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/selected/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
				.to_string()
		} else {
			format!(
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/{row:08}/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			)
		}
	})?;
	fixture.atom = string_atom(PredicateAtomOp::Eq, &hot);
	Ok(fixture)
}

fn fsst_log_line_eq_5p_fixture(rows: usize) -> std::io::Result<FsstFixture> {
	let selected_rows = rows.saturating_div(20).max(1);
	let hot =
		b"level=error service=checkout region=us-east-1 status=500 message=payment_authorization_timeout"
			.to_vec();
	let mut fixture = fsst_fixture(rows, "fsst_log_line_eq_5p", |row| {
		if row < selected_rows {
			"level=error service=checkout region=us-east-1 status=500 message=payment_authorization_timeout"
				.to_string()
		} else {
			format!(
				"level=info service=checkout region=us-east-1 status=200 request_id={row:08} message=completed_cart_checkout user={:06}",
				row % 100_000
			)
		}
	})?;
	fixture.atom = string_atom(PredicateAtomOp::Eq, &hot);
	Ok(fixture)
}

fn fsst_short_string_fixture(rows: usize) -> std::io::Result<FsstFixture> {
	fsst_fixture(rows, "fsst_short_string_diag", |row| {
		format!("{:02x}", row % 256)
	})
}

fn fsst_metadata_floor_decline_fixture(rows: usize) -> std::io::Result<FsstFixture> {
	fsst_fixture(rows, "fsst_metadata_floor_decline_diag", |_row| {
		"a".to_string()
	})
}

fn fsst_weak_compression_fixture(rows: usize) -> std::io::Result<FsstFixture> {
	fsst_fixture(rows, "fsst_weak_compression_diag", |row| {
		let mixed = (row as u64)
			.wrapping_mul(0x9e37_79b9_7f4a_7c15)
			.rotate_left((row % 31) as u32);
		format!("{mixed:016x}{:08x}", row as u32 ^ 0xa5a5_5a5a)
	})
}

fn fsst_disjoint_length_fixture(rows: usize) -> std::io::Result<FsstFixture> {
	let mut fixture = fsst_fixture(rows, "fsst_disjoint_length_prefilter", |row| {
		format!("/tenant/alpha/catalog/product/{row:06}/details")
	})?;
	fixture.atom = string_atom(PredicateAtomOp::Eq, b"x");
	Ok(fixture)
}

fn fsst_fixture(
	rows: usize,
	name: &'static str,
	mut value_for_row: impl FnMut(usize) -> String,
) -> std::io::Result<FsstFixture> {
	let mut decoded = Vec::new();
	let mut offsets = Vec::with_capacity(rows.saturating_add(1));
	let mut offsets_u64 = Vec::with_capacity(rows.saturating_add(1));
	offsets.push(0_i32);
	offsets_u64.push(0_u64);
	for row in 0..rows {
		let value = value_for_row(row);
		decoded.extend_from_slice(value.as_bytes());
		let offset = i32::try_from(decoded.len()).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"FSST bench offset exceeds i32",
			)
		})?;
		offsets.push(offset);
		offsets_u64.push(offset as u64);
	}
	let cascade = EncodingCascade::FsstWholeBuffer {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_value_count_buffer(&decoded, &DataType::Utf8, &cascade, rows)?;
	let encoding = encoding_for(&decoded, &encoded, rows);
	let target_row = rows.saturating_div(3).min(rows.saturating_sub(1));
	let start = offsets[target_row] as usize;
	let end = offsets[target_row + 1] as usize;
	let target_literal = decoded[start..end].to_vec();
	let offset_bytes = offsets
		.iter()
		.flat_map(|offset| offset.to_le_bytes())
		.collect::<Vec<_>>();
	Ok(FsstFixture {
		name,
		rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded,
		offsets,
		offsets_u64,
		offset_bytes,
		data_type: DataType::Utf8,
		atom: string_atom(PredicateAtomOp::Eq, &target_literal),
	})
}

fn fsst_row_safe_fixture_from(
	fixture: &FsstFixture,
	name: &'static str,
) -> std::io::Result<FsstFixture> {
	let cascade = EncodingCascade::FsstRowSafe {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_buffer_with_value_offsets(
		&fixture.decoded,
		&fixture.data_type,
		cascade.clone(),
		BufferValueCount::new(fixture.rows as u32),
		SimdPageValueCount::new(PAGE_VALUES),
		Some(&fixture.offsets_u64),
	)?;
	let encoding = encoding_for(&fixture.decoded, &encoded, fixture.rows);
	Ok(FsstFixture {
		name,
		rows: fixture.rows,
		cascade,
		encoding,
		encoded: encoded.bytes,
		decoded: fixture.decoded.clone(),
		offsets: fixture.offsets.clone(),
		offsets_u64: fixture.offsets_u64.clone(),
		offset_bytes: fixture.offset_bytes.clone(),
		data_type: fixture.data_type.clone(),
		atom: fixture.atom.clone(),
	})
}

fn bench_predicate_cocd(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	let coercion = coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
		.expect("bench literal coerce");
	let elapsed = bench(config.iterations, || {
		let mask = evaluate(
			black_box(&fixture.encoded),
			black_box(&fixture.encoding),
			black_box(&fixture.cascade),
			black_box(&fixture.atom),
			black_box(&coercion),
		)
		.expect("bench predicate eval");
		black_box(mask);
	});
	emit_row(
		"predicate_cocd",
		fixture.name,
		fixture.rows,
		fixture.encoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_fsst_predicate_matrix(config: &BenchConfig, rows: usize) -> std::io::Result<()> {
	let fixtures = [
		fsst_repeated_prefix_fixture(rows)?,
		fsst_url_eq_5p_fixture(rows)?,
		fsst_url_inlist4_5p_fixture(rows)?,
		fsst_log_line_eq_5p_fixture(rows)?,
		fsst_short_string_fixture(rows)?,
		fsst_weak_compression_fixture(rows)?,
		fsst_disjoint_length_fixture(rows)?,
	];
	for fixture in &fixtures {
		let coercion = coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
			.expect("bench FSST literal coerce");
		let kernel_elapsed = bench(config.iterations, || {
			let mask = evaluate_fsst_predicate(
				black_box(&fixture.encoded),
				black_box(&fixture.encoding),
				black_box(&fixture.offset_bytes),
				None,
				None,
				black_box(&fixture.data_type),
				black_box(&fixture.atom),
				black_box(&coercion),
			)
			.expect("bench FSST kernel");
			black_box(mask);
		});
		let (stats_mask, kernel_stats) = evaluate_fsst_predicate_with_stats(
			black_box(&fixture.encoded),
			black_box(&fixture.encoding),
			black_box(&fixture.offset_bytes),
			None,
			None,
			black_box(&fixture.data_type),
			black_box(&fixture.atom),
			black_box(&coercion),
		)
		.expect("bench FSST stats");
		black_box(stats_mask);
		emit_row(
			"fsst_kernel_case_a1",
			fixture.name,
			fixture.rows,
			fixture.encoded.len() as u64,
			config.iterations,
			kernel_elapsed,
		);
		let dense_elapsed = bench(config.iterations, || {
			let decoded = dense_decode(
				black_box(&fixture.encoded),
				black_box(&fixture.data_type),
				black_box(&fixture.cascade),
				black_box(&fixture.encoding),
			)
			.expect("bench FSST dense decode");
			let selected = fsst_dense_string_compare(&decoded, &fixture.offsets, &fixture.atom);
			black_box(selected);
		});
		emit_row(
			"fsst_dense_fallback",
			fixture.name,
			fixture.rows,
			fixture.encoded.len() as u64,
			config.iterations,
			dense_elapsed,
		);
		let encoded_payload_ratio_micros =
			ratio_micros(fixture.encoded.len() as u64, fixture.decoded.len() as u64);
		let kernel_vs_dense_cpu_micros = duration_ratio_micros(kernel_elapsed, dense_elapsed);
		println!(
			"encoding_compute_bench scenario=fsst_recorded_diagnostic fixture={} rows={} iterations={} encoded_payload_bytes={} decoded_plaintext_bytes={} encoded_payload_ratio_micros={} kernel_vs_dense_cpu_micros={} length_match_rows={} length_mismatch_rows={} length_prefilter_elimination_micros={} fsst_byte_comparisons={} decoded_bytes_walked={} decoded_bytes_skipped={}",
			fixture.name,
			fixture.rows,
			config.iterations,
			fixture.encoded.len(),
			fixture.decoded.len(),
			encoded_payload_ratio_micros,
			kernel_vs_dense_cpu_micros,
			kernel_stats.length_match_rows,
			kernel_stats.length_mismatch_rows,
			ratio_micros(
				kernel_stats.length_mismatch_rows as u64,
				kernel_stats.rows as u64
			),
			kernel_stats.byte_comparisons,
			kernel_stats.decoded_bytes_walked,
			kernel_stats.decoded_bytes_skipped,
		);
	}
	Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct FsstRowSafeFrameLengths {
	symbol_table_bytes: usize,
	metadata_bytes: usize,
	payload_bytes: usize,
}

fn fsst_row_safe_frame_lengths(
	encoded: &[u8],
	rows: usize,
) -> std::io::Result<FsstRowSafeFrameLengths> {
	if encoded.len() < 13 || &encoded[..4] != b"RSFS" {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS frame header missing",
		));
	}
	let symbol_count = encoded[4] as usize;
	let symbol_table_bytes = symbol_count.checked_mul(2).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS symbol table overflow",
		)
	})?;
	let row_count_offset = 5_usize.checked_add(symbol_table_bytes).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS row-count offset overflow",
		)
	})?;
	let row_count_end = row_count_offset.checked_add(4).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS row-count end overflow",
		)
	})?;
	if encoded.len() < row_count_end {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS row-count field truncated",
		));
	}
	let row_count = u32::from_le_bytes(
		encoded[row_count_offset..row_count_end]
			.try_into()
			.expect("slice length checked"),
	) as usize;
	if row_count != rows {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS row count mismatch",
		));
	}
	const FSST_ROW_SAFE_BLOCK_ROWS: usize = 256;
	let block_count = row_count.div_ceil(FSST_ROW_SAFE_BLOCK_ROWS);
	let block_offset_bytes = block_count.checked_mul(8).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS block_offsets table overflow",
		)
	})?;
	let lengths_start = row_count_end
		.checked_add(block_offset_bytes)
		.ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"RSFS block_offsets end overflow",
			)
		})?;
	let exception_count_start = lengths_start.checked_add(row_count).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS encoded_lengths end overflow",
		)
	})?;
	let exception_count_end = exception_count_start.checked_add(4).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS exception_count end overflow",
		)
	})?;
	if encoded.len() < exception_count_end {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS metadata truncated",
		));
	}
	let exception_count = u32::from_le_bytes(
		encoded[exception_count_start..exception_count_end]
			.try_into()
			.expect("slice length checked"),
	) as usize;
	let exception_bytes = exception_count.checked_mul(4).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS exception_lengths overflow",
		)
	})?;
	let payload_start = exception_count_end
		.checked_add(exception_bytes)
		.ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"RSFS payload start overflow",
			)
		})?;
	if payload_start > encoded.len() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS metadata table truncated",
		));
	}
	// metadata = block_offsets + encoded_lengths + 4-byte exception_count + exception_lengths
	let metadata_bytes = block_offset_bytes
		.saturating_add(row_count)
		.saturating_add(4)
		.saturating_add(exception_bytes);
	Ok(FsstRowSafeFrameLengths {
		symbol_table_bytes,
		metadata_bytes,
		payload_bytes: encoded.len() - payload_start,
	})
}

fn fsst_literal_slices(atom: &PredicateAtom) -> Vec<&[u8]> {
	match atom.literal.as_ref().expect("FSST atom literal") {
		ColumnPredicateLiteral::Str(value) => vec![value.as_slice()],
		ColumnPredicateLiteral::List(values) => values
			.iter()
			.map(|value| match value {
				ColumnPredicateLiteral::Str(value) => value.as_slice(),
				_ => panic!("FSST bench supports string list literals only"),
			})
			.collect(),
		_ => panic!("FSST bench supports string literals only"),
	}
}

fn fsst_row_safe_literal_cache_bytes(
	encoded: &[u8],
	atom: &PredicateAtom,
) -> std::io::Result<usize> {
	if encoded.len() < 13 || &encoded[..4] != b"RSFS" {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS frame header missing",
		));
	}
	let symbol_count = encoded[4] as usize;
	let symbol_table_end = 5_usize
		.checked_add(symbol_count.checked_mul(2).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"RSFS symbol table overflow",
			)
		})?)
		.ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"RSFS symbol table end overflow",
			)
		})?;
	if symbol_table_end > encoded.len() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"RSFS symbol table truncated",
		));
	}
	let mut pair_to_symbol = vec![u16::MAX; 65_536];
	for (index, pair) in encoded[5..symbol_table_end].chunks_exact(2).enumerate() {
		let key = u16::from(pair[0]) << 8 | u16::from(pair[1]);
		pair_to_symbol[key as usize] = 0x80 | (index as u16);
	}
	let mut total = 0_usize;
	for literal in fsst_literal_slices(atom) {
		let mut out = Vec::new();
		encode_fsst_payload_with_symbol_map(literal, &pair_to_symbol, &mut out).map_err(|err| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("RSFS literal pre-encode failed: {err:?}"),
			)
		})?;
		total = total.saturating_add(out.len());
	}
	Ok(total)
}

fn bool_mask_digest(values: &[bool]) -> String {
	let mut hasher = blake3::Hasher::new();
	for value in values {
		hasher.update(&[u8::from(*value)]);
	}
	hasher.finalize().to_hex().to_string()
}

fn bench_fsst_row_safe_predicate_matrix(config: &BenchConfig, rows: usize) -> std::io::Result<()> {
	let cases = [
		(
			fsst_repeated_prefix_fixture(rows)?,
			"fsst_row_safe_url_repeated_prefix",
		),
		(fsst_url_eq_5p_fixture(rows)?, "fsst_row_safe_url_eq_5p"),
		(
			fsst_url_inlist4_5p_fixture(rows)?,
			"fsst_row_safe_url_inlist4_5p",
		),
		(
			fsst_long_pair_eq_5p_fixture(rows)?,
			"fsst_row_safe_long_pair_eq_5p",
		),
		(
			fsst_log_line_eq_5p_fixture(rows)?,
			"fsst_row_safe_log_line_eq_5p",
		),
		(
			fsst_short_string_fixture(rows)?,
			"fsst_row_safe_short_string_diag",
		),
		(
			fsst_weak_compression_fixture(rows)?,
			"fsst_row_safe_weak_compression_diag",
		),
		(
			fsst_disjoint_length_fixture(rows)?,
			"fsst_row_safe_disjoint_length_prefilter",
		),
	];
	for (fsst_fixture, row_safe_name) in &cases {
		let row_safe_fixture = fsst_row_safe_fixture_from(fsst_fixture, row_safe_name)?;
		let coercion = coerce_literal(
			row_safe_fixture.atom.literal.as_ref(),
			&row_safe_fixture.data_type,
		)
		.expect("bench RowSafe literal coerce");
		let strategy_b_elapsed = bench(config.iterations, || {
			let mask = evaluate_fsst_row_safe_predicate(
				black_box(&row_safe_fixture.encoded),
				black_box(&row_safe_fixture.encoding),
				black_box(&row_safe_fixture.offset_bytes),
				None,
				None,
				black_box(&row_safe_fixture.data_type),
				black_box(&row_safe_fixture.atom),
				black_box(&coercion),
			)
			.expect("bench RowSafe Strategy B kernel");
			black_box(mask);
		});
		let (row_safe_mask, row_safe_stats) = evaluate_fsst_row_safe_predicate_with_stats(
			black_box(&row_safe_fixture.encoded),
			black_box(&row_safe_fixture.encoding),
			black_box(&row_safe_fixture.offset_bytes),
			None,
			None,
			black_box(&row_safe_fixture.data_type),
			black_box(&row_safe_fixture.atom),
			black_box(&coercion),
		)
		.expect("bench RowSafe stats");
		emit_row(
			"fsst_row_safe_kernel_case_a1",
			row_safe_fixture.name,
			row_safe_fixture.rows,
			row_safe_fixture.encoded.len() as u64,
			config.iterations,
			strategy_b_elapsed,
		);

		let dense_fsst_row_safe_elapsed = bench(config.iterations, || {
			let decoded = dense_decode(
				black_box(&row_safe_fixture.encoded),
				black_box(&row_safe_fixture.data_type),
				black_box(&row_safe_fixture.cascade),
				black_box(&row_safe_fixture.encoding),
			)
			.expect("bench RowSafe dense RSFS decode");
			let selected = fsst_dense_string_compare(
				&decoded,
				&row_safe_fixture.offsets,
				&row_safe_fixture.atom,
			);
			black_box(selected);
		});
		emit_row(
			"fsst_row_safe_dense_fsst_row_safe",
			row_safe_fixture.name,
			row_safe_fixture.rows,
			row_safe_fixture.encoded.len() as u64,
			config.iterations,
			dense_fsst_row_safe_elapsed,
		);

		let strategy_a_coercion =
			coerce_literal(fsst_fixture.atom.literal.as_ref(), &fsst_fixture.data_type)
				.expect("bench FSST Strategy A literal coerce");
		let strategy_a_elapsed = bench(config.iterations, || {
			let mask = evaluate_fsst_predicate(
				black_box(&fsst_fixture.encoded),
				black_box(&fsst_fixture.encoding),
				black_box(&fsst_fixture.offset_bytes),
				None,
				None,
				black_box(&fsst_fixture.data_type),
				black_box(&fsst_fixture.atom),
				black_box(&strategy_a_coercion),
			)
			.expect("bench FSST Strategy A kernel");
			black_box(mask);
		});
		emit_row(
			"fsst_a_strategy_a_baseline",
			fsst_fixture.name,
			fsst_fixture.rows,
			fsst_fixture.encoded.len() as u64,
			config.iterations,
			strategy_a_elapsed,
		);

		let dense_fsst_whole_buffer_elapsed = bench(config.iterations, || {
			let decoded = dense_decode(
				black_box(&fsst_fixture.encoded),
				black_box(&fsst_fixture.data_type),
				black_box(&fsst_fixture.cascade),
				black_box(&fsst_fixture.encoding),
			)
			.expect("bench FSST dense WBFS decode");
			let selected =
				fsst_dense_string_compare(&decoded, &fsst_fixture.offsets, &fsst_fixture.atom);
			black_box(selected);
		});
		emit_row(
			"fsst_dense_fsst_whole_buffer_baseline",
			fsst_fixture.name,
			fsst_fixture.rows,
			fsst_fixture.encoded.len() as u64,
			config.iterations,
			dense_fsst_whole_buffer_elapsed,
		);

		let dense_decoded = dense_decode(
			&row_safe_fixture.encoded,
			&row_safe_fixture.data_type,
			&row_safe_fixture.cascade,
			&row_safe_fixture.encoding,
		)
		.expect("bench RowSafe dense RSFS oracle");
		let dense_digest = bool_mask_digest(&fsst_dense_string_compare(
			&dense_decoded,
			&row_safe_fixture.offsets,
			&row_safe_fixture.atom,
		));
		let row_safe_digest = encoded_mask_digest(&row_safe_mask);
		let strategy_a_mask = evaluate_fsst_predicate(
			&fsst_fixture.encoded,
			&fsst_fixture.encoding,
			&fsst_fixture.offset_bytes,
			None,
			None,
			&fsst_fixture.data_type,
			&fsst_fixture.atom,
			&strategy_a_coercion,
		)
		.expect("bench FSST Strategy A correctness mask");
		let strategy_a_digest = encoded_mask_digest(&strategy_a_mask);
		let frame = fsst_row_safe_frame_lengths(&row_safe_fixture.encoded, row_safe_fixture.rows)?;
		let literal_cache_bytes =
			fsst_row_safe_literal_cache_bytes(&row_safe_fixture.encoded, &row_safe_fixture.atom)?;
		println!(
			"encoding_compute_bench scenario=fsst_row_safe_recorded_diagnostic route=fsst_row_safe_kernel_case_a1 fixture={} rows={} iterations={} fsst_row_safe_encoded_bytes={} fsst_row_safe_payload_bytes={} fsst_row_safe_symbol_table_bytes={} fsst_row_safe_metadata_bytes={} fsst_whole_buffer_encoded_bytes={} decoded_plaintext_bytes={} fsst_row_safe_vs_fsst_whole_buffer_ratio_micros={} strategy_b_vs_dense_fsst_row_safe_cpu_micros={} strategy_b_vs_dense_fsst_whole_buffer_cpu_micros={} strategy_b_vs_strategy_a_cpu_micros={} speedup_vs_dense_fsst_row_safe_micros={} speedup_vs_dense_fsst_whole_buffer_micros={} speedup_vs_strategy_a_micros={} mask_equal_dense={} mask_equal_strategy_a={} preencoded_literal_cache_bytes={} decoded_length_match_rows={} decoded_length_mismatch_rows={} encoded_length_match_rows={} encoded_length_mismatch_rows={} encoded_payload_memcmp_bytes={} decoded_prefilter_elimination_micros={} encoded_prefilter_elimination_micros={}",
			row_safe_fixture.name,
			row_safe_fixture.rows,
			config.iterations,
			row_safe_fixture.encoded.len(),
			frame.payload_bytes,
			frame.symbol_table_bytes,
			frame.metadata_bytes,
			fsst_fixture.encoded.len(),
			row_safe_fixture.decoded.len(),
			ratio_micros(
				row_safe_fixture.encoded.len() as u64,
				fsst_fixture.encoded.len() as u64
			),
			duration_ratio_micros(strategy_b_elapsed, dense_fsst_row_safe_elapsed),
			duration_ratio_micros(strategy_b_elapsed, dense_fsst_whole_buffer_elapsed),
			duration_ratio_micros(strategy_b_elapsed, strategy_a_elapsed),
			duration_ratio_micros(dense_fsst_row_safe_elapsed, strategy_b_elapsed),
			duration_ratio_micros(dense_fsst_whole_buffer_elapsed, strategy_b_elapsed),
			duration_ratio_micros(strategy_a_elapsed, strategy_b_elapsed),
			row_safe_digest == dense_digest,
			row_safe_digest == strategy_a_digest,
			literal_cache_bytes,
			row_safe_stats.decoded_length_match_rows,
			row_safe_stats.decoded_length_mismatch_rows,
			row_safe_stats.encoded_length_match_rows,
			row_safe_stats.encoded_length_mismatch_rows,
			row_safe_stats.encoded_bytes_compared,
			ratio_micros(
				row_safe_stats.decoded_length_mismatch_rows as u64,
				row_safe_stats.rows as u64,
			),
			ratio_micros(
				row_safe_stats.encoded_length_mismatch_rows as u64,
				row_safe_stats.decoded_length_match_rows.max(1) as u64,
			),
		);
	}
	Ok(())
}

fn bench_fsst_row_safe_declined_paths(config: &BenchConfig, rows: usize) -> std::io::Result<()> {
	let fsst_fixture = fsst_repeated_prefix_fixture(rows)?;
	let fixture = fsst_row_safe_fixture_from(&fsst_fixture, "fsst_row_safe_decline_surface")?;
	let iterations = config.iterations.max(200);
	let target_literal = match fixture
		.atom
		.literal
		.as_ref()
		.expect("bench RowSafe literal")
	{
		ColumnPredicateLiteral::Str(value) => value.clone(),
		_ => unreachable!("RowSafe fixture uses string literal"),
	};
	let cases = [
		(
			"fsst_row_safe_dense_fallback_atom_unsupported",
			DataType::Utf8,
			string_atom(PredicateAtomOp::Lt, &target_literal),
		),
		(
			"fsst_row_safe_dense_fallback_type_unsupported",
			DataType::Utf8View,
			string_atom(PredicateAtomOp::Eq, &target_literal),
		),
	];
	let mut enabled_config = EncodingComputeConfig {
		enabled: true,
		..Default::default()
	};
	enabled_config
		.enabled_predicate_evaluators
		.insert(EncodingComputeEvaluatorKind::FsstRowSafe);
	for (route, data_type, atom) in cases {
		let coercion = coerce_literal(atom.literal.as_ref(), &data_type)
			.expect("bench RowSafe declined literal coerce");
		let dense_elapsed = bench(iterations, || {
			let decoded = dense_decode(
				black_box(&fixture.encoded),
				black_box(&fixture.data_type),
				black_box(&fixture.cascade),
				black_box(&fixture.encoding),
			)
			.expect("bench RowSafe declined dense decode");
			let selected = fsst_dense_string_compare(&decoded, &fixture.offsets, &atom);
			black_box(selected);
		});
		emit_row(
			"fsst_row_safe_declined_dense_baseline",
			route,
			fixture.rows,
			fixture.encoded.len() as u64,
			iterations,
			dense_elapsed,
		);
		let declined_elapsed = bench(iterations, || {
			let kind = evaluator_kind_for_cascade(black_box(&fixture.cascade));
			let allowed = kind
				.map(|kind| black_box(&enabled_config).predicate_evaluator_enabled(black_box(kind)))
				.unwrap_or(false);
			let decline = evaluate_fsst_row_safe_predicate(
				black_box(&fixture.encoded),
				black_box(&fixture.encoding),
				black_box(&fixture.offset_bytes),
				None,
				None,
				black_box(&data_type),
				black_box(&atom),
				black_box(&coercion),
			)
			.is_err();
			black_box((kind, allowed, decline));
			let decoded = dense_decode(
				black_box(&fixture.encoded),
				black_box(&fixture.data_type),
				black_box(&fixture.cascade),
				black_box(&fixture.encoding),
			)
			.expect("bench RowSafe declined dense decode");
			let selected = fsst_dense_string_compare(&decoded, &fixture.offsets, &atom);
			black_box(selected);
		});
		emit_row(
			"fsst_row_safe_declined_dense_fallback",
			route,
			fixture.rows,
			fixture.encoded.len() as u64,
			iterations,
			declined_elapsed,
		);
		println!(
			"encoding_compute_bench scenario=fsst_row_safe_declined_path_diagnostic route={} rows={} iterations={} dense_fallback_overhead_micros={} declined_vs_dense_cpu_micros={}",
			route,
			fixture.rows,
			iterations,
			duration_overhead_micros(declined_elapsed, dense_elapsed),
			duration_ratio_micros(declined_elapsed, dense_elapsed),
		);
	}
	let mut corrupt = fixture.encoded.clone();
	if let Some(first) = corrupt.first_mut() {
		*first ^= 0xff;
	}
	let corruption_elapsed = bench(iterations, || {
		let rejected = evaluate_fsst_row_safe_predicate(
			black_box(&corrupt),
			black_box(&fixture.encoding),
			black_box(&fixture.offset_bytes),
			None,
			None,
			black_box(&fixture.data_type),
			black_box(&fixture.atom),
			black_box(
				&coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
					.expect("bench RowSafe corruption coerce"),
			),
		)
		.is_err();
		black_box(rejected);
	});
	emit_row(
		"fsst_row_safe_corruption_fail_closed",
		"fsst_row_safe_magic",
		fixture.rows,
		fixture.encoded.len() as u64,
		iterations,
		corruption_elapsed,
	);
	Ok(())
}

fn bench_fsst_row_safe_writer_matrix(config: &BenchConfig, rows: usize) -> std::io::Result<()> {
	let fixtures = [
		fsst_long_pair_eq_5p_fixture(rows)?,
		fsst_repeated_prefix_fixture(rows)?,
		fsst_log_line_eq_5p_fixture(rows)?,
		fsst_short_string_fixture(rows)?,
		fsst_weak_compression_fixture(rows)?,
	];
	let writer_config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: BTreeSet::from([
			EncodingCascadeKind::Plain,
			EncodingCascadeKind::FsstWholeBuffer,
			EncodingCascadeKind::FsstRowSafe,
		]),
		..EncodingWriterConfig::default()
	};
	for fixture in &fixtures {
		let fsst_cascade = EncodingCascade::FsstWholeBuffer {
			symbol_table_byte_len: RecordBodyByteLen::ZERO,
		};
		let row_safe_cascade = EncodingCascade::FsstRowSafe {
			symbol_table_byte_len: RecordBodyByteLen::ZERO,
		};
		let fsst_whole_buffer_elapsed = bench(config.iterations, || {
			let encoded = encode_buffer_with_value_offsets(
				black_box(&fixture.decoded),
				black_box(&fixture.data_type),
				black_box(fsst_cascade.clone()),
				BufferValueCount::new(fixture.rows as u32),
				SimdPageValueCount::new(PAGE_VALUES),
				Some(black_box(&fixture.offsets_u64)),
			)
			.expect("bench WBFS writer");
			black_box(encoded);
		});
		let dual_elapsed = bench(config.iterations, || {
			let fsst_whole_buffer = encode_buffer_with_value_offsets(
				black_box(&fixture.decoded),
				black_box(&fixture.data_type),
				black_box(fsst_cascade.clone()),
				BufferValueCount::new(fixture.rows as u32),
				SimdPageValueCount::new(PAGE_VALUES),
				Some(black_box(&fixture.offsets_u64)),
			)
			.expect("bench WBFS writer");
			let fsst_row_safe = encode_buffer_with_value_offsets(
				black_box(&fixture.decoded),
				black_box(&fixture.data_type),
				black_box(row_safe_cascade.clone()),
				BufferValueCount::new(fixture.rows as u32),
				SimdPageValueCount::new(PAGE_VALUES),
				Some(black_box(&fixture.offsets_u64)),
			)
			.expect("bench RSFS writer");
			black_box((fsst_whole_buffer, fsst_row_safe));
		});
		let selected_elapsed = bench(config.iterations, || {
			let selected = select_and_encode_buffer_with_value_offsets(
				black_box(&fixture.decoded),
				black_box(&fixture.data_type),
				BufferValueCount::new(fixture.rows as u32),
				black_box(&writer_config),
				Some(black_box(&fixture.offsets_u64)),
			)
			.expect("bench production FSST writer selection");
			black_box(selected);
		});
		let selected = select_and_encode_buffer_with_value_offsets(
			&fixture.decoded,
			&fixture.data_type,
			BufferValueCount::new(fixture.rows as u32),
			&writer_config,
			Some(&fixture.offsets_u64),
		)?;
		let row_safe = fsst_row_safe_fixture_from(fixture, "fsst_row_safe_writer_probe")?;
		let route = match selected.cascade {
			EncodingCascade::FsstRowSafe { .. } => "fsst_row_safe_writer_selected",
			EncodingCascade::FsstWholeBuffer { .. } => "fsst_row_safe_writer_declined_to_fsst",
			_ => "fsst_row_safe_writer_declined_to_plain",
		};
		println!(
			"encoding_compute_bench scenario=fsst_row_safe_writer route={} fixture={} rows={} iterations={} fsst_whole_buffer_writer_ms={:.3} dual_writer_ms={:.3} selected_writer_ms={:.3} dual_vs_fsst_whole_buffer_cpu_micros={} selected_vs_dual_cpu_micros={} fsst_whole_buffer_encoded_bytes={} fsst_row_safe_encoded_bytes={} selected_cascade={} selected_encoded_bytes={} fsst_row_safe_vs_fsst_whole_buffer_ratio_micros={}",
			route,
			fixture.name,
			fixture.rows,
			config.iterations,
			fsst_whole_buffer_elapsed.as_secs_f64() * 1_000.0,
			dual_elapsed.as_secs_f64() * 1_000.0,
			selected_elapsed.as_secs_f64() * 1_000.0,
			duration_ratio_micros(dual_elapsed, fsst_whole_buffer_elapsed),
			duration_ratio_micros(selected_elapsed, dual_elapsed),
			fixture.encoded.len(),
			row_safe.encoded.len(),
			selected.cascade.kind().as_str(),
			selected.bytes.len(),
			ratio_micros(row_safe.encoded.len() as u64, fixture.encoded.len() as u64),
		);
	}
	Ok(())
}

fn bench_fsst_row_safe_writer_column_cache_matrix(
	config: &BenchConfig,
	rows: usize,
) -> std::io::Result<()> {
	let fixtures = [
		fsst_url_eq_5p_fixture(rows)?,
		fsst_url_inlist4_5p_fixture(rows)?,
		fsst_long_pair_eq_5p_fixture(rows)?,
		fsst_weak_compression_fixture(rows)?,
		fsst_log_line_eq_5p_fixture(rows)?,
		fsst_short_string_fixture(rows)?,
	];
	let metadata_floor_decline = fsst_metadata_floor_decline_fixture(rows)?;
	let writer_config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: BTreeSet::from([
			EncodingCascadeKind::Plain,
			EncodingCascadeKind::FsstWholeBuffer,
			EncodingCascadeKind::FsstRowSafe,
		]),
		..EncodingWriterConfig::default()
	};
	let row_group_config = RowGroupWriterConfig::scan().with_enabled(true);
	let record_id = TieredRecordId {
		slab_id: SlabId::new(68),
		record_offset: RecordBodyByteOffset::ZERO,
	};
	let selected_fixtures =
		if config.writer_column_cache_fixture.as_deref() == Some(metadata_floor_decline.name) {
			vec![&metadata_floor_decline]
		} else {
			fixtures
				.iter()
				.filter(
					|fixture| match config.writer_column_cache_fixture.as_deref() {
						Some(filter) => fixture.name == filter,
						None => true,
					},
				)
				.collect::<Vec<_>>()
		};
	for fixture in selected_fixtures {
		for requested_row_groups in [1_usize, 2, 4, 8, 16] {
			let group_count = requested_row_groups.min(rows.max(1));
			let rows_per_group = rows.div_ceil(group_count);
			let values = fsst_fixture_strings(fixture)?;
			let prepared_shards = prepare_writer_column_cache_string_shards(
				&values,
				rows_per_group,
				record_id,
				&row_group_config,
			)?;
			let actual_row_groups = prepared_shards
				.iter()
				.map(|shard| shard.batches.len())
				.sum::<usize>();
			let elapsed = bench(config.iterations, || {
				for shard in &prepared_shards {
					let result = build_encoding_directory_for_ipc_stream(
						record_id,
						black_box(&shard.ipc_bytes),
						black_box(&shard.page_directory),
						black_box(&shard.batches),
						black_box(&writer_config),
						black_box(&row_group_config),
					)
					.expect("bench writer column-cache encoding directory");
					black_box(result);
				}
			});
			let exact_elapsed = bench(config.iterations, || {
				for shard in &prepared_shards {
					let result = build_encoding_directory_for_ipc_stream_exact_fsst_for_bench(
						record_id,
						black_box(&shard.ipc_bytes),
						black_box(&shard.page_directory),
						black_box(&shard.batches),
						black_box(&writer_config),
						black_box(&row_group_config),
					)
					.expect("bench exact FSST encoding directory");
					black_box(result);
				}
			});
			let mut observed_value_buffers = 0_usize;
			let mut cache_miss_estimate = 0_usize;
			let mut cache_hit_estimate = 0_usize;
			let mut exact_row_safe_winners = 0_usize;
			let mut actual_row_safe_selected = 0_usize;
			let mut false_declines = 0_usize;
			let mut cascade_matches = 0_usize;
			let mut byte_len_matches = 0_usize;
			let mut selected_bytes = 0_usize;
			let mut exact_bytes = 0_usize;
			for shard in &prepared_shards {
				let mut shard_observed_value_buffers = 0_usize;
				let (encoding_directory, applied) = build_encoding_directory_for_ipc_stream(
					record_id,
					&shard.ipc_bytes,
					&shard.page_directory,
					&shard.batches,
					&writer_config,
					&row_group_config,
				)?;
				let Some(encoding_directory) = encoding_directory else {
					continue;
				};
				for group in &shard.page_directory.row_groups {
					let Some(value_buffer_ordinal) = value_buffer_ordinal_for_column(
						&shard.page_directory,
						group.row_group_index,
						1,
					) else {
						continue;
					};
					let Some(buffer) = encoding_directory.buffers.iter().find(|buffer| {
						buffer.row_group_index == group.row_group_index
							&& buffer.ipc_column_ordinal == IpcColumnOrdinal::new(1)
							&& buffer.ipc_buffer_ordinal == value_buffer_ordinal
					}) else {
						continue;
					};
					let Some(applied_buffer) = applied.iter().find(|applied| {
						applied.row_group_index == group.row_group_index
							&& applied.ipc_column_ordinal == IpcColumnOrdinal::new(1)
							&& applied.ipc_buffer_ordinal == value_buffer_ordinal
					}) else {
						continue;
					};
					let cascade = &encoding_directory.cascades[buffer.cascade_index.get() as usize];
					let group_start = shard.start_row + group.first_record_row_index.get() as usize;
					let group_rows = group.row_count.get() as usize;
					let oracle = exact_fsst_oracle_for_group(fixture, group_start, group_rows)?;
					observed_value_buffers = observed_value_buffers.saturating_add(1);
					shard_observed_value_buffers = shard_observed_value_buffers.saturating_add(1);
					if oracle.row_safe_selected {
						exact_row_safe_winners = exact_row_safe_winners.saturating_add(1);
					}
					if matches!(cascade, EncodingCascade::FsstRowSafe { .. }) {
						actual_row_safe_selected = actual_row_safe_selected.saturating_add(1);
					}
					if oracle.row_safe_selected
						&& !matches!(cascade, EncodingCascade::FsstRowSafe { .. })
					{
						false_declines = false_declines.saturating_add(1);
					}
					if cascade.kind() == oracle.cascade_kind {
						cascade_matches = cascade_matches.saturating_add(1);
					}
					if applied_buffer.encoded_bytes.len() == oracle.encoded_len {
						byte_len_matches = byte_len_matches.saturating_add(1);
					}
					selected_bytes =
						selected_bytes.saturating_add(applied_buffer.encoded_bytes.len());
					exact_bytes = exact_bytes.saturating_add(oracle.encoded_len);
				}
				if shard_observed_value_buffers > 0 {
					cache_miss_estimate = cache_miss_estimate.saturating_add(1);
					cache_hit_estimate = cache_hit_estimate
						.saturating_add(shard_observed_value_buffers.saturating_sub(1));
				}
			}
			if observed_value_buffers == 0 {
				println!(
					"encoding_compute_bench scenario=fsst_row_safe_writer_column_cache_matrix route=no_encoding fixture={} rows={} requested_row_groups={} actual_row_groups={} observed_value_buffers=0 iterations={} writer_ms={:.3} exact_writer_ms={:.3} writer_vs_exact_cpu_micros={} exact_vs_writer_cpu_micros={}",
					fixture.name,
					fixture.rows,
					requested_row_groups,
					actual_row_groups,
					config.iterations,
					elapsed.as_secs_f64() * 1_000.0,
					exact_elapsed.as_secs_f64() * 1_000.0,
					duration_ratio_micros(elapsed, exact_elapsed),
					duration_ratio_micros(exact_elapsed, elapsed),
				);
				continue;
			}
			println!(
				"encoding_compute_bench scenario=fsst_row_safe_writer_column_cache_matrix route=public_vs_exact fixture={} rows={} requested_row_groups={} actual_row_groups={} observed_value_buffers={} cache_miss_estimate={} cache_hit_estimate={} iterations={} writer_ms={:.3} exact_writer_ms={:.3} writer_vs_exact_cpu_micros={} exact_vs_writer_cpu_micros={} exact_row_safe_winners={} actual_row_safe_selected={} false_declines={} cascade_matches={} byte_len_matches={} selected_encoded_bytes={} exact_encoded_bytes={} selected_vs_exact_bytes_micros={}",
				fixture.name,
				fixture.rows,
				requested_row_groups,
				actual_row_groups,
				observed_value_buffers,
				cache_miss_estimate,
				cache_hit_estimate,
				config.iterations,
				elapsed.as_secs_f64() * 1_000.0,
				exact_elapsed.as_secs_f64() * 1_000.0,
				duration_ratio_micros(elapsed, exact_elapsed),
				duration_ratio_micros(exact_elapsed, elapsed),
				exact_row_safe_winners,
				actual_row_safe_selected,
				false_declines,
				cascade_matches,
				byte_len_matches,
				selected_bytes,
				exact_bytes,
				ratio_micros(selected_bytes as u64, exact_bytes as u64),
			);
		}
	}
	Ok(())
}

struct FsstExactOracle {
	cascade_kind: EncodingCascadeKind,
	encoded_len: usize,
	row_safe_selected: bool,
}

struct WriterColumnCacheStringBatchShard {
	start_row: usize,
	batches: Vec<RecordBatch>,
}

struct WriterColumnCachePreparedShard {
	start_row: usize,
	ipc_bytes: Vec<u8>,
	page_directory: wendelon::storage::row_group::RecordPageDirectory,
	batches: Vec<RecordBatch>,
}

fn exact_fsst_oracle_for_group(
	fixture: &FsstFixture,
	start_row: usize,
	row_count: usize,
) -> std::io::Result<FsstExactOracle> {
	let (decoded, offsets) = fsst_fixture_group_payload(fixture, start_row, row_count);
	let decoded_value_count = BufferValueCount::new(row_count as u32);
	let fsst_cascade = EncodingCascade::FsstWholeBuffer {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let row_safe_cascade = EncodingCascade::FsstRowSafe {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let fsst = encode_buffer_with_value_offsets(
		&decoded,
		&DataType::Utf8,
		fsst_cascade,
		decoded_value_count,
		SimdPageValueCount::new(PAGE_VALUES),
		Some(&offsets),
	)?;
	let row_safe = encode_buffer_with_value_offsets(
		&decoded,
		&DataType::Utf8,
		row_safe_cascade,
		decoded_value_count,
		SimdPageValueCount::new(PAGE_VALUES),
		Some(&offsets),
	)?;
	let row_safe_selected = row_safe.bytes.len() <= decoded.len()
		&& row_safe.bytes.len() <= fsst.bytes.len().saturating_mul(11).div_ceil(10);
	if row_safe_selected {
		Ok(FsstExactOracle {
			cascade_kind: EncodingCascadeKind::FsstRowSafe,
			encoded_len: row_safe.bytes.len(),
			row_safe_selected,
		})
	} else {
		Ok(FsstExactOracle {
			cascade_kind: EncodingCascadeKind::FsstWholeBuffer,
			encoded_len: fsst.bytes.len(),
			row_safe_selected,
		})
	}
}

fn fsst_fixture_group_payload(
	fixture: &FsstFixture,
	start_row: usize,
	row_count: usize,
) -> (Vec<u8>, Vec<u64>) {
	let mut decoded = Vec::new();
	let mut offsets = Vec::with_capacity(row_count.saturating_add(1));
	offsets.push(0_u64);
	for row in start_row..start_row.saturating_add(row_count) {
		let start = fixture.offsets_u64[row] as usize;
		let end = fixture.offsets_u64[row + 1] as usize;
		decoded.extend_from_slice(&fixture.decoded[start..end]);
		offsets.push(decoded.len() as u64);
	}
	(decoded, offsets)
}

fn fsst_fixture_strings(fixture: &FsstFixture) -> std::io::Result<Vec<String>> {
	(0..fixture.rows)
		.map(|row| {
			let start = fixture.offsets_u64[row] as usize;
			let end = fixture.offsets_u64[row + 1] as usize;
			std::str::from_utf8(&fixture.decoded[start..end])
				.map(str::to_owned)
				.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
		})
		.collect()
}

fn prepare_writer_column_cache_string_shards(
	values: &[String],
	rows_per_group: usize,
	record_id: TieredRecordId,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<Vec<WriterColumnCachePreparedShard>> {
	let batch_shards = writer_column_cache_string_batch_shards(values, rows_per_group)?;
	let Some(first_batch) = batch_shards.first().and_then(|shard| shard.batches.first()) else {
		return Ok(Vec::new());
	};
	let descriptor = writer_column_cache_string_descriptor(first_batch.schema().as_ref())?;
	batch_shards
		.into_iter()
		.map(|shard| {
			let ipc_bytes = encode_record_batches_bytes(
				&shard.batches,
				RecordBodyCompressionPolicy::Uncompressed,
			)?;
			let page_directory = build_page_directory_for_ipc_stream(
				record_id,
				&ipc_bytes,
				&shard.batches,
				descriptor.logical_schema_fingerprint,
				&row_group_config.stats_supported_types,
			)?;
			Ok(WriterColumnCachePreparedShard {
				start_row: shard.start_row,
				ipc_bytes,
				page_directory,
				batches: shard.batches,
			})
		})
		.collect()
}

fn writer_column_cache_string_batch_shards(
	values: &[String],
	rows_per_group: usize,
) -> std::io::Result<Vec<WriterColumnCacheStringBatchShard>> {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("value", DataType::Utf8, false),
	]));
	let mut shards = Vec::new();
	let mut current_batches = Vec::new();
	let mut current_value_bytes = 0_usize;
	let mut current_rows = 0_usize;
	let mut current_start_row = 0_usize;
	let rows_per_group = rows_per_group.max(1);
	let mut start = 0_usize;
	while start < values.len() {
		let mut end = start;
		let mut value_bytes = 0_usize;
		while end < values.len()
			&& end - start < rows_per_group
			&& end - start < WRITER_COLUMN_CACHE_ROW_GROUP_ROW_TARGET
		{
			let next_len = values[end].len();
			if end > start
				&& value_bytes.saturating_add(next_len)
					> WRITER_COLUMN_CACHE_VALUE_BUFFER_BYTE_TARGET
			{
				break;
			}
			value_bytes = value_bytes.saturating_add(next_len);
			end += 1;
		}
		if !current_batches.is_empty()
			&& (current_value_bytes.saturating_add(value_bytes)
				> WRITER_COLUMN_CACHE_IPC_STREAM_VALUE_BYTE_TARGET
				|| current_rows.saturating_add(end - start)
					> WRITER_COLUMN_CACHE_IPC_STREAM_ROW_TARGET)
		{
			shards.push(WriterColumnCacheStringBatchShard {
				start_row: current_start_row,
				batches: std::mem::take(&mut current_batches),
			});
			current_value_bytes = 0;
			current_rows = 0;
		}
		if current_batches.is_empty() {
			current_start_row = start;
		}
		let ids = (start..end).map(|row| row as i64).collect::<Vec<_>>();
		let strings = values[start..end].to_vec();
		current_batches.push(
			RecordBatch::try_new(
				schema.clone(),
				vec![
					Arc::new(Int64Array::from(ids)) as ArrayRef,
					Arc::new(StringArray::from(strings)) as ArrayRef,
				],
			)
			.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?,
		);
		current_value_bytes = current_value_bytes.saturating_add(value_bytes);
		current_rows = current_rows.saturating_add(end - start);
		start = end;
	}
	if !current_batches.is_empty() {
		shards.push(WriterColumnCacheStringBatchShard {
			start_row: current_start_row,
			batches: current_batches,
		});
	}
	Ok(shards)
}

fn writer_column_cache_string_descriptor(
	schema: &Schema,
) -> std::io::Result<TableSchemaDescriptor> {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x68; 16]),
		1,
		"bench",
		"encoding_compute_writer_column_cache",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(1024, 1024, 8 * 1024 * 1024)
				.expect("bench multi-row layout"),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn value_buffer_ordinal_for_column(
	page_directory: &wendelon::storage::row_group::RecordPageDirectory,
	row_group_index: RowGroupIndex,
	column_ordinal: u32,
) -> Option<IpcBufferIndex> {
	page_directory
		.buffer_pages
		.iter()
		.filter(|page| {
			page.row_group_index == row_group_index
				&& page.ipc_column_ordinal == IpcColumnOrdinal::new(column_ordinal)
		})
		.map(|page| page.ipc_buffer_ordinal.get())
		.min()
		.map(|first| IpcBufferIndex::new(first.saturating_add(2)))
}

fn bench_fsst_row_safe_cross_replica_determinism(
	config: &BenchConfig,
	rows: usize,
) -> std::io::Result<()> {
	let left_fsst = fsst_repeated_prefix_fixture(rows)?;
	let right_fsst = fsst_repeated_prefix_fixture(rows)?;
	let left = fsst_row_safe_fixture_from(&left_fsst, "fsst_row_safe_url_repeated_prefix")?;
	let right = fsst_row_safe_fixture_from(&right_fsst, "fsst_row_safe_url_repeated_prefix")?;
	let left_coercion = coerce_literal(left.atom.literal.as_ref(), &left.data_type)
		.expect("bench RowSafe left literal coerce");
	let right_coercion = coerce_literal(right.atom.literal.as_ref(), &right.data_type)
		.expect("bench RowSafe right literal coerce");
	let left_mask = evaluate_fsst_row_safe_predicate(
		&left.encoded,
		&left.encoding,
		&left.offset_bytes,
		None,
		None,
		&left.data_type,
		&left.atom,
		&left_coercion,
	)
	.expect("bench RowSafe left deterministic mask");
	let right_mask = evaluate_fsst_row_safe_predicate(
		&right.encoded,
		&right.encoding,
		&right.offset_bytes,
		None,
		None,
		&right.data_type,
		&right.atom,
		&right_coercion,
	)
	.expect("bench RowSafe right deterministic mask");
	let left_digest = encoded_mask_digest(&left_mask);
	let right_digest = encoded_mask_digest(&right_mask);
	println!(
		"encoding_compute_bench scenario=fsst_row_safe_cross_replica_determinism fixture={} rows={} iterations={} mask_equal={} encoded_equal={} left_digest={} right_digest={} encoded_bytes={}",
		left.name,
		rows,
		config.iterations,
		left_digest == right_digest,
		left.encoded == right.encoded && left.offset_bytes == right.offset_bytes,
		left_digest,
		right_digest,
		left.encoded.len(),
	);
	Ok(())
}

fn bench_fsst_declined_paths(config: &BenchConfig, rows: usize) -> std::io::Result<()> {
	let fixture = fsst_repeated_prefix_fixture(rows)?;
	let iterations = config.iterations.max(200);
	let target_literal = match fixture.atom.literal.as_ref().expect("bench FSST literal") {
		ColumnPredicateLiteral::Str(value) => value.clone(),
		_ => unreachable!("FSST fixture uses string literal"),
	};
	let cases = [
		(
			"dense_fallback_atom_unsupported",
			DataType::Utf8,
			string_atom(PredicateAtomOp::Lt, &target_literal),
		),
		(
			"dense_fallback_type_unsupported_utf8_view",
			DataType::Utf8View,
			string_atom(PredicateAtomOp::Eq, &target_literal),
		),
		(
			"dense_fallback_type_unsupported_binary_view",
			DataType::BinaryView,
			string_atom(PredicateAtomOp::Eq, &target_literal),
		),
	];
	let mut enabled_config = EncodingComputeConfig {
		enabled: true,
		..Default::default()
	};
	enabled_config
		.enabled_predicate_evaluators
		.insert(EncodingComputeEvaluatorKind::FsstWholeBuffer);
	for (route, data_type, atom) in cases {
		let coercion = coerce_literal(atom.literal.as_ref(), &data_type)
			.expect("bench FSST declined literal coerce");
		let dense_elapsed = bench(iterations, || {
			let decoded = dense_decode(
				black_box(&fixture.encoded),
				black_box(&data_type),
				black_box(&fixture.cascade),
				black_box(&fixture.encoding),
			)
			.expect("bench FSST declined dense decode");
			let selected = fsst_dense_string_compare(&decoded, &fixture.offsets, &atom);
			black_box(selected);
		});
		emit_row(
			"fsst_declined_dense_baseline",
			route,
			fixture.rows,
			fixture.encoded.len() as u64,
			iterations,
			dense_elapsed,
		);
		let declined_elapsed = bench(iterations, || {
			let kind = evaluator_kind_for_cascade(black_box(&fixture.cascade));
			let allowed = kind
				.map(|kind| black_box(&enabled_config).predicate_evaluator_enabled(black_box(kind)))
				.unwrap_or(false);
			let decline = evaluate_fsst_predicate(
				black_box(&fixture.encoded),
				black_box(&fixture.encoding),
				black_box(&fixture.offset_bytes),
				None,
				None,
				black_box(&data_type),
				black_box(&atom),
				black_box(&coercion),
			)
			.is_err();
			black_box((kind, allowed, decline));
			let decoded = dense_decode(
				black_box(&fixture.encoded),
				black_box(&data_type),
				black_box(&fixture.cascade),
				black_box(&fixture.encoding),
			)
			.expect("bench FSST declined dense decode");
			let selected = fsst_dense_string_compare(&decoded, &fixture.offsets, &atom);
			black_box(selected);
		});
		emit_row(
			"fsst_declined_dense_fallback",
			route,
			fixture.rows,
			fixture.encoded.len() as u64,
			iterations,
			declined_elapsed,
		);
		println!(
			"encoding_compute_bench scenario=fsst_declined_path_diagnostic route={} rows={} iterations={} dense_fallback_overhead_micros={} declined_vs_dense_cpu_micros={}",
			route,
			fixture.rows,
			iterations,
			duration_overhead_micros(declined_elapsed, dense_elapsed),
			duration_ratio_micros(declined_elapsed, dense_elapsed),
		);
	}
	Ok(())
}

fn bench_fsst_cross_replica_determinism(config: &BenchConfig, rows: usize) -> std::io::Result<()> {
	let left = fsst_repeated_prefix_fixture(rows)?;
	let right = fsst_repeated_prefix_fixture(rows)?;
	let left_coercion = coerce_literal(left.atom.literal.as_ref(), &left.data_type)
		.expect("bench FSST left literal coerce");
	let right_coercion = coerce_literal(right.atom.literal.as_ref(), &right.data_type)
		.expect("bench FSST right literal coerce");
	let left_mask = evaluate_fsst_predicate(
		&left.encoded,
		&left.encoding,
		&left.offset_bytes,
		None,
		None,
		&left.data_type,
		&left.atom,
		&left_coercion,
	)
	.expect("bench FSST left deterministic mask");
	let right_mask = evaluate_fsst_predicate(
		&right.encoded,
		&right.encoding,
		&right.offset_bytes,
		None,
		None,
		&right.data_type,
		&right.atom,
		&right_coercion,
	)
	.expect("bench FSST right deterministic mask");
	let left_digest = encoded_mask_digest(&left_mask);
	let right_digest = encoded_mask_digest(&right_mask);
	println!(
		"encoding_compute_bench scenario=fsst_cross_replica_determinism fixture={} rows={} iterations={} mask_equal={} encoded_equal={} left_digest={} right_digest={} encoded_bytes={}",
		left.name,
		rows,
		config.iterations,
		left_digest == right_digest,
		left.encoded == right.encoded && left.offset_bytes == right.offset_bytes,
		left_digest,
		right_digest,
		left.encoded.len(),
	);
	Ok(())
}

fn bench_predicate_dense_micro(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	let coercion = coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
		.expect("bench literal coerce");
	let elapsed = bench(config.iterations, || {
		let decoded = dense_decode(
			black_box(&fixture.encoded),
			black_box(&fixture.data_type),
			black_box(&fixture.cascade),
			black_box(&fixture.encoding),
		)
		.expect("bench dense decode");
		let selected = scalar_compare(&decoded, &fixture.data_type, &fixture.atom, &coercion);
		black_box(selected);
	});
	emit_row(
		"predicate_dense_micro",
		fixture.name,
		fixture.rows,
		fixture.encoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_mixed_prefilter(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	// Mixed (A AND B) where A is encoded (BitPacked Lt literal) and B
	// is a residual scalar atom (Gte literal) over the same column.
	// Prefilter mode: encoded evaluation of A produces a survivor
	// mask; the residual B runs only against rows where A selected.
	for (label, lt_literal, expected_selectivity) in [
		("high_reject", 13_i64, "~5%"),
		("low_reject", 240_i64, "~95%"),
	] {
		let atom_a = int_atom(PredicateAtomOp::Lt, lt_literal);
		let coercion_a =
			coerce_literal(atom_a.literal.as_ref(), &fixture.data_type).expect("bench coerce A");
		let elapsed = bench(config.iterations, || {
			let mask_a = evaluate(
				black_box(&fixture.encoded),
				black_box(&fixture.encoding),
				black_box(&fixture.cascade),
				black_box(&atom_a),
				black_box(&coercion_a),
			)
			.expect("bench encoded A");
			let survivors = mask_a.to_bool_vec().expect("bench survivors");
			let width = value_byte_width(&fixture.data_type);
			let mut combined = Vec::with_capacity(survivors.len());
			for (row, &keep) in survivors.iter().enumerate() {
				if !keep {
					combined.push(false);
					continue;
				}
				let chunk = &fixture.decoded[row * width..(row + 1) * width];
				let value = read_signed_le(chunk, &fixture.data_type);
				combined.push(value >= 100);
			}
			black_box(combined);
		});
		emit_row(
			&format!("mixed_prefilter_{label}"),
			&format!("{}/A_lt_{lt_literal}_{expected_selectivity}", fixture.name),
			fixture.rows,
			fixture.encoded.len() as u64,
			config.iterations,
			elapsed,
		);
	}
	Ok(())
}

fn bench_mixed_dense_fallback(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	// (A AND B) DenseFallback: dense scalar over both atoms across all
	// rows, then boolean combine. Plus the (A OR B) DenseFallback
	// shape, which encoded-narrowing of A cannot help safely.
	let atom_a = int_atom(PredicateAtomOp::Lt, 13_i64);
	let coercion_a =
		coerce_literal(atom_a.literal.as_ref(), &fixture.data_type).expect("bench coerce A");
	let atom_b = int_atom(PredicateAtomOp::Gte, 100_i64);
	let coercion_b =
		coerce_literal(atom_b.literal.as_ref(), &fixture.data_type).expect("bench coerce B");
	for (label, op_label, op) in [
		("mixed_dense_fallback_and", "AND", true),
		("mixed_dense_fallback_or", "OR", false),
	] {
		let elapsed = bench(config.iterations, || {
			let decoded = dense_decode(
				black_box(&fixture.encoded),
				black_box(&fixture.data_type),
				black_box(&fixture.cascade),
				black_box(&fixture.encoding),
			)
			.expect("bench dense decode");
			let a = scalar_compare(&decoded, &fixture.data_type, &atom_a, &coercion_a);
			let b = scalar_compare(&decoded, &fixture.data_type, &atom_b, &coercion_b);
			let combined = a
				.into_iter()
				.zip(b)
				.map(|(left, right)| if op { left && right } else { left || right })
				.collect::<Vec<_>>();
			black_box(combined);
		});
		emit_row(
			label,
			&format!("{}/{op_label}", fixture.name),
			fixture.rows,
			fixture.encoded.len() as u64,
			config.iterations,
			elapsed,
		);
	}
	Ok(())
}

fn bench_mask_completeness(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	let coercion = coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
		.expect("bench literal coerce");
	let bitmap_a = evaluate(
		&fixture.encoded,
		&fixture.encoding,
		&fixture.cascade,
		&fixture.atom,
		&coercion,
	)
	.expect("bench bitmap_a");
	let bitmap_b = bitmap_a.not().expect("bench bitmap_b");

	// Bitmap NOT
	let elapsed = bench(config.iterations, || {
		let inverted = black_box(&bitmap_a).not().expect("bench bitmap not");
		black_box(inverted);
	});
	emit_row(
		"mask_algebra_bitmap_not",
		fixture.name,
		fixture.rows,
		bitmap_a.byte_len() as u64,
		config.iterations,
		elapsed,
	);

	// Bitmap × Bitmap OR (kept distinct from existing bitmap_or row to
	// stress a non-self-inverse pair).
	let elapsed = bench(config.iterations, || {
		let combined = black_box(&bitmap_a)
			.or(black_box(&bitmap_b))
			.expect("bench bitmap or");
		black_box(combined.popcount().expect("bench popcount"));
	});
	emit_row(
		"mask_algebra_bitmap_pair_or",
		fixture.name,
		fixture.rows,
		bitmap_a.byte_len() as u64,
		config.iterations,
		elapsed,
	);

	// Construct a synthetic Runs mask for run-shape combinatorics.
	let runs_mask = synth_runs_mask(fixture.rows);
	let runs_b = runs_mask.not().expect("bench runs_b");

	// Runs × Runs AND
	let elapsed = bench(config.iterations, || {
		let combined = black_box(&runs_mask)
			.and(black_box(&runs_b))
			.expect("bench runs and runs");
		black_box(combined.popcount().expect("bench popcount"));
	});
	emit_row(
		"mask_algebra_runs_and_runs",
		fixture.name,
		fixture.rows,
		runs_mask.byte_len() as u64,
		config.iterations,
		elapsed,
	);

	// Runs NOT
	let elapsed = bench(config.iterations, || {
		let inverted = black_box(&runs_mask).not().expect("bench runs not");
		black_box(inverted);
	});
	emit_row(
		"mask_algebra_runs_not",
		fixture.name,
		fixture.rows,
		runs_mask.byte_len() as u64,
		config.iterations,
		elapsed,
	);

	// Densification: V1 algebra always exits as Bitmap regardless of
	// input shape. This row times the Runs -> Bitmap conversion path
	// implicit in every Runs-shaped algebra invocation.
	let elapsed = bench(config.iterations, || {
		let densified = black_box(&runs_mask)
			.and(black_box(&bitmap_a))
			.expect("bench densify");
		black_box(densified);
	});
	emit_row(
		"mask_algebra_densification",
		fixture.name,
		fixture.rows,
		runs_mask.byte_len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_config_kernel_disabled(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	// Kernel kill switch: with the BitPacked evaluator disabled in
	// EncodingComputeConfig, the planner short-circuits to DenseFallback
	// before any encoded eval runs. The bench shape mirrors
	// predicate_classify_then_fallback exactly with one additional
	// config-set check inserted between the cascade-kind lookup and the
	// dense path; the delta vs. classify_then_fallback bounds the
	// kill-switch's incremental overhead.
	let mut config_disabled = EncodingComputeConfig {
		enabled: true,
		..Default::default()
	};
	config_disabled
		.enabled_predicate_evaluators
		.remove(&EncodingComputeEvaluatorKind::BitPacked);

	let elapsed = bench(config.iterations, || {
		let kind = evaluator_kind_for_cascade(black_box(&fixture.cascade));
		let allowed = kind
			.map(|k| black_box(&config_disabled).predicate_evaluator_enabled(black_box(k)))
			.unwrap_or(false);
		let coercion = coerce_literal(
			black_box(fixture.atom.literal.as_ref()),
			black_box(&fixture.data_type),
		);
		black_box((kind, allowed, coercion.as_ref()));
		let coercion = coercion.expect("bench coercion");
		let decoded = dense_decode(
			black_box(&fixture.encoded),
			black_box(&fixture.data_type),
			black_box(&fixture.cascade),
			black_box(&fixture.encoding),
		)
		.expect("bench dense decode");
		let selected = scalar_compare(&decoded, &fixture.data_type, &fixture.atom, &coercion);
		black_box(selected);
	});
	emit_row(
		"predicate_kernel_disabled_fallback",
		fixture.name,
		fixture.rows,
		fixture.encoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn synth_runs_mask(rows: usize) -> EncodedRowMask {
	use wendelon::schema::{RecordRowIndex, RowCount};
	use wendelon::storage::encoding::compute::SelectionRun;
	let run_len = 64_u32;
	let mut runs = Vec::new();
	let mut emitted = 0_u32;
	let mut selected = true;
	let total = u32::try_from(rows).expect("synth_runs_mask rows fit u32");
	while emitted < total {
		let remaining = total - emitted;
		let len = run_len.min(remaining);
		runs.push(SelectionRun {
			first_row: RecordRowIndex::new(emitted),
			row_count: RowCount::new(u64::from(len)),
			selected,
		});
		emitted += len;
		selected = !selected;
	}
	EncodedRowMask::Runs {
		rows: RowCount::new(u64::from(total)),
		runs,
	}
}

fn bench_predicate_classify_then_fallback(
	config: &BenchConfig,
	fixture: &Fixture,
) -> std::io::Result<()> {
	let elapsed = bench(config.iterations, || {
		let kind = evaluator_kind_for_cascade(black_box(&fixture.cascade));
		let coercion = coerce_literal(
			black_box(fixture.atom.literal.as_ref()),
			black_box(&fixture.data_type),
		);
		black_box((kind, coercion.as_ref()));
		let coercion = coercion.expect("bench coercion");
		let decoded = dense_decode(
			black_box(&fixture.encoded),
			black_box(&fixture.data_type),
			black_box(&fixture.cascade),
			black_box(&fixture.encoding),
		)
		.expect("bench dense decode");
		let selected = scalar_compare(&decoded, &fixture.data_type, &fixture.atom, &coercion);
		black_box(selected);
	});
	emit_row(
		"predicate_classify_then_fallback",
		fixture.name,
		fixture.rows,
		fixture.encoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn dense_decode(
	encoded: &[u8],
	data_type: &DataType,
	cascade: &EncodingCascade,
	encoding: &BufferEncoding,
) -> Result<Vec<u8>, wendelon::storage::encoding::EncodingRejectCode> {
	if encoding.page_end_offsets.is_empty() {
		return decode_buffer(
			encoded,
			data_type,
			cascade,
			encoding.decoded_byte_len,
			encoding.decoded_value_count,
		);
	}
	let pages = expand_page_descriptors(encoding)?;
	let bytes_per_value = match data_type {
		DataType::UInt8 | DataType::Int8 => 1,
		DataType::UInt16 | DataType::Int16 => 2,
		DataType::UInt32 | DataType::Int32 => 4,
		DataType::UInt64 | DataType::Int64 => 8,
		_ => {
			return decode_buffer(
				encoded,
				data_type,
				cascade,
				encoding.decoded_byte_len,
				encoding.decoded_value_count,
			);
		}
	};
	let mut out = Vec::with_capacity(encoding.decoded_byte_len.get() as usize);
	for page in pages {
		let start = page.byte_range.start.get() as usize;
		let len = page.byte_range.len.get() as usize;
		let end = start + len;
		let expected_bytes = page.value_count.get() as usize * bytes_per_value;
		let decoded_page = decode_page(
			&encoded[start..end],
			data_type,
			cascade,
			RecordBodyByteLen::new(expected_bytes as u64),
			page.value_count,
		)?;
		out.extend_from_slice(&decoded_page);
	}
	Ok(out)
}

fn bench_predicate_exact_legacy(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	let coercion = coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
		.expect("bench literal coerce");
	let elapsed = bench(config.iterations, || {
		let mask = evaluate(
			black_box(&fixture.encoded),
			black_box(&fixture.encoding),
			black_box(&fixture.cascade),
			black_box(&fixture.atom),
			black_box(&coercion),
		)
		.expect("bench predicate eval");
		black_box(mask);
	});
	emit_row(
		"predicate_exact",
		fixture.name,
		fixture.rows,
		fixture.encoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_mask_algebra(config: &BenchConfig, fixtures: &[Fixture]) -> std::io::Result<()> {
	let bp = fixtures
		.iter()
		.find(|f| f.name == "bp_u8")
		.expect("bp_u8 fixture present");
	let bp_coercion = coerce_literal(bp.atom.literal.as_ref(), &bp.data_type).expect("coerce");
	let bitmap_a = evaluate(
		&bp.encoded,
		&bp.encoding,
		&bp.cascade,
		&bp.atom,
		&bp_coercion,
	)
	.expect("bench bp mask");
	let bitmap_b = bitmap_a.not().expect("bench bp not");
	let bp_rows = bp.rows;
	let all_mask = EncodedRowMask::All {
		rows: RowCount::new(bp_rows as u64),
	};
	let none_mask = EncodedRowMask::None {
		rows: RowCount::new(bp_rows as u64),
	};
	let runs_mask = if let Some(rle) = fixtures.iter().find(|f| f.name == "rle_bitpacked") {
		let rle_coercion =
			coerce_literal(rle.atom.literal.as_ref(), &rle.data_type).expect("coerce");
		Some(
			evaluate(
				&rle.encoded,
				&rle.encoding,
				&rle.cascade,
				&rle.atom,
				&rle_coercion,
			)
			.expect("bench rle mask"),
		)
	} else {
		None
	};

	let elapsed = bench(config.iterations, || {
		let combined = black_box(&bitmap_a)
			.and(black_box(&bitmap_b))
			.expect("bench bitmap and");
		black_box(combined.popcount().expect("bench popcount"));
	});
	emit_row(
		"mask_algebra_bitmap_and",
		"bp_u8",
		bp_rows,
		bitmap_a.byte_len() as u64,
		config.iterations,
		elapsed,
	);
	let elapsed = bench(config.iterations, || {
		let combined = black_box(&bitmap_a)
			.or(black_box(&bitmap_b))
			.expect("bench bitmap or");
		black_box(combined.popcount().expect("bench popcount"));
	});
	emit_row(
		"mask_algebra_bitmap_or",
		"bp_u8",
		bp_rows,
		bitmap_a.byte_len() as u64,
		config.iterations,
		elapsed,
	);
	let elapsed = bench(config.iterations, || {
		let combined = black_box(&bitmap_a)
			.and(black_box(&all_mask))
			.expect("bench all and");
		black_box(combined.popcount().expect("bench popcount"));
	});
	emit_row(
		"mask_algebra_all_and",
		"bp_u8",
		bp_rows,
		bitmap_a.byte_len() as u64,
		config.iterations,
		elapsed,
	);
	let elapsed = bench(config.iterations, || {
		let combined = black_box(&bitmap_a)
			.or(black_box(&none_mask))
			.expect("bench none or");
		black_box(combined.popcount().expect("bench popcount"));
	});
	emit_row(
		"mask_algebra_none_or",
		"bp_u8",
		bp_rows,
		bitmap_a.byte_len() as u64,
		config.iterations,
		elapsed,
	);
	if let Some(runs) = runs_mask {
		let elapsed = bench(config.iterations, || {
			let combined = black_box(&runs)
				.and(black_box(&bitmap_a))
				.expect("bench runs and");
			black_box(combined.popcount().expect("bench popcount"));
		});
		emit_row(
			"mask_algebra_runs_and_bitmap",
			"rle_bitpacked",
			bp_rows,
			runs.byte_len() as u64,
			config.iterations,
			elapsed,
		);
	}

	// Legacy alias preserved for the architectural guard token check.
	let elapsed = bench(config.iterations, || {
		let combined = black_box(&bitmap_a)
			.and(black_box(&bitmap_b))
			.expect("bench mask and");
		black_box(combined.popcount().expect("bench popcount"));
	});
	emit_row(
		"mask_algebra",
		"bp_u8",
		bp_rows,
		bitmap_a.byte_len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_aggregate_count_star(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	let request = aggregate_request_count_star();
	let context = aggregate_context_for(fixture);
	let coercion = coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
		.expect("bench literal coerce");
	let mask = evaluate(
		&fixture.encoded,
		&fixture.encoding,
		&fixture.cascade,
		&fixture.atom,
		&coercion,
	)
	.expect("bench predicate eval");
	let elapsed = bench(config.iterations, || {
		let partials = try_fold_encoded_aggregate_request(
			black_box(&request),
			black_box(&context),
			Some(black_box(&mask)),
		)
		.expect("bench aggregate fold")
		.expect("bench aggregate supported");
		black_box(partials);
	});
	emit_row(
		"aggregate_count_star",
		fixture.name,
		fixture.rows,
		fixture.decoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_aggregate_value_kernel(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	let kind = fixture
		.aggregate_kind
		.expect("aggregate_value_kernel requires aggregate_kind");
	let (label, tag) = match kind {
		AggregateKind::UnsignedSum => {
			("aggregate_unsigned_sum", AggregateFunctionTag::UNSIGNED_SUM)
		}
		AggregateKind::SignedSum => ("aggregate_signed_sum", AggregateFunctionTag::SIGNED_SUM),
	};
	let request = aggregate_request_count_star_plus(tag);
	let context = aggregate_context_for(fixture);
	let coercion = coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
		.expect("bench literal coerce");
	let mask = evaluate(
		&fixture.encoded,
		&fixture.encoding,
		&fixture.cascade,
		&fixture.atom,
		&coercion,
	)
	.expect("bench predicate eval");
	let elapsed = bench(config.iterations, || {
		let partials = try_fold_encoded_aggregate_request(
			black_box(&request),
			black_box(&context),
			Some(black_box(&mask)),
		)
		.expect("bench aggregate fold");
		black_box(partials);
	});
	emit_row(
		label,
		fixture.name,
		fixture.rows,
		fixture.decoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_aggregate_fold_legacy(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	let request = aggregate_request_count_star_plus(AggregateFunctionTag::UNSIGNED_SUM);
	let context = aggregate_context_for(fixture);
	let coercion = coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
		.expect("bench literal coerce");
	let mask = evaluate(
		&fixture.encoded,
		&fixture.encoding,
		&fixture.cascade,
		&fixture.atom,
		&coercion,
	)
	.expect("bench predicate eval");
	let elapsed = bench(config.iterations, || {
		let partials = try_fold_encoded_aggregate_request(
			black_box(&request),
			black_box(&context),
			Some(black_box(&mask)),
		)
		.expect("bench aggregate fold")
		.expect("bench aggregate supported");
		black_box(partials);
	});
	emit_row(
		"aggregate_fold",
		fixture.name,
		fixture.rows,
		fixture.decoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_selectivity_sweep(config: &BenchConfig, rows: usize) -> std::io::Result<()> {
	let fixture = bp_u8_fixture(rows)?;
	let coercion =
		coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type).expect("coerce");
	for (label, literal) in [
		("p001", 3_i64),   // ~1.2%
		("p005", 13_i64),  // ~5.2%
		("p025", 63_i64),  // ~25.1%
		("p050", 126_i64), // ~50.2%
	] {
		let atom = int_atom(PredicateAtomOp::Lt, literal);
		let coercion_local = coerce_literal(atom.literal.as_ref(), &fixture.data_type)
			.unwrap_or_else(|| coercion.clone());
		let elapsed = bench(config.iterations, || {
			let mask = evaluate(
				black_box(&fixture.encoded),
				black_box(&fixture.encoding),
				black_box(&fixture.cascade),
				black_box(&atom),
				black_box(&coercion_local),
			)
			.expect("bench selectivity sweep");
			black_box(mask);
		});
		emit_row(
			"predicate_selectivity_sweep",
			&format!("bp_u8/{label}"),
			fixture.rows,
			fixture.encoded.len() as u64,
			config.iterations,
			elapsed,
		);
	}
	Ok(())
}

fn aggregate_context_for(fixture: &Fixture) -> EncodedAggregateContext {
	let context = EncodedAggregateContext::new(RowCount::new(fixture.rows as u64));
	if matches!(
		fixture.cascade,
		EncodingCascade::BitPacked { .. }
			| EncodingCascade::Dictionary { .. }
			| EncodingCascade::Delta { .. }
			| EncodingCascade::Rle { .. }
	) {
		return context
			.with_native_encoded_column(
				0,
				fixture.aggregate_column_dtype.clone(),
				&fixture.encoded,
				&fixture.encoding,
				&fixture.cascade,
			)
			.expect("bench aggregate native encoded context");
	}
	context
		.with_encoded_column(
			0,
			fixture.aggregate_column_dtype.clone(),
			&fixture.encoded,
			&fixture.encoding,
			&fixture.cascade,
		)
		.expect("bench aggregate decoded context")
}

fn aggregate_request_count_star() -> AggregateRequest {
	aggregate_request(vec![AggregateExpression::count_star()])
}

fn aggregate_request_count_star_plus(tag: AggregateFunctionTag) -> AggregateRequest {
	aggregate_request(vec![
		AggregateExpression::count_star(),
		AggregateExpression {
			tag,
			column_ordinal: 0,
			parameter_block: Vec::new(),
		},
	])
}

fn aggregate_request(aggregates: Vec<AggregateExpression>) -> AggregateRequest {
	AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(58),
		tenant_id: TenantId::new(58),
		table_object_id: TableObjectId::new([0x58; 16]),
		schema_generation: 1,
		logical_schema_fingerprint: bench_logical_fingerprint(),
		consistency: Consistency::SERIALIZABLE,
		min_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		},
		as_of_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		},
		locality_ranges: Vec::new(),
		predicate_program: None,
		aggregates,
		budgets: AggregateBudgets::default(),
	}
}

fn bench_logical_fingerprint() -> LogicalSchemaFingerprint {
	postcard::from_bytes(&[0x58; 32]).expect("bench logical fingerprint")
}

fn scalar_compare(
	decoded: &[u8],
	data_type: &DataType,
	atom: &PredicateAtom,
	_coercion: &wendelon::storage::encoding::compute::LiteralCoercion,
) -> Vec<bool> {
	let literal = match atom.literal.as_ref().expect("atom literal") {
		ColumnPredicateLiteral::Int(value) => *value,
		_ => panic!("dense baseline supports integer literals only"),
	};
	let width = value_byte_width(data_type);
	let count = decoded.len() / width;
	let mut out = Vec::with_capacity(count);
	for chunk in decoded.chunks_exact(width) {
		let value = read_signed_le(chunk, data_type);
		let selected = match atom.op {
			PredicateAtomOp::Eq => value == literal,
			PredicateAtomOp::Neq => value != literal,
			PredicateAtomOp::Lt => value < literal,
			PredicateAtomOp::Lte => value <= literal,
			PredicateAtomOp::Gt => value > literal,
			PredicateAtomOp::Gte => value >= literal,
			_ => panic!("dense baseline supports value atoms only"),
		};
		out.push(selected);
	}
	out
}

fn fsst_dense_string_compare(decoded: &[u8], offsets: &[i32], atom: &PredicateAtom) -> Vec<bool> {
	let literal = atom.literal.as_ref().expect("FSST atom literal");
	let literal_values = match literal {
		ColumnPredicateLiteral::Str(value) => vec![value.as_slice()],
		ColumnPredicateLiteral::List(values) => values
			.iter()
			.map(|value| match value {
				ColumnPredicateLiteral::Str(value) => value.as_slice(),
				_ => panic!("FSST dense baseline supports string lists only"),
			})
			.collect::<Vec<_>>(),
		_ => panic!("FSST dense baseline supports string literals only"),
	};
	offsets
		.windows(2)
		.map(|window| {
			let start = window[0] as usize;
			let end = window[1] as usize;
			let value = &decoded[start..end];
			match atom.op {
				PredicateAtomOp::Eq => value == literal_values[0],
				PredicateAtomOp::Neq => value != literal_values[0],
				PredicateAtomOp::InList => literal_values.contains(&value),
				PredicateAtomOp::Lt => value < literal_values[0],
				PredicateAtomOp::Lte => value <= literal_values[0],
				PredicateAtomOp::Gt => value > literal_values[0],
				PredicateAtomOp::Gte => value >= literal_values[0],
				_ => panic!("FSST dense baseline supports value diagnostics only"),
			}
		})
		.collect()
}

fn value_byte_width(data_type: &DataType) -> usize {
	match data_type {
		DataType::UInt8 | DataType::Int8 => 1,
		DataType::UInt16 | DataType::Int16 => 2,
		DataType::UInt32 | DataType::Int32 => 4,
		DataType::UInt64 | DataType::Int64 => 8,
		_ => 1,
	}
}

fn ratio_micros(numerator: u64, denominator: u64) -> u64 {
	if denominator == 0 {
		u64::MAX
	} else {
		(numerator as f64 / denominator as f64 * 1_000_000.0) as u64
	}
}

fn duration_ratio_micros(numerator: Duration, denominator: Duration) -> u64 {
	let denominator = denominator.as_secs_f64();
	if denominator == 0.0 {
		u64::MAX
	} else {
		(numerator.as_secs_f64() / denominator * 1_000_000.0) as u64
	}
}

fn duration_overhead_micros(numerator: Duration, denominator: Duration) -> u64 {
	duration_ratio_micros(numerator, denominator).saturating_sub(1_000_000)
}

fn encoded_mask_digest(mask: &EncodedRowMask) -> String {
	let values = mask.to_bool_vec().expect("bench mask digest");
	let mut hasher = blake3::Hasher::new();
	for value in values {
		hasher.update(&[u8::from(value)]);
	}
	hasher.finalize().to_hex().to_string()
}

fn read_signed_le(chunk: &[u8], data_type: &DataType) -> i64 {
	match data_type {
		DataType::UInt8 => chunk[0] as i64,
		DataType::Int8 => chunk[0] as i8 as i64,
		DataType::UInt16 => u16::from_le_bytes([chunk[0], chunk[1]]) as i64,
		DataType::Int16 => i16::from_le_bytes([chunk[0], chunk[1]]) as i64,
		DataType::UInt32 => u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as i64,
		DataType::Int32 => i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as i64,
		DataType::UInt64 => u64::from_le_bytes([
			chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
		]) as i64,
		DataType::Int64 => i64::from_le_bytes([
			chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
		]),
		_ => 0,
	}
}

fn bench_aggregate_min_max(config: &BenchConfig, fixture: &Fixture) -> std::io::Result<()> {
	let coercion = coerce_literal(fixture.atom.literal.as_ref(), &fixture.data_type)
		.expect("bench literal coerce");
	let mask = evaluate(
		&fixture.encoded,
		&fixture.encoding,
		&fixture.cascade,
		&fixture.atom,
		&coercion,
	)
	.expect("bench predicate eval");
	let context = aggregate_context_for(fixture);
	for (label, tag) in [
		("aggregate_min", AggregateFunctionTag::MIN),
		("aggregate_max", AggregateFunctionTag::MAX),
	] {
		let request = aggregate_request(vec![AggregateExpression {
			tag,
			column_ordinal: 0,
			parameter_block: Vec::new(),
		}]);
		let elapsed = bench(config.iterations, || {
			let partials = try_fold_encoded_aggregate_request(
				black_box(&request),
				black_box(&context),
				Some(black_box(&mask)),
			)
			.expect("bench aggregate fold");
			black_box(partials);
		});
		emit_row(
			label,
			fixture.name,
			fixture.rows,
			fixture.decoded.len() as u64,
			config.iterations,
			elapsed,
		);
	}
	Ok(())
}

fn bench_aggregate_selectivity_sweep(
	config: &BenchConfig,
	fixture: &Fixture,
) -> std::io::Result<()> {
	let context = aggregate_context_for(fixture);
	for (label, literal) in [
		("p001", 3_i64),
		("p005", 13_i64),
		("p025", 63_i64),
		("p050", 126_i64),
	] {
		let atom = int_atom(PredicateAtomOp::Lt, literal);
		let coercion =
			coerce_literal(atom.literal.as_ref(), &fixture.data_type).expect("bench coerce");
		let mask = evaluate(
			&fixture.encoded,
			&fixture.encoding,
			&fixture.cascade,
			&atom,
			&coercion,
		)
		.expect("bench predicate eval");
		for (kind_label, request_builder) in [
			(
				"count_star",
				aggregate_request(vec![AggregateExpression::count_star()]),
			),
			(
				"unsigned_sum",
				aggregate_request(vec![AggregateExpression {
					tag: AggregateFunctionTag::UNSIGNED_SUM,
					column_ordinal: 0,
					parameter_block: Vec::new(),
				}]),
			),
		] {
			let elapsed = bench(config.iterations, || {
				let partials = try_fold_encoded_aggregate_request(
					black_box(&request_builder),
					black_box(&context),
					Some(black_box(&mask)),
				)
				.expect("bench aggregate fold");
				black_box(partials);
			});
			emit_row(
				&format!("aggregate_{kind_label}_selectivity_sweep"),
				&format!("{}/{label}", fixture.name),
				fixture.rows,
				fixture.decoded.len() as u64,
				config.iterations,
				elapsed,
			);
		}
	}
	Ok(())
}

#[derive(Clone, Copy, Debug)]
enum PipelineShape {
	LowCard,
	Monotonic,
	BitpackLow,
	RunShaped,
}

impl PipelineShape {
	fn label(self) -> &'static str {
		match self {
			Self::LowCard => "low_card",
			Self::Monotonic => "monotonic",
			Self::BitpackLow => "bitpack_low",
			Self::RunShaped => "run_shaped",
		}
	}
}

struct PipelineFixture {
	frame: Vec<u8>,
	descriptor: TableSchemaDescriptor,
	output_only: ColumnFetchRequirement,
	output_with_predicate: ColumnFetchRequirement,
	rows: usize,
	shape_label: &'static str,
}

fn build_pipeline_fixture_shape(
	rows: usize,
	shape: PipelineShape,
) -> std::io::Result<PipelineFixture> {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("value", DataType::Int64, false),
	]));
	let ids: Vec<i64> = match shape {
		PipelineShape::LowCard => (0..rows).map(|row| (row % 8) as i64).collect(),
		PipelineShape::Monotonic => (0..rows as i64).collect(),
		PipelineShape::BitpackLow => (0..rows).map(|row| ((row * 7) % 1024) as i64).collect(),
		PipelineShape::RunShaped => (0..rows).map(|row| (row / 64) as i64 % 32).collect(),
	};
	let values: Vec<i64> = (0..rows).map(|row| row as i64).collect();
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from(ids)) as ArrayRef,
			Arc::new(Int64Array::from(values)) as ArrayRef,
		],
	)
	.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x58; 16]),
		1,
		"bench",
		"encoding_compute_pipeline",
		schema.as_ref(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(1024, 1024, 8 * 1024 * 1024)
				.expect("bench multi-row layout"),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
	let source: Vec<MultiRowSourceRow> = (0..rows)
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
			partition: None,
			table_object_id: Some(TableObjectId::new([0x58; 16])),
		})
		.collect();
	let row_group_config = RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(1024),
		RowGroupRowCount::new(1024),
		RowCount::new(1024),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true);
	let encoding_config = EncodingWriterConfig {
		enabled: true,
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		..EncodingWriterConfig::default()
	};
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&source,
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config,
		&encoding_config,
	)
	.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
	let output_only = ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([1]),
		..ColumnFetchRequirement::default()
	};
	let output_with_predicate = ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([1]),
		predicate_columns: ColumnOrdinalSet::from_columns([0]),
		..ColumnFetchRequirement::default()
	};
	Ok(PipelineFixture {
		frame: encoded.frame,
		descriptor,
		output_only,
		output_with_predicate,
		rows,
		shape_label: shape.label(),
	})
}

fn bench_pipeline(config: &BenchConfig, rows: usize) -> std::io::Result<()> {
	// The MultiRow layout caps records at 8 MiB; the pipeline fixture's
	// dual-Int64 schema at 1M rows overflows that. The bench hits
	// 8K and 65K for end-to-end shape; the per-fixture predicate /
	// mask / aggregate rows already cover 1M for substrate floor.
	if rows > 65_536 {
		return Ok(());
	}
	for shape in [
		PipelineShape::LowCard,
		PipelineShape::Monotonic,
		PipelineShape::BitpackLow,
		PipelineShape::RunShaped,
	] {
		let fixture = build_pipeline_fixture_shape(rows, shape)?;
		// Only bench pipeline_full + neutrality on the LowCard fixture
		// to keep iteration cost bounded; the multi-shape coverage is
		// for the cascade-selection observability surfaced through
		// bench_provider_leak.
		if matches!(shape, PipelineShape::LowCard) {
			bench_pipeline_full(config, &fixture, "output_only", &fixture.output_only)?;
			bench_pipeline_full(
				config,
				&fixture,
				"with_predicate",
				&fixture.output_with_predicate,
			)?;
			bench_pipeline_neutrality(&fixture)?;
		}
		bench_provider_leak(&fixture)?;
	}
	Ok(())
}

fn bench_pipeline_neutrality(fixture: &PipelineFixture) -> std::io::Result<()> {
	// Run the pipeline twice with EncodingComputeConfig.enabled = false
	// vs. true on the same record body. The pipeline path uses public
	// storage entries that don't consult EncodingComputeConfig, so the
	// two outputs are byte-identical by construction. This row records
	// the assertion at bench layer; the real neutrality claim sits at
	// the test layer (94 integration tests across ADR 056 / 057 / 040 /
	// 052 / 055 with COCD off).
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(58),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: fixture.frame.len() as u64,
		},
		fixture.frame.as_slice(),
		&fixture.descriptor,
	)
	.expect("neutrality parse directory");
	let page_directory = directory
		.page_directory
		.as_ref()
		.expect("neutrality page directory");
	let selected_groups: Vec<RowGroupIndex> = page_directory
		.row_groups
		.iter()
		.map(|group| group.row_group_index)
		.collect();
	let ranges = selected_row_group_record_body_ranges(
		&directory,
		page_directory,
		&fixture.output_only,
		&selected_groups,
	)
	.expect("neutrality select ranges");
	let plaintext_ranges: Vec<TieredPlaintextRange> = ranges
		.iter()
		.map(|range| TieredPlaintextRange::new(range.start, range.len))
		.collect();
	let extracts: Vec<Vec<u8>> = ranges
		.iter()
		.map(|range| {
			let start = range.start.get() as usize;
			let end = range.end().expect("range end") as usize;
			fixture.frame[start..end].to_vec()
		})
		.collect();
	let projected_a = decode_projected_row_groups_from_plaintext_ranges(
		&directory,
		page_directory,
		&fixture.output_only,
		&plaintext_ranges,
		&extracts,
		&selected_groups,
	)
	.expect("neutrality decode A");
	let projected_b = decode_projected_row_groups_from_plaintext_ranges(
		&directory,
		page_directory,
		&fixture.output_only,
		&plaintext_ranges,
		&extracts,
		&selected_groups,
	)
	.expect("neutrality decode B");
	let rows_a = projected_a.batch.num_rows();
	let rows_b = projected_b.batch.num_rows();
	let cols_a = projected_a.batch.num_columns();
	let cols_b = projected_b.batch.num_columns();
	let identical = rows_a == rows_b && cols_a == cols_b;
	println!(
		"encoding_compute_pipeline_neutrality scenario=config_toggle rows={} byte_identical={} (note: pipeline path bypasses EncodingComputeConfig — neutrality is structural at this layer; 94 integration tests assert byte-identity end-to-end with config.enabled=false vs. true)",
		fixture.rows, identical,
	);
	Ok(())
}

fn bench_pipeline_full(
	config: &BenchConfig,
	fixture: &PipelineFixture,
	scenario_label: &str,
	requirement: &ColumnFetchRequirement,
) -> std::io::Result<()> {
	let elapsed = bench(config.iterations, || {
		let directory = parse_record_buffer_directory(
			TieredRecordId {
				slab_id: SlabId::new(58),
				record_offset: RecordBodyByteOffset::ZERO,
			},
			PlaintextByteRange {
				start: 0,
				len: fixture.frame.len() as u64,
			},
			black_box(fixture.frame.as_slice()),
			black_box(&fixture.descriptor),
		)
		.expect("bench parse directory");
		let page_directory = directory
			.page_directory
			.as_ref()
			.expect("bench page directory");
		let selected_groups: Vec<RowGroupIndex> = page_directory
			.row_groups
			.iter()
			.map(|group| group.row_group_index)
			.collect();
		let ranges = selected_row_group_record_body_ranges(
			&directory,
			page_directory,
			black_box(requirement),
			&selected_groups,
		)
		.expect("bench select ranges");
		let plaintext_ranges: Vec<TieredPlaintextRange> = ranges
			.iter()
			.map(|range| TieredPlaintextRange::new(range.start, range.len))
			.collect();
		let extracts: Vec<Vec<u8>> = ranges
			.iter()
			.map(|range| {
				let start = range.start.get() as usize;
				let end = range.end().expect("bench range end") as usize;
				fixture.frame[start..end].to_vec()
			})
			.collect();
		let projected = decode_projected_row_groups_from_plaintext_ranges(
			&directory,
			page_directory,
			black_box(requirement),
			&plaintext_ranges,
			&extracts,
			&selected_groups,
		)
		.expect("bench decode projected");
		black_box(projected.batch.num_rows());
	});
	emit_row(
		"pipeline_full",
		scenario_label,
		fixture.rows,
		fixture.frame.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_provider_leak(fixture: &PipelineFixture) -> std::io::Result<()> {
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(58),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: fixture.frame.len() as u64,
		},
		fixture.frame.as_slice(),
		&fixture.descriptor,
	)
	.expect("provider leak parse directory");
	let page_directory = directory
		.page_directory
		.as_ref()
		.expect("provider leak page directory");
	let encoding_directory = directory
		.encoding_directory
		.as_ref()
		.expect("provider leak encoding directory");
	let selected_groups: Vec<RowGroupIndex> = page_directory
		.row_groups
		.iter()
		.map(|group| group.row_group_index)
		.collect();

	for (label, requirement) in [
		("output_only", &fixture.output_only),
		("with_predicate", &fixture.output_with_predicate),
	] {
		let ranges = selected_row_group_record_body_ranges(
			&directory,
			page_directory,
			requirement,
			&selected_groups,
		)
		.expect("provider leak select ranges");
		let dense_bytes: u64 = ranges.iter().map(|range| range.len.get()).sum();
		let dense_count = ranges.len();
		println!(
			"encoding_compute_provider_leak scenario=adr057_dense shape={} path={label} rows={} ranges={} bytes={} frame_bytes={}",
			fixture.shape_label,
			fixture.rows,
			dense_count,
			dense_bytes,
			fixture.frame.len(),
		);
	}

	// COCD encoded slim: sum the encoded byte lengths of predicate-column
	// buffers across all row groups. Dictionary / BitPacked / Rle / Delta
	// cascades produce encoded byte lengths smaller than the decoded byte
	// length they replace; the delta vs. the dense ranges above is the
	// upper bound on per-record bytes a COCD reader avoids fetching when
	// it targets only encoded predicate slices.
	let predicate_buffer_ordinals: std::collections::BTreeSet<u32> = encoding_directory
		.buffers
		.iter()
		.filter(|encoding| encoding.ipc_column_ordinal == IpcColumnOrdinal::ZERO)
		.map(|encoding| encoding.ipc_buffer_ordinal.get())
		.collect();
	let cocd_encoded_bytes: u64 = encoding_directory
		.buffers
		.iter()
		.filter(|encoding| encoding.ipc_column_ordinal == IpcColumnOrdinal::ZERO)
		.map(|encoding| encoding.encoded_range.len.get())
		.sum();
	let cocd_decoded_bytes: u64 = encoding_directory
		.buffers
		.iter()
		.filter(|encoding| encoding.ipc_column_ordinal == IpcColumnOrdinal::ZERO)
		.map(|encoding| encoding.decoded_byte_len.get())
		.sum();
	let cocd_count = predicate_buffer_ordinals.len() * selected_groups.len();
	let cascades_picked: std::collections::BTreeMap<&'static str, usize> = encoding_directory
		.buffers
		.iter()
		.filter(|encoding| encoding.ipc_column_ordinal == IpcColumnOrdinal::ZERO)
		.filter_map(|encoding| {
			encoding_directory
				.cascades
				.get(encoding.cascade_index.get() as usize)
				.map(|cascade| cascade.kind().as_str())
		})
		.fold(std::collections::BTreeMap::new(), |mut acc, kind| {
			*acc.entry(kind).or_insert(0) += 1;
			acc
		});
	let cascades_summary = cascades_picked
		.iter()
		.map(|(kind, count)| format!("{kind}:{count}"))
		.collect::<Vec<_>>()
		.join(",");
	println!(
		"encoding_compute_provider_leak scenario=cocd_encoded_slim shape={} path=predicate_only rows={} buffers={} encoded_bytes={} decoded_bytes={} frame_bytes={} ratio_encoded_vs_decoded={:.3} cascades={}",
		fixture.shape_label,
		fixture.rows,
		cocd_count,
		cocd_encoded_bytes,
		cocd_decoded_bytes,
		fixture.frame.len(),
		if cocd_decoded_bytes > 0 {
			cocd_encoded_bytes as f64 / cocd_decoded_bytes as f64
		} else {
			0.0
		},
		cascades_summary,
	);
	Ok(())
}

#[derive(Clone, Copy, Debug)]
enum EncodedValidityNullShape {
	Clustered5,
	Clustered1,
	AllValid,
	AllNull,
	Random50,
	AlternatingBit,
}

impl EncodedValidityNullShape {
	fn name(self) -> &'static str {
		match self {
			Self::Clustered5 => "clustered_5p",
			Self::Clustered1 => "clustered_1p",
			Self::AllValid => "all_valid",
			Self::AllNull => "all_null",
			Self::Random50 => "random_50p",
			Self::AlternatingBit => "alternating_bit",
		}
	}

	fn is_valid(self, row: usize, rows: usize) -> bool {
		match self {
			Self::Clustered5 => row >= rows / 20,
			Self::Clustered1 => row >= rows / 100,
			Self::AllValid => true,
			Self::AllNull => false,
			Self::Random50 => {
				let mixed = (row as u64)
					.wrapping_mul(6_364_136_223_846_793_005)
					.wrapping_add(0x9e37_79b9_7f4a_7c15);
				(mixed >> 63) != 0
			}
			Self::AlternatingBit => row.is_multiple_of(2),
		}
	}
}

struct EncodedValidityFixture {
	record_id: TieredRecordId,
	ipc_bytes: Vec<u8>,
	page_directory: RecordPageDirectory,
	batches: Vec<RecordBatch>,
	writer_config: EncodingWriterConfig,
	row_group_config: RowGroupWriterConfig,
	raw_validity_bytes: usize,
	first_page_raw_validity_bytes: usize,
}

fn bench_encoded_validity_sparse_projection_matrix(config: &BenchConfig) -> std::io::Result<()> {
	for rows in config.rows.iter().copied() {
		for shape in [
			EncodedValidityNullShape::Clustered5,
			EncodedValidityNullShape::Clustered1,
			EncodedValidityNullShape::AllValid,
			EncodedValidityNullShape::AllNull,
			EncodedValidityNullShape::Random50,
			EncodedValidityNullShape::AlternatingBit,
		] {
			let fixture = encoded_validity_fixture(rows, shape)?;
			let disabled = encoded_validity_directory_summary(&fixture, false)?;
			let enabled = encoded_validity_directory_summary(&fixture, true)?;
			let enabled_again = encoded_validity_directory_summary(&fixture, true)?;
			let deterministic = enabled.validity_page_end_offsets
				== enabled_again.validity_page_end_offsets
				&& enabled.validity_encoded_bytes == enabled_again.validity_encoded_bytes;
			let disabled_elapsed = bench(config.iterations, || {
				let result = build_encoding_directory_for_ipc_stream_exact_fsst_for_bench(
					fixture.record_id,
					&fixture.ipc_bytes,
					&fixture.page_directory,
					&fixture.batches,
					&fixture.writer_config,
					&fixture.row_group_config,
				)
				.expect("encoded-validity disabled bench writer");
				black_box(result);
			});
			let enabled_elapsed = bench(config.iterations, || {
				let result = build_encoding_directory_for_ipc_stream(
					fixture.record_id,
					&fixture.ipc_bytes,
					&fixture.page_directory,
					&fixture.batches,
					&fixture.writer_config,
					&fixture.row_group_config,
				)
				.expect("encoded-validity enabled bench writer");
				black_box(result);
			});
			println!(
				"encoding_compute_bench scenario=encoded_validity_sparse_projection fixture={} rows={} iterations={} raw_validity_bytes={} disabled_validity_encoded={} enabled_validity_encoded={} enabled_validity_payload_bytes={} enabled_validity_page_table_bytes={} selected_validity_raw_bytes={} selected_validity_encoded_bytes={} deterministic={} disabled_writer_micros_per_op={:.3} enabled_writer_micros_per_op={:.3}",
				shape.name(),
				rows,
				config.iterations,
				fixture.raw_validity_bytes,
				disabled.validity_buffer_encoded,
				enabled.validity_buffer_encoded,
				enabled.validity_payload_bytes,
				enabled.validity_page_table_bytes,
				fixture.first_page_raw_validity_bytes,
				enabled.first_page_encoded_bytes,
				deterministic,
				disabled_elapsed.as_secs_f64() * 1_000_000.0 / config.iterations.max(1) as f64,
				enabled_elapsed.as_secs_f64() * 1_000_000.0 / config.iterations.max(1) as f64,
			);
		}
	}
	Ok(())
}

fn encoded_validity_fixture(
	rows: usize,
	shape: EncodedValidityNullShape,
) -> std::io::Result<EncodedValidityFixture> {
	let page_values = PAGE_VALUES as usize;
	let null_bytes = validity_bitmap_bytes(rows, |row| shape.is_valid(row, rows));
	let mut value_bytes = Vec::with_capacity(rows * std::mem::size_of::<i64>());
	for _ in 0..rows {
		value_bytes.extend_from_slice(&0_i64.to_le_bytes());
	}
	let value_start = null_bytes.len() as u64;
	let mut ipc_bytes = null_bytes.clone();
	ipc_bytes.extend_from_slice(&value_bytes);
	let record_id = TieredRecordId {
		slab_id: SlabId::new(69),
		record_offset: RecordBodyByteOffset::ZERO,
	};
	let page_directory = RecordPageDirectory {
		record_id,
		row_count: RowCount::new(rows as u64),
		row_groups: vec![RowGroupDirectoryEntry {
			row_group_index: RowGroupIndex::ZERO,
			first_record_row_index: RecordRowIndex::ZERO,
			row_count: RowGroupRowCount::new(rows as u32),
			metadata_range: RecordBodyByteRange {
				start: RecordBodyByteOffset::ZERO,
				len: RecordBodyByteLen::new(64),
			},
			body_range: RecordBodyByteRange {
				start: RecordBodyByteOffset::ZERO,
				len: RecordBodyByteLen::new(ipc_bytes.len() as u64),
			},
		}],
		buffer_pages: vec![
			BufferPageDirectoryEntry {
				row_group_index: RowGroupIndex::ZERO,
				ipc_column_ordinal: IpcColumnOrdinal::ZERO,
				ipc_buffer_ordinal: IpcBufferIndex::ZERO,
				range: RecordBodyByteRange {
					start: RecordBodyByteOffset::ZERO,
					len: RecordBodyByteLen::new(null_bytes.len() as u64),
				},
			},
			BufferPageDirectoryEntry {
				row_group_index: RowGroupIndex::ZERO,
				ipc_column_ordinal: IpcColumnOrdinal::ZERO,
				ipc_buffer_ordinal: IpcBufferIndex::new(1),
				range: RecordBodyByteRange {
					start: RecordBodyByteOffset::new(value_start),
					len: RecordBodyByteLen::new(value_bytes.len() as u64),
				},
			},
		],
		row_group_stats: Vec::new(),
		logical_schema_fingerprint: bench_logical_fingerprint(),
	};
	let schema = Arc::new(Schema::new(vec![Field::new(
		"value",
		DataType::Int64,
		true,
	)]));
	let values = (0..rows)
		.map(|row| shape.is_valid(row, rows).then_some(0_i64))
		.collect::<Vec<_>>();
	let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef])
		.map_err(|err| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("encoded-validity bench batch: {err}"),
			)
		})?;
	let writer_config = EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: SimdPageValueCount::new(page_values as u32),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: [EncodingCascadeKind::Plain, EncodingCascadeKind::Rle]
			.into_iter()
			.collect(),
		..EncodingWriterConfig::default()
	};
	Ok(EncodedValidityFixture {
		record_id,
		ipc_bytes,
		page_directory,
		batches: vec![batch],
		writer_config,
		row_group_config: RowGroupWriterConfig::default(),
		raw_validity_bytes: null_bytes.len(),
		first_page_raw_validity_bytes: rows.min(page_values).div_ceil(8),
	})
}

fn validity_bitmap_bytes(rows: usize, mut is_valid: impl FnMut(usize) -> bool) -> Vec<u8> {
	let mut bytes = vec![0_u8; rows.div_ceil(8)];
	for row in 0..rows {
		if is_valid(row) {
			bytes[row / 8] |= 1_u8 << (row % 8);
		}
	}
	bytes
}

#[derive(Clone, Debug)]
struct EncodedValidityDirectorySummary {
	validity_buffer_encoded: bool,
	validity_payload_bytes: usize,
	validity_page_table_bytes: usize,
	first_page_encoded_bytes: usize,
	validity_page_end_offsets: Vec<u32>,
	validity_encoded_bytes: Vec<u8>,
}

fn encoded_validity_directory_summary(
	fixture: &EncodedValidityFixture,
	enabled: bool,
) -> std::io::Result<EncodedValidityDirectorySummary> {
	let (directory, applied) = if enabled {
		build_encoding_directory_for_ipc_stream(
			fixture.record_id,
			&fixture.ipc_bytes,
			&fixture.page_directory,
			&fixture.batches,
			&fixture.writer_config,
			&fixture.row_group_config,
		)?
	} else {
		build_encoding_directory_for_ipc_stream_exact_fsst_for_bench(
			fixture.record_id,
			&fixture.ipc_bytes,
			&fixture.page_directory,
			&fixture.batches,
			&fixture.writer_config,
			&fixture.row_group_config,
		)?
	};
	let Some(directory) = directory else {
		return Ok(EncodedValidityDirectorySummary {
			validity_buffer_encoded: false,
			validity_payload_bytes: 0,
			validity_page_table_bytes: 0,
			first_page_encoded_bytes: 0,
			validity_page_end_offsets: Vec::new(),
			validity_encoded_bytes: Vec::new(),
		});
	};
	Ok(encoded_validity_summary_from_directory(&directory, applied))
}

fn encoded_validity_summary_from_directory(
	directory: &EncodingDirectory,
	applied: Vec<wendelon::storage::encoding::directory::AppliedEncodingBuffer>,
) -> EncodedValidityDirectorySummary {
	let validity = directory
		.buffers
		.iter()
		.find(|encoding| encoding.ipc_buffer_ordinal == IpcBufferIndex::ZERO);
	let Some(validity) = validity else {
		return EncodedValidityDirectorySummary {
			validity_buffer_encoded: false,
			validity_payload_bytes: 0,
			validity_page_table_bytes: 0,
			first_page_encoded_bytes: 0,
			validity_page_end_offsets: Vec::new(),
			validity_encoded_bytes: Vec::new(),
		};
	};
	let validity_page_end_offsets = validity
		.page_end_offsets
		.iter()
		.map(|offset| offset.get())
		.collect::<Vec<_>>();
	let first_page_encoded_bytes = validity_page_end_offsets
		.first()
		.copied()
		.unwrap_or_default() as usize;
	let validity_encoded_bytes = applied
		.into_iter()
		.find(|buffer| buffer.ipc_buffer_ordinal == IpcBufferIndex::ZERO)
		.map(|buffer| buffer.encoded_bytes)
		.unwrap_or_default();
	EncodedValidityDirectorySummary {
		validity_buffer_encoded: true,
		validity_payload_bytes: validity_encoded_bytes.len(),
		validity_page_table_bytes: validity.page_end_offsets.len() * std::mem::size_of::<u32>(),
		first_page_encoded_bytes,
		validity_page_end_offsets,
		validity_encoded_bytes,
	}
}

fn bench(iterations: usize, mut f: impl FnMut()) -> Duration {
	for _ in 0..iterations.min(5) {
		f();
	}
	let started = Instant::now();
	for _ in 0..iterations {
		f();
	}
	started.elapsed()
}

fn emit_row(
	scenario: &str,
	fixture: &str,
	rows: usize,
	bytes: u64,
	iterations: usize,
	elapsed: Duration,
) {
	let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / iterations.max(1) as f64;
	println!(
		"encoding_compute_bench scenario={scenario} fixture={fixture} rows={rows} iterations={iterations} total_ms={:.3} micros_per_op={micros_per_op:.3} bytes={bytes}",
		elapsed.as_secs_f64() * 1_000.0
	);
}
