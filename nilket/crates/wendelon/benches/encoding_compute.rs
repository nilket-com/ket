//! ADR 058 compute-over-encoded-data benchmark harness.
//!
//! `cargo bench --bench encoding_compute -- --quick`.

use arrow::datatypes::DataType;
use std::hint::black_box;
use std::time::{Duration, Instant};
use wendelon::schema::aggregate::{
	AGG_FORMAT_VERSION, AggregateBudgets, AggregateExpression, AggregateFunctionTag,
};
use wendelon::schema::pbc::ColumnPredicateLiteralV1;
use wendelon::schema::{
	AggregateRequest, AggregateRequestId, BaseValue, BitsPerValue, BufferValueCount, Consistency,
	EncodingCascadeIndex, HybridTimestamp, IpcBufferIndex, IpcColumnOrdinal,
	LogicalSchemaFingerprint, RecordBodyByteLen, RowCount, RowGroupIndex, SimdPageValueCount,
	TableObjectId, TenantId,
};
use wendelon::storage::encoding::cascade::encode_buffer;
use wendelon::storage::encoding::compute::{
	EncodedAggregateContext, EncodedRowMask, PredicateAtom, PredicateAtomOp, coerce_literal,
	evaluate, try_fold_encoded_aggregate_request,
};
use wendelon::storage::encoding::{BufferEncoding, EncodingCascade};

fn main() -> std::io::Result<()> {
	let config = BenchConfig::from_args();
	for rows in config.rows.iter().copied() {
		let fixture = BenchFixture::new(rows)?;
		bench_predicate(&config, &fixture)?;
		bench_mask_algebra(&config, &fixture)?;
		bench_aggregate_fold(&config, &fixture)?;
	}
	Ok(())
}

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
	iterations: usize,
}

impl BenchConfig {
	fn from_args() -> Self {
		let quick = std::env::args().any(|arg| arg == "--quick");
		if quick {
			Self {
				rows: vec![8_192],
				iterations: 50,
			}
		} else {
			Self {
				rows: vec![8_192, 65_536, 262_144],
				iterations: 200,
			}
		}
	}
}

#[derive(Clone, Debug)]
struct BenchFixture {
	rows: usize,
	decoded: Vec<u8>,
	encoded: Vec<u8>,
	cascade: EncodingCascade,
	encoding: BufferEncoding,
	atom: PredicateAtom,
	mask: EncodedRowMask,
	aggregate_request: AggregateRequest,
	aggregate_context: EncodedAggregateContext,
}

impl BenchFixture {
	fn new(rows: usize) -> std::io::Result<Self> {
		let decoded = (0..rows).map(|row| (row % 251) as u8).collect::<Vec<_>>();
		let cascade = EncodingCascade::BitPacked {
			bits: BitsPerValue::new(8),
			base: BaseValue::new(0),
		};
		let encoded = encode_buffer(
			&decoded,
			&DataType::UInt8,
			cascade.clone(),
			BufferValueCount::new(rows as u32),
			SimdPageValueCount::new(256),
		)?;
		let encoding = BufferEncoding {
			row_group_index: RowGroupIndex::ZERO,
			ipc_column_ordinal: IpcColumnOrdinal::new(0),
			ipc_buffer_ordinal: IpcBufferIndex::new(1),
			cascade_index: EncodingCascadeIndex::new(0),
			decoded_byte_len: RecordBodyByteLen::new(decoded.len() as u64),
			decoded_value_count: BufferValueCount::new(rows as u32),
			encoded_range: encoded.encoded_range(),
			page_value_count: SimdPageValueCount::new(256),
			page_end_offsets: encoded.page_end_offsets.clone(),
		};
		let atom = PredicateAtom {
			op: PredicateAtomOp::Lte,
			literal: Some(ColumnPredicateLiteralV1::Int(12)),
		};
		let coercion = coerce_literal(atom.literal.as_ref(), &DataType::UInt8)
			.expect("bench literal should coerce");
		let mask = evaluate(&encoded.bytes, &encoding, &cascade, &atom, &coercion)
			.expect("bench predicate should evaluate");
		let aggregate_request = AggregateRequest {
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
			aggregates: vec![
				AggregateExpression::count_star(),
				AggregateExpression {
					tag: AggregateFunctionTag::UNSIGNED_SUM,
					column_ordinal: 0,
					parameter_block: Vec::new(),
				},
			],
			budgets: AggregateBudgets::default(),
		};
		let aggregate_context = EncodedAggregateContext::new(RowCount::new(rows as u64))
			.with_decoded_column(0, DataType::UInt8, decoded.clone());
		Ok(Self {
			rows,
			decoded,
			encoded: encoded.bytes,
			cascade,
			encoding,
			atom,
			mask,
			aggregate_request,
			aggregate_context,
		})
	}
}

fn bench_predicate(config: &BenchConfig, fixture: &BenchFixture) -> std::io::Result<()> {
	let coercion = coerce_literal(fixture.atom.literal.as_ref(), &DataType::UInt8)
		.expect("bench literal should coerce");
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
	print_bench(
		"predicate_exact",
		fixture.rows,
		fixture.encoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_logical_fingerprint() -> LogicalSchemaFingerprint {
	postcard::from_bytes(&[0x58; 32]).expect("bench logical fingerprint")
}

fn bench_mask_algebra(config: &BenchConfig, fixture: &BenchFixture) -> std::io::Result<()> {
	let inverse = fixture.mask.not().expect("bench mask not");
	let elapsed = bench(config.iterations, || {
		let combined = black_box(&fixture.mask)
			.and(black_box(&inverse))
			.expect("bench mask and");
		black_box(combined.popcount().expect("bench popcount"));
	});
	print_bench(
		"mask_algebra",
		fixture.rows,
		fixture.mask.byte_len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_aggregate_fold(config: &BenchConfig, fixture: &BenchFixture) -> std::io::Result<()> {
	let elapsed = bench(config.iterations, || {
		let partials = try_fold_encoded_aggregate_request(
			black_box(&fixture.aggregate_request),
			black_box(&fixture.aggregate_context),
			Some(black_box(&fixture.mask)),
		)
		.expect("bench aggregate fold")
		.expect("bench aggregate supported");
		black_box(partials);
	});
	print_bench(
		"aggregate_fold",
		fixture.rows,
		fixture.decoded.len() as u64,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench(iterations: usize, mut f: impl FnMut()) -> Duration {
	let started = Instant::now();
	for _ in 0..iterations {
		f();
	}
	started.elapsed()
}

fn print_bench(scenario: &str, rows: usize, bytes: u64, iterations: usize, elapsed: Duration) {
	let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / iterations.max(1) as f64;
	println!(
		"encoding_compute_bench scenario={scenario} rows={rows} iterations={iterations} total_ms={:.3} micros_per_op={micros_per_op:.3} bytes={bytes}",
		elapsed.as_secs_f64() * 1_000.0
	);
}
