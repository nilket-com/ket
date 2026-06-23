//! ADR 002 storage-backed execution contract benchmark harness.
//!
//! Smoke check:
//! `cargo bench --bench storage_execution -- --quick --storage-execution-only`.

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio::runtime::Runtime;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::StorageError;
use toron::exec::Executor;
use toron::ir::{
	AggregateFunction, ColumnOrdinal, ExecutionPlan, Expr, FilterDisposition, LogicalPlan,
	MorselId, QueryProfileHint, ScalarValue, TableId,
};
use toron::storage::{RowIdentityProjection, ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

const DEFAULT_ROWS: &[usize] = &[8_192, 65_536];
const QUICK_ROWS: &[usize] = &[8_192];
const CANONICAL_BATCH_SIZE: usize = 65_536;

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
	quick: bool,
}

impl BenchConfig {
	fn parse() -> Self {
		let mut rows = DEFAULT_ROWS.to_vec();
		let mut quick = false;
		for arg in std::env::args().skip(1) {
			if arg == "--quick" {
				rows = QUICK_ROWS.to_vec();
				quick = true;
			} else if let Some(value) = arg.strip_prefix("--rows=") {
				rows = value
					.split(',')
					.map(|part| {
						part.parse()
							.expect("--rows entries must be positive integers")
					})
					.collect();
			} else if arg == "--storage-execution-only" || arg == "--bench" {
			} else {
				eprintln!("storage_execution ignoring unrecognized arg: {arg}");
			}
		}
		Self { rows, quick }
	}
}

#[derive(Clone)]
struct BenchCase {
	name: &'static str,
	plan: LogicalPlan,
	expect_pbc: bool,
}

#[derive(Clone, Debug)]
struct ScanObservation {
	pbc_engaged: bool,
	projection_cols: Option<Vec<ColumnOrdinal>>,
	profile_hint: Option<QueryProfileHint>,
	returned_bytes: usize,
	returned_rows: usize,
	returned_columns: usize,
}

struct InstrumentedStorageScan {
	table_data: RecordBatch,
	observations: Arc<Mutex<Vec<ScanObservation>>>,
}

#[async_trait::async_trait]
impl StorageScan for InstrumentedStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch = self.table_data.clone();

		if let Some((morsel_id, total_morsels)) = request.morsel {
			batch = slice_morsel(batch, morsel_id, total_morsels);
		}

		if let Some(pbc) = request.pbc_predicate.clone() {
			batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}

		if let Some(columns) = &request.projected_columns {
			let indices = columns
				.iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}

		let returned_bytes = record_batch_ipc_bytes(&batch)?;
		self.observations.lock().unwrap().push(ScanObservation {
			pbc_engaged: request.pbc_predicate.is_some(),
			projection_cols: request.projected_columns.clone(),
			profile_hint: request.profile_hint,
			returned_bytes,
			returned_rows: batch.num_rows(),
			returned_columns: batch.num_columns(),
		});

		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}

fn main() -> Result<(), Box<dyn Error>> {
	let config = BenchConfig::parse();
	let rt = Runtime::new()?;
	for rows in &config.rows {
		let fixture = fixture(*rows)?;
		for case in cases() {
			let row = run_case(&rt, fixture.clone(), case)?;
			print_row(*rows, &config, &row);
		}
	}
	Ok(())
}

#[derive(Debug)]
struct BenchRow {
	case: &'static str,
	baseline_elapsed: Duration,
	engaged_elapsed: Duration,
	baseline: ScanObservation,
	engaged: ScanObservation,
	digest: u64,
	expect_pbc: bool,
}

fn run_case(rt: &Runtime, fixture: Fixture, case: BenchCase) -> Result<BenchRow, Box<dyn Error>> {
	let mut catalog = MockCatalog::new();
	catalog.register_descriptor(TableId(1), fixture.descriptor.clone())?;
	let bound = rt.block_on(bind(&case.plan, &catalog, &PlannerConfig::default()))?;
	let baseline_bound = disable_pbc(bound.clone());

	let baseline_observations = Arc::new(Mutex::new(Vec::new()));
	let baseline_executor = Executor::new(Arc::new(InstrumentedStorageScan {
		table_data: fixture.batch.clone(),
		observations: baseline_observations.clone(),
	}));
	let baseline_started = Instant::now();
	let baseline = rt.block_on(baseline_executor.execute_bound(&baseline_bound, None))?;
	let baseline_elapsed = baseline_started.elapsed();

	let engaged_observations = Arc::new(Mutex::new(Vec::new()));
	let engaged_executor = Executor::new(Arc::new(InstrumentedStorageScan {
		table_data: fixture.batch,
		observations: engaged_observations.clone(),
	}));
	let engaged_started = Instant::now();
	let engaged = rt.block_on(engaged_executor.execute_bound(&bound, None))?;
	let engaged_elapsed = engaged_started.elapsed();

	let baseline_digest = digest_batches(canonical_normalize(baseline, CANONICAL_BATCH_SIZE)?)?;
	let engaged_digest = digest_batches(canonical_normalize(engaged, CANONICAL_BATCH_SIZE)?)?;
	if baseline_digest != engaged_digest {
		return Err(format!(
			"semantic parity digest mismatch for {}: baseline={baseline_digest:x} engaged={engaged_digest:x}",
			case.name
		)
		.into());
	}

	let baseline_observation = take_one_observation(&baseline_observations, case.name, "baseline")?;
	let engaged_observation = take_one_observation(&engaged_observations, case.name, "engaged")?;
	if case.expect_pbc && !engaged_observation.pbc_engaged {
		return Err(format!("expected PBC engagement for {}", case.name).into());
	}

	Ok(BenchRow {
		case: case.name,
		baseline_elapsed,
		engaged_elapsed,
		baseline: baseline_observation,
		engaged: engaged_observation,
		digest: engaged_digest,
		expect_pbc: case.expect_pbc,
	})
}

fn print_row(rows: usize, config: &BenchConfig, row: &BenchRow) {
	let baseline_ms = row.baseline_elapsed.as_secs_f64() * 1_000.0;
	let engaged_ms = row.engaged_elapsed.as_secs_f64() * 1_000.0;
	let speedup = if engaged_ms > 0.0 {
		baseline_ms / engaged_ms
	} else {
		0.0
	};
	let baseline_projection = projection_len(&row.baseline.projection_cols);
	let engaged_projection = projection_len(&row.engaged.projection_cols);

	println!(
		"storage_execution case={} rows={} quick={} baseline_ms={baseline_ms:.3} engaged_ms={engaged_ms:.3} speedup={speedup:.2} baseline_bytes={} engaged_bytes={} baseline_rows={} engaged_rows={} baseline_columns={} engaged_columns={} pbc_engaged={} expect_pbc={} profile={:?} baseline_projection_cols={baseline_projection} engaged_projection_cols={engaged_projection} digest={:016x}",
		row.case,
		rows,
		config.quick,
		row.baseline.returned_bytes,
		row.engaged.returned_bytes,
		row.baseline.returned_rows,
		row.engaged.returned_rows,
		row.baseline.returned_columns,
		row.engaged.returned_columns,
		row.engaged.pbc_engaged,
		row.expect_pbc,
		row.engaged.profile_hint,
		row.digest,
	);
	black_box(row.digest);
}

fn projection_len(columns: &Option<Vec<ColumnOrdinal>>) -> usize {
	columns.as_ref().map_or(0, Vec::len)
}

fn take_one_observation(
	observations: &Arc<Mutex<Vec<ScanObservation>>>,
	case: &str,
	mode: &str,
) -> Result<ScanObservation, Box<dyn Error>> {
	let observations = observations.lock().unwrap();
	if observations.len() != 1 {
		return Err(format!(
			"expected one {mode} scan observation for {case}, got {}",
			observations.len()
		)
		.into());
	}
	Ok(observations[0].clone())
}

fn disable_pbc(plan: ExecutionPlan) -> ExecutionPlan {
	match plan {
		ExecutionPlan::Scan {
			local_table_id,
			table,
			fingerprint,
			schema,
			projection,
			pbc_program,
			row_limit,
			profile_hint,
			primary_key_columns,
			column_ndv,
			column_range,
			column_range_distributions,
			column_heavy_hitters,
			column_group_top_k,
			column_group_ndv,
			..
		} => ExecutionPlan::Scan {
			local_table_id,
			table,
			fingerprint,
			schema,
			projection: if pbc_program.is_some() {
				None
			} else {
				projection
			},
			pbc_program: None,
			row_limit,
			profile_hint,
			primary_key_columns,
			column_ndv,
			column_range,
			column_range_distributions,
			column_heavy_hitters,
			column_group_top_k,
			column_group_ndv,
			row_identity: RowIdentityProjection::None,
		},
		ExecutionPlan::Filter {
			input, predicate, ..
		} => ExecutionPlan::Filter {
			input: Box::new(disable_pbc(*input)),
			predicate,
			disposition: FilterDisposition::EvaluateInToron,
		},
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema,
		} => ExecutionPlan::Project {
			input: Box::new(disable_pbc(*input)),
			exprs,
			output_schema,
		},
		ExecutionPlan::Aggregate {
			input,
			group_expr,
			aggr_expr,
			output_schema,
		} => ExecutionPlan::Aggregate {
			input: Box::new(disable_pbc(*input)),
			group_expr,
			aggr_expr,
			output_schema,
		},
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => ExecutionPlan::Exchange {
			input: Box::new(disable_pbc(*input)),
			partition_count,
			key,
		},
		ExecutionPlan::Sort {
			input,
			order_by,
			limit,
			offset,
			output_schema,
		} => ExecutionPlan::Sort {
			input: Box::new(disable_pbc(*input)),
			order_by,
			limit,
			offset,
			output_schema,
		},
		ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left,
			right,
			kind,
			left_keys,
			right_keys,
			match_filter,
			strategy,
			null_safe_keys,
			output_schema,
		} => ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left: Box::new(disable_pbc(*left)),
			right: Box::new(disable_pbc(*right)),
			kind,
			left_keys,
			right_keys,
			match_filter,
			strategy,
			null_safe_keys,
			output_schema,
		},
		other => other,
	}
}

fn canonical_normalize(
	batches: Vec<RecordBatch>,
	batch_size: usize,
) -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
	if batches.is_empty() {
		return Ok(Vec::new());
	}
	let schema = batches[0].schema();
	let concatenated = concat_batches(&schema, batches.iter())?;
	if concatenated.num_rows() == 0 {
		return Ok(vec![concatenated]);
	}

	let mut normalized = Vec::new();
	let mut offset = 0;
	while offset < concatenated.num_rows() {
		let len = batch_size.min(concatenated.num_rows() - offset);
		normalized.push(concatenated.slice(offset, len));
		offset += len;
	}
	Ok(normalized)
}

fn digest_batches(batches: Vec<RecordBatch>) -> Result<u64, Box<dyn Error>> {
	let mut hasher = DefaultHasher::new();
	for batch in batches {
		let bytes = record_batch_ipc(&batch)?;
		bytes.hash(&mut hasher);
	}
	Ok(hasher.finish())
}

fn record_batch_ipc_bytes(batch: &RecordBatch) -> Result<usize, StorageError> {
	record_batch_ipc(batch)
		.map(|bytes| bytes.len())
		.map_err(|error| StorageError::Internal {
			message: error.to_string(),
		})
}

fn record_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>, arrow::error::ArrowError> {
	let mut bytes = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema())?;
		writer.write(batch)?;
		writer.finish()?;
	}
	Ok(bytes)
}

fn slice_morsel(batch: RecordBatch, morsel_id: MorselId, total_morsels: u32) -> RecordBatch {
	let total_rows = batch.num_rows();
	let rows_per_morsel = (total_rows + total_morsels as usize - 1) / total_morsels as usize;
	let start_row = (morsel_id.0 as usize) * rows_per_morsel;
	let end_row = std::cmp::min(start_row + rows_per_morsel, total_rows);
	if start_row < total_rows {
		batch.slice(start_row, end_row - start_row)
	} else {
		batch.slice(0, 0)
	}
}

#[derive(Clone)]
struct Fixture {
	batch: RecordBatch,
	descriptor: TableSchemaDescriptor,
}

fn fixture(rows: usize) -> Result<Fixture, Box<dyn Error>> {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("region", DataType::Utf8, true),
		Field::new("all_null_i32", DataType::Int32, true),
		Field::new("float64_amount", DataType::Float64, true),
		Field::new("float32_amount", DataType::Float32, true),
	]));
	let ids = Int64Array::from_iter_values((0..rows).map(|row| row as i64));
	let amounts = Int32Array::from(
		(0..rows)
			.map(|row| {
				if row % 7 == 0 {
					None
				} else {
					Some((row % 1_000) as i32)
				}
			})
			.collect::<Vec<_>>(),
	);
	let regions = StringArray::from(
		(0..rows)
			.map(|row| match row % 5 {
				0 => None,
				1 => Some("east"),
				2 => Some("west"),
				3 => Some("north"),
				_ => Some("south"),
			})
			.collect::<Vec<_>>(),
	);
	let all_null = Int32Array::from(vec![None::<i32>; rows]);
	let float64_amounts = Float64Array::from(
		(0..rows)
			.map(|row| {
				if row % 11 == 0 {
					None
				} else {
					Some((row % 1_000) as f64 + 0.25)
				}
			})
			.collect::<Vec<_>>(),
	);
	let float32_amounts = Float32Array::from(
		(0..rows)
			.map(|row| {
				if row % 13 == 0 {
					None
				} else {
					Some((row % 1_000) as f32 + 0.5)
				}
			})
			.collect::<Vec<_>>(),
	);
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(ids) as ArrayRef,
			Arc::new(amounts) as ArrayRef,
			Arc::new(regions) as ArrayRef,
			Arc::new(all_null) as ArrayRef,
			Arc::new(float64_amounts) as ArrayRef,
			Arc::new(float32_amounts) as ArrayRef,
		],
	)?;
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x02; 16]),
		1,
		"bench",
		"storage_execution",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)?;
	Ok(Fixture { batch, descriptor })
}

fn cases() -> Vec<BenchCase> {
	vec![
		BenchCase {
			name: "scan_projection",
			plan: project_scan(vec!["id", "region"]),
			expect_pbc: false,
		},
		BenchCase {
			name: "scan_filter",
			plan: filter_scan("amount", 500),
			expect_pbc: true,
		},
		BenchCase {
			name: "filter_projection",
			plan: LogicalPlan::Project {
				input: Box::new(filter_scan("amount", 500)),
				exprs: vec![Expr::column("id")],
				output_names: vec![None],
			},
			expect_pbc: true,
		},
		BenchCase {
			name: "point_read_filter",
			plan: LogicalPlan::Filter {
				input: Box::new(scan()),
				predicate: Expr::Eq(
					Box::new(Expr::column("id")),
					Box::new(Expr::Literal(ScalarValue::Int64(Some(777)))),
				),
			},
			expect_pbc: true,
		},
		BenchCase {
			name: "count_all",
			plan: aggregate(vec![], vec![(AggregateFunction::CountAll, vec![])]),
			expect_pbc: false,
		},
		BenchCase {
			name: "count_nullable",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Count, vec![column("amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "sum_nullable",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Sum, vec![column("amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "avg_nullable_i32",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Avg, vec![column("amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "avg_i64",
			plan: aggregate(vec![], vec![(AggregateFunction::Avg, vec![column("id")])]),
			expect_pbc: false,
		},
		BenchCase {
			name: "avg_float64",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Avg, vec![column("float64_amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "min_nullable_i32",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Min, vec![column("amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "min_float64",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Min, vec![column("float64_amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "min_nullable_f32",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Min, vec![column("float32_amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "max_i64",
			plan: aggregate(vec![], vec![(AggregateFunction::Max, vec![column("id")])]),
			expect_pbc: false,
		},
		BenchCase {
			name: "max_float64",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Max, vec![column("float64_amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "sum_all_null",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Sum, vec![column("all_null_i32")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "avg_all_null_i32",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Avg, vec![column("all_null_i32")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "min_all_null_i32",
			plan: aggregate(
				vec![],
				vec![(AggregateFunction::Min, vec![column("all_null_i32")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "group_sum",
			plan: aggregate(
				vec![column("region")],
				vec![(AggregateFunction::Sum, vec![column("amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "group_avg",
			plan: aggregate(
				vec![column("region")],
				vec![(AggregateFunction::Avg, vec![column("amount")])],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "group_min_max",
			plan: aggregate(
				vec![column("region")],
				vec![
					(AggregateFunction::Min, vec![column("amount")]),
					(AggregateFunction::Max, vec![column("id")]),
				],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "group_float_min_max",
			plan: aggregate(
				vec![column("region")],
				vec![
					(AggregateFunction::Min, vec![column("float64_amount")]),
					(AggregateFunction::Max, vec![column("float64_amount")]),
				],
			),
			expect_pbc: false,
		},
		BenchCase {
			name: "group_null_key",
			plan: aggregate(
				vec![column("region")],
				vec![(AggregateFunction::CountAll, vec![])],
			),
			expect_pbc: false,
		},
	]
}

fn scan() -> LogicalPlan {
	LogicalPlan::Scan {
		table: TableId(1),
		projection: None,
	}
}

fn column(name: &str) -> Expr {
	Expr::column(name)
}

fn project_scan(columns: Vec<&str>) -> LogicalPlan {
	let exprs = columns.into_iter().map(column).collect::<Vec<_>>();
	let output_names = vec![None; exprs.len()];
	LogicalPlan::Project {
		input: Box::new(scan()),
		exprs,
		output_names,
	}
}

fn filter_scan(column: &str, value: i32) -> LogicalPlan {
	LogicalPlan::Filter {
		input: Box::new(scan()),
		predicate: Expr::Gt(
			Box::new(Expr::column(column)),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(value)))),
		),
	}
}

fn aggregate(group_expr: Vec<Expr>, aggr_expr: Vec<(AggregateFunction, Vec<Expr>)>) -> LogicalPlan {
	LogicalPlan::Aggregate {
		input: Box::new(scan()),
		group_expr,
		aggr_expr,
	}
}
