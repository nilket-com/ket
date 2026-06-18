//! ADR 261: TPC-H Reachability Harness — the library half (CLI lives in `main.rs`).
//!
//! Dev mode generates the Q6-needed `LINEITEM` columns from the pure-Rust `tpchgen` row structs,
//! converts `TPCHDecimal → Float64` and `TPCHDate → Date32` into workspace-Arrow, loads them through a
//! local [`StorageScan`], and runs a Toron-authored Q6 proxy single-node through `plan → bind →
//! execute`. A `pass` here means *reachability* (the shape ran and produced a digest), not TPC-H
//! correctness — dev data is synthetic and explicitly NOT a TPC-H result.

pub mod telemetry;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Date32Array, Float64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tpchgen::generators::LineItemGenerator;

use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::error::StorageError;
use toron::exec::Executor;
use toron::ir::TableId;
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};

use telemetry::{Attempt, Phase, Status, reason_for_error, status_for_reason};

/// Default cache / output root — always under `target/`, never a tracked path.
pub const CACHE_ROOT: &str = "target/tpch";

/// The 22 TPC-H query IDs (the reachability inventory).
pub const ALL_QUERY_IDS: [&str; 22] = [
	"q01", "q02", "q03", "q04", "q05", "q06", "q07", "q08", "q09", "q10", "q11", "q12", "q13",
	"q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22",
];

/// The Toron-authored Q6 dev proxy. Two deliberate, visible departures from strict TPC-H: `Float64`
/// columns/literals replace `DECIMAL`, and the upper date bound is a literal date rather than
/// `DATE '1994-01-01' + INTERVAL '1' YEAR` (Toron's `Date32 ± Interval` is still narrow). NOT the
/// official Q6.
pub const Q6_DEV_SQL: &str = "SELECT SUM(l_extendedprice * l_discount) AS revenue \
	FROM lineitem \
	WHERE l_shipdate >= DATE '1994-01-01' \
	  AND l_shipdate <  DATE '1995-01-01' \
	  AND l_discount BETWEEN 0.05 AND 0.07 \
	  AND l_quantity < 24.0";

/// The dev `LINEITEM` schema: only the Q6 columns, money/qty/discount as `Float64`, ship date as
/// `Date32`. No string carriers; an append-only (PK-less) identity, so no synthetic key column.
fn dev_lineitem_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("l_quantity", DataType::Float64, false),
		Field::new("l_extendedprice", DataType::Float64, false),
		Field::new("l_discount", DataType::Float64, false),
		Field::new("l_shipdate", DataType::Date32, false),
	]))
}

fn dev_lineitem_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	// Append-only (PK-less) identity needs the multi-row record layout explicitly.
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([1; 16]),
		1,
		"default",
		"lineitem",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("dev lineitem descriptor")
}

/// Generate the Q6-needed `LINEITEM` columns at `scale_factor` from `tpchgen` rows into workspace-Arrow.
pub fn generate_dev_lineitem(scale_factor: f64) -> RecordBatch {
	let generator = LineItemGenerator::new(scale_factor, 1, 1);
	let mut quantity = Vec::new();
	let mut extendedprice = Vec::new();
	let mut discount = Vec::new();
	let mut shipdate = Vec::new();
	for item in generator.iter() {
		quantity.push(item.l_quantity as f64);
		extendedprice.push(item.l_extendedprice.as_f64());
		discount.push(item.l_discount.as_f64());
		shipdate.push(item.l_shipdate.to_unix_epoch());
	}
	RecordBatch::try_new(
		dev_lineitem_schema(),
		vec![
			Arc::new(Float64Array::from(quantity)),
			Arc::new(Float64Array::from(extendedprice)),
			Arc::new(Float64Array::from(discount)),
			Arc::new(Date32Array::from(shipdate)),
		],
	)
	.expect("dev lineitem batch")
}

/// The load seam: an in-memory [`StorageScan`] over generated batches, mirroring the test `FixtureScan`
/// (honor a pushed PBC predicate, then a projection). This is the one place the harness reaches into
/// Toron's storage trait.
pub struct HarnessScan {
	pub batches: HashMap<TableId, RecordBatch>,
}

#[async_trait]
impl StorageScan for HarnessScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch =
			self.batches
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;

		if let Some(pbc) = request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}

		if let Some(columns) = request.projected_columns {
			let indices = columns
				.into_iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}

		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}

/// Run the dev Q6 proxy single-node, recording the deepest phase reached. A stage error is classified
/// against the recognized-blocker allowlist: a recognized reason is `blocked`, anything else is `fail`.
pub async fn run_dev_q6(scale_factor: f64) -> Attempt {
	let start = Instant::now();
	let attempt =
		|phase: Phase, status: Status, reason: Option<String>, digest: Option<String>| Attempt {
			query: "q06".to_string(),
			scale_factor,
			mode: "dev".to_string(),
			phase,
			status,
			reason,
			elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
			digest,
		};

	// generate + load
	let batch = generate_dev_lineitem(scale_factor);
	let schema = dev_lineitem_schema();
	let mut catalog = MockCatalog::new();
	if let Err(error) =
		catalog.register_descriptor(TableId(1), dev_lineitem_descriptor(schema.as_ref()))
	{
		let reason = reason_for_error(&format!("{error:?}"));
		return attempt(
			Phase::Load,
			status_for_reason(reason),
			Some(reason.to_string()),
			None,
		);
	}
	let storage = Arc::new(HarnessScan {
		batches: HashMap::from([(TableId(1), batch)]),
	});

	// plan
	let logical = match SqlPlanner::new().plan(Q6_DEV_SQL) {
		Ok(plan) => plan,
		Err(error) => {
			let reason = reason_for_error(&error.to_string());
			return attempt(
				Phase::Plan,
				status_for_reason(reason),
				Some(reason.to_string()),
				None,
			);
		}
	};

	// bind
	let bound = match bind(&logical, &catalog, &PlannerConfig::default()).await {
		Ok(bound) => bound,
		Err(error) => {
			let reason = reason_for_error(&format!("{error:?}"));
			return attempt(
				Phase::Bind,
				status_for_reason(reason),
				Some(reason.to_string()),
				None,
			);
		}
	};

	// execute
	let batches = match Executor::new(storage).execute_bound(&bound, None).await {
		Ok(batches) => batches,
		Err(error) => {
			let reason = reason_for_error(&format!("{error:?}"));
			return attempt(
				Phase::Execute,
				status_for_reason(reason),
				Some(reason.to_string()),
				None,
			);
		}
	};

	let digest = match canonical_arrow_ipc_digest(&batches, SortPolicy::FullRowLex) {
		Ok(digest) => format!("{digest:?}"),
		Err(error) => {
			let reason = reason_for_error(&format!("{error:?}"));
			return attempt(
				Phase::Execute,
				status_for_reason(reason),
				Some(reason.to_string()),
				None,
			);
		}
	};

	attempt(Phase::Execute, Status::Pass, None, Some(digest))
}

/// Strict-mode Q6 is a skeleton in ADR 261: it requires the (deferred) official schema/data/answer
/// pipeline, so it records `not_attempted reason=strict_inputs_required` rather than fabricating an
/// engine blocker. Reporting the first real phase a user-supplied official run reaches is a follow-on.
pub fn run_strict_q6() -> Attempt {
	Attempt {
		query: "q06".to_string(),
		scale_factor: 0.0,
		mode: "strict".to_string(),
		phase: Phase::Generate,
		status: Status::NotAttempted,
		reason: Some("strict_inputs_required".to_string()),
		elapsed_ms: 0.0,
		digest: None,
	}
}

/// The 22-query reachability inventory: dev implementation state + strict (input-gated) state.
pub fn status_rows() -> Vec<String> {
	ALL_QUERY_IDS
		.iter()
		.map(|id| {
			let dev = if *id == "q06" {
				"implemented"
			} else {
				"not_attempted"
			};
			format!("{id} dev={dev} strict=input_gated")
		})
		.collect()
}
