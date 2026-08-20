//! Base-relation acquisition for the executor (ADR 551).
//!
//! Carved out of the exec root: acquisition is one concern with one door, and
//! ADR 354 bounds the root's size. `open_scan` is the only place an execution
//! path calls `StorageScan::scan`, so the span and the origin-classified
//! interval live here rather than at each of the routes that acquire rows.

use futures::StreamExt;
#[allow(unused_imports)]
use futures::stream::StreamExt as _;
use tracing::Instrument;

use super::{Executor, join};
use crate::distributed::budget::BudgetDimension;
use crate::error::ExecutionError;
use crate::storage::ScanRequest;
use arrow::record_batch::RecordBatch;

impl Executor {
	/// ADR 551: the one way an execution path acquires rows — bare node, fused
	/// aggregate and fused join alike — so `scan` means base-relation acquisition
	/// rather than acquisition that happened not to be fused.
	pub(crate) async fn open_scan(
		&self,
		req: ScanRequest,
	) -> Result<crate::scan_metering::MeteredScanStream, crate::error::StorageError> {
		let span = crate::telemetry::storage_scan_span(&req);
		self.context
			.scan_meter
			.open(self.storage.scan(req).instrument(span))
			.await
	}

	pub(in crate::exec) async fn materialize_scan_stream_with_budget(
		&self,
		stream: crate::scan_metering::MeteredScanStream,
		row_limit: Option<usize>,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let task_memory_cap = self.context.task_budget.memory.get();
		let memory_cap = (task_memory_cap >= 64 * 1024 * 1024)
			.then(|| join::join_output_materialization_memory_cap(task_memory_cap));
		let schema = stream.schema().clone();
		let mut stream = stream;
		let mut rows = 0usize;
		let mut output_bytes = 0u64;
		let mut batches = Vec::new();
		while row_limit.map(|limit| rows < limit).unwrap_or(true) {
			let Some(batch) = stream.next().await else {
				break;
			};
			let batch = batch?;
			let batch = if let Some(limit) = row_limit {
				let remaining = limit - rows;
				if batch.num_rows() > remaining {
					batch.slice(0, remaining)
				} else {
					batch
				}
			} else {
				batch
			};
			rows = rows.saturating_add(batch.num_rows());
			output_bytes = output_bytes.saturating_add(batch.get_array_memory_size() as u64);
			if let Some(memory_cap) = memory_cap
				&& output_bytes > memory_cap
			{
				return Err(join::budget_exhausted(
					BudgetDimension::Memory,
					"scan output accumulator",
					output_bytes,
					memory_cap,
				));
			}
			batches.push(batch);
		}
		if batches.is_empty() {
			batches.push(RecordBatch::new_empty(schema));
		}
		Ok(batches)
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};

	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

	use super::*;
	use crate::distributed::task::{TaskPhaseReceipt, TaskTelemetry};
	use crate::exec::ExecutionContext;
	use crate::ir::ExecutionPlan;
	use crate::ir::{ColumnOrdinal, TableId};
	use crate::scan_metering::{Clock, ManualClock, ScanAcquisitionLedger, ScanMeter};
	use crate::storage::{ScanOrigin, ScanStream, StorageScan};
	use wendelon::schema::TableObjectId;

	/// Micros the scripted storage charges for the whole read.
	const READ_COST: u64 = 40;

	fn schema() -> SchemaRef {
		Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
	}

	fn row_batch() -> RecordBatch {
		RecordBatch::try_new(
			schema(),
			vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
		)
		.expect("batch builds")
	}

	/// Storage whose read consumes a fixed, injected amount of time.
	///
	/// The cost is charged on the shared clock rather than by sleeping, so the
	/// measured interval is exact and a fast in-memory read cannot measure zero —
	/// which is what would let the old authority pass this control by accident.
	#[derive(Debug)]
	struct ScriptedStorage {
		clock: Arc<ManualClock>,
	}

	#[async_trait::async_trait]
	impl StorageScan for ScriptedStorage {
		async fn scan(
			&self,
			request: crate::storage::ScanRequest,
		) -> Result<ScanStream, crate::error::StorageError> {
			self.clock.advance(READ_COST);
			Ok(ScanStream::from_batches(
				schema(),
				request.session_id,
				request.query_id,
				ScanOrigin::BaseRelation,
				vec![row_batch()],
			))
		}
	}

	fn scan_plan() -> ExecutionPlan {
		use wendelon::schema::{
			LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy,
			TableSchemaDescriptor,
		};
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([7; 16]),
			1,
			"default",
			"scan_route",
			schema().as_ref(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("k".to_string())),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor");
		ExecutionPlan::Scan {
			local_table_id: TableId(1),
			table: TableObjectId::new([7; 16]),
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema: schema(),
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
			row_limit: None,
			profile_hint: crate::ir::QueryProfileHint::Scan,
			estimated_byte_size: None,
			primary_key_columns: vec![ColumnOrdinal(0)],
			column_ndv: Vec::new(),
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		}
	}

	/// A global aggregate directly over the scan: the fused route.
	fn fused_plan() -> ExecutionPlan {
		use crate::ir::{AggregateFunction, BoundAggregateCall, BoundExprProgramBuilder};
		ExecutionPlan::Aggregate {
			input: Box::new(scan_plan()),
			group_expr: BoundExprProgramBuilder::new().build(),
			aggr_expr: vec![BoundAggregateCall {
				function: AggregateFunction::CountAll,
				args: BoundExprProgramBuilder::new().build(),
			}],
			output_schema: Arc::new(Schema::new(vec![Field::new(
				"count",
				DataType::Int64,
				false,
			)])),
		}
	}

	/// Execute a plan through the real executor and task-telemetry path, and
	/// return what the receipt publishes as base-relation scan time.
	async fn published_scan_micros(plan: &ExecutionPlan) -> u64 {
		let clock = Arc::new(ManualClock::new());
		let storage = Arc::new(ScriptedStorage {
			clock: clock.clone(),
		});
		let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
		let meter = ScanMeter::new(
			clock.clone() as Arc<dyn Clock>,
			Arc::new(ScanAcquisitionLedger::new()),
			Arc::new(crate::phase_ledger::TaskPhaseLedger::new(
				clock as Arc<dyn Clock>,
			)),
		);
		// `with_task_telemetry` installs a system meter, so the injected one has
		// to come after it.
		let context = ExecutionContext::local_test()
			.with_task_telemetry(telemetry.clone())
			.with_scan_meter(meter);
		let executor = Executor::with_context(storage, context);

		executor
			.execute_bound(plan, None)
			.await
			.expect("plan executes");

		let telemetry = telemetry.lock().expect("telemetry");
		assert_eq!(telemetry.phase_receipt, TaskPhaseReceipt::Sealed);
		telemetry.phases.scan_micros
	}

	/// The engine-facing control ADR 550 lacked.
	///
	/// The same base-relation read, executed once at a bare `Scan` node and once
	/// fused into a global aggregate, must attribute the **same** base-scan time.
	/// Both arms use identical scripted storage and one injected clock schedule,
	/// so any difference is the route and nothing else.
	///
	/// Red under the bare-node authority: the bare arm reports the read and the
	/// fused arm reports zero, because `scan_micros` is written only where a bare
	/// `Scan` node executes. It passes once publication moves to the metered
	/// ledger, which measures the read on both routes.
	#[tokio::test]
	async fn bare_and_fused_routes_publish_equal_base_scan_time() {
		let bare = scan_plan();
		let fused = fused_plan();

		// The route difference is asserted directly, not inferred from the plan's
		// SQL or its output shape.
		assert!(
			!crate::exec::aggregate::fuses_scan_into_global_aggregate(&bare),
			"the bare arm reads at an ExecutionPlan::Scan node"
		);
		assert!(
			crate::exec::aggregate::fuses_scan_into_global_aggregate(&fused),
			"the fused arm reads inside a streaming global aggregate"
		);

		let bare_micros = published_scan_micros(&bare).await;
		let fused_micros = published_scan_micros(&fused).await;

		assert_eq!(
			bare_micros, READ_COST,
			"the bare route measures the scripted read exactly"
		);
		assert_eq!(
			bare_micros, fused_micros,
			"the same read must attribute the same base-scan time on both routes"
		);
	}

	/// A broken ledger does not turn into a successful zero-valued receipt at the
	/// executor boundary. The query still succeeds; only its attribution is
	/// withheld, with the exact veto carried on the telemetry record.
	#[tokio::test]
	async fn a_ledger_fault_withholds_the_published_phase_receipt() {
		let clock = Arc::new(ManualClock::new());
		let phases = Arc::new(crate::phase_ledger::TaskPhaseLedger::new(
			clock.clone() as Arc<dyn Clock>
		));
		phases.record_clock_regression();
		let meter = ScanMeter::new(
			clock as Arc<dyn Clock>,
			Arc::new(ScanAcquisitionLedger::new()),
			phases,
		);
		let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
		let context = ExecutionContext::local_test()
			.with_task_telemetry(telemetry.clone())
			.with_scan_meter(meter);
		let executor = Executor::with_context(
			Arc::new(ScriptedStorage {
				clock: Arc::new(ManualClock::new()),
			}),
			context,
		);

		executor
			.execute_bound(&ExecutionPlan::EmptyRelation { schema: schema() }, None)
			.await
			.expect("instrumentation veto does not fail the query");

		assert!(matches!(
			telemetry.lock().expect("telemetry").phase_receipt,
			TaskPhaseReceipt::Withheld(faults) if faults.clock_regressions == 1
		));
	}
}

/// Synthesizes the range relation's batches: `count` non-null `Int64`
/// ordinals in `[0, count)`, split at the standard batch width. The
/// cardinality cap is re-enforced here — a plan can arrive over the wire
/// without ever passing the binder, and this source materializes, so an
/// unbounded count is an unbounded memory commitment.
pub(super) fn synthesize_range_batches(
	count: i64,
	schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	const RANGE_BATCH_ROWS: i64 = 8_192;
	if count > crate::ir::RANGE_MAX_CARDINALITY {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"range cardinality {count} exceeds the materializing source's cap of {}",
				crate::ir::RANGE_MAX_CARDINALITY
			),
		});
	}
	if count <= 0 {
		return Ok(vec![RecordBatch::new_empty(schema.clone())]);
	}
	let mut batches =
		Vec::with_capacity(((count + RANGE_BATCH_ROWS - 1) / RANGE_BATCH_ROWS) as usize);
	let mut start = 0_i64;
	while start < count {
		let end = (start + RANGE_BATCH_ROWS).min(count);
		let ordinals = arrow::array::Int64Array::from_iter_values(start..end);
		batches.push(
			RecordBatch::try_new(schema.clone(), vec![std::sync::Arc::new(ordinals)])
				.expect("range schema is the one ordinal column"),
		);
		start = end;
	}
	Ok(batches)
}
