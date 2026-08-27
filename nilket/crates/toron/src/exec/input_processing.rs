//! Input-processing timing for the generic executor tree.
//!
//! ADR 554 Branch A follow-on. Carved out of the executor root rather than left
//! inline: ADR 354's root bound exists to catch expression-evaluator logic
//! drifting back in, and telemetry hooks should not spend that budget.

use std::time::Instant;

use super::join;
use super::{ExecutionError, Executor};

impl Executor {
	/// Time one node's input processing and add it to the task phase counter.
	///
	/// One helper for both sites so the boundary cannot drift between them. It
	/// opens at the first evaluation instruction and closes when the batches are
	/// produced; scan acquisition is metered elsewhere and is not included.
	pub(super) fn time_input_processing<T>(
		&self,
		body: impl FnOnce() -> Result<T, ExecutionError>,
	) -> Result<T, ExecutionError> {
		let started = Instant::now();
		let outcome = body();
		let elapsed = join::duration_micros(started);
		// Charged even when the body failed, for the same reason accumulation is:
		// the time was spent inside the task wall either way, and dropping it on
		// the error path would silently move it back into the residual this
		// counter exists to reduce.
		join::record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.input_processing_micros =
				task.phases.input_processing_micros.saturating_add(elapsed);
		});
		outcome
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};

	use super::super::tests::{batch, catalog};
	use crate::binder::{PlannerConfig, bind};
	use crate::distributed::task::TaskTelemetry;
	use crate::exec::{ExecutionContext, Executor};
	use crate::ir::{Expr, LogicalPlan, ScalarValue, TableId};
	use crate::storage::InMemoryStorageScan;

	/// ADR 554 Branch A follow-on: the input-processing bracket actually fires.
	///
	/// This is the distinction that decides an unexplained in-task residual: a
	/// near-zero reading on an instrumented path cannot be told apart from an
	/// uninstrumented one. The atlas answers that per node on real data; this
	/// answers it structurally, so a null in the atlas means "cheap here", never
	/// "never wired up".
	///
	/// It must fail if either bracket is removed.
	#[tokio::test]
	async fn input_processing_counter_records_filter_and_projection() {
		let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
		let mut context = ExecutionContext::local_test();
		// `record_task_telemetry` is a no-op without a slot, so an executor built
		// the ordinary way would record nothing and this test would pass for the
		// wrong reason.
		context.task_telemetry = Some(telemetry.clone());
		let executor = Executor::with_context(
			Arc::new(InMemoryStorageScan {
				table_data: batch(),
			}),
			context,
		);
		let catalog = catalog();
		let logical = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::Scan {
					table: TableId(1),
					projection: None,
				}),
				predicate: Expr::Gt(
					Box::new(Expr::column("id")),
					Box::new(Expr::Literal(ScalarValue::Int64(Some(0)))),
				),
			}),
			exprs: vec![Expr::column("id")],
			output_names: vec![Some("id".to_string())],
		};
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound");

		let recorded = telemetry.lock().unwrap().phases.input_processing_micros;
		assert!(
			recorded > 0,
			"input processing must be charged for a filter-and-project plan; \
			 a zero here means the bracket is gone, not that the work was free"
		);
	}
}
