//! ADR 606: the exchange's repartition and finalize over an already materialized input, shared
//! by `bound_exchange` and the reoriented chain's reuse of a cap check's batches.

use std::time::Instant;

use arrow::record_batch::RecordBatch;

use super::{Executor, finalize_exchange_partitions, join};
use crate::error::ExecutionError;
use crate::ir::BoundExprProgram;

impl Executor {
	/// `bound_exchange`'s repartition and finalize over an already materialized input: every
	/// batch repartitioned by the key into `partition_count` partitions, each partition's batches
	/// concatenated, an empty partition as one empty batch, one output batch per partition in
	/// partition order; the repartition time recorded. ADR 606 builds from a cap check's batches
	/// through this same function, so its exchange is `bound_exchange`'s, not a copy of it.
	pub(in crate::exec) fn exchange_input_batches(
		&self,
		input_batches: Vec<RecordBatch>,
		schema: arrow::datatypes::SchemaRef,
		partition_count: u32,
		key: &BoundExprProgram,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if partition_count == 0 {
			return Err(ExecutionError::InvalidExpression {
				message: "exchange partition_count must be greater than zero".to_string(),
			});
		}
		let partition_count_usize =
			usize::try_from(partition_count).expect("partition count fits usize");
		let mut partition_batches: Vec<Vec<RecordBatch>> =
			(0..partition_count_usize).map(|_| Vec::new()).collect();

		// Accumulated in a local, then recorded once: no per-batch locking or allocation
		// inside the timed path.
		let repartition_started = Instant::now();
		for batch in input_batches {
			for (partition, partitioned) in
				self.repartition_exchange_batch(&batch, partition_count, key)?
			{
				partition_batches[partition].push(partitioned);
			}
		}
		let finalized = finalize_exchange_partitions(schema, partition_batches);
		join::record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.exchange_repartition_micros = task
				.phases
				.exchange_repartition_micros
				.saturating_add(join::duration_micros(repartition_started));
		});
		finalized
	}
}
