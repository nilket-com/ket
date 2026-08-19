use serde::{Deserialize, Serialize};

use crate::exec::ExecutionContext;
use crate::ir::SessionId;
use crate::storage::{HlcBound, TenantId, TraceContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskExecutionContext {
	pub session_id: SessionId,
	pub tenant_id: TenantId,
	pub read_bound: HlcBound,
	/// ADR 556: the coordinator's statement timestamp; workers must not mint
	/// their own clock value for current-timestamp expressions.
	pub statement_timestamp_nanos: i64,
	pub trace_context: Option<TraceContext>,
	pub quantile_execution_seed: [u8; 32],
	pub quantile_state_allowance: usize,
}

const fn default_quantile_state_allowance() -> usize {
	1_024
}

/// The seed [`TaskExecutionContext::local_test`] supplies. Named and confined to
/// the test constructor on purpose: Wendelon ADR 83 requires compaction
/// randomness drawn independently of the data, which a constant cannot satisfy.
/// `new` takes the seed as a parameter so no production caller can inherit this
/// value by omission.
pub const LOCAL_TEST_QUANTILE_SEED: [u8; 32] = [0xA5; 32];

impl TaskExecutionContext {
	pub const fn new(
		session_id: SessionId,
		tenant_id: TenantId,
		read_bound: HlcBound,
		statement_timestamp_nanos: i64,
		trace_context: Option<TraceContext>,
		quantile_execution_seed: [u8; 32],
		quantile_state_allowance: usize,
	) -> Self {
		Self {
			session_id,
			tenant_id,
			read_bound,
			statement_timestamp_nanos,
			trace_context,
			quantile_execution_seed,
			quantile_state_allowance,
		}
	}

	pub const fn local_test() -> Self {
		Self::new(
			SessionId(1),
			TenantId(1),
			HlcBound(1),
			0,
			None,
			LOCAL_TEST_QUANTILE_SEED,
			default_quantile_state_allowance(),
		)
	}

	pub fn from_execution_context(context: &ExecutionContext) -> Self {
		Self::new(
			context.session_id,
			context.tenant_id,
			context.read_bound,
			context.statement_timestamp_nanos,
			context.trace_context,
			context.quantile_execution_seed,
			context.quantile_state_allowance,
		)
	}

	pub const fn with_quantile_execution(mut self, seed: [u8; 32], state_allowance: usize) -> Self {
		self.quantile_execution_seed = seed;
		self.quantile_state_allowance = state_allowance;
		self
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn statement_timestamp_survives_the_coordinator_worker_context_boundary() {
		let context = ExecutionContext::local_test().with_statement_timestamp_nanos(42);
		let task = TaskExecutionContext::from_execution_context(&context);
		assert_eq!(task.statement_timestamp_nanos, 42);
	}
}
