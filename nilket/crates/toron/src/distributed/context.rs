use serde::{Deserialize, Serialize};

use crate::exec::ExecutionContext;
use crate::ir::SessionId;
use crate::storage::{HlcBound, TenantId, TraceContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskExecutionContext {
	pub session_id: SessionId,
	pub tenant_id: TenantId,
	pub read_bound: HlcBound,
	pub trace_context: Option<TraceContext>,
	pub quantile_execution_seed: [u8; 32],
	pub quantile_state_allowance: usize,
}

const fn default_quantile_state_allowance() -> usize {
	1_024
}

impl TaskExecutionContext {
	pub const fn new(
		session_id: SessionId,
		tenant_id: TenantId,
		read_bound: HlcBound,
		trace_context: Option<TraceContext>,
	) -> Self {
		Self {
			session_id,
			tenant_id,
			read_bound,
			trace_context,
			quantile_execution_seed: [0xA5; 32],
			quantile_state_allowance: default_quantile_state_allowance(),
		}
	}

	pub const fn local_test() -> Self {
		Self::new(SessionId(1), TenantId(1), HlcBound(1), None)
	}

	pub fn from_execution_context(context: &ExecutionContext) -> Self {
		Self::new(
			context.session_id,
			context.tenant_id,
			context.read_bound,
			context.trace_context,
		)
		.with_quantile_execution(
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
