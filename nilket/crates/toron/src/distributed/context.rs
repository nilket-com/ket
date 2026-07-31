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
	}
}
