use serde::{Deserialize, Serialize};

use crate::distributed::budget::{ByteCount, TaskBudget};
use crate::distributed::identity::{
	AttemptId, ExchangeId, PartitionId, QueryId, StageId, TaskId, TaskIdentity, WorkerId,
};
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::ir::{ExecutionExpr, ExecutionPlan};
use crate::server::spill::SpillId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArrowIpcDigest(pub [u8; 32]);

impl ArrowIpcDigest {
	pub const fn new(value: [u8; 32]) -> Self {
		Self(value)
	}

	pub const fn get(self) -> [u8; 32] {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct RowCount(pub u64);

impl RowCount {
	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct PartitionCount(pub u32);

impl PartitionCount {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct AttemptLimit(pub u32);

impl AttemptLimit {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceContext {
	pub trace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
	pub identity: TaskIdentity,
	pub partition: Option<PartitionId>,
	pub plan: ExecutionPlan,
	pub inputs: Vec<TaskInputBinding>,
	pub output: TaskOutputBinding,
	pub budget: TaskBudget,
	pub trace: Option<TraceContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskInputBinding {
	Scan,
	ShuffleConsumer {
		exchange_id: ExchangeId,
		producer_stage_id: StageId,
		partition_id: PartitionId,
		producer_partitions: Vec<ProducerPartitionRef>,
	},
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerPartitionRef {
	pub address: PartitionAddress,
	pub worker_id: WorkerId,
	pub endpoint: WorkerEndpoint,
	pub size_bytes: ByteCount,
	pub digest: ArrowIpcDigest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskOutputBinding {
	FinalResult,
	ShuffleProducer {
		exchange_id: ExchangeId,
		partition_count: PartitionCount,
		partition_key: Vec<ExecutionExpr>,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionAddress {
	pub query_id: QueryId,
	pub exchange_id: ExchangeId,
	pub producer_stage_id: StageId,
	pub producer_task_id: TaskId,
	pub attempt_id: AttemptId,
	pub partition_id: PartitionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionManifest {
	pub query_id: QueryId,
	pub stage_id: StageId,
	pub task_id: TaskId,
	pub attempt_id: AttemptId,
	pub exchange_id: ExchangeId,
	pub producer_worker_id: WorkerId,
	pub producer_endpoint: WorkerEndpoint,
	pub partitions: Vec<PartitionEntry>,
	pub backend: PartitionBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionEntry {
	pub partition_id: PartitionId,
	pub size_bytes: ByteCount,
	pub row_count: RowCount,
	pub digest: ArrowIpcDigest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionBackend {
	InMemory,
	EncryptedSpill { spill_id: SpillId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskCompletionOutput {
	FinalResult {
		output_digest: Option<ArrowIpcDigest>,
	},
	ShuffleProducer {
		manifest: PartitionManifest,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
	pub max_attempts: AttemptLimit,
}

impl RetryPolicy {
	pub const fn one_retry() -> Self {
		Self {
			max_attempts: AttemptLimit::new(2),
		}
	}
}

impl Default for RetryPolicy {
	fn default() -> Self {
		Self::one_retry()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn retry_policy_defaults_to_one_retry() {
		let policy = RetryPolicy::default();

		assert_eq!(policy.max_attempts, AttemptLimit::new(2));
	}

	#[test]
	fn partition_address_includes_attempt_identity() {
		let address = PartitionAddress {
			query_id: QueryId::from_u128(1),
			exchange_id: ExchangeId::new(2),
			producer_stage_id: StageId::new(3),
			producer_task_id: TaskId::new(4),
			attempt_id: AttemptId::new(5),
			partition_id: PartitionId::new(6),
		};

		assert_eq!(address.attempt_id.get(), 5);
		assert_eq!(address.partition_id.get(), 6);
	}
}
