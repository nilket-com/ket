use serde::{Deserialize, Serialize};

use crate::distributed::budget::{ByteCount, TaskBudget};
use crate::distributed::identity::{
	AttemptId, ExchangeId, PartitionId, QueryId, StageId, TaskId, TaskIdentity, WorkerId,
};
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::ir::{ExecutionExpr, ExecutionPlan, TableId};
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTelemetry {
	pub labels: TaskTelemetryLabels,
	pub counters: TaskTelemetryCounters,
	pub phases: TaskTelemetryPhases,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTelemetryLabels {
	pub strategy: Option<String>,
	pub stage_role: String,
	pub operator_kind: String,
	pub worker_count: Option<u32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTelemetryCounters {
	pub pulled_shuffle_bytes: u64,
	pub left_shuffle_bytes: u64,
	pub right_shuffle_bytes: u64,
	pub published_shuffle_bytes: u64,
	pub build_rows: u64,
	pub probe_rows: u64,
	pub matched_rows: u64,
	pub emitted_rows: u64,
	pub emitted_batches: u64,
	pub spill_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTelemetryPhases {
	pub task_wall_micros: u64,
	pub shuffle_pull_decode_micros: u64,
	pub build_micros: u64,
	pub probe_micros: u64,
	pub spill_micros: u64,
	pub output_publish_micros: u64,
	pub task_setup_micros: u64,
	pub batch_encode_micros: u64,
	pub batch_send_micros: u64,
	pub batch_decode_micros: u64,
	pub dispatch_wait_micros: u64,
	pub telemetry_lock_micros: u64,
	pub framework_overhead_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanShare {
	pub worker_index: u32,
	pub worker_count: u32,
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
	Scan {
		share: Option<ScanShare>,
	},
	ShuffleConsumer {
		exchange_id: ExchangeId,
		producer_stage_id: StageId,
		partition_id: PartitionId,
		producer_partitions: Vec<ProducerPartitionRef>,
	},
}

pub fn shuffle_input_table_id(exchange_id: ExchangeId) -> TableId {
	TableId(u64::MAX - u64::from(exchange_id.get()))
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

	#[test]
	fn scan_share_round_trips_through_task_input_binding() {
		let input = TaskInputBinding::Scan {
			share: Some(ScanShare {
				worker_index: 2,
				worker_count: 4,
			}),
		};

		let encoded = postcard::to_allocvec(&input).expect("encode scan share");
		let decoded: TaskInputBinding = postcard::from_bytes(&encoded).expect("decode scan share");

		assert_eq!(decoded, input);
	}
}
