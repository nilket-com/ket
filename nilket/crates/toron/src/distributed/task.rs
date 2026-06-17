use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::distributed::budget::{ByteCount, TaskBudget};
use crate::distributed::context::TaskExecutionContext;
use crate::distributed::identity::{
	AttemptId, ExchangeId, PartitionId, QueryId, StageId, TaskId, TaskIdentity, WorkerId,
};
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::ir::{BoundExprProgram, ExecutionPlan, JoinReplayId, JoinStrategy, TableId};
use crate::server::spill::SpillId;
use wendelon::schema::{RuntimeFilterDescriptor, RuntimeFilterKeyType, RuntimeFilterKind};

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
	pub join_observations: Vec<JoinRuntimeObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRuntimeObservation {
	pub replay_id: JoinReplayId,
	pub strategy: JoinStrategy,
	pub build_rows: RowCount,
	pub probe_rows: RowCount,
	pub output_rows: RowCount,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinRuntimeObservationMode {
	#[default]
	Disabled,
	MeasureJoins {
		replay_ids: BTreeSet<JoinReplayId>,
	},
}

impl JoinRuntimeObservationMode {
	pub fn measures(&self, replay_id: JoinReplayId) -> bool {
		match self {
			Self::Disabled => false,
			Self::MeasureJoins { replay_ids } => replay_ids.contains(&replay_id),
		}
	}

	pub fn is_disabled(&self) -> bool {
		matches!(self, Self::Disabled)
	}
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
	pub context: TaskExecutionContext,
	pub join_runtime_observation_mode: JoinRuntimeObservationMode,
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
		#[serde(default)]
		runtime_filter: Option<RuntimeFilterDescriptor>,
	},
	ShuffleConsumer {
		exchange_id: ExchangeId,
		producer_stage_id: StageId,
		partition_id: PartitionId,
		producer_partitions: Vec<ShufflePartitionRef>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShufflePartitionRef {
	Sealed(ProducerPartitionRef),
	Live(LiveProducerPartitionRef),
}

impl ShufflePartitionRef {
	pub const fn address(&self) -> PartitionAddress {
		match self {
			Self::Sealed(partition) => partition.address,
			Self::Live(partition) => partition.address,
		}
	}

	pub fn endpoint(&self) -> &WorkerEndpoint {
		match self {
			Self::Sealed(partition) => &partition.endpoint,
			Self::Live(partition) => &partition.endpoint,
		}
	}

	pub const fn as_sealed(&self) -> Option<&ProducerPartitionRef> {
		match self {
			Self::Sealed(partition) => Some(partition),
			Self::Live(_) => None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveProducerPartitionRef {
	pub address: PartitionAddress,
	pub worker_id: WorkerId,
	pub endpoint: WorkerEndpoint,
}

pub fn is_streamable_exchange_producer(spec: &TaskSpec) -> bool {
	matches!(spec.output, TaskOutputBinding::ShuffleProducer { .. })
		&& matches!(
			&spec.plan,
			ExecutionPlan::Exchange { input, .. } if streamable_unary_plan(input)
		)
}

pub fn is_streamable_shuffle_consumer(spec: &TaskSpec) -> bool {
	spec.inputs
		.iter()
		.any(|input| matches!(input, TaskInputBinding::ShuffleConsumer { .. }))
		&& streamable_unary_plan(&spec.plan)
}

pub fn is_streamable_final_result(spec: &TaskSpec) -> bool {
	matches!(spec.output, TaskOutputBinding::FinalResult) && streamable_unary_plan(&spec.plan)
}

fn streamable_unary_plan(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Scan { .. } => true,
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Take { input, .. } => streamable_unary_plan(input),
		ExecutionPlan::Exchange { .. }
		| ExecutionPlan::Union { .. }
		| ExecutionPlan::Repeat { .. }
		| ExecutionPlan::Sort { .. }
		| ExecutionPlan::Window { .. }
		| ExecutionPlan::Aggregate { .. }
		| ExecutionPlan::Join { .. }
		// ADR 255: opaque-local — a recursive CTE is a self-contained local fixpoint, not a
		// streamable unary scan chain, and its self-reference leaf is not a streamable source.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. }
		| ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => false,
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskOutputBinding {
	FinalResult,
	ShuffleProducer {
		exchange_id: ExchangeId,
		partition_count: PartitionCount,
		partition_key: BoundExprProgram,
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
	#[serde(default)]
	pub runtime_filter_fragment: Option<RuntimeFilterFragment>,
	pub backend: PartitionBackend,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFilterFragment {
	pub key_encoding_version: u32,
	pub key_types: Vec<RuntimeFilterKeyType>,
	pub kind: RuntimeFilterKind,
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
	use arrow::datatypes::{DataType, Field, Schema};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, RuntimeFilterBloom,
		SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
	};

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
	fn scan_binding_round_trips_with_and_without_runtime_filter() {
		for runtime_filter in [None, Some(runtime_filter_descriptor())] {
			let input = TaskInputBinding::Scan {
				share: Some(ScanShare {
					worker_index: 2,
					worker_count: 4,
				}),
				runtime_filter,
			};

			let encoded = postcard::to_allocvec(&input).expect("encode scan binding");
			let decoded: TaskInputBinding =
				postcard::from_bytes(&encoded).expect("decode scan binding");

			assert_eq!(decoded, input);
		}
	}

	#[test]
	fn partition_manifest_round_trips_with_and_without_fragment() {
		for runtime_filter_fragment in [None, Some(runtime_filter_fragment())] {
			let manifest = PartitionManifest {
				query_id: QueryId::from_u128(1),
				stage_id: StageId::new(2),
				task_id: TaskId::new(3),
				attempt_id: AttemptId::new(4),
				exchange_id: ExchangeId::new(5),
				producer_worker_id: WorkerId::from_u128(6),
				producer_endpoint: WorkerEndpoint::new("127.0.0.1:9191"),
				partitions: vec![PartitionEntry {
					partition_id: PartitionId::new(7),
					size_bytes: ByteCount::new(8),
					row_count: RowCount::new(9),
					digest: ArrowIpcDigest::new([10; 32]),
				}],
				runtime_filter_fragment,
				backend: PartitionBackend::InMemory,
			};

			let encoded = postcard::to_allocvec(&manifest).expect("encode manifest");
			let decoded: PartitionManifest =
				postcard::from_bytes(&encoded).expect("decode manifest");

			assert_eq!(decoded, manifest);
		}
	}

	fn runtime_filter_descriptor() -> RuntimeFilterDescriptor {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([6; 16]),
			1,
			"default",
			"runtime_filter_test",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor");
		RuntimeFilterDescriptor {
			key_encoding_version: 1,
			key_types: vec![RuntimeFilterKeyType::Int64],
			probe_key_columns: vec![0],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: 7,
				bit_count: 8,
				hash_count: 1,
				byte_budget: 1,
				target_fp_rate_bp: 100,
				payload: vec![0xFF],
			}),
			probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
		}
	}

	fn runtime_filter_fragment() -> RuntimeFilterFragment {
		RuntimeFilterFragment {
			key_encoding_version: 1,
			key_types: vec![RuntimeFilterKeyType::Int64],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: 7,
				bit_count: 16,
				hash_count: 2,
				byte_budget: 2,
				target_fp_rate_bp: 100,
				payload: vec![0b1010_0101, 0b0101_1010],
			}),
		}
	}
}
