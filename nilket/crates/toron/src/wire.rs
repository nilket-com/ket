use crate::distributed::budget::{BudgetDimension, ByteCount};
use crate::distributed::identity::{
	AttemptId, FrameByteLen, LeaseEpoch, QueryId, StreamSeq, TaskIdentity, WorkerId,
};
use crate::distributed::namespace_lifecycle::QueryTerminalCause;
use crate::distributed::task::{
	PartitionAddress, RowCount, TaskCompletionOutput, TaskSpec, TaskTelemetry, TraceContext,
};
use crate::error::{RuntimeError, TaskFailureKind, WireError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProtocolVersion(pub u16);

impl ProtocolVersion {
	pub const CURRENT: Self = Self(2);

	pub const fn new(value: u16) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u16 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MessageKind {
	StartTask,
	TaskStarted,
	TaskBatch,
	TaskComplete,
	TaskFailed,
	CancelTask,
	TaskCancelled,
	Progress,
	Heartbeat,
	PullPartition,
	PartitionData,
	PartitionMissing,
	QueryLease,
	QueryLeaseAccepted,
	RetireQuery,
	QueryRetired,
}

impl MessageKind {
	pub const TASK_LIFECYCLE_COUNT: usize = 13;
	pub const DATA_MOVEMENT_COUNT: usize = 3;
	pub const COUNT: usize = Self::TASK_LIFECYCLE_COUNT + Self::DATA_MOVEMENT_COUNT;
	pub const ALL: [Self; Self::COUNT] = [
		Self::StartTask,
		Self::TaskStarted,
		Self::TaskBatch,
		Self::TaskComplete,
		Self::TaskFailed,
		Self::CancelTask,
		Self::TaskCancelled,
		Self::Progress,
		Self::Heartbeat,
		Self::PullPartition,
		Self::PartitionData,
		Self::PartitionMissing,
		Self::QueryLease,
		Self::QueryLeaseAccepted,
		Self::RetireQuery,
		Self::QueryRetired,
	];

	pub const fn is_task_lifecycle(self) -> bool {
		matches!(
			self,
			Self::StartTask
				| Self::TaskStarted
				| Self::TaskBatch
				| Self::TaskComplete
				| Self::TaskFailed
				| Self::CancelTask
				| Self::TaskCancelled
				| Self::Progress
				| Self::Heartbeat
				| Self::QueryLease
				| Self::QueryLeaseAccepted
				| Self::RetireQuery
				| Self::QueryRetired
		)
	}

	pub const fn is_data_movement(self) -> bool {
		matches!(
			self,
			Self::PullPartition | Self::PartitionData | Self::PartitionMissing
		)
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnvelopeIdentity {
	Task(TaskIdentity),
	Partition(PartitionAddress),
	Worker {
		worker_id: WorkerId,
		lease_epoch: LeaseEpoch,
	},
	Query(QueryId),
}

impl EnvelopeIdentity {
	pub const fn task(identity: TaskIdentity) -> Self {
		Self::Task(identity)
	}

	pub const fn partition(address: PartitionAddress) -> Self {
		Self::Partition(address)
	}

	pub const fn worker(worker_id: WorkerId, lease_epoch: LeaseEpoch) -> Self {
		Self::Worker {
			worker_id,
			lease_epoch,
		}
	}

	pub const fn query(query_id: QueryId) -> Self {
		Self::Query(query_id)
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireEnvelope {
	pub protocol_version: ProtocolVersion,
	pub kind: MessageKind,
	pub identity: EnvelopeIdentity,
	pub stream_seq: Option<StreamSeq>,
	pub payload_len: FrameByteLen,
	pub trace: Option<TraceContext>,
	#[serde(with = "serde_bytes")]
	pub payload: Vec<u8>,
}

impl WireEnvelope {
	pub fn try_new(
		kind: MessageKind,
		identity: EnvelopeIdentity,
		stream_seq: Option<StreamSeq>,
		trace: Option<TraceContext>,
		payload: Vec<u8>,
	) -> Result<Self, WireError> {
		let payload_len = FrameByteLen::new(payload.len()).ok_or(WireError::FrameTooLarge {
			payload_len: payload.len(),
		})?;
		Ok(Self {
			protocol_version: ProtocolVersion::CURRENT,
			kind,
			identity,
			stream_seq,
			payload_len,
			trace,
			payload,
		})
	}

	pub fn encode(&self) -> Vec<u8> {
		postcard::to_allocvec(self).expect("failed to encode WireEnvelope")
	}

	pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
		Ok(postcard::from_bytes(bytes)?)
	}

	pub fn payload_len_matches(&self) -> bool {
		FrameByteLen::new(self.payload.len()) == Some(self.payload_len)
	}

	pub fn validate_runtime(
		&self,
		expected_protocol: ProtocolVersion,
		max_payload_len: FrameByteLen,
	) -> Result<(), RuntimeError> {
		if self.protocol_version != expected_protocol {
			return Err(RuntimeError::ProtocolMismatch {
				expected: expected_protocol,
				observed: self.protocol_version,
			});
		}

		if self.payload_len > max_payload_len {
			return Err(RuntimeError::TaskFailure {
				kind: TaskFailureKind::BudgetExhausted {
					dimension: BudgetDimension::FrameSize,
					requested: ByteCount::new(u64::from(self.payload_len.get())),
					available: ByteCount::new(u64::from(max_payload_len.get())),
				},
			});
		}

		Ok(())
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrowIpcBytes(#[serde(with = "serde_bytes")] pub Vec<u8>);

impl ArrowIpcBytes {
	pub fn new(value: Vec<u8>) -> Self {
		Self(value)
	}

	pub fn as_slice(&self) -> &[u8] {
		&self.0
	}

	pub fn into_inner(self) -> Vec<u8> {
		self.0
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartTaskPayload {
	pub spec: TaskSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStartedPayload {
	pub identity: TaskIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskBatchPayload {
	pub identity: TaskIdentity,
	pub stream_seq: StreamSeq,
	pub payload: ArrowIpcBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCompletePayload {
	pub identity: TaskIdentity,
	pub output: TaskCompletionOutput,
	#[serde(default)]
	pub telemetry: Option<TaskTelemetry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFailedPayload {
	pub identity: TaskIdentity,
	pub kind: TaskFailureKind,
}

impl TaskFailedPayload {
	pub fn new_worker_emitted(
		identity: TaskIdentity,
		kind: TaskFailureKind,
	) -> Result<Self, TaskFailureKind> {
		if kind.worker_emittable() {
			Ok(Self { identity, kind })
		} else {
			Err(kind)
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelTaskPayload {
	pub identity: TaskIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCancelledPayload {
	pub identity: TaskIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressPayload {
	pub identity: TaskIdentity,
	pub bytes_produced: ByteCount,
	pub rows_produced: RowCount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatPayload {
	pub worker_id: WorkerId,
	pub lease_epoch: LeaseEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryLeasePayload {
	pub query_id: QueryId,
	pub lease_term_millis: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryLeaseAcceptedPayload {
	pub query_id: QueryId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireQueryPayload {
	pub query_id: QueryId,
	pub cause: QueryTerminalCause,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRetiredPayload {
	pub query_id: QueryId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullPartitionPayload {
	pub address: PartitionAddress,
	pub expected_attempt: AttemptId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionDataPayload {
	pub address: PartitionAddress,
	pub stream_seq: StreamSeq,
	pub payload: ArrowIpcBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionMissingPayload {
	pub address: PartitionAddress,
	pub reason: crate::error::PartitionMissingReason,
}

pub fn encode_payload<Payload: Serialize>(payload: &Payload) -> Vec<u8> {
	postcard::to_allocvec(payload).expect("failed to encode wire payload")
}

pub fn decode_payload<Payload: DeserializeOwned>(payload: &[u8]) -> Result<Payload, WireError> {
	Ok(postcard::from_bytes(payload)?)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::identity::{ExchangeId, PartitionId, QueryId, StageId, TaskId};
	use crate::error::PartitionMissingReason;

	fn identity() -> TaskIdentity {
		TaskIdentity::new(
			QueryId::from_u128(1),
			StageId::new(2),
			TaskId::new(3),
			AttemptId::first(),
		)
	}

	fn address() -> PartitionAddress {
		PartitionAddress {
			query_id: QueryId::from_u128(1),
			exchange_id: ExchangeId::new(7),
			producer_stage_id: StageId::new(2),
			producer_task_id: TaskId::new(3),
			attempt_id: AttemptId::first(),
			partition_id: PartitionId::new(5),
		}
	}

	fn identity_for(kind: MessageKind) -> EnvelopeIdentity {
		if matches!(
			kind,
			MessageKind::QueryLease
				| MessageKind::QueryLeaseAccepted
				| MessageKind::RetireQuery
				| MessageKind::QueryRetired
		) {
			EnvelopeIdentity::query(QueryId::from_u128(1))
		} else if kind == MessageKind::Heartbeat {
			EnvelopeIdentity::worker(WorkerId::from_u128(11), LeaseEpoch::new(1))
		} else if kind.is_data_movement() {
			EnvelopeIdentity::partition(address())
		} else {
			EnvelopeIdentity::task(identity())
		}
	}

	#[test]
	fn envelope_round_trips_every_message_kind() {
		for kind in MessageKind::ALL {
			let envelope = WireEnvelope::try_new(
				kind,
				identity_for(kind),
				Some(StreamSeq::new(9)),
				Some(TraceContext {
					trace_id: "trace-1".to_string(),
				}),
				vec![1, 2, 3],
			)
			.expect("envelope");

			let decoded = WireEnvelope::decode(&envelope.encode()).expect("decode");
			assert_eq!(decoded, envelope);
			assert!(decoded.payload_len_matches());
		}
	}

	#[test]
	fn protocol_mismatch_is_typed_runtime_error() {
		let mut envelope = WireEnvelope::try_new(
			MessageKind::TaskStarted,
			EnvelopeIdentity::task(identity()),
			None,
			None,
			Vec::new(),
		)
		.expect("envelope");
		envelope.protocol_version = ProtocolVersion::new(3);

		let error = envelope
			.validate_runtime(ProtocolVersion::CURRENT, FrameByteLen::from_u32(1024))
			.expect_err("protocol mismatch");

		assert!(matches!(
			error,
			RuntimeError::ProtocolMismatch {
				expected: ProtocolVersion::CURRENT,
				observed: ProtocolVersion(3)
			}
		));
	}

	#[test]
	fn oversized_frame_maps_to_frame_size_budget_exhaustion() {
		let envelope = WireEnvelope::try_new(
			MessageKind::PartitionData,
			EnvelopeIdentity::partition(address()),
			Some(StreamSeq::new(0)),
			None,
			vec![1, 2, 3, 4],
		)
		.expect("envelope");

		let error = envelope
			.validate_runtime(ProtocolVersion::CURRENT, FrameByteLen::from_u32(3))
			.expect_err("oversized frame");

		assert!(matches!(
			error,
			RuntimeError::TaskFailure {
				kind: TaskFailureKind::BudgetExhausted {
					dimension: BudgetDimension::FrameSize,
					requested,
					available,
				}
			} if requested == ByteCount::new(4) && available == ByteCount::new(3)
		));
	}

	#[test]
	fn partition_missing_payload_round_trips_all_reasons() {
		let reasons = [
			PartitionMissingReason::AttemptStale {
				expected_attempt: AttemptId::first(),
				observed_attempt: AttemptId::new(1),
			},
			PartitionMissingReason::CancelledTaskCleanup,
			PartitionMissingReason::QueryFailedCleanup,
			PartitionMissingReason::NamespaceRetiring,
			PartitionMissingReason::Reclaimed,
			PartitionMissingReason::Abandoned,
			PartitionMissingReason::NamespaceUnknown,
			PartitionMissingReason::WorkerLost,
			PartitionMissingReason::StoreEvicted,
		];

		for reason in reasons {
			let payload = PartitionMissingPayload {
				address: address(),
				reason,
			};
			let encoded = encode_payload(&payload);
			let decoded: PartitionMissingPayload = decode_payload(&encoded).expect("decode");
			assert_eq!(decoded, payload);
		}
	}

	#[test]
	fn query_lifecycle_carriers_are_query_scoped_and_typed() {
		let query_id = QueryId::from_u128(42);
		let lease = QueryLeasePayload {
			query_id,
			lease_term_millis: 35_001,
		};
		let decoded: QueryLeasePayload =
			decode_payload(&encode_payload(&lease)).expect("decode lease");
		assert_eq!(decoded, lease);

		for cause in [
			QueryTerminalCause::Success,
			QueryTerminalCause::Cancelled,
			QueryTerminalCause::Failed,
			QueryTerminalCause::Abandoned,
		] {
			let retirement = RetireQueryPayload { query_id, cause };
			let decoded: RetireQueryPayload =
				decode_payload(&encode_payload(&retirement)).expect("decode retirement");
			assert_eq!(decoded, retirement);
		}
		assert_eq!(
			EnvelopeIdentity::query(query_id),
			EnvelopeIdentity::Query(query_id)
		);
	}

	#[test]
	fn worker_lost_is_not_worker_emittable_task_failed_payload() {
		let rejected = TaskFailedPayload::new_worker_emitted(
			identity(),
			TaskFailureKind::WorkerLost {
				worker_id: WorkerId::from_u128(9),
			},
		)
		.expect_err("worker lost is coordinator-synthesized");
		assert!(matches!(rejected, TaskFailureKind::WorkerLost { .. }));

		let accepted = TaskFailedPayload::new_worker_emitted(
			identity(),
			TaskFailureKind::TaskFailed {
				reason: "operator failed".to_string(),
				retryable: true,
			},
		)
		.expect("task failure is worker-emittable");
		assert!(accepted.kind.worker_emittable());
	}
}
