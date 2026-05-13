use serde::{Deserialize, Serialize};
use thiserror::Error;
use wendelon::schema::PhysicalDescriptorFingerprint;

use crate::distributed::budget::{BudgetDimension, ByteCount};
use crate::distributed::identity::{AttemptId, WorkerId};
use crate::wire::ProtocolVersion;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskFailureKind {
	ProtocolMismatch,
	DescriptorEvolved {
		expected: PhysicalDescriptorFingerprint,
		current: PhysicalDescriptorFingerprint,
	},
	WorkerLost {
		worker_id: WorkerId,
	},
	TaskFailed {
		reason: String,
		retryable: bool,
	},
	BudgetExhausted {
		dimension: BudgetDimension,
		requested: ByteCount,
		available: ByteCount,
	},
	Cancelled,
	Internal {
		reason: String,
	},
}

impl TaskFailureKind {
	pub const fn worker_emittable(&self) -> bool {
		!matches!(self, Self::WorkerLost { .. })
	}

	pub const fn taxonomy_label(&self) -> &'static str {
		match self {
			Self::ProtocolMismatch => "ProtocolMismatch",
			Self::DescriptorEvolved { .. } => "DescriptorEvolved",
			Self::WorkerLost { .. } => "WorkerLost",
			Self::TaskFailed { .. } => "TaskFailed",
			Self::BudgetExhausted { .. } => "BudgetExhausted",
			Self::Cancelled => "Cancelled",
			Self::Internal { .. } => "Internal",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionMissingReason {
	AttemptStale {
		expected_attempt: AttemptId,
		observed_attempt: AttemptId,
	},
	CancelledTaskCleanup,
	WorkerLost,
	StoreEvicted,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
	#[error("protocol version mismatch: expected {expected:?}, observed {observed:?}")]
	ProtocolMismatch {
		expected: ProtocolVersion,
		observed: ProtocolVersion,
	},
	#[error("partition store mismatch: {message}")]
	PartitionStoreMismatch { message: String },
	#[error("task failure: {kind:?}")]
	TaskFailure { kind: TaskFailureKind },
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn task_failure_taxonomy_has_seven_variants() {
		let worker_lost = TaskFailureKind::WorkerLost {
			worker_id: WorkerId::from_u128(1),
		};
		assert!(!worker_lost.worker_emittable());
		assert_eq!(worker_lost.taxonomy_label(), "WorkerLost");

		let task_failed = TaskFailureKind::TaskFailed {
			reason: "failed".to_string(),
			retryable: true,
		};
		assert!(task_failed.worker_emittable());
		assert_eq!(task_failed.taxonomy_label(), "TaskFailed");
	}
}
