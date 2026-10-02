use serde::{Deserialize, Serialize};
use thiserror::Error;
use wendelon::schema::PhysicalDescriptorFingerprint;

use crate::distributed::budget::{BudgetDimension, ByteCount};
use crate::distributed::identity::{AttemptId, WorkerId};
use crate::distributed::task::PartitionAddress;
use crate::ir::TableId;
use crate::wire::ProtocolVersion;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskFailureKind {
	ProtocolMismatch,
	DescriptorEvolved {
		table_id: TableId,
		expected: Option<PhysicalDescriptorFingerprint>,
		current: Option<PhysicalDescriptorFingerprint>,
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
	QueryLeaseMissing,
	QueryLeaseExpired,
	Internal {
		reason: String,
	},
	/// ADR 626: a consumer's sealed pull could not establish a stream to the accepted producer of
	/// `address` because of a liveness failure (its establishment deadline expired, or QUIC's idle
	/// timeout lapsed). A report for the scheduler to bind and confirm, never a verdict on the
	/// producer by itself. Appended last: earlier variants keep their wire encoding.
	ProducerUnreachable {
		producer_worker: WorkerId,
		producer_endpoint: String,
		address: PartitionAddress,
		failure: EstablishmentFailure,
		elapsed_micros: u64,
	},
	/// ADR 626: a peer answered establishment with a refusal (a TLS, certificate, version or
	/// protocol refusal, or a close). Never a liveness signal: terminal, never exclusion or replay.
	PeerRefused {
		peer_worker: WorkerId,
		peer_endpoint: String,
		failure: EstablishmentFailure,
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
			Self::QueryLeaseMissing => "QueryLeaseMissing",
			Self::QueryLeaseExpired => "QueryLeaseExpired",
			Self::Internal { .. } => "Internal",
			Self::ProducerUnreachable { .. } => "ProducerUnreachable",
			Self::PeerRefused { .. } => "PeerRefused",
		}
	}
}

/// ADR 626: why a QUIC connection or stream could not be established, typed from quinn's own
/// errors (never from message text). Only the two liveness variants may lead to recovery; every
/// other variant is a refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EstablishmentFailure {
	/// The caller's establishment deadline expired before the stream opened.
	DeadlineExpired { deadline_millis: u64 },
	/// QUIC's idle timeout lapsed during establishment (`ConnectionError::TimedOut`).
	IdleTimedOut,
	/// The peer implements no supported QUIC version.
	VersionMismatch,
	/// The TLS handshake failed with this alert (a QUIC crypto error code).
	Tls { alert: u8, reason: String },
	/// Any other QUIC transport error.
	Transport { code: u64, reason: String },
	/// The peer's QUIC stack closed the connection.
	PeerClosed { code: u64, reason: String },
	/// The peer's application closed the connection.
	PeerApplicationClosed { code: u64, reason: String },
	/// The peer reset the connection (it has usually restarted).
	PeerReset,
	/// This side closed the connection.
	LocallyClosed,
	/// Not enough connection-ID space for a new connection.
	CidsExhausted,
	/// The connection could not be started at all (endpoint stopping, bad name or address, no
	/// client configuration, unsupported version).
	Connect { reason: String },
}

impl EstablishmentFailure {
	/// Only a lapsed deadline or idle timeout says the peer may be gone.
	pub const fn is_liveness(&self) -> bool {
		matches!(self, Self::DeadlineExpired { .. } | Self::IdleTimedOut)
	}
}

impl std::fmt::Display for EstablishmentFailure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::DeadlineExpired { deadline_millis } => {
				write!(f, "establishment deadline of {deadline_millis} ms expired")
			}
			Self::IdleTimedOut => write!(f, "timed out"),
			Self::VersionMismatch => write!(f, "peer doesn't implement any supported version"),
			Self::Tls { alert, reason } => write!(f, "TLS alert {alert}: {reason}"),
			Self::Transport { code, reason } => write!(f, "transport error {code}: {reason}"),
			Self::PeerClosed { code, reason } => write!(f, "aborted by peer ({code}): {reason}"),
			Self::PeerApplicationClosed { code, reason } => {
				write!(f, "closed by peer ({code}): {reason}")
			}
			Self::PeerReset => write!(f, "reset by peer"),
			Self::LocallyClosed => write!(f, "closed"),
			Self::CidsExhausted => write!(f, "CIDs exhausted"),
			Self::Connect { reason } => write!(f, "{reason}"),
		}
	}
}

impl std::error::Error for EstablishmentFailure {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionMissingReason {
	AttemptStale {
		expected_attempt: AttemptId,
		observed_attempt: AttemptId,
	},
	CancelledTaskCleanup,
	QueryFailedCleanup,
	NamespaceRetiring,
	Reclaimed,
	Abandoned,
	NamespaceUnknown,
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
	/// ADR 628 B0: the worker answered a lease issuance or renewal with `Ignored`.
	#[error("query lease ignored by worker for {query_id:?}: {reason:?}")]
	QueryLeaseIgnored {
		query_id: crate::distributed::identity::QueryId,
		reason: crate::distributed::namespace_lifecycle::LeaseIgnored,
	},
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn task_failure_taxonomy_has_nine_variants() {
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
