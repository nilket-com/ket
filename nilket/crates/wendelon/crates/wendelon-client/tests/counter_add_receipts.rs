//! How the SDK surfaces counter-add answers: a write-key conflict and an
//! invalid write identity are terminal `WriteFailed`s, an expired write id is a
//! `MutationStopped` with indeterminate knowledge, none of them is retried, and
//! a receipt whose values do not describe the request's own delta is refused as
//! an internal error.

mod counter_transport;

use std::sync::Arc;

use counter_transport::{CounterWire, committed_response, inner_client, response};
use wendelon::integration::compute::{StorageStatusKind, map_write_status_for_compute};
use wendelon::schema::{MutationCommitKnowledge, MutationStopReason, TenantId, WriteStatus};
use wendelon_client::{Client, ClientError, WriteOptions};

/// One add of `delta` on a client whose server answers every request with
/// `(status, previous, current)`. Returns the result and how many requests
/// reached the wire.
async fn add_answered(
	delta: u64,
	status: WriteStatus,
	previous: u64,
	current: u64,
) -> (
	Result<wendelon_client::CounterAddReceipt, ClientError>,
	usize,
) {
	// A node that answers OK or DUPLICATE has committed the add and says so.
	let wire = CounterWire::answering(Arc::new(move |request| {
		Some(
			if status == WriteStatus::OK || status == WriteStatus::DUPLICATE {
				committed_response(request, status, previous, current)
			} else {
				response(request, status, previous, current)
			},
		)
	}));
	let client = Client::from_inner(inner_client(&wire));
	let result = client
		.counter_fetch_add(TenantId::new(7), b"counter", delta, WriteOptions::default())
		.await;
	(result, wire.requests().len())
}

/// One add of 5 answered with exactly `status` and `knowledge` and a delta of 5.
async fn add_answered_with(
	status: WriteStatus,
	knowledge: MutationCommitKnowledge,
) -> (
	Result<wendelon_client::CounterAddReceipt, ClientError>,
	CounterWire,
) {
	let wire = CounterWire::answering(Arc::new(move |request| {
		Some(
			response(request, status, 10, 15)
				.with_mutation_outcome(knowledge, MutationStopReason::NONE),
		)
	}));
	let client = Client::from_inner(inner_client(&wire));
	let result = client
		.counter_fetch_add(TenantId::new(7), b"counter", 5, WriteOptions::default())
		.await;
	(result, wire)
}

/// ADR 0732: a counter receipt needs a committed answer, like every other receipt. A
/// success status without a commit, and a refusal status without known non-commit, are
/// answers whose outcome is unknown; a recorded refusal names the add and its knowledge.
#[tokio::test]
async fn a_counter_receipt_needs_a_committed_answer() {
	for status in [WriteStatus::OK, WriteStatus::DUPLICATE] {
		let (result, wire) = add_answered_with(status, MutationCommitKnowledge::COMMITTED).await;
		let receipt = result.unwrap_or_else(|error| panic!("{status:?}: {error:?}"));
		assert_eq!(receipt.duplicate, status == WriteStatus::DUPLICATE);
		assert_eq!(wire.requests().len(), 1);
	}
	let unknown = MutationCommitKnowledge(9);
	for (status, knowledge) in [
		(WriteStatus::OK, MutationCommitKnowledge::UNSPECIFIED),
		(WriteStatus::OK, MutationCommitKnowledge::INDETERMINATE),
		(WriteStatus::DUPLICATE, MutationCommitKnowledge::UNSPECIFIED),
		(WriteStatus::DUPLICATE, unknown),
		(
			WriteStatus::WRITE_REFUSED,
			MutationCommitKnowledge::COMMITTED,
		),
		(
			WriteStatus::WRITE_REFUSED,
			MutationCommitKnowledge::INDETERMINATE,
		),
		(
			WriteStatus::WRITE_REFUSED,
			MutationCommitKnowledge::UNSPECIFIED,
		),
		(WriteStatus::WRITE_REFUSED, unknown),
	] {
		let (result, wire) = add_answered_with(status, knowledge).await;
		let sent = counter_transport::write_id(&wire.requests()[0]);
		match result {
			Err(ClientError::MutationAnswerInconsistent {
				operation_id,
				status: got_status,
				knowledge: got_knowledge,
			}) => {
				assert_eq!((got_status, got_knowledge), (status, knowledge));
				assert_eq!(operation_id, sent);
			}
			other => panic!("{status:?} with {knowledge:?}: {other:?}"),
		}
		assert_eq!(
			wire.requests().len(),
			1,
			"{status:?} with {knowledge:?}: no retry"
		);
	}
	for status in [
		WriteStatus::WRITE_REFUSED,
		WriteStatus::OK,
		WriteStatus::DUPLICATE,
	] {
		let (result, wire) =
			add_answered_with(status, MutationCommitKnowledge::KNOWN_NOT_COMMITTED).await;
		let sent = counter_transport::write_id(&wire.requests()[0]);
		match result {
			Err(ClientError::MutationStopped {
				operation_id,
				status: WriteStatus::WRITE_REFUSED,
				knowledge: MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
				..
			}) => assert_eq!(operation_id, sent),
			other => panic!("{status:?} known not committed: {other:?}"),
		}
		assert_eq!(wire.requests().len(), 1);
	}
	assert_eq!(WriteStatus::WRITE_REFUSED.0, 26);
	assert_eq!(WriteStatus::WRITE_REFUSED.as_str(), "write_refused");
	assert_eq!(
		map_write_status_for_compute(WriteStatus::WRITE_REFUSED),
		StorageStatusKind::TerminalStorageError
	);
}

#[tokio::test]
async fn write_key_conflict_is_a_terminal_write_failure_sent_once() {
	assert_eq!(WriteStatus::WRITE_KEY_CONFLICT.0, 24);
	assert_eq!(
		WriteStatus::WRITE_KEY_CONFLICT.as_str(),
		"write_key_conflict"
	);
	assert_eq!(
		map_write_status_for_compute(WriteStatus::WRITE_KEY_CONFLICT),
		StorageStatusKind::TerminalStorageError
	);
	let (result, requests) = add_answered(5, WriteStatus::WRITE_KEY_CONFLICT, 0, 0).await;
	let Err(error) = result else {
		panic!("a write-key conflict must fail: {result:?}");
	};
	assert!(
		matches!(
			error,
			ClientError::WriteFailed {
				status: WriteStatus::WRITE_KEY_CONFLICT
			}
		),
		"{error:?}"
	);
	assert_eq!(
		error.storage_status_kind(),
		StorageStatusKind::TerminalStorageError
	);
	assert_eq!(requests, 1, "a write-key conflict is never retried");
}

#[tokio::test]
async fn a_receipt_must_describe_the_requests_own_delta() {
	for status in [WriteStatus::OK, WriteStatus::DUPLICATE] {
		let (ok, _) = add_answered(5, status, 10, 15).await;
		let receipt = ok.unwrap_or_else(|error| panic!("{status:?}: {error:?}"));
		assert_eq!((receipt.previous_value, receipt.current_value), (10, 15));
		assert_eq!(receipt.duplicate, status == WriteStatus::DUPLICATE);
		for (previous, current) in [(0, 7), (10, 13), (15, 10), (0, 0)] {
			let (result, _) = add_answered(5, status, previous, current).await;
			assert!(
				matches!(
					result,
					Err(ClientError::WriteFailed {
						status: WriteStatus::INTERNAL_ERROR
					})
				),
				"{status:?} receipt ({previous}, {current}) for +5 must be refused: {result:?}"
			);
		}
	}
}

/// One add answered with `status` and the given outcome axes.
async fn add_stopped(
	status: WriteStatus,
	knowledge: MutationCommitKnowledge,
) -> (
	Result<wendelon_client::CounterAddReceipt, ClientError>,
	Vec<wendelon::schema::CounterAddRequestMeta>,
) {
	let wire = CounterWire::answering(Arc::new(move |request| {
		Some(
			response(request, status, 0, 0)
				.with_mutation_outcome(knowledge, MutationStopReason::NONE),
		)
	}));
	let client = Client::from_inner(inner_client(&wire));
	let result = client
		.counter_fetch_add(TenantId::new(7), b"counter", 5, WriteOptions::default())
		.await;
	(result, wire.requests())
}

// T-WIRE (SDK): WRITE_OPERATION_EXPIRED (22) alone becomes `MutationStopped`
// carrying the echoed write id and the response's INDETERMINATE knowledge,
// sent once; WRITE_IDENTITY_INVALID (25) is a terminal `WriteFailed`, sent
// once. The controls: 24 and a server's 19 stay `WriteFailed`.
#[tokio::test]
async fn expired_write_id_stops_indeterminate_and_invalid_identity_fails() {
	let (result, requests) = add_stopped(
		WriteStatus::WRITE_OPERATION_EXPIRED,
		MutationCommitKnowledge::INDETERMINATE,
	)
	.await;
	let Err(error) = result else {
		panic!("an expired write id must fail: {result:?}");
	};
	assert_eq!(requests.len(), 1, "never retried");
	let ClientError::MutationStopped {
		operation_id,
		status,
		reason,
		knowledge,
	} = &error
	else {
		panic!("22 must be MutationStopped: {error:?}");
	};
	assert_eq!(*operation_id, requests[0].write_id);
	assert_eq!(
		(*status, *reason, *knowledge),
		(
			WriteStatus::WRITE_OPERATION_EXPIRED,
			MutationStopReason::NONE,
			MutationCommitKnowledge::INDETERMINATE
		)
	);
	assert_eq!(
		error.storage_status_kind(),
		StorageStatusKind::TerminalStorageError
	);

	assert_eq!(WriteStatus::WRITE_IDENTITY_INVALID.0, 25);
	assert_eq!(
		map_write_status_for_compute(WriteStatus::WRITE_IDENTITY_INVALID),
		StorageStatusKind::TerminalStorageError
	);
	for status in [
		WriteStatus::WRITE_IDENTITY_INVALID,
		WriteStatus::WRITE_KEY_CONFLICT,
		WriteStatus::WRITE_DEADLINE_EXCEEDED,
	] {
		let (result, requests) = add_stopped(status, MutationCommitKnowledge::UNSPECIFIED).await;
		assert!(
			matches!(result, Err(ClientError::WriteFailed { status: failed }) if failed == status),
			"{status:?}: {result:?}"
		);
		assert_eq!(requests.len(), 1, "{status:?} is never retried");
	}
}
