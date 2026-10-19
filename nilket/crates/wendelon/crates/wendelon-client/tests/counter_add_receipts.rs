//! How the SDK surfaces counter-add answers: a write-key conflict is a terminal
//! `WriteFailed`, never retried, and a receipt whose values do not describe the
//! request's own delta is refused as an internal error.

mod counter_transport;

use std::sync::Arc;

use counter_transport::{CounterWire, inner_client, response};
use wendelon::integration::compute::{StorageStatusKind, map_write_status_for_compute};
use wendelon::schema::{TenantId, WriteStatus};
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
	let wire = CounterWire::answering(Arc::new(move |request| {
		Some(response(request, status, previous, current))
	}));
	let client = Client::from_inner(inner_client(&wire));
	let result = client
		.counter_fetch_add(TenantId::new(7), b"counter", delta, WriteOptions::default())
		.await;
	(result, wire.requests().len())
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
