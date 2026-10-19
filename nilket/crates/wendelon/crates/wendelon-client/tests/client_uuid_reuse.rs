//! Client write identity (K4). Every `Client` incarnation must write under its
//! own `client_uuid`: the SDK starts the per-client sequence at 1 in every
//! instance, so two instances under one uuid issue the same canonical counter
//! write key `(client_uuid, sequence_no)` for different adds, and the server
//! refuses the second with `WRITE_KEY_CONFLICT`.
//!
//! The fixture captures the real `COUNTER_ADD_REQ` frame each client writes,
//! through an in-process transport that records the request and then closes
//! the stream. No server runs, so each add returns an error after its request
//! is on the wire. Only the captured request is asserted.

mod counter_transport;

use counter_transport::{CounterWire, inner_client, write_id};
use wendelon::schema::{ClientWriteId, TenantId};
use wendelon_client::{Client, WriteOptions};

/// Run one counter add on `client` and return the `ClientWriteId` it put on
/// the wire.
async fn counter_add_write_id(
	client: &Client,
	wire: &CounterWire,
	key: &[u8],
	delta: u64,
) -> ClientWriteId {
	let before = wire.requests().len();
	let _ = client
		.counter_fetch_add(TenantId::new(7), key, delta, WriteOptions::default())
		.await;
	let requests = wire.requests();
	assert_eq!(
		requests.len(),
		before + 1,
		"exactly one request reached the wire"
	);
	write_id(requests.last().unwrap())
}

fn is_v7(uuid: [u8; 16]) -> bool {
	uuid::Uuid::from_bytes(uuid).get_version_num() == 7
		&& uuid::Uuid::from_bytes(uuid).get_variant() == uuid::Variant::RFC4122
}

// Two incarnations of the default client (a restarted process, or a second
// live instance) write under different UUIDv7 identities, so the first add of
// each carries a different canonical write key.
#[tokio::test]
async fn restarted_client_does_not_reissue_a_prior_counter_write_key() {
	let wire = CounterWire::default();

	let before_restart = Client::from_inner(inner_client(&wire));
	let first = counter_add_write_id(&before_restart, &wire, b"counter-a", 5).await;
	drop(before_restart);

	let after_restart = Client::from_inner(inner_client(&wire));
	let second = counter_add_write_id(&after_restart, &wire, b"counter-b", 7).await;

	assert!(is_v7(first.client_uuid), "{:02x?}", first.client_uuid);
	assert!(is_v7(second.client_uuid), "{:02x?}", second.client_uuid);
	assert_eq!(after_restart.client_uuid(), second.client_uuid);
	assert_ne!(first.client_uuid, second.client_uuid);
	assert_eq!((first.sequence_no, second.sequence_no), (1, 1));
	assert_ne!(
		first.canonical_idempotency_bytes(),
		second.canonical_idempotency_bytes(),
		"a restarted client re-issued the canonical counter write key of a DIFFERENT prior add"
	);
}

// `with_client_uuid` puts K4 on the caller: two instances built from one fixed
// uuid reissue the same canonical write key for different adds. The server
// refuses the second add rather than replaying the first (see the data-server
// and leader write-key tests); this test pins why the caller must never share
// a uuid between instances.
#[tokio::test]
async fn instances_sharing_a_fixed_uuid_reissue_write_keys() {
	let wire = CounterWire::default();
	let fixed = *uuid::Uuid::now_v7().as_bytes();

	let one = Client::with_client_uuid(inner_client(&wire), fixed);
	let first = counter_add_write_id(&one, &wire, b"counter-a", 5).await;
	drop(one);
	let two = Client::with_client_uuid(inner_client(&wire), fixed);
	let second = counter_add_write_id(&two, &wire, b"counter-b", 7).await;

	assert_eq!(two.client_uuid(), fixed);
	assert_eq!(
		first.canonical_idempotency_bytes(),
		second.canonical_idempotency_bytes(),
		"a shared uuid reissues write keys; only a unique uuid per instance avoids it"
	);
}
