//! E6, the SDK's counter incarnation (liveness, never a safety premise). A
//! client rotates to a fresh UUIDv7, its sequence restarting at 1, before the
//! uuid is `INCARNATION_LIFETIME` old; every write id comes from one
//! incarnation; and no attempt of an add is sent once it is older than
//! `COUNTER_ATTEMPT_HORIZON` since its write id was allocated. Retries resend
//! the built request, so a rotation never splits a retry.
//!
//! The clock is a substitute: a monotonic instant and a wall time that the
//! test steps. The negative control's server is the in-process transport
//! answering as E1 does: `WRITE_OPERATION_EXPIRED` for a write id issued at or
//! before `now - E` on the same wall clock.

mod counter_transport;

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use counter_transport::{CounterWire, inner_client, response};
use wendelon::schema::{
	ClientWriteId, MutationCommitKnowledge, MutationStopReason, TenantId, WriteStatus,
	uuid_v7_unix_millis,
};
use wendelon::storage::counter::CounterRetentionConfig;
use wendelon_client::{
	COUNTER_ATTEMPT_HORIZON, Client, ClientClock, ClientError, INCARNATION_LIFETIME, WriteOptions,
};

const UNIX_BASE: u64 = 1_900_000_000_000;

/// A monotonic and wall clock the test steps together. `jump` adds a step the
/// moment `now` is read for the `n`-th time.
struct SteppedClock {
	base: Instant,
	offset_ms: AtomicU64,
	reads: AtomicUsize,
	jump: Mutex<Option<(usize, u64)>>,
}

impl SteppedClock {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			base: Instant::now(),
			offset_ms: AtomicU64::new(0),
			reads: AtomicUsize::new(0),
			jump: Mutex::new(None),
		})
	}

	fn advance(&self, ms: u64) {
		self.offset_ms.fetch_add(ms, Ordering::SeqCst);
	}

	fn unix_now(&self) -> u64 {
		UNIX_BASE + self.offset_ms.load(Ordering::SeqCst)
	}
}

impl ClientClock for SteppedClock {
	fn now(&self) -> Instant {
		let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
		let jump = *self.jump.lock().unwrap();
		if let Some((at, ms)) = jump {
			if read == at {
				self.advance(ms);
			}
		}
		self.base + Duration::from_millis(self.offset_ms.load(Ordering::SeqCst))
	}

	fn unix_millis(&self) -> u64 {
		self.unix_now()
	}
}

fn ok_wire() -> CounterWire {
	CounterWire::answering(Arc::new(|request| {
		Some(response(request, WriteStatus::OK, 0, request.delta))
	}))
}

async fn add(client: &Client, delta: u64) -> Result<(), ClientError> {
	client
		.counter_fetch_add(TenantId::new(7), b"counter", delta, WriteOptions::default())
		.await
		.map(|_| ())
}

fn issued(write_id: &ClientWriteId) -> u64 {
	uuid_v7_unix_millis(write_id.client_uuid).expect("a UUIDv7 write id")
}

// T-E6: the incarnation rotates exactly at the lifetime: a fresh UUIDv7 issued
// at the current wall time, the sequence restarting at 1.
#[tokio::test]
async fn incarnation_rotates_at_its_lifetime_with_a_fresh_v7_and_sequence() {
	let clock = SteppedClock::new();
	let wire = ok_wire();
	let client = Client::from_inner_with_clock(inner_client(&wire), clock.clone());
	add(&client, 1).await.unwrap();
	add(&client, 1).await.unwrap();
	clock.advance(INCARNATION_LIFETIME - 1);
	add(&client, 1).await.unwrap();
	clock.advance(1);
	add(&client, 1).await.unwrap();
	add(&client, 1).await.unwrap();
	let ids: Vec<_> = wire.requests().iter().map(|r| r.write_id).collect();
	let first = ids[0].client_uuid;
	let second = ids[3].client_uuid;
	assert_ne!(first, second, "rotated");
	assert_eq!(
		ids.iter()
			.map(|id| (id.client_uuid == first, id.sequence_no))
			.collect::<Vec<_>>(),
		vec![(true, 1), (true, 2), (true, 3), (false, 1), (false, 2)]
	);
	assert_eq!(issued(&ids[0]), UNIX_BASE);
	assert_eq!(issued(&ids[3]), UNIX_BASE + INCARNATION_LIFETIME);
	assert_eq!(client.client_uuid(), second);
}

// T-E6: concurrent allocation across a rotation never mixes one incarnation's
// uuid with another's sequence: every write id is distinct and each uuid's
// sequence numbers are exactly 1..=n.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_allocation_never_mixes_incarnations() {
	let clock = SteppedClock::new();
	let wire = ok_wire();
	let client = Arc::new(Client::from_inner_with_clock(
		inner_client(&wire),
		clock.clone(),
	));
	// The incarnation is one millisecond from its lifetime; the rotation step
	// ages the adds already in flight by that millisecond only, far inside H.
	clock.advance(INCARNATION_LIFETIME - 1);
	// One add is sure to precede the rotation.
	add(&client, 1).await.unwrap();
	let mut tasks = Vec::new();
	for n in 0..200 {
		if n == 100 {
			clock.advance(1);
		}
		let client = client.clone();
		tasks.push(tokio::spawn(async move { add(&client, 1).await }));
	}
	for task in tasks {
		task.await.unwrap().unwrap();
	}
	let ids: Vec<_> = wire.requests().iter().map(|r| r.write_id).collect();
	assert_eq!(ids.len(), 201);
	let distinct: HashSet<_> = ids
		.iter()
		.map(ClientWriteId::canonical_idempotency_bytes)
		.collect();
	assert_eq!(distinct.len(), 201, "every write id is distinct");
	let mut by_uuid: BTreeMap<[u8; 16], Vec<u64>> = BTreeMap::new();
	for id in &ids {
		by_uuid
			.entry(id.client_uuid)
			.or_default()
			.push(id.sequence_no);
	}
	assert_eq!(by_uuid.len(), 2, "one rotation");
	for sequences in by_uuid.values_mut() {
		sequences.sort_unstable();
		let expected: Vec<u64> = (1..=sequences.len() as u64).collect();
		assert_eq!(
			*sequences, expected,
			"one incarnation's contiguous sequence"
		);
	}
}

// T-E6: no attempt is sent once the add is older than the attempt horizon. A
// stop after an attempt was sent is INDETERMINATE; a stop before any attempt
// is KNOWN_NOT_COMMITTED. Every attempt that was sent carried one write id.
#[tokio::test]
async fn no_attempt_is_sent_past_the_attempt_horizon() {
	let clock = SteppedClock::new();
	let wire = CounterWire::answering(Arc::new({
		let clock = clock.clone();
		move |request| {
			// Each attempt takes just over half the horizon, then is backpressured.
			clock.advance(COUNTER_ATTEMPT_HORIZON / 2 + 1);
			Some(response(request, WriteStatus::BACKPRESSURED, 0, 0))
		}
	}));
	let client = Client::from_inner_with_clock(inner_client(&wire), clock.clone());
	let Err(ClientError::MutationStopped {
		operation_id,
		status,
		reason,
		knowledge,
	}) = add(&client, 1).await
	else {
		panic!("the add must stop at its horizon");
	};
	let requests = wire.requests();
	assert_eq!(
		requests.len(),
		2,
		"the third attempt would be past the horizon"
	);
	assert!(
		requests.iter().all(|r| r.write_id == operation_id),
		"one write id"
	);
	assert_eq!(
		(status, reason, knowledge),
		(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			MutationCommitKnowledge::INDETERMINATE
		)
	);

	// The clock jumps past the horizon between allocation and the first attempt.
	let clock = SteppedClock::new();
	let wire = ok_wire();
	let client = Client::from_inner_with_clock(inner_client(&wire), clock.clone());
	*clock.jump.lock().unwrap() = Some((
		clock.reads.load(Ordering::SeqCst) + 2,
		COUNTER_ATTEMPT_HORIZON + 1,
	));
	let Err(ClientError::MutationStopped {
		status,
		reason,
		knowledge,
		..
	}) = add(&client, 1).await
	else {
		panic!("the add must stop before its first attempt");
	};
	assert_eq!(
		(status, reason, knowledge),
		(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED
		)
	);
	assert!(wire.requests().is_empty(), "no attempt was sent");
}

/// A server that answers as E1 does on the test's wall clock.
fn expiring_wire(clock: Arc<SteppedClock>) -> CounterWire {
	let expiry = CounterRetentionConfig::default().expiry_ms;
	CounterWire::answering(Arc::new(move |request| {
		let issued = uuid_v7_unix_millis(request.write_id.client_uuid)?;
		if issued <= clock.unix_now().saturating_sub(expiry) {
			Some(
				response(request, WriteStatus::WRITE_OPERATION_EXPIRED, 0, 0)
					.with_mutation_outcome(
						MutationCommitKnowledge::INDETERMINATE,
						MutationStopReason::NONE,
					),
			)
		} else {
			Some(response(request, WriteStatus::OK, 0, request.delta))
		}
	}))
}

// T-E6: a long-lived client that rotates keeps its adds inside every node's
// horizon. The control never rotates (a caller-supplied uuid): once the uuid is
// older than E its fresh adds are refused with WRITE_OPERATION_EXPIRED.
#[tokio::test]
async fn a_rotating_client_stays_inside_the_horizon_and_a_fixed_uuid_expires() {
	let clock = SteppedClock::new();
	let client =
		Client::from_inner_with_clock(inner_client(&expiring_wire(clock.clone())), clock.clone());
	for _ in 0..12 {
		add(&client, 1).await.unwrap();
		clock.advance(INCARNATION_LIFETIME / 2);
	}

	let clock = SteppedClock::new();
	let fixed = {
		let mut uuid = *uuid::Uuid::now_v7().as_bytes();
		uuid[..6].copy_from_slice(&UNIX_BASE.to_be_bytes()[2..]);
		uuid
	};
	let client = Client::with_client_uuid_and_clock(
		inner_client(&expiring_wire(clock.clone())),
		fixed,
		clock.clone(),
	);
	add(&client, 1).await.unwrap();
	clock.advance(CounterRetentionConfig::default().expiry_ms);
	let Err(ClientError::MutationStopped {
		status, knowledge, ..
	}) = add(&client, 1).await
	else {
		panic!("control: a client that never rotates must expire");
	};
	assert_eq!(status, WriteStatus::WRITE_OPERATION_EXPIRED);
	assert_eq!(knowledge, MutationCommitKnowledge::INDETERMINATE);
	assert_eq!(
		client.client_uuid(),
		fixed,
		"with_client_uuid never rotates"
	);
}
