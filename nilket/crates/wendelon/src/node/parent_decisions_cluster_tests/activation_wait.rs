//! DC-281 (Codex 01a1fc24, 01a1fcc6): a public read routed to a current member whose
//! local base has not yet activated the committed generation. That member's read
//! authority refuses (`the committed generation is not active here`, a retryable
//! BACKPRESSURED), and the refusal hints the same member, so a client whose route
//! names it exhausts its retries while another member serves the same cut. The
//! control holds one member's activation (the fixture's `hold_activation`), shows the
//! other member serving and the held member refusing, then releases the hold while a
//! read is waiting there: the read must be served the exact rows once the member
//! activates.

use super::*;
use crate::client::{
	ClusterPeerWire, ClusterStatusWire, StaticTopologySource, WendelonClient, WendelonClientConfig,
};
use crate::network::tls::ActorIdentity;
use crate::network::transport::{
	NodeCapability, NodeCapabilitySet, NodeRole, NodeRoleSet, TransportProvider, TransportTierSet,
};
use std::sync::atomic::Ordering::Release;

const TIER: TransportTier = TransportTier::QuicIoUring;
/// The client's own node id on the network: not a member.
const CLIENT_NODE: u32 = 99;

/// A public client whose topology names only `member`: every read goes there. It
/// presents a non-member data node identity with no tenant claim, as Toron's
/// storage identity does.
fn client_through_only(cluster: &Cluster, member: u32, leader: u32) -> Arc<WendelonClient> {
	let status = ClusterStatusWire {
		epoch: cluster.node(member).ring.current_epoch(),
		leader: Some(leader),
		replication_factor: NODES.len() as u32,
		peers: vec![ClusterPeerWire {
			node_id: member,
			address: address(member),
			tier_bits: TransportTierSet::empty().with(TIER).as_u8(),
			role_bits: NodeRoleSet::empty()
				.with(NodeRole::Data)
				.with(NodeRole::Control)
				.as_u8(),
			capability_bits: NodeCapabilitySet::empty()
				.with(NodeCapability::ParentDecisions)
				.with(NodeCapability::ParentDecisionsBatchedLeases)
				.as_u8(),
		}],
	};
	let local = crate::schema::NodeId {
		id: CLIENT_NODE,
		_reserved: 0,
	};
	let provider: Arc<dyn TransportProvider> = Arc::new(
		SimulatedTransportProvider::new(
			local,
			cluster.network.clone(),
			crate::network::pool::BufPool::new(64, 1024 * 1024),
			TIER,
		)
		.with_caller(ActorIdentity::data_node(local)),
	);
	WendelonClient::new(WendelonClientConfig {
		local_tiers: TransportTierSet::empty().with(TIER),
		providers: [(TIER, provider)].into(),
		bootstrap: status.clone(),
		topology: Arc::new(StaticTopologySource::new(status)),
		pool_max_retained: 64,
		pool_max_cap_bytes: 1024 * 1024,
	})
}

/// The table's rows through `client`'s public query, sorted; or the error.
async fn read_rows(
	client: &WendelonClient,
	descriptor: &TableSchemaDescriptor,
) -> Result<Vec<(i64, String)>, String> {
	let mut result = crate::logical_backup::export::query_table(client, TenantId::ZERO, descriptor)
		.await
		.map_err(|error| format!("{error:?}"))?;
	let mut rows = Vec::new();
	while let Some(batch) = result
		.next_batch()
		.await
		.map_err(|error| format!("{error:?}"))?
	{
		let ids = batch
			.column_by_name("id")
			.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
			.unwrap();
		let values = batch
			.column_by_name("v")
			.and_then(|column| column.as_any().downcast_ref::<StringArray>())
			.unwrap();
		for row in 0..batch.num_rows() {
			rows.push((ids.value(row), values.value(row).to_owned()));
		}
	}
	rows.sort();
	Ok(rows)
}

/// `node`'s committed generation (its applied state) and its locally active one.
async fn generations_at(cluster: &Cluster, node: u32) -> (u64, u64) {
	let committed = cluster
		.node(node)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.map_or(0, |decisions| decisions.generations().current_gid())
		})
		.await;
	let active = cluster
		.node(node)
		.slab
		.persistent_index()
		.unwrap()
		.base_meta()
		.await
		.unwrap()
		.active_gid;
	(committed, active)
}

/// A three-voter cluster whose generation 1 is committed and active at every member
/// but `held`, whose activation is held: rows `expected` written before it.
struct Lagging {
	cluster: Cluster,
	leader: u32,
	descriptor: TableSchemaDescriptor,
	held: u32,
	other: u32,
	hold: Arc<std::sync::atomic::AtomicBool>,
	expected: Vec<(i64, String)>,
}

async fn lagging_member() -> Lagging {
	let (cluster, leader, descriptor) = ready().await;
	let rows = [(1_i64, "one"), (2, "two"), (3, "three")];
	for (id, value) in rows {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, value)]),
			)
			.await,
			"a row",
		);
	}
	let expected = rows
		.iter()
		.map(|(id, value)| (*id, (*value).to_owned()))
		.collect();
	let held = *NODES.iter().find(|id| **id != leader).unwrap();
	let other = *NODES
		.iter()
		.find(|id| **id != leader && **id != held)
		.unwrap();
	let hold = cluster
		.node(held)
		.data_server
		.decision_peer_for_test()
		.hold_activation
		.clone();
	cluster
		.node(held)
		.data_server
		.decision_peer_for_test()
		.observe_activation_waits
		.store(true, Release);
	hold.store(true, Release);
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(&cluster, now + 60_000).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let (committed, active) = generations_at(&cluster, held).await;
		let mut others_active = true;
		for node in NODES.into_iter().filter(|node| *node != held) {
			let (other_committed, other_active) = generations_at(&cluster, node).await;
			others_active &= other_committed >= 1 && other_active == other_committed;
		}
		if committed >= 1 && active < committed && others_active {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"premise never reached: node {held} committed {committed} active {active}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
	Lagging {
		cluster,
		leader,
		descriptor,
		held,
		other,
		hold,
		expected,
	}
}

type Attempts = Arc<std::sync::Mutex<Vec<crate::schema::TableQueryRequestId>>>;

/// A public table query of `descriptor` through `client` with `deadline_ms` (0: no
/// caller deadline), each attempt's request id recorded in `attempts`; the sorted
/// rows, or the client's error.
async fn query(
	client: &WendelonClient,
	descriptor: &TableSchemaDescriptor,
	deadline_ms: u64,
	attempts: Attempts,
) -> std::io::Result<Vec<(i64, String)>> {
	let mut meta: crate::schema::TableQueryRequestMeta = bytemuck::Zeroable::zeroed();
	meta.expected_epoch = client.epoch();
	meta.tenant_id = TenantId::ZERO;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.physical_descriptor_fingerprint = *descriptor.physical_descriptor_fingerprint.as_bytes();
	meta.consistency = crate::schema::Consistency::LINEARIZABLE;
	meta.row_identity_projection = crate::client::core::RowIdentityProjection::None.as_u8();
	meta.request_id = crate::schema::TableQueryRequestId::random();
	meta.deadline_remaining_millis = deadline_ms;
	let mut result = client
		.table_query_with_attempt_hook(
			crate::client::core::TableQueryRequest {
				meta,
				descriptor: descriptor.clone(),
				pbc: Vec::new(),
				runtime_filter: None,
				row_identity: crate::client::core::RowIdentityProjection::None,
			},
			move |attempt| {
				attempts.lock().unwrap().push(attempt.request_id);
				Ok(Box::new(()) as crate::client::core::TableQueryAttemptGuard)
			},
		)
		.await?;
	let mut rows = Vec::new();
	while let Some(batch) = result.next_batch().await? {
		let ids = batch
			.column_by_name("id")
			.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
			.unwrap();
		let values = batch
			.column_by_name("v")
			.and_then(|column| column.as_any().downcast_ref::<StringArray>())
			.unwrap();
		for row in 0..batch.num_rows() {
			rows.push((ids.value(row), values.value(row).to_owned()));
		}
	}
	rows.sort();
	Ok(rows)
}

fn attempts() -> Attempts {
	Arc::new(std::sync::Mutex::new(Vec::new()))
}

/// What table-query admissions at `node` observed while waiting for its activation.
fn events(
	cluster: &Cluster,
	node: u32,
) -> Vec<crate::network::data_server::reader::ActivationWaitEvent> {
	cluster
		.node(node)
		.data_server
		.decision_peer_for_test()
		.activation_waits
		.lock()
		.unwrap()
		.clone()
}

/// The first admission at `node`, among events after the first `after`, that
/// observed the activation lag: its request (attempt) id and cut.
async fn first_lag(
	cluster: &Cluster,
	node: u32,
	after: usize,
) -> (crate::schema::TableQueryRequestId, u64) {
	use crate::network::data_server::reader::ActivationWaitEvent;
	let seen = cluster
		.node(node)
		.data_server
		.decision_peer_for_test()
		.activation_wait_seen
		.clone();
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let notified = seen.notified();
		if let Some((request_id, k)) =
			events(cluster, node)
				.into_iter()
				.skip(after)
				.find_map(|event| match event {
					ActivationWaitEvent::Lag { request_id, k, .. } => Some((request_id, k)),
					ActivationWaitEvent::Admitted { .. } => None,
				}) {
			return (request_id, k);
		}
		assert!(
			tokio::time::timeout_at(deadline, notified).await.is_ok(),
			"no admission at node {node} observed the lag"
		);
	}
}

fn admitted_ids(
	events: &[crate::network::data_server::reader::ActivationWaitEvent],
) -> Vec<(crate::schema::TableQueryRequestId, u64)> {
	use crate::network::data_server::reader::ActivationWaitEvent;
	events
		.iter()
		.filter_map(|event| match event {
			ActivationWaitEvent::Admitted { request_id, k } => Some((*request_id, *k)),
			ActivationWaitEvent::Lag { .. } => None,
		})
		.collect()
}

/// DC-281: another member serves the committed generation; the member held for good
/// refuses every attempt, each of which observed its lag and none admitted; and a
/// read waiting at the held member is admitted, by the attempt that observed the lag
/// and at that attempt's own cut, once the member activates: exactly the rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_public_read_on_a_member_still_activating_the_committed_generation_is_served_on_activation()
 {
	use crate::network::data_server::reader::ActivationWaitEvent;
	let Lagging {
		cluster,
		leader,
		descriptor,
		held,
		other,
		hold,
		expected,
	} = lagging_member().await;
	let (committed, active) = generations_at(&cluster, held).await;
	// An independent linearizable query at a healthy member: the same rows.
	assert_eq!(
		read_rows(&client_through_only(&cluster, other, leader), &descriptor).await,
		Ok(expected.clone()),
		"node {other} serves the committed generation"
	);

	// Held for good: every attempt observed the lag, none was admitted, and the
	// client's retries ended.
	let client = client_through_only(&cluster, held, leader);
	let tried = attempts();
	let error = query(&client, &descriptor, 0, tried.clone())
		.await
		.expect_err("a member that never activates does not serve");
	eprintln!("DC-281 at node {held} (committed {committed}, active {active}): {error}");
	assert!(
		error.to_string().contains("retry budget exhausted"),
		"the retries of a refused admission end: {error}"
	);
	let tried = tried.lock().unwrap().clone();
	assert_eq!(tried.len(), crate::client::core::DATA_RETRY_BUDGET);
	let seen = events(&cluster, held);
	for id in &tried {
		assert!(
			seen.iter().any(|event| matches!(
				event,
				ActivationWaitEvent::Lag { request_id, committed: 1, active: 0, .. }
					if request_id == id
			)),
			"attempt {id:?} observed the lag: {seen:?}"
		);
	}
	assert!(admitted_ids(&seen).is_empty(), "nothing admitted: {seen:?}");

	// A read waiting at the held member, admitted once it activates.
	let before = seen.len();
	let tried = attempts();
	let waiting = tokio::spawn({
		let (descriptor, tried) = (descriptor.clone(), tried.clone());
		async move { query(&client, &descriptor, 0, tried).await }
	});
	let (request_id, k) = first_lag(&cluster, held, before).await;
	hold.store(false, Release);
	let served = waiting.await.unwrap();
	assert_eq!(
		served.map_err(|error| error.to_string()),
		Ok(expected),
		"the exact rows once node {held} activates"
	);
	let after: Vec<_> = events(&cluster, held).into_iter().skip(before).collect();
	assert_eq!(
		admitted_ids(&after),
		vec![(request_id, k)],
		"the attempt that observed the lag was admitted, at its own cut: {after:?}"
	);
	assert_eq!(
		tried.lock().unwrap().as_slice(),
		&[request_id],
		"one attempt"
	);
}

/// DC-281: a read waiting for this store's activation ends promptly when that very
/// attempt is cancelled through the public cancellation, while the hold stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_read_waiting_for_activation_is_cancelled_promptly() {
	let Lagging {
		cluster,
		leader,
		descriptor,
		held,
		hold,
		..
	} = lagging_member().await;
	let client = client_through_only(&cluster, held, leader);
	let waiting = tokio::spawn({
		let (client, descriptor) = (client.clone(), descriptor.clone());
		async move { query(&client, &descriptor, 0, attempts()).await }
	});
	let (request_id, _) = first_lag(&cluster, held, 0).await;
	let cancelled_at = tokio::time::Instant::now();
	let cancel = client
		.cancel_table_query_attempt(
			crate::schema::NodeId {
				id: held,
				_reserved: 0,
			},
			TenantId::ZERO,
			request_id,
		)
		.await;
	eprintln!("DC-281 cancel of {request_id:?}: {cancel:?}");
	let ended = tokio::time::timeout(std::time::Duration::from_secs(3), waiting)
		.await
		.expect("the cancelled read ends promptly")
		.unwrap();
	let error = ended.expect_err("a cancelled read is not served");
	eprintln!(
		"DC-281 cancelled after {:?}: {error}",
		cancelled_at.elapsed()
	);
	assert!(
		error.to_string().contains("query_cancelled"),
		"QUERY_CANCELLED: {error}"
	);
	assert!(
		hold.load(std::sync::atomic::Ordering::Acquire),
		"the hold stayed"
	);
	let (committed, active) = generations_at(&cluster, held).await;
	assert!(active < committed, "still lagging: {committed} {active}");
}

/// DC-281: the wait ends with the statement: a read with a 500 ms deadline at a
/// member held for good ends with QUERY_DEADLINE_EXCEEDED, not at the 5 s limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_read_waiting_for_activation_ends_at_its_own_deadline() {
	let Lagging {
		cluster,
		leader,
		descriptor,
		held,
		..
	} = lagging_member().await;
	let client = client_through_only(&cluster, held, leader);
	let started = tokio::time::Instant::now();
	let error = query(&client, &descriptor, 500, attempts())
		.await
		.expect_err("a member held for good does not serve");
	let elapsed = started.elapsed();
	eprintln!("DC-281 deadline at node {held} after {elapsed:?}: {error}");
	assert!(
		error.to_string().contains("query_deadline_exceeded"),
		"the statement's own deadline: {error}"
	);
	assert!(
		elapsed < std::time::Duration::from_secs(3),
		"promptly, not at the activation limit: {elapsed:?}"
	);
}

/// DC-281 (Codex 01a1fced R1, 01a1fd19, 01a1fd66): a recheck stalled (as a slow state or
/// base read) cannot outlive the statement or admit late. For each deadline case, on a
/// fresh cluster: the request's first check is seen observing the lag; only then are
/// checks held, and the held recheck is seen reached. The request expires (at its
/// deadline, or with none at the local limit, the retries ending). Then activation is
/// released and the member is seen active at the committed generation, so a surviving
/// request would now pass; only then is the recheck released. Nothing those attempts
/// started is admitted, while a fresh read, identified by its own attempt, is
/// admitted there with the exact rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_stalled_authority_recheck_cannot_outlive_the_statement_or_admit_late() {
	use std::sync::atomic::Ordering::Acquire;
	for deadline_ms in [1_500_u64, 0] {
		let Lagging {
			cluster,
			leader,
			descriptor,
			held,
			hold,
			expected,
			..
		} = lagging_member().await;
		let peer = cluster.node(held).data_server.decision_peer_for_test();
		let (stall, reached) = (
			peer.hold_read_checks.clone(),
			peer.held_read_checks_reached.clone(),
		);
		let client = client_through_only(&cluster, held, leader);
		let tried = attempts();
		let started = tokio::time::Instant::now();
		let reading = tokio::spawn({
			let (client, descriptor, tried) = (client.clone(), descriptor.clone(), tried.clone());
			async move { query(&client, &descriptor, deadline_ms, tried).await }
		});
		let (request_id, _) = first_lag(&cluster, held, 0).await;
		let reached_before = reached.load(Acquire);
		stall.store(true, Release);
		let by = tokio::time::Instant::now() + BOUND;
		while reached.load(Acquire) == reached_before {
			assert!(
				tokio::time::Instant::now() < by,
				"the held recheck of {request_id:?} was never reached"
			);
			tokio::time::sleep(std::time::Duration::from_millis(5)).await;
		}
		let error = reading
			.await
			.unwrap()
			.expect_err("a stalled recheck is not an admission");
		let elapsed = started.elapsed();
		eprintln!("DC-281 stalled recheck, deadline {deadline_ms} ms: {elapsed:?}: {error}");
		if deadline_ms > 0 {
			assert!(
				error.to_string().contains("query_deadline_exceeded"),
				"{error}"
			);
			assert!(
				elapsed < std::time::Duration::from_millis(deadline_ms + 1_500),
				"at the deadline: {elapsed:?}"
			);
		} else {
			assert!(
				error.to_string().contains("retry budget exhausted"),
				"{error}"
			);
			assert!(
				elapsed < std::time::Duration::from_secs(25),
				"each attempt ends at the local limit: {elapsed:?}"
			);
		}

		// The prerequisite of a late admission made true: the member activates.
		hold.store(false, Release);
		let by = tokio::time::Instant::now() + BOUND;
		loop {
			let (committed, active) = generations_at(&cluster, held).await;
			if active == committed {
				break;
			}
			assert!(
				tokio::time::Instant::now() < by,
				"node {held} never activated: committed {committed} active {active}"
			);
			tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		}
		// Only now is the recheck released: nothing the expired attempts started admits.
		stall.store(false, Release);
		tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
		let expired = tried.lock().unwrap().clone();
		assert!(expired.contains(&request_id));
		let admitted = admitted_ids(&events(&cluster, held));
		assert!(
			expired
				.iter()
				.all(|id| admitted.iter().all(|(seen, _)| seen != id)),
			"no late admission: {admitted:?}"
		);
		// A fresh read, by its own attempt, is admitted there with the exact rows.
		let fresh = attempts();
		assert_eq!(
			query(&client, &descriptor, 0, fresh.clone())
				.await
				.map_err(|error| error.to_string()),
			Ok(expected),
			"a fresh read at node {held}"
		);
		let fresh = fresh.lock().unwrap().clone();
		let admitted = admitted_ids(&events(&cluster, held));
		assert!(
			fresh
				.iter()
				.any(|id| admitted.iter().any(|(seen, _)| seen == id)),
			"the fresh read was admitted: {fresh:?} {admitted:?}"
		);
	}
}

/// DC-281 (Codex 01a1fced R2): a finite caller deadline is one budget across the
/// client's attempts. At a member held for good, 7 s (more than one 5 s local wait,
/// less than two) ends at about 7 s with QUERY_DEADLINE_EXCEEDED after a retry,
/// rather than giving the retry a fresh 7 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_finite_deadline_is_one_budget_across_activation_retries() {
	let Lagging {
		cluster,
		leader,
		descriptor,
		held,
		..
	} = lagging_member().await;
	let client = client_through_only(&cluster, held, leader);
	let tried = attempts();
	let started = tokio::time::Instant::now();
	let error = query(&client, &descriptor, 7_000, tried.clone())
		.await
		.expect_err("a member held for good does not serve");
	let elapsed = started.elapsed();
	let tried = tried.lock().unwrap().len();
	eprintln!("DC-281 one budget: {elapsed:?}, {tried} attempts: {error}");
	assert!(
		error.to_string().contains("query_deadline_exceeded"),
		"the caller's deadline ends it: {error}"
	);
	assert!(tried >= 2, "the budget crossed a retry: {tried}");
	assert!(
		elapsed >= std::time::Duration::from_millis(6_500)
			&& elapsed < std::time::Duration::from_secs(9),
		"one 7 s budget: {elapsed:?}"
	);
}

/// DC-281: only this store's own lag is a wait; a stale observation is refused.
#[test]
fn only_an_active_generation_behind_the_committed_one_is_a_lag() {
	use crate::network::data_server::reader::{AuthorityRefusal, generation_authority};
	assert_eq!(generation_authority(1, 1), Ok(()));
	assert_eq!(
		generation_authority(2, 1),
		Err(AuthorityRefusal::ActivationLag {
			committed: 2,
			active: 1
		})
	);
	assert!(matches!(
		generation_authority(1, 2),
		Err(AuthorityRefusal::Refused(reason)) if reason.contains("stale observation")
	));
}
