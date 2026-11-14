//! DC-237: a node's lease renewals after the control leader it was asking stops answering.
//!
//! The lease manager runs one renewal pass at a time (`run_lease_refresh_schedule` awaits a
//! pass before it can start another) and a follower's pass asks the leader it read at the start
//! of that pass (`request_remote_leases`). These tests ask one thing of the node's OWN lease
//! manager, as the node spawned it: when the control leader changes while the node's streams to
//! the old leader get no answer, does the node renew through the new leader within a stated
//! bound, by itself?
//!
//! What is held is the ROUTE: every stream the follower opens to the old leader, of any kind.
//! The tests observe that such a stream is held and that the follower's renewals have stopped
//! arriving; they do not observe which request the held stream carries.
//!
//! Takeover proposals are paused on every node here, so a range keeps its holder and the only
//! thing that can move a lease is its holder's own renewal. Admission of a grant is untouched.

use super::primary_down::agreed_leader_other_than;
use super::*;
use crate::simulator::network::{
	InjectedSimulatedTransportFault, SimulatedTransportEventKind, SimulatedTransportFaultAction,
	SimulatedTransportFaultOp,
};
use std::sync::atomic::Ordering::Release;

/// From the instant the running nodes agree on the new control leader: a renewal of the
/// follower's lease, granted after that instant, must be applied. Five lease lengths.
const RENEWAL_BOUND: std::time::Duration = std::time::Duration::from_secs(15);
/// How long an intercepted stream to the old leader stays unanswered. Finite, and longer than
/// every bound here, so that a worker waiting on it is released by itself after the test has
/// judged it and nothing waits on it for ever.
const UNANSWERED: std::time::Duration = std::time::Duration::from_secs(60);

fn node_id(id: u32) -> crate::schema::NodeId {
	crate::schema::NodeId { id, _reserved: 0 }
}

/// A range whose lineage `holder` holds, and the start stamp of its applied lease, as `asked`
/// reads its applied state.
async fn lease_start_of(cluster: &Cluster, asked: u32, range_start: u64, holder: u32) -> u64 {
	cluster
		.node(asked)
		.state_machine
		.read_state(move |state| {
			state
				.leases
				.get(&range_start)
				.filter(|lease| lease.holder.id == holder)
				.map(|lease| lease.lease_start.physical_ms)
		})
		.await
		.unwrap_or_else(|| panic!("node {holder} has no applied lease at {range_start}"))
}

async fn a_range_of(cluster: &Cluster, asked: u32, holder: u32) -> u64 {
	cluster
		.node(asked)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.lineages()
				.iter()
				.find(|(start, lineage)| {
					lineage.holder.id == holder
						&& state
							.leases
							.get(*start)
							.is_some_and(|lease| lease.holder.id == holder)
				})
				.map(|(start, _)| *start)
		})
		.await
		.unwrap_or_else(|| panic!("node {holder} holds no leased lineage"))
}

/// The follower's lease manager is renewing through the leader; the leader then stops and the
/// two that remain agree on another. With `unanswered`, every stream the follower opens to the
/// old leader from a chosen moment on gets no answer (the transport holds it), and the test
/// goes on only once a held stream is recorded and the follower's renewals are seen to have
/// stopped arriving. Returns how long after the agreement was OBSERVED (the election itself
/// was earlier) a lease of the follower over the watched range, ISSUED BY THE NEW LEADER with
/// a later start, was seen applied; `None` if none was seen before `RENEWAL_BOUND` had passed.
async fn renewal_after_the_leader_changes(unanswered: bool) -> Option<std::time::Duration> {
	let (mut cluster, leader, _descriptor) = ready().await;
	let follower = NODES
		.iter()
		.copied()
		.find(|node| *node != leader)
		.expect("a follower");
	let third = NODES
		.iter()
		.copied()
		.find(|node| *node != leader && *node != follower)
		.expect("a third node");
	for node in &cluster.nodes {
		node.data_server
			.decision_peer_for_test()
			.pause_takeover
			.store(true, Release);
	}
	let range_start = a_range_of(&cluster, leader, follower).await;
	// The follower renews by itself while the leader answers: its lease's start moves.
	let first = lease_start_of(&cluster, leader, range_start, follower).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	while lease_start_of(&cluster, leader, range_start, follower).await == first {
		assert!(
			tokio::time::Instant::now() < deadline,
			"node {follower} never renewed its lease at {range_start} while the leader answered"
		);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}

	if unanswered {
		cluster.network.clear_events();
		for _ in 0..4096 {
			cluster.network.inject(
				InjectedSimulatedTransportFault::on(
					SimulatedTransportFaultOp::OpenBi,
					SimulatedTransportFaultAction::Delay(UNANSWERED),
				)
				.from(node_id(follower))
				.to(node_id(leader)),
			);
		}
		// Synchronise on the intercepted request, by its effect and by the transport's own
		// record: the follower's lease start stops moving for three renewal intervals while a
		// stream of the follower to the leader is being held.
		let deadline = tokio::time::Instant::now() + BOUND;
		let mut seen = lease_start_of(&cluster, leader, range_start, follower).await;
		let mut since = tokio::time::Instant::now();
		loop {
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			let now = lease_start_of(&cluster, leader, range_start, follower).await;
			if now != seen {
				seen = now;
				since = tokio::time::Instant::now();
			}
			let held = cluster.network.events().iter().any(|event| {
				event.source == Some(node_id(follower))
					&& event.target == Some(node_id(leader))
					&& matches!(event.kind, SimulatedTransportEventKind::Fault { .. })
			});
			if held && since.elapsed() >= std::time::Duration::from_secs(3) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"node {follower}'s renewals never stopped arriving (held stream seen: {held})"
			);
		}
		eprintln!(
			"DC237 renewal: node {follower}'s renewals stopped arriving at the leader {leader} with a stream of its held; last applied lease start {seen}"
		);
	}
	let before = lease_start_of(&cluster, leader, range_start, follower).await;

	// The control leader stops; the two that remain agree on another.
	cluster.stop(leader, true).await;
	let new_leader = agreed_leader_other_than(&cluster, leader).await;
	let agreed = tokio::time::Instant::now();
	let mark = agreed + RENEWAL_BOUND;
	eprintln!(
		"DC237 renewal: old leader {leader} stopped; nodes {follower} and {third} agree on leader {new_leader}; unanswered = {unanswered}"
	);
	let renewed = loop {
		let lease = cluster
			.node(third)
			.state_machine
			.read_state(move |state| state.leases.get(&range_start).copied())
			.await;
		// The observation and when it was made: one made after the mark is late, whatever
		// it shows.
		let observed = tokio::time::Instant::now();
		if observed >= mark {
			break None;
		}
		if lease.is_some_and(|lease| {
			lease.holder.id == follower
				&& lease.range_start == range_start
				&& lease.issuer.id == new_leader
				&& lease.lease_start.physical_ms > before
		}) {
			break Some(observed - agreed);
		}
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	};
	eprintln!(
		"DC237 renewal: unanswered = {unanswered}: a renewal of node {follower}'s lease at {range_start} granted after the leader change was {}",
		match renewed {
			Some(after) => format!("applied {after:?} after the agreement"),
			None => format!("NOT applied within {RENEWAL_BOUND:?} of the agreement"),
		}
	);
	renewed
}

/// The control: the old leader's streams fail at once (it is gone), nothing is held. The
/// follower's own lease manager renews through the new leader inside the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_follower_renews_through_the_new_leader_when_the_old_one_refuses_at_once() {
	let renewed = renewal_after_the_leader_changes(false).await;
	assert!(
		renewed.is_some(),
		"no renewal through the new leader within {RENEWAL_BOUND:?}"
	);
}

/// The case: the follower's streams to the old leader get no answer, its renewals have
/// stopped arriving, and the leader changes. The follower's own lease manager must still
/// renew through the new leader inside the same bound, with nothing restarted.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_follower_renews_through_the_new_leader_though_its_request_to_the_old_one_is_unanswered()
{
	let renewed = renewal_after_the_leader_changes(true).await;
	assert!(
		renewed.is_some(),
		"no renewal through the new leader within {RENEWAL_BOUND:?} of the leader change, with the route to the old leader unanswered"
	);
}

/// A clock that reads the system's and remembers every reading it gave.
struct Recording(std::sync::Mutex<Vec<u64>>);

impl crate::consensus::monitor::PhysicalClock for Recording {
	fn now_ms(&self) -> u64 {
		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		self.0.lock().unwrap().push(now);
		now
	}
}

/// The leader's own grants, when its commit cannot complete: the pass ends at its budget
/// instead of waiting; the dropped proposal is neither a grant nor a denial; nothing is cached
/// from it; the batch that was attempted is where the next pass begins again (it was the
/// only one); and once a quorum is back a later pass, from the applied state, is granted and
/// cached. The leader's own lease manager is stopped, so the test alone runs its passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_leaders_pass_ends_at_its_budget_when_its_commit_cannot_complete() {
	const TTL_MS: u32 = 3_000;
	let (mut cluster, leader, _descriptor) = ready().await;
	for node in &cluster.nodes {
		node.data_server
			.decision_peer_for_test()
			.pause_takeover
			.store(true, Release);
	}
	let followers: Vec<u32> = NODES.iter().copied().filter(|id| *id != leader).collect();
	cluster.node(leader).handles.lease_manager.abort();
	let held: Vec<u64> = {
		let node = cluster.node(leader);
		let mut held: Vec<u64> = node
			.state_machine
			.read_state(move |state| {
				state
					.decisions
					.as_ref()
					.unwrap()
					.lineages()
					.iter()
					.filter(|(_, lineage)| lineage.holder.id == leader)
					.map(|(start, _)| *start)
					.collect()
			})
			.await;
		held.sort_unstable();
		held
	};
	assert!(!held.is_empty());
	assert!(
		held.len() <= crate::schema::MAX_LEASES_PER_BATCH,
		"this control is about one batch"
	);
	let cached_before = cluster.node(leader).data_server.cached_leases_for_test();

	// No quorum: both followers stop. The leader still names itself.
	for follower in &followers {
		cluster.stop(*follower, false).await;
	}
	let node = cluster.node(leader);
	assert_eq!(node.raft.current_leader().await, Some(leader));
	let clock = Recording(Default::default());
	let began = tokio::time::Instant::now();
	let granted = tokio::time::timeout(
		BOUND,
		node.data_server
			.refresh_leases_once(&node.ring, &clock, TTL_MS),
	)
	.await
	.expect("the pass did not end: it is waiting without a budget");
	let took = began.elapsed();
	let stamps = clock.0.lock().unwrap().clone();
	eprintln!(
		"DC237 leader pass: with no quorum the pass ended after {took:?} with {granted} granted; request stamps {stamps:?}"
	);
	assert_eq!(granted, 0);
	assert!(
		took >= std::time::Duration::from_millis(u64::from(TTL_MS))
			&& took < std::time::Duration::from_millis(u64::from(TTL_MS) + 1_500),
		"the pass ended after {took:?}, not at its {TTL_MS} ms budget"
	);
	// The one batch was attempted and not answered: the walk begins with it again.
	assert_eq!(
		node.data_server.lease_renewal_cursor_for_test(),
		Some(held[0])
	);
	// Nothing new was cached, and nothing that was cached was removed.
	let cached = node.data_server.cached_leases_for_test();
	assert!(
		cached
			.iter()
			.all(|lease| !stamps.contains(&lease.lease_start.physical_ms)),
		"a lease of the unanswered batch was cached"
	);
	assert_eq!(
		cached
			.iter()
			.map(|lease| (lease.range_start, lease.lease_start))
			.collect::<Vec<_>>(),
		cached_before
			.iter()
			.map(|lease| (lease.range_start, lease.lease_start))
			.collect::<Vec<_>>()
	);

	// A spent budget before the batch is attempted: that batch is the continuation, not
	// skipped, and again nothing is cached.
	let node = cluster.node(leader);
	let targets = node.data_server.decision_lease_targets_for_test().await;
	let spent = crate::network::data_server::lease_pass::RenewalPass::begin(0);
	node.data_server
		.set_lease_renewal_cursor_for_test(Some(held[held.len() - 1]));
	let granted = node
		.data_server
		.renew_leases_batched_for_test(
			&targets,
			node.ring.snapshot().epoch(),
			&clock,
			TTL_MS,
			Some(leader),
			&spent,
		)
		.await;
	assert_eq!(granted, 0);
	assert_eq!(
		node.data_server.lease_renewal_cursor_for_test(),
		Some(held[held.len() - 1]),
		"the unattempted batch begins at the cursor's own range"
	);

	// The quorum returns. Whatever became of the dropped proposals, a later pass from the
	// applied state is granted for every held range and cached, and leaves no continuation.
	for follower in &followers {
		cluster.reopen(*follower).await;
	}
	let deadline = tokio::time::Instant::now() + BOUND;
	let granted = loop {
		let node = cluster.node(leader);
		let still_holds = node
			.data_server
			.decision_lease_targets_for_test()
			.await
			.len();
		let granted = node
			.data_server
			.refresh_leases_once(&node.ring, &clock, TTL_MS)
			.await;
		if still_holds > 0 && granted == still_holds {
			assert_eq!(node.data_server.lease_renewal_cursor_for_test(), None);
			break granted;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"no pass was granted whole after the quorum returned (last: {granted} of {still_holds})"
		);
		tokio::time::sleep(std::time::Duration::from_millis(200)).await;
	};
	let node = cluster.node(leader);
	assert_eq!(node.data_server.cached_leases_for_test().len(), granted);
}

/// A grant that APPLIED and whose answer never came back inside the budget. The leader's own
/// batch is committed and applied (seen in the applied state, with the request's own stamp,
/// before anything else can renew it), and its answer is then withheld past the pass's
/// deadline. The pass ends unanswered; nothing is cached from that answer and what was cached
/// before stays; the next pass, from the applied state as it now is, is granted and cached.
/// The leader's own lease manager is stopped, so only the test runs passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_grant_that_applied_with_its_answer_withheld_is_not_cached_and_the_next_pass_renews() {
	const TTL_MS: u32 = 3_000;
	let (cluster, leader, _descriptor) = ready().await;
	for node in &cluster.nodes {
		node.data_server
			.decision_peer_for_test()
			.pause_takeover
			.store(true, Release);
	}
	let node = cluster.node(leader);
	node.handles.lease_manager.abort();
	// Let a pass of the stopped manager that was in flight finish before the cache is read.
	tokio::time::sleep(std::time::Duration::from_millis(500)).await;
	let held = node.data_server.decision_lease_targets_for_test().await;
	assert!(!held.is_empty() && held.len() <= crate::schema::MAX_LEASES_PER_BATCH);
	let key = |leases: &[crate::schema::LeaseDescriptor]| {
		let mut key: Vec<(u64, u64)> = leases
			.iter()
			.map(|lease| (lease.range_start, lease.lease_start.physical_ms))
			.collect();
		key.sort_unstable();
		key
	};
	let cached_before = key(&node.data_server.cached_leases_for_test());
	assert_eq!(cached_before.len(), held.len());

	let (applied, release) = (
		std::sync::Arc::new(tokio::sync::Notify::new()),
		std::sync::Arc::new(tokio::sync::Notify::new()),
	);
	*node
		.data_server
		.decision_peer_for_test()
		.hold_lease_answer
		.lock()
		.unwrap() = Some((applied.clone(), release.clone()));
	let clock = Recording(Default::default());
	let pass = async {
		tokio::time::timeout(
			BOUND,
			node.data_server
				.refresh_leases_once(&node.ring, &clock, TTL_MS),
		)
		.await
		.expect("the pass did not end")
	};
	let observe = async {
		// The batch has been committed and applied; its answer is being withheld.
		tokio::time::timeout(BOUND, applied.notified())
			.await
			.expect("the batch was never applied");
		let stamp = *clock.0.lock().unwrap().first().expect("a request stamp");
		let starts: Vec<u64> = node
			.state_machine
			.read_state(move |state| {
				state
					.leases
					.values()
					.filter(|lease| lease.holder.id == leader)
					.map(|lease| lease.lease_start.physical_ms)
					.collect()
			})
			.await;
		(stamp, starts)
	};
	let began = tokio::time::Instant::now();
	let (granted, (stamp, applied_starts)) = tokio::join!(pass, observe);
	let took = began.elapsed();
	eprintln!(
		"DC237 applied grant: the batch stamped {stamp} was applied for {} of {} held ranges while its answer was withheld; the pass ended after {took:?} with {granted} granted",
		applied_starts
			.iter()
			.filter(|start| **start == stamp)
			.count(),
		held.len()
	);
	// The grant applied, for every held range, before its answer was withheld.
	assert_eq!(applied_starts.len(), held.len());
	assert!(
		applied_starts.iter().all(|start| *start == stamp),
		"{applied_starts:?}"
	);
	// The pass ended at its budget, unanswered: not granted, not denied.
	assert_eq!(granted, 0);
	assert!(
		took >= std::time::Duration::from_millis(u64::from(TTL_MS))
			&& took < std::time::Duration::from_millis(u64::from(TTL_MS) + 1_500),
		"{took:?}"
	);
	// Nothing was cached from the withheld answer; what was cached before is still there.
	assert_eq!(
		key(&node.data_server.cached_leases_for_test()),
		cached_before
	);
	assert!(cached_before.iter().all(|(_, start)| *start != stamp));

	// The next pass asks from the applied state as it is now and is granted and cached.
	*node
		.data_server
		.decision_peer_for_test()
		.hold_lease_answer
		.lock()
		.unwrap() = None;
	let granted = node
		.data_server
		.refresh_leases_once(&node.ring, &clock, TTL_MS)
		.await;
	assert_eq!(granted, held.len());
	assert_eq!(node.data_server.lease_renewal_cursor_for_test(), None);
	let cached_after = key(&node.data_server.cached_leases_for_test());
	assert_eq!(cached_after.len(), held.len());
	assert!(
		cached_after.iter().all(|(_, start)| *start > stamp),
		"the later pass's own leases: {cached_after:?}"
	);
	drop(release);
}
