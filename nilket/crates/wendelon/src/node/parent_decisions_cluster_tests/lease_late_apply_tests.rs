//! DC-287 (Codex 01a20a6b): a lease batch whose APPLY comes after its own expiry, on a one-voter
//! ParentDecisions store (the S8 shape). Only the test renews (takeover paused, the lease manager
//! stopped), with the wall clock the server's lease source reads.
//!
//! - A, the timely comparator: a pass's batch applies at once; a write commits.
//! - B, the late apply: probes are staged ahead of it (W1 for plan admission, site 1; W2 through
//!   site 1 under the live lease, then held at its stripe across the expiry, for site 2; W4 an
//!   uncompleted reservation). A real `AcquireLeases` is handed to Raft and committed, and its
//!   apply is held BEFORE the state lock (a test-only seam); the pass ends at its budget, its
//!   response waiter dropped as usual. After the prior grant's expiry both probes refuse
//!   `LeaseMissing`; the held entry is released after its OWN expiry, applies, and is dead on
//!   arrival: a resumed W1 attempt still refuses.
//! - C, recovery: the next pass is granted; its admission effect is read; a fresh write commits;
//!   the old reservation W4 is answered and never acknowledged.
//!
//! The claim is a late APPLY: the entry was committed when it reached apply (Raft hands only
//! committed entries to the state machine). Nothing here models the WAL, or claims a late commit.

use super::one_voter::{one_voter, open, until_settled};
use super::write_deadline::until_reached;
use super::*;
use crate::network::data_server::child_writes::{ChildRefusal, PauseStage, registry_probe};
use crate::network::data_server::lease_evidence::{
	IssueOutcome, LeaseEvidence, LeaseSite, RowClass, VerifyState,
};
use crate::network::data_server::write_stall_trace::{
	StallSite, WriteStallRecord, WriteStallTrace,
};

const TTL_MS: u32 = 3_000;

fn wall_ms() -> u64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64
}

/// The wall clock (what the server's lease source reads), recording each request stamp.
#[derive(Default)]
struct Stamps(std::sync::Mutex<Vec<u64>>);

impl crate::consensus::monitor::PhysicalClock for Stamps {
	fn now_ms(&self) -> u64 {
		let now = wall_ms();
		self.0.lock().unwrap().push(now);
		now
	}
}

impl Stamps {
	fn last(&self) -> u64 {
		*self.0.lock().unwrap().last().expect("a request stamp")
	}
}

/// The stored lease of `range` and its lineage generation, from the applied state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Stored {
	start: u64,
	expiry: u64,
	epoch: u64,
	holder: u32,
	generation: Option<u64>,
}

async fn stored(node: &WendelonNode, range: u64) -> Option<Stored> {
	node.state_machine
		.read_state(move |state| {
			let lease = state.leases.get(&range)?;
			Some(Stored {
				start: lease.lease_start.physical_ms,
				expiry: lease.lease_expiry.physical_ms,
				epoch: lease.epoch,
				holder: lease.holder.id,
				generation: state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.lineages().get(&range))
					.map(|lineage| lineage.generation),
			})
		})
		.await
}

/// The cached leases as (range start, lease start).
fn cached(node: &WendelonNode) -> Vec<(u64, u64)> {
	let mut cached: Vec<(u64, u64)> = node
		.data_server
		.cached_leases_for_test()
		.iter()
		.map(|lease| (lease.range_start, lease.lease_start.physical_ms))
		.collect();
	cached.sort_unstable();
	cached
}

async fn record(
	node: &WendelonNode,
	key: ParentKey,
) -> Option<crate::consensus::decisions::ParentRecord> {
	node.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()?
				.bodies()
				.values()
				.find(|body| body.claim.key == key)
				.cloned()
		})
		.await
}

fn attempt(descriptor: &TableSchemaDescriptor, attempts: u8) -> TableWriteRequestMeta {
	let mut meta = write_meta(descriptor, 1, uuid_v7());
	meta.set_attempts_remaining(attempts);
	meta
}

type Answer = crate::schema::TableWriteResponseMeta;

fn spawn_write(
	node: &WendelonNode,
	meta: TableWriteRequestMeta,
	batch: RecordBatch,
) -> tokio::task::JoinHandle<Answer> {
	let server = node.data_server.clone();
	tokio::spawn(async move { server.table_write_for_test(meta, batch).await.unwrap().0 })
}

/// A write held after its Reserve answer, before its children: (meta, task, release).
async fn stage(
	node: &WendelonNode,
	descriptor: &TableSchemaDescriptor,
	id: i64,
) -> (
	TableWriteRequestMeta,
	tokio::task::JoinHandle<Answer>,
	Arc<tokio::sync::Notify>,
) {
	let (reached, release) = node
		.data_server
		.pause_coordinator_for_test(PauseStage::AfterReserve);
	let meta = attempt(descriptor, 3);
	let task = spawn_write(node, meta, rows(descriptor, &[id]));
	until_reached("a staged write after its Reserve", &reached).await;
	(meta, task, release)
}

async fn answered(what: &str, task: tokio::task::JoinHandle<Answer>) -> Answer {
	tokio::time::timeout(BOUND, task)
		.await
		.unwrap_or_else(|_| panic!("{what}: no answer"))
		.unwrap()
}

fn records_of(trace: &WriteStallTrace, meta: &TableWriteRequestMeta) -> Vec<WriteStallRecord> {
	trace
		.snapshot()
		.records
		.into_iter()
		.filter(|record| {
			record.client_uuid == meta.parent_write_id.client_uuid
				&& record.sequence_no == meta.parent_write_id.sequence_no
		})
		.collect()
}

/// Wendelon ADR 0127: the lease evidence published with each record of `meta`, in order.
fn evidence_of(
	trace: &WriteStallTrace,
	meta: &TableWriteRequestMeta,
) -> Vec<Option<LeaseEvidence>> {
	let snapshot = trace.snapshot();
	snapshot
		.records
		.iter()
		.zip(snapshot.leases)
		.filter(|(record, _)| {
			record.client_uuid == meta.parent_write_id.client_uuid
				&& record.sequence_no == meta.parent_write_id.sequence_no
		})
		.map(|(_, lease)| lease)
		.collect()
}

/// (site, decision, holder refusal code, acked, acks, attempts) of each record.
fn shape(records: &[WriteStallRecord]) -> Vec<String> {
	records
		.iter()
		.map(|record| {
			format!(
				"{:?}/{:?}/holder {:?}/acks {}/attempts {}",
				record.site,
				record.answer.decision,
				record.holder.map(|holder| (holder.refusal, holder.acked)),
				record.acks,
				record.attempts_remaining
			)
		})
		.collect()
}

async fn no_intent(node: &WendelonNode, rsv: u64) -> bool {
	node.slab
		.persistent_index()
		.unwrap()
		.parent_intent(rsv, 0)
		.await
		.unwrap()
		.is_none()
}

async fn sleep_until_wall(at_ms: u64) {
	let now = wall_ms();
	if at_ms > now {
		tokio::time::sleep(std::time::Duration::from_millis(at_ms - now)).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_lease_batch_applied_after_its_own_expiry_leaves_the_holder_refusing_until_the_next_grant()
 {
	let (_dir, mut cfg) = one_voter();
	cfg.write_stall_trace = true;
	let node = open(&cfg).await;
	until_settled(&node, None).await;
	let descriptor = descriptor();
	node.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_settled(&node, Some(descriptor.table_object_id)).await;
	let trace = node
		.data_server
		.write_stall_trace()
		.expect("the configuration enables the trace")
		.clone();
	let lease_missing = ChildRefusal::LeaseMissing.code();
	// Only the test renews.
	node.data_server
		.decision_peer_for_test()
		.pause_takeover
		.store(true, std::sync::atomic::Ordering::Release);
	node.handles.lease_manager.abort();
	tokio::time::sleep(std::time::Duration::from_millis(500)).await;
	let targets = node.data_server.decision_lease_targets_for_test().await;
	let [(range, _, generation_named)] = targets[..] else {
		panic!("one voter, one target: {targets:?}");
	};
	let clock = Stamps::default();

	// ---- A: the timely comparator ----
	assert_eq!(
		node.data_server
			.refresh_leases_once(&node.ring, &clock, TTL_MS)
			.await,
		1
	);
	let a_stamp = clock.last();
	let a = stored(&node, range).await.expect("A applied");
	assert_eq!((a.start, a.expiry), (a_stamp, a_stamp + u64::from(TTL_MS)));
	assert_eq!(a.generation, generation_named);
	assert_eq!(node.data_server.held_leases_for_test().await.len(), 1);
	assert!(cached(&node).contains(&(range, a_stamp)));
	let comparator = attempt(&descriptor, 3);
	let committed = node
		.data_server
		.table_write_for_test(comparator, rows(&descriptor, &[1]))
		.await
		.unwrap()
		.0;
	assert_eq!(committed.write.status, WriteStatus::OK, "{committed:?}");
	eprintln!("DC287 lease A: stored {a:?}; the comparator write committed");

	// ---- B: the late apply, with the probes staged ahead of it ----
	let (w1, t1, go1) = stage(&node, &descriptor, 11).await;
	let (w2, t2, go2) = stage(&node, &descriptor, 12).await;
	let (w4, t4, go4) = stage(&node, &descriptor, 14).await;
	let r1 = record(&node, key_of(&descriptor, &w1)).await.unwrap().rsv;
	let r2 = record(&node, key_of(&descriptor, &w2)).await.unwrap().rsv;
	let w4_record = record(&node, key_of(&descriptor, &w4)).await.unwrap();
	let r4 = w4_record.rsv;
	// W2 through its plan admission (site 1) under A's live lease, then held at its stripe.
	let stripe = node
		.data_server
		.decision_peer_for_test()
		.locks
		.lock_for_test(r2)
		.await;
	let persists = registry_probe::arm(node.slab.data_dir());
	go2.notify_one();
	let deadline = tokio::time::Instant::now() + BOUND;
	while persists.evidence().before_driver == 0 {
		assert!(
			tokio::time::Instant::now() < deadline,
			"W2 never reached its stripe"
		);
		tokio::time::sleep(std::time::Duration::from_millis(2)).await;
	}
	let w2_past_site_1 = wall_ms();
	assert!(
		w2_past_site_1 < a.expiry,
		"W2 passed site 1 at {w2_past_site_1}, before A's expiry {}",
		a.expiry
	);

	// A real AcquireLeases, handed to Raft and committed; its apply held before the state lock.
	let hold = node.state_machine.hold_next_lease_batch_apply_for_test();
	let cached_before = cached(&node);
	let began = tokio::time::Instant::now();
	let (granted, (held_id, applied_at_hold)) = tokio::join!(
		node.data_server
			.refresh_leases_once(&node.ring, &clock, TTL_MS),
		async {
			tokio::time::timeout(BOUND, hold.entered.notified())
				.await
				.expect("the AcquireLeases batch never reached apply");
			let held = hold.log_id.lock().unwrap().expect("the held entry's id");
			let applied = node.raft.metrics().borrow().last_applied;
			(held, applied)
		}
	);
	let took = began.elapsed();
	let b_stamp = clock.last();
	let b_expiry = b_stamp + u64::from(TTL_MS);
	eprintln!(
		"DC287 lease B: entry {held_id:?} held at apply (last applied then {applied_at_hold:?}); request stamp {b_stamp}, its expiry {b_expiry}; the pass ended after {took:?} with {granted} granted"
	);
	assert!(b_stamp < a.expiry, "requested while A was live");
	assert!(applied_at_hold.is_none_or(|applied| applied.index < held_id.index));
	assert_eq!(granted, 0, "unanswered inside its budget");
	assert!(
		took >= std::time::Duration::from_millis(u64::from(TTL_MS)),
		"{took:?}"
	);
	// Nothing from the unanswered grant is cached; what was cached stays.
	assert!(!cached(&node).contains(&(range, b_stamp)));
	assert_eq!(cached(&node), cached_before);
	assert_eq!(stored(&node, range).await, Some(a), "not yet applied");

	// Past A's expiry: no live lease; both probes refuse at the holder.
	sleep_until_wall(a.expiry + 50).await;
	assert!(node.data_server.held_leases_for_test().await.is_empty());
	drop(stripe);
	go1.notify_one();

	// The held entry released after its OWN expiry: it applies, dead on arrival.
	sleep_until_wall(b_expiry + 50).await;
	hold.release.notify_one();
	let deadline = tokio::time::Instant::now() + BOUND;
	let b = loop {
		let now = stored(&node, range).await.expect("a stored lease");
		if now.start == b_stamp {
			break now;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the held batch never applied"
		);
		tokio::time::sleep(std::time::Duration::from_millis(5)).await;
	};
	let applied_at = wall_ms();
	eprintln!(
		"DC287 lease B applied at {applied_at} (> its expiry {b_expiry}): stored {b:?}; live {:?}",
		node.data_server.held_leases_for_test().await
	);
	assert_eq!(
		(b.start, b.expiry, b.epoch, b.holder),
		(b_stamp, b_expiry, a.epoch, a.holder)
	);
	assert!(
		node.data_server.held_leases_for_test().await.is_empty(),
		"dead on arrival"
	);
	assert!(!cached(&node).contains(&(range, b_stamp)));

	let a1 = answered("W1", t1).await;
	let a2 = answered("W2", t2).await;
	let (s1, s2) = (records_of(&trace, &w1), records_of(&trace, &w2));
	eprintln!(
		"DC287 lease probes: W1 (site 1) {:?}/{:?} {:?}; W2 (site 2, stripe-crossing) {:?}/{:?} {:?}",
		a1.write.status,
		a1.write.mutation_commit_knowledge(),
		shape(&s1),
		a2.write.status,
		a2.write.mutation_commit_knowledge(),
		shape(&s2)
	);
	for (what, answer, records, rsv) in [("W1", &a1, &s1, r1), ("W2", &a2, &s2, r2)] {
		assert_ne!(answer.write.status, WriteStatus::OK, "{what}: {answer:?}");
		let [record] = &records[..] else {
			panic!("{what}: one stalled attempt: {records:?}");
		};
		assert_eq!(record.site, StallSite::Finalize, "{what}");
		let holder = record.holder.expect("a Finalize keeps its holder answer");
		assert_eq!(
			(holder.refusal, holder.acked),
			(Some(lease_missing), false),
			"{what}"
		);
		assert_eq!(record.acks, 0, "{what}");
		assert!(no_intent(&node, rsv).await, "{what}: no intent");
	}
	// ADR 0127: what each refusing check's own reads saw, and the witness at its cutoff.
	let witness = trace.leases.slots_for_test();
	let ticket_of = |stamp: u64| {
		witness
			.iter()
			.find(|slot| slot.request.physical_ms == stamp)
			.map(|slot| slot.ticket)
	};
	let (a_ticket, b_ticket) = (
		ticket_of(a_stamp).expect("A was witnessed"),
		ticket_of(b_stamp).expect("B was witnessed"),
	);
	// The common facts of a probe's evidence: the site, the hint, the class, both epochs, the one
	// covering row expired by the read's own filter (so never verified), nothing truncated.
	let common = |what: &str, evidence: &LeaseEvidence, site: LeaseSite| {
		assert_eq!(evidence.site, site, "{what}");
		assert!(evidence.hint_matches, "{what}");
		assert_eq!(evidence.class, RowClass::Expired, "{what}");
		assert_eq!(
			(evidence.e_view, evidence.e_dec),
			(Some(a.epoch), Some(a.epoch)),
			"{what}"
		);
		let row = evidence.rows[0].expect("the consulted row");
		assert_eq!(
			(row.filter_live, row.verify),
			(false, VerifyState::NotEvaluated),
			"{what}"
		);
		assert!(row.covered && row.epoch_dec, "{what}");
		assert!(evidence.rows[1].is_none() && !evidence.truncated, "{what}");
		(row, evidence.provenance.expect("a covering row"))
	};
	// W2 refused under its stripe BEFORE the held apply was released: it consulted A's row, which
	// agrees with A's issue (Granted); the later issue B (unanswered at its budget) is kept too.
	let [Some(w2_evidence)] = evidence_of(&trace, &w2)[..] else {
		panic!("W2: one evidence: {:?}", evidence_of(&trace, &w2));
	};
	eprintln!("DC287 lease evidence W2: {w2_evidence:?}");
	let (row, provenance) = common("W2", &w2_evidence, LeaseSite::Stripe);
	assert!(
		w2_evidence
			.applied_index
			.is_some_and(|applied| applied < held_id.index),
		"W2 read before the held entry applied"
	);
	assert_eq!(
		(row.start.physical_ms, row.expiry.physical_ms),
		(a.start, a.expiry)
	);
	assert_eq!(
		provenance
			.agree
			.map(|agree| (agree.ticket, agree.retained_matches, agree.outcome)),
		Some((a_ticket, 1, IssueOutcome::Granted)),
		"W2: the row agrees with A"
	);
	assert_eq!(
		provenance
			.later
			.map(|later| (later.ticket, later.request.physical_ms, later.outcome)),
		Some((b_ticket, b_stamp, IssueOutcome::Unanswered)),
		"W2: and the later issue B is kept"
	);
	// W1, released while the apply was held, re-reads its record after its Reserve through the
	// log, so it reached its holder only AFTER the held apply: it consulted B's row, applied and
	// already expired, which agrees with B (unanswered at its budget); no issue followed B.
	let [Some(w1_evidence)] = evidence_of(&trace, &w1)[..] else {
		panic!("W1: one evidence: {:?}", evidence_of(&trace, &w1));
	};
	eprintln!("DC287 lease evidence W1: {w1_evidence:?}");
	let (row, provenance) = common("W1", &w1_evidence, LeaseSite::PlanAdmission);
	assert!(
		w1_evidence
			.applied_index
			.is_some_and(|applied| applied >= held_id.index),
		"W1 read after the held entry applied"
	);
	assert_eq!(
		(row.start.physical_ms, row.expiry.physical_ms),
		(b_stamp, b_expiry)
	);
	assert_eq!(
		provenance
			.agree
			.map(|agree| (agree.ticket, agree.retained_matches, agree.outcome)),
		Some((b_ticket, 1, IssueOutcome::Unanswered)),
		"W1: the row agrees with B"
	);
	assert_eq!(provenance.later, None);
	// A resumed W1 attempt after the late apply: still no live lease, still refused.
	let mut resumed = w1;
	resumed.set_attempts_remaining(2);
	let a1b = answered(
		"W1 resumed",
		spawn_write(&node, resumed, rows(&descriptor, &[11])),
	)
	.await;
	let s1b = records_of(&trace, &w1);
	eprintln!(
		"DC287 lease W1 resumed after the late apply: {:?}/{:?} {:?}",
		a1b.write.status,
		a1b.write.mutation_commit_knowledge(),
		shape(&s1b)
	);
	assert_ne!(a1b.write.status, WriteStatus::OK);
	assert_eq!(s1b.len(), 2);
	assert_eq!(
		s1b[1].holder.map(|holder| holder.refusal),
		Some(Some(lease_missing))
	);
	let resumed_evidence = evidence_of(&trace, &w1)[1].expect("the resumed attempt's evidence");
	eprintln!("DC287 lease evidence W1 resumed: {resumed_evidence:?}");
	assert_eq!(resumed_evidence.class, RowClass::Expired);
	assert!(
		resumed_evidence
			.applied_index
			.is_some_and(|applied| applied >= held_id.index)
	);
	let row = resumed_evidence.rows[0].expect("the consulted row");
	assert_eq!(
		(row.start.physical_ms, row.expiry.physical_ms),
		(b_stamp, b_expiry)
	);
	let provenance = resumed_evidence.provenance.expect("a covering row");
	assert_eq!(
		provenance
			.agree
			.map(|agree| (agree.ticket, agree.retained_matches, agree.outcome)),
		Some((b_ticket, 1, IssueOutcome::Unanswered)),
		"the row agrees with B, which was unanswered at its budget"
	);
	assert_eq!(provenance.later, None);

	// ---- C: recovery ----
	assert_eq!(
		node.data_server
			.refresh_leases_once(&node.ring, &clock, TTL_MS)
			.await,
		1
	);
	let c_stamp = clock.last();
	let c = stored(&node, range).await.expect("C applied");
	eprintln!(
		"DC287 lease C: request {c_stamp} after B's stored expiry {b_expiry}: stored {c:?} (generation {:?} -> {:?})",
		b.generation, c.generation
	);
	assert!(c_stamp > b_expiry);
	assert_eq!((c.start, c.expiry), (c_stamp, c_stamp + u64::from(TTL_MS)));
	assert_eq!(node.data_server.held_leases_for_test().await.len(), 1);
	assert!(cached(&node).contains(&(range, c_stamp)));
	// The source's gap rule: a grant requested after the stored lease expired is a regrant, a
	// new generation (Codex 01a20a6b: a different outcome is evidence to explain).
	assert_eq!(
		c.generation,
		b.generation.map(|generation| generation + 1),
		"a regrant after a gap"
	);
	// A fresh write after the regrant. Its first attempt can be refused at Reserve with
	// CommitFromFuture: the regrant's new lineage moves the decision clock floor, and a claim made
	// at once may lead the leader's stamp by more than the bound (seen in 2 of 12 runs of this
	// control before ADR 0127, and in this tree). Only that refusal is tolerated on the way; the
	// write is resumed until it commits, and the tolerated refusals are reported.
	let mut fresh = attempt(&descriptor, 3);
	let mut tolerated = 0;
	let a3 = loop {
		let answer = node
			.data_server
			.table_write_for_test(fresh, rows(&descriptor, &[13]))
			.await
			.unwrap()
			.0;
		if answer.write.status == WriteStatus::OK || fresh.attempts_remaining() <= 1 {
			break answer;
		}
		let records = records_of(&trace, &fresh);
		let last = records.last().expect("a stalled attempt is recorded");
		assert_eq!(
			(last.site, last.answer.decision_refusal()),
			(
				StallSite::Reserve,
				Some(crate::consensus::decisions::DecisionRefusal::CommitFromFuture)
			),
			"only the regrant's Reserve refusal is tolerated: {:?}",
			shape(&records)
		);
		tolerated += 1;
		fresh.set_attempts_remaining(fresh.attempts_remaining() - 1);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	};
	eprintln!(
		"DC287 lease C fresh write: {:?} after {tolerated} tolerated Reserve CommitFromFuture refusal(s) {:?}; evidence {:?}; live {:?}; stored {:?}",
		a3.write.status,
		shape(&records_of(&trace, &fresh)),
		evidence_of(&trace, &fresh),
		node.data_server.held_leases_for_test().await,
		stored(&node, range).await
	);
	assert_eq!(a3.write.status, WriteStatus::OK, "{a3:?}");
	// The old reservation, staged under the earlier generation and never completed.
	go4.notify_one();
	let a4 = answered("W4", t4).await;
	let s4 = records_of(&trace, &w4);
	eprintln!(
		"DC287 lease C old reservation (children {:?}): {:?}/{:?} {:?}",
		w4_record
			.claim
			.children
			.iter()
			.map(|child| child.lineage)
			.collect::<Vec<_>>(),
		a4.write.status,
		a4.write.mutation_commit_knowledge(),
		shape(&s4)
	);
	assert_ne!(
		a4.write.status,
		WriteStatus::OK,
		"never acknowledged: {a4:?}"
	);
	assert!(no_intent(&node, r4).await);
	drop(persists);

	tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a complete close")
		.unwrap();
}

/// Wendelon ADR 0127 D1: the witness's terminal outcomes come from the actual answer. A real
/// per-target denial inside a valid results vector is NotGranted (the older row stays, and a
/// holder refusing after its expiry sees it agree with its own Granted issue and a later
/// NotGranted one: no claim that anything was pending); an answer that fails validation after
/// its real apply is Error (and it did apply).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_witness_records_granted_not_granted_and_error_from_the_actual_answer() {
	let (_dir, mut cfg) = one_voter();
	cfg.write_stall_trace = true;
	let node = open(&cfg).await;
	until_settled(&node, None).await;
	let descriptor = descriptor();
	node.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_settled(&node, Some(descriptor.table_object_id)).await;
	let trace = node
		.data_server
		.write_stall_trace()
		.expect("the configuration enables the trace")
		.clone();
	let peer = node.data_server.decision_peer_for_test();
	peer.pause_takeover
		.store(true, std::sync::atomic::Ordering::Release);
	node.handles.lease_manager.abort();
	tokio::time::sleep(std::time::Duration::from_millis(500)).await;
	let targets = node.data_server.decision_lease_targets_for_test().await;
	let [(range, _, _)] = targets[..] else {
		panic!("one voter, one target: {targets:?}");
	};
	let clock = Stamps::default();
	let slot = |stamp: u64| {
		*trace
			.leases
			.slots_for_test()
			.iter()
			.find(|slot| slot.request.physical_ms == stamp)
			.expect("witnessed")
	};

	// Granted.
	assert_eq!(
		node.data_server
			.refresh_leases_once(&node.ring, &clock, TTL_MS)
			.await,
		1
	);
	let g_stamp = clock.last();
	let g = stored(&node, range).await.expect("applied");
	assert_eq!(g.start, g_stamp);
	assert_eq!(slot(g_stamp).outcome, IssueOutcome::Granted);

	// A distinct request stamp (the HLC order of a later issue is strict).
	tokio::time::sleep(std::time::Duration::from_millis(10)).await;
	// NotGranted: the leader's apply denies the target; the older row stays.
	peer.skew_lease_generation
		.store(true, std::sync::atomic::Ordering::Release);
	assert_eq!(
		node.data_server
			.refresh_leases_once(&node.ring, &clock, TTL_MS)
			.await,
		0
	);
	let n_stamp = clock.last();
	assert!(n_stamp > g_stamp);
	assert_eq!(stored(&node, range).await, Some(g), "the older row stays");
	assert_eq!(slot(n_stamp).outcome, IssueOutcome::NotGranted);

	// The holder after the older row's expiry.
	sleep_until_wall(g.expiry + 50).await;
	let probe = attempt(&descriptor, 3);
	let answer = answered(
		"the probe",
		spawn_write(&node, probe, rows(&descriptor, &[21])),
	)
	.await;
	assert_ne!(answer.write.status, WriteStatus::OK, "{answer:?}");
	let [Some(evidence)] = evidence_of(&trace, &probe)[..] else {
		panic!("one evidence: {:?}", evidence_of(&trace, &probe));
	};
	eprintln!("ADR0127 outcomes: the probe's evidence {evidence:?}");
	assert_eq!(evidence.site, LeaseSite::PlanAdmission);
	assert_eq!(evidence.class, RowClass::Expired);
	let row = evidence.rows[0].expect("the consulted row");
	assert_eq!(row.start.physical_ms, g_stamp);
	let provenance = evidence.provenance.expect("a covering row");
	assert_eq!(
		provenance.agree.map(|agree| (agree.ticket, agree.outcome)),
		Some((slot(g_stamp).ticket, IssueOutcome::Granted))
	);
	assert_eq!(
		provenance.later.map(|later| (later.ticket, later.outcome)),
		Some((slot(n_stamp).ticket, IssueOutcome::NotGranted)),
		"a later issue, denied: nothing pending is claimed"
	);

	// Error: the answer fails validation after its real apply.
	peer.malformed_lease_results
		.store(true, std::sync::atomic::Ordering::Release);
	assert_eq!(
		node.data_server
			.refresh_leases_once(&node.ring, &clock, TTL_MS)
			.await,
		0
	);
	let e_stamp = clock.last();
	assert_eq!(slot(e_stamp).outcome, IssueOutcome::Error);
	let applied = stored(&node, range).await.expect("a stored lease");
	eprintln!(
		"ADR0127 outcomes: granted {g_stamp}, not granted {n_stamp}, error {e_stamp}; stored after the Error {applied:?}"
	);
	assert_eq!(
		applied.start, e_stamp,
		"an Error does not say it was not applied"
	);

	tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a complete close")
		.unwrap();
}
