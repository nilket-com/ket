//! ADR 0725: a follower's offload at the committer boundary. The real slab
//! manager, sweeper, operation driver and `RaftCommitter` of a node that
//! follows, and a ParentDecisions state machine applied as another node's
//! leader would append it. The channel between them is the fixture: it can
//! stop after an entry applied and before its answer returns, and it can lose
//! an answer.

use super::*;
use crate::consensus::owner_ops::{OwnerOpAnswer, OwnerOpChannel, OwnerOpOutcome, SignedOwnerOp};
use crate::consensus::slab_ops::{
	OpResponse, RecordedOutcome, RetireOp, SlabCommand, TierGeneration,
};
use crate::consensus::{OwnerRef, WendelonRequest, WendelonResponse};
use crate::storage::core::StorageEngine;
use crate::storage::index::LoadedOp;
use crate::storage::op_driver::OpDriver;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::Semaphore;

/// The log as the owner sees it from a follower: every proposal is applied to
/// `sm` as an entry appended by `leader`, another node.
struct Ring {
	sm: WendelonStateMachine,
	leader: u32,
	index: AtomicU64,
	/// The next applied answer is reported as an unknown outcome.
	lose_next_answer: AtomicBool,
	/// The next Submit is not applied at all and is reported as an unknown
	/// outcome: a request that never reached the log.
	drop_next_submit: AtomicBool,
	/// Held after the next Submit applied and before its answer returns.
	pause_after_submit: std::sync::Mutex<Option<(Arc<Semaphore>, Arc<Semaphore>)>>,
	/// Every proposal's kind, in order.
	kinds: std::sync::Mutex<Vec<&'static str>>,
}

impl Ring {
	async fn apply(&self, request: WendelonRequest) -> (u64, WendelonResponse) {
		use openraft::storage::RaftStateMachine;
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
		let index = self.index.fetch_add(1, Ordering::SeqCst);
		let mut target = self.sm.clone();
		let mut responses = RaftStateMachine::apply(
			&mut target,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(3, self.leader), index),
				payload: EntryPayload::Normal(request),
			}],
		)
		.await
		.unwrap();
		(index, responses.remove(0))
	}

	fn kinds(&self) -> Vec<&'static str> {
		self.kinds.lock().unwrap().clone()
	}
}

#[async_trait]
impl OwnerOpChannel for Ring {
	async fn propose_owner_op(&self, op: SignedOwnerOp) -> OwnerOpOutcome {
		let submit = op.claimed_command().is_some();
		if submit && self.drop_next_submit.swap(false, Ordering::AcqRel) {
			self.kinds.lock().unwrap().push("submit-dropped");
			return OwnerOpOutcome::Unknown("test: the request never arrived".into());
		}
		let (log_index, response) = self.apply(WendelonRequest::OwnerOp(op)).await;
		let answer = match response {
			WendelonResponse::Op(OpResponse::Receipt(receipt)) => OwnerOpAnswer::Receipt(receipt),
			WendelonResponse::Op(OpResponse::Ack(receipt)) => OwnerOpAnswer::Ack(receipt),
			WendelonResponse::Refused(refusal) => OwnerOpAnswer::Refused(refusal),
			other => panic!("unexpected response to an owner operation: {other:?}"),
		};
		self.kinds.lock().unwrap().push(match (&answer, submit) {
			(OwnerOpAnswer::Ack(_), _) => "ack",
			(_, true) => "submit",
			(_, false) => "cancel",
		});
		let pause = if submit {
			self.pause_after_submit.lock().unwrap().take()
		} else {
			None
		};
		if let Some((entered, release)) = pause {
			entered.add_permits(1);
			release.acquire().await.unwrap().forget();
		}
		if self.lose_next_answer.swap(false, Ordering::AcqRel) {
			return OwnerOpOutcome::Unknown("test: the answer was lost".into());
		}
		OwnerOpOutcome::Applied { answer, log_index }
	}

	async fn read_cut(&self) -> std::io::Result<u64> {
		Ok(self.index.load(Ordering::SeqCst).saturating_sub(1))
	}

	async fn wait_applied(&self, _index: u64, _bound: Duration) -> std::io::Result<()> {
		Ok(())
	}

	fn leader_known(&self) -> bool {
		true
	}
}

struct Follower {
	_dirs: Vec<crate::test_support::TestDir>,
	manager: Arc<SlabManager>,
	l2: Arc<dyn ObjectStore>,
	ring: Arc<Ring>,
	committer: Arc<RaftCommitter>,
	raft: WendelonRaft,
	sweeper: Arc<SlabSweeper>,
	driver: OpDriver,
	owner: OwnerRef,
}

async fn follower(tag: &str) -> Follower {
	let l2_dir = fresh_dir(&format!("offload-{tag}-l2"));
	let wal = fresh_dir(&format!("offload-{tag}-wal"));
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let (l1, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
	let owner = manager.owner_ref().unwrap();
	let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
	let sm = WendelonStateMachine::new().with_tier_observer(observer);
	sm.seed_complete_snapshot_state_for_test(crate::consensus::raft_storage::SmState {
		decisions: Some(Default::default()),
		..Default::default()
	})
	.await;
	let ring = Arc::new(Ring {
		sm: sm.clone(),
		leader: owner.node() + 1,
		index: AtomicU64::new(10),
		lose_next_answer: AtomicBool::new(false),
		drop_next_submit: AtomicBool::new(false),
		pause_after_submit: std::sync::Mutex::new(None),
		kinds: std::sync::Mutex::new(Vec::new()),
	});
	// The owner registers and binds its key through the leader, as a node's
	// maintenance does.
	let key = ed25519_dalek::SigningKey::from_bytes(&[0x25; 32]);
	let signer = crate::consensus::decisions::Signer {
		node: owner.node(),
		incarnation: owner.incarnation(),
	};
	let (_, registered) = ring
		.apply(WendelonRequest::RegisterIncarnation(
			crate::consensus::RegisterIncarnation {
				node: owner.node(),
				incarnation: owner.incarnation(),
				supersedes: None,
			},
		))
		.await;
	assert_eq!(registered, WendelonResponse::Success);
	ring.apply(WendelonRequest::Decision(
		crate::consensus::decisions::DecisionCommand::BindSigner {
			signer,
			key: key.verifying_key().to_bytes(),
		},
	))
	.await;
	assert!(
		sm.read_state(move |state| state.decisions.as_ref().unwrap().signer_key(signer))
			.await
			.is_some()
	);
	let (raft, _log, _follower_sm) = b1_real_follower(&wal).await;
	let committer = Arc::new(
		RaftCommitter::new(raft.clone())
			.with_state_machine(sm.clone())
			.with_owner_authority(Some(OwnerAuthority::new(key, ring.clone()))),
	);
	assert!(!committer.should_propose(), "this node follows");
	assert!(committer.may_offload());
	let sweeper = Arc::new(SlabSweeper::new(
		manager.clone(),
		l2.clone(),
		committer.clone(),
		sm,
		b1_config(),
	));
	let driver = OpDriver::new(manager.clone(), committer.clone());
	Follower {
		_dirs: vec![l1, l2_dir, wal],
		manager,
		l2,
		ring,
		committer,
		raft,
		sweeper,
		driver,
		owner,
	}
}

impl Follower {
	async fn sealed(&self, key: u64) -> SealedSlabInfo {
		let slab_id = self
			.manager
			.lookup(tenant(), id(key))
			.unwrap()
			.unwrap()
			.slab_id;
		self.manager
			.list_sealed_slabs()
			.await
			.into_iter()
			.find(|info| info.slab_id == slab_id)
			.expect("the row's slab is sealed")
	}

	async fn intents(&self) -> Vec<LoadedOp> {
		self.manager
			.require_op_index()
			.unwrap()
			.load_op_intents()
			.await
			.unwrap()
	}

	/// The ring's pointer for this store's slab.
	async fn committed(&self, slab_id: SlabId) -> Option<TieredSlab> {
		let slab = self.owner.slab(slab_id);
		self.ring
			.sm
			.read_state(move |state| state.tiered_slabs.get(&slab).cloned())
			.await
	}

	/// The ring's record of this owner's operation `seq`, and whether it is acknowledged.
	async fn recorded(&self, seq: u64) -> (Option<(RecordedOutcome, bool)>, u64) {
		let owner = self.owner;
		self.ring
			.sm
			.read_state(move |state| {
				let table = state.op_tables.get(&owner);
				(
					table
						.and_then(|table| table.outcomes.get(&seq))
						.map(|entry| (entry.outcome.clone(), entry.acked)),
					table.map_or(0, |table| table.acked_through),
				)
			})
			.await
	}

	/// The committed object is in the store, whole.
	async fn object_is_intact(&self, tier: &TieredSlab) {
		let bytes = self.l2.get(&tier.l2_uri).await.unwrap();
		assert_eq!(crate::hash::content::digest(&bytes), tier.payload_hash);
	}

	async fn close(self) {
		self.raft.shutdown().await.unwrap();
	}
}

fn first_publication_seq(intents: &[LoadedOp]) -> u64 {
	let seqs: Vec<u64> = intents
		.iter()
		.filter_map(|loaded| match loaded {
			LoadedOp::Submittable(ticket) if ticket.command().is_first_publication() => {
				Some(ticket.key().op_seq)
			}
			_ => None,
		})
		.collect();
	assert_eq!(
		seqs.len(),
		1,
		"one first publication is pending: {intents:?}"
	);
	seqs[0]
}

/// A follower's whole pass: every sealed slab is uploaded, committed through
/// the other node's leader entries, and its local file removed; the driver then
/// hands off and acknowledges from where it is. Nothing is cancelled.
#[tokio::test]
async fn a_followers_pass_offloads_every_sealed_slab_and_removes_its_files() {
	let node = follower("pass").await;
	let sealed = node.manager.list_sealed_slabs().await;
	assert!(sealed.len() >= 2);
	let report = node.sweeper.sweep_once().await.unwrap();
	assert_eq!((report.tiered, report.failed), (sealed.len(), 0));
	for info in &sealed {
		assert!(
			!node.manager.slab_file_exists_for_test(info.slab_id),
			"slab {:?} still has its local file",
			info.slab_id
		);
		let tier = node
			.committed(info.slab_id)
			.await
			.expect("a committed pointer");
		node.object_is_intact(&tier).await;
		assert_eq!(
			node.manager
				.tier_pointer(info.slab_id)
				.await
				.unwrap()
				.l2_uri,
			tier.l2_uri
		);
	}
	let location = node.manager.lookup(tenant(), id(1)).unwrap().unwrap();
	assert_eq!(node.manager.read(location).await.unwrap(), b"hello-1");
	node.driver.drive_once().await.unwrap();
	assert!(node.intents().await.is_empty());
	let (_, acked_through) = node.recorded(1).await;
	assert_eq!(acked_through, sealed.len() as u64);
	assert!(!node.ring.kinds().contains(&"cancel"));
	assert_eq!(node.manager.tier_authority_readiness_reason(), None);
	assert!(!node.committer.should_propose(), "it followed throughout");
	node.close().await;
}

/// The receipt is not completion. With the follower's own apply observer
/// failing, and the pass stopped before its own post-commit step: the pointer
/// is decided, the local file is still there, the intent remains and nothing is
/// acknowledged, even if the driver runs. Handoff and acknowledgement come only
/// after the local effect is durable.
#[tokio::test]
async fn a_decided_pointer_is_not_acknowledged_before_its_local_effect_is_durable() {
	let node = follower("observer").await;
	let info = node.sealed(1).await;
	let (entered, release) = (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)));
	*node.ring.pause_after_submit.lock().unwrap() = Some((entered.clone(), release.clone()));
	tier_apply_failure::arm(node.owner);
	let pass = tokio::spawn({
		let sweeper = node.sweeper.clone();
		let info = info.clone();
		async move { sweeper.tier_one_slab(&info).await }
	});
	tokio::time::timeout(Duration::from_secs(30), entered.acquire())
		.await
		.expect("the Submit applied")
		.unwrap()
		.forget();
	assert!(
		!tier_apply_failure::is_armed(node.owner),
		"the observer ran for this store's pointer and failed"
	);
	let tier = node
		.committed(info.slab_id)
		.await
		.expect("the ring decided");
	assert!(node.manager.slab_file_exists_for_test(info.slab_id));
	assert!(node.manager.tier_pointer(info.slab_id).await.is_none());
	let seq = first_publication_seq(&node.intents().await);
	let (record, acked_through) = node.recorded(seq).await;
	assert!(matches!(
		record,
		Some((RecordedOutcome::Applied { .. }, false))
	));
	assert_eq!(acked_through, 0);
	// The handoff the submission makes on an Applied receipt, called directly on
	// the pending ticket: its durability predicate refuses, and the same intent
	// stays. (The pause is before the answer returns; this call is what shows the
	// predicate, not the pause.)
	let ticket = match node.intents().await.into_iter().next() {
		Some(LoadedOp::Submittable(ticket)) => ticket,
		other => panic!("{other:?}"),
	};
	assert_eq!(ticket.key().op_seq, seq);
	node.manager.hand_off_op_if_complete(&ticket).await.unwrap();
	let after = node.intents().await;
	assert!(
		matches!(
			after.as_slice(),
			[LoadedOp::Submittable(same)]
				if same.key() == ticket.key() && same.digest() == ticket.digest()
		),
		"the intent survives a handoff attempt before its effect is durable: {after:?}"
	);
	assert_eq!(node.ring.kinds(), vec!["submit"]);
	// The driver, running now, neither hands the operation off nor acknowledges it.
	node.driver.drive_once().await.unwrap();
	assert_eq!(first_publication_seq(&node.intents().await), seq);
	assert_eq!(node.ring.kinds(), vec!["submit"]);
	// The pass continues: its own post-commit step completes the local effect.
	release.add_permits(1);
	pass.await.unwrap().unwrap();
	assert!(!node.manager.slab_file_exists_for_test(info.slab_id));
	assert_eq!(
		node.manager
			.tier_pointer(info.slab_id)
			.await
			.unwrap()
			.l2_uri,
		tier.l2_uri
	);
	assert_eq!(node.ring.kinds(), vec!["submit"], "still not acknowledged");
	// Now durable: the driver hands it off and its acknowledgement is accepted.
	node.driver.drive_once().await.unwrap();
	assert!(node.intents().await.is_empty());
	assert_eq!(node.ring.kinds(), vec!["submit", "ack"]);
	let (record, acked_through) = node.recorded(seq).await;
	assert!(record.is_none() || matches!(record, Some((_, true))));
	assert_eq!(acked_through, seq);
	node.object_is_intact(&tier).await;
	node.close().await;
}

/// The Submit commits and its answer is lost. The pass ends with an unknown
/// outcome and deletes nothing; the same ticket then resolves to its recorded
/// Applied outcome, without a Cancel and with its live object intact, and a
/// reopen fence over it lifts while the node still follows.
#[tokio::test]
async fn a_lost_applied_answer_resolves_to_its_record_and_keeps_its_live_object() {
	let node = follower("lost").await;
	let info = node.sealed(1).await;
	let location = node.manager.lookup(tenant(), id(1)).unwrap().unwrap();
	node.ring.lose_next_answer.store(true, Ordering::Release);
	let error = node.sweeper.tier_one_slab(&info).await.unwrap_err();
	assert!(
		!tier_proposal_not_submitted(&error),
		"a lost answer is not a proof of no submission: {error}"
	);
	let tier = node
		.committed(info.slab_id)
		.await
		.expect("the ring decided");
	node.object_is_intact(&tier).await;
	let seq = first_publication_seq(&node.intents().await);
	assert!(matches!(
		node.recorded(seq).await.0,
		Some((RecordedOutcome::Applied { .. }, false))
	));
	// As a reopen with this intent would find it.
	node.manager.fence_unresolved_op_for_test();
	assert_eq!(
		node.manager.tier_authority_readiness_reason().as_deref(),
		Some("UnresolvedOpIntent")
	);
	node.driver.drive_once().await.unwrap();
	assert!(node.intents().await.is_empty());
	assert_eq!(
		node.ring.kinds(),
		vec!["submit", "ack"],
		"no Cancel, no second Submit"
	);
	let (_, acked_through) = node.recorded(seq).await;
	assert_eq!(acked_through, seq);
	assert_eq!(node.committed(info.slab_id).await, Some(tier.clone()));
	node.object_is_intact(&tier).await;
	assert!(!node.manager.slab_file_exists_for_test(info.slab_id));
	assert_eq!(node.manager.read(location).await.unwrap(), b"hello-1");
	assert_eq!(node.manager.tier_authority_readiness_reason(), None);
	assert!(!node.committer.should_propose(), "it followed throughout");
	node.close().await;
}

/// The same recovery with another, leader-only operation unresolved: the first
/// publication still resolves, the other intent is neither sent nor dropped,
/// and the store stays unready on both counts.
#[tokio::test]
async fn a_leader_only_intent_keeps_a_follower_unready_after_its_first_publication_resolves() {
	let node = follower("leader-only").await;
	let info = node.sealed(1).await;
	let other = node.sealed(2).await;
	node.ring.lose_next_answer.store(true, Ordering::Release);
	node.sweeper.tier_one_slab(&info).await.unwrap_err();
	let first = first_publication_seq(&node.intents().await);
	let retire = SlabCommand::Retire(RetireOp {
		slab: node.owner.slab(other.slab_id),
		expected: TierGeneration {
			l2_uri: "never-published".into(),
			payload_hash: [0; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
		},
		certified: None,
	});
	let held = node
		.manager
		.require_op_index()
		.unwrap()
		.allocate_ops(vec![retire.clone()], None)
		.await
		.unwrap()
		.first
		.key()
		.op_seq;
	node.manager.fence_unresolved_op_for_test();
	let _ = node.driver.drive_once().await;
	let intents = node.intents().await;
	assert!(
		matches!(
			intents.as_slice(),
			[LoadedOp::Submittable(ticket)]
				if ticket.key().op_seq == held && ticket.command() == &retire
		),
		"only the leader-only intent remains: {intents:?}"
	);
	assert!(matches!(
		node.recorded(first).await.0,
		None | Some((RecordedOutcome::Applied { .. }, _))
	));
	assert!(node.recorded(held).await.0.is_none(), "it was never sent");
	assert!(!node.ring.kinds().contains(&"cancel"));
	assert_eq!(
		node.manager.tier_authority_readiness_reason().as_deref(),
		Some("AwaitingLeadership")
	);
	// Under that report the reopen fence itself is still raised.
	node.manager.note_awaiting_leadership(false);
	assert_eq!(
		node.manager.tier_authority_readiness_reason().as_deref(),
		Some("UnresolvedOpIntent")
	);
	node.close().await;
}

/// A Submit that never reached the log ends with an unknown outcome and an
/// ambiguity record that fences the slab. While the outcome is unknown the slab
/// stays fenced. Once the follower's Cancel is recorded and the operation is
/// handed off Rejected, the durable rejected-candidate row resolves that record:
/// the next pass publishes the slab with a new operation and removes its file.
/// The first upload is neither published nor deleted, and stays accounted for.
#[tokio::test]
async fn a_rejected_unknown_outcome_does_not_fence_its_slab_for_good() {
	let node = follower("rejected").await;
	let info = node.sealed(1).await;
	let location = node.manager.lookup(tenant(), id(1)).unwrap().unwrap();
	node.ring.drop_next_submit.store(true, Ordering::Release);
	let error = node.sweeper.tier_one_slab(&info).await.unwrap_err();
	assert!(!tier_proposal_not_submitted(&error), "{error}");
	let seq = first_publication_seq(&node.intents().await);
	assert!(node.recorded(seq).await.0.is_none(), "nothing was decided");
	assert!(
		node.manager
			.refuse_unresolved_cleanup(info.slab_id)
			.is_err()
	);
	// Still unknown: a pass resolves nothing and the slab stays fenced and local.
	let _ = node.sweeper.sweep_once().await;
	assert!(
		node.manager
			.refuse_unresolved_cleanup(info.slab_id)
			.is_err()
	);
	assert!(node.manager.slab_file_exists_for_test(info.slab_id));
	assert!(node.committed(info.slab_id).await.is_none());
	assert_eq!(first_publication_seq(&node.intents().await), seq);
	// The follower's driver cancels the undecided operation and hands it off.
	node.driver.drive_once().await.unwrap();
	assert!(matches!(
		node.recorded(seq).await.0,
		None | Some((RecordedOutcome::Rejected { .. }, _))
	));
	let index = node.manager.require_op_index().unwrap();
	let rejected = index.load_rejected_tier_candidates().await.unwrap();
	assert_eq!(rejected.len(), 1);
	assert_eq!(
		(rejected[0].op_seq, rejected[0].slab_id),
		(seq, info.slab_id)
	);
	let first_uri = rejected[0].l2_uri.clone();
	assert!(
		node.manager
			.refuse_unresolved_cleanup(info.slab_id)
			.is_err(),
		"the record is resolved by a pass, not by the handoff"
	);
	// The next pass resolves the record on that evidence and publishes the slab.
	node.sweeper.sweep_once().await.unwrap();
	assert!(node.manager.refuse_unresolved_cleanup(info.slab_id).is_ok());
	let tier = node
		.committed(info.slab_id)
		.await
		.expect("published by a second operation");
	assert_ne!(
		tier.l2_uri, first_uri,
		"a rejected candidate is never published"
	);
	node.object_is_intact(&tier).await;
	assert!(!node.manager.slab_file_exists_for_test(info.slab_id));
	assert_eq!(node.manager.read(location).await.unwrap(), b"hello-1");
	// The first upload is still in the store and still accounted as rejected.
	node.l2
		.get(&first_uri)
		.await
		.expect("the rejected upload is not deleted");
	assert_eq!(
		index.load_rejected_tier_candidates().await.unwrap(),
		rejected
	);
	node.driver.drive_once().await.unwrap();
	assert!(node.intents().await.is_empty());
	assert_eq!(node.manager.tier_authority_readiness_reason(), None);
	assert!(!node.committer.should_propose(), "it followed throughout");
	node.close().await;
}

/// A Submit that never reached the log, then the driver's Cancel and rejected
/// handoff: the slab's ambiguity record, its first upload's URI, and the
/// rejected-candidate row for it.
async fn rejected_ambiguity(node: &Follower, key: u64) -> (SealedSlabInfo, String) {
	let info = node.sealed(key).await;
	node.ring.drop_next_submit.store(true, Ordering::Release);
	node.sweeper.tier_one_slab(&info).await.unwrap_err();
	node.driver.drive_once().await.unwrap();
	let rejected = node
		.manager
		.require_op_index()
		.unwrap()
		.load_rejected_tier_candidates()
		.await
		.unwrap();
	assert_eq!(rejected.len(), 1);
	assert_eq!(rejected[0].slab_id, info.slab_id);
	assert!(
		node.manager
			.refuse_unresolved_cleanup(info.slab_id)
			.is_err()
	);
	(info, rejected[0].l2_uri.clone())
}

/// The durable removal committed and its caller never updated its memory (a
/// task cancelled after the commit). The next pass still sees the record in
/// memory; the removal answers that it is already made, the fence clears and
/// the offload continues.
#[tokio::test]
async fn a_removal_committed_before_its_caller_noticed_still_clears_the_fence() {
	let node = follower("retry").await;
	let (info, first_uri) = rejected_ambiguity(&node, 1).await;
	let index = node.manager.require_op_index().unwrap();
	assert!(
		index
			.remove_standalone_rejected_provisional(info.slab_id, first_uri.clone())
			.await
			.unwrap()
	);
	assert!(
		index
			.load_snapshot_cleanup_intents()
			.await
			.unwrap()
			.is_empty(),
		"the durable record is gone"
	);
	assert!(
		node.manager
			.refuse_unresolved_cleanup(info.slab_id)
			.is_err(),
		"memory still holds it"
	);
	node.sweeper.sweep_once().await.unwrap();
	assert!(node.manager.refuse_unresolved_cleanup(info.slab_id).is_ok());
	let tier = node.committed(info.slab_id).await.expect("published");
	assert_ne!(tier.l2_uri, first_uri);
	assert!(!node.manager.slab_file_exists_for_test(info.slab_id));
	node.close().await;
}

/// What the removal refuses, at the index: no rejected-candidate row is no
/// authority, whether or not a record exists; a provisional that an old-object
/// intent owns is refused even with the row. Each leaves every row as it was.
#[tokio::test]
async fn the_rejected_removal_needs_its_row_and_leaves_other_records_alone() {
	use crate::storage::index::{RetainedObjectIntent, RetainedObjectIntentState};
	let node = follower("authority").await;
	let (info, first_uri) = rejected_ambiguity(&node, 1).await;
	let other = node.sealed(2).await;
	let index = node.manager.require_op_index().unwrap();
	let record = |replacement: &str, state| RetainedObjectIntent {
		bytes: Some(1),
		replacement_uri: Some(replacement.to_owned()),
		replacement_bytes: None,
		published_replacements: Default::default(),
		retirement_source: None,
		state,
	};
	// No rejected-candidate row for this slab and URI: nothing, with or without a record.
	assert!(
		!index
			.remove_standalone_rejected_provisional(other.slab_id, "never-rejected".into())
			.await
			.unwrap()
	);
	index
		.put_snapshot_cleanup_intent(
			other.slab_id,
			"never-rejected".into(),
			record("never-rejected", RetainedObjectIntentState::Unresolved),
		)
		.await
		.unwrap();
	let before = index.load_snapshot_cleanup_intents().await.unwrap();
	assert_eq!(before.len(), 2);
	assert!(
		!index
			.remove_standalone_rejected_provisional(other.slab_id, "never-rejected".into())
			.await
			.unwrap()
	);
	// The same URI under another slab is not this row either.
	assert!(
		!index
			.remove_standalone_rejected_provisional(other.slab_id, first_uri.clone())
			.await
			.unwrap()
	);
	assert_eq!(index.load_snapshot_cleanup_intents().await.unwrap(), before);
	// An old-object intent owns the rejected URI as its replacement: refused.
	index
		.put_snapshot_cleanup_intent(
			info.slab_id,
			"old-object".into(),
			record(&first_uri, RetainedObjectIntentState::Unresolved),
		)
		.await
		.unwrap();
	let owned = index.load_snapshot_cleanup_intents().await.unwrap();
	assert_eq!(owned.len(), 3);
	let refused = index
		.remove_standalone_rejected_provisional(info.slab_id, first_uri.clone())
		.await
		.unwrap_err();
	assert_eq!(refused.kind(), std::io::ErrorKind::WouldBlock);
	assert_eq!(index.load_snapshot_cleanup_intents().await.unwrap(), owned);
	// The rejected-candidate row is never touched by any of it.
	assert_eq!(
		index.load_rejected_tier_candidates().await.unwrap().len(),
		1
	);
	node.close().await;
}

/// A record of another kind under a rejected URI is left alone: with the
/// rejected-candidate row present, a not-submitted record is not removed.
#[tokio::test]
async fn the_rejected_removal_leaves_a_not_submitted_record_alone() {
	use crate::storage::index::{RetainedObjectIntent, RetainedObjectIntentState};
	let node = follower("other-kind").await;
	let (info, first_uri) = rejected_ambiguity(&node, 1).await;
	let index = node.manager.require_op_index().unwrap();
	// The Unresolved record goes, on the row's evidence; the key is free again.
	assert!(
		index
			.remove_standalone_rejected_provisional(info.slab_id, first_uri.clone())
			.await
			.unwrap()
	);
	index
		.put_snapshot_cleanup_intent(
			info.slab_id,
			first_uri.clone(),
			RetainedObjectIntent {
				bytes: Some(1),
				replacement_uri: Some(first_uri.clone()),
				replacement_bytes: None,
				published_replacements: Default::default(),
				retirement_source: None,
				state: RetainedObjectIntentState::CandidateNotSubmitted,
			},
		)
		.await
		.unwrap();
	let rows = index.load_snapshot_cleanup_intents().await.unwrap();
	assert_eq!(rows.len(), 1);
	assert!(
		!index
			.remove_standalone_rejected_provisional(info.slab_id, first_uri.clone())
			.await
			.unwrap()
	);
	assert_eq!(index.load_snapshot_cleanup_intents().await.unwrap(), rows);
	assert_eq!(
		index.load_rejected_tier_candidates().await.unwrap().len(),
		1
	);
	node.close().await;
}
